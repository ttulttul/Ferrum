use super::{detail::can_write, error, failure};
use crate::provider_rpc::{Failure, Github, Reply};
use serde_json::{json, Value};

const OP: &str = "runAction";
const SUMMARY: &str = include_str!("PullRequestActionSummary.graphql");

fn provider_error(f: Failure) -> Value {
    failure(OP, f)
}

fn required<'a>(record: &'a Value, key: &str) -> Result<&'a str, Value> {
    record[key]
        .as_str()
        .filter(|s| !s.is_empty())
        .ok_or_else(|| provider_error(Failure::Invalid))
}

fn refused(action: &str) -> Value {
    let detail = match action {
        "merge" => "You need write access on this repository to merge.",
        "ready" => "You need write access on this repository, or to have opened this change request, to mark it ready for review.",
        "draft" => "You need write access on this repository, or to have opened this change request, to return it to a draft.",
        "close" => "You need write access on this repository, or to have opened this change request, to close it.",
        "reopen" => "You need write access on this repository, or to have opened this change request, to reopen it.",
        "update-branch" => "You need write access on this repository, or to have opened this change request, to update its branch.",
        "enable-auto-merge" => "Auto-merge needs write access on this repository, and the repository has to allow it.",
        "disable-auto-merge" => "You need write access on this repository to stop it being merged for you once it is ready.",
        "revert" => "You need write access on this repository to open a revert pull request.",
        "delete-branch" => "You need write access on the source repository to delete its branch.",
        "approve-workflows" => "You need write access on this repository to approve workflows from a fork pull request.",
        _ => return error(OP, &format!("This host cannot {action} a change request.")),
    };
    error(OP, detail)
}

async fn query_pr(gh: &Github<'_>, query: &str, vars: &Value) -> Reply {
    let data = gh
        .graphql(query, vars.clone())
        .await
        .map_err(provider_error)?;
    let pr = &data["repository"]["pullRequest"];
    if pr.is_null() {
        return Err(provider_error(Failure::NotFound));
    }
    Ok(pr.clone())
}

fn action_summary(project: &str, repo: &str, pr: &Value, at: u128) -> Reply {
    if pr.is_null() {
        return Ok(Value::Null);
    }
    let actor = &pr["author"];
    let state = match required(pr, "state")? {
        _ if pr["mergedAt"].is_string() => "merged",
        "MERGED" => "merged",
        "CLOSED" => "closed",
        "OPEN" => "open",
        _ => return Err(provider_error(Failure::Invalid)),
    };
    let author = actor["login"]
        .as_str()
        .map(|login| json!({"login":login,"name":actor["name"],"avatarUrl":actor["avatarUrl"]}));
    let mut out = json!({
        "provider": "github", "projectId": project, "repository": repo,
        "number": pr["number"], "title": required(pr, "title")?, "url": required(pr, "url")?,
        "state":state, "isDraft":pr["isDraft"], "headBranch":required(pr,"headRefName")?,
        "baseBranch":required(pr,"baseRefName")?, "closedAt":pr["closedAt"], "mergedAt":pr["mergedAt"],
        "updatedAt":required(pr,"updatedAt")?, "observedAt":at, "author":author,
        "headSha":required(pr,"headRefOid")?, "autoMergeEnabled":!pr["autoMergeRequest"].is_null()
    });
    if let Some(method) = pr["autoMergeRequest"]["mergeMethod"].as_str() {
        out["autoMergeMethod"] = json!(method.to_ascii_lowercase());
    }
    Ok(out)
}

pub(super) async fn run(
    gh: &mut Github<'_>,
    project: &str,
    repo: &str,
    number: u64,
    input: &Value,
) -> Reply {
    let (owner, name) = repo.split_once('/').unwrap();
    let vars = json!({"owner":owner, "name":name, "number":number});
    let action = required(input, "action")?;
    if !input["stackNumber"].is_null() {
        return Err(error(
            OP,
            "This stack action is not supported or has no expected head revision.",
        ));
    }
    if action == "delete-branch" && input["expectedAccountId"].is_null() {
        gh.pin_credential().await.map_err(provider_error)?;
    }
    let access = gh
        .graphql(include_str!("ViewerPermissions.graphql"), vars.clone())
        .await
        .map_err(provider_error)?;
    let repository = &access["repository"];
    if repository.is_null() {
        return Err(provider_error(Failure::NotFound));
    }
    let write = can_write(repository);
    let permitted = match action {
        "close" | "reopen" | "ready" | "draft" => {
            repository["pullRequest"]["viewerCanUpdate"] != false
        }
        "merge" | "revert" | "approve-workflows" | "disable-auto-merge" => write,
        "enable-auto-merge" => write && repository["autoMergeAllowed"] == true,
        "delete-branch" => can_write(&repository["pullRequest"]["headRepository"]),
        "update-branch" => {
            let mut core = vars.clone();
            core["headRef"] = json!(format!("refs/pull/{number}/head"));
            core["required"] = json!(true);
            core["requestedReviewers"] = json!(100);
            core["mergeQueue"] = json!(true);
            let pr = query_pr(gh, include_str!("PullRequestCore.graphql"), &core).await?;
            pr["state"] == "OPEN"
                && pr["viewerCanUpdateBranch"] == true
                && !pr["baseRef"]["compare"].is_null()
        }
        _ => false,
    };
    if !permitted {
        return Err(refused(action));
    }
    if input.get("updateMethod").is_some() && action != "update-branch" {
        return Err(refused("update-branch"));
    }
    let pinned = if let Some(expected) = input["expectedHeadSha"].as_str() {
        let (current, _) = super::detail::read_core(gh, OP, repo, number).await?;
        let head = current["repository"]["pullRequest"]["headRefOid"].as_str();
        Some(super::pinned_head(head, expected)?.to_owned())
    } else {
        None
    };
    if action == "delete-branch" {
        return delete_branch(gh, &vars, number).await;
    }
    if action == "approve-workflows" {
        return workflows::approve(gh, repo, &vars, number).await;
    }

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    let (method, signature, argument, parameters) = match action {
        "merge" | "enable-auto-merge" | "update-branch" => {
            let mut lookup = vars.clone();
            lookup["headRef"] = json!(format!("refs/pull/{number}/head"));
            let state =
                query_pr(gh, include_str!("PullRequestActionState.graphql"), &lookup).await?;
            let id = required(&state, "id")?;
            if action == "update-branch" {
                if state["baseRef"]["compare"]["behindBy"] == 0 {
                    return Ok(Value::Null);
                }
                let strategy = match input["updateMethod"].as_str() {
                    None | Some("merge") => "MERGE",
                    Some("rebase") => "REBASE",
                    Some(other) => {
                        return Err(error(
                            OP,
                            &format!("This host cannot update a branch by {other}."),
                        ))
                    }
                };
                (
                    "updatePullRequestBranch",
                    "$input:UpdatePullRequestBranchInput!",
                    "$input",
                    json!({"input":{"pullRequestId":id,"expectedHeadOid":required(&state,"headRefOid")?,"updateMethod":strategy}}),
                )
            } else {
                let strategy = match input["mergeMethod"].as_str() {
                    None | Some("merge") => "MERGE",
                    Some("squash") => "SQUASH",
                    Some("rebase") => "REBASE",
                    Some(other) => {
                        return Err(error(
                            OP,
                            &format!("This host cannot merge with the {other} strategy."),
                        ))
                    }
                };
                let queue = state["isMergeQueueEnabled"] == true;
                let ready = matches!(
                    state["mergeStateStatus"].as_str(),
                    Some("CLEAN" | "HAS_HOOKS" | "UNSTABLE")
                );
                let (method, signature) = if queue || (action == "enable-auto-merge" && !ready) {
                    (
                        "enablePullRequestAutoMerge",
                        "$input:EnablePullRequestAutoMergeInput!",
                    )
                } else {
                    ("mergePullRequest", "$input:MergePullRequestInput!")
                };
                let mut merge = json!({"pullRequestId":id,"mergeMethod":strategy});
                if let Some(head) = pinned {
                    merge["expectedHeadOid"] = json!(head);
                }
                (method, signature, "$input", json!({"input":merge}))
            }
        }
        _ => {
            let pr = query_pr(gh, include_str!("PullRequestNodeId.graphql"), &vars).await?;
            let parameters = json!({"pullRequestId":required(&pr, "id")?});
            let method = match action {
                "close" => "closePullRequest",
                "reopen" => "reopenPullRequest",
                "ready" => "markPullRequestReadyForReview",
                "draft" => "convertPullRequestToDraft",
                "disable-auto-merge" => "disablePullRequestAutoMerge",
                "revert" => {
                    gh.graphql("mutation($pullRequestId:ID!){revertPullRequest(input:{pullRequestId:$pullRequestId}){revertPullRequest{id}}}",parameters)
                        .await.map_err(provider_error)?;
                    return Ok(Value::Null);
                }
                _ => return Err(refused(action)),
            };
            (
                method,
                "$pullRequestId:ID!",
                "{pullRequestId:$pullRequestId}",
                parameters,
            )
        }
    };
    let query = format!("mutation({signature}){{{method}(input:{argument}){{pullRequest{{...PullRequestActionSummary}}}}}}\n{SUMMARY}");
    let data = gh
        .graphql(&query, parameters)
        .await
        .map_err(provider_error)?;
    if input["replyWithSummary"] != true {
        return Ok(Value::Null);
    }
    action_summary(project, repo, &data[method]["pullRequest"], now)
}

async fn delete_branch(gh: &Github<'_>, vars: &Value, number: u64) -> Reply {
    let pr = query_pr(gh, include_str!("PullRequestBranchToDelete.graphql"), vars).await?;
    let incomplete = || {
        error(
            OP,
            &format!("GitHub did not report a complete source branch for pull request #{number}."),
        )
    };
    let unsafe_branch = || {
        error(
            OP,
            "The source branch is a default or base branch and cannot be deleted here.",
        )
    };
    match pr["state"].as_str() {
        Some("OPEN") => {
            return Err(error(
                OP,
                &format!(
                    "Close or merge pull request #{number} before deleting its source branch."
                ),
            ))
        }
        Some("CLOSED" | "MERGED") => {}
        _ => return Err(incomplete()),
    }
    let id = required(&pr["headRepository"], "id").map_err(|_| incomplete())?;
    let branch = required(&pr, "headRefName").map_err(|_| incomplete())?;
    let tip = required(&pr, "headRefOid").map_err(|_| incomplete())?;
    if pr["baseRefName"] == branch {
        return Err(unsafe_branch());
    }
    let reference = format!("refs/heads/{branch}");
    let query = include_str!("PullRequestSourceBranch.graphql");
    let data = gh
        .graphql(query, json!({"repositoryId":id, "ref":reference}))
        .await
        .map_err(provider_error)?;
    let source = &data["node"];
    if source["__typename"] != "Repository" || source.get("ref").is_none() {
        return Err(incomplete());
    }
    if source["ref"].is_null() {
        return Ok(Value::Null);
    }
    if required(&source["defaultBranchRef"], "name").is_err()
        || source["defaultBranchRef"]["name"] == branch
    {
        return Err(unsafe_branch());
    }
    if source["ref"]["target"]["oid"] != tip {
        return Err(error(
            OP,
            "The source branch has new commits and was not deleted.",
        ));
    }
    let args = json!({"input":{"repositoryId":id,"refUpdates":[{"name":reference,"beforeOid":tip,"afterOid":"0000000000000000000000000000000000000000"}]}});
    gh.graphql(
        "mutation($input:UpdateRefsInput!){updateRefs(input:$input){clientMutationId}}",
        args,
    )
    .await
    .map_err(provider_error)?;
    Ok(Value::Null)
}

mod workflows;
