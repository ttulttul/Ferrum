use super::{error, provider_error, query_pr, required, Failure, Github, Reply, OP};
use serde_json::{json, Value};
use std::collections::HashSet;

const LIMIT: usize = 1_000;
const PR_HEAD: &str = "query($owner:String!,$name:String!,$number:Int!){repository(owner:$owner,name:$name){pullRequest(number:$number){isCrossRepository headRefName headRefOid headRepositoryOwner{login}}}}";

#[derive(PartialEq, Eq)]
struct Head {
    cross_repository: bool,
    sha: String,
    branch: String,
    owner: String,
}

async fn head(gh: &Github<'_>, vars: &Value, number: u64) -> Result<Head, Value> {
    let pr = query_pr(gh, PR_HEAD, vars).await?;
    let missing = |_| {
        error(
            OP,
            &format!("GitHub did not report a complete head revision for #{number}."),
        )
    };
    Ok(Head {
        cross_repository: pr["isCrossRepository"]
            .as_bool()
            .ok_or_else(|| provider_error(Failure::Invalid))?,
        sha: required(&pr, "headRefOid").map_err(missing)?.to_owned(),
        branch: required(&pr, "headRefName").map_err(missing)?.to_owned(),
        owner: required(&pr["headRepositoryOwner"], "login")
            .map_err(missing)?
            .to_ascii_lowercase(),
    })
}

async fn verify_head(gh: &Github<'_>, repo: &str, number: u64, head: &Head) -> Result<(), Value> {
    let (owner, name) = repo.split_once('/').unwrap();
    let mut vars = json!({"owner":owner,"name":name,"head":head.branch});
    let mut matches = Vec::new();
    let mut count = 0;
    let mut cursors = HashSet::new();
    loop {
        let data = gh
            .graphql(include_str!("../PullRequestHeads.graphql"), vars.clone())
            .await
            .map_err(provider_error)?;
        let connection = &data["repository"]["pullRequests"];
        let nodes = connection["nodes"]
            .as_array()
            .ok_or_else(|| provider_error(Failure::Invalid))?;
        count += nodes.len();
        if count > LIMIT || cursors.len() > LIMIT / 100 {
            return Err(error(
                OP,
                &format!("GitHub returned more than {LIMIT} pull requests for this head branch."),
            ));
        }
        for pr in nodes {
            if pr["headRefOid"] == head.sha
                && pr["isCrossRepository"].as_bool() == Some(head.cross_repository)
                && pr["headRepositoryOwner"]["login"]
                    .as_str()
                    .is_some_and(|v| v.eq_ignore_ascii_case(&head.owner))
            {
                matches.push(pr["number"].as_u64());
            }
        }
        match connection["pageInfo"]["hasNextPage"].as_bool() {
            Some(false) => break,
            Some(true) => {
                let cursor = required(&connection["pageInfo"], "endCursor")?;
                if !cursors.insert(cursor.to_owned()) {
                    return Err(provider_error(Failure::Invalid));
                }
                vars["after"] = json!(cursor);
            }
            None => return Err(provider_error(Failure::Invalid)),
        }
    }
    if matches != [Some(number)] {
        return Err(error(OP,&format!("The head revision matched {} pull requests instead of uniquely matching #{number}.",matches.len())));
    }
    Ok(())
}

async fn runs(gh: &Github<'_>, repo: &str, head: &Head) -> Result<Vec<Value>, Value> {
    let mut found = Vec::new();
    let mut query = reqwest::Url::parse("https://api.github.com").expect("fixed URL");
    query
        .query_pairs_mut()
        .append_pair("head_sha", &head.sha)
        .append_pair("branch", &head.branch)
        .append_pair("event", "pull_request")
        .append_pair("status", "action_required")
        .append_pair("per_page", "100");
    for page in 1.. {
        let url = format!(
            "repos/{repo}/actions/runs?{}&page={page}",
            query.query().unwrap()
        );
        let response = gh.json("GET", &url, None).await.map_err(provider_error)?;
        let runs = response["workflow_runs"]
            .as_array()
            .ok_or_else(|| provider_error(Failure::Invalid))?;
        for run in runs {
            if !run["id"].is_u64()
                || run.get("pull_requests").is_some_and(|refs| {
                    !refs.is_null()
                        && !refs
                            .as_array()
                            .is_some_and(|rows| rows.iter().all(|pr| pr["number"].is_u64()))
                })
            {
                return Err(provider_error(Failure::Invalid));
            }
            found.push(run.clone());
        }
        if found.len() > LIMIT {
            return Err(error(
                OP,
                &format!("GitHub returned more than {LIMIT} workflow runs awaiting approval."),
            ));
        }
        if runs.len() < 100 {
            break;
        }
    }
    Ok(found)
}

async fn matched_runs(
    gh: &Github<'_>,
    repo: &str,
    number: u64,
    head: &Head,
) -> Result<Vec<Value>, Value> {
    let (_, mut rows) =
        tokio::try_join!(verify_head(gh, repo, number, head), runs(gh, repo, head))?;
    if !head.cross_repository
        && rows
            .iter()
            .any(|run| run["pull_requests"].as_array().is_none_or(Vec::is_empty))
    {
        return Err(error(
            OP,
            "GitHub omitted workflow run pull-request associations.",
        ));
    }
    rows.retain(|row| match row["pull_requests"].as_array() {
        Some(prs) if !prs.is_empty() => prs.iter().any(|pr| pr["number"] == number),
        _ => head.cross_repository,
    });
    Ok(rows)
}

/// The check reader needs both the fork identity and the names of approval-gated runs.
pub(in crate::pull_requests) async fn approval_runs(
    gh: &Github<'_>,
    repo: &str,
    number: u64,
    pr: &Value,
) -> Result<Vec<Value>, Value> {
    let head = Head {
        cross_repository: pr["isCrossRepository"]
            .as_bool()
            .ok_or_else(|| provider_error(Failure::Invalid))?,
        sha: required(pr, "headRefOid")?.into(),
        branch: required(pr, "headRefName")?.into(),
        owner: required(&pr["headRepositoryOwner"], "login")?.into(),
    };
    matched_runs(gh, repo, number, &head).await
}

pub(super) async fn approve(gh: &Github<'_>, repo: &str, vars: &Value, number: u64) -> Reply {
    let initial = head(gh, vars, number).await?;
    for run in matched_runs(gh, repo, number, &initial).await? {
        let id = run["id"]
            .as_u64()
            .ok_or_else(|| provider_error(Failure::Invalid))?;
        if head(gh, vars, number).await? != initial {
            return Err(error(OP,&format!("The head revision of #{number} changed before its workflows could be approved.")));
        }
        if matched_runs(gh, repo, number, &initial)
            .await?
            .iter()
            .any(|run| run["id"] == id)
        {
            gh.request(
                "POST",
                &format!("repos/{repo}/actions/runs/{id}/approve"),
                "application/vnd.github+json",
                None,
            )
            .await
            .map_err(provider_error)?;
        }
    }
    Ok(Value::Null)
}
