use super::{error, failure, identity};
use crate::{
    provider_rpc::{Failure, Github, Reply},
    Project,
};
use indexmap::IndexMap;
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet};

pub(super) const ACTIONS: [&str; 11] = [
    "merge",
    "ready",
    "draft",
    "close",
    "reopen",
    "delete-branch",
    "update-branch",
    "enable-auto-merge",
    "disable-auto-merge",
    "revert",
    "approve-workflows",
];
fn capabilities() -> Value {
    json!({"diff":true,"comment":true,"actions":ACTIONS,"mergeMethods":["merge","squash","rebase"],"deleteBranch":true,"updateMethods":["merge","rebase"],"search":true,"reactions":true,"viewedFiles":"host","review":{"inlineComment":true,"rangeComment":true,"reply":true,"resolve":true,"verdicts":["comment","approve","request-changes"]},"reviewers":{"request":true,"listCandidates":true,"searchCandidates":true},"edit":{"changeRequest":true,"comment":true,"maintainerEdits":true},"stacks":true,"stackActions":true,"labels":true,"mentions":true,"referenceSigils":["#"],"checksAlwaysReport":true,"withheldFileDiff":true})
}
fn trim(v: &Value) -> Option<&str> {
    v.as_str().map(str::trim).filter(|s| !s.is_empty())
}
fn nodes(v: &Value) -> impl Iterator<Item = &Value> {
    v["nodes"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|n| !n.is_null())
}
pub(super) fn can_write(repository: &Value) -> bool {
    match repository.get("viewerPermission") {
        Some(Value::Null) => true,
        Some(Value::String(role)) => matches!(
            role.trim().to_ascii_uppercase().as_str(),
            "WRITE" | "ADMIN" | "MAINTAIN"
        ),
        _ => false,
    }
}
fn permissions(repo: &Value, pr: &Value) -> Value {
    let write = can_write(repo);
    let update = pr["viewerCanUpdate"] != false;
    let own = pr["viewerDidAuthor"] == true;
    let branch = pr["state"] == "OPEN"
        && pr["viewerCanUpdateBranch"] == true
        && !pr["baseRef"]["compare"].is_null();
    let mut actions = Vec::new();
    if can_write(&pr["headRepository"]) && !pr["headRef"].is_null() {
        actions.push("delete-branch")
    }
    if write {
        actions.push("merge");
        if repo["autoMergeAllowed"] == true
            && !matches!(
                pr["mergeStateStatus"].as_str(),
                Some("CLEAN" | "HAS_HOOKS" | "UNSTABLE")
            )
        {
            actions.push("enable-auto-merge")
        }
        actions.extend(["disable-auto-merge", "revert", "approve-workflows"]);
    }
    if update {
        actions.extend(["ready", "draft", "close", "reopen"])
    }
    if branch {
        actions.push("update-branch")
    }
    let mut row = json!({});
    if write {
        row["stackRebase"] = json!(true)
    }
    row["actions"] = json!(actions);
    row["comment"] = json!(true);
    row["resolve"] = json!(write || own);
    row["verdicts"] = if own {
        json!(["comment"])
    } else {
        json!(["comment", "approve", "request-changes"])
    };
    row["requestReviewers"] = json!(write);
    if branch {
        row["updateMethods"] = json!(["merge", "rebase"])
    }
    row["labels"] = json!(write || repo["viewerPermission"] == "TRIAGE");
    row["maintainerEdits"] = json!(
        pr["state"] == "OPEN"
            && pr["isCrossRepository"] == true
            && own
            && pr["headRepositoryOwner"]["__typename"] == "User"
            && !pr["headRepository"].is_null()
            && !pr["headRef"].is_null()
    );
    row
}
pub(super) fn check_rows(contexts: Vec<Value>) -> Vec<Value> {
    let mut kept = IndexMap::new();
    for c in contexts {
        let Some(name) = trim(&c["name"]).or_else(|| trim(&c["context"])) else {
            continue;
        };
        let status = if c["status"]
            .as_str()
            .is_some_and(|s| !s.is_empty() && s != "COMPLETED")
        {
            "pending"
        } else {
            match c["conclusion"].as_str().or_else(|| c["state"].as_str()) {
                Some("SUCCESS") => "success",
                Some("ACTION_REQUIRED") => "action-required",
                Some("FAILURE" | "ERROR" | "TIMED_OUT" | "STARTUP_FAILURE") => "failure",
                Some("CANCELLED") => "cancelled",
                Some("SKIPPED") => "skipped",
                Some("PENDING" | "EXPECTED") => "pending",
                _ => "neutral",
            }
        };
        let at = if status == "pending" {
            Some("9999-12-31T23:59:59Z")
        } else {
            ["completedAt", "startedAt"]
                .into_iter()
                .filter_map(|k| trim(&c[k]))
                .find(|s| *s != "0001-01-01T00:00:00Z")
        };
        let workflow = trim(&c["checkSuite"]["workflowRun"]["workflow"]["name"]).unwrap_or("");
        let key = (workflow.to_owned(), name.to_owned());
        let when = at.unwrap_or("").to_owned();
        let mut row = json!({"name":name,"status":status,"description":trim(&c["description"]),"url":trim(&c["detailsUrl"]).or_else(||trim(&c["targetUrl"]))});
        if let Some(r) = c["isRequired"].as_bool() {
            row["required"] = json!(r)
        }
        if kept.get(&key).is_none_or(|(prev, _)| &when >= prev) {
            kept.insert(key, (when, row));
        }
    }
    let mut counts = HashMap::new();
    for (_, name) in kept.keys() {
        *counts.entry(name.clone()).or_insert(0usize) += 1
    }
    let mut result = kept
        .into_iter()
        .map(|((w, name), (_, mut row))| {
            if !w.is_empty() && counts[&name] > 1 {
                row["name"] = json!(format!("{w} / {name}"))
            }
            row
        })
        .collect::<Vec<_>>();
    result.sort_by_key(|v| !matches!(v["status"].as_str(), Some("failure" | "cancelled")));
    result
}
/// The provider's change request read, including subsequent pages of check contexts.
pub(super) async fn read_core(
    gh: &Github<'_>,
    op: &str,
    named: &str,
    number: u64,
) -> Result<(Value, Vec<Value>), Value> {
    let (owner, name) = named.split_once('/').unwrap();
    let vars = json!({"owner":owner,"name":name,"number":number,"headRef":format!("refs/pull/{number}/head"),"required":true,"requestedReviewers":100,"mergeQueue":true});
    let data = gh
        .graphql(include_str!("PullRequestCore.graphql"), vars)
        .await
        .map_err(|e| failure(op, e))?;
    let repo = &data["repository"];
    let pr = &repo["pullRequest"];
    if pr.is_null() {
        return Err(failure(op, Failure::NotFound));
    }
    let mut page = pr["commits"]["nodes"][0]["commit"]["statusCheckRollup"]["contexts"].clone();
    let mut contexts = Vec::new();
    let mut seen = HashSet::new();
    loop {
        contexts.extend(nodes(&page).cloned());
        if page["pageInfo"]["hasNextPage"] != true {
            break;
        }
        let after = page["pageInfo"]["endCursor"]
            .as_str()
            .ok_or_else(|| failure(op, Failure::Invalid))?;
        if !seen.insert(after.to_owned()) {
            return Err(error(op, "GitHub did not return complete check contexts."));
        }
        let next = gh
            .graphql(
                include_str!("PullRequestCheckContexts.graphql"),
                json!({"owner":owner,"name":name,"number":number,"required":true,"after":after}),
            )
            .await
            .map_err(|e| failure(op, e))?;
        let next_pr = &next["repository"]["pullRequest"];
        if next_pr["headRefOid"] != pr["headRefOid"] {
            break;
        }
        page = next_pr["commits"]["nodes"][0]["commit"]["statusCheckRollup"]["contexts"].clone();
    }
    Ok((data, contexts))
}

pub(super) async fn read(
    gh: &Github<'_>,
    p: &Project,
    op: &str,
    named: &str,
    number: u64,
) -> Reply {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;
    let (data, contexts) = read_core(gh, op, named, number).await?;
    let repo = &data["repository"];
    let pr = &repo["pullRequest"];
    let state = match pr["state"].as_str() {
        Some("MERGED") => "merged",
        Some("CLOSED") => "closed",
        Some("OPEN") => "open",
        _ => return Err(failure(op, Failure::Invalid)),
    };
    let checks = check_rows(contexts);
    if op == "checks" {
        return Ok(json!({"observedAt":now,"state":state,"checks":checks}));
    }
    let a = &pr["author"];
    let author = trim(&a["login"]).map(|l| {
        let mut actor =
            json!({"login":l,"name":trim(&a["name"]),"avatarUrl":trim(&a["avatarUrl"])});
        if actor["avatarUrl"].is_null()
            && l.len() <= 39
            && l.as_bytes().first().is_some_and(u8::is_ascii_alphanumeric)
            && l.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-')
        {
            actor["avatarUrl"] = json!(format!("https://github.com/{l}.png?size=80"))
        }
        if a["__typename"] == "Bot" {
            actor["isBot"] = json!(true)
        }
        actor
    });
    let mut reviewers = Vec::new();
    let mut teams = Vec::new();
    for r in nodes(&pr["reviewRequests"]).map(|r| &r["requestedReviewer"]) {
        if let Some(login) = trim(&r["login"]) {
            reviewers.push(json!({"login":login,"name":null,"avatarUrl":null}))
        }
        if let Some(login) = trim(&r["combinedSlug"]) {
            teams.push(json!({"login":login,"name":trim(&r["teamName"]),"avatarUrl":trim(&r["teamAvatarUrl"])}))
        }
    }
    reviewers.extend(teams);
    let labels=nodes(&pr["labels"]).filter_map(|l|Some(json!({"name":trim(&l["name"])?,"color":trim(&l["color"]),"description":trim(&l["description"])}))).collect::<Vec<_>>();
    let mut result = json!({"provider":"github","capabilities":capabilities(),"viewerPermissions":permissions(repo,pr),"projectId":p.id,"projectTitle":p.name,"workspaceRoot":p.path,"repository":named,"number":number,"title":pr["title"],"body":pr["body"].as_str().unwrap_or(""),"url":pr["url"],"author":author,"state":state,"isDraft":pr["isDraft"],"mergeability":match pr["mergeable"].as_str(){Some("MERGEABLE")=>"mergeable",Some("CONFLICTING")=>"conflicting",_=>"unknown"},"additions":pr["additions"],"deletions":pr["deletions"],"changedFiles":pr["changedFiles"],"headBranch":pr["headRefName"],"headRepositoryNameWithOwner":trim(&pr["headRepository"]["nameWithOwner"])});
    if let Some(sha) = trim(&pr["headRefOid"]) {
        result["headSha"] = json!(sha)
    }
    let more = json!({"baseBranch":pr["baseRefName"],"createdAt":pr["createdAt"],"updatedAt":pr["updatedAt"],"observedAt":now,"mergedAt":trim(&pr["mergedAt"]),"closedAt":trim(&pr["closedAt"]),"reviewers":reviewers,"labels":labels,"checks":checks,"mergeCapabilities":{"merge":repo["mergeCommitAllowed"],"squash":repo["squashMergeAllowed"],"rebase":repo["rebaseMergeAllowed"]}});
    result
        .as_object_mut()
        .unwrap()
        .extend(more.as_object().unwrap().clone());
    if let Ok(identity) = identity(gh).await {
        result["viewer"] = identity["viewer"].clone()
    }
    let behind = if state == "open" {
        pr["baseRef"]["compare"]["behindBy"].as_u64()
    } else {
        None
    };
    result["baseComparison"] = json!(match behind {
        Some(0) => "up-to-date",
        Some(_) => "behind",
        None => "unknown",
    });
    if let Some(n) = behind {
        result["behindBy"] = json!(n)
    }
    if let Some(req) = pr.get("autoMergeRequest") {
        result["autoMergeEnabled"] = json!(!req.is_null());
        if let Some(method) = req["mergeMethod"].as_str() {
            result["autoMergeMethod"] = json!(method.to_lowercase())
        }
    }
    if state != "open" || pr["isCrossRepository"] != true {
        result["workflowApprovalsRequired"] = json!(0)
    }
    if let Some(rem) = nodes(&pr["mergeQueueRemovals"])
        .last()
        .filter(|r| r["id"].is_string())
    {
        result["mergeQueueRemoval"] =
            json!({"id":rem["id"],"removedAt":rem["createdAt"],"reason":trim(&rem["reason"])})
    }
    if let Some(flag) = pr["maintainerCanModify"].as_bool() {
        result["maintainerCanModify"] = json!(flag)
    }
    Ok(result)
}

pub(super) fn select(checks: &[Value], input: &Value, op: &str) -> Result<Vec<Value>, Value> {
    let wanted = input["check"].as_str();
    let selected: Vec<_> = checks
        .iter()
        .filter(|c| match wanted {
            Some(name) => c["name"]
                .as_str()
                .is_some_and(|n| n.to_lowercase() == name.trim().to_lowercase()),
            None => matches!(c["status"].as_str(), Some("failure" | "cancelled")),
        })
        .cloned()
        .collect();
    if let Some(name) = wanted.filter(|_| selected.is_empty()) {
        let available = checks
            .iter()
            .filter_map(|c| c["name"].as_str())
            .collect::<Vec<_>>()
            .join(", ");
        return Err(error(
            op,
            &format!(
                "No check is named {name}. Checks: {}.",
                if available.is_empty() {
                    "none"
                } else {
                    &available
                }
            ),
        ));
    }
    Ok(selected)
}

/// Read the tail of selected GitHub Actions jobs. Non-Actions checks have no log endpoint and are
/// reported as a per-check detail, matching the provider contract.
pub(super) async fn logs(gh: &Github<'_>, repository: &str, number: u64, input: &Value) -> Reply {
    let checks = read(
        gh,
        &Project {
            id: String::new(),
            path: String::new(),
            name: String::new(),
            created_at: 0,
        },
        "checks",
        repository,
        number,
    )
    .await?;
    let checks = checks["checks"].as_array().cloned().unwrap_or_default();
    let selected = select(&checks, input, "checkLogs")?;
    let tail_lines = input["tailLines"].as_u64().unwrap_or(200).clamp(1, 2000) as usize;
    let mut output = Vec::new();
    for check in selected {
        let name = check["name"].as_str().unwrap_or_default();
        let url = check["url"].as_str().map(str::to_owned);
        let Some(url) = url.as_deref() else {
            output.push(json!({"name":name,"url":null,"log":null,"truncated":false,"detail":"This check is not a GitHub Actions job, so GitHub holds no log for it."}));
            continue;
        };
        let Some(job) = url
            .split("/job/")
            .nth(1)
            .and_then(|s| s.split(['/', '?']).next())
            .filter(|s| !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit()))
        else {
            output.push(json!({"name":name,"url":url,"log":null,"truncated":false,"detail":"This check is not a GitHub Actions job, so GitHub holds no log for it."}));
            continue;
        };
        let (owner, repo) = repository.split_once('/').unwrap();
        match gh.request("GET", &format!("repos/{owner}/{repo}/actions/jobs/{job}/logs"), "application/vnd.github+json", None).await {
            Ok(bytes) => {
                let text = String::from_utf8_lossy(&bytes);
                let lines: Vec<&str> = text.lines().collect();
                let start = lines.len().saturating_sub(tail_lines);
                output.push(json!({"name":name,"url":url,"log":lines[start..].join("\n"),"truncated":start > 0,"detail":null}));
            }
            Err(Failure::NotFound) => output.push(json!({"name":name,"url":url,"log":null,"truncated":false,"detail":"GitHub could not find the check log."})),
            Err(e) => return Err(failure("checkLogs", e)),
        }
    }
    Ok(json!(output))
}
