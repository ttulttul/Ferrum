use super::{error, failure, identity};
use crate::{
    provider_rpc::{github_remote, project, Github, Reply},
    AppState, Project,
};
use serde_json::{json, Value};
use std::{
    collections::{HashMap, HashSet},
    path::Path,
};

pub(super) async fn stats(state: &AppState, cli: &Path, input: &Value) -> Reply {
    let mut seen = HashSet::new();
    let mut result = Vec::new();
    for r in input["refs"].as_array().into_iter().flatten() {
        let Some(p) = project(state, r).await else {
            continue;
        };
        let Some(repo) = github_remote(&p).await else {
            continue;
        };
        if !r["repository"]
            .as_str()
            .is_some_and(|s| repo.eq_ignore_ascii_case(s.trim()))
        {
            continue;
        }
        let Some(n) = r["number"].as_u64() else {
            continue;
        };
        if !seen.insert((p.id.clone(), n)) {
            continue;
        }
        let (owner, name) = repo.split_once('/').unwrap();
        let Ok(raw)=Github::new(cli,&p.path).graphql("query($owner:String!,$name:String!,$number:Int!){repository(owner:$owner,name:$name){pullRequest(number:$number){additions deletions}}}",json!({"owner":owner,"name":name,"number":n})).await else {continue};
        let pr = &raw["repository"]["pullRequest"];
        if let (Some(add), Some(del)) = (pr["additions"].as_u64(), pr["deletions"].as_u64()) {
            result.push(json!({"projectId":p.id,"repository":repo,"number":n,"additions":add,"deletions":del}))
        }
    }
    Ok(json!({"stats":result}))
}
pub(super) struct Cursor {
    pub(super) before: String,
    pub(super) delivered: u64,
    pub(super) seen: Vec<u64>,
    after: Option<String>,
}
pub(super) fn cursor(raw: &str) -> Option<Cursor> {
    let mut fields = raw.split('|');
    let before = fields.next()?;
    if before.len() < 20
        || !before.is_ascii()
        || before.as_bytes().get(10) != Some(&b'T')
        || !before
            .bytes()
            .all(|b| b.is_ascii_digit() || b"-:T.Z+".contains(&b))
    {
        return None;
    }
    let count = |v: &str| {
        if (1..=9).contains(&v.len()) && v.bytes().all(|b| b.is_ascii_digit()) {
            v.parse().ok()
        } else {
            None
        }
    };
    let delivered = count(fields.next()?)?;
    let seen = fields.next()?;
    let seen = if seen.is_empty() {
        vec![]
    } else {
        seen.split(',').map(count).collect::<Option<Vec<u64>>>()?
    };
    let after = fields.next();
    if after.is_some_and(|s| {
        s.is_empty()
            || s.len() > 128
            || !s
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"_.:~+-".contains(&b))
    }) || fields.next().is_some()
    {
        return None;
    }
    Some(Cursor {
        before: before.into(),
        delivered,
        seen,
        after: after.map(str::to_owned),
    })
}
fn query(input: &Value, repo: &str, viewer: &str, cursor: Option<&Cursor>) -> String {
    let mut q = vec!["is:pr".into()];
    match input["state"].as_str() {
        Some("open") => q.push("is:open".into()),
        Some("merged") => q.push("is:merged".into()),
        Some("closed") => {
            q.push("is:closed".into());
            q.push("-is:merged".into())
        }
        _ => (),
    }
    match input["involvement"].as_str() {
        Some("authored") => q.push(format!("author:{viewer}")),
        Some("reviewing") => q.push(format!("review-requested:{viewer}")),
        _ => (),
    }
    let quote = |s: &str| format!("\"{}\"", s.replace('"', "").trim());
    if let Some(text) = input["query"].as_str() {
        q.push(quote(text))
    }
    if let Some(c) = cursor {
        q.push(format!("updated:<={}", c.before))
    }
    let filters = &input["filters"];
    for group in filters["labels"].as_array().into_iter().flatten() {
        let names = group
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(Value::as_str)
            .map(quote)
            .collect::<Vec<_>>();
        if !names.is_empty() {
            q.push(format!("label:{}", names.join(",")))
        }
    }
    for label in filters["excludedLabels"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
    {
        q.push(format!("-label:{}", quote(label)))
    }
    if let Some(a) = filters["author"].as_str() {
        q.push(format!(
            "author:{}",
            quote(if a == "@me" { viewer } else { a })
        ))
    }
    if let Some(d) = filters["draft"].as_str() {
        q.push(format!("draft:{}", d == "only"))
    }
    if let Some(r) = filters["review"].as_str() {
        q.push(format!("review:{}", r.replace('-', "_")))
    }
    if let Some(c) = filters["checks"].as_str() {
        q.push(format!(
            "status:{}",
            if c == "passing" { "success" } else { "failure" }
        ))
    }
    q.push("sort:updated-desc".into());
    q.push(format!("repo:{repo}"));
    q.join(" ")
}

fn row(p: &Project, repo: &str, raw: &Value, viewer: &str, at: u64) -> Option<Value> {
    let n = raw["number"].as_u64()?;
    let mut row = json!({});
    if !raw["stack"].is_null() && !raw["stackEntry"].is_null() {
        let s = &raw["stack"];
        row["stack"] = json!({"number":s["number"],"position":raw["stackEntry"]["position"],"size":s["size"],"base":s["baseRefName"]})
    }
    let a = &raw["author"];
    let author = a["login"].as_str().map(|login| {
        let mut author = json!({"login":login,"name":a["name"],"avatarUrl":a["avatarUrl"]});
        if a["__typename"] == "Bot" {
            author["isBot"] = json!(true)
        }
        author
    });
    let requested = !a["login"]
        .as_str()
        .is_some_and(|a| a.eq_ignore_ascii_case(viewer))
        && raw["reviewRequests"]["nodes"]
            .as_array()
            .into_iter()
            .flatten()
            .any(|r| {
                r["requestedReviewer"]["login"]
                    .as_str()
                    .is_some_and(|l| l.eq_ignore_ascii_case(viewer))
            });
    let labels = raw["labels"]["nodes"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|l| l["name"].is_string())
        .map(|l| json!({"name":l["name"],"color":l["color"],"description":l["description"]}))
        .collect::<Vec<_>>();
    let state = match raw["state"].as_str()? {
        "MERGED" => "merged",
        "CLOSED" => "closed",
        "OPEN" => "open",
        _ => return None,
    };
    let fields = json!({"provider":"github","host":"github.com","projectId":p.id,"projectTitle":p.name,"repository":repo,"number":n,"title":raw["title"],"url":raw["url"],"author":author,"headBranch":raw["headRefName"],"baseBranch":raw["baseRefName"],"state":state,"isDraft":raw["isDraft"],"mergeability":match raw["mergeable"].as_str(){Some("MERGEABLE")=>"mergeable",Some("CONFLICTING")=>"conflicting",_=>"unknown"},"additions":raw["additions"].as_u64().unwrap_or(0),"deletions":raw["deletions"].as_u64().unwrap_or(0),"createdAt":raw["createdAt"],"updatedAt":raw["updatedAt"],"observedAt":at,"viewerReviewRequested":requested,"labels":labels});
    row.as_object_mut()
        .unwrap()
        .extend(fields.as_object()?.clone());
    let reviews = raw["latestReviews"]["nodes"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|r| r["isMinimized"] != true)
        .filter_map(|r| r["state"].as_str())
        .collect::<Vec<_>>();
    let decision = match raw["reviewDecision"].as_str() {
        Some("APPROVED") => Some("approved"),
        Some("CHANGES_REQUESTED") => Some("changes-requested"),
        _ if reviews.contains(&"CHANGES_REQUESTED") => Some("changes-requested"),
        Some("REVIEW_REQUIRED") => Some("review-required"),
        _ if reviews.contains(&"APPROVED") => Some("approved"),
        _ => None,
    };
    if let Some(s) = decision {
        row["reviewDecision"] = json!(s)
    }
    if let Some(check) =
        match raw["commits"]["nodes"][0]["commit"]["statusCheckRollup"]["state"].as_str() {
            Some("SUCCESS") => Some("passing"),
            Some("FAILURE" | "ERROR") => Some("failing"),
            Some("PENDING" | "EXPECTED") => Some("pending"),
            _ => None,
        }
    {
        row["checksState"] = json!(check)
    }
    Some(row)
}
use super::bitbucket::{
    self, Viewer as BitbucketViewer, UNAUTHENTICATED as BITBUCKET_UNAUTHENTICATED,
};

/// The host and repository a Bitbucket project's identity names.
fn bitbucket_repository(project: &Project) -> Option<(String, String)> {
    let identity = crate::repository_identity::resolve(&project.path)?;
    if identity["provider"] != "bitbucket" {
        return None;
    }
    let host = identity["canonicalKey"]
        .as_str()?
        .split('/')
        .next()?
        .to_owned();
    let repository = identity["displayName"].as_str()?.to_owned();
    Some((host, repository))
}

/// Provider summaries, and per-project errors when the viewer could not be read (`detail`).
fn bitbucket_unreadable(
    projects: &[(Project, String, String)],
    input: &Value,
    detail: Option<&str>,
    has_cursor: impl Fn(&str) -> bool,
) -> (Vec<Value>, Vec<Value>) {
    let mut providers: Vec<Value> = Vec::new();
    let mut errors = Vec::new();
    for (project, host, repository) in projects {
        match providers.iter_mut().find(|p| p["host"] == host.as_str()) {
            Some(provider) => {
                provider["projectCount"] = json!(provider["projectCount"].as_u64().unwrap_or(0) + 1)
            }
            None => providers.push(json!({"host":host,"kind":"bitbucket","searchesOnHost":true,
                "actions":["merge","close"],"projectCount":1,"projectIds":null,"configured":detail.is_none(),"detail":detail})),
        }
        if detail.is_some()
            && (input.get("cursors").is_none()
                || has_cursor(&format!("{host} {}", repository.to_lowercase())))
        {
            errors.push(json!({"projectId":project.id,"projectTitle":project.name,
                "message":format!("{repository} could not be read.")}));
        }
    }
    (providers, errors)
}

fn extend(target: &mut Value, values: &[Value]) {
    if let Some(array) = target.as_array_mut() {
        array.extend(values.iter().cloned());
    }
}

pub(super) async fn list(state: &AppState, cli: &Path, input: &Value) -> Reply {
    let mut cursors = HashMap::new();
    if let Some(map) = input["cursors"].as_object() {
        for (key, v) in map {
            cursors.insert(
                key,
                v.as_str().and_then(cursor).ok_or_else(|| {
                    error(
                        "list",
                        "The list could not be carried on from where it left off.",
                    )
                })?,
            );
        }
    }
    let mut projects = state
        .store
        .read()
        .await
        .projects
        .values()
        .cloned()
        .collect::<Vec<_>>();
    projects.sort_by_key(|p| (p.created_at, p.id.clone()));
    let mut repos = Vec::new();
    let mut bitbucket = Vec::new();
    for p in projects {
        if input["projectId"].as_str().is_some_and(|id| p.id != id)
            || input["projectIds"]
                .as_array()
                .is_some_and(|ids| !ids.iter().any(|id| id == &p.id))
        {
            continue;
        }
        if let Some((host, repo)) = bitbucket_repository(&p) {
            if input["host"]
                .as_str()
                .is_none_or(|h| h.trim().eq_ignore_ascii_case(&host))
            {
                bitbucket.push((p, host, repo));
            }
            continue;
        }
        if input["host"]
            .as_str()
            .is_some_and(|h| !h.eq_ignore_ascii_case("github.com"))
        {
            continue;
        }
        if let Some(repo) = github_remote(&p).await {
            repos.push((p, repo))
        }
    }
    let mut out = json!({"viewers":{},"providers":[],"entries":[],"errors":[],"truncated":false,"nextCursors":{}});
    let viewer = if bitbucket.is_empty() {
        BitbucketViewer::Unauthenticated
    } else {
        bitbucket::viewer().await
    };
    let detail = match &viewer {
        BitbucketViewer::Known(_) => None,
        BitbucketViewer::Unauthenticated => Some(BITBUCKET_UNAUTHENTICATED),
        BitbucketViewer::Failed { detail, .. } => Some(detail.as_str()),
    };
    let (bitbucket_providers, mut bitbucket_errors) =
        bitbucket_unreadable(&bitbucket, input, detail, |key| {
            cursors.contains_key(&key.to_owned())
        });
    // With the viewer known, each Bitbucket repository is read (`readRepository`).
    let mut bitbucket_entries = Vec::new();
    let mut bitbucket_truncated = false;
    let mut bitbucket_cursors = serde_json::Map::new();
    if let BitbucketViewer::Known(name) = &viewer {
        let at = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        let limit = input["limit"].as_u64().unwrap_or(99).clamp(1, 500) as usize;
        let mut seen = HashSet::new();
        for (p, host, repo) in &bitbucket {
            let key = format!("{host} {}", repo.to_lowercase());
            if !seen.insert(key.clone()) {
                continue;
            }
            let prev = cursors.get(&key);
            if input.get("cursors").is_some() && prev.is_none() {
                continue;
            }
            let target = bitbucket::Target {
                host,
                repository: repo,
                project_id: &p.id,
                project_title: &p.name,
                input,
                viewer: name,
                limit,
                observed_at: at,
            };
            let batch = bitbucket::read_repository(&target, prev).await;
            if batch.failed {
                bitbucket_errors.push(json!({"projectId":p.id,"projectTitle":p.name,
                    "message":format!("{repo} could not be read.")}));
                continue;
            }
            bitbucket_truncated |= batch.truncated;
            if let Some(next) = batch.next_cursor {
                bitbucket_cursors.insert(key, json!(next));
            }
            bitbucket_entries.extend(batch.entries);
        }
        out["viewers"][bitbucket[0].1.as_str()] = json!(name);
    }
    let Some((first, _)) = repos.first() else {
        out["providers"] = json!(bitbucket_providers);
        out["errors"] = json!(bitbucket_errors);
        if matches!(viewer, BitbucketViewer::Known(_)) {
            bitbucket_entries.sort_by(|a, b| b["updatedAt"].as_str().cmp(&a["updatedAt"].as_str()));
            out["entries"] = json!(bitbucket_entries);
            out["truncated"] = json!(bitbucket_truncated);
            out["nextCursors"] = Value::Object(bitbucket_cursors);
            return Ok(out);
        }
        if bitbucket_errors.is_empty() {
            return Ok(out);
        }
        return Err(match viewer {
            BitbucketViewer::Failed { detail, not_found } => {
                let mut error = json!({"_tag":"PullRequestOperationError","operation":"list",
                    "detail":detail,"listResult":out});
                if not_found {
                    error["reason"] = json!("not-found");
                }
                error
            }
            _ => json!({"_tag":"PullRequestUnavailableError","reason":"cli-unauthenticated",
                "provider":"bitbucket","requirement":BITBUCKET_UNAUTHENTICATED,"listResult":out}),
        });
    };
    let actions = [
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
    let mut provider = json!({"host":"github.com","kind":"github","searchesOnHost":true,"actions":actions,"projectCount":repos.len(),"configured":true,"detail":null});
    let identity = identity(&Github::new(cli, &first.path)).await;
    let id = match identity {
        Ok(i) => i,
        Err(mut err) => {
            provider["configured"] = json!(false);
            provider["detail"] = err.get("requirement").unwrap_or(&err["detail"]).clone();
            out["providers"] = json!([provider]);
            extend(&mut out["providers"], &bitbucket_providers);
            let mut errors = Vec::new();
            for (p, repo) in &repos {
                if input.get("cursors").is_none()
                    || cursors.contains_key(&format!("github.com {}", repo.to_lowercase()))
                {
                    errors.push(json!({"projectId":p.id,"projectTitle":p.name,"message":format!("{repo} could not be read.")}))
                }
            }
            errors.extend(bitbucket_errors.iter().cloned());
            out["errors"] = json!(errors);
            // Another host's readable repositories still list (`readable` is not empty).
            if matches!(viewer, BitbucketViewer::Known(_)) {
                bitbucket_entries
                    .sort_by(|a, b| b["updatedAt"].as_str().cmp(&a["updatedAt"].as_str()));
                out["entries"] = json!(bitbucket_entries);
                out["truncated"] = json!(bitbucket_truncated);
                out["nextCursors"] = Value::Object(bitbucket_cursors);
                return Ok(out);
            }
            if !out["errors"].as_array().unwrap().is_empty() {
                err["listResult"] = out;
                return Err(err);
            }
            return Ok(out);
        }
    };
    let viewer = id["viewer"].as_str().unwrap();
    out["viewers"]["github.com"] = json!(viewer);
    out["providers"] = json!([provider]);
    extend(&mut out["providers"], &bitbucket_providers);
    extend(&mut out["errors"], &bitbucket_errors);
    let mut seen = HashSet::new();
    let mut entries = Vec::new();
    for (p, repo) in repos {
        let key = format!("github.com {}", repo.to_lowercase());
        if !seen.insert(key.clone()) {
            continue;
        }
        let prev = cursors.get(&key);
        if input.get("cursors").is_some() && prev.is_none() {
            continue;
        }
        let limit = input["limit"].as_u64().unwrap_or(99).clamp(1, 500) as usize;
        let gh = Github::new(cli, &p.path);
        let mut after = prev.and_then(|c| c.after.clone());
        let mut rows = Vec::new();
        let mut visited = HashSet::new();
        let at = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        let search = query(input, &repo, viewer, prev);
        let more = loop {
            let args = json!({"q":search,"first":(limit-rows.len()).min(100),"after":after,"stacks":true,"stats":false});
            let page = match gh
                .graphql(include_str!("PullRequestSearch.graphql"), args)
                .await
            {
                Ok(v) => v,
                Err(e) => {
                    out["errors"].as_array_mut().unwrap().push(json!({"projectId":p.id,"projectTitle":p.name,"message":failure("list",e)["detail"]}));
                    break false;
                }
            };
            for raw in page["search"]["nodes"].as_array().into_iter().flatten() {
                if prev.is_some_and(|c| {
                    raw["updatedAt"] == c.before
                        && raw["number"].as_u64().is_some_and(|n| c.seen.contains(&n))
                }) {
                    continue;
                }
                if let Some(v) = row(&p, &repo, raw, viewer, at) {
                    rows.push(v)
                }
            }
            if page["search"]["pageInfo"]["hasNextPage"] != true {
                break false;
            }
            if rows.len() >= limit {
                break true;
            }
            let Some(c) = page["search"]["pageInfo"]["endCursor"].as_str() else {
                break false;
            };
            if !visited.insert(c.to_owned()) {
                break false;
            }
            after = Some(c.into());
        };
        if more {
            if let Some(boundary) = rows.iter().filter_map(|r| r["updatedAt"].as_str()).min() {
                let mut numbers = prev
                    .filter(|c| c.before == boundary)
                    .map(|c| c.seen.clone())
                    .unwrap_or_default();
                for n in rows
                    .iter()
                    .filter(|r| r["updatedAt"] == boundary)
                    .filter_map(|r| r["number"].as_u64())
                {
                    if !numbers.contains(&n) {
                        numbers.push(n)
                    }
                }
                let count = prev.map(|c| c.delivered).unwrap_or(0) + rows.len() as u64;
                out["nextCursors"][&key] = json!(format!(
                    "{boundary}|{count}|{}",
                    numbers
                        .iter()
                        .map(ToString::to_string)
                        .collect::<Vec<_>>()
                        .join(",")
                ));
            }
            out["truncated"] = json!(true);
        }
        entries.extend(rows);
    }
    entries.extend(bitbucket_entries);
    if bitbucket_truncated {
        out["truncated"] = json!(true);
    }
    for (key, next) in bitbucket_cursors {
        out["nextCursors"][&key] = next;
    }
    entries.sort_by(|a, b| b["updatedAt"].as_str().cmp(&a["updatedAt"].as_str()));
    out["entries"] = json!(entries);
    Ok(out)
}
