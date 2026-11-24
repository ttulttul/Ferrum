use crate::{
    provider_rpc::{github_remote, project, valid_repository, Failure, Github, Reply},
    AppState,
};
use serde_json::{json, Value};
use std::path::Path;

mod actions;
mod activity;
pub(crate) mod bitbucket;
mod bitbucket_pr;
mod detail;
mod discussion;
mod files;
mod listing;
mod review;
pub(crate) fn handles(method: &str) -> bool {
    let Some(op) = method.strip_prefix("pullRequests.") else {
        return false;
    };
    matches!(
        op,
        "detail"
            | "runAction"
            | "filesViewed"
            | "setFilesViewed"
            | "changeBreakdown"
            | "stack"
            | "threadComments"
            | "checks"
            | "list"
            | "listStats"
            | "preview"
            | "summary"
            | "routing"
            | "routingIdentity"
            | "diffFileContents"
            | "activity"
            | "checkLogs"
    ) || review::handles(op)
}

type SummaryKey = (String, String, u64, String);
type HeldSummaries = indexmap::IndexMap<SummaryKey, (std::time::Instant, Value)>;

#[derive(Default, Debug)]
pub struct PullRequestService {
    summaries: tokio::sync::Mutex<HeldSummaries>,
}
impl PullRequestService {
    async fn clear(&self, project: &str, repository: &str, number: u64) {
        self.summaries.lock().await.retain(|(p, r, n, _), _| {
            p != project || !r.eq_ignore_ascii_case(repository) || *n != number
        });
    }
    async fn remember(&self, key: SummaryKey, next: &Value, from_detail: bool) {
        let mut cache = self.summaries.lock().await;
        if cache
            .get(&key)
            .is_some_and(|(_, last)| last["observedAt"].as_u64() > next["observedAt"].as_u64())
        {
            return;
        }
        let summary = if from_detail {
            let mut summary = json!({});
            let prior = cache.get(&key).map(|(_, value)| value);
            for field in [
                "provider",
                "projectId",
                "repository",
                "number",
                "title",
                "url",
                "state",
                "isDraft",
                "headBranch",
                "baseBranch",
                "closedAt",
                "mergedAt",
                "updatedAt",
                "observedAt",
                "author",
                "additions",
                "deletions",
                "changedFiles",
                "reviewDecision",
                "checksState",
                "mergeability",
                "rebaseRequired",
                "stack",
                "headSha",
                "autoMergeEnabled",
                "autoMergeMethod",
            ] {
                let value = if matches!(
                    field,
                    "stack"
                        | "headSha"
                        | "autoMergeEnabled"
                        | "autoMergeMethod"
                        | "checksState"
                        | "reviewDecision"
                ) {
                    prior.and_then(|s| s.get(field))
                } else {
                    next.get(field)
                };
                if let Some(v) = value {
                    summary[field] = v.clone()
                }
            }
            summary
        } else {
            next.clone()
        };
        if cache.len() >= 256 && !cache.contains_key(&key) {
            cache.shift_remove_index(0);
        }
        cache.insert(key, (std::time::Instant::now(), summary));
    }
}

fn error(op: &str, detail: &str) -> Value {
    json!({"_tag":"PullRequestOperationError","operation":op,"detail":detail})
}
fn unavailable() -> Value {
    json!({"_tag":"PullRequestUnavailableError","reason":"provider-unsupported"})
}
fn failure(op: &str, f: Failure) -> Value {
    match f {
        Failure::Auth | Failure::Missing => {
            json!({"_tag":"PullRequestUnavailableError","reason":if f==Failure::Auth{"cli-unauthenticated"}else{"cli-missing"},"provider":"github","requirement":if f==Failure::Missing{"No GitHub credential on the server. Set GH_TOKEN, or install the GitHub CLI (https://cli.github.com/) and run `gh auth login`."}else{"GitHub has no working credential for this host. Run `gh auth login`, or check the account and hosts in Settings → Source control & issues."}})
        }
        Failure::NotFound => {
            json!({"_tag":"PullRequestOperationError","operation":op,"detail":"GitHub could not find the requested resource, or the credential cannot see it.","reason":"not-found"})
        }
        Failure::RateLimit => {
            json!({"_tag":"PullRequestOperationError","operation":op,"detail":"GitHub API rate limit exceeded."})
        }
        _ => error(op, "The GitHub pull request could not be read."),
    }
}

fn valid_sha(v: &str) -> bool {
    (7..=64).contains(&v.len()) && v.bytes().all(|b| b.is_ascii_hexdigit())
}

/// One pull request of a Bitbucket project (`bitbucket_pr`), its cached summaries cleared
/// after a write.
async fn bitbucket_read(
    state: &AppState,
    p: &crate::Project,
    host: &str,
    own: &str,
    op: &str,
    input: &Value,
) -> Reply {
    let named = input["repository"].as_str().unwrap_or("").trim();
    if input["host"]
        .as_str()
        .is_some_and(|h| !h.trim().eq_ignore_ascii_case(host))
    {
        return Err(unavailable());
    }
    if !named.eq_ignore_ascii_case(own) {
        return Err(error(
            "resolveRepository",
            "The change request does not belong to the selected project.",
        ));
    }
    let number = input["number"]
        .as_u64()
        .filter(|n| *n > 0)
        .ok_or_else(|| error(op, "A pull request number is required."))?;
    let repository = bitbucket_pr::Repository {
        repository: named,
        host,
        project: p,
    };
    let result = bitbucket_pr::run(repository, op, input, number).await;
    if !matches!(
        op,
        "detail"
            | "preview"
            | "checks"
            | "activity"
            | "reviewerCandidates"
            | "diff"
            | "diffFileContents"
            | "changeBreakdown"
    ) {
        state.pull_requests.clear(&p.id, named, number).await;
    }
    result
}

pub(crate) async fn read(state: &AppState, cli: &Path, op: &str, input: &Value) -> Reply {
    if op == "list" {
        return listing::list(state, cli, input).await;
    }
    if op == "listStats" {
        return listing::stats(state, cli, input).await;
    }
    if op == "routingIdentity" {
        if !input["host"]
            .as_str()
            .is_some_and(|host| host.eq_ignore_ascii_case("github.com"))
        {
            return Err(unavailable());
        }
        let mut candidates = state
            .store
            .read()
            .await
            .projects
            .values()
            .cloned()
            .collect::<Vec<_>>();
        candidates.sort_by_key(|p| (p.created_at, p.id.clone()));
        for p in candidates {
            if github_remote(&p).await.is_some() {
                return identity(&Github::new(cli, &p.path)).await;
            }
        }
        return Err(unavailable());
    }
    let p = project(state, input).await.ok_or_else(unavailable)?;
    if let Some((host, own)) = listing::bitbucket_repository(&p) {
        return bitbucket_read(state, &p, &host, &own, op, input).await;
    }
    let named = input["repository"].as_str().unwrap_or("").trim();
    let remote = github_remote(&p).await;
    if input["host"]
        .as_str()
        .is_some_and(|h| !h.trim().eq_ignore_ascii_case("github.com"))
    {
        return Err(unavailable());
    }
    if input["host"].is_null() {
        let own = remote.ok_or_else(unavailable)?;
        if !named.eq_ignore_ascii_case(&own) {
            return Err(error(
                "resolveRepository",
                "The change request does not belong to the selected project.",
            ));
        }
    }
    if !valid_repository(named) {
        return Err(error(
            op,
            "A repository was named that GitHub cannot address.",
        ));
    }
    if matches!(op, "comment" | "updateComment" | "replyToThread")
        && input["body"].as_str().is_none_or(|s| s.trim().is_empty())
    {
        return Err(error(
            op,
            if op == "replyToThread" {
                "A reply cannot be empty."
            } else {
                "A comment cannot be empty."
            },
        ));
    }
    let mut gh = Github::new(cli, &p.path);
    if op == "routing" {
        let mut response = identity(&gh).await?;
        response["projectTitle"] = json!(p.name);
        response["workspaceRoot"] = json!(p.path);
        return Ok(response);
    }
    if let Some(account) = input["expectedAccountId"].as_str() {
        let rejected = || {
            error(
                "routeIdentity",
                "The signed-in account could not be verified before starting the operation.",
            )
        };
        if !input["host"]
            .as_str()
            .is_some_and(|h| h.eq_ignore_ascii_case("github.com"))
        {
            return Err(rejected());
        }
        gh.pin_credential().await.map_err(|_| rejected())?;
        if identity(&gh).await.map_err(|_| rejected())?["accountId"] != account {
            return Err(rejected());
        }
    }
    let commit = input["commit"].as_str();
    if commit.is_some_and(|sha| !valid_sha(sha)) {
        return Err(error(op, "The named commit was not a commit sha."));
    }
    let number = input["number"]
        .as_u64()
        .filter(|n| *n > 0)
        .ok_or_else(|| error(op, "A pull request number is required."))?;
    if matches!(op, "summary" | "detail") {
        let viewer = identity(&gh)
            .await
            .ok()
            .and_then(|v| v["accountId"].as_str().map(str::to_owned));
        let key = viewer.map(|v| (p.id.clone(), named.to_ascii_lowercase(), number, v));
        if op == "summary" && input["allowStale"] != false {
            if let Some(k) = &key {
                if let Some((when, cached)) = state.pull_requests.summaries.lock().await.get(k) {
                    if when.elapsed() < std::time::Duration::from_secs(15) {
                        return Ok(cached.clone());
                    }
                }
            }
        }
        let result = if op == "detail" {
            detail::read(&gh, &p, op, named, number).await?
        } else {
            metadata(&gh, op, named, &p.id, number).await?
        };
        if let Some(k) = key {
            state
                .pull_requests
                .remember(k, &result, op == "detail")
                .await;
        }
        return Ok(result);
    }
    if op == "stack" {
        return discussion::stack(&gh, named, number).await;
    }
    if op == "threadComments" {
        return discussion::comments(&gh, named, number, input).await;
    }
    if matches!(op, "filesViewed" | "setFilesViewed" | "changeBreakdown") {
        return files::run(&gh, named, number, op, input).await;
    }
    if op == "runAction" {
        state.pull_requests.clear(&p.id, named, number).await;
        let result = actions::run(&mut gh, &p.id, named, number, input).await;
        state.pull_requests.clear(&p.id, named, number).await;
        return result;
    }
    if op == "checks" {
        return detail::read(&gh, &p, op, named, number).await;
    }
    if op == "activity" {
        return activity::read(&gh, named, number).await;
    }
    if op == "checkLogs" {
        return detail::logs(&gh, named, number, input).await;
    }
    if review::handles(op) {
        let result = review::run(&gh, op, named, number, input).await;
        if !matches!(op, "labelCandidates" | "reviewerCandidates") {
            state.pull_requests.clear(&p.id, named, number).await;
        }
        return result;
    }
    if op == "preview" {
        return metadata(&gh, op, named, &p.id.to_string(), number).await;
    }
    let repo = format!("repos/{named}");
    let target = match commit {
        Some(sha) => format!("{repo}/commits/{sha}"),
        None => format!("{repo}/pulls/{number}"),
    };
    let files = if commit.is_some() {
        target.clone()
    } else {
        format!("{target}/files")
    };
    if op == "diffFileContents" {
        return contents(&gh, input, &repo, &target, &files).await;
    }
    let page = |page, readable| files_page(&gh, &files, page, commit.is_some(), readable);
    if let Some(path) = input["path"].as_str() {
        let data = gh
            .request("GET", &target, "application/vnd.github.diff", None)
            .await
            .map_err(|f| failure(op, f))?;
        if data.len() > 32 * 1024 * 1024 {
            return Err(error(
                op,
                &format!("The diff for {path} is too large to load."),
            ));
        }
        let patch = String::from_utf8_lossy(&data);
        let mut positions = patch
            .match_indices("diff --git ")
            .filter_map(|(i, _)| (i == 0 || patch.as_bytes()[i - 1] == b'\n').then_some(i))
            .collect::<Vec<_>>();
        positions.push(patch.len());
        for pair in positions.windows(2) {
            let section = &patch[pair[0]..pair[1]];
            let headers = section.split_once("\n@@").map(|p| p.0).unwrap_or(section);
            let name = quote_path(&format!("b/{path}"));
            let old = quote_path(&format!("a/{path}"));
            let new_line = format!("+++ {name}");
            let old_line = format!("--- {old}");
            let ren = format!("rename to {}", quote_path(path));
            if headers.lines().any(|line| line == new_line || line == ren)
                || (headers.lines().any(|l| l == "+++ /dev/null")
                    && headers.lines().any(|l| l == old_line))
                || headers.lines().next() == Some(&format!("diff --git {old} {name}"))
            {
                return Ok(json!({"patch":truncate(section),"truncated":false,"nextCursor":null}));
            }
        }
        return Err(error(
            op,
            &format!("GitHub's diff does not include {path}."),
        ));
    }
    if let Some(cursor) = input["cursor"].as_str() {
        let n = cursor
            .parse::<usize>()
            .ok()
            .filter(|p| *p > 0 && cursor.len() <= 7 && p.to_string() == cursor)
            .ok_or_else(|| {
                error(
                    op,
                    "The diff cursor was not one this pull request handed out.",
                )
            })?;
        return page(n, false).await;
    }
    if commit.is_some() {
        return page(1, true).await;
    }
    match gh
        .request("GET", &target, "application/vnd.github.diff", None)
        .await
    {
        Ok(raw) if raw.len() <= 8 * 1024 * 1024 => {
            Ok(json!({"patch":String::from_utf8_lossy(&raw),"truncated":false,"nextCursor":null}))
        }
        Ok(_) => page(1, true).await,
        Err(e @ Failure::Refused) => page(1, false).await.map_err(|_| failure(op, e)),
        Err(e) => Err(failure(op, e)),
    }
}

fn truncate(section: &str) -> String {
    let mut hunk = false;
    section
        .split('\n')
        .map(|line| {
            hunk |= line.starts_with("@@");
            if !hunk || line.len() <= 1001 {
                return line.to_owned();
            }
            let units = line.encode_utf16().collect::<Vec<_>>();
            if units.len() <= 1001 {
                return line.to_owned();
            }
            let digits = (units.len() - 1001).to_string();
            let mut number = String::new();
            for (i, c) in digits.chars().enumerate() {
                if i > 0 && (digits.len() - i) % 3 == 0 {
                    number.push(',')
                }
                number.push(c)
            }
            format!(
                "{} …[{number} more characters]",
                String::from_utf16_lossy(&units[..1001])
            )
        })
        .collect::<Vec<_>>()
        .join("\n")
}
fn quote_path(s: &str) -> String {
    let mut result = String::new();
    for c in s.chars() {
        match c {
            '"' => result.push_str("\\\""),
            '\\' => result.push_str("\\\\"),
            '\x07' => result.push_str("\\a"),
            '\x08' => result.push_str("\\b"),
            '\t' => result.push_str("\\t"),
            '\n' => result.push_str("\\n"),
            '\x0b' => result.push_str("\\v"),
            '\x0c' => result.push_str("\\f"),
            '\r' => result.push_str("\\r"),
            c if c < ' ' || c == '\x7f' => result.push_str(&format!("\\{:03o}", c as u32)),
            c => result.push(c),
        }
    }
    if result == s {
        result
    } else {
        format!("\"{result}\"")
    }
}

async fn file_rows(
    gh: &Github<'_>,
    url: &str,
    page: usize,
    commit: bool,
) -> Result<Vec<Value>, Failure> {
    let mut v = gh
        .json("GET", &format!("{url}?per_page=30&page={page}"), None)
        .await?;
    if commit {
        v = v["files"].take();
        if v.is_null() {
            return Ok(Vec::new());
        }
    }
    match v {
        Value::Array(rows) => Ok(rows),
        _ => Err(Failure::Invalid),
    }
}

async fn files_page(
    gh: &Github<'_>,
    url: &str,
    page: usize,
    commit: bool,
    readable: bool,
) -> Reply {
    let rows = file_rows(gh, url, page, commit)
        .await
        .map_err(|e| failure("diff", e))?;
    let mut patch = String::new();
    let mut omitted = Vec::new();
    for file in &rows {
        let Some(name) = file["filename"].as_str() else {
            continue;
        };
        let kind = file["status"].as_str().unwrap_or_default();
        let old = if kind == "renamed" {
            file["previous_filename"].as_str().unwrap_or(name)
        } else {
            name
        };
        let (old_path, new_path) = (
            quote_path(&format!("a/{old}")),
            quote_path(&format!("b/{name}")),
        );
        let hunks = file["patch"].as_str().unwrap_or("");
        let adds = file["additions"].as_u64().unwrap_or(0);
        let dels = file["deletions"].as_u64().unwrap_or(0);
        let changed = adds > 0 || dels > 0;
        if changed && hunks.is_empty() {
            omitted.push(json!({"path":name,"additions":adds,"deletions":dels}))
        }
        patch.push_str(&format!("diff --git {old_path} {new_path}\n"));
        match kind {
            "added" => patch.push_str("new file mode 100644\n"),
            "removed" => patch.push_str("deleted file mode 100644\n"),
            "renamed" => patch.push_str(&format!(
                "similarity index {}%\nrename from {}\nrename to {}\n",
                if changed || !hunks.is_empty() { 0 } else { 100 },
                quote_path(old),
                quote_path(name)
            )),
            _ => (),
        }
        patch.push_str(&format!(
            "--- {}\n+++ {}\n",
            if kind == "added" {
                "/dev/null"
            } else {
                &old_path
            },
            if kind == "removed" {
                "/dev/null"
            } else {
                &new_path
            }
        ));
        patch.push_str(hunks);
        if !hunks.is_empty() && !hunks.ends_with('\n') {
            patch.push('\n')
        }
    }
    let mut result = json!({"patch":patch,"truncated":!omitted.is_empty(),"nextCursor":(rows.len()>=30).then(||(page+1).to_string())});
    if !omitted.is_empty() {
        result["omittedFileStats"] = json!(omitted)
    }
    if readable {
        result["withheldFilesReadable"] = json!(true)
    }
    Ok(result)
}

async fn contents(gh: &Github<'_>, input: &Value, repo: &str, target: &str, files: &str) -> Reply {
    let op = "diffFileContents";
    let fail = |f| failure(op, f);
    let missing = || json!({"_tag":"PullRequestOperationError","operation":op,"detail":"This file is not part of the change request.","reason":"not-found"});
    let new = input["newPath"].as_str().ok_or_else(missing)?;
    let old = input["oldPath"].as_str().ok_or_else(missing)?;
    let kind = input["changeType"].as_str().ok_or_else(missing)?;
    let commit = input["commit"].as_str();
    let mut page = 1;
    loop {
        let rows = file_rows(gh, files, page, commit.is_some())
            .await
            .map_err(fail)?;
        if rows.iter().any(|r| {
            let name = r["filename"].as_str();
            match (kind, r["status"].as_str()) {
                ("new", Some("added")) => name == Some(new),
                ("deleted", Some("removed")) => name == Some(old),
                ("change", Some("modified" | "changed")) => name == Some(old) && name == Some(new),
                ("rename-pure" | "rename-changed", Some("renamed")) => {
                    name == Some(new) && r["previous_filename"].as_str() == Some(old)
                }
                _ => false,
            }
        }) {
            break;
        }
        if rows.len() < 30 || page >= 100 {
            return Err(missing());
        }
        page += 1;
    }
    let refs = gh.json("GET", target, None).await.map_err(fail)?;
    let mut base = if commit.is_none() {
        refs["base"]["sha"].as_str()
    } else {
        refs["parents"][0]["sha"].as_str()
    }
    .unwrap_or("")
    .to_owned();
    let head = if commit.is_none() {
        refs["head"]["sha"].as_str()
    } else {
        refs["sha"].as_str()
    }
    .unwrap_or("");
    if !valid_sha(head)
        || !(valid_sha(&base) || commit.is_some() && kind == "new" && base.is_empty())
    {
        let msg = match commit {
            Some(c) => format!("Commit {c} reported no usable revisions for this file."),
            None => format!(
                "Pull request #{} reported no usable base and head revisions.",
                input["number"]
            ),
        };
        return Err(error(op, &msg));
    }
    if commit.is_none() && kind != "new" {
        if let Ok(v) = gh
            .json(
                "GET",
                &format!("{repo}/compare/{base}...{head}?per_page=1&page=2"),
                None,
            )
            .await
        {
            if let Some(sha) = v["merge_base_commit"]["sha"]
                .as_str()
                .filter(|s| valid_sha(s))
            {
                base = sha.to_owned()
            }
        }
    }
    async fn read(gh: &Github<'_>, repo: &str, rev: &str, path: &str) -> Result<String, Value> {
        let op = "diffFileContents";
        if path.split('/').any(|p| matches!(p, "" | "." | "..")) {
            return Err(error(op, "Invalid file path."));
        }
        let mut encoded = String::new();
        for b in path.bytes() {
            if b.is_ascii_alphanumeric() || b"/-_.!~*'()".contains(&b) {
                encoded.push(b as char)
            } else {
                encoded.push_str(&format!("%{b:02X}"))
            }
        }
        let bytes = gh
            .request(
                "GET",
                &format!("{repo}/contents/{encoded}?ref={rev}"),
                "application/vnd.github.raw+json",
                None,
            )
            .await
            .map_err(|e| failure(op, e))?;
        if bytes.len() > 1024 * 1024 {
            return Err(error(
                op,
                &format!("The diff file '{path}' exceeds the 1 MB expansion limit."),
            ));
        }
        let binary = || error(op, &format!("The diff file '{path}' is binary."));
        if bytes.contains(&0) {
            return Err(binary());
        }
        String::from_utf8(bytes).map_err(|_| binary())
    }
    let before = if kind == "new" {
        String::new()
    } else {
        read(gh, repo, &base, old).await?
    };
    let after = if kind == "deleted" {
        String::new()
    } else {
        read(gh, repo, head, new).await?
    };
    Ok(json!({"oldContents":before,"newContents":after}))
}

async fn identity(gh: &Github<'_>) -> Reply {
    let value = gh
        .json("GET", "user", None)
        .await
        .map_err(|e| failure("routeIdentity", e))?;
    let invalid = || {
        error(
            "routeIdentity",
            "The signed-in account could not be verified.",
        )
    };
    let account = value["id"].as_u64().ok_or_else(invalid)?;
    let viewer = value["login"]
        .as_str()
        .filter(|v| !v.trim().is_empty())
        .ok_or_else(invalid)?;
    Ok(
        json!({"accountId":account.to_string(),"host":"github.com","provider":"github","viewer":viewer}),
    )
}

async fn metadata(gh: &Github<'_>, op: &str, repo: &str, project: &str, number: u64) -> Reply {
    let (owner, name) = repo.split_once('/').unwrap();
    let mut args = json!({"owner":owner,"name":name,"number":number});
    let summary = op == "summary";
    if summary {
        args["stacks"] = json!(true)
    }
    let time = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;
    let query = if summary {
        include_str!("pull_requests/PullRequestSummary.graphql")
    } else {
        include_str!("pull_requests/PullRequestPreview.graphql")
    };
    let raw = gh.graphql(query, args).await.map_err(|f| failure(op, f))?;
    let pr = &raw["repository"]["pullRequest"];
    if pr.is_null() {
        return Err(failure(op, Failure::NotFound));
    }
    let required = |key: &str| {
        pr[key]
            .as_str()
            .ok_or_else(|| failure(op, Failure::Invalid))
    };
    let title = required("title")?;
    let url = required("url")?;
    let state = match pr["state"].as_str() {
        _ if pr["mergedAt"].is_string() => "merged",
        Some("MERGED") => "merged",
        Some("CLOSED") => "closed",
        Some("OPEN") => "open",
        _ => return Err(failure(op, Failure::Invalid)),
    };
    let draft = pr["isDraft"]
        .as_bool()
        .ok_or_else(|| failure(op, Failure::Invalid))?;
    let raw_author = &pr["author"];
    let author = raw_author["login"].as_str().map(|login| {
        let mut a =
            json!({"login":login,"name":raw_author["name"],"avatarUrl":raw_author["avatarUrl"]});
        if summary
            && a["avatarUrl"].is_null()
            && login.len() <= 39
            && login
                .as_bytes()
                .first()
                .is_some_and(u8::is_ascii_alphanumeric)
            && login
                .bytes()
                .all(|c| c.is_ascii_alphanumeric() || c == b'-')
        {
            a["avatarUrl"] = json!(format!("https://github.com/{login}.png?size=80"))
        }
        if raw_author["__typename"] == "Bot" {
            a["isBot"] = json!(true)
        }
        a
    });
    if !summary {
        return Ok(
            json!({"projectId":project,"repository":repo,"number":number,"title":title,"url":url,"author":author,"state":state,"isDraft":draft,"createdAt":required("createdAt")?}),
        );
    }
    let mut decision = match pr["reviewDecision"].as_str() {
        Some("APPROVED") => Some("approved"),
        Some("CHANGES_REQUESTED") => Some("changes-requested"),
        Some("REVIEW_REQUIRED") => Some("review-required"),
        _ => None,
    };
    if !matches!(decision, Some("approved" | "changes-requested")) {
        let reviews = pr["latestReviews"]["nodes"]
            .as_array()
            .into_iter()
            .flatten()
            .filter(|r| r["isMinimized"] != true)
            .filter_map(|r| r["state"].as_str())
            .collect::<Vec<_>>();
        if reviews.contains(&"CHANGES_REQUESTED") {
            decision = Some("changes-requested")
        } else if decision.is_none() && reviews.contains(&"APPROVED") {
            decision = Some("approved")
        }
    }
    let checks = match pr["commits"]["nodes"][0]["commit"]["statusCheckRollup"]["state"].as_str() {
        Some("SUCCESS") => Some("passing"),
        Some("FAILURE" | "ERROR") => Some("failing"),
        Some("PENDING" | "EXPECTED") => Some("pending"),
        _ => None,
    };
    let merge = match pr["mergeable"].as_str() {
        Some("MERGEABLE") => "mergeable",
        Some("CONFLICTING") => "conflicting",
        _ => "unknown",
    };
    let mut result = json!({"provider":"github","projectId":project,"repository":repo,"number":number,"title":title,"url":url,"state":state,"isDraft":draft,"headBranch":required("headRefName")?,"baseBranch":required("baseRefName")?,"closedAt":pr["closedAt"],"mergedAt":pr["mergedAt"],"updatedAt":required("updatedAt")?,"observedAt":time,"author":author,"additions":pr["additions"],"deletions":pr["deletions"],"changedFiles":pr["changedFiles"],"reviewDecision":decision,"checksState":checks,"mergeability":merge});
    if let Some(stack) = pr.get("stack") {
        result["stack"] = if !stack.is_null() && !pr["stackEntry"].is_null() {
            json!({"number":stack["number"],"position":pr["stackEntry"]["position"],"size":stack["size"],"base":stack["baseRefName"]})
        } else {
            Value::Null
        }
    }
    Ok(result)
}
