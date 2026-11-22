//! One Bitbucket pull request: its detail, preview, checks and activity, and the
//! actions on it, as the reference's `BitbucketPullRequestApi`,
//! `BitbucketPullRequestProvider` and `PullRequestService` read and run them.
//!
//! Reads that may fail without failing the whole answer (mergeability, checks and
//! permissions in a detail; comments and commits in an activity) fall back as the
//! reference's `recoverRead` does. Errors carry the reference's cause chain:
//! `PullRequestProviderError`, then the Bitbucket API error.
use super::bitbucket::{actor, base_url, iso_utc, origin, trimmed, viewer_for, Viewer};
use crate::Project;
use serde_json::{json, Map, Value};
use std::time::Duration;

const MAX_PAGE_SIZE: usize = 50;
const CONVERSATION_PAGES: usize = 10;
const ACTIONS: [&str; 2] = ["merge", "close"];
const MERGE_METHODS: [&str; 3] = ["merge", "squash", "rebase"];
const VERDICTS: [&str; 3] = ["comment", "approve", "request-changes"];

/// `CAPABILITIES` of the reference's Bitbucket provider.
fn capabilities() -> Value {
    json!({"diff": true, "comment": true, "actions": ACTIONS, "mergeMethods": MERGE_METHODS, "search": true,
        "reactions": false, "viewedFiles": "environment",
        "review": {"inlineComment": true, "reply": true, "resolve": true, "verdicts": VERDICTS},
        "reviewers": {"request": true, "listCandidates": true, "searchCandidates": false},
        "edit": {"changeRequest": true, "comment": true}, "mentions": false, "referenceSigils": []})
}

/// `bitbucketViewerPermissions`: merging needs write access; the rest any reader may do.
fn viewer_permissions(can_write: bool) -> Value {
    let actions: Vec<&str> = ACTIONS
        .iter()
        .copied()
        .filter(|a| *a != "merge" || can_write)
        .collect();
    json!({"actions": actions, "comment": true, "resolve": true, "verdicts": VERDICTS, "requestReviewers": true})
}

/// A Bitbucket API failure: its provider detail, reason, and the API error under it.
#[derive(Debug, Clone)]
pub(super) struct ApiError {
    detail: String,
    reason: &'static str,
    cause: Value,
}

impl ApiError {
    fn status(status: u16, method: &str, url: &str) -> Self {
        let detail = format!("Bitbucket returned HTTP {status}.");
        let reason = match status {
            401 => "unauthenticated",
            404 => "not-found",
            429 => "rate-limited",
            _ => "failed",
        };
        // A 2xx reply with no body cannot be read (`BitbucketResponseBodyReadError`).
        let cause = if (200..300).contains(&status) {
            json!({"name": "BitbucketResponseBodyReadError", "message": format!("Bitbucket API failed in request: {detail}"),
                "cause": {"name": "HttpClientError", "message": format!("EmptyBody: can not create stream from empty body ({status} {method} {url})")}})
        } else {
            json!({"name": "BitbucketResponseError", "message": format!("Bitbucket API failed in request: {detail}")})
        };
        Self {
            detail,
            reason,
            cause,
        }
    }

    fn send() -> Self {
        let detail = "Failed to send the Bitbucket request.".to_owned();
        Self {
            cause: json!({"name": "BitbucketRequestError", "message": format!("Bitbucket API failed in request: {detail}")}),
            detail,
            reason: "failed",
        }
    }

    fn untrusted(host: &str) -> Self {
        let detail =
            format!("Bitbucket returned a link to {host}, which is not the configured Bitbucket.");
        Self {
            cause: json!({"name": "BitbucketUntrustedUrlError", "message": detail}),
            detail,
            reason: "failed",
        }
    }

    fn unreadable(operation: &str) -> Self {
        let detail = format!("Bitbucket returned an unreadable {operation} response.");
        Self {
            cause: json!({"name": "BitbucketPullRequestReadError", "message": format!("Bitbucket failed in {operation}: {detail}")}),
            detail,
            reason: "failed",
        }
    }

    fn unresolved(message: String) -> Self {
        Self {
            cause: json!({"name": "SettingsSecretUnresolvedError", "message": message.clone()}),
            detail: message,
            reason: "unauthenticated",
        }
    }

    fn not_addressable(repository: &str) -> Self {
        let detail = "A Bitbucket repository is addressed as workspace/repository.".to_owned();
        let _ = repository;
        Self {
            cause: json!({"name": "BitbucketRepositoryUnsupportedError", "message": format!("Bitbucket failed in resolveRepository: {detail}")}),
            detail,
            reason: "failed",
        }
    }

    /// Whether `recoverRead` falls back past this failure (everything but rate limits).
    fn recoverable(&self) -> bool {
        self.reason != "rate-limited" && self.reason != "unauthenticated"
    }

    /// `toPullRequestErrorWith(operation)` for the provider operation that failed.
    fn to_error(&self, operation: &str, provider_operation: &str) -> Value {
        let provider_error = json!({"name": "PullRequestProviderError",
            "message": format!("bitbucket failed in {provider_operation}: {}", self.detail), "cause": self.cause});
        if self.reason == "unauthenticated" {
            return json!({"_tag": "PullRequestUnavailableError", "reason": "cli-unauthenticated", "provider": "bitbucket",
                "requirement": super::bitbucket::UNAUTHENTICATED, "cause": provider_error});
        }
        let mut error = json!({"_tag": "PullRequestOperationError", "operation": operation, "detail": self.detail, "cause": provider_error});
        if self.reason == "not-found" {
            error["reason"] = json!("not-found");
        }
        error
    }
}

/// `BitbucketApi.request`: a request to the API (a path under it, or a link on its
/// origin) with the credential, answering the body of a 2xx reply.
async fn request(method: &str, url: &str, body: Option<Value>) -> Result<String, ApiError> {
    let base = base_url();
    let url = if url.starts_with("http://") || url.starts_with("https://") {
        if origin(url).is_none() || origin(url) != origin(&base) {
            return Err(ApiError::untrusted(
                &origin(url).unwrap_or_else(|| "an unreadable url".into()),
            ));
        }
        url.to_owned()
    } else {
        format!("{}{url}", base.trim_end_matches('/'))
    };
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .map_err(|_| ApiError::send())?;
    let verb = reqwest::Method::from_bytes(method.as_bytes()).map_err(|_| ApiError::send())?;
    let mut builder = client
        .request(verb, &url)
        .header("Accept", "application/json");
    if let Some(body) = body {
        builder = builder
            .header("Content-Type", "application/json")
            .body(body.to_string());
    }
    match crate::bitbucket_credential::current().await {
        Ok(Some(credential)) => builder = credential.apply(builder),
        Ok(None) => {}
        Err(message) => return Err(ApiError::unresolved(message)),
    }
    let response = builder.send().await.map_err(|_| ApiError::send())?;
    let status = response.status().as_u16();
    if !(200..300).contains(&status) || status == 204 {
        return Err(ApiError::status(status, method, &url));
    }
    response.text().await.map_err(|_| ApiError::send())
}

fn get_json(body: &str, operation: &str) -> Result<Value, ApiError> {
    serde_json::from_str(body).map_err(|_| ApiError::unreadable(operation))
}

/// `repositoryPathOf`, or the repository's own error when it is not workspace/repository.
fn repository_path(repository: &str) -> Result<(String, String), ApiError> {
    let parts: Vec<&str> = repository
        .split('/')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .collect();
    match parts.as_slice() {
        [workspace, slug] => Ok((
            format!(
                "/repositories/{}/{}",
                crate::thread_projection::uri_component(workspace),
                crate::thread_projection::uri_component(slug)
            ),
            (*workspace).to_owned(),
        )),
        _ => Err(ApiError::not_addressable(repository)),
    }
}

/// `toPullRequest`, keeping what one pull request's reads need.
#[derive(Clone)]
struct PullRequest {
    raw: Value,
    number: u64,
    title: String,
    url: String,
    author: Option<Value>,
    head_branch: String,
    head_sha: Option<String>,
    head_repository: Option<String>,
    base_branch: String,
    state: &'static str,
    is_draft: bool,
    created_at: String,
    updated_at: String,
    closed_at: Option<String>,
    body: String,
    reviewers: Vec<Value>,
    reviewer_ids: Vec<String>,
}

fn decode_pull_request(raw: &Value) -> Option<PullRequest> {
    let list = super::bitbucket::pull_request(raw)?;
    let reviewers: Vec<Value> = raw["reviewers"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(actor)
        .collect();
    Some(PullRequest {
        raw: raw.clone(),
        number: list.number,
        title: list.title,
        url: list.url,
        author: list.author,
        head_branch: list.head_branch,
        head_sha: trimmed(&raw["source"]["commit"]["hash"]),
        head_repository: trimmed(&raw["source"]["repository"]["full_name"]),
        base_branch: list.base_branch,
        state: list.state,
        is_draft: list.is_draft,
        created_at: list.created_at,
        updated_at: list.updated_at,
        closed_at: raw["closed_on"].as_str().map(iso_utc),
        body: raw["description"].as_str().unwrap_or_default().to_owned(),
        reviewer_ids: raw["reviewers"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|r| trimmed(&r["uuid"]))
            .collect(),
        reviewers,
    })
}

/// `toReviews`: each participant's vote as a review.
fn reviews(pr: &PullRequest) -> Vec<Value> {
    pr.raw["participants"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|participant| {
            let author = actor(&participant["user"])?;
            let voted_at = trimmed(&participant["participated_on"])?;
            let state = trimmed(&participant["state"]).or_else(|| (participant["approved"] == true).then(|| "approved".to_owned()))?;
            Some(json!({"id": format!("{}:{}", pr.number, author["login"].as_str().unwrap_or_default()), "kind": "review",
                "author": author, "body": "", "createdAt": iso_utc(&voted_at), "url": null, "path": null, "reviewState": state}))
        })
        .collect()
}

/// A client for one repository's pull requests.
pub(super) struct Repository<'a> {
    pub repository: &'a str,
    pub host: &'a str,
    pub project: &'a Project,
}

impl Repository<'_> {
    fn path(&self) -> Result<(String, String), ApiError> {
        repository_path(self.repository)
    }

    async fn pull_request(&self, number: u64, closed_on: bool) -> Result<PullRequest, ApiError> {
        let (path, _) = self.path()?;
        let suffix = if closed_on {
            "?fields=%2Bclosed_on"
        } else {
            ""
        };
        let body = request(
            "GET",
            &format!("{path}/pullrequests/{number}{suffix}"),
            None,
        )
        .await?;
        decode_pull_request(&get_json(&body, "getPullRequest")?)
            .ok_or_else(|| ApiError::unreadable("getPullRequest"))
    }

    /// `getRepositoryPermission`: whether the viewer can write. A permission endpoint that
    /// refuses (403, 404, 410) leaves the write to enforce access.
    async fn can_write(&self) -> Result<bool, ApiError> {
        let (_, workspace) = self.path()?;
        let q = format!(
            "repository.full_name=\"{}\"",
            self.repository.replace('\\', "\\\\").replace('"', "\\\"")
        );
        let url = format!(
            "/user/workspaces/{}/permissions/repositories?q={}",
            crate::thread_projection::uri_component(&workspace),
            crate::thread_projection::uri_component(&q)
        );
        match request("GET", &url, None).await {
            Ok(body) => {
                let raw = get_json(&body, "getRepositoryPermission")?;
                let permission = raw["values"][0]["permission"]
                    .as_str()
                    .map(|p| p.trim().to_lowercase())
                    .filter(|p| !p.is_empty());
                Ok(permission.is_none_or(|p| p == "admin" || p == "write"))
            }
            Err(e)
                if matches!(e.reason, "not-found")
                    || e.detail.ends_with("HTTP 403.")
                    || e.detail.ends_with("HTTP 410.") =>
            {
                Ok(true)
            }
            Err(e) => Err(e),
        }
    }

    async fn diffstat(&self, number: u64) -> Result<(u64, u64, u64), ApiError> {
        let (path, _) = self.path()?;
        let mut url = format!("{path}/pullrequests/{number}/diffstat?pagelen={MAX_PAGE_SIZE}");
        let (mut additions, mut deletions, mut files) = (0, 0, 0);
        loop {
            let raw = get_json(&request("GET", &url, None).await?, "getDiffStat")?;
            let values = raw["values"]
                .as_array()
                .ok_or_else(|| ApiError::unreadable("getDiffStat"))?;
            for value in values {
                let int = |k: &str| value.get(k).is_none_or(|v| v.is_null() || v.is_u64());
                if !value.is_object() || !int("lines_added") || !int("lines_removed") {
                    continue;
                }
                additions += value["lines_added"].as_u64().unwrap_or(0);
                deletions += value["lines_removed"].as_u64().unwrap_or(0);
                files += 1;
            }
            match trimmed(&raw["next"]) {
                Some(next) => url = next,
                None => return Ok((additions, deletions, files)),
            }
        }
    }

    async fn mergeability(&self, number: u64) -> Result<&'static str, ApiError> {
        let (path, _) = self.path()?;
        let raw = get_json(
            &request(
                "GET",
                &format!("{path}/pullrequests/{number}/conflicts"),
                None,
            )
            .await?,
            "getMergeability",
        )?;
        let values = raw["values"]
            .as_array()
            .ok_or_else(|| ApiError::unreadable("getMergeability"))?;
        Ok(if values.is_empty() {
            "mergeable"
        } else {
            "conflicting"
        })
    }

    /// `itemPages`: every page of a list, up to the end.
    async fn pages(&self, url: String, operation: &str) -> Result<Vec<Value>, ApiError> {
        let mut items = Vec::new();
        let mut url = url;
        loop {
            let raw = get_json(&request("GET", &url, None).await?, operation)?;
            items.push(raw.clone());
            match trimmed(&raw["next"]) {
                Some(next) => url = next,
                None => return Ok(items),
            }
        }
    }

    /// `listChecks`: statuses, reruns of a key collapsed to the last, failures first.
    async fn checks(&self, number: u64) -> Result<Vec<Value>, ApiError> {
        let (path, _) = self.path()?;
        let pages = self
            .pages(
                format!("{path}/pullrequests/{number}/statuses?pagelen={MAX_PAGE_SIZE}"),
                "listChecks",
            )
            .await?;
        let mut all = Vec::new();
        for page in pages {
            let entries: Vec<(String, Value)> = page["values"]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(|status| {
                    let name = trimmed(&status["name"]).or_else(|| trimmed(&status["key"]))?;
                    let state = match status["state"].as_str().map(|s| s.trim().to_uppercase()).as_deref() {
                        Some("SUCCESSFUL") => "success",
                        Some("FAILED") => "failure",
                        Some("STOPPED") => "cancelled",
                        Some("INPROGRESS") => "pending",
                        _ => "neutral",
                    };
                    let check = json!({"name": name, "status": state, "description": trimmed(&status["description"]), "url": trimmed(&status["url"])});
                    Some((trimmed(&status["key"]).unwrap_or_default(), check))
                })
                .collect();
            all.extend(dedupe_checks(entries));
        }
        Ok(sort_checks(all))
    }

    /// `listComments`: comments and their review threads, up to ten pages.
    async fn comments(&self, number: u64) -> Result<(Vec<Value>, Vec<Value>, bool), ApiError> {
        let (path, _) = self.path()?;
        let mut url = format!("{path}/pullrequests/{number}/comments?pagelen={MAX_PAGE_SIZE}");
        let (mut comments, mut kept) = (Vec::new(), Vec::new());
        for page in 1.. {
            let raw = get_json(&request("GET", &url, None).await?, "listComments")?;
            for comment in raw["values"].as_array().into_iter().flatten() {
                if comment["id"].as_i64().is_none() || comment["created_on"].as_str().is_none() {
                    continue;
                }
                if comment["deleted"] == true || comment["pending"] == true {
                    continue;
                }
                let body = comment["content"]["raw"].as_str().unwrap_or_default();
                if body.trim().is_empty() {
                    continue;
                }
                let path = trimmed(&comment["inline"]["path"]);
                comments.push(json!({"id": comment["id"].to_string(), "kind": if path.is_some() { "review-comment" } else { "issue-comment" },
                    "author": actor(&comment["user"]), "body": body, "createdAt": iso_utc(comment["created_on"].as_str().unwrap_or_default()),
                    "url": trimmed(&comment["links"]["html"]["href"]), "path": path, "reviewState": null}));
                kept.push(comment.clone());
            }
            match trimmed(&raw["next"]) {
                Some(next) if page < CONVERSATION_PAGES => url = next,
                next => return Ok((comments, review_threads(&kept), next.is_some())),
            }
        }
        unreachable!()
    }

    async fn commits(&self, number: u64) -> Result<Vec<Value>, ApiError> {
        let (path, _) = self.path()?;
        let pages = self
            .pages(
                format!("{path}/pullrequests/{number}/commits?pagelen={MAX_PAGE_SIZE}"),
                "listCommits",
            )
            .await?;
        let mut all: Vec<Value> = Vec::new();
        // Each page lists newest first; the timeline reads oldest first.
        for page in pages {
            let mut items: Vec<Value> = page["values"]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(|commit| {
                    let hash = trimmed(&commit["hash"])?;
                    let date = trimmed(&commit["date"])?;
                    let linked = actor(&commit["author"]["user"]);
                    let raw_author = trimmed(&commit["author"]["raw"]);
                    let authors = match (linked, raw_author) {
                        (Some(a), _) => vec![a],
                        (None, Some(r)) => vec![json!({"login": r, "name": r, "avatarUrl": null})],
                        (None, None) => vec![],
                    };
                    let headline = commit["message"].as_str().unwrap_or_default().split('\n').next().unwrap_or_default().to_owned();
                    Some(json!({"oid": hash, "messageHeadline": headline, "committedDate": iso_utc(&date), "authors": authors}))
                })
                .collect();
            items.reverse();
            items.extend(all);
            all = items;
        }
        Ok(all)
    }

    /// `getChangeRequest`: the pull request with its diffstat, mergeability, checks and the
    /// viewer's permissions, read side by side.
    async fn change_request(&self, number: u64) -> Result<Value, ApiError> {
        let (pr, diffstat, mergeability, checks, can_write) = tokio::join!(
            self.pull_request(number, true),
            self.diffstat(number),
            self.mergeability(number),
            self.checks(number),
            self.can_write()
        );
        let pr = pr?;
        let (additions, deletions, changed_files) = diffstat?;
        let recover = |r: Result<Value, ApiError>, fallback: Value| match r {
            Ok(v) => Ok(v),
            Err(e) if e.recoverable() => Ok(fallback),
            Err(e) => Err(e),
        };
        let mergeability = recover(mergeability.map(|m| json!(m)), json!("unknown"))?;
        let checks = recover(checks.map(|c| json!(c)), json!([]))?;
        let can_write = recover(can_write.map(|w| json!(w)), json!(true))?
            .as_bool()
            .unwrap_or(true);
        let mut detail = json!({"number": pr.number, "title": pr.title, "url": pr.url, "author": pr.author, "headBranch": pr.head_branch,
            "baseBranch": pr.base_branch, "state": pr.state, "isDraft": pr.is_draft, "mergeability": mergeability,
            "additions": additions, "deletions": deletions, "changedFiles": changed_files, "createdAt": pr.created_at,
            "updatedAt": pr.updated_at, "body": pr.body,
            "mergedAt": if pr.state == "merged" { json!(pr.closed_at) } else { Value::Null },
            "closedAt": if pr.state == "closed" { json!(pr.closed_at) } else { Value::Null },
            "reviewers": pr.reviewers, "labels": [], "checks": checks,
            // Bitbucket exposes no strategy permissions; unsupported strategies fail at merge.
            "mergeCapabilities": {"merge": true, "squash": true, "rebase": true},
            "viewerPermissions": viewer_permissions(can_write)});
        if let Some(sha) = &pr.head_sha {
            detail["headSha"] = json!(sha);
        }
        if let Some(head) = &pr.head_repository {
            detail["headRepositoryNameWithOwner"] = json!(head);
        }
        Ok(detail)
    }
}

/// `dedupeChecks` for one page: reruns of a workflow and name keep the last, and names shared
/// by several workflows take the workflow's name in front.
fn dedupe_checks(entries: Vec<(String, Value)>) -> Vec<Value> {
    let mut kept: indexmap::IndexMap<(String, String), (String, Value)> = indexmap::IndexMap::new();
    for (workflow, check) in entries {
        let key = (
            workflow.clone(),
            check["name"].as_str().unwrap_or_default().to_owned(),
        );
        kept.insert(key, (workflow, check));
    }
    let mut counts: std::collections::HashMap<String, usize> = std::collections::HashMap::new();
    for (_, check) in kept.values() {
        *counts
            .entry(check["name"].as_str().unwrap_or_default().to_owned())
            .or_default() += 1;
    }
    sort_checks(
        kept.into_values()
            .map(|(workflow, mut check)| {
                let name = check["name"].as_str().unwrap_or_default().to_owned();
                if !workflow.is_empty() && counts.get(&name).copied().unwrap_or(0) > 1 {
                    check["name"] = json!(format!("{workflow} / {name}"));
                }
                check
            })
            .collect(),
    )
}

/// `sortChecks`: failed and cancelled checks first, otherwise in order.
fn sort_checks(mut checks: Vec<Value>) -> Vec<Value> {
    checks.sort_by_key(|c| !matches!(c["status"].as_str(), Some("failure" | "cancelled")));
    checks
}

/// `buildReviewThreads`: inline comments grouped under their root comment.
fn review_threads(comments: &[Value]) -> Vec<Value> {
    let by_id: std::collections::HashMap<i64, &Value> = comments
        .iter()
        .filter_map(|c| Some((c["id"].as_i64()?, c)))
        .collect();
    let root_of = |comment: &Value| -> Value {
        let mut current = comment.clone();
        for _ in 0..by_id.len() {
            match current["parent"]["id"]
                .as_i64()
                .and_then(|id| by_id.get(&id))
            {
                Some(parent) => current = (*parent).clone(),
                None => break,
            }
        }
        current
    };
    let mut threads: indexmap::IndexMap<i64, Value> = indexmap::IndexMap::new();
    let mut replies: std::collections::HashMap<i64, Vec<Value>> = std::collections::HashMap::new();
    for comment in comments {
        let root = root_of(comment);
        let inline = &root["inline"];
        let Some(path) = trimmed(&inline["path"]) else {
            continue;
        };
        let root_id = root["id"].as_i64().unwrap_or_default();
        if root_id == comment["id"].as_i64().unwrap_or_default() {
            let side = if inline["to"].is_null() {
                "left"
            } else {
                "right"
            };
            let line = if side == "left" {
                &inline["from"]
            } else {
                &inline["to"]
            };
            threads.insert(root_id, json!({"id": root_id.to_string(), "path": path,
                "line": line.as_i64().filter(|l| *l > 0), "side": side,
                "isResolved": !root["resolution"].is_null(), "isOutdated": inline["outdated"] == true, "comments": []}));
        }
        replies.entry(root_id).or_default().push(comment.clone());
    }
    threads
        .into_iter()
        .filter_map(|(id, mut thread)| {
            let mut entries = replies.remove(&id).unwrap_or_default();
            entries.sort_by(|a, b| a["created_on"].as_str().cmp(&b["created_on"].as_str()));
            let entries: Vec<Value> = entries
                .iter()
                .map(|c| json!({"id": c["id"].to_string(), "author": actor(&c["user"]), "body": c["content"]["raw"].as_str().unwrap_or_default(),
                    "createdAt": iso_utc(c["created_on"].as_str().unwrap_or_default()), "url": trimmed(&c["links"]["html"]["href"])}))
                .collect();
            (!entries.is_empty()).then(|| {
                thread["comments"] = json!(entries);
                thread
            })
        })
        .collect()
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn refusal(operation: &str, detail: &str) -> Value {
    json!({"_tag": "PullRequestOperationError", "operation": operation, "detail": detail})
}

fn action_refusal(action: &str) -> &'static str {
    match action {
        "merge" => "You need write access on this repository to merge.",
        "ready" => "You need write access on this repository, or to have opened this change request, to mark it ready for review.",
        "draft" => "You need write access on this repository, or to have opened this change request, to return it to a draft.",
        "close" => "You need write access on this repository, or to have opened this change request, to close it.",
        "update-branch" => "You need write access on this repository, or to have opened this change request, to update its branch.",
        "reopen" => "You need write access on this repository, or to have opened this change request, to reopen it.",
        "enable-auto-merge" => "Auto-merge needs write access on this repository, and the repository has to allow it.",
        "disable-auto-merge" => "You need write access on this repository to stop it being merged for you once it is ready.",
        "revert" => "You need write access on this repository to open a revert pull request.",
        "delete-branch" => "You need write access on the source repository to delete its branch.",
        _ => "You need write access on this repository to approve workflows from a fork pull request.",
    }
}

/// The RPCs for one Bitbucket pull request (`op` as `pullRequests.<op>`).
pub(super) async fn run(
    repo: Repository<'_>,
    op: &str,
    input: &Value,
    number: u64,
) -> Result<Value, Value> {
    let project = repo.project;
    let body_of = |k: &str| input[k].as_str().unwrap_or_default().to_owned();
    match op {
        "detail" => {
            let (detail, viewer) = tokio::join!(repo.change_request(number), viewer_for(repo.host));
            let mut detail = detail.map_err(|e| e.to_error("detail", "getChangeRequest"))?;
            let fields = json!({"provider": "bitbucket", "capabilities": capabilities(), "projectId": project.id,
                "projectTitle": project.name, "workspaceRoot": project.path, "repository": repo.repository, "observedAt": now_ms()});
            for (k, v) in fields.as_object().into_iter().flatten() {
                detail[k] = v.clone();
            }
            if let Viewer::Known(name) = viewer {
                if !name.trim().is_empty() {
                    detail["viewer"] = json!(name);
                }
            }
            Ok(detail)
        }
        "preview" => {
            let pr = repo
                .pull_request(number, true)
                .await
                .map_err(|e| e.to_error("preview", "getChangeRequestPreview"))?;
            Ok(
                json!({"projectId": project.id, "repository": repo.repository, "number": pr.number, "title": pr.title,
                "url": pr.url, "author": pr.author, "state": pr.state, "isDraft": pr.is_draft, "createdAt": pr.created_at}),
            )
        }
        "checks" => {
            let (pr, checks) = tokio::join!(repo.pull_request(number, true), repo.checks(number));
            let observed_at = now_ms();
            let pr = pr.map_err(|e| e.to_error("checks", "getChangeRequestChecks"))?;
            let checks = checks.map_err(|e| e.to_error("checks", "getChangeRequestChecks"))?;
            Ok(json!({"observedAt": observed_at, "state": pr.state, "checks": checks}))
        }
        "activity" => {
            let (pr, comments, commits) = tokio::join!(
                repo.pull_request(number, true),
                repo.comments(number),
                repo.commits(number)
            );
            let failed = |e: ApiError| e.to_error("activity", "getChangeRequestActivity");
            let pr = pr.map_err(failed)?;
            let (comments, threads, truncated) = match comments {
                Ok(read) => read,
                Err(e) if e.recoverable() => (vec![], vec![], true),
                Err(e) => return Err(failed(e)),
            };
            let commits = match commits {
                Ok(read) => read,
                Err(e) if e.recoverable() => vec![],
                Err(e) => return Err(failed(e)),
            };
            let reviews = reviews(&pr);
            let count = comments.len() + reviews.len();
            let mut all: Vec<Value> = comments.into_iter().chain(reviews).collect();
            all.sort_by(|a, b| a["createdAt"].as_str().cmp(&b["createdAt"].as_str()));
            Ok(
                json!({"comments": all, "commentCount": count, "commentsTruncated": truncated, "reviewThreads": threads, "commits": commits}),
            )
        }
        "reviewerCandidates" => {
            repo.can_write()
                .await
                .map_err(|e| e.to_error("reviewerCandidates", "getViewerPermissions"))?;
            let (_, workspace) = repo
                .path()
                .map_err(|e| e.to_error("reviewerCandidates", "listReviewerCandidates"))?;
            let members_url = format!(
                "/workspaces/{}/members?pagelen={MAX_PAGE_SIZE}",
                crate::thread_projection::uri_component(&workspace)
            );
            let (pr, members) = tokio::join!(
                repo.pull_request(number, false),
                request("GET", &members_url, None)
            );
            let failed = |e: ApiError| e.to_error("reviewerCandidates", "listReviewerCandidates");
            let pr = pr.map_err(failed)?;
            let members =
                get_json(&members.map_err(failed)?, "listReviewerCandidates").map_err(failed)?;
            let author = pr
                .author
                .as_ref()
                .and_then(|a| a["login"].as_str())
                .map(str::to_owned);
            let candidates: Vec<Value> = members["values"]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(|member| {
                    let uuid = trimmed(&member["user"]["uuid"])?;
                    let mut candidate = actor(&member["user"])?;
                    // Bitbucket forbids requesting a review from the pull request's author.
                    if candidate["login"].as_str() == author.as_deref() {
                        return None;
                    }
                    candidate["id"] = json!(uuid);
                    candidate["kind"] = json!("user");
                    candidate["isRequested"] = json!(pr.reviewer_ids.contains(&uuid));
                    Some(candidate)
                })
                .collect();
            Ok(json!({"candidates": candidates, "truncated": trimmed(&members["next"]).is_some()}))
        }
        "requestReviewers" => {
            repo.can_write()
                .await
                .map_err(|e| e.to_error("requestReviewers", "getViewerPermissions"))?;
            let failed = |e: ApiError| e.to_error("requestReviewers", "setReviewerRequest");
            let (path, _) = repo.path().map_err(failed)?;
            let current = repo.pull_request(number, false).await.map_err(failed)?;
            // Bitbucket replaces the whole reviewer list.
            let mut uuids: indexmap::IndexSet<String> = current.reviewer_ids.into_iter().collect();
            for reviewer in input["reviewers"].as_array().into_iter().flatten() {
                let id = reviewer["id"]
                    .as_str()
                    .unwrap_or_default()
                    .trim()
                    .to_owned();
                if input["requested"] == true {
                    uuids.insert(id);
                } else {
                    uuids.shift_remove(&id);
                }
            }
            let reviewers: Vec<Value> = uuids
                .into_iter()
                .map(|uuid| json!({"uuid": uuid}))
                .collect();
            request(
                "PUT",
                &format!("{path}/pullrequests/{number}"),
                Some(json!({"reviewers": reviewers})),
            )
            .await
            .map_err(failed)?;
            Ok(Value::Null)
        }
        "update" => {
            if input.get("maintainerCanModify").is_some() {
                return Err(refusal(
                    "update",
                    "This host does not support maintainer edits.",
                ));
            }
            if input.get("title").is_none() && input.get("body").is_none() {
                return Err(refusal("update", "Nothing was changed."));
            }
            let failed = |e: ApiError| e.to_error("update", "updateChangeRequest");
            let (path, _) = repo.path().map_err(failed)?;
            // Partial PUTs keep concurrent edits to the fields left alone.
            let mut patch = Map::new();
            if let Some(title) = input["title"].as_str() {
                patch.insert("title".into(), json!(title.trim()));
            }
            if let Some(body) = input["body"].as_str() {
                patch.insert("description".into(), json!(body));
            }
            request(
                "PUT",
                &format!("{path}/pullrequests/{number}"),
                Some(Value::Object(patch)),
            )
            .await
            .map_err(failed)?;
            Ok(Value::Null)
        }
        "comment" => {
            if body_of("body").trim().is_empty() {
                return Err(refusal("comment", "A comment cannot be empty."));
            }
            repo.can_write()
                .await
                .map_err(|e| e.to_error("comment", "getViewerPermissions"))?;
            let failed = |e: ApiError| e.to_error("comment", "comment");
            let (path, _) = repo.path().map_err(failed)?;
            request(
                "POST",
                &format!("{path}/pullrequests/{number}/comments"),
                Some(json!({"content": {"raw": body_of("body")}})),
            )
            .await
            .map_err(failed)?;
            Ok(Value::Null)
        }
        "updateComment" => {
            if body_of("body").trim().is_empty() {
                return Err(refusal("updateComment", "A comment cannot be empty."));
            }
            let failed = |e: ApiError| e.to_error("updateComment", "updateComment");
            let (path, _) = repo.path().map_err(failed)?;
            let id = crate::thread_projection::uri_component(body_of("commentId").trim());
            request(
                "PUT",
                &format!("{path}/pullrequests/{number}/comments/{id}"),
                Some(json!({"content": {"raw": body_of("body")}})),
            )
            .await
            .map_err(failed)?;
            Ok(Value::Null)
        }
        "submitReview" => {
            let verdict = body_of("verdict");
            let comments = input["comments"].as_array().cloned().unwrap_or_default();
            if verdict != "approve" && body_of("body").trim().is_empty() && comments.is_empty() {
                return Err(refusal(
                    "submitReview",
                    "A review needs a summary or at least one comment.",
                ));
            }
            repo.can_write()
                .await
                .map_err(|e| e.to_error("submitReview", "getViewerPermissions"))?;
            let failed = |e: ApiError| e.to_error("submitReview", "submitReview");
            let (path, _) = repo.path().map_err(failed)?;
            let pull = format!("{path}/pullrequests/{number}");
            // A review that fails part way must not leave an approval.
            for comment in &comments {
                let position = &comment["position"];
                let mut inline =
                    json!({"path": comment["path"].as_str().unwrap_or_default().trim()});
                match position["kind"].as_str() {
                    Some("added") => inline["to"] = position["newLine"].clone(),
                    Some("deleted") => inline["from"] = position["oldLine"].clone(),
                    _ if position["side"] == "left" => inline["from"] = position["oldLine"].clone(),
                    _ => inline["to"] = position["newLine"].clone(),
                }
                request(
                    "POST",
                    &format!("{pull}/comments"),
                    Some(json!({"content": {"raw": comment["body"]}, "inline": inline})),
                )
                .await
                .map_err(failed)?;
            }
            if !body_of("body").trim().is_empty() {
                request(
                    "POST",
                    &format!("{pull}/comments"),
                    Some(json!({"content": {"raw": body_of("body")}})),
                )
                .await
                .map_err(failed)?;
            }
            match verdict.as_str() {
                "approve" => drop(
                    request("POST", &format!("{pull}/approve"), None)
                        .await
                        .map_err(failed)?,
                ),
                "request-changes" => drop(
                    request("POST", &format!("{pull}/request-changes"), None)
                        .await
                        .map_err(failed)?,
                ),
                _ => {}
            }
            Ok(Value::Null)
        }
        "replyToThread" => {
            if body_of("body").trim().is_empty() {
                return Err(refusal("replyToThread", "A reply cannot be empty."));
            }
            repo.can_write()
                .await
                .map_err(|e| e.to_error("replyToThread", "getViewerPermissions"))?;
            let failed = |e: ApiError| e.to_error("replyToThread", "replyToThread");
            let (path, _) = repo.path().map_err(failed)?;
            let parent: Value = body_of("threadId")
                .trim()
                .parse::<f64>()
                .map(|n| json!(n as i64))
                .unwrap_or(Value::Null);
            request(
                "POST",
                &format!("{path}/pullrequests/{number}/comments"),
                Some(json!({"content": {"raw": body_of("body")}, "parent": {"id": parent}})),
            )
            .await
            .map_err(failed)?;
            Ok(Value::Null)
        }
        "setThreadResolution" => {
            repo.can_write()
                .await
                .map_err(|e| e.to_error("setThreadResolution", "getViewerPermissions"))?;
            let failed = |e: ApiError| e.to_error("setThreadResolution", "setThreadResolution");
            let (path, _) = repo.path().map_err(failed)?;
            let id = crate::thread_projection::uri_component(body_of("threadId").trim());
            let method = if input["resolved"] == true {
                "POST"
            } else {
                "DELETE"
            };
            request(
                method,
                &format!("{path}/pullrequests/{number}/comments/{id}/resolve"),
                None,
            )
            .await
            .map_err(failed)?;
            Ok(Value::Null)
        }
        "setReaction" => Err(refusal("setReaction", "This host has no reactions.")),
        "runAction" => run_action(&repo, input, number).await,
        _ => Err(json!({"_tag": "PullRequestUnavailableError", "reason": "provider-unsupported"})),
    }
}

/// `runAction` and `runActionAndInvalidate`: the action, its reply as a summary, and for a
/// merge whose reply has no merge time, the pull request read again to confirm it.
async fn run_action(repo: &Repository<'_>, input: &Value, number: u64) -> Result<Value, Value> {
    let action = input["action"].as_str().unwrap_or_default();
    if action == "delete-branch" {
        return Err(refusal(
            "runAction",
            "This host cannot delete a change request's source branch.",
        ));
    }
    if input.get("stackNumber").is_some() {
        return Err(refusal(
            "runAction",
            "This stack action is not supported or has no expected head revision.",
        ));
    }
    if !ACTIONS.contains(&action) {
        return Err(refusal(
            "runAction",
            &format!("This host cannot {action} a change request."),
        ));
    }
    if let Some(method) = input["mergeMethod"]
        .as_str()
        .filter(|m| !MERGE_METHODS.contains(m))
    {
        return Err(refusal(
            "runAction",
            &format!("This host cannot merge with the {method} strategy."),
        ));
    }
    if let Some(method) = input["updateMethod"].as_str() {
        return Err(refusal(
            "runAction",
            &format!("This host cannot update a branch by {method}."),
        ));
    }
    let can_write = repo
        .can_write()
        .await
        .map_err(|e| e.to_error("runAction", "getViewerPermissions"))?;
    if action == "merge" && !can_write {
        return Err(refusal("runAction", action_refusal(action)));
    }
    let failed = |e: ApiError| e.to_error("runAction", "runAction");
    let (path, _) = repo.path().map_err(failed)?;
    let pull = format!("{path}/pullrequests/{number}");
    let reply = if action == "merge" {
        let strategy = match input["mergeMethod"].as_str() {
            Some("squash") => "squash",
            Some("rebase") => "rebase_fast_forward",
            _ => "merge_commit",
        };
        request(
            "POST",
            &format!("{pull}/merge"),
            Some(json!({"merge_strategy": strategy})),
        )
        .await
        .map_err(failed)?
    } else {
        request("POST", &format!("{pull}/decline"), None)
            .await
            .map_err(failed)?
    };
    let observed_at = now_ms();
    // Both reply with the pull request; a merge that runs long replies with a task instead.
    let summary = serde_json::from_str::<Value>(&reply).ok().as_ref().and_then(decode_pull_request).map(|pr| {
        let mut summary = json!({"provider": "bitbucket", "projectId": repo.project.id, "repository": repo.repository,
            "number": pr.number, "title": pr.title, "url": pr.url, "state": pr.state, "isDraft": pr.is_draft,
            "headBranch": pr.head_branch, "baseBranch": pr.base_branch, "updatedAt": pr.updated_at,
            "observedAt": observed_at, "author": pr.author});
        if let Some(sha) = pr.head_sha {
            summary["headSha"] = json!(sha);
        }
        summary
    });
    if action == "merge" {
        // A merge action can merely queue the pull request; its reply has no merge time to confirm it.
        let _ = repo.change_request(number).await;
    }
    Ok(if input["replyWithSummary"] == true {
        summary.unwrap_or(Value::Null)
    } else {
        Value::Null
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn checks_keep_the_last_rerun_and_name_shared_jobs_by_workflow() {
        let check = |name: &str, status: &str| json!({"name": name, "status": status, "description": null, "url": null});
        let deduped = dedupe_checks(vec![
            ("a".into(), check("build", "success")),
            ("b".into(), check("build", "failure")),
            ("lint".into(), check("lint", "pending")),
            ("a".into(), check("build", "cancelled")),
        ]);
        let names: Vec<_> = deduped
            .iter()
            .map(|c| (c["name"].as_str().unwrap(), c["status"].as_str().unwrap()))
            .collect();
        assert_eq!(
            names,
            [
                ("a / build", "cancelled"),
                ("b / build", "failure"),
                ("lint", "pending")
            ]
        );
    }

    #[test]
    fn replies_join_their_inline_root() {
        let threads = review_threads(&[
            json!({"id": 1, "created_on": "2026-01-01T00:00:00Z", "inline": {"path": "a.rs", "to": 3}, "content": {"raw": "root"}}),
            json!({"id": 2, "created_on": "2026-01-01T00:01:00Z", "parent": {"id": 1}, "content": {"raw": "reply"}}),
            json!({"id": 3, "created_on": "2026-01-01T00:02:00Z", "content": {"raw": "general"}}),
        ]);
        assert_eq!(threads.len(), 1);
        assert_eq!(threads[0]["comments"].as_array().unwrap().len(), 2);
        assert_eq!(
            (threads[0]["side"].as_str(), threads[0]["line"].as_i64()),
            (Some("right"), Some(3))
        );
    }
}
