//! Bitbucket pull requests for `pullRequests.list`, as the reference's
//! `BitbucketPullRequestApi` (viewer and listing) and `PullRequestService.list`
//! read them.
//!
//! The API is `T3CODE_BITBUCKET_API_BASE_URL` (Bitbucket Cloud by default), with
//! the credential `bitbucket_credential` reads for each request (settings, then the
//! `T3CODE_BITBUCKET_*` environment variables). A repository is listed newest
//! first, 50 a page and at most ten pages, continuing from the shared list cursor;
//! next-page links are followed only on the configured API's origin.
use super::listing::Cursor;
use serde_json::{json, Value};
use std::time::Duration;

const MAX_PAGE_SIZE: usize = 50;
const MAX_LIST_PAGES: usize = 10;
/// A filter Bitbucket cannot apply reads at most this many slices (`FILTERED_LIST_READS`).
const FILTERED_LIST_READS: usize = 5;

pub(super) const UNAUTHENTICATED: &str =
    "Bitbucket rejected the configured credentials. Check them in Settings → Source control & issues.";

/// The viewer, or why it could not be read (`bitbucketProviderFailure`): `unauthenticated` for
/// HTTP 401, otherwise an operation failure with the API's detail.
pub(super) enum Viewer {
    Known(String),
    Unauthenticated,
    Failed { detail: String, not_found: bool },
}

pub(super) fn base_url() -> String {
    std::env::var("T3CODE_BITBUCKET_API_BASE_URL")
        .unwrap_or_else(|_| "https://api.bitbucket.org/2.0".into())
}

pub(super) fn origin(url: &str) -> Option<String> {
    let parsed = url::Url::parse(url).ok()?;
    Some(parsed.origin().ascii_serialization())
}

/// Why a request failed, as the reference names it.
enum Failure {
    Send,
    Status(u16),
    Untrusted,
    /// A secret reference in the Bitbucket settings could not be read (`SettingsSecretUnresolvedError`).
    Unresolved,
}

impl Failure {
    fn detail(&self) -> String {
        match self {
            Failure::Send => "Failed to send the Bitbucket request.".into(),
            Failure::Status(status) => format!("Bitbucket returned HTTP {status}."),
            Failure::Untrusted => "Bitbucket returned a link to another host.".into(),
            Failure::Unresolved => "The Bitbucket credential could not be read.".into(),
        }
    }
}

/// A GET of `url` (a path under the API, or a link on its origin), with the credential.
async fn get(url: &str) -> Result<String, Failure> {
    let base = base_url();
    let url = if url.starts_with("http://") || url.starts_with("https://") {
        if origin(url).is_none() || origin(url) != origin(&base) {
            return Err(Failure::Untrusted);
        }
        url.to_owned()
    } else {
        format!("{}{url}", base.trim_end_matches('/'))
    };
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .map_err(|_| Failure::Send)?;
    let mut request = client.get(&url).header("Accept", "application/json");
    match crate::bitbucket_credential::current().await {
        Ok(Some(credential)) => request = credential.apply(request),
        Ok(None) => {}
        Err(_) => return Err(Failure::Unresolved),
    }
    let response = request.send().await.map_err(|_| Failure::Send)?;
    let status = response.status().as_u16();
    if !(200..300).contains(&status) {
        return Err(Failure::Status(status));
    }
    response.text().await.map_err(|_| Failure::Send)
}

pub(super) fn trimmed(value: &Value) -> Option<String> {
    value
        .as_str()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_owned)
}

/// Viewers read in the last ten minutes, by host (`viewersByHost`); failures are not kept.
static VIEWERS: std::sync::LazyLock<
    std::sync::Mutex<std::collections::HashMap<String, (std::time::Instant, String)>>,
> = std::sync::LazyLock::new(Default::default);
const VIEWER_CACHE_TTL: Duration = Duration::from_secs(10 * 60);

/// `resolveViewers` for a Bitbucket host: the viewer kept from the last ten minutes, or a fresh one.
pub(super) async fn viewer_for(host: &str) -> Viewer {
    if let Some((at, name)) = VIEWERS.lock().unwrap().get(host) {
        if at.elapsed() <= VIEWER_CACHE_TTL {
            return Viewer::Known(name.clone());
        }
    }
    let viewer = viewer().await;
    if let Viewer::Known(name) = &viewer {
        VIEWERS
            .lock()
            .unwrap()
            .insert(host.to_owned(), (std::time::Instant::now(), name.clone()));
    }
    viewer
}

/// A whole refresh (`pullRequests.invalidate` without a reference) forgets the viewers.
pub(crate) fn forget_viewers() {
    VIEWERS.lock().unwrap().clear();
}

/// `getViewer`: the account's nickname, or its display name.
async fn viewer() -> Viewer {
    match get("/user").await {
        // An unreadable secret is `unauthenticated`, as a rejected credential is.
        Err(Failure::Status(401) | Failure::Unresolved) => Viewer::Unauthenticated,
        Err(failure) => Viewer::Failed {
            detail: failure.detail(),
            not_found: matches!(failure, Failure::Status(404)),
        },
        Ok(body) => match serde_json::from_str::<Value>(&body) {
            Ok(raw) if raw.is_object() && optional_strings(&raw, &["nickname", "display_name"]) => {
                match trimmed(&raw["nickname"]).or_else(|| trimmed(&raw["display_name"])) {
                    Some(name) => Viewer::Known(name),
                    None => Viewer::Failed {
                        detail:
                            "Bitbucket returned no account name for the configured credentials."
                                .into(),
                        not_found: false,
                    },
                }
            }
            _ => Viewer::Failed {
                detail: "Bitbucket returned an unreadable getViewer response.".into(),
                not_found: false,
            },
        },
    }
}

pub(super) fn optional_strings(raw: &Value, keys: &[&str]) -> bool {
    keys.iter()
        .all(|k| raw.get(*k).is_none_or(|v| v.is_null() || v.is_string()))
}

/// `toIsoUtc`: an instant as UTC with milliseconds, or the text as it came.
pub(super) fn iso_utc(value: &str) -> String {
    use time::format_description::well_known::Rfc3339;
    match time::OffsetDateTime::parse(value, &Rfc3339) {
        Ok(at) => {
            let at = at.to_offset(time::UtcOffset::UTC);
            format!(
                "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}.{:03}Z",
                at.year(),
                at.month() as u8,
                at.day(),
                at.hour(),
                at.minute(),
                at.second(),
                at.millisecond()
            )
        }
        Err(_) => value.to_owned(),
    }
}

/// `toActor`.
pub(super) fn actor(raw: &Value) -> Option<Value> {
    let login = trimmed(&raw["nickname"]).or_else(|| trimmed(&raw["display_name"]))?;
    Some(
        json!({"login": login, "name": trimmed(&raw["display_name"]), "avatarUrl": trimmed(&raw["links"]["avatar"]["href"])}),
    )
}

pub(super) fn user_ok(raw: &Value) -> bool {
    raw.is_null()
        || raw.is_object()
            && optional_strings(raw, &["uuid", "nickname", "display_name"])
            && (raw["links"].is_null()
                || raw["links"].is_object()
                    && (raw["links"]["avatar"].is_null()
                        || raw["links"]["avatar"].is_object()
                            && raw["links"]["avatar"]
                                .get("href")
                                .is_none_or(Value::is_string)))
}

pub(super) fn branch(raw: &Value) -> Option<String> {
    let commit_ok = raw["commit"].is_null()
        || raw["commit"].is_object()
            && raw["commit"]
                .get("hash")
                .is_none_or(|h| h.is_null() || h.is_string());
    let repository_ok =
        raw["repository"].is_null() || trimmed(&raw["repository"]["full_name"]).is_some();
    (commit_ok && repository_ok)
        .then(|| trimmed(&raw["branch"]["name"]))
        .flatten()
}

/// A listed pull request (`RawPullRequestSchema` and `toPullRequest`): what an entry needs.
pub(super) struct PullRequest {
    pub number: u64,
    pub title: String,
    pub url: String,
    pub author: Option<Value>,
    pub head_branch: String,
    pub base_branch: String,
    pub state: &'static str,
    pub is_draft: bool,
    pub created_at: String,
    pub updated_at: String,
    pub review_request_logins: Vec<String>,
}

pub(super) fn pull_request(raw: &Value) -> Option<PullRequest> {
    let number = raw["id"].as_u64()?;
    let title = raw["title"].as_str()?.to_owned();
    if !optional_strings(raw, &["description", "state", "closed_on"])
        || raw.get("draft").is_some_and(|d| !d.is_boolean())
        || !user_ok(&raw["author"])
        || !(raw["reviewers"].is_null()
            || raw["reviewers"]
                .as_array()
                .is_some_and(|r| r.iter().all(user_ok)))
        || !(raw["participants"].is_null() || raw["participants"].is_array())
    {
        return None;
    }
    let head_branch = branch(&raw["source"])?;
    let base_branch = branch(&raw["destination"])?;
    let created = raw["created_on"].as_str()?;
    let updated = raw["updated_on"].as_str()?;
    let url = trimmed(&raw["links"]["html"]["href"])?;
    let state = match raw["state"]
        .as_str()
        .map(|s| s.trim().to_uppercase())
        .as_deref()
    {
        Some("MERGED") => "merged",
        Some("DECLINED" | "SUPERSEDED") => "closed",
        _ => "open",
    };
    let review_request_logins = raw["reviewers"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(actor)
        .filter_map(|a| a["login"].as_str().map(str::to_owned))
        .collect();
    Some(PullRequest {
        number,
        title,
        url,
        author: actor(&raw["author"]),
        head_branch,
        base_branch,
        state,
        is_draft: raw["draft"].as_bool().unwrap_or(false),
        created_at: iso_utc(created),
        updated_at: iso_utc(updated),
        review_request_logins,
    })
}

/// `filterLiteral`: the backslash first, or escaping the quote would leave a live quote.
fn literal(value: &str) -> String {
    value.replace('\\', "\\\\").replace('"', "\\\"")
}

/// `updatedBeforeFilter`: Bitbucket keeps microseconds, so take the cursor's whole millisecond.
fn updated_before(boundary: &str) -> String {
    use time::format_description::well_known::Rfc3339;
    match time::OffsetDateTime::parse(boundary, &Rfc3339) {
        Ok(at) => format!(
            "updated_on < {}",
            iso_utc(
                &(at + time::Duration::milliseconds(1))
                    .format(&Rfc3339)
                    .unwrap_or_default()
            )
        ),
        Err(_) => format!("updated_on <= {boundary}"),
    }
}

/// `listPullRequests`: a repository's pull requests in `state`, newest first, from `cursor`.
async fn list_page(
    repository: &str,
    input: &Value,
    viewer: &str,
    cursor: Option<&Cursor>,
    limit: usize,
) -> Result<(Vec<PullRequest>, bool), ()> {
    let segments: Vec<&str> = repository
        .split('/')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .collect();
    let [workspace, slug] = segments.as_slice() else {
        return Err(());
    };
    let path = format!(
        "/repositories/{}/{}",
        crate::thread_projection::uri_component(workspace),
        crate::thread_projection::uri_component(slug)
    );
    let states: &[&str] = match input["state"].as_str() {
        Some("open") => &["OPEN"],
        Some("merged") => &["MERGED"],
        Some("closed") => &["DECLINED", "SUPERSEDED"],
        _ => &["OPEN", "MERGED", "DECLINED", "SUPERSEDED"],
    };
    let mut predicates = Vec::new();
    if let Some(search) = input["query"]
        .as_str()
        .map(str::trim)
        .filter(|s| !s.is_empty())
    {
        let l = literal(search);
        predicates.push(format!("(title ~ \"{l}\" OR description ~ \"{l}\")"));
    }
    if let Some(author) = input["filters"]["author"].as_str() {
        let l = literal(if author == "@me" { viewer } else { author });
        predicates.push(format!(
            "(author.nickname = \"{l}\" OR author.display_name = \"{l}\")"
        ));
    }
    if let Some(draft) = input["filters"]["draft"].as_str() {
        predicates.push(format!("draft = {}", draft == "only"));
    }
    if let Some(cursor) = cursor {
        predicates.push(updated_before(&cursor.before));
        predicates.extend(cursor.seen.iter().map(|n| format!("id != {n}")));
    }
    let mut url = format!(
        "{path}/pullrequests?{}&pagelen={MAX_PAGE_SIZE}&sort=-updated_on&fields=%2Bvalues.reviewers",
        states.iter().map(|s| format!("state={s}")).collect::<Vec<_>>().join("&")
    );
    if !predicates.is_empty() {
        url.push_str(&format!(
            "&q={}",
            crate::thread_projection::uri_component(&predicates.join(" AND "))
        ));
    }
    let mut collected = Vec::new();
    for page in 1.. {
        let body = get(&url).await.map_err(|_| ())?;
        let raw: Value = serde_json::from_str(&body).map_err(|_| ())?;
        let values = raw["values"].as_array().ok_or(())?;
        if !(raw["next"].is_null() || raw["next"].is_string()) {
            return Err(());
        }
        collected.extend(values.iter().filter_map(pull_request));
        let next = trimmed(&raw["next"]);
        match next {
            Some(next) if collected.len() < limit && page < MAX_LIST_PAGES => url = next,
            next => {
                let truncated = next.is_some() || collected.len() > limit;
                collected.truncate(limit);
                return Ok((collected, truncated));
            }
        }
    }
    unreachable!()
}

/// `matchesRowFilters` for a Bitbucket row: no labels, review decision or checks state.
fn matches_filters(item: &PullRequest, filters: &Value, viewer: &str) -> bool {
    if !filters.is_object() {
        return true;
    }
    let draft_ok = filters["draft"]
        .as_str()
        .is_none_or(|d| item.is_draft == (d == "only"));
    // A row with no labels holds no label group.
    let labels_ok = filters["labels"]
        .as_array()
        .is_none_or(|groups| groups.is_empty());
    let author_ok = filters["author"].as_str().is_none_or(|a| {
        let wanted = if a == "@me" { viewer } else { a };
        item.author
            .as_ref()
            .and_then(|x| x["login"].as_str())
            .is_some_and(|l| l.eq_ignore_ascii_case(wanted))
    });
    draft_ok && labels_ok && author_ok
}

/// A repository's slice of the list: entries, whether more remain, and the cursor to them.
pub(super) struct Batch {
    pub entries: Vec<Value>,
    pub truncated: bool,
    pub next_cursor: Option<String>,
    pub failed: bool,
}

/// `listCursorAt`.
fn cursor_at(
    previous: Option<&Cursor>,
    boundary: &str,
    fetched: &[PullRequest],
    delivered: usize,
) -> String {
    let mut seen: Vec<u64> = previous
        .filter(|c| c.before == boundary)
        .map(|c| c.seen.clone())
        .unwrap_or_default();
    for item in fetched.iter().filter(|i| i.updated_at == boundary) {
        if !seen.contains(&item.number) {
            seen.push(item.number);
        }
    }
    let count = previous.map_or(0, |c| c.delivered) + delivered as u64;
    format!(
        "{boundary}|{count}|{}",
        seen.iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join(",")
    )
}

/// `readSlice`: one read of the repository from `cursor`.
async fn read_slice(target: &Target<'_>, cursor: Option<&Cursor>) -> Batch {
    let Ok((fetched, truncated)) = list_page(
        target.repository,
        target.input,
        target.viewer,
        cursor,
        target.limit,
    )
    .await
    else {
        return Batch {
            entries: vec![],
            truncated: false,
            next_cursor: None,
            failed: true,
        };
    };
    let delivered: Vec<&PullRequest> = fetched
        .iter()
        .filter(|item| {
            cursor.is_none_or(|c| item.updated_at != c.before || !c.seen.contains(&item.number))
        })
        .collect();
    let viewer = target.viewer.to_lowercase();
    let entries = delivered
        .iter()
        .filter(|item| matches_filters(item, &target.input["filters"], target.viewer))
        .map(|item| {
            let requested = item.author.as_ref().and_then(|a| a["login"].as_str()).map(str::to_lowercase).as_deref() != Some(viewer.as_str())
                && item.review_request_logins.iter().any(|l| l.to_lowercase() == viewer);
            json!({"provider": "bitbucket", "host": target.host, "projectId": target.project_id,
                "projectTitle": target.project_title, "repository": target.repository, "number": item.number,
                "title": item.title, "url": item.url, "author": item.author, "headBranch": item.head_branch,
                "baseBranch": item.base_branch, "state": item.state, "isDraft": item.is_draft,
                "mergeability": "unknown", "additions": 0, "deletions": 0, "createdAt": item.created_at,
                "updatedAt": item.updated_at, "observedAt": target.observed_at,
                "viewerReviewRequested": requested, "labels": []})
        })
        .collect();
    // Use every fetched row: a slice of only duplicates must still move to older rows.
    let next_cursor = (truncated && !fetched.is_empty()).then(|| {
        let oldest = fetched
            .iter()
            .map(|i| i.updated_at.as_str())
            .min()
            .unwrap_or_default()
            .to_owned();
        cursor_at(cursor, &oldest, &fetched, delivered.len())
    });
    Batch {
        entries,
        truncated,
        next_cursor,
        failed: false,
    }
}

/// What a repository read needs.
pub(super) struct Target<'a> {
    pub host: &'a str,
    pub repository: &'a str,
    pub project_id: &'a str,
    pub project_title: &'a str,
    pub input: &'a Value,
    pub viewer: &'a str,
    pub limit: usize,
    pub observed_at: u64,
}

/// `readRepository`: a slice, then more while a filter thins it below a page (bounded).
pub(super) async fn read_repository(target: &Target<'_>, cursor: Option<&Cursor>) -> Batch {
    let mut batch = read_slice(target, cursor).await;
    let mut reads = 1;
    while target.input["filters"].is_object()
        && batch.entries.len() < target.limit
        && reads < FILTERED_LIST_READS
    {
        let Some(next) = batch
            .next_cursor
            .as_deref()
            .and_then(super::listing::cursor)
        else {
            break;
        };
        let more = read_slice(target, Some(&next)).await;
        // A slice that failed keeps the cursor before it, so the reader can try again.
        if more.failed {
            break;
        }
        let mut entries = std::mem::take(&mut batch.entries);
        entries.extend(more.entries);
        batch = Batch { entries, ..more };
        reads += 1;
    }
    batch
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn instants_become_utc_milliseconds() {
        assert_eq!(
            iso_utc("2026-11-20T10:00:00.123456+00:00"),
            "2026-11-20T10:00:00.123Z"
        );
        assert_eq!(
            iso_utc("2026-11-20T12:00:00+02:00"),
            "2026-11-20T10:00:00.000Z"
        );
        assert_eq!(iso_utc("yesterday"), "yesterday");
        assert_eq!(
            updated_before("2026-11-20T10:00:00.123Z"),
            "updated_on < 2026-11-20T10:00:00.124Z"
        );
    }

    #[test]
    fn pull_requests_need_their_branches_and_link() {
        let raw = json!({"id": 7, "title": "Fix", "state": "DECLINED", "source": {"branch": {"name": " feature "}},
            "destination": {"branch": {"name": "main"}}, "created_on": "2026-11-20T10:00:00+00:00",
            "updated_on": "2026-11-21T10:00:00+00:00", "links": {"html": {"href": "https://example.invalid/pr/7"}},
            "author": {"display_name": "Fixture"}, "reviewers": [{"nickname": "viewer"}]});
        let item = pull_request(&raw).unwrap();
        assert_eq!(
            (
                item.state,
                item.head_branch.as_str(),
                item.review_request_logins.len()
            ),
            ("closed", "feature", 1)
        );
        assert_eq!(item.author.unwrap()["login"], "Fixture");
        let mut broken = raw.clone();
        broken["links"] = json!({});
        assert!(pull_request(&broken).is_none());
    }
}
