use super::{hosts::Remote, read, read_branch_base, valid_repository, Github, FIELDS};
use crate::repository_identity::{
    parse_git_remote_host, recognize_remote, repository_key_from_remote,
};
use serde_json::{json, Value};
use std::{
    collections::VecDeque,
    path::Path,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

pub(in crate::local_services) fn provider(url: &str) -> Option<Value> {
    let (host, hostname) = parse_git_remote_host(url)?;
    let kind = recognize_remote(url)?;
    let name = match kind {
        "github" if hostname == "github.com" => "GitHub",
        "github" => "GitHub Self-Hosted",
        "gitlab" if hostname == "gitlab.com" => "GitLab",
        "gitlab" => "GitLab Self-Hosted",
        "bitbucket" if hostname == "bitbucket.org" || hostname.ends_with(".bitbucket.org") => {
            "Bitbucket"
        }
        "bitbucket" => "Bitbucket Self-Hosted",
        "forgejo" => "Forgejo",
        "azure-devops" => "Azure DevOps",
        "cursor-origin" => "Cursor Origin",
        "gitcafe" => "GitCafe",
        _ => &host,
    };
    let base = match kind {
        "cursor-origin" => "https://cursor.com/codebase".into(),
        "bitbucket" if name == "Bitbucket" => "https://bitbucket.org".into(),
        "forgejo" if url.starts_with("http://") => format!("http://{host}"),
        _ => format!("https://{host}"),
    };
    Some(json!({"kind":kind,"name":name,"baseUrl":base}))
}

#[derive(Default)]
struct Entry {
    expires: Option<Instant>,
    row: Option<Value>,
    failures: u32,
}

type Slot = Arc<tokio::sync::Mutex<Entry>>;

#[derive(Default)]
pub(in crate::local_services) struct Cache {
    entries: Mutex<VecDeque<(Key, Slot)>>,
    snapshots: Mutex<VecDeque<(String, Option<String>, Value)>>,
}

const CAPACITY: usize = 2048;

#[derive(Clone, PartialEq, Eq)]
struct Key {
    cwd: String,
    local: String,
    head: String,
    target: String,
    source: Option<String>,
    default: Option<String>,
}

impl Cache {
    pub(in crate::local_services) fn snapshot(
        &self,
        cwd: &str,
        branch: Option<&str>,
    ) -> Option<Value> {
        self.snapshots
            .lock()
            .unwrap()
            .iter()
            .rev()
            .find(|(c, b, _)| c == cwd && b.as_deref() == branch)
            .map(|(_, _, v)| v.clone())
    }
    pub(in crate::local_services) fn remember(
        &self,
        cwd: &str,
        branch: Option<String>,
        value: Value,
    ) {
        let mut rows = self.snapshots.lock().unwrap();
        rows.retain(|(c, _, _)| c != cwd);
        if rows.len() == CAPACITY {
            rows.pop_front();
        }
        rows.push_back((cwd.into(), branch, value));
    }
    async fn latest(&self, key: Key, source_url: &str, refresh: bool) -> Option<Value> {
        let slot = {
            let mut rows = self.entries.lock().unwrap();
            if let Some((_, slot)) = rows.iter().find(|(k, _)| k == &key) {
                slot.clone()
            } else {
                if rows.len() == CAPACITY {
                    rows.pop_front();
                }
                let slot = Slot::default();
                rows.push_back((key.clone(), slot.clone()));
                slot
            }
        };
        let mut entry = slot.lock().await;
        if refresh || entry.expires.is_none_or(|t| t <= Instant::now()) {
            let ttl = match key.lookup(source_url).await {
                Ok(result) => {
                    entry.failures = 0;
                    entry.row = result;
                    if entry.row.as_ref().is_some_and(|r| r["state"] == "open") {
                        60
                    } else {
                        300
                    }
                }
                Err(()) => {
                    let seconds = (20u64 << entry.failures.min(6)).min(900);
                    entry.failures += 1;
                    seconds
                }
            };
            entry.expires = Some(Instant::now() + Duration::from_secs(ttl));
        }
        entry.row.clone()
    }

    pub(in crate::local_services) async fn pull_request(
        &self,
        cwd: &str,
        branch: &str,
        upstream: Option<&str>,
        default: Option<&str>,
        remotes: &str,
        refresh: bool,
    ) -> Option<Value> {
        let primary = remotes
            .lines()
            .find(|r| *r == "origin")
            .or_else(|| remotes.lines().next())?;
        let target_url = read(cwd, &["remote", "get-url", primary]).await?;
        let kind = recognize_remote(&target_url)?;
        if kind == "unknown" {
            return None;
        }
        let mut remote = read(
            cwd,
            &["config", "--get", &format!("branch.{branch}.remote")],
        )
        .await
        .filter(|r| r != ".");
        let mut head = remote
            .as_ref()
            .and_then(|r| upstream?.strip_prefix(&format!("{r}/")))
            .unwrap_or(branch)
            .to_owned();
        let published = if upstream.is_none() || head != branch {
            let refs = read(
                cwd,
                &[
                    "for-each-ref",
                    "--format=%(refname)%00%(objectname)",
                    "refs/remotes",
                ],
            )
            .await?;
            let matches = remotes
                .lines()
                .filter_map(|r| {
                    let wanted = format!("refs/remotes/{r}/{branch}");
                    refs.lines()
                        .filter_map(|s| s.split_once('\0'))
                        .find(|(name, _)| *name == wanted)
                        .map(|(_, oid)| (r, oid.to_owned()))
                })
                .collect::<Vec<_>>();
            if upstream.is_none() {
                if matches.is_empty()
                    && !refs.is_empty()
                    && (remote.is_none()
                        || read(cwd, &["config", "--get", &format!("branch.{branch}.merge")])
                            .await
                            .is_none())
                {
                    return None;
                }
                let local = read(cwd, &["rev-parse", "HEAD"]).await?;
                let exact: Vec<_> = matches.iter().filter(|(_, id)| id == &local).collect();
                let inferred = match exact.as_slice() {
                    [one] => Some(one.0),
                    [] => {
                        let mut ancestors = vec![];
                        for (r, id) in &matches {
                            if read(cwd, &["merge-base", "--is-ancestor", id, &local])
                                .await
                                .is_some()
                            {
                                ancestors.push(*r);
                            }
                        }
                        if ancestors.len() == 1 {
                            Some(ancestors[0])
                        } else {
                            None
                        }
                    }
                    _ => None,
                };
                if let Some(r) = inferred {
                    remote = Some(r.into());
                }
            }
            matches.into_iter().map(|(r, _)| r).collect::<Vec<_>>()
        } else {
            vec![]
        };
        let target = if kind == "github" {
            github_target(cwd, remotes, &target_url).await?
        } else {
            repository_key_from_remote(&target_url)
        };
        let mut source = match remote.as_deref() {
            Some(r) => read(cwd, &["remote", "get-url", r])
                .await
                .map(|s| repository_key_from_remote(&s)),
            None => None,
        };
        let tracks_fork_head = source
            .as_ref()
            .is_some_and(|s| !s.eq_ignore_ascii_case(&target))
            && (remote.as_deref() != Some("origin")
                || branch.starts_with("t3code/pr-")
                || branch.starts_with("t3code-pr-"));
        if head != branch
            && !tracks_fork_head
            && (default.map_or(matches!(head.as_str(), "main" | "master"), |s| s == head)
                || read_branch_base(cwd, branch).await.as_deref() == Some(head.as_str()))
        {
            let r = published
                .iter()
                .find(|r| Some(**r) == remote.as_deref())
                .or_else(|| published.iter().find(|r| **r == "origin"))
                .or_else(|| published.first())?;
            head = branch.into();
            source = read(cwd, &["remote", "get-url", r])
                .await
                .map(|s| repository_key_from_remote(&s));
        }
        self.latest(
            Key {
                cwd: cwd.into(),
                local: branch.into(),
                head,
                default: default.map(str::to_owned),
                target,
                source,
            },
            &target_url,
            refresh,
        )
        .await
    }
}

async fn github_target(cwd: &str, remotes: &str, url: &str) -> Option<String> {
    let (host, _) = parse_git_remote_host(url)?;
    if let Ok(repo) = std::env::var("GH_REPO") {
        let repo = repo.strip_prefix(&format!("{host}/")).unwrap_or(&repo);
        if valid_repository(repo) {
            return Some(format!("{host}/{repo}"));
        }
    }
    let mut best = None;
    for r in remotes.lines() {
        let Some(url) = read(cwd, &["remote", "get-url", r]).await else {
            continue;
        };
        if parse_git_remote_host(&url)
            .as_ref()
            .is_none_or(|(h, _)| h != &host)
        {
            continue;
        }
        let repo = repository_key_from_remote(&url);
        if let Some(mark) = read(
            cwd,
            &["config", "--get", &format!("remote.{r}.gh-resolved")],
        )
        .await
        {
            if mark == "base" {
                return Some(repo);
            }
            if valid_repository(&mark) {
                return Some(format!("{host}/{mark}"));
            }
        }
        let rank = ["origin", "github", "upstream"]
            .iter()
            .position(|s| r.eq_ignore_ascii_case(s))
            .map_or(0, |i| i + 1);
        if best.as_ref().is_none_or(|(prev, _)| rank > *prev) {
            best = Some((rank, repo));
        }
    }
    best.map(|(_, r)| r)
}

impl Key {
    async fn lookup(&self, url: &str) -> Result<Option<Value>, ()> {
        let host = Remote::parse(url);
        let limit = if host
            .as_ref()
            .is_none_or(|h| matches!(h.kind, "gitlab" | "azure-devops"))
        {
            100
        } else {
            20
        };
        let mut latest = None;
        for open in [false, true] {
            let rows = if let Some(remote) = &host {
                remote
                    .list(&self.cwd, &self.head, open)
                    .await
                    .map_err(|_| ())?
            } else {
                if recognize_remote(url) != Some("github") {
                    return Ok(None);
                }
                self.github_rows(open).await?
            };
            let full = rows.len() >= limit;
            let mut matches: Vec<_> = rows
                .into_iter()
                .filter(|row| self.matches(row))
                .filter_map(|row| status_row(&row, host.is_none()))
                .collect();
            matches.sort_by(|a, b| b["updatedAt"].as_str().cmp(&a["updatedAt"].as_str()));
            if let Some(row) = matches.iter().find(|r| r["state"] == "open") {
                return Ok(Some(row.clone()));
            }
            if !open {
                latest = matches.into_iter().next();
            }
            if !full {
                break;
            }
        }
        Ok(latest)
    }
    async fn github_rows(&self, open: bool) -> Result<Vec<Value>, ()> {
        let (hostname, repo) = self.target.split_once('/').ok_or(())?;
        let (owner, name) = repo.split_once('/').ok_or(())?;
        let gh = Github::at_host(Path::new("gh"), &self.cwd, hostname);
        let owner_head = self
            .source
            .as_deref()
            .and_then(|s| s.split_once('/'))
            .and_then(|(_, r)| r.split('/').next())
            .filter(|_| open || self.default.as_deref() == Some(&self.head));
        if let Some(head_owner) = owner_head {
            let selector = format!("{head_owner}:{}", self.head);
            let state = if open { "open" } else { "all" };
            let query = url::form_urlencoded::Serializer::new(String::new())
                .append_pair("head", &selector)
                .append_pair("state", state)
                .append_pair("sort", "created")
                .append_pair("direction", "desc")
                .append_pair("per_page", "100")
                .finish();
            let data = gh
                .request(
                    "GET",
                    &format!("repos/{repo}/pulls?{query}"),
                    "application/vnd.github+json",
                    None,
                )
                .await
                .map_err(|_| ())?;
            let rows: Vec<Value> = serde_json::from_slice(&data).map_err(|_| ())?;
            return Ok(rows.into_iter().map(|r| json!({"number":r["number"],"title":r["title"],"url":r["html_url"],"baseRefName":r["base"]["ref"],
                "headRefName":r["head"]["ref"],"headRefOid":r["head"]["sha"],"isDraft":r["draft"],"updatedAt":r["updated_at"],
                "state":if r["merged_at"].is_string() {json!("merged")} else {r["state"].clone()},
                "isCrossRepository":r["head"]["repo"]["full_name"]!=r["base"]["repo"]["full_name"],
                "headRepository":{"nameWithOwner":r["head"]["repo"]["full_name"]},"headRepositoryOwner":r["head"]["repo"]["owner"]})).collect());
        }
        let query = format!("query PullRequestsByHead($owner:String!,$name:String!,$head:String!,$states:[PullRequestState!]){{repository(owner:$owner,name:$name){{pullRequests(headRefName:$head,states:$states,first:100,orderBy:{{field:CREATED_AT,direction:DESC}}){{nodes{{{FIELDS}}}}}}}}}");
        let result = gh.graphql(&query,json!({"owner":owner,"name":name,"head":self.head,"states":if open {json!(["OPEN"])} else {Value::Null}})).await.map_err(|_|())?;
        let nodes = &result["repository"]["pullRequests"]["nodes"];
        if nodes.is_null() && result["repository"]["pullRequests"].is_object() {
            return Ok(vec![]);
        }
        nodes.as_array().cloned().ok_or(())
    }
    fn matches(&self, row: &Value) -> bool {
        if row["headRefName"] != self.head {
            return false;
        }
        let repo = self
            .source
            .as_deref()
            .and_then(|s| s.split_once('/'))
            .map(|(_, r)| r);
        let owner = repo.and_then(|r| r.split('/').next());
        let got = row["headRepository"]["nameWithOwner"].as_str();
        let got_owner = row["headRepositoryOwner"]["login"]
            .as_str()
            .or_else(|| got?.split('/').next());
        if repo
            .zip(got)
            .is_some_and(|(a, b)| !a.eq_ignore_ascii_case(b))
            || owner
                .zip(got_owner)
                .is_some_and(|(a, b)| !a.eq_ignore_ascii_case(b))
        {
            return false;
        }
        let cross = self
            .source
            .as_ref()
            .is_some_and(|s| !s.eq_ignore_ascii_case(&self.target));
        if cross {
            return row["isCrossRepository"] != false && (got.is_some() || got_owner.is_some());
        }
        row["isCrossRepository"] != true
            || (repo.is_some() && (got.is_some() || got_owner.is_some()))
    }
}

fn status_row(row: &Value, github: bool) -> Option<Value> {
    let state = row["state"].as_str()?.to_ascii_lowercase();
    let updated = row["updatedAt"]
        .as_str()
        .and_then(|s| {
            time::OffsetDateTime::parse(s, &time::format_description::well_known::Rfc3339).ok()
        })
        .map(|d| crate::store::iso(d.to_offset(time::UtcOffset::UTC)));
    if github && updated.is_none() {
        return None;
    }
    let mut pr = json!({"number":row["number"].as_u64()?,"title":row["title"].as_str()?.trim(),"url":row["url"].as_str()?.trim(),"baseRef":row["baseRefName"].as_str()?.trim(),"headRef":row["headRefName"].as_str()?.trim(),"state":state,"updatedAt":updated,"headSha":row["headRefOid"]});
    if row["isDraft"] == true {
        pr["isDraft"] = json!(true);
    }
    Some(pr)
}
