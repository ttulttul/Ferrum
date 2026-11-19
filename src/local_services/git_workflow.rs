use super::vcs::{branch, checked, default_ref, read};
use super::{invalid, string, Reply};
use crate::{
    provider_rpc::{valid_repository, Failure, Github},
    AppState,
};
use serde_json::{json, Value};
use std::path::Path;

mod hosts;
mod stacked;
pub use stacked::stream;

fn manager(cwd: &str, op: &str, detail: &str) -> Value {
    json!({"_tag":"GitManagerError","operation":op,"cwd":cwd,"detail":detail})
}
fn provider_error(provider: &str, op: &str, cwd: &str, reference: &str, detail: &str) -> Value {
    let safe = if let Ok(mut url) = url::Url::parse(reference) {
        let _ = url.set_password(None);
        let _ = url.set_username("");
        url.set_query(None);
        url.set_fragment(None);
        url.to_string()
    } else {
        reference.split_whitespace().collect::<Vec<_>>().join(" ")
    };
    json!({"_tag":"SourceControlProviderError","provider":provider,"operation":op,"cwd":cwd,"reference":safe.chars().take(256).collect::<String>(),"detail":detail})
}
fn github_error(error: Failure, op: &str, cwd: &str, reference: &str) -> Value {
    let detail=match error {
        Failure::Missing=>"No GitHub credential on the server. Set GH_TOKEN, or install the GitHub CLI and run `gh auth login`.",
        Failure::Auth=>"GitHub is not authenticated. Run `gh auth login` (or set GH_TOKEN) and retry.",
        Failure::NotFound=>"Pull request not found. Check the PR number or URL and try again.",
        Failure::RateLimit=>"GitHub API rate limit exceeded. Requests resume when the limit resets.",
        Failure::Invalid=>"GitHub returned an invalid pull request.",
        Failure::Refused=>"GitHub request failed."
    };
    provider_error("github", op, cwd, reference, detail)
}
fn scope(remote: &str) -> Option<String> {
    let scope = remote
        .strip_prefix("git@github.com:")
        .or_else(|| remote.strip_prefix("https://github.com/"))
        .or_else(|| remote.strip_prefix("ssh://git@github.com/"))
        .or_else(|| remote.strip_prefix("git://github.com/"))?;
    let scope = scope.trim_end_matches('/').trim_end_matches(".git");
    valid_repository(scope).then(|| scope.to_owned())
}
async fn primary(cwd: &str) -> Option<(String, Option<String>)> {
    let remotes = read(cwd, &["remote"]).await?;
    let name = remotes
        .lines()
        .find(|s| *s == "origin")
        .or_else(|| remotes.lines().next())?;
    let url = read(cwd, &["remote", "get-url", name]).await?;
    Some((name.into(), scope(&url)))
}
const FIELDS:&str="number title url baseRefName headRefName headRefOid state isDraft mergedAt closedAt updatedAt isCrossRepository headRepository{name nameWithOwner} headRepositoryOwner{login}";
async fn lookup(
    cli: &Github<'_>,
    repo: &str,
    reference: &str,
    only_open: bool,
) -> Result<Option<Value>, Failure> {
    let mut target = repo.to_owned();
    let mut selector = reference.to_owned();
    if let Ok(url) = url::Url::parse(reference) {
        let parts: Vec<_> = url.path().trim_matches('/').split('/').collect();
        if url.host_str() != Some("github.com") || parts.len() != 4 || parts[2] != "pull" {
            return Err(Failure::NotFound);
        }
        target = format!("{}/{}", parts[0], parts[1]);
        if !valid_repository(&target) {
            return Err(Failure::NotFound);
        }
        selector = parts[3].into();
    }
    let (owner, name) = target.split_once('/').ok_or(Failure::Invalid)?;
    if let Ok(number) = selector.parse::<i32>() {
        let query=format!("query PullRequestByNumber($owner:String!,$name:String!,$number:Int!){{repository(owner:$owner,name:$name){{pullRequest(number:$number){{{FIELDS}}}}}}}");
        let data = cli
            .graphql(&query, json!({"owner":owner,"name":name,"number":number}))
            .await?;
        return Ok(data["repository"]
            .get("pullRequest")
            .filter(|v| v.is_object())
            .cloned());
    }
    let (head_owner, head) = selector
        .split_once(':')
        .map_or((None, selector.as_str()), |(o, h)| (Some(o), h));
    let query=format!("query PullRequestsByHead($owner:String!,$name:String!,$head:String!,$states:[PullRequestState!]){{repository(owner:$owner,name:$name){{pullRequests(headRefName:$head,states:$states,first:50,orderBy:{{field:CREATED_AT,direction:DESC}}){{nodes{{{FIELDS}}}}}}}}}");
    for states in [json!(["OPEN"]), Value::Null] {
        let data = cli
            .graphql(
                &query,
                json!({"owner":owner,"name":name,"head":head,"states":states}),
            )
            .await?;
        let rows = data["repository"]["pullRequests"]["nodes"]
            .as_array()
            .ok_or(Failure::Invalid)?;
        if let Some(row) = rows.iter().find(|row| {
            head_owner.is_none_or(|o| {
                row["headRepositoryOwner"]["login"]
                    .as_str()
                    .is_some_and(|s| o.eq_ignore_ascii_case(s))
            })
        }) {
            return Ok(Some(row.clone()));
        }
        if only_open {
            break;
        }
    }
    Ok(None)
}
fn resolved(row: &Value) -> Option<Value> {
    Some(
        json!({"number":row["number"].as_u64()?,"title":row["title"].as_str()?.trim(),"url":row["url"].as_str()?,"baseBranch":row["baseRefName"].as_str()?,"headBranch":row["headRefName"].as_str()?,"state":row["state"].as_str()?.to_lowercase()}),
    )
}
fn fragment(input: &str) -> String {
    let mut name = String::new();
    for c in input
        .to_lowercase()
        .chars()
        .filter(|c| !matches!(c, '\'' | '"' | '`'))
    {
        let c = if c.is_ascii_alphanumeric() || matches!(c, '/' | '_') {
            c
        } else {
            '-'
        };
        if c == '-' && name.ends_with('-') || c == '/' && name.ends_with('/') {
            continue;
        }
        name.push(c);
    }
    let trimmed = name.trim_matches(['-', '_', '/']);
    let out = trimmed[..trimmed.len().min(64)].trim_end_matches(['-', '_', '/']);
    if out.is_empty() {
        "update".into()
    } else {
        out.into()
    }
}
fn fork_branch(head: &str, number: u64, branches: &[&str]) -> String {
    let fragment = fragment(head);
    let preferred = format!("t3code/pr-{number}/{fragment}");
    let conflicts = |s: &str| {
        branches
            .iter()
            .any(|b| b.starts_with(&format!("{s}/")) || s.starts_with(&format!("{b}/")))
    };
    if !conflicts(&preferred) {
        return preferred;
    }
    let flat = format!("t3code-pr-{number}-{}", fragment.replace('/', "-"));
    let mut name = flat.clone();
    for suffix in 2.. {
        if !conflicts(&name) {
            break;
        }
        name = format!("{flat}-{suffix}")
    }
    name
}
pub async fn pull_request(state: &AppState, name: &str, input: &Value) -> Reply {
    let cwd = string(input, "cwd")?;
    let reference = string(input, "reference")?;
    let reference = reference
        .strip_prefix('#')
        .filter(|s| !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit()))
        .unwrap_or(reference);
    let mode = if name == "preparePullRequestThread" {
        Some(string(input, "mode")?)
    } else {
        None
    };
    if mode.is_some_and(|m| !matches!(m, "worktree" | "local")) {
        return Err(invalid("mode", "Expected \"local\" | \"worktree\""));
    }
    let unknown = || {
        provider_error(
            "unknown",
            "getChangeRequest",
            cwd,
            reference,
            "No unknown source control provider is registered.",
        )
    };
    let (remote, repo) = primary(cwd).await.ok_or_else(unknown)?;
    let url = read(cwd, &["remote", "get-url", &remote])
        .await
        .unwrap_or_default();
    let host = hosts::Remote::parse(&url);
    let row = if let Some(host) = &host {
        host.read(cwd, reference).await?
    } else if let Some(repo) = repo {
        let bin = state.issues.lock().await.gh.clone();
        lookup(&Github::new(&bin, cwd), &repo, reference, false)
            .await
            .and_then(|v| v.ok_or(Failure::NotFound))
            .map_err(|e| github_error(e, "getChangeRequest", cwd, reference))?
    } else {
        return Err(unknown());
    };
    let pr = resolved(&row).ok_or_else(unknown)?;
    let Some(mode) = mode else {
        return Ok(json!({"pullRequest":pr}));
    };
    let head = pr["headBranch"].as_str().unwrap();
    let number = pr["number"].as_u64().unwrap();
    let branches = read(
        cwd,
        &["for-each-ref", "--format=%(refname:lstrip=2)", "refs/heads"],
    )
    .await
    .unwrap_or_default();
    let branches: Vec<_> = branches.lines().collect();
    let mut local = head.to_owned();
    let mut head_remote = Some(remote.clone());
    if row["isCrossRepository"] == true {
        if host.as_ref().is_some_and(|h| h.kind == "azure-devops") && mode == "local" {
            return Err(provider_error(
                "azure-devops",
                "checkoutChangeRequest",
                cwd,
                reference,
                "Checking out an Azure DevOps pull request from a fork is not supported.",
            ));
        }
        if mode == "local" && host.as_ref().is_some_and(|h| h.kind == "gitcafe") {
            local = format!("pr-{number}/{head}");
        } else if mode == "worktree" || host.as_ref().is_some_and(|h| h.kind == "bitbucket") {
            local = fork_branch(head, number, &branches)
        } else if row["baseRefName"] == head || default_ref(cwd).await.as_deref() == Some(head) {
            let owner = row["headRepositoryOwner"]["login"]
                .as_str()
                .filter(|s| !s.is_empty())
                .ok_or_else(|| manager(cwd, "preparePullRequestThread", "The fork is gone."))?;
            local = format!("{owner}/{head}");
        }
        head_remote = None;
        if let Some(head_repo) = row["headRepository"]["nameWithOwner"]
            .as_str()
            .filter(|s| valid_repository(s))
        {
            for line in read(cwd, &["remote", "-v"])
                .await
                .unwrap_or_default()
                .lines()
            {
                let mut fields = line.split_whitespace();
                if let (Some(name), Some(url), Some("(fetch)")) =
                    (fields.next(), fields.next(), fields.next())
                {
                    if host.as_ref().is_some_and(|h| h.matches(url, head_repo))
                        || scope(url).is_some_and(|s| s.eq_ignore_ascii_case(head_repo))
                    {
                        head_remote = Some(name.into());
                        break;
                    }
                }
            }
        }
    }
    if head.starts_with('-')
        || local.starts_with('-')
        || read(cwd, &["check-ref-format", "--branch", &local])
            .await
            .is_none()
    {
        return Err(manager(
            cwd,
            "preparePullRequestThread",
            "The pull request names an invalid head branch.",
        ));
    }
    let exists = branches.contains(&local.as_str());
    let mut path = None;
    if mode == "local" {
        if !checked(
            cwd,
            "preparePullRequestThread.trackedChanges",
            &["status", "--porcelain", "--untracked-files=no"],
            "Could not read tracked changes.",
        )
        .await?
        .is_empty()
        {
            return Err(manager(cwd,"preparePullRequestThread",&format!("This repository has uncommitted changes. Commit or stash them before checking out pull request #{number} here, or check it out in its own folder.")));
        }
    } else if exists {
        path = read(
            cwd,
            &[
                "for-each-ref",
                "--format=%(worktreepath)",
                &format!("refs/heads/{local}"),
            ],
        )
        .await
        .filter(|p| !p.is_empty());
        if let Some(p) = path.as_deref() {
            let root = read(cwd, &["rev-parse", "--show-toplevel"])
                .await
                .unwrap_or_else(|| cwd.into());
            if std::fs::canonicalize(p).ok() == std::fs::canonicalize(root).ok() {
                return Err(manager(cwd,"preparePullRequestThread","This PR branch is already checked out in the main repo. Use Local, or switch the main repo off that branch before creating a worktree thread."));
            }
        }
    }
    if mode == "local" {
        if let Some(host) = &host {
            if host.local_checkout(cwd, reference, number, &remote).await? {
                let local = branch(cwd).await.unwrap_or_else(|| head.to_owned());
                let sha = row["headRefOid"].as_str().filter(|s| {
                    (7..=64).contains(&s.len()) && s.bytes().all(|b| b.is_ascii_hexdigit())
                });
                if let Some(sha) = sha {
                    let _ = read(cwd, &["merge", "--ff-only", "--quiet", sha]).await;
                }
                let actual = read(cwd, &["rev-parse", "HEAD"]).await;
                let on_head = actual.is_some() && actual.as_deref() == sha;
                let _ = read(
                    cwd,
                    &[
                        "config",
                        "--local",
                        &format!("branch.{local}.t3-pull-request"),
                        pr["url"].as_str().unwrap(),
                    ],
                )
                .await;
                return Ok(
                    json!({"pullRequest":pr,"branch":local,"worktreePath":null,"isOnPullRequestHead":on_head}),
                );
            }
        }
    }
    let mut tracking = None;
    if let Some(remote) = head_remote {
        let spec = format!("+refs/heads/{head}:refs/remotes/{remote}/{head}");
        if read(
            cwd,
            &["fetch", "--quiet", "--no-tags", "--", &remote, &spec],
        )
        .await
        .is_some()
        {
            tracking = Some(format!("{remote}/{head}"))
        }
    }
    let target = if let Some(target) = &tracking {
        format!("refs/remotes/{target}")
    } else {
        checked(
            cwd,
            "fetchPullRef",
            &[
                "fetch",
                "--quiet",
                "--no-tags",
                "--",
                &remote,
                &format!(
                    "refs/{}/{number}/head",
                    if host.as_ref().is_some_and(|h| h.kind == "gitlab") {
                        "merge-requests"
                    } else {
                        "pull"
                    }
                ),
            ],
            "Failed to fetch pull request head.",
        )
        .await?;
        checked(
            cwd,
            "resolveCommit",
            &["rev-parse", "--verify", "FETCH_HEAD^{commit}"],
            "Failed to resolve pull request head.",
        )
        .await?
    };
    if !exists {
        checked(
            cwd,
            "createBranch",
            &["branch", "--", &local, &target],
            "git branch create failed",
        )
        .await?;
    }
    if mode == "local" {
        checked(
            cwd,
            "switchRef",
            &["switch", "--", &local],
            "git checkout failed",
        )
        .await?;
    } else if path.is_none() {
        let worktree = state
            .vcs
            .call(
                "createWorktree",
                &json!({"cwd":cwd,"refName":local,"path":null}),
                Path::new(&state.data_dir),
            )
            .await
            .unwrap()?;
        path = worktree["worktree"]["path"].as_str().map(str::to_owned);
    }
    let checkout = path.as_deref().unwrap_or(cwd);
    if exists {
        let sync = checked(
            checkout,
            "refreshCheckedOutBranch",
            &["merge", "--ff-only", "--quiet", &target],
            "Could not fast-forward the pull request branch.",
        )
        .await;
        if mode == "local" {
            sync?;
        }
    }
    if let Some(upstream) = tracking {
        let _ = read(
            checkout,
            &[
                "branch",
                &format!("--set-upstream-to={upstream}"),
                "--",
                &local,
            ],
        )
        .await;
    }
    let actual = read(checkout, &["rev-parse", "HEAD"]).await;
    let on_head = actual
        .as_deref()
        .is_some_and(|s| row["headRefOid"].as_str() == Some(s));
    let _ = read(
        cwd,
        &[
            "config",
            "--local",
            &format!("branch.{local}.t3-pull-request"),
            pr["url"].as_str().unwrap(),
        ],
    )
    .await;
    Ok(json!({"pullRequest":pr,"branch":local,"worktreePath":path,"isOnPullRequestHead":on_head}))
}
