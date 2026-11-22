use super::{read, remote_default_branch};

/// The name shown to the client and the fully qualified ref Git must compare.
type Base = (String, String);

async fn exists(cwd: &str, full: &str) -> bool {
    read(cwd, &["show-ref", "--verify", "--quiet", full])
        .await
        .is_some()
}

async fn remote_base(cwd: &str, name: &str) -> Option<Base> {
    let full = format!("refs/remotes/{name}");
    exists(cwd, &full).await.then(|| (name.into(), full))
}

async fn tracked(cwd: &str, name: &str) -> Option<Base> {
    let full = read(
        cwd,
        &[
            "rev-parse",
            "--symbolic-full-name",
            "--end-of-options",
            &format!("{name}@{{upstream}}"),
        ],
    )
    .await?;
    Some((full.strip_prefix("refs/remotes/")?.to_owned(), full))
}

async fn config(cwd: &str, branch: &str, key: &str) -> Option<String> {
    read(cwd, &["config", "--get", &format!("branch.{branch}.{key}")]).await
}

pub(in crate::local_services) async fn read_branch_base(cwd: &str, branch: &str) -> Option<String> {
    match config(cwd, branch, "t3-merge-base").await {
        Some(name) => Some(name),
        None => config(cwd, branch, "gh-merge-base").await,
    }
}

pub(in crate::local_services) async fn record_worktree_base(cwd: &str, branch: &str, base: &str) {
    let remotes = read(cwd, &["remote"]).await.unwrap_or_default();
    let primary = remotes
        .lines()
        .find(|r| *r == "origin")
        .or_else(|| remotes.lines().next());
    let full = match read(
        cwd,
        &[
            "rev-parse",
            "--symbolic-full-name",
            "--verify",
            "--end-of-options",
            base,
        ],
    )
    .await
    {
        Some(full) => full,
        None => match primary {
            Some(remote) if exists(cwd, &format!("refs/remotes/{remote}/{base}")).await => {
                format!("refs/remotes/{remote}/{base}")
            }
            _ => return,
        },
    };
    let name = if let Some(name) = full.strip_prefix("refs/heads/") {
        name
    } else if let Some(remote_ref) = full.strip_prefix("refs/remotes/") {
        let Some(remote) = remotes
            .lines()
            .filter(|r| remote_ref.starts_with(&format!("{r}/")))
            .max_by_key(|r| r.len())
        else {
            return;
        };
        let _ = read(
            cwd,
            &[
                "config",
                &format!("branch.{branch}.t3-merge-base-ref"),
                &full,
            ],
        )
        .await;
        &remote_ref[remote.len() + 1..]
    } else {
        return;
    };
    let _ = read(
        cwd,
        &["config", &format!("branch.{branch}.t3-merge-base"), name],
    )
    .await;
}

pub(in crate::local_services) async fn branch_base(
    cwd: &str,
    branch: Option<&str>,
    allow_current: bool,
) -> Option<Base> {
    let branch = branch?;
    if let Some(full) = config(cwd, branch, "t3-merge-base-ref").await {
        if (full.starts_with("refs/remotes/") || full.starts_with("refs/heads/"))
            && exists(cwd, &full).await
        {
            let short = full.trim_start_matches("refs/remotes/");
            let label = if exists(cwd, &format!("refs/heads/{short}")).await {
                &full
            } else {
                short
            };
            return Some((label.to_owned(), full));
        }
    }
    let configured = read_branch_base(cwd, branch).await;
    if let Some(name) = configured.as_deref() {
        if (name.starts_with("refs/remotes/") || name.starts_with("refs/heads/"))
            && exists(cwd, name).await
        {
            return Some((name.into(), name.into()));
        }
        if name.contains('/') && !name.starts_with("refs/") {
            if let Some(base) = remote_base(cwd, name).await {
                let local = format!("refs/heads/{name}");
                return Some(if exists(cwd, &local).await {
                    (local.clone(), local)
                } else {
                    base
                });
            }
        }
        if name != branch {
            if let Some(base) = tracked(cwd, name).await {
                return Some(base);
            }
        }
    }
    let remotes = read(cwd, &["remote"]).await.unwrap_or_default();
    let remote = remotes
        .lines()
        .find(|r| *r == "origin")
        .or_else(|| remotes.lines().next());
    let default = match remote {
        Some(r) => remote_default_branch(cwd, r).await,
        None => None,
    };
    for candidate in [
        configured.as_deref(),
        default.as_deref(),
        Some("main"),
        Some("master"),
    ]
    .into_iter()
    .flatten()
    {
        let name = candidate
            .strip_prefix("origin/")
            .or_else(|| remote.and_then(|r| candidate.strip_prefix(&format!("{r}/"))))
            .unwrap_or(candidate);
        if name == branch {
            if !allow_current {
                continue;
            }
            if let Some(base) = tracked(cwd, branch).await {
                return Some(base);
            }
        }
        if let Some(remote) = remote {
            if let Some(base) = remote_base(cwd, &format!("{remote}/{name}")).await {
                return Some(base);
            }
        }
        let local = format!("refs/heads/{name}");
        if name != branch && exists(cwd, &local).await {
            return Some((name.into(), local));
        }
    }
    None
}

pub(in crate::local_services) async fn review_base(
    cwd: &str,
    branch: Option<&str>,
    explicit: Option<&str>,
) -> Result<(Option<String>, String), serde_json::Value> {
    let base = if let Some(name) = explicit {
        let full = if name.contains('/')
            && !name.starts_with("refs/")
            && exists(cwd, &format!("refs/remotes/{name}")).await
        {
            format!("refs/remotes/{name}")
        } else if !name.starts_with("refs/") && exists(cwd, &format!("refs/heads/{name}")).await {
            format!("refs/heads/{name}")
        } else {
            name.into()
        };
        Some((name.to_owned(), full))
    } else {
        branch_base(cwd, branch, true).await
    };
    if let Some((name, full)) = base {
        let out = super::run(cwd, &["merge-base", "--end-of-options", &full, "HEAD"]).await;
        if let Ok(out) = &out {
            if out.status.success() {
                return Ok((
                    Some(name),
                    String::from_utf8_lossy(&out.stdout).trim().to_owned(),
                ));
            }
        }
        if explicit.is_some()
            || read(cwd, &["rev-parse", "--verify", "HEAD"])
                .await
                .is_some()
        {
            return Err(
                serde_json::json!({"_tag":"GitCommandError","operation":"GitVcsDriver.resolveReviewMergeBase","command":"git","cwd":cwd,"argumentCount":4,"detail":format!("Could not find a common commit between '{name}' and HEAD."),"exitCode":out.ok().and_then(|o| o.status.code())}),
            );
        }
    }
    Ok((None, "HEAD".to_owned()))
}
