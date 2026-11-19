use super::{invalid, string, Reply};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, HashSet},
    path::Path,
    process::{Output, Stdio},
};
use tokio::process::Command;

pub struct VcsService;
pub(super) async fn run(cwd: &str, args: &[&str]) -> std::io::Result<Output> {
    Command::new("git")
        .current_dir(cwd)
        .args(args)
        .env("LC_ALL", "C")
        .env("GIT_TERMINAL_PROMPT", "0")
        .stdin(Stdio::null())
        .kill_on_drop(true)
        .output()
        .await
}
pub(super) async fn read(cwd: &str, args: &[&str]) -> Option<String> {
    let out = run(cwd, args).await.ok()?;
    out.status.success().then(|| {
        String::from_utf8_lossy(&out.stdout)
            .trim_end_matches('\n')
            .to_string()
    })
}
fn failure_context(op: &str, cwd: &str, args: &[&str]) -> Value {
    json!({"_tag":"GitCommandError","operation":format!("GitVcsDriver.{op}"),"command":"git","cwd":cwd,"argumentCount":args.len()})
}
pub(super) async fn checked(
    cwd: &str,
    op: &str,
    args: &[&str],
    detail: &str,
) -> Result<String, Value> {
    let mut err = failure_context(op, cwd, args);
    match run(cwd, args).await {
        Ok(out) if out.status.success() => Ok(String::from_utf8_lossy(&out.stdout)
            .trim_end_matches('\n')
            .to_string()),
        Ok(out) => {
            let stderr = String::from_utf8_lossy(&out.stderr);
            err["exitCode"] = json!(out.status.code());
            err["stdoutLength"] =
                json!(String::from_utf8_lossy(&out.stdout).encode_utf16().count());
            err["stderrLength"] = json!(stderr.encode_utf16().count());
            let reason = if stderr.contains("already exists") {
                Some("branch_already_exists")
            } else if stderr.contains("not a git repository") {
                Some("not_a_repository")
            } else if stderr.contains("did not match any file")
                || stderr.contains("invalid reference")
            {
                Some("ref_not_found")
            } else if stderr.contains("already checked out")
                || stderr.contains("already used by worktree")
            {
                Some("branch_checked_out_in_worktree")
            } else {
                None
            };
            if let Some(r) = reason {
                err["reason"] = json!(r);
            }
            err["detail"] = json!(detail);
            Err(err)
        }
        Err(_) => {
            err["detail"] = json!("Failed to spawn git.");
            Err(err)
        }
    }
}
fn safe_ref(cwd: &str, op: &str, reference: &str) -> Result<(), Value> {
    if reference.starts_with('-') && reference != "-" {
        return Err(
            json!({"_tag":"GitCommandError","operation":format!("GitVcsDriver.{op}"),"command":"git","cwd":cwd,"detail":"Ref names cannot start with '-'."}),
        );
    }
    Ok(())
}
fn numstat(text: &str) -> BTreeMap<String, (u64, u64)> {
    let mut entries = BTreeMap::new();
    let mut fields = text.split_terminator('\0');
    while let Some(row) = fields.next() {
        let mut cols = row.splitn(3, '\t');
        let ins = cols.next().and_then(|v| v.parse().ok()).unwrap_or(0);
        let del = cols.next().and_then(|v| v.parse().ok()).unwrap_or(0);
        if let Some(mut path) = cols.next() {
            if path.is_empty() {
                fields.next();
                path = fields.next().unwrap_or("");
            }
            if !path.is_empty() {
                entries.insert(path.to_owned(), (ins, del));
            }
        }
    }
    entries
}
fn totals(map: &BTreeMap<String, (u64, u64)>) -> (u64, u64) {
    map.values().fold((0, 0), |(i, d), (a, b)| (i + a, d + b))
}
pub(super) async fn branch(cwd: &str) -> Option<String> {
    read(cwd, &["symbolic-ref", "--quiet", "--short", "HEAD"]).await
}
pub(super) async fn default_ref(cwd: &str) -> Option<String> {
    read(
        cwd,
        &[
            "symbolic-ref",
            "--quiet",
            "--short",
            "refs/remotes/origin/HEAD",
        ],
    )
    .await?
    .strip_prefix("origin/")
    .map(str::to_owned)
}
fn nonrepo() -> Value {
    json!({"isRepo":false,"hasPrimaryRemote":false,"isDefaultRef":false,"refName":null,"hasWorkingTreeChanges":false,"workingTree":{"files":[],"insertions":0,"deletions":0},"hasUpstream":false,"aheadCount":0,"behindCount":0,"aheadOfDefaultCount":0,"pr":null})
}
async fn status(root: &str) -> Reply {
    let head = branch(root).await;
    let default = default_ref(root).await;
    let is_default = head.as_ref().is_some_and(|s| {
        default
            .as_deref()
            .map_or(s == "main" || s == "master", |d| d == s)
    });
    let origin = read(root, &["remote"]).await.unwrap_or_default();
    let has_remote = !origin.is_empty();
    let mut result = json!({"isRepo":true});
    if let Some(remote) = origin
        .lines()
        .find(|r| *r == "origin")
        .or_else(|| origin.lines().next())
    {
        if let Some(url) = read(root, &["remote", "get-url", remote]).await {
            let host = url::Url::parse(&url)
                .ok()
                .and_then(|u| u.host_str().map(str::to_owned))
                .or_else(|| {
                    url.split_once('@')?
                        .1
                        .split_once(':')
                        .map(|(h, _)| h.to_owned())
                });
            if let Some(h) = host.as_deref() {
                if let Some((kind, name)) = match h {
                    "github.com" => Some(("github", "GitHub")),
                    "gitlab.com" => Some(("gitlab", "GitLab")),
                    "bitbucket.org" => Some(("bitbucket", "Bitbucket")),
                    _ => None,
                } {
                    result["sourceControlProvider"] =
                        json!({"kind":kind,"name":name,"baseUrl":format!("https://{h}")});
                }
            }
        }
    }
    result["hasPrimaryRemote"] = json!(has_remote);
    result["isDefaultRef"] = json!(is_default);
    result["refName"] = json!(head);
    let head_sha = read(root, &["rev-parse", "--verify", "HEAD"]).await;
    let basis = match head_sha {
        Some(id) => id,
        None => {
            checked(
                root,
                "statusDetails.emptyTree",
                &["hash-object", "-t", "tree", "--stdin"],
                "Could not resolve empty tree.",
            )
            .await?
        }
    };
    let diff = checked(
        root,
        "statusDetails.numstat",
        &["diff-index", "-M", "--numstat", "-z", &basis, "--"],
        "git diff-index HEAD --numstat failed.",
    )
    .await?;
    let mut files = numstat(&diff);
    let porcelain = checked(
        root,
        "statusDetails",
        &["status", "--porcelain=v1", "-z", "--untracked-files=normal"],
        "git status failed.",
    )
    .await?;
    let mut records = porcelain.split_terminator('\0');
    while let Some(line) = records.next() {
        if line.len() >= 4 {
            files.entry(line[3..].to_string()).or_default();
            if line[..2].contains(['R', 'C']) {
                records.next();
            }
        }
    }
    let (ins, del) = totals(&files);
    result["hasWorkingTreeChanges"] = json!(!porcelain.is_empty());
    result["workingTree"] = json!({"rootPath":root,"files":files.iter().map(|(p,(a,b))|json!({"path":p,"insertions":a,"deletions":b})).collect::<Vec<_>>(),"insertions":ins,"deletions":del});
    let mut base = None;
    if !is_default {
        let candidates = [
            default.as_ref().map(|d| format!("origin/{d}")),
            Some("main".into()),
            Some("master".into()),
        ];
        for cand in candidates.into_iter().flatten() {
            if Some(&cand) != head.as_ref()
                && read(
                    root,
                    &["rev-parse", "--verify", &format!("{cand}^{{commit}}")],
                )
                .await
                .is_some()
            {
                base = Some(cand);
                break;
            }
        }
    }
    let mut left = ins;
    let mut right = del;
    let mut divergence = 0;
    if let Some(base) = &base {
        if let Some(merge) = read(root, &["merge-base", "HEAD", base]).await {
            if let Some(stats) =
                read(root, &["diff-index", "-M", "--numstat", "-z", &merge, "--"]).await
            {
                (left, right) = totals(&numstat(&stats));
            }
            divergence = read(root, &["rev-list", "--count", &format!("{merge}..HEAD")])
                .await
                .and_then(|n| n.parse().ok())
                .unwrap_or(0);
        }
    }
    let untracked = checked(
        root,
        "statusDetails.untracked",
        &["ls-files", "--others", "--exclude-standard", "-z"],
        "git ls-files failed.",
    )
    .await?;
    for name in untracked.split_terminator('\0') {
        let path = Path::new(root).join(name);
        if let Ok(stat) = tokio::fs::symlink_metadata(&path).await {
            if stat.is_file() {
                let null = if cfg!(windows) { "NUL" } else { "/dev/null" };
                if let Ok(out) = run(
                    root,
                    &["diff", "--no-index", "--numstat", "-z", "--", null, name],
                )
                .await
                {
                    left += totals(&numstat(&String::from_utf8_lossy(&out.stdout))).0;
                }
            } else if stat.is_symlink() {
                left += 1;
            }
        }
    }
    result["branchChanges"] = json!({"baseRef":base,"insertions":left,"deletions":right});
    let upstream = read(
        root,
        &["rev-parse", "--abbrev-ref", "--symbolic-full-name", "@{u}"],
    )
    .await;
    let counts = read(
        root,
        &["rev-list", "--left-right", "--count", "HEAD...@{u}"],
    )
    .await
    .unwrap_or_default();
    let mut parts = counts
        .split_whitespace()
        .filter_map(|s| s.parse::<u64>().ok());
    result["hasUpstream"] = json!(upstream.is_some());
    result["aheadCount"] = json!(parts.next().unwrap_or(divergence));
    result["behindCount"] = json!(parts.next().unwrap_or(0));
    result["aheadOfDefaultCount"] = json!(divergence);
    result["pr"] = Value::Null;
    Ok(result)
}

async fn refs(root: &str, cwd: &str, input: &Value) -> Reply {
    let head = branch(cwd).await;
    let default = default_ref(cwd).await;
    let remotes = read(cwd, &["remote"]).await.unwrap_or_default();
    let raw = checked(
        cwd,
        "listRefs.snapshotRefs",
        &[
            "for-each-ref",
            "--sort=refname",
            "--sort=-committerdate",
            "--format=%(refname)%09%(symref)%09%(upstream)%09%(worktreepath)",
            "refs/heads",
            "refs/remotes",
        ],
        "Failed to list refs.",
    )
    .await?;
    let mut refs = Vec::new();
    let mut locals = HashSet::new();
    for row in raw.lines() {
        let mut cols = row.split('\t');
        let full = cols.next().unwrap_or_default();
        let symbolic = cols.next().unwrap_or_default();
        if !symbolic.is_empty() {
            continue;
        }
        let upstream = cols.next().unwrap_or_default();
        let worktree = cols.next().filter(|v| !v.is_empty());
        let remote = full.starts_with("refs/remotes/");
        let Some(name) = full
            .strip_prefix("refs/heads/")
            .or_else(|| full.strip_prefix("refs/remotes/"))
        else {
            continue;
        };
        let remote_name = if remote {
            remotes
                .lines()
                .filter(|r| name.starts_with(&format!("{r}/")))
                .max_by_key(|r| r.len())
        } else {
            None
        };
        if !remote {
            locals.insert(name.to_owned());
        }
        let current = !remote && head.as_deref() == Some(name);
        let default_name = if remote {
            name.strip_prefix("origin/")
        } else {
            Some(name)
        };
        let mut entry = json!({"name":name,"isRemote":remote});
        if let Some(r) = remote_name {
            entry["remoteName"] = json!(r);
        }
        if !upstream.is_empty() {
            entry["upstreamRef"] = json!(upstream);
        }
        entry["current"] = json!(current);
        entry["isDefault"] = json!(default.is_some() && default_name == default.as_deref());
        entry["worktreePath"] = json!(if current { Some(root) } else { worktree });
        refs.push(entry);
    }
    let query = input["query"].as_str().unwrap_or_default().to_lowercase();
    let kind = input["refKind"].as_str().unwrap_or("all");
    refs.retain(|r| {
        let name = r["name"].as_str().unwrap();
        (input["includeMatchingRemoteRefs"] == true
            || r["remoteName"] != "origin"
            || !locals.contains(name.strip_prefix("origin/").unwrap_or(name)))
            && (kind == "all" || (kind == "remote") == r["isRemote"].as_bool().unwrap())
            && name.to_lowercase().contains(&query)
    });
    refs.sort_by_key(|r| {
        (
            if r["current"] == true {
                0
            } else if r["isDefault"] == true {
                1
            } else {
                2
            },
            r["isRemote"] == true,
        )
    });
    let total = refs.len();
    let cursor = input["cursor"].as_u64().unwrap_or(0) as usize;
    let limit = input["limit"].as_u64().unwrap_or(100) as usize;
    let next = cursor.saturating_add(limit);
    let at_root = std::fs::canonicalize(cwd).ok().as_deref() == Some(Path::new(root));
    Ok(
        json!({"isAtRepositoryRoot":at_root,"refs":refs.into_iter().skip(cursor).take(limit).collect::<Vec<_>>(),"isRepo":true,"hasPrimaryRemote":!remotes.is_empty(),"nextCursor":(next<total).then_some(next),"totalCount":total}),
    )
}

async fn checkout(cwd: &str, name: &str) -> Reply {
    safe_ref(cwd, "switchRef", name)?;
    let remote_ref = format!("refs/remotes/{name}");
    let local = read(
        cwd,
        &["show-ref", "--verify", &format!("refs/heads/{name}")],
    )
    .await;
    if local.is_none()
        && read(cwd, &["show-ref", "--verify", &remote_ref])
            .await
            .is_some()
    {
        let tracking = read(
            cwd,
            &[
                "for-each-ref",
                "--format=%(refname:lstrip=2)%09%(upstream)",
                "refs/heads",
            ],
        )
        .await
        .unwrap_or_default();
        if let Some(existing) = tracking.lines().find_map(|l| {
            l.split_once('\t')
                .filter(|(_, r)| *r == remote_ref)
                .map(|(n, _)| n)
        }) {
            safe_ref(cwd, "switchRef", existing)?;
            checked(
                cwd,
                "switchRef",
                &["checkout", existing],
                "git checkout failed",
            )
            .await?;
        } else {
            checked(
                cwd,
                "switchRef",
                &["checkout", "--track", name],
                "git checkout failed",
            )
            .await?;
        }
    } else {
        checked(cwd, "switchRef", &["checkout", name], "git checkout failed").await?;
    }
    Ok(json!({"refName":branch(cwd).await}))
}

impl VcsService {
    pub async fn branch_status(&self, input: &Value) -> Reply {
        let cwd = string(input, "cwd")?;
        let is_repo = read(cwd, &["rev-parse", "--git-dir"]).await.is_some();
        let name = if is_repo { branch(cwd).await } else { None };
        Ok(json!({"refName":name,"isRepo":is_repo}))
    }

    pub async fn call(&self, method: &str, input: &Value, base_dir: &Path) -> Option<Reply> {
        if !matches!(
            method,
            "init"
                | "refreshBranch"
                | "refreshStatus"
                | "pull"
                | "listRefs"
                | "createRef"
                | "switchRef"
                | "createWorktree"
                | "removeWorktree"
        ) {
            return None;
        }
        Some(self.apply(method, input, base_dir).await)
    }
    async fn apply(&self, method: &str, input: &Value, base_dir: &Path) -> Reply {
        let cwd = string(input, "cwd")?;
        if method == "init" {
            if input.get("kind").is_some() && input["kind"] != "git" {
                return Err(
                    json!({"_tag":"VcsUnsupportedOperationError","kind":input["kind"],"operation":"init","detail":"No driver is registered for this VCS kind."}),
                );
            }
            checked(cwd, "initRepo", &["init"], "git init failed").await?;
            return Ok(Value::Null);
        }
        if method == "refreshBranch" {
            let _ = branch(cwd).await;
            return Ok(Value::Null);
        }
        let root = read(cwd, &["rev-parse", "--show-toplevel"]).await;
        let Some(root) = root else {
            return match method {
                "refreshStatus" => Ok(nonrepo()),
                "listRefs" => Ok(
                    json!({"refs":[],"isRepo":false,"hasPrimaryRemote":false,"nextCursor":null,"totalCount":0}),
                ),
                _ => {
                    let op = if method == "pull" {
                        "pullCurrentBranch"
                    } else {
                        method
                    };
                    Err(
                        json!({"_tag":"GitCommandError","operation":format!("GitWorkflowService.{op}"),"command":"vcs-route","cwd":cwd,"detail":"Failed to resolve the VCS driver for this Git command.","cause":{"name":"VcsUnsupportedOperationError","message":format!("VCS operation is unsupported for unknown in VcsDriverRegistry.resolve: No supported VCS repository was detected at {cwd}.")}}),
                    )
                }
            };
        };
        match method {
            "refreshStatus" => status(&root).await,
            "listRefs" => refs(&root, cwd, input).await,
            "pull" => {
                let head = branch(cwd).await;
                let upstream = read(
                    cwd,
                    &["rev-parse", "--abbrev-ref", "--symbolic-full-name", "@{u}"],
                )
                .await;
                if head.is_none() || upstream.is_none() {
                    let mut e = failure_context("pullCurrentBranch", cwd, &["pull", "--ff-only"]);
                    e["detail"] = json!(if head.is_none() {
                        "Cannot pull from detached HEAD."
                    } else {
                        "Current branch has no upstream configured. Push with upstream first."
                    });
                    return Err(e);
                }
                let before = read(cwd, &["rev-parse", "HEAD"]).await;
                checked(
                    cwd,
                    "pullCurrentBranch.pull",
                    &["pull", "--ff-only"],
                    "git pull --ff-only failed",
                )
                .await?;
                Ok(
                    json!({"status":if before==read(cwd,&["rev-parse","HEAD"]).await{"skipped_up_to_date"}else{"pulled"},"refName":head,"upstreamRef":upstream}),
                )
            }
            "createRef" => {
                let name = string(input, "refName")?;
                checked(
                    cwd,
                    "createRef",
                    &["branch", "--", name],
                    "git branch create failed",
                )
                .await?;
                if input["switchRef"] == true {
                    checkout(cwd, name).await?;
                }
                Ok(json!({"refName":name}))
            }
            "switchRef" => checkout(cwd, string(input, "refName")?).await,
            "createWorktree" => {
                let name = string(input, "refName")?;
                let path = input
                    .get("path")
                    .ok_or_else(|| invalid("path", "Missing key"))?;
                if !path.is_null() {
                    return Err(invalid("path", "Expected null"));
                }
                let new = input
                    .get("newRefName")
                    .map(|_| string(input, "newRefName"))
                    .transpose()?;
                for r in [Some(name), new].into_iter().flatten() {
                    safe_ref(cwd, "createWorktree", r)?;
                }
                let target = new.unwrap_or(name);
                let common = checked(
                    cwd,
                    "createWorktree",
                    &["rev-parse", "--path-format=absolute", "--git-common-dir"],
                    "Failed to resolve Git directory.",
                )
                .await?;
                let common = std::fs::canonicalize(&common).unwrap_or_else(|_| common.into());
                let hash = format!(
                    "{:x}",
                    Sha256::digest(format!("{}\0{}", common.display(), target).as_bytes())
                );
                let mut prefix = String::new();
                for c in target.replace('/', "-").chars() {
                    if prefix.len() + c.len_utf8() > 23 {
                        break;
                    }
                    prefix.push(c);
                }
                let dir = super::base_directory(base_dir)
                    .join("worktrees")
                    .join(Path::new(&root).file_name().unwrap_or_default());
                let file = format!("{prefix}-{}", &hash[..16]);
                let mut path = dir.join(&file);
                let mut suffix = 2;
                while path.exists() {
                    path = dir.join(format!("{file}-{suffix}"));
                    suffix += 1;
                }
                tokio::fs::create_dir_all(&dir).await.map_err(|_|json!({"_tag":"GitCommandError","operation":"GitVcsDriver.createWorktree","command":"git worktree add","cwd":cwd,"detail":"Failed to create managed worktree directory."}))?;
                let path = std::path::absolute(path)
                    .map_err(|e| json!({"_tag":"Die","defect":e.to_string()}))?;
                let path = path.to_string_lossy();
                let args = if let Some(new) = new {
                    vec!["worktree", "add", "-b", new, "--", &path, name]
                } else {
                    vec!["worktree", "add", "--", &path, name]
                };
                checked(cwd, "createWorktree", &args, "git worktree add failed").await?;
                Ok(json!({"worktree":{"path":path,"refName":target}}))
            }
            "removeWorktree" => {
                let path = string(input, "path")?;
                let mut args = vec!["worktree", "remove"];
                if input["force"] == true {
                    args.push("--force");
                }
                args.extend(["--", path]);
                checked(cwd, "removeWorktree", &args, "git worktree remove failed").await?;
                Ok(Value::Null)
            }
            _ => unreachable!(),
        }
    }
}
