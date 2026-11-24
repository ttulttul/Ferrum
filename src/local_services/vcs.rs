use super::{invalid, string, Reply};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, HashSet},
    path::Path,
    process::{Output, Stdio},
};

#[derive(Default)]
pub struct VcsService {
    hosted: super::git_workflow::status::Cache,
}
pub(super) async fn run(cwd: &str, args: &[&str]) -> std::io::Result<Output> {
    crate::login_shell::tokio_command("git")
        .current_dir(cwd)
        .args(args)
        .env("LC_ALL", "C")
        .env("GIT_TERMINAL_PROMPT", "0")
        .stdin(Stdio::null())
        .kill_on_drop(true)
        .output()
        .await
}
/// The reference's `refsDigest` (#40385, #40452, #40556): lets a client keep
/// its cached branch list while the refs and worktree paths are unchanged.
/// The first 32 hex digits of the SHA-256 of `readRefSnapshot`.
async fn refs_digest(cwd: &str) -> Option<String> {
    let snapshot = ref_snapshot(cwd).await?;
    Some(hex::encode(Sha256::digest(snapshot.as_bytes()))[..32].to_owned())
}

/// `readLocalStatusRefSnapshot` and the worktree stamp after it.
async fn ref_snapshot(cwd: &str) -> Option<String> {
    let refs = run(cwd, &["show-ref", "--head"]).await.ok()?;
    if !refs.status.success() {
        return None;
    }
    let head = run(cwd, &["symbolic-ref", "--quiet", "HEAD"]).await.ok()?;
    // Exit 1 means a detached HEAD.
    if !matches!(head.status.code(), Some(0 | 1)) {
        return None;
    }
    let common = read(
        cwd,
        &["rev-parse", "--path-format=absolute", "--git-common-dir"],
    )
    .await?;
    let common = Path::new(common.trim());
    let shallow = std::fs::read_to_string(common.join("shallow")).unwrap_or_default();
    let stamp = worktree_stamp(common).await?;
    Some(format!(
        "{}\0{}\0{shallow}\0{stamp}",
        String::from_utf8_lossy(&refs.stdout),
        String::from_utf8_lossy(&head.stdout),
    ))
}

/// `readWorktreeBranchPaths`' stamp: each existing, unprunable worktree
/// with a branch, as `[branch, path]` JSON, sorted, one per line.
async fn worktree_stamp(common: &Path) -> Option<String> {
    let dir = if common.file_name().is_some_and(|n| n == ".git") {
        common.parent()?
    } else {
        common
    };
    let (dir, git_dir) = (dir.to_str()?, common.to_str()?);
    let args = ["--git-dir", git_dir, "worktree", "list", "--porcelain"];
    let listed = run(dir, &[&args[..], &["-z"]].concat()).await.ok()?;
    let stdout = if listed.status.success() {
        String::from_utf8_lossy(&listed.stdout).into_owned()
    } else {
        let lines = run(dir, &args).await.ok()?;
        if !lines.status.success() {
            return None;
        }
        String::from_utf8_lossy(&lines.stdout).replace('\n', "\0")
    };
    let mut paths: BTreeMap<String, String> = BTreeMap::new();
    let (mut path, mut branch, mut prunable) = (None::<String>, None::<String>, false);
    let mut flush =
        |path: &mut Option<String>, branch: &mut Option<String>, prunable: &mut bool| {
            if let (Some(p), Some(b), false) = (path.take(), branch.take(), *prunable) {
                paths.insert(b, p);
            }
            *prunable = false;
        };
    for field in stdout.split('\0') {
        if field.is_empty() {
            flush(&mut path, &mut branch, &mut prunable);
        } else if let Some(p) = field.strip_prefix("worktree ") {
            path = Some(p.to_owned());
        } else if let Some(b) = field.strip_prefix("branch refs/heads/") {
            branch = Some(b.to_owned());
        } else if field == "prunable" || field.starts_with("prunable ") {
            prunable = true;
        }
    }
    flush(&mut path, &mut branch, &mut prunable);
    let common = lexical(common);
    let mut stamp: Vec<String> = paths
        .into_iter()
        .map(|(branch, path)| (branch, lexical(Path::new(&path))))
        .filter(|(_, path)| *path != common && path.exists())
        .map(|(branch, path)| json!([branch, path.to_string_lossy()]).to_string())
        .collect();
    stamp.sort();
    Some(stamp.join("\n"))
}

/// Node's `path.normalize` of an absolute path: `.` and `..` resolved
/// without touching the file system.
fn lexical(path: &Path) -> std::path::PathBuf {
    let mut out = std::path::PathBuf::new();
    for part in path.components() {
        match part {
            std::path::Component::CurDir => {}
            std::path::Component::ParentDir => {
                out.pop();
            }
            other => out.push(other),
        }
    }
    out
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
    checked_output(run(cwd, args).await, failure_context(op, cwd, args), detail)
}
fn checked_output(
    output: std::io::Result<Output>,
    mut err: Value,
    detail: &str,
) -> Result<String, Value> {
    match output {
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
/// The branch `refs/remotes/<remote>/HEAD` names, as `resolveDefaultBranchName` reads it. Unlike
/// `symbolic-ref`, `rev-parse --verify` fails when the remote deleted the branch HEAD points at.
async fn remote_default_branch(cwd: &str, remote: &str) -> Option<String> {
    let head = format!("refs/remotes/{remote}/HEAD");
    let target = read(
        cwd,
        &[
            "rev-parse",
            "--verify",
            "--quiet",
            "--symbolic-full-name",
            &head,
        ],
    )
    .await?;
    let name = target
        .trim()
        .strip_prefix(&format!("refs/remotes/{remote}/"))?
        .trim();
    (!name.is_empty() && name != "HEAD").then(|| name.to_owned())
}
pub(super) async fn default_ref(cwd: &str) -> Option<String> {
    remote_default_branch(cwd, "origin").await
}
mod base;
mod removal;
pub(crate) mod worktree;
pub(super) use base::{branch_base, read_branch_base, record_worktree_base, review_base};
fn nonrepo() -> Value {
    json!({"isRepo":false,"hasPrimaryRemote":false,"isDefaultRef":false,"refName":null,"hasWorkingTreeChanges":false,"workingTree":{"files":[],"insertions":0,"deletions":0},"hasUpstream":false,"aheadCount":0,"behindCount":0,"aheadOfDefaultCount":0,"pr":null})
}
async fn status(
    root: &str,
    hosted: &super::git_workflow::status::Cache,
    local_only: bool,
    subscription: bool,
) -> Reply {
    let head = branch(root).await;
    let origin = read(root, &["remote"]).await.unwrap_or_default();
    let primary = origin
        .lines()
        .find(|r| *r == "origin")
        .or_else(|| origin.lines().next());
    let default = match primary {
        Some(remote) => remote_default_branch(root, remote).await,
        None => None,
    };
    let is_default = head.as_ref().is_some_and(|s| {
        default
            .as_deref()
            .map_or(s == "main" || s == "master", |d| d == s)
    });
    let has_remote = !origin.is_empty();
    let mut result = json!({"isRepo":true});
    let preferred = match head.as_ref() {
        Some(name) => read(root, &["config", "--get", &format!("branch.{name}.remote")]).await,
        None => None,
    };
    let mut url = read(
        root,
        &[
            "remote",
            "get-url",
            preferred.as_deref().unwrap_or("origin"),
        ],
    )
    .await;
    if url.is_none() && preferred.is_some() {
        url = read(root, &["remote", "get-url", "origin"]).await;
    }
    if let Some(provider) = url
        .as_deref()
        .and_then(super::git_workflow::status::provider)
    {
        result["sourceControlProvider"] = provider;
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
    let mut has_untracked = false;
    let mut records = porcelain.split_terminator('\0');
    while let Some(line) = records.next() {
        if line.len() >= 4 {
            has_untracked |= line.starts_with("?? ");
            files.entry(line[3..].to_string()).or_default();
            if line[..2].contains(['R', 'C']) {
                if let Some(original) = records.next() {
                    files.entry(original.to_owned()).or_default();
                }
            }
        }
    }
    let (ins, del) = totals(&files);
    result["hasWorkingTreeChanges"] = json!(!porcelain.is_empty());
    result["workingTree"] = json!({"rootPath":root,"files":files.iter().map(|(p,(a,b))|json!({"path":p,"insertions":a,"deletions":b})).collect::<Vec<_>>(),"insertions":ins,"deletions":del});
    let base = review_base(root, head.as_deref(), None).await;
    let index = if has_untracked {
        super::review::index::prepare(root, None).await.ok()
    } else {
        Some(Default::default())
    };
    if let (Ok((label, merge)), Some(index)) = (base, index.filter(|i| !i.omitted)) {
        let reference = if merge == "HEAD" { &basis } else { &merge };
        let mut cmd = crate::login_shell::tokio_command("git");
        if let Some(dir) = &index.dir {
            cmd.env("GIT_INDEX_FILE", dir.path().join("index"));
        }
        let stats = cmd
            .current_dir(root)
            .args(["diff-index", "-M", "--numstat", "-z", reference, "--"])
            .kill_on_drop(true)
            .output()
            .await;
        if let Ok(out) = stats {
            if out.status.success() {
                let (ins, del) = totals(&numstat(&String::from_utf8_lossy(&out.stdout)));
                result["branchChanges"] = json!({"baseRef":label,"insertions":ins,"deletions":del});
            }
        }
    }
    if local_only {
        let cached = hosted.snapshot(root, head.as_deref());
        if let Some(remote) = cached.or_else(|| (!subscription).then(||json!({"hasUpstream":false,"aheadCount":0,"behindCount":0,"aheadOfDefaultCount":0,"pr":null}))) {
            result.as_object_mut().unwrap().extend(remote.as_object().unwrap().clone());
        }
        return Ok(result);
    }
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
    let divergence = if !is_default || upstream.is_none() {
        if let Some((_, comparison)) = branch_base(root, head.as_deref(), false).await {
            read(
                root,
                &[
                    "rev-list",
                    "--count",
                    "--end-of-options",
                    &format!("{comparison}..HEAD"),
                ],
            )
            .await
            .and_then(|n| n.parse().ok())
            .unwrap_or(0)
        } else {
            0
        }
    } else {
        0
    };
    let mut pr = match head.as_deref() {
        Some(branch) => {
            hosted
                .pull_request(
                    root,
                    branch,
                    upstream.as_deref(),
                    default.as_deref(),
                    &origin,
                    !subscription,
                )
                .await
        }
        None => None,
    }
    .filter(|pr| !is_default || pr["state"] == "open");
    let mut shipped = false;
    if let Some(pr) = &mut pr {
        let sha = pr.as_object_mut().unwrap().remove("headSha");
        shipped = pr["state"] == "merged"
            && sha.as_ref().and_then(Value::as_str) == Some(&basis)
            && default.as_ref().map_or(
                matches!(pr["baseRef"].as_str(), Some("main" | "master")),
                |d| pr["baseRef"] == *d,
            );
    }
    let remote = json!({"hasUpstream":upstream.is_some(),
        "aheadCount":if shipped && upstream.is_none() {0} else {parts.next().unwrap_or(divergence)},
        "behindCount":parts.next().unwrap_or(0),
        "aheadOfDefaultCount": if is_default || shipped {0} else {divergence},"pr":pr});
    result
        .as_object_mut()
        .unwrap()
        .extend(remote.as_object().unwrap().clone());
    hosted.remember(root, head, remote);
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
        let worktree = cols
            .next()
            .filter(|v| !v.is_empty() && Path::new(v).is_dir());
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

pub(super) fn lexical_path(path: &Path) -> std::path::PathBuf {
    let mut resolved = std::path::PathBuf::new();
    for component in path.components() {
        if component == std::path::Component::ParentDir {
            resolved.pop();
        } else {
            resolved.push(component);
        }
    }
    resolved
}

impl VcsService {
    pub(crate) async fn subscription(&self, input: &Value, fetch: bool, initial: bool) -> Reply {
        let cwd = string(input, "cwd")?;
        let Some(root) = read(cwd, &["rev-parse", "--show-toplevel"]).await else {
            return Ok(nonrepo());
        };
        let cached_only = initial || input["includeRemote"] == false;
        if fetch && !cached_only {
            super::vcs_upstream::refresh_if_stale(cwd).await;
        }
        status(&root, &self.hosted, cached_only, true).await
    }

    pub async fn branch_status(&self, input: &Value) -> Reply {
        let cwd = string(input, "cwd")?;
        let is_repo = read(cwd, &["rev-parse", "--git-dir"]).await.is_some();
        let name = if is_repo { branch(cwd).await } else { None };
        Ok(json!({"refName":name,"isRepo":is_repo}))
    }

    pub async fn call(&self, method: &str, input: &Value, base_dir: &Path) -> Option<Reply> {
        self.call_with(method, input, base_dir, true).await
    }

    /// `refresh_upstream` is false when the automatic Git fetch interval is zero; a status read
    /// then reports the refs as last fetched, as the reference does.
    pub async fn call_with(
        &self,
        method: &str,
        input: &Value,
        base_dir: &Path,
        refresh_upstream: bool,
    ) -> Option<Reply> {
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
                | "resolveCommit"
                | "renameBranch"
        ) {
            return None;
        }
        Some(
            self.apply(
                method,
                input,
                &super::base_directory(base_dir).join("worktrees"),
                refresh_upstream,
                None,
            )
            .await,
        )
    }
    pub(crate) async fn create_worktree(
        &self,
        input: &Value,
        state_dir: &Path,
        directory: &str,
        submodules: Option<&str>,
    ) -> Reply {
        let parent = if directory.is_empty() {
            super::base_directory(state_dir).join("worktrees")
        } else {
            let path = std::path::PathBuf::from(crate::provider_status::expand_home(directory));
            let normalized = lexical_path(&path);
            if !normalized.is_absolute() || normalized.parent().is_none() {
                return Err(
                    json!({"_tag":"GitCommandError","operation":"GitVcsDriver.createWorktree","command":"git worktree add","cwd":input["cwd"],"detail":format!("The worktree location \"{directory}\" must be an absolute folder on this machine, not a drive root. Change it in Settings → Worktrees.")}),
                );
            }
            normalized
        };
        self.apply("createWorktree", input, &parent, false, submodules)
            .await
    }
    async fn apply(
        &self,
        method: &str,
        input: &Value,
        base_dir: &Path,
        refresh_upstream: bool,
        submodules: Option<&str>,
    ) -> Reply {
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
            "refreshStatus" => {
                let local_only = input["localOnly"] == true;
                if refresh_upstream && !local_only {
                    super::vcs_upstream::refresh_if_stale(cwd).await;
                }
                let mut reply = status(&root, &self.hosted, local_only, false).await;
                if let Some(status) = reply.as_mut().ok().filter(|s| s["isRepo"] == true) {
                    if let Some(digest) = refs_digest(&root).await {
                        status["refsDigest"] = json!(digest);
                    }
                }
                reply
            }
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
                let cwd_path = lexical_path(
                    &std::path::absolute(cwd)
                        .map_err(|e| json!({"_tag":"Die","defect":e.to_string()}))?,
                );
                let dir = base_dir.join(cwd_path.file_name().unwrap_or_default());
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
                worktree::create(cwd, name, new, &path, &common, submodules).await?;
                if let (Some(new), Some(base)) = (new, input["baseRefName"].as_str()) {
                    record_worktree_base(cwd, new, base).await;
                }
                Ok(json!({"worktree":{"path":path,"refName":target}}))
            }
            "removeWorktree" => {
                removal::remove(cwd, string(input, "path")?, input["force"] == true).await
            }
            // Server-internal (not client RPCs): a delegated task's worktree preparation.
            "resolveCommit" => {
                let revision = format!("{}^{{commit}}", string(input, "revision")?);
                let sha = checked(
                    cwd,
                    "resolveCommit",
                    &[
                        "rev-parse",
                        "--verify",
                        "--quiet",
                        "--end-of-options",
                        &revision,
                    ],
                    "Unable to resolve the commit.",
                )
                .await?;
                Ok(json!({"commitSha": sha.trim()}))
            }
            "renameBranch" => {
                let old = string(input, "oldBranch")?;
                let new = string(input, "newBranch")?;
                if old == new {
                    return Ok(json!({"branch": new}));
                }
                let target = if input["exactName"] == true {
                    new.to_owned()
                } else {
                    let listed = checked(
                        cwd,
                        "resolveAvailableBranchName",
                        &["for-each-ref", "--format=%(refname:strip=2)", "refs/heads/"],
                        "git for-each-ref failed",
                    )
                    .await?;
                    let branches: Vec<String> = listed
                        .lines()
                        .filter(|line| !line.is_empty())
                        .map(str::to_owned)
                        .collect();
                    crate::title_generation::branch::available_branch_name(&branches, new)
                        .ok_or_else(|| {
                            let mut error = failure_context("renameBranch", cwd, &["branch", "-m"]);
                            error["detail"] = json!(format!(
                                "Could not find an available branch name for '{new}'."
                            ));
                            error
                        })?
                };
                checked(
                    cwd,
                    "renameBranch",
                    &["branch", "-m", "--", old, &target],
                    "git branch rename failed",
                )
                .await?;
                Ok(json!({"branch": target}))
            }
            _ => unreachable!(),
        }
    }
}
