use super::{checked, checked_output, failure_context, read, run};
use regex::{Captures, Regex};
use serde_json::{json, Value};
use std::io::{Seek, Write};
use std::path::{Path, PathBuf};
use std::sync::LazyLock;

fn nonempty(value: &Value, max: usize) -> bool {
    value
        .as_str()
        .is_some_and(|s| !s.trim().is_empty() && s.encode_utf16().count() <= max)
}

pub(crate) fn project_file(text: &str) -> Option<Value> {
    // t3.json permits comments and trailing commas, including in nested scripts.
    static COMMENTS: LazyLock<Regex> =
        LazyLock::new(|| Regex::new(r#"("(?:[^"\\]|\\.)*")|//[^\r\n]*|/\*(?s:.*?)\*/"#).unwrap());
    static TRAILING_COMMAS: LazyLock<Regex> =
        LazyLock::new(|| Regex::new(r#"("(?:[^"\\]|\\.)*")|,(\s*[}\]])"#).unwrap());
    let text = COMMENTS.replace_all(text, |c: &Captures<'_>| {
        c.get(1).map_or(" ", |m| m.as_str()).to_owned()
    });
    let text = TRAILING_COMMAS.replace_all(&text, "$1$2");
    let file: Value = serde_json::from_str(&text).ok()?;
    let obj = file.as_object()?;
    for key in ["iconPath", "worktreeSparsePatternsPath"] {
        if obj.get(key).is_some_and(|v| !nonempty(v, 512)) {
            return None;
        }
    }
    if obj.get("$schema").is_some_and(|v| !v.is_string())
        || obj
            .get("defaultThreadEnvMode")
            .is_some_and(|v| !matches!(v.as_str(), Some("worktree" | "local")))
        || obj
            .get("worktreeSubmodules")
            .is_some_and(|v| !matches!(v.as_str(), Some("recursive" | "top-level" | "none")))
    {
        return None;
    }
    if let Some(scripts) = obj.get("scripts") {
        let scripts = scripts.as_array()?;
        if scripts.len() > 50
            || scripts.iter().any(|s| {
                !nonempty(&s["name"], usize::MAX)
                    || !nonempty(&s["command"], usize::MAX)
                    || s.get("icon").is_some_and(|v| {
                        !matches!(
                            v.as_str(),
                            Some("play" | "test" | "lint" | "configure" | "build" | "debug")
                        )
                    })
                    || s.get("previewUrl")
                        .is_some_and(|v| !nonempty(v, usize::MAX))
                    || [
                        "async",
                        "runOnWorktreeCreate",
                        "runOnSettle",
                        "stopOnSettle",
                        "autoOpenPreview",
                    ]
                    .iter()
                    .any(|k| s.get(*k).is_some_and(|v| !v.is_boolean()))
            })
        {
            return None;
        }
    }
    Some(file)
}

async fn sparse_checkout(path: &str, config: &str, patterns: Vec<u8>) -> Result<String, Value> {
    let args = ["sparse-checkout", "set", "--no-cone", "--stdin"];
    let output = async {
        let mut input = tempfile::tempfile()?;
        input.write_all(&patterns)?;
        input.rewind()?;
        crate::login_shell::tokio_command("git")
            .current_dir(path)
            .args(args)
            .env("LC_ALL", "C")
            .stdin(input)
            .kill_on_drop(true)
            .output()
            .await
    }
    .await;
    checked_output(
        output,
        failure_context("createWorktree.sparseCheckout", path, &args),
        "git sparse-checkout set failed",
    )?;
    checked(
        path,
        "createWorktree.checkout",
        &["-c", config, "checkout"],
        "git checkout failed",
    )
    .await
}

async fn initialize_submodules(path: &str, mode: &str) {
    if mode == "none" {
        return;
    }
    let mut update = vec!["submodule", "update", "--init"];
    if mode == "recursive" {
        update.push("--recursive");
    }
    if read(path, &["submodule", "init"]).await.is_some() && read(path, &update).await.is_some() {
        return;
    }
    // An unsuccessful clone can leave the other modules initialized with empty worktrees.
    let modules = read(
        path,
        &[
            "config",
            "-z",
            "--blob",
            "HEAD:.gitmodules",
            "--get-regexp",
            "^submodule\\..*\\.path$",
        ],
    )
    .await
    .unwrap_or_default();
    let cloned: Vec<_> = modules
        .split_terminator('\0')
        .filter_map(|s| s.split_once('\n').map(|(_, p)| p))
        .filter(|p| Path::new(path).join(p).join(".git").exists())
        .collect();
    if !cloned.is_empty() {
        update.extend(["--force", "--"]);
        update.extend(cloned);
        let _ = run(path, &update).await;
    }
}

// The registration can move while checkout runs. Git's unique lock reason
// identifies the worktree this operation owns even if its path is reused.
struct CreationLock {
    cwd: String,
    registrations: PathBuf,
    reason: String,
    branch: String,
}

impl CreationLock {
    async fn release(&self, remove: bool) {
        let Ok(entries) = std::fs::read_dir(&self.registrations) else {
            return;
        };
        for entry in entries.flatten() {
            let admin = entry.path();
            if !std::fs::read_to_string(admin.join("locked"))
                .is_ok_and(|v| v.trim_end() == self.reason)
            {
                continue;
            }
            let Ok(back) = std::fs::read_to_string(admin.join("gitdir")) else {
                continue;
            };
            let back = admin.join(back.trim_end());
            let Some(path) = back.parent().and_then(Path::to_str) else {
                continue;
            };
            if read(&self.cwd, &["worktree", "unlock", "--", path])
                .await
                .is_some()
                && remove
            {
                let _ = super::removal::remove(&self.cwd, path, true).await;
            }
        }
    }
}

// Cancelling an RPC between worktree add and sparse checkout must also
// release its registration. Without a matching reason another tree is safe.
struct PendingCheckout(Option<CreationLock>);
impl Drop for PendingCheckout {
    fn drop(&mut self) {
        if let Some(lock) = self.0.take() {
            tokio::spawn(async move {
                lock.release(true).await;
                let _ = run(&lock.cwd, &["branch", "-D", "--", &lock.branch]).await;
            });
        }
    }
}

pub(super) async fn create(
    cwd: &str,
    name: &str,
    new: Option<&str>,
    path: &str,
    common: &Path,
    submodules: Option<&str>,
) -> Result<(), Value> {
    let start = if name == "-" { "@{-1}" } else { name };
    let project = read(cwd, &["show", &format!("{start}:t3.json")])
        .await
        .and_then(|text| project_file(&text))
        .unwrap_or_default();
    let patterns = if let Some(file) = project["worktreeSparsePatternsPath"].as_str() {
        let file = file.trim();
        let blob = file.trim_start_matches("./");
        let output = if blob.split('/').any(|p| p == "..") {
            None
        } else {
            run(cwd, &["cat-file", "blob", &format!("{start}:{blob}")])
                .await
                .ok()
                .filter(|o| o.status.success())
        };
        Some(output.ok_or_else(|| json!({"_tag":"GitCommandError","operation":"GitVcsDriver.createWorktree","command":"git worktree add","cwd":cwd,
            "detail":format!("t3.json names the sparse-checkout pattern file \"{file}\", which is not a file in {name}.")}))?.stdout)
    } else {
        None
    };
    let mut pending = PendingCheckout(None);
    if let Some(new) = new {
        checked(
            cwd,
            "createWorktree.createBranch",
            &["branch", "--no-track", "--", new, start],
            "git branch failed",
        )
        .await?;
        if run(cwd, &["worktree", "add", "-h"]).await.is_ok_and(|out| {
            [out.stdout, out.stderr]
                .iter()
                .any(|bytes| String::from_utf8_lossy(bytes).contains("--reason"))
        }) {
            pending.0 = Some(CreationLock {
                cwd: cwd.into(),
                registrations: common.join("worktrees"),
                reason: format!("t3-create-{}", uuid::Uuid::new_v4()),
                branch: new.into(),
            });
        }
        if let Some(remote) = read(cwd, &["rev-parse", "--symbolic-full-name", start])
            .await
            .filter(|r| r.starts_with("refs/remotes/"))
        {
            let _ = read(
                cwd,
                &["branch", &format!("--set-upstream-to={remote}"), "--", new],
            )
            .await;
        }
    }
    let workers = read(cwd, &["config", "--get", "checkout.workers"])
        .await
        .unwrap_or_else(|| "0".into());
    let config = format!("checkout.workers={workers}");
    let mut args = vec!["-c", &config, "worktree", "add"];
    if patterns.is_some() {
        args.push("--no-checkout");
    }
    if let Some(lock) = &pending.0 {
        args.extend(["--lock", "--reason", &lock.reason]);
    }
    args.extend(["--", path, new.unwrap_or(name)]);
    let mut added = checked(cwd, "createWorktree", &args, "git worktree add failed").await;
    if let (Ok(_), Some(patterns)) = (&added, patterns) {
        added = sparse_checkout(path, &config, patterns).await;
        if added.is_err() {
            if let Some(lock) = &pending.0 {
                lock.release(true).await;
            } else {
                let _ = super::removal::remove(cwd, path, true).await;
            }
        }
    }
    if let Some(lock) = &pending.0 {
        lock.release(false).await;
    }
    pending.0.take();
    if let Err(error) = added {
        if let Some(new) = new {
            let _ = run(cwd, &["branch", "-D", "--", new]).await;
        }
        return Err(error);
    }
    if run(path, &["cat-file", "-e", "HEAD:.gitmodules"])
        .await
        .is_ok_and(|o| o.status.success())
    {
        initialize_submodules(
            path,
            submodules
                .or_else(|| project["worktreeSubmodules"].as_str())
                .unwrap_or("recursive"),
        )
        .await;
    }
    Ok(())
}
