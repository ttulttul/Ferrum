use super::policy::{Eligibility, Rules};
use std::path::Path;
use std::process::Stdio;

fn command(cwd: &Path, args: &[&str]) -> std::io::Result<std::process::Output> {
    crate::login_shell::command("git")
        .current_dir(cwd)
        .args(args)
        .env("LC_ALL", "C")
        .env("GIT_TERMINAL_PROMPT", "0")
        .stdin(Stdio::null())
        .output()
}
pub fn linked(path: &Path) -> bool {
    path.join(".git").is_file() || path.join(".git").is_symlink()
}
fn read(cwd: &Path, args: &[&str]) -> anyhow::Result<String> {
    let out = command(cwd, args)?;
    anyhow::ensure!(
        out.status.success(),
        "git {}: {}",
        args.join(" "),
        String::from_utf8_lossy(&out.stderr).trim()
    );
    Ok(String::from_utf8_lossy(&out.stdout).trim().to_owned())
}
pub fn changes(path: &Path, rules: &Rules) -> anyhow::Result<Option<String>> {
    let args = if rules.worktree_keep_when == "tracked-changes" {
        [
            "status",
            "--porcelain=v1",
            "-z",
            "--ignore-submodules=none",
            "--untracked-files=no",
        ]
    } else {
        [
            "status",
            "--porcelain=v1",
            "-z",
            "--ignore-submodules=none",
            "--untracked-files=all",
        ]
    };
    let out = command(path, &args)?;
    anyhow::ensure!(out.status.success(), "git status failed");
    let rows = String::from_utf8_lossy(&out.stdout)
        .split(char::from(0))
        .filter(|s| !s.is_empty())
        .count();
    if rows > 0 {
        return Ok(Some(format!(
            "Has uncommitted changes ({rows} {})",
            if rows == 1 { "file" } else { "files" }
        )));
    }
    Ok(None)
}
fn default_branch(project: &Path) -> Option<(String, String)> {
    let value = read(
        project,
        &[
            "symbolic-ref",
            "--quiet",
            "--short",
            "refs/remotes/origin/HEAD",
        ],
    )
    .ok()?;
    Some(("origin".into(), value))
}
pub fn integration(
    path: &Path,
    project: Option<&Path>,
    rules: &Rules,
    eligibility: &Eligibility,
) -> anyhow::Result<Option<String>> {
    let Some(project) = project else {
        return Ok(None);
    };
    let (remote, default) = default_branch(project).unwrap_or_else(|| {
        (
            "".into(),
            read(project, &["symbolic-ref", "--quiet", "--short", "HEAD"]).unwrap_or_default(),
        )
    });
    if default.is_empty() {
        return Ok(None);
    }
    let base = if remote.is_empty() {
        default.clone()
    } else {
        format!("refs/remotes/{remote}/{default}")
    };
    let head = read(path, &["rev-parse", "HEAD"])?;
    let base_sha = read(project, &["rev-parse", &base])
        .or_else(|_| read(project, &["rev-parse", &default]))?;
    let integrated = command(path, &["merge-base", "--is-ancestor", &head, &base_sha])?
        .status
        .success();
    if eligibility.unchanged && integrated {
        return Ok(Some(
            if remote.is_empty() {
                "No commits beyond the project branch"
            } else {
                "No commits beyond the default branch"
            }
            .into(),
        ));
    }
    if rules.worktree_on_merge { /* PR metadata is unavailable without hosted credentials */ }
    Ok(None)
}
pub fn remove(path: &Path, force: bool) -> anyhow::Result<()> {
    let common = read(path, &["rev-parse", "--git-common-dir"])?;
    let root = path.join(common).canonicalize()?;
    let root = if root.file_name().is_some_and(|n| n == ".git") {
        root.parent().unwrap()
    } else {
        &root
    };
    let mut args = vec![
        "-c",
        "status.showUntrackedFiles=normal",
        "worktree",
        "remove",
    ];
    if force {
        args.push("--force");
    }
    args.extend(["--", path.to_str().unwrap()]);
    let out = command(root, &args)?;
    anyhow::ensure!(
        out.status.success(),
        "git worktree remove: {}",
        String::from_utf8_lossy(&out.stderr).trim()
    );
    Ok(())
}
