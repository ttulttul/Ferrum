#[cfg(any(windows, test))]
use super::read;
use super::{checked, checked_output, failure_context, lexical_path, run, Reply};
use serde_json::{json, Value};
use std::{
    fs, io,
    path::{Path, PathBuf},
};

fn refusal(cwd: &str, detail: String) -> Value {
    json!({"_tag":"GitCommandError","operation":"GitVcsDriver.removeWorktree","command":"git worktree remove","cwd":cwd,"detail":format!("fatal: {detail}")})
}

fn same_path(a: &Path, b: &Path) -> bool {
    match (a.canonicalize(), b.canonicalize()) {
        (Ok(a), Ok(b)) if cfg!(windows) => a
            .to_string_lossy()
            .eq_ignore_ascii_case(&b.to_string_lossy()),
        (Ok(a), Ok(b)) => a == b,
        _ => false,
    }
}

// Annex uses a directory symlink in place of the normal .git file. Confirm the
// registration points back before replacing the link, and restore it on refusal.
struct AnnexLink {
    file: PathBuf,
    link: PathBuf,
    contents: String,
}

fn unlink(path: &Path) -> io::Result<()> {
    #[cfg(windows)]
    {
        use std::os::windows::fs::MetadataExt;
        // Both symlink directories and junctions carry FILE_ATTRIBUTE_DIRECTORY.
        if fs::symlink_metadata(path)?.file_attributes() & 0x10 != 0 {
            return fs::remove_dir(path);
        }
    }
    fs::remove_file(path)
}

impl AnnexLink {
    fn replace(target: &Path) -> io::Result<Option<Self>> {
        let file = target.join(".git");
        let Ok(link) = fs::read_link(&file) else {
            return Ok(None);
        };
        let admin = target.join(&link);
        let Ok(back) = fs::read_to_string(admin.join("gitdir")) else {
            return Ok(None);
        };
        if !admin
            .join(back.trim())
            .parent()
            .is_some_and(|p| same_path(p, target))
        {
            return Ok(None);
        }
        let contents = format!("gitdir: {}\n", admin.display());
        unlink(&file)?;
        let guard = Self {
            file,
            link,
            contents,
        };
        fs::write(&guard.file, &guard.contents)?;
        Ok(Some(guard))
    }
}
impl Drop for AnnexLink {
    fn drop(&mut self) {
        if !self.file.parent().is_some_and(Path::is_dir) {
            return;
        }
        // Preserve anything another writer installed in the meantime.
        match fs::read_to_string(&self.file) {
            Ok(s) if s == self.contents => {
                if fs::remove_file(&self.file).is_err() {
                    return;
                }
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            _ => return,
        }
        #[cfg(windows)]
        let result = std::os::windows::fs::symlink_dir(&self.link, &self.file);
        #[cfg(not(windows))]
        let result = std::os::unix::fs::symlink(&self.link, &self.file);
        if result.is_err() {
            let _ = fs::write(&self.file, &self.contents);
        }
    }
}

// Older Git for Windows recurses through directory junctions. Verify Git's
// ownership, lock, submodule and dirty-state checks before using remove_dir_all,
// which unlinks reparse points. Tests also exercise the checks on Unix.
#[cfg(any(windows, test))]
async fn delete_windows_worktree(cwd: &str, target: &Path, force: bool) -> Result<(), Value> {
    let path = target.to_string_lossy();
    let file = target.join(".git");
    let not_linked = || refusal(cwd, format!("'{path}' is not a working tree"));
    let admin = PathBuf::from(
        read(&path, &["rev-parse", "--absolute-git-dir"])
            .await
            .ok_or_else(not_linked)?,
    );
    let common = read(cwd, &["rev-parse", "--git-common-dir"])
        .await
        .ok_or_else(not_linked)?;
    let back = fs::read_to_string(admin.join("gitdir")).map_err(|_| not_linked())?;
    if !same_path(&admin.join(back.trim()), &file)
        || !admin
            .parent()
            .is_some_and(|p| same_path(p, &Path::new(cwd).join(common).join("worktrees")))
    {
        return Err(not_linked());
    }
    match fs::symlink_metadata(admin.join("locked")) {
        Ok(_) => {
            let reason = fs::read_to_string(admin.join("locked")).unwrap_or_default();
            let reason = reason.trim();
            let note = if reason.is_empty() {
                String::new()
            } else {
                format!(", lock reason: {reason}")
            };
            return Err(refusal(cwd,format!("cannot remove a locked working tree{note}\nuse 'remove -f -f' to override or unlock first")));
        }
        Err(e) if e.kind() == io::ErrorKind::NotFound => {}
        Err(_) => return Err(refusal(cwd, format!("cannot inspect the lock of '{path}'"))),
    }
    if !force {
        let file = file.to_string_lossy();
        let bound = |args: &[&str]| {
            let mut cmd = crate::login_shell::tokio_command("git");
            cmd.current_dir(target)
                .env("GIT_DIR", file.as_ref())
                .env("GIT_WORK_TREE", target)
                .env("LC_ALL", "C")
                .args(args)
                .kill_on_drop(true);
            cmd.output()
        };
        let args = ["ls-files", "--stage", "-z"];
        let index = checked_output(
            bound(&args).await,
            failure_context("removeWorktree", cwd, &args),
            "git ls-files failed",
        )?;
        let populated = index
            .split_terminator('\0')
            .filter(|s| s.starts_with("160000 "))
            .filter_map(|s| s.split_once('\t'))
            .any(|(_, p)| target.join(p).join(".git").try_exists().unwrap_or(true));
        if admin.join("modules").try_exists().unwrap_or(true) || populated {
            return Err(refusal(
                cwd,
                "working trees containing submodules cannot be moved or removed".into(),
            ));
        }
        let args = [
            "-c",
            "status.showUntrackedFiles=normal",
            "status",
            "--porcelain",
            "--ignore-submodules=none",
        ];
        if !checked_output(
            bound(&args).await,
            failure_context("removeWorktree", cwd, &args),
            "git status failed",
        )?
        .is_empty()
        {
            return Err(refusal(
                cwd,
                format!("'{path}' contains modified or untracked files, use --force to delete it"),
            ));
        }
    }
    let contents = fs::read(&file).map_err(|_| not_linked())?;
    fs::remove_dir_all(target).map_err(|_| {
        if !file.exists() {
            let _ = fs::write(&file, contents);
        }
        refusal(cwd, format!("failed to delete '{path}'"))
    })
}

fn remove_leftover_links(path: &Path) -> io::Result<()> {
    let kind = fs::symlink_metadata(path)?.file_type();
    if kind.is_symlink() || fs::read_link(path).is_ok() {
        return unlink(path);
    }
    if kind.is_dir() {
        for entry in fs::read_dir(path)? {
            remove_leftover_links(&entry?.path())?;
        }
        match fs::remove_dir(path) {
            Err(e) if e.kind() == io::ErrorKind::DirectoryNotEmpty => {}
            result => return result,
        }
    }
    Ok(())
}

pub(super) async fn remove(cwd: &str, path: &str, force: bool) -> Reply {
    let target = lexical_path(
        &std::path::absolute(Path::new(cwd).join(path))
            .map_err(|e| json!({"_tag":"Die","defect":e.to_string()}))?,
    );
    let path = target.to_string_lossy();
    let _annex = AnnexLink::replace(&target).map_err(|_| {
        refusal(
            cwd,
            format!("failed to replace '{}'", target.join(".git").display()),
        )
    })?;
    let mut args = vec![
        "-c",
        "status.showUntrackedFiles=normal",
        "worktree",
        "remove",
    ];
    if force {
        args.push("--force");
    }
    #[cfg(windows)]
    if target.join(".git").is_file() {
        delete_windows_worktree(cwd, &target, force).await?;
        args = vec!["worktree", "remove", "--force", "--force"];
    }
    args.extend(["--", &path]);
    let result = run(cwd, &args).await;
    if let Ok(out) = &result {
        let message = String::from_utf8_lossy(&out.stderr);
        if !out.status.success()
            && (message.contains("is not a working tree")
                || message.contains("cannot remove working tree"))
            && matches!(target.try_exists(), Ok(false))
        {
            checked(
                cwd,
                "pruneWorktrees",
                &["worktree", "prune"],
                "git worktree prune failed",
            )
            .await?;
            return Ok(Value::Null);
        }
    }
    checked_output(
        result,
        failure_context("removeWorktree", cwd, &args),
        "git worktree remove failed",
    )?;
    if target.exists() {
        let _ = remove_leftover_links(&target);
    }
    Ok(Value::Null)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn windows_removal_checks_registration_lock_submodules_and_dirtiness_before_deletion() {
        let temp = tempfile::tempdir().unwrap();
        let repo = temp.path().join("repo");
        let foreign = temp.path().join("foreign");
        let target = temp.path().join("worktree");
        for dir in [&repo, &foreign] {
            fs::create_dir(dir).unwrap();
            let cwd = dir.to_str().unwrap();
            assert!(read(cwd, &["init", "-q"]).await.is_some());
            assert!(read(
                cwd,
                &[
                    "-c",
                    "user.name=Test",
                    "-c",
                    "user.email=test@example.test",
                    "commit",
                    "--allow-empty",
                    "-qm",
                    "base"
                ]
            )
            .await
            .is_some());
        }
        let cwd = repo.to_str().unwrap();
        let path = target.to_str().unwrap();
        assert!(read(cwd, &["worktree", "add", "-qb", "topic", path])
            .await
            .is_some());
        assert!(
            delete_windows_worktree(foreign.to_str().unwrap(), &target, true)
                .await
                .is_err()
        );
        read(cwd, &["worktree", "lock", "--reason", "fixture", path])
            .await
            .unwrap();
        assert!(delete_windows_worktree(cwd, &target, true)
            .await
            .unwrap_err()["detail"]
            .as_str()
            .unwrap()
            .contains("lock reason: fixture"));
        read(cwd, &["worktree", "unlock", path]).await.unwrap();
        let modules = PathBuf::from(
            read(path, &["rev-parse", "--absolute-git-dir"])
                .await
                .unwrap(),
        )
        .join("modules");
        fs::create_dir(&modules).unwrap();
        assert!(delete_windows_worktree(cwd, &target, false)
            .await
            .unwrap_err()["detail"]
            .as_str()
            .unwrap()
            .contains("submodules"));
        fs::remove_dir(modules).unwrap();
        fs::write(target.join("new"), "retain").unwrap();
        read(path, &["config", "status.showUntrackedFiles", "no"])
            .await
            .unwrap();
        assert!(delete_windows_worktree(cwd, &target, false).await.is_err());
        assert_eq!(fs::read(target.join("new")).unwrap(), b"retain");
        let outside = temp.path().join("outside");
        fs::create_dir(&outside).unwrap();
        fs::write(outside.join("keep"), "external").unwrap();
        #[cfg(windows)]
        assert!(std::process::Command::new("cmd")
            .current_dir(&target)
            .args(["/d", "/c", "mklink", "/J", "junction", "..\\\\outside"])
            .status()
            .unwrap()
            .success());
        #[cfg(unix)]
        std::os::unix::fs::symlink(&outside, target.join("junction")).unwrap();
        delete_windows_worktree(cwd, &target, true).await.unwrap();
        assert!(!target.exists());
        assert_eq!(fs::read(outside.join("keep")).unwrap(), b"external");
        assert!(foreign.join(".git").is_dir());
    }
}
