use super::{git_error, read};
use serde_json::Value;
use std::{collections::HashSet, path::Path, process::Stdio};
use tokio::io::AsyncWriteExt;

#[derive(Default)]
pub(in crate::local_services) struct ReviewIndex {
    pub dir: Option<tempfile::TempDir>,
    pub omitted: bool,
}

pub(in crate::local_services) async fn prepare(
    cwd: &str,
    only_path: Option<&str>,
) -> Result<ReviewIndex, Value> {
    let mut result = ReviewIndex::default();
    let mut args = vec!["ls-files", "--others", "--exclude-standard", "-z", "--"];
    let pathspec = only_path.map(|p| format!(":(top,literal){p}"));
    args.extend(pathspec.as_deref());
    let Some(names) = read(cwd, &args).await else {
        result.omitted = true;
        return Ok(result);
    };
    if names.is_empty() {
        return Ok(result);
    }
    if names.split_terminator('\0').count() > 5000 {
        result.omitted = true;
        return Ok(result);
    }
    let deleted = read(
        cwd,
        &[
            "diff",
            "--cached",
            "--no-renames",
            "--name-only",
            "--diff-filter=D",
            "-z",
            "HEAD",
            "--",
        ],
    )
    .await
    .unwrap_or_default();
    let deleted: HashSet<_> = deleted.split_terminator('\0').collect();
    let mut files = Vec::new();
    for name in names
        .split_terminator('\0')
        .filter(|n| !deleted.contains(n))
    {
        let path = Path::new(cwd).join(name);
        if name.ends_with('/')
            && read(
                &path.to_string_lossy(),
                &["rev-parse", "--verify", "--quiet", "HEAD"],
            )
            .await
            .is_none()
        {
            continue;
        }
        let size = tokio::fs::symlink_metadata(path)
            .await
            .ok()
            .filter(|m| m.is_file())
            .map_or(0, |m| m.len());
        files.push((size, name));
    }
    files.sort_by_key(|f| f.0);
    let mut bytes = 0;
    let mut paths = Vec::new();
    for (size, name) in files {
        bytes += size;
        if bytes > 256 * 1024 * 1024 {
            result.omitted = true;
            break;
        }
        paths.extend_from_slice(name.as_bytes());
        paths.push(0);
    }
    if paths.is_empty() {
        return Ok(result);
    }

    let error = || {
        git_error(
            "GitVcsDriver.prepareReviewIndex",
            cwd,
            "Could not prepare the review index.",
        )
    };
    let dir = tempfile::tempdir().map_err(|_| error())?;
    let index = dir.path().join("index");
    let real = read(
        cwd,
        &["rev-parse", "--path-format=absolute", "--git-path", "index"],
    )
    .await
    .ok_or_else(error)?;
    if let Err(e) = tokio::fs::copy(&real, &index).await {
        if e.kind() != std::io::ErrorKind::NotFound {
            return Err(error());
        }
    }
    // Keep Git's racy-clean check conservative after copying the index.
    if let Ok(modified) = std::fs::metadata(&real).and_then(|m| m.modified()) {
        let file = std::fs::OpenOptions::new()
            .write(true)
            .open(&index)
            .map_err(|_| error())?;
        file.set_modified(modified).map_err(|_| error())?;
    }
    let cmd = || {
        let mut cmd = crate::login_shell::tokio_command("git");
        cmd.current_dir(cwd)
            .env("GIT_INDEX_FILE", &index)
            .args([
                "-c",
                "core.splitIndex=false",
                "-c",
                "splitIndex.sharedIndexExpire=never",
            ])
            .kill_on_drop(true);
        cmd
    };
    if !cmd()
        .args(["update-index", "--no-split-index"])
        .output()
        .await
        .map_err(|_| error())?
        .status
        .success()
    {
        return Err(error());
    }
    let mut child = cmd()
        .args([
            "-c",
            "core.sparseCheckout=false",
            "--literal-pathspecs",
            "add",
            "--intent-to-add",
            "--pathspec-from-file=-",
            "--pathspec-file-nul",
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .map_err(|_| error())?;
    child
        .stdin
        .take()
        .unwrap()
        .write_all(&paths)
        .await
        .map_err(|_| error())?;
    if !child.wait().await.map_err(|_| error())?.success() {
        return Err(error());
    }
    result.dir = Some(dir);
    Ok(result)
}
