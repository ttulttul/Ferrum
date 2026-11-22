use std::{path::Path, process::Stdio};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

pub(super) struct GitOutput {
    pub(super) code: i32,
    pub(super) paths: Vec<String>,
    pub(super) truncated: bool,
}

pub(super) fn git(
    root: &Path,
    args: &[&str],
    input: Option<Vec<u8>>,
    max_bytes: u64,
) -> Option<GitOutput> {
    // Drain stdout while writing paths. Either pipe can fill on large worktrees.
    tokio::runtime::Handle::current().block_on(async {
        let mut child = crate::login_shell::tokio_command("git")
            .args(["-c", "core.fsmonitor=false"])
            .args(args)
            .current_dir(root)
            .stdin(if input.is_some() {
                Stdio::piped()
            } else {
                Stdio::null()
            })
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .kill_on_drop(true)
            .spawn()
            .ok()?;
        let stdin = child.stdin.take();
        let mut stdout = child.stdout.take()?;
        let mut bytes = Vec::new();
        let write = async {
            if let (Some(mut stdin), Some(input)) = (stdin, input) {
                let _ = stdin.write_all(&input).await;
            }
        };
        let read = async {
            (&mut stdout)
                .take(max_bytes + 1)
                .read_to_end(&mut bytes)
                .await?;
            tokio::io::copy(&mut stdout, &mut tokio::io::sink()).await
        };
        let (_, read, status) = tokio::time::timeout(std::time::Duration::from_secs(10), async {
            tokio::join!(write, read, child.wait())
        })
        .await
        .ok()?;
        read.ok()?;
        let truncated = bytes.len() as u64 > max_bytes;
        bytes.truncate(max_bytes as usize);
        Some(GitOutput {
            code: status.ok()?.code()?,
            truncated,
            paths: String::from_utf8_lossy(&bytes)
                .split('\0')
                .filter(|p| !p.is_empty())
                .map(str::to_owned)
                .collect(),
        })
    })
}
pub(super) fn git_ignored(root: &Path, files_only: bool) -> Vec<String> {
    let mut args = vec![
        "ls-files",
        "--others",
        "--ignored",
        "--exclude-standard",
        "--directory",
        "-z",
    ];
    if files_only {
        args.push("--no-empty-directory");
    }
    git(
        root,
        &args,
        None,
        if files_only { 4 } else { 16 } * 1024 * 1024,
    )
    .filter(|output| output.code == 0 && (files_only || !output.truncated))
    .map(|output| output.paths)
    .unwrap_or_default()
}

pub(super) fn check_ignore<'a>(
    root: &Path,
    paths: impl Iterator<Item = &'a str>,
    no_index: bool,
) -> Option<GitOutput> {
    let mut args = vec!["check-ignore", "-z", "--stdin"];
    if no_index {
        args.push("--no-index");
    }
    let mut bytes = Vec::new();
    for path in paths {
        bytes.extend_from_slice(path.as_bytes());
        bytes.push(0);
    }
    git(root, &args, Some(bytes), 16 * 1024 * 1024)
}
