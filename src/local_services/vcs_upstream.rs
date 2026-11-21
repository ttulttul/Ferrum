//! The upstream fetch a status read starts, as `refreshStatusUpstreamIfStale` in the reference
//! `GitVcsDriverCore.ts` does: when the checked-out branch tracks a remote, fetch that remote at
//! most every 15 s (backing off after failures), wait up to 5 s for it, and otherwise report
//! status from the refs already fetched while the fetch finishes in the background.
//!
//! Besides moving remote-tracking refs, this fetch is how a newer Git (2.48+, whose
//! `remote.<name>.followRemoteHEAD` defaults to `create`) records `refs/remotes/origin/HEAD`, which
//! `vcs.listRefs` and status use to pick the default branch.

use super::vcs::read;
use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    process::Stdio,
    sync::{Mutex, OnceLock},
    time::{Duration, Instant},
};
use tokio::sync::{watch, Semaphore};

const REFRESH_INTERVAL: Duration = Duration::from_secs(15);
const JOIN_TIMEOUT: Duration = Duration::from_secs(5);
const FETCH_TIMEOUT: Duration = Duration::from_secs(5 * 60);
const FAILURE_BASE_COOLDOWN: Duration = Duration::from_secs(30);
const FAILURE_MAX_COOLDOWN: Duration = Duration::from_secs(15 * 60);
const CACHE_CAPACITY: usize = 2_048;
const CONCURRENT_FETCHES: usize = 4;
/// `STATUS_UPSTREAM_REFRESH_ENV`: a background fetch never prompts.
const FETCH_ENV: [(&str, &str); 5] = [
    ("GCM_INTERACTIVE", "never"),
    ("GIT_ASKPASS", ""),
    ("GIT_TERMINAL_PROMPT", "0"),
    ("SSH_ASKPASS", ""),
    ("SSH_ASKPASS_REQUIRE", "never"),
];

type Key = (PathBuf, String);

#[derive(Default)]
struct Refresh {
    fresh_until: Option<Instant>,
    failures: u32,
    retry_at: Option<Instant>,
    running: Option<watch::Receiver<bool>>,
}

fn refreshes() -> &'static Mutex<HashMap<Key, Refresh>> {
    static REFRESHES: OnceLock<Mutex<HashMap<Key, Refresh>>> = OnceLock::new();
    REFRESHES.get_or_init(Default::default)
}

fn failure_cooldown(failures: u32) -> Duration {
    let exponent = failures.saturating_sub(1).min(16);
    FAILURE_BASE_COOLDOWN
        .saturating_mul(1 << exponent)
        .min(FAILURE_MAX_COOLDOWN)
}

/// `rev-parse --git-common-dir`, resolved against the physical cwd as the reference does.
async fn git_common_dir(cwd: &str) -> Option<PathBuf> {
    let output = read(cwd, &["rev-parse", "--git-common-dir"]).await?;
    let output = output.trim();
    if output.is_empty() {
        return None;
    }
    let path = Path::new(output);
    if path.is_absolute() {
        return Some(path.to_path_buf());
    }
    let physical = std::fs::canonicalize(cwd).unwrap_or_else(|_| PathBuf::from(cwd));
    Some(physical.join(path))
}

async fn fetch(common_dir: &Path, remote: &str) -> bool {
    let fetch_cwd = if common_dir.file_name().is_some_and(|name| name == ".git") {
        common_dir.parent().unwrap_or(common_dir)
    } else {
        common_dir
    };
    let mut command = crate::login_shell::tokio_command("git");
    command
        .current_dir(fetch_cwd)
        .arg("--git-dir")
        .arg(common_dir)
        .args([
            "fetch",
            "--quiet",
            "--no-tags",
            // Keeps a poll from starting `git gc --auto`, which can fill a disk with tmp packs.
            "--no-auto-gc",
            // FETCH_HEAD is written without a lock; a user's concurrent `git pull` would fail.
            "--no-write-fetch-head",
            "--",
            remote,
        ])
        .env("LC_ALL", "C")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .kill_on_drop(true);
    for (name, value) in FETCH_ENV {
        command.env(name, value);
    }
    match tokio::time::timeout(FETCH_TIMEOUT, command.status()).await {
        Ok(Ok(status)) => status.success(),
        _ => false,
    }
}

async fn run(key: Key, done: watch::Sender<bool>) {
    static PROCESSES: Semaphore = Semaphore::const_new(CONCURRENT_FETCHES);
    let succeeded = match PROCESSES.acquire().await {
        Ok(_permit) => fetch(&key.0, &key.1).await,
        Err(_) => false,
    };
    if !succeeded {
        tracing::warn!(
            git_dir = %key.0.display(),
            remote = %key.1,
            "background Git fetch failed"
        );
    }
    {
        let mut map = refreshes().lock().unwrap_or_else(|e| e.into_inner());
        let entry = map.entry(key).or_default();
        let now = Instant::now();
        entry.running = None;
        if succeeded {
            entry.failures = 0;
            entry.retry_at = None;
            entry.fresh_until = Some(now + REFRESH_INTERVAL);
        } else {
            entry.failures = entry.failures.saturating_add(1);
            entry.retry_at = Some(now + failure_cooldown(entry.failures));
            entry.fresh_until = None;
        }
    }
    let _ = done.send(true);
}

/// Fetches the remote the checked-out branch tracks unless that happened in the last 15 s.
pub(super) async fn refresh_if_stale(cwd: &str) {
    let Some(head_ref) = read(cwd, &["symbolic-ref", "--quiet", "HEAD"])
        .await
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
    else {
        return;
    };
    let remote = read(
        cwd,
        &["for-each-ref", "--format=%(upstream:remotename)", &head_ref],
    )
    .await
    .unwrap_or_default();
    let remote = remote.trim();
    // Empty means no upstream; `.` means the upstream is a local branch.
    if remote.is_empty() || remote == "." {
        return;
    }
    let Some(common_dir) = git_common_dir(cwd).await else {
        return;
    };
    let key = (common_dir, remote.to_owned());
    let mut running = {
        let mut map = refreshes().lock().unwrap_or_else(|e| e.into_inner());
        let now = Instant::now();
        if map.len() >= CACHE_CAPACITY && !map.contains_key(&key) {
            map.retain(|_, entry| {
                entry.running.is_some()
                    || entry.fresh_until.is_some_and(|t| now < t)
                    || entry.retry_at.is_some_and(|t| now < t)
            });
        }
        let entry = map.entry(key.clone()).or_default();
        if let Some(running) = &entry.running {
            running.clone()
        } else if entry.fresh_until.is_some_and(|t| now < t)
            || entry.retry_at.is_some_and(|t| now < t)
        {
            return;
        } else {
            let (done, running) = watch::channel(false);
            entry.running = Some(running.clone());
            tokio::spawn(run(key, done));
            running
        }
    };
    // A slow fetch keeps running; status then uses the refs fetched last time.
    let _ = tokio::time::timeout(JOIN_TIMEOUT, running.wait_for(|done| *done)).await;
}

#[cfg(test)]
mod tests {
    use super::*;

    fn git(cwd: &Path, args: &[&str]) -> String {
        let output = std::process::Command::new("git")
            .current_dir(cwd)
            .args(args)
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .env("GIT_AUTHOR_NAME", "Fixture")
            .env("GIT_AUTHOR_EMAIL", "fixture@localhost")
            .env("GIT_COMMITTER_NAME", "Fixture")
            .env("GIT_COMMITTER_EMAIL", "fixture@localhost")
            .output()
            .unwrap();
        assert!(output.status.success(), "git {args:?}: {output:?}");
        String::from_utf8(output.stdout).unwrap().trim().to_owned()
    }

    #[test]
    fn failures_back_off_exponentially_up_to_fifteen_minutes() {
        assert_eq!(failure_cooldown(1), Duration::from_secs(30));
        assert_eq!(failure_cooldown(2), Duration::from_secs(60));
        assert_eq!(failure_cooldown(5), Duration::from_secs(480));
        assert_eq!(failure_cooldown(6), FAILURE_MAX_COOLDOWN);
        assert_eq!(failure_cooldown(500), FAILURE_MAX_COOLDOWN);
    }

    #[tokio::test]
    async fn fetches_the_tracked_remote_once_per_interval() {
        let root = tempfile::tempdir().unwrap();
        let remote = root.path().join("remote.git");
        let repo = root.path().join("repo");
        let other = root.path().join("other");
        git(
            root.path(),
            &[
                "init",
                "-q",
                "--bare",
                "-b",
                "main",
                remote.to_str().unwrap(),
            ],
        );
        git(
            root.path(),
            &["init", "-q", "-b", "main", repo.to_str().unwrap()],
        );
        std::fs::write(repo.join("a.txt"), "a\n").unwrap();
        git(&repo, &["add", "-A"]);
        git(&repo, &["commit", "-qm", "a"]);
        git(
            &repo,
            &["remote", "add", "origin", remote.to_str().unwrap()],
        );
        git(&repo, &["push", "-q", "-u", "origin", "main"]);
        git(
            root.path(),
            &[
                "clone",
                "-q",
                remote.to_str().unwrap(),
                other.to_str().unwrap(),
            ],
        );
        std::fs::write(other.join("b.txt"), "b\n").unwrap();
        git(&other, &["add", "-A"]);
        git(&other, &["commit", "-qm", "b"]);
        git(&other, &["push", "-q", "origin", "main"]);
        let pushed = git(&other, &["rev-parse", "HEAD"]);
        let cwd = repo.to_str().unwrap();

        refresh_if_stale(cwd).await;
        assert_eq!(git(&repo, &["rev-parse", "origin/main"]), pushed);
        // Unlike a plain fetch, the status fetch leaves no FETCH_HEAD behind.
        assert!(!repo.join(".git/FETCH_HEAD").exists());

        // Within the interval the remote is not fetched again.
        std::fs::write(other.join("c.txt"), "c\n").unwrap();
        git(&other, &["add", "-A"]);
        git(&other, &["commit", "-qm", "c"]);
        git(&other, &["push", "-q", "origin", "main"]);
        refresh_if_stale(cwd).await;
        assert_eq!(git(&repo, &["rev-parse", "origin/main"]), pushed);
    }

    #[tokio::test]
    async fn branches_without_a_remote_upstream_are_not_fetched() {
        let root = tempfile::tempdir().unwrap();
        let repo = root.path().join("repo");
        git(
            root.path(),
            &["init", "-q", "-b", "main", repo.to_str().unwrap()],
        );
        std::fs::write(repo.join("a.txt"), "a\n").unwrap();
        git(&repo, &["add", "-A"]);
        git(&repo, &["commit", "-qm", "a"]);
        // A local upstream (`.`) and no upstream both skip the fetch.
        git(&repo, &["branch", "--track", "topic", "main"]);
        git(&repo, &["checkout", "-q", "topic"]);
        refresh_if_stale(repo.to_str().unwrap()).await;
        git(&repo, &["checkout", "-q", "main"]);
        refresh_if_stale(repo.to_str().unwrap()).await;
        let git_dir = std::fs::canonicalize(&repo).unwrap().join(".git");
        let map = refreshes().lock().unwrap();
        assert!(!map.keys().any(|(dir, _)| *dir == git_dir));
    }
}
