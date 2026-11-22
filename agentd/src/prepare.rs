//! Run an optional hook command once per directory under `--root` before
//! agentd first uses that directory as a working directory.
use std::{
    collections::{HashMap, VecDeque},
    path::{Path, PathBuf},
    process::Stdio,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{io::AsyncReadExt, process::Command, sync::watch};

/// Default timeout for `--prepare-dir-command`, in seconds.
pub const DEFAULT_PREPARE_DIR_TIMEOUT_SECS: u64 = 300;

/// Environment variable set to the directory being prepared.
pub const PREPARE_DIR_ENV: &str = "T3_PREPARE_DIR";

/// Maximum number of trailing stderr bytes retained for failure/timeout logs.
const MAX_STDERR_TAIL_BYTES: usize = 4096;

/// Configuration for the optional directory-preparation hook.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PrepareDirConfig {
    /// Program run as `<command> <absolute-directory>` with [`PREPARE_DIR_ENV`] set.
    pub command: PathBuf,
    /// Maximum duration to wait for one invocation before terminating it.
    pub timeout: Duration,
}

enum Entry {
    Running(watch::Receiver<bool>),
    Done,
}

/// Coordinates at-most-once execution of the prepare-dir hook per directory.
pub(crate) struct DirPreparer {
    command: PathBuf,
    timeout: Duration,
    entries: Arc<Mutex<HashMap<PathBuf, Entry>>>,
}

impl DirPreparer {
    pub(crate) fn new(config: PrepareDirConfig) -> Self {
        Self {
            command: config.command,
            timeout: config.timeout,
            entries: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Ensure the hook has completed for `dir`.
    ///
    /// Returns `true` if this call ran or waited on an in-flight invocation, or
    /// `false` if `dir` was already prepared earlier in this process.
    pub(crate) async fn prepare(&self, dir: &Path) -> bool {
        let mut rx = {
            let mut entries = self.entries.lock().unwrap_or_else(|e| e.into_inner());
            match entries.get(dir) {
                Some(Entry::Done) => return false,
                Some(Entry::Running(rx)) => rx.clone(),
                None => {
                    let (tx, rx) = watch::channel(false);
                    entries.insert(dir.to_path_buf(), Entry::Running(rx.clone()));
                    let entries_ref = self.entries.clone();
                    let command = self.command.clone();
                    let timeout = self.timeout;
                    let target = dir.to_path_buf();
                    // Detach the hook from the initiating request's task so
                    // cancelling that request does not abort preparation for
                    // concurrent waiters.
                    tokio::spawn(async move {
                        let _guard = FinishGuard {
                            entries: entries_ref,
                            dir: target.clone(),
                            tx,
                        };
                        run_hook(&command, timeout, &target).await;
                    });
                    rx
                }
            }
        };
        let _ = rx.wait_for(|&done| done).await;
        true
    }
}

struct FinishGuard {
    entries: Arc<Mutex<HashMap<PathBuf, Entry>>>,
    dir: PathBuf,
    tx: watch::Sender<bool>,
}

impl Drop for FinishGuard {
    fn drop(&mut self) {
        self.entries
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(self.dir.clone(), Entry::Done);
        let _ = self.tx.send(true);
    }
}

#[derive(Default)]
struct StderrTail(VecDeque<u8>);

impl StderrTail {
    fn push(&mut self, chunk: &[u8]) {
        if chunk.len() >= MAX_STDERR_TAIL_BYTES {
            self.0.clear();
            self.0.extend(&chunk[chunk.len() - MAX_STDERR_TAIL_BYTES..]);
            return;
        }
        let overflow = (self.0.len() + chunk.len()).saturating_sub(MAX_STDERR_TAIL_BYTES);
        if overflow > 0 {
            self.0.drain(..overflow);
        }
        self.0.extend(chunk);
    }

    fn text(&self) -> String {
        let (front, back) = self.0.as_slices();
        let mut bytes = Vec::with_capacity(front.len() + back.len());
        bytes.extend_from_slice(front);
        bytes.extend_from_slice(back);
        String::from_utf8_lossy(&bytes).trim().to_owned()
    }
}

async fn collect_stderr(
    stderr_task: Option<tokio::task::JoinHandle<()>>,
    tail: &Mutex<StderrTail>,
    grace: Duration,
) -> String {
    if let Some(mut task) = stderr_task {
        if tokio::time::timeout(grace, &mut task).await.is_err() {
            task.abort();
        }
    }
    tail.lock().unwrap_or_else(|e| e.into_inner()).text()
}

async fn run_hook(command: &Path, timeout: Duration, dir: &Path) {
    let mut cmd = Command::new(command);
    cmd.arg(dir)
        .env(PREPARE_DIR_ENV, dir)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    let mut child = match cmd.spawn() {
        Ok(child) => child,
        Err(error) => {
            tracing::warn!(
                dir = %dir.display(),
                command = %command.display(),
                %error,
                "prepare-dir command failed to start"
            );
            return;
        }
    };
    let tail = Arc::new(Mutex::new(StderrTail::default()));
    let stderr_task = child.stderr.take().map(|mut stderr| {
        let tail = tail.clone();
        tokio::spawn(async move {
            let mut buf = [0u8; 4096];
            while let Ok(n) = stderr.read(&mut buf).await {
                if n == 0 {
                    break;
                }
                tail.lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .push(&buf[..n]);
            }
        })
    });
    match tokio::time::timeout(timeout, child.wait()).await {
        Ok(Ok(status)) if status.success() => {
            if let Some(mut task) = stderr_task {
                if tokio::time::timeout(Duration::from_millis(200), &mut task)
                    .await
                    .is_err()
                {
                    task.abort();
                }
            }
            tracing::debug!(
                dir = %dir.display(),
                command = %command.display(),
                "prepare-dir command succeeded"
            );
        }
        Ok(Ok(status)) => {
            let stderr = collect_stderr(stderr_task, &tail, Duration::from_secs(1)).await;
            tracing::warn!(
                dir = %dir.display(),
                command = %command.display(),
                %status,
                stderr = %stderr,
                "prepare-dir command exited with non-zero status"
            );
        }
        Ok(Err(error)) => {
            let stderr = collect_stderr(stderr_task, &tail, Duration::from_millis(200)).await;
            tracing::warn!(
                dir = %dir.display(),
                command = %command.display(),
                %error,
                stderr = %stderr,
                "prepare-dir command could not be waited on"
            );
        }
        Err(_) => {
            let _ = child.kill().await;
            let _ = child.wait().await;
            let stderr = collect_stderr(stderr_task, &tail, Duration::from_millis(200)).await;
            tracing::warn!(
                dir = %dir.display(),
                command = %command.display(),
                timeout = ?timeout,
                stderr = %stderr,
                "prepare-dir command timed out"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stderr_tail_keeps_only_the_trailing_bytes() {
        let mut tail = StderrTail::default();
        tail.push(b"first-");
        tail.push(&vec![b'x'; MAX_STDERR_TAIL_BYTES]);
        tail.push(b"-end\n");
        let text = tail.text();
        assert_eq!(text.len(), MAX_STDERR_TAIL_BYTES - 1);
        assert!(text.ends_with("-end"), "{text}");
        assert!(!text.contains("first"), "{text}");
    }
}
