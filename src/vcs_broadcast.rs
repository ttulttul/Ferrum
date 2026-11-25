//! `subscribeVcsStatus` and `subscribeVcsBranch`, shared per folder, as the
//! reference's `VcsStatusBroadcaster` serves them.
//!
//! Every subscription to the same folder (with the same input) shares one
//! status, read by one task while anyone holds it. That task reads again only
//! when something can have changed it:
//! - the repository's `HEAD`, its log or `packed-refs` changed: a checkout,
//!   commit, reset or merge, as the reference's `HEAD` watch notices a
//!   checkout;
//! - a client or the server asked for a refresh of that folder
//!   (`vcs.refreshStatus` and the other `vcs.*` and `git.*` calls on it);
//! - a run in that folder finished, as the reference's run finalization
//!   refreshes it;
//! - for a status with its remote part, the automatic Git fetch interval
//!   (`automaticGitFetchInterval`, 30 s by default) passed.
//!
//! The repository files are checked by their modification times, in this
//! process, every two seconds: no git process runs while nothing changes. A
//! workspace whose folders are not on this machine cannot be checked that
//! way, so its shared status is read again every two seconds instead.

use serde_json::Value;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, SystemTime};
use tokio::sync::{broadcast, watch};

/// How often the repository's files are checked for a change.
const WATCH_PERIOD: Duration = Duration::from_secs(2);

type Snapshot = Option<Result<Value, Value>>;

type Entries = Arc<Mutex<HashMap<String, Weak<Entry>>>>;

/// The shared statuses, by stream and input.
#[derive(Default)]
pub(crate) struct Hub {
    entries: Entries,
    refreshes: Mutex<Option<broadcast::Sender<String>>>,
    /// Whether the task that refreshes a folder when a run in it finishes runs.
    finished_runs: std::sync::atomic::AtomicBool,
}

/// One shared status, alive while a subscriber holds it.
struct Entry {
    key: String,
    entries: Weak<Mutex<HashMap<String, Weak<Entry>>>>,
    latest: watch::Receiver<Snapshot>,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for Entry {
    fn drop(&mut self) {
        // The last subscriber left: stop reading and forget the status.
        self.task.abort();
        if let Some(entries) = self.entries.upgrade() {
            let mut entries = entries.lock().unwrap();
            if entries
                .get(&self.key)
                .is_some_and(|w| w.strong_count() == 0)
            {
                entries.remove(&self.key);
            }
        }
    }
}

/// One subscriber's view of a shared status.
pub(crate) struct Subscription {
    /// Holds the shared status open; dropping the last one stops it.
    _entry: Arc<Entry>,
    receiver: watch::Receiver<Snapshot>,
    first: bool,
}

impl Subscription {
    /// The status now, then each newer one. `None` once the status ends.
    pub(crate) async fn next(&mut self) -> Option<Result<Value, Value>> {
        loop {
            if !self.first {
                self.receiver.changed().await.ok()?;
            }
            self.first = false;
            if let Some(snapshot) = self.receiver.borrow_and_update().clone() {
                return Some(snapshot);
            }
        }
    }
}

impl Hub {
    fn refresh_sender(&self) -> broadcast::Sender<String> {
        self.refreshes
            .lock()
            .unwrap()
            .get_or_insert_with(|| broadcast::channel(256).0)
            .clone()
    }

    /// Asks every shared status of `cwd` to read again.
    pub(crate) fn refresh(&self, cwd: &str) {
        if let Some(sender) = self.refreshes.lock().unwrap().as_ref() {
            let _ = sender.send(cwd.to_owned());
        }
    }

    /// Joins the shared status for `method` and `input`, starting it if no one
    /// holds it.
    pub(crate) fn subscribe(
        &self,
        state: &Arc<crate::AppState>,
        method: &str,
        input: &Value,
    ) -> Subscription {
        if !self
            .finished_runs
            .swap(true, std::sync::atomic::Ordering::Relaxed)
        {
            tokio::spawn(refresh_finished_runs(
                Arc::downgrade(state),
                state.control.events.subscribe(),
            ));
        }
        let key = format!("{method}\u{0}{}", crate::wire_json::compact(input));
        let mut entries = self.entries.lock().unwrap();
        if let Some(entry) = entries.get(&key).and_then(Weak::upgrade) {
            let receiver = entry.latest.clone();
            return Subscription {
                _entry: entry,
                receiver,
                first: true,
            };
        }
        let (sender, receiver) = watch::channel(None);
        let task = tokio::spawn(drive(
            Arc::downgrade(state),
            method.to_owned(),
            input.clone(),
            sender,
            self.refresh_sender().subscribe(),
        ));
        let entry = Arc::new(Entry {
            key: key.clone(),
            entries: Arc::downgrade(&self.entries),
            latest: receiver.clone(),
            task,
        });
        entries.insert(key, Arc::downgrade(&entry));
        Subscription {
            _entry: entry,
            receiver,
            first: true,
        }
    }
}

/// A run that finished may have changed its folder (the reference's run
/// finalization refreshes it): its shared statuses read it again.
async fn refresh_finished_runs(
    state: Weak<crate::AppState>,
    mut events: broadcast::Receiver<crate::ServerEvent>,
) {
    loop {
        let record = match events.recv().await {
            Ok(record) => record,
            Err(broadcast::error::RecvError::Lagged(_)) => continue,
            Err(broadcast::error::RecvError::Closed) => return,
        };
        let crate::Event::RunFinished { thread_id, .. } = &record.event else {
            continue;
        };
        let Some(app) = state.upgrade() else { return };
        let root = app
            .control
            .store
            .read()
            .await
            .thread_workspace_root(thread_id);
        if let Some(root) = root {
            app.vcs_hub.refresh(&root);
        }
    }
}

/// Reads the status as the old per-subscription loop did.
async fn read(
    state: &crate::AppState,
    method: &str,
    input: &Value,
    initial: bool,
) -> Option<Result<Value, Value>> {
    crate::dispatch::polled_stream(
        &*state.workspace,
        &state.workspace_settings(method).await,
        method,
        input,
        initial,
    )
    .await
}

/// The automatic Git fetch interval, or `None` when it is off.
fn fetch_interval(state: &crate::AppState) -> Option<Duration> {
    let ms = state
        .settings
        .internal()
        .ok()
        .and_then(|settings| settings["automaticGitFetchInterval"].as_f64())
        .unwrap_or(30_000.0);
    (ms > 0.0).then(|| Duration::from_millis(ms as u64))
}

async fn drive(
    state: Weak<crate::AppState>,
    method: String,
    input: Value,
    sender: watch::Sender<Snapshot>,
    mut refreshes: broadcast::Receiver<String>,
) {
    let cwd = input["cwd"].as_str().unwrap_or_default().to_owned();
    let with_remote = method == "subscribeVcsStatus" && input["includeRemote"] != false;
    let Some(app) = state.upgrade() else { return };
    let local = app.workspace.watches_local_paths();
    // Watch before reading, so a change in between is still read.
    let mut files = if local {
        Some(RepositoryFiles::new(&cwd).await)
    } else {
        None
    };
    let mut seen = files.as_ref().map(RepositoryFiles::fingerprint);
    let mut snapshot = read(&app, &method, &input, true).await;
    let ended = !matches!(snapshot, Some(Ok(_)));
    // The first status reports the refs as last fetched; a status with its
    // remote part then reads them fresh, as the reference's first remote
    // refresh does.
    let start_remote = with_remote
        && snapshot
            .as_ref()
            .and_then(|s| s.as_ref().ok())
            .is_some_and(|s| s["local"]["isRepo"] == true);
    sender.send_replace(Some(snapshot.take().unwrap_or_else(|| {
        Err(serde_json::json!({"_tag":"Die","defect":format!("Unknown stream: {method}")}))
    })));
    if ended {
        return;
    }
    if start_remote {
        if let Some(next) = read(&app, &method, &input, false).await {
            publish(&sender, next);
        }
    }
    let fetch = if with_remote {
        fetch_interval(&app)
    } else {
        None
    };
    drop(app);
    let mut watch = tokio::time::interval(WATCH_PERIOD);
    watch.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    watch.tick().await;
    let mut remote = fetch.map(|every| {
        let mut interval = tokio::time::interval_at(tokio::time::Instant::now() + every, every);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        interval
    });
    loop {
        let fresh = tokio::select! {
            _ = watch.tick() => {
                match files.as_mut() {
                    // Not on this machine: read the shared status again.
                    None => Some(true),
                    Some(files) => {
                        files.reresolve_if_created().await;
                        let now = files.fingerprint();
                        if seen.as_ref() == Some(&now) {
                            None
                        } else {
                            seen = Some(now);
                            Some(true)
                        }
                    }
                }
            }
            request = refreshes.recv() => match request {
                Ok(folder) if folder == cwd => Some(true),
                Ok(_) => None,
                // Some refreshes were missed: one of them may have been ours.
                Err(broadcast::error::RecvError::Lagged(_)) => Some(true),
                Err(broadcast::error::RecvError::Closed) => return,
            },
            _ = async { remote.as_mut().unwrap().tick().await }, if remote.is_some() => Some(false),
        };
        let Some(cached_only) = fresh else { continue };
        let Some(app) = state.upgrade() else { return };
        // Fingerprinted before the read, so a change during it is read next.
        if let Some(files) = files.as_ref() {
            seen = Some(files.fingerprint());
        }
        let Some(next) = read(&app, &method, &input, cached_only).await else {
            return;
        };
        drop(app);
        let failed = next.is_err();
        publish(&sender, next);
        if failed {
            return;
        }
    }
}

fn publish(sender: &watch::Sender<Snapshot>, next: Result<Value, Value>) {
    sender.send_if_modified(|current| {
        if current.as_ref() == Some(&next) {
            return false;
        }
        *current = Some(next);
        true
    });
}

/// The files of a repository whose change means its status may have changed.
struct RepositoryFiles {
    cwd: PathBuf,
    paths: Vec<PathBuf>,
}

impl RepositoryFiles {
    async fn new(cwd: &str) -> Self {
        let mut files = Self {
            cwd: PathBuf::from(cwd),
            paths: Vec::new(),
        };
        files.resolve().await;
        files
    }

    /// Finds the repository's own folders once (`git rev-parse`). Outside a
    /// repository only `.git` itself is watched, so a new one is noticed.
    async fn resolve(&mut self) {
        let output = crate::login_shell::tokio_command("git")
            .args(["rev-parse", "--absolute-git-dir", "--git-common-dir"])
            .current_dir(&self.cwd)
            .stdin(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .kill_on_drop(true)
            .output()
            .await;
        let lines = output
            .ok()
            .filter(|output| output.status.success())
            .map(|output| String::from_utf8_lossy(&output.stdout).into_owned())
            .unwrap_or_default();
        let mut lines = lines.lines();
        self.paths = match (lines.next(), lines.next()) {
            (Some(git_dir), Some(common)) => {
                let git_dir = PathBuf::from(git_dir);
                let common = if Path::new(common).is_absolute() {
                    PathBuf::from(common)
                } else {
                    self.cwd.join(common)
                };
                // Not the index or FETCH_HEAD: reading the status rewrites
                // the index (git refreshes its stat data) and a remote read
                // writes FETCH_HEAD, so watching them would read again after
                // every read.
                vec![
                    git_dir.join("HEAD"),
                    git_dir.join("logs").join("HEAD"),
                    common.join("packed-refs"),
                ]
            }
            _ => vec![self.cwd.join(".git")],
        };
    }

    /// Outside a repository, a `.git` that appears makes it one.
    async fn reresolve_if_created(&mut self) {
        if self.paths.len() == 1 && self.paths[0].exists() {
            self.resolve().await;
        }
    }

    fn fingerprint(&self) -> Vec<Option<(SystemTime, u64)>> {
        self.paths
            .iter()
            .map(|path| {
                let metadata = std::fs::metadata(path).ok()?;
                Some((metadata.modified().ok()?, metadata.len()))
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn git(dir: &Path, args: &[&str]) {
        let status = std::process::Command::new("git")
            .args(["-c", "user.email=t@example.test", "-c", "user.name=T"])
            .args(args)
            .current_dir(dir)
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .status()
            .unwrap();
        assert!(status.success(), "git {args:?}");
    }

    fn repo() -> tempfile::TempDir {
        let dir = tempfile::tempdir().unwrap();
        git(dir.path(), &["init", "-qb", "main"]);
        std::fs::write(dir.path().join("a.txt"), "one\n").unwrap();
        git(dir.path(), &["add", "."]);
        git(dir.path(), &["commit", "-qm", "first"]);
        dir
    }

    async fn next_within(sub: &mut Subscription, seconds: u64) -> Option<Value> {
        tokio::time::timeout(Duration::from_secs(seconds), sub.next())
            .await
            .ok()
            .flatten()
            .map(Result::unwrap)
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn subscribers_of_a_folder_share_one_status_until_the_last_leaves() {
        let data = tempfile::tempdir().unwrap();
        let state = crate::AppState::new(data.path(), Vec::new()).await.unwrap();
        let repo = repo();
        let input = json!({"cwd": repo.path(), "includeRemote": false});
        let mut a = state
            .vcs_hub
            .subscribe(&state, "subscribeVcsBranch", &input);
        let mut b = state
            .vcs_hub
            .subscribe(&state, "subscribeVcsBranch", &input);
        let first = next_within(&mut a, 30).await.unwrap();
        assert_eq!(first["refName"], "main");
        assert_eq!(next_within(&mut b, 5).await.unwrap(), first);
        assert_eq!(state.vcs_hub.entries.lock().unwrap().len(), 1);
        drop(a);
        assert_eq!(state.vcs_hub.entries.lock().unwrap().len(), 1);
        drop(b);
        assert!(state.vcs_hub.entries.lock().unwrap().is_empty());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_checkout_is_noticed_through_the_repository_files() {
        let data = tempfile::tempdir().unwrap();
        let state = crate::AppState::new(data.path(), Vec::new()).await.unwrap();
        let repo = repo();
        let input = json!({"cwd": repo.path()});
        let mut sub = state
            .vcs_hub
            .subscribe(&state, "subscribeVcsBranch", &input);
        assert_eq!(next_within(&mut sub, 30).await.unwrap()["refName"], "main");
        git(repo.path(), &["checkout", "-qb", "feature"]);
        let next = next_within(&mut sub, 10).await.unwrap();
        assert_eq!(next["refName"], "feature");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_working_tree_edit_is_read_on_a_refresh_not_by_polling() {
        let data = tempfile::tempdir().unwrap();
        let state = crate::AppState::new(data.path(), Vec::new()).await.unwrap();
        let repo = repo();
        let cwd = repo.path().to_str().unwrap().to_owned();
        let input = json!({"cwd": cwd, "includeRemote": false});
        let mut sub = state
            .vcs_hub
            .subscribe(&state, "subscribeVcsStatus", &input);
        let first = next_within(&mut sub, 30).await.unwrap();
        assert_eq!(first["local"]["isRepo"], true);
        std::fs::write(repo.path().join("a.txt"), "one\ntwo\n").unwrap();
        // No git file changed, so nothing reads the folder again.
        assert!(next_within(&mut sub, 5).await.is_none());
        state.vcs_hub.refresh(&cwd);
        let next = next_within(&mut sub, 10).await.unwrap();
        assert_ne!(next["local"], first["local"]);
    }
}
