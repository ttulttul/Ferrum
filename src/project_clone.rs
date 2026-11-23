//! Clones that become projects, followed while they run (`ProjectCloneTracker`
//! in the reference): `projectClone.start`, `.cancel` and `.retry`, and the
//! snapshots `subscribeProjectClones` streams.
//!
//! A clone's project is created first, on its empty destination, so threads
//! and deletes see it at once; git then runs without a deadline, its progress
//! lines become the snapshot's stage, percent and detail, and twenty seconds
//! without one mark it stalled. Cancel stops git and empties the destination;
//! retry empties it and starts again; a finished clone is forgotten after
//! thirty seconds.
//!
//! The tracker itself only runs git; where the project comes from is the
//! host's. The native server creates it in-process ([`Tracker::start`]); a
//! workspace served by `t3-agentd` answers `clone.call` with
//! [`Tracker::call`] and the control plane creates the project between the
//! `claim` and the `launch`.
use crate::source_control_clone::{self as clone, CloneInput, CloneOptions, Progress};
use crate::AppState;
use indexmap::IndexMap;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::future::Future;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{broadcast, oneshot, Notify};

const DONE_RETENTION: Duration = Duration::from_secs(30);
// A claim whose host never launched it (the host stopped in between) stops
// holding its destination after this long.
#[cfg(not(test))]
const CLAIM_EXPIRY: Duration = Duration::from_secs(120);
#[cfg(test)]
const CLAIM_EXPIRY: Duration = Duration::from_millis(100);
// Git prints nothing while a transfer waits on the network, and the kernel can
// take many minutes to give up on a dead connection.
const STALL_AFTER: Duration = Duration::from_secs(20);
const DETAIL_MAX_LENGTH: usize = 200;
const ERROR_MAX_LENGTH: usize = 1000;

/// A clone that is running: how to stop it, and when it has stopped.
struct Running {
    cancel: oneshot::Sender<()>,
    stopped: oneshot::Receiver<()>,
}

struct Tracked {
    snapshot: Value,
    running: Option<Running>,
    /// What git is given; may carry credentials and never leaves the server.
    clone_url: String,
    destination: String,
    protocol: String,
}

#[derive(Default)]
struct Clones {
    tracked: IndexMap<String, Tracked>,
    sequence: u64,
    retention: HashMap<String, tokio::task::AbortHandle>,
}

/// Every tracked clone, and the changes subscribers follow.
pub(crate) struct Tracker {
    clones: Mutex<Clones>,
    changes: broadcast::Sender<Value>,
    /// Serializes start, cancel, retry and discard on the shared entries and folders.
    actions: tokio::sync::Mutex<()>,
}

impl Default for Tracker {
    fn default() -> Self {
        Self {
            clones: Mutex::default(),
            changes: broadcast::channel(64).0,
            actions: tokio::sync::Mutex::new(()),
        }
    }
}

fn clamp_text(text: &str, max: usize) -> String {
    if text.encode_utf16().count() <= max {
        return text.to_owned();
    }
    let mut out = String::new();
    let mut units = 0;
    for c in text.chars() {
        units += c.len_utf16();
        if units > max - 1 {
            break;
        }
        out.push(c);
    }
    out.push('…');
    out
}

fn now_iso() -> String {
    crate::auth_service::iso(crate::auth_service::now())
}

fn repository_error(provider: &str, operation: &str, detail: &str) -> Value {
    json!({"_tag": "SourceControlRepositoryError", "provider": provider, "operation": operation, "detail": detail})
}

/// `discardClone`: empties a partial clone's destination and keeps the folder.
/// Only a `.git`-bearing folder is emptied; other files arrived after the
/// empty-folder check.
pub(crate) async fn discard_clone(destination: &str) -> Result<(), Value> {
    let home = std::env::var_os("HOME")
        .map(PathBuf::from)
        .unwrap_or_default();
    let refused = |detail: &str| repository_error("unknown", "discardClone", detail);
    let path = clone::normalize_destination(destination, &home)
        .map_err(|_| refused("The clone destination could not be inspected."))?;
    let mut entries = Vec::new();
    match tokio::fs::read_dir(&path).await {
        Ok(mut dir) => {
            while let Ok(Some(entry)) = dir.next_entry().await {
                entries.push(entry.file_name().to_string_lossy().into_owned());
            }
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(_) => return Err(refused("The clone destination could not be inspected.")),
    }
    if !entries.is_empty() && !entries.iter().any(|e| e == ".git") {
        return Err(refused(
            "Destination path contains files that are not from the clone.",
        ));
    }
    // Retry while an interrupted git still closes its files.
    for attempt in 0..6 {
        let removed = match tokio::fs::remove_dir_all(&path).await {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e),
        };
        if removed.is_ok() && tokio::fs::create_dir_all(&path).await.is_ok() {
            return Ok(());
        }
        if attempt < 5 {
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }
    Err(refused("The partial clone could not be removed."))
}

/// Why a tracked clone failed (`describeCloneFailure`).
fn describe_failure(error: &Value) -> String {
    match error["detail"].as_str() {
        Some(detail) if error["_tag"] == "SourceControlRepositoryError" => detail.to_owned(),
        _ => "The repository could not be cloned.".to_owned(),
    }
}

impl Tracker {
    fn list(clones: &Clones) -> Vec<Value> {
        clones
            .tracked
            .values()
            .map(|t| t.snapshot.clone())
            .collect()
    }

    fn publish(&self, clones: &Clones) {
        let _ = self.changes.send(Value::Array(Self::list(clones)));
    }

    /// Every snapshot, oldest clone first.
    pub(crate) fn snapshots(&self) -> Vec<Value> {
        Self::list(&self.clones.lock().unwrap())
    }

    /// The clone tracked for `project_id`, if any.
    pub(crate) fn get(&self, project_id: &str) -> Option<Value> {
        self.clones
            .lock()
            .unwrap()
            .tracked
            .get(project_id)
            .map(|t| t.snapshot.clone())
    }

    /// The snapshot list after each change.
    pub(crate) fn subscribe(&self) -> broadcast::Receiver<Value> {
        self.changes.subscribe()
    }

    /// Changes `project_id`'s entry, gives its snapshot the next sequence, and publishes.
    fn modify(&self, project_id: &str, mutate: impl FnOnce(&mut Tracked)) -> Option<Value> {
        let mut clones = self.clones.lock().unwrap();
        if !clones.tracked.contains_key(project_id) {
            return None;
        }
        clones.sequence += 1;
        let sequence = clones.sequence;
        let tracked = clones.tracked.get_mut(project_id)?;
        mutate(tracked);
        tracked.snapshot["sequence"] = json!(sequence);
        let snapshot = tracked.snapshot.clone();
        self.publish(&clones);
        Some(snapshot)
    }

    fn remove(&self, project_id: &str) {
        let mut clones = self.clones.lock().unwrap();
        clones.tracked.shift_remove(project_id);
        self.publish(&clones);
    }

    fn clear_retention(&self, project_id: &str) {
        if let Some(handle) = self.clones.lock().unwrap().retention.remove(project_id) {
            handle.abort();
        }
    }

    fn schedule_removal(self: &Arc<Self>, project_id: &str) {
        self.clear_retention(project_id);
        let tracker = self.clone();
        let id = project_id.to_owned();
        let task = tokio::spawn(async move {
            tokio::time::sleep(DONE_RETENTION).await;
            tracker.clones.lock().unwrap().retention.remove(&id);
            tracker.remove(&id);
        });
        self.clones
            .lock()
            .unwrap()
            .retention
            .insert(project_id.to_owned(), task.abort_handle());
    }

    /// Forgets `project_id`'s claim unless it is launched within [`CLAIM_EXPIRY`].
    fn expire_unlaunched(self: &Arc<Self>, project_id: &str) {
        self.clear_retention(project_id);
        let tracker = self.clone();
        let id = project_id.to_owned();
        let task = tokio::spawn(async move {
            tokio::time::sleep(CLAIM_EXPIRY).await;
            let unlaunched = {
                let mut clones = tracker.clones.lock().unwrap();
                clones.retention.remove(&id);
                clones
                    .tracked
                    .get(&id)
                    .is_some_and(|t| t.running.is_none() && t.snapshot["phase"] == "running")
            };
            if unlaunched {
                tracker.remove(&id);
            }
        });
        self.clones
            .lock()
            .unwrap()
            .retention
            .insert(project_id.to_owned(), task.abort_handle());
    }

    fn progress(&self, project_id: &str, update: Progress) {
        self.modify(project_id, |t| {
            t.snapshot["stage"] = json!(update.stage);
            t.snapshot["percent"] = json!(update.percent);
            t.snapshot["detail"] = json!(update
                .detail
                .as_deref()
                .map(|d| clamp_text(d, DETAIL_MAX_LENGTH)));
            t.snapshot["stalled"] = json!(false);
        });
    }

    fn finish(self: &Arc<Self>, project_id: &str, phase: &str, error: Option<String>) {
        let ended_at = now_iso();
        self.modify(project_id, |t| {
            t.running = None;
            t.snapshot["phase"] = json!(phase);
            t.snapshot["endedAt"] = json!(ended_at);
            if phase == "done" {
                t.snapshot["percent"] = json!(100);
            }
            t.snapshot["stalled"] = json!(false);
            t.snapshot["error"] = json!(error.map(|e| clamp_text(&e, ERROR_MAX_LENGTH)));
        });
        if phase == "done" {
            self.schedule_removal(project_id);
        }
    }

    /// Runs `project_id`'s clone in the background. The outcome is recorded
    /// even when cancelled; a done clone is marked before `on_done` (the
    /// host's post-clone refresh) so a late cancel cannot remove it.
    fn launch<F, Fut>(self: &Arc<Self>, project_id: &str, on_done: F)
    where
        F: FnOnce(String) -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.clear_retention(project_id);
        let (cancel, cancel_rx) = oneshot::channel();
        let (stopped_tx, stopped) = oneshot::channel();
        let (clone_url, destination, protocol) = {
            let mut clones = self.clones.lock().unwrap();
            let Some(tracked) = clones.tracked.get_mut(project_id) else {
                return;
            };
            tracked.running = Some(Running { cancel, stopped });
            (
                tracked.clone_url.clone(),
                tracked.destination.clone(),
                tracked.protocol.clone(),
            )
        };
        let tracker = self.clone();
        let id = project_id.to_owned();
        tokio::spawn(async move {
            let pulses = Arc::new(Notify::new());
            // Marks the clone stalled once, then waits for git to speak again.
            let watcher = {
                let (tracker, pulses, id) = (tracker.clone(), pulses.clone(), id.clone());
                tokio::spawn(async move {
                    loop {
                        if tokio::time::timeout(STALL_AFTER, pulses.notified())
                            .await
                            .is_err()
                        {
                            tracker.modify(&id, |t| t.snapshot["stalled"] = json!(true));
                            pulses.notified().await;
                        }
                    }
                })
            };
            let input = CloneInput {
                provider: None,
                repository: None,
                remote_url: Some(clone_url),
                destination_path: destination.clone(),
                protocol: Some(protocol),
            };
            let home = std::env::var_os("HOME")
                .map(PathBuf::from)
                .unwrap_or_default();
            let outcome = match clone::prepare_clone(&input, &home, async |_: &str| {
                Ok::<_, crate::source_control_lookup::LookupFailure>(Value::Null)
            })
            .await
            {
                Err(error) => Err(error),
                Ok(prepared) => {
                    let on_progress = |update: Progress| {
                        pulses.notify_one();
                        tracker.progress(&id, update);
                    };
                    let options = CloneOptions {
                        on_progress: &on_progress,
                        timeout: None,
                        cancel: Some(cancel_rx),
                    };
                    clone::run_prepared(&input, &prepared, options).await
                }
            };
            watcher.abort();
            match outcome {
                Ok(_) => {
                    tracker.finish(&id, "done", None);
                    on_done(id.clone()).await;
                }
                Err(error) if error["_tag"] == "Cancelled" => {
                    tracker.finish(&id, "cancelled", None)
                }
                Err(error) => tracker.finish(&id, "failed", Some(describe_failure(&error))),
            }
            let _ = stopped_tx.send(());
        });
    }

    /// Stops `project_id`'s running clone, if any, and waits for it to record its outcome.
    async fn interrupt(&self, project_id: &str) -> bool {
        let running = {
            let mut clones = self.clones.lock().unwrap();
            clones
                .tracked
                .get_mut(project_id)
                .and_then(|t| t.running.take())
        };
        let Some(running) = running else {
            return false;
        };
        let _ = running.cancel.send(());
        let _ = running.stopped.await;
        true
    }

    /// `projectClone.start`: claims the destination, creates the project on
    /// it, and starts the clone.
    pub(crate) async fn start(
        self: &Arc<Self>,
        state: &Arc<AppState>,
        params: &Value,
    ) -> Result<Value, Value> {
        let _action = self.actions.lock().await;
        let claim = self.claim(params).await?;
        let project_id = claim["projectId"].as_str().unwrap_or_default().to_owned();
        let destination = claim["cwd"].as_str().unwrap_or_default().to_owned();
        let title = params["title"]
            .as_str()
            .unwrap_or_default()
            .trim()
            .to_owned();
        // The project is registered before git runs, so racing thread creates
        // and project deletes see the clone.
        let created = crate::projects::mutate_checked(
            state,
            &json!({"type": "project.create", "commandId": format!("project-clone-create:{project_id}"),
                "projectId": project_id, "title": title, "workspaceRoot": destination,
                "createWorkspaceRootIfMissing": true}),
        )
        .await;
        let owner = match created {
            Ok(_) => project_id.clone(),
            // A failed clone's project still owns its empty folder after a
            // restart forgets the clone; clone for that project instead.
            Err((_, crate::projects::Refusal::Conflict)) => {
                let canonical = tokio::fs::canonicalize(&destination)
                    .await
                    .map(|p| p.display().to_string())
                    .unwrap_or_else(|_| destination.clone());
                let conflicting = state
                    .store
                    .read()
                    .await
                    .projects
                    .values()
                    .find(|p| p.id != project_id && p.path == canonical)
                    .map(|p| p.id.clone());
                match conflicting.filter(|to| self.move_claim(&project_id, to)) {
                    Some(to) => to,
                    None => {
                        self.remove(&project_id);
                        return Err(already_in_progress(&claim));
                    }
                }
            }
            Err((error, _)) => {
                self.remove(&project_id);
                return Err(project_create_failed(&error["message"]));
            }
        };
        {
            let clones = self.clones.lock().unwrap();
            self.publish(&clones);
        }
        let host = state.clone();
        self.launch(&owner, move |id| async move { on_cloned(&host, &id).await });
        Ok(with_project(claim, &owner))
    }

    /// Claims a clone's destination: checks the input and the destination,
    /// and tracks the clone as running, not yet started. Answers
    /// `{projectId, cwd, remoteUrl, repository}`; `cwd` is where the
    /// project goes. A malformed input is `{"defect": message}`.
    async fn claim(self: &Arc<Self>, params: &Value) -> Result<Value, Value> {
        let input = CloneInput::from_json(params).map_err(|e| json!({"defect": e}))?;
        let project_id = params["projectId"].as_str().unwrap_or_default().to_owned();
        let home = std::env::var_os("HOME")
            .map(PathBuf::from)
            .unwrap_or_default();
        let provider = input.provider.clone().unwrap_or_default();
        let repository = input.repository.clone().unwrap_or_default();
        let lookup = async |cwd: &str| {
            crate::source_control_lookup::lookup(&provider, &repository, cwd).await
        };
        let prepared = clone::prepare_clone(&input, &home, lookup).await?;
        let destination = prepared.destination.display().to_string();
        let started_at = now_iso();
        let claim = json!({"projectId": project_id, "cwd": destination,
            "remoteUrl": prepared.remote_url, "repository": prepared.repository,
            "provider": input.provider.as_deref().unwrap_or("unknown")});
        let superseded: Vec<String>;
        {
            let mut clones = self.clones.lock().unwrap();
            clones.sequence += 1;
            let sequence = clones.sequence;
            let overlapping: Vec<(String, bool)> = clones
                .tracked
                .iter()
                .filter(|(id, t)| **id == project_id || t.destination == destination)
                .map(|(id, t)| (id.clone(), t.snapshot["phase"] == "running"))
                .collect();
            // Only a running clone owns its destination; a failed or
            // cancelled one no longer blocks it (#40739).
            if overlapping.iter().any(|(_, running)| *running) {
                return Err(already_in_progress(&claim));
            }
            superseded = overlapping.into_iter().map(|(id, _)| id).collect();
            for stale in &superseded {
                clones.tracked.shift_remove(stale);
            }
            let snapshot = json!({
                "projectId": project_id, "remoteUrl": prepared.remote_url, "destinationPath": destination,
                "repository": prepared.repository, "phase": "running", "stage": "connecting",
                "percent": null, "detail": null, "stalled": false, "error": null,
                "startedAt": started_at, "endedAt": null, "sequence": sequence,
            });
            clones.tracked.insert(
                project_id.clone(),
                Tracked {
                    snapshot,
                    running: None,
                    clone_url: prepared.clone_url.clone(),
                    destination: destination.clone(),
                    protocol: input.protocol.clone().unwrap_or_else(|| "auto".into()),
                },
            );
        }
        // A superseded entry's pending removal would otherwise drop the new one.
        for stale in &superseded {
            self.clear_retention(stale);
        }
        self.expire_unlaunched(&project_id);
        Ok(claim)
    }

    /// `clone.call` for a workspace served by `t3-agentd`: the host creates
    /// the project between `claim` and `launch`, and follows the clone with
    /// [`Self::subscribe`]. Each call takes its turn with the others.
    pub(crate) async fn call(
        self: &Arc<Self>,
        method: &str,
        input: &Value,
    ) -> Option<Result<Value, Value>> {
        let project_id = input["projectId"].as_str().unwrap_or_default();
        Some(match method {
            "claim" => {
                let _action = self.actions.lock().await;
                self.claim(input).await.map(|mut claim| {
                    if let Some(claim) = claim.as_object_mut() {
                        claim.remove("provider");
                    }
                    claim
                })
            }
            "adopt" => {
                let _action = self.actions.lock().await;
                let (from, to) = (
                    input["from"].as_str().unwrap_or_default(),
                    input["to"].as_str().unwrap_or_default(),
                );
                let moved = self.move_claim(from, to);
                if moved {
                    self.clear_retention(from);
                    self.expire_unlaunched(to);
                }
                Ok(json!(moved))
            }
            "release" => {
                let _action = self.actions.lock().await;
                // Only a claim that never started.
                let unstarted = self
                    .clones
                    .lock()
                    .unwrap()
                    .tracked
                    .get(project_id)
                    .is_some_and(|t| t.running.is_none() && t.snapshot["phase"] == "running");
                if unstarted {
                    self.remove(project_id);
                }
                Ok(json!(unstarted))
            }
            "launch" => {
                let _action = self.actions.lock().await;
                let unstarted = self
                    .clones
                    .lock()
                    .unwrap()
                    .tracked
                    .get(project_id)
                    .is_some_and(|t| t.running.is_none() && t.snapshot["phase"] == "running");
                if unstarted {
                    {
                        let clones = self.clones.lock().unwrap();
                        self.publish(&clones);
                    }
                    self.launch(project_id, |_| async {});
                }
                Ok(json!(unstarted))
            }
            "cancel" => Ok(json!(self.cancel(project_id).await)),
            "retry" => self
                .retry_with(project_id, |_| async {})
                .await
                .map(|applied| json!(applied)),
            "discard" => {
                self.discard(project_id).await;
                Ok(Value::Null)
            }
            _ => return None,
        })
    }

    fn move_claim(&self, from: &str, to: &str) -> bool {
        let mut clones = self.clones.lock().unwrap();
        if clones.tracked.contains_key(to) {
            return false;
        }
        let Some(mut claim) = clones.tracked.shift_remove(from) else {
            return false;
        };
        claim.snapshot["projectId"] = json!(to);
        clones.tracked.insert(to.to_owned(), claim);
        true
    }

    /// `projectClone.cancel`: stops a running clone and empties its destination.
    pub(crate) async fn cancel(self: &Arc<Self>, project_id: &str) -> bool {
        let _action = self.actions.lock().await;
        let destination = match self.clones.lock().unwrap().tracked.get(project_id) {
            Some(t) if t.snapshot["phase"] == "running" && t.running.is_some() => {
                t.destination.clone()
            }
            _ => return false,
        };
        self.interrupt(project_id).await;
        match self.get(project_id) {
            Some(after) if after["phase"] == "done" => return true,
            Some(after) if after["phase"] == "running" => {
                self.finish(project_id, "cancelled", None)
            }
            _ => {}
        }
        let _ = discard_clone(&destination).await;
        true
    }

    /// `projectClone.retry`: empties a failed or cancelled clone's destination and clones again.
    pub(crate) async fn retry(
        self: &Arc<Self>,
        state: &Arc<AppState>,
        project_id: &str,
    ) -> Result<bool, Value> {
        let host = state.clone();
        self.retry_with(
            project_id,
            move |id| async move { on_cloned(&host, &id).await },
        )
        .await
    }

    async fn retry_with<F, Fut>(
        self: &Arc<Self>,
        project_id: &str,
        on_done: F,
    ) -> Result<bool, Value>
    where
        F: FnOnce(String) -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let _action = self.actions.lock().await;
        let destination = match self.clones.lock().unwrap().tracked.get(project_id) {
            Some(t) if matches!(t.snapshot["phase"].as_str(), Some("failed" | "cancelled")) => {
                t.destination.clone()
            }
            _ => return Ok(false),
        };
        discard_clone(&destination).await?;
        let started_at = now_iso();
        self.modify(project_id, |t| {
            for (key, value) in [
                ("phase", json!("running")),
                ("stage", json!("connecting")),
                ("percent", Value::Null),
                ("detail", Value::Null),
                ("stalled", json!(false)),
                ("error", Value::Null),
                ("startedAt", json!(started_at)),
                ("endedAt", Value::Null),
            ] {
                t.snapshot[key] = value;
            }
        });
        self.launch(project_id, on_done);
        Ok(true)
    }

    /// Stops tracking `project_id`, stopping any running clone and emptying an
    /// unfinished checkout; a complete checkout is the user's.
    pub(crate) async fn discard(&self, project_id: &str) {
        let _action = self.actions.lock().await;
        let Some(destination) = self
            .clones
            .lock()
            .unwrap()
            .tracked
            .get(project_id)
            .map(|t| t.destination.clone())
        else {
            return;
        };
        self.interrupt(project_id).await;
        if self.get(project_id).is_some_and(|s| s["phase"] != "done") {
            let _ = discard_clone(&destination).await;
        }
        self.clear_retention(project_id);
        self.remove(project_id);
    }

    /// `rejectCommandsDuringClone`: why a thread cannot start in `project_id`
    /// yet, if it cannot.
    pub(crate) fn refusal(&self, project_id: &str) -> Option<&'static str> {
        let snapshot = self.get(project_id)?;
        match snapshot["phase"].as_str() {
            Some("done") => None,
            Some("running") => Some("The repository is still being cloned."),
            _ => Some("The repository was not cloned. Retry the clone first."),
        }
    }
}

fn already_in_progress(claim: &Value) -> Value {
    repository_error(
        claim["provider"].as_str().unwrap_or("unknown"),
        "cloneRepository",
        "A clone into this destination is already in progress.",
    )
}

/// The error `projectClone.start` reports when the clone's project cannot be created.
pub(crate) fn project_create_failed(message: &Value) -> Value {
    json!({"_tag": "OrchestrationDispatchCommandError",
        "message": "Failed to create clone project.",
        "cause": {"name": "ProjectOperationError", "message": message}})
}

/// `projectClone.start`'s answer for a claim, for the project that owns it.
fn with_project(claim: Value, owner: &str) -> Value {
    json!({"projectId": owner, "cwd": claim["cwd"], "remoteUrl": claim["remoteUrl"],
        "repository": claim["repository"]})
}

/// After a clone: the project is updated so clients see it as a repository.
async fn on_cloned(state: &Arc<AppState>, project_id: &str) {
    let _ = crate::projects::mutate_checked(
        state,
        &json!({"type": "project.update", "commandId": format!("project-clone-done:{project_id}"),
            "projectId": project_id}),
    )
    .await;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn long_text_is_clamped_with_an_ellipsis() {
        assert_eq!(clamp_text("short", 10), "short");
        assert_eq!(clamp_text("abcdefghijkl", 5), "abcd…");
        assert_eq!(clamp_text(&"x".repeat(200), 200).len(), 200);
    }

    #[tokio::test]
    async fn a_claim_that_is_never_launched_lets_go_of_its_destination() {
        let root = std::env::temp_dir().join(format!("t3-claim-{}", std::process::id()));
        let tracker = Arc::new(Tracker::default());
        let claim = |id: &str| {
            json!({"projectId": id, "title": "x", "remoteUrl": "/nowhere",
                "destinationPath": root.join("dest").to_string_lossy()})
        };
        let first = tracker.call("claim", &claim("p1")).await.unwrap();
        assert!(first.is_ok(), "{first:?}");
        // Held: a second claim of the folder is refused.
        assert!(tracker.call("claim", &claim("p2")).await.unwrap().is_err());
        tokio::time::sleep(CLAIM_EXPIRY * 3).await;
        assert!(tracker.snapshots().is_empty());
        assert!(tracker.call("claim", &claim("p2")).await.unwrap().is_ok());
        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn discarding_keeps_the_folder_and_refuses_foreign_files() {
        let root = std::env::temp_dir().join(format!("t3-discard-{}", std::process::id()));
        let partial = root.join("partial");
        std::fs::create_dir_all(partial.join(".git")).unwrap();
        std::fs::write(partial.join("file"), "x").unwrap();
        discard_clone(partial.to_str().unwrap()).await.unwrap();
        assert!(partial.is_dir() && std::fs::read_dir(&partial).unwrap().next().is_none());
        let foreign = root.join("foreign");
        std::fs::create_dir_all(&foreign).unwrap();
        std::fs::write(foreign.join("notes.txt"), "keep").unwrap();
        let refused = discard_clone(foreign.to_str().unwrap()).await.unwrap_err();
        assert_eq!(
            refused["detail"],
            "Destination path contains files that are not from the clone."
        );
        assert!(foreign.join("notes.txt").exists());
        std::fs::remove_dir_all(root).unwrap();
    }
}
