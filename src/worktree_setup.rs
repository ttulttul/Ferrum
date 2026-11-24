//! Worktree setup progress per thread, as the reference's project/WorktreeSetupTracker.ts.
//!
//! A setup is the work between launching a thread into a new worktree and starting its agent:
//! fetch, checkout, submodules, the project's setup script, then the agent. Each change bumps
//! the snapshot's sequence and goes to `subscribeWorktreeSetup` subscribers. A finished setup
//! stays readable for 30 seconds and then reads as `null`.
//!
//! The thread launcher here creates worktrees synchronously and does not begin setups yet, so
//! servers report `null` for every thread until it does. The contract (initial value, updates,
//! retention and cancellation) is complete and tested.
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{broadcast, oneshot};

pub const STAGES: [&str; 5] = ["fetch", "checkout", "submodules", "setup-script", "agent"];
const DETAIL_MAX: usize = 200;
const TAIL_LINE_MAX: usize = 400;
const ERROR_MAX: usize = 1000;
const TAIL_LINES: usize = 4;
pub const FINISHED_RETENTION: Duration = Duration::from_secs(30);

struct Tracked {
    snapshot: Value,
    /// Interrupts the setup's work; `None` once the agent may already have started.
    cancel: Option<oneshot::Sender<()>>,
}

#[derive(Default)]
struct Inner {
    setups: HashMap<String, Tracked>,
    /// Sequences keep increasing across setups so subscribers accept the next setup's first one.
    last_sequence: HashMap<String, u64>,
    /// Bumped on each finish so an older retention timer leaves a newer setup alone.
    retention: HashMap<String, u64>,
}

#[derive(Clone)]
pub struct WorktreeSetupTracker {
    inner: Arc<Mutex<Inner>>,
    changes: broadcast::Sender<(String, Option<Value>)>,
    retention: Duration,
}

impl Default for WorktreeSetupTracker {
    fn default() -> Self {
        Self::with_retention(FINISHED_RETENTION)
    }
}

fn clamp(text: &str, max: usize) -> String {
    if text.chars().count() <= max {
        return text.to_owned();
    }
    let mut out: String = text.chars().take(max - 1).collect();
    out.push('\u{2026}');
    out
}

fn now() -> String {
    crate::auth_service::iso(crate::auth_service::now())
}

fn empty_stage(id: &str) -> Value {
    json!({"id":id,"status":"pending","startedAt":null,"endedAt":null,"percent":null,"detail":null,"tail":[]})
}

impl WorktreeSetupTracker {
    /// A tracker that keeps finished setups readable for `retention`.
    pub fn with_retention(retention: Duration) -> Self {
        Self {
            inner: Arc::default(),
            changes: broadcast::channel(256).0,
            retention,
        }
    }

    fn publish(&self, thread_id: &str, snapshot: Option<Value>) {
        let _ = self.changes.send((thread_id.to_owned(), snapshot));
    }

    fn modify(&self, thread_id: &str, mutate: impl FnOnce(&mut Value)) -> Option<Value> {
        let snapshot = {
            let mut inner = self.inner.lock().expect("worktree setup state");
            let tracked = inner.setups.get_mut(thread_id)?;
            let sequence = tracked.snapshot["sequence"].as_u64().unwrap_or(0) + 1;
            mutate(&mut tracked.snapshot);
            tracked.snapshot["sequence"] = json!(sequence);
            let snapshot = tracked.snapshot.clone();
            inner.last_sequence.insert(thread_id.to_owned(), sequence);
            snapshot
        };
        self.publish(thread_id, Some(snapshot.clone()));
        Some(snapshot)
    }

    /// Starts tracking a setup of `stages` (in the reference's order). `cancel` fires when a
    /// client cancels it before its agent starts.
    pub fn begin(
        &self,
        thread_id: &str,
        branch: Option<&str>,
        base_ref: Option<&str>,
        stages: &[&str],
        cancel: oneshot::Sender<()>,
    ) -> Value {
        let snapshot = {
            let mut inner = self.inner.lock().expect("worktree setup state");
            inner.retention.remove(thread_id);
            let sequence = inner
                .last_sequence
                .get(thread_id)
                .map_or(0, |last| last + 1);
            inner.last_sequence.insert(thread_id.to_owned(), sequence);
            let snapshot = json!({
                "threadId": thread_id,
                "phase": "running",
                "startedAt": now(),
                "endedAt": null,
                "branch": branch,
                "baseRef": base_ref,
                "worktreePath": null,
                "setupScript": null,
                "stages": STAGES.iter().filter(|id| stages.contains(id)).map(|id| empty_stage(id)).collect::<Vec<_>>(),
                "error": null,
                "sequence": sequence,
            });
            inner.setups.insert(
                thread_id.to_owned(),
                Tracked {
                    snapshot: snapshot.clone(),
                    cancel: Some(cancel),
                },
            );
            snapshot
        };
        self.publish(thread_id, Some(snapshot.clone()));
        snapshot
    }

    /// Skips `stage_id` with `detail` when it never started.
    pub fn skip_pending(&self, thread_id: &str, stage_id: &str, detail: &str) -> Option<Value> {
        self.modify(thread_id, |snapshot| {
            if let Some(stage) = snapshot["stages"].as_array_mut().and_then(|stages| {
                stages
                    .iter_mut()
                    .find(|stage| stage["id"] == stage_id && stage["status"] == "pending")
            }) {
                stage["status"] = json!("skipped");
                stage["detail"] = json!(detail);
            }
        })
    }

    /// Names the worktree and its branch once it exists.
    pub fn set_workspace(&self, thread_id: &str, path: &str, branch: &str) -> Option<Value> {
        self.modify(thread_id, |snapshot| {
            snapshot["worktreePath"] = json!(path);
            snapshot["branch"] = json!(branch);
        })
    }

    /// Names the worktree once it exists.
    pub fn set_worktree_path(&self, thread_id: &str, path: &str) -> Option<Value> {
        self.modify(thread_id, |snapshot| snapshot["worktreePath"] = json!(path))
    }

    /// Names the project's setup script and the terminal it runs in.
    pub fn set_setup_script(
        &self,
        thread_id: &str,
        name: &str,
        command: &str,
        terminal_id: &str,
    ) -> Option<Value> {
        self.modify(thread_id, |snapshot| {
            snapshot["setupScript"] =
                json!({"name":name,"command":command,"terminalId":terminal_id});
        })
    }

    /// Moves a stage to `status`. A stage starts when it leaves `pending` and ends when it
    /// leaves `running`; `detail` of `Some(None)` clears the detail.
    pub fn stage_status(
        &self,
        thread_id: &str,
        stage_id: &str,
        status: &str,
        detail: Option<Option<&str>>,
    ) -> Option<Value> {
        let at = now();
        self.modify(thread_id, |snapshot| {
            let Some(stage) = snapshot["stages"]
                .as_array_mut()
                .and_then(|stages| stages.iter_mut().find(|stage| stage["id"] == stage_id))
            else {
                return;
            };
            if stage["startedAt"].is_null() && status != "pending" {
                stage["startedAt"] = json!(at);
            }
            if matches!(status, "running" | "pending") {
                stage["endedAt"] = Value::Null;
            } else if stage["endedAt"].is_null() {
                stage["endedAt"] = json!(at);
            }
            stage["status"] = json!(status);
            if let Some(detail) = detail {
                stage["detail"] = detail.map_or(Value::Null, |d| json!(clamp(d, DETAIL_MAX)));
            }
        })
    }

    /// Keeps a stage's last few output lines.
    pub fn append_tail(&self, thread_id: &str, stage_id: &str, line: &str) -> Option<Value> {
        self.modify(thread_id, |snapshot| {
            if let Some(tail) = snapshot["stages"]
                .as_array_mut()
                .and_then(|stages| stages.iter_mut().find(|stage| stage["id"] == stage_id))
                .and_then(|stage| stage["tail"].as_array_mut())
            {
                tail.push(json!(clamp(line, TAIL_LINE_MAX)));
                let excess = tail.len().saturating_sub(TAIL_LINES);
                tail.drain(..excess);
            }
        })
    }

    /// Call just before the agent starts, so a late cancellation cannot undo a started thread.
    pub fn mark_uncancellable(&self, thread_id: &str) {
        if let Some(tracked) = self
            .inner
            .lock()
            .expect("worktree setup state")
            .setups
            .get_mut(thread_id)
        {
            tracked.cancel = None;
        }
    }

    /// Ends the setup. Running stages end with it; the snapshot stays for the retention period.
    pub fn finish(&self, thread_id: &str, phase: &str, error: Option<&str>) -> Option<Value> {
        let at = now();
        let snapshot = self.modify(thread_id, |snapshot| {
            snapshot["phase"] = json!(phase);
            snapshot["endedAt"] = json!(at);
            snapshot["error"] = error.map_or(Value::Null, |e| json!(clamp(e, ERROR_MAX)));
            let ended = match phase {
                "done" => "done",
                "cancelled" => "skipped",
                _ => "failed",
            };
            for stage in snapshot["stages"].as_array_mut().into_iter().flatten() {
                if stage["status"] == "running" {
                    stage["status"] = json!(ended);
                    stage["endedAt"] = json!(at);
                }
            }
        })?;
        let generation = {
            let mut inner = self.inner.lock().expect("worktree setup state");
            if let Some(tracked) = inner.setups.get_mut(thread_id) {
                tracked.cancel = None;
            }
            let generation = inner.retention.get(thread_id).map_or(1, |g| g + 1);
            inner.retention.insert(thread_id.to_owned(), generation);
            generation
        };
        let tracker = self.clone();
        let thread_id = thread_id.to_owned();
        tokio::spawn(async move {
            tokio::time::sleep(tracker.retention).await;
            tracker.expire(&thread_id, generation);
        });
        Some(snapshot)
    }

    fn expire(&self, thread_id: &str, generation: u64) {
        let removed = {
            let mut inner = self.inner.lock().expect("worktree setup state");
            if inner.retention.get(thread_id) != Some(&generation) {
                return;
            }
            inner.retention.remove(thread_id);
            // After retention subscribers accept any sequence, so the counter starts over.
            inner.last_sequence.remove(thread_id);
            inner.setups.remove(thread_id).is_some()
        };
        if removed {
            self.publish(thread_id, None);
        }
    }

    /// Cancels a running setup that has not reached its agent. True when it was cancelled.
    pub fn cancel(&self, thread_id: &str) -> bool {
        let mut inner = self.inner.lock().expect("worktree setup state");
        let Some(tracked) = inner.setups.get_mut(thread_id) else {
            return false;
        };
        if tracked.snapshot["phase"] != "running" {
            return false;
        }
        tracked
            .cancel
            .take()
            .is_some_and(|cancel| cancel.send(()).is_ok())
    }

    pub fn get(&self, thread_id: &str) -> Option<Value> {
        self.inner
            .lock()
            .expect("worktree setup state")
            .setups
            .get(thread_id)
            .map(|tracked| tracked.snapshot.clone())
    }

    /// The thread's current snapshot or `null`, then each later change, newest first: a slow
    /// subscriber skips to the latest snapshot as the reference's one-slot sliding mailbox does.
    pub fn subscribe(&self, thread_id: &str) -> WorktreeSetupSubscription {
        let changes = self.changes.subscribe();
        let initial = self.get(thread_id);
        WorktreeSetupSubscription {
            thread_id: thread_id.to_owned(),
            last_sequence: initial.as_ref().and_then(|s| s["sequence"].as_u64()),
            initial: Some(initial.unwrap_or(Value::Null)),
            changes,
        }
    }
}

pub struct WorktreeSetupSubscription {
    thread_id: String,
    last_sequence: Option<u64>,
    initial: Option<Value>,
    changes: broadcast::Receiver<(String, Option<Value>)>,
}

impl WorktreeSetupSubscription {
    /// The next value to send, or `None` when the tracker is gone.
    pub async fn next(&mut self) -> Option<Value> {
        if let Some(initial) = self.initial.take() {
            return Some(initial);
        }
        loop {
            let (thread_id, snapshot) = match self.changes.recv().await {
                Ok(change) => change,
                Err(broadcast::error::RecvError::Lagged(_)) => continue,
                Err(broadcast::error::RecvError::Closed) => return None,
            };
            if thread_id != self.thread_id {
                continue;
            }
            match snapshot {
                // Changes queued before the initial snapshot are already part of it.
                Some(snapshot)
                    if self
                        .last_sequence
                        .is_some_and(|last| snapshot["sequence"].as_u64().unwrap_or(0) <= last) => {
                }
                Some(snapshot) => {
                    self.last_sequence = snapshot["sequence"].as_u64();
                    return Some(snapshot);
                }
                None => {
                    self.last_sequence = None;
                    return Some(Value::Null);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn next(subscription: &mut WorktreeSetupSubscription) -> Value {
        tokio::time::timeout(Duration::from_secs(5), subscription.next())
            .await
            .expect("a value within 5 s")
            .expect("tracker open")
    }

    #[tokio::test]
    async fn a_setup_streams_from_its_first_snapshot_to_null() {
        let tracker = WorktreeSetupTracker::with_retention(Duration::from_millis(200));
        let mut subscription = tracker.subscribe("thread:a");
        assert_eq!(next(&mut subscription).await, Value::Null);

        let (cancel, _cancelled) = oneshot::channel();
        tracker.begin(
            "thread:a",
            Some("t3/abcd"),
            Some("main"),
            &["agent", "fetch", "checkout"],
            cancel,
        );
        let first = next(&mut subscription).await;
        assert_eq!(first["sequence"], 0);
        assert_eq!(first["phase"], "running");
        let stages: Vec<_> = first["stages"]
            .as_array()
            .unwrap()
            .iter()
            .map(|s| s["id"].clone())
            .collect();
        assert_eq!(stages, [json!("fetch"), json!("checkout"), json!("agent")]);
        assert_eq!(
            first["stages"][0],
            json!({"id":"fetch","status":"pending","startedAt":null,"endedAt":null,"percent":null,"detail":null,"tail":[]})
        );

        tracker.stage_status("thread:a", "fetch", "running", Some(Some("Fetching main")));
        let running = next(&mut subscription).await;
        assert_eq!(running["sequence"], 1);
        assert!(running["stages"][0]["startedAt"].is_string());
        assert!(running["stages"][0]["endedAt"].is_null());
        for line in ["1", "2", "3", "4", "5"] {
            tracker.append_tail("thread:a", "fetch", line);
            next(&mut subscription).await;
        }
        assert_eq!(
            tracker.get("thread:a").unwrap()["stages"][0]["tail"],
            json!(["2", "3", "4", "5"])
        );
        tracker.set_worktree_path("thread:a", "/work/t3-abcd");
        assert_eq!(
            next(&mut subscription).await["worktreePath"],
            "/work/t3-abcd"
        );

        let done = tracker.finish("thread:a", "done", None).unwrap();
        assert_eq!(next(&mut subscription).await, done);
        assert_eq!(done["phase"], "done");
        assert_eq!(done["stages"][0]["status"], "done");
        assert_eq!(done["stages"][1]["status"], "pending");
        assert!(
            !tracker.cancel("thread:a"),
            "a finished setup cannot be cancelled"
        );

        // Retained for the retention period, then null.
        assert!(tracker.get("thread:a").is_some());
        assert_eq!(next(&mut subscription).await, Value::Null);
        assert_eq!(tracker.get("thread:a"), None);
    }

    #[tokio::test]
    async fn cancellation_reaches_a_running_setup_until_its_agent_starts() {
        let tracker = WorktreeSetupTracker::default();
        assert!(!tracker.cancel("thread:none"));
        let (cancel, cancelled) = oneshot::channel();
        tracker.begin("thread:b", None, None, &STAGES, cancel);
        assert!(tracker.cancel("thread:b"));
        assert!(cancelled.await.is_ok());
        assert!(!tracker.cancel("thread:b"), "the signal is sent once");

        let (cancel, _cancelled) = oneshot::channel();
        tracker.begin("thread:c", None, None, &STAGES, cancel);
        tracker.mark_uncancellable("thread:c");
        assert!(!tracker.cancel("thread:c"));
    }

    #[tokio::test]
    async fn a_late_subscriber_starts_from_the_current_snapshot() {
        let tracker = WorktreeSetupTracker::default();
        let (cancel, _cancelled) = oneshot::channel();
        tracker.begin("thread:d", None, None, &STAGES, cancel);
        tracker.stage_status("thread:d", "fetch", "failed", Some(Some(&"x".repeat(500))));
        let mut subscription = tracker.subscribe("thread:d");
        let current = next(&mut subscription).await;
        assert_eq!(current["sequence"], 1);
        assert_eq!(
            current["stages"][0]["detail"]
                .as_str()
                .unwrap()
                .chars()
                .count(),
            200
        );
        // Another thread's changes are not this subscriber's.
        let (cancel, _other) = oneshot::channel();
        tracker.begin("thread:e", None, None, &STAGES, cancel);
        tracker.finish("thread:d", "failed", Some("checkout failed"));
        let failed = next(&mut subscription).await;
        assert_eq!(failed["threadId"], "thread:d");
        assert_eq!(failed["error"], "checkout failed");
        // A setup begun after the last keeps counting, so subscribers accept it.
        let (cancel, _again) = oneshot::channel();
        assert_eq!(
            tracker.begin("thread:d", None, None, &STAGES, cancel)["sequence"],
            3
        );
    }
}
