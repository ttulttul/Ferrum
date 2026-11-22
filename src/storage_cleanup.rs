//! The latest completed storage cleanup report and the changes seen by settings clients.
use serde_json::{json, Value};
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc;

#[derive(Default)]
pub(crate) struct Reports {
    inner: Mutex<State>,
}

#[derive(Default)]
struct State {
    latest: Arc<Value>,
    subscribers: Vec<mpsc::UnboundedSender<Arc<Value>>>,
}

/// Cleanup policy includes per-project worktree overrides, but unrelated
/// project settings changes do not request a run.
pub(crate) fn policy_changed(old: &Value, new: &Value) -> bool {
    let before = &old["projectSettingsOverrides"];
    let after = &new["projectSettingsOverrides"];
    old["storageCleanup"] != new["storageCleanup"]
        || old["worktreeCleanup"] != new["worktreeCleanup"]
        || [before, after]
            .into_iter()
            .filter_map(Value::as_object)
            .flat_map(|m| m.keys())
            .any(|id| before[id].get("worktreeCleanup") != after[id].get("worktreeCleanup"))
}

pub(crate) async fn automatic_runs(state: Arc<crate::AppState>) {
    let hour = std::time::Duration::from_secs(60 * 60);
    loop {
        state.storage_cleanup.run("automatic");
        let timer = tokio::time::sleep(hour);
        tokio::pin!(timer);
        loop {
            tokio::select! {
                _ = &mut timer => break,
                _ = state.settings.cleanup_changed.notified() => {},
                _ = state.thread_deleted.notified() => {},
            }
            state.storage_cleanup.run("automatic");
        }
    }
}

impl Reports {
    /// Queue the current value (null until a run completes) and register for
    /// future reports under the same lock so a concurrent completion cannot be lost.
    pub(crate) fn subscribe(&self) -> mpsc::UnboundedReceiver<Arc<Value>> {
        let (tx, rx) = mpsc::unbounded_channel();
        let mut state = self.inner.lock().unwrap();
        tx.send(state.latest.clone()).expect("receiver is alive");
        state.subscribers.retain(|sub| !sub.is_closed());
        state.subscribers.push(tx);
        rx
    }

    pub(crate) fn run(&self, trigger: &str) -> Value {
        let now = crate::auth_service::iso(crate::auth_service::now());
        let report = json!({
            "trigger":trigger, "startedAt":now, "finishedAt":now,
            "entries":[], "counts":{"removed":0,"kept":0,"failed":0},
            "omittedCount":0, "bytesFreed":0
        });
        self.publish(report.clone());
        report
    }

    fn publish(&self, report: Value) {
        let mut state = self.inner.lock().unwrap();
        let shared = Arc::new(report);
        state.latest = shared.clone();
        state
            .subscribers
            .retain(|sub| sub.send(shared.clone()).is_ok());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn storage_cleanup_replays_the_latest_and_queues_each_completion() {
        let service = Reports::default();
        let mut live = service.subscribe();
        assert!(live.try_recv().unwrap().is_null());
        let automatic = service.run("automatic");
        let manual = service.run("manual");
        assert_eq!(*live.try_recv().unwrap(), automatic);
        assert_eq!(*live.try_recv().unwrap(), manual);
        let mut reopened = service.subscribe();
        assert_eq!(*reopened.try_recv().unwrap(), manual);
        assert!(reopened.try_recv().is_err());
    }
}
