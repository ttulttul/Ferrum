//! The latest completed storage cleanup report and the changes seen by settings clients.
mod checkpoints;
mod files;

use serde_json::{json, Value};
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc;

#[derive(Default)]
pub(crate) struct Reports {
    inner: Mutex<State>,
    /// Held for an entire sweep. Shutdown closes the executor before releasing home ownership.
    closed: Arc<tokio::sync::Mutex<bool>>,
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
        run_automatic(&state).await;
        let timer = tokio::time::sleep(hour);
        tokio::pin!(timer);
        loop {
            tokio::select! {
                _ = &mut timer => break,
                _ = state.settings.cleanup_changed.notified() => {},
                _ = state.thread_deleted.notified() => {},
            }
            run_automatic(&state).await;
        }
    }
}

async fn run_automatic(state: &Arc<crate::AppState>) {
    if run(state.clone(), "automatic").await.is_err() {
        tracing::warn!("automatic storage cleanup failed");
    }
}

pub(crate) async fn run(
    state: Arc<crate::AppState>,
    trigger: &'static str,
) -> Result<Value, crate::rpc_protocol::Reply> {
    use crate::rpc_protocol::Reply;
    let guard = state.storage_cleanup.closed.clone().lock_owned().await;
    if *guard {
        return Err(Reply::defect("Storage cleanup is shutting down".into()));
    }
    tokio::task::spawn_blocking(move || {
        // Keep ownership of the sweep through publication even if a client disconnects.
        let _guard = guard;
        let started = crate::auth_service::now();
        let settings = state
            .settings
            .client(false)
            .map_err(|error| Reply::Failure(json!({"_tag":"Fail","error":error})))?;
        if let Err(error) = checkpoints::prune(&state.paths.state_dir) {
            tracing::warn!(%error, "checkpoint ref cleanup failed");
        }
        let entries = [
            (
                files::Category::BrowserArtifacts,
                &state.paths.browser_artifacts,
            ),
            (files::Category::Logs, &state.paths.logs),
        ]
        .into_iter()
        .filter_map(|(category, root)| category.clean(root, started, &settings, &state.settings))
        .collect();
        let report = report(trigger, started, entries);
        state.storage_cleanup.publish(report.clone());
        Ok(report)
    })
    .await
    .map_err(|error| Reply::defect(error.to_string()))?
}

fn report(trigger: &str, started: i64, mut entries: Vec<Value>) -> Value {
    let (mut removed, mut kept, mut failed) = (0, 0, 0);
    let mut bytes: u64 = 0;
    for entry in &entries {
        match entry["outcome"].as_str() {
            Some("kept") => {
                kept += 1;
                continue;
            }
            Some("removed") => removed += 1,
            Some("failed") => failed += 1,
            _ => unreachable!("unknown cleanup outcome"),
        }
        bytes += entry["bytes"].as_u64().unwrap_or(0);
    }
    entries.sort_by_key(|entry| match entry["outcome"].as_str() {
        Some("failed") => 0,
        Some("removed") => 1,
        _ if entry["kind"] == "worktree" && entry["threadId"].is_null() => 2,
        _ => 3,
    });
    let omitted = entries.len().saturating_sub(200);
    entries.truncate(200);
    json!({"trigger":trigger, "startedAt":crate::auth_service::iso(started),
        "finishedAt":crate::auth_service::iso(crate::auth_service::now()), "entries":entries,
        "counts":{"removed":removed,"kept":kept,"failed":failed}, "omittedCount":omitted, "bytesFreed":bytes})
}

impl Reports {
    pub(crate) async fn shutdown(&self) {
        *self.closed.lock().await = true;
    }

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
        let automatic = report("automatic", 0, vec![]);
        service.publish(automatic.clone());
        let manual = report("manual", 0, vec![]);
        service.publish(manual.clone());
        assert_eq!(*live.try_recv().unwrap(), automatic);
        assert_eq!(*live.try_recv().unwrap(), manual);
        let mut reopened = service.subscribe();
        assert_eq!(*reopened.try_recv().unwrap(), manual);
        assert!(reopened.try_recv().is_err());
    }
}
