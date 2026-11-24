//! The Claude Code CLI a thread keeps running across its turns, as the
//! reference's provider session keeps its Claude query open: a later turn
//! (a delegated task's delivery too) is written to the running CLI rather
//! than to a new one, unless what the CLI was launched with changed (model,
//! mode, settings, the thread's MCP sign-in), which starts a new CLI that
//! resumes the session. Stop closes it, as the reference does; so do a
//! detach and server shutdown. An idle CLI closes after
//! [`IDLE_RELEASE_DELAY`].
use super::Cli;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

/// How long a CLI stays open after its last turn (the reference's idle
/// timeout).
pub(super) const IDLE_RELEASE_DELAY: Duration = Duration::from_secs(30 * 60);

/// A workspace's running Claude CLIs, by native session id, and the sessions
/// Claude Code said it no longer has.
#[derive(Default)]
pub(crate) struct Sessions(Mutex<HashMap<String, Live>>, Mutex<HashSet<String>>);

struct Live {
    cli: Cli,
    /// Prompts the CLI holds queued (steers a turn ended without taking).
    queued: Vec<String>,
    /// What the CLI was launched with, but for its session and its model
    /// selection.
    launch: String,
    /// The model selection it runs (model, effort, settings), which a later
    /// turn can change without a new CLI.
    selection: serde_json::Value,
    idle: tokio::task::JoinHandle<()>,
}

impl Sessions {
    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<String, Live>> {
        self.0.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Whether a running CLI holds `session` (which it may not have saved yet).
    pub(super) fn holds(&self, session: &str) -> bool {
        self.lock().contains_key(session)
    }

    /// Remember that Claude Code could not find `session` to resume
    /// (the reference's `missingNativeThreads`).
    pub(super) fn mark_missing(&self, session: &str) {
        self.1
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(session.to_owned());
    }

    /// Whether Claude Code said it no longer has `session`.
    pub(super) fn is_missing(&self, session: &str) -> bool {
        self.1
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .contains(session)
    }

    /// The running CLI of `session`, for a turn launched as `launch`: taken
    /// for the turn when it was launched the same way and still runs, else
    /// closed.
    /// With it, the prompts it holds queued.
    /// A turn with another model selection takes it too when the CLI can
    /// switch to that one (`canApplyClaudeSelectionLive`), with the selection
    /// it runs, for the turn to switch it.
    pub(super) async fn take(
        &self,
        session: &str,
        launch: &str,
        selection: &serde_json::Value,
    ) -> Option<(Cli, Vec<String>, Option<serde_json::Value>)> {
        let live = self.lock().remove(session)?;
        live.idle.abort();
        let mut cli = live.cli;
        if live.launch == launch && cli.alive() {
            if &live.selection == selection {
                return Some((cli, live.queued, None));
            }
            if can_apply_live(&live.selection, selection) {
                return Some((cli, live.queued, Some(live.selection)));
            }
        }
        cli.stop().await;
        None
    }

    /// Keep `cli` running for `session`'s next turn.
    pub(super) fn keep(
        self: &Arc<Self>,
        session: String,
        cli: Cli,
        launch: String,
        selection: serde_json::Value,
        queued: Vec<String>,
    ) {
        let sessions: Weak<Self> = Arc::downgrade(self);
        let key = session.clone();
        let idle = tokio::spawn(async move {
            tokio::time::sleep(IDLE_RELEASE_DELAY).await;
            if let Some(sessions) = sessions.upgrade() {
                let live = sessions.lock().remove(&key);
                if let Some(live) = live {
                    live.cli.close().await;
                }
            }
        });
        let previous = self.lock().insert(
            session,
            Live {
                cli,
                queued,
                launch,
                selection,
                idle,
            },
        );
        if let Some(previous) = previous {
            previous.idle.abort();
            tokio::spawn(previous.cli.stop());
        }
    }

    /// Close `session`'s CLI, if it runs (a detach).
    pub(crate) fn close(&self, session: &str) {
        if let Some(live) = self.lock().remove(session) {
            live.idle.abort();
            tokio::spawn(live.cli.stop());
        }
    }

    /// Close every CLI (the server is shutting down).
    pub(crate) async fn shutdown(&self) {
        let all: Vec<Live> = self.lock().drain().map(|(_, live)| live).collect();
        for live in all {
            live.idle.abort();
            live.cli.stop().await;
        }
    }
}

/// Whether a running CLI can switch from the `live` model selection to
/// `next` (`canApplyClaudeSelectionLive`): thinking is set at launch, and an
/// effort the CLI was given cannot be taken back.
fn can_apply_live(live: &serde_json::Value, next: &serde_json::Value) -> bool {
    live["settings"]["alwaysThinkingEnabled"] == next["settings"]["alwaysThinkingEnabled"]
        && (!next["effort"].is_null() || live["effort"].is_null())
}

#[cfg(test)]
mod tests {
    use super::can_apply_live;
    use serde_json::json;

    #[test]
    fn a_running_cli_takes_another_model_or_effort_but_not_thinking_or_a_dropped_effort() {
        let live = json!({"model": "claude-opus-5-5", "effort": "medium", "settings": {"fastMode": false}});
        let sonnet = json!({"model": "claude-sonnet-5-5", "effort": "high", "settings": {}});
        assert!(can_apply_live(&live, &sonnet));
        let thinking = json!({"model": "claude-opus-5-5", "effort": "medium",
            "settings": {"alwaysThinkingEnabled": true}});
        assert!(!can_apply_live(&live, &thinking));
        let no_effort = json!({"model": "claude-haiku-5-5", "effort": null, "settings": {}});
        assert!(!can_apply_live(&live, &no_effort));
        assert!(can_apply_live(&no_effort, &sonnet));
    }
}
