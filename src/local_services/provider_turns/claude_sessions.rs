//! The Claude Code CLI a thread keeps running across its turns, as the
//! reference's provider session keeps its Claude query open: a later turn
//! (a delegated task's delivery too) is written to the running CLI rather
//! than to a new one, unless what the CLI was launched with changed (model,
//! mode, settings, the thread's MCP sign-in), which starts a new CLI that
//! resumes the session. Stop closes it, as the reference does; so do a
//! detach and server shutdown. An idle CLI closes after
//! [`IDLE_RELEASE_DELAY`].
//!
//! While no turn runs, the CLI's output is still read, as the reference's
//! query keeps reading it: what Claude writes on its own (a wake turn) is
//! kept, in order, for the thread's next turn to handle first, and a wake
//! that starts (`system/init`) or ends (`result`) is reported once
//! ([`ProviderWake`]) so a continuation run can show it
//! (`bufferWakeMessage` / `offerWakeContinuation` in the reference).
use super::Cli;
use crate::workspace::provider::{ProviderIdleEvent, ProviderWake, CLAUDE_AGENT};
use crate::workspace::ProviderEvent;
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;
use tokio::sync::mpsc;

/// How long a CLI stays open after its last turn (the reference's idle
/// timeout).
pub(super) const IDLE_RELEASE_DELAY: Duration = Duration::from_secs(30 * 60);

/// Each reported wake's token, so retiring a stale one leaves a newer alone.
static WAKE_TOKENS: AtomicU64 = AtomicU64::new(1);

/// A workspace's running Claude CLIs, by native session id, the sessions
/// Claude Code said it no longer has, and the wakes the CLIs report while
/// idle.
pub(crate) struct Sessions {
    live: Mutex<HashMap<String, Live>>,
    missing: Mutex<HashSet<String>>,
    wakes: mpsc::UnboundedSender<ProviderWake>,
    wake_receiver: Mutex<Option<mpsc::UnboundedReceiver<ProviderWake>>>,
    /// What idle CLIs report as it arrives (a background subagent's child answering).
    idle_events: mpsc::UnboundedSender<ProviderIdleEvent>,
    idle_receiver: Mutex<Option<mpsc::UnboundedReceiver<ProviderIdleEvent>>>,
}

impl Default for Sessions {
    fn default() -> Self {
        let (wakes, receiver) = mpsc::unbounded_channel();
        let (idle_events, idle_receiver) = mpsc::unbounded_channel();
        Sessions {
            live: Mutex::default(),
            missing: Mutex::default(),
            wakes,
            wake_receiver: Mutex::new(Some(receiver)),
            idle_events,
            idle_receiver: Mutex::new(Some(idle_receiver)),
        }
    }
}

struct Live {
    /// Reads the CLI while it is idle; told to stop, it hands the CLI back
    /// with what it read still to be handled.
    reader: tokio::task::JoinHandle<Cli>,
    control: mpsc::UnboundedSender<Control>,
    /// What the CLI was launched with, but for its session and its model
    /// selection.
    launch: String,
    /// The model selection it runs (model, effort, settings), which a later
    /// turn can change without a new CLI.
    selection: serde_json::Value,
    idle: tokio::task::JoinHandle<()>,
}

enum Control {
    /// Stop reading and hand the CLI back.
    Stop,
    /// The continuation run reported for wake `token` ended without
    /// starting (`retireOfferedWake`): drop the wake it would have shown.
    Retire(u64),
}

/// Where a session's wakes go: the app thread whose turns it runs.
pub(super) struct WakeRoute {
    pub app_thread_id: String,
}

impl Sessions {
    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<String, Live>> {
        self.live.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Where wakes go, for other providers' idle work (a Codex subagent).
    pub(crate) fn wake_sender(&self) -> mpsc::UnboundedSender<ProviderWake> {
        self.wakes.clone()
    }

    /// The wakes idle CLIs report; taken once, by the host that opens
    /// their continuation runs.
    pub(crate) fn take_wakes(&self) -> Option<mpsc::UnboundedReceiver<ProviderWake>> {
        self.wake_receiver
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take()
    }

    /// What idle CLIs report as it arrives; taken once, by the host that records it.
    pub(crate) fn take_idle_events(&self) -> Option<mpsc::UnboundedReceiver<ProviderIdleEvent>> {
        self.idle_receiver
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take()
    }

    /// Whether a running CLI holds `session` (which it may not have saved yet).
    pub(super) fn holds(&self, session: &str) -> bool {
        self.lock().contains_key(session)
    }

    /// Remember that Claude Code could not find `session` to resume
    /// (the reference's `missingNativeThreads`).
    pub(super) fn mark_missing(&self, session: &str) {
        self.missing
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(session.to_owned());
    }

    /// Whether Claude Code said it no longer has `session`.
    pub(super) fn is_missing(&self, session: &str) -> bool {
        self.missing
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .contains(session)
    }

    /// The running CLI of `session`, for a turn launched as `launch`: taken
    /// for the turn when it was launched the same way and still runs, else
    /// closed.
    /// A turn with another model selection takes it too when the CLI can
    /// switch to that one (`canApplyClaudeSelectionLive`), with the selection
    /// it runs, for the turn to switch it.
    pub(super) async fn take(
        &self,
        session: &str,
        launch: &str,
        selection: &serde_json::Value,
    ) -> Option<(Cli, Option<serde_json::Value>)> {
        let live = self.lock().remove(session)?;
        live.idle.abort();
        let same = live.launch == launch;
        let running = live.selection.clone();
        let mut cli = live.hand_back().await?;
        if same && cli.alive() {
            if &running == selection {
                return Some((cli, None));
            }
            if can_apply_live(&running, selection) {
                return Some((cli, Some(running)));
            }
        }
        cli.stop().await;
        None
    }

    /// Keep `cli` running for `session`'s next turn, reading what it writes
    /// meanwhile.
    pub(super) fn keep(
        self: &Arc<Self>,
        session: String,
        cli: Cli,
        launch: String,
        selection: serde_json::Value,
        route: WakeRoute,
    ) {
        let sessions: Weak<Self> = Arc::downgrade(self);
        let key = session.clone();
        let idle = tokio::spawn(async move {
            tokio::time::sleep(IDLE_RELEASE_DELAY).await;
            if let Some(sessions) = sessions.upgrade() {
                let live = sessions.lock().remove(&key);
                if let Some(live) = live {
                    if let Some(cli) = live.hand_back().await {
                        cli.close().await;
                    }
                }
            }
        });
        let (control, commands) = mpsc::unbounded_channel();
        let reporter = Reporter {
            wakes: self.wakes.clone(),
            idle_events: self.idle_events.clone(),
            route,
            session: session.clone(),
        };
        let reader = tokio::spawn(read_idle(cli, commands, reporter));
        let previous = self.lock().insert(
            session,
            Live {
                reader,
                control,
                launch,
                selection,
                idle,
            },
        );
        if let Some(previous) = previous {
            previous.idle.abort();
            tokio::spawn(previous.stop());
        }
    }

    /// The continuation run reported for `session`'s wake `token` ended
    /// without starting: drop that wake, so a later one is reported.
    pub(crate) fn retire_wake(&self, session: &str, token: u64) {
        if let Some(live) = self.lock().get(session) {
            let _ = live.control.send(Control::Retire(token));
        }
    }

    /// Close `session`'s CLI, if it runs (a detach).
    pub(crate) fn close(&self, session: &str) {
        if let Some(live) = self.lock().remove(session) {
            live.idle.abort();
            tokio::spawn(live.stop());
        }
    }

    /// Close every CLI (the server is shutting down).
    pub(crate) async fn shutdown(&self) {
        let all: Vec<Live> = self.lock().drain().map(|(_, live)| live).collect();
        for live in all {
            live.idle.abort();
            live.stop().await;
        }
    }
}

impl Live {
    async fn hand_back(self) -> Option<Cli> {
        let _ = self.control.send(Control::Stop);
        self.reader.await.ok()
    }

    async fn stop(self) {
        if let Some(cli) = self.hand_back().await {
            cli.stop().await;
        }
    }
}

/// Reports an idle session's wakes.
struct Reporter {
    wakes: mpsc::UnboundedSender<ProviderWake>,
    idle_events: mpsc::UnboundedSender<ProviderIdleEvent>,
    route: WakeRoute,
    session: String,
}

impl Reporter {
    /// Report what the idle CLI changed, for the thread's latest run.
    fn record(&self, event: ProviderEvent) {
        let _ = self.idle_events.send(ProviderIdleEvent {
            app_thread_id: self.route.app_thread_id.clone(),
            event,
        });
    }

    /// Report a wake, with what ended in the background since the last one; its
    /// token, once reported.
    fn report(&self, cli: &mut Cli) -> Option<u64> {
        let token = WAKE_TOKENS.fetch_add(1, Ordering::Relaxed);
        let (detail, notification) = cli.background.take_wake();
        self.wakes
            .send(ProviderWake {
                driver: CLAUDE_AGENT.into(),
                app_thread_id: self.route.app_thread_id.clone(),
                native_thread_id: self.session.clone(),
                token,
                detail,
                notification,
                delivery: None,
            })
            .ok()?;
        Some(token)
    }
}

/// Whether a running CLI can switch from the `live` model selection to
/// `next` (`canApplyClaudeSelectionLive`): thinking is set at launch, and an
/// effort the CLI was given cannot be taken back.
fn can_apply_live(live: &serde_json::Value, next: &serde_json::Value) -> bool {
    live["settings"]["alwaysThinkingEnabled"] == next["settings"]["alwaysThinkingEnabled"]
        && (!next["effort"].is_null() || live["effort"].is_null())
}

/// Whether a line Claude wrote while idle starts a turn of its own
/// (`system/init`, `isClaudeTurnStartMessage`) or ends one (a `result`,
/// but not the empty success a task notification can append).
pub(super) fn wake_boundary(line: &str) -> bool {
    let Ok(message) = serde_json::from_str::<Value>(line) else {
        return false;
    };
    match message["type"].as_str() {
        Some("system") => message["subtype"] == "init",
        Some("result") => !empty_task_notification_success(&message),
        _ => false,
    }
}

/// `isClaudeEmptyTaskNotificationSuccess`: the result Claude appends after
/// a task notification that started no turn.
fn empty_task_notification_success(message: &Value) -> bool {
    message["type"] == "result"
        && message["origin"]["kind"] == "task-notification"
        && message["subtype"] == "success"
        && message["is_error"] != true
        && message["num_turns"] == 0
        && message["result"].as_str().is_none_or(str::is_empty)
}

/// Read the idle CLI until it is taken back. A background subagent's own messages are recorded
/// as they arrive; what else it writes stays buffered, in order, for the next turn; the first
/// wake boundary reports a wake, and no other is reported until that one is taken or retired.
async fn read_idle(
    mut cli: Cli,
    mut commands: mpsc::UnboundedReceiver<Control>,
    reporter: Reporter,
) -> Cli {
    // The token of the wake reported and not yet retired.
    let mut reported: Option<u64> = None;
    let mut open = true;
    loop {
        tokio::select! {
            command = commands.recv() => match command {
                Some(Control::Stop) | None => return cli,
                Some(Control::Retire(token)) if reported == Some(token) => {
                    cli.drop_buffered_wake();
                    reported = None;
                    if cli.buffered_lines().any(wake_boundary) {
                        reported = reporter.report(&mut cli);
                    }
                }
                Some(Control::Retire(_)) => {}
            },
            line = cli.read_line(), if open => match line {
                Some(line) => {
                    let boundary = wake_boundary(&line);
                    let message = serde_json::from_str::<Value>(&line).ok();
                    if let Some(message) = &message {
                        cli.background.observe_idle(message);
                        // A background subagent's own message is recorded as it arrives, as
                        // the reference's adapter routes it, not kept for the wake turn.
                        if let Some(items) = super::claude::child_items(&mut cli.subagents, message) {
                            for item in items {
                                reporter.record(ProviderEvent::TurnItem { item });
                            }
                            continue;
                        }
                    }
                    cli.buffer_line(line);
                    if boundary && reported.is_none() {
                        reported = reporter.report(&mut cli);
                    }
                }
                None => open = false,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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

    #[test]
    fn a_wake_starts_with_init_and_ends_with_a_result() {
        assert!(wake_boundary(r#"{"type":"system","subtype":"init"}"#));
        assert!(wake_boundary(
            r#"{"type":"result","subtype":"success","num_turns":1,"result":"Hi."}"#
        ));
        assert!(!wake_boundary(
            r#"{"type":"result","subtype":"success","num_turns":0,"result":"","origin":{"kind":"task-notification"}}"#
        ));
        // Only a task notification's empty success ends no turn.
        assert!(wake_boundary(
            r#"{"type":"result","subtype":"success","num_turns":0,"result":""}"#
        ));
        assert!(!wake_boundary(r#"{"type":"assistant","message":{}}"#));
        assert!(!wake_boundary(r#"{"type":"system","subtype":"status"}"#));
        assert!(!wake_boundary("not json"));
    }
}
