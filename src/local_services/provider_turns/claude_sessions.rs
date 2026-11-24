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
    /// What the CLI was launched with, but for its session.
    launch: String,
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
    pub(super) async fn take(&self, session: &str, launch: &str) -> Option<(Cli, Vec<String>)> {
        let live = self.lock().remove(session)?;
        live.idle.abort();
        let mut cli = live.cli;
        if live.launch == launch && cli.alive() {
            return Some((cli, live.queued));
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
