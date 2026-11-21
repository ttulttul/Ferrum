//! Provider turns: run a coding agent's CLI for one turn and translate its
//! protocol into [`ProviderEvent`]s.
//!
//! Each turn starts the CLI, continues the thread's native session when the
//! spec names one, sends the prompt, and stops the CLI once the turn ends.
//! The thread's runtime mode decides what the CLI may do without asking;
//! every approval or question it still asks waits for the user (see
//! [`requests`]).
mod claude;
mod codex;
mod requests;

pub(crate) use requests::respond;

use crate::workspace::provider::{
    ProviderFailure, ProviderItem, ThreadDisposition, TurnStatus, CLAUDE_AGENT, CODEX,
};
use crate::workspace::{ProviderEvent, TurnEvent, TurnExit, TurnSpec};
use std::process::Stdio;
use tokio::{
    io::{AsyncBufReadExt, AsyncRead, AsyncWriteExt, BufReader, Lines},
    process::{Child, ChildStdin, ChildStdout, Command},
    sync::{mpsc, oneshot},
};

/// Start a provider turn. Its events arrive on the returned channel and end
/// with `Exited`.
pub(crate) fn start(
    spec: TurnSpec,
    cancel: oneshot::Receiver<()>,
) -> anyhow::Result<mpsc::UnboundedReceiver<TurnEvent>> {
    let provider = spec
        .provider
        .clone()
        .ok_or_else(|| anyhow::anyhow!("the turn names no provider"))?;
    if provider.driver != CODEX && provider.driver != CLAUDE_AGENT {
        anyhow::bail!("provider driver `{}` cannot run turns", provider.driver);
    }
    let (events, receiver) = mpsc::unbounded_channel();
    let sink = Sink::new(events, provider.driver.clone(), spec.turn_id.clone());
    // As the reference opens a session: only in a folder that exists.
    if !std::path::Path::new(&spec.cwd).is_dir() {
        let failure = crate::workspace::provider::workspace_missing(&spec.cwd);
        sink.finish(TurnEnd::failed(failure, ThreadDisposition::Reusable));
        return Ok(receiver);
    }
    let responses = requests::register(&spec.turn_id);
    if provider.driver == CODEX {
        tokio::spawn(codex::run(spec, provider, cancel, responses, sink));
    } else {
        tokio::spawn(claude::run(spec, provider, cancel, responses, sink));
    }
    Ok(receiver)
}

/// Where a turn's events go.
#[derive(Clone)]
pub(crate) struct Sink {
    events: mpsc::UnboundedSender<TurnEvent>,
    driver: String,
    /// The turn's id, under which its answers arrive.
    turn_id: String,
    /// Items reported as running and not finished since, in order.
    open: std::sync::Arc<std::sync::Mutex<Vec<ProviderItem>>>,
}

impl Sink {
    fn new(events: mpsc::UnboundedSender<TurnEvent>, driver: String, turn_id: String) -> Self {
        Self {
            events,
            driver,
            turn_id,
            open: std::sync::Arc::default(),
        }
    }
    fn send(&self, event: ProviderEvent) {
        if let ProviderEvent::TurnItem { item } = &event {
            if let Ok(mut open) = self.open.lock() {
                open.retain(|known| known.id != item.id);
                if item.status == "running" {
                    open.push(item.clone());
                }
            }
        }
        let _ = self.events.send(TurnEvent::Provider(event));
    }
    /// The turn's terminal event, then its exit. Items still running end
    /// first: interrupted or failed with the turn, otherwise cancelled
    /// (save a completed Codex turn's commands).
    fn finish(&self, end: TurnEnd) {
        if !self.turn_id.is_empty() {
            requests::unregister(&self.turn_id);
        }
        let open = self
            .open
            .lock()
            .map(|mut open| std::mem::take(&mut *open))
            .unwrap_or_default();
        // An interrupted turn's open items end interrupted, as the
        // reference closes them.
        let closed = match end.status {
            TurnStatus::Failed => "failed",
            TurnStatus::Interrupted => "interrupted",
            _ => "cancelled",
        };
        for mut item in open {
            // A Codex command can outlive a completed turn, as background
            // work; the reference ends only an interrupted or failed turn's.
            if self.driver == CODEX
                && end.status == TurnStatus::Completed
                && item.kind == "command_execution"
            {
                continue;
            }
            item.status = closed.into();
            // The reference rebuilds an interrupted command without it.
            if end.status == TurnStatus::Interrupted {
                item.fields.remove("completionWake");
            }
            if item.fields.contains_key("streaming") {
                item.fields.insert("streaming".into(), false.into());
            }
            self.send(ProviderEvent::TurnItem { item });
        }
        let success = end.status == TurnStatus::Completed;
        self.send(ProviderEvent::TurnTerminal {
            status: end.status,
            failure: end.failure,
            thread_disposition: end.disposition,
            driver: Some(self.driver.clone()),
        });
        let _ = self.events.send(TurnEvent::Exited(TurnExit {
            success,
            cancelled: end.cancelled,
            status: end.status.as_str().into(),
        }));
    }
}

/// How a turn ended.
pub(crate) struct TurnEnd {
    status: TurnStatus,
    failure: Option<ProviderFailure>,
    disposition: ThreadDisposition,
    cancelled: bool,
}

impl TurnEnd {
    fn completed() -> Self {
        Self {
            status: TurnStatus::Completed,
            failure: None,
            disposition: ThreadDisposition::Reusable,
            cancelled: false,
        }
    }
    fn interrupted(cancelled: bool) -> Self {
        Self {
            status: TurnStatus::Interrupted,
            failure: None,
            disposition: ThreadDisposition::Reusable,
            cancelled,
        }
    }
    fn failed(failure: ProviderFailure, disposition: ThreadDisposition) -> Self {
        Self {
            status: TurnStatus::Failed,
            failure: Some(failure),
            disposition,
            cancelled: false,
        }
    }
}

/// The CLI of one provider turn: its process and protocol streams.
pub(crate) struct Cli {
    child: Child,
    stdin: Option<ChildStdin>,
    lines: Lines<BufReader<ChildStdout>>,
    /// The end of the CLI's standard error, for recognizing known refusals.
    stderr_tail: std::sync::Arc<std::sync::Mutex<String>>,
}

/// What a CLI is launched with.
pub(crate) struct Launch<'a> {
    pub program: String,
    pub args: Vec<String>,
    /// The variable naming the CLI's home, set when `home` is not empty.
    pub home_var: &'static str,
    pub home: &'a str,
    pub extra_env: Vec<(String, String)>,
}

impl Cli {
    /// Start the CLI in the turn's directory with the turn's environment.
    /// The CLI leads its own process group, so stopping it stops the
    /// commands it ran too.
    pub(crate) fn spawn(spec: &TurnSpec, launch: Launch<'_>) -> std::io::Result<Self> {
        let mut command = Command::new(&launch.program);
        for (name, _) in std::env::vars_os() {
            let text = name.to_string_lossy();
            if spec
                .env_remove
                .iter()
                .any(|removed| text.eq_ignore_ascii_case(removed))
            {
                command.env_remove(&name);
            }
        }
        if !launch.home.trim().is_empty() {
            command.env(launch.home_var, launch.home);
        }
        for (name, value) in spec.env.iter().chain(&launch.extra_env) {
            command.env(name, value);
        }
        #[cfg(unix)]
        command.process_group(0);
        let mut child = command
            .args(&launch.args)
            .current_dir(&spec.cwd)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()?;
        let stdin = child.stdin.take();
        let stdout = child.stdout.take().expect("stdout is piped");
        let stderr_tail = std::sync::Arc::default();
        if let Some(stderr) = child.stderr.take() {
            // The CLI's own diagnostics can hold private text, so they stay
            // in this process's log rather than in turn events.
            tokio::spawn(log_stderr(
                launch.program.clone(),
                stderr,
                std::sync::Arc::clone(&stderr_tail),
            ));
        }
        Ok(Self {
            child,
            stdin,
            lines: BufReader::new(stdout).lines(),
            stderr_tail,
        })
    }

    /// Whether the CLI's standard error so far contains `needle`.
    pub(crate) fn stderr_contains(&self, needle: &str) -> bool {
        self.stderr_tail
            .lock()
            .map(|tail| tail.contains(needle))
            .unwrap_or(false)
    }

    /// The next protocol line, or `None` once the CLI closed its output.
    pub(crate) async fn next_line(&mut self) -> Option<String> {
        loop {
            match self.lines.next_line().await {
                Ok(Some(line)) if line.trim().is_empty() => continue,
                Ok(line) => return line,
                Err(_) => return None,
            }
        }
    }

    /// Write one protocol message as a line.
    pub(crate) async fn write(&mut self, message: &serde_json::Value) -> std::io::Result<()> {
        let Some(stdin) = self.stdin.as_mut() else {
            return Err(std::io::ErrorKind::BrokenPipe.into());
        };
        let mut line = message.to_string();
        line.push('\n');
        stdin.write_all(line.as_bytes()).await?;
        stdin.flush().await
    }

    /// Stop the CLI and everything it started.
    pub(crate) async fn stop(mut self) {
        drop(self.stdin.take());
        #[cfg(unix)]
        if let Some(pid) = self.child.id().and_then(|pid| i32::try_from(pid).ok()) {
            // SAFETY: kill(2) with a negative pid signals that process group;
            // it touches no memory of ours.
            unsafe { libc::kill(-pid, libc::SIGKILL) };
        }
        let _ = self.child.kill().await;
        let _ = self.child.wait().await;
    }

    /// Close the CLI's input and give it a moment to exit on its own, then
    /// stop it.
    pub(crate) async fn close(mut self) {
        drop(self.stdin.take());
        let _ = tokio::time::timeout(std::time::Duration::from_secs(2), self.child.wait()).await;
        self.stop().await;
    }

    /// How the CLI exited, if it has.
    pub(crate) async fn exit_status(&mut self) -> Option<std::process::ExitStatus> {
        tokio::time::timeout(std::time::Duration::from_secs(2), self.child.wait())
            .await
            .ok()
            .and_then(Result::ok)
    }
}

async fn log_stderr(
    program: String,
    stderr: impl AsyncRead + Unpin,
    tail: std::sync::Arc<std::sync::Mutex<String>>,
) {
    const TAIL: usize = 8 * 1024;
    let mut lines = BufReader::new(stderr).lines();
    while let Ok(Some(line)) = lines.next_line().await {
        tracing::debug!(target: "provider_turns", program = %program, "{line}");
        if let Ok(mut tail) = tail.lock() {
            tail.push_str(&line);
            tail.push('\n');
            if tail.len() > TAIL {
                let mut cut = tail.len() - TAIL;
                while !tail.is_char_boundary(cut) {
                    cut += 1;
                }
                tail.drain(..cut);
            }
        }
    }
}

/// The failure of a CLI that exited before the turn ended.
/// The reference's failure when Codex's app-server closes mid-turn: its
/// event stream ends, whatever the exit status.
fn codex_crashed(status: Option<std::process::ExitStatus>) -> ProviderFailure {
    tracing::warn!(?status, "Codex app-server exited during a turn");
    ProviderFailure::new("unknown", crate::workspace::provider::CODEX_STREAM_CLOSED)
}

/// The Claude Agent SDK's failure when Claude Code exits mid-turn.
fn claude_crashed(status: Option<std::process::ExitStatus>) -> ProviderFailure {
    let mut failure = ProviderFailure::new(
        "transport_error",
        match status.and_then(|s| s.code()) {
            Some(code) => format!("Claude Code process exited with code {code}."),
            None => "Claude Code process exited unexpectedly.".into(),
        },
    );
    failure.code = Some("process_exited_nonzero".into());
    failure
}

/// The program a driver runs by default, or the configured one.
fn program(binary_path: &str, default: &str) -> String {
    let trimmed = binary_path.trim();
    if trimmed.is_empty() {
        default.into()
    } else {
        trimmed.into()
    }
}

/// Resolves once the turn is cancelled: its cancel fired or was dropped, as
/// for command turns. Resolves only once; later calls never resolve.
async fn cancelled(cancel: &mut Option<oneshot::Receiver<()>>) {
    match cancel.as_mut() {
        Some(receiver) => {
            let _ = receiver.await;
            *cancel = None;
        }
        None => std::future::pending::<()>().await,
    }
}

/// The next answer to one of the turn's runtime requests; never resolves
/// once the channel closed.
async fn next_response(
    responses: &mut mpsc::UnboundedReceiver<crate::workspace::provider::RuntimeResponse>,
) -> crate::workspace::provider::RuntimeResponse {
    match responses.recv().await {
        Some(response) => response,
        None => std::future::pending().await,
    }
}

async fn sleep_until(deadline: Option<tokio::time::Instant>) {
    match deadline {
        Some(deadline) => tokio::time::sleep_until(deadline).await,
        None => std::future::pending().await,
    }
}

#[cfg(all(test, unix))]
mod tests;
