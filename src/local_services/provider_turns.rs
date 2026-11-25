//! Provider turns: run a coding agent's CLI for one turn and translate its
//! protocol into [`ProviderEvent`]s.
//!
//! A Claude turn writes its prompt to the CLI its thread keeps running
//! ([`claude_sessions`]), or starts one that continues the thread's native
//! session. Codex turns run in the app-server their provider instance
//! shares across threads ([`codex_server`]), as the reference's do.
//! The thread's runtime mode decides what the CLI may do without asking;
//! every approval or question it still asks waits for the user (see
//! [`requests`]).
mod acp;
mod claude;
mod claude_background;
mod claude_sessions;
mod codex;
mod codex_server;
mod codex_subagents;
mod muse;
mod opencode;
mod opencode2;
pub(crate) mod pi;
mod requests;

pub(crate) use codex_server::Registry as CodexServers;

/// The provider processes a workspace keeps across turns: each Codex
/// instance's shared app-server, and each thread's Claude CLI.
#[derive(Default)]
pub(crate) struct ProviderProcesses {
    codex: std::sync::Arc<CodexServers>,
    claude: std::sync::Arc<claude_sessions::Sessions>,
    acp: std::sync::Arc<acp::Sessions>,
    pi: std::sync::Arc<pi::Sessions>,
    muse: std::sync::Arc<muse::Sessions>,
    opencode: std::sync::Arc<opencode::Sessions>,
    /// Where the provider processes' protocol frames are recorded.
    events: ProviderEventLog,
}

impl ProviderProcesses {
    /// Processes whose protocol frames go to `events`' per-thread logs.
    pub(crate) fn with_event_log(events: ProviderEventLog) -> Self {
        Self {
            events,
            ..Self::default()
        }
    }

    /// Run OpenCode turns on the managed servers `owners` keeps (the status
    /// checks'), as the reference has one OpenCode 2 server per instance.
    pub(crate) fn share_opencode_servers(&self, owners: crate::provider_status::opencode::Owners) {
        self.opencode.share_servers(owners);
    }

    /// Unload a detached thread's native thread or session: its Codex thread
    /// from its instance's app-server, its Claude CLI closed.
    pub(crate) fn unload_thread(&self, instance: &str, thread: &str) {
        codex_server::unload_thread(&self.codex, instance, thread);
        self.claude.close(thread);
        self.acp.close(thread);
        self.pi.close(thread);
        self.muse.close(thread);
        self.opencode.close(thread);
    }

    /// The wakes idle Claude CLIs report; taken once.
    pub(crate) fn take_wakes(
        &self,
    ) -> Option<mpsc::UnboundedReceiver<crate::workspace::provider::ProviderWake>> {
        self.claude.take_wakes()
    }

    /// The continuation for `wake` ended without starting.
    pub(crate) fn retire_wake(&self, wake: &crate::workspace::provider::ProviderWake) {
        self.claude.retire_wake(&wake.native_thread_id, wake.token);
    }

    /// Stop them all (the server is shutting down), once their turns ended
    /// or after `grace`.
    pub(crate) async fn shutdown(&self, grace: std::time::Duration) {
        codex_server::shutdown(&self.codex, grace).await;
        self.claude.shutdown().await;
        self.acp.shutdown().await;
        self.pi.shutdown().await;
        self.muse.shutdown().await;
        self.opencode.shutdown().await;
        self.events.flush().await;
    }
}
pub(crate) use requests::respond;

use super::provider_event_log::{Direction, Protocol, ProtocolLog, ProviderEventLog};
use crate::workspace::provider::{
    ProviderFailure, ProviderItem, ThreadDisposition, TurnStatus, CODEX,
};
use crate::workspace::{ProviderEvent, TurnEvent, TurnExit, TurnSpec};
use std::process::Stdio;
use tokio::{
    io::{AsyncBufReadExt, AsyncRead, AsyncWriteExt, BufReader, Lines},
    process::{Child, ChildStdin, ChildStdout},
    sync::{mpsc, oneshot},
};

/// Start a provider turn. Its events arrive on the returned channel and end
/// with `Exited`.
pub(crate) fn start(
    spec: TurnSpec,
    cancel: oneshot::Receiver<()>,
    processes: std::sync::Arc<ProviderProcesses>,
) -> anyhow::Result<mpsc::UnboundedReceiver<TurnEvent>> {
    let provider = spec
        .provider
        .clone()
        .ok_or_else(|| anyhow::anyhow!("the turn names no provider"))?;
    if !crate::workspace::provider::runs_turns(&provider.driver) {
        anyhow::bail!("provider driver `{}` cannot run turns", provider.driver);
    }
    let (events, receiver) = mpsc::unbounded_channel();
    let sink = Sink::new(
        events,
        provider.driver.clone(),
        spec.turn_id.clone(),
        processes.events.clone(),
    );
    // As the reference opens a session: only in a folder that exists.
    if !std::path::Path::new(&spec.cwd).is_dir() {
        let failure = crate::workspace::provider::workspace_missing(&spec.cwd);
        sink.finish(TurnEnd::failed(failure, ThreadDisposition::Reusable));
        return Ok(receiver);
    }
    let responses = requests::register(&spec.turn_id);
    if provider.driver == CODEX {
        tokio::spawn(codex::run(
            spec,
            provider,
            cancel,
            responses,
            sink,
            processes.codex.clone(),
        ));
    } else if provider.driver == crate::workspace::provider::PI {
        tokio::spawn(pi::run(
            spec,
            provider,
            cancel,
            responses,
            sink,
            processes.pi.clone(),
        ));
    } else if provider.driver == crate::workspace::provider::MUSE {
        tokio::spawn(muse::run(
            spec,
            provider,
            cancel,
            responses,
            sink,
            processes.muse.clone(),
        ));
    } else if provider.driver == crate::workspace::provider::OPENCODE {
        tokio::spawn(opencode::run(
            spec,
            provider,
            cancel,
            responses,
            sink,
            processes.opencode.clone(),
        ));
    } else if crate::workspace::provider::is_acp(&provider.driver) {
        tokio::spawn(acp::run(
            spec,
            provider,
            cancel,
            responses,
            sink,
            processes.acp.clone(),
        ));
    } else {
        tokio::spawn(claude::run(
            spec,
            provider,
            cancel,
            responses,
            sink,
            processes.claude.clone(),
        ));
    }
    Ok(receiver)
}

/// The models the provider's CLI lists, launched as a turn of `spec` would
/// launch it. Only Codex lists them (`model/list`).
pub(crate) async fn list_models(spec: TurnSpec) -> anyhow::Result<Vec<serde_json::Value>> {
    let provider = spec
        .provider
        .clone()
        .ok_or_else(|| anyhow::anyhow!("the request names no provider"))?;
    if provider.driver != CODEX {
        anyhow::bail!("provider driver `{}` lists no models", provider.driver);
    }
    codex::list_models(&spec, &provider).await
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
    /// A Codex turn's token usage, held to go with its end.
    usage: std::sync::Arc<std::sync::Mutex<Option<ProviderEvent>>>,
    /// The native thread the turn's end reports (a new Claude session's
    /// own id), held to go with the end.
    thread_report: std::sync::Arc<std::sync::Mutex<Option<ProviderEvent>>>,
    /// The background tasks the turn's end reports (Claude's completed
    /// provider thread lists them).
    tasks_report: std::sync::Arc<std::sync::Mutex<Option<ProviderEvent>>>,
    /// The provider event logs the turn's protocol frames go to.
    log: ProviderEventLog,
}

impl Sink {
    fn new(
        events: mpsc::UnboundedSender<TurnEvent>,
        driver: String,
        turn_id: String,
        log: ProviderEventLog,
    ) -> Self {
        Self {
            events,
            driver,
            turn_id,
            log,
            open: std::sync::Arc::default(),
            usage: std::sync::Arc::default(),
            thread_report: std::sync::Arc::default(),
            tasks_report: std::sync::Arc::default(),
        }
    }
    /// The recorder for a provider process this turn starts, speaking
    /// `protocol` for `thread`; `None` when no logs are kept.
    fn protocol_log(&self, protocol: Protocol, thread: &str) -> Option<ProtocolLog> {
        self.log.protocol(protocol, Some(thread))
    }
    /// Report `event` (a `provider_thread.updated`) with the turn's end.
    fn report_thread_at_end(&self, event: ProviderEvent) {
        if let Ok(mut report) = self.thread_report.lock() {
            *report = Some(event);
        }
    }
    /// Report `event` (a `background_tasks.updated`) with the turn's end.
    fn report_at_end(&self, event: ProviderEvent) {
        if let Ok(mut report) = self.tasks_report.lock() {
            *report = Some(event);
        }
    }
    fn send(&self, event: ProviderEvent) {
        // A Codex or ACP turn's usage goes with its end; Claude's adapter
        // reports it on its own first.
        if let (ProviderEvent::TurnUsage { .. }, true) = (
            &event,
            self.driver == CODEX || crate::workspace::provider::is_acp(&self.driver),
        ) {
            if let Ok(mut usage) = self.usage.lock() {
                *usage = Some(event);
                return;
            }
        }
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
        // A Codex turn's end goes with what it closes: its usage and its
        // open items, as the reference writes them together.
        let mut closing: Vec<ProviderEvent> =
            [&self.thread_report, &self.tasks_report, &self.usage]
                .into_iter()
                .filter_map(|held| held.lock().ok().and_then(|mut held| held.take()))
                .collect();
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
            // Claude's adapter closes its open tool calls before it reports
            // the turn's end; Codex's turn end closes its items.
            if self.driver == CODEX {
                closing.push(ProviderEvent::TurnItem { item });
            } else {
                self.send(ProviderEvent::TurnItem { item });
            }
        }
        let success = end.status == TurnStatus::Completed;
        self.send(ProviderEvent::TurnTerminal {
            status: end.status,
            failure: end.failure,
            thread_disposition: end.disposition,
            driver: Some(self.driver.clone()),
            closing,
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

/// Turns a session detach stopped (by turn id), which a driver whose
/// session is the thread's own process ends instead of interrupting.
static DETACHED: std::sync::Mutex<Vec<String>> = std::sync::Mutex::new(Vec::new());

/// Mark `turn_id` as stopped by a session detach, before its cancel fires.
pub(crate) fn mark_detached(turn_id: &str) {
    if let Ok(mut detached) = DETACHED.lock() {
        detached.push(turn_id.to_owned());
    }
}

/// Whether `turn_id` was detached, forgetting it.
fn take_detached(turn_id: &str) -> bool {
    DETACHED.lock().is_ok_and(|mut detached| {
        let before = detached.len();
        detached.retain(|id| id != turn_id);
        detached.len() != before
    })
}

/// How the reference ends a turn whose session a thread change detached
/// by stopping its process: the event stream closed.
fn detached_failure() -> ProviderFailure {
    ProviderFailure::new("unknown", crate::workspace::provider::CODEX_STREAM_CLOSED)
}

/// The CLI of one provider turn: its process and protocol streams.
pub(crate) struct Cli {
    child: Child,
    stdin: Option<ChildStdin>,
    lines: Lines<BufReader<ChildStdout>>,
    /// Lines read while no turn ran (an idle Claude CLI's own wake), which
    /// the next turn handles first.
    buffered: std::collections::VecDeque<String>,
    /// The background tasks a Claude session runs (`local_bash` commands), whose end
    /// while no turn runs is reported with the wake it starts.
    pub(crate) background: claude_background::Roster,
    /// The end of the CLI's standard error, for recognizing known refusals.
    stderr_tail: std::sync::Arc<std::sync::Mutex<String>>,
    /// The task reading standard error, until it has read all of it.
    stderr_reader: Option<tokio::task::JoinHandle<()>>,
    /// Where the lines it reads and writes are recorded.
    log: Option<ProtocolLog>,
}

/// What a CLI is launched with.
pub(crate) struct Launch<'a> {
    pub program: String,
    pub args: Vec<String>,
    /// The variable naming the CLI's home, set when `home` is not empty.
    pub home_var: &'static str,
    pub home: &'a str,
    pub extra_env: Vec<(String, String)>,
    /// Where its protocol lines are recorded.
    pub log: Option<ProtocolLog>,
}

impl Cli {
    /// Start the CLI in the turn's directory with the turn's environment.
    /// The CLI leads its own process group, so stopping it stops the
    /// commands it ran too.
    pub(crate) fn spawn(spec: &TurnSpec, launch: Launch<'_>) -> std::io::Result<Self> {
        let mut command = crate::login_shell::tokio_command(&launch.program);
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
        // The CLI's own diagnostics can hold private text, so they stay in
        // this process's log rather than in turn events.
        let stderr_reader = child.stderr.take().map(|stderr| {
            tokio::spawn(log_stderr(
                launch.program.clone(),
                stderr,
                std::sync::Arc::clone(&stderr_tail),
            ))
        });
        Ok(Self {
            child,
            stdin,
            lines: BufReader::new(stdout).lines(),
            buffered: Default::default(),
            background: Default::default(),
            stderr_tail,
            stderr_reader,
            log: launch.log,
        })
    }

    /// Where the CLI's protocol lines are recorded.
    pub(crate) fn log(&self) -> Option<&ProtocolLog> {
        self.log.as_ref()
    }

    /// Whether the CLI's standard error so far contains `needle`.
    pub(crate) fn stderr_contains(&self, needle: &str) -> bool {
        self.stderr_tail
            .lock()
            .map(|tail| tail.contains(needle))
            .unwrap_or(false)
    }

    /// The next protocol line, or `None` once the CLI closed its output:
    /// first those read while it was idle.
    pub(crate) async fn next_line(&mut self) -> Option<String> {
        if let Some(line) = self.buffered.pop_front() {
            return Some(line);
        }
        self.read_line().await
    }

    /// Keep a line read while no turn ran, for the next turn.
    pub(crate) fn buffer_line(&mut self, line: String) {
        self.buffered.push_back(line);
    }

    /// The lines read while no turn ran, in order.
    pub(crate) fn buffered_lines(&self) -> impl Iterator<Item = &str> {
        self.buffered.iter().map(String::as_str)
    }

    /// Drop the buffered wake a continuation would have shown: through its
    /// `result`, or all of it when it has not ended.
    pub(crate) fn drop_buffered_wake(&mut self) {
        let end = self.buffered.iter().position(|line| {
            claude_sessions::wake_boundary(line)
                && serde_json::from_str::<serde_json::Value>(line)
                    .is_ok_and(|message| message["type"] == "result")
        });
        match end {
            Some(end) => drop(self.buffered.drain(..=end)),
            None => self.buffered.clear(),
        }
    }

    /// The next line the CLI writes, or `None` once it closed its output.
    /// Cancel safe.
    pub(crate) async fn read_line(&mut self) -> Option<String> {
        loop {
            match self.lines.next_line().await {
                Ok(Some(line)) if line.trim().is_empty() => continue,
                Ok(Some(line)) => {
                    if let Some(log) = &self.log {
                        log.line(Direction::Incoming, &line);
                    }
                    return Some(line);
                }
                Ok(None) => return None,
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
        if let Some(log) = &self.log {
            log.line(Direction::Outgoing, &line);
        }
        line.push('\n');
        stdin.write_all(line.as_bytes()).await?;
        stdin.flush().await
    }

    /// Kill the CLI and everything it started, now, keeping the handle so
    /// its closed output still ends the turn.
    /// Whether the CLI is still running.
    pub(crate) fn alive(&mut self) -> bool {
        matches!(self.child.try_wait(), Ok(None))
    }

    pub(crate) fn kill_now(&mut self) {
        drop(self.stdin.take());
        #[cfg(unix)]
        if let Some(pid) = self.child.id().and_then(|pid| i32::try_from(pid).ok()) {
            // SAFETY: kill(2) with a negative pid signals that process group;
            // it touches no memory of ours.
            unsafe { libc::kill(-pid, libc::SIGKILL) };
        }
        let _ = self.child.start_kill();
    }

    /// Stop the CLI and everything it started.
    pub(crate) async fn stop(mut self) {
        if let Some(log) = &self.log {
            log.closing();
        }
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

    /// How the CLI exited, if it has. Once it has, its standard error is
    /// read to the end (briefly), so `stderr_contains` sees all of it.
    pub(crate) async fn exit_status(&mut self) -> Option<std::process::ExitStatus> {
        let status = tokio::time::timeout(std::time::Duration::from_secs(2), self.child.wait())
            .await
            .ok()
            .and_then(Result::ok);
        if let Some(reader) = self.stderr_reader.take().filter(|_| status.is_some()) {
            let _ = tokio::time::timeout(std::time::Duration::from_secs(1), reader).await;
        }
        status
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

/// The reference's failure when Codex's app-server closes mid-turn: its
/// event stream ends, naming how the process exited (#39299).
fn codex_crashed(status: Option<std::process::ExitStatus>) -> ProviderFailure {
    tracing::warn!(?status, "Codex app-server exited during a turn");
    ProviderFailure::new(
        "unknown",
        format!(
            "The provider event stream closed unexpectedly: the provider process exited{}. {}",
            process_exit_status(status),
            "Check the server logs for the provider's output, then retry the turn."
        ),
    )
}

/// The reference's `processExitStatus`: the signal, else the code.
fn process_exit_status(status: Option<std::process::ExitStatus>) -> String {
    #[cfg(unix)]
    let signal = {
        use std::os::unix::process::ExitStatusExt;
        status.and_then(|s| s.signal()).and_then(|n| {
            Some(match n {
                1 => "SIGHUP",
                2 => "SIGINT",
                3 => "SIGQUIT",
                4 => "SIGILL",
                6 => "SIGABRT",
                9 => "SIGKILL",
                11 => "SIGSEGV",
                13 => "SIGPIPE",
                15 => "SIGTERM",
                _ => return None,
            })
        })
    };
    #[cfg(not(unix))]
    let signal: Option<&str> = None;
    match (signal, status.and_then(|s| s.code())) {
        (Some(signal), _) => format!(" after signal {signal}"),
        (None, Some(code)) => format!(" with code {code}"),
        _ => String::new(),
    }
}

/// The reference's failure when an ACP agent's process exits mid-turn: the
/// transport closed.
fn acp_crashed(status: Option<std::process::ExitStatus>) -> ProviderFailure {
    tracing::warn!(?status, "ACP agent exited during a turn");
    ProviderFailure::new(
        "provider_error",
        format!(
            "The provider event stream closed unexpectedly: the provider process exited{}. {}",
            process_exit_status(status),
            "Check the server logs for the provider's output, then retry the turn."
        ),
    )
}

/// The Claude Agent SDK's failure when Claude Code exits mid-turn.
fn claude_crashed(status: Option<std::process::ExitStatus>) -> ProviderFailure {
    let mut failure = ProviderFailure::new("transport_error", format!("{}.", claude_exit(status)));
    failure.code = Some("process_exited_nonzero".into());
    failure
}

/// How Claude Code exited, as the SDK words it.
fn claude_exit(status: Option<std::process::ExitStatus>) -> String {
    match status.and_then(|s| s.code()) {
        Some(code) => format!("Claude Code process exited with code {code}"),
        None => "Claude Code process exited unexpectedly".into(),
    }
}

/// `ClaudeCodeStartupExitError` (upstream #39299): Claude Code exited before it
/// answered. `stderr` only picks the guidance (`providerProcessExitGuidance`).
fn claude_startup_exit(status: Option<std::process::ExitStatus>, eperm: bool) -> ProviderFailure {
    let guidance = if eperm {
        "It was denied access to a file or folder (EPERM). On macOS, allow T3 Code under System Settings > Privacy & Security > Files and Folders, or move the project out of Desktop, Documents, or Downloads, then retry the turn."
    } else {
        "Check the server logs for the provider's output, then retry the turn."
    };
    let mut failure = ProviderFailure::new(
        "transport_error",
        format!("{} before it started. {guidance}", claude_exit(status)),
    );
    failure.code = Some("process_exited_nonzero".into());
    failure
}

/// The provider session a turn opening its provider process names, as the
/// projection names it (Codex's instance-wide one, else the thread's own,
/// named after the opening run).
fn provider_session_id(
    spec: &TurnSpec,
    provider: &crate::workspace::provider::ProviderTurn,
    opencode2: bool,
) -> String {
    crate::provider_projection::opened_session_id(
        &provider.driver,
        &provider.instance_id,
        &provider.app_thread_id,
        &spec.turn_id,
        opencode2,
    )
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
