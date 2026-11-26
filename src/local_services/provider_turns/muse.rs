//! Muse Code turns over MSP (`muse serve`), as the reference's Muse adapter
//! (`provider-muse`, through Meta's `@muse-code/sdk`) runs them.
//!
//! MSP is newline-delimited JSON-RPC 2.0 on the stdio of a long-lived
//! `muse serve`. Each thread keeps its host across turns ([`Sessions`]).
//! T3 mints the native session id and each native turn id (UUIDv7s): a
//! mutating request is a command carrying a `commandId`, which `turn/start`
//! sets to the turn's id. Muse streams items (`item/started`, `item/delta`,
//! `item/completed`, with a revision each), usage and approvals as
//! notifications, and ends the turn with `turn/completed`. Its approval
//! server requests are answered `{}` at once; the user's decision goes back
//! with `approval/decide`.
//!
//! Any frame that is not JSON-RPC 2.0, a reply T3 did not ask for, a request
//! that times out or an exit fails the host: a turn it ran fails `broken`,
//! and the thread's next turn launches another.
use super::{cancelled, next_response, program, Cli, Launch, Protocol, ProtocolLog, Sink, TurnEnd};
use crate::workspace::provider::{
    ContextUsage, ProviderFailure, ProviderItem, ProviderTurn, RuntimeResponse, SessionReport,
    SessionStatus, ThreadDisposition, TokenUsage, TurnStatus, MUSE,
};
use crate::workspace::{ProviderEvent, TurnSpec};
use serde_json::{json, Map, Value};
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};
use tokio::time::Instant;

/// Every request's bound (`requestTimeoutMs`); a timeout fails the host.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
/// `initialize`'s bound for a turn's host.
const STARTUP_TIMEOUT: Duration = Duration::from_secs(20);
/// How long Stop waits for Muse to end the turn.
const INTERRUPT_TIMEOUT: Duration = Duration::from_secs(30);
/// How soon streamed deltas show (the adapter's flush).
const STREAM_FLUSH: Duration = Duration::from_millis(50);
/// How long a host stays open after its thread's last turn.
const IDLE_RELEASE_DELAY: Duration = Duration::from_secs(30 * 60);
/// How long a closing host may take to exit once its input ended, then
/// after SIGTERM (`shutdownTimeoutMs`, then the SDK's kill grace).
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(10);
const TERMINATE_GRACE: Duration = Duration::from_secs(2);
/// `turn/start` is retried this often when Muse admitted nothing.
const TURN_START_ATTEMPTS: u32 = 3;
const HARNESS: &str = "Muse Code";
/// Tools whose rows the reference draws natively (not as tool items).
const TOOLS_WITH_NATIVE_ROWS: [&str; 3] = ["write_todos", "request_user_input", "workflow"];
/// Notifications whose unreadable params are only skipped.
const INFORMATIONAL: [&str; 6] = [
    "session/contextUsage",
    "session/tokenUsage",
    "session/todoListChanged",
    "turn/retryScheduled",
    "turn/modelRetryScheduled",
    "usage/changed",
];

/// The threads' Muse hosts, by app thread.
#[derive(Default)]
pub(crate) struct Sessions {
    live: Mutex<HashMap<String, Live>>,
}

struct Live {
    host: Host,
    idle: tokio::task::JoinHandle<()>,
}

impl Sessions {
    fn live(&self) -> std::sync::MutexGuard<'_, HashMap<String, Live>> {
        self.live.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// The thread's host, when it was launched as `launch` and holds
    /// `session` (when one is named); any other is closed.
    async fn take(&self, thread: &str, launch: &str, session: Option<&str>) -> Option<Host> {
        let live = self.live().remove(thread)?;
        live.idle.abort();
        let mut host = live.host;
        if host.launch == launch
            && host.cli.alive()
            && session.is_none_or(|session| session == host.session)
        {
            return Some(host);
        }
        close(host.cli).await;
        None
    }

    fn keep(self: &Arc<Self>, thread: String, host: Host) {
        let sessions: Weak<Self> = Arc::downgrade(self);
        let key = thread.clone();
        let idle = tokio::spawn(async move {
            tokio::time::sleep(IDLE_RELEASE_DELAY).await;
            if let Some(sessions) = sessions.upgrade() {
                let live = sessions.live().remove(&key);
                if let Some(live) = live {
                    close(live.host.cli).await;
                }
            }
        });
        if let Some(previous) = self.live().insert(thread, Live { host, idle }) {
            previous.idle.abort();
            tokio::spawn(close(previous.host.cli));
        }
    }

    /// Close the host holding the native `session`, if one runs (a detach).
    pub(crate) fn close(&self, session: &str) {
        let mut live = self.live();
        let threads: Vec<String> = live
            .iter()
            .filter(|(_, live)| live.host.session == session)
            .map(|(thread, _)| thread.clone())
            .collect();
        for thread in threads {
            if let Some(live) = live.remove(&thread) {
                live.idle.abort();
                tokio::spawn(close(live.host.cli));
            }
        }
    }

    pub(crate) async fn shutdown(&self) {
        let all: Vec<Live> = self.live().drain().map(|(_, live)| live).collect();
        for live in all {
            live.idle.abort();
            close(live.host.cli).await;
        }
    }
}

/// `host.close()`: end Muse's input (MSP has no close call), give it 10 s to
/// exit, then SIGTERM its process group, 2 s, then kill it.
async fn close(mut cli: Cli) {
    drop(cli.stdin.take());
    let exited = tokio::time::timeout(SHUTDOWN_TIMEOUT, cli.child.wait()).await;
    #[cfg(unix)]
    if exited.is_err() {
        if let Some(pid) = cli.child.id().and_then(|pid| i32::try_from(pid).ok()) {
            // SAFETY: kill(2) with a negative pid signals that process group;
            // it touches no memory of ours.
            unsafe { libc::kill(-pid, libc::SIGTERM) };
        }
        let _ = tokio::time::timeout(TERMINATE_GRACE, cli.child.wait()).await;
    }
    #[cfg(not(unix))]
    let _ = exited;
    cli.stop().await;
}

/// The SDK's UUIDv7 mint (`createUuidV7Mint`): the time in the first 48
/// bits, a 12-bit sequence for ids minted in the same millisecond, the rest
/// random.
#[derive(Default)]
struct Mint {
    last_ms: u64,
    sequence: u16,
}

impl Mint {
    fn next(&mut self) -> String {
        let mut now = u64::try_from(crate::platform::now_ms()).unwrap_or(0);
        if now <= self.last_ms {
            now = self.last_ms;
            self.sequence += 1;
            if self.sequence > 0x0fff {
                now += 1;
                self.sequence = 0;
            }
        } else {
            self.sequence = 0;
        }
        self.last_ms = now;
        let mut bytes: [u8; 16] = crate::platform::random_bytes();
        bytes[..6].copy_from_slice(&now.to_be_bytes()[2..]);
        bytes[6] = 0x70 | ((self.sequence >> 8) as u8 & 0x0f);
        bytes[7] = (self.sequence & 0xff) as u8;
        bytes[8] = (bytes[8] & 0x3f) | 0x80;
        uuid::Uuid::from_bytes(bytes).hyphenated().to_string()
    }
}

/// Why a call to Muse failed.
#[derive(Debug)]
enum MspError {
    /// Muse closed its output.
    Closed,
    TimedOut,
    /// A frame that breaks the protocol: the host is failed with its text.
    Protocol(String),
    /// Muse answered with an error (`MspError`): its code, text and kind.
    Rpc {
        code: i64,
        message: String,
        kind: String,
    },
}

impl MspError {
    /// Whether the host can no longer be used.
    fn fails_host(&self) -> bool {
        !matches!(self, Self::Rpc { .. })
    }

    /// The adapter's text for `method` failing this way.
    fn detail(&self, method: &str) -> String {
        match self {
            Self::Closed => "Muse connection closed unexpectedly".into(),
            Self::TimedOut => format!("Muse {method} timed out; its host was closed"),
            Self::Protocol(text) => text.clone(),
            Self::Rpc { message, .. } if message.is_empty() => format!("Muse {method} failed"),
            Self::Rpc { message, .. } => format!("Muse {method} failed: {message}"),
        }
    }
}

/// What Muse wrote: a notification, or a reply to a request.
enum Incoming {
    Notification(Value),
    Reply(Value),
}

/// One `muse serve` host and its session.
struct Host {
    cli: Cli,
    /// What it was launched with: a turn launched otherwise starts another.
    launch: String,
    next_id: i64,
    mint: Mint,
    /// The native session (empty until one started or resumed).
    session: String,
    /// The model the session runs (`session.model`).
    model: Option<String>,
    /// The canonical folder: its workspace root.
    root: String,
    /// Whether Muse granted `sessionMcp`.
    session_mcp: bool,
    /// Notifications read while waiting for a reply, handled next in order.
    queued: VecDeque<Value>,
    /// The session's context usage, as Muse last reported it.
    context: Option<ContextUsage>,
}

impl Host {
    async fn write(&mut self, frame: &Value) -> Result<(), MspError> {
        self.cli.write(frame).await.map_err(|_| MspError::Closed)
    }

    /// The next notification or reply. Server requests are answered here:
    /// `{}` for approvals and questions (their notifications carry them),
    /// an error for anything else. A frame that is not JSON-RPC 2.0 is a
    /// protocol error.
    async fn read(&mut self) -> Result<Incoming, MspError> {
        loop {
            let Some(line) = self.cli.next_line().await else {
                return Err(MspError::Closed);
            };
            let line = line.strip_suffix('\r').unwrap_or(&line);
            if line.trim().is_empty() {
                continue;
            }
            let Ok(frame) = serde_json::from_str::<Value>(line) else {
                return Err(MspError::Protocol("inbound frame is not valid JSON".into()));
            };
            if !frame.is_object() || frame["jsonrpc"] != "2.0" {
                return Err(MspError::Protocol(
                    "inbound frame is not a JSON-RPC 2.0 object".into(),
                ));
            }
            let id = frame
                .get("id")
                .filter(|id| id.is_string() || id.is_number());
            match (frame["method"].as_str(), id) {
                (Some(method), Some(id)) => {
                    if id.as_i64().is_none_or(|id| id <= 0) {
                        return Err(MspError::Protocol(
                            "server request id must be a positive integer".into(),
                        ));
                    }
                    let answer = if matches!(method, "approval/request" | "userInput/request") {
                        json!({"jsonrpc": "2.0", "id": id, "result": {}})
                    } else {
                        json!({"jsonrpc": "2.0", "id": id, "error": {"code": -32603,
                            "message": format!("Unsupported Muse server request: {method}"),
                            "data": {"kind": "internal"}}})
                    };
                    self.write(&answer).await?;
                }
                (Some(_), None) => return Ok(Incoming::Notification(frame)),
                (None, Some(_)) => return Ok(Incoming::Reply(frame)),
                (None, None) => {
                    return Err(MspError::Protocol("response has no usable id".into()));
                }
            }
        }
    }

    /// A request, with the session merged in first; a command (`command_id`
    /// set) also carries its id, which the answer must echo. Notifications
    /// read meanwhile are queued.
    async fn call(
        &mut self,
        method: &str,
        params: Value,
        command_id: Option<&str>,
        timeout: Duration,
    ) -> Result<Value, MspError> {
        let mut merged = Map::new();
        if !self.session.is_empty() {
            merged.insert("sessionId".into(), json!(self.session));
        }
        if let Value::Object(params) = params {
            merged.extend(params);
        }
        if let Some(command_id) = command_id {
            merged.insert("commandId".into(), json!(command_id));
        }
        self.next_id += 1;
        let id = self.next_id;
        self.write(&json!({"jsonrpc": "2.0", "id": id, "method": method, "params": merged}))
            .await?;
        let deadline = Instant::now() + timeout;
        loop {
            let incoming = tokio::select! {
                incoming = self.read() => incoming?,
                _ = tokio::time::sleep_until(deadline) => return Err(MspError::TimedOut),
            };
            let reply = match incoming {
                Incoming::Notification(frame) => {
                    self.queued.push_back(frame);
                    continue;
                }
                Incoming::Reply(reply) => reply,
            };
            if reply["id"].as_i64() != Some(id) {
                return Err(MspError::Protocol(format!(
                    "response for unknown request id {}",
                    reply["id"]
                )));
            }
            return reply_result(&reply, method, command_id);
        }
    }

    /// A command; `turn/start` is sent again (same id) while Muse says it
    /// admitted nothing (overloaded, backpressured).
    async fn command(
        &mut self,
        method: &str,
        params: Value,
        command_id: Option<String>,
    ) -> Result<Value, MspError> {
        let command_id = command_id.unwrap_or_else(|| self.mint.next());
        let attempts = if method == "turn/start" {
            TURN_START_ATTEMPTS
        } else {
            1
        };
        let mut attempt = 1;
        loop {
            match self
                .call(method, params.clone(), Some(&command_id), REQUEST_TIMEOUT)
                .await
            {
                Err(MspError::Rpc { code, ref kind, .. })
                    if attempt < attempts
                        && ((code == -32001 && kind == "overloaded")
                            || (code == -32031 && kind == "backpressured")) =>
                {
                    let ceiling = 50u64 << (attempt - 1).min(5);
                    let delay = rand::random::<u64>() % (ceiling.min(2_000) + 1);
                    tokio::time::sleep(Duration::from_millis(delay)).await;
                    attempt += 1;
                }
                result => return result,
            }
        }
    }
}

/// A reply's result, or its error as the SDK reads it.
fn reply_result(reply: &Value, method: &str, command_id: Option<&str>) -> Result<Value, MspError> {
    let has_result = reply.get("result").is_some();
    let has_error = reply.get("error").is_some();
    if has_result == has_error {
        return Err(MspError::Protocol(
            "response must carry exactly one of result or error".into(),
        ));
    }
    if has_result {
        let result = &reply["result"];
        if !result.is_object() {
            return Err(MspError::Rpc {
                code: 0,
                message: "response result must be an object".into(),
                kind: String::new(),
            });
        }
        if let (Some(command_id), Some(echo)) = (command_id, result.get("commandId")) {
            if echo.as_str() != Some(command_id) {
                return Err(MspError::Rpc {
                    code: 0,
                    message: format!("{method} ack commandId {echo} did not echo {command_id}"),
                    kind: String::new(),
                });
            }
        }
        return Ok(result.clone());
    }
    let error = &reply["error"];
    let (Some(code), Some(message)) = (error["code"].as_i64(), error["message"].as_str()) else {
        return Err(MspError::Rpc {
            code: 0,
            message: "error response has an invalid error object".into(),
            kind: String::new(),
        });
    };
    let Some(kind) = error["data"]["kind"].as_str() else {
        return Err(MspError::Rpc {
            code,
            message: "MSP error response has no data.kind".into(),
            kind: String::new(),
        });
    };
    Err(MspError::Rpc {
        code,
        message: message.to_owned(),
        kind: kind.to_owned(),
    })
}

/// `museApprovalMode`.
fn approval_mode(runtime_mode: &str) -> &'static str {
    if runtime_mode == "full-access" {
        "allowAll"
    } else {
        "promptUnmatched"
    }
}

/// `musePermissionProfile`: explicit, as Muse's defaults may let an AI
/// reviewer decide approvals.
fn permission_profile(runtime_mode: &str) -> &'static str {
    if runtime_mode == "full-access" {
        ":unrestricted"
    } else {
        ":ask-me"
    }
}

/// `museServeArgs` for a turn's host.
fn serve_args(runtime_mode: &str) -> Vec<String> {
    let mut args = vec!["serve".to_owned(), "--trust-workspace".to_owned()];
    if runtime_mode == "full-access" {
        args.push("--disable-sandbox".into());
    }
    args
}

/// What a launch is keyed by: a host serves only turns launched alike (its
/// argv follows the runtime mode).
fn launch_key(spec: &TurnSpec, provider: &ProviderTurn) -> String {
    json!([
        provider.binary_path,
        spec.env,
        spec.cwd,
        provider.runtime_mode(),
        provider
            .mcp
            .as_ref()
            .map(|mcp| (&mcp.url, &mcp.authorization)),
    ])
    .to_string()
}

/// `museCanonicalPath`: the folder's real path, or itself.
fn canonical(path: &str) -> String {
    std::fs::canonicalize(path)
        .map(|path| path.display().to_string())
        .unwrap_or_else(|_| path.to_owned())
}

fn failure(class: &str, detail: impl Into<String>) -> ProviderFailure {
    ProviderFailure::new(class, detail)
}

async fn spawn(
    spec: &TurnSpec,
    provider: &ProviderTurn,
    launch: String,
    log: Option<ProtocolLog>,
) -> Result<Host, ProviderFailure> {
    let mut spec = spec.clone();
    // `makeMuseEnvironment` under the instance's variables: no inherited
    // META_API_KEY (Muse uses its login), and no self-update.
    spec.env_remove.push("META_API_KEY".into());
    spec.env
        .insert(0, ("MUSE_NO_AUTO_UPDATE".into(), "1".into()));
    let root = canonical(&spec.cwd);
    let cli = Cli::spawn(
        &spec,
        Launch {
            program: program(&provider.binary_path, "muse"),
            args: serve_args(provider.runtime_mode()),
            home_var: "MUSE_HOME",
            home: "",
            extra_env: Vec::new(),
            log,
        },
    )
    .map_err(|error| failure("provider_error", error.to_string()))?;
    Ok(Host {
        cli,
        launch,
        next_id: 0,
        mint: Mint::default(),
        session: String::new(),
        model: None,
        root,
        session_mcp: false,
        queued: VecDeque::new(),
        context: None,
    })
}

/// `initialize` (20 s), checked as the SDK and adapter check it, then
/// `initialized`.
async fn initialize(host: &mut Host) -> Result<(), String> {
    let result = host
        .call(
            "initialize",
            json!({"clientInfo": {"name": "t3_code", "title": "T3 Code", "version": "1"},
                "capabilities": {"requestedCapabilities": ["sessionMcp"]}}),
            None,
            STARTUP_TIMEOUT,
        )
        .await
        .map_err(|error| error.detail("initialize"))?;
    if !result["schema"]["fingerprint"].is_string() {
        return Err("initialize result has no schema fingerprint".into());
    }
    if result["schema"]["version"] != 1 {
        return Err("Muse SDK returned an unsupported protocol envelope version.".into());
    }
    host.session_mcp = result["grantedCapabilities"]
        .as_array()
        .is_some_and(|granted| granted.iter().any(|c| c == "sessionMcp"));
    host.write(&json!({"jsonrpc": "2.0", "method": "initialized"}))
        .await
        .map_err(|error| error.detail("initialized"))
}

/// How opening the thread's session failed.
enum OpenError {
    /// Before the session was asked for: the session failed to open.
    Launch(ProviderFailure),
    /// The host failed while attaching the session.
    Host(ProviderFailure),
}

/// Attach the thread's session to a new host: `session/resume` of the
/// thread's native session (then `session/setApprovalMode`), or
/// `session/start` with a minted id. A session Muse no longer has starts
/// fresh on the same host. Whether it resumed.
async fn attach(
    host: &mut Host,
    provider: &ProviderTurn,
    resume: Option<&str>,
) -> Result<bool, ProviderFailure> {
    let host_failure = |detail: String| failure("provider_error", detail);
    if provider.mcp.is_some() && !host.session_mcp {
        return Err(host_failure(
            "Update Muse Code to a version that supports session MCP servers".into(),
        ));
    }
    let config = provider.mcp.as_ref().map(|mcp| {
        json!({"mcpServers": {"t3-code": {"transport": "streamableHttp", "mode": "optional",
            "url": mcp.url, "headers": {"Authorization": mcp.authorization}}}})
    });
    let runtime_mode = provider.runtime_mode();
    let launch = provider.muse.clone().unwrap_or_default();
    let mut resume = resume;
    loop {
        host.session = match resume {
            Some(session) => session.to_owned(),
            None => host.mint.next(),
        };
        let mut params = Map::new();
        let method = if resume.is_some() {
            params.insert("excludeItems".into(), json!(true));
            if let Some(config) = &config {
                params.insert("config".into(), config.clone());
            }
            params.insert(
                "permissionProfile".into(),
                json!(permission_profile(runtime_mode)),
            );
            "session/resume"
        } else {
            params.insert("workspaceRoot".into(), json!(host.root));
            if let Some(config) = &config {
                params.insert("config".into(), config.clone());
            }
            let model = start_model(provider, &launch);
            if let Some(model) = &model {
                params.insert("modelId".into(), json!(model));
            }
            params.insert("providerId".into(), json!("meta"));
            params.insert("approvalMode".into(), json!(approval_mode(runtime_mode)));
            params.insert(
                "permissionProfile".into(),
                json!(permission_profile(runtime_mode)),
            );
            "session/start"
        };
        let result = match host.command(method, Value::Object(params), None).await {
            Ok(result) => result,
            Err(MspError::Rpc { kind, .. })
                if resume.is_some() && matches!(kind.as_str(), "sessionNotFound" | "notFound") =>
            {
                // Muse no longer has it: a fresh session on the same host.
                resume = None;
                continue;
            }
            Err(error) => return Err(host_failure(error.detail(method))),
        };
        if result["session"]["sessionId"].as_str() != Some(host.session.as_str()) {
            return Err(host_failure(
                "Muse returned a different session identity".into(),
            ));
        }
        if result["history"]["noneReason"] == "projectionUnavailable" {
            return Err(host_failure(
                "Muse cannot stream this session because its live view is unavailable".into(),
            ));
        }
        if resume.is_some() {
            host.command(
                "session/setApprovalMode",
                json!({"mode": approval_mode(runtime_mode)}),
                None,
            )
            .await
            .map_err(|error| host_failure(error.detail("session/setApprovalMode")))?;
        }
        host.model = result["session"]["modelId"]
            .as_str()
            .map(str::to_owned)
            .or_else(|| selection_model(provider));
        return Ok(resume.is_some());
    }
}

/// The selection's model as the session records it (`default` for none).
fn selection_model(provider: &ProviderTurn) -> Option<String> {
    Some(match provider.model.trim() {
        "" => "default".to_owned(),
        model => model.to_owned(),
    })
}

/// `resolveModel`: the selection's model, or for "default" the catalog's
/// default model (none when it has none).
fn start_model(
    provider: &ProviderTurn,
    launch: &crate::workspace::provider::MuseLaunch,
) -> Option<String> {
    match provider.model.trim() {
        "" | "default" => Some(launch.default_model.clone()).filter(|model| !model.is_empty()),
        model => Some(model.to_owned()),
    }
}

pub(super) async fn run(
    spec: TurnSpec,
    provider: ProviderTurn,
    cancel: oneshot::Receiver<()>,
    mut responses: mpsc::UnboundedReceiver<RuntimeResponse>,
    sink: Sink,
    sessions: Arc<Sessions>,
) {
    let mut cancel = Some(cancel);
    let thread = provider.app_thread_id.clone();
    let launch = launch_key(&spec, &provider);
    let resume = provider.resume.clone().filter(|id| !id.is_empty());
    let opened = tokio::select! {
        opened = open(&spec, &provider, &sessions, &thread, launch, resume.as_deref(),
            sink.protocol_log(Protocol::Muse, &thread)) => opened,
        _ = cancelled(&mut cancel) => return sink.finish(TurnEnd::interrupted(true)),
    };
    let (mut host, resumed) = match opened {
        Ok(opened) => opened,
        Err(OpenError::Launch(failure) | OpenError::Host(failure)) => {
            return sink.finish(TurnEnd::failed(failure, ThreadDisposition::Reusable))
        }
    };
    let mut turn = Turn::new(&provider, &sink, &mut responses, &host);
    let end = turn
        .run(&spec, &provider, &mut host, &mut cancel, resumed)
        .await;
    host.context = turn.context.take();
    let broken = end.disposition == ThreadDisposition::Broken;
    sink.finish(end);
    if broken {
        close(host.cli).await;
    } else {
        sessions.keep(thread, host);
    }
}

/// The thread's host: its running one, or a new one, initialized, with the
/// thread's session attached. Whether the session was resumed.
async fn open(
    spec: &TurnSpec,
    provider: &ProviderTurn,
    sessions: &Sessions,
    thread: &str,
    launch: String,
    resume: Option<&str>,
    log: Option<ProtocolLog>,
) -> Result<(Host, bool), OpenError> {
    if let Some(host) = sessions.take(thread, &launch, resume).await {
        return Ok((host, false));
    }
    let mut host = spawn(spec, provider, launch, log)
        .await
        .map_err(OpenError::Launch)?;
    if let Err(detail) = initialize(&mut host).await {
        close(host.cli).await;
        return Err(OpenError::Launch(failure("provider_error", detail)));
    }
    match attach(&mut host, provider, resume).await {
        Ok(resumed) => Ok((host, resumed)),
        Err(failure) => {
            close(host.cli).await;
            Err(OpenError::Host(failure))
        }
    }
}

/// A Muse item as the turn last stored it.
struct Stored {
    item: Value,
    revision: u64,
}

/// A waiting approval: Muse's request, as last updated, and its card.
struct Waiting {
    approval: Value,
}

struct Turn<'a> {
    sink: &'a Sink,
    responses: &'a mut mpsc::UnboundedReceiver<RuntimeResponse>,
    /// The provider instance and thread, which item ids are scoped by.
    instance: String,
    provider_thread: String,
    /// The native session and turn.
    session: String,
    native: String,
    items: HashMap<String, Stored>,
    /// Items in the order first seen, and those with unshown deltas.
    order: Vec<String>,
    dirty: Vec<String>,
    flush_at: Option<Instant>,
    /// Stop was asked for, and by when Muse must have ended the turn.
    interrupt_requested: bool,
    interrupt_deadline: Option<Instant>,
    /// Waiting approvals by T3 request id, and Muse's approval ids settled.
    waiting: HashMap<String, Waiting>,
    settled: HashSet<String>,
    runtime_mode: String,
    /// The turn's usage so far (`turnTokenUsage`) and the latest context.
    turn_usage: Option<Map<String, Value>>,
    token_usage: Option<TokenUsage>,
    context: Option<ContextUsage>,
}

impl<'a> Turn<'a> {
    fn new(
        provider: &ProviderTurn,
        sink: &'a Sink,
        responses: &'a mut mpsc::UnboundedReceiver<RuntimeResponse>,
        host: &Host,
    ) -> Self {
        Self {
            sink,
            responses,
            instance: provider.instance_id.clone(),
            provider_thread: provider
                .muse
                .as_ref()
                .map(|launch| launch.provider_thread.clone())
                .unwrap_or_default(),
            session: host.session.clone(),
            native: String::new(),
            items: HashMap::new(),
            order: Vec::new(),
            dirty: Vec::new(),
            flush_at: None,
            interrupt_requested: false,
            interrupt_deadline: None,
            waiting: HashMap::new(),
            settled: HashSet::new(),
            runtime_mode: provider.runtime_mode().to_owned(),
            turn_usage: None,
            token_usage: None,
            context: host.context.clone(),
        }
    }

    async fn run(
        &mut self,
        spec: &TurnSpec,
        provider: &ProviderTurn,
        host: &mut Host,
        cancel: &mut Option<oneshot::Receiver<()>>,
        resumed: bool,
    ) -> TurnEnd {
        self.sink.send(ProviderEvent::ProviderThread {
            driver: MUSE.into(),
            native_thread_id: host.session.clone(),
            resumed,
            model: None,
            opencode2: None,
            native_title: None,
            native_updated_at: None,
            context_usage: None,
            session_started: None,
        });
        let launch = provider.muse.clone().unwrap_or_default();
        if let Some(target) = start_model(provider, &launch) {
            if host.model.as_deref() != Some(target.as_str()) {
                let switched = host
                    .command(
                        "session/setModel",
                        json!({"model": {"modelId": target, "providerId": "meta"}}),
                        None,
                    )
                    .await;
                match switched {
                    Ok(_) => host.model = Some(target),
                    Err(error) => {
                        let broken = error.fails_host();
                        return self.failed_before_start(error.detail("session/setModel"), broken);
                    }
                }
            }
        }
        let effort = Some(launch.reasoning_effort.clone()).filter(|effort| !effort.is_empty());
        let mut input = vec![
            json!({"type": "text", "text": crate::agent_instructions::runtime(
            HARNESS, host.model.as_deref(), effort.as_deref())}),
        ];
        input.extend(prompt_parts(&spec.prompt, &provider.attachments));
        self.native = host.mint.next();
        self.sink.send(ProviderEvent::ProviderTurn {
            native_turn_id: Some(format!(
                "{}:{}:{}",
                self.instance, self.session, self.native
            )),
            native_turn_ref: Some(native_ref(&self.native)),
            conversation_head: None,
            status: TurnStatus::Running,
            model: None,
            session: Some(Box::new(SessionReport {
                status: SessionStatus::Running,
                model: host.model.clone(),
                last_error: None,
            })),
            history_injected: false,
        });
        let mut params = json!({
            "input": input,
            "displayText": if spec.prompt.is_empty() { "Image attachment" } else { spec.prompt.as_str() },
            "workspaceRoots": [host.root],
            "ifBusy": "queue",
        });
        if let Some(effort) = &effort {
            params["reasoningEffort"] = json!(effort);
        }
        let native = self.native.clone();
        match host
            .command("turn/start", params, Some(native.clone()))
            .await
        {
            Ok(result) if result["turnId"].as_str() == Some(native.as_str()) => {}
            Ok(_) => {
                return self.finish(
                    TurnStatus::Failed,
                    Some("Muse admitted an unowned turn".into()),
                    true,
                )
            }
            Err(error) => {
                let broken = error.fails_host();
                return self.finish(TurnStatus::Failed, Some(error.detail("turn/start")), broken);
            }
        }
        loop {
            let frame = if let Some(queued) = host.queued.pop_front() {
                Incoming::Notification(queued)
            } else {
                let read = tokio::select! {
                    read = host.read() => read,
                    _ = cancelled(cancel) => {
                        if let Some(end) = self.interrupt(host).await {
                            return end;
                        }
                        continue;
                    }
                    response = next_response(self.responses) => {
                        if let Some(end) = self.respond(host, response).await {
                            return end;
                        }
                        continue;
                    }
                    _ = super::sleep_until(self.flush_at) => {
                        self.flush();
                        continue;
                    }
                    _ = super::sleep_until(self.interrupt_deadline) => {
                        return self.host_failed(host, "Muse did not confirm interruption".into()).await;
                    }
                };
                match read {
                    Ok(frame) => frame,
                    // Its output closed (it exited) or broke the protocol.
                    Err(error) => {
                        return self.host_failed(host, error.detail("read")).await;
                    }
                }
            };
            let frame = match frame {
                Incoming::Notification(frame) => frame,
                Incoming::Reply(reply) => {
                    let detail = format!("response for unknown request id {}", reply["id"]);
                    return self.host_failed(host, detail).await;
                }
            };
            let method = frame["method"].as_str().unwrap_or_default().to_owned();
            match self.notification(&method, &frame["params"]) {
                Ok(Some(end)) => return end,
                Ok(None) => {}
                Err(detail) if INFORMATIONAL.contains(&method.as_str()) => {
                    tracing::warn!(%method, %detail, "skipped an unreadable Muse notification");
                }
                Err(detail) => return self.host_failed(host, detail).await,
            }
        }
    }

    /// The host failed mid-turn: the turn fails `broken`.
    async fn host_failed(&mut self, _host: &mut Host, detail: String) -> TurnEnd {
        self.finish(TurnStatus::Failed, Some(detail), true)
    }

    /// The turn could not start (its model switch failed).
    fn failed_before_start(&mut self, detail: String, broken: bool) -> TurnEnd {
        let class = if broken {
            "transport_error"
        } else {
            "provider_error"
        };
        TurnEnd::failed(
            failure(class, detail),
            if broken {
                ThreadDisposition::Broken
            } else {
                ThreadDisposition::Reusable
            },
        )
    }

    /// Stop: `turn/interrupt`, then up to 30 s for Muse to end the turn.
    async fn interrupt(&mut self, host: &mut Host) -> Option<TurnEnd> {
        self.interrupt_requested = true;
        let native = self.native.clone();
        match host
            .command("turn/interrupt", json!({"turnId": native}), None)
            .await
        {
            Err(error) if error.fails_host() => {
                let detail = error.detail("turn/interrupt");
                return Some(self.host_failed(host, detail).await);
            }
            _ => {}
        }
        self.interrupt_deadline = Some(Instant::now() + INTERRUPT_TIMEOUT);
        None
    }

    /// The user's answer to an approval, or a steer.
    async fn respond(&mut self, host: &mut Host, response: RuntimeResponse) -> Option<TurnEnd> {
        if let Some(text) = &response.steer {
            let native = self.native.clone();
            let steered = host
                .command(
                    "turn/steer",
                    json!({"expectedTurnId": native, "input": prompt_parts(text, &[])}),
                    None,
                )
                .await;
            if let Err(error) = steered {
                if error.fails_host() {
                    let detail = error.detail("turn/steer");
                    return Some(self.host_failed(host, detail).await);
                }
                tracing::warn!(detail = %error.detail("turn/steer"), "Muse refused a steer");
            }
            return None;
        }
        let approval = self.waiting.get(&response.id)?.approval.clone();
        let decision = response.decision.as_deref().unwrap_or("decline");
        // A decision the card did not offer is refused: nothing is sent, the
        // answer is not delivered, and Muse keeps waiting.
        let Some(choice) = approval_choices(&approval)
            .into_iter()
            .find(|(offered, _)| *offered == decision)
            .map(|(_, choice)| choice)
        else {
            self.sink
                .send(ProviderEvent::RuntimeRequestDeliveryFailed { id: response.id });
            return None;
        };
        let decided = host
            .command(
                "approval/decide",
                json!({"approvalId": approval["approvalId"],
                    "requirementId": approval["currentRequirementId"],
                    "choiceId": choice["choiceId"]}),
                None,
            )
            .await;
        match decided {
            Err(error) if error.fails_host() => {
                let detail = error.detail("approval/decide");
                Some(self.host_failed(host, detail).await)
            }
            Err(error) => {
                tracing::warn!(detail = %error.detail("approval/decide"), "Muse refused a decision");
                self.sink
                    .send(ProviderEvent::RuntimeRequestDeliveryFailed { id: response.id });
                None
            }
            Ok(_) => None,
        }
    }

    /// One notification: `Ok(Some)` when it ends the turn, `Err` with the
    /// text a malformed one fails the host with.
    fn notification(&mut self, method: &str, params: &Value) -> Result<Option<TurnEnd>, String> {
        if !params.is_object() {
            return Err("Invalid Muse protocol response".into());
        }
        if method == "usage/changed" {
            return Ok(None);
        }
        if params["sessionId"].as_str() != Some(self.session.as_str()) {
            return Ok(None);
        }
        if method == "view/gap" {
            return Err("Muse delivery gap requires session recovery".into());
        }
        if method == "session/contextUsage" {
            let used = params["usedTokens"]
                .as_u64()
                .ok_or("Invalid Muse protocol response")?;
            self.context_usage(used, params["windowTokens"].as_u64());
            return Ok(None);
        }
        if let Some(turn) = params["turnId"].as_str() {
            if turn != self.native {
                return Ok(None);
            }
        }
        match method {
            "item/started" | "item/updated" | "item/completed" => {
                let item = &params["item"];
                let (Some(id), Some(revision)) =
                    (item["itemId"].as_str(), item["revision"].as_u64())
                else {
                    return Err("Invalid Muse protocol response".into());
                };
                if item["turnId"]
                    .as_str()
                    .is_some_and(|turn| turn != self.native)
                {
                    return Ok(None);
                }
                if self
                    .items
                    .get(id)
                    .is_some_and(|stored| stored.revision >= revision)
                {
                    return Ok(None);
                }
                if !self.items.contains_key(id) {
                    self.order.push(id.to_owned());
                }
                self.dirty.retain(|dirty| dirty != id);
                self.items.insert(
                    id.to_owned(),
                    Stored {
                        item: item.clone(),
                        revision,
                    },
                );
                self.publish(id, None);
            }
            "item/delta" => {
                let (Some(id), Some(delta)) = (params["itemId"].as_str(), params["delta"].as_str())
                else {
                    return Err("Invalid Muse protocol response".into());
                };
                let Some(stored) = self.items.get_mut(id) else {
                    return Ok(None);
                };
                if !apply_delta(
                    &mut stored.item,
                    params["field"].as_str().unwrap_or("text"),
                    delta,
                ) {
                    return Ok(None);
                }
                if !self.dirty.iter().any(|dirty| dirty == id) {
                    self.dirty.push(id.to_owned());
                }
                if self.flush_at.is_none() {
                    self.flush_at = Some(Instant::now() + STREAM_FLUSH);
                }
            }
            "turn/completed" => {
                if params["turnId"].as_str() != Some(self.native.as_str()) {
                    return Ok(None);
                }
                if params["usage"].is_object() {
                    self.final_usage(&params["usage"]);
                }
                let status = match params["terminal"].as_str() {
                    Some("completed") => TurnStatus::Completed,
                    // Lacking a cancelled status, a cancel Muse made itself
                    // is interrupted too.
                    Some("cancelled") => TurnStatus::Interrupted,
                    _ => TurnStatus::Failed,
                };
                let detail = params["error"]["message"]
                    .as_str()
                    .or(params["reason"].as_str())
                    .map(str::to_owned);
                let quota = params["error"]["quota"].clone();
                return Ok(Some(self.finish_with(status, detail, false, &quota)));
            }
            "approval/requested" | "approval/updated" => self.approval(method, params),
            "approval/resolved" => {
                if let Some(id) = params["approvalId"].as_str() {
                    self.settled.insert(id.to_owned());
                    self.waiting
                        .retain(|_, waiting| waiting.approval["approvalId"] != id);
                }
            }
            "session/tokenUsage" => self.token_usage(params)?,
            _ => {}
        }
        Ok(None)
    }

    /// `approval/requested` opens a request for the user (once per
    /// approval); `approval/updated` moves a waiting one to its next stage.
    fn approval(&mut self, method: &str, params: &Value) {
        let Some(id) = params["approvalId"].as_str() else {
            return;
        };
        if self.settled.contains(id) {
            return;
        }
        if let Some(waiting) = self
            .waiting
            .values_mut()
            .find(|waiting| waiting.approval["approvalId"] == id)
        {
            waiting.approval = params.clone();
            return;
        }
        // A workflow child's approval (no turn of T3's) is not shown yet.
        if method != "approval/requested" || params.get("subagentOrigin").is_some() {
            return;
        }
        let subject = &params["subject"];
        let kind = match subject["kind"].as_str() {
            Some("shell") => "command",
            Some("fileAccess") if subject["access"] == "read" => "file-read",
            Some("fileAccess") => "file-change",
            _ => "permission",
        };
        let mut open = super::requests::open(id, id, kind, &self.runtime_mode);
        let prompt = ["command", "path", "host", "target"]
            .iter()
            .find_map(|key| subject[*key].as_str())
            .or(params["toolName"].as_str())
            .unwrap_or("Muse requests permission");
        open.fields.insert("prompt".into(), json!(prompt));
        open.fields
            .insert("options".into(), Value::Array(approval_options(params)));
        self.waiting.insert(
            open.id.clone(),
            Waiting {
                approval: params.clone(),
            },
        );
        self.sink
            .send(ProviderEvent::RuntimeRequestOpened { request: open });
    }

    /// `session/tokenUsage`: one model step's usage, summed into the turn's.
    fn token_usage(&mut self, params: &Value) -> Result<(), String> {
        let usage = &params["usage"];
        let read = |key: &str| usage[key].as_u64();
        let (Some(prompt), Some(output)) = (params["promptTokens"].as_u64(), read("outputTokens"))
        else {
            return Err("Invalid Muse protocol response".into());
        };
        let cached = read("cacheReadTokens")
            .or(read("cachedTokens"))
            .unwrap_or(0);
        let reasoning = read("reasoningTokens").unwrap_or(0);
        let mut total = self.turn_usage.take().unwrap_or_default();
        let add = |total: &mut Map<String, Value>, key: &str, value: u64| {
            let before = total.get(key).and_then(Value::as_u64).unwrap_or(0);
            total.insert(key.into(), json!(before + value));
        };
        total.insert("usageScope".into(), json!("main_agent"));
        total.insert("usageStatus".into(), json!("partial"));
        total.insert("hasSubagents".into(), json!(self.has_subagents()));
        add(&mut total, "inputTokens", prompt);
        add(&mut total, "outputTokens", output);
        add(&mut total, "cachedInputTokens", cached);
        if read("cacheWriteTokens").is_some() || total.contains_key("cacheCreationTokens") {
            let written = read("cacheWriteTokens").unwrap_or(0);
            let before = total.remove("cacheCreationTokens");
            let before = before.and_then(|value| value.as_u64()).unwrap_or(0);
            total.insert("cacheCreationTokens".into(), json!(before + written));
        }
        add(&mut total, "reasoningTokens", reasoning);
        // Keep the reference's key order: the creation count before reasoning.
        let reasoning_total = total.remove("reasoningTokens");
        if let Some(value) = reasoning_total {
            total.insert("reasoningTokens".into(), value);
        }
        self.sink.send(ProviderEvent::TurnUsage {
            usage: Value::Object(total.clone()),
        });
        self.turn_usage = Some(total);
        if let Some(context) = &self.context {
            let snapshot = TokenUsage {
                used_tokens: context.used_tokens,
                max_tokens: self.token_usage.as_ref().and_then(|usage| usage.max_tokens),
                input_tokens: read("inputTokens").unwrap_or(0),
                cached_input_tokens: cached,
                output_tokens: output,
                reasoning_output_tokens: reasoning,
                ..TokenUsage::default()
            };
            self.token_usage = Some(snapshot.clone());
            self.sink.send(ProviderEvent::Usage { usage: snapshot });
        }
        Ok(())
    }

    /// `session/contextUsage`: the thread's context, and the turn's.
    fn context_usage(&mut self, used: u64, window: Option<u64>) {
        let max = window.or(self.token_usage.as_ref().and_then(|usage| usage.max_tokens));
        let context = ContextUsage {
            used_tokens: used,
            max_tokens: max,
        };
        self.context = Some(context.clone());
        self.sink.send(ProviderEvent::ProviderThread {
            driver: MUSE.into(),
            native_thread_id: self.session.clone(),
            resumed: true,
            model: None,
            opencode2: None,
            native_title: None,
            native_updated_at: None,
            context_usage: Some(Box::new(context)),
            session_started: None,
        });
        let mut usage = self.token_usage.clone().unwrap_or_default();
        usage.used_tokens = used;
        usage.max_tokens = max;
        self.token_usage = Some(usage.clone());
        self.sink.send(ProviderEvent::Usage { usage });
    }

    /// `turn/completed`'s usage: the turn's, complete.
    fn final_usage(&mut self, usage: &Value) {
        let read = |key: &str| usage[key].as_u64();
        let mut total = Map::new();
        total.insert("usageScope".into(), json!("main_agent"));
        total.insert("usageStatus".into(), json!("complete"));
        total.insert("hasSubagents".into(), json!(self.has_subagents()));
        let input = self
            .turn_usage
            .as_ref()
            .and_then(|partial| partial.get("inputTokens").cloned())
            .or_else(|| read("inputTokens").map(Value::from));
        if let Some(input) = input {
            total.insert("inputTokens".into(), input);
        }
        for (key, from) in [
            ("outputTokens", read("outputTokens")),
            (
                "cachedInputTokens",
                read("cacheReadTokens").or(read("cachedTokens")),
            ),
            ("reasoningTokens", read("reasoningTokens")),
            ("cacheCreationTokens", read("cacheWriteTokens")),
        ] {
            if let Some(value) = from {
                total.insert(key.into(), json!(value));
            }
        }
        self.sink.send(ProviderEvent::TurnUsage {
            usage: Value::Object(total.clone()),
        });
        self.turn_usage = Some(total);
    }

    fn has_subagents(&self) -> bool {
        self.items
            .values()
            .any(|stored| stored.item["kind"] == "subagent")
    }

    /// Show the items whose deltas are not shown yet.
    fn flush(&mut self) {
        self.flush_at = None;
        for id in std::mem::take(&mut self.dirty) {
            self.publish(&id, None);
        }
    }

    /// Show the stored item `id` (`publishItem`), with `terminal` as its
    /// status when the turn's end closes it.
    fn publish(&mut self, id: &str, terminal: Option<&str>) {
        let Some(stored) = self.items.get(id) else {
            return;
        };
        let identity = format!(
            "{}:{}:{}:{id}",
            self.instance, self.provider_thread, self.native
        );
        if let Some(item) = present(&identity, &stored.item, terminal) {
            self.sink.send(ProviderEvent::TurnItem { item });
        }
    }

    /// The turn's end (`finish`).
    fn finish(&mut self, status: TurnStatus, detail: Option<String>, broken: bool) -> TurnEnd {
        self.finish_with(status, detail, broken, &Value::Null)
    }

    fn finish_with(
        &mut self,
        status: TurnStatus,
        detail: Option<String>,
        broken: bool,
        quota: &Value,
    ) -> TurnEnd {
        let closing = match status {
            TurnStatus::Completed => "completed",
            TurnStatus::Interrupted => "interrupted",
            _ => "failed",
        };
        // Unshown deltas, and items still running, end with the turn.
        let order = self.order.clone();
        for id in &order {
            let running = self
                .items
                .get(id)
                .is_some_and(|stored| stored.item["status"] == "inProgress");
            if running {
                self.publish(id, Some(closing));
            } else if self.dirty.contains(id) {
                self.publish(id, None);
            }
        }
        self.dirty.clear();
        self.flush_at = None;
        let mut waiting: Vec<String> = self.waiting.drain().map(|(id, _)| id).collect();
        waiting.sort();
        for id in waiting {
            self.sink.send(ProviderEvent::RuntimeRequestClosed {
                id,
                status: "cancelled".into(),
            });
        }
        self.sink.send(ProviderEvent::ProviderTurn {
            native_turn_id: None,
            native_turn_ref: None,
            conversation_head: (status == TurnStatus::Completed).then(|| self.native.clone()),
            status,
            model: None,
            session: Some(Box::new(SessionReport {
                status: if broken {
                    SessionStatus::Error
                } else {
                    SessionStatus::Ready
                },
                model: None,
                last_error: detail
                    .as_ref()
                    .map(|text| failure("provider_error", text).message),
            })),
            history_injected: false,
        });
        match status {
            TurnStatus::Completed => TurnEnd::completed(),
            TurnStatus::Interrupted => TurnEnd::interrupted(self.interrupt_requested),
            _ => {
                let detail = detail.unwrap_or_else(|| "Muse turn failed.".into());
                let limited = !broken && (quota.is_object() || usage_limited(&detail));
                let reset_at = limited
                    .then(|| reset_at(quota, &detail, crate::platform::now_ms()))
                    .flatten();
                let class = if broken {
                    "transport_error"
                } else if limited {
                    "usage_limit"
                } else {
                    "provider_error"
                };
                let mut failure = failure(class, detail);
                failure.reset_at = reset_at;
                // The turn's own failure row (`failure:<turn>`).
                let native = format!("failure:{}", self.native);
                let identity = format!(
                    "{}:{}:{}:{native}",
                    self.instance, self.provider_thread, self.native
                );
                let mut row = ProviderItem::new(identity, "error", "failed")
                    .with(
                        "failure",
                        serde_json::to_value(&failure).unwrap_or_default(),
                    )
                    .with("nativeItemRef", native_ref(&native));
                if limited {
                    row.title = Some("Usage limit reached".into());
                }
                TurnEnd {
                    failure_item: Some(Box::new(row)),
                    ..TurnEnd::failed(
                        failure,
                        if broken {
                            ThreadDisposition::Broken
                        } else {
                            ThreadDisposition::Reusable
                        },
                    )
                }
            }
        }
    }
}

/// `museUsageLimitResetAt` without the account's usage windows: the latest
/// future reset of the refusal's quota or the one its text names.
fn reset_at(quota: &Value, message: &str, now_ms: i64) -> Option<String> {
    const LATEST_DATE_MS: f64 = 8.64e15;
    let named = regex::Regex::new(
        r"(?i)\bresets at (\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2}))",
    )
    .unwrap()
    .captures(message)
    .and_then(|found| {
        time::OffsetDateTime::parse(&found[1], &time::format_description::well_known::Rfc3339).ok()
    })
    .map(|at| (at.unix_timestamp_nanos() / 1_000_000) as f64);
    [quota["resetsAtMs"].as_f64(), named]
        .into_iter()
        .flatten()
        .filter(|ms| ms.is_finite() && *ms > now_ms as f64 && *ms <= LATEST_DATE_MS)
        .reduce(f64::max)
        .map(|ms| crate::auth_service::iso(ms.trunc() as i64))
}

/// `isMuseUsageLimitFailure`: a 429 about a quota or usage limit.
fn usage_limited(detail: &str) -> bool {
    let lower = detail.to_lowercase();
    regex::Regex::new(r"\b429\b").unwrap().is_match(detail)
        && (lower.contains("quota") || lower.contains("usage limit"))
}

fn native_ref(native: &str) -> Value {
    json!({"driver": MUSE, "nativeId": native, "strength": "strong"})
}

/// A delta (`item/delta`) applied to its stored item; whether it changed.
/// Only a running item takes deltas.
fn apply_delta(item: &mut Value, field: &str, delta: &str) -> bool {
    if item["status"] != "inProgress" {
        return false;
    }
    let append = |item: &mut Value, key: &str| {
        let text = format!("{}{delta}", item[key].as_str().unwrap_or_default());
        item[key] = json!(text);
    };
    match field {
        "text" => append(item, "text"),
        "output" | "visibleOutput" => append(item, "visibleOutput"),
        _ => {
            let Some(index) = field
                .strip_prefix("summary.")
                .and_then(|index| index.parse::<usize>().ok())
                .filter(|index| *index < 100)
            else {
                return false;
            };
            let mut summary: Vec<Value> = item["summary"].as_array().cloned().unwrap_or_default();
            while summary.len() <= index {
                summary.push(json!(""));
            }
            let text = format!("{}{delta}", summary[index].as_str().unwrap_or_default());
            summary[index] = json!(text);
            item["summary"] = Value::Array(summary);
        }
    }
    true
}

/// The turn's input parts after its runtime notes: the text, then each
/// image attachment's bytes.
fn prompt_parts(text: &str, attachments: &[Value]) -> Vec<Value> {
    use base64::Engine;
    let mut parts = Vec::new();
    if !text.is_empty() {
        parts.push(json!({"type": "text", "text": text}));
    }
    for attachment in attachments {
        let Some(mime) = attachment["mimeType"].as_str() else {
            continue;
        };
        if !mime.starts_with("image/") {
            continue;
        }
        let Some(bytes) = attachment["path"]
            .as_str()
            .and_then(|path| std::fs::read(path).ok())
        else {
            continue;
        };
        parts.push(json!({"type": "image",
            "base64Data": base64::engine::general_purpose::STANDARD.encode(bytes),
            "mediaType": mime}));
    }
    parts
}

/// `museItemStatus`.
fn item_status(item: &Value) -> &'static str {
    match item["status"].as_str() {
        Some("inProgress") => "running",
        Some("cancelled") => "cancelled",
        Some("completed")
            if !(item["kind"] == "compaction"
                && item["outcome"]
                    .as_str()
                    .is_some_and(|outcome| outcome != "compacted")) =>
        {
            "completed"
        }
        _ => "failed",
    }
}

/// A Muse item as T3 shows it (`publishItem`), or `None` for the items the
/// reference leaves out: user messages, reminder children, and the tools
/// with native rows (unless they failed).
fn present(identity: &str, item: &Value, terminal: Option<&str>) -> Option<ProviderItem> {
    let kind = item["kind"].as_str().unwrap_or_default();
    if matches!(kind, "userMessage" | "reminderChild") {
        return None;
    }
    let tool = item["tool"].as_str();
    if kind == "toolCall"
        && tool.is_some_and(|tool| TOOLS_WITH_NATIVE_ROWS.contains(&tool))
        && item["status"] != "failed"
    {
        return None;
    }
    let status = terminal.unwrap_or_else(|| item_status(item));
    let streaming = status == "running";
    let text = item["text"]
        .as_str()
        .map(str::to_owned)
        .or_else(|| summary_text(item))
        .or_else(|| item["fallbackText"].as_str().map(str::to_owned))
        .unwrap_or_default();
    let id = item["itemId"].as_str().unwrap_or_default();
    let mut out = match kind {
        "agentMessage" => ProviderItem::new(identity, "assistant_message", status)
            .with("text", text)
            .with("streaming", streaming),
        "reasoning" => ProviderItem::new(identity, "reasoning", status)
            .with("text", text)
            .with("streaming", streaming),
        "compaction" => {
            let summary = if text.is_empty() {
                item["reason"].clone()
            } else {
                json!(text)
            };
            ProviderItem::new(identity, "compaction", status)
                .with("driver", MUSE)
                .with("summary", summary)
        }
        _ => tool_presentation(identity, item, status),
    };
    if out.title.is_none()
        && !matches!(
            out.kind.as_str(),
            "command_execution" | "file_change" | "web_search" | "dynamic_tool"
        )
    {
        out.title = tool.map(str::to_owned);
    }
    out.fields.insert("nativeItemRef".into(), native_ref(id));
    Some(out)
}

fn summary_text(item: &Value) -> Option<String> {
    item["summary"].as_array().map(|parts| {
        parts
            .iter()
            .map(|part| part.as_str().unwrap_or_default())
            .collect::<Vec<_>>()
            .join("\n")
    })
}

/// `museToolPresentation`: a command, a file change, a web search, or a
/// dynamic tool.
fn tool_presentation(identity: &str, item: &Value, status: &str) -> ProviderItem {
    let kind = item["kind"].as_str().unwrap_or_default();
    let raw_args = item["args"].as_str();
    // Arguments stream as JSON text, which may not parse yet.
    let args: Option<Map<String, Value>> = raw_args
        .and_then(|args| serde_json::from_str::<Value>(args).ok())
        .and_then(|args| args.as_object().cloned());
    let string_arg = |keys: &[&str]| {
        keys.iter()
            .find_map(|key| args.as_ref()?.get(*key)?.as_str().map(str::to_owned))
    };
    let tool_name = item["tool"].as_str().unwrap_or(kind).to_owned();
    let title = match kind {
        "subagent" => [&item["objective"], &item["role"]]
            .iter()
            .find_map(|value| value.as_str().filter(|text| !text.is_empty()))
            .unwrap_or("Muse agent")
            .to_owned(),
        "workflow" => item["entryId"]
            .as_str()
            .filter(|text| !text.is_empty())
            .unwrap_or("Workflow")
            .to_owned(),
        _ => tool_name.clone(),
    };
    let detail = (status == "failed").then(|| {
        item["failureReason"]
            .as_str()
            .filter(|reason| !reason.is_empty())
            .map(str::to_owned)
            .unwrap_or_else(|| {
                format!(
                    "Muse reported {}.",
                    item["status"].as_str().unwrap_or("undefined")
                )
            })
    });
    let title = match &detail {
        Some(detail) => format!("{title}: {detail}"),
        None => title,
    };
    let text = item["visibleOutput"]
        .as_str()
        .or(item["result"]["summary"].as_str())
        .or(item["text"].as_str())
        .map(str::to_owned)
        .or_else(|| summary_text(item))
        .or_else(|| {
            ["finalSummary", "fallbackText", "objective"]
                .iter()
                .find_map(|key| item[*key].as_str().map(str::to_owned))
        })
        .unwrap_or_default();
    let output = match &detail {
        Some(detail) if text.is_empty() => detail.clone(),
        Some(detail) => format!("{detail}\n{text}"),
        None => text.clone(),
    };
    let shell = regex::Regex::new(r"(?i)shell|bash|exec_command").unwrap();
    if kind == "userShell" || shell.is_match(item["tool"].as_str().unwrap_or_default()) {
        let input = item["commandText"]
            .as_str()
            .map(str::to_owned)
            .or_else(|| string_arg(&["command", "cmd"]))
            .or_else(|| raw_args.map(str::to_owned))
            .unwrap_or_default();
        let mut out = ProviderItem::new(identity, "command_execution", status)
            .titled(title)
            .with("input", input)
            .with("output", output);
        if let Some(code) = item.get("exitCode").filter(|code| !code.is_null()) {
            out = out.with("exitCode", code.clone());
        }
        return out;
    }
    let editing = regex::Regex::new(r"(?i)write|edit|patch").unwrap();
    if kind == "toolCall" && editing.is_match(&tool_name) {
        if let Some(file) = string_arg(&["file_path", "path", "filePath"])
            .map(|file| file.trim().to_owned())
            .filter(|file| !file.is_empty())
        {
            let mut out = ProviderItem::new(identity, "file_change", status)
                .titled(title)
                .with("fileName", file);
            for (key, keys) in [
                (
                    "oldStr",
                    &["old_string", "old_str", "oldStr", "oldText"][..],
                ),
                (
                    "newStr",
                    &["new_string", "new_str", "newStr", "newText", "content"][..],
                ),
                ("diffStr", &["diff", "patch", "diffStr"][..]),
            ] {
                if let Some(value) = string_arg(keys) {
                    out = out.with(key, value);
                }
            }
            return out;
        }
    }
    let web = regex::Regex::new(r"(?i)web|browse|fetch").unwrap();
    if kind == "toolCall" && web.is_match(&tool_name) {
        let query = string_arg(&["query", "url", "pattern"])
            .map(|query| query.trim().to_owned())
            .filter(|query| !query.is_empty());
        let page = string_arg(&["url"])
            .map(|url| url.trim().to_owned())
            .filter(|url| !url.is_empty());
        let hits = (item["tool"] == "web_search" && detail.is_none())
            .then(|| serde_json::from_str::<Value>(&text).ok())
            .flatten()
            .and_then(|parsed| parsed["results"].as_array().cloned())
            .map(|results| {
                results
                    .iter()
                    .map(|hit| {
                        let mut out = Map::new();
                        for key in ["url", "title"] {
                            if let Some(value) =
                                hit[key].as_str().map(str::trim).filter(|v| !v.is_empty())
                            {
                                out.insert(key.into(), json!(value));
                            }
                        }
                        if let Some(snippet) = hit["snippet"].as_str().filter(|s| !s.is_empty()) {
                            out.insert("snippet".into(), json!(snippet));
                        }
                        Value::Object(out)
                    })
                    .collect::<Vec<_>>()
            });
        let results = hits.unwrap_or_else(|| {
            if output.is_empty() {
                Vec::new()
            } else {
                let mut hit = Map::new();
                if let Some(page) = &page {
                    hit.insert("url".into(), json!(page));
                }
                hit.insert("snippet".into(), json!(output));
                vec![Value::Object(hit)]
            }
        });
        let mut out = ProviderItem::new(identity, "web_search", status).titled(title);
        if let Some(query) = query {
            out = out.with("patterns", json!([query]));
        }
        if !results.is_empty() {
            out = out.with("results", Value::Array(results));
        }
        return out;
    }
    let input = match (&args, raw_args) {
        (Some(args), _) => Value::Object(args.clone()),
        (None, Some(raw)) => json!(raw),
        (None, None) => json!(""),
    };
    ProviderItem::new(identity, "dynamic_tool", status)
        .titled(title)
        .with("toolName", tool_name)
        .with("input", input)
        .with("output", output)
}

/// `museApprovalDecision`: the T3 decision a Muse choice stands for.
fn choice_decision(choice: &Value) -> Option<&'static str> {
    match choice["decision"].as_str()? {
        "approved" => Some("accept"),
        "approvedForSession" => Some("acceptForSession"),
        "approvedPolicyAmendment" => match choice["scope"].as_str() {
            Some("session") => Some("acceptForSession"),
            Some("localPersistent") => Some("acceptAlways"),
            _ => None,
        },
        "denied" | "deniedPolicyAmendment" => Some("decline"),
        "abort" => Some("cancel"),
        _ => None,
    }
}

/// `museApprovalChoices`: each decision's lowest-ranked choice, in the order
/// first offered; `decline` falls back to the abort choice (shell approvals
/// offer abort instead of denied).
fn approval_choices(approval: &Value) -> Vec<(&'static str, Value)> {
    let rank = |choice: &Value| {
        let scope = match choice["scope"].as_str() {
            Some("once") => 0,
            Some("session") => 2,
            _ => 4,
        };
        let amendment = choice["decision"]
            .as_str()
            .is_some_and(|decision| decision.ends_with("PolicyAmendment"));
        scope + u8::from(amendment)
    };
    let mut choices: Vec<(&'static str, Value)> = Vec::new();
    for choice in approval["availableChoices"]
        .as_array()
        .into_iter()
        .flatten()
    {
        let Some(decision) = choice_decision(choice) else {
            continue;
        };
        match choices.iter_mut().find(|(known, _)| *known == decision) {
            Some(known) if rank(choice) < rank(&known.1) => known.1 = choice.clone(),
            Some(_) => {}
            None => choices.push((decision, choice.clone())),
        }
    }
    if !choices.iter().any(|(decision, _)| *decision == "decline") {
        if let Some((_, abort)) = choices.iter().find(|(decision, _)| *decision == "cancel") {
            let abort = abort.clone();
            choices.push(("decline", abort));
        }
    }
    choices
}

/// `museApprovalOptions`: the card's options, without `cancel` when it is
/// the same choice as `decline`.
fn approval_options(approval: &Value) -> Vec<Value> {
    let choices = approval_choices(approval);
    let decline = choices
        .iter()
        .find(|(decision, _)| *decision == "decline")
        .map(|(_, choice)| choice["choiceId"].clone());
    choices
        .iter()
        .filter(|(decision, choice)| {
            *decision != "cancel" || Some(&choice["choiceId"]) != decline.as_ref()
        })
        .map(|(decision, choice)| json!({"decision": decision, "label": choice["label"]}))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn shell_approval() -> Value {
        json!({"approvalId": "a1", "currentRequirementId": {"approvalId": "a1", "sourceIndex": 0},
            "subject": {"kind": "shell", "command": "touch x"},
            "availableChoices": [
                {"choiceId": "allow_once", "label": "Allow once", "decision": "approved", "scope": "once"},
                {"choiceId": "allow_local_prefix", "label": "Always allow in this workspace: touch ...",
                 "decision": "approvedPolicyAmendment", "scope": "localPersistent"},
                {"choiceId": "abort", "label": "Reject", "decision": "abort", "scope": "once"}]})
    }

    #[test]
    fn minted_ids_are_uuid_v7_in_order() {
        let mut mint = Mint::default();
        let first = mint.next();
        let second = mint.next();
        let parsed = uuid::Uuid::parse_str(&first).unwrap();
        assert_eq!(parsed.get_version_num(), 7);
        assert_eq!(parsed.get_variant(), uuid::Variant::RFC4122);
        // The same millisecond counts up in the sequence, so ids sort.
        assert!(second > first, "{first} {second}");
    }

    #[test]
    fn a_shell_card_offers_once_always_and_reject() {
        let approval = shell_approval();
        assert_eq!(
            approval_options(&approval),
            vec![
                json!({"decision": "accept", "label": "Allow once"}),
                json!({"decision": "acceptAlways", "label": "Always allow in this workspace: touch ..."}),
                json!({"decision": "decline", "label": "Reject"}),
            ]
        );
        let choices = approval_choices(&approval);
        let choice = |decision: &str| {
            choices
                .iter()
                .find(|(offered, _)| *offered == decision)
                .map(|(_, choice)| choice["choiceId"].clone())
        };
        assert_eq!(choice("decline"), Some(json!("abort")));
        assert_eq!(choice("acceptAlways"), Some(json!("allow_local_prefix")));
        // Not offered: refused rather than downgraded.
        assert_eq!(choice("acceptForSession"), None);
    }

    #[test]
    fn a_lower_ranked_choice_wins_its_decision() {
        let approval = json!({"availableChoices": [
            {"choiceId": "session_rule", "label": "Rule", "decision": "approvedPolicyAmendment", "scope": "session"},
            {"choiceId": "session", "label": "Session", "decision": "approvedForSession", "scope": "session"},
            {"choiceId": "deny", "label": "Deny", "decision": "denied", "scope": "once"},
            {"choiceId": "abort", "label": "Abort", "decision": "abort", "scope": "once"}]});
        assert_eq!(
            approval_options(&approval),
            vec![
                json!({"decision": "acceptForSession", "label": "Session"}),
                json!({"decision": "decline", "label": "Deny"}),
                json!({"decision": "cancel", "label": "Abort"}),
            ]
        );
    }

    #[test]
    fn items_show_as_the_reference_presents_them() {
        let message = json!({"itemId": "m", "kind": "agentMessage", "revision": 1,
            "status": "inProgress", "text": "hi"});
        let item = present("id", &message, None).unwrap();
        assert_eq!(item.kind, "assistant_message");
        assert_eq!(item.status, "running");
        assert_eq!(item.fields["streaming"], true);
        assert_eq!(item.fields["nativeItemRef"]["nativeId"], "m");
        // Housekeeping and native-row tools are left out.
        assert!(present(
            "id",
            &json!({"itemId": "r", "kind": "reminderChild", "status": "completed"}),
            None
        )
        .is_none());
        assert!(present("id", &json!({"itemId": "t", "kind": "toolCall", "tool": "write_todos", "status": "completed"}), None).is_none());
        let reasoning = json!({"itemId": "r", "kind": "reasoning", "status": "completed",
            "summary": ["One", "Two"]});
        assert_eq!(
            present("id", &reasoning, None).unwrap().fields["text"],
            "One\nTwo"
        );
        let bash = json!({"itemId": "b", "kind": "toolCall", "tool": "bash", "status": "completed",
            "args": "{\"command\":\"touch x\"}", "visibleOutput": "{}"});
        let item = present("id", &bash, None).unwrap();
        assert_eq!(item.kind, "command_execution");
        assert_eq!(item.title.as_deref(), Some("bash"));
        assert_eq!(item.fields["input"], "touch x");
        assert_eq!(item.fields["output"], "{}");
        assert!(!item.fields.contains_key("exitCode"));
        let edit = json!({"itemId": "e", "kind": "toolCall", "tool": "edit_file", "status": "failed",
            "args": "{\"path\":\"a.txt\",\"old_string\":\"a\",\"new_string\":\"b\"}"});
        let item = present("id", &edit, None).unwrap();
        assert_eq!(item.kind, "file_change");
        assert_eq!(
            item.title.as_deref(),
            Some("edit_file: Muse reported failed.")
        );
        assert_eq!(
            (item.fields["oldStr"].clone(), item.fields["newStr"].clone()),
            (json!("a"), json!("b"))
        );
        let search = json!({"itemId": "w", "kind": "toolCall", "tool": "web_search", "status": "completed",
            "args": "{\"query\":\"rust\"}", "visibleOutput": "{\"results\":[{\"url\":\" https://a \",\"title\":\"A\"}]}"});
        let item = present("id", &search, None).unwrap();
        assert_eq!(item.fields["patterns"], json!(["rust"]));
        assert_eq!(
            item.fields["results"],
            json!([{"url": "https://a", "title": "A"}])
        );
        let other = json!({"itemId": "o", "kind": "toolCall", "tool": "grep", "status": "inProgress", "args": "{\"pat"});
        let item = present("id", &other, Some("interrupted")).unwrap();
        assert_eq!(
            (item.kind.as_str(), item.status.as_str()),
            ("dynamic_tool", "interrupted")
        );
        assert_eq!(item.fields["input"], "{\"pat");
    }

    #[test]
    fn deltas_grow_a_running_item_only() {
        let mut item = json!({"status": "inProgress", "summary": [""]});
        assert!(apply_delta(&mut item, "text", "a"));
        assert!(apply_delta(&mut item, "text", "b"));
        assert!(apply_delta(&mut item, "summary.1", "s"));
        assert!(apply_delta(&mut item, "output", "o"));
        assert!(!apply_delta(&mut item, "summary.100", "x"));
        assert!(!apply_delta(&mut item, "other", "x"));
        assert_eq!(item["text"], "ab");
        assert_eq!(item["summary"], json!(["", "s"]));
        assert_eq!(item["visibleOutput"], "o");
        let mut done = json!({"status": "completed", "text": "x"});
        assert!(!apply_delta(&mut done, "text", "y"));
    }

    #[test]
    fn errors_and_echoes_are_read_as_the_sdk_reads_them() {
        let ok = json!({"jsonrpc": "2.0", "id": 3, "result": {"commandId": "c", "turnId": "c"}});
        assert!(reply_result(&ok, "turn/start", Some("c")).is_ok());
        assert!(matches!(
            reply_result(&ok, "turn/start", Some("d")),
            Err(MspError::Rpc { .. })
        ));
        let error = json!({"jsonrpc": "2.0", "id": 3, "error": {"code": -32001, "message": "busy",
            "data": {"kind": "overloaded"}}});
        let Err(MspError::Rpc {
            code,
            message,
            kind,
        }) = reply_result(&error, "turn/start", None)
        else {
            panic!("an error");
        };
        assert_eq!(
            (code, message.as_str(), kind.as_str()),
            (-32001, "busy", "overloaded")
        );
        let both = json!({"jsonrpc": "2.0", "id": 3, "result": {}, "error": {}});
        assert!(matches!(
            reply_result(&both, "x", None),
            Err(MspError::Protocol(_))
        ));
        assert_eq!(
            MspError::Rpc {
                code: 1,
                message: "nope".into(),
                kind: "internal".into()
            }
            .detail("turn/start"),
            "Muse turn/start failed: nope"
        );
    }

    #[test]
    fn a_usage_limit_resets_at_its_latest_future_reset() {
        let now = 1_800_000_000_000;
        assert_eq!(
            reset_at(&json!({"resetsAtMs": now + 60_000}), "429 usage limit", now).as_deref(),
            Some("2027-01-15T08:01:00.000Z")
        );
        assert_eq!(
            reset_at(
                &json!({"resetsAtMs": now + 60_000}),
                "429 quota; resets at 2027-01-15T09:00:00Z",
                now
            )
            .as_deref(),
            Some("2027-01-15T09:00:00.000Z")
        );
        assert_eq!(
            reset_at(&json!({"resetsAtMs": now - 1}), "429 quota", now),
            None
        );
    }

    #[test]
    fn usage_limits_are_429s_about_quota() {
        assert!(usage_limited("HTTP 429: quota exhausted"));
        assert!(usage_limited("429 Usage limit reached"));
        assert!(!usage_limited("4290 quota"));
        assert!(!usage_limited("429 too many requests"));
    }

    #[test]
    fn the_launch_follows_the_runtime_mode() {
        assert_eq!(
            serve_args("full-access"),
            ["serve", "--trust-workspace", "--disable-sandbox"]
        );
        assert_eq!(
            serve_args("approval-required"),
            ["serve", "--trust-workspace"]
        );
        assert_eq!(
            (approval_mode("auto"), permission_profile("auto")),
            ("promptUnmatched", ":ask-me")
        );
        assert_eq!(
            (
                approval_mode("full-access"),
                permission_profile("full-access")
            ),
            ("allowAll", ":unrestricted")
        );
    }
}
