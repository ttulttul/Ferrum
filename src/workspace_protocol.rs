//! The workspace protocol: how a server reaches a [`Workspace`] that runs in
//! another process (`t3-agentd`) over one WebSocket.
//!
//! # Frames
//!
//! The server sends JSON text frames `{"id", "op", "args"}`. agentd answers
//! each request that has an `id` with `{"id", "ok"}` or
//! `{"id", "err": {"code", "message", "detail"?}}`. A request without an `id`
//! (only `ack`) gets no answer.
//!
//! Long-lived work (turns, terminal and device events, polled VCS and local
//! server state) answers with `{"stream": <id>}` and then pushes
//! `{"stream", "seq", "event"}` frames. A stream's last frame carries `end`
//! instead of `event`. Sequence numbers start at 1 and rise by one per frame.
//!
//! Any frame may instead travel as a binary frame that moves one long string
//! out of the JSON (see [`encode_frame`]), so bulk text such as file contents
//! and terminal output is not escaped.
//!
//! # Delivery
//!
//! agentd keeps every stream frame until the server acknowledges it with
//! `ack {stream, seq}`. After a reconnect the server sends `hello` and then
//! `resume {streams: {id: lastSeq}}`, and agentd replays the frames after
//! each `lastSeq`. Streams the server does not resume are closed. Work keeps
//! running while no server is connected. When a stream's buffer fills, the
//! stream ends with [`EndReason::Gap`]; a turn's process is stopped and the
//! server reports the turn as interrupted.
//!
//! # Epochs
//!
//! `hello` carries the server's epoch, which rises each time a server adopts
//! the agentd. agentd remembers the highest epoch it has seen and refuses
//! requests on connections that said hello with a lower one.
//!
//! [`Workspace`]: crate::workspace::Workspace
use crate::workspace::{Reply, TurnEvent, TurnExit};
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use std::collections::BTreeMap;

/// The protocol version `hello` exchanges.
pub const VERSION: u32 = 1;

/// How often agentd re-reads polled state (VCS status and branch, local
/// servers) for a watch stream.
pub const POLL_INTERVAL_MS: u64 = 2_000;

/// The environment variable and the file agentd and the server read the
/// shared bearer token from.
pub const TOKEN_ENV: &str = "T3_AGENTD_TOKEN";
pub const TOKEN_FILE_ENV: &str = "T3_AGENTD_TOKEN_FILE";

/// Operation names. Each group mirrors a group of [`Workspace`] methods.
///
/// [`Workspace`]: crate::workspace::Workspace
pub mod op {
    // Session.
    pub const HELLO: &str = "hello";
    pub const PING: &str = "ping";
    pub const ACK: &str = "ack";
    pub const RESUME: &str = "resume";
    pub const IDLE_STATE: &str = "idle.state";
    /// Stop a stream: unsubscribe from events or stop polling. A turn's
    /// process keeps running; use [`TURN_CANCEL`] to stop it.
    pub const STREAM_CLOSE: &str = "stream.close";

    // fs.*: the method's RPC input is the args.
    pub const FS_LIST_ENTRIES: &str = "fs.listEntries";
    pub const FS_LIST_IGNORED_FILES: &str = "fs.listIgnoredFiles";
    pub const FS_SEARCH_ENTRIES: &str = "fs.searchEntries";
    pub const FS_SEARCH_CONTENTS: &str = "fs.searchContents";
    /// Prepare a grant at the workspace, open its HTTP response, and pull its body.
    /// Response bodies belong to the connection that opened them.
    pub const ASSET_PREPARE: &str = "asset.prepare";
    pub const ASSET_OPEN: &str = "asset.open";
    pub const ASSET_READ: &str = "asset.read";
    pub const ASSET_CLOSE: &str = "asset.close";
    pub const FS_READ_FILE: &str = "fs.readFile";
    pub const FS_WRITE_FILE: &str = "fs.writeFile";
    pub const FS_METADATA: &str = "fs.metadata";
    pub const FS_BROWSE: &str = "fs.browse";
    /// `{path}`; answers `null` or an `io` error.
    pub const FS_CREATE_DIR_ALL: &str = "fs.createDirAll";
    /// `{path}`; answers the canonical path or an `io` error.
    pub const FS_CANONICALIZE: &str = "fs.canonicalize";
    /// `{path}`; answers a boolean.
    pub const FS_IS_DIR: &str = "fs.isDir";
    /// A project started from its title (`rootPath`, `name`): a new folder
    /// under `rootPath` holding a repository with a first commit. Answers a
    /// [`crate::workspace::NamedProject`].
    pub const FS_CREATE_NAMED_PROJECT: &str = "fs.createNamedProject";

    // git.*
    /// `{method, input, fetchUpstream}`: a `vcs.*` method.
    pub const GIT_VCS: &str = "git.vcs";
    /// `{method, input}`: a `review.*` method.
    pub const GIT_REVIEW: &str = "git.review";
    /// `{cwd}`; answers a tree id or `null`.
    pub const GIT_CAPTURE_TREE: &str = "git.captureTree";
    /// `{method, input, tools}`: `git.resolvePullRequest` or
    /// `git.preparePullRequestThread`. `tools` is a [`super::WireGitTools`].
    pub const GIT_PULL_REQUEST: &str = "git.pullRequest";
    /// `{input, tools}`: `git.runStackedAction`, as a stream of
    /// `{"progress": batch}` events and a last `{"reply": Polled}` event.
    pub const GIT_STACKED_ACTION: &str = "git.stackedAction";

    // term.*
    /// `{method, input}`: a `terminal.*` method.
    pub const TERM_CALL: &str = "term.call";
    /// Stream: every terminal's events.
    pub const TERM_EVENTS: &str = "term.events";
    /// Stream: the metadata snapshot, then its changes.
    pub const TERM_METADATA: &str = "term.metadata";

    // turn.* and proc.*
    /// [`super::TurnStart`]; answers a stream of [`super::WireTurnEvent`].
    pub const TURN_START: &str = "turn.start";
    /// `{stream}`: stop a turn's process. Its stream ends after `exited`.
    pub const TURN_CANCEL: &str = "turn.cancel";
    /// [`super::TurnRespond`]: answer a runtime request of a running turn.
    /// Answers `true` when a turn waited for it.
    pub const TURN_RESPOND: &str = "turn.respond";
    /// `{input}`: `preview.stopLocalServer`.
    pub const PROC_STOP_LOCAL_SERVER: &str = "proc.stopLocalServer";
    /// The models a provider's CLI lists, run as a turn of the given spec
    /// would run it (`TurnStart`); answers the CLI's raw model entries.
    pub const PROVIDER_MODELS: &str = "provider.models";

    // watch.*: streams agentd feeds by polling every POLL_INTERVAL_MS. Each
    // event is a [`super::Polled`] and is sent only when it changed.
    /// `{input, fetchUpstream}`: `vcs.refreshStatus`.
    pub const WATCH_VCS_STATUS: &str = "watch.vcsStatus";
    /// `{input}`: the branch `subscribeVcsBranch` reports.
    pub const WATCH_VCS_BRANCH: &str = "watch.vcsBranch";
    /// `{input}`: the servers `subscribeDiscoveredLocalServers` reports.
    pub const WATCH_LOCAL_SERVERS: &str = "watch.localServers";

    // preview.*
    /// `{method, input}`: a `preview.*` tab method.
    pub const PREVIEW_CALL: &str = "preview.call";
    /// `{method, input}`: `preview.reportProfiles` or `preview.clearProfile`.
    pub const PREVIEW_PROFILE: &str = "preview.profile";
    /// `{thread, tab, profile}`, each optional.
    pub const PREVIEW_CLOSE_PAGES: &str = "preview.closePages";
    /// Stream: preview events.
    pub const PREVIEW_EVENTS: &str = "preview.events";

    // device.*
    /// `{method, input}`: a `device.*` method.
    pub const DEVICE_CALL: &str = "device.call";
    /// Stream: the device state, then each new state.
    pub const DEVICE_EVENTS: &str = "device.events";
}

/// A request from the server.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Request {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<u64>,
    pub op: String,
    #[serde(default)]
    pub args: Value,
}

/// agentd's answer to a request.
#[derive(Clone, Debug, PartialEq)]
pub struct Response {
    pub id: u64,
    pub result: Result<Value, WireError>,
}

/// One frame of a stream.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct StreamFrame {
    pub stream: u64,
    pub seq: u64,
    #[serde(flatten)]
    pub body: StreamBody,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum StreamBody {
    Event(Value),
    End(StreamEnd),
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct StreamEnd {
    pub reason: EndReason,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<WireError>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum EndReason {
    /// The work finished (a turn exited).
    Done,
    /// The server closed the stream.
    Closed,
    /// The stream's buffer overflowed; frames after the last one were lost.
    Gap,
    /// The stream failed; `error` says why.
    Error,
}

/// A frame agentd sends.
#[derive(Clone, Debug, PartialEq)]
pub enum Inbound {
    Response(Response),
    Stream(StreamFrame),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorCode {
    /// The operation's RPC failed; `detail` is the RPC error value, as the
    /// in-process workspace returns it.
    Rpc,
    /// The operation does not know the method (the in-process workspace
    /// answers `None`).
    UnknownMethod,
    UnknownOp,
    UnknownStream,
    BadRequest,
    /// A filesystem call failed; `detail` is `{"kind": <io::ErrorKind>}`.
    Io,
    Unauthorized,
    /// The connection's epoch is older than one agentd has seen.
    StaleEpoch,
    /// The first request on a connection must be `hello`.
    HelloRequired,
    VersionMismatch,
    /// The connection to agentd is down (reported by the client).
    Unavailable,
    /// A path the operation names resolves outside the directories agentd
    /// serves (its root and the worktrees it creates).
    OutsideRoot,
    Internal,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct WireError {
    pub code: ErrorCode,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<Value>,
}

impl WireError {
    pub fn new(code: ErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            detail: None,
        }
    }
    pub fn with_detail(mut self, detail: Value) -> Self {
        self.detail = Some(detail);
        self
    }
    pub fn rpc(error: Value) -> Self {
        Self::new(ErrorCode::Rpc, "the operation failed").with_detail(error)
    }
}

impl std::fmt::Display for WireError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}: {}", self.code, self.message)
    }
}

impl std::error::Error for WireError {}

// ---- Arguments and answers -------------------------------------------------

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Hello {
    pub version: u32,
    pub epoch: u64,
    /// The bearer token, for transports that cannot set an `Authorization`
    /// header on the upgrade request.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HelloReply {
    pub version: u32,
    /// The highest epoch agentd has seen, which is this connection's.
    pub epoch: u64,
    /// The directory agentd serves projects under.
    pub root: String,
    /// Streams still open, which a reconnecting server may resume.
    pub streams: Vec<StreamInfo>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StreamInfo {
    pub stream: u64,
    pub op: String,
    /// The last sequence number agentd has produced.
    pub last_seq: u64,
    /// The last sequence number the server has acknowledged.
    pub acked_seq: u64,
    pub ended: bool,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Resume {
    /// The last sequence number the server has recorded for each stream it
    /// keeps.
    pub streams: BTreeMap<u64, u64>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResumeReply {
    /// Streams agentd no longer has; the server must treat them as lost.
    pub missing: Vec<u64>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Ack {
    pub stream: u64,
    pub seq: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamRef {
    pub stream: u64,
}

/// What agentd is doing, so a supervisor can tell when it is idle.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct IdleState {
    /// Turns whose process has not exited.
    pub running_turns: u64,
    /// Terminals whose process is running.
    pub running_terminals: u64,
    /// Child processes agentd runs: turns and terminals.
    pub running_processes: u64,
    /// Open streams, including watches.
    pub open_streams: u64,
    /// Stream frames produced but not yet acknowledged.
    pub unacked_frames: u64,
    pub connected: bool,
    /// Milliseconds since the last request or stream frame.
    pub idle_ms: u64,
}

/// The programs git workflows run besides git, as [`GitTools`] names them.
///
/// [`GitTools`]: crate::workspace::GitTools
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WireGitTools {
    pub gh: String,
    #[serde(default)]
    pub text_generation: Vec<String>,
}

impl From<&crate::workspace::GitTools> for WireGitTools {
    fn from(tools: &crate::workspace::GitTools) -> Self {
        Self {
            gh: tools.gh.to_string_lossy().into_owned(),
            text_generation: tools.text_generation.clone(),
        }
    }
}

impl From<WireGitTools> for crate::workspace::GitTools {
    fn from(tools: WireGitTools) -> Self {
        Self {
            gh: tools.gh.into(),
            text_generation: tools.text_generation,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TurnStart {
    #[serde(default)]
    pub command: Vec<String>,
    pub prompt: String,
    pub cwd: String,
    #[serde(default)]
    pub env_remove: Vec<String>,
    /// `[name, value]` pairs.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub env: Vec<(String, String)>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider: Option<crate::workspace::ProviderTurn>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub turn_id: String,
}

/// `turn.respond`'s arguments.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TurnRespond {
    pub turn_id: String,
    pub response: crate::workspace::provider::RuntimeResponse,
}

impl From<crate::workspace::TurnSpec> for TurnStart {
    fn from(spec: crate::workspace::TurnSpec) -> Self {
        Self {
            command: spec.command,
            prompt: spec.prompt,
            cwd: spec.cwd,
            env_remove: spec.env_remove,
            env: spec.env,
            provider: spec.provider,
            turn_id: spec.turn_id,
        }
    }
}

impl From<TurnStart> for crate::workspace::TurnSpec {
    fn from(start: TurnStart) -> Self {
        Self {
            command: start.command,
            prompt: start.prompt,
            cwd: start.cwd,
            env_remove: start.env_remove,
            env: start.env,
            provider: start.provider,
            turn_id: start.turn_id,
        }
    }
}

/// A turn stream's events.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum WireTurnEvent {
    Stdout {
        text: String,
    },
    Stderr {
        text: String,
    },
    Exited {
        success: bool,
        cancelled: bool,
        status: String,
    },
    /// A provider turn's event: `{type: "provider", event: {type: ...}}`.
    Provider {
        event: crate::workspace::ProviderEvent,
    },
}

impl From<TurnEvent> for WireTurnEvent {
    fn from(event: TurnEvent) -> Self {
        match event {
            TurnEvent::Stdout(text) => Self::Stdout { text },
            TurnEvent::Stderr(text) => Self::Stderr { text },
            TurnEvent::Provider(event) => Self::Provider { event },
            TurnEvent::Exited(exit) => Self::Exited {
                success: exit.success,
                cancelled: exit.cancelled,
                status: exit.status,
            },
        }
    }
}

impl From<WireTurnEvent> for TurnEvent {
    fn from(event: WireTurnEvent) -> Self {
        match event {
            WireTurnEvent::Stdout { text } => Self::Stdout(text),
            WireTurnEvent::Stderr { text } => Self::Stderr(text),
            WireTurnEvent::Provider { event } => Self::Provider(event),
            WireTurnEvent::Exited {
                success,
                cancelled,
                status,
            } => Self::Exited(TurnExit {
                success,
                cancelled,
                status,
            }),
        }
    }
}

/// How a turn whose stream lost frames is reported.
pub fn interrupted_turn() -> TurnEvent {
    TurnEvent::Exited(TurnExit {
        success: false,
        cancelled: false,
        status: "interrupted: the workspace dropped the turn's output".into(),
    })
}

/// One reading of polled state: what the in-process call would have
/// returned.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum Polled {
    Ok(Value),
    Err(Value),
    /// The workspace does not know the method.
    Unknown,
}

impl Polled {
    pub fn from_routed(reply: Option<Reply>) -> Self {
        match reply {
            Some(Ok(value)) => Self::Ok(value),
            Some(Err(error)) => Self::Err(error),
            None => Self::Unknown,
        }
    }
    pub fn into_routed(self) -> Option<Reply> {
        match self {
            Self::Ok(value) => Some(Ok(value)),
            Self::Err(error) => Some(Err(error)),
            Self::Unknown => None,
        }
    }
}

// ---- Reply and error mapping ----------------------------------------------

/// Encode a routed reply (`None` when the method is unknown).
pub fn encode_routed(reply: Option<Reply>) -> Result<Value, WireError> {
    match reply {
        Some(reply) => encode_reply(reply),
        None => Err(WireError::new(
            ErrorCode::UnknownMethod,
            "the workspace does not know this method",
        )),
    }
}

/// Decode a routed reply. Errors that are not about the method become the
/// RPC defect `unavailable` gives.
pub fn decode_routed(result: Result<Value, WireError>, method: &str) -> Option<Reply> {
    match result {
        Err(error) if error.code == ErrorCode::UnknownMethod => None,
        result => Some(decode_reply(result, method)),
    }
}

pub fn encode_reply(reply: Reply) -> Result<Value, WireError> {
    reply.map_err(WireError::rpc)
}

pub fn decode_reply(result: Result<Value, WireError>, method: &str) -> Reply {
    result.map_err(|error| match error {
        WireError {
            code: ErrorCode::Rpc,
            detail: Some(detail),
            ..
        } => detail,
        error => defect(method, &error),
    })
}

/// The RPC error a transport or protocol failure becomes: a defect naming
/// the method.
pub fn defect(method: &str, error: &WireError) -> Value {
    json!({
        "_tag": "Die",
        "defect": format!("The workspace could not run {method}: {}", error.message),
    })
}

pub fn io_error(error: &std::io::Error) -> WireError {
    WireError::new(ErrorCode::Io, error.to_string())
        .with_detail(json!({"kind": io_kind_name(error.kind())}))
}

pub fn decode_io_error(error: WireError) -> std::io::Error {
    let kind = error
        .detail
        .as_ref()
        .and_then(|d| d["kind"].as_str())
        .map_or(std::io::ErrorKind::Other, io_kind);
    std::io::Error::new(kind, error.message)
}

const IO_KINDS: [(std::io::ErrorKind, &str); 9] = [
    (std::io::ErrorKind::NotFound, "notFound"),
    (std::io::ErrorKind::PermissionDenied, "permissionDenied"),
    (std::io::ErrorKind::AlreadyExists, "alreadyExists"),
    (std::io::ErrorKind::InvalidInput, "invalidInput"),
    (std::io::ErrorKind::InvalidData, "invalidData"),
    (std::io::ErrorKind::TimedOut, "timedOut"),
    (std::io::ErrorKind::Unsupported, "unsupported"),
    (std::io::ErrorKind::NotConnected, "notConnected"),
    (std::io::ErrorKind::Interrupted, "interrupted"),
];

fn io_kind_name(kind: std::io::ErrorKind) -> &'static str {
    IO_KINDS
        .iter()
        .find(|(k, _)| *k == kind)
        .map_or("other", |(_, name)| name)
}

fn io_kind(name: &str) -> std::io::ErrorKind {
    IO_KINDS
        .iter()
        .find(|(_, n)| *n == name)
        .map_or(std::io::ErrorKind::Other, |(k, _)| *k)
}

// ---- Frame encoding --------------------------------------------------------

/// A WebSocket message.
#[derive(Clone, Debug, PartialEq)]
pub enum Frame {
    Text(String),
    Binary(Vec<u8>),
}

/// The first byte of a binary frame.
pub const BINARY_MAGIC: u8 = 0xB1;

/// Strings at least this long travel in a binary frame's payload.
pub const BULK_THRESHOLD: usize = 16 * 1024;

impl Response {
    pub fn to_value(&self) -> Value {
        match &self.result {
            Ok(ok) => json!({"id": self.id, "ok": ok}),
            Err(err) => json!({"id": self.id, "err": err}),
        }
    }
}

impl Inbound {
    pub fn to_value(&self) -> Value {
        match self {
            Self::Response(response) => response.to_value(),
            Self::Stream(frame) => serde_json::to_value(frame).expect("stream frames serialize"),
        }
    }

    pub fn from_value(value: Value) -> Result<Self, String> {
        if value.get("stream").is_some() && value.get("seq").is_some() {
            return serde_json::from_value(value)
                .map(Self::Stream)
                .map_err(|e| e.to_string());
        }
        let id = value["id"].as_u64().ok_or("a reply has no id")?;
        let result = if let Some(err) = value.get("err") {
            Err(serde_json::from_value(err.clone()).map_err(|e| e.to_string())?)
        } else if let Some(ok) = value.get("ok") {
            Ok(ok.clone())
        } else {
            return Err("a reply has neither ok nor err".into());
        };
        Ok(Self::Response(Response { id, result }))
    }
}

/// Encode a message. When it holds a string of at least `threshold` bytes,
/// the longest such string moves into a binary frame's payload:
/// `[BINARY_MAGIC][header length: u32 BE][header JSON][payload]`, where the
/// header is `{"msg": <message with the string set to null>, "at": <JSON
/// pointer>}` and the payload is the string's UTF-8 bytes.
pub fn encode_frame(message: &Value, threshold: usize) -> Frame {
    let mut longest: Option<(String, usize)> = None;
    find_longest(message, &mut String::new(), threshold, &mut longest);
    let Some((at, _)) = longest else {
        return Frame::Text(message.to_string());
    };
    let mut header_msg = message.clone();
    let slot = header_msg.pointer_mut(&at).expect("the pointer was found");
    let Value::String(payload) = std::mem::take(slot) else {
        unreachable!("the pointer names a string")
    };
    let header = json!({"msg": header_msg, "at": at}).to_string();
    let mut bytes = Vec::with_capacity(5 + header.len() + payload.len());
    bytes.push(BINARY_MAGIC);
    bytes.extend_from_slice(&(header.len() as u32).to_be_bytes());
    bytes.extend_from_slice(header.as_bytes());
    bytes.extend_from_slice(payload.as_bytes());
    Frame::Binary(bytes)
}

fn find_longest(
    value: &Value,
    path: &mut String,
    threshold: usize,
    longest: &mut Option<(String, usize)>,
) {
    match value {
        Value::String(s)
            if s.len() >= threshold && longest.as_ref().is_none_or(|(_, len)| s.len() > *len) =>
        {
            *longest = Some((path.clone(), s.len()));
        }
        Value::Array(items) => {
            for (i, item) in items.iter().enumerate() {
                let len = path.len();
                path.push_str(&format!("/{i}"));
                find_longest(item, path, threshold, longest);
                path.truncate(len);
            }
        }
        Value::Object(map) => {
            for (key, item) in map {
                let len = path.len();
                path.push('/');
                path.push_str(&key.replace('~', "~0").replace('/', "~1"));
                find_longest(item, path, threshold, longest);
                path.truncate(len);
            }
        }
        _ => {}
    }
}

/// Decode a text or binary frame into its JSON message.
pub fn decode_frame(frame: &Frame) -> Result<Value, String> {
    match frame {
        Frame::Text(text) => serde_json::from_str(text).map_err(|e| e.to_string()),
        Frame::Binary(bytes) => {
            if bytes.len() < 5 || bytes[0] != BINARY_MAGIC {
                return Err("not a workspace binary frame".into());
            }
            let len = u32::from_be_bytes([bytes[1], bytes[2], bytes[3], bytes[4]]) as usize;
            let header = bytes
                .get(5..5 + len)
                .ok_or("the binary frame's header is truncated")?;
            let mut header: Map<String, Value> =
                serde_json::from_slice(header).map_err(|e| e.to_string())?;
            let at = header
                .remove("at")
                .and_then(|a| a.as_str().map(str::to_owned))
                .ok_or("the binary frame's header has no pointer")?;
            let mut message = header
                .remove("msg")
                .ok_or("the binary frame's header has no message")?;
            let payload = std::str::from_utf8(&bytes[5 + len..])
                .map_err(|_| "the binary frame's payload is not UTF-8")?;
            let slot = message
                .pointer_mut(&at)
                .ok_or("the binary frame's pointer names nothing")?;
            *slot = Value::String(payload.to_owned());
            Ok(message)
        }
    }
}

/// Encode a request as a frame.
pub fn request_frame(request: &Request) -> Frame {
    encode_frame(
        &serde_json::to_value(request).expect("requests serialize"),
        BULK_THRESHOLD,
    )
}

/// Encode agentd's frame.
pub fn inbound_frame(inbound: &Inbound) -> Frame {
    encode_frame(&inbound.to_value(), BULK_THRESHOLD)
}

pub fn decode_request(frame: &Frame) -> Result<Request, String> {
    serde_json::from_value(decode_frame(frame)?).map_err(|e| e.to_string())
}

pub fn decode_inbound(frame: &Frame) -> Result<Inbound, String> {
    Inbound::from_value(decode_frame(frame)?)
}

/// Compare two tokens in time that does not depend on where they differ.
pub fn token_matches(expected: &str, given: &str) -> bool {
    let (a, b) = (expected.as_bytes(), given.as_bytes());
    let mut diff = a.len() ^ b.len();
    for i in 0..a.len().max(b.len()) {
        diff |= usize::from(a.get(i).copied().unwrap_or(0) ^ b.get(i).copied().unwrap_or(0));
    }
    diff == 0 && !a.is_empty()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn round_trip_inbound(inbound: Inbound) {
        for threshold in [BULK_THRESHOLD, 1] {
            let frame = encode_frame(&inbound.to_value(), threshold);
            assert_eq!(decode_inbound(&frame).unwrap(), inbound);
        }
    }

    #[test]
    fn requests_round_trip() {
        let request = Request {
            id: Some(7),
            op: op::FS_WRITE_FILE.into(),
            args: json!({"cwd":"/w","relativePath":"a.txt","contents":"x".repeat(40_000)}),
        };
        let frame = request_frame(&request);
        assert!(matches!(frame, Frame::Binary(_)));
        assert_eq!(decode_request(&frame).unwrap(), request);

        let ack = Request {
            id: None,
            op: op::ACK.into(),
            args: serde_json::to_value(Ack { stream: 3, seq: 9 }).unwrap(),
        };
        let Frame::Text(text) = request_frame(&ack) else {
            panic!("a small request is text")
        };
        assert_eq!(text, r#"{"op":"ack","args":{"stream":3,"seq":9}}"#);
        assert_eq!(decode_request(&Frame::Text(text)).unwrap(), ack);
        assert_eq!(
            decode_request(&Frame::Text(r#"{"id":1,"op":"ping"}"#.into())).unwrap(),
            Request {
                id: Some(1),
                op: "ping".into(),
                args: Value::Null
            }
        );
    }

    #[test]
    fn replies_and_stream_frames_round_trip() {
        round_trip_inbound(Inbound::Response(Response {
            id: 1,
            result: Ok(json!({"contents":"line\n".repeat(10_000),"relativePath":"a/b~c"})),
        }));
        round_trip_inbound(Inbound::Response(Response {
            id: 2,
            result: Err(WireError::rpc(json!({"_tag":"ProjectReadFileError"}))),
        }));
        round_trip_inbound(Inbound::Stream(StreamFrame {
            stream: 4,
            seq: 1,
            body: StreamBody::Event(
                serde_json::to_value(WireTurnEvent::Stdout {
                    text: "hi\n".into(),
                })
                .unwrap(),
            ),
        }));
        round_trip_inbound(Inbound::Stream(StreamFrame {
            stream: 4,
            seq: 2,
            body: StreamBody::End(StreamEnd {
                reason: EndReason::Gap,
                error: None,
            }),
        }));
        assert_eq!(
            Inbound::Stream(StreamFrame {
                stream: 4,
                seq: 2,
                body: StreamBody::End(StreamEnd {
                    reason: EndReason::Done,
                    error: None
                }),
            })
            .to_value(),
            json!({"stream":4,"seq":2,"end":{"reason":"done"}})
        );
    }

    #[test]
    fn binary_frames_reject_garbage() {
        assert!(decode_frame(&Frame::Binary(vec![1, 2, 3])).is_err());
        assert!(decode_frame(&Frame::Binary(vec![BINARY_MAGIC, 0, 0, 0, 99])).is_err());
        let mut bytes = vec![BINARY_MAGIC];
        let header = br#"{"msg":{"a":null},"at":"/b"}"#;
        bytes.extend_from_slice(&(header.len() as u32).to_be_bytes());
        bytes.extend_from_slice(header);
        assert!(decode_frame(&Frame::Binary(bytes)).is_err());
    }

    #[test]
    fn replies_map_like_the_in_process_workspace() {
        let error = json!({"_tag":"VcsError","message":"no"});
        for reply in [None, Some(Ok(json!({"ok":true}))), Some(Err(error.clone()))] {
            let wire = encode_routed(reply.clone());
            let value = Inbound::Response(Response {
                id: 1,
                result: wire,
            });
            let Inbound::Response(back) = decode_inbound(&inbound_frame(&value)).unwrap() else {
                unreachable!()
            };
            assert_eq!(decode_routed(back.result, "vcs.status"), reply);
        }
        let lost = decode_reply(
            Err(WireError::new(ErrorCode::Unavailable, "not connected")),
            "projects.readFile",
        )
        .unwrap_err();
        assert_eq!(lost["_tag"], "Die");
        assert_eq!(
            lost["defect"],
            "The workspace could not run projects.readFile: not connected"
        );
    }

    #[test]
    fn io_errors_keep_their_kind() {
        for kind in [
            std::io::ErrorKind::NotFound,
            std::io::ErrorKind::PermissionDenied,
            std::io::ErrorKind::Other,
        ] {
            let wire = io_error(&std::io::Error::new(kind, "boom"));
            let back: WireError =
                serde_json::from_value(serde_json::to_value(&wire).unwrap()).unwrap();
            let error = decode_io_error(back);
            assert_eq!(error.kind(), kind);
            assert_eq!(error.to_string(), "boom");
        }
    }

    #[test]
    fn session_messages_use_camel_case() {
        let hello = HelloReply {
            version: VERSION,
            epoch: 3,
            root: "/w".into(),
            streams: vec![StreamInfo {
                stream: 1,
                op: op::TURN_START.into(),
                last_seq: 5,
                acked_seq: 2,
                ended: false,
            }],
        };
        let value = serde_json::to_value(&hello).unwrap();
        assert_eq!(value["streams"][0]["lastSeq"], 5);
        assert_eq!(serde_json::from_value::<HelloReply>(value).unwrap(), hello);
        let resume: Resume = serde_json::from_value(json!({"streams":{"1":5,"9":0}})).unwrap();
        assert_eq!(resume.streams.get(&1), Some(&5));
        assert_eq!(
            serde_json::to_value(IdleState::default()).unwrap()["runningTurns"],
            0
        );
        assert_eq!(
            serde_json::to_value(Polled::from_routed(None)).unwrap(),
            json!("unknown")
        );
        assert_eq!(
            serde_json::from_value::<Polled>(json!({"ok":{"a":1}}))
                .unwrap()
                .into_routed(),
            Some(Ok(json!({"a":1})))
        );
    }

    #[test]
    fn turn_events_round_trip() {
        for event in [
            TurnEvent::Stdout("a\n".into()),
            TurnEvent::Stderr("b\n".into()),
            TurnEvent::Provider(crate::workspace::ProviderEvent::TurnItemDelta {
                item_id: "msg_1".into(),
                delta: "Hel".into(),
            }),
            TurnEvent::Exited(TurnExit {
                success: true,
                cancelled: false,
                status: "exit status: 0".into(),
            }),
        ] {
            let wire = serde_json::to_value(WireTurnEvent::from(event.clone())).unwrap();
            let back: WireTurnEvent = serde_json::from_value(wire).unwrap();
            assert_eq!(TurnEvent::from(back), event);
        }
        assert_eq!(
            serde_json::to_value(WireTurnEvent::Stdout { text: "x".into() }).unwrap(),
            json!({"type":"stdout","text":"x"})
        );
    }

    #[test]
    fn tokens_compare_exactly() {
        assert!(token_matches("secret", "secret"));
        assert!(!token_matches("secret", "secreT"));
        assert!(!token_matches("secret", "secret1"));
        assert!(!token_matches("secret", ""));
        assert!(!token_matches("", ""));
    }
}
