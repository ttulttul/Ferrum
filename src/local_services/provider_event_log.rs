//! Per-thread provider event logs, as the reference's `EventNdjsonLogger`
//! (`apps/server/src/provider/EventNdjsonLogger.ts`) writes them: each T3
//! thread's provider protocol traffic goes to
//! `<state>/logs/provider/events.<thread>.log`, one
//! `[<ISO time>] NTIVE: <json>` line per frame, and traffic tied to no thread
//! to `events._global.log`.
//!
//! Files rotate at 10 MiB, keeping 10 (`events.<thread>.log.1` …), as the
//! shared `makeAsyncRotatingFileSink` rotates them; the directory keeps at
//! most 512 MiB and 14 days of them, checked at start and every 5 minutes,
//! never removing a file that is being written. Transient deltas are not
//! written, and a record over 64 KiB is summarized.
//!
//! Writing happens on a thread of its own: a turn only hands its line over
//! (or drops it, with a warning, when the writer is behind), so logging never
//! fails or slows a turn.
use serde_json::{json, Map, Value};
use std::collections::{HashMap, HashSet};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{self, RecvTimeoutError, SyncSender, TrySendError};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const MEBIBYTE: u64 = 1024 * 1024;
const DAY_MS: u64 = 24 * 60 * 60 * 1_000;
const MAX_RECORD_CHARACTERS: i64 = 64 * 1024;
const MAX_RECORD_FIELDS: i64 = 1_024;
const MAX_RECORD_DEPTH: usize = 16;
const GLOBAL_THREAD_SEGMENT: &str = "_global";
/// `ATTACHMENT_ID_THREAD_SEGMENT_MAX_CHARS`.
const THREAD_SEGMENT_MAX_CHARS: usize = 80;
/// Records waiting for the writer; more are dropped.
const CHANNEL_CAPACITY: usize = 4_096;
const LOG_TARGET: &str = "provider_observability";

const TRANSIENT_CANONICAL_EVENT_TYPES: &[&str] = &[
    "content.delta",
    "hook.progress",
    "item.updated",
    "task.progress",
    "thread.realtime.audio.delta",
    "tool.progress",
    "turn.proposed.delta",
];
const TRANSIENT_NATIVE_METHODS: &[&str] = &[
    "item/agentMessage/delta",
    "item/commandExecution/outputDelta",
    "item/fileChange/outputDelta",
    "item/plan/delta",
    "item/reasoning/summaryTextDelta",
    "item/reasoning/textDelta",
    "thread/realtime/outputAudio/delta",
    "thread/realtime/transcript/delta",
    "turn/diff/updated",
];
const TRANSIENT_ACP_UPDATES: &[&str] = &["agent_message_chunk", "agent_thought_chunk"];
const TRANSIENT_CURSOR_UPDATES: &[&str] = &[
    "text-delta",
    "thinking-delta",
    "token-delta",
    "shell-output-delta",
    "partial-tool-call",
];

/// Which view of the provider a line records (`EventNdjsonStream`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[allow(dead_code)] // The reference names all three; only `Native` is written.
pub(crate) enum Stream {
    Native,
    Canonical,
    Orchestration,
}

impl Stream {
    /// `resolveStreamLabel`.
    fn label(self) -> &'static str {
        match self {
            Self::Native => "NTIVE",
            Self::Orchestration => "ORCH",
            Self::Canonical => "CANON",
        }
    }
}

/// The store's limits (`ResolvedOptions`); tests shrink them.
#[derive(Clone, Debug)]
pub(crate) struct Options {
    pub max_bytes: u64,
    pub max_files: u32,
    pub batch_window: Duration,
    pub max_total_bytes: u64,
    pub max_age_ms: u64,
    pub retention_interval_ms: u64,
    pub max_buffered_bytes: u64,
    pub max_buffered_records: usize,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            max_bytes: 10 * MEBIBYTE,
            max_files: 10,
            batch_window: Duration::from_millis(1_000),
            max_total_bytes: 512 * MEBIBYTE,
            max_age_ms: 14 * DAY_MS,
            retention_interval_ms: 5 * 60 * 1_000,
            max_buffered_bytes: MEBIBYTE,
            max_buffered_records: 512,
        }
    }
}

/// Where a frame went.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Direction {
    Incoming,
    Outgoing,
}

impl Direction {
    fn as_str(self) -> &'static str {
        match self {
            Self::Incoming => "incoming",
            Self::Outgoing => "outgoing",
        }
    }
}

/// How a provider's protocol frames are recorded, as the reference's
/// adapter for it records them.
pub(crate) enum Protocol {
    /// `makeCodexAppServerProtocolLogger`: every decoded frame, redacted.
    Codex { session: String },
    /// `makeClaudeAgentSdkProtocolLogger`: the SDK's messages and calls.
    Claude { session: String },
    /// Pi's RPC frames, recorded as Codex's are (the reference keeps no Pi
    /// transcript).
    Pi { session: String },
    /// `makeAcpNativeLoggerFactory`: each request the client makes, when it
    /// starts and when it is answered, summarized.
    Acp { provider: String },
    /// The Muse adapter's native logger: each notification `muse serve`
    /// sends (`{provider, method, params}`), but its streamed deltas.
    Muse,
}

/// One provider process's frames: its protocol, the thread they belong to
/// (unless each frame names its own), and the ACP requests awaiting replies.
pub(crate) struct FrameSource {
    protocol: Protocol,
    thread: Option<String>,
    pending: Mutex<HashMap<String, (String, Value)>>,
}

/// A store's handle; empty (a no-op) when the server keeps no provider logs.
#[derive(Clone, Default)]
pub(crate) struct ProviderEventLog(Option<Arc<Store>>);

struct Store {
    sender: SyncSender<Message>,
    dropped: AtomicU64,
}

enum Message {
    Record(Record),
    Flush(tokio::sync::oneshot::Sender<()>),
}

struct Record {
    stream: Stream,
    at: SystemTime,
    thread: Option<String>,
    body: Body,
}

enum Body {
    Event(Value),
    Frame {
        source: Arc<FrameSource>,
        direction: Direction,
        frame: Frame,
    },
}

enum Frame {
    Line(String),
    Decoded(Value),
}

impl ProviderEventLog {
    /// The store writing `<dir>/events.<thread>.log`
    /// (`providerEventLogPath`), or a no-op when it cannot start.
    pub(crate) fn open(dir: &Path) -> Self {
        Self::with_options(&dir.join("events.log"), Options::default())
    }

    pub(crate) fn with_options(file_path: &Path, options: Options) -> Self {
        let Some(directory) = file_path.parent().map(Path::to_path_buf) else {
            return Self::default();
        };
        if let Err(error) = std::fs::create_dir_all(&directory) {
            tracing::warn!(target: LOG_TARGET, directory = %directory.display(), %error,
                "Failed to create provider event log directory");
            return Self::default();
        }
        let (sender, receiver) = mpsc::sync_channel(CHANNEL_CAPACITY);
        let writer = Writer::new(directory, provider_log_prefix(file_path), options);
        let spawned = std::thread::Builder::new()
            .name("provider-event-log".into())
            .spawn(move || writer.run(receiver));
        match spawned {
            Ok(_) => Self(Some(Arc::new(Store {
                sender,
                dropped: AtomicU64::new(0),
            }))),
            Err(error) => {
                tracing::warn!(target: LOG_TARGET, %error, "provider event log writer did not start");
                Self::default()
            }
        }
    }

    pub(crate) fn enabled(&self) -> bool {
        self.0.is_some()
    }

    /// Record `event` on the native stream for `thread`.
    pub(crate) fn native(&self, event: Value, thread: Option<&str>) {
        if self.enabled() && should_persist_provider_event(Stream::Native, &event) {
            self.send(Record {
                stream: Stream::Native,
                at: SystemTime::now(),
                thread: thread.map(str::to_owned),
                body: Body::Event(event),
            });
        }
    }

    /// The frames of one provider process speaking `protocol`, for `thread`
    /// (`None`: each frame names its own).
    pub(crate) fn protocol(&self, protocol: Protocol, thread: Option<&str>) -> Option<ProtocolLog> {
        self.enabled().then(|| ProtocolLog {
            log: self.clone(),
            source: Arc::new(FrameSource {
                protocol,
                thread: thread.map(str::to_owned),
                pending: Mutex::default(),
            }),
        })
    }

    /// Wait (briefly) until what was recorded so far is written.
    pub(crate) async fn flush(&self) {
        let Some(store) = &self.0 else { return };
        let (ack, done) = tokio::sync::oneshot::channel();
        if store.sender.try_send(Message::Flush(ack)).is_ok() {
            let _ = tokio::time::timeout(Duration::from_secs(5), done).await;
        }
    }

    fn send(&self, record: Record) {
        let Some(store) = &self.0 else { return };
        if let Err(TrySendError::Full(_)) = store.sender.try_send(Message::Record(record)) {
            if store.dropped.fetch_add(1, Ordering::Relaxed) % 1_000 == 0 {
                tracing::warn!(target: LOG_TARGET, "provider event log is behind; dropping records");
            }
        }
    }
}

/// A provider process's frame recorder.
#[derive(Clone)]
pub(crate) struct ProtocolLog {
    log: ProviderEventLog,
    source: Arc<FrameSource>,
}

impl ProtocolLog {
    /// A line the process wrote (`incoming`) or was sent (`outgoing`).
    pub(crate) fn line(&self, direction: Direction, line: &str) {
        self.line_for(direction, line, None);
    }

    /// As [`Self::line`], for `thread` (a process shared by threads).
    pub(crate) fn line_for(&self, direction: Direction, line: &str, thread: Option<&str>) {
        self.frame(direction, Frame::Line(line.to_owned()), thread);
    }

    /// A message the client sent that is not a wire line (Claude's
    /// `query.open`).
    pub(crate) fn decoded(&self, direction: Direction, payload: Value) {
        self.frame(direction, Frame::Decoded(payload), None);
    }

    /// The process is being stopped: Claude's SDK logs its query closing.
    pub(crate) fn closing(&self) {
        if matches!(self.source.protocol, Protocol::Claude { .. }) {
            self.decoded(Direction::Outgoing, json!({"type": "query.close"}));
        }
    }

    fn frame(&self, direction: Direction, frame: Frame, thread: Option<&str>) {
        let thread = thread
            .map(str::to_owned)
            .or_else(|| self.source.thread.clone());
        self.log.send(Record {
            stream: Stream::Native,
            at: SystemTime::now(),
            thread,
            body: Body::Frame {
                source: self.source.clone(),
                direction,
                frame,
            },
        });
    }
}

/// `toSafeThreadAttachmentSegment`.
pub(crate) fn safe_thread_segment(thread: &str) -> Option<String> {
    let lowered = thread.trim().to_lowercase();
    let mut segment = String::with_capacity(lowered.len());
    for c in lowered.chars() {
        if c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_' || c == '-' {
            segment.push(c);
        } else if !segment.ends_with('-') {
            segment.push('-');
        }
    }
    // Runs of `-` collapse, then `-` and `_` leave both ends.
    let mut collapsed = String::with_capacity(segment.len());
    for c in segment.chars() {
        if !(c == '-' && collapsed.ends_with('-')) {
            collapsed.push(c);
        }
    }
    let trimmed = collapsed.trim_matches(['-', '_']);
    let cut: String = trimmed.chars().take(THREAD_SEGMENT_MAX_CHARS).collect();
    let cut = cut.trim_end_matches(['-', '_']);
    match cut {
        "" => None,
        "pending" => Some("_pending".into()),
        other => Some(other.to_owned()),
    }
}

/// `resolveThreadSegment`: the thread's file segment, hashed when the id is
/// long enough to be cut; `_global` without one.
pub(crate) fn resolve_thread_segment(thread: Option<&str>) -> String {
    let Some(raw) = thread else {
        return GLOBAL_THREAD_SEGMENT.into();
    };
    let Some(normalized) = safe_thread_segment(raw) else {
        return GLOBAL_THREAD_SEGMENT.into();
    };
    if js_length(raw) <= THREAD_SEGMENT_MAX_CHARS {
        return normalized;
    }
    use sha2::{Digest, Sha256};
    let hash = hex::encode(Sha256::digest(raw.as_bytes()));
    format!("{normalized}-{}", &hash[..16])
}

/// A string's JavaScript `length` (UTF-16 units).
fn js_length(text: &str) -> usize {
    text.encode_utf16().count()
}

/// `providerLogPrefix`: `events.log` → `events.`.
fn provider_log_prefix(file_path: &Path) -> String {
    let stem = file_path
        .file_stem()
        .map(|s| s.to_string_lossy().into_owned())
        .unwrap_or_default();
    format!("{stem}.")
}

fn provider_log_path(directory: &Path, prefix: &str, segment: &str) -> PathBuf {
    directory.join(format!("{prefix}{segment}.log"))
}

/// The reference's `DateTime.formatIso`: UTC, milliseconds.
pub(crate) fn iso_time(at: SystemTime) -> String {
    let at = time::OffsetDateTime::from(at);
    at.format(time::macros::format_description!(
        "[year]-[month]-[day]T[hour]:[minute]:[second].[subsecond digits:3]Z"
    ))
    .unwrap_or_default()
}

/// One log line: `[<ISO time>] <LABEL>: <json>\n`.
pub(crate) fn format_line(at: SystemTime, stream: Stream, payload: &str) -> String {
    format!("[{}] {}: {payload}\n", iso_time(at), stream.label())
}

fn str_in(value: Option<&Value>, set: &[&str]) -> bool {
    value
        .and_then(Value::as_str)
        .is_some_and(|text| set.contains(&text))
}

/// `shouldPersistProviderEvent`: transient deltas are not worth a line.
pub(crate) fn should_persist_provider_event(stream: Stream, event: &Value) -> bool {
    if stream == Stream::Orchestration || !event.is_object() {
        return true;
    }
    if str_in(event.get("type"), TRANSIENT_CANONICAL_EVENT_TYPES) {
        return false;
    }
    if stream != Stream::Native {
        return true;
    }
    let envelope = event
        .get("event")
        .filter(|e| e.is_object())
        .unwrap_or(event);
    // Decoded frames preserve raw-frame information without duplicating every token delta.
    if envelope.get("stage").and_then(Value::as_str) == Some("raw") {
        return false;
    }
    let native = match envelope.get("payload") {
        Some(payload)
            if payload.is_object()
                && envelope.get("stage").and_then(Value::as_str) == Some("decoded") =>
        {
            payload
        }
        _ => envelope,
    };
    let method = native.get("method").and_then(Value::as_str);
    if method.is_some_and(|m| {
        TRANSIENT_NATIVE_METHODS.contains(&m)
            || m.starts_with("claude/stream_event/content_block_delta/")
    }) {
        return false;
    }
    let native_type = native.get("type").and_then(Value::as_str);
    if native_type == Some("system")
        && native.get("subtype").and_then(Value::as_str) == Some("thinking_tokens")
    {
        return false;
    }
    if native_type == Some("message.part.delta") {
        return false;
    }
    if native_type == Some("stream_event")
        && native
            .get("event")
            .filter(|e| e.is_object())
            .and_then(|e| e.get("type"))
            .and_then(Value::as_str)
            == Some("content_block_delta")
    {
        return false;
    }
    if native_type == Some("interaction.update") {
        let Some(mut update) = native.get("update").filter(|u| u.is_object()) else {
            return true;
        };
        // Subagent updates arrive wrapped; keep the wrapper when it carries a lifecycle record.
        if update.get("type").and_then(Value::as_str) == Some("tool-call-delta") {
            match update.get("taskUpdate").filter(|u| u.is_object()) {
                Some(task) => update = task,
                None => return true,
            }
        }
        return !str_in(update.get("type"), TRANSIENT_CURSOR_UPDATES);
    }
    let Some(payload) = native.get("payload").filter(|p| p.is_object()) else {
        return true;
    };
    if method == Some("session/update") {
        let Some(update) = payload.get("update").filter(|u| u.is_object()) else {
            return true;
        };
        return !str_in(update.get("sessionUpdate"), TRANSIENT_ACP_UPDATES);
    }
    if native_type == Some("message.part.updated") {
        let Some(part) = payload
            .get("properties")
            .filter(|p| p.is_object())
            .and_then(|p| p.get("part"))
            .filter(|p| p.is_object())
        else {
            return true;
        };
        match part.get("type").and_then(Value::as_str) {
            Some("text" | "reasoning") => return false,
            // Running snapshots repeat growing output.
            Some("tool") => {
                if let Some(state) = part.get("state").filter(|s| s.is_object()) {
                    return state.get("status").and_then(Value::as_str) != Some("running");
                }
            }
            _ => {}
        }
        return true;
    }
    true
}

const SUMMARY_FIELDS: &[&str] = &[
    "provider",
    "protocol",
    "kind",
    "providerSessionId",
    "direction",
    "stage",
    "type",
    "subtype",
    "method",
    "id",
    "threadId",
    "turnId",
    "requestId",
    "session_id",
    "status",
    "is_error",
    "api_error_status",
    "terminal_reason",
    "stop_reason",
    "operation",
    "code",
    "willRetry",
    "message",
    "event",
    "payload",
    "params",
    "result",
    "thread",
    "turn",
    "error",
    "turns",
    "items",
    "content",
];

/// `summarizeProviderEvent`: the record's identifying fields, bounded.
pub(crate) fn summarize_provider_event(event: &Value) -> Value {
    fn summarize(value: &Value, depth: usize, fields: &mut i64, characters: &mut i64) -> Value {
        match value {
            Value::String(text) => {
                let length = js_length(text) as i64;
                if length > (*characters).min(1_024) {
                    return json!({"omittedCharacters": length});
                }
                *characters -= length;
                value.clone()
            }
            Value::Null | Value::Number(_) | Value::Bool(_) => value.clone(),
            Value::Array(items) => json!({"itemCount": items.len()}),
            Value::Object(object) => {
                let mut summary = Map::new();
                summary.insert("truncated".into(), true.into());
                if depth >= 6 {
                    return Value::Object(summary);
                }
                for key in SUMMARY_FIELDS {
                    if *fields <= 0 {
                        break;
                    }
                    let Some(nested) = object.get(*key) else {
                        continue;
                    };
                    *fields -= 1;
                    let nested = summarize(nested, depth + 1, fields, characters);
                    summary.insert((*key).into(), nested);
                }
                Value::Object(summary)
            }
        }
    }
    summarize(event, 0, &mut 128, &mut (8 * 1024))
}

/// `boundProviderEventForLogging`: the record itself when it is within
/// 64 Ki characters, 1,024 fields and 16 levels, else its summary.
pub(crate) fn bound_provider_event_for_logging(event: Value) -> Value {
    if fits_record_bounds(&event) {
        event
    } else {
        summarize_provider_event(&event)
    }
}

/// Whether a record is within 64 Ki characters, 1,024 fields and 16 levels.
fn fits_record_bounds(event: &Value) -> bool {
    fn fits(value: &Value, depth: usize, characters: &mut i64, fields: &mut i64) -> bool {
        match value {
            Value::String(text) => {
                *characters -= js_length(text) as i64;
                *characters >= 0
            }
            Value::Array(items) => {
                if depth > MAX_RECORD_DEPTH || items.len() as i64 > *fields {
                    return false;
                }
                // `for…in` walks an array's indices as its keys.
                items.iter().enumerate().all(|(index, item)| {
                    *fields -= 1;
                    *characters -= index.to_string().len() as i64;
                    *fields >= 0 && *characters >= 0 && fits(item, depth + 1, characters, fields)
                })
            }
            Value::Object(object) => {
                if depth > MAX_RECORD_DEPTH {
                    return false;
                }
                object.iter().all(|(key, item)| {
                    *fields -= 1;
                    *characters -= js_length(key) as i64;
                    *fields >= 0 && *characters >= 0 && fits(item, depth + 1, characters, fields)
                })
            }
            _ => true,
        }
    }
    let (mut characters, mut fields) = (MAX_RECORD_CHARACTERS, MAX_RECORD_FIELDS);
    fits(event, 0, &mut characters, &mut fields)
}

/// `structuralProtocolMethod`: a method name, or `unknown` when it is not
/// one.
pub(crate) fn structural_protocol_method(value: &str) -> String {
    if is_structural(value) {
        value.to_owned()
    } else {
        "unknown".into()
    }
}

fn is_structural(value: &str) -> bool {
    let mut chars = value.chars();
    value.len() <= 128
        && chars.next().is_some_and(|c| c.is_ascii_alphabetic())
        && chars.all(|c| c.is_ascii_alphanumeric() || "._:/-".contains(c))
}

/// `summarizeNativeProtocolPayload`: a payload's shape, never its values.
pub(crate) fn summarize_native_protocol_payload(payload: Option<&Value>) -> Value {
    match payload {
        None => json!({"valueType": "undefined"}),
        Some(Value::Null) => json!({"valueType": "null"}),
        Some(Value::String(text)) => json!({"valueType": "string", "byteLength": text.len()}),
        Some(Value::Array(items)) => json!({"valueType": "array", "itemCount": items.len()}),
        Some(Value::Number(_)) => json!({"valueType": "number"}),
        Some(Value::Bool(_)) => json!({"valueType": "boolean"}),
        Some(Value::Object(record)) => {
            let mut summary = json!({"valueType": "object", "fieldCount": record.len()});
            if let Some(tag) = record.get("_tag").and_then(Value::as_str) {
                summary["messageTag"] = if is_structural(tag) && !tag.is_empty() {
                    tag.into()
                } else {
                    "TaggedError".into()
                };
            }
            if let Some(tag) = record.get("tag").and_then(Value::as_str) {
                summary["method"] = structural_protocol_method(tag).into();
            }
            summary
        }
    }
}

/// `redactCodexProtocolValue`: credentials out of a Codex frame, including
/// those inside JSON strings.
pub(crate) fn redact_codex_protocol_value(value: Value) -> Value {
    match value {
        Value::Array(items) => {
            Value::Array(items.into_iter().map(redact_codex_protocol_value).collect())
        }
        Value::Object(object) => Value::Object(
            object
                .into_iter()
                .map(|(key, nested)| {
                    let nested = if is_sensitive_codex_protocol_key(&key) {
                        "[REDACTED]".into()
                    } else {
                        redact_codex_protocol_value(nested)
                    };
                    (key, nested)
                })
                .collect(),
        ),
        Value::String(text) => {
            // `/^Bearer\s+/i`.
            if text
                .get(..6)
                .is_some_and(|head| head.eq_ignore_ascii_case("bearer"))
                && text[6..].starts_with(char::is_whitespace)
            {
                return "[REDACTED]".into();
            }
            let trimmed = text.trim();
            let looks_json = (trimmed.starts_with('{') && trimmed.ends_with('}'))
                || (trimmed.starts_with('[') && trimmed.ends_with(']'));
            if !looks_json {
                return Value::String(text);
            }
            match serde_json::from_str::<Value>(trimmed) {
                Ok(parsed) => Value::String(
                    redact_codex_protocol_value(bound_provider_event_for_logging(parsed))
                        .to_string(),
                ),
                Err(_) => Value::String(text),
            }
        }
        other => other,
    }
}

fn is_sensitive_codex_protocol_key(key: &str) -> bool {
    let normalized: String = key
        .chars()
        .filter(char::is_ascii_alphanumeric)
        .collect::<String>()
        .to_ascii_lowercase();
    ["authorization", "apikey", "token", "password", "secret"]
        .iter()
        .any(|suffix| normalized.ends_with(suffix))
}

/// A Claude client message as the SDK's logger names it: a prompt is
/// `prompt.offer`, and the controls the SDK wraps are its `query.*` calls.
/// The SDK's own handshake (another control request) is not its logger's;
/// `None` for it.
fn claude_outgoing(frame: Value) -> Option<Value> {
    Some(match frame["type"].as_str() {
        Some("user") => json!({"type": "prompt.offer", "message": frame}),
        Some("control_request") => {
            let request = &frame["request"];
            match request["subtype"].as_str() {
                Some("interrupt") => json!({"type": "query.interrupt"}),
                Some("set_model") => {
                    json!({"type": "query.set_model", "model": request["model"]})
                }
                Some("set_permission_mode") => {
                    json!({"type": "query.set_permission_mode", "mode": request["mode"]})
                }
                _ => return None,
            }
        }
        _ => frame,
    })
}

/// Pi's streamed text and thinking: one frame per token.
fn pi_transient(frame: &Value) -> bool {
    frame["type"] == "message_update"
        && frame["assistantMessageEvent"]["type"]
            .as_str()
            .is_some_and(|kind| kind.ends_with("_delta"))
}

/// The record a protocol frame becomes, if any.
fn render_frame(
    source: &FrameSource,
    direction: Direction,
    frame: Frame,
    at: SystemTime,
    thread: Option<&str>,
) -> Option<Value> {
    let decoded = match frame {
        Frame::Decoded(value) => Ok(value),
        Frame::Line(line) => serde_json::from_str::<Value>(&line).map_err(|_| ()),
    };
    let envelope = |provider: &str, protocol: &str, session: &str, event: Value| {
        json!({"provider": provider, "protocol": protocol, "kind": "protocol",
            "providerSessionId": session, "event": event})
    };
    let event = |payload: Result<Value, ()>| match payload {
        Ok(payload) => {
            json!({"direction": direction.as_str(), "stage": "decoded", "payload": payload})
        }
        Err(()) => json!({"direction": direction.as_str(), "stage": "decode_failed",
            "payload": {"operation": "decode-wire-message"}}),
    };
    match &source.protocol {
        Protocol::Codex { session } => {
            let event =
                redact_codex_protocol_value(bound_provider_event_for_logging(event(decoded)));
            Some(envelope("codex", "codex.app-server", session, event))
        }
        Protocol::Pi { session } => {
            if decoded.as_ref().is_ok_and(pi_transient) {
                return None;
            }
            let event =
                redact_codex_protocol_value(bound_provider_event_for_logging(event(decoded)));
            Some(envelope("pi", "pi.rpc", session, event))
        }
        Protocol::Claude { session } => {
            let decoded = match (direction, decoded) {
                (Direction::Outgoing, Ok(frame)) => Ok(claude_outgoing(frame)?),
                // The answers to the SDK's controls stay inside it.
                (Direction::Incoming, Ok(frame)) if frame["type"] == "control_response" => {
                    return None
                }
                (_, decoded) => decoded,
            };
            let event = bound_provider_event_for_logging(event(decoded));
            Some(envelope(
                "claudeAgent",
                "claude-agent-sdk.query",
                session,
                event,
            ))
        }
        Protocol::Muse => {
            let frame = decoded.ok()?;
            let method = frame.get("method").and_then(Value::as_str)?;
            if direction != Direction::Incoming
                || frame.get("id").is_some_and(|id| !id.is_null())
                || method == "item/delta"
            {
                return None;
            }
            Some(json!({"provider": "muse", "method": method,
                "params": frame.get("params").cloned().unwrap_or(Value::Null)}))
        }
        Protocol::Acp { provider } => {
            let frame = decoded.ok()?;
            let id = frame.get("id").filter(|id| !id.is_null())?.to_string();
            let mut pending = source.pending.lock().unwrap_or_else(|e| e.into_inner());
            let payload = match (direction, frame.get("method").and_then(Value::as_str)) {
                (Direction::Outgoing, Some(method)) => {
                    let request = summarize_native_protocol_payload(frame.get("params"));
                    pending.insert(id, (method.to_owned(), request.clone()));
                    json!({"method": structural_protocol_method(method), "status": "started",
                        "request": request})
                }
                (Direction::Incoming, None) => {
                    let (method, request) = pending.remove(&id)?;
                    let mut payload = json!({"method": structural_protocol_method(&method)});
                    match frame.get("error") {
                        Some(_) => {
                            payload["status"] = "failed".into();
                            payload["request"] = request;
                            payload["errorTag"] = "AcpRequestError".into();
                            payload["reasonCount"] = 1.into();
                        }
                        None => {
                            payload["status"] = "succeeded".into();
                            payload["request"] = request;
                            payload["result"] =
                                summarize_native_protocol_payload(frame.get("result"));
                        }
                    }
                    payload
                }
                _ => return None,
            };
            let observed = iso_time(at);
            Some(json!({"observedAt": observed, "event": {
                "id": uuid::Uuid::new_v4().to_string(),
                "kind": "request",
                "provider": provider,
                "createdAt": observed,
                "threadId": thread,
                "payload": payload,
            }}))
        }
    }
}

/// The line a record becomes: bounded, then summarized when its JSON is
/// still over 64 KiB; `None` when it is transient.
fn record_line(stream: Stream, at: SystemTime, event: Value) -> Option<String> {
    if !should_persist_provider_event(stream, &event) {
        return None;
    }
    let summary = || serde_json::to_string(&summarize_provider_event(&event)).ok();
    let mut payload = if fits_record_bounds(&event) {
        serde_json::to_string(&event).ok()?
    } else {
        summary()?
    };
    // Escaping can expand strings beyond their input size.
    if payload.len() as i64 > MAX_RECORD_CHARACTERS {
        payload = summary()?;
    }
    Some(format_line(at, stream, &payload))
}

/// `makeAsyncRotatingFileSink`: appends to a file, which moves to `.1` (and
/// `.1` to `.2` …, keeping `max_files`) once the next write would take it
/// over `max_bytes`.
pub(crate) struct RotatingSink {
    path: PathBuf,
    max_bytes: u64,
    max_files: u32,
    size: u64,
}

impl RotatingSink {
    pub(crate) fn open(path: &Path, max_bytes: u64, max_files: u32) -> std::io::Result<Self> {
        if max_bytes < 1 || max_files < 1 {
            return Err(std::io::ErrorKind::InvalidInput.into());
        }
        let directory = path.parent().unwrap_or(Path::new("."));
        std::fs::create_dir_all(directory)?;
        let basename = path
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_default();
        // Rotated files past the kept count (from a larger count) go.
        for entry in std::fs::read_dir(directory)? {
            let name = entry?.file_name().to_string_lossy().into_owned();
            let Some(suffix) = name.strip_prefix(&format!("{basename}.")) else {
                continue;
            };
            let canonical = suffix
                .parse::<u64>()
                .ok()
                .filter(|n| n.to_string() == suffix);
            if canonical.is_some_and(|n| n > u64::from(max_files)) {
                remove_file_forced(&directory.join(&name))?;
            }
        }
        let mut sink = Self {
            path: path.to_path_buf(),
            max_bytes,
            max_files,
            size: 0,
        };
        sink.size = sink.read_size()?;
        Ok(sink)
    }

    fn read_size(&self) -> std::io::Result<u64> {
        match std::fs::metadata(&self.path) {
            Ok(meta) => Ok(meta.len()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(0),
            Err(error) => Err(error),
        }
    }

    fn rotated(&self, index: u32) -> PathBuf {
        let mut name = self.path.as_os_str().to_owned();
        name.push(format!(".{index}"));
        PathBuf::from(name)
    }

    fn rotate(&mut self) -> std::io::Result<()> {
        remove_file_forced(&self.rotated(self.max_files))?;
        for index in (1..self.max_files).rev() {
            let source = self.rotated(index);
            if source.exists() {
                std::fs::rename(&source, self.rotated(index + 1))?;
            }
        }
        if self.path.exists() {
            std::fs::rename(&self.path, self.rotated(1))?;
        }
        self.size = 0;
        Ok(())
    }

    pub(crate) fn write(&mut self, chunk: &[u8]) -> std::io::Result<()> {
        if chunk.is_empty() {
            return Ok(());
        }
        let written = (|| {
            if self.size > 0 && self.size + chunk.len() as u64 > self.max_bytes {
                self.rotate()?;
            }
            std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&self.path)?
                .write_all(chunk)?;
            self.size += chunk.len() as u64;
            Ok(())
        })();
        if written.is_err() {
            if let Ok(size) = self.read_size() {
                self.size = size;
            }
        }
        written
    }
}

fn remove_file_forced(path: &Path) -> std::io::Result<()> {
    match std::fs::remove_file(path) {
        Err(error) if error.kind() != std::io::ErrorKind::NotFound => Err(error),
        _ => Ok(()),
    }
}

/// `writeBatchedMessages`: lines joined into writes of at most `max_bytes`
/// (a lone longer line goes alone), so rotation falls between batches.
fn write_batched(sink: &mut RotatingSink, lines: &[String], max_bytes: u64) -> std::io::Result<()> {
    let mut batch = String::new();
    for line in lines {
        if !batch.is_empty() && (batch.len() + line.len()) as u64 > max_bytes {
            sink.write(batch.as_bytes())?;
            batch.clear();
        }
        batch.push_str(line);
        if batch.len() as u64 >= max_bytes {
            sink.write(batch.as_bytes())?;
            batch.clear();
        }
    }
    sink.write(batch.as_bytes())
}

/// `isProviderLogFile`: a `.log` or `.log.<n>` file named with the prefix,
/// or whose first line is a provider log line.
fn is_provider_log_file(path: &Path, name: &str, prefix: &str) -> std::io::Result<bool> {
    let rotated = name
        .rsplit_once(".log.")
        .is_some_and(|(_, n)| !n.is_empty() && n.bytes().all(|b| b.is_ascii_digit()));
    if !(name.ends_with(".log") || rotated) {
        return Ok(false);
    }
    if name.starts_with(prefix) {
        return Ok(true);
    }
    let mut header = [0u8; 256];
    let mut file = std::fs::File::open(path)?;
    let mut read = 0;
    while read < header.len() {
        match file.read(&mut header[read..])? {
            0 => break,
            n => read += n,
        }
    }
    let header = String::from_utf8_lossy(&header[..read]);
    let Some(rest) = header.strip_prefix('[') else {
        return Ok(false);
    };
    let Some(end) = rest.find(']') else {
        return Ok(false);
    };
    let stamp = &rest[..end];
    Ok(!stamp.is_empty()
        && !stamp.contains(['\r', '\n'])
        && ["NTIVE: ", "CANON: ", "ORCH: "]
            .iter()
            .any(|label| rest[end + 1..].starts_with(&format!(" {label}"))))
}

fn millis(at: SystemTime) -> u64 {
    at.duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or_default()
}

/// `enforceRetention`: files older than `max_age_ms`, then the oldest until
/// the rest fit in `max_total_bytes`; never an active one.
pub(crate) fn enforce_retention(
    directory: &Path,
    prefix: &str,
    max_total_bytes: u64,
    max_age_ms: u64,
    active: &HashSet<PathBuf>,
    now_ms: u64,
) -> Vec<(PathBuf, std::io::Error)> {
    let mut failures = Vec::new();
    let entries = match std::fs::read_dir(directory) {
        Ok(entries) => entries,
        Err(error) => return vec![(directory.to_path_buf(), error)],
    };
    let mut files: Vec<(PathBuf, u64, u64)> = Vec::new();
    for entry in entries.flatten() {
        if !entry.file_type().is_ok_and(|t| t.is_file()) {
            continue;
        }
        let path = entry.path();
        let name = entry.file_name().to_string_lossy().into_owned();
        let found = is_provider_log_file(&path, &name, prefix).and_then(|is| {
            if !is {
                return Ok(None);
            }
            let meta = std::fs::metadata(&path)?;
            Ok(Some((millis(meta.modified()?), meta.len())))
        });
        match found {
            Ok(Some((mtime, size))) => files.push((path, mtime, size)),
            Ok(None) => {}
            Err(error) => failures.push((path, error)),
        }
    }
    let mut total: u64 = files.iter().map(|(_, _, size)| size).sum();
    let remove = |path: &Path, failures: &mut Vec<(PathBuf, std::io::Error)>| {
        if active.contains(path) {
            return false;
        }
        match remove_file_forced(path) {
            Ok(()) => true,
            Err(error) => {
                failures.push((path.to_path_buf(), error));
                false
            }
        }
    };
    let mut retained = Vec::new();
    for file in files {
        if now_ms.saturating_sub(file.1) > max_age_ms && remove(&file.0, &mut failures) {
            total = total.saturating_sub(file.2);
        } else {
            retained.push(file);
        }
    }
    retained.sort_by(|a, b| a.1.cmp(&b.1).then_with(|| a.0.cmp(&b.0)));
    for (path, _, size) in retained {
        if total <= max_total_bytes {
            break;
        }
        if remove(&path, &mut failures) {
            total = total.saturating_sub(size);
        }
    }
    failures
}

/// The writer thread's state (`StoreState`).
pub(crate) struct Writer {
    directory: PathBuf,
    prefix: String,
    options: Options,
    sinks: HashMap<String, RotatingSink>,
    written_since_retention: HashSet<String>,
    last_retention_ms: u64,
    pending: Vec<(String, String)>,
    pending_bytes: u64,
    first_pending: Option<Instant>,
}

impl Writer {
    pub(crate) fn new(directory: PathBuf, prefix: String, options: Options) -> Self {
        Self {
            directory,
            prefix,
            options,
            sinks: HashMap::new(),
            written_since_retention: HashSet::new(),
            last_retention_ms: 0,
            pending: Vec::new(),
            pending_bytes: 0,
            first_pending: None,
        }
    }

    fn run(mut self, receiver: mpsc::Receiver<Message>) {
        self.start(millis(SystemTime::now()));
        loop {
            let message = match self.first_pending {
                None => receiver.recv().map_err(|_| RecvTimeoutError::Disconnected),
                Some(first) => receiver.recv_timeout(
                    (first + self.options.batch_window).saturating_duration_since(Instant::now()),
                ),
            };
            match message {
                Ok(Message::Record(record)) => {
                    if self.push(record) {
                        self.drain(millis(SystemTime::now()));
                    }
                }
                Ok(Message::Flush(ack)) => {
                    self.drain(millis(SystemTime::now()));
                    let _ = ack.send(());
                }
                Err(RecvTimeoutError::Timeout) => self.drain(millis(SystemTime::now())),
                Err(RecvTimeoutError::Disconnected) => {
                    self.drain(millis(SystemTime::now()));
                    return;
                }
            }
        }
    }

    /// The store's start: retention over what earlier runs left.
    pub(crate) fn start(&mut self, now_ms: u64) {
        let failures = enforce_retention(
            &self.directory,
            &self.prefix,
            self.options.max_total_bytes,
            self.options.max_age_ms,
            &HashSet::new(),
            now_ms,
        );
        warn_failures("provider event log retention failed", failures);
        self.last_retention_ms = now_ms;
    }

    /// Queue a record's line; whether the buffer is full enough to write.
    fn push(&mut self, record: Record) -> bool {
        let Record {
            stream,
            at,
            thread,
            body,
        } = record;
        let event = match body {
            Body::Event(event) => Some(event),
            Body::Frame {
                source,
                direction,
                frame,
            } => render_frame(&source, direction, frame, at, thread.as_deref()),
        };
        let Some(line) = event.and_then(|event| record_line(stream, at, event)) else {
            return false;
        };
        self.push_line(resolve_thread_segment(thread.as_deref()), line)
    }

    pub(crate) fn push_line(&mut self, segment: String, line: String) -> bool {
        self.pending_bytes += line.len() as u64;
        self.pending.push((segment, line));
        self.first_pending.get_or_insert_with(Instant::now);
        self.options.batch_window.is_zero()
            || self.pending.len() >= self.options.max_buffered_records
            || self.pending_bytes >= self.options.max_buffered_bytes
    }

    /// `drainPending`: write what is queued, by thread file, then run
    /// retention when it is due.
    pub(crate) fn drain(&mut self, now_ms: u64) {
        let pending = std::mem::take(&mut self.pending);
        self.pending_bytes = 0;
        self.first_pending = None;
        let mut order: Vec<String> = Vec::new();
        let mut by_segment: HashMap<String, Vec<String>> = HashMap::new();
        for (segment, line) in pending {
            by_segment
                .entry(segment.clone())
                .or_insert_with(|| {
                    order.push(segment);
                    Vec::new()
                })
                .push(line);
        }
        let mut failures = Vec::new();
        for segment in order {
            let lines = by_segment.remove(&segment).unwrap_or_default();
            let path = provider_log_path(&self.directory, &self.prefix, &segment);
            if !self.sinks.contains_key(&segment) {
                match RotatingSink::open(&path, self.options.max_bytes, self.options.max_files) {
                    Ok(sink) => {
                        self.sinks.insert(segment.clone(), sink);
                    }
                    Err(error) => {
                        failures.push((path, error));
                        continue;
                    }
                }
            }
            self.written_since_retention.insert(segment.clone());
            let sink = self.sinks.get_mut(&segment).expect("opened above");
            if let Err(error) = write_batched(sink, &lines, self.options.max_bytes) {
                self.sinks.remove(&segment);
                failures.push((path, error));
            }
        }
        if now_ms.saturating_sub(self.last_retention_ms) >= self.options.retention_interval_ms {
            // A sink idle for a whole retention interval reopens from disk on its next
            // write, so retention may remove its files without leaving a stale size.
            let written = std::mem::take(&mut self.written_since_retention);
            self.sinks.retain(|segment, _| written.contains(segment));
            let active: HashSet<PathBuf> = self
                .sinks
                .keys()
                .map(|segment| provider_log_path(&self.directory, &self.prefix, segment))
                .collect();
            failures.extend(enforce_retention(
                &self.directory,
                &self.prefix,
                self.options.max_total_bytes,
                self.options.max_age_ms,
                &active,
                now_ms,
            ));
            self.last_retention_ms = now_ms;
        }
        warn_failures("provider event log write or retention failed", failures);
    }
}

fn warn_failures(message: &str, failures: Vec<(PathBuf, std::io::Error)>) {
    for (path, error) in failures {
        tracing::warn!(target: LOG_TARGET, file = %path.display(), %error, "{message}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at() -> SystemTime {
        UNIX_EPOCH + Duration::from_millis(1_764_072_000_123)
    }

    #[test]
    fn thread_segments_are_safe_and_long_ids_hashed() {
        assert_eq!(resolve_thread_segment(Some("thread-1")), "thread-1");
        assert_eq!(
            resolve_thread_segment(Some("  Thread:ABC/def__ ")),
            "thread-abc-def"
        );
        assert_eq!(resolve_thread_segment(Some("pending")), "_pending");
        assert_eq!(resolve_thread_segment(Some("::")), "_global");
        assert_eq!(resolve_thread_segment(None), "_global");
        let long = format!("thread:delegated-task:{}", "x".repeat(90));
        let segment = resolve_thread_segment(Some(&long));
        use sha2::{Digest, Sha256};
        let hash = hex::encode(Sha256::digest(long.as_bytes()));
        let head = safe_thread_segment(&long).unwrap();
        assert_eq!(head.len(), 80);
        assert_eq!(segment, format!("{head}-{}", &hash[..16]));
        // Exactly the limit is not hashed.
        let limit = "a".repeat(80);
        assert_eq!(resolve_thread_segment(Some(&limit)), limit);
    }

    #[test]
    fn lines_carry_the_time_and_the_stream_label() {
        assert_eq!(
            format_line(at(), Stream::Native, r#"{"a":1}"#),
            "[2025-11-25T12:00:00.123Z] NTIVE: {\"a\":1}\n"
        );
        assert!(format_line(at(), Stream::Canonical, "{}").contains("] CANON: {}"));
        assert!(format_line(at(), Stream::Orchestration, "{}").contains("] ORCH: {}"));
    }

    #[test]
    fn transient_deltas_are_not_persisted() {
        let codex = |method: &str| {
            json!({"provider": "codex", "event": {"direction": "incoming", "stage": "decoded",
                "payload": {"method": method, "params": {}}}})
        };
        assert!(!should_persist_provider_event(
            Stream::Native,
            &codex("item/agentMessage/delta")
        ));
        assert!(should_persist_provider_event(
            Stream::Native,
            &codex("item/completed")
        ));
        assert!(!should_persist_provider_event(
            Stream::Native,
            &json!({"event": {"stage": "raw", "payload": "{}"}})
        ));
        let claude = json!({"event": {"stage": "decoded", "payload": {"type": "stream_event",
            "event": {"type": "content_block_delta"}}}});
        assert!(!should_persist_provider_event(Stream::Native, &claude));
        let thinking = json!({"event": {"stage": "decoded", "payload": {"type": "system",
            "subtype": "thinking_tokens"}}});
        assert!(!should_persist_provider_event(Stream::Native, &thinking));
        let acp = |kind: &str| json!({"method": "session/update", "payload": {"update": {"sessionUpdate": kind}}});
        assert!(!should_persist_provider_event(
            Stream::Native,
            &acp("agent_message_chunk")
        ));
        assert!(should_persist_provider_event(
            Stream::Native,
            &acp("tool_call")
        ));
        let part = |kind: &str, status: &str| {
            json!({"type": "message.part.updated", "payload": {"properties": {"part":
                {"type": kind, "state": {"status": status}}}}})
        };
        assert!(!should_persist_provider_event(
            Stream::Native,
            &part("text", "")
        ));
        assert!(!should_persist_provider_event(
            Stream::Native,
            &part("tool", "running")
        ));
        assert!(should_persist_provider_event(
            Stream::Native,
            &part("tool", "completed")
        ));
        let cursor = json!({"type": "interaction.update", "update": {"type": "tool-call-delta",
            "taskUpdate": {"type": "text-delta"}}});
        assert!(!should_persist_provider_event(Stream::Native, &cursor));
        assert!(!should_persist_provider_event(
            Stream::Canonical,
            &json!({"type": "content.delta"})
        ));
        assert!(should_persist_provider_event(
            Stream::Orchestration,
            &json!({"type": "content.delta"})
        ));
        assert!(should_persist_provider_event(
            Stream::Canonical,
            &json!({"type": "turn.completed"})
        ));
    }

    #[test]
    fn oversized_records_are_summarized() {
        let small = json!({"method": "x", "params": {"text": "hi"}});
        assert_eq!(bound_provider_event_for_logging(small.clone()), small);
        let big = json!({"provider": "codex", "method": "item/completed",
            "params": {"text": "y".repeat(70_000)}, "other": 1});
        assert_eq!(
            bound_provider_event_for_logging(big),
            json!({"truncated": true, "provider": "codex", "method": "item/completed",
                "params": {"truncated": true}})
        );
        let wide = json!({"items": (0..2_000).collect::<Vec<_>>(), "type": "t"});
        assert_eq!(
            bound_provider_event_for_logging(wide),
            json!({"truncated": true, "type": "t", "items": {"itemCount": 2000}})
        );
        let mut deep = json!("leaf");
        for _ in 0..20 {
            deep = json!({"event": deep});
        }
        let summary = bound_provider_event_for_logging(deep);
        assert_eq!(summary["truncated"], true);
        // Six levels, then only the marker.
        let mut level = &summary;
        for _ in 0..6 {
            level = &level["event"];
        }
        assert_eq!(level, &json!({"truncated": true}));
        // Long strings in a summary are only counted.
        assert_eq!(
            summarize_provider_event(&json!({"message": "z".repeat(2_000)})),
            json!({"truncated": true, "message": {"omittedCharacters": 2000}})
        );
        // A record whose escaped JSON passes 64 KiB is summarized too.
        let quotes = json!({"type": "q", "text": "\"".repeat(40_000)});
        let line = record_line(Stream::Native, at(), quotes).unwrap();
        assert!(
            line.ends_with("NTIVE: {\"truncated\":true,\"type\":\"q\"}\n"),
            "{line}"
        );
    }

    #[test]
    fn protocol_payload_summaries_hold_no_values() {
        assert_eq!(
            summarize_native_protocol_payload(Some(&json!({"_tag": "X", "tag": "a b", "k": 1}))),
            json!({"valueType": "object", "fieldCount": 3, "messageTag": "X", "method": "unknown"})
        );
        assert_eq!(
            summarize_native_protocol_payload(Some(&json!("héllo"))),
            json!({"valueType": "string", "byteLength": 6})
        );
        assert_eq!(
            summarize_native_protocol_payload(None),
            json!({"valueType": "undefined"})
        );
        assert_eq!(
            structural_protocol_method("session/prompt"),
            "session/prompt"
        );
        assert_eq!(structural_protocol_method("9bad"), "unknown");
    }

    #[test]
    fn codex_frames_lose_their_credentials() {
        let frame = json!({"params": {"apiKey": "k", "headers": {"Authorization": "x"},
            "bearer": "Bearer abc", "nested": "{\"access_token\":\"t\"}", "count": 2}});
        assert_eq!(
            redact_codex_protocol_value(frame),
            json!({"params": {"apiKey": "[REDACTED]", "headers": {"Authorization": "[REDACTED]"},
                "bearer": "[REDACTED]", "nested": "{\"access_token\":\"[REDACTED]\"}", "count": 2}})
        );
    }

    fn lines_of(path: &Path) -> Vec<String> {
        std::fs::read_to_string(path)
            .unwrap_or_default()
            .lines()
            .map(str::to_owned)
            .collect()
    }

    #[test]
    fn files_rotate_at_max_bytes_keeping_max_files() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("events.t.log");
        // A leftover past the kept count goes when the sink opens.
        std::fs::write(dir.path().join("events.t.log.7"), "old").unwrap();
        let mut sink = RotatingSink::open(&path, 10, 2).unwrap();
        assert!(!dir.path().join("events.t.log.7").exists());
        for chunk in [
            "aaaa\n", "bbbb\n", "cccc\n", "dddd\n", "eeee\n", "ffff\n", "gggg\n",
        ] {
            sink.write(chunk.as_bytes()).unwrap();
        }
        assert_eq!(lines_of(&path), ["gggg"]);
        assert_eq!(
            lines_of(&dir.path().join("events.t.log.1")),
            ["eeee", "ffff"]
        );
        assert_eq!(
            lines_of(&dir.path().join("events.t.log.2")),
            ["cccc", "dddd"]
        );
        assert!(!dir.path().join("events.t.log.3").exists());
        // A reopened sink knows the file's size.
        let mut sink = RotatingSink::open(&path, 10, 2).unwrap();
        sink.write(b"hhhh\n").unwrap();
        sink.write(b"iiii\n").unwrap();
        assert_eq!(lines_of(&path), ["iiii"]);
        assert_eq!(
            lines_of(&dir.path().join("events.t.log.1")),
            ["gggg", "hhhh"]
        );
        // A batch is split so rotation falls between writes.
        let mut sink = RotatingSink::open(&dir.path().join("events.b.log"), 10, 3).unwrap();
        let lines: Vec<String> = ["1111\n", "2222\n", "3333\n"].map(String::from).into();
        write_batched(&mut sink, &lines, 10).unwrap();
        assert_eq!(lines_of(&dir.path().join("events.b.log")), ["3333"]);
        assert_eq!(
            lines_of(&dir.path().join("events.b.log.1")),
            ["1111", "2222"]
        );
    }

    fn set_mtime(path: &Path, ms: u64) {
        let file = std::fs::File::options().write(true).open(path).unwrap();
        file.set_modified(UNIX_EPOCH + Duration::from_millis(ms))
            .unwrap();
    }

    #[test]
    fn retention_removes_old_and_excess_files_but_spares_active_ones() {
        let dir = tempfile::tempdir().unwrap();
        let d = dir.path();
        let now = 100 * DAY_MS;
        let file = |name: &str, bytes: usize, age_days: u64| {
            let path = d.join(name);
            std::fs::write(&path, "x".repeat(bytes)).unwrap();
            set_mtime(&path, now - age_days * DAY_MS);
            path
        };
        let old = file("events.old.log", 10, 20);
        let old_active = file("events.active.log", 10, 20);
        let oldest = file("events.a.log.1", 40, 3);
        let older = file("events.a.log", 40, 2);
        let newest = file("events.b.log", 40, 1);
        let other = file("notes.txt", 500, 30);
        let foreign = d.join("other.log");
        std::fs::write(&foreign, "[2025-01-01T00:00:00.000Z] NTIVE: {}\n").unwrap();
        set_mtime(&foreign, now - 30 * DAY_MS);
        let unrelated = file("server.log", 10, 30);
        std::fs::write(&unrelated, "plain text").unwrap();
        set_mtime(&unrelated, now - 30 * DAY_MS);
        let active: HashSet<PathBuf> = [old_active.clone(), older.clone()].into();
        let failures = enforce_retention(d, "events.", 50, 14 * DAY_MS, &active, now);
        assert!(failures.is_empty(), "{failures:?}");
        // Old: removed unless active. Then oldest first until 50 bytes fit,
        // sparing the active file.
        assert!(!old.exists() && !foreign.exists());
        assert!(old_active.exists() && older.exists());
        assert!(!oldest.exists());
        assert!(!newest.exists());
        assert!(other.exists() && unrelated.exists());
    }

    #[test]
    fn the_writer_batches_by_thread_and_retains_on_schedule() {
        let dir = tempfile::tempdir().unwrap();
        let options = Options {
            retention_interval_ms: 1_000,
            max_total_bytes: 1,
            ..Options::default()
        };
        let mut writer = Writer::new(dir.path().to_path_buf(), "events.".into(), options);
        writer.start(0);
        assert!(!writer.push_line("t1".into(), "a\n".into()));
        writer.push_line("_global".into(), "g\n".into());
        writer.push_line("t1".into(), "b\n".into());
        writer.drain(10);
        assert_eq!(lines_of(&dir.path().join("events.t1.log")), ["a", "b"]);
        assert_eq!(lines_of(&dir.path().join("events._global.log")), ["g"]);
        // Retention, once due, spares the files written since the last one,
        // however far over the total they are.
        writer.drain(1_500);
        assert!(dir.path().join("events._global.log").exists());
        writer.push_line("t1".into(), "c\n".into());
        writer.drain(2_000);
        assert!(dir.path().join("events._global.log").exists());
        writer.drain(3_000);
        assert_eq!(lines_of(&dir.path().join("events.t1.log")), ["a", "b", "c"]);
        assert!(!dir.path().join("events._global.log").exists());
    }

    #[tokio::test]
    async fn the_store_writes_lines_off_thread_and_flushes() {
        let dir = tempfile::tempdir().unwrap();
        let log = ProviderEventLog::open(dir.path());
        log.native(json!({"type": "x"}), Some("thread-1"));
        log.native(json!({"type": "content.delta"}), Some("thread-1"));
        let acp = log
            .protocol(
                Protocol::Acp {
                    provider: "grok".into(),
                },
                Some("thread-1"),
            )
            .unwrap();
        acp.line(
            Direction::Outgoing,
            r#"{"jsonrpc":"2.0","id":1,"method":"session/new","params":{"cwd":"/secret"}}"#,
        );
        acp.line(
            Direction::Incoming,
            r#"{"jsonrpc":"2.0","method":"session/update","params":{}}"#,
        );
        acp.line(
            Direction::Incoming,
            r#"{"jsonrpc":"2.0","id":1,"result":{"sessionId":"s"}}"#,
        );
        log.flush().await;
        let lines = lines_of(&dir.path().join("events.thread-1.log"));
        assert_eq!(lines.len(), 3, "{lines:?}");
        assert!(lines[0].ends_with("] NTIVE: {\"type\":\"x\"}"));
        let record = |line: &str| -> Value {
            serde_json::from_str(line.split_once("NTIVE: ").unwrap().1).unwrap()
        };
        let started = record(&lines[1]);
        assert_eq!(
            started["event"]["payload"],
            json!({"method": "session/new", "status": "started",
                "request": {"valueType": "object", "fieldCount": 1}})
        );
        assert_eq!(started["event"]["threadId"], "thread-1");
        assert_eq!(started["event"]["kind"], "request");
        let done = record(&lines[2]);
        assert_eq!(done["event"]["payload"]["status"], "succeeded");
        assert_eq!(
            done["event"]["payload"]["result"],
            json!({"valueType": "object", "fieldCount": 1})
        );
        assert!(!lines.iter().any(|l| l.contains("/secret")));
    }
}
