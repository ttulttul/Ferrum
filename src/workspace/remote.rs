//! A workspace in another process (`t3-agentd`), reached over the workspace
//! protocol (see [`crate::workspace_protocol`]).
//!
//! [`RemoteWorkspace`] does not own a socket. Its host supplies a
//! [`Transport`] that sends frames, feeds received frames to
//! [`RemoteWorkspace::receive`] (then calls [`RemoteWorkspace::flush_acks`]),
//! and reports the connection's life with [`RemoteWorkspace::connected`] and
//! [`RemoteWorkspace::disconnected`]. It also calls
//! [`RemoteWorkspace::maintain`] every few hundred milliseconds, which reopens
//! streams that ended, closes idle watches and forwards turn cancellations.
//! Natively, [`connect`] does all of this over a WebSocket.
//!
//! Until a connection is up, every operation answers as unavailable.
use super::{
    GitTools, Reply, StreamPosition, TurnEvent, TurnItem, TurnItems, TurnSpec, Workspace,
    WorkspaceFuture,
};
use crate::workspace_protocol::{
    self as proto, op, Ack, EndReason, ErrorCode, Frame, Hello, HelloReply, Inbound, Polled,
    Request, Resume, ResumeReply, StreamBody, StreamRef, TurnStart, WireError, WireGitTools,
    WireTurnEvent,
};
use serde_json::{json, Value};
use std::{
    collections::{BTreeMap, HashMap},
    io,
    path::{Path, PathBuf},
    sync::{Arc, Mutex, MutexGuard},
};
use tokio::sync::{broadcast, mpsc, oneshot, watch};

#[cfg(feature = "native")]
mod native;
#[cfg(feature = "native")]
pub use native::{connect, NativeLink};

/// Sends frames to agentd.
pub trait Transport: super::MaybeSendSync {
    /// Queue a frame. An error means the connection is gone.
    fn send(&self, frame: Frame) -> Result<(), WireError>;
}

/// A watch no one has read for this long is closed.
const WATCH_IDLE_MS: u64 = 10_000;

/// The event streams that stay open for as long as a connection does.
const BUS_OPS: [&str; 5] = [
    op::TERM_EVENTS,
    op::TERM_METADATA,
    op::PREVIEW_EVENTS,
    op::DEVICE_EVENTS,
    op::CLONE_EVENTS,
];

/// A workspace served by `t3-agentd`. Clones share one connection.
#[derive(Clone)]
pub struct RemoteWorkspace {
    inner: Arc<Inner>,
}

struct Inner {
    /// The server's epoch, which `hello` presents.
    epoch: u64,
    /// The token `hello` presents, for transports that cannot send it in
    /// the upgrade request.
    token: Option<String>,
    state: Mutex<State>,
    terminal_events: broadcast::Sender<Value>,
    metadata_events: broadcast::Sender<Value>,
    /// This mirror's terminal manager id (`managerId`, #38998): its own, as
    /// each terminal manager has.
    terminal_manager_id: String,
    preview_events: broadcast::Sender<Value>,
    device_events: broadcast::Sender<Value>,
    clone_events: broadcast::Sender<Value>,
    bus_frames: broadcast::Sender<BusFrame>,
}

/// One event of a stream that stays open for as long as the connection
/// does (terminal, terminal metadata, preview and device events), with its
/// place in that stream. A reconnect opens new streams, so `stream` changes
/// after one.
#[derive(Clone, Debug, PartialEq)]
pub struct BusFrame {
    /// The op that opened the stream, such as `term.events`.
    pub op: &'static str,
    pub stream: u64,
    pub seq: u64,
    pub event: Value,
}

#[derive(Default)]
struct State {
    transport: Option<Arc<dyn Transport>>,
    /// Whether `hello` and `resume` have finished on this connection.
    ready: bool,
    /// The workspace root `agentd` reported in `hello`, if connected.
    root: Option<PathBuf>,
    next_id: u64,
    pending: HashMap<u64, Pending>,
    streams: HashMap<u64, Sink>,
    /// The highest sequence number received per stream since the last flush.
    acks: BTreeMap<u64, u64>,
    /// Tracked streams that ended before their consumer acknowledged every
    /// event: the end frame's seq, acknowledged with the last event.
    ending: HashMap<u64, u64>,
    buses: HashMap<&'static str, Bus>,
    /// The terminals as `term.metadata` last described them.
    terminals: Vec<Value>,
    device_state: Value,
    /// The clones as `clone.events` last listed them.
    clones: Vec<Value>,
    watches: HashMap<String, Watch>,
    /// Turn cancellations `maintain` forwards (no spawned task can wait on
    /// them in every build).
    cancels: Vec<(u64, oneshot::Receiver<()>)>,
}

struct Pending {
    reply: oneshot::Sender<Result<Value, WireError>>,
    /// Installed as soon as the reply names a stream, before that stream's
    /// first frame can be handled.
    sink: Option<Sink>,
}

struct Sink {
    /// The last frame received; older ones are duplicates.
    last_seq: u64,
    /// The last frame acknowledged, which a reconnect resumes after.
    acked: u64,
    /// Whether the consumer acknowledges frames itself ([`Workspace::ack`])
    /// rather than on delivery.
    manual: bool,
    kind: SinkKind,
}

impl Sink {
    fn new(kind: SinkKind) -> Self {
        Self {
            last_seq: 0,
            acked: 0,
            manual: false,
            kind,
        }
    }
}

enum SinkKind {
    Turn {
        events: TurnSender,
        exited: bool,
    },
    Bus(&'static str),
    Watch(String),
    /// Frames for a caller that awaits them itself.
    Items(mpsc::UnboundedSender<Item>),
}

/// Where a turn's events go: plain, or with their positions for a consumer
/// that acknowledges them.
enum TurnSender {
    Plain(mpsc::UnboundedSender<TurnEvent>),
    Tracked(mpsc::UnboundedSender<TurnItem>),
}

impl TurnSender {
    fn send(&self, event: TurnEvent, position: Option<StreamPosition>) {
        match self {
            Self::Plain(events) => {
                let _ = events.send(event);
            }
            Self::Tracked(items) => {
                let position = position.filter(|p| p.seq > 0);
                let _ = items.send(TurnItem { event, position });
            }
        }
    }
}

/// A stream's event or end, for [`SinkKind::Items`].
enum Item {
    Event(Value),
    End(EndReason),
}

#[derive(Clone, Copy, Default, PartialEq)]
enum Bus {
    #[default]
    Closed,
    Opening,
    Open(u64),
    /// agentd does not know the op (an older build): not asked again on
    /// this connection.
    Unsupported,
}

struct Watch {
    stream: Option<u64>,
    opening: bool,
    latest: watch::Sender<Option<Polled>>,
    last_read: u64,
}

impl Default for RemoteWorkspace {
    fn default() -> Self {
        Self::new(0, None)
    }
}

/// The defect a workspace RPC answers with while agentd is not connected.
pub fn unavailable(method: &str) -> Value {
    json!({
        "_tag": "Die",
        "defect": format!("RPC contract is registered but unavailable in the control plane: {method}"),
    })
}

fn not_connected() -> WireError {
    WireError::new(ErrorCode::Unavailable, "the workspace is not connected")
}

fn rpc_error(method: &str, error: WireError) -> Value {
    if error.code == ErrorCode::Unavailable {
        unavailable(method)
    } else {
        proto::decode_reply(Err(error), method).unwrap_err()
    }
}

fn routed(result: Result<Value, WireError>, method: &str) -> Option<Reply> {
    match result {
        Err(error) if error.code == ErrorCode::UnknownMethod => None,
        Err(error) => Some(Err(rpc_error(method, error))),
        Ok(value) => Some(Ok(value)),
    }
}

fn reply(result: Result<Value, WireError>, method: &str) -> Reply {
    result.map_err(|error| match error {
        WireError {
            code: ErrorCode::Rpc,
            detail: Some(detail),
            ..
        } => detail,
        error => rpc_error(method, error),
    })
}

fn io_result(result: Result<Value, WireError>) -> io::Result<Value> {
    result.map_err(|error| match error.code {
        ErrorCode::Io => proto::decode_io_error(error),
        _ => io::Error::new(io::ErrorKind::NotConnected, error.message),
    })
}

fn stream_id(value: &Value) -> Option<u64> {
    value["stream"].as_u64()
}

/// Order terminals as `LocalWorkspace` does: newest first.
fn sort_terminals(items: &mut [Value]) {
    items.sort_by(|a, b| {
        b["updatedAt"]
            .as_str()
            .cmp(&a["updatedAt"].as_str())
            .then_with(|| a["threadId"].as_str().cmp(&b["threadId"].as_str()))
            .then_with(|| a["terminalId"].as_str().cmp(&b["terminalId"].as_str()))
    });
}

#[cfg(feature = "native")]
fn boxed<'a, T: Send + 'a>(
    future: impl std::future::Future<Output = T> + Send + 'a,
) -> WorkspaceFuture<'a, T> {
    Box::pin(future)
}
#[cfg(not(feature = "native"))]
fn boxed<'a, T: 'a>(future: impl std::future::Future<Output = T> + 'a) -> WorkspaceFuture<'a, T> {
    Box::pin(future)
}

impl RemoteWorkspace {
    /// A workspace for a server at `epoch`, not yet connected.
    // In the single-threaded WebAssembly build the transport need not be
    // `Send`, so neither is this `Arc`'s contents.
    #[allow(clippy::arc_with_non_send_sync)]
    pub fn new(epoch: u64, token: Option<String>) -> Self {
        Self {
            inner: Arc::new(Inner {
                epoch,
                token,
                state: Mutex::default(),
                terminal_events: broadcast::channel(512).0,
                metadata_events: broadcast::channel(128).0,
                terminal_manager_id: crate::platform::uuid_v4().to_string(),
                preview_events: broadcast::channel(512).0,
                device_events: broadcast::channel(128).0,
                clone_events: broadcast::channel(128).0,
                bus_frames: broadcast::channel(1024).0,
            }),
        }
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        self.inner.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Every event-stream frame as it arrives, for a consumer that keeps its
    /// own position in each stream.
    pub fn bus_frames(&self) -> broadcast::Receiver<BusFrame> {
        self.inner.bus_frames.subscribe()
    }

    /// The stream event-stream op `op` (such as `term.events`) is open as
    /// on the current connection, the one [`bus_frames`](Self::bus_frames)
    /// frames name.
    pub fn bus_stream(&self, op: &str) -> Option<u64> {
        match self.lock().buses.get(op) {
            Some(Bus::Open(stream)) => Some(*stream),
            _ => None,
        }
    }

    /// Whether a connection is up and has finished its handshake.
    pub fn is_ready(&self) -> bool {
        self.lock().ready
    }

    /// The workspace root `agentd` reported in `hello`, if one has connected.
    pub fn root(&self) -> Option<PathBuf> {
        self.lock().root.clone()
    }

    /// Adopt a new connection: say hello, resume every stream this side
    /// still reads, then open the event streams.
    pub async fn connected(&self, transport: Arc<dyn Transport>) -> Result<HelloReply, WireError> {
        {
            let mut state = self.lock();
            state.transport = Some(transport);
            state.ready = false;
        }
        let hello = Hello {
            version: proto::VERSION,
            epoch: self.inner.epoch,
            token: self.inner.token.clone(),
        };
        let hello: HelloReply = serde_json::from_value(
            self.send_request(op::HELLO, json!(hello), None, false)
                .await?,
        )
        .map_err(|e| WireError::new(ErrorCode::Internal, e.to_string()))?;
        let resume = Resume {
            streams: self
                .lock()
                .streams
                .iter()
                .map(|(&id, sink)| (id, sink.acked))
                .collect(),
        };
        let resumed: ResumeReply = serde_json::from_value(
            self.send_request(op::RESUME, json!(resume), None, false)
                .await?,
        )
        .map_err(|e| WireError::new(ErrorCode::Internal, e.to_string()))?;
        {
            let mut state = self.lock();
            for id in resumed.missing {
                if let Some(sink) = state.streams.remove(&id) {
                    end_sink(&mut state, sink, EndReason::Gap);
                }
            }
            if !hello.root.is_empty() {
                state.root = Some(PathBuf::from(&hello.root));
            }
            state.ready = true;
        }
        self.maintain().await;
        Ok(hello)
    }

    /// The connection is gone. Requests in flight fail; streams wait for
    /// the next connection to resume them.
    pub fn disconnected(&self) {
        let mut state = self.lock();
        state.transport = None;
        state.ready = false;
        state.pending.clear();
        state.acks.clear();
        for bus in state.buses.values_mut() {
            if matches!(*bus, Bus::Opening | Bus::Unsupported) {
                *bus = Bus::Closed;
            }
        }
        // Readers waiting for a first value would wait forever.
        state.watches.retain(|_, w| w.latest.borrow().is_some());
        for watch in state.watches.values_mut() {
            watch.opening = false;
        }
    }

    /// Handle a frame from agentd.
    pub fn receive(&self, frame: Frame) {
        let inbound = match proto::decode_inbound(&frame) {
            Ok(inbound) => inbound,
            Err(error) => {
                tracing::warn!(%error, "ignoring a frame the workspace sent");
                return;
            }
        };
        let mut state = self.lock();
        match inbound {
            Inbound::Response(response) => {
                let Some(pending) = state.pending.remove(&response.id) else {
                    return;
                };
                if let (Ok(value), Some(sink)) = (&response.result, pending.sink) {
                    if let Some(id) = stream_id(value) {
                        state.streams.insert(id, sink);
                    }
                }
                let _ = pending.reply.send(response.result);
            }
            Inbound::Stream(frame) => {
                let Some(sink) = state.streams.get_mut(&frame.stream) else {
                    return;
                };
                if frame.seq <= sink.last_seq {
                    return;
                }
                sink.last_seq = frame.seq;
                let manual = sink.manual;
                let all_acked = sink.acked + 1 == frame.seq;
                if !manual {
                    sink.acked = frame.seq;
                }
                let ending = matches!(frame.body, StreamBody::End(_));
                if !manual || (ending && all_acked) {
                    state.acks.insert(frame.stream, frame.seq);
                } else if ending {
                    state.ending.insert(frame.stream, frame.seq);
                }
                match frame.body {
                    StreamBody::Event(event) => {
                        deliver(&self.inner, &mut state, frame.stream, frame.seq, event)
                    }
                    StreamBody::End(end) => {
                        if let Some(sink) = state.streams.remove(&frame.stream) {
                            end_sink(&mut state, sink, end.reason);
                        }
                    }
                }
            }
        }
    }

    /// Acknowledge every stream frame received so far.
    pub fn flush_acks(&self) {
        let mut state = self.lock();
        let acks = std::mem::take(&mut state.acks);
        let Some(transport) = state.transport.clone() else {
            return;
        };
        drop(state);
        for (stream, seq) in acks {
            let _ = transport.send(proto::request_frame(&Request {
                id: None,
                op: op::ACK.into(),
                args: json!(Ack { stream, seq }),
            }));
        }
    }

    /// Reopen event streams that ended, close idle watches and forward turn
    /// cancellations. Hosts call this every few hundred milliseconds.
    pub async fn maintain(&self) {
        let (reopen, idle, cancels) = {
            let mut state = self.lock();
            if !state.ready {
                return;
            }
            let mut reopen = Vec::new();
            for op in BUS_OPS {
                let bus = state.buses.entry(op).or_default();
                if *bus == Bus::Closed {
                    *bus = Bus::Opening;
                    reopen.push(op);
                }
            }
            let now = crate::platform::monotonic_ms();
            let mut idle = Vec::new();
            state.watches.retain(|_, watch| {
                let keep = now.saturating_sub(watch.last_read) < WATCH_IDLE_MS;
                if !keep {
                    idle.extend(watch.stream);
                }
                keep
            });
            let mut cancels = Vec::new();
            state.cancels.retain_mut(|(stream, cancel)| {
                match cancel.try_recv() {
                    Err(oneshot::error::TryRecvError::Empty) => true,
                    Ok(()) => {
                        cancels.push(*stream);
                        false
                    }
                    // Dropped, as when the server that started the turn
                    // stops: the turn keeps running for the next one to
                    // resume.
                    Err(oneshot::error::TryRecvError::Closed) => false,
                }
            });
            (reopen, idle, cancels)
        };
        for stream in idle {
            let _ = self
                .request(op::STREAM_CLOSE, json!(StreamRef { stream }))
                .await;
        }
        for stream in cancels {
            let _ = self.cancel_turn(stream).await;
        }
        for op in reopen {
            let opened = self
                .send_request(op, Value::Null, Some(Sink::new(SinkKind::Bus(op))), true)
                .await;
            let mut state = self.lock();
            let bus = state.buses.entry(op).or_default();
            if *bus == Bus::Opening {
                *bus = match &opened {
                    Ok(value) => stream_id(value).map_or(Bus::Closed, Bus::Open),
                    Err(error) if error.code == ErrorCode::UnknownOp => Bus::Unsupported,
                    Err(_) => Bus::Closed,
                };
            }
        }
    }

    async fn open_turn(
        &self,
        spec: TurnSpec,
        cancel: oneshot::Receiver<()>,
        events: TurnSender,
        manual: bool,
    ) -> anyhow::Result<u64> {
        let mut sink = Sink::new(SinkKind::Turn {
            events,
            exited: false,
        });
        sink.manual = manual;
        let opened = self
            .send_request(
                op::TURN_START,
                json!(TurnStart::from(spec)),
                Some(sink),
                true,
            )
            .await
            .map_err(|e| anyhow::anyhow!(e.message))?;
        let stream = stream_id(&opened)
            .ok_or_else(|| anyhow::anyhow!("the workspace did not name the turn's stream"))?;
        self.forward_cancel(stream, cancel);
        Ok(stream)
    }

    fn forward_cancel(&self, stream: u64, cancel: oneshot::Receiver<()>) {
        // Inside tokio a task forwards it at once; elsewhere `maintain` does.
        #[cfg(feature = "native")]
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            let this = self.clone();
            runtime.spawn(async move {
                if cancel.await.is_ok() {
                    let _ = this.cancel_turn(stream).await;
                }
            });
            return;
        }
        self.lock().cancels.push((stream, cancel));
    }

    /// Read a tracked turn this server started before it restarted, from
    /// just after `position`: the last event it recorded (seq 0 for none).
    /// Events after it arrive again, each once. Call this before
    /// [`RemoteWorkspace::connected`] so the first `resume` names the
    /// stream; called later, it resumes the stream itself. A stream agentd
    /// no longer holds reports an interrupted exit.
    pub async fn resume_turn(
        &self,
        position: StreamPosition,
        cancel: oneshot::Receiver<()>,
    ) -> TurnItems {
        let (items, receiver) = mpsc::unbounded_channel();
        let ready = {
            let mut state = self.lock();
            let mut sink = Sink::new(SinkKind::Turn {
                events: TurnSender::Tracked(items),
                exited: false,
            });
            sink.manual = true;
            sink.last_seq = position.seq;
            sink.acked = position.seq;
            state.streams.insert(position.stream, sink);
            state.ready
        };
        if ready {
            let resume = Resume {
                streams: BTreeMap::from([(position.stream, position.seq)]),
            };
            let missing = match self.request(op::RESUME, json!(resume)).await {
                Ok(reply) => serde_json::from_value::<ResumeReply>(reply)
                    .map(|r| r.missing.contains(&position.stream))
                    .unwrap_or(true),
                // The next connection resumes it.
                Err(_) => false,
            };
            if missing {
                let mut state = self.lock();
                if let Some(sink) = state.streams.remove(&position.stream) {
                    end_sink(&mut state, sink, EndReason::Gap);
                }
            }
        }
        self.forward_cancel(position.stream, cancel);
        TurnItems::tracked(position.stream, receiver)
    }

    /// The last acknowledged position of every tracked stream still open.
    pub fn acked_positions(&self) -> Vec<StreamPosition> {
        let state = self.lock();
        let mut positions: Vec<_> = state
            .streams
            .iter()
            .filter(|(_, sink)| sink.manual)
            .map(|(&stream, sink)| StreamPosition {
                stream,
                seq: sink.acked,
            })
            .collect();
        positions.sort_by_key(|p| p.stream);
        positions
    }

    /// Answer a runtime request of a turn agentd runs.
    pub async fn respond(
        &self,
        turn_id: &str,
        response: crate::workspace::provider::RuntimeResponse,
    ) -> bool {
        self.request(
            op::TURN_RESPOND,
            json!(proto::TurnRespond {
                turn_id: turn_id.to_owned(),
                response,
            }),
        )
        .await
        .is_ok_and(|answered| answered == true)
    }

    async fn cancel_turn(&self, stream: u64) -> Result<Value, WireError> {
        self.request(op::TURN_CANCEL, json!(StreamRef { stream }))
            .await
    }

    /// `agentd`'s idle state (see [`proto::IdleState`]).
    pub async fn idle_state(&self) -> Result<proto::IdleState, WireError> {
        let value = self.request(op::IDLE_STATE, Value::Null).await?;
        serde_json::from_value(value)
            .map_err(|e| WireError::new(ErrorCode::Internal, e.to_string()))
    }

    pub(crate) fn close_asset(&self, id: u64) {
        if let Some(transport) = self.lock().transport.clone() {
            let _ = transport.send(proto::request_frame(&Request {
                id: None,
                op: op::ASSET_CLOSE.into(),
                args: json!({"body":id}),
            }));
        }
    }

    pub(crate) async fn request(&self, op: &str, args: Value) -> Result<Value, WireError> {
        self.send_request(op, args, None, true).await
    }

    async fn send_request(
        &self,
        op: &str,
        args: Value,
        sink: Option<Sink>,
        need_ready: bool,
    ) -> Result<Value, WireError> {
        let (reply, answer) = oneshot::channel();
        let (transport, id) = {
            let mut state = self.lock();
            let transport = match &state.transport {
                Some(transport) if state.ready || !need_ready => transport.clone(),
                _ => return Err(not_connected()),
            };
            state.next_id += 1;
            let id = state.next_id;
            state.pending.insert(id, Pending { reply, sink });
            (transport, id)
        };
        let frame = proto::request_frame(&Request {
            id: Some(id),
            op: op.into(),
            args,
        });
        if let Err(error) = transport.send(frame) {
            self.lock().pending.remove(&id);
            return Err(error);
        }
        answer.await.unwrap_or_else(|_| Err(not_connected()))
    }

    async fn routed(&self, op: &str, args: Value, method: &str) -> Option<Reply> {
        routed(self.request(op, args).await, method)
    }

    async fn method(&self, op: &str, method: &str, input: &Value, rpc: &str) -> Option<Reply> {
        let args = json!({"method": method, "input": input});
        routed(self.request(op, args).await, &format!("{rpc}.{method}"))
    }

    /// The latest reading of a watch, opening it on first use. agentd polls
    /// and pushes changes, so repeated reads cost no round trip.
    async fn polled(&self, op: &'static str, args: Value, method: &str) -> Option<Reply> {
        let key = format!("{op} {args}");
        let (open, mut latest) = {
            let mut state = self.lock();
            let ready = state.ready;
            let watch = state.watches.entry(key.clone()).or_insert_with(|| Watch {
                stream: None,
                opening: false,
                latest: watch::channel(None).0,
                last_read: 0,
            });
            watch.last_read = crate::platform::monotonic_ms();
            let open = ready && watch.stream.is_none() && !watch.opening;
            if open {
                watch.opening = true;
            }
            let latest = watch.latest.subscribe();
            if !open {
                if let Some(value) = latest.borrow().clone() {
                    return value.into_routed();
                }
            }
            if !ready && latest.borrow().is_none() {
                state.watches.remove(&key);
                return Some(Err(unavailable(method)));
            }
            (open, latest)
        };
        if open {
            let sink = Sink::new(SinkKind::Watch(key.clone()));
            let opened = self.send_request(op, args, Some(sink), true).await;
            let mut state = self.lock();
            if let Some(watch) = state.watches.get_mut(&key) {
                watch.opening = false;
                watch.stream = opened.as_ref().ok().and_then(stream_id);
            }
            if let Err(error) = opened {
                if latest.borrow().is_none() {
                    state.watches.remove(&key);
                }
                drop(state);
                return Some(Err(rpc_error(method, error)));
            }
        }
        loop {
            if let Some(value) = latest.borrow_and_update().clone() {
                return value.into_routed();
            }
            if latest.changed().await.is_err() {
                return Some(Err(unavailable(method)));
            }
        }
    }
}

/// Hand a stream event to whoever reads that stream.
fn deliver(inner: &Inner, state: &mut State, stream: u64, seq: u64, event: Value) {
    enum Target {
        Bus(&'static str),
        Watch(String),
    }
    let target = match state.streams.get_mut(&stream).map(|sink| &mut sink.kind) {
        None => return,
        Some(SinkKind::Turn { events, exited }) => {
            match serde_json::from_value::<WireTurnEvent>(event) {
                Ok(event) => {
                    let event = TurnEvent::from(event);
                    *exited |= matches!(event, TurnEvent::Exited(_));
                    events.send(event, Some(StreamPosition { stream, seq }));
                }
                Err(error) => tracing::warn!(%error, "ignoring a malformed turn event"),
            }
            return;
        }
        Some(SinkKind::Items(items)) => {
            let _ = items.send(Item::Event(event));
            return;
        }
        Some(SinkKind::Bus(op)) => Target::Bus(op),
        Some(SinkKind::Watch(key)) => Target::Watch(key.clone()),
    };
    match target {
        Target::Bus(op) => {
            let _ = inner.bus_frames.send(BusFrame {
                op,
                stream,
                seq,
                event: event.clone(),
            });
            deliver_bus(inner, state, op, event)
        }
        Target::Watch(key) => {
            if let (Some(watch), Ok(reading)) = (
                state.watches.get(&key),
                serde_json::from_value::<Polled>(event),
            ) {
                watch.latest.send_replace(Some(reading));
            }
        }
    }
}

fn deliver_bus(inner: &Inner, state: &mut State, op: &'static str, event: Value) {
    match op {
        op::TERM_EVENTS => {
            let _ = inner.terminal_events.send(event);
        }
        op::TERM_METADATA => match event["type"].as_str() {
            Some("snapshot") => {
                state.terminals = event["terminals"].as_array().cloned().unwrap_or_default();
            }
            Some("upsert") => {
                let terminal = &event["terminal"];
                state.terminals.retain(|t| {
                    t["threadId"] != terminal["threadId"]
                        || t["terminalId"] != terminal["terminalId"]
                });
                state.terminals.push(terminal.clone());
                sort_terminals(&mut state.terminals);
                let _ = inner.metadata_events.send(event);
            }
            Some("remove") => {
                state.terminals.retain(|t| {
                    t["threadId"] != event["threadId"] || t["terminalId"] != event["terminalId"]
                });
                let _ = inner.metadata_events.send(event);
            }
            _ => {
                let _ = inner.metadata_events.send(event);
            }
        },
        op::PREVIEW_EVENTS => {
            let _ = inner.preview_events.send(event);
        }
        op::DEVICE_EVENTS if state.device_state != event => {
            state.device_state = event.clone();
            let _ = inner.device_events.send(event);
        }
        op::CLONE_EVENTS => {
            if let Some(clones) = event.as_array() {
                if state.clones != *clones {
                    state.clones = clones.clone();
                    let _ = inner.clone_events.send(event);
                }
            }
        }
        _ => {}
    }
}

/// A stream ended or was lost.
fn end_sink(state: &mut State, sink: Sink, reason: EndReason) {
    match sink.kind {
        // A turn that ends without exiting (a gap, an error, a lost stream)
        // was interrupted.
        SinkKind::Turn { events, exited } => {
            if !exited {
                events.send(proto::interrupted_turn(), None);
            }
        }
        SinkKind::Items(items) => {
            let _ = items.send(Item::End(reason));
        }
        SinkKind::Bus(op) => {
            state.buses.insert(op, Bus::Closed);
        }
        SinkKind::Watch(key) => {
            if let Some(watch) = state.watches.get_mut(&key) {
                watch.stream = None;
            }
        }
    }
}

impl RemoteWorkspace {
    /// An attachment of agentd's own, through the asset operations it already
    /// serves: a grant for it (`asset.prepare`), its response opened
    /// (`asset.open`), and its body pulled a chunk at a time (`asset.read`).
    async fn pull_attachment(&self, id: &str, max_bytes: u64) -> Result<Vec<u8>, String> {
        use base64::{engine::general_purpose::STANDARD, Engine};
        let failed = |what: &str| format!("could not {what} the attachment {id} at the workspace");
        let grant = self
            .request(
                op::ASSET_PREPARE,
                json!({"resource":{"_tag":"attachment","attachmentId":id}}),
            )
            .await
            .map_err(|_| failed("prepare"))?;
        let opened = self
            .request(
                op::ASSET_OPEN,
                json!({"claims":grant["claims"],"name":grant["name"],"method":"GET","headers":{}}),
            )
            .await
            .map_err(|_| failed("open"))?;
        let body = opened["body"].as_u64();
        // agentd releases the body once read to its end; close it otherwise.
        struct Open<'a>(&'a RemoteWorkspace, Option<u64>);
        impl Drop for Open<'_> {
            fn drop(&mut self) {
                if let Some(id) = self.1 {
                    self.0.close_asset(id);
                }
            }
        }
        let mut open = Open(self, body);
        if opened["status"] != 200 {
            return Err(failed("find"));
        }
        let mut bytes = Vec::new();
        if let Some(body) = body {
            loop {
                let chunk = self
                    .request(op::ASSET_READ, json!({"body":body}))
                    .await
                    .map_err(|_| failed("read"))?;
                let Some(chunk) = chunk.as_str() else {
                    break;
                };
                bytes.extend(STANDARD.decode(chunk).map_err(|_| failed("read"))?);
                if bytes.len() as u64 > max_bytes {
                    return Err(format!(
                        "the attachment {id} is larger than {max_bytes} bytes"
                    ));
                }
            }
        }
        open.1 = None;
        Ok(bytes)
    }
}

impl Workspace for RemoteWorkspace {
    fn fetch_attachment<'a>(
        &'a self,
        id: &'a str,
        max_bytes: u64,
    ) -> WorkspaceFuture<'a, Option<Result<Vec<u8>, String>>> {
        Box::pin(async move { Some(self.pull_attachment(id, max_bytes).await) })
    }
    fn list_entries<'a>(&'a self, input: &'a Value) -> WorkspaceFuture<'a, Option<Reply>> {
        boxed(self.routed(op::FS_LIST_ENTRIES, input.clone(), "projects.listEntries"))
    }
    fn list_ignored_files<'a>(&'a self, input: &'a Value) -> WorkspaceFuture<'a, Option<Reply>> {
        boxed(self.routed(
            op::FS_LIST_IGNORED_FILES,
            input.clone(),
            "projects.listIgnoredFiles",
        ))
    }
    fn search_entries<'a>(&'a self, input: &'a Value) -> WorkspaceFuture<'a, Option<Reply>> {
        boxed(self.routed(
            op::FS_SEARCH_ENTRIES,
            input.clone(),
            "projects.searchEntries",
        ))
    }
    fn search_contents<'a>(&'a self, input: &'a Value) -> WorkspaceFuture<'a, Option<Reply>> {
        boxed(self.routed(
            op::FS_SEARCH_CONTENTS,
            input.clone(),
            "projects.searchContents",
        ))
    }
    fn read_file<'a>(&'a self, input: &'a Value) -> WorkspaceFuture<'a, Option<Reply>> {
        boxed(self.routed(op::FS_READ_FILE, input.clone(), "projects.readFile"))
    }
    fn write_file<'a>(&'a self, input: &'a Value) -> WorkspaceFuture<'a, Option<Reply>> {
        boxed(self.routed(op::FS_WRITE_FILE, input.clone(), "projects.writeFile"))
    }
    fn file_metadata<'a>(&'a self, input: &'a Value) -> WorkspaceFuture<'a, Option<Reply>> {
        boxed(self.routed(op::FS_METADATA, input.clone(), "filesystem.getMetadata"))
    }
    fn browse<'a>(&'a self, input: &'a Value) -> WorkspaceFuture<'a, Option<Reply>> {
        boxed(self.routed(op::FS_BROWSE, input.clone(), "filesystem.browse"))
    }

    fn vcs<'a>(
        &'a self,
        method: &'a str,
        input: &'a Value,
        fetch_upstream: bool,
    ) -> WorkspaceFuture<'a, Option<Reply>> {
        let args = json!({"method": method, "input": input, "fetchUpstream": fetch_upstream});
        boxed(async move {
            self.routed(op::GIT_VCS, args, &format!("vcs.{method}"))
                .await
        })
    }
    fn vcs_status<'a>(
        &'a self,
        input: &'a Value,
        fetch_upstream: bool,
        _initial: bool,
    ) -> WorkspaceFuture<'a, Option<Reply>> {
        let args = json!({"input": input, "fetchUpstream": fetch_upstream});
        boxed(self.polled(op::WATCH_VCS_STATUS, args, "subscribeVcsStatus"))
    }
    fn vcs_branch<'a>(&'a self, input: &'a Value) -> WorkspaceFuture<'a, Reply> {
        boxed(async move {
            self.polled(
                op::WATCH_VCS_BRANCH,
                json!({"input": input}),
                "subscribeVcsBranch",
            )
            .await
            .unwrap_or_else(|| Err(unavailable("subscribeVcsBranch")))
        })
    }
    fn review<'a>(&'a self, method: &'a str, input: &'a Value) -> WorkspaceFuture<'a, Reply> {
        boxed(async move {
            let args = json!({"method": method, "input": input});
            reply(
                self.request(op::GIT_REVIEW, args).await,
                &format!("review.{method}"),
            )
        })
    }
    fn capture_git_tree<'a>(&'a self, cwd: &'a str) -> WorkspaceFuture<'a, Option<String>> {
        boxed(async move {
            self.request(op::GIT_CAPTURE_TREE, json!({"cwd": cwd}))
                .await
                .ok()
                .and_then(|v| v.as_str().map(str::to_owned))
        })
    }
    fn pull_request<'a>(
        &'a self,
        method: &'a str,
        input: &'a Value,
        tools: &'a GitTools,
    ) -> WorkspaceFuture<'a, Reply> {
        boxed(async move {
            let args =
                json!({"method": method, "input": input, "tools": WireGitTools::from(tools)});
            reply(
                self.request(op::GIT_PULL_REQUEST, args).await,
                &format!("git.{method}"),
            )
        })
    }
    fn stacked_action<'a>(
        &'a self,
        input: &'a Value,
        tools: &'a GitTools,
        progress: mpsc::Sender<Value>,
    ) -> WorkspaceFuture<'a, Reply> {
        boxed(async move {
            let method = "git.runStackedAction";
            let (items, mut received) = mpsc::unbounded_channel();
            let sink = Sink::new(SinkKind::Items(items));
            let args = json!({"input": input, "tools": WireGitTools::from(tools)});
            if let Err(error) = self
                .send_request(op::GIT_STACKED_ACTION, args, Some(sink), true)
                .await
            {
                return Err(rpc_error(method, error));
            }
            // Progress waits for its reader, as it does in-process.
            while let Some(item) = received.recv().await {
                match item {
                    Item::Event(event) => {
                        if let Some(batch) = event.get("progress") {
                            let _ = progress.send(batch.clone()).await;
                        } else if let Ok(reading) =
                            serde_json::from_value::<Polled>(event["reply"].clone())
                        {
                            return reading
                                .into_routed()
                                .unwrap_or_else(|| Err(unavailable(method)));
                        }
                    }
                    Item::End(reason) => {
                        let message =
                            format!("the action's stream ended ({reason:?}) without a reply");
                        return Err(rpc_error(
                            method,
                            WireError::new(ErrorCode::Internal, message),
                        ));
                    }
                }
            }
            Err(unavailable(method))
        })
    }

    fn terminal<'a>(
        &'a self,
        method: &'a str,
        input: &'a Value,
    ) -> WorkspaceFuture<'a, Option<Reply>> {
        boxed(self.method(op::TERM_CALL, method, input, "terminal"))
    }
    fn terminal_events(&self) -> broadcast::Receiver<Value> {
        self.inner.terminal_events.subscribe()
    }
    fn terminal_metadata(&self) -> Value {
        json!({"type": "snapshot", "terminals": self.lock().terminals,
            "managerId": self.inner.terminal_manager_id})
    }
    fn clone_call<'a>(
        &'a self,
        method: &'a str,
        input: &'a Value,
    ) -> WorkspaceFuture<'a, Option<Reply>> {
        boxed(self.method(op::CLONE_CALL, method, input, "projectClone"))
    }
    fn clone_snapshots(&self) -> Value {
        Value::Array(self.lock().clones.clone())
    }
    fn clone_events(&self) -> broadcast::Receiver<Value> {
        self.inner.clone_events.subscribe()
    }
    fn terminal_metadata_events(&self) -> broadcast::Receiver<Value> {
        self.inner.metadata_events.subscribe()
    }

    fn start_turn(
        &self,
        spec: TurnSpec,
        cancel: oneshot::Receiver<()>,
    ) -> WorkspaceFuture<'_, anyhow::Result<mpsc::UnboundedReceiver<TurnEvent>>> {
        boxed(async move {
            let (events, receiver) = mpsc::unbounded_channel();
            self.open_turn(spec, cancel, TurnSender::Plain(events), false)
                .await?;
            Ok(receiver)
        })
    }
    fn start_tracked_turn(
        &self,
        spec: TurnSpec,
        cancel: oneshot::Receiver<()>,
    ) -> WorkspaceFuture<'_, anyhow::Result<TurnItems>> {
        boxed(async move {
            let (items, receiver) = mpsc::unbounded_channel();
            let stream = self
                .open_turn(spec, cancel, TurnSender::Tracked(items), true)
                .await?;
            Ok(TurnItems::tracked(stream, receiver))
        })
    }
    fn respond_turn<'a>(
        &'a self,
        turn_id: &'a str,
        response: crate::workspace::provider::RuntimeResponse,
    ) -> WorkspaceFuture<'a, bool> {
        boxed(async move { self.respond(turn_id, response).await })
    }
    fn provider_models<'a>(
        &'a self,
        spec: TurnSpec,
    ) -> WorkspaceFuture<'a, anyhow::Result<Vec<Value>>> {
        boxed(async move {
            let models = self
                .request(op::PROVIDER_MODELS, json!(TurnStart::from(spec)))
                .await
                .map_err(|e| anyhow::anyhow!(e.message))?;
            Ok(models.as_array().cloned().unwrap_or_default())
        })
    }
    fn ack(&self, position: StreamPosition) {
        let (transport, seq) = {
            let mut state = self.lock();
            if let Some(sink) = state.streams.get_mut(&position.stream) {
                sink.acked = sink.acked.max(position.seq.min(sink.last_seq));
            }
            let mut seq = position.seq;
            // The last event of an ended stream: acknowledge its end too.
            if state.ending.get(&position.stream) == Some(&(seq + 1)) {
                state.ending.remove(&position.stream);
                seq += 1;
            }
            (state.transport.clone(), seq)
        };
        // Without a connection, the next `resume` carries the position.
        if let Some(transport) = transport {
            let _ = transport.send(proto::request_frame(&Request {
                id: None,
                op: op::ACK.into(),
                args: json!(Ack {
                    stream: position.stream,
                    seq
                }),
            }));
        }
    }
    fn local_servers<'a>(&'a self, input: &'a Value) -> WorkspaceFuture<'a, Reply> {
        boxed(async move {
            let method = "subscribeDiscoveredLocalServers";
            self.polled(op::WATCH_LOCAL_SERVERS, json!({"input": input}), method)
                .await
                .unwrap_or_else(|| Err(unavailable(method)))
        })
    }
    fn stop_local_server<'a>(&'a self, input: &'a Value) -> WorkspaceFuture<'a, Reply> {
        boxed(async move {
            reply(
                self.request(op::PROC_STOP_LOCAL_SERVER, json!({"input": input}))
                    .await,
                "preview.stopLocalServer",
            )
        })
    }

    fn preview<'a>(
        &'a self,
        method: &'a str,
        input: &'a Value,
    ) -> WorkspaceFuture<'a, Option<Reply>> {
        boxed(self.method(op::PREVIEW_CALL, method, input, "preview"))
    }
    fn preview_profile<'a>(
        &'a self,
        method: &'a str,
        input: &'a Value,
    ) -> WorkspaceFuture<'a, Reply> {
        boxed(async move {
            let args = json!({"method": method, "input": input});
            reply(
                self.request(op::PREVIEW_PROFILE, args).await,
                &format!("preview.{method}"),
            )
        })
    }
    fn close_preview_pages<'a>(
        &'a self,
        thread: Option<&'a str>,
        tab: Option<&'a str>,
        profile: Option<&'a str>,
    ) -> WorkspaceFuture<'a, ()> {
        boxed(async move {
            let args = json!({"thread": thread, "tab": tab, "profile": profile});
            let _ = self.request(op::PREVIEW_CLOSE_PAGES, args).await;
        })
    }
    fn preview_events(&self) -> broadcast::Receiver<Value> {
        self.inner.preview_events.subscribe()
    }

    fn device<'a>(
        &'a self,
        method: &'a str,
        input: &'a Value,
    ) -> WorkspaceFuture<'a, Option<Reply>> {
        boxed(self.method(op::DEVICE_CALL, method, input, "device"))
    }
    fn device_state(&self) -> Value {
        self.lock().device_state.clone()
    }
    fn device_events(&self) -> broadcast::Receiver<Value> {
        self.inner.device_events.subscribe()
    }

    fn create_dir_all<'a>(&'a self, path: &'a Path) -> WorkspaceFuture<'a, io::Result<()>> {
        boxed(async move {
            io_result(
                self.request(op::FS_CREATE_DIR_ALL, json!({"path": path}))
                    .await,
            )
            .map(|_| ())
        })
    }
    fn canonicalize<'a>(&'a self, path: &'a Path) -> WorkspaceFuture<'a, io::Result<PathBuf>> {
        boxed(async move {
            let value = io_result(
                self.request(op::FS_CANONICALIZE, json!({"path": path}))
                    .await,
            )?;
            value
                .as_str()
                .map(PathBuf::from)
                .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "not a path"))
        })
    }
    fn create_named_project<'a>(
        &'a self,
        root: &'a Path,
        name: &'a str,
    ) -> WorkspaceFuture<'a, io::Result<super::NamedProject>> {
        boxed(async move {
            let value = io_result(
                self.request(
                    op::FS_CREATE_NAMED_PROJECT,
                    json!({"rootPath": root, "name": name}),
                )
                .await,
            )?;
            serde_json::from_value(value).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
        })
    }
    fn is_dir<'a>(&'a self, path: &'a Path) -> WorkspaceFuture<'a, bool> {
        boxed(async move {
            self.request(op::FS_IS_DIR, json!({"path": path}))
                .await
                .ok()
                .and_then(|v| v.as_bool())
                .unwrap_or(false)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn every_rpc_answers_unavailable_until_connected() {
        let workspace = RemoteWorkspace::default();
        let input = json!({"cwd":"/workspace"});
        assert_eq!(
            workspace.read_file(&input).await,
            Some(Err(unavailable("projects.readFile")))
        );
        assert_eq!(
            workspace.vcs("refreshStatus", &input, true).await,
            Some(Err(unavailable("vcs.refreshStatus")))
        );
        assert_eq!(
            workspace.vcs_branch(&input).await,
            Err(unavailable("subscribeVcsBranch"))
        );
        assert_eq!(
            unavailable("vcs.refreshStatus")["defect"],
            "RPC contract is registered but unavailable in the control plane: vcs.refreshStatus"
        );
        assert!(workspace
            .start_turn(
                TurnSpec {
                    command: vec!["true".into()],
                    prompt: String::new(),
                    cwd: "/".into(),
                    env_remove: vec![],
                    ..TurnSpec::default()
                },
                oneshot::channel().1,
            )
            .await
            .is_err());
        let metadata = workspace.terminal_metadata();
        assert_eq!(metadata["terminals"], json!([]));
        assert!(metadata["managerId"].is_string());
    }

    /// A transport whose frames the test reads and answers by hand, as a
    /// host without a socket of its own would.
    struct Queue(std::sync::Mutex<Vec<Frame>>);
    impl Transport for Queue {
        fn send(&self, frame: Frame) -> Result<(), WireError> {
            self.0.lock().unwrap().push(frame);
            Ok(())
        }
    }

    fn take(queue: &Queue) -> Vec<Request> {
        std::mem::take(&mut *queue.0.lock().unwrap())
            .iter()
            .map(|f| proto::decode_request(f).unwrap())
            .collect()
    }

    /// Let spawned tasks run until they send something.
    async fn settle(queue: &Queue) -> Vec<Request> {
        for _ in 0..100 {
            tokio::task::yield_now().await;
            let requests = take(queue);
            if !requests.is_empty() {
                return requests;
            }
        }
        panic!("nothing was sent")
    }

    fn answer(workspace: &RemoteWorkspace, id: Option<u64>, ok: Value) {
        workspace.receive(proto::inbound_frame(&Inbound::Response(proto::Response {
            id: id.unwrap(),
            result: Ok(ok),
        })));
    }

    fn event(workspace: &RemoteWorkspace, stream: u64, seq: u64, body: StreamBody) {
        workspace.receive(proto::inbound_frame(&Inbound::Stream(proto::StreamFrame {
            stream,
            seq,
            body,
        })));
    }

    #[tokio::test]
    async fn a_host_fed_queue_carries_a_turn() {
        let workspace = RemoteWorkspace::new(4, Some("t".into()));
        let queue = Arc::new(Queue(Default::default()));
        let handshake = {
            let workspace = workspace.clone();
            let queue = queue.clone();
            tokio::spawn(async move { workspace.connected(queue).await })
        };
        let hello = settle(&queue).await;
        assert_eq!(hello[0].op, "hello");
        assert_eq!(
            hello[0].args,
            json!({"version": proto::VERSION, "epoch": 4, "token": "t"})
        );
        answer(
            &workspace,
            hello[0].id,
            json!({"version": proto::VERSION, "epoch": 4, "root": "/", "streams": []}),
        );
        let resume = settle(&queue).await;
        assert_eq!(resume[0].op, "resume");
        answer(&workspace, resume[0].id, json!({"missing": []}));
        tokio::task::yield_now().await;
        // The event streams open once the handshake is done.
        for (i, stream) in (10..15).enumerate() {
            let opens = settle(&queue).await;
            assert_eq!(opens.len(), 1);
            assert_eq!(opens[0].op, BUS_OPS[i]);
            answer(&workspace, opens[0].id, json!({"stream": stream}));
        }
        handshake.await.unwrap().unwrap();
        assert!(workspace.is_ready());

        let (cancel, cancelled) = oneshot::channel();
        let turn = {
            let workspace = workspace.clone();
            tokio::spawn(async move {
                workspace
                    .start_turn(
                        TurnSpec {
                            command: vec!["provider".into()],
                            prompt: "hi".into(),
                            cwd: "/".into(),
                            env_remove: vec![],
                            ..TurnSpec::default()
                        },
                        cancelled,
                    )
                    .await
            })
        };
        let start = settle(&queue).await;
        assert_eq!(start[0].op, op::TURN_START);
        answer(&workspace, start[0].id, json!({"stream": 20}));
        // The stream's frames may follow the reply before the caller runs.
        let stdout = json!(WireTurnEvent::Stdout { text: "a\n".into() });
        event(&workspace, 20, 1, StreamBody::Event(stdout.clone()));
        event(&workspace, 20, 1, StreamBody::Event(stdout));
        let mut events = turn.await.unwrap().unwrap();
        assert_eq!(events.recv().await, Some(TurnEvent::Stdout("a\n".into())));
        workspace.flush_acks();
        let acks = take(&queue);
        assert_eq!(acks[0].op, "ack");
        assert_eq!(acks[0].args, json!({"stream": 20, "seq": 1}));
        // A gap ends the turn as interrupted.
        event(
            &workspace,
            20,
            2,
            StreamBody::End(proto::StreamEnd {
                reason: EndReason::Gap,
                error: None,
            }),
        );
        assert_eq!(events.recv().await, Some(proto::interrupted_turn()));
        drop(cancel);
    }

    #[tokio::test]
    async fn an_agentd_without_clone_events_is_not_asked_again() {
        let workspace = RemoteWorkspace::new(1, None);
        let queue = Arc::new(Queue(Default::default()));
        let handshake = {
            let workspace = workspace.clone();
            let queue = queue.clone();
            tokio::spawn(async move { workspace.connected(queue).await })
        };
        let hello = settle(&queue).await;
        answer(
            &workspace,
            hello[0].id,
            json!({"version": proto::VERSION, "epoch": 1, "root": "/", "streams": []}),
        );
        let resume = settle(&queue).await;
        answer(&workspace, resume[0].id, json!({"missing": []}));
        for stream in 10..14 {
            let opens = settle(&queue).await;
            answer(&workspace, opens[0].id, json!({"stream": stream}));
        }
        // An agentd older than projectClone does not know the op.
        let opens = settle(&queue).await;
        assert_eq!(opens[0].op, op::CLONE_EVENTS);
        workspace.receive(proto::inbound_frame(&Inbound::Response(proto::Response {
            id: opens[0].id.unwrap(),
            result: Err(WireError::new(
                ErrorCode::UnknownOp,
                "unknown op: clone.events",
            )),
        })));
        handshake.await.unwrap().unwrap();
        workspace.maintain().await;
        assert!(take(&queue).is_empty(), "the op is not asked for again");
        assert_eq!(workspace.clone_snapshots(), json!([]));
    }

    #[tokio::test]
    async fn only_a_sent_cancellation_stops_a_turn() {
        let workspace = RemoteWorkspace::new(1, None);
        let queue = Arc::new(Queue(Default::default()));
        let handshake = {
            let workspace = workspace.clone();
            let queue = queue.clone();
            tokio::spawn(async move { workspace.connected(queue).await })
        };
        let hello = settle(&queue).await;
        answer(
            &workspace,
            hello[0].id,
            json!({"version": proto::VERSION, "epoch": 1, "root": "/", "streams": []}),
        );
        let resume = settle(&queue).await;
        answer(&workspace, resume[0].id, json!({"missing": []}));
        for stream in 10..15 {
            let opens = settle(&queue).await;
            answer(&workspace, opens[0].id, json!({"stream": stream}));
        }
        handshake.await.unwrap().unwrap();
        let spec = TurnSpec {
            command: vec!["provider".into()],
            prompt: String::new(),
            cwd: "/".into(),
            ..Default::default()
        };
        let mut cancels = Vec::new();
        for stream in [20, 21] {
            let (cancel, cancelled) = oneshot::channel();
            cancels.push(cancel);
            let turn = {
                let workspace = workspace.clone();
                let spec = spec.clone();
                tokio::spawn(async move { workspace.start_turn(spec, cancelled).await })
            };
            let start = settle(&queue).await;
            answer(&workspace, start[0].id, json!({"stream": stream}));
            turn.await.unwrap().unwrap();
        }
        // A server that stops drops its senders; its turns keep running
        // for the server that resumes them.
        let sent = cancels.remove(1);
        drop(cancels);
        for _ in 0..20 {
            tokio::task::yield_now().await;
        }
        assert!(take(&queue).is_empty(), "a dropped sender cancels nothing");
        sent.send(()).unwrap();
        let cancel = settle(&queue).await;
        assert_eq!(cancel[0].op, op::TURN_CANCEL);
        assert_eq!(cancel[0].args, json!({"stream": 21}));
    }
}
