//! `t3-agentd` serves a [`Workspace`] over the workspace protocol (see
//! [`t3code_server::workspace_protocol`]): one WebSocket at a time, with
//! token auth, epochs, streams whose frames survive a disconnect until
//! acknowledged, and watches that poll VCS and local-server state here and
//! push only changes.
use axum::{
    extract::{
        ws::{Message, WebSocket},
        State, WebSocketUpgrade,
    },
    http::{header::AUTHORIZATION, HeaderMap, StatusCode},
    response::{IntoResponse, Response as HttpResponse},
    routing::get,
    Router,
};
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use t3code_server::{
    workspace::{TurnEvent, Workspace},
    workspace_protocol::{
        self as proto, op, Ack, EndReason, ErrorCode, Frame, Hello, HelloReply, IdleState, Inbound,
        Polled, Request, Resume, ResumeReply, StreamBody, StreamEnd, StreamFrame, StreamInfo,
        StreamRef, TurnStart, WireError, WireTurnEvent,
    },
};
use tokio::{
    sync::{broadcast, mpsc, oneshot},
    task::AbortHandle,
};

mod buffer;
mod confine;
use buffer::DiskBuffer;
pub use confine::Confinement;

/// The largest frame agentd accepts.
const MAX_MESSAGE_BYTES: usize = 256 * 1024 * 1024;

pub struct Config {
    /// The bearer token a server must present.
    pub token: String,
    /// The directory agentd serves projects under, reported by `hello`.
    /// Operations may not name a path outside it or `extra_roots`.
    pub root: PathBuf,
    /// Other directories operations may use, such as where worktrees are
    /// created.
    pub extra_roots: Vec<PathBuf>,
    /// Where stream buffers live. agentd empties it at start-up.
    pub buffer_dir: PathBuf,
    /// The most bytes of unacknowledged frames one stream may hold.
    pub buffer_limit: u64,
    /// How often watches re-read polled state.
    pub poll_interval: Duration,
}

pub struct Agentd {
    workspace: Arc<dyn Workspace>,
    config: Config,
    confinement: Confinement,
    state: Mutex<SessionState>,
}

struct SessionState {
    max_epoch: u64,
    /// The connection stream frames go to: the last to say hello.
    current: Option<Conn>,
    next_conn: u64,
    next_stream: u64,
    streams: HashMap<u64, StreamState>,
    last_activity: Instant,
}

#[derive(Clone)]
struct Conn {
    id: u64,
    tx: mpsc::UnboundedSender<Frame>,
}

struct StreamState {
    op: String,
    buffer: DiskBuffer,
    last_seq: u64,
    acked_seq: u64,
    ended: bool,
    /// The connection that opened or resumed the stream.
    conn: u64,
    /// Held for a turn; dropping it stops the process.
    cancel: Option<oneshot::Sender<()>>,
    task: Option<AbortHandle>,
    turn_running: bool,
}

impl Drop for StreamState {
    fn drop(&mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

struct Session {
    id: u64,
    tx: mpsc::UnboundedSender<Frame>,
    authorized: bool,
    epoch: Option<u64>,
}

type OpResult = Result<Value, WireError>;

fn bad_request(message: impl Into<String>) -> WireError {
    WireError::new(ErrorCode::BadRequest, message)
}

fn parse<T: serde::de::DeserializeOwned>(args: &Value) -> Result<T, WireError> {
    serde_json::from_value(args.clone()).map_err(|e| bad_request(e.to_string()))
}

fn str_arg<'a>(args: &'a Value, key: &str) -> Result<&'a str, WireError> {
    args[key]
        .as_str()
        .ok_or_else(|| bad_request(format!("`{key}` must be a string")))
}

impl Agentd {
    pub fn new(workspace: Arc<dyn Workspace>, config: Config) -> anyhow::Result<Arc<Self>> {
        if config.token.is_empty() {
            anyhow::bail!("the token is empty");
        }
        if config.buffer_dir.exists() {
            std::fs::remove_dir_all(&config.buffer_dir)?;
        }
        std::fs::create_dir_all(&config.buffer_dir)?;
        let confinement = Confinement::new(
            std::iter::once(config.root.clone()).chain(config.extra_roots.iter().cloned()),
        )?;
        Ok(Arc::new(Self {
            workspace,
            config,
            confinement,
            state: Mutex::new(SessionState {
                max_epoch: 0,
                current: None,
                next_conn: 1,
                next_stream: 1,
                streams: HashMap::new(),
                last_activity: Instant::now(),
            }),
        }))
    }

    /// The HTTP routes: a WebSocket upgrade at `/`.
    pub fn router(self: &Arc<Self>) -> Router {
        Router::new()
            .route("/", get(upgrade))
            .with_state(self.clone())
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, SessionState> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    async fn serve(self: Arc<Self>, socket: WebSocket, authorized: bool) {
        let (mut sink, mut incoming) = socket.split();
        let (tx, mut rx) = mpsc::unbounded_channel::<Frame>();
        let writer = tokio::spawn(async move {
            while let Some(frame) = rx.recv().await {
                let message = match frame {
                    Frame::Text(text) => Message::Text(text),
                    Frame::Binary(bytes) => Message::Binary(bytes),
                };
                if sink.send(message).await.is_err() {
                    break;
                }
            }
            let _ = sink.close().await;
        });
        let id = {
            let mut state = self.lock();
            state.next_conn += 1;
            state.next_conn
        };
        let mut session = Session {
            id,
            tx,
            authorized,
            epoch: None,
        };
        while let Some(Ok(message)) = incoming.next().await {
            let frame = match message {
                Message::Text(text) => Frame::Text(text),
                Message::Binary(bytes) => Frame::Binary(bytes),
                Message::Close(_) => break,
                _ => continue,
            };
            match proto::decode_request(&frame) {
                Ok(request) => self.handle(&mut session, request).await,
                Err(error) => tracing::warn!(%error, "ignoring a frame that is not a request"),
            }
        }
        {
            let mut state = self.lock();
            if state.current.as_ref().is_some_and(|c| c.id == id) {
                state.current = None;
            }
        }
        drop(session);
        let _ = writer.await;
    }

    fn reply(tx: &mpsc::UnboundedSender<Frame>, id: Option<u64>, result: OpResult) {
        if let Some(id) = id {
            let _ = tx.send(proto::inbound_frame(&Inbound::Response(proto::Response {
                id,
                result,
            })));
        }
    }

    async fn handle(self: &Arc<Self>, session: &mut Session, request: Request) {
        let Request { id, op, args } = request;
        self.lock().last_activity = Instant::now();
        if op == op::HELLO {
            let result = self.hello(session, &args);
            return Self::reply(&session.tx, id, result);
        }
        let Some(epoch) = session.epoch else {
            let error = if session.authorized {
                WireError::new(ErrorCode::HelloRequired, "say hello first")
            } else {
                WireError::new(ErrorCode::Unauthorized, "a valid token is required")
            };
            return Self::reply(&session.tx, id, Err(error));
        };
        if epoch < self.lock().max_epoch {
            let error = WireError::new(
                ErrorCode::StaleEpoch,
                "a server with a newer epoch has adopted this workspace",
            );
            return Self::reply(&session.tx, id, Err(error));
        }
        if let Err(error) = self.confinement.check(&op, &args) {
            return Self::reply(&session.tx, id, Err(error));
        }
        // Stream openers answer with the stream id themselves, before the
        // stream's first frame.
        let opener = Opener {
            conn: session.id,
            tx: session.tx.clone(),
            id,
        };
        let result = match op.as_str() {
            op::PING => Ok(json!("pong")),
            op::ACK => parse::<Ack>(&args).map(|ack| {
                self.ack(ack);
                Value::Null
            }),
            op::RESUME => parse::<Resume>(&args).and_then(|r| self.resume(session, r)),
            op::IDLE_STATE => Ok(self.idle_state()),
            op::STREAM_CLOSE => parse::<StreamRef>(&args).map(|r| {
                self.lock().streams.remove(&r.stream);
                Value::Null
            }),
            op::TURN_CANCEL => parse::<StreamRef>(&args).and_then(|r| {
                let mut state = self.lock();
                let stream = state
                    .streams
                    .get_mut(&r.stream)
                    .ok_or_else(|| WireError::new(ErrorCode::UnknownStream, "no such stream"))?;
                if let Some(cancel) = stream.cancel.take() {
                    let _ = cancel.send(());
                }
                Ok(Value::Null)
            }),
            op::TURN_START => self.start_turn(&opener, &args).await,
            op::TERM_EVENTS => {
                let events = self.workspace.terminal_events();
                self.open_bus(&opener, &op, None, events)
            }
            op::TERM_METADATA => {
                let events = self.workspace.terminal_metadata_events();
                let initial = self.workspace.terminal_metadata();
                self.open_bus(&opener, &op, Some(initial), events)
            }
            op::PREVIEW_EVENTS => {
                let events = self.workspace.preview_events();
                self.open_bus(&opener, &op, None, events)
            }
            op::DEVICE_EVENTS => {
                let events = self.workspace.device_events();
                let initial = self.workspace.device_state();
                self.open_bus(&opener, &op, Some(initial), events)
            }
            op::GIT_STACKED_ACTION => self.open_stacked_action(&opener, args),
            op::WATCH_VCS_STATUS | op::WATCH_VCS_BRANCH | op::WATCH_LOCAL_SERVERS => {
                self.open_watch(&opener, &op, args)
            }
            _ => {
                // Unary operations run concurrently so a slow one (a search,
                // a fetch) does not hold up the rest.
                let this = self.clone();
                let tx = session.tx.clone();
                tokio::spawn(async move {
                    let result = call(&*this.workspace, &op, &args).await;
                    Self::reply(&tx, id, result);
                });
                return;
            }
        };
        match result {
            Ok(Value::Null) if is_stream_op(&op) => {}
            result => Self::reply(&session.tx, id, result),
        }
    }

    fn hello(&self, session: &mut Session, args: &Value) -> OpResult {
        let hello: Hello = parse(args)?;
        if hello.version != proto::VERSION {
            return Err(WireError::new(
                ErrorCode::VersionMismatch,
                format!(
                    "agentd speaks version {}, not {}",
                    proto::VERSION,
                    hello.version
                ),
            ));
        }
        if !session.authorized {
            match hello.token {
                Some(token) if proto::token_matches(&self.config.token, &token) => {
                    session.authorized = true;
                }
                _ => {
                    return Err(WireError::new(
                        ErrorCode::Unauthorized,
                        "a valid token is required",
                    ))
                }
            }
        }
        let mut state = self.lock();
        if hello.epoch < state.max_epoch {
            return Err(WireError::new(
                ErrorCode::StaleEpoch,
                format!("epoch {} is older than {}", hello.epoch, state.max_epoch),
            ));
        }
        state.max_epoch = hello.epoch;
        state.current = Some(Conn {
            id: session.id,
            tx: session.tx.clone(),
        });
        session.epoch = Some(hello.epoch);
        let mut streams: Vec<_> = state
            .streams
            .iter()
            .map(|(&stream, s)| StreamInfo {
                stream,
                op: s.op.clone(),
                last_seq: s.last_seq,
                acked_seq: s.acked_seq,
                ended: s.ended,
            })
            .collect();
        streams.sort_by_key(|s| s.stream);
        serde_json::to_value(HelloReply {
            version: proto::VERSION,
            epoch: hello.epoch,
            root: self.config.root.to_string_lossy().into_owned(),
            streams,
        })
        .map_err(|e| WireError::new(ErrorCode::Internal, e.to_string()))
    }

    fn ack(&self, ack: Ack) {
        let mut state = self.lock();
        let Some(stream) = state.streams.get_mut(&ack.stream) else {
            return;
        };
        let seq = ack.seq.min(stream.last_seq);
        if let Err(error) = stream.buffer.ack(seq) {
            tracing::warn!(%error, stream = ack.stream, "could not trim a stream buffer");
        }
        stream.acked_seq = stream.acked_seq.max(seq);
        if stream.ended && stream.acked_seq == stream.last_seq {
            state.streams.remove(&ack.stream);
        }
    }

    fn resume(&self, session: &Session, resume: Resume) -> OpResult {
        let mut state = self.lock();
        let current = state.current.clone();
        if current.as_ref().map(|c| c.id) != Some(session.id) {
            return Err(WireError::new(
                ErrorCode::StaleEpoch,
                "another connection has adopted this workspace",
            ));
        }
        let mut missing = Vec::new();
        for (&id, &seq) in &resume.streams {
            let Some(stream) = state.streams.get_mut(&id) else {
                missing.push(id);
                continue;
            };
            let _ = stream.buffer.ack(seq.min(stream.last_seq));
            stream.acked_seq = stream.acked_seq.max(seq.min(stream.last_seq));
            stream.conn = session.id;
            match stream.buffer.replay(seq) {
                Ok(frames) => {
                    for bytes in frames {
                        let Ok(value) = serde_json::from_slice::<Value>(&bytes) else {
                            continue;
                        };
                        let _ = session
                            .tx
                            .send(proto::encode_frame(&value, proto::BULK_THRESHOLD));
                    }
                }
                Err(error) => tracing::warn!(%error, stream = id, "could not replay a stream"),
            }
        }
        // Streams the server no longer knows about have no reader; stop them.
        state
            .streams
            .retain(|id, stream| stream.conn == session.id || resume.streams.contains_key(id));
        state
            .streams
            .retain(|_, stream| !(stream.ended && stream.acked_seq == stream.last_seq));
        serde_json::to_value(ResumeReply { missing })
            .map_err(|e| WireError::new(ErrorCode::Internal, e.to_string()))
    }

    fn idle_state(&self) -> Value {
        let terminals = self.workspace.terminal_metadata()["terminals"]
            .as_array()
            .map_or(0, |items| {
                items.iter().filter(|t| t["status"] == "running").count() as u64
            });
        let state = self.lock();
        let turns = state.streams.values().filter(|s| s.turn_running).count() as u64;
        let idle = IdleState {
            running_turns: turns,
            running_terminals: terminals,
            running_processes: turns + terminals,
            open_streams: state.streams.values().filter(|s| !s.ended).count() as u64,
            unacked_frames: state.streams.values().map(|s| s.buffer.held()).sum(),
            connected: state.current.is_some(),
            idle_ms: state.last_activity.elapsed().as_millis() as u64,
        };
        serde_json::to_value(idle).unwrap_or(Value::Null)
    }

    /// Register a stream that `conn` opened.
    fn register(
        &self,
        conn: u64,
        op: &str,
        cancel: Option<oneshot::Sender<()>>,
    ) -> Result<u64, WireError> {
        let mut state = self.lock();
        let id = state.next_stream;
        state.next_stream += 1;
        let buffer = DiskBuffer::create(
            self.config.buffer_dir.join(format!("{id}.frames")),
            self.config.buffer_limit,
        )
        .map_err(|e| proto::io_error(&e))?;
        let turn = cancel.is_some();
        state.streams.insert(
            id,
            StreamState {
                op: op.to_owned(),
                buffer,
                last_seq: 0,
                acked_seq: 0,
                ended: false,
                conn,
                cancel,
                task: None,
                turn_running: turn,
            },
        );
        Ok(id)
    }

    fn set_task(&self, id: u64, task: AbortHandle) {
        match self.lock().streams.get_mut(&id) {
            Some(stream) if !stream.ended => stream.task = Some(task),
            _ => task.abort(),
        }
    }

    /// Record a stream frame and send it to the stream's connection.
    /// Returns false once the stream has ended or is gone.
    fn emit(&self, id: u64, body: StreamBody) -> bool {
        let mut state = self.lock();
        state.last_activity = Instant::now();
        let current = state.current.clone();
        let Some(stream) = state.streams.get_mut(&id) else {
            return false;
        };
        if stream.ended {
            return false;
        }
        let ending = matches!(body, StreamBody::End(_));
        if let StreamBody::Event(event) = &body {
            if stream.op == op::TURN_START && event["type"] == "exited" {
                stream.turn_running = false;
            }
        }
        let seq = stream.last_seq + 1;
        let mut frame = Inbound::Stream(StreamFrame {
            stream: id,
            seq,
            body,
        });
        let mut value = frame.to_value();
        let held = stream
            .buffer
            .push(seq, value.to_string().as_bytes(), ending)
            .unwrap_or(false);
        if !held {
            // Out of room: end the stream with a gap, and stop its work.
            tracing::warn!(stream = id, op = %stream.op, "a stream buffer overflowed");
            frame = Inbound::Stream(StreamFrame {
                stream: id,
                seq,
                body: StreamBody::End(StreamEnd {
                    reason: EndReason::Gap,
                    error: None,
                }),
            });
            value = frame.to_value();
            let _ = stream.buffer.push(seq, value.to_string().as_bytes(), true);
            stream.cancel = None;
            stream.turn_running = false;
        }
        stream.last_seq = seq;
        stream.ended = ending || !held;
        if stream.ended {
            stream.task = None;
            stream.turn_running = false;
        }
        if let Some(conn) = current.filter(|c| c.id == stream.conn) {
            let _ = conn
                .tx
                .send(proto::encode_frame(&value, proto::BULK_THRESHOLD));
        }
        !stream.ended
    }

    fn end(&self, id: u64, reason: EndReason, error: Option<WireError>) {
        self.emit(id, StreamBody::End(StreamEnd { reason, error }));
    }

    async fn start_turn(self: &Arc<Self>, opener: &Opener, args: &Value) -> OpResult {
        let start: TurnStart = parse(args)?;
        if start.command.is_empty() && start.provider.is_none() {
            return Err(bad_request("`command` is empty"));
        }
        let (cancel, cancelled) = oneshot::channel();
        let mut events = self
            .workspace
            .start_turn(start.into(), cancelled)
            .await
            .map_err(|e| WireError::new(ErrorCode::Internal, e.to_string()))?;
        // Registering after the spawn would drop `cancel` on failure, which
        // stops the process; that is what we want.
        let id = self.register(opener.conn, op::TURN_START, Some(cancel))?;
        opener.opened(id);
        let this = self.clone();
        let task = tokio::spawn(async move {
            while let Some(event) = events.recv().await {
                let exited = matches!(event, TurnEvent::Exited(_));
                let event = serde_json::to_value(WireTurnEvent::from(event))
                    .expect("turn events serialize");
                if !this.emit(id, StreamBody::Event(event)) {
                    return;
                }
                if exited {
                    return this.end(id, EndReason::Done, None);
                }
            }
            this.end(
                id,
                EndReason::Error,
                Some(WireError::new(
                    ErrorCode::Internal,
                    "the turn's process could not be waited on",
                )),
            );
        });
        self.set_task(id, task.abort_handle());
        Ok(Value::Null)
    }

    fn open_bus(
        self: &Arc<Self>,
        opener: &Opener,
        op: &str,
        initial: Option<Value>,
        mut events: broadcast::Receiver<Value>,
    ) -> OpResult {
        let id = self.register(opener.conn, op, None)?;
        opener.opened(id);
        if let Some(initial) = initial {
            self.emit(id, StreamBody::Event(initial));
        }
        let this = self.clone();
        let task = tokio::spawn(async move {
            loop {
                match events.recv().await {
                    Ok(event) => {
                        if !this.emit(id, StreamBody::Event(event)) {
                            return;
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => {
                        return this.end(id, EndReason::Gap, None)
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        return this.end(id, EndReason::Done, None)
                    }
                }
            }
        });
        self.set_task(id, task.abort_handle());
        Ok(Value::Null)
    }

    /// `git.runStackedAction`: progress batches, then the reply.
    fn open_stacked_action(self: &Arc<Self>, opener: &Opener, args: Value) -> OpResult {
        let tools: t3code_server::workspace::GitTools =
            parse::<proto::WireGitTools>(&args["tools"])?.into();
        let id = self.register(opener.conn, op::GIT_STACKED_ACTION, None)?;
        opener.opened(id);
        let this = self.clone();
        let task = tokio::spawn(async move {
            let (progress, mut batches) = mpsc::channel(16);
            let action = this
                .workspace
                .stacked_action(&args["input"], &tools, progress);
            let forward = async {
                while let Some(batch) = batches.recv().await {
                    this.emit(id, StreamBody::Event(json!({"progress": batch})));
                }
            };
            let (reply, ()) = tokio::join!(action, forward);
            let reply =
                serde_json::to_value(Polled::from_routed(Some(reply))).expect("readings serialize");
            if this.emit(id, StreamBody::Event(json!({"reply": reply}))) {
                this.end(id, EndReason::Done, None);
            }
        });
        self.set_task(id, task.abort_handle());
        Ok(Value::Null)
    }

    fn open_watch(self: &Arc<Self>, opener: &Opener, op: &str, args: Value) -> OpResult {
        let id = self.register(opener.conn, op, None)?;
        opener.opened(id);
        let this = self.clone();
        let op = op.to_owned();
        let task = tokio::spawn(async move {
            let mut previous: Option<Polled> = None;
            loop {
                let input = &args["input"];
                let reading = match op.as_str() {
                    op::WATCH_VCS_STATUS => Polled::from_routed(
                        this.workspace
                            .vcs(
                                "refreshStatus",
                                input,
                                args["fetchUpstream"].as_bool().unwrap_or(false),
                            )
                            .await,
                    ),
                    op::WATCH_VCS_BRANCH => {
                        Polled::from_routed(Some(this.workspace.vcs_branch(input).await))
                    }
                    _ => Polled::from_routed(Some(this.workspace.local_servers(input).await)),
                };
                if previous.as_ref() != Some(&reading) {
                    let event = serde_json::to_value(&reading).expect("readings serialize");
                    if !this.emit(id, StreamBody::Event(event)) {
                        return;
                    }
                    previous = Some(reading);
                }
                tokio::time::sleep(this.config.poll_interval).await;
            }
        });
        self.set_task(id, task.abort_handle());
        Ok(Value::Null)
    }
}

/// Who asked for a stream, so the opener can answer with its id.
struct Opener {
    conn: u64,
    tx: mpsc::UnboundedSender<Frame>,
    id: Option<u64>,
}

impl Opener {
    fn opened(&self, stream: u64) {
        Agentd::reply(&self.tx, self.id, Ok(json!(StreamRef { stream })));
    }
}

fn is_stream_op(op: &str) -> bool {
    matches!(
        op,
        op::TURN_START
            | op::TERM_EVENTS
            | op::TERM_METADATA
            | op::PREVIEW_EVENTS
            | op::DEVICE_EVENTS
            | op::WATCH_VCS_STATUS
            | op::WATCH_VCS_BRANCH
            | op::WATCH_LOCAL_SERVERS
            | op::GIT_STACKED_ACTION
    )
}

async fn upgrade(
    State(agentd): State<Arc<Agentd>>,
    headers: HeaderMap,
    ws: WebSocketUpgrade,
) -> HttpResponse {
    let presented = headers
        .get(AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "));
    let authorized = match presented {
        Some(token) if proto::token_matches(&agentd.config.token, token) => true,
        Some(_) => return StatusCode::UNAUTHORIZED.into_response(),
        // The token may come in `hello` instead.
        None => false,
    };
    ws.max_message_size(MAX_MESSAGE_BYTES)
        .max_frame_size(MAX_MESSAGE_BYTES)
        .on_upgrade(move |socket| agentd.serve(socket, authorized))
}

/// Run a unary operation.
async fn call(workspace: &dyn Workspace, op: &str, args: &Value) -> OpResult {
    let method = || str_arg(args, "method");
    let input = &args["input"];
    let path = || str_arg(args, "path").map(Path::new);
    match op {
        op::FS_LIST_ENTRIES => proto::encode_routed(workspace.list_entries(args).await),
        op::FS_LIST_IGNORED_FILES => proto::encode_routed(workspace.list_ignored_files(args).await),
        op::FS_SEARCH_ENTRIES => proto::encode_routed(workspace.search_entries(args).await),
        op::FS_SEARCH_CONTENTS => proto::encode_routed(workspace.search_contents(args).await),
        op::FS_READ_FILE => proto::encode_routed(workspace.read_file(args).await),
        op::FS_WRITE_FILE => proto::encode_routed(workspace.write_file(args).await),
        op::FS_METADATA => proto::encode_routed(workspace.file_metadata(args).await),
        op::FS_BROWSE => proto::encode_routed(workspace.browse(args).await),
        op::FS_CREATE_DIR_ALL => workspace
            .create_dir_all(path()?)
            .await
            .map(|()| Value::Null)
            .map_err(|e| proto::io_error(&e)),
        op::FS_CANONICALIZE => workspace
            .canonicalize(path()?)
            .await
            .map(|p| json!(p.to_string_lossy()))
            .map_err(|e| proto::io_error(&e)),
        op::FS_IS_DIR => Ok(json!(workspace.is_dir(path()?).await)),
        op::GIT_VCS => proto::encode_routed(
            workspace
                .vcs(
                    method()?,
                    input,
                    args["fetchUpstream"].as_bool().unwrap_or(false),
                )
                .await,
        ),
        op::GIT_REVIEW => proto::encode_reply(workspace.review(method()?, input).await),
        op::GIT_PULL_REQUEST => {
            let tools: t3code_server::workspace::GitTools =
                parse::<proto::WireGitTools>(&args["tools"])?.into();
            proto::encode_reply(workspace.pull_request(method()?, input, &tools).await)
        }
        op::GIT_CAPTURE_TREE => Ok(json!(
            workspace.capture_git_tree(str_arg(args, "cwd")?).await
        )),
        op::TERM_CALL => proto::encode_routed(workspace.terminal(method()?, input).await),
        op::PROC_STOP_LOCAL_SERVER => proto::encode_reply(workspace.stop_local_server(input).await),
        op::PREVIEW_CALL => proto::encode_routed(workspace.preview(method()?, input).await),
        op::PREVIEW_PROFILE => {
            proto::encode_reply(workspace.preview_profile(method()?, input).await)
        }
        op::PREVIEW_CLOSE_PAGES => {
            workspace
                .close_preview_pages(
                    args["thread"].as_str(),
                    args["tab"].as_str(),
                    args["profile"].as_str(),
                )
                .await;
            Ok(Value::Null)
        }
        op::DEVICE_CALL => proto::encode_routed(workspace.device(method()?, input).await),
        _ => Err(WireError::new(
            ErrorCode::UnknownOp,
            format!("unknown op: {op}"),
        )),
    }
}
