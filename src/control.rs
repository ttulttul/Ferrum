//! The control plane: what the server runs without a local workspace.
//!
//! It holds the same [`ControlState`] as the native server (auth, the
//! orchestration event log and its in-memory store, state documents) and
//! answers HTTP routes and WebSocket RPCs that need nothing from a
//! host. Workspace calls (files, git, terminals, provider processes) belong to
//! the workspace plane, `t3-agentd`, and are not wired up yet; they answer as
//! unavailable.
//!
//! Unlike the native socket loop (`rpc::session`), the RPC handler here is
//! message driven: [`ControlPlane::on_message`] takes one text frame and
//! returns the frames to send. Requests go through the same checks, stream
//! records and workspace routing as the native server ([`crate::dispatch`],
//! [`crate::subscription`]). A [`Connection`] is small and serializable: it
//! names its open streams, and each stream's record (parameters and cursor)
//! is a document in the state's [`DocumentStore`]. After each message the
//! host takes the plane's pending signals ([`ControlPlane::take_signals`])
//! and hands them to every connection ([`ControlPlane::deliver`]), which
//! returns the frames to push.
//!
//! Platform neutral: the native build compiles and tests it too.
use crate::{
    access_http::{self, AuthHost},
    auth_service::{self, AuthService, Session},
    control_state::ControlState,
    dispatch::{self, WorkspaceSettings},
    environment,
    event_log::EventLog,
    persistence::DocumentStore,
    rpc_protocol::{js_template_string, request_key, split_socket_frame, Reply},
    rpc_protocol::{SOCKET_FRAME_PARTS_QUERY_PARAM, SOCKET_FRAME_PART_BYTES},
    runtime::Runtime,
    server_config,
    subscription::{Signal, Subscription},
    workspace::{RemoteWorkspace, Transport, Workspace},
    workspace_protocol::{Frame, HelloReply, WireError},
    Event, ServerEvent,
};
use axum::{
    extract::{Query, State},
    http::{HeaderMap, StatusCode, Uri},
    response::{IntoResponse, Response},
    routing::get,
    Json, Router,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::{
    collections::{BTreeMap, HashMap},
    path::PathBuf,
    sync::{Arc, Mutex},
};
use tokio::sync::{broadcast, mpsc};

/// Where the workspace lives on the `t3-agentd` side.
pub const WORKSPACE_ROOT: &str = "/workspace";
/// Where the control plane's state documents live, as the native
/// server's `userdata` directory (auth records, keybindings).
pub const STATE_DIR: &str = "/home/t3/.t3/userdata";

#[derive(Clone, Default)]
pub struct Config {
    pub environment_id: String,
    pub label: String,
    /// The desktop bootstrap credential the pairing exchange accepts.
    pub bootstrap_credential: String,
    /// The port in the session cookie name (`t3_session_<port>`).
    pub cookie_port: u16,
    /// The executable and leading arguments of a provider turn; the prompt
    /// follows them. Empty for one that echoes the prompt.
    pub provider_command: Vec<String>,
}

/// What the plane runs on: the host's event loop and the workspace it
/// reaches `t3-agentd` through.
pub struct Host {
    pub runtime: Arc<dyn Runtime>,
    /// Starts disconnected; the host reports its connection with
    /// [`ControlPlane::workspace_connected`] and feeds it frames with
    /// [`ControlPlane::workspace_frame`].
    pub workspace: RemoteWorkspace,
}

// The plane is shared with the HTTP router, whose state must be `Send` and
// `Sync`. In the WebAssembly build the runtime and the transport are not,
// but that build has one thread, so nothing is ever sent or shared across
// threads.
#[cfg(all(
    not(feature = "native"),
    target_arch = "wasm32",
    not(target_feature = "atomics")
))]
unsafe impl Send for Host {}
#[cfg(all(
    not(feature = "native"),
    target_arch = "wasm32",
    not(target_feature = "atomics")
))]
unsafe impl Sync for Host {}

/// How often a connected workspace needs [`RemoteWorkspace::maintain`].
pub const MAINTAIN_INTERVAL_MS: i64 = 250;

pub struct ControlPlane {
    config: Config,
    started_at: String,
    state: ControlState,
    signals: Mutex<Signals>,
    /// Held while an orchestration command checks and commits.
    mutation_lock: tokio::sync::Mutex<()>,
    cancellations: tokio::sync::Mutex<HashMap<uuid::Uuid, tokio::sync::oneshot::Sender<()>>>,
    host: Host,
    /// Whether changes are waiting for the host's next alarm to deliver.
    wake: std::sync::atomic::AtomicBool,
}
impl AuthHost for ControlPlane {
    fn auth(&self) -> &AuthService {
        &self.state.auth
    }
}
impl std::ops::Deref for ControlPlane {
    type Target = ControlState;
    fn deref(&self) -> &ControlState {
        &self.state
    }
}

/// One client socket: who it is and which streams it has open. Serializable,
/// so a host can keep it with the socket while the process sleeps.
#[derive(Clone, Serialize, Deserialize)]
pub struct Connection {
    /// Names this connection's stream records in the document store.
    #[serde(default)]
    id: String,
    pub principal: Session,
    pub frame_parts: bool,
    pub references: bool,
    /// Open streams by request key, with the id the client used.
    open: BTreeMap<String, OpenStream>,
}
#[derive(Clone, Serialize, Deserialize)]
struct OpenStream {
    id: Value,
    method: String,
    /// Whether the stream's record is stored (it reports later changes).
    #[serde(default)]
    live: bool,
}

/// The channels the plane learns of changes on, drained after each message.
struct Signals {
    events: broadcast::Receiver<ServerEvent>,
    suggestions: broadcast::Receiver<Value>,
    linked: broadcast::Receiver<u64>,
    scheduled_tasks: broadcast::Receiver<()>,
    access: mpsc::UnboundedReceiver<auth_service::access::Change>,
}

impl Signals {
    fn new(state: &ControlState) -> Self {
        Self {
            events: state.events.subscribe(),
            suggestions: state.prompt_suggestion_events.subscribe(),
            linked: state.pull_request_linked_events.subscribe(),
            scheduled_tasks: state.scheduled_task_events.subscribe(),
            access: state.auth.subscribe_access().1,
        }
    }

    fn drain(&mut self) -> Vec<Signal> {
        use broadcast::error::TryRecvError::{Empty, Lagged};
        let mut signals = Vec::new();
        loop {
            match self.events.try_recv() {
                Ok(event) => signals.push(Signal::Committed(Box::new(event))),
                Err(Lagged(_)) => signals.push(Signal::Resync),
                Err(Empty | broadcast::error::TryRecvError::Closed) => break,
            }
        }
        while let Ok(revision) = self.linked.try_recv() {
            signals.push(Signal::LinkedRefresh(revision));
        }
        while let Ok(()) = self.scheduled_tasks.try_recv() {
            signals.push(Signal::ScheduledTasks);
        }
        while let Ok(event) = self.suggestions.try_recv() {
            signals.push(Signal::PromptSuggestion(event));
        }
        while let Ok(change) = self.access.try_recv() {
            signals.push(Signal::AuthAccess(change));
        }
        signals
    }
}

impl ControlPlane {
    /// Rebuilds the store from `log`. Auth records and other state documents
    /// live in `documents` under [`STATE_DIR`], written through on change.
    ///
    /// Turns that were running when the plane last stopped are picked up
    /// again: each is resumed in `host.workspace` from the last event the
    /// log recorded, so pass a workspace that is not connected yet and
    /// connect it afterwards with
    /// [`workspace_connected`](Self::workspace_connected). A running run
    /// whose turn cannot be resumed is recorded as failed.
    // One thread in the WebAssembly build: the plane need not be `Send`.
    #[allow(clippy::arc_with_non_send_sync)]
    pub fn open(
        config: Config,
        documents: Arc<dyn DocumentStore>,
        log: Box<dyn EventLog + Send>,
        host: Host,
    ) -> anyhow::Result<Arc<Self>> {
        let auth = AuthService::with_bootstrap(config.bootstrap_credential.clone())
            .with_port(config.cookie_port)
            .open_in(documents.clone(), std::path::Path::new(STATE_DIR))?;
        let started_at = auth_service::iso(auth_service::now());
        let state = ControlState::open(auth, documents, log, std::path::Path::new(STATE_DIR))?;
        let signals = Mutex::new(Signals::new(&state));
        let plane = Arc::new(Self {
            config,
            started_at,
            state,
            signals,
            mutation_lock: Default::default(),
            cancellations: Default::default(),
            host,
            wake: Default::default(),
        });
        plane.resume_turns();
        Ok(plane)
    }

    fn turn_path(run: &uuid::Uuid) -> PathBuf {
        std::path::Path::new(STATE_DIR)
            .join("turns")
            .join(format!("{run}.json"))
    }

    /// Pick up the turns of runs still running, or end them as failed.
    fn resume_turns(self: &Arc<Self>) {
        let running = crate::turn::running_runs(&self.store_now());
        let mut kept = std::collections::HashSet::new();
        for (run, recorded) in running {
            let record = self
                .documents
                .read_optional(&Self::turn_path(&run.id))
                .ok()
                .flatten()
                .and_then(|bytes| serde_json::from_slice::<TurnRecord>(&bytes).ok());
            let Some(TurnRecord { stream, cwd }) = record else {
                // The turn never reached the workspace, or a host that
                // cannot resume turns ran it.
                if let Err(error) = self.commit_now(crate::turn::interrupted(&run)) {
                    tracing::error!(%error, "could not end an interrupted run");
                }
                self.wake();
                continue;
            };
            kept.insert(run.id);
            let position = crate::workspace::StreamPosition {
                stream,
                seq: recorded
                    .filter(|position| position.stream == stream)
                    .map_or(0, |position| position.seq),
            };
            let (cancel, cancelled) = tokio::sync::oneshot::channel();
            if let Ok(mut cancellations) = self.cancellations.try_lock() {
                cancellations.insert(run.id, cancel);
            }
            // Before the workspace connects, this completes at once, so the
            // connection's first `resume` names the stream.
            let workspace = self.host.workspace.clone();
            let mut resumed =
                Box::pin(async move { workspace.resume_turn(position, cancelled).await });
            let plane = self.clone();
            let task: crate::runtime::Task =
                match futures_util::FutureExt::now_or_never(resumed.as_mut()) {
                    Some(turn) => Box::pin(crate::turn::resume(plane, run, cwd, turn)),
                    None => Box::pin(async move {
                        let turn = resumed.await;
                        crate::turn::resume(plane, run, cwd, turn).await
                    }),
                };
            self.host.runtime.spawn(task);
        }
        // Records of turns that ended while their end was being recorded.
        let dir = std::path::Path::new(STATE_DIR).join("turns");
        for path in self.documents.list(&dir).unwrap_or_default() {
            let run = path
                .file_stem()
                .and_then(|stem| stem.to_str())
                .and_then(|stem| stem.parse::<uuid::Uuid>().ok());
            if !run.is_some_and(|run| kept.contains(&run)) {
                let _ = self.documents.remove(&path);
            }
        }
    }

    /// The workspace this plane runs turns and workspace methods in.
    pub fn workspace(&self) -> &RemoteWorkspace {
        &self.host.workspace
    }

    /// The host opened a connection to `t3-agentd`: say hello and resume
    /// the streams this plane still reads. Then keep calling
    /// [`on_alarm`](Self::on_alarm) when [`next_alarm`](Self::next_alarm)
    /// says.
    pub async fn workspace_connected(
        &self,
        transport: Arc<dyn Transport>,
    ) -> Result<HelloReply, WireError> {
        let hello = self.host.workspace.connected(transport).await;
        self.schedule();
        hello
    }

    /// A frame arrived from `t3-agentd`.
    pub fn workspace_frame(&self, frame: Frame) {
        self.host.workspace.receive(frame);
        self.host.workspace.flush_acks();
    }

    /// The connection to `t3-agentd` closed. Turns wait for the next one.
    pub fn workspace_disconnected(&self) {
        self.host.workspace.disconnected();
    }

    /// When the host should next call [`on_alarm`](Self::on_alarm), in
    /// wall-clock milliseconds: now when changes are waiting to be
    /// delivered, every [`MAINTAIN_INTERVAL_MS`] while the workspace is
    /// connected, otherwise never.
    pub fn next_alarm(&self) -> Option<i64> {
        let now = self.host.runtime.now_ms();
        if self.wake.load(std::sync::atomic::Ordering::Relaxed) {
            Some(now)
        } else if self.host.workspace.is_ready() {
            Some(now + MAINTAIN_INTERVAL_MS)
        } else {
            None
        }
    }

    /// The alarm the plane asked for is due: keep the workspace connection
    /// healthy and ask for the next alarm. The host then delivers what
    /// changed ([`take_signals`](Self::take_signals) and
    /// [`deliver`](Self::deliver)) to every connection.
    pub async fn on_alarm(&self) {
        self.wake.store(false, std::sync::atomic::Ordering::Relaxed);
        self.host.workspace.maintain().await;
        self.host.workspace.flush_acks();
        self.schedule();
    }

    /// Ask the runtime for the alarm [`next_alarm`](Self::next_alarm) names.
    fn schedule(&self) {
        if let Some(at) = self.next_alarm() {
            self.host.runtime.set_alarm(at);
        }
    }

    /// Something changed outside a client message; ask to deliver it soon.
    fn wake(&self) {
        if !self.wake.swap(true, std::sync::atomic::Ordering::Relaxed) {
            self.host.runtime.set_alarm(self.host.runtime.now_ms());
        }
    }

    pub fn sequence(&self) -> u64 {
        self.sequence_now()
    }

    /// Appends one event to the log, then applies it to the store.
    pub fn commit(&self, event: Event) -> anyhow::Result<ServerEvent> {
        self.commit_now(event)
    }

    pub fn descriptor(&self) -> Value {
        environment::descriptor(
            &self.config.environment_id,
            &self.config.label,
            ("linux", "x64"),
            true,
        )
    }

    /// HTTP routes the control plane answers. `/ws` is the host's: it
    /// upgrades with [`authorize_socket`](Self::authorize_socket) and then
    /// feeds frames to [`on_message`](Self::on_message).
    pub fn router(self: &Arc<Self>) -> Router {
        Router::new()
            .route("/.well-known/t3/environment", get(descriptor))
            .route("/api/orchestration/shell", get(shell_snapshot))
            .merge(access_http::auth_routes::<ControlPlane>())
            .fallback(unavailable)
            .layer(axum::middleware::from_fn(access_http::contextualize))
            .layer(axum::middleware::from_fn(crate::cors::browser_api))
            .with_state(self.clone())
    }

    /// Checks a `/ws` upgrade as the native server does: protocol 2, then a
    /// `wsTicket` or a session credential. On success returns the connection
    /// to keep with the socket.
    #[allow(clippy::result_large_err)] // the rejection is the HTTP response to send
    pub fn authorize_socket(&self, headers: &HeaderMap, uri: &Uri) -> Result<Connection, Response> {
        let query = url::form_urlencoded::parse(uri.query().unwrap_or("").as_bytes());
        if !query
            .clone()
            .any(|(key, value)| key == "orchestrationProtocol" && value == "2")
        {
            let body = json!({
                "code": "orchestration_protocol_incompatible",
                "message": "Update this client to one that supports orchestration protocol 2.",
                "orchestrationProtocolVersion": 2
            });
            return Err((
                StatusCode::UPGRADE_REQUIRED,
                [
                    ("vary", "Accept-Encoding"),
                    ("access-control-allow-origin", "*"),
                ],
                Json(body),
            )
                .into_response());
        }
        let ticket = query
            .into_iter()
            .filter(|(key, _)| key == "wsTicket")
            .last()
            .map(|(_, value)| value.into_owned())
            .filter(|v| !v.trim().is_empty());
        // The host accepts the upgrade itself, outside the router whose middleware
        // records the request a DPoP proof must match, so record it here.
        let principal = access_http::with_request_context("GET", uri, headers, || match ticket {
            Some(ticket) => access_http::authenticate_ticket(self, headers, uri, &ticket, "rpc"),
            None => access_http::principal(self, headers),
        })
        .map_err(|reason| crate::auth_error(headers, reason))?;
        let references = query
            .clone()
            .find(|(k, _)| k == "clientCapabilities")
            .is_some_and(|(_, v)| v.split(',').any(|c| c == "settingsSecretReferences"));
        let frame_parts = query
            .clone()
            .any(|(key, _)| key == SOCKET_FRAME_PARTS_QUERY_PARAM);
        self.auth.socket_presence(&principal.id, true);
        Ok(Connection {
            id: crate::platform::uuid_v4().to_string(),
            principal,
            frame_parts,
            references,
            open: BTreeMap::new(),
        })
    }

    pub fn disconnect(&self, connection: &Connection) {
        for key in connection.open.keys() {
            self.forget(connection, key);
        }
        self.auth.socket_presence(&connection.principal.id, false);
    }

    /// The changes since the last call, for [`deliver`](Self::deliver).
    pub fn take_signals(&self) -> Vec<Signal> {
        self.signals
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .drain()
    }

    /// The frames `signals` add to `connection`'s live streams, moving each
    /// stream's stored cursor past them.
    pub async fn deliver(&self, connection: &Connection, signals: &[Signal]) -> Vec<String> {
        let mut frames = Vec::new();
        for (key, stream) in connection.open.iter().filter(|(_, s)| s.live) {
            let Some(mut subscription) = self.load(connection, key) else {
                continue;
            };
            let before = subscription.cursor.clone();
            for signal in signals {
                for batch in subscription
                    .advance(self, &connection.principal, signal.clone())
                    .await
                {
                    frames.push(Reply::Chunk(batch).frame(&stream.id));
                }
            }
            if subscription.cursor != before {
                self.store(connection, key, &subscription);
            }
        }
        encode(connection, &frames)
    }

    fn record_path(connection: &Connection, key: &str) -> PathBuf {
        std::path::Path::new(STATE_DIR)
            .join("streams")
            .join(&connection.id)
            .join(hex::encode(key))
    }

    fn load(&self, connection: &Connection, key: &str) -> Option<Subscription> {
        let bytes = self
            .documents
            .read_optional(&Self::record_path(connection, key))
            .ok()??;
        serde_json::from_slice(&bytes).ok()
    }

    fn store(&self, connection: &Connection, key: &str, subscription: &Subscription) {
        let path = Self::record_path(connection, key);
        if let Ok(bytes) = serde_json::to_vec(subscription) {
            if let Some(dir) = path.parent() {
                let _ = self.documents.create_dir_all(dir);
            }
            let _ = self
                .documents
                .write(&path, &bytes, crate::persistence::WriteMode::Replace);
        }
    }

    /// Delete the stored stream records of connections that are gone, which
    /// a socket that closed without a close event leaves behind. A host
    /// calls this when it rebuilds the plane from storage, with every
    /// connection whose socket is still open. Returns how many it deleted.
    pub fn prune_streams<'a>(&self, open: impl IntoIterator<Item = &'a Connection>) -> usize {
        let open: std::collections::HashSet<&str> =
            open.into_iter().map(|c| c.id.as_str()).collect();
        let dir = std::path::Path::new(STATE_DIR).join("streams");
        let Ok(paths) = self.documents.list(&dir) else {
            return 0;
        };
        let mut removed = 0;
        for path in paths {
            let Ok(relative) = path.strip_prefix(&dir) else {
                continue;
            };
            let mut parts = relative.components();
            let (Some(first), Some(_)) = (parts.next(), parts.next()) else {
                // Directly under `streams/`: a connection from before
                // connections had ids. Keep it while one may still be open.
                if !open.contains("") && self.documents.remove(&path).is_ok() {
                    removed += 1;
                }
                continue;
            };
            let owner = first.as_os_str().to_string_lossy();
            if !open.contains(owner.as_ref()) && self.documents.remove(&path).is_ok() {
                removed += 1;
            }
        }
        removed
    }

    fn forget(&self, connection: &Connection, key: &str) {
        let _ = self.documents.remove(&Self::record_path(connection, key));
    }

    /// Handles one text frame from a client and returns the frames to send.
    /// Workspace methods go to `workspace`.
    pub async fn on_message(
        self: &Arc<Self>,
        connection: &mut Connection,
        text: &str,
    ) -> Vec<String> {
        let Ok(request) = serde_json::from_str::<Value>(text) else {
            return Vec::new();
        };
        let frames = match request["_tag"].as_str() {
            Some("Ping") => vec![json!({"_tag":"Pong"})],
            // Every stream here sends its values at once, so there is no
            // backpressure window to reopen.
            Some("Ack") => Vec::new(),
            Some("Interrupt") => match request_key(&request["requestId"])
                .and_then(|key| connection.open.remove(&key).map(|stream| (key, stream)))
            {
                Some((key, stream)) => {
                    if stream.live {
                        self.forget(connection, &key);
                    }
                    vec![Reply::Failure(json!({"_tag":"Interrupt"})).frame(&stream.id)]
                }
                None => Vec::new(),
            },
            Some("Request") => self.request(connection, &request).await,
            other => vec![
                json!({"_tag":"Defect","defect":format!("Unknown request tag: {}", other.unwrap_or("undefined"))}),
            ],
        };
        encode(connection, &frames)
    }

    async fn request(self: &Arc<Self>, connection: &mut Connection, request: &Value) -> Vec<Value> {
        let id = request.get("id").cloned().unwrap_or(Value::Null);
        let Some(key) = request_key(&id) else {
            let shown = if request.get("id").is_some() {
                js_template_string(&id)
            } else {
                "undefined".into()
            };
            return vec![json!({"_tag":"Defect","defect":format!("Invalid request id: {shown}")})];
        };
        if connection.open.contains_key(&key) {
            return Vec::new();
        }
        let method = request["tag"].as_str().unwrap_or("undefined");
        let params = &request["payload"];
        match self.call(&connection.principal, method, params).await {
            Call::Done(reply) => vec![reply.frame(&id)],
            Call::Live(subscription, values) => {
                self.store(connection, &key, &subscription);
                connection.open.insert(
                    key,
                    OpenStream {
                        id: id.clone(),
                        method: method.to_owned(),
                        live: true,
                    },
                );
                values
                    .into_iter()
                    .map(|batch| Reply::Chunk(batch).frame(&id))
                    .collect()
            }
            Call::Stream(values) => {
                connection.open.insert(
                    key,
                    OpenStream {
                        id: id.clone(),
                        method: method.to_owned(),
                        live: false,
                    },
                );
                values
                    .into_iter()
                    .map(|batch| Reply::Chunk(batch).frame(&id))
                    .collect()
            }
        }
    }

    async fn call(self: &Arc<Self>, principal: &Session, method: &str, params: &Value) -> Call {
        if !crate::rpc_known(method) {
            return Call::Done(Reply::defect(format!("Unknown request tag: {method}")));
        }
        let decoded = match dispatch::check(principal, method, params) {
            Ok(decoded) => decoded,
            Err(reply) => return Call::Done(reply),
        };
        let params = decoded.as_ref().unwrap_or(params);
        if crate::subscription::serves(method) {
            return match Subscription::open(self, principal, method, params, false).await {
                Ok(opened) => Call::Live(opened.subscription, opened.first),
                Err(reply) => Call::Done(reply),
            };
        }
        if method == "orchestration.dispatchCommand" {
            // Command dispatch takes its host by `Arc`. In the single-threaded
            // WebAssembly build the plane need not be `Send`.
            #[allow(clippy::arc_with_non_send_sync)]
            let host = Arc::new(PlaneCommands {
                plane: self.clone(),
            });
            return Call::Done(
                match crate::dispatch_orchestration_command(host, params.clone()).await {
                    Ok(sequence) => Reply::Success(json!({ "sequence": sequence })),
                    Err(error) => Reply::Failure(json!({"_tag":"Fail","error":error})),
                },
            );
        }
        if let Some(call) = self.host_call(method, params) {
            return call;
        }
        let settings = WorkspaceSettings {
            automatic_git_fetch: true,
            ..Default::default()
        };
        match dispatch::workspace(&self.host.workspace, &settings, method, params).await {
            Some(Ok(value)) => Call::Done(Reply::Success(value)),
            Some(Err(error)) if error["_tag"] == "Die" => Call::Done(Reply::Failure(error)),
            Some(Err(error)) => Call::Done(Reply::Failure(json!({"_tag":"Fail","error":error}))),
            None => Call::Done(Reply::defect(format!(
                "RPC contract is registered but unavailable in the control plane: {method}"
            ))),
        }
    }

    /// The methods this host answers itself.
    fn host_call(&self, method: &str, params: &Value) -> Option<Call> {
        Some(match method {
            "server.probe" => Call::Done(Reply::Success(json!({}))),
            "subscribeServerConfig" => {
                let mut batches = vec![
                    json!([{"version":1,"type":"snapshot","config":self.config_snapshot(params)}]),
                ];
                // As the native server: report empty theme and usage-limit sources to
                // clients that opted in.
                if params["usageLimitSources"] == true {
                    batches.push(json!([{"version":1,"type":"usageLimitSourcesUpdated","payload":{"sources":[]}}]));
                }
                if params["environmentThemes"] == true {
                    batches.push(json!([{"version":1,"type":"environmentThemesUpdated","payload":{"themes":[]}}]));
                }
                Call::Stream(batches)
            }
            "server.getConfig" => Call::Done(Reply::Success(self.config_snapshot(&Value::Null))),
            "server.refreshProviders" => Call::Done(Reply::Success(
                json!({"providers":self.config_snapshot(&Value::Null)["providers"]}),
            )),
            "subscribeServerLifecycle" => {
                let environment = self.descriptor();
                Call::Stream(vec![json!([
                    {"version":1,"sequence":1,"type":"welcome","payload":{"environment":environment,"cwd":WORKSPACE_ROOT,"projectName":"workspace"}},
                    {"version":1,"sequence":2,"type":"ready","payload":{"at":self.started_at,"environment":environment}}
                ])])
            }
            "subscribeProjectClones" => Call::Stream(vec![json!([[]])]),
            // As native before any refresh: the stream stays open and silent.
            "pullRequests.subscribeRefreshes" => Call::Stream(Vec::new()),
            "subscribeDeviceState" => Call::Stream(vec![json!([{
                "supportsHostRetry": true, "supportsToolUpdate": true, "supportsToolInspection": true,
                "supportsEasHosts": true, "supportsSimlockHosts": true, "hostStatus": "disabled",
                "hostStatuses": {}, "devices": [], "sessions": [], "onboardingCompleted": false,
                "agentAccessEnabled": false, "hubBasePath": "/api/device-hub", "revision": 0, "hosts": []
            }])]),
            "server.reportClientActivity" => Call::Done(Reply::Success(Value::Null)),
            "preview.reportProfiles" => Call::Done(Reply::Success(Value::Null)),
            "projects.mutate" => Call::Done(self.create_project(params)),
            "orchestration.subscribeShell" => {
                let sequence = self.sequence_now();
                let store = self.store_now();
                let after = params.get("afterSequence").filter(|value| !value.is_null());
                let resumable = after.and_then(Value::as_u64) == Some(sequence);
                if after.is_some() && !resumable && params["httpSnapshotFallback"] == true {
                    return Some(Call::Stream(vec![json!([{"kind":"snapshot-required"}])]));
                }
                let mut items = Vec::new();
                if !resumable {
                    items.push(
                        json!({"kind":"snapshot","snapshot":environment::shell(&store, sequence)}),
                    );
                }
                if params["requestCompletionMarker"] == true {
                    items.push(json!({"kind":"synchronized"}));
                }
                Call::Stream(if items.is_empty() {
                    Vec::new()
                } else {
                    vec![Value::Array(items)]
                })
            }
            _ => return None,
        })
    }

    /// `projects.mutate` with `project.create` only. The native server first
    /// creates and canonicalizes the workspace root; that check belongs to the
    /// workspace plane, so the root is recorded as given.
    fn create_project(&self, input: &Value) -> Reply {
        let command = input["commandId"].as_str().unwrap_or_default();
        let id = input["projectId"].as_str().unwrap_or_default();
        let error = |msg: &str| {
            Reply::Failure(
                json!({"_tag":"Fail","error":{"_tag":"ProjectMutationError","commandId":command,"message":msg}}),
            )
        };
        if input["type"] != "project.create" {
            return error("Only project.create is available in the control plane.");
        }
        let (Some(title), Some(root)) = (
            input["title"]
                .as_str()
                .map(str::trim)
                .filter(|s| !s.is_empty()),
            input["workspaceRoot"]
                .as_str()
                .map(str::trim)
                .filter(|s| !s.is_empty()),
        ) else {
            return error("A title and a workspace root are required.");
        };
        if command.trim().is_empty()
            || id.trim().is_empty()
            || self.store_now().project_rows.contains_key(id)
        {
            return error("Failed to mutate project.");
        }
        let project = crate::Project {
            id: id.into(),
            name: title.into(),
            path: root.into(),
            created_at: crate::now(),
        };
        let row = crate::store::wire_project(&project);
        match self.commit(Event::ProjectCreated(project)) {
            Ok(_) => Reply::Success(row),
            Err(_) => error("Failed to mutate project."),
        }
    }

    /// The `subscribeServerConfig` snapshot. Host facts describe the
    /// workspace process; settings are the defaults until settings move into
    /// the control plane's storage.
    fn config_snapshot(&self, options: &Value) -> Value {
        let settings = default_settings();
        let keybindings_path = std::path::Path::new(STATE_DIR).join("keybindings.json");
        let (keybindings, issues) =
            server_config::keybindings::read(self.documents.as_ref(), &keybindings_path);
        let mut config = json!({
            "environment": self.descriptor(),
            "auth": self.auth.descriptor(),
            "cwd": WORKSPACE_ROOT,
            "hostname": self.config.label,
            "keybindingsConfigPath": keybindings_path,
            "keybindings": keybindings,
            "issues": issues,
            "providers": server_config::providers::list_with(&self.started_at, |_| None, &settings, options, &crate::model_manifest::BUNDLED),
            "availableEditors": [],
            "remoteOpenTargets": [],
            "directEndpoints": [],
            "observability": {"logsDirectoryPath":"/home/t3/.t3/userdata/logs","localTracingEnabled":true,"otlpTracesEnabled":false,"otlpMetricsEnabled":false,"otlpLogsEnabled":false},
            "settings": settings,
        });
        for key in [
            "shellResumeCompletionMarker",
            "shellThreadFamilies",
            "threadLinkMetadata",
            "shellOlderSettled",
            "threadResumeCompletionMarker",
            "threadSnapshotPagination",
            "threadFind",
            "threadFindProgressive",
            "secretProviderSettings",
            "secretReferences",
            "secretRequestReferences",
        ] {
            config[key] = true.into();
        }
        config["scratchWorkspaceRoot"] = json!(format!("{WORKSPACE_ROOT}/scratch"));
        config["newProjectsRoot"] = json!(format!("{WORKSPACE_ROOT}/projects"));
        config["serverTime"] = json!(auth_service::iso(auth_service::now()));
        config
    }
}

/// Orchestration commands as this plane runs them: over its control state,
/// with no managed policy, default settings and no guest shares. Turns run
/// in its workspace.
struct PlaneCommands {
    plane: Arc<ControlPlane>,
}

impl std::ops::Deref for PlaneCommands {
    type Target = ControlState;
    fn deref(&self) -> &ControlState {
        &self.plane.state
    }
}

impl crate::orchestration::CommandHost for PlaneCommands {
    fn mutation_lock(&self) -> &tokio::sync::Mutex<()> {
        &self.plane.mutation_lock
    }
    fn policy_summary(&self) -> Option<&Value> {
        None
    }
    fn runtime_denial(&self, _: &str) -> Option<String> {
        None
    }
    fn provider_denial(&self, _: &str) -> Option<String> {
        None
    }
    fn effective_settings(&self) -> anyhow::Result<Value> {
        Ok(default_settings())
    }
    fn workspace(&self) -> &dyn Workspace {
        &self.plane.host.workspace
    }
    fn cancellations(
        &self,
    ) -> &tokio::sync::Mutex<HashMap<uuid::Uuid, tokio::sync::oneshot::Sender<()>>> {
        &self.plane.cancellations
    }
    fn launch_run(
        self: Arc<Self>,
        run: crate::Run,
        cwd: String,
        text: String,
        cancel: tokio::sync::oneshot::Receiver<()>,
    ) {
        let plane = self.plane.clone();
        let command = if plane.config.provider_command.is_empty() {
            crate::turn::echo_command()
        } else {
            plane.config.provider_command.clone()
        };
        let spec = crate::workspace::TurnSpec {
            command,
            prompt: text,
            cwd,
            env_remove: Vec::new(),
            ..Default::default()
        };
        let runtime = plane.host.runtime.clone();
        runtime.spawn(Box::pin(crate::turn::launch(plane, run, spec, cancel)));
    }
    fn close_guest_thread(&self, _: &str) -> Result<(), String> {
        Err("Guest shares are not available here.".into())
    }
    fn resolve_guest_message(&self, _: &str, _: &str, _: &str) -> Result<(), String> {
        Err("Guest shares are not available here.".into())
    }
}

impl crate::turn::TurnHost for ControlPlane {
    fn workspace(&self) -> &dyn Workspace {
        &self.host.workspace
    }
    fn cancellations(
        &self,
    ) -> &tokio::sync::Mutex<HashMap<uuid::Uuid, tokio::sync::oneshot::Sender<()>>> {
        &self.cancellations
    }
    fn turn_started(&self, run: &crate::Run, cwd: &str, stream: u64) {
        let record = TurnRecord {
            stream,
            cwd: cwd.to_owned(),
        };
        let path = Self::turn_path(&run.id);
        if let Some(dir) = path.parent() {
            let _ = self.documents.create_dir_all(dir);
        }
        let written = serde_json::to_vec(&record)
            .map_err(std::io::Error::from)
            .and_then(|bytes| {
                self.documents
                    .write(&path, &bytes, crate::persistence::WriteMode::Replace)
            });
        if let Err(error) = written {
            tracing::error!(%error, "could not record a turn's stream; a restart will end it");
        }
    }
    fn turn_recorded(
        &self,
        _run: &crate::Run,
        _position: Option<crate::workspace::StreamPosition>,
    ) {
        self.wake();
    }
    fn turn_finished(&self, run: &crate::Run) {
        let _ = self.documents.remove(&Self::turn_path(&run.id));
        self.wake();
    }
}

/// What a restarted plane needs to resume a running turn, kept in its
/// documents while the turn runs. The last recorded position comes from the
/// event log, which records it with each output.
#[derive(Serialize, Deserialize)]
struct TurnRecord {
    stream: u64,
    cwd: String,
}

enum Call {
    Done(Reply),
    /// A stream that reports later changes: its record and first batches.
    Live(Subscription, Vec<Value>),
    /// Stream batches sent now; the stream stays open until interrupted.
    Stream(Vec<Value>),
}

/// Frames as text, split into parts for clients that asked for them.
fn encode(connection: &Connection, frames: &[Value]) -> Vec<String> {
    frames
        .iter()
        .flat_map(|frame| {
            let text = crate::wire_json::compact(frame);
            if connection.frame_parts {
                split_socket_frame(&text, SOCKET_FRAME_PART_BYTES)
            } else {
                vec![text]
            }
        })
        .collect()
}

fn default_settings() -> Value {
    use crate::settings::schema;
    schema::normalize(&json!({}))
        .and_then(|settings| schema::normalize_runtime(&settings))
        .expect("default settings normalize")
}

async fn descriptor(State(plane): State<Arc<ControlPlane>>) -> Json<Value> {
    Json(plane.descriptor())
}

async fn shell_snapshot(
    State(plane): State<Arc<ControlPlane>>,
    headers: HeaderMap,
    Query(query): Query<HashMap<String, String>>,
) -> Response {
    let session = match access_http::principal(&*plane, &headers) {
        Ok(session) => session,
        Err(reason) => return crate::auth_error(&headers, reason),
    };
    for scope in auth_service::permissions::http_scopes("/api/orchestration/shell", "GET") {
        if !session.has(scope) {
            return access_http::scope_error(&headers, scope);
        }
    }
    if headers
        .get("x-t3-orchestration-protocol")
        .is_none_or(|v| v != "2")
    {
        return StatusCode::BAD_REQUEST.into_response();
    }
    let sequence = plane.sequence_now();
    let store = plane.store_now();
    if query.get("resumeFrom").and_then(|v| v.parse::<u64>().ok()) == Some(sequence) {
        return Json(json!({"resumable":true})).into_response();
    }
    Json(environment::shell_snapshot(&store, sequence, &query)).into_response()
}

async fn unavailable(uri: Uri) -> Response {
    (
        StatusCode::NOT_IMPLEMENTED,
        Json(json!({
            "_tag": "EnvironmentHttpUnavailableError",
            "message": format!("{} is not served by the control plane yet.", uri.path())
        })),
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event_log::MemoryLog;
    use crate::persistence::MemoryStore;
    use crate::runtime::{Task, Wait};
    use crate::workspace::fake_agentd::FakeAgentd;
    use crate::workspace_protocol::WireTurnEvent;
    use futures_util::{stream::FuturesUnordered, StreamExt};

    /// A runtime the test drives by hand: spawned tasks run when
    /// [`run`](Self::run) polls them, and the alarm is a stored time.
    #[derive(Default)]
    struct ManualRuntime {
        spawned: std::sync::Mutex<Vec<Task>>,
        tasks: std::sync::Mutex<FuturesUnordered<Task>>,
        alarm: std::sync::Mutex<Option<i64>>,
    }

    impl Runtime for ManualRuntime {
        fn spawn(&self, task: Task) {
            self.spawned.lock().unwrap().push(task);
        }
        fn sleep(&self, _: std::time::Duration) -> Wait {
            Box::pin(futures_util::future::ready(()))
        }
        fn yield_now(&self) -> Wait {
            Box::pin(futures_util::future::ready(()))
        }
        fn set_alarm(&self, at_ms: i64) {
            *self.alarm.lock().unwrap() = Some(at_ms);
        }
        fn push(&self, _: crate::runtime::ConnId, _: Vec<Value>) {}
        fn connections(&self) -> Vec<crate::runtime::ConnId> {
            Vec::new()
        }
    }

    impl ManualRuntime {
        /// Poll every task until none can make progress.
        fn run(&self) {
            let mut context = std::task::Context::from_waker(futures_util::task::noop_waker_ref());
            loop {
                let spawned = std::mem::take(&mut *self.spawned.lock().unwrap());
                let mut tasks = self.tasks.lock().unwrap();
                tasks.extend(spawned);
                let progressed = matches!(
                    tasks.poll_next_unpin(&mut context),
                    std::task::Poll::Ready(Some(()))
                );
                drop(tasks);
                if !progressed && self.spawned.lock().unwrap().is_empty() {
                    return;
                }
            }
        }

        fn take_alarm(&self) -> Option<i64> {
            self.alarm.lock().unwrap().take()
        }
    }

    fn config() -> Config {
        Config {
            environment_id: "env-test".into(),
            label: "test".into(),
            bootstrap_credential: "secret".into(),
            cookie_port: 3773,
            provider_command: vec!["provider".into()],
        }
    }

    fn host(runtime: &Arc<ManualRuntime>) -> Host {
        Host {
            runtime: runtime.clone(),
            workspace: RemoteWorkspace::default(),
        }
    }

    fn plane() -> Arc<ControlPlane> {
        ControlPlane::open(
            config(),
            Arc::new(MemoryStore::new()),
            Box::new(MemoryLog::default()),
            host(&Arc::default()),
        )
        .unwrap()
    }

    fn owner(plane: &ControlPlane) -> Connection {
        let (_, token) = plane.auth.browser_session("secret", None).unwrap();
        let mut headers = HeaderMap::new();
        headers.insert(
            "cookie",
            format!("t3_session_3773={token}").parse().unwrap(),
        );
        plane
            .authorize_socket(&headers, &"/ws?orchestrationProtocol=2".parse().unwrap())
            .map_err(|_| "rejected")
            .unwrap()
    }

    /// Runs `future` to completion without suspending, as a host that
    /// handles one message at a time needs.
    fn now<T>(future: impl std::future::Future<Output = T>) -> T {
        futures_util::FutureExt::now_or_never(future).expect("completes without waiting")
    }

    fn parse(frames: Vec<String>) -> Vec<Value> {
        frames
            .iter()
            .map(|text| serde_json::from_str(text).unwrap())
            .collect()
    }

    fn send(plane: &Arc<ControlPlane>, connection: &mut Connection, frame: Value) -> Vec<Value> {
        parse(now(plane.on_message(connection, &frame.to_string())))
    }

    #[test]
    fn a_live_thread_stream_reports_later_commits() {
        let plane = plane();
        let mut connection = owner(&plane);
        let project = send(
            &plane,
            &mut connection,
            json!({"_tag":"Request","id":0,"tag":"projects.mutate","payload":{"type":"project.create","commandId":"c1","projectId":"p1","title":"One","workspaceRoot":"/workspace/one"},"headers":[]}),
        );
        assert_eq!(project[0]["exit"]["_tag"], "Success", "{project:?}");
        plane
            .commit(Event::ThreadCreated(
                serde_json::from_value(json!({
                    "id": "t1", "project_id": "p1", "title": "One", "cwd": "/workspace/one",
                    "status": "idle", "messages": [], "runs": [], "created_at": 1,
                    "archived": false, "settled": false, "snoozed_until": null,
                    "pinned": false, "unread": false, "tags": []
                }))
                .unwrap(),
            ))
            .unwrap();
        plane.take_signals();
        let first = send(
            &plane,
            &mut connection,
            json!({"_tag":"Request","id":1,"tag":"orchestration.subscribeThread","payload":{"threadId":"t1","requestCompletionMarker":true},"headers":[]}),
        );
        assert_eq!(first[0]["values"][0]["kind"], "snapshot");
        assert_eq!(first[0]["values"][1], json!({"kind":"synchronized"}));

        plane
            .commit(Event::ThreadPatched {
                thread_id: "t1".into(),
                patch: json!({"title":"Two"}),
            })
            .unwrap();
        let signals = plane.take_signals();
        let pushed = parse(now(plane.deliver(&connection, &signals)));
        assert_eq!(pushed.len(), 1, "{pushed:?}");
        assert_eq!(pushed[0]["_tag"], "Chunk");
        assert_eq!(pushed[0]["requestId"], 1);
        assert_eq!(
            pushed[0]["values"][0]["projection"]["thread"]["title"],
            "Two"
        );
        assert!(
            now(plane.deliver(&connection, &signals)).is_empty(),
            "the cursor moved past it"
        );

        send(
            &plane,
            &mut connection,
            json!({"_tag":"Interrupt","requestId":1}),
        );
        plane
            .commit(Event::ThreadPatched {
                thread_id: "t1".into(),
                patch: json!({"title":"Three"}),
            })
            .unwrap();
        let signals = plane.take_signals();
        assert!(now(plane.deliver(&connection, &signals)).is_empty());
    }

    #[test]
    fn answers_the_probe_and_streams_until_interrupted() {
        let plane = plane();
        let mut connection = owner(&plane);
        let probe = send(
            &plane,
            &mut connection,
            json!({"_tag":"Request","id":0,"tag":"server.probe","payload":{},"headers":[]}),
        );
        assert_eq!(
            probe,
            vec![json!({"_tag":"Exit","requestId":0,"exit":{"_tag":"Success","value":{}}})]
        );
        let shell = send(
            &plane,
            &mut connection,
            json!({"_tag":"Request","id":1,"tag":"orchestration.subscribeShell","payload":{"requestCompletionMarker":true},"headers":[]}),
        );
        assert_eq!(shell[0]["values"][1], json!({"kind":"synchronized"}));
        let interrupted = send(
            &plane,
            &mut connection,
            json!({"_tag":"Interrupt","requestId":1}),
        );
        assert_eq!(interrupted[0]["exit"]["cause"][0]["_tag"], "Interrupt");
    }

    /// An event log that outlives the plane, as durable storage does.
    #[derive(Clone, Default)]
    struct SharedLog(Arc<std::sync::Mutex<Vec<ServerEvent>>>);
    impl EventLog for SharedLog {
        fn load(&mut self) -> anyhow::Result<Vec<ServerEvent>> {
            Ok(self.0.lock().unwrap().clone())
        }
        fn append(&mut self, record: &ServerEvent) -> anyhow::Result<()> {
            self.0.lock().unwrap().push(record.clone());
            Ok(())
        }
    }

    /// A host that can be suspended at any point between messages. Only what
    /// it stores survives: the documents, the event log and each socket's
    /// serialized connection. Suspending drops the plane with everything it
    /// held in memory; the next message rebuilds it from storage.
    struct SuspendingHost {
        documents: Arc<crate::persistence::MemoryStore>,
        log: SharedLog,
        sockets: BTreeMap<u64, String>,
        plane: Option<Arc<ControlPlane>>,
    }

    impl SuspendingHost {
        fn new() -> Self {
            Self {
                documents: Arc::new(MemoryStore::new()),
                log: SharedLog::default(),
                sockets: BTreeMap::new(),
                plane: None,
            }
        }

        fn plane(&mut self) -> &Arc<ControlPlane> {
            let (documents, log) = (self.documents.clone(), self.log.clone());
            let open: Vec<Connection> = self
                .sockets
                .values()
                .map(|stored| serde_json::from_str(stored).unwrap())
                .collect();
            self.plane.get_or_insert_with(|| {
                let plane =
                    ControlPlane::open(config(), documents, Box::new(log), host(&Arc::default()))
                        .unwrap();
                // As the host does when it is rebuilt from storage.
                plane.prune_streams(&open);
                plane
            })
        }

        /// The socket dies without a close event: its records stay behind.
        fn lose(&mut self, socket: u64) {
            self.sockets.remove(&socket);
        }

        fn records(&self) -> Vec<PathBuf> {
            self.documents
                .list(&std::path::Path::new(STATE_DIR).join("streams"))
                .unwrap()
        }

        fn suspend(&mut self) {
            self.plane = None;
        }

        fn connect(&mut self, socket: u64) -> Vec<(u64, Value)> {
            let connection = owner(self.plane());
            self.sockets
                .insert(socket, serde_json::to_string(&connection).unwrap());
            self.fan_out()
        }

        /// One client message, then what pending changes add to every
        /// socket. Returns the frames sent, by socket.
        fn message(&mut self, socket: u64, frame: Value) -> Vec<(u64, Value)> {
            let mut connection: Connection = serde_json::from_str(&self.sockets[&socket]).unwrap();
            let replies = parse(now(self
                .plane()
                .on_message(&mut connection, &frame.to_string())));
            self.sockets
                .insert(socket, serde_json::to_string(&connection).unwrap());
            let mut sent: Vec<_> = replies.into_iter().map(|frame| (socket, frame)).collect();
            sent.extend(self.fan_out());
            sent
        }

        /// A change made outside any client message.
        fn commit(&mut self, event: Event) -> Vec<(u64, Value)> {
            self.plane().commit(event).unwrap();
            self.fan_out()
        }

        fn fan_out(&mut self) -> Vec<(u64, Value)> {
            let signals = self.plane().take_signals();
            let mut sent = Vec::new();
            for (socket, stored) in self.sockets.clone() {
                let connection: Connection = serde_json::from_str(&stored).unwrap();
                for frame in parse(now(self.plane().deliver(&connection, &signals))) {
                    sent.push((socket, frame));
                }
            }
            sent
        }
    }

    fn retitle(title: &str) -> Event {
        Event::ThreadPatched {
            thread_id: "t1".into(),
            patch: json!({ "title": title }),
        }
    }

    #[test]
    fn live_streams_resume_from_their_cursor_after_a_suspension() {
        let mut host = SuspendingHost::new();
        host.connect(1);
        host.connect(2);
        host.message(1, json!({"_tag":"Request","id":0,"tag":"projects.mutate","payload":{"type":"project.create","commandId":"c1","projectId":"p1","title":"One","workspaceRoot":"/workspace/one"},"headers":[]}));
        host.commit(Event::ThreadCreated(
            serde_json::from_value(json!({
                "id": "t1", "project_id": "p1", "title": "One", "cwd": "/workspace/one",
                "status": "idle", "messages": [], "runs": [], "created_at": 1,
                "archived": false, "settled": false, "snoozed_until": null,
                "pinned": false, "unread": false, "tags": []
            }))
            .unwrap(),
        ));
        let opened = host.message(1, json!({"_tag":"Request","id":7,"tag":"orchestration.subscribeThread","payload":{"threadId":"t1"},"headers":[]}));
        assert_eq!(opened.len(), 1);
        assert_eq!(
            opened[0].1["values"][0]["projection"]["thread"]["title"],
            "One"
        );
        let access = host.message(2, json!({"_tag":"Request","id":"a","tag":"subscribeAuthAccess","payload":{},"headers":[]}));
        assert_eq!(access[0].1["values"][0]["revision"], 1);

        host.suspend();

        // A change that leaves the thread as it was sends nothing: the
        // rebuilt plane compares against the stored cursor, not a fresh one.
        let unchanged = host.commit(retitle("One"));
        assert!(
            unchanged.iter().all(|(socket, _)| *socket != 1),
            "{unchanged:?}"
        );

        let changed = host.commit(retitle("Two"));
        let thread: Vec<_> = changed.iter().filter(|(socket, _)| *socket == 1).collect();
        assert_eq!(thread.len(), 1, "{changed:?}");
        assert_eq!(thread[0].1["requestId"], 7);
        assert_eq!(
            thread[0].1["values"][0]["projection"]["thread"]["title"],
            "Two"
        );

        host.suspend();

        // The cursor delivery moved was stored too.
        let again = host.commit(retitle("Two"));
        assert!(again.iter().all(|(socket, _)| *socket != 1), "{again:?}");

        // Auth access numbering continues from the stored revision: a new
        // socket's session and presence reach socket 2's stream as revision
        // 2 onwards, not 1 again.
        let joined = host.connect(3);
        let revisions: Vec<_> = joined
            .iter()
            .filter(|(socket, _)| *socket == 2)
            .map(|(_, frame)| frame["values"][0]["revision"].as_u64().unwrap())
            .collect();
        assert!(!revisions.is_empty(), "{joined:?}");
        assert_eq!(revisions[0], 2, "{joined:?}");
        assert!(revisions.windows(2).all(|pair| pair[1] == pair[0] + 1));

        // Interrupting after a suspension still removes the stored stream.
        host.suspend();
        let interrupted = host.message(1, json!({"_tag":"Interrupt","requestId":7}));
        assert_eq!(interrupted[0].1["exit"]["cause"][0]["_tag"], "Interrupt");
        assert!(host.commit(retitle("Four")).is_empty());
    }

    #[test]
    fn a_rebuilt_plane_deletes_records_of_connections_that_are_gone() {
        let mut host = SuspendingHost::new();
        host.connect(1);
        host.connect(2);
        host.message(1, json!({"_tag":"Request","id":"a","tag":"subscribeAuthAccess","payload":{},"headers":[]}));
        host.message(2, json!({"_tag":"Request","id":"b","tag":"subscribeAuthAccess","payload":{},"headers":[]}));
        let records = host.records();
        assert_eq!(records.len(), 2, "{records:?}");
        let id = |socket: u64| -> String {
            let connection: Connection = serde_json::from_str(&host.sockets[&socket]).unwrap();
            connection.id
        };
        let (gone, kept) = (id(1), id(2));

        // Socket 1 dies without a close event, so `disconnect` never runs.
        host.lose(1);
        host.suspend();
        assert_eq!(host.records().len(), 2, "nothing deletes them yet");

        // Rebuilding the plane keeps only the open socket's record...
        host.plane();
        let records = host.records();
        assert_eq!(records.len(), 1, "{records:?}");
        let owner = |path: &PathBuf| path.parent().unwrap().file_name().unwrap().to_owned();
        assert_eq!(owner(&records[0]), kept.as_str());
        assert_ne!(owner(&records[0]), gone.as_str());

        // ...which still reports changes (a new session changes access).
        host.plane().auth.browser_session("secret", None).unwrap();
        let sent = host.fan_out();
        assert!(sent.iter().all(|(socket, _)| *socket == 2), "{sent:?}");
        assert!(
            sent.iter().any(|(_, frame)| frame["requestId"] == "b"),
            "{sent:?}"
        );
    }

    #[test]
    fn creates_threads_through_orchestration_commands() {
        let plane = plane();
        let mut connection = owner(&plane);
        send(
            &plane,
            &mut connection,
            json!({"_tag":"Request","id":0,"tag":"projects.mutate","payload":{"type":"project.create","commandId":"c1","projectId":"p1","title":"One","workspaceRoot":"/workspace/one"},"headers":[]}),
        );
        let create = json!({"_tag":"Request","id":1,"tag":"orchestration.dispatchCommand","payload":{
            "type":"thread.create","commandId":"c2","threadId":"t1","projectId":"p1","title":"Draft",
            "modelSelection":{"instanceId":"codex","model":"fixture"},"runtimeMode":"full-access",
            "interactionMode":"default","branch":null,"worktreePath":null,"createdBy":"user","creationSource":"web"
        },"headers":[]});
        let created = send(&plane, &mut connection, create.clone());
        assert_eq!(created[0]["exit"]["value"]["sequence"], 2, "{created:?}");
        // Replaying the command id is idempotent, as natively.
        let mut replay = create;
        replay["id"] = json!(2);
        let replayed = send(&plane, &mut connection, replay);
        assert_eq!(replayed[0]["exit"]["value"]["sequence"], 2, "{replayed:?}");
        let thread = send(
            &plane,
            &mut connection,
            json!({"_tag":"Request","id":3,"tag":"orchestration.subscribeThread","payload":{"threadId":"t1","requestCompletionMarker":true},"headers":[]}),
        );
        assert_eq!(
            thread[0]["values"][0]["projection"]["thread"]["title"],
            "Draft"
        );
        assert_eq!(thread[0]["values"][1], json!({"kind":"synchronized"}));
    }

    /// A desktop client's DPoP key (a fixed P-256 test key).
    const TEST_KEY: &str = "-----BEGIN PRIVATE KEY-----
MIGHAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBG0wawIBAQQgZliUoRhEGT671J/s
nAXKuByunnPT+6q5wiJ8eoRZJbChRANCAATsNcEJTRae+VunFX1XZc4WLj05fQ1B
xjwIHRE7S5fx9m9ZBJY9QKqAVZwYEwuHZpcikkHGCc4GH6/rRFHJN7BQ
-----END PRIVATE KEY-----";

    fn dpop(method: &str, url: &str, access: Option<&str>) -> String {
        use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
        use sha2::Digest;
        let header = json!({"alg":"ES256","typ":"dpop+jwt","jwk":{"crv":"P-256","kty":"EC",
            "x":"7DXBCU0WnvlbpxV9V2XOFi49OX0NQcY8CB0RO0uX8fY","y":"b1kElj1AqoBVnBgTC4dmlyKSQcYJzgYfr-tEUck3sFA"}});
        let mut claims = json!({"htm":method,"htu":url,"jti":crate::platform::uuid_v4().to_string(),
            "iat":crate::platform::now_ms() / 1000});
        if let Some(access) = access {
            claims["ath"] = json!(URL_SAFE_NO_PAD.encode(sha2::Sha256::digest(access.as_bytes())));
        }
        let message = format!(
            "{}.{}",
            URL_SAFE_NO_PAD.encode(header.to_string()),
            URL_SAFE_NO_PAD.encode(claims.to_string())
        );
        let key = jsonwebtoken::EncodingKey::from_ec_pem(TEST_KEY.as_bytes()).unwrap();
        let signature =
            jsonwebtoken::crypto::sign(message.as_bytes(), &key, jsonwebtoken::Algorithm::ES256)
                .unwrap();
        format!("{message}.{signature}")
    }

    #[test]
    fn accepts_a_socket_ticket_with_its_dpop_proof() {
        use tower::ServiceExt;
        let plane = Arc::new(plane());
        let base = "https://env.example.test";
        let request = |method: &str, path: &str, headers: Vec<(&str, String)>, body: String| {
            let mut builder = axum::http::Request::builder()
                .method(method)
                .uri(path)
                .header("host", "env.example.test")
                .header("x-forwarded-proto", "https");
            for (name, value) in headers {
                builder = builder.header(name, value);
            }
            builder.body(axum::body::Body::from(body)).unwrap()
        };
        let read = |response: Response| -> Value {
            let bytes = now(axum::body::to_bytes(response.into_body(), usize::MAX)).unwrap();
            serde_json::from_slice(&bytes).unwrap()
        };
        let form = "grant_type=urn%3Aietf%3Aparams%3Aoauth%3Agrant-type%3Atoken-exchange&subject_token=secret&subject_token_type=urn%3At3%3Aparams%3Aoauth%3Atoken-type%3Aenvironment-bootstrap&requested_token_type=urn%3Aietf%3Aparams%3Aoauth%3Atoken-type%3Aaccess_token";
        let token = read(
            now(plane.router().oneshot(request(
                "POST",
                "/oauth/token",
                vec![
                    ("content-type", "application/x-www-form-urlencoded".into()),
                    ("dpop", dpop("POST", &format!("{base}/oauth/token"), None)),
                ],
                form.into(),
            )))
            .unwrap(),
        );
        let access = token["access_token"]
            .as_str()
            .expect("an access token")
            .to_owned();
        let ticket = read(
            now(plane.router().oneshot(request(
                "POST",
                "/api/auth/websocket-ticket/rpc",
                vec![
                    ("authorization", format!("DPoP {access}")),
                    (
                        "dpop",
                        dpop(
                            "POST",
                            &format!("{base}/api/auth/websocket-ticket/rpc"),
                            Some(&access),
                        ),
                    ),
                ],
                String::new(),
            )))
            .unwrap(),
        );
        let ticket = ticket["ticket"].as_str().expect("a ticket").to_owned();

        let mut headers = HeaderMap::new();
        headers.insert("host", "env.example.test".parse().unwrap());
        headers.insert("x-forwarded-proto", "https".parse().unwrap());
        let socket = |ticket: &str, proof: &str| -> Uri {
            let query: String = url::form_urlencoded::Serializer::new(String::new())
                .append_pair("orchestrationProtocol", "2")
                .append_pair("wsTicket", ticket)
                .append_pair("dpop", proof)
                .finish();
            format!("/ws?{query}").parse().unwrap()
        };
        // A proof for another URL is refused.
        let wrong = socket(
            &ticket,
            &dpop("GET", &format!("{base}/other"), Some(&ticket)),
        );
        assert!(plane.authorize_socket(&headers, &wrong).is_err());
        let uri = socket(&ticket, &dpop("GET", &format!("{base}/ws"), Some(&ticket)));
        assert!(
            plane.authorize_socket(&headers, &uri).is_ok(),
            "the ticket and its proof open the socket"
        );
    }

    #[test]
    fn rejects_sockets_without_a_credential() {
        let plane = plane();
        assert!(plane
            .authorize_socket(
                &HeaderMap::new(),
                &"/ws?orchestrationProtocol=2".parse().unwrap()
            )
            .is_err());
    }

    #[test]
    fn rebuilds_the_store_from_its_log() {
        let plane = plane();
        owner(&plane);
        plane
            .commit(Event::ProjectCreated(crate::Project {
                id: "p1".into(),
                name: "One".into(),
                path: "/workspace/one".into(),
                created_at: 1,
            }))
            .unwrap();
        let records = plane.log.try_lock().unwrap().load().unwrap();
        let mut log = MemoryLog::default();
        for record in &records {
            log.append(record).unwrap();
        }
        let reopened = ControlPlane::open(
            config(),
            plane.documents.clone(),
            Box::new(log),
            host(&Arc::default()),
        )
        .unwrap();
        assert_eq!(reopened.sequence(), 1);
        assert_eq!(
            reopened.auth.records_json(),
            plane.auth.records_json(),
            "sessions survive through the document store"
        );
        assert!(reopened.store_now().projects.contains_key("p1"));
    }

    fn thread_runs(plane: &ControlPlane) -> Vec<crate::Run> {
        plane.store_now().threads["t1"].runs.clone()
    }

    fn output(plane: &ControlPlane) -> Vec<String> {
        plane
            .log
            .try_lock()
            .unwrap()
            .load()
            .unwrap()
            .into_iter()
            .filter_map(|record| match record.event {
                Event::RunOutput { text, .. } => Some(text),
                _ => None,
            })
            .collect()
    }

    /// A project and a thread, created as a client would.
    fn seed(plane: &Arc<ControlPlane>, connection: &mut Connection) {
        send(
            plane,
            connection,
            json!({"_tag":"Request","id":"p","tag":"projects.mutate","payload":{"type":"project.create","commandId":"c1","projectId":"p1","title":"One","workspaceRoot":"/workspace/one"},"headers":[]}),
        );
        let created = send(
            plane,
            connection,
            json!({"_tag":"Request","id":"t","tag":"orchestration.dispatchCommand","payload":{
                "type":"thread.create","commandId":"c2","threadId":"t1","projectId":"p1","title":"Draft",
                "modelSelection":{"instanceId":"codex","model":"fixture"},"runtimeMode":"full-access",
                "interactionMode":"default","branch":null,"worktreePath":null,"createdBy":"user","creationSource":"web"
            },"headers":[]}),
        );
        assert_eq!(created[0]["exit"]["_tag"], "Success", "{created:?}");
    }

    fn dispatch_message(plane: &Arc<ControlPlane>, connection: &mut Connection, text: &str) {
        let sent = send(
            plane,
            connection,
            json!({"_tag":"Request","id":"m","tag":"orchestration.dispatchCommand","payload":{
                "type":"message.dispatch","commandId":"c3","threadId":"t1","messageId":"message:1","text":text,
                "attachments":[],"createdBy":"user","creationSource":"web","dispatchMode":{"type":"start_immediately"}
            },"headers":[]}),
        );
        assert_eq!(sent[0]["exit"]["_tag"], "Success", "{sent:?}");
    }

    #[test]
    fn runs_a_turn_in_its_workspace_and_acknowledges_what_it_records() {
        let runtime = Arc::new(ManualRuntime::default());
        let plane = ControlPlane::open(
            config(),
            Arc::new(MemoryStore::new()),
            Box::new(MemoryLog::default()),
            host(&runtime),
        )
        .unwrap();
        let agentd = Arc::new(FakeAgentd::default());
        now(plane.workspace_connected(agentd.link(plane.workspace()))).unwrap();
        assert!(
            runtime.take_alarm().is_some(),
            "a connected workspace is maintained"
        );
        let mut connection = owner(&plane);
        seed(&plane, &mut connection);
        let thread = send(
            &plane,
            &mut connection,
            json!({"_tag":"Request","id":"s","tag":"orchestration.subscribeThread","payload":{"threadId":"t1"},"headers":[]}),
        );
        assert_eq!(thread[0]["_tag"], "Chunk");
        plane.take_signals();

        dispatch_message(&plane, &mut connection, "hello");
        runtime.run();
        let turns = agentd.turns();
        let (&stream, start) = turns
            .iter()
            .next()
            .expect("the turn started in the workspace");
        assert_eq!(start.command, vec!["provider".to_string()]);
        assert_eq!(start.prompt, "hello");
        assert_eq!(start.cwd, "/workspace/one");

        agentd.emit(
            stream,
            WireTurnEvent::Stdout {
                text: "hi\n".into(),
            },
        );
        agentd.emit(
            stream,
            WireTurnEvent::Stderr {
                text: "note\n".into(),
            },
        );
        // Nothing is acknowledged before it is recorded.
        assert_eq!(agentd.acked(stream), 0);
        runtime.run();
        assert_eq!(output(&plane), vec!["hi\n", "note\n"]);
        assert_eq!(agentd.acked(stream), 2);
        // The host is asked to deliver what the turn recorded.
        let due = runtime
            .take_alarm()
            .expect("an alarm to deliver the output");
        assert!(due <= plane.host.runtime.now_ms());
        now(plane.on_alarm());
        let signals = plane.take_signals();
        let pushed = parse(now(plane.deliver(&connection, &signals)));
        assert!(
            pushed.iter().any(|frame| frame["requestId"] == "s"),
            "{pushed:?}"
        );

        agentd.emit(
            stream,
            WireTurnEvent::Exited {
                success: true,
                cancelled: false,
                status: "exit status: 0".into(),
            },
        );
        runtime.run();
        let runs = thread_runs(&plane);
        assert_eq!(runs.len(), 1);
        assert_eq!(runs[0].status, "completed");
        assert!(
            agentd.turns_acked(),
            "the end is acknowledged once recorded"
        );
    }

    #[test]
    fn a_turn_that_cannot_start_ends_as_failed() {
        let runtime = Arc::new(ManualRuntime::default());
        let plane = ControlPlane::open(
            config(),
            Arc::new(MemoryStore::new()),
            Box::new(MemoryLog::default()),
            host(&runtime),
        )
        .unwrap();
        let mut connection = owner(&plane);
        seed(&plane, &mut connection);
        dispatch_message(&plane, &mut connection, "hello");
        runtime.run();
        let runs = thread_runs(&plane);
        assert_eq!(runs[0].status, "failed");
        assert_eq!(
            runs[0].error.as_deref(),
            Some("the workspace is not connected")
        );
        assert!(runtime.take_alarm().is_some(), "the failure is delivered");
    }

    #[test]
    fn stopping_a_thread_cancels_its_turn_on_the_next_alarm() {
        let runtime = Arc::new(ManualRuntime::default());
        let plane = ControlPlane::open(
            config(),
            Arc::new(MemoryStore::new()),
            Box::new(MemoryLog::default()),
            host(&runtime),
        )
        .unwrap();
        let agentd = Arc::new(FakeAgentd::default());
        now(plane.workspace_connected(agentd.link(plane.workspace()))).unwrap();
        let mut connection = owner(&plane);
        seed(&plane, &mut connection);
        dispatch_message(&plane, &mut connection, "hello");
        runtime.run();
        let stopped = send(
            &plane,
            &mut connection,
            json!({"_tag":"Request","id":"x","tag":"orchestration.dispatchCommand","payload":{
                "type":"thread.stop","commandId":"c4","threadId":"t1","createdBy":"user","creationSource":"web"
            },"headers":[]}),
        );
        assert_eq!(stopped[0]["exit"]["_tag"], "Success", "{stopped:?}");
        // Without a task of its own to wait on the cancellation, the
        // workspace forwards it when the host calls back.
        now(plane.on_alarm());
        runtime.run();
        assert_eq!(thread_runs(&plane)[0].status, "cancelled");
    }

    /// Storage that outlives a plane, and a plane over it.
    struct Restartable {
        documents: Arc<MemoryStore>,
        log: SharedLog,
        runtime: Arc<ManualRuntime>,
        plane: Arc<ControlPlane>,
    }

    impl Restartable {
        fn new(agentd: &Arc<FakeAgentd>) -> Self {
            let documents = Arc::new(MemoryStore::new());
            let log = SharedLog::default();
            let (runtime, plane) = Self::open(&documents, &log, agentd);
            Self {
                documents,
                log,
                runtime,
                plane,
            }
        }

        fn open(
            documents: &Arc<MemoryStore>,
            log: &SharedLog,
            agentd: &Arc<FakeAgentd>,
        ) -> (Arc<ManualRuntime>, Arc<ControlPlane>) {
            let runtime = Arc::new(ManualRuntime::default());
            let plane = ControlPlane::open(
                config(),
                documents.clone(),
                Box::new(log.clone()),
                host(&runtime),
            )
            .unwrap();
            now(plane.workspace_connected(agentd.link(plane.workspace()))).unwrap();
            runtime.run();
            (runtime, plane)
        }

        /// Stop the plane with whatever it had in memory, then rebuild it
        /// from storage and reconnect it to the same agentd.
        fn restart(&mut self, agentd: &Arc<FakeAgentd>) {
            agentd.disconnect();
            let (runtime, plane) = Self::open(&self.documents, &self.log, agentd);
            self.runtime = runtime;
            self.plane = plane;
        }

        fn output(&self) -> Vec<String> {
            self.log
                .0
                .lock()
                .unwrap()
                .iter()
                .filter_map(|record| match &record.event {
                    Event::RunOutput { text, .. } => Some(text.clone()),
                    _ => None,
                })
                .collect()
        }
    }

    fn line(text: &str) -> WireTurnEvent {
        WireTurnEvent::Stdout {
            text: format!("{text}\n"),
        }
    }

    fn exit_ok() -> WireTurnEvent {
        WireTurnEvent::Exited {
            success: true,
            cancelled: false,
            status: "exit status: 0".into(),
        }
    }

    #[test]
    fn a_turn_keeps_going_across_a_restart_without_losing_or_repeating_output() {
        let agentd = Arc::new(FakeAgentd::default());
        let mut host = Restartable::new(&agentd);
        let mut connection = owner(&host.plane);
        seed(&host.plane, &mut connection);
        dispatch_message(&host.plane, &mut connection, "hello");
        host.runtime.run();
        let stream = *agentd.turns().keys().next().unwrap();
        agentd.emit(stream, line("one"));
        agentd.emit(stream, line("two"));
        host.runtime.run();
        assert_eq!(agentd.acked(stream), 2);

        // Recorded but not acknowledged: the connection drops first.
        agentd.emit(stream, line("three"));
        agentd.disconnect();
        host.runtime.run();
        assert_eq!(agentd.acked(stream), 2);
        // Produced while the server is down.
        agentd.emit(stream, line("four"));

        host.restart(&agentd);
        assert_eq!(agentd.turns().len(), 1, "the turn is not started again");
        assert_eq!(host.output(), vec!["one\n", "two\n", "three\n", "four\n"]);
        assert_eq!(agentd.acked(stream), 4);
        assert_eq!(thread_runs(&host.plane)[0].status, "running");

        agentd.emit(stream, line("five"));
        agentd.emit(stream, exit_ok());
        host.runtime.run();
        assert_eq!(host.output().len(), 5);
        assert_eq!(thread_runs(&host.plane)[0].status, "completed");
        assert!(agentd.turns_acked());
        assert!(
            host.documents
                .list(&std::path::Path::new(STATE_DIR).join("turns"))
                .unwrap()
                .is_empty(),
            "the turn's record is gone once it ends"
        );
    }

    #[test]
    fn a_restart_ends_runs_whose_turn_is_lost() {
        let agentd = Arc::new(FakeAgentd::default());
        let mut host = Restartable::new(&agentd);
        let mut connection = owner(&host.plane);
        seed(&host.plane, &mut connection);
        dispatch_message(&host.plane, &mut connection, "hello");
        host.runtime.run();
        let stream = *agentd.turns().keys().next().unwrap();
        agentd.emit(stream, line("one"));
        host.runtime.run();
        // agentd restarts too and forgets the stream.
        agentd.forget(stream);
        host.restart(&agentd);
        let run = &thread_runs(&host.plane)[0];
        assert_eq!(run.status, "failed");
        assert!(
            run.error
                .as_deref()
                .unwrap_or_default()
                .contains("interrupted"),
            "{run:?}"
        );
    }
}
