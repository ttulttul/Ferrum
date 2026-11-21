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
    server_config,
    subscription::{Signal, Subscription},
    workspace::Workspace,
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

pub struct Config {
    pub environment_id: String,
    pub label: String,
    /// The desktop bootstrap credential the pairing exchange accepts.
    pub bootstrap_credential: String,
    /// The port in the session cookie name (`t3_session_<port>`).
    pub cookie_port: u16,
}

pub struct ControlPlane {
    config: Config,
    started_at: String,
    state: ControlState,
    signals: Mutex<Signals>,
    /// Held while an orchestration command checks and commits.
    mutation_lock: tokio::sync::Mutex<()>,
    cancellations: tokio::sync::Mutex<HashMap<uuid::Uuid, tokio::sync::oneshot::Sender<()>>>,
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
    pub fn open(
        config: Config,
        documents: Arc<dyn DocumentStore>,
        log: Box<dyn EventLog + Send>,
    ) -> anyhow::Result<Self> {
        let auth = AuthService::with_bootstrap(config.bootstrap_credential.clone())
            .with_port(config.cookie_port)
            .open_in(documents.clone(), std::path::Path::new(STATE_DIR))?;
        let started_at = auth_service::iso(auth_service::now());
        let state = ControlState::open(auth, documents, log, std::path::Path::new(STATE_DIR))?;
        let signals = Mutex::new(Signals::new(&state));
        Ok(Self {
            config,
            started_at,
            state,
            signals,
            mutation_lock: Default::default(),
            cancellations: Default::default(),
        })
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
        let principal = match ticket {
            Some(ticket) => access_http::authenticate_ticket(self, headers, uri, &ticket, "rpc"),
            None => access_http::principal(self, headers),
        }
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

    fn forget(&self, connection: &Connection, key: &str) {
        let _ = self.documents.remove(&Self::record_path(connection, key));
    }

    /// Handles one text frame from a client and returns the frames to send.
    /// Workspace methods go to `workspace`.
    pub async fn on_message(
        &self,
        workspace: &dyn Workspace,
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
            Some("Request") => self.request(workspace, connection, &request).await,
            other => vec![
                json!({"_tag":"Defect","defect":format!("Unknown request tag: {}", other.unwrap_or("undefined"))}),
            ],
        };
        encode(connection, &frames)
    }

    async fn request(
        &self,
        workspace: &dyn Workspace,
        connection: &mut Connection,
        request: &Value,
    ) -> Vec<Value> {
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
        match self
            .call(workspace, &connection.principal, method, params)
            .await
        {
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

    async fn call(
        &self,
        workspace: &dyn Workspace,
        principal: &Session,
        method: &str,
        params: &Value,
    ) -> Call {
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
            // WebAssembly build the workspace need not be `Send`.
            #[allow(clippy::arc_with_non_send_sync)]
            let host = Arc::new(PlaneCommands {
                plane: self,
                workspace,
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
        match dispatch::workspace(workspace, &settings, method, params).await {
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
            "providers": server_config::providers::list_with(&self.started_at, |_| None, &settings, options),
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
/// with no managed policy, default settings and no guest shares. Runs need
/// a process to execute in, which the plane does not start yet, so a run
/// that would start ends at once as failed.
struct PlaneCommands<'a> {
    plane: &'a ControlPlane,
    workspace: &'a dyn Workspace,
}

impl std::ops::Deref for PlaneCommands<'_> {
    type Target = ControlState;
    fn deref(&self) -> &ControlState {
        &self.plane.state
    }
}

impl crate::orchestration::CommandHost for PlaneCommands<'_> {
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
        self.workspace
    }
    fn cancellations(
        &self,
    ) -> &tokio::sync::Mutex<HashMap<uuid::Uuid, tokio::sync::oneshot::Sender<()>>> {
        &self.plane.cancellations
    }
    fn launch_run(
        self: Arc<Self>,
        run: crate::Run,
        _cwd: String,
        _text: String,
        _cancel: tokio::sync::oneshot::Receiver<()>,
    ) {
        if let Ok(mut cancellations) = self.plane.cancellations.try_lock() {
            cancellations.remove(&run.id);
        }
        let _ = self.commit_now(Event::RunFinished {
            run_id: run.id,
            thread_id: run.thread_id,
            status: "failed".into(),
            error: Some("Turns cannot run here yet.".into()),
        });
    }
    fn close_guest_thread(&self, _: &str) -> Result<(), String> {
        Err("Guest shares are not available here.".into())
    }
    fn resolve_guest_message(&self, _: &str, _: &str, _: &str) -> Result<(), String> {
        Err("Guest shares are not available here.".into())
    }
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

    fn plane() -> ControlPlane {
        ControlPlane::open(
            Config {
                environment_id: "env-test".into(),
                label: "test".into(),
                bootstrap_credential: "secret".into(),
                cookie_port: 3773,
            },
            Arc::new(MemoryStore::new()),
            Box::new(MemoryLog::default()),
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

    fn send(plane: &ControlPlane, connection: &mut Connection, frame: Value) -> Vec<Value> {
        let workspace = crate::workspace::RemoteWorkspace::default();
        parse(now(plane.on_message(
            &workspace,
            connection,
            &frame.to_string(),
        )))
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
            Config {
                environment_id: "env-test".into(),
                label: "test".into(),
                bootstrap_credential: "secret".into(),
                cookie_port: 3773,
            },
            plane.documents.clone(),
            Box::new(log),
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
}
