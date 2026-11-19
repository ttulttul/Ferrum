use axum::{
    extract::{FromRequest, Request, State, WebSocketUpgrade},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{any, get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    path::{Path as FsPath, PathBuf},
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::{
    io::{AsyncBufReadExt, BufReader},
    process::Command,
    sync::{broadcast, oneshot, Mutex, RwLock},
};
use tracing::info;
use uuid::Uuid;

pub mod access_http;
pub mod asset_http;
pub mod auth_service;
pub mod config;
mod config_rpc;
mod contracts;
mod environment_http;
pub mod guest_service;
pub mod host_rpc;
pub mod issues;
pub mod local_services;
pub mod mcp_oauth;
mod orchestration;
mod projects;
mod settings;
mod wire_json;
pub use settings::policy::ManagedPolicyFileError;
mod thread_projection;
use orchestration::dispatch_orchestration_command;
pub mod provider_http;
pub mod provider_rpc;
pub mod pull_requests;
pub mod relay_http;
pub mod relay_service;
mod rpc;
pub mod voice;

pub use contracts::{http_known, rpc_known, HTTP_CONTRACTS, RPC_CONTRACTS};

pub const PROTOCOL_VERSION: u32 = 2;
fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Project {
    pub id: String,
    pub name: String,
    pub path: String,
    pub created_at: u64,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ConversationMessage {
    pub id: String,
    pub role: String,
    pub content: String,
    pub created_at: u64,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Run {
    pub id: Uuid,
    pub thread_id: String,
    pub provider: String,
    pub status: String,
    pub started_at: u64,
    pub finished_at: Option<u64>,
    pub error: Option<String>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Thread {
    #[serde(default)]
    row: serde_json::Value,
    pub id: String,
    pub project_id: String,
    pub title: String,
    pub cwd: String,
    pub status: String,
    pub messages: Vec<ConversationMessage>,
    pub runs: Vec<Run>,
    pub created_at: u64,
    pub archived: bool,
    pub settled: bool,
    pub snoozed_until: Option<String>,
    pub pinned: bool,
    pub unread: bool,
    pub tags: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", content = "data")]
pub enum Event {
    ProjectCreated(Project),
    ProjectCommitted {
        project: Project,
        row: serde_json::Value,
        command_id: String,
        command_type: String,
        removed_threads: Vec<String>,
    },
    ThreadCreated(Thread),
    MessageAdded {
        thread_id: String,
        message: ConversationMessage,
    },
    RunStarted(Run),
    RunQueued {
        message: ConversationMessage,
        run: Run,
    },
    RunOutput {
        run_id: Uuid,
        thread_id: String,
        stream: String,
        text: String,
    },
    RunFinished {
        run_id: Uuid,
        thread_id: String,
        status: String,
        error: Option<String>,
    },
    CommandDispatched {
        command_id: String,
        command_type: String,
        thread_id: Option<String>,
        payload: serde_json::Value,
    },
    ThreadPatched {
        thread_id: String,
        patch: serde_json::Value,
    },
    ThreadDeleted {
        thread_id: String,
    },
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ServerEvent {
    pub id: u64,
    pub at: u64,
    pub event: Event,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    receipt: Option<CommandReceipt>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct CommandReceipt {
    command_id: String,
    thread_id: String,
}

#[derive(Default)]
pub struct Store {
    pub projects: HashMap<String, Project>,
    project_rows: indexmap::IndexMap<String, serde_json::Value>,
    project_receipts: HashMap<String, (String, String)>,
    deleted_thread_ids: std::collections::HashSet<String>,
    command_receipts: HashMap<String, (String, u64)>,
    pub threads: HashMap<String, Thread>,
}
impl Store {
    fn apply_record(&mut self, record: &ServerEvent) {
        self.apply(&record.event);
        if let Some(receipt) = &record.receipt {
            self.command_receipts.insert(
                receipt.command_id.clone(),
                (receipt.thread_id.clone(), record.id),
            );
        }
    }

    fn apply(&mut self, event: &Event) {
        match event {
            Event::ProjectCreated(p) => {
                self.project_rows.insert(p.id.clone(), projects::wire(p));
                self.projects.insert(p.id.clone(), p.clone());
            }
            Event::ProjectCommitted {
                project,
                row,
                command_id,
                command_type,
                removed_threads,
            } => {
                self.project_rows.insert(project.id.clone(), row.clone());
                self.project_receipts.insert(
                    command_id.clone(),
                    (project.id.clone(), command_type.clone()),
                );
                if row["deletedAt"].is_null() {
                    self.projects.insert(project.id.clone(), project.clone());
                } else {
                    self.projects.remove(&project.id);
                }
                for id in removed_threads {
                    self.threads.remove(id);
                    self.deleted_thread_ids.insert(id.clone());
                }
            }
            Event::ThreadCreated(t) => {
                self.threads.insert(t.id.clone(), t.clone());
            }
            Event::MessageAdded { thread_id, message } => {
                if let Some(t) = self.threads.get_mut(thread_id) {
                    t.messages.push(message.clone());
                }
            }
            Event::RunQueued { message, run } => {
                if let Some(thread) = self.threads.get_mut(&run.thread_id) {
                    thread.messages.push(message.clone());
                    thread.status = "running".into();
                    thread.runs.push(run.clone());
                }
            }
            Event::RunStarted(run) => {
                if let Some(t) = self.threads.get_mut(&run.thread_id) {
                    t.status = "running".into();
                    t.runs.push(run.clone());
                }
            }
            Event::RunOutput { .. } => {}
            Event::RunFinished {
                run_id,
                thread_id,
                status,
                error,
            } => {
                if let Some(t) = self.threads.get_mut(thread_id) {
                    t.status = if status == "completed" {
                        "idle".into()
                    } else {
                        status.clone()
                    };
                    if let Some(run) = t.runs.iter_mut().find(|r| r.id == *run_id) {
                        run.status = status.clone();
                        run.error = error.clone();
                        run.finished_at = Some(now());
                    }
                }
            }
            Event::CommandDispatched { .. } => {}
            Event::ThreadPatched { thread_id, patch } => {
                if let Some(thread) = self.threads.get_mut(thread_id) {
                    if let Some(fields) = patch["row"].as_object() {
                        let mut row = thread.stored_row();
                        row.as_object_mut().unwrap().extend(fields.clone());
                        thread.row = row;
                    }
                    if let Some(value) = patch.get("title").and_then(serde_json::Value::as_str) {
                        thread.title = value.to_owned();
                    }
                    if let Some(value) = patch.get("archived").and_then(serde_json::Value::as_bool)
                    {
                        thread.archived = value;
                    }
                    if let Some(value) = patch.get("settled").and_then(serde_json::Value::as_bool) {
                        thread.settled = value;
                    }
                    if let Some(value) = patch.get("pinned").and_then(serde_json::Value::as_bool) {
                        thread.pinned = value;
                    }
                    if let Some(value) = patch.get("unread").and_then(serde_json::Value::as_bool) {
                        thread.unread = value;
                    }
                    if let Some(value) = patch.get("status").and_then(serde_json::Value::as_str) {
                        thread.status = value.to_owned();
                    }
                    if let Some(value) = patch.get("snoozed_until") {
                        thread.snoozed_until = value.as_str().map(str::to_owned);
                    }
                    if let Some(values) = patch.get("tags").and_then(serde_json::Value::as_array) {
                        thread.tags = values
                            .iter()
                            .filter_map(serde_json::Value::as_str)
                            .map(str::to_owned)
                            .collect();
                    }
                }
            }
            Event::ThreadDeleted { thread_id } => {
                self.threads.remove(thread_id);
                self.deleted_thread_ids.insert(thread_id.clone());
            }
        }
    }
}

pub struct AppState {
    config_started_at: String,
    environment_id: String,
    pub oauth: mcp_oauth::OAuth,
    connect: access_http::connect::Service,
    pub data_dir: PathBuf,
    pub paths: config::ServerPaths,
    pub runtime_config: config::RuntimeConfig,
    pub settings: Arc<settings::Settings>,
    pub store: RwLock<Store>,
    pub events: broadcast::Sender<ServerEvent>,
    pub log: Mutex<tokio::fs::File>,
    pub provider_command: Vec<String>,
    pub cancellations: Mutex<HashMap<Uuid, oneshot::Sender<()>>>,
    pub auth: auth_service::AuthService,
    pub guest: guest_service::GuestService,
    pub voice: voice::VoiceService,
    pub pull_requests: pull_requests::PullRequestService,
    pub issues: tokio::sync::Mutex<issues::IssueService>,
    pub vcs: local_services::VcsService,
    pub terminals: local_services::TerminalService,
    pub previews: local_services::PreviewService,
    pub devices: local_services::DeviceService,
    project_mutation_lock: Mutex<()>,
    next_event_id: Mutex<u64>,
    pub(crate) storage_cleanup_report: Mutex<Option<serde_json::Value>>,
    pub(crate) pull_request_refresh: Mutex<u64>,
}
impl AppState {
    pub async fn new(
        data_dir: impl Into<PathBuf>,
        provider_command: Vec<String>,
    ) -> anyhow::Result<Arc<Self>> {
        Self::with_auth(data_dir, provider_command, auth_service::AuthService::new()).await
    }

    pub async fn with_auth(
        data_dir: impl Into<PathBuf>,
        provider_command: Vec<String>,
        auth: auth_service::AuthService,
    ) -> anyhow::Result<Arc<Self>> {
        Self::with_config(
            data_dir,
            provider_command,
            auth,
            config::RuntimeConfig::load()?,
        )
        .await
    }

    pub async fn with_config(
        data_dir: impl Into<PathBuf>,
        provider_command: Vec<String>,
        auth: auth_service::AuthService,
        runtime_config: config::RuntimeConfig,
    ) -> anyhow::Result<Arc<Self>> {
        let dir = data_dir.into();
        let paths = config::ServerPaths::from_state_dir(dir.clone());
        paths.ensure()?;
        let settings = Arc::new(settings::Settings::open(&paths)?);
        let auth = auth.open(&dir)?;
        auth.revoke_disallowed(
            settings.policy.allows_client("remote"),
            settings.policy.allows_connect(),
        )?;
        let connect = access_http::connect::Service::open(&dir)?;
        let guest = guest_service::GuestService::open(&dir)?;
        if !settings.policy.allows_connect() {
            connect.clear_disabled_link()?;
            guest.set_tunnel_hostname(None);
        }
        let log_path = dir.join("events.jsonl");
        let mut store = Store::default();
        let mut next_event_id = 0;
        if let Ok(contents) = tokio::fs::read_to_string(&log_path).await {
            for line in contents.lines() {
                if let Ok(record) = serde_json::from_str::<ServerEvent>(line) {
                    next_event_id = next_event_id.max(record.id);
                    store.apply_record(&record);
                }
            }
        }
        let log = tokio::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(log_path)
            .await?;
        let (events, _) = broadcast::channel(512);
        Ok(Arc::new(Self {
            config_started_at: auth_service::iso(auth_service::now()),
            environment_id: environment_http::identity(&dir)?,
            oauth: mcp_oauth::OAuth::open(&dir)?,
            connect,
            data_dir: dir.clone(),
            paths,
            runtime_config,
            settings: settings.clone(),
            store: RwLock::new(store),
            events,
            log: Mutex::new(log),
            provider_command,
            cancellations: Mutex::new(HashMap::new()),
            auth,
            guest,
            voice: voice::VoiceService::new(settings.clone()),
            pull_requests: pull_requests::PullRequestService::default(),
            issues: tokio::sync::Mutex::new(issues::IssueService::new(dir.clone())),
            vcs: local_services::VcsService,
            terminals: local_services::TerminalService::with_logs(dir.join("logs/terminals")),
            previews: local_services::PreviewService::default(),
            devices: local_services::DeviceService::new(settings.clone(), &dir)?,
            project_mutation_lock: Mutex::new(()),
            next_event_id: Mutex::new(next_event_id),
            storage_cleanup_report: Mutex::new(None),
            pull_request_refresh: Mutex::new(0),
        }))
    }
    async fn commit(&self, event: Event) -> anyhow::Result<ServerEvent> {
        self.commit_receipted(event, None).await
    }

    async fn commit_receipted(
        &self,
        event: Event,
        receipt: Option<CommandReceipt>,
    ) -> anyhow::Result<ServerEvent> {
        let mut store = self.store.write().await;
        let mut next_event_id = self.next_event_id.lock().await;
        let record = ServerEvent {
            id: *next_event_id + 1,
            at: now(),
            event,
            receipt,
        };
        let mut log = self.log.lock().await;
        use tokio::io::AsyncWriteExt;
        let mut bytes = serde_json::to_vec(&record)?;
        bytes.push(b'\n');
        log.write_all(&bytes).await?;
        log.flush().await?;
        *next_event_id = record.id;
        store.apply_record(&record);
        let _ = self.events.send(record.clone());
        Ok(record)
    }
}

pub fn router(state: Arc<AppState>) -> Router {
    Router::new()
        .merge(provider_http::routes())
        .route("/api/projects", get(projects::snapshot))
        .route("/api/projects/mutate", post(projects::mutate_http))
        .route("/api/hooks/:hook_id/:token", any(webhook_not_found))
        .route(
            "/api/orchestration/shell",
            get(environment_http::shell_snapshot),
        )
        .route(
            "/api/orchestration/threads/:thread_id",
            get(environment_http::thread_snapshot),
        )
        .route(
            "/api/orchestration/threads/:thread_id/transcript",
            get(environment_http::thread_snapshot),
        )
        .route(
            "/api/orchestration/threads/:thread_id/bounded",
            get(environment_http::thread_snapshot),
        )
        .route(
            "/api/orchestration/threads/:thread_id/history",
            get(environment_http::thread_snapshot),
        )
        .route_layer(axum::middleware::from_fn_with_state(
            state.clone(),
            require_session,
        ))
        .merge(asset_http::routes())
        .merge(mcp_oauth::routes())
        .merge(access_http::routes())
        .merge(host_rpc::routes())
        .route(
            "/.well-known/t3/environment",
            get(environment_http::descriptor),
        )
        .route("/ws", get(websocket))
        .fallback(any(contract_fallback))
        .layer(axum::middleware::from_fn(access_http::contextualize))
        .with_state(state)
}

async fn webhook_not_found() -> Response {
    (
        StatusCode::NOT_FOUND,
        Json(serde_json::json!({"error":"hook_not_found"})),
    )
        .into_response()
}

async fn contract_fallback(State(state): State<Arc<AppState>>, request: Request) -> Response {
    let method = request.method().as_str();
    let path = request.uri().path();
    if HTTP_CONTRACTS
        .iter()
        .any(|c| c.environment_auth && c.method == method && contracts::path_matches(c.path, path))
    {
        if let Err(reason) = access_http::authenticate(&state, request.headers()).await {
            return auth_error(request.headers(), reason);
        }
    }
    let (status, code, message) = if HTTP_CONTRACTS
        .iter()
        .any(|c| !c.relay && c.method == method && contracts::path_matches(c.path, path))
    {
        (
            StatusCode::NOT_IMPLEMENTED,
            "contract_not_implemented",
            "This contract endpoint is registered but its provider integration is unavailable.",
        )
    } else {
        return environment_http::no_assets();
    };
    let body = serde_json::json!({ "code": code, "message": message });
    Response::builder()
        .status(status)
        .header("content-type", "application/json")
        .header("access-control-allow-origin", "*")
        .body(body.to_string().into())
        .expect("valid response")
}
async fn execute_run(
    state: Arc<AppState>,
    run: Run,
    cwd: String,
    prompt: String,
    mut cancel_rx: oneshot::Receiver<()>,
) -> anyhow::Result<()> {
    let mut command = if state.provider_command.is_empty() {
        vec!["printf".into(), "%s\\n".into()]
    } else {
        state.provider_command.clone()
    };
    let executable = command.remove(0);
    let mut child_command = Command::new(executable);
    if state.settings.policy.summary().is_some() {
        for (k, _) in std::env::vars_os() {
            if k.to_string_lossy()
                .eq_ignore_ascii_case("T3CODE_CODEX_LAUNCH_ARGS")
            {
                child_command.env_remove(k);
            }
        }
    }
    let mut child = child_command
        .args(command)
        .arg(prompt)
        .current_dir(cwd)
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()?;
    let stdout = child.stdout.take().map(BufReader::new);
    let stderr = child.stderr.take().map(BufReader::new);
    let mut output_tasks = Vec::new();
    if let Some(mut reader) = stdout {
        let s = state.clone();
        let r = run.clone();
        output_tasks.push(tokio::spawn(async move {
            let mut line = String::new();
            while reader.read_line(&mut line).await.unwrap_or(0) > 0 {
                let text = std::mem::take(&mut line);
                let _ = s
                    .commit(Event::RunOutput {
                        run_id: r.id,
                        thread_id: r.thread_id.clone(),
                        stream: "stdout".into(),
                        text,
                    })
                    .await;
            }
        }));
    }
    if let Some(mut reader) = stderr {
        let s = state.clone();
        let r = run.clone();
        output_tasks.push(tokio::spawn(async move {
            let mut line = String::new();
            while reader.read_line(&mut line).await.unwrap_or(0) > 0 {
                let text = std::mem::take(&mut line);
                let _ = s
                    .commit(Event::RunOutput {
                        run_id: r.id,
                        thread_id: r.thread_id.clone(),
                        stream: "stderr".into(),
                        text,
                    })
                    .await;
            }
        }));
    }
    let (status, cancelled) = tokio::select! {
        result = child.wait() => (result?, false),
        _ = &mut cancel_rx => {
            child.kill().await?;
            (child.wait().await?, true)
        }
    };
    state.cancellations.lock().await.remove(&run.id);
    for task in output_tasks {
        let _ = task.await;
    }
    let ok = status.success();
    state
        .commit(Event::RunFinished {
            run_id: run.id,
            thread_id: run.thread_id,
            status: if cancelled {
                "cancelled"
            } else if ok {
                "completed"
            } else {
                "failed"
            }
            .into(),
            error: (!ok && !cancelled).then(|| format!("provider exited with {status}")),
        })
        .await?;
    Ok(())
}

pub fn trace_id(headers: &HeaderMap) -> String {
    headers
        .get("traceparent")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split('-').nth(1))
        .filter(|id| {
            id.len() == 32
                && id.bytes().all(|c| c.is_ascii_hexdigit())
                && id.bytes().any(|c| c != b'0')
        })
        .map(str::to_owned)
        .unwrap_or_else(|| Uuid::new_v4().simple().to_string())
}
pub fn auth_error(headers: &HeaderMap, reason: &str) -> Response {
    let trace_id = trace_id(headers);
    let mut body = serde_json::json!({"_tag":"EnvironmentAuthInvalidError","code":"auth_invalid","reason":if access_http::proof_failure(reason){"invalid_credential"}else{reason}});
    if access_http::proof_failure(reason) {
        body["dpopFailureReason"] = serde_json::json!(reason);
    }
    body["traceId"] = serde_json::json!(trace_id);
    let mut response = (
        StatusCode::UNAUTHORIZED,
        [
            ("vary", "Accept-Encoding"),
            ("access-control-allow-origin", "*"),
        ],
        Json(body),
    )
        .into_response();
    if access_http::uses_proof(headers) {
        response
            .headers_mut()
            .insert("www-authenticate", "DPoP".parse().unwrap());
    }
    response
}

async fn require_session(
    State(state): State<Arc<AppState>>,
    request: Request,
    next: axum::middleware::Next,
) -> Response {
    use auth_service::permissions::{http_scopes, project_scopes};
    let session = match access_http::principal(&state, request.headers()) {
        Ok(session) => session,
        Err(e) => return auth_error(request.headers(), e),
    };
    for scope in http_scopes(request.uri().path(), request.method().as_str()) {
        if !session.has(scope) {
            return access_http::scope_error(request.headers(), scope);
        }
    }
    if request.uri().path() == "/api/projects/mutate" {
        let (parts, body) = request.into_parts();
        let bytes = match axum::body::to_bytes(body, 2 * 1024 * 1024).await {
            Ok(bytes) => bytes,
            Err(_) => return StatusCode::PAYLOAD_TOO_LARGE.into_response(),
        };
        if let Ok(body) = serde_json::from_slice(&bytes) {
            if let Some(scope) = project_scopes(&body)
                .into_iter()
                .find(|scope| !session.has(scope))
            {
                return access_http::scope_error(&parts.headers, scope);
            }
        }
        return next
            .run(Request::from_parts(parts, axum::body::Body::from(bytes)))
            .await;
    }
    next.run(request).await
}

pub async fn authorize_upgrade(
    state: &AppState,
    headers: &HeaderMap,
    uri: &axum::http::Uri,
    audience: &str,
) -> Result<auth_service::Session, &'static str> {
    let ticket = url::form_urlencoded::parse(uri.query().unwrap_or("").as_bytes())
        .filter(|(key, _)| key == "wsTicket")
        .last()
        .map(|(_, value)| value);
    if let Some(ticket) = ticket.filter(|v| !v.trim().is_empty()) {
        access_http::authenticate_ticket(state, headers, uri, &ticket, audience)
    } else {
        access_http::principal(state, headers)
    }
}

async fn websocket(State(state): State<Arc<AppState>>, request: Request) -> Response {
    let query = url::form_urlencoded::parse(request.uri().query().unwrap_or("").as_bytes());
    let references = query
        .clone()
        .find(|(k, _)| k == "clientCapabilities")
        .is_some_and(|(_, v)| v.split(',').any(|c| c == "settingsSecretReferences"));
    let compatible = query
        .clone()
        .find(|(key, _)| key == "orchestrationProtocol")
        .is_some_and(|(_, value)| value == "2");
    if !compatible {
        let body = serde_json::json!({
            "code": "orchestration_protocol_incompatible",
            "message": "Update this client to one that supports orchestration protocol 2.",
            "orchestrationProtocolVersion": 2
        });
        return Response::builder()
            .status(StatusCode::UPGRADE_REQUIRED)
            .header("content-type", "application/json")
            .header("vary", "Accept-Encoding")
            .header("access-control-allow-origin", "*")
            .body(body.to_string().into())
            .expect("valid response");
    }
    let auth = authorize_upgrade(&state, request.headers(), request.uri(), "rpc").await;
    let principal = match auth {
        Ok(s) => s,
        Err(reason) => return auth_error(request.headers(), reason),
    };
    match WebSocketUpgrade::from_request(request, &state).await {
        Ok(upgrade) => upgrade
            .on_upgrade(move |socket| rpc::session(state, socket, principal, references))
            .into_response(),
        Err(rejection) => rejection.into_response(),
    }
}
pub async fn run_server(state: Arc<AppState>, listen: std::net::SocketAddr) -> anyhow::Result<()> {
    let policy = &state.settings.policy;
    let client = if !listen.ip().is_loopback() {
        "remote"
    } else if state.auth.desktop() {
        "desktop"
    } else {
        "local-browser"
    };
    if !policy.allows_client(client) {
        let action = match client {
            "remote" => "remote clients",
            "desktop" => "the desktop app",
            _ => "browsers on this machine",
        };
        let refusal = policy.denial(action);
        if client == "remote" {
            anyhow::bail!(
                "{}, so the server cannot listen on {}.",
                refusal.trim_end_matches('.'),
                listen.ip()
            );
        }
        anyhow::bail!(refusal);
    }
    if !policy.allows_client("remote") && state.runtime_config.tailscale_serve == Some(true) {
        anyhow::bail!(
            "{}, so Tailscale Serve cannot be turned on.",
            policy.denial("remote clients").trim_end_matches('.')
        );
    }
    policy.close_tailscale_routes(listen).await?;
    let app = router(state);
    let listener = tokio::net::TcpListener::bind(listen).await?;
    info!(%listen, "t3code server listening");
    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<std::net::SocketAddr>(),
    )
    .await?;
    Ok(())
}
pub fn default_data_dir() -> PathBuf {
    std::env::var_os("T3CODE_DATA_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| config::default_base_dir().join("userdata"))
}
pub fn default_provider_command() -> Vec<String> {
    std::env::var("T3_PROVIDER_COMMAND")
        .ok()
        .map(|s| s.split_whitespace().map(str::to_owned).collect())
        .unwrap_or_else(|| vec!["printf".into(), "%s\\n".into()])
}
pub fn ensure_path(path: &str) -> anyhow::Result<()> {
    let p = FsPath::new(path);
    if !p.exists() {
        anyhow::bail!("working directory does not exist: {path}");
    }
    Ok(())
}
