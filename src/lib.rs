use axum::extract::ws::{Message as WsMessage, WebSocket};
use axum::{
    extract::{FromRequest, Path, Request, State, WebSocketUpgrade},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{any, get, post},
    Json, Router,
};
use futures_util::{SinkExt, StreamExt};
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
    sync::{broadcast, mpsc, oneshot, Mutex, RwLock},
};
use tracing::{error, info, warn};
use uuid::Uuid;

mod contracts;

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
    pub id: Uuid,
    pub name: String,
    pub path: String,
    pub created_at: u64,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ConversationMessage {
    pub id: Uuid,
    pub role: String,
    pub content: String,
    pub created_at: u64,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Run {
    pub id: Uuid,
    pub thread_id: Uuid,
    pub provider: String,
    pub status: String,
    pub started_at: u64,
    pub finished_at: Option<u64>,
    pub error: Option<String>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Thread {
    pub id: Uuid,
    pub project_id: Uuid,
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
    ThreadCreated(Thread),
    MessageAdded {
        thread_id: Uuid,
        message: ConversationMessage,
    },
    RunStarted(Run),
    RunOutput {
        run_id: Uuid,
        thread_id: Uuid,
        stream: String,
        text: String,
    },
    RunFinished {
        run_id: Uuid,
        thread_id: Uuid,
        status: String,
        error: Option<String>,
    },
    CommandDispatched {
        command_id: String,
        command_type: String,
        thread_id: Option<Uuid>,
        payload: serde_json::Value,
    },
    ThreadPatched {
        thread_id: Uuid,
        patch: serde_json::Value,
    },
    ThreadDeleted {
        thread_id: Uuid,
    },
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ServerEvent {
    pub id: u64,
    pub at: u64,
    pub event: Event,
}

#[derive(Default)]
pub struct Store {
    pub projects: HashMap<Uuid, Project>,
    pub threads: HashMap<Uuid, Thread>,
}
impl Store {
    fn apply(&mut self, event: &Event) {
        match event {
            Event::ProjectCreated(p) => {
                self.projects.insert(p.id, p.clone());
            }
            Event::ThreadCreated(t) => {
                self.threads.insert(t.id, t.clone());
            }
            Event::MessageAdded { thread_id, message } => {
                if let Some(t) = self.threads.get_mut(thread_id) {
                    t.messages.push(message.clone());
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
            }
        }
    }
}

pub struct AppState {
    pub store: RwLock<Store>,
    pub events: broadcast::Sender<ServerEvent>,
    pub log: Mutex<tokio::fs::File>,
    pub provider_command: Vec<String>,
    pub cancellations: Mutex<HashMap<Uuid, oneshot::Sender<()>>>,
    next_event_id: Mutex<u64>,
}
impl AppState {
    pub async fn new(
        data_dir: impl Into<PathBuf>,
        provider_command: Vec<String>,
    ) -> anyhow::Result<Arc<Self>> {
        let dir = data_dir.into();
        tokio::fs::create_dir_all(&dir).await?;
        let log_path = dir.join("events.jsonl");
        let mut store = Store::default();
        let mut next_event_id = 0;
        if let Ok(contents) = tokio::fs::read_to_string(&log_path).await {
            for line in contents.lines() {
                if let Ok(record) = serde_json::from_str::<ServerEvent>(line) {
                    next_event_id = next_event_id.max(record.id);
                    store.apply(&record.event);
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
            store: RwLock::new(store),
            events,
            log: Mutex::new(log),
            provider_command,
            cancellations: Mutex::new(HashMap::new()),
            next_event_id: Mutex::new(next_event_id),
        }))
    }
    async fn commit(&self, event: Event) -> anyhow::Result<ServerEvent> {
        let mut store = self.store.write().await;
        store.apply(&event);
        let mut next_event_id = self.next_event_id.lock().await;
        *next_event_id += 1;
        let record = ServerEvent {
            id: *next_event_id,
            at: now(),
            event,
        };
        let mut log = self.log.lock().await;
        use tokio::io::AsyncWriteExt;
        log.write_all(serde_json::to_string(&record)?.as_bytes())
            .await?;
        log.write_all(b"\n").await?;
        log.flush().await?;
        let _ = self.events.send(record.clone());
        Ok(record)
    }
}

#[derive(Deserialize)]
pub struct CreateProject {
    pub name: String,
    pub path: String,
}
#[derive(Deserialize)]
pub struct CreateThread {
    pub project_id: Uuid,
    pub title: Option<String>,
    pub cwd: Option<String>,
}
#[derive(Deserialize)]
pub struct SendMessage {
    pub content: String,
    pub provider: Option<String>,
}
#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ClientCommand {
    Subscribe,
    CreateThread(CreateThread),
    SendMessage {
        thread_id: Uuid,
        content: String,
        provider: Option<String>,
    },
    CancelRun {
        run_id: Uuid,
    },
}

pub fn router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/v1/projects", get(list_projects).post(create_project))
        .route("/api/projects", get(api_projects))
        .route("/api/orchestration/shell", get(api_shell_snapshot))
        .route(
            "/api/orchestration/threads/:thread_id",
            get(api_thread_snapshot),
        )
        .route(
            "/api/orchestration/threads/:thread_id/transcript",
            get(api_thread_transcript),
        )
        .route(
            "/api/orchestration/threads/:thread_id/bounded",
            get(api_thread_snapshot),
        )
        .route(
            "/api/orchestration/threads/:thread_id/history",
            get(api_thread_transcript),
        )
        .route("/api/t3-connect/health", post(relay_health))
        .route("/api/auth/session", get(auth_session))
        .route("/.well-known/t3/environment", get(environment_descriptor))
        .route("/v1/environments", get(relay_environments))
        .route("/v1/client/devices", get(relay_devices))
        .route("/v2/client/devices", get(relay_devices))
        .route("/v1/client/environment-links", post(relay_link_environment))
        .route(
            "/v1/client/environment-links/:environment_id",
            axum::routing::delete(relay_unlink_environment),
        )
        .route(
            "/v1/environments/:environment_id/status",
            post(relay_environment_status),
        )
        .route(
            "/v1/environments/:environment_id/connect",
            post(relay_environment_connect),
        )
        .route(
            "/v1/environments/:environment_id/hooks/wake",
            post(relay_wake_hooks),
        )
        .route("/v1/projects/:project_id/threads", post(create_thread))
        .route("/v1/threads/:thread_id", get(get_thread))
        .route("/v1/threads/:thread_id/messages", post(send_message))
        .route("/v1/runs/:run_id/cancel", post(cancel_run))
        .route("/ws", get(websocket))
        .fallback(any(contract_fallback))
        .with_state(state)
}

async fn contract_fallback(request: Request) -> Response {
    let method = request.method().as_str();
    let path = request.uri().path();
    let (status, code, message) = if contracts::http_known(method, path) {
        (
            StatusCode::NOT_IMPLEMENTED,
            "contract_not_implemented",
            "This contract endpoint is registered but its provider integration is unavailable.",
        )
    } else {
        (StatusCode::NOT_FOUND, "not_found", "Route not found.")
    };
    let body = serde_json::json!({ "code": code, "message": message });
    Response::builder()
        .status(status)
        .header("content-type", "application/json")
        .header("access-control-allow-origin", "*")
        .body(body.to_string().into())
        .expect("valid response")
}
async fn health() -> impl IntoResponse {
    Json(serde_json::json!({"ok": true, "protocol_version": PROTOCOL_VERSION}))
}
async fn environment_descriptor() -> impl IntoResponse {
    Json(
        serde_json::json!({"environmentId":"local","label":"t3code-rust","platform":{"os":std::env::consts::OS,"arch":std::env::consts::ARCH}}),
    )
}
async fn auth_session() -> impl IntoResponse {
    Json(serde_json::json!({"authenticated":false,"scopes":[],"auth":{}}))
}
async fn relay_health() -> impl IntoResponse {
    Json(serde_json::json!({"ok":true}))
}
async fn relay_environments() -> impl IntoResponse {
    Json(serde_json::json!({"environments":[]}))
}
async fn relay_devices() -> impl IntoResponse {
    Json(serde_json::json!({"devices":[]}))
}
async fn relay_link_environment(Json(input): Json<serde_json::Value>) -> impl IntoResponse {
    Json(
        serde_json::json!({"environmentId":input.get("environmentId").cloned().unwrap_or(serde_json::Value::Null),"status":"linked"}),
    )
}
async fn relay_unlink_environment(Path(_environment_id): Path<String>) -> StatusCode {
    StatusCode::NO_CONTENT
}
async fn relay_environment_status(Path(environment_id): Path<String>) -> impl IntoResponse {
    Json(serde_json::json!({"environmentId":environment_id,"status":"offline"}))
}
async fn relay_environment_connect(Path(environment_id): Path<String>) -> impl IntoResponse {
    Json(serde_json::json!({"environmentId":environment_id,"status":"offline"}))
}
async fn relay_wake_hooks(Path(_environment_id): Path<String>) -> StatusCode {
    StatusCode::NO_CONTENT
}
async fn list_projects(State(state): State<Arc<AppState>>) -> Json<Vec<Project>> {
    Json(
        state
            .store
            .read()
            .await
            .projects
            .values()
            .cloned()
            .collect(),
    )
}
async fn api_projects(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let store = state.store.read().await;
    Json(serde_json::json!({"projects": store.projects.values().collect::<Vec<_>>()}))
}
async fn api_shell_snapshot(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let store = state.store.read().await;
    Json(
        serde_json::json!({"schemaVersion":1,"snapshotSequence":0,"projects":store.projects.values().collect::<Vec<_>>(),"threads":store.threads.values().collect::<Vec<_>>()}),
    )
}
async fn api_thread_snapshot(
    State(state): State<Arc<AppState>>,
    Path(thread_id): Path<Uuid>,
) -> impl IntoResponse {
    match state.store.read().await.threads.get(&thread_id).cloned() {
        Some(thread) => (StatusCode::OK, Json(serde_json::json!(thread))),
        None => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({"code":"not_found","message":"Thread not found."})),
        ),
    }
}
async fn api_thread_transcript(
    State(state): State<Arc<AppState>>,
    Path(thread_id): Path<Uuid>,
) -> impl IntoResponse {
    match state.store.read().await.threads.get(&thread_id).cloned() {
        Some(thread) => (
            StatusCode::OK,
            Json(
                serde_json::json!({"threadId":thread.id,"title":thread.title,"items":thread.messages}),
            ),
        ),
        None => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({"code":"not_found","message":"Thread not found."})),
        ),
    }
}
async fn create_project(
    State(state): State<Arc<AppState>>,
    Json(input): Json<CreateProject>,
) -> impl IntoResponse {
    let project = Project {
        id: Uuid::new_v4(),
        name: input.name,
        path: input.path,
        created_at: now(),
    };
    match state.commit(Event::ProjectCreated(project.clone())).await {
        Ok(_) => (StatusCode::CREATED, Json(serde_json::json!(project))),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error":e.to_string()})),
        ),
    }
}
async fn create_thread(
    State(state): State<Arc<AppState>>,
    Path(project_id): Path<Uuid>,
    Json(input): Json<CreateThread>,
) -> impl IntoResponse {
    if project_id != input.project_id {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({ "error": "project id in path and body must match" })),
        );
    }
    let project = state.store.read().await.projects.get(&project_id).cloned();
    let Some(project) = project else {
        return (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({"error":"project not found"})),
        );
    };
    let thread = Thread {
        id: Uuid::new_v4(),
        project_id: project.id,
        title: input.title.unwrap_or_else(|| "New thread".into()),
        cwd: input.cwd.unwrap_or(project.path),
        status: "idle".into(),
        messages: vec![],
        runs: vec![],
        created_at: now(),
        archived: false,
        settled: false,
        snoozed_until: None,
        pinned: false,
        unread: false,
        tags: vec![],
    };
    match state.commit(Event::ThreadCreated(thread.clone())).await {
        Ok(_) => (StatusCode::CREATED, Json(serde_json::json!(thread))),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error":e.to_string()})),
        ),
    }
}
async fn get_thread(
    State(state): State<Arc<AppState>>,
    Path(thread_id): Path<Uuid>,
) -> impl IntoResponse {
    match state.store.read().await.threads.get(&thread_id).cloned() {
        Some(t) => (StatusCode::OK, Json(serde_json::json!(t))),
        None => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({"error":"thread not found"})),
        ),
    }
}
async fn send_message(
    State(state): State<Arc<AppState>>,
    Path(thread_id): Path<Uuid>,
    Json(input): Json<SendMessage>,
) -> impl IntoResponse {
    match start_run(
        state,
        thread_id,
        input.content,
        input.provider.unwrap_or_else(|| "default".into()),
    )
    .await
    {
        Ok(run) => (StatusCode::ACCEPTED, Json(serde_json::json!(run))),
        Err((s, e)) => (s, Json(serde_json::json!({"error":e}))),
    }
}
async fn cancel_run(
    State(state): State<Arc<AppState>>,
    Path(run_id): Path<Uuid>,
) -> impl IntoResponse {
    let mut cancellations = state.cancellations.lock().await;
    if let Some(cancel) = cancellations.remove(&run_id) {
        if cancel.send(()).is_ok() {
            return StatusCode::NO_CONTENT;
        }
        warn!("provider cancellation was already completed");
    }
    StatusCode::NOT_FOUND
}

async fn start_run(
    state: Arc<AppState>,
    thread_id: Uuid,
    content: String,
    provider: String,
) -> Result<Run, (StatusCode, String)> {
    let thread = state
        .store
        .read()
        .await
        .threads
        .get(&thread_id)
        .cloned()
        .ok_or((StatusCode::NOT_FOUND, "thread not found".into()))?;
    let message = ConversationMessage {
        id: Uuid::new_v4(),
        role: "user".into(),
        content: content.clone(),
        created_at: now(),
    };
    state
        .commit(Event::MessageAdded { thread_id, message })
        .await
        .map_err(internal)?;
    let run = Run {
        id: Uuid::new_v4(),
        thread_id,
        provider,
        status: "running".into(),
        started_at: now(),
        finished_at: None,
        error: None,
    };
    state
        .commit(Event::RunStarted(run.clone()))
        .await
        .map_err(internal)?;
    let state_for_task = state.clone();
    let run_for_task = run.clone();
    tokio::spawn(async move {
        if let Err(e) = execute_run(state_for_task, run_for_task, thread.cwd, content).await {
            error!(%e, "run failed");
        }
    });
    Ok(run)
}

async fn dispatch_orchestration_command(
    state: Arc<AppState>,
    command: serde_json::Value,
) -> anyhow::Result<u64> {
    let command_type = command
        .get("type")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default();
    let thread_id = command
        .get("threadId")
        .and_then(serde_json::Value::as_str)
        .and_then(|value| Uuid::parse_str(value).ok());
    match command_type {
        "thread.create" => {
            let project_id = command
                .get("projectId")
                .and_then(serde_json::Value::as_str)
                .and_then(|value| Uuid::parse_str(value).ok())
                .ok_or_else(|| anyhow::anyhow!("thread.create requires projectId"))?;
            let project = state
                .store
                .read()
                .await
                .projects
                .get(&project_id)
                .cloned()
                .ok_or_else(|| anyhow::anyhow!("project not found"))?;
            let id = thread_id.unwrap_or_else(Uuid::new_v4);
            let thread = Thread {
                id,
                project_id,
                title: command
                    .get("title")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("New thread")
                    .to_owned(),
                cwd: command
                    .get("worktreePath")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or(&project.path)
                    .to_owned(),
                status: "idle".into(),
                messages: vec![],
                runs: vec![],
                created_at: now(),
                archived: false,
                settled: false,
                snoozed_until: None,
                pinned: false,
                unread: false,
                tags: vec![],
            };
            return Ok(state.commit(Event::ThreadCreated(thread)).await?.id);
        }
        "message.dispatch" => {
            let thread_id =
                thread_id.ok_or_else(|| anyhow::anyhow!("message.dispatch requires threadId"))?;
            let text = command
                .get("text")
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default()
                .to_owned();
            let run = start_run(state.clone(), thread_id, text, "default".into())
                .await
                .map_err(|(_, error)| anyhow::anyhow!(error))?;
            return Ok(state
                .commit(Event::CommandDispatched {
                    command_id: command_id(&command),
                    command_type: command_type.into(),
                    thread_id: Some(thread_id),
                    payload: command,
                })
                .await?
                .id
                .max(run.started_at));
        }
        "thread.stop" | "run.interrupt" | "subagent.stop" | "queued-run.cancel" => {
            if let Some(thread_id) = thread_id {
                let run_ids = state
                    .store
                    .read()
                    .await
                    .threads
                    .get(&thread_id)
                    .map(|thread| {
                        thread
                            .runs
                            .iter()
                            .filter(|run| run.status == "running")
                            .map(|run| run.id)
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default();
                let mut cancellations = state.cancellations.lock().await;
                for run_id in run_ids {
                    if let Some(cancel) = cancellations.remove(&run_id) {
                        let _ = cancel.send(());
                    }
                }
                return Ok(state
                    .commit(Event::ThreadPatched {
                        thread_id,
                        patch: serde_json::json!({"status":"cancelled"}),
                    })
                    .await?
                    .id);
            }
        }
        "thread.archive"
        | "thread.unarchive"
        | "thread.settle"
        | "thread.unsettle"
        | "thread.pin"
        | "thread.unpin"
        | "thread.mark-unread"
        | "thread.visit"
        | "thread.snooze"
        | "thread.unsnooze"
        | "thread.metadata.update"
        | "thread.runtime-mode.set"
        | "thread.interaction-mode.set"
        | "thread.model-selection.set"
        | "thread.tag.set" => {
            if let Some(thread_id) = thread_id {
                let mut patch = serde_json::Map::new();
                match command_type {
                    "thread.archive" => {
                        patch.insert("archived".into(), true.into());
                    }
                    "thread.unarchive" => {
                        patch.insert("archived".into(), false.into());
                    }
                    "thread.settle" => {
                        patch.insert("settled".into(), true.into());
                    }
                    "thread.unsettle" => {
                        patch.insert("settled".into(), false.into());
                    }
                    "thread.pin" => {
                        patch.insert("pinned".into(), true.into());
                    }
                    "thread.unpin" => {
                        patch.insert("pinned".into(), false.into());
                    }
                    "thread.mark-unread" => {
                        patch.insert("unread".into(), true.into());
                    }
                    "thread.visit" => {
                        patch.insert("unread".into(), false.into());
                    }
                    "thread.snooze" => {
                        patch.insert(
                            "snoozed_until".into(),
                            command
                                .get("snoozedUntil")
                                .cloned()
                                .unwrap_or(serde_json::Value::Null),
                        );
                    }
                    "thread.unsnooze" => {
                        patch.insert("snoozed_until".into(), serde_json::Value::Null);
                    }
                    "thread.metadata.update" => {
                        if let Some(title) = command.get("title") {
                            patch.insert("title".into(), title.clone());
                        }
                    }
                    "thread.tag.set" => {
                        let tag = command.get("tag").and_then(serde_json::Value::as_str);
                        let tagged = command
                            .get("tagged")
                            .and_then(serde_json::Value::as_bool)
                            .unwrap_or(false);
                        let mut tags = state
                            .store
                            .read()
                            .await
                            .threads
                            .get(&thread_id)
                            .map(|thread| thread.tags.clone())
                            .unwrap_or_default();
                        if let Some(tag) = tag {
                            if tagged && !tags.iter().any(|value| value == tag) {
                                tags.push(tag.into());
                            }
                            if !tagged {
                                tags.retain(|value| value != tag);
                            }
                        }
                        patch.insert("tags".into(), serde_json::json!(tags));
                    }
                    _ => {}
                }
                return Ok(state
                    .commit(Event::ThreadPatched {
                        thread_id,
                        patch: patch.into(),
                    })
                    .await?
                    .id);
            }
        }
        "thread.delete" => {
            if let Some(thread_id) = thread_id {
                return Ok(state.commit(Event::ThreadDeleted { thread_id }).await?.id);
            }
        }
        _ => {}
    }
    Ok(state
        .commit(Event::CommandDispatched {
            command_id: command_id(&command),
            command_type: command_type.into(),
            thread_id,
            payload: command,
        })
        .await?
        .id)
}

fn command_id(command: &serde_json::Value) -> String {
    command
        .get("commandId")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default()
        .to_owned()
}
fn internal(e: anyhow::Error) -> (StatusCode, String) {
    (StatusCode::INTERNAL_SERVER_ERROR, e.to_string())
}
async fn execute_run(
    state: Arc<AppState>,
    run: Run,
    cwd: String,
    prompt: String,
) -> anyhow::Result<()> {
    let mut command = if state.provider_command.is_empty() {
        vec!["printf".into(), "%s\\n".into()]
    } else {
        state.provider_command.clone()
    };
    let executable = command.remove(0);
    let mut child = Command::new(executable)
        .args(command)
        .arg(prompt)
        .current_dir(cwd)
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()?;
    let stdout = child.stdout.take().map(BufReader::new);
    let stderr = child.stderr.take().map(BufReader::new);
    let (cancel_tx, mut cancel_rx) = oneshot::channel();
    state.cancellations.lock().await.insert(run.id, cancel_tx);
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
                        thread_id: r.thread_id,
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
                        thread_id: r.thread_id,
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

async fn websocket(State(state): State<Arc<AppState>>, request: Request) -> Response {
    let compatible = request
        .uri()
        .query()
        .and_then(|query| {
            query.split('&').find_map(|part| {
                let (key, value) = part.split_once('=')?;
                (key == "orchestrationProtocol").then_some(value == "2")
            })
        })
        .unwrap_or(false);
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
    if request.headers().get("authorization").is_none() && request.headers().get("cookie").is_none()
    {
        let body = serde_json::json!({
            "_tag": "EnvironmentAuthInvalidError",
            "code": "auth_invalid",
            "reason": "missing_credential",
            "traceId": format!("{:032x}", Uuid::new_v4().as_u128()),
        });
        return Response::builder()
            .status(StatusCode::UNAUTHORIZED)
            .header("content-type", "application/json")
            .header("vary", "Accept-Encoding")
            .header("access-control-allow-origin", "*")
            .body(body.to_string().into())
            .expect("valid response");
    }
    match WebSocketUpgrade::from_request(request, &state).await {
        Ok(upgrade) => upgrade
            .on_upgrade(move |socket| websocket_session(state, socket))
            .into_response(),
        Err(rejection) => rejection.into_response(),
    }
}
async fn websocket_session(state: Arc<AppState>, socket: WebSocket) {
    let (mut sender, mut receiver) = socket.split();
    let mut events = state.events.subscribe();
    let (outbound, mut outbound_rx) = mpsc::unbounded_channel::<String>();
    let outbound_for_events = outbound.clone();
    let writer = tokio::spawn(async move {
        loop {
            tokio::select! {
                event = events.recv() => {
                    let Ok(event) = event else { break };
                    let Ok(text) = serde_json::to_string(&serde_json::json!({"type":"event","data":event})) else { break; };
                    if sender.send(WsMessage::Text(text)).await.is_err() { break; }
                }
                text = outbound_rx.recv() => {
                    let Some(text) = text else { break; };
                    if sender.send(WsMessage::Text(text)).await.is_err() { break; }
                }
            }
        }
    });
    while let Some(Ok(message)) = receiver.next().await {
        let WsMessage::Text(text) = message else {
            continue;
        };
        if let Ok(rpc) = serde_json::from_str::<serde_json::Value>(&text) {
            if rpc.get("jsonrpc").and_then(serde_json::Value::as_str) == Some("2.0") {
                handle_rpc_message(&state, &outbound_for_events, rpc).await;
                continue;
            }
        }
        match serde_json::from_str::<ClientCommand>(&text) {
            Ok(ClientCommand::Subscribe) => {
                let snapshot = state.store.read().await;
                let payload = serde_json::json!({"protocol_version":PROTOCOL_VERSION,"projects":snapshot.projects.values().collect::<Vec<_>>(),"threads":snapshot.threads.values().collect::<Vec<_>>()});
                let _ = state.events.send(ServerEvent {
                    id: 0,
                    at: now(),
                    event: Event::RunOutput {
                        run_id: Uuid::nil(),
                        thread_id: Uuid::nil(),
                        stream: "snapshot".into(),
                        text: payload.to_string(),
                    },
                });
            }
            Ok(ClientCommand::CreateThread(input)) => {
                let _ =
                    create_thread(State(state.clone()), Path(input.project_id), Json(input)).await;
            }
            Ok(ClientCommand::SendMessage {
                thread_id,
                content,
                provider,
            }) => {
                let _ = start_run(
                    state.clone(),
                    thread_id,
                    content,
                    provider.unwrap_or_else(|| "default".into()),
                )
                .await;
            }
            Ok(ClientCommand::CancelRun { run_id }) => {
                let _ = cancel_run(State(state.clone()), Path(run_id)).await;
            }
            Err(e) => {
                let _ = state.events.send(ServerEvent {
                    id: 0,
                    at: now(),
                    event: Event::RunOutput {
                        run_id: Uuid::nil(),
                        thread_id: Uuid::nil(),
                        stream: "error".into(),
                        text: e.to_string(),
                    },
                });
            }
        }
    }
    writer.abort();
}

async fn handle_rpc_message(
    state: &Arc<AppState>,
    outbound: &mpsc::UnboundedSender<String>,
    message: serde_json::Value,
) {
    let Some(id) = message.get("id").cloned() else {
        return;
    };
    let method = message
        .get("method")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default();
    if method == "@effect/rpc/Ping" {
        let _ = outbound
            .send(serde_json::json!({"jsonrpc":"2.0","method":"@effect/rpc/Pong"}).to_string());
        return;
    }
    match method {
        "orchestration.dispatchCommand" => {
            let payload = message
                .get("params")
                .cloned()
                .unwrap_or(serde_json::Value::Null);
            match dispatch_orchestration_command(state.clone(), payload).await {
                Ok(sequence) => {
                    let _ = outbound.send(
                        serde_json::json!({"jsonrpc":"2.0","id":id,"result":{"sequence":sequence}})
                            .to_string(),
                    );
                }
                Err(error) => send_rpc_error(outbound, id, "dispatch_failed", error.to_string()),
            }
        }
        "orchestration.getThreadTitles" => {
            let ids = message
                .get("params")
                .and_then(|params| params.get("threadIds"))
                .and_then(serde_json::Value::as_array)
                .cloned()
                .unwrap_or_default();
            let store = state.store.read().await;
            let titles = ids
                .iter()
                .filter_map(serde_json::Value::as_str)
                .filter_map(|id| Uuid::parse_str(id).ok())
                .filter_map(|id| {
                    store
                        .threads
                        .get(&id)
                        .map(|thread| (id.to_string(), thread.title.clone()))
                })
                .collect::<HashMap<_, _>>();
            let _ = outbound
                .send(serde_json::json!({"jsonrpc":"2.0","id":id,"result":titles}).to_string());
        }
        "orchestration.getThreadFamily" => {
            let requested = message
                .get("params")
                .and_then(|params| params.get("threadId"))
                .and_then(serde_json::Value::as_str)
                .and_then(|id| Uuid::parse_str(id).ok());
            let store = state.store.read().await;
            let threads = requested
                .and_then(|thread_id| store.threads.get(&thread_id).map(|thread| vec![thread]))
                .unwrap_or_default();
            let _ = outbound.send(serde_json::json!({"jsonrpc":"2.0","id":id,"result":{"snapshotSequence":0,"threads":threads}}).to_string());
        }
        "orchestration.getWorktreeThreadIds" => {
            let worktree = message
                .get("params")
                .and_then(|params| params.get("worktreePath"))
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default();
            let store = state.store.read().await;
            let thread_ids = store
                .threads
                .values()
                .filter(|thread| thread.cwd == worktree)
                .map(|thread| thread.id.to_string())
                .collect::<Vec<_>>();
            let _ = outbound.send(
                serde_json::json!({"jsonrpc":"2.0","id":id,"result":{"threadIds":thread_ids}})
                    .to_string(),
            );
        }
        "orchestration.getArchivedShellSnapshot" => {
            let _ = outbound.send(serde_json::json!({"jsonrpc":"2.0","id":id,"result":{"schemaVersion":1,"snapshotSequence":0,"projects":[],"threads":[]}}).to_string());
        }
        "orchestration.getThreadLinkMetadata" => {
            let ids = message
                .get("params")
                .and_then(|params| params.get("threadIds"))
                .and_then(serde_json::Value::as_array)
                .cloned()
                .unwrap_or_default();
            let store = state.store.read().await;
            let metadata = ids
                .iter()
                .filter_map(serde_json::Value::as_str)
                .filter_map(|value| Uuid::parse_str(value).ok())
                .filter_map(|thread_id| {
                    store.threads.get(&thread_id).map(|thread| {
                        (
                            thread_id.to_string(),
                            serde_json::json!({"title":thread.title,"projectId":thread.project_id}),
                        )
                    })
                })
                .collect::<HashMap<_, _>>();
            let _ = outbound
                .send(serde_json::json!({"jsonrpc":"2.0","id":id,"result":metadata}).to_string());
        }
        "orchestration.searchThreads" => {
            let query = message
                .get("params")
                .and_then(|params| params.get("query"))
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default()
                .to_ascii_lowercase();
            let store = state.store.read().await;
            let threads = store
                .threads
                .values()
                .filter(|thread| thread.title.to_ascii_lowercase().contains(&query))
                .cloned()
                .collect::<Vec<_>>();
            let _ = outbound.send(serde_json::json!({"jsonrpc":"2.0","id":id,"result":{"snapshotSequence":0,"threads":threads,"complete":true}}).to_string());
        }
        "orchestration.searchThread" => {
            let _ = outbound.send(serde_json::json!({"jsonrpc":"2.0","id":id,"result":{"complete":true,"snapshotSequence":0,"totalMatches":0,"activeIndex":0,"match":null}}).to_string());
        }
        "orchestration.getOlderSettledThreads" => {
            let _ = outbound.send(serde_json::json!({"jsonrpc":"2.0","id":id,"result":{"snapshotSequence":0,"threads":[],"counts":[]}}).to_string());
        }
        "orchestration.getThreadProjection"
        | "orchestration.getFullThreadDiff"
        | "orchestration.getTurnDiff" => {
            let _ = outbound
                .send(serde_json::json!({"jsonrpc":"2.0","id":id,"result":null}).to_string());
        }
        "orchestration.stopWorkflow"
        | "orchestration.forceStopThread"
        | "orchestration.getWorkflowScript"
        | "orchestration.getNativeChildWork"
        | "orchestration.getTurnItem" => {
            let _ = outbound
                .send(serde_json::json!({"jsonrpc":"2.0","id":id,"result":null}).to_string());
        }
        "server.probe" => {
            let _ =
                outbound.send(serde_json::json!({"jsonrpc":"2.0","id":id,"result":{}}).to_string());
        }
        "orchestration.subscribeShell" => {
            let store = state.store.read().await;
            let snapshot = serde_json::json!({"kind":"snapshot","snapshot":{"schemaVersion":1,"snapshotSequence":0,"projects":store.projects.values().collect::<Vec<_>>(),"threads":store.threads.values().collect::<Vec<_>>(),"archivedThreads":[]}});
            let _ = outbound.send(
                serde_json::json!({"jsonrpc":"2.0","chunk":true,"id":id,"result":[snapshot]})
                    .to_string(),
            );
        }
        "orchestration.subscribeThread" => {
            let thread_id = message
                .get("params")
                .and_then(|params| params.get("threadId"))
                .and_then(serde_json::Value::as_str)
                .and_then(|id| Uuid::parse_str(id).ok());
            let thread = match thread_id {
                Some(thread_id) => state.store.read().await.threads.get(&thread_id).cloned(),
                None => None,
            };
            let items = thread
                .map(|thread| vec![serde_json::json!({"kind":"snapshot","snapshotSequence":0,"projection":thread}), serde_json::json!({"kind":"synchronized"})])
                .unwrap_or_default();
            let _ = outbound.send(
                serde_json::json!({"jsonrpc":"2.0","chunk":true,"id":id,"result":items})
                    .to_string(),
            );
        }
        "orchestration.subscribeCommandOutput" => {
            let frame =
                serde_json::json!({"kind":"replace","text":"","truncated":false,"running":false});
            let _ = outbound.send(
                serde_json::json!({"jsonrpc":"2.0","chunk":true,"id":id,"result":[frame]})
                    .to_string(),
            );
        }
        "orchestration.subscribePromptSuggestion" => {
            let _ = outbound.send(
                serde_json::json!({"jsonrpc":"2.0","chunk":true,"id":id,"result":[null]})
                    .to_string(),
            );
        }
        _ => {
            let (code, message) = if contracts::rpc_known(method) {
                (
                    "contract_not_implemented",
                    format!("RPC contract is registered but unavailable: {method}"),
                )
            } else {
                ("method_not_found", format!("Unknown RPC method: {method}"))
            };
            let _ = outbound.send(serde_json::json!({"jsonrpc":"2.0","id":id,"error":{"_tag":"Cause","code":0,"message":message,"data":[{"code":code}]}}).to_string());
        }
    }
}

fn send_rpc_error(
    outbound: &mpsc::UnboundedSender<String>,
    id: serde_json::Value,
    code: &str,
    message: String,
) {
    let _ = outbound.send(
        serde_json::json!({"jsonrpc":"2.0","id":id,"error":{"_tag":"Cause","code":0,"message":message,"data":[{"code":code}]}}).to_string(),
    );
}
pub async fn run_server(state: Arc<AppState>, listen: std::net::SocketAddr) -> anyhow::Result<()> {
    let app = router(state);
    let listener = tokio::net::TcpListener::bind(listen).await?;
    info!(%listen, "t3code server listening");
    axum::serve(listener, app).await?;
    Ok(())
}
pub fn default_data_dir() -> PathBuf {
    std::env::var_os("T3CODE_DATA_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(".t3"))
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
