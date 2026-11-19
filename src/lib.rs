use axum::extract::ws::{Message as WsMessage, WebSocket};
use axum::{
    extract::{FromRequest, Path, Request, State, WebSocketUpgrade},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
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
        .route("/v1/projects/:project_id/threads", post(create_thread))
        .route("/v1/threads/:thread_id", get(get_thread))
        .route("/v1/threads/:thread_id/messages", post(send_message))
        .route("/v1/runs/:run_id/cancel", post(cancel_run))
        .route("/ws", get(websocket))
        .with_state(state)
}
async fn health() -> impl IntoResponse {
    Json(serde_json::json!({"ok": true, "protocol_version": PROTOCOL_VERSION}))
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
        _ => {
            let _ = outbound.send(serde_json::json!({"jsonrpc":"2.0","id":id,"error":{"_tag":"Cause","code":0,"message":format!("Unknown RPC method: {method}"),"data":[]}}).to_string());
        }
    }
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
