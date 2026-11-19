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
use tracing::{error, info};
use uuid::Uuid;

pub mod access_http;
pub mod auth_service;
mod contracts;
mod environment_http;
pub mod guest_service;
pub mod host_rpc;
pub mod issues;
pub mod local_services;
pub mod mcp_oauth;
mod projects;
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
        removed_threads: Vec<Uuid>,
    },
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
    pub projects: HashMap<String, Project>,
    project_rows: indexmap::IndexMap<String, serde_json::Value>,
    project_receipts: HashMap<String, (String, String)>,
    pub threads: HashMap<Uuid, Thread>,
}
impl Store {
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
                }
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
    environment_id: String,
    pub oauth: mcp_oauth::OAuth,
    connect: access_http::connect::Service,
    pub data_dir: PathBuf,
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
            environment_id: environment_http::identity(&dir)?,
            oauth: mcp_oauth::OAuth::open(&dir)?,
            connect: access_http::connect::Service::open(&dir)?,
            data_dir: dir.clone(),
            store: RwLock::new(store),
            events,
            log: Mutex::new(log),
            provider_command,
            cancellations: Mutex::new(HashMap::new()),
            auth: auth.open(&dir)?,
            guest: guest_service::GuestService::open(&dir)?,
            voice: voice::VoiceService::new(dir.clone()),
            pull_requests: pull_requests::PullRequestService::default(),
            issues: tokio::sync::Mutex::new(issues::IssueService::new(dir.clone())),
            vcs: local_services::VcsService,
            terminals: local_services::TerminalService::with_logs(dir.join("logs/terminals")),
            previews: local_services::PreviewService::default(),
            devices: local_services::DeviceService::default(),
            project_mutation_lock: Mutex::new(()),
            next_event_id: Mutex::new(next_event_id),
        }))
    }
    async fn commit(&self, event: Event) -> anyhow::Result<ServerEvent> {
        let mut store = self.store.write().await;
        let mut next_event_id = self.next_event_id.lock().await;
        let record = ServerEvent {
            id: *next_event_id + 1,
            at: now(),
            event,
        };
        let mut log = self.log.lock().await;
        use tokio::io::AsyncWriteExt;
        log.write_all(serde_json::to_string(&record)?.as_bytes())
            .await?;
        log.write_all(b"\n").await?;
        log.flush().await?;
        *next_event_id = record.id;
        store.apply(&record.event);
        let _ = self.events.send(record.clone());
        Ok(record)
    }
}

pub fn router(state: Arc<AppState>) -> Router {
    Router::new()
        .merge(provider_http::routes())
        .route("/api/projects", get(projects::snapshot))
        .route("/api/projects/mutate", post(projects::mutate_http))
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
        .merge(mcp_oauth::routes())
        .merge(access_http::routes())
        .merge(host_rpc::routes())
        .route(
            "/.well-known/t3/environment",
            get(environment_http::descriptor),
        )
        .route("/ws", get(websocket))
        .fallback(any(contract_fallback))
        .with_state(state)
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
                .map(str::to_owned)
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
            start_run(state.clone(), thread_id, text, "default".into())
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
                .id);
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
        "thread.guest-share.close" | "thread.guest-message.dismiss" => {
            let id = thread_id.ok_or_else(|| anyhow::anyhow!("Missing thread id."))?;
            if command_type == "thread.guest-share.close" {
                state.guest.close_thread(id)
            } else {
                state.guest.resolve_message(
                    id,
                    command["guestMessageId"].as_str().unwrap_or(""),
                    "dismissed",
                )
            }
            .map_err(anyhow::Error::msg)?;
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
        _ => anyhow::bail!("Unsupported orchestration command: {command_type}"),
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
    (StatusCode::UNAUTHORIZED, [("vary","Accept-Encoding"),("access-control-allow-origin","*")],
        Json(serde_json::json!({"_tag":"EnvironmentAuthInvalidError","code":"auth_invalid","reason":reason,"traceId":trace_id}))).into_response()
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
        state
            .auth
            .ticket_principal(&ticket, audience)
            .ok_or("invalid_credential")
    } else {
        access_http::principal(state, headers)
    }
}

async fn websocket(State(state): State<Arc<AppState>>, request: Request) -> Response {
    let query = url::form_urlencoded::parse(request.uri().query().unwrap_or("").as_bytes());
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
            .on_upgrade(move |socket| rpc::session(state, socket, principal))
            .into_response(),
        Err(rejection) => rejection.into_response(),
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
