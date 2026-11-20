#![recursion_limit = "512"]

use axum::{
    body::Bytes,
    extract::{FromRequest, Path, Request, State, WebSocketUpgrade},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{any, get, post},
    Json, Router,
};
use base64::{engine::general_purpose::STANDARD as BASE64, Engine};
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::Sha256;
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
mod native_child_work;
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
pub mod server_runtime;
mod static_http;
pub mod voice;
mod workspace;

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
    #[serde(default)]
    pub user_message_id: Option<String>,
    #[serde(default)]
    pub queue_held: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CheckpointRecord {
    pub turn_count: u64,
    pub tree: String,
    pub cwd: String,
    pub captured_at: u64,
    #[serde(default)]
    pub ready: bool,
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
    #[serde(default)]
    pub checkpoints: Vec<CheckpointRecord>,
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
    MessageEdited {
        thread_id: String,
        message_id: String,
        content: String,
    },
    RunReordered {
        thread_id: String,
        run_id: Uuid,
        before_run_id: Option<Uuid>,
    },
    RunStarted(Run),
    RunUpdated(Run),
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
    CheckpointCaptured {
        thread_id: String,
        checkpoint: CheckpointRecord,
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
            Event::MessageEdited {
                thread_id,
                message_id,
                content,
            } => {
                if let Some(t) = self.threads.get_mut(thread_id) {
                    if let Some(message) = t.messages.iter_mut().find(|m| m.id == *message_id) {
                        message.content = content.clone();
                    }
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
                    if let Some(existing) =
                        t.runs.iter_mut().find(|candidate| candidate.id == run.id)
                    {
                        *existing = run.clone();
                    } else {
                        t.runs.push(run.clone());
                    }
                }
            }
            Event::RunUpdated(run) => {
                if let Some(t) = self.threads.get_mut(&run.thread_id) {
                    if let Some(existing) =
                        t.runs.iter_mut().find(|candidate| candidate.id == run.id)
                    {
                        *existing = run.clone();
                    } else {
                        t.runs.push(run.clone());
                    }
                    t.status = match run.status.as_str() {
                        "completed" => "idle".into(),
                        "queued" => "running".into(),
                        status => status.into(),
                    };
                }
            }
            Event::RunReordered {
                thread_id,
                run_id,
                before_run_id,
            } => {
                if let Some(t) = self.threads.get_mut(thread_id) {
                    let Some(index) = t.runs.iter().position(|run| run.id == *run_id) else {
                        return;
                    };
                    let run = t.runs.remove(index);
                    let target = before_run_id
                        .and_then(|id| t.runs.iter().position(|candidate| candidate.id == id))
                        .unwrap_or(t.runs.len());
                    t.runs.insert(target, run);
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
            Event::CheckpointCaptured {
                thread_id,
                checkpoint,
            } => {
                if let Some(thread) = self.threads.get_mut(thread_id) {
                    if !thread.checkpoints.iter().any(|candidate| {
                        candidate.turn_count == checkpoint.turn_count
                            && candidate.tree == checkpoint.tree
                    }) {
                        thread.checkpoints.push(checkpoint.clone());
                        thread
                            .checkpoints
                            .sort_by_key(|candidate| candidate.turn_count);
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
                    if let Some(value) = patch.get("project_id").and_then(serde_json::Value::as_str)
                    {
                        thread.project_id = value.to_owned();
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
    pub(crate) pull_request_linked_refresh: Mutex<u64>,
    pub(crate) pull_request_linked_events: broadcast::Sender<u64>,
    pub(crate) scheduled_tasks: Mutex<indexmap::IndexMap<String, serde_json::Value>>,
    pub(crate) scheduled_tasks_path: PathBuf,
    pub(crate) scheduled_webhook_deliveries: Mutex<indexmap::IndexMap<String, serde_json::Value>>,
    pub(crate) scheduled_webhook_deliveries_path: PathBuf,
    pub(crate) scheduled_task_events: broadcast::Sender<()>,
    pub(crate) prompt_suggestion_events: broadcast::Sender<serde_json::Value>,
    pub(crate) provider_install_states: Mutex<HashMap<String, serde_json::Value>>,
    pub(crate) provider_auth_states: Mutex<HashMap<String, serde_json::Value>>,
    pub(crate) project_clone_states: Mutex<HashMap<String, serde_json::Value>>,
    pub(crate) project_clone_events: broadcast::Sender<()>,
    pub(crate) project_clone_cancellations: Mutex<HashMap<String, (u64, oneshot::Sender<()>)>>,
    pub(crate) project_clone_sequence: Mutex<u64>,
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
        let (pull_request_linked_events, _) = broadcast::channel(64);
        let (scheduled_task_events, _) = broadcast::channel(64);
        let (prompt_suggestion_events, _) = broadcast::channel(64);
        let (project_clone_events, _) = broadcast::channel(64);
        let scheduled_tasks_path = dir.join("scheduled-tasks.json");
        let scheduled_tasks = tokio::fs::read(&scheduled_tasks_path)
            .await
            .ok()
            .and_then(|bytes| serde_json::from_slice::<Vec<serde_json::Value>>(&bytes).ok())
            .map(|items| {
                items
                    .into_iter()
                    .filter_map(|task| {
                        let id = task["id"].as_str()?.to_owned();
                        Some((id, task))
                    })
                    .collect::<indexmap::IndexMap<_, _>>()
            })
            .unwrap_or_default();
        let scheduled_webhook_deliveries_path = dir.join("scheduled-webhook-deliveries.json");
        let scheduled_webhook_deliveries = tokio::fs::read(&scheduled_webhook_deliveries_path)
            .await
            .ok()
            .and_then(|bytes| serde_json::from_slice::<Vec<serde_json::Value>>(&bytes).ok())
            .map(|items| {
                items
                    .into_iter()
                    .filter_map(|delivery| {
                        let id = delivery["id"].as_str()?.to_owned();
                        Some((id, delivery))
                    })
                    .collect::<indexmap::IndexMap<_, _>>()
            })
            .unwrap_or_default();
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
            pull_request_linked_refresh: Mutex::new(0),
            pull_request_linked_events,
            scheduled_tasks: Mutex::new(scheduled_tasks),
            scheduled_tasks_path,
            scheduled_webhook_deliveries: Mutex::new(scheduled_webhook_deliveries),
            scheduled_webhook_deliveries_path,
            scheduled_task_events,
            prompt_suggestion_events,
            provider_install_states: Mutex::new(HashMap::new()),
            provider_auth_states: Mutex::new(HashMap::new()),
            project_clone_states: Mutex::new(HashMap::new()),
            project_clone_events,
            project_clone_cancellations: Mutex::new(HashMap::new()),
            project_clone_sequence: Mutex::new(0),
        }))
    }

    pub(crate) async fn persist_scheduled_tasks(&self) -> anyhow::Result<()> {
        let tasks = self.scheduled_tasks.lock().await;
        let bytes = serde_json::to_vec(&tasks.values().collect::<Vec<_>>())?;
        drop(tasks);
        let temporary = self.scheduled_tasks_path.with_extension("json.tmp");
        tokio::fs::write(&temporary, bytes).await?;
        tokio::fs::rename(temporary, &self.scheduled_tasks_path).await?;
        Ok(())
    }
    pub(crate) async fn persist_scheduled_webhook_deliveries(&self) -> anyhow::Result<()> {
        let deliveries = self.scheduled_webhook_deliveries.lock().await;
        let bytes = serde_json::to_vec(&deliveries.values().collect::<Vec<_>>())?;
        drop(deliveries);
        let temporary = self
            .scheduled_webhook_deliveries_path
            .with_extension("json.tmp");
        tokio::fs::write(&temporary, bytes).await?;
        tokio::fs::rename(temporary, &self.scheduled_webhook_deliveries_path).await?;
        Ok(())
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
        if let Event::ThreadPatched { ref patch, .. } = record.event {
            if patch["row"].get("pullRequests").is_some() || patch["row"].get("issues").is_some() {
                let mut revision = self.pull_request_linked_refresh.lock().await;
                *revision = revision.saturating_add(1);
                let _ = self.pull_request_linked_events.send(*revision);
            }
        }
        let _ = self.events.send(record.clone());
        Ok(record)
    }
}

pub fn router(state: Arc<AppState>) -> Router {
    Router::new()
        .merge(provider_http::routes())
        .route("/api/projects", get(projects::snapshot))
        .route("/api/projects/mutate", post(projects::mutate_http))
        .route("/api/observability/v1/traces", post(otlp_traces_proxy))
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
        // Hook credentials are carried in the URL. The public webhook route
        // must bypass the environment session middleware above.
        .route("/api/hooks/:hook_id/:token", any(webhook_handler))
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

async fn otlp_traces_proxy(
    State(state): State<Arc<AppState>>,
    headers: axum::http::HeaderMap,
    body: Bytes,
) -> Response {
    let session = match access_http::principal(&state, &headers) {
        Ok(session) => session,
        Err(reason) => return auth_error(&headers, reason),
    };
    if !session.has("orchestration:operate") {
        return access_http::scope_error(&headers, "orchestration:operate");
    }
    if serde_json::from_slice::<serde_json::Value>(&body).is_err() {
        return StatusCode::BAD_REQUEST.into_response();
    }
    let exporter_disabled = std::env::var("OTEL_TRACES_EXPORTER")
        .ok()
        .map(|value| {
            value
                .split(',')
                .any(|item| item.trim().eq_ignore_ascii_case("none"))
        })
        .unwrap_or(false)
        && !std::env::var("OTEL_TRACES_EXPORTER")
            .ok()
            .map(|value| {
                value
                    .split(',')
                    .any(|item| item.trim().eq_ignore_ascii_case("otlp"))
            })
            .unwrap_or(false);
    let endpoint = if exporter_disabled {
        None
    } else {
        let explicit = std::env::var("T3CODE_OTLP_TRACES_URL")
            .ok()
            .map(|value| value.trim().to_owned())
            .filter(|value| !value.is_empty())
            .or_else(|| {
                std::env::var("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT")
                    .ok()
                    .map(|value| value.trim().to_owned())
                    .filter(|value| !value.is_empty())
            })
            .or_else(|| {
                state
                    .settings
                    .internal()
                    .ok()
                    .and_then(|settings| {
                        settings["observability"]["otlpTracesUrl"]
                            .as_str()
                            .map(str::to_owned)
                    })
                    .filter(|value| !value.trim().is_empty())
            });
        let generic = std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT")
            .ok()
            .map(|value| value.trim().to_owned())
            .filter(|value| !value.is_empty())
            .and_then(|value| {
                let mut parsed = url::Url::parse(&value).ok()?;
                if !matches!(parsed.scheme(), "http" | "https") {
                    return None;
                }
                parsed.set_path(&format!(
                    "{}/v1/traces",
                    parsed.path().trim_end_matches('/')
                ));
                Some(parsed.to_string())
            });
        explicit.or(generic)
    };
    let Some(endpoint) = endpoint else {
        return StatusCode::NO_CONTENT.into_response();
    };
    let mut request = reqwest::Client::new()
        .post(endpoint)
        .header("content-type", "application/json");
    let headers_value = std::env::var("OTEL_EXPORTER_OTLP_TRACES_HEADERS")
        .ok()
        .or_else(|| std::env::var("OTEL_EXPORTER_OTLP_HEADERS").ok());
    if let Some(headers_value) = headers_value {
        for pair in headers_value.split(',') {
            let Some((name, value)) = pair.split_once('=') else {
                continue;
            };
            let Ok(value) = percent_encoding::percent_decode_str(value).decode_utf8() else {
                continue;
            };
            if let (Ok(name), Ok(value)) = (
                reqwest::header::HeaderName::from_bytes(name.trim().as_bytes()),
                reqwest::header::HeaderValue::from_str(value.trim()),
            ) {
                request = request.header(name, value);
            }
        }
    }
    let response = request.body(body).send().await;
    match response {
        Ok(response) if response.status().is_success() => StatusCode::NO_CONTENT.into_response(),
        _ => (StatusCode::BAD_GATEWAY, "Trace export failed.").into_response(),
    }
}

async fn webhook_handler(
    State(state): State<Arc<AppState>>,
    Path((hook_id, token)): Path<(String, String)>,
    request: Request,
) -> Response {
    let outcome = |status: StatusCode, body: serde_json::Value, name: &'static str| {
        let mut response = (status, Json(body)).into_response();
        response.headers_mut().insert(
            "x-t3-hook-outcome",
            axum::http::HeaderValue::from_static(name),
        );
        response
    };
    if request.method() == axum::http::Method::HEAD {
        let mut response = outcome(
            StatusCode::METHOD_NOT_ALLOWED,
            serde_json::json!({"error":"method_not_allowed"}),
            "method_not_allowed",
        );
        response.headers_mut().insert(
            axum::http::header::ALLOW,
            axum::http::HeaderValue::from_static("GET, POST, PUT, PATCH"),
        );
        return response;
    }
    let headers = request.headers().clone();
    let method = request.method().as_str().to_owned();
    let query = request.uri().query().map(str::to_owned).unwrap_or_default();
    let body = match axum::body::to_bytes(request.into_body(), 1024 * 1024 + 1).await {
        Ok(bytes) if bytes.len() <= 1024 * 1024 => bytes,
        _ => {
            return outcome(
                StatusCode::PAYLOAD_TOO_LARGE,
                serde_json::json!({"error":"body_too_large"}),
                "body_too_large",
            )
        }
    };
    let tasks = state.scheduled_tasks.lock().await;
    let task = tasks
        .values()
        .find(|task| {
            task["schedule"]["type"] == "webhook"
                && task["id"].as_str().is_some_and(|id| id == hook_id)
                && task["_webhookToken"].as_str() == Some(token.as_str())
        })
        .cloned();
    let Some(task) = task else {
        return outcome(
            StatusCode::NOT_FOUND,
            serde_json::json!({"error":"hook_not_found"}),
            "not_found",
        );
    };
    let delivery_id = format!("delivery:{}", uuid::Uuid::new_v4());
    let headers_json = headers
        .iter()
        .filter_map(|(name, value)| {
            Some((
                name.as_str().to_ascii_lowercase(),
                json!(value.to_str().ok()?.to_owned()),
            ))
        })
        .collect::<serde_json::Map<_, _>>();
    let content_type = headers_json
        .get("content-type")
        .and_then(serde_json::Value::as_str)
        .map(str::to_owned);
    let record_delivery = |outcome_name: &str, signature_verified: bool, error: Option<String>| {
        let state = state.clone();
        let delivery_id = delivery_id.clone();
        let hook_id = hook_id.clone();
        let method = method.clone();
        let query = query.clone();
        let headers_json = headers_json.clone();
        let content_type = content_type.clone();
        let outcome_name = outcome_name.to_owned();
        let delivery_key = delivery_id.clone();
        let delivery = json!({
            "id":delivery_id,
            "taskId":hook_id,
            "receivedAt":crate::auth_service::iso(crate::auth_service::now()),
            "method":method,
            "query":query,
            "headers":headers_json,
            "body":String::from_utf8_lossy(&body),
            "bodyBytes":body.len(),
            "bodyTruncated":false,
            "outcome":outcome_name,
            "signatureVerified":signature_verified,
            "pendingWindow":false,
            "missingFields":[],
            "error":error,
            "renderedPrompt":null,
            "contentType":content_type
        });
        async move {
            state
                .scheduled_webhook_deliveries
                .lock()
                .await
                .insert(delivery_key, delivery);
            let _ = state.persist_scheduled_webhook_deliveries().await;
        }
    };
    if task["enabled"] != true {
        record_delivery("disabled", false, None).await;
        return outcome(
            StatusCode::CONFLICT,
            serde_json::json!({"error":"hook_disabled"}),
            "disabled",
        );
    }
    if let Some(signature) = task["schedule"]["signature"].as_object() {
        let valid =
            webhook_signature_valid(signature, task["_webhookSecret"].as_str(), &headers, &body);
        if !valid {
            record_delivery("rejected_signature", false, None).await;
            return outcome(
                StatusCode::UNAUTHORIZED,
                serde_json::json!({"error":"invalid_signature"}),
                "rejected_signature",
            );
        }
    }
    record_delivery("accepted", task["schedule"]["signature"].is_object(), None).await;
    let _ = body;
    outcome(
        StatusCode::ACCEPTED,
        serde_json::json!({"deliveryId":delivery_id}),
        "accepted",
    )
}

fn webhook_signature_valid(
    signature: &serde_json::Map<String, serde_json::Value>,
    secret: Option<&str>,
    headers: &HeaderMap,
    body: &[u8],
) -> bool {
    let Some(secret) = secret else { return false };
    let Some(header) = signature["header"].as_str() else {
        return false;
    };
    let Some(received) = headers.get(header).and_then(|v| v.to_str().ok()) else {
        return false;
    };
    let prefix = signature["prefix"].as_str().unwrap_or_default();
    let value = received.trim();
    if value.len() < prefix.len() {
        return false;
    }
    let Some(head) = value.get(..prefix.len()) else {
        return false;
    };
    if !head.eq_ignore_ascii_case(prefix) {
        return false;
    }
    let candidate = &value[prefix.len()..];
    let decoded = match signature["encoding"].as_str().unwrap_or("base64") {
        "hex" => hex::decode(candidate.to_ascii_lowercase()).ok(),
        _ => BASE64.decode(candidate).ok(),
    };
    let Some(decoded) = decoded else { return false };
    let Ok(mut mac) = Hmac::<Sha256>::new_from_slice(secret.as_bytes()) else {
        return false;
    };
    mac.update(body);
    mac.verify_slice(&decoded).is_ok()
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
        return static_http::client(
            state.runtime_config.static_dir.as_deref(),
            request.method(),
            path,
        )
        .await;
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
        .current_dir(&cwd)
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
                if let Ok(event) = serde_json::from_str::<serde_json::Value>(text.trim()) {
                    if event["type"] == "prompt_suggestion.updated"
                        && event["threadId"].is_string()
                        && event["runId"].is_string()
                        && event["text"].is_string()
                    {
                        let _ = s.prompt_suggestion_events.send(serde_json::json!({
                            "threadId": event["threadId"].clone(),
                            "runId": event["runId"].clone(),
                            "text": event["text"].clone(),
                        }));
                        continue;
                    }
                }
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
    let thread_id = run.thread_id.clone();
    state
        .commit(Event::RunFinished {
            run_id: run.id,
            thread_id: thread_id.clone(),
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
    if ok && !cancelled {
        if let Some(tree) = capture_git_tree(&cwd).await {
            let turn_count = state
                .store
                .read()
                .await
                .threads
                .get(&thread_id)
                .map(|thread| thread.messages.len() as u64)
                .unwrap_or_default();
            let _ = state
                .commit(Event::CheckpointCaptured {
                    thread_id,
                    checkpoint: CheckpointRecord {
                        turn_count,
                        tree,
                        cwd,
                        captured_at: now(),
                        ready: true,
                    },
                })
                .await;
        }
    }
    Ok(())
}

async fn capture_git_tree(cwd: &str) -> Option<String> {
    let cwd = std::path::Path::new(cwd);
    if !cwd.is_dir() {
        return None;
    }
    let probe = Command::new("git")
        .args(["-C", cwd.to_str()?, "rev-parse", "--show-toplevel"])
        .output()
        .await
        .ok()?;
    if !probe.status.success() {
        return None;
    }
    let index = tempfile::NamedTempFile::new().ok()?;
    let index_path = index.path().to_path_buf();
    drop(index);
    let _ = tokio::fs::remove_file(&index_path).await;
    let index_text = index_path.to_str()?.to_owned();
    let mut command = Command::new("git");
    command
        .args(["-C", cwd.to_str()?, "read-tree", "HEAD"])
        .env("GIT_INDEX_FILE", &index_text);
    if !command.status().await.ok()?.success() {
        let _ = tokio::fs::remove_file(&index_path).await;
        return None;
    }
    let mut add = Command::new("git");
    add.args(["-C", cwd.to_str()?, "add", "-A", "--"])
        .arg(".")
        .env("GIT_INDEX_FILE", &index_text);
    if !add.status().await.ok()?.success() {
        let _ = tokio::fs::remove_file(&index_path).await;
        return None;
    }
    let output = Command::new("git")
        .args(["-C", cwd.to_str()?, "write-tree"])
        .env("GIT_INDEX_FILE", &index_text)
        .output()
        .await
        .ok()?;
    let _ = tokio::fs::remove_file(&index_path).await;
    output
        .status
        .success()
        .then(|| String::from_utf8_lossy(&output.stdout).trim().to_owned())
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
    run_server_owned(state, listen, None).await
}

/// [`run_server`] for a process that holds the state directory's ownership
/// lock: the runtime record is published once listening and cleared on
/// SIGINT/SIGTERM.
pub async fn run_server_owned(
    state: Arc<AppState>,
    listen: std::net::SocketAddr,
    ownership: Option<server_runtime::ServerOwnership>,
) -> anyhow::Result<()> {
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
    let listener = tokio::net::TcpListener::bind(listen).await?;
    serve_bound(
        state,
        listener,
        ownership,
        server_runtime::shutdown_signal(),
    )
    .await
}

/// How long a graceful shutdown waits for open connections before the
/// process exits anyway.
const SHUTDOWN_GRACE: std::time::Duration = std::time::Duration::from_secs(5);

/// Serve on an already bound listener until `shutdown` resolves. When this
/// process owns the state directory, the runtime record is published with
/// the listener's real port (so port 0 records the chosen one) and is
/// cleared again, if still ours, when serving stops.
pub async fn serve_bound<F>(
    state: Arc<AppState>,
    listener: tokio::net::TcpListener,
    ownership: Option<server_runtime::ServerOwnership>,
    shutdown: F,
) -> anyhow::Result<()>
where
    F: std::future::Future<Output = ()> + Send + 'static,
{
    use std::future::IntoFuture;
    let bound = listener.local_addr()?;
    info!(listen = %bound, "t3code server listening");
    if let Some(owner) = &ownership {
        let record = server_runtime::RuntimeRecord::new(
            state.runtime_config.requested_host,
            bound.port(),
            Some(state.settings.policy.loaded()),
        );
        if let Err(error) = owner.publish(record) {
            tracing::warn!(
                path = %owner.state_path().display(),
                %error,
                "failed to persist server runtime state"
            );
        }
    }
    let (stopping_tx, stopping_rx) = oneshot::channel::<()>();
    let server = axum::serve(
        listener,
        router(state).into_make_service_with_connect_info::<std::net::SocketAddr>(),
    )
    .with_graceful_shutdown(async move {
        shutdown.await;
        info!("shutting down t3code server");
        let _ = stopping_tx.send(());
    });
    let grace = async move {
        match stopping_rx.await {
            Ok(()) => tokio::time::sleep(SHUTDOWN_GRACE).await,
            Err(_) => std::future::pending().await,
        }
    };
    let result = tokio::select! {
        result = server.into_future() => result.map_err(anyhow::Error::from),
        () = grace => {
            tracing::warn!("open connections did not close within the shutdown grace period");
            Ok(())
        }
    };
    drop(ownership);
    result
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
