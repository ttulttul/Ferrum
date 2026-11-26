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

mod assets;

/// The defect `assets.createUrl` answers for a resource it cannot decode,
/// as the reference's schema words it.
pub(crate) const ASSET_RESOURCE_DEFECT: &str = "Expected { readonly \"_tag\": \"workspace-file\", ... } | { readonly \"_tag\": \"media-file\", readonly \"disposition\"?: \"attachment\", readonly \"hostOnly\"?: true, ... } | { readonly \"_tag\": \"draft-workspace-file\", readonly \"disposition\"?: \"attachment\", ... } | { readonly \"_tag\": \"attachment\", readonly \"renderIntent\"?: \"mcp-app\", ... } | { readonly \"_tag\": \"tool-output-image\", ... } | { readonly \"_tag\": \"project-favicon\", ... } | { readonly \"_tag\": \"native-app-icon\", ... } | { readonly \"_tag\": \"environment-theme-background\", ... } | { readonly \"_tag\": \"github-media\", ... }\n  at [\"resource\"]";

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
    /// This environment's /mcp endpoint as reached from its workspace.
    /// Supplied by the embedding host, independently of incoming requests.
    pub workspace_mcp_endpoint: Option<url::Url>,
    /// Administrator-supplied managed policy in the reference policy.json format.
    pub managed_policy: Option<Value>,
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
    settings: crate::settings::Settings,
    started_at: String,
    state: ControlState,
    /// The MCP sign-in's grants, kept in the state documents.
    oauth: crate::mcp_oauth::OAuth,
    #[cfg(feature = "native")]
    /// Links from this environment to other environments.
    peer_links: crate::peer_links::PeerLinks,
    #[cfg(feature = "native")]
    /// MCP sessions with linked environments.
    peer_mcp: crate::peer_mcp::PeerMcpClient,
    #[cfg(feature = "native")]
    /// Link cards being answered now.
    link_answers: crate::peer_link_requests::Answering,
    asset_key: Mutex<Option<[u8; 32]>>,
    signals: Mutex<Signals>,
    /// Held while an orchestration command checks and commits.
    mutation_lock: tokio::sync::Mutex<()>,
    /// Serializes `projectClone.*`: a start's claim, project and launch are
    /// separate workspace calls.
    clone_lock: tokio::sync::Mutex<()>,
    cancellations: tokio::sync::Mutex<HashMap<uuid::Uuid, tokio::sync::oneshot::Sender<()>>>,
    host: Host,
    /// Whether changes are waiting for the host's next alarm to deliver.
    wake: std::sync::atomic::AtomicBool,
    shell_deadline: Mutex<Option<i64>>,
    /// The models each Codex instance's CLI last listed, by instance, kept
    /// in the state documents.
    listed_models: Mutex<BTreeMap<String, Vec<Value>>>,
}
impl AuthHost for ControlPlane {
    fn auth(&self) -> &AuthService {
        &self.state.auth
    }
    fn pairing_refusal(&self) -> Option<String> {
        self.settings.policy.pairing_refusal()
    }
}
impl crate::mcp_oauth::McpHost for ControlPlane {
    fn oauth(&self) -> &crate::mcp_oauth::OAuth {
        &self.oauth
    }
    fn environment_id(&self) -> &str {
        &self.config.environment_id
    }
    fn linked_environments_enabled(&self) -> bool {
        self.settings()["linkedEnvironmentsEnabled"] == true
    }
    fn control_state(&self) -> &ControlState {
        &self.state
    }
    fn descriptor(&self) -> Value {
        ControlPlane::descriptor(self)
    }
    fn settings(&self) -> Value {
        ControlPlane::settings(self)
    }
    fn providers(&self) -> Value {
        self.providers_for(&self.settings(), &Value::Null)
    }
    fn dispatch(
        self: &Arc<Self>,
        command: Value,
    ) -> crate::mcp_oauth::HostFuture<Result<u64, Value>> {
        // As `orchestration.dispatchCommand` runs it; see `call`.
        #[allow(clippy::arc_with_non_send_sync)]
        let host = Arc::new(PlaneCommands {
            plane: self.clone(),
        });
        Box::pin(crate::dispatch_orchestration_command(host, command))
    }
    fn cancel_delegated_task(
        self: &Arc<Self>,
        parent_thread_id: String,
        task_id: String,
        stop: Value,
    ) -> crate::mcp_oauth::HostFuture<Result<(), Value>> {
        #[allow(clippy::arc_with_non_send_sync)]
        let host = Arc::new(PlaneCommands {
            plane: self.clone(),
        });
        Box::pin(async move {
            crate::orchestration::cancel_task(host, &parent_thread_id, &task_id, stop).await
        })
    }
    fn mutate_project(
        self: &Arc<Self>,
        command: Value,
    ) -> crate::mcp_oauth::HostFuture<Result<Value, Value>> {
        let plane = self.clone();
        Box::pin(async move {
            match plane.create_project(&command).await {
                Reply::Success(row) => Ok(row),
                Reply::Failure(mut error) => Err(error["error"].take()),
                _ => Err(json!({"message":"Failed to mutate project."})),
            }
        })
    }
    fn update_settings(
        self: &Arc<Self>,
        patch: Value,
    ) -> crate::mcp_oauth::HostFuture<Result<(), ()>> {
        let plane = self.clone();
        Box::pin(async move {
            plane
                .settings
                .update(&json!({"patch": patch}), false)
                .map(|_| ())
                .map_err(|_| ())
        })
    }
    #[cfg(feature = "native")]
    fn peer_links_listed(
        self: &Arc<Self>,
    ) -> crate::mcp_oauth::HostFuture<Result<Vec<Value>, String>> {
        let plane = self.clone();
        Box::pin(async move {
            plane
                .peer_links
                .list()
                .await
                .map(|links| {
                    links
                        .into_iter()
                        .map(|(link, status)| {
                            let mut row = link.to_json();
                            row["status"] = json!(status);
                            row
                        })
                        .collect()
                })
                .map_err(|error| error.message)
        })
    }
    #[cfg(feature = "native")]
    fn unlink_peer(&self, environment_id: &str) -> Result<bool, String> {
        self.peer_links
            .unlink(environment_id)
            .map_err(|error| error.message)
    }
    #[cfg(feature = "native")]
    fn unlink_peer_ending(
        self: &Arc<Self>,
        environment_id: &str,
    ) -> crate::mcp_oauth::HostFuture<Result<(bool, bool), String>> {
        let plane = self.clone();
        let environment_id = environment_id.to_owned();
        Box::pin(async move {
            plane
                .peer_links
                .unlink_ending(&environment_id)
                .await
                .map_err(|error| error.message)
        })
    }
    #[cfg(feature = "native")]
    fn peer_link(&self, environment_id: &str) -> Option<crate::mcp_oauth::PeerLinkInfo> {
        let link = self.peer_links.get(environment_id).ok().flatten()?;
        Some(crate::mcp_oauth::PeerLinkInfo {
            expired: link.expired(crate::platform::now_ms()),
            label: link.label,
            access: link.access,
        })
    }
    #[cfg(feature = "native")]
    fn forward_tool(
        self: &Arc<Self>,
        environment_id: String,
        tool: String,
        arguments: Value,
        limit: String,
        sender: Option<String>,
    ) -> Option<crate::mcp_oauth::HostFuture<Result<Value, (&'static str, String)>>> {
        let plane = self.clone();
        Some(Box::pin(async move {
            plane
                .peer_mcp
                .call(
                    &plane.peer_links,
                    &environment_id,
                    &tool,
                    arguments,
                    &limit,
                    sender.as_deref(),
                )
                .await
        }))
    }
    fn create_named_project(
        self: &Arc<Self>,
        name: String,
    ) -> crate::mcp_oauth::HostFuture<Result<Value, Value>> {
        let plane = self.clone();
        Box::pin(async move { plane.create_new_project(&name).await })
    }
    fn is_scratch_project(
        self: &Arc<Self>,
        project_id: &str,
    ) -> crate::mcp_oauth::HostFuture<bool> {
        let plane = self.clone();
        let project_id = project_id.to_owned();
        Box::pin(async move { plane.scratch_project_matches(&project_id).await })
    }
    fn ensure_scratch_project(
        self: &Arc<Self>,
    ) -> crate::mcp_oauth::HostFuture<Result<String, String>> {
        let plane = self.clone();
        Box::pin(async move {
            plane.scratch_project_id().await.map_err(|error| {
                error["message"]
                    .as_str()
                    .unwrap_or("Failed to create scratch project.")
                    .to_owned()
            })
        })
    }
    fn scratch_thread_folder(
        self: &Arc<Self>,
        thread_id: &str,
        text: &str,
    ) -> crate::mcp_oauth::HostFuture<Result<Option<String>, String>> {
        let plane = self.clone();
        let thread_id = thread_id.to_owned();
        let text = text.to_owned();
        Box::pin(async move { plane.new_scratch_thread_folder(&thread_id, &text).await })
    }
    fn workspace_call(
        self: &Arc<Self>,
        method: String,
        input: Value,
    ) -> crate::mcp_oauth::HostFuture<Result<Value, Value>> {
        let plane = self.clone();
        Box::pin(async move {
            let settings = WorkspaceSettings {
                automatic_git_fetch: true,
                ..Default::default()
            };
            match dispatch::mcp_workspace(&plane.host.workspace, &settings, &method, &input).await {
                Some(result) => result,
                None => Err(json!({
                    "_tag": "Unavailable",
                    "message": format!("Workspace operation is unavailable: {method}"),
                })),
            }
        })
    }
    fn sleep(&self, duration: std::time::Duration) -> crate::runtime::Wait {
        self.host.runtime.sleep(duration)
    }
    fn spawn(&self, task: crate::runtime::Task) {
        self.host.runtime.spawn(task)
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
    shell: broadcast::Receiver<crate::shell_stream::Change>,
    suggestions: broadcast::Receiver<Value>,
    linked: broadcast::Receiver<u64>,
    scheduled_tasks: broadcast::Receiver<()>,
    access: mpsc::UnboundedReceiver<auth_service::access::Change>,
    workspace: broadcast::Receiver<crate::workspace::BusFrame>,
    /// Whether polled workspace state is due for another reading.
    poll: bool,
    settings: broadcast::Receiver<(Value, Value)>,
}

impl Signals {
    fn new(
        state: &ControlState,
        workspace: &RemoteWorkspace,
        settings: &crate::settings::Settings,
    ) -> Self {
        Self {
            workspace: workspace.bus_frames(),
            poll: false,
            settings: settings.subscribe(),
            events: state.events.subscribe(),
            shell: state.shell_events.subscribe(),
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
        let mut changes = Vec::new();
        loop {
            match self.shell.try_recv() {
                Ok(change) => changes.push(change),
                Err(Lagged(_)) => {
                    changes.clear();
                    signals.push(Signal::ShellResync);
                }
                Err(Empty | broadcast::error::TryRecvError::Closed) => break,
            }
        }
        if !changes.is_empty() {
            signals.push(Signal::Shell(changes));
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
        loop {
            match self.workspace.try_recv() {
                Ok(frame) => signals.push(Signal::Workspace(Box::new(frame))),
                // Records catch up when they see a frame of the next stream.
                Err(Lagged(_)) => {}
                Err(Empty | broadcast::error::TryRecvError::Closed) => break,
            }
        }
        loop {
            match self.settings.try_recv() {
                Ok(views) => signals.push(Signal::SettingsChanged(Arc::new(views))),
                Err(Lagged(_)) => continue,
                Err(Empty | broadcast::error::TryRecvError::Closed) => break,
            }
        }
        if std::mem::take(&mut self.poll) {
            signals.push(Signal::Poll);
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
        mut config: Config,
        documents: Arc<dyn DocumentStore>,
        log: Box<dyn EventLog + Send>,
        host: Host,
    ) -> anyhow::Result<Arc<Self>> {
        if let Some(url) = &config.workspace_mcp_endpoint {
            anyhow::ensure!(
                matches!(url.scheme(), "https" | "http")
                    && url.has_host()
                    && url.username().is_empty()
                    && url.password().is_none()
                    && url.query().is_none()
                    && url.fragment().is_none(),
                "workspace_mcp_endpoint must be an HTTP(S) URL without credentials, query or fragment"
            );
        }
        let settings = crate::settings::Settings::open_plane(
            std::path::Path::new(STATE_DIR),
            documents.clone(),
            config.managed_policy.take(),
        )?;
        let auth = AuthService::with_bootstrap(config.bootstrap_credential.clone())
            .with_port(config.cookie_port)
            .open_in(documents.clone(), std::path::Path::new(STATE_DIR))?;
        auth.revoke_disallowed(
            settings.policy.allows_client("remote"),
            settings.policy.allows_connect(),
        )?;
        let started_at = auth_service::iso(auth_service::now());
        let mut oauth =
            crate::mcp_oauth::OAuth::open(documents.clone(), std::path::Path::new(STATE_DIR))?;
        if config.workspace_mcp_endpoint.is_some() {
            oauth.restore_thread_credentials()?;
        }
        let state = ControlState::open(auth, documents, log, std::path::Path::new(STATE_DIR))?;
        let signals = Mutex::new(Signals::new(&state, &host.workspace, &settings));
        let plane = Arc::new(Self {
            config,
            settings,
            started_at,
            state,
            oauth,
            #[cfg(feature = "native")]
            peer_links: crate::peer_links::PeerLinks::new(
                std::path::Path::new(STATE_DIR),
                &std::path::Path::new(STATE_DIR).join("secrets"),
            ),
            #[cfg(feature = "native")]
            peer_mcp: Default::default(),
            #[cfg(feature = "native")]
            link_answers: Default::default(),
            signals,
            asset_key: Default::default(),
            mutation_lock: Default::default(),
            clone_lock: Default::default(),
            cancellations: Default::default(),
            host,
            wake: Default::default(),
            shell_deadline: Default::default(),
            listed_models: Default::default(),
        });
        let listed = plane
            .documents
            .read_optional(&Self::listed_models_path())
            .ok()
            .flatten()
            .and_then(|bytes| serde_json::from_slice(&bytes).ok());
        if let Some(listed) = listed {
            *plane
                .listed_models
                .lock()
                .unwrap_or_else(|e| e.into_inner()) = listed;
        }
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
        self: &Arc<Self>,
        transport: Arc<dyn Transport>,
    ) -> Result<HelloReply, WireError> {
        let hello = self.host.workspace.connected(transport).await;
        self.schedule();
        if hello.is_ok() {
            let plane = self.clone();
            self.host
                .runtime
                .spawn(Box::pin(async move { plane.list_provider_models().await }));
        }
        hello
    }

    fn listed_models_path() -> PathBuf {
        std::path::Path::new(STATE_DIR).join("provider-models.json")
    }

    /// Ask each enabled Codex instance's CLI for its models, as the
    /// reference's provider check does, and keep what it lists.
    async fn list_provider_models(&self) {
        let Ok(settings) = self.settings.internal() else {
            return;
        };
        let providers = self.providers_for(&settings, &Value::Null);
        let instances: Vec<String> = providers
            .as_array()
            .into_iter()
            .flatten()
            .filter(|p| p["driver"] == "codex" && p["enabled"] == true)
            .filter_map(|p| p["instanceId"].as_str().map(str::to_owned))
            .collect();
        let mut changed = false;
        for id in instances {
            let row = json!({"modelSelection": {"instanceId": id}});
            let Some((provider, env)) = crate::provider_state::provider_turn(
                &settings,
                &crate::model_manifest::BUNDLED,
                &row,
                &Default::default(),
            ) else {
                continue;
            };
            let spec = crate::workspace::TurnSpec {
                cwd: WORKSPACE_ROOT.into(),
                env_remove: self.managed_env_remove(),
                env,
                provider: Some(provider),
                ..Default::default()
            };
            match self.host.workspace.provider_models(spec).await {
                Ok(entries) if !entries.is_empty() => {
                    let models = crate::codex_models::from_model_list(&entries);
                    let mut listed = self.listed_models.lock().unwrap_or_else(|e| e.into_inner());
                    changed |= listed.get(&id) != Some(&models);
                    listed.insert(id, models);
                }
                Ok(_) => tracing::warn!(instance = %id, "Codex listed no models"),
                Err(error) => {
                    tracing::warn!(instance = %id, %error, "could not list Codex's models")
                }
            }
        }
        if changed {
            let listed = self
                .listed_models
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .clone();
            if let Ok(bytes) = serde_json::to_vec(&listed) {
                let path = Self::listed_models_path();
                if let Err(error) =
                    self.documents
                        .write(&path, &bytes, crate::persistence::WriteMode::Replace)
                {
                    tracing::error!(%error, "could not keep Codex's models");
                }
            }
            self.wake();
        }
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
        } else {
            let deadline = *self
                .shell_deadline
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            self.host
                .workspace
                .is_ready()
                .then_some(now + MAINTAIN_INTERVAL_MS)
                .into_iter()
                .chain(deadline)
                .min()
        }
    }

    /// The alarm the plane asked for is due: keep the workspace connection
    /// healthy and ask for the next alarm. The host then delivers what
    /// changed ([`take_signals`](Self::take_signals) and
    /// [`deliver`](Self::deliver)) to every connection.
    pub async fn on_alarm(&self) {
        self.shell_deadline
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take();
        self.wake.store(false, std::sync::atomic::Ordering::Relaxed);
        self.signals.lock().unwrap_or_else(|e| e.into_inner()).poll = true;
        self.host.workspace.maintain().await;
        self.host.workspace.flush_acks();
        self.schedule();
    }

    /// Whether the host may serve this type of client (remote, desktop or
    /// local-browser). The embedding host knows where its listener is bound.
    pub fn allows_client(&self, kind: &str) -> bool {
        self.settings.policy.allows_client(kind)
    }

    /// Effective settings in the legacy client's redacted format.
    pub fn settings(&self) -> Value {
        self.settings.client(false).unwrap_or_default()
    }

    /// How `run` runs: its thread's provider instance's CLI when the
    /// workspace can run that provider, otherwise the configured command.
    async fn turn_spec(
        &self,
        run: &crate::Run,
        cwd: String,
        prompt: String,
    ) -> std::io::Result<crate::workspace::TurnSpec> {
        let provider = {
            let store = self.store.read().await;
            crate::provider_state::thread_provider_turn(
                &self.settings.internal().map_err(std::io::Error::other)?,
                &crate::model_manifest::BUNDLED,
                &store,
                &run.thread_id,
                &self.oauth,
                self.config
                    .workspace_mcp_endpoint
                    .as_ref()
                    .map(url::Url::as_str),
            )?
        };
        let mut spec = crate::workspace::TurnSpec {
            prompt,
            cwd,
            turn_id: run.id.to_string(),
            env_remove: self.managed_env_remove(),
            ..Default::default()
        };
        match provider {
            Some((provider, env)) => {
                spec.provider = Some(provider);
                spec.env = env;
            }
            None => {
                spec.command = if self.config.provider_command.is_empty() {
                    crate::turn::echo_command()
                } else {
                    self.config.provider_command.clone()
                };
            }
        }
        Ok(spec)
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
            ("linux", "x64", Some("desktop")),
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
            .merge(assets::routes())
            .merge(access_http::auth_routes::<ControlPlane>())
            .merge(crate::mcp_oauth::routes::<ControlPlane>())
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
        for signal in signals {
            if let Signal::SettingsChanged(views) = signal {
                let settings = if connection.references {
                    &views.1
                } else {
                    &views.0
                };
                for stream in connection
                    .open
                    .values()
                    .filter(|s| s.method == "subscribeServerConfig")
                {
                    let event = json!({"version":1,"type":"settingsUpdated","payload":{"settings":settings}});
                    frames.push(Reply::Chunk(json!([event])).frame(&stream.id));
                }
            }
        }
        for (key, stream) in connection.open.iter().filter(|(_, s)| s.live) {
            let Some(mut subscription) = self.load(connection, key) else {
                continue;
            };
            let before = subscription.cursor.clone();
            if subscription.method == "orchestration.subscribeShell" {
                let now = self.host.runtime.now_ms();
                let (batches, deadline) =
                    crate::shell_stream::deliver(self, &mut subscription, signals, now).await;
                frames.extend(
                    batches
                        .into_iter()
                        .map(|batch| Reply::Chunk(batch).frame(&stream.id)),
                );
                if let Some(deadline) = deadline {
                    let mut next = self
                        .shell_deadline
                        .lock()
                        .unwrap_or_else(|e| e.into_inner());
                    *next = Some(next.map_or(deadline, |at| at.min(deadline)));
                    drop(next);
                    self.schedule();
                }
                if subscription.cursor != before {
                    self.store(connection, key, &subscription);
                }
                continue;
            }
            let workspace_stream = crate::control_streams::serves(&subscription.method);
            for signal in signals {
                let batches = if workspace_stream {
                    crate::control_streams::advance(&self.host.workspace, &mut subscription, signal)
                        .await
                } else {
                    subscription
                        .advance(self, &connection.principal, signal.clone())
                        .await
                };
                for batch in batches {
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
        match self.call(connection, method, params).await {
            Call::Done(reply) => vec![reply.frame(&id)],
            Call::Live(subscription, values) if subscription.cursor["finished"] == true => {
                let mut frames: Vec<_> = values
                    .into_iter()
                    .map(|v| Reply::Chunk(v).frame(&id))
                    .collect();
                frames.push(Reply::Success(Value::Null).frame(&id));
                frames
            }
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

    async fn call(self: &Arc<Self>, connection: &Connection, method: &str, params: &Value) -> Call {
        if !crate::rpc_known(method) {
            return Call::Done(Reply::defect(format!("Unknown request tag: {method}")));
        }
        let decoded = match dispatch::check(&connection.principal, method, params) {
            Ok(decoded) => decoded,
            Err(reply) => return Call::Done(reply),
        };
        let params = decoded.as_ref().unwrap_or(params);
        if crate::control_streams::serves(method) {
            return match crate::control_streams::open(&self.host.workspace, method, params).await {
                Ok((subscription, first)) => Call::Live(subscription, first),
                Err(reply) => Call::Done(reply),
            };
        }
        if crate::subscription::serves(method) {
            return match Subscription::open(self, &connection.principal, method, params, false)
                .await
            {
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
        if method == "orchestration.launchThread" {
            if params["workspaceStrategy"]["type"] == "worktree" {
                return Call::Done(crate::thread_launch::failure(
                    params,
                    json!("Creating a new git worktree during launch is unavailable on this host."),
                ));
            }
            return Call::Done(crate::thread_launch::launch(self, params).await);
        }
        if method == "projects.mutate" {
            return Call::Done(self.create_project(params).await);
        }
        if method == "projects.ensureScratch" {
            return Call::Done(match self.scratch_project_id().await {
                Ok(project_id) => Reply::Success(json!({"projectId": project_id})),
                Err(error) => Reply::Failure(json!({"_tag":"Fail","error":error})),
            });
        }
        if method == "server.refreshProviders" {
            if self.host.workspace.is_ready() {
                self.list_provider_models().await;
            }
            return Call::Done(Reply::Success(
                json!({"providers":self.providers_for(&self.settings(), &Value::Null)}),
            ));
        }
        if method == "projects.createNew" {
            let name = params["name"].as_str().unwrap_or_default().trim();
            if name.is_empty() {
                return Call::Done(Reply::defect(
                    "Expected a non-blank string\n  at [\"name\"]".into(),
                ));
            }
            return Call::Done(match self.create_new_project(name).await {
                Ok(created) => Reply::Success(created),
                Err(error) => Reply::Failure(json!({"_tag":"Fail","error":error})),
            });
        }
        if method == "assets.createUrl" {
            return Call::Done(self.create_asset_url(params).await);
        }
        if method == "projectClone.start" {
            return Call::Done(self.start_project_clone(params).await);
        }
        if method == "projectClone.cancel" || method == "projectClone.retry" {
            let name = method.trim_start_matches("projectClone.");
            let _guard = self.clone_lock.lock().await;
            let input = json!({"projectId": params["projectId"]});
            return Call::Done(match self.host.workspace.clone_call(name, &input).await {
                Some(Ok(applied)) => Reply::Success(json!({ "applied": applied == true })),
                // As native, cancel has no error to report: nothing was applied.
                Some(Err(_)) | None if name == "cancel" => {
                    Reply::Success(json!({ "applied": false }))
                }
                Some(Err(error)) => clone_failure(error),
                None => clone_failure(Value::Null),
            });
        }
        #[cfg(feature = "native")]
        if method == "peerLinks.list" {
            return Call::Done(self.peer_links_list().await);
        }
        #[cfg(feature = "native")]
        if method == "peerLinks.link" {
            return Call::Done(self.peer_links_link(params).await);
        }
        #[cfg(feature = "native")]
        if method == "peerLinks.unlink" {
            return Call::Done(self.peer_links_unlink(params).await);
        }
        #[cfg(feature = "native")]
        if method == "peerLinks.answerRequest" {
            return Call::Done(self.peer_links_answer(params).await);
        }
        if let Some(call) = self.host_call(method, params, connection.references) {
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

    #[cfg(feature = "native")]
    async fn peer_links_list(&self) -> Reply {
        match self.peer_links.list().await {
            Ok(links) => Reply::Success(json!({
                "links": links.into_iter().map(|(link, status)| {
                    let mut row = link.to_json();
                    row["status"] = json!(status);
                    row
                }).collect::<Vec<_>>()
            })),
            Err(error) => Reply::Failure(json!({"_tag":"Fail","error":error.to_json()})),
        }
    }

    #[cfg(feature = "native")]
    async fn peer_links_link(&self, params: &Value) -> Reply {
        let input = match crate::peer_links::LinkInput::from_json(params) {
            Ok(input) => input,
            Err(path) => return Reply::defect(path),
        };
        let descriptor = self.descriptor();
        let own = crate::peer_links::SelfDescriptor {
            environment_id: self.config.environment_id.clone(),
            label: descriptor["label"].as_str().unwrap_or("T3 Code").to_owned(),
        };
        match self
            .peer_links
            .link(
                crate::mcp_oauth::McpHost::linked_environments_enabled(self),
                &own,
                &input,
            )
            .await
        {
            Ok(link) => Reply::Success(link.to_json()),
            Err(error) => Reply::Failure(json!({"_tag":"Fail","error":error.to_json()})),
        }
    }

    #[cfg(feature = "native")]
    async fn peer_links_unlink(&self, params: &Value) -> Reply {
        let Some(environment_id) = params["environmentId"].as_str() else {
            return Reply::defect("Missing key\n  at [\"environmentId\"]".into());
        };
        match self.peer_links.unlink_ending(environment_id).await {
            Ok((removed, ended_there)) => {
                Reply::Success(json!({"removed": removed, "endedThere": ended_there}))
            }
            Err(error) => Reply::Failure(json!({"_tag":"Fail","error":error.to_json()})),
        }
    }

    #[cfg(feature = "native")]
    async fn peer_links_answer(self: &Arc<Self>, params: &Value) -> Reply {
        let descriptor = self.descriptor();
        let own = crate::peer_links::SelfDescriptor {
            environment_id: self.config.environment_id.clone(),
            label: descriptor["label"].as_str().unwrap_or("T3 Code").to_owned(),
        };
        let plane = self.clone();
        match crate::peer_link_requests::answer_with(
            &self.state,
            &self.peer_links,
            &self.link_answers,
            crate::mcp_oauth::McpHost::linked_environments_enabled(&**self),
            &own,
            params,
            move |command| {
                let plane = plane.clone();
                async move {
                    #[allow(clippy::arc_with_non_send_sync)]
                    let host = Arc::new(PlaneCommands { plane });
                    crate::dispatch_orchestration_command(host, command).await
                }
            },
        )
        .await
        {
            Ok(()) => Reply::Success(Value::Null),
            Err(error) => Reply::Failure(json!({"_tag":"Fail","error":error.to_json()})),
        }
    }

    /// The methods this host answers itself.
    fn host_call(&self, method: &str, params: &Value, references: bool) -> Option<Call> {
        Some(match method {
            "server.probe" => Call::Done(Reply::Success(
                json!({"serverTime": auth_service::iso(auth_service::now())}),
            )),
            "subscribeServerConfig" => {
                let snapshot = match self.config_snapshot(params, references) {
                    Ok(snapshot) => snapshot,
                    Err(error) => return Some(Call::result(Err(error))),
                };
                let mut batches = vec![json!([{"version":1,"type":"snapshot","config":snapshot}])];
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
            "server.getConfig" => Call::result(self.config_snapshot(&Value::Null, references)),
            "subscribeServerLifecycle" => {
                let environment = self.descriptor();
                Call::Stream(vec![json!([
                    {"version":1,"sequence":1,"type":"welcome","payload":{"environment":environment,"cwd":WORKSPACE_ROOT,"projectName":"workspace"}},
                    {"version":1,"sequence":2,"type":"ready","payload":{"at":self.started_at,"environment":environment}}
                ])])
            }
            // As native before any refresh: the stream stays open and silent.
            "pullRequests.subscribeRefreshes" => Call::Stream(Vec::new()),
            "server.getSettings" => Call::result(self.settings.client(references)),
            "server.updateSettings" => Call::result(self.settings.update(params, references)),
            "orchestration.getOlderSettledThreads" => {
                let sequence = self.sequence_now();
                let store = self.store_now();
                Call::Done(Reply::Success(crate::store::older_settled_threads(
                    &store, sequence, params,
                )))
            }
            "server.reportClientActivity" => Call::Done(Reply::Success(Value::Null)),
            "preview.reportProfiles" => Call::Done(Reply::Success(Value::Null)),
            "orchestration.getThreadProjection" => {
                let store = self.store_now();
                Call::Done(
                    match params["threadId"]
                        .as_str()
                        .and_then(|id| store.threads.get(id))
                    {
                        Some(thread) => Reply::Success(thread.snapshot_projection()),
                        None => Reply::Failure(json!({"_tag":"Fail",
                        "error":crate::subscription::thread_projection_error(&params["threadId"])})),
                    },
                )
            }
            _ => return None,
        })
    }

    /// `projectClone.start`, as the native server runs it, with git in the
    /// workspace: the workspace claims the destination, the plane creates
    /// the project on it, and the workspace starts the clone. Progress
    /// reaches clients through `subscribeProjectClones`.
    async fn start_project_clone(&self, params: &Value) -> Reply {
        let workspace = &self.host.workspace;
        if !workspace.is_ready() {
            return clone_failure(json!({"_tag": "Die", "defect":
                "The environment's workspace is still starting. Try again in a few seconds."}));
        }
        let _guard = self.clone_lock.lock().await;
        let claim = match workspace.clone_call("claim", params).await {
            Some(Ok(claim)) => claim,
            Some(Err(error)) => return clone_failure(error),
            None => return clone_failure(Value::Null),
        };
        let project_id = claim["projectId"].as_str().unwrap_or_default().to_owned();
        let cwd = claim["cwd"].as_str().unwrap_or_default().to_owned();
        let release = || async {
            let _ = workspace
                .clone_call("release", &json!({"projectId": project_id}))
                .await;
        };
        // The project is registered before git runs, so racing thread
        // creates and project deletes see the clone.
        let created = self
            .create_project(&json!({"type": "project.create",
                "commandId": format!("project-clone-create:{project_id}"),
                "projectId": project_id, "title": params["title"], "workspaceRoot": cwd,
                "createWorkspaceRootIfMissing": true}))
            .await;
        let owner = match created {
            Reply::Success(_) => project_id.clone(),
            refused => {
                // A failed clone's project still owns its empty folder after
                // a restart forgets the clone; clone for that project instead.
                let canonical = workspace
                    .canonicalize(std::path::Path::new(&cwd))
                    .await
                    .map(|p| p.to_string_lossy().into_owned())
                    .unwrap_or_else(|_| cwd.clone());
                let conflicting = self
                    .store_now()
                    .projects
                    .values()
                    .find(|p| p.id != project_id && p.path == canonical)
                    .map(|p| p.id.clone());
                let adopted = match &conflicting {
                    Some(to) => workspace
                        .clone_call("adopt", &json!({"from": project_id, "to": to}))
                        .await
                        .is_some_and(|r| r == Ok(json!(true))),
                    None => false,
                };
                match conflicting {
                    Some(to) if adopted => to,
                    Some(_) => {
                        release().await;
                        return Reply::Failure(json!({"_tag": "Fail", "error": {
                            "_tag": "SourceControlRepositoryError", "provider": "unknown",
                            "operation": "cloneRepository",
                            "detail": "A clone into this destination is already in progress."}}));
                    }
                    None => {
                        release().await;
                        let message = match &refused {
                            Reply::Failure(f) => f["error"]["message"].clone(),
                            _ => Value::Null,
                        };
                        return Reply::Failure(json!({"_tag": "Fail", "error": {
                            "_tag": "OrchestrationDispatchCommandError",
                            "message": "Failed to create clone project.",
                            "cause": {"name": "ProjectOperationError", "message": message}}}));
                    }
                }
            }
        };
        match workspace
            .clone_call("launch", &json!({"projectId": owner}))
            .await
        {
            Some(Ok(_)) => Reply::Success(json!({"projectId": owner, "cwd": cwd,
                "remoteUrl": claim["remoteUrl"], "repository": claim["repository"]})),
            Some(Err(error)) => clone_failure(error),
            None => clone_failure(Value::Null),
        }
    }

    /// `projects.mutate` with `project.create` only. As the native server,
    /// it creates the workspace root when asked, and records it as the
    /// workspace resolves it; a root that is not a folder there, or that
    /// another project has, is refused. Without a connected workspace the
    /// root is recorded as given.
    async fn create_project(&self, input: &Value) -> Reply {
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
        let _guard = self.mutation_lock.lock().await;
        if command.trim().is_empty()
            || id.trim().is_empty()
            || self.store_now().project_rows.contains_key(id)
        {
            return error("Failed to mutate project.");
        }
        let workspace = &self.host.workspace;
        let mut path = PathBuf::from(root);
        if input["createWorkspaceRootIfMissing"] == true {
            if let Err(cause) = workspace.create_dir_all(&path).await {
                tracing::warn!(%cause, root, "could not create a project's workspace root");
                return error("Failed to mutate project.");
            }
        }
        if workspace.is_ready() {
            path = match workspace.canonicalize(&path).await {
                Ok(path) => path,
                Err(_) => return error("Failed to mutate project."),
            };
            if !workspace.is_dir(&path).await {
                return error("Failed to mutate project.");
            }
        }
        let path = path.to_string_lossy().into_owned();
        if self.store_now().projects.values().any(|p| p.path == path) {
            return error("Failed to mutate project.");
        }
        let project = crate::Project {
            id: id.into(),
            name: title.into(),
            path,
            created_at: crate::now(),
        };
        let row = crate::store::wire_project(&project);
        match self.commit(Event::ProjectCreated(project)) {
            Ok(_) => Reply::Success(row),
            Err(_) => error("Failed to mutate project."),
        }
    }

    fn workspace_root(&self) -> PathBuf {
        self.host
            .workspace
            .root()
            .unwrap_or_else(|| PathBuf::from(WORKSPACE_ROOT))
    }

    fn scratch_root(&self) -> PathBuf {
        self.workspace_root().join("scratch")
    }

    /// `projects.ensureScratch`, as the native server runs it: ensures the
    /// scratch folder exists in the workspace and returns the single active
    /// "No project" project for it.
    async fn scratch_project_id(&self) -> Result<String, Value> {
        let root = self.scratch_root();
        crate::scratch::ensure_project(
            &self.host.workspace,
            &root,
            &self.state,
            &self.mutation_lock,
            // The plane's own commit, so subscribers hear of the project.
            |event| async move { self.commit(event).is_ok() },
        )
        .await
    }

    async fn scratch_project_matches(&self, project_id: &str) -> bool {
        let root = self.scratch_root();
        crate::scratch::is_project(&self.host.workspace, &root, &self.state, project_id).await
    }

    async fn new_scratch_thread_folder(
        &self,
        thread_id: &str,
        text: &str,
    ) -> Result<Option<String>, String> {
        let root = self.scratch_root();
        crate::scratch::folder_for_thread(&self.host.workspace, &root, thread_id, text)
            .await
            .map(Some)
    }

    /// `projects.createNew`, as the native server runs it: the workspace
    /// makes a new folder under its `projects` with a repository in it, and
    /// the project is recorded there.
    async fn create_new_project(&self, name: &str) -> Result<Value, Value> {
        let failure =
            |message: String| json!({"_tag":"OrchestrationDispatchCommandError","message":message});
        let root = self.workspace_root().join("projects");
        let created = self
            .host
            .workspace
            .create_named_project(&root, name)
            .await
            .map_err(|error| failure(error.to_string()))?;
        let id = crate::platform::uuid_v4().to_string();
        let project = crate::Project {
            id: id.clone(),
            name: name.into(),
            path: created.workspace_root.to_string_lossy().into_owned(),
            created_at: crate::now(),
        };
        {
            let _guard = self.mutation_lock.lock().await;
            if self.commit(Event::ProjectCreated(project)).is_err() {
                return Err(failure("Failed to create project.".into()));
            }
        }
        let mut result = json!({"projectId": id, "workspaceRoot": created.workspace_root});
        if let Some(error) = created.commit_error.filter(|e| !e.is_empty()) {
            result["commitError"] = json!(error);
        }
        Ok(result)
    }

    fn managed_env_remove(&self) -> Vec<String> {
        if self.settings.policy.summary().is_some() {
            vec!["T3CODE_CODEX_LAUNCH_ARGS".into()]
        } else {
            Vec::new()
        }
    }

    fn providers_for(&self, settings: &Value, options: &Value) -> Value {
        server_config::providers::list_with(
            &self.started_at,
            |driver| self.settings.policy.provider_denial(driver),
            |id| self.listed_models.lock().ok()?.get(id).cloned(),
            settings,
            options,
            &crate::model_manifest::BUNDLED,
            |_, row| row,
        )
    }

    /// The `subscribeServerConfig` snapshot. Host facts describe the
    /// workspace process; settings are [`settings`](Self::settings).
    fn config_snapshot(&self, options: &Value, references: bool) -> Result<Value, Value> {
        let settings = self.settings.client(references)?;
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
            "providers": self.providers_for(&settings, options),
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
        config["featureFlags"] = crate::feature_flags::server_feature_flags();
        if let Some(policy) = self.settings.policy.summary() {
            config["managedPolicy"] = policy.clone();
        }
        let workspace_root = self.workspace_root();
        config["scratchWorkspaceRoot"] = json!(workspace_root.join("scratch"));
        config["newProjectsRoot"] = json!(workspace_root.join("projects"));
        config["serverTime"] = json!(auth_service::iso(auth_service::now()));
        Ok(config)
    }
}

/// Orchestration commands as this plane runs them: over its control state,
/// with its effective settings and managed policy. Turns run
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
        self.plane.settings.policy.summary()
    }
    fn runtime_denial(&self, mode: &str) -> Option<String> {
        self.plane.settings.policy.runtime_denial(mode)
    }
    fn provider_denial(&self, driver: &str) -> Option<String> {
        self.plane.settings.policy.provider_denial(driver)
    }
    fn effective_settings(&self) -> anyhow::Result<Value> {
        self.plane.settings.internal()
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
        let runtime = plane.host.runtime.clone();
        runtime.spawn(Box::pin(async move {
            match plane.turn_spec(&run, cwd, text).await {
                Ok(spec) => crate::turn::launch(plane, run, spec, cancel).await,
                Err(error) => crate::turn::fail(&*plane, &run, error.into()).await,
            }
        }));
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
    fn turn_wait(&self, duration: std::time::Duration) -> crate::runtime::Wait {
        self.host.runtime.sleep(duration)
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

impl Call {
    fn result(result: Result<Value, Value>) -> Self {
        Self::Done(match result {
            Ok(value) => Reply::Success(value),
            Err(error) => Reply::Failure(json!({"_tag":"Fail","error":error})),
        })
    }
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

/// A `projectClone.*` failure clients can show: the workspace's typed errors
/// as they are, and anything else (a transport failure, a refused path, a
/// workspace too old to clone) as a `SourceControlRepositoryError` whose
/// detail says what happened. T3 Code shows a defect only as "An error
/// occurred.", so none is sent. `Null` means the workspace cannot clone.
fn clone_failure(error: Value) -> Reply {
    if matches!(
        error["_tag"].as_str(),
        Some("SourceControlRepositoryError" | "OrchestrationDispatchCommandError")
    ) {
        return Reply::Failure(json!({"_tag": "Fail", "error": error}));
    }
    let text = error["defect"]
        .as_str()
        .or(error["message"].as_str())
        .unwrap_or_default();
    let detail = if error.is_null() || text.contains("unknown op: clone.call") {
        "This environment's workspace cannot clone projects yet: its t3-agentd is older than \
         projectClone. Restart the environment on a newer image."
            .to_owned()
    } else if text.is_empty() {
        "The repository could not be cloned.".to_owned()
    } else {
        // "The workspace could not run <method>: <why>" says only <why> to a user.
        text.strip_prefix("The workspace could not run ")
            .and_then(|rest| rest.split_once(": "))
            .map_or(text, |(_, why)| why)
            .to_owned()
    };
    Reply::Failure(json!({"_tag": "Fail", "error": {
        "_tag": "SourceControlRepositoryError", "provider": "unknown",
        "operation": "cloneRepository", "detail": detail}}))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event_log::MemoryLog;
    use crate::persistence::MemoryStore;
    use crate::runtime::{Task, Wait};
    use crate::workspace::fake_agentd::FakeAgentd;
    use crate::workspace_protocol::{op, WireTurnEvent};
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

    /// An agentd whose workspace has the folder the tests' project uses.
    fn agentd_with_project_folder() -> Arc<FakeAgentd> {
        let agentd = Arc::new(FakeAgentd::default());
        agentd.add_dir("/workspace/one");
        agentd
    }

    fn config() -> Config {
        Config {
            environment_id: "env-test".into(),
            label: "test".into(),
            bootstrap_credential: "secret".into(),
            cookie_port: 3773,
            provider_command: vec!["provider".into()],
            workspace_mcp_endpoint: None,
            managed_policy: None,
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
        owner_socket(plane, "/ws?orchestrationProtocol=2")
    }

    fn owner_socket(plane: &ControlPlane, uri: &str) -> Connection {
        let (_, token) = plane.auth.browser_session("secret", None).unwrap();
        let mut headers = HeaderMap::new();
        headers.insert(
            "cookie",
            format!("t3_session_3773={token}").parse().unwrap(),
        );
        plane
            .authorize_socket(&headers, &uri.parse().unwrap())
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
    fn settings_rpcs_enforce_policy_and_keep_literals_out_of_client_views() {
        let documents = Arc::new(MemoryStore::new());
        let policy = json!({"schemaVersion":1,"organization":"Example",
            "settings":{"defaultAutoPull":{"locked":true,"value":false}},
            "limits":{"providers":{"allowedDrivers":["codex"]}}});
        let open = || {
            ControlPlane::open(
                Config {
                    managed_policy: Some(policy.clone()),
                    ..config()
                },
                documents.clone(),
                Box::new(MemoryLog::default()),
                host(&Arc::default()),
            )
            .unwrap()
        };
        let plane = open();
        let mut client = owner(&plane);
        let rpc =
            |plane: &Arc<ControlPlane>, client: &mut Connection, method: &str, payload: Value| {
                send(
                plane,
                client,
                json!({"_tag":"Request","id":method,"tag":method,"payload":payload,"headers":[]}),
            )
            .remove(0)
            };
        assert!(now(crate::mcp_oauth::McpHost::update_settings(
            &plane,
            json!({"defaultAutoPull":true})
        ))
        .is_err());
        let literal = "fixture-value-for-private-storage";
        let reference = json!({"provider":"vault","reference":"op://Example/Item/field"});
        let subscribed = rpc(&plane, &mut client, "subscribeServerConfig", json!({}));
        let snapshot = &subscribed["values"][0]["config"];
        assert_eq!(snapshot["managedPolicy"]["organization"], "Example");
        assert_eq!(snapshot["settings"]["defaultAutoPull"], false);
        let denied = rpc(
            &plane,
            &mut client,
            "server.updateSettings",
            json!({"patch":{
                "defaultAutoPull":true, "cloudSpeechCredentials":{"xai":literal}
            }}),
        );
        let reason = &denied["exit"]["cause"][0]["error"];
        assert_eq!(reason["operation"], "write-managed-setting");
        assert_eq!(reason["managedSetting"], "defaultAutoPull");
        assert_eq!(reason["managedBy"], "Example");
        let dir = std::path::Path::new(STATE_DIR);
        assert!(documents
            .read_optional(&dir.join("secrets/voice-cloud-xai.bin"))
            .unwrap()
            .is_none());

        let saved = rpc(
            &plane,
            &mut client,
            "server.updateSettings",
            json!({"patch":{
                "secretProviders":{"vault":{"kind":"one_password","label":"Vault","config":{"account":"example.test"}}},
                "cloudSpeechCredentials":{"openai":reference,"xai":literal}
            }}),
        );
        assert_eq!(saved["exit"]["_tag"], "Success");
        let public = &saved["exit"]["value"]["cloudSpeechCredentials"];
        assert_eq!(public["xai"], "••••••");
        assert_eq!(public["openai"], "••••••");
        let signals = plane.take_signals();
        let updates = parse(now(plane.deliver(&client, &signals)));
        assert_eq!(
            updates[0]["values"][0]["payload"]["settings"]["cloudSpeechCredentials"],
            *public
        );
        let disk = documents.read(&dir.join("settings.json")).unwrap();
        assert!(!String::from_utf8(disk).unwrap().contains(literal));
        assert!(
            documents
                .read(&dir.join("secrets/voice-cloud-xai.bin"))
                .unwrap()
                == literal.as_bytes()
        );
        drop(plane);

        let reopened = open();
        let mut client = owner_socket(
            &reopened,
            "/ws?orchestrationProtocol=2&clientCapabilities=settingsSecretReferences",
        );
        for tag in ["server.getSettings", "server.getConfig"] {
            let answer = rpc(&reopened, &mut client, tag, json!({}));
            assert!(!answer.to_string().contains(literal));
            let value = &answer["exit"]["value"];
            let settings = if tag == "server.getConfig" {
                &value["settings"]
            } else {
                value
            };
            assert_eq!(settings["cloudSpeechCredentials"]["openai"], reference);
            assert_eq!(settings["cloudSpeechCredentials"]["xai"], "••••••");
        }
        assert!(reopened.settings.internal().unwrap()["cloudSpeechCredentials"]["xai"] == literal);
        let cleared = rpc(
            &reopened,
            &mut client,
            "server.updateSettings",
            json!({"patch":{
                "cloudSpeechCredentials":{"xai":""}
            }}),
        );
        assert_eq!(cleared["exit"]["_tag"], "Success");
        assert!(documents
            .read_optional(&dir.join("secrets/voice-cloud-xai.bin"))
            .unwrap()
            .is_none());
    }

    #[test]
    fn agentd_turn_receives_the_saved_sensitive_environment_and_managed_arguments() {
        let runtime = Arc::new(ManualRuntime::default());
        let cfg = Config {
            managed_policy: Some(json!({"schemaVersion":1,"organization":"Example",
                "limits":{"providers":{"instances":{"codex":{"locked":false,"launchArgs":"--managed"}}}}})),
            ..config()
        };
        let documents = Arc::new(MemoryStore::new());
        let plane = ControlPlane::open(
            cfg,
            documents.clone(),
            Box::new(MemoryLog::default()),
            host(&runtime),
        )
        .unwrap();
        let agentd = agentd_with_project_folder();
        now(plane.workspace_connected(agentd.link(plane.workspace()))).unwrap();
        let mut client = owner(&plane);
        let value = "fixture-private-environment-value";
        let reply = send(
            &plane,
            &mut client,
            json!({"_tag":"Request","id":"settings","tag":"server.updateSettings","payload":{
            "patch":{"providerInstances":{"codex":{"driver":"codex","environment":[{"name":"EXAMPLE_KEY","value":value,"sensitive":true}]}}}
        },"headers":[]}),
        );
        assert_eq!(reply[0]["exit"]["_tag"], "Success");
        assert!(!reply[0].to_string().contains(value));
        let stored = documents
            .read(&std::path::Path::new(STATE_DIR).join("settings.json"))
            .unwrap();
        assert!(!String::from_utf8(stored).unwrap().contains(value));
        seed(&plane, &mut client);
        dispatch_message(&plane, &mut client, "hello");
        runtime.run();
        let turns = agentd.turns();
        let start = turns.values().next().unwrap();
        assert!(start
            .env
            .iter()
            .any(|(key, v)| key == "EXAMPLE_KEY" && v == value));
        assert_eq!(start.env_remove, ["T3CODE_CODEX_LAUNCH_ARGS"]);
        assert_eq!(start.provider.as_ref().unwrap().launch_args, ["--managed"]);
    }

    #[test]
    fn creates_a_projects_folder_in_the_workspace_when_asked() {
        let plane = plane();
        let agentd = Arc::new(FakeAgentd::default());
        now(plane.workspace_connected(agentd.link(plane.workspace()))).unwrap();
        let mut connection = owner(&plane);
        let mut create = |command: &str, project: &str, root: &str, create: bool| {
            let reply = send(
                &plane,
                &mut connection,
                json!({"_tag":"Request","id":command,"tag":"projects.mutate","payload":{
                    "type":"project.create","commandId":command,"projectId":project,
                    "title":"Sandbox","workspaceRoot":root,"createWorkspaceRootIfMissing":create},"headers":[]}),
            );
            reply[0]["exit"].clone()
        };
        // A missing folder is refused unless the caller asks for it.
        let refused = create("c1", "p1", "/workspace/sandbox", false);
        assert_eq!(refused["_tag"], "Failure", "{refused}");
        assert!(plane.store_now().project_rows.is_empty());
        let created = create("c2", "p1", "/workspace/sandbox", true);
        assert_eq!(created["_tag"], "Success", "{created}");
        assert_eq!(created["value"]["workspaceRoot"], "/workspace/sandbox");
        assert!(now(plane
            .workspace()
            .is_dir(std::path::Path::new("/workspace/sandbox"))));
        // Another project cannot take the same folder.
        let taken = create("c3", "p2", "/workspace/sandbox", true);
        assert_eq!(taken["_tag"], "Failure", "{taken}");
    }

    #[test]
    fn lists_codex_models_from_the_workspace_and_keeps_them() {
        let documents = Arc::new(MemoryStore::new());
        let runtime = Arc::new(ManualRuntime::default());
        let open = || {
            ControlPlane::open(
                config(),
                documents.clone(),
                Box::new(MemoryLog::default()),
                host(&runtime),
            )
            .unwrap()
        };
        let codex_models = |plane: &ControlPlane| -> Vec<String> {
            plane.config_snapshot(&Value::Null, false).unwrap()["providers"]
                .as_array()
                .unwrap()
                .iter()
                .find(|p| p["instanceId"] == "codex")
                .unwrap()["models"]
                .as_array()
                .unwrap()
                .iter()
                .map(|m| m["slug"].as_str().unwrap().to_owned())
                .collect()
        };
        let plane = open();
        assert!(codex_models(&plane).is_empty());
        let agentd = Arc::new(FakeAgentd::default());
        agentd.set_reading(
            op::PROVIDER_MODELS,
            json!([{"model": "gpt-6-luna", "displayName": "gpt-6-luna", "isDefault": true,
                "supportedReasoningEfforts": [{"reasoningEffort": "high"}], "defaultReasoningEffort": "high"}]),
        );
        now(plane.workspace_connected(agentd.link(plane.workspace()))).unwrap();
        runtime.run();
        assert_eq!(codex_models(&plane), ["gpt-6-luna"]);
        let snapshot = plane.config_snapshot(&Value::Null, false).unwrap();
        let codex = snapshot["providers"]
            .as_array()
            .unwrap()
            .iter()
            .find(|p| p["instanceId"] == "codex")
            .unwrap();
        assert_eq!(codex["models"][0]["name"], "GPT-6-Luna");
        assert_eq!(
            codex["models"][0]["capabilities"]["optionDescriptors"][0]["currentValue"],
            "high"
        );
        drop(plane);
        // A restarted plane has them before the workspace connects again.
        assert_eq!(codex_models(&open()), ["gpt-6-luna"]);
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
        // A rename arrives as the reference's domain event, not a snapshot.
        let event = &pushed[0]["values"][0];
        assert_eq!(event["kind"], "event", "{event:#}");
        assert_eq!(event["event"]["type"], "thread.metadata-updated");
        assert_eq!(event["event"]["payload"]["title"], "Two");
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
        // The probe answers the server's time (#41037).
        assert_eq!(probe.len(), 1);
        assert_eq!(probe[0]["exit"]["_tag"], "Success");
        assert!(probe[0]["exit"]["value"]["serverTime"]
            .as_str()
            .is_some_and(|time| time.ends_with('Z')));
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
        assert_eq!(thread[0].1["values"][0]["event"]["payload"]["title"], "Two");

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

    /// One HTTP request to `plane`'s router, as `env.example.test` over https.
    fn http(
        plane: &Arc<ControlPlane>,
        method: &str,
        path: &str,
        headers: &[(&str, &str)],
        body: String,
    ) -> (u16, HeaderMap, Value) {
        use tower::ServiceExt;
        let mut request = axum::http::Request::builder()
            .method(method)
            .uri(path)
            .header("host", "env.example.test")
            .header("x-forwarded-proto", "https");
        for (name, value) in headers {
            request = request.header(*name, *value);
        }
        let response = now(plane
            .router()
            .oneshot(request.body(axum::body::Body::from(body)).unwrap()))
        .unwrap();
        let status = response.status().as_u16();
        let headers = response.headers().clone();
        let bytes = now(axum::body::to_bytes(response.into_body(), usize::MAX)).unwrap();
        (
            status,
            headers,
            serde_json::from_slice(&bytes).unwrap_or(Value::Null),
        )
    }

    fn enable_linked_environments(documents: &MemoryStore) {
        let dir = std::path::Path::new(STATE_DIR);
        documents.create_dir_all(dir).unwrap();
        documents
            .write(
                &dir.join("settings.json"),
                br#"{"linkedEnvironmentsEnabled":true}"#,
                crate::persistence::WriteMode::Replace,
            )
            .unwrap();
    }

    /// The sign-in another server runs to link to this one: register as a
    /// peer link, approve with a pairing code, exchange the code. Returns
    /// the access token. The plane is reopened from storage between steps,
    /// as a host that suspends between requests does.
    fn peer_link_sign_in(
        documents: &Arc<MemoryStore>,
        access: &str,
    ) -> (Arc<ControlPlane>, String) {
        use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
        use sha2::Digest;
        let open = || {
            ControlPlane::open(
                config(),
                documents.clone(),
                Box::new(MemoryLog::default()),
                host(&Arc::default()),
            )
            .unwrap()
        };
        let json = [("content-type", "application/json")];
        let redirect = "http://127.0.0.1/t3code/peer-link/callback";
        let plane = open();
        let (status, _, client) = http(
            &plane,
            "POST",
            "/oauth/mcp/register",
            &json,
            json!({"client_name":"T3 Code · laptop","redirect_uris":[redirect],"software_id":"t3code-peer-link"}).to_string(),
        );
        assert_eq!(status, 201, "{client}");
        let (_, owner) = plane.auth.browser_session("secret", None).unwrap();
        let owner = plane.auth.principal(&owner).unwrap();
        let code = plane
            .auth
            .pair(&owner, &json!({"purpose":"peer-link","access":access}))
            .unwrap()["credential"]
            .as_str()
            .unwrap()
            .to_owned();
        let verifier = "v".repeat(64);
        let authorization = json!({
            "response_type":"code","client_id":client["client_id"],"redirect_uri":redirect,
            "code_challenge":URL_SAFE_NO_PAD.encode(sha2::Sha256::digest(verifier.as_bytes())),
            "code_challenge_method":"S256","state":"s1","resource":"https://env.example.test/mcp"
        });
        let plane = open();
        let (status, _, decided) = http(
            &plane,
            "POST",
            "/oauth/mcp/decision",
            &json,
            json!({"authorization":authorization,"decision":{"_tag":"pairing-code","access":access,"code":code}}).to_string(),
        );
        assert_eq!(status, 200, "{decided}");
        let redirect_to = url::Url::parse(decided["redirectTo"].as_str().unwrap()).unwrap();
        let query: HashMap<_, _> = redirect_to.query_pairs().into_owned().collect();
        assert_eq!(query["state"], "s1");
        assert_eq!(query["iss"], "https://env.example.test");
        let form = url::form_urlencoded::Serializer::new(String::new())
            .extend_pairs([
                ("grant_type", "authorization_code"),
                ("code", query["code"].as_str()),
                ("redirect_uri", redirect),
                ("client_id", client["client_id"].as_str().unwrap()),
                ("code_verifier", verifier.as_str()),
                ("resource", "https://env.example.test/mcp"),
            ])
            .finish();
        let plane = open();
        let form_type = [("content-type", "application/x-www-form-urlencoded")];
        let (status, _, token) = http(&plane, "POST", "/oauth/mcp/token", &form_type, form.clone());
        assert_eq!(status, 200, "{token}");
        assert_eq!(token["token_type"], "Bearer");
        // A code is spent once.
        let (status, _, again) = http(&plane, "POST", "/oauth/mcp/token", &form_type, form);
        assert_eq!((status, &again["error"]), (400, &json!("invalid_grant")));
        (plane, token["access_token"].as_str().unwrap().to_owned())
    }

    #[test]
    fn a_peer_link_is_refused_while_linked_environments_are_off() {
        let plane = plane();
        let (status, _, refused) = http(
            &plane,
            "POST",
            "/oauth/mcp/register",
            &[("content-type", "application/json")],
            json!({"redirect_uris":["http://127.0.0.1/cb"],"software_id":"t3code-peer-link"})
                .to_string(),
        );
        assert_eq!(status, 400);
        assert_eq!(refused["error"], "invalid_client_metadata");
        let (_, _, metadata) = http(
            &plane,
            "GET",
            "/.well-known/oauth-authorization-server",
            &[],
            String::new(),
        );
        assert_eq!(
            metadata["registration_endpoint"],
            "https://env.example.test/oauth/mcp/register"
        );
    }

    #[test]
    fn a_peer_link_signs_in_and_calls_mcp_across_restarts() {
        let documents = Arc::new(MemoryStore::new());
        enable_linked_environments(&documents);
        let (plane, token) = peer_link_sign_in(&documents, "full-access");
        let bearer = format!("Bearer {token}");
        let mcp = |plane: &Arc<ControlPlane>, session: Option<&str>, body: Value| {
            let mut headers = vec![
                ("authorization", bearer.as_str()),
                ("content-type", "application/json"),
                ("accept", "application/json, text/event-stream"),
            ];
            if let Some(session) = session {
                headers.push(("mcp-session-id", session));
            }
            http(plane, "POST", "/mcp", &headers, body.to_string())
        };
        let (status, headers, initialized) = mcp(
            &plane,
            None,
            json!({"jsonrpc":"2.0","id":0,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"t3code-peer","version":"1"}}}),
        );
        assert_eq!(status, 200, "{initialized}");
        assert_eq!(initialized["result"]["serverInfo"]["name"], "T3 Code");
        let session = headers["mcp-session-id"].to_str().unwrap().to_owned();

        // The session and token outlive the process.
        let plane = ControlPlane::open(
            config(),
            documents.clone(),
            Box::new(MemoryLog::default()),
            host(&Arc::default()),
        )
        .unwrap();
        plane
            .commit(Event::ProjectCreated(crate::Project {
                id: "p1".into(),
                name: "One".into(),
                path: "/workspace/one".into(),
                created_at: crate::now(),
            }))
            .unwrap();
        let (status, _, listed) = mcp(
            &plane,
            Some(&session),
            json!({"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"t3_project_list","arguments":{}}}),
        );
        assert_eq!(status, 200, "{listed}");
        let projects = &listed["result"]["structuredContent"]["projects"];
        assert_eq!(projects[0]["id"], "p1", "{listed}");

        // Another token's holder cannot use the session.
        let (status, _, _) = http(
            &plane,
            "POST",
            "/mcp",
            &[
                ("authorization", "Bearer someone-else"),
                ("mcp-session-id", &session),
            ],
            json!({"jsonrpc":"2.0","id":2,"method":"ping"}).to_string(),
        );
        assert_eq!(status, 401);
        let (status, _, _) = http(
            &plane,
            "DELETE",
            "/mcp",
            &[("authorization", &bearer), ("mcp-session-id", &session)],
            String::new(),
        );
        assert_eq!(status, 204);
        let (status, _, _) = mcp(
            &plane,
            Some(&session),
            json!({"jsonrpc":"2.0","id":3,"method":"ping"}),
        );
        assert_eq!(status, 404);
    }

    #[test]
    fn a_peer_link_launches_and_follows_a_thread_over_mcp() {
        let documents = Arc::new(MemoryStore::new());
        enable_linked_environments(&documents);
        let (_, token) = peer_link_sign_in(&documents, "auto");
        let runtime = Arc::new(ManualRuntime::default());
        let plane = ControlPlane::open(
            config(),
            documents.clone(),
            Box::new(MemoryLog::default()),
            host(&runtime),
        )
        .unwrap();
        let agentd = agentd_with_project_folder();
        now(plane.workspace_connected(agentd.link(plane.workspace()))).unwrap();
        plane
            .commit(Event::ProjectCreated(crate::Project {
                id: "p1".into(),
                name: "One".into(),
                path: "/workspace/one".into(),
                created_at: crate::now(),
            }))
            .unwrap();
        let bearer = format!("Bearer {token}");
        let rpc = |session: Option<&str>, limit: Option<&str>, method: &str, params: Value| {
            let mut headers = vec![
                ("authorization", bearer.as_str()),
                ("content-type", "application/json"),
            ];
            if let Some(session) = session {
                headers.push(("mcp-session-id", session));
            }
            if let Some(limit) = limit {
                headers.push(("t3-mode-limit", limit));
            }
            let body = json!({"jsonrpc":"2.0","id":1,"method":method,"params":params});
            http(&plane, "POST", "/mcp", &headers, body.to_string())
        };
        let (_, headers, _) = rpc(
            None,
            None,
            "initialize",
            json!({"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"t3code-peer","version":"1"}}),
        );
        let session = headers["mcp-session-id"].to_str().unwrap().to_owned();
        let tool = |limit: Option<&str>, name: &str, arguments: Value| -> Value {
            let (status, _, reply) = rpc(
                Some(&session),
                limit,
                "tools/call",
                json!({"name":name,"arguments":arguments}),
            );
            assert_eq!(status, 200, "{reply}");
            let result = &reply["result"];
            if result["isError"] == true {
                return serde_json::from_str(result["content"][0]["text"].as_str().unwrap())
                    .unwrap();
            }
            result["structuredContent"].clone()
        };

        let (_, _, listed) = rpc(Some(&session), None, "tools/list", json!({}));
        let names: Vec<&str> = listed["result"]["tools"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|t| t["name"].as_str())
            .collect();
        for name in [
            "orchestrator_capabilities",
            "t3_thread_launch",
            "t3_thread_read",
            "t3_thread_wait",
            "t3_thread_send",
            "t3_project_list",
        ] {
            assert!(names.contains(&name), "{name} in {names:?}");
        }
        let capabilities = tool(None, "orchestrator_capabilities", json!({}));
        assert_eq!(capabilities["runtimeMode"], "auto");
        assert!(capabilities["providers"]
            .as_array()
            .unwrap()
            .iter()
            .any(|p| p["providerInstanceId"] == "codex"));

        // The link was approved for auto; creating a project needs full access.
        let refused = tool(None, "t3_project_create", json!({"title":"From title"}));
        assert_eq!(refused["code"], "capability_denied", "{refused}");

        // Over a full-access link, a project with no folder starts from its
        // title in a new repository.
        let (_, full) = peer_link_sign_in(&documents, "full-access");
        let full = format!("Bearer {full}");
        let full_rpc = |session: Option<&str>, method: &str, params: Value| {
            let mut headers = vec![
                ("authorization", full.as_str()),
                ("content-type", "application/json"),
            ];
            if let Some(session) = session {
                headers.push(("mcp-session-id", session));
            }
            let body = json!({"jsonrpc":"2.0","id":1,"method":method,"params":params});
            http(&plane, "POST", "/mcp", &headers, body.to_string())
        };
        let (_, headers, _) = full_rpc(
            None,
            "initialize",
            json!({"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"t3code-peer","version":"1"}}),
        );
        let full_session = headers["mcp-session-id"].to_str().unwrap().to_owned();
        let full_tool = |name: &str, arguments: Value| -> Value {
            let (_, _, reply) = full_rpc(
                Some(&full_session),
                "tools/call",
                json!({"name":name,"arguments":arguments}),
            );
            let result = &reply["result"];
            if result["isError"] == true {
                return serde_json::from_str(result["content"][0]["text"].as_str().unwrap())
                    .unwrap();
            }
            result["structuredContent"].clone()
        };
        let named = full_tool("t3_project_create", json!({"title":"From title"}));
        assert_eq!(
            named["workspaceRoot"], "/workspace/projects/from-title",
            "{named}"
        );
        let refused = full_tool(
            "t3_project_create",
            json!({"title":"From title","createWorkspaceRootIfMissing":true}),
        );
        assert_eq!(refused["code"], "invalid_request", "{refused}");

        // The link was approved for auto, so full access is refused.
        let refused = tool(
            None,
            "t3_thread_launch",
            json!({"projectId":"p1","title":"Too broad","runtimeMode":"full-access",
                "modelSelection":{"instanceId":"codex","model":"fixture"}}),
        );
        assert_eq!(
            refused["code"], "runtime_mode_escalation_denied",
            "{refused}"
        );

        let launched = tool(
            None,
            "t3_thread_launch",
            json!({"projectId":"p1","title":"Remote work","message":"hello",
                "modelSelection":{"instanceId":"codex","model":"fixture"},"clientRequestId":"k1"}),
        );
        let thread = launched["threadId"].as_str().expect("a thread").to_owned();
        assert_eq!(launched["projectId"], "p1");
        assert!(launched["runId"].is_string(), "{launched}");
        assert_eq!(
            launched["link"],
            format!("[Remote work](t3-thread://v2/env-test/{thread})")
        );
        // A retry with the same key reports the same launch.
        let again = tool(
            None,
            "t3_thread_launch",
            json!({"projectId":"p1","title":"Remote work","message":"hello",
                "modelSelection":{"instanceId":"codex","model":"fixture"},"clientRequestId":"k1"}),
        );
        assert_eq!(again["runId"], launched["runId"]);

        runtime.run();
        let turns = agentd.turns();
        assert_eq!(turns.len(), 1, "one turn, the retry started none");
        let (&stream, start) = turns.iter().next().unwrap();
        assert_eq!(start.prompt, "hello");
        assert_eq!(start.cwd, "/workspace/one");
        let waiting = tool(
            None,
            "t3_thread_wait",
            json!({"threadId":thread,"timeoutMs":1}),
        );
        assert_eq!(waiting["timedOut"], true, "{waiting}");
        agentd.emit(
            stream,
            WireTurnEvent::Exited {
                success: true,
                cancelled: false,
                status: "exit status: 0".into(),
            },
        );
        runtime.run();
        let waited = tool(None, "t3_thread_wait", json!({"threadId":thread}));
        assert_eq!(
            (waited["status"].as_str(), waited["timedOut"].as_bool()),
            (Some("completed"), Some(false)),
            "{waited}"
        );
        assert_eq!(waited["runId"], launched["runId"]);

        let read = tool(None, "t3_thread_read", json!({"threadId":thread}));
        assert_eq!(read["thread"]["runtimeMode"], "auto");
        assert_eq!(read["thread"]["creationSource"], "mcp");
        assert_eq!(read["thread"]["status"], "completed");
        assert_eq!(read["recentRuns"][0]["status"], "completed");
        assert_eq!(read["items"][0]["type"], "user_message", "{read}");
        assert_eq!(read["items"][0]["text"], "hello");

        // The caller's own modes narrow what it may do.
        let narrowed = tool(
            Some("approval-required/default"),
            "t3_thread_send",
            json!({"threadId":thread,"message":"more"}),
        );
        assert_eq!(narrowed["code"], "runtime_mode_escalation_denied");
        let sent = tool(
            None,
            "t3_thread_send",
            json!({"threadId":thread,"message":"more"}),
        );
        assert_eq!(sent["delivery"], "started", "{sent}");
        runtime.run();
        assert_eq!(agentd.turns().len(), 2);
        // A linked caller's cursorless read starts at the newest message and
        // pages back with `beforePosition`; `afterPosition` reads forward.
        let text = |read: &Value| -> Vec<String> {
            read["items"]
                .as_array()
                .unwrap()
                .iter()
                .map(|item| item["text"].as_str().unwrap_or_default().to_owned())
                .collect()
        };
        let newest = tool(None, "t3_thread_read", json!({"threadId":thread,"limit":1}));
        assert_eq!(text(&newest), ["more"], "{newest}");
        assert_eq!(newest["hasMore"], false);
        let earlier = newest["earlierPosition"].clone();
        assert!(earlier.is_number(), "{newest}");
        let older = tool(
            None,
            "t3_thread_read",
            json!({"threadId":thread,"limit":1,"beforePosition":earlier}),
        );
        assert_eq!(text(&older), ["hello"], "{older}");
        assert_eq!(older["hasMore"], true);
        assert_eq!(older["earlierPosition"], Value::Null);
        let forward = tool(
            None,
            "t3_thread_read",
            json!({"threadId":thread,"limit":1,"afterPosition":-1}),
        );
        assert_eq!(text(&forward), ["hello"], "{forward}");
        assert_eq!(forward["hasMore"], true);
        let interrupted = tool(None, "t3_thread_interrupt", json!({"threadId":thread}));
        assert_eq!(
            interrupted["status"], "interrupt_requested",
            "{interrupted}"
        );
        assert_eq!(interrupted["runId"], sent["runId"]);

        let listed = tool(None, "t3_thread_list", json!({"allProjects":true}));
        assert_eq!(listed["total"], 1);
        assert_eq!(listed["threads"][0]["threadId"], thread.as_str());
        let environment = tool(None, "t3_environment_read", json!({}));
        assert_eq!(environment["environmentId"], "env-test");
    }

    #[test]
    fn a_peer_link_needs_a_plain_pairing_code() {
        use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
        let documents = Arc::new(MemoryStore::new());
        enable_linked_environments(&documents);
        let plane = ControlPlane::open(
            config(),
            documents,
            Box::new(MemoryLog::default()),
            host(&Arc::default()),
        )
        .unwrap();
        let json = [("content-type", "application/json")];
        let (_, _, client) = http(
            &plane,
            "POST",
            "/oauth/mcp/register",
            &json,
            json!({"redirect_uris":["http://127.0.0.1/cb"],"software_id":"t3code-peer-link"})
                .to_string(),
        );
        let (_, owner) = plane.auth.browser_session("secret", None).unwrap();
        let owner = plane.auth.principal(&owner).unwrap();
        // A code made for an app's sign-in, not for linking.
        let app_code = plane
            .auth
            .pair(&owner, &json!({"access":"read-only"}))
            .unwrap();
        let authorization = json!({"response_type":"code","client_id":client["client_id"],
            "redirect_uri":"http://127.0.0.1/cb","code_challenge":URL_SAFE_NO_PAD.encode([7u8; 32]),
            "code_challenge_method":"S256","state":"s"});
        let (status, _, refused) = http(
            &plane,
            "POST",
            "/oauth/mcp/decision",
            &json,
            json!({"authorization":authorization,"decision":{"_tag":"pairing-code","access":"read-only","code":app_code["credential"]}}).to_string(),
        );
        assert_eq!(status, 400);
        assert_eq!(refused["_tag"], "AuthMcpApprovalError");
    }

    #[test]
    fn the_owner_creates_and_revokes_pairing_links() {
        use tower::ServiceExt;
        let plane = plane();
        let (_, owner) = plane.auth.browser_session("secret", None).unwrap();
        let post = |path: &str, cookie: Option<&str>, body: Value| {
            let mut request = axum::http::Request::post(path)
                .header("host", "env.example.test")
                .header("content-type", "application/json");
            if let Some(token) = cookie {
                request = request.header("cookie", format!("t3_session_3773={token}"));
            }
            let response = now(plane.router().oneshot(
                request
                    .body(axum::body::Body::from(body.to_string()))
                    .unwrap(),
            ))
            .unwrap();
            let status = response.status();
            let bytes = now(axum::body::to_bytes(response.into_body(), usize::MAX)).unwrap();
            (
                status,
                serde_json::from_slice::<Value>(&bytes).unwrap_or(Value::Null),
            )
        };
        let request = json!({"purpose":"peer-link","access":"full-access"});
        assert_eq!(
            post("/api/auth/pairing-token", None, request.clone()).0,
            401
        );
        let (status, link) = post("/api/auth/pairing-token", Some(&owner), request);
        assert_eq!(status, 200, "{link}");
        assert_eq!(link["purpose"], "peer-link");
        assert_eq!(link["access"], "full-access");
        assert_eq!(link["credential"].as_str().map(str::len), Some(12));
        let (status, revoked) = post(
            "/api/auth/pairing-links/revoke",
            Some(&owner),
            json!({"id":link["id"]}),
        );
        assert_eq!((status.as_u16(), revoked), (200, json!({"revoked":true})));
        assert_eq!(
            plane.auth.consume_approval_code(
                link["credential"].as_str().unwrap(),
                "full-access",
                true
            ),
            Err("unknown_or_used")
        );
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
        dispatch_nth_message(plane, connection, 1, text)
    }

    fn dispatch_nth_message(
        plane: &Arc<ControlPlane>,
        connection: &mut Connection,
        n: u32,
        text: &str,
    ) {
        let sent = send(
            plane,
            connection,
            json!({"_tag":"Request","id":format!("m{n}"),"tag":"orchestration.dispatchCommand","payload":{
                "type":"message.dispatch","commandId":format!("send-{n}"),"threadId":"t1","messageId":format!("message:{n}"),"text":text,
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
        let agentd = agentd_with_project_folder();
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
        // The thread's instance is the built-in Codex one.
        assert!(start.command.is_empty());
        let provider = start.provider.as_ref().expect("a provider turn");
        assert_eq!(provider.driver, "codex");
        assert!(
            provider.mcp.is_none(),
            "no reachable endpoint was configured"
        );
        assert_eq!(provider.resume, None);
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
        let agentd = agentd_with_project_folder();
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

    #[test]
    fn a_turn_stopped_before_it_starts_launches_nothing_and_ends_interrupted() {
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
        // The stop lands before the run's launch task has run.
        let stopped = send(
            &plane,
            &mut connection,
            json!({"_tag":"Request","id":"x","tag":"orchestration.dispatchCommand","payload":{
                "type":"thread.stop","commandId":"c4","threadId":"t1","createdBy":"user","creationSource":"web"
            },"headers":[]}),
        );
        assert_eq!(stopped[0]["exit"]["_tag"], "Success", "{stopped:?}");
        runtime.run();
        let runs = thread_runs(&plane);
        // No workspace is connected, so a launch would have failed the run.
        assert_eq!(runs[0].status, "interrupted");
        assert_eq!(runs[0].error, None);
    }

    #[test]
    fn a_restart_offers_a_delegated_delivery_once_and_accepts_it_with_its_message() {
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
        let run_id = thread_runs(&plane)[0].wire_id();
        // A finished task whose delivery was claimed but never sent, as a
        // stop between the claim and its message leaves it.
        let host = Arc::new(PlaneCommands {
            plane: plane.clone(),
        });
        let mut extras = json!({});
        extras[run_id.as_str()]["delegatedCompletion"] = json!({
            "disposition": "open", "nextGeneration": 2,
            "delivery": {"generation": 1, "messageId": "message:delivery", "taskIds": ["node:task"]},
        });
        now(host.commit(Event::ThreadPatched {
            thread_id: "t1".into(),
            patch: json!({"row": {
                "subagents": [{"id": "node:task", "origin": "app_owned", "threadId": "t1", "runId": run_id,
                    "childThreadId": "t2", "title": "Greeter", "status": "completed", "result": "Done.",
                    "completionDelivery": {"state": "claimed", "observedByRunId": null}}],
                "runExtras": extras,
            }}),
        }))
        .unwrap();
        let delivered = |plane: &ControlPlane| {
            plane.store_now().threads["t1"]
                .messages
                .iter()
                .filter(|m| m.id == "message:delivery")
                .count()
        };
        now(crate::orchestration::recover_delegated_tasks(
            host.clone(),
            &Default::default(),
        ));
        runtime.run();
        assert_eq!(delivered(&plane), 1, "the delivery is sent");
        let row = plane.store_now().threads["t1"].stored_row();
        assert_eq!(
            row["subagents"][0]["completionDelivery"]["state"],
            "delivered"
        );
        assert!(row["runExtras"][run_id.as_str()]["delegatedCompletion"]["delivery"].is_null());
        // Recovering again finds nothing owed.
        now(crate::orchestration::recover_delegated_tasks(
            host,
            &Default::default(),
        ));
        runtime.run();
        assert_eq!(delivered(&plane), 1, "the delivery is sent once");
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
        let agentd = agentd_with_project_folder();
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
        let agentd = agentd_with_project_folder();
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

    /// The host's alarm: the plane's upkeep, then what changed for `connection`.
    fn alarm(plane: &Arc<ControlPlane>, connection: &Connection) -> Vec<Value> {
        now(plane.on_alarm());
        let signals = plane.take_signals();
        parse(now(plane.deliver(connection, &signals)))
    }

    fn values_for(frames: &[Value], id: &str) -> Vec<Value> {
        frames
            .iter()
            .filter(|frame| frame["requestId"] == id)
            .flat_map(|frame| frame["values"].as_array().cloned().unwrap_or_default())
            .collect()
    }

    fn subscribe(
        plane: &Arc<ControlPlane>,
        connection: &mut Connection,
        id: &str,
        tag: &str,
        payload: Value,
    ) -> Vec<Value> {
        let frames = send(
            plane,
            connection,
            json!({"_tag":"Request","id":id,"tag":tag,"payload":payload,"headers":[]}),
        );
        values_for(&frames, id)
    }

    #[test]
    fn workspace_event_streams_resume_after_a_restart_without_repeats() {
        let agentd = agentd_with_project_folder();
        let mut host = Restartable::new(&agentd);
        let mut connection = owner(&host.plane);
        let device = subscribe(
            &host.plane,
            &mut connection,
            "d",
            "subscribeDeviceState",
            json!({}),
        );
        assert_eq!(device[0]["hostStatus"], "disabled", "{device:?}");
        let preview = subscribe(
            &host.plane,
            &mut connection,
            "p",
            "subscribePreviewEvents",
            json!({}),
        );
        assert!(preview.is_empty());
        host.plane.take_signals();

        agentd.emit_bus(op::DEVICE_EVENTS, json!({"revision": 1}));
        agentd.emit_bus(op::PREVIEW_EVENTS, json!({"type": "opened"}));
        let sent = alarm(&host.plane, &connection);
        assert_eq!(values_for(&sent, "d"), vec![json!({"revision": 1})]);
        assert_eq!(values_for(&sent, "p"), vec![json!({"type": "opened"})]);
        assert!(alarm(&host.plane, &connection).is_empty(), "nothing twice");

        // The connection outlives the plane, as a socket's stored state does.
        let stored = serde_json::to_string(&connection).unwrap();
        host.restart(&agentd);
        let connection: Connection = serde_json::from_str(&stored).unwrap();
        host.plane.take_signals();
        // The new workspace connection reports the same device state again.
        agentd.emit_bus(op::DEVICE_EVENTS, json!({"revision": 1}));
        agentd.emit_bus(op::PREVIEW_EVENTS, json!({"type": "closed"}));
        let sent = alarm(&host.plane, &connection);
        assert!(values_for(&sent, "d").is_empty(), "{sent:?}");
        assert_eq!(values_for(&sent, "p"), vec![json!({"type": "closed"})]);
        agentd.emit_bus(op::DEVICE_EVENTS, json!({"revision": 2}));
        let sent = alarm(&host.plane, &connection);
        assert_eq!(values_for(&sent, "d"), vec![json!({"revision": 2})]);
    }

    fn clone_start(id: &str, project: &str, destination: &str) -> Value {
        json!({"_tag":"Request","id":id,"tag":"projectClone.start","payload":{
            "projectId": project, "title": "example-repo",
            "createdAt": "2026-11-23T00:00:00.000Z",
            "remoteUrl": "git@ssh.bitbucket.org:example-org/example-repo.git",
            "destinationPath": destination},"headers":[]})
    }

    #[test]
    fn project_clones_start_in_the_workspace_and_stream_their_progress() {
        let agentd = agentd_with_project_folder();
        let host = Restartable::new(&agentd);
        let mut connection = owner(&host.plane);
        let first = subscribe(
            &host.plane,
            &mut connection,
            "c",
            "subscribeProjectClones",
            json!({}),
        );
        assert_eq!(first, vec![json!([])]);
        host.plane.take_signals();

        let reply = send(
            &host.plane,
            &mut connection,
            clone_start("s", "p-clone", "/workspace/projects/vgd"),
        );
        let exit = &reply[0]["exit"];
        assert_eq!(exit["_tag"], "Success", "{reply:?}");
        assert_eq!(exit["value"]["projectId"], "p-clone");
        assert_eq!(exit["value"]["cwd"], "/workspace/projects/vgd");
        let methods: Vec<String> = agentd.clone_calls().into_iter().map(|(m, _)| m).collect();
        assert_eq!(methods, ["claim", "launch"]);
        // The project exists, on the clone's folder, before git runs.
        let store = host.plane.store_now();
        assert_eq!(
            store.project_rows["p-clone"]["workspaceRoot"],
            "/workspace/projects/vgd"
        );

        let running = json!([{"projectId": "p-clone", "phase": "running", "stage": "receiving"}]);
        agentd.emit_bus(op::CLONE_EVENTS, running.clone());
        let sent = alarm(&host.plane, &connection);
        assert_eq!(values_for(&sent, "c"), vec![running.clone()]);
        agentd.emit_bus(op::CLONE_EVENTS, running);
        assert!(
            values_for(&alarm(&host.plane, &connection), "c").is_empty(),
            "nothing twice"
        );

        let cancel = send(
            &host.plane,
            &mut connection,
            json!({"_tag":"Request","id":"x","tag":"projectClone.cancel","payload":{"projectId":"p-clone"},"headers":[]}),
        );
        assert_eq!(
            cancel[0]["exit"]["value"],
            json!({"applied": true}),
            "{cancel:?}"
        );
    }

    #[test]
    fn a_refused_project_clone_says_why_and_creates_no_project() {
        let agentd = agentd_with_project_folder();
        agentd.fail_clone_call(
            "claim",
            json!({"_tag": "Die", "defect": "The workspace could not run projectClone.claim: /home/agent/vgd is outside the directories this workspace serves"}),
        );
        let host = Restartable::new(&agentd);
        let mut connection = owner(&host.plane);
        let reply = send(
            &host.plane,
            &mut connection,
            clone_start("s", "p-clone", "~/vgd"),
        );
        let cause = &reply[0]["exit"]["cause"][0];
        // A typed error T3 Code shows, never a bare defect.
        assert_eq!(cause["_tag"], "Fail", "{reply:?}");
        assert_eq!(cause["error"]["_tag"], "SourceControlRepositoryError");
        assert_eq!(
            cause["error"]["detail"],
            "/home/agent/vgd is outside the directories this workspace serves"
        );
        assert!(host.plane.store_now().project_rows.is_empty());
    }

    #[test]
    fn a_project_clone_into_a_registered_empty_folder_clones_for_that_project() {
        let agentd = agentd_with_project_folder();
        let host = Restartable::new(&agentd);
        let mut connection = owner(&host.plane);
        let created = send(
            &host.plane,
            &mut connection,
            json!({"_tag":"Request","id":"m","tag":"projects.mutate","payload":{"type":"project.create","commandId":"c1","projectId":"p-old","title":"Old","workspaceRoot":"/workspace/one"},"headers":[]}),
        );
        assert_eq!(created[0]["exit"]["_tag"], "Success", "{created:?}");
        let reply = send(
            &host.plane,
            &mut connection,
            clone_start("s", "p-new", "/workspace/one"),
        );
        assert_eq!(reply[0]["exit"]["value"]["projectId"], "p-old", "{reply:?}");
        let calls = agentd.clone_calls();
        let methods: Vec<&str> = calls.iter().map(|(m, _)| m.as_str()).collect();
        assert_eq!(methods, ["claim", "adopt", "launch"]);
        assert_eq!(calls[1].1, json!({"from": "p-new", "to": "p-old"}));
        assert_eq!(calls[2].1, json!({"projectId": "p-old"}));
    }

    #[test]
    fn clone_failures_are_errors_clients_can_show() {
        let detail = |reply: Reply| match reply {
            Reply::Failure(f) => f["error"]["detail"].as_str().unwrap_or_default().to_owned(),
            other => panic!("{other:?}"),
        };
        assert!(detail(clone_failure(Value::Null)).contains("older than projectClone"));
        assert!(detail(clone_failure(json!({"_tag": "Die",
            "defect": "The workspace could not run projectClone.claim: unknown op: clone.call"})))
        .contains("older than projectClone"));
        let typed = json!({"_tag": "SourceControlRepositoryError", "provider": "unknown",
            "operation": "cloneRepository", "detail": "Destination path is not empty."});
        assert_eq!(
            detail(clone_failure(typed.clone())),
            "Destination path is not empty."
        );
    }

    #[test]
    fn vcs_status_streams_report_only_changes_across_a_restart() {
        let agentd = agentd_with_project_folder();
        let status = |branch: &str| json!({"isRepo": true, "branch": branch, "hasUpstream": false});
        agentd.set_reading(op::WATCH_VCS_STATUS, status("main"));
        let mut host = Restartable::new(&agentd);
        let mut connection = owner(&host.plane);
        let first = subscribe(
            &host.plane,
            &mut connection,
            "v",
            "subscribeVcsStatus",
            json!({"cwd": "/workspace/one"}),
        );
        assert_eq!(first[0]["local"]["branch"], "main", "{first:?}");
        assert_eq!(first[0]["remote"], json!({"hasUpstream":false}));
        assert_eq!(first.len(), 1);
        host.plane.take_signals();
        assert!(alarm(&host.plane, &connection).is_empty());

        agentd.set_reading(op::WATCH_VCS_STATUS, status("dev"));
        let sent = values_for(&alarm(&host.plane, &connection), "v");
        assert_eq!(sent.len(), 1, "{sent:?}");
        assert_eq!(sent[0]["_tag"], "localUpdated");
        assert_eq!(sent[0]["local"]["branch"], "dev");

        let stored = serde_json::to_string(&connection).unwrap();
        host.restart(&agentd);
        let connection: Connection = serde_json::from_str(&stored).unwrap();
        host.plane.take_signals();
        assert!(
            values_for(&alarm(&host.plane, &connection), "v").is_empty(),
            "the restarted plane reads the same status and sends nothing"
        );
        agentd.set_reading(op::WATCH_VCS_STATUS, status("topic"));
        let sent = values_for(&alarm(&host.plane, &connection), "v");
        assert_eq!(sent[0]["local"]["branch"], "topic", "{sent:?}");
    }

    #[test]
    fn terminal_streams_catch_up_after_a_restart() {
        let agentd = agentd_with_project_folder();
        let snapshot = |sequence: u64, text: &str| json!({"type": "snapshot", "snapshot": {"threadId": "t1", "terminalId": "a", "sequence": sequence, "history": text}});
        let output = |sequence: u64, data: &str| json!({"type": "output", "threadId": "t1", "terminalId": "a", "sequence": sequence, "data": data});
        agentd.set_reading(op::TERM_CALL, snapshot(1, "$ "));
        let mut host = Restartable::new(&agentd);
        let mut connection = owner(&host.plane);
        let first = subscribe(
            &host.plane,
            &mut connection,
            "t",
            "terminal.observe",
            json!({"threadId": "t1", "terminalId": "a"}),
        );
        assert_eq!(first, vec![snapshot(1, "$ ")]);
        host.plane.take_signals();

        agentd.emit_bus(op::TERM_EVENTS, output(2, "ls\n"));
        // Another terminal's output is not this stream's.
        agentd.emit_bus(
            op::TERM_EVENTS,
            json!({"type": "output", "threadId": "t1", "terminalId": "b", "sequence": 9, "data": "x"}),
        );
        let sent = values_for(&alarm(&host.plane, &connection), "t");
        assert_eq!(sent, vec![output(2, "ls\n")]);

        let stored = serde_json::to_string(&connection).unwrap();
        host.restart(&agentd);
        let connection: Connection = serde_json::from_str(&stored).unwrap();
        host.plane.take_signals();
        // Output came while no server was connected; the first frame of the
        // new stream makes the record read the terminal again.
        agentd.set_reading(op::TERM_CALL, snapshot(4, "$ ls\nfile\n$ "));
        agentd.emit_bus(op::TERM_EVENTS, output(4, "$ "));
        let sent = values_for(&alarm(&host.plane, &connection), "t");
        assert_eq!(sent, vec![snapshot(4, "$ ls\nfile\n$ ")]);
        agentd.emit_bus(op::TERM_EVENTS, output(4, "$ "));
        assert!(values_for(&alarm(&host.plane, &connection), "t").is_empty());
        agentd.emit_bus(op::TERM_EVENTS, output(5, "pwd\n"));
        let sent = values_for(&alarm(&host.plane, &connection), "t");
        assert_eq!(sent, vec![output(5, "pwd\n")]);
    }

    #[test]
    fn the_next_turn_resumes_the_native_thread_the_last_one_reported() {
        use crate::workspace::provider::{ProviderEvent, ThreadDisposition, TurnStatus};
        let runtime = Arc::new(ManualRuntime::default());
        let plane = ControlPlane::open(
            config(),
            Arc::new(MemoryStore::new()),
            Box::new(MemoryLog::default()),
            host(&runtime),
        )
        .unwrap();
        let agentd = agentd_with_project_folder();
        now(plane.workspace_connected(agentd.link(plane.workspace()))).unwrap();
        let mut connection = owner(&plane);
        seed(&plane, &mut connection);
        let provider = |event| WireTurnEvent::Provider { event };
        for (n, terminal) in [(1, TurnStatus::Completed), (2, TurnStatus::Failed)] {
            dispatch_nth_message(&plane, &mut connection, n, "hello");
            runtime.run();
            let turns = agentd.turns();
            let (&stream, start) = turns.iter().last().unwrap();
            let resume = start.provider.as_ref().unwrap().resume.clone();
            assert_eq!(resume, (n == 2).then(|| "thr_1".to_string()));
            agentd.emit(
                stream,
                provider(ProviderEvent::ProviderThread {
                    driver: "codex".into(),
                    native_thread_id: "thr_1".into(),
                    resumed: n == 2,
                    model: None,

                    opencode2: None,
                    native_title: None,
                    native_updated_at: None,
                    context_usage: None,
                    session_started: None,
                }),
            );
            agentd.emit(
                stream,
                provider(ProviderEvent::TurnTerminal {
                    status: terminal,
                    failure: (terminal == TurnStatus::Failed).then(|| {
                        crate::workspace::provider::ProviderFailure::new("provider_error", "boom")
                    }),
                    thread_disposition: if n == 1 {
                        ThreadDisposition::Reusable
                    } else {
                        ThreadDisposition::Broken
                    },
                    closing: Vec::new(),
                    driver: None,
                }),
            );
            // The CLI's own exit status does not decide the run's.
            agentd.emit(
                stream,
                WireTurnEvent::Exited {
                    success: true,
                    cancelled: false,
                    status: "exit status: 0".into(),
                },
            );
            runtime.run();
        }
        let runs = thread_runs(&plane);
        assert_eq!(runs[0].status, "completed");
        assert_eq!(runs[1].status, "failed");
        assert_eq!(runs[1].error.as_deref(), Some("boom"));
        // The last turn left the native thread broken: the next starts over.
        dispatch_nth_message(&plane, &mut connection, 3, "again");
        runtime.run();
        let turns = agentd.turns();
        assert_eq!(
            turns
                .values()
                .last()
                .unwrap()
                .provider
                .as_ref()
                .unwrap()
                .resume,
            None
        );
        assert!(agentd.turns_acked());
    }

    #[test]
    fn provider_events_become_the_projection_a_client_renders() {
        use crate::workspace::provider::{
            ProviderEvent, ProviderFailure, ProviderItem, ThreadDisposition, TokenUsage, TurnStatus,
        };
        let runtime = Arc::new(ManualRuntime::default());
        let plane = ControlPlane::open(
            config(),
            Arc::new(MemoryStore::new()),
            Box::new(MemoryLog::default()),
            host(&runtime),
        )
        .unwrap();
        let agentd = agentd_with_project_folder();
        now(plane.workspace_connected(agentd.link(plane.workspace()))).unwrap();
        let mut connection = owner(&plane);
        seed(&plane, &mut connection);
        dispatch_message(&plane, &mut connection, "hello");
        runtime.run();
        let stream = *agentd.turns().keys().next().unwrap();
        let emit = |event| agentd.emit(stream, WireTurnEvent::Provider { event });
        emit(ProviderEvent::ProviderThread {
            driver: "codex".into(),
            native_thread_id: "thr_1".into(),
            resumed: false,
            model: None,

            opencode2: None,
            native_title: None,
            native_updated_at: None,
            context_usage: None,
            session_started: None,
        });
        emit(ProviderEvent::ProviderTurn {
            native_turn_id: Some("turn_1".into()),
            native_turn_ref: None,
            conversation_head: None,
            status: TurnStatus::Running,
            model: None,
            history_injected: false,
        });
        emit(ProviderEvent::TurnItem {
            item: ProviderItem::new("msg_1", "assistant_message", "running").with("text", ""),
        });
        emit(ProviderEvent::TurnItemDelta {
            held: false,
            item_id: "msg_1".into(),
            delta: "Hel".into(),
        });
        emit(ProviderEvent::TurnItemDelta {
            held: false,
            item_id: "msg_1".into(),
            delta: "lo".into(),
        });
        runtime.run();
        let projection = || {
            let reply = send(
                &plane,
                &mut owner(&plane),
                json!({"_tag":"Request","id":"g","tag":"orchestration.getThreadProjection","payload":{"threadId":"t1"},"headers":[]}),
            );
            reply[0]["exit"]["value"].clone()
        };
        let streaming = projection();
        let message = &streaming["messages"][1];
        assert_eq!(message["text"], "Hello", "{streaming:#}");
        assert_eq!(message["streaming"], true);
        assert_eq!(message["id"], "message:provider:codex:native-item:msg_1");

        emit(ProviderEvent::TurnItem {
            item: ProviderItem::new("call_1", "command_execution", "completed")
                .with("input", "ls")
                .with("output", "a\n")
                .with("exitCode", 0),
        });
        emit(ProviderEvent::Usage {
            usage: TokenUsage {
                used_tokens: 10,
                output_tokens: 3,
                ..Default::default()
            },
        });
        emit(ProviderEvent::TurnTerminal {
            status: TurnStatus::Failed,
            failure: Some(ProviderFailure::new("provider_error", "boom")),
            thread_disposition: ThreadDisposition::Reusable,
            driver: None,
            closing: Vec::new(),
        });
        agentd.emit(
            stream,
            WireTurnEvent::Exited {
                success: false,
                cancelled: false,
                status: "exit status: 1".into(),
            },
        );
        runtime.run();
        let ended = projection();
        let run = &ended["runs"][0];
        let run_id = run["id"].as_str().unwrap();
        assert_eq!(run["status"], "failed");
        assert_eq!(run["rootNodeId"], format!("node:run:{}:root", uri(run_id)));
        assert_eq!(
            ended["thread"]["activeProviderThreadId"],
            run["providerThreadId"]
        );
        let items: Vec<(u64, &str, &str)> = ended["turnItems"]
            .as_array()
            .unwrap()
            .iter()
            .map(|i| {
                (
                    i["ordinal"].as_u64().unwrap(),
                    i["type"].as_str().unwrap(),
                    i["status"].as_str().unwrap(),
                )
            })
            .collect();
        assert_eq!(
            items,
            [
                (1_000_001, "user_message", "completed"),
                // Still streaming when the turn failed: it ends with it.
                (1_000_002, "assistant_message", "failed"),
                (1_000_003, "command_execution", "completed"),
                (1_000_004, "error", "failed"),
            ]
        );
        let error = &ended["turnItems"][3];
        assert_eq!(error["failure"]["message"], "boom");
        assert_eq!(error["title"], "Provider error");
        assert_eq!(
            error["id"],
            "turn-item:provider:codex:native-item:terminal-failure%3Aprovider-turn%3Aprovider%3Acodex%3Anative-turn%3Aturn_1"
        );
        let turn = &ended["providerTurns"][0];
        assert_eq!(turn["status"], "failed");
        assert_eq!(turn["tokenUsage"]["usedTokens"], 10);
        assert_eq!(
            ended["providerThreads"][0]["nativeThreadRef"]["nativeId"],
            "thr_1"
        );
        assert_eq!(ended["providerThreads"][0]["status"], "idle");
        assert_eq!(ended["visibleTurnItems"].as_array().unwrap().len(), 4);
        assert!(agentd.turns_acked());
    }

    /// A turn whose CLI never started (a missing `codex`) still names a
    /// provider thread on its run, as a client must decode, and reports the
    /// session as the reference does. Reasoning without text is not shown.
    #[test]
    fn a_turn_whose_folder_is_missing_says_so() {
        use crate::workspace::provider::{ProviderEvent, ThreadDisposition, TurnStatus};
        let runtime = Arc::new(ManualRuntime::default());
        let plane = ControlPlane::open(
            config(),
            Arc::new(MemoryStore::new()),
            Box::new(MemoryLog::default()),
            host(&runtime),
        )
        .unwrap();
        let agentd = agentd_with_project_folder();
        now(plane.workspace_connected(agentd.link(plane.workspace()))).unwrap();
        let mut connection = owner(&plane);
        seed(&plane, &mut connection);
        dispatch_message(&plane, &mut connection, "hello");
        runtime.run();
        let stream = *agentd.turns().keys().next().unwrap();
        let failure = crate::workspace::provider::workspace_missing("/workspace/one");
        agentd.emit(
            stream,
            WireTurnEvent::Provider {
                event: ProviderEvent::TurnTerminal {
                    status: TurnStatus::Failed,
                    failure: Some(failure.clone()),
                    thread_disposition: ThreadDisposition::Reusable,
                    driver: Some("codex".into()),
                    closing: Vec::new(),
                },
            },
        );
        agentd.emit(
            stream,
            WireTurnEvent::Exited {
                success: false,
                cancelled: false,
                status: "failed".into(),
            },
        );
        runtime.run();
        let reply = send(
            &plane,
            &mut owner(&plane),
            json!({"_tag":"Request","id":"g","tag":"orchestration.getThreadProjection","payload":{"threadId":"t1"},"headers":[]}),
        );
        let error = &reply[0]["exit"]["value"]["turnItems"][1];
        assert_eq!(error["title"], "Provider session failed to open", "{error}");
        assert_eq!(error["failure"]["message"], failure.message);
        assert!(failure
            .message
            .starts_with("This thread's workspace folder no longer exists"));
    }

    #[test]
    fn a_turn_that_never_opened_a_session_names_its_provider_thread() {
        use crate::workspace::provider::{
            ProviderEvent, ProviderFailure, ProviderItem, ThreadDisposition, TurnStatus,
        };
        let runtime = Arc::new(ManualRuntime::default());
        let plane = ControlPlane::open(
            config(),
            Arc::new(MemoryStore::new()),
            Box::new(MemoryLog::default()),
            host(&runtime),
        )
        .unwrap();
        let agentd = agentd_with_project_folder();
        now(plane.workspace_connected(agentd.link(plane.workspace()))).unwrap();
        let mut connection = owner(&plane);
        seed(&plane, &mut connection);
        dispatch_message(&plane, &mut connection, "hello");
        runtime.run();
        let stream = *agentd.turns().keys().next().unwrap();
        let emit = |event| agentd.emit(stream, WireTurnEvent::Provider { event });
        emit(ProviderEvent::TurnTerminal {
            status: TurnStatus::Failed,
            failure: Some(ProviderFailure::new("provider_error", "spawn failed")),
            thread_disposition: ThreadDisposition::Reusable,
            driver: Some("codex".into()),
            closing: Vec::new(),
        });
        agentd.emit(
            stream,
            WireTurnEvent::Exited {
                success: false,
                cancelled: false,
                status: "failed".into(),
            },
        );
        runtime.run();
        let reply = send(
            &plane,
            &mut owner(&plane),
            json!({"_tag":"Request","id":"g","tag":"orchestration.getThreadProjection","payload":{"threadId":"t1"},"headers":[]}),
        );
        let ended = &reply[0]["exit"]["value"];
        let run = &ended["runs"][0];
        let thread_id = run["providerThreadId"].as_str().unwrap();
        assert!(thread_id.starts_with("provider-thread:provider:codex:native-thread:pending"));
        let thread = &ended["providerThreads"][0];
        assert_eq!(thread["id"], thread_id);
        assert_eq!(thread["status"], "not_loaded");
        assert_eq!(thread["nativeThreadRef"], Value::Null);
        assert_eq!(ended["providerTurns"], json!([]));
        let error = &ended["turnItems"][1];
        assert_eq!(error["title"], "Provider session failed to open");
        assert_eq!(
            error["failure"]["message"],
            "Failed to open codex provider session provider-session:provider-instance:codex:shared."
        );
        assert_eq!(
            error["id"],
            format!(
                "turn-item:run:{}:signal:provider-session-open-failure",
                uri(run["id"].as_str().unwrap())
            )
        );

        // The next turn opens its thread, and its empty reasoning is dropped.
        dispatch_nth_message(&plane, &mut connection, 2, "again");
        runtime.run();
        let stream = *agentd.turns().keys().max().unwrap();
        let emit = |event| agentd.emit(stream, WireTurnEvent::Provider { event });
        emit(ProviderEvent::ProviderThread {
            driver: "codex".into(),
            native_thread_id: "thr_1".into(),
            resumed: false,
            model: None,

            opencode2: None,
            native_title: None,
            native_updated_at: None,
            context_usage: None,
            session_started: None,
        });
        emit(ProviderEvent::TurnItem {
            item: ProviderItem::new("rs_1", "reasoning", "completed").with("text", ""),
        });
        emit(ProviderEvent::TurnTerminal {
            status: TurnStatus::Completed,
            failure: None,
            thread_disposition: ThreadDisposition::Reusable,
            driver: Some("codex".into()),
            closing: Vec::new(),
        });
        agentd.emit(
            stream,
            WireTurnEvent::Exited {
                success: true,
                cancelled: false,
                status: "completed".into(),
            },
        );
        runtime.run();
        let reply = send(
            &plane,
            &mut owner(&plane),
            json!({"_tag":"Request","id":"g","tag":"orchestration.getThreadProjection","payload":{"threadId":"t1"},"headers":[]}),
        );
        let ended = &reply[0]["exit"]["value"];
        assert_eq!(ended["runs"][1]["status"], "completed");
        assert_eq!(ended["providerThreads"][0]["status"], "idle");
        assert!(ended["turnItems"]
            .as_array()
            .unwrap()
            .iter()
            .all(|item| item["type"] != "reasoning"));
    }

    #[test]
    fn scratch_projects_and_mcp_scratch_launches_work_through_the_workspace() {
        let documents = Arc::new(MemoryStore::new());
        enable_linked_environments(&documents);
        let runtime = Arc::new(ManualRuntime::default());
        let workspace = RemoteWorkspace::new(1, None);
        let plane = ControlPlane::open(
            config(),
            documents.clone(),
            Box::new(MemoryLog::default()),
            Host {
                runtime: runtime.clone(),
                workspace: workspace.clone(),
            },
        )
        .unwrap();
        let mut connection = owner(&plane);

        // Before a workspace connects, ensureScratch reports the workspace error.
        let disconnected = send(
            &plane,
            &mut connection,
            json!({"_tag":"Request","id":"d1","tag":"projects.ensureScratch","payload":{},"headers":[]}),
        );
        assert_eq!(
            disconnected[0]["exit"]["_tag"], "Failure",
            "{disconnected:?}"
        );
        assert_eq!(
            disconnected[0]["exit"]["cause"][0]["error"]["_tag"],
            "OrchestrationDispatchCommandError",
            "{disconnected:?}"
        );

        let agentd = Arc::new(FakeAgentd::default());
        now(plane.workspace_connected(agentd.link(&workspace))).unwrap();

        let first = send(
            &plane,
            &mut connection,
            json!({"_tag":"Request","id":"s1","tag":"projects.ensureScratch","payload":{},"headers":[]}),
        );
        assert_eq!(first[0]["exit"]["_tag"], "Success", "{first:?}");
        let project_id = first[0]["exit"]["value"]["projectId"]
            .as_str()
            .expect("a projectId")
            .to_owned();

        // Repeated calls return the same project id.
        let second = send(
            &plane,
            &mut connection,
            json!({"_tag":"Request","id":"s2","tag":"projects.ensureScratch","payload":{},"headers":[]}),
        );
        assert_eq!(
            second[0]["exit"]["value"]["projectId"], project_id,
            "{second:?}"
        );

        let store = plane.store_now();
        assert_eq!(store.projects.len(), 1);
        let row = &store.project_rows[&project_id];
        assert_eq!(row["title"], "No project");
        assert_eq!(row["workspaceRoot"], "/workspace/scratch");
        assert_eq!(
            row["projectIcon"],
            json!({"kind":"lucide","name":"message-square-dashed","color":"gray"})
        );
        drop(store);

        // MCP t3_thread_launch with scratch: true creates a fresh folder under /workspace/scratch.
        let (_, token) = peer_link_sign_in(&documents, "full-access");
        let bearer = format!("Bearer {token}");
        let rpc = |session: Option<&str>, method: &str, params: Value| {
            let mut headers = vec![
                ("authorization", bearer.as_str()),
                ("content-type", "application/json"),
            ];
            if let Some(session) = session {
                headers.push(("mcp-session-id", session));
            }
            let body = json!({"jsonrpc":"2.0","id":1,"method":method,"params":params});
            http(&plane, "POST", "/mcp", &headers, body.to_string())
        };
        let (_, headers, _) = rpc(
            None,
            "initialize",
            json!({"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"t3code-peer","version":"1"}}),
        );
        let session = headers["mcp-session-id"].to_str().unwrap().to_owned();
        let tool = |name: &str, arguments: Value| -> Value {
            let (status, _, reply) = rpc(
                Some(&session),
                "tools/call",
                json!({"name":name,"arguments":arguments}),
            );
            assert_eq!(status, 200, "{reply}");
            let result = &reply["result"];
            if result["isError"] == true {
                return serde_json::from_str(result["content"][0]["text"].as_str().unwrap())
                    .unwrap();
            }
            result["structuredContent"].clone()
        };

        // scratch: true refuses projectId or workspaceStrategy.
        let bad_project = tool(
            "t3_thread_launch",
            json!({"scratch":true,"projectId":project_id,"title":"Bad"}),
        );
        assert_eq!(bad_project["code"], "invalid_request", "{bad_project}");
        let bad_strategy = tool(
            "t3_thread_launch",
            json!({"scratch":true,"workspaceStrategy":{"type":"root"},"title":"Bad"}),
        );
        assert_eq!(bad_strategy["code"], "invalid_request", "{bad_strategy}");

        // Launching with scratch: true uses the No project id and a per-thread folder.
        let launched = tool(
            "t3_thread_launch",
            json!({"scratch":true,"title":"Quick note","message":"Convert these PNGs to WebP!",
                "modelSelection":{"instanceId":"codex","model":"fixture"},"clientRequestId":"scratch-1"}),
        );
        assert_eq!(launched["projectId"], project_id, "{launched}");
        let thread_id = launched["threadId"].as_str().expect("threadId").to_owned();
        let read = tool("t3_thread_read", json!({"threadId":thread_id}));
        let folder = read["thread"]["worktreePath"]
            .as_str()
            .expect("a scratch folder")
            .to_owned();
        assert!(folder.starts_with("/workspace/scratch/"), "{folder}");
        assert!(folder.contains("convert-these-pngs-to-webp"), "{folder}");

        // Retrying with the same clientRequestId replays without making a second folder or turn.
        let retried = tool(
            "t3_thread_launch",
            json!({"scratch":true,"title":"Quick note","message":"Convert these PNGs to WebP!",
                "modelSelection":{"instanceId":"codex","model":"fixture"},"clientRequestId":"scratch-1"}),
        );
        assert_eq!(retried["threadId"], launched["threadId"], "{retried}");
        assert_eq!(retried["runId"], launched["runId"], "{retried}");

        runtime.run();
        let turns = agentd.turns();
        assert_eq!(turns.len(), 1);
        assert_eq!(turns.values().next().unwrap().cwd, folder);

        // A second scratch launch reuses the scratch project with a distinct folder.
        let second_launch = tool(
            "t3_thread_launch",
            json!({"scratch":true,"title":"Another quick note",
                "modelSelection":{"instanceId":"codex","model":"fixture"}}),
        );
        assert_eq!(second_launch["projectId"], project_id, "{second_launch}");
        let second_read = tool(
            "t3_thread_read",
            json!({"threadId":second_launch["threadId"]}),
        );
        let second_folder = second_read["thread"]["worktreePath"].as_str().unwrap();
        assert!(
            second_folder.starts_with("/workspace/scratch/"),
            "{second_folder}"
        );
        assert_ne!(second_folder, folder);

        // Deleting the built-in scratch project through MCP is refused.
        let refused_delete = tool("t3_project_delete", json!({"projectId":project_id}));
        assert_eq!(
            refused_delete["code"], "invalid_request",
            "{refused_delete}"
        );
    }

    fn uri(value: &str) -> String {
        crate::thread_projection::uri_component(value)
    }
}
