//! The native server: AppState, the axum router and the process-backed run
//! loop. Only built with the `native` feature.
use crate::*;

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

use serde_json::json;
use sha2::Sha256;
use std::{
    collections::HashMap,
    path::{Path as FsPath, PathBuf},
    sync::Arc,
};
use tokio::sync::{oneshot, Mutex};
use tracing::info;
use uuid::Uuid;

pub struct AppState {
    /// The event log, store, auth and state documents; AppState dereferences
    /// to it.
    pub control: crate::control_state::ControlState,
    pub(crate) scheduled_database: Arc<Mutex<crate::statev2::scheduled_tasks::Database>>,
    pub(crate) config_started_at: String,
    /// What the startup's provider runtime recovery did, for the
    /// `server.startup` trace span ([`crate::startup_trace`]).
    pub(crate) startup_recovery: crate::startup_trace::RecoverySummary,
    pub(crate) environment_id: String,
    pub oauth: mcp_oauth::OAuth,
    /// Links from this environment to others.
    pub(crate) peer_links: crate::peer_links::PeerLinks,
    /// MCP sessions with linked environments.
    pub(crate) peer_mcp: crate::peer_mcp::PeerMcpClient,
    /// The link cards being answered now.
    pub(crate) link_answers: crate::peer_link_requests::Answering,
    /// `http://<host>:<port>/mcp` once the server listens: where provider
    /// CLIs reach T3's tools.
    pub(crate) mcp_endpoint: std::sync::OnceLock<String>,
    pub(crate) connect: access_http::connect::Service,
    pub data_dir: PathBuf,
    pub paths: config::ServerPaths,
    pub(crate) attachment_sweep_ms: std::sync::atomic::AtomicI64,
    pub runtime_config: config::RuntimeConfig,
    pub settings: Arc<settings::Settings>,
    pub provider_command: Vec<String>,
    pub cancellations: Mutex<HashMap<Uuid, oneshot::Sender<()>>>,
    pub guest: guest_service::GuestService,
    pub voice: voice::VoiceService,
    pub pull_requests: pull_requests::PullRequestService,
    pub issues: tokio::sync::Mutex<issues::IssueService>,
    /// The workspace plane: files, git, terminals, processes and previews.
    pub(crate) workspace: Arc<dyn crate::workspace::Workspace>,
    /// The same workspace, for the native-only HTTP routes that reach into
    /// its services (media, browser pages).
    pub(crate) local: Arc<crate::workspace::LocalWorkspace>,
    pub worktree_setup: worktree_setup::WorktreeSetupTracker,
    pub(crate) project_mutation_lock: Mutex<()>,
    pub(crate) find_cache: std::sync::Mutex<crate::thread_search::FindCache>,
    pub(crate) storage_cleanup: crate::storage_cleanup::Reports,
    pub(crate) pull_request_refresh: Mutex<u64>,
    pub(crate) provider_install_states: Mutex<HashMap<String, serde_json::Value>>,
    pub(crate) provider_auth_states: Mutex<HashMap<String, serde_json::Value>>,
    pub(crate) project_clones: Arc<crate::project_clone::Tracker>,
    /// The provider model manifest, refreshed from upstream.
    pub(crate) model_manifest:
        Arc<crate::model_manifest::ModelManifest<crate::model_manifest::NativeHost>>,
    /// The provider CLIs' status checks.
    pub(crate) provider_status: crate::provider_status::ProviderStatus,
    /// Background tasks, timers and the open sockets.
    pub(crate) runtime: Arc<crate::runtime::TokioRuntime>,
    /// Set once the server starts to stop; see [`crate::turn::TurnHost::stopping`].
    pub(crate) stopping: std::sync::atomic::AtomicBool,
    /// Set once the server stops serving its clients: their RPC sockets
    /// close.
    pub(crate) closing: tokio::sync::watch::Sender<bool>,
}
impl AppState {
    /// Do not check the provider CLIs (`claude`, `codex`) on PATH, for a
    /// server in a test, which must never run the host's real CLIs. This is
    /// the default; see [`Self::enable_provider_status_checks`].
    pub fn disable_provider_status_checks(&self) {
        self.provider_status.disable();
    }

    /// Check the provider CLIs at start-up, on their refresh interval and
    /// when a client asks, as the reference does. Only the server binary
    /// turns this on: a server a test builds never runs the host's CLIs.
    pub fn enable_provider_status_checks(&self) {
        self.provider_status.enable();
    }

    /// The server is stopping: record how its running turns are cut off,
    /// then stop their CLIs, which do not outlive it.
    pub(crate) async fn stop_provider_runtime(&self) {
        self.stopping
            .store(true, std::sync::atomic::Ordering::SeqCst);
        // The reference stops serving before it reconciles, so no client
        // watches the reconciliation; one that reconnects reads its result.
        self.closing.send_replace(true);
        tokio::task::yield_now().await;
        {
            let _guard = self.project_mutation_lock.lock().await;
            reconcile_provider_runtime(&self.control, &self.settings, "shutdown").await;
        }
        for (_, cancel) in self.cancellations.lock().await.drain() {
            let _ = cancel.send(());
        }
        // The instances' shared Codex app-servers do not outlive the server.
        self.workspace.stop_providers().await;
    }

    /// Continue the turns a restart cut off or woke, as the reference's
    /// `provider-runtime.continue` effects do once the server is up, and
    /// schedule the retry of a continuation that failed to start.
    pub(crate) async fn continue_restarted_runs(self: Arc<Self>) {
        self.continue_restarted(None).await
    }

    /// [`Self::continue_restarted_runs`], for the continuation of `only`
    /// when given.
    fn continue_restarted(
        self: Arc<Self>,
        only: Option<Uuid>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> {
        Box::pin(self.continue_restarted_now(only))
    }

    async fn continue_restarted_now(self: Arc<Self>, only: Option<Uuid>) {
        let settings = match self.settings.internal() {
            Ok(settings) => settings,
            Err(error) => {
                tracing::warn!(%error, "could not read settings to continue restarted turns");
                return;
            }
        };
        // A scheduled prompt its provider never took is sent again.
        let resends = if only.is_none() {
            crate::runtime_recovery::resends(&*self.store.read().await)
        } else {
            vec![]
        };
        for (thread_id, run_id) in resends {
            if let Err(error) =
                crate::orchestration::resend_run(self.clone(), &thread_id, run_id).await
            {
                tracing::info!(%thread_id, %run_id, %error, "orchestration-v2.restart-resend.not-sent");
            }
        }
        let commands = {
            let store = self.store.read().await;
            crate::runtime_recovery::continuations(
                &store,
                &settings,
                &auth_service::iso(auth_service::now()),
            )
        };
        let now = crate::now();
        for continuation in commands {
            let crate::runtime_recovery::Continuation {
                source,
                command,
                due,
            } = continuation;
            if only.is_some_and(|only| only != source) {
                continue;
            }
            // A retry waits, then checks again that it is still owed.
            if due > now && only.is_none() {
                let state = self.clone();
                tokio::spawn(async move {
                    tokio::time::sleep(std::time::Duration::from_secs(due - now)).await;
                    state.continue_restarted(Some(source)).await;
                });
                continue;
            }
            let thread_id = command["threadId"].as_str().unwrap_or_default().to_owned();
            if let Err(error) =
                crate::orchestration::dispatch_restart_continuation(self.clone(), command, source)
                    .await
            {
                tracing::info!(
                    %thread_id,
                    source_run_id = %source,
                    error = %error["message"],
                    "orchestration-v2.restart-continuation.not-sent"
                );
            }
        }
    }

    /// The settings workspace method `method` takes, as configured now. Only
    /// the fields that method reads are filled in; reading them costs a
    /// settings load or a lock.
    pub(crate) async fn workspace_settings(
        &self,
        method: &str,
    ) -> crate::dispatch::WorkspaceSettings {
        let mut settings = crate::dispatch::WorkspaceSettings {
            automatic_git_fetch: true,
            ..Default::default()
        };
        if method.starts_with("git.") {
            settings.git = crate::workspace::GitTools {
                gh: self.issues.lock().await.gh.clone(),
                text_generation: self.provider_command.clone(),
            };
        }
        if matches!(method, "vcs.refreshStatus" | "subscribeVcsStatus") {
            // `VcsAutomaticFetchInterval`: a zero interval turns off the fetch a status read starts.
            settings.automatic_git_fetch = self.settings.internal().map_or(true, |settings| {
                settings["automaticGitFetchInterval"].as_f64() != Some(0.0)
            });
        }
        settings
    }

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
        let documents: Arc<dyn persistence::DocumentStore> = Arc::new(persistence::FsStore);
        let settings = Arc::new(settings::Settings::open_in(&paths, documents.clone())?);
        let auth = auth.open_in(documents.clone(), &dir)?;
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
        // The orchestration database both servers share. An unsupported one
        // stops start-up here, unchanged (docs/shared-store.md).
        let shared_store = crate::statev2::open(&dir)?;
        let webhook_denial = settings
            .policy
            .summary()
            .filter(|summary| summary["limits"]["webhookTasks"] == false)
            .map(|_| settings.policy.denial("webhook tasks"));
        let scheduled_database =
            crate::statev2::scheduled_tasks::Database::open(&dir, webhook_denial)?;
        let log: Box<dyn event_log::EventLog + Send> =
            Box::new(event_log::NdjsonLog::open(dir.join("events.jsonl"))?);
        let oauth = mcp_oauth::OAuth::open(documents.clone(), &dir)?;
        let mut control = crate::control_state::ControlState::open(auth, documents, log, &dir)?;
        let scheduled_database = Arc::new(Mutex::new(scheduled_database));
        control.scheduled_database = Some(scheduled_database.clone());
        {
            let database = dir.join(crate::statev2::FILE_NAME);
            let read_error = |error: rusqlite::Error| {
                anyhow::anyhow!("Could not read {}: {error}", database.display())
            };
            let mut store = control.store.write().await;
            let loaded =
                crate::statev2::load_into(&shared_store, &mut store).map_err(read_error)?;
            if loaded != crate::statev2::Loaded::default() {
                tracing::info!(
                    projects = loaded.projects,
                    threads = loaded.threads,
                    replaced = loaded.replaced,
                    "read threads from the shared orchestration database"
                );
            }
            drop(store);
            // What the previous process (either server) left in the outbox.
            let (requeued, cancelled) =
                crate::statev2::outbox::reconcile_after_process_loss(&shared_store)
                    .map_err(read_error)?;
            if requeued + cancelled > 0 {
                tracing::info!(
                    requeued,
                    cancelled,
                    "reconciled the effect outbox after a restart"
                );
            }
            let sequence = crate::statev2::read::max_sequence(&shared_store).map_err(read_error)?;
            let (threads, projects) = {
                let store = control.store.read().await;
                crate::statev2::missing_from_database(&shared_store, &store).map_err(read_error)?
            };
            // Every later commit is written to the shared database first.
            control.attach_shared_store(
                Box::new(crate::statev2::write::Writer::new(shared_store)),
                sequence,
            );
            // The one-time import of this server's own threads (events.jsonl
            // stays as it is): one commit that writes them whole.
            if !threads.is_empty() || !projects.is_empty() {
                tracing::info!(
                    threads = threads.len(),
                    projects = projects.len(),
                    "importing this server's threads into the shared orchestration database"
                );
                let all = Event::Imported {
                    thread_ids: threads.clone(),
                    project_ids: projects.clone(),
                };
                if let Err(error) = control.commit(all).await {
                    // One at a time, so a thread the database refuses keeps
                    // only that one in this server's log.
                    tracing::warn!(%error, "importing one thread or project at a time");
                    let one = |threads: Vec<String>, projects: Vec<String>| Event::Imported {
                        thread_ids: threads,
                        project_ids: projects,
                    };
                    for event in projects
                        .into_iter()
                        .map(|id| one(vec![], vec![id]))
                        .chain(threads.into_iter().map(|id| one(vec![id], vec![])))
                    {
                        if let Err(error) = control.commit(event.clone()).await {
                            tracing::error!(%error, ?event, "could not import into the shared orchestration database");
                        }
                    }
                }
            }
        }
        // Turns run as this process's children, so none survives a restart.
        let startup_recovery = reconcile_provider_runtime(&control, &settings, "startup").await;
        // Restart continuations the other server (or this one) left owed.
        if let Err(error) = control.adopt_continuations().await {
            tracing::warn!(%error, "could not take over restart continuations from the outbox");
        }
        let local = Arc::new(crate::workspace::LocalWorkspace::new(
            dir.clone(),
            settings.clone(),
            local_services::TerminalService::with_logs(dir.join("logs/terminals")),
            local_services::PreviewService::default(),
            local_services::DeviceService::new(settings.clone(), &dir)?,
        ));
        Ok(Arc::new(Self {
            control,
            scheduled_database,
            config_started_at: auth_service::iso(auth_service::now()),
            startup_recovery,
            environment_id: environment_http::identity(&dir)?,
            oauth,
            peer_links: crate::peer_links::PeerLinks::new(&dir, &paths.secrets),
            peer_mcp: Default::default(),
            link_answers: Default::default(),
            mcp_endpoint: Default::default(),
            connect,
            data_dir: dir.clone(),
            paths: paths.clone(),
            runtime_config,
            settings: settings.clone(),
            provider_command,
            cancellations: Mutex::new(HashMap::new()),
            guest,
            voice: voice::VoiceService::new(settings.clone()),
            pull_requests: pull_requests::PullRequestService::default(),
            issues: tokio::sync::Mutex::new(issues::IssueService::new(dir.clone())),
            workspace: local.clone(),
            local,
            worktree_setup: worktree_setup::WorktreeSetupTracker::default(),
            project_mutation_lock: Mutex::new(()),
            find_cache: Default::default(),
            storage_cleanup: Default::default(),
            attachment_sweep_ms: Default::default(),
            pull_request_refresh: Mutex::new(0),
            provider_install_states: Mutex::new(HashMap::new()),
            provider_auth_states: Mutex::new(HashMap::new()),
            project_clones: Arc::default(),
            model_manifest: Arc::new(crate::model_manifest::ModelManifest::new(
                crate::model_manifest::NativeHost::new(&paths.state_dir, settings.clone()),
            )),
            provider_status: crate::provider_status::ProviderStatus::new(&paths),
            runtime: Arc::default(),
            stopping: Default::default(),
            closing: tokio::sync::watch::Sender::new(false),
        }))
    }
}

impl crate::orchestration::CommandHost for AppState {
    fn generate_title(self: Arc<Self>, effect: crate::title_generation::TitleEffect) {
        tokio::spawn(crate::title_generation::execute(self, effect));
    }
    fn mutation_lock(&self) -> &Mutex<()> {
        &self.project_mutation_lock
    }
    fn policy_summary(&self) -> Option<&serde_json::Value> {
        self.settings.policy.summary()
    }
    fn runtime_denial(&self, mode: &str) -> Option<String> {
        self.settings.policy.runtime_denial(mode)
    }
    fn provider_denial(&self, driver: &str) -> Option<String> {
        self.settings.policy.provider_denial(driver)
    }
    fn effective_settings(&self) -> anyhow::Result<serde_json::Value> {
        self.settings.internal()
    }
    fn workspace(&self) -> &dyn crate::workspace::Workspace {
        &*self.workspace
    }
    fn cancellations(&self) -> &Mutex<HashMap<Uuid, oneshot::Sender<()>>> {
        &self.cancellations
    }
    fn launch_run(
        self: Arc<Self>,
        run: Run,
        cwd: String,
        text: String,
        cancel: oneshot::Receiver<()>,
    ) {
        let env_remove = if self.settings.policy.summary().is_some() {
            vec!["T3CODE_CODEX_LAUNCH_ARGS".into()]
        } else {
            vec![]
        };
        let runtime = self.runtime.clone();
        crate::runtime::Runtime::spawn(
            &*runtime,
            Box::pin(async move {
                // A thread whose provider instance runs Codex or Claude runs
                // that CLI, as the control plane does; any other runs the
                // configured command.
                let provider = match self.settings.internal() {
                    Ok(settings) => {
                        let store = self.store.read().await;
                        crate::provider_state::thread_provider_turn(
                            &settings,
                            &self.model_manifest.current(),
                            store.threads.get(&run.thread_id),
                            &self.oauth,
                            self.mcp_endpoint.get().map(String::as_str),
                        )
                    }
                    Err(error) => {
                        tracing::warn!(%error, "could not read settings for a turn");
                        Ok(None)
                    }
                };
                let mut spec = crate::workspace::TurnSpec {
                    prompt: text,
                    cwd,
                    env_remove,
                    turn_id: run.id.to_string(),
                    ..Default::default()
                };
                match provider {
                    Ok(Some((mut provider, env))) => {
                        // Claude may read the thread's attachments, as the
                        // reference lets it.
                        provider.extra_dirs = vec![self.paths.attachments.display().to_string()];
                        spec.provider = Some(provider);
                        spec.env = env;
                    }
                    Err(error) => {
                        crate::turn::fail(&*self, &run, error.into()).await;
                        return;
                    }
                    Ok(None) => {
                        spec.command = if self.provider_command.is_empty() {
                            crate::turn::echo_command()
                        } else {
                            self.provider_command.clone()
                        };
                    }
                }
                // A continuation that failed to start may be owed a retry.
                let continues = run.restart_continuation_of.is_some().then(|| self.clone());
                let (host, thread_id) = (self.clone(), run.thread_id.clone());
                crate::turn::launch(self, run, spec, cancel).await;
                // A delegated child's end settles its task; an idle parent gets its results.
                crate::orchestration::after_run(host, &thread_id).await;
                if let Some(state) = continues {
                    state.continue_restarted_runs().await;
                }
            }),
        );
    }
    fn detach_turn(&self, run: Uuid) {
        crate::local_services::provider_turns::mark_detached(&run.to_string());
    }
    fn unload_provider_thread(&self, instance: &str, thread: &str) {
        self.workspace().unload_provider_thread(instance, thread);
    }
    fn close_guest_thread(&self, thread_id: &str) -> Result<(), String> {
        self.guest.close_thread(thread_id).map_err(str::to_owned)
    }
    fn resolve_guest_message(
        &self,
        thread_id: &str,
        message_id: &str,
        outcome: &str,
    ) -> Result<(), String> {
        self.guest
            .resolve_message(thread_id, message_id, outcome)
            .map_err(|error| error.to_string())
    }
}

impl crate::turn::TurnHost for AppState {
    fn workspace(&self) -> &dyn crate::workspace::Workspace {
        &*self.workspace
    }
    fn turn_wait(&self, duration: std::time::Duration) -> crate::runtime::Wait {
        crate::runtime::Runtime::sleep(&*self.runtime, duration)
    }
    fn cancellations(&self) -> &Mutex<HashMap<Uuid, oneshot::Sender<()>>> {
        &self.cancellations
    }
    fn stopping(&self) -> bool {
        self.stopping.load(std::sync::atomic::Ordering::SeqCst)
    }
}

/// Record a restart's reconciliation of every thread's provider runtime
/// ([`crate::runtime_recovery::reconcile`]), and count what it ended as the
/// reference's recovery summary does.
async fn reconcile_provider_runtime(
    control: &crate::control_state::ControlState,
    settings: &settings::Settings,
    trigger: &str,
) -> crate::startup_trace::RecoverySummary {
    let settings = settings.internal().unwrap_or_else(|error| {
        tracing::warn!(%error, "could not read settings to reconcile provider turns");
        serde_json::Value::Null
    });
    let mut summary = crate::startup_trace::RecoverySummary::default();
    let events = {
        let store = control.store.read().await;
        let events = crate::runtime_recovery::reconcile(
            &store,
            trigger,
            crate::now(),
            &auth_service::iso(auth_service::now()),
            &settings,
        );
        for event in &events {
            let Event::ProviderRuntimeReconciled { thread_id, cut, .. } = event else {
                continue;
            };
            summary.terminalized_runs += cut.len();
            let Some(thread) = store.threads.get(thread_id) else {
                continue;
            };
            summary.stopped_sessions += usize::from(thread.provider.session_live());
            summary.closed_requests += cut
                .iter()
                .filter_map(|(run, _)| thread.provider.runs.get(run))
                .flat_map(|record| &record.requests)
                .filter(|request| request.pending())
                .count();
        }
        events
    };
    for event in events {
        if let Err(error) = control.commit(event).await {
            tracing::error!(%error, trigger, "could not record a provider runtime reconciliation");
        }
    }
    summary
}

impl std::ops::Deref for AppState {
    type Target = crate::control_state::ControlState;
    fn deref(&self) -> &Self::Target {
        &self.control
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
        .merge(mcp_oauth::routes::<AppState>())
        .merge(access_http::routes())
        .merge(host_rpc::routes())
        .route(
            "/.well-known/t3/environment",
            get(environment_http::descriptor),
        )
        .route("/ws", get(websocket))
        .fallback(any(contract_fallback))
        .layer(axum::middleware::from_fn(access_http::contextualize))
        // Outermost, like the reference's global CORS middleware: preflights
        // never reach a route and every response, errors included, is tagged.
        .layer(axum::middleware::from_fn(cors::browser_api))
        .with_state(state)
}

pub(crate) async fn otlp_traces_proxy(
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

pub(crate) async fn webhook_handler(
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
    let task = match state.scheduled_database.lock().await.get(&hook_id) {
        Ok(task) => task.filter(|task| {
            task["schedule"]["type"] == "webhook"
                && task["_webhookToken"].as_str() == Some(token.as_str())
        }),
        Err(_) => {
            return outcome(
                StatusCode::INTERNAL_SERVER_ERROR,
                json!({"error":"internal_error"}),
                "error",
            )
        }
    };
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

pub(crate) fn webhook_signature_valid(
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

pub(crate) async fn contract_fallback(
    State(state): State<Arc<AppState>>,
    request: Request,
) -> Response {
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
pub(crate) async fn require_session(
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

pub(crate) async fn websocket(State(state): State<Arc<AppState>>, request: Request) -> Response {
    let query = url::form_urlencoded::parse(request.uri().query().unwrap_or("").as_bytes());
    let references = query
        .clone()
        .find(|(k, _)| k == "clientCapabilities")
        .is_some_and(|(_, v)| v.split(',').any(|c| c == "settingsSecretReferences"));
    // The reference server checks only that the parameter is present.
    let frame_parts = query
        .clone()
        .any(|(key, _)| key == rpc::SOCKET_FRAME_PARTS_QUERY_PARAM);
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
            .on_upgrade(move |socket| {
                rpc::session(state, socket, principal, references, frame_parts)
            })
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
    // As the reference: an unspecified address is reached on loopback.
    let host = match bound.ip() {
        ip if ip.is_unspecified() => "127.0.0.1".to_owned(),
        std::net::IpAddr::V6(ip) => format!("[{ip}]"),
        ip => ip.to_string(),
    };
    let _ = state
        .mcp_endpoint
        .set(format!("http://{host}:{}/mcp", bound.port()));
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
    let cleanup = tokio::spawn(crate::storage_cleanup::automatic_runs(state.clone()));
    // Refresh the model manifest now and then on the provider-check cadence;
    // each refresh is TTL-gated, so most ticks read nothing.
    let manifest = state.model_manifest.clone();
    let manifest_refresh = tokio::spawn(async move {
        let period =
            std::time::Duration::from_millis(crate::model_manifest::MANIFEST_RETRY_MS as u64);
        loop {
            manifest.refresh().await;
            tokio::time::sleep(period).await;
        }
    });
    // T3 Connect: run the stored managed tunnel's relay client while this server listens.
    let policy = &state.settings.policy;
    let endpoint = crate::managed_endpoint::ManagedEndpointRuntime::open(
        crate::managed_endpoint::RuntimeOptions::for_paths(
            &state.paths,
            policy.allows_connect() && policy.allows_client("remote"),
        ),
    )
    .await;
    // `server.ts`'s cloud link fiber: register (or provision) the tunnel with T3 Connect,
    // serve recovery requests, and release a CLI-provisioned tunnel on shutdown.
    let cloud_link = policy.allows_connect().then(|| {
        let descriptor_state = state.clone();
        Arc::new(crate::cloud_link::CloudLink::new(
            endpoint.clone(),
            crate::cloud_link::CloudLinkOptions {
                environment_id: state.environment_id.clone(),
                public: crate::cloud_link::PublicConfig::from_env(),
                descriptor: Arc::new(move || {
                    let state = descriptor_state.clone();
                    Box::pin(async move {
                        crate::environment_http::descriptor(axum::extract::State(state))
                            .await
                            .0
                    })
                }),
                timings: crate::cloud_link::Timings::default(),
            },
        ))
    });
    let endpoint_startup = cloud_link.clone().map(|cloud_link| {
        let origin = crate::managed_endpoint::LocalOrigin::for_address(bound);
        tokio::spawn(async move {
            let recoveries = cloud_link.serve_recovery_requests(origin.clone());
            let startup = cloud_link.run_startup(&origin);
            tokio::join!(recoveries, startup);
        })
    });
    // Continue what the last restart cut off, now that clients can watch it,
    // then end the startup span as the reference does once its recovery has
    // queued every continuation.
    let started = std::time::SystemTime::now();
    let startup = state.clone();
    tokio::spawn(async move {
        startup.clone().continue_restarted_runs().await;
        // Title requests the other server (or this one) left unanswered.
        for effect in startup.pending_title_effects().await {
            tokio::spawn(crate::title_generation::execute(startup.clone(), effect));
        }
        let span = crate::startup_trace::startup_span(
            &startup.runtime_config.mode,
            &bound.ip().to_string(),
            bound.port(),
            started,
            std::time::SystemTime::now(),
            startup.startup_recovery,
        );
        if let Err(error) = crate::startup_trace::append(&startup.paths.logs, &span) {
            tracing::warn!(%error, "could not write the startup span to the server trace");
        }
    });
    // Check the provider CLIs once clients can load, as the reference does.
    tokio::spawn(crate::provider_status::run(state.clone()));
    let (stopping_tx, stopping_rx) = oneshot::channel::<()>();
    let stopping_endpoint = endpoint.clone();
    let stopping_state = state.clone();
    let server = axum::serve(
        listener,
        router(state.clone()).into_make_service_with_connect_info::<std::net::SocketAddr>(),
    )
    .with_graceful_shutdown(async move {
        shutdown.await;
        info!("shutting down t3code server");
        stopping_state.stop_provider_runtime().await;
        stopping_endpoint.begin_shutdown();
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
    cleanup.abort();
    state.storage_cleanup.shutdown().await;
    manifest_refresh.abort();
    if let Some(startup) = endpoint_startup {
        startup.abort();
    }
    if let Some(cloud_link) = &cloud_link {
        endpoint.begin_shutdown();
        cloud_link.release_on_shutdown_logged().await;
    }
    endpoint.shutdown().await;
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
        .unwrap_or_else(crate::turn::echo_command)
}
pub fn ensure_path(path: &str) -> anyhow::Result<()> {
    let p = FsPath::new(path);
    if !p.exists() {
        anyhow::bail!("working directory does not exist: {path}");
    }
    Ok(())
}

impl mcp_oauth::McpHost for AppState {
    fn oauth(&self) -> &mcp_oauth::OAuth {
        &self.oauth
    }
    fn environment_id(&self) -> &str {
        &self.environment_id
    }
    fn linked_environments_enabled(&self) -> bool {
        self.settings
            .internal()
            .is_ok_and(|settings| settings["linkedEnvironmentsEnabled"] == true)
    }
    fn control_state(&self) -> &crate::control_state::ControlState {
        &self.control
    }
    fn clone_repository(
        self: &Arc<Self>,
        input: serde_json::Value,
    ) -> mcp_oauth::HostFuture<Result<serde_json::Value, String>> {
        Box::pin(async move {
            // Agents' MCP calls have their own long budget, and an aborted
            // call stops git, so the clone runs without a deadline.
            crate::source_control_clone::clone_json(&input, None)
                .await
                .map_err(|refusal| match refusal {
                    crate::source_control_clone::CloneRefusal::Defect(defect) => defect,
                    crate::source_control_clone::CloneRefusal::Failure(error) => {
                        error["detail"].as_str().unwrap_or_default().to_owned()
                    }
                })
        })
    }
    fn peer_links_listed(
        self: &Arc<Self>,
    ) -> mcp_oauth::HostFuture<Result<Vec<serde_json::Value>, String>> {
        let state = self.clone();
        Box::pin(async move {
            state
                .peer_links
                .list()
                .await
                .map(|links| {
                    links
                        .into_iter()
                        .map(|(link, status)| {
                            let mut row = link.to_json();
                            row["status"] = serde_json::json!(status);
                            row
                        })
                        .collect()
                })
                .map_err(|error| error.message)
        })
    }
    fn unlink_peer(&self, environment_id: &str) -> Result<bool, String> {
        self.peer_links
            .unlink(environment_id)
            .map_err(|error| error.message)
    }
    fn peer_link(&self, environment_id: &str) -> Option<mcp_oauth::PeerLinkInfo> {
        let link = self.peer_links.get(environment_id).ok().flatten()?;
        Some(mcp_oauth::PeerLinkInfo {
            expired: link.expired(crate::platform::now_ms()),
            label: link.label,
            access: link.access,
        })
    }
    fn forward_tool(
        self: &Arc<Self>,
        environment_id: String,
        tool: String,
        arguments: serde_json::Value,
        limit: String,
        sender: Option<String>,
    ) -> Option<mcp_oauth::HostFuture<Result<serde_json::Value, (&'static str, String)>>> {
        let state = self.clone();
        Some(Box::pin(async move {
            state
                .peer_mcp
                .call(
                    &state.peer_links,
                    &environment_id,
                    &tool,
                    arguments,
                    &limit,
                    sender.as_deref(),
                )
                .await
        }))
    }
    fn project_row(&self, row: &serde_json::Value) -> serde_json::Value {
        crate::repository_identity::with_identity(row)
    }
    fn descriptor(&self) -> serde_json::Value {
        environment_http::descriptor_value(self)
    }
    fn settings(&self) -> serde_json::Value {
        self.settings.internal().unwrap_or_default()
    }
    fn providers(&self) -> serde_json::Value {
        let settings = mcp_oauth::McpHost::settings(self);
        crate::server_config::providers::list(self, &settings, &serde_json::Value::Null)
    }
    fn dispatch(
        self: &Arc<Self>,
        command: serde_json::Value,
    ) -> mcp_oauth::HostFuture<Result<u64, serde_json::Value>> {
        Box::pin(crate::dispatch_orchestration_command(self.clone(), command))
    }
    fn mutate_project(
        self: &Arc<Self>,
        command: serde_json::Value,
    ) -> mcp_oauth::HostFuture<Result<serde_json::Value, serde_json::Value>> {
        let state = self.clone();
        Box::pin(async move { crate::projects::mutate(&state, &command).await })
    }
    fn create_named_project(
        self: &Arc<Self>,
        name: String,
    ) -> mcp_oauth::HostFuture<Result<serde_json::Value, serde_json::Value>> {
        let state = self.clone();
        Box::pin(async move { crate::rpc::create_new_project(&state, &name).await })
    }
    fn sleep(&self, duration: std::time::Duration) -> crate::runtime::Wait {
        crate::runtime::Runtime::sleep(&*self.runtime, duration)
    }
    fn spawn(&self, task: crate::runtime::Task) {
        crate::runtime::Runtime::spawn(&*self.runtime, task)
    }
    fn project_mutation(
        self: &Arc<Self>,
        command: serde_json::Value,
    ) -> mcp_oauth::HostFuture<Result<serde_json::Value, crate::mcp_oauth::ProjectRefusal>> {
        let state = self.clone();
        Box::pin(async move {
            crate::projects::mutate_checked(&state, &command)
                .await
                .map_err(|(_, refusal)| refusal)
        })
    }
    fn is_scratch_project(self: &Arc<Self>, project_id: &str) -> mcp_oauth::HostFuture<bool> {
        let state = self.clone();
        let project_id = project_id.to_owned();
        Box::pin(async move {
            let root = state.paths.base_dir.join("scratch");
            let Some(path) = state
                .store
                .read()
                .await
                .projects
                .get(&project_id)
                .map(|p| std::path::PathBuf::from(&p.path))
            else {
                return false;
            };
            match (
                state.workspace.canonicalize(&root).await,
                state.workspace.canonicalize(&path).await,
            ) {
                (Ok(root), Ok(path)) => root == path,
                _ => false,
            }
        })
    }
    fn update_settings(
        self: &Arc<Self>,
        patch: serde_json::Value,
    ) -> mcp_oauth::HostFuture<Result<(), ()>> {
        let state = self.clone();
        Box::pin(async move {
            state
                .settings
                .update(&serde_json::json!({ "patch": patch }), false)
                .map(|_| ())
                .map_err(|_| ())
        })
    }
    fn theme_read(
        self: &Arc<Self>,
        theme_id: Option<String>,
    ) -> mcp_oauth::HostFuture<Result<mcp_oauth::ThemesRead, mcp_oauth::ThemeRefusal>> {
        let dir = self.paths.themes.clone();
        Box::pin(async move {
            tokio::task::spawn_blocking(move || {
                let (themes, theme) = crate::environment_theme::read(&dir, theme_id.as_deref())?;
                Ok((themes, theme, dir.to_string_lossy().into_owned()))
            })
            .await
            .unwrap_or(Err(mcp_oauth::ThemeRefusal::NotSelected))
        })
    }
    fn theme_apply(
        self: &Arc<Self>,
        theme_id: Option<String>,
        theme: Option<serde_json::Value>,
        appearance: Option<String>,
    ) -> mcp_oauth::HostFuture<Result<(), mcp_oauth::ThemeRefusal>> {
        let state = self.clone();
        Box::pin(async move {
            tokio::task::spawn_blocking(move || {
                crate::environment_theme::apply(
                    &state.paths.themes,
                    theme_id.as_deref(),
                    theme.as_ref(),
                    appearance.as_deref(),
                    |patch| {
                        state
                            .settings
                            .update(&serde_json::json!({ "patch": patch }), false)
                            .map(|_| ())
                            .map_err(|_| ())
                    },
                )
            })
            .await
            .unwrap_or(Err(mcp_oauth::ThemeRefusal::NotSelected))
        })
    }
    fn list_refs(
        self: &Arc<Self>,
        input: serde_json::Value,
    ) -> mcp_oauth::HostFuture<Result<serde_json::Value, ()>> {
        let state = self.clone();
        Box::pin(async move {
            // Storage cleanup removes a worktree but keeps its path on the thread.
            let mut input = input;
            let worktree = input["worktreePath"].as_str().map(std::path::PathBuf::from);
            let live = match &worktree {
                Some(path) => state.workspace.is_dir(path).await,
                None => false,
            };
            let cwd = if live {
                input["worktreePath"].clone()
            } else {
                input["workspaceRoot"].clone()
            };
            if let Some(object) = input.as_object_mut() {
                object.remove("worktreePath");
                object.remove("workspaceRoot");
                object.insert("cwd".into(), cwd);
            }
            match state.workspace.vcs("listRefs", &input, false).await {
                Some(Ok(refs)) => Ok(refs),
                _ => Err(()),
            }
        })
    }
    fn search_threads(
        &self,
        query: String,
        limit: usize,
        project: Option<String>,
    ) -> mcp_oauth::HostFuture<Result<Vec<serde_json::Value>, ()>> {
        let dir = self.data_dir.clone();
        Box::pin(async move {
            tokio::task::spawn_blocking(move || {
                crate::thread_search::search_agent_threads(&dir, &query, limit, project.as_deref())
            })
            .await
            .map_err(|_| ())?
            .map_err(|_| ())
        })
    }
}

impl access_http::AuthHost for AppState {
    fn auth(&self) -> &auth_service::AuthService {
        &self.auth
    }
    fn pairing_refusal(&self) -> Option<String> {
        self.settings.policy.pairing_refusal()
    }
}
