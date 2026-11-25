//! Turns over the Agent Client Protocol (ACP): newline-delimited JSON-RPC 2.0
//! with the agent's CLI on its standard streams, as the reference's generic
//! ACP adapter speaks it, in its Grok flavor (`grok agent stdio`) or its
//! generic one for ACP Registry agents ([`Flavor`]).
//!
//! A thread keeps its agent process across turns ([`Sessions`]), as the
//! reference's provider session does: the first turn sends `initialize` and
//! `session/new`, later turns only `session/prompt`. A Stop kills the
//! process (Grok's flavor sends no `session/cancel`), so the next turn
//! starts a new one that loads the session again; after a server restart the
//! new process loads it without a `session/new`.
//!
//! Each `session/update` becomes turn items: thought and message chunks
//! stream as reasoning and assistant text (the first chunk at once, then a
//! snapshot at most every 50 ms), and each tool call is one item whose type
//! follows its ACP kind. A `session/request_permission` the runtime mode does
//! not answer goes to the user as a runtime request.
use super::{cancelled, next_response, program, requests, Cli, Launch, Sink, TurnEnd};
use crate::thread_projection::uri_component;
use crate::workspace::provider::{
    ProviderFailure, ProviderItem, ProviderTurn, RuntimeResponse, ThreadDisposition, TurnStatus,
    ACP_REGISTRY, ANTIGRAVITY, GROK,
};
use crate::workspace::{ProviderEvent, TurnSpec};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};
use tokio::time::Instant;

/// How long `initialize`, `session/new` and `session/load` may take.
const STARTUP_TIMEOUT: Duration = Duration::from_secs(90);
/// Grok may never answer `session/load`: it is ready once its replay went
/// quiet for this long.
const LOAD_REPLAY_IDLE: Duration = Duration::from_secs(2);
/// How often streamed text is shown again while it grows.
const TEXT_FLUSH_INTERVAL: Duration = Duration::from_millis(50);
/// How long an agent stays open after its thread's last turn.
const IDLE_RELEASE_DELAY: Duration = Duration::from_secs(30 * 60);
/// How long `session/close` may take when an agent is released.
const CLOSE_TIMEOUT: Duration = Duration::from_secs(10);
/// The JSON-RPC error for a method the client does not offer.
const METHOD_NOT_FOUND: i64 = -32601;
/// The error an agent answers a session request with until it is signed in.
const AUTH_REQUIRED: i64 = -32000;

/// agy's request for a browser sign-in, and what a turn says of it.
const AGY_SIGN_IN_LINE: &str = "Open the following link to authenticate the ACP server: ";
const AGY_SIGN_IN: &str = "Sign in to Antigravity in Settings before you continue.";
/// The largest file agy may read through T3 (8 MiB).
const AGY_READ_LIMIT: u64 = 8 * 1024 * 1024;

/// How long a Stop waits for an agent to answer its cancelled prompt.
const STOP_CONFIRM_TIMEOUT: Duration = Duration::from_secs(10);

/// Which flavor of the reference's ACP adapter a turn runs in.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Flavor {
    /// Grok (`provider-grok`): its launch modes, `_meta` and extensions.
    Grok,
    /// An ACP Registry agent (`provider-acp-registry`), in the generic
    /// flavor: launched as resolved, cancelled with `session/cancel`.
    Registry,
    /// Antigravity's `agy acp` (`AntigravityAdapterV2`): the generic flavor
    /// with eager sign-in, client file reads, `session/resume`, its tool
    /// shapes, its approval labels and questions.
    Antigravity,
}

impl Flavor {
    fn of(provider: &ProviderTurn) -> Self {
        match provider.driver.as_str() {
            ACP_REGISTRY => Self::Registry,
            ANTIGRAVITY => Self::Antigravity,
            _ => Self::Grok,
        }
    }
    fn driver(self) -> &'static str {
        match self {
            Self::Grok => GROK,
            Self::Registry => ACP_REGISTRY,
            Self::Antigravity => ANTIGRAVITY,
        }
    }
    /// The harness the runtime notes name (`runtimeHarness ?? driver`).
    fn harness(self) -> &'static str {
        match self {
            Self::Grok => "Grok",
            Self::Registry => ACP_REGISTRY,
            Self::Antigravity => "Antigravity",
        }
    }
    /// The agent's command line.
    fn args(self, provider: &ProviderTurn) -> Vec<String> {
        match self {
            Self::Grok => {
                let mut args = grok_args(provider.runtime_mode());
                args.extend(provider.launch_args.iter().cloned());
                args
            }
            Self::Registry | Self::Antigravity => provider.launch_args.clone(),
        }
    }
}

/// The Grok launch for a runtime mode (`grokAcpSpawnArgs`): its flags
/// override Grok's own config, so Supervised cannot inherit always-approve.
fn grok_args(runtime_mode: &str) -> Vec<String> {
    let args: &[&str] = match runtime_mode {
        "auto" => &["--permission-mode", "auto", "agent", "stdio"],
        "full-access" => &["agent", "--always-approve", "stdio"],
        _ => &["--permission-mode", "default", "agent", "stdio"],
    };
    args.iter().map(|arg| (*arg).to_owned()).collect()
}

/// The thread's agent processes, by app thread.
#[derive(Default)]
pub(crate) struct Sessions {
    live: Mutex<HashMap<String, Live>>,
    /// Threads whose agent a Stop killed: the session the next turn loads in
    /// a new process, after a `session/new` of its own, as the reference's
    /// restarted runtime does.
    stopped: Mutex<HashMap<String, Stopped>>,
}

/// An agent kept for its thread's next turn.
struct Live {
    agent: Agent,
    idle: tokio::task::JoinHandle<()>,
}

/// What a Stop leaves of a thread's agent for its next turn.
#[derive(Clone)]
struct Stopped {
    session: String,
    scoped: bool,
}

impl Sessions {
    fn live(&self) -> std::sync::MutexGuard<'_, HashMap<String, Live>> {
        self.live.lock().unwrap_or_else(|e| e.into_inner())
    }
    fn stopped(&self) -> std::sync::MutexGuard<'_, HashMap<String, Stopped>> {
        self.stopped.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// The thread's running agent, when it was launched as `launch` and on
    /// `session`; any other is closed.
    async fn take(&self, thread: &str, launch: &str, session: Option<&str>) -> Option<Agent> {
        let live = self.live().remove(thread)?;
        live.idle.abort();
        let mut agent = live.agent;
        if agent.launch == launch
            && agent.cli.alive()
            && session.is_none_or(|session| session == agent.session)
        {
            return Some(agent);
        }
        agent.close().await;
        None
    }

    /// Keep `agent` for the thread's next turn; it closes after
    /// [`IDLE_RELEASE_DELAY`] without one.
    fn keep(self: &Arc<Self>, thread: String, agent: Agent) {
        let sessions: Weak<Self> = Arc::downgrade(self);
        let key = thread.clone();
        let idle = tokio::spawn(async move {
            tokio::time::sleep(IDLE_RELEASE_DELAY).await;
            if let Some(sessions) = sessions.upgrade() {
                let live = sessions.live().remove(&key);
                if let Some(live) = live {
                    live.agent.close().await;
                }
            }
        });
        if let Some(previous) = self.live().insert(thread, Live { agent, idle }) {
            previous.idle.abort();
            tokio::spawn(previous.agent.stop());
        }
    }

    /// Close the agent holding the native `session`, if one runs (a
    /// detach); the thread's next turn starts a new one.
    pub(crate) fn close(&self, session: &str) {
        self.stopped()
            .retain(|_, stopped| stopped.session != session);
        let mut live = self.live();
        let threads: Vec<String> = live
            .iter()
            .filter(|(_, live)| live.agent.session == session)
            .map(|(thread, _)| thread.clone())
            .collect();
        for thread in threads {
            if let Some(live) = live.remove(&thread) {
                live.idle.abort();
                tokio::spawn(live.agent.stop());
            }
        }
    }

    /// Close every agent (the server is shutting down).
    pub(crate) async fn shutdown(&self) {
        let all: Vec<Live> = self.live().drain().map(|(_, live)| live).collect();
        for live in all {
            live.idle.abort();
            live.agent.close().await;
        }
    }
}

/// What the instructions a prompt carries were last given for, the
/// reference's `T3AcpInstructionState`.
#[derive(Clone, PartialEq)]
struct InstructionState {
    interaction_mode: String,
    has_mcp: bool,
    has_device_tools: bool,
}

/// One agent process and its ACP session.
struct Agent {
    cli: Cli,
    flavor: Flavor,
    /// What it was launched with: a turn launched otherwise starts another.
    launch: String,
    next_id: u64,
    /// The ACP session its turns run in.
    session: String,
    /// Whether native item ids are scoped to the provider instance (a
    /// session the adapter created; one it loaded after a server restart
    /// keeps the plain ids).
    scoped: bool,
    /// The agent's `sessionCapabilities.close`.
    can_close: bool,
    /// The agent's `authMethods` ids.
    auth_methods: Vec<String>,
    /// The method the instance names to sign in with, if any.
    auth_method: String,
    /// The session's current model (`models.currentModelId`).
    current_model: Option<String>,
    /// The model and effort last applied.
    configured: Option<(String, Option<String>)>,
    /// The session's models (`models.availableModels`).
    available_models: Vec<String>,
    /// The session's config options (`configOptions`).
    config_options: Vec<Value>,
    /// Prompts sent by this process (`t3-xai-prompt-<n>`).
    prompts: u64,
    /// The instructions the session has, once a turn delivered them.
    instructions: Option<InstructionState>,
    /// The runtime notes the session has.
    notes: Option<String>,
    /// The session's mode (`modes.currentModeId`) and its modes' ids.
    current_mode: Option<String>,
    modes: Vec<String>,
    /// Antigravity: the process's own temporary folder (`run-XXXXXX`),
    /// removed with the agent.
    _run_temp: Option<tempfile::TempDir>,
    /// The installed CLI's version, which names the client to it.
    installed_version: Option<String>,
    /// The version the agent reports (`agentInfo.version`).
    agent_version: Option<String>,
    /// The agent takes T3's pull request instructions in the session's
    /// system prompt (`_meta.antigravity.systemPrompt.append`).
    system_prompt_append: bool,
    /// The agent reaches MCP servers over HTTP (`mcpCapabilities.http`).
    mcp_http: bool,
}

impl Agent {
    async fn spawn(
        spec: &TurnSpec,
        provider: &ProviderTurn,
        launch: String,
    ) -> std::io::Result<Self> {
        let flavor = Flavor::of(provider);
        let (default, home_var, extra_env) = match flavor {
            Flavor::Grok => (
                "grok",
                "GROK_HOME",
                vec![("GROK_OAUTH2_REFERRER".into(), "t3code".into())],
            ),
            // The generic flavor gives the agent its thread and T3's MCP
            // endpoint (`processEnvironment`), for the terminal fallback.
            Flavor::Registry | Flavor::Antigravity => ("", "", process_environment(provider)),
        };
        let mut extra_env = extra_env;
        // Each Antigravity process gets a temporary folder of its own.
        let run_temp = match provider.antigravity.as_ref() {
            Some(launch) if flavor == Flavor::Antigravity => {
                let parent = std::path::Path::new(&launch.run_temp_parent);
                std::fs::create_dir_all(parent)?;
                let temp = tempfile::Builder::new()
                    .prefix("run-")
                    .rand_bytes(6)
                    .tempdir_in(parent)
                    .map_err(|_| {
                        std::io::Error::other(
                            "Could not create an Antigravity runtime temp directory.",
                        )
                    })?;
                extra_env.push(("TMPDIR".into(), temp.path().display().to_string()));
                Some(temp)
            }
            _ => None,
        };
        if flavor != Flavor::Grok && provider.binary_path.trim().is_empty() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "Local ACP provider requires an executable.",
            ));
        }
        let cli = Cli::spawn(
            spec,
            Launch {
                program: program(&provider.binary_path, default),
                args: flavor.args(provider),
                home_var,
                home: if flavor == Flavor::Grok {
                    &provider.home_path
                } else {
                    ""
                },
                extra_env,
            },
        )?;
        Ok(Self {
            cli,
            flavor,
            auth_method: provider.auth_method.trim().to_owned(),
            launch,
            next_id: 1,
            session: String::new(),
            scoped: true,
            can_close: false,
            auth_methods: Vec::new(),
            current_model: None,
            configured: None,
            available_models: Vec::new(),
            config_options: Vec::new(),
            prompts: 0,
            instructions: None,
            notes: None,
            current_mode: None,
            modes: Vec::new(),
            _run_temp: run_temp,
            installed_version: provider
                .antigravity
                .as_ref()
                .map(|launch| launch.version.clone())
                .filter(|version| !version.is_empty()),
            agent_version: None,
            system_prompt_append: false,
            mcp_http: false,
        })
    }

    async fn send(&mut self, method: &str, params: Value) -> std::io::Result<u64> {
        let id = self.next_id;
        self.next_id += 1;
        self.cli
            .write(&json!({"jsonrpc": "2.0", "id": id, "method": method, "params": params}))
            .await?;
        Ok(id)
    }

    async fn reply(&mut self, id: &Value, result: Value) {
        let _ = self
            .cli
            .write(&json!({"jsonrpc": "2.0", "id": id, "result": result}))
            .await;
    }

    async fn reply_error(&mut self, id: &Value, code: i64, message: &str) {
        let _ = self
            .cli
            .write(
                &json!({"jsonrpc": "2.0", "id": id, "error": {"code": code, "message": message}}),
            )
            .await;
    }

    /// Answer a request the agent makes outside a turn: a permission is
    /// cancelled, anything else is not offered.
    async fn refuse(&mut self, message: &Value) {
        let id = &message["id"];
        if message["method"] == "session/request_permission" {
            self.reply(id, json!({"outcome": {"outcome": "cancelled"}}))
                .await;
        } else {
            self.reply_error(id, METHOD_NOT_FOUND, "Method not found")
                .await;
        }
    }

    /// Send a request and wait for its answer, answering what the agent asks
    /// meanwhile. With `replay`, the answer may also be the session's replay
    /// going quiet ([`LOAD_REPLAY_IDLE`]).
    async fn request(
        &mut self,
        method: &str,
        params: Value,
        replay: bool,
    ) -> Result<Value, AcpError> {
        let id = self
            .send(method, params)
            .await
            .map_err(|_| AcpError::Closed)?;
        let deadline = Instant::now() + STARTUP_TIMEOUT;
        let mut replayed: Option<Instant> = None;
        loop {
            let idle = replayed.map(|at| at + LOAD_REPLAY_IDLE);
            let line = tokio::select! {
                line = self.cli.next_line() => line,
                _ = tokio::time::sleep_until(deadline) => return Err(AcpError::Timeout(method.to_owned())),
                _ = super::sleep_until(idle) => {
                    return Ok(json!({"_meta": {"t3SessionLoadReady": "replay_idle"}}));
                }
            };
            let Some(line) = line else {
                return Err(AcpError::Closed);
            };
            let Ok(message) = serde_json::from_str::<Value>(&line) else {
                continue;
            };
            if message.get("method").is_some() {
                if message.get("id").is_some() {
                    self.refuse(&message).await;
                } else if replay && message["method"] == "session/update" {
                    replayed = Some(Instant::now());
                }
                continue;
            }
            if message["id"].as_u64() != Some(id) {
                continue;
            }
            if let Some(error) = message.get("error") {
                return Err(AcpError::Rpc {
                    code: error["code"].as_i64().unwrap_or(-32603),
                    message: error["message"].as_str().unwrap_or_default().to_owned(),
                    data: error.get("data").cloned().unwrap_or(Value::Null),
                });
            }
            return Ok(message.get("result").cloned().unwrap_or(Value::Null));
        }
    }

    /// `initialize`, as the reference's runtime negotiates it.
    async fn initialize(&mut self) -> Result<(), AcpError> {
        // A registry agent may also ask the user to sign in at a URL.
        let elicitation = match self.flavor {
            Flavor::Grok | Flavor::Antigravity => json!({"form": {}}),
            Flavor::Registry => json!({"form": {}, "url": {}}),
        };
        let mut params = json!({
            "protocolVersion": 2,
            "info": {"name": "t3-code", "version": "0.0.0"},
            "capabilities": {"elicitation": elicitation},
            "clientCapabilities": {
                "fs": {"readTextFile": false, "writeTextFile": false},
                "terminal": false,
                "session": {"notices": {}, "compaction": {}},
                "elicitation": elicitation,
            },
            "clientInfo": {"name": "t3-code", "version": "0.0.0"},
        });
        if self.flavor == Flavor::Grok {
            // Grok then asks about auto-mode refusals instead of denying them.
            params["_meta"] = json!({"clientType": "extension"});
        }
        if self.flavor == Flavor::Antigravity {
            // agy reads files through T3 (`clientFileReads`), and from 1.5.1
            // on is told it runs in T3 Code (`antigravityClientInfo`).
            params["clientCapabilities"]["fs"]["readTextFile"] = json!(true);
            let client = if version_at_least(self.installed_version.as_deref(), (1, 5, 1)) {
                json!({"name": "t3-code", "title": "T3 Code",
                    "version": crate::server_runtime::SERVER_VERSION})
            } else {
                json!({"name": "zed", "version": "0.0.0"})
            };
            params["clientInfo"] = client.clone();
            params["info"] = client;
        }
        let result = self.request("initialize", params, false).await?;
        let capabilities = &result["agentCapabilities"];
        self.agent_version = result["agentInfo"]["version"]
            .as_str()
            .filter(|version| !version.is_empty())
            .map(str::to_owned);
        self.system_prompt_append =
            capabilities["_meta"]["antigravity"]["systemPrompt"]["append"] == true;
        self.mcp_http = capabilities["mcpCapabilities"]["http"] == true;
        self.can_close = !result["agentCapabilities"]["sessionCapabilities"]["close"].is_null();
        self.auth_methods = result["authMethods"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|method| method["id"].as_str().map(str::to_owned))
            .collect();
        Ok(())
    }

    /// A session request, signing in once when the agent asks for it.
    async fn session_request(
        &mut self,
        spec: &TurnSpec,
        method: &str,
        params: Value,
    ) -> Result<Value, AcpError> {
        let replay = method == "session/load";
        match self.request(method, params.clone(), replay).await {
            Err(AcpError::Rpc {
                code: AUTH_REQUIRED,
                ..
            }) => {
                let xai_key = spec
                    .env
                    .iter()
                    .any(|(name, value)| name == "XAI_API_KEY" && !value.trim().is_empty())
                    || std::env::var("XAI_API_KEY").is_ok_and(|key| !key.trim().is_empty());
                let method_id = match self.flavor {
                    Flavor::Grok if xai_key => "xai.api_key".to_owned(),
                    Flavor::Grok => "cached_token".to_owned(),
                    // The instance's method, else the agent's first.
                    Flavor::Registry | Flavor::Antigravity if !self.auth_method.is_empty() => {
                        self.auth_method.clone()
                    }
                    Flavor::Registry | Flavor::Antigravity => {
                        self.auth_methods.first().cloned().unwrap_or_default()
                    }
                };
                let method_id = method_id.as_str();
                if !self.auth_methods.iter().any(|id| id == method_id) {
                    return Err(AcpError::Startup(format!(
                        "ACP agent did not advertise configured authentication method \"{method_id}\""
                    )));
                }
                self.request("authenticate", json!({"methodId": method_id}), false)
                    .await?;
                self.request(method, params, replay).await
            }
            other => other,
        }
    }

    /// Apply the turn's model and effort (`applyGrokSessionModelSelection`):
    /// `grok-build` keeps the session's model, and any effort is applied.
    async fn configure(&mut self, provider: &ProviderTurn) -> Result<(), AcpError> {
        if self.flavor == Flavor::Registry {
            return self.configure_generic(provider).await;
        }
        if self.flavor == Flavor::Antigravity {
            return self.configure_antigravity(provider).await;
        }
        let requested = match provider.model.trim() {
            "" => "grok-build",
            model => model,
        };
        let target = if requested == "grok-build" {
            self.current_model
                .clone()
                .unwrap_or_else(|| requested.to_owned())
        } else {
            requested.to_owned()
        };
        let effort = provider
            .reasoning_effort
            .clone()
            .filter(|effort| !effort.trim().is_empty());
        let wanted = (target.clone(), effort.clone());
        if self.configured.as_ref() == Some(&wanted) && effort.is_none() {
            return Ok(());
        }
        if self.current_model.as_deref() != Some(target.as_str()) || effort.is_some() {
            let mut params = json!({"sessionId": self.session, "modelId": target});
            if let Some(effort) = &effort {
                params["_meta"] = json!({"reasoningEffort": effort});
            }
            let applied = match self.request("session/set_model", params, false).await {
                Err(AcpError::Rpc { .. }) if effort.is_some() => {
                    self.request(
                        "session/set_model",
                        json!({"sessionId": self.session, "modelId": target}),
                        false,
                    )
                    .await
                }
                other => other,
            };
            if let Err(error) = applied {
                return Err(match error {
                    AcpError::Rpc { code: -32602, .. } => AcpError::Startup(format!(
                        "The agent did not accept the model \"{target}\". Choose another model for this provider, then retry."
                    )),
                    other => other,
                });
            }
            self.current_model = Some(target);
        }
        self.configured = Some(wanted);
        Ok(())
    }

    /// The generic flavor's model selection (`configureSession`): the
    /// session's model option when it offers the model, else
    /// `session/set_model` when its models list it; a model neither offers
    /// is left to the agent.
    async fn configure_generic(&mut self, provider: &ProviderTurn) -> Result<(), AcpError> {
        let requested = provider.model.trim().to_owned();
        if requested.is_empty() || self.configured.as_ref().map(|c| &c.0) == Some(&requested) {
            return Ok(());
        }
        let option = self
            .config_options
            .iter()
            .find(|option| option["category"] == "model" || option["id"] == "model");
        let rejected = |error: AcpError| {
            match error {
            AcpError::Rpc { code: -32602, .. } => AcpError::Startup(format!(
                "The agent did not accept the model \"{requested}\". Choose another model for this provider, then retry."
            )),
            other => other,
        }
        };
        if let Some(option) = option.filter(|option| option["type"] == "select").cloned() {
            let offered = option["options"]
                .as_array()
                .into_iter()
                .flatten()
                .any(|choice| {
                    choice["value"] == requested.as_str()
                        || choice["options"]
                            .as_array()
                            .into_iter()
                            .flatten()
                            .any(|nested| nested["value"] == requested.as_str())
                });
            if offered && option["currentValue"] != requested.as_str() {
                let params = json!({"sessionId": self.session, "configId": option["id"],
                    "value": requested});
                self.request("session/set_config_option", params, false)
                    .await
                    .map_err(rejected)?;
            }
        } else if self
            .available_models
            .iter()
            .any(|model| model == &requested)
            && self.current_model.as_deref() != Some(requested.as_str())
        {
            let params = json!({"sessionId": self.session, "modelId": requested});
            self.request("session/set_model", params, false)
                .await
                .map_err(rejected)?;
            self.current_model = Some(requested.clone());
        }
        self.configured = Some((requested, None));
        Ok(())
    }

    /// Antigravity's model and mode (`applyModelSelection`,
    /// `antigravitySessionMode`): `antigravity-default` keeps the session's
    /// model, another is set as the model option; the mode stays `default`
    /// (agy refuses its own full-access modes, so T3 answers its requests),
    /// or `auto` when the thread is in auto and the session offers it.
    async fn configure_antigravity(&mut self, provider: &ProviderTurn) -> Result<(), AcpError> {
        let requested = match provider.model.trim() {
            "" | "antigravity-default" => None,
            model => Some(model.to_owned()),
        };
        let model_option = self
            .config_options
            .iter()
            .find(|option| option["category"] == "model" && option["type"] == "select")
            .cloned();
        if let Some(model) = requested {
            let current = model_option
                .as_ref()
                .and_then(|option| option["currentValue"].as_str().map(str::to_owned))
                .or_else(|| self.current_model.clone());
            if current.as_deref() != Some(model.as_str())
                && self.configured.as_ref().map(|c| &c.0) != Some(&model)
            {
                let config = model_option
                    .as_ref()
                    .and_then(|option| option["id"].as_str())
                    .unwrap_or("model")
                    .to_owned();
                let params = json!({"sessionId": self.session, "configId": config, "value": model});
                self.request("session/set_config_option", params, false)
                    .await
                    .map_err(|error| match error {
                        AcpError::Rpc { code: -32602, .. } => AcpError::Startup(format!(
                            "Antigravity model '{model}' is unavailable for this Google account. Select an available model."
                        )),
                        other => other,
                    })?;
                self.current_model = Some(model.clone());
            }
            self.configured = Some((model, None));
        }
        let mode_option = self
            .config_options
            .iter()
            .find(|option| option["category"] == "mode" && option["type"] == "select")
            .cloned();
        let offers_auto = mode_option.as_ref().is_some_and(|option| {
            option["options"]
                .as_array()
                .into_iter()
                .flatten()
                .any(|choice| choice["value"] == "auto")
        }) || self.modes.iter().any(|mode| mode == "auto");
        let wanted = if provider.runtime_mode() == "auto" && offers_auto {
            "auto"
        } else {
            "default"
        };
        let current = mode_option
            .as_ref()
            .and_then(|option| option["currentValue"].as_str().map(str::to_owned))
            .or_else(|| self.current_mode.clone());
        if current.as_deref() == Some(wanted) {
            return Ok(());
        }
        let result = match &mode_option {
            Some(option) => {
                let params = json!({"sessionId": self.session, "configId": option["id"],
                    "value": wanted});
                self.request("session/set_config_option", params, false)
                    .await
            }
            None => {
                let params = json!({"sessionId": self.session, "modeId": wanted});
                self.request("session/set_mode", params, false).await
            }
        };
        result?;
        self.current_mode = Some(wanted.to_owned());
        Ok(())
    }

    /// Note a session's setup result: its models and config options.
    fn note_setup(&mut self, setup: &Value) {
        if let Some(model) = current_model(setup) {
            self.current_model = Some(model);
        }
        if let Some(models) = setup["models"]["availableModels"].as_array() {
            self.available_models = models
                .iter()
                .filter_map(|model| model["modelId"].as_str().map(str::to_owned))
                .collect();
        }
        if let Some(options) = setup["configOptions"].as_array() {
            self.config_options = options.clone();
        }
        if let Some(mode) = setup["modes"]["currentModeId"].as_str() {
            self.current_mode = Some(mode.to_owned());
        }
        if let Some(modes) = setup["modes"]["availableModes"].as_array() {
            self.modes = modes
                .iter()
                .filter_map(|mode| mode["id"].as_str().map(str::to_owned))
                .collect();
        }
    }

    /// Close the session (`session/close`) and the process.
    async fn close(mut self) {
        if self.can_close && !self.session.is_empty() {
            let session = self.session.clone();
            let _ = tokio::time::timeout(
                CLOSE_TIMEOUT,
                self.request("session/close", json!({"sessionId": session}), false),
            )
            .await;
        }
        self.cli.close().await;
    }

    async fn stop(self) {
        self.cli.stop().await;
    }
}

/// Why a request to the agent failed.
#[derive(Debug)]
enum AcpError {
    /// The agent's process closed its output.
    Closed,
    Timeout(String),
    Rpc {
        code: i64,
        message: String,
        data: Value,
    },
    Startup(String),
}

impl AcpError {
    /// The turn failure this error is (Grok's `promptFailure`).
    fn failure(&self) -> ProviderFailure {
        match self {
            Self::Closed => ProviderFailure::new(
                "provider_error",
                "The provider event stream closed unexpectedly. Retry the turn; if it keeps failing, check the provider and server logs.",
            ),
            Self::Timeout(method) => ProviderFailure::new(
                "provider_error",
                format!("{method} timed out waiting for RPC response"),
            ),
            Self::Rpc { code, message, data } => {
                let text = data["agentResult"]
                    .as_str()
                    .filter(|text| !text.trim().is_empty())
                    .unwrap_or(message);
                let class = if *code == -32003 {
                    "usage_limit"
                } else {
                    "provider_error"
                };
                let mut failure = ProviderFailure::new(class, text);
                failure.code = Some(code.to_string());
                failure
            }
            Self::Startup(message) => ProviderFailure::new("provider_error", message.clone()),
        }
    }
}

/// The T3 MCP server the agent is given: this executable as a stdio bridge
/// to the thread's MCP endpoint (`acp-mcp-bridge`).
fn mcp_servers(provider: &ProviderTurn) -> Value {
    let Some(mcp) = &provider.mcp else {
        return json!([]);
    };
    let command = std::env::current_exe()
        .map(|path| path.display().to_string())
        .unwrap_or_else(|_| "t3code-server".into());
    json!([{
        "name": "t3-code",
        "command": command,
        "args": ["acp-mcp-bridge"],
        "env": [
            {"name": "ELECTRON_RUN_AS_NODE", "value": "1"},
            {"name": "T3_ACP_MCP_ENDPOINT", "value": mcp.url},
            {"name": "T3_ACP_MCP_AUTHORIZATION", "value": mcp.authorization},
        ],
    }])
}

/// What the generic flavor adds to its agent's environment
/// (`processEnvironment`): the thread, so a tool its shell runs can find it,
/// and T3's MCP endpoint with this executable for the terminal fallback
/// (`acp-mcp-call`).
fn process_environment(provider: &ProviderTurn) -> Vec<(String, String)> {
    let Some(mcp) = &provider.mcp else {
        return Vec::new();
    };
    let exe = std::env::current_exe()
        .map(|path| path.display().to_string())
        .unwrap_or_else(|_| "t3code-server".into());
    vec![
        ("T3CODE_THREAD_ID".into(), provider.app_thread_id.clone()),
        ("T3_ACP_MCP_ENDPOINT".into(), mcp.url.clone()),
        ("T3_ACP_MCP_AUTHORIZATION".into(), mcp.authorization.clone()),
        ("T3_ACP_MCP_NODE".into(), exe),
    ]
}

/// What a launch is keyed by: a turn launched otherwise needs a new agent.
fn launch_key(spec: &TurnSpec, provider: &ProviderTurn) -> String {
    json!([
        provider.binary_path,
        Flavor::of(provider).args(provider),
        provider.home_path,
        spec.env,
        spec.cwd,
        provider
            .mcp
            .as_ref()
            .map(|mcp| (&mcp.url, &mcp.authorization)),
    ])
    .to_string()
}

pub(super) async fn run(
    spec: TurnSpec,
    provider: ProviderTurn,
    cancel: oneshot::Receiver<()>,
    mut responses: mpsc::UnboundedReceiver<RuntimeResponse>,
    sink: Sink,
    sessions: Arc<Sessions>,
) {
    let mut cancel = Some(cancel);
    let thread = provider.app_thread_id.clone();
    let launch = launch_key(&spec, &provider);
    let resume = provider.resume.clone().filter(|id| !id.is_empty());
    let agent = tokio::select! {
        agent = open(&spec, &provider, &sessions, &thread, launch, resume.as_deref()) => agent,
        _ = cancelled(&mut cancel) => {
            return sink.finish(TurnEnd::interrupted(true));
        }
    };
    let (mut agent, resumed) = match agent {
        Ok(opened) => opened,
        Err(error) => {
            return sink.finish(TurnEnd::failed(
                error.failure(),
                ThreadDisposition::Reusable,
            ));
        }
    };
    let mut turn = Turn::new(&spec, &provider, &sink, &agent, &mut responses);
    let outcome = turn.run(&mut agent, &mut cancel, resumed).await;
    match outcome {
        Ended::Settled(end) => {
            sink.finish(end);
            sessions.keep(thread, agent);
        }
        Ended::Stopped => {
            sessions.stopped().insert(
                thread,
                Stopped {
                    session: agent.session.clone(),
                    scoped: agent.scoped,
                },
            );
            sink.finish(TurnEnd::interrupted(true));
            agent.stop().await;
        }
        Ended::Detached => {
            sink.finish(TurnEnd::failed(
                super::detached_failure(),
                ThreadDisposition::Reusable,
            ));
            agent.stop().await;
        }
        Ended::Crashed(failure) => {
            sink.finish(TurnEnd::failed(failure, ThreadDisposition::Reusable));
            agent.stop().await;
        }
    }
}

/// The thread's agent for this turn: its running one, or a new one that
/// creates or loads the session. With whether the session was loaded.
async fn open(
    spec: &TurnSpec,
    provider: &ProviderTurn,
    sessions: &Sessions,
    thread: &str,
    launch: String,
    resume: Option<&str>,
) -> Result<(Agent, bool), AcpError> {
    if let Some(agent) = sessions.take(thread, &launch, resume).await {
        return Ok((agent, false));
    }
    // What a Stop left, when this turn resumes that same session.
    let stopped = sessions
        .stopped()
        .remove(thread)
        .filter(|stopped| resume == Some(stopped.session.as_str()));
    if let Some(error) = provider
        .antigravity
        .as_ref()
        .map(|launch| launch.launch_error.as_str())
        .filter(|error| !error.is_empty())
    {
        return Err(AcpError::Startup(error.to_owned()));
    }
    let mut agent = Agent::spawn(spec, provider, launch)
        .await
        .map_err(|error| AcpError::Startup(error.to_string()))?;
    let started = start(spec, provider, &mut agent, stopped, resume).await;
    match started {
        Ok(resumed) => Ok((agent, resumed)),
        Err(error) => {
            agent.stop().await;
            Err(error)
        }
    }
}

async fn start(
    spec: &TurnSpec,
    provider: &ProviderTurn,
    agent: &mut Agent,
    stopped: Option<Stopped>,
    resume: Option<&str>,
) -> Result<bool, AcpError> {
    agent.initialize().await?;
    let mut mcp = mcp_servers(provider);
    // The session's setup carries T3's pull request instructions for an
    // agent that takes them in its system prompt (`antigravitySessionMeta`).
    let mut meta = None;
    if agent.flavor == Flavor::Antigravity {
        // Eagerly, with the configured method: agy takes ones it does not list.
        let method = if agent.auth_method.is_empty() {
            "oauth-personal".to_owned()
        } else {
            agent.auth_method.clone()
        };
        agent
            .request("authenticate", json!({"methodId": method}), false)
            .await?;
        if agent.system_prompt_append {
            meta = Some(json!({"antigravity": {"systemPrompt": {
                "append": crate::agent_instructions::pull_request_linking()}}}));
        }
        if agent.mcp_http {
            if let Some(server) = &provider.mcp {
                mcp = json!([{"type": "http", "name": "t3-code", "url": server.url,
                    "headers": [{"name": "Authorization", "value": server.authorization}]}]);
            }
        }
    }
    let with_meta = |mut params: Value| {
        if let Some(meta) = &meta {
            params["_meta"] = meta.clone();
        }
        params
    };
    let new_session = |agent: &Agent| {
        let _ = agent;
        with_meta(json!({"cwd": spec.cwd, "mcpServers": mcp}))
    };
    let resumed = match (stopped, resume) {
        // After a Stop, the restarted runtime starts a session of its own,
        // then loads the thread's.
        (Some(stopped), _) => {
            let created = agent
                .session_request(spec, "session/new", new_session(agent))
                .await?;
            agent.note_setup(&created);
            agent.session = stopped.session.clone();
            agent.scoped = stopped.scoped;
            let loaded = agent
                .session_request(
                    spec,
                    "session/load",
                    with_meta(
                        json!({"sessionId": stopped.session, "cwd": spec.cwd, "mcpServers": mcp}),
                    ),
                )
                .await?;
            agent.note_setup(&loaded);
            true
        }
        // After a server restart, the session is loaded straight away, with
        // the plain item ids of a session the adapter did not create.
        (None, Some(session)) => {
            agent.session = session.to_owned();
            agent.scoped = false;
            // agy resumes without replaying its history (`resumeMethod`).
            let method = if agent.flavor == Flavor::Antigravity {
                "session/resume"
            } else {
                "session/load"
            };
            let loaded = agent
                .session_request(
                    spec,
                    method,
                    with_meta(json!({"sessionId": session, "cwd": spec.cwd, "mcpServers": mcp})),
                )
                .await?;
            agent.note_setup(&loaded);
            true
        }
        (None, None) => {
            let created = agent
                .session_request(spec, "session/new", new_session(agent))
                .await?;
            agent.session = created["sessionId"].as_str().unwrap_or_default().to_owned();
            if agent.session.is_empty() {
                return Err(AcpError::Startup(
                    "ACP session/new returned no sessionId".into(),
                ));
            }
            agent.note_setup(&created);
            false
        }
    };
    agent.configure(provider).await?;
    Ok(resumed)
}

/// Whether `version` (`1.5.1`, `1.6.2-beta`) is at least `at`.
fn version_at_least(version: Option<&str>, at: (u64, u64, u64)) -> bool {
    let Some(version) = version else {
        return false;
    };
    let parts: Vec<u64> = version
        .trim()
        .trim_start_matches('v')
        .split('.')
        .take(3)
        .filter_map(|part| {
            part.chars()
                .take_while(char::is_ascii_digit)
                .collect::<String>()
                .parse()
                .ok()
        })
        .collect();
    parts.len() == 3 && (parts[0], parts[1], parts[2]) >= at
}

/// A failed Antigravity prompt (no `promptFailure` of its own): the
/// curated sign-in text, else the generic one.
fn antigravity_prompt_failure(error: &AcpError) -> ProviderFailure {
    match error {
        AcpError::Rpc {
            code: AUTH_REQUIRED,
            ..
        } => ProviderFailure::new(
            "provider_error",
            "The provider needs you to sign in. Sign in from its provider settings or with its own CLI or app, then retry the turn.",
        ),
        _ => ProviderFailure::new("provider_error", "Provider turn failed."),
    }
}

/// `path` with every link resolved: its real path, or for a missing path
/// its deepest existing ancestor's real path with the rest appended.
fn resolve_existing(path: &std::path::Path) -> Option<std::path::PathBuf> {
    if !path.is_absolute() {
        return None;
    }
    let mut missing = Vec::new();
    let mut at = path.to_path_buf();
    loop {
        if let Ok(real) = std::fs::canonicalize(&at) {
            return Some(
                missing
                    .into_iter()
                    .rev()
                    .fold(real, |path, part| path.join(part)),
            );
        }
        let name = at.file_name()?.to_owned();
        missing.push(name);
        at = at.parent()?.to_path_buf();
    }
}

/// agy's tool shapes (`normalizeAntigravityToolCall`): its command line,
/// working folder, combined output and exit code become the generic ones,
/// and a call naming a command is a command.
fn normalize_antigravity(tool: &mut Tool) {
    let text = |value: &Value, keys: &[&str]| {
        keys.iter()
            .find_map(|key| value[*key].as_str().filter(|v| !v.trim().is_empty()))
            .map(str::to_owned)
    };
    let command = text(
        &tool.raw_input,
        &["CommandLine", "command_line", "commandLine", "command"],
    )
    .or_else(|| text(&tool.raw_output, &["commandLine", "command_line"]));
    if let Some(command) = command {
        if tool.raw_input.is_null() {
            tool.raw_input = json!({});
        }
        if tool.raw_input.is_object() {
            tool.raw_input["command"] = json!(command.trim());
        }
        if tool.kind.is_none() {
            tool.kind = Some("execute".into());
        }
    }
    if let Some(output) = text(&tool.raw_output, &["combinedOutput", "combined_output"]) {
        tool.raw_output["output"] = json!(output);
    }
}

fn current_model(setup: &Value) -> Option<String> {
    setup["models"]["currentModelId"]
        .as_str()
        .map(str::trim)
        .filter(|model| !model.is_empty())
        .map(str::to_owned)
}

/// How a turn ended, for what becomes of its agent.
enum Ended {
    /// The prompt settled; the agent serves the thread's next turn.
    Settled(TurnEnd),
    /// The user stopped it: the agent is killed.
    Stopped,
    /// A thread change detached the session.
    Detached,
    /// The agent's process closed.
    Crashed(ProviderFailure),
}

/// The open segment of one text stream.
struct Segment {
    id: String,
    native: String,
    text: String,
    dirty: bool,
}

/// A tool call as its updates merged it.
#[derive(Default, Clone)]
struct Tool {
    kind: Option<String>,
    title: Option<String>,
    /// `pending`, `inProgress`, `completed` or `failed`.
    status: String,
    raw_input: Value,
    raw_output: Value,
    content: Value,
    meta: Value,
    /// The item last shown, to show only changes.
    shown: Option<ProviderItem>,
    /// Updates not shown since the last one that was.
    skipped: u32,
    /// The item's status once T3 ended it (`interrupted`).
    forced: Option<&'static str>,
}

/// A permission request waiting for the user.
struct Pending {
    rpc_id: Value,
    options: Vec<Value>,
    offered: Vec<&'static str>,
}

/// The text streams, in the order a turn's end closes them.
const STREAMS: [&str; 2] = ["reasoning", "assistant"];

struct Turn<'a> {
    flavor: Flavor,
    sink: &'a Sink,
    responses: &'a mut mpsc::UnboundedReceiver<RuntimeResponse>,
    instance: String,
    runtime_mode: String,
    interaction_mode: String,
    model: String,
    has_mcp: bool,
    /// The thread's MCP credential may use the device tools.
    has_device_tools: bool,
    /// `<session>:turn:<ordinal>`.
    native_turn: String,
    /// The JSON-RPC id of the turn's `session/prompt`.
    prompt_rpc: u64,
    /// Its `t3-xai-prompt-<n>`.
    prompt_label: String,
    segments: HashMap<&'static str, Segment>,
    /// Segments opened per stream this turn.
    counters: HashMap<&'static str, u64>,
    flush_at: Option<Instant>,
    tools: indexmap::IndexMap<String, Tool>,
    pending: HashMap<String, Pending>,
    attachments: Vec<Value>,
    prompt_text: String,
    scoped: bool,
    session: String,
    /// Whether the prompt's instructions and notes reached the session.
    delivered: bool,
    /// The user stopped the turn (the generic flavor's `session/cancel`).
    interrupting: bool,
    /// Antigravity: where the agent may read files through T3 (the turn's
    /// folder and the attachments folder).
    read_roots: Vec<std::path::PathBuf>,
    /// Antigravity: its pull request instructions ride in the session.
    system_prompt_append: bool,
    /// Antigravity from 1.5.0 on steers with another `session/prompt`.
    prompt_steering: bool,
    /// The prompts' usage (`PromptResponse.usage`), summed.
    prompt_usage: Option<Value>,
    /// Questions agy asked (`interaction_*`), by request id: the RPC id and
    /// the options.
    questions: HashMap<String, (Value, Vec<Value>)>,
}

impl<'a> Turn<'a> {
    fn new(
        spec: &TurnSpec,
        provider: &ProviderTurn,
        sink: &'a Sink,
        agent: &Agent,
        responses: &'a mut mpsc::UnboundedReceiver<RuntimeResponse>,
    ) -> Self {
        let ordinal = provider.turn_ordinal.unwrap_or(1);
        Self {
            flavor: agent.flavor,
            sink,
            responses,
            instance: if provider.instance_id.is_empty() {
                agent.flavor.driver().to_owned()
            } else {
                provider.instance_id.clone()
            },
            runtime_mode: provider.runtime_mode().to_owned(),
            interaction_mode: provider.interaction_mode().to_owned(),
            model: provider.model.clone(),
            has_mcp: provider.mcp.is_some(),
            has_device_tools: provider.mcp.as_ref().is_some_and(|mcp| mcp.device_tools),
            native_turn: format!("{}:turn:{ordinal}", agent.session),
            prompt_rpc: 0,
            prompt_label: String::new(),
            segments: HashMap::new(),
            counters: HashMap::new(),
            flush_at: None,
            tools: indexmap::IndexMap::new(),
            pending: HashMap::new(),
            attachments: provider.attachments.clone(),
            prompt_text: spec.prompt.clone(),
            scoped: agent.scoped,
            session: agent.session.clone(),
            delivered: false,
            interrupting: false,
            read_roots: std::iter::once(spec.cwd.clone())
                .chain(provider.extra_dirs.iter().cloned())
                .map(std::path::PathBuf::from)
                .collect(),
            system_prompt_append: agent.system_prompt_append,
            prompt_steering: agent.flavor == Flavor::Antigravity
                && version_at_least(agent.agent_version.as_deref(), (1, 5, 0)),
            prompt_usage: None,
            questions: HashMap::new(),
        }
    }

    /// An item's id: its native id, scoped to the instance in a session the
    /// adapter created (item identity version 2).
    fn scoped(&self, native: &str) -> String {
        if self.scoped {
            format!(
                "provider-instance:{}:{native}",
                uri_component(&self.instance)
            )
        } else {
            native.to_owned()
        }
    }

    async fn run(
        &mut self,
        agent: &mut Agent,
        cancel: &mut Option<oneshot::Receiver<()>>,
        resumed: bool,
    ) -> Ended {
        // The thread first, as the turn's records hang from it (the
        // reference reports its turn first).
        self.sink.send(ProviderEvent::ProviderThread {
            driver: self.flavor.driver().into(),
            native_thread_id: agent.session.clone(),
            resumed,
            model: None,

            opencode2: None,
        });
        self.sink.send(ProviderEvent::ProviderTurn {
            native_turn_id: Some(self.scoped(&self.native_turn)),
            native_turn_ref: None,
            conversation_head: None,
            status: TurnStatus::Running,
            model: None,
        });
        let (prompt, state, notes) = self.prompt(agent);
        agent.prompts += 1;
        self.prompt_label = format!("t3-xai-prompt-{}", agent.prompts);
        let mut params = json!({"sessionId": agent.session, "prompt": prompt});
        if self.flavor == Flavor::Grok {
            params["_meta"] =
                json!({"promptId": self.prompt_label, "requestId": self.prompt_label});
        }
        let sent = agent.send("session/prompt", params).await;
        let Ok(prompt_rpc) = sent else {
            return Ended::Crashed(AcpError::Closed.failure());
        };
        self.prompt_rpc = prompt_rpc;
        let ended = loop {
            let line = tokio::select! {
                line = agent.cli.next_line() => line,
                _ = cancelled(cancel) => {
                    self.cancel_pending(agent).await;
                    if super::take_detached(&self.sink.turn_id) {
                        break Ended::Detached;
                    }
                    if self.flavor != Flavor::Grok {
                        break self.cancel_prompt(agent).await;
                    }
                    self.finalize(TurnStatus::Interrupted);
                    break Ended::Stopped;
                }
                response = next_response(self.responses) => {
                    self.respond(agent, response).await;
                    continue;
                }
                _ = super::sleep_until(self.flush_at) => {
                    self.flush();
                    continue;
                }
            };
            let Some(line) = line else {
                let status = agent.cli.exit_status().await;
                self.cancel_pending(agent).await;
                self.close_streams();
                break Ended::Crashed(super::acp_crashed(status));
            };
            let Ok(message) = serde_json::from_str::<Value>(&line) else {
                // agy asks for a browser sign-in on its output.
                if self.flavor == Flavor::Antigravity && line.starts_with(AGY_SIGN_IN_LINE) {
                    self.cancel_pending(agent).await;
                    self.close_streams();
                    break Ended::Crashed(ProviderFailure::new("provider_error", AGY_SIGN_IN));
                }
                continue;
            };
            if let Some(end) = self.handle(agent, message).await {
                break end;
            }
        };
        if self.delivered {
            agent.instructions = Some(state);
            agent.notes = Some(notes);
        } else if !matches!(ended, Ended::Settled(_)) {
            // A new process starts over.
            agent.instructions = None;
            agent.notes = None;
        }
        ended
    }

    /// The generic flavor's Stop: `session/cancel`, then the prompt's answer
    /// (any stop reason) ends the turn interrupted, the agent kept for the
    /// thread's next turn. One that does not answer is stopped.
    async fn cancel_prompt(&mut self, agent: &mut Agent) -> Ended {
        self.interrupting = true;
        let session = agent.session.clone();
        let _ = agent
            .cli
            .write(&json!({"jsonrpc": "2.0", "method": "session/cancel",
                "params": {"sessionId": session}}))
            .await;
        let deadline = Instant::now() + STOP_CONFIRM_TIMEOUT;
        loop {
            let line = tokio::select! {
                line = agent.cli.next_line() => line,
                _ = tokio::time::sleep_until(deadline) => None,
            };
            let Some(line) = line else {
                self.finalize(TurnStatus::Interrupted);
                return Ended::Stopped;
            };
            let Ok(message) = serde_json::from_str::<Value>(&line) else {
                continue;
            };
            if message.get("method").is_none() && message["id"].as_u64() == Some(self.prompt_rpc) {
                self.finalize(TurnStatus::Interrupted);
                return Ended::Settled(TurnEnd::interrupted(true));
            }
            if message.get("id").is_some() && message.get("method").is_some() {
                agent.refuse(&message).await;
            } else if message["method"] == "session/update" {
                let update = message["params"]["update"].clone();
                self.update(&update);
            }
        }
    }

    /// The prompt's content blocks (`resolvePromptParts`): the text, with
    /// the T3 instructions when the session lacks them, its images, then the
    /// runtime notes when they changed. With the state and notes it gives.
    fn prompt(&self, agent: &Agent) -> (Value, InstructionState, String) {
        let state = InstructionState {
            interaction_mode: self.interaction_mode.clone(),
            has_mcp: self.has_mcp,
            has_device_tools: self.has_device_tools,
        };
        let slash = self.prompt_text.trim_start().starts_with('/');
        let text = if slash || agent.instructions.as_ref() == Some(&state) {
            self.prompt_text.clone()
        } else {
            crate::agent_instructions::acp_prompt(
                &self.prompt_text,
                &state.interaction_mode,
                self.has_mcp,
                self.has_device_tools,
            )
        };
        // Antigravity's prompt (`buildAttachmentPrompt`): its text trimmed,
        // with each attachment's path, and only the images it takes.
        let text = if self.flavor == Flavor::Antigravity {
            crate::workspace::attachment_prompt::text_with_paths(&text, &self.attachments)
                .trim()
                .to_owned()
        } else {
            text
        };
        let mut blocks = vec![json!({"type": "text", "text": text})];
        for attachment in &self.attachments {
            let mime = attachment["mimeType"].as_str().unwrap_or_default();
            let Some(path) = attachment["path"].as_str() else {
                continue;
            };
            if !mime.starts_with("image/") {
                continue;
            }
            if self.flavor == Flavor::Antigravity
                && !matches!(
                    mime,
                    "image/bmp" | "image/jpeg" | "image/png" | "image/webp"
                )
            {
                continue;
            }
            if let Ok(bytes) = std::fs::read(path) {
                use base64::Engine;
                blocks.push(json!({"type": "image",
                    "data": base64::engine::general_purpose::STANDARD.encode(bytes),
                    "mimeType": mime}));
            }
        }
        // The selection's model ("default", the agent's own, says none).
        let model = match (self.model.as_str(), self.flavor) {
            ("", Flavor::Grok) => Some("grok-build"),
            ("", Flavor::Registry | Flavor::Antigravity) => None,
            (model, _) => Some(model),
        };
        // An agent whose session took the pull request text has notes
        // without it.
        let notes = if self.system_prompt_append {
            crate::agent_instructions::runtime_info(self.flavor.harness(), model, None)
        } else {
            crate::agent_instructions::runtime(self.flavor.harness(), model, None)
        };
        if !slash && agent.notes.as_deref() != Some(notes.as_str()) {
            blocks.push(json!({"type": "text", "text": notes}));
        }
        (Value::Array(blocks), state, notes)
    }

    /// Handle one message from the agent; the turn's end once it ended.
    async fn handle(&mut self, agent: &mut Agent, message: Value) -> Option<Ended> {
        let method = message["method"].as_str().map(str::to_owned);
        match (method.as_deref(), message.get("id")) {
            (Some("session/request_permission"), Some(_)) => {
                self.permission(agent, &message).await;
                None
            }
            (Some("fs/read_text_file"), Some(id)) if self.flavor == Flavor::Antigravity => {
                let id = id.clone();
                match self.read_text_file(&message["params"]) {
                    Ok(content) => agent.reply(&id, json!({"content": content})).await,
                    Err((code, text)) => agent.reply_error(&id, code, &text).await,
                }
                None
            }
            (Some(_), Some(_)) => {
                agent.refuse(&message).await;
                None
            }
            (Some("session/update"), None) => {
                let params = &message["params"];
                if params["sessionId"].as_str() == Some(self.session.as_str())
                    && params["update"]["_meta"]["isReplay"] != true
                {
                    self.update(&params["update"]);
                }
                None
            }
            (Some(method), None) => self.extension(method, &message["params"]),
            (None, Some(id)) if id.as_u64() == Some(self.prompt_rpc) => {
                if let Some(error) = message.get("error") {
                    let error = AcpError::Rpc {
                        code: error["code"].as_i64().unwrap_or(-32603),
                        message: error["message"].as_str().unwrap_or_default().to_owned(),
                        data: error.get("data").cloned().unwrap_or(Value::Null),
                    };
                    let failure = if self.flavor == Flavor::Antigravity {
                        antigravity_prompt_failure(&error)
                    } else {
                        error.failure()
                    };
                    return Some(self.failed(failure, true));
                }
                let stop = message["result"]["stopReason"]
                    .as_str()
                    .unwrap_or("end_turn")
                    .to_owned();
                if self.flavor == Flavor::Antigravity {
                    self.add_usage(&message["result"]["usage"]);
                }
                Some(self.stopped(&stop))
            }
            _ => None,
        }
    }

    /// Grok's prompt-completion notifications, which may settle the prompt
    /// before its response does.
    fn extension(&mut self, method: &str, params: &Value) -> Option<Ended> {
        if self.flavor != Flavor::Grok {
            return None;
        }
        let (stop, prompt, result) = match method.trim_start_matches('_') {
            "x.ai/session/prompt_complete" => (
                params["stopReason"].as_str(),
                params["promptId"].as_str(),
                params["agentResult"].as_str(),
            ),
            "x.ai/session_notification" | "x.ai/session/update"
                if params["update"]["sessionUpdate"] == "turn_completed" =>
            {
                (
                    params["update"]["stop_reason"].as_str(),
                    params["update"]["prompt_id"].as_str(),
                    None,
                )
            }
            _ => return None,
        };
        if prompt.is_some_and(|id| id.starts_with("task-completed-") || id != self.prompt_label) {
            return None;
        }
        Some(match stop.unwrap_or("end_turn") {
            "rate_limit" => {
                let mut failure = ProviderFailure::new(
                    "usage_limit",
                    "Grok usage limit reached. Try again later.",
                );
                failure.code = Some("-32003".into());
                self.failed(failure, true)
            }
            "error" => {
                let message = result
                    .filter(|text| !text.trim().is_empty())
                    .unwrap_or("Grok ended the turn with an error.");
                let mut failure = ProviderFailure::new("provider_error", message);
                failure.code = Some("-32603".into());
                self.failed(failure, true)
            }
            stop => {
                let stop = stop.to_owned();
                self.stopped(&stop)
            }
        })
    }

    /// The turn's end for a prompt's stop reason (`followPrompt`).
    fn stopped(&mut self, stop: &str) -> Ended {
        let refusal = |code: &str, message: &str| {
            let mut failure = ProviderFailure::new("provider_error", message);
            failure.code = Some(code.into());
            failure
        };
        match stop {
            "cancelled" => {
                self.finalize(TurnStatus::Interrupted);
                Ended::Settled(TurnEnd::interrupted(false))
            }
            "refusal" => self.failed(
                refusal(
                    "refusal",
                    "The agent refused this request. The agent's next turn won't include this message or its reply.",
                ),
                false,
            ),
            "max_tokens" => {
                self.delivered = true;
                self.failed(
                    refusal(
                        "max_tokens",
                        "The agent stopped because its reply reached the output token limit.",
                    ),
                    false,
                )
            }
            "max_turn_requests" => {
                self.delivered = true;
                self.failed(
                    refusal(
                        "max_turn_requests",
                        "The agent stopped because it reached its request limit for this turn.",
                    ),
                    false,
                )
            }
            _ => {
                self.delivered = true;
                self.finalize(TurnStatus::Completed);
                Ended::Settled(TurnEnd::completed())
            }
        }
    }

    /// A failed turn; after a request error only tools that never started
    /// fail with it.
    fn failed(&mut self, failure: ProviderFailure, request_error: bool) -> Ended {
        if request_error {
            for tool in self.tools.values_mut() {
                if tool.status == "pending" {
                    tool.status = "failed".into();
                    tool.forced = Some("failed");
                }
            }
            let ids: Vec<String> = self.tools.keys().cloned().collect();
            for id in ids {
                self.show_tool(&id, true);
            }
        }
        self.finalize(TurnStatus::Failed);
        Ended::Settled(TurnEnd::failed(failure, ThreadDisposition::Reusable))
    }

    /// `finalizeTurn`: open tools end with the turn, then the text streams,
    /// then the turn's usage.
    fn finalize(&mut self, status: TurnStatus) {
        let closed = match status {
            TurnStatus::Interrupted => Some("interrupted"),
            TurnStatus::Completed => Some("completed"),
            _ => None,
        };
        if let Some(closed) = closed {
            let ids: Vec<String> = self.tools.keys().cloned().collect();
            for id in ids {
                let tool = &mut self.tools[&id];
                if matches!(tool.status.as_str(), "pending" | "inProgress") {
                    tool.forced = Some(closed);
                    self.show_tool(&id, true);
                }
            }
        }
        self.close_streams();
        let usage = match &self.prompt_usage {
            // Gemini's input counts its cached reads apart (`geminiPromptUsage`).
            Some(usage) => {
                let n = |key: &str| usage[key].as_u64().unwrap_or(0);
                let input = n("inputTokens") + n("cachedReadTokens");
                let output = n("outputTokens");
                json!({
                    "usageScope": "main_agent",
                    "usageStatus": if status == TurnStatus::Completed { "complete" } else { "partial" },
                    "hasSubagents": false,
                    "inputTokens": input,
                    "outputTokens": output,
                    "cachedInputTokens": n("cachedReadTokens").min(input),
                    "cacheCreationTokens": n("cachedWriteTokens"),
                    "reasoningTokens": n("thoughtTokens").min(output),
                })
            }
            None => json!({"usageScope": "main_agent", "usageStatus": "unavailable",
                "hasSubagents": false}),
        };
        self.sink.send(ProviderEvent::TurnUsage { usage });
    }

    /// A prompt's usage, added to the turn's (`addAcpUsage`).
    fn add_usage(&mut self, usage: &Value) {
        let Some(usage) = usage.as_object() else {
            return;
        };
        let total = self.prompt_usage.get_or_insert_with(|| json!({}));
        for (key, value) in usage {
            if let Some(n) = value.as_u64() {
                total[key] = json!(total[key].as_u64().unwrap_or(0) + n);
            }
        }
    }

    /// `fs/read_text_file` (`AntigravityClientFiles`): a text file in the
    /// turn's folder or the attachments folder, sliced by `line` (1-based)
    /// and `limit` (lines); or the JSON-RPC error the reference answers.
    fn read_text_file(&self, params: &Value) -> Result<String, (i64, String)> {
        let path = params["path"].as_str().unwrap_or_default();
        let outside = || {
            (
                -32602,
                format!("Path '{path}' is outside the session workspace."),
            )
        };
        let resolved = resolve_existing(std::path::Path::new(path)).ok_or_else(outside)?;
        let inside = self
            .read_roots
            .iter()
            .any(|root| resolve_existing(root).is_some_and(|root| resolved.starts_with(&root)));
        if !inside {
            return Err(outside());
        }
        let meta = std::fs::metadata(&resolved)
            .map_err(|_| (-32002, format!("File '{path}' not found.")))?;
        if !meta.is_file() || meta.len() > AGY_READ_LIMIT {
            return Err((
                -32602,
                format!("File '{path}' is not a readable text file under {AGY_READ_LIMIT} bytes."),
            ));
        }
        let bytes =
            std::fs::read(&resolved).map_err(|_| (-32603, format!("Could not read '{path}'.")))?;
        let text = String::from_utf8(bytes)
            .map_err(|_| (-32602, format!("File '{path}' is not a UTF-8 text file.")))?;
        if text.contains('\0') {
            return Err((-32602, format!("File '{path}' contains binary data.")));
        }
        let line = params["line"].as_u64().filter(|line| *line > 0);
        let limit = params["limit"].as_u64();
        if line.is_none() && limit.is_none() {
            return Ok(text);
        }
        let start = line.unwrap_or(1) as usize - 1;
        let lines = text.split('\n').skip(start);
        let lines: Vec<&str> = match limit {
            Some(limit) => lines.take(limit as usize).collect(),
            None => lines.collect(),
        };
        Ok(lines.join("\n"))
    }

    /// One `session/update`.
    fn update(&mut self, update: &Value) {
        match update["sessionUpdate"].as_str().unwrap_or_default() {
            "agent_message_chunk" => self.append("assistant", &update["content"]),
            "agent_thought_chunk" => self.append("reasoning", &update["content"]),
            "tool_call" | "tool_call_update" => self.tool(update),
            "plan" => self.plan(update),
            _ => {}
        }
    }

    /// Append a content block's text to a stream, closing the others.
    fn append(&mut self, stream: &'static str, content: &Value) {
        let text = display_text(content);
        if text.is_empty() {
            return;
        }
        for other in STREAMS {
            if other != stream {
                self.close_stream(other);
            }
        }
        if let Some(segment) = self.segments.get_mut(stream) {
            segment.text.push_str(&text);
            segment.dirty = true;
            if self.flush_at.is_none() {
                self.flush_at = Some(Instant::now() + TEXT_FLUSH_INTERVAL);
            }
            return;
        }
        let counter = self.counters.entry(stream).or_insert(0);
        let native = format!("{}:{stream}:{counter}", self.native_turn);
        *counter += 1;
        let segment = Segment {
            id: self.scoped(&native),
            native,
            text,
            dirty: false,
        };
        self.emit_segment(stream, &segment, false);
        self.segments.insert(stream, segment);
    }

    fn emit_segment(&self, stream: &str, segment: &Segment, completed: bool) {
        let kind = if stream == "assistant" {
            "assistant_message"
        } else {
            "reasoning"
        };
        let status = if completed { "completed" } else { "running" };
        let item = ProviderItem::new(segment.id.clone(), kind, status)
            .with("text", segment.text.clone())
            .with("streaming", !completed)
            .with(
                "nativeItemRef",
                native_ref(self.flavor.driver(), &segment.native, "weak"),
            );
        self.sink.send(ProviderEvent::TurnItem { item });
    }

    /// Show the streams' grown text.
    fn flush(&mut self) {
        self.flush_at = None;
        for stream in STREAMS {
            if let Some(segment) = self.segments.get_mut(stream) {
                if segment.dirty {
                    segment.dirty = false;
                    let segment = &self.segments[stream];
                    self.emit_segment(stream, segment, false);
                }
            }
        }
    }

    fn close_stream(&mut self, stream: &'static str) {
        if let Some(segment) = self.segments.remove(stream) {
            self.emit_segment(stream, &segment, true);
        }
    }

    fn close_streams(&mut self) {
        for stream in STREAMS {
            self.close_stream(stream);
        }
        self.flush_at = None;
    }

    /// A `tool_call` or `tool_call_update`: merged into the call's state and
    /// shown when it changed.
    fn tool(&mut self, update: &Value) {
        let Some(id) = update["toolCallId"]
            .as_str()
            .map(str::trim)
            .filter(|id| !id.is_empty())
        else {
            return;
        };
        let id = id.to_owned();
        self.close_streams();
        let first = !self.tools.contains_key(&id);
        let tool = self.tools.entry(id.clone()).or_default();
        if let Some(kind) = update["kind"]
            .as_str()
            .map(str::trim)
            .filter(|k| !k.is_empty())
        {
            tool.kind = Some(kind.to_owned());
        }
        if let Some(title) = update["title"].as_str() {
            tool.title = Some(title.to_owned());
        }
        match update["status"].as_str() {
            Some("pending") => tool.status = "pending".into(),
            Some("in_progress" | "inProgress") => tool.status = "inProgress".into(),
            Some("completed") => tool.status = "completed".into(),
            Some("failed" | "cancelled") => tool.status = "failed".into(),
            _ if first => tool.status = "pending".into(),
            _ => {}
        }
        for (key, slot) in [
            ("rawInput", &mut tool.raw_input),
            ("rawOutput", &mut tool.raw_output),
            ("content", &mut tool.content),
            ("_meta", &mut tool.meta),
        ] {
            if let Some(value) = update.get(key).filter(|value| !value.is_null()) {
                *slot = value.clone();
            }
        }
        // Grok's own tool shapes (`normalizeXAiAcpToolCallState`).
        if self.flavor == Flavor::Grok {
            normalize_mcp(tool);
            normalize_bash(tool);
        }
        if self.flavor == Flavor::Antigravity {
            normalize_antigravity(tool);
        }
        self.show_tool(&id, false);
    }

    /// Emit the tool's item when it changed (`shouldPersistToolUpdate`).
    fn show_tool(&mut self, id: &str, force: bool) {
        let item = self.tool_item(id);
        let tool = &mut self.tools[id];
        let changed = tool.shown.as_ref().is_none_or(|shown| {
            shown.status != item.status || shown.title != item.title || shown.fields != item.fields
        });
        let terminal = matches!(item.status.as_str(), "completed" | "failed" | "interrupted");
        if !(force || terminal || changed || tool.skipped >= 9) {
            tool.skipped += 1;
            return;
        }
        if tool.shown.as_ref() == Some(&item) {
            return;
        }
        tool.skipped = 0;
        tool.shown = Some(item.clone());
        self.sink.send(ProviderEvent::TurnItem { item });
    }

    /// The tool's turn item (`emitTool`), its type following its kind.
    fn tool_item(&self, id: &str) -> ProviderItem {
        let tool = &self.tools[id];
        let status = tool.forced.unwrap_or(match tool.status.as_str() {
            "completed" => "completed",
            // A call T3 interrupted ends interrupted, whatever the agent says.
            _ if self.interrupting => "interrupted",
            "failed" => "failed",
            "pending" => "pending",
            _ => "running",
        });
        let native = format!("{}:tool:{id}", self.session);
        let mut item = ProviderItem::new(self.scoped(&native), "dynamic_tool", status).with(
            "nativeItemRef",
            native_ref(self.flavor.driver(), id, "strong"),
        );
        let command = tool_command(tool);
        let output = text_of(if tool.raw_output.is_null() {
            &tool.content
        } else {
            &tool.raw_output
        });
        let kind = tool.kind.as_deref().unwrap_or_default();
        match kind {
            "execute" => {
                item.kind = "command_execution".into();
                item.title = Some("Ran command".into());
                item.fields
                    .insert("input".into(), json!(command.clone().unwrap_or_default()));
                if let Some(output) = output.filter(|text| !text.is_empty()) {
                    item.fields.insert("output".into(), json!(output));
                }
                if matches!(tool.status.as_str(), "completed" | "failed") {
                    if let Some(code) = exit_code(&tool.raw_output) {
                        item.fields.insert("exitCode".into(), json!(code));
                    }
                }
            }
            "edit" | "delete" | "move" => {
                item.kind = "file_change".into();
                item.title = Some("Changed files".into());
                let path = tool_path(tool);
                item.fields.insert(
                    "fileName".into(),
                    json!(path
                        .clone()
                        .or_else(|| tool.title.clone())
                        .unwrap_or_else(|| "File change".into())),
                );
                if let Some(diff) = unified_diff(tool) {
                    item.fields.insert("diffStr".into(), json!(diff));
                }
            }
            "read" => {
                let path = tool_path(tool);
                item.title = Some(match &path {
                    Some(path) => format!("Read {}", path.trim()),
                    None => tool.title.clone().unwrap_or_else(|| "Read file".into()),
                });
                item.fields.insert("toolName".into(), json!("Read"));
                item.fields.insert("input".into(), input_of(tool));
                if let Some(output) = output {
                    item.fields.insert("output".into(), json!(output));
                }
            }
            _ => {
                item.title = tool.title.clone();
                item.fields.insert(
                    "toolName".into(),
                    json!(tool.title.clone().or(tool.kind.clone())),
                );
                item.fields.insert("input".into(), input_of(tool));
                let output = if tool.raw_output.is_null() {
                    tool.content.clone()
                } else {
                    tool.raw_output.clone()
                };
                if !output.is_null() {
                    item.fields.insert("output".into(), output);
                }
            }
        }
        item
    }

    /// A v1 `plan`: the turn's todo list.
    fn plan(&mut self, update: &Value) {
        self.close_streams();
        let steps: Vec<crate::workspace::provider::PlanStep> = update["entries"]
            .as_array()
            .into_iter()
            .flatten()
            .enumerate()
            .filter(|(_, entry)| entry["status"] != "cancelled")
            .map(|(index, entry)| crate::workspace::provider::PlanStep {
                step: entry["content"]
                    .as_str()
                    .map(str::to_owned)
                    .unwrap_or_else(|| format!("Step {}", index + 1)),
                status: match entry["status"].as_str() {
                    Some("in_progress") => "inProgress",
                    Some("completed") => "completed",
                    _ => "pending",
                }
                .into(),
            })
            .collect();
        self.sink.send(ProviderEvent::Plan {
            steps,
            explanation: None,
        });
    }

    /// A `session/request_permission`: answered at once when the runtime
    /// mode decides it, else asked of the user.
    async fn permission(&mut self, agent: &mut Agent, message: &Value) {
        let params = &message["params"];
        let rpc_id = message["id"].clone();
        let call = &params["toolCall"];
        let call_id = call["toolCallId"]
            .as_str()
            .unwrap_or_default()
            .trim()
            .to_owned();
        let tracked = self.tools.get(&call_id).cloned();
        let kind = call["kind"]
            .as_str()
            .map(str::to_owned)
            .or_else(|| tracked.as_ref().and_then(|tool| tool.kind.clone()))
            .unwrap_or_else(|| "other".into());
        let options: Vec<Value> = params["options"].as_array().cloned().unwrap_or_default();
        // agy's questions come as permission requests (`interaction_*`).
        if self.flavor == Flavor::Antigravity && call_id.starts_with("interaction_") {
            return self.question(agent, rpc_id, &call_id, call, options).await;
        }
        // A call to T3's own tools runs in agy's auto modes.
        let t3_tool = self.flavor == Flavor::Antigravity
            && matches!(self.runtime_mode.as_str(), "auto" | "auto-accept-edits")
            && (call["rawInput"]["ServerName"] == "t3-code"
                || call["title"]
                    .as_str()
                    .is_some_and(|title| title.starts_with("Run mcp_t3-code_")));
        let option = |wanted: &str| {
            options
                .iter()
                .find(|option| option["kind"] == wanted)
                .and_then(|option| option["optionId"].as_str())
                .map(str::to_owned)
        };
        let disposed = if t3_tool {
            Disposition::Allow
        } else {
            disposition(self.flavor, &self.runtime_mode, &kind)
        };
        match disposed {
            Disposition::Allow => {
                let chosen = option("allow_once").or_else(|| option("allow_always"));
                return agent.reply(&rpc_id, outcome(chosen)).await;
            }
            Disposition::Ask => {}
        }
        self.close_streams();
        let request_kind = match kind.as_str() {
            "execute" => "command",
            "read" | "search" | "fetch" => "file-read",
            "edit" | "delete" | "move" => "file-change",
            _ => "command",
        };
        let has = |kind: &str, id: Option<&str>| {
            options.iter().any(|option| {
                option["kind"] == kind
                    && id.is_none_or(|id| option["optionId"].as_str().map(str::trim) == Some(id))
            })
        };
        let mut offered = vec!["cancel"];
        let mut labels = vec![json!({"decision": "cancel", "label": "Cancel"})];
        if has("reject_once", None) {
            offered.push("decline");
            labels.push(json!({"decision": "decline", "label": "Decline"}));
        }
        if has("allow_always", Some("allow-edits-session")) {
            offered.push("acceptForSession");
            labels.push(
                json!({"decision": "acceptForSession", "label": "Allow all edits this session"}),
            );
        }
        if has("allow_once", None) {
            offered.push("accept");
            labels.push(json!({"decision": "accept", "label": "Approve"}));
        }
        let mut merged = tracked.unwrap_or_default();
        if merged.kind.is_none() {
            merged.kind = Some(kind.clone());
        }
        if let Some(title) = call["title"].as_str() {
            merged.title = Some(title.to_owned());
        }
        if let Some(input) = call.get("rawInput").filter(|input| !input.is_null()) {
            merged.raw_input = input.clone();
        }
        let prompt = tool_command(&merged)
            .or_else(|| {
                merged
                    .title
                    .clone()
                    .filter(|title| !title.trim().is_empty())
            })
            .unwrap_or_else(|| format!("Session {}", self.session));
        let native = format!("{}:tool:{call_id}", self.session);
        let mut open = requests::open(
            &call_id,
            &self.scoped(&native),
            request_kind,
            &self.runtime_mode,
        );
        open.fields.insert("prompt".into(), json!(prompt));
        // Grok labels its options (`grokApprovalOptions`); the generic
        // flavor offers the default decisions.
        if self.flavor == Flavor::Grok {
            open.fields.insert("options".into(), Value::Array(labels));
        }
        // agy's labels (`antigravityApprovalOptions`), for the options it offers.
        if self.flavor == Flavor::Antigravity {
            let offers = |kind: &str| {
                options.iter().find(|option| {
                    option["kind"] == kind
                        && option["optionId"]
                            .as_str()
                            .is_some_and(|id| !id.trim().is_empty())
                })
            };
            let mut labels = Vec::new();
            if offers("allow_once").is_some() {
                labels.push(json!({"decision": "accept", "label": "Allow once"}));
            }
            if let Some(option) = offers("allow_always") {
                let mut label =
                    json!({"decision": "acceptForSession", "label": "Allow for this thread"});
                let warning = &option["_meta"]["agy.security.warning"];
                if let Some(text) = warning["message"]
                    .as_str()
                    .or(warning["title"].as_str())
                    .filter(|text| !text.trim().is_empty())
                {
                    label["warning"] = json!(text.chars().take(512).collect::<String>());
                }
                labels.push(label);
            }
            if offers("reject_once").is_some() {
                labels.push(json!({"decision": "decline", "label": "Deny"}));
            }
            labels.push(json!({"decision": "cancel", "label": "Cancel"}));
            open.fields.insert("options".into(), Value::Array(labels));
        }
        self.pending.insert(
            open.id.clone(),
            Pending {
                rpc_id,
                options,
                offered,
            },
        );
        self.sink
            .send(ProviderEvent::RuntimeRequestOpened { request: open });
    }

    /// The user's answer to a waiting permission request, or a message
    /// that steers the turn: Grok's `_x.ai/interject` queues it for the
    /// model's next step, letting a running tool finish. Like a Codex steer
    /// its reply does not matter; the turn's items show what it did.
    async fn respond(&mut self, agent: &mut Agent, response: RuntimeResponse) {
        if let Some(text) = &response.steer {
            // agy from 1.5.0 takes another prompt, which the turn then
            // follows (`steersWithPrompt`).
            if self.flavor == Flavor::Antigravity {
                if self.prompt_steering {
                    let text = crate::workspace::attachment_prompt::text_with_paths(text, &[]);
                    let params = json!({"sessionId": self.session,
                        "prompt": [{"type": "text", "text": text}]});
                    if let Ok(id) = agent.send("session/prompt", params).await {
                        self.prompt_rpc = id;
                    }
                }
                return;
            }
            let params = json!({"sessionId": self.session, "text": text});
            let _ = agent.send("_x.ai/interject", params).await;
            return;
        }
        if let Some((rpc_id, options)) = self.questions.remove(&response.id) {
            let answer = response
                .answers
                .as_ref()
                .and_then(|answers| answers.as_object())
                .and_then(|answers| answers.values().next().cloned())
                .map(|answer| match answer {
                    Value::Array(values) => values.into_iter().next().unwrap_or(Value::Null),
                    other => other,
                })
                .and_then(|answer| answer.as_str().map(str::to_owned));
            let chosen = answer.and_then(|answer| {
                options
                    .iter()
                    .find(|option| option["optionId"] == answer.as_str())
                    .or_else(|| {
                        let named: Vec<&Value> = options
                            .iter()
                            .filter(|option| {
                                option["name"].as_str().map(str::trim) == Some(answer.trim())
                            })
                            .collect();
                        (named.len() == 1).then(|| named[0])
                    })
                    .and_then(|option| option["optionId"].as_str().map(str::to_owned))
            });
            agent.reply(&rpc_id, outcome(chosen)).await;
            return;
        }
        let Some(pending) = self.pending.remove(&response.id) else {
            return;
        };
        let mut decision = requests::decision(&response);
        // Grok offers "allow for session" for edits only; the generic flavor
        // takes the agent's first always-allow option.
        if decision == "acceptForSession"
            && self.flavor == Flavor::Grok
            && !pending.offered.contains(&"acceptForSession")
        {
            decision = "accept";
        }
        let option = |wanted: &str| {
            pending
                .options
                .iter()
                .find(|option| option["kind"] == wanted)
                .and_then(|option| option["optionId"].as_str())
                .map(str::to_owned)
        };
        let chosen = match decision {
            "accept" => option("allow_once"),
            "acceptForSession" | "acceptAlways" => {
                option("allow_always").or_else(|| option("allow_once"))
            }
            "decline" => option("reject_once"),
            _ => None,
        };
        agent.reply(&pending.rpc_id, outcome(chosen)).await;
    }

    /// An agy question (`interaction_*`): the user picks one of its options.
    async fn question(
        &mut self,
        agent: &mut Agent,
        rpc_id: Value,
        call_id: &str,
        call: &Value,
        options: Vec<Value>,
    ) {
        let valid = !options.is_empty()
            && options.iter().all(|option| {
                option["optionId"]
                    .as_str()
                    .is_some_and(|id| !id.trim().is_empty())
            });
        if !valid {
            return agent.reply(&rpc_id, outcome(None)).await;
        }
        self.close_streams();
        let title = call["title"].as_str().unwrap_or_default().trim();
        let question = if title.is_empty() {
            "Choose an option."
        } else {
            title
        };
        let choices: Vec<Value> = options
            .iter()
            .map(|option| {
                let id = option["optionId"].as_str().unwrap_or_default();
                let name = option["name"].as_str().unwrap_or_default().trim();
                let label = if name.is_empty() { id } else { name };
                json!({"value": id, "label": label, "description": label})
            })
            .collect();
        let native = format!("{}:question:{call_id}", self.session);
        let mut open = requests::open(
            call_id,
            &self.scoped(&native),
            "user_input",
            &self.runtime_mode,
        );
        open.fields.insert(
            "questions".into(),
            json!([{"id": call_id, "header": "Question", "question": question,
                "multiSelect": false, "allowCustomAnswer": false, "options": choices}]),
        );
        self.questions.insert(open.id.clone(), (rpc_id, options));
        self.sink
            .send(ProviderEvent::RuntimeRequestOpened { request: open });
    }

    /// Cancel every waiting request: the agent hears `cancelled`.
    async fn cancel_pending(&mut self, agent: &mut Agent) {
        let mut questions: Vec<(String, (Value, Vec<Value>))> = self.questions.drain().collect();
        questions.sort_by(|a, b| a.0.cmp(&b.0));
        for (id, (rpc_id, _)) in questions {
            agent.reply(&rpc_id, outcome(None)).await;
            self.sink.send(ProviderEvent::RuntimeRequestClosed {
                id,
                status: "cancelled".into(),
            });
        }
        let mut waiting: Vec<(String, Pending)> = self.pending.drain().collect();
        waiting.sort_by(|a, b| a.0.cmp(&b.0));
        for (id, pending) in waiting {
            agent.reply(&pending.rpc_id, outcome(None)).await;
            self.sink.send(ProviderEvent::RuntimeRequestClosed {
                id,
                status: "cancelled".into(),
            });
        }
    }
}

/// How the runtime mode answers a permission request
/// (`acpPermissionDisposition`, and Grok's auto mode, which always asks).
/// The policies that deny outright (read-only and workspace-write
/// sandboxes) are not ones a thread's runtime mode sets.
enum Disposition {
    Allow,
    Ask,
}

/// Grok has no auto-accept-edits launch mode: a thread in it runs under the
/// approval policy the reference gives it, so it asks about edits too. In
/// auto mode Grok asks only about what its classifier refused, which goes to
/// the user; the generic flavor lets an agent's requests through.
fn disposition(flavor: Flavor, runtime_mode: &str, kind: &str) -> Disposition {
    let reads = matches!(kind, "read" | "search" | "think");
    match runtime_mode {
        // agy's own Auto already decided what it asks (`antigravityPermissionDisposition`).
        "auto" if matches!(flavor, Flavor::Grok | Flavor::Antigravity) => Disposition::Ask,
        "auto" => Disposition::Allow,
        "full-access" => Disposition::Allow,
        _ if reads => Disposition::Allow,
        _ => Disposition::Ask,
    }
}

fn outcome(option: Option<String>) -> Value {
    match option {
        Some(id) => json!({"outcome": {"outcome": "selected", "optionId": id}}),
        None => json!({"outcome": {"outcome": "cancelled"}}),
    }
}

fn native_ref(driver: &str, native: &str, strength: &str) -> Value {
    json!({"driver": driver, "nativeId": native, "strength": strength})
}

/// A content block's display text (`displayTextFromContentBlock`).
fn display_text(content: &Value) -> String {
    match content["type"].as_str() {
        Some("text") => content["text"].as_str().unwrap_or_default().to_owned(),
        Some("image") => format!(
            "[ACP image ({})]",
            content["mimeType"].as_str().unwrap_or("image")
        ),
        Some("resource_link") => content["uri"].as_str().unwrap_or_default().to_owned(),
        Some("resource") => content["resource"]["text"]
            .as_str()
            .unwrap_or_default()
            .to_owned(),
        _ => String::new(),
    }
}

/// The tool's command: `rawInput.command` (a string or words), else its
/// executable and arguments, else for `execute` the first backticked text of
/// its title.
fn tool_command(tool: &Tool) -> Option<String> {
    let input = &tool.raw_input;
    let command = match &input["command"] {
        Value::String(text) => Some(text.clone()),
        Value::Array(words) => Some(
            words
                .iter()
                .filter_map(Value::as_str)
                .collect::<Vec<_>>()
                .join(" "),
        ),
        _ => input["executable"].as_str().map(|exe| {
            let args: Vec<&str> = input["args"]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(Value::as_str)
                .collect();
            std::iter::once(exe)
                .chain(args)
                .collect::<Vec<_>>()
                .join(" ")
        }),
    };
    command
        .filter(|command| !command.trim().is_empty())
        .or_else(|| {
            tool.title
                .as_deref()
                .filter(|_| tool.kind.as_deref() == Some("execute"))
                .and_then(|title| title.split('`').nth(1))
                .filter(|command| !command.trim().is_empty())
                .map(str::to_owned)
        })
}

fn tool_path(tool: &Tool) -> Option<String> {
    for source in [&tool.raw_input, &tool.raw_output] {
        for key in ["path", "filePath", "file_path"] {
            if let Some(path) = source[key].as_str().filter(|path| !path.trim().is_empty()) {
                return Some(path.to_owned());
            }
        }
    }
    tool.content
        .as_array()
        .into_iter()
        .flatten()
        .find(|entry| entry["type"] == "diff")
        .and_then(|entry| entry["path"].as_str())
        .map(str::to_owned)
}

fn input_of(tool: &Tool) -> Value {
    if tool.raw_input.is_null() {
        json!({})
    } else {
        tool.raw_input.clone()
    }
}

/// Text from a tool's output (`textFromUnknown`): a string, a stdout
/// field, or the text of content blocks.
fn text_of(value: &Value) -> Option<String> {
    match value {
        Value::Null => None,
        Value::String(text) => Some(text.clone()),
        Value::Array(entries) => {
            let texts: Vec<String> = entries
                .iter()
                .filter_map(|entry| {
                    entry["content"]["text"]
                        .as_str()
                        .or(entry["text"].as_str())
                        .map(str::to_owned)
                })
                .collect();
            (!texts.is_empty()).then(|| texts.join(""))
        }
        Value::Object(map) => ["stdout", "output", "text", "content"]
            .iter()
            .find_map(|key| map.get(*key).and_then(text_of)),
        other => Some(other.to_string()),
    }
}

fn exit_code(output: &Value) -> Option<i64> {
    ["exitCode", "exit_code", "code"]
        .iter()
        .find_map(|key| output[*key].as_i64())
}

/// Grok's MCP calls (`normalizeXAiMcpToolCall`): its `use_tool` built-in
/// names the tool `<server>__<tool>` and wraps the result as `{type: "MCP",
/// output: {OkayOutput} | {Error}}`; both become the generic ACP shapes.
fn normalize_mcp(tool: &mut Tool) {
    let name = tool.raw_input["tool_name"].as_str().unwrap_or_default();
    let use_tool = tool.meta["x.ai/tool"]["name"] == "use_tool"
        && name
            .find("__")
            .is_some_and(|at| at > 0 && at + 2 < name.len());
    if use_tool {
        let (server, rest) = name.split_at(name.find("__").unwrap_or_default());
        let arguments = match &tool.raw_input["tool_input"] {
            Value::Object(arguments) => Value::Object(arguments.clone()),
            _ => json!({}),
        };
        tool.raw_input = json!({"server": server, "tool": &rest[2..], "arguments": arguments});
        tool.meta["is_mcp_tool_call"] = json!(true);
    }
    if tool.raw_output["type"] == "MCP" {
        let result = &tool.raw_output["output"];
        let (text, error) = match (result["Error"].as_str(), result["OkayOutput"].as_str()) {
            (Some(error), _) => (error.to_owned(), true),
            (None, Some(text)) => (text.to_owned(), false),
            _ => return,
        };
        tool.raw_output = if error {
            json!({"isError": true, "content": [{"type": "text", "text": text}]})
        } else {
            json!({"content": [{"type": "text", "text": text}]})
        };
    }
}

/// Grok's bash results (`normalizeXAiAcpToolCallState`): a finished bash
/// call's status follows its exit code.
fn normalize_bash(tool: &mut Tool) {
    let bash = tool.raw_output["type"]
        .as_str()
        .is_some_and(|kind| kind.eq_ignore_ascii_case("bash"));
    if !bash || tool.status == "inProgress" {
        return;
    }
    match exit_code(&tool.raw_output) {
        Some(0) => tool.status = "completed".into(),
        Some(_) => tool.status = "failed".into(),
        None => {}
    }
}

/// A unified patch of the tool's `diff` content blocks, headers only.
fn unified_diff(tool: &Tool) -> Option<String> {
    let diffs: Vec<&Value> = tool
        .content
        .as_array()
        .into_iter()
        .flatten()
        .filter(|entry| entry["type"] == "diff")
        .collect();
    if diffs.is_empty() {
        return None;
    }
    let mut out = String::new();
    for diff in diffs {
        let path = diff["path"].as_str().unwrap_or_default();
        let old = diff["oldText"].as_str().unwrap_or_default();
        let new = diff["newText"].as_str().unwrap_or_default();
        out.push_str(&format!("--- {path}\n+++ {path}\n"));
        for line in old.lines() {
            out.push_str(&format!("-{line}\n"));
        }
        for line in new.lines() {
            out.push_str(&format!("+{line}\n"));
        }
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn grok_launches_in_the_runtime_modes_mode() {
        assert_eq!(
            grok_args("full-access"),
            ["agent", "--always-approve", "stdio"]
        );
        assert_eq!(
            grok_args("auto"),
            ["--permission-mode", "auto", "agent", "stdio"]
        );
        for mode in ["approval-required", "auto-accept-edits"] {
            assert_eq!(
                grok_args(mode),
                ["--permission-mode", "default", "agent", "stdio"]
            );
        }
    }

    #[test]
    fn the_runtime_mode_decides_which_requests_ask() {
        let asks = |flavor, mode, kind| matches!(disposition(flavor, mode, kind), Disposition::Ask);
        for flavor in [Flavor::Grok, Flavor::Registry] {
            assert!(!asks(flavor, "approval-required", "read"));
            assert!(asks(flavor, "approval-required", "execute"));
            assert!(asks(flavor, "auto-accept-edits", "edit"));
            assert!(!asks(flavor, "full-access", "execute"));
        }
        // Grok's auto mode asks about what its classifier refused; the
        // generic flavor's lets requests through.
        assert!(asks(Flavor::Grok, "auto", "read"));
        assert!(!asks(Flavor::Registry, "auto", "execute"));
    }

    #[test]
    fn groks_mcp_calls_take_the_generic_shapes() {
        let mut tool = Tool {
            raw_input: json!({"tool_name": "t3-code__t3_thread_list", "tool_input": {"limit": 2}}),
            raw_output: json!({"type": "MCP", "output": {"Error": "nope"}}),
            meta: json!({"x.ai/tool": {"name": "use_tool"}}),
            ..Tool::default()
        };
        normalize_mcp(&mut tool);
        assert_eq!(
            tool.raw_input,
            json!({"server": "t3-code", "tool": "t3_thread_list", "arguments": {"limit": 2}})
        );
        assert_eq!(
            tool.raw_output,
            json!({"isError": true, "content": [{"type": "text", "text": "nope"}]})
        );
    }

    #[test]
    fn a_finished_bash_call_follows_its_exit_code() {
        let mut tool = Tool {
            status: "completed".into(),
            raw_output: json!({"type": "Bash", "exitCode": 2}),
            ..Tool::default()
        };
        normalize_bash(&mut tool);
        assert_eq!(tool.status, "failed");
    }

    #[test]
    fn a_tools_command_comes_from_its_input_or_title() {
        let mut tool = Tool {
            raw_input: json!({"command": ["ls", "-la"]}),
            ..Tool::default()
        };
        assert_eq!(tool_command(&tool).as_deref(), Some("ls -la"));
        tool.raw_input = Value::Null;
        tool.kind = Some("execute".into());
        tool.title = Some("Run `echo hi`".into());
        assert_eq!(tool_command(&tool).as_deref(), Some("echo hi"));
    }
}
