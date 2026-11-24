//! Pi turns over its RPC mode (`pi --mode rpc`), as the reference's Pi
//! adapter (`provider-pi`) runs them.
//!
//! A thread keeps its Pi process across turns ([`Sessions`]). Pi's session
//! file is the native thread: a new process for a thread whose earlier one
//! stopped resumes it with `--session <file>`. T3's tools, the runtime mode's
//! approval hook and the system prompt's instructions reach Pi through the
//! reference's extension file (`pi_extension.ts`), which the server writes
//! and passes with `--extension`.
//!
//! Requests carry `t3-<n>` ids and wait for their `response`; a turn's
//! `prompt` (`t3-prompt-<n>`) does not, and its answer, with every event Pi
//! writes, is handled in order. A turn ends once Pi says its agent settled
//! and a `get_state` finds it idle; its entries since the last turn name the
//! turn, and its session statistics give the turn's usage.
use super::{cancelled, next_response, program, Cli, Launch, Sink, TurnEnd};
use crate::workspace::provider::{
    ProviderFailure, ProviderItem, ProviderTurn, RuntimeResponse, ThreadDisposition, TokenUsage,
    TurnStatus, PI,
};
use crate::workspace::{ProviderEvent, TurnSpec};
use serde_json::{json, Value};
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};
use tokio::time::Instant;

/// A request's default timeout (`PI_REQUEST_TIMEOUT_MS`).
const REQUEST_TIMEOUT: Duration = Duration::from_secs(15);
/// Registering a resumed session (`PI_SESSION_TIMEOUT_MS`).
const SESSION_TIMEOUT: Duration = Duration::from_secs(60);
/// A probe: the settle check, the turn's statistics, Stop's requests.
const PROBE_TIMEOUT: Duration = Duration::from_secs(2);
/// How often streamed text is shown again while it grows (`STREAM_FLUSH_MS`).
const STREAM_FLUSH: Duration = Duration::from_millis(50);
/// How long a process stays open after its thread's last turn.
const IDLE_RELEASE_DELAY: Duration = Duration::from_secs(30 * 60);
/// The environment the extension reads.
const URL_ENV: &str = "T3_MCP_URL";
const TOKEN_ENV: &str = "T3_MCP_BEARER_TOKEN";
const RUNTIME_MODE_ENV: &str = "T3_PI_RUNTIME_MODE";
/// Where the server wrote the extension (also the extension's own variable).
pub(crate) const EXTENSION_PATH_ENV: &str = "T3_PI_MCP_EXTENSION_PATH";
/// The extension's file name in the server's caches.
pub(crate) const EXTENSION_FILE: &str = "pi-t3-mcp-extension.ts";
/// The extension's source, the reference's `PI_T3_MCP_EXTENSION_SOURCE`
/// (scripts/reference_pi_extension.mjs).
pub(crate) const EXTENSION_SOURCE: &str = include_str!("pi_extension.ts");
const MODEL_ERROR: &str = "Pi reported a model error.";
const PROMPT_REJECTED: &str = "Pi rejected the prompt.";
const PROCESS_EXITED: &str = "Pi process exited unexpectedly.";

/// Write the extension where Pi loads it, unless it already holds exactly
/// these bytes (`materializePiT3McpExtension`): through a temporary file
/// renamed over it, so Pi never reads half a file.
pub(crate) fn materialize_extension(
    caches: &std::path::Path,
) -> std::io::Result<std::path::PathBuf> {
    std::fs::create_dir_all(caches)?;
    let path = caches.join(EXTENSION_FILE);
    if std::fs::read(&path).is_ok_and(|bytes| bytes == EXTENSION_SOURCE.as_bytes()) {
        return Ok(path);
    }
    let staging = tempfile::Builder::new()
        .prefix(".pi-t3-mcp-extension-")
        .tempdir_in(caches)?;
    let temporary = staging.path().join(EXTENSION_FILE);
    std::fs::write(&temporary, EXTENSION_SOURCE)?;
    std::fs::rename(&temporary, &path)?;
    Ok(path)
}

/// The thread's Pi processes, by app thread.
#[derive(Default)]
pub(crate) struct Sessions {
    live: Mutex<HashMap<String, Live>>,
}

struct Live {
    pi: Pi,
    idle: tokio::task::JoinHandle<()>,
}

impl Sessions {
    fn live(&self) -> std::sync::MutexGuard<'_, HashMap<String, Live>> {
        self.live.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// The thread's running process, when it was launched as `launch` and
    /// holds `session`; any other is stopped.
    async fn take(&self, thread: &str, launch: &str, session: Option<&str>) -> Option<Pi> {
        let live = self.live().remove(thread)?;
        live.idle.abort();
        let mut pi = live.pi;
        if pi.launch == launch
            && pi.cli.alive()
            && session.is_none_or(|session| session == pi.session_file)
        {
            return Some(pi);
        }
        pi.cli.stop().await;
        None
    }

    fn keep(self: &Arc<Self>, thread: String, pi: Pi) {
        let sessions: Weak<Self> = Arc::downgrade(self);
        let key = thread.clone();
        let idle = tokio::spawn(async move {
            tokio::time::sleep(IDLE_RELEASE_DELAY).await;
            if let Some(sessions) = sessions.upgrade() {
                let live = sessions.live().remove(&key);
                if let Some(live) = live {
                    live.pi.cli.stop().await;
                }
            }
        });
        if let Some(previous) = self.live().insert(thread, Live { pi, idle }) {
            previous.idle.abort();
            tokio::spawn(previous.pi.cli.stop());
        }
    }

    /// Stop the process holding the native `session`, if one runs (a detach).
    pub(crate) fn close(&self, session: &str) {
        let mut live = self.live();
        let threads: Vec<String> = live
            .iter()
            .filter(|(_, live)| live.pi.session_file == session)
            .map(|(thread, _)| thread.clone())
            .collect();
        for thread in threads {
            if let Some(live) = live.remove(&thread) {
                live.idle.abort();
                tokio::spawn(live.pi.cli.stop());
            }
        }
    }

    pub(crate) async fn shutdown(&self) {
        let all: Vec<Live> = self.live().drain().map(|(_, live)| live).collect();
        for live in all {
            live.idle.abort();
            live.pi.cli.stop().await;
        }
    }
}

/// Why a request to Pi failed.
#[derive(Debug)]
enum PiError {
    /// Pi closed its output.
    Closed,
    TimedOut,
    /// Pi answered `success: false`, with its error text.
    Failed(Option<String>),
}

/// One Pi process and what the turns before applied to it.
struct Pi {
    cli: Cli,
    /// What it was launched with: a turn launched otherwise starts another.
    launch: String,
    next_id: u64,
    next_prompt: u64,
    /// The session file: the native thread.
    session_file: String,
    /// The session's last entry, from which a turn's entries are read.
    leaf: Option<String>,
    /// The models' context windows, by `provider/id`.
    windows: HashMap<String, u64>,
    /// The context window of the model the session is on.
    window: Option<u64>,
    /// The model and thinking level the session started with.
    baseline_model: Option<(String, String)>,
    baseline_thinking: Option<String>,
    /// What turns applied: the model's slug, the thinking level, the title.
    applied_model: Option<String>,
    applied_thinking: Option<String>,
    applied_title: Option<String>,
    /// Records read while waiting for a response, for the turn to handle in
    /// order.
    queued: VecDeque<Value>,
    /// Dialogs allowed "for this session" (`<title length>:<title><message>`).
    granted: std::collections::HashSet<String>,
}

impl Pi {
    async fn write(&mut self, record: &Value) -> Result<(), PiError> {
        self.cli.write(record).await.map_err(|_| PiError::Closed)
    }

    /// Send a `t3-<n>` request and wait for its response; whatever else Pi
    /// writes meanwhile is queued.
    async fn request(&mut self, mut record: Value, timeout: Duration) -> Result<Value, PiError> {
        let id = format!("t3-{}", self.next_id);
        self.next_id += 1;
        record["id"] = json!(id);
        self.write(&record).await?;
        let deadline = Instant::now() + timeout;
        loop {
            let line = tokio::select! {
                line = self.cli.next_line() => line,
                _ = tokio::time::sleep_until(deadline) => return Err(PiError::TimedOut),
            };
            let Some(line) = line else {
                return Err(PiError::Closed);
            };
            let Ok(message) =
                serde_json::from_str::<Value>(line.strip_suffix('\r').unwrap_or(&line))
            else {
                continue;
            };
            if !message.is_object() {
                continue;
            }
            if message["type"] == "response" && message["id"] == id.as_str() {
                return if message["success"] == true {
                    Ok(message["data"].clone())
                } else {
                    Err(PiError::Failed(
                        message["error"].as_str().map(str::to_owned),
                    ))
                };
            }
            self.queued.push_back(message);
        }
    }

    async fn ask(&mut self, kind: &str, timeout: Duration) -> Result<Value, PiError> {
        self.request(json!({"type": kind}), timeout).await
    }

    /// `registerThread`: the session file, the models' windows and the
    /// session's last entry.
    async fn register(&mut self, resumed: bool) -> Result<(), ProviderFailure> {
        let timeout = if resumed {
            SESSION_TIMEOUT
        } else {
            REQUEST_TIMEOUT
        };
        let state = self
            .ask("get_state", timeout)
            .await
            .map_err(|error| open_failure(&error, "get_state"))?;
        let Some(file) = state["sessionFile"].as_str().filter(|f| !f.is_empty()) else {
            return Err(ProviderFailure::new(
                "provider_error",
                "get_state returned no persisted sessionFile",
            ));
        };
        self.session_file = file.to_owned();
        if let Ok(models) = self.ask("get_available_models", REQUEST_TIMEOUT).await {
            for model in models["models"].as_array().into_iter().flatten() {
                if let (Some(provider), Some(id), Some(window)) = (
                    model["provider"].as_str(),
                    model["id"].as_str(),
                    model["contextWindow"].as_u64(),
                ) {
                    self.windows.insert(format!("{provider}/{id}"), window);
                }
            }
        }
        let model = &state["model"];
        self.window = model["contextWindow"].as_u64();
        if let (Some(provider), Some(id)) = (model["provider"].as_str(), model["id"].as_str()) {
            self.baseline_model = Some((provider.to_owned(), id.to_owned()));
        }
        self.baseline_thinking = state["thinkingLevel"].as_str().map(str::to_owned);
        self.leaf = self
            .ask("get_entries", REQUEST_TIMEOUT)
            .await
            .ok()
            .and_then(|entries| entries["leafId"].as_str().map(str::to_owned));
        Ok(())
    }

    /// `applySelection`: the turn's model and thinking level.
    async fn apply_selection(&mut self, provider: &ProviderTurn) -> Result<(), ProviderFailure> {
        let model = match provider.model.trim() {
            "" => "default",
            model => model,
        };
        let thinking = provider
            .reasoning_effort
            .as_deref()
            .filter(|level| THINKING_LEVELS.contains(level));
        if self.applied_model.is_some() || self.applied_thinking.is_some() {
            // An extension may have changed the model on its own.
            if let Ok(state) = self.ask("get_state", REQUEST_TIMEOUT).await {
                let current = state["model"]["provider"]
                    .as_str()
                    .zip(state["model"]["id"].as_str())
                    .map(|(provider, id)| format!("{provider}/{id}"));
                if self.applied_model.is_some() && current != self.applied_model {
                    self.applied_model = None;
                }
            }
        }
        if model == "default" {
            if self.applied_model.take().is_some() {
                if let Some((provider, id)) = self.baseline_model.clone() {
                    self.set_model(&provider, &id).await?;
                }
            }
            if thinking.is_none() && self.applied_thinking.is_some() {
                if let Some(level) = self.baseline_thinking.clone() {
                    self.set_thinking(&level).await?;
                }
            }
        } else if self.applied_model.as_deref() != Some(model) {
            let (provider_id, model_id) = self.resolve_model(model).ok_or_else(|| {
                ProviderFailure::new(
                    "provider_error",
                    format!("Pi model '{model}' must use provider/model format"),
                )
            })?;
            self.set_model(&provider_id, &model_id).await?;
            self.applied_model = Some(model.to_owned());
            // Pi resets the thinking level with the model.
            self.applied_thinking = None;
        }
        if let Some(level) =
            thinking.filter(|level| self.applied_thinking.as_deref() != Some(level))
        {
            self.set_thinking(level).await?;
            self.applied_thinking = Some(level.to_owned());
        }
        Ok(())
    }

    /// A slug's provider and model: an exact discovered `provider/id`, else
    /// split at its first `/`.
    fn resolve_model(&self, slug: &str) -> Option<(String, String)> {
        if self.windows.contains_key(slug) {
            if let Some((provider, id)) = slug.split_once('/') {
                return Some((provider.to_owned(), id.to_owned()));
            }
        }
        slug.split_once('/')
            .filter(|(provider, id)| !provider.is_empty() && !id.is_empty())
            .map(|(provider, id)| (provider.to_owned(), id.to_owned()))
    }

    async fn set_model(&mut self, provider: &str, id: &str) -> Result<(), ProviderFailure> {
        let answer = self
            .request(
                json!({"type": "set_model", "provider": provider, "modelId": id}),
                REQUEST_TIMEOUT,
            )
            .await
            .map_err(|error| request_failure(&error, "set_model"))?;
        self.window = answer["contextWindow"]
            .as_u64()
            .or_else(|| self.windows.get(&format!("{provider}/{id}")).copied());
        Ok(())
    }

    async fn set_thinking(&mut self, level: &str) -> Result<(), ProviderFailure> {
        self.request(
            json!({"type": "set_thinking_level", "level": level}),
            REQUEST_TIMEOUT,
        )
        .await
        .map(|_| ())
        .map_err(|error| request_failure(&error, "set_thinking_level"))
    }
}

const THINKING_LEVELS: [&str; 7] = ["off", "minimal", "low", "medium", "high", "xhigh", "max"];

fn request_failure(error: &PiError, operation: &str) -> ProviderFailure {
    let detail = match error {
        PiError::Closed => "pi process closed stdout".to_owned(),
        PiError::TimedOut => format!("timed out after {}ms", REQUEST_TIMEOUT.as_millis()),
        PiError::Failed(text) => text.clone().unwrap_or_default(),
    };
    ProviderFailure::new(
        "provider_error",
        format!("Pi RPC {operation} failed: {detail}."),
    )
}

fn open_failure(error: &PiError, operation: &str) -> ProviderFailure {
    request_failure(error, operation)
}

/// What a launch is keyed by.
fn launch_key(spec: &TurnSpec, provider: &ProviderTurn) -> String {
    json!([
        provider.binary_path,
        provider.launch_args,
        spec.env,
        spec.cwd,
        provider.runtime_mode(),
        provider
            .mcp
            .as_ref()
            .map(|mcp| (&mcp.url, &mcp.authorization)),
    ])
    .to_string()
}

/// The session file to resume, when its first line names this folder.
fn resumable(session_file: &str, cwd: &str) -> bool {
    let Ok(text) = std::fs::read_to_string(session_file) else {
        return true;
    };
    let Some(header) = text.lines().next() else {
        return true;
    };
    let Ok(header) = serde_json::from_str::<Value>(header) else {
        return true;
    };
    match header["cwd"].as_str() {
        None => true,
        Some(at) => {
            at == cwd || std::fs::canonicalize(cwd).is_ok_and(|real| real.as_os_str() == at)
        }
    }
}

async fn spawn(
    spec: &TurnSpec,
    provider: &ProviderTurn,
    launch: String,
    session: Option<&str>,
) -> Result<Pi, ProviderFailure> {
    let launch_args =
        crate::provider_status::pi::resolve_launch_tokens(provider.launch_args.clone())
            .map_err(|message| ProviderFailure::new("provider_error", message))?;
    let extension = spec
        .env
        .iter()
        .find(|(name, _)| name == EXTENSION_PATH_ENV)
        .map(|(_, path)| path.replace('\\', "/"))
        .unwrap_or_default();
    let mut args = vec!["--mode".to_owned(), "rpc".to_owned()];
    if let Some(file) = session {
        args.extend(["--session".to_owned(), file.to_owned()]);
    }
    let already = launch_args.windows(2).any(|pair| {
        matches!(pair[0].as_str(), "--extension" | "-e")
            && pair[1].replace('\\', "/").trim_end_matches('/') == extension.trim_end_matches('/')
    });
    args.extend(launch_args);
    if !extension.is_empty() && !already {
        args.extend(["--extension".to_owned(), extension]);
    }
    let mut extra_env = vec![(
        "T3CODE_THREAD_ID".to_owned(),
        provider.app_thread_id.clone(),
    )];
    if !provider.runtime_mode.is_empty() {
        let mode = match provider.runtime_mode.as_str() {
            "auto" => "approval-required",
            mode => mode,
        };
        extra_env.push((RUNTIME_MODE_ENV.into(), mode.into()));
    }
    if let Some(mcp) = &provider.mcp {
        extra_env.push((URL_ENV.into(), mcp.url.clone()));
        let token = mcp
            .authorization
            .strip_prefix("Bearer ")
            .unwrap_or(&mcp.authorization);
        extra_env.push((TOKEN_ENV.into(), token.to_owned()));
    }
    let mut spec = spec.clone();
    // Only this turn's MCP endpoint, never one the server inherited.
    spec.env_remove
        .extend([URL_ENV.to_owned(), TOKEN_ENV.to_owned()]);
    let cli = Cli::spawn(
        &spec,
        Launch {
            program: program(&provider.binary_path, "pi"),
            args,
            home_var: "PI_CODING_AGENT_DIR",
            home: "",
            extra_env,
        },
    )
    .map_err(|error| ProviderFailure::new("provider_error", error.to_string()))?;
    Ok(Pi {
        cli,
        launch,
        next_id: 0,
        next_prompt: 0,
        session_file: String::new(),
        leaf: None,
        windows: HashMap::new(),
        window: None,
        baseline_model: None,
        baseline_thinking: None,
        applied_model: None,
        applied_thinking: None,
        applied_title: None,
        queued: VecDeque::new(),
        granted: std::collections::HashSet::new(),
    })
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
    let opened = tokio::select! {
        opened = open(&spec, &provider, &sessions, &thread, launch, resume.as_deref()) => opened,
        _ = cancelled(&mut cancel) => return sink.finish(TurnEnd::interrupted(true)),
    };
    let (mut pi, resumed) = match opened {
        Ok(opened) => opened,
        Err(failure) => return sink.finish(TurnEnd::failed(failure, ThreadDisposition::Reusable)),
    };
    let mut turn = Turn::new(&provider, &sink, &mut responses);
    let ended = turn
        .run(&spec, &provider, &mut pi, &mut cancel, resumed)
        .await;
    match ended {
        Ended::Settled(end) => {
            sink.finish(end);
            sessions.keep(thread, pi);
        }
        Ended::Stopped(end) => {
            sink.finish(end);
            pi.cli.stop().await;
        }
    }
}

/// The thread's process: its running one, or a new one (resuming the
/// thread's session file when its header names this folder), registered.
async fn open(
    spec: &TurnSpec,
    provider: &ProviderTurn,
    sessions: &Sessions,
    thread: &str,
    launch: String,
    resume: Option<&str>,
) -> Result<(Pi, bool), ProviderFailure> {
    if let Some(pi) = sessions.take(thread, &launch, resume).await {
        return Ok((pi, false));
    }
    let session = resume.filter(|file| resumable(file, &spec.cwd));
    let mut pi = spawn(spec, provider, launch, session).await?;
    match pi.register(session.is_some()).await {
        Ok(()) => Ok((pi, session.is_some())),
        Err(failure) => {
            pi.cli.stop().await;
            Err(failure)
        }
    }
}

enum Ended {
    /// The turn settled; the process serves the thread's next turn.
    Settled(TurnEnd),
    /// Stop, or the process ended: it is stopped.
    Stopped(TurnEnd),
}

/// A text or thinking block as it streams.
struct Block {
    id: String,
    kind: &'static str,
    text: String,
    dirty: bool,
    published: bool,
    completed: bool,
}

struct Turn<'a> {
    sink: &'a Sink,
    responses: &'a mut mpsc::UnboundedReceiver<RuntimeResponse>,
    /// The provider turn's id, which its items' ids extend.
    provider_turn: String,
    prompt_id: String,
    /// The assistant messages seen (`m<k>`).
    message_ordinal: u64,
    /// Open blocks by content index, of the current message.
    blocks: HashMap<u64, Block>,
    flush_at: Option<Instant>,
    /// Tools by call id: their args and the output bytes already reported.
    tools: HashMap<String, (Value, usize)>,
    model: Option<String>,
    failure: Option<ProviderFailure>,
    interrupted: bool,
    last_usage: Option<TokenUsage>,
    reported_tokens: u64,
    /// The thread's runtime mode, which a request records.
    runtime_mode: String,
    /// Dialogs waiting for the user: Pi's id and the dialog's key.
    dialogs: HashMap<String, (String, String)>,
}

impl<'a> Turn<'a> {
    fn new(
        provider: &ProviderTurn,
        sink: &'a Sink,
        responses: &'a mut mpsc::UnboundedReceiver<RuntimeResponse>,
    ) -> Self {
        Self {
            sink,
            responses,
            provider_turn: format!(
                "provider-turn:provider:pi:native-turn:{}",
                crate::thread_projection::uri_component(&provider.native_turn)
            ),
            prompt_id: String::new(),
            message_ordinal: 0,
            blocks: HashMap::new(),
            flush_at: None,
            tools: HashMap::new(),
            model: None,
            failure: None,
            interrupted: false,
            last_usage: None,
            reported_tokens: 0,
            runtime_mode: provider.runtime_mode().to_owned(),
            dialogs: HashMap::new(),
        }
    }

    async fn run(
        &mut self,
        spec: &TurnSpec,
        provider: &ProviderTurn,
        pi: &mut Pi,
        cancel: &mut Option<oneshot::Receiver<()>>,
        resumed: bool,
    ) -> Ended {
        self.sink.send(ProviderEvent::ProviderThread {
            driver: PI.into(),
            native_thread_id: pi.session_file.clone(),
            resumed,
            model: None,
        });
        if let Err(failure) = pi.apply_selection(provider).await {
            return Ended::Settled(TurnEnd::failed(failure, ThreadDisposition::Reusable));
        }
        let title = provider.thread_title.trim();
        if !title.is_empty() && pi.applied_title.as_deref() != Some(title) {
            let named = pi
                .request(
                    json!({"type": "set_session_name", "name": title}),
                    REQUEST_TIMEOUT,
                )
                .await;
            if named.is_ok() {
                pi.applied_title = Some(title.to_owned());
            }
        }
        self.prompt_id = format!("t3-prompt-{}", pi.next_prompt);
        pi.next_prompt += 1;
        let mut prompt =
            json!({"type": "prompt", "message": spec.prompt, "streamingBehavior": "steer"});
        let images = images(&provider.attachments);
        if !images.is_empty() {
            prompt["images"] = Value::Array(images);
        }
        prompt["id"] = json!(self.prompt_id);
        if pi.write(&prompt).await.is_err() {
            return self.exited(pi).await;
        }
        self.sink.send(ProviderEvent::ProviderTurn {
            native_turn_id: Some(provider.native_turn.clone()),
            native_turn_ref: None,
            conversation_head: None,
            status: TurnStatus::Running,
            model: None,
        });
        loop {
            let message = if let Some(queued) = pi.queued.pop_front() {
                Some(queued)
            } else {
                let line = tokio::select! {
                    line = pi.cli.next_line() => line,
                    _ = cancelled(cancel) => return self.stop(pi).await,
                    response = next_response(self.responses) => {
                        self.respond(pi, response).await;
                        continue;
                    }
                    _ = super::sleep_until(self.flush_at) => {
                        self.flush();
                        continue;
                    }
                };
                let Some(line) = line else {
                    return self.exited(pi).await;
                };
                serde_json::from_str::<Value>(line.strip_suffix('\r').unwrap_or(&line)).ok()
            };
            let Some(message) = message.filter(Value::is_object) else {
                continue;
            };
            if let Some(ended) = self.handle(pi, message).await {
                return ended;
            }
        }
    }

    /// One record: an event, or the prompt's answer.
    async fn handle(&mut self, pi: &mut Pi, message: Value) -> Option<Ended> {
        match message["type"].as_str().unwrap_or_default() {
            "response" => {
                let ours = message["id"] == self.prompt_id.as_str()
                    || (message["id"].is_null() && message["command"] == "prompt");
                if ours && message["success"] == false && self.failure.is_none() {
                    let text = message["error"].as_str().unwrap_or(PROMPT_REJECTED);
                    self.failure = Some(ProviderFailure::new("provider_error", text));
                    return Some(self.finalize(pi, true).await);
                }
                if ours && message["data"]["disposition"] == "handled" {
                    return self.settle(pi).await;
                }
                None
            }
            "message_start" => {
                if message["message"]["role"] == "assistant" {
                    self.message_ordinal += 1;
                }
                None
            }
            "message_update" => {
                self.usage(&message["usage"], pi.window);
                let event = &message["assistantMessageEvent"];
                let index = event["contentIndex"].as_u64().unwrap_or(0);
                match event["type"].as_str().unwrap_or_default() {
                    "text_start" | "thinking_start" => {
                        if let Some(block) = self.blocks.get_mut(&index).filter(|b| !b.completed) {
                            block.text.clear();
                        }
                    }
                    kind @ ("text_delta" | "thinking_delta") => {
                        let delta = event["delta"].as_str().unwrap_or_default();
                        self.block(index, kind == "text_delta").text.push_str(delta);
                        self.block(index, kind == "text_delta").dirty = true;
                        if self.flush_at.is_none() {
                            self.flush_at = Some(Instant::now() + STREAM_FLUSH);
                        }
                    }
                    kind @ ("text_end" | "thinking_end") => {
                        let text = event["content"]
                            .as_str()
                            .or(event["thinking"].as_str())
                            .unwrap_or_default()
                            .to_owned();
                        let block = self.block(index, kind == "text_end");
                        if !text.is_empty() {
                            block.text = text;
                        }
                        self.complete_block(index);
                    }
                    _ => {}
                }
                None
            }
            "message_end" => {
                let role = &message["message"]["role"];
                if role == "assistant" {
                    self.complete_blocks();
                    let reported = &message["message"];
                    if let (Some(provider), Some(model)) =
                        (reported["provider"].as_str(), reported["model"].as_str())
                    {
                        self.model = Some(format!("{provider}/{model}"));
                    }
                    if reported["stopReason"] == "error" && self.failure.is_none() {
                        let text = reported["errorMessage"]
                            .as_str()
                            .filter(|text| !text.is_empty())
                            .unwrap_or(MODEL_ERROR);
                        self.failure = Some(ProviderFailure::new("provider_error", text));
                    }
                }
                None
            }
            "tool_execution_start" | "tool_execution_update" | "tool_execution_end" => {
                self.tool(&message);
                None
            }
            "agent_settled" => self.settle(pi).await,
            "extension_ui_request" => {
                self.dialog(pi, &message).await;
                None
            }
            _ => None,
        }
    }

    /// An extension's dialog (`extension_ui_request`): the approval hook's
    /// `confirm` asks the user, unless "for this session" already answered
    /// it. Other dialogs (questions) are not offered yet: they are
    /// cancelled, so Pi does not wait on them. Notices need no answer.
    async fn dialog(&mut self, pi: &mut Pi, message: &Value) {
        let Some(id) = message["id"].as_str().map(str::to_owned) else {
            return;
        };
        match message["method"].as_str().unwrap_or_default() {
            "confirm" => {
                let title = message["title"].as_str().unwrap_or_default().to_owned();
                let body = message["message"].as_str().unwrap_or_default().to_owned();
                let key = format!("{}:{title}{body}", title.chars().count());
                if pi.granted.contains(&key) {
                    let _ = pi
                        .write(
                            &json!({"type": "extension_ui_response", "id": id, "confirmed": true}),
                        )
                        .await;
                    return;
                }
                let kind = if matches!(title.as_str(), "Allow edit?" | "Allow write?") {
                    "file-change"
                } else {
                    "command"
                };
                let mut open = super::requests::open(&id, &id, kind, &self.runtime_mode);
                let prompt = if body.is_empty() { title.clone() } else { body };
                open.fields.insert("prompt".into(), json!(prompt));
                open.fields.insert("title".into(), json!(title));
                self.dialogs.insert(open.id.clone(), (id, key));
                self.sink
                    .send(ProviderEvent::RuntimeRequestOpened { request: open });
            }
            "select" | "input" | "editor" => {
                let _ = pi
                    .write(&json!({"type": "extension_ui_response", "id": id, "cancelled": true}))
                    .await;
            }
            _ => {}
        }
    }

    /// The user's answer to a dialog.
    async fn respond(&mut self, pi: &mut Pi, response: RuntimeResponse) {
        // A message that steers the turn (`steerTurn`): another prompt Pi
        // reads at its next step. Its answer is the prompt's own.
        if let Some(text) = &response.steer {
            let id = format!("t3-prompt-{}", pi.next_prompt);
            pi.next_prompt += 1;
            let _ = pi
                .write(
                    &json!({"type": "prompt", "message": text, "streamingBehavior": "steer",
                    "id": id}),
                )
                .await;
            return;
        }
        let Some((id, key)) = self.dialogs.remove(&response.id) else {
            return;
        };
        let answer = match super::requests::decision(&response) {
            "accept" => json!({"confirmed": true}),
            "acceptForSession" | "acceptAlways" => {
                pi.granted.insert(key);
                json!({"confirmed": true})
            }
            "decline" => json!({"confirmed": false}),
            _ => json!({"cancelled": true}),
        };
        let mut record = json!({"type": "extension_ui_response", "id": id});
        if let (Some(record), Some(answer)) = (record.as_object_mut(), answer.as_object()) {
            record.extend(answer.clone());
        }
        let _ = pi.write(&record).await;
    }

    /// Waiting dialogs end with the turn: Pi hears them cancelled.
    async fn cancel_dialogs(&mut self, pi: &mut Pi) {
        let mut waiting: Vec<(String, (String, String))> = self.dialogs.drain().collect();
        waiting.sort();
        for (request, (id, _)) in waiting {
            let _ = pi
                .write(&json!({"type": "extension_ui_response", "id": id, "cancelled": true}))
                .await;
            self.sink.send(ProviderEvent::RuntimeRequestClosed {
                id: request,
                status: "cancelled".into(),
            });
        }
    }

    /// After `agent_settled`: finish the turn once Pi is idle.
    async fn settle(&mut self, pi: &mut Pi) -> Option<Ended> {
        let mut attempts = 0;
        let state = loop {
            match pi.ask("get_state", PROBE_TIMEOUT).await {
                Ok(state) => break state,
                Err(PiError::Closed) => return Some(self.exited(pi).await),
                Err(_) if attempts < 2 => {
                    attempts += 1;
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
                Err(_) => return Some(self.exited(pi).await),
            }
        };
        // What Pi wrote before the state, first.
        while let Some(queued) = pi.queued.pop_front() {
            if queued["type"] == "agent_settled" {
                continue;
            }
            if let Some(ended) = Box::pin(self.handle(pi, queued)).await {
                return Some(ended);
            }
        }
        let idle = state["isStreaming"] != true
            && state["isCompacting"] != true
            && state["pendingMessageCount"].as_u64().unwrap_or(0) == 0;
        if !idle {
            return None;
        }
        if let Some(file) = state["sessionFile"].as_str().filter(|f| !f.is_empty()) {
            pi.session_file = file.to_owned();
        }
        Some(self.finalize(pi, true).await)
    }

    /// `finalizeTurn`: the open items end, the turn's entries name it, its
    /// statistics give its usage, then its end.
    async fn finalize(&mut self, pi: &mut Pi, read_usage: bool) -> Ended {
        self.complete_blocks();
        self.cancel_dialogs(pi).await;
        let reference = self.entries(pi, REQUEST_TIMEOUT).await;
        if read_usage {
            if let Ok(stats) = pi.ask("get_session_stats", PROBE_TIMEOUT).await {
                self.final_usage(&stats);
            }
        }
        let status = if self.interrupted {
            TurnStatus::Interrupted
        } else if self.failure.is_some() {
            TurnStatus::Failed
        } else {
            TurnStatus::Completed
        };
        self.sink.send(ProviderEvent::ProviderTurn {
            native_turn_id: None,
            native_turn_ref: reference,
            conversation_head: pi.leaf.clone(),
            status,
            model: self.model.clone(),
        });
        let end = match status {
            TurnStatus::Interrupted => TurnEnd::interrupted(true),
            TurnStatus::Failed => TurnEnd::failed(
                self.failure
                    .take()
                    .unwrap_or_else(|| ProviderFailure::new("provider_error", MODEL_ERROR)),
                ThreadDisposition::Reusable,
            ),
            _ => TurnEnd::completed(),
        };
        if self.interrupted {
            Ended::Stopped(end)
        } else {
            Ended::Settled(end)
        }
    }

    /// The turn's entries since the last turn's leaf: the earliest user
    /// message among them names the turn (`Value::Null` when there are
    /// none); `None` when they cannot be read.
    async fn entries(&mut self, pi: &mut Pi, timeout: Duration) -> Option<Value> {
        let mut request = json!({"type": "get_entries"});
        if let Some(leaf) = &pi.leaf {
            request["since"] = json!(leaf);
        }
        let listing = pi.request(request, timeout).await.ok()?;
        let entries: HashMap<&str, &Value> = listing["entries"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|entry| Some((entry["id"].as_str()?, entry)))
            .collect();
        let old = pi.leaf.clone();
        let new_leaf = listing["leafId"].as_str().map(str::to_owned);
        let mut at = new_leaf.clone();
        let mut user = None;
        let mut walked = false;
        while let Some(id) = at.take() {
            if Some(&id) == old.as_ref() {
                break;
            }
            let Some(entry) = entries.get(id.as_str()) else {
                break;
            };
            walked = true;
            if entry["message"]["role"] == "user" {
                user = Some(id.clone());
            }
            at = entry["parentId"].as_str().map(str::to_owned);
        }
        pi.leaf = new_leaf;
        Some(match (walked, user) {
            (true, Some(id)) => json!({"driver": PI, "nativeId": id, "strength": "strong"}),
            _ => Value::Null,
        })
    }

    /// Stop (`interruptTurn` with a restart): clear Pi's queue, abort, read
    /// where the session is, then the process is stopped.
    async fn stop(&mut self, pi: &mut Pi) -> Ended {
        self.interrupted = true;
        self.cancel_dialogs(pi).await;
        let _ = pi.ask("clear_queue", PROBE_TIMEOUT).await;
        let _ = pi.ask("abort", PROBE_TIMEOUT).await;
        if let Ok(state) = pi.ask("get_state", PROBE_TIMEOUT).await {
            if let Some(file) = state["sessionFile"].as_str().filter(|f| !f.is_empty()) {
                pi.session_file = file.to_owned();
            }
        }
        // The events up to now end the turn's items.
        while let Some(queued) = pi.queued.pop_front() {
            if matches!(queued["type"].as_str(), Some("agent_settled" | "response")) {
                continue;
            }
            let _ = Box::pin(self.handle(pi, queued)).await;
        }
        self.close_tools("interrupted");
        let reference = self.entries(pi, PROBE_TIMEOUT).await;
        self.complete_blocks();
        self.sink.send(ProviderEvent::ProviderTurn {
            native_turn_id: None,
            native_turn_ref: reference,
            conversation_head: pi.leaf.clone(),
            status: TurnStatus::Interrupted,
            model: self.model.clone(),
        });
        Ended::Stopped(TurnEnd::interrupted(true))
    }

    /// The process ended mid-turn.
    async fn exited(&mut self, pi: &mut Pi) -> Ended {
        let _ = pi.cli.exit_status().await;
        self.complete_blocks();
        self.close_tools("failed");
        let mut failure = ProviderFailure::new("transport_error", PROCESS_EXITED);
        failure.code = None;
        Ended::Stopped(TurnEnd::failed(failure, ThreadDisposition::Reusable))
    }

    fn block(&mut self, index: u64, text: bool) -> &mut Block {
        let id = format!("{}:m{}:c{index}", self.provider_turn, self.message_ordinal);
        self.blocks.entry(index).or_insert_with(|| Block {
            id,
            kind: if text {
                "assistant_message"
            } else {
                "reasoning"
            },
            text: String::new(),
            dirty: false,
            published: false,
            completed: false,
        })
    }

    fn emit(&self, block: &Block) {
        let status = if block.completed {
            "completed"
        } else {
            "running"
        };
        let item = ProviderItem::new(block.id.clone(), block.kind, status)
            .with("text", block.text.clone())
            .with("streaming", !block.completed);
        self.sink.send(ProviderEvent::TurnItem { item });
    }

    fn flush(&mut self) {
        self.flush_at = None;
        let mut emitted = Vec::new();
        for (index, block) in self.blocks.iter_mut() {
            if block.dirty && !block.completed {
                block.dirty = false;
                block.published = true;
                emitted.push(*index);
            }
        }
        for index in emitted {
            self.emit(&self.blocks[&index]);
        }
    }

    fn complete_block(&mut self, index: u64) {
        let Some(block) = self.blocks.get_mut(&index) else {
            return;
        };
        if block.completed {
            return;
        }
        block.completed = true;
        if block.text.is_empty() && !block.published {
            return;
        }
        block.published = true;
        let block = &self.blocks[&index];
        self.emit(block);
    }

    fn complete_blocks(&mut self) {
        let mut indexes: Vec<u64> = self.blocks.keys().copied().collect();
        indexes.sort_unstable();
        for index in indexes {
            self.complete_block(index);
        }
        self.blocks.retain(|_, block| !block.completed);
        if self.blocks.is_empty() {
            self.flush_at = None;
        }
    }

    /// A tool's start, update or end (`emitToolItem`).
    fn tool(&mut self, event: &Value) {
        let Some(id) = event["toolCallId"].as_str() else {
            return;
        };
        let name = event["toolName"].as_str().unwrap_or_default().to_owned();
        let kind = event["type"].as_str().unwrap_or_default();
        let entry = self.tools.entry(id.to_owned()).or_insert((Value::Null, 0));
        if !event["args"].is_null() {
            entry.0 = event["args"].clone();
        }
        let args = entry.0.clone();
        let result = if kind == "tool_execution_end" {
            &event["result"]
        } else {
            &event["partialResult"]
        };
        let output: String = result["content"]
            .as_array()
            .into_iter()
            .flatten()
            .filter(|block| block["type"] == "text")
            .filter_map(|block| block["text"].as_str())
            .collect();
        let shell = matches!(name.as_str(), "bash" | "powershell");
        if kind == "tool_execution_update" && shell {
            // A command's output streams as deltas, not snapshots.
            let seen = entry.1;
            if output.len() > seen {
                let mut from = seen;
                while !output.is_char_boundary(from) {
                    from += 1;
                }
                entry.1 = output.len();
                self.sink.send(ProviderEvent::CommandOutputDelta {
                    item_id: id.to_owned(),
                    chunk: output[from..].to_owned(),
                });
            }
            return;
        }
        let status = match kind {
            "tool_execution_end" if event["isError"] == true && self.interrupted => "interrupted",
            "tool_execution_end" if event["isError"] == true => "failed",
            "tool_execution_end" => "completed",
            _ => "running",
        };
        let mut item = ProviderItem::new(id, "dynamic_tool", status).titled(name.clone());
        if shell {
            item.kind = "command_execution".into();
            item = item.with("input", args["command"].as_str().unwrap_or_default());
            if !output.is_empty() {
                item = item.with("output", output);
            }
            let code = result["structuredContent"]["exit_code"]
                .as_i64()
                .or(result["details"]["exitCode"].as_i64());
            if let Some(code) = code.filter(|_| kind == "tool_execution_end") {
                item = item.with("exitCode", code);
            }
        } else if matches!(name.as_str(), "edit" | "write")
            && (args["path"].is_string() || args["file_path"].is_string())
        {
            item.kind = "file_change".into();
            let path = args["path"]
                .as_str()
                .or(args["file_path"].as_str())
                .unwrap_or_default();
            item = item.with("fileName", path);
            let patch = result["details"]["patch"]
                .as_str()
                .map(str::to_owned)
                .or_else(|| {
                    (event["isError"] == true && !output.trim().is_empty()).then(|| output.clone())
                });
            if let Some(patch) = patch {
                item = item.with("diffStr", patch);
            }
            if name == "write" {
                if let Some(content) = args["content"].as_str() {
                    item = item.with("newStr", content);
                }
            }
        } else {
            item = item
                .with("toolName", name.clone())
                .with("input", if args.is_null() { json!({}) } else { args });
            if kind == "tool_execution_end" {
                let all_text = result["content"]
                    .as_array()
                    .is_some_and(|blocks| blocks.iter().all(|b| b["type"] == "text"));
                let value = if all_text && result["structuredContent"].is_null() {
                    json!(output)
                } else {
                    let mut value = json!({"content": result["content"]});
                    if !result["structuredContent"].is_null() {
                        value["structuredContent"] = result["structuredContent"].clone();
                    }
                    value
                };
                item = item.with("output", value);
            }
        }
        if kind == "tool_execution_end" {
            self.tools.remove(id);
        }
        self.sink.send(ProviderEvent::TurnItem { item });
    }

    /// Tools still running end with the turn.
    fn close_tools(&mut self, status: &str) {
        let _ = status;
        self.tools.clear();
    }

    /// Live usage (`message_update.usage`).
    fn usage(&mut self, usage: &Value, window: Option<u64>) {
        let total = usage["totalTokens"].as_u64().unwrap_or(0);
        if total == 0 {
            return;
        }
        let read = |key: &str| usage[key].as_u64().unwrap_or(0);
        let snapshot = TokenUsage {
            used_tokens: total,
            max_tokens: window,
            input_tokens: read("input") + read("cacheRead") + read("cacheWrite"),
            cached_input_tokens: read("cacheRead"),
            output_tokens: read("output"),
            ..TokenUsage::default()
        };
        self.last_usage = Some(snapshot.clone());
        if window.is_some() && total != self.reported_tokens {
            self.reported_tokens = total;
            self.sink.send(ProviderEvent::Usage { usage: snapshot });
        }
    }

    /// The turn's usage from Pi's session statistics.
    fn final_usage(&mut self, stats: &Value) {
        let (Some(tokens), Some(window)) = (
            stats["contextUsage"]["tokens"].as_u64(),
            stats["contextUsage"]["contextWindow"]
                .as_u64()
                .filter(|w| *w > 0),
        ) else {
            return;
        };
        let mut usage = self.last_usage.clone().unwrap_or_default();
        usage.used_tokens = tokens;
        usage.max_tokens = Some(window);
        self.sink.send(ProviderEvent::Usage { usage });
    }
}

/// The prompt's images: each image attachment's bytes.
fn images(attachments: &[Value]) -> Vec<Value> {
    use base64::Engine;
    attachments
        .iter()
        .filter_map(|attachment| {
            let mime = attachment["mimeType"].as_str()?;
            if !mime.starts_with("image/") {
                return None;
            }
            let bytes = std::fs::read(attachment["path"].as_str()?).ok()?;
            Some(json!({"type": "image",
                "data": base64::engine::general_purpose::STANDARD.encode(bytes),
                "mimeType": mime}))
        })
        .collect()
}
