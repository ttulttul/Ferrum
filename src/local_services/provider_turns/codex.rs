//! Codex turns over `codex app-server` (JSON-RPC lines on stdio), as the
//! reference's Codex adapter runs them.
use super::requests;
use super::{
    cancelled, codex_crashed, next_response, program, sleep_until, Cli, Launch, Sink, TurnEnd,
};
use crate::workspace::provider::{
    PlanStep, ProviderFailure, ProviderItem, ProviderTurn, RuntimeResponse, ThreadDisposition,
    TokenUsage, TurnStatus, CODEX, PROVIDER_RETRY_ITEM,
};
use crate::workspace::{ProviderEvent, TurnSpec};
use serde_json::{json, Value};

/// A T3 tool such as `request_secret` can wait an hour; Codex's MCP timeout stays above that.
const T3_MCP_TOOL_TIMEOUT_SEC: u64 = 65 * 60;
/// T3 tools Codex must call directly, never from Code Mode (`t3-code-direct`).
const DIRECT_ONLY_T3_TOOLS: [&str; 2] = ["t3_thread_wait", "delegate_task"];
use std::collections::HashMap;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};

/// The server version the reference reports to Codex.
const SERVER_VERSION: &str = "0.1.11";

/// How long an interrupted turn may take to report that it stopped.
const INTERRUPT_GRACE: Duration = Duration::from_secs(5);

/// The reference's PROVIDER_RETRY_BUDGET_MS: the longest a turn may stay in
/// provider retries before it is stopped as failed. Codex waits for the
/// network without limit, so counting attempts alone never ends a turn.
const PROVIDER_RETRY_BUDGET: Duration = Duration::from_secs(10 * 60);

/// The retry budget of `spec`'s turn; tests shorten it.
fn retry_budget(spec: &TurnSpec) -> Duration {
    #[cfg(test)]
    if let Some(ms) = spec
        .env
        .iter()
        .find(|(key, _)| key == "TEST_PROVIDER_RETRY_BUDGET_MS")
        .and_then(|(_, ms)| ms.parse().ok())
    {
        return Duration::from_millis(ms);
    }
    let _ = spec;
    PROVIDER_RETRY_BUDGET
}

pub(super) async fn run(
    spec: TurnSpec,
    provider: ProviderTurn,
    cancel: oneshot::Receiver<()>,
    responses: mpsc::UnboundedReceiver<RuntimeResponse>,
    sink: Sink,
    servers: std::sync::Arc<super::codex_server::Registry>,
) {
    let cli = match super::codex_server::acquire(&servers, &spec, &provider, &sink.log) {
        Ok(cli) => cli,
        Err(_) => {
            // The reference's CodexAppServerSpawnError.
            let binary = program(&provider.binary_path, "codex");
            let failure = ProviderFailure::new(
                "provider_error",
                format!(
                    "Failed to spawn Codex App Server process for command: {binary} app-server"
                ),
            );
            return sink.finish(TurnEnd::failed(failure, ThreadDisposition::Reusable));
        }
    };
    let mut session = Session {
        cli,
        sink: sink.clone(),
        cancel: Some(cancel),
        cancelled: false,
        responses,
        runtime_mode: provider.runtime_mode().to_owned(),
        pending: HashMap::new(),
        file_changes: HashMap::new(),
        subagents: Default::default(),
        child_reads: HashMap::new(),
        usage_baseline: None,
        rate_limits: None,
        turn_usage: TurnUsage::default(),
        thread_id: None,
        history_injected: false,
        turn_id: None,
        terminal: None,
        last_error: None,
        retry: None,
        recovered_retries: 0,
        retry_budget: retry_budget(&spec),
        stalled: None,
    };
    let end = session.turn(&spec, &provider).await;
    // A thread whose turn failed resumes on its next turn, as the
    // reference forgets what it loaded.
    if end.status == TurnStatus::Failed {
        if let Some(thread) = session.thread_id.as_deref() {
            session.cli.server.mark_loaded(thread, None);
        }
    }
    // The app-server stays for the instance's next turns.
    drop(session);
    sink.finish(end);
}

/// How long listing Codex's models may take, start-up included.
const MODEL_LIST_TIMEOUT: Duration = Duration::from_secs(20);

/// Codex's `model/list` entries, every page, from an app-server launched as
/// a turn of `spec` would launch it.
pub(super) async fn list_models(
    spec: &TurnSpec,
    provider: &ProviderTurn,
) -> anyhow::Result<Vec<Value>> {
    let binary = program(&provider.binary_path, "codex");
    let mut args = vec!["app-server".to_string()];
    args.extend(provider.launch_args.iter().cloned());
    let mut cli = Cli::spawn(
        spec,
        Launch {
            program: binary.clone(),
            args,
            home_var: "CODEX_HOME",
            home: &provider.home_path,
            extra_env: vec![(
                "CODEX_INTERNAL_ORIGINATOR_OVERRIDE".into(),
                "T3 Code".into(),
            )],
            log: None,
        },
    )
    .map_err(|e| anyhow::anyhow!("could not start {binary} app-server: {e}"))?;
    let listed = tokio::time::timeout(MODEL_LIST_TIMEOUT, async {
        let mut next_id = 1u64;
        let mut call = async |cli: &mut Cli, method: &str, params: Value| {
            let id = next_id;
            next_id += 1;
            cli.write(&json!({"id": id, "method": method, "params": params}))
                .await?;
            while let Some(line) = cli.next_line().await {
                let Ok(message) = serde_json::from_str::<Value>(&line) else {
                    continue;
                };
                if message.get("method").is_none() && message["id"].as_u64() == Some(id) {
                    return match message.get("error") {
                        Some(error) => Err(anyhow::anyhow!(
                            "{method}: {}",
                            error["message"].as_str().unwrap_or("Codex refused it")
                        )),
                        None => Ok(message["result"].clone()),
                    };
                }
            }
            Err(anyhow::anyhow!("Codex closed before answering {method}"))
        };
        call(
            &mut cli,
            "initialize",
            json!({"clientInfo": {"name": "T3 Code", "title": "T3 Code", "version": SERVER_VERSION},
                "capabilities": {"experimentalApi": true}}),
        )
        .await?;
        cli.write(&json!({"method": "initialized"})).await?;
        let mut models = Vec::new();
        let mut cursor: Option<String> = None;
        loop {
            let params = match &cursor {
                Some(cursor) => json!({"cursor": cursor}),
                None => json!({}),
            };
            let page = call(&mut cli, "model/list", params).await?;
            models.extend(page["data"].as_array().cloned().unwrap_or_default());
            cursor = page["nextCursor"]
                .as_str()
                .filter(|c| !c.is_empty())
                .map(str::to_owned);
            if cursor.is_none() {
                break Ok::<_, anyhow::Error>(models);
            }
        }
    })
    .await;
    cli.stop().await;
    listed.map_err(|_| anyhow::anyhow!("Codex did not list its models in time"))?
}

struct Session {
    cli: super::codex_server::Conn,
    sink: Sink,
    cancel: Option<oneshot::Receiver<()>>,
    cancelled: bool,
    /// Answers to the turn's runtime requests.
    responses: mpsc::UnboundedReceiver<RuntimeResponse>,
    runtime_mode: String,
    /// Requests waiting for the user, by request id.
    pending: HashMap<String, Pending>,
    /// Each file change item's changes by path, for approval prompts.
    file_changes: HashMap<String, Value>,
    /// The native subagents the turn spawned, by their child thread.
    subagents: super::codex_subagents::Subagents,
    /// Requests reading a child's model (`thread/read`, then `thread/resume`), by request id:
    /// the child and whether this is the resume.
    child_reads: HashMap<u64, (String, bool)>,
    /// The rate limits Codex last reported (`account/rateLimits/updated`).
    rate_limits: Option<Value>,
    /// The thread-wide token totals Codex last reported.
    usage_baseline: Option<Breakdown>,
    /// What this turn used so far.
    turn_usage: TurnUsage,
    thread_id: Option<String>,
    /// The turn's context handoff went to the thread with
    /// `thread/inject_items` (reported on its provider turn).
    history_injected: bool,
    turn_id: Option<String>,
    /// `turn/completed` for our turn, once it arrived.
    terminal: Option<Value>,
    /// The last error Codex reported without retrying
    /// (`latestProviderFailure`).
    last_error: Option<Notice>,
    /// The retry Codex is in, shown as a provider retry item.
    retry: Option<ProviderRetry>,
    /// How many retries the turn recovered from; each later retry gets its
    /// own item.
    recovered_retries: u32,
    /// How long a retry may last before the turn fails.
    retry_budget: Duration,
    /// The failure a retry that outlasted its budget ends the turn with,
    /// once it asked Codex to stop.
    stalled: Option<ProviderFailure>,
}

/// An `error` notification's failure, with what Codex said
/// (`latestProviderFailure`).
#[derive(Clone)]
struct Notice {
    native_message: String,
    native_code: Option<String>,
    error_info: Value,
    failure: ProviderFailure,
}

/// The reference's ActiveCodexProviderRetry: a retry Codex announced
/// (`error` with `willRetry`) and has not recovered from.
struct ProviderRetry {
    notice: Notice,
    /// The notice's details, which a turn that fails with them repeats.
    native_details: String,
    attempt: u64,
    max_attempts: Option<u64>,
    /// The turn item's id ([`PROVIDER_RETRY_ITEM`]), one per retry the turn
    /// recovered from before, which the projection names as the reference
    /// does (`codexFailureItemKey`).
    item_id: String,
    /// When the retry fails the turn (`failStalledProviderRetry`): its budget
    /// counts from the first notice, which later notices cannot extend.
    deadline: tokio::time::Instant,
}

impl ProviderRetry {
    /// The reference's codexRetryTurnItem, which the projection completes.
    fn item(&self, status: &str) -> ProviderItem {
        let failure = &self.notice.failure;
        ProviderItem::new(&self.item_id, "error", status)
            .with(
                "failure",
                json!({"class": failure.class, "message": failure.message,
                    "code": failure.code, "retryable": failure.retryable}),
            )
            .with(
                "retry",
                json!({"attempt": self.attempt, "maxAttempts": self.max_attempts,
                    "retryDelayMs": null}),
            )
    }
}

/// Token counts as Codex reports them (`total` or `last`).
#[derive(Clone, Copy, Default)]
struct Breakdown {
    input: u64,
    cached: u64,
    cache_creation: Option<u64>,
    output: u64,
    reasoning: u64,
}

impl Breakdown {
    fn of(usage: &Value) -> Self {
        let count = |key: &str| usage[key].as_i64().unwrap_or(0).max(0) as u64;
        Self {
            input: count("inputTokens"),
            cached: count("cachedInputTokens"),
            cache_creation: usage["cacheWriteInputTokens"].as_u64(),
            output: count("outputTokens"),
            reasoning: count("reasoningOutputTokens"),
        }
    }
}

/// The reference's CodexTurnTokenUsage accumulator.
#[derive(Default)]
struct TurnUsage {
    input: u64,
    cached: u64,
    cache_creation: Option<u64>,
    output: u64,
    reasoning: u64,
    observed: bool,
    /// The turn spawned a native subagent (`markSubagentUsage`).
    has_subagents: bool,
}

impl TurnUsage {
    fn new() -> Self {
        Self {
            cache_creation: Some(0),
            ..Self::default()
        }
    }

    /// Add the growth from `previous` to `current` (thread-wide totals),
    /// or `last` when there is no usable baseline.
    fn add(&mut self, previous: Option<Breakdown>, current: Breakdown, last: Breakdown) {
        let delta = match previous {
            Some(previous)
                if current.input >= previous.input
                    && current.cached >= previous.cached
                    && current.output >= previous.output
                    && current.reasoning >= previous.reasoning =>
            {
                Breakdown {
                    input: current.input - previous.input,
                    cached: current.cached - previous.cached,
                    cache_creation: match (current.cache_creation, previous.cache_creation) {
                        (Some(now), Some(before)) if now >= before => Some(now - before),
                        _ => None,
                    },
                    output: current.output - previous.output,
                    reasoning: current.reasoning - previous.reasoning,
                }
            }
            _ => last,
        };
        if delta.input > 0 || delta.cached > 0 || delta.output > 0 || delta.reasoning > 0 {
            self.observed = true;
        }
        self.input += delta.input;
        self.cached += delta.cached;
        self.output += delta.output;
        self.reasoning += delta.reasoning;
        self.cache_creation = match (self.cache_creation, delta.cache_creation) {
            (Some(sum), Some(more)) => Some(sum + more),
            _ => None,
        };
    }

    /// The reference's completeCodexTurnTokenUsage.
    fn complete(&self, completed: bool) -> Value {
        if !self.observed {
            return json!({"usageStatus": "unavailable", "usageScope": "main_agent",
                "hasSubagents": self.has_subagents});
        }
        let mut usage = json!({
            "usageStatus": if completed { "complete" } else { "partial" },
            "usageScope": "main_agent",
            "inputTokens": self.input,
            "cachedInputTokens": self.input.min(self.cached),
            "outputTokens": self.output,
            "reasoningTokens": self.output.min(self.reasoning),
            "hasSubagents": self.has_subagents,
        });
        if let Some(creation) = self.cache_creation {
            usage["cacheCreationTokens"] = json!(self.input.min(creation));
        }
        usage
    }
}

/// A Codex request waiting for the user.
struct Pending {
    /// The JSON-RPC id to answer.
    rpc_id: Value,
    reply: Reply,
}

/// How to answer a pending request.
enum Reply {
    /// `{decision}`; `allows` is the kind an accept-for-session allows for
    /// the native thread.
    Decision {
        allows: &'static str,
        thread: String,
    },
    /// The legacy `{decision}` review decisions.
    Legacy {
        allows: &'static str,
        thread: String,
    },
    Permissions {
        permissions: Value,
    },
    Elicitation {
        payload: Value,
    },
    UserInput {
        native_ids: HashMap<String, String>,
    },
}

/// The reference's codexRuntimeModeTurnDefaults: approval policy, reviewer
/// and sandbox for a runtime mode.
fn runtime_mode_policy(mode: &str) -> (Value, &'static str, Value) {
    match mode {
        "approval-required" => (json!("untrusted"), "user", json!({"type": "readOnly"})),
        "auto-accept-edits" => (
            json!("on-request"),
            "user",
            json!({"type": "workspaceWrite"}),
        ),
        "auto" => (
            json!("on-request"),
            "auto_review",
            json!({"type": "workspaceWrite"}),
        ),
        // `never` also rejects browser and app access prompts before the
        // client sees them.
        _ => (
            json!({"granular": {"mcp_elicitations": true, "request_permissions": false,
                "rules": false, "sandbox_approval": false, "skill_approval": false}}),
            "user",
            json!({"type": "dangerFullAccess"}),
        ),
    }
}

/// Why a request got no reply.
enum Failed {
    /// The CLI answered with a JSON-RPC error.
    Rpc(Value),
    /// The CLI exited or closed its output.
    Closed,
    /// The turn was cancelled first.
    Cancelled,
}

impl Session {
    async fn turn(&mut self, spec: &TurnSpec, provider: &ProviderTurn) -> TurnEnd {
        let end = self.drive_turn(spec, provider).await;
        // A retry still running ends with its turn; a failed turn's end
        // fails it with the turn's failure, which it then shows.
        if let Some(retry) = self.retry.take() {
            let status = match end.status {
                TurnStatus::Completed => Some("completed"),
                TurnStatus::Interrupted => Some("interrupted"),
                _ => None,
            };
            if let Some(status) = status {
                self.sink.send(ProviderEvent::TurnItem {
                    item: retry.item(status),
                });
            }
        }
        // A turn Codex started and ended reports what it used, as the
        // reference does; one whose app-server closed never ends there.
        let crashed = end
            .failure
            .as_ref()
            .is_some_and(|failure| crate::workspace::provider::stream_closed(&failure.message));
        if self.turn_id.is_some() && !crashed {
            let completed = end.status == TurnStatus::Completed;
            self.sink.send(ProviderEvent::TurnUsage {
                usage: self.turn_usage.complete(completed),
            });
        }
        end
    }

    async fn drive_turn(&mut self, spec: &TurnSpec, provider: &ProviderTurn) -> TurnEnd {
        match self.drive(spec, provider).await {
            Ok(end) => end,
            Err(Failed::Cancelled) => TurnEnd::interrupted(true),
            Err(Failed::Closed) => {
                let status = self.cli.exit_status().await;
                TurnEnd::failed(codex_crashed(status), ThreadDisposition::Reusable)
            }
            Err(Failed::Rpc(error)) => TurnEnd::failed(
                ProviderFailure::new(
                    "provider_error",
                    error["message"]
                        .as_str()
                        .unwrap_or("Codex rejected the request."),
                ),
                ThreadDisposition::Reusable,
            ),
        }
    }

    async fn drive(&mut self, spec: &TurnSpec, provider: &ProviderTurn) -> Result<TurnEnd, Failed> {
        // The shared app-server is initialized once.
        let server = self.cli.server.clone();
        let mut initialized = server.initialized.lock().await;
        if !*initialized {
            self.initialize().await?;
            *initialized = true;
        }
        drop(initialized);
        self.open_thread(spec, provider).await
    }

    async fn initialize(&mut self) -> Result<(), Failed> {
        self.request(
            "initialize",
            json!({
                "clientInfo": {"name": "T3 Code", "title": "T3 Code", "version": SERVER_VERSION},
                "capabilities": {
                    "experimentalApi": true,
                    "extensions": {"io.modelcontextprotocol/ui": {"mimeTypes": ["text/html;profile=mcp-app"]}},
                    "optOutNotificationMethods": ["turn/diff/updated"],
                },
            }),
        )
        .await?;
        self.notify("initialized").await
    }

    async fn open_thread(
        &mut self,
        spec: &TurnSpec,
        provider: &ProviderTurn,
    ) -> Result<TurnEnd, Failed> {
        let model = (!provider.model.trim().is_empty()).then(|| provider.model.clone());
        // The turn carries the runtime policy, as the reference sends it; the
        // thread carries the reference's thread config, with the app thread
        // the CLI's shells report as theirs.
        let mut config = json!({
            "tools.update_plan.enabled": true,
            "mcp_optional_startup_grace_ms": 10_000,
            "features.concurrent_reasoning_summaries": true,
        });
        if !provider.app_thread_id.is_empty() {
            config["shell_environment_policy.set.T3CODE_THREAD_ID"] = json!(provider.app_thread_id);
        }
        if let Some(mcp) = &provider.mcp {
            // Only T3's servers, so MCP servers from the launch arguments survive.
            let server = json!({
                "url": mcp.url,
                "tool_timeout_sec": T3_MCP_TOOL_TIMEOUT_SEC,
                "http_headers": {"Authorization": mcp.authorization},
            });
            let mut all = server.clone();
            all["disabled_tools"] = json!(DIRECT_ONLY_T3_TOOLS);
            config["mcp_servers.t3-code"] = all;
            // Code Mode runs a blocking wait as a nested call the model keeps
            // polling; a direct-only registration makes it a plain tool call.
            let mut direct = server;
            direct["enabled_tools"] = json!(DIRECT_ONLY_T3_TOOLS);
            direct["omit_tools_from"] = json!(["code_mode", "deferred"]);
            config["mcp_servers.t3-code-direct"] = direct;
        }
        let mut thread_params = json!({"config": config, "cwd": spec.cwd});
        if let Some(model) = &model {
            thread_params["model"] = json!(model);
        }
        let mut resumed = false;
        let mut thread = None;
        // The prompt the turn sends: a thread Codex no longer has starts over
        // with the thread's recovered context ahead of it, as the reference's
        // resumeThread fails into its resume fallback.
        let mut prompt = spec.prompt.as_str();
        // Whether the turn takes the resume fallback's history (a fresh
        // thread), not the resumed thread's handoffs.
        let mut recovered = false;
        // A fork starts a fresh thread with its source's history: this
        // server does not fork Codex threads natively (`thread/fork`) yet.
        if provider.fork_session.is_some() && provider.resume.is_none() {
            if let Some(recovery) = provider.recovery_prompt.as_deref() {
                prompt = recovery;
                recovered = true;
            }
        }
        // What the thread was loaded with: a thread still loaded with the
        // same model and runtime policy is not resumed again.
        let load_key = json!({
            "model": model, "effort": provider.reasoning_effort, "runtimeMode": self.runtime_mode,
            "interactionMode": provider.interaction_mode(), "cwd": spec.cwd,
        })
        .to_string();
        if let Some(loaded) = provider
            .resume
            .as_deref()
            .filter(|id| !id.is_empty() && self.cli.server.holds(id, &load_key))
        {
            self.cli.bind(loaded);
            resumed = true;
            thread = Some(json!({"thread": {"id": loaded}}));
        } else if let Some(resume) = provider.resume.as_deref().filter(|id| !id.is_empty()) {
            let mut params = thread_params.clone();
            params["threadId"] = json!(resume);
            // The reference resumes without replaying the thread's turns.
            params["excludeTurns"] = json!(true);
            match self.request("thread/resume", params).await {
                Ok(reply) => {
                    resumed = true;
                    thread = Some(reply);
                }
                // A thread Codex no longer has starts over, as the reference does.
                Err(Failed::Rpc(error)) if thread_not_found(&error) => {
                    if let Some(recovery) = provider.recovery_prompt.as_deref() {
                        prompt = recovery;
                        recovered = true;
                    }
                }
                Err(other) => return Err(other),
            }
        }
        let thread = match thread {
            Some(thread) => thread,
            None => {
                // Codex tags a delegated child's MCP calls with thread_source
                // "subagent", as the reference starts it (`isDelegatedTaskThread`).
                let mut params = thread_params;
                if provider.app_thread_id.starts_with("thread:delegated-task:") {
                    params["threadSource"] = json!("subagent");
                }
                self.request("thread/start", params).await?
            }
        };
        let Some(thread_id) = thread["thread"]["id"].as_str().map(str::to_owned) else {
            return Ok(TurnEnd::failed(
                ProviderFailure::new("provider_error", "Codex started a thread without an id."),
                ThreadDisposition::Broken,
            ));
        };
        self.thread_id = Some(thread_id.clone());
        self.cli.bind(&thread_id);
        self.cli.server.mark_loaded(&thread_id, Some(load_key));
        self.sink.send(ProviderEvent::ProviderThread {
            driver: CODEX.into(),
            native_thread_id: thread_id.clone(),
            resumed,
            model: thread["model"]
                .as_str()
                .map(str::to_owned)
                .or(model.clone()),
            opencode2: None,
            native_title: None,
            native_updated_at: None,
            context_usage: None,
        });
        // The context handoff's history goes to the thread as native items
        // before the prompt (the reference's injectHistory); a Codex that
        // does not know `thread/inject_items` takes it inline instead.
        let items = if recovered {
            provider.recovery_inject_items.as_ref()
        } else {
            provider.inject_items.as_ref()
        };
        if let Some(items) = items.filter(|items| items.as_array().is_some_and(|i| !i.is_empty())) {
            let params = json!({"threadId": thread_id, "items": items});
            match self.request("thread/inject_items", params).await {
                Ok(_) => {
                    self.history_injected = true;
                    if let Some(plain) = provider.injected_prompt.as_deref() {
                        prompt = plain;
                    }
                }
                Err(Failed::Rpc(error)) if error["code"] == -32601 => {}
                Err(other) => return Err(other),
            }
        }

        let (approval_policy, reviewer, sandbox_policy) = runtime_mode_policy(&self.runtime_mode);
        // `toCodexInput`: the text naming each attachment's file, then each
        // supported image as a data URL.
        let mut input = vec![json!({"type": "text", "text": prompt})];
        if !provider.attachments.is_empty() {
            use crate::workspace::attachment_prompt;
            let text = attachment_prompt::text_with_paths(prompt, &provider.attachments);
            input.clear();
            if !text.is_empty() {
                input.push(json!({"type": "text", "text": text}));
            }
            for attachment in provider
                .attachments
                .iter()
                .filter(|a| attachment_prompt::is_native_image(a))
            {
                match attachment_prompt::image_base64(attachment) {
                    Ok(data) => input.push(json!({
                        "type": "image",
                        "url": format!("data:{};base64,{data}", attachment["mimeType"].as_str().unwrap_or_default()),
                    })),
                    Err(message) => {
                        return Ok(TurnEnd::failed(
                            ProviderFailure::new("provider_error", message),
                            ThreadDisposition::Reusable,
                        ))
                    }
                }
            }
        }
        let mut turn_params = json!({
            "threadId": thread_id,
            "input": input,
            "cwd": spec.cwd,
            "approvalsReviewer": reviewer,
            "approvalPolicy": approval_policy,
            "sandboxPolicy": sandbox_policy,
            "summary": "auto",
        });
        if let Some(model) = &model {
            turn_params["model"] = json!(model);
        }
        if let Some(effort) = &provider.reasoning_effort {
            turn_params["effort"] = json!(effort);
        }
        // Codex supplies its own mode instructions; both modes go, so
        // leaving Plan clears it.
        let mut settings = json!({"model": provider.model});
        if let Some(effort) = &provider.reasoning_effort {
            settings["reasoning_effort"] = json!(effort);
        }
        turn_params["collaborationMode"] =
            json!({"mode": provider.interaction_mode(), "settings": settings});
        // T3's instructions for the agent, as the reference's turns carry
        // them (`buildCodexAdditionalContext`).
        if let Some(mcp) = &provider.mcp {
            turn_params["additionalContext"] = crate::agent_instructions::codex_additional_context(
                &provider.model,
                provider.reasoning_effort.as_deref(),
                mcp.device_tools,
            );
        }
        let started = self.request("turn/start", turn_params).await?;
        if let Some(id) = started["turn"]["id"].as_str() {
            self.turn_started(id);
        }

        // Wait for the turn to end; a cancel interrupts it.
        let mut interrupt_deadline = None;
        while self.terminal.is_none() {
            // A retry that outlasts its budget fails the turn, unless the
            // turn is already being stopped.
            let retry_deadline = self
                .retry
                .as_ref()
                .filter(|_| !self.cancelled && self.stalled.is_none())
                .map(|retry| retry.deadline);
            let line = tokio::select! {
                line = self.cli.next_line() => line,
                _ = cancelled(&mut self.cancel), if !self.cancelled => {
                    self.cancelled = true;
                    self.interrupt().await;
                    interrupt_deadline = Some(tokio::time::Instant::now() + INTERRUPT_GRACE);
                    continue;
                }
                _ = sleep_until(interrupt_deadline) => {
                    if let Some(failure) = self.stalled.take() {
                        return Ok(TurnEnd::failed(failure, ThreadDisposition::Reusable));
                    }
                    return Ok(TurnEnd::interrupted(true));
                }
                _ = sleep_until(retry_deadline) => {
                    self.fail_stalled_retry();
                    self.interrupt().await;
                    interrupt_deadline = Some(tokio::time::Instant::now() + INTERRUPT_GRACE);
                    continue;
                }
                response = next_response(&mut self.responses) => {
                    self.respond(response).await;
                    continue;
                }
            };
            let Some(line) = line else {
                if let Some(failure) = self.stalled.take() {
                    return Ok(TurnEnd::failed(failure, ThreadDisposition::Reusable));
                }
                if self.cancelled {
                    return Ok(TurnEnd::interrupted(true));
                }
                return Err(Failed::Closed);
            };
            self.handle_line(&line).await;
        }
        Ok(self.end().await)
    }

    /// The reference's failStalledProviderRetry: a retry that outlasted its
    /// budget fails the turn with the retry's failure, no longer retryable
    /// (a usage limit when its last attempt got a 429), unless Codex
    /// reported a failure of its own first. The turn is then stopped.
    fn fail_stalled_retry(&mut self) {
        let Some(retry) = &self.retry else { return };
        let mut failure = retry.notice.failure.clone();
        failure.retryable = Some(false);
        if http_status(&retry.notice.error_info) == Some(429) {
            failure.class = "usage_limit".into();
        }
        let failure = match &self.last_error {
            Some(notice) => notice.failure.clone(),
            None => failure,
        };
        self.stalled = Some(failure);
    }

    async fn end(&mut self) -> TurnEnd {
        let turn = self.terminal.take().unwrap_or_default();
        if let Some(mut failure) = self.stalled.take() {
            if failure.class == "usage_limit" {
                failure.reset_at = self.usage_limit_reset().await;
            }
            return TurnEnd::failed(failure, ThreadDisposition::Reusable);
        }
        match turn["status"].as_str() {
            Some("completed") => TurnEnd::completed(),
            Some("interrupted") => TurnEnd::interrupted(self.cancelled),
            _ => {
                let mut failure = self.turn_failure(&turn["error"]);
                if failure.class == "usage_limit" {
                    failure.reset_at = self.usage_limit_reset().await;
                }
                TurnEnd::failed(failure, ThreadDisposition::Reusable)
            }
        }
    }

    /// The failure of a turn that ended with `error`, as the reference's
    /// makeRootTerminalEvent finds it: what an `error` notification (or the
    /// retry still running) said, when the turn's own error repeats it,
    /// else the turn's error.
    fn turn_failure(&self, error: &Value) -> ProviderFailure {
        let message = error["message"].as_str();
        let info = &error["codexErrorInfo"];
        let code = error_code(info);
        let class = failure_class(info, code.as_deref());
        let retry = self.retry.as_ref();
        let previous = self
            .last_error
            .as_ref()
            .or(retry.map(|retry| &retry.notice));
        // Retry exhaustion without a status cannot replace the notice's evidence.
        let has_class = code
            .as_deref()
            .is_some_and(|code| !needs_evidence(info, code));
        let mut failure = match previous {
            Some(previous)
                if message.is_none_or(|message| message == previous.native_message)
                    && (code.is_none() || code == previous.native_code)
                    && (!has_class || previous.failure.class == class) =>
            {
                previous.failure.clone()
            }
            _ => {
                let mut failure =
                    ProviderFailure::new(class, message.unwrap_or("Provider turn failed."));
                failure.code = code.clone();
                failure
            }
        };
        // Codex reports an exhausted stream error as `other` with the same details.
        let notification = self.last_error.as_ref();
        if failure.class == "provider_error"
            && failure
                .code
                .as_deref()
                .is_some_and(|code| needs_evidence(info, code))
            && notification.is_none_or(|notice| {
                notice.failure.class == "transport_error"
                    || notice
                        .native_code
                        .as_deref()
                        .is_some_and(|code| needs_evidence(&notice.error_info, code))
            })
            && (retry.is_some_and(|retry| {
                message == Some(retry.native_details.as_str())
                    && failure_class(
                        &retry.notice.error_info,
                        retry.notice.native_code.as_deref(),
                    ) == "transport_error"
            }) || notification.is_some_and(|notice| {
                notice.failure.class == "transport_error"
                    && message == Some(notice.native_message.as_str())
            }))
        {
            failure.class = "transport_error".into();
        }
        failure
    }

    /// Codex's `error` notification: one it will not retry is the failure
    /// its turn ends with; one it retries shows as a provider retry item
    /// until the turn makes progress or ends.
    fn error_notice(&mut self, params: &Value) {
        // A turn already failing for its stalled retry ignores later notices.
        if self.stalled.is_some() {
            return;
        }
        let error = &params["error"];
        let native_message = error["message"].as_str().unwrap_or_default().to_owned();
        let info = error["codexErrorInfo"].clone();
        let code = error_code(&info);
        let details = error["additionalDetails"]
            .as_str()
            .map(str::trim)
            .filter(|details| !details.is_empty());
        if params["willRetry"] != true {
            let mut failure = ProviderFailure::new(
                failure_class(&info, code.as_deref()),
                details.unwrap_or(&native_message),
            );
            failure.code = code.clone();
            self.last_error = Some(Notice {
                native_message,
                native_code: code,
                error_info: info,
                failure,
            });
            return;
        }
        let class = if usage_limit_error(code.as_deref(), &info) {
            "usage_limit"
        } else if code
            .as_deref()
            .is_some_and(|code| code.starts_with("http") || code.starts_with("responseStream"))
        {
            "transport_error"
        } else {
            "provider_error"
        };
        let mut failure = ProviderFailure::new(class, details.unwrap_or(&native_message));
        failure.code = code.clone();
        failure.retryable = Some(true);
        let previous = self.retry.take();
        let progress = retry_progress(&native_message);
        let retry = ProviderRetry {
            attempt: progress.map_or_else(
                || previous.as_ref().map_or(0, |retry| retry.attempt) + 1,
                |(attempt, _)| attempt,
            ),
            // Codex drops the count once it waits for the network, which has no limit.
            max_attempts: progress.map(|(_, max)| max),
            native_details: match details {
                Some(_) => error["additionalDetails"]
                    .as_str()
                    .unwrap_or_default()
                    .to_owned(),
                None => native_message.clone(),
            },
            notice: Notice {
                native_message,
                native_code: code,
                error_info: info,
                failure,
            },
            item_id: previous.as_ref().map_or_else(
                || match self.recovered_retries {
                    0 => PROVIDER_RETRY_ITEM.to_owned(),
                    n => format!("{PROVIDER_RETRY_ITEM}:{n}"),
                },
                |retry| retry.item_id.clone(),
            ),
            deadline: previous.as_ref().map_or_else(
                || tokio::time::Instant::now() + self.retry_budget,
                |retry| retry.deadline,
            ),
        };
        self.sink.send(ProviderEvent::TurnItem {
            item: retry.item("running"),
        });
        self.retry = Some(retry);
    }

    /// New model output proves Codex recovered from its retry
    /// (`completeProviderRetry`).
    fn complete_retry(&mut self) {
        if self.stalled.is_some() {
            return;
        }
        if let Some(retry) = self.retry.take() {
            self.recovered_retries += 1;
            self.sink.send(ProviderEvent::TurnItem {
                item: retry.item("completed"),
            });
        }
    }

    /// When the usage limit a turn hit resets, as the reference finds it:
    /// from the rate limits Codex reported, else by asking Codex
    /// (`account/rateLimits/read`, ten seconds at most).
    async fn usage_limit_reset(&mut self) -> Option<String> {
        if let Some(reset) = self.rate_limits.as_ref().and_then(usage_limit_reset_at) {
            return Some(reset);
        }
        let read = tokio::time::timeout(
            Duration::from_secs(10),
            self.request("account/rateLimits/read", Value::Null),
        )
        .await
        .ok()?
        .ok()?;
        let limits = if read["rateLimitsByLimitId"]["codex"].is_object() {
            &read["rateLimitsByLimitId"]["codex"]
        } else {
            &read["rateLimits"]
        };
        usage_limit_reset_at(limits)
    }

    fn turn_started(&mut self, id: &str) {
        if self.turn_id.is_none() {
            self.turn_id = Some(id.to_owned());
            self.turn_usage = TurnUsage::new();
            self.sink.send(ProviderEvent::ProviderTurn {
                native_turn_id: Some(id.to_owned()),
                native_turn_ref: None,
                conversation_head: None,
                status: TurnStatus::Running,
                model: None,
                history_injected: self.history_injected,
            });
        }
    }

    /// `turn/steer`: `text` joins the running turn as the user's input.
    /// Its reply does not matter; the turn's items report what it did.
    async fn steer(&mut self, text: &str) {
        let (Some(thread_id), Some(turn_id)) = (self.thread_id.clone(), self.turn_id.clone())
        else {
            return;
        };
        let id = self.cli.next_id();
        let _ = self
            .cli
            .write(&json!({"id": id, "method": "turn/steer", "params": {
                "expectedTurnId": turn_id,
                "input": [{"type": "text", "text": text}],
                "threadId": thread_id,
            }}))
            .await;
    }

    async fn interrupt(&mut self) {
        let (Some(thread_id), Some(turn_id)) = (self.thread_id.clone(), self.turn_id.clone())
        else {
            return;
        };
        let id = self.cli.next_id();
        // The reply does not matter; `turn/completed` reports the outcome.
        let _ = self
            .cli
            .write(&json!({"id": id, "method": "turn/interrupt",
                "params": {"threadId": thread_id, "turnId": turn_id}}))
            .await;
    }

    /// Asks Codex for a child's model without waiting on it: `thread/read`, or once that names
    /// none, `thread/resume` (`registerSubagentThread`).
    async fn read_child(&mut self, native: String, resume: bool) {
        let id = self.cli.next_id();
        let (method, params) = if resume {
            (
                "thread/resume",
                json!({"threadId": native, "excludeTurns": true}),
            )
        } else {
            (
                "thread/read",
                json!({"threadId": native, "includeTurns": false}),
            )
        };
        if self
            .cli
            .write(&json!({"id": id, "method": method, "params": params}))
            .await
            .is_ok()
        {
            self.child_reads.insert(id, (native, resume));
        }
    }

    /// What Codex answered about a child's model.
    async fn child_selection(&mut self, native: String, resumed: bool, result: &Value) {
        let thread = &result["thread"];
        let (model, effort) = if resumed {
            (&result["model"], &result["reasoningEffort"])
        } else {
            (&thread["model"], &thread["reasoningEffort"])
        };
        match model.as_str().filter(|m| !m.trim().is_empty()) {
            Some(model) if thread["id"] == native.as_str() => {
                if let Some(item) = self.subagents.selected(&native, model, effort.as_str()) {
                    self.sink.send(ProviderEvent::TurnItem { item });
                }
            }
            _ if !resumed => self.read_child(native, true).await,
            _ => {}
        }
    }

    async fn notify(&mut self, method: &str) -> Result<(), Failed> {
        self.cli
            .write(&json!({"method": method}))
            .await
            .map_err(|_| Failed::Closed)
    }

    /// Send a request and read until its reply, handling what arrives
    /// before it.
    async fn request(&mut self, method: &str, params: Value) -> Result<Value, Failed> {
        let id = self.cli.next_id();
        self.cli
            .write(&json!({"id": id, "method": method, "params": params}))
            .await
            .map_err(|_| Failed::Closed)?;
        loop {
            let line = tokio::select! {
                line = self.cli.next_line() => line,
                _ = cancelled(&mut self.cancel), if !self.cancelled => {
                    self.cancelled = true;
                    return Err(Failed::Cancelled);
                }
                response = next_response(&mut self.responses) => {
                    self.respond(response).await;
                    continue;
                }
            };
            let Some(line) = line else {
                return Err(Failed::Closed);
            };
            let Ok(message) = serde_json::from_str::<Value>(&line) else {
                continue;
            };
            if message.get("method").is_none() && message["id"].as_u64() == Some(id) {
                return match message.get("error") {
                    Some(error) => Err(Failed::Rpc(error.clone())),
                    None => Ok(message["result"].clone()),
                };
            }
            self.handle(message).await;
        }
    }

    async fn handle_line(&mut self, line: &str) {
        if let Ok(message) = serde_json::from_str::<Value>(line) {
            self.handle(message).await;
        }
    }

    async fn handle(&mut self, message: Value) {
        let Some(method) = message["method"].as_str() else {
            // A child's model, which nothing waits for.
            if let Some(id) = message["id"].as_u64() {
                if let Some((native, resumed)) = self.child_reads.remove(&id) {
                    self.child_selection(native, resumed, &message["result"])
                        .await;
                }
            }
            return; // A reply to a request nobody waits for.
        };
        let params = &message["params"];
        if let Some(id) = message.get("id").filter(|id| !id.is_null()) {
            if let Some(reply) = self.server_request(id, method, params) {
                let response = match reply {
                    Ok(result) => json!({"id": id, "result": result}),
                    Err(error) => json!({"id": id, "error": error}),
                };
                let _ = self.cli.write(&response).await;
            }
            return;
        }
        // Only our turn's notifications; the app-server runs only our thread,
        // but a resumed thread can replay others.
        if let Some(turn) = params["turnId"].as_str().or(params["turn"]["id"].as_str()) {
            if self.turn_id.as_deref().is_some_and(|ours| ours != turn) && method != "turn/started"
            {
                return;
            }
        }
        match method {
            "turn/started" => {
                if let Some(id) = params["turn"]["id"].as_str() {
                    self.turn_started(id);
                }
            }
            "turn/completed" => {
                let turn = &params["turn"];
                if self.turn_id.is_none() || turn["id"].as_str() == self.turn_id.as_deref() {
                    self.terminal = Some(turn.clone());
                }
            }
            "item/started" | "item/completed" => {
                let native = &params["item"];
                // Only new model output proves recovery; an item's end can
                // arrive while Codex still retries.
                if method == "item/started"
                    && !matches!(
                        native["type"].as_str(),
                        Some("contextCompaction" | "userMessage" | "subAgentActivity")
                    )
                {
                    self.complete_retry();
                }
                if let (Some("fileChange"), Some(id)) =
                    (native["type"].as_str(), native["id"].as_str())
                {
                    if method == "item/started" {
                        let changes: serde_json::Map<String, Value> = native["changes"]
                            .as_array()
                            .into_iter()
                            .flatten()
                            .filter_map(|change| {
                                Some((change["path"].as_str()?.to_owned(), change["kind"].clone()))
                            })
                            .collect();
                        self.file_changes
                            .insert(id.to_owned(), Value::Object(changes));
                    } else {
                        self.file_changes.remove(id);
                    }
                }
                // A native subagent's spawn and the calls that follow it show as
                // its subagent item.
                if native["type"] == "subAgentActivity" {
                    if native["kind"] == "started" {
                        self.turn_usage.has_subagents = true;
                    }
                    let (item, read) = self.subagents.activity(native);
                    if let Some(item) = item {
                        self.sink.send(ProviderEvent::TurnItem { item });
                    }
                    if let Some(child) = read {
                        self.read_child(child, false).await;
                    }
                } else if native["type"] == "collabAgentToolCall" {
                    if native["tool"] == "spawnAgent" {
                        self.turn_usage.has_subagents = true;
                    }
                    for item in self.subagents.observe(native) {
                        self.sink.send(ProviderEvent::TurnItem { item });
                    }
                } else if let Some(item) = map_item(&params["item"], method == "item/completed") {
                    self.sink.send(ProviderEvent::TurnItem { item });
                }
            }
            "item/agentMessage/delta"
            | "item/reasoning/summaryTextDelta"
            | "item/reasoning/textDelta"
            | "item/plan/delta" => {
                if let (Some(item_id), Some(delta)) =
                    (params["itemId"].as_str(), params["delta"].as_str())
                {
                    // An empty reasoning delta is no progress.
                    if method == "item/agentMessage/delta"
                        || method == "item/plan/delta"
                        || !delta.is_empty()
                    {
                        self.complete_retry();
                    }
                    self.sink.send(ProviderEvent::TurnItemDelta {
                        held: false,
                        item_id: item_id.into(),
                        delta: delta.into(),
                    });
                }
            }
            "item/commandExecution/outputDelta" => {
                if let (Some(item_id), Some(chunk)) =
                    (params["itemId"].as_str(), params["delta"].as_str())
                {
                    self.sink.send(ProviderEvent::CommandOutputDelta {
                        item_id: item_id.into(),
                        chunk: chunk.into(),
                    });
                }
            }
            "turn/plan/updated" => {
                self.complete_retry();
                let steps = params["plan"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .filter_map(|step| {
                        Some(PlanStep {
                            step: step["step"].as_str()?.into(),
                            status: step["status"].as_str().unwrap_or("pending").into(),
                        })
                    })
                    .collect();
                self.sink.send(ProviderEvent::Plan {
                    steps,
                    explanation: params["explanation"].as_str().map(str::to_owned),
                });
            }
            // A resumed thread first repeats its last turn's usage, which
            // only sets the baseline the turn's usage counts from.
            "thread/tokenUsage/updated" if self.turn_id.is_none() => {
                self.usage_baseline = Some(Breakdown::of(&params["tokenUsage"]["total"]));
            }
            "thread/tokenUsage/updated" => {
                let usage = &params["tokenUsage"];
                let total = Breakdown::of(&usage["total"]);
                self.turn_usage
                    .add(self.usage_baseline, total, Breakdown::of(&usage["last"]));
                self.usage_baseline = Some(total);
                let last = &usage["last"];
                let count = |key: &str| last[key].as_i64().unwrap_or(0).max(0) as u64;
                self.sink.send(ProviderEvent::Usage {
                    usage: TokenUsage {
                        used_tokens: count("totalTokens"),
                        max_tokens: usage["modelContextWindow"].as_u64(),
                        input_tokens: count("inputTokens"),
                        cached_input_tokens: count("cachedInputTokens"),
                        output_tokens: count("outputTokens"),
                        reasoning_output_tokens: count("reasoningOutputTokens"),
                        ..TokenUsage::default()
                    },
                });
            }
            "account/rateLimits/updated" => {
                // The newest snapshot of each window wins.
                let mut merged = self.rate_limits.take().unwrap_or_else(|| json!({}));
                for (key, value) in params["rateLimits"].as_object().into_iter().flatten() {
                    if !value.is_null() {
                        merged[key] = value.clone();
                    }
                }
                self.rate_limits = Some(merged);
            }
            "error" => self.error_notice(params),
            _ => {}
        }
    }

    /// A request from Codex: answered now when the reference answers it
    /// itself, otherwise recorded as a runtime request that waits for the
    /// user (`None`).
    fn server_request(
        &mut self,
        rpc_id: &Value,
        method: &str,
        params: &Value,
    ) -> Option<Result<Value, Value>> {
        let text = |key: &str| params[key].as_str().unwrap_or_default().to_owned();
        let mode = self.runtime_mode.clone();
        let (request, reply) = match method {
            "item/commandExecution/requestApproval" | "item/fileChange/requestApproval" => {
                let command = method == "item/commandExecution/requestApproval";
                let kind = if command { "command" } else { "file-change" };
                let thread = text("threadId");
                // T3's Full access policy is granular, under which Codex still
                // asks before commands it deems dangerous; the reference
                // accepts those itself (#40773). Managed-network asks enforce
                // the user's allowlist, so they still ask.
                if command
                    && mode == "full-access"
                    && params["networkApprovalContext"].is_null()
                    && params["proposedNetworkPolicyAmendments"]
                        .as_array()
                        .is_none_or(Vec::is_empty)
                {
                    return Some(Ok(json!({"decision": "accept"})));
                }
                if requests::session_allows(&thread, &mode, kind) {
                    return Some(Ok(json!({"decision": "acceptForSession"})));
                }
                let item = text("itemId");
                let native = match params["approvalId"].as_str() {
                    Some(approval) if command => approval.to_owned(),
                    _ => item.clone(),
                };
                let prompt = if command {
                    params["reason"]
                        .as_str()
                        .or(params["command"].as_str())
                        .map(str::to_owned)
                } else {
                    requests::file_change_prompt(params, self.file_changes.get(&item))
                };
                let mut request = requests::open(&native, &item, kind, &mode);
                if let Some(prompt) = prompt {
                    request.fields.insert("prompt".into(), json!(prompt));
                }
                (
                    request,
                    Reply::Decision {
                        allows: kind,
                        thread,
                    },
                )
            }
            "item/permissions/requestApproval" => {
                let item = text("itemId");
                let kind = requests::permissions_kind(&params["permissions"]);
                let mut request = requests::open(&item, &item, kind, &mode);
                if let Some(reason) = params["reason"].as_str() {
                    request.fields.insert("prompt".into(), json!(reason));
                }
                (
                    request,
                    Reply::Permissions {
                        permissions: params["permissions"].clone(),
                    },
                )
            }
            "mcpServer/elicitation/request" => {
                let acceptance = requests::elicitation_response(params, "accept");
                if acceptance["action"] != "accept" || mode == "full-access" {
                    return Some(Ok(if acceptance["action"] == "accept" {
                        acceptance
                    } else {
                        json!({"action": "cancel"})
                    }));
                }
                let native = if params["mode"] == "url" {
                    text("elicitationId")
                } else {
                    format!("mcp-elicitation:{}", text("serverName"))
                };
                let (app_name, options) = requests::describe_elicitation(params);
                let mut request = requests::open(&native, &native, "mcp-elicitation", &mode);
                request
                    .fields
                    .insert("prompt".into(), json!(text("message")));
                request.fields.insert("appName".into(), json!(app_name));
                request.fields.insert("options".into(), options);
                (
                    request,
                    Reply::Elicitation {
                        payload: params.clone(),
                    },
                )
            }
            "execCommandApproval" | "applyPatchApproval" => {
                let command = method == "execCommandApproval";
                let kind = if command { "command" } else { "file-change" };
                let thread = text("conversationId");
                if requests::session_allows(&thread, &mode, kind) {
                    return Some(Ok(json!({"decision": "approved_for_session"})));
                }
                let call = text("callId");
                let (native, prompt) = if command {
                    let joined = params["command"]
                        .as_array()
                        .map(|words| {
                            words
                                .iter()
                                .filter_map(Value::as_str)
                                .collect::<Vec<_>>()
                                .join(" ")
                        })
                        .unwrap_or_default();
                    (
                        params["approvalId"].as_str().unwrap_or(&call).to_owned(),
                        Some(params["reason"].as_str().map_or(joined, str::to_owned)),
                    )
                } else {
                    (
                        call.clone(),
                        requests::file_change_prompt(params, Some(&params["fileChanges"])),
                    )
                };
                let mut request = requests::open(&native, &call, kind, &mode);
                if let Some(prompt) = prompt {
                    request.fields.insert("prompt".into(), json!(prompt));
                }
                (
                    request,
                    Reply::Legacy {
                        allows: kind,
                        thread,
                    },
                )
            }
            "item/tool/requestUserInput" => {
                let item = text("itemId");
                let (questions, native_ids) = requests::codex_questions(&params["questions"]);
                let mut request = requests::open(&item, &item, "user_input", &mode);
                request.fields.insert("questions".into(), questions);
                (request, Reply::UserInput { native_ids })
            }
            _ => {
                return Some(Err(
                    json!({"code": -32601, "message": format!("Method not found: {method}")}),
                ))
            }
        };
        self.pending.insert(
            request.id.clone(),
            Pending {
                rpc_id: rpc_id.clone(),
                reply,
            },
        );
        self.sink
            .send(ProviderEvent::RuntimeRequestOpened { request });
        None
    }

    /// Answer a pending request with the user's response.
    async fn respond(&mut self, response: RuntimeResponse) {
        if let Some(text) = &response.steer {
            return self.steer(text).await;
        }
        let Some(pending) = self.pending.remove(&response.id) else {
            return;
        };
        let decision = requests::decision(&response);
        let session = matches!(decision, "acceptForSession" | "acceptAlways");
        let result = match pending.reply {
            Reply::Decision { allows, thread } => {
                if session {
                    requests::allow_for_session(&thread, &self.runtime_mode, allows);
                }
                let decision = if decision == "acceptAlways" {
                    "acceptForSession"
                } else {
                    decision
                };
                json!({"decision": decision})
            }
            Reply::Legacy { allows, thread } => {
                if session {
                    requests::allow_for_session(&thread, &self.runtime_mode, allows);
                }
                json!({"decision": requests::legacy_decision(decision)})
            }
            Reply::Permissions { permissions } => {
                requests::permissions_response(decision, &permissions)
            }
            Reply::Elicitation { payload } => requests::elicitation_response(&payload, decision),
            Reply::UserInput { native_ids } => {
                let Some(answers) = &response.answers else {
                    // The reference refuses an answer without answers; the
                    // request keeps waiting.
                    self.pending.insert(
                        response.id.clone(),
                        Pending {
                            rpc_id: pending.rpc_id,
                            reply: Reply::UserInput { native_ids },
                        },
                    );
                    return;
                };
                json!({"answers": requests::codex_answers(answers, &native_ids)})
            }
        };
        let _ = self
            .cli
            .write(&json!({"id": pending.rpc_id, "result": result}))
            .await;
    }
}

fn thread_not_found(error: &Value) -> bool {
    error["message"].as_str().is_some_and(|message| {
        message.starts_with("thread not found:") || message.contains("no rollout found")
    })
}

/// The reference's codexItemStatus, for turn items.
fn item_status(item: &Value, completed: bool) -> &'static str {
    match item["status"].as_str() {
        Some("inProgress") => "running",
        Some("completed") => "completed",
        Some("failed") => "failed",
        Some("declined") | Some("cancelled") => "cancelled",
        _ if completed => "completed",
        _ => "running",
    }
}

/// A Codex thread item as a turn item, or `None` for items a client does not
/// render from the provider (the user's own message).
fn map_item(item: &Value, completed: bool) -> Option<ProviderItem> {
    let id = item["id"].as_str()?;
    let status = item_status(item, completed);
    let mapped = match item["type"].as_str()? {
        "agentMessage" => {
            let mut mapped = ProviderItem::new(id, "assistant_message", status)
                .with("text", item["text"].as_str().unwrap_or_default())
                .with("streaming", !completed);
            if let Some(phase) = item["phase"].as_str() {
                let phase = match phase {
                    "final_answer" | "finalAnswer" => "final_answer",
                    "commentary" => "commentary",
                    _ => "partial_answer",
                };
                mapped = mapped.with("phase", phase);
            }
            mapped
        }
        "reasoning" => {
            let joined = |key: &str| {
                item[key]
                    .as_array()
                    .map(|parts| {
                        parts
                            .iter()
                            .filter_map(|part| part.as_str().or(part["text"].as_str()))
                            .collect::<Vec<_>>()
                            .join("\n\n")
                    })
                    .unwrap_or_default()
            };
            let summary = joined("summary");
            let text = if summary.is_empty() {
                joined("content")
            } else {
                summary
            };
            ProviderItem::new(id, "reasoning", status)
                .with("text", text)
                .with("streaming", !completed)
        }
        "plan" => ProviderItem::new(id, "proposed_plan", status)
            .with("markdown", item["text"].as_str().unwrap_or_default())
            .with("streaming", !completed),
        "commandExecution" => {
            let mut mapped = ProviderItem::new(id, "command_execution", status)
                .with("input", item["command"].as_str().unwrap_or_default());
            if let Some(output) = item["aggregatedOutput"].as_str() {
                mapped = mapped.with("output", output);
            }
            if let Some(code) = item["exitCode"].as_i64() {
                mapped = mapped.with("exitCode", code);
            }
            // The reference wakes the agent when a command outlives its
            // turn, and marks the command so.
            mapped.with("completionWake", "always")
        }
        "fileChange" => {
            let changes = item["changes"].as_array()?;
            let first = changes.first()?;
            let details: Vec<Value> = changes
                .iter()
                .map(|change| {
                    let operation = change["kind"]["type"].as_str().unwrap_or("update");
                    match change["kind"]["move_path"].as_str() {
                        Some(to) => {
                            json!({"operation": operation, "path": to, "oldPath": change["path"]})
                        }
                        None => json!({"operation": operation, "path": change["path"]}),
                    }
                })
                .collect();
            let mut mapped = ProviderItem::new(id, "file_change", status)
                .with("fileName", first["path"].as_str().unwrap_or_default())
                .with("changes", details);
            if let Some(diff) = first["diff"].as_str() {
                mapped = mapped.with("diffStr", diff);
            }
            mapped
        }
        "mcpToolCall" => {
            let tool = item["tool"].as_str().unwrap_or_default();
            let name = match item["server"].as_str() {
                Some(server) => format!("{server}.{tool}"),
                None => tool.to_owned(),
            };
            let mut mapped = ProviderItem::new(id, "dynamic_tool", status)
                .with("toolName", name)
                .with("input", item["arguments"].clone());
            if !item["result"].is_null() {
                mapped = mapped.with("output", item["result"].clone());
            } else if !item["error"].is_null() {
                mapped = mapped.with("output", item["error"].clone());
            }
            mapped
        }
        "dynamicToolCall" => {
            let mut mapped = ProviderItem::new(id, "dynamic_tool", status)
                .with("toolName", item["tool"].clone())
                .with("input", item["arguments"].clone());
            if !item["contentItems"].is_null() {
                mapped = mapped.with("output", item["contentItems"].clone());
            }
            mapped
        }
        "webSearch" => {
            let mut mapped = ProviderItem::new(id, "web_search", status);
            if let Some(query) = item["query"].as_str().filter(|q| !q.is_empty()) {
                mapped = mapped.with("patterns", vec![query.to_owned()]);
            }
            mapped
        }
        _ => return None,
    };
    Some(mapped)
}

/// The reference's codexUsageLimitResetAt: the latest reset of the windows
/// used up, when every one of them names its reset.
fn usage_limit_reset_at(limits: &Value) -> Option<String> {
    if limits["limitId"].as_str().is_some_and(|id| id != "codex") {
        return None;
    }
    let exhausted: Vec<&Value> = ["primary", "secondary"]
        .iter()
        .map(|key| &limits[*key])
        .filter(|window| {
            window["usedPercent"]
                .as_f64()
                .is_some_and(|used| used >= 100.0)
        })
        .collect();
    if exhausted.is_empty() {
        return None;
    }
    let resets: Option<Vec<i64>> = exhausted.iter().map(|w| w["resetsAt"].as_i64()).collect();
    resets?
        .into_iter()
        .max()
        .map(|seconds| crate::auth_service::iso(seconds.saturating_mul(1000)))
}

fn error_code(info: &Value) -> Option<String> {
    match info {
        Value::String(code) => Some(code.clone()),
        Value::Object(map) => map.keys().next().cloned(),
        _ => None,
    }
}

/// The HTTP status of a Codex error's details, if it names one
/// (`codexErrorHttpStatus`).
fn http_status(info: &Value) -> Option<u64> {
    info.as_object()?.values().next()?["httpStatusCode"].as_u64()
}

/// The reference's codexUsageLimitError: a usage or rate limit, or retries
/// that ran out on a 429.
fn usage_limit_error(code: Option<&str>, info: &Value) -> bool {
    matches!(code, Some("usageLimitExceeded" | "rateLimitExceeded"))
        || info["responseTooManyFailedAttempts"]["httpStatusCode"].as_u64() == Some(429)
}

/// The reference's codexTerminalFailureClass for a Codex error.
fn failure_class(info: &Value, code: Option<&str>) -> &'static str {
    if usage_limit_error(code, info) {
        return "usage_limit";
    }
    let transport = [
        "responseStreamDisconnected",
        "responseStreamConnectionFailed",
        "httpConnectionFailed",
    ]
    .iter()
    .find_map(|key| info.get(key));
    if transport.is_some_and(|transport| transport["httpStatusCode"].is_null()) {
        return "transport_error";
    }
    "provider_error"
}

/// The reference's codexTerminalErrorNeedsEvidence: a code that says
/// nothing of why the turn failed.
fn needs_evidence(info: &Value, code: &str) -> bool {
    code == "other"
        || (code == "responseTooManyFailedAttempts"
            && info["responseTooManyFailedAttempts"]["httpStatusCode"]
                .as_u64()
                .is_none())
}

/// The reference's parseCodexRetryProgress: the attempt and the most there
/// are, from a notice such as "Reconnecting... 2/5".
fn retry_progress(message: &str) -> Option<(u64, u64)> {
    let bytes = message.as_bytes();
    let digits = |from: usize| {
        let end = bytes[from..]
            .iter()
            .position(|b| !b.is_ascii_digit())
            .map_or(bytes.len(), |n| from + n);
        (end, &message[from..end])
    };
    let word = |at: usize| {
        bytes
            .get(at)
            .is_some_and(|b| b.is_ascii_alphanumeric() || *b == b'_')
    };
    let mut i = 0;
    while i < bytes.len() {
        // `\b(\d+)`: digits that start a word.
        if !bytes[i].is_ascii_digit() || (i > 0 && word(i - 1)) {
            i += 1;
            continue;
        }
        let (end, first) = digits(i);
        let mut j = end;
        while bytes.get(j).is_some_and(u8::is_ascii_whitespace) {
            j += 1;
        }
        if bytes.get(j) == Some(&b'/') {
            j += 1;
            while bytes.get(j).is_some_and(u8::is_ascii_whitespace) {
                j += 1;
            }
            if bytes.get(j).is_some_and(u8::is_ascii_digit) {
                let (last, second) = digits(j);
                // `(\d+)\b`: digits that end a word.
                if !word(last) {
                    let attempt = first.parse().ok()?;
                    let max = second.parse().ok()?;
                    return (attempt >= 1 && max >= 1).then_some((attempt, max));
                }
            }
        }
        i = end;
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_usage_limit_resets_when_its_last_exhausted_window_does() {
        let limits = json!({"limitId": "codex",
            "primary": {"usedPercent": 100, "resetsAt": 1_795_240_000},
            "secondary": {"usedPercent": 40, "resetsAt": 1_795_800_000}});
        assert_eq!(
            usage_limit_reset_at(&limits).as_deref(),
            Some("2026-11-21T05:46:40.000Z")
        );
        assert_eq!(
            usage_limit_reset_at(&json!({"primary": {"usedPercent": 100}})),
            None
        );
        assert_eq!(usage_limit_reset_at(&json!({"limitId": "other"})), None);
    }

    #[test]
    fn reads_retry_progress_as_the_reference_does() {
        assert_eq!(retry_progress("Reconnecting... 2/5"), Some((2, 5)));
        assert_eq!(
            retry_progress("Reconnecting... 3 / 10 attempts"),
            Some((3, 10))
        );
        assert_eq!(retry_progress("Reconnecting... waiting for network"), None);
        assert_eq!(retry_progress("Reconnecting... 0/5"), None);
        assert_eq!(retry_progress("v2/5"), None);
        assert_eq!(retry_progress("2/5x"), None);
        assert_eq!(retry_progress("a 1/2b then 4/7"), Some((4, 7)));
    }

    #[test]
    fn classes_codex_errors_as_the_reference_does() {
        let class = |info: Value| failure_class(&info, error_code(&info).as_deref());
        assert_eq!(class(json!("usageLimitExceeded")), "usage_limit");
        assert_eq!(
            class(json!({"responseTooManyFailedAttempts": {"httpStatusCode": 429}})),
            "usage_limit"
        );
        assert_eq!(
            class(json!({"responseStreamDisconnected": {"httpStatusCode": null}})),
            "transport_error"
        );
        assert_eq!(
            class(json!({"responseStreamDisconnected": {"httpStatusCode": 529}})),
            "provider_error"
        );
        assert_eq!(class(json!("other")), "provider_error");
    }

    #[test]
    fn maps_command_and_file_items() {
        let command = map_item(
            &json!({"type":"commandExecution","id":"c1","command":"ls","status":"completed",
                "aggregatedOutput":"a\n","exitCode":0}),
            true,
        )
        .unwrap();
        assert_eq!(
            serde_json::to_value(&command).unwrap(),
            json!({"id":"c1","type":"command_execution","status":"completed","title":null,
                "input":"ls","output":"a\n","exitCode":0,"completionWake":"always"})
        );
        let file = map_item(
            &json!({"type":"fileChange","id":"f1","status":"completed",
                "changes":[{"path":"a.txt","kind":{"type":"add"},"diff":"+hi\n"}]}),
            true,
        )
        .unwrap();
        assert_eq!(file.fields["fileName"], "a.txt");
        assert_eq!(file.fields["diffStr"], "+hi\n");
        assert_eq!(
            file.fields["changes"],
            json!([{"operation":"add","path":"a.txt"}])
        );
        assert!(map_item(&json!({"type":"userMessage","id":"u1"}), true).is_none());
    }
}
