//! Codex turns over `codex app-server` (JSON-RPC lines on stdio), as the
//! reference's Codex adapter runs them.
use super::requests;
use super::{
    cancelled, codex_crashed, next_response, program, sleep_until, Cli, Launch, Sink, TurnEnd,
};
use crate::workspace::provider::{
    PlanStep, ProviderFailure, ProviderItem, ProviderTurn, RuntimeResponse, ThreadDisposition,
    TokenUsage, TurnStatus, CODEX,
};
use crate::workspace::{ProviderEvent, TurnSpec};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};

/// The server version the reference reports to Codex.
const SERVER_VERSION: &str = "0.1.10";

/// How long an interrupted turn may take to report that it stopped.
const INTERRUPT_GRACE: Duration = Duration::from_secs(5);

pub(super) async fn run(
    spec: TurnSpec,
    provider: ProviderTurn,
    cancel: oneshot::Receiver<()>,
    responses: mpsc::UnboundedReceiver<RuntimeResponse>,
    sink: Sink,
) {
    let binary = program(&provider.binary_path, "codex");
    let mut args = vec![
        "app-server".to_string(),
        "-c".into(),
        "thread_unload_delay_secs=0".into(),
    ];
    args.extend(provider.launch_args.iter().cloned());
    let cli = match Cli::spawn(
        &spec,
        Launch {
            program: binary.clone(),
            args,
            home_var: "CODEX_HOME",
            home: &provider.home_path,
            // Codex names the client in its requests, as for the reference.
            extra_env: vec![(
                "CODEX_INTERNAL_ORIGINATOR_OVERRIDE".into(),
                "T3 Code".into(),
            )],
        },
    ) {
        Ok(cli) => cli,
        Err(_) => {
            // The reference's CodexAppServerSpawnError.
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
        next_id: 1,
        cancel: Some(cancel),
        cancelled: false,
        responses,
        runtime_mode: provider.runtime_mode().to_owned(),
        pending: HashMap::new(),
        file_changes: HashMap::new(),
        usage_baseline: None,
        rate_limits: None,
        turn_usage: TurnUsage::default(),
        thread_id: None,
        turn_id: None,
        terminal: None,
        last_error: None,
    };
    let end = session.turn(&spec, &provider).await;
    let Session { cli, .. } = session;
    cli.stop().await;
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
    cli: Cli,
    sink: Sink,
    next_id: u64,
    cancel: Option<oneshot::Receiver<()>>,
    cancelled: bool,
    /// Answers to the turn's runtime requests.
    responses: mpsc::UnboundedReceiver<RuntimeResponse>,
    runtime_mode: String,
    /// Requests waiting for the user, by request id.
    pending: HashMap<String, Pending>,
    /// Each file change item's changes by path, for approval prompts.
    file_changes: HashMap<String, Value>,
    /// The rate limits Codex last reported (`account/rateLimits/updated`).
    rate_limits: Option<Value>,
    /// The thread-wide token totals Codex last reported.
    usage_baseline: Option<Breakdown>,
    /// What this turn used so far.
    turn_usage: TurnUsage,
    thread_id: Option<String>,
    turn_id: Option<String>,
    /// `turn/completed` for our turn, once it arrived.
    terminal: Option<Value>,
    /// The last error Codex reported without retrying.
    last_error: Option<String>,
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
                "hasSubagents": false});
        }
        let mut usage = json!({
            "usageStatus": if completed { "complete" } else { "partial" },
            "usageScope": "main_agent",
            "inputTokens": self.input,
            "cachedInputTokens": self.input.min(self.cached),
            "outputTokens": self.output,
            "reasoningTokens": self.output.min(self.reasoning),
            "hasSubagents": false,
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
        // A turn Codex started and ended reports what it used, as the
        // reference does; one whose app-server closed never ends there.
        let crashed = end.failure.as_ref().is_some_and(|failure| {
            failure.message == crate::workspace::provider::CODEX_STREAM_CLOSED
        });
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
        self.notify("initialized").await?;

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
        let mut thread_params = json!({"config": config, "cwd": spec.cwd});
        if let Some(model) = &model {
            thread_params["model"] = json!(model);
        }
        let mut resumed = false;
        let mut thread = None;
        if let Some(resume) = provider.resume.as_deref().filter(|id| !id.is_empty()) {
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
                Err(Failed::Rpc(error)) if thread_not_found(&error) => {}
                Err(other) => return Err(other),
            }
        }
        let thread = match thread {
            Some(thread) => thread,
            None => self.request("thread/start", thread_params).await?,
        };
        let Some(thread_id) = thread["thread"]["id"].as_str().map(str::to_owned) else {
            return Ok(TurnEnd::failed(
                ProviderFailure::new("provider_error", "Codex started a thread without an id."),
                ThreadDisposition::Broken,
            ));
        };
        self.thread_id = Some(thread_id.clone());
        self.sink.send(ProviderEvent::ProviderThread {
            driver: CODEX.into(),
            native_thread_id: thread_id.clone(),
            resumed,
            model: thread["model"]
                .as_str()
                .map(str::to_owned)
                .or(model.clone()),
        });

        let (approval_policy, reviewer, sandbox_policy) = runtime_mode_policy(&self.runtime_mode);
        let mut turn_params = json!({
            "threadId": thread_id,
            "input": [{"type": "text", "text": spec.prompt, "text_elements": []}],
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
        let started = self.request("turn/start", turn_params).await?;
        if let Some(id) = started["turn"]["id"].as_str() {
            self.turn_started(id);
        }

        // Wait for the turn to end; a cancel interrupts it.
        let mut interrupt_deadline = None;
        while self.terminal.is_none() {
            let line = tokio::select! {
                line = self.cli.next_line() => line,
                _ = cancelled(&mut self.cancel), if !self.cancelled => {
                    self.cancelled = true;
                    self.interrupt().await;
                    interrupt_deadline = Some(tokio::time::Instant::now() + INTERRUPT_GRACE);
                    continue;
                }
                _ = sleep_until(interrupt_deadline) => {
                    return Ok(TurnEnd::interrupted(true));
                }
                response = next_response(&mut self.responses) => {
                    self.respond(response).await;
                    continue;
                }
            };
            let Some(line) = line else {
                if self.cancelled {
                    return Ok(TurnEnd::interrupted(true));
                }
                return Err(Failed::Closed);
            };
            self.handle_line(&line).await;
        }
        Ok(self.end().await)
    }

    async fn end(&mut self) -> TurnEnd {
        let turn = self.terminal.take().unwrap_or_default();
        match turn["status"].as_str() {
            Some("completed") => TurnEnd::completed(),
            Some("interrupted") => TurnEnd::interrupted(self.cancelled),
            _ => {
                let message = turn["error"]["message"]
                    .as_str()
                    .map(str::to_owned)
                    .or_else(|| self.last_error.clone())
                    .unwrap_or_else(|| "Codex turn failed.".into());
                let mut failure = ProviderFailure::new(failure_class(&turn["error"]), message);
                failure.code = error_code(&turn["error"]["codexErrorInfo"]);
                if failure.class == "usage_limit" {
                    failure.reset_at = self.usage_limit_reset().await;
                }
                TurnEnd::failed(failure, ThreadDisposition::Reusable)
            }
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
                status: TurnStatus::Running,
                model: None,
            });
        }
    }

    async fn interrupt(&mut self) {
        let (Some(thread_id), Some(turn_id)) = (self.thread_id.clone(), self.turn_id.clone())
        else {
            return;
        };
        let id = self.next_id;
        self.next_id += 1;
        // The reply does not matter; `turn/completed` reports the outcome.
        let _ = self
            .cli
            .write(&json!({"id": id, "method": "turn/interrupt",
                "params": {"threadId": thread_id, "turnId": turn_id}}))
            .await;
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
        let id = self.next_id;
        self.next_id += 1;
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
                if let Some(item) = map_item(&params["item"], method == "item/completed") {
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
                    self.sink.send(ProviderEvent::TurnItemDelta {
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
            "error" if params["willRetry"] != true => {
                if let Some(message) = params["error"]["message"].as_str() {
                    self.last_error = Some(message.into());
                }
            }
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

/// The reference's failure class for a Codex turn error.
fn failure_class(error: &Value) -> &'static str {
    match error_code(&error["codexErrorInfo"]).as_deref() {
        Some("usageLimitExceeded") => "usage_limit",
        Some("unauthorized") => "permission_error",
        Some("httpConnectionFailed")
        | Some("responseStreamConnectionFailed")
        | Some("responseStreamDisconnected") => "transport_error",
        Some("badRequest") | Some("contextWindowExceeded") => "validation_error",
        _ => "provider_error",
    }
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
