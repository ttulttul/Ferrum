//! Claude turns over the Claude Code CLI's stream-json protocol, the one the
//! Claude Agent SDK speaks and the reference's Claude adapter runs through.
use super::requests;
use super::{
    cancelled, claude_crashed, next_response, program, sleep_until, Cli, Launch, Sink, TurnEnd,
};
use crate::workspace::provider::{
    PlanStep, ProviderFailure, ProviderItem, ProviderTurn, RuntimeResponse, ThreadDisposition,
    TokenUsage, TurnStatus, CLAUDE_AGENT,
};
use crate::workspace::{ProviderEvent, TurnSpec};
use futures_util::FutureExt;
use indexmap::IndexMap;
use serde_json::{json, Value};

/// A T3 tool such as `request_secret` can wait an hour; Claude's MCP timeout stays above that.
const T3_MCP_TOOL_TIMEOUT_MS: u64 = 65 * 60 * 1000;
use std::collections::HashMap;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};

/// The Claude Agent SDK release the reference runs Claude Code through.
const CLAUDE_AGENT_SDK_VERSION: &str = "0.3.321";

/// How long an interrupted turn may take to report that it stopped.
const INTERRUPT_GRACE: Duration = Duration::from_secs(5);
/// What Claude Code prints when `--resume` names a session it does not have.
const CONVERSATION_NOT_FOUND: &str = "No conversation found with session ID:";

pub(super) async fn run(
    spec: TurnSpec,
    provider: ProviderTurn,
    cancel: oneshot::Receiver<()>,
    mut responses: mpsc::UnboundedReceiver<RuntimeResponse>,
    sink: Sink,
    sessions: std::sync::Arc<super::claude_sessions::Sessions>,
) {
    let mut cancel = Some(cancel);
    let mut resume = provider.resume.clone().filter(|id| !id.is_empty());
    loop {
        let attempt = Attempt::run(
            &spec,
            &provider,
            resume.as_deref(),
            &mut cancel,
            &mut responses,
            &sink,
            &sessions,
        )
        .await;
        match attempt {
            // A session Claude no longer has starts over, as the reference does.
            Outcome::ConversationMissing if resume.is_some() => resume = None,
            Outcome::ConversationMissing => {
                return sink.finish(TurnEnd::failed(
                    ProviderFailure::new("provider_error", CONVERSATION_NOT_FOUND),
                    ThreadDisposition::Broken,
                ))
            }
            Outcome::Ended(end) => return sink.finish(end),
        }
    }
}

enum Outcome {
    Ended(TurnEnd),
    /// The CLI refused `--resume` before it started the turn.
    ConversationMissing,
}

/// The reference's permissionModeForClaudeRuntimePolicy, without Plan mode.
fn permission_mode(runtime_mode: &str) -> &'static str {
    match runtime_mode {
        "approval-required" => "default",
        "auto-accept-edits" => "acceptEdits",
        "auto" => "auto",
        _ => "bypassPermissions",
    }
}

/// A `can_use_tool` request waiting for the user.
struct PendingPermission {
    /// The control request to answer.
    request_id: Value,
    tool_name: String,
    input: Value,
    tool_use_id: String,
    suggestions: Option<Value>,
    question: bool,
}

struct Attempt<'a> {
    sink: &'a Sink,
    cli: Cli,
    resume: Option<&'a str>,
    /// The thread's running CLI took the turn: it has its MCP servers.
    reused: bool,
    /// The native session the CLI was launched with (`--session-id` or
    /// `--resume`), under which it stays running for the thread's next turn.
    /// As the reference keys its live query: a session Claude Code reports
    /// under another id is resumed by a new CLI.
    session: Option<String>,
    /// Whether the CLI has said anything; refusals come before that.
    answered: bool,
    interrupted: bool,
    /// The message the streamed blocks belong to.
    message_id: Option<String>,
    /// Streamed blocks by index: their item id and kind.
    blocks: HashMap<u64, (String, &'static str)>,
    /// Streamed items the final assistant message has not completed yet.
    open_items: Vec<(String, &'static str)>,
    /// Tool calls still waiting for a result, in the order they started.
    tools: IndexMap<String, ProviderItem>,
    next_control: u64,
    runtime_mode: String,
    responses: &'a mut mpsc::UnboundedReceiver<RuntimeResponse>,
    /// Permission requests waiting for the user, by request id.
    pending: HashMap<String, PendingPermission>,
    /// The model's context window, the usage's `maxTokens`.
    context_window: Option<u64>,
    /// The latest root model call's usage, as it streams.
    stream_usage: Option<StreamUsage>,
    /// Models of the turn's root assistant messages, in order.
    root_models: Vec<String>,
    /// Whether the turn started (Claude Code's `init`).
    turn_started: bool,
    /// The turn's `result` message, once it came.
    result: Option<Value>,
    /// Steers written to the CLI and not yet taken: their prompt uuid, and
    /// the steer message's id.
    steers: HashMap<String, String>,
    /// Each steer's text, by prompt uuid.
    steer_texts: HashMap<String, String>,
    /// Texts the running CLI already holds as queued prompts: steers an
    /// earlier turn ended without taking, which Claude runs next on its own.
    queued: Vec<String>,
    /// A message steered the turn.
    steered: bool,
    /// What the prompt added to the session's attachment budget, reported
    /// once the turn starts.
    delivered: Option<(u64, u64)>,
    /// The turn's whole usage was reported (with its result, ahead of the
    /// outstanding tool calls it fails).
    usage_reported: bool,
}

/// One model call's usage as it streams (`message_start`, then deltas).
struct StreamUsage {
    message_id: String,
    usage: Value,
    /// When the call started (ms), from which its prompt cache lasts.
    requested_at_ms: Option<i64>,
}

impl<'a> Attempt<'a> {
    async fn run(
        spec: &TurnSpec,
        provider: &ProviderTurn,
        resume: Option<&'a str>,
        cancel: &mut Option<oneshot::Receiver<()>>,
        responses: &'a mut mpsc::UnboundedReceiver<RuntimeResponse>,
        sink: &'a Sink,
        sessions: &std::sync::Arc<super::claude_sessions::Sessions>,
    ) -> Outcome {
        let binary = program(&provider.binary_path, "claude");
        let mode = if provider.interaction_mode() == "plan" {
            "plan"
        } else {
            permission_mode(provider.runtime_mode())
        };
        // The flags the reference's Claude Agent SDK query passes, in its
        // order.
        let mut args: Vec<String> = [
            "--output-format",
            "stream-json",
            "--verbose",
            "--input-format",
            "stream-json",
            "--thinking=adaptive",
            "--thinking-display=summarized",
        ]
        .map(String::from)
        .into();
        if let Some(effort) = &provider.reasoning_effort {
            args.push(format!("--effort={effort}"));
        }
        if !provider.model.trim().is_empty() {
            args.push(format!("--model={}", provider.model));
        }
        // Requests the CLI does not settle itself come to us in every mode,
        // as the reference installs its permission callback.
        args.push("--permission-prompt-tool=stdio".into());
        if let Some(resume) = resume {
            args.push(format!("--resume={resume}"));
        }
        // T3's own tools run without asking, as the reference allows them.
        if provider.mcp.is_some() {
            args.push("--allowedTools=mcp__t3-code__*".into());
        }
        args.push("--tools=default".into());
        args.push(format!("--permission-mode={mode}"));
        if mode == "bypassPermissions" {
            args.push("--allow-dangerously-skip-permissions".into());
        }
        args.push("--include-partial-messages".into());
        args.push(format!("--add-dir={}", spec.cwd));
        for dir in &provider.extra_dirs {
            args.push(format!("--add-dir={dir}"));
        }
        let mut session = resume.map(str::to_owned);
        let mut new_session = None;
        if resume.is_none() {
            // The reference picks a new session's id itself, so the thread
            // names its session even if the CLI never starts.
            let id = uuid::Uuid::new_v4().to_string();
            new_session = Some(format!("--session-id={id}"));
            session = Some(id);
        }
        let mut settings = provider
            .claude_settings
            .as_object()
            .cloned()
            .unwrap_or_default();
        settings.insert("showThinkingSummaries".into(), json!(true));
        args.extend([
            "--thinking-display".into(),
            "summarized".into(),
            "--settings".into(),
            Value::Object(settings).to_string(),
        ]);
        args.extend(provider.launch_args.iter().cloned());
        let mut extra_env = vec![
            ("CLAUDE_CODE_ENTRYPOINT".into(), "t3-code".into()),
            ("CLAUDE_CODE_SDK_READS_SESSION_STATE".into(), "1".into()),
            (
                "CLAUDE_AGENT_SDK_VERSION".into(),
                CLAUDE_AGENT_SDK_VERSION.into(),
            ),
        ];
        if !provider.app_thread_id.is_empty() {
            extra_env.push(("T3CODE_THREAD_ID".into(), provider.app_thread_id.clone()));
        }
        // What the CLI is launched with, but for its session: a turn launched
        // the same way goes to the thread's running CLI.
        let launch = json!({
            "binary": binary,
            "args": args.iter().filter(|a| !a.starts_with("--resume=")).collect::<Vec<_>>(),
            "env": spec.env, "extraEnv": extra_env, "home": provider.home_path,
            "mcp": provider.mcp.as_ref().map(|mcp| (&mcp.url, &mcp.authorization)),
        })
        .to_string();
        let (running, queued) = match resume {
            Some(id) => match sessions.take(id, &launch).await {
                Some((cli, queued)) => (Some(cli), queued),
                None => (None, Vec::new()),
            },
            None => (None, Vec::new()),
        };
        let reused = running.is_some();
        if let (Some(flag), Some(id)) = (&new_session, &session) {
            // A new session's id goes after the flags it shares with a
            // resumed one, as before.
            let at = args
                .iter()
                .position(|a| a == "--thinking-display")
                .unwrap_or(args.len());
            args.insert(at, flag.clone());
            sink.send(ProviderEvent::ProviderThread {
                driver: CLAUDE_AGENT.into(),
                native_thread_id: id.clone(),
                resumed: false,
                model: Some(provider.model.clone()).filter(|model| !model.trim().is_empty()),
            });
        }
        // The reference marks only a turn that resumes an interrupted one
        // without a prompt (`resumeTurn`), which this server does not send;
        // a turn in a resumed session is not marked.
        let cli = match running {
            Some(cli) => Ok(cli),
            None => Cli::spawn(
                spec,
                Launch {
                    program: binary.clone(),
                    args,
                    home_var: "CLAUDE_CONFIG_DIR",
                    home: &provider.home_path,
                    extra_env,
                },
            ),
        };
        let cli = match cli {
            Ok(cli) => cli,
            Err(_) => {
                // The reference's SDK starts Claude Code through a shell, which
                // reports a missing one as exit 127 (upstream #39299).
                tracing::warn!(%binary, "Claude Code could not be started");
                let failure = ProviderFailure::new(
                    "transport_error",
                    "Claude Code process exited with code 127 before it started. Check the server logs for the provider's output, then retry the turn.",
                );
                return Outcome::Ended(TurnEnd::failed(failure, ThreadDisposition::Reusable));
            }
        };
        let mut attempt = Attempt {
            sink,
            cli,
            resume,
            reused,
            session,
            answered: false,
            interrupted: false,
            message_id: None,
            blocks: HashMap::new(),
            open_items: vec![],
            tools: IndexMap::new(),
            next_control: 1,
            runtime_mode: provider.runtime_mode().to_owned(),
            responses,
            pending: HashMap::new(),
            context_window: provider.context_window,
            stream_usage: None,
            root_models: Vec::new(),
            turn_started: false,
            result: None,
            steers: HashMap::new(),
            steer_texts: HashMap::new(),
            queued,
            steered: false,
            delivered: None,
            usage_reported: false,
        };
        let outcome = attempt.drive(spec, cancel).await;
        // A prompt the CLI took without starting a turn still counts.
        attempt.report_delivery();
        // Steers a completed turn took settle before its usage; ones a
        // stopped turn never took are cancelled after it, as the reference
        // reports them.
        let completed =
            matches!(&outcome, Outcome::Ended(end) if end.status == TurnStatus::Completed);
        if completed {
            attempt.settle_steers(TurnStatus::Completed);
        }
        if let (Outcome::Ended(end), true, false) =
            (&outcome, attempt.turn_started, attempt.usage_reported)
        {
            attempt.sink.send(ProviderEvent::TurnUsage {
                usage: turn_token_usage(attempt.result.as_ref(), end.status, &attempt.root_models),
            });
        }
        if let Outcome::Ended(end) = &outcome {
            if !completed {
                attempt.settle_steers(end.status);
            }
        }
        // A turn that ended on its own leaves its CLI running for the
        // thread's next turn; Stop closes it, as the reference's interrupt
        // does, and a CLI that stopped (or was stopped) is gone.
        let keep = matches!(&outcome, Outcome::Ended(end) if end.status != TurnStatus::Interrupted);
        let Attempt {
            mut cli,
            session,
            queued,
            ..
        } = attempt;
        match session {
            Some(session) if keep && cli.alive() => sessions.keep(session, cli, launch, queued),
            _ => cli.close().await,
        }
        outcome
    }

    async fn drive(
        &mut self,
        spec: &TurnSpec,
        cancel: &mut Option<oneshot::Receiver<()>>,
    ) -> Outcome {
        // T3's instructions for the agent, appended to Claude Code's system
        // prompt as the reference's query initializes it, once per CLI.
        if !self.reused {
            let has_mcp = spec.provider.as_ref().is_some_and(|p| p.mcp.is_some());
            let request_id = format!("t3-initialize-{}", self.next_control);
            self.next_control += 1;
            let request = json!({"type": "control_request", "request_id": request_id,
                "request": {"subtype": "initialize",
                    "appendSystemPrompt": crate::agent_instructions::claude_append(has_mcp)}});
            if self.cli.write(&request).await.is_err() {
                return self.closed().await;
            }
        }
        // T3's MCP server, acting as the thread, as the reference's query
        // sets it (`setMcpServers`) before the prompt, once per CLI.
        if let Some(mcp) = spec
            .provider
            .as_ref()
            .and_then(|p| p.mcp.as_ref())
            .filter(|_| !self.reused)
        {
            let request_id = format!("t3-mcp-{}", self.next_control);
            self.next_control += 1;
            let request = json!({"type": "control_request", "request_id": request_id,
            "request": {"subtype": "mcp_set_servers", "servers": {"t3-code": {
                "type": "http",
                "url": mcp.url,
                "headers": {"Authorization": mcp.authorization},
                "timeout": T3_MCP_TOOL_TIMEOUT_MS,
            }}}});
            if self.cli.write(&request).await.is_err() {
                return self.closed().await;
            }
        }
        // A plain prompt goes as a string, as the reference's
        // makeClaudeUserMessage builds it. With attachments
        // (`makeClaudeUserMessageWithAttachments`), each image goes as a
        // base64 block, then the text naming every attachment's file.
        let attachments = spec
            .provider
            .as_ref()
            .map(|p| p.attachments.as_slice())
            .unwrap_or_default();
        let mut image_bytes = 0u64;
        let content = if attachments.is_empty() {
            json!(spec.prompt)
        } else {
            use crate::workspace::attachment_prompt as prompt;
            let text = prompt::text_with_paths(&spec.prompt, attachments);
            let mut blocks = Vec::new();
            for attachment in attachments.iter().filter(|a| a["type"] == "image") {
                let mime = attachment["mimeType"].as_str().unwrap_or_default();
                let failure = |detail: String| {
                    Outcome::Ended(TurnEnd::failed(
                        ProviderFailure::new("provider_error", detail),
                        ThreadDisposition::Reusable,
                    ))
                };
                if !matches!(
                    mime,
                    "image/gif" | "image/jpeg" | "image/png" | "image/webp"
                ) {
                    return failure(format!("Unsupported Claude image attachment type '{mime}'"));
                }
                let data = match prompt::image_base64(attachment) {
                    Ok(data) => data,
                    Err(detail) => return failure(detail.trim_end_matches('.').to_owned()),
                };
                image_bytes += data.len() as u64 + 128;
                blocks.push(json!({"type": "image", "source": {"type": "base64", "media_type": mime, "data": data}}));
            }
            if !text.is_empty() {
                blocks.push(json!({"type": "text", "text": text}));
            }
            json!(blocks)
        };
        // A steer the last turn ended without taking is still queued in the
        // CLI, which runs it next on its own: the turn that sends that
        // message again takes it rather than writing it a second time (the
        // reference's continuation adopts the wake). Its bytes counted with
        // the steer.
        let adopted = attachments.is_empty()
            && self
                .queued
                .iter()
                .position(|text| *text == spec.prompt)
                .map(|at| self.queued.remove(at))
                .is_some();
        let prompt = json!({
            "type": "user",
            "session_id": "",
            "message": {"role": "user", "content": content},
            "parent_tool_use_id": null,
        });
        if !adopted && self.cli.write(&prompt).await.is_err() {
            return self.closed().await;
        }
        // What the prompt adds to the session's attachment budget, which
        // the reference reports once the turn starts.
        // With attachments it counts the text block and each image's base64.
        let delivered = if attachments.is_empty() {
            content.to_string().len() as u64
        } else {
            let text = content
                .as_array()
                .into_iter()
                .flatten()
                .filter(|block| block["type"] == "text")
                .cloned()
                .collect::<Vec<_>>();
            json!(text).to_string().len() as u64 + image_bytes
        };
        self.delivered = (!adopted).then_some((delivered, 0));
        let mut deadline = None;
        loop {
            let line = tokio::select! {
                line = self.cli.next_line() => line,
                _ = cancelled(cancel), if !self.interrupted => {
                    // A thread change detached the session: the reference
                    // stops the session's process, and the turn's stream
                    // closes.
                    if super::take_detached(&spec.turn_id) {
                        self.cli.kill_now();
                        return Outcome::Ended(TurnEnd::failed(
                            super::detached_failure(),
                            ThreadDisposition::Reusable,
                        ));
                    }
                    self.interrupted = true;
                    self.interrupt().await;
                    // The reference closes the query after its interrupt,
                    // which aborts a waiting request's callback: it ends
                    // cancelled.
                    let mut waiting: Vec<String> = self.pending.drain().map(|(id, _)| id).collect();
                    waiting.sort();
                    for id in waiting {
                        self.sink.send(ProviderEvent::RuntimeRequestClosed {
                            id,
                            status: "cancelled".into(),
                        });
                    }
                    deadline = Some(tokio::time::Instant::now() + INTERRUPT_GRACE);
                    continue;
                }
                _ = sleep_until(deadline) => return Outcome::Ended(TurnEnd::interrupted(true)),
                response = next_response(self.responses) => {
                    self.respond(response).await;
                    continue;
                }
            };
            let Some(line) = line else {
                if self.interrupted {
                    return Outcome::Ended(TurnEnd::interrupted(true));
                }
                return self.closed().await;
            };
            if let Some(outcome) = self.handle_available(line).await {
                return outcome;
            }
        }
    }

    /// The SDK reads control requests directly from the transport while the adapter
    /// consumes queued assistant output. Service a ready callback before publishing
    /// that backlog, without waiting for another frame or passing a turn boundary.
    async fn handle_available(&mut self, mut line: String) -> Option<Outcome> {
        let mut queued = Vec::new();
        let barrier = loop {
            let Ok(message) = serde_json::from_str::<Value>(&line) else {
                break None;
            };
            match message["type"].as_str() {
                Some("stream_event" | "assistant") => queued.push(message),
                Some("control_request" | "control_cancel_request") => {
                    // The reference's adapter has taken the new model call's
                    // start (and its usage) off the queue by the time the
                    // SDK fires the callback; the call's blocks wait.
                    if queued.first().is_some_and(|first: &Value| {
                        first["type"] == "stream_event" && first["event"]["type"] == "message_start"
                    }) {
                        let start = queued.remove(0);
                        if let Some(end) = self.handle(start).await {
                            return Some(end);
                        }
                    }
                    self.handle(message).await;
                    break None;
                }
                _ => break Some(message),
            }
            // Return to the select loop regularly so a busy stream cannot starve
            // an interrupt or the user's reply.
            if queued.len() >= 32 {
                break None;
            }
            match self.cli.next_line().now_or_never().flatten() {
                Some(next) => line = next,
                None => break None,
            }
        };
        for frame in queued.into_iter().chain(barrier) {
            if let Some(end) = self.handle(frame).await {
                return Some(end);
            }
        }
        None
    }

    /// The CLI closed its output before the turn ended.
    async fn closed(&mut self) -> Outcome {
        let status = self.cli.exit_status().await;
        if !self.answered
            && self.resume.is_some()
            && self.cli.stderr_contains(CONVERSATION_NOT_FOUND)
        {
            return Outcome::ConversationMissing;
        }
        let failure = if self.answered {
            claude_crashed(status)
        } else {
            let eperm = self.cli.stderr_contains("EPERM")
                || self.cli.stderr_contains("Operation not permitted");
            super::claude_startup_exit(status, eperm)
        };
        Outcome::Ended(TurnEnd::failed(failure, ThreadDisposition::Reusable))
    }

    async fn interrupt(&mut self) {
        let request_id = format!("t3-interrupt-{}", self.next_control);
        self.next_control += 1;
        let _ = self
            .cli
            .write(&json!({"type": "control_request", "request_id": request_id,
                "request": {"subtype": "interrupt"}}))
            .await;
    }

    /// Report what the prompt added to the attachment budget, once.
    fn report_delivery(&mut self) {
        if let Some((bytes, pages)) = self.delivered.take() {
            self.sink
                .send(ProviderEvent::PromptDelivered { bytes, pages });
        }
    }

    async fn handle(&mut self, message: Value) -> Option<Outcome> {
        let kind = message["type"].as_str().unwrap_or_default();
        if kind != "control_response" {
            self.answered = true;
        }
        self.accept_steers(&message);
        // Subagents' own messages stay inside their tool call.
        let nested = !message["parent_tool_use_id"].is_null();
        match kind {
            "system" if message["subtype"] == "init" => {
                let session = message["session_id"]
                    .as_str()
                    .unwrap_or_default()
                    .to_owned();
                let thread = ProviderEvent::ProviderThread {
                    driver: CLAUDE_AGENT.into(),
                    // The CLI was told to resume a session, so the one it
                    // reports continues it.
                    resumed: self.resume.is_some(),
                    native_thread_id: session,
                    model: message["model"].as_str().map(str::to_owned),
                };
                // A new session keeps the id it was given until the turn
                // ends, as the reference's thread does; then it names the
                // session the CLI reported.
                if self.resume.is_some() {
                    self.sink.send(thread);
                } else {
                    self.sink.report_thread_at_end(thread);
                }
                self.turn_started = true;
                self.sink.send(ProviderEvent::ProviderTurn {
                    native_turn_id: None,
                    status: TurnStatus::Running,
                    model: None,
                });
                self.report_delivery();
            }
            "stream_event" if !nested => self.stream_event(&message["event"]),
            "assistant" if !nested => self.assistant(&message["message"]),
            "user" if !nested => self.tool_results(&message),
            "control_request" => self.answer(&message).await,
            "control_cancel_request" => self.withdraw(&message["request_id"]),
            // A steer ends the segment it cut off, not the turn.
            "result"
                if self.steered
                    && matches!(
                        message["terminal_reason"].as_str(),
                        Some("aborted_streaming" | "aborted_tools")
                    ) => {}
            "result" => return Some(self.result(&message)),
            _ => {}
        }
        None
    }

    fn stream_event(&mut self, event: &Value) {
        match event["type"].as_str() {
            Some("message_start") => {
                self.message_id = event["message"]["id"].as_str().map(str::to_owned);
                self.blocks.clear();
                let usage = &event["message"]["usage"];
                if usage.is_object() {
                    self.stream_usage = Some(StreamUsage {
                        message_id: self.message_id.clone().unwrap_or_default(),
                        usage: usage.clone(),
                        requested_at_ms: Some(now_ms()),
                    });
                    self.emit_usage();
                }
            }
            Some("message_delta") => {
                let delta = &event["usage"];
                if let (Some(stream), true) = (self.stream_usage.as_mut(), delta.is_object()) {
                    // The reference's mergeClaudeStreamUsage.
                    for key in [
                        "input_tokens",
                        "cache_creation_input_tokens",
                        "cache_read_input_tokens",
                    ] {
                        if !delta[key].is_null() {
                            stream.usage[key] = delta[key].clone();
                        }
                    }
                    stream.usage["output_tokens"] = delta["output_tokens"].clone();
                    self.emit_usage();
                }
            }
            Some("content_block_start") => {
                let index = event["index"].as_u64().unwrap_or_default();
                let kind = match event["content_block"]["type"].as_str() {
                    Some("text") => "assistant_message",
                    Some("thinking") => "reasoning",
                    _ => return,
                };
                let id = format!(
                    "{}:{index}",
                    self.message_id.as_deref().unwrap_or("message")
                );
                self.blocks.insert(index, (id.clone(), kind));
                self.open_items.push((id.clone(), kind));
                self.sink.send(ProviderEvent::TurnItem {
                    item: ProviderItem::new(id, kind, "running")
                        .with("text", "")
                        .with("streaming", true),
                });
            }
            Some("content_block_delta") => {
                let index = event["index"].as_u64().unwrap_or_default();
                let Some((id, _)) = self.blocks.get(&index) else {
                    return;
                };
                let delta = &event["delta"];
                let text = delta["text"].as_str().or(delta["thinking"].as_str());
                if let Some(text) = text {
                    self.sink.send(ProviderEvent::TurnItemDelta {
                        held: false,
                        item_id: id.clone(),
                        delta: text.into(),
                    });
                }
            }
            _ => {}
        }
    }

    /// A complete assistant message: finish its streamed items and start its
    /// tool calls.
    /// The latest call's usage as the turn's `tokenUsage`, as the
    /// reference's emitContextUsage reports it; usage without input is a
    /// placeholder and is skipped.
    fn emit_usage(&mut self) {
        let Some(stream) = &self.stream_usage else {
            return;
        };
        let usage = &stream.usage;
        let count = |key: &str| usage[key].as_u64().unwrap_or(0);
        let input = count("input_tokens")
            + count("cache_creation_input_tokens")
            + count("cache_read_input_tokens");
        if input == 0 {
            return;
        }
        let output = count("output_tokens");
        let creation = &usage["cache_creation"];
        let ttl = if creation["ephemeral_1h_input_tokens"].as_u64().unwrap_or(0) > 0 {
            Some(60 * 60_000)
        } else if creation["ephemeral_5m_input_tokens"].as_u64().unwrap_or(0) > 0 {
            Some(5 * 60_000)
        } else {
            None
        };
        let expires = ttl
            .zip(stream.requested_at_ms)
            .map(|(ttl, started)| crate::auth_service::iso(started + ttl as i64));
        self.sink.send(ProviderEvent::Usage {
            usage: TokenUsage {
                used_tokens: input + output,
                max_tokens: self.context_window,
                input_tokens: input,
                cached_input_tokens: count("cache_read_input_tokens"),
                output_tokens: output,
                reasoning_output_tokens: 0,
                prompt_cache_ttl_ms: ttl,
                prompt_cache_expires_at: expires,
            },
        });
    }

    fn assistant(&mut self, message: &Value) {
        // The model each root call ran on (not Claude Code's own notices).
        if let Some(model) = message["model"].as_str().filter(|m| *m != "<synthetic>") {
            if !self.root_models.iter().any(|known| known == model) {
                self.root_models.push(model.to_owned());
            }
            self.sink.send(ProviderEvent::ProviderTurn {
                native_turn_id: None,
                status: TurnStatus::Running,
                model: Some(model.to_owned()),
            });
            // An assistant snapshot of a call that did not stream fills in
            // its usage, as the reference fills gaps in streamed usage.
            let snapshot = &message["usage"];
            let id = message["id"].as_str().unwrap_or_default();
            if snapshot.is_object()
                && self
                    .stream_usage
                    .as_ref()
                    .is_none_or(|stream| stream.message_id != id)
            {
                self.stream_usage = Some(StreamUsage {
                    message_id: id.to_owned(),
                    usage: snapshot.clone(),
                    requested_at_ms: None,
                });
                self.emit_usage();
            }
        }
        let message_id = message["id"].as_str().unwrap_or("message").to_owned();
        for (index, block) in message["content"]
            .as_array()
            .into_iter()
            .flatten()
            .enumerate()
        {
            match block["type"].as_str() {
                Some(kind @ ("text" | "thinking")) => {
                    let kind = if kind == "text" {
                        "assistant_message"
                    } else {
                        "reasoning"
                    };
                    let text = block["text"]
                        .as_str()
                        .or(block["thinking"].as_str())
                        .unwrap_or_default();
                    // The oldest open item of this kind is the one just streamed.
                    let id =
                        match self.open_items.iter().position(|(id, k)| {
                            *k == kind && id.starts_with(&format!("{message_id}:"))
                        }) {
                            Some(position) => self.open_items.remove(position).0,
                            None => format!("{message_id}:{kind}:{index}"),
                        };
                    self.sink.send(ProviderEvent::TurnItem {
                        item: ProviderItem::new(id, kind, "completed")
                            .with("text", text)
                            .with("streaming", false),
                    });
                }
                Some("tool_use") => self.tool_use(block),
                _ => {}
            }
        }
    }

    fn tool_use(&mut self, block: &Value) {
        let Some(id) = block["id"].as_str() else {
            return;
        };
        // A permission callback may already have started this native call.
        if self.tools.contains_key(id) {
            return;
        }
        let name = block["name"].as_str().unwrap_or_default();
        let input = &block["input"];
        if name == "TodoWrite" {
            let steps = input["todos"]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(|todo| {
                    Some(PlanStep {
                        step: todo["content"].as_str()?.into(),
                        status: match todo["status"].as_str() {
                            Some("in_progress") => "inProgress",
                            Some("completed") => "completed",
                            _ => "pending",
                        }
                        .into(),
                    })
                })
                .collect();
            self.sink.send(ProviderEvent::Plan {
                steps,
                explanation: None,
            });
            return;
        }
        let item = tool_item(id, name, input);
        self.sink
            .send(ProviderEvent::TurnItem { item: item.clone() });
        self.tools.insert(id.into(), item);
    }

    fn tool_results(&mut self, message: &Value) {
        for block in message["message"]["content"]
            .as_array()
            .into_iter()
            .flatten()
        {
            if block["type"] != "tool_result" {
                continue;
            }
            let Some(mut item) = block["tool_use_id"]
                .as_str()
                .and_then(|id| self.tools.shift_remove(id))
            else {
                continue;
            };
            let failed = block["is_error"] == true;
            let text = result_text(&block["content"]);
            item.status = if failed { "failed" } else { "completed" }.into();
            match item.kind.as_str() {
                "command_execution" => {
                    let structured = &message["tool_use_result"];
                    let output =
                        match (structured["stdout"].as_str(), structured["stderr"].as_str()) {
                            (Some(out), Some(err)) if !err.is_empty() => format!("{out}\n{err}"),
                            (Some(out), _) if !out.is_empty() => out.to_owned(),
                            // Nothing printed, or the command went to the background.
                            _ => text,
                        };
                    item.fields.insert("output".into(), output.into());
                    if failed {
                        item.fields
                            .insert("outputIndicatesFailure".into(), true.into());
                    }
                }
                "file_change" => {}
                _ => {
                    // The reference keeps a lone result's structured
                    // `tool_use_result` (a Read's file, ...), else the raw
                    // result content.
                    let results = message["message"]["content"]
                        .as_array()
                        .map_or(0, |blocks| {
                            blocks.iter().filter(|b| b["type"] == "tool_result").count()
                        });
                    let structured = &message["tool_use_result"];
                    let present = match structured {
                        Value::Object(map) => !map.is_empty(),
                        Value::Array(items) => !items.is_empty(),
                        Value::Null => false,
                        _ => true,
                    };
                    let output = if results == 1 && present {
                        structured.clone()
                    } else if block["content"].is_null() {
                        Value::from(text)
                    } else {
                        block["content"].clone()
                    };
                    item.fields.insert("output".into(), output);
                }
            }
            self.sink.send(ProviderEvent::TurnItem { item });
        }
    }

    /// Answer a control request from Claude Code. A `can_use_tool` request
    /// is settled now where the reference settles it itself, and otherwise
    /// waits for the user.
    async fn answer(&mut self, message: &Value) {
        let request = &message["request"];
        let request_id = message["request_id"].clone();
        let response = match request["subtype"].as_str() {
            Some("can_use_tool") => match self.can_use_tool(&request_id, request) {
                Some(result) => json!({"subtype": "success", "request_id": request_id,
                    "response": result}),
                None => return,
            },
            _ => json!({"subtype": "error", "request_id": request_id,
                "error": format!("Unsupported control request: {}", request["subtype"])}),
        };
        let _ = self
            .cli
            .write(&json!({"type": "control_response", "response": response}))
            .await;
    }

    /// The reference's canUseTool: the permission result to send now, or
    /// `None` when the request waits for the user.
    fn can_use_tool(&mut self, request_id: &Value, request: &Value) -> Option<Value> {
        let tool_name = request["tool_name"].as_str().unwrap_or_default().to_owned();
        let input = request["input"].clone();
        let tool_use_id = request["tool_use_id"]
            .as_str()
            .or(request_id.as_str())
            .unwrap_or_default()
            .to_owned();
        if tool_name == "ExitPlanMode" {
            return Some(json!({"behavior": "deny", "toolUseID": tool_use_id,
                "message": "The client captured your proposed plan. Stop here and wait for the user's feedback or implementation request in a later turn."}));
        }
        // A request for a call not streamed yet starts its tool item, as the
        // reference's canUseTool does (subagent launches excepted).
        if tool_name != "Agent" {
            self.tool_use(&json!({"id": tool_use_id, "name": tool_name, "input": input}));
        }
        let mode = permission_mode(&self.runtime_mode);
        let question = tool_name == "AskUserQuestion";
        // Only Auto mode lets Claude Code's own classifier settle what it
        // forwards; every other mode asks the user.
        if !question && mode == "auto" {
            return Some(json!({"behavior": "allow", "updatedInput": input,
                "toolUseID": tool_use_id}));
        }
        let reason =
            strip_terminal_escapes(request["decision_reason"].as_str().unwrap_or_default());
        if tool_name == "Bash"
            && mode == "bypassPermissions"
            && request["matched_ask_rule"].is_null()
            && (removal_warning(&reason)
                || reason == "This shell -c script runs rm and could not be checked"
                || reason == "A permission rule denies a command in this shell -c script")
        {
            return Some(
                json!({"behavior": "deny", "toolUseID": tool_use_id, "message": format!(
                "Claude Code's removal safety check denied this command; the user did not reject it. The command did not run. Do not bypass the check by splitting the removal, putting it in a script, or using another tool or shell. If the warning below supplies a safe rewrite, use it and let the same check evaluate it. Otherwise continue the rest of the task without the removal and tell the user what you needed to delete and why. Warning: {reason}")}),
            );
        }
        let kind = if question {
            "user_input"
        } else {
            tool_request_kind(&tool_name)
        };
        let mut open = requests::open(&tool_use_id, &tool_use_id, kind, &self.runtime_mode);
        if question {
            open.fields
                .insert("questions".into(), user_input_questions(&input));
        } else {
            open.supports_feedback = true;
            let command = input["command"].as_str().filter(|c| !c.trim().is_empty());
            let prompt = command
                .map(str::to_owned)
                .or_else(|| request["title"].as_str().map(str::to_owned))
                .or_else(|| request["description"].as_str().map(str::to_owned))
                .or_else(|| request["decision_reason"].as_str().map(str::to_owned))
                .unwrap_or_else(|| summarize_tool_request(&tool_name, &input));
            if !reason.is_empty() && reason != prompt {
                open.fields.insert("reason".into(), json!(reason));
            }
            if mode == "bypassPermissions" {
                let cause = if request["matched_ask_rule"].is_null() {
                    "provider_check"
                } else {
                    "ask_rule"
                };
                open.fields
                    .insert("fullAccessOverride".into(), json!(cause));
            }
            if request["suppress_always_allow_rule"] == true {
                open.fields.insert(
                    "options".into(),
                    json!([{"decision": "decline", "label": "Decline"},
                        {"decision": "accept", "label": "Approve"}]),
                );
            }
            open.fields.insert("prompt".into(), json!(prompt));
        }
        self.pending.insert(
            open.id.clone(),
            PendingPermission {
                request_id: request_id.clone(),
                tool_name,
                input,
                tool_use_id,
                suggestions: request
                    .get("permission_suggestions")
                    .filter(|s| !s.is_null())
                    .cloned(),
                question,
            },
        );
        self.sink
            .send(ProviderEvent::RuntimeRequestOpened { request: open });
        None
    }

    /// Send the user's answer to a waiting `can_use_tool` request.
    async fn respond(&mut self, response: RuntimeResponse) {
        if let Some(text) = &response.steer {
            return self.steer(&response.id, text).await;
        }
        let Some(pending) = self.pending.remove(&response.id) else {
            return;
        };
        let result = if pending.question {
            let Some(answers) = &response.answers else {
                self.pending.insert(response.id.clone(), pending);
                return;
            };
            json!({"behavior": "allow", "toolUseID": pending.tool_use_id, "updatedInput": {
                "questions": pending.input["questions"],
                "answers": sdk_answers(answers),
            }})
        } else {
            permission_result(
                &pending,
                requests::decision(&response),
                response.feedback.as_deref(),
            )
        };
        let reply = json!({"type": "control_response", "response": {
            "subtype": "success", "request_id": pending.request_id, "response": result}});
        let _ = self.cli.write(&reply).await;
    }

    /// Write the steer message `message_id` to the CLI as the reference's
    /// steerTurn does: a user message with priority `now` and a uuid Claude
    /// echoes once it takes it. Until then the steer is pending.
    async fn steer(&mut self, message_id: &str, text: &str) {
        let uuid = uuid::Uuid::new_v4().to_string();
        self.steers.insert(uuid.clone(), message_id.to_owned());
        self.steer_texts.insert(uuid.clone(), text.to_owned());
        self.steered = true;
        self.sink.send(ProviderEvent::SteerUpdated {
            message_id: message_id.to_owned(),
            status: "pending".into(),
            untaken: false,
        });
        let message = json!({
            "type": "user",
            "message": {"role": "user", "content": text},
            "parent_tool_use_id": null,
            "priority": "now",
            "uuid": uuid,
        });
        let written = self.cli.write(&message).await.is_ok();
        if written {
            // What the steer adds to the session's attachment budget, as a prompt.
            self.sink.send(ProviderEvent::PromptDelivered {
                bytes: json!(text).to_string().len() as u64,
                pages: 0,
            });
        } else if let Some(message_id) = self.steers.remove(&uuid) {
            // The reference settles a steer it could not offer.
            self.sink.send(ProviderEvent::SteerUpdated {
                message_id,
                status: "completed".into(),
                untaken: false,
            });
        }
    }

    /// Steers `message` shows Claude took: those whose uuid it echoes
    /// (`user_message_uuids`), or a prompt lifecycle that started one.
    /// Completed alone does not count; Claude completes prompts it never
    /// started.
    fn accept_steers(&mut self, message: &Value) {
        if self.steers.is_empty() {
            return;
        }
        let accepted: Vec<String> = if message["type"] == "command_lifecycle" {
            match (message["command_uuid"].as_str(), message["state"].as_str()) {
                (Some(uuid), Some("started")) => vec![uuid.to_owned()],
                _ => vec![],
            }
        } else if let Some(uuids) = message["user_message_uuids"].as_array() {
            uuids
                .iter()
                .filter_map(Value::as_str)
                .map(str::to_owned)
                .collect()
        } else {
            message["user_message_uuid"]
                .as_str()
                .map(|uuid| vec![uuid.to_owned()])
                .unwrap_or_default()
        };
        for uuid in accepted {
            self.steer_texts.remove(&uuid);
            if let Some(message_id) = self.steers.remove(&uuid) {
                self.sink.send(ProviderEvent::SteerUpdated {
                    message_id,
                    status: "completed".into(),
                    untaken: false,
                });
            }
        }
    }

    /// The turn ended: steers still pending end with it, completed if the
    /// turn did, otherwise cancelled.
    fn settle_steers(&mut self, status: TurnStatus) {
        let settled = if status == TurnStatus::Completed {
            "completed"
        } else {
            "cancelled"
        };
        // A turn that completed before Claude took a steer leaves it to be
        // sent again as its own run, as the reference's steer fails with
        // `turnCompleted` and its EffectWorker re-dispatches the message.
        let untaken = status == TurnStatus::Completed;
        for (uuid, message_id) in self.steers.drain() {
            // Still queued in the CLI for the turn that sends it again.
            if untaken {
                if let Some(text) = self.steer_texts.remove(&uuid) {
                    self.queued.push(text);
                }
            }
            self.sink.send(ProviderEvent::SteerUpdated {
                message_id,
                status: settled.into(),
                untaken,
            });
        }
    }

    /// Claude Code withdrew the control request `request_id`.
    fn withdraw(&mut self, request_id: &Value) {
        let ids: Vec<String> = self
            .pending
            .iter()
            .filter(|(_, pending)| &pending.request_id == request_id)
            .map(|(id, _)| id.clone())
            .collect();
        for id in ids {
            self.pending.remove(&id);
            self.sink.send(ProviderEvent::RuntimeRequestClosed {
                id,
                status: "cancelled".into(),
            });
        }
    }

    fn result(&mut self, message: &Value) -> Outcome {
        self.result = Some(message.clone());
        let errors: Vec<&str> = message["errors"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(Value::as_str)
            .collect();
        if self.resume.is_some() && errors.iter().any(|e| e.starts_with(CONVERSATION_NOT_FOUND)) {
            return Outcome::ConversationMissing;
        }
        if self.interrupted {
            return Outcome::Ended(TurnEnd::interrupted(true));
        }
        if message["subtype"] == "success" && message["is_error"] != true {
            // A successful turn cannot supply any more tool results. The SDK
            // fails its outstanding calls in start order before ending the turn,
            // after the reference has reported the turn's usage with its result.
            if !self.tools.is_empty() && self.turn_started {
                self.sink.send(ProviderEvent::TurnUsage {
                    usage: turn_token_usage(
                        Some(message),
                        TurnStatus::Completed,
                        &self.root_models,
                    ),
                });
                self.usage_reported = true;
            }
            for (_, mut item) in self.tools.drain(..) {
                item.status = "failed".into();
                self.sink.send(ProviderEvent::TurnItem { item });
            }
            return Outcome::Ended(TurnEnd::completed());
        }
        let text = message["result"]
            .as_str()
            .map(str::to_owned)
            .or_else(|| (!errors.is_empty()).then(|| errors.join("\n")))
            .unwrap_or_else(|| "Claude turn failed.".into());
        let class = match message["subtype"].as_str() {
            Some("error_max_turns") | Some("error_max_budget_usd") => "usage_limit",
            _ if text.contains("usage limit") || text.contains("limit reached") => "usage_limit",
            _ => "provider_error",
        };
        let mut failure = ProviderFailure::new(class, text);
        failure.code = message["subtype"].as_str().map(str::to_owned);
        Outcome::Ended(TurnEnd::failed(failure, ThreadDisposition::Reusable))
    }
}

/// A Claude Code tool call as a turn item.
fn tool_item(id: &str, name: &str, input: &Value) -> ProviderItem {
    match name {
        "Bash" => ProviderItem::new(id, "command_execution", "running")
            .with("input", input["command"].as_str().unwrap_or_default()),
        "Edit" | "Write" | "MultiEdit" | "NotebookEdit" => {
            let path = input["file_path"]
                .as_str()
                .or(input["notebook_path"].as_str())
                .unwrap_or_default();
            // The reference's Claude file changes name the file only.
            let mut item = ProviderItem::new(id, "file_change", "running").with("fileName", path);
            if let Some(old) = input["old_string"].as_str() {
                item = item.with("oldStr", old);
            }
            if let Some(new) = input["new_string"].as_str().or(input["content"].as_str()) {
                item = item.with("newStr", new);
            }
            item
        }
        "WebSearch" => {
            let mut item = ProviderItem::new(id, "web_search", "running");
            if let Some(query) = input["query"].as_str() {
                item = item.with("patterns", vec![query.to_owned()]);
            }
            item
        }
        _ => {
            let mut item = ProviderItem::new(id, "dynamic_tool", "running")
                .with("toolName", name)
                .with("input", input.clone());
            item.title = dynamic_tool_title(name, input);
            item
        }
    }
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_millis() as i64)
}

/// The reference's normalizeClaudeTurnTokenUsage for a turn that ran in one
/// segment: the `result`'s totals, `complete` only for a turn that completed
/// with a successful result.
fn turn_token_usage(result: Option<&Value>, status: TurnStatus, models: &[String]) -> Value {
    let usage = result.map_or(&Value::Null, |result| &result["usage"]);
    let count = |key: &str| {
        usage[key]
            .as_f64()
            .filter(|n| n.is_finite() && *n >= 0.0)
            .map(|n| n.round() as u64)
    };
    let uncached = count("input_tokens");
    let cached = count("cache_read_input_tokens");
    let creation = count("cache_creation_input_tokens");
    let output = count("output_tokens");
    let thinking = usage["output_tokens_details"]["thinking_tokens"]
        .as_f64()
        .filter(|n| n.is_finite() && *n >= 0.0)
        .map(|n| n.round() as u64);
    let success = result.is_some_and(|result| result["subtype"] == "success");
    let known = uncached.is_some() || cached.is_some() || creation.is_some() || output.is_some();
    let positive =
        uncached.unwrap_or(0) + cached.unwrap_or(0) + creation.unwrap_or(0) + output.unwrap_or(0)
            > 0;
    let mut out = serde_json::Map::new();
    if !models.is_empty() {
        out.insert("models".into(), json!(models));
    }
    if !known || (!success && !positive) {
        out.insert("usageStatus".into(), json!("unavailable"));
        out.insert("usageScope".into(), json!("main_agent"));
        out.insert("hasSubagents".into(), json!(false));
        return Value::Object(out);
    }
    let input = uncached.map(|uncached| {
        uncached
            + if usage["cache_read_input_tokens"].is_null() {
                0
            } else {
                cached.unwrap_or(0)
            }
            + if usage["cache_creation_input_tokens"].is_null() {
                0
            } else {
                creation.unwrap_or(0)
            }
    });
    out.insert("usageScope".into(), json!("main_agent"));
    if let Some(cached) = cached {
        out.insert("cachedInputTokens".into(), json!(cached));
    }
    if let Some(creation) = creation {
        out.insert("cacheCreationTokens".into(), json!(creation));
    }
    if let (Some(thinking), Some(output)) = (thinking, output) {
        out.insert("reasoningTokens".into(), json!(output.min(thinking)));
    }
    out.insert("hasSubagents".into(), json!(false));
    let complete =
        status == TurnStatus::Completed && success && input.is_some() && output.is_some();
    out.insert(
        "usageStatus".into(),
        json!(if complete { "complete" } else { "partial" }),
    );
    if let Some(input) = input {
        out.insert("inputTokens".into(), json!(input));
    }
    if let Some(output) = output {
        out.insert("outputTokens".into(), json!(output));
    }
    Value::Object(out)
}

/// The reference's classifyClaudeNativeTool request kind.
fn tool_request_kind(tool_name: &str) -> &'static str {
    let normalized: String = tool_name
        .to_lowercase()
        .chars()
        .filter(|c| !c.is_whitespace() && *c != '_' && *c != '-')
        .collect();
    match normalized.as_str() {
        "edit" | "multiedit" | "notebookedit" | "write" => "file-change",
        "glob" | "grep" | "ls" | "read" => "file-read",
        _ => "command",
    }
}

/// The reference's summarizeClaudeToolRequest.
fn summarize_tool_request(tool_name: &str, input: &Value) -> String {
    let first = |keys: &[&str]| {
        keys.iter().find_map(|key| {
            input[*key]
                .as_str()
                .map(str::trim)
                .filter(|value| !value.is_empty())
        })
    };
    let cut = |text: &str, limit: usize| text.chars().take(limit).collect::<String>();
    if let Some(command) = first(&["command", "cmd", "script"]) {
        return format!("{tool_name}: {}", cut(command, 400));
    }
    if let Some(path) = first(&["file_path", "path", "filename", "fileName"]) {
        return format!("{tool_name}: {}", cut(path, 400));
    }
    let serialized = match input {
        Value::String(text) => text.clone(),
        other => other.to_string(),
    };
    if serialized.chars().count() <= 400 {
        format!("{tool_name}: {serialized}")
    } else {
        format!("{tool_name}: {}...", cut(&serialized, 397))
    }
}

/// Text without terminal control sequences, trimmed.
fn strip_terminal_escapes(text: &str) -> String {
    static ESCAPES: std::sync::LazyLock<regex::Regex> = std::sync::LazyLock::new(|| {
        regex::Regex::new(r"\x1b\[[0-9;?]*[ -/]*[@-~]|\x1b\][^\x07\x1b]*(?:\x07|\x1b\\)|\x1b[@-_]")
            .expect("valid pattern")
    });
    ESCAPES.replace_all(text, "").trim().to_owned()
}

/// Claude Code's removal safety warnings (`Dangerous rm...`).
fn removal_warning(reason: &str) -> bool {
    static RM: std::sync::LazyLock<regex::Regex> = std::sync::LazyLock::new(|| {
        regex::Regex::new(r"(?i)^Dangerous rm(?:dir)?\b").expect("valid pattern")
    });
    RM.is_match(reason)
}

/// The reference's claudeUserInputQuestions.
fn user_input_questions(input: &Value) -> Value {
    let questions = input["questions"]
        .as_array()
        .into_iter()
        .flatten()
        .enumerate()
        .filter_map(|(index, record)| {
            let question = record["question"].as_str()?.trim();
            if question.is_empty() {
                return None;
            }
            let header = record["header"]
                .as_str()
                .map(str::trim)
                .filter(|h| !h.is_empty())
                .map_or_else(|| format!("Question {}", index + 1), str::to_owned);
            let options: Vec<Value> = record["options"]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(|option| {
                    let label = option["label"].as_str()?.trim();
                    if label.is_empty() {
                        return None;
                    }
                    let description = option["description"]
                        .as_str()
                        .map(str::trim)
                        .filter(|d| !d.is_empty())
                        .unwrap_or(label);
                    let mut row = json!({"label": label, "description": description});
                    if let Some(preview) = option["preview"].as_str() {
                        row["preview"] = json!(preview);
                    }
                    Some(row)
                })
                .collect();
            Some(
                json!({"id": question, "header": header, "question": question,
                "options": options, "multiSelect": record["multiSelect"] == true}),
            )
        })
        .collect();
    Value::Array(questions)
}

/// The reference's claudeSdkUserInputAnswers.
fn sdk_answers(answers: &Value) -> Value {
    let out: serde_json::Map<String, Value> = answers
        .as_object()
        .into_iter()
        .flatten()
        .map(|(question, answer)| {
            let text = match answer {
                Value::Array(values) => values
                    .iter()
                    .filter_map(Value::as_str)
                    .collect::<Vec<_>>()
                    .join(", "),
                Value::String(text) => text.clone(),
                Value::Null => String::new(),
                other => other.to_string(),
            };
            (question.clone(), json!(text))
        })
        .collect();
    Value::Object(out)
}

/// The reference's permissionResultFromDecision, with the tool use id the
/// SDK adds.
fn permission_result(pending: &PendingPermission, decision: &str, feedback: Option<&str>) -> Value {
    if decision == "accept" || decision == "acceptForSession" {
        let mut result = json!({"behavior": "allow", "updatedInput": pending.input,
            "toolUseID": pending.tool_use_id,
            "decisionClassification": if decision == "acceptForSession" { "user_permanent" } else { "user_temporary" }});
        if decision == "acceptForSession" {
            let mut updates: Vec<Value> = pending
                .suggestions
                .as_ref()
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .map(|suggestion| {
                    let mut update = suggestion.clone();
                    update["destination"] = json!("session");
                    update
                })
                .collect();
            if updates.is_empty() {
                updates.push(
                    json!({"type": "addRules", "rules": [{"toolName": pending.tool_name}],
                    "behavior": "allow", "destination": "session"}),
                );
            }
            result["updatedPermissions"] = json!(updates);
        }
        return result;
    }
    let feedback = feedback.map(str::trim).filter(|f| !f.is_empty());
    let message = match feedback {
        Some(feedback) => format!("The user declined this tool call and said:\n{feedback}"),
        None if decision == "cancel" => "User cancelled tool execution.".into(),
        None => "User declined tool execution.".into(),
    };
    let mut result = json!({"behavior": "deny", "message": message, "toolUseID": pending.tool_use_id,
        "decisionClassification": "user_reject"});
    if decision == "cancel" {
        result["interrupt"] = json!(true);
    }
    result
}

/// The title the reference gives a Claude tool item: `Read <path>` for a
/// read (`formatReadToolLabel`) and `Skill: <name>` for a skill
/// (`dynamicToolTitle`); none for the others it titles from elsewhere.
fn dynamic_tool_title(name: &str, input: &Value) -> Option<String> {
    let text = |key: &str| {
        input[key]
            .as_str()
            .map(str::trim)
            .filter(|value| !value.is_empty())
    };
    if name == "Skill" {
        return text("skill").map(|skill| format!("Skill: {skill}"));
    }
    // The reference's formatSearchToolLabel for Grep, Glob and LS.
    let normalized: String = name
        .to_lowercase()
        .chars()
        .filter(|c| !c.is_whitespace() && *c != '_' && *c != '-')
        .collect();
    if matches!(normalized.as_str(), "grep" | "glob" | "ls") {
        let first = |keys: &[&str]| keys.iter().find_map(|key| text(key));
        let query = first(&["pattern", "query", "searchTerm", "regex", "grep", "needle"]);
        let glob = first(&[
            "glob",
            "globPattern",
            "glob_pattern",
            "include",
            "filePattern",
            "file_pattern",
        ]);
        let target = first(&[
            "path",
            "target_directory",
            "targetDirectory",
            "directory",
            "cwd",
            "root",
        ])
        .and_then(|path| {
            path.split(['/', '\\'])
                .rfind(|part| !part.is_empty() && *part != ".")
        });
        let title = match (query, glob, target) {
            (Some(query), _, Some(target)) => Some(format!("Searched {query} in {target}")),
            (None, Some(glob), Some(target)) => Some(format!("Searched files {glob} in {target}")),
            (None, Some(glob), None) => Some(format!("Searched files {glob}")),
            (Some(query), _, None) => Some(format!("Searched {query}")),
            (None, None, Some(target)) => Some(format!("Searched in {target}")),
            (None, None, None) => None,
        };
        if title.is_some() {
            return title;
        }
    }
    matches!(name.to_ascii_lowercase().as_str(), "read" | "read file").then(|| {
        match text("file_path").or_else(|| text("path")) {
            Some(path) => format!("Read {path}"),
            None => "Read file".into(),
        }
    })
}

/// A tool result's content as text.
fn result_text(content: &Value) -> String {
    match content {
        Value::String(text) => text.clone(),
        Value::Array(parts) => parts
            .iter()
            .filter_map(|part| part["text"].as_str())
            .collect::<Vec<_>>()
            .join("\n"),
        _ => String::new(),
    }
}

#[cfg(test)]
mod search_title_tests {
    use super::*;

    #[test]
    fn search_tools_are_titled_as_the_reference_titles_them() {
        assert_eq!(
            dynamic_tool_title("Grep", &json!({"pattern": "todo", "path": "/w/src"})).as_deref(),
            Some("Searched todo in src")
        );
        assert_eq!(
            dynamic_tool_title("Glob", &json!({"pattern": "**/*.rs"})).as_deref(),
            Some("Searched **/*.rs")
        );
        assert_eq!(
            dynamic_tool_title("LS", &json!({"path": "/w/"})).as_deref(),
            Some("Searched in w")
        );
        assert_eq!(dynamic_tool_title("Grep", &json!({})), None);
    }
}
