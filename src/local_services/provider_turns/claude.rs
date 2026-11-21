//! Claude turns over the Claude Code CLI's stream-json protocol, the one the
//! Claude Agent SDK speaks and the reference's Claude adapter runs through.
use super::{cancelled, claude_crashed, program, sleep_until, Cli, Launch, Sink, TurnEnd};
use crate::workspace::provider::{
    PlanStep, ProviderFailure, ProviderItem, ProviderTurn, ThreadDisposition, TokenUsage,
    TurnStatus, CLAUDE_AGENT,
};
use crate::workspace::{ProviderEvent, TurnSpec};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::time::Duration;
use tokio::sync::oneshot;

/// How long an interrupted turn may take to report that it stopped.
const INTERRUPT_GRACE: Duration = Duration::from_secs(5);
/// What Claude Code prints when `--resume` names a session it does not have.
const CONVERSATION_NOT_FOUND: &str = "No conversation found with session ID:";

pub(super) async fn run(
    spec: TurnSpec,
    provider: ProviderTurn,
    cancel: oneshot::Receiver<()>,
    sink: Sink,
) {
    let mut cancel = Some(cancel);
    let mut resume = provider.resume.clone().filter(|id| !id.is_empty());
    loop {
        let attempt = Attempt::run(&spec, &provider, resume.as_deref(), &mut cancel, &sink).await;
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

/// A tool call waiting for its result.
struct PendingTool {
    item: ProviderItem,
}

struct Attempt<'a> {
    sink: &'a Sink,
    cli: Cli,
    resume: Option<&'a str>,
    /// Whether the CLI has said anything; refusals come before that.
    answered: bool,
    interrupted: bool,
    /// The message the streamed blocks belong to.
    message_id: Option<String>,
    /// Streamed blocks by index: their item id and kind.
    blocks: HashMap<u64, (String, &'static str)>,
    /// Streamed items the final assistant message has not completed yet.
    open_items: Vec<(String, &'static str)>,
    tools: HashMap<String, PendingTool>,
    next_control: u64,
    usage: Option<TokenUsage>,
}

impl<'a> Attempt<'a> {
    async fn run(
        spec: &TurnSpec,
        provider: &ProviderTurn,
        resume: Option<&'a str>,
        cancel: &mut Option<oneshot::Receiver<()>>,
        sink: &'a Sink,
    ) -> Outcome {
        let binary = program(&provider.binary_path, "claude");
        let mut args: Vec<String> = [
            "--output-format",
            "stream-json",
            "--verbose",
            "--input-format",
            "stream-json",
            "--include-partial-messages",
            "--permission-mode",
            "bypassPermissions",
            "--allow-dangerously-skip-permissions",
        ]
        .map(String::from)
        .into();
        if !provider.model.trim().is_empty() {
            args.extend(["--model".into(), provider.model.clone()]);
        }
        if let Some(resume) = resume {
            args.extend(["--resume".into(), resume.to_owned()]);
        } else {
            // The reference picks a new session's id itself, so the thread
            // names its session even if the CLI never starts.
            let session = uuid::Uuid::new_v4().to_string();
            args.extend(["--session-id".into(), session.clone()]);
            sink.send(ProviderEvent::ProviderThread {
                driver: CLAUDE_AGENT.into(),
                native_thread_id: session,
                resumed: false,
                model: Some(provider.model.clone()).filter(|model| !model.trim().is_empty()),
            });
        }
        args.extend(provider.launch_args.iter().cloned());
        let mut extra_env = vec![];
        if resume.is_some() {
            extra_env.push(("CLAUDE_CODE_RESUME_INTERRUPTED_TURN".into(), "1".into()));
            extra_env.push(("CLAUDE_CODE_RESUME_REASON".into(), "user_resume".into()));
        }
        let cli = match Cli::spawn(
            spec,
            Launch {
                program: binary.clone(),
                args,
                home_var: "CLAUDE_CONFIG_DIR",
                home: &provider.home_path,
                extra_env,
            },
        ) {
            Ok(cli) => cli,
            Err(_) => {
                // What the reference reports when the SDK cannot start
                // Claude Code (its own error names the missing binary).
                tracing::warn!(%binary, "Claude Code could not be started");
                let failure = ProviderFailure::new(
                    "provider_error",
                    "The provider could not start this turn. Retry the turn; if it keeps failing, check the provider setup and server logs.",
                );
                return Outcome::Ended(TurnEnd::failed(failure, ThreadDisposition::Reusable));
            }
        };
        let mut attempt = Attempt {
            sink,
            cli,
            resume,
            answered: false,
            interrupted: false,
            message_id: None,
            blocks: HashMap::new(),
            open_items: vec![],
            tools: HashMap::new(),
            next_control: 1,
            usage: None,
        };
        let outcome = attempt.drive(spec, cancel).await;
        let Attempt { cli, .. } = attempt;
        cli.close().await;
        outcome
    }

    async fn drive(
        &mut self,
        spec: &TurnSpec,
        cancel: &mut Option<oneshot::Receiver<()>>,
    ) -> Outcome {
        let prompt = json!({
            "type": "user",
            "session_id": "",
            "message": {"role": "user", "content": [{"type": "text", "text": spec.prompt}]},
            "parent_tool_use_id": null,
        });
        if self.cli.write(&prompt).await.is_err() {
            return self.closed().await;
        }
        let mut deadline = None;
        loop {
            let line = tokio::select! {
                line = self.cli.next_line() => line,
                _ = cancelled(cancel), if !self.interrupted => {
                    self.interrupted = true;
                    self.interrupt().await;
                    deadline = Some(tokio::time::Instant::now() + INTERRUPT_GRACE);
                    continue;
                }
                _ = sleep_until(deadline) => return Outcome::Ended(TurnEnd::interrupted(true)),
            };
            let Some(line) = line else {
                if self.interrupted {
                    return Outcome::Ended(TurnEnd::interrupted(true));
                }
                return self.closed().await;
            };
            let Ok(message) = serde_json::from_str::<Value>(&line) else {
                continue;
            };
            if let Some(outcome) = self.handle(message).await {
                return outcome;
            }
        }
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
        Outcome::Ended(TurnEnd::failed(
            claude_crashed(status),
            ThreadDisposition::Reusable,
        ))
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

    async fn handle(&mut self, message: Value) -> Option<Outcome> {
        let kind = message["type"].as_str().unwrap_or_default();
        if kind != "control_response" {
            self.answered = true;
        }
        // Subagents' own messages stay inside their tool call.
        let nested = !message["parent_tool_use_id"].is_null();
        match kind {
            "system" if message["subtype"] == "init" => {
                let session = message["session_id"]
                    .as_str()
                    .unwrap_or_default()
                    .to_owned();
                self.sink.send(ProviderEvent::ProviderThread {
                    driver: CLAUDE_AGENT.into(),
                    resumed: self.resume == Some(session.as_str()),
                    native_thread_id: session,
                    model: message["model"].as_str().map(str::to_owned),
                });
                self.sink.send(ProviderEvent::ProviderTurn {
                    native_turn_id: None,
                    status: TurnStatus::Running,
                });
            }
            "stream_event" if !nested => self.stream_event(&message["event"]),
            "assistant" if !nested => self.assistant(&message["message"]),
            "user" if !nested => self.tool_results(&message),
            "control_request" => self.answer(&message).await,
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
    fn assistant(&mut self, message: &Value) {
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
        self.tools.insert(id.into(), PendingTool { item });
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
            let Some(pending) = block["tool_use_id"]
                .as_str()
                .and_then(|id| self.tools.remove(id))
            else {
                continue;
            };
            let failed = block["is_error"] == true;
            let text = result_text(&block["content"]);
            let mut item = pending.item;
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
                    item.fields.insert("output".into(), text.into());
                }
            }
            self.sink.send(ProviderEvent::TurnItem { item });
        }
    }

    /// Approve whatever Claude Code asks, as bypassPermissions would.
    async fn answer(&mut self, message: &Value) {
        let request = &message["request"];
        let request_id = message["request_id"].clone();
        let response = match request["subtype"].as_str() {
            Some("can_use_tool") => {
                self.sink.send(ProviderEvent::RuntimeRequest {
                    native_request_id: request["tool_use_id"]
                        .as_str()
                        .or(request_id.as_str())
                        .unwrap_or_default()
                        .into(),
                    kind: match request["tool_name"].as_str() {
                        Some("Bash") => "command",
                        Some("Edit" | "Write" | "MultiEdit" | "NotebookEdit") => "file_change",
                        _ => "permissions",
                    }
                    .into(),
                    decision: "acceptForSession".into(),
                });
                json!({"subtype": "success", "request_id": request_id,
                    "response": {"behavior": "allow", "updatedInput": request["input"]}})
            }
            _ => json!({"subtype": "error", "request_id": request_id,
                "error": format!("Unsupported control request: {}", request["subtype"])}),
        };
        let _ = self
            .cli
            .write(&json!({"type": "control_response", "response": response}))
            .await;
    }

    fn result(&mut self, message: &Value) -> Outcome {
        let usage = &message["usage"];
        if usage.is_object() {
            let count = |key: &str| usage[key].as_u64().unwrap_or(0);
            let input = count("input_tokens")
                + count("cache_creation_input_tokens")
                + count("cache_read_input_tokens");
            let output = count("output_tokens");
            let max_tokens = message["modelUsage"]
                .as_object()
                .and_then(|models| models.values().find_map(|m| m["contextWindow"].as_u64()));
            let usage = TokenUsage {
                used_tokens: input + output,
                max_tokens,
                input_tokens: input,
                cached_input_tokens: count("cache_read_input_tokens"),
                output_tokens: output,
                reasoning_output_tokens: 0,
            };
            self.usage = Some(usage.clone());
            self.sink.send(ProviderEvent::Usage { usage });
        }
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
            let operation = if name == "Write" { "add" } else { "update" };
            let mut item = ProviderItem::new(id, "file_change", "running")
                .with("fileName", path)
                .with("changes", json!([{"operation": operation, "path": path}]));
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
        _ => ProviderItem::new(id, "dynamic_tool", "running")
            .with("toolName", name)
            .with("input", input.clone()),
    }
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
