//! Codex turns over `codex app-server` (JSON-RPC lines on stdio), as the
//! reference's Codex adapter runs them.
use super::{cancelled, crashed, program, sleep_until, Cli, Launch, Sink, TurnEnd};
use crate::workspace::provider::{
    PlanStep, ProviderFailure, ProviderItem, ProviderTurn, ThreadDisposition, TokenUsage,
    TurnStatus, CODEX,
};
use crate::workspace::{ProviderEvent, TurnSpec};
use serde_json::{json, Value};
use std::time::Duration;
use tokio::sync::oneshot;

/// How long an interrupted turn may take to report that it stopped.
const INTERRUPT_GRACE: Duration = Duration::from_secs(5);

pub(super) async fn run(
    spec: TurnSpec,
    provider: ProviderTurn,
    cancel: oneshot::Receiver<()>,
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
            extra_env: vec![],
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

struct Session {
    cli: Cli,
    sink: Sink,
    next_id: u64,
    cancel: Option<oneshot::Receiver<()>>,
    cancelled: bool,
    thread_id: Option<String>,
    turn_id: Option<String>,
    /// `turn/completed` for our turn, once it arrived.
    terminal: Option<Value>,
    /// The last error Codex reported without retrying.
    last_error: Option<String>,
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
        match self.drive(spec, provider).await {
            Ok(end) => end,
            Err(Failed::Cancelled) => TurnEnd::interrupted(true),
            Err(Failed::Closed) => {
                let status = self.cli.exit_status().await;
                TurnEnd::failed(
                    crashed("Codex App Server", status),
                    ThreadDisposition::Reusable,
                )
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
                "clientInfo": {"name": "T3 Code", "title": "T3 Code", "version": env!("CARGO_PKG_VERSION")},
                "capabilities": {"experimentalApi": true},
            }),
        )
        .await?;
        self.notify("initialized").await?;

        let model = (!provider.model.trim().is_empty()).then(|| provider.model.clone());
        let mut thread_params = json!({
            "cwd": spec.cwd,
            "approvalPolicy": "never",
            "sandbox": "danger-full-access",
        });
        if let Some(model) = &model {
            thread_params["model"] = json!(model);
        }
        let mut resumed = false;
        let mut thread = None;
        if let Some(resume) = provider.resume.as_deref().filter(|id| !id.is_empty()) {
            let mut params = thread_params.clone();
            params["threadId"] = json!(resume);
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

        let mut turn_params = json!({
            "threadId": thread_id,
            "input": [{"type": "text", "text": spec.prompt, "text_elements": []}],
            "cwd": spec.cwd,
            "approvalPolicy": "never",
            "sandboxPolicy": {"type": "dangerFullAccess"},
            "summary": "auto",
        });
        if let Some(model) = &model {
            turn_params["model"] = json!(model);
        }
        if let Some(effort) = &provider.reasoning_effort {
            turn_params["effort"] = json!(effort);
        }
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
            };
            let Some(line) = line else {
                if self.cancelled {
                    return Ok(TurnEnd::interrupted(true));
                }
                return Err(Failed::Closed);
            };
            self.handle_line(&line).await;
        }
        Ok(self.end())
    }

    fn end(&mut self) -> TurnEnd {
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
                TurnEnd::failed(failure, ThreadDisposition::Reusable)
            }
        }
    }

    fn turn_started(&mut self, id: &str) {
        if self.turn_id.is_none() {
            self.turn_id = Some(id.to_owned());
            self.sink.send(ProviderEvent::ProviderTurn {
                native_turn_id: Some(id.to_owned()),
                status: TurnStatus::Running,
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
            let reply = self.answer(method, params);
            let response = match reply {
                Ok(result) => json!({"id": id, "result": result}),
                Err(error) => json!({"id": id, "error": error}),
            };
            let _ = self.cli.write(&response).await;
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
            // A resumed thread first repeats its last turn's usage.
            "thread/tokenUsage/updated" if self.turn_id.is_some() => {
                let usage = &params["tokenUsage"];
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
                    },
                });
            }
            "error" if params["willRetry"] != true => {
                if let Some(message) = params["error"]["message"].as_str() {
                    self.last_error = Some(message.into());
                }
            }
            _ => {}
        }
    }

    /// The answer to a request from Codex. Approvals are granted for the
    /// session, as in the reference's full-access mode.
    fn answer(&mut self, method: &str, params: &Value) -> Result<Value, Value> {
        let request_id = params["approvalId"]
            .as_str()
            .or(params["itemId"].as_str())
            .or(params["callId"].as_str())
            .unwrap_or_default()
            .to_owned();
        let (kind, result) = match method {
            "item/commandExecution/requestApproval" => {
                ("command", json!({"decision": "acceptForSession"}))
            }
            "item/fileChange/requestApproval" => {
                ("file_change", json!({"decision": "acceptForSession"}))
            }
            "item/permissions/requestApproval" => (
                "permissions",
                json!({"permissions": params["permissions"], "scope": "session"}),
            ),
            "execCommandApproval" => ("command", json!({"decision": "approved_for_session"})),
            "applyPatchApproval" => ("file_change", json!({"decision": "approved_for_session"})),
            _ => {
                return Err(
                    json!({"code": -32601, "message": format!("Method not found: {method}")}),
                )
            }
        };
        self.sink.send(ProviderEvent::RuntimeRequest {
            native_request_id: request_id,
            kind: kind.into(),
            decision: "acceptForSession".into(),
        });
        Ok(result)
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
            mapped
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
                "input":"ls","output":"a\n","exitCode":0})
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
