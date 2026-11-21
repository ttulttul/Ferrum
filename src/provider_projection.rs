//! The projection records of a thread's provider turns, in the reference's
//! shapes: for each run, its attempt, root node, provider turn (with token
//! usage), the items the provider reported with their nodes, assistant
//! messages, the todo list, and the error or interrupt items its end adds;
//! and the thread's provider thread.
//!
//! Ids follow the reference's id allocator (`joinId`: parts joined with
//! `:`, each percent-encoded, so nested ids are encoded again). Item
//! ordinals follow its position store: run N owns
//! `N * 1_000_000 ..= N * 1_000_000 + 999_999`, its user message takes the
//! first slot and each later item the next free one, in the order the items
//! were first seen.
use crate::{
    provider_state::{ItemRecord, RunRecord, Terminal},
    thread_projection::{iso, uri_component},
    workspace::provider::{self, TurnStatus},
    Run, Thread,
};
use serde_json::{json, Map, Value};

fn join(prefix: &str, parts: &[&str]) -> String {
    let mut id = prefix.to_owned();
    for part in parts {
        id.push(':');
        id.push_str(&uri_component(part));
    }
    id
}

/// The ids one provider run's records share.
pub(crate) struct RunIds {
    pub run: String,
    pub attempt: String,
    pub root_node: String,
    pub provider_thread: String,
    pub provider_turn: String,
    pub driver: String,
}

fn item_id(prefix: &str, driver: &str, native: &str) -> String {
    format!(
        "{prefix}:provider:{}:native-item:{}",
        uri_component(driver),
        uri_component(native)
    )
}

fn native_ref(driver: &str, native: &str, strength: &str) -> Value {
    json!({"driver": driver, "nativeId": native, "strength": strength})
}

/// What the projection adds for a thread's provider turns.
#[derive(Default)]
pub(crate) struct ProviderRecords {
    pub attempts: Vec<Value>,
    pub nodes: Vec<Value>,
    pub provider_threads: Vec<Value>,
    pub provider_sessions: Vec<Value>,
    pub provider_turns: Vec<Value>,
    pub messages: Vec<Value>,
    pub plans: Vec<Value>,
    /// Turn items by the index of their run, in the order they take slots.
    pub items: Vec<(usize, Value)>,
}

impl Thread {
    fn instance_id(&self) -> String {
        let row = self.stored_row();
        row["modelSelection"]["instanceId"]
            .as_str()
            .or(row["providerInstanceId"].as_str())
            .unwrap_or(provider::CODEX)
            .to_owned()
    }

    /// The provider thread record's id, named after the run that first used
    /// it (`pending:<run id>`), as the reference names a new thread's record.
    pub(crate) fn provider_thread_id(&self) -> Option<String> {
        let thread = self.provider.thread.as_ref()?;
        let run = self.runs.iter().find(|run| run.id == thread.first_run)?;
        let run_id = run.id.to_string();
        Some(format!(
            "provider-thread:provider:{}:native-thread:{}",
            uri_component(&thread.driver),
            uri_component(&format!("pending:{run_id}"))
        ))
    }

    /// The ids of `run`'s provider records, when it ran a provider turn.
    pub(crate) fn provider_run_ids(&self, run: &Run) -> Option<RunIds> {
        let record = self.provider.runs.get(&run.id)?;
        let run_id = run.id.to_string();
        let attempt = join("run-attempt:run", &[&run_id]) + ":attempt:1";
        let native_turn = record
            .native_turn_id
            .clone()
            .unwrap_or_else(|| format!("turn:{attempt}"));
        Some(RunIds {
            root_node: join("node:run", &[&run_id]) + ":root",
            provider_thread: self.provider_thread_id().unwrap_or_default(),
            provider_turn: format!(
                "provider-turn:provider:{}:native-turn:{}",
                uri_component(&record.driver),
                uri_component(&native_turn)
            ),
            driver: record.driver.clone(),
            run: run_id,
            attempt,
        })
    }

    /// Every provider record of the thread.
    pub(crate) fn provider_records(&self) -> ProviderRecords {
        let mut out = ProviderRecords::default();
        let instance = self.instance_id();
        let mut turn_ordinal = 0;
        let mut first_ordinal = None;
        let mut last_ordinal = 0;
        for (index, run) in self.runs.iter().enumerate() {
            let (Some(record), Some(ids)) =
                (self.provider.runs.get(&run.id), self.provider_run_ids(run))
            else {
                continue;
            };
            turn_ordinal += 1;
            first_ordinal.get_or_insert(index + 1);
            last_ordinal = index + 1;
            let terminal = self.provider.terminals.get(&run.id);
            self.provider_run(
                &mut out,
                index,
                run,
                record,
                terminal,
                &ids,
                &instance,
                turn_ordinal,
            );
        }
        let session = self.provider_session(&instance);
        let session_id = session.as_ref().map(|session| session["id"].clone());
        out.provider_sessions.extend(session);
        if let (Some(thread), Some(id)) = (&self.provider.thread, self.provider_thread_id()) {
            out.provider_threads.push(json!({
                "id": id,
                "driver": thread.driver,
                "providerInstanceId": instance,
                "providerSessionId": session_id,
                "appThreadId": self.id,
                "ownerNodeId": null,
                "nativeThreadRef": thread.native_id.as_deref()
                    .map_or(Value::Null, |native| native_ref(&thread.driver, native, "strong")),
                "nativeConversationHeadRef": null,
                "status": thread.status,
                "firstRunOrdinal": first_ordinal,
                "lastRunOrdinal": last_ordinal,
                "handoffIds": [],
                "forkedFrom": null,
                "pendingBackgroundTasks": [],
                "contextUsage": null,
                "nativeMetadata": null,
                "goal": null,
                "createdAt": iso(thread.created_at),
                "updatedAt": iso(thread.updated_at)
            }));
        }
        out.messages
            .sort_by(|a, b| a["createdAt"].as_str().cmp(&b["createdAt"].as_str()));
        out
    }

    /// The provider session the thread's turns ran in, once one opened: the
    /// shared app-server session of a Codex instance, or the thread's own
    /// Claude session (which the reference opens even when the CLI is
    /// missing). A Codex session whose app-server closed mid-turn is `error`.
    fn provider_session(&self, instance: &str) -> Option<Value> {
        let thread = self.provider.thread.as_ref()?;
        let first = self.runs.iter().find(|run| run.id == thread.first_run)?;
        let claude = thread.driver == provider::CLAUDE_AGENT;
        if !claude && thread.native_id.is_none() {
            return None;
        }
        let id = if claude {
            format!(
                "provider-session:provider-instance:{}:thread:{}:{}",
                uri_component(instance),
                uri_component(&self.id),
                first.id
            )
        } else {
            format!(
                "provider-session:provider-instance:{}:shared",
                uri_component(instance)
            )
        };
        let last = self
            .runs
            .iter()
            .rev()
            .find_map(|run| self.provider.terminals.get(&run.id));
        let crashed = !claude
            && last
                .and_then(|t| t.failure.as_ref())
                .is_some_and(|failure| failure.message == provider::CODEX_STREAM_CLOSED);
        let capabilities: Value =
            serde_json::from_str(include_str!("provider_capabilities.json")).unwrap_or_default();
        let row = self.stored_row();
        Some(json!({
            "capabilities": capabilities[thread.driver.as_str()],
            "createdAt": iso(thread.created_at),
            "cwd": self.cwd,
            "driver": thread.driver,
            "extensionStatuses": [],
            "id": id,
            "lastError": if crashed { json!(provider::CODEX_STREAM_CLOSED) } else { Value::Null },
            "model": row["modelSelection"]["model"],
            "providerInstanceId": instance,
            "status": if crashed { "error" } else { "ready" },
            "updatedAt": iso(if crashed { thread.updated_at } else { thread.created_at })
        }))
    }

    #[allow(clippy::too_many_arguments)]
    fn provider_run(
        &self,
        out: &mut ProviderRecords,
        index: usize,
        run: &Run,
        record: &RunRecord,
        terminal: Option<&Terminal>,
        ids: &RunIds,
        instance: &str,
        turn_ordinal: usize,
    ) {
        let driver = ids.driver.as_str();
        let ended = terminal.map(|_| record.ended_at.unwrap_or(record.started_at));
        let status = match terminal.map(|terminal| terminal.status) {
            None | Some(TurnStatus::Running) => "running",
            Some(TurnStatus::Completed) => "completed",
            Some(TurnStatus::Interrupted) => "interrupted",
            Some(TurnStatus::Failed) => "failed",
        };
        let started = iso(record.started_at);
        let completed = ended.map(iso);
        out.attempts.push(json!({
            "id": ids.attempt,
            "runId": ids.run,
            "attemptOrdinal": 1,
            "rootNodeId": ids.root_node,
            "providerInstanceId": instance,
            "providerThreadId": ids.provider_thread,
            "providerTurnId": null,
            "reason": "initial",
            "status": status,
            "startedAt": started,
            "completedAt": completed,
            "nativeThreadId": self.provider.thread.as_ref().and_then(|t| t.native_id.clone())
        }));
        out.nodes.push(json!({
            "id": ids.root_node,
            "threadId": self.id,
            "runId": ids.run,
            "parentNodeId": null,
            "rootNodeId": ids.root_node,
            "kind": "root_turn",
            "status": status,
            "countsForRun": true,
            "providerThreadId": ids.provider_thread,
            "providerTurnId": null,
            "nativeItemRef": null,
            "runtimeRequestId": null,
            "checkpointScopeId": null,
            "startedAt": started,
            "completedAt": completed
        }));
        let native_turn = record
            .native_turn_id
            .clone()
            .unwrap_or_else(|| format!("turn:{}", ids.attempt));
        let strength = if record.native_turn_id.is_some() {
            "strong"
        } else {
            "weak"
        };
        let mut turn = json!({
            "id": ids.provider_turn,
            "providerThreadId": ids.provider_thread,
            "nodeId": ids.root_node,
            "runAttemptId": ids.attempt,
            "nativeTurnRef": native_ref(driver, &native_turn, strength),
            "ordinal": turn_ordinal,
            "status": status,
            "startedAt": started,
            "completedAt": completed
        });
        if let Some((usage, at)) = &record.usage {
            turn["tokenUsage"] = json!({
                "usedTokens": usage.used_tokens,
                "maxTokens": usage.max_tokens,
                "inputTokens": usage.input_tokens,
                "cachedInputTokens": usage.cached_input_tokens,
                "outputTokens": usage.output_tokens,
                "reasoningOutputTokens": usage.reasoning_output_tokens,
                "updatedAt": iso(*at)
            });
        }
        // The reference records a provider turn once the CLI starts one.
        if record.turn_started || record.native_turn_id.is_some() || !record.items.is_empty() {
            out.provider_turns.push(turn);
        }

        for item in &record.items {
            self.provider_item(out, index, ids, item, terminal.map(|t| t.status));
        }
        if let Some(plan) = &record.plan {
            let native = format!("turn-plan:{native_turn}");
            let node = item_id("node", driver, &native);
            let plan_id = format!(
                "plan:thread:{}:provider:{}:run:{}:{}",
                uri_component(&self.id),
                uri_component(driver),
                uri_component(&ids.run),
                uri_component(&native)
            );
            let claude = driver == provider::CLAUDE_AGENT;
            let steps: Vec<Value> = plan
                .steps
                .iter()
                .enumerate()
                .map(|(i, step)| {
                    let text = step.step.trim();
                    json!({
                        "id": if claude { format!("todo-{i}") } else { format!("step-{}", i + 1) },
                        "text": if text.is_empty() { format!("Step {}", i + 1) } else { text.to_owned() },
                        "status": match step.status.as_str() {
                            "inProgress" | "in_progress" | "running" => "running",
                            "completed" => "completed",
                            _ => "pending",
                        }
                    })
                })
                .collect();
            let done = !steps.is_empty() && steps.iter().all(|s| s["status"] == "completed");
            let mut plan_row = json!({
                "id": plan_id,
                "threadId": self.id,
                "runId": ids.run,
                "nodeId": node,
                "kind": "todo_list",
                "status": if claude && done { "completed" } else { "active" },
                "steps": steps
            });
            if let Some(explanation) = &plan.explanation {
                plan_row["explanation"] = json!(explanation);
            }
            out.plans.push(plan_row);
            let mut item = json!({
                "id": item_id("turn-item", driver, &native),
                "threadId": self.id,
                "runId": ids.run,
                "nodeId": node,
                "providerThreadId": ids.provider_thread,
                "providerTurnId": ids.provider_turn,
                "nativeItemRef": native_ref(driver, &native, "strong"),
                "parentItemId": null,
                "ordinal": 0,
                "status": "completed",
                "title": null,
                "startedAt": iso(plan.first_at),
                "completedAt": iso(plan.updated_at),
                "updatedAt": iso(plan.updated_at),
                "type": "todo_list",
                "planId": plan_id,
                "steps": steps
            });
            if let Some(explanation) = &plan.explanation {
                item["explanation"] = json!(explanation);
            }
            out.items.push((index, item));
        }

        let Some(terminal) = terminal else { return };
        let at = completed.clone().unwrap_or_else(|| started.clone());
        match terminal.status {
            // A Codex turn whose app-server never opened a session: the
            // reference reports the session, not the turn, as failed.
            TurnStatus::Failed
                if driver == provider::CODEX
                    && !record.turn_started
                    && self
                        .provider
                        .thread
                        .as_ref()
                        .is_some_and(|thread| thread.native_id.is_none()) =>
            {
                let session = join("provider-session:provider-instance", &[instance]) + ":shared";
                out.items.push((index, json!({
                    "id": join("turn-item:run", &[&ids.run]) + ":signal:provider-session-open-failure",
                    "threadId": self.id,
                    "runId": ids.run,
                    "nodeId": ids.root_node,
                    "providerThreadId": ids.provider_thread,
                    "providerTurnId": null,
                    "nativeItemRef": null,
                    "parentItemId": null,
                    "ordinal": 0,
                    "status": "failed",
                    "title": "Provider session failed to open",
                    "startedAt": at,
                    "completedAt": at,
                    "updatedAt": at,
                    "type": "error",
                    "failure": {
                        "class": "provider_error",
                        "message": format!("Failed to open {driver} provider session {session}."),
                        "code": null,
                        "retryable": null
                    }
                })));
            }
            TurnStatus::Failed => {
                let failure = terminal.failure.clone().unwrap_or_else(|| {
                    provider::ProviderFailure::new("unknown", "Provider turn failed.")
                });
                let native = format!("terminal-failure:{}", ids.provider_turn);
                out.items.push((index, json!({
                    "id": item_id("turn-item", driver, &native),
                    "threadId": self.id,
                    "runId": ids.run,
                    "nodeId": ids.root_node,
                    "providerThreadId": ids.provider_thread,
                    "providerTurnId": ids.provider_turn,
                    "nativeItemRef": null,
                    "parentItemId": null,
                    "ordinal": 0,
                    "status": "failed",
                    "title": if failure.class == "usage_limit" { "Usage limit reached" } else { "Provider error" },
                    "startedAt": at,
                    "completedAt": at,
                    "updatedAt": at,
                    "type": "error",
                    "failure": {
                        "class": failure.class,
                        "message": failure.message,
                        "code": failure.code,
                        "retryable": failure.retryable
                    }
                })));
            }
            TurnStatus::Interrupted => {
                let request = join("turn-item:run", &[&ids.run]) + ":signal:interrupt-request";
                let signal = |id: String, parent: Value, status: &str, title: &str, kind: &str| {
                    json!({
                        "id": id,
                        "threadId": self.id,
                        "runId": ids.run,
                        "nodeId": ids.root_node,
                        "providerThreadId": ids.provider_thread,
                        "providerTurnId": null,
                        "nativeItemRef": null,
                        "parentItemId": parent,
                        "ordinal": 0,
                        "status": status,
                        "title": title,
                        "startedAt": at,
                        "completedAt": at,
                        "updatedAt": at,
                        "type": kind,
                        "message": "Stopped by user"
                    })
                };
                out.items.push((
                    index,
                    signal(
                        request.clone(),
                        Value::Null,
                        "completed",
                        "Interrupt requested",
                        "run_interrupt_request",
                    ),
                ));
                out.items.push((
                    index,
                    signal(
                        join("turn-item:run", &[&ids.run]) + ":signal:interrupt-result",
                        json!(request),
                        "interrupted",
                        "Interrupted",
                        "run_interrupt_result",
                    ),
                ));
            }
            TurnStatus::Completed | TurnStatus::Running => {}
        }
        let _ = run;
    }

    fn provider_item(
        &self,
        out: &mut ProviderRecords,
        index: usize,
        ids: &RunIds,
        record: &ItemRecord,
        ended: Option<TurnStatus>,
    ) {
        let driver = ids.driver.as_str();
        let item = &record.item;
        // The reference shows no reasoning that has no text.
        if item.kind == "reasoning"
            && item
                .fields
                .get("text")
                .and_then(Value::as_str)
                .is_none_or(|text| text.trim().is_empty())
        {
            return;
        }
        let native = item.id.as_str();
        // An item still running when its turn ended ends with the turn.
        let status = match (item.status.as_str(), ended) {
            ("running", Some(TurnStatus::Interrupted)) => "interrupted",
            ("running", Some(TurnStatus::Failed)) => "failed",
            ("running", Some(TurnStatus::Completed)) => "completed",
            (status, _) => status,
        }
        .to_owned();
        let running = status == "running";
        let node = item_id("node", driver, native);
        let reference = native_ref(driver, native, "strong");
        let started = iso(record.first_at);
        let updated = iso(record.updated_at);
        let completed = (!running).then(|| updated.clone());
        let mut row = Map::new();
        for (key, value) in [
            ("id", json!(item_id("turn-item", driver, native))),
            ("threadId", json!(self.id)),
            ("runId", json!(ids.run)),
            ("nodeId", json!(node)),
            ("providerThreadId", json!(ids.provider_thread)),
            ("providerTurnId", json!(ids.provider_turn)),
            ("nativeItemRef", reference.clone()),
            ("parentItemId", Value::Null),
            ("ordinal", json!(0)),
            ("status", json!(status)),
            ("title", json!(item.title)),
            ("startedAt", json!(started)),
            ("completedAt", json!(completed)),
            ("updatedAt", json!(updated)),
            ("type", json!(item.kind)),
        ] {
            row.insert(key.into(), value);
        }
        for (key, value) in &item.fields {
            row.insert(key.clone(), value.clone());
        }
        let streams = matches!(item.kind.as_str(), "assistant_message" | "reasoning");
        if streams {
            row.insert(
                "text".into(),
                json!(item
                    .fields
                    .get("text")
                    .and_then(Value::as_str)
                    .unwrap_or_default()),
            );
            row.insert("streaming".into(), json!(running));
        }
        if item.kind == "assistant_message" {
            let message = item_id("message", driver, native);
            row.insert("messageId".into(), json!(message));
            out.messages.push(json!({
                "createdBy": "agent",
                "creationSource": "provider",
                "id": message,
                "threadId": self.id,
                "runId": ids.run,
                "nodeId": node,
                "role": "assistant",
                "text": row["text"],
                "attachments": [],
                "streaming": running,
                "createdAt": started,
                "updatedAt": updated
            }));
        }
        out.nodes.push(json!({
            "id": node,
            "threadId": self.id,
            "runId": ids.run,
            "parentNodeId": ids.root_node,
            "rootNodeId": ids.root_node,
            "kind": match item.kind.as_str() {
                "assistant_message" => "assistant_message",
                "reasoning" => "reasoning",
                _ => "tool_call",
            },
            "status": status,
            "countsForRun": false,
            "providerThreadId": ids.provider_thread,
            "providerTurnId": ids.provider_turn,
            "nativeItemRef": reference,
            "runtimeRequestId": null,
            "checkpointScopeId": null,
            "startedAt": started,
            "completedAt": completed
        }));
        out.items.push((index, Value::Object(row)));
    }
}
