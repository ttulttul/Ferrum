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
    provider_state::{ItemRecord, RequestRecord, RunRecord, Terminal},
    thread_projection::{iso, uri_component},
    workspace::provider::{self, TurnStatus},
    Run, Thread,
};
use serde_json::{json, Map, Value};

/// The key of a placeholder turn item: one the reference does not show yet,
/// which still takes its slot (see [`ProviderRecords::items`]).
pub(crate) const HIDDEN_ITEM: &str = "\u{0}hidden";

/// The id of the provider session a turn of `run_id` opens (see
/// [`pending_session_id`]), which the provider event logs name.
#[cfg(feature = "native")]
pub(crate) fn opened_session_id(
    driver: &str,
    instance: &str,
    thread_id: &str,
    run_id: &str,
    opencode2: bool,
) -> String {
    pending_session_id(driver, instance, thread_id, run_id, opencode2)
}

/// The id of the provider session a thread's provider thread names before
/// its session opens: a Codex instance's shared session, or the thread's own
/// Claude or ACP session, named after the run that opens it.
fn pending_session_id(
    driver: &str,
    instance: &str,
    thread_id: &str,
    run_id: &str,
    opencode2: bool,
) -> String {
    if provider::thread_session(driver) && !opencode2 {
        acp_session_id(instance, thread_id, run_id)
    } else if driver == provider::CLAUDE_AGENT {
        format!(
            "provider-session:provider-instance:{}:thread:{}:{}",
            uri_component(instance),
            uri_component(thread_id),
            run_id
        )
    } else {
        format!(
            "provider-session:provider-instance:{}:shared",
            uri_component(instance)
        )
    }
}

/// The id of an ACP thread's provider session, which the reference names
/// after the thread and a random UUID: here a UUID drawn from the run that
/// opens it, so it stays the same however often it is read.
fn acp_session_id(instance: &str, thread_id: &str, run_id: &str) -> String {
    use sha2::{Digest, Sha256};
    let hash = Sha256::digest(run_id.as_bytes());
    let hex = hex::encode(&hash[..16]);
    format!(
        "provider-session:provider-instance:{}:thread:{}:{}-{}-{}-{}-{}",
        uri_component(instance),
        uri_component(thread_id),
        &hex[..8],
        &hex[8..12],
        &hex[12..16],
        &hex[16..20],
        &hex[20..32]
    )
}

/// The id the reference gives a new provider thread's record: named after
/// the run that opens it (`pending:<run id>`).
fn pending_thread_id(driver: &str, run_id: &str) -> String {
    format!(
        "provider-thread:provider:{}:native-thread:{}",
        uri_component(driver),
        uri_component(&format!("pending:{run_id}"))
    )
}

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

pub(crate) fn item_id(prefix: &str, driver: &str, native: &str) -> String {
    format!(
        "{prefix}:provider:{}:native-item:{}",
        uri_component(driver),
        uri_component(native)
    )
}

/// A provider session's capabilities: its driver's. Antigravity's say
/// whether it has native goals (`readCapabilities`), which the reference
/// reads from its status snapshot's `/goal` command; Rust's status check
/// lists no slash commands, so it has none.
fn session_capabilities(capabilities: &Value, driver: &str) -> Value {
    let mut out = capabilities[driver].clone();
    if driver == provider::ANTIGRAVITY {
        out["turns"]["supportsNativeGoals"] = json!(false);
    }
    out
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
    pub runtime_requests: Vec<Value>,
    /// Native subagents the provider reported (`subagent.updated`), with
    /// `origin: provider_native`.
    pub subagents: Vec<Value>,
    /// Turn items by the index of their run, in the order they take slots;
    /// an item with [`HIDDEN_ITEM`] only holds its slot.
    pub items: Vec<(usize, Value)>,
}

/// The reference's RUNTIME_REQUEST_DELIVERY_FAILED_MESSAGE.
const DELIVERY_FAILED: &str = "T3 Code could not confirm that your answer reached the agent. If the run stays stuck, stop it and try again.";

/// A failure as the projection carries it.
fn failure_json(failure: &provider::ProviderFailure) -> Value {
    let mut out = json!({
        "class": failure.class,
        "message": failure.message,
        "code": failure.code,
        "retryable": failure.retryable
    });
    if let Some(reset) = &failure.reset_at {
        out["resetAt"] = json!(reset);
    }
    out
}

/// What the server says when a restart cut off the turn a request waited in.
const RESTART_REASON: &str = "The server restarted before this runtime request was resolved.";
const SHUTDOWN_REASON: &str = "The server shut down before this runtime request was resolved.";

/// How a request stands, after a restart may have cut its turn off: its
/// status, how it can be answered, and when it stopped waiting.
pub(crate) struct RequestState {
    pub status: &'static str,
    pub capability: Value,
    pub resolved_at: Option<u64>,
}

impl Thread {
    /// `request`'s state in `run`, whose session is `session_id`. A restart
    /// loses the CLI that waited for it: an approval expires, and a question
    /// stays open to be answered by a message, as the reference recovers
    /// them.
    pub(crate) fn request_state(
        &self,
        run: &Run,
        request: &RequestRecord,
        session_id: &Value,
    ) -> RequestState {
        let restarted = request.pending()
            && (run.restart_cut.is_some()
                || run.error.as_deref() == Some(crate::turn::RESTART_INTERRUPTION));
        let shut_down = run
            .restart_cut
            .as_ref()
            .is_some_and(|cut| cut.trigger == "shutdown");
        if restarted && request.request.kind == "user_input" {
            return RequestState {
                status: "pending",
                capability: json!({"type": "message"}),
                resolved_at: None,
            };
        }
        if restarted {
            return RequestState {
                status: if shut_down { "cancelled" } else { "expired" },
                capability: json!({"type": "not_resumable", "reason": if shut_down { SHUTDOWN_REASON } else { RESTART_REASON }}),
                resolved_at: run.finished_at.or(Some(request.created_at)),
            };
        }
        let mut capability = json!({"type": "live", "providerSessionId": session_id});
        if request.request.supports_feedback {
            capability["supportsFeedback"] = json!(true);
        }
        RequestState {
            status: request.status,
            capability,
            resolved_at: request.resolved_at,
        }
    }

    /// The projection id of a request of `run`.
    pub(crate) fn request_id(&self, ids: &RunIds, request: &RequestRecord) -> String {
        format!(
            "{}:{}",
            join(
                "runtime-request:provider",
                &[
                    &ids.driver,
                    "provider-turn",
                    &ids.provider_turn,
                    "native-request",
                    &request.request.native_request_id
                ]
            ),
            request.request.id
        )
    }
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
        let Some(thread) = self.provider.thread.as_ref() else {
            // A run waiting to start (queued, preparing, or sent and not yet
            // reported) opens one.
            let run = self.runs.iter().find(|run| {
                self.starting(run) || matches!(run.status.as_str(), "queued" | "preparing")
            })?;
            let driver = run.driver.clone().unwrap_or_else(|| self.instance_id());
            return Some(pending_thread_id(&driver, &run.wire_id()));
        };
        let run = self.runs.iter().find(|run| run.id == thread.first_run)?;
        Some(pending_thread_id(&thread.driver, &run.wire_id()))
    }

    /// The provider thread a run not yet reported by its CLI opens or
    /// returns to: on another driver than the thread's current provider
    /// thread, that driver's earlier one, or a new one named after the run.
    fn waiting_thread_id(&self, run: &Run) -> Option<String> {
        let driver = run.driver.as_deref();
        match (&self.provider.thread, driver) {
            (Some(thread), Some(driver)) if thread.driver != driver => {
                let earlier = self
                    .provider
                    .other_threads
                    .iter()
                    .find(|other| other.driver == driver)
                    .and_then(|other| {
                        let first = self.runs.iter().find(|r| r.id == other.first_run)?;
                        Some(pending_thread_id(driver, &first.wire_id()))
                    });
                Some(earlier.unwrap_or_else(|| pending_thread_id(driver, &run.wire_id())))
            }
            _ => self.provider_thread_id(),
        }
    }

    /// The id of the provider thread `run`'s turn ran on: after a switch of
    /// provider instance, an earlier run keeps the thread it ran on.
    pub(crate) fn provider_thread_id_for(&self, run: &Run) -> Option<String> {
        let opened_by = self.provider.run_threads.get(&run.id)?;
        let record = std::iter::once(self.provider.thread.as_ref())
            .flatten()
            .chain(self.provider.other_threads.iter())
            .find(|thread| thread.first_run == *opened_by)?;
        let first = self
            .runs
            .iter()
            .find(|candidate| candidate.id == *opened_by)?;
        Some(pending_thread_id(&record.driver, &first.wire_id()))
    }

    /// Whether `run` was sent to a CLI that has not reported its thread
    /// yet: the reference's run is `starting` then, with a pending attempt
    /// and root node.
    pub(crate) fn starting(&self, run: &Run) -> bool {
        run.driver.is_some()
            && run.status == "running"
            && run.restart_cut.is_none()
            && !run.resend_owed
            && !self.provider.runs.contains_key(&run.id)
    }

    /// The ids of a [`Self::starting`] run's pending records.
    pub(crate) fn starting_run_ids(&self, run: &Run) -> Option<RunIds> {
        if !self.starting(run) {
            return None;
        }
        let run_id = run.wire_id();
        let attempt = join("run-attempt:run", &[&run_id]) + ":attempt:1";
        Some(RunIds {
            root_node: join("node:run", &[&run_id]) + ":root",
            provider_thread: self.waiting_thread_id(run)?,
            provider_turn: String::new(),
            driver: run.driver.clone()?,
            run: run_id,
            attempt,
        })
    }

    /// The ids a run still waiting in the queue has on the thread's provider
    /// thread, once the thread has one, before its provider turn runs.
    pub(crate) fn queued_run_ids(&self, run: &Run) -> Option<RunIds> {
        if !matches!(run.status.as_str(), "queued" | "preparing")
            || self.provider.runs.contains_key(&run.id)
        {
            return None;
        }
        let run_id = run.wire_id();
        let attempt = join("run-attempt:run", &[&run_id]) + ":attempt:1";
        let driver = self
            .provider
            .thread
            .as_ref()
            .map(|thread| thread.driver.clone())
            .unwrap_or_else(|| self.instance_id());
        Some(RunIds {
            root_node: join("node:run", &[&run_id]) + ":root",
            provider_thread: self.waiting_thread_id(run)?,
            provider_turn: String::new(),
            driver,
            run: run_id,
            attempt,
        })
    }

    /// The ids of `run`'s provider records, when it ran a provider turn.
    pub(crate) fn provider_run_ids(&self, run: &Run) -> Option<RunIds> {
        let record = self.provider.runs.get(&run.id)?;
        let run_id = run.wire_id();
        let attempt = join("run-attempt:run", &[&run_id]) + ":attempt:1";
        // A run that failed before the provider started a turn uses
        // `failed:<attempt>` as its provider turn id (`RunExecutionService`).
        let failed_before_start = !record.turn_started
            && record.native_turn_id.is_none()
            && self
                .provider
                .terminals
                .get(&run.id)
                .is_some_and(|terminal| terminal.status == TurnStatus::Failed);
        let native_turn = record.native_turn_id.clone().unwrap_or_else(|| {
            if failed_before_start {
                format!("failed:{attempt}")
            } else {
                format!("turn:{attempt}")
            }
        });
        Some(RunIds {
            root_node: join("node:run", &[&run_id]) + ":root",
            provider_thread: self
                .provider_thread_id_for(run)
                .or_else(|| self.provider_thread_id())
                .unwrap_or_default(),
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
        // A detached session leaves the thread's records, but the records
        // that ran in it still name it, as the reference keeps them.
        let session_id = self
            .session_row(&instance)
            .map_or(Value::Null, |session| session["id"].clone());
        let session = self.provider_session(&instance);
        let (mut turn_ordinal, mut first_ordinal, mut last_ordinal) =
            match self.provider.prior_turns {
                Some((turns, first)) => (turns, Some(first), first),
                None => (0, None, 0),
            };
        // Turns count per provider thread; the current one's runs bound it.
        let active_thread = self.provider_thread_id();
        let mut other_turns: std::collections::HashMap<String, usize> =
            std::collections::HashMap::new();
        for (index, run) in self.runs.iter().enumerate() {
            let (Some(record), Some(ids)) =
                (self.provider.runs.get(&run.id), self.provider_run_ids(run))
            else {
                continue;
            };
            let turn_ordinal = if active_thread
                .as_deref()
                .is_none_or(|id| id == ids.provider_thread)
            {
                turn_ordinal += 1;
                first_ordinal.get_or_insert(index + 1);
                last_ordinal = index + 1;
                turn_ordinal
            } else {
                let count = other_turns.entry(ids.provider_thread.clone()).or_default();
                *count += 1;
                *count
            };
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
            for request in &record.requests {
                self.provider_request(&mut out, index, run, &ids, request, &session_id);
            }
        }
        // A run waiting in the queue or workspace preparation already has its
        // pending attempt and root node on the thread's provider thread, as
        // the reference gives it them; so does a run sent and not yet
        // started.
        for (index, run) in self.runs.iter().enumerate() {
            let Some(ids) = self
                .queued_run_ids(run)
                .or_else(|| self.starting_run_ids(run))
            else {
                continue;
            };
            first_ordinal.get_or_insert(index + 1);
            // A queued run joins the provider thread when it leaves the
            // queue, as the reference's queued dispatch records it then.
            if run.status != "queued" {
                last_ordinal = last_ordinal.max(index + 1);
            }
            out.attempts.push(json!({
                "id": ids.attempt,
                "runId": ids.run,
                "attemptOrdinal": 1,
                "rootNodeId": ids.root_node,
                "providerInstanceId": instance,
                "providerThreadId": ids.provider_thread,
                "providerTurnId": null,
                "reason": "initial",
                "status": "pending",
                "startedAt": null,
                "completedAt": null
            }));
            out.nodes.push(json!({
                "id": ids.root_node,
                "threadId": self.id,
                "runId": ids.run,
                "parentNodeId": null,
                "rootNodeId": ids.root_node,
                "kind": "root_turn",
                "status": "pending",
                "countsForRun": true,
                "providerThreadId": ids.provider_thread,
                "providerTurnId": null,
                "nativeItemRef": null,
                "runtimeRequestId": null,
                "checkpointScopeId": null,
                "startedAt": null,
                "completedAt": null
            }));
            if run.status == "preparing" {
                let at = self
                    .messages
                    .iter()
                    .find(|message| run.user_message_id.as_deref() == Some(message.id.as_str()))
                    .map(|message| iso(message.created_at))
                    .unwrap_or_else(|| iso(run.started_at));
                let native = format!("workspace-preparation:{}", ids.run);
                out.items.push((
                    index,
                    json!({
                        "id": item_id("turn-item", &ids.driver, &native),
                        "threadId": self.id,
                        "runId": ids.run,
                        "nodeId": ids.root_node,
                        "providerThreadId": ids.provider_thread,
                        "providerTurnId": null,
                        "nativeItemRef": null,
                        "parentItemId": null,
                        "ordinal": 0,
                        "status": "running",
                        "title": "Preparing workspace",
                        "startedAt": at,
                        "completedAt": null,
                        "updatedAt": at,
                        "type": "command_execution",
                        "input": "Preparing workspace"
                    }),
                ));
            }
        }
        // A run that waited for its workspace keeps the preparation's item,
        // completed, once it is released (`ThreadLaunchService`'s "Workspace
        // ready").
        for (index, run) in self.runs.iter().enumerate() {
            let prepared =
                !self.row["runExtras"][run.wire_id().as_str()]["workspacePreparation"].is_null();
            if !prepared || matches!(run.status.as_str(), "preparing" | "queued") {
                continue;
            }
            // A preparation that failed or was stopped never released the run.
            if !self.provider.runs.contains_key(&run.id) && run.status != "running" {
                continue;
            }
            let Some(ids) = self
                .provider_run_ids(run)
                .or_else(|| self.starting_run_ids(run))
            else {
                continue;
            };
            let at = iso(run.started_at);
            let native = format!("workspace-preparation:{}", ids.run);
            // It comes before what the run's provider did.
            let first = out
                .items
                .iter()
                .position(|(run_index, _)| *run_index == index)
                .unwrap_or(out.items.len());
            out.items.insert(
                first,
                (
                    index,
                    json!({
                        "id": item_id("turn-item", &ids.driver, &native),
                        "threadId": self.id,
                        "runId": ids.run,
                        "nodeId": ids.root_node,
                        "providerThreadId": ids.provider_thread,
                        "providerTurnId": null,
                        "nativeItemRef": null,
                        "parentItemId": null,
                        "ordinal": 0,
                        "status": "completed",
                        "title": "Workspace ready",
                        "startedAt": at,
                        "completedAt": at,
                        "updatedAt": at,
                        "type": "command_execution",
                        "input": "Preparing workspace",
                        "output": "Workspace preparation completed.",
                        "exitCode": 0
                    }),
                ),
            );
        }
        // Attempts in their runs' order, as the reference reads them.
        let position = |attempt: &Value| {
            self.runs
                .iter()
                .position(|run| attempt["runId"] == run.wire_id().as_str())
                .unwrap_or(usize::MAX)
        };
        out.attempts.sort_by_key(position);
        out.provider_sessions.extend(session);
        if let Some(id) = self.provider_thread_id() {
            let (driver, native_thread_ref, status, created_at, updated_at, provider_session_id) =
                match &self.provider.thread {
                    Some(thread) => {
                        // A thread whose session never opened keeps what it
                        // had when its first run was sent.
                        let first = self.runs.iter().find(|run| run.id == thread.first_run);
                        let unopened = thread.native_id.is_none() && thread.status == "not_loaded";
                        let (created_at, updated_at) = match first.filter(|_| unopened) {
                            Some(run) => (run.started_at, run.started_at),
                            None => (thread.created_at, thread.updated_at),
                        };
                        let session = match (&session_id, first) {
                            (Value::Null, Some(run)) => json!(pending_session_id(
                                &thread.driver,
                                &instance,
                                &self.id,
                                &run.wire_id(),
                                thread.opencode2.is_some()
                            )),
                            _ => session_id.clone(),
                        };
                        (
                            thread.driver.clone(),
                            thread.native_id.as_deref().map_or(Value::Null, |native| {
                                native_ref(&thread.driver, native, "strong")
                            }),
                            thread.status,
                            created_at,
                            updated_at,
                            session,
                        )
                    }
                    // A run waiting to start has its provider thread, not
                    // loaded until the CLI reports it.
                    None => {
                        let run = self.runs.iter().find(|run| {
                            self.starting(run)
                                || matches!(run.status.as_str(), "queued" | "preparing")
                        });
                        let driver = run
                            .and_then(|run| run.driver.clone())
                            .unwrap_or_else(|| instance.clone());
                        let at = run.map_or_else(crate::store::now, |run| run.started_at);
                        let session = json!(pending_session_id(
                            &driver,
                            &instance,
                            &self.id,
                            &run.map(Run::wire_id).unwrap_or_default(),
                            false
                        ));
                        (driver, Value::Null, "not_loaded", at, at, session)
                    }
                };
            let metadata = self.claude_native_metadata(&driver);
            // A fork's portable fallback, or a provider switch's handoff, is
            // the provider thread's handoff.
            let handoff_ids: Vec<&Value> = self
                .context_handoffs
                .iter()
                .filter(|handoff| {
                    handoff["toProviderThreadId"] == id.as_str()
                        && self.context_transfers.iter().any(|transfer| {
                            transfer["id"] == handoff["transferId"]
                                && (transfer["type"] == "fork"
                                    || transfer["id"].as_str().is_some_and(|id| {
                                        id.starts_with("context-transfer:type:provider_handoff:")
                                    }))
                        })
                })
                .map(|handoff| &handoff["id"])
                .collect();
            out.provider_threads.push(json!({
                "id": id,
                "driver": driver,
                "providerInstanceId": instance,
                "providerSessionId": provider_session_id,
                "appThreadId": self.id,
                "ownerNodeId": null,
                "nativeThreadRef": native_thread_ref,
                // The conversation's last entry (Pi), once a turn ended.
                "nativeConversationHeadRef": self
                    .provider
                    .thread
                    .as_ref()
                    .and_then(|thread| thread.conversation_head.as_deref())
                    .map_or(Value::Null, |head| {
                        // OpenCode's head is the turn's user message (weak).
                        let strength = if driver == provider::OPENCODE { "weak" } else { "strong" };
                        native_ref(&driver, head, strength)
                    }),
                "status": self.held_open_status(&driver, status),
                "firstRunOrdinal": first_ordinal,
                "lastRunOrdinal": last_ordinal,
                "handoffIds": handoff_ids,
                "forkedFrom": null,
                "pendingBackgroundTasks": self.pending_background_tasks(&driver),
                "contextUsage": self.agent_context_usage().unwrap_or_else(|| self.context_usage()),
                "nativeMetadata": metadata,
                "goal": null,
                "createdAt": iso(created_at),
                "updatedAt": iso(updated_at)
            }));
        }
        // The provider threads of instances the thread switched away from,
        // idle with their native threads.
        for thread in &self.provider.other_threads {
            let Some(first) = self.runs.iter().find(|run| run.id == thread.first_run) else {
                continue;
            };
            let instance = first
                .model_selection
                .as_deref()
                .and_then(|selection| selection["instanceId"].as_str())
                .unwrap_or(&thread.driver)
                .to_owned();
            let ordinals: Vec<usize> = self
                .runs
                .iter()
                .enumerate()
                .filter(|(_, run)| {
                    self.provider.run_threads.get(&run.id) == Some(&thread.first_run)
                })
                .map(|(index, _)| index + 1)
                .collect();
            let session = if thread.driver == provider::CLAUDE_AGENT {
                format!(
                    "provider-session:provider-instance:{}:thread:{}:{}",
                    uri_component(&instance),
                    uri_component(&self.id),
                    first.wire_id()
                )
            } else if provider::thread_session(&thread.driver) {
                acp_session_id(&instance, &self.id, &first.wire_id())
            } else {
                format!(
                    "provider-session:provider-instance:{}:shared",
                    uri_component(&instance)
                )
            };
            out.provider_threads.push(json!({
                "id": pending_thread_id(&thread.driver, &first.wire_id()),
                "driver": thread.driver,
                "providerInstanceId": instance,
                "providerSessionId": session,
                "appThreadId": self.id,
                "ownerNodeId": null,
                "nativeThreadRef": thread.native_id.as_deref().map_or(Value::Null, |native| {
                    native_ref(&thread.driver, native, "strong")
                }),
                "nativeConversationHeadRef": thread
                    .conversation_head
                    .as_deref()
                    .map_or(Value::Null, |head| native_ref(&thread.driver, head, "strong")),
                "status": thread.status,
                "firstRunOrdinal": ordinals.first(),
                "lastRunOrdinal": ordinals.last(),
                "handoffIds": [],
                "forkedFrom": null,
                "pendingBackgroundTasks": [],
                "contextUsage": null,
                "nativeMetadata": self.left_thread_metadata(thread),
                "goal": null,
                "createdAt": iso(thread.created_at),
                "updatedAt": iso(thread.updated_at)
            }));
        }
        // Provider turns by provider thread, then ordinal, as the reference
        // reads them.
        out.provider_turns.sort_by(|a, b| {
            (a["providerThreadId"].as_str(), a["ordinal"].as_u64())
                .cmp(&(b["providerThreadId"].as_str(), b["ordinal"].as_u64()))
        });
        // Provider threads in the order the thread opened them.
        out.provider_threads
            .sort_by_key(|thread| thread["firstRunOrdinal"].as_u64().unwrap_or(u64::MAX));
        out.messages
            .sort_by(|a, b| a["createdAt"].as_str().cmp(&b["createdAt"].as_str()));
        out.items
            .retain(|(_, item)| item.get("requestSlot").is_none());
        // A later proposed plan supersedes the thread's earlier ones, as the
        // reference marks the previous plan of a kind (`latestPlanByKind`).
        let proposed: Vec<usize> = out
            .plans
            .iter()
            .enumerate()
            .filter(|(_, plan)| plan["kind"] == "proposed_plan")
            .map(|(index, _)| index)
            .collect();
        if let Some((_, earlier)) = proposed.split_last() {
            for index in earlier {
                out.plans[*index]["status"] = json!("superseded");
            }
        }
        out
    }

    /// The provider thread's status once its last run ended. A Claude
    /// continuation that completed while background tasks still run is active
    /// again once the run completes: the reference's run, whose events those
    /// tasks keep it reading, stores the turn's completed provider thread
    /// (`active`) again after its own final `idle` update, before its
    /// checkpoint.
    fn held_open_status(&self, driver: &str, status: &'static str) -> &'static str {
        let holds =
            |thread: &crate::provider_state::ThreadRecord| !thread.background_tasks.is_empty();
        if status != "idle"
            || driver != provider::CLAUDE_AGENT
            || !self.provider.thread.as_ref().is_some_and(holds)
        {
            return status;
        }
        let last = self
            .runs
            .iter()
            .enumerate()
            .rev()
            .find(|(_, run)| self.provider.runs.contains_key(&run.id));
        let continued = last.is_some_and(|(index, run)| {
            // Its checkpoint's capture, or the run's end without one.
            let settled = run.status == "completed"
                || self
                    .checkpoints
                    .iter()
                    .any(|checkpoint| checkpoint.turn_count == index as u64 + 1);
            self.provider
                .terminals
                .get(&run.id)
                .is_some_and(|terminal| terminal.status == TurnStatus::Completed)
                && settled
                && run.user_message_id.as_deref().is_some_and(|id| {
                    self.messages.iter().any(|message| {
                        message.id == id && crate::thread_projection::provider_continuation(message)
                    })
                })
        });
        if continued {
            "active"
        } else {
            status
        }
    }

    /// The background tasks the provider thread runs now: as the adapter listed
    /// them, with the turn item and thread of the command that launched each
    /// (`backgroundTaskLaunch`) and the run each first appeared under, which
    /// the reference's projection stamps (`attributePendingBackgroundTasks`).
    fn pending_background_tasks(&self, driver: &str) -> Value {
        let Some(thread) = &self.provider.thread else {
            return json!([]);
        };
        let tasks: Vec<Value> = thread
            .background_tasks
            .iter()
            .map(|(task, owner)| {
                let mut task = task.clone();
                if let Some(native) = task["nativeItemId"]
                    .as_str()
                    .filter(|_| !task["command"].is_null())
                {
                    task["itemId"] = json!(item_id("turn-item", driver, native));
                    task["sourceThreadId"] = json!(self.id);
                }
                if let Some(run) = self.runs.iter().find(|run| run.id == *owner) {
                    task["runId"] = json!(run.wire_id());
                }
                task
            })
            .collect();
        json!(tasks)
    }

    /// The provider thread's context usage, as the reference sets it when a
    /// turn starts on a thread whose earlier turns reported usage: the
    /// usage they reported last on the same native thread
    /// (`latestNativeContextUsage`, `contextUsageForHandoff`).
    fn context_usage(&self) -> Value {
        let Some(last) = self
            .runs
            .iter()
            .rposition(|run| self.provider.runs.contains_key(&run.id))
        else {
            return Value::Null;
        };
        // Only turns on the native thread the turn runs on count: a thread
        // that could not be resumed starts with no usage. Claude resumes the
        // session it names up front, whatever id the CLI reports.
        let current = &self.provider.runs[&self.runs[last].id];
        // OpenCode's provider turns name no native thread, so none of them
        // is found on the turn's (`latestNativeContextUsage`).
        if current.driver == provider::OPENCODE {
            return Value::Null;
        }
        let native = current.native_thread_id.as_ref();
        // A Claude turn continues the session unless it started a new one.
        let claude_resumed = current.driver == provider::CLAUDE_AGENT
            && (current.resumed || current.native_thread_id.is_none());
        self.runs[..last]
            .iter()
            .filter_map(|run| self.provider.runs.get(&run.id))
            .filter(|record| {
                claude_resumed || (native.is_some() && record.native_thread_id.as_ref() == native)
            })
            .filter_map(|record| record.usage.as_ref())
            .max_by_key(|(_, at)| *at)
            .map(|(usage, _)| (usage.used_tokens, usage.max_tokens))
            // A thread read from the shared database: its earlier turns ran
            // on the other server.
            .or_else(|| {
                let (thread, used, max) = self.provider.prior_usage.clone()?;
                (claude_resumed || (native.is_some() && thread.as_ref() == native))
                    .then_some((used, max))
            })
            .map_or(Value::Null, |(used, max)| {
                let mut out = json!({"usedTokens": used});
                if let Some(max) = max.filter(|max| *max > 0) {
                    out["maxTokens"] = json!(max);
                }
                out
            })
    }

    /// The context usage the agent reported for its session itself (an ACP
    /// `usage_update`), which the reference keeps on the provider thread.
    fn agent_context_usage(&self) -> Option<Value> {
        let usage = self.provider.thread.as_ref()?.context_usage.as_ref()?;
        let mut out = json!({"usedTokens": usage.used_tokens});
        if let Some(max) = usage.max_tokens.filter(|max| *max > 0) {
            out["maxTokens"] = json!(max);
        }
        Some(out)
    }

    /// A Claude session's metadata: its title (the thread's) and what its
    /// prompts used of its attachment budget.
    /// The native metadata of a Claude provider thread the thread switched
    /// away from: its session's attachment budget and title.
    fn left_thread_metadata(&self, thread: &crate::provider_state::ThreadRecord) -> Value {
        if thread.driver != provider::CLAUDE_AGENT {
            return Value::Null;
        }
        let mut metadata = Map::new();
        if let Some((bytes, pages)) = thread.attachment_budget {
            metadata.insert(
                "claudeAttachmentBudget".into(),
                json!({"bytesUsed": bytes, "pagesUsed": pages}),
            );
        }
        let title = self.title.trim();
        if !title.is_empty() {
            metadata.insert("title".into(), json!(title));
        }
        if metadata.is_empty() {
            Value::Null
        } else {
            Value::Object(metadata)
        }
    }

    fn claude_native_metadata(&self, driver: &str) -> Value {
        if provider::is_acp(driver) {
            return self.acp_native_metadata();
        }
        // An OpenCode 2 session's model (`withReportedModel`).
        if let Some(opencode2) = self.opencode2() {
            return if opencode2.model_selection.is_null() {
                Value::Null
            } else {
                json!({"modelSelection": opencode2.model_selection})
            };
        }
        if driver != provider::CLAUDE_AGENT {
            return Value::Null;
        }
        let mut metadata = Map::new();
        if let Some((bytes, pages)) = self.provider.attachment_budget {
            metadata.insert(
                "claudeAttachmentBudget".into(),
                json!({"bytesUsed": bytes, "pagesUsed": pages}),
            );
        }
        let title = self
            .provider
            .session_title
            .as_deref()
            .unwrap_or(&self.title)
            .trim();
        if !title.is_empty() {
            metadata.insert("title".into(), json!(title));
        }
        if metadata.is_empty() {
            Value::Null
        } else {
            Value::Object(metadata)
        }
    }

    /// An ACP thread's native metadata: item identity version 2 once a
    /// session the server created ran on it, whose item ids are scoped to
    /// the provider instance. The thread keeps it after a restart, whose
    /// loaded session goes back to the plain ids.
    fn acp_native_metadata(&self) -> Value {
        let scoped = self.provider.runs.values().any(|run| {
            provider::is_acp(&run.driver)
                && run
                    .native_turn_id
                    .as_deref()
                    .is_some_and(|turn| turn.starts_with("provider-instance:"))
        });
        let mut metadata = Map::new();
        if scoped {
            metadata.insert("itemIdentityVersion".into(), json!(2));
        }
        // The title the agent gave its session (`SessionInfoUpdated`).
        if let Some(title) = self
            .provider
            .thread
            .as_ref()
            .and_then(|thread| thread.native_title.as_deref())
        {
            metadata.insert("title".into(), json!(title));
        }
        // When the agent says it changed its session (fx's `updatedAt`).
        if let Some(at) = self
            .provider
            .thread
            .as_ref()
            .and_then(|thread| thread.native_updated_at.as_deref())
        {
            metadata.insert("updatedAt".into(), json!(at));
        }
        if metadata.is_empty() {
            Value::Null
        } else {
            Value::Object(metadata)
        }
    }

    /// The OpenCode 2 server the thread's turns ran on, when they ran on
    /// one: its provider session is the instance's (shared by its threads),
    /// a failed turn leaves it ready, and its requests resolve as answered.
    pub(crate) fn opencode2(&self) -> Option<&provider::OpenCode2Thread> {
        self.provider
            .thread
            .as_ref()
            .filter(|thread| thread.driver == provider::OPENCODE)
            .and_then(|thread| thread.opencode2.as_ref())
    }

    /// The provider session the thread's turns ran in, once one opened: the
    /// shared app-server session of a Codex instance, or the thread's own
    /// Claude session (which the reference opens even when the CLI is
    /// missing). A Codex session whose app-server closed mid-turn is `error`.
    fn provider_session(&self, instance: &str) -> Option<Value> {
        // A detached session no longer belongs to the thread.
        if self.provider.session_detached_at.is_some() {
            return None;
        }
        self.session_row(instance)
    }

    /// [`Self::provider_session`], detached or not.
    fn session_row(&self, instance: &str) -> Option<Value> {
        let thread = self.provider.thread.as_ref()?;
        let first = self.runs.iter().find(|run| run.id == thread.first_run)?;
        let claude = thread.driver == provider::CLAUDE_AGENT;
        if !claude && thread.native_id.is_none() {
            return None;
        }
        let id = if let Some(id) = &self.provider.prior_session_id {
            id.clone()
        } else if provider::thread_session(&thread.driver) && thread.opencode2.is_none() {
            acp_session_id(instance, &self.id, &first.wire_id())
        } else if claude {
            format!(
                "provider-session:provider-instance:{}:thread:{}:{}",
                uri_component(instance),
                uri_component(&self.id),
                first.wire_id()
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
        // A Pi session ends in error with any failed turn, and stopped with
        // a stopped one (its process is stopped).
        // An OpenCode session ends in error with a failed turn too, but
        // stays ready after a stop (its server keeps running).
        // An OpenCode 2 session stays ready after a failed turn
        // (`finishTurn`), unless its event stream was lost (`failAll`).
        let pi = thread.driver == provider::PI;
        let opencode2 = thread.opencode2.as_ref();
        let opencode = thread.driver == provider::OPENCODE && opencode2.is_none();
        let crash = last.and_then(|t| t.failure.as_ref()).filter(|failure| {
            pi || opencode
                || (opencode2.is_some() && failure.message == provider::OPENCODE2_STREAM_LOST)
                || (!claude && provider::stream_closed(&failure.message))
        });
        let crashed = crash.is_some();
        // A restart stops a session that has not failed.
        let stopped = self
            .provider
            .session_stopped_at
            .filter(|_| !crashed)
            .or_else(|| {
                last.filter(|t| pi && t.status == TurnStatus::Interrupted)
                    .map(|_| thread.updated_at)
            });
        let capabilities: Value =
            serde_json::from_str(include_str!("provider_capabilities.json")).unwrap_or_default();
        let capabilities = match opencode2 {
            // OpenCode 2's own, with native goals from 2.2.5 on.
            Some(opencode2) => {
                let mut capabilities = capabilities["opencode2"].clone();
                capabilities["turns"]["supportsNativeGoals"] =
                    json!(provider::version_at_least(&opencode2.version, (2, 2, 5)));
                capabilities
            }
            None => session_capabilities(&capabilities, &thread.driver),
        };
        let row = self.stored_row();
        Some(json!({
            "capabilities": capabilities,
            "createdAt": iso(thread.created_at),
            "cwd": self.cwd,
            "driver": thread.driver,
            "extensionStatuses": [],
            "id": id,
            "lastError": crash.map(|failure| json!(failure.message)).unwrap_or(Value::Null),
            // The model of the latest run sent on it.
            "model": self
                .runs
                .iter()
                .rev()
                .find(|run| self.provider.runs.contains_key(&run.id) || self.starting(run))
                .and_then(|run| run.model_selection.as_deref())
                .map_or(&row["modelSelection"]["model"], |selection| &selection["model"]),
            "providerInstanceId": instance,
            "status": if crashed { "error" } else if stopped.is_some() { "stopped" } else { "ready" },
            "updatedAt": iso(if crashed { thread.updated_at } else { stopped.unwrap_or(thread.created_at) })
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
        // A turn a restart cut off before it ended is cancelled then.
        let cut = run
            .restart_cut
            .as_ref()
            .filter(|_| terminal.is_none())
            .map(|cut| cut.at);
        let ended = terminal
            .map(|_| record.ended_at.unwrap_or(record.started_at))
            .or(cut);
        let status = match terminal.map(|terminal| terminal.status) {
            None if cut.is_some() => "cancelled",
            None | Some(TurnStatus::Running) => "running",
            Some(TurnStatus::Completed) => "completed",
            Some(TurnStatus::Interrupted) => "interrupted",
            Some(TurnStatus::Failed) => "failed",
        };
        // A run a restart put back in the queue waits to start again.
        // So does one whose scheduled prompt a restart owes it again.
        let requeued = (run.status == "queued" && run.requeued_at.is_some()) || run.resend_owed;
        let status = if requeued { "pending" } else { status };
        let started = iso(record.started_at);
        let completed = ended.map(iso);
        let started_value = if requeued {
            Value::Null
        } else {
            json!(started)
        };
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
            "startedAt": started_value,
            "completedAt": completed
        }));
        // `nativeThreadId` is optional in the contract: absent, never null.
        if let Some(native) = self
            .provider
            .thread
            .as_ref()
            .and_then(|t| t.native_id.clone())
        {
            if let Some(attempt) = out.attempts.last_mut() {
                attempt["nativeThreadId"] = json!(native);
            }
        }
        // Once its turn ends, the root node goes with the run (`waiting`
        // until a completed turn's checkpoint completes both).
        let (root_status, root_completed) = match terminal {
            Some(_) if !requeued => {
                let (status, end) = self.projected_run_status(run);
                (status, end.map(iso).map_or(Value::Null, Value::from))
            }
            _ => (status, json!(completed)),
        };
        out.nodes.push(json!({
            "id": ids.root_node,
            "threadId": self.id,
            "runId": ids.run,
            "parentNodeId": null,
            "rootNodeId": ids.root_node,
            "kind": "root_turn",
            "status": root_status,
            "countsForRun": true,
            "providerThreadId": ids.provider_thread,
            "providerTurnId": null,
            "nativeItemRef": null,
            "runtimeRequestId": null,
            "checkpointScopeId": null,
            "startedAt": started_value,
            "completedAt": root_completed
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
        // A Codex app-server that closed mid-turn never reports the turn's
        // end: the reference fails the session and the run, and leaves the
        // provider turn running.
        let crashed = driver == provider::CODEX
            && terminal
                .and_then(|terminal| terminal.failure.as_ref())
                .is_some_and(|failure| provider::stream_closed(&failure.message));
        let mut turn = json!({
            "id": ids.provider_turn,
            "providerThreadId": ids.provider_thread,
            "nodeId": ids.root_node,
            "runAttemptId": ids.attempt,
            "nativeTurnRef": native_ref(driver, &native_turn, strength),
            "ordinal": turn_ordinal,
            "status": if crashed { "running" } else { status },
            "startedAt": started,
            "completedAt": if crashed { None } else { completed.clone() }
        });
        // A turn a session detach stopped ends interrupted, whatever the
        // closed stream made of its run, as the reference closes it.
        let detached = self.provider.detached_runs.contains(&run.id);
        if detached && terminal.is_some() {
            turn["status"] = json!("interrupted");
        }
        // Pi's turn is named by its provider thread and attempt (weak) until
        // its end names its first user entry (strong), or none; its records
        // also carry the session file.
        if driver == provider::OPENCODE {
            // OpenCode's turn is named by its session and attempt until its
            // user message is seen (both weak).
            turn["nativeTurnRef"] = record
                .native_turn_ref
                .clone()
                .unwrap_or_else(|| native_ref(driver, &native_turn, "weak"));
        }
        if driver == provider::PI {
            turn["nativeTurnRef"] = record
                .native_turn_ref
                .clone()
                .unwrap_or_else(|| native_ref(driver, &native_turn, "weak"));
            if let Some(file) = &record.native_thread_id {
                turn["nativeThreadRef"] = native_ref(driver, file, "strong");
            }
        }
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
            // Pi reports no reasoning tokens.
            if driver == provider::PI {
                if let Some(usage) = turn["tokenUsage"].as_object_mut() {
                    usage.remove("reasoningOutputTokens");
                }
            }
            if let Some(ttl) = usage.prompt_cache_ttl_ms {
                turn["tokenUsage"]["promptCacheTtlMs"] = json!(ttl);
            }
            if let Some(expires) = &usage.prompt_cache_expires_at {
                turn["tokenUsage"]["promptCacheExpiresAt"] = json!(expires);
            }
        }
        // A Claude turn whose process a detach stopped never reported its
        // whole-turn usage; a Codex turn interrupted in its shared session did.
        let unreported = detached && record.driver == provider::CLAUDE_AGENT;
        if let Some(usage) = record.turn_usage.as_ref().filter(|_| !unreported) {
            turn["turnTokenUsage"] = usage.clone();
        }
        if let Some(model) = &record.model {
            turn["model"] = json!(model);
        }
        // The reference records a provider turn once the CLI starts one.
        if record.turn_started || record.native_turn_id.is_some() || !record.items.is_empty() {
            out.provider_turns.push(turn);
        }

        // A stop or interrupt's request card, in the slot it took when it
        // was asked for.
        let request = join("turn-item:run", &[&ids.run]) + ":signal:interrupt-request";
        let requested = self.provider.interrupt_requests.get(&run.id).copied();
        // Why it was asked to end, which both cards say.
        let reason = self.provider.interrupt_reasons.get(&run.id).cloned();
        let request_card = |at: u64| {
            let mut card = self.run_signal(
                ids,
                request.clone(),
                Value::Null,
                "completed",
                "Interrupt requested",
                "run_interrupt_request",
                &iso(at),
            );
            if let Some(reason) = &reason {
                card["message"] = json!(reason);
            }
            card
        };
        let at = completed.clone().unwrap_or_else(|| started.clone());
        // Whether a retry the turn failed in became its failure row.
        let mut failure_placed = false;
        for (position, item) in record.items.iter().enumerate() {
            if let Some((at, _)) = requested.filter(|(_, slot)| *slot == position) {
                out.items.push((index, request_card(at)));
            }
            if item.item.kind == "error" && item.item.fields.contains_key("retry") {
                let row = self.retry_item(ids, item, terminal, cut, &at);
                failure_placed |= row["status"] == "failed";
                out.items.push((index, row));
                continue;
            }
            if matches!(
                item.item.kind.as_str(),
                "approval_request" | "user_input_request"
            ) {
                // Placed by provider_request, in this slot.
                out.items
                    .push((index, json!({"requestSlot": item.item.id})));
                continue;
            }
            self.provider_item(
                out,
                index,
                ids,
                item,
                terminal.map(|t| t.status),
                cut.or_else(|| {
                    self.provider
                        .cancelled_items
                        .get(&(run.id, item.item.id.clone()))
                        .copied()
                }),
            );
        }
        if let Some((at, _)) = requested.filter(|(_, slot)| *slot >= record.items.len()) {
            out.items.push((index, request_card(at)));
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

        if let Some(restart) = &run.restart_cut {
            out.items
                .push((index, self.restart_notice(run, restart, Some(ids))));
        }
        let Some(terminal) = terminal else { return };
        match terminal.status {
            // A turn whose folder is missing, or a Codex or Grok turn
            // whose process never opened a session: the reference reports
            // the session, not the turn, as failed.
            TurnStatus::Failed
                if terminal
                    .failure
                    .as_ref()
                    .is_some_and(provider::is_workspace_missing)
                    || ((driver == provider::CODEX || provider::thread_session(driver))
                        && !record.turn_started
                        && self
                            .provider
                            .thread
                            .as_ref()
                            .is_some_and(|thread| thread.native_id.is_none())) =>
            {
                let session = if provider::thread_session(driver) && self.opencode2().is_none() {
                    acp_session_id(instance, &self.id, &ids.run)
                } else {
                    join("provider-session:provider-instance", &[instance]) + ":shared"
                };
                let message = match terminal.failure.as_ref() {
                    Some(failure) if provider::is_workspace_missing(failure) => {
                        failure.message.clone()
                    }
                    // Antigravity's driver fails to open with its setup
                    // error (`ProviderSetupError`), which the item says.
                    Some(failure) if driver == provider::ANTIGRAVITY => failure.message.clone(),
                    _ => format!("Failed to open {driver} provider session {session}."),
                };
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
                        "message": message,
                        "code": null,
                        "retryable": null
                    }
                })));
            }
            // A retry the turn failed in already shows its failure.
            TurnStatus::Failed if failure_placed => {}
            TurnStatus::Failed => {
                let failure = terminal.failure.clone().unwrap_or_else(|| {
                    provider::ProviderFailure::new("unknown", "Provider turn failed.")
                });
                // A turn that recovered from retries fails in a row of its
                // own, after them (`codexFailureItemKey`).
                let recovered = record
                    .items
                    .iter()
                    .filter(|item| {
                        item.item.kind == "error"
                            && item.item.fields.contains_key("retry")
                            && item.item.status == "completed"
                    })
                    .count();
                let mut native = format!("terminal-failure:{}", ids.provider_turn);
                if recovered > 0 {
                    native = format!("{native}:{recovered}");
                }
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
                    "failure": failure_json(&failure)
                })));
            }
            TurnStatus::Interrupted => {
                // A detach stopped it: the provider, not the user, did.
                let detached = self.provider.detached_runs.contains(&run.id);
                // A turn the provider interrupted without a request still
                // shows one, as it always has here.
                if !detached && requested.is_none() {
                    out.items.push((
                        index,
                        self.run_signal(
                            ids,
                            request.clone(),
                            Value::Null,
                            "completed",
                            "Interrupt requested",
                            "run_interrupt_request",
                            &at,
                        ),
                    ));
                }
                let mut result = self.run_signal(
                    ids,
                    join("turn-item:run", &[&ids.run]) + ":signal:interrupt-result",
                    json!(request),
                    "interrupted",
                    "Interrupted",
                    "run_interrupt_result",
                    &at,
                );
                // The request's reason, else the provider stopped it
                // (`makeInterruptResultTurnItem`).
                result["message"] = match reason.as_deref().filter(|_| !detached) {
                    Some(reason) => json!(reason),
                    None => json!("Stopped by the provider"),
                };
                out.items.push((index, result));
            }
            TurnStatus::Completed | TurnStatus::Running => {}
        }
    }

    /// A provider retry item (the reference's makeProviderRetryTurnItem):
    /// running while the provider retries, then recovered, stopped, or the
    /// turn's failure row when the turn failed in it
    /// (makeProviderFailureTurnItem with its `retry`).
    fn retry_item(
        &self,
        ids: &RunIds,
        record: &ItemRecord,
        terminal: Option<&Terminal>,
        cut: Option<u64>,
        ended_at: &str,
    ) -> Value {
        let driver = ids.driver.as_str();
        let item = &record.item;
        // `terminal-failure`, or `terminal-failure:N` after N recoveries.
        let suffix = item
            .id
            .strip_prefix(provider::PROVIDER_RETRY_ITEM)
            .unwrap_or_default();
        let native = format!("terminal-failure:{}{suffix}", ids.provider_turn);
        let open = matches!(item.status.as_str(), "running" | "failed");
        let ended = terminal.map(|terminal| terminal.status);
        let status = match (item.status.as_str(), ended) {
            (_, Some(TurnStatus::Failed)) if open => "failed",
            ("running", _) if cut.is_some() => "cancelled",
            ("running", Some(TurnStatus::Interrupted)) => "interrupted",
            ("running", Some(TurnStatus::Completed)) => "completed",
            (status, _) => status,
        };
        let mut failure = item.fields.get("failure").cloned().unwrap_or(Value::Null);
        let started = iso(record.first_at);
        let mut updated = iso(cut
            .filter(|_| status == "cancelled")
            .unwrap_or(record.updated_at));
        let title = match status {
            "running" => "Provider retry",
            "completed" => "Provider recovered",
            "failed" => {
                // The turn's failure, as its end reported it.
                if let Some(turn_failure) = terminal.and_then(|t| t.failure.as_ref()) {
                    failure = failure_json(turn_failure);
                }
                updated = ended_at.to_owned();
                if failure["class"] == "usage_limit" {
                    "Usage limit reached"
                } else {
                    "Provider error"
                }
            }
            _ => "Provider retry stopped",
        };
        json!({
            "id": item_id("turn-item", driver, &native),
            "threadId": self.id,
            "runId": ids.run,
            "nodeId": ids.root_node,
            "providerThreadId": ids.provider_thread,
            "providerTurnId": ids.provider_turn,
            "nativeItemRef": null,
            "parentItemId": null,
            "ordinal": 0,
            "status": status,
            "title": title,
            "startedAt": started,
            "completedAt": if status == "running" { Value::Null } else { json!(updated) },
            "updatedAt": updated,
            "type": "error",
            "failure": failure,
            "retry": item.fields.get("retry").cloned().unwrap_or(Value::Null)
        })
    }

    /// A run's interrupt request or result card.
    #[allow(clippy::too_many_arguments)]
    fn run_signal(
        &self,
        ids: &RunIds,
        id: String,
        parent: Value,
        status: &str,
        title: &str,
        kind: &str,
        at: &str,
    ) -> Value {
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
    }

    /// The notice the reference adds to a run a restart cut off
    /// (`server-recovery`), after the run's items.
    pub(crate) fn restart_notice(
        &self,
        run: &Run,
        restart: &crate::RestartCut,
        ids: Option<&RunIds>,
    ) -> Value {
        let at = iso(restart.at);
        let startup = restart.trigger == "startup";
        json!({
            "id": join("turn-item:run", &[&run.wire_id()]) + ":signal:server-recovery",
            "threadId": self.id,
            "runId": run.wire_id(),
            "nodeId": ids.map(|ids| ids.root_node.clone()),
            "providerThreadId": ids.map(|ids| ids.provider_thread.clone()),
            "providerTurnId": null,
            "nativeItemRef": null,
            "parentItemId": null,
            "ordinal": 0,
            "status": "completed",
            "title": if startup { "Server restarted" } else { "Server shut down" },
            "startedAt": at,
            "completedAt": at,
            "updatedAt": at,
            "type": "system_notice",
            "message": format!(
                "Cancelled because the server {} before the provider work completed.",
                if startup { "restarted" } else { "shut down" }
            )
        })
    }

    /// A runtime request's record, node and turn item, as the reference's
    /// adapters build them (`buildApprovalRequestArtifacts`,
    /// `buildUserInputRequestArtifacts`) and its respond command updates
    /// them.
    fn provider_request(
        &self,
        out: &mut ProviderRecords,
        index: usize,
        run: &Run,
        ids: &RunIds,
        request: &RequestRecord,
        session_id: &Value,
    ) {
        let driver = ids.driver.as_str();
        let open = &request.request;
        let id = self.request_id(ids, request);
        let state = self.request_state(run, request, session_id);
        let question = open.kind == "user_input";
        // Codex questions are items of their tool call; everything else
        // hangs under the call it asks about.
        let codex_question = question && driver == provider::CODEX;
        let (item_row_id, node_id, parent) = if codex_question {
            (
                item_id("turn-item", driver, &open.native_item_id),
                item_id("node", driver, &open.native_item_id),
                ids.root_node.clone(),
            )
        } else {
            (
                join("turn-item:runtime-request", &[&id]),
                join("node:runtime-request", &[&id]),
                item_id("node", driver, &open.native_item_id),
            )
        };
        let native_item = if driver == provider::CODEX {
            &open.native_item_id
        } else {
            &open.native_request_id
        };
        // ACP agents' request ids are not their own (`approvalsHaveNativeRequestIds`).
        let strength = if provider::is_acp(driver) {
            "weak"
        } else {
            "strong"
        };
        let reference = native_ref(driver, native_item, strength);
        // The node and item end with the answer: completed when accepted or
        // answered, cancelled when declined, withdrawn or expired.
        let done = match state.status {
            // Pi's adapter resolves its own dialogs: answered is completed.
            "resolved" => Some(
                if !(provider::resolves_own_requests(driver) && self.opencode2().is_none())
                    && matches!(request.decision.as_deref(), Some("decline" | "cancel"))
                {
                    "cancelled"
                } else {
                    "completed"
                },
            ),
            "cancelled" | "expired" => Some("cancelled"),
            _ => None,
        };
        let status = done.unwrap_or("waiting");
        let created = iso(request.created_at);
        let completed = done.and(state.resolved_at).map(iso);
        let updated = completed.clone().unwrap_or_else(|| created.clone());
        let mut record = json!({
            "id": id,
            "nodeId": node_id,
            "providerTurnId": ids.provider_turn,
            "nativeRequestRef": native_ref(driver, &open.native_request_id, strength),
            "kind": open.kind,
            "status": state.status,
            "responseCapability": state.capability,
            "createdAt": created,
            "resolvedAt": state.resolved_at.map(iso)
        });
        // Pi's resolution records no decision.
        if state.status == "resolved"
            && !(provider::resolves_own_requests(driver) && self.opencode2().is_none())
        {
            for (key, value) in [
                ("decision", &request.decision),
                ("feedback", &request.feedback),
            ] {
                if let Some(value) = value {
                    record[key] = json!(value);
                }
            }
            if let Some(answers) = &request.answers {
                record["answers"] = answers.clone();
            }
        }
        out.runtime_requests.push(record);
        out.nodes.push(json!({
            "id": node_id,
            "threadId": self.id,
            "runId": ids.run,
            "parentNodeId": parent,
            "rootNodeId": ids.root_node,
            "kind": if question { "user_input_request" } else { "approval_request" },
            "status": status,
            "countsForRun": false,
            "providerThreadId": ids.provider_thread,
            "providerTurnId": ids.provider_turn,
            "nativeItemRef": reference,
            "runtimeRequestId": id,
            "checkpointScopeId": null,
            "startedAt": created,
            "completedAt": completed
        }));
        let mut item = Map::new();
        for (key, value) in [
            ("id", json!(item_row_id)),
            ("threadId", json!(self.id)),
            ("runId", json!(ids.run)),
            ("nodeId", json!(node_id)),
            ("providerThreadId", json!(ids.provider_thread)),
            ("providerTurnId", json!(ids.provider_turn)),
            ("nativeItemRef", reference),
            ("parentItemId", Value::Null),
            ("ordinal", json!(0)),
            ("status", json!(status)),
            ("title", Value::Null),
            ("startedAt", json!(created)),
            ("completedAt", json!(completed)),
            ("updatedAt", json!(updated)),
            (
                "type",
                json!(if question {
                    "user_input_request"
                } else {
                    "approval_request"
                }),
            ),
            ("requestId", json!(id)),
        ] {
            item.insert(key.into(), value);
        }
        if !question {
            item.insert("requestKind".into(), json!(open.kind));
        }
        for (key, value) in &open.fields {
            item.insert(key.clone(), value.clone());
        }
        if let (true, "resolved", Some(answers)) = (question, state.status, &request.answers) {
            let texts: Map<String, Value> = open.fields["questions"]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(|q| Some((q["id"].as_str()?.to_owned(), q["question"].clone())))
                .collect();
            item.insert(
                "questionAnswer".into(),
                json!({"requestId": id, "answers": answers, "attachmentsByQuestionId": {},
                    "questionTextById": texts}),
            );
        }
        if let Some(at) = request.delivery_failed_at {
            let at = iso(at);
            let failure = json!({
                "id": format!("turn-item:runtime-request-delivery-failure:{id}"),
                "threadId": self.id,
                "runId": ids.run,
                "nodeId": node_id,
                "providerThreadId": ids.provider_thread,
                "providerTurnId": ids.provider_turn,
                "nativeItemRef": null,
                "parentItemId": null,
                "ordinal": 0,
                "status": "failed",
                "title": "Answer delivery not confirmed",
                "startedAt": at,
                "completedAt": at,
                "updatedAt": at,
                "type": "error",
                "failure": {"class": "transport_error", "message": DELIVERY_FAILED, "code": null, "retryable": null}
            });
            let slot = format!("delivery:{}", open.id);
            if let Some(existing) = out
                .items
                .iter_mut()
                .find(|(_, existing)| existing["requestSlot"] == slot.as_str())
            {
                existing.1 = failure;
            }
        }
        // Into the slot the request took among its run's items.
        let slot = format!("request:{}", open.id);
        let row = Value::Object(item);
        match out
            .items
            .iter_mut()
            .find(|(_, existing)| existing["requestSlot"] == slot.as_str())
        {
            Some(existing) => existing.1 = row,
            None => out.items.push((index, row)),
        }
    }

    /// A native subagent's turn item, as the reference's adapter writes it
    /// (`emitSubagentTaskUpdate`), with its subagent record and the child's
    /// provider thread (`registerSubagentThread`): the item names the
    /// subagent (its node), the child's app thread and provider thread.
    fn native_subagent(
        &self,
        out: &mut ProviderRecords,
        ids: &RunIds,
        row: &mut Map<String, Value>,
        node: &str,
        reference: &Value,
    ) {
        let driver = ids.driver.as_str();
        let instance = self.instance_id();
        let native_thread = row
            .remove("nativeThreadId")
            .and_then(|v| v.as_str().map(str::to_owned))
            .unwrap_or_default();
        let model = row.remove("model").unwrap_or(Value::Null);
        let effort = row.remove("reasoningEffort");
        let child_provider_thread = join(
            "provider-thread:provider",
            &[driver, "native-thread", &native_thread],
        );
        row.insert("subagentId".into(), json!(node));
        row.insert("driver".into(), json!(driver));
        row.insert("providerInstanceId".into(), json!(instance));
        row.insert("providerThreadId".into(), json!(child_provider_thread));
        let mut record = json!({
            "id": node,
            "threadId": self.id,
            "runId": ids.run,
            "parentNodeId": ids.root_node,
            "origin": "provider_native",
            "createdBy": "agent",
            "driver": driver,
            "providerInstanceId": instance,
            "providerThreadId": child_provider_thread,
            "childThreadId": row["childThreadId"],
            "nativeTaskRef": reference,
            "prompt": row["prompt"],
            "title": row["title"],
            "model": model,
            "status": row["status"],
            "result": row["result"],
            "startedAt": row["startedAt"],
            "completedAt": row["completedAt"],
            "updatedAt": row["updatedAt"],
        });
        // The model Codex was asked to run it on, with its effort.
        if let Some(model) = model.as_str() {
            let mut selection = json!({"instanceId": instance, "model": model});
            if let Some(effort) = effort.as_ref().and_then(Value::as_str) {
                selection["options"] = json!([{"id": "reasoningEffort", "value": effort}]);
            }
            record["modelSelection"] = selection;
        }
        out.subagents.push(record);
        if !out
            .provider_threads
            .iter()
            .any(|t| t["id"] == child_provider_thread.as_str())
        {
            out.provider_threads.push(json!({
                "id": child_provider_thread,
                "driver": driver,
                "providerInstanceId": instance,
                "providerSessionId": self.session_row(&instance).map_or(Value::Null, |s| s["id"].clone()),
                "appThreadId": row["childThreadId"],
                "ownerNodeId": null,
                "nativeThreadRef": native_ref(driver, &native_thread, "strong"),
                "nativeConversationHeadRef": null,
                "status": "idle",
                "firstRunOrdinal": null,
                "lastRunOrdinal": null,
                "handoffIds": [],
                "forkedFrom": {"providerThreadId": ids.provider_thread, "providerTurnId": ids.provider_turn},
                "pendingBackgroundTasks": [],
                "contextUsage": null,
                "nativeMetadata": null,
                "goal": null,
                "createdAt": row["startedAt"],
                "updatedAt": row["startedAt"],
            }));
        }
    }

    fn provider_item(
        &self,
        out: &mut ProviderRecords,
        index: usize,
        ids: &RunIds,
        record: &ItemRecord,
        ended: Option<TurnStatus>,
        cut: Option<u64>,
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
        // An item still running when its turn ended ends with the turn, but
        // a Codex command can run on after a completed turn, as background
        // work, until a restart cancels it.
        let background = driver == provider::CODEX && item.kind == "command_execution"
            // A native subagent keeps the status its own calls report.
            || item.kind == "subagent";
        let status = match (item.status.as_str(), ended) {
            ("pending" | "running" | "waiting", _) if cut.is_some() => "cancelled",
            ("running", Some(TurnStatus::Interrupted)) => "interrupted",
            ("running", Some(TurnStatus::Failed)) => "failed",
            ("running", Some(TurnStatus::Completed)) if !background => "completed",
            (status, _) => status,
        }
        .to_owned();
        let running = status == "running";
        // The reference's adapters report an assistant message only once it
        // has text, and a Codex file change only once it ends. Such an item
        // keeps its place among the turn's items while it is not shown.
        let empty = item
            .fields
            .get("text")
            .and_then(Value::as_str)
            .is_none_or(str::is_empty);
        if running
            && ((item.kind == "assistant_message" && empty)
                || (driver == provider::CODEX && item.kind == "file_change"))
        {
            out.items.push((index, json!({ HIDDEN_ITEM: true })));
            return;
        }
        let node = item_id("node", driver, native);
        let plan = item.kind == "proposed_plan";
        // An ACP item names its native ref itself: its session's tool call
        // (strong) or its turn's text segment (weak).
        let reference = item
            .fields
            .get("nativeItemRef")
            .cloned()
            .unwrap_or_else(|| native_ref(driver, native, "strong"));
        let started = iso(record.first_at);
        // An item a restart cancelled changed then.
        let cancelled_at = cut.filter(|_| status == "cancelled" && item.status != "cancelled");
        let updated = iso(cancelled_at.unwrap_or(record.updated_at));
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
        let native_subagent =
            item.kind == "subagent" && item.fields.get("origin") == Some(&json!("provider_native"));
        if native_subagent {
            self.native_subagent(out, ids, &mut row, &node, &reference);
        }
        // A proposed plan (Claude's ExitPlanMode) is a plan artifact as well
        // as its turn item (`emitClaudePlanProjection`): its id is the
        // reference's random plan id, made stable from the native item.
        if plan {
            let digest = {
                use sha2::Digest;
                sha2::Sha256::digest(format!("{}:{native}", self.id).as_bytes())
            };
            let mut bytes = [0u8; 16];
            bytes.copy_from_slice(&digest[..16]);
            let plan_id = format!(
                "plan:thread:{}:provider:{}:run:{}:{}",
                uri_component(&self.id),
                uri_component(driver),
                uri_component(&ids.run),
                uuid::Builder::from_random_bytes(bytes).into_uuid()
            );
            row.insert("planId".into(), json!(plan_id));
            row.insert("title".into(), Value::Null);
            out.plans.push(json!({
                "id": plan_id,
                "threadId": self.id,
                "runId": ids.run,
                "nodeId": node,
                "kind": "proposed_plan",
                "status": "active",
                "markdown": item.fields.get("markdown").cloned().unwrap_or(json!("")),
            }));
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
                "proposed_plan" => "plan",
                "subagent" if native_subagent => "subagent",
                _ => "tool_call",
            },
            // A pending tool call's node is running.
            "status": if status == "pending" { "running" } else { status.as_str() },
            // An ACP tool call counts toward its run.
            "countsForRun": provider::is_acp(driver)
                && !matches!(item.kind.as_str(), "assistant_message" | "reasoning"),
            "providerThreadId": if native_subagent { row["providerThreadId"].clone() } else { json!(ids.provider_thread) },
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

#[cfg(test)]
mod tests {
    use crate::workspace::provider::{ProviderEvent, RuntimeRequestOpen, TurnStatus};
    use crate::{Run, Thread};
    use serde_json::{json, Map, Value};
    use uuid::Uuid;

    fn thread(run: Run) -> Thread {
        Thread {
            row: json!({"modelSelection": {"instanceId": "codex", "model": "m"}}),
            id: "t1".into(),
            project_id: "p1".into(),
            title: "T".into(),
            cwd: "/w".into(),
            status: "running".into(),
            messages: vec![],
            runs: vec![run],
            created_at: 1,
            archived: false,
            settled: false,
            snoozed_until: None,
            pinned: false,
            unread: false,
            tags: vec![],
            checkpoints: vec![],
            link_requests: Vec::new(),
            context_handoffs: Vec::new(),
            context_transfers: Vec::new(),
            handoff_items: Vec::new(),
            inherited_items: Vec::new(),
            provider: Box::default(),
            stored: None,
        }
    }

    fn opened(id: &str, kind: &str) -> ProviderEvent {
        let mut fields = Map::new();
        fields.insert("prompt".into(), json!("touch x"));
        ProviderEvent::RuntimeRequestOpened {
            request: RuntimeRequestOpen {
                id: id.into(),
                native_request_id: format!("call_{id}"),
                native_item_id: format!("call_{id}"),
                kind: kind.into(),
                runtime_mode: "full-access".into(),
                supports_feedback: false,
                fields,
            },
        }
    }

    fn requests(thread: &Thread) -> Vec<Value> {
        thread.provider_records().runtime_requests
    }

    #[test]
    fn requests_wait_resolve_and_expire_as_the_reference_records_them() {
        let run_id = Uuid::new_v4();
        let mut run = Run {
            id: run_id,
            thread_id: "t1".into(),
            provider: "codex".into(),
            status: "running".into(),
            started_at: 1,
            finished_at: None,
            error: None,
            user_message_id: None,
            queue_held: false,
            restart_cut: None,
            restart_continuation_of: None,
            requeued_at: None,
            background_work: vec![],
            continuation: None,
            resend_owed: false,
            resends: 0,
            wire_id: None,
            driver: None,
            model_selection: None,
            output_schema: None,
            fresh_context: false,
        };
        let mut live = thread(run.clone());
        for event in [
            ProviderEvent::ProviderThread {
                driver: "codex".into(),
                native_thread_id: "thr".into(),
                resumed: false,
                model: None,

                opencode2: None,
                native_title: None,
                native_updated_at: None,
                context_usage: None,
            },
            ProviderEvent::ProviderTurn {
                native_turn_id: Some("turn".into()),
                native_turn_ref: None,
                conversation_head: None,
                status: TurnStatus::Running,
                model: None,
                history_injected: false,
            },
            opened("a", "command"),
            opened("q", "user_input"),
        ] {
            live.provider.apply(run_id, &event, 2);
        }
        let pending = requests(&live);
        assert_eq!(pending.len(), 2);
        assert_eq!(pending[0]["status"], "pending");
        assert_eq!(
            pending[0]["id"],
            "runtime-request:provider:codex:provider-turn:provider-turn%3Aprovider%3Acodex%3Anative-turn%3Aturn:native-request:call_a:a"
        );
        assert_eq!(pending[0]["responseCapability"]["type"], "live");
        // An approval comes before a question in the shell's summary.
        assert_eq!(live.shell()["pendingRuntimeRequest"]["kind"], "command");
        let items = live.provider_records().items;
        let approval = items
            .iter()
            .find(|(_, item)| item["type"] == "approval_request")
            .unwrap();
        assert_eq!(approval.1["status"], "waiting");
        assert_eq!(approval.1["prompt"], "touch x");

        // A restart: the approval expires and the question waits for a message.
        run.status = "failed".into();
        run.finished_at = Some(5);
        run.error = Some(crate::turn::RESTART_INTERRUPTION.into());
        let mut restarted = thread(run);
        restarted.provider = live.provider.clone();
        let after = requests(&restarted);
        assert_eq!(after[0]["status"], "expired");
        assert_eq!(after[0]["responseCapability"]["type"], "not_resumable");
        assert_eq!(after[1]["status"], "pending");
        assert_eq!(after[1]["responseCapability"], json!({"type": "message"}));

        // An answer resolves the request and completes or cancels its card.
        live.provider.apply(
            run_id,
            &ProviderEvent::RuntimeRequestAnswered {
                id: "a".into(),
                decision: Some("decline".into()),
                feedback: None,
                answers: None,
            },
            3,
        );
        let answered = requests(&live);
        assert_eq!(answered[0]["status"], "resolved");
        assert_eq!(answered[0]["decision"], "decline");
        let items = live.provider_records().items;
        let approval = items
            .iter()
            .find(|(_, item)| item["type"] == "approval_request")
            .unwrap();
        assert_eq!(approval.1["status"], "cancelled");
        assert_eq!(live.shell()["pendingRuntimeRequest"]["kind"], "user_input");
    }

    fn item(id: &str, kind: &str, status: &str, text: Option<&str>) -> ProviderEvent {
        let mut fields = Map::new();
        if let Some(text) = text {
            fields.insert("text".into(), json!(text));
        }
        ProviderEvent::TurnItem {
            item: crate::workspace::provider::ProviderItem {
                id: id.into(),
                kind: kind.into(),
                status: status.into(),
                title: None,
                fields,
            },
        }
    }

    #[test]
    fn items_the_reference_does_not_report_yet_keep_their_place_unshown() {
        let run_id = Uuid::new_v4();
        let mut live = thread(
            serde_json::from_value(json!({
                "id": run_id, "thread_id": "t1", "provider": "codex", "status": "running",
                "started_at": 1, "finished_at": null, "error": null, "user_message_id": null
            }))
            .unwrap(),
        );
        let shown = |live: &Thread| -> Vec<(String, u64)> {
            live.projection()["turnItems"]
                .as_array()
                .unwrap()
                .iter()
                .map(|item| {
                    (
                        item["type"].as_str().unwrap().to_owned(),
                        item["ordinal"].as_u64().unwrap(),
                    )
                })
                .collect()
        };
        for event in [
            ProviderEvent::ProviderThread {
                driver: "codex".into(),
                native_thread_id: "thr".into(),
                resumed: false,
                model: None,

                opencode2: None,
                native_title: None,
                native_updated_at: None,
                context_usage: None,
            },
            item("msg", "assistant_message", "running", Some("")),
            item("patch", "file_change", "running", None),
            item("cmd", "command_execution", "running", None),
        ] {
            live.provider.apply(run_id, &event, 2);
        }
        // An assistant message without text and a running Codex file change
        // are not shown, but take their slots.
        assert_eq!(shown(&live), [("command_execution".into(), 1_000_003)]);
        assert!(live.projection()["messages"].as_array().unwrap().is_empty());
        for event in [
            ProviderEvent::TurnItemDelta {
                held: false,
                item_id: "msg".into(),
                delta: "Hi".into(),
            },
            item("patch", "file_change", "completed", None),
        ] {
            live.provider.apply(run_id, &event, 3);
        }
        assert_eq!(
            shown(&live),
            [
                ("assistant_message".into(), 1_000_001),
                ("file_change".into(), 1_000_002),
                ("command_execution".into(), 1_000_003)
            ]
        );
    }

    fn terminal(status: TurnStatus) -> ProviderEvent {
        ProviderEvent::TurnTerminal {
            status,
            failure: None,
            thread_disposition: crate::workspace::provider::ThreadDisposition::Reusable,
            driver: None,
            closing: Vec::new(),
        }
    }

    #[test]
    fn a_claude_thread_lists_its_background_tasks_and_a_continuation_leaves_it_active() {
        let claude_run = |status: &str, message: &str| {
            let mut run = running_codex_thread(Uuid::new_v4()).runs.remove(0);
            run.provider = "claudeAgent".into();
            run.status = status.into();
            run.user_message_id = Some(message.into());
            run.wire_id = Some(format!("run:{message}"));
            run
        };
        let first = claude_run("completed", "m1");
        let wake = claude_run("running", "m2");
        let mut live = thread(first.clone());
        live.runs.push(wake.clone());
        live.messages.push(crate::ConversationMessage {
            id: "m2".into(),
            role: crate::thread_projection::NOTIFICATION_ROLE.into(),
            creation_source: Some("provider".into()),
            ..Default::default()
        });
        let opened = ProviderEvent::ProviderThread {
            driver: "claudeAgent".into(),
            native_thread_id: "sess".into(),
            resumed: false,
            model: None,
            opencode2: None,
            native_title: None,
            native_updated_at: None,
            context_usage: None,
        };
        let monitor = json!({"taskId": "m1", "kind": "monitor", "nativeItemId": "toolu_m1",
            "command": "tail -f build.log", "awaited": true});
        live.provider.apply(first.id, &opened, 1);
        live.provider.apply(
            first.id,
            &ProviderEvent::BackgroundTasks {
                tasks: vec![monitor.clone()],
            },
            1,
        );
        live.provider
            .apply(first.id, &terminal(TurnStatus::Completed), 2);
        live.provider.apply(wake.id, &opened, 3);
        live.provider
            .apply(wake.id, &terminal(TurnStatus::Completed), 4);
        let thread_record = |live: &Thread| live.provider_records().provider_threads.remove(0);
        // The task keeps the run it appeared under, with its launching item.
        let record = thread_record(&live);
        assert_eq!(
            record["pendingBackgroundTasks"],
            json!([{"taskId": "m1", "kind": "monitor", "nativeItemId": "toolu_m1",
                "command": "tail -f build.log", "awaited": true, "runId": "run:m1",
                "itemId": "turn-item:provider:claudeAgent:native-item:toolu_m1",
                "sourceThreadId": "t1"}])
        );
        // The continuation's turn ended: idle until its checkpoint.
        assert_eq!(record["status"], "idle");
        live.checkpoints.push(crate::CheckpointRecord {
            turn_count: 2,
            tree: "tree".into(),
            cwd: "/w".into(),
            captured_at: 5,
            ready: true,
        });
        assert_eq!(thread_record(&live)["status"], "active");
        // With nothing left running it stays idle.
        live.provider.apply(
            wake.id,
            &ProviderEvent::BackgroundTasks { tasks: vec![] },
            5,
        );
        assert_eq!(thread_record(&live)["status"], "idle");
    }

    fn running_codex_thread(run_id: Uuid) -> Thread {
        let mut live = thread(
            serde_json::from_value(json!({
                "id": run_id, "thread_id": "t1", "provider": "codex", "status": "running",
                "started_at": 1, "finished_at": null, "error": null, "user_message_id": null
            }))
            .unwrap(),
        );
        for event in [
            ProviderEvent::ProviderThread {
                driver: "codex".into(),
                native_thread_id: "thr".into(),
                resumed: false,
                model: None,

                opencode2: None,
                native_title: None,
                native_updated_at: None,
                context_usage: None,
            },
            ProviderEvent::ProviderTurn {
                native_turn_id: Some("turn".into()),
                native_turn_ref: None,
                conversation_head: None,
                status: TurnStatus::Running,
                model: None,
                history_injected: false,
            },
        ] {
            live.provider.apply(run_id, &event, 2);
        }
        live
    }

    fn retry(id: &str, status: &str, attempt: u64) -> ProviderEvent {
        let ProviderEvent::TurnItem { mut item } = item(id, "error", status, None) else {
            unreachable!()
        };
        item.fields.insert(
            "failure".into(),
            json!({"class": "transport_error", "message": "dropped", "code": null, "retryable": true}),
        );
        item.fields.insert(
            "retry".into(),
            json!({"attempt": attempt, "maxAttempts": 5, "retryDelayMs": null}),
        );
        ProviderEvent::TurnItem { item }
    }

    #[test]
    fn a_provider_retry_shows_in_its_slot_and_a_failure_after_a_recovery_gets_its_own_row() {
        let run_id = Uuid::new_v4();
        let mut live = running_codex_thread(run_id);
        live.provider
            .apply(run_id, &retry("terminal-failure", "running", 1), 3);
        let rows = |live: &Thread| -> Vec<Value> {
            live.projection()["turnItems"]
                .as_array()
                .unwrap()
                .iter()
                .filter(|row| row["type"] == "error")
                .cloned()
                .collect()
        };
        let row = &rows(&live)[0];
        assert_eq!(row["status"], "running");
        assert_eq!(row["title"], "Provider retry");
        assert_eq!(row["completedAt"], Value::Null);
        let first = row["id"].as_str().unwrap().to_owned();
        assert!(first
            .ends_with("terminal-failure%3Aprovider-turn%3Aprovider%3Acodex%3Anative-turn%3Aturn"));
        live.provider
            .apply(run_id, &retry("terminal-failure", "completed", 1), 4);
        assert_eq!(rows(&live)[0]["title"], "Provider recovered");
        let mut failed = terminal(TurnStatus::Failed);
        if let ProviderEvent::TurnTerminal { failure, .. } = &mut failed {
            *failure = Some(crate::workspace::provider::ProviderFailure::new(
                "provider_error",
                "boom",
            ));
        }
        live.provider.apply(run_id, &failed, 5);
        let rows = rows(&live);
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0]["status"], "completed");
        assert_eq!(rows[1]["status"], "failed");
        assert_eq!(rows[1]["id"], format!("{first}%3A1"));
        assert_eq!(rows[1]["failure"]["message"], "boom");
    }

    #[test]
    fn a_turn_that_fails_in_its_retry_makes_the_retry_its_failure_row() {
        let run_id = Uuid::new_v4();
        let mut live = running_codex_thread(run_id);
        live.provider
            .apply(run_id, &retry("terminal-failure", "running", 2), 3);
        let mut failed = terminal(TurnStatus::Failed);
        if let ProviderEvent::TurnTerminal {
            failure, closing, ..
        } = &mut failed
        {
            *failure = Some(crate::workspace::provider::ProviderFailure::new(
                "transport_error",
                "Connection lost.",
            ));
            closing.push(retry("terminal-failure", "failed", 2));
        }
        live.provider.apply(run_id, &failed, 7);
        let projection = live.projection();
        let rows: Vec<&Value> = projection["turnItems"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|row| row["type"] == "error")
            .collect();
        assert_eq!(rows.len(), 1);
        let row = rows[0];
        assert_eq!(row["status"], "failed");
        assert_eq!(row["title"], "Provider error");
        assert_eq!(row["failure"]["message"], "Connection lost.");
        assert_eq!(row["retry"]["attempt"], 2);
        assert_eq!(row["startedAt"], crate::thread_projection::iso(3));
        assert_eq!(row["completedAt"], crate::thread_projection::iso(7));
    }

    #[test]
    fn a_completed_turn_leaves_its_run_waiting_for_its_checkpoint() {
        let run_id = Uuid::new_v4();
        let mut live = running_codex_thread(run_id);
        live.provider
            .apply(run_id, &terminal(TurnStatus::Completed), 3);
        live.checkpoints.push(crate::CheckpointRecord {
            turn_count: 1,
            tree: "tree".into(),
            cwd: "/w".into(),
            captured_at: 4,
            ready: true,
        });
        let projection = live.projection();
        let root = |projection: &Value| {
            projection["nodes"]
                .as_array()
                .unwrap()
                .iter()
                .find(|node| node["kind"] == "root_turn")
                .unwrap()
                .clone()
        };
        assert_eq!(projection["runs"][0]["status"], "waiting");
        assert_eq!(projection["runs"][0]["completedAt"], Value::Null);
        assert_eq!(projection["runs"][0]["checkpointId"], Value::Null);
        assert_eq!(root(&projection)["status"], "waiting");
        assert_eq!(projection["attempts"][0]["status"], "completed");
        // The run's end, recorded after its checkpoint, completes both.
        live.runs[0].status = "completed".into();
        live.runs[0].finished_at = Some(5);
        let projection = live.projection();
        assert_eq!(projection["runs"][0]["status"], "completed");
        assert!(projection["runs"][0]["checkpointId"].is_string());
        assert_eq!(root(&projection)["status"], "completed");
    }

    #[test]
    fn an_interrupt_request_shows_from_when_it_was_asked_for() {
        let run_id = Uuid::new_v4();
        let mut live = running_codex_thread(run_id);
        live.provider.apply(
            run_id,
            &item("cmd", "command_execution", "running", None),
            2,
        );
        live.provider.interrupt_requests.insert(run_id, (3, 1));
        live.provider.apply(
            run_id,
            &item("late", "command_execution", "running", None),
            3,
        );
        let kinds = |live: &Thread| -> Vec<String> {
            live.projection()["turnItems"]
                .as_array()
                .unwrap()
                .iter()
                .map(|item| {
                    format!(
                        "{} {}",
                        item["type"].as_str().unwrap(),
                        item["status"].as_str().unwrap()
                    )
                })
                .collect()
        };
        // The request's card takes the slot after the items before it, and
        // the run is still running.
        assert_eq!(
            kinds(&live),
            [
                "command_execution running",
                "run_interrupt_request completed",
                "command_execution running"
            ]
        );
        assert_eq!(live.projection()["runs"][0]["status"], "running");
        // The interrupted turn ends the run at once, with its result card.
        live.provider
            .apply(run_id, &terminal(TurnStatus::Interrupted), 4);
        assert_eq!(
            kinds(&live),
            [
                "command_execution interrupted",
                "run_interrupt_request completed",
                "command_execution interrupted",
                "run_interrupt_result interrupted"
            ]
        );
        assert_eq!(live.projection()["runs"][0]["status"], "interrupted");
        assert!(live.projection()["runs"][0]["completedAt"].is_string());
    }

    #[test]
    fn a_sent_run_has_its_pending_records_until_its_cli_reports() {
        let run_id = Uuid::new_v4();
        let mut live = thread(
            serde_json::from_value(json!({
                "id": run_id, "thread_id": "t1", "provider": "default", "status": "running",
                "started_at": 1, "finished_at": null, "error": null, "user_message_id": null,
                "driver": "codex", "model_selection": {"instanceId": "codex", "model": "m"}
            }))
            .unwrap(),
        );
        let projection = live.projection();
        assert_eq!(projection["runs"][0]["status"], "starting");
        assert_eq!(projection["runs"][0]["startedAt"], Value::Null);
        assert_eq!(projection["attempts"][0]["status"], "pending");
        assert_eq!(projection["nodes"][0]["status"], "pending");
        let pending = &projection["providerThreads"][0];
        assert_eq!(pending["status"], "not_loaded");
        assert_eq!(
            pending["providerSessionId"],
            "provider-session:provider-instance:codex:shared"
        );
        assert_eq!(
            projection["thread"]["activeProviderThreadId"],
            pending["id"]
        );
        assert_eq!(projection["runs"][0]["providerThreadId"], pending["id"]);
        assert_eq!(
            projection["checkpointScopes"][0]["providerThreadId"],
            pending["id"]
        );
        // The CLI reports its thread: the same records, running.
        live.provider.apply(
            run_id,
            &ProviderEvent::ProviderThread {
                driver: "codex".into(),
                native_thread_id: "thr".into(),
                resumed: false,
                model: None,

                opencode2: None,
                native_title: None,
                native_updated_at: None,
                context_usage: None,
            },
            2,
        );
        let started = live.projection();
        assert_eq!(started["runs"][0]["status"], "running");
        assert_eq!(started["attempts"][0]["status"], "running");
        assert_eq!(started["providerThreads"][0]["id"], pending["id"]);
        assert_eq!(started["providerThreads"][0]["status"], "active");
        // The thread's model changes during the run; the run keeps its own.
        live.row["modelSelection"] = json!({"instanceId": "codex", "model": "other"});
        assert_eq!(live.projection()["runs"][0]["modelSelection"]["model"], "m");
    }

    #[test]
    fn a_queued_run_has_no_checkpoint_scope_until_it_is_sent() {
        let run_id = Uuid::new_v4();
        let mut live = thread(
            serde_json::from_value(json!({
                "id": run_id, "thread_id": "t1", "provider": "default", "status": "queued",
                "started_at": 1, "finished_at": null, "error": null, "user_message_id": null,
                "driver": "codex"
            }))
            .unwrap(),
        );
        // A deferred message's run waits in the queue: the reference has no
        // scope yet.
        assert_eq!(live.projection()["checkpointScopes"], json!([]));
        live.runs[0].status = "running".into();
        assert_eq!(
            live.projection()["checkpointScopes"]
                .as_array()
                .unwrap()
                .len(),
            1
        );
    }

    #[test]
    fn an_opencode2_thread_shares_its_instances_session_and_stays_ready_after_a_failure() {
        let run_id = Uuid::new_v4();
        let mut live = thread(
            serde_json::from_value(json!({
                "id": run_id, "thread_id": "t1", "provider": "opencode", "status": "running",
                "started_at": 1, "finished_at": null, "error": null, "user_message_id": null,
                "driver": "opencode"
            }))
            .unwrap(),
        );
        live.row = json!({"modelSelection": {"instanceId": "opencode", "model": "a/m"}});
        let failure = crate::workspace::provider::ProviderFailure::new("usage_limit", "Rate limit");
        for event in [
            ProviderEvent::ProviderThread {
                driver: "opencode".into(),
                native_thread_id: "ses_1".into(),
                resumed: false,
                model: None,
                opencode2: Some(crate::workspace::provider::OpenCode2Thread {
                    version: "2.2.6".into(),
                    model_selection: json!({"instanceId": "opencode", "model": "a/m", "options": []}),
                }),
                native_title: None,
                native_updated_at: None,
                context_usage: None,
            },
            ProviderEvent::ProviderTurn {
                native_turn_id: Some("ses_1:attempt:a".into()),
                native_turn_ref: Some(
                    json!({"driver": "opencode", "nativeId": "msg_t3_turn_ses_1:a", "strength": "weak"}),
                ),
                conversation_head: None,
                status: TurnStatus::Running,
                model: None,
                history_injected: false,
            },
            ProviderEvent::TurnTerminal {
                status: TurnStatus::Failed,
                failure: Some(failure),
                thread_disposition: crate::workspace::provider::ThreadDisposition::Reusable,
                driver: None,
                closing: Vec::new(),
            },
        ] {
            live.provider.apply(run_id, &event, 2);
        }
        let projection = live.projection();
        let session = &projection["providerSessions"][0];
        assert_eq!(
            session["id"],
            "provider-session:provider-instance:opencode:shared"
        );
        assert_eq!(session["status"], "ready");
        assert_eq!(session["lastError"], Value::Null);
        let capabilities = &session["capabilities"];
        assert_eq!(
            capabilities["sessions"]["supportsMultipleProviderThreadsPerSession"],
            true
        );
        assert_eq!(capabilities["turns"]["supportsNativeGoals"], true);
        let thread = &projection["providerThreads"][0];
        assert_eq!(thread["providerSessionId"], session["id"]);
        assert_eq!(thread["nativeMetadata"]["modelSelection"]["model"], "a/m");
        assert_eq!(
            projection["providerTurns"][0]["nativeTurnRef"]["nativeId"],
            "msg_t3_turn_ses_1:a"
        );
    }
}
