use crate::{auth_service, Store, Thread};
use serde_json::{json, Map, Value};

const APP_FIELDS: &[&str] = include!(concat!(env!("OUT_DIR"), "/OrchestrationV2AppThread.rs"));
const SHELL_FIELDS: &[&str] = include!(concat!(env!("OUT_DIR"), "/OrchestrationV2ThreadShell.rs"));

fn ordered(fields: &[&str], row: &Value) -> Value {
    Value::Object(
        fields
            .iter()
            .filter_map(|&k| row.get(k).map(|v| (k.to_string(), v.clone())))
            .collect(),
    )
}

/// `withMessageSenderNote`: `text` as the provider reads it, after a line
/// saying who sent `message` (a projected message row) when a person did not
/// type it. Text starting with a slash command or a skill mention such as
/// `$deploy` stays as sent, since providers parse those from the start.
pub(crate) fn with_message_sender_note(message: &Value, text: &str) -> String {
    use std::sync::LazyLock;
    static LEADING_COMMAND: LazyLock<regex::Regex> = LazyLock::new(|| {
        regex::Regex::new(r"^\s*(?:/|\p{Sc}[a-zA-Z0-9:_-]*[a-zA-Z])").expect("pattern")
    });
    let quoted = |value: &Value| serde_json::to_string(value).unwrap_or_default();
    let agent = message["createdBy"] == "agent";
    let linked = &message["linkedSender"];
    let note = if agent && linked.is_object() {
        Some(format!(
            "[T3 Code: the agent in T3 thread {} on linked environment {} sent this message, not the user.]",
            quoted(&linked["threadId"]),
            quoted(&linked["environmentId"])
        ))
    } else if agent && message["senderThreadId"].is_string() {
        Some(format!(
            "[T3 Code: the agent in T3 thread {} sent this message, not the user.]",
            quoted(&message["senderThreadId"])
        ))
    } else if message["scheduledTaskId"].is_string() {
        Some(format!(
            "[T3 Code: scheduled task {} sent this prompt, not the user typing in this chat.]",
            quoted(&message["scheduledTaskId"])
        ))
    } else if agent && message["creationSource"] == "mcp" {
        Some("[T3 Code: this message came through T3 Code's tools, not from the user typing in this chat.]".to_owned())
    } else {
        None
    };
    match note {
        Some(note) if !LEADING_COMMAND.is_match(text) => {
            if text.is_empty() {
                note
            } else {
                format!("{note}\n\n{text}")
            }
        }
        _ => text.to_owned(),
    }
}

/// The attachments a message carries, as its wire fields keep them.
pub(crate) fn message_attachments(message: &crate::ConversationMessage) -> Value {
    message
        .wire
        .as_ref()
        .and_then(|wire| wire.get("attachments"))
        .filter(|attachments| attachments.is_array())
        .cloned()
        .unwrap_or_else(|| json!([]))
}

pub(crate) fn iso(seconds: u64) -> String {
    auth_service::iso((seconds as i64).saturating_mul(1000))
}

/// JavaScript's encodeURIComponent, which the reference uses to nest ids in ids.
pub(crate) fn uri_component(value: &str) -> String {
    const RESERVED: &percent_encoding::AsciiSet = &percent_encoding::NON_ALPHANUMERIC
        .remove(b'-')
        .remove(b'_')
        .remove(b'.')
        .remove(b'!')
        .remove(b'~')
        .remove(b'*')
        .remove(b'\'')
        .remove(b'(')
        .remove(b')');
    percent_encoding::utf8_percent_encode(value, RESERVED).to_string()
}

fn merge(target: &mut Value, fields: Value) {
    if let (Some(target), Value::Object(fields)) = (target.as_object_mut(), fields) {
        target.extend(fields);
    }
}

/// A queued run, whose message waits to be sent out of the timeline.
fn queued(run: &crate::Run) -> bool {
    run.status == "queued"
}

/// The role of a message the server sent as a notification: the prompt that
/// continues a run a restart cut off.
pub(crate) const NOTIFICATION_ROLE: &str = "notification";
/// How many of its run's provider items a link card was asked after, kept on
/// the stored card and left out of the projection.
pub(crate) const LINK_CARD_POSITION: &str = "afterProviderItems";

/// The reference's run statuses; older stores can hold others.
fn run_status(status: &str) -> &'static str {
    match status {
        "preparing" => "preparing",
        "queued" => "queued",
        "starting" => "starting",
        "running" => "running",
        "waiting" => "waiting",
        "completed" => "completed",
        "interrupted" => "interrupted",
        "cancelled" => "cancelled",
        "rolled_back" => "rolled_back",
        _ => "failed",
    }
}

/// Drops from `projection` each delegated wake's message whose run has
/// ended, as the reference's snapshot window reads only the messages its
/// turn items name and those of live runs.
fn window_messages(projection: &mut Value) {
    let live: std::collections::HashSet<String> = projection["runs"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|run| {
            matches!(
                run["status"].as_str(),
                Some("queued" | "preparing" | "starting" | "running" | "waiting")
            )
        })
        .filter_map(|run| run["id"].as_str().map(str::to_owned))
        .collect();
    if let Some(messages) = projection["messages"].as_array_mut() {
        messages.retain(|message| {
            let wake = message["delegatedCompletion"].is_object()
                || (message["role"] == "user"
                    && message["createdBy"] == "agent"
                    && message["creationSource"] == "provider"
                    && message["notification"].is_object());
            !wake
                || message["runId"]
                    .as_str()
                    .is_some_and(|run| live.contains(run))
        });
    }
}

/// Whether `message` delivers delegated tasks' results to their parent.
pub(crate) fn delegated_delivery(message: &crate::ConversationMessage) -> bool {
    message
        .wire
        .as_ref()
        .is_some_and(|wire| wire["delegatedCompletion"].is_object())
}

/// A provider's continuation run's message (`creationSource: "provider"`),
/// which the reference records as the agent's user message carrying its
/// notification.
pub(crate) fn provider_continuation(message: &crate::ConversationMessage) -> bool {
    message.role == NOTIFICATION_ROLE && message.creation_source.as_deref() == Some("provider")
}

/// A notification the reference also records as the agent's message: a
/// delegated task's delivery or a provider's continuation.
pub(crate) fn agent_notification(message: &crate::ConversationMessage) -> bool {
    delegated_delivery(message) || provider_continuation(message)
}

fn message_role(role: &str) -> &'static str {
    match role {
        "assistant" => "assistant",
        "system" => "system",
        _ => "user",
    }
}

/// The thread's model selection; rows from before selections were stored name no model.
fn model_selection(row: &Value) -> Value {
    if row["modelSelection"]["instanceId"].is_string() && row["modelSelection"]["model"].is_string()
    {
        return row["modelSelection"].clone();
    }
    json!({"instanceId":row["providerInstanceId"].as_str().unwrap_or("codex"),"model":"default"})
}

/// The reference's dedupeSnapshotBodies: a message item repeats its message's text, so the
/// item carries `textOmitted` instead and the client restores the text from the message;
/// a subagent item likewise drops the prompt and result its task carries.
fn dedupe_bodies(projection: &mut Value) {
    let texts: std::collections::HashMap<String, String> = projection["messages"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|m| Some((m["id"].as_str()?.to_owned(), m["text"].as_str()?.to_owned())))
        .collect();
    let subagents: std::collections::HashMap<String, Value> = projection["subagents"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|t| Some((t["id"].as_str()?.to_owned(), t.clone())))
        .collect();
    let dedupe = |item: &mut Value| {
        // `dedupeSubagentBodies`: a prompt or result its task record repeats.
        if item["type"] == "subagent" {
            let Some(task) = item["subagentId"].as_str().and_then(|id| subagents.get(id)) else {
                return;
            };
            if item["prompt"].as_str().is_some_and(|p| !p.is_empty())
                && task["prompt"] == item["prompt"]
            {
                item["prompt"] = json!("");
                item["promptOmitted"] = json!(true);
            }
            if !item["result"].is_null() && task["result"] == item["result"] {
                item["result"] = Value::Null;
                item["resultOmitted"] = json!(true);
            }
            return;
        }
        if !matches!(
            item["type"].as_str(),
            Some("user_message" | "assistant_message")
        ) {
            return;
        }
        let text = item["text"].as_str().unwrap_or_default();
        if !text.is_empty()
            && item["messageId"]
                .as_str()
                .and_then(|id| texts.get(id))
                .map(String::as_str)
                == Some(text)
        {
            item["text"] = json!("");
            item["textOmitted"] = json!(true);
        }
    };
    if let Some(items) = projection["turnItems"].as_array_mut() {
        items.iter_mut().for_each(dedupe);
    }
    if let Some(rows) = projection["visibleTurnItems"].as_array_mut() {
        rows.iter_mut().for_each(|row| dedupe(&mut row["item"]));
    }
}

/// The id of a thread's root checkpoint scope, as the reference names it.
pub(crate) fn checkpoint_scope_id(thread_id: &str) -> String {
    format!(
        "checkpoint-scope:thread:{}:name:root",
        uri_component(thread_id)
    )
}

/// The id of a thread's checkpoint after `turn_count` turns.
pub(crate) fn checkpoint_id(thread_id: &str, turn_count: u64) -> String {
    format!(
        "checkpoint:scope:{}:name:{turn_count}",
        uri_component(&checkpoint_scope_id(thread_id))
    )
}

pub(crate) fn created(command: &Value, at: &str) -> Value {
    let mut row = json!({"createdBy":command["createdBy"],"creationSource":command["creationSource"],
        "id":command["threadId"],"projectId":command["projectId"],"title":command["title"],
        "providerInstanceId":command["modelSelection"]["instanceId"],"modelSelection":command["modelSelection"],
        "runtimeMode":command["runtimeMode"],"interactionMode":command["interactionMode"],
        "branch":command["branch"],"worktreePath":command["worktreePath"],
        "pullRequests":[],
        "activeProviderThreadId":null,"lineage":{"parentThreadId":null,"relationshipToParent":null,"rootThreadId":command["threadId"]},
        "forkedFrom":null,"createdAt":at,"updatedAt":at,"archivedAt":null,"settledOverride":null,
        "settledAt":null,"snoozedUntil":null,"snoozedAt":null,"lastVisitedAt":null,"deletedAt":null});
    // Work a link started carries that link (`linkOrigin`), and with it where a
    // link's delegation or workflow run came from; without one those are dropped.
    if command["linkOrigin"].is_object() {
        for key in ["linkOrigin", "delegatedFrom", "workflowRun"] {
            if command[key].is_object() {
                row[key] = command[key].clone();
            }
        }
    }
    row
}

impl Thread {
    /// Each run as `OrchestrationV2Run`. Runs have no provider records here, so the ids of
    /// those (provider thread, root node, attempt, checkpoint) are null.
    fn run_rows(&self, row: &Value) -> Vec<Value> {
        let selection = model_selection(row);
        let instance = row["providerInstanceId"]
            .as_str()
            .or(selection["instanceId"].as_str())
            .unwrap_or("codex")
            .to_owned();
        let mut queue_position = 0;
        self.runs
            .iter()
            .enumerate()
            .map(|(index, run)| {
                let (status, ended_at) = self.projected_run_status(run);
                let waiting = matches!(status, "queued" | "preparing" | "starting");
                let requested_at = self
                    .messages
                    .iter()
                    .find(|message| Some(&message.id) == run.user_message_id.as_ref())
                    .map_or(run.started_at, |message| message.created_at);
                let mut out = json!({
                    "id": run.wire_id(),
                    "threadId": self.id,
                    "ordinal": index + 1,
                    // Each run names the instance it ran on (a switch changes it).
                    "providerInstanceId": run
                        .model_selection
                        .as_deref()
                        .and_then(|selection| selection["instanceId"].as_str())
                        .unwrap_or(&instance),
                    "modelSelection": run.model_selection.as_deref().unwrap_or(&selection),
                    "providerThreadId": null,
                    "userMessageId": run.user_message_id.clone().filter(|id| !id.trim().is_empty()).unwrap_or_else(|| run.wire_id()),
                    "rootNodeId": null,
                    "activeAttemptId": null,
                    "status": status,
                    "queuePosition": null,
                    "requestedAt": iso(requested_at),
                    "startedAt": if waiting { Value::Null } else { iso(run.started_at).into() },
                    "completedAt": ended_at.map(iso),
                    "checkpointId": null,
                    "contextHandoffId": null
                });
                // A merge-back's delta, or a provider switch's handoff, is
                // the run's handoff.
                if let Some(handoff) = self
                    .context_handoffs
                    .iter()
                    .find(|handoff| handoff["targetRunId"] == out["id"] && run_handoff(handoff))
                {
                    out["contextHandoffId"] = handoff["id"].clone();
                }
                let turn_count = index as u64 + 1;
                // A completed turn's checkpoint shows on its run once the run
                // completes, as the reference's capture completes it.
                if status != "waiting"
                    && self.checkpoints.iter().any(|c| c.turn_count == turn_count)
                {
                    out["checkpointId"] = json!(checkpoint_id(&self.id, turn_count));
                }
                if let Some(ids) = self.provider_run_ids(run) {
                    out["providerThreadId"] = json!(ids.provider_thread);
                    out["rootNodeId"] = json!(ids.root_node);
                    out["activeAttemptId"] = json!(ids.attempt);
                } else if let Some(ids) = self
                    .queued_run_ids(run)
                    .or_else(|| self.starting_run_ids(run))
                {
                    out["providerThreadId"] = json!(ids.provider_thread);
                    out["rootNodeId"] = json!(ids.root_node);
                    out["activeAttemptId"] = json!(ids.attempt);
                }
                if let Some(source) = run.restart_continuation_of {
                    out["restartContinuationOfRunId"] = json!(source.to_string());
                    out["workStartedAt"] = json!(iso(self.work_started_at(run)));
                }
                if let Some(schema) = &run.output_schema {
                    out["outputSchema"] = (**schema).clone();
                }
                if run.fresh_context {
                    out["freshContext"] = json!(true);
                }
                if !run.background_work.is_empty() {
                    out["restartCancelledBackgroundWork"] = json!(run.background_work);
                }
                // Fields the orchestrator records on a run itself, such as a
                // delegated task cohort (`delegatedCompletion`).
                for (key, value) in row["runExtras"][run.wire_id()]
                    .as_object()
                    .into_iter()
                    .flatten()
                    // A queued wake takes its work's start when it leaves
                    // the queue, as the reference's dispatch sets it then.
                    .filter(|(key, _)| !(key.as_str() == "workStartedAt" && status == "queued"))
                {
                    out[key] = value.clone();
                }
                if status == "queued" {
                    queue_position += 1;
                    out["queuePosition"] = json!(queue_position);
                    out["queueHeld"] = json!(run.queue_held);
                }
                out
            })
            .collect()
    }

    /// `run`'s status and end time as the reference records them. A
    /// provider turn that completed leaves its run `waiting` until the
    /// turn's checkpoint completes it (here, until the run's end is
    /// recorded, after the checkpoint); one interrupted or failed ends its
    /// run at once, at the turn's end.
    pub(crate) fn projected_run_status(&self, run: &crate::Run) -> (&'static str, Option<u64>) {
        use crate::workspace::provider::TurnStatus;
        let status = run_status(&run.status);
        let turn = self
            .provider
            .terminals
            .get(&run.id)
            .map(|terminal| terminal.status);
        let turn_end = || {
            self.provider
                .runs
                .get(&run.id)
                .map(|record| record.ended_at.unwrap_or(record.started_at))
        };
        match (status, turn) {
            ("running", None) if self.starting(run) => ("starting", None),
            ("running", Some(TurnStatus::Completed)) => ("waiting", None),
            ("running", Some(TurnStatus::Interrupted)) => ("interrupted", turn_end()),
            ("running", Some(TurnStatus::Failed)) => ("failed", turn_end()),
            ("interrupted", Some(TurnStatus::Interrupted))
            | ("failed", Some(TurnStatus::Failed)) => (status, turn_end().or(run.finished_at)),
            _ => (status, run.finished_at),
        }
    }

    /// When the work `run` continues began: the start of the first run of
    /// its chain of restart continuations.
    fn work_started_at(&self, run: &crate::Run) -> u64 {
        let mut current = run;
        for _ in 0..self.runs.len() {
            let Some(source) = current
                .restart_continuation_of
                .and_then(|id| self.runs.iter().find(|r| r.id == id))
            else {
                break;
            };
            current = source;
        }
        current.started_at
    }

    fn run_of(&self, message_id: &str) -> Option<(usize, &crate::Run)> {
        self.runs
            .iter()
            .enumerate()
            .find(|(_, run)| run.user_message_id.as_deref() == Some(message_id))
    }

    /// Each message as `OrchestrationV2ConversationMessage`.
    fn message_rows(&self) -> Vec<Value> {
        self.messages
            .iter()
            // A notification shows as its turn item only, except a delegated
            // task's delivery or a provider's continuation, which the
            // reference also records as the agent's user message.
            .filter(|message| message.role != NOTIFICATION_ROLE || agent_notification(message))
            .map(|message| {
                let at = iso(message.created_at);
                let pending_ids = self
                    .run_of(&message.id)
                    .filter(|(_, run)| matches!(run.status.as_str(), "queued" | "preparing"))
                    .and_then(|(_, run)| self.queued_run_ids(run));
                let mut row = json!({
                    "createdBy": if message.role == "user" { "user" } else { "agent" },
                    "creationSource": if message.role == "user" { message.creation_source.as_deref().unwrap_or("web") } else { "provider" },
                    "id": message.id,
                    "threadId": self.id,
                    "runId": self.run_of(&message.id).map(|(_, run)| run.wire_id()),
                    "nodeId": pending_ids
                        .as_ref()
                        .map_or(Value::Null, |ids| json!(ids.root_node)),
                    "role": message_role(&message.role),
                    "text": message.content,
                    "attachments": [],
                    "streaming": false,
                    "createdAt": at,
                    "updatedAt": at
                });
                if let Some(task) = &message.scheduled_task_id {
                    row["scheduledTaskId"] = json!(task);
                }
                if message.follows_thread_selection {
                    row["followsThreadSelection"] = json!(true);
                }
                // A delegated wake's message stays on its run's root node.
                if pending_ids.is_none() && agent_notification(message) {
                    if let Some(ids) = self.run_of(&message.id).and_then(|(_, run)| {
                        self.provider_run_ids(run)
                            .or_else(|| self.starting_run_ids(run))
                    }) {
                        row["nodeId"] = json!(ids.root_node);
                    }
                }
                // A steer belongs to the run it joined.
                if let Some(steer) = &message.steer {
                    row["runId"] = steer["runId"].clone();
                    row["nodeId"] = steer["nodeId"].clone();
                }
                for (key, value) in message.wire.iter().flat_map(|w| w.as_object()).flatten() {
                    row[key] = value.clone();
                }
                row
            })
            .collect()
    }

    /// The timeline as `OrchestrationV2TurnItem`s: each message, then its run's failure,
    /// then the `provider` items (by run index) the run's provider turn reported.
    /// Ordinals follow the reference's TurnItemPositionStore: a run's items start after
    /// `ordinal * 1_000_000`, so items stay ordered when earlier runs gain items later;
    /// each provider item takes the next free slot of its run.
    fn turn_items_with(&self, provider: Vec<(usize, Value)>) -> Vec<Value> {
        let mut items = Vec::new();
        let mut steers: Vec<(usize, u64, Value)> = Vec::new();
        let mut ordinal: u64 = 0;
        for message in &self.messages {
            // A steer takes the next free slot of the run it joined, as a
            // link card does (`nextTurnItemOrdinal`).
            if let Some(steer) = &message.steer {
                let Some(index) = self
                    .runs
                    .iter()
                    .position(|run| steer["runId"] == run.wire_id().as_str())
                else {
                    continue;
                };
                let at = iso(message.created_at);
                // The CLI's account of the steer (`steer.updated`): pending until
                // it takes it, as the reference shows it.
                let status = steer["status"].as_str().unwrap_or("completed");
                let changed = steer["statusAt"]
                    .as_u64()
                    .map(iso)
                    .unwrap_or_else(|| at.clone());
                let completed_at = if status == "pending" {
                    Value::Null
                } else {
                    json!(changed)
                };
                let item = json!({
                    "id": format!("turn-item:message:{}", uri_component(&message.id)),
                    "threadId": self.id,
                    "runId": steer["runId"],
                    "nodeId": steer["nodeId"],
                    "providerThreadId": steer["providerThreadId"],
                    "providerTurnId": steer["providerTurnId"],
                    "nativeItemRef": null,
                    "parentItemId": null,
                    "ordinal": 0,
                    "status": status,
                    "title": null,
                    "startedAt": at,
                    "completedAt": completed_at,
                    "updatedAt": changed,
                    "type": "user_message",
                    "createdBy": "user",
                    "creationSource": message.creation_source.as_deref().unwrap_or("web"),
                    "messageId": message.id,
                    "inputIntent": "steer",
                    "text": message.content,
                    "attachments": message_attachments(message)
                });
                // A steered notification, such as a delegated task's delivery,
                // shows as its card (`notificationTurnItem`).
                let mut item = item;
                if message.role == NOTIFICATION_ROLE {
                    if let (Some(fields), Some(notification)) =
                        (item.as_object_mut(), message.notification.as_ref())
                    {
                        for key in [
                            "type",
                            "messageId",
                            "inputIntent",
                            "text",
                            "attachments",
                            "createdBy",
                            "creationSource",
                        ] {
                            fields.remove(key);
                        }
                        fields.insert("type".into(), json!("notification"));
                        for (key, value) in notification.as_object().into_iter().flatten() {
                            fields.insert(key.clone(), value.clone());
                        }
                    }
                }
                let after = steer[LINK_CARD_POSITION].as_u64().unwrap_or(0);
                steers.push((index, after, item));
                continue;
            }
            let run = self.run_of(&message.id);
            ordinal = match run {
                Some((index, _)) => ((index as u64 + 1) * 1_000_000).max(ordinal) + 1,
                None => ordinal + 1,
            };
            // A handoff recorded at its run's start (a resume fallback's
            // recovered context) sits ahead of the run's prompt, as the
            // reference's `run_start` placement puts it.
            if let Some((index, run)) = run.filter(|_| message.role == "user") {
                let start = (index as u64 + 1) * 1_000_000;
                let run_id = run.wire_id();
                for handoff in self
                    .handoff_items
                    .iter()
                    .filter(|item| item["runId"] == run_id.as_str())
                {
                    let mut handoff = handoff.clone();
                    if ordinal == start + 1 && !items.iter().any(|i: &Value| i["ordinal"] == start)
                    {
                        handoff["ordinal"] = json!(start);
                    } else {
                        handoff["ordinal"] = json!(ordinal);
                        ordinal += 1;
                    }
                    items.push(handoff);
                }
            }
            let run_id = run.map(|(_, run)| run.wire_id());
            let at = iso(message.created_at);
            let base = |id: String,
                        ordinal: u64,
                        status: &str,
                        title: Value,
                        started: &str,
                        completed: Value,
                        updated: &str| {
                json!({
                    "id": id,
                    "threadId": self.id,
                    "runId": run_id,
                    "nodeId": null,
                    "providerThreadId": null,
                    "providerTurnId": null,
                    "nativeItemRef": null,
                    "parentItemId": null,
                    "ordinal": ordinal,
                    "status": status,
                    "title": title,
                    "startedAt": started,
                    "completedAt": completed,
                    "updatedAt": updated
                })
            };
            let mut item = base(
                format!("turn-item:message:{}", uri_component(&message.id)),
                ordinal,
                "completed",
                Value::Null,
                &at,
                at.clone().into(),
                &at,
            );
            if let Some((_, pending_run)) =
                run.filter(|(_, run)| matches!(run.status.as_str(), "queued" | "preparing"))
            {
                if let Some(ids) = self.queued_run_ids(pending_run) {
                    item["nodeId"] = json!(ids.root_node);
                    item["providerThreadId"] = json!(ids.provider_thread);
                }
            } else if let Some((_, started)) = run.filter(|_| message.role == "user") {
                // A prompt stays on its run's root node and provider thread
                // once the run starts, as the reference writes it.
                if let Some(ids) = self
                    .provider_run_ids(started)
                    .or_else(|| self.starting_run_ids(started))
                    .filter(|ids| !ids.provider_thread.is_empty())
                {
                    item["nodeId"] = json!(ids.root_node);
                    item["providerThreadId"] = json!(ids.provider_thread);
                }
            }
            let fields = match message.role.as_str() {
                "user" => json!({
                    "type": "user_message",
                    "createdBy": "user",
                    "creationSource": message.creation_source.as_deref().unwrap_or("web"),
                    "messageId": message.id,
                    "inputIntent": "turn_start",
                    "text": message.content,
                    "attachments": message_attachments(message)
                }),
                "assistant" => json!({
                    "type": "assistant_message",
                    "messageId": message.id,
                    "text": message.content,
                    "streaming": false
                }),
                // The prompt that continues a run a restart cut off.
                NOTIFICATION_ROLE => {
                    let delivery = agent_notification(message);
                    // A delegated delivery's card waits with its queued run,
                    // as the reference writes it when the run leaves the queue.
                    if delivery && run.is_some_and(|(_, run)| queued(run)) {
                        continue;
                    }
                    let ids = run.and_then(|(_, run)| {
                        self.provider_run_ids(run).or_else(|| {
                            // The reference's card names its run's node and
                            // provider thread from the start.
                            delivery
                                .then(|| {
                                    self.queued_run_ids(run)
                                        .or_else(|| self.starting_run_ids(run))
                                })
                                .flatten()
                        })
                    });
                    if let Some(ids) = ids {
                        item["nodeId"] = json!(ids.root_node);
                        item["providerThreadId"] = json!(ids.provider_thread);
                    }
                    // A notification the server sent, such as the answer to a link card.
                    if let Some(notification) = &message.notification {
                        let mut fields = json!({"type": "notification"});
                        merge(&mut fields, notification.clone());
                        fields
                    } else {
                        json!({
                            "type": "notification",
                            "source": {"kind": "system"},
                            "outcome": "updated",
                            "summary": crate::runtime_recovery::continuation_summary(&message.content),
                            "detail": message.content
                        })
                    }
                }
                _ => continue,
            };
            merge(&mut item, fields);
            if message.role == "user" {
                if let Some(switch) = run
                    .filter(|(_, run)| !queued(run))
                    .and_then(|(index, run)| self.model_switch(index, run))
                {
                    item["modelSwitch"] = switch;
                }
            }
            if let Some(task) = message
                .scheduled_task_id
                .as_ref()
                .filter(|_| message.role == "user")
            {
                item["scheduledTaskId"] = json!(task);
            }
            // A queued message is pending input on its run, which snapshots
            // leave out until the run starts (the reference's queued input).
            if message.role == "user" {
                if let Some((_, queued_run)) = run.filter(|(_, run)| queued(run)) {
                    item["status"] = json!("pending");
                    item["inputIntent"] = json!("queued_turn");
                    item["startedAt"] = Value::Null;
                    item["completedAt"] = Value::Null;
                    if let Some(ids) = self
                        .provider_run_ids(queued_run)
                        .or_else(|| self.queued_run_ids(queued_run))
                    {
                        item["nodeId"] = json!(ids.root_node);
                        item["providerThreadId"] = json!(ids.provider_thread);
                    }
                    items.push(item);
                    continue;
                }
            }
            items.push(item);
            let Some((_, run)) = run else { continue };
            // A provider turn's end adds its own items.
            if self.provider.terminals.contains_key(&run.id) {
                continue;
            }
            // So does a provider turn a restart cut off.
            if let Some(restart) = &run.restart_cut {
                if !self.provider.runs.contains_key(&run.id) {
                    ordinal += 1;
                    let mut notice = self.restart_notice(run, restart, None);
                    notice["ordinal"] = json!(ordinal);
                    items.push(notice);
                }
                continue;
            }
            let Some(error) = run
                .error
                .as_deref()
                .map(str::trim)
                .filter(|e| !e.is_empty())
            else {
                continue;
            };
            ordinal += 1;
            let failed_at = iso(run.finished_at.unwrap_or(run.started_at));
            let mut failure = base(
                format!(
                    "turn-item:run:{}:signal:provider-failure",
                    uri_component(&run.wire_id())
                ),
                ordinal,
                "failed",
                json!("Provider failed"),
                &failed_at,
                failed_at.clone().into(),
                &failed_at,
            );
            merge(
                &mut failure,
                json!({"type":"error","failure":{
                    "class":"provider_error",
                    "message": error.chars().take(4096).collect::<String>().trim_end(),
                    "code":null,
                    "retryable":null
                }}),
            );
            items.push(failure);
        }
        // A link card takes the next free slot of its run when it is asked, as
        // the reference's `nextTurnItemOrdinal`: after the provider items its
        // run had then (`afterProviderItems`), before the ones that came later.
        // Items the orchestrator records itself, such as a delegated task's
        // card, sit the same way.
        let app_items = self.stored_row()["appTurnItems"]
            .as_array()
            .cloned()
            .unwrap_or_default();
        let mut cards: Vec<(usize, u64, Value)> = self
            .link_requests
            .iter()
            .chain(app_items.iter())
            .filter_map(|card| {
                let index = self.runs.iter().position(|run| {
                    card["runId"] == run.id.to_string().as_str()
                        || card["runId"] == run.wire_id().as_str()
                })?;
                let after = card[LINK_CARD_POSITION].as_u64().unwrap_or(0);
                let mut card = card.clone();
                if let Some(fields) = card.as_object_mut() {
                    fields.remove(LINK_CARD_POSITION);
                }
                Some((index, after, card))
            })
            .chain(steers)
            .collect();
        let next_slot = |items: &[Value], index: usize| {
            let lower = (index as u64 + 1) * 1_000_000;
            let upper = lower + 999_999;
            items
                .iter()
                .filter_map(|i| i["ordinal"].as_u64())
                .filter(|o| (lower..=upper).contains(o))
                .max()
                .unwrap_or(lower)
                + 1
        };
        let mut seen: std::collections::HashMap<usize, u64> = std::collections::HashMap::new();
        let mut place_cards = |items: &mut Vec<Value>, index: usize, upto: u64| {
            let mut position = 0;
            while position < cards.len() {
                if cards[position].0 == index && cards[position].1 <= upto {
                    let (_, _, mut card) = cards.remove(position);
                    card["ordinal"] = json!(next_slot(items, index));
                    items.push(card);
                } else {
                    position += 1;
                }
            }
        };
        for (index, mut item) in provider {
            let count = *seen.get(&index).unwrap_or(&0);
            place_cards(&mut items, index, count);
            item["ordinal"] = json!(next_slot(&items, index));
            items.push(item);
            seen.insert(index, count + 1);
        }
        // Cards asked after the run's last provider item, or in a run with none.
        for index in 0..self.runs.len() {
            place_cards(&mut items, index, u64::MAX);
        }
        items.retain(|item| item.get(crate::provider_projection::HIDDEN_ITEM).is_none());
        items.sort_by(|a, b| {
            (a["ordinal"].as_u64(), a["id"].as_str())
                .cmp(&(b["ordinal"].as_u64(), b["id"].as_str()))
        });
        items
    }

    pub(crate) fn stored_row(&self) -> Value {
        if self.row.get("id").is_some() {
            return self.row.clone();
        }
        // A thread logged without its row; a patch may since have set a few
        // fields on it, which win over the defaults.
        let at = auth_service::iso((self.created_at as i64).saturating_mul(1000));
        let mut row = self.default_row(&at);
        if let (Some(row), Some(set)) = (row.as_object_mut(), self.row.as_object()) {
            for (key, value) in set {
                row.insert(key.clone(), value.clone());
            }
        }
        row
    }

    fn default_row(&self, at: &str) -> Value {
        created(
            &json!({"createdBy":"user","creationSource":"web","threadId":self.id,"projectId":self.project_id,
            "title":self.title,"modelSelection":null,"runtimeMode":"full-access","interactionMode":"default","worktreePath":self.cwd}),
            at,
        )
    }

    /// `OrchestrationV2ThreadProjection`, as the reference's getThreadProjection returns it.
    /// The model change run `index` brings (`modelSwitchSince`): from the
    /// model of the latest earlier run the timeline shows by its prompt, when
    /// that differs from the run's own.
    fn model_switch(&self, index: usize, run: &crate::Run) -> Option<Value> {
        // A run with a handoff shows the switch by its handoff item.
        let run_id = run.wire_id();
        if self
            .context_handoffs
            .iter()
            .any(|handoff| handoff["targetRunId"] == run_id.as_str() && run_handoff(handoff))
        {
            return None;
        }
        let to = run.model_selection.as_deref()?;
        let previous = self.runs[..index].iter().rev().find(|earlier| {
            !matches!(earlier.status.as_str(), "rolled_back" | "queued")
                && earlier.user_message_id.as_deref().is_some_and(|id| {
                    self.messages.iter().any(|message| {
                        message.id == id
                            && message.role == "user"
                            && !message.id.starts_with("async-answer:")
                            && !(message.content.trim().eq_ignore_ascii_case("/compact")
                                && message_attachments(message)
                                    .as_array()
                                    .is_none_or(Vec::is_empty))
                    })
                })
        })?;
        let from = previous.model_selection.as_deref()?;
        (!crate::model_manifest::selections_equal(from, to))
            .then(|| json!({"from": from, "to": to}))
    }

    pub(crate) fn projection(&self) -> Value {
        self.projection_with(true)
    }

    /// The projection a snapshot gives (`getThreadSnapshotWindow`): a
    /// delegated wake's or a provider continuation's message shows only
    /// while its run is live, since no turn item names it (its card is a
    /// notification).
    pub(crate) fn snapshot_projection(&self) -> Value {
        let mut projection = self.projection();
        window_messages(&mut projection);
        projection
    }

    /// A turn item with everything it holds, bounded for transport, as
    /// `orchestration.getTurnItem` returns it.
    pub(crate) fn turn_item_detail(&self, item_id: &str) -> Option<Value> {
        self.projection_with(false)["turnItems"]
            .as_array()?
            .iter()
            .find(|item| item["id"] == item_id)
            .map(crate::wire_items::detail_item)
    }

    /// The projection; with `wire`, its turn items as snapshots and events
    /// carry them (output omitted).
    fn projection_with(&self, wire: bool) -> Value {
        if let Some(stored) = &self.stored {
            return stored_projection(&self.id, stored.get(), wire, None);
        }
        self.computed_projection(wire)
    }

    /// The projection as `statev2.sqlite` holds it: every record in full.
    pub(crate) fn full_projection(&self) -> Value {
        self.projection_with(false)
    }

    /// The projection this server's own state gives, in full, ignoring any
    /// records read from `statev2.sqlite`.
    pub(crate) fn computed_full_projection(&self) -> Value {
        self.computed_projection(false)
    }

    fn computed_projection(&self, wire: bool) -> Value {
        let row = self.stored_row();
        let array = |key: &str| {
            row.get(key)
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default()
        };
        // The root scope and each checkpoint hang off the run they belong to
        // (the first run for the scope and the baseline), as in the reference.
        let scope_id = checkpoint_scope_id(&self.id);
        let run_ids = |turn_count: u64| {
            let index = turn_count.max(1) as usize - 1;
            self.runs.get(index).map(|run| {
                let ids = self
                    .provider_run_ids(run)
                    .or_else(|| self.queued_run_ids(run))
                    .or_else(|| self.starting_run_ids(run));
                (run.wire_id(), ids)
            })
        };
        let node_of = |turn_count: u64| {
            run_ids(turn_count)
                .and_then(|(_, ids)| ids.map(|ids| ids.root_node))
                .unwrap_or_else(|| format!("node:{}", self.id))
        };
        // The scope exists from the thread's first provider run once it is
        // sent to start, as the reference creates it then (not while the
        // run waits in the queue or for its workspace); else from the
        // baseline checkpoint.
        let first_run = self.runs.first().filter(|run| {
            run.driver.is_some() && !matches!(run.status.as_str(), "queued" | "preparing")
        });
        // Each later run the thread dispatches prepares the scope again
        // (`prepareRootRunScope`), so it belongs to the latest one, queued
        // or not, but not to one still waiting for its workspace.
        let scope_turn = first_run.map_or(1, |_| {
            self.runs
                .iter()
                .rposition(|run| run.driver.is_some() && run.status != "preparing")
                .map_or(1, |index| index as u64 + 1)
        });
        let scope_from = self
            .checkpoints
            .first()
            .map(|checkpoint| (checkpoint.cwd.clone(), checkpoint.captured_at))
            .or_else(|| first_run.map(|_| (self.cwd.clone(), 0)))
            .map(|(cwd, at)| {
                let scope_run = first_run.and(self.runs.get(scope_turn as usize - 1));
                (cwd, scope_run.map_or(at, |run| run.started_at))
            });
        let checkpoint_scopes = scope_from
            .map(|(cwd, created_at)| {
                let first = run_ids(scope_turn);
                vec![json!({
                    "id":scope_id,
                    "threadId":self.id,
                    "runId":first.as_ref().map(|(id, _)| id.clone()),
                    "nodeId":node_of(scope_turn),
                    "parentScopeId":null,
                    "providerThreadId":first.and_then(|(_, ids)| ids.map(|ids| ids.provider_thread)),
                    "kind":"root_run",
                    "ordinalWithinParent":0,
                    "advancesAppRunCount":true,
                    "cwd":cwd,
                    "createdAt":iso(created_at)
                })]
            })
            .unwrap_or_default();
        // The reference records the baseline with the first turn's checkpoint,
        // when a run ends; until then only the scope exists.
        let captured_turn = self.checkpoints.iter().any(|c| c.turn_count > 0);
        let checkpoints = self.checkpoints.iter().enumerate()
            .filter(|(_, checkpoint)| captured_turn || checkpoint.turn_count > 0)
            .map(|(position, checkpoint)| json!({
            "id":checkpoint_id(&self.id, checkpoint.turn_count),
            "threadId":self.id,
            "scopeId":scope_id,
            "runId":if checkpoint.turn_count == 0 { None } else { run_ids(checkpoint.turn_count).map(|(id, _)| id) },
            "nodeId":node_of(checkpoint.turn_count),
            "parentCheckpointId":position.checked_sub(1)
                .map(|previous| checkpoint_id(&self.id, self.checkpoints[previous].turn_count)),
            "ordinalWithinScope":checkpoint.turn_count,
            "appRunOrdinal":if checkpoint.turn_count == 0 {Value::Null} else {json!(checkpoint.turn_count)},
            "ref":checkpoint.tree,
            "status":if checkpoint.ready {"ready"} else {"missing"},
            "files":[],
            "capturedAt":iso(checkpoint.captured_at)
        })).collect::<Vec<_>>();
        let mut provider = self.provider_records();
        // A provider turn that captured a checkpoint ends with its item.
        for checkpoint in self.checkpoints.iter().filter(|c| c.turn_count > 0) {
            let index = checkpoint.turn_count as usize - 1;
            let Some(ids) = self.runs.get(index).and_then(|run| {
                self.provider_run_ids(run)
                    .or_else(|| self.queued_run_ids(run))
            }) else {
                continue;
            };
            let id = checkpoint_id(&self.id, checkpoint.turn_count);
            let at = iso(checkpoint.captured_at);
            provider.items.push((
                index,
                json!({
                    "id": format!(
                        "turn-item:provider:{}:native-item:{}",
                        uri_component(&ids.driver),
                        uri_component(&format!("checkpoint:{id}"))
                    ),
                    "threadId": self.id,
                    "runId": ids.run,
                    "nodeId": ids.root_node,
                    "providerThreadId": ids.provider_thread,
                    "providerTurnId": null,
                    "nativeItemRef": null,
                    "parentItemId": null,
                    "ordinal": 0,
                    "status": "completed",
                    "title": null,
                    "startedAt": at,
                    "completedAt": at,
                    "updatedAt": at,
                    "type": "checkpoint",
                    "checkpointId": id,
                    "scopeId": scope_id,
                    "files": []
                }),
            ));
        }
        let mut turn_items = self.turn_items_with(provider.items);
        // What a native subagent's child thread holds outside any run: its prompt.
        turn_items.splice(0..0, array("nativeTurnItems"));
        if wire {
            // As snapshots serve them: without input still waiting in the
            // queue (the database keeps it).
            turn_items = turn_items
                .iter()
                .filter(|item| {
                    !(item["type"] == "user_message" && item["inputIntent"] == "queued_turn")
                })
                .map(crate::wire_items::wire_item)
                .collect();
        }
        let mut app = ordered(APP_FIELDS, &row);
        if let Some(id) = self.provider_thread_id() {
            app["activeProviderThreadId"] = json!(id);
        }
        let with = |key: &str, extra: Vec<Value>| {
            let mut records = array(key);
            records.extend(extra);
            Value::Array(records)
        };
        let mut messages = array("nativeMessages");
        messages.extend(self.message_rows());
        messages.extend(provider.messages);
        // The reference lists messages by creation time: each run's prompt,
        // then what its provider said, run after run. A message without a
        // run stays with the one before it.
        let mut position = 0;
        let mut keyed: Vec<(usize, Value)> = messages
            .into_iter()
            .map(|message| {
                if let Some(index) = message["runId"]
                    .as_str()
                    .and_then(|id| self.runs.iter().position(|run| run.wire_id() == id))
                {
                    position = index;
                }
                (position, message)
            })
            .collect();
        keyed.sort_by_key(|(index, _)| *index);
        let mut messages: Vec<Value> = keyed.into_iter().map(|(_, message)| message).collect();
        // Rust's message times are whole seconds, so a steer goes right
        // after the message that was last when it arrived.
        for message in self.messages.iter().filter(|m| m.steer.is_some()) {
            let Some(from) = messages.iter().position(|m| m["id"] == message.id.as_str()) else {
                continue;
            };
            let row = messages.remove(from);
            let after = message.steer.as_ref().map(|s| &s["afterMessageId"]);
            let at = after
                .and_then(|after| messages.iter().position(|m| &m["id"] == after))
                .map_or(messages.len(), |index| index + 1);
            messages.insert(at, row);
        }
        // A fork shows its source's conversation up to the fork, then its
        // marker, ahead of its own items (`buildVisibleTurnItems`).
        let visible = self
            .inherited_items
            .iter()
            .map(|row| {
                let mut row = row.clone();
                if wire {
                    row["item"] = crate::wire_items::wire_item(&row["item"]);
                }
                row
            })
            .chain(turn_items.iter().map(|item| {
                json!({"visibility":"local","sourceThreadId":self.id,
                    "sourceItemId":item["id"],"item":item})
            }))
            .enumerate()
            .map(|(position, mut row)| {
                row["position"] = json!(position);
                row
            })
            .collect::<Vec<_>>();
        let context_handoffs: Vec<Value> = if wire {
            self.context_handoffs
                .iter()
                .map(crate::context_handoff::project_for_wire)
                .collect()
        } else {
            self.context_handoffs.clone()
        };
        // Transfers this server recorded, after those the stored thread had.
        let mut context_transfers = array("contextTransfers");
        for transfer in &self.context_transfers {
            if !context_transfers.iter().any(|t| t["id"] == transfer["id"]) {
                context_transfers.push(transfer.clone());
            }
        }
        let mut projection = json!({"thread":app,"runs":self.run_rows(&row),"attempts":with("attempts",provider.attempts),"nodes":with("nodes",provider.nodes),"subagents":with("subagents",provider.subagents),
            "providerSessions":with("providerSessions",provider.provider_sessions),"providerThreads":with("providerThreads",provider.provider_threads),"providerTurns":with("providerTurns",provider.provider_turns),"runtimeRequests":with("runtimeRequests",provider.runtime_requests),
            "messages":messages,"plans":provider.plans,"turnItems":turn_items,"checkpointScopes":checkpoint_scopes,"checkpoints":checkpoints,
            "contextHandoffs":context_handoffs,"contextTransfers":context_transfers,"visibleTurnItems":visible,"updatedAt":row["updatedAt"]});
        sort_messages_by_turn_item_order(&mut projection, None);
        projection
    }

    /// The thread's whole timeline as `OrchestrationV2ThreadTranscript` items.
    pub(crate) fn transcript(&self) -> Value {
        let row = self.stored_row();
        json!({"threadId":self.id,"title":self.title,"updatedAt":row["updatedAt"],
            "items":self.projection()["visibleTurnItems"]})
    }

    /// One `snapshot` item of `orchestration.subscribeThread`, shaped by the options the
    /// client sent as the reference's ThreadStream shapes it. A bounded snapshot holds only the
    /// newest window of the timeline and a cursor to the turns before it.
    pub(crate) fn stream_snapshot(&self, sequence: u64, options: &Value) -> Value {
        self.stream_snapshot_of(self.projection(), sequence, options)
    }

    /// [`Thread::stream_snapshot`] from `projection`, this thread's
    /// [`Thread::projection`], which the caller already built.
    pub(crate) fn stream_snapshot_of(
        &self,
        projection: Value,
        sequence: u64,
        options: &Value,
    ) -> Value {
        let mut projection = projection;
        window_messages(&mut projection);
        if options["acceptBoundedSnapshot"] != true {
            return json!({"kind":"snapshot","snapshotSequence":sequence,"projection":projection});
        }
        // A message waiting in the queue keeps its place in the timeline.
        let queued = self
            .runs
            .iter()
            .enumerate()
            .filter(|(_, run)| queued(run))
            .map(|(index, _)| (index as u64 + 1) * 1_000_000 + 1);
        let latest_local_turn_ordinal = projection["turnItems"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|item| item["ordinal"].as_u64())
            .chain(queued)
            .max();
        // Only the newest window of the timeline, with a cursor to the turns before it, as the
        // reference's getThreadSnapshotWindow and buildBoundedThreadProjection give it.
        let (bounded, history_cursor, has_more_history) =
            crate::thread_history::bounded_snapshot(&projection, sequence);
        projection = bounded;
        if options["acceptDedupedBodies"] == true {
            dedupe_bodies(&mut projection);
        }
        let mut omitted = false;
        if options["acceptCompactTurnItems"] == true {
            // Every turn item is a local visible row, so the rows alone restore them.
            let rows = projection["visibleTurnItems"]
                .as_array()
                .map_or(0, Vec::len);
            if rows > 0 {
                let items = projection["turnItems"].as_array_mut().expect("turn items");
                items.drain(..rows.min(items.len()));
                omitted = true;
            }
        }
        // The reference's field order (ThreadStream.boundedSnapshotResponseFields).
        let mut item = json!({"kind":"snapshot","snapshotSequence":sequence,"projection":projection,
            "historyCursor":history_cursor,"hasMoreHistory":has_more_history,"latestLocalTurnOrdinal":latest_local_turn_ordinal});
        if omitted {
            item["turnItemsOmitLocalVisible"] = json!(true);
        }
        item
    }

    /// The thread's `OrchestrationV2ThreadShell` row, derived from its
    /// projection as the reference's `threadShellFromProjection` derives it,
    /// so a thread lists the same on both servers.
    pub(crate) fn shell(&self) -> Value {
        if let Some(slim) = self.stored.as_ref().and_then(|s| s.slim_for_shell()) {
            return slim_shell(&self.id, &slim);
        }
        crate::projection_shell::shell(&self.projection(), SHELL_FIELDS)
    }
}

fn field<'a>(row: &'a Value, key: &str) -> &'a str {
    row[key].as_str().unwrap_or_default().trim()
}

pub(crate) fn same_link(a: &Value, b: &Value, issue: bool) -> bool {
    field(a, "host").eq_ignore_ascii_case(field(b, "host"))
        && if issue {
            a["tracker"] == b["tracker"]
                && field(a, "scope").eq_ignore_ascii_case(field(b, "scope"))
                && field(a, "key") == field(b, "key")
        } else {
            field(a, "repository").eq_ignore_ascii_case(field(b, "repository"))
                && a["number"] == b["number"]
        }
}

pub(crate) fn link_patch(thread: &Thread, cmd: &Value, at: &str) -> Value {
    let kind = field(cmd, "type");
    let issue = kind.starts_with("thread.issue.");
    let key = if issue { "issues" } else { "pullRequests" };
    let mut links = thread.row[key].as_array().cloned().unwrap_or_default();
    let i = links.iter().position(|l| same_link(l, cmd, issue));
    if kind.ends_with(".unlink") {
        let Some(i) = i else {
            return json!({});
        };
        if !issue && (links[i]["source"] == "stack" || !links[i]["stack"].is_null()) {
            links[i]["source"] = "stack-dismissed".into();
            links[i].as_object_mut().unwrap().shift_remove("watch");
        } else {
            links.remove(i);
        }
    } else if kind == "thread.pull-request-link.sync" {
        let Some(i) = i else {
            return json!({});
        };
        links[i]["snapshot"] = cmd["snapshot"].clone();
        links[i]["stack"] = cmd["stack"].clone();
    } else if let Some(i) = i {
        if issue || links[i]["source"] != "stack-dismissed" || cmd["source"] == "stack" {
            return json!({});
        }
        links[i]["source"] = cmd["source"].clone();
        links[i]["url"] = cmd["url"].clone();
    } else if issue {
        links.push(json!({"tracker":cmd["tracker"],"host":field(cmd,"host").to_lowercase(),
            "scope":field(cmd,"scope"),"key":field(cmd,"key"),"projectId":cmd["projectId"],
            "url":cmd["url"],"source":cmd["source"],"linkedAt":at,"displayKey":cmd["displayKey"],"title":cmd["title"]}));
    } else {
        let mut link = json!({"host":field(cmd,"host").to_lowercase(),"repository":field(cmd,"repository").to_lowercase(),"number":cmd["number"],"url":cmd["url"]});
        if let Some(url) = cmd.get("repositoryWebUrl") {
            link["repositoryWebUrl"] = url.clone();
        }
        link["source"] = cmd["source"].clone();
        link["linkedAt"] = json!(at);
        link["snapshot"] = Value::Null;
        link["stack"] = Value::Null;
        links.push(link);
    }
    let mut patch = Map::new();
    patch.insert(key.into(), Value::Array(links));
    if !issue {
        patch.insert("linkedPullRequest".into(), Value::Null);
    }
    if !kind.ends_with(".sync") {
        patch.insert("updatedAt".into(), json!(at));
    }
    Value::Object(patch)
}

pub(crate) fn watch_patch(thread: &Thread, cmd: &Value, at: &str) -> Value {
    let mut links = thread.row["pullRequests"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    let host = field(cmd, "host");
    let repository = field(cmd, "repository");
    let number = cmd["number"].clone();
    let matches = |link: &Value| {
        field(link, "host").eq_ignore_ascii_case(host)
            && field(link, "repository").eq_ignore_ascii_case(repository)
            && link["number"] == number
            && link["source"] != "stack-dismissed"
    };
    let key_matches = |link: &Value| {
        field(link, "host").eq_ignore_ascii_case(host)
            && field(link, "repository").eq_ignore_ascii_case(repository)
            && link["number"] == number
    };
    let mut index = links.iter().position(matches);
    if cmd["watching"] == true {
        if index.is_none() {
            let Some(link) = cmd.get("link").cloned() else {
                return json!({});
            };
            let ended_watch = links
                .iter()
                .find(|candidate| {
                    key_matches(candidate) && candidate["source"] == "stack-dismissed"
                })
                .and_then(|candidate| candidate.get("endedWatch").cloned());
            links.retain(|candidate| !key_matches(candidate));
            let mut created = json!({
                "host": host.to_lowercase(),
                "repository": repository.to_lowercase(),
                "number": number,
                "url": link["url"],
                "source": link["source"],
                "linkedAt": at,
                "snapshot": Value::Null,
                "stack": Value::Null
            });
            if let Some(repository_web_url) = link.get("repositoryWebUrl") {
                created["repositoryWebUrl"] = repository_web_url.clone();
            }
            if let Some(ended_watch) = ended_watch {
                created["endedWatch"] = ended_watch;
            }
            links.push(created);
            index = Some(links.len() - 1);
        }
        let index = index.expect("watch link index exists");
        if links[index].get("watch").is_some() {
            return json!({});
        }
        let previous = links[index].get("endedWatch").cloned();
        let mut watch = previous.unwrap_or_else(|| {
            json!({
                "startedAt": at,
                "headSha": Value::Null,
                "failedChecks": [],
                "passed": false,
                "passedChecks": [],
                "remarksThrough": at,
                "remarkIds": [],
                "conflicting": false,
                "wakes": 0
            })
        });
        watch["startedAt"] = json!(at);
        watch["remarksThrough"] = json!(at);
        watch["remarkIds"] = json!([]);
        watch["wakes"] = json!(0);
        let pending = cmd["checksPending"].as_bool().unwrap_or_else(|| {
            links[index]["snapshot"]["checksState"] == "pending" || watch["checksPending"] == true
        });
        if pending {
            watch["checksPending"] = json!(true);
        } else {
            watch.as_object_mut().unwrap().remove("checksPending");
        }
        if let Some(object) = links[index].as_object_mut() {
            object.remove("endedWatch");
            object.insert("watch".into(), watch);
        }
    } else if let Some(index) = index {
        let Some(link) = links[index].as_object_mut() else {
            return json!({});
        };
        let Some(previous) = link.remove("watch") else {
            return json!({});
        };
        link.insert("endedWatch".into(), previous);
    } else {
        return json!({});
    }
    json!({"pullRequests":links,"updatedAt":at})
}

pub(crate) fn linked(store: &Store, input: &Value, issue: bool) -> Value {
    let key = if issue { "issues" } else { "pullRequests" };
    let mut rows: Vec<_> = store
        .threads
        .values()
        .filter(|t| {
            t.row["lineage"]["sideChat"] != true
                && t.row[key]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .any(|l| l["source"] != "stack-dismissed" && same_link(l, input, issue))
        })
        .collect();
    rows.sort_by(|a, b| {
        field(&b.row, "updatedAt")
            .cmp(field(&a.row, "updatedAt"))
            .then_with(|| a.id.cmp(&b.id))
    });
    json!({"threads":rows.into_iter().map(|t|json!({"id":t.id,"projectId":t.project_id,"title":t.title,"archivedAt":t.row["archivedAt"],"settled":t.settled})).collect::<Vec<_>>()})
}

/// The projection of a thread read from `statev2.sqlite`: its stored records,
/// with turn items as snapshots carry them when `wire`, and each turn item a
/// local visible row, as Rust's own projections are.
/// The projection `statev2.sqlite` holds for `stored` (a thread's stored
/// records, whole or cut down to a commit's change), as
/// [`Thread::full_projection`] gives it for a stored thread.
pub(crate) fn database_projection(thread_id: &str, stored: &Value) -> Value {
    stored_projection(thread_id, stored, false, None)
}

/// The projection subscribers get for `stored`, as [`Thread::projection`]
/// gives it for a stored thread.
pub(crate) fn wire_projection(thread_id: &str, stored: &Value) -> Value {
    stored_projection(thread_id, stored, true, None)
}

fn stored_projection(
    thread_id: &str,
    stored: &Value,
    wire: bool,
    message_ordinals: Option<&std::collections::HashMap<String, i64>>,
) -> Value {
    let mut projection = stored.clone();
    // The thread's fields in the contract's order, as the reference encodes
    // it; a change made here adds its fields at the end of the stored record.
    projection["thread"] = ordered(APP_FIELDS, &stored["thread"]);
    sort_messages_by_turn_item_order(&mut projection, message_ordinals);
    if wire {
        // As the reference's snapshot window reads them: without a rolled
        // back run's items or input still waiting in the queue. (Only when
        // serving: the full projection is what the database holds.)
        let items: Vec<Value> = crate::projection_shell::visible_turn_items(&projection)
            .into_iter()
            .map(crate::wire_items::wire_item)
            .collect();
        projection["turnItems"] = Value::Array(items);
        if let Some(handoffs) = projection["contextHandoffs"].as_array() {
            projection["contextHandoffs"] = Value::Array(
                handoffs
                    .iter()
                    .map(crate::context_handoff::project_for_wire)
                    .collect(),
            );
        }
    }
    let visible = crate::projection_shell::visible_turn_items(&projection)
        .into_iter()
        .enumerate()
        .map(|(position, item)| {
            json!({"position":position,"visibility":"local","sourceThreadId":thread_id,
                "sourceItemId":item["id"],"item":item})
        })
        .collect::<Vec<_>>();
    // As the reference reads it: the thread's own `updatedAt`.
    let updated_at = projection["thread"]["updatedAt"].clone();
    if let Some(object) = projection.as_object_mut() {
        // The reference's field order (OrchestrationV2ThreadProjection).
        object.remove("updatedAt");
        object.insert("visibleTurnItems".into(), Value::Array(visible));
        object.insert("updatedAt".into(), updated_at);
    }
    projection
}

/// The shell row of a thread whose slim records alone are loaded
/// (`stored_projection::Slim`): derived from them as from the whole
/// projection, since they hold every record the shell looks at, with its
/// item counts adding the turn items they leave out. Those are never hidden
/// by the visibility rule except as a rolled-back run's (the slim records
/// hold every queued input and stop result).
fn slim_shell(thread_id: &str, slim: &crate::stored_projection::SlimView<'_>) -> Value {
    let projection = stored_projection(
        thread_id,
        slim.projection,
        true,
        Some(slim.message_ordinals),
    );
    let mut shell = crate::projection_shell::shell(&projection, SHELL_FIELDS);
    let mut rolled_back = std::collections::HashSet::new();
    for run in projection["runs"].as_array().into_iter().flatten() {
        let id = run["id"].as_str().unwrap_or_default();
        if run["status"] == "rolled_back" {
            rolled_back.insert(id);
        } else {
            rolled_back.remove(id);
        }
    }
    let unloaded: u64 = slim
        .unloaded_items
        .iter()
        .filter(|(run, _)| !run.as_deref().is_some_and(|run| rolled_back.contains(run)))
        .map(|(_, count)| count)
        .sum();
    for key in ["itemCount", "visibleItemCount"] {
        if let Some(count) = shell[key].as_u64() {
            shell[key] = json!(count + unloaded);
        }
    }
    shell
}

/// Seconds since the epoch of an ISO timestamp, 0 when it does not parse.
fn iso_seconds(value: &Value) -> u64 {
    value
        .as_str()
        .and_then(|text| {
            time::OffsetDateTime::parse(text, &time::format_description::well_known::Rfc3339).ok()
        })
        .map_or(0, |at| at.unix_timestamp().max(0) as u64)
}

impl Thread {
    /// A thread read from `statev2.sqlite` (`projection` as
    /// `statev2::read::thread_projection` returns it). Its snapshots serve the
    /// stored records; the typed fields hold what listing and commands read.
    pub(crate) fn from_stored(projection: Value, project_root: Option<&str>) -> Thread {
        let mut thread = Self::typed_from_stored(&projection, project_root);
        thread.stored = Some(crate::stored_projection::Stored::whole(projection));
        thread
    }

    /// A thread read from `statev2.sqlite` whose slim records alone are
    /// loaded (`statev2::read::slim_projections`): the typed state comes from
    /// its small collections, which the slim records hold whole; the whole
    /// projection is read from `source` when first needed.
    pub(crate) fn from_slim(
        slim: crate::stored_projection::Slim,
        project_root: Option<&str>,
        source: std::sync::Arc<dyn crate::stored_projection::Hydrate>,
    ) -> Thread {
        let mut thread = Self::typed_from_stored(&slim.projection, project_root);
        let mut slim = slim;
        slim.keep_only_shell_records();
        thread.stored = Some(crate::stored_projection::Stored::lazy(
            thread.id.clone(),
            slim,
            source,
        ));
        thread
    }

    /// The typed state of a thread read from `statev2.sqlite`. It reads no
    /// nodes or turn items.
    fn typed_from_stored(projection: &Value, project_root: Option<&str>) -> Thread {
        let row = projection["thread"].clone();
        let text = |key: &str| row[key].as_str().map(str::to_owned);
        let active = projection["runs"]
            .as_array()
            .into_iter()
            .flatten()
            .any(|run| {
                matches!(
                    run["status"].as_str(),
                    Some("preparing" | "starting" | "running" | "waiting")
                )
            });
        // As this server keeps its own threads' messages: what was sent to
        // the thread (users' messages, and the notifications that continue a
        // run). Provider messages are the provider turns' records.
        let messages = projection["messages"]
            .as_array()
            .into_iter()
            .flatten()
            .filter(|message| message["role"] == "user")
            .map(|message| crate::ConversationMessage {
                id: message["id"].as_str().unwrap_or_default().to_owned(),
                role: if message["notification"].is_object() {
                    NOTIFICATION_ROLE
                } else {
                    "user"
                }
                .to_owned(),
                content: message["text"].as_str().unwrap_or_default().to_owned(),
                created_at: iso_seconds(&message["createdAt"]),
                creation_source: message["creationSource"].as_str().map(str::to_owned),
                scheduled_task_id: message["scheduledTaskId"].as_str().map(str::to_owned),
                follows_thread_selection: message["followsThreadSelection"] == true,
                notification: None,
                steer: None,
                wire: None,
            })
            .collect();
        let thread_id = text("id").unwrap_or_default();
        let runs = stored_runs(&thread_id, projection);
        let checkpoints = stored_checkpoints(projection);
        let context_handoffs = projection["contextHandoffs"]
            .as_array()
            .cloned()
            .unwrap_or_default();
        let provider = Box::new(stored_provider(&thread_id, &row, projection));
        Thread {
            id: thread_id,
            project_id: text("projectId").unwrap_or_default(),
            title: text("title").unwrap_or_default(),
            cwd: text("worktreePath")
                .or_else(|| project_root.map(str::to_owned))
                .unwrap_or_default(),
            status: if active { "running" } else { "idle" }.into(),
            messages,
            runs,
            created_at: iso_seconds(&row["createdAt"]),
            archived: !row["archivedAt"].is_null(),
            settled: !row["settledAt"].is_null(),
            snoozed_until: text("snoozedUntil"),
            pinned: !row["pinnedAt"].is_null(),
            unread: false,
            tags: row["tags"]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(|tag| tag.as_str().map(str::to_owned))
                .collect(),
            checkpoints,
            link_requests: Vec::new(),
            context_handoffs,
            context_transfers: Vec::new(),
            handoff_items: Vec::new(),
            inherited_items: Vec::new(),
            provider,
            stored: None,
            revision: 0,
            row,
        }
    }
}

/// The typed runs of a thread read from the shared database, in ordinal
/// order, each keeping its wire id, so this server can queue, start,
/// continue and stop runs on it as on its own threads.
fn stored_runs(thread_id: &str, projection: &Value) -> Vec<crate::Run> {
    let id_of = |wire: &str| crate::Run::uuid_for(wire);
    projection["runs"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|run| {
            let wire = run["id"].as_str()?.to_owned();
            let started = if run["startedAt"].is_null() {
                &run["requestedAt"]
            } else {
                &run["startedAt"]
            };
            Some(crate::Run {
                id: id_of(&wire),
                thread_id: thread_id.to_owned(),
                provider: "default".into(),
                status: run["status"].as_str().unwrap_or("failed").to_owned(),
                started_at: iso_seconds(started),
                finished_at: (!run["completedAt"].is_null())
                    .then(|| iso_seconds(&run["completedAt"])),
                error: None,
                user_message_id: run["userMessageId"].as_str().map(str::to_owned),
                queue_held: run["queueHeld"] == true,
                restart_cut: None,
                restart_continuation_of: run["restartContinuationOfRunId"].as_str().map(id_of),
                requeued_at: None,
                background_work: run["restartCancelledBackgroundWork"]
                    .as_array()
                    .cloned()
                    .unwrap_or_default(),
                continuation: None,
                resend_owed: false,
                resends: 0,
                // A run this server made keeps its own (UUID) id.
                wire_id: uuid::Uuid::parse_str(&wire).is_err().then_some(wire),
                driver: None,
                model_selection: None,
                output_schema: run["outputSchema"]
                    .is_object()
                    .then(|| Box::new(run["outputSchema"].clone())),
                fresh_context: run["freshContext"] == true,
            })
        })
        .collect()
}

/// The checkpoints of a thread read from the shared database.
fn stored_checkpoints(projection: &Value) -> Vec<crate::CheckpointRecord> {
    let scopes = projection["checkpointScopes"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    projection["checkpoints"]
        .as_array()
        .into_iter()
        .flatten()
        .map(|checkpoint| crate::CheckpointRecord {
            turn_count: checkpoint["appRunOrdinal"].as_u64().unwrap_or(0),
            tree: checkpoint["ref"].as_str().unwrap_or_default().to_owned(),
            cwd: scopes
                .iter()
                .find(|scope| scope["id"] == checkpoint["scopeId"])
                .and_then(|scope| scope["cwd"].as_str())
                .unwrap_or_default()
                .to_owned(),
            captured_at: iso_seconds(&checkpoint["capturedAt"]),
            ready: checkpoint["status"] == "ready",
        })
        .collect()
}

/// What a thread read from the shared database tells the provider runtime:
/// its provider thread record (named after the run that first used it) and
/// the native thread the next turn resumes.
fn stored_provider(
    thread_id: &str,
    row: &Value,
    projection: &Value,
) -> crate::provider_state::ProviderState {
    let mut state = crate::provider_state::ProviderState::default();
    let threads = projection["providerThreads"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    let record = threads
        .iter()
        .find(|t| t["id"] == row["activeProviderThreadId"])
        .or_else(|| {
            threads
                .iter()
                .filter(|t| t["appThreadId"] == thread_id && t["ownerNodeId"].is_null())
                .max_by_key(|t| iso_seconds(&t["updatedAt"]))
        });
    let Some(record) = record else {
        return state;
    };
    let driver = record["driver"].as_str().unwrap_or("codex").to_owned();
    let native = &record["nativeThreadRef"];
    let native_id = native["nativeId"].as_str().map(str::to_owned);
    if let Some(id) = native_id.clone().filter(|_| native["strength"] == "strong") {
        state.native_thread = Some(crate::provider_state::NativeThread {
            driver: driver.clone(),
            id,
        });
    }
    // `provider-thread:provider:<driver>:native-thread:pending:<run id>`.
    let first_run = record["id"]
        .as_str()
        .and_then(|id| id.split_once(":native-thread:"))
        .map(|(_, rest)| {
            percent_encoding::percent_decode_str(rest)
                .decode_utf8_lossy()
                .into_owned()
        })
        .and_then(|rest| rest.strip_prefix("pending:").map(str::to_owned));
    let turns = projection["providerTurns"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|turn| turn["providerThreadId"] == record["id"])
        .filter_map(|turn| turn["ordinal"].as_u64())
        .max()
        .unwrap_or(0) as usize;
    if let Some(first) = record["firstRunOrdinal"].as_u64() {
        state.prior_turns = Some((turns, first as usize));
    }
    let instance = row["providerInstanceId"].clone();
    let session = projection["providerSessions"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|session| session["providerInstanceId"] == instance)
        .max_by_key(|session| iso_seconds(&session["updatedAt"]));
    state.prior_session_id = session.and_then(|session| session["id"].as_str().map(str::to_owned));
    // No session of the thread is live when the database holds none, or its
    // latest is stopped: a restart then has none to stop, as the reference's
    // recovery selects only threads with a live session.
    match session {
        Some(session) if session["status"] == "stopped" => {
            state.session_stopped_at = Some(iso_seconds(&session["updatedAt"]));
        }
        None => state.session_stopped_at = Some(iso_seconds(&record["updatedAt"])),
        Some(_) => {}
    }
    state.prior_usage = projection["providerTurns"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|turn| turn["providerThreadId"] == record["id"])
        .max_by_key(|turn| turn["ordinal"].as_u64())
        .and_then(|turn| {
            let usage = &turn["tokenUsage"];
            Some((
                record["nativeThreadRef"]["nativeId"]
                    .as_str()
                    .map(str::to_owned),
                usage["usedTokens"].as_u64()?,
                usage["maxTokens"].as_u64(),
            ))
        });
    let budget = &record["nativeMetadata"]["claudeAttachmentBudget"];
    if let (Some(bytes), Some(pages)) = (budget["bytesUsed"].as_u64(), budget["pagesUsed"].as_u64())
    {
        state.attachment_budget = Some((bytes, pages));
    }
    // Only an OpenCode 2 thread's record names the session's model.
    let opencode2 = (driver == crate::workspace::provider::OPENCODE
        && record["nativeMetadata"]["modelSelection"].is_object())
    .then(|| crate::workspace::provider::OpenCode2Thread {
        version: String::new(),
        model_selection: record["nativeMetadata"]["modelSelection"].clone(),
    });
    // fx's turns are the ones whose sessions report these (Rust's other ACP
    // flavors do not read them yet).
    let fx = driver == crate::workspace::provider::FX;
    if let Some(first_run) = first_run {
        state.thread = Some(crate::provider_state::ThreadRecord {
            opencode2,
            // An ACP agent's session title (`nativeMetadata.title`).
            native_title: record["nativeMetadata"]["title"]
                .as_str()
                .map(str::to_owned),
            // And what fx itself reported of its session: when it changed
            // it, and its context usage (its `usage_update`).
            native_updated_at: fx
                .then(|| record["nativeMetadata"]["updatedAt"].as_str())
                .flatten()
                .map(str::to_owned),
            context_usage: fx
                .then(|| {
                    Some(crate::workspace::provider::ContextUsage {
                        used_tokens: record["contextUsage"]["usedTokens"].as_u64()?,
                        max_tokens: record["contextUsage"]["maxTokens"].as_u64(),
                    })
                })
                .flatten(),
            driver,
            first_run: crate::Run::uuid_for(&first_run),
            created_at: iso_seconds(&record["createdAt"]),
            updated_at: iso_seconds(&record["updatedAt"]),
            native_id,
            conversation_head: record["nativeConversationHeadRef"]["nativeId"]
                .as_str()
                .map(str::to_owned),
            background_tasks: record["pendingBackgroundTasks"]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(|task| {
                    let owner = crate::Run::uuid_for(task["runId"].as_str()?);
                    let mut task = task.clone();
                    let fields = task.as_object_mut()?;
                    for key in ["runId", "itemId", "sourceThreadId"] {
                        fields.remove(key);
                    }
                    Some((task, owner))
                })
                .collect(),
            status: match record["status"].as_str() {
                Some("active") => "active",
                Some("idle") => "idle",
                Some("error") => "error",
                _ => "not_loaded",
            },
            attachment_budget: None,
        });
    }
    state
}

/// The reference's `sortMessagesByTurnItemOrder`: messages in the order of
/// the first user or assistant turn item that shows each, then by creation
/// time and id.
fn sort_messages_by_turn_item_order(
    projection: &mut Value,
    known: Option<&std::collections::HashMap<String, i64>>,
) {
    let mut ordinals: std::collections::HashMap<String, i64> = std::collections::HashMap::new();
    if let Some(known) = known {
        ordinals.clone_from(known);
    }
    for item in projection["turnItems"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|_| known.is_none())
    {
        if !matches!(
            item["type"].as_str(),
            Some("user_message" | "assistant_message")
        ) {
            continue;
        }
        let (Some(message), Some(ordinal)) = (item["messageId"].as_str(), item["ordinal"].as_i64())
        else {
            continue;
        };
        let entry = ordinals.entry(message.to_owned()).or_insert(ordinal);
        *entry = (*entry).min(ordinal);
    }
    let millis = |value: &Value| {
        value
            .as_str()
            .and_then(|text| {
                time::OffsetDateTime::parse(text, &time::format_description::well_known::Rfc3339)
                    .ok()
            })
            .map_or(0, |at| at.unix_timestamp_nanos() / 1_000_000)
    };
    if let Some(messages) = projection["messages"].as_array_mut() {
        messages.sort_by(|left, right| {
            let ordinal = |m: &Value| {
                m["id"]
                    .as_str()
                    .and_then(|id| ordinals.get(id))
                    .copied()
                    .unwrap_or(i64::MAX)
            };
            ordinal(left)
                .cmp(&ordinal(right))
                // Rust stores message times at second precision. Keep the
                // recorded order for equal timestamps, as the reference's
                // millisecond timestamps do for messages created together.
                .then_with(|| millis(&left["createdAt"]).cmp(&millis(&right["createdAt"])))
        });
    }
}

/// Whether `handoff` is the one its target run names (`contextHandoffId`):
/// a merge-back's delta or a provider switch's handoff, prepared for the run.
fn run_handoff(handoff: &Value) -> bool {
    handoff["strategy"] == "fork_delta_summary"
        || handoff["transferId"]
            .as_str()
            .is_some_and(|id| id.starts_with("context-transfer:type:provider_handoff:"))
}

#[cfg(test)]
mod stored_tests {
    use super::*;

    #[test]
    fn a_stored_thread_gets_the_typed_state_to_run_and_continue_turns() {
        let pending =
            "provider-thread:provider:codex:native-thread:pending%3Arun%3Athread%3At%3Aordinal%3A1";
        let projection = json!({
            "thread": {"id": "t", "projectId": "p", "title": "T", "providerInstanceId": "codex",
                "activeProviderThreadId": pending, "createdAt": "2026-01-01T00:00:00.000Z", "archivedAt": null},
            "runs": [{"id": "run:thread:t:ordinal:1", "status": "cancelled", "requestedAt": "2026-01-01T00:00:01.000Z",
                "startedAt": "2026-01-01T00:00:01.000Z", "completedAt": "2026-01-01T00:00:05.000Z", "userMessageId": "m1"}],
            "messages": [
                {"id": "m1", "role": "user", "text": "hi", "createdAt": "2026-01-01T00:00:01.000Z"},
                {"id": "m2", "role": "assistant", "text": "hello", "createdAt": "2026-01-01T00:00:02.000Z"}],
            "providerThreads": [{"id": pending, "driver": "codex", "appThreadId": "t", "ownerNodeId": null,
                "nativeThreadRef": {"driver": "codex", "nativeId": "native-1", "strength": "strong"},
                "status": "idle", "firstRunOrdinal": 1, "createdAt": "2026-01-01T00:00:01.000Z", "updatedAt": "2026-01-01T00:00:05.000Z"}],
            "providerTurns": [{"id": "turn-1", "providerThreadId": pending, "ordinal": 1,
                "tokenUsage": {"usedTokens": 100, "maxTokens": 1000}}],
            "providerSessions": [{"id": "provider-session:provider-instance:codex:shared", "providerInstanceId": "codex",
                "updatedAt": "2026-01-01T00:00:05.000Z"}],
            "checkpoints": [], "checkpointScopes": [], "turnItems": []
        });
        let thread = Thread::from_stored(projection, Some("/work"));
        assert_eq!(thread.runs.len(), 1);
        assert_eq!(thread.runs[0].wire_id(), "run:thread:t:ordinal:1");
        assert_eq!(
            thread.runs[0].id,
            crate::Run::uuid_for("run:thread:t:ordinal:1")
        );
        assert_eq!(thread.runs[0].status, "cancelled");
        // Only what was sent to the thread; provider messages are provider records.
        assert_eq!(thread.messages.len(), 1);
        let provider = &thread.provider;
        assert_eq!(
            provider.native_thread.as_ref().map(|n| n.id.as_str()),
            Some("native-1")
        );
        assert_eq!(
            provider.thread.as_ref().map(|t| t.first_run),
            Some(thread.runs[0].id)
        );
        assert_eq!(provider.prior_turns, Some((1, 1)));
        assert_eq!(
            provider.prior_session_id.as_deref(),
            Some("provider-session:provider-instance:codex:shared")
        );
        assert_eq!(
            provider.prior_usage,
            Some((Some("native-1".into()), 100, Some(1000)))
        );
        // The provider thread record keeps the reference's id.
        assert_eq!(thread.provider_thread_id().as_deref(), Some(pending));
    }
}

#[cfg(test)]
mod sender_note_tests {
    use super::*;

    #[test]
    fn a_message_nobody_typed_says_who_sent_it() {
        let agent =
            json!({"createdBy": "agent", "creationSource": "mcp", "senderThreadId": "thread:a"});
        assert_eq!(
            with_message_sender_note(&agent, "Hi"),
            "[T3 Code: the agent in T3 thread \"thread:a\" sent this message, not the user.]\n\nHi"
        );
        let tool = json!({"createdBy": "agent", "creationSource": "mcp"});
        assert!(
            with_message_sender_note(&tool, "").starts_with("[T3 Code: this message came through")
        );
        let scheduled = json!({"createdBy": "user", "scheduledTaskId": "task:1"});
        assert!(with_message_sender_note(&scheduled, "Go")
            .starts_with("[T3 Code: scheduled task \"task:1\""));
        // Commands and skill mentions stay as sent, and a person's text is unchanged.
        assert_eq!(with_message_sender_note(&agent, " /compact"), " /compact");
        assert_eq!(
            with_message_sender_note(&agent, "$deploy now"),
            "$deploy now"
        );
        assert_eq!(
            with_message_sender_note(&json!({"createdBy": "user"}), "Hi"),
            "Hi"
        );
    }
}
