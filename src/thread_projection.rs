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
/// item carries `textOmitted` instead and the client restores the text from the message.
fn dedupe_bodies(projection: &mut Value) {
    let texts: std::collections::HashMap<String, String> = projection["messages"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|m| Some((m["id"].as_str()?.to_owned(), m["text"].as_str()?.to_owned())))
        .collect();
    let dedupe = |item: &mut Value| {
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
    json!({"createdBy":command["createdBy"],"creationSource":command["creationSource"],
        "id":command["threadId"],"projectId":command["projectId"],"title":command["title"],
        "providerInstanceId":command["modelSelection"]["instanceId"],"modelSelection":command["modelSelection"],
        "runtimeMode":command["runtimeMode"],"interactionMode":command["interactionMode"],
        "branch":command["branch"],"worktreePath":command["worktreePath"],"pullRequests":[],
        "activeProviderThreadId":null,"lineage":{"parentThreadId":null,"relationshipToParent":null,"rootThreadId":command["threadId"]},
        "forkedFrom":null,"createdAt":at,"updatedAt":at,"archivedAt":null,"settledOverride":null,
        "settledAt":null,"snoozedUntil":null,"snoozedAt":null,"lastVisitedAt":null,"deletedAt":null})
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
                let status = run_status(&run.status);
                let waiting = matches!(status, "queued" | "preparing");
                let requested_at = self
                    .messages
                    .iter()
                    .find(|message| Some(&message.id) == run.user_message_id.as_ref())
                    .map_or(run.started_at, |message| message.created_at);
                let mut out = json!({
                    "id": run.wire_id(),
                    "threadId": self.id,
                    "ordinal": index + 1,
                    "providerInstanceId": instance,
                    "modelSelection": selection,
                    "providerThreadId": null,
                    "userMessageId": run.user_message_id.clone().filter(|id| !id.trim().is_empty()).unwrap_or_else(|| run.wire_id()),
                    "rootNodeId": null,
                    "activeAttemptId": null,
                    "status": status,
                    "queuePosition": null,
                    "requestedAt": iso(requested_at),
                    "startedAt": if waiting { Value::Null } else { iso(run.started_at).into() },
                    "completedAt": run.finished_at.map(iso),
                    "checkpointId": null,
                    "contextHandoffId": null
                });
                let turn_count = index as u64 + 1;
                if self.checkpoints.iter().any(|c| c.turn_count == turn_count) {
                    out["checkpointId"] = json!(checkpoint_id(&self.id, turn_count));
                }
                if let Some(ids) = self.provider_run_ids(run) {
                    out["providerThreadId"] = json!(ids.provider_thread);
                    out["rootNodeId"] = json!(ids.root_node);
                    out["activeAttemptId"] = json!(ids.attempt);
                } else if let Some(ids) = self.queued_run_ids(run) {
                    out["providerThreadId"] = json!(ids.provider_thread);
                    out["rootNodeId"] = json!(ids.root_node);
                    out["activeAttemptId"] = json!(ids.attempt);
                }
                if let Some(source) = run.restart_continuation_of {
                    out["restartContinuationOfRunId"] = json!(source.to_string());
                    out["workStartedAt"] = json!(iso(self.work_started_at(run)));
                }
                if !run.background_work.is_empty() {
                    out["restartCancelledBackgroundWork"] = json!(run.background_work);
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
            // A notification shows as its turn item only.
            .filter(|message| message.role != NOTIFICATION_ROLE)
            .map(|message| {
                let at = iso(message.created_at);
                let mut row = json!({
                    "createdBy": if message.role == "user" { "user" } else { "agent" },
                    "creationSource": if message.role == "user" { message.creation_source.as_deref().unwrap_or("web") } else { "provider" },
                    "id": message.id,
                    "threadId": self.id,
                    "runId": self.run_of(&message.id).map(|(_, run)| run.wire_id()),
                    "nodeId": null,
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
        let mut ordinal: u64 = 0;
        for message in &self.messages {
            let run = self.run_of(&message.id);
            ordinal = match run {
                Some((index, _)) => ((index as u64 + 1) * 1_000_000).max(ordinal) + 1,
                None => ordinal + 1,
            };
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
            let fields = match message.role.as_str() {
                "user" => json!({
                    "type": "user_message",
                    "createdBy": "user",
                    "creationSource": message.creation_source.as_deref().unwrap_or("web"),
                    "messageId": message.id,
                    "inputIntent": "turn_start",
                    "text": message.content,
                    "attachments": []
                }),
                "assistant" => json!({
                    "type": "assistant_message",
                    "messageId": message.id,
                    "text": message.content,
                    "streaming": false
                }),
                // The prompt that continues a run a restart cut off.
                NOTIFICATION_ROLE => {
                    if let Some(ids) = run.and_then(|(_, run)| self.provider_run_ids(run)) {
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
        let mut cards: Vec<(usize, u64, Value)> = self
            .link_requests
            .iter()
            .filter_map(|card| {
                let index = self
                    .runs
                    .iter()
                    .position(|run| card["runId"] == run.id.to_string().as_str())?;
                let after = card[LINK_CARD_POSITION].as_u64().unwrap_or(0);
                let mut card = card.clone();
                if let Some(fields) = card.as_object_mut() {
                    fields.remove(LINK_CARD_POSITION);
                }
                Some((index, after, card))
            })
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
    pub(crate) fn projection(&self) -> Value {
        self.projection_with(true)
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
            return stored_projection(&self.id, stored, wire);
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
            self.runs
                .get(index)
                .map(|run| (run.wire_id(), self.provider_run_ids(run)))
        };
        let node_of = |turn_count: u64| {
            run_ids(turn_count)
                .and_then(|(_, ids)| ids.map(|ids| ids.root_node))
                .unwrap_or_else(|| format!("node:{}", self.id))
        };
        let checkpoint_scopes = self
            .checkpoints
            .first()
            .map(|checkpoint| {
                let first = run_ids(1);
                vec![json!({
                    "id":scope_id,
                    "threadId":self.id,
                    "runId":first.as_ref().map(|(id, _)| id.clone()),
                    "nodeId":node_of(1),
                    "parentScopeId":null,
                    "providerThreadId":first.and_then(|(_, ids)| ids.map(|ids| ids.provider_thread)),
                    "kind":"root_run",
                    "ordinalWithinParent":0,
                    "advancesAppRunCount":true,
                    "cwd":checkpoint.cwd,
                    "createdAt":iso(checkpoint.captured_at)
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
            let Some(ids) = self
                .runs
                .get(index)
                .and_then(|run| self.provider_run_ids(run))
            else {
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
        let mut messages = self.message_rows();
        messages.extend(provider.messages);
        let visible = turn_items
            .iter()
            .enumerate()
            .map(|(position, item)| {
                json!({"position":position,"visibility":"local","sourceThreadId":self.id,
                    "sourceItemId":item["id"],"item":item})
            })
            .collect::<Vec<_>>();
        json!({"thread":app,"runs":self.run_rows(&row),"attempts":with("attempts",provider.attempts),"nodes":with("nodes",provider.nodes),"subagents":array("subagents"),
            "providerSessions":with("providerSessions",provider.provider_sessions),"providerThreads":with("providerThreads",provider.provider_threads),"providerTurns":with("providerTurns",provider.provider_turns),"runtimeRequests":with("runtimeRequests",provider.runtime_requests),
            "messages":messages,"plans":provider.plans,"turnItems":turn_items,"checkpointScopes":checkpoint_scopes,"checkpoints":checkpoints,
            "contextHandoffs":[],"contextTransfers":[],"visibleTurnItems":visible,"updatedAt":row["updatedAt"]})
    }

    /// The thread's whole timeline as `OrchestrationV2ThreadTranscript` items.
    pub(crate) fn transcript(&self) -> Value {
        let row = self.stored_row();
        json!({"threadId":self.id,"title":self.title,"updatedAt":row["updatedAt"],
            "items":self.projection()["visibleTurnItems"]})
    }

    /// One `snapshot` item of `orchestration.subscribeThread`, shaped by the options the
    /// client sent as the reference's ThreadStream shapes it. The whole timeline always fits
    /// one window, so a bounded snapshot has no earlier history.
    pub(crate) fn stream_snapshot(&self, sequence: u64, options: &Value) -> Value {
        let mut projection = self.projection();
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
            "historyCursor":null,"hasMoreHistory":false,"latestLocalTurnOrdinal":latest_local_turn_ordinal});
        if omitted {
            item["turnItemsOmitLocalVisible"] = json!(true);
        }
        item
    }

    /// The thread's `OrchestrationV2ThreadShell` row, derived from its
    /// projection as the reference's `threadShellFromProjection` derives it,
    /// so a thread lists the same on both servers.
    pub(crate) fn shell(&self) -> Value {
        crate::projection_shell::shell(&self.projection(), SHELL_FIELDS)
    }
}

fn field<'a>(row: &'a Value, key: &str) -> &'a str {
    row[key].as_str().unwrap_or_default().trim()
}

fn same_link(a: &Value, b: &Value, issue: bool) -> bool {
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
fn stored_projection(thread_id: &str, stored: &Value, wire: bool) -> Value {
    let mut projection = stored.clone();
    // The thread's fields in the contract's order, as the reference encodes
    // it; a change made here adds its fields at the end of the stored record.
    projection["thread"] = ordered(APP_FIELDS, &stored["thread"]);
    sort_messages_by_turn_item_order(&mut projection);
    if wire {
        // As the reference's snapshot window reads them: without a rolled
        // back run's items or input still waiting in the queue. (Only when
        // serving: the full projection is what the database holds.)
        let items: Vec<Value> = crate::projection_shell::visible_turn_items(&projection)
            .into_iter()
            .map(crate::wire_items::wire_item)
            .collect();
        projection["turnItems"] = Value::Array(items);
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
            })
            .collect();
        let thread_id = text("id").unwrap_or_default();
        let runs = stored_runs(&thread_id, &projection);
        let checkpoints = stored_checkpoints(&projection);
        let provider = Box::new(stored_provider(&thread_id, &row, &projection));
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
            provider,
            stored: Some(Box::new(projection)),
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
    state.prior_session_id = projection["providerSessions"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|session| session["providerInstanceId"] == instance)
        .max_by_key(|session| iso_seconds(&session["updatedAt"]))
        .and_then(|session| session["id"].as_str().map(str::to_owned));
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
    if let Some(first_run) = first_run {
        state.thread = Some(crate::provider_state::ThreadRecord {
            driver,
            first_run: crate::Run::uuid_for(&first_run),
            created_at: iso_seconds(&record["createdAt"]),
            updated_at: iso_seconds(&record["updatedAt"]),
            native_id,
            status: match record["status"].as_str() {
                Some("active") => "active",
                Some("idle") => "idle",
                Some("error") => "error",
                _ => "not_loaded",
            },
        });
    }
    state
}

/// The reference's `sortMessagesByTurnItemOrder`: messages in the order of
/// the first user or assistant turn item that shows each, then by creation
/// time and id.
fn sort_messages_by_turn_item_order(projection: &mut Value) {
    let mut ordinals: std::collections::HashMap<String, i64> = std::collections::HashMap::new();
    for item in projection["turnItems"].as_array().into_iter().flatten() {
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
                .then_with(|| millis(&left["createdAt"]).cmp(&millis(&right["createdAt"])))
                .then_with(|| left["id"].as_str().cmp(&right["id"].as_str()))
        });
    }
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
