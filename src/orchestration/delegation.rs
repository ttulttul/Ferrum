//! Delegated tasks, as the reference's orchestrator runs them
//! (`dispatchDelegatedTaskRequest` and the finalization around
//! `finalizeAppOwnedSubagentTask`): a child thread that runs the task, and
//! on its parent the task, its node and its timeline card.
use super::*;
use crate::thread_projection::uri_component;

/// `joinId`: parts joined by `:`, each encoded as `encodeURIComponent` does.
pub(crate) fn join_id(prefix: &str, parts: &[&str]) -> String {
    std::iter::once(prefix.to_owned())
        .chain(parts.iter().map(|part| uri_component(part)))
        .collect::<Vec<_>>()
        .join(":")
}

pub(crate) fn task_node_id(command_id: &str) -> String {
    join_id("node", &["delegated-task", command_id])
}
pub(crate) fn task_thread_id(command_id: &str) -> String {
    format!(
        "{}{}",
        join_id("thread", &["delegated-task", ""]),
        uri_component(command_id)
    )
}
fn task_message_id(command_id: &str) -> String {
    join_id("message", &["delegated-task", command_id])
}
fn task_turn_item_id(command_id: &str) -> String {
    join_id("turn-item", &["delegated-task", command_id])
}
/// A run's root node (`derive.rootNode`).
pub(crate) fn root_node_id(run_id: &str) -> String {
    join_id("node", &["run", run_id, "root"])
}

/// `contextSourcePointForRun`: where in `projection`'s thread `run` left off.
fn context_source_point(projection: &Value, run_id: &str) -> Value {
    let list = |key: &str| projection[key].as_array().cloned().unwrap_or_default();
    let mut point = json!({"threadId": projection["thread"]["id"], "runId": run_id});
    let Some(run) = list("runs").into_iter().find(|r| r["id"] == run_id) else {
        return point;
    };
    if !run["checkpointId"].is_null() {
        point["checkpointId"] = run["checkpointId"].clone();
    }
    let provider_thread = (!run["providerThreadId"].is_null())
        .then(|| {
            list("providerThreads")
                .into_iter()
                .find(|t| t["id"] == run["providerThreadId"])
        })
        .flatten();
    if let Some(native) = provider_thread
        .map(|t| t["nativeThreadRef"].clone())
        .filter(|r| !r.is_null())
    {
        point["providerThreadRef"] = native;
    }
    let attempt = &run["activeAttemptId"];
    if !attempt.is_null() {
        let turns = list("providerTurns");
        let turn = turns
            .iter()
            .filter(|t| t["runAttemptId"] == *attempt)
            .max_by_key(|t| t["ordinal"].as_u64().unwrap_or(0))
            .or_else(|| {
                let attempt_turn = list("attempts")
                    .into_iter()
                    .find(|a| a["id"] == *attempt)
                    .map(|a| a["providerTurnId"].clone())?;
                turns.iter().find(|t| t["id"] == attempt_turn)
            });
        if let Some(native) = turn
            .map(|t| t["nativeTurnRef"].clone())
            .filter(|r| !r.is_null())
        {
            point["providerTurnRef"] = native;
        }
    }
    point
}

/// `subagentThreadTitle`: the task's title, else its prompt, cut to 72
/// characters, else "<parent> subagent <n>".
pub(crate) fn subagent_thread_title(
    parent: &str,
    title: Option<&str>,
    prompt: &str,
    ordinal: usize,
) -> String {
    let detail = title
        .map(str::trim)
        .filter(|t| !t.is_empty())
        .or_else(|| Some(prompt.trim()).filter(|p| !p.is_empty()));
    let Some(detail) = detail else {
        return format!("{parent} subagent {ordinal}");
    };
    let units: Vec<u16> = detail.encode_utf16().collect();
    if units.len() <= 72 {
        return detail.to_owned();
    }
    let mut end = 69;
    if (0xd800..=0xdbff).contains(&units[end - 1]) && (0xdc00..=0xdfff).contains(&units[end]) {
        end -= 1;
    }
    format!("{}...", String::from_utf16_lossy(&units[..end]))
}

/// The driver an instance runs (`providerInstances[id].driver`, else the id).
fn driver_of(settings: &Value, instance: &str) -> String {
    settings["providerInstances"][instance]["driver"]
        .as_str()
        .unwrap_or(instance)
        .to_owned()
}

/// Thread fields a child never takes from its parent: records kept on the
/// row, and links that belong to the parent alone.
const PARENT_ONLY: &[&str] = &[
    "subagents",
    "nodes",
    "appTurnItems",
    "contextTransfers",
    "runExtras",
    "delegatedFrom",
    "historyOrigin",
];

/// `makeSubagentChildThread`: the parent's thread row as the child's.
#[allow(clippy::too_many_arguments)]
fn child_thread_row(
    parent: &Value,
    child_id: &str,
    task_node: &str,
    command: &Value,
    title: &str,
    at: &str,
) -> Value {
    let mut row = parent.clone();
    if let Some(object) = row.as_object_mut() {
        for key in PARENT_ONLY {
            object.remove(*key);
        }
    }
    let root = parent["lineage"]["rootThreadId"]
        .as_str()
        .or(parent["id"].as_str())
        .unwrap_or_default();
    for (key, value) in [
        ("createdBy", command["createdBy"].clone()),
        ("creationSource", command["creationSource"].clone()),
        ("id", json!(child_id)),
        ("title", json!(title)),
        ("titleRegeneration", Value::Null),
        ("titleRenamed", json!(false)),
        ("linkedPullRequest", Value::Null),
        ("pullRequests", json!([])),
        (
            "providerInstanceId",
            command["modelSelection"]["instanceId"].clone(),
        ),
        ("modelSelection", command["modelSelection"].clone()),
        ("activeProviderThreadId", Value::Null),
        (
            "lineage",
            json!({"parentThreadId": parent["id"], "relationshipToParent": "subagent", "rootThreadId": root}),
        ),
        ("forkedFrom", json!({"type": "node", "nodeId": task_node})),
        ("createdAt", json!(at)),
        ("updatedAt", json!(at)),
        ("archivedAt", Value::Null),
        ("settledOverride", Value::Null),
        ("settledAt", Value::Null),
        ("snoozedUntil", Value::Null),
        ("snoozedAt", Value::Null),
        ("pinnedAt", Value::Null),
        ("pinOrderKey", Value::Null),
        ("autoSettleDisabledAt", Value::Null),
        ("guestShare", Value::Null),
        ("lastVisitedAt", Value::Null),
        ("deletedAt", Value::Null),
        ("runtimeMode", command["runtimeMode"].clone()),
        ("interactionMode", command["interactionMode"].clone()),
    ] {
        row[key] = value;
    }
    if let Some(strategy) = command.get("workspaceStrategy") {
        row["branch"] = Value::Null;
        row["worktreePath"] = Value::Null;
        // The child's first run waits for its worktree (`defer_start`).
        row["runExtras"] = json!({
            crate::store::Run::reference_id(child_id, 1): {"workspacePreparation": strategy}
        });
    }
    row
}

/// `dispatchDelegatedTaskRequest`: starts a child thread on the task and
/// records the task on its parent's active run.
pub(super) async fn request<H: CommandHost>(
    state: Arc<H>,
    command: &Value,
    receipt: &CommandReceipt,
) -> anyhow::Result<u64> {
    let command_id = command["commandId"].as_str().unwrap_or_default();
    let parent_id = command["parentThreadId"].as_str().unwrap_or_default();
    let parent_run_id = command["parentRunId"].as_str().unwrap_or_default();
    let parent_node_id = command["parentNodeId"].as_str().unwrap_or_default();
    let parent = state
        .store
        .read()
        .await
        .threads
        .get(parent_id)
        .cloned()
        .ok_or_else(|| {
            anyhow::anyhow!(format!(
                "No orchestration projection exists for thread {parent_id}."
            ))
        })?;
    let parent_run = parent
        .runs
        .iter()
        .find(|run| run.wire_id() == parent_run_id && run.status == "running")
        .cloned()
        .ok_or_else(|| anyhow::anyhow!(format!("Parent run {parent_run_id} is not active.")))?;
    if parent_node_id != root_node_id(parent_run_id) {
        anyhow::bail!("Parent node {parent_node_id} is not part of active run {parent_run_id}.");
    }
    let settings = state.effective_settings().unwrap_or_default();
    let instance = command["modelSelection"]["instanceId"]
        .as_str()
        .unwrap_or_default();
    let driver = driver_of(&settings, instance);
    let at = crate::auth_service::iso(crate::auth_service::now());
    let task_node = task_node_id(command_id);
    let child_id = task_thread_id(command_id);
    let parent_row = parent.stored_row();
    let prompt = command["task"].as_str().unwrap_or_default();
    let ordinal = parent_row["subagents"].as_array().map_or(0, Vec::len) + 1;
    let title = subagent_thread_title(&parent.title, command["title"].as_str(), prompt, ordinal);
    let projection = parent.full_projection();
    let provider_turn = projection["providerTurns"]
        .as_array()
        .into_iter()
        .flatten()
        .rfind(|turn| turn["runId"] == parent_run_id)
        .map(|turn| turn["id"].clone())
        .unwrap_or(Value::Null);
    let provider_thread = projection["runs"]
        .as_array()
        .into_iter()
        .flatten()
        .find(|run| run["id"] == parent_run_id)
        .map(|run| run["providerThreadId"].clone())
        .unwrap_or(Value::Null);
    // Where the card sits among its run's items, as a link card does.
    let after_items = parent
        .runs
        .iter()
        .position(|r| r.wire_id() == parent_run_id)
        .map_or(0, |index| {
            parent
                .provider_records()
                .items
                .iter()
                .filter(|(i, _)| *i == index)
                .count()
        });
    let mut task = json!({
        "id": task_node,
        "threadId": parent_id,
        "runId": parent_run_id,
        "parentNodeId": parent_node_id,
        "origin": "app_owned",
        "createdBy": command["createdBy"],
        "driver": driver,
        "providerInstanceId": instance,
        "providerThreadId": null,
        "childThreadId": child_id,
        "nativeTaskRef": null,
        "prompt": prompt,
        "title": command.get("title").cloned().unwrap_or(Value::Null),
        "model": command["modelSelection"]["model"],
        "modelSelection": command["modelSelection"],
        "status": "running",
        "result": null,
        "startedAt": at,
        "completedAt": null,
        "updatedAt": at,
    });
    if let Some(wake) = command.get("completionWake") {
        task["completionWake"] = wake.clone();
    }
    let node = json!({
        "id": task_node,
        "threadId": parent_id,
        "runId": parent_run_id,
        "parentNodeId": parent_node_id,
        "rootNodeId": parent_node_id,
        "kind": "subagent",
        "status": "running",
        "countsForRun": false,
        "providerThreadId": null,
        "providerTurnId": null,
        "nativeItemRef": null,
        "runtimeRequestId": null,
        "checkpointScopeId": null,
        "startedAt": at,
        "completedAt": null,
    });
    let item = json!({
        "id": task_turn_item_id(command_id),
        "threadId": parent_id,
        "runId": parent_run_id,
        "nodeId": task_node,
        "providerThreadId": provider_thread,
        "providerTurnId": provider_turn,
        "nativeItemRef": null,
        "parentItemId": null,
        "status": "running",
        "title": command["title"].as_str().unwrap_or(&title),
        "startedAt": at,
        "completedAt": null,
        "updatedAt": at,
        "type": "subagent",
        "subagentId": task_node,
        "origin": "app_owned",
        "driver": driver,
        "providerInstanceId": instance,
        "childThreadId": child_id,
        "prompt": prompt,
        "result": null,
        (crate::thread_projection::LINK_CARD_POSITION): after_items,
    });
    let push = |key: &str, record: Value| {
        let mut records = parent_row[key].as_array().cloned().unwrap_or_default();
        records.push(record);
        Value::Array(records)
    };
    let child_row = child_thread_row(&parent_row, &child_id, &task_node, command, &title, &at);
    let child = Thread {
        row: child_row,
        id: child_id.clone(),
        project_id: parent.project_id.clone(),
        title: title.clone(),
        // A child in its own worktree starts in the project's folder until it is prepared.
        cwd: if command.get("workspaceStrategy").is_some() {
            state
                .store
                .read()
                .await
                .projects
                .get(&parent.project_id)
                .map_or_else(|| parent.cwd.clone(), |project| project.path.clone())
        } else {
            parent.cwd.clone()
        },
        status: "idle".into(),
        messages: vec![],
        runs: vec![],
        created_at: now(),
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
        provider: Default::default(),
        stored: None,
        revision: 0,
    };
    state.commit(Event::ThreadCreated(child)).await?;
    state
        .commit(Event::ThreadPatched {
            thread_id: parent_id.to_owned(),
            patch: json!({"row": {
                "subagents": push("subagents", task),
                "nodes": push("nodes", node),
                "appTurnItems": push("appTurnItems", item),
            }}),
        })
        .await?;
    // The child's first message, sent by the parent.
    let message_id = json!(task_message_id(command_id));
    let wire = json!({
        "createdBy": command["createdBy"],
        "creationSource": command["creationSource"],
        "senderThreadId": parent_id,
    });
    let deferred = command.get("workspaceStrategy").is_some();
    let seq = start_child_run(
        state.clone(),
        receipt,
        &child_id,
        prompt,
        &message_id,
        wire,
        deferred,
    )
    .await?;
    let child_run = state
        .store
        .read()
        .await
        .threads
        .get(&child_id)
        .and_then(|t| t.runs.first().map(|r| r.wire_id()))
        .ok_or_else(|| {
            anyhow::anyhow!(format!(
                "Delegated child thread {child_id} did not create a run."
            ))
        })?;
    let mut spawn_point = context_source_point(&projection, parent_run_id);
    spawn_point["turnItemId"] = json!(task_turn_item_id(command_id));
    let spawn_instance = projection["runs"]
        .as_array()
        .into_iter()
        .flatten()
        .find(|run| run["id"] == parent_run_id)
        .map_or_else(
            || projection["thread"]["providerInstanceId"].clone(),
            |run| run["providerInstanceId"].clone(),
        );
    let transfer = json!({
        "id": format!(
            "{}:{}",
            join_id(
                "context-transfer",
                &["type", "subagent_spawn", "source-thread", parent_id, "target-thread", &child_id]
            ),
            crate::platform::uuid_v4()
        ),
        "type": "subagent_spawn",
        "sourceThreadId": parent_id,
        "targetThreadId": child_id,
        "sourcePoint": spawn_point,
        "basePoint": null,
        "sourceProviderInstanceId": spawn_instance,
        "targetProviderInstanceId": instance,
        "targetRunId": child_run,
        "status": "consumed",
        "resolution": null,
        "createdBy": command["createdBy"],
        "error": null,
        "createdAt": at,
        "updatedAt": at,
        "consumedAt": at,
    });
    let _ = parent_run;
    state
        .commit(Event::ThreadPatched {
            thread_id: child_id,
            patch: json!({"row": {"contextTransfers": [transfer]}}),
        })
        .await?;
    Ok(seq)
}

/// Starts the child's run on the task, as `message.dispatch` with
/// `start_immediately` does, with the message's wire fields; `deferred`, as
/// `defer_start` does, leaves it preparing its workspace.
async fn start_child_run<H: CommandHost>(
    state: Arc<H>,
    receipt: &CommandReceipt,
    child_id: &str,
    prompt: &str,
    message_id: &Value,
    wire: Value,
    deferred: bool,
) -> anyhow::Result<u64> {
    let seq = start_run(
        state.clone(),
        receipt,
        child_id,
        prompt.to_owned(),
        message_id,
        false,
        None,
        wire["creationSource"].as_str().map(str::to_owned),
        None,
        false,
        None,
        Some(wire),
        deferred,
        None,
        false,
    )
    .await?;
    Ok(seq)
}

const TERMINAL_RUN: [&str; 5] = [
    "completed",
    "failed",
    "cancelled",
    "interrupted",
    "rolled_back",
];
const DELEGATED_RESULT_INLINE_BYTES: usize = 16_000;

/// `delegatedTaskTerminalStatus`.
fn terminal_status(run_status: &str) -> Option<&'static str> {
    match run_status {
        "completed" => Some("completed"),
        "failed" => Some("failed"),
        "cancelled" | "rolled_back" => Some("cancelled"),
        "interrupted" => Some("interrupted"),
        _ => None,
    }
}

fn records(projection: &Value, key: &str) -> Vec<Value> {
    projection[key].as_array().cloned().unwrap_or_default()
}

/// `delegatedTaskProgress`: whether the child still works, waits on its own
/// tasks, or has a result, and the run the result comes from.
fn progress(child: &Value) -> (&'static str, Option<Value>) {
    let runs: Vec<Value> = records(child, "runs")
        .into_iter()
        .filter(|run| run["status"] != "rolled_back")
        .collect();
    let terminal = |run: &Value| TERMINAL_RUN.contains(&run["status"].as_str().unwrap_or_default());
    let active = runs
        .iter()
        .any(|run| !(terminal(run) || run["status"] == "queued" && run["queueHeld"] == true));
    let holds = records(child, "subagents").iter().any(|task| {
        matches!(
            task["status"].as_str(),
            Some("pending" | "running" | "waiting" | "starting")
        ) || (task["status"] != "idle"
            && matches!(
                task["completionDelivery"]["state"].as_str(),
                Some("pending" | "claimed")
            ))
    });
    let result = runs
        .iter()
        .filter(|run| terminal(run) && (!run["startedAt"].is_null() || run["ordinal"] == 1))
        .max_by_key(|run| run["ordinal"].as_u64().unwrap_or(0))
        .cloned();
    let state = if active || result.is_none() {
        "working"
    } else if holds {
        "waiting_for_children"
    } else {
        "result_available"
    };
    (state, result)
}

/// What a finished child run reports, as a task's result.
pub(crate) fn subagent_result(child: &Value, run: &Value) -> String {
    result_for_run(child, run).0
}

/// `subagentResultForRun`: what a finished child run reports.
fn result_for_run(child: &Value, run: &Value) -> (String, Value) {
    let run_id = &run["id"];
    let status = run["status"].as_str().unwrap_or_default();
    let items = records(child, "turnItems");
    let item = items
        .iter()
        .filter(|i| {
            i["runId"] == *run_id
                && i["type"] == "assistant_message"
                && i["text"].as_str().is_some_and(|t| !t.trim().is_empty())
        })
        .max_by_key(|i| i["ordinal"].as_u64().unwrap_or(0));
    let messages: Vec<Value> = records(child, "messages")
        .into_iter()
        .filter(|m| {
            m["runId"] == *run_id
                && m["role"] == "assistant"
                && m["text"].as_str().is_some_and(|t| !t.trim().is_empty())
        })
        .collect();
    let message = item
        .and_then(|i| messages.iter().find(|m| m["id"] == i["messageId"]))
        .or_else(|| {
            messages.iter().max_by(|a, b| {
                (a["updatedAt"].as_str(), a["createdAt"].as_str())
                    .cmp(&(b["updatedAt"].as_str(), b["createdAt"].as_str()))
            })
        });
    let failure = (status == "failed")
        .then(|| {
            items
                .iter()
                .filter(|i| i["runId"] == *run_id && i["type"] == "error")
                .max_by_key(|i| i["ordinal"].as_u64().unwrap_or(0))
        })
        .flatten();
    let report_before_failure = match (failure, item) {
        (Some(f), Some(i)) if i["ordinal"].as_u64() < f["ordinal"].as_u64() => message
            .and_then(|m| m["text"].as_str())
            .or(i["text"].as_str())
            .map(str::to_owned),
        _ => None,
    };
    let text = if let Some(failure) = failure {
        let reason = failure["failure"]["message"].as_str().unwrap_or_default();
        match report_before_failure {
            None => reason.to_owned(),
            Some(report) => {
                format!("{reason}\n\nLast assistant message before the run failed:\n\n{report}")
            }
        }
    } else {
        let report = if let Some(text) = message.and_then(|m| m["text"].as_str()) {
            text.to_owned()
        } else if let Some(text) = item.and_then(|i| i["text"].as_str()) {
            text.to_owned()
        } else if status == "completed" {
            "Child task completed without an assistant result.".to_owned()
        } else {
            format!("Child task ended with status {status}.")
        };
        // `withStopNotice` (#37859): a stopped run's result ends with why it
        // stopped.
        let notice = items.iter().find(|i| {
            i["runId"] == *run_id
                && i["type"] == "run_interrupt_result"
                && i["message"].as_str().is_some_and(|m| !m.is_empty())
        });
        match notice.and_then(|n| n["message"].as_str()) {
            Some(notice) => format!("{report}\n\n{notice}"),
            None => report,
        }
    };
    let turn_item = failure
        .map(|f| f["id"].clone())
        .or_else(|| item.map(|i| i["id"].clone()))
        .unwrap_or(Value::Null);
    (text, turn_item)
}

/// `delegatedCompletionWakeDetail`: each finished task and its result,
/// inlined up to 16,000 bytes in all.
pub(crate) fn wake_detail(task_ids: &[Value], tasks: &[Value]) -> String {
    let mut remaining = DELEGATED_RESULT_INLINE_BYTES;
    task_ids
        .iter()
        .map(|id| {
            let id = id.as_str().unwrap_or_default();
            let task = tasks.iter().find(|t| t["id"] == id);
            let title = task
                .and_then(|t| t["title"].as_str())
                .map(|t| format!(" ({t})"))
                .unwrap_or_default();
            // A follow-up gets a fresh task id; the child thread links it to
            // the task delegate_task returned (#39297).
            let thread = task
                .and_then(|t| t["childThreadId"].as_str())
                .map(|c| format!(" in child thread {c}"))
                .unwrap_or_default();
            let state = match task {
                None => " reached a terminal state".to_owned(),
                Some(t) => format!(" [{}]", t["status"].as_str().unwrap_or_default()),
            };
            let label = format!("Delegated task {id}{title}{thread}{state}");
            if let Some(result) = task.and_then(|t| t["result"].as_str()) {
                let detail = format!("{label}:\n{result}");
                if detail.len() <= remaining {
                    remaining -= detail.len();
                    return detail;
                }
            }
            format!("{label}. Use task_status with taskId {id} to read the result.")
        })
        .collect::<Vec<_>>()
        .join("\n\n")
}

/// `reportLabel`: a label's first line, cut to 80 characters.
fn report_label(label: Option<&str>) -> Option<String> {
    let first = label?.trim().split('\n').next()?.trim();
    if first.is_empty() {
        return None;
    }
    let units: Vec<u16> = first.encode_utf16().collect();
    if units.len() <= 80 {
        return Some(first.to_owned());
    }
    let mut end = 79;
    if (0xd800..=0xdbff).contains(&units[end - 1]) && (0xdc00..=0xdfff).contains(&units[end]) {
        end -= 1;
    }
    Some(format!("{}…", String::from_utf16_lossy(&units[..end])))
}

/// `delegatedCompletionNotification`: how a delivery shows in the parent's
/// timeline.
fn completion_notification(parent_run: &str, task_ids: &[Value], tasks: &[Value]) -> Value {
    let reports: Vec<(&str, Option<String>, Value)> = task_ids
        .iter()
        .map(|id| {
            let task = tasks.iter().find(|t| t["id"] == *id);
            let outcome = match task.and_then(|t| t["status"].as_str()) {
                Some("failed") => "failed",
                Some("cancelled" | "interrupted") => "cancelled",
                Some("completed") => "completed",
                _ => "unknown",
            };
            let label = task.and_then(|t| {
                t["title"]
                    .as_str()
                    .map(str::trim)
                    .filter(|l| !l.is_empty())
                    .or(t["prompt"].as_str())
                    .map(str::to_owned)
            });
            let child = task
                .map(|t| t["childThreadId"].clone())
                .unwrap_or(Value::Null);
            (outcome, label, child)
        })
        .collect();
    let outcomes: Vec<&str> = reports.iter().map(|r| r.0).collect();
    let outcome = if outcomes.contains(&"failed") {
        "failed"
    } else if outcomes.contains(&"cancelled") {
        "cancelled"
    } else if !outcomes.is_empty() && outcomes.iter().all(|o| *o == "completed") {
        "completed"
    } else if outcomes.contains(&"updated") {
        "updated"
    } else {
        "unknown"
    };
    let verb = match outcome {
        "failed" => "failed",
        "cancelled" => "stopped",
        _ => "finished",
    };
    if let [(_, label, child)] = reports.as_slice() {
        let mut source = json!({"kind": "delegated_task", "taskIds": task_ids});
        if !child.is_null() {
            source["childThreadId"] = child.clone();
        }
        let label = report_label(label.as_deref())
            .map(|l| format!(" \"{l}\""))
            .unwrap_or_default();
        return json!({"source": source, "outcome": outcome, "summary": format!("Delegated task{label} {verb}")});
    }
    let labels: Vec<String> = reports
        .iter()
        .filter_map(|r| report_label(r.1.as_deref()))
        .collect();
    let by_run = tasks
        .iter()
        .filter(|t| t["origin"] == "app_owned" && t["runId"] == parent_run)
        .count();
    let count = if by_run > task_ids.len() {
        format!("{} of {by_run}", task_ids.len())
    } else {
        task_ids.len().to_string()
    };
    let labels = if labels.is_empty() {
        String::new()
    } else {
        format!(": {}", labels.join(", "))
    };
    json!({
        "source": {"kind": "delegated_task", "taskIds": task_ids},
        "outcome": outcome,
        "summary": format!("{count} delegated tasks {verb}{labels}"),
    })
}

/// `delegatedCompletionText`: the delivery message's own text.
fn completion_text(task_ids: &[Value]) -> String {
    let list = task_ids
        .iter()
        .filter_map(Value::as_str)
        .collect::<Vec<_>>()
        .join(", ");
    if task_ids.len() == 1 {
        format!("Delegated task {list} reached a terminal state. Use task_status with taskId {list} to read the result.")
    } else {
        format!("Delegated tasks {list} reached terminal states. Use task_status with each taskId to read the results.")
    }
}

fn replace_by_id(records: &mut [Value], id: &Value, update: impl FnOnce(&mut Value)) {
    if let Some(record) = records.iter_mut().find(|r| r["id"] == *id) {
        update(record);
    }
}

/// After any run ends: settles the task of a delegated child whose run it
/// was, and delivers finished tasks to a parent that is idle now.
/// `wakeWorkStartedAt`: a run a wake starts carries on the work of the
/// thread's latest started run, so it counts from when that work began.
pub(crate) async fn wake_work_started_at<H: CommandHost>(
    state: &H,
    thread_id: &str,
) -> Option<Value> {
    let store = state.store.read().await;
    let projection = store.threads.get(thread_id)?.full_projection();
    let previous = records(&projection, "runs")
        .into_iter()
        .filter(|run| run["startedAt"].is_string())
        .max_by(|a, b| {
            let at = |r: &Value| r["startedAt"].as_str().unwrap_or_default().to_owned();
            at(a)
                .cmp(&at(b))
                .then(a["ordinal"].as_u64().cmp(&b["ordinal"].as_u64()))
        })?;
    ["workStartedAt", "startedAt", "requestedAt"]
        .iter()
        .map(|key| previous[*key].clone())
        .find(Value::is_string)
}

/// Records `at` as the `workStartedAt` of the run `thread_id` creates next,
/// so the run carries it from its creation, as the reference's does.
pub(crate) async fn record_work_started_at<H: CommandHost>(
    state: &H,
    thread_id: &str,
    at: Value,
) -> anyhow::Result<()> {
    let (run_id, mut extras) = {
        let store = state.store.read().await;
        let Some(thread) = store.threads.get(thread_id) else {
            return Ok(());
        };
        (
            crate::Run::reference_id(thread_id, thread.next_run_ordinal()),
            thread.stored_row()["runExtras"].clone(),
        )
    };
    if !extras.is_object() {
        extras = json!({});
    }
    extras[run_id.as_str()]["workStartedAt"] = at;
    state
        .commit(Event::ThreadPatched {
            thread_id: thread_id.to_owned(),
            patch: json!({"row": {"runExtras": extras}}),
        })
        .await?;
    Ok(())
}

/// `thread.created.record`: the parent's timeline card for a thread its
/// agent created (`dispatchCreatedThreadRecord`), after the provider items its
/// run had then.
pub(crate) async fn record_created_thread<H: CommandHost>(
    state: Arc<H>,
    command: &Value,
    receipt: &crate::store::CommandReceipt,
) -> anyhow::Result<u64> {
    let parent_id = command["parentThreadId"].as_str().unwrap_or_default();
    let parent_run_id = command["parentRunId"].as_str().unwrap_or_default();
    let target_id = command["targetThreadId"].as_str().unwrap_or_default();
    let (row, item) = {
        let store = state.store.read().await;
        let parent = store.threads.get(parent_id).ok_or_else(|| {
            anyhow::anyhow!("No orchestration projection exists for thread {parent_id}.")
        })?;
        let target = store.threads.get(target_id).ok_or_else(|| {
            anyhow::anyhow!("No orchestration projection exists for thread {target_id}.")
        })?;
        let projection = parent.full_projection();
        let run = records(&projection, "runs")
            .into_iter()
            .find(|run| run["id"] == parent_run_id)
            .unwrap_or(Value::Null);
        // `providerTurnForRun`: the run's latest provider turn for its attempt.
        let provider_turn = records(&projection, "providerTurns")
            .into_iter()
            .filter(|turn| {
                !run["activeAttemptId"].is_null() && turn["runAttemptId"] == run["activeAttemptId"]
            })
            .max_by_key(|turn| turn["ordinal"].as_u64().unwrap_or(0))
            .map_or(Value::Null, |turn| turn["id"].clone());
        let after_items = parent
            .runs
            .iter()
            .position(|r| r.wire_id() == parent_run_id)
            .map_or(0, |index| {
                parent
                    .provider_records()
                    .items
                    .iter()
                    .filter(|(i, _)| *i == index)
                    .count()
            });
        let selection = target.full_projection()["thread"]["modelSelection"].clone();
        let at = crate::auth_service::iso(crate::auth_service::now());
        let item = json!({
            "id": join_id("turn-item", &["created-thread", command["commandId"].as_str().unwrap_or_default()]),
            "threadId": parent_id,
            "runId": parent_run_id,
            "nodeId": command["parentNodeId"],
            "providerThreadId": run["providerThreadId"],
            "providerTurnId": provider_turn,
            "nativeItemRef": null,
            "parentItemId": null,
            "status": "completed",
            "title": target.title,
            "startedAt": at,
            "completedAt": at,
            "updatedAt": at,
            "type": "thread_created",
            "targetThreadId": target_id,
            "targetRunId": command["targetRunId"],
            "targetProviderInstanceId": selection["instanceId"],
            "targetModel": selection["model"],
            (crate::thread_projection::LINK_CARD_POSITION): after_items,
        });
        (parent.stored_row(), item)
    };
    let mut items = records(&row, "appTurnItems");
    // A replay of the same command writes the same card once.
    if items.iter().any(|existing| existing["id"] == item["id"]) {
        return Ok(state
            .commit_receipted(
                Event::CommandDispatched {
                    command_id: receipt.command_id.clone(),
                    command_type: "thread.created.record".into(),
                    thread_id: Some(parent_id.to_owned()),
                    payload: command.clone(),
                },
                Some(receipt.clone()),
            )
            .await?
            .id);
    }
    items.push(item);
    Ok(state
        .commit_receipted(
            Event::ThreadPatched {
                thread_id: parent_id.to_owned(),
                patch: json!({"row": {"appTurnItems": items}}),
            },
            Some(receipt.clone()),
        )
        .await?
        .id)
}

/// `acceptDelegatedCompletionDelivery`: the delivery message for `completion`
/// (`{parentRunId, generation, taskIds}`) marks the tasks it claimed as
/// delivered and frees its cohort for the next delivery. Recorded with the
/// message, so a delivery is never accepted without being sent.
pub(crate) async fn accept_delivery<H: CommandHost>(
    state: &Arc<H>,
    thread_id: &str,
    completion: &Value,
) -> anyhow::Result<()> {
    let row = {
        let store = state.store.read().await;
        let Some(thread) = store.threads.get(thread_id) else {
            return Ok(());
        };
        thread.stored_row()
    };
    let Some(run_id) = completion["parentRunId"].as_str() else {
        return Ok(());
    };
    let delivery = &row["runExtras"][run_id]["delegatedCompletion"]["delivery"];
    if !delivery.is_object() || delivery["generation"] != completion["generation"] {
        return Ok(());
    }
    let task_ids = records(delivery, "taskIds");
    let at = crate::auth_service::iso(crate::auth_service::now());
    let mut tasks = records(&row, "subagents");
    for task in tasks.iter_mut() {
        if task_ids.contains(&task["id"]) && task["completionDelivery"]["state"] == "claimed" {
            task["completionDelivery"] = json!({"state": "delivered", "observedByRunId": null});
            task["updatedAt"] = json!(at);
        }
    }
    let mut extras = row["runExtras"].clone();
    extras[run_id]["delegatedCompletion"]["delivery"] = Value::Null;
    state
        .commit(Event::ThreadPatched {
            thread_id: thread_id.to_owned(),
            patch: json!({"row": {"subagents": tasks, "runExtras": extras}}),
        })
        .await?;
    Ok(())
}

/// `recoverDelegatedTasks`, once a restarted server is up: settles the tasks
/// of delegated children whose work ended without them being settled, and
/// offers again each delivery a cohort still holds. A child whose cut run
/// still awaits its restart continuation (`awaiting`, wire run ids) waits
/// for it, as the reference's `childAwaitsRestartContinuation` does.
pub(crate) async fn recover<H: CommandHost>(
    state: Arc<H>,
    awaiting: &std::collections::HashSet<String>,
) {
    let (children, cohorts) = {
        let store = state.store.read().await;
        let mut children = Vec::new();
        let mut cohorts = Vec::new();
        for thread in store.threads.values() {
            let row = thread.stored_row();
            if row["lineage"]["relationshipToParent"] == "subagent"
                && row["forkedFrom"]["type"] == "node"
            {
                if let Some(parent) = row["lineage"]["parentThreadId"].as_str() {
                    let waits = thread
                        .runs
                        .iter()
                        .any(|run| awaiting.contains(&run.wire_id()));
                    if !waits {
                        children.push((thread.id.clone(), parent.to_owned()));
                    }
                }
            }
            let holds = row["runExtras"].as_object().is_some_and(|extras| {
                extras.values().any(|extra| {
                    extra["delegatedCompletion"]["disposition"] == "open"
                        && extra["delegatedCompletion"]["delivery"].is_object()
                })
            });
            if holds {
                cohorts.push(thread.id.clone());
            }
        }
        (children, cohorts)
    };
    for (child, parent) in children {
        if let Err(error) = finalize_child(state.clone(), &child, &parent).await {
            tracing::warn!(%error, child, "could not recover a delegated task");
        }
        offer_delivery(state.clone(), &parent).await;
    }
    for thread_id in cohorts {
        offer_delivery(state.clone(), &thread_id).await;
    }
}

/// What a blocking `delegate_task` wait gets when its child's run ends: it
/// reads the task, drops `read`, and answers once `settled` closes.
pub(crate) struct RunEnd {
    pub read: tokio::sync::oneshot::Sender<()>,
    pub settled: tokio::sync::oneshot::Receiver<()>,
}

type RunEndWaiters = std::collections::HashMap<String, Vec<tokio::sync::oneshot::Sender<RunEnd>>>;
static RUN_END_WAITERS: std::sync::LazyLock<std::sync::Mutex<RunEndWaiters>> =
    std::sync::LazyLock::new(Default::default);

/// A wait on `child_thread_id`'s next run end. The reference's wait reads the
/// task on the child's `run.updated` event, before the task is finalized, and
/// its answer goes out after finalization; `after_run` keeps that order.
pub(crate) fn watch_run_end(child_thread_id: &str) -> tokio::sync::oneshot::Receiver<RunEnd> {
    let (tx, rx) = tokio::sync::oneshot::channel();
    let mut waiters = RUN_END_WAITERS.lock().unwrap_or_else(|e| e.into_inner());
    waiters.retain(|_, list| {
        list.retain(|w| !w.is_closed());
        !list.is_empty()
    });
    waiters
        .entry(child_thread_id.to_owned())
        .or_default()
        .push(tx);
    rx
}

/// Lets each waiter read; the returned senders release them once dropped.
async fn let_waiters_read(child_thread_id: &str) -> Vec<tokio::sync::oneshot::Sender<()>> {
    let waiters = RUN_END_WAITERS
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .remove(child_thread_id)
        .unwrap_or_default();
    let mut releases = Vec::new();
    for waiter in waiters {
        let (read, read_rx) = tokio::sync::oneshot::channel();
        let (release, settled) = tokio::sync::oneshot::channel();
        if waiter.send(RunEnd { read, settled }).is_ok() {
            let _ = read_rx.await;
            releases.push(release);
        }
    }
    releases
}

pub(crate) async fn after_run<H: CommandHost>(state: Arc<H>, thread_id: &str) {
    let (parent_id, is_child) = {
        let store = state.store.read().await;
        let Some(thread) = store.threads.get(thread_id) else {
            return;
        };
        let row = thread.stored_row();
        let child = row["lineage"]["relationshipToParent"] == "subagent"
            && row["forkedFrom"]["type"] == "node";
        (
            row["lineage"]["parentThreadId"].as_str().map(str::to_owned),
            child,
        )
    };
    if is_child {
        let releases = let_waiters_read(thread_id).await;
        if let Some(parent) = parent_id.as_deref() {
            if let Err(error) = finalize_child(state.clone(), thread_id, parent).await {
                tracing::warn!(%error, thread_id, "could not settle a delegated task");
            }
            drop(releases);
            offer_delivery(state.clone(), parent).await;
        }
    }
    if let Err(error) = finalize_delivery(&*state, thread_id).await {
        tracing::warn!(%error, thread_id, "could not settle a delegated delivery");
    }
    resend_untaken_deliveries(state.clone(), thread_id).await;
    offer_delivery(state.clone(), thread_id).await;
    start_queued_wake(state, thread_id).await;
}

/// A delegated delivery steered into a turn that ended before the steer
/// was sent is sent again as a queued message of its own, so its results
/// still reach the thread: the reference's steer fails with
/// `turnCompleted` and its EffectWorker dispatches the message again
/// (`queue_after_active` for a delivery). The command id is the steer's, so
/// this happens once. (A steer the CLI received is delivered; Claude
/// answers it as a wake.)
async fn resend_untaken_deliveries<H: CommandHost>(state: Arc<H>, thread_id: &str) {
    let untaken: Vec<(String, String, Value, Value)> = {
        let store = state.store.read().await;
        let Some(thread) = store.threads.get(thread_id) else {
            return;
        };
        thread
            .messages
            .iter()
            .filter(|m| m.steer.as_ref().is_some_and(|s| s["untaken"] == true))
            .filter_map(|m| {
                let completion = m.wire.as_ref()?["delegatedCompletion"].clone();
                completion.is_object().then(|| {
                    (
                        m.id.clone(),
                        m.content.clone(),
                        completion,
                        m.notification.clone().unwrap_or(Value::Null),
                    )
                })
            })
            .collect()
    };
    for (message_id, text, completion, notification) in untaken {
        let command = json!({
            "type": "message.dispatch",
            "commandId": join_id("command:steer-follow-up", &[&message_id]),
            "threadId": thread_id,
            "messageId": join_id("message:steer-follow-up", &[&message_id]),
            "text": text,
            "attachments": [],
            "dispatchMode": {"type": "queue_after_active"},
            "createdBy": "agent",
            "creationSource": "server",
            "delegatedCompletion": completion,
            "notification": notification,
        });
        if let Err(error) = super::dispatch_orchestration_command(state.clone(), command).await {
            tracing::warn!(
                ?error,
                thread_id,
                "could not send an untaken delivery again"
            );
        }
    }
}

/// `finalizeDelegatedCompletionDelivery`, once a wake run has ended: the
/// tasks it claimed are delivered (pending again if it was cancelled), and
/// the cohort reserves the next wake for results that waited meanwhile.
pub(crate) async fn finalize_delivery<H: CommandHost>(
    state: &H,
    thread_id: &str,
) -> anyhow::Result<()> {
    let (row, ended) = {
        let store = state.store.read().await;
        let Some(thread) = store.threads.get(thread_id) else {
            return Ok(());
        };
        let row = thread.stored_row();
        let ended = thread.runs.iter().find_map(|run| {
            terminal_status(&run.status)?;
            let message = thread
                .messages
                .iter()
                .find(|m| Some(&m.id) == run.user_message_id.as_ref())?;
            let completion = message.wire.as_ref()?["delegatedCompletion"].clone();
            let parent = completion["parentRunId"].as_str()?;
            let delivery = &row["runExtras"][parent]["delegatedCompletion"]["delivery"];
            (delivery["messageId"] == message.id.as_str()
                && delivery["generation"] == completion["generation"])
                .then(|| (parent.to_owned(), run.status == "cancelled"))
        });
        (row, ended)
    };
    let Some((parent, cancelled)) = ended else {
        return Ok(());
    };
    let cohort = row["runExtras"][parent.as_str()]["delegatedCompletion"].clone();
    let delivered = records(&cohort["delivery"], "taskIds");
    let at = crate::auth_service::iso(crate::auth_service::now());
    let mut tasks = records(&row, "subagents");
    for task in tasks.iter_mut() {
        if task["runId"] == parent.as_str()
            && delivered.contains(&task["id"])
            && task["completionDelivery"]["state"] == "claimed"
        {
            let state = if cancelled { "pending" } else { "delivered" };
            task["completionDelivery"] = json!({"state": state, "observedByRunId": null});
            task["updatedAt"] = json!(at);
        }
    }
    let pending: Vec<Value> = tasks
        .iter()
        .filter(|task| {
            task["runId"] == parent.as_str()
                && task["completionDelivery"]["state"] == "pending"
                && TERMINAL_RUN.contains(&task["status"].as_str().unwrap_or_default())
        })
        .map(|task| task["id"].clone())
        .collect();
    let mut next_cohort = cohort.clone();
    next_cohort["delivery"] = Value::Null;
    if cohort["disposition"] == "open" && !pending.is_empty() {
        let generation = cohort["nextGeneration"].as_u64().unwrap_or(1);
        let ordinal = {
            let store = state.store.read().await;
            store.threads.get(thread_id).map_or(0, |thread| {
                thread.full_projection()["messages"]
                    .as_array()
                    .map_or(0, Vec::len)
            }) + 1
        };
        next_cohort["nextGeneration"] = json!(generation + 1);
        next_cohort["delivery"] = json!({
            "generation": generation,
            "messageId": format!(
                "{}:{}",
                join_id("message", &["thread", thread_id, "ordinal", &ordinal.to_string()]),
                crate::platform::uuid_v4()
            ),
            "taskIds": pending,
        });
        for task in tasks.iter_mut() {
            if pending.contains(&task["id"]) {
                task["completionDelivery"] = json!({"state": "claimed", "observedByRunId": null});
                task["updatedAt"] = json!(at);
            }
        }
    }
    let mut extras = row["runExtras"].clone();
    extras[parent.as_str()]["delegatedCompletion"] = next_cohort;
    state
        .commit(Event::ThreadPatched {
            thread_id: thread_id.to_owned(),
            patch: json!({"row": {"subagents": tasks, "runExtras": extras}}),
        })
        .await?;
    Ok(())
}

/// What resolving a task leaves its queued wake to do.
pub(crate) enum WakeFollowUp {
    None,
    /// No task is left to deliver: the wake leaves the queue.
    Cancel(String),
    /// The wake carries the tasks left: `(message id, parent run, task ids)`.
    Update(Value, String, Vec<Value>),
}

/// `dispatchDelegatedTaskCompletionDeliveryResolution`'s cohort update: an
/// acknowledged or disposed task leaves the delivery that carried it, which
/// is cleared once it carries none and was never sent or is still queued.
/// `None` when no delivery carries the task.
pub(crate) async fn resolve_delivery_task<H: CommandHost>(
    state: &H,
    thread_id: &str,
    task_id: &Value,
    task_run: &Value,
) -> Option<(Value, WakeFollowUp)> {
    let store = state.store.read().await;
    let thread = store.threads.get(thread_id)?;
    let row = thread.stored_row();
    let parent = task_run.as_str()?;
    let cohort = &row["runExtras"][parent]["delegatedCompletion"];
    let delivery = &cohort["delivery"];
    let ids = records(delivery, "taskIds");
    if !delivery.is_object() || !ids.contains(task_id) {
        return None;
    }
    let remaining: Vec<Value> = ids.into_iter().filter(|id| id != task_id).collect();
    let wake = thread
        .runs
        .iter()
        .find(|run| run.user_message_id.as_deref() == delivery["messageId"].as_str());
    let queued = wake.is_some_and(|run| run.status == "queued");
    let mut extras = row["runExtras"].clone();
    extras[parent]["delegatedCompletion"]["delivery"] =
        if remaining.is_empty() && (wake.is_none() || queued) {
            Value::Null
        } else {
            let mut next = delivery.clone();
            next["taskIds"] = json!(remaining);
            next
        };
    let follow_up = match wake.filter(|_| queued) {
        Some(run) if remaining.is_empty() => WakeFollowUp::Cancel(run.wire_id()),
        Some(_) => {
            WakeFollowUp::Update(delivery["messageId"].clone(), parent.to_owned(), remaining)
        }
        None => WakeFollowUp::None,
    };
    Some((extras, follow_up))
}

/// Carries out what resolving a task left its queued wake to do.
pub(crate) async fn follow_up_delivery<H: CommandHost>(
    state: &H,
    thread_id: &str,
    follow_up: WakeFollowUp,
    tasks: &[Value],
) -> anyhow::Result<()> {
    match follow_up {
        WakeFollowUp::None => Ok(()),
        WakeFollowUp::Cancel(run_id) => {
            state
                .commit(Event::RunFinished {
                    run_id: crate::Run::uuid_for(&run_id),
                    thread_id: thread_id.to_owned(),
                    status: "cancelled".into(),
                    error: None,
                })
                .await?;
            Ok(())
        }
        WakeFollowUp::Update(message_id, parent, ids) => {
            update_queued_wake(state, thread_id, &parent, &message_id, &ids, tasks).await
        }
    }
}

/// Starts the thread's next queued run once it has no live run, as the
/// reference dispatches the next queued run when the active one ends
/// (`startNextQueuedRun`): a wake or a queued message, in delivery order,
/// unless a hold keeps it waiting.
async fn start_queued_wake<H: CommandHost>(state: Arc<H>, thread_id: &str) {
    let next = {
        let store = state.store.read().await;
        let Some(thread) = store.threads.get(thread_id) else {
            return;
        };
        super::next_queued_run(thread).map(|run| {
            let wake = thread
                .messages
                .iter()
                .find(|m| Some(&m.id) == run.user_message_id.as_ref())
                .is_some_and(crate::thread_projection::agent_notification);
            (run.wire_id(), wake)
        })
    };
    let Some((run_id, wake)) = next else {
        return;
    };
    let prefix = if wake {
        "command:server:delegated-completion:start"
    } else {
        "command:server:queued-run:start"
    };
    let command = json!({
        "type": "queued-run.start",
        "commandId": join_id(prefix, &[&run_id]),
        "threadId": thread_id,
        "runId": run_id,
    });
    if let Err(error) = super::dispatch_orchestration_command(state, command).await {
        tracing::warn!(?error, thread_id, "could not start the next queued run");
    }
}

/// `finalizeAppOwnedSubagent` for a child whose work is done.
async fn finalize_child<H: CommandHost>(
    state: Arc<H>,
    child_id: &str,
    parent_id: &str,
) -> anyhow::Result<()> {
    let _guard = state.mutation_lock().lock().await;
    // Each of the child's tasks settles from its own runs: the original from
    // the child's progress, a follow-up from the runs tagged with it.
    while settle_next(&state, child_id, parent_id).await? {}
    Ok(())
}

/// Settles the first of `child_id`'s unsettled tasks whose work has ended
/// (`finalizeAppOwnedSubagentTask`); whether one was.
async fn settle_next<H: CommandHost>(
    state: &Arc<H>,
    child_id: &str,
    parent_id: &str,
) -> anyhow::Result<bool> {
    let (child, parent_row, parent_projection) = {
        let store = state.store.read().await;
        let Some(child) = store.threads.get(child_id) else {
            return Ok(false);
        };
        let Some(parent) = store.threads.get(parent_id) else {
            return Ok(false);
        };
        let parent_row = parent.stored_row();
        // No unsettled task of this child: nothing to read (the projections
        // of a thread read from the shared database are read when needed).
        if !records(&parent_row, "subagents").iter().any(|t| {
            t["origin"] == "app_owned" && t["childThreadId"] == child_id && t["result"].is_null()
        }) {
            return Ok(false);
        }
        (
            child.full_projection(),
            parent_row,
            parent.full_projection(),
        )
    };
    let task_node = child["thread"]["forkedFrom"]["nodeId"].clone();
    let (work, progress_run) = progress(&child);
    let tasks = records(&parent_row, "subagents");
    let picked = tasks
        .iter()
        .filter(|t| {
            t["origin"] == "app_owned" && t["childThreadId"] == child_id && t["result"].is_null()
        })
        .find_map(|task| {
            let task_runs = delegated_task_runs(&child, &task["id"]);
            let run = if task_runs.is_empty() {
                progress_run
                    .clone()
                    .filter(|_| task["id"] == task_node && work == "result_available")
            } else if delegated_task_awaits_work(&task_runs, &child) {
                // A follow-up ends with the work it awaits, as the original task does.
                None
            } else {
                delegated_task_result_run(&task_runs)
            }?;
            let terminal = terminal_status(run["status"].as_str().unwrap_or_default())?;
            Some((task.clone(), run, terminal))
        });
    let Some((task, result_run, terminal)) = picked else {
        return Ok(false);
    };
    // A follow-up's run passes through `waiting` as its turn ends, which
    // the reference's run reactor syncs to the task before it settles.
    if result_run["delegatedTaskId"] == task["id"]
        && result_run["status"] == "completed"
        && task["status"] == "running"
    {
        sync_task_status(&**state, parent_id, &parent_row, &task, "waiting").await?;
    }
    let parent_row = {
        let store = state.store.read().await;
        store
            .threads
            .get(parent_id)
            .map_or(parent_row, |parent| parent.stored_row())
    };
    let mut tasks = records(&parent_row, "subagents");
    let mut source_point =
        context_source_point(&child, result_run["id"].as_str().unwrap_or_default());
    // The result describes the child at its terminal run update. A stopped
    // run gets its rollback checkpoint in a later update.
    if matches!(
        result_run["status"].as_str(),
        Some("interrupted" | "cancelled")
    ) {
        source_point.as_object_mut().unwrap().remove("checkpointId");
    }
    // A cancelled task's completed run ends it interrupted.
    let cancelled = task["cancelledRunId"] == result_run["id"];
    let terminal = if cancelled && terminal == "completed" {
        "interrupted"
    } else {
        terminal
    };
    let mut result_run = result_run;
    result_run["status"] = json!(terminal);
    let at = crate::auth_service::iso(crate::auth_service::now());
    let (text, turn_item) = result_for_run(&child, &result_run);
    let mut updated = task.clone();
    updated["providerThreadId"] = result_run["providerThreadId"].clone();
    updated["status"] = json!(terminal);
    updated["result"] = json!(text);
    updated["completedAt"] = json!(at);
    updated["updatedAt"] = json!(at);
    // `planDelegatedCompletionDelivery`.
    let parent_run = records(&parent_projection, "runs")
        .into_iter()
        .find(|run| run["id"] == task["runId"]);
    let mut extras = parent_row["runExtras"].clone();
    if !extras.is_object() {
        extras = json!({});
    }
    // The cancelling parent gets no wake.
    if cancelled {
        updated["cancelledRunId"] = result_run["id"].clone();
        updated["completionDelivery"] = json!({"state": "disposed", "observedByRunId": null});
    }
    let queued_wake = plan_delivery(
        parent_id,
        &parent_projection,
        &task,
        &mut updated,
        &mut extras,
    );
    replace_by_id(&mut tasks, &task["id"], |t| *t = updated.clone());
    let mut nodes = records(&parent_row, "nodes");
    replace_by_id(&mut nodes, &task["id"], |node| {
        node["status"] = json!(terminal);
        node["providerThreadId"] = result_run["providerThreadId"].clone();
        node["completedAt"] = json!(at);
    });
    let mut items = records(&parent_row, "appTurnItems");
    if let Some(item) = items
        .iter_mut()
        .find(|i| i["type"] == "subagent" && i["subagentId"] == task["id"])
    {
        item["status"] = json!(terminal);
        item["result"] = json!(text);
        item["completedAt"] = json!(at);
        item["updatedAt"] = json!(at);
    }
    if !turn_item.is_null() {
        source_point["turnItemId"] = turn_item;
    }
    let transfer = json!({
        "id": format!(
            "{}:{}",
            join_id(
                "context-transfer",
                &["type", "subagent_result", "source-thread", child_id, "target-thread", parent_id]
            ),
            crate::platform::uuid_v4()
        ),
        "type": "subagent_result",
        "sourceThreadId": child_id,
        "targetThreadId": parent_id,
        "sourcePoint": source_point,
        "basePoint": null,
        "sourceProviderInstanceId": result_run["providerInstanceId"],
        "targetProviderInstanceId": parent_run
            .as_ref()
            .map(|run| run["providerInstanceId"].clone())
            .unwrap_or_else(|| parent_projection["thread"]["providerInstanceId"].clone()),
        "targetRunId": parent_run.as_ref().map(|run| run["id"].clone()).unwrap_or(Value::Null),
        "status": "consumed",
        "resolution": null,
        "createdBy": "system",
        "error": null,
        "createdAt": at,
        "updatedAt": at,
        "consumedAt": at,
    });
    let mut child_transfers = records(&child, "contextTransfers");
    child_transfers.push(transfer.clone());
    state
        .commit(Event::ThreadPatched {
            thread_id: child_id.to_owned(),
            patch: json!({"row": {"contextTransfers": child_transfers}}),
        })
        .await?;
    let mut parent_transfers = records(&parent_row, "contextTransfers");
    parent_transfers.push(transfer);
    state
        .commit(Event::ThreadPatched {
            thread_id: parent_id.to_owned(),
            patch: json!({"row": {
                "subagents": tasks,
                "nodes": nodes,
                "appTurnItems": items,
                "contextTransfers": parent_transfers,
                "runExtras": extras,
            }}),
        })
        .await?;
    if let Some((message_id, ids)) = queued_wake {
        if let Some(run_id) = task["runId"].as_str() {
            update_queued_wake(&**state, parent_id, run_id, &message_id, &ids, &tasks).await?;
        }
    }
    if work == "result_available" {
        release_finished_delegate(state, child_id, &child).await?;
    }
    Ok(true)
}

/// `planDelegatedCompletionDelivery`: how the result of `task` (ending as `updated`) reaches
/// `parent_id`: disposed, left to the blocking wait that spawned it, added to the parent run's
/// pending or queued delivery (the queued wake's message and tasks, returned), or a new one
/// recorded in the run's `extras`.
fn plan_delivery(
    parent_id: &str,
    parent_projection: &Value,
    task: &Value,
    updated: &mut Value,
    extras: &mut Value,
) -> Option<(Value, Vec<Value>)> {
    let parent_run = records(parent_projection, "runs")
        .into_iter()
        .find(|run| run["id"] == task["runId"]);
    let mut queued_wake = None;
    let delivery_state = updated["completionDelivery"]["state"]
        .as_str()
        .map(str::to_owned);
    if !matches!(
        delivery_state.as_deref(),
        Some("acknowledged" | "delivered" | "disposed")
    ) {
        let cohort = parent_run
            .as_ref()
            .map(|run| run["delegatedCompletion"].clone())
            .unwrap_or(Value::Null);
        let parent_live = parent_run.as_ref().is_some_and(|run| {
            matches!(
                run["status"].as_str(),
                Some("preparing" | "starting" | "running" | "waiting")
            )
        });
        let thread = &parent_projection["thread"];
        if parent_run.is_none()
            || !thread["archivedAt"].is_null()
            || !thread["deletedAt"].is_null()
            || (cohort.is_object() && cohort["disposition"] != "open")
        {
            updated["completionDelivery"] = json!({"state": "disposed", "observedByRunId": null});
        } else if task["completionWake"].as_str().unwrap_or("settled_only") == "settled_only"
            && parent_live
        {
            // The blocking wait that spawned it delivers it.
        } else if let Some(run) = &parent_run {
            let delivery = &cohort["delivery"];
            // The run the delivery's message started, if it was sent.
            let delivery_run = records(parent_projection, "runs")
                .into_iter()
                .find(|r| delivery.is_object() && r["userMessageId"] == delivery["messageId"]);
            let next = if delivery_run
                .as_ref()
                .is_some_and(|r| r["status"] != "queued")
            {
                // A wake already running has this result wait; its end
                // reserves the next wake (`finalize_delivery`).
                None
            } else if delivery.is_object() {
                let mut ids = records(delivery, "taskIds");
                if !ids.contains(&task["id"]) {
                    ids.push(task["id"].clone());
                }
                // A queued wake carries this result too.
                if delivery_run.is_some() {
                    queued_wake = Some((delivery["messageId"].clone(), ids.clone()));
                }
                let mut next = cohort.clone();
                next["delivery"]["taskIds"] = Value::Array(ids);
                Some(next)
            } else {
                let generation = cohort["nextGeneration"].as_u64().unwrap_or(1);
                let ordinal = records(parent_projection, "messages").len() + 1;
                Some(json!({
                    "disposition": "open",
                    "nextGeneration": generation + 1,
                    "delivery": {
                        "generation": generation,
                        "messageId": format!(
                            "{}:{}",
                            join_id("message", &["thread", parent_id, "ordinal", &ordinal.to_string()]),
                            crate::platform::uuid_v4()
                        ),
                        "taskIds": [task["id"]],
                    },
                }))
            };
            if let Some(next) = next {
                updated["completionDelivery"] =
                    json!({"state": "claimed", "observedByRunId": null});
                extras[run["id"].as_str().unwrap_or_default()]["delegatedCompletion"] = next;
            } else {
                updated["completionDelivery"] =
                    json!({"state": "pending", "observedByRunId": null});
            }
        }
    }
    queued_wake
}

/// `dispatchRemoteDelegatedTaskRequest`: records a task whose thread runs in a linked
/// environment, as a local task is recorded on its parent (its node, the task and its card),
/// with no child thread here.
pub(super) async fn remote_request<H: CommandHost>(
    state: Arc<H>,
    command: &Value,
    receipt: &CommandReceipt,
) -> anyhow::Result<u64> {
    let command_id = command["commandId"].as_str().unwrap_or_default();
    let parent_id = command["parentThreadId"].as_str().unwrap_or_default();
    let parent_run_id = command["parentRunId"].as_str().unwrap_or_default();
    let parent_node_id = command["parentNodeId"].as_str().unwrap_or_default();
    let parent = state
        .store
        .read()
        .await
        .threads
        .get(parent_id)
        .cloned()
        .ok_or_else(|| {
            anyhow::anyhow!(format!(
                "No orchestration projection exists for thread {parent_id}."
            ))
        })?;
    let projection = parent.full_projection();
    let parent_row = parent.stored_row();
    let runs = records(&projection, "runs");
    let remote = &command["remoteChild"];
    // A follow-up to a finished task there binds as one to a child here does.
    let follows_up = records(&parent_row, "subagents").iter().any(|task| {
        !task["result"].is_null()
            && task["remoteChild"]["environmentId"] == remote["environmentId"]
            && task["remoteChild"]["threadId"] == remote["threadId"]
    });
    let parent_run = runs.iter().find(|run| run["id"] == parent_run_id);
    let bound = parent_run.is_some_and(|run| {
        if follows_up {
            follow_up_owner_run(&runs).is_some_and(|owner| owner["id"] == run["id"])
        } else {
            live_run(run)
        }
    });
    let Some(parent_run) = parent_run.filter(|run| bound && !run["rootNodeId"].is_null()) else {
        anyhow::bail!("Parent run {parent_run_id} is not active.");
    };
    let at = crate::auth_service::iso(crate::auth_service::now());
    let task_node = task_node_id(command_id);
    let prompt = command["task"].as_str().unwrap_or_default();
    let ordinal = parent_row["subagents"].as_array().map_or(0, Vec::len) + 1;
    let title = subagent_thread_title(&parent.title, command["title"].as_str(), prompt, ordinal);
    let instance = &command["modelSelection"]["instanceId"];
    let provider_turn = projection["providerTurns"]
        .as_array()
        .into_iter()
        .flatten()
        .rfind(|turn| turn["runId"] == parent_run_id)
        .map(|turn| turn["id"].clone())
        .unwrap_or(Value::Null);
    let after_items = parent
        .runs
        .iter()
        .position(|r| r.wire_id() == parent_run_id)
        .map_or(0, |index| {
            parent
                .provider_records()
                .items
                .iter()
                .filter(|(i, _)| *i == index)
                .count()
        });
    let mut task = json!({
        "id": task_node,
        "threadId": parent_id,
        "runId": parent_run_id,
        "parentNodeId": parent_node_id,
        "origin": "app_owned",
        "createdBy": "agent",
        "driver": command["driver"],
        "providerInstanceId": instance,
        "providerThreadId": null,
        "childThreadId": null,
        "remoteChild": remote,
        "nativeTaskRef": null,
        "prompt": prompt,
        "title": command.get("title").cloned().unwrap_or(Value::Null),
        "model": command["modelSelection"]["model"],
        "modelSelection": command["modelSelection"],
        "status": "running",
        "result": null,
        "startedAt": at,
        "completedAt": null,
        "updatedAt": at,
    });
    if let Some(wake) = command.get("completionWake") {
        task["completionWake"] = wake.clone();
    }
    let node = json!({
        "id": task_node,
        "threadId": parent_id,
        "runId": parent_run_id,
        "parentNodeId": parent_node_id,
        "rootNodeId": parent_run["rootNodeId"],
        "kind": "subagent",
        "status": "running",
        "countsForRun": false,
        "providerThreadId": null,
        "providerTurnId": null,
        "nativeItemRef": null,
        "runtimeRequestId": null,
        "checkpointScopeId": null,
        "startedAt": at,
        "completedAt": null,
    });
    let item = json!({
        "id": task_turn_item_id(command_id),
        "threadId": parent_id,
        "runId": parent_run_id,
        "nodeId": task_node,
        "providerThreadId": parent_run["providerThreadId"],
        "providerTurnId": provider_turn,
        "nativeItemRef": null,
        "parentItemId": null,
        "status": "running",
        "title": command["title"].as_str().unwrap_or(&title),
        "startedAt": at,
        "completedAt": null,
        "updatedAt": at,
        "type": "subagent",
        "subagentId": task_node,
        "origin": "app_owned",
        "driver": command["driver"],
        "providerInstanceId": instance,
        "childThreadId": null,
        "remoteChild": remote,
        "prompt": prompt,
        "result": null,
        (crate::thread_projection::LINK_CARD_POSITION): after_items,
    });
    let push = |key: &str, record: Value| {
        let mut records = parent_row[key].as_array().cloned().unwrap_or_default();
        records.push(record);
        Value::Array(records)
    };
    Ok(state
        .commit_receipted(
            Event::ThreadPatched {
                thread_id: parent_id.to_owned(),
                patch: json!({"row": {
                    "subagents": push("subagents", task),
                    "nodes": push("nodes", node),
                    "appTurnItems": push("appTurnItems", item),
                }}),
            },
            Some(receipt.clone()),
        )
        .await?
        .id)
}

/// `dispatchRemoteDelegatedTaskComplete`: a remote task's result, the parent half of
/// finalizing a task, or that nothing is left to stop there. The first result stands; a
/// report that changes nothing is refused.
pub(super) async fn remote_complete<H: CommandHost>(
    state: Arc<H>,
    command: &Value,
    receipt: &CommandReceipt,
) -> anyhow::Result<u64> {
    let parent_id = command["parentThreadId"].as_str().unwrap_or_default();
    let task_id = command["taskId"].as_str().unwrap_or_default();
    let (parent_row, parent_projection) = {
        let store = state.store.read().await;
        let parent = store.threads.get(parent_id).ok_or_else(|| {
            anyhow::anyhow!(format!(
                "No orchestration projection exists for thread {parent_id}."
            ))
        })?;
        (parent.stored_row(), parent.full_projection())
    };
    let mut tasks = records(&parent_row, "subagents");
    let task = tasks
        .iter()
        .find(|t| t["id"] == task_id && t["origin"] == "app_owned" && t["remoteChild"].is_object())
        .cloned()
        .ok_or_else(|| {
            anyhow::anyhow!(format!(
                "Delegated task {task_id} is not a remote task of thread {parent_id}."
            ))
        })?;
    let stopped_there = command["stoppedThere"].as_str();
    let unchanged = || anyhow::anyhow!(format!("Delegated task {task_id} already has its result."));
    if stopped_there == Some("stopped")
        && command["stopCount"].as_u64().unwrap_or(0)
            != task["remoteChild"]["stopCount"].as_u64().unwrap_or(0)
    {
        return Err(unchanged());
    }
    let commit = |tasks: Vec<Value>, extra: Option<(Vec<Value>, Vec<Value>, Value)>| {
        let mut row = json!({"subagents": tasks});
        if let Some((nodes, items, extras)) = extra {
            row["nodes"] = json!(nodes);
            row["appTurnItems"] = json!(items);
            row["runExtras"] = extras;
        }
        Event::ThreadPatched {
            thread_id: parent_id.to_owned(),
            patch: json!({"row": row}),
        }
    };
    let at = crate::auth_service::iso(crate::auth_service::now());
    if !task["result"].is_null() {
        let owes_stop = matches!(task["status"].as_str(), Some("cancelled" | "interrupted"))
            || task["remoteChild"]["stoppedThere"] == false;
        let next = match stopped_there {
            // A Stop or cancel here that lost the race to the result still owes its stop.
            None if owes_stop && task["remoteChild"]["stoppedThere"] != true => false,
            None => return Err(unchanged()),
            Some(_) if task["remoteChild"]["stoppedThere"] == true => return Err(unchanged()),
            Some(how) if owes_stop && how != "stopped" => return Err(unchanged()),
            Some(_) => true,
        };
        if task["remoteChild"]["stoppedThere"] == next {
            return Err(unchanged());
        }
        replace_by_id(&mut tasks, &task["id"], |t| {
            t["remoteChild"]["stoppedThere"] = json!(next);
            t["updatedAt"] = json!(at);
        });
        return Ok(state
            .commit_receipted(commit(tasks, None), Some(receipt.clone()))
            .await?
            .id);
    }
    let status = command["status"].as_str().unwrap_or("failed");
    let mut updated = task.clone();
    if stopped_there.is_some() {
        updated["remoteChild"]["stoppedThere"] = json!(true);
    }
    updated["status"] = json!(status);
    updated["result"] = command["result"].clone();
    if let Some(model) = command.get("executedModel").filter(|m| m.is_string()) {
        updated["executedModel"] = model.clone();
    }
    updated["completedAt"] = json!(at);
    updated["updatedAt"] = json!(at);
    let mut extras = parent_row["runExtras"].clone();
    if !extras.is_object() {
        extras = json!({});
    }
    let queued_wake = plan_delivery(
        parent_id,
        &parent_projection,
        &task,
        &mut updated,
        &mut extras,
    );
    replace_by_id(&mut tasks, &task["id"], |t| *t = updated.clone());
    let mut nodes = records(&parent_row, "nodes");
    replace_by_id(&mut nodes, &task["id"], |node| {
        node["status"] = json!(status);
        node["completedAt"] = json!(at);
    });
    let mut items = records(&parent_row, "appTurnItems");
    if let Some(item) = items
        .iter_mut()
        .find(|i| i["type"] == "subagent" && i["subagentId"] == task["id"])
    {
        item["status"] = json!(status);
        item["result"] = command["result"].clone();
        item["completedAt"] = json!(at);
        item["updatedAt"] = json!(at);
    }
    let sequence = state
        .commit_receipted(
            commit(tasks.clone(), Some((nodes, items, extras))),
            Some(receipt.clone()),
        )
        .await?
        .id;
    if let Some((message_id, ids)) = queued_wake {
        if let Some(run_id) = task["runId"].as_str() {
            update_queued_wake(&*state, parent_id, run_id, &message_id, &ids, &tasks).await?;
        }
    }
    Ok(sequence)
}

/// After a remote task's result is recorded: a wait on it reads the result, and the parent
/// gets its delivery.
pub(crate) async fn after_remote_complete<H: CommandHost>(
    state: Arc<H>,
    parent_id: &str,
    task_id: &str,
) {
    let releases = let_waiters_read(task_id).await;
    drop(releases);
    offer_delivery(state, parent_id).await;
}

/// `releaseFinishedDelegateSessions`: a child whose work is done gives up
/// its own provider process (a session that serves one thread, as
/// Claude's), so it does not linger until the idle timeout; a follow-up
/// resumes the session in a new one. Unawaited background work keeps it.
async fn release_finished_delegate<H: CommandHost>(
    state: &Arc<H>,
    child_id: &str,
    child: &Value,
) -> anyhow::Result<()> {
    let capabilities: Value =
        serde_json::from_str(include_str!("../provider_capabilities.json")).unwrap_or_default();
    let background = records(child, "providerThreads").iter().any(|thread| {
        thread["pendingBackgroundTasks"]
            .as_array()
            .is_some_and(|tasks| !tasks.is_empty())
    });
    let per_thread = records(child, "providerSessions").iter().any(|session| {
        !matches!(session["status"].as_str(), Some("stopped" | "error"))
            && capabilities[session["providerInstanceId"].as_str().unwrap_or_default()]["sessions"]
                ["supportsMultipleProviderThreadsPerSession"]
                == false
    });
    if background || !per_thread {
        return Ok(());
    }
    super::detach_session(state, child_id, "Delegated task finished.", false).await
}

/// A delegated task's live status (`finalizeAppOwnedSubagentTask` with a
/// `syncRunId`): the task, its node and turn item take `status`.
async fn sync_task_status<H: CommandHost>(
    state: &H,
    parent_id: &str,
    parent_row: &Value,
    task: &Value,
    status: &str,
) -> anyhow::Result<()> {
    let at = crate::auth_service::iso(crate::auth_service::now());
    let mut tasks = records(parent_row, "subagents");
    replace_by_id(&mut tasks, &task["id"], |t| {
        t["status"] = json!(status);
        t["completedAt"] = Value::Null;
        t["updatedAt"] = json!(at);
    });
    let mut nodes = records(parent_row, "nodes");
    replace_by_id(&mut nodes, &task["id"], |node| {
        node["status"] = json!(status)
    });
    let mut items = records(parent_row, "appTurnItems");
    if let Some(item) = items
        .iter_mut()
        .find(|i| i["type"] == "subagent" && i["subagentId"] == task["id"])
    {
        item["status"] = json!(status);
        item["updatedAt"] = json!(at);
    }
    state
        .commit(Event::ThreadPatched {
            thread_id: parent_id.to_owned(),
            patch: json!({"row": {"subagents": tasks, "nodes": nodes, "appTurnItems": items}}),
        })
        .await?;
    Ok(())
}

/// A queued wake's message after the tasks it carries changed: its text,
/// `delegatedCompletion` and card name `task_ids`, as the reference's
/// `message.updated` does.
async fn update_queued_wake<H: CommandHost>(
    state: &H,
    thread_id: &str,
    parent_run: &str,
    message_id: &Value,
    task_ids: &[Value],
    tasks: &[Value],
) -> anyhow::Result<()> {
    let Some(message_id) = message_id.as_str() else {
        return Ok(());
    };
    let wire = {
        let store = state.store.read().await;
        store
            .threads
            .get(thread_id)
            .and_then(|thread| thread.messages.iter().find(|m| m.id == message_id))
            .and_then(|message| message.wire.as_deref().cloned())
    };
    let Some(mut wire) = wire else {
        return Ok(());
    };
    wire["delegatedCompletion"]["taskIds"] = json!(task_ids);
    state
        .commit(Event::MessageEdited {
            thread_id: thread_id.to_owned(),
            message_id: message_id.to_owned(),
            content: wake_detail(task_ids, tasks),
            wire: Some(wire),
            notification: Some(completion_notification(parent_run, task_ids, tasks)),
        })
        .await?;
    Ok(())
}

/// Delivers a thread's open delivery as the reference's continuation
/// service does: a message queued after the active run (or steered into it)
/// that wakes the thread with the finished tasks (`delegatedCompletion`).
/// A steer accepts it at once (`acceptDelegatedCompletionDelivery`); a wake
/// run settles it when it ends (`finalize_delivery`).
async fn offer_delivery<H: CommandHost>(state: Arc<H>, thread_id: &str) {
    let pending = {
        let store = state.store.read().await;
        let Some(thread) = store.threads.get(thread_id) else {
            return;
        };
        let busy = thread
            .runs
            .iter()
            .any(|run| matches!(run.status.as_str(), "running" | "queued" | "preparing"));
        let row = thread.stored_row();
        let found = row["runExtras"].as_object().and_then(|extras| {
            extras.iter().find_map(|(run_id, extra)| {
                let cohort = &extra["delegatedCompletion"];
                (cohort["disposition"] == "open" && cohort["delivery"].is_object())
                    .then(|| (run_id.clone(), cohort.clone()))
            })
        });
        found.map(|(run_id, cohort)| (run_id, cohort, row, busy))
    };
    let Some((run_id, cohort, row, busy)) = pending else {
        return;
    };
    let delivery = &cohort["delivery"];
    let task_ids = records(delivery, "taskIds");
    if task_ids.is_empty() {
        return;
    }
    // A host whose runs start no queued wakes as they end delivers to a
    // busy thread only by steering into its running turn.
    if busy && !state.hooks_run_ends() {
        let completion = json!({"parentRunId": run_id, "taskIds": task_ids});
        if super::delivery_steer_target(&state, thread_id, &completion)
            .await
            .is_none()
        {
            return;
        }
    }
    let tasks = records(&row, "subagents");
    // The delivery message records the acceptance in its own command, so an
    // offer made again (after a restart, say) finds it sent and stops here.
    {
        let store = state.store.read().await;
        let sent = store.threads.get(thread_id).is_some_and(|thread| {
            thread
                .messages
                .iter()
                .any(|m| Some(m.id.as_str()) == delivery["messageId"].as_str())
        });
        if sent {
            return;
        }
    }
    let accepted = tasks;
    let command = json!({
        "type": "message.dispatch",
        "commandId": join_id(
            "command:server:delegated-completion:dispatch",
            &[&run_id, &delivery["generation"].to_string()]
        ),
        "threadId": thread_id,
        "messageId": delivery["messageId"],
        "text": completion_text(&task_ids),
        "attachments": [],
        "dispatchMode": {"type": "queue_after_active"},
        "createdBy": "agent",
        "creationSource": "server",
        "delegatedCompletion": {
            "parentRunId": run_id,
            "generation": delivery["generation"],
            "taskIds": task_ids,
        },
        "notification": completion_notification(&run_id, &task_ids, &accepted),
    });
    if let Err(error) = super::dispatch_orchestration_command(state, command).await {
        tracing::warn!(
            ?error,
            thread_id,
            "could not deliver finished delegated tasks"
        );
    }
}

/// `disposeDelegatedCompletionCohort` with `disposition: "stopped"` for each
/// of `run_ids` on `thread_id`, as a stop or interrupt closes them: the
/// cohort takes no more deliveries, and its tasks' undelivered results are
/// disposed. With `thread_stop`, also the runs whose cohorts hold a delivery
/// or whose tasks still owe one (`holdStoppedThread`).
pub(crate) async fn stop_cohorts<H: CommandHost>(
    state: &Arc<H>,
    thread_id: &str,
    run_ids: &[String],
    thread_stop: bool,
) -> anyhow::Result<()> {
    let (row, projection) = {
        let store = state.store.read().await;
        let Some(thread) = store.threads.get(thread_id) else {
            return Ok(());
        };
        (thread.stored_row(), thread.full_projection())
    };
    let runs = records(&projection, "runs");
    let mut tasks = records(&row, "subagents");
    let owes = |task: &Value| {
        !matches!(
            task["completionDelivery"]["state"].as_str(),
            Some("acknowledged" | "delivered" | "disposed")
        )
    };
    let mut cohort_runs: Vec<String> = run_ids.to_vec();
    if thread_stop {
        cohort_runs.extend(
            runs.iter()
                .filter(|run| {
                    !run["delegatedCompletion"]["delivery"].is_null()
                        && run["delegatedCompletion"].is_object()
                })
                .filter_map(|run| run["id"].as_str().map(str::to_owned)),
        );
        cohort_runs.extend(
            tasks
                .iter()
                .filter(|task| task["origin"] == "app_owned" && owes(task))
                .filter_map(|task| task["runId"].as_str().map(str::to_owned)),
        );
    }
    let mut extras = row["runExtras"].clone();
    if !extras.is_object() {
        extras = json!({});
    }
    let at = crate::auth_service::iso(crate::auth_service::now());
    let mut changed = false;
    let mut seen = std::collections::HashSet::new();
    for run_id in cohort_runs {
        if !seen.insert(run_id.clone()) {
            continue;
        }
        let Some(run) = runs.iter().find(|run| run["id"] == run_id.as_str()) else {
            continue;
        };
        let cohort = &run["delegatedCompletion"];
        let owned = tasks
            .iter()
            .any(|task| task["origin"] == "app_owned" && task["runId"] == run_id.as_str());
        if !cohort.is_object() && !owned {
            continue;
        }
        extras[run_id.as_str()]["delegatedCompletion"] = json!({
            "disposition": if cohort["disposition"] == "disposed" { "disposed" } else { "stopped" },
            "nextGeneration": cohort["nextGeneration"].as_u64().unwrap_or(1),
            "delivery": null,
        });
        for task in tasks
            .iter_mut()
            .filter(|task| task["origin"] == "app_owned" && task["runId"] == run_id.as_str())
        {
            if owes(task) {
                task["completionDelivery"] = json!({"state": "disposed", "observedByRunId": null});
                task["updatedAt"] = json!(at);
            }
        }
        changed = true;
    }
    if changed {
        state
            .commit(Event::ThreadPatched {
                thread_id: thread_id.to_owned(),
                // `disposeDelegatedCompletionCohort` writes each run, then
                // its tasks.
                patch: json!({"row": {"runExtras": extras, "subagents": tasks}, "cohortStopped": true}),
            })
            .await?;
    }
    Ok(())
}

/// `delegatedTaskRuns`: a follow-up task's runs, those tagged with it and the
/// untagged wakes for subagents they started.
pub(crate) fn delegated_task_runs(child: &Value, task_id: &Value) -> Vec<Value> {
    let runs = records(child, "runs");
    let mut owned: std::collections::HashSet<String> = runs
        .iter()
        .filter(|run| run["delegatedTaskId"] == *task_id)
        .filter_map(|run| run["id"].as_str().map(str::to_owned))
        .collect();
    let untagged: std::collections::HashSet<String> = runs
        .iter()
        .filter(|run| run["delegatedTaskId"].is_null())
        .filter_map(|run| run["id"].as_str().map(str::to_owned))
        .collect();
    let messages = records(child, "messages");
    let mut grew = !owned.is_empty();
    while grew {
        grew = false;
        for message in &messages {
            let Some(run_id) = message["runId"].as_str() else {
                continue;
            };
            let parent_run = message["delegatedCompletion"]["parentRunId"].as_str();
            if untagged.contains(run_id)
                && !owned.contains(run_id)
                && parent_run.is_some_and(|parent| owned.contains(parent))
            {
                owned.insert(run_id.to_owned());
                grew = true;
            }
        }
    }
    runs.into_iter()
        .filter(|run| run["id"].as_str().is_some_and(|id| owned.contains(id)))
        .collect()
}

/// `delegatedTaskAwaitsWork`: a follow-up's runs still owe a subagent they
/// started its result.
pub(crate) fn delegated_task_awaits_work(runs: &[Value], child: &Value) -> bool {
    records(child, "subagents").iter().any(|task| {
        runs.iter().any(|run| run["id"] == task["runId"])
            && (work_active(&task["status"])
                || (task["status"] != "idle"
                    && matches!(
                        task["completionDelivery"]["state"].as_str(),
                        Some("pending" | "claimed")
                    )))
    })
}

/// `delegatedTaskResultRun`: a follow-up's latest run, passing over queued
/// work it no longer waits on.
pub(crate) fn delegated_task_result_run(runs: &[Value]) -> Option<Value> {
    let mut sorted: Vec<&Value> = runs.iter().collect();
    sorted.sort_by_key(|run| std::cmp::Reverse(run["ordinal"].as_u64().unwrap_or(0)));
    let awaited: Vec<&Value> = sorted
        .iter()
        .copied()
        .filter(|run| {
            let cancelled_before_start = run["status"] == "cancelled" && run["startedAt"].is_null();
            let held = run["status"] == "queued" && run["queueHeld"] == true;
            !(cancelled_before_start || held)
        })
        .collect();
    awaited
        .iter()
        .find(|run| work_active(&run["status"]) || run["status"] == "preparing")
        .or_else(|| awaited.iter().find(|run| run["status"] == "queued"))
        .or(awaited.first())
        .or(sorted.first())
        .map(|run| (*run).clone())
}

/// `followUpOwnerRun`: the parent's latest active run, else the last that ran.
pub(crate) fn follow_up_owner_run(runs: &[Value]) -> Option<Value> {
    let active = |run: &&Value| {
        matches!(
            run["status"].as_str(),
            Some("preparing" | "starting" | "running" | "waiting")
        )
    };
    runs.iter()
        .filter(active)
        .max_by_key(|run| run["ordinal"].as_u64().unwrap_or(0))
        .or_else(|| {
            runs.iter()
                .filter(|run| {
                    run["status"] != "queued"
                        && !(run["status"] == "cancelled" && run["startedAt"].is_null())
                })
                .max_by_key(|run| run["ordinal"].as_u64().unwrap_or(0))
        })
        .cloned()
}

/// `mayStartSubagentFollowup`: a message that may start follow-up work on a
/// child. A monitor's progress update only reports.
fn may_start_followup(command: &Value) -> bool {
    if command["scheduledTaskId"].is_string() || command["restartContinuationOfRunId"].is_string() {
        return false;
    }
    if command["notification"].is_object() {
        return command["notification"]["source"]["kind"] != "monitor"
            || command["notification"]["outcome"] != "updated";
    }
    command["createdBy"] == "user"
        || command["creationSource"] == "mcp"
        || command["usageLimitContinuationOfRunId"].is_string()
}

/// `recordSubagentFollowup`: a message that starts a new run on a delegated
/// child whose task already finished becomes a follow-up task on the parent,
/// bound to the parent's active run (or the last that ran), its card on the
/// parent's timeline and its runs tagged with it. A wake continues the open
/// follow-up instead.
pub(crate) async fn record_followup<H: CommandHost>(
    state: &Arc<H>,
    child_id: &str,
    command: &Value,
) -> anyhow::Result<()> {
    if !may_start_followup(command) {
        return Ok(());
    }
    let command_id = command["commandId"].as_str().unwrap_or_default();
    let (child, child_row, parent_row, parent_projection, run_index) = {
        let store = state.store.read().await;
        let Some(thread) = store.threads.get(child_id) else {
            return Ok(());
        };
        let row = thread.stored_row();
        let Some(parent_id) = row["lineage"]["parentThreadId"].as_str() else {
            return Ok(());
        };
        let Some(parent) = store.threads.get(parent_id) else {
            return Ok(());
        };
        let message_id = command["messageId"].as_str();
        let Some(run_index) = thread
            .runs
            .iter()
            .position(|run| run.user_message_id.as_deref() == message_id)
        else {
            return Ok(());
        };
        (
            thread.full_projection(),
            row,
            parent.stored_row(),
            parent.full_projection(),
            run_index,
        )
    };
    let parent_id = child_row["lineage"]["parentThreadId"]
        .as_str()
        .unwrap_or_default();
    // A send with no sender thread comes from a client acting for the owner, like a user send.
    if child_row["lineage"]["relationshipToParent"] != "subagent"
        || child_row["forkedFrom"]["type"] != "node"
        || (!command["notification"].is_object()
            && command["createdBy"] != "user"
            && !command["usageLimitContinuationOfRunId"].is_string()
            && command["senderThreadId"]
                .as_str()
                .is_some_and(|sender| sender != parent_id))
    {
        return Ok(());
    }
    let runs = records(&child, "runs");
    let Some(run) = runs.get(run_index).cloned() else {
        return Ok(());
    };
    if !run["delegatedTaskId"].is_null() {
        return Ok(());
    }
    let run_id = run["id"].as_str().unwrap_or_default().to_owned();
    let mut child_extras = child_row["runExtras"].clone();
    if !child_extras.is_object() {
        child_extras = json!({});
    }
    let mut tasks = records(&parent_row, "subagents");
    let original = tasks
        .iter()
        .find(|t| {
            t["origin"] == "app_owned"
                && t["childThreadId"] == child_id
                && t["id"] == child_row["forkedFrom"]["nodeId"]
        })
        .cloned();
    let Some(original) = original.filter(|t| !t["result"].is_null()) else {
        return Ok(());
    };
    // A wake continues the latest follow-up while it is open.
    if command["notification"].is_object() {
        let latest = runs
            .iter()
            .filter(|r| r["id"] != run["id"] && !r["delegatedTaskId"].is_null())
            .max_by_key(|r| r["ordinal"].as_u64().unwrap_or(0))
            .map(|r| r["delegatedTaskId"].clone());
        if let Some(open) = latest.and_then(|id| {
            tasks.iter().find(|t| {
                t["id"] == id
                    && t["origin"] == "app_owned"
                    && t["childThreadId"] == child_id
                    && t["cancelledRunId"].is_null()
                    && t["result"].is_null()
            })
        }) {
            child_extras[run_id.as_str()]["delegatedTaskId"] = open["id"].clone();
            state
                .commit(Event::ThreadPatched {
                    thread_id: child_id.to_owned(),
                    patch: json!({"row": {"runExtras": child_extras}}),
                })
                .await?;
            return Ok(());
        }
    }
    let Some(owner) = follow_up_owner_run(&records(&parent_projection, "runs"))
        .filter(|owner| !owner["rootNodeId"].is_null())
    else {
        return Ok(());
    };
    let owner_id = owner["id"].as_str().unwrap_or_default().to_owned();
    let at = crate::auth_service::iso(crate::auth_service::now());
    let task_id = task_node_id(command_id);
    let instance = run["providerInstanceId"].as_str().unwrap_or_default();
    let settings = state.effective_settings().unwrap_or(Value::Null);
    let status = if run["status"] == "queued" {
        if run["queueHeld"] == true {
            "idle"
        } else {
            "pending"
        }
    } else {
        "running"
    };
    let mut task = original.clone();
    task["createdBy"] = command["createdBy"].clone();
    task["driver"] = json!(driver_of(&settings, instance));
    task["id"] = json!(task_id);
    task["runId"] = json!(owner_id);
    task["parentNodeId"] = owner["rootNodeId"].clone();
    task["providerInstanceId"] = json!(instance);
    task["providerThreadId"] = run["providerThreadId"].clone();
    task["model"] = run["modelSelection"]["model"].clone();
    task["modelSelection"] = run["modelSelection"].clone();
    task["prompt"] = command["text"].clone();
    task["title"] = child["thread"]["title"].clone();
    task["completionWake"] = json!("always");
    task["completionDelivery"] = json!({"state": "pending", "observedByRunId": null});
    task["status"] = json!(status);
    task["result"] = Value::Null;
    task["startedAt"] = json!(at);
    task["completedAt"] = Value::Null;
    task["updatedAt"] = json!(at);
    if let Some(fields) = task.as_object_mut() {
        fields.remove("progress");
        fields.remove("cancelledRunId");
    }
    let node = json!({
        "id": task_id,
        "threadId": parent_id,
        "runId": owner_id,
        "parentNodeId": owner["rootNodeId"],
        "rootNodeId": owner["rootNodeId"],
        "kind": "subagent",
        "status": status,
        "countsForRun": false,
        "providerThreadId": run["providerThreadId"],
        "providerTurnId": null,
        "nativeItemRef": null,
        "runtimeRequestId": null,
        "checkpointScopeId": null,
        "startedAt": at,
        "completedAt": null,
    });
    // `providerTurnForRun` for the owner, and where the card sits among its items.
    let provider_turn = records(&parent_projection, "providerTurns")
        .into_iter()
        .filter(|turn| {
            !owner["activeAttemptId"].is_null() && turn["runAttemptId"] == owner["activeAttemptId"]
        })
        .max_by_key(|turn| turn["ordinal"].as_u64().unwrap_or(0))
        .map_or(Value::Null, |turn| turn["id"].clone());
    let after_items = {
        let store = state.store.read().await;
        store.threads.get(parent_id).map_or(0, |parent| {
            parent
                .runs
                .iter()
                .position(|r| r.wire_id() == owner_id)
                .map_or(0, |index| {
                    parent
                        .provider_records()
                        .items
                        .iter()
                        .filter(|(i, _)| *i == index)
                        .count()
                })
        })
    };
    let item = json!({
        "id": task_turn_item_id(command_id),
        "threadId": parent_id,
        "runId": owner_id,
        "nodeId": task_id,
        "providerThreadId": owner["providerThreadId"],
        "providerTurnId": provider_turn,
        "nativeItemRef": null,
        "parentItemId": null,
        "status": status,
        "title": child["thread"]["title"],
        "startedAt": at,
        "completedAt": null,
        "updatedAt": at,
        "type": "subagent",
        "subagentId": task_id,
        "origin": "app_owned",
        "driver": task["driver"],
        "providerInstanceId": instance,
        "childThreadId": child_id,
        "prompt": command["text"],
        "result": null,
        (crate::thread_projection::LINK_CARD_POSITION): after_items,
    });
    child_extras[run_id.as_str()]["delegatedTaskId"] = json!(task_id);
    state
        .commit(Event::ThreadPatched {
            thread_id: child_id.to_owned(),
            patch: json!({"row": {"runExtras": child_extras}}),
        })
        .await?;
    tasks.push(task);
    let mut nodes = records(&parent_row, "nodes");
    nodes.push(node);
    let mut items = records(&parent_row, "appTurnItems");
    items.push(item);
    state
        .commit(Event::ThreadPatched {
            thread_id: parent_id.to_owned(),
            patch: json!({"row": {"subagents": tasks, "nodes": nodes, "appTurnItems": items}}),
        })
        .await?;
    Ok(())
}

/// `isLiveRun`.
fn live_run(run: &Value) -> bool {
    matches!(
        run["status"].as_str(),
        Some("preparing" | "starting" | "running")
    )
}

fn work_active(status: &Value) -> bool {
    matches!(
        status.as_str(),
        Some("pending" | "starting" | "running" | "waiting")
    )
}

/// The recursive stop's future: `Send` where the host's futures are.
#[cfg(feature = "native")]
pub(crate) type StopFuture<'a> = futures_util::future::BoxFuture<'a, Result<(), Value>>;
#[cfg(not(feature = "native"))]
pub(crate) type StopFuture<'a> = futures_util::future::LocalBoxFuture<'a, Result<(), Value>>;

/// The child stops a thread's stop leaves until its stopped runs end, by
/// thread: the stop's command id and reason.
type DeferredChildStops = std::collections::HashMap<String, (String, Option<String>)>;
static DEFERRED_CHILD_STOPS: std::sync::LazyLock<std::sync::Mutex<DeferredChildStops>> =
    std::sync::LazyLock::new(Default::default);

/// Leaves the stop of `thread_id`'s delegated tasks until its stopped runs
/// end, as the reference's `delegated-tasks.stop` effect waits for the
/// interrupt effect before it.
pub(crate) fn defer_child_stop(thread_id: &str, command_id: &str, reason: Option<String>) {
    DEFERRED_CHILD_STOPS
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .insert(thread_id.to_owned(), (command_id.to_owned(), reason));
}

/// Whether the stop `command_id` left its child stops for later.
pub(crate) fn child_stop_deferred(thread_id: &str, command_id: &str) -> bool {
    DEFERRED_CHILD_STOPS
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .get(thread_id)
        .is_some_and(|(id, _)| id == command_id)
}

/// Once `run`, a run a stop interrupted, has ended its turn: its open task
/// cards end interrupted (`thread.background-work.settle`), then the stop's
/// delegated tasks stop and settle (`delegated-tasks.stop`).
pub(crate) async fn stop_children_after_interrupt<H: CommandHost>(
    state: Arc<H>,
    thread_id: String,
    run_id: String,
) -> Result<(), Value> {
    let (row, others_run) = {
        let store = state.store.read().await;
        let Some(thread) = store.threads.get(&thread_id) else {
            return Ok(());
        };
        let others = thread
            .runs
            .iter()
            .any(|run| run.status == "running" && run.wire_id() != run_id);
        (thread.stored_row(), others)
    };
    if others_run {
        return Ok(());
    }
    let Some((command_id, reason)) = DEFERRED_CHILD_STOPS
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .remove(&thread_id)
    else {
        return Ok(());
    };
    let at = crate::auth_service::iso(crate::auth_service::now());
    let mut items = records(&row, "appTurnItems");
    let mut settled = false;
    for item in items.iter_mut().filter(|item| {
        item["type"] == "subagent"
            && item["runId"] == run_id.as_str()
            && matches!(
                item["status"].as_str(),
                Some("pending" | "running" | "waiting")
            )
    }) {
        item["status"] = json!("interrupted");
        item["completedAt"] = json!(at);
        item["updatedAt"] = json!(at);
        settled = true;
    }
    if settled {
        state
            .commit(Event::ThreadPatched {
                thread_id: thread_id.clone(),
                patch: json!({"row": {"appTurnItems": items}}),
            })
            .await
            .map_err(|e| json!({"message": e.to_string()}))?;
    }
    // The children whose runs this stop ends settle before the parent's
    // turn goes on to its checkpoint.
    let ends: Vec<_> = {
        let store = state.store.read().await;
        records(&row, "subagents")
            .iter()
            .filter(|task| task["origin"] == "app_owned")
            .filter_map(|task| task["childThreadId"].as_str())
            .filter(|child| {
                store
                    .threads
                    .get(*child)
                    .is_some_and(|t| t.runs.iter().any(|run| run.status == "running"))
            })
            .collect::<std::collections::HashSet<_>>()
            .into_iter()
            .map(watch_run_end)
            .collect()
    };
    let stopped = stop_delegated_tasks(state, thread_id, command_id, reason).await;
    for end in ends {
        if let Ok(end) = end.await {
            drop(end.read);
            let _ = end.settled.await;
        }
    }
    stopped
}

/// `stopDelegatedTasks`: stops each app-owned child of `thread_id`, then its
/// children, each once.
pub(crate) fn stop_delegated_tasks<'a, H: CommandHost + 'a>(
    state: Arc<H>,
    thread_id: String,
    command_id: String,
    reason: Option<String>,
) -> StopFuture<'a> {
    Box::pin(async move {
        let tasks = {
            let store = state.store.read().await;
            store
                .threads
                .get(&thread_id)
                .map(|thread| records(&thread.stored_row(), "subagents"))
                .unwrap_or_default()
        };
        let mut stopped = std::collections::HashSet::new();
        let mut failure = None;
        for task in tasks {
            if task["origin"] != "app_owned" {
                continue;
            }
            // A task in a linked environment is not served here.
            let Some(child) = task["childThreadId"].as_str().map(str::to_owned) else {
                continue;
            };
            // Follow-up tasks reuse a child thread; walk its subtree only once.
            if !stopped.insert(child.clone()) {
                continue;
            }
            let mut stop = json!({
                "type": "thread.stop",
                "commandId": format!("{command_id}:stop:{child}"),
                "threadId": child,
                "stopDelegatedTasks": false,
            });
            if let Some(reason) = &reason {
                stop["reason"] = json!(reason);
            }
            let outcome = match dispatch_command(state.clone(), stop, None).await {
                Ok(_) => {
                    stop_delegated_tasks(
                        state.clone(),
                        child.clone(),
                        command_id.clone(),
                        reason.clone(),
                    )
                    .await
                }
                Err(error) => Err(error),
            };
            if let Err(error) = outcome {
                tracing::warn!(parent = %thread_id, child = %child, "could not stop a delegated task");
                failure.get_or_insert(error);
            }
        }
        failure.map_or(Ok(()), Err)
    })
}

/// `cancelDelegatedTask`: marks the run the task settles from as cancelled,
/// stops the child's descendants, then the child itself with `stop`.
pub(crate) async fn cancel_task<H: CommandHost>(
    state: Arc<H>,
    parent_id: &str,
    task_id: &str,
    stop: Value,
) -> Result<(), Value> {
    let child_id = stop["threadId"].as_str().unwrap_or_default().to_owned();
    {
        let _guard = state.mutation_lock().lock().await;
        let (parent_row, child) = {
            let store = state.store.read().await;
            let parent_row = store.threads.get(parent_id).map(|t| t.stored_row());
            let child = store.threads.get(&child_id).map(|t| t.full_projection());
            (parent_row, child)
        };
        if let (Some(parent_row), Some(child)) = (parent_row, child) {
            let mut tasks = records(&parent_row, "subagents");
            let task = tasks
                .iter()
                .find(|t| {
                    t["id"] == task_id
                        && t["origin"] == "app_owned"
                        && t["childThreadId"] == child_id.as_str()
                        && work_active(&t["status"])
                })
                .cloned();
            let (work, result_run) = progress(&child);
            let unsettled = child["thread"]["forkedFrom"]["type"] == "node"
                && child["thread"]["forkedFrom"]["nodeId"] == task_id
                && work != "result_available";
            let active = records(&child, "runs")
                .into_iter()
                .filter(live_run)
                .max_by_key(|run| run["ordinal"].as_u64().unwrap_or(0));
            let cancelled_run = active.or(result_run);
            if let (Some(task), true, Some(run)) = (task, unsettled, cancelled_run) {
                let at = crate::auth_service::iso(crate::auth_service::now());
                replace_by_id(&mut tasks, &task["id"], |t| {
                    t["cancelledRunId"] = run["id"].clone();
                    t["updatedAt"] = json!(at);
                });
                state
                    .commit(Event::ThreadPatched {
                        thread_id: parent_id.to_owned(),
                        patch: json!({"row": {"subagents": tasks}}),
                    })
                    .await
                    .map_err(|e| json!({"message": e.to_string()}))?;
            }
        }
    }
    // Hold the child until its descendants stop, then stop it.
    stop_delegated_tasks(
        state.clone(),
        child_id.clone(),
        stop["commandId"].as_str().unwrap_or_default().to_owned(),
        stop["reason"].as_str().map(str::to_owned),
    )
    .await?;
    dispatch_command(state.clone(), stop, None).await?;
    if let Err(error) = finalize_child(state.clone(), &child_id, parent_id).await {
        tracing::warn!(%error, child_id, "could not settle a cancelled task");
    }
    offer_delivery(state, parent_id).await;
    Ok(())
}

/// `subagent.stop` for an app-owned task (`dispatchSubagentStop`): one with
/// no bound child run is cancelled; one whose child still runs it (its
/// original task) is left to settle from that run. `None` when the task is
/// not an app-owned one on that run.
pub(crate) async fn stop_app_owned<H: CommandHost>(
    state: &Arc<H>,
    command: &Value,
    receipt: &crate::store::CommandReceipt,
) -> anyhow::Result<Option<u64>> {
    let thread_id = command["threadId"].as_str().unwrap_or_default();
    let (row, child) = {
        let store = state.store.read().await;
        let Some(thread) = store.threads.get(thread_id) else {
            return Ok(None);
        };
        let row = thread.stored_row();
        let child = records(&row, "subagents")
            .into_iter()
            .find(|t| t["id"] == command["subagentId"])
            .and_then(|t| t["childThreadId"].as_str().map(str::to_owned))
            .and_then(|id| store.threads.get(&id).map(|t| t.full_projection()));
        (row, child)
    };
    let mut tasks = records(&row, "subagents");
    let Some(task) = tasks
        .iter()
        .find(|t| t["id"] == command["subagentId"])
        .cloned()
        .filter(|t| {
            t["origin"] == "app_owned"
                && t["childThreadId"].is_string()
                && t["runId"] == command["runId"]
        })
    else {
        return Ok(None);
    };
    let active = work_active(&task["status"]);
    let bound = child.as_ref().is_some_and(|child| {
        (child["thread"]["forkedFrom"]["type"] == "node"
            && child["thread"]["forkedFrom"]["nodeId"] == task["id"])
            || records(child, "runs")
                .iter()
                .any(|run| run["delegatedTaskId"] == task["id"])
    });
    if active && bound {
        return Ok(Some(
            state
                .commit_receipted(
                    Event::CommandDispatched {
                        command_id: receipt.command_id.clone(),
                        command_type: "subagent.stop".into(),
                        thread_id: Some(thread_id.to_owned()),
                        payload: command.clone(),
                    },
                    Some(receipt.clone()),
                )
                .await?
                .id,
        ));
    }
    let at = crate::auth_service::iso(crate::auth_service::now());
    let mut written = task.clone();
    if active {
        // `cancelledAppOwnedTask`.
        written["status"] = json!("cancelled");
        written["completedAt"] = json!(at);
        written["updatedAt"] = json!(at);
        written["completionDelivery"] = json!({"state": "disposed", "observedByRunId": null});
    }
    replace_by_id(&mut tasks, &task["id"], |t| *t = written.clone());
    let mut nodes = records(&row, "nodes");
    replace_by_id(&mut nodes, &task["id"], |node| {
        node["status"] = written["status"].clone();
        node["completedAt"] = written["completedAt"].clone();
    });
    let mut items = records(&row, "appTurnItems");
    for item in items
        .iter_mut()
        .filter(|i| i["type"] == "subagent" && i["subagentId"] == task["id"])
    {
        item["status"] = written["status"].clone();
        item["completedAt"] = written["completedAt"].clone();
        item["updatedAt"] = json!(at);
    }
    Ok(Some(
        state
            .commit_receipted(
                Event::ThreadPatched {
                    thread_id: thread_id.to_owned(),
                    patch: json!({"row": {"subagents": tasks, "nodes": nodes, "appTurnItems": items}}),
                },
                Some(receipt.clone()),
            )
            .await?
            .id,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_child_in_its_own_worktree_waits_for_it_on_its_first_run() {
        let parent = json!({"id": "thread:p", "branch": "main", "worktreePath": "/w/p",
            "runExtras": {"run:thread:p:ordinal:1": {"x": 1}}});
        let strategy = json!({"type": "worktree", "baseRef": "abc", "startFromOrigin": false});
        let command = json!({"modelSelection": {"instanceId": "codex", "model": "m"},
            "workspaceStrategy": strategy});
        let row = child_thread_row(&parent, "thread:c", "node:c", &command, "Child", "t");
        assert_eq!(row["branch"], Value::Null);
        assert_eq!(row["worktreePath"], Value::Null);
        assert_eq!(
            row["runExtras"],
            json!({"run:thread:thread%3Ac:ordinal:1": {"workspacePreparation": strategy}})
        );
        let shared = json!({"modelSelection": {"instanceId": "codex", "model": "m"}});
        let row = child_thread_row(&parent, "thread:c", "node:c", &shared, "Child", "t");
        assert_eq!(row["worktreePath"], "/w/p");
        assert!(row.get("runExtras").is_none());
    }

    #[test]
    fn ids_derive_from_the_command_as_the_reference_joins_them() {
        let command = "command:mcp:ns:delegate-task:key";
        assert_eq!(
            task_node_id(command),
            "node:delegated-task:command%3Amcp%3Ans%3Adelegate-task%3Akey"
        );
        assert_eq!(
            task_thread_id(command),
            "thread:delegated-task:command%3Amcp%3Ans%3Adelegate-task%3Akey"
        );
        assert_eq!(
            root_node_id("run:thread:t:ordinal:1"),
            "node:run:run%3Athread%3At%3Aordinal%3A1:root"
        );
    }

    #[test]
    fn a_child_title_is_the_title_or_prompt_cut_to_72_characters() {
        assert_eq!(
            subagent_thread_title("P", Some(" Greeter "), "x", 1),
            "Greeter"
        );
        assert_eq!(
            subagent_thread_title("P", Some("  "), " Say hi ", 1),
            "Say hi"
        );
        assert_eq!(subagent_thread_title("P", None, "   ", 3), "P subagent 3");
        let long = "a".repeat(80);
        assert_eq!(
            subagent_thread_title("P", None, &long, 1),
            format!("{}...", "a".repeat(69))
        );
    }

    #[test]
    fn a_stopped_run_s_result_ends_with_why_it_stopped() {
        let run = json!({"id": "run:c", "status": "interrupted"});
        let child = json!({"turnItems": [
            {"id": "i1", "runId": "run:c", "type": "run_interrupt_result", "status": "interrupted",
             "ordinal": 3, "message": "Stopped by an agent: No longer needed"},
        ], "messages": []});
        assert_eq!(
            subagent_result(&child, &run),
            "Child task ended with status interrupted.\n\nStopped by an agent: No longer needed"
        );
        let report = json!({"turnItems": [
            {"id": "i0", "runId": "run:c", "type": "assistant_message", "status": "completed",
             "ordinal": 2, "text": "Counted to five.", "messageId": null},
            {"id": "i1", "runId": "run:c", "type": "run_interrupt_result", "status": "interrupted",
             "ordinal": 3, "message": "Stopped by user"},
        ], "messages": []});
        assert_eq!(
            subagent_result(&report, &run),
            "Counted to five.\n\nStopped by user"
        );
    }

    #[test]
    fn a_wake_inlines_results_and_points_at_task_status_otherwise() {
        let tasks = vec![
            json!({"id": "t1", "title": "Greeter", "status": "completed", "result": "Hi."}),
            json!({"id": "t3", "status": "failed", "childThreadId": "thread:child"}),
        ];
        assert_eq!(
            wake_detail(&[json!("t1")], &tasks),
            "Delegated task t1 (Greeter) [completed]:\nHi."
        );
        assert_eq!(
            wake_detail(&[json!("t3")], &tasks),
            "Delegated task t3 in child thread thread:child [failed]. Use task_status with taskId t3 to read the result."
        );
        assert_eq!(
            wake_detail(&[json!("t2")], &tasks),
            "Delegated task t2 reached a terminal state. Use task_status with taskId t2 to read the result."
        );
        assert_eq!(
            completion_text(&[json!("a"), json!("b")]),
            "Delegated tasks a, b reached terminal states. Use task_status with each taskId to read the results."
        );
    }

    #[test]
    fn a_source_point_names_the_run_and_its_native_refs() {
        let thread_ref = json!({"driver": "claudeAgent", "nativeId": "s1", "strength": "strong"});
        let turn_ref = json!({"driver": "claudeAgent", "nativeId": "u1", "strength": "strong"});
        let projection = json!({
            "thread": {"id": "th"},
            "runs": [{"id": "r1", "checkpointId": "c1", "providerThreadId": "pt", "activeAttemptId": "a1"}],
            "providerThreads": [{"id": "pt", "nativeThreadRef": thread_ref}],
            "providerTurns": [
                {"id": "x", "runAttemptId": "a1", "ordinal": 1, "nativeTurnRef": null},
                {"id": "y", "runAttemptId": "a1", "ordinal": 2, "nativeTurnRef": turn_ref},
            ],
            "attempts": [],
        });
        assert_eq!(
            context_source_point(&projection, "r1"),
            json!({"threadId": "th", "runId": "r1", "checkpointId": "c1",
                   "providerThreadRef": thread_ref, "providerTurnRef": turn_ref})
        );
        assert_eq!(
            context_source_point(&projection, "missing"),
            json!({"threadId": "th", "runId": "missing"})
        );
    }

    #[test]
    fn a_followup_settles_from_its_executing_run_else_its_latest() {
        let run = |id: &str, ordinal: u64, status: &str| json!({"id": id, "ordinal": ordinal, "status": status, "startedAt": "t"});
        let runs = vec![run("a", 1, "completed"), run("b", 2, "running")];
        assert_eq!(delegated_task_result_run(&runs).unwrap()["id"], "b");
        let runs = vec![run("a", 1, "completed"), run("b", 2, "interrupted")];
        assert_eq!(delegated_task_result_run(&runs).unwrap()["id"], "b");
        let held = json!({"id": "c", "ordinal": 3, "status": "queued", "queueHeld": true, "startedAt": null});
        let runs = vec![run("a", 1, "completed"), held];
        assert_eq!(delegated_task_result_run(&runs).unwrap()["id"], "a");
    }

    #[test]
    fn a_followup_binds_to_the_active_run_else_the_last_that_ran() {
        let runs = vec![
            json!({"id": "a", "ordinal": 1, "status": "completed", "startedAt": "t"}),
            json!({"id": "b", "ordinal": 2, "status": "queued", "startedAt": null}),
        ];
        assert_eq!(follow_up_owner_run(&runs).unwrap()["id"], "a");
        let runs = vec![
            json!({"id": "a", "ordinal": 1, "status": "completed", "startedAt": "t"}),
            json!({"id": "b", "ordinal": 2, "status": "waiting", "startedAt": "t"}),
        ];
        assert_eq!(follow_up_owner_run(&runs).unwrap()["id"], "b");
    }

    #[test]
    fn only_user_mcp_and_real_wakes_start_followups() {
        assert!(may_start_followup(&json!({"createdBy": "user"})));
        assert!(may_start_followup(
            &json!({"createdBy": "agent", "creationSource": "mcp"})
        ));
        assert!(!may_start_followup(
            &json!({"createdBy": "agent", "creationSource": "server"})
        ));
        assert!(!may_start_followup(
            &json!({"createdBy": "user", "scheduledTaskId": "s"})
        ));
        let monitor =
            json!({"notification": {"source": {"kind": "monitor"}, "outcome": "updated"}});
        assert!(!may_start_followup(&monitor));
        let done = json!({"notification": {"source": {"kind": "monitor"}, "outcome": "completed"}});
        assert!(may_start_followup(&done));
    }

    #[tokio::test]
    async fn a_waiter_reads_before_the_task_settles_and_answers_after() {
        let rx = watch_run_end("thread:test-waiter");
        let order = Arc::new(std::sync::Mutex::new(Vec::new()));
        let waiter = {
            let order = order.clone();
            tokio::spawn(async move {
                let RunEnd { read, settled } = rx.await.expect("run end");
                order.lock().unwrap().push("read");
                drop(read);
                let _ = settled.await;
                order.lock().unwrap().push("answered");
            })
        };
        let releases = let_waiters_read("thread:test-waiter").await;
        order.lock().unwrap().push("settled");
        drop(releases);
        waiter.await.unwrap();
        assert_eq!(*order.lock().unwrap(), ["read", "settled", "answered"]);
        // With no waiter, a run end lets nothing read.
        assert!(let_waiters_read("thread:test-waiter").await.is_empty());
    }
}
