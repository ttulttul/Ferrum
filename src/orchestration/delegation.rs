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
    if command.get("workspaceStrategy").is_some() {
        row["branch"] = Value::Null;
        row["worktreePath"] = Value::Null;
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
        cwd: parent.cwd.clone(),
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
        provider: Default::default(),
        stored: None,
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
    let seq = start_child_run(state.clone(), receipt, &child_id, prompt, &message_id, wire).await?;
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
/// `start_immediately` does, with the message's wire fields.
async fn start_child_run<H: CommandHost>(
    state: Arc<H>,
    receipt: &CommandReceipt,
    child_id: &str,
    prompt: &str,
    message_id: &Value,
    wire: Value,
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
    } else if let Some(text) = message.and_then(|m| m["text"].as_str()) {
        text.to_owned()
    } else if let Some(text) = item.and_then(|i| i["text"].as_str()) {
        text.to_owned()
    } else if status == "completed" {
        "Child task completed without an assistant result.".to_owned()
    } else {
        format!("Child task ended with status {status}.")
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
            let state = match task {
                None => " reached a terminal state".to_owned(),
                Some(t) => format!(" [{}]", t["status"].as_str().unwrap_or_default()),
            };
            let label = format!("Delegated task {id}{title}{state}");
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

/// Records `at` as the `workStartedAt` of the run `message_id` started.
pub(crate) async fn record_work_started_at<H: CommandHost>(
    state: &H,
    thread_id: &str,
    message_id: &Value,
    at: Value,
) -> anyhow::Result<()> {
    let (run_id, mut extras) = {
        let store = state.store.read().await;
        let Some(thread) = store.threads.get(thread_id) else {
            return Ok(());
        };
        let Some(run) = thread
            .runs
            .iter()
            .find(|run| run.user_message_id.as_deref() == message_id.as_str())
        else {
            return Ok(());
        };
        (run.wire_id(), thread.stored_row()["runExtras"].clone())
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
            deliver_if_idle(state.clone(), parent).await;
        }
    }
    deliver_if_idle(state, thread_id).await;
}

/// `finalizeAppOwnedSubagent` for a child whose work is done.
async fn finalize_child<H: CommandHost>(
    state: Arc<H>,
    child_id: &str,
    parent_id: &str,
) -> anyhow::Result<()> {
    let _guard = state.mutation_lock().lock().await;
    let (child, parent_row, parent_projection) = {
        let store = state.store.read().await;
        let Some(child) = store.threads.get(child_id) else {
            return Ok(());
        };
        let Some(parent) = store.threads.get(parent_id) else {
            return Ok(());
        };
        (
            child.full_projection(),
            parent.stored_row(),
            parent.full_projection(),
        )
    };
    let (work, result_run) = progress(&child);
    let Some(result_run) = result_run.filter(|_| work == "result_available") else {
        return Ok(());
    };
    let Some(terminal) = terminal_status(result_run["status"].as_str().unwrap_or_default()) else {
        return Ok(());
    };
    let task_node = child["thread"]["forkedFrom"]["nodeId"].clone();
    let mut tasks = records(&parent_row, "subagents");
    let Some(task) = tasks
        .iter()
        .find(|t| {
            t["id"] == task_node
                && t["origin"] == "app_owned"
                && t["childThreadId"] == child_id
                && t["result"].is_null()
        })
        .cloned()
    else {
        return Ok(());
    };
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
            let next = if delivery.is_object() {
                let mut ids = records(delivery, "taskIds");
                if !ids.contains(&task["id"]) {
                    ids.push(task["id"].clone());
                }
                let mut next = cohort.clone();
                next["delivery"]["taskIds"] = Value::Array(ids);
                next
            } else {
                let generation = cohort["nextGeneration"].as_u64().unwrap_or(1);
                let ordinal = records(&parent_projection, "messages").len() + 1;
                json!({
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
                })
            };
            updated["completionDelivery"] = json!({"state": "claimed", "observedByRunId": null});
            extras[run["id"].as_str().unwrap_or_default()]["delegatedCompletion"] = next;
        }
    }
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
    let mut source_point =
        context_source_point(&child, result_run["id"].as_str().unwrap_or_default());
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
    Ok(())
}

/// Delivers a thread's open delivery once it has no live run, as the
/// reference's continuation service does: a queued message that wakes it with
/// the finished tasks (`delegatedCompletion`). Accepting it marks the claimed
/// tasks delivered and frees the cohort (`acceptDelegatedCompletionDelivery`).
async fn deliver_if_idle<H: CommandHost>(state: Arc<H>, thread_id: &str) {
    let pending = {
        let store = state.store.read().await;
        let Some(thread) = store.threads.get(thread_id) else {
            return;
        };
        if thread
            .runs
            .iter()
            .any(|run| matches!(run.status.as_str(), "running" | "queued" | "preparing"))
        {
            return;
        }
        let row = thread.stored_row();
        let found = row["runExtras"].as_object().and_then(|extras| {
            extras.iter().find_map(|(run_id, extra)| {
                let cohort = &extra["delegatedCompletion"];
                (cohort["disposition"] == "open" && cohort["delivery"].is_object())
                    .then(|| (run_id.clone(), cohort.clone()))
            })
        });
        found.map(|(run_id, cohort)| (run_id, cohort, row))
    };
    let Some((run_id, cohort, row)) = pending else {
        return;
    };
    let delivery = &cohort["delivery"];
    let task_ids = records(delivery, "taskIds");
    let tasks = records(&row, "subagents");
    // Accepted: claimed tasks are delivered, and the cohort holds no delivery.
    let mut accepted = tasks.clone();
    for task in accepted.iter_mut() {
        if task_ids.contains(&task["id"]) && task["completionDelivery"]["state"] == "claimed" {
            task["completionDelivery"] = json!({"state": "delivered", "observedByRunId": null});
            task["updatedAt"] = json!(crate::auth_service::iso(crate::auth_service::now()));
        }
    }
    let mut extras = row["runExtras"].clone();
    extras[&run_id]["delegatedCompletion"]["delivery"] = Value::Null;
    if state
        .commit(Event::ThreadPatched {
            thread_id: thread_id.to_owned(),
            patch: json!({"row": {"subagents": accepted, "runExtras": extras}}),
        })
        .await
        .is_err()
    {
        return;
    }
    let command = json!({
        "type": "message.dispatch",
        "commandId": format!("command:server:delegated-completion:dispatch:{}", crate::platform::uuid_v4()),
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

#[cfg(test)]
mod tests {
    use super::*;

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
    fn a_wake_inlines_results_and_points_at_task_status_otherwise() {
        let tasks =
            vec![json!({"id": "t1", "title": "Greeter", "status": "completed", "result": "Hi."})];
        assert_eq!(
            wake_detail(&[json!("t1")], &tasks),
            "Delegated task t1 (Greeter) [completed]:\nHi."
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
