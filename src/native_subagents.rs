//! The child threads of provider-native subagents (a Codex agent's `spawnAgent` or
//! `subAgentActivity`), as the reference's adapter creates them (`registerSubagentThread`,
//! `makeSubagentChildThread`): when a parent's turn first reports a native subagent, its child
//! app thread is created, a subagent thread of the parent titled after the subagent's path,
//! prompt or ordinal, with the root node its turns hang from and, when Codex named one, the
//! prompt the parent gave it. The child's model, once Codex reports it, becomes the child
//! thread's (`updateSubagentSelection`).
use crate::control_state::ControlState;
use crate::provider_projection::item_id;
use crate::store::{Event, Thread};
use crate::workspace::provider::ProviderItem;
use serde_json::{json, Value};

/// A native subagent's item, by its fields: the child app thread, its native thread, model
/// and effort.
struct Subagent<'a> {
    child_id: &'a str,
    native_thread: &'a str,
    model: Option<&'a str>,
    effort: Option<&'a str>,
}

fn subagent(item: &ProviderItem) -> Option<Subagent<'_>> {
    if item.kind != "subagent" || item.fields.get("origin") != Some(&json!("provider_native")) {
        return None;
    }
    if item.fields.contains_key("claudeTaskId") {
        return None;
    }
    let text = |key: &str| {
        item.fields
            .get(key)
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
    };
    Some(Subagent {
        child_id: text("childThreadId")?,
        native_thread: text("nativeThreadId")?,
        model: text("model"),
        effort: text("reasoningEffort"),
    })
}

/// The child's model selection: the one Codex reported, else the parent's with the subagent's
/// model and no effort (a child's effort can differ from its parent's).
fn selection(instance: &str, parent: &Value, model: Option<&str>, effort: Option<&str>) -> Value {
    match model {
        Some(model) => {
            let mut selection = json!({"instanceId": instance, "model": model});
            if let Some(effort) = effort {
                selection["options"] = json!([{"id": "reasoningEffort", "value": effort}]);
            }
            selection
        }
        None => {
            let mut selection = parent.clone();
            if let Some(options) = selection.get_mut("options").and_then(Value::as_array_mut) {
                options.retain(|option| option["id"] != "reasoningEffort");
            }
            selection
        }
    }
}

/// Before `parent_thread_id`'s turn records `item`: the child thread a native subagent runs on,
/// created when it is new, or its model once Codex reported it.
pub(crate) async fn record_child(
    state: &ControlState,
    parent_thread_id: &str,
    driver: &str,
    item: &ProviderItem,
) {
    // What a subagent's own turn reports belongs to its child thread.
    if let Some(native) = item.fields.get("subagentThread").and_then(Value::as_str) {
        return record_child_turn(state, parent_thread_id, driver, native, item).await;
    }
    if item.fields.contains_key("claudeTaskId") || item.fields.contains_key("subagentChild") {
        return record_claude(state, parent_thread_id, driver, item).await;
    }
    let Some(subagent) = subagent(item) else {
        return;
    };
    let at = crate::auth_service::iso(crate::auth_service::now());
    let event = {
        let store = state.store.read().await;
        let Some(parent) = store.threads.get(parent_thread_id) else {
            return;
        };
        let parent_row = parent.stored_row();
        let instance = parent_row["providerInstanceId"]
            .as_str()
            .or(parent_row["modelSelection"]["instanceId"].as_str())
            .unwrap_or(driver)
            .to_owned();
        if let Some(child) = store.threads.get(subagent.child_id) {
            // Codex reported the child's model after the child was made.
            let Some(model) = subagent.model else {
                return;
            };
            let current = &child.stored_row()["modelSelection"];
            let selected = selection(&instance, current, Some(model), subagent.effort);
            if *current == selected {
                return;
            }
            Event::ThreadPatched {
                thread_id: subagent.child_id.to_owned(),
                patch: json!({"row": {"modelSelection": selected, "updatedAt": at}}),
            }
        } else {
            let ordinal = store
                .threads
                .values()
                .filter(|t| t.stored_row()["lineage"]["parentThreadId"] == parent_thread_id)
                .count()
                + 1;
            let prompt = item
                .fields
                .get("prompt")
                .and_then(Value::as_str)
                .unwrap_or_default();
            let title = crate::orchestration::delegation::subagent_thread_title(
                &parent.title,
                item.title.as_deref(),
                prompt,
                ordinal,
            );
            let child = child_thread(
                parent, &subagent, item, driver, &instance, &title, prompt, &at,
            );
            Event::ThreadCreated(child)
        }
    };
    if let Err(error) = state.commit(event).await {
        tracing::warn!(%error, parent_thread_id, "failed to record a native subagent's thread");
    }
}

/// `IdAllocator.derive.subagentThread`.
fn child_thread_id(native_thread: &str) -> String {
    driver_child_thread_id("codex", native_thread)
}

/// `IdAllocator.derive.subagentThread` for `driver`'s native thread.
pub(crate) fn driver_child_thread_id(driver: &str, native_thread: &str) -> String {
    use sha2::Digest;
    let legacy = format!(
        "thread:provider:{}:native-thread:{}",
        crate::thread_projection::uri_component(driver),
        crate::thread_projection::uri_component(native_thread)
    );
    let digest = hex::encode(sha2::Sha256::digest(legacy.as_bytes()));
    format!("thread:subagent:{}", &digest[..16])
}

/// Replaces the record with `record`'s id in `records`, or adds it.
fn upsert(records: &mut Vec<Value>, record: Value) {
    match records.iter_mut().find(|r| r["id"] == record["id"]) {
        Some(existing) => *existing = record,
        None => records.push(record),
    }
}

/// A subagent's own turn, as the reference's adapter records it on the child thread
/// (`emitSubagentProviderTurnStarted`, its items, `finalizeTurn` for a subagent): the turn's
/// provider turn and root node, the child's provider thread active while it runs, and each
/// item with its node and, for an answer, its message.
async fn record_child_turn(
    state: &ControlState,
    parent_thread_id: &str,
    driver: &str,
    native: &str,
    item: &ProviderItem,
) {
    let at = crate::auth_service::iso(crate::auth_service::now());
    let child_id = child_thread_id(native);
    let turn = item
        .fields
        .get("nativeTurnId")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned();
    let event = {
        let store = state.store.read().await;
        let (Some(parent), Some(child)) = (
            store.threads.get(parent_thread_id),
            store.threads.get(&child_id),
        ) else {
            return;
        };
        let row = child.stored_row();
        let records = |key: &str| row[key].as_array().cloned().unwrap_or_default();
        let (mut nodes, mut turns, mut threads) = (
            records("nodes"),
            records("providerTurns"),
            records("providerThreads"),
        );
        let (mut items, mut messages) = (records("nativeTurnItems"), records("nativeMessages"));
        let provider_thread = row["activeProviderThreadId"]
            .as_str()
            .unwrap_or_default()
            .to_owned();
        let turn_id = format!(
            "provider-turn:provider:{}:native-turn:{}",
            crate::thread_projection::uri_component(driver),
            crate::thread_projection::uri_component(&turn)
        );
        let strong =
            |native: &str| json!({"driver": driver, "nativeId": native, "strength": "strong"});
        // The child's first turn hangs from the root node it was made with; a later one gets its own.
        let ordinal = turns
            .iter()
            .position(|t| t["id"] == turn_id.as_str())
            .unwrap_or(turns.len())
            + 1;
        let first_root = nodes
            .iter()
            .find(|n| n["kind"] == "root_turn" && n["runId"].is_null())
            .cloned();
        let root_id = if ordinal == 1 {
            first_root
                .as_ref()
                .map(|n| n["id"].clone())
                .unwrap_or(Value::Null)
        } else {
            json!(item_id("node", driver, &format!("{turn}:thread-root")))
        };
        if item.kind == "subagent_turn" {
            let running = item.status == "running";
            let previous = turns.iter().find(|t| t["id"] == turn_id.as_str()).cloned();
            let started = previous
                .as_ref()
                .map_or(json!(at), |t| t["startedAt"].clone());
            let mut provider_turn = json!({
                "id": turn_id,
                "providerThreadId": provider_thread,
                "nodeId": root_id,
                "runAttemptId": null,
                "nativeTurnRef": strong(&turn),
                "ordinal": ordinal,
                "status": item.status,
                "startedAt": started,
                "completedAt": if running { Value::Null } else { json!(at) },
            });
            let model = item.fields.get("model").cloned().filter(|m| m.is_string());
            if !running {
                let mut usage = json!({
                    "usageStatus": "unavailable",
                    "usageScope": "main_agent",
                    "hasSubagents": false,
                });
                if let Some(model) = &model {
                    usage["models"] = json!([model]);
                }
                provider_turn["turnTokenUsage"] = usage;
                if let Some(model) = &model {
                    provider_turn["model"] = model.clone();
                }
            }
            upsert(&mut turns, provider_turn);
            let mut root = first_root.filter(|_| ordinal == 1).unwrap_or_else(|| {
                json!({
                    "id": root_id, "threadId": child_id, "runId": null, "parentNodeId": null,
                    "rootNodeId": root_id, "kind": "root_turn", "countsForRun": false,
                    "providerThreadId": provider_thread, "runtimeRequestId": null,
                    "checkpointScopeId": null, "startedAt": at,
                })
            });
            root["status"] = json!(item.status);
            root["providerTurnId"] = json!(turn_id);
            root["completedAt"] = if running { Value::Null } else { json!(at) };
            upsert(&mut nodes, root);
            // The child's provider thread, as its parent records it, active while it runs.
            if let Some(mut record) = parent
                .provider_records()
                .provider_threads
                .into_iter()
                .find(|t| t["id"] == provider_thread.as_str())
            {
                record["status"] = json!(if running { "active" } else { "idle" });
                record["updatedAt"] = json!(at);
                upsert(&mut threads, record);
            }
        } else {
            let native_item = item.id.as_str();
            let running = item.status == "running";
            // An answer shows once it has text.
            let empty = item
                .fields
                .get("text")
                .and_then(Value::as_str)
                .is_none_or(str::is_empty);
            if item.kind == "assistant_message" && running && empty {
                return;
            }
            let row_id = item_id("turn-item", driver, native_item);
            let node_id = item_id("node", driver, native_item);
            let previous = items.iter().find(|i| i["id"] == row_id.as_str()).cloned();
            let started = previous
                .as_ref()
                .map_or(json!(at), |i| i["startedAt"].clone());
            let ordinal = previous
                .as_ref()
                .and_then(|i| i["ordinal"].as_u64())
                .unwrap_or(items.len() as u64 + 1);
            let completed = if running { Value::Null } else { json!(at) };
            let mut record = json!({
                "id": row_id,
                "threadId": child_id,
                "runId": null,
                "nodeId": node_id,
                "providerThreadId": provider_thread,
                "providerTurnId": turn_id,
                "nativeItemRef": strong(native_item),
                "parentItemId": null,
                "ordinal": ordinal,
                "status": item.status,
                "title": item.title,
                "startedAt": started,
                "completedAt": completed,
                "updatedAt": at,
                "type": item.kind,
            });
            for (key, value) in &item.fields {
                if !matches!(key.as_str(), "subagentThread" | "nativeTurnId") {
                    record[key] = value.clone();
                }
            }
            if item.kind == "assistant_message" {
                let message_id = item_id("message", driver, native_item);
                record["messageId"] = json!(message_id);
                upsert(
                    &mut messages,
                    json!({
                        "createdBy": "agent", "creationSource": "provider", "id": message_id,
                        "threadId": child_id, "runId": null, "nodeId": node_id, "role": "assistant",
                        "text": record["text"], "attachments": [], "streaming": running,
                        "createdAt": started, "updatedAt": at,
                    }),
                );
            }
            upsert(&mut items, record);
            upsert(
                &mut nodes,
                json!({
                    "id": node_id,
                    "threadId": child_id,
                    "runId": null,
                    "parentNodeId": root_id,
                    "rootNodeId": root_id,
                    "kind": match item.kind.as_str() {
                        "assistant_message" => "assistant_message",
                        "reasoning" => "reasoning",
                        _ => "tool_call",
                    },
                    "status": item.status,
                    "countsForRun": false,
                    "providerThreadId": provider_thread,
                    "providerTurnId": turn_id,
                    "nativeItemRef": strong(native_item),
                    "runtimeRequestId": null,
                    "checkpointScopeId": null,
                    "startedAt": started,
                    "completedAt": completed,
                }),
            );
        }
        Event::ThreadPatched {
            thread_id: child_id.clone(),
            patch: json!({"row": {
                "nodes": nodes, "providerTurns": turns, "providerThreads": threads,
                "nativeTurnItems": items, "nativeMessages": messages,
            }}),
        }
    };
    if let Err(error) = state.commit(event).await {
        tracing::warn!(%error, parent_thread_id, "failed to record a native subagent's turn");
    }
}

/// A Claude subagent's child thread (`claudeSubagentIds`): named by the parent's provider
/// thread and the task, made when the task starts; its records outside any run (the child's
/// messages and the task's result message) hang from its root node, which ends as the task.
async fn record_claude(
    state: &ControlState,
    parent_thread_id: &str,
    driver: &str,
    item: &ProviderItem,
) {
    let task = item
        .fields
        .get("claudeTaskId")
        .or_else(|| item.fields.get("subagentChild"))
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned();
    let at = crate::auth_service::iso(crate::auth_service::now());
    let event = {
        let store = state.store.read().await;
        let Some(parent) = store.threads.get(parent_thread_id) else {
            return;
        };
        let Some(provider_thread) = parent.provider_thread_id() else {
            return;
        };
        let child_id = driver_child_thread_id(driver, &format!("{provider_thread}:{task}"));
        let parent_row = parent.stored_row();
        let instance = parent_row["providerInstanceId"]
            .as_str()
            .or(parent_row["modelSelection"]["instanceId"].as_str())
            .unwrap_or(driver)
            .to_owned();
        match store.threads.get(&child_id) {
            None if item.kind == "subagent" => {
                let ordinal = store
                    .threads
                    .values()
                    .filter(|t| t.stored_row()["lineage"]["parentThreadId"] == parent_thread_id)
                    .count()
                    + 1;
                let prompt = item
                    .fields
                    .get("prompt")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                let title = crate::orchestration::delegation::subagent_thread_title(
                    &parent.title,
                    item.title.as_deref(),
                    prompt,
                    ordinal,
                );
                let subagent = Subagent {
                    child_id: &child_id,
                    native_thread: "",
                    model: item.fields.get("model").and_then(Value::as_str),
                    effort: None,
                };
                Event::ThreadCreated(child_thread(
                    parent, &subagent, item, driver, &instance, &title, prompt, &at,
                ))
            }
            None => return,
            Some(child) => {
                let row = child.stored_row();
                let mut nodes = row["nodes"].as_array().cloned().unwrap_or_default();
                let root = nodes
                    .iter()
                    .find(|n| n["kind"] == "root_turn")
                    .map(|n| n["id"].clone())
                    .unwrap_or(Value::Null);
                let mut patch = json!({});
                if item.kind == "subagent" {
                    // The root node ends as the task does.
                    if item.status == "running" {
                        return;
                    }
                    for node in nodes.iter_mut().filter(|n| n["kind"] == "root_turn") {
                        node["status"] = json!(item.status);
                        node["completedAt"] = json!(at);
                    }
                    patch["nodes"] = json!(nodes);
                } else {
                    let mut items = row["nativeTurnItems"]
                        .as_array()
                        .cloned()
                        .unwrap_or_default();
                    let mut messages = row["nativeMessages"]
                        .as_array()
                        .cloned()
                        .unwrap_or_default();
                    let native = item.id.as_str();
                    let message_id = item_id("message", driver, native);
                    let mut record = json!({
                        "id": item_id("turn-item", driver, native),
                        "threadId": child_id,
                        "runId": null,
                        "nodeId": root,
                        "providerThreadId": null,
                        "providerTurnId": null,
                        "nativeItemRef": {"driver": driver, "nativeId": native, "strength": "strong"},
                        "parentItemId": null,
                        "ordinal": items.len() + 1,
                        "status": item.status,
                        "title": item.title,
                        "startedAt": at,
                        "completedAt": at,
                        "updatedAt": at,
                        "type": item.kind,
                        "messageId": message_id,
                    });
                    for (key, value) in &item.fields {
                        if key != "subagentChild" {
                            record[key] = value.clone();
                        }
                    }
                    messages.push(json!({
                        "createdBy": "agent", "creationSource": "provider", "id": message_id,
                        "threadId": child_id, "runId": null, "nodeId": root, "role": "assistant",
                        "text": record["text"], "attachments": [], "streaming": false,
                        "createdAt": at, "updatedAt": at,
                    }));
                    items.push(record);
                    patch["nativeTurnItems"] = json!(items);
                    patch["nativeMessages"] = json!(messages);
                }
                Event::ThreadPatched {
                    thread_id: child_id.clone(),
                    patch: json!({"row": patch}),
                }
            }
        }
    };
    if let Err(error) = state.commit(event).await {
        tracing::warn!(%error, parent_thread_id, "failed to record a Claude subagent's thread");
    }
}

#[allow(clippy::too_many_arguments)]
fn child_thread(
    parent: &Thread,
    subagent: &Subagent<'_>,
    item: &ProviderItem,
    driver: &str,
    instance: &str,
    title: &str,
    prompt: &str,
    at: &str,
) -> Thread {
    let parent_row = parent.stored_row();
    // A Claude task's records are named by the task (`task:<id>`), its refs by its id; a
    // Codex subagent's by its item. A Claude child has no provider thread of its own.
    let native_item = item
        .fields
        .get("nodeNativeId")
        .and_then(Value::as_str)
        .unwrap_or(item.id.as_str());
    let root_ref = item
        .fields
        .get("nativeItemRef")
        .and_then(|reference| reference["nativeId"].as_str())
        .unwrap_or(native_item)
        .to_owned();
    let claude = item.fields.contains_key("claudeTaskId");
    let subagent_node = item_id("node", driver, native_item);
    let root_node = item_id("node", driver, &format!("{native_item}:thread-root"));
    let provider_thread = if claude {
        Value::Null
    } else {
        json!(format!(
            "provider-thread:provider:{}:native-thread:{}",
            crate::thread_projection::uri_component(driver),
            crate::thread_projection::uri_component(subagent.native_thread)
        ))
    };
    let strong = |native: &str| json!({"driver": driver, "nativeId": native, "strength": "strong"});
    // The parent's app thread, as the child's (`makeSubagentChildThread`): the remote parent
    // delegated the parent thread, not its subagents.
    let mut row = parent_row.clone();
    if let Some(fields) = row.as_object_mut() {
        for key in [
            "delegatedFrom",
            "historyOrigin",
            "issues",
            "linkedPullRequest",
            "branchPullRequest",
            "nodes",
            "appTurnItems",
            "runExtras",
            "subagents",
        ] {
            fields.remove(key);
        }
    }
    for (key, value) in [
        ("id", json!(subagent.child_id)),
        ("title", json!(title)),
        ("createdBy", json!("agent")),
        ("creationSource", json!("provider")),
        ("titleRegeneration", Value::Null),
        ("titleRenamed", json!(false)),
        ("linkedPullRequest", Value::Null),
        ("pullRequests", json!([])),
        ("providerInstanceId", json!(instance)),
        (
            "modelSelection",
            selection(
                instance,
                &parent_row["modelSelection"],
                subagent.model,
                subagent.effort,
            ),
        ),
        ("activeProviderThreadId", provider_thread.clone()),
        (
            "lineage",
            json!({
                "parentThreadId": parent.id,
                "relationshipToParent": "subagent",
                "rootThreadId": parent_row["lineage"]["rootThreadId"].as_str().unwrap_or(&parent.id),
            }),
        ),
        (
            "forkedFrom",
            json!({"type": "node", "nodeId": subagent_node}),
        ),
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
    ] {
        row[key] = value;
    }
    // The root node the child's turns hang from.
    row["nodes"] = json!([{
        "id": root_node,
        "threadId": subagent.child_id,
        "runId": null,
        "parentNodeId": null,
        "rootNodeId": root_node,
        "kind": "root_turn",
        "status": "running",
        "countsForRun": false,
        "providerThreadId": provider_thread,
        "providerTurnId": null,
        "nativeItemRef": strong(&root_ref),
        "runtimeRequestId": null,
        "checkpointScopeId": null,
        "startedAt": at,
        "completedAt": null
    }]);
    // The prompt the parent gave it (`makeSubagentConversationArtifacts`).
    if !prompt.is_empty() {
        let native_prompt = format!("{native_item}:prompt");
        let message_id = item_id("message", driver, &native_prompt);
        row["nativeMessages"] = json!([{
            "createdBy": "agent",
            "creationSource": "provider",
            "id": message_id,
            "threadId": subagent.child_id,
            "runId": null,
            "nodeId": root_node,
            "role": "user",
            "text": prompt,
            "attachments": [],
            "senderThreadId": parent.id,
            "streaming": false,
            "createdAt": at,
            "updatedAt": at
        }]);
        // A Claude brief is no workflow script (`workflowScript`).
        if claude {
            row["nativeMessages"][0]["workflowScript"] = json!(false);
        }
        row["nativeTurnItems"] = json!([{
            "id": item_id("turn-item", driver, &native_prompt),
            "threadId": subagent.child_id,
            "runId": null,
            "nodeId": root_node,
            "providerThreadId": provider_thread,
            "providerTurnId": null,
            "nativeItemRef": strong(&native_prompt),
            "parentItemId": null,
            "ordinal": 1,
            "status": "completed",
            "title": null,
            "startedAt": at,
            "completedAt": at,
            "updatedAt": at,
            "type": "user_message",
            "messageId": message_id,
            "text": prompt,
            "attachments": [],
            "createdBy": "agent",
            "creationSource": "provider",
            "senderThreadId": parent.id,
            "inputIntent": "turn_start"
        }]);
        if claude {
            row["nativeTurnItems"][0]["workflowScript"] = json!(false);
        }
    }
    Thread {
        row,
        id: subagent.child_id.to_owned(),
        project_id: parent.project_id.clone(),
        title: title.to_owned(),
        cwd: parent.cwd.clone(),
        status: "idle".into(),
        messages: vec![],
        runs: vec![],
        created_at: crate::now(),
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
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_child_runs_the_model_codex_reported_else_its_parents_without_effort() {
        let parent = json!({"instanceId": "codex", "model": "gpt-a",
            "options": [{"id": "reasoningEffort", "value": "high"}, {"id": "fastMode", "value": true}]});
        assert_eq!(
            selection("codex", &parent, None, None),
            json!({"instanceId": "codex", "model": "gpt-a", "options": [{"id": "fastMode", "value": true}]})
        );
        assert_eq!(
            selection("codex", &parent, Some("gpt-b"), Some("low")),
            json!({"instanceId": "codex", "model": "gpt-b",
                "options": [{"id": "reasoningEffort", "value": "low"}]})
        );
    }

    #[test]
    fn only_a_provider_native_subagent_has_a_child_thread() {
        let mut item = ProviderItem::new("call_1:c1", "subagent", "running")
            .with("origin", "provider_native")
            .with("childThreadId", "thread:subagent:abc")
            .with("nativeThreadId", "c1");
        assert_eq!(
            subagent(&item).map(|s| s.child_id),
            Some("thread:subagent:abc")
        );
        item.fields.insert("origin".into(), json!("app_owned"));
        assert!(subagent(&item).is_none());
    }
}
