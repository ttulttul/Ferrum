//! A thread forked from another (`thread.fork`): the new thread's record,
//! the pending `fork` context transfer its first run resolves, and its view
//! of the source's conversation (`ThreadForkService.plan`,
//! `ProjectionStore.buildVisibleTurnItems`).
use crate::thread_projection::uri_component;
use crate::Thread;
use serde_json::{json, Value};

/// The source run a fork starts from, by the command's source point
/// (`latest_stable`, `run` or `checkpoint`), from the source's projection.
pub(crate) fn source_run<'a>(projection: &'a Value, source_point: &Value) -> Option<&'a Value> {
    let runs = projection["runs"].as_array()?;
    match source_point["type"].as_str()? {
        "run" => runs.iter().find(|run| run["id"] == source_point["runId"]),
        "checkpoint" => runs
            .iter()
            .find(|run| run["checkpointId"] == source_point["checkpointId"]),
        _ => runs.iter().rev().find(|run| run["status"] == "completed"),
    }
}

/// The source run's latest provider turn (`providerTurnForRun`).
fn provider_turn_for_run<'a>(projection: &'a Value, run: &Value) -> Option<&'a Value> {
    let attempt = run["activeAttemptId"].as_str()?;
    projection["providerTurns"]
        .as_array()?
        .iter()
        .rev()
        .find(|turn| turn["runAttemptId"] == attempt)
}

/// The source run's provider thread (`providerThreadForRun`).
fn provider_thread_for_run<'a>(projection: &'a Value, run: &Value) -> Option<&'a Value> {
    projection["providerThreads"]
        .as_array()?
        .iter()
        .find(|thread| thread["id"] == run["providerThreadId"])
}

/// Where in the source a fork starts (`contextSourcePointForRun`).
fn source_point_for_run(thread_id: &str, projection: &Value, run: &Value) -> Value {
    let mut point = json!({"threadId": thread_id, "runId": run["id"]});
    if !run["checkpointId"].is_null() {
        point["checkpointId"] = run["checkpointId"].clone();
    }
    if let Some(reference) = provider_thread_for_run(projection, run)
        .map(|thread| &thread["nativeThreadRef"])
        .filter(|reference| !reference.is_null())
    {
        point["providerThreadRef"] = reference.clone();
    }
    if let Some(reference) = provider_turn_for_run(projection, run)
        .map(|turn| &turn["nativeTurnRef"])
        .filter(|reference| !reference.is_null())
    {
        point["providerTurnRef"] = reference.clone();
    }
    point
}

/// The pending `fork` context transfer of a fork of `source` at `run`
/// (`ThreadForkService.plan`).
pub(crate) fn pending_transfer(
    source: &Thread,
    projection: &Value,
    run: Option<&Value>,
    target_thread_id: &str,
    created_by: &Value,
    created_at: &str,
) -> Value {
    let id = format!(
        "context-transfer:type:fork:source-thread:{}:target-thread:{}:{}",
        uri_component(&source.id),
        uri_component(target_thread_id),
        crate::platform::uuid_v4()
    );
    let provider_thread = run.and_then(|run| provider_thread_for_run(projection, run));
    let strong =
        provider_thread.is_some_and(|thread| thread["nativeThreadRef"]["strength"] == "strong");
    let mut transfer = json!({
        "id": id,
        "type": "fork",
        "sourceThreadId": source.id,
        "targetThreadId": target_thread_id,
        "sourcePoint": match run {
            Some(run) => source_point_for_run(&source.id, projection, run),
            None => json!({"threadId": source.id}),
        },
        "basePoint": Value::Null,
        "sourceProviderInstanceId": run
            .map(|run| run["providerInstanceId"].clone())
            .filter(|instance| !instance.is_null())
            .unwrap_or_else(|| projection["thread"]["providerInstanceId"].clone()),
        "targetProviderInstanceId": Value::Null,
        "targetRunId": Value::Null,
        "status": "pending",
        "resolution": Value::Null,
        "createdBy": created_by,
        "error": if strong {
            Value::Null
        } else {
            json!("Source provider thread does not expose a strong native thread ref.")
        },
        "createdAt": created_at,
        "updatedAt": created_at,
        "consumedAt": Value::Null,
    });
    // Only native forks keep the source's context; portable handoffs rebuild it.
    if let Some(usage) = run
        .filter(|run| matches!(run["status"].as_str(), Some("completed" | "waiting")) && strong)
        .and_then(|run| provider_turn_for_run(projection, run))
        .map(|turn| &turn["tokenUsage"])
        .filter(|usage| !usage.is_null())
    {
        transfer["sourceTokenUsage"] = usage.clone();
    }
    transfer
}

/// The new thread's row: the source's settings under a new id and title,
/// with none of its state (`ThreadForkService.plan`).
#[allow(clippy::too_many_arguments)]
pub(crate) fn thread_row(
    source: &Thread,
    run: Option<&Value>,
    target_thread_id: &str,
    title: Option<&str>,
    side_chat: bool,
    created_by: &Value,
    creation_source: &Value,
    created_at: &str,
) -> Value {
    let mut row = source.stored_row();
    let mut lineage = json!({
        "parentThreadId": source.id,
        "relationshipToParent": "fork",
        "rootThreadId": source.row["lineage"]["rootThreadId"]
            .as_str()
            .unwrap_or(&source.id),
    });
    if side_chat {
        lineage["sideChat"] = json!(true);
    }
    let fields = json!({
        "id": target_thread_id,
        "title": title.map(str::to_owned).unwrap_or_else(|| format!("{} fork", source.title)),
        "createdBy": created_by,
        "creationSource": creation_source,
        "titleRegeneration": Value::Null,
        "titleRenamed": false,
        "activeProviderThreadId": Value::Null,
        "lineage": lineage,
        "forkedFrom": run.map_or(Value::Null, |run| {
            json!({"type": "run", "threadId": source.id, "runId": run["id"]})
        }),
        "createdAt": created_at,
        "updatedAt": created_at,
        "archivedAt": Value::Null,
        "settledOverride": Value::Null,
        "settledAt": Value::Null,
        "snoozedUntil": Value::Null,
        "snoozedAt": Value::Null,
        "lastSnoozeWakeAt": Value::Null,
        "pinnedAt": Value::Null,
        "pinOrderKey": Value::Null,
        "activeOrderKey": Value::Null,
        "autoSettleDisabledAt": Value::Null,
        "guestShare": Value::Null,
        "lastVisitedAt": Value::Null,
        "opens": Value::Null,
        "deletedAt": Value::Null,
    });
    for (key, value) in fields.as_object().into_iter().flatten() {
        row[key.as_str()] = value.clone();
    }
    for key in ["appTurnItems", "pullRequests"] {
        if let Some(fields) = row.as_object_mut() {
            fields.remove(key);
        }
    }
    row["pullRequests"] = json!([]);
    row
}

/// What a fork shows of its source: the source's visible rows up to the
/// fork's run, as inherited, then the fork marker (`buildVisibleTurnItems`).
/// A side chat shows only its own rows.
pub(crate) fn inherited_rows(
    source_projection: &Value,
    run: Option<&Value>,
    target_thread_id: &str,
    side_chat: bool,
    created_at: &str,
) -> Vec<Value> {
    let Some(run) = run.filter(|_| !side_chat) else {
        return Vec::new();
    };
    let source_id = source_projection["thread"]["id"]
        .as_str()
        .unwrap_or_default();
    let fork_ordinal = run["ordinal"].as_u64().unwrap_or(0);
    let ordinals: std::collections::HashMap<&str, u64> = source_projection["runs"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|run| run["status"] != "rolled_back")
        .filter_map(|run| Some((run["id"].as_str()?, run["ordinal"].as_u64()?)))
        .collect();
    let mut rows: Vec<Value> = source_projection["visibleTurnItems"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|row| {
            let item = &row["item"];
            if item["threadId"] != source_id || item["type"] == "fork" {
                // The source's own inherited history and marker.
                return true;
            }
            item["runId"]
                .as_str()
                .and_then(|run_id| ordinals.get(run_id))
                .is_some_and(|ordinal| *ordinal <= fork_ordinal)
        })
        .map(|row| {
            json!({
                "visibility": "inherited",
                "sourceThreadId": row["sourceThreadId"],
                "sourceItemId": row["sourceItemId"],
                "item": row["item"],
            })
        })
        .collect();
    let marker_id = format!("turn-item:fork:{target_thread_id}");
    rows.push(json!({
        "visibility": "synthetic",
        "sourceThreadId": source_id,
        "sourceItemId": marker_id,
        "item": {
            "id": marker_id,
            "threadId": target_thread_id,
            "runId": Value::Null,
            "nodeId": Value::Null,
            "providerTurnId": Value::Null,
            "nativeItemRef": Value::Null,
            "parentItemId": Value::Null,
            "ordinal": 0,
            "status": "completed",
            "title": "Forked from conversation",
            "startedAt": Value::Null,
            "completedAt": created_at,
            "updatedAt": created_at,
            "type": "fork",
            "source": {"type": "run", "threadId": source_id, "runId": run["id"]},
            "targetThreadId": target_thread_id,
        },
    }));
    rows
}

#[cfg(test)]
mod tests {
    use super::*;

    fn thread(id: &str, title: &str, row: Value) -> crate::Thread {
        crate::Thread {
            row,
            id: id.into(),
            project_id: "p".into(),
            title: title.into(),
            cwd: "/w".into(),
            status: "idle".into(),
            messages: vec![],
            runs: vec![],
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

    fn source_projection() -> Value {
        let item = |run: &str, id: &str, kind: &str| {
            json!({"visibility": "local", "sourceThreadId": "thread:s", "sourceItemId": id,
                "item": {"id": id, "threadId": "thread:s", "runId": run, "type": kind}})
        };
        json!({
            "thread": {"id": "thread:s", "providerInstanceId": "claudeAgent"},
            "runs": [
                {"id": "run:1", "ordinal": 1, "status": "completed", "checkpointId": "cp:1",
                 "activeAttemptId": "attempt:1", "providerThreadId": "pt:1",
                 "providerInstanceId": "claudeAgent"},
                {"id": "run:2", "ordinal": 2, "status": "failed", "checkpointId": null,
                 "activeAttemptId": null, "providerThreadId": "pt:1",
                 "providerInstanceId": "claudeAgent"},
            ],
            "providerThreads": [{"id": "pt:1",
                "nativeThreadRef": {"driver": "claudeAgent", "nativeId": "sess", "strength": "strong"}}],
            "providerTurns": [{"runAttemptId": "attempt:1",
                "nativeTurnRef": {"driver": "claudeAgent", "nativeId": "turn", "strength": "weak"},
                "tokenUsage": {"usedTokens": 5}}],
            "visibleTurnItems": [
                item("run:1", "a", "user_message"),
                item("run:1", "b", "assistant_message"),
                item("run:2", "c", "user_message"),
            ],
        })
    }

    #[test]
    fn a_fork_starts_from_the_latest_stable_run_and_shows_the_source_up_to_it() {
        let projection = source_projection();
        let run = source_run(&projection, &json!({"type": "latest_stable"})).unwrap();
        assert_eq!(run["id"], "run:1");
        let by_run = source_run(&projection, &json!({"type": "run", "runId": "run:2"})).unwrap();
        assert_eq!(by_run["id"], "run:2");
        let rows = inherited_rows(
            &projection,
            Some(run),
            "thread:f",
            false,
            "2026-01-01T00:00:00.000Z",
        );
        let kinds: Vec<(&str, &str)> = rows
            .iter()
            .map(|row| {
                (
                    row["visibility"].as_str().unwrap(),
                    row["item"]["type"].as_str().unwrap(),
                )
            })
            .collect();
        assert_eq!(
            kinds,
            [
                ("inherited", "user_message"),
                ("inherited", "assistant_message"),
                ("synthetic", "fork")
            ]
        );
        assert_eq!(rows[2]["item"]["id"], "turn-item:fork:thread:f");
        assert_eq!(rows[2]["item"]["source"]["runId"], "run:1");
        // A side chat shows only its own rows.
        assert!(inherited_rows(&projection, Some(run), "thread:f", true, "x").is_empty());
    }

    #[test]
    fn a_fork_transfer_waits_with_the_source_point_and_its_token_usage() {
        let projection = source_projection();
        let source = thread("thread:s", "Thread", json!({}));
        let run = source_run(&projection, &json!({"type": "latest_stable"}));
        let transfer = pending_transfer(&source, &projection, run, "thread:f", &json!("user"), "t");
        assert!(transfer["id"].as_str().unwrap().starts_with(
            "context-transfer:type:fork:source-thread:thread%3As:target-thread:thread%3Af:"
        ));
        assert_eq!(transfer["status"], "pending");
        assert_eq!(
            transfer["sourcePoint"],
            json!({"threadId": "thread:s", "runId": "run:1", "checkpointId": "cp:1",
                "providerThreadRef": {"driver": "claudeAgent", "nativeId": "sess", "strength": "strong"},
                "providerTurnRef": {"driver": "claudeAgent", "nativeId": "turn", "strength": "weak"}})
        );
        assert_eq!(transfer["sourceTokenUsage"], json!({"usedTokens": 5}));
        assert!(transfer["error"].is_null());
    }

    #[test]
    fn a_fork_row_is_a_new_thread_named_after_its_source() {
        let source = thread(
            "thread:s",
            "Thread",
            json!({"id": "thread:s", "title": "Thread", "pinnedAt": "x",
                "lineage": {"rootThreadId": "thread:root"}}),
        );
        let row = thread_row(
            &source,
            None,
            "thread:f",
            None,
            false,
            &json!("user"),
            &json!("web"),
            "t",
        );
        assert_eq!(row["title"], "Thread fork");
        assert_eq!(row["pinnedAt"], Value::Null);
        assert_eq!(row["titleRenamed"], false);
        assert_eq!(
            row["lineage"],
            json!({"parentThreadId": "thread:s", "relationshipToParent": "fork",
                "rootThreadId": "thread:root"})
        );
        let side = thread_row(
            &source,
            None,
            "thread:f",
            Some("Side"),
            true,
            &json!("user"),
            &json!("web"),
            "t",
        );
        assert_eq!(side["title"], "Side");
        assert_eq!(side["lineage"]["sideChat"], true);
    }
}
