//! A thread's shell row and visible timeline, worked out from its
//! `OrchestrationV2ThreadProjection` as the reference's
//! `threadShellFromProjection` and `activeLocalTurnItems`
//! (`orchestration-v2/ProjectionStore.ts`) do. Used for threads read from
//! `statev2.sqlite`, whose records are the reference's own.
//!
//! Not ported yet (kept at their empty values): pending background work
//! (`derivePendingBackgroundWork`) and thread-wait blocking beyond
//! `t3_thread_wait` / waiting `delegate_task` calls.
use serde_json::{json, Map, Value};

/// Milliseconds since the epoch of an ISO timestamp.
fn millis(value: &Value) -> Option<i128> {
    let text = value.as_str()?;
    time::OffsetDateTime::parse(text, &time::format_description::well_known::Rfc3339)
        .ok()
        .map(|at| at.unix_timestamp_nanos() / 1_000_000)
}

fn array<'a>(projection: &'a Value, key: &str) -> &'a [Value] {
    projection[key].as_array().map_or(&[], Vec::as_slice)
}

fn ordinal(record: &Value) -> i64 {
    record["ordinal"].as_i64().unwrap_or(0)
}

/// `createOrchestrationV2TurnItemVisibility` (`shared/orchestrationV2Timeline.ts`).
fn visibility(projection: &Value) -> impl Fn(&Value) -> bool + '_ {
    let mut statuses: Map<String, Value> = Map::new();
    for run in array(projection, "runs") {
        let id = run["id"].as_str().unwrap_or_default().to_owned();
        match run["status"].as_str() {
            Some(status @ ("rolled_back" | "cancelled" | "queued")) => {
                statuses.insert(id, json!(status));
            }
            _ => {
                statuses.remove(&id);
            }
        }
    }
    let mut superseded: Vec<(String, String)> = Vec::new();
    for attempt in array(projection, "attempts") {
        if attempt["status"] == "superseded" {
            superseded.push((
                attempt["runId"].as_str().unwrap_or_default().to_owned(),
                attempt["rootNodeId"]
                    .as_str()
                    .unwrap_or_default()
                    .to_owned(),
            ));
        }
    }
    let interrupt_runs: Vec<Value> = if superseded.is_empty() {
        Vec::new()
    } else {
        array(projection, "turnItems")
            .iter()
            .filter(|item| item["type"] == "run_interrupt_request")
            .map(|item| item["runId"].clone())
            .collect()
    };
    move |item: &Value| {
        let status = item["runId"].as_str().and_then(|run| statuses.get(run));
        if status.is_some_and(|status| status == "rolled_back") {
            return false;
        }
        if status.is_some_and(|status| status == "queued" || status == "cancelled")
            && item["type"] == "user_message"
            && item["inputIntent"] == "queued_turn"
        {
            return false;
        }
        !(item["type"] == "run_interrupt_result"
            && !item["runId"].is_null()
            && !item["nodeId"].is_null()
            && superseded.iter().any(|(run, node)| {
                item["runId"].as_str() == Some(run) && item["nodeId"].as_str() == Some(node)
            })
            && !interrupt_runs.contains(&item["runId"]))
    }
}

/// The projection's turn items a local timeline shows, in order.
pub(crate) fn visible_turn_items(projection: &Value) -> Vec<&Value> {
    let visible = visibility(projection);
    array(projection, "turnItems")
        .iter()
        .filter(|item| visible(item))
        .collect()
}

fn is_interruptible(run: &Value) -> bool {
    matches!(
        run["status"].as_str(),
        Some("preparing" | "starting" | "running")
    )
}

fn is_activity(run: &Value) -> bool {
    is_interruptible(run) || run["status"] == "waiting"
}

/// `idleCompactionOf`: the idle compaction a `/compact` message asked for.
fn idle_compaction(message: &Value) -> bool {
    let compaction = message["role"] == "user"
        && message["text"]
            .as_str()
            .unwrap_or_default()
            .trim()
            .to_lowercase()
            == "/compact"
        && message["attachments"].as_array().map_or(0, Vec::len) == 0;
    compaction
        && (!message["idleCompaction"].is_null()
            || (message["createdBy"] == "agent" && message["creationSource"] == "server"))
}

fn is_silent(run: &Value, projection: &Value) -> bool {
    run["status"] != "failed"
        && array(projection, "messages")
            .iter()
            .any(|message| message["id"] == run["userMessageId"] && idle_compaction(message))
        && !array(projection, "turnItems").iter().any(|item| {
            item["runId"] == run["id"] && item["type"] == "compaction" && item["status"] == "failed"
        })
}

/// `latestRootProviderFailure`: the failure of a failed run's root node.
fn root_failure<'a>(run: Option<&Value>, items: &'a [Value]) -> Option<&'a Value> {
    let run = run.filter(|run| run["status"] == "failed")?;
    let mut latest: Option<&Value> = None;
    for item in items {
        if item["type"] != "error"
            || item["status"] != "failed"
            || item["runId"] != run["id"]
            || item["nodeId"] != run["rootNodeId"]
        {
            continue;
        }
        let later = match latest {
            None => true,
            Some(latest) => {
                let (a, b) = (millis(&item["updatedAt"]), millis(&latest["updatedAt"]));
                a > b
                    || (a == b
                        && (ordinal(item) > ordinal(latest)
                            || (ordinal(item) == ordinal(latest)
                                && item["id"].as_str() > latest["id"].as_str())))
            }
        };
        if later {
            latest = Some(item);
        }
    }
    latest
        .map(|item| &item["failure"])
        .filter(|failure| !failure.is_null())
}

/// `threadErrorSummary`.
fn error_summary(failure: Option<&Value>, session_error: Option<&str>) -> Value {
    let current = match (session_error, failure) {
        (Some(error), Some(failure)) if failure["message"].as_str() != Some(error) => None,
        (Some(_), None) => None,
        (_, failure) => failure,
    };
    json!({
        "usageLimitResetAt": current
            .filter(|failure| failure["class"] == "usage_limit")
            .map_or(Value::Null, |failure| failure["resetAt"].clone()),
        "lastError": session_error
            .map(Value::from)
            .or_else(|| failure.map(|failure| failure["message"].clone()))
            .unwrap_or(Value::Null),
        "lastErrorClass": current.map_or(Value::Null, |failure| failure["class"].clone()),
        "lastErrorCode": current.map_or(Value::Null, |failure| failure["code"].clone()),
    })
}

/// `runRanAfter`.
fn ran_after(run: &Value, other: &Value) -> bool {
    let end = |run: &Value| millis(&run["completedAt"]).unwrap_or(i128::MAX);
    if end(run) == end(other) {
        ordinal(run) > ordinal(other)
    } else {
        end(run) > end(other)
    }
}

/// `usageLimitRunPresentedAsLatest`.
fn usage_limit_latest<'a>(
    runs: &[&'a Value],
    items: &[Value],
    session_error: Option<&str>,
) -> Option<&'a Value> {
    let mut executed: Option<&Value> = None;
    for run in runs {
        if run["status"] == "queued" || (run["status"] == "cancelled" && run["startedAt"].is_null())
        {
            continue;
        }
        if executed.is_none_or(|latest| ran_after(run, latest)) {
            executed = Some(run);
        }
    }
    let blocked = executed.filter(|run| run["status"] == "failed")?;
    let summary = error_summary(root_failure(Some(blocked), items), session_error);
    (summary["lastErrorClass"] == "usage_limit"
        && runs.iter().any(|run| ordinal(run) > ordinal(blocked)))
    .then_some(blocked)
}

/// `latestUnheldRun`.
fn latest_unheld<'a>(runs: &[&'a Value]) -> Option<&'a Value> {
    let outlived = |run: &Value| {
        if run["status"] != "cancelled" || !run["startedAt"].is_null() {
            return false;
        }
        let Some(cancelled_at) = millis(&run["completedAt"]) else {
            return false;
        };
        runs.iter().any(|earlier| {
            ordinal(earlier) < ordinal(run)
                && earlier["status"] != "queued"
                && millis(&earlier["completedAt"]).is_none_or(|at| at >= cancelled_at)
        })
    };
    let mut latest: Option<&Value> = None;
    for run in runs {
        if run["status"] == "queued" && run["queueHeld"] == true {
            continue;
        }
        if outlived(run) {
            continue;
        }
        if latest.is_none_or(|latest| ordinal(run) > ordinal(latest)) {
            latest = Some(run);
        }
    }
    latest
}

fn is_background_wake(run: &Value, projection: &Value) -> bool {
    let messages = array(projection, "messages");
    let Some(message) = messages.iter().find(|m| m["id"] == run["userMessageId"]) else {
        return false;
    };
    message.get("delegatedCompletion").is_none()
        && message["notification"]["outcome"] == "updated"
        && matches!(
            message["notification"]["source"]["kind"].as_str(),
            Some("background_task" | "command" | "monitor")
        )
        && !messages
            .iter()
            .any(|m| m["runId"] == run["id"] && m["createdBy"] == "user")
}

/// `deriveBlockedOnThreadWait`, for the thread-wait tools.
fn blocked_on_thread_wait(active: Option<&Value>, items: &[Value]) -> bool {
    let Some(active) = active.filter(|run| run["status"] == "running") else {
        return false;
    };
    const EXECUTION: &[&str] = &[
        "command_execution",
        "file_change",
        "file_search",
        "web_search",
        "dynamic_tool",
        "mcp_tool_call",
        "subagent",
    ];
    let mut waits = false;
    for item in items {
        let kind = item["type"].as_str().unwrap_or_default();
        if item["runId"] != active["id"]
            || !EXECUTION.contains(&kind)
            || !matches!(item["status"].as_str(), Some("running" | "waiting"))
            || (kind == "subagent" && item["origin"] == "app_owned")
        {
            continue;
        }
        if kind != "dynamic_tool" {
            return false;
        }
        let tool = item["toolName"].as_str().unwrap_or_default();
        let tool = tool.rsplit("__").next().unwrap_or(tool);
        if tool == "t3_thread_wait" || (tool == "delegate_task" && item["input"]["mode"] == "wait")
        {
            waits = true;
        } else {
            return false;
        }
    }
    waits
}

fn pending_request(projection: &Value, live_runs: &[&Value], latest_authored: &Value) -> Value {
    let summary = |request: &Value| {
        json!({"id": request["id"], "kind": request["kind"], "createdAt": request["createdAt"],
            "blocking": request["responseCapability"]["type"] != "message"})
    };
    let mut pending = array(projection, "runtimeRequests")
        .iter()
        .filter(|request| request["status"] == "pending")
        .min_by(|a, b| {
            let question = |r: &Value| r["kind"] == "user_input";
            question(a)
                .cmp(&question(b))
                .then_with(|| millis(&b["createdAt"]).cmp(&millis(&a["createdAt"])))
        })
        .map(summary);
    if pending
        .as_ref()
        .is_none_or(|request| request["kind"] == "user_input")
    {
        let waits_on_user = |item: &Value| {
            let asks = |item: &Value| {
                millis(latest_authored).is_none_or(|authored| {
                    authored
                        <= millis(if item["startedAt"].is_null() {
                            &item["updatedAt"]
                        } else {
                            &item["startedAt"]
                        })
                        .unwrap_or(i128::MIN)
                })
            };
            (item["type"] == "link_request" && asks(item))
                || (item["type"] == "secret_request"
                    && !item["runId"].is_null()
                    && live_runs.iter().any(|run| run["id"] == item["runId"]))
        };
        let card = array(projection, "turnItems")
            .iter()
            .filter(|item| item["status"] == "waiting" && waits_on_user(item))
            .max_by_key(|item| millis(&item["updatedAt"]))
            .filter(|item| !item["nodeId"].is_null())
            .map(|item| {
                json!({"id": item["id"], "kind": "user_input",
                    "createdAt": if item["startedAt"].is_null() { &item["updatedAt"] } else { &item["startedAt"] },
                    "blocking": true, "userOnly": true})
            });
        if let Some(card) = card {
            if pending
                .as_ref()
                .is_none_or(|request| millis(&card["createdAt"]) >= millis(&request["createdAt"]))
            {
                pending = Some(card);
            }
        }
    }
    pending.unwrap_or(Value::Null)
}

/// `threadShellFromProjection`, with the shell's fields in the reference's order.
pub(crate) fn shell(projection: &Value, fields: &[&str]) -> Value {
    let thread = &projection["thread"];
    let instance = &thread["providerInstanceId"];
    let session = array(projection, "providerSessions")
        .iter()
        .filter(|session| &session["providerInstanceId"] == instance)
        .max_by_key(|session| millis(&session["updatedAt"]));
    let session_error = session.and_then(|session| session["lastError"].as_str());
    let items = array(projection, "turnItems");
    let runs: Vec<&Value> = array(projection, "runs")
        .iter()
        .filter(|run| !is_silent(run, projection))
        .collect();
    let latest = usage_limit_latest(&runs, items, session_error).or_else(|| latest_unheld(&runs));
    let active = runs
        .iter()
        .copied()
        .filter(|run| is_interruptible(run))
        .max_by_key(|run| ordinal(run));
    let live: Vec<&Value> = runs
        .iter()
        .copied()
        .filter(|run| is_activity(run))
        .collect();
    let activity = live.iter().copied().max_by_key(|run| ordinal(run));
    let mut user_messages: Vec<&Value> = array(projection, "messages")
        .iter()
        .filter(|message| message["role"] == "user" && !idle_compaction(message))
        .collect();
    // Newest first, keeping the stored order among equal times as the reference's stable sort does.
    user_messages.sort_by(|a, b| millis(&b["updatedAt"]).cmp(&millis(&a["updatedAt"])));
    let latest_authored = user_messages
        .iter()
        .find(|message| message["createdBy"] == "user")
        .map_or(Value::Null, |message| message["updatedAt"].clone());
    let mut history: Vec<Value> = Vec::new();
    let mut provider_threads: Vec<&Value> = array(projection, "providerThreads")
        .iter()
        .filter(|t| t["appThreadId"] == thread["id"] && t["ownerNodeId"].is_null())
        .collect();
    provider_threads.sort_by(|a, b| {
        millis(&a["createdAt"])
            .cmp(&millis(&b["createdAt"]))
            .then_with(|| a["id"].as_str().cmp(&b["id"].as_str()))
    });
    for provider_thread in provider_threads {
        if !history.contains(&provider_thread["providerInstanceId"]) {
            history.push(provider_thread["providerInstanceId"].clone());
        }
    }
    let goal = array(projection, "providerThreads")
        .iter()
        .find(|t| t["id"] == thread["activeProviderThreadId"])
        .map_or(Value::Null, |t| {
            t.get("goal").cloned().unwrap_or(Value::Null)
        });
    let pull_requests = match thread.get("pullRequests") {
        Some(links) => links.clone(),
        None => json!([]),
    };
    let field = |key: &str| thread.get(key).cloned().unwrap_or(Value::Null);
    let mut row = Map::new();
    for key in [
        "createdBy",
        "creationSource",
        "id",
        "projectId",
        "title",
        "providerInstanceId",
        "modelSelection",
        "runtimeMode",
        "interactionMode",
        "branch",
        "worktreePath",
    ] {
        row.insert(key.into(), field(key));
    }
    row.insert("pullRequests".into(), pull_requests);
    for key in [
        "linkedPullRequest",
        "branchPullRequest",
        "issues",
        "activeOrderKey",
        "historyOrigin",
        "linkOrigin",
        "delegatedFrom",
    ] {
        if let Some(value) = thread.get(key) {
            row.insert(key.into(), value.clone());
        }
    }
    for key in ["lineage", "forkedFrom", "activeProviderThreadId"] {
        row.insert(key.into(), field(key));
    }
    let run_field =
        |run: Option<&Value>, key: &str| run.map_or(Value::Null, |run| run[key].clone());
    row.insert("latestRunId".into(), run_field(latest, "id"));
    row.insert(
        "latestRunRequestedAt".into(),
        run_field(latest, "requestedAt"),
    );
    row.insert("latestRunStartedAt".into(), run_field(latest, "startedAt"));
    row.insert(
        "latestRunCompletedAt".into(),
        run_field(latest, "completedAt"),
    );
    row.insert(
        "latestRunStopRequested".into(),
        json!(latest.is_some_and(|run| items
            .iter()
            .any(|item| item["runId"] == run["id"] && item["type"] == "run_interrupt_request"))),
    );
    row.insert(
        "latestRunBackgroundWake".into(),
        json!(latest.is_some_and(|run| is_background_wake(run, projection))),
    );
    row.insert("activeRunId".into(), run_field(active, "id"));
    row.insert(
        "blockedOnThreadWait".into(),
        json!(blocked_on_thread_wait(active, items)),
    );
    row.insert("activityRunStatus".into(), run_field(activity, "status"));
    row.insert(
        "activityRunStartedAt".into(),
        activity.map_or(Value::Null, |run| {
            ["workStartedAt", "startedAt", "requestedAt"]
                .iter()
                .map(|key| &run[*key])
                .find(|value| !value.is_null())
                .cloned()
                .unwrap_or(Value::Null)
        }),
    );
    row.insert(
        "status".into(),
        latest.map_or(json!("idle"), |run| run["status"].clone()),
    );
    let failure = root_failure(latest, items);
    if let Value::Object(summary) = error_summary(failure, session_error) {
        row.extend(summary);
    }
    row.insert(
        "pendingRuntimeRequest".into(),
        pending_request(projection, &live, &latest_authored),
    );
    row.insert(
        "pendingGuestMessageCount".into(),
        json!(thread["guestShare"]["messages"]
            .as_array()
            .into_iter()
            .flatten()
            .filter(|m| m["status"] == "pending")
            .count()),
    );
    row.insert("latestVisibleMessage".into(), Value::Null);
    row.insert(
        "latestUserMessageAt".into(),
        user_messages
            .first()
            .map_or(Value::Null, |m| m["updatedAt"].clone()),
    );
    row.insert("latestUserAuthoredMessageAt".into(), latest_authored);
    row.insert(
        "hasActionableProposedPlan".into(),
        json!(array(projection, "plans")
            .iter()
            .any(|plan| plan["kind"] == "proposed_plan" && plan["status"] == "active")),
    );
    row.insert("pendingBackgroundTasks".into(), json!([]));
    row.insert("providerInstanceHistory".into(), Value::Array(history));
    row.insert("goal".into(), goal);
    row.insert(
        "itemCount".into(),
        json!(visible_turn_items(projection).len()),
    );
    row.insert(
        "visibleItemCount".into(),
        json!(array(projection, "visibleTurnItems").len()),
    );
    for key in [
        "createdAt",
        "updatedAt",
        "archivedAt",
        "settledOverride",
        "settledAt",
    ] {
        row.insert(key.into(), field(key));
    }
    for key in [
        "unsettledAt",
        "snoozedUntil",
        "snoozedAt",
        "pinnedAt",
        "autoSettleDisabledAt",
        "pinOrderKey",
    ] {
        row.insert(key.into(), field(key));
    }
    if thread["tags"]
        .as_array()
        .is_some_and(|tags| !tags.is_empty())
    {
        row.insert("tags".into(), thread["tags"].clone());
    }
    row.insert("lastVisitedAt".into(), field("lastVisitedAt"));
    for key in [
        "opens",
        "titleRegeneration",
        "lastTitleRegeneration",
        "limitRecovery",
    ] {
        row.insert(key.into(), field(key));
    }
    row.insert("deletedAt".into(), field("deletedAt"));
    // The reference's shell field order, then anything it does not list.
    let mut ordered = Map::new();
    for key in fields {
        if let Some(value) = row.remove(*key) {
            ordered.insert((*key).to_owned(), value);
        }
    }
    ordered.extend(row);
    Value::Object(ordered)
}
