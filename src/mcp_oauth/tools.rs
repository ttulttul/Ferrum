//! The thread, project and environment tools `/mcp` serves to a signed-in
//! client: an outside agent, or another T3 Code server through a peer link.
//!
//! Reads come from the control state's store. Changes are orchestration
//! commands, dispatched through the same path as the WebSocket RPC
//! `orchestration.dispatchCommand`.
use super::{McpAccess, McpHost};
use serde_json::{json, Value};
use std::sync::{Arc, LazyLock};

/// The reference's definitions of the tools served here, as `tools/list` returns them.
pub(super) static TOOLS: LazyLock<Value> = LazyLock::new(|| {
    serde_json::from_str(include_str!("tools.json")).expect("MCP tool definitions")
});

const RUNTIME_MODES: [&str; 4] = [
    "approval-required",
    "auto-accept-edits",
    "auto",
    "full-access",
];
const TERMINAL: [&str; 5] = [
    "completed",
    "interrupted",
    "failed",
    "cancelled",
    "rolled_back",
];
const DEFAULT_WAIT_TIMEOUT_MS: i64 = 30_000;
const MAX_WAIT_TIMEOUT_MS: i64 = 45_000;
const WAIT_POLL_MS: u64 = 250;
const DEFAULT_THREAD_LIST_LIMIT: usize = 10;
const DEFAULT_THREAD_READ_LIMIT: usize = 10;
const DEFAULT_THREAD_RUN_LIMIT: usize = 10;
const DEFAULT_THREAD_ITEM_MAX_CHARS: usize = 500;
/// The header a peer link sends with each call: the calling agent's modes there.
pub(super) const MODE_LIMIT_HEADER: &str = "t3-mode-limit";

/// How a tool call ends when it does not succeed.
pub(super) enum Refusal {
    /// A JSON-RPC error: unknown tool or malformed arguments.
    Rpc(i32, String),
    /// A tool failure the agent reads (`OrchestratorMcpFailure`).
    Failure(&'static str, String),
}
type Outcome = Result<Value, Refusal>;

fn failure(code: &'static str, message: impl Into<String>) -> Refusal {
    Refusal::Failure(code, message.into())
}
fn invalid_arguments() -> Refusal {
    Refusal::Rpc(-32602, "Invalid tool arguments".into())
}

/// What one call may do: the token's access, narrowed by the modes the caller
/// runs in (`T3-Mode-Limit: <runtime>/<interaction>`).
pub(super) struct Caller {
    pub granted: McpAccess,
    pub runtime_limit: Option<String>,
    pub plan_only: bool,
}

fn rank(mode: &str) -> Option<usize> {
    RUNTIME_MODES.iter().position(|m| *m == mode)
}

impl Caller {
    pub(super) fn new(granted: McpAccess, limit: Option<&str>) -> Self {
        let (runtime, interaction) = limit
            .and_then(|l| l.split_once('/'))
            .map(|(r, i)| (Some(r.trim().to_owned()), i.trim() == "plan"))
            .unwrap_or((None, false));
        Self {
            granted,
            runtime_limit: runtime.filter(|r| rank(r).is_some()),
            plan_only: interaction,
        }
    }
    /// The broadest runtime mode this call may start or drive work in, or
    /// `None` when it may only read.
    fn ceiling(&self) -> Option<&str> {
        let granted = rank(&self.granted.access)?;
        let limited = self
            .runtime_limit
            .as_deref()
            .and_then(rank)
            .unwrap_or(granted);
        Some(RUNTIME_MODES[granted.min(limited)])
    }
    fn interaction(&self) -> &'static str {
        if self.plan_only {
            "plan"
        } else {
            "default"
        }
    }
    fn writable(&self) -> Result<&str, Refusal> {
        self.ceiling().ok_or_else(|| {
            failure(
                "capability_denied",
                "This MCP client was approved with read-only access, so it cannot start or change work.",
            )
        })
    }
}

fn thread_link<S: McpHost>(host: &S, caller: &Caller, thread_id: &str, title: &str) -> String {
    let environment = host.environment_id();
    if caller.granted.peer_link {
        let label = title
            .replace(['[', ']', '\\', '\r', '\n'], " ")
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" ");
        let href = format!("t3-thread://v2/{environment}/{thread_id}");
        let destination = if href.contains(['(', ')']) {
            format!("<{href}>")
        } else {
            href.clone()
        };
        if label.is_empty() {
            format!("<{href}>")
        } else {
            format!("[{label}]({destination})")
        }
    } else {
        let segment = |id: &str| {
            percent_encoding::utf8_percent_encode(id, percent_encoding::NON_ALPHANUMERIC)
                .to_string()
                .replace("%2D", "-")
                .replace("%5F", "_")
        };
        format!(
            "https://app.t3.codes/threads/{}/{}",
            segment(environment),
            segment(thread_id)
        )
    }
}

fn arg_str<'a>(args: &'a Value, key: &str) -> Result<Option<&'a str>, Refusal> {
    match args.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(s)) => Ok(Some(s)),
        Some(_) => Err(invalid_arguments()),
    }
}
fn arg_bool(args: &Value, key: &str) -> Result<Option<bool>, Refusal> {
    match args.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Bool(b)) => Ok(Some(*b)),
        Some(_) => Err(invalid_arguments()),
    }
}
fn arg_count(args: &Value, key: &str, min: u64) -> Result<Option<u64>, Refusal> {
    match args.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(v) => v
            .as_u64()
            .filter(|n| *n >= min)
            .map(Some)
            .ok_or_else(invalid_arguments),
    }
}
fn required<'a>(args: &'a Value, key: &str) -> Result<&'a str, Refusal> {
    arg_str(args, key)?.ok_or_else(invalid_arguments)
}

/// A thread's runs as `OrchestrationV2Run` rows, oldest first.
fn runs(projection: &Value) -> Vec<Value> {
    projection["runs"].as_array().cloned().unwrap_or_default()
}
fn latest(runs: &[Value]) -> Option<&Value> {
    runs.iter().max_by_key(|run| run["ordinal"].as_u64())
}
fn active(runs: &[Value]) -> Option<&Value> {
    runs.iter()
        .filter(|run| !TERMINAL.contains(&run["status"].as_str().unwrap_or("")))
        .max_by_key(|run| run["ordinal"].as_u64())
}
fn thread_status(runs: &[Value]) -> Value {
    latest(runs).map_or(json!("idle"), |run| run["status"].clone())
}
fn snoozed(row: &Value) -> bool {
    row["snoozedUntil"].as_str().is_some_and(|until| {
        time::OffsetDateTime::parse(until, &time::format_description::well_known::Rfc3339)
            .is_ok_and(|at| at.unix_timestamp() * 1000 > crate::platform::now_ms())
    })
}

/// The fields a thread list item and a thread read share.
fn thread_summary<S: McpHost>(host: &S, caller: &Caller, thread: &crate::Thread) -> Value {
    let row = thread.shell();
    let projection = thread.projection();
    let runs = runs(&projection);
    let selection = &projection["runs"]
        .as_array()
        .and_then(|r| r.last())
        .map(|run| run["modelSelection"].clone())
        .filter(|s| s.is_object())
        .unwrap_or_else(|| row["modelSelection"].clone());
    let instance = row["providerInstanceId"]
        .as_str()
        .or(selection["instanceId"].as_str())
        .unwrap_or("codex");
    json!({
        "threadId": thread.id,
        "projectId": thread.project_id,
        "title": thread.title,
        "link": thread_link(host, caller, &thread.id, &thread.title),
        "createdBy": row["createdBy"].as_str().unwrap_or("user"),
        "creationSource": row["creationSource"].as_str().unwrap_or("web"),
        "status": thread_status(&runs),
        "latestRunId": latest(&runs).map(|run| run["id"].clone()),
        "providerInstanceId": instance,
        "model": selection["model"].as_str().unwrap_or("default"),
        "runtimeMode": row["runtimeMode"].as_str().unwrap_or("full-access"),
        "interactionMode": row["interactionMode"].as_str().unwrap_or("default"),
        "linkedPullRequest": null,
        "pullRequests": row["pullRequests"].as_array().cloned().unwrap_or_default(),
        "settled": thread.settled,
        "settledAt": row["settledAt"],
        "snoozed": snoozed(&row),
        "snoozedUntil": row["snoozedUntil"],
        "pinned": thread.pinned,
        "blockedOnThreadWait": false,
        "parentThreadId": row["lineage"]["parentThreadId"],
        "relationshipToParent": row["lineage"]["relationshipToParent"],
        "itemCount": projection["visibleTurnItems"].as_array().map_or(0, Vec::len),
        "createdAt": row["createdAt"],
        "updatedAt": row["updatedAt"],
    })
}

fn thread_not_found() -> Refusal {
    failure("thread_not_found", "The thread was not found.")
}

/// A live thread from the store, or the failure an agent hears.
fn find_thread<'a>(store: &'a crate::store::Store, id: &str) -> Result<&'a crate::Thread, Refusal> {
    store
        .threads
        .get(id)
        .filter(|t| t.stored_row()["deletedAt"].is_null())
        .ok_or_else(thread_not_found)
}

pub(super) async fn call<S: McpHost>(
    host: &Arc<S>,
    caller: &Caller,
    name: &str,
    args: &Value,
) -> Outcome {
    if !args.is_object() && !args.is_null() {
        return Err(invalid_arguments());
    }
    match name {
        "t3_project_list" => project_list(&**host, args).await,
        "t3_project_read" => project_read(&**host, args).await,
        "t3_project_create" => project_create(host, caller, args).await,
        "t3_environment_read" => Ok(environment_read(&**host)),
        "orchestrator_capabilities" => Ok(capabilities(&**host, caller)),
        "t3_thread_list" => thread_list(&**host, caller, args).await,
        "t3_thread_read" => thread_read(&**host, caller, args).await,
        "t3_thread_launch" => thread_launch(host, caller, args).await,
        "t3_thread_send" => thread_send(host, caller, args).await,
        "t3_thread_wait" => thread_wait(&**host, args).await,
        "t3_thread_interrupt" => thread_interrupt(host, caller, args).await,
        _ => Err(Refusal::Rpc(-32602, format!("Tool '{name}' not found"))),
    }
}

async fn project_list<S: McpHost>(host: &S, a: &Value) -> Outcome {
    if a.get("cursor").is_some_and(|n| !n.is_u64())
        || a.get("limit")
            .is_some_and(|n| n.as_u64().is_none_or(|n| n == 0))
    {
        return Err(invalid_arguments());
    }
    let cursor = a["cursor"].as_u64().unwrap_or(0) as usize;
    let limit = a["limit"].as_u64().unwrap_or(20) as usize;
    let store = host.control_state().store.read().await;
    let rows = store
        .project_rows
        .values()
        .filter(|r| r["deletedAt"].is_null())
        .collect::<Vec<_>>();
    let end = cursor.saturating_add(limit);
    Ok(json!({
        "projects": rows.iter().skip(cursor).take(limit).map(|row| host.project_row(row)).collect::<Vec<_>>(),
        "nextCursor": (end < rows.len()).then_some(end)
    }))
}

async fn project_read<S: McpHost>(host: &S, a: &Value) -> Outcome {
    let store = host.control_state().store.read().await;
    a["projectId"]
        .as_str()
        .and_then(|id| store.project_rows.get(id))
        .filter(|r| r["deletedAt"].is_null())
        .map(|row| host.project_row(row))
        .ok_or_else(|| failure("invalid_request", "The project was not found."))
}

async fn project_create<S: McpHost>(host: &Arc<S>, caller: &Caller, a: &Value) -> Outcome {
    caller.writable()?;
    let title = required(a, "title")?.trim();
    if title.is_empty() {
        return Err(invalid_arguments());
    }
    let id = crate::platform::uuid_v4().to_string();
    let mut command = json!({
        "type": "project.create",
        "commandId": format!("mcp:{}", crate::platform::uuid_v4()),
        "projectId": id,
        "title": title,
    });
    for key in [
        "workspaceRoot",
        "createWorkspaceRootIfMissing",
        "defaultModelSelection",
        "scripts",
    ] {
        if let Some(value) = a.get(key).filter(|v| !v.is_null()) {
            command[key] = value.clone();
        }
    }
    if command.get("workspaceRoot").is_none() {
        // Started from its title: a new repository, as the reference makes one.
        if [
            "scripts",
            "createWorkspaceRootIfMissing",
            "defaultModelSelection",
        ]
        .iter()
        .any(|key| command.get(*key).is_some())
        {
            return Err(failure(
                "invalid_request",
                "A project started from its title takes only a title; set scripts or defaultModelSelection afterwards with t3_project_update.",
            ));
        }
        let created = host
            .create_named_project(title.to_owned())
            .await
            .map_err(|error| {
                failure(
                    "orchestration_error",
                    error["message"]
                        .as_str()
                        .unwrap_or("Failed to create the project.")
                        .to_owned(),
                )
            })?;
        let store = host.control_state().store.read().await;
        let mut project = created["projectId"]
            .as_str()
            .and_then(|id| store.project_rows.get(id))
            .map(|row| host.project_row(row))
            .ok_or_else(|| failure("orchestration_error", "The project was not recorded."))?;
        if let Some(error) = created.get("commitError").filter(|e| e.is_string()) {
            project["commitError"] = error.clone();
        }
        return Ok(project);
    }
    match host.mutate_project(command).await {
        Ok(row) => Ok(host.project_row(&row)),
        Err(error) => Err(failure(
            "orchestration_error",
            error["message"]
                .as_str()
                .unwrap_or("Failed to mutate project.")
                .to_owned(),
        )),
    }
}

fn environment_read<S: McpHost>(host: &S) -> Value {
    let descriptor = host.descriptor();
    let settings = host.settings();
    let style = &settings["sourceControlWritingStyle"];
    json!({
        "environmentId": descriptor["environmentId"],
        "label": descriptor["label"],
        "serverVersion": descriptor["serverVersion"],
        "platform": descriptor["platform"],
        "preferences": {
            "defaultThreadEnvMode": settings.get("defaultThreadEnvMode").cloned().unwrap_or(Value::Null),
            "newWorktreesStartFromOrigin": settings["newWorktreesStartFromOrigin"],
            "enableProviderUpdateChecks": settings["enableProviderUpdateChecks"],
            "backgroundActivity": {"profile": settings["backgroundActivity"]["profile"].as_str().unwrap_or("balanced")},
            "sourceControlWritingStyle": {
                "mode": style["mode"].as_str().unwrap_or("repo_conventions"),
                "followChangeRequestTemplates": style["followChangeRequestTemplates"].as_bool().unwrap_or(true),
                "customInstructions": style["customInstructions"].as_str().unwrap_or(""),
                "truncated": false
            }
        }
    })
}

/// Providers as `orchestrator_capabilities` lists them.
fn provider_entries<S: McpHost>(host: &S) -> Vec<Value> {
    host.providers()
        .as_array()
        .into_iter()
        .flatten()
        .filter(|p| p["enabled"] != false)
        .filter_map(|p| {
            let id = p["instanceId"].as_str().or(p["provider"].as_str())?;
            let driver = p["driver"]
                .as_str()
                .or(p["provider"].as_str())
                .unwrap_or(id);
            let models = p["models"]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(|m| {
                    let slug = m["slug"].as_str().or(m["id"].as_str())?;
                    let mut model = json!({"id": slug});
                    if let Some(label) = m["name"].as_str().or(m["label"].as_str()) {
                        model["label"] = json!(label);
                    }
                    Some(model)
                })
                .collect::<Vec<_>>();
            let available =
                p["status"].as_str().is_none_or(|s| s != "error") && p["installed"] != false;
            Some(json!({
                "providerInstanceId": id,
                "driverKind": driver,
                "displayName": p["displayName"].as_str().or(p["name"].as_str()),
                "models": models,
                "runtimeModes": RUNTIME_MODES,
                "canRunChildTask": available,
                "canRunCrossProviderChildTask": available,
                "constraints": []
            }))
        })
        .collect()
}

fn capabilities<S: McpHost>(host: &S, caller: &Caller) -> Value {
    json!({
        "parentThreadId": null,
        "inheritedProviderInstanceId": null,
        "inheritedModel": null,
        "runtimeMode": caller.ceiling().unwrap_or("approval-required"),
        "interactionMode": caller.interaction(),
        "providers": provider_entries(host),
        "features": {
            "appOwnedSubagents": false,
            "asyncPolling": true,
            "cancellation": true,
            "batchThreadCreation": false,
            "threadManagement": true,
            "incrementalThreadRead": true,
            "scheduledTasks": false,
            "maxBatchThreads": 0
        }
    })
}

async fn thread_list<S: McpHost>(host: &S, caller: &Caller, a: &Value) -> Outcome {
    let all = arg_bool(a, "allProjects")?.unwrap_or(false);
    let project = arg_str(a, "projectId")?;
    let title = arg_str(a, "titleContains")?.map(str::to_lowercase);
    let settled = arg_bool(a, "settled")?;
    let snoozed_only = arg_bool(a, "snoozed")?;
    let cursor = arg_count(a, "cursor", 0)?.unwrap_or(0) as usize;
    let limit = arg_count(a, "limit", 1)?.map_or(DEFAULT_THREAD_LIST_LIMIT, |n| n as usize);
    let statuses: Option<Vec<String>> = match a.get("statuses") {
        None | Some(Value::Null) => None,
        Some(v) => Some(serde_json::from_value(v.clone()).map_err(|_| invalid_arguments())?),
    }
    .filter(|s: &Vec<String>| !s.is_empty());
    if !all && project.is_none() {
        return Err(failure(
            "invalid_request",
            "Pass projectId, or allProjects: true for every project. This MCP client is not running inside a T3 thread.",
        ));
    }
    let store = host.control_state().store.read().await;
    if let Some(id) = project.filter(|_| !all) {
        if !store
            .project_rows
            .get(id)
            .is_some_and(|r| r["deletedAt"].is_null())
        {
            return Err(failure("invalid_request", "The project was not found."));
        }
    }
    let mut threads = store
        .threads
        .values()
        .filter(|t| t.stored_row()["deletedAt"].is_null())
        .filter(|t| all || Some(t.project_id.as_str()) == project)
        .filter(|t| settled.is_none_or(|s| t.settled == s))
        .filter(|t| {
            title
                .as_deref()
                .is_none_or(|q| t.title.to_lowercase().contains(q))
        })
        .map(|t| thread_summary(host, caller, t))
        .filter(|row| {
            statuses
                .as_ref()
                .is_none_or(|s| s.iter().any(|status| row["status"] == status.as_str()))
        })
        .filter(|row| snoozed_only.is_none_or(|s| row["snoozed"] == s))
        .collect::<Vec<_>>();
    threads.sort_by(|a, b| {
        b["createdAt"]
            .as_str()
            .cmp(&a["createdAt"].as_str())
            .then_with(|| b["threadId"].as_str().cmp(&a["threadId"].as_str()))
    });
    let total = threads.len();
    let page = threads
        .into_iter()
        .skip(cursor)
        .take(limit)
        .collect::<Vec<_>>();
    Ok(json!({
        "projectId": if all { None } else { project },
        "currentThreadId": null,
        "threads": page,
        "nextCursor": (cursor + limit < total).then_some(cursor + limit),
        "total": total
    }))
}

fn item_status(status: &str) -> &'static str {
    match status {
        "idle" => "idle",
        "pending" | "queued" | "preparing" | "starting" => "pending",
        "running" | "in_progress" | "streaming" => "running",
        "waiting" => "waiting",
        "failed" | "error" => "failed",
        "cancelled" => "cancelled",
        "interrupted" => "interrupted",
        _ => "completed",
    }
}

fn read_item(
    thread: &crate::Thread,
    texts: &std::collections::HashMap<String, String>,
    visible: &Value,
    max_chars: usize,
    offset: usize,
) -> Value {
    let item = &visible["item"];
    let message_id = item["messageId"].as_str();
    let full = item["text"]
        .as_str()
        .map(str::to_owned)
        .or_else(|| message_id.and_then(|id| texts.get(id).cloned()))
        .or_else(|| item["failure"]["message"].as_str().map(str::to_owned));
    let (text, truncated, next) = match full {
        Some(full) => {
            let chars = full.chars().collect::<Vec<_>>();
            let start = offset.min(chars.len());
            let end = start.saturating_add(max_chars).min(chars.len());
            let truncated = end < chars.len();
            (
                Some(chars[start..end].iter().collect::<String>()),
                truncated,
                truncated.then_some(end),
            )
        }
        None => (None, false, None),
    };
    let (created_by, source) = match item["type"].as_str() {
        Some("user_message") => (item["createdBy"].clone(), item["creationSource"].clone()),
        Some("assistant_message") => (json!("agent"), json!("provider")),
        _ => (Value::Null, Value::Null),
    };
    json!({
        "position": visible["position"],
        "visibility": "local",
        "sourceThreadId": thread.id,
        "itemId": item["id"],
        "runId": item["runId"],
        "messageId": message_id,
        "createdBy": created_by,
        "creationSource": source,
        "type": item["type"],
        "status": item_status(item["status"].as_str().unwrap_or("completed")),
        "title": item["title"],
        "text": text,
        "textTruncated": truncated,
        "nextTextOffset": next,
        "updatedAt": item["updatedAt"],
    })
}

async fn thread_read<S: McpHost>(host: &S, caller: &Caller, a: &Value) -> Outcome {
    let id = required(a, "threadId")?;
    let item_id = arg_str(a, "itemId")?;
    let text_offset = arg_count(a, "textOffset", 0)?.unwrap_or(0) as usize;
    let view = arg_str(a, "view")?.unwrap_or("messages");
    if !matches!(view, "messages" | "activity") {
        return Err(invalid_arguments());
    }
    let after = match a.get("afterPosition") {
        None | Some(Value::Null) => None,
        Some(v) => Some(
            v.as_i64()
                .filter(|n| *n >= -1)
                .ok_or_else(invalid_arguments)?,
        ),
    };
    let before = arg_count(a, "beforePosition", 0)?;
    let limit =
        arg_count(a, "limit", 1)?.map_or(DEFAULT_THREAD_READ_LIMIT, |n| (n as usize).min(100));
    let run_limit =
        arg_count(a, "runLimit", 1)?.map_or(DEFAULT_THREAD_RUN_LIMIT, |n| (n as usize).min(50));
    let max_chars = arg_count(a, "maxCharsPerItem", 1)?
        .map_or(DEFAULT_THREAD_ITEM_MAX_CHARS, |n| (n as usize).min(20_000));
    let store = host.control_state().store.read().await;
    let thread = find_thread(&store, id)?;
    let projection = thread.projection();
    let row = thread.shell();
    let all_runs = runs(&projection);
    let texts = projection["messages"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|m| Some((m["id"].as_str()?.to_owned(), m["text"].as_str()?.to_owned())))
        .collect::<std::collections::HashMap<_, _>>();
    let visible = projection["visibleTurnItems"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    let shown = |entry: &&Value| {
        view == "activity"
            || matches!(
                entry["item"]["type"].as_str(),
                Some("user_message" | "assistant_message" | "error")
            )
    };
    let timeline = visible.iter().filter(shown).collect::<Vec<_>>();
    let position = |entry: &Value| entry["position"].as_i64().unwrap_or(0);
    // Cursorless reads by a linked environment get the newest items, as its
    // agents expect; other clients page forward from the start.
    let newest_first = before.is_some() || (after.is_none() && caller.granted.peer_link);
    let (page, has_more, has_earlier, offset) = if let Some(item_id) = item_id {
        let found = timeline
            .iter()
            .copied()
            .filter(|e| e["item"]["id"] == item_id)
            .collect::<Vec<_>>();
        if found.is_empty() {
            return Err(failure(
                "invalid_request",
                "The item was not found in this thread.",
            ));
        }
        (found, false, false, text_offset)
    } else {
        let after = after.unwrap_or(-1);
        let before = before.map_or(i64::MAX, |n| n as i64);
        let matching = timeline
            .iter()
            .copied()
            .filter(|e| position(e) > after && position(e) < before)
            .collect::<Vec<_>>();
        let page = if newest_first {
            matching[matching.len().saturating_sub(limit)..].to_vec()
        } else {
            matching.iter().copied().take(limit).collect()
        };
        let has_more = if newest_first {
            timeline.last().is_some_and(|e| position(e) >= before)
        } else {
            matching.len() > page.len()
        };
        let start = page.first().map_or(after + 1, |e| position(e));
        let has_earlier = timeline.first().is_some_and(|e| position(e) < start);
        (page, has_more, has_earlier, 0)
    };
    let items = page
        .iter()
        .map(|e| read_item(thread, &texts, e, max_chars, offset))
        .collect::<Vec<_>>();
    let mut sorted = all_runs.clone();
    sorted.sort_by_key(|run| std::cmp::Reverse(run["ordinal"].as_u64()));
    let recent = sorted
        .iter()
        .take(run_limit)
        .map(|run| {
            json!({
                "runId": run["id"],
                "ordinal": run["ordinal"],
                "status": run["status"],
                "providerInstanceId": run["providerInstanceId"],
                "model": run["modelSelection"]["model"].as_str().unwrap_or("default"),
                "requestedAt": run["requestedAt"],
                "startedAt": run["startedAt"],
                "completedAt": run["completedAt"],
            })
        })
        .collect::<Vec<_>>();
    let mut detail = thread_summary(host, caller, thread);
    let extra = json!({
        "activeRunId": active(&all_runs).map(|run| run["id"].clone()),
        "titleRegeneration": row["titleRegeneration"],
        "lastTitleRegeneration": row["lastTitleRegeneration"],
        "branch": row["branch"],
        "worktreePath": row["worktreePath"],
        "runCount": all_runs.len(),
        "pendingRequestCount": projection["runtimeRequests"].as_array().into_iter().flatten()
            .filter(|r| r["status"] == "pending").count(),
        "archived": thread.archived,
        "pendingBackgroundTasks": [],
    });
    for (key, value) in extra.as_object().into_iter().flatten() {
        detail[key] = value.clone();
    }
    // Optional, and omitted when absent: the reference's result schema rejects null.
    if let Some(from) = row.get("delegatedFrom").filter(|v| v.is_object()) {
        detail["delegatedFrom"] = from.clone();
    }
    Ok(json!({
        "thread": detail,
        "recentRuns": recent,
        "nextPosition": items.last().map(|item| item["position"].clone()),
        "hasMore": has_more,
        "earlierPosition": if has_earlier { items.first().map(|item| item["position"].clone()) } else { None },
        "items": items,
    }))
}

/// The model a new thread in `project` starts with when the caller names none.
fn default_selection<S: McpHost>(host: &S, project: &Value) -> Option<Value> {
    let settings = host.settings();
    let usable = |s: &Value| s["model"].is_string() && s["instanceId"].is_string();
    for candidate in [
        &project["defaultModelSelection"],
        &settings["defaultModelSelection"],
        &settings["fallbackModelSelection"],
    ] {
        if usable(candidate) {
            return Some(candidate.clone());
        }
    }
    provider_entries(host)
        .into_iter()
        .filter(|p| p["canRunChildTask"] == true)
        .find_map(|p| {
            let model = p["models"][0]["id"].as_str()?.to_owned();
            Some(json!({"instanceId": p["providerInstanceId"], "model": model}))
        })
}

async fn thread_launch<S: McpHost>(host: &Arc<S>, caller: &Caller, a: &Value) -> Outcome {
    let ceiling = caller.writable()?.to_owned();
    let title = required(a, "title")?.to_owned();
    if a.get("attachments")
        .and_then(Value::as_array)
        .is_some_and(|items| !items.is_empty())
    {
        return Err(failure(
            "invalid_request",
            "A new thread accepts only pending attachment uploads.",
        ));
    }
    if arg_bool(a, "scratch")? == Some(true) {
        return Err(failure(
            "invalid_request",
            "Pass projectId: this server has no Scratch folder.",
        ));
    }
    let Some(project_id) = arg_str(a, "projectId")? else {
        return Err(failure(
            "invalid_request",
            "Pass projectId or scratch: this MCP client is not running inside a T3 thread.",
        ));
    };
    let strategy = a.get("workspaceStrategy").filter(|v| !v.is_null());
    if strategy.is_some_and(|s| s["type"] != "root") {
        return Err(failure(
            "invalid_request",
            "This server runs new threads in the project's checkout: omit workspaceStrategy or use {\"type\":\"root\"}.",
        ));
    }
    let message = arg_str(a, "message")?.map(str::to_owned);
    let requested_mode = arg_str(a, "runtimeMode")?;
    let requested_interaction = arg_str(a, "interactionMode")?;
    let (project, root) = {
        let store = host.control_state().store.read().await;
        let project = store
            .project_rows
            .get(project_id)
            .filter(|r| r["deletedAt"].is_null())
            .cloned()
            .ok_or_else(|| failure("invalid_request", "The project was not found."))?;
        let root = store
            .projects
            .get(project_id)
            .map(|p| p.path.clone())
            .unwrap_or_default();
        (project, root)
    };
    let selection = match a.get("modelSelection").filter(|v| !v.is_null()) {
        Some(selection) => {
            if !selection["model"].is_string() {
                return Err(invalid_arguments());
            }
            let mut selection = selection.clone();
            if selection["instanceId"].is_null() {
                selection["instanceId"] = selection
                    .get("provider")
                    .cloned()
                    .filter(Value::is_string)
                    .unwrap_or(json!("codex"));
            }
            selection.as_object_mut().map(|s| s.remove("provider"));
            selection
        }
        None => default_selection(&**host, &project).ok_or_else(|| {
            failure(
                "invalid_request",
                "Pass modelSelection: the project has no default model. orchestrator_capabilities lists providers and models.",
            )
        })?,
    };
    let runtime_mode = match requested_mode {
        Some(mode) => {
            let wanted = rank(mode).ok_or_else(invalid_arguments)?;
            if wanted > rank(&ceiling).unwrap_or(0) {
                return Err(failure(
                    "runtime_mode_escalation_denied",
                    format!("This client may start work in {ceiling} at most, not {mode}."),
                ));
            }
            mode.to_owned()
        }
        None => {
            let default = host.settings()["defaultRuntimeMode"]
                .as_str()
                .and_then(rank)
                .unwrap_or(3);
            RUNTIME_MODES[default.min(rank(&ceiling).unwrap_or(0))].to_owned()
        }
    };
    let interaction = match requested_interaction {
        Some("plan") => "plan",
        Some("default") if caller.plan_only => {
            return Err(failure(
                "interaction_mode_escalation_denied",
                "This client runs in plan mode, so it cannot start work in default mode.",
            ))
        }
        Some("default") => "default",
        Some(_) => return Err(invalid_arguments()),
        None => caller.interaction(),
    };
    let key = arg_str(a, "clientRequestId")?;
    let command_id = match key {
        Some(key) => format!(
            "mcp:launch:{}",
            hex::encode(<sha2::Sha256 as sha2::Digest>::digest(
                json!([host.environment_id(), key]).to_string().as_bytes()
            ))
        ),
        None => format!("mcp:launch:{}", crate::platform::uuid_v4().simple()),
    };
    let thread_id = command_id.clone();
    let replay = {
        let store = host.control_state().store.read().await;
        store.threads.get(&thread_id).map(|t| t.project_id.clone())
    };
    match replay {
        Some(existing) if existing != project_id => {
            return Err(failure(
                "invalid_request",
                "A launch retry must use the original project.",
            ))
        }
        Some(_) => {}
        None => {
            let mut create = json!({
                "type": "thread.create",
                "commandId": command_id,
                "threadId": thread_id,
                "projectId": project_id,
                "title": title,
                "modelSelection": selection,
                "runtimeMode": runtime_mode,
                "interactionMode": interaction,
                "branch": null,
                "worktreePath": if root.is_empty() { Value::Null } else { json!(root) },
                "createdBy": "agent",
                "creationSource": "mcp",
            });
            if let Some(from) = a.get("delegatedFrom").filter(|v| v.is_object()) {
                create["delegatedFrom"] = from.clone();
            }
            host.dispatch(create)
                .await
                .map_err(|e| failure("orchestration_error", dispatch_message(&e)))?;
            if let Some(text) = message.as_ref() {
                let dispatch = json!({
                    "type": "message.dispatch",
                    "commandId": format!("{command_id}:message"),
                    "threadId": thread_id,
                    "messageId": command_id,
                    "text": text,
                    "attachments": [],
                    "modelSelection": selection,
                    "dispatchMode": {"type": "start_immediately"},
                    "createdBy": "agent",
                    "creationSource": "mcp",
                });
                host.dispatch(dispatch)
                    .await
                    .map_err(|e| failure("orchestration_error", dispatch_message(&e)))?;
            }
        }
    }
    let store = host.control_state().store.read().await;
    let thread = find_thread(&store, &thread_id)?;
    let projection = thread.projection();
    let run = runs(&projection)
        .into_iter()
        .find(|run| run["userMessageId"] == command_id.as_str());
    let row = thread.stored_row();
    Ok(json!({
        "threadId": thread.id,
        "link": thread_link(&**host, caller, &thread.id, &thread.title),
        "projectId": thread.project_id,
        "modelSelection": row["modelSelection"],
        "runId": run.as_ref().map(|r| r["id"].clone()),
        "status": run.as_ref().map(|r| r["status"].clone()),
    }))
}

fn dispatch_message(error: &Value) -> String {
    error["message"]
        .as_str()
        .or(error["detail"].as_str())
        .or(error.as_str())
        .map(str::to_owned)
        .unwrap_or_else(|| error.to_string())
}

/// Refuses work on a thread whose mode is broader than this call may drive.
fn within_limits(caller: &Caller, row: &Value) -> Result<(), Refusal> {
    let ceiling = caller.writable()?;
    let mode = row["runtimeMode"].as_str().unwrap_or("full-access");
    if rank(mode).unwrap_or(3) > rank(ceiling).unwrap_or(0) {
        return Err(failure(
            "runtime_mode_escalation_denied",
            format!("The thread runs in {mode}, above this client's {ceiling}."),
        ));
    }
    if caller.plan_only && row["interactionMode"] != "plan" {
        return Err(failure(
            "interaction_mode_escalation_denied",
            "The thread runs in default mode, above this client's plan mode.",
        ));
    }
    Ok(())
}

async fn thread_send<S: McpHost>(host: &Arc<S>, caller: &Caller, a: &Value) -> Outcome {
    let id = required(a, "threadId")?.to_owned();
    let text = required(a, "message")?.to_owned();
    let mode = arg_str(a, "mode")?.unwrap_or("auto");
    if !matches!(mode, "auto" | "queue" | "steer" | "restart") {
        return Err(invalid_arguments());
    }
    if matches!(mode, "steer" | "restart") || arg_bool(a, "freshContext")? == Some(true) {
        return Err(failure(
            "invalid_request",
            "This server starts or queues messages only: use mode \"auto\" or \"queue\", without freshContext.",
        ));
    }
    let (busy, title, selection) = {
        let store = host.control_state().store.read().await;
        let thread = find_thread(&store, &id)?;
        let row = thread.stored_row();
        within_limits(caller, &row)?;
        let projection = thread.projection();
        (
            active(&runs(&projection)).is_some(),
            thread.title.clone(),
            row["modelSelection"].clone(),
        )
    };
    let key = arg_str(a, "clientRequestId")?;
    let message_id = match key {
        Some(key) => format!(
            "mcp:send:{}",
            hex::encode(<sha2::Sha256 as sha2::Digest>::digest(
                json!([host.environment_id(), id, key])
                    .to_string()
                    .as_bytes()
            ))
        ),
        None => format!("mcp:send:{}", crate::platform::uuid_v4().simple()),
    };
    let queued = busy;
    let already = {
        let store = host.control_state().store.read().await;
        find_thread(&store, &id)?
            .messages
            .iter()
            .any(|m| m.id == message_id)
    };
    if !already {
        let mut dispatch = json!({
            "type": "message.dispatch",
            "commandId": format!("{message_id}:command"),
            "threadId": id,
            "messageId": message_id,
            "text": text,
            "attachments": [],
            "dispatchMode": {"type": if queued { "queue_after_active" } else { "start_immediately" }},
            "createdBy": "agent",
            "creationSource": "mcp",
        });
        if selection.is_object() {
            dispatch["modelSelection"] = selection;
        }
        host.dispatch(dispatch).await.map_err(|e| {
            failure(
                "orchestration_error",
                format!("Unable to send to thread {id}: {}", dispatch_message(&e)),
            )
        })?;
    }
    let store = host.control_state().store.read().await;
    let thread = find_thread(&store, &id)?;
    let run = runs(&thread.projection())
        .into_iter()
        .find(|run| run["userMessageId"] == message_id.as_str())
        .ok_or_else(|| {
            failure(
                "orchestration_error",
                format!("Unable to send to thread {id}."),
            )
        })?;
    Ok(json!({
        "threadId": id,
        "link": thread_link(&**host, caller, &id, &title),
        "messageId": message_id,
        "runId": run["id"],
        "status": run["status"],
        "delivery": if queued { "queued" } else { "started" },
    }))
}

async fn thread_wait<S: McpHost>(host: &S, a: &Value) -> Outcome {
    let id = required(a, "threadId")?.to_owned();
    let run_id = arg_str(a, "runId")?.map(str::to_owned);
    let timeout = match a.get("timeoutMs") {
        None | Some(Value::Null) => DEFAULT_WAIT_TIMEOUT_MS,
        Some(v) => v.as_f64().ok_or_else(invalid_arguments)? as i64,
    }
    .clamp(1, MAX_WAIT_TIMEOUT_MS);
    let deadline = crate::platform::now_ms() + timeout;
    loop {
        let run = {
            let store = host.control_state().store.read().await;
            let thread = find_thread(&store, &id)?;
            let all = runs(&thread.projection());
            match run_id.as_deref() {
                Some(wanted) => Some(
                    all.iter()
                        .find(|run| run["id"] == wanted)
                        .cloned()
                        .ok_or_else(|| {
                            failure("invalid_request", "The run was not found in this thread.")
                        })?,
                ),
                None => latest(&all).cloned(),
            }
        };
        let Some(run) = run else {
            return Ok(json!({"threadId": id, "runId": null, "status": "idle", "timedOut": false}));
        };
        let status = run["status"].as_str().unwrap_or("failed").to_owned();
        let settled = TERMINAL.contains(&status.as_str()) || status == "waiting";
        let now = crate::platform::now_ms();
        if settled || now >= deadline {
            return Ok(json!({
                "threadId": id,
                "runId": run["id"],
                "status": status,
                "timedOut": !settled,
            }));
        }
        let pause = (deadline - now).clamp(1, WAIT_POLL_MS as i64) as u64;
        host.sleep(std::time::Duration::from_millis(pause)).await;
    }
}

async fn thread_interrupt<S: McpHost>(host: &Arc<S>, caller: &Caller, a: &Value) -> Outcome {
    let id = required(a, "threadId")?.to_owned();
    let run_id = arg_str(a, "runId")?.map(str::to_owned);
    let stop = arg_bool(a, "stop")?.unwrap_or(false);
    if a["reason"]
        .as_str()
        .is_some_and(|r| r.chars().count() > 2000)
    {
        return Err(invalid_arguments());
    }
    let target = {
        let store = host.control_state().store.read().await;
        let thread = find_thread(&store, &id)?;
        within_limits(caller, &thread.stored_row())?;
        let all = runs(&thread.projection());
        match run_id.as_deref() {
            Some(wanted) => Some(
                all.iter()
                    .find(|run| run["id"] == wanted)
                    .cloned()
                    .ok_or_else(|| {
                        failure("invalid_request", "The run was not found in this thread.")
                    })?,
            ),
            None => active(&all).cloned(),
        }
    };
    let Some(run) = target else {
        return Ok(json!({"threadId": id, "runId": null, "status": "no_active_run"}));
    };
    let status = run["status"].as_str().unwrap_or("failed");
    if TERMINAL.contains(&status) {
        return Ok(json!({"threadId": id, "runId": run["id"], "status": status}));
    }
    let command_id = format!("mcp:interrupt:{}", crate::platform::uuid_v4().simple());
    let command = if stop {
        json!({"type": "thread.stop", "commandId": command_id, "threadId": id})
    } else {
        json!({"type": "run.interrupt", "commandId": command_id, "threadId": id, "runId": run["id"]})
    };
    host.dispatch(command).await.map_err(|e| {
        failure(
            "orchestration_error",
            format!("Unable to interrupt thread {id}: {}", dispatch_message(&e)),
        )
    })?;
    Ok(json!({"threadId": id, "runId": run["id"], "status": "interrupt_requested"}))
}
