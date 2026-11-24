//! The thread, project and environment tools `/mcp` serves to a signed-in
//! client: an outside agent, or another T3 Code server through a peer link.
//!
//! Reads come from the control state's store. Changes are orchestration
//! commands, dispatched through the same path as the WebSocket RPC
//! `orchestration.dispatchCommand`.
use super::{McpAccess, McpHost};
use serde_json::{json, Value};
use std::sync::{Arc, LazyLock};

mod agent_tools;
mod arguments;
mod attachments;
mod delegation;
mod devices;
mod forge;
pub(crate) mod html;
mod issues;
mod projects;
mod pull_requests;
mod secrets;
mod session;
mod theme;
mod threads;

/// Tools whose arguments are checked against their advertised schema first,
/// with the reference's wording, and the fields the reference declares as
/// tagged unions.
const CHECKED: &[(&str, &[(&str, &str)])] = &[
    ("t3_thread_update", &[]),
    ("t3_thread_organize", &[]),
    ("t3_thread_configure", &[]),
    (
        "t3_thread_transfer",
        &[(
            "sourcePoint",
            r#"{ readonly "type": "latest_stable", ... } | { readonly "type": "run", ... } | { readonly "type": "checkpoint", ... }"#,
        )],
    ),
    ("t3_queue", &[]),
    ("t3_submit_output", &[]),
    ("t3_feedback", &[]),
    ("t3_pending_request", &[]),
    ("t3_workflow_stop", &[]),
    ("t3_workflow", &[]),
    ("t3_thread_search", &[]),
    ("t3_project_update", &[]),
    ("t3_project_delete", &[]),
    ("t3_project_clone", &[]),
    ("t3_environment_preferences_update", &[]),
    ("t3_worktree_list", &[]),
    ("t3_theme_read", &[]),
    ("send_to_user", &[]),
    ("t3_background_wait", &[]),
    ("tool_search", &[]),
    // Scheduled tasks (batch 6).
    ("list_scheduled_tasks", &[]),
    ("schedule_task", &[]),
    ("delete_scheduled_task", &[]),
    ("run_scheduled_task_now", &[]),
    // Delegation (batch 4b).
    ("delegate_task", &[]),
    ("task_status", &[]),
    ("task_cancel", &[]),
    ("create_threads", &[]),
    ("request_secret", &[]),
    ("t3_attachment_prepare_upload", &[]),
    ("t3_attachment_discard", &[]),
    ("t3_thread_send_attachments", &[]),
    ("html_preview", &[]),
    ("html_render", &[]),
    // Pull requests and issues (batch 7).
    ("link_pull_request", &[]),
    ("link_pull_requests", &[]),
    ("unlink_pull_request", &[]),
    ("unlink_pull_requests", &[]),
    ("list_thread_pull_requests", &[]),
    ("watch_pull_request", &[]),
    ("unwatch_pull_request", &[]),
    ("write_pull_request_guides", &[]),
    ("read_pull_request_guide", &[]),
    ("pull_request_list", &[]),
    ("pull_request_read", &[]),
    ("ci_logs", &[]),
    ("list_issues", &[]),
    ("read_issue", &[]),
    ("create_issue", &[]),
    ("comment_on_issue", &[]),
    ("set_issue_state", &[]),
    ("link_issue", &[]),
    ("unlink_issue", &[]),
    ("list_thread_issues", &[]),
    // Worktree and preview session controls (agent-tools audit).
    ("t3_worktree_status", &[]),
    ("t3_worktree_handoff", &[]),
    ("t3_preview_list", &[]),
    ("t3_preview_close", &[]),
    // Chromium preview automation.
    ("preview_evaluate", &[]),
    ("preview_set_appearance", &[]),
    // Chromium viewport sizing.
    ("preview_resize", &[]),
    ("preview_open", &[]),
    ("preview_navigate", &[]),
    ("preview_snapshot", &[]),
    ("preview_act", &[]),
    ("preview_wait_for", &[]),
    ("preview_status", &[]),
    ("preview_dialog", &[]),
    ("preview_recording_start", &[]),
    ("preview_recording_stop", &[]),
    // Device hub and sessions.
    ("device_list", &[]),
    ("device_open", &[]),
    ("device_screenshot", &[]),
    ("device_close", &[]),
];

/// The reference's agent tools that Rust serves. `tools.json` holds every tool
/// the pinned reference advertises; `scripts/update_agent_tools.py` regenerates
/// it and the checklist in docs/agent-tools.md, which reads this list.
const SERVED: &[&str] = &[
    "orchestrator_capabilities",
    "t3_thread_list",
    "t3_thread_read",
    "t3_thread_send",
    "t3_thread_wait",
    "t3_thread_interrupt",
    "t3_environment_links",
    "t3_environment_link",
    "t3_environment_unlink",
    "t3_thread_launch",
    "t3_project_list",
    "t3_project_read",
    "t3_project_create",
    "t3_environment_read",
    "t3_thread_update",
    "t3_thread_organize",
    "t3_thread_configure",
    "t3_thread_transfer",
    "t3_queue",
    "t3_submit_output",
    "t3_feedback",
    "t3_pending_request",
    "t3_workflow_stop",
    "t3_workflow",
    "t3_thread_search",
    "t3_project_update",
    "t3_project_delete",
    "t3_project_clone",
    "t3_environment_preferences_update",
    "t3_worktree_list",
    // Themes (batch 3b).
    "t3_theme_read",
    "t3_theme_set",
    // The calling thread's session (batch 4a).
    "send_to_user",
    "t3_background_wait",
    // Scheduled tasks (batch 6).
    "list_scheduled_tasks",
    "schedule_task",
    "delete_scheduled_task",
    "run_scheduled_task_now",
    // Delegation (batch 4b).
    "delegate_task",
    "task_status",
    "task_cancel",
    "create_threads",
    // Secrets the user gives the agent.
    "request_secret",
    // Pages an agent checks and shows.
    "html_preview",
    "html_render",
    // Attachments an agent uploads and sends.
    "t3_attachment_prepare_upload",
    "t3_attachment_discard",
    "t3_thread_send_attachments",
    // Pull requests and issues (batch 7).
    "link_pull_request",
    "link_pull_requests",
    "unlink_pull_request",
    "unlink_pull_requests",
    "list_thread_pull_requests",
    "watch_pull_request",
    "unwatch_pull_request",
    "write_pull_request_guides",
    "read_pull_request_guide",
    "pull_request_list",
    "pull_request_read",
    "ci_logs",
    "list_issues",
    "read_issue",
    "create_issue",
    "comment_on_issue",
    "set_issue_state",
    "link_issue",
    "unlink_issue",
    "list_thread_issues",
    // Worktree and preview session controls (agent-tools audit).
    "t3_worktree_status",
    "t3_worktree_handoff",
    "t3_preview_list",
    "t3_preview_close",
    // Chromium preview automation.
    "preview_evaluate",
    "preview_set_appearance",
    // Chromium viewport sizing.
    "preview_resize",
    // Chromium preview lifecycle, inspection and interaction.
    "preview_open",
    "preview_navigate",
    "preview_snapshot",
    "preview_act",
    "preview_wait_for",
    // Chromium preview status and dialogs.
    "preview_status",
    "preview_dialog",
    "preview_recording_start",
    "preview_recording_stop",
    // Device hub and sessions.
    "device_list",
    "device_open",
    "device_screenshot",
    "device_close",
];

/// The tools only the Linked environments beta lists (`hiddenWhileLinksOff`).
/// Their `environmentId` names a machine; it is never a target to act in.
const LINKS_ONLY: &str = "hiddenWhileLinksOff";

/// The reference's agent tools this server does not serve yet, which
/// instructions sent to an agent leave out (`crate::agent_instructions`).
pub(crate) fn unserved_tool_names() -> Vec<String> {
    let reference: Value =
        serde_json::from_str(include_str!("tools.json")).expect("MCP tool definitions");
    reference["tools"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|tool| tool["name"].as_str())
        .filter(|name| !SERVED.contains(name))
        .map(str::to_owned)
        .collect()
}

/// The reference's definitions of the tools served here, as `tools/list`
/// returns them, in the reference's order, each with whom it is listed to.
static SERVED_TOOLS: LazyLock<Vec<(Value, String)>> = LazyLock::new(|| {
    let reference: Value =
        serde_json::from_str(include_str!("tools.json")).expect("MCP tool definitions");
    let tools = reference["tools"]
        .as_array()
        .expect("MCP tool definitions")
        .iter()
        .filter_map(|tool| {
            let name = tool["name"].as_str().filter(|n| SERVED.contains(n))?;
            let listed = reference["listedTo"][name].as_str().unwrap_or("everyone");
            Some((tool.clone(), listed.to_owned()))
        })
        .collect::<Vec<_>>();
    assert_eq!(tools.len(), SERVED.len(), "SERVED names an unknown tool");
    tools
});

/// The served tools every caller is listed.
pub(super) static TOOLS: LazyLock<Value> = LazyLock::new(|| {
    Value::Array(
        SERVED_TOOLS
            .iter()
            .filter(|(_, listed)| listed == "everyone")
            .map(|(tool, _)| tool.clone())
            .collect(),
    )
});

/// `tool_search`, which only a provider session that defers its tools lists.
static TOOL_SEARCH: LazyLock<Value> = LazyLock::new(|| {
    let reference: Value =
        serde_json::from_str(include_str!("tools.json")).expect("MCP tool definitions");
    reference["toolSearch"].clone()
});

/// Whether `caller` may see a tool listed to `listed` (its `McpSchema.EnabledWhen`):
/// thread-only tools need a thread credential; links tools, the beta on.
/// This server issues no credential to a subagent, so `hiddenFromSubagents`
/// never hides a tool here.
fn visible_to<S: McpHost>(host: &S, granted: &McpAccess, listed: &str) -> bool {
    match listed {
        LINKS_ONLY => host.linked_environments_enabled(),
        "hiddenFromOutsideClients" => granted.thread.is_some(),
        // `hiddenWhileFeedbackOff`: listed while agent feedback is on.
        "hiddenWhileFeedbackOff" => host.settings()["agentFeedback"]["mode"]
            .as_str()
            .is_some_and(|mode| mode != "off"),
        "hiddenFromThreads" => granted.thread.is_none(),
        // `hiddenWithoutBackgroundWait`: a thread whose provider can hold for
        // background work (the reference's `supportsBackgroundWait`). Of the
        // providers this server runs turns for, that is Claude.
        "hiddenWithoutBackgroundWait" => granted.thread.as_ref().is_some_and(|scope| {
            let settings = host.settings();
            let driver = settings["providerInstances"][&scope.provider_instance_id]["driver"]
                .as_str()
                .unwrap_or(&scope.provider_instance_id)
                .to_owned();
            driver == crate::workspace::provider::CLAUDE_AGENT
        }),
        "hiddenWithoutForge" => host
            .control_state()
            .store
            .try_read()
            .map(|store| {
                store.project_rows.values().any(|row| {
                    let row = host.project_row(row);
                    matches!(
                        row["repositoryIdentity"]["provider"].as_str(),
                        Some(
                            "github"
                                | "bitbucket"
                                | "gitlab"
                                | "forgejo"
                                | "azure-devops"
                                | "gitcafe"
                                | "cursor-origin"
                        )
                    )
                })
            })
            .unwrap_or(false),
        _ => true,
    }
}

/// The tools `tools/list` offers `granted`, as the reference lists them: by
/// each tool's `EnabledWhen`, and for a provider session that sends
/// `t3-tool-exposure: deferred`, only the direct tools and those its
/// `tool_search` calls loaded.
pub(super) fn listed<S: McpHost>(host: &S, granted: &McpAccess, deferred: bool) -> Value {
    let session = granted
        .thread
        .as_ref()
        .filter(|_| deferred)
        .map(|scope| scope.provider_session_id.as_str());
    let mut tools: Vec<Value> = SERVED_TOOLS
        .iter()
        .filter(|(_, listed)| visible_to(host, granted, listed))
        .filter(|(tool, _)| {
            session.is_none_or(|session| {
                session::is_listed(session, tool["name"].as_str().unwrap_or_default())
            })
        })
        .map(|(tool, _)| sendable(tool))
        .collect();
    if session.is_some() {
        tools.push(sendable(&TOOL_SEARCH));
    }
    Value::Array(tools)
}

/// Refuses a tool a client from outside a thread is not offered, as if it did
/// not exist: the reference registers a thread-only tool for threads alone,
/// and `t3_background_wait` for threads whose provider can wait.
pub(super) fn callable<S: McpHost>(host: &S, caller: &Caller, name: &str) -> Result<(), Refusal> {
    let thread_only = name == "tool_search"
        || SERVED_TOOLS
            .iter()
            .any(|(tool, listed)| tool["name"] == name && listed == "hiddenFromOutsideClients");
    let hidden = SERVED_TOOLS.iter().any(|(tool, listed)| {
        tool["name"] == name
            && matches!(
                listed.as_str(),
                "hiddenWithoutBackgroundWait" | "hiddenWhileFeedbackOff" | "hiddenFromThreads"
            )
            && !visible_to(host, &caller.granted, listed)
    });
    if (thread_only && caller.granted.thread.is_none()) || hidden {
        return Err(Refusal::Rpc(-32602, format!("Tool '{name}' not found")));
    }
    Ok(())
}

fn tool_definition(name: &str) -> Option<&'static Value> {
    if name == "tool_search" {
        return Some(&TOOL_SEARCH);
    }
    SERVED_TOOLS
        .iter()
        .map(|(tool, _)| tool)
        .find(|t| t["name"] == name)
}

/// A served tool as `tools/list` sends it: the MCP protocol layer the
/// reference uses (effect's `isToolOutputSchema`) leaves out an output schema
/// whose root is not a plain object, such as a union of answers.
/// `update_agent_tools.py` already leaves such schemas out of `tools.json`.
fn sendable(tool: &Value) -> Value {
    let mut tool = tool.clone();
    let output = &tool["outputSchema"];
    let keeps = output["type"] == "object"
        && output.get("properties").is_none_or(|p| {
            p.as_object()
                .is_some_and(|p| p.values().all(Value::is_object))
        })
        && output
            .get("required")
            .is_none_or(|r| r.as_array().is_some_and(|r| r.iter().all(Value::is_string)));
    if !keeps {
        if let Some(object) = tool.as_object_mut() {
            object.remove("outputSchema");
        }
    }
    tool
}

fn links_only(name: &str) -> bool {
    SERVED_TOOLS
        .iter()
        .any(|(tool, listed)| tool["name"] == name && listed == LINKS_ONLY)
}

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
    /// A domain error rendered as its message by the reference toolkit.
    Text(String),
    /// A JSON-RPC error: unknown tool or malformed arguments.
    Rpc(i32, String),
    /// A tool failure the agent reads (`OrchestratorMcpFailure`).
    Failure(&'static str, String),
}
type Outcome = Result<Value, Refusal>;

pub(super) fn text_errors(name: &str) -> bool {
    name.contains("pull_request")
        || matches!(
            name,
            "list_issues"
                | "read_issue"
                | "create_issue"
                | "comment_on_issue"
                | "set_issue_state"
                | "link_issue"
                | "unlink_issue"
                | "list_thread_issues"
        )
}

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
    /// The thread a provider session's credential acts as, read for this call.
    pub thread: Option<ThreadCaller>,
    /// The origin this `/mcp` request reached (`McpInvocationContext.origin`).
    pub origin: Option<String>,
}

/// The calling thread, as a provider session's tools see it.
#[derive(Clone, Debug)]
pub(super) struct ThreadCaller {
    pub id: String,
    pub title: String,
    pub project_id: String,
    pub runtime_mode: String,
    pub interaction_mode: String,
    pub provider_instance_id: String,
    pub model: String,
    /// The thread's model options, as `[{id, value}]`.
    pub model_options: Value,
}

/// The thread `scope` acts as, if it still exists.
pub(super) async fn thread_caller<S: McpHost>(
    host: &S,
    scope: &super::ThreadScope,
) -> Option<ThreadCaller> {
    let store = host.control_state().store.read().await;
    let thread = find_thread(&store, &scope.thread_id).ok()?;
    let row = thread.shell();
    let summary_model = row["modelSelection"]["model"]
        .as_str()
        .unwrap_or_default()
        .to_owned();
    Some(ThreadCaller {
        id: thread.id.clone(),
        title: thread.title.clone(),
        project_id: thread.project_id.clone(),
        runtime_mode: row["runtimeMode"]
            .as_str()
            .unwrap_or("full-access")
            .to_owned(),
        interaction_mode: row["interactionMode"]
            .as_str()
            .unwrap_or("default")
            .to_owned(),
        provider_instance_id: row["providerInstanceId"]
            .as_str()
            .or(row["modelSelection"]["instanceId"].as_str())
            .unwrap_or(&scope.provider_instance_id)
            .to_owned(),
        model: summary_model,
        model_options: match &row["modelSelection"]["options"] {
            Value::Array(options) => Value::Array(options.clone()),
            _ => json!([]),
        },
    })
}

/// Whether `name` acts as its caller, so a thread credential whose thread is
/// gone cannot use it (`readCaller`).
pub(super) fn needs_caller(name: &str) -> bool {
    !matches!(
        name,
        "t3_project_list" | "t3_project_read" | "t3_environment_read"
    )
}

pub(super) fn calling_thread_missing() -> Refusal {
    failure("thread_not_found", "The calling thread was not found.")
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
            thread: None,
            origin: None,
        }
    }
    /// The broadest runtime mode this call may start or drive work in, or
    /// `None` when it may only read.
    fn ceiling(&self) -> Option<&str> {
        if let Some(thread) = &self.thread {
            return Some(RUNTIME_MODES[rank(&thread.runtime_mode).unwrap_or(0)]);
        }
        let granted = rank(&self.granted.access)?;
        let limited = self
            .runtime_limit
            .as_deref()
            .and_then(rank)
            .unwrap_or(granted);
        Some(RUNTIME_MODES[granted.min(limited)])
    }
    fn interaction(&self) -> &'static str {
        if let Some(thread) = &self.thread {
            return if thread.interaction_mode == "plan" {
                "plan"
            } else {
                "default"
            };
        }
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
    if caller.granted.peer_link || caller.thread.is_some() {
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
        "pullRequestCount": row["pullRequests"].as_array().map_or(0, Vec::len),
        "openPullRequestCount": row["pullRequests"].as_array().map_or(0, |prs| {
            prs.iter().filter(|pr| pr["state"] == "open").count()
        }),
        "settled": thread.settled,
        "settledAt": row["settledAt"],
        "snoozed": snoozed(&row),
        "snoozedUntil": row["snoozedUntil"],
        "pinned": thread.pinned,
        "blockedOnThreadWait": false,
        // This server runs no background tasks for a thread.
        "pendingBackgroundTasks": [],
        "parentThreadId": row["lineage"]["parentThreadId"],
        "relationshipToParent": row["lineage"]["relationshipToParent"],
        "itemCount": projection["visibleTurnItems"].as_array().map_or(0, Vec::len),
        "createdAt": row["createdAt"],
        "updatedAt": row["updatedAt"],
    })
}

/// `threadNotFound` for a thread a read names: where it is no longer, and
/// where else to look when this caller may use linked environments.
fn no_longer_available<S: McpHost>(host: &S, caller: &Caller, id: &str) -> Refusal {
    let links = if host.linked_environments_enabled() && !caller.granted.peer_link {
        " Thread ids belong to one environment: for a thread on a linked one, find it with `t3_environment_links` and pass its `environmentId` to a tool that takes one."
    } else {
        ""
    };
    failure(
        "thread_not_found",
        format!("Thread {id} is no longer available.{links}"),
    )
}

/// A project as the project tools return it: the reference leaves
/// `hasCustomTitle` out until a title is chosen, and shows the actions saved
/// in Settings (#39676).
fn project_out<S: McpHost>(host: &S, row: &Value) -> Value {
    let mut out = host.project_row(row);
    if out.is_object() {
        out["scripts"] = project_scripts(&host.settings(), &out);
    }
    if out["hasCustomTitle"] == false {
        if let Some(object) = out.as_object_mut() {
            object.remove("hasCustomTitle");
        }
    }
    out
}

/// `resolveProjectScripts`: the actions new threads of `project` use.
fn project_scripts(settings: &Value, project: &Value) -> Value {
    let id = project["id"].as_str().unwrap_or_default();
    let defaults = || match &settings["defaultProjectScripts"] {
        scripts @ Value::Array(_) => scripts.clone(),
        _ => json!([]),
    };
    let overridden = &settings["projectSettingsOverrides"][id]["defaultProjectScripts"];
    if overridden.is_array() {
        return overridden.clone();
    }
    if settings["projectSettingsFolded"] == true {
        return defaults();
    }
    match settings["projectScriptOverrides"].get(id) {
        Some(Value::Null) => defaults(),
        Some(legacy @ Value::Array(_)) => legacy.clone(),
        _ if project["scripts"].as_array().is_some_and(|s| !s.is_empty()) => {
            project["scripts"].clone()
        }
        _ => defaults(),
    }
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
    if let Some((_, unions)) = CHECKED.iter().find(|(tool, _)| *tool == name) {
        match arguments::check(name, args, unions) {
            // An image tool answers a parameter failure as its own failure.
            Err(Refusal::Rpc(_, message)) if name == "device_screenshot" => {
                return Ok(html::image_failure_in("AiError", "screenshot", message))
            }
            Err(Refusal::Rpc(_, message)) if name == "html_preview" => {
                return Ok(html::image_failure(
                    "AiError",
                    format!("Toolkit.html_preview.handle: {message}"),
                ))
            }
            other => other?,
        }
    }
    match name {
        "t3_project_list" => project_list(&**host, args).await,
        "t3_project_read" => project_read(&**host, args).await,
        "t3_project_create" => project_create(host, caller, args).await,
        "t3_environment_read" => Ok(environment_read(&**host)),
        "orchestrator_capabilities" => {
            // #39247: the caller's folder is scanned first, so a provider's
            // project catalog is current when the agent picks a model.
            if let Some(cwd) = caller_workspace(&**host, caller).await {
                host.scan_workspace(cwd).await;
            }
            Ok(capabilities(&**host, caller, args))
        }
        "t3_thread_list" => thread_list(&**host, caller, args).await,
        "t3_thread_read" => thread_read(&**host, caller, args).await,
        "t3_thread_launch" => thread_launch(host, caller, args).await,
        "t3_thread_send" => thread_send(host, caller, args).await,
        "t3_thread_wait" => thread_wait(&**host, args).await,
        "t3_thread_interrupt" => thread_interrupt(host, caller, args).await,
        "t3_thread_update" => threads::update(host, caller, args).await,
        "t3_thread_organize" => threads::organize(host, caller, args).await,
        "t3_thread_configure" => threads::configure(host, caller, args).await,
        "t3_thread_transfer" => threads::transfer(host, caller, args).await,
        "t3_queue" => threads::queue(host, caller, args).await,
        "t3_submit_output" => threads::submit_output(host, caller, args).await,
        "t3_feedback" => threads::feedback(host, caller, args).await,
        "t3_pending_request" => threads::pending_request(host, caller, args).await,
        "t3_workflow_stop" => threads::workflow_stop(host, caller, args).await,
        "t3_workflow" => workflow(host, caller, args).await,
        "t3_environment_links" => environment_links(host, caller).await,
        "t3_environment_link" => environment_link(host, caller, args).await,
        "t3_environment_unlink" => environment_unlink(host, caller, args).await,
        "t3_thread_search" => threads::search(host, caller, args).await,
        "t3_project_update" => projects::update(host, caller, args).await,
        "t3_project_delete" => projects::delete(host, caller, args).await,
        "t3_project_clone" => projects::clone(host, caller, args).await,
        "t3_environment_preferences_update" => {
            projects::preferences_update(host, caller, args).await
        }
        "t3_worktree_list" => projects::worktree_list(host, caller, args).await,
        "t3_theme_read" => theme::read(host, args).await,
        "t3_theme_set" => theme::set(host, caller, args).await,
        "tool_search" => session::tool_search(caller, args),
        "send_to_user" => session::send_to_user(&**host, caller, args).await,
        "t3_background_wait" => session::background_wait(&**host, caller, args).await,

        "list_scheduled_tasks" => list_scheduled_tasks(&**host, caller, args).await,
        "schedule_task" => schedule_task(host, caller, args).await,
        "delete_scheduled_task" => delete_scheduled_task(&**host, caller, args).await,
        "run_scheduled_task_now" => run_scheduled_task_now(host, caller, args).await,

        "delegate_task" => delegation::delegate_task(host, caller, args).await,
        "task_status" => delegation::task_status(host, caller, args).await,
        "task_cancel" => delegation::task_cancel(host, caller, args).await,
        "create_threads" => delegation::create_threads(host, caller, args).await,
        "request_secret" => secrets::request_secret(host, caller, args).await,
        "t3_attachment_prepare_upload" => attachments::prepare_upload(host, caller, args).await,
        "t3_attachment_discard" => attachments::discard(host, caller, args).await,
        "t3_thread_send_attachments" => attachments::send(host, caller, args).await,
        "html_preview" => html::preview(host, caller, args).await,
        "html_render" => html::render(host, caller, args).await,
        "link_pull_request"
        | "link_pull_requests"
        | "unlink_pull_request"
        | "unlink_pull_requests"
        | "list_thread_pull_requests"
        | "watch_pull_request"
        | "unwatch_pull_request"
        | "write_pull_request_guides"
        | "read_pull_request_guide"
        | "pull_request_list"
        | "pull_request_read"
        | "ci_logs" => pull_requests::call(host, caller, name, args).await,
        "list_issues" | "read_issue" | "create_issue" | "comment_on_issue" | "set_issue_state"
        | "link_issue" | "unlink_issue" | "list_thread_issues" => {
            issues::call(host, caller, name, args).await
        }
        "t3_worktree_status" => agent_tools::worktree_status(host, caller).await,
        "t3_worktree_handoff" => agent_tools::worktree_handoff(host, caller, args).await,
        "preview_evaluate" => agent_tools::preview_automation(host, caller, "evaluate", args).await,
        "preview_resize" => agent_tools::preview_automation(host, caller, "resize", args).await,
        "preview_open" => agent_tools::preview_automation(host, caller, "open", args).await,
        "preview_navigate" => agent_tools::preview_automation(host, caller, "navigate", args).await,
        "preview_snapshot" => agent_tools::preview_automation(host, caller, "snapshot", args).await,
        "preview_status" => agent_tools::preview_automation(host, caller, "status", args).await,
        "preview_dialog" => agent_tools::preview_automation(host, caller, "dialog", args).await,
        "preview_recording_start" => {
            agent_tools::preview_automation(host, caller, "recordingStart", args).await
        }
        "preview_recording_stop" => agent_tools::preview_recording_stop(host, caller, args).await,
        "preview_act" => agent_tools::preview_automation(host, caller, "act", args).await,
        "preview_wait_for" => agent_tools::preview_automation(host, caller, "waitFor", args).await,
        "preview_set_appearance" => {
            agent_tools::preview_automation(host, caller, "setColorScheme", args).await
        }
        "t3_preview_list" => agent_tools::preview_list(host, caller, args).await,
        "t3_preview_close" => agent_tools::preview_close(host, caller, args).await,
        "device_list" => devices::list(host, caller, args).await,
        "device_close" => devices::close(host, caller, args).await,
        "device_open" => devices::open(host, caller, args).await,
        "device_screenshot" => devices::screenshot(host, caller, args).await,
        _ => Err(Refusal::Rpc(-32602, format!("Tool '{name}' not found"))),
    }
}

async fn list_scheduled_tasks<S: McpHost>(host: &S, caller: &Caller, args: &Value) -> Outcome {
    let project = args["projectId"].as_str().or_else(|| {
        caller
            .thread
            .as_ref()
            .map(|thread| thread.project_id.as_str())
    });
    let id = args.get("scheduledTaskId").and_then(Value::as_str);
    let tasks = host
        .control_state()
        .scheduled_task_list()
        .await
        .map_err(|_| {
            failure(
                "orchestration_error",
                "Could not list scheduled tasks: Could not list schedule tasks.",
            )
        })?;
    let summaries: Vec<_> = tasks
        .iter()
        .filter(|task| project.is_none_or(|p| task["projectId"] == p))
        .filter(|task| id.is_none_or(|id| task["id"] == id))
        .map(scheduled_task_summary)
        .collect();
    Ok(json!({"tasks": summaries}))
}

fn scheduled_task_summary(task: &Value) -> Value {
    let mut summary = json!({
        "scheduledTaskId": task["id"], "title": task["title"], "prompt": task["prompt"],
        "enabled": task["enabled"], "projectId": task["projectId"], "boundThreadId": task["threadId"],
        "freshSession": task["threadMode"] == "fresh_session", "schedule": task["schedule"],
        "nextRunAt": task["nextRunAt"], "lastRunStatus": task["lastRunStatus"]
    });
    if let Some(has_secret) = task["webhook"]["hasSecret"].as_bool() {
        summary["webhookSignature"] = json!(if has_secret { "set" } else { "none" });
    }
    if let Some(message) = task.get("blockedByPolicy") {
        summary["blockedByPolicy"] = message.clone();
    }
    summary
}

fn scheduled_task_next_run(schedule: &Value, now_ms: i64) -> Option<String> {
    match schedule["type"].as_str() {
        Some("interval") => schedule["everyMs"]
            .as_i64()
            .map(|every| crate::auth_service::iso(now_ms.saturating_add(every.max(60_000)))),
        Some("once") => schedule["runAt"].as_str().and_then(|run_at| {
            time::OffsetDateTime::parse(run_at, &time::format_description::well_known::Rfc3339)
                .ok()
                .filter(|at| at.unix_timestamp_nanos() / 1_000_000 > now_ms as i128)
                .map(|_| run_at.to_owned())
        }),
        Some("fixed_time") => {
            let time = schedule["timeOfDay"].as_str()?.trim();
            let (hour, minute) = time.split_once(':')?;
            let hour: u8 = hour.parse().ok()?;
            let minute: u8 = minute.parse().ok()?;
            if hour > 23 || minute > 59 {
                return None;
            }
            let now =
                time::OffsetDateTime::from_unix_timestamp_nanos(now_ms as i128 * 1_000_000).ok()?;
            let weekdays = schedule["weekdays"].as_array();
            for offset in 0..=7 {
                let date = now.date().checked_add(time::Duration::days(offset))?;
                let candidate =
                    time::PrimitiveDateTime::new(date, time::Time::from_hms(hour, minute, 0).ok()?)
                        .assume_utc();
                if candidate <= now {
                    continue;
                }
                let weekday = candidate.weekday().number_days_from_sunday() as i64;
                if weekdays
                    .is_some_and(|days| !days.iter().any(|day| day.as_i64() == Some(weekday)))
                {
                    continue;
                }
                return Some(crate::auth_service::iso(
                    candidate.unix_timestamp_nanos() as i64 / 1_000_000,
                ));
            }
            None
        }
        _ => None,
    }
}

/// `t3_workflow` (`McpToolAccess.either`): its reads check nothing, its
/// changes (and `hooks`, whose paths hold their secrets) the writer, as
/// `writes` does. The workflow service answers (`src/workflows.rs`).
async fn workflow<S: McpHost>(host: &Arc<S>, caller: &Caller, args: &Value) -> Outcome {
    const READS: [&str; 4] = ["list", "read", "questions", "store"];
    if !args["action"]
        .as_str()
        .is_some_and(|action| READS.contains(&action))
    {
        threads::writing_caller(&**host, caller).await?;
        assert_not_linked(caller, "change this environment's scheduled tasks")?;
    }
    host.workflow_tool(args.clone())
        .await
        .map_err(|(code, message)| match code {
            "invalid_arguments" => Refusal::Rpc(
                -32602,
                format!("Invalid parameters for tool 't3_workflow': {message}"),
            ),
            _ => failure(code, message),
        })
}

/// `OrchestratorMcpService.scheduleTask`: a new task, saved as the client's
/// `scheduledTasks.upsert` saves one (so a webhook task gets its URL token and
/// signing secret, a `secretRef` spent for it), bound to the caller's thread in
/// its own project, at the caller's modes.
async fn create_scheduled_task<S: McpHost>(
    host: &Arc<S>,
    caller: &Caller,
    args: &Value,
) -> Outcome {
    let (Some(prompt), Some(schedule)) = (
        args["prompt"].as_str(),
        args.get("schedule").filter(|value| value.is_object()),
    ) else {
        return Err(failure(
            "invalid_request",
            "A new scheduled task needs `prompt` and `schedule`.",
        ));
    };
    let project = args["projectId"]
        .as_str()
        .or_else(|| {
            caller
                .thread
                .as_ref()
                .map(|thread| thread.project_id.as_str())
        })
        .ok_or_else(|| {
            failure(
                "invalid_request",
                "projectId is required outside a project thread.",
            )
        })?;
    let own_project = caller
        .thread
        .as_ref()
        .is_some_and(|thread| thread.project_id == project);
    let bind = args["bindToCurrentThread"].as_bool().unwrap_or(own_project);
    if bind && !own_project {
        return Err(failure(
            "invalid_request",
            if caller.thread.is_none() {
                "bindToCurrentThread needs an agent running inside a T3 thread."
            } else {
                "bindToCurrentThread binds to this thread, which belongs to a different project."
            },
        ));
    }
    // The first line, at most 80 UTF-16 units, never ending on a lone high surrogate.
    let mut units: Vec<u16> = prompt
        .lines()
        .next()
        .unwrap_or_default()
        .trim()
        .encode_utf16()
        .take(80)
        .collect();
    if units
        .last()
        .is_some_and(|unit| (0xD800..0xDC00).contains(unit))
    {
        units.pop();
    }
    let derived = String::from_utf16_lossy(&units);
    let title = args["title"]
        .as_str()
        .map(str::to_owned)
        .unwrap_or(if derived.is_empty() {
            "Scheduled task".to_owned()
        } else {
            derived
        });
    let (runtime, interaction) = caller.limits();
    let thread_id = bind
        .then(|| caller.thread.as_ref().map(|thread| thread.id.clone()))
        .flatten();
    let mut input = json!({
        "title": title,
        "prompt": prompt,
        "enabled": args["enabled"].as_bool().unwrap_or(true),
        "schedule": schedule,
        "projectId": project,
        "threadId": thread_id,
        "threadMode": if args["freshSession"] == true { "fresh_session" } else if bind { "continue" } else { "new_thread" },
        "workspaceStrategy": {"type": "root"},
        "modelSelection": caller.thread.as_ref().map(|thread| json!({"instanceId": thread.provider_instance_id, "model": thread.model})).unwrap_or(json!({"instanceId": "codex", "model": ""})),
        "runtimeMode": runtime,
        "interactionMode": interaction,
        "createdBy": "agent",
        "creationSource": "mcp",
    });
    if let Some(key) = args["clientRequestId"].as_str() {
        input["commandId"] = json!(delegation::stable_command_id(caller, "schedule-task", key));
    }
    let task = host.scheduled_task_save(input).await.map_err(|message| {
        failure(
            "orchestration_error",
            format!("Could not schedule task: {message}"),
        )
    })?;
    Ok(scheduled_task_summary(&task))
}

async fn schedule_task<S: McpHost>(host: &Arc<S>, caller: &Caller, args: &Value) -> Outcome {
    caller.refuse_read_only()?;
    if args.get("scheduledTaskId").is_none() {
        return create_scheduled_task(host, caller, args).await;
    }
    let project = args["projectId"]
        .as_str()
        .or_else(|| {
            caller
                .thread
                .as_ref()
                .map(|thread| thread.project_id.as_str())
        })
        .ok_or_else(|| {
            failure(
                "invalid_request",
                "projectId is required outside a project thread.",
            )
        })?;
    let prompt = args["prompt"]
        .as_str()
        .filter(|text| !text.trim().is_empty())
        .ok_or_else(invalid_arguments)?;
    let schedule = args
        .get("schedule")
        .filter(|value| value.is_object())
        .cloned()
        .ok_or_else(invalid_arguments)?;
    let now = crate::auth_service::now();
    let id = args["scheduledTaskId"]
        .as_str()
        .map(str::to_owned)
        .unwrap_or_else(|| format!("scheduled-task:{}", uuid::Uuid::new_v4()));
    let title = args["title"]
        .as_str()
        .filter(|text| !text.trim().is_empty())
        .unwrap_or_else(|| prompt.lines().next().unwrap_or("Scheduled task"));
    let enabled = args["enabled"].as_bool().unwrap_or(true);
    let thread_id = args["bindToCurrentThread"]
        .as_bool()
        .unwrap_or(
            caller
                .thread
                .as_ref()
                .is_some_and(|thread| thread.project_id == project),
        )
        .then(|| caller.thread.as_ref().map(|thread| thread.id.clone()))
        .flatten();
    let next = scheduled_task_next_run(&schedule, now);
    let task = json!({"id":id,"title":title,"prompt":prompt,"enabled":enabled,"schedule":schedule,"projectId":project,"threadId":thread_id,
        "threadMode":if args["freshSession"] == true {"fresh_session"} else if thread_id.is_some() {"continue"} else {"new_thread"},
        "workspaceStrategy":{"type":"root"},"modelSelection":caller.thread.as_ref().map(|thread| json!({"instanceId":thread.provider_instance_id,"model":thread.model})).unwrap_or(json!({"instanceId":"codex","model":""})),
        "runtimeMode":caller.thread.as_ref().map(|thread| thread.runtime_mode.clone()).unwrap_or_else(|| "approval-required".into()),"interactionMode":"default",
        "createdBy":"agent","creationSource":"mcp","createdAt":crate::auth_service::iso(now),"updatedAt":crate::auth_service::iso(now),
        "nextRunAt":if enabled {next} else {None::<String>},"lastRunAt":null,"lastRunStatus":"never","lastRunError":null,"runCount":0});
    host.control_state()
        .scheduled_task_upsert(task.clone())
        .await
        .map_err(|_| failure("orchestration_error", "Could not schedule task."))?;
    Ok(scheduled_task_summary(&task))
}

async fn delete_scheduled_task<S: McpHost>(host: &S, caller: &Caller, args: &Value) -> Outcome {
    caller.refuse_read_only()?;
    let id = args["scheduledTaskId"]
        .as_str()
        .ok_or_else(invalid_arguments)?;
    if !host
        .control_state()
        .scheduled_task_delete(id)
        .await
        .map_err(|_| failure("orchestration_error", "Could not delete scheduled task."))?
    {
        return Err(failure("not_found", "Schedule task not found."));
    }
    Ok(json!({"scheduledTaskId":id,"deleted":true}))
}

async fn run_scheduled_task_now<S: McpHost>(
    host: &Arc<S>,
    caller: &Caller,
    args: &Value,
) -> Outcome {
    caller.refuse_read_only()?;
    let id = args["taskId"].as_str().ok_or_else(invalid_arguments)?;
    let task = host
        .control_state()
        .scheduled_task_list()
        .await
        .map_err(|_| failure("orchestration_error", "Could not run scheduled task."))?
        .into_iter()
        .find(|task| task["id"] == id)
        .ok_or_else(|| failure("not_found", "Schedule task not found."))?;
    if task["schedule"]["type"] == "webhook" {
        return Err(failure(
            "invalid_request",
            "Webhook tasks run when their URL receives a request.",
        ));
    }
    let task = host
        .run_scheduled_task_now(id.to_owned())
        .await
        .map_err(|error| failure("orchestration_error", dispatch_message(&error)))?;
    let mut result = json!({
        "taskId":id,
        "threadId":task["threadId"],
        "lastRunStatus":task["lastRunStatus"],
        "lastRunError":task["lastRunError"],
        "runCount":task["runCount"],
        "nextRunAt":task["nextRunAt"]
    });
    if let Some(thread_id) = task["threadId"].as_str() {
        result["link"] = thread_link(
            &**host,
            caller,
            thread_id,
            task["title"].as_str().unwrap_or("Scheduled task"),
        )
        .into();
    }
    Ok(result)
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
        "projects": rows.iter().skip(cursor).take(limit).map(|row| project_out(host, row)).collect::<Vec<_>>(),
        "nextCursor": (end < rows.len()).then_some(end)
    }))
}

async fn project_read<S: McpHost>(host: &S, a: &Value) -> Outcome {
    let store = host.control_state().store.read().await;
    a["projectId"]
        .as_str()
        .and_then(|id| store.project_rows.get(id))
        .filter(|r| r["deletedAt"].is_null())
        .map(|row| project_out(host, row))
        .ok_or_else(|| failure("invalid_request", "The project was not found."))
}

async fn project_create<S: McpHost>(host: &Arc<S>, caller: &Caller, a: &Value) -> Outcome {
    // Linked work may bring the repository it works in.
    writes_environment(caller, None)?;
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
            .map(|row| project_out(&**host, row))
            .ok_or_else(|| failure("orchestration_error", "The project was not recorded."))?;
        if let Some(error) = created.get("commitError").filter(|e| e.is_string()) {
            project["commitError"] = error.clone();
        }
        return Ok(project);
    }
    match host.mutate_project(command).await {
        Ok(row) => Ok(project_out(&**host, &row)),
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
    json!({
        "environmentId": descriptor["environmentId"],
        "label": descriptor["label"],
        "serverVersion": descriptor["serverVersion"],
        "platform": descriptor["platform"],
        "preferences": projects::preferences(&host.settings()),
    })
}

/// Providers as `orchestrator_capabilities` lists them.
/// How many of a provider's current models a catalogue lists (`LISTED_MODEL_LIMIT`).
const LISTED_MODEL_LIMIT: usize = 8;
/// Drivers whose whole current catalogue is listed (`FULL_CATALOG_DRIVERS`).
const FULL_CATALOG_DRIVERS: [&str; 4] = ["codex", "claudeAgent", "muse", "grok"];

/// `listCapabilityModels`: the models a provider's catalogue entry lists, and
/// how many it leaves out.
fn capability_models(
    provider: &Value,
    preferences: &Value,
    search_terms: &[String],
    pinned: impl Fn(&Value) -> bool,
) -> (Vec<Value>, usize) {
    let order: Vec<&str> = preferences["modelOrder"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .collect();
    let hidden: Vec<&str> = preferences["hiddenModels"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .collect();
    let rank = |model: &Value| {
        let slug = model["slug"].as_str().unwrap_or_default();
        order.iter().position(|s| *s == slug).unwrap_or(order.len())
    };
    let mut visible: Vec<Value> = provider["models"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|m| {
            m["isCustom"] == true || !hidden.contains(&m["slug"].as_str().unwrap_or_default())
        })
        .cloned()
        .collect();
    visible.sort_by_key(|m| rank(m));
    let provider_text = format!(
        "{} {} {}",
        provider["instanceId"].as_str().unwrap_or_default(),
        provider["driver"].as_str().unwrap_or_default(),
        provider["displayName"].as_str().unwrap_or_default()
    )
    .to_lowercase();
    // Whole ids like "codex-work" and their parts both name the provider.
    let provider_words: Vec<&str> = provider_text
        .split_whitespace()
        .chain(provider_text.split(|c: char| !c.is_alphanumeric()))
        .filter(|w| !w.is_empty())
        .collect();
    let model_text = |m: &Value| {
        format!(
            "{} {} {}",
            m["slug"].as_str().unwrap_or_default(),
            m["name"].as_str().unwrap_or_default(),
            m["subProvider"].as_str().unwrap_or_default()
        )
        .to_lowercase()
    };
    // Part of a provider word names the provider unless a model matches it.
    let model_terms: Vec<&String> = search_terms
        .iter()
        .filter(|term| {
            !provider_words.contains(&term.as_str())
                && (!provider_text.contains(term.as_str())
                    || visible
                        .iter()
                        .any(|m| model_text(m).contains(term.as_str())))
        })
        .collect();
    if !model_terms.is_empty() {
        let matches: Vec<Value> = visible
            .iter()
            .filter(|m| {
                let text = model_text(m);
                model_terms.iter().all(|term| text.contains(term.as_str()))
            })
            .cloned()
            .collect();
        let listed: Vec<Value> = matches.iter().take(LISTED_MODEL_LIMIT).cloned().collect();
        let omitted = matches.len() - listed.len();
        return (listed, omitted);
    }
    let current: Vec<&Value> = visible.iter().filter(|m| m["isLegacy"] != true).collect();
    let full = FULL_CATALOG_DRIVERS.contains(&provider["driver"].as_str().unwrap_or_default());
    let shown: Vec<&Value> = if full {
        current
    } else {
        current.into_iter().take(LISTED_MODEL_LIMIT).collect()
    };
    let listed: Vec<Value> = visible
        .iter()
        .filter(|m| pinned(m) || shown.iter().any(|s| s["slug"] == m["slug"]))
        .cloned()
        .collect();
    let omitted = visible.len() - listed.len();
    (listed, omitted)
}

/// `capabilityModelOption`: an option a model takes, its values and default.
fn capability_option(descriptor: &Value) -> Value {
    let boolean = descriptor["type"] == "boolean";
    let values = if boolean {
        json!([false, true])
    } else {
        json!(descriptor["options"]
            .as_array()
            .into_iter()
            .flatten()
            .map(|o| o["id"].clone())
            .collect::<Vec<_>>())
    };
    let mut option = json!({"id": descriptor["id"], "values": values});
    // `getProviderOptionCurrentValue` with no selection: a boolean's current
    // value, else a non-empty current value, else the default choice.
    let current = &descriptor["currentValue"];
    let default = if boolean || current.as_str().is_some_and(|v| !v.is_empty()) {
        current.clone()
    } else {
        descriptor["options"]
            .as_array()
            .into_iter()
            .flatten()
            .find(|o| o["isDefault"] == true)
            .map_or(Value::Null, |o| o["id"].clone())
    };
    if !default.is_null() {
        option["default"] = default;
    }
    option
}

/// `orchestrator_capabilities`' provider catalogue: every provider instance,
/// what it can run and why not, its models and runtime modes.
fn provider_entries<S: McpHost>(
    host: &S,
    inherited: Option<(&str, &str)>,
    search: Option<&str>,
) -> Vec<Value> {
    let settings = host.settings();
    let search_terms: Vec<String> = search
        .unwrap_or_default()
        .to_lowercase()
        .split_whitespace()
        .map(str::to_owned)
        .collect();
    host.providers()
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|provider| {
            let id = provider["instanceId"].as_str()?;
            let constraints = delegation::provider_constraints(provider);
            let pinned = |model: &Value| {
                let slug = model["slug"].as_str().unwrap_or_default();
                model["isCustom"] == true
                    || inherited.is_some_and(|(instance, model)| instance == id && model == slug)
                    || settings["favorites"]
                        .as_array()
                        .into_iter()
                        .flatten()
                        .any(|f| f["provider"] == id && f["model"] == slug)
            };
            let (listed, omitted) = capability_models(
                provider,
                &settings["providerModelPreferences"][id],
                &search_terms,
                pinned,
            );
            let models: Vec<Value> = listed
                .iter()
                .map(|model| {
                    let slug = model["slug"].as_str().unwrap_or_default();
                    let mut entry = json!({"id": slug});
                    let name = model["name"].as_str().unwrap_or(slug);
                    if name.to_lowercase() != slug.to_lowercase() {
                        entry["label"] = json!(name);
                    }
                    let options: Vec<Value> = model["capabilities"]["optionDescriptors"]
                        .as_array()
                        .into_iter()
                        .flatten()
                        .map(capability_option)
                        .collect();
                    if !options.is_empty() {
                        entry["options"] = json!(options);
                    }
                    entry
                })
                .collect();
            let mut entry = json!({
                "providerInstanceId": id,
                "driverKind": provider["driver"],
                "displayName": provider["displayName"],
                "models": models,
                "canRunChildTask": constraints.is_empty(),
                "canRunCrossProviderChildTask": constraints.is_empty(),
                "constraints": constraints,
            });
            if omitted > 0 {
                entry["omittedModels"] = json!(omitted);
            }
            if provider["supportedRuntimeModes"]
                .as_array()
                .is_some_and(|modes| !modes.is_empty())
            {
                entry["runtimeModes"] = provider["supportedRuntimeModes"].clone();
            }
            Some(entry)
        })
        .collect()
}

/// `parentWorkspaceCwd`: the folder the composer's model picker scans for the
/// calling thread, its worktree or else its project's root.
async fn caller_workspace<S: McpHost>(host: &S, caller: &Caller) -> Option<String> {
    let thread = caller.thread.as_ref()?;
    let store = host.control_state().store.read().await;
    let row = find_thread(&store, &thread.id).ok()?.shell();
    if let Some(worktree) = row["worktreePath"].as_str().filter(|p| !p.is_empty()) {
        return Some(worktree.to_owned());
    }
    store.project_rows.get(&thread.project_id)?["workspaceRoot"]
        .as_str()
        .map(str::to_owned)
}

fn capabilities<S: McpHost>(host: &S, caller: &Caller, a: &Value) -> Value {
    json!({
        "parentThreadId": caller.thread.as_ref().map(|t| t.id.clone()),
        "inheritedProviderInstanceId": caller.thread.as_ref().map(|t| t.provider_instance_id.clone()),
        "inheritedModel": caller.thread.as_ref().map(|t| t.model.clone()),
        // Always a list, as the reference gives it: empty with no thread to inherit from.
        "inheritedOptions": caller.thread.as_ref().map_or_else(|| json!([]), |t| t.model_options.clone()),
        "runtimeMode": caller.ceiling().unwrap_or("approval-required"),
        "interactionMode": caller.interaction(),
        "providers": provider_entries(
            host,
            caller
                .thread
                .as_ref()
                .map(|t| (t.provider_instance_id.as_str(), t.model.as_str())),
            a["search"].as_str(),
        ),
        "features": {
            "appOwnedSubagents": true,
            "asyncPolling": true,
            "cancellation": true,
            "batchThreadCreation": true,
            "threadManagement": true,
            "incrementalThreadRead": true,
            "scheduledTasks": true,
            "maxBatchThreads": 20
        }
    })
}

async fn thread_list<S: McpHost>(host: &S, caller: &Caller, a: &Value) -> Outcome {
    let all = arg_bool(a, "allProjects")?.unwrap_or(false);
    // Inside a thread, its own project is the default.
    let project =
        arg_str(a, "projectId")?.or(caller.thread.as_ref().map(|t| t.project_id.as_str()));
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
        "currentThreadId": caller.thread.as_ref().map(|t| t.id.clone()),
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
    let thread = find_thread(&store, id).map_err(|_| no_longer_available(host, caller, id))?;
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
    provider_entries(host, None, None)
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
    // A result is asked of the first message, which must ask for it.
    let output_schema = a.get("outputSchema").filter(|s| s.is_object()).cloned();
    if let Some(schema) = &output_schema {
        let text = a["message"].as_str().unwrap_or_default();
        let attachments = a["attachments"].as_array().map_or(0, Vec::len);
        let problem = crate::run_output::request_problem(schema, text, attachments);
        if problem.is_some() || text.is_empty() {
            return Err(failure(
                "invalid_request",
                problem.unwrap_or_else(|| {
                    "outputSchema needs a message that asks for the result.".into()
                }),
            ));
        }
    }
    if a.get("attachments")
        .and_then(Value::as_array)
        .is_some_and(|items| !items.is_empty())
    {
        return Err(failure(
            "invalid_request",
            "A new thread accepts only pending attachment uploads.",
        ));
    }
    let scratch = arg_bool(a, "scratch")? == Some(true);
    let explicit_project_id = arg_str(a, "projectId")?;
    let strategy = a.get("workspaceStrategy").filter(|v| !v.is_null());
    if scratch && (explicit_project_id.is_some() || strategy.is_some()) {
        return Err(failure(
            "invalid_request",
            "scratch:true picks its own project and folder; omit projectId and workspaceStrategy.",
        ));
    }
    // Inside a thread, its own project, model and modes are the defaults.
    let inherited = caller.thread.clone();
    let project_id = if scratch {
        host.ensure_scratch_project()
            .await
            .map_err(|message| failure("orchestration_error", message))?
    } else if let Some(id) =
        explicit_project_id.or(inherited.as_ref().map(|t| t.project_id.as_str()))
    {
        id.to_owned()
    } else {
        return Err(failure(
            "target_required",
            "Pass projectId: this MCP client is not running inside a T3 thread.",
        ));
    };
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
            .get(&project_id)
            .filter(|r| r["deletedAt"].is_null())
            .cloned()
            .ok_or_else(|| failure("invalid_request", "The project was not found."))?;
        let root = store
            .projects
            .get(&project_id)
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
        None if inherited.as_ref().is_some_and(|t| !t.model.is_empty()) => {
            let thread = inherited.as_ref().expect("checked");
            json!({"instanceId": thread.provider_instance_id, "model": thread.model})
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
                    format!("Child runtime mode {mode} is broader than parent mode {ceiling}."),
                ));
            }
            mode.to_owned()
        }
        None if inherited.is_some() => ceiling.clone(),
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
        Some("default") if caller.interaction() == "plan" => {
            return Err(failure(
                "interaction_mode_escalation_denied",
                "Child interaction mode default is broader than parent mode plan.",
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
            let folder = if scratch || host.is_scratch_project(&project_id).await {
                let seed = message.as_deref().unwrap_or(&title);
                host.scratch_thread_folder(&thread_id, seed)
                    .await
                    .map_err(|message| failure("orchestration_error", message))?
            } else {
                None
            };
            let worktree_path = match folder {
                Some(path) => json!(path),
                None if root.is_empty() => Value::Null,
                None => json!(root),
            };
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
                "worktreePath": worktree_path,
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
                let mut dispatch = json!({
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
                if let Some(schema) = &output_schema {
                    dispatch["outputSchema"] = schema.clone();
                }
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
    let mut mode = arg_str(a, "mode")?.unwrap_or("auto");
    if !matches!(mode, "auto" | "queue" | "steer" | "restart") {
        return Err(invalid_arguments());
    }
    // A send that asks for a result never joins a running turn.
    let output_schema = a.get("outputSchema").filter(|s| s.is_object()).cloned();
    if output_schema.is_some() && matches!(mode, "steer" | "restart") {
        mode = "queue";
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
        if let Some(problem) = output_schema
            .as_ref()
            .and_then(|schema| crate::run_output::request_problem(schema, &text, 0))
        {
            return Err(failure("invalid_request", problem));
        }
        let projection = thread.projection();
        (
            active(&runs(&projection)).is_some(),
            thread.title.clone(),
            row["modelSelection"].clone(),
        )
    };
    // The reference's stable ids (`stableOperationMessageId`, `stableCommandId`).
    let key = arg_str(a, "clientRequestId")?
        .map(str::to_owned)
        .unwrap_or_else(|| crate::platform::uuid_v4().to_string());
    let part = crate::thread_projection::uri_component;
    let stable = |prefix: &str| {
        format!(
            "{prefix}:mcp:{}:thread-send:{}",
            part(&caller.request_namespace()),
            part(&key)
        )
    };
    let message_id = stable("message");
    let command_id = stable("command");
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
            "commandId": command_id,
            "threadId": id,
            "messageId": message_id,
            "text": text,
            "attachments": [],
            "dispatchMode": {"type": if queued { "queue_after_active" } else { "start_immediately" }},
            "createdBy": "agent",
            "creationSource": "mcp",
        });
        // A thread's agent names itself as the sender.
        if let Some(sender) = caller.granted.thread.as_ref() {
            dispatch["senderThreadId"] = json!(sender.thread_id);
        }
        if selection.is_object() {
            dispatch["modelSelection"] = selection;
        }
        if let Some(schema) = &output_schema {
            dispatch["outputSchema"] = schema.clone();
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
    let mut answer = json!({
        "threadId": id,
        "link": thread_link(&**host, caller, &id, &title),
        "messageId": message_id,
        "runId": run["id"],
        // Rust records a run as running from its start; the reference's run is
        // starting until its provider turn has an attempt.
        "status": if run["status"] == "running" && run["activeAttemptId"].is_null() {
            json!("starting")
        } else {
            run["status"].clone()
        },
        "delivery": if queued { "queued" } else { "started" },
    });
    // A send that started follow-up work names the task it became.
    let task_id = thread.full_projection()["runs"]
        .as_array()
        .into_iter()
        .flatten()
        .find(|r| r["id"] == run["id"])
        .map(|r| r["delegatedTaskId"].clone())
        .filter(|t| !t.is_null());
    if let Some(task_id) = task_id {
        answer["taskId"] = task_id;
    }
    Ok(answer)
}

async fn thread_wait<S: McpHost>(host: &S, a: &Value) -> Outcome {
    let id = required(a, "threadId")?.to_owned();
    let until_output = arg_bool(a, "untilOutput")? == Some(true);
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
        // With untilOutput, a completed run that still owes its result is not
        // settled: a later turn may submit it.
        let owes_output = until_output
            && status == "completed"
            && run["outputSchema"].is_object()
            && run["output"].is_null();
        // Settled once the run ends (`isTerminalRunStatus`): a run waiting on
        // a question, an approval or background work is still going, as is one
        // whose output was submitted while its completion is not yet recorded.
        let settled = TERMINAL.contains(&status.as_str()) && !owes_output;
        let now = crate::platform::now_ms();
        if settled || now >= deadline {
            let mut answer = json!({
                "threadId": id,
                "runId": run["id"],
                "status": status,
                "timedOut": !settled,
            });
            if run["output"].is_object() {
                answer["output"] = run["output"]["value"].clone();
            }
            return Ok(answer);
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

/// What an agent hears when it names another machine while the beta is off here.
const LINKED_ENVIRONMENTS_OFF: &str = "Linked environments is off on this machine, so no tool can act on another machine. Leave out environmentId to work here; the user can turn it on in Settings → Labs.";
const FULL_ACCESS_REQUIRED: &str = "Changing projects or environment settings needs a live full-access/default calling thread or a full-access client.";
const FORWARDED_DEFAULT_WAIT_MS: u64 = 30_000;
const FORWARDED_MAX_WAIT_MS: u64 = 45_000;

/// What a call names as `environmentId`, when that is another environment.
/// `Err` when it is not an id at all.
pub(super) fn linked_target<'a, S: McpHost>(
    host: &S,
    name: &str,
    args: &'a Value,
) -> Result<Option<&'a str>, Refusal> {
    // Only a tool whose `environmentId` is the environment to act in runs
    // there; the link tools name a machine for their own reasons.
    if links_only(name) {
        return Ok(None);
    }
    match args.get("environmentId") {
        // The advertised codec takes null for an optional field.
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(id)) if id.is_empty() => Err(invalid_arguments()),
        Some(Value::String(id)) => Ok((id != host.environment_id()).then_some(id.as_str())),
        Some(_) => Err(Refusal::Rpc(
            -32602,
            format!("Invalid parameters for tool '{name}': Expected string | null\n  at [\"environmentId\"]"),
        )),
    }
}

impl Caller {
    /// The modes this caller's writes are capped at (`clientModeCeiling`).
    fn limits(&self) -> (&'static str, &'static str) {
        if let Some(thread) = &self.thread {
            return (
                RUNTIME_MODES[rank(&thread.runtime_mode).unwrap_or(0)],
                self.interaction(),
            );
        }
        let approved = rank(&self.granted.access).unwrap_or(0);
        let runtime = self
            .runtime_limit
            .as_deref()
            .and_then(rank)
            .map_or(approved, |asked| asked.min(approved));
        (RUNTIME_MODES[runtime], self.interaction())
    }
    /// What scopes this caller's retry keys (`requestNamespace`): its
    /// provider session, or `client:<session>` for an outside client.
    fn request_namespace(&self) -> String {
        match &self.granted.thread {
            Some(scope) => scope.provider_session_id.clone(),
            None => format!("client:{}", self.granted.session_id),
        }
    }
    /// `refuseReadOnlyClient`: the check a changing tool makes before it goes to a link.
    fn refuse_read_only(&self) -> Result<(), Refusal> {
        if self.granted.access == "read-only" {
            return Err(failure(
                "capability_denied",
                "This tool changes the environment, and this MCP client was approved for read-only access.",
            ));
        }
        Ok(())
    }
}

/// What a call bound for a linked environment checks here before it goes:
/// the caller, not the thread or project it names, which are checked there.
fn remote_check(caller: &Caller, name: &str, args: &Value) -> Result<(), Refusal> {
    match name {
        "t3_thread_send"
        | "t3_thread_interrupt"
        | "t3_thread_organize"
        | "t3_thread_update"
        | "t3_workflow_stop" => caller.refuse_read_only(),
        // One tool, two rules: its reads go as they are, its changes as changes.
        "t3_thread_configure" if args.get("modelSelection").is_some() => caller.refuse_read_only(),
        "t3_thread_transfer" if args["action"] != "status" => caller.refuse_read_only(),
        "t3_pending_request" if args["action"] == "respond" => caller.refuse_read_only(),
        "t3_queue" if !matches!(args["action"].as_str(), Some("list" | "read")) => {
            caller.refuse_read_only()
        }
        "t3_thread_launch" => {
            caller.refuse_read_only()?;
            let (runtime, interaction) = caller.limits();
            match arg_str(args, "runtimeMode")? {
                None | Some("inherit") => {}
                Some(mode) => {
                    let wanted = rank(mode).ok_or_else(invalid_arguments)?;
                    if wanted > rank(runtime).unwrap_or(0) {
                        return Err(failure(
                            "runtime_mode_escalation_denied",
                            format!(
                                "Child runtime mode {mode} is broader than parent mode {runtime}."
                            ),
                        ));
                    }
                }
            }
            match arg_str(args, "interactionMode")? {
                Some("default") if interaction == "plan" => Err(failure(
                    "interaction_mode_escalation_denied",
                    "Child interaction mode default is broader than parent mode plan.",
                )),
                None | Some("default" | "plan" | "inherit") => Ok(()),
                Some(_) => Err(invalid_arguments()),
            }
        }
        "t3_project_create"
        | "t3_project_update"
        | "t3_project_delete"
        | "t3_project_clone"
        | "t3_environment_preferences_update" => {
            caller.refuse_read_only()?;
            if caller.limits() != ("full-access", "default") {
                return Err(failure("capability_denied", FULL_ACCESS_REQUIRED));
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

/// Runs `name` in the linked environment `target` (`McpToolAccess.routed`
/// and `PeerForwarding.forward` in the reference). The tool's own checks hold
/// the caller here; the linked environment then holds the call to the link's
/// access, narrowed to the caller's modes, which every forwarded call carries.
pub(super) async fn forward<S: McpHost>(
    host: &Arc<S>,
    caller: &Caller,
    name: &str,
    args: &Value,
    target: &str,
) -> Outcome {
    if !TOOLS
        .as_array()
        .is_some_and(|tools| tools.iter().any(|t| t["name"] == name))
    {
        return Err(Refusal::Rpc(-32602, format!("Tool '{name}' not found")));
    }
    if !host.linked_environments_enabled() {
        return Err(failure("capability_denied", LINKED_ENVIRONMENTS_OFF));
    }
    remote_check(caller, name, args)?;
    // Every caller here reaches the peer as the one link session, so work a
    // fenced link started never uses this environment's links.
    assert_not_linked(caller, "use this environment's links")?;
    let mut args = args.as_object().cloned().unwrap_or_default();
    args.remove("environmentId");
    // The linked environment refuses a mode above the link's access too, but as
    // "broader than parent mode", which misleads when the parent here is broader.
    if let Some(requested) = args
        .get("runtimeMode")
        .and_then(Value::as_str)
        .filter(|m| rank(m).is_some())
    {
        if let Some(link) = host.peer_link(target).filter(|l| !l.expired) {
            if link.access != "read-only" && rank(requested) > rank(&link.access) {
                return Err(failure(
                    "runtime_mode_escalation_denied",
                    format!(
                        "Linked environment \"{label}\" was linked with {access} access, so work there cannot run in {requested}. Omit runtimeMode to run it at a mode up to {access}, or link it again with broader access.",
                        label = link.label,
                        access = link.access
                    ),
                ));
            }
        }
    }
    // A pending upload lives here, so it cannot go with the call.
    if args
        .get("attachments")
        .and_then(Value::as_array)
        .is_some_and(|items| !items.is_empty())
    {
        return Err(failure(
            "invalid_request",
            "A launch in a linked environment cannot carry attachments yet.",
        ));
    }
    // Retry keys are scoped to their caller before they leave: two callers
    // here reusing a key never collide there.
    if let Some(key) = args.get("clientRequestId").and_then(Value::as_str) {
        let digest = <sha2::Sha256 as sha2::Digest>::digest(
            format!("{}\n{key}", caller.request_namespace()).as_bytes(),
        );
        args.insert(
            "clientRequestId".into(),
            json!(format!("fwd:{}", hex::encode(digest))),
        );
    }
    if name == "t3_thread_wait" {
        let asked = args
            .get("timeoutMs")
            .and_then(Value::as_u64)
            .unwrap_or(FORWARDED_DEFAULT_WAIT_MS);
        args.insert(
            "timeoutMs".into(),
            json!(asked.clamp(1, FORWARDED_MAX_WAIT_MS)),
        );
    }
    let (runtime, interaction) = caller.limits();
    let sender = caller
        .thread
        .as_ref()
        .map(|thread| sender_thread(host.environment_id(), &thread.id, &thread.title));
    let Some(call) = host.forward_tool(
        target.to_owned(),
        name.to_owned(),
        Value::Object(args),
        format!("{runtime}/{interaction}"),
        sender,
    ) else {
        return Err(failure(
            "orchestration_error",
            "This environment cannot reach linked environments.",
        ));
    };
    call.await
        .map_err(|(code, message)| Refusal::Failure(code, message))
}

/// Whether `value` is what `tool` answers, by the `outputSchema` it lists:
/// the shape the reference decodes a linked environment's answer with.
/// Extra keys are ignored, as the reference's decoding ignores them.
pub(crate) fn answer_matches(tool: &str, value: &Value) -> bool {
    TOOLS
        .as_array()
        .and_then(|tools| tools.iter().find(|t| t["name"] == tool))
        .map(|t| &t["outputSchema"])
        .is_none_or(|schema| schema.is_null() || matches_schema(schema, value))
}

fn matches_schema(schema: &Value, value: &Value) -> bool {
    if let Some(options) = schema["anyOf"].as_array() {
        if !options.iter().any(|option| matches_schema(option, value)) {
            return false;
        }
    }
    if let Some(allowed) = schema["enum"].as_array() {
        if !allowed.contains(value) {
            return false;
        }
    }
    if let Some(kind) = schema["type"].as_str() {
        let fits = match kind {
            "string" => value.is_string(),
            "boolean" => value.is_boolean(),
            "null" => value.is_null(),
            "number" => value.is_number(),
            "integer" => value.as_f64().is_some_and(|n| n.fract() == 0.0),
            "array" => value.is_array(),
            "object" => value.is_object(),
            _ => true,
        };
        if !fits {
            return false;
        }
    }
    if let Some(n) = value.as_f64() {
        if schema["minimum"].as_f64().is_some_and(|min| n < min)
            || schema["maximum"].as_f64().is_some_and(|max| n > max)
        {
            return false;
        }
    }
    if let (Some(items), Some(values)) = (schema.get("items"), value.as_array()) {
        if !values.iter().all(|v| matches_schema(items, v)) {
            return false;
        }
    }
    if let Some(object) = value.as_object() {
        let required = schema["required"]
            .as_array()
            .map(Vec::as_slice)
            .unwrap_or_default();
        if !required
            .iter()
            .filter_map(Value::as_str)
            .all(|key| object.contains_key(key))
        {
            return false;
        }
        if let Some(properties) = schema["properties"].as_object() {
            for (key, property) in properties {
                if let Some(v) = object.get(key) {
                    if !matches_schema(property, v) {
                        return false;
                    }
                }
            }
        }
    }
    true
}

/// Top-level arguments the tool's advertised schema does not list, named as
/// the reference names them (`advertisedArgumentsOnly`).
pub(super) fn unknown_arguments(name: &str, args: &Value) -> Result<(), Refusal> {
    const MAX_NAMED: usize = 5;
    const MAX_NAME_CHARS: usize = 64;
    let Some(object) = args.as_object() else {
        return Ok(());
    };
    let Some(tool) = tool_definition(name) else {
        return Ok(());
    };
    let accepted: Vec<&str> = tool["inputSchema"]["properties"]
        .as_object()
        .map(|p| p.keys().map(String::as_str).collect())
        .unwrap_or_default();
    let unknown: Vec<&String> = object
        .keys()
        .filter(|k| !accepted.contains(&k.as_str()))
        .collect();
    if unknown.is_empty() {
        return Ok(());
    }
    let named: Vec<String> = unknown
        .iter()
        .take(MAX_NAMED)
        .map(|key| {
            let cut: String = if key.chars().count() > MAX_NAME_CHARS {
                key.chars().take(MAX_NAME_CHARS).chain(['…']).collect()
            } else {
                key.to_string()
            };
            Value::String(cut).to_string()
        })
        .collect();
    let more = if unknown.len() > named.len() {
        format!(" and {} more", unknown.len() - named.len())
    } else {
        String::new()
    };
    let accepted_text = if accepted.is_empty() {
        "none".to_owned()
    } else {
        accepted.join(", ")
    };
    Err(Refusal::Rpc(
        -32602,
        format!(
            "Invalid parameters for tool '{name}': Unknown argument{} {}{more}. Accepted arguments: {accepted_text}.",
            if unknown.len() == 1 { "" } else { "s" },
            named.join(", ")
        ),
    ))
}

/// A title can be any length; the header carrying it stays well under HTTP header limits.
const SENDER_TITLE_MAX_CHARS: usize = 200;

/// `T3-Sender-Thread: <environmentId>/<threadId>/<title>`, each part encoded as
/// `encodeURIComponent` encodes it, the title cut to whole characters.
fn sender_thread(environment_id: &str, thread_id: &str, title: &str) -> String {
    const COMPONENT: &percent_encoding::AsciiSet = &percent_encoding::NON_ALPHANUMERIC
        .remove(b'-')
        .remove(b'_')
        .remove(b'.')
        .remove(b'!')
        .remove(b'~')
        .remove(b'*')
        .remove(b'\'')
        .remove(b'(')
        .remove(b')');
    let title: String = title.chars().take(SENDER_TITLE_MAX_CHARS).collect();
    [environment_id, thread_id, title.as_str()]
        .iter()
        .map(|part| percent_encoding::utf8_percent_encode(part, COMPONENT).to_string())
        .collect::<Vec<_>>()
        .join("/")
}

/// `assertNotLinked`: below full access, work a link started may not change
/// what this environment already has, nor use its links.
fn assert_not_linked(caller: &Caller, what: &str) -> Result<(), Refusal> {
    if caller.granted.peer_link && caller.granted.access != "full-access" {
        return Err(failure(
            "capability_denied",
            format!(
                "This work was started from {} through a linked environment, so it cannot {what}. A link paired with full access is not limited this way.",
                caller.granted.label
            ),
        ));
    }
    Ok(())
}

/// `McpToolAccess.writesEnvironment`: a caller that may change projects or
/// environment settings. `refused` names what linked work may not do, or
/// `None` where linked callers may.
fn writes_environment(caller: &Caller, refused: Option<&str>) -> Result<(), Refusal> {
    caller.refuse_read_only()?;
    if let Some(what) = refused {
        assert_not_linked(caller, what)?;
    }
    if caller.limits() != ("full-access", "default") {
        // A client a link holds below full access is refused naming that
        // link (`assertFullAccessNamingLink`, #30471).
        if caller.thread.is_none()
            && caller.granted.peer_link
            && caller.granted.access != "full-access"
        {
            return Err(failure(
                "capability_denied",
                format!(
                    "This work was started from {} through a linked environment, so it has that link's {} access, which is below the full access needed to change projects or environment settings. A link paired with full access is not limited this way.",
                    caller.granted.label, caller.granted.access
                ),
            ));
        }
        return Err(failure("capability_denied", FULL_ACCESS_REQUIRED));
    }
    Ok(())
}

const LINKS_OFF_HERE: &str =
    "Linked environments is off on this machine. Turn it on in Settings → Labs.";

/// `t3_environment_links`: the environments this one links to, as an agent sees them.
async fn environment_links<S: McpHost>(host: &Arc<S>, caller: &Caller) -> Outcome {
    assert_not_linked(caller, "use this environment's links")?;
    if !host.linked_environments_enabled() {
        return Err(failure("capability_denied", LINKED_ENVIRONMENTS_OFF));
    }
    let links = host
        .peer_links_listed()
        .await
        .map_err(|message| failure("orchestration_error", message))?;
    Ok(json!({
        "environmentId": host.environment_id(),
        "links": links.iter().map(|link| json!({
            "environmentId": link["environmentId"],
            "label": link["label"],
            "status": link["status"],
            "access": link["access"],
            "expiresAt": link["expiresAt"],
            "lastError": link["lastError"],
        })).collect::<Vec<_>>(),
        "notListed": "Not listed: not linked yet. Call t3_environment_link with a hint from the user's words.",
    }))
}

/// `t3_environment_unlink`: forgets a link here.
async fn environment_unlink<S: McpHost>(host: &Arc<S>, caller: &Caller, a: &Value) -> Outcome {
    let environment_id = required(a, "environmentId")?.to_owned();
    writes_environment(caller, Some("change projects or environment settings"))?;
    let (removed, ended_there) = host
        .unlink_peer_ending(&environment_id)
        .await
        .map_err(|message| failure("orchestration_error", message))?;
    Ok(json!({"environmentId": environment_id, "removed": removed, "endedThere": ended_there}))
}

/// The card id a link request gets: turn item ids are global, so the key is
/// scoped to its thread and provider session (`cardId`).
fn link_card_id(thread_id: &str, namespace: &str, key: &str) -> String {
    use crate::thread_projection::uri_component;
    format!(
        "turn-item:link-request:{}:{}:{}",
        uri_component(thread_id),
        uri_component(namespace),
        uri_component(key)
    )
}

/// What a retry hears: the card's outcome, without anything the user typed.
fn link_card_result(card: &Value) -> Value {
    let target = card["url"]
        .as_str()
        .or(card["environmentId"].as_str())
        .unwrap_or("a machine");
    match card["linkStatus"].as_str() {
        Some("linked") => json!({
            "status": "linked",
            "environmentId": card["linkedEnvironmentId"],
            "label": card["linkedLabel"].as_str().unwrap_or(target),
            "access": card["linkedAccess"],
        }),
        Some("failed") => json!({
            "status": "failed",
            "message": card["failure"].as_str().unwrap_or("The link could not be made."),
        }),
        Some(status @ ("declined" | "cancelled")) => json!({"status": status}),
        _ => json!({"status": "pending", "turnItemId": card["id"]}),
    }
}

/// `t3_environment_link` (`PeerLinkRequests.request`): shows a link card in
/// the calling thread and returns at once; the answer reaches the agent as a
/// notification.
async fn environment_link<S: McpHost>(host: &Arc<S>, caller: &Caller, a: &Value) -> Outcome {
    let url = arg_str(a, "url")?.map(str::to_owned);
    let environment_id = arg_str(a, "environmentId")?.map(str::to_owned);
    let hint = arg_str(a, "hint")?.map(str::to_owned);
    let reason = arg_str(a, "reason")?.unwrap_or_default().to_owned();
    let key = arg_str(a, "clientRequestId")?.map(str::to_owned);
    let requested_access = match arg_str(a, "requestedAccess")? {
        None => None,
        Some(
            access @ ("read-only" | "approval-required" | "auto-accept-edits" | "auto"
            | "full-access"),
        ) => Some(access.to_owned()),
        Some(_) => return Err(invalid_arguments()),
    };
    writes_environment(caller, Some("change projects or environment settings"))?;
    let (Some(thread), Some(scope)) = (caller.thread.as_ref(), caller.granted.thread.as_ref())
    else {
        return Err(failure(
            "thread_credential_required",
            "t3_environment_link acts as the calling T3 thread, so it needs an agent running inside T3 Code. This MCP client signed in from outside a thread.",
        ));
    };
    if !host.linked_environments_enabled() {
        return Err(failure("capability_denied", LINKS_OFF_HERE));
    }
    if url.is_some() && environment_id.is_some() {
        return Err(failure(
            "invalid_request",
            "Pass either environmentId or url, not both. Pass neither to let the user pick the machine.",
        ));
    }
    let namespace = caller.request_namespace();
    // A machine already linked needs no card: the agent can use it now.
    if let Some(id) = &environment_id {
        if let Some(link) = host.peer_link(id).filter(|l| !l.expired) {
            if let Some(key) = &key {
                // A card still waiting for this key is answered by the link.
                let card_id = link_card_id(&thread.id, &namespace, key);
                let pending = {
                    let store = host.control_state().store.read().await;
                    store
                        .threads
                        .get(&thread.id)
                        .and_then(|t| {
                            t.link_requests
                                .iter()
                                .find(|c| c["id"] == card_id.as_str())
                                .cloned()
                        })
                        .filter(|card| card["linkStatus"] == "pending")
                };
                if let Some(card) = pending {
                    let mut command =
                        card_command(&card, &format!("link-request:{card_id}:linked-on-retry"));
                    command["linkStatus"] = json!("linked");
                    command["linkedEnvironmentId"] = json!(id);
                    command["linkedLabel"] = json!(link.label);
                    command["linkedAccess"] = json!(link.access);
                    host.dispatch(command).await.map_err(|error| {
                        failure(
                            "orchestration_error",
                            format!(
                                "Could not close the link request: {}",
                                dispatch_message(&error)
                            ),
                        )
                    })?;
                }
            }
            return Ok(
                json!({"status": "linked", "environmentId": id, "label": link.label, "access": link.access}),
            );
        }
    }
    let key = key.unwrap_or_else(|| crate::platform::uuid_v4().to_string());
    let card_id = link_card_id(&thread.id, &namespace, &key);
    let run = {
        let store = host.control_state().store.read().await;
        let current = store
            .threads
            .get(&thread.id)
            .ok_or_else(calling_thread_missing)?;
        if let Some(card) = current
            .link_requests
            .iter()
            .find(|c| c["id"] == card_id.as_str())
        {
            return Ok(link_card_result(card));
        }
        // The card shows in, and is answered from, the caller's running turn.
        current
            .runs
            .iter()
            .rev()
            .find(|run| run.status == "running")
            .filter(|_| thread.provider_instance_id == scope.provider_instance_id)
            .and_then(|run| {
                current
                    .provider_run_ids(run)
                    .map(|ids| (run.id.to_string(), ids.root_node))
            })
    };
    let Some((run_id, node_id)) = run else {
        return Err(failure(
            "parent_not_active",
            "Asking to link an environment requires an active run owned by this MCP provider session.",
        ));
    };
    let mut command = json!({
        "type": "link_request.record",
        "commandId": format!(
            "command:mcp:{}:link-pending:{}",
            crate::thread_projection::uri_component(&namespace),
            crate::thread_projection::uri_component(&key)
        ),
        "threadId": thread.id,
        "runId": run_id,
        "nodeId": node_id,
        "turnItemId": card_id,
        "reason": reason,
        "linkStatus": "pending",
    });
    for (field, value) in [
        ("url", &url),
        ("environmentId", &environment_id),
        ("hint", &hint),
        ("requestedAccess", &requested_access),
    ] {
        if let Some(value) = value {
            command[field] = json!(value);
        }
    }
    host.dispatch(command).await.map_err(|error| {
        failure(
            "orchestration_error",
            format!(
                "Could not record the link request: {}",
                dispatch_message(&error)
            ),
        )
    })?;
    Ok(json!({"status": "pending", "turnItemId": card_id}))
}

/// A `link_request.record` command that repeats `card` as it was asked.
pub(crate) fn card_command(card: &Value, command_id: &str) -> Value {
    let mut command = json!({
        "type": "link_request.record",
        "commandId": command_id,
        "threadId": card["threadId"],
        "runId": card["runId"],
        "nodeId": card["nodeId"],
        "turnItemId": card["id"],
        "reason": card["reason"],
        "linkStatus": card["linkStatus"],
    });
    for field in ["url", "environmentId", "label", "hint", "requestedAccess"] {
        if !card[field].is_null() {
            command[field] = card[field].clone();
        }
    }
    command
}

#[cfg(test)]
mod sender_tests {
    use super::*;

    #[test]
    fn lists_the_served_tools_with_the_reference_definitions() {
        let reference: Value = serde_json::from_str(include_str!("tools.json")).unwrap();
        let listed: Vec<&Value> = SERVED_TOOLS.iter().map(|(tool, _)| tool).collect();
        assert_eq!(listed.len(), SERVED.len());
        for tool in listed {
            let name = tool["name"].as_str().unwrap();
            assert!(
                matches!(
                    reference["listedTo"][name].as_str(),
                    Some(
                        "everyone"
                            | LINKS_ONLY
                            | "hiddenFromOutsideClients"
                            | "hiddenFromSubagents"
                            | "hiddenWithoutForge"
                            | "hiddenFromThreads"
                            | "hiddenWithoutBackgroundWait"
                            | "hiddenWhileFeedbackOff",
                    )
                ),
                "{name} needs listing by caller"
            );
            assert!(reference["tools"]
                .as_array()
                .unwrap()
                .iter()
                .any(|t| t == tool));
        }
    }

    #[test]
    fn the_sender_header_encodes_as_encode_uri_component_does() {
        // Expected values from Node's encodeURIComponent.
        assert_eq!(
            sender_thread("env-1", "thread:a/b", "Fix (it) — naïve 🚀 * ~ !' 100%"),
            "env-1/thread%3Aa%2Fb/Fix%20(it)%20%E2%80%94%20na%C3%AFve%20%F0%9F%9A%80%20*%20~%20!'%20100%25"
        );
        let header = sender_thread("e", "t", &"é".repeat(250));
        assert_eq!(header.len(), "e/t/".len() + 1200);
    }
}

#[cfg(test)]
mod catalogue_tests {
    use super::*;

    #[test]
    fn a_catalogue_lists_current_and_pinned_models_and_counts_the_rest() {
        let models: Vec<Value> = (0..12)
            .map(|i| json!({"slug": format!("m{i}"), "name": format!("M{i}"), "isLegacy": i >= 10}))
            .collect();
        let provider = json!({"instanceId": "cursor", "driver": "cursor", "displayName": "Cursor", "models": models});
        let (listed, omitted) =
            capability_models(&provider, &Value::Null, &[], |m| m["slug"] == "m11");
        // The first eight current models, and the pinned legacy one.
        let slugs: Vec<&str> = listed.iter().filter_map(|m| m["slug"].as_str()).collect();
        assert_eq!(
            slugs,
            ["m0", "m1", "m2", "m3", "m4", "m5", "m6", "m7", "m11"]
        );
        assert_eq!(omitted, 3);
        // A full-catalogue driver lists every current model.
        let codex = json!({"instanceId": "codex", "driver": "codex", "models": provider["models"]});
        assert_eq!(capability_models(&codex, &Value::Null, &[], |_| false).1, 2);
        // A term that names the provider lists as usual; one that names models filters.
        let terms = vec!["cursor".to_owned()];
        assert_eq!(
            capability_models(&provider, &Value::Null, &terms, |_| false).1,
            4
        );
        let terms = vec!["m1".to_owned()];
        let (found, rest) = capability_models(&provider, &Value::Null, &terms, |_| false);
        assert_eq!(found.len(), 3);
        assert_eq!(rest, 0);
        // Hidden models drop out; the preferred order comes first.
        let prefs = json!({"hiddenModels": ["m0"], "modelOrder": ["m5"]});
        let (ordered, _) = capability_models(&provider, &prefs, &[], |_| false);
        assert_eq!(ordered[0]["slug"], "m5");
        assert!(ordered.iter().all(|m| m["slug"] != "m0"));
    }
}
