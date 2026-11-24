//! Orchestration commands (`orchestration.dispatchCommand`): checks, then the
//! events each command commits. Dispatch runs over the control state; what
//! it needs beyond that (policy, run execution, guest shares) comes from a
//! [`CommandHost`].
use super::{now, CommandReceipt, ConversationMessage, Event, Run, Thread};
use crate::control_state::ControlState;
use serde_json::{json, Value};
use std::{collections::HashMap, sync::Arc};
use tokio::sync::{oneshot, Mutex};
use uuid::Uuid;

mod feedback_request;
mod secret_request;
mod steer;

/// What command dispatch needs from the server beyond the control state.
pub(crate) trait CommandHost:
    std::ops::Deref<Target = ControlState> + crate::workspace::MaybeSendSync + Sized
{
    /// Held while a command checks and commits, so commands apply one at a time.
    fn mutation_lock(&self) -> &Mutex<()>;
    /// The managed policy's summary, when a policy applies.
    fn policy_summary(&self) -> Option<&Value>;
    fn runtime_denial(&self, mode: &str) -> Option<String>;
    fn provider_denial(&self, driver: &str) -> Option<String>;
    /// The effective settings.
    fn effective_settings(&self) -> anyhow::Result<Value>;
    fn workspace(&self) -> &dyn crate::workspace::Workspace;
    /// Running runs by id; resolving a sender stops its run.
    fn cancellations(&self) -> &Mutex<HashMap<Uuid, oneshot::Sender<()>>>;
    /// Execute `run` in the background until it ends or `cancel` resolves,
    /// recording a failure to start or run as the run's end.
    fn launch_run(
        self: Arc<Self>,
        run: Run,
        cwd: String,
        text: String,
        cancel: oneshot::Receiver<()>,
    );
    /// Mark `run`'s turn as stopped by a session detach, before its cancel
    /// fires: a driver whose session is the thread's own process (Claude)
    /// then ends that process instead of interrupting the turn, as the
    /// reference's detach does. Hosts without local provider turns ignore it.
    fn detach_turn(&self, _run: Uuid) {}
    /// Unload the detached thread's native Codex `thread` from `instance`'s
    /// shared app-server once its turn ended, or close its Claude session's
    /// CLI. Hosts without local provider turns ignore it.
    fn unload_provider_thread(&self, _instance: &str, _thread: &str) {}
    /// Run a `thread-title.generate` effect in the background. Hosts that
    /// run no provider CLIs ignore it.
    fn generate_title(self: Arc<Self>, _effect: crate::title_generation::TitleEffect) {}
    /// Refresh a newly linked pull request after its link command commits.
    /// Hosts without a provider service leave the link unsynchronized.
    fn sync_pull_request_link(self: Arc<Self>, _thread_id: String, _target: Value) {}
    /// Whether the host's runs call [`after_run`] and
    /// [`crate::turn::TurnHost::turn_stopped`] as they end, so work may wait
    /// for a run's end as the reference's reactors do: a wake queued behind
    /// a busy parent, a delivery settled when its wake ends, a stop's
    /// delegated tasks stopped once its turn ended. Without them a delivery
    /// waits for an idle parent and is accepted with its message.
    fn hooks_run_ends(&self) -> bool {
        false
    }
    fn close_guest_thread(&self, thread_id: &str) -> Result<(), String>;
    fn resolve_guest_message(
        &self,
        thread_id: &str,
        message_id: &str,
        outcome: &str,
    ) -> Result<(), String>;
}

pub(crate) async fn dispatch_orchestration_command<H: CommandHost>(
    state: Arc<H>,
    command: Value,
) -> Result<u64, Value> {
    dispatch_command(state, command, None).await
}

/// The reason a stop without one records: a person stopped the run
/// (`RunExecutionService.USER_STOP_REASON`).
pub(crate) const USER_STOP_REASON: &str = "Stopped by user";

/// Where a delegated delivery (`completion`) on `thread_id` steers: into the
/// running turn, when every task it carries wakes the parent always and that
/// turn's steering does not interrupt its tools, as the reference's
/// `dispatchMessage` decides; since #39528 that includes Claude turns. A
/// steer the turn ends without taking is sent again as a run of its own
/// (`delegation::after_run`).
pub(crate) async fn delivery_steer_target<H: CommandHost>(
    state: &Arc<H>,
    thread_id: &str,
    completion: &Value,
) -> Option<steer::Target> {
    let running = {
        let store = state.store.read().await;
        let thread = store.threads.get(thread_id)?;
        let tasks = thread.stored_row()["subagents"].clone();
        let always = completion["taskIds"].as_array().is_some_and(|ids| {
            ids.iter().all(|id| {
                tasks
                    .as_array()
                    .into_iter()
                    .flatten()
                    .any(|task| task["id"] == *id && task["completionWake"] == "always")
            })
        });
        if !always {
            return None;
        }
        let run = thread.runs.iter().find(|run| run.status == "running")?;
        let ids = thread.provider_run_ids(run)?;
        let capabilities: Value =
            serde_json::from_str(include_str!("provider_capabilities.json")).unwrap_or_default();
        let turns = &capabilities[ids.driver.as_str()]["turns"];
        (turns["supportsActiveSteering"] == true && turns["activeSteeringInterruptsTools"] != true)
            .then(|| run.wire_id())?
    };
    let mut steering = json!({"dispatchMode": {"type": "steer_active", "targetRunId": running}});
    steering["threadId"] = json!(thread_id);
    match steer::plan(state, thread_id, &steering).await {
        Ok(steer::SteerPlan::Steer(target)) => Some(target),
        _ => None,
    }
}

/// Dispatch the server's own `message.dispatch` that continues `source`, a
/// run a restart cut off, with the checks a client's message gets.
pub(crate) async fn dispatch_restart_continuation<H: CommandHost>(
    state: Arc<H>,
    command: Value,
    source: Uuid,
) -> Result<u64, Value> {
    dispatch_command(state, command, Some(source)).await
}

/// Why `thread.settle` may not settle `thread` now, as the reference's
/// orchestrator refuses it: another run arrived since the caller looked
/// (`expectedLatestRunId`), or the thread still waits on a question, runs, or
/// holds a message the user queued.
fn settle_blocker(thread: &crate::Thread, command: &Value) -> Option<String> {
    let projection = thread.full_projection();
    let runs = projection["runs"].as_array().cloned().unwrap_or_default();
    let id = &thread.id;
    if let Some(expected) = command.get("expectedLatestRunId").and_then(Value::as_str) {
        let latest = runs.iter().max_by_key(|run| run["ordinal"].as_u64());
        if latest.and_then(|run| run["id"].as_str()) != Some(expected) {
            return Some("The thread received another run before settlement.".into());
        }
    }
    let messages = projection["messages"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    let automatic = |message_id: &Value| {
        messages.iter().any(|message| {
            message["id"] == *message_id
                && message["role"] == "user"
                && (message["createdBy"].as_str().is_some_and(|by| by != "user")
                    || !message["scheduledTaskId"].is_null()
                    || !message["notification"].is_null()
                    || !message["delegatedCompletion"].is_null())
        })
    };
    let running = runs.iter().any(|run| {
        matches!(
            run["status"].as_str(),
            Some("preparing" | "starting" | "running" | "waiting")
        )
    });
    let queued = runs
        .iter()
        .any(|run| run["status"] == "queued" && !automatic(&run["userMessageId"]));
    let blocking = projection["runtimeRequests"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|request| request["status"] == "pending")
        .any(|request| {
            request["kind"] != "user_input" || request["responseCapability"]["type"] != "message"
        });
    let blocker = if blocking {
        "is waiting on an approval or question. Answer it before settling."
    } else if running {
        "is still running. Stop it before settling."
    } else if queued {
        "has a queued message. Send it or remove it from the queue before settling."
    } else {
        return None;
    };
    Some(format!("Thread {id} {blocker}"))
}

async fn dispatch_command<H: CommandHost>(
    state: Arc<H>,
    command: Value,
    continuation_of: Option<Uuid>,
) -> Result<u64, Value> {
    let kind = command["type"].as_str().unwrap_or_default();
    let command_id = command["commandId"].as_str().unwrap_or_default();
    let id = command["threadId"]
        .as_str()
        .or_else(|| {
            matches!(kind, "thread.fork" | "thread.merge_back")
                .then(|| command["targetThreadId"].as_str())
                .flatten()
        })
        .or_else(|| command["parentThreadId"].as_str())
        .unwrap_or_default();
    // Scheduled fresh-session deliveries detach an idle provider session
    // before their new run is accepted. A live run remains queued and keeps
    // its session until it ends.
    if kind == "message.dispatch"
        && command["freshContext"] == true
        && !thread_has_active_run(&state, id).await
    {
        detach_session(&state, id, "Fresh scheduled session.")
            .await
            .map_err(|error| {
                json!({"_tag":"OrchestrationV2DispatchCommandError","commandId":command_id,"commandType":kind,"message":error.to_string()})
            })?;
    }
    let error = |detail: String, cause: Value| json!({"_tag":"OrchestrationV2DispatchCommandError","commandId":command_id,"commandType":kind,"message":detail,"detail":detail,"cause":cause});
    let dispatch_error = |detail: String| {
        error(
            detail.clone(),
            json!({"name":"OrchestratorDispatchError","message":format!("Failed to dispatch orchestration command {kind} ({command_id})."),"cause":detail}),
        )
    };
    let guard = state.mutation_lock().lock().await;
    let store = state.store.read().await;
    if let Some((prior_id, seq)) = store.command_receipts.get(command_id) {
        if id == prior_id {
            return Ok(*seq);
        }
        let msg = format!("Command {command_id} was already handled for thread {prior_id} and cannot be replayed for {id}.");
        return Err(error(
            msg.clone(),
            json!({"name":"OrchestratorCommandIdConflictError","message":msg}),
        ));
    }
    if matches!(kind, "thread.fork" | "delegated_task.request") {
        let target = command["targetThreadId"]
            .as_str()
            .or_else(|| command["childThreadId"].as_str())
            .unwrap_or_default();
        if !target.is_empty()
            && (store.threads.contains_key(target) || store.deleted_thread_ids.contains(target))
        {
            let msg = format!("Thread {target} already exists. Use a new thread id.");
            return Err(error(
                msg.clone(),
                json!({"name":"OrchestratorThreadAlreadyExistsError","message":msg}),
            ));
        }
    }
    if kind == "thread.create"
        && (store.threads.contains_key(id) || store.deleted_thread_ids.contains(id))
    {
        let msg = format!("Thread {id} already exists. Use a new thread id.");
        return Err(error(
            msg.clone(),
            json!({"name":"OrchestratorThreadAlreadyExistsError","message":msg}),
        ));
    }
    if let Some(id) = command["threadId"].as_str().filter(|_| {
        !matches!(
            kind,
            "thread.create" | "thread.fork" | "delegated_task.request"
        )
    }) {
        let Some(thread) = store.threads.get(id) else {
            let msg = format!("No orchestration projection exists for thread {id}.");
            return Err(error(
                msg.clone(),
                json!({"name":"OrchestratorProjectionError","message":format!("Failed to load orchestration projection for thread {id}."),"cause":{"name":"ProjectionStoreThreadNotFoundError","message":msg}}),
            ));
        };
        if kind == "thread.archive" && thread.archived {
            return Err(dispatch_error(format!("Thread {id} is already archived.")));
        }
        if kind == "thread.unarchive" && !thread.archived {
            return Err(dispatch_error(format!("Thread {id} is not archived.")));
        }
        if kind == "thread.mark-unread" && !thread.runs.iter().any(|r| r.finished_at.is_some()) {
            return Err(dispatch_error(format!(
                "Thread {id} has no completed run to mark unread."
            )));
        }
        if thread.archived
            && matches!(
                kind,
                "thread.settle"
                    | "thread.auto-settle"
                    | "thread.unsettle"
                    | "thread.snooze"
                    | "thread.unsnooze"
                    | "thread.auto-settle.set"
                    | "thread.pin"
                    | "thread.unpin"
                    | "thread.pin.reorder"
                    | "thread.active.reorder"
                    | "queue.resume"
                    | "queued-message.promote-to-steer"
                    | "thread.pull-request-watch.sync"
                    | "thread.pull-request.sync"
            )
        {
            return Err(dispatch_error(format!("Thread {id} is archived.")));
        }
        if kind == "thread.settle" {
            if let Some(refusal) = settle_blocker(thread, &command) {
                return Err(dispatch_error(refusal));
            }
        }
        if kind == "message.dispatch" && !store.projects.contains_key(&thread.project_id) {
            let runtime = json!({
                "name":"RuntimePolicyResolveError",
                "message":format!("Failed to resolve runtime policy for provider instance {} in project {}.", thread.row["providerInstanceId"].as_str().unwrap_or("codex"), thread.project_id),
                "cause":"Project not found."
            });
            let nested = json!({
                "name":"OrchestratorDispatchError",
                "message":format!("Failed to dispatch orchestration command {kind} ({command_id})."),
                "cause":runtime
            });
            return Err(error(
                "Project not found.".into(),
                json!({"name":"OrchestratorDispatchError","message":format!("Failed to dispatch orchestration command {kind} ({command_id})."),"cause":nested}),
            ));
        }
    }
    if let Some(summary) = state.policy_summary() {
        let row = store
            .threads
            .get(id)
            .map(Thread::stored_row)
            .unwrap_or(Value::Null);
        let starts_work = kind == "message.dispatch";
        let sets_mode = matches!(kind, "thread.create" | "thread.runtime-mode.set");
        if sets_mode || starts_work {
            let mode = if sets_mode {
                &command["runtimeMode"]
            } else {
                &row["runtimeMode"]
            };
            if let Some(mut message) = mode.as_str().and_then(|mode| state.runtime_denial(mode)) {
                if starts_work {
                    message.push_str(" Switch this thread's mode to continue.");
                }
                return Err(error(
                    message.clone(),
                    json!({"name":"OrchestratorRuntimeModeAboveCeilingError","message":message}),
                ));
            }
        }
        if starts_work && summary["limits"]["providers"]["allowedDrivers"].is_array() {
            let id = command["modelSelection"]["instanceId"]
                .as_str()
                .or(row["providerInstanceId"].as_str())
                .unwrap_or("codex");
            let settings = state
                .effective_settings()
                .map_err(|e| dispatch_error(e.to_string()))?;
            let driver = settings["providerInstances"][id]["driver"]
                .as_str()
                .unwrap_or(id);
            if let Some(reason) = state.provider_denial(driver) {
                return Err(dispatch_error(reason));
            }
        }
    }
    // A message, or a metadata update that continues the thread, starts
    // work on a provider instance; the reference refuses a disabled one.
    let continues = kind == "thread.metadata.update" && !command["continuation"].is_null();
    if kind == "message.dispatch" || continues {
        if let Some(thread) = store.threads.get(id) {
            let row = thread.stored_row();
            let instance = (!continues)
                .then(|| command["modelSelection"]["instanceId"].as_str())
                .flatten()
                .or(row["modelSelection"]["instanceId"].as_str())
                .or(row["providerInstanceId"].as_str())
                .unwrap_or("codex");
            let settings = state
                .effective_settings()
                .map_err(|e| dispatch_error(e.to_string()))?;
            if !provider_instance_enabled(&settings, &thread.project_id, instance) {
                return Err(dispatch_error(format!(
                    "Provider instance \"{instance}\" is disabled for project {}.",
                    thread.project_id
                )));
            }
        }
    }
    drop(store);
    let receipt = CommandReceipt {
        command_id: command_id.to_string(),
        thread_id: id.to_string(),
        command_type: kind.to_string(),
    };
    // Boxed: the command's future is large (hundreds of KB in a debug
    // build), and a caller that awaits it inline, such as an RPC on its
    // WebSocket task, would otherwise hold it on a worker's stack.
    let mut seq = Box::pin(apply_command(
        state.clone(),
        &command,
        &receipt,
        continuation_of,
    ))
    .await
    .map_err(|e| dispatch_error(e.to_string()))?;
    if kind == "thread.pull-request.link" {
        state
            .clone()
            .sync_pull_request_link(id.to_owned(), command.clone());
    }
    // Stopping, settling or archiving a thread means the user is done with
    // its link cards; nobody is woken (`closePendingLinkRequests`).
    if matches!(
        kind,
        "thread.archive" | "thread.settle" | "thread.stop" | "run.interrupt"
    ) {
        if let Some(last) = close_pending_link_requests(&state, id).await {
            seq = last;
        }
    }
    drop(guard);
    // A thread's stop then stops its delegated tasks, as the reference's
    // effect after the interrupt does (`delegatedTasksStopEffect`).
    if kind == "thread.stop"
        && command["stopDelegatedTasks"] != false
        && !delegation::child_stop_deferred(id, command_id)
    {
        let _ = delegation::stop_delegated_tasks(
            state.clone(),
            id.to_owned(),
            command_id.to_owned(),
            command["reason"].as_str().map(str::to_owned),
        )
        .await;
    }
    Ok(seq)
}

/// Cancels `thread_id`'s link cards still waiting for the user; the sequence
/// of the last commit, if there was one.
async fn close_pending_link_requests<H: CommandHost>(
    state: &Arc<H>,
    thread_id: &str,
) -> Option<u64> {
    let waiting: Vec<Value> = state
        .store
        .read()
        .await
        .threads
        .get(thread_id)?
        .link_requests
        .iter()
        .filter(|card| card["linkStatus"] == "pending")
        .cloned()
        .collect();
    let at = crate::thread_projection::iso(now());
    let mut last = None;
    for mut card in waiting {
        card["status"] = json!("cancelled");
        card["linkStatus"] = json!("cancelled");
        card["completedAt"] = json!(at);
        card["updatedAt"] = json!(at);
        if let Ok(record) = state
            .commit(Event::LinkRequestRecorded {
                thread_id: thread_id.to_owned(),
                card,
            })
            .await
        {
            last = Some(record.id);
        }
    }
    last
}

/// Why the reference detaches the provider session for a thread change
/// (`provider-session.detached`), or `None` when the change keeps it.
const PROVIDER_CHANGED: &str = "Provider or model selection changed.";

fn detach_reason(command_type: &str, command: &Value, row: &Value) -> Option<&'static str> {
    match command_type {
        "thread.archive" => Some("Thread archived."),
        "thread.settle" => Some("Thread settled."),
        "thread.metadata.update"
            if command.get("worktreePath").is_some()
                && command["worktreePath"] != row["worktreePath"] =>
        {
            Some("Workspace changed.")
        }
        // Only a driver that cannot switch runtime mode in its session
        // (`supportsRuntimeModeSwitchInSession`).
        "thread.runtime-mode.set" => {
            let instance = row["modelSelection"]["instanceId"]
                .as_str()
                .or(row["providerInstanceId"].as_str())
                .unwrap_or_default();
            let capabilities: Value =
                serde_json::from_str(include_str!("provider_capabilities.json"))
                    .unwrap_or_default();
            (capabilities[instance]["sessions"]["supportsRuntimeModeSwitchInSession"] == false)
                .then_some("Runtime mode changed.")
        }
        // A new provider instance releases the session (ProviderSwitchService).
        "thread.model-selection.set" | "provider.switch" => {
            let current = row["modelSelection"]["instanceId"]
                .as_str()
                .or(row["providerInstanceId"].as_str());
            let next = command["modelSelection"]["instanceId"].as_str();
            (next.is_some() && next != current).then_some(PROVIDER_CHANGED)
        }
        _ => None,
    }
}

/// Whether a run of the thread is preparing, starting, running or waiting.
async fn thread_has_active_run<H: CommandHost>(state: &Arc<H>, thread_id: &str) -> bool {
    state
        .store
        .read()
        .await
        .threads
        .get(thread_id)
        .is_some_and(|thread| {
            thread.runs.iter().any(|run| {
                matches!(
                    run.status.as_str(),
                    "preparing" | "starting" | "running" | "waiting"
                )
            })
        })
}

/// Detach the thread's live provider session, as the reference does for a
/// thread change: record it, then stop the turns it was running.
pub(crate) async fn detach_session<H: CommandHost>(
    state: &Arc<H>,
    thread_id: &str,
    reason: &str,
) -> anyhow::Result<()> {
    let (live, run_ids, codex_thread) = {
        let store = state.store.read().await;
        let Some(thread) = store.threads.get(thread_id) else {
            return Ok(());
        };
        let run_ids: Vec<Uuid> = thread
            .runs
            .iter()
            .filter(|run| run.status == "running")
            .map(|run| run.id)
            .collect();
        let row = thread.stored_row();
        let instance = row["modelSelection"]["instanceId"]
            .as_str()
            .or(row["providerInstanceId"].as_str())
            .unwrap_or(crate::workspace::provider::CODEX)
            .to_owned();
        // The native Codex thread or Claude session the thread's provider
        // process holds, which the detach unloads (Codex) or closes (Claude).
        let codex_thread = thread
            .provider
            .resume_for(crate::workspace::provider::CODEX)
            .or_else(|| {
                thread
                    .provider
                    .resume_for(crate::workspace::provider::CLAUDE_AGENT)
            })
            .or_else(|| thread.provider.resume_for(crate::workspace::provider::GROK))
            .or_else(|| {
                thread
                    .provider
                    .resume_for(crate::workspace::provider::ACP_REGISTRY)
            })
            .map(|native| (instance, native));
        (thread.provider.session_live(), run_ids, codex_thread)
    };
    if !live && run_ids.is_empty() {
        return Ok(());
    }
    state
        .commit(Event::ProviderSessionDetached {
            thread_id: thread_id.to_owned(),
            run_ids: run_ids.clone(),
            reason: reason.to_owned(),
        })
        .await?;
    let mut cancellations = state.cancellations().lock().await;
    for run_id in run_ids {
        state.detach_turn(run_id);
        if let Some(cancel) = cancellations.remove(&run_id) {
            let _ = cancel.send(());
        }
    }
    drop(cancellations);
    if let Some((instance, native)) = codex_thread {
        state.unload_provider_thread(&instance, &native);
    }
    Ok(())
}

/// Drivers whose instances are off unless enabled, as the reference decodes
/// them.
const DEFAULT_DISABLED_DRIVERS: &[&str] = &[
    "cursor",
    "grok",
    "fx",
    "kiro",
    "muse",
    "pi",
    "opencode",
    "antigravity",
];

/// Whether `instance` may run in `project`: the project's override, else the
/// instance's own setting, else its driver's default. An instance that is
/// not configured is enabled only if it is a built-in default (`codex`,
/// `claudeAgent`). The reference's `resolveProjectProviderInstanceEnabled`.
pub(crate) fn provider_instance_enabled(settings: &Value, project: &str, instance: &str) -> bool {
    let overridden =
        &settings["projectSettingsOverrides"][project]["providerInstances"][instance]["enabled"];
    if let Some(enabled) = overridden.as_bool() {
        return enabled;
    }
    let Some(config) = settings["providerInstances"]
        .as_object()
        .and_then(|instances| instances.get(instance))
    else {
        return matches!(instance, "codex" | "claudeAgent");
    };
    let own = config["enabled"].as_bool();
    let nested = config["config"]["enabled"].as_bool();
    if own == Some(false) || nested == Some(false) {
        return false;
    }
    own.or(nested).unwrap_or_else(|| {
        let driver = config["driver"].as_str().unwrap_or(instance);
        !DEFAULT_DISABLED_DRIVERS.contains(&driver)
    })
}

pub(crate) mod delegation;
pub(crate) use delegation::{
    after_remote_complete, after_run, cancel_task, delegated_task_awaits_work,
    delegated_task_result_run, delegated_task_runs, recover as recover_delegated_tasks,
    subagent_result, task_node_id, task_thread_id, watch_run_end, RunEnd,
};

/// The CLI driver a thread's provider instance runs (`codex` or
/// `claudeAgent`), when it runs one this server drives.
fn cli_driver<H: CommandHost>(state: &H, row: &Value) -> Option<String> {
    let instance = row["modelSelection"]["instanceId"]
        .as_str()
        .or(row["providerInstanceId"].as_str())?;
    let settings = state.effective_settings().ok()?;
    let driver = settings["providerInstances"][instance]["driver"]
        .as_str()
        .unwrap_or(instance);
    crate::workspace::provider::runs_turns(driver).then(|| driver.to_owned())
}

/// Whether a message's run follows the thread's model pick when it starts
/// rather than a copy from now: a scheduled delivery or (#40373) a queued
/// message that names no model, unless it is a delegated completion, a
/// monitor notification or a provider's buffered notification.
fn follows_thread_selection(command: &Value, queued: bool, wire: Option<&Value>) -> bool {
    if !command["modelSelection"].is_null() {
        return false;
    }
    let delegated = wire.is_some_and(|w| w["delegatedCompletion"].is_object());
    let notification = command.get("notification").filter(|n| n.is_object());
    let buffered = command["createdBy"] == "agent"
        && command["creationSource"] == "provider"
        && notification.is_some()
        && !delegated;
    let monitor = notification.is_some_and(|n| n["source"]["kind"] == "monitor");
    command["scheduledTaskId"].is_string() || (queued && !delegated && !monitor && !buffered)
}

#[allow(clippy::too_many_arguments)]
async fn start_run<H: CommandHost>(
    state: Arc<H>,
    receipt: &CommandReceipt,
    thread_id: &str,
    text: String,
    message_id: &Value,
    queued: bool,
    continues: Option<Uuid>,
    creation_source: Option<String>,
    scheduled_task_id: Option<String>,
    follows_thread_selection: bool,
    notification: Option<Value>,
    wire: Option<Value>,
    deferred: bool,
    output_schema: Option<Value>,
) -> anyhow::Result<u64> {
    let (cwd, needs_baseline, wire_id, row) = state
        .store
        .read()
        .await
        .threads
        .get(thread_id)
        .map(|t| {
            // The reference's id (`run:thread:<thread>:ordinal:<n>`), so a
            // run reads the same on both servers and sorts by ordinal there.
            let wire_id = Some(Run::reference_id(thread_id, t.runs.len() + 1));
            (
                t.cwd.clone(),
                t.checkpoints.is_empty(),
                wire_id,
                t.stored_row(),
            )
        })
        .ok_or_else(|| anyhow::anyhow!("thread not found"))?;
    let driver = cli_driver(&*state, &row);
    let baseline = needs_baseline.then(|| cwd.clone());
    // A run that continues one cut short still owes that run's result.
    let output_schema = match (output_schema, continues) {
        (Some(schema), _) => Some(schema),
        (None, Some(continued)) => state
            .store
            .read()
            .await
            .threads
            .get(thread_id)
            .and_then(|t| {
                t.runs
                    .iter()
                    .find(|run| run.id == continued)
                    .and_then(|run| run.output_schema.as_deref().cloned())
            }),
        (None, None) => None,
    };
    // A delegated wake queued behind the active run starts when that run
    // ends; other queued messages wait to be sent.
    let wake = wire
        .as_ref()
        .is_some_and(|wire| wire["delegatedCompletion"].is_object());
    let run = Run {
        id: wire_id
            .as_deref()
            .map_or_else(crate::platform::uuid_v4, Run::uuid_for),
        thread_id: thread_id.to_owned(),
        provider: "default".into(),
        status: if queued {
            "queued"
        } else if deferred {
            "preparing"
        } else {
            "running"
        }
        .into(),
        started_at: now(),
        finished_at: None,
        error: None,
        user_message_id: Some(message_id.as_str().unwrap_or_default().to_owned()),
        queue_held: queued && !wake,
        restart_cut: None,
        restart_continuation_of: continues,
        requeued_at: None,
        background_work: vec![],
        continuation: None,
        resend_owed: false,
        resends: 0,
        wire_id,
        driver,
        model_selection: row["modelSelection"]
            .is_object()
            .then(|| Box::new(row["modelSelection"].clone())),
        output_schema: output_schema.map(Box::new),
    };
    let message = ConversationMessage {
        id: message_id.as_str().unwrap_or_default().to_owned(),
        // The prompt that continues a run shows as a notification.
        role: if continues.is_some() || notification.is_some() {
            crate::thread_projection::NOTIFICATION_ROLE
        } else {
            "user"
        }
        .into(),
        content: text.clone(),
        created_at: now(),
        creation_source: creation_source.filter(|_| continues.is_none()),
        scheduled_task_id: scheduled_task_id.filter(|_| continues.is_none()),
        follows_thread_selection,
        notification,
        steer: None,
        wire: wire.map(Box::new),
    };
    let seq = state
        .commit_receipted(
            Event::RunQueued {
                message,
                run: run.clone(),
            },
            Some(receipt.clone()),
        )
        .await?
        .id;
    if !deferred {
        if let Some(baseline_cwd) = baseline {
            if let Some(tree) = state.workspace().capture_git_tree(&baseline_cwd).await {
                let _ = state
                    .commit(Event::CheckpointCaptured {
                        thread_id: thread_id.to_owned(),
                        checkpoint: crate::CheckpointRecord {
                            turn_count: 0,
                            tree,
                            cwd: baseline_cwd,
                            captured_at: now(),
                            ready: true,
                        },
                    })
                    .await;
            }
        }
    }
    if queued || deferred {
        return Ok(seq);
    }
    let (tx, rx) = oneshot::channel();
    state.cancellations().lock().await.insert(run.id, tx);
    state.launch_run(run, cwd, text, rx);
    Ok(seq)
}

async fn start_queued_run<H: CommandHost>(
    state: Arc<H>,
    receipt: &CommandReceipt,
    thread_id: &str,
    run_id: &str,
) -> anyhow::Result<u64> {
    start_existing_run(state, receipt, thread_id, run_id, "running").await
}

async fn start_existing_run<H: CommandHost>(
    state: Arc<H>,
    receipt: &CommandReceipt,
    thread_id: &str,
    run_id: &str,
    start_status: &str,
) -> anyhow::Result<u64> {
    let (mut run, cwd, text) = {
        let store = state.store.read().await;
        let thread = store
            .threads
            .get(thread_id)
            .ok_or_else(|| anyhow::anyhow!("thread not found"))?;
        let run = thread
            .runs
            .iter()
            .find(|candidate| {
                candidate.wire_id() == run_id
                    && (candidate.status == "queued" || candidate.status == "preparing")
            })
            .cloned()
            .ok_or_else(|| anyhow::anyhow!(format!("Run {run_id} is not queued.")))?;
        if thread
            .runs
            .iter()
            .any(|candidate| candidate.status == "running")
        {
            return Err(anyhow::anyhow!(
                "A queued user message can only be sent now while the thread is stopped."
            ));
        }
        let message_id = run
            .user_message_id
            .as_deref()
            .ok_or_else(|| anyhow::anyhow!(format!("Run {run_id} has no queued message.")))?;
        let text = thread
            .messages
            .iter()
            .find(|message| message.id == message_id)
            .map(|message| message.content.clone())
            .ok_or_else(|| anyhow::anyhow!(format!("Run {run_id} has no queued message.")))?;
        (run, thread.cwd.clone(), text)
    };
    run.status = start_status.into();
    run.queue_held = false;
    let sequence = state
        .commit_receipted(Event::RunStarted(run.clone()), Some(receipt.clone()))
        .await?
        .id;
    let (tx, rx) = oneshot::channel();
    state.cancellations().lock().await.insert(run.id, tx);
    state.launch_run(run, cwd, text, rx);
    Ok(sequence)
}

/// Send again the prompt of `run_id`, a run whose provider never took a
/// scheduled prompt before a restart cut it off, as the reference's restart
/// resend (`provider-turn.start`) does.
pub(crate) async fn resend_run<H: CommandHost>(
    state: Arc<H>,
    thread_id: &str,
    run_id: Uuid,
) -> anyhow::Result<()> {
    let (mut run, cwd, text) = {
        let store = state.store.read().await;
        let thread = store
            .threads
            .get(thread_id)
            .ok_or_else(|| anyhow::anyhow!("thread not found"))?;
        let run = thread
            .runs
            .iter()
            .find(|candidate| candidate.id == run_id && candidate.resend_owed)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!(format!("Run {run_id} is not owed a resend.")))?;
        let text = thread
            .messages
            .iter()
            .find(|message| Some(&message.id) == run.user_message_id.as_ref())
            .map(|message| message.content.clone())
            .ok_or_else(|| anyhow::anyhow!(format!("Run {run_id} has no message.")))?;
        (run, thread.cwd.clone(), text)
    };
    run.status = "running".into();
    run.resend_owed = false;
    run.resends += 1;
    run.finished_at = None;
    state.commit(Event::RunStarted(run.clone())).await?;
    let (tx, rx) = oneshot::channel();
    state.cancellations().lock().await.insert(run.id, tx);
    state.launch_run(run, cwd, text, rx);
    Ok(())
}

async fn apply_command<H: CommandHost>(
    state: Arc<H>,
    command: &Value,
    receipt: &CommandReceipt,
    continues: Option<Uuid>,
) -> anyhow::Result<u64> {
    let command_type = command
        .get("type")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default();
    let thread_id = command.get("threadId").and_then(serde_json::Value::as_str);
    match command_type {
        "link_request.record" => return record_link_request(state, command, receipt).await,
        "run.output.submit" => return submit_run_output(state, command, receipt).await,
        "secret_request.record" => return secret_request::record(state, command, receipt).await,
        "feedback_request.record" => {
            return feedback_request::record(state, command, receipt).await
        }
        "thread.create" => {
            let project_id = command
                .get("projectId")
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned)
                .ok_or_else(|| anyhow::anyhow!("thread.create requires projectId"))?;
            let cwd = state
                .store
                .read()
                .await
                .projects
                .get(&project_id)
                .map(|project| project.path.clone())
                .unwrap_or_default();
            let id = receipt.thread_id.clone();
            let thread = Thread {
                row: crate::thread_projection::created(
                    command,
                    &crate::auth_service::iso(crate::auth_service::now()),
                ),
                id,
                project_id,
                title: command
                    .get("title")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("New thread")
                    .to_owned(),
                cwd: command
                    .get("worktreePath")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or(&cwd)
                    .to_owned(),
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
                provider: Default::default(),
                stored: None,
            };
            let sequence = state
                .commit_receipted(Event::ThreadCreated(thread), Some(receipt.clone()))
                .await?
                .id;
            // The first message starts the thread's run in the same command, so
            // no client sees it committed and idle (`threadCreateInitialMessage`).
            if let Some(initial) = command.get("initialMessage").filter(|m| m.is_object()) {
                let mut message = initial.clone();
                let strategy = message
                    .as_object_mut()
                    .and_then(|m| m.remove("workspaceStrategy"));
                message["type"] = json!("message.dispatch");
                message["commandId"] = command["commandId"].clone();
                message["threadId"] = command["threadId"].clone();
                message["modelSelection"] = command["modelSelection"].clone();
                message["dispatchMode"] = match strategy {
                    Some(strategy) if !strategy.is_null() => {
                        json!({"type": "defer_start", "workspaceStrategy": strategy})
                    }
                    _ => json!({"type": "start_immediately"}),
                };
                message["createdBy"] = command["createdBy"].clone();
                message["creationSource"] = command["creationSource"].clone();
                return Box::pin(apply_command(state.clone(), &message, receipt, None)).await;
            }
            return Ok(sequence);
        }
        "message.dispatch" => {
            let thread_id =
                thread_id.ok_or_else(|| anyhow::anyhow!("message.dispatch requires threadId"))?;
            let mut text = command
                .get("text")
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default()
                .to_owned();
            let mut queue_steer = false;
            // A message asked for a result must be one the turn can answer
            // (`outputRequestProblem`), and never joins a running turn: the
            // result is asked of one whole turn.
            let asks_output = command.get("outputSchema").is_some_and(Value::is_object);
            if asks_output {
                let attachments = command["attachments"].as_array().map_or(0, Vec::len);
                if let Some(problem) =
                    crate::run_output::request_problem(&command["outputSchema"], &text, attachments)
                {
                    return Err(anyhow::anyhow!(problem));
                }
                if command["dispatchMode"]["type"] == "steer_active" {
                    queue_steer = true;
                }
            }
            if command["dispatchMode"]["type"] == "steer_active" && !asks_output {
                match steer::plan(&state, thread_id, command).await? {
                    steer::SteerPlan::Start => {}
                    steer::SteerPlan::Queue => queue_steer = true,
                    steer::SteerPlan::Steer(target) => {
                        return steer::steer(state.clone(), receipt, thread_id, command, target)
                            .await;
                    }
                }
            }
            // A delivery whose tasks all wake the parent steers into its
            // running turn when it can (see `delivery_steer_target`).
            let delivery_steer = match command.get("delegatedCompletion").filter(|c| c.is_object())
            {
                Some(completion) => delivery_steer_target(&state, thread_id, completion).await,
                None => None,
            };
            // A delegated task delivery says what each task found.
            let mut wire = None;
            let mut wake_started = None;
            if let Some(completion) = command.get("delegatedCompletion").filter(|c| c.is_object()) {
                let tasks = state
                    .store
                    .read()
                    .await
                    .threads
                    .get(thread_id)
                    .map(|t| t.stored_row()["subagents"].clone())
                    .and_then(|t| t.as_array().cloned())
                    .unwrap_or_default();
                let ids = completion["taskIds"]
                    .as_array()
                    .cloned()
                    .unwrap_or_default();
                text = delegation::wake_detail(&ids, &tasks);
                if let Some(target) = delivery_steer {
                    let delivery = steer::Delivery {
                        text: text.clone(),
                        notification: command["notification"].clone(),
                        wire: json!({
                            "createdBy": command["createdBy"],
                            "creationSource": command["creationSource"],
                            "delegatedCompletion": completion,
                        }),
                    };
                    let sequence = steer::steer_with(
                        state.clone(),
                        receipt,
                        thread_id,
                        command,
                        target,
                        Some(delivery),
                    )
                    .await?;
                    // The steered message accepts its delivery once sent
                    // (`notification.delivery.accept`); a wake run's settles
                    // when the run ends (`finalize_delivery`).
                    delegation::accept_delivery(&state, thread_id, completion).await?;
                    return Ok(sequence);
                }
                // A host whose runs settle no deliveries as they end accepts
                // one with its message.
                if !state.hooks_run_ends() {
                    delegation::accept_delivery(&state, thread_id, completion).await?;
                }
                wake_started = delegation::wake_work_started_at(&*state, thread_id).await;
                wire = Some(json!({
                    "createdBy": command["createdBy"],
                    "creationSource": command["creationSource"],
                    "delegatedCompletion": completion,
                }));
            }
            // An agent's message names the thread that sent it.
            if wire.is_none()
                && command["createdBy"] == "agent"
                && command["senderThreadId"].is_string()
            {
                wire = Some(json!({
                    "createdBy": command["createdBy"],
                    "creationSource": command["creationSource"],
                    "senderThreadId": command["senderThreadId"],
                }));
            }
            // The message keeps its attachments, already claimed for this
            // thread (`ThreadMessageIntake`), on its row and turn item.
            if let Some(attachments) = command["attachments"]
                .as_array()
                .filter(|attachments| !attachments.is_empty())
            {
                let fields = wire.get_or_insert_with(|| json!({}));
                fields["attachments"] = json!(attachments);
            }
            let queued = (command["dispatchMode"]["type"] == "queue_after_active" || queue_steer)
                && state
                    .store
                    .read()
                    .await
                    .threads
                    .get(thread_id)
                    .is_some_and(|thread| thread.runs.iter().any(|run| run.status == "running"));
            let title_effect = state
                .store
                .read()
                .await
                .threads
                .get(thread_id)
                .filter(|thread| {
                    continues.is_none()
                        && crate::title_generation::initial_title_requested(
                            &thread.full_projection(),
                            command,
                        )
                })
                .map(|_| {
                    crate::title_generation::TitleEffect::new(
                        &receipt.command_id,
                        thread_id,
                        json!({"type": "initial", "messageId": command["messageId"]}),
                    )
                });
            // As the reference's orchestrator, the request is recorded
            // before the message's run.
            if let Some(effect) = &title_effect {
                let at = crate::auth_service::iso(crate::auth_service::now());
                state
                    .commit(Event::ThreadPatched {
                        thread_id: thread_id.to_owned(),
                        patch: crate::title_generation::initial_title_patch(command, effect, &at),
                    })
                    .await?;
            }
            if let Some(at) = wake_started {
                delegation::record_work_started_at(&*state, thread_id, at).await?;
            }
            // A message that picks another model switches the thread to it
            // before its run, as the reference's dispatch does
            // (`thread.model-selection-updated` / `thread.provider-switched`).
            if !queued && command["modelSelection"].is_object() {
                let current = state
                    .store
                    .read()
                    .await
                    .threads
                    .get(thread_id)
                    .map(|thread| thread.stored_row()["modelSelection"].clone())
                    .unwrap_or_default();
                if !crate::model_manifest::selections_equal(&current, &command["modelSelection"]) {
                    let selection = &command["modelSelection"];
                    let reason = detach_reason(
                        "thread.model-selection.set",
                        command,
                        &json!({"modelSelection": current}),
                    );
                    let at = crate::auth_service::iso(crate::auth_service::now());
                    let row = json!({"updatedAt": at, "modelSelection": selection,
                        "providerInstanceId": selection["instanceId"]});
                    state
                        .commit(Event::ThreadPatched {
                            thread_id: thread_id.to_owned(),
                            patch: json!({"row": row}),
                        })
                        .await?;
                    if let Some(reason) = reason {
                        detach_session(&state, thread_id, reason).await?;
                    }
                }
            }
            let sequence = start_run(
                state.clone(),
                receipt,
                thread_id,
                text,
                &command["messageId"],
                queued,
                continues,
                command["creationSource"]
                    .as_str()
                    .filter(|source| *source != "web")
                    .map(str::to_owned),
                command["scheduledTaskId"].as_str().map(str::to_owned),
                follows_thread_selection(command, queued, wire.as_ref()),
                // A delegated task delivery shows as its notification.
                wire.as_ref().and_then(|_| {
                    command
                        .get("notification")
                        .filter(|n| n.is_object())
                        .cloned()
                }),
                wire,
                command["dispatchMode"]["type"] == "defer_start",
                command
                    .get("outputSchema")
                    .filter(|schema| schema.is_object())
                    .cloned(),
            )
            .await?;
            // A message to a delegated child whose task finished starts a follow-up task.
            delegation::record_followup(&state, thread_id, command).await?;
            // Its effect runs once the message it titles is recorded.
            if let Some(effect) = title_effect {
                state.clone().generate_title(effect);
            }
            return Ok(sequence);
        }
        "thread.stop" | "run.interrupt" | "subagent.stop" => {
            if command_type == "subagent.stop" {
                if let Some(sequence) = delegation::stop_app_owned(&state, command, receipt).await?
                {
                    return Ok(sequence);
                }
            }
            if let Some(thread_id) = thread_id {
                let (run_ids, wire_ids) = state
                    .store
                    .read()
                    .await
                    .threads
                    .get(thread_id)
                    .map(|thread| {
                        let running = thread.runs.iter().filter(|run| run.status == "running");
                        (
                            running.clone().map(|run| run.id).collect::<Vec<_>>(),
                            running.map(|run| run.wire_id()).collect::<Vec<_>>(),
                        )
                    })
                    .unwrap_or_default();
                let requested: Vec<String> = run_ids.iter().map(Uuid::to_string).collect();
                // The request is recorded before the runs are told to stop, so
                // its card comes before what the stop ends, as the
                // reference's does.
                let sequence = state
                    .commit_receipted(
                        Event::ThreadPatched {
                            thread_id: thread_id.to_owned(),
                            // Clients stop without a reason; agents always give one.
                            patch: serde_json::json!({"status":"cancelled","interruptRequested":requested,
                                "interruptReason": command["reason"].as_str().unwrap_or(USER_STOP_REASON)}),
                        },
                        Some(receipt.clone()),
                    )
                    .await?
                    .id;
                // The stopped runs' delegated results wake nobody.
                delegation::stop_cohorts(
                    &state,
                    thread_id,
                    &wire_ids,
                    command_type == "thread.stop",
                )
                .await?;
                // A stop's delegated tasks stop once its runs have ended,
                // where the host can wait for that.
                if command_type == "thread.stop"
                    && command["stopDelegatedTasks"] != false
                    && !run_ids.is_empty()
                    && state.hooks_run_ends()
                {
                    delegation::defer_child_stop(
                        thread_id,
                        &receipt.command_id,
                        command["reason"].as_str().map(str::to_owned),
                    );
                }
                let mut cancellations = state.cancellations().lock().await;
                for run_id in run_ids {
                    if let Some(cancel) = cancellations.remove(&run_id) {
                        let _ = cancel.send(());
                    }
                }
                return Ok(sequence);
            }
        }
        "thread.pull-request-watch.checks-reopen" => {
            let id = thread_id.ok_or_else(|| anyhow::anyhow!("Missing thread id."))?;
            let store = state.store.read().await;
            let thread = store
                .threads
                .get(id)
                .ok_or_else(|| anyhow::anyhow!("thread not found"))?;
            let mut row = thread.stored_row();
            if let Some(links) = row["pullRequests"].as_array_mut() {
                for link in links {
                    if let Some(watch) = link.get_mut("watch").and_then(Value::as_object_mut) {
                        watch.remove("checksPending");
                    }
                }
            }
            drop(store);
            return Ok(state
                .commit_receipted(
                    Event::ThreadPatched {
                        thread_id: id.to_owned(),
                        patch: json!({"row":row}),
                    },
                    Some(receipt.clone()),
                )
                .await?
                .id);
        }
        "thread.pull-request-watch.sync" => {
            let id = thread_id.ok_or_else(|| anyhow::anyhow!("Missing thread id."))?;
            let store = state.store.read().await;
            let thread = store
                .threads
                .get(id)
                .ok_or_else(|| anyhow::anyhow!("thread not found"))?;
            let mut row = thread.stored_row();
            let host = command["host"].as_str().unwrap_or_default();
            let repository = command["repository"].as_str().unwrap_or_default();
            let number = command["number"].clone();
            let Some(link) = row["pullRequests"].as_array_mut().and_then(|links| {
                links.iter_mut().find(|link| {
                    link["host"]
                        .as_str()
                        .unwrap_or_default()
                        .eq_ignore_ascii_case(host)
                        && link["repository"]
                            .as_str()
                            .unwrap_or_default()
                            .eq_ignore_ascii_case(repository)
                        && link["number"] == number
                })
            }) else {
                drop(store);
                return Ok(state
                    .commit_receipted(
                        Event::CommandDispatched {
                            command_id: receipt.command_id.clone(),
                            command_type: command_type.into(),
                            thread_id: Some(id.to_owned()),
                            payload: command.clone(),
                        },
                        Some(receipt.clone()),
                    )
                    .await?
                    .id);
            };
            let existing_watch = link["watch"].clone();
            if existing_watch["startedAt"] != command["startedAt"] {
                drop(store);
                return Ok(state
                    .commit_receipted(
                        Event::CommandDispatched {
                            command_id: receipt.command_id.clone(),
                            command_type: command_type.into(),
                            thread_id: Some(id.to_owned()),
                            payload: command.clone(),
                        },
                        Some(receipt.clone()),
                    )
                    .await?
                    .id);
            }
            if command["watch"].is_null() {
                let ended = command
                    .get("endedWatch")
                    .cloned()
                    .or_else(|| (!existing_watch.is_null()).then_some(existing_watch));
                link.as_object_mut().unwrap().remove("watch");
                if let Some(ended) = ended {
                    link["endedWatch"] = ended;
                }
            } else {
                let mut watch = command["watch"].clone();
                if command.get("checksPendingBefore").is_none()
                    || existing_watch.get("checksPending").is_some()
                {
                    link["watch"] = watch;
                } else if let Some(object) = watch.as_object_mut() {
                    object.remove("checksPending");
                    link["watch"] = Value::Object(object.clone());
                }
                link.as_object_mut().unwrap().remove("endedWatch");
            }
            drop(store);
            return Ok(state
                .commit_receipted(
                    Event::ThreadPatched {
                        thread_id: id.to_owned(),
                        patch: json!({"row":row}),
                    },
                    Some(receipt.clone()),
                )
                .await?
                .id);
        }
        "thread.issue.link"
        | "thread.issue.unlink"
        | "thread.pull-request.link"
        | "thread.pull-request.unlink"
        | "thread.pull-request.watch"
        | "thread.pull-request.sync"
        | "thread.pull-request-link.sync" => {
            let id = &receipt.thread_id;
            let store = state.store.read().await;
            let thread = store
                .threads
                .get(id)
                .ok_or_else(|| anyhow::anyhow!("thread not found"))?;
            let at = crate::auth_service::iso(crate::auth_service::now());
            let row = if command_type == "thread.pull-request.watch" {
                crate::thread_projection::watch_patch(thread, command, &at)
            } else if command_type == "thread.pull-request.sync" {
                let mut row = thread.stored_row();
                row["branchPullRequest"] = command["branchPullRequest"].clone();
                if command.get("linkedPullRequest").is_some() {
                    row["linkedPullRequest"] = command["linkedPullRequest"].clone();
                }
                row
            } else {
                crate::thread_projection::link_patch(thread, command, &at)
            };
            drop(store);
            return Ok(state
                .commit_receipted(
                    Event::ThreadPatched {
                        thread_id: id.clone(),
                        patch: json!({"row":row}),
                    },
                    Some(receipt.clone()),
                )
                .await?
                .id);
        }
        "thread.guest-share.close" | "thread.guest-message.dismiss" => {
            let id = thread_id.ok_or_else(|| anyhow::anyhow!("Missing thread id."))?;
            if command_type == "thread.guest-share.close" {
                state.close_guest_thread(id)
            } else {
                state.resolve_guest_message(
                    id,
                    command["guestMessageId"].as_str().unwrap_or(""),
                    "dismissed",
                )
            }
            .map_err(anyhow::Error::msg)?;
        }
        "thread.guest-share.open" | "thread.guest-share.redeem" | "thread.guest-message.submit" => {
            let id = thread_id.ok_or_else(|| anyhow::anyhow!("Missing thread id."))?;
            let mut row = state
                .store
                .read()
                .await
                .threads
                .get(id)
                .ok_or_else(|| anyhow::anyhow!("thread not found"))?
                .stored_row();
            match command_type {
                "thread.guest-share.open" => {
                    row["guestShare"] = json!({"id":command["shareId"],"host":command["host"],"createdAt":crate::auth_service::iso(crate::auth_service::now()),"expiresAt":command["expiresAt"],"guestName":Value::Null,"messages":[]});
                }
                "thread.guest-share.redeem" => {
                    if !row["guestShare"].is_object() {
                        return Err(anyhow::anyhow!("Guest share is not open."));
                    }
                    row["guestShare"]["guestName"] = command["guestName"].clone();
                }
                _ => {
                    let mut messages = row["guestShare"]["messages"]
                        .as_array()
                        .cloned()
                        .unwrap_or_default();
                    let mut message = json!({"id":command["guestMessageId"],"text":command["text"],"status":"pending","submittedAt":crate::auth_service::iso(crate::auth_service::now())});
                    if let Some(attachments) = command.get("attachments") {
                        message["attachments"] = attachments.clone();
                    }
                    messages.push(message);
                    row["guestShare"]["messages"] = json!(messages);
                }
            }
            return Ok(state
                .commit_receipted(
                    Event::ThreadPatched {
                        thread_id: id.to_owned(),
                        patch: json!({"row":row}),
                    },
                    Some(receipt.clone()),
                )
                .await?
                .id);
        }
        "thread.project.adopt-folder" => {
            let id = thread_id.ok_or_else(|| anyhow::anyhow!("Missing thread id."))?;
            let project_id = command["projectId"].as_str().unwrap_or_default();
            let store = state.store.read().await;
            let project = store
                .projects
                .get(project_id)
                .ok_or_else(|| anyhow::anyhow!(format!("Project {project_id} was not found.")))?;
            let thread = store
                .threads
                .get(id)
                .ok_or_else(|| anyhow::anyhow!("thread not found"))?;
            if thread
                .runs
                .iter()
                .any(|run| matches!(run.status.as_str(), "preparing" | "starting" | "running"))
            {
                return Err(anyhow::anyhow!(format!(
                    "Thread {id} still has work in progress. Finish or stop it first."
                )));
            }
            if thread.cwd != project.path {
                return Err(anyhow::anyhow!(format!(
                    "Thread {id} does not work in {}.",
                    project.path
                )));
            }
            let mut row = thread.stored_row();
            row["projectId"] = json!(project_id);
            row["worktreePath"] = Value::Null;
            drop(store);
            return Ok(state
                .commit_receipted(
                    Event::ThreadPatched {
                        thread_id: id.to_owned(),
                        patch: json!({"row":row}),
                    },
                    Some(receipt.clone()),
                )
                .await?
                .id);
        }
        "thread.archive"
        | "thread.unarchive"
        | "thread.auto-settle"
        | "thread.settle"
        | "thread.unsettle"
        | "thread.pin"
        | "thread.unpin"
        | "thread.mark-unread"
        | "thread.visit"
        | "thread.snooze"
        | "thread.unsnooze"
        | "thread.metadata.update"
        | "thread.title.regeneration.complete"
        | "thread.runtime-mode.set"
        | "thread.interaction-mode.set"
        | "thread.model-selection.set"
        | "thread.tag.set"
        | "thread.auto-settle.set"
        | "thread.pin.reorder"
        | "thread.active.reorder"
        | "provider.switch" => {
            if let Some(thread_id) = thread_id {
                let current = state.store.read().await;
                let existing_thread = current.threads.get(thread_id);
                let existing_row = existing_thread
                    .map(|thread| thread.stored_row())
                    .unwrap_or_else(|| json!({}));
                if command_type == "thread.metadata.update" {
                    if let Some(expected) = command.get("expectedWorktreePath") {
                        if expected != &existing_row["worktreePath"] {
                            return Err(anyhow::anyhow!(format!(
                                "Thread {thread_id} worktree changed before the metadata update could be applied."
                            )));
                        }
                    }
                    if let Some(expected) = command.get("expectedBranch") {
                        if expected != &existing_row["branch"] {
                            return Err(anyhow::anyhow!(format!(
                                "Thread {thread_id} branch changed before the metadata update could be applied."
                            )));
                        }
                    }
                    if command["expectedEmpty"] == true
                        && existing_thread.is_some_and(|thread| {
                            !thread.messages.is_empty() || !thread.runs.is_empty()
                        })
                    {
                        return Err(anyhow::anyhow!(format!(
                            "Thread {thread_id} is no longer empty."
                        )));
                    }
                }
                drop(current);
                let mut patch = serde_json::Map::new();
                let at = json!(crate::auth_service::iso(crate::auth_service::now()));
                let existing_title_regeneration = state
                    .store
                    .read()
                    .await
                    .threads
                    .get(thread_id)
                    .map(|thread| thread.stored_row()["titleRegeneration"].clone())
                    .unwrap_or(Value::Null);
                let existing_row = state
                    .store
                    .read()
                    .await
                    .threads
                    .get(thread_id)
                    .map(|thread| thread.stored_row())
                    .unwrap_or_else(|| json!({}));
                let mut row = json!({"updatedAt":at});
                let mut title_effect = None;
                match command_type {
                    "thread.archive" => {
                        patch.insert("archived".into(), true.into());
                        row["archivedAt"] = at;
                        row["titleRegeneration"] = Value::Null;
                        row["guestShare"] = Value::Null;
                    }
                    "thread.unarchive" => {
                        patch.insert("archived".into(), false.into());
                        row["archivedAt"] = Value::Null;
                    }
                    "thread.auto-settle" => {
                        let snapshot = command["snapshotAt"].as_str().unwrap_or_default();
                        let changed_at = state
                            .store
                            .read()
                            .await
                            .threads
                            .get(thread_id)
                            .map(|thread| {
                                thread.stored_row()["updatedAt"]
                                    .as_str()
                                    .unwrap_or_default()
                                    .to_owned()
                            })
                            .unwrap_or_default();
                        if !snapshot.is_empty() && changed_at.as_str() > snapshot {
                            return Err(anyhow::anyhow!(format!(
                                "Thread {thread_id} changed before automatic settlement."
                            )));
                        }
                        patch.insert("settled".into(), true.into());
                        patch.insert("pinned".into(), false.into());
                        row["settledOverride"] = "settled".into();
                        row["settledAt"] = command.get("settledAt").cloned().unwrap_or(at.clone());
                        row["unsettledAt"] = Value::Null;
                        row["pinnedAt"] = Value::Null;
                        row["pinOrderKey"] = Value::Null;
                        row["activeOrderKey"] = Value::Null;
                        row["snoozedUntil"] = Value::Null;
                        row["snoozedAt"] = Value::Null;
                    }
                    "thread.settle" => {
                        patch.insert("settled".into(), true.into());
                        patch.insert("pinned".into(), false.into());
                        row["settledOverride"] = "settled".into();
                        row["settledAt"] = command.get("settledAt").cloned().unwrap_or(at);
                        for k in [
                            "unsettledAt",
                            "pinnedAt",
                            "pinOrderKey",
                            "activeOrderKey",
                            "snoozedUntil",
                            "snoozedAt",
                        ] {
                            row[k] = Value::Null;
                        }
                    }
                    "thread.unsettle" => {
                        patch.insert("settled".into(), false.into());
                        row["settledOverride"] = "active".into();
                        row["settledAt"] = Value::Null;
                        row["unsettledAt"] = at;
                    }
                    "thread.pin" => {
                        patch.insert("pinned".into(), true.into());
                        // Pinning wakes a snoozed thread, as unsnoozing does.
                        if !existing_row["snoozedUntil"].is_null() {
                            row["lastSnoozeWakeAt"] = at.clone();
                        }
                        row["pinnedAt"] = at;
                        row["settledOverride"] = if existing_row["settledOverride"] == "settled" {
                            Value::String("active".into())
                        } else {
                            existing_row["settledOverride"].clone()
                        };
                        if row["settledOverride"] == "active" {
                            row["settledAt"] = Value::Null;
                            row["unsettledAt"] =
                                json!(crate::auth_service::iso(crate::auth_service::now()));
                        }
                        row["snoozedUntil"] = Value::Null;
                        row["snoozedAt"] = Value::Null;
                        if let Some(key) = command.get("orderKey") {
                            row["pinOrderKey"] = key.clone();
                        }
                    }
                    "thread.unpin" => {
                        patch.insert("pinned".into(), false.into());
                        row["pinnedAt"] = Value::Null;
                        row["pinOrderKey"] = Value::Null;
                    }
                    "thread.mark-unread" => {
                        patch.insert("unread".into(), true.into());
                        if let Some(completed) = state
                            .store
                            .read()
                            .await
                            .threads
                            .get(thread_id)
                            .and_then(|thread| {
                                thread.runs.iter().filter_map(|run| run.finished_at).max()
                            })
                        {
                            row["lastVisitedAt"] = json!(crate::auth_service::iso(
                                (completed as i64).saturating_mul(1000).saturating_sub(1)
                            ));
                        }
                    }
                    "thread.snooze" => {
                        let snoozed_until =
                            command.get("snoozedUntil").cloned().unwrap_or(Value::Null);
                        patch.insert("snoozed_until".into(), snoozed_until.clone());
                        row["snoozedUntil"] = snoozed_until;
                        row["snoozedAt"] = at.clone();
                        // A snooze the user chose ends any snooze a usage limit asked for.
                        row["limitRecovery"] = match &existing_row["limitRecovery"] {
                            Value::Object(recovery) => {
                                let mut recovery = recovery.clone();
                                recovery.insert("snooze".into(), false.into());
                                Value::Object(recovery)
                            }
                            _ => Value::Null,
                        };
                    }
                    "thread.unsnooze" => {
                        patch.insert("snoozed_until".into(), serde_json::Value::Null);
                        if !existing_row["snoozedUntil"].is_null() {
                            row["lastSnoozeWakeAt"] = at.clone();
                        }
                        row["snoozedUntil"] = Value::Null;
                        row["snoozedAt"] = Value::Null;
                    }
                    "thread.visit" => {
                        if let Some(visited_at) = command.get("visitedAt") {
                            row["lastVisitedAt"] = visited_at.clone();
                        }
                        patch.insert("unread".into(), false.into());
                    }
                    "thread.title.regeneration.complete" => {
                        let request_id = command["requestId"].as_str().unwrap_or_default();
                        if existing_title_regeneration["requestId"] == request_id {
                            if let Some(title) = command.get("title") {
                                row["title"] = title.clone();
                                row["titleRenamed"] = false.into();
                                patch.insert("title".into(), title.clone());
                            }
                            if let Some(outcome) = command.get("outcome") {
                                // `{...outcome, requestId, completedAt}`.
                                let mut last = outcome.as_object().cloned().unwrap_or_default();
                                last.insert("requestId".into(), json!(request_id));
                                last.insert("completedAt".into(), at.clone());
                                row["lastTitleRegeneration"] = Value::Object(last);
                            }
                            row["titleRegeneration"] = Value::Null;
                        }
                    }
                    "thread.metadata.update" => {
                        if let Some(title) = command.get("title") {
                            patch.insert("title".into(), title.clone());
                            row["title"] = title.clone();
                            row["titleRenamed"] = true.into();
                            row["titleRegeneration"] = Value::Null;
                        }
                        // Turns run in the thread's worktree, or its project's folder without one.
                        if let Some(path) = command.get("worktreePath") {
                            let cwd = match path.as_str() {
                                Some(path) => Some(path.to_owned()),
                                None => {
                                    let store = state.store.read().await;
                                    store
                                        .threads
                                        .get(thread_id)
                                        .and_then(|thread| store.projects.get(&thread.project_id))
                                        .map(|project| project.path.clone())
                                }
                            };
                            if let Some(cwd) = cwd {
                                patch.insert("cwd".into(), json!(cwd));
                            }
                        }
                        for field in ["branch", "worktreePath", "linkedPullRequest"] {
                            if let Some(value) = command.get(field) {
                                row[field] = value.clone();
                                if field == "linkedPullRequest" {
                                    patch.insert(field.into(), value.clone());
                                    let mut links = existing_row["pullRequests"]
                                        .as_array()
                                        .cloned()
                                        .unwrap_or_default();
                                    if value.is_null() {
                                        if let Some(previous) =
                                            existing_row["linkedPullRequest"].as_object()
                                        {
                                            let repository =
                                                previous["repository"].as_str().unwrap_or_default();
                                            let number =
                                                previous["number"].as_i64().unwrap_or_default();
                                            links.retain(|link| {
                                                link["repository"] != repository
                                                    || link["number"] != number
                                            });
                                        }
                                    } else if let Some(linked) = value.as_object() {
                                        let repository =
                                            linked["repository"].as_str().unwrap_or_default();
                                        let number = linked["number"].as_i64().unwrap_or_default();
                                        links.retain(|link| {
                                            link["repository"] != repository
                                                || link["number"] != number
                                        });
                                        let host = linked["url"]
                                            .as_str()
                                            .and_then(|url| url::Url::parse(url).ok())
                                            .and_then(|url| url.host_str().map(str::to_owned))
                                            .unwrap_or_else(|| "github.com".into());
                                        links.push(json!({
                                            "host":host,"repository":repository,"number":number,
                                            "url":linked["url"],"source":"manual","linkedAt":at,
                                            "snapshot":null,"stack":null
                                        }));
                                    }
                                    row["pullRequests"] = json!(links);
                                }
                            }
                        }
                        if command.get("regenerateTitle") == Some(&Value::Bool(true)) {
                            row["titleRegeneration"] = json!({
                                "requestId": command["commandId"],
                                "startedAt": at
                            });
                            let effect = crate::title_generation::TitleEffect::new(
                                &receipt.command_id,
                                thread_id,
                                json!({"type": "regenerate"}),
                            );
                            patch.insert("titleGeneration".into(), effect.patch_field());
                            title_effect = Some(effect);
                        } else if command.get("regenerateTitle") == Some(&Value::Bool(false)) {
                            row["titleRegeneration"] = Value::Null;
                        }
                    }
                    "thread.runtime-mode.set" => {
                        row["runtimeMode"] = command["runtimeMode"].clone()
                    }
                    "thread.interaction-mode.set" => {
                        row["interactionMode"] = command["interactionMode"].clone()
                    }
                    "thread.model-selection.set" | "provider.switch" => {
                        row["modelSelection"] = command["modelSelection"].clone();
                        row["providerInstanceId"] = command["modelSelection"]["instanceId"].clone();
                    }
                    "thread.auto-settle.set" => {
                        row["autoSettleDisabledAt"] = if command["enabled"] == true {
                            Value::Null
                        } else {
                            at
                        }
                    }
                    "thread.pin.reorder" => row["pinOrderKey"] = command["orderKey"].clone(),
                    "thread.active.reorder" => row["activeOrderKey"] = command["orderKey"].clone(),
                    "thread.tag.set" => {
                        let tag = command.get("tag").and_then(serde_json::Value::as_str);
                        let tagged = command
                            .get("tagged")
                            .and_then(serde_json::Value::as_bool)
                            .unwrap_or(false);
                        let mut tags = state
                            .store
                            .read()
                            .await
                            .threads
                            .get(thread_id)
                            .map(|thread| thread.tags.clone())
                            .unwrap_or_default();
                        if let Some(tag) = tag {
                            if tagged && !tags.iter().any(|value| value == tag) {
                                tags.push(tag.into());
                            }
                            if !tagged {
                                tags.retain(|value| value != tag);
                            }
                        }
                        row["tags"] = json!(tags);
                        patch.insert("tags".into(), serde_json::json!(tags));
                    }
                    _ => {}
                }
                patch.insert("row".into(), row);
                let reason = detach_reason(command_type, command, &existing_row);
                if let Some(reason) = reason {
                    if reason == PROVIDER_CHANGED && thread_has_active_run(&state, thread_id).await
                    {
                        return Err(anyhow::anyhow!(format!(
                            "Thread {thread_id} has an active turn. Wait for it to finish, then retry the provider selection change."
                        )));
                    }
                }
                let sequence = state
                    .commit_receipted(
                        Event::ThreadPatched {
                            thread_id: thread_id.to_owned(),
                            patch: patch.into(),
                        },
                        Some(receipt.clone()),
                    )
                    .await?
                    .id;
                if let Some(reason) = reason {
                    detach_session(&state, thread_id, reason).await?;
                }
                if let Some(effect) = title_effect {
                    state.clone().generate_title(effect);
                }
                return Ok(sequence);
            }
        }
        "thread.delete" => {
            if let Some(thread_id) = thread_id {
                if command["onlyIfUnused"] == true {
                    let used =
                        state
                            .store
                            .read()
                            .await
                            .threads
                            .get(thread_id)
                            .is_some_and(|thread| {
                                !thread.messages.is_empty() || !thread.runs.is_empty()
                            });
                    if used {
                        return Ok(state
                            .commit_receipted(
                                Event::CommandDispatched {
                                    command_id: receipt.command_id.clone(),
                                    command_type: command_type.into(),
                                    thread_id: Some(thread_id.to_owned()),
                                    payload: command.clone(),
                                },
                                Some(receipt.clone()),
                            )
                            .await?
                            .id);
                    }
                }
                return Ok(state
                    .commit_receipted(
                        Event::ThreadDeleted {
                            thread_id: thread_id.to_owned(),
                        },
                        Some(receipt.clone()),
                    )
                    .await?
                    .id);
            }
        }
        "prepared-run.progress" => {
            let run_id = command["runId"].as_str().unwrap_or_default();
            let valid = state
                .store
                .read()
                .await
                .threads
                .get(thread_id.unwrap_or_default())
                .and_then(|thread| thread.runs.iter().find(|run| run.wire_id() == run_id))
                .is_some_and(|run| run.status == "preparing");
            if !valid {
                return Err(anyhow::anyhow!(format!(
                    "Run {run_id} is not awaiting workspace preparation."
                )));
            }
            return Ok(state
                .commit_receipted(
                    Event::CommandDispatched {
                        command_id: receipt.command_id.clone(),
                        command_type: command_type.into(),
                        thread_id: Some(thread_id.unwrap_or_default().to_owned()),
                        payload: command.clone(),
                    },
                    Some(receipt.clone()),
                )
                .await?
                .id);
        }
        "prepared-run.release" => {
            let run_id = command["runId"].as_str().unwrap_or_default();
            let valid = state
                .store
                .read()
                .await
                .threads
                .get(thread_id.unwrap_or_default())
                .and_then(|thread| thread.runs.iter().find(|run| run.wire_id() == run_id))
                .is_some_and(|run| run.status == "preparing");
            if !valid {
                return Err(anyhow::anyhow!(format!(
                    "Run {run_id} is not awaiting workspace preparation."
                )));
            }
            return start_existing_run(
                state,
                receipt,
                thread_id.unwrap_or_default(),
                run_id,
                "starting",
            )
            .await;
        }
        "prepared-run.fail" => {
            let run_id = command["runId"].as_str().unwrap_or_default();
            let thread_id = thread_id.unwrap_or_default();
            let valid = state
                .store
                .read()
                .await
                .threads
                .get(thread_id)
                .and_then(|thread| thread.runs.iter().find(|run| run.wire_id() == run_id))
                .is_some_and(|run| run.status == "preparing");
            if !valid {
                return Err(anyhow::anyhow!(format!(
                    "Run {run_id} is not awaiting workspace preparation."
                )));
            }
            return Ok(state
                .commit_receipted(
                    Event::RunFinished {
                        run_id: Run::uuid_for(run_id),
                        thread_id: thread_id.to_owned(),
                        status: "failed".into(),
                        error: command["failure"]["message"].as_str().map(str::to_owned),
                    },
                    Some(receipt.clone()),
                )
                .await?
                .id);
        }
        "prepared-run.retry" => {
            let run_id = command["runId"].as_str().unwrap_or_default();
            let thread_id = thread_id.unwrap_or_default();
            let current = state
                .store
                .read()
                .await
                .threads
                .get(thread_id)
                .and_then(|thread| thread.runs.iter().find(|run| run.wire_id() == run_id))
                .cloned();
            let Some(mut run) = current else {
                return Err(anyhow::anyhow!(format!(
                    "Run {run_id} has no failed workspace preparation to retry."
                )));
            };
            if run.status != "failed" {
                return Err(anyhow::anyhow!(format!(
                    "Run {run_id} has no failed workspace preparation to retry."
                )));
            }
            run.status = "preparing".into();
            run.finished_at = None;
            run.error = None;
            return Ok(state
                .commit_receipted(Event::RunStarted(run), Some(receipt.clone()))
                .await?
                .id);
        }
        "provider-session.detach" => {
            let provider_session_id = command["providerSessionId"].as_str().unwrap_or_default();
            let thread_id = thread_id.unwrap_or_default();
            return Err(anyhow::anyhow!(format!(
                "Provider session {provider_session_id} does not belong to thread {thread_id}."
            )));
        }
        "queued-run.cancel" => {
            let run_id = command["runId"].as_str().unwrap_or_default();
            let valid = state
                .store
                .read()
                .await
                .threads
                .get(thread_id.unwrap_or_default())
                .and_then(|thread| thread.runs.iter().find(|run| run.wire_id() == run_id))
                .is_some_and(|run| run.status == "queued");
            if !valid {
                return Err(anyhow::anyhow!(format!("Run {run_id} is not queued.")));
            }
            return Ok(state
                .commit_receipted(
                    Event::RunFinished {
                        run_id: Run::uuid_for(run_id),
                        thread_id: thread_id.unwrap_or_default().to_owned(),
                        status: "cancelled".into(),
                        error: None,
                    },
                    Some(receipt.clone()),
                )
                .await?
                .id);
        }
        "queued-run.edit" => {
            let run_id = command["runId"].as_str().unwrap_or_default();
            let Some((message_id, thread_id, wire)) = state
                .store
                .read()
                .await
                .threads
                .get(thread_id.unwrap_or_default())
                .and_then(|thread| {
                    let run = thread
                        .runs
                        .iter()
                        .find(|run| run.wire_id() == run_id)
                        .filter(|run| run.status == "queued")?;
                    let message_id = run.user_message_id.clone()?;
                    let wire = thread
                        .messages
                        .iter()
                        .find(|message| message.id == message_id)
                        .and_then(|message| message.wire.as_deref().cloned());
                    Some((message_id, thread_id.unwrap_or_default().to_owned(), wire))
                })
            else {
                return Err(anyhow::anyhow!(format!("Run {run_id} is not queued.")));
            };
            // The edit's attachments replace the queued ones; absent keeps them.
            let wire = command
                .get("attachments")
                .filter(|a| a.is_array())
                .map(|attachments| {
                    let mut wire = wire.unwrap_or_else(|| json!({}));
                    wire["attachments"] = attachments.clone();
                    wire
                });
            return Ok(state
                .commit_receipted(
                    Event::MessageEdited {
                        thread_id,
                        message_id,
                        content: command["text"].as_str().unwrap_or_default().to_owned(),
                        wire,
                        notification: None,
                    },
                    Some(receipt.clone()),
                )
                .await?
                .id);
        }
        "queued-run.reorder" => {
            let run_id = command["runId"].as_str().unwrap_or_default();
            let thread_id = thread_id.unwrap_or_default();
            let valid = state
                .store
                .read()
                .await
                .threads
                .get(thread_id)
                .and_then(|thread| thread.runs.iter().find(|run| run.wire_id() == run_id))
                .is_some_and(|run| run.status == "queued");
            if !valid {
                return Err(anyhow::anyhow!(format!("Run {run_id} is not queued.")));
            }
            let before_run_id = match command.get("beforeRunId") {
                Some(Value::Null) | None => None,
                Some(value) => Some(Run::uuid_for(value.as_str().unwrap_or_default())),
            };
            return Ok(state
                .commit_receipted(
                    Event::RunReordered {
                        thread_id: thread_id.to_owned(),
                        run_id: Run::uuid_for(run_id),
                        before_run_id,
                    },
                    Some(receipt.clone()),
                )
                .await?
                .id);
        }
        "queued-run.start" => {
            let thread_id = thread_id.unwrap_or_default();
            let run_id = command["runId"].as_str().unwrap_or_default();
            return start_queued_run(state, receipt, thread_id, run_id).await;
        }
        "queue.resume" => {
            let thread_id = thread_id.unwrap_or_default();
            let (queued, next, active) = {
                let store = state.store.read().await;
                let thread = store
                    .threads
                    .get(thread_id)
                    .ok_or_else(|| anyhow::anyhow!("thread not found"))?;
                let active = thread
                    .runs
                    .iter()
                    .any(|run| matches!(run.status.as_str(), "preparing" | "starting" | "running"));
                let queued = thread
                    .runs
                    .iter()
                    .filter(|run| run.status == "queued")
                    .cloned()
                    .collect::<Vec<_>>();
                let next = queued.first().cloned();
                (queued, next, active)
            };
            let held = queued
                .iter()
                .filter(|run| run.queue_held)
                .cloned()
                .collect::<Vec<_>>();
            let mut sequence = None;
            for (index, mut run) in held.into_iter().enumerate() {
                run.queue_held = false;
                sequence = Some(
                    state
                        .commit_receipted(
                            Event::RunUpdated(run),
                            (index + 1 == queued.iter().filter(|run| run.queue_held).count())
                                .then(|| receipt.clone()),
                        )
                        .await?
                        .id,
                );
            }
            if !active {
                if let Some(run) = next {
                    if queued.iter().all(|run| !run.queue_held) {
                        sequence = Some(
                            state
                                .commit_receipted(
                                    Event::CommandDispatched {
                                        command_id: receipt.command_id.clone(),
                                        command_type: command_type.into(),
                                        thread_id: Some(thread_id.to_owned()),
                                        payload: command.clone(),
                                    },
                                    Some(receipt.clone()),
                                )
                                .await?
                                .id,
                        );
                    }
                    start_existing_run(
                        state.clone(),
                        &CommandReceipt {
                            command_id: format!("{}:start", receipt.command_id),
                            thread_id: thread_id.to_owned(),
                            command_type: receipt.command_type.clone(),
                        },
                        thread_id,
                        &run.wire_id(),
                        "running",
                    )
                    .await?;
                }
            }
            if sequence.is_none() {
                sequence = Some(
                    state
                        .commit_receipted(
                            Event::CommandDispatched {
                                command_id: receipt.command_id.clone(),
                                command_type: command_type.into(),
                                thread_id: Some(thread_id.to_owned()),
                                payload: command.clone(),
                            },
                            Some(receipt.clone()),
                        )
                        .await?
                        .id,
                );
            }
            return Ok(sequence.unwrap_or(0));
        }
        "queued-message.promote-to-steer" => {
            let thread_id = thread_id.unwrap_or_default();
            let queued_run_id = command["queuedRunId"].as_str().unwrap_or_default();
            let target_run_id = command["targetRunId"].as_str().unwrap_or_default();
            let (queued_run, target_run, message) = {
                let store = state.store.read().await;
                let thread = store
                    .threads
                    .get(thread_id)
                    .ok_or_else(|| anyhow::anyhow!("thread not found"))?;
                let queued_run = thread
                    .runs
                    .iter()
                    .find(|run| run.wire_id() == queued_run_id && run.status == "queued")
                    .cloned()
                    .ok_or_else(|| {
                        anyhow::anyhow!(format!("Queued run {queued_run_id} is not queued."))
                    })?;
                let target_run = thread
                    .runs
                    .iter()
                    .find(|run| run.wire_id() == target_run_id)
                    .cloned()
                    .ok_or_else(|| {
                        anyhow::anyhow!(format!("Target run {target_run_id} was not found."))
                    })?;
                let message_id = queued_run.user_message_id.as_deref().ok_or_else(|| {
                    anyhow::anyhow!(format!("Queued run {queued_run_id} has no queued message."))
                })?;
                let message = thread
                    .messages
                    .iter()
                    .find(|message| message.id == message_id)
                    .cloned()
                    .ok_or_else(|| {
                        anyhow::anyhow!(format!(
                            "Queued run {queued_run_id} has no queued message."
                        ))
                    })?;
                (queued_run, target_run, message)
            };
            if message.content.trim_start().starts_with("/goal") {
                return Err(anyhow::anyhow!(
                    "Goal commands must run as a separate turn. Queue it or wait for the active turn to finish."
                ));
            }
            if message.content.trim_start().starts_with("/compact") {
                return Err(anyhow::anyhow!(
                    "Context compaction must run as a separate turn. Queue it or wait for the active turn to finish."
                ));
            }
            if target_run.status == "running" {
                if let Some(cancel) = state.cancellations().lock().await.remove(&target_run.id) {
                    let _ = cancel.send(());
                }
            }
            let mut cancelled = queued_run;
            cancelled.status = "cancelled".into();
            cancelled.finished_at = Some(now());
            cancelled.queue_held = false;
            return Ok(state
                .commit_receipted(Event::RunUpdated(cancelled), Some(receipt.clone()))
                .await?
                .id);
        }
        "runtime-request.respond" => {
            let request_id = command["requestId"].as_str().unwrap_or_default();
            let thread_id = thread_id.unwrap_or_default();
            if let Some(sequence) =
                respond_provider_request(&state, thread_id, request_id, command, receipt).await?
            {
                return Ok(sequence);
            }
            let current = state
                .store
                .read()
                .await
                .threads
                .get(thread_id)
                .and_then(|thread| {
                    thread
                        .row
                        .get("runtimeRequests")
                        .and_then(Value::as_array)
                        .and_then(|requests| {
                            requests.iter().find(|request| request["id"] == request_id)
                        })
                })
                .cloned();
            let Some(request) = current else {
                return Err(anyhow::anyhow!(format!(
                    "Runtime request {request_id} was not found."
                )));
            };
            if request["status"]
                .as_str()
                .is_some_and(|status| status != "pending")
            {
                return Err(anyhow::anyhow!(format!(
                    "Runtime request {request_id} is {}.",
                    request["status"].as_str().unwrap_or("resolved")
                )));
            }
            let mut requests = state
                .store
                .read()
                .await
                .threads
                .get(thread_id)
                .and_then(|thread| thread.row["runtimeRequests"].as_array().cloned())
                .unwrap_or_default();
            if let Some(existing) = requests
                .iter_mut()
                .find(|candidate| candidate["id"] == request_id)
            {
                existing["status"] = json!("resolved");
                existing["decision"] = command.get("decision").cloned().unwrap_or(Value::Null);
                existing["feedback"] = command.get("feedback").cloned().unwrap_or(Value::Null);
                existing["answers"] = command.get("answers").cloned().unwrap_or(Value::Null);
            }
            return Ok(state
                .commit_receipted(
                    Event::ThreadPatched {
                        thread_id: thread_id.to_owned(),
                        patch: json!({"row":{"runtimeRequests":requests}}),
                    },
                    Some(receipt.clone()),
                )
                .await?
                .id);
        }
        "thread.user-input.dismiss" => {
            return Err(anyhow::anyhow!("This question has already been answered."));
        }
        "thread.fork" => {
            let source_thread_id = command["sourceThreadId"].as_str().unwrap_or_default();
            let source_point = command["sourcePoint"]["type"].as_str().unwrap_or_default();
            let target_thread_id = command["targetThreadId"].as_str().unwrap_or_default();
            let source = state
                .store
                .read()
                .await
                .threads
                .get(source_thread_id)
                .cloned();
            let Some(source) = source else {
                return Err(anyhow::anyhow!(format!(
                    "No orchestration projection exists for thread {source_thread_id}."
                )));
            };
            if source_point == "latest_stable"
                && !source.runs.iter().any(|run| run.status == "completed")
            {
                return Err(anyhow::anyhow!(
                    "No stable source run was found for fork source latest_stable."
                ));
            }
            let at = crate::auth_service::iso(crate::auth_service::now());
            let mut row = source.stored_row();
            row["id"] = json!(target_thread_id);
            row["title"] = command
                .get("title")
                .cloned()
                .unwrap_or_else(|| json!(source.title));
            row["createdAt"] = json!(at.clone());
            row["updatedAt"] = json!(at);
            row["archivedAt"] = Value::Null;
            row["deletedAt"] = Value::Null;
            row["lineage"] = json!({"parentThreadId":source_thread_id,"relationshipToParent":"fork","rootThreadId":source.row["lineage"]["rootThreadId"].as_str().unwrap_or(source_thread_id),"sideChat":command.get("sideChat").and_then(Value::as_bool).unwrap_or(false)});
            let forked_from = source
                .runs
                .iter()
                .rev()
                .find(|run| run.status == "completed")
                .map(|run| json!({"type":"run","threadId":source_thread_id,"runId":run.id}))
                .unwrap_or(Value::Null);
            row["forkedFrom"] = forked_from;
            let thread = Thread {
                row,
                id: target_thread_id.to_owned(),
                project_id: source.project_id,
                title: command
                    .get("title")
                    .and_then(Value::as_str)
                    .unwrap_or(&source.title)
                    .to_owned(),
                cwd: source.cwd,
                status: "idle".into(),
                messages: source.messages,
                runs: vec![],
                created_at: now(),
                archived: false,
                settled: false,
                snoozed_until: None,
                pinned: false,
                unread: false,
                tags: source.tags,
                checkpoints: vec![],
                link_requests: Vec::new(),
                context_handoffs: Vec::new(),
                context_transfers: Vec::new(),
                handoff_items: Vec::new(),
                provider: Default::default(),
                stored: None,
            };
            return Ok(state
                .commit_receipted(Event::ThreadCreated(thread), Some(receipt.clone()))
                .await?
                .id);
        }
        "thread.merge_back" => {
            let source_thread_id = command["sourceThreadId"].as_str().unwrap_or_default();
            let target_thread_id = command["targetThreadId"].as_str().unwrap_or_default();
            let store = state.store.read().await;
            let source = store
                .threads
                .get(source_thread_id)
                .cloned()
                .ok_or_else(|| {
                    anyhow::anyhow!(format!(
                        "No orchestration projection exists for thread {source_thread_id}."
                    ))
                })?;
            let target = store
                .threads
                .get(target_thread_id)
                .cloned()
                .ok_or_else(|| {
                    anyhow::anyhow!(format!(
                        "No orchestration projection exists for thread {target_thread_id}."
                    ))
                })?;
            let existing = target
                .messages
                .iter()
                .map(|m| m.id.as_str())
                .collect::<std::collections::HashSet<_>>();
            let additions = source
                .messages
                .into_iter()
                .filter(|m| !existing.contains(m.id.as_str()))
                .collect::<Vec<_>>();
            drop(store);
            for message in additions {
                state
                    .commit(Event::MessageAdded {
                        thread_id: target_thread_id.to_owned(),
                        message,
                    })
                    .await?;
            }
            let sequence = state
                .commit_receipted(
                    Event::CommandDispatched {
                        command_id: receipt.command_id.clone(),
                        command_type: command_type.into(),
                        thread_id: Some(target_thread_id.to_owned()),
                        payload: command.clone(),
                    },
                    Some(receipt.clone()),
                )
                .await?
                .id;
            return Ok(sequence);
        }
        "delegated_task.request" => {
            return delegation::request(state, command, receipt).await;
        }
        "delegated_task.wake-policy"
        | "delegated_task.completion-delivery.acknowledge"
        | "delegated_task.completion-delivery.dispose" => {
            let parent_thread_id = command["parentThreadId"].as_str().unwrap_or_default();
            let task_id = command["taskId"].as_str().unwrap_or_default();
            let mut subagents = state
                .store
                .read()
                .await
                .threads
                .get(parent_thread_id)
                .and_then(|thread| thread.row["subagents"].as_array().cloned())
                .unwrap_or_default();
            let Some(task) = subagents.iter_mut().find(|task| task["id"] == task_id) else {
                return Err(anyhow::anyhow!(format!(
                    "Delegated task {task_id} is not an app-owned task of thread {parent_thread_id}."
                )));
            };
            if command_type == "delegated_task.wake-policy" {
                let wake = command["completionWake"].as_str().unwrap_or_default();
                if task["completionWake"].as_str() == Some(wake) {
                    return Err(anyhow::anyhow!(format!(
                        "Delegated task {task_id} already wakes the parent with completionWake {wake}."
                    )));
                }
                task["completionWake"] = json!(wake);
            } else {
                let delivery_state = if command_type.ends_with("acknowledge") {
                    "acknowledged"
                } else {
                    "disposed"
                };
                let current = task["completionDelivery"]["state"].as_str();
                if current == Some(delivery_state)
                    || (delivery_state == "acknowledged" && current == Some("disposed"))
                {
                    return Ok(state
                        .commit_receipted(
                            Event::ThreadPatched {
                                thread_id: parent_thread_id.to_owned(),
                                patch: json!({"row":{"subagents":subagents}}),
                            },
                            Some(receipt.clone()),
                        )
                        .await?
                        .id);
                }
                task["completionDelivery"] = json!({
                    "state": delivery_state,
                    "observedByRunId": command.get("observedByRunId").cloned().unwrap_or(Value::Null)
                });
                // A resolved task leaves the delivery that carried it.
                let (task_id, task_run) = (task["id"].clone(), task["runId"].clone());
                let resolved = delegation::resolve_delivery_task(
                    &*state,
                    parent_thread_id,
                    &task_id,
                    &task_run,
                )
                .await;
                if let Some((extras, follow_up)) = resolved {
                    let sequence = state
                        .commit_receipted(
                            Event::ThreadPatched {
                                thread_id: parent_thread_id.to_owned(),
                                patch: json!({"row":{"subagents":subagents,"runExtras":extras}}),
                            },
                            Some(receipt.clone()),
                        )
                        .await?
                        .id;
                    delegation::follow_up_delivery(
                        &*state,
                        parent_thread_id,
                        follow_up,
                        &subagents,
                    )
                    .await?;
                    return Ok(sequence);
                }
            }
            return Ok(state
                .commit_receipted(
                    Event::ThreadPatched {
                        thread_id: parent_thread_id.to_owned(),
                        patch: json!({"row":{"subagents":subagents}}),
                    },
                    Some(receipt.clone()),
                )
                .await?
                .id);
        }
        "delegated_task.remote.request" => {
            return delegation::remote_request(state, command, receipt).await;
        }
        // The follower that reported it then settles the parent (`after_remote_complete`).
        "delegated_task.remote.complete" => {
            return delegation::remote_complete(state, command, receipt).await;
        }
        "delegated_task.remote.stop" => {
            let parent_thread_id = command["parentThreadId"].as_str().unwrap_or_default();
            let task_id = command["taskId"].as_str().unwrap_or_default();
            let mut subagents = state
                .store
                .read()
                .await
                .threads
                .get(parent_thread_id)
                .map(|thread| {
                    thread.row["subagents"]
                        .as_array()
                        .cloned()
                        .unwrap_or_default()
                })
                .ok_or_else(|| {
                    anyhow::anyhow!(format!(
                        "No orchestration projection exists for thread {parent_thread_id}."
                    ))
                })?;
            let task = subagents
                .iter_mut()
                .find(|task| task["id"] == task_id)
                .ok_or_else(|| {
                    anyhow::anyhow!(format!("Delegated task {task_id} was not found."))
                })?;
            if command_type == "delegated_task.remote.complete" {
                if task["result"].is_null() {
                    task["status"] = command["status"].clone();
                    task["result"] = command["result"].clone();
                    task["completedAt"] =
                        json!(crate::auth_service::iso(crate::auth_service::now()));
                }
                if let Some(stopped) = command.get("stoppedThere") {
                    task["remoteChild"]["stoppedThere"] = json!(stopped == "stopped");
                }
            } else {
                let count = task["remoteChild"]["stopCount"].as_u64().unwrap_or(0) + 1;
                task["remoteChild"]["stopCount"] = json!(count);
            }
            return Ok(state
                .commit_receipted(
                    Event::ThreadPatched {
                        thread_id: parent_thread_id.to_owned(),
                        patch: json!({"row":{"subagents":subagents}}),
                    },
                    Some(receipt.clone()),
                )
                .await?
                .id);
        }
        "checkpoint.rollback.fail" => {
            let id = thread_id.unwrap_or_default();
            let row = state
                .store
                .read()
                .await
                .threads
                .get(id)
                .map(|t| t.stored_row())
                .ok_or_else(|| anyhow::anyhow!("thread not found"))?;
            if row["rollbackRequestId"].as_str() != command["requestId"].as_str() {
                return Err(anyhow::anyhow!(format!(
                    "Rollback {} is no longer the thread's current rollback.",
                    command["requestId"].as_str().unwrap_or_default()
                )));
            }
            let mut next = row;
            next["rollbackFailure"] =
                json!({"requestId":command["requestId"],"message":command["message"]});
            return Ok(state
                .commit_receipted(
                    Event::ThreadPatched {
                        thread_id: id.to_owned(),
                        patch: json!({"row":next}),
                    },
                    Some(receipt.clone()),
                )
                .await?
                .id);
        }
        "runtime-request.delivery.fail" => {
            let id = thread_id.unwrap_or_default();
            let mut row = state
                .store
                .read()
                .await
                .threads
                .get(id)
                .map(|t| t.stored_row())
                .ok_or_else(|| anyhow::anyhow!("thread not found"))?;
            let request_id = command["requestId"].as_str().unwrap_or_default();
            if let Some(request) = row["runtimeRequests"]
                .as_array_mut()
                .and_then(|items| items.iter_mut().find(|item| item["id"] == request_id))
            {
                request["deliveryError"] = json!(true);
            }
            return Ok(state
                .commit_receipted(
                    Event::ThreadPatched {
                        thread_id: id.to_owned(),
                        patch: json!({"row":row}),
                    },
                    Some(receipt.clone()),
                )
                .await?
                .id);
        }
        "thread.interrupt.unresponsive" => {
            let id = thread_id.unwrap_or_default();
            return Ok(state.commit_receipted(Event::ThreadPatched {thread_id:id.to_owned(),patch:json!({"row":{"interruptUnresponsive":true,"providerThreadId":command["providerThreadId"],"providerTurnId":command["providerTurnId"]}})},Some(receipt.clone())).await?.id);
        }
        "thread.background-work.settle" => {
            let id = thread_id.unwrap_or_default();
            return Ok(state.commit_receipted(Event::ThreadPatched {thread_id:id.to_owned(),patch:json!({"row":{"backgroundWorkSettled":true,"backgroundWorkSettleReason":command.get("reason").cloned().unwrap_or(Value::Null)}})},Some(receipt.clone())).await?.id);
        }
        "notification.delivery.accept" => {
            let thread_id = thread_id.unwrap_or_default();
            let message_id = command["messageId"].as_str().unwrap_or_default();
            let exists = state
                .store
                .read()
                .await
                .threads
                .get(thread_id)
                .is_some_and(|thread| {
                    thread
                        .messages
                        .iter()
                        .any(|message| message.id == message_id)
                });
            if !exists {
                return Err(anyhow::anyhow!(format!(
                    "Notification message {message_id} was not found."
                )));
            }
            return Ok(state
                .commit_receipted(
                    Event::CommandDispatched {
                        command_id: receipt.command_id.clone(),
                        command_type: command_type.into(),
                        thread_id: Some(thread_id.to_owned()),
                        payload: command.clone(),
                    },
                    Some(receipt.clone()),
                )
                .await?
                .id);
        }
        "checkpoint.rollback" => {
            let thread_id = thread_id.unwrap_or_default();
            let checkpoint_id = command["checkpointId"].as_str().unwrap_or_default();
            let target = state
                .store
                .read()
                .await
                .threads
                .get(thread_id)
                .and_then(|thread| {
                    thread.checkpoints.iter().find(|checkpoint| {
                        crate::thread_projection::checkpoint_id(thread_id, checkpoint.turn_count)
                            == checkpoint_id
                    })
                })
                .cloned();
            let Some(target) = target else {
                return Err(anyhow::anyhow!(format!(
                    "Checkpoint {checkpoint_id} was not found."
                )));
            };
            if !target.ready && command.get("restoreFiles").and_then(Value::as_bool) != Some(false)
            {
                return Err(anyhow::anyhow!(format!(
                    "Checkpoint {checkpoint_id} is missing and cannot be restored."
                )));
            }
            let expected_scope = crate::thread_projection::checkpoint_scope_id(thread_id);
            if command["scopeId"].as_str() != Some(expected_scope.as_str()) {
                return Err(anyhow::anyhow!(format!(
                    "Checkpoint {checkpoint_id} belongs to scope {expected_scope}, not {}.",
                    command["scopeId"].as_str().unwrap_or_default()
                )));
            }
            let second = state
                .commit_receipted(
                    Event::ThreadPatched {
                        thread_id: thread_id.to_owned(),
                        patch: json!({"row":{"rollbackRequestId":receipt.command_id,"rollbackFailure":Value::Null}}),
                    },
                    Some(receipt.clone()),
                )
                .await?;
            return Ok(second.id);
        }
        "thread.created.record" => {
            let parent_thread_id = command["parentThreadId"].as_str().unwrap_or_default();
            let target_thread_id = command["targetThreadId"].as_str().unwrap_or_default();
            let store = state.store.read().await;
            let parent = store.threads.get(parent_thread_id).ok_or_else(|| {
                anyhow::anyhow!(format!(
                    "No orchestration projection exists for thread {parent_thread_id}."
                ))
            })?;
            let target = store.threads.get(target_thread_id).ok_or_else(|| {
                anyhow::anyhow!(format!(
                    "No orchestration projection exists for thread {target_thread_id}."
                ))
            })?;
            if parent.project_id != target.project_id {
                return Err(anyhow::anyhow!(format!(
                    "Target thread {target_thread_id} belongs to another project."
                )));
            }
            let parent_run_id = command["parentRunId"].as_str().unwrap_or_default();
            if !parent.runs.iter().any(|run| run.wire_id() == parent_run_id) {
                return Err(anyhow::anyhow!(format!(
                    "Parent node {} is not the root of run {parent_run_id}.",
                    command["parentNodeId"].as_str().unwrap_or_default()
                )));
            }
            if let Some(target_run_id) = command["targetRunId"].as_str() {
                if !target.runs.iter().any(|run| run.wire_id() == target_run_id) {
                    return Err(anyhow::anyhow!(format!(
                        "Target run {target_run_id} does not belong to thread {target_thread_id}."
                    )));
                }
            }
            drop(store);
            return delegation::record_created_thread(state, command, receipt).await;
        }
        // Every command discriminator has a registered wire contract. Commands
        // whose provider-specific state is not available locally still need a
        // durable receipt so retries remain idempotent and clients observe the
        // same dispatch sequence shape as the reference server.
        _ => {}
    }
    Ok(state
        .commit_receipted(
            Event::CommandDispatched {
                command_id: receipt.command_id.clone(),
                command_type: command_type.into(),
                thread_id: thread_id.map(str::to_owned),
                payload: command.clone(),
            },
            Some(receipt.clone()),
        )
        .await?
        .id)
}

/// Answer a runtime request a provider turn waits on, as the reference's
/// `runtime-request.respond` does: check it can be answered, record the
/// answer, then hand it to the turn. `None` when the thread's provider turns
/// made no request `request_id`.
async fn respond_provider_request<H: CommandHost>(
    state: &Arc<H>,
    thread_id: &str,
    request_id: &str,
    command: &Value,
    receipt: &crate::CommandReceipt,
) -> anyhow::Result<Option<u64>> {
    let found = {
        let store = state.store.read().await;
        let Some(thread) = store.threads.get(thread_id) else {
            return Ok(None);
        };
        let Some(record) = thread
            .provider_records()
            .runtime_requests
            .into_iter()
            .find(|record| record["id"] == request_id)
        else {
            return Ok(None);
        };
        let uuid = request_id.rsplit(':').next().unwrap_or_default().to_owned();
        let Some((run_id, _)) = thread.provider.request(&uuid) else {
            return Ok(None);
        };
        (record, uuid, run_id)
    };
    let (record, uuid, run_id) = found;
    let status = record["status"].as_str().unwrap_or("resolved");
    if status != "pending" {
        anyhow::bail!("Runtime request {request_id} is {status}.");
    }
    let capability = &record["responseCapability"];
    if capability["type"] == "not_resumable" {
        anyhow::bail!(
            "{}",
            capability["reason"]
                .as_str()
                .unwrap_or("This request cannot be answered.")
        );
    }
    let decision = command["decision"].as_str().map(str::to_owned);
    let feedback = command["feedback"].as_str().map(str::to_owned);
    if feedback.is_some()
        && (capability["type"] != "live"
            || capability["supportsFeedback"] != true
            || record["kind"] == "user_input"
            || decision.as_deref() != Some("decline")
            || feedback.as_deref().is_some_and(|f| f.trim().is_empty()))
    {
        anyhow::bail!("Feedback requires a live approval that supports it and a decline decision.");
    }
    if capability["type"] == "message" {
        // The CLI that asked is gone; the reference sends the answers as a
        // new message, which this server does not do yet.
        anyhow::bail!("This question can only be answered with a new message.");
    }
    // Each answer as its plain value, as the reference decodes them.
    let answers = command
        .get("answers")
        .filter(|a| !a.is_null())
        .map(|answers| {
            let flat: serde_json::Map<String, Value> = answers
                .as_object()
                .into_iter()
                .flatten()
                .map(|(id, answer)| {
                    let value = answer
                        .get("answers")
                        .cloned()
                        .unwrap_or_else(|| answer.clone());
                    (id.clone(), value)
                })
                .collect();
            Value::Object(flat)
        });
    let sequence = state
        .commit_receipted(
            Event::RunProvider {
                run_id,
                thread_id: thread_id.to_owned(),
                event: crate::workspace::ProviderEvent::RuntimeRequestAnswered {
                    id: uuid.clone(),
                    decision: decision.clone(),
                    feedback: feedback.clone(),
                    answers: answers.clone(),
                },
                position: None,
            },
            Some(receipt.clone()),
        )
        .await?
        .id;
    let delivered = state
        .workspace()
        .respond_turn(
            &run_id.to_string(),
            crate::workspace::provider::RuntimeResponse {
                id: uuid.clone(),
                decision,
                feedback,
                answers,
                steer: None,
            },
        )
        .await;
    if !delivered {
        let _ = state
            .commit(Event::RunProvider {
                run_id,
                thread_id: thread_id.to_owned(),
                event: crate::workspace::ProviderEvent::RuntimeRequestDeliveryFailed { id: uuid },
                position: None,
            })
            .await;
    }
    Ok(Some(sequence))
}

/// Whether a newer link to the machine `linked` names answers `card`, an
/// older card asking for it with the same access (`askedForLinkedMachine`).
fn asked_for_linked_machine(card: &Value, linked: &Value) -> bool {
    if card["requestedAccess"] != linked["requestedAccess"] {
        return false;
    }
    if !card["environmentId"].is_null() {
        return card["environmentId"] == linked["linkedEnvironmentId"];
    }
    if let Some(url) = card["url"].as_str() {
        return linked["url"].as_str() == Some(url);
    }
    let normal = |v: &Value| v.as_str().unwrap_or_default().trim().to_lowercase();
    let hint = normal(&card["hint"]);
    !hint.is_empty() && hint == normal(&linked["hint"]) && hint == normal(&linked["linkedLabel"])
}

/// `run.output.submit`, internal to the server: keeps the result an agent
/// submitted, already checked against its run's schema, on the run
/// (`dispatchRunOutputSubmit`, `run.output-submitted`). A run takes it while
/// active, or once completed while it still owes it.
async fn submit_run_output<H: CommandHost>(
    state: Arc<H>,
    command: &Value,
    receipt: &CommandReceipt,
) -> anyhow::Result<u64> {
    let thread_id = command["threadId"].as_str().unwrap_or_default().to_owned();
    let run_id = command["runId"].as_str().unwrap_or_default();
    let extras = {
        let store = state.store.read().await;
        let thread = store
            .threads
            .get(&thread_id)
            .ok_or_else(|| anyhow::anyhow!("thread not found"))?;
        let run = thread
            .runs
            .iter()
            .find(|run| run.wire_id() == run_id)
            .filter(|run| run.output_schema.is_some())
            .ok_or_else(|| anyhow::anyhow!("Run {run_id} was not asked for a result."))?;
        let mut extras = thread.stored_row()["runExtras"].clone();
        if !extras.is_object() {
            extras = json!({});
        }
        let status = thread.projected_run_status(run).0;
        let active = matches!(status, "preparing" | "starting" | "running" | "waiting");
        let owes = status == "completed" && extras[run_id]["output"].is_null();
        if !active && !owes {
            anyhow::bail!("Run {run_id} is {status} and takes no result.");
        }
        if !extras[run_id].is_object() {
            extras[run_id] = json!({});
        }
        extras[run_id]["output"] = json!({
            "value": command["value"],
            "submittedAt": crate::auth_service::iso(crate::auth_service::now()),
        });
        extras
    };
    Ok(state
        .commit_receipted(
            Event::ThreadPatched {
                thread_id,
                patch: json!({"row": {"runExtras": extras}}),
            },
            Some(receipt.clone()),
        )
        .await?
        .id)
}

/// `link_request.record`, internal to the server: records or updates a card
/// in which an agent asked the user to link another machine, and wakes the
/// agent with how it ended (`dispatchLinkRequestRecord` in the reference).
async fn record_link_request<H: CommandHost>(
    state: Arc<H>,
    command: &Value,
    receipt: &CommandReceipt,
) -> anyhow::Result<u64> {
    let thread_id = command["threadId"].as_str().unwrap_or_default().to_owned();
    let run_id = command["runId"].as_str().unwrap_or_default().to_owned();
    let card_id = command["turnItemId"]
        .as_str()
        .unwrap_or_default()
        .to_owned();
    let pending = command["linkStatus"] == "pending";
    let (card, closed, wake_allowed, unsnooze) = {
        let store = state.store.read().await;
        let thread = store
            .threads
            .get(&thread_id)
            .ok_or_else(|| anyhow::anyhow!("thread not found"))?;
        let run = thread
            .runs
            .iter()
            .find(|run| run.id.to_string() == run_id)
            .ok_or_else(|| anyhow::anyhow!("Run {run_id} was not found."))?;
        let existing = thread
            .link_requests
            .iter()
            .find(|c| c["id"] == card_id.as_str());
        if existing.is_some_and(|c| c["runId"] != run_id.as_str()) {
            anyhow::bail!("Link request {card_id} belongs to another run.");
        }
        if existing.is_none() && run.status != "running" {
            anyhow::bail!("Run {run_id} is not active.");
        }
        // A request is answered once, and a retry finds its card as it was asked.
        let unchanged = existing.is_some_and(|c| c["linkStatus"] != "pending" || pending);
        let at = crate::thread_projection::iso(now());
        let card = if unchanged {
            None
        } else {
            let mut card = serde_json::Map::new();
            for key in [
                "threadId",
                "runId",
                "nodeId",
                "url",
                "reason",
                "environmentId",
                "label",
                "hint",
                "requestedAccess",
                "linkStatus",
                "linkedEnvironmentId",
                "linkedLabel",
                "linkedAccess",
                "failure",
            ] {
                if let Some(value) = command.get(key).filter(|v| !v.is_null()) {
                    card.insert(key.into(), value.clone());
                }
            }
            let mut card = Value::Object(card);
            card["id"] = json!(card_id);
            // An update keeps where the card was asked, even after a restart.
            match existing {
                Some(existing) => {
                    for key in ["nodeId", "providerThreadId", "providerTurnId", "startedAt"] {
                        card[key] = existing[key].clone();
                    }
                }
                None => {
                    let ids = thread.provider_run_ids(run);
                    card["providerThreadId"] =
                        json!(ids.as_ref().map(|ids| ids.provider_thread.clone()));
                    card["providerTurnId"] =
                        json!(ids.as_ref().map(|ids| ids.provider_turn.clone()));
                    if let Some(ids) = &ids {
                        card["nodeId"] = json!(ids.root_node);
                    }
                    card["startedAt"] = json!(at);
                }
            }
            // Where the card sits among its run's items: after those it had
            // when it was asked, which an answer keeps.
            card[crate::thread_projection::LINK_CARD_POSITION] = match existing {
                Some(existing) => existing[crate::thread_projection::LINK_CARD_POSITION].clone(),
                None => {
                    let index = thread.runs.iter().position(|r| r.id == run.id).unwrap_or(0);
                    json!(thread
                        .provider_records()
                        .items
                        .iter()
                        .filter(|(i, _)| *i == index)
                        .count())
                }
            };
            card["nativeItemRef"] = Value::Null;
            card["parentItemId"] = Value::Null;
            card["status"] = json!(match command["linkStatus"].as_str() {
                Some("pending") => "waiting",
                Some("linked") => "completed",
                Some("failed") => "failed",
                _ => "cancelled",
            });
            card["title"] = command["label"]
                .as_str()
                .or(command["url"].as_str())
                .map_or(json!("Link an environment"), |t| json!(t));
            card["completedAt"] = if pending { Value::Null } else { json!(at) };
            card["updatedAt"] = json!(at);
            card["type"] = json!("link_request");
            Some(card)
        };
        // The link answers older cards that asked for the same machine and
        // access; a card still being answered stays open for its own answer.
        let answering: Vec<&str> = command["answeringTurnItemIds"]
            .as_array()
            .map(|ids| ids.iter().filter_map(Value::as_str).collect())
            .unwrap_or_default();
        let position = |id: &str| thread.link_requests.iter().position(|c| c["id"] == id);
        let own = position(&card_id).unwrap_or(thread.link_requests.len());
        let closed: Vec<Value> = match &card {
            Some(card) if command["linkStatus"] == "linked" => thread
                .link_requests
                .iter()
                .enumerate()
                .filter(|(index, older)| {
                    *index < own
                        && older["linkStatus"] == "pending"
                        && !answering.contains(&older["id"].as_str().unwrap_or_default())
                        && asked_for_linked_machine(older, card)
                })
                .map(|(_, older)| {
                    let mut older = older.clone();
                    older["status"] = json!("cancelled");
                    older["linkStatus"] = json!("cancelled");
                    older["completedAt"] = json!(at);
                    older["updatedAt"] = json!(at);
                    older
                })
                .collect(),
            _ => Vec::new(),
        };
        let deleted = thread.stored_row()["deletedAt"].is_string();
        // A link wakes a snoozed thread, as the reference's `thread.unsnoozed`.
        let unsnooze = card.is_some()
            && command["linkStatus"] == "linked"
            && !thread.stored_row()["snoozedUntil"].is_null();
        (
            card,
            closed,
            !unchanged && !thread.archived && !deleted,
            unsnooze,
        )
    };
    if unsnooze {
        let at = json!(crate::auth_service::iso(crate::auth_service::now()));
        state
            .commit(Event::ThreadPatched {
                thread_id: thread_id.clone(),
                patch: json!({
                    "snoozed_until": null,
                    "row": {"updatedAt": at, "snoozedUntil": null, "snoozedAt": null},
                }),
            })
            .await?;
    }
    let mut seq = match card {
        Some(card) => {
            state
                .commit_receipted(
                    Event::LinkRequestRecorded {
                        thread_id: thread_id.clone(),
                        card,
                    },
                    Some(receipt.clone()),
                )
                .await?
                .id
        }
        None => {
            state
                .commit_receipted(
                    Event::CommandDispatched {
                        command_id: receipt.command_id.clone(),
                        command_type: "link_request.record".into(),
                        thread_id: Some(thread_id.clone()),
                        payload: command.clone(),
                    },
                    Some(receipt.clone()),
                )
                .await?
                .id
        }
    };
    for older in closed {
        seq = state
            .commit(Event::LinkRequestRecorded {
                thread_id: thread_id.clone(),
                card: older,
            })
            .await?
            .id;
    }
    // The agent asked without waiting, so the answer reaches it as a
    // notification: it starts a turn, or follows the one running.
    let wake = &command["wake"];
    if wake.is_object() && wake_allowed {
        let queued = state
            .store
            .read()
            .await
            .threads
            .get(&thread_id)
            .is_some_and(|thread| thread.runs.iter().any(|run| run.status == "running"));
        seq = start_run(
            state.clone(),
            receipt,
            &thread_id,
            wake["text"].as_str().unwrap_or_default().to_owned(),
            &wake["messageId"],
            queued,
            None,
            Some("server".into()),
            None,
            false,
            Some(wake["notification"].clone()),
            None,
            false,
            None,
        )
        .await?;
    }
    Ok(seq)
}

#[cfg(test)]
mod tests {
    use super::provider_instance_enabled;
    use serde_json::json;

    #[test]
    fn provider_instances_are_enabled_as_the_reference_resolves_them() {
        let settings = json!({
            "providerInstances": {
                "codex": {"driver": "codex"},
                "work": {"driver": "codex", "enabled": false},
                "nested": {"driver": "claudeAgent", "config": {"enabled": false}},
                "cursor": {"driver": "cursor"},
                "cursorOn": {"driver": "cursor", "enabled": true}
            },
            "projectSettingsOverrides": {
                "p2": {"providerInstances": {"codex": {"enabled": false}, "work": {"enabled": true}}}
            }
        });
        let enabled =
            |project: &str, instance: &str| provider_instance_enabled(&settings, project, instance);
        assert!(enabled("p1", "codex"));
        assert!(!enabled("p1", "work"));
        assert!(!enabled("p1", "nested"));
        assert!(!enabled("p1", "cursor"));
        assert!(enabled("p1", "cursorOn"));
        // Unconfigured: only the built-in defaults.
        assert!(enabled("p1", "claudeAgent"));
        assert!(!enabled("p1", "parity-absent"));
        // A project's override wins.
        assert!(!enabled("p2", "codex"));
        assert!(enabled("p2", "work"));
    }
}

#[cfg(test)]
mod detach_tests {
    use super::*;

    #[test]
    fn thread_changes_detach_the_session_as_the_reference_does() {
        let codex =
            json!({"modelSelection": {"instanceId": "codex", "model": "m"}, "worktreePath": null});
        let claude = json!({"modelSelection": {"instanceId": "claudeAgent", "model": "m"}, "worktreePath": null});
        assert_eq!(
            detach_reason("thread.archive", &json!({}), &codex),
            Some("Thread archived.")
        );
        assert_eq!(
            detach_reason("thread.settle", &json!({}), &codex),
            Some("Thread settled.")
        );
        // Codex switches runtime mode in its session; Claude cannot.
        let mode = json!({"runtimeMode": "approval-required"});
        assert_eq!(
            detach_reason("thread.runtime-mode.set", &mode, &codex),
            None
        );
        assert_eq!(
            detach_reason("thread.runtime-mode.set", &mode, &claude),
            Some("Runtime mode changed.")
        );
        // Only a new provider instance releases the session.
        let same = json!({"modelSelection": {"instanceId": "codex", "model": "other"}});
        let other = json!({"modelSelection": {"instanceId": "claudeAgent", "model": "m"}});
        assert_eq!(
            detach_reason("thread.model-selection.set", &same, &codex),
            None
        );
        assert_eq!(
            detach_reason("thread.model-selection.set", &other, &codex),
            Some(PROVIDER_CHANGED)
        );
        // A new workspace does; a title change does not.
        assert_eq!(
            detach_reason(
                "thread.metadata.update",
                &json!({"worktreePath": "/w"}),
                &codex
            ),
            Some("Workspace changed.")
        );
        assert_eq!(
            detach_reason("thread.metadata.update", &json!({"title": "T"}), &codex),
            None
        );
    }
}
