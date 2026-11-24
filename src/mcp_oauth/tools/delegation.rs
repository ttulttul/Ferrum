//! `delegate_task` and `task_status` (`OrchestratorMcpService.delegateTask`,
//! `taskStatus` and `readTask`): a thread hands a task to a child thread and
//! reads how it went.
use super::*;

const RUNNING_TASK_NEXT: &str = "Completion wakes this thread. Do not wait on it or poll it. Continue other work, and end your turn when none is left.";
const TERMINAL_TASK: [&str; 4] = ["completed", "failed", "cancelled", "interrupted"];
const TERMINAL_RUN: [&str; 5] = [
    "completed",
    "failed",
    "cancelled",
    "interrupted",
    "rolled_back",
];
const WAIT_DEFAULT_MS: u64 = 30_000;
const WAIT_MAX_MS: u64 = 45_000;

fn records(value: &Value, key: &str) -> Vec<Value> {
    value[key].as_array().cloned().unwrap_or_default()
}

pub(super) fn thread_scope<'a>(
    caller: &'a Caller,
    operation: &str,
) -> Result<&'a super::super::ThreadScope, Refusal> {
    caller.granted.thread.as_ref().ok_or_else(|| {
        failure(
            "thread_credential_required",
            format!("{operation} acts as the calling T3 thread, so it needs an agent running inside T3 Code. This MCP client signed in from outside a thread."),
        )
    })
}

/// `stableCommandId`.
pub(super) fn stable_command_id(caller: &Caller, operation: &str, key: &str) -> String {
    let part = crate::thread_projection::uri_component;
    [
        "command".to_owned(),
        "mcp".to_owned(),
        part(&caller.request_namespace()),
        part(operation),
        part(key),
    ]
    .join(":")
}

/// `stableCommandId` with an `index`, `stableThreadId` or `stableMessageId`
/// for the `index`th item of a request.
fn stable_indexed(
    caller: &Caller,
    prefix: &str,
    operation: Option<&str>,
    key: &str,
    index: usize,
) -> String {
    let part = crate::thread_projection::uri_component;
    let mut parts = vec![
        prefix.to_owned(),
        "mcp".to_owned(),
        part(&caller.request_namespace()),
    ];
    if let Some(operation) = operation {
        parts.push(part(operation));
    }
    parts.push(part(key));
    parts.push(index.to_string());
    parts.join(":")
}

/// `threadTitle`: the title, else the prompt, cut to 80 characters, else
/// "<parent> thread <n>" (`placeholderThreadTitle`).
fn created_thread_title(
    parent: &str,
    prompt: Option<&str>,
    title: Option<&str>,
    index: usize,
) -> String {
    let detail = title
        .map(str::trim)
        .filter(|t| !t.is_empty())
        .or_else(|| prompt.map(str::trim).filter(|p| !p.is_empty()));
    let Some(detail) = detail else {
        return format!("{parent} thread {}", index + 1);
    };
    let units: Vec<u16> = detail.encode_utf16().collect();
    if units.len() <= 80 {
        return detail.to_owned();
    }
    let mut end = 77;
    if (0xd800..=0xdbff).contains(&units[end - 1]) && (0xdc00..=0xdfff).contains(&units[end]) {
        end -= 1;
    }
    format!("{}...", String::from_utf16_lossy(&units[..end]))
}

/// `providerConstraints`: why an instance cannot run a child task.
pub(super) fn provider_constraints(provider: &Value) -> Vec<String> {
    let mut out = Vec::new();
    let driver = provider["driver"].as_str().unwrap_or_default();
    // Rust runs children on Codex and Claude only. A disabled instance says
    // so first, as every instance the reference lists runs its children.
    if !matches!(driver, "codex" | "claudeAgent") && provider["enabled"] != false {
        out.push("No V2 provider adapter is registered.".into());
    }
    if provider["available"] == false {
        out.push(
            provider["unavailableReason"]
                .as_str()
                .unwrap_or("Provider driver is unavailable.")
                .to_owned(),
        );
    }
    if provider["enabled"] == false {
        out.push("Provider instance is disabled.".into());
        return out;
    }
    if provider["installed"] != true {
        out.push("Provider executable is not installed.".into());
    }
    if matches!(provider["status"].as_str(), Some("error" | "disabled")) {
        out.push(provider["message"].as_str().map_or_else(
            || {
                format!(
                    "Provider status is {}.",
                    provider["status"].as_str().unwrap_or_default()
                )
            },
            str::to_owned,
        ));
    }
    if provider["auth"]["status"] == "unauthenticated" {
        out.push("Provider is not authenticated.".into());
    }
    out
}

/// `resolveTarget`: the model selection a child runs on, inheriting the
/// parent's where the target leaves it out.
fn resolve_target(providers: &[Value], parent: &Value, target: &Value) -> Result<Value, Refusal> {
    let inherited = &parent["modelSelection"];
    let instance = target["providerInstanceId"]
        .as_str()
        .or_else(|| {
            target["driverKind"].as_str().and_then(|driver| {
                let candidates: Vec<&Value> = providers
                    .iter()
                    .filter(|p| p["driver"] == driver && provider_constraints(p).is_empty())
                    .collect();
                candidates
                    .iter()
                    .find(|p| p["instanceId"] == inherited["instanceId"])
                    .or(candidates.first())
                    .and_then(|p| p["instanceId"].as_str())
            })
        })
        .or(inherited["instanceId"].as_str())
        .unwrap_or_default()
        .to_owned();
    if let Some(driver) = target["driverKind"].as_str() {
        if target["providerInstanceId"].is_null()
            && !providers
                .iter()
                .any(|p| p["driver"] == driver && p["instanceId"] == instance.as_str())
        {
            return Err(failure(
                "provider_unavailable",
                format!("No V2 provider adapter is registered for driver {driver}."),
            ));
        }
    }
    let Some(provider) = providers
        .iter()
        .find(|p| p["instanceId"] == instance.as_str())
    else {
        return Err(failure(
            "provider_unavailable",
            format!("Provider instance {instance} is not registered."),
        ));
    };
    if let Some(driver) = target["driverKind"].as_str() {
        if provider["driver"] != driver {
            return Err(failure(
                "invalid_request",
                format!(
                    "Provider instance {instance} uses driver {}, not {driver}.",
                    provider["driver"].as_str().unwrap_or_default()
                ),
            ));
        }
    }
    let constraints = provider_constraints(provider);
    if !constraints.is_empty() {
        return Err(failure(
            "provider_unavailable",
            format!(
                "Provider {instance} cannot run a child task: {}",
                constraints.join(" ")
            ),
        ));
    }
    let same_instance = inherited["instanceId"] == instance.as_str();
    let model = target["model"]
        .as_str()
        .map(str::to_owned)
        .or_else(|| {
            if same_instance {
                inherited["model"].as_str().map(str::to_owned)
            } else {
                provider["models"][0]["slug"].as_str().map(str::to_owned)
            }
        })
        .ok_or_else(|| {
            failure(
                "model_unavailable",
                format!("Provider {instance} has no model available for inheritance."),
            )
        })?;
    let mut selection = json!({"instanceId": instance, "model": model});
    let requested: Option<Vec<Value>> = match &target["options"] {
        Value::Array(options) => Some(options.clone()),
        Value::Object(options) => Some(
            options
                .iter()
                .map(|(id, value)| json!({"id": id, "value": value}))
                .collect(),
        ),
        _ => None,
    };
    let mut options = (same_instance && inherited["model"] == model.as_str())
        .then(|| records(inherited, "options"))
        .filter(|o| !o.is_empty());
    if let Some(requested) = requested {
        let mut merged: Vec<Value> = if requested.is_empty() {
            vec![]
        } else {
            options.clone().unwrap_or_default()
        };
        for selection in requested {
            merged.retain(|o| o["id"] != selection["id"]);
            merged.push(selection);
        }
        options = Some(merged);
    }
    if let Some(options) = options {
        selection["options"] = Value::Array(options);
    }
    Ok(selection)
}

/// `resolveRuntimeMode` and `resolveInteractionMode`: `runtime` and
/// `interaction` for work this thread starts or stops, never broader than
/// the thread's own.
fn resolve_modes<'a>(
    parent: &'a Value,
    runtime: Option<&'a str>,
    interaction: Option<&'a str>,
) -> Result<(&'a str, &'a str), Refusal> {
    let parent_runtime = parent["runtimeMode"].as_str().unwrap_or("full-access");
    let runtime = match runtime {
        None | Some("inherit") => parent_runtime,
        Some(mode) => mode,
    };
    if rank(runtime).unwrap_or(0) > rank(parent_runtime).unwrap_or(0) {
        return Err(failure(
            "runtime_mode_escalation_denied",
            format!("Child runtime mode {runtime} is broader than parent mode {parent_runtime}."),
        ));
    }
    let parent_interaction = parent["interactionMode"].as_str().unwrap_or("default");
    let interaction = match interaction {
        None | Some("inherit") => parent_interaction,
        Some(mode) => mode,
    };
    if interaction == "default" && parent_interaction == "plan" {
        return Err(failure(
            "interaction_mode_escalation_denied",
            format!("Child interaction mode {interaction} is broader than parent mode {parent_interaction}."),
        ));
    }
    Ok((runtime, interaction))
}

/// The modes a child task runs in.
fn child_modes(parent: &Value, a: &Value) -> Result<(String, String), Refusal> {
    let (runtime, interaction) = resolve_modes(
        parent,
        arg_str(a, "runtimeMode")?,
        arg_str(a, "interactionMode")?,
    )?;
    Ok((runtime.to_owned(), interaction.to_owned()))
}

/// `taskPrompt`: the task as its child receives it, with the role asked for.
fn task_prompt(a: &Value) -> String {
    let task = a["task"].as_str().unwrap_or_default();
    match a["role"].as_str() {
        None | Some("general") => task.to_owned(),
        Some(role) => format!("Act as the {role} sub-agent for this task.\n\n{task}"),
    }
}

pub(super) async fn delegate_task<S: McpHost>(
    host: &Arc<S>,
    caller: &Caller,
    a: &Value,
) -> Outcome {
    thread_scope(caller, "This tool")?;
    threads::writing_caller(&**host, caller).await?;
    required(a, "task")?;
    let target = a.get("target").cloned().unwrap_or(Value::Null);
    if target["environmentId"]
        .as_str()
        .is_some_and(|env| env != host.environment_id())
    {
        return Err(failure(
            "invalid_request",
            "A task in a linked environment is not available on this server yet.",
        ));
    }
    let scope = thread_scope(caller, "delegate_task")?;
    let (parent, parent_runs) = {
        let store = host.control_state().store.read().await;
        let thread = find_thread(&store, &scope.thread_id)?;
        let projection = thread.full_projection();
        (projection["thread"].clone(), records(&projection, "runs"))
    };
    if target["projectId"]
        .as_str()
        .is_some_and(|project| parent["projectId"] != project)
    {
        return Err(failure(
            "invalid_request",
            "A local task runs in this thread's project. Omit target.projectId, or pass target.environmentId to run it in that environment's project.",
        ));
    }
    let parent_run = parent_runs
        .iter()
        .filter(|run| {
            matches!(
                run["status"].as_str(),
                Some("preparing" | "starting" | "running" | "waiting")
            )
        })
        .max_by_key(|run| run["ordinal"].as_u64().unwrap_or(0))
        .cloned();
    let Some(parent_run) = parent_run.filter(|run| {
        !run["rootNodeId"].is_null()
            && run["providerInstanceId"] == scope.provider_instance_id.as_str()
    }) else {
        return Err(failure(
            "parent_not_active",
            "Delegated tasks require an active run owned by this MCP provider session.",
        ));
    };
    let providers = host.providers().as_array().cloned().unwrap_or_default();
    let key = arg_str(a, "clientRequestId")?
        .map(str::to_owned)
        .unwrap_or_else(|| crate::platform::uuid_v4().to_string());
    let command_id = stable_command_id(caller, "delegate-task", &key);
    let selection = resolve_target(&providers, &parent, &target)?;
    let (runtime, interaction) = child_modes(&parent, a)?;
    if a["isolation"] == "worktree" && !host.prepares_worktrees() {
        return Err(failure(
            "invalid_request",
            "Isolation \"worktree\" is not available on this server yet. Use isolation \"shared\".",
        ));
    }
    let retry = host
        .control_state()
        .store
        .read()
        .await
        .command_receipts
        .contains_key(&command_id);
    let workspace_strategy = if a["isolation"] == "worktree" && !retry {
        Some(worktree_strategy(host, &parent).await?)
    } else {
        None
    };
    let wait = a["mode"] == "wait";
    let mut command = json!({
        "type": "delegated_task.request",
        "createdBy": "agent",
        "creationSource": "mcp",
        "commandId": command_id,
        "parentThreadId": scope.thread_id,
        "parentRunId": parent_run["id"],
        "parentNodeId": parent_run["rootNodeId"],
        "task": task_prompt(a),
        "modelSelection": selection,
        "runtimeMode": runtime,
        "interactionMode": interaction,
        "completionWake": if wait { "settled_only" } else { "always" },
    });
    if let Some(title) = arg_str(a, "title")? {
        command["title"] = json!(title);
    }
    if let Some(strategy) = workspace_strategy {
        command["workspaceStrategy"] = strategy;
    }
    let run_end = wait.then(|| {
        crate::orchestration::watch_run_end(&crate::orchestration::task_thread_id(&command_id))
    });
    host.dispatch(command).await.map_err(|error| {
        failure(
            "orchestration_error",
            format!(
                "Unable to create delegated task: {}",
                error["detail"]
                    .as_str()
                    .or(error["message"].as_str())
                    .unwrap_or("The command was rejected.")
            ),
        )
    })?;
    let task_id = crate::orchestration::task_node_id(&command_id);
    // A child that waits for its worktree is prepared now, in the background.
    let child_id = crate::orchestration::task_thread_id(&command_id);
    let preparing = {
        let store = host.control_state().store.read().await;
        store.threads.get(&child_id).and_then(|thread| {
            let row = thread.stored_row();
            thread
                .runs
                .iter()
                .find(|run| {
                    run.status == "preparing"
                        && row["runExtras"][run.wire_id()]["workspacePreparation"].is_object()
                })
                .map(|run| run.wire_id())
        })
    };
    if let Some(run_id) = preparing {
        host.prepare_delegated_run(command_id.clone(), child_id, run_id);
    }
    if wait {
        let timeout = a["timeoutMs"]
            .as_f64()
            .map_or(WAIT_DEFAULT_MS, |ms| ms.max(1.0) as u64)
            .min(WAIT_MAX_MS);
        return await_task(host, caller, &task_id, &key, run_end, timeout).await;
    }
    read_task(host, caller, &task_id, false, true).await
}

/// The worktree a child with `isolation: "worktree"` gets: a checkout of the parent's
/// commit, made from the project's repository.
async fn worktree_strategy<S: McpHost>(host: &Arc<S>, parent: &Value) -> Result<Value, Refusal> {
    let (root, title) = {
        let store = host.control_state().store.read().await;
        let project = parent["projectId"]
            .as_str()
            .and_then(|id| store.project_rows.get(id))
            .filter(|row| row["deletedAt"].is_null())
            .ok_or_else(|| failure("project_not_found", "The project was not found."))?;
        (
            project["workspaceRoot"]
                .as_str()
                .unwrap_or_default()
                .to_owned(),
            project["title"].as_str().unwrap_or_default().to_owned(),
        )
    };
    let resolve = |cwd: String, revision: String| async move {
        host.workspace_call(
            "vcs.resolveCommit".into(),
            json!({"cwd": cwd, "revision": revision}),
        )
        .await
        .ok()
        .and_then(|reply| reply["commitSha"].as_str().map(str::to_owned))
    };
    let parent_checkout = parent["worktreePath"].as_str().map(str::to_owned);
    let base = resolve(
        parent_checkout.clone().unwrap_or_else(|| root.clone()),
        "HEAD".into(),
    )
    .await
    .ok_or_else(|| {
        failure(
            "invalid_request",
            "Unable to resolve the parent checkout commit.",
        )
    })?;
    // The worktree is made from the project's repository, which must hold that commit.
    if parent_checkout.is_some() && resolve(root.clone(), base.clone()).await.is_none() {
        return Err(failure(
            "invalid_request",
            format!(
                "Isolation \"worktree\" creates the child checkout in project \"{title}\" ({root}), which does not contain the parent checkout's commit. Use isolation \"shared\" to work in the parent's checkout."
            ),
        ));
    }
    Ok(json!({"type": "worktree", "baseRef": base, "startFromOrigin": false}))
}

pub(super) async fn task_status<S: McpHost>(host: &Arc<S>, caller: &Caller, a: &Value) -> Outcome {
    thread_scope(caller, "This tool")?;
    threads::writing_caller(&**host, caller).await?;
    let task_id = required(a, "taskId")?.to_owned();
    thread_scope(caller, "task_status")?;
    read_task(host, caller, &task_id, false, true).await
}

/// `agentStopReason`.
fn agent_stop_reason(reason: Option<&str>) -> String {
    match reason {
        None => "Stopped by an agent".to_owned(),
        Some(reason) => format!("Stopped by an agent: {reason}"),
    }
}

/// `cancelTask`: stops a task's child, its delegates and the task's queued
/// turn. A finished task keeps its result.
pub(super) async fn task_cancel<S: McpHost>(host: &Arc<S>, caller: &Caller, a: &Value) -> Outcome {
    thread_scope(caller, "This tool")?;
    threads::writing_caller(&**host, caller).await?;
    let task_id = required(a, "taskId")?.to_owned();
    let reason = arg_str(a, "reason")?.map(str::to_owned);
    let scope = thread_scope(caller, "task_cancel")?;
    let current = read_task(host, caller, &task_id, false, false).await?;
    let child_id = current["childThreadId"]
        .as_str()
        .unwrap_or_default()
        .to_owned();
    let key = arg_str(a, "clientRequestId")?
        .map(str::to_owned)
        .unwrap_or_else(|| crate::platform::uuid_v4().to_string());
    let (parent_task, child_runs) = {
        let store = host.control_state().store.read().await;
        let parent = find_thread(&store, &scope.thread_id)?.full_projection();
        // Cancelling stops the child and every task under it, each a write to a
        // thread its user may have raised above the parent's modes since it was
        // delegated. All of them are checked before anything is stopped.
        let mut pending = vec![child_id.clone()];
        let mut seen = std::collections::HashSet::new();
        while let Some(id) = pending.pop() {
            if !seen.insert(id.clone()) {
                continue;
            }
            let Some(thread) = store.threads.get(&id) else {
                continue;
            };
            let row = thread.stored_row();
            if row["deletedAt"].is_null() {
                resolve_modes(
                    &parent["thread"],
                    row["runtimeMode"].as_str(),
                    row["interactionMode"].as_str(),
                )?;
            }
            pending.extend(
                records(&row, "subagents")
                    .iter()
                    .filter(|t| t["origin"] == "app_owned")
                    .filter_map(|t| t["childThreadId"].as_str().map(str::to_owned)),
            );
        }
        let parent_task = records(&parent, "subagents")
            .into_iter()
            .find(|t| t["id"] == task_id.as_str() && t["origin"] == "app_owned");
        let child_runs = store
            .threads
            .get(&child_id)
            .map(|t| records(&t.full_projection(), "runs"))
            .unwrap_or_default();
        (parent_task, child_runs)
    };
    let dispatch_failure = |code: &'static str, what: String| {
        move |error: Value| {
            failure(
                code,
                format!(
                    "{what}: {}",
                    error["detail"]
                        .as_str()
                        .or(error["message"].as_str())
                        .unwrap_or("The command was rejected.")
                ),
            )
        }
    };
    let dispose = || async {
        if parent_task
            .as_ref()
            .is_some_and(|t| t["completionDelivery"]["state"] == "disposed")
        {
            return Ok(());
        }
        host.dispatch(json!({
            "type": "delegated_task.completion-delivery.dispose",
            "commandId": stable_command_id(caller, "cancel-task-completion-delivery", &key),
            "parentThreadId": scope.thread_id,
            "taskId": task_id,
        }))
        .await
        .map(|_| ())
        .map_err(dispatch_failure(
            "orchestration_error",
            format!("Unable to dispose delegated task {task_id} completion delivery"),
        ))
    };
    let stop_child = || async {
        let stop = json!({
            "type": "thread.stop",
            "commandId": stable_command_id(caller, "cancel-task", &key),
            "threadId": child_id,
            "stopDelegatedTasks": false,
            "reason": agent_stop_reason(reason.as_deref()),
        });
        host.cancel_delegated_task(scope.thread_id.clone(), task_id.clone(), stop)
            .await
            .map_err(dispatch_failure(
                "task_not_cancellable",
                format!("Unable to stop delegated task {task_id}"),
            ))?;
        let runs = {
            let store = host.control_state().store.read().await;
            store
                .threads
                .get(&child_id)
                .map(|t| records(&t.full_projection(), "runs"))
                .unwrap_or_default()
        };
        // Stop preserves the queue for Resume. Cancelling a task must also
        // remove that task's queued turn, including a previously held one.
        for run in runs.iter().filter(|r| {
            r["status"] == "queued"
                && (r["delegatedTaskId"] == task_id.as_str() || r["id"] == current["childRunId"])
        }) {
            let run_id = run["id"].as_str().unwrap_or_default();
            host.dispatch(json!({
                "type": "queued-run.cancel",
                "commandId": stable_command_id(caller, &format!("cancel-task-queued-run:{run_id}"), &key),
                "threadId": child_id,
                "runId": run_id,
            }))
            .await
            .map_err(dispatch_failure(
                "orchestration_error",
                format!("Unable to cancel delegated task {task_id}'s queued turn"),
            ))?;
        }
        Ok::<(), Refusal>(())
    };
    // A follow-up that lost its child run stays running on the parent, while
    // its status here comes from the child's last run. Stop on the parent
    // cancels that task.
    let cancel_orphan = || async {
        let Some(task) = parent_task.as_ref().filter(|t| {
            !t["runId"].is_null()
                && matches!(
                    t["status"].as_str(),
                    Some("pending" | "starting" | "running" | "waiting")
                )
        }) else {
            return Ok::<bool, Refusal>(false);
        };
        host.dispatch(json!({
            "type": "subagent.stop",
            "commandId": stable_command_id(caller, "cancel-task-orphan", &key),
            "threadId": scope.thread_id,
            "runId": task["runId"],
            "subagentId": task["id"],
        }))
        .await
        .map_err(dispatch_failure(
            "orchestration_error",
            "Unable to stop the task".into(),
        ))?;
        let store = host.control_state().store.read().await;
        Ok(find_thread(&store, &scope.thread_id)
            .map(|t| {
                records(&t.stored_row(), "subagents")
                    .iter()
                    .any(|t| t["id"] == task_id.as_str() && t["status"] == "cancelled")
            })
            .unwrap_or(false))
    };
    let status = current["status"].as_str().unwrap_or_default();
    if TERMINAL_TASK.contains(&status) {
        // The child can still run work for another turn, so it is stopped either way.
        stop_child().await?;
        let cancelled = cancel_orphan().await?;
        dispose().await?;
        return Ok(json!({
            "taskId": task_id,
            "status": if cancelled { "cancelled" } else { status },
        }));
    }
    let active_child = child_runs.iter().any(|r| {
        matches!(
            r["status"].as_str(),
            Some("preparing" | "starting" | "running" | "waiting")
        )
    });
    if current["workState"] != "waiting_for_children"
        && !active_child
        && !child_runs.iter().any(|r| {
            r["status"] == "queued"
                && (r["delegatedTaskId"] == task_id.as_str() || r["id"] == current["childRunId"])
        })
    {
        if cancel_orphan().await? {
            return Ok(json!({"taskId": task_id, "status": "cancelled"}));
        }
        return Err(failure(
            "task_not_cancellable",
            format!("Delegated task {task_id} has no interruptible child run."),
        ));
    }
    stop_child().await?;
    if let Err(error) = dispose().await {
        let _ = error;
        tracing::warn!(task_id, "could not dispose a cancelled task's delivery");
    }
    Ok(json!({"taskId": task_id, "status": "cancel_requested"}))
}

/// `createThreads`: new top-level threads in this thread's project, each
/// started with its prompt and recorded on the parent's timeline.
pub(super) async fn create_threads<S: McpHost>(
    host: &Arc<S>,
    caller: &Caller,
    a: &Value,
) -> Outcome {
    thread_scope(caller, "This tool")?;
    threads::writing_caller(&**host, caller).await?;
    let requests = a["threads"].as_array().cloned().unwrap_or_default();
    let scope = thread_scope(caller, "create_threads")?;
    let (parent, parent_runs) = {
        let store = host.control_state().store.read().await;
        let thread = find_thread(&store, &scope.thread_id)?;
        let projection = thread.full_projection();
        (projection["thread"].clone(), records(&projection, "runs"))
    };
    let parent_run = parent_runs
        .iter()
        .filter(|run| {
            matches!(
                run["status"].as_str(),
                Some("preparing" | "starting" | "running" | "waiting")
            )
        })
        .max_by_key(|run| run["ordinal"].as_u64().unwrap_or(0))
        .cloned()
        .filter(|run| {
            !run["rootNodeId"].is_null()
                && run["providerInstanceId"] == scope.provider_instance_id.as_str()
        })
        .ok_or_else(|| {
            failure(
                "parent_not_active",
                "Thread creation requires an active run owned by this MCP provider session.",
            )
        })?;
    let providers = host.providers().as_array().cloned().unwrap_or_default();
    let key = arg_str(a, "clientRequestId")?
        .map(str::to_owned)
        .unwrap_or_else(|| crate::platform::uuid_v4().to_string());
    let mut created = Vec::new();
    for (index, request) in requests.iter().enumerate() {
        let command_id = stable_indexed(caller, "command", Some("create-thread"), &key, index);
        let thread_id = stable_indexed(caller, "thread", None, &key, index);
        let committed = {
            let store = host.control_state().store.read().await;
            store
                .command_receipts
                .contains_key(&command_id)
                .then(|| {
                    store
                        .threads
                        .get(&thread_id)
                        .map(|t| t.full_projection()["thread"]["modelSelection"].clone())
                })
                .flatten()
        };
        let mut target = request.get("target").cloned().unwrap_or(Value::Null);
        if let Some(selection) = &committed {
            if !target.is_object() {
                target = json!({});
            }
            target["providerInstanceId"] = selection["instanceId"].clone();
        }
        if target["environmentId"]
            .as_str()
            .is_some_and(|env| env != host.environment_id())
        {
            return Err(failure(
                "invalid_request",
                "A thread in a linked environment is not available on this server yet.",
            ));
        }
        let selection = resolve_target(&providers, &parent, &target)?;
        let (runtime, interaction) = resolve_modes(
            &parent,
            request["runtimeMode"].as_str(),
            request["interactionMode"].as_str(),
        )?;
        let prompt = request["prompt"].as_str();
        let title = created_thread_title(
            parent["title"].as_str().unwrap_or_default(),
            prompt,
            request["title"].as_str(),
            index,
        );
        let initial = prompt.map(|text| {
            json!({
                "senderThreadId": scope.thread_id,
                "messageId": stable_indexed(caller, "message", None, &key, index),
                "text": text,
                "attachments": [],
            })
        });
        let mut create = json!({
            "type": "thread.create",
            "createdBy": "agent",
            "creationSource": "mcp",
            "commandId": command_id,
            "threadId": thread_id,
            "projectId": parent["projectId"],
            "title": title,
            "modelSelection": selection,
            "runtimeMode": runtime,
            "interactionMode": interaction,
            "branch": parent["branch"],
            "worktreePath": parent["worktreePath"],
        });
        if parent["linkOrigin"].is_object() {
            create["linkOrigin"] = parent["linkOrigin"].clone();
        }
        if let Some(initial) = &initial {
            create["initialMessage"] = initial.clone();
        }
        let number = index + 1;
        let reason = |error: &Value| {
            error["detail"]
                .as_str()
                .or(error["message"].as_str())
                .unwrap_or("The command was rejected.")
                .to_owned()
        };
        host.dispatch(create).await.map_err(|error| {
            failure(
                "orchestration_error",
                format!("Unable to create thread {number}: {}", reason(&error)),
            )
        })?;
        // A receipt can appear after target resolution; use the committed thread's selection.
        let (resolved, has_run) = {
            let store = host.control_state().store.read().await;
            let thread = find_thread(&store, &thread_id)?;
            let projection = thread.full_projection();
            let message_id = initial.as_ref().map(|m| m["messageId"].clone());
            let has_run = records(&projection, "runs")
                .iter()
                .any(|run| Some(&run["userMessageId"]) == message_id.as_ref());
            (projection["thread"]["modelSelection"].clone(), has_run)
        };
        // A replayed creation that carried no prompt has no run yet.
        if let Some(initial) = initial.as_ref().filter(|_| !has_run) {
            let mut dispatch = initial.clone();
            dispatch["type"] = json!("message.dispatch");
            dispatch["createdBy"] = json!("agent");
            dispatch["creationSource"] = json!("mcp");
            dispatch["commandId"] = json!(stable_indexed(
                caller,
                "command",
                Some("dispatch-thread"),
                &key,
                index
            ));
            dispatch["threadId"] = json!(thread_id);
            dispatch["modelSelection"] = resolved.clone();
            dispatch["dispatchMode"] = json!({"type": "start_immediately"});
            host.dispatch(dispatch).await.map_err(|error| {
                failure(
                    "orchestration_error",
                    format!("Unable to start thread {number}: {}", reason(&error)),
                )
            })?;
        }
        let (run, thread_title, thread_row) = {
            let store = host.control_state().store.read().await;
            let thread = find_thread(&store, &thread_id)?;
            let projection = thread.full_projection();
            (
                records(&projection, "runs").last().cloned(),
                thread.title.clone(),
                projection["thread"].clone(),
            )
        };
        host.dispatch(json!({
            "type": "thread.created.record",
            "commandId": stable_indexed(caller, "command", Some("record-created-thread"), &key, index),
            "parentThreadId": scope.thread_id,
            "parentRunId": parent_run["id"],
            "parentNodeId": parent_run["rootNodeId"],
            "targetThreadId": thread_id,
            "targetRunId": run.as_ref().map_or(Value::Null, |r| r["id"].clone()),
        }))
        .await
        .map_err(|error| {
            failure(
                "orchestration_error",
                format!(
                    "Unable to record thread {number} in the parent timeline: {}",
                    reason(&error)
                ),
            )
        })?;
        created.push(json!({
            "threadId": thread_id,
            "link": thread_link(&**host, caller, &thread_id, &thread_title),
            "runId": run.as_ref().map_or(Value::Null, |r| r["id"].clone()),
            // Rust records a run as running from its start; the reference's run is
            // starting until its provider turn has an attempt.
            "status": run.as_ref().map_or(json!("idle"), |r| {
                if r["status"] == "running" && r["activeAttemptId"].is_null() {
                    json!("starting")
                } else {
                    r["status"].clone()
                }
            }),
            "title": thread_title,
            "createdBy": thread_row["createdBy"],
            "creationSource": thread_row["creationSource"],
            "providerInstanceId": resolved["instanceId"],
            "model": resolved["model"],
        }));
    }
    Ok(json!({"threads": created}))
}

/// `awaitTask`: reads the task until it ends a wait or `timeout` passes.
/// A wait that ends without a terminal result upgrades the task's wake, so a
/// later result still reaches the parent.
async fn await_task<S: McpHost>(
    host: &Arc<S>,
    caller: &Caller,
    task_id: &str,
    request_key: &str,
    mut run_end: Option<tokio::sync::oneshot::Receiver<crate::orchestration::RunEnd>>,
    timeout: u64,
) -> Outcome {
    let deadline = crate::platform::now_ms() + timeout as i64;
    // `waitForTask`: a run end is read before the task settles, and answered
    // once `after_run` has settled it.
    let mut ended: Option<crate::orchestration::RunEnd> = None;
    let waited = loop {
        // A read at a run end, before the task settles, does not acknowledge
        // the result: the reference acknowledges once the task's completion
        // is recorded.
        let read = read_task(host, caller, task_id, false, ended.is_none()).await;
        if let Some(crate::orchestration::RunEnd {
            read: done,
            settled,
        }) = ended.take()
        {
            drop(done);
            let _ = settled.await;
            let _ = read_task(host, caller, task_id, false, true).await;
        }
        let read = read?;
        let terminal = TERMINAL_TASK.contains(&read["status"].as_str().unwrap_or_default());
        // A settled task has its result transfer; its run end has passed.
        if terminal && ended.is_none() && read["resultContextTransferId"].is_null() {
            // A poll that sees the run end before `after_run` does reads again
            // on its signal, as the reference reads on the run's event.
            if let Some(rx) = run_end.take() {
                let left = (deadline - crate::platform::now_ms()).max(0) as u64;
                let pause = host.sleep(std::time::Duration::from_millis(left.min(5_000)));
                tokio::select! {
                    _ = pause => {}
                    end = rx => {
                        ended = end.ok();
                        if ended.is_some() {
                            continue;
                        }
                    }
                }
            }
        }
        if ends_wait(&read) {
            break Some(read);
        }
        if run_end.is_none() {
            if let Some(child) = read["childThreadId"].as_str() {
                run_end = Some(crate::orchestration::watch_run_end(child));
            }
        }
        let left = deadline - crate::platform::now_ms();
        if left <= 0 {
            break None;
        }
        let pause = host.sleep(std::time::Duration::from_millis(left.min(250) as u64));
        match run_end.as_mut() {
            Some(rx) => {
                tokio::select! {
                    _ = pause => {}
                    end = rx => {
                        ended = end.ok();
                        run_end = None;
                    }
                }
            }
            None => pause.await,
        }
    };
    if let Some(read) = waited
        .as_ref()
        .filter(|r| TERMINAL_TASK.contains(&r["status"].as_str().unwrap_or_default()))
    {
        return Ok(read.clone());
    }
    let _ = host
        .dispatch(json!({
            "type": "delegated_task.wake-policy",
            "commandId": stable_command_id(caller, "delegate-task-wake-policy", request_key),
            "parentThreadId": caller.granted.thread.as_ref().map(|t| t.thread_id.clone()),
            "taskId": task_id,
            "completionWake": "always",
        }))
        .await;
    read_task(host, caller, task_id, waited.is_none(), true).await
}

/// `endsTaskWait`.
fn ends_wait(result: &Value) -> bool {
    result["hasPendingQuestion"] == true
        || !result["pendingApproval"].is_null()
        || TERMINAL_TASK.contains(&result["status"].as_str().unwrap_or_default())
}

/// `taskStatusForRun`. Rust records a run as running from its start; the
/// reference's run is starting until its provider turn has an attempt.
fn status_for_run(run: Option<&Value>) -> &'static str {
    match run.and_then(|r| r["status"].as_str()) {
        Some("running") if run.is_some_and(|r| r["activeAttemptId"].is_null()) => "starting",
        Some("queued") => "queued",
        Some("starting") => "starting",
        Some("waiting") => "waiting",
        Some("completed") => "completed",
        Some("failed") => "failed",
        Some("cancelled" | "rolled_back") => "cancelled",
        Some("interrupted") => "interrupted",
        _ => "running",
    }
}

/// Waits, briefly, until `run_id` of `thread_id` has recorded its start
/// (no longer `preparing` or `starting`) and, for a Claude run, the CLI's
/// receipt of its prompt: a tool call can reach this server before the
/// provider events that precede it are recorded, while the reference
/// records them before its provider gets the prompt.
async fn await_run_start<S: McpHost>(host: &Arc<S>, thread_id: &str, run_id: &str) {
    for _ in 0..100 {
        let pending = {
            let store = host.control_state().store.read().await;
            store.threads.get(thread_id).is_some_and(|thread| {
                let starting = records(&thread.full_projection(), "runs")
                    .iter()
                    .any(|run| {
                        run["id"] == run_id
                            && matches!(run["status"].as_str(), Some("preparing" | "starting"))
                    });
                let undelivered = thread.runs.iter().any(|run| {
                    run.wire_id() == run_id
                        && run.driver.as_deref() == Some(crate::workspace::provider::CLAUDE_AGENT)
                        && !thread
                            .provider
                            .runs
                            .get(&run.id)
                            .is_some_and(|record| record.prompt_delivered)
                });
                starting || undelivered
            })
        };
        if !pending {
            return;
        }
        host.sleep(std::time::Duration::from_millis(20)).await;
    }
}

/// `readTask`: the task as the parent's agent sees it.
async fn read_task<S: McpHost>(
    host: &Arc<S>,
    caller: &Caller,
    task_id: &str,
    wait_timed_out: bool,
    acknowledge: bool,
) -> Outcome {
    let scope = thread_scope(caller, "task_status")?;
    let (task, child, parent_runs, open) = {
        let store = host.control_state().store.read().await;
        let parent = find_thread(&store, &scope.thread_id)?;
        let parent_projection = parent.full_projection();
        let subagents = records(&parent_projection, "subagents");
        let open = open_delegated_tasks(&subagents, &records(&parent_projection, "runs"));
        let task = subagents.into_iter().find(|t| {
            t["id"] == task_id
                && t["origin"] == "app_owned"
                && t["threadId"] == scope.thread_id.as_str()
        });
        let child = task
            .as_ref()
            .and_then(|t| t["childThreadId"].as_str())
            .and_then(|id| store.threads.get(id))
            .map(|t| t.full_projection());
        (task, child, records(&parent_projection, "runs"), open)
    };
    let (Some(task), Some(child)) = (task, child) else {
        return Err(task_not_found(task_id, &scope.thread_id, &open));
    };
    let runs = records(&child, "runs");
    let transfers = records(&child, "contextTransfers");
    // `delegatedTaskRun`: a follow-up's first run, else the run its spawn started.
    let child_run = runs
        .iter()
        .find(|r| r["delegatedTaskId"] == task["id"])
        .cloned()
        .or_else(|| {
            transfers
                .iter()
                .find(|t| {
                    t["type"] == "subagent_spawn"
                        && t["sourceThreadId"] == task["threadId"]
                        && t["targetThreadId"] == task["childThreadId"]
                })
                .map_or_else(
                    || runs.first().cloned(),
                    |t| runs.iter().find(|r| r["id"] == t["targetRunId"]).cloned(),
                )
        });
    let terminal_run = |r: &Value| TERMINAL_RUN.contains(&r["status"].as_str().unwrap_or_default());
    let work_runs: Vec<&Value> = runs
        .iter()
        .filter(|r| r["status"] != "rolled_back")
        .collect();
    let active = work_runs
        .iter()
        .any(|r| !(terminal_run(r) || r["status"] == "queued" && r["queueHeld"] == true));
    let result_run = work_runs
        .iter()
        .filter(|r| terminal_run(r) && (!r["startedAt"].is_null() || r["ordinal"] == 1))
        .max_by_key(|r| r["ordinal"].as_u64().unwrap_or(0))
        .map(|r| (*r).clone());
    let holds = records(&child, "subagents").iter().any(|t| {
        matches!(
            t["status"].as_str(),
            Some("pending" | "running" | "waiting" | "starting")
        )
    });
    let work_state = if active || result_run.is_none() {
        "working"
    } else if holds {
        "waiting_for_children"
    } else {
        "result_available"
    };
    // A follow-up's progress comes from its own runs.
    let followup_runs = child_run
        .as_ref()
        .filter(|run| run["delegatedTaskId"] == task["id"])
        .map(|_| crate::orchestration::delegated_task_runs(&child, &task["id"]));
    let (result_run, work_state) = match &followup_runs {
        None => (result_run, work_state),
        Some(task_runs) => {
            let run = crate::orchestration::delegated_task_result_run(task_runs);
            let state = if !TERMINAL_TASK.contains(&status_for_run(run.as_ref())) {
                "working"
            } else if crate::orchestration::delegated_task_awaits_work(task_runs, &child) {
                "waiting_for_children"
            } else {
                "result_available"
            };
            (run, state)
        }
    };
    let work_state = if !task["result"].is_null() {
        "result_available"
    } else {
        work_state
    };
    let terminal = runs
        .iter()
        .filter(|r| {
            terminal_run(r)
                && r["status"] != "rolled_back"
                && (child_run.as_ref().is_some_and(|c| c["id"] == r["id"])
                    || !r["startedAt"].is_null())
        })
        .max_by_key(|r| r["ordinal"].as_u64().unwrap_or(0))
        .cloned();
    let run_status = status_for_run(result_run.as_ref().or(child_run.as_ref()));
    let status = if !task["result"].is_null() {
        if TERMINAL_TASK.contains(&task["status"].as_str().unwrap_or_default()) {
            task["status"].as_str().unwrap_or("running")
        } else {
            status_for_run(child_run.as_ref())
        }
    } else if work_state == "result_available" {
        if !task["cancelledRunId"].is_null() && run_status == "completed" {
            "interrupted"
        } else {
            run_status
        }
    } else if matches!(run_status, "queued" | "starting") {
        run_status
    } else {
        "running"
    };
    let summary = if !task["result"].is_null() {
        task["result"].clone()
    } else {
        match &result_run {
            Some(run) if TERMINAL_TASK.contains(&status) => {
                json!(crate::orchestration::subagent_result(&child, run))
            }
            _ => Value::Null,
        }
    };
    // `taskResultTransfer`: the result sent from one of the task's own runs.
    let original_tagged = child_run
        .as_ref()
        .is_some_and(|run| !run["delegatedTaskId"].is_null());
    let result_run_ids: Vec<&Value> = runs
        .iter()
        .filter(|run| {
            run["delegatedTaskId"] == task["id"]
                || (!original_tagged && run["delegatedTaskId"].is_null())
        })
        .map(|run| &run["id"])
        .chain(child_run.as_ref().map(|run| &run["id"]))
        .collect();
    let result_transfer = transfers
        .iter()
        .find(|t| {
            t["type"] == "subagent_result"
                && t["sourceThreadId"] == task["childThreadId"]
                && t["targetThreadId"] == scope.thread_id.as_str()
                && (result_run_ids.contains(&&t["sourcePoint"]["runId"])
                    || (t["sourcePoint"]["runId"].is_null() && !original_tagged))
        })
        .map(|t| t["id"].clone())
        .unwrap_or(Value::Null);
    let terminal_status = terminal.as_ref().map(|r| status_for_run(Some(r)));
    // `canExposeTaskRunResult`.
    fn exposable(run: &Option<Value>) -> Option<&Value> {
        run.as_ref().filter(|r| {
            r["status"] != "rolled_back" && TERMINAL_TASK.contains(&status_for_run(Some(r)))
        })
    }
    let is_child_run = |run: &Value| child_run.as_ref().is_some_and(|c| c["id"] == run["id"]);
    let latest_summary = match exposable(&terminal) {
        Some(run) if is_child_run(run) => summary.clone(),
        Some(run) => json!(crate::orchestration::subagent_result(&child, run)),
        None => Value::Null,
    };
    let latest_transfer = exposable(&terminal)
        .and_then(|run| {
            let results = || {
                transfers.iter().filter(|t| {
                    t["type"] == "subagent_result"
                        && t["sourceThreadId"] == task["childThreadId"]
                        && t["targetThreadId"] == scope.thread_id.as_str()
                })
            };
            results()
                .find(|t| t["sourcePoint"]["runId"] == run["id"])
                .or_else(|| {
                    is_child_run(run)
                        .then(|| results().find(|t| t["sourcePoint"]["runId"].is_null()))
                        .flatten()
                })
        })
        .map_or(Value::Null, |t| t["id"].clone());
    let execution = if task["result"].is_null() {
        result_run.as_ref().or(child_run.as_ref())
    } else {
        transfers
            .iter()
            .find(|t| t["id"] == result_transfer)
            .and_then(|t| runs.iter().find(|r| r["id"] == t["sourcePoint"]["runId"]))
            .or(child_run.as_ref())
    };
    // `runExecutedModel`: the model of the run's latest provider turn.
    let executed_model = execution
        .filter(|run| !run["activeAttemptId"].is_null())
        .and_then(|run| {
            records(&child, "providerTurns")
                .into_iter()
                .filter(|t| t["runAttemptId"] == run["activeAttemptId"])
                .max_by_key(|t| t["ordinal"].as_u64().unwrap_or(0))
        })
        .map_or(Value::Null, |t| t["model"].clone());
    let selection = execution
        .map(|r| r["modelSelection"].clone())
        .filter(Value::is_object)
        .unwrap_or_else(|| child["thread"]["modelSelection"].clone());
    let has_pending_child_runs = runs.iter().any(|r| {
        !(terminal_run(r) || r["status"] == "queued" && r["queueHeld"] == true)
            && child_run
                .as_ref()
                .is_none_or(|c| r["ordinal"].as_u64() > c["ordinal"].as_u64())
    });
    let title = child["thread"]["title"]
        .as_str()
        .unwrap_or_default()
        .to_owned();
    let child_id = task["childThreadId"]
        .as_str()
        .unwrap_or_default()
        .to_owned();
    // Only a latest result that differs from the task's own is repeated.
    let latest_summary = if latest_summary == summary {
        Value::Null
    } else {
        latest_summary
    };
    let latest_transfer = if latest_transfer == result_transfer {
        Value::Null
    } else {
        latest_transfer
    };
    let mut result = json!({
        "taskId": task["id"],
        "childThreadId": child_id,
        "link": thread_link(&**host, caller, &child_id, &title),
        "childRunId": child_run.as_ref().map(|r| r["id"].clone()).unwrap_or(Value::Null),
        "childNodeId": task["id"],
        "status": status,
        "workState": work_state,
        "hasPendingChildRuns": has_pending_child_runs,
        "providerInstanceId": selection["instanceId"],
        "model": selection["model"],
        "executedModel": executed_model,
        "options": selection.get("options").cloned().unwrap_or_else(|| json!([])),
        "summary": summary,
        "resultContextTransferId": result_transfer,
        "latestTerminalRunId": terminal.as_ref().map(|r| r["id"].clone()).unwrap_or(Value::Null),
        "latestTerminalStatus": terminal_status
            .filter(|s| TERMINAL_TASK.contains(s))
            .map_or(Value::Null, |s| json!(s)),
        "latestTerminalSummary": latest_summary,
        "latestTerminalResultContextTransferId": latest_transfer,
        "hasPendingQuestion": !TERMINAL_TASK.contains(&status)
            && records(&child, "runtimeRequests").iter().any(|r| {
                r["kind"] == "user_input" && r["status"] == "pending" && r["responseCapability"]["type"] == "live"
            }),
        "pendingApproval": null,
        "waitTimedOut": wait_timed_out,
    });
    if !ends_wait(&result) && task["completionDelivery"]["state"] != "disposed" {
        result["next"] = json!(RUNNING_TASK_NEXT);
    }
    let delivery = task["completionDelivery"]["state"].as_str();
    if acknowledge
        && TERMINAL_TASK.contains(&status)
        && !matches!(delivery, Some("acknowledged" | "disposed"))
    {
        let observing = parent_runs
            .iter()
            .filter(|r| {
                r["providerInstanceId"] == scope.provider_instance_id.as_str()
                    && matches!(
                        r["status"].as_str(),
                        Some("preparing" | "starting" | "running" | "waiting")
                    )
            })
            .max_by_key(|r| r["ordinal"].as_u64().unwrap_or(0))
            .map(|r| r["id"].clone())
            .unwrap_or(Value::Null);
        // The reference records a run's start before its provider gets the
        // prompt, so whatever the run observes comes after that start.
        if let Some(run_id) = observing.as_str() {
            await_run_start(host, &scope.thread_id, run_id).await;
        }
        host.dispatch(json!({
            "type": "delegated_task.completion-delivery.acknowledge",
            "commandId": stable_command_id(caller, "task-status-acknowledge", &crate::platform::uuid_v4().to_string()),
            "parentThreadId": scope.thread_id,
            "taskId": task_id,
            "observedByRunId": observing,
        }))
        .await
        .map_err(|error| {
            failure(
                "orchestration_error",
                format!(
                    "Unable to acknowledge delegated task {task_id}: {}",
                    error["detail"].as_str().unwrap_or_default()
                ),
            )
        })?;
    }
    Ok(result)
}

/// The delegated tasks a thread still has open, as the reference's
/// `taskNotFound` lists them: app-owned with a child, and not settled (ended,
/// its result delivered, and its run no longer active).
fn open_delegated_tasks(subagents: &[Value], runs: &[Value]) -> Vec<Value> {
    subagents
        .iter()
        .filter(|task| {
            task["origin"] == "app_owned"
                && (!task["childThreadId"].is_null() || !task["remoteChild"].is_null())
        })
        .filter(|task| {
            let ended = matches!(
                task["status"].as_str(),
                Some("completed" | "failed" | "cancelled" | "interrupted")
            );
            let delivered = matches!(
                task["completionDelivery"]["state"].as_str(),
                Some("acknowledged" | "delivered" | "disposed")
            );
            let run_active = runs.iter().any(|run| {
                run["id"] == task["runId"]
                    && matches!(
                        run["status"].as_str(),
                        Some("preparing" | "queued" | "starting" | "running" | "waiting")
                    )
            });
            !(ended && delivered && !run_active)
        })
        .cloned()
        .collect()
}

/// `taskNotFound`: the reference's answer to a task id the thread has no
/// task by, listing up to ten open ones (upstream #40886).
fn task_not_found(task_id: &str, thread_id: &str, open: &[Value]) -> Refusal {
    const MAX_LISTED: usize = 10;
    let listed = open
        .iter()
        .take(MAX_LISTED)
        .map(
            |task| match task["title"].as_str().filter(|t| !t.is_empty()) {
                Some(title) => format!("{} ({title})", task["id"].as_str().unwrap_or_default()),
                None => task["id"].as_str().unwrap_or_default().to_owned(),
            },
        )
        .collect::<Vec<_>>()
        .join(", ");
    let more = if open.len() > MAX_LISTED {
        format!(", and {} more", open.len() - MAX_LISTED)
    } else {
        String::new()
    };
    failure(
        "task_not_found",
        format!(
            "No delegated task with id {task_id} exists on thread {thread_id}. {}",
            if open.is_empty() {
                "This thread has no open delegated tasks.".to_owned()
            } else {
                format!("Open delegated tasks on this thread: {listed}{more}.")
            }
        ),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_created_thread_is_named_by_its_title_prompt_or_place() {
        assert_eq!(
            created_thread_title("P", Some("Say hi"), Some(" Greeter "), 0),
            "Greeter"
        );
        assert_eq!(
            created_thread_title("P", Some(" Say hi "), None, 0),
            "Say hi"
        );
        assert_eq!(created_thread_title("P", None, Some(" "), 1), "P thread 2");
        let long = "b".repeat(81);
        assert_eq!(
            created_thread_title("P", Some(&long), None, 0),
            format!("{}...", "b".repeat(77))
        );
    }
}
