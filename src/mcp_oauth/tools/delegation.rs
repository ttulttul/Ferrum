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

fn thread_scope<'a>(
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
fn stable_command_id(caller: &Caller, operation: &str, key: &str) -> String {
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

/// `providerConstraints`: why an instance cannot run a child task.
fn provider_constraints(provider: &Value) -> Vec<String> {
    let mut out = Vec::new();
    let driver = provider["driver"].as_str().unwrap_or_default();
    if !matches!(driver, "codex" | "claudeAgent") {
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

/// `resolveRuntimeMode` and `resolveInteractionMode` for a child.
fn child_modes(parent: &Value, a: &Value) -> Result<(String, String), Refusal> {
    let parent_runtime = parent["runtimeMode"].as_str().unwrap_or("full-access");
    let runtime = match arg_str(a, "runtimeMode")? {
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
    let interaction = match arg_str(a, "interactionMode")? {
        None | Some("inherit") => parent_interaction,
        Some(mode) => mode,
    };
    if interaction == "default" && parent_interaction == "plan" {
        return Err(failure(
            "interaction_mode_escalation_denied",
            format!("Child interaction mode {interaction} is broader than parent mode {parent_interaction}."),
        ));
    }
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
    if a["isolation"] == "worktree" {
        return Err(failure(
            "invalid_request",
            "Isolation \"worktree\" is not available on this server yet. Use isolation \"shared\".",
        ));
    }
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
    if wait {
        let timeout = a["timeoutMs"]
            .as_f64()
            .map_or(WAIT_DEFAULT_MS, |ms| ms.max(1.0) as u64)
            .min(WAIT_MAX_MS);
        return await_task(host, caller, &task_id, &key, run_end, timeout).await;
    }
    read_task(host, caller, &task_id, false, true).await
}

pub(super) async fn task_status<S: McpHost>(host: &Arc<S>, caller: &Caller, a: &Value) -> Outcome {
    thread_scope(caller, "This tool")?;
    threads::writing_caller(&**host, caller).await?;
    let task_id = required(a, "taskId")?.to_owned();
    thread_scope(caller, "task_status")?;
    read_task(host, caller, &task_id, false, true).await
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
        let read = read_task(host, caller, task_id, false, true).await;
        if let Some(crate::orchestration::RunEnd {
            read: done,
            settled,
        }) = ended.take()
        {
            drop(done);
            let _ = settled.await;
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

/// `readTask`: the task as the parent's agent sees it.
async fn read_task<S: McpHost>(
    host: &Arc<S>,
    caller: &Caller,
    task_id: &str,
    wait_timed_out: bool,
    acknowledge: bool,
) -> Outcome {
    let scope = thread_scope(caller, "task_status")?;
    let (task, child, parent_runs) = {
        let store = host.control_state().store.read().await;
        let parent = find_thread(&store, &scope.thread_id)?;
        let parent_projection = parent.full_projection();
        let task = records(&parent_projection, "subagents")
            .into_iter()
            .find(|t| {
                t["id"] == task_id
                    && t["origin"] == "app_owned"
                    && t["threadId"] == scope.thread_id.as_str()
            });
        let child = task
            .as_ref()
            .and_then(|t| t["childThreadId"].as_str())
            .and_then(|id| store.threads.get(id))
            .map(|t| t.full_projection());
        (task, child, records(&parent_projection, "runs"))
    };
    let (Some(task), Some(child)) = (task, child) else {
        return Err(failure(
            "task_not_found",
            format!(
                "Delegated task {task_id} does not belong to thread {}.",
                scope.thread_id
            ),
        ));
    };
    let runs = records(&child, "runs");
    let transfers = records(&child, "contextTransfers");
    // `delegatedTaskRun`: the run its spawn started.
    let child_run = transfers
        .iter()
        .find(|t| {
            t["type"] == "subagent_spawn"
                && t["sourceThreadId"] == task["threadId"]
                && t["targetThreadId"] == task["childThreadId"]
        })
        .map_or_else(
            || runs.first().cloned(),
            |t| runs.iter().find(|r| r["id"] == t["targetRunId"]).cloned(),
        );
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
    let result_transfer = transfers
        .iter()
        .find(|t| {
            t["type"] == "subagent_result"
                && t["sourceThreadId"] == task["childThreadId"]
                && t["targetThreadId"] == scope.thread_id.as_str()
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
