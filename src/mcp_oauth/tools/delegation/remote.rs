//! Delegated tasks in a linked environment (`RemoteDelegation` in the reference): the task's
//! child runs as an ordinary thread there, launched through the link with
//! `t3_thread_launch`, while this thread keeps the task (`delegated_task.remote.request`). A
//! follower waits on that thread through the link (`t3_thread_wait`), reads its last reply
//! (`t3_thread_read`) and records it as the task's result (`delegated_task.remote.complete`),
//! which wakes the parent as a local task's result does. A cancel stops the thread there
//! (`t3_thread_interrupt` with `stop`).
use super::*;
use std::time::Duration;

/// How long one follow of a remote task waits on its thread there before checking again.
const FOLLOW_WAIT_MS: u64 = 10 * 60 * 1_000;
/// A linked environment that stops answering is retried, backing off to this.
const FOLLOW_MAX_BACKOFF: Duration = Duration::from_secs(5 * 60);
/// The reference's `MAX_WAIT_TIMEOUT_MS` for a forwarded `t3_thread_wait`.
const FORWARDED_WAIT_MAX_MS: u64 = 45_000;
/// A task's thread there that ended without a reply.
const NO_REPLY: &str = "(The thread in the linked environment ended without a reply.)";

/// `isFinal`: refusals following again cannot change (the link revoked, expired or forgotten,
/// or the thread or run there gone).
fn is_final(code: &str) -> bool {
    matches!(
        code,
        "capability_denied" | "thread_not_found" | "run_not_found"
    )
}

fn is_terminal(status: &str) -> bool {
    matches!(
        status,
        "completed" | "failed" | "cancelled" | "interrupted" | "rolled_back"
    )
}

/// `terminalStatus`.
fn terminal_status(status: &str) -> &'static str {
    match status {
        "completed" => "completed",
        "failed" => "failed",
        "interrupted" => "interrupted",
        _ => "cancelled",
    }
}

fn refusal_code(refusal: &Refusal) -> Option<(&'static str, String)> {
    match refusal {
        Refusal::Failure(code, message) => Some((code, message.clone())),
        Refusal::Text(message) => Some(("orchestration_error", message.clone())),
        Refusal::Rpc(_, message) => Some(("orchestration_error", message.clone())),
    }
}

/// `encodeURIComponent`.
fn encode_component(text: &str) -> String {
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
    percent_encoding::utf8_percent_encode(text, COMPONENT).to_string()
}

/// `formatThreadLink` for a thread in another environment.
fn remote_link(remote: &Value) -> String {
    let label = remote["title"]
        .as_str()
        .unwrap_or_default()
        .replace(['[', ']', '\\', '\r', '\n'], " ")
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ");
    let href = format!(
        "t3-thread://v2/{}/{}",
        remote["environmentId"].as_str().unwrap_or_default(),
        remote["threadId"].as_str().unwrap_or_default()
    );
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
}

/// How this environment reaches a task's thread there (`followScope`): as the parent thread,
/// under one namespace per parent, limited to the parent's modes.
struct Follow {
    parent_id: String,
    title: String,
    namespace: String,
    limit: String,
}

impl Follow {
    fn of(parent: &Value) -> Self {
        let id = parent["id"].as_str().unwrap_or_default().to_owned();
        Follow {
            namespace: format!("remote-task:{id}"),
            title: parent["title"].as_str().unwrap_or_default().to_owned(),
            limit: format!(
                "{}/{}",
                parent["runtimeMode"].as_str().unwrap_or("full-access"),
                parent["interactionMode"].as_str().unwrap_or("default")
            ),
            parent_id: id,
        }
    }

    async fn call<S: McpHost>(
        &self,
        host: &Arc<S>,
        environment: &str,
        tool: &str,
        args: Value,
    ) -> Outcome {
        forward_as(
            host,
            environment,
            tool,
            args,
            &self.namespace,
            &self.limit,
            Some((&self.parent_id, &self.title)),
        )
        .await
    }
}

/// `resolveRemoteProject`: the project there with this thread's project's repository.
async fn resolve_project<S: McpHost>(
    host: &Arc<S>,
    caller: &Caller,
    environment: &str,
    parent_project: &str,
) -> Result<String, Refusal> {
    let key = {
        let store = host.control_state().store.read().await;
        store
            .project_rows
            .get(parent_project)
            .map(|row| host.project_row(row))
            .and_then(|row| {
                row["repositoryIdentity"]["canonicalKey"]
                    .as_str()
                    .map(str::to_owned)
            })
    };
    let Some(key) = key else {
        return Err(failure(
            "target_required",
            "Pass target.projectId: this thread's project has no repository to match there.",
        ));
    };
    let mut matches: Vec<(String, String)> = Vec::new();
    let mut cursor = json!(0);
    while !cursor.is_null() {
        let listed = forward(
            host,
            caller,
            "t3_project_list",
            &json!({"cursor": cursor, "limit": 100}),
            environment,
        )
        .await?;
        for project in listed["projects"].as_array().into_iter().flatten() {
            if project["repositoryIdentity"]["canonicalKey"] == key.as_str() {
                matches.push((
                    project["id"].as_str().unwrap_or_default().to_owned(),
                    project["title"].as_str().unwrap_or_default().to_owned(),
                ));
            }
        }
        cursor = listed["nextCursor"].clone();
    }
    match matches.as_slice() {
        [(id, _)] => Ok(id.clone()),
        [] => Err(failure(
            "invalid_request",
            format!("No project there has this repository ({key}). Pass target.projectId; t3_project_list with this environmentId lists them. To bring the repository there, use t3_project_clone and then t3_project_create with this environmentId."),
        )),
        several => Err(failure(
            "invalid_request",
            format!(
                "Several projects there have this repository: {}. Pass target.projectId.",
                several
                    .iter()
                    .map(|(id, title)| format!("{title} ({id})"))
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
        )),
    }
}

/// `resolveRemoteInstance`: the instance there of the driver the caller named, preferring the
/// one this thread runs on.
async fn resolve_instance<S: McpHost>(
    host: &Arc<S>,
    caller: &Caller,
    environment: &str,
    driver: &str,
    named: Option<&str>,
    inherited: &str,
) -> Result<String, Refusal> {
    let capabilities = forward(
        host,
        caller,
        "orchestrator_capabilities",
        &json!({}),
        environment,
    )
    .await?;
    let providers = records(&capabilities, "providers");
    if let Some(named) = named {
        if let Some(provider) = providers
            .iter()
            .find(|p| p["providerInstanceId"] == named && p["driverKind"] != driver)
        {
            return Err(failure(
                "invalid_request",
                format!(
                    "Provider instance {named} there uses driver {}, not {driver}.",
                    provider["driverKind"].as_str().unwrap_or_default()
                ),
            ));
        }
        return Ok(named.to_owned());
    }
    let candidates: Vec<&Value> = providers
        .iter()
        .filter(|p| p["driverKind"] == driver && p["canRunChildTask"] == true)
        .collect();
    candidates
        .iter()
        .find(|p| p["providerInstanceId"] == inherited)
        .or(candidates.first())
        .and_then(|p| p["providerInstanceId"].as_str().map(str::to_owned))
        .ok_or_else(|| {
            failure(
                "provider_unavailable",
                format!("No instance of driver {driver} there can run a child task. orchestrator_capabilities with this environmentId lists its providers."),
            )
        })
}

/// `RemoteDelegation.delegate`: launches the task's thread in `environment` and records the
/// task here. Returns the task's id and, for a wait, its end.
pub(super) async fn delegate<S: McpHost>(
    host: &Arc<S>,
    caller: &Caller,
    a: &Value,
    environment: &str,
) -> Result<
    (
        String,
        Option<tokio::sync::oneshot::Receiver<crate::orchestration::RunEnd>>,
    ),
    Refusal,
> {
    let scope = thread_scope(caller, "delegate_task")?;
    let (parent, parent_run) = {
        let store = host.control_state().store.read().await;
        let thread = store
            .threads
            .get(&scope.thread_id)
            .ok_or_else(|| failure("thread_not_found", "The calling thread was not found."))?;
        let projection = thread.full_projection();
        let run = records(&projection, "runs")
            .into_iter()
            .filter(|run| {
                matches!(
                    run["status"].as_str(),
                    Some("preparing" | "starting" | "running" | "waiting")
                )
            })
            .max_by_key(|run| run["ordinal"].as_u64().unwrap_or(0));
        (projection["thread"].clone(), run)
    };
    let Some(parent_run) = parent_run.filter(|run| {
        !run["rootNodeId"].is_null()
            && run["providerInstanceId"] == scope.provider_instance_id.as_str()
    }) else {
        return Err(failure(
            "parent_not_active",
            "Delegated tasks require an active run owned by this MCP provider session.",
        ));
    };
    // A worktree there would branch from a commit this checkout has and the other may not.
    if a["isolation"] == "worktree" {
        return Err(failure(
            "invalid_request",
            "A task in a linked environment runs in that project's checkout; isolation \"worktree\" is not supported there. Omit isolation, or use t3_thread_launch with environmentId and a worktree workspaceStrategy.",
        ));
    }
    let target = &a["target"];
    let inherited = &parent["modelSelection"];
    let inherited_instance = inherited["instanceId"].as_str().unwrap_or_default();
    let instance = match target["driverKind"].as_str() {
        None => target["providerInstanceId"]
            .as_str()
            .unwrap_or(inherited_instance)
            .to_owned(),
        Some(driver) => {
            resolve_instance(
                host,
                caller,
                environment,
                driver,
                target["providerInstanceId"].as_str(),
                inherited_instance,
            )
            .await?
        }
    };
    let model = target["model"].as_str().map(str::to_owned).or_else(|| {
        (instance == inherited_instance)
            .then(|| inherited["model"].as_str().map(str::to_owned))
            .flatten()
    });
    let Some(model) = model else {
        return Err(failure(
            "target_required",
            "Pass target.model from orchestrator_capabilities with this environmentId.",
        ));
    };
    let options = target
        .get("options")
        .filter(|o| !o.is_null())
        .cloned()
        .or_else(|| {
            (instance == inherited_instance && inherited["model"] == model.as_str())
                .then(|| inherited.get("options").cloned())
                .flatten()
        });
    // Modes asked for are checked against this thread; omitted ones are left to the other side.
    let runtime = match arg_str(a, "runtimeMode")? {
        None | Some("inherit") => None,
        Some(mode) => Some(resolve_modes(&parent, Some(mode), None)?.0.to_owned()),
    };
    let interaction = match arg_str(a, "interactionMode")? {
        None | Some("inherit") => None,
        Some(mode) => Some(resolve_modes(&parent, None, Some(mode))?.1.to_owned()),
    };
    let project_id = match target["projectId"].as_str() {
        Some(project) => project.to_owned(),
        None => {
            resolve_project(
                host,
                caller,
                environment,
                parent["projectId"].as_str().unwrap_or_default(),
            )
            .await?
        }
    };
    let key = arg_str(a, "clientRequestId")?
        .map(str::to_owned)
        .unwrap_or_else(|| crate::platform::uuid_v4().to_string());
    let task = task_prompt(a);
    let title = arg_str(a, "title")?
        .map(str::to_owned)
        .unwrap_or_else(|| parent["title"].as_str().unwrap_or_default().to_owned());
    let mut selection = json!({"instanceId": instance, "model": model});
    if let Some(options) = options {
        selection["options"] = options;
    }
    let mut launch = json!({
        "projectId": project_id,
        "title": title,
        "modelSelection": selection,
        "message": task,
        "clientRequestId": format!("delegate:{}:{key}", scope.thread_id),
        "delegatedFrom": {
            "environmentId": host.environment_id(),
            "threadId": parent["id"],
            "title": parent["title"],
        },
    });
    if let Some(runtime) = runtime {
        launch["runtimeMode"] = json!(runtime);
    }
    if let Some(interaction) = interaction {
        launch["interactionMode"] = json!(interaction);
    }
    let launched = forward(host, caller, "t3_thread_launch", &launch, environment).await?;
    let label = host
        .peer_link(environment)
        .map(|link| link.label)
        .unwrap_or_else(|| environment.to_owned());
    let mut remote_child = json!({
        "environmentId": environment,
        "threadId": launched["threadId"],
        "label": label,
        "title": title,
    });
    if !launched["runId"].is_null() {
        remote_child["runId"] = launched["runId"].clone();
    }
    let command_id = format!(
        "command:mcp:{}:remote-delegate:{}",
        encode_component(&caller.request_namespace()),
        encode_component(&key)
    );
    let task_id = crate::orchestration::task_node_id(&command_id);
    let run_end = (a["mode"] == "wait").then(|| crate::orchestration::watch_run_end(&task_id));
    let mut record = json!({
        "type": "delegated_task.remote.request",
        "commandId": command_id,
        "parentThreadId": scope.thread_id,
        "parentRunId": parent_run["id"],
        "parentNodeId": parent_run["rootNodeId"],
        "task": task,
        "driver": "remote",
        "modelSelection": launched["modelSelection"],
        "remoteChild": remote_child,
        "completionWake": if a["mode"] == "wait" { "settled_only" } else { "always" },
    });
    if let Some(title) = arg_str(a, "title")? {
        record["title"] = json!(title);
    }
    if let Err(error) = host.dispatch(record).await {
        tracing::warn!(?error, parent = %scope.thread_id, "remote-delegation.record-failed");
        // Nothing here follows a thread the task was never recorded for, so stop it there.
        let follow = Follow::of(&parent);
        let _ = stop_there(host, &follow, &remote_child, "stop", None).await;
        return Err(failure(
            "orchestration_error",
            "Unable to record the delegated task.",
        ));
    }
    let recorded = {
        let store = host.control_state().store.read().await;
        store.threads.get(&scope.thread_id).is_some_and(|thread| {
            records(&thread.stored_row(), "subagents")
                .iter()
                .any(|t| t["id"] == task_id.as_str())
        })
    };
    if !recorded {
        return Err(failure(
            "orchestration_error",
            "The delegated task was not recorded.",
        ));
    }
    spawn_follower(host, &scope.thread_id, &task_id);
    Ok((task_id, run_end))
}

/// Stops the task's own thread there as its Stop button does (`stopThere`).
async fn stop_there<S: McpHost>(
    host: &Arc<S>,
    follow: &Follow,
    remote: &Value,
    key: &str,
    reason: Option<&str>,
) -> Outcome {
    let thread = remote["threadId"].as_str().unwrap_or_default();
    let mut args = json!({
        "threadId": thread,
        "stop": true,
        "clientRequestId": format!("{key}:{thread}"),
    });
    if let Some(reason) = reason {
        args["reason"] = json!(reason);
    }
    follow
        .call(
            host,
            remote["environmentId"].as_str().unwrap_or_default(),
            "t3_thread_interrupt",
            args,
        )
        .await
}

#[allow(clippy::too_many_arguments)]
/// `complete`: records the task's result (or that nothing is left to stop there), then lets
/// the parent read it. A report refused because the task already has its result is done.
async fn complete<S: McpHost>(
    host: &Arc<S>,
    parent_id: &str,
    task_id: &str,
    status: &str,
    result: &str,
    stopped_there: Option<&str>,
    stop_count: Option<u64>,
    executed_model: Option<&str>,
) -> Result<(), Value> {
    let mut command = json!({
        "type": "delegated_task.remote.complete",
        "commandId": format!("command:remote-task-complete:{task_id}:{}", crate::platform::uuid_v4()),
        "parentThreadId": parent_id,
        "taskId": task_id,
        "status": status,
        "result": result,
    });
    if let Some(how) = stopped_there {
        command["stoppedThere"] = json!(how);
    }
    if let Some(count) = stop_count {
        command["stopCount"] = json!(count);
    }
    if let Some(model) = executed_model {
        command["executedModel"] = json!(model);
    }
    match host.dispatch(command).await {
        Ok(_) => {
            host.remote_task_settled(parent_id.to_owned(), task_id.to_owned())
                .await;
            Ok(())
        }
        Err(error) => {
            if read_task_row(host, parent_id, task_id)
                .await
                .is_some_and(|task| !task["result"].is_null())
            {
                Ok(())
            } else {
                Err(error)
            }
        }
    }
}

async fn read_task_row<S: McpHost>(host: &Arc<S>, parent_id: &str, task_id: &str) -> Option<Value> {
    let store = host.control_state().store.read().await;
    let thread = store.threads.get(parent_id)?;
    records(&thread.stored_row(), "subagents")
        .into_iter()
        .find(|task| task["id"] == task_id)
}

async fn parent_row<S: McpHost>(host: &Arc<S>, parent_id: &str) -> Option<Value> {
    let store = host.control_state().store.read().await;
    store
        .threads
        .get(parent_id)
        .map(|thread| thread.full_projection()["thread"].clone())
}

/// `lastReply`: the last reply `run_id` gave on the task's thread there, read whole; a failed
/// run prefers its error. Led by a note when its provider ran another model than it asked for.
async fn last_reply<S: McpHost>(
    host: &Arc<S>,
    follow: &Follow,
    remote: &Value,
    run_id: Option<&str>,
    failed: bool,
) -> Result<(Option<String>, Option<String>), Refusal> {
    let environment = remote["environmentId"].as_str().unwrap_or_default();
    let thread = remote["threadId"].as_str().unwrap_or_default();
    let mut reply: Option<Value> = None;
    let mut error: Option<Value> = None;
    let mut after: i64 = -1;
    let mut run: Option<Value> = None;
    let matches_run = |item: &Value| run_id.is_none_or(|id| item["runId"] == id);
    loop {
        let read = follow
            .call(
                host,
                environment,
                "t3_thread_read",
                json!({
                    "threadId": thread,
                    "view": if failed { "activity" } else { "messages" },
                    "limit": 100,
                    "runLimit": 50,
                    "maxCharsPerItem": 20_000,
                    "afterPosition": after,
                }),
            )
            .await?;
        if run.is_none() {
            run = records(&read, "recentRuns")
                .into_iter()
                .find(|r| run_id.is_some_and(|id| r["runId"] == id));
        }
        let items = records(&read, "items");
        if let Some(item) = items
            .iter()
            .rfind(|i| i["type"] == "assistant_message" && matches_run(i))
        {
            reply = Some(item.clone());
        }
        if failed {
            if let Some(item) = items
                .iter()
                .rfind(|i| i["type"] == "error" && matches_run(i))
            {
                error = Some(item.clone());
            }
        }
        match read["nextPosition"].as_i64() {
            Some(next) if read["hasMore"] == true => after = next,
            _ => break,
        }
    }
    let executed = run
        .as_ref()
        .and_then(|r| r["executedModel"].as_str().map(str::to_owned));
    let Some(reply) = error.or(reply) else {
        return Ok((None, executed));
    };
    let mut text = reply["text"].as_str().unwrap_or_default().to_owned();
    let mut offset = (reply["textTruncated"] == true)
        .then(|| reply["nextTextOffset"].as_u64())
        .flatten();
    while let Some(at) = offset {
        let rest = follow
            .call(
                host,
                environment,
                "t3_thread_read",
                json!({"threadId": thread, "itemId": reply["itemId"], "textOffset": at, "maxCharsPerItem": 20_000}),
            )
            .await?;
        let Some(part) = records(&rest, "items").into_iter().next() else {
            break;
        };
        text.push_str(part["text"].as_str().unwrap_or_default());
        offset = (part["textTruncated"] == true)
            .then(|| part["nextTextOffset"].as_u64())
            .flatten();
    }
    if let Some(run) = &run {
        // `withExecutedModelNote`.
        if let Some(executed) = run["executedModel"].as_str() {
            let requested = run["model"].as_str().unwrap_or_default();
            if executed != requested {
                text = format!("Requested model {requested}; the provider reported running {executed}.\n\n{text}");
            }
        }
    }
    Ok((Some(text), executed))
}

/// Starts the task's follower, unless one already follows it.
pub(super) fn spawn_follower<S: McpHost>(host: &Arc<S>, parent_id: &str, task_id: &str) {
    if !FOLLOWING
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .insert(task_id.to_owned())
    {
        return;
    }
    let host2 = host.clone();
    let parent = parent_id.to_owned();
    let task = task_id.to_owned();
    host.spawn(Box::pin(async move {
        follow(&host2, &parent, &task).await;
        FOLLOWING
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&task);
    }));
}

static FOLLOWING: std::sync::LazyLock<std::sync::Mutex<std::collections::HashSet<String>>> =
    std::sync::LazyLock::new(Default::default);

/// Tasks a cancel is stopping there: their follower leaves the result to the cancel, which
/// records it once the stop is answered (the reference's cancel records first, as its follower
/// still reads the thread's reply).
static CANCELLING: std::sync::LazyLock<std::sync::Mutex<std::collections::HashSet<String>>> =
    std::sync::LazyLock::new(Default::default);

fn cancelling(task_id: &str) -> bool {
    CANCELLING
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .contains(task_id)
}

/// Waits while a cancel of `task_id` is recording its result.
async fn after_cancel<S: McpHost>(host: &Arc<S>, task_id: &str) {
    while cancelling(task_id) {
        host.sleep(Duration::from_millis(20)).await;
    }
}

/// `follow`: waits for the task's run there to end and completes the task here, stopping it
/// there when it ended here first. A gone link, thread or run fails the task; anything else is
/// retried with back-off.
async fn follow<S: McpHost>(host: &Arc<S>, parent_id: &str, task_id: &str) {
    let mut backoff = Duration::from_secs(2);
    loop {
        match follow_once(host, parent_id, task_id).await {
            Ok(true) => return,
            Ok(false) => backoff = Duration::from_secs(2),
            Err((code, message)) => {
                let Some(task) = read_task_row(host, parent_id, task_id).await else {
                    return;
                };
                let environment = task["remoteChild"]["environmentId"]
                    .as_str()
                    .unwrap_or_default();
                let link_gone = code == "invalid_request" && host.peer_link(environment).is_none();
                if is_final(code) || link_gone {
                    let _ = complete(
                        host,
                        parent_id,
                        task_id,
                        "failed",
                        &message,
                        Some("stopped"),
                        task["remoteChild"]["stopCount"].as_u64(),
                        None,
                    )
                    .await;
                    return;
                }
                host.sleep(backoff).await;
                backoff = (backoff * 2).min(FOLLOW_MAX_BACKOFF);
            }
        }
    }
}

/// `followOnce`: one wait on the task's thread there; true once the task here is done.
async fn follow_once<S: McpHost>(
    host: &Arc<S>,
    parent_id: &str,
    task_id: &str,
) -> Result<bool, (&'static str, String)> {
    after_cancel(host, task_id).await;
    let Some(task) = read_task_row(host, parent_id, task_id).await else {
        return Ok(true);
    };
    let remote = task["remoteChild"].clone();
    if !remote.is_object() {
        return Ok(true);
    }
    let Some(parent) = parent_row(host, parent_id).await else {
        return Ok(true);
    };
    let follow = Follow::of(&parent);
    let fail =
        |refusal: Refusal| refusal_code(&refusal).unwrap_or(("orchestration_error", String::new()));
    // Ended here: it still has to be stopped there.
    if !task["result"].is_null() {
        let owes = remote["stoppedThere"] == false
            || (matches!(task["status"].as_str(), Some("cancelled" | "interrupted"))
                && remote["stoppedThere"] != true);
        if owes {
            let count = match remote["stopCount"].as_u64() {
                None => "stop".to_owned(),
                Some(n) => format!("stop-{n}"),
            };
            let key = match remote["runId"].as_str() {
                None => count,
                Some(run) => format!("{count}:{run}"),
            };
            match stop_there(host, &follow, &remote, &key, None).await {
                Ok(_) => {}
                Err(Refusal::Failure(
                    "runtime_mode_escalation_denied" | "interaction_mode_escalation_denied",
                    reason,
                )) => {
                    tracing::warn!(task_id, %reason, "a linked environment refused to stop a delegated task");
                }
                Err(refusal) => return Err(fail(refusal)),
            }
            let _ = complete(
                host,
                parent_id,
                task_id,
                terminal_status(task["status"].as_str().unwrap_or_default()),
                task["result"].as_str().unwrap_or_default(),
                Some("stopped"),
                remote["stopCount"].as_u64(),
                None,
            )
            .await;
        }
        return Ok(true);
    }
    let environment = remote["environmentId"].as_str().unwrap_or_default();
    let mut wait = json!({
        "threadId": remote["threadId"],
        "timeoutMs": FOLLOW_WAIT_MS.min(FORWARDED_WAIT_MAX_MS),
        "untilDelegatedWorkSettled": true,
    });
    if let Some(run) = remote.get("runId").filter(|r| r.is_string()) {
        wait["runId"] = run.clone();
    }
    let waited = follow
        .call(host, environment, "t3_thread_wait", wait)
        .await
        .map_err(fail)?;
    // A cancel or Stop recorded here meanwhile: the next look reads the result and stops it
    // there. One still being recorded wins the result after this reads the reply.
    if read_task_row(host, parent_id, task_id)
        .await
        .is_some_and(|t| !t["result"].is_null())
    {
        return Ok(false);
    }
    if waited["timedOut"] == true {
        return Ok(false);
    }
    let complete_with = |status: &str, text: Option<String>, model: Option<String>| {
        let status = status.to_owned();
        async move {
            after_cancel(host, task_id).await;
            let _ = complete(
                host,
                parent_id,
                task_id,
                &status,
                text.as_deref().unwrap_or(NO_REPLY),
                Some("ran-out"),
                None,
                model.as_deref(),
            )
            .await;
        }
    };
    let Some(run_id) = waited["runId"].as_str() else {
        // A thread there with no run has nothing left to wait for.
        let (text, _) = last_reply(host, &follow, &remote, None, false)
            .await
            .map_err(fail)?;
        let status = if text.is_some() {
            "completed"
        } else {
            "failed"
        };
        complete_with(
            status,
            Some(text.unwrap_or_else(|| {
                "The thread in the linked environment has no run to follow.".into()
            })),
            None,
        )
        .await;
        return Ok(settled(host, parent_id, task_id).await);
    };
    if !is_terminal(waited["status"].as_str().unwrap_or_default()) {
        return Ok(false);
    }
    let (result_run, result_status) = match waited["resultRun"]["runId"].as_str() {
        Some(other) => (
            other.to_owned(),
            waited["resultRun"]["status"]
                .as_str()
                .unwrap_or_default()
                .to_owned(),
        ),
        None => (
            run_id.to_owned(),
            waited["status"].as_str().unwrap_or_default().to_owned(),
        ),
    };
    let (text, model) = last_reply(
        host,
        &follow,
        &remote,
        Some(&result_run),
        result_status == "failed",
    )
    .await
    .map_err(fail)?;
    complete_with(terminal_status(&result_status), text, model).await;
    Ok(settled(host, parent_id, task_id).await)
}

/// Whether the task is done here: its result recorded and nothing left to stop there.
async fn settled<S: McpHost>(host: &Arc<S>, parent_id: &str, task_id: &str) -> bool {
    read_task_row(host, parent_id, task_id)
        .await
        .is_none_or(|task| task["remoteChild"]["stoppedThere"] == true)
}

/// Starts a follower for each open remote task of the server's threads, after a restart.
pub(crate) async fn resume_followers<S: McpHost>(host: &Arc<S>) {
    let open: Vec<(String, String)> = {
        let store = host.control_state().store.read().await;
        store
            .threads
            .values()
            .flat_map(|thread| {
                records(&thread.stored_row(), "subagents")
                    .into_iter()
                    .filter(|task| {
                        task["remoteChild"].is_object()
                            && (task["result"].is_null()
                                || task["remoteChild"]["stoppedThere"] == false)
                    })
                    .map(|task| {
                        (
                            thread.id.clone(),
                            task["id"].as_str().unwrap_or_default().to_owned(),
                        )
                    })
                    .collect::<Vec<_>>()
            })
            .collect()
    };
    for (parent, task) in open {
        spawn_follower(host, &parent, &task);
    }
}

/// `readRemoteTask`: a remote task, answered from its row here.
pub(super) async fn read<S: McpHost>(
    host: &Arc<S>,
    caller: &Caller,
    task: &Value,
    parent_runs: &[Value],
    wait_timed_out: bool,
    acknowledge_terminal: bool,
) -> Outcome {
    let remote = &task["remoteChild"];
    let status = if task["result"].is_null() {
        if run_status(host, task).await.as_deref() == Some("starting") {
            "starting"
        } else {
            "running"
        }
    } else {
        let recorded = task["status"].as_str().unwrap_or_default();
        if matches!(recorded, "failed" | "cancelled" | "interrupted") {
            recorded
        } else {
            "completed"
        }
    };
    let terminal = TERMINAL_TASK.contains(&status);
    if acknowledge_terminal
        && terminal
        && !matches!(
            task["completionDelivery"]["state"].as_str(),
            Some("acknowledged" | "disposed")
        )
    {
        acknowledge_result(
            host,
            caller,
            parent_runs,
            task["id"].as_str().unwrap_or_default(),
        )
        .await?;
    }
    let result = json!({
        "taskId": task["id"],
        "childThreadId": null,
        // The answer's `remoteChild` names where the task runs, as the reference's schema has it.
        "remoteChild": {
            "environmentId": remote["environmentId"],
            "threadId": remote["threadId"],
            "label": remote["label"],
        },
        "link": remote_link(remote),
        "childRunId": null,
        "childNodeId": task["id"],
        "status": status,
        "workState": if terminal { "result_available" } else { "working" },
        "hasPendingChildRuns": false,
        "providerInstanceId": task["providerInstanceId"],
        "model": task["model"],
        "executedModel": task.get("executedModel").cloned().unwrap_or(Value::Null),
        "summary": task["result"],
        "resultContextTransferId": null,
        "latestTerminalRunId": null,
        "latestTerminalStatus": if terminal { json!(status) } else { Value::Null },
        "latestTerminalSummary": if terminal { task["result"].clone() } else { Value::Null },
        "latestTerminalResultContextTransferId": null,
        "hasPendingQuestion": false,
        "pendingApproval": null,
        "waitTimedOut": wait_timed_out,
    });
    // The reference's remote answer carries no `next`.
    Ok(result)
}

/// `runStatus`: the status of the task's run there, if it can be read.
async fn run_status<S: McpHost>(host: &Arc<S>, task: &Value) -> Option<String> {
    let remote = &task["remoteChild"];
    let run_id = remote["runId"].as_str()?;
    let parent = parent_row(host, task["threadId"].as_str()?).await?;
    let read = Follow::of(&parent)
        .call(
            host,
            remote["environmentId"].as_str()?,
            "t3_thread_read",
            json!({"threadId": remote["threadId"], "limit": 1, "runLimit": 50}),
        )
        .await
        .ok()?;
    records(&read, "recentRuns")
        .into_iter()
        .find(|run| run["runId"] == run_id)
        .and_then(|run| run["status"].as_str().map(str::to_owned))
}

/// `RemoteDelegation.cancel`: stops the task's thread there and records the cancel here. A
/// link that is gone or a machine that is down still cancels the task here; its follower keeps
/// trying to stop it there.
pub(super) async fn cancel<S: McpHost>(
    host: &Arc<S>,
    caller: &Caller,
    task: &Value,
    reason: Option<&str>,
    client_request_id: Option<&str>,
) -> Outcome {
    let remote = &task["remoteChild"];
    let task_id = task["id"].as_str().unwrap_or_default();
    let parent_id = task["threadId"].as_str().unwrap_or_default();
    let Some(parent) = parent_row(host, parent_id).await else {
        return Err(failure(
            "thread_not_found",
            "The calling thread was not found.",
        ));
    };
    let follow = Follow::of(&parent);
    let key = format!(
        "cancel:{}:{}",
        encode_component(&caller.request_namespace()),
        client_request_id
            .map(str::to_owned)
            .unwrap_or_else(|| crate::platform::uuid_v4().to_string())
    );
    let stop_count = remote["stopCount"].as_u64();
    if !task["result"].is_null() {
        // Best effort: the result stands whether or not the stop gets there.
        if stop_there(host, &follow, remote, &key, reason)
            .await
            .is_ok()
        {
            let _ = complete(
                host,
                parent_id,
                task_id,
                terminal_status(task["status"].as_str().unwrap_or_default()),
                task["result"].as_str().unwrap_or_default(),
                Some("stopped"),
                stop_count,
                None,
            )
            .await;
        }
        let recorded = task["status"].as_str().unwrap_or_default();
        let status = if matches!(recorded, "completed" | "failed" | "interrupted") {
            recorded
        } else {
            "cancelled"
        };
        return Ok(json!({"taskId": task_id, "status": status}));
    }
    CANCELLING
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .insert(task_id.to_owned());
    let unstopped = stop_there(host, &follow, remote, &key, reason)
        .await
        .err()
        .and_then(|refusal| refusal_code(&refusal).map(|(_, message)| message));
    let cancelled = match reason {
        None => "Cancelled.".to_owned(),
        Some(reason) => format!("Cancelled: {reason}"),
    };
    let result = match &unstopped {
        None => cancelled,
        Some(why) => format!(
            "{cancelled} It could not be stopped in {} and may still run there: {why}",
            remote["label"].as_str().unwrap_or_default()
        ),
    };
    let recorded = complete(
        host,
        parent_id,
        task_id,
        "cancelled",
        &result,
        unstopped.is_none().then_some("stopped"),
        stop_count,
        None,
    )
    .await;
    CANCELLING
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .remove(task_id);
    recorded.map_err(|_| failure("orchestration_error", "Unable to record the cancellation."))?;
    if unstopped.is_some() {
        spawn_follower(host, parent_id, task_id);
    }
    Ok(json!({"taskId": task_id, "status": "cancel_requested"}))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_a_gone_link_thread_or_run_ends_following() {
        assert!(is_final("capability_denied"));
        assert!(is_final("thread_not_found"));
        assert!(is_final("run_not_found"));
        assert!(!is_final("orchestration_error"));
        assert!(!is_final("invalid_request"));
    }

    #[test]
    fn a_run_there_ends_the_task_as_the_reference_maps_it() {
        assert_eq!(terminal_status("completed"), "completed");
        assert_eq!(terminal_status("interrupted"), "interrupted");
        assert_eq!(terminal_status("rolled_back"), "cancelled");
        assert!(is_terminal("rolled_back"));
        assert!(!is_terminal("waiting"));
    }

    #[test]
    fn a_remote_task_links_to_its_thread_there() {
        let remote =
            json!({"environmentId": "env-peer", "threadId": "thread:there", "title": "Say [hi]"});
        assert_eq!(
            remote_link(&remote),
            "[Say hi](t3-thread://v2/env-peer/thread:there)"
        );
        let untitled = json!({"environmentId": "env-peer", "threadId": "t(1)", "title": ""});
        assert_eq!(remote_link(&untitled), "<t3-thread://v2/env-peer/t(1)>");
    }
}
