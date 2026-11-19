use super::{execute_run, now, AppState, CommandReceipt, ConversationMessage, Event, Run, Thread};
use serde_json::{json, Value};
use std::sync::Arc;
use tokio::sync::oneshot;
use uuid::Uuid;

pub(crate) async fn dispatch_orchestration_command(
    state: Arc<AppState>,
    command: Value,
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
        .unwrap_or_default();
    let error = |detail: String, cause: Value| json!({"_tag":"OrchestrationV2DispatchCommandError","commandId":command_id,"commandType":kind,"message":detail,"detail":detail,"cause":cause});
    let dispatch_error = |detail: String| {
        error(
            detail.clone(),
            json!({"name":"OrchestratorDispatchError","message":format!("Failed to dispatch orchestration command {kind} ({command_id})."),"cause":detail}),
        )
    };
    let _guard = state.project_mutation_lock.lock().await;
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
                    | "thread.pull-request.sync"
            )
        {
            return Err(dispatch_error(format!("Thread {id} is archived.")));
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
    if let Some(summary) = state.settings.policy.summary() {
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
            if let Some(mut message) = mode
                .as_str()
                .and_then(|mode| state.settings.policy.runtime_denial(mode))
            {
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
                .settings
                .internal()
                .map_err(|e| dispatch_error(e.to_string()))?;
            let driver = settings["providerInstances"][id]["driver"]
                .as_str()
                .unwrap_or(id);
            if let Some(reason) = state.settings.policy.provider_denial(driver) {
                return Err(dispatch_error(reason));
            }
        }
    }
    drop(store);
    let receipt = CommandReceipt {
        command_id: command_id.to_string(),
        thread_id: id.to_string(),
    };
    apply_command(state.clone(), &command, &receipt)
        .await
        .map_err(|e| dispatch_error(e.to_string()))
}

async fn start_run(
    state: Arc<AppState>,
    receipt: &CommandReceipt,
    thread_id: &str,
    text: String,
    message_id: &Value,
    queued: bool,
) -> anyhow::Result<u64> {
    let (cwd, needs_baseline) = state
        .store
        .read()
        .await
        .threads
        .get(thread_id)
        .map(|t| (t.cwd.clone(), t.checkpoints.is_empty()))
        .ok_or_else(|| anyhow::anyhow!("thread not found"))?;
    let baseline = needs_baseline.then(|| cwd.clone());
    let run = Run {
        id: Uuid::new_v4(),
        thread_id: thread_id.to_owned(),
        provider: "default".into(),
        status: if queued { "queued" } else { "running" }.into(),
        started_at: now(),
        finished_at: None,
        error: None,
        user_message_id: Some(message_id.as_str().unwrap_or_default().to_owned()),
        queue_held: queued,
    };
    let message = ConversationMessage {
        id: message_id.as_str().unwrap_or_default().to_owned(),
        role: "user".into(),
        content: text.clone(),
        created_at: now(),
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
    if let Some(baseline_cwd) = baseline {
        if let Some(tree) = crate::capture_git_tree(&baseline_cwd).await {
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
    if queued {
        return Ok(seq);
    }
    let (tx, rx) = oneshot::channel();
    state.cancellations.lock().await.insert(run.id, tx);
    tokio::spawn(async move {
        if let Err(e) = execute_run(state.clone(), run.clone(), cwd, text, rx).await {
            state.cancellations.lock().await.remove(&run.id);
            if let Err(write) = state
                .commit(Event::RunFinished {
                    run_id: run.id,
                    thread_id: run.thread_id,
                    status: "failed".into(),
                    error: Some(e.to_string()),
                })
                .await
            {
                tracing::error!(%write,"could not record provider failure");
            }
        }
    });
    Ok(seq)
}

async fn start_queued_run(
    state: Arc<AppState>,
    receipt: &CommandReceipt,
    thread_id: &str,
    run_id: &str,
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
            .find(|candidate| candidate.id.to_string() == run_id && candidate.status == "queued")
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
    run.status = "running".into();
    run.queue_held = false;
    let sequence = state
        .commit_receipted(Event::RunStarted(run.clone()), Some(receipt.clone()))
        .await?
        .id;
    let (tx, rx) = oneshot::channel();
    state.cancellations.lock().await.insert(run.id, tx);
    tokio::spawn(async move {
        if let Err(error) = execute_run(state.clone(), run.clone(), cwd, text, rx).await {
            state.cancellations.lock().await.remove(&run.id);
            let _ = state
                .commit(Event::RunFinished {
                    run_id: run.id,
                    thread_id: run.thread_id.clone(),
                    status: "failed".into(),
                    error: Some(error.to_string()),
                })
                .await;
        }
    });
    Ok(sequence)
}

async fn apply_command(
    state: Arc<AppState>,
    command: &Value,
    receipt: &CommandReceipt,
) -> anyhow::Result<u64> {
    let command_type = command
        .get("type")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default();
    let thread_id = command.get("threadId").and_then(serde_json::Value::as_str);
    match command_type {
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
            };
            return Ok(state
                .commit_receipted(Event::ThreadCreated(thread), Some(receipt.clone()))
                .await?
                .id);
        }
        "message.dispatch" => {
            let thread_id =
                thread_id.ok_or_else(|| anyhow::anyhow!("message.dispatch requires threadId"))?;
            let text = command
                .get("text")
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default()
                .to_owned();
            let queued = command["dispatchMode"]["type"] == "queue_after_active"
                && state
                    .store
                    .read()
                    .await
                    .threads
                    .get(thread_id)
                    .is_some_and(|thread| thread.runs.iter().any(|run| run.status == "running"));
            return start_run(
                state,
                receipt,
                thread_id,
                text,
                &command["messageId"],
                queued,
            )
            .await;
        }
        "thread.stop" | "run.interrupt" | "subagent.stop" => {
            if let Some(thread_id) = thread_id {
                let run_ids = state
                    .store
                    .read()
                    .await
                    .threads
                    .get(thread_id)
                    .map(|thread| {
                        thread
                            .runs
                            .iter()
                            .filter(|run| run.status == "running")
                            .map(|run| run.id)
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default();
                let mut cancellations = state.cancellations.lock().await;
                for run_id in run_ids {
                    if let Some(cancel) = cancellations.remove(&run_id) {
                        let _ = cancel.send(());
                    }
                }
                return Ok(state
                    .commit_receipted(
                        Event::ThreadPatched {
                            thread_id: thread_id.to_owned(),
                            patch: serde_json::json!({"status":"cancelled"}),
                        },
                        Some(receipt.clone()),
                    )
                    .await?
                    .id);
            }
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
                state.guest.close_thread(id)
            } else {
                state.guest.resolve_message(
                    id,
                    command["guestMessageId"].as_str().unwrap_or(""),
                    "dismissed",
                )
            }
            .map_err(anyhow::Error::msg)?;
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
                    }
                    "thread.unsnooze" => {
                        patch.insert("snoozed_until".into(), serde_json::Value::Null);
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
                                row["lastTitleRegeneration"] = json!({
                                    "requestId": request_id,
                                    "completedAt": at,
                                    "outcome": outcome
                                });
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
                return Ok(state
                    .commit_receipted(
                        Event::ThreadPatched {
                            thread_id: thread_id.to_owned(),
                            patch: patch.into(),
                        },
                        Some(receipt.clone()),
                    )
                    .await?
                    .id);
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
        "prepared-run.release"
        | "prepared-run.progress"
        | "prepared-run.fail"
        | "prepared-run.retry" => {
            let run_id = command["runId"].as_str().unwrap_or_default();
            return Err(anyhow::anyhow!(format!(
                "Run {run_id} is not awaiting workspace preparation."
            )));
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
                .and_then(|thread| thread.runs.iter().find(|run| run.id.to_string() == run_id))
                .is_some_and(|run| run.status == "queued");
            if !valid {
                return Err(anyhow::anyhow!(format!("Run {run_id} is not queued.")));
            }
            return Ok(state
                .commit_receipted(
                    Event::RunFinished {
                        run_id: Uuid::parse_str(run_id)?,
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
            let Some((message_id, thread_id)) = state
                .store
                .read()
                .await
                .threads
                .get(thread_id.unwrap_or_default())
                .and_then(|thread| thread.runs.iter().find(|run| run.id.to_string() == run_id))
                .filter(|run| run.status == "queued")
                .and_then(|run| run.user_message_id.clone())
                .map(|message_id| (message_id, thread_id.unwrap_or_default().to_owned()))
            else {
                return Err(anyhow::anyhow!(format!("Run {run_id} is not queued.")));
            };
            return Ok(state
                .commit_receipted(
                    Event::MessageEdited {
                        thread_id,
                        message_id,
                        content: command["text"].as_str().unwrap_or_default().to_owned(),
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
                .and_then(|thread| thread.runs.iter().find(|run| run.id.to_string() == run_id))
                .is_some_and(|run| run.status == "queued");
            if !valid {
                return Err(anyhow::anyhow!(format!("Run {run_id} is not queued.")));
            }
            let before_run_id = match command.get("beforeRunId") {
                Some(Value::Null) | None => None,
                Some(value) => Some(Uuid::parse_str(value.as_str().unwrap_or_default())?),
            };
            return Ok(state
                .commit_receipted(
                    Event::RunReordered {
                        thread_id: thread_id.to_owned(),
                        run_id: Uuid::parse_str(run_id)?,
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
        "runtime-request.respond" => {
            let request_id = command["requestId"].as_str().unwrap_or_default();
            return Err(anyhow::anyhow!(format!(
                "Runtime request {request_id} was not found."
            )));
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
            let parent_thread_id = command["parentThreadId"].as_str().unwrap_or_default();
            let generated_child_id;
            let target_thread_id = if let Some(value) = command["childThreadId"]
                .as_str()
                .or_else(|| command["targetThreadId"].as_str())
            {
                value
            } else if !receipt.thread_id.is_empty() {
                receipt.thread_id.as_str()
            } else {
                generated_child_id = format!(
                    "delegated-task:{}",
                    command["commandId"].as_str().unwrap_or_default()
                );
                generated_child_id.as_str()
            };
            let parent = state
                .store
                .read()
                .await
                .threads
                .get(parent_thread_id)
                .cloned()
                .ok_or_else(|| {
                    anyhow::anyhow!(format!(
                        "No orchestration projection exists for thread {parent_thread_id}."
                    ))
                })?;
            let at = crate::auth_service::iso(crate::auth_service::now());
            let mut row = parent.stored_row();
            row["id"] = json!(target_thread_id);
            row["title"] = command
                .get("title")
                .cloned()
                .unwrap_or_else(|| json!("Subagent"));
            row["createdAt"] = json!(at.clone());
            row["updatedAt"] = json!(at);
            row["lineage"] = json!({"parentThreadId":parent_thread_id,"relationshipToParent":"subagent","rootThreadId":parent.row["lineage"]["rootThreadId"].as_str().unwrap_or(parent_thread_id),"sideChat":false});
            row["forkedFrom"] = json!({"type":"node","nodeId":command["parentNodeId"]});
            let prompt = command["task"].as_str().unwrap_or_default();
            let message = ConversationMessage {
                id: format!(
                    "{}:task",
                    command["commandId"].as_str().unwrap_or(target_thread_id)
                ),
                role: "user".into(),
                content: prompt.into(),
                created_at: now(),
            };
            let thread = Thread {
                row,
                id: target_thread_id.to_owned(),
                project_id: parent.project_id,
                title: command
                    .get("title")
                    .and_then(Value::as_str)
                    .unwrap_or("Subagent")
                    .to_owned(),
                cwd: parent.cwd,
                status: "idle".into(),
                messages: vec![message],
                runs: vec![],
                created_at: now(),
                archived: false,
                settled: false,
                snoozed_until: None,
                pinned: false,
                unread: false,
                tags: vec![],
                checkpoints: vec![],
            };
            return Ok(state
                .commit_receipted(Event::ThreadCreated(thread), Some(receipt.clone()))
                .await?
                .id);
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
            if !parent
                .runs
                .iter()
                .any(|run| run.id.to_string() == parent_run_id)
            {
                return Err(anyhow::anyhow!(format!(
                    "Parent node {} is not the root of run {parent_run_id}.",
                    command["parentNodeId"].as_str().unwrap_or_default()
                )));
            }
            if let Some(target_run_id) = command["targetRunId"].as_str() {
                if !target
                    .runs
                    .iter()
                    .any(|run| run.id.to_string() == target_run_id)
                {
                    return Err(anyhow::anyhow!(format!(
                        "Target run {target_run_id} does not belong to thread {target_thread_id}."
                    )));
                }
            }
            drop(store);
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
