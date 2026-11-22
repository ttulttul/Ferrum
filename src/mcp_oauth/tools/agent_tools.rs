//! MCP tools whose implementation belongs to the caller's workspace.
use super::*;

fn thread_id(caller: &Caller) -> Result<&str, Refusal> {
    caller
        .thread
        .as_ref()
        .map(|thread| thread.id.as_str())
        .ok_or_else(|| {
            failure(
                "thread_credential_required",
                "This tool acts as the calling T3 thread, so it needs an agent running inside T3 Code.",
            )
        })
}

async fn workspace<S: McpHost>(host: &Arc<S>, method: &str, input: Value) -> Outcome {
    host.workspace_call(method.to_owned(), input)
        .await
        .map_err(|error| {
            let message = error["message"]
                .as_str()
                .or_else(|| error["reason"].as_str())
                .unwrap_or("The workspace operation could not be completed.");
            failure(
                if error["_tag"] == "Unavailable" {
                    "orchestration_error"
                } else {
                    "invalid_request"
                },
                message,
            )
        })
}

fn workspace_error(error: &Value, prefix: &str) -> Refusal {
    let message = error["message"]
        .as_str()
        .or_else(|| error["detail"].as_str())
        .or_else(|| error["cause"]["message"].as_str())
        .or_else(|| error["reason"].as_str())
        .unwrap_or("The workspace operation could not be completed.");
    failure("operation_failed", format!("{prefix}: {message}"))
}

async fn workspace_raw<S: McpHost>(
    host: &Arc<S>,
    method: &str,
    input: Value,
) -> Result<Value, Value> {
    host.workspace_call(method.to_owned(), input).await
}

pub(super) async fn worktree_status<S: McpHost>(host: &Arc<S>, caller: &Caller) -> Outcome {
    let id = thread_id(caller)?;
    let store = host.control_state().store.read().await;
    let thread = find_thread(&store, id)?;
    let project = store
        .project_rows
        .get(&thread.project_id)
        .filter(|row| row["deletedAt"].is_null())
        .ok_or_else(|| failure("project_not_found", "The project was not found."))?;
    let row = thread.stored_row();
    Ok(json!({
        "attached": row["worktreePath"].is_string(),
        "worktreePath": row["worktreePath"],
        "branch": row["branch"],
        "projectWorkspaceRoot": project["workspaceRoot"],
        "defaultStartFromOrigin": host.settings()["newWorktreesStartFromOrigin"].as_bool().unwrap_or(true),
    }))
}

pub(super) async fn worktree_handoff<S: McpHost>(
    host: &Arc<S>,
    caller: &Caller,
    args: &Value,
) -> Outcome {
    let id = thread_id(caller)?;
    let branch = required(args, "branch")?.to_owned();
    let requested_base = arg_str(args, "baseRef")?.map(str::to_owned);
    let start_from_origin = arg_bool(args, "startFromOrigin")?;
    let run_setup = arg_bool(args, "runSetupScript")?.unwrap_or(true);
    let continuation_prompt = arg_str(args, "continuationPrompt")?.map(str::to_owned);

    let (project_id, project_root) = {
        let store = host.control_state().store.read().await;
        let thread = find_thread(&store, id)?;
        let row = thread.stored_row();
        if row["worktreePath"].is_string() {
            return Err(failure(
                "already_in_worktree",
                format!(
                    "Thread '{id}' is already attached to worktree '{}'.",
                    row["worktreePath"].as_str().unwrap_or_default()
                ),
            ));
        }
        if row["archivedAt"].is_string() || row["archived"] == true {
            return Err(failure(
                "invalid_request",
                format!("Thread '{id}' is archived and cannot be handed off to a worktree."),
            ));
        }
        let project = store
            .project_rows
            .get(&thread.project_id)
            .filter(|row| row["deletedAt"].is_null())
            .ok_or_else(|| {
                failure(
                    "project_not_found",
                    format!(
                        "Project '{}' was not found for thread '{}'.",
                        thread.project_id, id
                    ),
                )
            })?;
        let root = project["workspaceRoot"]
            .as_str()
            .ok_or_else(|| failure("project_not_found", "The project workspace was not found."))?;
        (thread.project_id.clone(), root.to_owned())
    };

    let status = workspace_raw(
        host,
        "vcs.refreshStatus",
        json!({"cwd": project_root.clone(), "localOnly": true}),
    )
    .await
    .map_err(|error| workspace_error(&error, "Unable to read git status"))?;
    if status["isRepo"] != true {
        return Err(failure(
            "invalid_request",
            format!("Project workspace '{project_root}' is not a git repository."),
        ));
    }

    let local_refs = workspace_raw(
        host,
        "vcs.listRefs",
        json!({"cwd": project_root.clone(), "refKind": "local", "query": branch.clone(), "limit": 100}),
    )
    .await
    .map_err(|error| workspace_error(&error, "Unable to list branches"))?;
    if let Some(existing) = local_refs["refs"].as_array().and_then(|refs| {
        refs.iter()
            .find(|reference| reference["name"] == branch && reference["isRemote"] != true)
    }) {
        let checkout = existing["worktreePath"].as_str();
        let location = checkout.map_or_else(String::new, |path| {
            format!(" and is checked out at '{path}'")
        });
        let remove = if checkout.is_some() {
            " and its worktree"
        } else {
            ""
        };
        return Err(failure(
            "invalid_request",
            format!(
                "Branch '{branch}' already exists{location}. Choose a different branch name, or delete the existing branch{remove} first."
            ),
        ));
    }

    let base_ref = match requested_base {
        Some(base) => base,
        None => status["refName"].as_str().map(str::to_owned).ok_or_else(|| {
            failure(
                "invalid_request",
                "Could not determine the current branch of the project workspace (detached HEAD?). Pass baseRef explicitly.",
            )
        })?,
    };
    let start_from_origin = start_from_origin.unwrap_or_else(|| {
        host.settings()["newWorktreesStartFromOrigin"]
            .as_bool()
            .unwrap_or(true)
    });

    let (worktree_ref, base_ref_name, started_from_origin) = if start_from_origin {
        // A status refresh with localOnly=false performs the configured automatic fetch. The
        // following refs read then sees the same remote base that the reference uses.
        let _ = workspace_raw(
            host,
            "vcs.refreshStatus",
            json!({"cwd": project_root.clone(), "localOnly": false}),
        )
        .await;
        let refs = workspace_raw(
            host,
            "vcs.listRefs",
            json!({"cwd": project_root.clone(), "refKind": "remote", "limit": 500}),
        )
        .await
        .map_err(|error| workspace_error(&error, "Unable to list remote branches"))?;
        let wanted = if base_ref.starts_with("origin/") {
            base_ref.clone()
        } else {
            format!("origin/{base_ref}")
        };
        let remote = refs["refs"]
            .as_array()
            .and_then(|all| all.iter().find(|reference| reference["name"] == wanted));
        if let Some(remote) = remote {
            let name = remote["name"].as_str().unwrap_or(&wanted);
            let remote_name = remote["remoteName"].as_str().unwrap_or("origin");
            let branch_name = name
                .strip_prefix(&format!("{remote_name}/"))
                .unwrap_or(name);
            (
                name.to_owned(),
                format!("refs/remotes/{remote_name}/{branch_name}"),
                true,
            )
        } else {
            (base_ref.clone(), base_ref.clone(), false)
        }
    } else {
        (base_ref.clone(), base_ref.clone(), false)
    };

    let worktree = workspace_raw(
        host,
        "vcs.createWorktree",
        json!({
            "cwd": project_root.clone(),
            "refName": worktree_ref,
            "newRefName": branch.clone(),
            "baseRefName": base_ref_name,
            "path": null,
        }),
    )
    .await
    .map_err(|error| workspace_error(&error, "Unable to create the worktree"))?;
    let worktree_path = worktree["worktree"]["path"]
        .as_str()
        .ok_or_else(|| {
            failure(
                "operation_failed",
                "Unable to create the worktree: missing path.",
            )
        })?
        .to_owned();
    let actual_branch = worktree["worktree"]["refName"]
        .as_str()
        .unwrap_or(&branch)
        .to_owned();

    let command_id = format!(
        "command:mcp:{}:worktree-handoff:{}",
        caller
            .thread
            .as_ref()
            .map(|thread| thread.provider_instance_id.as_str())
            .unwrap_or("thread"),
        crate::platform::uuid_v4()
    );
    let continuation_message_id = format!(
        "message:mcp:worktree-continuation:{}",
        crate::platform::uuid_v4()
    );
    let mut command = json!({
        "type": "thread.metadata.update",
        "commandId": command_id,
        "threadId": id,
        "branch": actual_branch,
        "worktreePath": worktree_path,
        "expectedWorktreePath": null,
    });
    if let Some(prompt) = &continuation_prompt {
        command["continuation"] = json!({
            "messageId": continuation_message_id,
            "text": prompt,
        });
    }
    if let Err(error) = host.dispatch(command).await {
        let _ = workspace_raw(
            host,
            "vcs.removeWorktree",
            json!({"cwd": project_root, "path": worktree_path, "force": true}),
        )
        .await;
        return Err(workspace_error(
            &error,
            "Unable to re-point the thread at the worktree",
        ));
    }

    let _ = workspace_raw(
        host,
        "vcs.refreshStatus",
        json!({"cwd": worktree_path, "localOnly": true}),
    )
    .await;

    let setup_script = if !run_setup {
        json!({"status":"skipped"})
    } else {
        let script = {
            let store = host.control_state().store.read().await;
            store
                .project_rows
                .get(&project_id)
                .and_then(|project| project["scripts"].as_array())
                .and_then(|scripts| {
                    scripts.iter().find(|script| {
                        script["runOnWorktreeCreate"] == true
                            && script["command"].as_str().is_some()
                    })
                })
                .cloned()
        };
        match script {
            None => json!({"status":"no-script"}),
            Some(script) => {
                let terminal_id = format!("terminal-{}", crate::platform::uuid_v4());
                let open = workspace_raw(
                    host,
                    "terminal.open",
                    json!({
                        "threadId": id,
                        "terminalId": terminal_id,
                        "cwd": worktree_path,
                        "worktreePath": worktree_path,
                        "cols": 120,
                        "rows": 30,
                        "env": {"T3CODE_WORKTREE_PATH": worktree_path},
                    }),
                )
                .await;
                if let Err(error) = open {
                    json!({"status":"failed","detail": error_message(&error)})
                } else {
                    let command = script["command"].as_str().unwrap_or_default();
                    match workspace_raw(
                host,
                "terminal.write",
                json!({"threadId": id, "terminalId": terminal_id, "data": format!("{command}\n")}),
            )
            .await
            {
                Ok(_) => json!({
                    "status":"started",
                    "scriptName": script["name"],
                    "terminalId": terminal_id,
                }),
                Err(error) => json!({"status":"failed","detail": error_message(&error)}),
            }
                }
            }
        }
    };

    let continuation = continuation_prompt.map_or_else(
        || json!({"status":"skipped"}),
        |_| json!({"status":"scheduled","delivery":"queued"}),
    );
    let note = if continuation["status"] == "scheduled" {
        "Handoff recorded. Changing the workspace detaches this provider session, so the current turn ends shortly after this call; the queued continuation prompt then starts the next turn inside the worktree with the conversation preserved. The worktree is not removed automatically when the thread is deleted."
    } else {
        "Handoff recorded. Changing the workspace detaches this provider session, so the current turn ends shortly after this call; the conversation continues inside the worktree when the thread receives its next message. Pass continuationPrompt to resume automatically. The worktree is not removed automatically when the thread is deleted."
    };
    Ok(json!({
        "worktreePath": worktree_path,
        "branch": actual_branch,
        "baseRef": base_ref,
        "startedFromOrigin": started_from_origin,
        "setupScript": setup_script,
        "continuation": continuation,
        "note": note,
    }))
}

fn error_message(error: &Value) -> String {
    error["message"]
        .as_str()
        .or_else(|| error["detail"].as_str())
        .or_else(|| error["reason"].as_str())
        .unwrap_or("The workspace operation could not be completed.")
        .to_owned()
}

pub(super) async fn preview_list<S: McpHost>(host: &Arc<S>, caller: &Caller, a: &Value) -> Outcome {
    let id = thread_id(caller)?;
    let result = workspace(host, "preview.list", json!({"threadId": id})).await?;
    let sessions = result["sessions"].as_array().cloned().unwrap_or_default();
    let start = a["cursor"].as_u64().unwrap_or(0) as usize;
    let limit = a["limit"].as_u64().unwrap_or(20).min(50) as usize;
    let end = (start + limit).min(sessions.len());
    Ok(json!({
        "sessions": sessions.get(start..end).unwrap_or_default(),
        "serverEpoch": result["serverEpoch"],
        "revision": result["revision"],
        "nextCursor": (end < sessions.len()).then_some(end),
    }))
}

pub(super) async fn preview_close<S: McpHost>(
    host: &Arc<S>,
    caller: &Caller,
    a: &Value,
) -> Outcome {
    let id = thread_id(caller)?;
    let tab = required(a, "tabId")?;
    workspace(host, "preview.close", json!({"threadId": id, "tabId": tab})).await?;
    Ok(json!({}))
}
