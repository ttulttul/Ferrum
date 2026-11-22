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
