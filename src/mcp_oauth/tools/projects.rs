//! The reference's project changes past creation (`toolkits/project/handlers.ts`),
//! `t3_environment_preferences_update` (`toolkits/environment/handlers.ts`) and
//! `t3_worktree_list` (`toolkits/worktree/handlers.ts`).
use super::*;

pub(super) const NOT_LINKED_WHAT: &str = "change projects or environment settings";

/// `McpToolAccess.writesEnvironment`: a writing caller (not read-only, a thread
/// that still owns a live run) that is full-access/default, and, unless the
/// tool lets it, not a fenced link's.
pub(super) async fn environment_writer<S: McpHost>(
    host: &S,
    caller: &Caller,
    refused: Option<&str>,
) -> Result<(), Refusal> {
    caller.refuse_read_only()?;
    threads::writing_caller(host, caller).await?;
    writes_environment(caller, refused)
}

fn new_command_id() -> String {
    format!("mcp:{}", crate::platform::uuid_v4())
}

/// A project as `ProjectService` returns it after a change: never with
/// `hasCustomTitle`, which only the shell rows carry.
fn changed_project<S: McpHost>(host: &S, row: &Value) -> Value {
    let mut out = project_out(host, row);
    if let Some(object) = out.as_object_mut() {
        object.remove("hasCustomTitle");
    }
    out
}

/// `projectFailure`.
fn project_failure(refusal: crate::mcp_oauth::ProjectRefusal) -> Refusal {
    use crate::mcp_oauth::ProjectRefusal as Project;
    let message = match refusal {
        Project::Operation => {
            return failure(
                "orchestration_error",
                "The operation could not be completed.",
            )
        }
        Project::Invalid(detail) => detail,
        Project::NotFound => "The project was not found.".into(),
        Project::Conflict => "The workspace is already registered to a project.".into(),
        Project::NotEmpty => {
            "The project is not empty; force=true is required to delete it.".into()
        }
    };
    failure("invalid_request", message)
}

fn without_environment(args: &Value) -> Value {
    let mut out = args.as_object().cloned().unwrap_or_default();
    out.remove("environmentId");
    Value::Object(out)
}

pub(super) async fn update<S: McpHost>(host: &Arc<S>, caller: &Caller, a: &Value) -> Outcome {
    environment_writer(&**host, caller, Some(NOT_LINKED_WHAT)).await?;
    let project_id = required(a, "projectId")?;
    let mut command = json!({
        "type": "project.update",
        "commandId": new_command_id(),
        "projectId": project_id,
    });
    for key in [
        "title",
        "workspaceRoot",
        "defaultModelSelection",
        "autoPull",
        "projectIcon",
        "faviconPath",
        "defaultThreadEnvMode",
        "scripts",
        "pinned",
        "archived",
    ] {
        if let Some(value) = a.get(key).filter(|v| !v.is_null()) {
            command[key] = value.clone();
        }
    }
    let row = host
        .project_mutation(command)
        .await
        .map_err(project_failure)?;
    Ok(changed_project(&**host, &row))
}

pub(super) async fn delete<S: McpHost>(host: &Arc<S>, caller: &Caller, a: &Value) -> Outcome {
    environment_writer(&**host, caller, Some(NOT_LINKED_WHAT)).await?;
    let project_id = required(a, "projectId")?;
    if host.is_scratch_project(project_id).await {
        return Err(failure(
            "invalid_request",
            "\"No project\" is the built-in scratch project and cannot be deleted through MCP. The user can remove it in Settings.",
        ));
    }
    let mut command = json!({
        "type": "project.delete",
        "commandId": new_command_id(),
        "projectId": project_id,
    });
    if let Some(force) = arg_bool(a, "force")? {
        command["force"] = json!(force);
    }
    let row = host
        .project_mutation(command)
        .await
        .map_err(project_failure)?;
    Ok(changed_project(&**host, &row))
}

pub(super) async fn clone<S: McpHost>(host: &Arc<S>, caller: &Caller, a: &Value) -> Outcome {
    // An existing destination is never adopted, so linked work may clone.
    environment_writer(&**host, caller, None).await?;
    host.clone_repository(without_environment(a))
        .await
        .map_err(|detail| failure("orchestration_error", detail))
}

/// `preferences`: the environment-wide settings an agent reads and changes,
/// custom instructions cut to 4,000 characters.
pub(super) fn preferences(settings: &Value) -> Value {
    let style = &settings["sourceControlWritingStyle"];
    let instructions = style["customInstructions"].as_str().unwrap_or("");
    let count = instructions.chars().count();
    let mut out = json!({
        "newWorktreesStartFromOrigin": settings["newWorktreesStartFromOrigin"],
        "enableProviderUpdateChecks": settings["enableProviderUpdateChecks"],
        "backgroundActivity": {"profile": settings["backgroundActivity"]["profile"].as_str().unwrap_or("balanced")},
        "sourceControlWritingStyle": {
            "mode": style["mode"].as_str().unwrap_or("repo_conventions"),
            "followChangeRequestTemplates": style["followChangeRequestTemplates"].as_bool().unwrap_or(true),
            "customInstructions": instructions.chars().take(4000).collect::<String>(),
            "truncated": count > 4000,
        }
    });
    // Present only when it is set, as the reference reads it.
    if let Some(mode) = settings
        .get("defaultThreadEnvMode")
        .filter(|m| !m.is_null())
    {
        out["defaultThreadEnvMode"] = mode.clone();
    }
    out
}

pub(super) async fn preferences_update<S: McpHost>(
    host: &Arc<S>,
    caller: &Caller,
    a: &Value,
) -> Outcome {
    environment_writer(&**host, caller, Some(NOT_LINKED_WHAT)).await?;
    host.update_settings(without_environment(a))
        .await
        .map_err(|()| {
            failure(
                "orchestration_error",
                "The operation could not be completed.",
            )
        })?;
    Ok(preferences(&host.settings()))
}

pub(super) async fn worktree_list<S: McpHost>(
    host: &Arc<S>,
    caller: &Caller,
    a: &Value,
) -> Outcome {
    let thread_id = arg_str(a, "threadId")?;
    let id = threads::read_thread(&**host, caller, thread_id).await?;
    let (worktree, root) = {
        let store = host.control_state().store.read().await;
        let thread = find_thread(&store, &id)?;
        let row = thread.stored_row();
        let root = store
            .project_rows
            .get(&thread.project_id)
            .filter(|p| p["deletedAt"].is_null())
            .map(|p| p["workspaceRoot"].clone())
            .ok_or_else(|| failure("invalid_request", "The project was not found."))?;
        (row["worktreePath"].clone(), root)
    };
    let mut input = without_environment(a);
    if let Some(object) = input.as_object_mut() {
        object.remove("threadId");
        object.insert("worktreePath".into(), worktree);
        object.insert("workspaceRoot".into(), root);
    }
    host.list_refs(input).await.map_err(|()| {
        failure(
            "orchestration_error",
            "The operation could not be completed.",
        )
    })
}
