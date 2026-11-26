//! The desktop's first send launches a thread over the orchestration WebSocket.
use crate::{mcp_oauth::McpHost, rpc_protocol::Reply};
use serde_json::{json, Value};
use std::sync::Arc;

pub(crate) fn failure(params: &Value, cause: Value) -> Reply {
    let message = cause["message"]
        .as_str()
        .or_else(|| cause.as_str())
        .unwrap_or("Failed to launch thread.");
    Reply::Failure(json!({"_tag":"Fail","error":{
        "_tag":"OrchestrationV2ThreadLaunchError","commandId":params["commandId"],
        "projectId":params["projectId"],"message":message,"cause":cause
    }}))
}

pub(crate) async fn launch<H: McpHost>(host: &Arc<H>, params: &Value) -> Reply {
    let command_id = params["commandId"].as_str().unwrap_or_default();
    let project_id = params["projectId"].as_str().unwrap_or_default();
    let _launch = host.control_state().thread_launch.lock().await;
    let store = host.control_state().store.read().await;
    let Some(project) = store.projects.get(project_id) else {
        return failure(
            params,
            json!(format!("Project {project_id} was not found.")),
        );
    };
    let root = project.path.clone();
    let receipt = store
        .command_receipts
        .get(command_id)
        .map(|(id, _)| id.as_str());
    let requested = params["threadId"]
        .as_str()
        .filter(|id| !id.trim().is_empty());
    if requested.is_some() && receipt.is_some() && requested != receipt {
        return failure(
            params,
            json!("Launch command already belongs to another thread."),
        );
    }
    let thread_id = requested
        .or(receipt)
        .map(str::to_owned)
        .unwrap_or_else(|| format!("thread:{}", crate::platform::uuid_v4()));
    let existing = store.threads.get(&thread_id);
    let resumed = existing.is_some();
    if existing.is_some_and(|thread| thread.project_id != project_id) {
        return failure(params, json!("The thread belongs to another project."));
    }
    if resumed && receipt.is_none() && params["reuseExistingThread"] != true {
        return failure(
            params,
            json!(format!(
                "Thread {thread_id} already exists. Use a new thread id."
            )),
        );
    }
    if params["reuseExistingThread"] == true && !resumed {
        return failure(params, json!("The thread to reuse does not exist."));
    }
    drop(store);

    let source = params["creationSource"].as_str().unwrap_or("web");
    if !resumed {
        let strategy = &params["workspaceStrategy"];
        // A root launch that names no branch records the project checkout's
        // (`launchBranch`).
        let branch = if strategy["type"] == "root" && strategy.get("branch").is_none() {
            json!(host.checkout_branch(root.clone()).await)
        } else {
            strategy["branch"].clone()
        };
        let path = match strategy["type"].as_str() {
            Some("root") if host.is_scratch_project(project_id).await => {
                let seed = params["initialMessage"]["text"]
                    .as_str()
                    .or_else(|| params["title"].as_str())
                    .unwrap_or_default();
                match host.scratch_thread_folder(&thread_id, seed).await {
                    Ok(folder) => folder.unwrap_or(root),
                    Err(message) => return failure(params, json!(message)),
                }
            }
            Some("root") => root,
            Some("existing_worktree") => match strategy["worktreePath"].as_str() {
                Some(path) => path.to_owned(),
                None => return failure(params, json!("The existing worktree path is required.")),
            },
            // The native server accepts a prepared worktree path at this point.
            Some("worktree") => strategy["path"].as_str().map(str::to_owned).unwrap_or(root),
            _ => return failure(params, json!("Unknown workspace strategy.")),
        };
        let create = json!({
            "type":"thread.create","commandId":command_id,"threadId":thread_id,
            "projectId":project_id,"title":params["title"],"modelSelection":params["modelSelection"],
            "runtimeMode":params["runtimeMode"],"interactionMode":params["interactionMode"],
            "createdBy":"user","creationSource":source,"branch":branch,"worktreePath":path
        });
        if let Err(error) = host.dispatch(create).await {
            return failure(params, error);
        }
    }

    let mut sequence = None;
    if let Some(initial) = params
        .get("initialMessage")
        .filter(|value| value.is_object())
    {
        let message_id = initial["messageId"]
            .as_str()
            .filter(|id| !id.is_empty())
            .map(str::to_owned)
            .unwrap_or_else(|| format!("{command_id}:initial-message"));
        let mut message = json!({
            "type":"message.dispatch","commandId":format!("{command_id}:initial-message"),
            "threadId":thread_id,"messageId":message_id,"text":initial["text"],
            "attachments":initial.get("attachments").cloned().unwrap_or_else(|| json!([])),
            "modelSelection":params["modelSelection"],"runtimeMode":params["runtimeMode"],
            "interactionMode":params["interactionMode"],"createdBy":"user","creationSource":source
        });
        if let Some(context) = initial.get("context") {
            message["context"] = context.clone();
        }
        match host.dispatch(message).await {
            Ok(n) => sequence = Some(n),
            Err(error) => return failure(params, error),
        }
    }
    let store = host.control_state().store.read().await;
    let Some(thread) = store.threads.get(&thread_id) else {
        return failure(params, json!("Thread was not created."));
    };
    let mut result =
        json!({"threadId":thread_id,"projection":thread.projection(),"resumed":resumed});
    if let Some(n) = sequence {
        result["sequence"] = json!(n);
    }
    Reply::Success(result)
}
