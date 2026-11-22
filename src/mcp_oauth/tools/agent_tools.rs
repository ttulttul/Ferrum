//! MCP tools whose implementation belongs to the caller's workspace.
//!
//! Workspace calls deliberately go through [`McpHost::workspace_call`].  A
//! native server and the control plane therefore share the same protocol and
//! agentd workspaces do not accidentally fall back to the plane's filesystem.
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
    Ok(json!({
        "attached": thread.stored_row()["worktreePath"].is_string(),
        "worktreePath": thread.stored_row()["worktreePath"],
        "branch": thread.stored_row()["branch"],
        "projectWorkspaceRoot": project["workspaceRoot"],
        "defaultStartFromOrigin": host.settings()["newWorktreesStartFromOrigin"].as_bool().unwrap_or(true),
    }))
}

pub(super) async fn worktree_handoff<S: McpHost>(
    _host: &Arc<S>,
    caller: &Caller,
    _args: &Value,
) -> Outcome {
    let _ = thread_id(caller)?;
    Err(failure(
        "operation_failed",
        "Worktree handoff is unavailable in this server build.",
    ))
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

fn target_url(target: &Value) -> Option<String> {
    match target["kind"].as_str()? {
        "url" => target["url"].as_str().map(str::to_owned),
        "environment-port" => {
            let protocol = target["protocol"].as_str().unwrap_or("http");
            let port = target["port"].as_u64()?;
            let path = target["path"].as_str().unwrap_or("");
            Some(format!("{protocol}://127.0.0.1:{port}{path}"))
        }
        _ => None,
    }
}

pub(super) async fn preview_call<S: McpHost>(
    host: &Arc<S>,
    caller: &Caller,
    name: &str,
    a: &Value,
) -> Outcome {
    let id = thread_id(caller)?;
    match name {
        "preview_open" => {
            let mut input = a.as_object().cloned().unwrap_or_default();
            input.insert("threadId".into(), json!(id));
            if let Some(target) = input.remove("target") {
                if let Some(url) = target_url(&target) {
                    input.insert("url".into(), json!(url));
                }
            }
            input.remove("open");
            input.remove("show");
            workspace(host, "preview.open", Value::Object(input)).await
        }
        "preview_navigate" => {
            let mut input = a.as_object().cloned().unwrap_or_default();
            input.insert("threadId".into(), json!(id));
            if let Some(target) = input.remove("target") {
                if let Some(url) = target_url(&target) {
                    input.insert("url".into(), json!(url));
                }
            }
            workspace(host, "preview.navigate", Value::Object(input)).await
        }
        "preview_resize" => {
            let mut input = a.as_object().cloned().unwrap_or_default();
            input.insert("threadId".into(), json!(id));
            let mode = input
                .remove("mode")
                .and_then(|value| value.as_str().map(str::to_owned));
            let viewport = match mode.as_deref() {
                Some("freeform") | Some("preset") => json!({
                    "_tag": "fixed",
                    "width": input.remove("width").and_then(|v| v.as_u64()).unwrap_or(1280),
                    "height": input.remove("height").and_then(|v| v.as_u64()).unwrap_or(800),
                }),
                _ => json!({"_tag": "fill"}),
            };
            input.insert("viewport".into(), viewport);
            workspace(host, "preview.resize", Value::Object(input)).await
        }
        "preview_set_appearance" => {
            let mut input = a.as_object().cloned().unwrap_or_default();
            input.insert("threadId".into(), json!(id));
            workspace(host, "preview.adjust", Value::Object(input)).await
        }
        "preview_status" => {
            let tab = a["tabId"].as_str();
            let result = workspace(host, "preview.list", json!({"threadId": id})).await?;
            let found = result["sessions"]
                .as_array()
                .and_then(|sessions| match tab {
                    Some(tab) => sessions.iter().find(|s| s["tabId"] == tab),
                    None if sessions.len() == 1 => sessions.first(),
                    _ => None,
                })
                .cloned()
                .ok_or_else(|| {
                    if tab.is_none()
                        && result["sessions"]
                            .as_array()
                            .is_some_and(|sessions| sessions.len() > 1)
                    {
                        failure(
                            "invalid_request",
                            "Multiple tabs are open. Pass tabId or reuseExistingTab=false.",
                        )
                    } else {
                        failure(
                            "orchestration_error",
                            "No server preview tab is open for this thread. Call preview_open first.",
                        )
                    }
                })?;
            Ok(found)
        }
        "preview_wait_for" => {
            let result = workspace(host, "preview.list", json!({"threadId": id})).await?;
            let tab = a["tabId"].as_str();
            let found = result["sessions"]
                .as_array()
                .and_then(|sessions| match tab {
                    Some(tab) => sessions.iter().find(|s| s["tabId"] == tab),
                    None if sessions.len() == 1 => sessions.first(),
                    _ => None,
                })
                .cloned()
                .ok_or_else(|| {
                    failure(
                        "orchestration_error",
                        "No server preview tab is open for this thread. Call preview_open first.",
                    )
                })?;
            Ok(found)
        }
        _ => Err(failure(
            "orchestration_error",
            "Preview automation is unavailable in this server build.",
        )),
    }
}

pub(super) async fn device<S: McpHost>(
    host: &Arc<S>,
    caller: &Caller,
    name: &str,
    a: &Value,
) -> Outcome {
    let id = thread_id(caller)?;
    match name {
        "device_list" => {
            let mut result = workspace(host, "device.list", a.clone()).await?;
            if let Some(host_id) = a["hostId"].as_str() {
                for key in ["hosts", "devices"] {
                    if let Some(rows) = result[key].as_array_mut() {
                        rows.retain(|row| row["hostId"] == host_id || row["id"] == host_id);
                    }
                }
            }
            if let Some(sessions) = result["sessions"].as_array() {
                result["open"] = json!(sessions
                    .iter()
                    .filter(|session| session["threadId"] == id)
                    .map(
                        |session| json!({"hostId":session["hostId"],"deviceId":session["deviceId"]})
                    )
                    .collect::<Vec<_>>());
            }
            if let Some(object) = result.as_object_mut() {
                object.remove("sessions");
            }
            Ok(result)
        }
        "device_open" => {
            let mut input = a.as_object().cloned().unwrap_or_default();
            input.insert("threadId".into(), json!(id));
            let session = workspace(host, "device.open", Value::Object(input.clone())).await?;
            let state = workspace(host, "device.list", json!({})).await?;
            let device = state["devices"]
                .as_array()
                .and_then(|devices| {
                    devices
                        .iter()
                        .find(|device| device["id"] == session["deviceId"])
                })
                .cloned()
                .unwrap_or_else(|| session.clone());
            let platform = session["platform"].as_str().unwrap_or("android");
            let device_id = session["deviceId"].as_str().unwrap_or_default();
            let flag = if platform == "ios" {
                "--udid"
            } else {
                "--serial"
            };
            let target = format!("agent-device --platform {platform} {flag} {device_id}");
            Ok(
                json!({"device":device,"agentDevice":{"command":"agent-device","targetArgs":["--platform",platform,flag,device_id],"session":null},"quickStart":target}),
            )
        }
        "device_close" => {
            let mut input = a.as_object().cloned().unwrap_or_default();
            input.insert("threadId".into(), json!(id));
            workspace(host, "device.close", Value::Object(input)).await?;
            Ok(json!({}))
        }
        "device_screenshot" => Err(failure(
            "orchestration_error",
            "Device screenshots are unavailable in this server build.",
        )),
        _ => Err(failure("invalid_request", "Unknown device tool.")),
    }
}

fn html_source(caller: &Caller, a: &Value) -> Result<(), Refusal> {
    let has_html = a.get("html").is_some_and(Value::is_string);
    let has_path = a.get("path").is_some_and(Value::is_string);
    if has_html == has_path {
        return Err(failure(
            "invalid_request",
            "Pass exactly one of html or path.",
        ));
    }
    if has_path
        && caller
            .thread
            .as_ref()
            .is_some_and(|t| t.runtime_mode != "full-access")
    {
        return Err(failure(
            "capability_denied",
            "Pass html instead of path: only full-access threads may pass a path.",
        ));
    }
    Ok(())
}

pub(super) async fn html<S: McpHost>(
    host: &Arc<S>,
    caller: &Caller,
    name: &str,
    a: &Value,
) -> Outcome {
    html_source(caller, a)?;
    let _ = host;
    let _ = name;
    Err(failure(
        "orchestration_error",
        "HTML rendering is unavailable in this server build.",
    ))
}
