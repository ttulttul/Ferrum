//! The attachment toolkit (`toolkits/attachment` in the reference): an agent
//! stages a file as a pending upload (`t3_attachment_prepare_upload`), posts
//! its bytes to the upload URL, and sends it to a thread
//! (`t3_thread_send_attachments`), or discards an upload it no longer needs
//! (`t3_attachment_discard`). The files are the host's (`attachment_store`).
use super::*;
use std::collections::HashMap;
use std::sync::{LazyLock, Mutex};

/// `ATTACHMENT_UPLOAD_URL_TTL_MS`.
const UPLOAD_URL_TTL_MS: i64 = 10 * 60 * 1000;
/// The 24-hour sweep starts at upload completion, possibly one upload-URL
/// lifetime after issue (`UPLOAD_OWNER_TTL_MS`).
const UPLOAD_OWNER_TTL_MS: i64 = 24 * 60 * 60 * 1000 + UPLOAD_URL_TTL_MS;

/// Who prepared each pending upload, and when (`uploadOwners`).
static UPLOAD_OWNERS: LazyLock<Mutex<HashMap<String, (String, i64)>>> =
    LazyLock::new(Mutex::default);

/// `uploadOwner`: the calling thread, else the client's session.
fn upload_owner(caller: &Caller) -> String {
    caller.granted.thread.as_ref().map_or_else(
        || caller.request_namespace(),
        |scope| scope.thread_id.clone(),
    )
}

fn unavailable() -> Refusal {
    failure(
        "orchestration_error",
        "The operation could not be completed.",
    )
}

fn store_failure((code, message): (&'static str, String)) -> Refusal {
    failure(code, message)
}

/// `t3_attachment_prepare_upload` (`writes`, linked callers allowed): an
/// upload URL for the file, which belongs to its caller until a send claims it.
pub(super) async fn prepare_upload<S: McpHost>(
    host: &Arc<S>,
    caller: &Caller,
    a: &Value,
) -> Outcome {
    threads::writing_caller(&**host, caller).await?;
    let mut result = match host.attachment_store("prepare", a["upload"].clone()).await {
        Ok(result) => result,
        // `AttachmentCreateUploadUrlInput` refused the upload: the tool's
        // parameter failure, at the field under `upload`.
        Err(("invalid_arguments", issue)) => {
            return Err(Refusal::Rpc(
                -32602,
                format!(
                    "Invalid parameters for tool 't3_attachment_prepare_upload': {}",
                    issue.replace("\n  at [", "\n  at [\"upload\"][")
                ),
            ))
        }
        Err(_) => return Err(unavailable()),
    };
    let now = crate::platform::now_ms();
    {
        let mut owners = UPLOAD_OWNERS.lock().unwrap();
        owners.retain(|_, (_, issued)| now - *issued <= UPLOAD_OWNER_TTL_MS);
        if let Some(id) = result["attachmentId"].as_str() {
            owners.insert(id.to_owned(), (upload_owner(caller), now));
        }
    }
    if let (Some(origin), Some(relative)) = (&caller.origin, result["relativeUrl"].as_str()) {
        result["uploadUrl"] = json!(format!("{origin}{relative}"));
    }
    Ok(result)
}

/// `t3_attachment_discard` (`writes`, linked callers allowed): only the
/// caller that prepared a pending upload may discard it.
pub(super) async fn discard<S: McpHost>(host: &Arc<S>, caller: &Caller, a: &Value) -> Outcome {
    threads::writing_caller(&**host, caller).await?;
    let id = a["attachmentId"].as_str().unwrap_or_default().to_owned();
    let owned = UPLOAD_OWNERS
        .lock()
        .unwrap()
        .get(&id)
        .is_some_and(|(owner, _)| *owner == upload_owner(caller));
    if !owned {
        return Err(failure(
            "invalid_request",
            "Only the caller that prepared a pending upload can discard it.",
        ));
    }
    host.attachment_store("discard", json!({"attachmentId": id}))
        .await
        .map_err(store_failure)?;
    UPLOAD_OWNERS.lock().unwrap().remove(&id);
    Ok(json!({}))
}

/// `resolveAttachmentReferences`: each attachment a pending upload, or one
/// the target thread's messages already hold (as they hold it).
fn resolve_references(requested: &[Value], stored: &[Value]) -> Result<Vec<Value>, Refusal> {
    requested
        .iter()
        .map(|attachment| {
            let id = attachment["id"].as_str().unwrap_or_default();
            if id.starts_with("pending-") {
                return Ok(attachment.clone());
            }
            stored
                .iter()
                .find(|s| s["id"] == id)
                .cloned()
                .ok_or_else(|| {
                    failure(
                    "invalid_request",
                    "Attachments must be pending uploads or already belong to the target thread.",
                )
                })
        })
        .collect()
}

/// `t3_thread_send_attachments` (`writesThreads`): the attachments, claimed
/// for the target thread, sent as a new message (`ThreadMessageIntake.sendToThread`).
pub(super) async fn send<S: McpHost>(host: &Arc<S>, caller: &Caller, a: &Value) -> Outcome {
    threads::writing_caller(&**host, caller).await?;
    let thread_id = match arg_str(a, "threadId")? {
        Some(id) => id.to_owned(),
        None => caller
            .thread
            .as_ref()
            .map(|t| t.id.clone())
            .ok_or_else(|| {
                failure(
                    "target_required",
                    "Pass threadId: this MCP client is not running inside a T3 thread.",
                )
            })?,
    };
    let text = arg_str(a, "message")?.unwrap_or_default().to_owned();
    let requested = a["attachments"].as_array().cloned().unwrap_or_default();
    let (busy, selection, stored) = {
        let store = host.control_state().store.read().await;
        let thread = find_thread(&store, &thread_id)?;
        let row = thread.stored_row();
        if caller.thread.as_ref().is_none_or(|t| t.id != thread_id) {
            within_limits(caller, &row)?;
        }
        if !row["archivedAt"].is_null() {
            return Err(failure(
                "invalid_request",
                "Unarchive the target thread before sending attachments.",
            ));
        }
        let projection = thread.projection();
        let stored: Vec<Value> = projection["messages"]
            .as_array()
            .into_iter()
            .flatten()
            .flat_map(|m| m["attachments"].as_array().cloned().unwrap_or_default())
            .collect();
        (
            active(&runs(&projection)).is_some(),
            row["modelSelection"].clone(),
            stored,
        )
    };
    let attachments = resolve_references(&requested, &stored)?;
    let claimed = host
        .attachment_store(
            "claim",
            json!({"threadId": thread_id, "attachments": attachments}),
        )
        .await
        .map_err(store_failure)?;
    let command_id = format!("mcp:{}", crate::platform::uuid_v4());
    let mut dispatch = json!({
        "type": "message.dispatch",
        "commandId": command_id,
        "threadId": thread_id,
        "messageId": command_id,
        "text": text,
        "attachments": claimed["attachments"],
        // Rust starts or queues a send; it does not steer a running turn with attachments.
        "dispatchMode": {"type": if busy { "queue_after_active" } else { "start_immediately" }},
        "createdBy": "agent",
        "creationSource": "mcp",
    });
    if let Some(sender) = caller.granted.thread.as_ref() {
        dispatch["senderThreadId"] = json!(sender.thread_id);
    }
    if selection.is_object() {
        dispatch["modelSelection"] = selection;
    }
    if let Err(error) = host.dispatch(dispatch).await {
        let _ = host
            .attachment_store("release", json!({"paths": claimed["paths"]}))
            .await;
        return Err(failure(
            "orchestration_error",
            format!(
                "Unable to send to thread {thread_id}: {}",
                dispatch_message(&error)
            ),
        ));
    }
    let store = host.control_state().store.read().await;
    let thread = find_thread(&store, &thread_id)?;
    let run = runs(&thread.projection())
        .into_iter()
        .find(|run| run["userMessageId"] == command_id.as_str())
        .ok_or_else(unavailable)?;
    Ok(json!({
        "threadId": thread_id,
        "messageId": command_id,
        "runId": run["id"],
        // Rust records a run as running from its start; the reference's run is
        // starting until its provider turn has an attempt.
        "status": if run["status"] == "running" && run["activeAttemptId"].is_null() {
            json!("starting")
        } else {
            run["status"].clone()
        },
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_send_names_pending_uploads_or_the_targets_own_attachments() {
        let stored = vec![
            json!({"id": "thread-t-0b9c2c7e-6a2b-4b8f-9f1e-2f5b7c3a1d00", "name": "kept.png", "type": "image"}),
        ];
        let pending =
            json!({"id": "pending-0b9c2c7e-6a2b-4b8f-9f1e-2f5b7c3a1d01", "name": "new.png"});
        let owned =
            json!({"id": "thread-t-0b9c2c7e-6a2b-4b8f-9f1e-2f5b7c3a1d00", "name": "renamed.png"});
        let Ok(resolved) = resolve_references(&[pending.clone(), owned], &stored) else {
            panic!("refused");
        };
        assert_eq!(resolved[0], pending);
        // An attachment the thread holds goes as the thread holds it.
        assert_eq!(resolved[1]["name"], "kept.png");
        let foreign = json!({"id": "thread-other-0b9c2c7e-6a2b-4b8f-9f1e-2f5b7c3a1d02"});
        assert!(resolve_references(&[foreign], &stored).is_err());
    }
}
