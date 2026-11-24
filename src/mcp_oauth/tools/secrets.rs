//! `request_secret` (`OrchestratorMcpService.requestSecret`): the agent asks
//! the user for a secret on a card in its running turn and waits for the
//! answer. The value never reaches the agent: it gets a one-use `secretRef`,
//! or with `delivery: "file"` the path of an owner-only file its commands
//! read.
use super::*;
use crate::secret_requests::{failure_message, terminal_run, Store};

const DEFAULT_TIMEOUT_MS: u64 = 10 * 60 * 1_000;
const MAX_TIMEOUT_MS: u64 = 60 * 60 * 1_000;
const POLL_INTERVAL_MS: u64 = 500;

/// `SECRET_REQUEST_UNSAVED_NOTE`.
fn unsaved_note(status: &str) -> &'static str {
    match status {
        "declined" => "The user declined. Don't ask for it in chat or another way; say what's blocked and go on without it.",
        "cancelled" => "The card closed with the run. Ask again with a new clientRequestId only if the secret is still needed.",
        _ => "No answer in time. Don't ask in chat; ask again with a new clientRequestId only if it is still needed.",
    }
}

fn store_failure(failure: crate::secret_requests::Failure) -> Refusal {
    super::failure("orchestration_error", failure_message(failure, None))
}

/// Closes the card as cancelled when the call ends before its answer, as
/// when the agent's MCP request is dropped: only this call can hand the
/// agent its ref, so the card must not outlive it.
struct CloseCard<S: McpHost> {
    host: Arc<S>,
    command: Option<Value>,
}

impl<S: McpHost> CloseCard<S> {
    async fn close(&mut self) {
        if let Some(command) = self.command.take() {
            if let Err(error) = self.host.dispatch(command).await {
                tracing::warn!(error = %dispatch_message(&error), "could not close a secret request card");
            }
        }
    }

    fn disarm(&mut self) {
        self.command = None;
    }
}

impl<S: McpHost> Drop for CloseCard<S> {
    fn drop(&mut self) {
        if let Some(command) = self.command.take() {
            let host = self.host.clone();
            self.host.spawn(Box::pin(async move {
                let _ = host.dispatch(command).await;
            }));
        }
    }
}

/// The card and its run's status, or why they could not be read.
async fn read_card<S: McpHost>(
    host: &S,
    thread_id: &str,
    card_id: &str,
    run_id: &str,
) -> Result<(Option<Value>, Option<&'static str>), Refusal> {
    let store = host.control_state().store.read().await;
    let thread = store.threads.get(thread_id).ok_or_else(|| {
        failure(
            "orchestration_error",
            format!("Unable to read the secret request: No orchestration projection exists for thread {thread_id}."),
        )
    })?;
    let card = thread
        .link_requests
        .iter()
        .find(|c| c["id"] == card_id && c["type"] == "secret_request")
        .cloned();
    let run = thread
        .runs
        .iter()
        .find(|run| run.id.to_string() == run_id || run.wire_id() == run_id)
        .map(|run| thread.projected_run_status(run).0);
    Ok((card, run))
}

pub(super) async fn request_secret<S: McpHost>(
    host: &Arc<S>,
    caller: &Caller,
    a: &Value,
) -> Outcome {
    let label = arg_str(a, "label")?.unwrap_or_default().to_owned();
    let reason = arg_str(a, "reason")?.unwrap_or_default().to_owned();
    let url = arg_str(a, "url")?.map(str::to_owned);
    let placeholder = arg_str(a, "placeholder")?.map(str::to_owned);
    let delivery = arg_str(a, "delivery")?.map(str::to_owned);
    let value = arg_str(a, "value")?.map(str::to_owned);
    let timeout = a["timeoutMs"].as_u64();
    let key = arg_str(a, "clientRequestId")?.map(str::to_owned);
    delegation::thread_scope(caller, "This tool")?;
    threads::writing_caller(&**host, caller).await?;
    let scope = delegation::thread_scope(caller, "request_secret")?;
    let thread_id = scope.thread_id.clone();
    let (run_id, node_id, project_id) = {
        let store = host.control_state().store.read().await;
        let thread = find_thread(&store, &thread_id)?;
        let projection = thread.full_projection();
        let run = projection["runs"]
            .as_array()
            .into_iter()
            .flatten()
            .filter(|run| {
                matches!(
                    run["status"].as_str(),
                    Some("preparing" | "starting" | "running" | "waiting")
                )
            })
            .max_by_key(|run| run["ordinal"].as_u64().unwrap_or(0))
            .filter(|run| {
                !run["rootNodeId"].is_null()
                    && run["providerInstanceId"] == scope.provider_instance_id.as_str()
            })
            .ok_or_else(|| {
                failure(
                    "parent_not_active",
                    "Asking for a secret requires an active run owned by this MCP provider session.",
                )
            })?;
        (
            run["id"].as_str().unwrap_or_default().to_owned(),
            run["rootNodeId"].clone(),
            thread.project_id.clone(),
        )
    };
    let store = host.secrets_dir().map(Store::new);
    let key = key.unwrap_or_else(|| crate::platform::uuid_v4().to_string());
    // A secret the user already pasted needs no card: it goes straight to a file.
    if let Some(value) = value {
        if delivery.as_deref() == Some("secretRef") {
            return Err(failure(
                "invalid_request",
                "A pasted `value` is saved as a file. Leave out `delivery: \"secretRef\"`.",
            ));
        }
        let store = store
            .as_ref()
            .ok_or_else(|| store_failure(crate::secret_requests::Failure::StoreFailed))?;
        let file = store
            .save_pasted(&thread_id, &caller.request_namespace(), &key, &value)
            .map_err(store_failure)?;
        return Ok(json!({"status": "saved", "file": file}));
    }
    // A later run that reuses a clientRequestId gets its own card (#38167).
    let key = format!("{run_id}:{key}");
    let part = crate::thread_projection::uri_component;
    let card_id = format!(
        "turn-item:secret-request:{}:{}",
        part(&thread_id),
        part(&key)
    );
    let record = |status: &str| {
        let mut command = json!({
            "type": "secret_request.record",
            "commandId": delegation::stable_command_id(caller, &format!("secret-{status}"), &key),
            "threadId": thread_id,
            "runId": run_id,
            "nodeId": node_id,
            "turnItemId": card_id,
            "label": label,
            "reason": reason,
        });
        for (field, value) in [("url", &url), ("placeholder", &placeholder)] {
            if let Some(value) = value {
                command[field] = json!(value);
            }
        }
        if delivery.as_deref() == Some("file") {
            command["delivery"] = json!("file");
        }
        command["secretStatus"] = json!(status);
        command
    };
    host.dispatch(record("pending")).await.map_err(|error| {
        failure(
            "orchestration_error",
            format!(
                "Could not record the secret request: {}",
                dispatch_message(&error)
            ),
        )
    })?;
    let mut close = CloseCard {
        host: host.clone(),
        command: Some(record("cancelled")),
    };
    let deadline =
        crate::platform::monotonic_ms() + timeout.unwrap_or(DEFAULT_TIMEOUT_MS).min(MAX_TIMEOUT_MS);
    let answered = loop {
        let (card, run) = match read_card(&**host, &thread_id, &card_id, &run_id).await {
            Ok(read) => read,
            Err(refusal) => {
                close.close().await;
                return Err(refusal);
            }
        };
        if let Some(status) = card
            .as_ref()
            .and_then(|c| c["secretStatus"].as_str())
            .filter(|s| *s != "pending")
        {
            break Some(status.to_owned());
        }
        if run.is_none_or(terminal_run) {
            close.close().await;
            break Some("cancelled".to_owned());
        }
        if crate::platform::monotonic_ms() >= deadline {
            break None;
        }
        host.sleep(std::time::Duration::from_millis(POLL_INTERVAL_MS))
            .await;
    };
    let status = match answered {
        Some(status) => {
            close.disarm();
            status
        }
        None => {
            close.close().await;
            match read_card(&**host, &thread_id, &card_id, &run_id).await {
                Ok((Some(card), _))
                    if matches!(card["secretStatus"].as_str(), Some("saved" | "declined")) =>
                {
                    card["secretStatus"].as_str().unwrap_or_default().to_owned()
                }
                _ => "timed_out".to_owned(),
            }
        }
    };
    if status != "saved" {
        return Ok(json!({"status": status, "note": unsaved_note(&status)}));
    }
    if delivery.as_deref() == Some("file") {
        let (card, _) = read_card(&**host, &thread_id, &card_id, &run_id)
            .await
            .unwrap_or((None, None));
        // Only a card that told the user the agent's commands can read the
        // value may write it out.
        if card
            .as_ref()
            .is_none_or(|c| c["secretStatus"] != "saved" || c["delivery"] != "file")
        {
            return Err(store_failure(
                crate::secret_requests::Failure::NotAFileRequest,
            ));
        }
        let store = store
            .as_ref()
            .ok_or_else(|| store_failure(crate::secret_requests::Failure::StoreFailed))?;
        let file = store
            .save_to_file(&thread_id, &card_id, &project_id)
            .map_err(store_failure)?;
        return Ok(json!({"status": status, "file": file}));
    }
    match store.and_then(|store| store.saved_ref(&thread_id, &card_id)) {
        Some(secret_ref) => Ok(json!({"status": status, "secretRef": secret_ref})),
        None => Err(failure(
            "orchestration_error",
            "The user saved the secret, but it could not be read. Ask again with a new clientRequestId.",
        )),
    }
}
