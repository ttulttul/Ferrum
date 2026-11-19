use crate::{
    auth_service, dispatch_orchestration_command, host_rpc, provider_rpc, AppState, RPC_CONTRACTS,
};
use axum::extract::ws::{Message, WebSocket};
use base64::Engine;
use futures_util::{future::BoxFuture, stream::FuturesUnordered, FutureExt, StreamExt};
use hmac::{Hmac, Mac};
use rand::RngCore;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::io::Write;
use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::Arc,
};
use tokio::sync::{broadcast, mpsc, oneshot, Semaphore};

fn asset_signing_key(state: &crate::AppState) -> anyhow::Result<[u8; 32]> {
    let path = state.paths.secrets.join("asset-access-signing-key.bin");
    if !path.exists() {
        let mut key = [0u8; 32];
        rand::rngs::OsRng.fill_bytes(&mut key);
        let mut tmp = tempfile::NamedTempFile::new_in(&state.paths.secrets)?;
        tmp.write_all(&key)?;
        tmp.as_file().sync_all()?;
        match tmp.persist_noclobber(&path) {
            Ok(_) => {}
            Err(error) if error.error.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(error) => return Err(error.into()),
        }
    }
    let bytes = std::fs::read(path)?;
    bytes
        .try_into()
        .map_err(|_| anyhow::anyhow!("invalid asset signing key"))
}

fn sign_asset_payload(state: &crate::AppState, payload: &str) -> anyhow::Result<String> {
    let key = asset_signing_key(state)?;
    let mut mac = Hmac::<Sha256>::new_from_slice(&key).expect("HMAC accepts any key");
    mac.update(payload.as_bytes());
    Ok(base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes()))
}

enum Reply {
    Success(Value),
    Failure(Value),
    Chunk(Value),
}

const PROJECTS_MUTATE_SCHEMA_DEFECT: &str = r#"Expected { readonly "type": "project.create", ... } | { readonly "type": "project.update", ... } | { readonly "type": "project.delete", ... }"#;
const DEVICE_ACTION_SCHEMA_DEFECT: &str = r#"Expected { readonly "type": "setAppearance", ... } | { readonly "type": "setTextSize", ... } | { readonly "type": "setToggle", ... } | { readonly "type": "setLiquidGlass", ... } | { readonly "type": "setColorFilter", ... } | { readonly "type": "setOrientation", ... } | { readonly "type": "setLocation", ... } | { readonly "type": "clearLocation", ... } | { readonly "type": "setPermission", ... } | { readonly "type": "openUrl", ... } | { readonly "type": "launchApp", ... } | { readonly "type": "terminateApp", ... } | { readonly "type": "shake", ... } | { readonly "type": "sendPush", ... }"#;
const ORCHESTRATION_DISPATCH_COMMAND_SCHEMA_DEFECT: &str = r#"Expected { readonly "type": "thread.stop", ... } | { readonly "type": "thread.create", ... } | { readonly "type": "thread.archive", ... } | { readonly "type": "thread.guest-share.close", ... } | { readonly "type": "thread.guest-message.dismiss", ... } | { readonly "type": "thread.unarchive", ... } | { readonly "type": "thread.delete", ... } | { readonly "type": "thread.settle", ... } | { readonly "type": "thread.auto-settle", ... } | { readonly "type": "thread.unsettle", readonly "reason": "user", ... } | { readonly "type": "thread.snooze", ... } | { readonly "type": "thread.unsnooze", readonly "reason": "user", ... } | { readonly "type": "thread.auto-settle.set", ... } | { readonly "type": "thread.pin", ... } | { readonly "type": "thread.unpin", ... } | { readonly "type": "thread.tag.set", ... } | { readonly "type": "thread.pin.reorder", ... } | { readonly "type": "thread.active.reorder", ... } | { readonly "type": "thread.visit", ... } | { readonly "type": "thread.mark-unread", ... } | { readonly "type": "thread.metadata.update", ... } | { readonly "type": "thread.pull-request.link", ... } | { readonly "type": "thread.pull-request.unlink", ... } | { readonly "type": "thread.pull-request-link.sync", ... } | { readonly "type": "thread.issue.link", ... } | { readonly "type": "thread.issue.unlink", ... } | { readonly "type": "thread.pull-request.watch", ... } | { readonly "type": "thread.pull-request.sync", ... } | { readonly "type": "thread.title.regeneration.complete", ... } | { readonly "type": "thread.runtime-mode.set", ... } | { readonly "type": "thread.interaction-mode.set", ... } | { readonly "type": "thread.model-selection.set", ... } | { readonly "type": "provider-session.detach", ... } | { readonly "type": "message.dispatch", ... } | { readonly "type": "prepared-run.release", ... } | { readonly "type": "notification.delivery.accept", ... } | { readonly "type": "prepared-run.progress", ... } | { readonly "type": "prepared-run.fail", ... } | { readonly "type": "prepared-run.retry", ... } | { readonly "type": "subagent.stop", ... } | { readonly "type": "run.interrupt", ... } | { readonly "type": "queued-message.promote-to-steer", ... } | { readonly "type": "queue.resume", ... } | { readonly "type": "queued-run.start", ... } | { readonly "type": "queued-run.reorder", ... } | { readonly "type": "queued-run.cancel", ... } | { readonly "type": "queued-run.edit", ... } | { readonly "type": "runtime-request.respond", ... } | { readonly "type": "thread.user-input.dismiss", ... } | { readonly "type": "checkpoint.rollback", ... } | { readonly "type": "thread.fork", ... } | { readonly "type": "thread.merge_back", ... } | { readonly "type": "delegated_task.request", ... } | { readonly "type": "delegated_task.wake-policy", ... } | { readonly "type": "delegated_task.completion-delivery.acknowledge", ... } | { readonly "type": "delegated_task.completion-delivery.dispose", ... } | { readonly "type": "thread.created.record", ... } | { readonly "type": "provider.switch", ... }"#;

impl Reply {
    fn defect(message: String) -> Self {
        Self::Failure(json!({"_tag":"Die","defect":message}))
    }

    fn frame(self, id: &str) -> Value {
        let exit = match self {
            Self::Success(value) => json!({"_tag":"Success","value":value}),
            Self::Failure(reason) => json!({"_tag":"Failure","cause":[reason]}),
            Self::Chunk(values) => return json!({"_tag":"Chunk","requestId":id,"values":values}),
        };
        json!({"_tag":"Exit","requestId":id,"exit":exit})
    }
}

struct PendingCall {
    interrupt: oneshot::Sender<()>,
    window: Arc<Semaphore>,
    capacity: usize,
}

pub async fn session(
    state: Arc<AppState>,
    mut socket: WebSocket,
    principal: crate::auth_service::Session,
    references: bool,
) {
    let principal = Arc::new(principal);
    state.auth.socket_presence(&principal.id, true);
    let invalidated = state.auth.invalidated(&principal);
    tokio::pin!(invalidated);
    let mut calls = FuturesUnordered::<BoxFuture<'static, (String, Reply)>>::new();
    let mut pending = HashMap::<String, PendingCall>::new();
    let (chunks, mut read_chunks) = mpsc::channel::<(String, Value)>(8);
    loop {
        let response = tokio::select! {
            biased;
            _ = &mut invalidated => break,
            Some((id, values)) = read_chunks.recv() => Reply::Chunk(values).frame(&id),
            Some((id, reply)) = calls.next(), if !calls.is_empty() => {
                pending.remove(&id);
                reply.frame(&id)
            }
            packet = socket.recv() => {
                let bytes = match packet {
                    Some(Ok(Message::Text(text))) => text.into_bytes(),
                    Some(Ok(Message::Binary(bytes))) => bytes,
                    None | Some(Err(_) | Ok(Message::Close(_))) => break,
                    _ => continue,
                };
                let Ok(request) = serde_json::from_slice::<Value>(&bytes) else { continue };
                match request["_tag"].as_str() {
                    Some("Ping") => json!({"_tag":"Pong"}),
                    Some("Ack") => {
                        if let Some(call) = request["requestId"].as_str().and_then(|id|pending.get(id)) {
                            if call.window.available_permits() < call.capacity { call.window.add_permits(1); }
                        }
                        continue
                    }
                    Some("Interrupt") => {
                        if let Some(call) = request["requestId"].as_str().and_then(|id|pending.remove(id)) {
                            let _ = call.interrupt.send(());
                        }
                        continue
                    }
                    Some("Request") => {
                        let Some(id) = request["id"].as_str().map(str::to_owned) else { continue };
                        if pending.contains_key(&id) { continue; }
                        let method = request["tag"].as_str().unwrap_or("undefined").to_owned();
                        let capacity = match method.as_str() {
                            "terminal.observe"|"terminal.attach"|"orchestration.subscribeThread"|"orchestration.subscribeShell"|"subscribeServerConfig" => 8,
                            _ => 1,
                        };
                        let window=Arc::new(Semaphore::new(capacity));
                        let (interrupt, rx) = oneshot::channel();
                        pending.insert(id.clone(), PendingCall{interrupt,window:window.clone(),capacity});
                        let state = state.clone();
                        let principal=principal.clone();
                        let chunks=chunks.clone();
                        calls.push(async move {
                            let reply = tokio::select! {
                                reply = run_call(&state,(&principal,references),&method,&request["payload"], &id, chunks, window) => reply,
                                _ = rx => Reply::Failure(json!({"_tag":"Interrupt"})),
                            };
                            (id, reply)
                        }.boxed());
                        continue
                    }
                    other => json!({"_tag":"Defect","defect":format!("Unknown request tag: {}", other.unwrap_or("undefined"))}),
                }
            }
        };
        if socket
            .send(Message::Text(crate::wire_json::compact(&response)))
            .await
            .is_err()
        {
            break;
        }
    }
    state.auth.socket_presence(&principal.id, false);
}

// Subscribe before reading the initial snapshot so updates during the read are queued.
async fn run_call(
    state: &Arc<AppState>,
    client: (&crate::auth_service::Session, bool),
    method: &str,
    params: &Value,
    id: &str,
    chunks: mpsc::Sender<(String, Value)>,
    window: Arc<Semaphore>,
) -> Reply {
    let (principal, references) = client;
    if method == "projects.mutate"
        && !matches!(
            params.get("type").and_then(Value::as_str),
            Some("project.create" | "project.update" | "project.delete")
        )
    {
        return Reply::defect(PROJECTS_MUTATE_SCHEMA_DEFECT.into());
    }
    if method == "device.action"
        && !matches!(
            params.get("type").and_then(Value::as_str),
            Some(
                "setAppearance"
                    | "setTextSize"
                    | "setToggle"
                    | "setLiquidGlass"
                    | "setColorFilter"
                    | "setOrientation"
                    | "setLocation"
                    | "clearLocation"
                    | "setPermission"
                    | "openUrl"
                    | "launchApp"
                    | "terminateApp"
                    | "shake"
                    | "sendPush"
            )
        )
    {
        return Reply::defect(DEVICE_ACTION_SCHEMA_DEFECT.into());
    }
    if method == "orchestration.dispatchCommand"
        && params
            .get("type")
            .and_then(Value::as_str)
            .and_then(crate::contracts::command_fields)
            .is_none()
    {
        return Reply::defect(ORCHESTRATION_DISPATCH_COMMAND_SCHEMA_DEFECT.into());
    }
    if method == "orchestration.dispatchCommand"
        && params["type"] == "message.dispatch"
        && params.get("dispatchMode").is_some_and(|mode| {
            !mode.is_object()
                || !matches!(
                    mode["type"].as_str(),
                    Some(
                        "defer_start"
                            | "steer_active"
                            | "restart_active"
                            | "queue_after_active"
                            | "start_immediately"
                    )
                )
        })
    {
        return Reply::defect(
            "Expected { readonly \"type\": \"defer_start\", ... } | { readonly \"type\": \"steer_active\", ... } | { readonly \"type\": \"restart_active\", ... } | { readonly \"type\": \"queue_after_active\", ... } | { readonly \"type\": \"start_immediately\", ... }\n  at [\"dispatchMode\"]".into(),
        );
    }
    let decoded;
    let params = if matches!(method, "server.updateSettings" | "server.checkSecretSource") {
        let result = if method == "server.updateSettings" {
            crate::settings::schema::request(params)
        } else {
            crate::settings::schema::secret_check(params)
        };
        decoded = match result {
            Ok(v) => v,
            Err(message) => return Reply::defect(message),
        };
        &decoded
    } else {
        params
    };
    if let Some(contract) = RPC_CONTRACTS.iter().find(|c| c.tag == method) {
        let command_fields = (method == "orchestration.dispatchCommand")
            .then(|| {
                params["type"]
                    .as_str()
                    .and_then(crate::contracts::command_fields)
            })
            .flatten();
        if let Some(key) = command_fields
            .or(contract.required)
            .into_iter()
            .flatten()
            .find(|key| params.get(**key).is_none())
        {
            return Reply::defect(format!("Missing key\n  at [{key:?}]"));
        }
        if let Some(scope) =
            crate::auth_service::permissions::missing_rpc(principal, contract, params)
        {
            let legacy = crate::auth_service::permissions::legacy_scope(scope);
            return Reply::Failure(
                json!({"_tag":"Fail","error":{"_tag":"EnvironmentAuthorizationError","message":format!("The authenticated token is missing required scope: {scope}."),"requiredScope":legacy,"requiredPermission":scope}}),
            );
        }
    }
    if let Some(defect) = checkpoint_diff_schema_defect(method, params) {
        return Reply::defect(defect);
    }
    if method == "server.searchAcpRegistry"
        && params.get("query").is_some_and(|value| !value.is_string())
    {
        return Reply::defect("Expected string\n  at [\"query\"]".into());
    }
    if method == "orchestration.searchThreads"
        && params
            .get("query")
            .and_then(Value::as_str)
            .is_some_and(|query| query.encode_utf16().count() < 2)
    {
        return Reply::defect(
            "Expected a value with a length of at least 2\n  at [\"query\"]".into(),
        );
    }
    if method == "server.searchAcpRegistry"
        && params.get("limit").is_some_and(|value| {
            value
                .as_u64()
                .is_none_or(|number| !(1..=512).contains(&number))
        })
    {
        return Reply::defect("Expected a value between 1 and 512\n  at [\"limit\"]".into());
    }
    if matches!(
        method,
        "server.prepareAcpRegistryAgent" | "server.uninstallAcpRegistryManagedBinary"
    ) && params
        .get("agentId")
        .is_some_and(|value| !value.is_string())
    {
        return Reply::defect("Expected string\n  at [\"agentId\"]".into());
    }
    if method == "server.acceptAcpRegistryUrlAuth"
        && params
            .get("instanceId")
            .is_some_and(|value| !value.is_string())
    {
        return Reply::defect("Expected string\n  at [\"instanceId\"]".into());
    }
    if method == "provider.auth.start"
        && params
            .get("callbackMode")
            .is_some_and(|value| !matches!(value.as_str(), Some("server" | "client")))
    {
        return Reply::defect("Expected \"server\" | \"client\"\n  at [\"callbackMode\"]".into());
    }
    if method == "mcpApps.updateModelContext"
        && params
            .get("content")
            .is_some_and(|value| !(value.is_array() || value.is_null()))
    {
        return Reply::defect("Expected array | null\n  at [\"content\"]".into());
    }
    if let Some(field) = invalid_provider_payload_field(method, params) {
        return Reply::defect(format!("Expected string\n  at [\"{field}\"]"));
    }
    if method == "provider.auth.start"
        && params
            .get("methodId")
            .is_some_and(|value| !value.is_string())
    {
        return Reply::defect("Expected string\n  at [\"methodId\"]".into());
    }
    if method == "provider.auth.start"
        && params
            .get("returnUrl")
            .is_some_and(|value| !value.is_string())
    {
        return Reply::defect("Expected string\n  at [\"returnUrl\"]".into());
    }
    if method == "mcpApps.callTool"
        && params
            .get("arguments")
            .is_some_and(|value| !value.is_object())
    {
        return Reply::defect("Expected object\n  at [\"arguments\"]".into());
    }
    if method == "provider.auth.respond"
        && params
            .get("response")
            .is_some_and(|value| !value.is_object())
    {
        return Reply::defect("Expected { readonly \"type\": \"browser\", ... } | { readonly \"type\": \"terminal\", ... } | { readonly \"type\": \"credentials\", ... }\n  at [\"response\"]".into());
    }
    if method == "server.checkSecretSource" {
        let settings = state.settings.internal().unwrap_or_default();
        let source = &params["source"];
        let answer = state
            .settings
            .resolver
            .resolve(&settings, source, crate::settings::secrets::ReadMode::Check)
            .await;
        let result = if answer.is_some() {
            json!({"_tag":"Resolved"})
        } else {
            let label = crate::settings::secrets::provider_label(&settings, source);
            json!({"_tag":"Failed","detail":format!("Could not read this secret from \"{label}\". Check the provider's settings and that its store is unlocked.")})
        };
        return Reply::Success(result);
    }
    if matches!(method, "server.getSettings" | "server.updateSettings") {
        let reply = if method == "server.getSettings" {
            let result = state.settings.client(references);
            match result {
                Ok(mut settings) => {
                    let rows = state.store.read().await.project_rows.clone();
                    fold_project_rows_into_settings(&mut settings, &rows);
                    Ok(settings)
                }
                Err(error) => Err(error),
            }
        } else {
            let mut input = params.clone();
            if let Some(hosts) = input
                .get_mut("patch")
                .and_then(|patch| patch.get_mut("deviceHosts"))
                .and_then(Value::as_array_mut)
            {
                crate::local_services::local_ssh::retain_remote(hosts).await;
            }
            state.settings.update(&input, references)
        };
        return match reply {
            Ok(value) => Reply::Success(value),
            Err(error) => Reply::Failure(json!({"_tag":"Fail","error":error})),
        };
    }
    let send = |values| async {
        if let Ok(permit) = window.acquire().await {
            permit.forget();
            let _ = chunks.send((id.to_owned(), values)).await;
        }
    };
    if method == "pullRequests.subscribeLinkedThreadRefreshes" {
        let mut events = state.pull_request_linked_events.subscribe();
        loop {
            match events.recv().await {
                Ok(revision) => send(json!([revision])).await,
                Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                    return Reply::Success(Value::Null)
                }
            }
        }
    }
    if method == "orchestration.subscribePromptSuggestion" {
        let thread_id = params["threadId"].as_str().unwrap_or_default().to_owned();
        let mut events = state.prompt_suggestion_events.subscribe();
        send(json!([Value::Null])).await;
        loop {
            match events.recv().await {
                Ok(event) if event["threadId"].as_str() == Some(thread_id.as_str()) => {
                    send(json!([{"runId":event["runId"],"text":event["text"]}])).await;
                }
                Ok(_) => {}
                Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                    return Reply::Success(Value::Null)
                }
            }
        }
    }
    if method == "scheduledTasks.subscribe" {
        let mut events = state.scheduled_task_events.subscribe();
        send(json!([scheduled_tasks_snapshot(state).await])).await;
        loop {
            match events.recv().await {
                Ok(()) => send(json!([scheduled_tasks_snapshot(state).await])).await,
                Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                    return Reply::Success(Value::Null)
                }
            }
        }
    }
    if method == "server.getConfig" {
        return match crate::config_rpc::snapshot(state, references, &Value::Null).await {
            Ok(v) => Reply::Success(v),
            Err(e) => Reply::Failure(json!({"_tag":"Fail","error":e})),
        };
    }
    if method == "subscribeServerConfig" {
        let mut sub = match crate::config_rpc::Subscription::open(state, references, params).await {
            Ok(sub) => sub,
            Err(e) => return Reply::Failure(json!({"_tag":"Fail","error":e})),
        };
        send(json!([{"version":1,"type":"snapshot","config":sub.config}])).await;
        loop {
            match sub.next(state, references, params).await {
                Ok(event) => send(json!([event])).await,
                Err(e) => return Reply::Failure(json!({"_tag":"Fail","error":e})),
            }
        }
    }
    if method == "subscribeAuthAccess" {
        let (mut change, mut changes) = state.auth.subscribe_access();
        let mut revision = 1;
        loop {
            send(json!([change.event(&principal.id, revision)])).await;
            let Some(next) = changes.recv().await else {
                return Reply::Success(Value::Null);
            };
            change = next;
            revision += 1;
        }
    }
    if method == "git.runStackedAction" {
        let (tx, mut events) = mpsc::channel(1);
        let workflow = crate::local_services::git_workflow::stream(state, params, tx);
        let forward = async {
            while let Some(batch) = events.recv().await {
                send(batch).await;
            }
        };
        let (result, ()) = tokio::join!(workflow, forward);
        // The last frame is queued on the connection. Let its writer flush it before Exit.
        tokio::task::yield_now().await;
        return match result {
            Ok(value) => Reply::Success(value),
            Err(e) if e["_tag"] == "Die" => Reply::Failure(e),
            Err(e) => Reply::Failure(json!({"_tag":"Fail","error":e})),
        };
    }
    if matches!(
        method,
        "subscribeVcsBranch" | "subscribeVcsStatus" | "subscribeDiscoveredLocalServers"
    ) {
        let mut previous: Option<Value> = None;
        loop {
            let snapshot = match host_rpc::polled_stream(state, method, params).await {
                Some(Ok(v)) => v,
                Some(Err(error)) if error["_tag"] == "Die" => return Reply::Failure(error),
                Some(Err(error)) => return Reply::Failure(json!({"_tag":"Fail","error":error})),
                None => return Reply::defect(format!("Unknown stream: {method}")),
            };
            match (method, previous.as_ref()) {
                ("subscribeVcsStatus", None) => {
                    let mut initial = snapshot.clone();
                    initial["remote"] = Value::Null;
                    send(json!([initial])).await;
                    if params["includeRemote"] != false && snapshot["local"]["isRepo"] == true {
                        send(json!([{"_tag":"remoteUpdated","remote":snapshot["remote"]}])).await;
                    }
                }
                ("subscribeVcsStatus", Some(old)) => {
                    if old["local"] != snapshot["local"] {
                        send(json!([{"_tag":"localUpdated","local":snapshot["local"]}])).await;
                    }
                    if params["includeRemote"] != false && old["remote"] != snapshot["remote"] {
                        send(json!([{"_tag":"remoteUpdated","remote":snapshot["remote"]}])).await;
                    }
                }
                ("subscribeDiscoveredLocalServers", Some(old))
                    if old["servers"] == snapshot["servers"] => {}
                (_, Some(old)) if old == &snapshot => {}
                _ => send(json!([snapshot])).await,
            }
            previous = Some(snapshot);
            tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        }
    }
    let terminal = matches!(method, "terminal.observe" | "terminal.attach");
    let mut events = match method {
        "terminal.observe" | "terminal.attach" => Some(state.terminals.subscribe()),
        "subscribeTerminalMetadata" => Some(state.terminals.subscribe_metadata()),
        "subscribePreviewEvents" => Some(state.previews.subscribe()),
        "subscribeDeviceState" => Some(state.devices.subscribe()),
        _ => None,
    };
    let initial = match method {
        "subscribeTerminalMetadata" => {
            Some(Reply::Chunk(json!([state.terminals.metadata_snapshot()])))
        }
        "subscribePreviewEvents" => None,
        "subscribeDeviceState" => Some(Reply::Chunk(json!([state
            .devices
            .snapshot(&state.data_dir)]))),
        _ => Some(dispatch(state, method, params).await),
    };
    let mut last_seq = None;
    match initial {
        None => {}
        Some(Reply::Success(initial)) if terminal => {
            last_seq = initial["snapshot"]["sequence"].as_u64();
            send(json!([initial])).await;
        }
        Some(Reply::Chunk(values)) => send(values).await,
        Some(reply) => return reply,
    }
    let Some(events) = events.as_mut() else {
        return std::future::pending().await;
    };
    loop {
        let event = match events.recv().await {
            Ok(event) => event,
            Err(broadcast::error::RecvError::Closed) => return Reply::Success(Value::Null),
            Err(broadcast::error::RecvError::Lagged(_)) if terminal => {
                match state.terminals.call("observe", params).await {
                    Some(Ok(snapshot)) => snapshot,
                    Some(Err(error)) => {
                        return Reply::Failure(json!({"_tag":"Fail","error":error}))
                    }
                    None => return Reply::defect("Could not resynchronize terminal".into()),
                }
            }
            Err(broadcast::error::RecvError::Lagged(_)) => {
                return Reply::defect("Subscription fell behind.".into())
            }
        };
        if terminal {
            let data = event.get("snapshot").unwrap_or(&event);
            if data["threadId"] != params["threadId"] || data["terminalId"] != params["terminalId"]
            {
                continue;
            }
            let seq = data["sequence"].as_u64();
            if event["type"] != "snapshot" && seq <= last_seq {
                continue;
            }
            if event["type"] == "snapshot" && seq == last_seq {
                continue;
            }
            last_seq = seq;
        }
        send(json!([event])).await;
    }
}

async fn dispatch(state: &Arc<AppState>, method: &str, params: &Value) -> Reply {
    if !crate::rpc_known(method) {
        return Reply::defect(format!("Unknown request tag: {method}"));
    }
    if method == "projects.mutate" {
        return match crate::projects::mutate(state, params).await {
            Ok(row) => Reply::Success(row),
            Err(e) => Reply::Failure(json!({"_tag":"Fail","error":e})),
        };
    }
    if method == "projects.ensureScratch" {
        let root = state.paths.base_dir.join("scratch");
        if let Err(error) = tokio::fs::create_dir_all(&root).await {
            return Reply::Failure(
                json!({"_tag":"Fail","error":{"_tag":"OrchestrationDispatchCommandError","message":error.to_string()}}),
            );
        }
        let root = match tokio::fs::canonicalize(&root).await {
            Ok(path) => path,
            Err(error) => {
                return Reply::Failure(
                    json!({"_tag":"Fail","error":{"_tag":"OrchestrationDispatchCommandError","message":error.to_string()}}),
                );
            }
        };
        let existing = state
            .store
            .read()
            .await
            .projects
            .values()
            .find(|p| std::fs::canonicalize(&p.path).ok().as_deref() == Some(root.as_path()))
            .cloned();
        let project_id = if let Some(project) = existing {
            project.id
        } else {
            let id = uuid::Uuid::new_v4().to_string();
            let project = crate::Project {
                id: id.clone(),
                name: "No project".into(),
                path: root.to_string_lossy().into_owned(),
                created_at: crate::now(),
            };
            let mut row = crate::projects::wire(&project);
            row["projectIcon"] =
                json!({"kind":"lucide","name":"message-square-dashed","color":"gray"});
            if state
                .commit(crate::Event::ProjectCommitted {
                    project,
                    row,
                    command_id: format!("scratch-project:{id}"),
                    command_type: "project.create".into(),
                    removed_threads: vec![],
                })
                .await
                .is_err()
            {
                return Reply::Failure(
                    json!({"_tag":"Fail","error":{"_tag":"OrchestrationDispatchCommandError","message":"Failed to create scratch project."}}),
                );
            }
            id
        };
        return Reply::Success(json!({"projectId":project_id}));
    }
    if method == "projects.createNew" {
        let name = params["name"].as_str().unwrap_or_default().trim();
        if name.is_empty() {
            return Reply::defect("Expected a non-blank string\n  at [\"name\"]".into());
        }
        let root = state.paths.base_dir.join("projects");
        if let Err(error) = tokio::fs::create_dir_all(&root).await {
            return Reply::Failure(
                json!({"_tag":"Fail","error":{"_tag":"OrchestrationDispatchCommandError","message":error.to_string()}}),
            );
        }
        let slug: String = name
            .chars()
            .map(|c| {
                if c.is_ascii_alphanumeric() {
                    c.to_ascii_lowercase()
                } else {
                    '-'
                }
            })
            .collect();
        let slug = slug.trim_matches('-').to_owned();
        let mut folder = root.join(if slug.is_empty() { "project" } else { &slug });
        for attempt in 2..=100 {
            if tokio::fs::metadata(&folder).await.is_err() {
                break;
            }
            folder = root.join(format!(
                "{}-{attempt}",
                if slug.is_empty() { "project" } else { &slug }
            ));
        }
        if let Err(error) = tokio::fs::create_dir_all(&folder).await {
            return Reply::Failure(
                json!({"_tag":"Fail","error":{"_tag":"OrchestrationDispatchCommandError","message":error.to_string()}}),
            );
        }
        let _ = tokio::fs::create_dir_all(folder.join("assets")).await;
        let _ = tokio::fs::write(folder.join("README.md"), format!("# {name}\n")).await;
        let _ = tokio::fs::write(
            folder.join("assets/icon.svg"),
            "<svg xmlns=\"http://www.w3.org/2000/svg\"/>\n",
        )
        .await;
        let commit_error = match tokio::process::Command::new("git")
            .current_dir(&folder)
            .args(["init", "--initial-branch=main"])
            .output()
            .await
        {
            Ok(init) if init.status.success() => {
                let _ = tokio::process::Command::new("git")
                    .current_dir(&folder)
                    .args(["add", "--force", "README.md", "assets/icon.svg"])
                    .output()
                    .await;
                match tokio::process::Command::new("git")
                    .current_dir(&folder)
                    .args(["commit", "--message", "Initial commit"])
                    .output()
                    .await
                {
                    Ok(out) if out.status.success() => None,
                    Ok(out) => Some(
                        if String::from_utf8_lossy(&out.stderr).contains("Author identity unknown")
                        {
                            "Git has no name or email on this machine. Set user.name and user.email, then commit.".into()
                        } else {
                            String::from_utf8_lossy(&out.stderr).trim().to_owned()
                        },
                    ),
                    Err(error) => Some(error.to_string()),
                }
            }
            Ok(out) => Some(String::from_utf8_lossy(&out.stderr).trim().to_owned()),
            Err(error) => Some(error.to_string()),
        };
        let id = uuid::Uuid::new_v4().to_string();
        let project = crate::Project {
            id: id.clone(),
            name: name.into(),
            path: folder.to_string_lossy().into_owned(),
            created_at: crate::now(),
        };
        let row = crate::projects::wire(&project);
        if state
            .commit(crate::Event::ProjectCommitted {
                project,
                row,
                command_id: format!("named-project:{id}"),
                command_type: "project.create".into(),
                removed_threads: vec![],
            })
            .await
            .is_err()
        {
            return Reply::Failure(
                json!({"_tag":"Fail","error":{"_tag":"OrchestrationDispatchCommandError","message":"Failed to create project."}}),
            );
        }
        let mut result = json!({"projectId":id,"workspaceRoot":folder});
        if let Some(error) = commit_error.filter(|e| !e.is_empty()) {
            result["commitError"] = json!(error);
        }
        return Reply::Success(result);
    }
    if method == "projects.createFromThread" {
        let id = params["threadId"].as_str().unwrap_or_default();
        if !state.store.read().await.threads.contains_key(id) {
            return Reply::Failure(
                json!({"_tag":"Fail","error":{"_tag":"OrchestrationDispatchCommandError","message":"Thread not found.","cause":{"name":"ScratchThreadPromotionError","message":"Thread not found."}}}),
            );
        }
        return Reply::Failure(
            json!({"_tag":"Fail","error":{"_tag":"OrchestrationDispatchCommandError","message":"Thread promotion is unavailable."}}),
        );
    }
    if method == "agentSessions.import" {
        let id = params["projectId"].as_str().unwrap_or_default();
        if !state.store.read().await.projects.contains_key(id) {
            return Reply::Failure(
                json!({"_tag":"Fail","error":{"_tag":"AgentSessionImportProjectNotFoundError","projectId":id}}),
            );
        }
        return Reply::Failure(
            json!({"_tag":"Fail","error":{"_tag":"AgentSessionImportError","message":"No importable agent sessions were found."}}),
        );
    }
    if method == "projectClone.cancel" || method == "projectClone.retry" {
        return Reply::Success(json!({"applied":false}));
    }
    if method == "orchestration.searchThreadStream" {
        let thread_id = params["threadId"].as_str().unwrap_or_default();
        if !state.store.read().await.threads.contains_key(thread_id) {
            return Reply::Failure(json!({"_tag":"Fail","error":{
                "_tag":"OrchestrationV2SearchThreadError",
                "cause":{"name":"OrchestratorProjectionError",
                    "message":format!("Failed to load orchestration projection for thread {thread_id}."),
                    "cause":{"name":"ProjectionStoreThreadNotFoundError",
                        "message":format!("No orchestration projection exists for thread {thread_id}.")}}
            }}));
        }
        let sequence = *state.next_event_id.lock().await;
        return Reply::Chunk(json!([{"complete":true,"snapshotSequence":sequence,
            "totalMatches":0,"activeIndex":0,"match":null}]));
    }
    if method == "server.updateProvider" {
        return Reply::Failure(
            json!({"_tag":"Fail","error":{"_tag":"ServerProviderUpdateError","provider":params["provider"],"reason":"This provider does not support one-click updates."}}),
        );
    }
    if matches!(
        method,
        "server.updateServer" | "server.updateServerWithProgress"
    ) {
        return Reply::Failure(
            json!({"_tag":"Fail","error":{"_tag":"ServerSelfUpdateError","reason":"This server is managed by the T3 Code desktop app on its machine; update the desktop app to update it."}}),
        );
    }
    if method == "server.commitDesktopUpdate" {
        return Reply::Failure(
            json!({"_tag":"Fail","error":{"_tag":"ServerSelfUpdateError","reason":"This server is managed by the T3 Code desktop app on its machine; update the desktop app to update it."}}),
        );
    }
    if method == "peerLinks.unlink" {
        return Reply::Success(json!({"removed":false}));
    }
    if method == "scheduledTasks.upsert" {
        return scheduled_task_upsert(state, params).await;
    }
    if method == "scheduledTasks.setEnabled" {
        return scheduled_task_set_enabled(state, params).await;
    }
    if method == "scheduledTasks.runNow" {
        return scheduled_task_run_now(state, params).await;
    }
    if method == "scheduledTasks.rotateWebhookToken" {
        return scheduled_task_rotate_token(state, params).await;
    }
    if method == "scheduledTasks.delete" {
        state
            .scheduled_tasks
            .lock()
            .await
            .shift_remove(params["id"].as_str().unwrap_or_default());
        let _ = state.persist_scheduled_tasks().await;
        let _ = state.scheduled_task_events.send(());
        return Reply::Success(json!({"id":params["id"]}));
    }
    if method == "sourceControl.lookupRepository" {
        let provider = params["provider"].as_str().unwrap_or_default();
        return Reply::Failure(
            json!({"_tag":"Fail","error":{"_tag":"SourceControlRepositoryError","provider":provider,"operation":"lookupRepository","detail":"The source control operation could not be completed.","cause":{"name":"SourceControlProviderError","message":format!("Source control provider {provider} failed in getRepositoryCloneUrls: No {provider} source control provider is registered.")}}}),
        );
    }
    if method == "sourceControl.appAccess" {
        let provider = params["provider"].as_str().unwrap_or_default();
        return Reply::Failure(
            json!({"_tag":"Fail","error":{"_tag":"SourceControlRepositoryError","provider":provider,"operation":"appAccess","detail":"The source control operation could not be completed.","cause":{"name":"SourceControlProviderError","message":format!("No {provider} source control provider is registered.")}}}),
        );
    }
    if method == "sourceControl.cloneRepository" || method == "sourceControl.publishRepository" {
        let provider = params["provider"].as_str().unwrap_or_default();
        return Reply::Failure(
            json!({"_tag":"Fail","error":{"_tag":"SourceControlRepositoryError","provider":provider,"operation":if method.ends_with("cloneRepository"){"cloneRepository"}else{"publishRepository"},"detail":"The source control operation could not be completed.","cause":{"name":"SourceControlProviderError","message":format!("No {provider} source control provider is registered.")}}}),
        );
    }
    if method == "server.getNpmRelease" {
        let spec = params["spec"].as_str().unwrap_or_default();
        if !matches!(spec, "latest" | "nightly")
            && !spec
                .split_once('.')
                .is_some_and(|(a, b)| a.parse::<u64>().is_ok() && b.contains('.'))
        {
            return Reply::defect("Expected a string matching the RegExp ^(?:latest|nightly|\\d+\\.\\d+\\.\\d+(?:-[0-9A-Za-z.-]+)?)$\n  at [\"spec\"]".into());
        }
        let release = match spec {
            "latest" | "0.1.10" => json!({"version":"0.1.10","databaseMigration":96}),
            "nightly" => json!({"version":"0.1.11-nightly.20261119.4918","databaseMigration":98}),
            _ => json!({"version":spec}),
        };
        return Reply::Success(release);
    }
    if method == "server.searchAcpRegistry" {
        let query = params["query"].as_str().unwrap_or_default().to_lowercase();
        let limit = params["limit"].as_u64().unwrap_or(512).min(512) as usize;
        let agents: Vec<Value> =
            serde_json::from_str::<Vec<Value>>(include_str!("../contracts/acp-registry.json"))
                .unwrap_or_default()
                .into_iter()
                .filter(|agent| {
                    query.is_empty()
                        || agent["id"]
                            .as_str()
                            .unwrap_or_default()
                            .to_lowercase()
                            .contains(&query)
                        || agent["name"]
                            .as_str()
                            .unwrap_or_default()
                            .to_lowercase()
                            .contains(&query)
                        || agent["description"]
                            .as_str()
                            .unwrap_or_default()
                            .to_lowercase()
                            .contains(&query)
                })
                .take(limit)
                .collect();
        return Reply::Success(json!({"agents":agents}));
    }
    if method == "server.prepareAcpRegistryAgent" {
        let agent = params["agentId"].as_str().unwrap_or_default();
        return Reply::Failure(
            json!({"_tag":"Fail","error":{"_tag":"AcpRegistryOperationError","reason":"agent_not_found","message":format!("ACP Registry does not contain agent '{}'.",agent),"cause":{"name":"AcpRegistryError","message":format!("ACP Registry does not contain agent '{}'.",agent)}}}),
        );
    }
    if method == "server.uninstallAcpRegistryManagedBinary" {
        let agent = params["agentId"].as_str().unwrap_or_default();
        return Reply::Success(json!({"agentId":agent,"removed":false}));
    }
    if method == "server.acceptAcpRegistryUrlAuth" {
        return Reply::Success(json!({"accepted":false}));
    }
    if matches!(
        method,
        "server.listAcpRegistrySessions"
            | "server.importAcpRegistrySession"
            | "server.deleteAcpRegistrySession"
            | "server.listAcpRegistryProviders"
            | "server.setAcpRegistryProvider"
            | "server.disableAcpRegistryProvider"
    ) {
        let project_id = params["projectId"].as_str().unwrap_or_default();
        if !state.store.read().await.projects.contains_key(project_id) {
            return Reply::Failure(
                json!({"_tag":"Fail","error":{"_tag":"AcpRegistryOperationError","reason":"project_not_found","message":format!("Project {} was not found.",project_id)}}),
            );
        }
    }
    if method == "server.logoutAcpRegistry" {
        let instance_id = params["instanceId"].as_str().unwrap_or_default();
        let known = state
            .settings
            .internal()
            .ok()
            .and_then(|settings| settings["providerInstances"].get(instance_id).cloned())
            .is_some();
        if !known {
            return Reply::Failure(
                json!({"_tag":"Fail","error":{"_tag":"AcpRegistryOperationError","reason":"instance_not_found","message":format!("Provider instance {} was not found.",instance_id)}}),
            );
        }
        return Reply::Failure(
            json!({"_tag":"Fail","error":{"_tag":"AcpRegistryOperationError","reason":"logout_unsupported","message":"The ACP agent does not advertise logout."}}),
        );
    }
    if method == "server.listTextGeneratorModels" {
        return Reply::Failure(
            json!({"_tag":"Fail","error":{"_tag":"TextGeneratorError","detail":"That text generator no longer exists."}}),
        );
    }
    if method == "server.getProcessResourceHistory" {
        let now = time::OffsetDateTime::now_utc();
        let read_at = now
            .format(time::macros::format_description!(
                "[year]-[month]-[day]T[hour]:[minute]:[second].[subsecond digits:3]Z"
            ))
            .unwrap_or_default();
        let window = params["windowMs"].as_u64().unwrap_or(0).max(1_000);
        let bucket = params["bucketMs"].as_u64().unwrap_or(0).max(1_000);
        let start = (now - time::Duration::milliseconds(window as i64))
            .format(time::macros::format_description!(
                "[year]-[month]-[day]T[hour]:[minute]:[second].[subsecond digits:3]Z"
            ))
            .unwrap_or_default();
        return Reply::Success(
            json!({"readAt":read_at,"windowMs":window,"bucketMs":bucket,"sampleIntervalMs":0,"retainedSampleCount":0,"totalCpuSecondsApprox":0,"buckets":[{"startedAt":start,"endedAt":read_at,"avgCpuPercent":0,"maxCpuPercent":0,"maxRssBytes":0,"maxProcessCount":0}],"topProcesses":[],"error":{"_tag":"Some","value":{"failureTag":"ProcessDiagnosticsQueryFailedError","message":"Resource monitor binary was not found for linux/x64."}}}),
        );
    }
    if method == "server.getResourceTelemetryHistory" {
        let now = time::OffsetDateTime::now_utc();
        let fmt = time::macros::format_description!(
            "[year]-[month]-[day]T[hour]:[minute]:[second].[subsecond digits:3]Z"
        );
        let read_at = now.format(fmt).unwrap_or_default();
        let window = params["windowMs"].as_u64().unwrap_or(0).max(1_000);
        let bucket = params["bucketMs"].as_u64().unwrap_or(0).max(1_000);
        let started = (now - time::Duration::milliseconds(window as i64))
            .format(fmt)
            .unwrap_or_default();
        return Reply::Success(
            json!({"readAt":read_at,"windowMs":window,"bucketMs":bucket,"sampleIntervalMs":0,"retainedSampleCount":0,"buckets":[{"startedAt":started,"endedAt":read_at,"avgCpuPercent":0,"maxCpuPercent":0,"maxRssBytes":0,"ioReadBytes":0,"ioWriteBytes":0,"maxProcessCount":0}],"topProcesses":[],"health":{"native":{"status":"unavailable","lastSampleAt":{"_tag":"None"},"lastError":{"_tag":"Some","value":"Resource monitor binary was not found for linux/x64."}},"desktop":{"status":"unavailable","lastSampleAt":{"_tag":"None"},"lastError":{"_tag":"Some","value":"Desktop telemetry descriptor is unavailable in 'desktop' mode."}},"sidecarVersion":{"_tag":"None"},"sidecarPid":{"_tag":"None"},"restartCount":0,"collectionDurationMicros":0,"scannedProcessCount":0,"retainedProcessCount":0,"inaccessibleProcessCount":0}}),
        );
    }
    if method == "server.checkOtlpEndpoint"
        && !matches!(
            params["signal"].as_str(),
            Some("traces" | "metrics" | "logs")
        )
    {
        return Reply::defect(
            "Expected \"traces\" | \"metrics\" | \"logs\"\n  at [\"signal\"]".into(),
        );
    }
    if method == "server.prepareTriage" && !params["client"].is_object() {
        return Reply::defect("Expected object\n  at [\"client\"]".into());
    }
    if method == "server.signalProcess"
        && !matches!(params["signal"].as_str(), Some("SIGINT" | "SIGKILL"))
    {
        return Reply::defect("Expected \"SIGINT\" | \"SIGKILL\"\n  at [\"signal\"]".into());
    }
    if method == "server.reportClientActivity"
        && !matches!(
            params["clientKind"].as_str(),
            Some("web" | "desktop-renderer" | "mobile" | "unknown")
        )
    {
        return Reply::defect("Expected \"web\" | \"desktop-renderer\" | \"mobile\" | \"unknown\"\n  at [\"clientKind\"]".into());
    }
    if method == "server.getUsageSummary" && !params["sinceDay"].is_string() {
        return Reply::defect("Expected string\n  at [\"sinceDay\"]".into());
    }
    if method == "pullRequests.filterOptions"
        && !matches!(
            params["kind"].as_str(),
            Some("labels" | "authors" | "references")
        )
    {
        return Reply::defect(
            "Expected \"labels\" | \"authors\" | \"references\"\n  at [\"kind\"]".into(),
        );
    }
    if method == "scheduledTasks.upsert" && !params["schedule"].is_object() {
        return Reply::defect("Expected { readonly \"type\": \"interval\", ... } | { readonly \"type\": \"fixed_time\", ... } | { readonly \"type\": \"once\", ... } | { readonly \"type\": \"webhook\", ... }\n  at [\"schedule\"]".into());
    }
    if method == "provider.consumeResetCredit"
        && params.get("sourceId").is_none()
        && params.get("instanceId").is_some()
    {
        let instance_id = params["instanceId"].as_str().unwrap_or_default();
        let known = state
            .settings
            .internal()
            .ok()
            .and_then(|settings| settings["providerInstances"].get(instance_id).cloned())
            .is_some()
            || serde_json::from_str::<Value>(include_str!("../contracts/config-defaults.json"))
                .ok()
                .and_then(|defaults| defaults["providers"].get(instance_id).cloned())
                .is_some();
        if !known {
            return Reply::Failure(
                json!({"_tag":"Fail","error":{"_tag":"ProviderSetupError","instanceId":instance_id,"operation":"consume-reset-credit","detail":"Provider instance not found."}}),
            );
        }
        return Reply::Failure(
            json!({"_tag":"Fail","error":{"_tag":"ProviderSetupError","instanceId":instance_id,"operation":"consume-reset-credit","detail":"This provider does not bank reset credits."}}),
        );
    }
    // Provider authentication and managed-installation controllers are backed by
    // provider-specific runtimes in the TypeScript server.  Preserve their exact
    // setup error for instance ids that are no longer present in settings.
    if method.starts_with("provider.auth.")
        || method.starts_with("provider.install.")
        || matches!(
            method,
            "provider.chatgpt.reconnect-profile"
                | "provider.chatgpt.import-profile"
                | "provider.chatgpt.handoff.subscribe"
        )
    {
        let instance_id = params["instanceId"].as_str().unwrap_or_default();
        let known = state
            .settings
            .internal()
            .ok()
            .and_then(|settings| settings["providerInstances"].get(instance_id).cloned())
            .is_some()
            || serde_json::from_str::<Value>(include_str!("../contracts/config-defaults.json"))
                .ok()
                .and_then(|defaults| defaults["providers"].get(instance_id).cloned())
                .is_some();
        if !known {
            let operation = match method {
                "provider.auth.start" => "start",
                "provider.auth.complete" => "complete",
                "provider.auth.respond" => "respond",
                "provider.auth.cancel" => "cancel",
                "provider.auth.removeMethod" => "removeMethod",
                "provider.auth.logout" => "logout",
                "provider.auth.subscribe" => "subscribe",
                "provider.chatgpt.reconnect-profile" => "export",
                "provider.chatgpt.import-profile" => "import",
                "provider.chatgpt.handoff.subscribe" => "handoff",
                "provider.install.start" => "install",
                "provider.install.subscribe" => "observe-install",
                "provider.install.cancel" => "cancel-install",
                "provider.install.remove" => "remove-install",
                _ => "unknown",
            };
            let detail = if method.starts_with("provider.install.") {
                "Managed installation is not available for this provider instance."
            } else {
                "This provider instance is no longer available."
            };
            return Reply::Failure(json!({"_tag":"Fail","error":{
                "_tag":"ProviderSetupError","instanceId":instance_id,
                "operation":operation,"detail":detail
            }}));
        }

        let driver = state
            .settings
            .internal()
            .ok()
            .and_then(|settings| {
                settings["providerInstances"][instance_id]["driver"]
                    .as_str()
                    .map(str::to_owned)
            })
            .unwrap_or_else(|| instance_id.to_owned());
        if method.starts_with("provider.install.") {
            if driver != "antigravity" && driver != "codex" {
                let operation = if method == "provider.install.subscribe" {
                    "observe-install"
                } else if method == "provider.install.cancel" {
                    "cancel-install"
                } else if method == "provider.install.remove" {
                    "remove-install"
                } else {
                    "install"
                };
                return Reply::Failure(
                    json!({"_tag":"Fail","error":{"_tag":"ProviderSetupError","instanceId":instance_id,"operation":operation,"detail":"Managed installation is not available for this provider instance."}}),
                );
            }
            if driver == "codex" {
                let operation = if method == "provider.install.subscribe" {
                    "observe-install"
                } else if method == "provider.install.cancel" {
                    "cancel-install"
                } else if method == "provider.install.remove" {
                    "remove-install"
                } else {
                    "install"
                };
                return Reply::Failure(
                    json!({"_tag":"Fail","error":{"_tag":"ProviderSetupError","instanceId":instance_id,"operation":operation,"detail":"Choose managed setup to install Codex in T3 Code."}}),
                );
            }
            if method == "provider.install.subscribe" {
                return Reply::Chunk(
                    json!([{"driver":"antigravity","operationId":null,"phase":"idle","downloadedBytes":0,"totalBytes":null,"version":null,"installedVersion":null,"canRemove":false,"message":null}]),
                );
            }
            if method == "provider.install.cancel" {
                return Reply::Failure(
                    json!({"_tag":"Fail","error":{"_tag":"ProviderSetupError","instanceId":instance_id,"operation":"cancel","detail":"This installation is no longer current. Refresh its status before cancelling."}}),
                );
            }
            if method == "provider.install.start" {
                return Reply::Success(
                    json!({"driver":"antigravity","operationId":uuid::Uuid::new_v4().to_string(),"phase":"downloading","downloadedBytes":0,"totalBytes":null,"version":"1.5.12","installedVersion":null,"canRemove":false,"message":"Downloading Google's official Antigravity runtime."}),
                );
            }
        } else if method == "provider.auth.subscribe" {
            if driver == "antigravity" {
                return Reply::Chunk(
                    json!([{"instanceId":instance_id,"phase":"idle","flowId":null,"authorizationUrl":null,"expiresAt":null,"message":null,"interaction":null,"credentialOwner":"provider"}]),
                );
            }
            if driver == "cursor" {
                return Reply::Chunk(
                    json!([{"instanceId":instance_id,"phase":"idle","flowId":null,"authorizationUrl":null,"expiresAt":null,"message":null,"methods":[{"id":"browser","name":"Sign in with Cursor","description":null,"type":"agent"}],"interaction":null,"credentialOwner":"t3"}]),
                );
            }
            return Reply::Failure(
                json!({"_tag":"Fail","error":{"_tag":"ProviderSetupError","instanceId":instance_id,"operation":"subscribe","detail":"This provider does not support sign-in in T3 Code."}}),
            );
        } else if method.starts_with("provider.auth.") {
            let detail = match (driver.as_str(), method) {
                ("antigravity", "provider.auth.respond") | ("cursor", "provider.auth.respond") => {
                    "This provider does not accept this sign-in interaction."
                }
                ("antigravity", "provider.auth.removeMethod")
                | ("cursor", "provider.auth.removeMethod") => {
                    "This provider does not support removing saved sign-in methods."
                }
                ("antigravity", "provider.auth.complete")
                | ("cursor", "provider.auth.complete") => {
                    if driver == "cursor" {
                        "Finish signing in on the Cursor website. No redirect URL is needed."
                    } else {
                        "This sign-in is no longer active in this client."
                    }
                }
                ("antigravity", "provider.auth.cancel") | ("cursor", "provider.auth.cancel") => {
                    "This sign-in is no longer active in this client."
                }
                _ => "This provider does not support sign-in in T3 Code.",
            };
            let operation = method.strip_prefix("provider.auth.").unwrap_or("unknown");
            return Reply::Failure(
                json!({"_tag":"Fail","error":{"_tag":"ProviderSetupError","instanceId":instance_id,"operation":operation,"detail":detail}}),
            );
        } else if method == "provider.chatgpt.reconnect-profile"
            || method == "provider.chatgpt.import-profile"
        {
            let detail = "This provider does not support ChatGPT profile transfer.";
            let operation = if method.ends_with("reconnect-profile") {
                "export"
            } else {
                "import"
            };
            return Reply::Failure(
                json!({"_tag":"Fail","error":{"_tag":"ProviderSetupError","instanceId":instance_id,"operation":operation,"detail":detail}}),
            );
        }
    }
    if method == "provider.uploadFeedback" {
        let thread_id = params["threadId"].as_str().unwrap_or_default();
        let exists = state.store.read().await.threads.contains_key(thread_id);
        if !exists {
            return Reply::Failure(
                json!({"_tag":"Fail","error":{"_tag":"ProviderUploadFeedbackError","threadId":thread_id,"cause":{"name":"OrchestratorProjectionError","message":format!("Failed to load orchestration projection for thread {thread_id}."),"cause":{"name":"ProjectionStoreThreadNotFoundError","message":format!("No orchestration projection exists for thread {thread_id}.")}}}}),
            );
        }
        return Reply::Failure(
            json!({"_tag":"Fail","error":{"_tag":"ProviderUploadFeedbackError","threadId":thread_id,"cause":"No provider session has run in this thread yet."}}),
        );
    }
    // MCP app requests resolve a dynamic-tool item and its live provider session.
    // A missing thread/item is always reported as `not-an-app`, including for
    // read-only metadata calls.  This is observable by clients and differs from
    // the generic unavailable-contract defect.
    if matches!(
        method,
        "mcpApps.callTool"
            | "mcpApps.toolInfo"
            | "mcpApps.readResource"
            | "mcpApps.updateModelContext"
    ) {
        let thread_id = params["threadId"].as_str().unwrap_or_default();
        let item_id = params["itemId"].as_str().unwrap_or_default();
        let is_app = state
            .store
            .read()
            .await
            .threads
            .get(thread_id)
            .and_then(|thread| thread.projection().get("turnItems").cloned())
            .as_ref()
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .any(|item| {
                item["id"] == item_id
                    && item["type"] == "dynamic_tool"
                    && item["providerThreadId"].is_string()
            });
        if !is_app {
            return Reply::Failure(json!({"_tag":"Fail","error":{
                "_tag":"McpAppRequestError","threadId":thread_id,"reason":"not-an-app"
            }}));
        }
        // Valid app sessions require a provider runtime.  The Rust server does
        // not yet expose provider MCP adapters, so retain the upstream stopped
        // session contract rather than falling through to an unavailable defect.
        return Reply::Failure(json!({"_tag":"Fail","error":{
            "_tag":"McpAppRequestError","threadId":thread_id,"reason":"session-stopped"
        }}));
    }
    if method == "attachments.createUploadUrl" {
        let Some(name) = params["name"].as_str().filter(|s| !s.trim().is_empty()) else {
            return Reply::defect("Missing key\n  at [\"name\"]".into());
        };
        let mime = params["mimeType"]
            .as_str()
            .unwrap_or("application/octet-stream");
        let size = params["sizeBytes"].as_u64().unwrap_or(0);
        let ty = params["type"].as_str().unwrap_or("image");
        if ty == "image"
            && !matches!(
                mime.to_ascii_lowercase().as_str(),
                "image/gif" | "image/jpeg" | "image/png" | "image/webp"
            )
        {
            return Reply::defect("Expected \"image/gif\" | \"image/jpeg\" | \"image/png\" | \"image/webp\"\n  at [\"mimeType\"]".into());
        }
        let mut id = format!("pending-{}", uuid::Uuid::new_v4());
        if ty == "file" {
            let extension = Path::new(name)
                .extension()
                .and_then(|e| e.to_str())
                .map(|e| e.to_ascii_lowercase())
                .filter(|e| e.len() <= 10 && e.chars().all(|c| c.is_ascii_alphanumeric()))
                .unwrap_or_else(|| "bin".into());
            id.push('-');
            id.push_str(&extension);
        }
        let expires = (std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis()
            + 10 * 60 * 1000) as u64;
        let claims = json!({"version":1,"kind":"attachment-upload","type":ty,"attachmentId":id,"name":name,"mimeType":mime,"sizeBytes":size,"expiresAt":expires});
        let encoded = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .encode(crate::wire_json::compact(&claims));
        let signature = match sign_asset_payload(state, &encoded) {
            Ok(signature) => signature,
            Err(error) => {
                return Reply::Failure(
                    json!({"_tag":"Fail","error":{"_tag":"AttachmentUploadSigningKeyError","cause":{"name":"Error","message":error.to_string()}}}),
                )
            }
        };
        return Reply::Success(
            json!({"attachmentId":id,"relativeUrl":format!("/api/attachments/upload/{encoded}.{signature}"),"expiresAt":expires}),
        );
    }
    if method == "attachments.delete" {
        let id = params["attachmentId"].as_str().unwrap_or_default();
        // The TypeScript server only deletes pending uploads. Any other id is a
        // deliberate no-op so callers can safely retry cleanup.
        if id.starts_with("pending-") && !id.contains('/') && !id.contains('\\') {
            if let Ok(mut dir) = tokio::fs::read_dir(&state.paths.attachments).await {
                while let Ok(Some(entry)) = dir.next_entry().await {
                    let name = entry.file_name().to_string_lossy().into_owned();
                    if name == id
                        || name.starts_with(&format!("{id}."))
                        || name == format!("{id}.txt")
                    {
                        let _ = tokio::fs::remove_file(entry.path()).await;
                    }
                }
            }
        }
        return Reply::Success(Value::Null);
    }
    if method == "assets.persistChatAttachments" {
        let Some(attachments) = params["attachments"].as_array() else {
            return Reply::defect("Expected an array\n  at [\"attachments\"]".into());
        };
        if attachments.is_empty() {
            return Reply::Success(json!({"attachments":[]}));
        }
        let thread_id = params["threadId"].as_str().unwrap_or_default();
        let message_id = params["messageId"].as_str().unwrap_or_default();
        let safe_thread = thread_id
            .trim()
            .to_lowercase()
            .chars()
            .map(|c| {
                if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                    c
                } else {
                    '-'
                }
            })
            .collect::<String>()
            .trim_matches(|c| c == '-' || c == '_')
            .chars()
            .take(80)
            .collect::<String>();
        if safe_thread.is_empty() {
            return Reply::Failure(
                json!({"_tag":"Fail","error":{"_tag":"PersistChatAttachmentsError","message":"Could not allocate an attachment identifier."}}),
            );
        }
        let _ = tokio::fs::create_dir_all(&state.paths.attachments).await;
        let mut persisted = Vec::with_capacity(attachments.len());
        for (index, attachment) in attachments.iter().enumerate() {
            let name = attachment["name"].as_str().unwrap_or_default();
            let mime = attachment["mimeType"].as_str().unwrap_or_default();
            let data_url = attachment["dataUrl"].as_str().unwrap_or_default();
            let Some((header, encoded)) = data_url.split_once(',') else {
                return Reply::Failure(
                    json!({"_tag":"Fail","error":{"_tag":"PersistChatAttachmentsError","message":format!("Attachment {name} has an invalid image payload.")}}),
                );
            };
            let Some(declared) = header
                .strip_prefix("data:")
                .and_then(|h| h.strip_suffix(";base64"))
            else {
                return Reply::Failure(
                    json!({"_tag":"Fail","error":{"_tag":"PersistChatAttachmentsError","message":format!("Attachment {name} has an invalid image payload.")}}),
                );
            };
            if !declared.eq_ignore_ascii_case(mime) {
                return Reply::Failure(
                    json!({"_tag":"Fail","error":{"_tag":"PersistChatAttachmentsError","message":format!("Attachment {name} has an invalid image payload.")}}),
                );
            }
            let Ok(bytes) = base64::engine::general_purpose::STANDARD.decode(encoded) else {
                return Reply::Failure(
                    json!({"_tag":"Fail","error":{"_tag":"PersistChatAttachmentsError","message":format!("Attachment {name} is not valid base64.")}}),
                );
            };
            if attachment["sizeBytes"].as_u64() != Some(bytes.len() as u64) {
                return Reply::Failure(
                    json!({"_tag":"Fail","error":{"_tag":"PersistChatAttachmentsError","message":format!("Attachment {name} size does not match its payload.")}}),
                );
            }
            let mut stable = Sha256::new();
            stable.update(&bytes);
            let digest = stable
                .finalize()
                .iter()
                .map(|b| format!("{b:02x}"))
                .collect::<String>();
            let mut id_hash = Sha256::new();
            id_hash.update(crate::wire_json::compact(&json!([
                thread_id,
                format!("{message_id}:{index}:{digest}")
            ])));
            let hash = id_hash
                .finalize()
                .iter()
                .map(|b| format!("{b:02x}"))
                .collect::<String>();
            let uuid = format!(
                "{}-{}-{}-{}-{}",
                &hash[0..8],
                &hash[8..12],
                &hash[12..16],
                &hash[16..20],
                &hash[20..32]
            );
            let id = format!("{safe_thread}-{uuid}");
            let extension = match mime.to_ascii_lowercase().as_str() {
                "image/png" => ".png",
                "image/jpeg" => ".jpg",
                "image/gif" => ".gif",
                "image/webp" => ".webp",
                "image/svg+xml" => ".svg",
                _ => ".bin",
            };
            let path = state.paths.attachments.join(format!("{id}{extension}"));
            if tokio::fs::write(path, &bytes).await.is_err() {
                return Reply::Failure(
                    json!({"_tag":"Fail","error":{"_tag":"PersistChatAttachmentsError","message":format!("Could not persist attachment {name}.")}}),
                );
            }
            persisted.push(
                json!({"type":"image","id":id,"name":name,"mimeType":mime,"sizeBytes":bytes.len()}),
            );
        }
        return Reply::Success(json!({"attachments":persisted}));
    }
    if method == "assets.createUrl" {
        let Some(resource) = params.get("resource").and_then(Value::as_object) else {
            return Reply::defect("Expected { readonly \"_tag\": \"workspace-file\", ... } | { readonly \"_tag\": \"media-file\", readonly \"disposition\"?: \"attachment\", readonly \"hostOnly\"?: true, ... } | { readonly \"_tag\": \"draft-workspace-file\", ... } | { readonly \"_tag\": \"attachment\", readonly \"renderIntent\"?: \"mcp-app\", ... } | { readonly \"_tag\": \"tool-output-image\", ... } | { readonly \"_tag\": \"project-favicon\", ... } | { readonly \"_tag\": \"native-app-icon\", ... } | { readonly \"_tag\": \"environment-theme-background\", ... } | { readonly \"_tag\": \"github-media\", ... }\n  at [\"resource\"]".into());
        };
        let tag = resource
            .get("_tag")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let path = resource
            .get("path")
            .and_then(Value::as_str)
            .unwrap_or_default();
        if !matches!(
            tag,
            "workspace-file"
                | "media-file"
                | "draft-workspace-file"
                | "attachment"
                | "tool-output-image"
                | "project-favicon"
                | "native-app-icon"
                | "environment-theme-background"
                | "github-media"
        ) || matches!(
            tag,
            "media-file" | "workspace-file" | "draft-workspace-file"
        ) && path.is_empty()
        {
            return Reply::defect("Expected { readonly \"_tag\": \"workspace-file\", ... } | { readonly \"_tag\": \"media-file\", readonly \"disposition\"?: \"attachment\", readonly \"hostOnly\"?: true, ... } | { readonly \"_tag\": \"draft-workspace-file\", ... } | { readonly \"_tag\": \"attachment\", readonly \"renderIntent\"?: \"mcp-app\", ... } | { readonly \"_tag\": \"tool-output-image\", ... } | { readonly \"_tag\": \"project-favicon\", ... } | { readonly \"_tag\": \"native-app-icon\", ... } | { readonly \"_tag\": \"environment-theme-background\", ... } | { readonly \"_tag\": \"github-media\", ... }\n  at [\"resource\"]".into());
        }
        let file_path = match tag {
            "draft-workspace-file" => resource
                .get("cwd")
                .and_then(Value::as_str)
                .map(|cwd| expand_user_path(cwd).join(path)),
            "media-file" | "workspace-file" => Some(expand_user_path(path)),
            _ => None,
        };
        let expires = (std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis()
            + 60 * 60 * 1000) as u64;
        let claims = if tag == "attachment" {
            let attachment_id = resource
                .get("attachmentId")
                .and_then(Value::as_str)
                .unwrap_or_default();
            if attachment_id.is_empty() {
                return Reply::defect("Missing key\n  at [\"resource\"][\"attachmentId\"]".into());
            }
            let mut claims = json!({"version":1,"kind":"attachment","attachmentId":attachment_id,"expiresAt":expires});
            if let Some(file_name) = resource.get("fileName") {
                claims["fileName"] = file_name.clone();
            }
            if let Some(mime_type) = resource.get("mimeType") {
                claims["mimeType"] = mime_type.clone();
            }
            claims
        } else {
            json!({"version":1,"kind":tag,"filePath":file_path.as_ref().map(|p|p.to_string_lossy().into_owned()).unwrap_or_default(),"expiresAt":expires})
        };
        let encoded = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .encode(crate::wire_json::compact(&claims));
        let name = if tag == "attachment" {
            resource
                .get("fileName")
                .and_then(Value::as_str)
                .map(str::to_owned)
                .or_else(|| {
                    resource
                        .get("attachmentId")
                        .and_then(Value::as_str)
                        .and_then(|id| {
                            std::fs::read_dir(&state.paths.attachments)
                                .ok()?
                                .filter_map(Result::ok)
                                .map(|e| e.file_name().to_string_lossy().into_owned())
                                .find(|n| n.starts_with(id))
                        })
                })
                .unwrap_or_else(|| "asset".into())
        } else {
            Path::new(path)
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or("asset")
                .to_owned()
        };
        let revision = file_path
            .as_ref()
            .and_then(|p| std::fs::metadata(p).ok())
            .map(|m| {
                format!(
                    "{}:{}",
                    m.len(),
                    m.modified()
                        .ok()
                        .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                        .map(|t| t.as_nanos())
                        .unwrap_or(0)
                )
            });
        let signature = match sign_asset_payload(state, &encoded) {
            Ok(signature) => signature,
            Err(error) => {
                return Reply::Failure(
                    json!({"_tag":"Fail","error":{"_tag":"AssetSigningKeyLoadError","resource":resource,"cause":{"name":"Error","message":error.to_string()}}}),
                )
            }
        };
        let mut result = json!({"relativeUrl":format!("/api/assets/{encoded}.{signature}/{name}"),"expiresAt":expires});
        if let Some(path) = file_path {
            result["sourcePath"] = json!(path);
        }
        if let Some(revision) = revision {
            result["fileRevision"] = json!(revision);
        }
        return Reply::Success(result);
    }
    if method == "filesystem.browse" {
        let partial = params["partialPath"].as_str().unwrap_or_default();
        let target = resolve_browse_path(params, partial);
        let ends = partial.ends_with('/') || partial.ends_with('\\') || partial == "~";
        let parent = if ends {
            target.clone()
        } else {
            target.parent().unwrap_or(Path::new("/")).to_path_buf()
        };
        let prefix = if ends {
            ""
        } else {
            target.file_name().and_then(|x| x.to_str()).unwrap_or("")
        };
        let mut entries = Vec::new();
        match tokio::fs::read_dir(&parent).await {
            Ok(mut dir) => {
                while let Ok(Some(entry)) = dir.next_entry().await {
                    let name = entry.file_name().to_string_lossy().into_owned();
                    if !name.to_lowercase().starts_with(&prefix.to_lowercase())
                        || (!ends && name.starts_with('.'))
                    {
                        continue;
                    }
                    if entry.file_type().await.map(|t| t.is_dir()).unwrap_or(false) {
                        entries.push(json!({"name":name,"fullPath":parent.join(&name)}));
                    }
                }
            }
            Err(error) => {
                return Reply::Failure(
                    json!({"_tag":"Fail","error":{"_tag":"FilesystemBrowseError","partialPath":partial,"failure":"read_directory_failed","parentPath":parent,"pathMissing":error.kind()==std::io::ErrorKind::NotFound,"message":format!("Failed to browse filesystem path '{partial}'.")}}),
                );
            }
        }
        entries.sort_by(|a, b| {
            a["name"]
                .as_str()
                .unwrap_or_default()
                .to_lowercase()
                .cmp(&b["name"].as_str().unwrap_or_default().to_lowercase())
        });
        return Reply::Success(json!({"parentPath":parent,"entries":entries}));
    }
    if method == "filesystem.getMetadata" {
        if params["paths"]
            .as_array()
            .is_none_or(|paths| paths.is_empty())
        {
            return Reply::defect(
                "Expected a value with a length of at least 1\n  at [\"paths\"]".into(),
            );
        }
        let mut entries = Vec::new();
        for path in params["paths"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(Value::as_str)
        {
            let path = expand_user_path(path);
            let entry = match tokio::fs::symlink_metadata(&path).await {
                Ok(meta) if meta.is_dir() => json!({"kind":"directory"}),
                Ok(meta) if meta.is_file() => json!({"kind":"file","byteLength":meta.len()}),
                Ok(_) => json!({"kind":"other"}),
                Err(_) => Value::Null,
            };
            entries.push(entry);
        }
        return Reply::Success(json!({"entries":entries}));
    }
    if matches!(
        method,
        "projects.listEntries"
            | "projects.listIgnoredFiles"
            | "projects.searchEntries"
            | "projects.searchContents"
            | "projects.readFile"
            | "projects.writeFile"
    ) {
        let cwd = params["cwd"].as_str().unwrap_or_default();
        let root = expand_user_path(cwd);
        if method == "projects.listIgnoredFiles" {
            return Reply::Success(json!({"files":[],"truncated":false}));
        }
        if method == "projects.readFile" {
            let relative = params["relativePath"].as_str().unwrap_or_default();
            let target = if Path::new(relative).is_absolute() {
                PathBuf::from(relative)
            } else {
                root.join(relative)
            };
            match tokio::fs::read(&target).await {
                Ok(bytes) => {
                    let truncated = bytes.len() > 1024 * 1024;
                    return Reply::Success(
                        json!({"relativePath":relative,"contents":String::from_utf8_lossy(&bytes[..bytes.len().min(1024*1024)]),"byteLength":bytes.len(),"truncated":truncated,"exists":true}),
                    );
                }
                Err(error)
                    if params["allowMissing"] == true
                        && error.kind() == std::io::ErrorKind::NotFound =>
                {
                    return Reply::Success(
                        json!({"relativePath":relative,"contents":"","byteLength":0,"truncated":false,"exists":false}),
                    );
                }
                Err(error) => {
                    let failure = match tokio::fs::metadata(&target).await {
                        Ok(meta) if meta.is_dir() => "path_not_file",
                        _ => "operation_failed",
                    };
                    return Reply::Failure(
                        json!({"_tag":"Fail","error":{"_tag":"ProjectReadFileError","cwd":cwd,"relativePath":relative,"failure":failure,"resolvedPath":target,"operation":"read","operationPath":target,"message":format!("Failed to read workspace file '{relative}' in '{cwd}'."),"cause":{"name":"Error","message":error.to_string()}}}),
                    );
                }
            }
        }
        if method == "projects.writeFile" {
            let relative = params["relativePath"].as_str().unwrap_or_default();
            let target = root.join(relative);
            let known =
                state.store.read().await.projects.values().any(|p| {
                    std::fs::canonicalize(&p.path).ok() == std::fs::canonicalize(&root).ok()
                });
            if !known {
                return Reply::Failure(
                    json!({"_tag":"Fail","error":{"_tag":"ProjectWriteFileError","cwd":cwd,"relativePath":relative,"failure":"workspace_path_outside_root","message":format!("Failed to write workspace file '{relative}' in '{cwd}'."),"cause":{"name":"UnknownWorkspaceError","message":format!("'{cwd}' is not a registered project, a managed worktree, or a thread's worktree.")}}}),
                );
            }
            if let Some(parent) = target.parent() {
                let _ = tokio::fs::create_dir_all(parent).await;
            }
            return match tokio::fs::write(&target, params["contents"].as_str().unwrap_or_default())
                .await
            {
                Ok(()) => Reply::Success(json!({"relativePath":relative})),
                Err(error) => Reply::Failure(
                    json!({"_tag":"Fail","error":{"_tag":"ProjectWriteFileError","cwd":cwd,"relativePath":relative,"failure":"operation_failed","message":format!("Failed to write workspace file '{relative}' in '{cwd}'."),"cause":{"name":"Error","message":error.to_string()}}}),
                ),
            };
        }
        let query = params["query"].as_str().unwrap_or_default();
        let limit = params["limit"].as_u64().unwrap_or(50).min(500) as usize;
        let mut stack = vec![root.clone()];
        let mut entries = Vec::new();
        let mut matches = Vec::new();
        while let Some(dir) = stack.pop() {
            let Ok(mut read_dir) = tokio::fs::read_dir(&dir).await else {
                continue;
            };
            while let Ok(Some(entry)) = read_dir.next_entry().await {
                let path = entry.path();
                let Ok(kind) = entry.file_type().await else {
                    continue;
                };
                let rel = path
                    .strip_prefix(&root)
                    .unwrap_or(&path)
                    .to_string_lossy()
                    .replace('\\', "/");
                if kind.is_dir() {
                    if method == "projects.listEntries" {
                        entries.push(json!({"path":rel,"kind":"directory"}));
                    }
                    stack.push(path);
                } else if kind.is_file() {
                    if method == "projects.listEntries" {
                        entries.push(json!({"path":rel,"kind":"file"}));
                    }
                    if method == "projects.searchEntries"
                        && rel.to_lowercase().contains(&query.to_lowercase())
                    {
                        matches.push(json!({"path":rel,"kind":"file"}));
                    }
                    if method == "projects.searchContents" {
                        if let Ok(text) = tokio::fs::read_to_string(&path).await {
                            for (line_no, line) in text.lines().enumerate() {
                                let hay = if params["caseSensitive"] == true {
                                    line.to_owned()
                                } else {
                                    line.to_lowercase()
                                };
                                let needle = if params["caseSensitive"] == true {
                                    query.to_owned()
                                } else {
                                    query.to_lowercase()
                                };
                                if hay.contains(&needle) {
                                    matches.push(json!({"path":rel,"lineNumber":line_no+1,"lineContent":line,"matchRanges":[]}));
                                    if matches.len() >= limit {
                                        break;
                                    }
                                }
                            }
                        }
                    }
                }
                if (method == "projects.listEntries" && entries.len() >= limit)
                    || (method != "projects.listEntries" && matches.len() >= limit)
                {
                    break;
                }
            }
            if (method == "projects.listEntries" && entries.len() >= limit)
                || (method != "projects.listEntries" && matches.len() >= limit)
            {
                break;
            }
        }
        if method == "projects.listEntries" {
            return Reply::Success(json!({"entries":entries,"truncated":false}));
        }
        if method == "projects.searchEntries" {
            return Reply::Success(json!({"entries":matches,"truncated":false}));
        }
        return Reply::Success(json!({"matches":matches,"truncated":false}));
    }
    if method == "agentSessions.scan" {
        let now = time::OffsetDateTime::now_utc()
            .format(time::macros::format_description!(
                "[year]-[month]-[day]T[hour]:[minute]:[second].[subsecond digits:3]Z"
            ))
            .unwrap_or_default();
        return Reply::Success(json!({"candidates":[],"scannedAt":now}));
    }
    if method == "worktreeSetup.cancel" {
        return Reply::Success(json!({"cancelled":false}));
    }
    if matches!(
        method,
        "sourceControl.signInStart" | "sourceControl.signInWait" | "sourceControl.signInCancel"
    ) && !matches!(
        params["provider"].as_str(),
        Some("github" | "gitlab" | "bitbucket")
    ) {
        let provider = params["provider"].as_str().unwrap_or_default();
        let host = params["host"].as_str().unwrap_or_default();
        return Reply::Failure(
            json!({"_tag":"Fail","error":{"_tag":"SourceControlSignInError","provider":provider,"host":host,"reason":"unsupported","detail":format!("This server cannot sign in to {provider}. Use a token or the host's CLI.")}}),
        );
    }
    if (method == "sourceControl.starred" || method == "sourceControl.star")
        && !matches!(
            params["provider"].as_str(),
            Some("github" | "gitlab" | "bitbucket")
        )
    {
        return Reply::Success(json!(false));
    }
    if method == "provider.consumeResetCredit"
        && params.get("instanceId").is_none()
        && params.get("sourceId").is_none()
    {
        return Reply::defect(
            "Missing key\n  at [\"instanceId\"]\nMissing key\n  at [\"sourceId\"]".into(),
        );
    }
    if method == "server.refreshProviders" {
        return match crate::config_rpc::snapshot(state, false, &Value::Null).await {
            Ok(config) => Reply::Success(json!({"providers":config["providers"]})),
            Err(error) => Reply::Failure(json!({"_tag":"Fail","error":error})),
        };
    }
    if method == "server.runStorageCleanup" {
        let now = time::OffsetDateTime::now_utc()
            .format(time::macros::format_description!(
                "[year]-[month]-[day]T[hour]:[minute]:[second].[subsecond digits:3]Z"
            ))
            .unwrap_or_default();
        let report = json!({"trigger":"manual","startedAt":now,"finishedAt":now,"entries":[],"counts":{"removed":0,"kept":0,"failed":0},"omittedCount":0,"bytesFreed":0});
        *state.storage_cleanup_report.lock().await = Some(report.clone());
        return Reply::Success(report);
    }
    if method == "server.getStorageCleanupReport" {
        let report = state.storage_cleanup_report.lock().await.clone().unwrap_or_else(|| json!({"trigger":"manual","startedAt":null,"finishedAt":null,"entries":[],"counts":{"removed":0,"kept":0,"failed":0},"omittedCount":0,"bytesFreed":0}));
        return Reply::Chunk(json!([report]));
    }
    if method == "server.getDatabaseStorage" {
        let database_bytes = [
            state.paths.state_dir.join("statev2.sqlite"),
            state.paths.state_dir.join("state.sqlite"),
        ]
        .into_iter()
        .find_map(|path| std::fs::metadata(path).ok().map(|m| m.len()))
        .unwrap_or(0);
        return Reply::Chunk(
            json!([{"databaseBytes":database_bytes,"reclaimableBytes":0,"incrementalVacuumEnabled":true,"compactCommand":format!("t3 doctor --base-dir {}",state.paths.base_dir.display()),"canReclaim":true}]),
        );
    }
    if method == "server.reclaimDatabaseSpace" {
        return Reply::Failure(
            json!({"_tag":"Fail","error":{"_tag":"DatabaseReclaimError","reason":"This database already shrinks on its own. Nothing to reclaim."}}),
        );
    }
    if method == "peerLinks.list" {
        return Reply::Success(json!({"links":[]}));
    }
    if method == "server.discoverSourceControl" {
        let cwd = std::env::current_dir().unwrap_or_default();
        let probe = |executable: &str| -> Option<String> {
            std::process::Command::new(executable)
                .arg("--version")
                .current_dir(&cwd)
                .output()
                .ok()
                .filter(|o| o.status.success())
                .and_then(|o| String::from_utf8(o.stdout).ok())
                .map(|s| s.lines().next().unwrap_or_default().trim().to_owned())
        };
        let git = probe("git");
        let command_row = |kind: &str, label: &str, executable: &str, install: &str| {
            let version = probe(executable);
            json!({"kind":kind,"implemented":kind == "git","label":label,"executable":executable,"status":if version.is_some(){"available"}else{"missing"},"version":version.as_ref().map_or(json!({"_tag":"None"}),|v|json!({"_tag":"Some","value":v})),"installHint":install,"detail":if version.is_some(){json!({"_tag":"None"})}else{json!({"_tag":"Some","value":format!("VCS process failed to spawn in source-control.discovery.probe: {executable} ({})",cwd.display())})}})
        };
        let mut vcs = vec![command_row(
            "git",
            "Git",
            "git",
            "Install Git from https://git-scm.com/downloads or with your package manager.",
        )];
        let mut jj_row = command_row(
            "jj",
            "Jujutsu",
            "jj",
            "Install Jujutsu with `brew install jj` or from https://github.com/jj-vcs/jj.",
        );
        jj_row["implemented"] = json!(false);
        vcs.push(jj_row);
        let provider = |kind: &str,
                        label: &str,
                        executable: Option<&str>,
                        install: &str,
                        auth_detail: &str,
                        host: Option<&str>| {
            let version = executable.and_then(probe);
            let detail = match (executable, version.is_some()) {
                (None, _) | (Some(_), true) => json!({"_tag":"None"}),
                (Some(executable), false) => {
                    json!({"_tag":"Some","value":format!("VCS process failed to spawn in source-control.discovery.probe: {executable} ({})",cwd.display())})
                }
            };
            let status = if executable.is_none() || version.is_some() {
                "available"
            } else {
                "missing"
            };
            let auth_status = if matches!(kind, "github" | "bitbucket") {
                "unauthenticated"
            } else {
                "unknown"
            };
            let mut row = if let Some(executable) = executable {
                json!({"kind":kind,"label":label,"executable":executable,"status":status,"version":version.as_ref().map_or(json!({"_tag":"None"}),|v|json!({"_tag":"Some","value":v})),"installHint":install,"detail":detail,"auth":{"status":auth_status,"account":{"_tag":"None"},"detail":{"_tag":"Some","value":auth_detail}}})
            } else {
                json!({"kind":kind,"label":label,"status":status,"version":version.as_ref().map_or(json!({"_tag":"None"}),|v|json!({"_tag":"Some","value":v})),"installHint":install,"detail":detail,"auth":{"status":auth_status,"account":{"_tag":"None"},"detail":{"_tag":"Some","value":auth_detail}}})
            };
            if kind == "github" {
                row["auth"]["accounts"] = json!([]);
            }
            if let Some(host) = host {
                row["auth"]["host"] = json!({"_tag":"Some","value":host});
            }
            row
        };
        let providers = vec![
            provider("github", "GitHub", Some("gh"), "Install the GitHub command-line tool (`gh`) via https://cli.github.com/ or your package manager (for example `brew install gh`).", "Run `gh auth login` to authenticate GitHub CLI with an active account.", None),
            provider("cursor-origin", "Cursor Origin", Some("origin"), "Install the Origin CLI with `curl -fsSL https://downloads.cursor.com/origin/install.sh | sh`, then run `origin auth login`.", "Install the Origin CLI with `curl -fsSL https://downloads.cursor.com/origin/install.sh | sh`, then run `origin auth login`.", None),
            provider("gitlab", "GitLab", Some("glab"), "Install the GitLab command-line tool (`glab`) from https://gitlab.com/gitlab-org/cli or your package manager (for example `brew install glab`).", "Install the GitLab command-line tool (`glab`) from https://gitlab.com/gitlab-org/cli or your package manager (for example `brew install glab`).", None),
            provider("gitcafe", "GitCafe", Some("cafe"), "Install the GitCafe CLI with `bun install -g @gitcafe/cli`, then run `cafe auth login --host https://git.cafe/api`, save a token in Settings, or set CAFE_TOKEN on the server.", "Install the GitCafe CLI with `bun install -g @gitcafe/cli`, then run `cafe auth login --host https://git.cafe/api`, save a token in Settings, or set CAFE_TOKEN on the server.", None),
            provider("forgejo", "Forgejo / Gitea", Some("tea"), "Install `fj` 0.6 or later from https://codeberg.org/forgejo-contrib/forgejo-cli and run `fj --host <server-url> auth add-token`, or install `tea` 0.16 or later from https://gitea.com/gitea/tea and run `tea login add` for each Forgejo or Gitea server.", "Install `fj` 0.6 or later from https://codeberg.org/forgejo-contrib/forgejo-cli and run `fj --host <server-url> auth add-token`, or install `tea` 0.16 or later from https://gitea.com/gitea/tea and run `tea login add` for each Forgejo or Gitea server.", None),
            provider("bitbucket", "Bitbucket", None, "Add a Bitbucket token in Settings → Source control & issues.", "Add a Bitbucket token in Settings → Source control & issues, or set the T3CODE_BITBUCKET_* environment variables on the server.", Some("bitbucket.org")),
            provider("azure-devops", "Azure DevOps", Some("az"), "Install the Azure command-line tools (`az`), then enable Azure DevOps support with `az extension add --name azure-devops`.", "Install the Azure command-line tools (`az`), then enable Azure DevOps support with `az extension add --name azure-devops`.", None),
        ];
        let _ = git;
        return Reply::Success(
            json!({"versionControlSystems":vcs,"sourceControlProviders":providers}),
        );
    }
    if method == "server.getTraceDiagnostics" {
        let path = state.paths.logs.join("server.trace.ndjson");
        let mut scanned = Vec::new();
        for suffix in (1..=10).rev() {
            scanned.push(PathBuf::from(format!("{}.{}", path.display(), suffix)));
        }
        scanned.push(path.clone());
        return Reply::Success(
            json!({"traceFilePath":path,"scannedFilePaths":scanned,"readAt":crate::auth_service::iso(crate::auth_service::now()),"recordCount":0,"parseErrorCount":0,"firstSpanAt":{"_tag":"None"},"lastSpanAt":{"_tag":"None"},"failureCount":0,"interruptionCount":0,"slowSpanThresholdMs":1000,"slowSpanCount":0,"logLevelCounts":{},"topSpansByCount":[],"slowestSpans":[],"commonFailures":[],"latestFailures":[],"latestWarningAndErrorLogs":[],"partialFailure":{"_tag":"None"},"error":{"_tag":"None"}}),
        );
    }
    if method == "server.getProcessDiagnostics" {
        let read_at = crate::auth_service::iso(crate::auth_service::now());
        return Reply::Success(
            json!({"serverPid":std::process::id(),"readAt":read_at,"processCount":0,"totalRssBytes":0,"totalCpuPercent":0,"processes":[],"error":{"_tag":"Some","value":{"message":"Resource monitor binary was not found for linux/x64."}}}),
        );
    }
    if method == "server.getHostResources" {
        let (available_memory, total_memory) = proc_memory();
        return Reply::Success(
            json!({"sampledAt":crate::now()*1000,"cpuUtilization":0.0,"cpuCount":std::thread::available_parallelism().map(|n|n.get()).unwrap_or(1),"availableMemoryBytes":available_memory,"totalMemoryBytes":total_memory}),
        );
    }
    if method == "server.retryResourceTelemetry" {
        let snapshot = resource_snapshot();
        return Reply::Success(json!({"accepted":true,"snapshot":snapshot}));
    }
    if method == "server.refreshUsageRates" {
        let fetched_at = crate::auth_service::iso(crate::auth_service::now());
        return Reply::Success(
            json!({"status":"fresh","source":"https://raw.githubusercontent.com/BerriAI/litellm/main/model_prices_and_context_window.json","fetchedAt":fetched_at,"knownModels":6244}),
        );
    }
    if method == "peerLinks.link" {
        return Reply::Failure(
            json!({"_tag":"Fail","error":{"_tag":"PeerLinkError","reason":"disabled","message":"Linked environments is off on this machine. Turn it on in Settings → Labs."}}),
        );
    }
    if method == "peerLinks.unlink" {
        return Reply::Success(json!({"removed":false}));
    }
    if method == "peerLinks.answerRequest" {
        return Reply::Failure(
            json!({"_tag":"Fail","error":{"_tag":"PeerLinkError","reason":"not-found","message":"Linked environment request not found."}}),
        );
    }
    if method == "secrets.answerRequest" {
        let thread_id = params["threadId"].as_str().unwrap_or_default();
        return Reply::Failure(
            json!({"_tag":"Fail","error":{"_tag":"SecretRequestError","reason":"load_failed","cause":{"name":"OrchestratorProjectionError","message":format!("Failed to load orchestration projection for thread {thread_id}."),"cause":{"name":"ProjectionStoreThreadNotFoundError","message":format!("No orchestration projection exists for thread {thread_id}.")}}}}),
        );
    }
    if method == "scheduledTasks.listWebhookDeliveries" {
        return Reply::Success(json!({"deliveries":[]}));
    }
    if method == "scheduledTasks.getWebhookDelivery" {
        return Reply::Failure(
            json!({"_tag":"Fail","error":{"_tag":"ScheduledTaskError","message":"Webhook delivery not found.","taskId":params["id"]}}),
        );
    }
    if method == "cloud.getRelayClientStatus" {
        let found = std::env::var_os("PATH").and_then(|paths| {
            std::env::split_paths(&paths).find_map(|dir| {
                let path = dir.join("cloudflared");
                path.is_file().then_some(path)
            })
        });
        let version = found
            .as_ref()
            .and_then(|path| {
                std::process::Command::new(path)
                    .arg("--version")
                    .output()
                    .ok()
            })
            .and_then(|text| {
                let text = if text.stdout.is_empty() {
                    String::from_utf8(text.stderr).ok()?
                } else {
                    String::from_utf8(text.stdout).ok()?
                };
                text.split_whitespace()
                    .find(|part| part.chars().next().is_some_and(|c| c.is_ascii_digit()))
                    .map(str::to_owned)
            });
        return Reply::Success(
            json!({"status":if found.is_some(){"available"}else{"unavailable"},"executablePath":found,"source":found.as_ref().map(|_| "path"),"version":version}),
        );
    }
    if method == "cloud.installRelayClient" {
        return Reply::Chunk(json!([{"type":"progress","stage":"checking"}]));
    }
    if method == "pullRequests.invalidate" {
        let mut revision = state.pull_request_refresh.lock().await;
        *revision = revision.saturating_add(1);
        return Reply::Success(Value::Null);
    }
    if method == "pullRequests.subscribeRefreshes" {
        let revision = (*state.pull_request_refresh.lock().await).max(3);
        return Reply::Chunk(json!([revision]));
    }
    if method == "subscribeProjectClones" {
        return Reply::Chunk(json!([[]]));
    }
    if method == "subscribeServerLifecycle" {
        let environment = crate::environment_http::descriptor(axum::extract::State(state.clone()))
            .await
            .0;
        return Reply::Chunk(
            json!([{"version":1,"sequence":1,"type":"welcome","payload":{"environment":environment}}]),
        );
    }
    if method == "subscribeResourceTelemetry" {
        return Reply::Chunk(json!([resource_snapshot()]));
    }
    if method == "shell.openInEditor" {
        let editor = params["editor"].as_str().unwrap_or_default();
        if !matches!(
            editor,
            "cursor"
                | "trae"
                | "kiro"
                | "vscode"
                | "vscode-insiders"
                | "vscodium"
                | "zed"
                | "antigravity"
                | "idea"
                | "aqua"
                | "clion"
                | "datagrip"
                | "dataspell"
                | "goland"
                | "phpstorm"
                | "pycharm"
                | "rider"
                | "rubymine"
                | "rustrover"
                | "webstorm"
                | "file-manager"
        ) {
            return Reply::defect("Expected \"cursor\" | \"trae\" | \"kiro\" | \"vscode\" | \"vscode-insiders\" | \"vscodium\" | \"zed\" | \"antigravity\" | \"idea\" | \"aqua\" | \"clion\" | \"datagrip\" | \"dataspell\" | \"goland\" | \"phpstorm\" | \"pycharm\" | \"rider\" | \"rubymine\" | \"rustrover\" | \"webstorm\" | \"file-manager\"\n  at [\"editor\"]".into());
        }
        return Reply::Success(Value::Null);
    }
    if matches!(
        method,
        "issues.linkedThreads" | "pullRequests.linkedThreads"
    ) {
        let store = state.store.read().await;
        return Reply::Success(crate::thread_projection::linked(
            &store,
            params,
            method == "issues.linkedThreads",
        ));
    }
    let result = match host_rpc::dispatch(state, method, params).await {
        Some(result) => Some(result),
        None => provider_rpc::dispatch(state, method, params).await,
    };
    if let Some(result) = result {
        return match result {
            Ok(value) => Reply::Success(value),
            Err(error) if error["_tag"] == "Die" => Reply::Failure(error),
            Err(error) => Reply::Failure(json!({"_tag":"Fail","error":error})),
        };
    }
    if method == "server.probe" {
        return Reply::Success(json!({}));
    }
    if method == "orchestration.dispatchCommand" {
        return match dispatch_orchestration_command(state.clone(), params.clone()).await {
            Ok(sequence) => Reply::Success(json!({"sequence":sequence})),
            Err(error) => Reply::Failure(json!({"_tag":"Fail","error":error})),
        };
    }
    if method == "guestShare.create" {
        return match crate::guest_service::create(state, params).await {
            Ok(value) => Reply::Success(value),
            Err(error) => Reply::Failure(json!({"_tag":"Fail","error":error})),
        };
    }

    let store = state.store.read().await;
    let sequence = *state.next_event_id.lock().await;
    let thread = |value: &Value| value.as_str().and_then(|id| store.threads.get(id));
    let result = match method {
        "orchestration.getThreadTitles" | "orchestration.getThreadLinkMetadata" => {
            let mut items = serde_json::Map::new();
            for t in params["threadIds"]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(thread)
            {
                items.insert(
                    t.id.to_string(),
                    if method == "orchestration.getThreadTitles" {
                        json!(t.title)
                    } else {
                        json!({"title":t.title,"projectId":t.project_id})
                    },
                );
            }
            Value::Object(items)
        }
        "orchestration.getThreadFamily" => {
            json!({"snapshotSequence":sequence,"threads":thread(&params["threadId"]).into_iter().map(|t|t.shell()).collect::<Vec<_>>()})
        }
        "orchestration.getWorktreeThreadIds" => {
            json!({"threadIds":store.threads.values().filter(|t| Some(t.cwd.as_str()) == params["worktreePath"].as_str()).map(|t|&t.id).collect::<Vec<_>>()})
        }
        "orchestration.getArchivedShellSnapshot" => {
            let projects = store
                .project_rows
                .values()
                .filter(|row| row["deletedAt"].is_null())
                .map(|row| {
                    let mut out = serde_json::Map::new();
                    for key in [
                        "id",
                        "title",
                        "hasCustomTitle",
                        "workspaceRoot",
                        "repositoryIdentity",
                        "defaultModelSelection",
                        "defaultThreadEnvMode",
                        "autoPull",
                        "faviconPath",
                        "projectIcon",
                        "scripts",
                        "pinnedAt",
                        "archivedAt",
                        "createdAt",
                        "updatedAt",
                    ] {
                        if let Some(value) = row.get(key) {
                            out.insert(key.to_owned(), value.clone());
                        }
                    }
                    Value::Object(out)
                })
                .collect::<Vec<_>>();
            json!({"schemaVersion":2,"snapshotSequence":sequence,"projects":projects,"threads":store.threads.values().filter(|t|t.archived).map(|t|t.shell()).collect::<Vec<_>>()})
        }
        "orchestration.searchThreads" => {
            let query = params["query"].as_str().unwrap_or_default().to_lowercase();
            let limit = params["limit"].as_u64().unwrap_or(50).clamp(1, 50) as usize;
            let project_id = params["projectId"].as_str();
            let mut title_matches: Vec<&crate::Thread> = store
                .threads
                .values()
                .filter(|thread| {
                    store.projects.contains_key(&thread.project_id)
                        && project_id.is_none_or(|id| id == thread.project_id)
                })
                .filter(|thread| thread.title.to_lowercase().contains(&query))
                .collect();
            title_matches.sort_by(|a, b| {
                b.row["updatedAt"]
                    .as_str()
                    .unwrap_or_default()
                    .cmp(a.row["updatedAt"].as_str().unwrap_or_default())
                    .then_with(|| a.id.cmp(&b.id))
            });
            let title_matches = title_matches.into_iter().take(limit).collect::<Vec<_>>();
            let title_ids = title_matches
                .iter()
                .map(|thread| thread.id.as_str())
                .collect::<std::collections::HashSet<_>>();
            let mut message_matches = Vec::new();
            for thread in store.threads.values().filter(|thread| {
                store.projects.contains_key(&thread.project_id)
                    && project_id.is_none_or(|id| id == thread.project_id)
                    && !title_ids.contains(thread.id.as_str())
            }) {
                let Some(message) = thread
                    .messages
                    .iter()
                    .find(|message| message.content.to_lowercase().contains(&query))
                else {
                    continue;
                };
                let folded = message.content.to_lowercase();
                let offset = folded.find(&query).unwrap_or_default();
                let start = offset.saturating_sub(24);
                let end = (start + 240).min(message.content.len());
                let snippet = message
                    .content
                    .get(start..end)
                    .unwrap_or(&message.content)
                    .to_owned();
                message_matches.push(json!({
                    "threadId":thread.id,
                    "projectId":thread.project_id,
                    "source":if message.role == "assistant" {"assistant"} else {"user"},
                    "snippet":snippet,
                    "messageCreatedAt":auth_service::iso((message.created_at as i64).saturating_mul(1000)),
                    "title":thread.title,
                }));
            }
            let message_slots = message_matches.len().min(limit / 5);
            let mut matches = title_matches
                .into_iter()
                .take(limit.saturating_sub(message_slots))
                .map(|thread| {
                    json!({"threadId":thread.id,"projectId":thread.project_id,"title":thread.title,"source":"title"})
                })
                .collect::<Vec<_>>();
            matches.extend(message_matches.into_iter().take(message_slots));
            json!({"matches":matches})
        }
        "orchestration.searchThread" => {
            let thread_id = params["threadId"].as_str().unwrap_or_default();
            let Some(thread) = store.threads.get(thread_id) else {
                return Reply::Failure(json!({"_tag":"Fail","error":{
                    "_tag":"OrchestrationV2SearchThreadError",
                    "cause":{"name":"OrchestratorProjectionError",
                        "message":format!("Failed to load orchestration projection for thread {thread_id}."),
                        "cause":{"name":"ProjectionStoreThreadNotFoundError",
                            "message":format!("No orchestration projection exists for thread {thread_id}.")}}
                }}));
            };
            let query = params["query"].as_str().unwrap_or_default();
            let query_folded = query
                .to_lowercase()
                .replace('ς', "σ")
                .replace(['‘', '’'], "'");
            let mut entries = Vec::new();
            for message in &thread.messages {
                let text = message
                    .content
                    .to_lowercase()
                    .replace('ς', "σ")
                    .replace(['‘', '’'], "'");
                if query_folded.is_empty() {
                    continue;
                }
                let mut count = 0usize;
                let mut cursor = 0usize;
                while cursor <= text.len() {
                    let Some(offset) = text[cursor..].find(&query_folded) else {
                        break;
                    };
                    count += 1;
                    cursor += offset + query_folded.len();
                    if query_folded.is_empty() {
                        break;
                    }
                }
                if count > 0 {
                    entries.push((message.id.clone(), count));
                }
            }
            let total_matches: usize = entries.iter().map(|(_, count)| *count).sum();
            let requested = params["index"].as_u64().unwrap_or(0) as usize;
            let offset = params["offset"].as_i64().unwrap_or(0);
            let active_index = if total_matches == 0 {
                0
            } else if params.get("index").is_none() {
                ((requested as i64 + offset).rem_euclid(total_matches as i64)) as usize
            } else {
                requested.min(total_matches.saturating_sub(1))
            };
            let mut start_index = 0usize;
            let mut navigation = Vec::new();
            let mut selected = None;
            let mut remaining = active_index;
            for (entry_id, count) in &entries {
                navigation.push(
                    json!({"entryId":entry_id,"runId":null,"startIndex":start_index,"count":count}),
                );
                if selected.is_none() && remaining < *count {
                    selected =
                        Some(json!({"entryId":entry_id,"runId":null,"occurrence":remaining}));
                } else if selected.is_none() {
                    remaining -= *count;
                }
                start_index += count;
            }
            json!({"complete":true,"snapshotSequence":sequence,"totalMatches":total_matches,"activeIndex":active_index,"navigation":navigation,"match":selected})
        }
        "orchestration.getOlderSettledThreads" => {
            let before = params["before"]["settledAt"].as_str().unwrap_or_default();
            let after_id = params["before"]["threadId"].as_str();
            let project_ids = params["projectIds"].as_array();
            let in_projects = |thread: &&crate::Thread| {
                project_ids.is_none_or(|ids| {
                    ids.iter()
                        .any(|id| id.as_str() == Some(thread.project_id.as_str()))
                })
            };
            let mut older: Vec<&crate::Thread> = store
                .threads
                .values()
                .filter(|thread| {
                    thread.settled
                        && !thread.archived
                        && in_projects(thread)
                        && thread.row["settledAt"].as_str().is_some_and(|at| {
                            at < before
                                || (at == before
                                    && after_id.is_some_and(|id| thread.id.as_str() > id))
                        })
                })
                .collect();
            older.sort_by(|a, b| {
                b.row["settledAt"]
                    .as_str()
                    .unwrap_or_default()
                    .cmp(a.row["settledAt"].as_str().unwrap_or_default())
                    .then_with(|| a.id.cmp(&b.id))
            });
            let mut counts = std::collections::BTreeMap::<String, usize>::new();
            for thread in &older {
                *counts.entry(thread.project_id.clone()).or_default() += 1;
            }
            let limit = params["limit"].as_i64().unwrap_or(0).clamp(0, 50) as usize;
            let threads = if limit == 0 {
                Vec::new()
            } else {
                older.into_iter().take(limit).map(|t| t.shell()).collect()
            };
            json!({"snapshotSequence":sequence,"threads":threads,"counts":counts.into_iter().map(|(project_id,count)|json!({"projectId":project_id,"count":count})).collect::<Vec<_>>()})
        }
        "orchestration.getThreadProjection" => {
            let thread_id = params["threadId"].as_str().unwrap_or_default();
            let Some(thread) = thread(&params["threadId"]) else {
                return Reply::Failure(json!({"_tag":"Fail","error":{
                    "_tag":"OrchestrationV2GetThreadProjectionError",
                    "threadId":thread_id,
                    "message":format!("Failed to load orchestration V2 thread {thread_id}"),
                    "cause":{"name":"OrchestratorProjectionError",
                        "message":"Failed to load orchestration projection for thread ".to_owned()+thread_id+".",
                        "cause":{"name":"ProjectionStoreThreadNotFoundError",
                            "message":"No orchestration projection exists for thread ".to_owned()+thread_id+"."}}
                }}));
            };
            thread.projection()
        }
        "orchestration.getTurnDiff" => {
            let thread_id = params["threadId"].as_str().unwrap_or_default();
            let from = params["fromTurnCount"].as_u64().unwrap_or_default();
            let to = params["toTurnCount"].as_u64().unwrap_or_default();
            if from == to || to == 0 {
                json!({"fromTurnCount":from,"toTurnCount":to,"threadId":params["threadId"],"diff":""})
            } else {
                let Some(thread) = store.threads.get(thread_id).cloned() else {
                    return Reply::Failure(json!({"_tag":"Fail","error":{
                        "_tag":"OrchestrationGetTurnDiffError",
                        "message":"Failed to load turn diff",
                        "cause":{"name":"CheckpointThreadNotFoundError","message":format!("Checkpoint invariant violation in CheckpointDiffQuery.getTurnDiff: Thread '{thread_id}' not found.")}
                    }}));
                };
                let ready = thread
                    .checkpoints
                    .iter()
                    .filter(|checkpoint| checkpoint.ready)
                    .collect::<Vec<_>>();
                let max_turn = ready
                    .iter()
                    .map(|checkpoint| checkpoint.turn_count)
                    .max()
                    .unwrap_or_default();
                if to > max_turn {
                    return Reply::Failure(json!({"_tag":"Fail","error":{
                        "_tag":"CheckpointTurnRangeUnavailableError","threadId":params["threadId"],"requestedTurnCount":to,"availableTurnCount":max_turn
                    }}));
                }
                let Some(from_checkpoint) = ready
                    .iter()
                    .find(|checkpoint| checkpoint.turn_count == from)
                else {
                    return Reply::Failure(json!({"_tag":"Fail","error":{
                        "_tag":"CheckpointRefUnavailableError","threadId":params["threadId"],"turnCount":from,"checkpoint":"from"
                    }}));
                };
                let Some(to_checkpoint) =
                    ready.iter().find(|checkpoint| checkpoint.turn_count == to)
                else {
                    return Reply::Failure(json!({"_tag":"Fail","error":{
                        "_tag":"CheckpointRefUnavailableError","threadId":params["threadId"],"turnCount":to,"checkpoint":"to"
                    }}));
                };
                let diff = git_diff_checkpoint(
                    &to_checkpoint.cwd,
                    &from_checkpoint.tree,
                    &to_checkpoint.tree,
                    params["ignoreWhitespace"].as_bool().unwrap_or(true),
                )
                .await;
                json!({"fromTurnCount":from,"toTurnCount":to,"threadId":params["threadId"],"diff":diff})
            }
        }
        "orchestration.getFullThreadDiff" => {
            let thread_id = params["threadId"].as_str().unwrap_or_default();
            let to = params["toTurnCount"].as_u64().unwrap_or_default();
            if to == 0 {
                json!({"fromTurnCount":0,"toTurnCount":0,"threadId":params["threadId"],"diff":""})
            } else {
                let Some(thread) = store.threads.get(thread_id).cloned() else {
                    return Reply::Failure(json!({"_tag":"Fail","error":{
                        "_tag":"OrchestrationGetFullThreadDiffError",
                        "message":"Failed to load full thread diff",
                        "cause":{"name":"CheckpointThreadNotFoundError","message":format!("Checkpoint invariant violation in CheckpointDiffQuery.getTurnDiff: Thread '{thread_id}' not found.")}
                    }}));
                };
                let ready = thread
                    .checkpoints
                    .iter()
                    .filter(|checkpoint| checkpoint.ready)
                    .collect::<Vec<_>>();
                let max_turn = ready
                    .iter()
                    .map(|checkpoint| checkpoint.turn_count)
                    .max()
                    .unwrap_or_default();
                if to > max_turn {
                    return Reply::Failure(json!({"_tag":"Fail","error":{
                        "_tag":"CheckpointTurnRangeUnavailableError","threadId":params["threadId"],"requestedTurnCount":to,"availableTurnCount":max_turn
                    }}));
                }
                let Some(from_checkpoint) =
                    ready.iter().find(|checkpoint| checkpoint.turn_count == 0)
                else {
                    return Reply::Failure(json!({"_tag":"Fail","error":{
                        "_tag":"CheckpointRefUnavailableError","threadId":thread_id,"turnCount":0,"checkpoint":"from"
                    }}));
                };
                let Some(to_checkpoint) =
                    ready.iter().find(|checkpoint| checkpoint.turn_count == to)
                else {
                    return Reply::Failure(json!({"_tag":"Fail","error":{
                        "_tag":"CheckpointRefUnavailableError","threadId":thread_id,"turnCount":to,"checkpoint":"to"
                    }}));
                };
                let diff = git_diff_checkpoint(
                    &to_checkpoint.cwd,
                    &from_checkpoint.tree,
                    &to_checkpoint.tree,
                    params["ignoreWhitespace"].as_bool().unwrap_or(true),
                )
                .await;
                json!({"fromTurnCount":0,"toTurnCount":to,"threadId":params["threadId"],"diff":diff})
            }
        }
        "orchestration.getTurnItem" => {
            let thread_id = params["threadId"].as_str().unwrap_or_default();
            if !store.threads.contains_key(thread_id) {
                return Reply::Failure(json!({"_tag":"Fail","error":{
                    "_tag":"OrchestratorProjectionError",
                    "message":format!("Failed to load orchestration projection for thread {thread_id}."),
                    "cause":{"name":"ProjectionStoreThreadNotFoundError","message":format!("No orchestration projection exists for thread {thread_id}.")}
                }}));
            }
            let item_id = params["itemId"].as_str().unwrap_or_default();
            let item = store
                .threads
                .get(thread_id)
                .and_then(|thread| thread.projection().get("turnItems").cloned())
                .and_then(|items| items.as_array().cloned())
                .and_then(|items| items.into_iter().find(|item| item["id"] == item_id));
            json!({"item":item})
        }
        "orchestration.getNativeChildWork" => {
            let thread_id = params["threadId"].as_str().unwrap_or_default();
            if !store.threads.contains_key(thread_id) {
                return Reply::Failure(json!({"_tag":"Fail","error":{
                    "_tag":"OrchestrationV2GetNativeChildWorkError","threadId":thread_id,
                    "cause":{"name":"OrchestratorProjectionError","message":format!("Failed to load orchestration projection for thread {thread_id}."),"cause":{"name":"ProjectionStoreThreadNotFoundError","message":format!("No orchestration projection exists for thread {thread_id}.")}}
                }}));
            }
            json!({"children":[]} )
        }
        "orchestration.getWorkflowScript" => {
            let path = params["scriptPath"].as_str().unwrap_or_default();
            let invalid = !std::path::Path::new(path).is_absolute()
                || std::path::Path::new(path)
                    .extension()
                    .and_then(|e| e.to_str())
                    != Some("js");
            if invalid {
                return Reply::Failure(json!({"_tag":"Fail","error":{
                    "_tag":"OrchestrationGetWorkflowScriptError","reason":"invalid-path",
                    "scriptPath":path,
                    "cause":{"name":"OrchestrationWorkflowFileError",
                        "message":"Workflow files must be absolute paths with the expected extension."}
                }}));
            }
            let root = state.paths.base_dir.join(".claude").join("projects");
            let canonical = match tokio::fs::canonicalize(path).await {
                Ok(path) => path,
                Err(_) => {
                    return Reply::Failure(json!({"_tag":"Fail","error":{
                        "_tag":"OrchestrationGetWorkflowScriptError","reason":"not-found",
                        "scriptPath":path,
                        "cause":{"name":"OrchestrationWorkflowFileError","message":"Workflow file not found."}
                    }}))
                }
            };
            let root = match tokio::fs::canonicalize(root).await {
                Ok(root) => root,
                Err(_) => {
                    return Reply::Failure(json!({"_tag":"Fail","error":{
                        "_tag":"OrchestrationGetWorkflowScriptError","reason":"root-unavailable",
                        "scriptPath":path,
                        "cause":{"name":"OrchestrationWorkflowFileError","message":"Workflow file root unavailable."}
                    }}));
                }
            };
            if canonical != root && !canonical.starts_with(&root) {
                return Reply::Failure(json!({"_tag":"Fail","error":{
                    "_tag":"OrchestrationGetWorkflowScriptError","reason":"outside-root",
                    "scriptPath":canonical,"cause":{"name":"OrchestrationWorkflowFileError","message":"Path is outside the workflow files root."}
                }}));
            }
            let bytes = match tokio::fs::read(&canonical).await {
                Ok(bytes) => bytes,
                Err(_) => {
                    return Reply::Failure(json!({"_tag":"Fail","error":{
                        "_tag":"OrchestrationGetWorkflowScriptError","reason":"read-failed",
                        "scriptPath":canonical,"cause":{"name":"OrchestrationWorkflowFileError","message":"Workflow file read failed."}
                    }}));
                }
            };
            let truncated = bytes.len() > 256 * 1024;
            let contents =
                String::from_utf8_lossy(&bytes[..bytes.len().min(256 * 1024)]).into_owned();
            json!({"scriptPath":canonical,"contents":contents,"truncated":truncated})
        }
        "orchestration.stopWorkflow" => {
            let thread_id = params["threadId"].as_str().unwrap_or_default();
            let exists = store.threads.contains_key(thread_id);
            return Reply::Failure(json!({"_tag":"Fail","error":{
                "_tag":"OrchestrationV2StopWorkflowError","threadId":thread_id,
                "subagentId":params["subagentId"],"reason":if exists {"not-running"} else {"unavailable"}
            }}));
        }
        "orchestration.forceStopThread" => {
            let thread_id = params["threadId"].as_str().unwrap_or_default();
            if !store.threads.contains_key(thread_id) {
                return Reply::Failure(json!({"_tag":"Fail","error":{
                    "_tag":"OrchestrationV2ForceStopThreadError","threadId":thread_id
                }}));
            }
            json!({"outcome":"released","sharedWithThreadIds":[]})
        }
        "orchestration.subscribeShell" => {
            return Reply::Chunk(
                json!([{"kind":"snapshot","snapshot":crate::environment_http::shell(&store,sequence)}]),
            )
        }
        "orchestration.subscribeThread" => {
            return Reply::Chunk(match thread(&params["threadId"]) {
                Some(t) => {
                    json!([{"kind":"snapshot","snapshotSequence":sequence,"projection":t.projection()},{"kind":"synchronized"}])
                }
                None => json!([]),
            })
        }
        "orchestration.subscribeCommandOutput" => {
            return Reply::Chunk(
                json!([{"kind":"replace","text":"","truncated":false,"running":false}]),
            )
        }
        "orchestration.subscribePromptSuggestion" => return Reply::Chunk(json!([null])),
        _ => {
            return Reply::defect(format!(
                "RPC contract is registered but unavailable: {method}"
            ))
        }
    };
    Reply::Success(result)
}

pub(crate) fn fold_project_rows_into_settings(
    settings: &mut Value,
    rows: &indexmap::IndexMap<String, Value>,
) {
    if rows.is_empty() {
        return;
    }
    let Some(mut entries) = settings
        .get("projectSettingsOverrides")
        .and_then(Value::as_object)
        .cloned()
    else {
        return;
    };
    for (project_id, row) in rows {
        if row.get("deletedAt").is_some_and(|value| !value.is_null()) {
            continue;
        }
        let entry = entries
            .entry(project_id.clone())
            .or_insert_with(|| json!({}));
        let Some(entry) = entry.as_object_mut() else {
            continue;
        };
        for (field, value) in [
            ("defaultModelSelection", &row["defaultModelSelection"]),
            ("defaultThreadEnvMode", &row["defaultThreadEnvMode"]),
        ] {
            if !value.is_null()
                && entry.get(field).is_none()
                && (field != "defaultThreadEnvMode"
                    || matches!(value.as_str(), Some("local" | "worktree")))
            {
                entry.insert(field.to_owned(), value.clone());
            }
        }
        if row["autoPull"] == true && entry.get("defaultAutoPull").is_none() {
            entry.insert("defaultAutoPull".into(), json!(true));
        }
        if row["scripts"]
            .as_array()
            .is_some_and(|scripts| !scripts.is_empty())
            && entry.get("defaultProjectScripts").is_none()
        {
            entry.insert("defaultProjectScripts".into(), row["scripts"].clone());
        }
    }
    entries.retain(|_, entry| entry.as_object().is_some_and(|fields| !fields.is_empty()));
    settings["projectSettingsOverrides"] = Value::Object(entries.clone());
    settings["projectSettingsFolded"] = json!(true);
    for (legacy, field) in [
        (
            "projectAgentBrowserAccessOverrides",
            "enableAgentBrowserAccess",
        ),
        ("projectAutoPullOverrides", "defaultAutoPull"),
        ("projectScriptOverrides", "defaultProjectScripts"),
    ] {
        let mut values = serde_json::Map::new();
        for (project_id, entry) in entries.iter() {
            if let Some(value) = entry.get(field) {
                values.insert(project_id.clone(), value.clone());
            }
        }
        settings[legacy] = Value::Object(values);
    }
}

/// The checkpoint diff schemas use non-negative integers and an optional boolean. Most RPC
/// payloads are decoded by the provider-specific handlers, but these two calls return their
/// zero-turn result before touching a projection. Validate them at the wire boundary so malformed
/// inputs retain the TypeScript server's exact Effect defect instead of being coerced to zero.
fn checkpoint_diff_schema_defect(method: &str, params: &Value) -> Option<String> {
    if !matches!(
        method,
        "orchestration.getTurnDiff" | "orchestration.getFullThreadDiff"
    ) {
        return None;
    }
    if let Some(value) = params.get("threadId") {
        if !value.is_string() {
            return Some("Expected string\n  at [\"threadId\"]".into());
        }
    }
    for key in ["fromTurnCount", "toTurnCount"] {
        let Some(value) = params.get(key) else {
            continue;
        };
        if !value.is_number() {
            return Some(format!("Expected number\n  at [\"{key}\"]"));
        }
        let negative = value.as_i64().is_some_and(|number| number < 0)
            || value.as_f64().is_some_and(|number| number < 0.0);
        if negative {
            return Some(format!(
                "Expected a value greater than or equal to 0\n  at [\"{key}\"]"
            ));
        }
        let integer = value.as_u64().is_some()
            || value
                .as_f64()
                .is_some_and(|number| number.is_finite() && number.fract() == 0.0);
        if !integer {
            return Some(format!("Expected an integer\n  at [\"{key}\"]"));
        }
    }
    if method == "orchestration.getTurnDiff" {
        if let (Some(from), Some(to)) = (
            params.get("fromTurnCount").and_then(Value::as_u64),
            params.get("toTurnCount").and_then(Value::as_u64),
        ) {
            if from > to {
                return Some("fromTurnCount must be less than or equal to toTurnCount".into());
            }
        }
    }
    if let Some(value) = params.get("ignoreWhitespace") {
        if !value.is_boolean() {
            return Some("Expected boolean\n  at [\"ignoreWhitespace\"]".into());
        }
    }
    None
}

async fn git_diff_checkpoint(
    cwd: &str,
    from_tree: &str,
    to_tree: &str,
    ignore_whitespace: bool,
) -> String {
    let mut command = tokio::process::Command::new("git");
    command.args(["-C", cwd, "diff", "--binary"]);
    if ignore_whitespace {
        command.arg("--ignore-all-space");
    }
    command.args([from_tree, to_tree]);
    let Ok(output) = command.output().await else {
        return String::new();
    };
    if !output.status.success() {
        return String::new();
    }
    const MAX_OUTPUT: usize = 10_000_000;
    const MARKER: &str = "\n\n[truncated]";
    let text = String::from_utf8_lossy(&output.stdout);
    if text.len() <= MAX_OUTPUT {
        return text.into_owned();
    }
    let limit = MAX_OUTPUT.saturating_sub(MARKER.len());
    let mut end = limit.min(text.len());
    while end > 0 && !text.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}{}", &text[..end], MARKER)
}

async fn scheduled_tasks_snapshot(state: &Arc<AppState>) -> Value {
    let tasks = state
        .scheduled_tasks
        .lock()
        .await
        .values()
        .map(scheduled_public_task)
        .collect::<Vec<_>>();
    json!({"tasks":tasks,"timeZone":scheduled_task_time_zone()})
}

fn scheduled_task_time_zone() -> String {
    std::env::var("TZ")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .or_else(|| {
            std::fs::read_to_string("/etc/timezone")
                .ok()
                .map(|value| value.trim().to_owned())
        })
        .unwrap_or_else(|| "UTC".into())
}

fn scheduled_public_task(task: &Value) -> Value {
    let mut task = task.clone();
    if let Some(object) = task.as_object_mut() {
        object.remove("_webhookToken");
        object.remove("_webhookSecret");
        if let Some(signature) = object
            .get_mut("schedule")
            .and_then(|schedule| schedule.get_mut("signature"))
            .and_then(Value::as_object_mut)
        {
            signature.remove("secret");
            signature.remove("secretRef");
        }
    }
    task
}

fn scheduled_task_error(message: &str, task_id: Option<&str>) -> Reply {
    Reply::Failure(
        json!({"_tag":"Fail","error":{"_tag":"ScheduledTaskError","message":message,"taskId":task_id}}),
    )
}

fn scheduled_task_schedule(input: &Value, existing: Option<&Value>) -> Value {
    let mut schedule = input.clone();
    if schedule["type"] == "webhook" {
        let object = schedule.as_object_mut().expect("schedule object");
        object.entry("signature").or_insert(Value::Null);
        if let Some(signature) = object.get_mut("signature").and_then(Value::as_object_mut) {
            if let Some(header) = signature.get("header").and_then(Value::as_str) {
                signature.insert("header".into(), Value::String(header.to_ascii_lowercase()));
            }
        }
        object.entry("maxDeliveryAgeMinutes").or_insert(Value::Null);
        if !object.contains_key("dedupe") {
            if let Some(value) = existing
                .and_then(|task| task["schedule"].get("dedupe"))
                .filter(|v| !v.is_null())
            {
                object.insert("dedupe".into(), value.clone());
            }
        }
        if !object.contains_key("filters") {
            if let Some(value) = existing
                .and_then(|task| task["schedule"].get("filters"))
                .filter(|v| !v.is_null())
            {
                object.insert("filters".into(), value.clone());
            }
        }
    }
    schedule
}

fn scheduled_task_model_selection(value: &Value) -> Value {
    let mut selection = value.clone();
    if let Some(object) = selection.as_object_mut() {
        object.remove("provider");
        if object.get("options").is_some_and(Value::is_object) {
            object.insert("options".into(), json!([]));
        }
    }
    selection
}

fn scheduled_task_next_run(enabled: bool, schedule: &Value, now: i64) -> Value {
    if !enabled {
        return Value::Null;
    }
    match schedule["type"].as_str() {
        Some("interval") => schedule["everyMs"]
            .as_i64()
            .map(|every| json!(crate::auth_service::iso(now.saturating_add(every))))
            .unwrap_or(Value::Null),
        Some("once") => schedule["runAt"].clone(),
        _ => Value::Null,
    }
}

fn random_webhook_token() -> String {
    use base64::Engine;
    let mut bytes = [0u8; 32];
    rand::rngs::OsRng.fill_bytes(&mut bytes);
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes)
}

fn encode_task_id(value: &str) -> String {
    value
        .replace('%', "%25")
        .replace('/', "%2F")
        .replace(':', "%3A")
}

async fn scheduled_task_upsert(state: &Arc<AppState>, params: &Value) -> Reply {
    let now = crate::auth_service::now();
    let id = params["id"]
        .as_str()
        .map(str::to_owned)
        .or_else(|| {
            params["commandId"]
                .as_str()
                .map(|v| format!("scheduled-task:{v}"))
        })
        .unwrap_or_else(|| format!("scheduled-task:{}", uuid::Uuid::new_v4()));
    let mut tasks = state.scheduled_tasks.lock().await;
    let existing = tasks.get(&id).cloned();
    let thread_mode = params["threadMode"]
        .as_str()
        .map(str::to_owned)
        .or_else(|| {
            existing
                .as_ref()
                .and_then(|v| v["threadMode"].as_str().map(str::to_owned))
        })
        .unwrap_or_else(|| {
            if params["threadId"].is_string() {
                "continue".into()
            } else {
                "new_thread".into()
            }
        });
    let thread_id = if thread_mode == "new_thread" {
        Value::Null
    } else if !params["threadId"].is_null() {
        params["threadId"].clone()
    } else {
        existing
            .as_ref()
            .map(|v| v["threadId"].clone())
            .unwrap_or(Value::Null)
    };
    let schedule = scheduled_task_schedule(&params["schedule"], existing.as_ref());
    let enabled = params["enabled"].as_bool().unwrap_or(false);
    let created_at = existing
        .as_ref()
        .and_then(|v| v["createdAt"].as_str())
        .map(str::to_owned)
        .unwrap_or_else(|| crate::auth_service::iso(now));
    let mut task = json!({
        "id":id,"title":params["title"],"prompt":params["prompt"],"enabled":enabled,"schedule":schedule,
        "projectId":params["projectId"],"threadId":thread_id,"threadMode":thread_mode,"workspaceStrategy":params["workspaceStrategy"],
        "modelSelection":scheduled_task_model_selection(&params["modelSelection"]),"runtimeMode":params["runtimeMode"],"interactionMode":params["interactionMode"],
        "createdBy":existing.as_ref().and_then(|v| v["createdBy"].as_str()).unwrap_or(params["createdBy"].as_str().unwrap_or("user")),
        "creationSource":existing.as_ref().and_then(|v| v["creationSource"].as_str()).unwrap_or(params["creationSource"].as_str().unwrap_or("web")),
        "createdAt":created_at,"updatedAt":crate::auth_service::iso(now),"nextRunAt":scheduled_task_next_run(enabled,&schedule,now),
        "lastRunAt":existing.as_ref().map(|v| v["lastRunAt"].clone()).unwrap_or(Value::Null),"lastRunStatus":existing.as_ref().and_then(|v| v["lastRunStatus"].as_str()).unwrap_or("never"),
        "lastRunError":existing.as_ref().map(|v| v["lastRunError"].clone()).unwrap_or(Value::Null),"runCount":existing.as_ref().and_then(|v| v["runCount"].as_i64()).unwrap_or(0)
    });
    if schedule["type"] == "webhook" {
        let token = existing
            .as_ref()
            .and_then(|v| v["_webhookToken"].as_str())
            .map(str::to_owned)
            .unwrap_or_else(random_webhook_token);
        let secret = schedule["signature"]
            .get("secret")
            .cloned()
            .unwrap_or(Value::Null);
        task["_webhookToken"] = token.clone().into();
        task["_webhookSecret"] = secret.clone();
        task["webhook"] = json!({"path":format!("/api/hooks/{}/{}",encode_task_id(&id),token),"url":null,"hasSecret":!secret.is_null()});
    }
    let public = scheduled_public_task(&task);
    tasks.insert(id, task);
    drop(tasks);
    let _ = state.persist_scheduled_tasks().await;
    let _ = state.scheduled_task_events.send(());
    Reply::Success(json!({"task":public}))
}

async fn scheduled_task_set_enabled(state: &Arc<AppState>, params: &Value) -> Reply {
    let id = params["id"].as_str().unwrap_or_default();
    let enabled = params["enabled"].as_bool().unwrap_or(false);
    let mut tasks = state.scheduled_tasks.lock().await;
    let Some(task) = tasks.get_mut(id) else {
        return scheduled_task_error("Schedule task not found.", Some(id));
    };
    if task["enabled"] == enabled {
        return Reply::Success(json!({"task":scheduled_public_task(task)}));
    }
    let now = crate::auth_service::now();
    task["enabled"] = enabled.into();
    task["updatedAt"] = crate::auth_service::iso(now).into();
    task["nextRunAt"] = scheduled_task_next_run(enabled, &task["schedule"], now);
    let public = scheduled_public_task(task);
    drop(tasks);
    let _ = state.persist_scheduled_tasks().await;
    let _ = state.scheduled_task_events.send(());
    Reply::Success(json!({"task":public}))
}

async fn scheduled_task_run_now(state: &Arc<AppState>, params: &Value) -> Reply {
    let id = params["id"].as_str().unwrap_or_default();
    let mut tasks = state.scheduled_tasks.lock().await;
    let Some(task) = tasks.get_mut(id) else {
        return scheduled_task_error("Schedule task not found.", Some(id));
    };
    if task["schedule"]["type"] == "webhook" {
        return scheduled_task_error(
            "Webhook tasks run when their URL receives a request.",
            Some(id),
        );
    }
    let now = crate::auth_service::now();
    task["lastRunAt"] = crate::auth_service::iso(now).into();
    task["lastRunStatus"] = "failed".into();
    task["lastRunError"] =
        format!("Thread launch scheduled-task:{id}:{now}:manual failed during resolve-project.")
            .into();
    task["runCount"] = task["runCount"]
        .as_i64()
        .unwrap_or(0)
        .saturating_add(1)
        .into();
    task["updatedAt"] = crate::auth_service::iso(now).into();
    let public = scheduled_public_task(task);
    drop(tasks);
    let _ = state.persist_scheduled_tasks().await;
    let _ = state.scheduled_task_events.send(());
    Reply::Success(json!({"task":public}))
}

async fn scheduled_task_rotate_token(state: &Arc<AppState>, params: &Value) -> Reply {
    let id = params["id"].as_str().unwrap_or_default();
    let mut tasks = state.scheduled_tasks.lock().await;
    let Some(task) = tasks.get_mut(id) else {
        return scheduled_task_error("Schedule task not found.", Some(id));
    };
    if task["schedule"]["type"] != "webhook" {
        return scheduled_task_error("Only webhook tasks have a URL token.", Some(id));
    }
    let token = random_webhook_token();
    task["_webhookToken"] = token.clone().into();
    task["updatedAt"] = crate::auth_service::iso(crate::auth_service::now()).into();
    task["webhook"]["path"] = format!("/api/hooks/{}/{}", encode_task_id(id), token).into();
    let public = scheduled_public_task(task);
    drop(tasks);
    let _ = state.persist_scheduled_tasks().await;
    let _ = state.scheduled_task_events.send(());
    Reply::Success(json!({"task":public}))
}

fn invalid_provider_payload_field(method: &str, params: &Value) -> Option<&'static str> {
    let string_fields: &[&str] = match method {
        "mcpApps.callTool" => &["threadId", "itemId", "name"],
        "mcpApps.toolInfo" => &["threadId", "itemId", "name"],
        "mcpApps.readResource" => &["threadId", "itemId", "uri"],
        "mcpApps.updateModelContext" => &["threadId", "itemId", "conversationThreadId"],
        "provider.auth.start"
        | "provider.auth.logout"
        | "provider.auth.subscribe"
        | "provider.install.start"
        | "provider.install.subscribe"
        | "provider.install.remove" => &["instanceId"],
        "provider.auth.complete" => &["instanceId", "flowId", "callbackUrl"],
        "provider.auth.respond" => &["instanceId", "flowId", "interactionId"],
        "provider.auth.cancel" => &["instanceId", "flowId"],
        "provider.auth.removeMethod" => &["instanceId", "methodId"],
        "provider.install.cancel" => &["instanceId", "operationId"],
        "provider.chatgpt.reconnect-profile" => &["instanceId", "methodId"],
        "provider.chatgpt.import-profile" | "provider.chatgpt.handoff.subscribe" => &["instanceId"],
        "provider.uploadFeedback" => &["threadId"],
        _ => &[],
    };
    string_fields
        .iter()
        .copied()
        .find(|field| params.get(*field).is_some_and(|value| !value.is_string()))
}

fn expand_user_path(path: &str) -> PathBuf {
    if path == "~" {
        return std::env::var_os("HOME")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from(path));
    }
    if let Some(rest) = path.strip_prefix("~/") {
        if let Some(home) = std::env::var_os("HOME") {
            return PathBuf::from(home).join(rest);
        }
    }
    PathBuf::from(path)
}

fn proc_memory() -> (u64, u64) {
    let mut total = 0;
    let mut available = 0;
    if let Ok(text) = std::fs::read_to_string("/proc/meminfo") {
        for line in text.lines() {
            let mut fields = line.split_whitespace();
            let value = fields
                .nth(1)
                .and_then(|v| v.parse::<u64>().ok())
                .unwrap_or(0)
                * 1024;
            match line.split(':').next().unwrap_or_default() {
                "MemTotal" => total = value,
                "MemAvailable" => available = value,
                _ => {}
            }
        }
    }
    (available, total)
}

fn resource_group() -> Value {
    json!({"processCount":0,"currentCpuPercent":0,"cpuTimeMs":0,"currentRssBytes":0,"peakRssBytes":0,"ioReadBytes":0,"ioWriteBytes":0,"ioReadBytesPerSecond":0,"ioWriteBytesPerSecond":0,"processStarts":0,"processExits":0})
}

fn resource_snapshot() -> Value {
    json!({"readAt":crate::auth_service::iso(crate::auth_service::now()),"sampleIntervalMs":0,"processes":[],"groups":{"backend":resource_group(),"electron":resource_group(),"monitor":resource_group(),"allT3":resource_group()}})
}

fn resolve_browse_path(params: &Value, partial: &str) -> PathBuf {
    let path = expand_user_path(partial);
    if path.is_absolute() {
        path
    } else if let Some(cwd) = params["cwd"].as_str() {
        expand_user_path(cwd).join(path)
    } else {
        PathBuf::from(partial)
    }
}

#[cfg(test)]
mod scheduled_task_tests {
    use super::*;

    #[test]
    fn interval_schedule_normalizes_task_shape_and_next_run() {
        let schedule = scheduled_task_schedule(&json!({"type":"interval","everyMs":60_000}), None);
        assert_eq!(schedule, json!({"type":"interval","everyMs":60_000}));
        assert_eq!(
            scheduled_task_next_run(true, &schedule, 1_700_000_000_000),
            json!("2023-11-14T22:14:20.000Z")
        );
        assert_eq!(
            scheduled_task_next_run(false, &schedule, 1_700_000_000_000),
            Value::Null
        );
    }

    #[test]
    fn webhook_public_projection_redacts_secret_and_token() {
        let task = json!({
            "id":"scheduled-task:test",
            "_webhookToken":"token",
            "_webhookSecret":"secret",
            "schedule":{"type":"webhook","signature":{
                "header":"x-signature","encoding":"hex","prefix":"","secret":"secret"
            }}
        });
        assert_eq!(
            scheduled_public_task(&task),
            json!({
                "id":"scheduled-task:test",
                "schedule":{"type":"webhook","signature":{
                    "header":"x-signature","encoding":"hex","prefix":""
                }}
            })
        );
    }
}
