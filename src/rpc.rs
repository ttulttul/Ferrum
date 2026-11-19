use crate::{dispatch_orchestration_command, host_rpc, provider_rpc, AppState, RPC_CONTRACTS};
use axum::extract::ws::{Message, WebSocket};
use futures_util::{future::BoxFuture, stream::FuturesUnordered, FutureExt, StreamExt};
use serde_json::{json, Value};
use std::{collections::HashMap, sync::Arc};
use tokio::sync::{broadcast, mpsc, oneshot, Semaphore};

enum Reply {
    Success(Value),
    Failure(Value),
    Chunk(Value),
}

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
                                reply = run_call(&state,&principal,&method,&request["payload"], &id, chunks, window) => reply,
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
            .send(Message::Text(response.to_string()))
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
    principal: &crate::auth_service::Session,
    method: &str,
    params: &Value,
    id: &str,
    chunks: mpsc::Sender<(String, Value)>,
    window: Arc<Semaphore>,
) -> Reply {
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
    let send = |values| async {
        if let Ok(permit) = window.acquire().await {
            permit.forget();
            let _ = chunks.send((id.to_owned(), values)).await;
        }
    };
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
            json!({"schemaVersion":2,"snapshotSequence":sequence,"projects":[],"threads":store.threads.values().filter(|t|t.archived).map(|t|t.shell()).collect::<Vec<_>>()})
        }
        "orchestration.searchThreads" => {
            let query = params["query"].as_str().unwrap_or_default().to_lowercase();
            json!({"snapshotSequence":sequence,"threads":store.threads.values().filter(|t| t.title.to_lowercase().contains(&query)).collect::<Vec<_>>(),"complete":true})
        }
        "orchestration.searchThread" => {
            json!({"complete":true,"snapshotSequence":sequence,"totalMatches":0,"activeIndex":0,"match":null})
        }
        "orchestration.getOlderSettledThreads" => {
            json!({"snapshotSequence":sequence,"threads":[],"counts":[]})
        }
        "orchestration.getThreadProjection" => thread(&params["threadId"])
            .map(|t| t.projection())
            .unwrap_or(Value::Null),
        "orchestration.getFullThreadDiff"
        | "orchestration.getTurnDiff"
        | "orchestration.stopWorkflow"
        | "orchestration.forceStopThread"
        | "orchestration.getWorkflowScript"
        | "orchestration.getNativeChildWork"
        | "orchestration.getTurnItem" => Value::Null,
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
