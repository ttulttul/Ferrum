use crate::{dispatch_orchestration_command, host_rpc, provider_rpc, AppState, RPC_CONTRACTS};
use axum::extract::ws::{Message, WebSocket};
use base64::Engine;
use futures_util::{future::BoxFuture, stream::FuturesUnordered, FutureExt, StreamExt};
use serde_json::{json, Value};
use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::Arc,
};
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
            state.settings.client(references)
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
    if method == "attachments.createUploadUrl" {
        let Some(name) = params["name"].as_str().filter(|s| !s.trim().is_empty()) else {
            return Reply::defect("Missing key\n  at [\"name\"]".into());
        };
        let mime = params["mimeType"]
            .as_str()
            .unwrap_or("application/octet-stream");
        let size = params["sizeBytes"].as_u64().unwrap_or(0);
        let ty = params["type"].as_str().unwrap_or("image");
        let id = format!("pending-{}", uuid::Uuid::new_v4());
        let expires = (std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis()
            + 15 * 60 * 1000) as u64;
        let claims = json!({"version":1,"kind":"attachment-upload","type":ty,"attachmentId":id,"name":name,"mimeType":mime,"sizeBytes":size,"expiresAt":expires});
        let encoded = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .encode(crate::wire_json::compact(&claims));
        return Reply::Success(
            json!({"attachmentId":id,"relativeUrl":format!("/api/attachments/upload/{encoded}.unsigned"),"expiresAt":expires}),
        );
    }
    if method == "assets.createUrl" {
        let Some(resource) = params.get("resource").and_then(Value::as_object) else {
            return Reply::defect("Expected an asset resource\n  at [\"resource\"]".into());
        };
        let tag = resource
            .get("_tag")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let path = resource
            .get("path")
            .and_then(Value::as_str)
            .unwrap_or_default();
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
        let claims = json!({"version":1,"kind":tag,"filePath":file_path.as_ref().map(|p|p.to_string_lossy().into_owned()).unwrap_or_default(),"expiresAt":expires});
        let encoded = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .encode(crate::wire_json::compact(&claims));
        let name = Path::new(path)
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("asset");
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
        let mut result = json!({"relativeUrl":format!("/api/assets/{encoded}.unsigned/{name}"),"expiresAt":expires});
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
                    return Reply::Failure(
                        json!({"_tag":"Fail","error":{"_tag":"ProjectReadFileError","cwd":cwd,"relativePath":relative,"failure":"operation_failed","resolvedPath":target,"operation":"read","operationPath":target,"message":format!("Failed to read workspace file '{relative}' in '{cwd}'."),"cause":{"name":"Error","message":error.to_string()}}}),
                    )
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
        return Reply::Success(
            json!({"trigger":"manual","startedAt":now,"finishedAt":now,"entries":[],"counts":{"removed":0,"kept":0,"failed":0},"omittedCount":0,"bytesFreed":0,"byKind":{}}),
        );
    }
    if method == "server.getStorageCleanupReport" {
        return Reply::Chunk(
            json!([{"trigger":"manual","startedAt":null,"finishedAt":null,"entries":[],"counts":{"removed":0,"kept":0,"failed":0},"omittedCount":0,"bytesFreed":0}]),
        );
    }
    if method == "server.getDatabaseStorage" {
        return Reply::Chunk(
            json!([{"databaseBytes":0,"reclaimableBytes":0,"incrementalVacuumEnabled":true,"compactCommand":format!("t3 doctor --base-dir {}",state.paths.base_dir.display()),"canReclaim":true}]),
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
        let version = tokio::process::Command::new("git")
            .arg("--version")
            .output()
            .await
            .ok()
            .and_then(|o| String::from_utf8(o.stdout).ok())
            .map(|s| s.trim().to_owned());
        return Reply::Success(
            json!({"versionControlSystems":[{"kind":"git","implemented":true,"label":"Git","executable":"git","status":if version.is_some(){"available"}else{"missing"},"version":version.map_or(json!({"_tag":"None"}),|v|json!({"_tag":"Some","value":v}))}]}),
        );
    }
    if method == "server.getTraceDiagnostics" {
        let path = state.paths.logs.join("server.trace.ndjson");
        return Reply::Success(
            json!({"traceFilePath":path,"scannedFilePaths":[],"spans":[],"errors":[]}),
        );
    }
    if method == "server.getProcessDiagnostics" {
        return Reply::Success(
            json!({"serverPid":std::process::id(),"readAt":null,"processCount":0,"totalRssBytes":0,"totalCpuPercent":0,"processes":[],"error":{"_tag":"Some","value":{"message":"Resource diagnostics are unavailable."}}}),
        );
    }
    if method == "server.getHostResources" {
        return Reply::Success(
            json!({"sampledAt":crate::now()*1000,"cpuUtilization":0.0,"cpuCount":std::thread::available_parallelism().map(|n|n.get()).unwrap_or(1),"availableMemoryBytes":0,"totalMemoryBytes":0}),
        );
    }
    if method == "server.retryResourceTelemetry" {
        return Reply::Success(
            json!({"accepted":true,"snapshot":{"readAt":null,"sampleIntervalMs":0,"processes":[],"groups":{}}}),
        );
    }
    if method == "server.refreshUsageRates" {
        return Reply::Success(
            json!({"status":"fresh","source":"https://raw.githubusercontent.com/BerriAI/litellm/main/model_prices_and_context_window.json","fetchedAt":null,"knownModels":0}),
        );
    }
    if method == "scheduledTasks.subscribe" {
        return Reply::Chunk(
            json!([{"tasks":[],"timeZone":std::env::var("TZ").unwrap_or_else(|_| "UTC".into())}]),
        );
    }
    if method == "cloud.getRelayClientStatus" {
        return Reply::Success(
            json!({"status":"unavailable","executablePath":null,"source":null,"version":null}),
        );
    }
    if method == "cloud.installRelayClient" {
        return Reply::Chunk(json!([{"type":"progress","stage":"checking"}]));
    }
    if method == "pullRequests.invalidate" {
        return Reply::Success(Value::Null);
    }
    if method == "pullRequests.subscribeRefreshes" {
        return Reply::Chunk(json!([1]));
    }
    if method == "pullRequests.subscribeLinkedThreadRefreshes" {
        return std::future::pending().await;
    }
    if method == "subscribeProjectClones" {
        return Reply::Chunk(json!([[]]));
    }
    if method == "subscribeServerLifecycle" {
        return Reply::Chunk(json!([{"version":1,"sequence":1,"type":"welcome","payload":{}}]));
    }
    if method == "subscribeResourceTelemetry" {
        return Reply::Chunk(
            json!([{"readAt":null,"sampleIntervalMs":0,"processes":[],"groups":{}}]),
        );
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
        "orchestration.getTurnDiff" => {
            let from = params["fromTurnCount"].as_u64().unwrap_or_default();
            let to = params["toTurnCount"].as_u64().unwrap_or_default();
            if from == to || to == 0 {
                json!({"fromTurnCount":from,"toTurnCount":to,"threadId":params["threadId"],"diff":""})
            } else {
                return Reply::Failure(json!({"_tag":"Fail","error":{
                    "_tag":"CheckpointTurnRangeUnavailableError",
                    "threadId":params["threadId"],"requestedTurnCount":to,"availableTurnCount":0
                }}));
            }
        }
        "orchestration.getFullThreadDiff" => {
            let to = params["toTurnCount"].as_u64().unwrap_or_default();
            if to == 0 {
                json!({"fromTurnCount":0,"toTurnCount":0,"threadId":params["threadId"],"diff":""})
            } else {
                return Reply::Failure(json!({"_tag":"Fail","error":{
                    "_tag":"CheckpointTurnRangeUnavailableError",
                    "threadId":params["threadId"],"requestedTurnCount":to,"availableTurnCount":0
                }}));
            }
        }
        "orchestration.getTurnItem" => json!({"item":null}),
        "orchestration.getNativeChildWork" => json!({"children":[]} ),
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
