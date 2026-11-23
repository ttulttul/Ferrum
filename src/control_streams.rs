//! The control plane's live streams over the workspace (terminals, terminal
//! metadata, previews, devices, VCS status and branch, local servers) as
//! stored [`Subscription`] records.
//!
//! Each record's cursor names where the stream last reported from: for the
//! workspace's event streams, the workspace stream id and the seq of the
//! last frame it handled (`{"stream", "seq"}`), plus what the method needs
//! to tell a change from a repeat; for polled state, the last reading sent.
//! The plane hands every record each [`Signal::Workspace`] frame and, on
//! each alarm, [`Signal::Poll`]. After a restart or a reconnect the
//! workspace's streams have new ids, so a record that sees a new id first
//! catches up from the workspace's current state, sending only what
//! changed since its cursor.
use crate::{
    dispatch::{self, WorkspaceSettings},
    rpc_protocol::Reply,
    subscription::{Signal, Subscription},
    workspace::{RemoteWorkspace, Workspace},
    workspace_protocol::op,
};
use serde_json::{json, Value};

/// The methods served here.
pub(crate) const METHODS: &[&str] = &[
    "terminal.observe",
    "terminal.attach",
    "subscribeTerminalMetadata",
    "subscribePreviewEvents",
    "subscribeDeviceState",
    "subscribeVcsStatus",
    "subscribeVcsBranch",
    "subscribeDiscoveredLocalServers",
    "subscribeProjectClones",
];

pub(crate) fn serves(method: &str) -> bool {
    METHODS.contains(&method)
}

/// The workspace event stream a method follows.
fn bus(method: &str) -> Option<&'static str> {
    Some(match method {
        "terminal.observe" | "terminal.attach" => op::TERM_EVENTS,
        "subscribeTerminalMetadata" => op::TERM_METADATA,
        "subscribePreviewEvents" => op::PREVIEW_EVENTS,
        "subscribeDeviceState" => op::DEVICE_EVENTS,
        "subscribeProjectClones" => op::CLONE_EVENTS,
        _ => return None,
    })
}

fn settings() -> WorkspaceSettings {
    WorkspaceSettings {
        automatic_git_fetch: true,
        ..Default::default()
    }
}

fn failure(error: Value) -> Reply {
    if error["_tag"] == "Die" {
        Reply::Failure(error)
    } else {
        Reply::Failure(json!({"_tag":"Fail","error":error}))
    }
}

/// Open `method`: its record and first batches.
pub(crate) async fn open(
    workspace: &RemoteWorkspace,
    method: &str,
    params: &Value,
) -> Result<(Subscription, Vec<Value>), Reply> {
    // The stream the first batches are current for: later frames of it
    // follow on, and frames of any other stream come after a gap.
    let stream = bus(method).and_then(|op| workspace.bus_stream(op));
    let mut subscription = Subscription {
        method: method.to_owned(),
        params: params.clone(),
        cursor: json!({ "stream": stream, "seq": 0 }),
    };
    let first = match method {
        "terminal.observe" | "terminal.attach" => {
            let name = method.trim_start_matches("terminal.");
            match workspace.terminal(name, params).await {
                Some(Ok(snapshot)) => {
                    subscription.cursor["sequence"] = snapshot["snapshot"]["sequence"].clone();
                    vec![json!([snapshot])]
                }
                Some(Err(error)) => return Err(failure(error)),
                None => return Err(Reply::defect(format!("Unknown stream: {method}"))),
            }
        }
        "subscribeTerminalMetadata" => {
            let snapshot = workspace.terminal_metadata();
            subscription.cursor["terminals"] = snapshot["terminals"].clone();
            vec![json!([snapshot])]
        }
        "subscribePreviewEvents" => Vec::new(),
        "subscribeProjectClones" => {
            // Every clone's snapshot, then the list after each change, as
            // the native server streams them.
            let clones = workspace.clone_snapshots();
            subscription.cursor["clones"] = clones.clone();
            vec![json!([clones])]
        }
        "subscribeDeviceState" => {
            let mut state = workspace.device_state();
            if state.is_null() {
                // Not connected yet: what a workspace with no devices reports.
                state = json!({
                    "supportsHostRetry": true, "supportsToolUpdate": true, "supportsToolInspection": true,
                    "supportsEasHosts": true, "supportsSimlockHosts": true, "hostStatus": "disabled",
                    "hostStatuses": {}, "devices": [], "sessions": [], "onboardingCompleted": false,
                    "agentAccessEnabled": false, "hubBasePath": "/api/device-hub", "revision": 0, "hosts": []
                });
            }
            subscription.cursor["state"] = state.clone();
            vec![json!([state])]
        }
        _ => {
            let reading = poll(workspace, method, params).await?;
            let first = changes(method, None, &reading);
            subscription.cursor = json!({ "previous": reading });
            first
        }
    };
    Ok((subscription, first))
}

async fn poll(workspace: &RemoteWorkspace, method: &str, params: &Value) -> Result<Value, Reply> {
    match dispatch::polled_stream(workspace, &settings(), method, params, false).await {
        Some(Ok(reading)) => Ok(reading),
        Some(Err(error)) => Err(failure(error)),
        None => Err(Reply::defect(format!("Unknown stream: {method}"))),
    }
}

/// The batches a polled stream sends for `reading`, after `previous`, as the
/// native server's polling loop sends them.
fn changes(method: &str, previous: Option<&Value>, reading: &Value) -> Vec<Value> {
    let mut batches = Vec::new();
    match (method, previous) {
        ("subscribeVcsStatus", Some(old)) => {
            if old["local"] != reading["local"] {
                batches.push(json!([{"_tag":"localUpdated","local":reading["local"]}]));
            }
            if old["remote"] != reading["remote"] {
                batches.push(json!([{"_tag":"remoteUpdated","remote":reading["remote"]}]));
            }
        }
        ("subscribeDiscoveredLocalServers", Some(old)) if old["servers"] == reading["servers"] => {}
        (_, Some(old)) if old == reading => {}
        _ => batches.push(json!([reading])),
    }
    batches
}

/// The batches `signal` adds to `subscription`, moving its cursor.
pub(crate) async fn advance(
    workspace: &RemoteWorkspace,
    subscription: &mut Subscription,
    signal: &Signal,
) -> Vec<Value> {
    let method = subscription.method.clone();
    match (bus(&method), signal) {
        (None, Signal::Poll) => {
            // A failed reading (the workspace is not connected) sends
            // nothing; the next one may succeed.
            let Ok(reading) = poll(workspace, &method, &subscription.params).await else {
                return Vec::new();
            };
            let previous = subscription.cursor.get("previous").cloned();
            let batches = changes(&method, previous.as_ref(), &reading);
            subscription.cursor = json!({ "previous": reading });
            batches
        }
        (Some(followed), Signal::Workspace(frame)) if frame.op == followed => {
            let cursor = &mut subscription.cursor;
            let last = cursor["stream"].as_u64();
            if last == Some(frame.stream) && frame.seq <= cursor["seq"].as_u64().unwrap_or(0) {
                return Vec::new();
            }
            // Another stream: the record outlived a workspace connection,
            // and frames may have been missed in between.
            let catch_up = last != Some(frame.stream);
            cursor["stream"] = json!(frame.stream);
            cursor["seq"] = json!(frame.seq);
            bus_event(workspace, subscription, &frame.event, catch_up).await
        }
        _ => Vec::new(),
    }
}

async fn bus_event(
    workspace: &RemoteWorkspace,
    subscription: &mut Subscription,
    event: &Value,
    catch_up: bool,
) -> Vec<Value> {
    let params = subscription.params.clone();
    let cursor = &mut subscription.cursor;
    match subscription.method.as_str() {
        "terminal.observe" | "terminal.attach" => {
            let mut event = event.clone();
            if catch_up {
                // Read the terminal again rather than trust that nothing
                // happened while the stream was down.
                match workspace.terminal("observe", &params).await {
                    Some(Ok(snapshot)) => event = snapshot,
                    _ => return Vec::new(),
                }
            }
            let data = event.get("snapshot").unwrap_or(&event);
            if data["threadId"] != params["threadId"] || data["terminalId"] != params["terminalId"]
            {
                return Vec::new();
            }
            let seq = data["sequence"].as_u64();
            let last = cursor["sequence"].as_u64();
            let snapshot = event["type"] == "snapshot" || event.get("snapshot").is_some();
            if (!snapshot && seq <= last) || (snapshot && seq == last) {
                return Vec::new();
            }
            cursor["sequence"] = json!(seq);
            vec![json!([event])]
        }
        "subscribeTerminalMetadata" => {
            if catch_up || event["type"] == "snapshot" {
                let snapshot = workspace.terminal_metadata();
                if cursor["terminals"] == snapshot["terminals"] {
                    return Vec::new();
                }
                cursor["terminals"] = snapshot["terminals"].clone();
                return vec![json!([snapshot])];
            }
            cursor["terminals"] = workspace.terminal_metadata()["terminals"].clone();
            vec![json!([event])]
        }
        "subscribePreviewEvents" => vec![json!([event])],
        "subscribeProjectClones" => {
            let clones = if catch_up || !event.is_array() {
                workspace.clone_snapshots()
            } else {
                event.clone()
            };
            if cursor["clones"] == clones {
                return Vec::new();
            }
            cursor["clones"] = clones.clone();
            vec![json!([clones])]
        }
        "subscribeDeviceState" => {
            if cursor["state"] == *event {
                return Vec::new();
            }
            cursor["state"] = event.clone();
            vec![json!([event])]
        }
        _ => Vec::new(),
    }
}
