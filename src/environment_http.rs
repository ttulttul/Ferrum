use crate::AppState;
use axum::{
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode, Uri},
    response::{IntoResponse, Response},
    Json,
};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use serde_json::{json, Value};
use std::{collections::HashMap, fs, io::Write, sync::Arc};
use uuid::Uuid;

pub(crate) fn identity(dir: &std::path::Path) -> anyhow::Result<String> {
    let p = dir.join("environment-id");
    if !p.exists() {
        let mut f = tempfile::NamedTempFile::new_in(dir)?;
        writeln!(f, "{}", Uuid::new_v4())?;
        match f.persist_noclobber(&p) {
            Ok(_) => {}
            Err(e) if e.error.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(e) => return Err(e.into()),
        }
    }
    let id = fs::read_to_string(p)?.trim().to_owned();
    anyhow::ensure!(!id.is_empty(), "empty environment-id");
    Ok(id)
}
pub(super) async fn descriptor(State(s): State<Arc<AppState>>) -> Json<Value> {
    Json(descriptor_value(&s))
}
/// This server's environment descriptor.
pub(crate) fn descriptor_value(s: &AppState) -> Value {
    let label = hostname::get()
        .ok()
        .map(|s| s.to_string_lossy().into_owned())
        .unwrap_or_else(|| "T3 Code".into());
    let os = match std::env::consts::OS {
        "macos" => "darwin",
        "windows" => "windows",
        "linux" => "linux",
        _ => "unknown",
    };
    let arch = match std::env::consts::ARCH {
        "aarch64" => "arm64",
        "x86_64" => "x64",
        _ => "other",
    };
    crate::environment::descriptor(
        &s.environment_id,
        &label,
        (os, arch, crate::machine_kind::current()),
        s.runtime_config.mode == "desktop",
    )
}
fn valid_protocol(h: &HeaderMap) -> bool {
    h.get("x-t3-orchestration-protocol")
        .is_some_and(|v| v == "2")
}
pub(super) async fn shell_snapshot(
    State(state): State<Arc<AppState>>,
    h: HeaderMap,
    Query(query): Query<HashMap<String, String>>,
) -> Response {
    if !valid_protocol(&h) {
        return StatusCode::BAD_REQUEST.into_response();
    }
    let store = state.store.read().await;
    let seq = *state.next_event_id.lock().await;
    let after = query.get("resumeFrom").and_then(|v| v.parse::<u64>().ok());
    if crate::shell_stream::replay(&state, &store, after, seq)
        .await
        .ok()
        .flatten()
        .is_some()
    {
        return Json(json!({"resumable":true})).into_response();
    }
    let snapshot = crate::environment::shell_snapshot(&store, seq, &query);
    Json(snapshot).into_response()
}
fn valid_cursor(value: &str) -> bool {
    if value.len() > 4096 {
        return false;
    }
    let Some(cursor) = URL_SAFE_NO_PAD
        .decode(value)
        .ok()
        .and_then(|bytes| serde_json::from_slice::<Value>(&bytes).ok())
    else {
        return false;
    };
    cursor["v"] == 1
        && cursor["seq"].is_u64()
        && cursor["p"].is_u64()
        && ["st", "si"]
            .iter()
            .all(|k| cursor[*k].as_str().is_some_and(|s| !s.is_empty()))
        && cursor.get("inclusive").is_none_or(Value::is_boolean)
}
pub(super) async fn thread_snapshot(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    h: HeaderMap,
    uri: Uri,
    Query(query): Query<HashMap<String, String>>,
) -> Response {
    if !valid_protocol(&h) {
        return StatusCode::BAD_REQUEST.into_response();
    }
    if uri.path().ends_with("/history") {
        let Some(cursor) = query.get("cursor").filter(|s| !s.trim().is_empty()) else {
            return StatusCode::BAD_REQUEST.into_response();
        };
        if !valid_cursor(cursor) {
            return (StatusCode::BAD_REQUEST,Json(json!({"_tag":"EnvironmentRequestInvalidError","code":"invalid_request","reason":"invalid_history_cursor","traceId":crate::trace_id(&h)}))).into_response();
        }
    }
    let store = state.store.read().await;
    let Some(thread) = store.threads.get(&id) else {
        return (StatusCode::NOT_FOUND,Json(json!({"_tag":"EnvironmentResourceNotFoundError","code":"not_found","reason":"thread_not_found","traceId":crate::trace_id(&h)}))).into_response();
    };
    if uri.path().ends_with("/transcript") {
        return Json(thread.transcript()).into_response();
    }
    let sequence = *state.next_event_id.lock().await;
    if uri.path().ends_with("/bounded") {
        let mut out = thread.stream_snapshot(sequence, &json!({"acceptBoundedSnapshot":true}));
        out.as_object_mut().expect("snapshot").shift_remove("kind");
        return Json(out).into_response();
    }
    Json(json!({"snapshotSequence":sequence,"projection":thread.projection()})).into_response()
}
