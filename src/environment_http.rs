use crate::{AppState, Store};
use axum::{
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode, Uri},
    response::{IntoResponse, Response},
    Json,
};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
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
    let mut capabilities = serde_json::Map::new();
    macro_rules! capability {
        ($name:literal, $value:expr) => {
            capabilities.insert($name.to_owned(), json!($value));
        };
    }
    capability!("repositoryIdentity", true);
    capability!("connectionProbe", true);
    capability!("accessTokenRenewal", true);
    capability!("attachmentUploads", true);
    capability!("questionAttachments", true);
    capabilities.insert("fileAttachments".into(), json!({"maxUploadBytes":52428800}));
    capability!("pullRequests", true);
    capability!("pullRequestChecks", true);
    capability!("issues", true);
    capability!("sideChats", true);
    capability!("vcsBranchStatus", true);
    capability!("inlineMessageContext", true);
    capability!("requiredWorktreeBootstrap", true);
    capability!("threadSettlement", true);
    capability!("threadAutoSettlement", true);
    capability!("storageCleanup", true);
    capability!("storageCleanupRun", true);
    capability!("databaseStorage", true);
    capability!("otlpEndpointCheck", true);
    capability!("secretSourceCheck", true);
    capability!("projectWorktreeCleanup", true);
    capability!("settledWorktreeCleanup", true);
    capability!("worktreesDirectory", true);
    capability!("threadAutoSettlementScope", true);
    capability!("threadRestartContinuation", true);
    capability!("projectSettingsOverrides", true);
    capability!("projectProviderInstanceOverrides", true);
    capability!("threadSnooze", true);
    capability!("threadForceStop", true);
    capability!("environmentThemes", true);
    capability!("usageLimitSources", true);
    capability!("providerWorkspaceSnapshotsOnDemand", true);
    capability!("modelCatalogRefresh", true);
    capability!("usagePriceOverrides", true);
    capability!("usageModelAliases", true);
    capability!("threadPinning", true);
    capability!("threadPinReorder", true);
    capability!("threadActiveReorder", true);
    capability!("threadAutoSettleOptOut", true);
    capability!("threadProjectFromScratch", true);
    capability!("threadTitleRegeneration", true);
    capability!("threadVisitedTracking", true);
    capability!("threadPullRequestLinking", true);
    capability!("serverResolvedCommandContext", true);
    capability!("threadStop", true);
    capability!("localServerStop", true);
    capability!("threadPullRequests", true);
    capability!("threadPullRequestWatch", true);
    capability!("pullRequestStackActions", true);
    if s.runtime_config.mode == "desktop" {
        capability!("serverSelfUpdate", "desktop-managed");
    }
    capability!("npmReleaseLookup", true);
    capability!("agentActivityPublishing", false);
    capability!("projectCloneTracking", true);
    capability!("projectOrganization", true);
    capability!("ignoredFileListing", true);
    capability!("ignoredFileSearch", true);
    capability!("environmentIcon", true);
    capability!("commandOutputStreaming", true);
    capability!("serverBrowser", true);
    capability!("peerLinks", true);
    capability!("mcpModeLimitHeader", true);
    capability!("linkFence", true);
    capability!("mcpApprovalCodes", true);
    capability!("peerLinkPairingCodes", true);
    capability!("mcpHostedApproval", true);
    capability!("triage", true);
    capability!("oneTimeSchedules", true);
    capability!("freshContext", true);
    capability!("scheduledTaskThreadModes", true);
    capability!("voiceTranscription", true);
    capability!("cloudVoiceTranscription", true);
    capability!("shellThreadFamilies", true);
    capability!("shellOlderSettled", true);
    capability!("linkedThreadRefreshes", true);
    capability!("issueLinkedThreads", true);
    Json(
        json!({"environmentId":s.environment_id,"label":label,"platform":{"os":os,"arch":arch,"machine":"desktop"},"serverVersion":"0.1.10","orchestrationProtocolVersion":2,"databaseMigration":98,"capabilities":capabilities}),
    )
}
fn valid_protocol(h: &HeaderMap) -> bool {
    h.get("x-t3-orchestration-protocol")
        .is_some_and(|v| v == "2")
}
pub(super) fn shell(store: &Store, sequence: u64) -> Value {
    let projects = store
        .project_rows
        .values()
        .filter(|r| r["deletedAt"].is_null())
        .map(|row| {
            let mut out = serde_json::Map::new();
            for k in [
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
                if let Some(v) = row.get(k) {
                    out.insert(k.to_owned(), v.clone());
                }
            }
            out
        })
        .collect::<Vec<_>>();
    json!({"schemaVersion":2,"snapshotSequence":sequence,"threads":store.threads.values().filter(|t|!t.archived).map(|t|t.shell()).collect::<Vec<_>>(),"archivedThreads":[],"projects":projects})
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
    if query.get("resumeFrom").and_then(|v| v.parse::<u64>().ok()) == Some(seq) {
        return Json(json!({"resumable":true})).into_response();
    }
    let mut snapshot = shell(&store, seq);
    let mut hash = Sha256::new();
    for p in snapshot["projects"].as_array().unwrap() {
        hash.update(json!([p["id"], p["repositoryIdentity"]]).to_string());
    }
    if query.get("omitOlderSettled").is_some_and(|v| v == "1") {
        snapshot["olderSettled"] = json!({"before":crate::auth_service::iso(crate::auth_service::now()),"asOfSequence":seq,"counts":[]});
    }
    snapshot["projectIdentities"] = json!(URL_SAFE_NO_PAD.encode(hash.finalize()));
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
        return Json(json!({"threadId":thread.id,"title":thread.title,"updatedAt":thread.stored_row()["updatedAt"],"items":thread.messages}))
            .into_response();
    }
    let mut out = json!({"snapshotSequence":*state.next_event_id.lock().await,"projection":thread.projection()});
    if uri.path().ends_with("/bounded") {
        out["historyCursor"] = Value::Null;
        out["hasMoreHistory"] = false.into();
        out["latestLocalTurnOrdinal"] = Value::Null;
    }
    Json(out).into_response()
}
