//! The environment descriptor and the shell snapshot: what a client reads
//! before it opens the socket. Platform neutral.
use crate::Store;
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::collections::HashMap;

/// `/.well-known/t3/environment`. `platform` is the workspace host's (os, arch,
/// machine kind) in the client's vocabulary, with no `machine` key when the
/// kind is unknown; `desktop_managed` marks a desktop-owned server.
pub fn descriptor(
    environment_id: &str,
    label: &str,
    platform: (&str, &str, Option<&str>),
    desktop_managed: bool,
) -> Value {
    let (os, arch, machine) = platform;
    let mut platform = json!({"os":os,"arch":arch});
    if let Some(machine) = machine {
        platform["machine"] = json!(machine);
    }
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
    if desktop_managed {
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
    json!({"environmentId":environment_id,"label":label,"platform":platform,"serverVersion":"0.1.10","orchestrationProtocolVersion":2,"databaseMigration":98,"capabilities":capabilities})
}

pub(crate) fn shell(store: &Store, sequence: u64) -> Value {
    let projects = store
        .project_rows
        .values()
        .filter(|r| r["deletedAt"].is_null())
        .map(|row| {
            // Native resolves the identity from the workspace's git remotes; a
            // build without a local workspace keeps the stored value.
            #[cfg(feature = "native")]
            let row = &crate::repository_identity::with_identity(row);
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

/// `GET /api/orchestration/shell` for a store at `sequence`, without the
/// resume short-cut (the caller answers `resumeFrom` at the head itself).
pub(crate) fn shell_snapshot(
    store: &Store,
    sequence: u64,
    query: &HashMap<String, String>,
) -> Value {
    let seq = sequence;
    let mut snapshot = shell(store, seq);
    let mut hash = Sha256::new();
    for p in snapshot["projects"].as_array().unwrap() {
        hash.update(json!([p["id"], p["repositoryIdentity"]]).to_string());
    }
    if query.get("omitOlderSettled").is_some_and(|v| v == "1") {
        snapshot["olderSettled"] = json!({"before":crate::auth_service::iso(crate::auth_service::now()),"asOfSequence":seq,"counts":[]});
    }
    snapshot["projectIdentities"] = json!(URL_SAFE_NO_PAD.encode(hash.finalize()));
    snapshot
}
