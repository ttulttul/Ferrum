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
/// The reference's migration head, as its descriptor reports it. The native
/// build checks it against the pinned ledger (`statev2::supported_head`).
pub const DATABASE_MIGRATION: i64 = 110;

pub fn descriptor(
    environment_id: &str,
    label: &str,
    platform: (&str, &str, Option<&str>),
    desktop_managed: bool,
) -> Value {
    descriptor_with_link_routes(environment_id, label, platform, desktop_managed, false)
}

/// [`descriptor`] for a server that also serves `/api/links/thread/*` and
/// `/api/peer-link/client-pairing` (`link_routes`), as the native server does,
/// along with its other native-only features (agent feedback).
pub fn descriptor_with_link_routes(
    environment_id: &str,
    label: &str,
    platform: (&str, &str, Option<&str>),
    desktop_managed: bool,
    link_routes: bool,
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
    // Served by the native server's `/api/links/thread/*` and `/api/peer-link/client-pairing`.
    if link_routes {
        capability!("linkThreadStream", true);
        capability!("linkClientPairing", true);
        capability!("linkPairingForLink", true);
    }
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
    // Served by the native server's `src/feedback.rs`, which honours the
    // `agentFeedback` setting (#35871); the control plane has no feedback.
    if link_routes {
        capability!("agentFeedback", true);
    }
    capability!("linkedThreadRefreshes", true);
    capability!("issueLinkedThreads", true);
    json!({"environmentId":environment_id,"label":label,"platform":platform,"serverVersion":"0.1.11","orchestrationProtocolVersion":2,"databaseMigration":DATABASE_MIGRATION,"capabilities":capabilities})
}

pub(crate) fn project_shell(row: &Value) -> Value {
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
        "trackedFolders",
        "pinnedAt",
        "archivedAt",
        "createdAt",
        "updatedAt",
    ] {
        if let Some(v) = row.get(k) {
            out.insert(k.to_owned(), v.clone());
        } else if k == "trackedFolders" {
            out.insert(k.to_owned(), json!([]));
        }
    }
    Value::Object(out)
}

pub(crate) fn shell(store: &Store, sequence: u64) -> Value {
    let projects = store
        .project_rows
        .values()
        .filter(|r| r["deletedAt"].is_null())
        .map(project_shell)
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
    let identities = project_identities(&snapshot["projects"]);
    if query.get("omitOlderSettled").is_some_and(|v| v == "1") {
        snapshot["olderSettled"] = json!({"before":crate::auth_service::iso(crate::auth_service::now()),"asOfSequence":seq,"counts":[]});
    }
    snapshot["projectIdentities"] = json!(identities);
    snapshot
}

pub(crate) fn project_identities(projects: &Value) -> String {
    let mut hash = Sha256::new();
    for p in projects.as_array().into_iter().flatten() {
        hash.update(json!([p["id"], p["repositoryIdentity"]]).to_string());
    }
    URL_SAFE_NO_PAD.encode(hash.finalize())
}

#[cfg(all(test, feature = "native"))]
mod migration_tests {
    #[test]
    fn the_descriptor_reports_the_pinned_migration_head() {
        assert_eq!(super::DATABASE_MIGRATION, crate::statev2::supported_head());
    }

    #[test]
    fn only_a_server_with_the_link_routes_reports_them() {
        let platform = ("linux", "x64", None);
        let served = super::descriptor_with_link_routes("env", "Label", platform, false, true);
        let plain = super::descriptor("env", "Label", platform, false);
        for capability in [
            "linkThreadStream",
            "linkClientPairing",
            "linkPairingForLink",
        ] {
            assert_eq!(served["capabilities"][capability], true, "{capability}");
            assert!(
                plain["capabilities"].get(capability).is_none(),
                "{capability}"
            );
        }
    }
}
