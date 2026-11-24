use super::Session;
use crate::contracts::RpcContract;
use serde_json::Value;
const READ: &str = "orchestration:read";
const OPERATE: &str = "orchestration:operate";
const PROVIDERS: &str = "providers:manage";
const SETTINGS: &str = "settings:write";
const FILES: &str = "filesystem:read";

pub(crate) fn legacy_scope(scope: &str) -> &str {
    match scope {
        "filesystem:read" | "diagnostics:read" => READ,
        "settings:write"
        | "providers:manage"
        | "environment:maintain"
        | "preview:operate"
        | "source-control:write"
        | "filesystem:write" => OPERATE,
        "terminal:read" => "terminal:operate",
        // Clients with the original scope list read it as the relay write it refines.
        "relay:configure" => "relay:write",
        s => s,
    }
}

fn references_secret(v: &Value) -> bool {
    match v {
        Value::Object(fields) => {
            fields.get("provider").is_some_and(Value::is_string)
                && fields.get("reference").is_some_and(Value::is_string)
                || fields.values().any(references_secret)
        }
        Value::Array(values) => values.iter().any(references_secret),
        _ => false,
    }
}
pub(crate) fn project_scopes(params: &Value) -> Vec<&'static str> {
    let mut required = vec![OPERATE];
    if params["type"] != "project.delete" && params.get("scripts").is_some() {
        required.push(SETTINGS);
        if references_secret(&params["scripts"]) {
            required.push(PROVIDERS)
        }
    }
    required
}
fn settings_scopes(p: &Value) -> Vec<&'static str> {
    let patch = &p["patch"];
    let mut providers = p.get("providerInstanceMutation").is_some() || references_secret(patch);
    let mut settings = patch["secretProviders"]
        .as_object()
        .is_some_and(|m| m.values().any(Value::is_null));
    let mut access = false;
    for key in patch.as_object().into_iter().flat_map(|m| m.keys()) {
        match key.as_str() {
            "requireConnectDeviceApproval" => access = true,
            "providers" | "providerInstances" | "usageLimitSources" | "textGenerators"
            | "secretProviders" => providers = true,
            _ => settings = true,
        }
    }
    let mut scopes = Vec::new();
    if settings || (!providers && !access) {
        scopes.push(SETTINGS)
    }
    if providers {
        scopes.push(PROVIDERS)
    }
    if access {
        scopes.push("access:write")
    }
    scopes
}
pub(crate) fn missing_rpc(session: &Session, rpc: &RpcContract, p: &Value) -> Option<&'static str> {
    let scopes = match rpc.tag {
        "projects.mutate" => project_scopes(p),
        "server.updateSettings" => settings_scopes(p),
        "server.retryResourceTelemetry" => vec!["environment:maintain", "diagnostics:read"],
        "assets.createUrl"
            if matches!(
                p["resource"]["_tag"].as_str(),
                Some("workspace-file" | "media-file" | "draft-workspace-file")
            ) || p["resource"]["_tag"] == "native-app-icon"
                && p["resource"]["app"]["_tag"] == "app-path" =>
        {
            vec![FILES]
        }
        "device.simlockIdentity" | "device.easAccount" | "server.listTextGeneratorModels"
            if references_secret(p) =>
        {
            vec![SETTINGS, PROVIDERS]
        }
        "git.preparePullRequestThread" if p["mode"] == "worktree" && p["threadId"].is_string() => {
            vec![rpc.scope, OPERATE]
        }
        "git.runStackedAction"
            if p["threadId"].is_string()
                && matches!(p["action"].as_str(), Some("create_pr" | "commit_push_pr")) =>
        {
            vec![rpc.scope, READ]
        }
        "device.list" if p["retryHostId"].is_string() || p["updateTool"].is_string() => {
            vec![OPERATE]
        }
        _ => vec![rpc.scope],
    };
    scopes.into_iter().find(|scope| !session.has(scope))
}
pub(crate) fn http_scopes(path: &str, method: &str) -> Vec<&'static str> {
    match path {
        "/api/connect/link-state" | "/api/connect/reachability" => vec!["relay:read"],
        "/api/connect/link-proof" | "/api/connect/relay-config" => vec!["relay:configure"],
        p if p.starts_with("/api/connect/") => vec!["relay:write"],
        "/api/voice/models/link" => vec![SETTINGS, FILES],
        "/api/voice/models/download"
        | "/api/voice/models/select"
        | "/api/voice/models/cancel"
        | "/api/voice/models/remove"
        | "/api/voice/acceleration"
        | "/api/voice/model-unload-timeout" => vec![SETTINGS],
        p if p.starts_with("/api/voice/cloud/") && p.ends_with("/key") => vec![SETTINGS],
        p if p.starts_with("/api/voice/") => vec![OPERATE],
        "/api/pull-requests/diff" => vec![READ],
        _ if method == "GET" || method == "HEAD" => vec![READ],
        _ => vec![OPERATE],
    }
}
