//! RPC dispatch shared by every build.
//!
//! A request is first checked here ([`check`]) the same way on every host:
//! payload shapes the reference server rejects, required keys, and the
//! caller's scopes. Streams over the control state are records
//! ([`crate::subscription`]); workspace methods route through
//! [`Workspace`](crate::workspace::Workspace). A host adds the methods only
//! it serves.
use crate::{
    auth_service::Session,
    rpc_protocol::Reply,
    workspace::{GitTools, Workspace},
    RPC_CONTRACTS,
};
use serde_json::{json, Value};

const PROJECTS_MUTATE_SCHEMA_DEFECT: &str = r#"Expected { readonly "type": "project.create", ... } | { readonly "type": "project.update", ... } | { readonly "type": "project.delete", ... }"#;
const DEVICE_ACTION_SCHEMA_DEFECT: &str = r#"Expected { readonly "type": "setAppearance", ... } | { readonly "type": "setTextSize", ... } | { readonly "type": "setToggle", ... } | { readonly "type": "setLiquidGlass", ... } | { readonly "type": "setColorFilter", ... } | { readonly "type": "setOrientation", ... } | { readonly "type": "setLocation", ... } | { readonly "type": "clearLocation", ... } | { readonly "type": "setPermission", ... } | { readonly "type": "openUrl", ... } | { readonly "type": "launchApp", ... } | { readonly "type": "terminateApp", ... } | { readonly "type": "shake", ... } | { readonly "type": "sendPush", ... }"#;
const ORCHESTRATION_DISPATCH_COMMAND_SCHEMA_DEFECT: &str = r#"Expected { readonly "type": "thread.stop", ... } | { readonly "type": "thread.create", ... } | { readonly "type": "thread.archive", ... } | { readonly "type": "thread.guest-share.close", ... } | { readonly "type": "thread.guest-message.dismiss", ... } | { readonly "type": "thread.unarchive", ... } | { readonly "type": "thread.delete", ... } | { readonly "type": "thread.settle", ... } | { readonly "type": "thread.auto-settle", ... } | { readonly "type": "thread.unsettle", readonly "reason": "user", ... } | { readonly "type": "thread.snooze", ... } | { readonly "type": "thread.unsnooze", readonly "reason": "user", ... } | { readonly "type": "thread.auto-settle.set", ... } | { readonly "type": "thread.pin", ... } | { readonly "type": "thread.unpin", ... } | { readonly "type": "thread.tag.set", ... } | { readonly "type": "thread.pin.reorder", ... } | { readonly "type": "thread.active.reorder", ... } | { readonly "type": "thread.visit", ... } | { readonly "type": "thread.mark-unread", ... } | { readonly "type": "thread.metadata.update", ... } | { readonly "type": "thread.pull-request.link", ... } | { readonly "type": "thread.pull-request.unlink", ... } | { readonly "type": "thread.pull-request-link.sync", ... } | { readonly "type": "thread.issue.link", ... } | { readonly "type": "thread.issue.unlink", ... } | { readonly "type": "thread.pull-request.watch", ... } | { readonly "type": "thread.pull-request.sync", ... } | { readonly "type": "thread.title.regeneration.complete", ... } | { readonly "type": "thread.runtime-mode.set", ... } | { readonly "type": "thread.interaction-mode.set", ... } | { readonly "type": "thread.model-selection.set", ... } | { readonly "type": "provider-session.detach", ... } | { readonly "type": "message.dispatch", ... } | { readonly "type": "prepared-run.release", ... } | { readonly "type": "notification.delivery.accept", ... } | { readonly "type": "prepared-run.progress", ... } | { readonly "type": "prepared-run.fail", ... } | { readonly "type": "prepared-run.retry", ... } | { readonly "type": "subagent.stop", ... } | { readonly "type": "run.interrupt", ... } | { readonly "type": "queued-message.promote-to-steer", ... } | { readonly "type": "queue.resume", ... } | { readonly "type": "queued-run.start", ... } | { readonly "type": "queued-run.reorder", ... } | { readonly "type": "queued-run.cancel", ... } | { readonly "type": "queued-run.edit", ... } | { readonly "type": "runtime-request.respond", ... } | { readonly "type": "thread.user-input.dismiss", ... } | { readonly "type": "checkpoint.rollback", ... } | { readonly "type": "thread.fork", ... } | { readonly "type": "thread.merge_back", ... } | { readonly "type": "delegated_task.request", ... } | { readonly "type": "delegated_task.wake-policy", ... } | { readonly "type": "delegated_task.completion-delivery.acknowledge", ... } | { readonly "type": "delegated_task.completion-delivery.dispose", ... } | { readonly "type": "thread.created.record", ... } | { readonly "type": "provider.switch", ... }"#;

/// Checks a request before dispatch. On success returns the decoded
/// payload for methods whose payload is decoded here (`server.updateSettings`
/// and `server.checkSecretSource`), to use in place of `params`.
pub(crate) fn check(
    principal: &Session,
    method: &str,
    params: &Value,
) -> Result<Option<Value>, Reply> {
    if method == "projects.mutate"
        && !matches!(
            params.get("type").and_then(Value::as_str),
            Some("project.create" | "project.update" | "project.delete")
        )
    {
        return Err(Reply::defect(PROJECTS_MUTATE_SCHEMA_DEFECT.into()));
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
        return Err(Reply::defect(DEVICE_ACTION_SCHEMA_DEFECT.into()));
    }
    if method == "orchestration.dispatchCommand"
        && params
            .get("type")
            .and_then(Value::as_str)
            .and_then(crate::contracts::command_fields)
            .is_none()
    {
        return Err(Reply::defect(
            ORCHESTRATION_DISPATCH_COMMAND_SCHEMA_DEFECT.into(),
        ));
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
        return Err(Reply::defect(
            "Expected { readonly \"type\": \"defer_start\", ... } | { readonly \"type\": \"steer_active\", ... } | { readonly \"type\": \"restart_active\", ... } | { readonly \"type\": \"queue_after_active\", ... } | { readonly \"type\": \"start_immediately\", ... }\n  at [\"dispatchMode\"]".into(),
        ));
    }
    let decoded = if matches!(method, "server.updateSettings" | "server.checkSecretSource") {
        let result = if method == "server.updateSettings" {
            crate::settings::schema::request(params)
        } else {
            crate::settings::schema::secret_check(params)
        };
        match result {
            Ok(v) => Some(v),
            Err(message) => return Err(Reply::defect(message)),
        }
    } else {
        None
    };
    let params = decoded.as_ref().unwrap_or(params);
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
            return Err(Reply::defect(format!("Missing key\n  at [{key:?}]")));
        }
        if let Some(scope) =
            crate::auth_service::permissions::missing_rpc(principal, contract, params)
        {
            let legacy = crate::auth_service::permissions::legacy_scope(scope);
            return Err(Reply::Failure(
                json!({"_tag":"Fail","error":{"_tag":"EnvironmentAuthorizationError","message":format!("The authenticated token is missing required scope: {scope}."),"requiredScope":legacy,"requiredPermission":scope}}),
            ));
        }
    }
    if let Some(defect) = checkpoint_diff_schema_defect(method, params) {
        return Err(Reply::defect(defect));
    }
    if method == "server.searchAcpRegistry"
        && params.get("query").is_some_and(|value| !value.is_string())
    {
        return Err(Reply::defect("Expected string\n  at [\"query\"]".into()));
    }
    if matches!(
        method,
        "orchestration.searchThreads"
            | "orchestration.searchThread"
            | "orchestration.searchThreadStream"
    ) {
        return search_input(method, params)
            .map(Some)
            .map_err(Reply::defect);
    }
    if method == "server.searchAcpRegistry"
        && params.get("limit").is_some_and(|value| {
            value
                .as_u64()
                .is_none_or(|number| !(1..=512).contains(&number))
        })
    {
        return Err(Reply::defect(
            "Expected a value between 1 and 512\n  at [\"limit\"]".into(),
        ));
    }
    if matches!(
        method,
        "server.prepareAcpRegistryAgent" | "server.uninstallAcpRegistryManagedBinary"
    ) {
        match params.get("agentId") {
            Some(value) if !value.is_string() => {
                return Err(Reply::defect("Expected string\n  at [\"agentId\"]".into()));
            }
            // `AcpRegistryAgentId`: trimmed, non-blank, at most 128, lowercase id characters.
            Some(Value::String(id)) => {
                let id = id.trim();
                let defect = if id.is_empty() {
                    Some("Expected a non-blank string".to_owned())
                } else if id.encode_utf16().count() > 128 {
                    Some("Expected a value with a length of at most 128".to_owned())
                } else if !id
                    .chars()
                    .next()
                    .is_some_and(|c| c.is_ascii_lowercase() || c.is_ascii_digit())
                    || !id
                        .chars()
                        .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || "._-".contains(c))
                {
                    Some("Expected a string matching the RegExp ^[a-z0-9][a-z0-9._-]*$".to_owned())
                } else {
                    None
                };
                if let Some(defect) = defect {
                    return Err(Reply::defect(format!("{defect}\n  at [\"agentId\"]")));
                }
            }
            _ => {}
        }
    }
    if method == "server.acceptAcpRegistryUrlAuth"
        && params
            .get("instanceId")
            .is_some_and(|value| !value.is_string())
    {
        return Err(Reply::defect(
            "Expected string\n  at [\"instanceId\"]".into(),
        ));
    }
    if method == "provider.auth.start"
        && params
            .get("callbackMode")
            .is_some_and(|value| !matches!(value.as_str(), Some("server" | "client")))
    {
        return Err(Reply::defect(
            "Expected \"server\" | \"client\"\n  at [\"callbackMode\"]".into(),
        ));
    }
    if method == "mcpApps.updateModelContext"
        && params
            .get("content")
            .is_some_and(|value| !(value.is_array() || value.is_null()))
    {
        return Err(Reply::defect(
            "Expected array | null\n  at [\"content\"]".into(),
        ));
    }
    if let Some(field) = invalid_provider_payload_field(method, params) {
        return Err(Reply::defect(format!(
            "Expected string\n  at [\"{field}\"]"
        )));
    }
    if method == "provider.auth.start"
        && params
            .get("methodId")
            .is_some_and(|value| !value.is_string())
    {
        return Err(Reply::defect("Expected string\n  at [\"methodId\"]".into()));
    }
    if method == "provider.auth.start"
        && params
            .get("returnUrl")
            .is_some_and(|value| !value.is_string())
    {
        return Err(Reply::defect(
            "Expected string\n  at [\"returnUrl\"]".into(),
        ));
    }
    if method == "mcpApps.callTool"
        && params
            .get("arguments")
            .is_some_and(|value| !value.is_object())
    {
        return Err(Reply::defect(
            "Expected object\n  at [\"arguments\"]".into(),
        ));
    }
    if method == "server.checkOtlpEndpoint"
        && !matches!(
            params["signal"].as_str(),
            Some("traces" | "metrics" | "logs")
        )
    {
        return Err(Reply::defect(
            "Expected \"traces\" | \"metrics\" | \"logs\"\n  at [\"signal\"]".into(),
        ));
    }
    if method == "server.prepareTriage" && !params["client"].is_object() {
        return Err(Reply::defect("Expected object\n  at [\"client\"]".into()));
    }
    if method == "server.signalProcess"
        && !matches!(params["signal"].as_str(), Some("SIGINT" | "SIGKILL"))
    {
        return Err(Reply::defect(
            "Expected \"SIGINT\" | \"SIGKILL\"\n  at [\"signal\"]".into(),
        ));
    }
    if method == "server.reportClientActivity"
        && !matches!(
            params["clientKind"].as_str(),
            Some("web" | "desktop-renderer" | "mobile" | "unknown")
        )
    {
        return Err(Reply::defect(
            "Expected \"web\" | \"desktop-renderer\" | \"mobile\" | \"unknown\"\n  at [\"clientKind\"]".into(),
        ));
    }
    if method == "server.getUsageSummary" && !params["sinceDay"].is_string() {
        return Err(Reply::defect("Expected string\n  at [\"sinceDay\"]".into()));
    }
    if method == "provider.auth.respond"
        && params
            .get("response")
            .is_some_and(|value| !value.is_object())
    {
        return Err(Reply::defect("Expected { readonly \"type\": \"browser\", ... } | { readonly \"type\": \"terminal\", ... } | { readonly \"type\": \"credentials\", ... }\n  at [\"response\"]".into()));
    }
    if method == "provider.auth.respond" {
        if let Some(defect) = provider_auth_response_defect(params.get("response")) {
            return Err(Reply::defect(defect));
        }
    }
    Ok(decoded)
}

/// Decode the search contracts before constructing SQL or a timeline cache key.
fn search_input(method: &str, params: &Value) -> Result<Value, String> {
    fn string(v: &Value, path: &str, trim: bool, min: usize, max: usize) -> Result<Value, String> {
        let text = v
            .as_str()
            .ok_or_else(|| format!("Expected string\n  at {path}"))?;
        let text = if trim { text.trim() } else { text };
        let len = text.encode_utf16().count();
        if len < min {
            let expected = if min == 1 {
                "a non-blank string".into()
            } else {
                format!("a value with a length of at least {min}")
            };
            return Err(format!("Expected {expected}\n  at {path}"));
        }
        if len > max {
            return Err(format!(
                "Expected a value with a length of at most {max}\n  at {path}"
            ));
        }
        Ok(json!(text))
    }
    fn integer(v: &Value, path: &str, min: i64, max: i64) -> Result<Value, String> {
        let n = v
            .as_f64()
            .ok_or_else(|| format!("Expected number\n  at {path}"))?;
        if n.fract() != 0. {
            return Err(format!("Expected an integer\n  at {path}"));
        }
        if n < min as f64 || n > max as f64 {
            let expected = if max == i64::MAX {
                format!("a value greater than or equal to {min}")
            } else {
                format!("a value between {min} and {max}")
            };
            return Err(format!("Expected {expected}\n  at {path}"));
        }
        Ok(json!(n as i64))
    }
    let palette = method == "orchestration.searchThreads";
    let mut out = params.clone();
    out["query"] = string(
        &params["query"],
        r#"["query"]"#,
        true,
        if palette { 2 } else { 1 },
        200,
    )?;
    if palette {
        if let Some(limit) = params.get("limit") {
            out["limit"] = integer(limit, r#"["limit"]"#, 1, 50)?;
        }
        for key in ["olderSettledBefore", "environmentLabel"] {
            if let Some(v) = params.get(key) {
                out[key] = string(v, &format!("[{key:?}]"), false, 0, usize::MAX)?;
            }
        }
        if params.get("titlesOnly").is_some_and(|v| !v.is_boolean()) {
            return Err("Expected boolean\n  at [\"titlesOnly\"]".into());
        }
        if let Some(ids) = params.get("projectIds") {
            let ids = ids
                .as_array()
                .ok_or("Expected array\n  at [\"projectIds\"]")?;
            for (i, id) in ids.iter().enumerate() {
                out["projectIds"][i] =
                    string(id, &format!(r#"["projectIds"][{i}]"#), true, 1, usize::MAX)?;
            }
        }
    } else {
        out["threadId"] = string(&params["threadId"], r#"["threadId"]"#, true, 1, usize::MAX)?;
        for (key, min) in [("offset", i64::MIN), ("index", 0)] {
            if let Some(v) = params.get(key) {
                out[key] = integer(v, &format!("[{key:?}]"), min, i64::MAX)?;
            }
        }
        if let Some(start) = params.get("start") {
            for key in ["entryId", "occurrence"] {
                if start.get(key).is_none() {
                    return Err(format!("Missing key\n  at [\"start\"][{key:?}]"));
                }
            }
            out["start"]["entryId"] = string(
                &start["entryId"],
                r#"["start"]["entryId"]"#,
                true,
                1,
                usize::MAX,
            )?;
            out["start"]["occurrence"] = integer(
                &start["occurrence"],
                r#"["start"]["occurrence"]"#,
                0,
                i64::MAX,
            )?;
        }
        if let Some(skills) = params.get("skills") {
            let skills = skills
                .as_array()
                .ok_or("Expected array\n  at [\"skills\"]")?;
            if skills.len() > 1000 {
                return Err(
                    "Expected a value with a length of at most 1000\n  at [\"skills\"]".into(),
                );
            }
            for (i, skill) in skills.iter().enumerate() {
                for key in ["name", "displayName"] {
                    let path = format!(r#"["skills"][{i}][{key:?}]"#);
                    if let Some(v) = skill.get(key) {
                        out["skills"][i][key] = string(v, &path, false, 0, 200)?;
                    } else if key == "name" {
                        return Err(format!("Missing key\n  at {path}"));
                    }
                }
            }
        }
    }
    Ok(out)
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

fn provider_auth_response_defect(response: Option<&Value>) -> Option<String> {
    let response = response?;
    let kind = response.get("type").and_then(Value::as_str);
    match kind {
        Some("browser") => {
            if !matches!(
                response.get("action").and_then(Value::as_str),
                Some("accept" | "decline")
            ) {
                return Some(
                    "Expected \"accept\" | \"decline\"\n  at [\"response\"][\"action\"]".into(),
                );
            }
        }
        Some("terminal") => {
            if response.get("data").and_then(Value::as_str).is_none() {
                return Some("Expected string\n  at [\"response\"][\"data\"]".into());
            }
            if response
                .get("binary")
                .is_some_and(|value| !value.is_boolean())
            {
                return Some("Expected boolean\n  at [\"response\"][\"binary\"]".into());
            }
            if let Some(size) = response.get("size") {
                if !size.is_object() {
                    return Some(
                        "Expected object | undefined\n  at [\"response\"][\"size\"]".into(),
                    );
                }
                for (field, min, max) in [("cols", 1, 500), ("rows", 1, 200)] {
                    if let Some(value) = size.get(field) {
                        let valid = value
                            .as_i64()
                            .is_some_and(|number| (min..=max).contains(&number));
                        if !valid {
                            return Some(format!(
                                "Expected a value between {min} and {max}\n  at [\"response\"][\"size\"][\"{field}\"]"
                            ));
                        }
                    } else {
                        return Some(format!(
                            "Missing key: \"{field}\"\n  at [\"response\"][\"size\"]"
                        ));
                    }
                }
            }
        }
        Some("credentials") => {
            let Some(values) = response.get("values") else {
                return Some("Missing key: \"values\"\n  at [\"response\"]".into());
            };
            if !values.is_object() {
                return Some("Expected object\n  at [\"response\"][\"values\"]".into());
            }
            let Some(values) = values.as_object() else {
                unreachable!();
            };
            if values.len() > 16 {
                return Some("Expected a value with a maximum number of properties of 16\n  at [\"response\"][\"values\"]".into());
            }
            if values.values().any(|value| value.as_str().is_none()) {
                return Some("Expected string\n  at [\"response\"][\"values\"]".into());
            }
            if values.values().any(|value| {
                value
                    .as_str()
                    .is_some_and(|text| text.encode_utf16().count() > 16_384)
            }) {
                return Some("Expected a value with a maximum length of 16384\n  at [\"response\"][\"values\"]".into());
            }
        }
        _ => {
            return Some(
                "Expected \"browser\" | \"terminal\" | \"credentials\"\n  at [\"response\"][\"type\"]".into(),
            );
        }
    }
    None
}

/// What workspace methods take from the host's settings.
#[derive(Clone, Debug, Default)]
pub(crate) struct WorkspaceSettings {
    pub git: GitTools,
    /// `VcsAutomaticFetchInterval` is not zero, so a status read may start a fetch.
    pub automatic_git_fetch: bool,
}

/// Only authenticated thread tool handlers can dispatch browser automation.
pub(crate) async fn mcp_workspace(
    workspace: &dyn Workspace,
    host: &WorkspaceSettings,
    method: &str,
    input: &Value,
) -> Option<Result<Value, Value>> {
    if method == "preview.automation" {
        return workspace.preview("automation", input).await;
    }
    self::workspace(workspace, host, method, input).await
}

/// Dispatch a host-backed unary RPC or obtain the first value of its stream.
pub(crate) async fn workspace(
    workspace: &dyn Workspace,
    host: &WorkspaceSettings,
    method: &str,
    input: &Value,
) -> Option<Result<Value, Value>> {
    let (service, name) = method.split_once('.')?;
    match service {
        "projects" => match name {
            "listEntries" => workspace.list_entries(input).await,
            "listIgnoredFiles" => workspace.list_ignored_files(input).await,
            "searchEntries" => workspace.search_entries(input).await,
            "searchContents" => workspace.search_contents(input).await,
            "readFile" => workspace.read_file(input).await,
            "writeFile" => workspace.write_file(input).await,
            _ => None,
        },
        "filesystem" => match name {
            "getMetadata" => workspace.file_metadata(input).await,
            "browse" => workspace.browse(input).await,
            _ => None,
        },
        "git" if matches!(name, "resolvePullRequest" | "preparePullRequestThread") => {
            Some(workspace.pull_request(name, input, &host.git).await)
        }
        "review" => Some(workspace.review(name, input).await),
        "vcs" => {
            workspace
                .vcs(
                    name,
                    input,
                    name == "refreshStatus" && host.automatic_git_fetch,
                )
                .await
        }
        "terminal" => workspace.terminal(name, input).await,
        "preview" if name == "stopLocalServer" => Some(workspace.stop_local_server(input).await),
        "preview" if matches!(name, "reportProfiles" | "clearProfile") => {
            if name == "clearProfile" {
                if let Some(id) = input["profileId"]
                    .as_str()
                    .filter(|s| !s.trim().is_empty() && *s != "incognito")
                {
                    workspace
                        .close_preview_pages(None, None, Some(id.trim()))
                        .await;
                }
            }
            Some(workspace.preview_profile(name, input).await)
        }
        "preview" if name != "automation" => {
            let reply = workspace.preview(name, input).await;
            if name == "close" && reply.as_ref().is_some_and(Result::is_ok) {
                workspace
                    .close_preview_pages(
                        input["threadId"].as_str().map(str::trim),
                        input["tabId"].as_str().map(str::trim),
                        None,
                    )
                    .await;
            }
            reply
        }
        "device" => workspace.device(name, input).await,
        _ => None,
    }
}

pub(crate) async fn polled_stream(
    workspace: &dyn Workspace,
    host: &WorkspaceSettings,
    method: &str,
    input: &Value,
    initial: bool,
) -> Option<Result<Value, Value>> {
    if method == "subscribeDiscoveredLocalServers" {
        return Some(workspace.local_servers(input).await);
    }
    if !matches!(method, "subscribeVcsBranch" | "subscribeVcsStatus") {
        return None;
    }
    if method == "subscribeVcsBranch" {
        return Some(workspace.vcs_branch(input).await);
    }
    let status = match workspace
        .vcs_status(input, host.automatic_git_fetch, initial)
        .await?
    {
        Ok(status) => status,
        Err(e) => return Some(Err(e)),
    };
    let mut local = serde_json::json!({});
    let mut remote = Value::Null;
    for (key, value) in status.as_object()? {
        // Only a `vcs.refreshStatus` reply carries the refs digest.
        if key == "refsDigest" {
            continue;
        }
        if matches!(
            key.as_str(),
            "hasUpstream" | "aheadCount" | "behindCount" | "aheadOfDefaultCount" | "pr"
        ) {
            remote[key] = value.clone();
        } else {
            if key != "branchChanges" || input["includeRemote"] != false {
                local[key] = value.clone();
            }
        }
    }
    if status["isRepo"] != true {
        remote = Value::Null;
    }
    Some(Ok(
        serde_json::json!({"_tag":"snapshot","local":local,"remote":remote}),
    ))
}
