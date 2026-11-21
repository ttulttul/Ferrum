//! Runtime requests: approvals and questions a provider CLI asks during a
//! turn. Like the reference, the workspace never answers one itself unless
//! the reference would (a kind the user already allowed for the session, or
//! an MCP elicitation in full-access mode). Everything else becomes a
//! [`ProviderEvent::RuntimeRequestOpened`] and the turn waits for the user's
//! answer, which arrives through [`respond`].
use crate::workspace::provider::{RuntimeRequestOpen, RuntimeResponse};
use serde_json::{json, Map, Value};
use std::collections::{HashMap, HashSet};
use std::sync::{LazyLock, Mutex};
use tokio::sync::mpsc;

/// The answer channels of running turns, by [`crate::workspace::TurnSpec::turn_id`].
static RESPONDERS: LazyLock<Mutex<HashMap<String, mpsc::UnboundedSender<RuntimeResponse>>>> =
    LazyLock::new(Default::default);

/// Request kinds the user allowed for the rest of a native thread's session
/// (Codex `acceptForSession`), with the runtime mode they were allowed in.
/// Like the reference's, these last as long as this process: a restart asks
/// again.
static SESSION_APPROVALS: LazyLock<Mutex<HashMap<String, Allowance>>> =
    LazyLock::new(Default::default);

/// Register a running turn's answer channel.
pub(super) fn register(turn_id: &str) -> mpsc::UnboundedReceiver<RuntimeResponse> {
    let (sender, receiver) = mpsc::unbounded_channel();
    if !turn_id.is_empty() {
        if let Ok(mut responders) = RESPONDERS.lock() {
            responders.insert(turn_id.to_owned(), sender);
        }
    }
    receiver
}

/// Forget a turn's answer channel once the turn ended.
pub(super) fn unregister(turn_id: &str) {
    if let Ok(mut responders) = RESPONDERS.lock() {
        responders.remove(turn_id);
    }
}

/// Deliver an answer to the running turn `turn_id`. False when no running
/// turn has that id.
pub(crate) fn respond(turn_id: &str, response: RuntimeResponse) -> bool {
    RESPONDERS
        .lock()
        .ok()
        .and_then(|responders| responders.get(turn_id).cloned())
        .is_some_and(|sender| sender.send(response).is_ok())
}

/// Whether `kind` is allowed for the session of `native_thread` in
/// `runtime_mode`. A different mode forgets earlier allowances.
pub(super) fn session_allows(native_thread: &str, runtime_mode: &str, kind: &str) -> bool {
    let Ok(mut approvals) = SESSION_APPROVALS.lock() else {
        return false;
    };
    match approvals.get(native_thread) {
        Some((mode, kinds)) if mode == runtime_mode => kinds.contains(kind),
        Some(_) => {
            approvals.remove(native_thread);
            false
        }
        None => false,
    }
}

/// Allow `kind` for the rest of `native_thread`'s session in `runtime_mode`.
pub(super) fn allow_for_session(native_thread: &str, runtime_mode: &str, kind: &str) {
    if let Ok(mut approvals) = SESSION_APPROVALS.lock() {
        let entry = approvals
            .entry(native_thread.to_owned())
            .or_insert_with(|| (runtime_mode.to_owned(), HashSet::new()));
        if entry.0 != runtime_mode {
            *entry = (runtime_mode.to_owned(), HashSet::new());
        }
        entry.1.insert(kind.to_owned());
    }
}

/// The runtime mode kinds were allowed in, and the kinds.
type Allowance = (String, HashSet<String>);

/// A new request, before its turn item fields are added.
pub(super) fn open(
    native_request_id: &str,
    native_item_id: &str,
    kind: &str,
    runtime_mode: &str,
) -> RuntimeRequestOpen {
    RuntimeRequestOpen {
        id: uuid::Uuid::new_v4().to_string(),
        native_request_id: native_request_id.to_owned(),
        native_item_id: native_item_id.to_owned(),
        kind: kind.to_owned(),
        runtime_mode: runtime_mode.to_owned(),
        supports_feedback: false,
        fields: Map::new(),
    }
}

/// The reference's approval decisions; anything else is a decline.
pub(super) fn decision(response: &RuntimeResponse) -> &str {
    match response.decision.as_deref() {
        Some(
            decision @ ("accept" | "acceptForSession" | "acceptAlways" | "decline" | "cancel"),
        ) => decision,
        _ => "decline",
    }
}

fn trim(value: &Value) -> Option<&str> {
    value
        .as_str()
        .map(str::trim)
        .filter(|text| !text.is_empty())
}

/// The reference's codexFileChangeApprovalPrompt.
pub(super) fn file_change_prompt(params: &Value, changes: Option<&Value>) -> Option<String> {
    if let Some(reason) = trim(&params["reason"]) {
        return Some(reason.to_owned());
    }
    let mut entries: Vec<(&String, &Value)> = changes
        .and_then(Value::as_object)
        .map(|changes| changes.iter().collect())
        .unwrap_or_default();
    entries.sort_by(|a, b| a.0.cmp(b.0));
    if !entries.is_empty() {
        let described: Vec<String> = entries
            .iter()
            .take(20)
            .map(|(path, change)| {
                let kind = change["type"].as_str().unwrap_or("update");
                match change["move_path"].as_str().filter(|_| kind == "update") {
                    Some(to) => format!("{kind} {path} -> {to}"),
                    None => format!("{kind} {path}"),
                }
            })
            .collect();
        let remaining = entries.len() - described.len();
        return Some(if remaining > 0 {
            format!("{}\n+{remaining} more", described.join("\n"))
        } else {
            described.join("\n")
        });
    }
    trim(&params["grantRoot"]).map(str::to_owned)
}

/// The reference's providerRequestKindFromPermissions.
pub(super) fn permissions_kind(permissions: &Value) -> &'static str {
    let file_system = &permissions["fileSystem"];
    let requests = |access: &str| {
        file_system[access]
            .as_array()
            .is_some_and(|paths| !paths.is_empty())
            || file_system["entries"]
                .as_array()
                .is_some_and(|entries| entries.iter().any(|entry| entry["access"] == access))
    };
    if requests("write") {
        "file-change"
    } else if requests("read") {
        "file-read"
    } else {
        "command"
    }
}

/// The reference's permissionsResponseFromDecision.
pub(super) fn permissions_response(decision: &str, permissions: &Value) -> Value {
    match decision {
        "accept" => json!({"permissions": permissions, "scope": "turn"}),
        "acceptForSession" => json!({"permissions": permissions, "scope": "session"}),
        _ => json!({"permissions": {}, "scope": "turn"}),
    }
}

/// The reference's approvalDecisionToLegacyReviewDecision.
pub(super) fn legacy_decision(decision: &str) -> Value {
    match decision {
        "accept" => json!("approved"),
        "acceptForSession" | "acceptAlways" => json!("approved_for_session"),
        "cancel" => json!("abort"),
        _ => json!({"denied": {"rejection": "User declined the request."}}),
    }
}

/// The reference's toCodexUserInputAnswers: each answer as strings, under
/// the CLI's question id.
pub(super) fn codex_answers(answers: &Value, native_ids: &HashMap<String, String>) -> Value {
    let mut out = Map::new();
    for (question, value) in answers.as_object().into_iter().flatten() {
        let Some(native) = native_ids.get(question) else {
            continue;
        };
        let strings: Vec<Value> = match value {
            Value::Array(items) => items.iter().map(|item| json!(stringify(item))).collect(),
            Value::Null => vec![],
            other => vec![json!(stringify(other))],
        };
        out.insert(native.clone(), json!({"answers": strings}));
    }
    Value::Object(out)
}

fn stringify(value: &Value) -> String {
    match value {
        Value::String(text) => text.clone(),
        Value::Null => "null".into(),
        Value::Bool(_) | Value::Number(_) => value.to_string(),
        other => other.to_string(),
    }
}

fn non_empty(value: &Value, fallback: &str) -> String {
    trim(value).unwrap_or(fallback).to_owned()
}

/// Codex questions as the reference projects them, with the CLI's id of
/// each.
pub(super) fn codex_questions(questions: &Value) -> (Value, HashMap<String, String>) {
    let mut native_ids = HashMap::new();
    let projected = questions
        .as_array()
        .into_iter()
        .flatten()
        .enumerate()
        .map(|(index, question)| {
            let id = non_empty(&question["id"], &format!("question-{}", index + 1));
            native_ids.insert(
                id.clone(),
                question["id"].as_str().unwrap_or_default().to_owned(),
            );
            let options: Vec<Value> = question["options"]
                .as_array()
                .into_iter()
                .flatten()
                .enumerate()
                .map(|(option_index, option)| {
                    let label =
                        non_empty(&option["label"], &format!("Option {}", option_index + 1));
                    let description = trim(&option["description"])
                        .map(str::to_owned)
                        .unwrap_or_else(|| option["label"].as_str().unwrap_or_default().to_owned());
                    json!({"label": label, "description": description})
                })
                .collect();
            json!({
                "id": id,
                "header": non_empty(&question["header"], "Question"),
                "question": non_empty(&question["question"], "Choose an answer."),
                "options": options,
            })
        })
        .collect();
    (Value::Array(projected), native_ids)
}

// MCP elicitations, as the reference's CodexMcpElicitation answers them.

fn persistence_decision(value: &str) -> Option<&'static str> {
    let normalized = value.to_lowercase();
    if normalized.contains("session") {
        Some("acceptForSession")
    } else if ["always", "permanent", "forever", "persistent"]
        .iter()
        .any(|word| normalized.contains(word))
    {
        Some("acceptAlways")
    } else {
        None
    }
}

fn form(payload: &Value) -> Option<&Value> {
    let schema = &payload["requestedSchema"];
    (payload["mode"] != "url" && schema.is_object()).then_some(schema)
}

fn field_options(field: &Value) -> Vec<(String, Option<String>)> {
    if let Some(one_of) = field["oneOf"].as_array() {
        return one_of
            .iter()
            .filter_map(|option| {
                Some((
                    option["const"].as_str()?.to_owned(),
                    option["title"].as_str().map(str::to_owned),
                ))
            })
            .collect();
    }
    field["enum"]
        .as_array()
        .into_iter()
        .flatten()
        .enumerate()
        .filter_map(|(index, value)| {
            Some((
                value.as_str()?.to_owned(),
                field["enumNames"][index].as_str().map(str::to_owned),
            ))
        })
        .collect()
}

fn is_persistence_field(key: &str, field: &Value) -> bool {
    persistence_decision(key).is_some()
        || key.eq_ignore_ascii_case("persist")
        || persistence_decision(field["title"].as_str().unwrap_or_default()).is_some()
        || persistence_decision(field["description"].as_str().unwrap_or_default()).is_some()
}

fn is_once_option(value: &str) -> bool {
    static ONCE: LazyLock<regex::Regex> = LazyLock::new(|| {
        regex::Regex::new(
            r"(?i)^(?:once|accept|approve|allow)(?:[_\s-]*(?:once(?:[_\s-]*only)?|this[_\s-]*time))?$",
        )
        .expect("valid pattern")
    });
    ONCE.is_match(value.trim()) && persistence_decision(value).is_none()
}

/// The reference's toMcpElicitationResponse.
pub(super) fn elicitation_response(payload: &Value, decision: &str) -> Value {
    if decision == "decline" || decision == "cancel" {
        return json!({"action": decision});
    }
    if payload["mode"] == "url" {
        return json!({"action": "cancel"});
    }
    let persist = match decision {
        "acceptForSession" => Some("session"),
        "acceptAlways" => Some("always"),
        _ => None,
    };
    let form = form(payload);
    let mut content = Map::new();
    for (key, field) in form
        .and_then(|form| form["properties"].as_object())
        .into_iter()
        .flatten()
    {
        let options = field_options(field);
        let chosen = options.iter().find(|(value, _)| match persist {
            Some(_) => persistence_decision(value) == Some(decision),
            None => is_once_option(value),
        });
        if let Some((value, _)) = chosen {
            content.insert(key.clone(), json!(value));
        } else if field["type"] == "boolean" && is_persistence_field(key, field) {
            content.insert(key.clone(), json!(decision == "acceptAlways"));
        } else if !field["default"].is_null() {
            content.insert(key.clone(), field["default"].clone());
        }
    }
    if form
        .and_then(|form| form["required"].as_array())
        .is_some_and(|required| {
            required
                .iter()
                .any(|key| key.as_str().is_none_or(|key| !content.contains_key(key)))
        })
    {
        return json!({"action": "cancel"});
    }
    let mut response = json!({"action": "accept"});
    if let Some(persist) = persist {
        response["_meta"] = json!({"persist": persist});
    }
    if form.is_some() {
        response["content"] = Value::Object(content);
    }
    response
}

/// The reference's describeMcpElicitation: the app to name and the choices
/// to offer.
pub(super) fn describe_elicitation(payload: &Value) -> (String, Value) {
    let meta = &payload["_meta"];
    let message = payload["message"].as_str().unwrap_or_default();
    let from_message = message
        .strip_prefix("Allow ChatGPT to use ")
        .and_then(|rest| rest.strip_suffix('?'))
        .filter(|app| !app.is_empty());
    let app_name = [
        &meta["app_name"],
        &meta["appName"],
        &meta["app"],
        &meta["target"]["app"],
        &meta["target"]["name"],
        &meta["tool_params"]["app_name"],
        &meta["tool_params"]["app"],
    ]
    .into_iter()
    .find_map(Value::as_str)
    .or(from_message)
    .or(meta["connector_name"].as_str())
    .or(meta["connectorName"].as_str())
    .unwrap_or(payload["serverName"].as_str().unwrap_or_default())
    .to_owned();
    let mut persistence: Vec<(&'static str, String)> = Vec::new();
    let mut set = |decision: &'static str, label: String| match persistence
        .iter_mut()
        .find(|(known, _)| *known == decision)
    {
        Some(entry) => entry.1 = label,
        None => persistence.push((decision, label)),
    };
    let persist: Vec<&str> = match &meta["persist"] {
        Value::String(value) => vec![value.as_str()],
        Value::Array(values) => values.iter().filter_map(Value::as_str).collect(),
        _ => vec![],
    };
    for value in persist {
        if let Some(decision) = persistence_decision(value) {
            set(decision, String::new());
        }
    }
    if meta["allowPersistentApproval"] == true {
        set("acceptAlways", String::new());
    }
    for (key, field) in form(payload)
        .and_then(|form| form["properties"].as_object())
        .into_iter()
        .flatten()
    {
        for (value, label) in field_options(field) {
            if let Some(decision) = persistence_decision(&value) {
                set(decision, label.unwrap_or_default());
            }
        }
        if field["type"] == "boolean" && is_persistence_field(key, field) {
            set(
                "acceptAlways",
                field["title"].as_str().unwrap_or_default().to_owned(),
            );
        }
    }
    let mut options = vec![
        json!({"decision": "cancel", "label": "Cancel"}),
        json!({"decision": "decline", "label": "Decline"}),
    ];
    for (decision, fallback) in [
        ("acceptForSession", "Always allow this session"),
        ("acceptAlways", "Always allow"),
    ] {
        if let Some((_, label)) = persistence.iter().find(|(known, _)| *known == decision) {
            if elicitation_response(payload, decision)["action"] == "accept" {
                let label = if label.is_empty() { fallback } else { label };
                options.push(json!({"decision": decision, "label": label}));
            }
        }
    }
    options.push(json!({"decision": "accept", "label": "Approve"}));
    (app_name, Value::Array(options))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn answers_reach_only_their_turn() {
        let mut receiver = register("run-1");
        assert!(respond(
            "run-1",
            RuntimeResponse {
                id: "r".into(),
                decision: Some("accept".into()),
                ..Default::default()
            }
        ));
        assert_eq!(receiver.try_recv().unwrap().id, "r");
        assert!(!respond("run-2", RuntimeResponse::default()));
        unregister("run-1");
        assert!(!respond("run-1", RuntimeResponse::default()));
    }

    #[test]
    fn session_allowances_end_with_a_mode_change() {
        assert!(!session_allows("thr-a", "full-access", "command"));
        allow_for_session("thr-a", "full-access", "command");
        assert!(session_allows("thr-a", "full-access", "command"));
        assert!(!session_allows("thr-a", "full-access", "file-change"));
        assert!(!session_allows("thr-a", "approval-required", "command"));
        assert!(!session_allows("thr-a", "full-access", "command"));
    }

    #[test]
    fn elicitations_are_answered_as_the_reference_answers_them() {
        let form = json!({"mode": "form", "serverName": "app", "message": "Allow ChatGPT to use Calendar?",
            "requestedSchema": {"properties": {"choice": {"type": "string",
                "enum": ["allow_once", "allow_session"], "enumNames": ["Once", "This session"]}},
                "required": ["choice"]}});
        assert_eq!(
            elicitation_response(&form, "accept"),
            json!({"action": "accept", "content": {"choice": "allow_once"}})
        );
        assert_eq!(
            elicitation_response(&form, "acceptForSession"),
            json!({"action": "accept", "_meta": {"persist": "session"}, "content": {"choice": "allow_session"}})
        );
        assert_eq!(
            elicitation_response(&form, "decline"),
            json!({"action": "decline"})
        );
        assert_eq!(
            elicitation_response(&json!({"mode": "url"}), "accept"),
            json!({"action": "cancel"})
        );
        let (app, options) = describe_elicitation(&form);
        assert_eq!(app, "Calendar");
        assert_eq!(
            options,
            json!([{"decision": "cancel", "label": "Cancel"}, {"decision": "decline", "label": "Decline"},
                {"decision": "acceptForSession", "label": "This session"}, {"decision": "accept", "label": "Approve"}])
        );
    }

    #[test]
    fn prompts_and_answers_follow_the_reference() {
        let changes =
            json!({"b.txt": {"type": "add"}, "a.txt": {"type": "update", "move_path": "c.txt"}});
        assert_eq!(
            file_change_prompt(&json!({}), Some(&changes)).unwrap(),
            "update a.txt -> c.txt\nadd b.txt"
        );
        assert_eq!(
            file_change_prompt(&json!({"reason": " why "}), Some(&changes)).unwrap(),
            "why"
        );
        assert_eq!(
            permissions_kind(&json!({"fileSystem": {"entries": [{"access": "write"}]}})),
            "file-change"
        );
        let (questions, ids) = codex_questions(&json!([{"id": "color", "header": "Color",
            "question": "Which?", "options": [{"label": "Red", "description": ""}]}]));
        assert_eq!(questions[0]["options"][0]["description"], "Red");
        assert_eq!(
            codex_answers(&json!({"color": "Red", "other": "x"}), &ids),
            json!({"color": {"answers": ["Red"]}})
        );
    }
}
