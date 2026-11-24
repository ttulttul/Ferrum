//! Agent feedback (`t3_feedback`), as the reference's `FeedbackService` and
//! `FeedbackStore` keep it: an agent's report about T3 Code, scrubbed of
//! anything private, is stored in the shared database's `agent_feedback`
//! table. One the user asked for is first shown in a card in their thread,
//! where they approve it (`agentFeedback.answerRequest`), with or without
//! the conversation. The reference then hands a stored report to its
//! telemetry; this server sends no telemetry, so a report stays on this
//! machine and reads as not sent, as the reference's does with telemetry off.
mod scrub;
mod trace;

use crate::AppState;
use rusqlite::{params, Connection, OptionalExtension};
use serde_json::{json, Value};
use std::sync::{Arc, LazyLock};

const CARD_PREFIX: &str = "turn-item:feedback-request:";
const MAX_REPORTS_PER_TURN: i64 = 3;
const MAX_REPORTS_OUTSIDE_TURNS_PER_HOUR: i64 = 10;
const OFF_MESSAGE: &str =
    "Agent feedback is off. Turn it on in T3 Code under Settings > Labs, then try again.";
const SUBJECT_MESSAGE: &str =
    "Set subject to the exact T3 tool name, the t3 command, or `general`.";
const APPROVAL_MESSAGE: &str = "A report the user asks for is shown to them for approval in their thread, and this call has no active thread turn, so nothing was sent.";
const STORAGE_MESSAGE: &str = "Could not save agent feedback.";
const AWAITING_APPROVAL_NOTE: &str = "The user sees this report in a card and chooses there whether to send it and whether to include the conversation. Nothing is sent until they do, and you will not learn their choice. Do not wait for it or send it another way. Call again only if the user asks to change it; that shows a new card.";

/// `T3_CLI_COMMANDS`: the `t3` command paths a report may name.
const T3_CLI_COMMANDS: &[&str] = &[
    "t3",
    "t3 help",
    "t3 serve",
    "t3 app",
    "t3 pair",
    "t3 auth",
    "t3 auth connect-device",
    "t3 auth connect-device approve",
    "t3 auth connect-device list",
    "t3 auth connect-device revoke",
    "t3 auth pairing",
    "t3 auth pairing create",
    "t3 auth pairing list",
    "t3 auth pairing revoke",
    "t3 auth session",
    "t3 auth session issue",
    "t3 auth session list",
    "t3 auth session revoke",
    "t3 environment",
    "t3 environment link",
    "t3 environment list",
    "t3 environment unlink",
    "t3 browser",
    "t3 browser setup",
    "t3 project",
    "t3 project add",
    "t3 project remove",
    "t3 project rename",
    "t3 thread",
    "t3 thread list",
    "t3 thread start",
    "t3 thread send",
    "t3 thread show",
    "t3 feedback",
    "t3 feedback list",
    "t3 workflow",
    "t3 workflow start",
    "t3 workflow list",
    "t3 workflow show",
    "t3 workflow resume",
    "t3 workflow cancel",
    "t3 workflow signal",
    "t3 workflow questions",
    "t3 workflow hooks",
    "t3 workflow store",
    "t3 workflow store list",
    "t3 workflow store set",
    "t3 workflow store delete",
    "t3 up",
    "t3 status",
    "t3 service",
    "t3 service install",
    "t3 service restart",
    "t3 service uninstall",
    "t3 service status",
    "t3 service protect",
    "t3 update",
    "t3 install",
    "t3 uninstall",
    "t3 theme",
    "t3 theme set",
    "t3 theme clear",
    "t3 theme show",
    "t3 trace",
    "t3 trace summary",
    "t3 doctor",
    "t3 policy",
    "t3 policy show",
    "t3 policy check",
    "t3 policy templates",
    "t3 policy settings",
    "t3 triage",
    "t3 connect",
    "t3 connect link",
    "t3 connect login",
    "t3 connect logout",
    "t3 connect publish",
    "t3 connect status",
    "t3 connect unlink",
];

/// Every T3 tool the reference registers, which a report's subject may name.
fn t3_tools() -> Vec<String> {
    static NAMES: LazyLock<Vec<String>> = LazyLock::new(|| {
        let tools: Value =
            serde_json::from_str(include_str!("mcp_oauth/tools.json")).expect("tools");
        tools["tools"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|t| t["name"].as_str().map(str::to_owned))
            .collect()
    });
    NAMES.clone()
}

/// `resolveT3ToolName`: the bare name of a T3 tool however a provider prefixed it.
pub(crate) fn resolve_t3_tool_name(name: &str, tools: &[String]) -> Option<String> {
    static PREFIX: LazyLock<regex::Regex> = LazyLock::new(|| {
        regex::Regex::new(
            r"(?i)^(?:mcp__t3[-_]?code(?:[-_]direct)?__|t3[-_]?code(?:[-_]direct)?[.:/])",
        )
        .expect("pattern")
    });
    let bare = PREFIX.replace(name, "").into_owned();
    if tools.contains(&bare) {
        return Some(bare);
    }
    // OpenCode 2 names tools `t3-code-<thread>_<tool>`: the longest name wins.
    if !name.to_ascii_lowercase().starts_with("t3-code-") {
        return None;
    }
    tools
        .iter()
        .filter(|tool| name.ends_with(&format!("_{tool}")))
        .max_by_key(|tool| tool.len())
        .cloned()
}

/// `resolveFeedbackSubject`.
fn resolve_subject(value: &str) -> Option<String> {
    let subject = value.trim();
    if subject == "general" || T3_CLI_COMMANDS.contains(&subject) {
        return Some(subject.to_owned());
    }
    resolve_t3_tool_name(subject, &t3_tools())
}

/// A failure as `t3_feedback` answers it: its code and message.
pub(crate) type Refused = (&'static str, String);

fn refused(message: impl Into<String>) -> Refused {
    ("invalid_request", message.into())
}

fn storage() -> Refused {
    ("orchestration_error", STORAGE_MESSAGE.to_owned())
}

/// Serializes the limit checks with their writes (`lock`), and answers (`answering`).
static LOCK: LazyLock<tokio::sync::Mutex<()>> = LazyLock::new(Default::default);
static ANSWERING: LazyLock<tokio::sync::Mutex<()>> = LazyLock::new(Default::default);

fn open(state: &AppState) -> Result<Connection, Refused> {
    let conn = Connection::open_with_flags(
        state.paths.state_dir.join(crate::statev2::FILE_NAME),
        rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE,
    )
    .map_err(|_| storage())?;
    crate::statev2::configure(&conn, false).map_err(|_| storage())?;
    Ok(conn)
}

fn mode(state: &AppState) -> String {
    state.settings.internal().unwrap_or_default()["agentFeedback"]["mode"]
        .as_str()
        .unwrap_or("off")
        .to_owned()
}

/// `allowedBy`: nothing while Off, only approved reports while When I ask.
fn allowed_by(mode: &str, user_requested: bool, source: &str) -> Result<(), Refused> {
    if mode == "off" {
        return Err(refused(OFF_MESSAGE));
    }
    if mode == "on-request" && !user_requested {
        return Err(refused(if source == "cli" {
            "Agent feedback is set to When I ask: ask your agent in a thread to send it, so you can approve what is sent."
        } else {
            "Agent feedback is set to When I ask: send a report only when the user asks for one."
        }));
    }
    Ok(())
}

/// A UUID v7: the time in its first 48 bits, the rest random.
fn uuid_v7() -> String {
    let millis = crate::platform::now_ms().max(0) as u64;
    let mut bytes: [u8; 16] = crate::platform::random_bytes();
    bytes[..6].copy_from_slice(&millis.to_be_bytes()[2..]);
    bytes[6] = (bytes[6] & 0x0f) | 0x70;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    uuid::Uuid::from_bytes(bytes).hyphenated().to_string()
}

fn now_iso() -> String {
    crate::auth_service::iso(crate::auth_service::now())
}

/// `overLimit`: the limit the next unprompted report would pass.
fn over_limit(conn: &Connection, run_id: Option<&str>) -> rusqlite::Result<Option<&'static str>> {
    match run_id {
        Some(run_id) => {
            let count: i64 = conn.query_row(
                "SELECT COUNT(*) FROM agent_feedback WHERE run_id = ?1 AND user_requested = 0",
                params![run_id],
                |row| row.get(0),
            )?;
            Ok((count >= MAX_REPORTS_PER_TURN).then_some("turn"))
        }
        None => {
            let since = crate::auth_service::iso(crate::auth_service::now() - 60 * 60 * 1000);
            let count: i64 = conn.query_row(
                "SELECT COUNT(*) FROM agent_feedback WHERE run_id IS NULL AND created_at > ?1",
                params![since],
                |row| row.get(0),
            )?;
            Ok((count >= MAX_REPORTS_OUTSIDE_TURNS_PER_HOUR).then_some("hour"))
        }
    }
}

fn limit_message(scope: &str) -> String {
    match scope {
        "turn" => format!("This turn already sent {MAX_REPORTS_PER_TURN} agent feedback reports; nothing was sent."),
        "cards" => format!("This turn already showed the user {MAX_REPORTS_PER_TURN} agent feedback reports to approve; nothing was shown."),
        _ => format!("This machine already sent {MAX_REPORTS_OUTSIDE_TURNS_PER_HOUR} agent feedback reports outside a turn in the last hour; nothing was sent."),
    }
}

/// `insert` then `deliver`: the report stored, marked sent before the
/// hand-off, then cleared as this server's telemetry refuses it. `Err` holds
/// the refusal; `Ok(false)` that it was not sent.
fn insert(state: &AppState, report: &Value, run_id: Option<&str>) -> Result<bool, Refused> {
    let mut conn = open(state)?;
    let tx = conn.transaction().map_err(|_| storage())?;
    let user_requested = report["userRequested"] == true;
    if !user_requested {
        if let Some(scope) = over_limit(&tx, run_id).map_err(|_| storage())? {
            return Err(refused(limit_message(scope)));
        }
    }
    allowed_by(
        &mode(state),
        user_requested,
        report["source"].as_str().unwrap_or("mcp"),
    )?;
    let trace = (!report["trace"].is_null()).then(|| report["trace"].to_string());
    tx.execute(
        "INSERT INTO agent_feedback (id, thread_id, run_id, created_at, kind, subject, expected, actual, workaround, user_requested, trace, source, sent) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, 1)",
        params![
            report["id"].as_str(),
            report["threadId"].as_str(),
            run_id,
            report["createdAt"].as_str(),
            report["kind"].as_str(),
            report["subject"].as_str(),
            report["expected"].as_str(),
            report["actual"].as_str(),
            report["workaround"].as_str(),
            user_requested as i64,
            trace,
            report["source"].as_str(),
        ],
    )
    .map_err(|_| storage())?;
    tx.commit().map_err(|_| storage())?;
    // This server sends no telemetry (`analytics.record` refuses), so the report is not sent.
    conn.execute(
        "UPDATE agent_feedback SET sent = 0 WHERE id = ?1",
        params![report["id"].as_str()],
    )
    .map_err(|_| storage())?;
    Ok(false)
}

/// What a submission names: the caller's thread (if any) and source.
pub(crate) struct Caller {
    pub source: &'static str,
    pub thread_id: Option<String>,
}

/// The values the server knows are private to the caller (`sensitive`).
fn sensitive(
    thread_id: Option<&str>,
    thread: Option<&Value>,
    project: Option<&Value>,
) -> Vec<String> {
    let text = |v: &Value| v.as_str().unwrap_or_default().to_owned();
    let home = std::env::var("HOME").unwrap_or_default();
    let user = std::env::var("USER")
        .or_else(|_| std::env::var("USERNAME"))
        .unwrap_or_default();
    let host = hostname::get()
        .map(|h| h.to_string_lossy().into_owned())
        .unwrap_or_default();
    let thread = thread.cloned().unwrap_or(Value::Null);
    let project = project.cloned().unwrap_or(Value::Null);
    let identity = &project["repositoryIdentity"];
    let owner_name = match (identity["owner"].as_str(), identity["name"].as_str()) {
        (Some(owner), Some(name)) => format!("{owner}/{name}"),
        _ => String::new(),
    };
    vec![
        thread_id.unwrap_or_default().to_owned(),
        text(&thread["projectId"]),
        home,
        user,
        host,
        text(&thread["worktreePath"]),
        text(&thread["branch"]),
        text(&project["workspaceRoot"]),
        text(&project["title"]),
        text(&identity["locator"]["remoteUrl"]),
        text(&identity["origin"]["remoteUrl"]),
        text(&identity["webUrl"]),
        text(&identity["canonicalKey"]),
        owner_name,
    ]
}

/// `FeedbackService.submit`.
pub(crate) async fn submit(
    state: &Arc<AppState>,
    input: &Value,
    caller: Caller,
) -> Result<Value, Refused> {
    let user_requested = input["userRequested"] == true;
    allowed_by(&mode(state), user_requested, caller.source)?;
    let subject = resolve_subject(input["subject"].as_str().unwrap_or_default())
        .ok_or_else(|| refused(SUBJECT_MESSAGE))?;
    if user_requested && caller.thread_id.is_none() {
        return Err(refused(APPROVAL_MESSAGE));
    }
    let (thread, project, projection) = {
        let store = state.control.store.read().await;
        let thread = caller
            .thread_id
            .as_deref()
            .and_then(|id| store.threads.get(id));
        let row = thread.map(|t| t.shell());
        let project = thread
            .and_then(|t| store.projects.get(&t.project_id))
            .map(|p| {
                crate::repository_identity::with_identity(&json!({
                    "id": p.id, "title": p.name, "workspaceRoot": p.path,
                }))
            });
        (row, project, thread.map(|t| t.full_projection()))
    };
    let private = sensitive(
        caller.thread_id.as_deref(),
        thread.as_ref(),
        project.as_ref(),
    );
    let field = |key: &str| {
        json!(scrub::scrub(
            input[key].as_str().unwrap_or_default(),
            &private
        ))
    };
    let fields = json!({
        "kind": input["kind"],
        "subject": subject,
        "expected": field("expected"),
        "actual": field("actual"),
        "workaround": field("workaround"),
    });
    let id = uuid_v7();
    let run_id = thread
        .as_ref()
        .and_then(|t| t["activeRunId"].as_str())
        .map(str::to_owned);
    if !user_requested {
        let mut report = fields.clone();
        report["trace"] = Value::Null;
        report["userRequested"] = json!(false);
        report["source"] = json!(caller.source);
        report["id"] = json!(id);
        report["threadId"] = json!(caller.thread_id);
        report["createdAt"] = json!(now_iso());
        let sent = {
            let _held = LOCK.lock().await;
            insert(state, &report, run_id.as_deref())?
        };
        report["sent"] = json!(sent);
        return Ok(report);
    }
    let thread_id = caller.thread_id.clone().unwrap_or_default();
    let projection = projection.unwrap_or(Value::Null);
    let runs: Vec<Value> = projection["runs"].as_array().cloned().unwrap_or_default();
    let run = run_id
        .as_deref()
        .and_then(|id| runs.iter().find(|r| r["id"] == id))
        .filter(|r| !r["rootNodeId"].is_null())
        .cloned()
        .ok_or_else(|| refused(APPROVAL_MESSAGE))?;
    let items: Vec<Value> = projection["turnItems"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|item| trace::ITEM_TYPES.contains(&item["type"].as_str().unwrap_or_default()))
        .cloned()
        .collect();
    let providers = crate::mcp_oauth::McpHost::providers(&**state);
    let driver_of = |run: &Value| {
        providers
            .as_array()
            .into_iter()
            .flatten()
            .find(|p| p["instanceId"] == run["providerInstanceId"])
            .and_then(|p| p["driver"].as_str().map(str::to_owned))
    };
    let turns = input["turns"].as_u64().unwrap_or(1).max(1);
    let tools = t3_tools();
    let build = |with_text: bool| {
        trace::build(&trace::TraceInput {
            runs: &runs,
            items: &items,
            turns,
            with_text,
            sensitive: &private,
            t3_tools: &tools,
            driver_of: &driver_of,
            submitting_run_id: run["id"].as_str().unwrap_or_default(),
        })
    };
    let (outline, conversation) = (build(false), build(true));
    {
        let _held = LOCK.lock().await;
        let shown = {
            let store = state.control.store.read().await;
            store.threads.get(&thread_id).map_or(0, |t| {
                t.link_requests
                    .iter()
                    .filter(|c| c["type"] == "feedback_request" && c["runId"] == run["id"])
                    .count()
            })
        };
        if shown as i64 >= MAX_REPORTS_PER_TURN {
            return Err(refused(limit_message("cards")));
        }
        let mut preview = fields;
        preview["trace"] = outline;
        let mut command = json!({
            "type": "feedback_request.record",
            "commandId": format!("feedback-request:{id}:pending"),
            "threadId": thread_id,
            "runId": run["id"],
            "nodeId": run["rootNodeId"],
            "turnItemId": format!("{CARD_PREFIX}{id}"),
            "preview": preview,
            "feedbackStatus": "pending",
        });
        if conversation["textTurns"].as_u64().unwrap_or(0) > 0 {
            command["conversationTrace"] = conversation;
        }
        crate::dispatch_orchestration_command(state.clone(), command)
            .await
            .map_err(|_| storage())?;
    }
    Ok(json!({"status": "awaiting_approval", "note": AWAITING_APPROVAL_NOTE}))
}

/// `AgentFeedbackRequestError`.
pub(crate) fn request_error(reason: &str) -> Value {
    json!({"_tag": "AgentFeedbackRequestError", "reason": reason})
}

/// `FeedbackStore.answer` (`agentFeedback.answerRequest`): Send stores the
/// report the card shows; either answer closes it, once.
pub(crate) async fn answer(state: &Arc<AppState>, params: &Value) -> Result<(), Value> {
    let thread_id = params["threadId"].as_str().unwrap_or_default().to_owned();
    let card_id = params["turnItemId"].as_str().unwrap_or_default().to_owned();
    let card = {
        let store = state.control.store.read().await;
        let Some(thread) = store.threads.get(&thread_id) else {
            let mut error = request_error("load_failed");
            error["cause"] = json!({
                "name": "OrchestratorProjectionError",
                "message": format!("Failed to load orchestration projection for thread {thread_id}."),
                "cause": {
                    "name": "ProjectionStoreThreadNotFoundError",
                    "message": format!("No orchestration projection exists for thread {thread_id}."),
                },
            });
            return Err(error);
        };
        thread
            .link_requests
            .iter()
            .find(|c| c["id"] == card_id.as_str())
            .filter(|c| {
                c["type"] == "feedback_request"
                    && c["threadId"] == thread_id.as_str()
                    && !c["runId"].is_null()
                    && !c["nodeId"].is_null()
                    && card_id.starts_with(CARD_PREFIX)
            })
            .cloned()
            .ok_or_else(|| request_error("not_found"))?
    };
    if card["feedbackStatus"] != "pending" {
        return Err(request_error("already_answered"));
    }
    let report_id = card_id[CARD_PREFIX.len()..].to_owned();
    let settle = |status: &str, trace: Value| {
        let mut preview = card["preview"].clone();
        preview["trace"] = trace;
        json!({
            "type": "feedback_request.record",
            "commandId": format!("feedback-request:{report_id}:{status}"),
            "threadId": thread_id,
            "runId": card["runId"],
            "nodeId": card["nodeId"],
            "turnItemId": card_id,
            "preview": preview,
            "feedbackStatus": status,
        })
    };
    let record_failed = || request_error("record_failed");
    let _answering = ANSWERING.lock().await;
    let waiting = {
        let store = state.control.store.read().await;
        store.threads.get(&thread_id).is_some_and(|t| {
            t.link_requests
                .iter()
                .any(|c| c["id"] == card_id.as_str() && c["status"] == "waiting")
        })
    };
    if !waiting {
        return Err(request_error("already_answered"));
    }
    let dispatch = |command: Value| crate::dispatch_orchestration_command(state.clone(), command);
    let preview_trace = card["preview"]["trace"].clone();
    // Stored before its card was settled: settle it now.
    let stored = open(state)
        .map_err(|_| record_failed())?
        .query_row(
            "SELECT sent, trace FROM agent_feedback WHERE id = ?1",
            params![report_id],
            |row| Ok((row.get::<_, i64>(0)?, row.get::<_, Option<String>>(1)?)),
        )
        .optional()
        .map_err(|_| record_failed())?;
    if let Some((sent, trace)) = stored {
        let command = if sent != 1 {
            settle("cancelled", preview_trace)
        } else {
            let trace = trace
                .and_then(|t| serde_json::from_str(&t).ok())
                .unwrap_or(Value::Null);
            settle("sent", trace)
        };
        return dispatch(command)
            .await
            .map(|_| ())
            .map_err(|_| record_failed());
    }
    if params["answer"] == "decline" {
        return dispatch(settle("declined", preview_trace))
            .await
            .map(|_| ())
            .map_err(|_| record_failed());
    }
    let trace = if params["includeConversation"] == true && card["conversationTrace"].is_object() {
        card["conversationTrace"].clone()
    } else {
        preview_trace.clone()
    };
    let mut report = card["preview"].clone();
    report["trace"] = trace.clone();
    report["userRequested"] = json!(true);
    report["source"] = json!("mcp");
    report["id"] = json!(report_id);
    report["threadId"] = json!(thread_id);
    report["createdAt"] = json!(now_iso());
    match insert(state, &report, card["runId"].as_str()) {
        Err((_, message)) if message == OFF_MESSAGE => {
            dispatch(settle("cancelled", preview_trace))
                .await
                .map_err(|_| record_failed())?;
            Err(request_error("feedback_off"))
        }
        Err(_) => Err(record_failed()),
        // Telemetry off keeps the report on this machine, so the card says not sent.
        Ok(sent) => {
            let command = if sent {
                settle("sent", trace)
            } else {
                settle("cancelled", preview_trace)
            };
            dispatch(command)
                .await
                .map(|_| ())
                .map_err(|_| record_failed())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tool_names_resolve_through_every_prefix() {
        let tools: Vec<String> = ["t3_thread_read", "t3_thread", "html_render"]
            .iter()
            .map(|s| s.to_string())
            .collect();
        for (name, expected) in [
            ("t3_thread_read", Some("t3_thread_read")),
            ("mcp__t3-code__t3_thread_read", Some("t3_thread_read")),
            ("mcp__t3_code_direct__html_render", Some("html_render")),
            ("t3code.html_render", Some("html_render")),
            ("t3-code-thread42_t3_thread_read", Some("t3_thread_read")),
            ("mcp__other__t3_thread_read", None),
            ("Bash", None),
        ] {
            assert_eq!(
                resolve_t3_tool_name(name, &tools).as_deref(),
                expected,
                "{name}"
            );
        }
    }

    #[test]
    fn subjects_are_tools_commands_or_general() {
        assert_eq!(resolve_subject(" general ").as_deref(), Some("general"));
        assert_eq!(
            resolve_subject("mcp__t3-code__t3_feedback").as_deref(),
            Some("t3_feedback")
        );
        assert_eq!(
            resolve_subject(T3_CLI_COMMANDS[0]).as_deref(),
            Some(T3_CLI_COMMANDS[0])
        );
        assert_eq!(resolve_subject("something else"), None);
    }

    #[test]
    fn the_mode_decides_who_may_send() {
        assert_eq!(allowed_by("off", true, "mcp"), Err(refused(OFF_MESSAGE)));
        assert!(allowed_by("on-request", false, "mcp")
            .unwrap_err()
            .1
            .contains("only when the user asks"));
        assert!(allowed_by("on-request", false, "cli")
            .unwrap_err()
            .1
            .contains("ask your agent"));
        assert_eq!(allowed_by("on-request", true, "mcp"), Ok(()));
        assert_eq!(allowed_by("auto", false, "mcp"), Ok(()));
    }

    #[test]
    fn ids_are_version_seven() {
        let id = uuid::Uuid::parse_str(&uuid_v7()).expect("uuid");
        assert_eq!(id.get_version_num(), 7);
    }
}
