//! Tools that act on the calling thread's own provider session: `tool_search`
//! (`McpToolExposure`), `send_to_user` (`toolkits/message`) and
//! `t3_background_wait` (`ThreadManagementService.awaitBackgroundTasks`).
use super::*;
use std::collections::HashSet;
use std::sync::Mutex;

/// What a deferring client lists before it searches.
const DIRECT_TOOLS: [&str; 4] = [
    "orchestrator_capabilities",
    "delegate_task",
    "task_status",
    "tool_search",
];
const MAX_RESULTS: usize = 10;
/// Sessions that ended are never reported, so only the newest are kept.
const MAX_SESSIONS: usize = 256;

/// The tools each deferring provider session has loaded, oldest session first.
static LOADED: LazyLock<Mutex<indexmap::IndexMap<String, HashSet<String>>>> =
    LazyLock::new(|| Mutex::new(indexmap::IndexMap::new()));

fn loaded() -> std::sync::MutexGuard<'static, indexmap::IndexMap<String, HashSet<String>>> {
    LOADED.lock().unwrap_or_else(|e| e.into_inner())
}

/// Whether a deferring provider session lists `tool` (`McpToolExposure.isListed`).
pub(super) fn is_listed(session: &str, tool: &str) -> bool {
    DIRECT_TOOLS.contains(&tool) || loaded().get(session).is_some_and(|t| t.contains(tool))
}

fn load(session: &str, names: &[String]) {
    let mut sessions = loaded();
    let mut listed = sessions.shift_remove(session).unwrap_or_default();
    listed.extend(names.iter().cloned());
    sessions.insert(session.to_owned(), listed);
    while sessions.len() > MAX_SESSIONS {
        sessions.shift_remove_index(0);
    }
}

/// `requireThreadCaller` for `readsAsCaller` and `actsAsCaller` tools.
fn thread_scope(caller: &Caller) -> Result<&super::super::ThreadScope, Refusal> {
    caller.granted.thread.as_ref().ok_or_else(|| {
        failure(
            "thread_credential_required",
            "This tool acts as the calling T3 thread, so it needs an agent running inside T3 Code. This MCP client signed in from outside a thread.",
        )
    })
}

/// `tool_search`: tools whose name or description match the query's words,
/// best first, which the session lists from then on.
pub(super) fn tool_search(caller: &Caller, a: &Value) -> Outcome {
    let scope = thread_scope(caller)?;
    let query = required(a, "query")?.to_lowercase();
    let terms: Vec<&str> = query
        .split(|c: char| !(c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_'))
        .filter(|t| !t.is_empty())
        .collect();
    let mut scored: Vec<(usize, &Value)> = SERVED_TOOLS
        .iter()
        .map(|(tool, _)| tool)
        .map(|tool| {
            let name = tool["name"].as_str().unwrap_or_default().to_lowercase();
            let description = tool["description"]
                .as_str()
                .unwrap_or_default()
                .to_lowercase();
            let score = terms
                .iter()
                .map(|term| {
                    if *term == name {
                        100
                    } else if name.contains(term) {
                        3
                    } else if description.contains(term) {
                        1
                    } else {
                        0
                    }
                })
                .sum();
            (score, tool)
        })
        .filter(|(score, _)| *score > 0)
        .collect();
    // A stable sort keeps registration order among equal scores, as the reference's does.
    scored.sort_by_key(|(score, _)| std::cmp::Reverse(*score));
    let found: Vec<Value> = scored
        .into_iter()
        .take(MAX_RESULTS)
        .map(|(_, tool)| json!({"name": tool["name"], "description": tool["description"].as_str().unwrap_or_default()}))
        .collect();
    if !found.is_empty() {
        let names: Vec<String> = found
            .iter()
            .filter_map(|t| t["name"].as_str().map(str::to_owned))
            .collect();
        load(&scope.provider_session_id, &names);
    }
    Ok(json!({"tools": found}))
}

/// `send_to_user`: the message shows in the thread as the tool call itself.
pub(super) async fn send_to_user<S: McpHost>(host: &S, caller: &Caller, a: &Value) -> Outcome {
    thread_scope(caller)?;
    threads::writing_caller(host, caller).await?;
    let message = required(a, "message")?;
    Ok(json!({
        "sentMessage": message,
        "message": "Shown to the user. Don't repeat it.",
    }))
}

/// `t3_background_wait`: marks background work the thread's provider runs as
/// awaited. Codex cannot; this server's Claude sessions report no background
/// work, so no task id matches one.
pub(super) async fn background_wait<S: McpHost>(host: &S, caller: &Caller, a: &Value) -> Outcome {
    thread_scope(caller)?;
    threads::writing_caller(host, caller).await?;
    let ids: Vec<String> = a["taskIds"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|v| v.as_str().map(str::to_owned))
        .collect();
    let awaited = arg_bool(a, "waiting")?.unwrap_or(true);
    let Some(thread) = caller.thread.as_ref() else {
        return Err(calling_thread_missing());
    };
    let refusal = |reason: &str| {
        let (code, message) = match reason {
            "unsupported" => (
                "invalid_request",
                "This provider cannot mark background work as awaited.",
            ),
            "unmatched" => (
                "invalid_request",
                "None of these IDs is running background work on this thread, so this request registers no wait for them. If the task already finished, read its output now. Otherwise pass the task ID of a background command or Monitor you started. Subagent, Agent tool, and delegate_task IDs don't go here.",
            ),
            _ => ("orchestration_error", "This thread has no live provider session."),
        };
        failure(code, message)
    };
    let driver = {
        let store = host.control_state().store.read().await;
        let thread = find_thread(&store, &thread.id)?;
        let projection = thread.full_projection();
        let active = &projection["thread"]["activeProviderThreadId"];
        let session_id = projection["providerThreads"]
            .as_array()
            .into_iter()
            .flatten()
            .find(|p| p["id"] == *active && p["appThreadId"] == thread.id.as_str())
            .map(|p| p["providerSessionId"].clone())
            .filter(|id| !id.is_null());
        let session = session_id.and_then(|id| {
            projection["providerSessions"]
                .as_array()
                .into_iter()
                .flatten()
                .find(|s| s["id"] == id && s["endedAt"].is_null() && s["detachedAt"].is_null())
                .cloned()
        });
        match session {
            Some(session) => session["driver"].as_str().unwrap_or_default().to_owned(),
            None => return Err(refusal("unavailable")),
        }
    };
    if driver != "claudeAgent" {
        return Err(refusal("unsupported"));
    }
    if awaited {
        return Err(refusal("unmatched"));
    }
    Ok(json!({"matched": [], "unmatched": ids}))
}
