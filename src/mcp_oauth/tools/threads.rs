//! The reference's thread toolkit (`toolkits/thread/handlers.ts`), with
//! `t3_thread_update` (`ThreadMetadataMcpService`) and `t3_workflow_stop`:
//! tools that read or change one thread, by default the caller's own.
//!
//! Every change is an orchestration command dispatched through
//! [`McpHost::dispatch`], the path `orchestration.dispatchCommand` takes, so it
//! is written to the shared store as a client's command is.
use super::*;

const THREAD_LINKS_HINT: &str = " Thread ids belong to one environment: for a thread on a linked one, find it with `t3_environment_links` and pass its `environmentId` to a tool that takes one.";
const HOSTED_THREADS: &str = "https://app.t3.codes/threads/";
const DELEGATED_TASK_PREFIX: &str = "thread:delegated-task:";
/// How long a thread's own agent's settle waits for its turn to end.
const SETTLE_AFTER_RUN_WAIT_MS: i64 = 24 * 60 * 60 * 1000;
const SETTLE_POLL_MS: u64 = 250;

fn unavailable() -> Refusal {
    failure(
        "orchestration_error",
        "The operation could not be completed.",
    )
}

/// `newCommandId`.
fn new_command_id() -> String {
    format!("mcp:{}", crate::platform::uuid_v4())
}

/// `required`: a field the chosen action needs; `null` counts as given.
fn needed<'a>(args: &'a Value, key: &str) -> Result<&'a Value, Refusal> {
    args.get(key)
        .ok_or_else(|| failure("invalid_request", format!("This action needs `{key}`.")))
}

fn one_of<'a>(args: &'a Value, key: &str, allowed: &[&str]) -> Result<Option<&'a str>, Refusal> {
    match arg_str(args, key)? {
        Some(value) if !allowed.contains(&value) => Err(invalid_arguments()),
        value => Ok(value),
    }
}

/// `dispatchFailure`: what an agent hears when a command is refused.
fn dispatch_failure(error: &Value) -> Refusal {
    let inner = &error["cause"];
    match inner["name"].as_str() {
        Some(
            "ThreadManagementProviderInstanceDisabledError"
            | "OrchestratorRuntimeModeAboveCeilingError"
            | "OrchestratorProviderSelectionBusyError",
        ) => failure(
            "invalid_request",
            inner["message"]
                .as_str()
                .map_or_else(|| dispatch_message(error), str::to_owned),
        ),
        Some("OrchestratorDispatchError" | "OrchestratorCommandRejectedError") => {
            match inner["cause"].as_str().filter(|cause| !cause.is_empty()) {
                Some(cause) => failure(
                    "orchestration_error",
                    cause.chars().take(1000).collect::<String>(),
                ),
                None => unavailable(),
            }
        }
        _ => unavailable(),
    }
}

async fn dispatch<S: McpHost>(host: &Arc<S>, command: Value) -> Result<u64, Refusal> {
    host.dispatch(command)
        .await
        .map_err(|e| dispatch_failure(&e))
}

/// The link a caller acts for, when it is fenced: paired below full access
/// (`fencingLinkOrigin`).
pub(super) fn fencing_origin(store: &crate::store::Store, caller: &Caller) -> Option<Value> {
    let own = caller
        .thread
        .as_ref()
        .and_then(|thread| store.threads.get(&thread.id))
        .map(|thread| thread.stored_row()["linkOrigin"].clone())
        .filter(Value::is_object);
    let origin = own.or_else(|| {
        (caller.thread.is_none() && caller.granted.peer_link).then(|| {
            json!({
                "sessionId": caller.granted.session_id,
                "label": caller.granted.label,
                "access": caller.granted.access,
            })
        })
    })?;
    (origin["access"] != "full-access").then_some(origin)
}

/// `writingCaller`: a read-only client may not change anything, and a thread
/// may act only while it owns a live run (`assertLiveCaller`).
pub(super) async fn writing_caller<S: McpHost>(host: &S, caller: &Caller) -> Result<(), Refusal> {
    caller.refuse_read_only()?;
    let (Some(thread), Some(scope)) = (&caller.thread, &caller.granted.thread) else {
        return Ok(());
    };
    let store = host.control_state().store.read().await;
    let live = store.threads.get(&thread.id).is_some_and(|t| {
        let row = t.shell();
        let instance = row["providerInstanceId"]
            .as_str()
            .or(row["modelSelection"]["instanceId"].as_str());
        row["archivedAt"].is_null()
            && instance == Some(scope.provider_instance_id.as_str())
            && (!row["activeRunId"].is_null()
                || row["pendingBackgroundTasks"]
                    .as_array()
                    .is_some_and(|tasks| {
                        tasks.iter().any(|task| {
                            task["kind"] == "subagent"
                                && task["providerInstanceId"] == scope.provider_instance_id.as_str()
                        })
                    }))
    });
    if live {
        Ok(())
    } else {
        Err(failure(
            "parent_not_active",
            "The calling provider no longer owns an active thread run.",
        ))
    }
}

/// `writesThreads`: the caller may change each named thread other than its
/// own only if that thread runs within the caller's modes and, for a fenced
/// caller, was started by the same link. Threads that do not exist are left
/// to the tool, which names them.
pub(super) async fn check_targets<S: McpHost>(
    host: &S,
    caller: &Caller,
    ids: &[Option<&str>],
) -> Result<(), Refusal> {
    writing_caller(host, caller).await?;
    let (runtime, interaction) = caller.limits();
    let store = host.control_state().store.read().await;
    for id in ids.iter().flatten() {
        if caller.thread.as_ref().is_some_and(|t| t.id == *id) {
            continue;
        }
        let Some(thread) = store.threads.get(*id) else {
            continue;
        };
        let row = thread.stored_row();
        if !row["deletedAt"].is_null() {
            continue;
        }
        let target_runtime = row["runtimeMode"].as_str().unwrap_or("full-access");
        let target_interaction = row["interactionMode"].as_str().unwrap_or("default");
        let code = if rank(target_runtime).unwrap_or(3) > rank(runtime).unwrap_or(0) {
            Some("runtime_mode_escalation_denied")
        } else if target_interaction != "plan" && interaction == "plan" {
            Some("interaction_mode_escalation_denied")
        } else {
            None
        };
        if let Some(code) = code {
            return Err(failure(
                code,
                format!(
                    "Thread \"{id}\" runs in {target_runtime}/{target_interaction}, above this caller's {runtime}/{interaction} limit. For linked calls, this limit is also capped by the link's approved access. This call does not create an approval request. Have the user act directly in the target thread through an authorized client."
                ),
            ));
        }
        if let Some(origin) = fencing_origin(&store, caller) {
            if row["linkOrigin"]["sessionId"] != origin["sessionId"] {
                return Err(failure(
                    "capability_denied",
                    format!(
                        "This work was started from {} through a linked environment, so it may change only threads that link started. A link paired with full access is not limited this way.",
                        origin["label"].as_str().unwrap_or_default()
                    ),
                ));
            }
        }
    }
    Ok(())
}

/// A thread link's ids as written: `t3-thread://v1/<thread>`,
/// `t3-thread://v2/<environment>/<thread>`, or a hosted link, whose segments
/// are percent-decoded (`parseThreadLinkHref`).
fn parse_thread_link(href: &str) -> Option<(Option<String>, String)> {
    if let Some(rest) = href.strip_prefix(HOSTED_THREADS) {
        let segment = |s: &str| {
            !s.is_empty()
                && s.chars()
                    .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '%' | '-'))
        };
        let (environment, thread) = rest.split_once('/')?;
        if !segment(environment) || !segment(thread) {
            return None;
        }
        let decode = |s: &str| {
            percent_encoding::percent_decode_str(s)
                .decode_utf8()
                .ok()
                .map(|d| d.into_owned())
                .filter(|d| !d.trim().is_empty())
        };
        return Some((Some(decode(environment)?), decode(thread)?));
    }
    if let Some(thread) = href.strip_prefix("t3-thread://v1/") {
        return (!thread.trim().is_empty()).then(|| (None, thread.to_owned()));
    }
    let (environment, thread) = href.strip_prefix("t3-thread://v2/")?.split_once('/')?;
    (!environment.trim().is_empty() && !thread.trim().is_empty())
        .then(|| (Some(environment.to_owned()), thread.to_owned()))
}

/// The threads a link can name in `environment`: as written, then decoded.
fn link_candidates(link: (Option<String>, String), environment: &str) -> Vec<(String, String)> {
    let (linked, thread) = link;
    let environment = linked.unwrap_or_else(|| environment.to_owned());
    let mut out = vec![(environment.clone(), thread.clone())];
    if let Ok(decoded) = percent_encoding::percent_decode_str(&thread).decode_utf8() {
        if decoded != thread && !decoded.trim().is_empty() {
            out.push((environment, decoded.into_owned()));
        }
    }
    out
}

fn has_thread_links(text: &str) -> bool {
    text.contains("t3-thread://v") || text.contains(HOSTED_THREADS)
}

/// The threads Markdown's links name (`threadLinkRefs`): labeled links,
/// autolinks and bare hosted links.
fn thread_link_refs(markdown: &str, environment: &str) -> Vec<(String, String)> {
    static LINK: LazyLock<regex::Regex> = LazyLock::new(|| {
        regex::Regex::new(concat!(
            r"\[[^\[\]\n]*\]\(<(t3-thread://v[12]/[^\s<>]+|https://app\.t3\.codes/threads/[A-Za-z0-9_%-]+/[A-Za-z0-9_%-]+)>\)",
            r"|\[[^\[\]\n]*\]\((t3-thread://v[12]/[^\s>)]+|https://app\.t3\.codes/threads/[A-Za-z0-9_%-]+/[A-Za-z0-9_%-]+)\)",
            r"|<(t3-thread://v[12]/[^\s<>]+|https://app\.t3\.codes/threads/[A-Za-z0-9_%-]+/[A-Za-z0-9_%-]+)>",
            r"|(https://app\.t3\.codes/threads/[A-Za-z0-9_%-]+/[A-Za-z0-9_%-]+)",
        ))
        .expect("thread link pattern")
    });
    let mut refs: Vec<(String, String)> = Vec::new();
    for captures in LINK.captures_iter(markdown) {
        let Some(href) = (1..=4).find_map(|i| captures.get(i)) else {
            continue;
        };
        if let Some(link) = parse_thread_link(href.as_str()) {
            for candidate in link_candidates(link, environment) {
                if !refs.contains(&candidate) {
                    refs.push(candidate);
                }
            }
        }
    }
    refs
}

/// The id an agent probably meant: `mcp:<id>` for a bare id, or a delegated
/// task's id with its rest encoded (`suggestedThreadId`).
fn suggested_thread_id(id: &str) -> Option<String> {
    let Some(rest) = id.strip_prefix(DELEGATED_TASK_PREFIX) else {
        return Some(format!("mcp:{id}"));
    };
    let encoded = crate::thread_projection::uri_component(rest);
    (encoded != rest).then(|| format!("{DELEGATED_TASK_PREFIX}{encoded}"))
}

/// `threadNotFound`: `message`, naming a live thread the agent probably meant
/// and, where this caller may use them, linked environments.
pub(super) fn thread_not_found_hint<S: McpHost>(
    host: &S,
    store: &crate::store::Store,
    caller: &Caller,
    id: &str,
    message: &str,
) -> Refusal {
    let environment = host.environment_id();
    let candidates: Vec<String> = if !has_thread_links(id) {
        suggested_thread_id(id).into_iter().collect()
    } else {
        let refs = match parse_thread_link(id) {
            Some(link) => link_candidates(link, environment),
            None => thread_link_refs(id, environment),
        };
        refs.into_iter()
            .filter(|(env, _)| env == environment)
            .map(|(_, thread)| thread)
            .collect()
    };
    let suggested = candidates.into_iter().find(|candidate| {
        store
            .threads
            .get(candidate)
            .is_some_and(|t| t.stored_row()["deletedAt"].is_null())
    });
    if let Some(suggested) = suggested.as_deref().filter(|_| has_thread_links(id)) {
        return failure(
            "thread_not_found",
            format!("`{id}` is a thread link, not a thread id. Pass `{suggested}` instead."),
        );
    }
    let mcp = suggested
        .map(|s| format!(" Did you mean `{s}`?"))
        .unwrap_or_default();
    let links = if !host.linked_environments_enabled()
        || caller.granted.peer_link
        || fencing_origin(store, caller).is_some()
    {
        ""
    } else {
        THREAD_LINKS_HINT
    };
    failure("thread_not_found", format!("{message}{mcp}{links}"))
}

/// `readThread`'s lookup: the named thread, else the caller's own.
pub(super) async fn read_thread<S: McpHost>(
    host: &S,
    caller: &Caller,
    id: Option<&str>,
) -> Result<String, Refusal> {
    let Some(id) = id.or(caller.thread.as_ref().map(|t| t.id.as_str())) else {
        return Err(failure(
            "target_required",
            "Pass threadId: this MCP client is not running inside a T3 thread.",
        ));
    };
    let store = host.control_state().store.read().await;
    if store
        .threads
        .get(id)
        .is_some_and(|t| t.stored_row()["deletedAt"].is_null())
    {
        return Ok(id.to_owned());
    }
    Err(thread_not_found_hint(
        host,
        &store,
        caller,
        id,
        "The thread was not found.",
    ))
}

/// The thread's projection, in full.
async fn projection<S: McpHost>(host: &S, id: &str) -> Result<Value, Refusal> {
    let store = host.control_state().store.read().await;
    Ok(find_thread(&store, id)?.full_projection())
}

fn records(projection: &Value, key: &str) -> Vec<Value> {
    projection[key].as_array().cloned().unwrap_or_default()
}

// ---------------------------------------------------------------------------
// t3_thread_organize

/// Key for a pin at the top of the Pinned section (`topOfPinnedRunOrderKey`).
fn top_of_pinned_key<'a>(keys: impl Iterator<Item = &'a str>) -> Option<String> {
    const DIGITS: &[u8] = b"abcdefghijklmnopqrstuvwxyz";
    let first = keys.min();
    let valid = |key: &str| {
        !key.is_empty()
            && key.bytes().all(|b| DIGITS.contains(&b))
            && !key.ends_with(DIGITS[0] as char)
    };
    fn midpoint(a: &[u8], b: &[u8]) -> Vec<u8> {
        const DIGITS: &[u8] = b"abcdefghijklmnopqrstuvwxyz";
        if !b.is_empty() {
            let mut n = 0;
            while a.get(n).copied().unwrap_or(DIGITS[0]) == b.get(n).copied().unwrap_or(0) {
                n += 1;
            }
            if n > 0 {
                let mut out = b[..n].to_vec();
                out.extend(midpoint(a.get(n..).unwrap_or_default(), &b[n..]));
                return out;
            }
        }
        let index = |c: u8| DIGITS.iter().position(|d| *d == c).unwrap_or(0);
        let digit_a = a.first().map_or(0, |c| index(*c));
        let digit_b = b.first().map_or(DIGITS.len(), |c| index(*c));
        if digit_b - digit_a > 1 {
            // Math.round of a half rounds up.
            return vec![DIGITS[(digit_a + digit_b).div_ceil(2)]];
        }
        if b.len() > 1 {
            return vec![b[0]];
        }
        let mut out = vec![DIGITS[digit_a]];
        out.extend(midpoint(a.get(1..).unwrap_or_default(), b""));
        out
    }
    match first {
        Some(key) if !valid(key) => None,
        Some(key) => Some(String::from_utf8(midpoint(b"", key.as_bytes())).ok()?),
        None => Some(String::from_utf8(midpoint(b"", b"")).ok()?),
    }
}

pub(super) async fn organize<S: McpHost>(host: &Arc<S>, caller: &Caller, a: &Value) -> Outcome {
    let thread_id = arg_str(a, "threadId")?;
    let action = one_of(
        a,
        "action",
        &[
            "pin",
            "unpin",
            "snooze",
            "unsnooze",
            "settle",
            "unsettle",
            "archive",
            "unarchive",
            "mark_unread",
        ],
    )?
    .ok_or_else(invalid_arguments)?;
    let snoozed_until = arg_str(a, "snoozedUntil")?;
    check_targets(&**host, caller, &[thread_id]).await?;
    let id = read_thread(&**host, caller, thread_id).await?;
    let command_id = new_command_id();
    let common = json!({"commandId": command_id, "threadId": id});
    let mut command = common.clone();
    match action {
        "settle" => {
            let by_own_agent = caller.thread.as_ref().is_some_and(|t| t.id == id);
            let active = if by_own_agent {
                let store = host.control_state().store.read().await;
                find_thread(&store, &id)?.shell()["activeRunId"]
                    .as_str()
                    .map(str::to_owned)
            } else {
                None
            };
            if let Some(run_id) = active {
                let task_host = host.clone();
                host.spawn(Box::pin(settle_after_run(task_host, id, run_id)));
                return Ok(json!({"settlesWhenTurnEnds": true}));
            }
            command["type"] = json!("thread.settle");
        }
        "snooze" => {
            let Some(until) = snoozed_until else {
                return Err(failure("invalid_request", "snooze requires snoozedUntil."));
            };
            command["type"] = json!("thread.snooze");
            command["snoozedUntil"] = json!(until);
        }
        "unsnooze" | "unsettle" => {
            command["type"] = json!(format!("thread.{action}"));
            command["reason"] = json!("user");
        }
        "mark_unread" => command["type"] = json!("thread.mark-unread"),
        "pin" => {
            // Always a key, as a client pins: the reducer ignores it for a
            // thread that is already pinned.
            let key = {
                let store = host.control_state().store.read().await;
                let keys: Vec<String> = store
                    .threads
                    .values()
                    .map(|t| t.stored_row())
                    .filter(|row| {
                        row["deletedAt"].is_null()
                            && row["archivedAt"].is_null()
                            && !row["pinnedAt"].is_null()
                    })
                    .filter_map(|row| row["pinOrderKey"].as_str().map(str::to_owned))
                    .collect();
                top_of_pinned_key(keys.iter().map(String::as_str))
            };
            command["type"] = json!("thread.pin");
            if let Some(key) = key {
                command["orderKey"] = json!(key);
            }
        }
        other => command["type"] = json!(format!("thread.{other}")),
    }
    let sequence = dispatch(host, command).await?;
    Ok(json!({"sequence": sequence}))
}

/// Settles a thread its own agent asked to settle mid-turn, once that turn
/// completes, unless the user or other work keeps it active
/// (`ThreadManagementService.settleAfterRun`).
async fn settle_after_run<S: McpHost>(host: Arc<S>, thread_id: String, run_id: String) {
    let requested_at = crate::platform::now_ms();
    let deadline = requested_at + SETTLE_AFTER_RUN_WAIT_MS;
    let pause = || host.sleep(std::time::Duration::from_millis(SETTLE_POLL_MS));
    let run = loop {
        let run = {
            let store = host.control_state().store.read().await;
            let Some(thread) = store.threads.get(&thread_id) else {
                return;
            };
            records(&thread.full_projection(), "runs")
                .into_iter()
                .find(|run| run["id"] == run_id.as_str())
        };
        let Some(run) = run else { return };
        if TERMINAL.contains(&run["status"].as_str().unwrap_or_default()) {
            break run;
        }
        if crate::platform::now_ms() >= deadline {
            return;
        }
        pause().await;
    };
    if run["status"] != "completed" {
        return;
    }
    let mut seen = None;
    loop {
        let sequence = host
            .control_state()
            .store
            .read()
            .await
            .thread_sequences
            .get(&thread_id)
            .copied()
            .unwrap_or(0);
        if seen != Some(sequence) {
            seen = Some(sequence);
            match try_settle(&host, &thread_id, &run, requested_at, sequence).await {
                Some(true) | None => return,
                Some(false) => {}
            }
        }
        if crate::platform::now_ms() >= deadline {
            return;
        }
        pause().await;
    }
}

/// One attempt: `Some(true)` when settled or kept active for good,
/// `Some(false)` to try again after the next change, `None` on a failure.
async fn try_settle<S: McpHost>(
    host: &Arc<S>,
    thread_id: &str,
    run: &Value,
    requested_at: i64,
    sequence: u64,
) -> Option<bool> {
    let projection = {
        let store = host.control_state().store.read().await;
        store.threads.get(thread_id)?.full_projection()
    };
    let row = &projection["thread"];
    // Settling ends every watch; one the agent started after asking must outlive it.
    let watched_since = row["pullRequests"]
        .as_array()
        .into_iter()
        .flatten()
        .chain(std::iter::once(&row["linkedPullRequest"]))
        .filter_map(|link| link["watch"]["startedAt"].as_str())
        .any(|started| {
            time::OffsetDateTime::parse(started, &time::format_description::well_known::Rfc3339)
                .is_ok_and(|at| (at.unix_timestamp_nanos() / 1_000_000) as i64 >= requested_at)
        });
    if watched_since {
        return Some(true);
    }
    let runs = records(&projection, "runs");
    let messages = records(&projection, "messages");
    let ordinal = run["ordinal"].as_u64().unwrap_or(0);
    let later: Vec<&Value> = runs
        .iter()
        .filter(|candidate| candidate["ordinal"].as_u64().unwrap_or(0) > ordinal)
        .collect();
    let cancelled: Vec<&Value> = later
        .iter()
        .filter(|candidate| candidate["status"] == "cancelled")
        .map(|candidate| &candidate["userMessageId"])
        .collect();
    // Send now cancels a queued run and moves its message into the running turn.
    let steered: Vec<&Value> = messages
        .iter()
        .filter(|m| cancelled.contains(&&m["id"]) && m["runId"] == run["id"])
        .map(|m| &m["id"])
        .collect();
    let following: Vec<&Value> = later
        .into_iter()
        .filter(|c| c["status"] != "cancelled" || !steered.contains(&&c["userMessageId"]))
        .collect();
    if !following.is_empty() {
        let ids: Vec<&Value> = following.iter().map(|c| &c["id"]).collect();
        let theirs: Vec<&Value> = messages
            .iter()
            .filter(|m| m["role"] == "user" && ids.contains(&&m["runId"]))
            .collect();
        let automatic: Vec<&Value> = theirs
            .iter()
            .filter(|m| !m["notification"].is_null() || !m["delegatedCompletion"].is_null())
            .map(|m| &m["id"])
            .collect();
        if theirs.iter().any(|m| !automatic.contains(&&m["id"]))
            || following
                .iter()
                .any(|c| !automatic.contains(&&c["userMessageId"]))
        {
            return Some(true);
        }
        let status = |c: &&Value| c["status"].as_str().unwrap_or_default().to_owned();
        if following
            .iter()
            .any(|c| status(c) != "queued" && !TERMINAL.contains(&status(c).as_str()))
        {
            return Some(false);
        }
        if following
            .iter()
            .any(|c| status(c) != "queued" && status(c) != "completed")
        {
            return Some(true);
        }
    }
    let Some(latest) = latest(&runs) else {
        return Some(true);
    };
    let command = json!({
        "type": "thread.settle",
        "commandId": format!("server:settle-after-run:{}:{sequence}", run["id"].as_str().unwrap_or_default()),
        "threadId": thread_id,
        "expectedLatestRunId": latest["id"],
    });
    match host.dispatch(command).await {
        Ok(_) => Some(true),
        // Another run can start between the read and the guarded dispatch.
        Err(error) if error["cause"]["name"] == "OrchestratorDispatchError" => Some(false),
        Err(_) => None,
    }
}

// ---------------------------------------------------------------------------
// t3_thread_configure

pub(super) async fn configure<S: McpHost>(host: &Arc<S>, caller: &Caller, a: &Value) -> Outcome {
    let thread_id = arg_str(a, "threadId")?;
    let selection = match a.get("modelSelection") {
        None => None,
        Some(Value::Object(s)) if s.contains_key("model") => Some(Value::Object(s.clone())),
        Some(_) => return Err(invalid_arguments()),
    };
    let Some(selection) = selection else {
        let id = read_thread(&**host, caller, thread_id).await?;
        let thread = &projection(&**host, &id).await?["thread"];
        return Ok(json!({
            "threadId": id,
            "modelSelection": thread["modelSelection"],
            "runtimeMode": thread["runtimeMode"],
            "interactionMode": thread["interactionMode"],
        }));
    };
    check_targets(&**host, caller, &[thread_id]).await?;
    let id = read_thread(&**host, caller, thread_id).await?;
    let current = projection(&**host, &id).await?["thread"]["providerInstanceId"].clone();
    let kind = if current == selection["instanceId"] {
        "thread.model-selection.set"
    } else {
        "provider.switch"
    };
    let sequence = dispatch(
        host,
        json!({"type": kind, "threadId": id, "commandId": new_command_id(), "modelSelection": selection}),
    )
    .await?;
    Ok(json!({"sequence": sequence}))
}

// ---------------------------------------------------------------------------
// t3_thread_transfer

pub(super) async fn transfer<S: McpHost>(host: &Arc<S>, caller: &Caller, a: &Value) -> Outcome {
    let action =
        one_of(a, "action", &["fork", "merge_back", "status"])?.ok_or_else(invalid_arguments)?;
    let thread_id = arg_str(a, "threadId")?;
    let target_id = arg_str(a, "targetThreadId")?;
    let title = arg_str(a, "title")?;
    let source_point = match a.get("sourcePoint") {
        None => json!({"type": "latest_stable"}),
        Some(point @ Value::Object(fields))
            if fields
                .get("type")
                .and_then(Value::as_str)
                .is_some_and(|t| matches!(t, "latest_stable" | "run" | "checkpoint")) =>
        {
            point.clone()
        }
        Some(_) => return Err(invalid_arguments()),
    };
    if action == "status" {
        let id = read_thread(&**host, caller, thread_id).await?;
        let transfers = records(&projection(&**host, &id).await?, "contextTransfers")
            .into_iter()
            .map(|t| {
                json!({
                    "id": t["id"],
                    "sourceThreadId": t["sourceThreadId"],
                    "targetThreadId": t["targetThreadId"],
                    "status": t["status"],
                })
            })
            .collect::<Vec<_>>();
        return Ok(json!({"transfers": transfers}));
    }
    check_targets(&**host, caller, &[thread_id, target_id]).await?;
    let id = read_thread(&**host, caller, thread_id).await?;
    let command_id = new_command_id();
    if action == "merge_back" {
        let Some(target) = target_id else {
            return Err(failure(
                "invalid_request",
                "This action needs `targetThreadId`.",
            ));
        };
        read_thread(&**host, caller, Some(target)).await?;
        let sequence = dispatch(
            host,
            json!({
                "type": "thread.merge_back",
                "commandId": command_id,
                "sourceThreadId": id,
                "targetThreadId": target,
                "sourcePoint": source_point,
                "createdBy": "agent",
                "creationSource": "mcp",
            }),
        )
        .await?;
        return Ok(json!({"sequence": sequence, "targetThreadId": target}));
    }
    let target = format!("{command_id}:fork");
    let mut command = json!({
        "type": "thread.fork",
        "commandId": command_id,
        "sourceThreadId": id,
        "targetThreadId": target,
        "sourcePoint": source_point,
        "createdBy": "agent",
        "creationSource": "mcp",
    });
    if let Some(title) = title {
        command["title"] = json!(title);
    }
    let sequence = dispatch(host, command).await?;
    Ok(json!({"sequence": sequence, "targetThreadId": target}))
}

// ---------------------------------------------------------------------------
// t3_pending_request

/// The pending question `request_id` asks, and its turn item (`readQuestion`).
async fn read_question<S: McpHost>(
    host: &S,
    caller: &Caller,
    thread_id: Option<&str>,
    request_id: &Value,
) -> Result<(String, Value), Refusal> {
    let id = read_thread(host, caller, thread_id).await?;
    let projection = projection(host, &id).await?;
    let request = records(&projection, "runtimeRequests")
        .into_iter()
        .find(|r| r["id"] == *request_id && r["kind"] == "user_input");
    let item = records(&projection, "turnItems")
        .into_iter()
        .find(|i| i["type"] == "user_input_request" && i["requestId"] == *request_id);
    let (Some(request), Some(item)) = (request, item) else {
        return Err(failure(
            "invalid_request",
            "The pending user-input request was not found.",
        ));
    };
    match request["status"].as_str().unwrap_or_default() {
        "pending" => Ok((id, item)),
        "resolved" => Err(failure(
            "invalid_request",
            "The user-input request was already answered.",
        )),
        status => Err(failure(
            "invalid_request",
            format!("The user-input request is no longer pending ({status})."),
        )),
    }
}

pub(super) async fn pending_request<S: McpHost>(
    host: &Arc<S>,
    caller: &Caller,
    a: &Value,
) -> Outcome {
    let action =
        one_of(a, "action", &["list", "read", "respond"])?.ok_or_else(invalid_arguments)?;
    let thread_id = arg_str(a, "threadId")?;
    arg_str(a, "requestId")?;
    if a.get("answers")
        .is_some_and(|v| !v.is_object() && !v.is_null())
    {
        return Err(invalid_arguments());
    }
    match action {
        "list" => {
            let id = read_thread(&**host, caller, thread_id).await?;
            let ids = records(&projection(&**host, &id).await?, "runtimeRequests")
                .into_iter()
                .filter(|r| r["kind"] == "user_input" && r["status"] == "pending")
                .map(|r| r["id"].clone())
                .collect::<Vec<_>>();
            Ok(json!({"requestIds": ids}))
        }
        "read" => {
            let request_id = needed(a, "requestId")?;
            let (_, item) = read_question(&**host, caller, thread_id, request_id).await?;
            Ok(json!({"requestId": request_id, "questions": item["questions"]}))
        }
        _ => {
            check_targets(&**host, caller, &[thread_id]).await?;
            let request_id = needed(a, "requestId")?;
            let answers = needed(a, "answers")?;
            let (id, _) = read_question(&**host, caller, thread_id, request_id).await?;
            let sequence = dispatch(
                host,
                json!({
                    "type": "runtime-request.respond",
                    "threadId": id,
                    "commandId": new_command_id(),
                    "requestId": request_id,
                    "answers": answers,
                }),
            )
            .await?;
            Ok(json!({"sequence": sequence}))
        }
    }
}

// ---------------------------------------------------------------------------
// t3_queue

/// Queued runs in the order they will be sent: completions and messages each
/// in queue order, interleaved by when they were asked for
/// (`queuedRunsInDeliveryOrder`).
fn delivery_order(runs: &[Value], messages: &[Value]) -> Vec<Value> {
    let completion_ids: Vec<&Value> = messages
        .iter()
        .filter(|m| !m["delegatedCompletion"].is_null())
        .map(|m| &m["id"])
        .collect();
    let mut queued: Vec<&Value> = runs.iter().filter(|r| r["status"] == "queued").collect();
    let position = |r: &Value| {
        r["queuePosition"]
            .as_f64()
            .unwrap_or_else(|| r["ordinal"].as_f64().unwrap_or(0.0))
    };
    queued.sort_by(|l, r| {
        position(l)
            .partial_cmp(&position(r))
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| {
                l["ordinal"]
                    .as_f64()
                    .partial_cmp(&r["ordinal"].as_f64())
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
    });
    let (completions, others): (Vec<&Value>, Vec<&Value>) = queued
        .into_iter()
        .partition(|r| completion_ids.contains(&&r["userMessageId"]));
    let requested = |r: &Value| {
        r["requestedAt"]
            .as_str()
            .and_then(|at| {
                time::OffsetDateTime::parse(at, &time::format_description::well_known::Rfc3339).ok()
            })
            .map_or(0, |at| at.unix_timestamp_nanos() / 1_000_000)
    };
    let (mut c, mut m) = (0, 0);
    let mut ordered = Vec::new();
    while c < completions.len() && m < others.len() {
        if requested(completions[c]) <= requested(others[m]) {
            ordered.push(completions[c].clone());
            c += 1;
        } else {
            ordered.push(others[m].clone());
            m += 1;
        }
    }
    ordered.extend(completions[c..].iter().map(|r| (*r).clone()));
    ordered.extend(others[m..].iter().map(|r| (*r).clone()));
    ordered
}

/// A queued message, cut to `limit` characters (`queueEntry`).
fn queue_entry(runs: &[Value], messages: &[Value], run_id: &Value, limit: usize) -> Option<Value> {
    let run = runs
        .iter()
        .find(|r| r["id"] == *run_id && r["status"] == "queued")?;
    let message = messages.iter().find(|m| m["id"] == run["userMessageId"])?;
    let text = message["text"].as_str().unwrap_or_default();
    // JavaScript's Array.from counts code points, as chars does.
    let count = text.chars().count();
    Some(json!({
        "queuedRunId": run["id"],
        "text": text.chars().take(limit).collect::<String>(),
        "truncated": count > limit,
    }))
}

pub(super) async fn queue<S: McpHost>(host: &Arc<S>, caller: &Caller, a: &Value) -> Outcome {
    let action = one_of(
        a,
        "action",
        &[
            "list",
            "read",
            "edit",
            "cancel",
            "reorder",
            "promote_to_steer",
        ],
    )?
    .ok_or_else(invalid_arguments)?;
    let thread_id = arg_str(a, "threadId")?;
    for key in ["queuedRunId", "text", "beforeRunId", "targetRunId"] {
        arg_str(a, key)?;
    }
    if a["text"]
        .as_str()
        .is_some_and(|t| t.chars().count() > 100_000)
    {
        return Err(invalid_arguments());
    }
    let cursor = arg_count(a, "cursor", 0)?.unwrap_or(0) as usize;
    let limit = arg_count(a, "limit", 1)?.unwrap_or(20) as usize;
    if matches!(action, "list" | "read") {
        let id = read_thread(&**host, caller, thread_id).await?;
        let projection = projection(&**host, &id).await?;
        let runs = records(&projection, "runs");
        let messages = records(&projection, "messages");
        if action == "list" {
            let ordered = delivery_order(&runs, &messages);
            let end = cursor + limit;
            let items = ordered
                .iter()
                .skip(cursor)
                .take(limit)
                .filter_map(|run| queue_entry(&runs, &messages, &run["id"], 1000))
                .collect::<Vec<_>>();
            return Ok(json!({
                "items": items,
                "nextCursor": if end < ordered.len() { json!(end) } else { Value::Null },
            }));
        }
        let run_id = needed(a, "queuedRunId")?;
        return queue_entry(&runs, &messages, run_id, 16_000)
            .ok_or_else(|| failure("invalid_request", "The queued message was not found."));
    }
    check_targets(&**host, caller, &[thread_id]).await?;
    let run_id = needed(a, "queuedRunId")?.clone();
    let mut command = json!({});
    match action {
        "edit" => {
            command =
                json!({"type": "queued-run.edit", "runId": run_id, "text": needed(a, "text")?});
        }
        "reorder" => {
            command = json!({"type": "queued-run.reorder", "runId": run_id, "beforeRunId": needed(a, "beforeRunId")?});
        }
        "promote_to_steer" => {
            command = json!({"type": "queued-message.promote-to-steer", "queuedRunId": run_id, "targetRunId": needed(a, "targetRunId")?});
        }
        _ => command["type"] = json!("queued-run.cancel"),
    }
    if command.get("runId").is_none() && action == "cancel" {
        command["runId"] = run_id;
    }
    let id = read_thread(&**host, caller, thread_id).await?;
    command["commandId"] = json!(new_command_id());
    command["threadId"] = json!(id);
    let sequence = dispatch(host, command).await?;
    Ok(json!({"sequence": sequence}))
}

// ---------------------------------------------------------------------------
// t3_thread_update

pub(super) async fn update<S: McpHost>(host: &Arc<S>, caller: &Caller, a: &Value) -> Outcome {
    let thread_id = arg_str(a, "threadId")?;
    let action =
        one_of(a, "action", &["rename", "regenerate_title"])?.ok_or_else(invalid_arguments)?;
    let title = arg_str(a, "title")?;
    let request_key = arg_str(a, "clientRequestId")?;
    check_targets(&**host, caller, &[thread_id]).await?;
    let Some(id) = thread_id.or(caller.thread.as_ref().map(|t| t.id.as_str())) else {
        return Err(failure(
            "target_required",
            "Pass threadId: this MCP client is not running inside a T3 thread.",
        ));
    };
    {
        let store = host.control_state().store.read().await;
        if !store
            .threads
            .get(id)
            .is_some_and(|t| t.stored_row()["deletedAt"].is_null())
        {
            return Err(thread_not_found_hint(
                &**host,
                &store,
                caller,
                id,
                &format!("Thread {id} was not found."),
            ));
        }
    }
    let key = request_key.map_or_else(|| crate::platform::uuid_v4().to_string(), str::to_owned);
    let part = crate::thread_projection::uri_component;
    let command_id = [
        "command".to_owned(),
        "mcp".to_owned(),
        part(&caller.request_namespace()),
        "thread-update".to_owned(),
        part(id),
        part(action),
        part(&key),
    ]
    .join(":");
    let mut command =
        json!({"type": "thread.metadata.update", "commandId": command_id, "threadId": id});
    if action == "rename" {
        if let Some(title) = title {
            command["title"] = json!(title);
        }
    } else {
        command["regenerateTitle"] = json!(true);
    }
    let sequence = host.dispatch(command).await.map_err(|error| {
        let message = error["cause"]["message"]
            .as_str()
            .map_or_else(|| dispatch_message(&error), str::to_owned);
        failure(
            "orchestration_error",
            format!("Unable to {action} for thread {id}: {message}"),
        )
    })?;
    let store = host.control_state().store.read().await;
    let row = find_thread(&store, id)?.stored_row();
    let regeneration = &row["titleRegeneration"];
    Ok(json!({
        "threadId": id,
        "action": action,
        "commandId": command_id,
        "sequence": sequence,
        "title": row["title"],
        "titleRegeneration": if regeneration.is_object() {
            json!({"requestId": regeneration["requestId"], "startedAt": regeneration["startedAt"]})
        } else {
            Value::Null
        },
        "linkedPullRequest": if row["linkedPullRequest"].is_object() { row["linkedPullRequest"].clone() } else { Value::Null },
        "updatedAt": row["updatedAt"],
    }))
}

// ---------------------------------------------------------------------------
// t3_thread_search

/// Active threads matching `query` by title, branch, pull request or project,
/// then by message text, in the given project, else the caller's, else all.
pub(super) async fn search<S: McpHost>(host: &Arc<S>, caller: &Caller, a: &Value) -> Outcome {
    let query = required(a, "query")?.to_owned();
    let limit = arg_count(a, "limit", 1)?.unwrap_or(50) as usize;
    let project = arg_str(a, "projectId")?
        .or(caller.thread.as_ref().map(|t| t.project_id.as_str()))
        .map(str::to_owned);
    let found = host
        .search_threads(query, limit, project)
        .await
        .map_err(|()| unavailable())?;
    let matches = found
        .into_iter()
        .map(|mut hit| {
            let link = thread_link(
                &**host,
                caller,
                hit["threadId"].as_str().unwrap_or_default(),
                hit["title"].as_str().unwrap_or_default(),
            );
            hit["link"] = json!(link);
            hit
        })
        .collect::<Vec<_>>();
    Ok(json!({
        "currentThreadId": caller.thread.as_ref().map(|t| t.id.clone()),
        "matches": matches,
    }))
}

// ---------------------------------------------------------------------------
// t3_workflow_stop

pub(super) async fn workflow_stop<S: McpHost>(
    host: &Arc<S>,
    caller: &Caller,
    a: &Value,
) -> Outcome {
    let thread_id = required(a, "threadId")?;
    let subagent_id = required(a, "subagentId")?;
    check_targets(&**host, caller, &[Some(thread_id)]).await?;
    let reason = {
        let store = host.control_state().store.read().await;
        match store
            .threads
            .get(thread_id)
            .filter(|t| t.stored_row()["deletedAt"].is_null())
        {
            None => "unavailable",
            Some(thread) => {
                let task = records(&thread.full_projection(), "subagents")
                    .into_iter()
                    .find(|task| task["id"] == subagent_id);
                match task {
                    Some(task)
                        if task["threadId"] == thread_id
                            && !task["workflow"].is_null()
                            && task["status"] == "running" =>
                    {
                        if task["origin"] != "provider_native" || task["driver"] != "claudeAgent" {
                            "unsupported"
                        } else {
                            // This server's Claude sessions keep no live workflow task.
                            "unavailable"
                        }
                    }
                    _ => "not-running",
                }
            }
        }
    };
    let message = match reason {
        "not-running" => "This workflow is no longer running.",
        "unsupported" => "This provider cannot stop a workflow.",
        _ => "This workflow has no live provider task.",
    };
    let code = if reason == "unavailable" {
        "orchestration_error"
    } else {
        "invalid_request"
    };
    Err(failure(code, message))
}

/// `t3_submit_output` (`actsAsCaller`, `ThreadManagementService.submitRunOutput`):
/// the result the caller's turn was asked for, checked against its run's
/// schema and kept on that run. The run is the active one when it was asked;
/// else the newest started run asked for a result, while it completed
/// without one (an agent may end a turn to wait, and answer from the turn
/// that woke it).
pub(super) async fn submit_output<S: McpHost>(
    host: &Arc<S>,
    caller: &Caller,
    a: &Value,
) -> Outcome {
    let Some(scope) = caller.granted.thread.as_ref() else {
        return Err(failure(
            "thread_credential_required",
            "This tool acts as the calling T3 thread, so it needs an agent running inside T3 Code. This MCP client signed in from outside a thread.",
        ));
    };
    writing_caller(&**host, caller).await?;
    let unavailable = || {
        failure(
            "orchestration_error",
            "The operation could not be completed.",
        )
    };
    let json = a["json"].as_str().unwrap_or_default();
    let thread_id = scope.thread_id.clone();
    let runs: Vec<Value> = {
        let store = host.control_state().store.read().await;
        let thread = store.threads.get(&thread_id).ok_or_else(unavailable)?;
        thread.projection()["runs"]
            .as_array()
            .cloned()
            .unwrap_or_default()
    };
    let ordinal = |run: &Value| run["ordinal"].as_u64().unwrap_or(0);
    let active = runs
        .iter()
        .filter(|run| {
            matches!(
                run["status"].as_str(),
                Some("preparing" | "starting" | "running" | "waiting")
            )
        })
        .max_by_key(|run| ordinal(run));
    let asked = runs
        .iter()
        .filter(|run| {
            run["outputSchema"].is_object()
                && run["status"] != "queued"
                && !(run["status"] == "cancelled" && run["startedAt"].is_null())
        })
        .max_by_key(|run| ordinal(run));
    let owed = asked.filter(|run| run["status"] == "completed" && run["output"].is_null());
    let run = match active {
        Some(active) if active["outputSchema"].is_object() => Some(active),
        Some(_) => owed,
        None => None,
    };
    let Some(run) = run else {
        return Err(failure(
            "invalid_request",
            "This turn was not asked for a result. Reply in your message instead.",
        ));
    };
    let value = crate::run_output::check_output(&run["outputSchema"], json)
        .map_err(|rejection| failure("invalid_request", rejection))?;
    let run_id = run["id"].clone();
    host.dispatch(json!({
        "type": "run.output.submit",
        "commandId": format!("mcp:{}", crate::platform::uuid_v4()),
        "threadId": thread_id,
        "runId": run_id,
        "value": value,
    }))
    .await
    .map_err(|_| unavailable())?;
    Ok(json!({"runId": run_id}))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pins_above_the_top_key_as_the_reference_does() {
        assert_eq!(top_of_pinned_key(std::iter::empty()).as_deref(), Some("n"));
        assert_eq!(
            top_of_pinned_key(["n", "t"].into_iter()).as_deref(),
            Some("h")
        );
        assert_eq!(top_of_pinned_key(["b"].into_iter()).as_deref(), Some("an"));
        assert_eq!(
            top_of_pinned_key(["ab"].into_iter()).as_deref(),
            Some("aan")
        );
        assert_eq!(top_of_pinned_key(["a"].into_iter()), None);
    }

    #[test]
    fn reads_thread_links_as_the_reference_parses_them() {
        assert_eq!(
            parse_thread_link("t3-thread://v2/env/mcp:abc"),
            Some((Some("env".into()), "mcp:abc".into()))
        );
        assert_eq!(
            parse_thread_link("https://app.t3.codes/threads/env/mcp%3Aabc"),
            Some((Some("env".into()), "mcp:abc".into()))
        );
        assert_eq!(
            thread_link_refs(
                "see [Child](t3-thread://v2/env/t1) and <t3-thread://v1/t2>",
                "here"
            ),
            vec![("env".into(), "t1".into()), ("here".into(), "t2".into())]
        );
        assert_eq!(suggested_thread_id("abc").as_deref(), Some("mcp:abc"));
        assert_eq!(
            suggested_thread_id("thread:delegated-task:a b").as_deref(),
            Some("thread:delegated-task:a%20b")
        );
        assert_eq!(suggested_thread_id("thread:delegated-task:ab"), None);
    }
}
