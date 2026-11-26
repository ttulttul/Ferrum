//! The reference's workflow service as it answers with no workflow defined:
//! durable runs are user TypeScript the reference's engine replays, which this
//! server does not run, so it is the reference with an empty workflows folder.
//!
//! - With `workflowsEnabled` off (the default) every request is refused (`Workflows.off`).
//! - On, with no run in the shared database, nothing is there (`Workflows.disabled`).
//! - On, with runs kept from the reference, those runs are read, signalled, cancelled and kept
//!   in the store as the reference's engine with nothing registered does (`WorkflowEngine`
//!   over `WorkflowStore`, the same tables in `statev2.sqlite`): nothing starts or resumes,
//!   since no workflow is registered.
use crate::AppState;
use rusqlite::{params, Connection, OptionalExtension, Row};
use serde_json::{json, Map, Value};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, LazyLock};
use tokio::sync::broadcast;

/// `Workflows.OFF`.
pub(crate) const OFF: &str = "Workflows are off on this server. Turn them on in its Settings → Labs → Workflows, or without a desktop, set \"workflowsEnabled\": true in userdata/settings.json in its T3 home (~/.t3 by default).";
const DISABLED: &str =
    "This server defines no workflows yet. Add a file to the workflows folder of its T3 home.";
/// `END_STEP`: the step a cancelled run's cleanup runs as.
const END_STEP: &str = "onEnd";
/// `DEFAULT_SIGNAL_TTL` and `SIGNAL_IDS_KEPT`: seven days.
const SEVEN_DAYS_MS: i64 = 7 * 24 * 60 * 60 * 1000;
const UNAVAILABLE: &str = "The operation could not be completed.";
/// `WORKFLOW_READ_ACTIONS`: the actions that change nothing.
pub(crate) const READ_ACTIONS: [&str; 4] = ["list", "read", "questions", "store"];

/// A failure as `t3_workflow` answers it: its code and message.
pub(crate) type Refused = (&'static str, String);

fn invalid(message: impl Into<String>) -> Refused {
    ("invalid_request", message.into())
}

fn unavailable(_: impl std::fmt::Debug) -> Refused {
    ("orchestration_error", UNAVAILABLE.to_owned())
}

fn not_found(run_id: &str) -> Refused {
    invalid(format!("Workflow run {run_id} was not found."))
}

fn not_registered(workflow: &str) -> Refused {
    invalid(format!("No workflow named {workflow} is registered."))
}

/// Which service answers: off, nothing defined, or history kept from before.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Mode {
    Off,
    Disabled,
    History,
}

fn now() -> String {
    crate::auth_service::iso(crate::platform::now_ms())
}

fn later(millis: i64) -> String {
    crate::auth_service::iso(crate::platform::now_ms() + millis)
}

fn open(state: &AppState) -> rusqlite::Result<Connection> {
    let conn = Connection::open_with_flags(
        state.paths.state_dir.join(crate::statev2::FILE_NAME),
        rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE,
    )?;
    crate::statev2::configure(&conn, false)?;
    Ok(conn)
}

/// `WorkflowStore.changes`: a run this server wrote to, for the streams.
static CHANGES: LazyLock<broadcast::Sender<()>> = LazyLock::new(|| broadcast::channel(64).0);

fn changed() {
    let _ = CHANGES.send(());
}

/// The engine is built once, when it is first needed.
static BUILT: AtomicBool = AtomicBool::new(false);

/// `WorkflowRuntime`: off unless `workflowsEnabled`; then an engine only when
/// runs are kept (`SELECT 1 FROM workflow_runs LIMIT 1`, kept when it cannot
/// be read). Built, the engine keeps only its definitions' schedules
/// (`triggersKeep`), which here is none.
fn mode(state: &AppState, conn: &Connection) -> Mode {
    let settings = state.settings.internal().unwrap_or_default();
    if settings["workflowsEnabled"] != true {
        return Mode::Off;
    }
    let kept = conn
        .query_row("SELECT 1 FROM workflow_runs LIMIT 1", [], |_| Ok(()))
        .optional()
        .map_or(true, |row| row.is_some());
    if !kept {
        return Mode::Disabled;
    }
    if !BUILT.swap(true, Ordering::SeqCst) {
        let _ = conn.execute("DELETE FROM workflow_triggers", []);
    }
    Mode::History
}

/// A row as a JSON object, by column name.
fn row_object(row: &Row) -> rusqlite::Result<Map<String, Value>> {
    let mut object = Map::new();
    for (index, name) in row.as_ref().column_names().iter().enumerate() {
        let value = match row.get_ref(index)? {
            rusqlite::types::ValueRef::Null => Value::Null,
            rusqlite::types::ValueRef::Integer(n) => json!(n),
            rusqlite::types::ValueRef::Real(n) => json!(n),
            rusqlite::types::ValueRef::Text(text) => json!(String::from_utf8_lossy(text)),
            rusqlite::types::ValueRef::Blob(_) => Value::Null,
        };
        object.insert((*name).to_owned(), value);
    }
    Ok(object)
}

fn select(
    conn: &Connection,
    sql: &str,
    values: impl rusqlite::Params,
) -> rusqlite::Result<Vec<Map<String, Value>>> {
    let mut statement = conn.prepare(sql)?;
    let rows = statement.query_map(values, row_object)?;
    rows.collect()
}

/// `jsonValue`: a JSON column, or null.
fn json_column(row: &Map<String, Value>, column: &str) -> Value {
    row.get(column)
        .and_then(Value::as_str)
        .and_then(|text| serde_json::from_str(text).ok())
        .unwrap_or(Value::Null)
}

fn text(row: &Map<String, Value>, column: &str) -> String {
    row.get(column)
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned()
}

fn field(row: &Map<String, Value>, column: &str) -> Value {
    row.get(column).cloned().unwrap_or(Value::Null)
}

/// `toRun`, with nothing registered: a run that has not ended is `retired`,
/// and none resumes by itself.
fn to_run(row: &Map<String, Value>) -> Value {
    let mut run = json!({
        "runId": field(row, "run_id"),
        "workflow": field(row, "workflow"),
        "version": field(row, "version"),
        "title": field(row, "title"),
        "status": field(row, "status"),
        "detail": field(row, "detail"),
        "result": json_column(row, "result_json"),
        "pauses": field(row, "pauses"),
        "retired": row.get("ended_at").is_none_or(Value::is_null),
        "resumesAt": null,
        "parentRunId": field(row, "parent_run_id"),
        "parentStep": field(row, "parent_wait"),
        "startedAt": field(row, "started_at"),
        "updatedAt": field(row, "updated_at"),
        "endedAt": field(row, "ended_at"),
    });
    if row
        .get("tags_json")
        .and_then(Value::as_str)
        .is_some_and(|tags| tags != "[]")
    {
        run["tags"] = json_column(row, "tags_json");
    }
    if row.get("subject_json").is_some_and(Value::is_string) {
        run["subject"] = json_column(row, "subject_json");
    }
    run
}

fn run_row(conn: &Connection, run_id: &str) -> rusqlite::Result<Option<Map<String, Value>>> {
    Ok(select(
        conn,
        "SELECT * FROM workflow_runs WHERE run_id = ?1",
        [run_id],
    )?
    .into_iter()
    .next())
}

fn require_run(conn: &Connection, run_id: &str) -> Result<Map<String, Value>, Refused> {
    run_row(conn, run_id)
        .map_err(unavailable)?
        .ok_or_else(|| not_found(run_id))
}

/// `WorkflowStore.runs`: the newest 500, with top-level skipped runs counted apart.
fn runs(conn: &Connection, status: Option<&str>) -> rusqlite::Result<Vec<Value>> {
    let rows = match status {
        None => select(
            conn,
            "SELECT * FROM (
               SELECT * FROM workflow_runs
               WHERE NOT (status = 'skipped' AND parent_run_id IS NULL)
               ORDER BY started_at DESC, rowid DESC LIMIT 500
             )
             UNION ALL
             SELECT * FROM (
               SELECT * FROM workflow_runs
               WHERE status = 'skipped' AND parent_run_id IS NULL
               ORDER BY started_at DESC, rowid DESC LIMIT 500
             )
             UNION
             SELECT * FROM workflow_runs WHERE status = 'paused' OR run_id IN (
                 SELECT run_id FROM workflow_waits WHERE request_json IS NOT NULL AND signalled_at IS NULL
             )
             ORDER BY started_at DESC",
            [],
        )?,
        Some(status) => select(
            conn,
            "SELECT * FROM workflow_runs WHERE status = ?1 ORDER BY started_at DESC, rowid DESC LIMIT 500",
            [status],
        )?,
    };
    Ok(rows.iter().map(to_run).collect())
}

/// `Workflows.get`: the run, its steps and what it refers to.
fn get(conn: &Connection, run_id: &str) -> Result<Value, Refused> {
    let row = require_run(conn, run_id)?;
    let steps = select(
        conn,
        "SELECT name, kind, status, attempts, detail, thread_id, environment_id, result_json,
                said, started_at, ended_at
         FROM workflow_steps WHERE run_id = ?1 ORDER BY started_at, rowid",
        [run_id],
    )
    .map_err(unavailable)?;
    let references = select(
        conn,
        "SELECT kind, key, address_json, title, url, state, steps_json, origin
         FROM workflow_run_references WHERE run_id = ?1 ORDER BY rowid",
        [run_id],
    )
    .map_err(unavailable)?;
    Ok(json!({
        "run": to_run(&row),
        "references": references.iter().map(|r| json!({
            "kind": field(r, "kind"),
            "key": field(r, "key"),
            "address": json_column(r, "address_json"),
            "title": field(r, "title"),
            "url": field(r, "url"),
            "state": field(r, "state"),
            "steps": json_column(r, "steps_json"),
            "origin": field(r, "origin"),
        })).collect::<Vec<_>>(),
        "steps": steps.iter().map(|s| json!({
            "name": field(s, "name"),
            "kind": field(s, "kind"),
            "status": field(s, "status"),
            "attempts": field(s, "attempts"),
            "detail": field(s, "detail"),
            "threadId": field(s, "thread_id"),
            "environmentId": field(s, "environment_id"),
            "result": json_column(s, "result_json"),
            "said": field(s, "said"),
            "startedAt": field(s, "started_at"),
            "endedAt": field(s, "ended_at"),
        })).collect::<Vec<_>>(),
    }))
}

/// `questions`: what open waits ask a person, oldest first, with the thread
/// the run last worked in.
fn questions(conn: &Connection) -> rusqlite::Result<Vec<Value>> {
    let waits = select(
        conn,
        "SELECT run_id, name, request_json, opened_at FROM workflow_waits
         WHERE request_json IS NOT NULL AND signalled_at IS NULL ORDER BY opened_at",
        [],
    )?;
    waits
        .iter()
        .map(|wait| {
            let run_id = text(wait, "run_id");
            let thread = select(
                conn,
                "SELECT thread_id, environment_id FROM workflow_steps
                 WHERE run_id = ?1 AND thread_id IS NOT NULL
                 ORDER BY started_at DESC, rowid DESC LIMIT 1",
                [&run_id],
            )?
            .first()
            .map_or(Value::Null, |row| {
                json!({"threadId": field(row, "thread_id"), "environmentId": field(row, "environment_id")})
            });
            let request = json_column(wait, "request_json");
            let mut question = json!({
                "runId": run_id,
                "name": field(wait, "name"),
                "request": request,
                "askedAt": field(wait, "opened_at"),
                "thread": thread,
            });
            if let Some(kind) = request["kind"].as_str() {
                question["kind"] = json!(kind);
            }
            Ok(question)
        })
        .collect()
}

/// `KeyValues.list`: keys starting with `prefix`, in key order.
fn stored(conn: &Connection, prefix: &str) -> rusqlite::Result<Vec<Value>> {
    let after = format!("{prefix}\u{10FFFF}");
    Ok(select(
        conn,
        "SELECT key, value_json FROM workflow_store WHERE key >= ?1 AND key < ?2 ORDER BY key",
        params![prefix, after],
    )?
    .iter()
    .map(|row| json!({"key": field(row, "key"), "value": json_column(row, "value_json")}))
    .collect())
}

/// Where a signal goes: a run's wait, or the oldest open wait on a workflow's channel.
#[derive(Clone, Debug)]
pub(crate) enum Address {
    Run { run_id: String, wait: String },
    Channel { workflow: String, channel: String },
}

impl Address {
    /// `mailAddress`.
    fn mail(&self) -> String {
        match self {
            Address::Run { run_id, wait } => json!(["run", run_id, wait]).to_string(),
            Address::Channel { workflow, channel } => json!(["key", workflow, channel]).to_string(),
        }
    }
}

/// `waitAt`: the open wait at `address`.
fn wait_at(conn: &Connection, address: &Address) -> rusqlite::Result<Option<Map<String, Value>>> {
    let rows = match address {
        Address::Run { run_id, wait } => select(
            conn,
            "SELECT run_id, name, token, schema_json, request_json FROM workflow_waits
             WHERE run_id = ?1 AND name = ?2 AND signalled_at IS NULL",
            [run_id, wait],
        )?,
        Address::Channel { workflow, channel } => select(
            conn,
            "SELECT run_id, name, token, schema_json, request_json FROM workflow_waits
             WHERE workflow = ?1 AND key = ?2 AND signalled_at IS NULL
             ORDER BY opened_at LIMIT 1",
            [workflow, channel],
        )?,
    };
    Ok(rows.into_iter().next())
}

fn signal_seen(conn: &Connection, signal_id: &str) -> rusqlite::Result<bool> {
    conn.query_row(
        "SELECT 1 FROM workflow_signals WHERE signal_id = ?1",
        [signal_id],
        |_| Ok(()),
    )
    .optional()
    .map(|row| row.is_some())
}

/// `waitSignalled`: claims the wait for `woke`, once; a signal id that
/// already woke a wait claims nothing.
fn wait_signalled(
    conn: &mut Connection,
    run_id: &str,
    name: &str,
    woke: &Value,
    signal_id: Option<&str>,
) -> rusqlite::Result<bool> {
    let at = now();
    let transaction = conn.transaction()?;
    let taken = transaction
        .query_row(
            "UPDATE workflow_waits SET signalled_at = ?1, woke_json = ?2
             WHERE run_id = ?3 AND name = ?4 AND signalled_at IS NULL
             RETURNING name",
            params![at, woke.to_string(), run_id, name],
            |_| Ok(()),
        )
        .optional()?
        .is_some();
    if taken {
        if let Some(signal_id) = signal_id {
            let receipt = transaction
                .query_row(
                    "INSERT INTO workflow_signals (signal_id, kept_until)
                     VALUES (?1, MAX(?2, COALESCE(
                       (SELECT expires_at FROM workflow_mail WHERE signal_id = ?1), ''
                     )))
                     ON CONFLICT (signal_id) DO NOTHING
                     RETURNING signal_id",
                    params![signal_id, later(SEVEN_DAYS_MS)],
                    |_| Ok(()),
                )
                .optional()?;
            if receipt.is_none() {
                return Ok(false);
            }
        }
    }
    transaction.commit()?;
    if taken {
        changed();
    }
    Ok(taken)
}

/// `mailPut`: the signal kept for a wait that has not opened.
fn mail_put(
    conn: &mut Connection,
    signal_id: &str,
    address: &Address,
    value_json: &str,
    expires_at: &str,
    sender: &Value,
) -> rusqlite::Result<()> {
    let transaction = conn.transaction()?;
    transaction.execute(
        "INSERT INTO workflow_mail (signal_id, address, value_json, expires_at, sender_json)
         VALUES (?1, ?2, ?3, ?4, ?5) ON CONFLICT (signal_id) DO NOTHING",
        params![
            signal_id,
            address.mail(),
            value_json,
            expires_at,
            sender.to_string()
        ],
    )?;
    transaction.execute(
        "UPDATE workflow_signals SET kept_until = ?2 WHERE signal_id = ?1 AND kept_until < ?2",
        params![signal_id, expires_at],
    )?;
    transaction.commit()
}

#[derive(PartialEq, Eq)]
enum MailOutcome {
    Delivered,
    Claimed,
    Gone,
}

/// `mailDeliver`: the kept signal handed to a wait that opened.
fn mail_deliver(
    conn: &mut Connection,
    signal_id: &str,
    run_id: &str,
    name: &str,
    woke: &Value,
) -> rusqlite::Result<MailOutcome> {
    let at = now();
    let transaction = conn.transaction()?;
    if signal_seen(&transaction, signal_id)? {
        transaction.execute(
            "DELETE FROM workflow_mail WHERE signal_id = ?1",
            [signal_id],
        )?;
        transaction.commit()?;
        return Ok(MailOutcome::Gone);
    }
    let claimed = transaction
        .query_row(
            "UPDATE workflow_waits SET signalled_at = ?1, woke_json = ?2
             WHERE run_id = ?3 AND name = ?4 AND signalled_at IS NULL
             RETURNING name",
            params![at, woke.to_string(), run_id, name],
            |_| Ok(()),
        )
        .optional()?;
    if claimed.is_none() {
        transaction.commit()?;
        return Ok(MailOutcome::Claimed);
    }
    let copy: Option<String> = transaction
        .query_row(
            "DELETE FROM workflow_mail WHERE signal_id = ?1 RETURNING expires_at",
            [signal_id],
            |row| row.get(0),
        )
        .optional()?;
    // Failing undoes the claim: the dropped transaction rolls back.
    let Some(expires_at) = copy else {
        return Ok(MailOutcome::Gone);
    };
    let keep = later(SEVEN_DAYS_MS);
    let receipt = transaction
        .query_row(
            "INSERT INTO workflow_signals (signal_id, kept_until) VALUES (?1, ?2)
             ON CONFLICT (signal_id) DO NOTHING RETURNING signal_id",
            params![signal_id, if expires_at > keep { expires_at } else { keep }],
            |_| Ok(()),
        )
        .optional()?;
    if receipt.is_none() {
        return Ok(MailOutcome::Gone);
    }
    transaction.commit()?;
    changed();
    Ok(MailOutcome::Delivered)
}

/// `Workflows.signal`: wakes the wait at `address` with `value`, or keeps it
/// for one that opens later. With nothing registered the wake reaches no
/// engine: the claim is what stays.
fn signal(
    conn: &mut Connection,
    address: &Address,
    value: &Value,
    signal_id: &str,
    label: Option<&str>,
    session: Option<&str>,
) -> Result<&'static str, Refused> {
    if signal_seen(conn, signal_id).map_err(unavailable)? {
        return Ok("delivered");
    }
    let value_json = value.to_string();
    let mut sender = json!({});
    if let Some(label) = label {
        sender["answeredBy"] = json!(label);
    }
    if let Some(session) = session {
        sender["answeredBySession"] = json!(session);
    }
    sender["answeredAt"] = json!(now());
    let kept_sender = |conn: &Connection| -> rusqlite::Result<Option<Value>> {
        let kept: Option<String> = conn.query_row(
            "SELECT sender_json FROM workflow_mail WHERE signal_id = ?1 AND sender_json IS NOT NULL",
            [signal_id], |row| row.get(0),
        ).optional()?;
        Ok(kept.and_then(|text| serde_json::from_str(&text).ok()))
    };
    let wake = |wait: &Map<String, Value>, sender: &Value| {
        let mut answer = value.clone();
        let schema = wait["schema_json"].as_str().unwrap_or_default();
        if let Some(fields) = answer
            .as_object_mut()
            .filter(|_| !wait["request_json"].is_null() && schema.contains("\"answeredAt\""))
        {
            for key in ["answeredBy", "answeredBySession", "answeredAt"] {
                fields.shift_remove(key);
                if let Some(field) = sender.get(key).filter(|_| {
                    key != "answeredBySession" || schema.contains("\"answeredBySession\"")
                }) {
                    fields.insert(key.to_owned(), field.clone());
                }
            }
        }
        json!({"expired": false, "value": answer})
    };
    if let Some(first) = kept_sender(conn).map_err(unavailable)? {
        sender = first;
    }
    while let Some(wait) = wait_at(conn, address).map_err(unavailable)? {
        let name = text(&wait, "name");
        let woke = wake(&wait, &sender);
        if let Err(rejection) = crate::run_output::check_output(
            &json_column(&wait, "schema_json"),
            &woke["value"].to_string(),
        ) {
            return Err(invalid(format!(
                "The value does not fit what {name} waits for. {rejection}"
            )));
        }
        let run_id = text(&wait, "run_id");
        if wait_signalled(conn, &run_id, &name, &woke, Some(signal_id)).map_err(unavailable)? {
            return Ok("delivered");
        }
        if signal_seen(conn, signal_id).map_err(unavailable)? {
            return Ok("delivered");
        }
    }
    let expires_at = later(SEVEN_DAYS_MS);
    if signal_seen(conn, signal_id).map_err(unavailable)? {
        return Ok("delivered");
    }
    mail_put(conn, signal_id, address, &value_json, &expires_at, &sender).map_err(unavailable)?;
    if let Some(first) = kept_sender(conn).map_err(unavailable)? {
        sender = first;
    }
    loop {
        let Some(late) = wait_at(conn, address).map_err(unavailable)? else {
            return Ok("buffered");
        };
        let woke = wake(&late, &sender);
        if crate::run_output::check_output(
            &json_column(&late, "schema_json"),
            &woke["value"].to_string(),
        )
        .is_err()
        {
            return Ok("buffered");
        }
        match mail_deliver(
            conn,
            signal_id,
            &text(&late, "run_id"),
            &text(&late, "name"),
            &woke,
        )
        .map_err(unavailable)?
        {
            MailOutcome::Delivered | MailOutcome::Gone => return Ok("delivered"),
            MailOutcome::Claimed => {}
        }
    }
}

/// `runEnded`: ends the run unless it ended first, closing its waits.
fn run_ended(
    conn: &mut Connection,
    run_id: &str,
    status: &str,
    detail: Option<&str>,
) -> rusqlite::Result<bool> {
    let at = now();
    let transaction = conn.transaction()?;
    let ended = transaction
        .query_row(
            "UPDATE workflow_runs
             SET status = ?1, detail = ?2, result_json = 'null', pauses = 0, updated_at = ?3,
                 ended_at = ?3
             WHERE run_id = ?4 AND ended_at IS NULL
             RETURNING run_id",
            params![status, detail, at, run_id],
            |_| Ok(()),
        )
        .optional()?
        .is_some();
    transaction.execute("DELETE FROM workflow_waits WHERE run_id = ?1", [run_id])?;
    transaction.commit()?;
    changed();
    Ok(ended)
}

/// `ended`: tells the run that invoked this one, if one did, how it ended.
fn ended(conn: &mut Connection, run_id: &str) {
    let Ok(Some(row)) = run_row(conn, run_id) else {
        return;
    };
    let (Some(parent), Some(wait)) = (
        row.get("parent_run_id").and_then(Value::as_str),
        row.get("parent_wait").and_then(Value::as_str),
    ) else {
        return;
    };
    if row.get("ended_at").is_none_or(Value::is_null) {
        return;
    }
    let mut value =
        json!({"outcome": field(&row, "status"), "value": json_column(&row, "result_json")});
    if let Some(detail) = row.get("detail").filter(|d| !d.is_null()) {
        value["reason"] = detail.clone();
    }
    let address = Address::Run {
        run_id: parent.to_owned(),
        wait: wait.to_owned(),
    };
    let _ = signal(
        conn,
        &address,
        &value,
        &format!("{run_id}/ended"),
        None,
        None,
    );
}

/// A step's thread still working for the run (`runningThreads`).
struct RunningThread {
    thread_id: String,
    environment_id: Option<String>,
    turn_id: Option<String>,
}

fn running_threads(conn: &Connection, run_id: &str) -> rusqlite::Result<Vec<RunningThread>> {
    Ok(select(
        conn,
        "SELECT thread_id, environment_id, turn_id FROM workflow_steps
         WHERE run_id = ?1 AND status = 'running' AND thread_id IS NOT NULL",
        [run_id],
    )?
    .iter()
    .map(|row| RunningThread {
        thread_id: text(row, "thread_id"),
        environment_id: row
            .get("environment_id")
            .and_then(Value::as_str)
            .map(str::to_owned),
        turn_id: row
            .get("turn_id")
            .and_then(Value::as_str)
            .map(str::to_owned),
    })
    .collect())
}

/// `WorkflowAgents.interrupt` for a thread here: its turn is stopped. A
/// thread in a linked environment is not reached yet.
async fn stop_thread(state: &Arc<AppState>, thread: &RunningThread) {
    if thread
        .environment_id
        .as_deref()
        .is_some_and(|id| id != crate::mcp_oauth::McpHost::environment_id(&**state))
    {
        return;
    }
    let known = state
        .control
        .store
        .read()
        .await
        .threads
        .contains_key(&thread.thread_id);
    if !known {
        return;
    }
    let command_id = format!(
        "workflow:interrupt:{}:{}:{}",
        thread.thread_id,
        thread.turn_id.as_deref().unwrap_or_default(),
        crate::platform::now_ms()
    );
    let command = match &thread.turn_id {
        Some(turn) => json!({"type": "run.interrupt", "commandId": command_id,
            "threadId": thread.thread_id, "runId": turn, "reason": "Its workflow run was cancelled"}),
        None => json!({"type": "thread.stop", "commandId": command_id,
            "threadId": thread.thread_id, "reason": "Its workflow run was cancelled"}),
    };
    let _ = crate::dispatch_orchestration_command(state.clone(), command).await;
}

/// `Workflows.cancel`: ends the run as cancelled, tells its parent, stops
/// what its steps were doing, and cancels what it invoked. With nothing
/// registered no cleanup (`onEnd`) runs.
fn cancel<'a>(
    state: &'a Arc<AppState>,
    conn: &'a mut Connection,
    run_id: &'a str,
    reason: Option<&'a str>,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), Refused>> + Send + 'a>> {
    Box::pin(async move {
        let row = require_run(conn, run_id)?;
        if !row.get("ended_at").is_none_or(Value::is_null) {
            return Ok(());
        }
        let threads = running_threads(conn, run_id).map_err(unavailable)?;
        if !run_ended(conn, run_id, "cancelled", reason).map_err(unavailable)? {
            return Ok(());
        }
        ended(conn, run_id);
        for thread in &threads {
            stop_thread(state, thread).await;
        }
        conn.execute(
            "UPDATE workflow_steps SET status = 'failed', detail = 'cancelled', ended_at = ?1
             WHERE run_id = ?2 AND status = 'running' AND name != ?3",
            params![now(), run_id, END_STEP],
        )
        .map_err(unavailable)?;
        changed();
        let children = select(
            conn,
            "SELECT run_id FROM workflow_runs WHERE parent_run_id = ?1 AND ended_at IS NULL",
            [run_id],
        )
        .map_err(unavailable)?;
        for child in children {
            let child = text(&child, "run_id");
            let _ = cancel(state, &mut *conn, &child, reason).await;
        }
        Ok(())
    })
}

fn store_set(conn: &Connection, key: &str, value: &Value) -> rusqlite::Result<()> {
    conn.execute(
        "INSERT INTO workflow_store (key, value_json, updated_at) VALUES (?1, ?2, ?3)
         ON CONFLICT (key) DO UPDATE
         SET value_json = excluded.value_json, updated_at = excluded.updated_at",
        params![key, value.to_string(), now()],
    )
    .map(|_| ())
}

/// The schema's refusal of a blank text, as effect words it.
fn blank(name: &str) -> Refused {
    (
        "invalid_arguments",
        format!("Expected a non-blank string\n  at [{}]", json!(name)),
    )
}

/// A text argument as the reference's `TrimmedNonEmptyString` decodes it.
fn trimmed(input: &Value, name: &str) -> Result<Option<String>, Refused> {
    match input.get(name) {
        None => Ok(None),
        Some(value) => match value.as_str().map(str::trim) {
            Some(text) if !text.is_empty() => Ok(Some(text.to_owned())),
            _ => Err(blank(name)),
        },
    }
}

/// A name argument as the reference's `ExactName` decodes it: kept as given, never blank.
fn exact(input: &Value, name: &str) -> Result<Option<String>, Refused> {
    match input.get(name) {
        None => Ok(None),
        Some(value) => match value.as_str() {
            Some(text) if !text.trim().is_empty() => Ok(Some(text.to_owned())),
            _ => Err(blank(name)),
        },
    }
}

fn required<T>(value: Option<T>, name: &str) -> Result<T, Refused> {
    value.ok_or_else(|| invalid(format!("This action needs `{name}`.")))
}

/// A run as the tool answers it: its output schema keeps the run and its
/// steps, not what it refers to.
fn answered(mut run: Value) -> Value {
    if let Some(fields) = run.as_object_mut() {
        fields.remove("references");
    }
    run
}

/// The refusal a service with no engine gives a change.
fn refusal(mode: Mode) -> Refused {
    invalid(if mode == Mode::Off { OFF } else { DISABLED })
}

/// `t3_workflow`, after its caller check: the handler's answer for `input`.
/// `Err(("invalid_arguments", problem))` is an argument its schema turns down.
pub(crate) async fn tool(
    state: &Arc<AppState>,
    input: &Value,
    client_label: &str,
) -> Result<Value, Refused> {
    let action = input["action"].as_str().unwrap_or_default();
    let workflow = trimmed(input, "workflow")?;
    let idempotency_key = trimmed(input, "idempotencyKey")?;
    let run_id = trimmed(input, "runId")?;
    let wait = exact(input, "wait")?;
    let key = exact(input, "key")?;
    let channel = trimmed(input, "channel")?;
    let signal_id = trimmed(input, "signalId")?;
    let reason = input.get("reason").and_then(Value::as_str);
    let status = input.get("status").and_then(Value::as_str);
    let mut conn = open(state).map_err(unavailable)?;
    let mode = mode(state, &conn);
    let off = || invalid(OFF);
    if READ_ACTIONS.contains(&action) {
        return match action {
            "questions" => match mode {
                Mode::Off => Err(off()),
                Mode::Disabled => Ok(json!({"questions": []})),
                Mode::History => {
                    let mut open = questions(&conn).map_err(unavailable)?;
                    if let Some(kind) = input["kind"].as_str() {
                        open.retain(|question| question["kind"] == kind);
                    }
                    Ok(json!({"questions": open}))
                }
            },
            "store" => match mode {
                Mode::Off => Err(off()),
                Mode::Disabled => Ok(json!({"entries": []})),
                Mode::History => Ok(
                    json!({"entries": stored(&conn, key.as_deref().unwrap_or_default()).map_err(unavailable)?}),
                ),
            },
            "list" => match mode {
                Mode::Off => Err(off()),
                Mode::Disabled => Ok(json!({"runs": []})),
                Mode::History if input["tag"].is_string() || input["subject"].is_string() => {
                    let rows = select(&conn,
                        "SELECT * FROM workflow_runs AS r
                         WHERE (?1 IS NULL OR status = ?1)
                           AND (?2 IS NULL OR EXISTS (SELECT 1 FROM json_each(r.tags_json) WHERE value = ?2))
                           AND (?3 IS NULL OR subject_key = ?3 OR subject_url = ?3)
                         ORDER BY started_at DESC, rowid DESC LIMIT 500",
                        params![status, input["tag"].as_str(), input["subject"].as_str()],
                    ).map_err(unavailable)?;
                    Ok(json!({"runs": rows.iter().map(to_run).collect::<Vec<_>>()}))
                }
                Mode::History => Ok(json!({"runs": runs(&conn, status).map_err(unavailable)?})),
            },
            _ => {
                let run_id = required(run_id, "runId")?;
                match mode {
                    Mode::Off => Err(off()),
                    Mode::Disabled => Err(not_found(&run_id)),
                    Mode::History => get(&conn, &run_id).map(answered),
                }
            }
        };
    }
    match action {
        "hooks" => match mode {
            Mode::Off => Err(off()),
            _ => Ok(json!({"hooks": []})),
        },
        "start" => {
            let name = required(workflow, "workflow")?;
            required(idempotency_key, "idempotencyKey")?;
            match mode {
                Mode::History => Err(not_registered(&name)),
                _ => Err(refusal(mode)),
            }
        }
        "signal" => {
            let value = required(input.get("value"), "value")?;
            let by_run = run_id.is_some() || wait.is_some();
            let by_channel = workflow.is_some() || channel.is_some();
            if by_run && by_channel {
                return Err(invalid(
                    "Address a signal with `runId` and `wait`, or with `workflow` and `channel`, not both.",
                ));
            }
            let address =
                match (run_id, wait, workflow, channel) {
                    (Some(run_id), Some(wait), _, _) => Address::Run { run_id, wait },
                    (_, _, Some(workflow), Some(channel)) => Address::Channel { workflow, channel },
                    _ => return Err(invalid(
                        "Address a signal with `runId` and `wait`, or `workflow` and `channel`.",
                    )),
                };
            let id = signal_id.unwrap_or_else(|| crate::platform::uuid_v4().to_string());
            if mode != Mode::History {
                return Err(refusal(mode));
            }
            let delivery = signal(&mut conn, &address, value, &id, Some(client_label), None)?;
            Ok(json!({"delivery": delivery}))
        }
        "store_set" | "store_delete" => {
            let key = required(key, "key")?;
            let value = if action == "store_set" {
                Some(required(input.get("value"), "value")?)
            } else {
                None
            };
            if mode != Mode::History {
                return Err(refusal(mode));
            }
            match value {
                Some(value) => store_set(&conn, &key, value),
                None => conn
                    .execute("DELETE FROM workflow_store WHERE key = ?1", [&key])
                    .map(|_| ()),
            }
            .map_err(unavailable)?;
            Ok(json!({"entries": stored(&conn, &key).map_err(unavailable)?}))
        }
        _ => {
            let run_id = required(run_id, "runId")?;
            if mode != Mode::History {
                return Err(refusal(mode));
            }
            if action == "resume" {
                let row = require_run(&conn, &run_id)?;
                return Err(not_registered(&text(&row, "engine_tag")));
            }
            cancel(state, &mut conn, &run_id, reason).await?;
            get(&conn, &run_id).map(answered)
        }
    }
}

fn enabled(state: &AppState) -> bool {
    state.settings.internal().unwrap_or_default()["workflowsEnabled"] == true
}

/// What the definitions loader says of an empty folder: nothing reported
/// yet, nothing wrong. The snapshot of a server with workflows on carries it.
fn definition() -> Value {
    json!({"revision": 0, "problem": null})
}

/// `Workflows.watch`'s snapshot: the workflows registered (none), the recent
/// runs and the open questions; with workflows on, also the definitions' state.
fn snapshot(state: &AppState) -> Value {
    let empty = json!({"workflows": [], "runs": [], "questions": []});
    let Ok(conn) = open(state) else {
        return empty;
    };
    match mode(state, &conn) {
        Mode::Off => empty,
        Mode::Disabled => {
            json!({"workflows": [], "runs": [], "questions": [], "definition": definition()})
        }
        Mode::History => json!({
            "workflows": [],
            "runs": runs(&conn, None).unwrap_or_default(),
            "questions": questions(&conn).unwrap_or_default(),
            "definition": definition(),
        }),
    }
}

/// `WorkflowStore.counts`: what waits on a person, what has not ended, and every run.
fn counts(state: &AppState) -> Value {
    let none = json!({"needsYou": 0, "active": 0, "total": 0});
    let Ok(conn) = open(state) else {
        return none;
    };
    if mode(state, &conn) != Mode::History {
        return none;
    }
    conn.query_row(
        "SELECT
           (SELECT COUNT(*) FROM workflow_waits
             WHERE request_json IS NOT NULL AND signalled_at IS NULL),
           (SELECT COUNT(*) FROM workflow_runs WHERE status = 'paused'),
           (SELECT COUNT(*) FROM workflow_runs WHERE ended_at IS NULL),
           (SELECT COUNT(*) FROM workflow_runs)",
        [],
        |row| {
            let (questions, paused, active, total): (i64, i64, i64, i64) =
                (row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?);
            Ok(json!({"needsYou": questions + paused, "active": active, "total": total}))
        },
    )
    .unwrap_or(none)
}

/// How long a burst of writes settles before the streams look again (`WATCH_SETTLE`).
const SETTLE: std::time::Duration = std::time::Duration::from_millis(100);

/// `workflows.subscribe` (`counts` false) and `workflows.counts`: the value
/// now, and again once a burst of changes to runs settles. Counts that did
/// not move are not sent again. Turning workflows on or off starts over with
/// the other service's value. Never returns.
pub(crate) async fn stream<F, Fut>(state: &Arc<AppState>, counted: bool, send: F)
where
    F: Fn(Value) -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    let mut settings = state.settings.subscribe();
    let mut changes = CHANGES.subscribe();
    let mut on = enabled(state);
    let mut last: Option<Value> = None;
    loop {
        let value = if counted {
            counts(state)
        } else {
            snapshot(state)
        };
        if !counted || last.as_ref() != Some(&value) {
            send(json!([value.clone()])).await;
        }
        last = Some(value);
        // Off, or with nothing kept, the service's streams say nothing more.
        let live = on && open(state).is_ok_and(|conn| mode(state, &conn) == Mode::History);
        loop {
            tokio::select! {
                change = changes.recv(), if live => {
                    if matches!(change, Err(broadcast::error::RecvError::Closed)) {
                        return std::future::pending().await;
                    }
                    while tokio::time::timeout(SETTLE, changes.recv()).await.is_ok() {}
                    break;
                }
                turned = settings.recv() => {
                    if matches!(turned, Err(broadcast::error::RecvError::Closed)) {
                        return std::future::pending().await;
                    }
                    if enabled(state) != on {
                        on = !on;
                        last = None;
                        break;
                    }
                }
            }
        }
    }
}

/// `WorkflowRequestError`.
fn request_error((_, message): Refused) -> Value {
    json!({"_tag": "WorkflowRequestError", "message": message})
}

/// `workflows.get`: a run with its steps and what it refers to.
pub(crate) fn get_run(state: &AppState, run_id: &str) -> Result<Value, Value> {
    let conn = open(state).map_err(|e| request_error(unavailable(e)))?;
    match mode(state, &conn) {
        Mode::Off => Err(request_error(invalid(OFF))),
        Mode::Disabled => Err(request_error(not_found(run_id))),
        Mode::History => get(&conn, run_id).map_err(request_error),
    }
}

/// `workflows.answer` (`Workflows.answer`): a person's answer to the question
/// a run asks, as one signal per asking. A question answered already, or never
/// asked, takes nothing.
pub(crate) fn answer(
    state: &AppState,
    params: &Value,
    principal: &crate::auth_service::Session,
) -> Result<(), Value> {
    let mut conn = open(state).map_err(|e| request_error(unavailable(e)))?;
    let mode = mode(state, &conn);
    if mode != Mode::History {
        return Err(request_error(refusal(mode)));
    }
    let address = Address::Run {
        run_id: params["runId"].as_str().unwrap_or_default().to_owned(),
        wait: params["name"].as_str().unwrap_or_default().to_owned(),
    };
    let Some(wait) = wait_at(&conn, &address).map_err(|e| request_error(unavailable(e)))? else {
        return Ok(());
    };
    if wait.get("request_json").is_none_or(Value::is_null) {
        return Ok(());
    }
    let mut value = json!({"choice": params["choice"]});
    if let Some(reason) = params.get("reason") {
        value["reason"] = reason.clone();
    }
    let id = format!("answer:{}", text(&wait, "token"));
    signal(
        &mut conn,
        &address,
        &value,
        &id,
        Some(&crate::peer_thread_relay::client_label(
            principal.client_metadata(),
        )),
        Some(&principal.id),
    )
    .map(|_| ())
    .map_err(request_error)
}

/// `workflows.resume` (`Workflows.resume`): with nothing registered, a run
/// that is there answers that its workflow is not registered.
pub(crate) fn resume_run(state: &AppState, run_id: &str) -> Result<(), Value> {
    let conn = open(state).map_err(|e| request_error(unavailable(e)))?;
    let mode = mode(state, &conn);
    if mode != Mode::History {
        return Err(request_error(refusal(mode)));
    }
    let row = require_run(&conn, run_id).map_err(request_error)?;
    Err(request_error(not_registered(&text(&row, "engine_tag"))))
}

/// `workflows.cancel` (`Workflows.cancel`): ends the run as cancelled, with
/// what it invoked, as `t3_workflow`'s cancel does.
pub(crate) async fn cancel_run(
    state: &Arc<AppState>,
    run_id: &str,
    reason: Option<&str>,
) -> Result<(), Value> {
    let mut conn = open(state).map_err(|e| request_error(unavailable(e)))?;
    let mode = mode(state, &conn);
    if mode != Mode::History {
        return Err(request_error(refusal(mode)));
    }
    cancel(state, &mut conn, run_id, reason)
        .await
        .map_err(request_error)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mail_addresses_are_tuples() {
        let run = Address::Run {
            run_id: "r".into(),
            wait: "go".into(),
        };
        assert_eq!(run.mail(), r#"["run","r","go"]"#);
        let channel = Address::Channel {
            workflow: "w".into(),
            channel: "c".into(),
        };
        assert_eq!(channel.mail(), r#"["key","w","c"]"#);
    }

    #[test]
    fn arguments_decode_as_the_reference_decodes_them() {
        let input = json!({"runId": "  r1 ", "wait": " go ", "key": ""});
        assert_eq!(trimmed(&input, "runId").unwrap().as_deref(), Some("r1"));
        assert_eq!(exact(&input, "wait").unwrap().as_deref(), Some(" go "));
        assert_eq!(
            exact(&input, "key").unwrap_err().1,
            "Expected a non-blank string\n  at [\"key\"]"
        );
        assert_eq!(trimmed(&input, "channel").unwrap(), None);
        assert_eq!(
            required::<String>(None, "runId").unwrap_err().1,
            "This action needs `runId`."
        );
    }
}
