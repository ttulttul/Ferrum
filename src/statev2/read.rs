//! Reads the reference's projections out of `statev2.sqlite`: projects from
//! `projection_projects` and each thread's `OrchestrationV2ThreadProjection`
//! from the `orchestration_v2_projection_*` tables, in the order the
//! reference's `ProjectionStore.readCanonicalProjection` reads a whole thread
//! (no window, no filter).
use rusqlite::{params, Connection, OptionalExtension};
use serde_json::{json, Map, Value};

/// A project row as the reference's `ProjectStore` reads it, in Rust's wire
/// shape (`wire_project`).
pub fn projects(conn: &Connection) -> rusqlite::Result<Vec<Value>> {
    let mut statement = conn.prepare(
        "SELECT project_id, title, has_custom_title, workspace_root, default_model_selection_json,
                default_thread_env_mode, auto_pull, favicon_path, project_icon_json, scripts_json,
                pinned_at, archived_at, created_at, updated_at, deleted_at, tracked_folders_json
         FROM projection_projects
         ORDER BY created_at ASC, project_id ASC",
    )?;
    let rows = statement.query_map([], |row| {
        let json_column = |index: usize| -> rusqlite::Result<Value> {
            Ok(row
                .get::<_, Option<String>>(index)?
                .and_then(|text| serde_json::from_str(&text).ok())
                .unwrap_or(Value::Null))
        };
        Ok(json!({
            "id": row.get::<_, String>(0)?,
            "title": row.get::<_, String>(1)?,
            "hasCustomTitle": row.get::<_, i64>(2)? != 0,
            "workspaceRoot": row.get::<_, String>(3)?,
            "repositoryIdentity": null,
            "faviconPath": row.get::<_, Option<String>>(7)?,
            "projectIcon": json_column(8)?,
            "defaultModelSelection": json_column(4)?,
            "defaultThreadEnvMode": row.get::<_, Option<String>>(5)?,
            "autoPull": row.get::<_, i64>(6)? != 0,
            "scripts": match json_column(9)? { Value::Null => json!([]), scripts => scripts },
            // A project's nested repositories (#35925).
            "trackedFolders": match json_column(15)? { folders @ Value::Array(_) => folders, _ => json!([]) },
            "pinnedAt": row.get::<_, Option<String>>(10)?,
            "archivedAt": row.get::<_, Option<String>>(11)?,
            "createdAt": row.get::<_, String>(12)?,
            "updatedAt": row.get::<_, String>(13)?,
            "deletedAt": row.get::<_, Option<String>>(14)?,
        }))
    })?;
    rows.collect()
}

/// Migration 116 snapshots the source runs at fork creation. A historical
/// fork has no snapshot and inherits a run rolled back at or after its creation.
pub(crate) fn fork_source_run_ids(
    conn: &Connection,
    fork_id: &str,
) -> rusqlite::Result<std::collections::HashSet<String>> {
    let ids: Option<String> = conn.query_row(
        "SELECT COALESCE(fork.fork_run_ids_json, (
           SELECT json_group_array(run.run_id)
           FROM orchestration_v2_projection_runs AS run
           WHERE run.thread_id = json_extract(fork.payload_json, '$.forkedFrom.threadId')
             AND run.ordinal <= (SELECT ordinal FROM orchestration_v2_projection_runs
                                 WHERE run_id = json_extract(fork.payload_json, '$.forkedFrom.runId'))
             AND NOT COALESCE(run.status = 'rolled_back' AND
                              julianday(run.completed_at) < julianday(fork.created_at), 0)
         )) FROM orchestration_v2_projection_threads AS fork WHERE fork.thread_id = ?1",
        [fork_id],
        |row| row.get(0),
    ).optional()?;
    serde_json::from_str(ids.as_deref().unwrap_or("[]")).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Text, Box::new(error))
    })
}

/// The ids of threads that are not deleted, oldest first.
pub fn thread_ids(conn: &Connection) -> rusqlite::Result<Vec<String>> {
    let mut statement = conn.prepare(
        "SELECT thread_id FROM orchestration_v2_projection_threads
         WHERE deleted_at IS NULL
         ORDER BY created_at ASC, thread_id ASC",
    )?;
    let rows = statement.query_map([], |row| row.get(0))?;
    rows.collect()
}

/// The ids of threads the database marks deleted.
pub fn deleted_thread_ids(conn: &Connection) -> rusqlite::Result<Vec<String>> {
    let mut statement = conn.prepare(
        "SELECT thread_id FROM orchestration_v2_projection_threads WHERE deleted_at IS NOT NULL",
    )?;
    let rows = statement.query_map([], |row| row.get(0))?;
    rows.collect()
}

/// The latest sequence of any V2 thread event, the shell's snapshot sequence.
pub fn latest_sequence(conn: &Connection) -> rusqlite::Result<u64> {
    conn.query_row(
        "SELECT COALESCE(MAX(sequence), 0) FROM orchestration_events
         WHERE application_event_version = 2",
        [],
        |row| row.get::<_, i64>(0),
    )
    .map(|sequence| sequence.max(0) as u64)
}

/// The latest sequence of any event.
pub fn max_sequence(conn: &Connection) -> rusqlite::Result<u64> {
    conn.query_row(
        "SELECT COALESCE(MAX(sequence), 0) FROM orchestration_events",
        [],
        |row| row.get::<_, i64>(0),
    )
    .map(|sequence| sequence.max(0) as u64)
}

/// The latest event sequence of one project's stream.
pub fn project_sequence(conn: &Connection, project_id: &str) -> rusqlite::Result<u64> {
    conn.query_row(
        "SELECT COALESCE(MAX(sequence), 0) FROM orchestration_events
         WHERE application_event_version = 2 AND aggregate_kind = 'project' AND stream_id = ?1",
        [project_id],
        |row| row.get::<_, i64>(0),
    )
    .map(|sequence| sequence.max(0) as u64)
}

/// The latest event sequence of one thread's stream.
pub fn thread_sequence(conn: &Connection, thread_id: &str) -> rusqlite::Result<u64> {
    conn.query_row(
        "SELECT COALESCE(MAX(sequence), 0) FROM orchestration_events
         WHERE application_event_version = 2 AND aggregate_kind = 'thread' AND stream_id = ?1",
        [thread_id],
        |row| row.get::<_, i64>(0),
    )
    .map(|sequence| sequence.max(0) as u64)
}

fn payloads(conn: &Connection, sql: &str, thread_id: &str) -> rusqlite::Result<Vec<Value>> {
    let mut statement = conn.prepare_cached(sql)?;
    let rows = statement.query_map(params![thread_id], |row| row.get::<_, String>(0))?;
    let mut out = Vec::new();
    for text in rows {
        let text = text?;
        // The reference decodes every row strictly and fails the whole read
        // on a bad one; a row that is not JSON is unreadable here too.
        let value = serde_json::from_str(&text).map_err(|error| {
            rusqlite::Error::FromSqlConversionFailure(
                0,
                rusqlite::types::Type::Text,
                Box::new(error),
            )
        })?;
        out.push(value);
    }
    Ok(out)
}

/// One thread's projection, `None` when the database has no row for it.
/// The collections are the stored records (`payload_json`), complete.
pub fn thread_projection(conn: &Connection, thread_id: &str) -> rusqlite::Result<Option<Value>> {
    let Some(thread) = conn
        .query_row(
            "SELECT payload_json FROM orchestration_v2_projection_threads WHERE thread_id = ?1 LIMIT 1",
            [thread_id],
            |row| row.get::<_, String>(0),
        )
        .optional()?
    else {
        return Ok(None);
    };
    let thread: Value = serde_json::from_str(&thread).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Text, Box::new(error))
    })?;
    let read = |sql: &str| payloads(conn, sql, thread_id);
    let mut projection = Map::new();
    projection.insert("thread".into(), thread.clone());
    for (key, sql) in [
        ("runs", "SELECT payload_json FROM orchestration_v2_projection_runs WHERE thread_id = ?1 ORDER BY ordinal ASC"),
        ("attempts", "SELECT payload_json FROM orchestration_v2_projection_run_attempts WHERE thread_id = ?1 ORDER BY run_id ASC, attempt_ordinal ASC"),
        ("nodes", "SELECT payload_json FROM orchestration_v2_projection_nodes WHERE thread_id = ?1 ORDER BY COALESCE(started_at, ''), node_id ASC"),
        ("subagents", "SELECT payload_json FROM orchestration_v2_projection_subagents WHERE thread_id = ?1 ORDER BY COALESCE(started_at, ''), subagent_id ASC"),
        ("providerSessions", "SELECT sessions.payload_json FROM orchestration_v2_projection_provider_sessions AS sessions
            INNER JOIN orchestration_v2_projection_provider_session_bindings AS bindings
              ON bindings.provider_session_id = sessions.provider_session_id
            WHERE bindings.thread_id = ?1
            ORDER BY sessions.updated_at ASC, sessions.provider_session_id ASC"),
        ("providerThreads", "SELECT payload_json FROM orchestration_v2_projection_provider_threads WHERE thread_id = ?1 ORDER BY COALESCE(first_run_ordinal, 0), provider_thread_id ASC"),
        ("providerTurns", "SELECT payload_json FROM orchestration_v2_projection_provider_turns WHERE thread_id = ?1 ORDER BY provider_thread_id ASC, ordinal ASC"),
        ("runtimeRequests", "SELECT payload_json FROM orchestration_v2_projection_runtime_requests WHERE thread_id = ?1 ORDER BY created_at ASC, runtime_request_id ASC"),
        ("messages", "SELECT payload_json FROM orchestration_v2_projection_messages WHERE thread_id = ?1 ORDER BY created_at ASC, message_id ASC"),
        ("plans", "SELECT payload_json FROM orchestration_v2_projection_plans WHERE thread_id = ?1 ORDER BY plan_id ASC"),
        ("turnItems", "SELECT payload_json FROM orchestration_v2_projection_turn_items WHERE thread_id = ?1 ORDER BY ordinal ASC, turn_item_id ASC"),
        ("checkpointScopes", "SELECT payload_json FROM orchestration_v2_projection_checkpoint_scopes WHERE thread_id = ?1 ORDER BY ordinal_within_parent ASC, scope_id ASC"),
        ("checkpoints", "SELECT payload_json FROM orchestration_v2_projection_checkpoints WHERE thread_id = ?1 ORDER BY scope_id ASC, ordinal_within_scope ASC"),
        ("contextHandoffs", "SELECT payload_json FROM orchestration_v2_projection_context_handoffs WHERE thread_id = ?1 ORDER BY rowid ASC"),
        ("contextTransfers", "SELECT payload_json FROM orchestration_v2_projection_context_transfers WHERE source_thread_id = ?1 OR target_thread_id = ?1 ORDER BY rowid ASC"),
    ] {
        projection.insert(key.into(), Value::Array(read(sql)?));
    }
    projection.insert("updatedAt".into(), thread["updatedAt"].clone());
    Ok(Some(Value::Object(projection)))
}

/// The turn items a shell row looks at (`projection_shell::shell` and its
/// visibility rule), by `(type, status)`, a `None` status meaning any:
/// failed compactions and errors, stop requests and their results, running or
/// waiting execution items, and waiting link and secret cards. Queued input
/// (`queued_turn` user messages) is read separately, by its own index.
const SHELL_ITEM_KINDS: &str = "VALUES ('compaction', 'failed'), ('error', 'failed'),
    ('run_interrupt_request', NULL), ('run_interrupt_result', NULL),
    ('command_execution', 'running'), ('command_execution', 'waiting'),
    ('file_change', 'running'), ('file_change', 'waiting'),
    ('file_search', 'running'), ('file_search', 'waiting'),
    ('web_search', 'running'), ('web_search', 'waiting'),
    ('dynamic_tool', 'running'), ('dynamic_tool', 'waiting'),
    ('mcp_tool_call', 'running'), ('mcp_tool_call', 'waiting'),
    ('subagent', 'running'), ('subagent', 'waiting'),
    ('link_request', 'waiting'), ('secret_request', 'waiting')";

fn decode(text: &str) -> rusqlite::Result<Value> {
    serde_json::from_str(text).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Text, Box::new(error))
    })
}

#[cfg(test)]
/// Every statement `slim_projections` ran (in any test).
pub(crate) static SLIM_STATEMENTS: std::sync::Mutex<Vec<String>> =
    std::sync::Mutex::new(Vec::new());

fn prepare_slim<'c>(conn: &'c Connection, sql: &str) -> rusqlite::Result<rusqlite::Statement<'c>> {
    #[cfg(test)]
    SLIM_STATEMENTS
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .push(sql.to_owned());
    conn.prepare(sql)
}

/// A row's columns, and its last column decoded when that is a `payload_json`.
type SlimRow = (Vec<rusqlite::types::Value>, Value);

fn slim_rows(conn: &Connection, sql: &str) -> rusqlite::Result<Vec<SlimRow>> {
    let mut statement = prepare_slim(conn, sql)?;
    let count = statement.column_count();
    let payload = count > 0 && statement.column_name(count - 1)? == "payload_json";
    let columns = if payload { count - 1 } else { count };
    let mut out = Vec::new();
    let mut rows = statement.query([])?;
    while let Some(row) = rows.next()? {
        let values = (0..columns)
            .map(|index| row.get::<_, rusqlite::types::Value>(index))
            .collect::<rusqlite::Result<Vec<_>>>()?;
        let value = if payload {
            decode(&row.get::<_, String>(columns)?)?
        } else {
            Value::Null
        };
        out.push((values, value));
    }
    Ok(out)
}

/// A connection to the database file at `path`, for reading beside the
/// start-up connection.
fn reader_at(path: &str) -> rusqlite::Result<Connection> {
    let reader = Connection::open(path)?;
    reader.busy_timeout(std::time::Duration::from_millis(5000))?;
    Ok(reader)
}

/// A read `in_parallel` runs on a connection of its own.
type Job<'a, T> = Box<dyn FnOnce(&Connection) -> rusqlite::Result<T> + Send + 'a>;

/// Each of `jobs` on a connection of its own, all at once: start-up reads
/// many tables of a large file, and a cold page cache serves them much
/// faster together than one after another. Nothing writes the database while
/// start-up reads it (this process owns the home and has not committed yet).
fn in_parallel<T: Send>(conn: &Connection, jobs: Vec<Job<'_, T>>) -> rusqlite::Result<Vec<T>> {
    let Some(path) = conn.path().filter(|path| !path.is_empty()) else {
        return jobs.into_iter().map(|job| job(conn)).collect();
    };
    std::thread::scope(|scope| {
        let handles: Vec<_> = jobs
            .into_iter()
            .map(|job| scope.spawn(move || job(&reader_at(path)?)))
            .collect();
        handles
            .into_iter()
            .map(|handle| handle.join().expect("a start-up reader panicked"))
            .collect()
    })
}

fn text(value: &rusqlite::types::Value) -> Option<&str> {
    match value {
        rusqlite::types::Value::Text(text) => Some(text),
        _ => None,
    }
}

/// The slim records of the threads `ids` names (`stored_projection::Slim`),
/// read with one query per table rather than one per thread, the tables at
/// once. Each collection holds what `thread_projection` gives, in its order,
/// except `nodes`, `subagents` and `contextTransfers` (empty: neither the
/// typed state nor the shell row reads them), `turnItems` (only what the
/// shell row looks at) and `messages` (only what those read). A thread the
/// database has no live row for is left out.
pub(crate) fn slim_projections(
    conn: &Connection,
    ids: &std::collections::HashSet<String>,
) -> rusqlite::Result<std::collections::HashMap<String, crate::stored_projection::Slim>> {
    use std::collections::{HashMap, HashSet};
    let mut out: HashMap<String, crate::stored_projection::Slim> = HashMap::new();
    if ids.is_empty() {
        return Ok(out);
    }
    // Each table whole, ordered by thread and then as `thread_projection`
    // orders one thread's rows.
    let tables: Vec<(&str, String)> = vec![
        ("thread", "SELECT thread_id, payload_json FROM orchestration_v2_projection_threads WHERE deleted_at IS NULL".into()),
        ("runs", "SELECT thread_id, payload_json FROM orchestration_v2_projection_runs ORDER BY thread_id, ordinal ASC".into()),
        ("attempts", "SELECT thread_id, payload_json FROM orchestration_v2_projection_run_attempts ORDER BY thread_id, run_id ASC, attempt_ordinal ASC".into()),
        ("providerSessions", "SELECT bindings.thread_id, sessions.payload_json FROM orchestration_v2_projection_provider_sessions AS sessions
            INNER JOIN orchestration_v2_projection_provider_session_bindings AS bindings
              ON bindings.provider_session_id = sessions.provider_session_id
            ORDER BY bindings.thread_id, sessions.updated_at ASC, sessions.provider_session_id ASC".into()),
        ("providerThreads", "SELECT thread_id, payload_json FROM orchestration_v2_projection_provider_threads ORDER BY thread_id, COALESCE(first_run_ordinal, 0), provider_thread_id ASC".into()),
        ("providerTurns", "SELECT thread_id, payload_json FROM orchestration_v2_projection_provider_turns ORDER BY thread_id, provider_thread_id ASC, ordinal ASC".into()),
        ("runtimeRequests", "SELECT thread_id, payload_json FROM orchestration_v2_projection_runtime_requests ORDER BY thread_id, created_at ASC, runtime_request_id ASC".into()),
        ("plans", "SELECT thread_id, payload_json FROM orchestration_v2_projection_plans ORDER BY thread_id, plan_id ASC".into()),
        ("checkpointScopes", "SELECT thread_id, payload_json FROM orchestration_v2_projection_checkpoint_scopes ORDER BY thread_id, ordinal_within_parent ASC, scope_id ASC".into()),
        ("checkpoints", "SELECT thread_id, payload_json FROM orchestration_v2_projection_checkpoints ORDER BY thread_id, scope_id ASC, ordinal_within_scope ASC".into()),
        ("contextHandoffs", "SELECT thread_id, payload_json FROM orchestration_v2_projection_context_handoffs ORDER BY thread_id, rowid ASC".into()),
        // Users' messages, by the role column through the reference's
        // partial index on it, without reading the other messages.
        ("userMessages", "SELECT thread_id, created_at, message_id, payload_json
            FROM orchestration_v2_projection_messages
              INDEXED BY orchestration_v2_projection_messages_latest_user_idx
            WHERE role = 'user'".into()),
        // The turn items the shell looks at, through the indexes on
        // `(thread_id, type, status)` and on queued input.
        ("shellItems", format!(
            "WITH kinds(type, status) AS ({SHELL_ITEM_KINDS})
             SELECT i.thread_id, i.ordinal, i.turn_item_id, i.run_id, i.payload_json
             FROM orchestration_v2_projection_threads AS t
             CROSS JOIN kinds AS k
             CROSS JOIN orchestration_v2_projection_turn_items AS i
               ON i.thread_id = t.thread_id AND i.type = k.type
              AND (k.status IS NULL OR i.status = k.status)
             WHERE t.deleted_at IS NULL"
        )),
        ("queuedItems", "SELECT thread_id, ordinal, turn_item_id, run_id, payload_json
            FROM orchestration_v2_projection_turn_items
            WHERE type = 'user_message' AND json_valid(payload_json)
              AND json_extract(payload_json, '$.inputIntent') = 'queued_turn'".into()),
        // How many turn items each run holds: the reference's own count,
        // which its triggers keep with every turn item written or deleted
        // (`run_key` is '' for items of no run).
        ("counts", "SELECT thread_id, run_key, item_count FROM orchestration_v2_projection_turn_item_counts".into()),
    ];
    let keys: Vec<&str> = tables.iter().map(|(key, _)| *key).collect();
    let jobs = tables
        .iter()
        .map(|(_, sql)| {
            let sql = sql.as_str();
            Box::new(move |conn: &Connection| slim_rows(conn, sql)) as Job<'_, Vec<SlimRow>>
        })
        .collect();
    let mut results: HashMap<&str, Vec<SlimRow>> =
        keys.into_iter().zip(in_parallel(conn, jobs)?).collect();
    let mut take = |key: &str| results.remove(key).unwrap_or_default();

    let mut collections: HashMap<String, Map<String, Value>> = HashMap::new();
    for (columns, thread) in take("thread") {
        let Some(id) = columns
            .first()
            .and_then(text)
            .filter(|id| ids.contains(*id))
        else {
            continue;
        };
        let mut projection = Map::new();
        projection.insert("thread".into(), thread);
        for key in [
            "runs",
            "attempts",
            "nodes",
            "subagents",
            "providerSessions",
            "providerThreads",
            "providerTurns",
            "runtimeRequests",
            "messages",
            "plans",
            "turnItems",
            "checkpointScopes",
            "checkpoints",
            "contextHandoffs",
            "contextTransfers",
        ] {
            projection.insert(key.into(), Value::Array(Vec::new()));
        }
        collections.insert(id.to_owned(), projection);
    }
    for key in [
        "runs",
        "attempts",
        "providerSessions",
        "providerThreads",
        "providerTurns",
        "runtimeRequests",
        "plans",
        "checkpointScopes",
        "checkpoints",
        "contextHandoffs",
    ] {
        for (columns, value) in take(key) {
            let Some(projection) = columns
                .first()
                .and_then(text)
                .and_then(|id| collections.get_mut(id))
            else {
                continue;
            };
            if let Some(Value::Array(list)) = projection.get_mut(key) {
                list.push(value);
            }
        }
    }
    // Messages: users' messages, then runs' prompts, and the messages a user
    // created in a run whose prompt is a notification (what
    // `projection_shell::is_background_wake` looks for). In each thread's
    // `created_at, message_id` order, as `thread_projection` reads them.
    {
        type Found = (String, String, Value);
        let mut found: HashMap<String, Vec<Found>> = HashMap::new();
        let mut seen: HashSet<String> = HashSet::new();
        let mut keep = |found: &mut HashMap<String, Vec<Found>>,
                        columns: &[rusqlite::types::Value],
                        value: Value| {
            let (Some(thread), Some(created), Some(id)) = (
                columns.first().and_then(text),
                columns.get(1).and_then(text),
                columns.get(2).and_then(text),
            ) else {
                return;
            };
            if collections.contains_key(thread) && seen.insert(id.to_owned()) {
                found.entry(thread.to_owned()).or_default().push((
                    created.to_owned(),
                    id.to_owned(),
                    value,
                ));
            }
        };
        for (columns, value) in take("userMessages") {
            keep(&mut found, &columns, value);
        }
        let mut by_id = prepare_slim(
            conn,
            "SELECT thread_id, created_at, message_id, payload_json
             FROM orchestration_v2_projection_messages WHERE message_id = ?1 AND thread_id = ?2",
        )?;
        let mut by_run = prepare_slim(
            conn,
            "SELECT thread_id, created_at, message_id, payload_json
             FROM orchestration_v2_projection_messages WHERE run_id = ?1 AND thread_id = ?2
               AND json_valid(payload_json) AND json_extract(payload_json, '$.createdBy') = 'user'",
        )?;
        let row_of = |row: &rusqlite::Row<'_>| -> rusqlite::Result<SlimRow> {
            Ok((
                vec![row.get(0)?, row.get(1)?, row.get(2)?],
                decode(&row.get::<_, String>(3)?)?,
            ))
        };
        for (id, projection) in &collections {
            let runs = projection.get("runs").and_then(Value::as_array);
            for run in runs.into_iter().flatten() {
                let Some(prompt) = run["userMessageId"].as_str() else {
                    continue;
                };
                if found
                    .get(id)
                    .is_some_and(|list| list.iter().any(|(_, known, _)| known == prompt))
                {
                    continue;
                }
                let mut rows = by_id.query(params![prompt, id])?;
                while let Some(row) = rows.next()? {
                    let (columns, value) = row_of(row)?;
                    keep(&mut found, &columns, value);
                }
            }
            for run in runs.into_iter().flatten() {
                let notified = found.get(id).is_some_and(|messages| {
                    messages.iter().any(|(_, message, value)| {
                        run["userMessageId"].as_str() == Some(message.as_str())
                            && value["notification"].is_object()
                    })
                });
                let Some(run_id) = run["id"].as_str().filter(|_| notified) else {
                    continue;
                };
                let mut rows = by_run.query(params![run_id, id])?;
                while let Some(row) = rows.next()? {
                    let (columns, value) = row_of(row)?;
                    keep(&mut found, &columns, value);
                }
            }
        }
        for (id, mut list) in found {
            list.sort_by(|a, b| (&a.0, &a.1).cmp(&(&b.0, &b.1)));
            if let Some(Value::Array(messages)) = collections
                .get_mut(&id)
                .and_then(|projection| projection.get_mut("messages"))
            {
                messages.extend(list.into_iter().map(|(_, _, value)| value));
            }
        }
    }
    // By thread: each shell item's ordinal, id, run id (columns) and record.
    type Item = (i64, String, Option<String>, Value);
    let mut items: HashMap<String, Vec<Item>> = HashMap::new();
    let mut item_ids: HashSet<String> = HashSet::new();
    for (columns, value) in take("shellItems").into_iter().chain(take("queuedItems")) {
        let (Some(id), Some(rusqlite::types::Value::Integer(ordinal)), Some(item_id)) = (
            columns.first().and_then(text),
            columns.get(1),
            columns.get(2).and_then(text),
        ) else {
            continue;
        };
        if !collections.contains_key(id) || !item_ids.insert(item_id.to_owned()) {
            continue;
        }
        let run = columns.get(3).and_then(text).map(str::to_owned);
        items
            .entry(id.to_owned())
            .or_default()
            .push((*ordinal, item_id.to_owned(), run, value));
    }
    let mut counts: HashMap<String, Vec<(Option<String>, u64)>> = HashMap::new();
    for (columns, _) in take("counts") {
        let (Some(id), Some(run), Some(rusqlite::types::Value::Integer(count))) = (
            columns.first().and_then(text),
            columns.get(1).and_then(text),
            columns.get(2),
        ) else {
            continue;
        };
        if collections.contains_key(id) {
            counts.entry(id.to_owned()).or_default().push((
                (!run.is_empty()).then(|| run.to_owned()),
                (*count).max(0) as u64,
            ));
        }
    }
    // The first turn item showing each message read, from the reference's
    // message-ordinal index (`sortMessagesByTurnItemOrder`), in a few
    // readers at once.
    let wanted: Vec<(String, String)> = collections
        .iter()
        .flat_map(|(id, projection)| {
            projection
                .get("messages")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .filter_map(move |message| Some((id.clone(), message["id"].as_str()?.to_owned())))
        })
        .collect();
    let chunk = wanted.len().div_ceil(8).max(1);
    let jobs = wanted
        .chunks(chunk)
        .map(|part| {
            Box::new(move |conn: &Connection| -> rusqlite::Result<Vec<(String, String, i64)>> {
                let mut statement = prepare_slim(
                    conn,
                    "SELECT MIN(ordinal) FROM orchestration_v2_projection_turn_items
                       INDEXED BY orchestration_v2_projection_turn_items_message_ordinal_idx
                     WHERE thread_id = ?1 AND json_extract(payload_json, '$.messageId') = ?2
                       AND type IN ('user_message', 'assistant_message') AND json_valid(payload_json)",
                )?;
                let mut out = Vec::new();
                for (id, message) in part {
                    let first: Option<i64> =
                        statement.query_row(params![id, message], |row| row.get(0))?;
                    if let Some(first) = first {
                        out.push((id.clone(), message.clone(), first));
                    }
                }
                Ok(out)
            }) as Job<'_, Vec<(String, String, i64)>>
        })
        .collect();
    let mut ordinals: HashMap<String, HashMap<String, i64>> = HashMap::new();
    for (id, message, first) in in_parallel(conn, jobs)?.into_iter().flatten() {
        ordinals.entry(id).or_default().insert(message, first);
    }
    for (id, mut projection) in collections {
        let mut list = items.remove(&id).unwrap_or_default();
        list.sort_by(|a, b| (a.0, &a.1).cmp(&(b.0, &b.1)));
        let mut loaded: HashMap<Option<String>, u64> = HashMap::new();
        for (_, _, run, _) in &list {
            *loaded.entry(run.clone()).or_default() += 1;
        }
        let unloaded_items = counts
            .remove(&id)
            .unwrap_or_default()
            .into_iter()
            .map(|(run, count)| {
                let loaded = loaded.get(&run).copied().unwrap_or(0);
                (run, count.saturating_sub(loaded))
            })
            .filter(|(_, count)| *count > 0)
            .collect();
        projection.insert(
            "turnItems".into(),
            Value::Array(list.into_iter().map(|(_, _, _, item)| item).collect()),
        );
        let updated_at = projection["thread"]["updatedAt"].clone();
        projection.insert("updatedAt".into(), updated_at);
        out.insert(
            id.clone(),
            crate::stored_projection::Slim {
                projection: Value::Object(projection),
                message_ordinals: ordinals.remove(&id).unwrap_or_default(),
                unloaded_items,
            },
        );
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reads_a_seeded_thread_in_the_reference_order() {
        let home = tempfile::tempdir().unwrap();
        let conn = crate::statev2::open(home.path()).unwrap();
        conn.execute_batch(
            r#"
            INSERT INTO projection_projects (project_id, title, workspace_root, scripts_json, created_at, updated_at, auto_pull, has_custom_title)
              VALUES ('project:a', 'A', '/tmp/a', '[]', '2026-01-01T00:00:00.000Z', '2026-01-01T00:00:00.000Z', 0, 1);
            INSERT INTO orchestration_v2_projection_threads (thread_id, project_id, title, default_provider, runtime_mode, interaction_mode, created_at, updated_at, payload_json, provider_instance_id)
              VALUES ('thread:a', 'project:a', 'T', 'codex', 'full-access', 'default', '2026-01-01T00:00:00.000Z', '2026-01-01T00:00:01.000Z', '{"id":"thread:a","updatedAt":"2026-01-01T00:00:01.000Z"}', 'codex');
            INSERT INTO orchestration_v2_projection_threads (thread_id, project_id, title, default_provider, runtime_mode, interaction_mode, created_at, updated_at, deleted_at, payload_json)
              VALUES ('thread:gone', 'project:a', 'T', 'codex', 'full-access', 'default', '2026-01-01T00:00:00.000Z', '2026-01-01T00:00:00.000Z', '2026-01-02T00:00:00.000Z', '{}');
            INSERT INTO orchestration_v2_projection_runs (run_id, thread_id, ordinal, provider, status, requested_at, payload_json)
              VALUES ('run:2', 'thread:a', 2, 'codex', 'queued', 'x', '{"id":"run:2"}'),
                     ('run:1', 'thread:a', 1, 'codex', 'completed', 'x', '{"id":"run:1"}');
            "#,
        )
        .unwrap();
        let projects = projects(&conn).unwrap();
        assert_eq!(projects[0]["hasCustomTitle"], true);
        assert_eq!(projects[0]["scripts"], json!([]));
        assert_eq!(thread_ids(&conn).unwrap(), vec!["thread:a".to_string()]);
        assert_eq!(
            deleted_thread_ids(&conn).unwrap(),
            vec!["thread:gone".to_string()]
        );
        let projection = thread_projection(&conn, "thread:a").unwrap().unwrap();
        assert_eq!(projection["runs"][0]["id"], "run:1");
        assert_eq!(projection["runs"][1]["id"], "run:2");
        assert_eq!(projection["turnItems"], json!([]));
        assert_eq!(projection["updatedAt"], "2026-01-01T00:00:01.000Z");
        assert!(thread_projection(&conn, "thread:none").unwrap().is_none());
    }
}
