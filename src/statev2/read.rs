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
thread_local! {
    /// Every statement `slim_projections` ran on this test thread.
    pub(crate) static SLIM_STATEMENTS: std::cell::RefCell<Vec<String>> = const { std::cell::RefCell::new(Vec::new()) };
}

fn prepare_slim<'c>(conn: &'c Connection, sql: &str) -> rusqlite::Result<rusqlite::Statement<'c>> {
    #[cfg(test)]
    SLIM_STATEMENTS.with(|statements| statements.borrow_mut().push(sql.to_owned()));
    conn.prepare(sql)
}

/// The slim records of the threads `ids` names (`stored_projection::Slim`),
/// read with one query per table rather than one per thread. Each
/// collection holds what `thread_projection` gives, in its order, except
/// `nodes` (empty), `turnItems` (only what the shell row looks at) and
/// `messages` (only users' messages and runs' prompts). A
/// thread the database has no live row for is left out.
pub(crate) fn slim_projections(
    conn: &Connection,
    ids: &std::collections::HashSet<String>,
) -> rusqlite::Result<std::collections::HashMap<String, crate::stored_projection::Slim>> {
    use std::collections::HashMap;
    let mut out: HashMap<String, crate::stored_projection::Slim> = HashMap::new();
    if ids.is_empty() {
        return Ok(out);
    }
    let mut collections: HashMap<String, Map<String, Value>> = HashMap::new();
    let mut statement = prepare_slim(
        conn,
        "SELECT thread_id, payload_json FROM orchestration_v2_projection_threads
         WHERE deleted_at IS NULL",
    )?;
    let mut rows = statement.query([])?;
    while let Some(row) = rows.next()? {
        let id: String = row.get(0)?;
        if !ids.contains(&id) {
            continue;
        }
        let thread = decode(&row.get::<_, String>(1)?)?;
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
        collections.insert(id, projection);
    }
    drop(rows);
    drop(statement);
    // Each table whole, ordered by thread and then as `thread_projection`
    // orders one thread's rows.
    for (key, sql) in [
        ("runs", "SELECT thread_id, payload_json FROM orchestration_v2_projection_runs ORDER BY thread_id, ordinal ASC"),
        ("attempts", "SELECT thread_id, payload_json FROM orchestration_v2_projection_run_attempts ORDER BY thread_id, run_id ASC, attempt_ordinal ASC"),
        ("subagents", "SELECT thread_id, payload_json FROM orchestration_v2_projection_subagents ORDER BY thread_id, COALESCE(started_at, ''), subagent_id ASC"),
        ("providerSessions", "SELECT bindings.thread_id, sessions.payload_json FROM orchestration_v2_projection_provider_sessions AS sessions
            INNER JOIN orchestration_v2_projection_provider_session_bindings AS bindings
              ON bindings.provider_session_id = sessions.provider_session_id
            ORDER BY bindings.thread_id, sessions.updated_at ASC, sessions.provider_session_id ASC"),
        ("providerThreads", "SELECT thread_id, payload_json FROM orchestration_v2_projection_provider_threads ORDER BY thread_id, COALESCE(first_run_ordinal, 0), provider_thread_id ASC"),
        ("providerTurns", "SELECT thread_id, payload_json FROM orchestration_v2_projection_provider_turns ORDER BY thread_id, provider_thread_id ASC, ordinal ASC"),
        ("runtimeRequests", "SELECT thread_id, payload_json FROM orchestration_v2_projection_runtime_requests ORDER BY thread_id, created_at ASC, runtime_request_id ASC"),
        // Only the messages the typed state or the shell row reads: users'
        // messages, and runs' prompts.
        ("messages", "SELECT thread_id, payload_json FROM orchestration_v2_projection_messages
            WHERE json_extract(payload_json, '$.role') = 'user'
               OR json_extract(payload_json, '$.createdBy') = 'user'
               OR json_extract(payload_json, '$.id') IN
                  (SELECT json_extract(payload_json, '$.userMessageId') FROM orchestration_v2_projection_runs)
            ORDER BY thread_id, created_at ASC, message_id ASC"),
        ("plans", "SELECT thread_id, payload_json FROM orchestration_v2_projection_plans ORDER BY thread_id, plan_id ASC"),
        ("checkpointScopes", "SELECT thread_id, payload_json FROM orchestration_v2_projection_checkpoint_scopes ORDER BY thread_id, ordinal_within_parent ASC, scope_id ASC"),
        ("checkpoints", "SELECT thread_id, payload_json FROM orchestration_v2_projection_checkpoints ORDER BY thread_id, scope_id ASC, ordinal_within_scope ASC"),
        ("contextHandoffs", "SELECT thread_id, payload_json FROM orchestration_v2_projection_context_handoffs ORDER BY thread_id, rowid ASC"),
    ] {
        let mut statement = prepare_slim(conn, sql)?;
        let mut rows = statement.query([])?;
        while let Some(row) = rows.next()? {
            let id: String = row.get(0)?;
            let Some(projection) = collections.get_mut(&id) else {
                continue;
            };
            let value = decode(&row.get::<_, String>(1)?)?;
            if let Some(Value::Array(list)) = projection.get_mut(key) {
                list.push(value);
            }
        }
    }
    // A transfer belongs to its source and its target thread.
    {
        let mut statement = prepare_slim(
            conn,
            "SELECT source_thread_id, target_thread_id, payload_json
             FROM orchestration_v2_projection_context_transfers ORDER BY rowid ASC",
        )?;
        let mut rows = statement.query([])?;
        while let Some(row) = rows.next()? {
            let source: Option<String> = row.get(0)?;
            let target: Option<String> = row.get(1)?;
            let owners: Vec<&String> = match (&source, &target) {
                (Some(source), Some(target)) if source == target => vec![source],
                (source, target) => source.iter().chain(target.iter()).collect(),
            };
            if !owners.iter().any(|id| collections.contains_key(*id)) {
                continue;
            }
            let value = decode(&row.get::<_, String>(2)?)?;
            for id in owners {
                if let Some(Value::Array(list)) = collections
                    .get_mut(id)
                    .and_then(|projection| projection.get_mut("contextTransfers"))
                {
                    list.push(value.clone());
                }
            }
        }
    }
    // The turn items the shell looks at, through the indexes on
    // `(thread_id, type, status)` and on queued input.
    // By thread: each item's ordinal, id, run id (columns) and record.
    type Item = (i64, String, Option<String>, Value);
    let mut items: HashMap<String, Vec<Item>> = HashMap::new();
    for sql in [
        format!(
            "WITH kinds(type, status) AS ({SHELL_ITEM_KINDS})
             SELECT i.thread_id, i.ordinal, i.turn_item_id, i.run_id, i.payload_json
             FROM orchestration_v2_projection_threads AS t
             CROSS JOIN kinds AS k
             CROSS JOIN orchestration_v2_projection_turn_items AS i
               ON i.thread_id = t.thread_id AND i.type = k.type
              AND (k.status IS NULL OR i.status = k.status)
             WHERE t.deleted_at IS NULL"
        ),
        "SELECT thread_id, ordinal, turn_item_id, run_id, payload_json
         FROM orchestration_v2_projection_turn_items
         WHERE type = 'user_message' AND json_valid(payload_json)
           AND json_extract(payload_json, '$.inputIntent') = 'queued_turn'"
            .to_owned(),
    ] {
        let mut statement = prepare_slim(conn, &sql)?;
        let mut rows = statement.query([])?;
        while let Some(row) = rows.next()? {
            let id: String = row.get(0)?;
            if !collections.contains_key(&id) {
                continue;
            }
            let item_id: String = row.get(2)?;
            let list = items.entry(id).or_default();
            if list.iter().any(|(_, known, _, _)| *known == item_id) {
                continue;
            }
            list.push((
                row.get(1)?,
                item_id,
                row.get(3)?,
                decode(&row.get::<_, String>(4)?)?,
            ));
        }
    }
    // How many turn items each run holds: the reference's own count, which
    // its triggers keep with every turn item written or deleted (`run_key`
    // is '' for items of no run).
    let mut counts: HashMap<String, Vec<(Option<String>, u64)>> = HashMap::new();
    {
        let mut statement = prepare_slim(
            conn,
            "SELECT thread_id, run_key, item_count FROM orchestration_v2_projection_turn_item_counts",
        )?;
        let mut rows = statement.query([])?;
        while let Some(row) = rows.next()? {
            let id: String = row.get(0)?;
            if collections.contains_key(&id) {
                let run: String = row.get(1)?;
                counts.entry(id).or_default().push((
                    (!run.is_empty()).then_some(run),
                    row.get::<_, i64>(2)?.max(0) as u64,
                ));
            }
        }
    }
    // The first turn item showing each message read, from the reference's
    // message-ordinal index (`sortMessagesByTurnItemOrder`).
    let mut ordinals: HashMap<String, HashMap<String, i64>> = HashMap::new();
    {
        let mut statement = prepare_slim(
            conn,
            "SELECT MIN(ordinal) FROM orchestration_v2_projection_turn_items
               INDEXED BY orchestration_v2_projection_turn_items_message_ordinal_idx
             WHERE thread_id = ?1 AND json_extract(payload_json, '$.messageId') = ?2
               AND type IN ('user_message', 'assistant_message') AND json_valid(payload_json)",
        )?;
        for (id, projection) in &collections {
            for message in projection
                .get("messages")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
            {
                let Some(message) = message["id"].as_str() else {
                    continue;
                };
                let first: Option<i64> =
                    statement.query_row(params![id, message], |row| row.get(0))?;
                if let Some(first) = first {
                    ordinals
                        .entry(id.clone())
                        .or_default()
                        .insert(message.to_owned(), first);
                }
            }
        }
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
