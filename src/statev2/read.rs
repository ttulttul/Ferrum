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
                pinned_at, archived_at, created_at, updated_at, deleted_at
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
