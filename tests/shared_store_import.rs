//! The one-time import of a Rust home's own threads into the shared
//! `statev2.sqlite` (docs/shared-store.md, 2.3).
mod support;

use rusqlite::Connection;
use serde_json::json;
use t3code_server::{
    auth_service::AuthService,
    config::{RuntimeConfig, ServerPaths},
    AppState, Event,
};

async fn open(paths: &ServerPaths) -> std::sync::Arc<AppState> {
    support::isolate_host();
    AppState::with_config(
        &paths.state_dir,
        Vec::new(),
        AuthService::new(),
        RuntimeConfig::default(),
    )
    .await
    .unwrap()
}

fn count(db: &std::path::Path, sql: &str) -> i64 {
    Connection::open(db)
        .unwrap()
        .query_row(sql, [], |row| row.get(0))
        .unwrap()
}

#[tokio::test]
async fn a_home_with_only_its_own_log_is_imported_once_and_its_log_kept() {
    let home = tempfile::tempdir().unwrap();
    let paths = ServerPaths::from_base(home.path());
    std::fs::create_dir_all(&paths.state_dir).unwrap();
    {
        let state = open(&paths).await;
        state
            .commit_now(Event::ThreadCreated(
                serde_json::from_value(json!({
                    "id": "thread:own", "project_id": "p1", "title": "Own", "cwd": "/tmp",
                    "status": "idle", "messages": [], "runs": [], "created_at": 1,
                    "archived": false, "settled": false, "snoozed_until": null,
                    "pinned": false, "unread": false, "tags": []
                }))
                .unwrap(),
            ))
            .unwrap();
    }
    // A home from before the shared store: its log, and no database.
    let db = paths.state_dir.join("statev2.sqlite");
    for suffix in ["", "-wal", "-shm"] {
        let _ = std::fs::remove_file(format!("{}{suffix}", db.display()));
    }
    let log = paths.state_dir.join("events.jsonl");
    let before = std::fs::read(&log).unwrap();

    let state = open(&paths).await;
    assert!(state.store_now().threads.contains_key("thread:own"));
    drop(state);
    let threads =
        "SELECT COUNT(*) FROM orchestration_v2_projection_threads WHERE thread_id = 'thread:own'";
    assert_eq!(count(&db, threads), 1);
    let created = "SELECT COUNT(*) FROM orchestration_events WHERE stream_id = 'thread:own' AND event_type = 'thread.created'";
    assert_eq!(count(&db, created), 1);
    // The log is kept: what it held is unchanged, the import appended.
    let after = std::fs::read(&log).unwrap();
    assert!(after.starts_with(&before));
    assert!(String::from_utf8_lossy(&after[before.len()..]).contains("\"Imported\""));

    // Imported once: a restart neither imports it again nor reads it back
    // from the database as changed elsewhere.
    let state = open(&paths).await;
    drop(state);
    assert_eq!(count(&db, created), 1);
    assert_eq!(std::fs::read(&log).unwrap().len(), after.len());
}
