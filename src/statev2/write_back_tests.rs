//! Writing back a thread start-up read lazily never loses rows: each write
//! path a client or the server takes commits a thread whose slim records
//! alone are loaded, through the real command dispatch, the control state and
//! the SQLite writer, and every `orchestration_v2_projection_*` row the
//! commit did not mean to change stays byte for byte as it was.
use super::lazy_tests::{control_with, seeded};
use super::*;
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

/// Every row of every `orchestration_v2_projection_*` table, by table and
/// primary key, with all its columns.
type Snapshot = BTreeMap<String, BTreeMap<String, String>>;

pub(super) fn snapshot(conn: &Connection) -> Snapshot {
    let mut tables = conn
        .prepare(
            "SELECT name FROM sqlite_schema WHERE type = 'table'
               AND name LIKE 'orchestration_v2_projection_%' ORDER BY name",
        )
        .unwrap();
    let names: Vec<String> = tables
        .query_map([], |row| row.get(0))
        .unwrap()
        .map(Result::unwrap)
        .collect();
    let mut out = Snapshot::new();
    for table in names {
        let mut info = conn
            .prepare(&format!(
                "SELECT name, pk FROM pragma_table_info('{table}')"
            ))
            .unwrap();
        let columns: Vec<(String, i64)> = info
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
            .unwrap()
            .map(Result::unwrap)
            .collect();
        let quoted = |names: Vec<&String>| {
            names
                .iter()
                .map(|name| format!("quote(\"{name}\")"))
                .collect::<Vec<_>>()
                .join(" || '|' || ")
        };
        let mut keys: Vec<&(String, i64)> = columns.iter().filter(|(_, pk)| *pk > 0).collect();
        keys.sort_by_key(|(_, pk)| *pk);
        let key = if keys.is_empty() {
            "rowid".to_owned()
        } else {
            quoted(keys.iter().map(|(name, _)| name).collect())
        };
        let row = quoted(columns.iter().map(|(name, _)| name).collect());
        let mut statement = conn
            .prepare(&format!("SELECT {key}, {row} FROM \"{table}\""))
            .unwrap();
        let rows: BTreeMap<String, String> = statement
            .query_map([], |row| {
                let key: rusqlite::types::Value = row.get(0)?;
                Ok((format!("{key:?}"), row.get(1)?))
            })
            .unwrap()
            .map(Result::unwrap)
            .collect();
        out.insert(table, rows);
    }
    out
}

/// What a step changed: rows added, removed and changed, as `table key`.
#[derive(Debug, Default, PartialEq)]
pub(super) struct Changes {
    pub added: BTreeSet<String>,
    pub removed: BTreeSet<String>,
    pub changed: BTreeSet<String>,
}

pub(super) fn changes(before: &Snapshot, after: &Snapshot) -> Changes {
    let mut out = Changes::default();
    for (table, rows) in after {
        let old = before.get(table).cloned().unwrap_or_default();
        for (key, row) in rows {
            match old.get(key) {
                None => out.added.insert(format!("{table} {key}")),
                Some(previous) if previous != row => out.changed.insert(format!("{table} {key}")),
                _ => false,
            };
        }
        for key in old.keys().filter(|key| !rows.contains_key(*key)) {
            out.removed.insert(format!("{table} {key}"));
        }
    }
    out
}

/// A command host over a control state with the SQLite writer: commands run
/// as the server runs them, and a started run's turn does nothing (its end
/// is committed by the test).
struct Host {
    control: crate::control_state::ControlState,
    lock: tokio::sync::Mutex<()>,
    cancellations:
        tokio::sync::Mutex<std::collections::HashMap<uuid::Uuid, tokio::sync::oneshot::Sender<()>>>,
    workspace: crate::workspace::RemoteWorkspace,
}

impl std::ops::Deref for Host {
    type Target = crate::control_state::ControlState;
    fn deref(&self) -> &Self::Target {
        &self.control
    }
}

impl crate::orchestration::CommandHost for Host {
    fn mutation_lock(&self) -> &tokio::sync::Mutex<()> {
        &self.lock
    }
    fn policy_summary(&self) -> Option<&Value> {
        None
    }
    fn runtime_denial(&self, _: &str) -> Option<String> {
        None
    }
    fn provider_denial(&self, _: &str) -> Option<String> {
        None
    }
    fn effective_settings(&self) -> anyhow::Result<Value> {
        Ok(json!({}))
    }
    fn workspace(&self) -> &dyn crate::workspace::Workspace {
        &self.workspace
    }
    fn cancellations(
        &self,
    ) -> &tokio::sync::Mutex<std::collections::HashMap<uuid::Uuid, tokio::sync::oneshot::Sender<()>>>
    {
        &self.cancellations
    }
    fn launch_run(
        self: Arc<Self>,
        _: crate::Run,
        _: String,
        _: String,
        _: tokio::sync::oneshot::Receiver<()>,
    ) {
    }
    fn close_guest_thread(&self, _: &str) -> Result<(), String> {
        Ok(())
    }
    fn resolve_guest_message(&self, _: &str, _: &str, _: &str) -> Result<(), String> {
        Ok(())
    }
}

fn host(home: &std::path::Path, conn: &Connection) -> Arc<Host> {
    let mut control = control_with(home);
    load_into(conn, control.store.get_mut()).unwrap();
    Arc::new(Host {
        control,
        lock: Default::default(),
        cancellations: Default::default(),
        workspace: Default::default(),
    })
}

async fn dispatch(host: &Arc<Host>, command: Value) {
    if let Err(error) =
        crate::orchestration::dispatch_orchestration_command(host.clone(), command.clone()).await
    {
        panic!("{} failed: {error}", command["type"]);
    }
}

fn partial(host: &Host, thread_id: &str) -> bool {
    host.store_now().threads[thread_id]
        .stored
        .as_ref()
        .is_some_and(|stored| stored.is_partial())
}

/// Every change of `changes`, apart from the commit bookkeeping every
/// commit updates (the projection metadata's last sequence).
fn without_bookkeeping(mut changes: Changes) -> Changes {
    changes
        .changed
        .retain(|change| !change.starts_with("orchestration_v2_projection_metadata "));
    changes
}

/// The table of a `table key` change, without its common prefix.
fn table_of(change: &str) -> &str {
    change
        .strip_prefix("orchestration_v2_projection_")
        .and_then(|rest| rest.split_once(' '))
        .map_or("", |(table, _)| table)
}

/// Whether the row `change` names is one of `thread`'s records, by the ids
/// the synthetic threads and this server give them.
fn belongs(change: &str, thread: &str) -> bool {
    let encoded = crate::thread_projection::uri_component(thread);
    // The synthetic records name their thread by its index.
    let index = thread.strip_prefix("thread:").unwrap_or(thread);
    let synthetic = [
        "message",
        "item",
        "node",
        "request",
        "provider-turn",
        "plan",
        "scope",
        "checkpoint",
        "provider-session",
    ]
    .iter()
    .flat_map(|kind| {
        [
            format!("'{kind}:{index}:"),
            format!("'{kind}:{index}'"),
            format!(":{kind}%3A{index}%3A"),
        ]
    });
    [
        format!("'{thread}'"),
        format!("{thread}:"),
        format!("{thread}%3A"),
        format!("{encoded}:"),
        format!("{encoded}%3A"),
        format!("{encoded}'"),
    ]
    .into_iter()
    .chain(synthetic)
    .any(|needle| change.contains(needle.as_str()))
}

fn quoted(id: &str) -> String {
    format!("Text(\"'{id}'\")")
}

/// The whole projection the database holds for `thread_id` equals the one
/// the store holds after a commit: what the store serves and writes next is
/// what the database has.
fn store_matches_database(host: &Host, conn: &Connection, thread_id: &str) {
    let stored = read::thread_projection(conn, thread_id).unwrap().unwrap();
    let store = host.store_now();
    let thread = &store.threads[thread_id];
    let held = thread.stored.as_ref().expect("a stored thread").get();
    same_records(held, &stored, thread_id);
}

/// `held` holds `stored`'s records: every record of every collection, and
/// the thread record. Records by id: a commit adds a new record after the
/// others, where a read orders them. The top-level `updatedAt` is not a row
/// (a read takes it from the thread record, and so does what the store
/// serves).
fn same_records(held: &Value, stored: &Value, thread_id: &str) {
    let by_id = |records: &Value| -> BTreeMap<String, Value> {
        records
            .as_array()
            .into_iter()
            .flatten()
            // An instance's shared session is one row for all its threads:
            // a commit to another of them changes it.
            .filter(|record| {
                !record["id"].as_str().is_some_and(|id| {
                    id.starts_with("provider-session:provider-instance:") && id.ends_with(":shared")
                })
            })
            .map(|record| (record["id"].to_string(), record.clone()))
            .collect()
    };
    for (key, value) in stored.as_object().unwrap() {
        match key.as_str() {
            "updatedAt" => {}
            "thread" => assert_eq!(&held["thread"], value, "the thread record of {thread_id}"),
            _ => assert_eq!(
                by_id(&held[key]),
                by_id(value),
                "the {key} of {thread_id} are the database's"
            ),
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn writing_back_lazily_read_threads_never_deletes_or_rewrites_other_rows() {
    let (home, conn) = seeded(36);
    let host = host(home.path(), &conn);
    let all = snapshot(&conn);
    // The seeded child tables are populated.
    for table in [
        "messages",
        "nodes",
        "turn_items",
        "runs",
        "run_attempts",
        "provider_turns",
        "runtime_requests",
        "provider_threads",
        "provider_sessions",
        "checkpoints",
        "checkpoint_scopes",
        "plans",
    ] {
        assert!(
            !all[&format!("orchestration_v2_projection_{table}")].is_empty(),
            "{table}"
        );
    }
    for id in host.store_now().threads.keys() {
        assert!(partial(&host, id), "{id} starts slim");
    }
    let thread_row = |id: &str| format!("orchestration_v2_projection_threads {}", quoted(id));
    let mut before = all;
    // Runs one step and checks what it changed against `expected`: the
    // thread rows it may change, and the tables it may add rows to, for
    // `thread`. Nothing is ever removed, and no other row changes.
    // Rows the steps added, by the thread whose step added them.
    let mut added_by: BTreeMap<String, String> = BTreeMap::new();
    // Runs one step's check: nothing is ever removed; a row the seed had
    // changes only when it is one of `changed_threads`' rows or a row of
    // `thread` in `changed_tables`; a row an earlier step added changes only
    // in a step on the same thread; and rows are added to exactly
    // `added_tables`, for `thread`.
    let mut step = |name: &str,
                    thread: &str,
                    changed_threads: &[&str],
                    added_tables: &[&str],
                    changed_tables: &[&str]| {
        let after = snapshot(&conn);
        let diff = without_bookkeeping(changes(&before, &after));
        assert!(
            diff.removed.is_empty(),
            "{name} removed rows: {:?}",
            diff.removed
        );
        let threads: BTreeSet<String> = changed_threads.iter().map(|id| thread_row(id)).collect();
        for change in &diff.changed {
            if let Some(owner) = added_by.get(change) {
                assert_eq!(owner, thread, "{name} changed {change}, added for {owner}");
                continue;
            }
            if threads.contains(change) {
                continue;
            }
            let table = table_of(change);
            assert!(
                changed_tables.contains(&table) && belongs(change, thread),
                "{name} changed {change}"
            );
        }
        for row in &threads {
            assert!(diff.changed.contains(row), "{name} left {row} as it was");
        }
        for change in &diff.added {
            assert!(
                added_tables.contains(&table_of(change)),
                "{name} added {change}"
            );
            added_by.insert(change.clone(), thread.to_owned());
        }
        for table in added_tables {
            assert!(
                diff.added.iter().any(|change| table_of(change) == *table),
                "{name} added no {table}"
            );
        }
        before = after;
    };

    // Rename (`thread.metadata.update`): the thread row alone.
    dispatch(
        &host,
        json!({"type": "thread.metadata.update", "commandId": "c:rename",
        "threadId": "thread:0", "title": "Renamed"}),
    )
    .await;
    step("rename", "thread:0", &["thread:0"], &[], &[]);
    store_matches_database(&host, &conn, "thread:0");

    // A thread whose shell row was read first (its slim records used), then
    // a meta update: pin, snooze, and the runtime mode.
    let _ = host.store_now().threads["thread:13"].shell();
    assert!(partial(&host, "thread:13"));
    dispatch(
        &host,
        json!({"type": "thread.pin", "commandId": "c:pin", "threadId": "thread:13"}),
    )
    .await;
    step("pin", "thread:13", &["thread:13"], &[], &[]);
    dispatch(
        &host,
        json!({"type": "thread.snooze", "commandId": "c:snooze", "threadId": "thread:14",
        "snoozedUntil": "2027-01-01T00:00:00.000Z"}),
    )
    .await;
    step("snooze", "thread:14", &["thread:14"], &[], &[]);
    dispatch(
        &host,
        json!({"type": "thread.runtime-mode.set", "commandId": "c:mode",
        "threadId": "thread:15", "runtimeMode": "approval-required"}),
    )
    .await;
    step("runtime mode", "thread:15", &["thread:15"], &[], &[]);
    store_matches_database(&host, &conn, "thread:13");

    // Settle, then unsettle.
    dispatch(
        &host,
        json!({"type": "thread.settle", "commandId": "c:settle", "threadId": "thread:6"}),
    )
    .await;
    step("settle", "thread:6", &["thread:6"], &[], &[]);
    dispatch(
        &host,
        json!({"type": "thread.unsettle", "commandId": "c:unsettle", "threadId": "thread:6",
        "reason": "user"}),
    )
    .await;
    step("unsettle", "thread:6", &["thread:6"], &[], &[]);
    store_matches_database(&host, &conn, "thread:6");

    // A message added to the thread (as guest and server messages are): its
    // message and turn item, and the thread's item counts.
    assert!(partial(&host, "thread:4"));
    host.commit(crate::Event::MessageAdded {
        thread_id: "thread:4".into(),
        message: crate::ConversationMessage {
            id: "message:added".into(),
            role: "user".into(),
            content: "An added message".into(),
            created_at: 1_800_000_000,
            creation_source: None,
            scheduled_task_id: None,
            follows_thread_selection: false,
            notification: None,
            steer: None,
            wire: None,
        },
    })
    .await
    .unwrap();
    step(
        "message added",
        "thread:4",
        // Its updatedAt moves on with the message, as the reference's does.
        &["thread:4"],
        &["messages", "turn_items", "turn_item_counts"],
        &["thread_item_counts", "turn_item_counts"],
    );
    store_matches_database(&host, &conn, "thread:4");

    // A new turn on an idle thread (`message.dispatch`): its run, attempt,
    // root node, prompt and prompt item; then the run's end.
    assert!(partial(&host, "thread:12"));
    dispatch(
        &host,
        json!({"type": "message.dispatch", "commandId": "c:turn", "threadId": "thread:12", "messageId": "message:new-turn",
        "text": "Another turn", "attachments": [], "createdBy": "user", "creationSource": "web"}),
    )
    .await;
    step(
        "new turn",
        "thread:12",
        // Its updatedAt moves on with its runs, as the reference's does.
        &["thread:12"],
        &[
            "runs",
            "run_attempts",
            "nodes",
            "messages",
            "turn_items",
            "turn_item_counts",
        ],
        &[
            "thread_item_counts",
            "turn_item_counts",
            "provider_threads",
            "provider_sessions",
        ],
    );
    store_matches_database(&host, &conn, "thread:12");
    let run = host.store_now().threads["thread:12"]
        .runs
        .last()
        .unwrap()
        .clone();
    host.commit(crate::Event::RunFinished {
        run_id: run.id,
        thread_id: "thread:12".into(),
        status: "completed".into(),
        error: None,
    })
    .await
    .unwrap();
    step(
        "run finished",
        "thread:12",
        // Its updatedAt moves on with its runs, as the reference's does.
        &["thread:12"],
        &[],
        &[
            "runs",
            "run_attempts",
            "nodes",
            "provider_threads",
            "provider_sessions",
        ],
    );
    store_matches_database(&host, &conn, "thread:12");

    // A message to a thread whose run is running queues a run.
    assert!(partial(&host, "thread:1"));
    dispatch(
        &host,
        json!({"type": "message.dispatch", "commandId": "c:queue", "threadId": "thread:1", "messageId": "message:queued",
        "text": "Queued", "attachments": [], "createdBy": "user", "creationSource": "web"}),
    )
    .await;
    step(
        "queued turn",
        "thread:1",
        // Its updatedAt moves on with its runs, as the reference's does.
        &["thread:1"],
        &[
            "runs",
            "run_attempts",
            "nodes",
            "messages",
            "turn_items",
            "turn_item_counts",
        ],
        &["thread_item_counts", "turn_item_counts", "provider_threads"],
    );
    store_matches_database(&host, &conn, "thread:1");

    // Delete: the thread row is marked deleted; its records stay.
    dispatch(
        &host,
        json!({"type": "thread.delete", "commandId": "c:delete", "threadId": "thread:5"}),
    )
    .await;
    step("delete", "thread:5", &["thread:5"], &[], &[]);
    assert!(!host.store_now().threads.contains_key("thread:5"));

    // The shared-store plan of a change to a slim thread is made from the
    // records the database holds, never the slim ones: a small change from
    // those records cut down to it (`shared_store::Partial`), each one the
    // database's, and it writes exactly what diffing the whole projection
    // writes, here the thread row and nothing else.
    assert!(partial(&host, "thread:8"));
    {
        let store = host.store_now();
        let record = crate::ServerEvent {
            id: 1,
            at: 1_800_000_000,
            event: crate::Event::ThreadPatched {
                thread_id: "thread:8".into(),
                patch: json!({"title": "Planned", "row": {"title": "Planned"}}),
            },
            receipt: None,
            thread_events: None,
        };
        let planned = crate::shared_store::plan(&store, &record, "2027-01-15T08:00:00.000Z");
        let whole = read::thread_projection(&conn, "thread:8").unwrap().unwrap();
        let before = planned.threads[0].before.as_ref().unwrap();
        let after = &planned.threads[0].after;
        for (key, records) in before.as_object().unwrap() {
            let Some(records) = records.as_array() else {
                continue;
            };
            for record in records {
                let id = &record["id"];
                let held = whole[key.as_str()]
                    .as_array()
                    .and_then(|all| all.iter().find(|r| &r["id"] == id));
                // An instance's shared session is one row for all its threads.
                if id.as_str().is_some_and(|id| id.ends_with(":shared")) {
                    continue;
                }
                assert_eq!(held, Some(record), "{key} {id} is the database's");
            }
        }
        let mut expected = whole.clone();
        expected["thread"]["title"] = json!("Planned");
        assert_eq!(
            crate::shared_store::write_effect(before, after).1,
            crate::shared_store::write_effect(&whole, &expected).1,
            "the plan writes what diffing the whole projection writes"
        );
        assert_eq!(after["thread"]["title"], "Planned");
    }

    // A restart's reconcile, on the threads it stops a session or a run of.
    let reconciled = {
        let store = host.store_now();
        crate::runtime_recovery::reconcile(
            &store,
            "startup",
            1_800_000_000,
            "2027-01-15T08:00:00.000Z",
            &Value::Null,
        )
    };
    let reconciled_threads: Vec<String> = reconciled
        .iter()
        .filter_map(|event| event.thread_id().map(str::to_owned))
        .collect();
    assert!(reconciled_threads.iter().any(|id| partial(&host, id)));
    for event in reconciled {
        host.commit(event).await.unwrap();
    }
    let after = snapshot(&conn);
    let diff = without_bookkeeping(changes(&before, &after));
    assert!(
        diff.removed.is_empty(),
        "reconcile removed rows: {:?}",
        diff.removed
    );
    for change in diff.changed.iter().chain(&diff.added) {
        assert!(
            reconciled_threads
                .iter()
                .any(|id| belongs(change, id) || added_by.get(change) == Some(id))
                // The Codex instance's session, which its threads share: a
                // restart stops it.
                || change.ends_with("'provider-session:provider-instance:codex:shared'\")"),
            "reconcile touched {change}"
        );
    }
    for id in &reconciled_threads {
        store_matches_database(&host, &conn, id);
    }
    // Every thread no step touched is still slim, and its rows unchanged.
    let touched: BTreeSet<&str> = [
        "thread:0",
        "thread:1",
        "thread:6",
        "thread:4",
        "thread:5",
        "thread:8",
        "thread:12",
        "thread:13",
        "thread:14",
        "thread:15",
    ]
    .into_iter()
    .chain(reconciled_threads.iter().map(String::as_str))
    .collect();
    for (id, thread) in host.store_now().threads.iter() {
        if !touched.contains(id.as_str()) {
            assert!(thread.stored.as_ref().unwrap().is_partial(), "{id}");
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn a_commit_to_a_thread_that_cannot_be_read_whole_fails_before_writing() {
    let (home, conn) = seeded(6);
    let host = host(home.path(), &conn);
    // The thread's rows can no longer be read whole.
    conn.execute_batch(
        "UPDATE orchestration_v2_projection_nodes SET payload_json = 'unreadable'
         WHERE thread_id = 'thread:2'",
    )
    .unwrap();
    let before = snapshot(&conn);
    let events: i64 = conn
        .query_row("SELECT COUNT(*) FROM orchestration_events", [], |row| {
            row.get(0)
        })
        .unwrap();
    let error = host
        .commit(crate::Event::ThreadPatched {
            thread_id: "thread:2".into(),
            patch: json!({"title": "Renamed", "row": {"title": "Renamed"}}),
        })
        .await
        .unwrap_err();
    assert!(format!("{error:#}").contains("expected"), "{error:#}");
    // Nothing was written, and the thread stays as start-up read it.
    assert_eq!(snapshot(&conn), before);
    let after: i64 = conn
        .query_row("SELECT COUNT(*) FROM orchestration_events", [], |row| {
            row.get(0)
        })
        .unwrap();
    assert_eq!(after, events);
    assert!(partial(&host, "thread:2"));
    assert_eq!(host.store_now().threads["thread:2"].title, "Thread 2");
    // Other threads still commit.
    host.commit(crate::Event::ThreadPatched {
        thread_id: "thread:3".into(),
        patch: json!({"title": "Renamed", "row": {"title": "Renamed"}}),
    })
    .await
    .unwrap();
}

/// One table's rows: how many, a hash of every row no `touched` thread owns
/// (all columns, in key order), and each touched thread's row's hash by its
/// first (key) column.
struct Summary {
    count: u64,
    others: u64,
    touched: BTreeMap<String, u64>,
}

fn table_summaries(conn: &Connection, touched: &BTreeSet<String>) -> BTreeMap<String, Summary> {
    use std::hash::{Hash, Hasher};
    let snapshot_tables: Vec<String> = conn
        .prepare(
            "SELECT name FROM sqlite_schema WHERE type = 'table'
               AND name LIKE 'orchestration_v2_projection_%' ORDER BY name",
        )
        .unwrap()
        .query_map([], |row| row.get(0))
        .unwrap()
        .map(Result::unwrap)
        .collect();
    let mut out = BTreeMap::new();
    for table in snapshot_tables {
        let columns: Vec<String> = conn
            .prepare(&format!("SELECT name FROM pragma_table_info('{table}')"))
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .map(Result::unwrap)
            .collect();
        let thread_column = columns.iter().position(|c| c == "thread_id");
        let mut statement = conn
            .prepare(&format!("SELECT * FROM \"{table}\" ORDER BY 1, 2"))
            .unwrap();
        let mut rows = statement.query([]).unwrap();
        let mut summary = Summary {
            count: 0,
            others: 0,
            touched: BTreeMap::new(),
        };
        let mut others = std::collections::hash_map::DefaultHasher::new();
        while let Some(row) = rows.next().unwrap() {
            summary.count += 1;
            let mut hasher = std::collections::hash_map::DefaultHasher::new();
            for index in 0..columns.len() {
                let value: rusqlite::types::Value = row.get(index).unwrap();
                format!("{value:?}").hash(&mut hasher);
            }
            let owner =
                thread_column.and_then(|index| row.get::<_, Option<String>>(index).ok().flatten());
            if owner.is_some_and(|owner| touched.contains(&owner)) {
                let key: rusqlite::types::Value = row.get(0).unwrap();
                summary.touched.insert(format!("{key:?}"), hasher.finish());
            } else {
                hasher.finish().hash(&mut others);
            }
        }
        summary.others = others.finish();
        out.insert(table, summary);
    }
    out
}

/// The same check on a real database: a copy of a home whose state
/// directory `T3CODE_WRITE_BACK_DIR` names (never a home in use). It renames
/// and settles a few threads start-up read lazily, then compares each
/// table's row count, and a hash of every row but those threads' own rows,
/// before and after. Prints only counts.
#[tokio::test(flavor = "current_thread")]
#[ignore]
async fn writing_back_lazily_read_threads_keeps_every_row_of_a_real_database() {
    let Ok(dir) = std::env::var("T3CODE_WRITE_BACK_DIR") else {
        return;
    };
    let dir = std::path::Path::new(&dir);
    let conn = open(dir).unwrap();
    let mut control = control_with(dir);
    load_into(&conn, control.store.get_mut()).unwrap();
    let host = Arc::new(Host {
        control,
        lock: Default::default(),
        cancellations: Default::default(),
        workspace: Default::default(),
    });
    // A few threads with turns and no live run, never opened.
    let candidates: Vec<String> = {
        let store = host.store_now();
        let mut ids: Vec<&String> = store
            .threads
            .iter()
            .filter(|(_, thread)| {
                thread.runs.len() > 1
                    && !thread.settled
                    && thread.runs.iter().all(|run| {
                        matches!(run.status.as_str(), "completed" | "failed" | "cancelled")
                    })
            })
            .map(|(id, _)| id)
            .collect();
        ids.sort();
        ids.into_iter().take(4).cloned().collect()
    };
    assert_eq!(candidates.len(), 4);
    let touched: BTreeSet<String> = candidates.iter().cloned().collect();
    let started = std::time::Instant::now();
    let before = table_summaries(&conn, &touched);
    eprintln!(
        "summarised {} tables in {:?}",
        before.len(),
        started.elapsed()
    );
    for (index, id) in candidates.iter().enumerate() {
        assert!(partial(&host, id));
        dispatch(
            &host,
            json!({"type": "thread.metadata.update",
            "commandId": format!("check:rename:{index}"), "threadId": id,
            "title": format!("Write-back check {index}")}),
        )
        .await;
        dispatch(
            &host,
            json!({"type": "thread.settle",
            "commandId": format!("check:settle:{index}"), "threadId": id}),
        )
        .await;
    }
    // Released while idle (`projection_eviction`): every planned projection
    // holds exactly the database's rows, so each goes back to its slim
    // records, and a second write reads it whole again first.
    let released =
        crate::projection_eviction::sweep(&host.control, std::time::Duration::ZERO).await;
    eprintln!("released {released:?}");
    for id in &candidates {
        assert!(partial(&host, id), "{id} is released");
    }
    assert!(released.planned >= candidates.len(), "{released:?}");
    for (index, id) in candidates.iter().enumerate() {
        dispatch(
            &host,
            json!({"type": "thread.metadata.update",
            "commandId": format!("check:rename-after-release:{index}"), "threadId": id,
            "title": format!("Write-back check {index}, after release")}),
        )
        .await;
        assert!(!partial(&host, id));
        store_matches_database(&host, &conn, id);
    }
    let after = table_summaries(&conn, &touched);
    let mut differing = Vec::new();
    for (table, before) in &before {
        let after = &after[table];
        let name = table.trim_start_matches("orchestration_v2_projection_");
        let changed = before
            .touched
            .iter()
            .filter(|(key, hash)| after.touched.get(*key).is_some_and(|other| other != *hash))
            .count();
        let removed = before
            .touched
            .keys()
            .filter(|key| !after.touched.contains_key(*key))
            .count();
        let added = after
            .touched
            .keys()
            .filter(|key| !before.touched.contains_key(*key))
            .count();
        eprintln!(
            "{name:28} rows {:>7} -> {:>7}  other threads' rows {}  touched threads' rows {:>4}: changed {changed} added {added} removed {removed}",
            before.count,
            after.count,
            if before.others == after.others { "identical" } else { "CHANGED" },
            before.touched.len(),
        );
        // Renaming and settling change the thread row and, as settling
        // detaches the thread's provider session, its provider thread; the
        // projection metadata keeps the last sequence.
        let may_change = matches!(name, "threads" | "provider_threads" | "provider_sessions");
        if after.count != before.count
            || removed > 0
            || added > 0
            || (changed > 0 && !may_change)
            || (before.others != after.others && name != "metadata")
        {
            differing.push(name.to_owned());
        }
    }
    assert!(differing.is_empty(), "{differing:?}");
}

/// The ids of `host`'s stored, unarchived threads, by whether every run of theirs has
/// settled (`projection_eviction` keeps a thread with a run going whole).
fn by_settled(host: &Host) -> (Vec<String>, Vec<String>) {
    let store = host.store_now();
    // Archived threads take no metadata commands.
    let mut ids: Vec<&String> = store
        .threads
        .iter()
        .filter(|(_, thread)| thread.stored_row()["archivedAt"].is_null())
        .map(|(id, _)| id)
        .collect();
    ids.sort();
    let (settled, busy): (Vec<&String>, Vec<&String>) = ids.into_iter().partition(|id| {
        store.threads[*id].runs.iter().all(|run| {
            matches!(
                run.status.as_str(),
                "completed" | "failed" | "cancelled" | "interrupted" | "rolled_back"
            )
        })
    });
    (
        settled.into_iter().cloned().collect(),
        busy.into_iter().cloned().collect(),
    )
}

/// A thread released while idle (`projection_eviction`) goes back to its
/// slim records. A later write reads it whole again before planning, and
/// loses no row: the database changes exactly as on a server that never
/// released it.
#[tokio::test(flavor = "current_thread")]
async fn writing_a_thread_released_while_idle_keeps_every_row() {
    let (home_kept, conn_kept) = seeded(24);
    let (home_evicted, conn_evicted) = seeded(24);
    let kept = host(home_kept.path(), &conn_kept);
    let evicted = host(home_evicted.path(), &conn_evicted);
    let (settled, _) = by_settled(&evicted);
    assert!(settled.len() >= 6, "{settled:?}");
    let written = &settled[..3];
    let read_only = &settled[3];
    let untouched = &settled[4..];
    // First writes leave planned projections; a read hydrates another.
    for (n, id) in written.iter().enumerate() {
        for host in [&kept, &evicted] {
            dispatch(
                host,
                json!({"type": "thread.metadata.update", "commandId": format!("c:first:{n}"),
                    "threadId": id, "title": format!("First {n}")}),
            )
            .await;
        }
    }
    for host in [&kept, &evicted] {
        let _ = host.store_now().threads[read_only].projection();
        assert!(!partial(host, read_only));
    }
    let before_kept = snapshot(&conn_kept);
    let before_evicted = snapshot(&conn_evicted);
    // Release everything idle on one server only.
    let released =
        crate::projection_eviction::sweep(&evicted.control, std::time::Duration::ZERO).await;
    assert_eq!(
        released,
        crate::projection_eviction::Released {
            read: 1,
            planned: 3
        }
    );
    for id in written.iter().chain([read_only]) {
        assert!(partial(&evicted, id), "{id} is released");
        assert!(!partial(&kept, id), "{id} stays whole");
    }
    // Releasing writes nothing.
    assert!(
        without_bookkeeping(changes(&before_evicted, &snapshot(&conn_evicted)))
            .changed
            .is_empty()
    );
    // Second writes: each reads its thread whole again first.
    for (n, id) in written.iter().chain([read_only]).enumerate() {
        for host in [&kept, &evicted] {
            dispatch(
                host,
                json!({"type": "thread.metadata.update", "commandId": format!("c:second:{n}"),
                    "threadId": id, "title": format!("Second {n}")}),
            )
            .await;
            dispatch(
                host,
                json!({"type": "thread.pin", "commandId": format!("c:pin:{n}"), "threadId": id}),
            )
            .await;
        }
        assert!(!partial(&evicted, id), "{id} is read whole to be written");
        store_matches_database(&evicted, &conn_evicted, id);
        store_matches_database(&kept, &conn_kept, id);
    }
    // Both databases changed the same rows, and neither removed one.
    let diff_kept = without_bookkeeping(changes(&before_kept, &snapshot(&conn_kept)));
    let diff_evicted = without_bookkeeping(changes(&before_evicted, &snapshot(&conn_evicted)));
    assert!(
        diff_evicted.removed.is_empty(),
        "{:?}",
        diff_evicted.removed
    );
    assert_eq!(diff_evicted.changed, diff_kept.changed);
    assert_eq!(diff_evicted.added, diff_kept.added);
    for id in untouched {
        assert!(partial(&evicted, id) && partial(&kept, id), "{id}");
    }
}

/// A thread a stream watches, one with a run still going, or one used
/// recently stays whole.
#[tokio::test(flavor = "current_thread")]
async fn a_watched_busy_or_recent_thread_is_not_released() {
    let (home, conn) = seeded(24);
    let host = host(home.path(), &conn);
    let (settled, busy) = by_settled(&host);
    let (watched, idle, busy) = (&settled[0], &settled[1], &busy[0]);
    for id in [watched, idle, busy] {
        let _ = host.store_now().threads[id].projection();
    }
    let watch = host.control.thread_watchers.watch(watched);
    // A long idle time: nothing was idle that long.
    let released =
        crate::projection_eviction::sweep(&host.control, std::time::Duration::from_secs(3600))
            .await;
    assert_eq!(released, Default::default());
    // No idle time: only the unwatched, settled thread goes.
    let released =
        crate::projection_eviction::sweep(&host.control, std::time::Duration::ZERO).await;
    assert_eq!(released.read, 1);
    assert!(partial(&host, idle));
    assert!(!partial(&host, watched));
    assert!(!partial(&host, busy), "{busy} has a run going");
    drop(watch);
    let released =
        crate::projection_eviction::sweep(&host.control, std::time::Duration::ZERO).await;
    assert_eq!(released.read, 1);
    assert!(partial(&host, watched));
    assert!(!partial(&host, busy));
}

/// What committing a streamed delta to a long thread costs, phase by phase:
/// `cargo test --release --lib commit_cost -- --ignored --nocapture`.
/// A thread of about 4.5 MB of turn items, a live subscriber and a shell
/// listener (so every phase of a commit runs), a turn, then 200 deltas to its
/// assistant message. Afterwards, the thread's records are the database's.
#[tokio::test(flavor = "current_thread")]
#[ignore]
async fn commit_cost_of_deltas_on_a_long_thread() {
    let (home, conn, id) = super::lazy_tests::seeded_large(12, 2000, 2048);
    let host = host(home.path(), &conn);
    let _live = host.control.events.subscribe();
    let _shell = host.control.shell_events.subscribe();
    dispatch(
        &host,
        json!({"type": "message.dispatch", "commandId": "c:bench", "threadId": id, "messageId": "message:bench",
        "text": "Stream", "attachments": [], "createdBy": "user", "creationSource": "web"}),
    )
    .await;
    let run = host.store_now().threads[&id].runs.last().unwrap().id;
    let size = crate::wire_json::compact(&host.store_now().threads[&id].projection()).len();
    let provider = |event| crate::Event::RunProvider {
        visible_text: None,
        run_id: run,
        thread_id: id.clone(),
        event,
        position: None,
    };
    host.commit(provider(crate::workspace::ProviderEvent::TurnItem {
        item: crate::workspace::provider::ProviderItem::new(
            "bench-answer",
            "assistant_message",
            "running",
        )
        .with("text", ""),
    }))
    .await
    .unwrap();
    crate::commit_timing::enable();
    let mut took = Vec::new();
    for n in 0..200 {
        let started = std::time::Instant::now();
        host.commit(provider(crate::workspace::ProviderEvent::TurnItemDelta {
            item_id: "bench-answer".into(),
            delta: format!("word {n} "),
            held: false,
        }))
        .await
        .unwrap();
        took.push(started.elapsed());
    }
    let phases = crate::commit_timing::totals();
    took.sort();
    let ms = |d: std::time::Duration| d.as_secs_f64() * 1000.0;
    eprintln!(
        "thread {:.1} MB; per delta: median {:.2} ms, p95 {:.2} ms, max {:.2} ms",
        size as f64 / 1e6,
        ms(took[took.len() / 2]),
        ms(took[took.len() * 95 / 100]),
        ms(*took.last().unwrap())
    );
    for (phase, (count, total)) in &phases {
        eprintln!(
            "  {phase:32} {count:4} x  {:.3} ms",
            ms(*total) / *count as f64
        );
    }
    host.commit(crate::Event::RunFinished {
        run_id: run,
        thread_id: id.clone(),
        status: "completed".into(),
        error: None,
    })
    .await
    .unwrap();
    store_matches_database(&host, &conn, &id);
    if let Ok(limit) = std::env::var("T3CODE_COMMIT_COST_MAX_MS") {
        let limit: f64 = limit.parse().unwrap();
        let median = ms(took[took.len() / 2]);
        assert!(
            median <= limit,
            "a delta took {median:.2} ms, over {limit} ms"
        );
    }
}

/// How long a turn's snapshots hold the store on a long thread (about 5 MB),
/// committed one at a time and as the snapshot worker's atomic batches
/// (`commit_batch`, at most `turn::ATOMIC_BATCH` at once): every reader of
/// the store (`thread_list`, a snapshot) waits up to that long. Run with
/// `cargo test --release --lib batch_lock_hold -- --ignored --nocapture`.
#[tokio::test(flavor = "current_thread")]
#[ignore]
async fn batch_lock_hold_on_a_long_thread() {
    let (home, conn, id) = super::lazy_tests::seeded_large(12, 2000, 2048);
    let host = host(home.path(), &conn);
    let _live = host.control.events.subscribe();
    let _shell = host.control.shell_events.subscribe();
    dispatch(
        &host,
        json!({"type": "message.dispatch", "commandId": "c:bench", "threadId": id, "messageId": "message:bench",
        "text": "Run tools", "attachments": [], "createdBy": "user", "creationSource": "web"}),
    )
    .await;
    let run = host.store_now().threads[&id].runs.last().unwrap().id;
    let size = crate::wire_json::compact(&host.store_now().threads[&id].projection()).len();
    let snapshot = |name: String| crate::Event::RunProvider {
        visible_text: None,
        run_id: run,
        thread_id: id.clone(),
        event: crate::workspace::ProviderEvent::TurnItem {
            item: crate::workspace::provider::ProviderItem::new(
                name,
                "command_execution",
                "running",
            )
            .with("command", "ls"),
        },
        position: None,
    };
    let ms = |d: std::time::Duration| d.as_secs_f64() * 1000.0;
    let batch: usize = std::env::var("T3CODE_BATCH")
        .ok()
        .and_then(|n| n.parse().ok())
        .unwrap_or(crate::turn::ATOMIC_BATCH);
    let (mut single, mut batched) = (Vec::new(), Vec::new());
    for round in 0..10 {
        for n in 0..batch {
            let started = std::time::Instant::now();
            host.commit(snapshot(format!("one-{round}-{n}")))
                .await
                .unwrap();
            single.push(started.elapsed());
        }
        let events = (0..batch)
            .map(|n| (snapshot(format!("batch-{round}-{n}")), Vec::new()))
            .collect();
        let started = std::time::Instant::now();
        let results = host.control.commit_batch(events).await;
        batched.push(started.elapsed());
        assert!(results.iter().all(Result::is_ok));
    }
    single.sort();
    batched.sort();
    eprintln!(
        "thread {:.1} MB; store held per snapshot alone: median {:.2} ms, max {:.2} ms; \
         per batch of {batch}: median {:.2} ms, max {:.2} ms",
        size as f64 / 1e6,
        ms(single[single.len() / 2]),
        ms(*single.last().unwrap()),
        ms(batched[batched.len() / 2]),
        ms(*batched.last().unwrap()),
    );
    host.commit(crate::Event::RunFinished {
        run_id: run,
        thread_id: id.clone(),
        status: "completed".into(),
        error: None,
    })
    .await
    .unwrap();
    store_matches_database(&host, &conn, &id);
}
