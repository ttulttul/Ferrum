//! Start-up reads each stored thread's slim records only, and a thread's
//! whole projection when first needed (`stored_projection`). These tests
//! build a database of synthetic threads and check that start-up reads no
//! thread whole, that every shell row and the typed state equal what a whole
//! read gives, and that a commit plans and writes from the whole projection.
use super::*;
use crate::stored_projection::HYDRATIONS;
use serde_json::{json, Value};

const PROJECT: &str = "project:lazy";
/// Bulk for the records start-up must not read.
fn filler(seed: usize) -> String {
    format!("{seed:08}-").repeat(120)
}

fn at(seconds: usize) -> String {
    format!(
        "2026-03-01T{:02}:{:02}:{:02}.{:03}Z",
        (seconds / 3600) % 24,
        (seconds / 60) % 60,
        seconds % 60,
        seconds % 7
    )
}

/// A thread's projection, varied by `index` over the cases the shell row
/// distinguishes.
fn synthetic_thread(index: usize) -> Value {
    let id = format!("thread:{index}");
    let case = index % 12;
    let runs = 3 + index % 3;
    let run_id = |n: usize| format!("run:thread:{index}:ordinal:{n}");
    // The reference's (and this server's) ids for a run's records.
    let uri = crate::thread_projection::uri_component;
    let root = |n: usize| format!("node:run:{}:root", uri(&run_id(n)));
    let provider_thread = format!(
        "provider-thread:provider:codex:native-thread:pending:{}",
        run_id(1)
    );
    let base = index * 10;
    let mut out = json!({
        "thread": {
            "id": id, "projectId": PROJECT, "title": format!("Thread {index}"),
            "providerInstanceId": "codex", "modelSelection": {"instanceId": "codex", "model": "gpt-5"},
            "runtimeMode": "full-access", "interactionMode": "default", "branch": null,
            "worktreePath": null, "createdBy": "user", "creationSource": "web",
            "activeProviderThreadId": provider_thread,
            "createdAt": at(base), "updatedAt": at(base + 900),
            "archivedAt": if index.is_multiple_of(17) { json!(at(base + 950)) } else { Value::Null },
            "deletedAt": null,
        },
        "runs": [], "attempts": [], "nodes": [], "subagents": [], "providerSessions": [],
        "providerThreads": [], "providerTurns": [], "runtimeRequests": [], "messages": [],
        "plans": [], "turnItems": [], "checkpointScopes": [], "checkpoints": [],
        "contextHandoffs": [], "contextTransfers": [],
    });
    let push = |out: &mut Value, key: &str, record: Value| {
        out[key].as_array_mut().unwrap().push(record);
    };
    push(
        &mut out,
        "providerThreads",
        json!({"id": provider_thread, "appThreadId": id, "ownerNodeId": null,
            "providerInstanceId": "codex", "driver": "codex",
            "status": if case == 1 { "active" } else { "idle" },
            "firstRunOrdinal": 1, "lastRunOrdinal": runs,
            "nativeThreadRef": {"driver": "codex", "nativeId": format!("native-{index}"), "strength": "strong"},
            "createdAt": at(base + 1), "updatedAt": at(base + 800)}),
    );
    if index % 5 != 4 {
        push(
            &mut out,
            "providerSessions",
            json!({"id": format!("provider-session:{index}"), "providerInstanceId": "codex",
                "driver": "codex", "status": if index.is_multiple_of(3) { "stopped" } else { "ready" },
                "lastError": if case == 4 && index % 24 == 4 { json!("the provider failed") } else { Value::Null },
                "updatedAt": at(base + 850)}),
        );
    }
    let ordinal_of = |n: usize, k: usize| (n * 1_000_000 + k) as i64;
    for n in 1..=runs {
        let last = n == runs;
        let mut status = "completed";
        let mut queue_held = false;
        let mut intent = "turn";
        let mut started = true;
        match case {
            1 | 2 if last => status = "running",
            3 if n == runs - 1 => status = "failed",
            3 if last => {
                status = "queued";
                intent = "queued_turn";
                started = false;
            }
            4 if last => status = "failed",
            5 if n == 2 => status = "rolled_back",
            5 if last => {
                status = "cancelled";
                intent = "queued_turn";
                started = false;
            }
            8 | 9 if last => status = "waiting",
            11 if last => {
                status = "queued";
                queue_held = true;
                intent = "queued_turn";
                started = false;
            }
            11 if n == runs - 1 => {
                status = "queued";
                intent = "queued_turn";
                started = false;
            }
            _ => {}
        }
        let live = matches!(status, "running" | "waiting" | "queued");
        let message = format!("message:{index}:{n}");
        push(
            &mut out,
            "runs",
            json!({"id": run_id(n), "threadId": id, "ordinal": n, "providerInstanceId": "codex",
                "providerThreadId": provider_thread, "status": status, "queueHeld": queue_held,
                "userMessageId": message, "rootNodeId": root(n),
                "requestedAt": at(base + 10 * n),
                "startedAt": if started { json!(at(base + 10 * n + 1)) } else { Value::Null },
                "completedAt": if live || (!started && status != "cancelled") { Value::Null } else { json!(at(base + 10 * n + 5 + (case == 6) as usize)) }}),
        );
        let superseded = case == 6 && n == 2;
        let attempts = if superseded { 2 } else { 1 };
        for a in 1..=attempts {
            push(
                &mut out,
                "attempts",
                json!({"id": format!("run-attempt:run:{}:attempt:{a}", uri(&run_id(n))), "runId": run_id(n), "attemptOrdinal": a,
                    "rootNodeId": if a == 1 { root(n) } else { format!("node:run:{}:retry", uri(&run_id(n))) },
                    "providerInstanceId": "codex", "providerThreadId": provider_thread,
                    "status": if superseded && a == 1 { "superseded" } else if live { "running" } else { "completed" }}),
            );
        }
        for k in 0..6 {
            push(
                &mut out,
                "nodes",
                json!({"id": if k == 0 { root(n) } else { format!("node:{index}:{n}:{k}") }, "threadId": id, "runId": run_id(n),
                    "rootNodeId": root(n), "kind": if k == 0 { "turn" } else { "tool_call" },
                    "status": "completed", "startedAt": at(base + 10 * n + k),
                    "detail": filler(index * 100 + n * 10 + k)}),
            );
        }
        push(
            &mut out,
            "providerTurns",
            json!({"id": format!("provider-turn:{index}:{n}"), "providerThreadId": provider_thread,
                "nodeId": root(n), "ordinal": n, "status": "completed",
                "tokenUsage": {"usedTokens": 1000 * n, "maxTokens": 200_000}}),
        );
        // The prompt: a user's message, an idle compaction, or a
        // background wake.
        let compaction = case == 7 && last;
        let wake = case == 11 && n == 1;
        let mut prompt = json!({"id": message, "threadId": id, "runId": run_id(n), "role": "user",
            "text": if compaction { "/compact".to_owned() } else { format!("Prompt {n} of thread {index}") },
            "attachments": [], "createdBy": if compaction { "agent" } else { "user" },
            "creationSource": if compaction { "server" } else { "web" }, "streaming": false,
            "createdAt": at(base + 10 * n), "updatedAt": at(base + 10 * n)});
        if compaction {
            prompt["idleCompaction"] = json!({"requestedAt": at(base + 10 * n)});
        }
        if wake {
            prompt["createdBy"] = json!("agent");
            prompt["notification"] =
                json!({"outcome": "updated", "source": {"kind": "background_task"}});
        }
        push(&mut out, "messages", prompt);
        push(
            &mut out,
            "messages",
            json!({"id": format!("{message}:a"), "threadId": id, "runId": run_id(n), "role": "assistant",
                "text": filler(index + n), "createdBy": "agent", "creationSource": "provider",
                "streaming": false, "createdAt": at(base + 10 * n + 4), "updatedAt": at(base + 10 * n + 4)}),
        );
        let item = |kind: &str, status: &str, k: usize| {
            json!({"id": format!("item:{index}:{n}:{k}"), "threadId": id, "runId": run_id(n),
                "nodeId": root(n), "ordinal": ordinal_of(n, k), "type": kind, "status": status,
                "startedAt": at(base + 10 * n + 1), "updatedAt": at(base + 10 * n + 2)})
        };
        let mut user_item = item("user_message", "completed", 0);
        user_item["messageId"] = json!(message);
        user_item["inputIntent"] = json!(intent);
        push(&mut out, "turnItems", user_item);
        for k in 1..5 {
            let mut bulk = item(
                if k % 2 == 0 {
                    "reasoning"
                } else {
                    "command_execution"
                },
                "completed",
                k,
            );
            bulk["output"] = json!(filler(index * 1000 + n * 10 + k));
            push(&mut out, "turnItems", bulk);
        }
        let mut answer = item("assistant_message", "completed", 5);
        answer["messageId"] = json!(format!("{message}:a"));
        push(&mut out, "turnItems", answer);
        // What each case adds to the run.
        match case {
            1 if last => push(
                &mut out,
                "turnItems",
                item("command_execution", "running", 6),
            ),
            2 if last => {
                let mut wait = item("dynamic_tool", "running", 6);
                wait["toolName"] = json!("mcp__t3__t3_thread_wait");
                push(&mut out, "turnItems", wait);
                if index % 24 == 14 {
                    let mut task = item("dynamic_tool", "waiting", 7);
                    task["toolName"] = json!("delegate_task");
                    task["input"] = json!({"mode": "wait"});
                    push(&mut out, "turnItems", task);
                }
            }
            3 | 4 if status == "failed" => {
                let mut error = item("error", "failed", 6);
                error["failure"] = json!({"class": if case == 3 { "usage_limit" } else { "provider_error" },
                    "message": "the provider failed", "code": "E1",
                    "resetAt": if case == 3 { json!(at(base + 3000)) } else { Value::Null }});
                push(&mut out, "turnItems", error);
                let mut older = item("error", "failed", 7);
                older["updatedAt"] = json!(at(base + 10 * n + 1));
                older["failure"] =
                    json!({"class": "transient", "message": "earlier", "code": "E0"});
                push(&mut out, "turnItems", older);
            }
            6 if superseded => {
                let mut result = item("run_interrupt_result", "completed", 6);
                result["nodeId"] = json!(root(n));
                push(&mut out, "turnItems", result);
                if index % 24 == 18 {
                    push(
                        &mut out,
                        "turnItems",
                        item("run_interrupt_request", "completed", 7),
                    );
                }
            }
            6 if last => push(
                &mut out,
                "turnItems",
                item("run_interrupt_request", "completed", 6),
            ),
            7 if last && index % 24 == 19 => {
                push(&mut out, "turnItems", item("compaction", "failed", 6))
            }
            8 if last => {
                let mut link = item("link_request", "waiting", 6);
                link["updatedAt"] = json!(at(base + 10 * n + 3));
                push(&mut out, "turnItems", link);
                push(&mut out, "turnItems", item("secret_request", "waiting", 7));
            }
            _ => {}
        }
    }
    match case {
        9 => {
            for (k, kind) in ["approval", "user_input"].iter().enumerate() {
                push(
                    &mut out,
                    "runtimeRequests",
                    json!({"id": format!("request:{index}:{k}"), "nodeId": root(runs), "kind": kind,
                        "status": "pending", "createdAt": at(base + 100 + k),
                        "responseCapability": {"type": if k == 0 { "decision" } else { "message" }}}),
                );
            }
        }
        10 => {
            push(
                &mut out,
                "plans",
                json!({"id": format!("plan:{index}"), "threadId": id, "nodeId": root(1),
                    "kind": "proposed_plan", "status": "active"}),
            );
            // Two messages sent together, shown in the other order.
            for (k, ordinal) in [(1usize, 9_000_002i64), (2, 9_000_001)] {
                let message = format!("message:{index}:tie:{k}");
                push(
                    &mut out,
                    "messages",
                    json!({"id": message, "threadId": id, "runId": null, "role": "user",
                        "text": format!("tie {k}"), "attachments": [], "createdBy": "user",
                        "creationSource": "web", "streaming": false,
                        "createdAt": "2026-03-01T23:00:00.000Z", "updatedAt": if k == 1 { "2026-03-01T23:00:00.000Z" } else { "2026-03-01T23:00:00Z" }}),
                );
                push(
                    &mut out,
                    "turnItems",
                    json!({"id": format!("item:{index}:tie:{k}"), "threadId": id, "runId": null,
                        "nodeId": null, "ordinal": ordinal, "type": "user_message",
                        "status": "completed", "messageId": message, "inputIntent": "turn",
                        "updatedAt": at(base + 500)}),
                );
            }
        }
        _ => {}
    }
    push(
        &mut out,
        "checkpointScopes",
        json!({"id": format!("scope:{index}"), "threadId": id, "runId": run_id(1), "nodeId": root(1),
            "kind": "thread", "ordinalWithinParent": 0, "advancesAppRunCount": true,
            "createdAt": at(base + 2), "cwd": "/work"}),
    );
    push(
        &mut out,
        "checkpoints",
        json!({"id": format!("checkpoint:{index}"), "threadId": id, "scopeId": format!("scope:{index}"),
            "runId": run_id(1), "nodeId": root(1), "ordinalWithinScope": 1, "appRunOrdinal": 1,
            "status": "ready", "ref": format!("refs/t3/{index}"), "capturedAt": at(base + 6)}),
    );
    out["updatedAt"] = out["thread"]["updatedAt"].clone();
    out
}

/// A database holding `count` synthetic threads, written as this server
/// writes a new thread.
fn seeded(count: usize) -> (tempfile::TempDir, Connection) {
    let home = tempfile::tempdir().unwrap();
    let conn = open(home.path()).unwrap();
    conn.execute_batch(&format!(
        "INSERT INTO projection_projects (project_id, title, workspace_root, scripts_json, created_at, updated_at, auto_pull, has_custom_title)
           VALUES ('{PROJECT}', 'Lazy', '/work', '[]', '2026-03-01T00:00:00.000Z', '2026-03-01T00:00:00.000Z', 0, 1);"
    ))
    .unwrap();
    let mut writer = write::Writer::new(open(home.path()).unwrap());
    let threads = (0..count)
        .map(|index| crate::shared_store::ThreadWrite {
            thread_id: format!("thread:{index}"),
            before: None,
            after: synthetic_thread(index),
            deleted: false,
        })
        .collect();
    crate::shared_store::SharedStore::write(
        &mut writer,
        &crate::shared_store::SharedCommit {
            threads,
            projects: Vec::new(),
            events: Vec::new(),
            receipt: None,
            effects: Vec::new(),
            sequence: 0,
            occurred_at: at(0),
        },
    )
    .unwrap();
    (home, conn)
}

/// Each thread as start-up read it before: whole.
fn eager_threads(conn: &Connection, store: &crate::Store) -> Vec<crate::Thread> {
    read::thread_ids(conn)
        .unwrap()
        .into_iter()
        .map(|id| {
            let projection = read::thread_projection(conn, &id).unwrap().unwrap();
            let root = store.projects.get(PROJECT).map(|p| p.path.clone());
            crate::Thread::from_stored(projection, root.as_deref())
        })
        .collect()
}

fn typed(thread: &crate::Thread) -> (Value, String) {
    (
        serde_json::to_value(thread).unwrap(),
        format!("{:?}", thread.provider),
    )
}

#[test]
fn start_up_reads_no_thread_whole_and_every_shell_row_is_the_whole_ones() {
    let (_home, conn) = seeded(240);
    read::SLIM_STATEMENTS.with(|s| s.borrow_mut().clear());
    HYDRATIONS.with(|count| count.set(0));
    let mut store = crate::Store::default();
    let loaded = load_into(&conn, &mut store).unwrap();
    assert_eq!(loaded.threads, 240);
    assert_eq!(HYDRATIONS.with(|count| count.get()), 0);
    // No node is read, and turn items only by their type or message.
    let statements = read::SLIM_STATEMENTS.with(|s| s.borrow().clone());
    assert!(!statements.is_empty());
    for sql in &statements {
        assert!(!sql.contains("projection_nodes"), "{sql}");
        if sql.contains("projection_turn_items") {
            assert!(
                sql.contains("i.type = k.type")
                    || sql.contains("'queued_turn'")
                    || sql.contains("json_extract(payload_json, '$.messageId') = ?2"),
                "{sql}"
            );
        }
    }
    let eager = eager_threads(&conn, &store);
    assert_eq!(eager.len(), 240);
    let mut cases = std::collections::HashSet::new();
    for whole in &eager {
        let lazy = &store.threads[&whole.id];
        let stored = lazy.stored.as_ref().unwrap();
        assert!(stored.is_partial());
        let expected = whole.shell();
        assert_eq!(lazy.shell(), expected, "the shell row of {}", whole.id);
        assert_eq!(typed(lazy), typed(whole), "the typed state of {}", whole.id);
        cases.insert((
            expected["status"].clone(),
            expected["blockedOnThreadWait"].clone(),
            expected["latestRunStopRequested"].clone(),
            expected["pendingRuntimeRequest"].is_null(),
            expected["lastErrorClass"].clone(),
        ));
        assert!(stored.is_partial(), "a shell row reads {} whole", whole.id);
    }
    // The synthetic threads reach the shell's distinct outcomes.
    assert!(cases.len() >= 8, "{cases:?}");
    assert_eq!(HYDRATIONS.with(|count| count.get()), 0);
    // Any other read of a thread reads it whole, once.
    let thread = &store.threads["thread:6"];
    let whole = eager.iter().find(|t| t.id == "thread:6").unwrap();
    assert_eq!(thread.full_projection(), whole.full_projection());
    assert_eq!(thread.projection(), whole.projection());
    assert_eq!(thread.shell(), whole.shell());
    assert_eq!(HYDRATIONS.with(|count| count.get()), 1);
}

#[test]
fn start_up_never_decodes_nodes_or_unfiltered_turn_items() {
    let (_home, conn) = seeded(60);
    let expected: Vec<(String, Value)> = eager_threads(&conn, &crate::Store::default())
        .iter()
        .map(|thread| (thread.id.clone(), thread.shell()))
        .collect();
    // Records start-up must not read are no longer JSON: reading one fails.
    conn.execute_batch(
        "UPDATE orchestration_v2_projection_nodes SET payload_json = 'unreadable';
         UPDATE orchestration_v2_projection_turn_items SET payload_json = 'unreadable'
           WHERE type IN ('reasoning', 'command_execution') AND status = 'completed';",
    )
    .unwrap();
    let mut store = crate::Store::default();
    load_into(&conn, &mut store).unwrap();
    for (id, shell) in expected {
        assert_eq!(store.threads[&id].shell(), shell, "the shell row of {id}");
    }
    // A thread read whole now fails, and says so instead of serving less.
    assert!(store.threads["thread:0"]
        .stored
        .as_ref()
        .unwrap()
        .try_get()
        .is_err());
}

fn control_with(home: &std::path::Path) -> crate::control_state::ControlState {
    let documents = std::sync::Arc::new(crate::persistence::MemoryStore::new());
    let dir = std::path::Path::new("/state");
    let auth = crate::auth_service::AuthService::with_bootstrap("secret")
        .open_in(documents.clone(), dir)
        .unwrap();
    let mut control = crate::control_state::ControlState::open(
        auth,
        documents,
        Box::new(crate::event_log::MemoryLog::default()),
        dir,
    )
    .unwrap();
    let writer = write::Writer::new(open(home).unwrap());
    control.attach_shared_store(Box::new(writer), 0);
    control
}

/// Every row of the tables a slim read leaves out, by id.
fn heavy_rows(conn: &Connection) -> std::collections::HashMap<String, String> {
    let mut rows = std::collections::HashMap::new();
    for sql in [
        "SELECT turn_item_id, payload_json FROM orchestration_v2_projection_turn_items ORDER BY turn_item_id",
        "SELECT node_id, payload_json FROM orchestration_v2_projection_nodes ORDER BY node_id",
    ] {
        let mut statement = conn.prepare(sql).unwrap();
        rows.extend(
            statement
                .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
                .unwrap()
                .map(Result::unwrap),
        );
    }
    rows
}

#[test]
fn a_commit_on_a_lazily_read_thread_plans_and_writes_from_its_whole_projection() {
    let (home, conn) = seeded(36);
    let mut control = control_with(home.path());
    load_into(&conn, control.store.get_mut()).unwrap();
    // The same threads read whole, as start-up read them before.
    let mut eager = crate::Store::default();
    for thread in eager_threads(&conn, control.store.get_mut()) {
        eager.threads.insert(thread.id.clone(), thread);
    }
    HYDRATIONS.with(|count| count.set(0));
    // The plans of a rename, a restart's reconcile (it cuts a running run
    // and adds its notice), and a delete are the whole projection's.
    let mut events = vec![
        crate::Event::ThreadPatched {
            thread_id: "thread:0".into(),
            patch: json!({"title": "Renamed", "row": {"title": "Renamed"}}),
        },
        crate::Event::ThreadDeleted {
            thread_id: "thread:12".into(),
        },
    ];
    let reconciled = crate::runtime_recovery::reconcile(
        control.store.get_mut(),
        "startup",
        1_800_000_000,
        "2027-01-15T08:00:00.000Z",
        &Value::Null,
    );
    assert!(reconciled.iter().any(|event| matches!(event,
        crate::Event::ProviderRuntimeReconciled { cut, .. } if !cut.is_empty())));
    // Only threads with something to stop: a stopped session is not live.
    assert!(reconciled.len() < 36, "{}", reconciled.len());
    events.extend(reconciled);
    for event in &events {
        let record = crate::ServerEvent {
            id: 1,
            at: 1_800_000_000,
            event: event.clone(),
            receipt: None,
            thread_events: None,
        };
        let occurred_at = "2027-01-15T08:00:00.000Z";
        let lazy = crate::shared_store::plan(control.store.get_mut(), &record, occurred_at);
        let whole = crate::shared_store::plan(&eager, &record, occurred_at);
        assert!(!whole.threads.is_empty());
        for (lazy, whole) in lazy.threads.iter().zip(&whole.threads) {
            assert_eq!(lazy.thread_id, whole.thread_id);
            assert_eq!(lazy.before, whole.before, "before {}", whole.thread_id);
            assert_eq!(lazy.after, whole.after, "after {}", whole.thread_id);
            assert_eq!(lazy.deleted, whole.deleted);
        }
        assert_eq!(lazy.threads.len(), whole.threads.len());
        assert_eq!(lazy.events, whole.events);
        assert_eq!(lazy.stored, whole.stored);
    }
    // Committed for real, each touched thread is read whole first, and the
    // database ends with the rows an eager read commits: no turn item or
    // node is lost or changed differently.
    let touched: std::collections::HashSet<String> = events
        .iter()
        .filter_map(|event| event.thread_id().map(str::to_owned))
        .collect();
    let (eager_home, eager_conn) = seeded(36);
    let mut eager_control = control_with(eager_home.path());
    load_into(&eager_conn, eager_control.store.get_mut()).unwrap();
    eager_control.store.get_mut().threads = eager.threads;
    let before = heavy_rows(&conn);
    for event in events {
        control.commit_now(event.clone()).unwrap();
        eager_control.commit_now(event).unwrap();
    }
    let after = heavy_rows(&conn);
    assert!(before.keys().all(|id| after.contains_key(id)));
    assert_eq!(after.len(), heavy_rows(&eager_conn).len());
    // The commits' own times aside (the two commits ran a moment apart).
    let now = regex::Regex::new(r"20[0-9]{2}-[0-9]{2}-[0-9]{2}T[0-9:.]+Z").unwrap();
    let masked = |rows: std::collections::HashMap<String, String>| -> std::collections::HashMap<String, String> {
        rows.into_iter()
            .map(|(id, payload)| {
                let payload = now.replace_all(&payload, |found: &regex::Captures| {
                    if found[0].starts_with("2026-03-01") {
                        found[0].to_owned()
                    } else {
                        "<now>".to_owned()
                    }
                });
                (id, payload.into_owned())
            })
            .collect()
    };
    assert_eq!(masked(after), masked(heavy_rows(&eager_conn)));
    let store = control.store_now();
    for (id, thread) in &store.threads {
        let stored = thread.stored.as_ref().unwrap();
        assert_eq!(stored.is_partial(), !touched.contains(id), "{id}");
    }
    // The deleted thread is gone from the store but kept in the database.
    assert!(!store.threads.contains_key("thread:12"));
    assert!(read::deleted_thread_ids(&conn)
        .unwrap()
        .contains(&"thread:12".to_string()));
}

/// The same parity on a real database, whose state directory
/// `T3CODE_LAZY_PARITY_DIR` names: every thread's shell row and typed state
/// from its slim records equal those from its whole projection. It reads
/// every thread whole, so it is slow on a large database. Prints only
/// counts and field names.
#[test]
#[ignore]
fn slim_shell_rows_equal_whole_ones_on_a_real_database() {
    let Ok(dir) = std::env::var("T3CODE_LAZY_PARITY_DIR") else {
        return;
    };
    let conn = open(std::path::Path::new(&dir)).unwrap();
    let mut store = crate::Store::default();
    let started = std::time::Instant::now();
    load_into(&conn, &mut store).unwrap();
    eprintln!(
        "slim load: {} threads in {:?}",
        store.threads.len(),
        started.elapsed()
    );
    let mut differing = std::collections::BTreeMap::<String, usize>::new();
    let mut checked = 0;
    for id in read::thread_ids(&conn).unwrap() {
        let Some(lazy) = store.threads.get(&id) else {
            continue;
        };
        let projection = read::thread_projection(&conn, &id).unwrap().unwrap();
        let root = projection["thread"]["projectId"]
            .as_str()
            .and_then(|project| store.projects.get(project))
            .map(|project| project.path.clone());
        let whole = crate::Thread::from_stored(projection, root.as_deref());
        let (a, b) = (lazy.shell(), whole.shell());
        for (key, value) in b.as_object().unwrap() {
            if a.get(key) != Some(value) {
                *differing.entry(key.clone()).or_default() += 1;
            }
        }
        if a.as_object().unwrap().len() != b.as_object().unwrap().len() {
            *differing.entry("<keys>".into()).or_default() += 1;
        }
        if typed(lazy) != typed(&whole) {
            *differing.entry("<typed state>".into()).or_default() += 1;
        }
        assert!(lazy.stored.as_ref().unwrap().is_partial());
        checked += 1;
    }
    eprintln!(
        "checked {checked} threads in {:?}; differing: {differing:?}",
        started.elapsed()
    );
    assert!(differing.is_empty(), "{differing:?}");
}
