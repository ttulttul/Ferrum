//! What one commit writes to the orchestration database both servers share
//! (`statev2.sqlite`, docs/shared-store.md), worked out from the store
//! before and after the commit. Platform neutral: the native server writes
//! it with SQLite (`statev2::write`); the workers build has no shared store.
use crate::{Event, ServerEvent, Store};
use serde_json::{json, Map, Value};

/// A database the commits are written to, in one transaction each, before
/// the commit is logged or applied.
pub trait SharedStore {
    /// A bounded, unpruned range of sidebar invalidations for a reconnect.
    fn shell_replay(
        &mut self,
        _after: u64,
        _head: u64,
        _store: &Store,
    ) -> anyhow::Result<Option<Vec<crate::shell_stream::Change>>> {
        Ok(None)
    }

    fn write(&mut self, commit: &SharedCommit) -> anyhow::Result<()>;

    /// The restart continuations the outbox still owes
    /// (`provider-runtime.continue` effects that are pending).
    fn pending_continuations(&mut self) -> anyhow::Result<Vec<PendingContinuation>> {
        Ok(Vec::new())
    }

    /// The `thread-title.generate` effects the outbox still owes.
    fn pending_title_effects(
        &mut self,
    ) -> anyhow::Result<Vec<crate::title_generation::TitleEffect>> {
        Ok(Vec::new())
    }

    /// Settles an outbox effect this server has taken over.
    fn settle_effect(&mut self, _effect_id: &str, _status: &str) -> anyhow::Result<()> {
        Ok(())
    }
}

/// A pending `provider-runtime.continue` effect.
#[derive(Clone, Debug, PartialEq)]
pub struct PendingContinuation {
    pub effect_id: String,
    pub thread_id: String,
    /// The wire id of the run to continue.
    pub source_run_id: String,
    pub process_loss_count: u32,
}

/// An `orchestration_v2_effect_outbox` row to enqueue (`ON CONFLICT DO
/// NOTHING`, as the reference's `EffectOutbox.enqueue`).
#[derive(Clone, Debug, PartialEq)]
pub struct EffectWrite {
    pub effect_id: String,
    pub command_id: String,
    pub thread_id: String,
    /// The request, with its `type`.
    pub request: Value,
}

/// One thread's records after the commit.
#[derive(Clone, Debug, PartialEq)]
pub struct ThreadWrite {
    pub thread_id: String,
    /// The full projection the database held before (`None` for a new
    /// thread). The writer writes every record that differs from it, and
    /// every record when the database has no row for the thread yet.
    pub before: Option<Value>,
    /// The full projection after the commit. For a deleted thread, only its
    /// thread record (with `deletedAt` set) is written.
    pub after: Value,
    pub deleted: bool,
}

/// A project row after the commit, with its event.
#[derive(Clone, Debug, PartialEq)]
pub struct ProjectWrite {
    pub project_id: String,
    /// The project in Rust's wire shape (`wire_project`).
    pub row: Value,
}

/// An `orchestration_events` row.
#[derive(Clone, Debug, PartialEq)]
pub struct StoredEvent {
    pub sequence: u64,
    /// `thread` or `project`.
    pub aggregate: &'static str,
    pub stream_id: String,
    pub event_type: String,
    pub occurred_at: String,
    pub command_id: Option<String>,
    pub payload: Value,
    /// `{runId?, nodeId?, driver?, providerInstanceId?}`.
    pub metadata: Value,
}

impl StoredEvent {
    pub fn event_id(&self) -> String {
        format!(
            "event:{}:{}:sequence:{}",
            self.aggregate,
            crate::thread_projection::uri_component(&self.stream_id),
            self.sequence
        )
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct ReceiptWrite {
    pub command_id: String,
    pub aggregate: &'static str,
    pub aggregate_id: String,
    pub command_type: String,
}

/// Everything one commit writes.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct SharedCommit {
    pub threads: Vec<ThreadWrite>,
    pub projects: Vec<ProjectWrite>,
    pub events: Vec<StoredEvent>,
    pub receipt: Option<ReceiptWrite>,
    pub effects: Vec<EffectWrite>,
    /// The commit's last sequence: the last event's, or the one it took
    /// without events.
    pub sequence: u64,
    pub occurred_at: String,
}

/// The fields of `computed_before`'s records that `computed_after` changed,
/// applied to `stored`: how a commit changes a thread whose records came
/// from the database. Records the computation adds are added whole; fields
/// and records only the database holds are kept.
pub fn overlay(stored: &Value, computed_before: &Value, computed_after: &Value) -> Value {
    let mut out = stored.clone();
    let patch = |target: &mut Value, before: &Value, after: &Value| {
        let (Some(target), Some(after)) = (target.as_object_mut(), after.as_object()) else {
            return;
        };
        for (key, value) in after {
            if before.get(key) != Some(value) {
                target.insert(key.clone(), value.clone());
            }
        }
        if let Some(before) = before.as_object() {
            for key in before.keys() {
                if !after.contains_key(key) {
                    target.remove(key);
                }
            }
        }
    };
    patch(
        &mut out["thread"],
        &computed_before["thread"],
        &computed_after["thread"],
    );
    let Some(collections) = computed_after.as_object() else {
        return out;
    };
    for (key, records) in collections {
        let Some(records) = records.as_array() else {
            continue;
        };
        if key == "visibleTurnItems" {
            continue;
        }
        let before: Map<String, Value> = computed_before[key]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|r| Some((r["id"].as_str()?.to_owned(), r.clone())))
            .collect();
        for record in records {
            let Some(id) = record["id"].as_str() else {
                continue;
            };
            let old = before.get(id);
            if old == Some(record) {
                continue;
            }
            let Some(target) = out[key.as_str()].as_array_mut() else {
                out[key.as_str()] = json!([record]);
                continue;
            };
            match target.iter_mut().find(|r| r["id"] == id) {
                Some(existing) => patch(existing, old.unwrap_or(&Value::Null), record),
                None => target.push(record.clone()),
            }
        }
    }
    out
}

/// The event metadata the reference keeps beside a thread event's payload.
fn metadata(event: &Value) -> Value {
    let mut out = Map::new();
    for key in ["runId", "nodeId", "driver", "providerInstanceId"] {
        if let Some(value) = event.get(key).filter(|v| !v.is_null()) {
            out.insert(key.into(), value.clone());
        }
    }
    Value::Object(out)
}

/// Stored events for domain events as `thread_events::diff` makes them.
pub fn thread_events(events: &[Value], command_id: Option<&str>) -> Vec<StoredEvent> {
    events
        .iter()
        .map(|event| StoredEvent {
            sequence: 0,
            aggregate: "thread",
            stream_id: event["threadId"].as_str().unwrap_or_default().to_owned(),
            event_type: event["type"].as_str().unwrap_or_default().to_owned(),
            occurred_at: event["occurredAt"].as_str().unwrap_or_default().to_owned(),
            command_id: command_id.map(str::to_owned),
            payload: event["payload"].clone(),
            metadata: metadata(event),
        })
        .collect()
}

/// A projection with only `thread` and empty collections: where a new
/// thread's records are diffed from.
pub fn empty_projection(thread: &Value) -> Value {
    let mut out = Map::new();
    out.insert("thread".into(), thread.clone());
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
        out.insert(key.into(), json!([]));
    }
    Value::Object(out)
}

/// The project event for a project row change, shaped as the reference's
/// `ProjectCommands` shapes it.
pub fn project_event(
    previous: Option<&Value>,
    row: &Value,
    command_id: Option<&str>,
    at: &str,
) -> StoredEvent {
    let id = row["id"].as_str().unwrap_or_default().to_owned();
    let (event_type, payload) = if !row["deletedAt"].is_null() {
        (
            "project.deleted",
            json!({"projectId": id, "deletedAt": row["deletedAt"]}),
        )
    } else if let Some(previous) = previous.filter(|p| p["deletedAt"].is_null()) {
        let mut payload = Map::new();
        payload.insert("projectId".into(), json!(id));
        for key in [
            "title",
            "workspaceRoot",
            "defaultModelSelection",
            "defaultThreadEnvMode",
            "autoPull",
            "faviconPath",
            "projectIcon",
            "scripts",
            "pinnedAt",
            "archivedAt",
        ] {
            if previous.get(key) != row.get(key)
                || (key == "title" && previous["hasCustomTitle"] != row["hasCustomTitle"])
            {
                payload.insert(key.into(), row[key].clone());
            }
        }
        payload.insert("updatedAt".into(), row["updatedAt"].clone());
        ("project.meta-updated", Value::Object(payload))
    } else {
        (
            "project.created",
            json!({"projectId": id, "title": row["title"], "workspaceRoot": row["workspaceRoot"],
                "defaultModelSelection": row["defaultModelSelection"], "faviconPath": row["faviconPath"],
                "projectIcon": row["projectIcon"], "scripts": row["scripts"],
                "createdAt": row["createdAt"], "updatedAt": row["updatedAt"]}),
        )
    };
    StoredEvent {
        sequence: 0,
        aggregate: "project",
        stream_id: id,
        event_type: event_type.into(),
        occurred_at: at.into(),
        command_id: command_id.map(str::to_owned),
        payload,
        metadata: json!({}),
    }
}

/// The restart continuations a reconcile owes, as outbox effects the
/// reference's `ProviderRuntimeRecoveryService` writes: at shutdown with no
/// lost process (`command:restart-prepare:<run>`), at start-up with the
/// processes lost in a row. Either server's recovery then continues the run.
pub fn continuation_effects(store: &Store, event: &Event, occurred_at: &str) -> Vec<EffectWrite> {
    let Event::ProviderRuntimeReconciled {
        thread_id,
        cut,
        continued,
        ..
    } = event
    else {
        return Vec::new();
    };
    let Some(thread) = store.threads.get(thread_id) else {
        return Vec::new();
    };
    let owed = cut
        .iter()
        .filter_map(|(run, cut)| Some((*run, cut.continuation?, cut.trigger.as_str())))
        .chain(
            continued
                .iter()
                .map(|(run, losses)| (*run, *losses, "startup")),
        );
    owed.filter_map(|(run, losses, trigger)| {
        let wire = thread.runs.iter().find(|r| r.id == run)?.wire_id();
        let shutdown = trigger == "shutdown";
        Some(EffectWrite {
            effect_id: format!("effect:restart-continuation:{wire}"),
            command_id: if shutdown {
                format!("command:restart-prepare:{wire}")
            } else {
                format!("command:runtime-reconcile:{trigger}:{thread_id}:{occurred_at}")
            },
            thread_id: thread_id.clone(),
            request: json!({"type": "provider-runtime.continue", "sourceRunId": wire,
                "processLossCount": if shutdown { 0 } else { losses }}),
        })
    })
    .collect()
}

/// The `thread-title.generate` effect a commit enqueues: the title request
/// its thread patch records.
pub fn title_effects(event: &Event) -> Vec<EffectWrite> {
    let Event::ThreadPatched { thread_id, patch } = event else {
        return Vec::new();
    };
    crate::title_generation::TitleEffect::from_patch(thread_id, &patch["titleGeneration"])
        .map(|effect| EffectWrite {
            effect_id: effect.effect_id.clone(),
            command_id: effect.request_id.clone(),
            thread_id: thread_id.clone(),
            request: effect.request(),
        })
        .into_iter()
        .collect()
}

/// The reason of a commit that detached a thread's provider session.
pub(crate) fn detach_reason(event: &Event) -> Option<&str> {
    match event {
        Event::ProviderSessionDetached { reason, .. } => Some(reason.as_str()),
        _ => None,
    }
}

/// The thread a commit touches and the threads it deletes.
fn touched_threads(store: &Store, event: &Event) -> Vec<String> {
    let mut ids: Vec<String> = event.thread_id().map(str::to_owned).into_iter().collect();
    match event {
        Event::ThreadDeleted { thread_id } => ids.push(thread_id.clone()),
        Event::ProjectCommitted {
            removed_threads, ..
        } => ids.extend(removed_threads.iter().cloned()),
        Event::Imported { thread_ids, .. } => ids.extend(thread_ids.iter().cloned()),
        // Output lines change a running run's turn items.
        Event::RunOutput { thread_id, .. } => ids.push(thread_id.clone()),
        _ => {}
    }
    ids.sort();
    ids.dedup();
    ids.retain(|id| {
        store.threads.contains_key(id) || matches!(event, Event::ThreadCreated(t) if &t.id == id)
    });
    ids
}

/// The thread changes of `record`, worked out on a copy of the touched
/// threads: each one's full projection before and after, the stored records
/// a thread read from the database ends with, and its domain events.
pub struct Planned {
    pub threads: Vec<ThreadWrite>,
    pub events: Vec<Value>,
    /// The stored records each thread read from the database ends with.
    pub stored: Vec<(String, Value)>,
}

pub fn plan(store: &Store, record: &ServerEvent, occurred_at: &str) -> Planned {
    let mut planned = Planned {
        threads: Vec::new(),
        events: Vec::new(),
        stored: Vec::new(),
    };
    for thread_id in touched_threads(store, &record.event) {
        let mut scratch = Store::default();
        let existing = store.threads.get(&thread_id);
        if let Some(thread) = existing {
            scratch.threads.insert(thread_id.clone(), thread.clone());
        }
        // An imported thread is new to the database: written whole, with
        // its thread.created.
        let before = existing
            .filter(|_| !matches!(record.event, Event::Imported { .. }))
            .map(|thread| thread.full_projection());
        let computed_before = existing
            .filter(|thread| thread.stored.is_some())
            .map(|thread| thread.computed_full_projection());
        scratch.apply_record(record);
        let Some(thread) = scratch.threads.get(&thread_id) else {
            // Deleted: the database keeps its records, marked deleted.
            if let Some(before) = before {
                let mut row = before["thread"].clone();
                row["deletedAt"] = json!(occurred_at);
                row["updatedAt"] = json!(occurred_at);
                planned
                    .events
                    .push(json!({"threadId": thread_id, "type": "thread.deleted",
                    "payload": row, "occurredAt": occurred_at,
                    "providerInstanceId": row["providerInstanceId"]}));
                let mut after = before.clone();
                after["thread"] = row;
                planned.threads.push(ThreadWrite {
                    thread_id,
                    before: Some(before),
                    after,
                    deleted: true,
                });
            }
            continue;
        };
        let after = match (&thread.stored, &computed_before) {
            (Some(stored), Some(computed_before)) => {
                let computed_after = thread.computed_full_projection();
                let stored_after = overlay(stored, computed_before, &computed_after);
                planned
                    .stored
                    .push((thread_id.clone(), stored_after.clone()));
                stored_after
            }
            _ => thread.full_projection(),
        };
        let diff_from = match &before {
            Some(before) => before.clone(),
            None => {
                planned.events.push(json!({"threadId": thread_id, "type": "thread.created",
                    "payload": after["thread"], "occurredAt": after["thread"]["createdAt"].as_str().unwrap_or(occurred_at),
                    "providerInstanceId": after["thread"]["providerInstanceId"]}));
                empty_projection(&after["thread"])
            }
        };
        planned.events.extend(crate::thread_events::diff_all(
            &thread_id,
            &diff_from,
            &after,
            occurred_at,
            detach_reason(&record.event),
            crate::thread_events::Order::of(&record.event),
        ));
        planned.threads.push(ThreadWrite {
            thread_id,
            before,
            after,
            deleted: false,
        });
    }
    planned
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    #[derive(Clone, Default)]
    struct Recorded(
        Arc<Mutex<Vec<SharedCommit>>>,
        Arc<Mutex<Vec<PendingContinuation>>>,
        Arc<Mutex<Vec<(String, String)>>>,
    );
    impl SharedStore for Recorded {
        fn write(&mut self, commit: &SharedCommit) -> anyhow::Result<()> {
            self.0.lock().unwrap().push(commit.clone());
            Ok(())
        }
        fn pending_continuations(&mut self) -> anyhow::Result<Vec<PendingContinuation>> {
            Ok(self.1.lock().unwrap().clone())
        }
        fn settle_effect(&mut self, effect_id: &str, status: &str) -> anyhow::Result<()> {
            self.2
                .lock()
                .unwrap()
                .push((effect_id.into(), status.into()));
            Ok(())
        }
    }

    fn cancelled_run(wire: &str) -> crate::Run {
        serde_json::from_value(json!({
            "id": crate::Run::uuid_for(wire), "thread_id": "t1", "provider": "default",
            "status": "cancelled", "started_at": 1, "finished_at": 2, "error": null, "wire_id": wire
        }))
        .unwrap()
    }

    #[test]
    fn a_restart_cut_owes_its_continuation_as_the_reference_writes_it() {
        let mut store = Store::default();
        let Event::ThreadCreated(mut thread) = thread() else {
            unreachable!()
        };
        thread.runs.push(cancelled_run("run:thread:t1:ordinal:1"));
        store.threads.insert("t1".into(), thread);
        let run = crate::Run::uuid_for("run:thread:t1:ordinal:1");
        let cut = |trigger: &str| Event::ProviderRuntimeReconciled {
            thread_id: "t1".into(),
            cut: vec![(
                run,
                crate::RestartCut {
                    trigger: trigger.into(),
                    at: 2,
                    continuation: Some(2),
                },
            )],
            held: vec![],
            requeued: vec![],
            resent: vec![],
            stale_items: vec![],
            background: vec![],
            continued: None,
        };
        let shutdown = continuation_effects(&store, &cut("shutdown"), "2026-01-01T00:00:00.000Z");
        assert_eq!(
            shutdown,
            vec![EffectWrite {
                effect_id: "effect:restart-continuation:run:thread:t1:ordinal:1".into(),
                command_id: "command:restart-prepare:run:thread:t1:ordinal:1".into(),
                thread_id: "t1".into(),
                request: json!({"type": "provider-runtime.continue", "sourceRunId": "run:thread:t1:ordinal:1", "processLossCount": 0}),
            }]
        );
        let startup = continuation_effects(&store, &cut("startup"), "2026-01-01T00:00:00.000Z");
        assert_eq!(
            startup[0].command_id,
            "command:runtime-reconcile:startup:t1:2026-01-01T00:00:00.000Z"
        );
        assert_eq!(startup[0].request["processLossCount"], 2);
    }

    #[test]
    fn a_continuation_the_outbox_owes_is_taken_over_and_settled() {
        let recorded = Recorded::default();
        let control = control(&recorded, 0);
        control.commit_now(thread()).unwrap();
        let wire = "run:thread:t1:ordinal:1";
        control
            .commit_now(Event::RunStarted(cancelled_run(wire)))
            .unwrap();
        recorded.1.lock().unwrap().extend([
            PendingContinuation {
                effect_id: "effect:restart-continuation:run:thread:t1:ordinal:1".into(),
                thread_id: "t1".into(),
                source_run_id: wire.into(),
                process_loss_count: 1,
            },
            PendingContinuation {
                effect_id: "effect:other".into(),
                thread_id: "elsewhere".into(),
                source_run_id: "run:x".into(),
                process_loss_count: 0,
            },
        ]);
        let adopted = futures_util::FutureExt::now_or_never(control.adopt_continuations())
            .unwrap()
            .unwrap();
        assert_eq!(adopted, 1);
        let store = control.store_now();
        let run = &store.threads["t1"].runs[0];
        assert_eq!(run.owed_continuation(), Some(1));
        assert_eq!(
            *recorded.2.lock().unwrap(),
            vec![(
                "effect:restart-continuation:run:thread:t1:ordinal:1".to_string(),
                "succeeded".to_string()
            )]
        );
    }

    fn control(recorded: &Recorded, sequence: u64) -> crate::control_state::ControlState {
        let documents = Arc::new(crate::persistence::MemoryStore::new());
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
        control.attach_shared_store(Box::new(recorded.clone()), sequence);
        control
    }

    fn thread() -> Event {
        Event::ThreadCreated(
            serde_json::from_value(json!({
                "id": "t1", "project_id": "p1", "title": "One", "cwd": "/work",
                "status": "idle", "messages": [], "runs": [], "created_at": 1,
                "archived": false, "settled": false, "snoozed_until": null,
                "pinned": false, "unread": false, "tags": []
            }))
            .unwrap(),
        )
    }

    #[test]
    fn each_commit_is_written_first_with_its_events_numbered_after_the_database() {
        let recorded = Recorded::default();
        let control = control(&recorded, 40);
        let created = control.commit_now(thread()).unwrap();
        let renamed = control
            .commit_now(Event::ThreadPatched {
                thread_id: "t1".into(),
                patch: json!({"title": "Two", "row": {"title": "Two"}}),
            })
            .unwrap();
        let deleted = control
            .commit_now(Event::ThreadDeleted {
                thread_id: "t1".into(),
            })
            .unwrap();
        let commits = recorded.0.lock().unwrap().clone();
        assert_eq!(commits.len(), 3);
        // The new thread: thread.created, then its records, from sequence 41.
        assert_eq!(commits[0].events[0].event_type, "thread.created");
        assert_eq!(commits[0].events[0].sequence, 41);
        assert_eq!(commits[0].sequence, created.id);
        assert_eq!(commits[0].threads[0].before, None);
        assert_eq!(commits[0].threads[0].after["thread"]["id"], "t1");
        // The rename: one event, the thread record.
        let types: Vec<&str> = commits[1]
            .events
            .iter()
            .map(|e| e.event_type.as_str())
            .collect();
        assert_eq!(types, ["thread.metadata-updated"]);
        assert_eq!(commits[1].events[0].sequence, created.id + 1);
        assert_eq!(commits[1].threads[0].after["thread"]["title"], "Two");
        assert_eq!(renamed.id, created.id + 1);
        // The delete keeps the records, marked deleted.
        assert!(commits[2].threads[0].deleted);
        assert_eq!(commits[2].events[0].event_type, "thread.deleted");
        assert!(!commits[2].threads[0].after["thread"]["deletedAt"].is_null());
        assert_eq!(deleted.id, created.id + 2);
    }

    #[test]
    fn overlay_keeps_what_only_the_database_holds() {
        let stored = json!({"thread": {"id": "t", "title": "A", "updatedAt": "2026-01-01T00:00:00.123Z", "only": 1},
            "runs": [{"id": "r", "status": "completed", "extra": true}]});
        let before = json!({"thread": {"id": "t", "title": "A", "updatedAt": "2026-01-01T00:00:00.000Z"},
            "runs": [{"id": "r", "status": "completed"}]});
        let after = json!({"thread": {"id": "t", "title": "B", "updatedAt": "2026-01-01T00:00:00.000Z"},
            "runs": [{"id": "r", "status": "completed"}, {"id": "r2", "status": "queued"}]});
        let out = overlay(&stored, &before, &after);
        assert_eq!(
            out["thread"],
            json!({"id": "t", "title": "B", "updatedAt": "2026-01-01T00:00:00.123Z", "only": 1})
        );
        assert_eq!(
            out["runs"],
            json!([{"id": "r", "status": "completed", "extra": true}, {"id": "r2", "status": "queued"}])
        );
    }

    #[test]
    fn project_events_follow_the_reference_shapes() {
        let row = json!({"id": "p", "title": "P", "workspaceRoot": "/w", "defaultModelSelection": null,
            "faviconPath": null, "projectIcon": null, "scripts": [], "createdAt": "a", "updatedAt": "a", "deletedAt": null});
        assert_eq!(
            project_event(None, &row, None, "a").event_type,
            "project.created"
        );
        let mut renamed = row.clone();
        renamed["title"] = json!("Q");
        renamed["updatedAt"] = json!("b");
        let event = project_event(Some(&row), &renamed, Some("c"), "b");
        assert_eq!(event.event_type, "project.meta-updated");
        assert_eq!(
            event.payload,
            json!({"projectId": "p", "title": "Q", "updatedAt": "b"})
        );
        let mut deleted = row.clone();
        deleted["deletedAt"] = json!("d");
        assert_eq!(
            project_event(Some(&row), &deleted, None, "d").payload,
            json!({"projectId": "p", "deletedAt": "d"})
        );
    }
}
