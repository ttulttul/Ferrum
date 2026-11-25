//! The control plane's state: the event log and the store projected from it,
//! the change broadcast, auth, and the state documents (scheduled tasks and
//! webhook deliveries). It holds nothing that needs a local process, socket
//! or filesystem, so it builds for the workers target as well as natively.
//! The native server's `AppState` dereferences to it.
use crate::{
    auth_service::AuthService,
    event_log::EventLog,
    persistence::{DocumentStore, WriteMode},
    CommandReceipt, Event, ServerEvent, Store,
};
use indexmap::IndexMap;
use serde_json::Value;
use std::{collections::HashMap, path::PathBuf, sync::Arc};
use tokio::sync::{broadcast, Mutex, RwLock};

pub struct ControlState {
    pub auth: AuthService,
    pub store: RwLock<Store>,
    /// Serializes desktop launches while they provision a folder and record its thread.
    pub(crate) thread_launch: Mutex<()>,
    #[cfg(feature = "native")]
    pub(crate) thread_deleted: tokio::sync::Notify,
    /// The threads open `subscribeThread` streams watch, whose whole
    /// projections stay in memory (`projection_eviction`).
    pub(crate) thread_watchers: crate::projection_eviction::Watchers,
    pub events: broadcast::Sender<ServerEvent>,
    pub(crate) shell_events: broadcast::Sender<crate::shell_stream::Change>,
    pub log: Mutex<Box<dyn EventLog + Send>>,
    /// The orchestration database shared with the reference, when this host
    /// has one: each commit is written there first (docs/shared-store.md).
    pub(crate) shared: Mutex<Option<Box<dyn crate::shared_store::SharedStore + Send>>>,
    pub(crate) next_event_id: Mutex<u64>,
    pub(crate) pull_request_linked_refresh: Mutex<u64>,
    pub(crate) pull_request_linked_events: broadcast::Sender<u64>,
    /// Where state documents live.
    pub(crate) documents: Arc<dyn DocumentStore>,
    #[cfg(feature = "native")]
    pub(crate) scheduled_database: Option<Arc<Mutex<crate::statev2::scheduled_tasks::Database>>>,
    scheduled_tasks_path: PathBuf,
    pub(crate) scheduled_webhook_deliveries: Mutex<IndexMap<String, Value>>,
    pub(crate) scheduled_webhook_deliveries_path: PathBuf,
    pub(crate) scheduled_task_events: broadcast::Sender<()>,
    pub(crate) prompt_suggestion_events: broadcast::Sender<Value>,
}

impl ControlState {
    /// Rebuilds the store from `log` and reads the scheduled-task documents
    /// kept in `dir`.
    pub fn open(
        auth: AuthService,
        documents: Arc<dyn DocumentStore>,
        mut log: Box<dyn EventLog + Send>,
        dir: &std::path::Path,
    ) -> anyhow::Result<Self> {
        let mut store = Store::default();
        let mut next_event_id = 0;
        for record in log.load()? {
            next_event_id = next_event_id.max(record.id);
            store.apply_record(&record);
        }
        let scheduled_tasks_path = dir.join("scheduled-tasks.json");
        let scheduled_webhook_deliveries_path = dir.join("scheduled-webhook-deliveries.json");
        let scheduled_webhook_deliveries =
            read_by_id(&*documents, &scheduled_webhook_deliveries_path);
        Ok(Self {
            auth,
            store: RwLock::new(store),
            thread_launch: Mutex::new(()),
            #[cfg(feature = "native")]
            thread_deleted: Default::default(),
            thread_watchers: Default::default(),
            events: broadcast::channel(512).0,
            shell_events: broadcast::channel(2048).0,
            log: Mutex::new(log),
            shared: Mutex::new(None),
            next_event_id: Mutex::new(next_event_id),
            pull_request_linked_refresh: Mutex::new(0),
            pull_request_linked_events: broadcast::channel(64).0,
            documents,
            scheduled_tasks_path,
            #[cfg(feature = "native")]
            scheduled_database: None,
            scheduled_webhook_deliveries: Mutex::new(scheduled_webhook_deliveries),
            scheduled_webhook_deliveries_path,
            scheduled_task_events: broadcast::channel(64).0,
            prompt_suggestion_events: broadcast::channel(64).0,
        })
    }

    /// Writes every later commit to `shared` first. `sequence` is the
    /// database's latest event sequence; commits continue after it.
    pub fn attach_shared_store(
        &mut self,
        shared: Box<dyn crate::shared_store::SharedStore + Send>,
        sequence: u64,
    ) {
        *self.shared.get_mut() = Some(shared);
        let next = self.next_event_id.get_mut();
        *next = (*next).max(sequence);
    }

    /// Takes over the restart continuations the shared outbox owes for
    /// threads this server holds: each source run is marked owed a
    /// continuation (unless it already is), which the startup continuation
    /// pass then sends, and the effect is settled so the other server never
    /// sends it too. Effects for threads this server does not hold stay
    /// pending.
    pub async fn adopt_continuations(&self) -> anyhow::Result<usize> {
        let pending = match self.shared.lock().await.as_mut() {
            Some(shared) => shared.pending_continuations()?,
            None => return Ok(0),
        };
        let mut adopted = 0;
        for effect in pending {
            let run_id = crate::Run::uuid_for(&effect.source_run_id);
            let owed = {
                let store = self.store.read().await;
                let Some(run) = store
                    .threads
                    .get(&effect.thread_id)
                    .and_then(|thread| thread.runs.iter().find(|run| run.id == run_id))
                else {
                    continue;
                };
                run.owed_continuation().is_some()
            };
            if !owed {
                self.commit(Event::ProviderRuntimeReconciled {
                    thread_id: effect.thread_id.clone(),
                    cut: vec![],
                    held: vec![],
                    requeued: vec![],
                    resent: vec![],
                    stale_items: vec![],
                    background: vec![],
                    continued: Some((run_id, effect.process_loss_count)),
                })
                .await?;
            }
            if let Some(shared) = self.shared.lock().await.as_mut() {
                shared.settle_effect(&effect.effect_id, "succeeded")?;
            }
            adopted += 1;
        }
        Ok(adopted)
    }

    /// The title effects the outbox still owes, for this server to run.
    pub async fn pending_title_effects(&self) -> Vec<crate::title_generation::TitleEffect> {
        match self.shared.lock().await.as_mut() {
            Some(shared) => shared.pending_title_effects().unwrap_or_else(|error| {
                tracing::warn!(%error, "could not read title effects from the outbox");
                Vec::new()
            }),
            None => Vec::new(),
        }
    }

    /// Settles an outbox effect this server carried out.
    pub async fn settle_effect(&self, effect_id: &str, status: &str) {
        if let Some(shared) = self.shared.lock().await.as_mut() {
            if let Err(error) = shared.settle_effect(effect_id, status) {
                tracing::warn!(%error, effect_id, "could not settle an outbox effect");
            }
        }
    }

    pub(crate) async fn scheduled_task_list(&self) -> anyhow::Result<Vec<Value>> {
        #[cfg(feature = "native")]
        if let Some(database) = &self.scheduled_database {
            return database.lock().await.list();
        }
        let mut tasks: Vec<Value> = self
            .documents
            .read_optional(&self.scheduled_tasks_path)?
            .map(|bytes| serde_json::from_slice(&bytes))
            .transpose()?
            .unwrap_or_default();
        tasks.sort_by(|a, b| {
            b["createdAt"]
                .as_str()
                .cmp(&a["createdAt"].as_str())
                .then_with(|| a["id"].as_str().cmp(&b["id"].as_str()))
        });
        Ok(tasks)
    }

    pub(crate) async fn scheduled_task_upsert(&self, task: Value) -> anyhow::Result<()> {
        #[cfg(feature = "native")]
        if let Some(database) = &self.scheduled_database {
            database.lock().await.save(&task)?;
            let _ = self.scheduled_task_events.send(());
            return Ok(());
        }
        let mut tasks = self.scheduled_task_list().await?;
        let id = task["id"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("scheduled task has no id"))?;
        tasks.retain(|item| item["id"] != id);
        tasks.push(task);
        let bytes = serde_json::to_vec(&tasks)?;
        self.documents
            .write(&self.scheduled_tasks_path, &bytes, WriteMode::Replace)?;
        let _ = self.scheduled_task_events.send(());
        Ok(())
    }

    pub(crate) async fn scheduled_task_delete(&self, id: &str) -> anyhow::Result<bool> {
        #[cfg(feature = "native")]
        if let Some(database) = &self.scheduled_database {
            let existed = database.lock().await.get(id)?.is_some();
            if existed {
                database.lock().await.delete(id)?;
            }
            if existed {
                let _ = self.scheduled_task_events.send(());
            }
            return Ok(existed);
        }
        let mut tasks = self.scheduled_task_list().await?;
        let before = tasks.len();
        tasks.retain(|item| item["id"] != id);
        if tasks.len() != before {
            self.documents.write(
                &self.scheduled_tasks_path,
                &serde_json::to_vec(&tasks)?,
                WriteMode::Replace,
            )?;
            let _ = self.scheduled_task_events.send(());
            return Ok(true);
        }
        Ok(false)
    }

    pub(crate) async fn persist_scheduled_webhook_deliveries(&self) -> anyhow::Result<()> {
        let deliveries = self.scheduled_webhook_deliveries.lock().await;
        let bytes = serde_json::to_vec(&deliveries.values().collect::<Vec<_>>())?;
        drop(deliveries);
        self.documents.write(
            &self.scheduled_webhook_deliveries_path,
            &bytes,
            WriteMode::Replace,
        )?;
        Ok(())
    }

    /// `commit` for hosts that never hold this state's locks across an
    /// await, such as a host handling one message at a time: the
    /// locks are free, so the commit completes without suspending.
    pub fn commit_now(&self, event: Event) -> anyhow::Result<ServerEvent> {
        futures_util::FutureExt::now_or_never(self.commit(event))
            .ok_or_else(|| anyhow::anyhow!("the control state is busy"))?
    }

    /// The last committed event's id, for hosts as `commit_now` describes.
    pub fn sequence_now(&self) -> u64 {
        *self
            .next_event_id
            .try_lock()
            .expect("the control state is not locked between messages")
    }

    /// The store, for hosts as `commit_now` describes.
    pub fn store_now(&self) -> tokio::sync::RwLockReadGuard<'_, Store> {
        self.store
            .try_read()
            .expect("the control state is not locked between messages")
    }

    pub(crate) async fn commit(&self, event: Event) -> anyhow::Result<ServerEvent> {
        self.commit_receipted(event, None).await
    }

    /// What each of `events` would send live if they were committed in turn
    /// now: its provider snapshot events (`node.updated`, `message.updated`,
    /// `turn-item.updated`, `plan.updated`) as `type:id` keys in order, and
    /// how many live events it sends in all.
    pub(crate) async fn dry_run(&self, events: &[Event]) -> Vec<(Vec<String>, usize)> {
        let store = self.store.read().await;
        let mut scratch = Store::default();
        for event in events {
            if let Some(id) = event.thread_id() {
                if let (false, Some(thread)) =
                    (scratch.threads.contains_key(id), store.threads.get(id))
                {
                    scratch.threads.insert(id.to_owned(), thread.clone());
                }
            }
        }
        drop(store);
        for event in events {
            // As a commit does; a thread that cannot be read fails below.
            let _ = crate::shared_store::hydrate_touched(&scratch, event);
        }
        let at = crate::now();
        events
            .iter()
            .map(|event| {
                let record = ServerEvent {
                    id: 0,
                    at,
                    event: event.clone(),
                    receipt: None,
                    thread_events: None,
                };
                // A thread read from the shared database projects its stored
                // records, which a commit plans along with it.
                let stored_after = if scratch
                    .threads
                    .values()
                    .any(|thread| thread.stored.is_some())
                {
                    shared_commit(&scratch, &record, 0).1
                } else {
                    Vec::new()
                };
                let items = thread_events(&scratch, &record, &stored_after, &[])
                    .and_then(|events| events.items)
                    .unwrap_or_default();
                scratch.apply_record(&record);
                for (thread_id, stored) in stored_after {
                    if let Some(thread) = scratch.threads.get_mut(&thread_id) {
                        stored.install(thread);
                    }
                }
                (items.iter().filter_map(snapshot_key).collect(), items.len())
            })
            .collect()
    }

    /// `commit`, leaving the live events `suppress` names (`type:id` keys
    /// of snapshots a later snapshot of the same batch replaces) out of what
    /// subscribers get, as the reference's provider commit batch drops them.
    pub(crate) async fn commit_suppressing(
        &self,
        event: Event,
        suppress: Vec<String>,
    ) -> anyhow::Result<ServerEvent> {
        self.commit_with(event, None, &suppress).await
    }

    /// Writes one event to the shared database (when there is one), appends
    /// it to the log, applies it to the store and broadcasts it.
    pub(crate) async fn commit_receipted(
        &self,
        event: Event,
        receipt: Option<CommandReceipt>,
    ) -> anyhow::Result<ServerEvent> {
        self.commit_with(event, receipt, &[]).await
    }

    /// Reads whole, on a blocking thread and without holding the store, each
    /// of `thread_ids` whose slim records alone are loaded
    /// (`stored_projection`), so no lock is held for the seconds a long
    /// thread takes to read. Concurrent callers share one read. A thread that
    /// fails to read is left for the caller, which reads it again and reports it.
    pub(crate) async fn load_whole(&self, thread_ids: impl IntoIterator<Item = String>) {
        let pending: Vec<crate::stored_projection::Stored> = {
            let store = self.store.read().await;
            thread_ids
                .into_iter()
                .filter_map(|id| store.threads.get(&id)?.stored.clone())
                .filter(crate::stored_projection::Stored::needs_read)
                .collect()
        };
        for stored in pending {
            let _ = tokio::task::spawn_blocking(move || stored.try_get().map(|_| ())).await;
        }
    }

    async fn commit_with(
        &self,
        event: Event,
        receipt: Option<CommandReceipt>,
        suppress: &[String],
    ) -> anyhow::Result<ServerEvent> {
        // Boxed: every command's future embeds its commits, and in a debug
        // build a commit's frames are large (see `rpc::stack_tests`).
        Box::pin(self.commit_unboxed(event, receipt, suppress)).await
    }

    async fn commit_unboxed(
        &self,
        event: Event,
        receipt: Option<CommandReceipt>,
        suppress: &[String],
    ) -> anyhow::Result<ServerEvent> {
        let _commit = crate::commit_timing::phase("commit");
        // Read the threads this commit plans from before taking the store
        // for writing, which every reader would wait for meanwhile.
        let phase = crate::commit_timing::phase("load whole");
        let touched = crate::shared_store::touched_threads(&*self.store.read().await, &event);
        self.load_whole(touched).await;
        drop(phase);
        let mut store = self.store.write().await;
        let mut next_event_id = self.next_event_id.lock().await;
        let mut shared = self.shared.lock().await;
        // A thread whose slim records alone are loaded is read whole first:
        // the plan below diffs whole projections only.
        let phase = crate::commit_timing::phase("hydrate check");
        crate::shared_store::hydrate_touched(&store, &event)?;
        drop(phase);
        let mut record = ServerEvent {
            id: *next_event_id + 1,
            at: crate::now(),
            event,
            receipt,
            thread_events: None,
        };
        // What the shared database gets: projection rows and one stored
        // event per domain event, numbered from the next sequence.
        let mut stored_after = Vec::new();
        let mut commit = None;
        let shell_listening = self.shell_events.receiver_count() > 0;
        if shared.is_some() || shell_listening {
            let _phase = crate::commit_timing::phase("plan (shared_commit)");
            let (planned_commit, stored) = shared_commit(&store, &record, *next_event_id);
            stored_after = stored;
            commit = Some(planned_commit);
        }
        let stored_events = commit.as_ref().map_or(0, |c| c.events.len()) as u64;
        // Live subscribers get what the commit changed in its thread as
        // domain events, one sequence number each.
        let mut live_events = 0;
        if self.events.receiver_count() > 0 {
            let _phase = crate::commit_timing::phase("live events (thread_events)");
            if let Some(mut events) = thread_events(&store, &record, &stored_after, suppress) {
                live_events = events.items.as_ref().map_or(0, Vec::len) as u64;
                record.id = *next_event_id + live_events.max(stored_events).max(1);
                if let Some(items) = events.items.take() {
                    events.items = Some(crate::thread_events::sequence(
                        &events.thread_id,
                        items,
                        record.id,
                    ));
                }
                record.thread_events = Some(Arc::new(events));
            }
        }
        record.id = *next_event_id + live_events.max(stored_events).max(1);
        let shell_phase = crate::commit_timing::phase("shell changes");
        let shell_changes: Vec<_> = commit
            .iter()
            .filter(|_| shell_listening)
            .flat_map(|commit| &commit.events)
            .filter_map(|event| crate::shell_stream::Change::from_event(event, &store))
            .collect();
        drop(shell_phase);
        if let (Some(writer), Some(mut commit)) = (shared.as_mut(), commit) {
            let _phase = crate::commit_timing::phase("database write");
            commit.sequence = record.id;
            if let Err(error) = writer.write(&commit) {
                // Nothing is logged or applied: the commit fails as a whole.
                tracing::error!(error = %format!("{error:#}"), "could not write a commit to the shared orchestration database");
                return Err(error);
            }
        }
        drop(shared);
        self.log.lock().await.append(&record)?;
        *next_event_id = record.id;
        #[cfg(feature = "native")]
        let thread_count = store.threads.len();
        let apply_phase = crate::commit_timing::phase("apply");
        store.apply_record(&record);
        drop(apply_phase);
        #[cfg(feature = "native")]
        if store.threads.len() < thread_count {
            self.thread_deleted.notify_one();
        }
        for (thread_id, stored) in stored_after {
            if let Some(thread) = store.threads.get_mut(&thread_id) {
                stored.install(thread);
            }
        }
        if let Event::ThreadPatched { ref patch, .. } = record.event {
            if patch["row"].get("pullRequests").is_some() || patch["row"].get("issues").is_some() {
                let mut revision = self.pull_request_linked_refresh.lock().await;
                *revision = revision.saturating_add(1);
                let _ = self.pull_request_linked_events.send(*revision);
            }
        }
        for change in shell_changes {
            let _ = self.shell_events.send(change);
        }
        let _ = self.events.send(record.clone());
        Ok(record)
    }
}

/// What `record` writes to the shared database, its events numbered after
/// `last`, and the stored records each thread read from the database ends
/// with.
fn shared_commit(
    store: &Store,
    record: &ServerEvent,
    last: u64,
) -> (
    crate::shared_store::SharedCommit,
    Vec<(String, crate::shared_store::StoredAfter)>,
) {
    use crate::shared_store::{project_event, thread_events, ProjectWrite, ReceiptWrite};
    let occurred_at = crate::thread_projection::iso(record.at);
    let planned = crate::shared_store::plan(store, record, &occurred_at);
    let command_id = record.receipt.as_ref().map(|r| r.command_id.as_str());
    let mut events = thread_events(&planned.events, command_id);
    let mut projects = Vec::new();
    let mut receipt = record.receipt.as_ref().map(|r| ReceiptWrite {
        command_id: r.command_id.clone(),
        aggregate: "thread",
        aggregate_id: r.thread_id.clone(),
        command_type: r.command_type.clone(),
    });
    let project_row = match &record.event {
        Event::ProjectCreated(project) => Some((crate::wire_project(project), None)),
        Event::ProjectCommitted {
            row,
            command_id,
            command_type,
            project,
            ..
        } => {
            receipt = Some(ReceiptWrite {
                command_id: command_id.clone(),
                aggregate: "project",
                aggregate_id: project.id.clone(),
                command_type: command_type.clone(),
            });
            Some((row.clone(), Some(command_id.clone())))
        }
        _ => None,
    };
    if let Event::Imported { project_ids, .. } = &record.event {
        for id in project_ids {
            if let Some(row) = store.project_rows.get(id) {
                events.push(project_event(None, row, None, &occurred_at));
                projects.push(ProjectWrite {
                    project_id: id.clone(),
                    row: row.clone(),
                });
            }
        }
    }
    if let Some((row, project_command)) = project_row {
        let id = row["id"].as_str().unwrap_or_default().to_owned();
        let previous = store.project_rows.get(&id);
        if previous != Some(&row) {
            events.push(project_event(
                previous,
                &row,
                project_command.as_deref(),
                &occurred_at,
            ));
            projects.push(ProjectWrite {
                project_id: id,
                row,
            });
        }
    }
    for (index, event) in events.iter_mut().enumerate() {
        event.sequence = last + 1 + index as u64;
    }
    let mut effects = crate::shared_store::continuation_effects(store, &record.event, &occurred_at);
    effects.extend(crate::shared_store::title_effects(&record.event));
    let commit = crate::shared_store::SharedCommit {
        threads: planned.threads,
        projects,
        events,
        receipt,
        effects,
        sequence: 0,
        occurred_at,
    };
    (commit, planned.stored)
}

/// What `record` changes in its thread's projection, worked out on a copy of
/// the thread so the store changes only once the record is logged. `None`
/// when the record touches no thread that exists before and after it.
/// A provider snapshot live event's `type:id` key.
fn snapshot_key(item: &Value) -> Option<String> {
    let event = if item["event"].is_object() {
        &item["event"]
    } else {
        item
    };
    let kind = event["type"].as_str()?;
    matches!(
        kind,
        "node.updated" | "message.updated" | "turn-item.updated" | "plan.updated"
    )
    .then(|| Some(format!("{kind}:{}", event["payload"]["id"].as_str()?)))
    .flatten()
}

/// The reference's `coalesce` (ProviderCommitBatch) over a batch's snapshot
/// keys, by commit: an earlier snapshot is dropped when a later one of the
/// same entity follows it with no first appearance of another entity in
/// between. Gives, per commit, the keys it leaves out.
pub(crate) fn coalesce(batch: &[Vec<String>]) -> Vec<Vec<String>> {
    let jobs: Vec<(usize, &String)> = batch
        .iter()
        .enumerate()
        .flat_map(|(commit, keys)| keys.iter().map(move |key| (commit, key)))
        .collect();
    let mut first_seen = HashMap::new();
    for (index, (_, key)) in jobs.iter().enumerate() {
        first_seen.entry(*key).or_insert(index);
    }
    let mut dropped = vec![Vec::new(); batch.len()];
    let mut next_of_key: HashMap<&String, usize> = HashMap::new();
    for index in (0..jobs.len()).rev() {
        let (commit, key) = jobs[index];
        let next = next_of_key.insert(key, index);
        let Some(next) = next else { continue };
        if (index + 1..next).all(|between| first_seen[jobs[between].1] <= index) {
            dropped[commit].push(key.clone());
        }
    }
    dropped
}

fn thread_events(
    store: &Store,
    record: &ServerEvent,
    stored_after: &[(String, crate::shared_store::StoredAfter)],
    suppress: &[String],
) -> Option<crate::thread_events::ThreadEvents> {
    let thread_id = record.event.thread_id()?.to_owned();
    let revision = store.threads.get(&thread_id)?.revision;
    let at = crate::thread_projection::iso(record.at);
    let order = crate::thread_events::Order::of(&record.event);
    let command_type = record
        .receipt
        .as_ref()
        .map(|receipt| receipt.command_type.as_str());
    let diff =
        |before: &Value, after: &Value| match crate::shared_store::detach_reason(&record.event) {
            Some(reason) => {
                crate::thread_events::diff_detaching(&thread_id, before, after, &at, reason, order)
            }
            None => crate::thread_events::diff_for_command(
                &thread_id,
                before,
                after,
                &at,
                order,
                command_type,
            ),
        };
    // The thread's revision after this commit (`Store::apply_record`).
    let after = revision + u64::from(!matches!(record.event, Event::RunOutput { .. }));
    let finish = |items: Option<Vec<Value>>| {
        let items = items.map(|mut items| {
            if !suppress.is_empty() {
                items.retain(|item| snapshot_key(item).is_none_or(|key| !suppress.contains(&key)));
            }
            items
        });
        crate::thread_events::ThreadEvents {
            before: crate::thread_events::token(revision),
            after: crate::thread_events::token(after),
            thread_id: thread_id.clone(),
            items,
        }
    };
    // A change planned from partial projections: the live events come from
    // them too, at the cost of the change.
    if let Some((_, crate::shared_store::StoredAfter::Patch(patch))) =
        stored_after.iter().find(|(id, _)| *id == thread_id)
    {
        let partial = &patch.partial;
        let _phase = crate::commit_timing::phase("  live: partial");
        let before = crate::thread_projection::wire_projection(&thread_id, &partial.before);
        let after = crate::thread_projection::wire_projection(&thread_id, &partial.after);
        let items = diff(&before, &after);
        #[cfg(debug_assertions)]
        assert_eq!(
            items,
            whole_thread_events(store, record, stored_after, &thread_id, &diff),
            "partial live events differ from the whole ones"
        );
        return Some(finish(items));
    }
    Some(finish(whole_thread_events(
        store,
        record,
        stored_after,
        &thread_id,
        &diff,
    )))
}

/// The live events of `record` from the thread's whole projections.
fn whole_thread_events(
    store: &Store,
    record: &ServerEvent,
    stored_after: &[(String, crate::shared_store::StoredAfter)],
    thread_id: &str,
    diff: &dyn Fn(&Value, &Value) -> Option<Vec<Value>>,
) -> Option<Vec<Value>> {
    let current = store.threads.get(thread_id)?;
    let mut scratch = Store::default();
    scratch
        .threads
        .insert(thread_id.to_owned(), current.clone());
    let phase = crate::commit_timing::phase("  live: before projection");
    let before = current.projection();
    drop(phase);
    scratch.apply_record(record);
    let thread = scratch.threads.get_mut(thread_id)?;
    if let Some((_, stored)) = stored_after.iter().find(|(id, _)| id == thread_id) {
        let whole = match current.stored.as_ref() {
            Some(current) => stored.whole(current.get()).into_owned(),
            None => stored.whole(&Value::Null).into_owned(),
        };
        thread.stored = Some(crate::stored_projection::Stored::whole(whole));
    }
    let phase = crate::commit_timing::phase("  live: after projection");
    let after = thread.projection();
    drop(phase);
    let _phase = crate::commit_timing::phase("  live: diff");
    diff(&before, &after)
}

/// A JSON array document as a map by each entry's `id`; empty when the
/// document is missing or unreadable.
fn read_by_id(documents: &dyn DocumentStore, path: &std::path::Path) -> IndexMap<String, Value> {
    documents
        .read(path)
        .ok()
        .and_then(|bytes| serde_json::from_slice::<Vec<Value>>(&bytes).ok())
        .map(|items| {
            items
                .into_iter()
                .filter_map(|item| Some((item["id"].as_str()?.to_owned(), item)))
                .collect()
        })
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::coalesce;

    fn keys(batch: &[&[&str]]) -> Vec<Vec<String>> {
        batch
            .iter()
            .map(|commit| commit.iter().map(|key| key.to_string()).collect())
            .collect()
    }

    #[test]
    fn a_later_snapshot_of_the_same_entity_replaces_an_earlier_one() {
        // A tool's running update, then its completion, in one batch: the
        // item's running snapshot goes. The node's stays, since the item
        // first appears after it (the reference's stream shows it so).
        let dropped = coalesce(&keys(&[
            &["node.updated:n", "turn-item.updated:i"],
            &["node.updated:n", "turn-item.updated:i"],
        ]));
        assert_eq!(dropped, keys(&[&["turn-item.updated:i"], &[]]));
    }

    #[test]
    fn another_entity_appearing_first_in_between_keeps_the_earlier_snapshot() {
        // b first appears between a's two snapshots, so a's first stays.
        let dropped = coalesce(&keys(&[&["a"], &["b"], &["a"]]));
        assert_eq!(dropped, keys(&[&[], &[], &[]]));
    }

    #[test]
    fn an_entity_seen_before_in_between_does_not_keep_it() {
        // b was seen before a's first snapshot, so a later b between a's
        // snapshots does not keep it; b's own earlier snapshot stays, as a
        // first appears after it.
        let dropped = coalesce(&keys(&[&["b", "a"], &["b", "a"]]));
        assert_eq!(dropped, keys(&[&["a"], &[]]));
    }

    #[test]
    fn a_lone_snapshot_is_kept() {
        assert_eq!(coalesce(&keys(&[&["a"]])), keys(&[&[]]));
        assert_eq!(coalesce(&keys(&[])), keys(&[]));
    }
}
