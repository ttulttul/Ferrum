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
use std::{path::PathBuf, sync::Arc};
use tokio::sync::{broadcast, Mutex, RwLock};

pub struct ControlState {
    pub auth: AuthService,
    pub store: RwLock<Store>,
    pub events: broadcast::Sender<ServerEvent>,
    pub log: Mutex<Box<dyn EventLog + Send>>,
    pub(crate) next_event_id: Mutex<u64>,
    pub(crate) pull_request_linked_refresh: Mutex<u64>,
    pub(crate) pull_request_linked_events: broadcast::Sender<u64>,
    /// Where state documents live.
    pub(crate) documents: Arc<dyn DocumentStore>,
    pub(crate) scheduled_tasks: Mutex<IndexMap<String, Value>>,
    pub(crate) scheduled_tasks_path: PathBuf,
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
        let scheduled_tasks = read_by_id(&*documents, &scheduled_tasks_path);
        let scheduled_webhook_deliveries_path = dir.join("scheduled-webhook-deliveries.json");
        let scheduled_webhook_deliveries =
            read_by_id(&*documents, &scheduled_webhook_deliveries_path);
        Ok(Self {
            auth,
            store: RwLock::new(store),
            events: broadcast::channel(512).0,
            log: Mutex::new(log),
            next_event_id: Mutex::new(next_event_id),
            pull_request_linked_refresh: Mutex::new(0),
            pull_request_linked_events: broadcast::channel(64).0,
            documents,
            scheduled_tasks: Mutex::new(scheduled_tasks),
            scheduled_tasks_path,
            scheduled_webhook_deliveries: Mutex::new(scheduled_webhook_deliveries),
            scheduled_webhook_deliveries_path,
            scheduled_task_events: broadcast::channel(64).0,
            prompt_suggestion_events: broadcast::channel(64).0,
        })
    }

    pub(crate) async fn persist_scheduled_tasks(&self) -> anyhow::Result<()> {
        let tasks = self.scheduled_tasks.lock().await;
        let bytes = serde_json::to_vec(&tasks.values().collect::<Vec<_>>())?;
        drop(tasks);
        self.documents
            .write(&self.scheduled_tasks_path, &bytes, WriteMode::Replace)?;
        Ok(())
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

    /// Appends one event to the log, applies it to the store and broadcasts it.
    pub(crate) async fn commit_receipted(
        &self,
        event: Event,
        receipt: Option<CommandReceipt>,
    ) -> anyhow::Result<ServerEvent> {
        let mut store = self.store.write().await;
        let mut next_event_id = self.next_event_id.lock().await;
        let mut record = ServerEvent {
            id: *next_event_id + 1,
            at: crate::now(),
            event,
            receipt,
            thread_events: None,
        };
        // Live subscribers get what the commit changed in its thread as
        // domain events, one sequence number each.
        if self.events.receiver_count() > 0 {
            if let Some(mut events) = thread_events(&store, &record) {
                let count = events.items.as_ref().map_or(0, Vec::len) as u64;
                record.id = *next_event_id + count.max(1);
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
        self.log.lock().await.append(&record)?;
        *next_event_id = record.id;
        store.apply_record(&record);
        if let Event::ThreadPatched { ref patch, .. } = record.event {
            if patch["row"].get("pullRequests").is_some() || patch["row"].get("issues").is_some() {
                let mut revision = self.pull_request_linked_refresh.lock().await;
                *revision = revision.saturating_add(1);
                let _ = self.pull_request_linked_events.send(*revision);
            }
        }
        let _ = self.events.send(record.clone());
        Ok(record)
    }
}

/// What `record` changes in its thread's projection, worked out on a copy of
/// the thread so the store changes only once the record is logged. `None`
/// when the record touches no thread that exists before and after it.
fn thread_events(
    store: &Store,
    record: &ServerEvent,
) -> Option<crate::thread_events::ThreadEvents> {
    let thread_id = record.event.thread_id()?.to_owned();
    let mut scratch = Store::default();
    let before = store.threads.get(&thread_id).map(|thread| {
        scratch.threads.insert(thread_id.clone(), thread.clone());
        thread.projection()
    })?;
    scratch.apply_record(record);
    let after = scratch.threads.get(&thread_id)?.projection();
    // An unchanged projection still moves subscribers past the commit.
    let items = crate::thread_events::diff(
        &thread_id,
        &before,
        &after,
        &crate::thread_projection::iso(record.at),
    );
    Some(crate::thread_events::ThreadEvents {
        before: crate::thread_events::digest(&before),
        after: crate::thread_events::digest(&after),
        thread_id,
        items,
    })
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
