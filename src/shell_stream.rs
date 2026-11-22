//! Sidebar changes are invalidations. Retain only aggregate IDs and sequences,
//! then read the authoritative shell once per aggregate at the end of a batch.
use crate::{
    control_state::ControlState,
    environment,
    rpc_protocol::Reply,
    shared_store::StoredEvent,
    subscription::{Signal, Subscription},
    Store,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::HashMap;

pub(crate) const BATCH_SIZE: usize = 512;
const BATCH_MS: i64 = 50;
const RESEND_MS: i64 = 5_000;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Change {
    pub sequence: u64,
    pub id: String,
    pub project: bool,
    pub deleted: bool,
}

impl Change {
    pub(crate) fn from_event(event: &StoredEvent, store: &Store) -> Option<Self> {
        let thread = store.threads.get(&event.stream_id);
        if thread.is_some_and(|t| t.row["lineage"]["relationshipToParent"] == "subagent")
            && transcript_only(&event.event_type, event.payload["type"].as_str())
        {
            return None;
        }
        Some(Self {
            sequence: event.sequence,
            id: event.stream_id.clone(),
            project: event.aggregate == "project",
            deleted: event.event_type == "project.deleted",
        })
    }
}

pub(crate) fn transcript_only(event: &str, item: Option<&str>) -> bool {
    match event {
        "turn-item.updated" => !matches!(
            item,
            Some("secret_request" | "link_request" | "user_message")
        ),
        "node.updated" | "message.updated" | "provider-turn.updated" => true,
        _ => false,
    }
}

fn snapshot(store: &Store, sequence: u64, cursor: &mut Value) -> Value {
    *cursor = json!({"sequence":sequence,"sent":{}});
    json!({"kind":"snapshot","snapshot":environment::shell(store,sequence)})
}

pub(crate) async fn resync(control: &ControlState, cursor: &mut Value) -> Vec<Value> {
    let store = control.store.read().await;
    let head = *control.next_event_id.lock().await;
    vec![json!([snapshot(&store, head, cursor)])]
}

pub(crate) async fn replay(
    control: &ControlState,
    store: &Store,
    after: Option<u64>,
    head: u64,
) -> anyhow::Result<Option<Vec<Change>>> {
    match after {
        Some(after) if after == head => Ok(Some(Vec::new())),
        Some(after) if head.checked_sub(after).is_some_and(|gap| gap <= 1000) => {
            match control.shared.lock().await.as_mut() {
                Some(db) => db.shell_replay(after, head, store),
                None => Ok(None),
            }
        }
        _ => Ok(None),
    }
}

pub(crate) async fn open(
    control: &ControlState,
    sub: &mut Subscription,
) -> Result<Vec<Value>, Reply> {
    let store = control.store.read().await;
    let head = *control.next_event_id.lock().await;
    let after = sub.params["afterSequence"].as_u64();
    let mut items = Vec::new();
    let replay = replay(control, &store, after, head)
        .await
        .map_err(|error| {
            tracing::error!(%error, "could not read shell replay");
            Reply::Failure(
                json!({"_tag":"Fail","error":{"_tag":"OrchestrationV2GetShellSnapshotError",
                        "message":"Failed to prepare the application shell stream"}}),
            )
        })?;
    if replay.is_none() && after.is_some() && sub.params["httpSnapshotFallback"] == true {
        sub.cursor = json!({"finished":true});
        return Ok(vec![json!([{"kind":"snapshot-required"}])]);
    }
    let projects = if replay.is_some() {
        sub.cursor = json!({"sequence":after,"sent":{}});
        Value::Array(
            store
                .project_rows
                .values()
                .filter(|row| row["deletedAt"].is_null())
                .map(environment::project_shell)
                .collect(),
        )
    } else {
        let first = snapshot(&store, head, &mut sub.cursor);
        let projects = first["snapshot"]["projects"].clone();
        items.push(first);
        projects
    };
    // Native project shells have already resolved each root, including a lookup
    // that returned no identity. A resumed client retains cached absent identities.
    if cfg!(feature = "native") {
        let known = replay.is_some()
            && sub.params["knownProjectIdentities"].as_str()
                == Some(&environment::project_identities(&projects));
        let resolved: Vec<_> = projects
            .as_array()
            .into_iter()
            .flatten()
            .filter(|p| !known || p["repositoryIdentity"].is_null())
            .collect();
        items.extend(marked(&mut sub.cursor, resolved, head));
    }
    if let Some(replay) = replay {
        for batch in replay.chunks(BATCH_SIZE) {
            for batch in project(&store, &mut sub.cursor, batch, crate::platform::now_ms()) {
                items.extend(batch.as_array().unwrap().iter().cloned());
            }
        }
        // The live tail and replay each have their own unchanged-row cache.
        sub.cursor["sequence"] = json!(head);
        sub.cursor["sent"] = json!({});
    }
    if sub.params["requestCompletionMarker"] == true {
        items.push(json!({"kind":"synchronized"}));
    }
    Ok(if items.is_empty() {
        vec![]
    } else {
        vec![Value::Array(items)]
    })
}

pub(crate) fn project(
    store: &Store,
    cursor: &mut Value,
    changes: &[Change],
    now: i64,
) -> Vec<Value> {
    if cursor["finished"] == true {
        return vec![];
    }
    let seen = cursor["sequence"].as_u64().unwrap_or_default();
    let mut latest = HashMap::new();
    for change in changes.iter().filter(|c| c.sequence > seen) {
        latest.insert((change.project, &change.id), change);
    }
    let mut latest: Vec<_> = latest.into_values().collect();
    latest.sort_unstable_by_key(|c| c.sequence);
    let mut items = Vec::new();
    for change in latest {
        let Change { id, sequence, .. } = change;
        cursor["sequence"] = json!(sequence);
        if change.project {
            cursor.as_object_mut().unwrap().remove("identityDigest");
            let row = store
                .project_rows
                .get(id)
                .filter(|r| !change.deleted && r["deletedAt"].is_null());
            items.push(match row {
                Some(row) => json!({"kind":"project.updated","project":environment::project_shell(row),"sequence":sequence}),
                None => json!({"kind":"project.removed","projectId":id,"sequence":sequence}),
            });
        } else if let Some(thread) = store.threads.get(id).filter(|t| !t.archived) {
            let shell = thread.shell();
            let mut comparable = shell.clone();
            comparable.as_object_mut().unwrap().remove("updatedAt");
            let digest = crate::thread_events::digest(&comparable);
            let last = &cursor["sent"][id];
            if last["digest"] == digest
                && last["at"].as_i64().is_some_and(|at| now - at < RESEND_MS)
            {
                continue;
            }
            cursor["sent"][id] = json!({"digest":digest,"at":now});
            items.push(json!({"kind":"thread.updated","sequence":sequence,"location":"active","thread":shell}));
        } else {
            cursor["sent"].as_object_mut().unwrap().remove(id);
            items.push(json!({"kind":"thread.removed","sequence":sequence,"location":"active","threadId":id}));
        }
    }
    if items.is_empty() {
        vec![]
    } else {
        vec![Value::Array(items)]
    }
}

#[cfg(feature = "native")]
pub use live::Live;

#[cfg(feature = "native")]
mod live {
    use super::*;
    use tokio::sync::broadcast::{error::RecvError, Receiver};
    use tokio::time::{timeout_at, Duration, Instant};

    struct Group<T> {
        receiver: Receiver<T>,
        pending: Vec<T>,
        deadline: Option<Instant>,
        delay: Duration,
        limit: usize,
    }

    impl<T: Clone> Group<T> {
        fn new(receiver: Receiver<T>, limit: usize, ms: u64) -> Self {
            Self {
                receiver,
                pending: Vec::new(),
                deadline: None,
                delay: Duration::from_millis(ms),
                limit,
            }
        }

        // Cancellation-safe: the other lane can finish without resetting this window.
        async fn next(&mut self) -> Result<Vec<T>, RecvError> {
            while self.pending.len() < self.limit {
                let value = match self.deadline {
                    None => self.receiver.recv().await,
                    Some(at) => match timeout_at(at, self.receiver.recv()).await {
                        Ok(value) => value,
                        Err(_) => break,
                    },
                };
                match value {
                    Ok(value) => {
                        self.deadline
                            .get_or_insert_with(|| Instant::now() + self.delay);
                        self.pending.push(value);
                    }
                    Err(error) => {
                        self.pending.clear();
                        self.deadline = None;
                        return Err(error);
                    }
                }
            }
            self.deadline = None;
            Ok(std::mem::take(&mut self.pending))
        }
    }

    fn overflow() -> Reply {
        Reply::Failure(
            json!({"_tag":"Fail","error":{"_tag":"OrchestrationV2GetShellSnapshotError",
            "message":"Failed while streaming the application shell",
            "cause":{"_tag":"LiveStreamBufferError","message":"The live event buffer is full. Resume from the last received sequence."}}}),
        )
    }

    pub struct Live {
        changes: Group<Change>,
        identities: Group<String>,
    }
    impl Live {
        pub(crate) fn new(control: &ControlState) -> Self {
            Self {
                changes: Group::new(
                    control.shell_events.subscribe(),
                    BATCH_SIZE,
                    BATCH_MS as u64,
                ),
                identities: Group::new(crate::repository_identity::changes(), 64, 25),
            }
        }
        pub(crate) async fn next(&mut self) -> Option<Signal> {
            self.receive(true).await
        }

        /// Keep reading invalidations while the socket waits for ACKs. Replacing
        /// queued full states lets a slow client skip intermediate versions.
        pub(crate) async fn stream(
            &mut self,
            control: &ControlState,
            cursor: &mut Value,
            id: &Value,
            chunks: &tokio::sync::mpsc::Sender<(Value, Value)>,
            window: &tokio::sync::Semaphore,
        ) -> Reply {
            let mut pending = HashMap::<(bool, String), (usize, Value)>::new();
            let mut bytes = 0usize;
            let mut identity = None;
            let mut delivered = (0, 0);
            loop {
                tokio::select! {
                    biased;
                    permit = window.acquire(), if !pending.is_empty() || identity.is_some() => {
                        let Ok(permit) = permit else {break};
                        permit.forget();
                        let values = if pending.is_empty() {
                            delivered = (0, 0);
                            identity.take().expect("an identity refresh is waiting")
                        } else {
                            delivered = (pending.len(), bytes);
                            bytes = 0;
                            let mut items: Vec<_> = pending.drain().map(|(_, (_, item))| item).collect();
                            items.sort_unstable_by_key(|v| v["sequence"].as_u64());
                            Value::Array(items)
                        };
                        if chunks.send((id.clone(), values)).await.is_err() {break;}
                    }
                    signal = self.receive(identity.is_none()) => {
                        match signal {
                            Some(Signal::Shell(changes)) => {
                                let store = control.store.read().await;
                                for batch in project(&store, cursor, &changes, crate::platform::now_ms()) {
                                    let Value::Array(items) = batch else {unreachable!()};
                                    for item in items {
                                        let (project, id) = match item["kind"].as_str() {
                                            Some("project.updated") => (true, &item["project"]["id"]),
                                            Some("project.removed") => (true, &item["projectId"]),
                                            Some("thread.updated") => (false, &item["thread"]["id"]),
                                            _ => (false, &item["threadId"]),
                                        };
                                        let key = (project, id.as_str().unwrap().to_owned());
                                        let size = crate::wire_json::compact(&item).len();
                                        bytes += size;
                                        if let Some((old, _)) = pending.insert(key, (size, item)) {
                                            bytes -= old;
                                        }
                                        if window.available_permits() > 0 { delivered = (0, 0); }
                                        if pending.len() + delivered.0 > 1000 || bytes + delivered.1 > 8 * 1024 * 1024 {
                                            return overflow();
                                        }
                                    }
                                }
                            }
                            Some(Signal::ShellIdentities(roots)) => {
                                identity = refresh(control, cursor, &roots).await.pop();
                            }
                            Some(Signal::ShellResync) => return overflow(),
                            _ => break,
                        }
                    }
                }
            }
            Reply::Success(Value::Null)
        }

        async fn receive(&mut self, accept_identity: bool) -> Option<Signal> {
            let result = tokio::select! {
                changes = self.changes.next() => changes.map(Signal::Shell),
                roots = self.identities.next(), if accept_identity => roots.map(Signal::ShellIdentities),
            };
            match result {
                Ok(signal) => Some(signal),
                Err(RecvError::Lagged(_)) => Some(Signal::ShellResync),
                Err(RecvError::Closed) => None,
            }
        }
    }
}

fn marked(cursor: &mut Value, projects: Vec<&Value>, sequence: u64) -> Option<Value> {
    if projects.is_empty() {
        return None;
    }
    let roots: indexmap::IndexSet<_> = projects
        .iter()
        .map(|p| p["workspaceRoot"].as_str().unwrap_or_default())
        .collect();
    let roots: Vec<_> = roots.into_iter().collect();
    let digest = crate::thread_events::digest(&json!([roots, projects]));
    if cursor["identityDigest"] == digest {
        return None;
    }
    cursor["identityDigest"] = json!(digest);
    Some(
        json!({"kind":"snapshot","resolvedRepositoryIdentityRoots":roots,
                "snapshot":{"schemaVersion":2,"snapshotSequence":sequence,"projects":projects,"threads":[],"archivedThreads":[]}}),
    )
}

pub(crate) async fn refresh(
    control: &ControlState,
    cursor: &mut Value,
    roots: &[String],
) -> Vec<Value> {
    let store = control.store.read().await;
    let sequence = *control.next_event_id.lock().await;
    let projects: Vec<_> = store
        .project_rows
        .values()
        .filter(|p| {
            p["deletedAt"].is_null()
                && p["workspaceRoot"]
                    .as_str()
                    .is_some_and(|r| roots.iter().any(|root| root == r))
        })
        .map(environment::project_shell)
        .collect();
    marked(cursor, projects.iter().collect(), sequence)
        .map_or_else(Vec::new, |item| vec![json!([item])])
}

/// The worker can hibernate between a commit and its batch deadline. Keep the
/// pending invalidations in its existing durable subscription record.
pub(crate) async fn deliver(
    control: &ControlState,
    sub: &mut Subscription,
    signals: &[Signal],
    now: i64,
) -> (Vec<Value>, Option<i64>) {
    let mut batches = Vec::new();
    if signals.iter().any(|s| matches!(s, Signal::ShellResync)) {
        batches.extend(resync(control, &mut sub.cursor).await);
    }
    let mut pending: Vec<Change> = sub
        .cursor
        .as_object_mut()
        .and_then(|o| o.remove("pending"))
        .and_then(|v| serde_json::from_value(v).ok())
        .unwrap_or_default();
    let mut deadline = sub
        .cursor
        .as_object_mut()
        .and_then(|o| o.remove("deadline"))
        .and_then(|v| v.as_i64());
    let store = control.store.read().await;
    if deadline.is_some_and(|at| at <= now) {
        batches.extend(project(&store, &mut sub.cursor, &pending, now));
        pending.clear();
        deadline = None;
    }
    for signal in signals {
        if let Signal::Shell(changes) = signal {
            for change in changes {
                if change.sequence <= sub.cursor["sequence"].as_u64().unwrap_or_default() {
                    continue;
                }
                pending.push(change.clone());
                deadline.get_or_insert(now + BATCH_MS);
                if pending.len() == BATCH_SIZE {
                    batches.extend(project(&store, &mut sub.cursor, &pending, now));
                    pending.clear();
                    deadline = None;
                }
            }
        }
    }
    if let Some(at) = deadline {
        sub.cursor["deadline"] = json!(at);
        sub.cursor["pending"] = json!(pending);
    }
    (batches, deadline)
}
