//! Live RPC streams over the control state, as plain records.
//!
//! A [`Subscription`] is the method, its parameters and a cursor (what the
//! stream last reported), all JSON, so it can be stored and resumed. The
//! stream holds no future: whoever learns of a change hands it to
//! [`Subscription::advance`] as a [`Signal`], and gets back the batches to
//! send. The native server feeds each stream from the broadcast channel it
//! listens on ([`Source`]). A host that keeps subscriptions in storage feeds
//! every stored subscription after each change instead.
//!
//! Streams over the workspace (terminals, VCS status, previews, devices) are
//! records too, but only the control plane keeps them that way (see
//! `crate::control_streams`); the native server serves them as tasks.
//! Streams over host-only services are not here.
use crate::{
    auth_service::{access::Change, Session},
    control_state::ControlState,
    rpc_protocol::Reply,
    Event, ServerEvent,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::sync::{broadcast, mpsc};

/// The methods this module serves.
pub const METHODS: &[&str] = &[
    "orchestration.subscribeShell",
    "orchestration.subscribeThread",
    "orchestration.subscribePromptSuggestion",
    "pullRequests.subscribeLinkedThreadRefreshes",
    "scheduledTasks.subscribe",
    "subscribeAuthAccess",
];

pub fn serves(method: &str) -> bool {
    METHODS.contains(&method)
}

/// One open stream.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Subscription {
    pub method: String,
    pub params: Value,
    /// What the stream last reported, as the method tracks it.
    pub cursor: Value,
}

/// A change a subscription may need to report.
#[derive(Clone)]
pub enum Signal {
    /// An event was committed to the log.
    Committed(Box<ServerEvent>),
    /// Events may have been missed; recompute from the store.
    Resync,
    Shell(Vec<crate::shell_stream::Change>),
    ShellResync,
    ShellIdentities(Vec<String>),

    /// The linked pull requests or issues of some thread changed.
    LinkedRefresh(u64),
    ScheduledTasks,
    /// Client-redacted settings, without and with secret reference support.
    SettingsChanged(std::sync::Arc<(Value, Value)>),
    /// A prompt suggestion was generated (`threadId`, `runId`, `text`).
    PromptSuggestion(Value),
    AuthAccess(Change),
    /// A frame of one of the workspace's event streams.
    Workspace(Box<crate::workspace::BusFrame>),
    /// Time to read the workspace's polled state (VCS status and branch,
    /// local servers) again.
    Poll,
}

/// A newly opened stream: the record, its first batches, and (when asked
/// for) where its later signals come from.
pub struct Opened {
    pub subscription: Subscription,
    pub first: Vec<Value>,
    pub source: Option<Source>,
}

impl Subscription {
    /// Opens `method`. With `listen`, also subscribes to the channel its
    /// signals arrive on, before reading the first snapshot, so nothing
    /// committed in between is missed.
    pub(crate) async fn open(
        control: &ControlState,
        principal: &Session,
        method: &str,
        params: &Value,
        listen: bool,
    ) -> Result<Opened, Reply> {
        let mut subscription = Subscription {
            method: method.to_owned(),
            params: params.clone(),
            cursor: Value::Null,
        };
        let (source, first) = match method {
            "orchestration.subscribeShell" => {
                #[cfg(feature = "native")]
                let source = listen.then(|| Source::Shell(crate::shell_stream::Live::new(control)));
                #[cfg(not(feature = "native"))]
                let source = None;
                let first = crate::shell_stream::open(control, &mut subscription).await?;
                (source, first)
            }
            "orchestration.subscribeThread" => {
                let source = listen.then(|| Source::Events(control.events.subscribe()));
                let Some((snapshot, cursor)) = thread_snapshot(control, params).await else {
                    return Err(Reply::Failure(
                        json!({"_tag":"Fail","error":thread_projection_error(&params["threadId"])}),
                    ));
                };
                subscription.cursor = cursor;
                let mut first = vec![snapshot];
                if params["requestCompletionMarker"] == true {
                    first.push(json!({"kind":"synchronized"}));
                }
                (source, vec![Value::Array(first)])
            }
            "orchestration.subscribePromptSuggestion" => (
                listen.then(|| Source::Suggestions(control.prompt_suggestion_events.subscribe())),
                vec![json!([null])],
            ),
            "pullRequests.subscribeLinkedThreadRefreshes" => {
                let source =
                    listen.then(|| Source::Linked(control.pull_request_linked_events.subscribe()));
                subscription.cursor = json!(*control.pull_request_linked_refresh.lock().await);
                (source, Vec::new())
            }
            "scheduledTasks.subscribe" => (
                listen.then(|| Source::ScheduledTasks(control.scheduled_task_events.subscribe())),
                vec![json!([scheduled_tasks_snapshot(control).await?])],
            ),
            "subscribeAuthAccess" => {
                let (snapshot, changes) = control.auth.subscribe_access();
                subscription.cursor = json!(0);
                let first = subscription
                    .advance(control, principal, Signal::AuthAccess(snapshot))
                    .await;
                (listen.then_some(Source::AuthAccess(changes)), first)
            }
            _ => return Err(Reply::defect(format!("Unknown stream: {method}"))),
        };
        Ok(Opened {
            subscription,
            first,
            source,
        })
    }

    /// The batches `signal` adds to this stream, moving its cursor past
    /// them. Signals the stream does not follow add nothing.
    pub async fn advance(
        &mut self,
        control: &ControlState,
        principal: &Session,
        signal: Signal,
    ) -> Vec<Value> {
        match (self.method.as_str(), signal) {
            ("orchestration.subscribeShell", Signal::Shell(changes)) => {
                let store = control.store.read().await;
                crate::shell_stream::project(
                    &store,
                    &mut self.cursor,
                    &changes,
                    crate::platform::now_ms(),
                )
            }
            ("orchestration.subscribeShell", Signal::ShellIdentities(roots)) => {
                crate::shell_stream::refresh(control, &mut self.cursor, &roots).await
            }
            ("orchestration.subscribeShell", Signal::ShellResync) => {
                crate::shell_stream::resync(control, &mut self.cursor).await
            }
            ("orchestration.subscribeThread", Signal::Committed(event))
                if matches!(event.event, Event::RunOutput { .. }) =>
            {
                // Provider output lines are not part of the projection.
                Vec::new()
            }
            ("orchestration.subscribeThread", Signal::Committed(event))
                if event.event.thread_id().is_some()
                    && event.event.thread_id() != self.params["threadId"].as_str() =>
            {
                // Another thread's change.
                Vec::new()
            }
            ("orchestration.subscribeThread", Signal::Committed(event))
                if self.cursor["sequence"]
                    .as_u64()
                    .is_some_and(|seen| event.id <= seen) =>
            {
                // Already in the snapshot this subscriber was last sent.
                Vec::new()
            }
            ("orchestration.subscribeThread", Signal::Committed(event))
                if event.thread_events.as_ref().is_some_and(|events| {
                    events.items.is_some() && self.cursor["projection"] == events.before.as_str()
                }) =>
            {
                // The subscriber has seen the projection this commit started
                // from: send what it changed as domain events.
                let events = event.thread_events.as_ref().expect("checked above");
                self.cursor = json!({"projection": events.after, "sequence": event.id});
                match events.items.as_deref() {
                    Some(items) if !items.is_empty() => vec![Value::Array(items.to_vec())],
                    _ => Vec::new(),
                }
            }
            ("orchestration.subscribeThread", Signal::Committed(_) | Signal::Resync) => {
                // A change no event expresses, or one the subscriber is not
                // ready for: a fresh snapshot, which the client applies the
                // same way.
                let Some((next, cursor)) = thread_snapshot(control, &self.params).await else {
                    return Vec::new();
                };
                let unchanged = self.cursor["projection"] == cursor["projection"];
                self.cursor = cursor;
                if unchanged {
                    return Vec::new();
                }
                vec![json!([next])]
            }
            ("orchestration.subscribePromptSuggestion", Signal::PromptSuggestion(event))
                if event["threadId"] == self.params["threadId"]
                    && self.params["threadId"].is_string() =>
            {
                vec![json!([{"runId":event["runId"],"text":event["text"]}])]
            }
            ("pullRequests.subscribeLinkedThreadRefreshes", Signal::LinkedRefresh(revision)) => {
                self.cursor = json!(revision);
                vec![json!([revision])]
            }
            ("scheduledTasks.subscribe", Signal::ScheduledTasks) => {
                scheduled_tasks_snapshot(control)
                    .await
                    .into_iter()
                    .map(|snapshot| json!([snapshot]))
                    .collect()
            }
            ("subscribeAuthAccess", Signal::AuthAccess(change)) => {
                let revision = self.cursor.as_u64().unwrap_or(0) + 1;
                self.cursor = json!(revision);
                vec![json!([change.event(&principal.id, revision)])]
            }
            _ => Vec::new(),
        }
    }
}

/// Where a stream's signals arrive natively: the channel it subscribed to.
pub enum Source {
    Events(broadcast::Receiver<ServerEvent>),
    #[cfg(feature = "native")]
    Shell(crate::shell_stream::Live),
    Suggestions(broadcast::Receiver<Value>),
    Linked(broadcast::Receiver<u64>),
    ScheduledTasks(broadcast::Receiver<()>),
    AuthAccess(mpsc::UnboundedReceiver<Change>),
}

impl Source {
    /// The next signal, or `None` once the channel has closed.
    pub async fn next(&mut self) -> Option<Signal> {
        use broadcast::error::RecvError::{Closed, Lagged};
        loop {
            return match self {
                #[cfg(feature = "native")]
                Source::Shell(events) => events.next().await,
                Source::Events(events) => match events.recv().await {
                    Ok(event) => Some(Signal::Committed(Box::new(event))),
                    Err(Lagged(_)) => Some(Signal::Resync),
                    Err(Closed) => None,
                },
                Source::Suggestions(events) => match events.recv().await {
                    Ok(event) => Some(Signal::PromptSuggestion(event)),
                    Err(Lagged(_)) => continue,
                    Err(Closed) => None,
                },
                Source::Linked(events) => match events.recv().await {
                    Ok(revision) => Some(Signal::LinkedRefresh(revision)),
                    Err(Lagged(_)) => continue,
                    Err(Closed) => None,
                },
                Source::ScheduledTasks(events) => match events.recv().await {
                    Ok(()) => Some(Signal::ScheduledTasks),
                    Err(Lagged(_)) => continue,
                    Err(Closed) => None,
                },
                Source::AuthAccess(changes) => changes.recv().await.map(Signal::AuthAccess),
            };
        }
    }
}

/// The thread's snapshot item and the stream cursor it leaves: the digest of
/// its projection, as commits name it (see [`crate::thread_events`]), and
/// the last commit it includes.
async fn thread_snapshot(control: &ControlState, params: &Value) -> Option<(Value, Value)> {
    let thread_id = params["threadId"].as_str()?;
    // The store first: a commit holds it while it numbers itself, so the
    // sequence read under it is the snapshot's.
    let store = control.store.read().await;
    let sequence = *control.next_event_id.lock().await;
    let thread = store.threads.get(thread_id)?;
    let cursor = json!({
        "projection": crate::thread_events::digest(&thread.projection()),
        "sequence": sequence,
    });
    Some((thread.stream_snapshot(sequence, params), cursor))
}

/// The reference's error for a thread with no projection, from getThreadProjection and
/// subscribeThread alike.
pub(crate) fn thread_projection_error(thread_id: &Value) -> Value {
    let thread_id = thread_id.as_str().unwrap_or_default();
    json!({
        "_tag":"OrchestrationV2GetThreadProjectionError",
        "threadId":thread_id,
        "message":format!("Failed to load orchestration V2 thread {thread_id}"),
        "cause":{"name":"OrchestratorProjectionError",
            "message":format!("Failed to load orchestration projection for thread {thread_id}."),
            "cause":{"name":"ProjectionStoreThreadNotFoundError",
                "message":format!("No orchestration projection exists for thread {thread_id}.")}}
    })
}

pub(crate) async fn scheduled_tasks_snapshot(control: &ControlState) -> Result<Value, Reply> {
    let tasks = control
        .scheduled_task_list()
        .await
        .map_err(|_| Reply::Failure(json!({"_tag":"Fail","error":{"_tag":"ScheduledTaskError","message":"Could not list schedule tasks."}})))?
        .iter()
        .map(scheduled_public_task)
        .collect::<Vec<_>>();
    Ok(json!({"tasks":tasks,"timeZone":scheduled_task_time_zone()}))
}

pub(crate) fn scheduled_task_time_zone() -> String {
    std::env::var("TZ")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .or_else(|| {
            std::fs::read_to_string("/etc/timezone")
                .ok()
                .map(|value| value.trim().to_owned())
        })
        .unwrap_or_else(|| "UTC".into())
}

pub(crate) fn scheduled_public_task(task: &Value) -> Value {
    let mut task = task.clone();
    if let Some(object) = task.as_object_mut() {
        object.remove("_webhookToken");
        object.remove("_webhookSecret");
        if let Some(signature) = object
            .get_mut("schedule")
            .and_then(|schedule| schedule.get_mut("signature"))
            .and_then(Value::as_object_mut)
        {
            signature.remove("secret");
            signature.remove("secretRef");
        }
    }
    task
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{auth_service::AuthService, event_log::MemoryLog, persistence::MemoryStore};
    use std::sync::Arc;

    fn control() -> (ControlState, Session) {
        let documents = Arc::new(MemoryStore::new());
        let dir = std::path::Path::new("/state");
        let auth = AuthService::with_bootstrap("secret")
            .open_in(documents.clone(), dir)
            .unwrap();
        let (_, token) = auth.browser_session("secret", None).unwrap();
        let principal = auth.principal(&token).unwrap();
        let control =
            ControlState::open(auth, documents, Box::new(MemoryLog::default()), dir).unwrap();
        (control, principal)
    }

    fn thread(id: &str) -> Event {
        Event::ThreadCreated(
            serde_json::from_value(json!({
                "id": id, "project_id": "p1", "title": "One", "cwd": "/work",
                "status": "idle", "messages": [], "runs": [], "created_at": 1,
                "archived": false, "settled": false, "snoozed_until": null,
                "pinned": false, "unread": false, "tags": []
            }))
            .unwrap(),
        )
    }

    fn retitle(id: &str, title: &str) -> Event {
        Event::ThreadPatched {
            thread_id: id.into(),
            patch: json!({"title": title}),
        }
    }

    #[tokio::test]
    async fn a_thread_stream_resumes_from_its_stored_cursor() {
        let (control, principal) = control();
        let method = "orchestration.subscribeThread";
        let params = json!({"threadId":"t1","requestCompletionMarker":true});
        let missing = Subscription::open(&control, &principal, method, &params, false).await;
        assert!(matches!(missing, Err(Reply::Failure(_))));

        control.commit(thread("t1")).await.unwrap();
        let opened = Subscription::open(&control, &principal, method, &params, false)
            .await
            .unwrap();
        assert!(opened.source.is_none());
        assert_eq!(opened.first.len(), 1);
        assert_eq!(opened.first[0][0]["kind"], "snapshot");
        assert_eq!(opened.first[0][1], json!({"kind":"synchronized"}));

        // Stored and read back, as a host that suspends between messages would.
        let stored = serde_json::to_string(&opened.subscription).unwrap();
        let mut resumed: Subscription = serde_json::from_str(&stored).unwrap();
        assert_eq!(resumed, opened.subscription);

        let event = control.commit(retitle("t1", "Two")).await.unwrap();
        let batches = resumed
            .advance(&control, &principal, Signal::Committed(Box::new(event)))
            .await;
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0][0]["projection"]["thread"]["title"], "Two");
        // Nothing changed since, so a resync reports nothing.
        assert!(resumed
            .advance(&control, &principal, Signal::Resync)
            .await
            .is_empty());
        // A change to another thread reports nothing either.
        control.commit(thread("t2")).await.unwrap();
        let event = control.commit(retitle("t2", "Other")).await.unwrap();
        assert!(resumed
            .advance(&control, &principal, Signal::Committed(Box::new(event)))
            .await
            .is_empty());
    }

    #[tokio::test]
    async fn shell_batches_survive_hibernation_and_coalesce_in_sequence_order() {
        use crate::shell_stream::deliver;
        let (control, principal) = control();
        let mut sub = Subscription::open(
            &control,
            &principal,
            "orchestration.subscribeShell",
            &json!({}),
            false,
        )
        .await
        .unwrap()
        .subscription;
        let mut rx = control.shell_events.subscribe();
        control.commit(thread("a")).await.unwrap();
        control.commit(thread("b")).await.unwrap();
        control.commit(retitle("a", "Renamed")).await.unwrap();
        let mut changes = Vec::new();
        while let Ok(change) = rx.try_recv() {
            changes.push(change);
        }
        let last = changes.last().unwrap().sequence;
        let (early, deadline) = deliver(&control, &mut sub, &[Signal::Shell(changes)], 100).await;
        assert!(early.is_empty());
        assert_eq!(deadline, Some(150));
        let mut sub = serde_json::from_value(serde_json::to_value(sub).unwrap()).unwrap();
        assert!(deliver(&control, &mut sub, &[Signal::Poll], 149)
            .await
            .0
            .is_empty());
        let (batches, deadline) = deliver(&control, &mut sub, &[Signal::Poll], 150).await;
        assert_eq!(deadline, None);
        let items = batches[0].as_array().unwrap();
        assert_eq!(items.len(), 2);
        assert_eq!(items[0]["thread"]["id"], "b");
        assert_eq!(items[1]["thread"]["title"], "Renamed");
        assert_eq!(items[1]["sequence"], last);
        let store = control.store.read().await;
        let invalidation = |sequence| crate::shell_stream::Change {
            id: "a".into(),
            project: false,
            deleted: false,
            sequence,
        };
        assert!(crate::shell_stream::project(
            &store,
            &mut sub.cursor,
            &[invalidation(last + 1)],
            5149
        )
        .is_empty());
        assert_eq!(
            crate::shell_stream::project(&store, &mut sub.cursor, &[invalidation(last + 2)], 5150)
                [0][0]["sequence"],
            last + 2
        );
    }

    #[tokio::test]
    async fn auth_access_numbers_each_change_from_its_cursor() {
        let (control, principal) = control();
        let opened = Subscription::open(
            &control,
            &principal,
            "subscribeAuthAccess",
            &json!({}),
            true,
        )
        .await
        .unwrap();
        assert_eq!(opened.first[0][0]["revision"], 1);
        let mut subscription = opened.subscription;
        let mut source = opened.source.unwrap();
        control.auth.socket_presence(&principal.id, true);
        let signal = source.next().await.unwrap();
        let batches = subscription.advance(&control, &principal, signal).await;
        assert_eq!(batches[0][0]["revision"], 2);
        assert_eq!(subscription.cursor, json!(2));
    }

    #[tokio::test]
    async fn prompt_suggestions_follow_their_thread_only() {
        let (control, principal) = control();
        let method = "orchestration.subscribePromptSuggestion";
        let opened = Subscription::open(
            &control,
            &principal,
            method,
            &json!({"threadId":"t1"}),
            false,
        )
        .await
        .unwrap();
        assert_eq!(opened.first, vec![json!([null])]);
        let mut subscription = opened.subscription;
        let other = json!({"threadId":"t2","runId":"r","text":"no"});
        assert!(subscription
            .advance(&control, &principal, Signal::PromptSuggestion(other))
            .await
            .is_empty());
        let mine = json!({"threadId":"t1","runId":"r","text":"yes"});
        assert_eq!(
            subscription
                .advance(&control, &principal, Signal::PromptSuggestion(mine))
                .await,
            vec![json!([{"runId":"r","text":"yes"}])]
        );
    }
}
