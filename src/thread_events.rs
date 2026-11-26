//! Live `orchestration.subscribeThread` events: what one commit changed in a
//! thread's projection, as the reference's domain events (`run.updated`,
//! `turn-item.updated`, `node.updated`, ...).
//!
//! The server keeps no domain event log. Instead each commit that touches a
//! thread diffs the thread's projection before and after it and names each
//! changed record with the event the reference sends for it. The commit
//! takes one event sequence number per domain event (see
//! [`crate::ServerEvent::span`]), so a client applies each in order; a
//! subscriber that is not exactly at the commit's starting point, or a change
//! no event can express (a record that went away), gets a snapshot instead.
use crate::thread_projection::uri_component;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::collections::HashMap;

/// What one commit changed in one thread.
#[derive(Clone, Debug, PartialEq)]
pub struct ThreadEvents {
    pub thread_id: String,
    /// Digests of the thread's projection before and after the commit.
    pub before: String,
    pub after: String,
    /// The stream items (`{kind: "event", sequence, event}`), in order, or
    /// `None` when only a snapshot can carry the change.
    pub items: Option<Vec<Value>>,
}

/// A projection's digest, to tell whether a subscriber has seen it.
/// What a live subscriber has seen of a thread: its revision
/// (`Thread::revision`), which a plane rebuilt from its log reaches again.
pub(crate) fn token(revision: u64) -> String {
    format!("r{revision}")
}

pub fn digest(projection: &Value) -> String {
    // Hashed as it is written: a long thread's projection is tens of MB as text.
    struct Hashing(Sha256);
    impl std::io::Write for Hashing {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            sha2::Digest::update(&mut self.0, bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut hashing = Hashing(Sha256::new());
    crate::wire_json::compact_to(&mut hashing, projection).expect("JSON value");
    hex::encode(&hashing.0.finalize()[..16])
}

/// The record collections that have their own events: the collection, the
/// event for a new record and the event for a changed one.
const COLLECTIONS: &[(&str, &str, &str)] = &[
    ("runs", "run.created", "run.updated"),
    ("attempts", "run-attempt.created", "run-attempt.updated"),
    (
        "checkpointScopes",
        "checkpoint-scope.created",
        "checkpoint-scope.created",
    ),
    ("subagents", "subagent.updated", "subagent.updated"),
    (
        "providerSessions",
        "provider-session.attached",
        "provider-session.updated",
    ),
    (
        "providerThreads",
        "provider-thread.updated",
        "provider-thread.updated",
    ),
    (
        "providerTurns",
        "provider-turn.updated",
        "provider-turn.updated",
    ),
    (
        "runtimeRequests",
        "runtime-request.updated",
        "runtime-request.updated",
    ),
    ("plans", "plan.updated", "plan.updated"),
    ("checkpoints", "checkpoint.captured", "checkpoint.captured"),
    (
        "contextHandoffs",
        "context-handoff.updated",
        "context-handoff.updated",
    ),
    (
        "contextTransfers",
        "context-transfer.created",
        "context-transfer.updated",
    ),
];

/// The event for a change of the thread record, by the field that changed,
/// as the reference's thread commands name them.
fn thread_event(before: &Value, after: &Value) -> &'static str {
    let changed = |key: &str| before[key] != after[key];
    let set = |key: &str| !after[key].is_null();
    if changed("deletedAt") && set("deletedAt") {
        "thread.deleted"
    } else if changed("archivedAt") {
        if set("archivedAt") {
            "thread.archived"
        } else {
            "thread.unarchived"
        }
    } else if changed("settledAt") || changed("settledOverride") {
        if set("settledAt") {
            "thread.settled"
        } else {
            "thread.unsettled"
        }
    } else if changed("snoozedUntil") {
        if set("snoozedUntil") {
            "thread.snoozed"
        } else {
            "thread.unsnoozed"
        }
    } else if changed("pinnedAt") {
        if set("pinnedAt") {
            "thread.pinned"
        } else {
            "thread.unpinned"
        }
    } else if changed("issues") {
        "thread.issue-synced"
    } else if changed("tags") {
        "thread.tags-updated"
    } else if changed("runtimeMode") {
        "thread.runtime-mode-updated"
    } else if changed("interactionMode") {
        "thread.interaction-mode-updated"
    } else if changed("modelSelection") {
        "thread.model-selection-updated"
    } else {
        "thread.metadata-updated"
    }
}

fn by_id(projection: &Value, collection: &str) -> HashMap<String, Value> {
    projection[collection]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|record| Some((record["id"].as_str()?.to_owned(), record.clone())))
        .collect()
}

/// The adapter that opened a runtime request.
fn request_driver(request: &Value) -> Option<&str> {
    if let Some(driver) = request["nativeRequestRef"]["driver"].as_str() {
        return Some(driver);
    }
    // Older projections may identify the provider only in the request id.
    let id = request["id"]
        .as_str()?
        .strip_prefix("runtime-request:provider:")?;
    id.split_once(':').map(|(driver, _)| driver)
}

/// A domain event for `payload`, without its id.
fn event(thread_id: &str, kind: &str, payload: &Value, occurred_at: &str) -> Value {
    let mut event = json!({"threadId": thread_id, "type": kind, "payload": payload});
    let run = if kind.starts_with("run.") {
        payload["id"].as_str()
    } else {
        payload["runId"].as_str()
    };
    if let Some(run) = run {
        event["runId"] = json!(run);
    }
    let node = if kind == "node.updated" {
        payload["id"].as_str()
    } else if kind.starts_with("run.") || kind.starts_with("run-attempt.") {
        payload["rootNodeId"].as_str()
    } else {
        payload["nodeId"].as_str()
    };
    if let Some(node) = node {
        event["nodeId"] = json!(node);
    }
    for key in ["driver", "providerInstanceId"] {
        if let Some(value) = payload[key].as_str() {
            event[key] = json!(value);
        }
    }
    event["occurredAt"] = json!(payload["updatedAt"]
        .as_str()
        .or(payload["completedAt"].as_str())
        .or(payload["createdAt"].as_str())
        .unwrap_or(occurred_at));
    event
}

/// The order a commit's domain events go in: the order the reference's
/// writes of that kind put their records in.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Order {
    /// The thread, the run and its attempt, its root node, then each
    /// changed turn item preceded by its node and message, then provider
    /// records (a turn's progress, as the reference's ingestor writes it).
    Default,
    /// A turn's or run's end: what the provider reported (its session, its
    /// turn, the turn's items), then the run's finalization
    /// (`writeFinalRunEvents`): the attempt, the interrupt result or a
    /// failure no turn reported, the run, its root node and the provider
    /// thread.
    End,
    /// A checkpoint capture (`CheckpointCaptureService`): a provider thread
    /// the run's completion changed, the checkpoints, the checkpoint item,
    /// then the run and its root node.
    Checkpoint,
    /// A run sent (`ThreadMessageIntake`): the thread, the provider thread
    /// the run opens, the run, its pending attempt and root node, the
    /// checkpoint scope, then the user's message and its item.
    Dispatch,
    /// A turn's start, once the CLI reports its thread: the provider
    /// session, the provider thread, then the run, its attempt and root
    /// node, running.
    Start,
    /// A runtime request opening. Codex's adapter writes the card's node
    /// and card, then the request; Claude's permission callback writes the
    /// card's node, the request, then the card, and its adapter then
    /// reports the tool call the request is about again (see
    /// [`Step::RequestedToolCall`]).
    RequestOpen,
    /// A runtime request answered, withdrawn or closed: the request, then
    /// its card and the card's node.
    Answer,
    /// A delegated task's subagent record changed. Starting, the reference
    /// writes its node, the subagent, then its turn item; ending, the
    /// subagent, its parent run (the delivery it claimed), its node and turn
    /// item, then the context transfer.
    Subagent,
    /// A stop closed delegated task cohorts: each run, then its tasks.
    CohortStop,
    /// A provider turn's report: as `Default`, but the turn before the
    /// provider thread it changed (the reference's adapters report the turn,
    /// then the thread it made active or newly marked).
    Turn,
    /// A proposed plan (Claude's ExitPlanMode): its node, the plan, then
    /// its turn item (`emitClaudePlanProjection`).
    Plan,
}

impl Order {
    const PLAN: &'static [Step] = &[
        Step::ItemNodes,
        Step::Collection("plans"),
        Step::Items(Phase::All),
        Step::PlannedToolCall,
        Step::Collection("runs"),
        Step::Collection("attempts"),
        Step::LooseNodes,
        Step::Collection("checkpointScopes"),
        Step::LooseMessages,
        Step::Collection("providerSessions"),
        Step::Collection("providerThreads"),
        Step::Collection("providerTurns"),
        Step::Collection("runtimeRequests"),
        Step::Collection("subagents"),
        Step::Collection("checkpoints"),
        Step::Collection("contextHandoffs"),
        Step::Collection("contextTransfers"),
    ];
    const SUBAGENT_START: &'static [Step] = &[
        Step::ItemNodes,
        Step::Collection("subagents"),
        Step::Items(Phase::All),
        Step::Collection("contextTransfers"),
        Step::Collection("runs"),
        Step::Collection("attempts"),
        Step::LooseNodes,
        Step::Collection("checkpointScopes"),
        Step::LooseMessages,
        Step::Collection("providerSessions"),
        Step::Collection("providerThreads"),
        Step::Collection("providerTurns"),
        Step::Collection("runtimeRequests"),
        Step::Collection("plans"),
        Step::Collection("checkpoints"),
        Step::Collection("contextHandoffs"),
    ];
    const DRAIN: &'static [Step] = &[
        Step::Collection("providerThreads"),
        Step::Items(Phase::All),
        Step::Collection("runs"),
        Step::Collection("attempts"),
        Step::LooseNodes,
        Step::Collection("checkpointScopes"),
        Step::LooseMessages,
        Step::Collection("providerSessions"),
        Step::Collection("providerTurns"),
        Step::Collection("runtimeRequests"),
        Step::Collection("subagents"),
        Step::Collection("plans"),
        Step::Collection("checkpoints"),
        Step::Collection("contextHandoffs"),
        Step::Collection("contextTransfers"),
    ];
    const CLAUDE_REQUEST_OPEN: &'static [Step] = &[
        Step::ItemNodes,
        Step::Collection("runtimeRequests"),
        Step::Items(Phase::All),
        Step::RequestedToolCall,
        Step::Collection("runs"),
        Step::Collection("attempts"),
        Step::LooseNodes,
        Step::Collection("checkpointScopes"),
        Step::LooseMessages,
        Step::Collection("providerSessions"),
        Step::Collection("providerThreads"),
        Step::Collection("providerTurns"),
        Step::Collection("subagents"),
        Step::Collection("plans"),
        Step::Collection("checkpoints"),
        Step::Collection("contextHandoffs"),
        Step::Collection("contextTransfers"),
    ];
    /// An ACP request: its node, the request, then its item; the tool call
    /// it is about is not reported again.
    const ACP_REQUEST_OPEN: &'static [Step] = &[
        Step::ItemNodes,
        Step::Collection("runtimeRequests"),
        Step::Items(Phase::All),
        Step::Collection("runs"),
        Step::Collection("attempts"),
        Step::LooseNodes,
        Step::Collection("checkpointScopes"),
        Step::LooseMessages,
        Step::Collection("providerSessions"),
        Step::Collection("providerThreads"),
        Step::Collection("providerTurns"),
        Step::Collection("subagents"),
        Step::Collection("plans"),
        Step::Collection("checkpoints"),
        Step::Collection("contextHandoffs"),
        Step::Collection("contextTransfers"),
    ];
    const SESSION_OPEN_FAILURE: &'static [Step] = &[
        Step::Items(Phase::All),
        Step::Collection("runs"),
        Step::Collection("attempts"),
        Step::LooseNodes,
        Step::Collection("providerSessions"),
        Step::Collection("providerThreads"),
        Step::Collection("providerTurns"),
        Step::Collection("checkpointScopes"),
        Step::LooseMessages,
        Step::Collection("runtimeRequests"),
        Step::Collection("subagents"),
        Step::Collection("plans"),
        Step::Collection("checkpoints"),
        Step::Collection("contextHandoffs"),
        Step::Collection("contextTransfers"),
    ];

    /// The order of the events of a commit of `event`.
    pub fn of(event: &crate::Event) -> Self {
        use crate::workspace::provider::ProviderEvent;
        match event {
            crate::Event::RunProvider {
                event: ProviderEvent::TurnTerminal { .. },
                ..
            }
            | crate::Event::RunFinished { .. } => Order::End,
            crate::Event::CheckpointCaptured { .. } => Order::Checkpoint,
            crate::Event::RunStarted(_) | crate::Event::RunQueued { .. } => Order::Dispatch,
            crate::Event::ThreadPatched { patch, .. } if patch["cohortStopped"] == true => {
                Order::CohortStop
            }
            crate::Event::ThreadPatched { patch, .. }
                if patch["row"].get("subagents").is_some() =>
            {
                Order::Subagent
            }
            crate::Event::RunProvider {
                event: ProviderEvent::RuntimeRequestOpened { .. },
                ..
            } => Order::RequestOpen,
            crate::Event::RunProvider {
                event:
                    ProviderEvent::RuntimeRequestAnswered { .. }
                    | ProviderEvent::RuntimeRequest { .. }
                    | ProviderEvent::RuntimeRequestClosed { .. }
                    | ProviderEvent::RuntimeRequestDeliveryFailed { .. },
                ..
            } => Order::Answer,
            crate::Event::RunProvider {
                event: ProviderEvent::ProviderThread { .. },
                ..
            } => Order::Start,
            crate::Event::RunProvider {
                event: ProviderEvent::TurnItem { item },
                ..
            } if item.kind == "proposed_plan" => Order::Plan,
            crate::Event::RunProvider {
                event: ProviderEvent::ProviderTurn { .. },
                ..
            } => Order::Turn,
            _ => Order::Default,
        }
    }

    fn steps(self) -> &'static [Step] {
        use Step::*;
        match self {
            Order::Default => &[
                Collection("runs"),
                Collection("attempts"),
                LooseNodes,
                Collection("checkpointScopes"),
                LooseMessages,
                Items(Phase::All),
                Collection("providerSessions"),
                Collection("providerThreads"),
                Collection("providerTurns"),
                Collection("runtimeRequests"),
                Collection("subagents"),
                Collection("plans"),
                Collection("checkpoints"),
                Collection("contextHandoffs"),
                Collection("contextTransfers"),
            ],
            Order::Turn => &[
                Collection("runs"),
                Collection("attempts"),
                LooseNodes,
                Collection("checkpointScopes"),
                LooseMessages,
                Items(Phase::All),
                Collection("providerSessions"),
                Collection("providerTurns"),
                Collection("providerThreads"),
                Collection("runtimeRequests"),
                Collection("subagents"),
                Collection("plans"),
                Collection("checkpoints"),
                Collection("contextHandoffs"),
                Collection("contextTransfers"),
            ],
            Order::End => &[
                Collection("providerSessions"),
                Collection("providerTurns"),
                ReportedProviderThread,
                Items(Phase::Reported),
                Collection("attempts"),
                Items(Phase::Finalized),
                Collection("runs"),
                LooseNodes,
                Collection("checkpointScopes"),
                LooseMessages,
                Collection("providerThreads"),
                Collection("runtimeRequests"),
                Collection("subagents"),
                Collection("plans"),
                Collection("checkpoints"),
                Collection("contextHandoffs"),
                Collection("contextTransfers"),
            ],
            Order::Dispatch => &[
                Collection("providerThreads"),
                Collection("runs"),
                Collection("attempts"),
                LooseNodes,
                Collection("checkpointScopes"),
                LooseMessages,
                Items(Phase::All),
                Collection("providerSessions"),
                Collection("providerTurns"),
                Collection("runtimeRequests"),
                Collection("subagents"),
                Collection("plans"),
                Collection("checkpoints"),
                Collection("contextHandoffs"),
                Collection("contextTransfers"),
            ],
            Order::Start => &[
                Collection("providerSessions"),
                Collection("providerThreads"),
                Collection("runs"),
                Collection("attempts"),
                LooseNodes,
                Collection("checkpointScopes"),
                LooseMessages,
                Items(Phase::All),
                Collection("providerTurns"),
                Collection("runtimeRequests"),
                Collection("subagents"),
                Collection("plans"),
                Collection("checkpoints"),
                Collection("contextHandoffs"),
                Collection("contextTransfers"),
            ],
            // Codex's order; a Claude request's is `CLAUDE_REQUEST_OPEN`.
            Order::RequestOpen => &[
                Collection("runs"),
                Collection("attempts"),
                LooseNodes,
                Collection("checkpointScopes"),
                LooseMessages,
                Items(Phase::All),
                Collection("runtimeRequests"),
                Collection("providerSessions"),
                Collection("providerThreads"),
                Collection("providerTurns"),
                Collection("subagents"),
                Collection("plans"),
                Collection("checkpoints"),
                Collection("contextHandoffs"),
                Collection("contextTransfers"),
            ],
            Order::Answer => &[
                Collection("runtimeRequests"),
                Items(Phase::All),
                Collection("runs"),
                Collection("attempts"),
                LooseNodes,
                Collection("checkpointScopes"),
                LooseMessages,
                Collection("providerSessions"),
                Collection("providerThreads"),
                Collection("providerTurns"),
                Collection("subagents"),
                Collection("plans"),
                Collection("checkpoints"),
                Collection("contextHandoffs"),
                Collection("contextTransfers"),
            ],
            Order::Plan => Self::PLAN,
            Order::CohortStop => &[
                Collection("runs"),
                Collection("subagents"),
                Collection("attempts"),
                LooseNodes,
                Collection("checkpointScopes"),
                LooseMessages,
                Items(Phase::All),
                Collection("providerSessions"),
                Collection("providerThreads"),
                Collection("providerTurns"),
                Collection("runtimeRequests"),
                Collection("plans"),
                Collection("checkpoints"),
                Collection("contextHandoffs"),
                Collection("contextTransfers"),
            ],
            // Its ending order; a subagent starting takes `SUBAGENT_START`.
            Order::Subagent => &[
                Collection("subagents"),
                Collection("runs"),
                ItemNodes,
                Items(Phase::All),
                Collection("contextTransfers"),
                Collection("attempts"),
                LooseNodes,
                Collection("checkpointScopes"),
                LooseMessages,
                Collection("providerSessions"),
                Collection("providerThreads"),
                Collection("providerTurns"),
                Collection("runtimeRequests"),
                Collection("plans"),
                Collection("checkpoints"),
                Collection("contextHandoffs"),
            ],
            // A provider thread the run's completion changes comes first:
            // the reference's run stores it before its checkpoint.
            Order::Checkpoint => &[
                Collection("providerThreads"),
                Collection("checkpoints"),
                Items(Phase::All),
                Collection("runs"),
                Collection("attempts"),
                LooseNodes,
                Collection("checkpointScopes"),
                LooseMessages,
                Collection("providerSessions"),
                Collection("providerTurns"),
                Collection("runtimeRequests"),
                Collection("subagents"),
                Collection("plans"),
                Collection("contextHandoffs"),
                Collection("contextTransfers"),
            ],
        }
    }
}

#[derive(Clone, Copy)]
enum Step {
    Collection(&'static str),
    /// Nodes that are not a changed turn item's (root nodes).
    LooseNodes,
    /// Messages that are not a changed turn item's.
    LooseMessages,
    /// Changed turn items, each preceded by its node and message.
    Items(Phase),
    /// A Claude provider thread at its turn's end, as Claude's adapter
    /// reports it with the turn's end, before the run's finalization writes
    /// it again.
    ReportedProviderThread,
    /// The nodes of the changed turn items, ahead of the items.
    ItemNodes,
    /// The tool call a Claude runtime request is about, as the reference's
    /// adapter reports it again once the stream it held back while the
    /// permission callback ran comes through, changed or not.
    RequestedToolCall,
    /// The tool call a proposed plan came from (Claude's ExitPlanMode),
    /// reported again after its plan, as the reference's adapter reports
    /// the denied call once the plan it held is out.
    PlannedToolCall,
}

/// Which of a commit's changed turn items a step takes.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Phase {
    All,
    /// What the provider reported, before the run's finalization.
    Reported,
    /// What the finalization adds: the interrupt result, and the error of a
    /// run that ended without its provider reporting the turn's end.
    Finalized,
}

/// The domain events that take `before` to `after`, in `order`. `None`
/// when a record went away, which no event expresses.
#[cfg(test)]
pub fn diff(
    thread_id: &str,
    before: &Value,
    after: &Value,
    occurred_at: &str,
    order: Order,
) -> Option<Vec<Value>> {
    diff_with_command(
        thread_id,
        before,
        after,
        occurred_at,
        false,
        None,
        order,
        None,
    )
}

/// [`diff`] with the orchestration command that produced the projection.
/// Pull-request and issue commands have one specialized event in the reference
/// rather than the generic thread metadata and link events.
pub fn diff_for_command(
    thread_id: &str,
    before: &Value,
    after: &Value,
    occurred_at: &str,
    order: Order,
    command_type: Option<&str>,
) -> Option<Vec<Value>> {
    diff_with_command(
        thread_id,
        before,
        after,
        occurred_at,
        false,
        None,
        order,
        command_type,
    )
}

/// [`diff`] for a commit that detached the thread's provider session for
/// `reason`: a provider session that went away is
/// `provider-session.detached`, as the reference names it.
pub fn diff_detaching(
    thread_id: &str,
    before: &Value,
    after: &Value,
    occurred_at: &str,
    reason: &str,
    order: Order,
) -> Option<Vec<Value>> {
    diff_with_command(
        thread_id,
        before,
        after,
        occurred_at,
        false,
        Some(reason),
        order,
        None,
    )
}

/// [`diff_all`] with the orchestration command that produced the projection.
pub fn diff_all_for_command(
    thread_id: &str,
    before: &Value,
    after: &Value,
    occurred_at: &str,
    detached: Option<&str>,
    order: Order,
    command_type: Option<&str>,
) -> Vec<Value> {
    diff_with_command(
        thread_id,
        before,
        after,
        occurred_at,
        true,
        detached,
        order,
        command_type,
    )
    .unwrap_or_default()
}

#[allow(clippy::too_many_arguments)]
fn diff_with_command(
    thread_id: &str,
    before: &Value,
    after: &Value,
    occurred_at: &str,
    allow_removals: bool,
    detached: Option<&str>,
    order: Order,
    command_type: Option<&str>,
) -> Option<Vec<Value>> {
    let mut out = Vec::new();
    // Every event moves its thread's updatedAt on; the reference emits no thread event for that
    // alone (the run's own events carry it).
    let without_updated_at = |thread: &Value| {
        let mut thread = thread.clone();
        if let Some(fields) = thread.as_object_mut() {
            fields.remove("updatedAt");
        }
        thread
    };
    if after["thread"].is_object()
        && without_updated_at(&before["thread"]) != without_updated_at(&after["thread"])
    {
        let special = command_type.and_then(|kind| match kind {
            "thread.pull-request.link"
            | "thread.pull-request.unlink"
            | "thread.pull-request.watch"
            | "thread.pull-request.sync"
            | "thread.pull-request-watch.checks-reopen" => {
                Some(("thread.pull-request-synced", after["thread"].clone()))
            }
            "thread.issue.link" | "thread.issue.unlink" => {
                Some(("thread.issue-synced", after["thread"].clone()))
            }
            "thread.pull-request-link.sync" | "thread.pull-request-watch.sync" => after["thread"]
                ["pullRequests"]
                .as_array()
                .into_iter()
                .flatten()
                .find(|link| {
                    before["thread"]["pullRequests"]
                        .as_array()
                        .into_iter()
                        .flatten()
                        .any(|old| crate::thread_projection::same_link(old, link, false))
                })
                .cloned()
                .map(|link| ("thread.pull-request-link-updated", link)),
            _ => None,
        });
        let has_special = special.is_some();
        if let Some((kind, payload)) = special {
            out.push(event(thread_id, kind, &payload, occurred_at));
        } else {
            out.push(event(
                thread_id,
                thread_event(&before["thread"], &after["thread"]),
                &after["thread"],
                occurred_at,
            ));
        }
        if !has_special && before["thread"]["pullRequests"] != after["thread"]["pullRequests"] {
            out.push(event(
                thread_id,
                "thread.pull-request-synced",
                &after["thread"],
                occurred_at,
            ));
            let before_links = before["thread"]["pullRequests"].as_array();
            if let Some(link) = after["thread"]["pullRequests"]
                .as_array()
                .into_iter()
                .flatten()
                .find(|link| {
                    !before_links
                        .into_iter()
                        .flatten()
                        .any(|old| crate::thread_projection::same_link(old, link, false))
                })
            {
                out.push(event(
                    thread_id,
                    "thread.pull-request-link-updated",
                    link,
                    occurred_at,
                ));
            }
        }
    }
    // A detached session leaves the thread's records.
    if let Some(reason) = detached {
        let remaining = by_id(after, "providerSessions");
        for session in before["providerSessions"].as_array().into_iter().flatten() {
            let Some(id) = session["id"]
                .as_str()
                .filter(|id| !remaining.contains_key(*id))
            else {
                continue;
            };
            let mut detach = event(
                thread_id,
                "provider-session.detached",
                &json!({"providerSessionId": id, "detachedAt": occurred_at, "reason": reason}),
                occurred_at,
            );
            for key in ["driver", "providerInstanceId"] {
                if let Some(value) = session[key].as_str() {
                    detach[key] = json!(value);
                }
            }
            out.push(detach);
        }
    }
    let changed = |collection: &str| -> Option<Vec<(bool, Value)>> {
        let old = by_id(before, collection);
        let new = by_id(after, collection);
        // A detached session's removal went out as its own event above.
        let removable = allow_removals || (detached.is_some() && collection == "providerSessions");
        if !removable && old.keys().any(|id| !new.contains_key(id)) {
            return None;
        }
        Some(
            after[collection]
                .as_array()
                .into_iter()
                .flatten()
                .filter(|record| {
                    record["id"]
                        .as_str()
                        .is_some_and(|id| old.get(id) != Some(record))
                })
                .map(|record| {
                    let created = record["id"]
                        .as_str()
                        .is_some_and(|id| !old.contains_key(id));
                    (created, record.clone())
                })
                .collect(),
        )
    };
    let mut collections: HashMap<&str, Vec<(bool, Value)>> = COLLECTIONS
        .iter()
        .map(|(name, _, _)| *name)
        .chain(["nodes", "messages", "turnItems"])
        .map(|name| changed(name).map(|records| (name, records)))
        .collect::<Option<_>>()?;
    // A turn starting on a session already open reports that session again
    // with its running transition, as the reference's turn start does.
    if order == Order::Start && collections["providerSessions"].is_empty() {
        let sessions = by_id(after, "providerSessions");
        let threads = by_id(after, "providerThreads");
        let started = collections["runs"]
            .iter()
            .filter(|(_, run)| run["status"] == "running")
            .filter_map(|(_, run)| threads.get(run["providerThreadId"].as_str()?))
            .filter_map(|thread| sessions.get(thread["providerSessionId"].as_str()?))
            .map(|session| (false, session.clone()))
            .next();
        collections
            .get_mut("providerSessions")
            .unwrap()
            .extend(started);
    }
    // A native subagent's record goes with its item (`emitSubagentTaskUpdate`), and a child's
    // provider thread is reported on the child's own thread, not here.
    let native_subagent =
        |item: &Value| item["type"] == "subagent" && item["origin"] == "provider_native";
    let item_subagents: std::collections::HashSet<String> = collections["turnItems"]
        .iter()
        .filter(|(_, item)| native_subagent(item))
        .filter_map(|(_, item)| item["subagentId"].as_str().map(str::to_owned))
        .collect();
    let old_nodes = by_id(before, "nodes");
    let push_collection = |out: &mut Vec<Value>, name: &str| {
        let (_, created, updated) = COLLECTIONS
            .iter()
            .find(|(collection, _, _)| *collection == name)
            .copied()
            .unwrap_or((name, "", ""));
        for (is_new, record) in &collections[name] {
            if name == "subagents"
                && record["id"]
                    .as_str()
                    .is_some_and(|id| item_subagents.contains(id))
            {
                continue;
            }
            if name == "providerThreads"
                && record["appThreadId"]
                    .as_str()
                    .is_some_and(|t| t != thread_id)
            {
                continue;
            }
            out.push(event(
                thread_id,
                if *is_new { created } else { updated },
                record,
                occurred_at,
            ));
            // The reference attaches a session, then reports it ready.
            if *is_new && name == "providerSessions" {
                out.push(event(thread_id, updated, record, occurred_at));
            }
        }
    };
    let mut nodes: Vec<&Value> = collections["nodes"].iter().map(|(_, r)| r).collect();
    let mut messages: Vec<&Value> = collections["messages"].iter().map(|(_, r)| r).collect();
    let mut items: Vec<&Value> = collections["turnItems"].iter().map(|(_, r)| r).collect();
    items.sort_by_key(|item| item["ordinal"].as_u64().unwrap_or(0));
    // A run's own items (its error, its interrupt) sit on its root node,
    // which goes with the run instead.
    let is_root = |id: &str| {
        nodes
            .iter()
            .any(|node| node["id"] == id && node["kind"] == "root_turn")
    };
    let item_nodes: std::collections::HashSet<&str> = items
        .iter()
        .filter_map(|item| item["nodeId"].as_str())
        .filter(|id| !is_root(id))
        .collect();
    let item_messages: std::collections::HashSet<&str> = items
        .iter()
        .filter_map(|item| item["messageId"].as_str())
        .collect();
    // The reference writes a turn's error with the turn's end when the
    // provider reports it, and with the run's end otherwise: when no turn
    // end was reported, or the provider's event stream closed.
    let turn_reported = !collections["providerTurns"].is_empty();
    let finalized = |item: &Value| {
        item["type"] == "run_interrupt_result"
            || (item["type"] == "error"
                && (!turn_reported
                    || item["failure"]["message"]
                        .as_str()
                        .is_some_and(crate::workspace::provider::stream_closed)))
    };
    // A session that failed to open (`ProviderSessionManager`) writes its
    // error, then the run, its attempt and root node.
    let session_failed = order == Order::End
        && items.iter().any(|item| {
            item["id"]
                .as_str()
                .is_some_and(|id| id.ends_with(":signal:provider-session-open-failure"))
        });
    let subagent_start =
        order == Order::Subagent && collections["subagents"].iter().any(|(created, _)| *created);
    // A queued run leaving the queue: its provider thread, the input it
    // carries, then the run, as the reference's queued dispatch writes them.
    let drain = order == Order::Dispatch && {
        let old = by_id(before, "runs");
        !collections["runs"].is_empty()
            && collections["runs"].iter().all(|(created, run)| {
                !created
                    && run["id"]
                        .as_str()
                        .and_then(|id| old.get(id))
                        .is_some_and(|old| old["status"] == "queued")
            })
    };
    let steps = if session_failed {
        Order::SESSION_OPEN_FAILURE
    } else if drain {
        Order::DRAIN
    } else if subagent_start {
        Order::SUBAGENT_START
    } else if order == Order::RequestOpen {
        match collections["runtimeRequests"]
            .iter()
            .find_map(|(_, request)| request_driver(request))
        {
            Some(crate::workspace::provider::CLAUDE_AGENT) => Order::CLAUDE_REQUEST_OPEN,
            Some(driver) if crate::workspace::provider::is_acp(driver) => Order::ACP_REQUEST_OPEN,
            _ => order.steps(),
        }
    } else {
        order.steps()
    };
    for step in steps {
        match *step {
            Step::Collection(name) => push_collection(&mut out, name),
            Step::ReportedProviderThread => {
                // A turn whose event stream closed reported nothing more.
                let closed = items.iter().any(|item| {
                    item["failure"]["message"]
                        .as_str()
                        .is_some_and(crate::workspace::provider::stream_closed)
                });
                if turn_reported && !closed {
                    for (_, record) in &collections["providerThreads"] {
                        // Claude's and ACP adapters report their thread with
                        // the turn's end; Codex's shared session does not.
                        if record["driver"] != crate::workspace::provider::CODEX {
                            let mut reported =
                                event(thread_id, "provider-thread.updated", record, occurred_at);
                            // ACP reports the active thread with the turn's end. The
                            // orchestrator publishes the idle thread after the run.
                            if record["status"] == "idle"
                                && record["driver"]
                                    .as_str()
                                    .is_some_and(crate::workspace::provider::is_acp)
                            {
                                reported["payload"]["status"] = json!("active");
                            }
                            out.push(reported);
                        }
                    }
                }
            }
            Step::ItemNodes => {
                for item in &items {
                    if let Some(index) = nodes.iter().position(|node| {
                        node["id"] == item["nodeId"] && node["kind"] != "root_turn"
                    }) {
                        out.push(event(
                            thread_id,
                            "node.updated",
                            nodes.remove(index),
                            occurred_at,
                        ));
                    }
                }
            }
            Step::PlannedToolCall => {
                for item in items.iter().filter(|item| item["type"] == "proposed_plan") {
                    let native = &item["nativeItemRef"]["nativeId"];
                    let tool = after["turnItems"]
                        .as_array()
                        .into_iter()
                        .flatten()
                        .find(|tool| {
                            !native.is_null()
                                && tool["nativeItemRef"]["nativeId"] == *native
                                && tool["type"] != "proposed_plan"
                        });
                    if let Some(tool) = tool {
                        out.push(event(thread_id, "turn-item.updated", tool, occurred_at));
                    }
                }
            }
            Step::RequestedToolCall => {
                // A question's tool call is held for its echo, not reported
                // again.
                for (_, request) in collections["runtimeRequests"]
                    .iter()
                    .filter(|(_, request)| request["kind"] != "user_input")
                {
                    let native = &request["nativeRequestRef"]["nativeId"];
                    let tool = after["turnItems"]
                        .as_array()
                        .into_iter()
                        .flatten()
                        .find(|item| {
                            !native.is_null()
                                && item["nativeItemRef"]["nativeId"] == *native
                                && !matches!(
                                    item["type"].as_str(),
                                    Some("approval_request" | "user_input_request")
                                )
                        });
                    if let Some(tool) = tool {
                        out.push(event(thread_id, "turn-item.updated", tool, occurred_at));
                    }
                }
            }
            Step::LooseNodes => {
                for node in nodes.iter().filter(|node| {
                    node["id"]
                        .as_str()
                        .is_none_or(|id| !item_nodes.contains(id))
                }) {
                    out.push(event(thread_id, "node.updated", node, occurred_at));
                }
            }
            Step::LooseMessages => {
                for message in messages.iter().filter(|m| {
                    m["id"]
                        .as_str()
                        .is_none_or(|id| !item_messages.contains(id))
                }) {
                    out.push(event(thread_id, "message.updated", message, occurred_at));
                }
            }
            Step::Items(phase) => {
                for item in items.iter().filter(|item| match phase {
                    Phase::All => true,
                    Phase::Reported => !finalized(item),
                    Phase::Finalized => finalized(item),
                }) {
                    // Each adapter writes an item's records in its own order:
                    // Pi's projection writes the turn item before its message,
                    // Muse's the message before the node; the others the
                    // node, the message, then the item.
                    let driver = item["nativeItemRef"]["driver"].as_str();
                    if driver == Some(crate::workspace::provider::MUSE) {
                        if let Some(message) = take_message(&mut messages, &item["messageId"]) {
                            out.push(event(thread_id, "message.updated", message, occurred_at));
                        }
                    }
                    if let Some(index) = nodes.iter().position(|node| {
                        node["id"] == item["nodeId"] && node["kind"] != "root_turn"
                    }) {
                        let node = nodes.remove(index);
                        // A native subagent's node is reported when it starts; its record
                        // carries what changes after.
                        let reported = native_subagent(item)
                            && node["id"]
                                .as_str()
                                .is_some_and(|id| old_nodes.contains_key(id));
                        if !reported {
                            out.push(event(thread_id, "node.updated", node, occurred_at));
                        }
                    }
                    if native_subagent(item) {
                        if let Some((_, record)) = collections["subagents"]
                            .iter()
                            .find(|(_, record)| record["id"] == item["subagentId"])
                        {
                            out.push(event(thread_id, "subagent.updated", record, occurred_at));
                        }
                    }
                    if driver == Some(crate::workspace::provider::PI) {
                        out.push(event(thread_id, "turn-item.updated", item, occurred_at));
                        if let Some(message) = take_message(&mut messages, &item["messageId"]) {
                            out.push(event(thread_id, "message.updated", message, occurred_at));
                        }
                        continue;
                    }
                    if let Some(message) = take_message(&mut messages, &item["messageId"]) {
                        out.push(event(thread_id, "message.updated", message, occurred_at));
                    }
                    out.push(event(thread_id, "turn-item.updated", item, occurred_at));
                }
            }
        }
    }
    Some(out)
}

/// The message `id` names, taken out of `messages`.
fn take_message<'a>(messages: &mut Vec<&'a Value>, id: &Value) -> Option<&'a Value> {
    let index = messages.iter().position(|m| &m["id"] == id)?;
    Some(messages.remove(index))
}

/// The stream items for `events`, numbered so the last one is `last`.
pub fn sequence(thread_id: &str, events: Vec<Value>, last: u64) -> Vec<Value> {
    let first = last + 1 - events.len() as u64;
    events
        .into_iter()
        .enumerate()
        .map(|(index, mut event)| {
            let sequence = first + index as u64;
            let id = format!(
                "event:thread:{}:sequence:{sequence}",
                uri_component(thread_id)
            );
            if let Some(object) = event.as_object_mut() {
                let mut ordered = serde_json::Map::new();
                ordered.insert("id".into(), json!(id));
                ordered.extend(std::mem::take(object));
                *object = ordered;
            }
            json!({"kind": "event", "sequence": sequence, "event": event})
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_provider_turn_report_comes_before_the_thread_it_changed() {
        let position = |order: Order, name: &str| {
            order
                .steps()
                .iter()
                .position(|step| matches!(step, Step::Collection(c) if *c == name))
                .unwrap()
        };
        assert!(position(Order::Turn, "providerTurns") < position(Order::Turn, "providerThreads"));
        // Anything else keeps the thread first.
        assert!(
            position(Order::Default, "providerThreads") < position(Order::Default, "providerTurns")
        );
    }

    #[test]
    fn each_adapter_writes_an_items_records_in_its_own_order() {
        let types = |driver: &str| {
            let before = json!({
                "thread": {"id": "t"},
                "runs": [], "attempts": [], "nodes": [], "messages": [], "turnItems": []
            });
            let after = json!({
                "thread": {"id": "t"},
                "runs": [], "attempts": [],
                "nodes": [{"id": "n:1", "runId": "r"}],
                "messages": [{"id": "m", "runId": "r", "nodeId": "n:1"}],
                "turnItems": [{"id": "i", "runId": "r", "nodeId": "n:1", "messageId": "m",
                    "type": "assistant_message", "nativeItemRef": {"driver": driver}}]
            });
            diff("t", &before, &after, "x", Order::Default)
                .unwrap()
                .iter()
                .map(|e| e["type"].as_str().unwrap().to_owned())
                .collect::<Vec<_>>()
        };
        assert_eq!(
            types("grok"),
            ["node.updated", "message.updated", "turn-item.updated"]
        );
        assert_eq!(
            types("pi"),
            ["node.updated", "turn-item.updated", "message.updated"]
        );
        assert_eq!(
            types("muse"),
            ["message.updated", "node.updated", "turn-item.updated"]
        );
    }

    #[test]
    fn a_commit_becomes_the_events_of_what_it_changed() {
        let before = json!({
            "thread": {"id": "t", "title": "A"},
            "runs": [], "attempts": [], "nodes": [], "messages": [], "turnItems": []
        });
        let after = json!({
            "thread": {"id": "t", "title": "B"},
            "runs": [{"id": "r", "rootNodeId": "n:root", "status": "running"}],
            "attempts": [],
            "nodes": [{"id": "n:root", "runId": "r"}, {"id": "n:1", "runId": "r"}],
            "messages": [{"id": "m", "runId": "r", "nodeId": "n:1"}],
            "turnItems": [{"id": "i", "runId": "r", "nodeId": "n:1", "messageId": "m",
                "type": "assistant_message", "ordinal": 2, "updatedAt": "2026-01-01T00:00:00.000Z"}]
        });
        let events = diff(
            "t",
            &before,
            &after,
            "2026-01-02T00:00:00.000Z",
            Order::Default,
        )
        .unwrap();
        let types: Vec<&str> = events.iter().map(|e| e["type"].as_str().unwrap()).collect();
        assert_eq!(
            types,
            [
                "thread.metadata-updated",
                "run.created",
                "node.updated",
                "node.updated",
                "message.updated",
                "turn-item.updated"
            ]
        );
        assert_eq!(events[1]["runId"], "r");
        assert_eq!(events[1]["nodeId"], "n:root");
        assert_eq!(events[5]["occurredAt"], "2026-01-01T00:00:00.000Z");
        let items = sequence("t", events, 10);
        assert_eq!(items[0]["sequence"], 5);
        assert_eq!(items[5]["sequence"], 10);
        assert_eq!(items[5]["event"]["id"], "event:thread:t:sequence:10");
        // A record that went away needs a snapshot.
        assert!(diff("t", &after, &before, "x", Order::Default).is_none());
        // Nothing changed, nothing to send.
        assert!(diff("t", &after, &after, "x", Order::Default)
            .unwrap()
            .is_empty());
    }

    #[test]
    fn a_detached_session_is_its_own_event() {
        let before = json!({"thread": {"id": "t"}, "providerSessions": [
            {"id": "s", "driver": "codex", "providerInstanceId": "codex"}]});
        let after = json!({"thread": {"id": "t"}, "providerSessions": []});
        assert!(diff("t", &before, &after, "x", Order::Default).is_none());
        let events = diff_detaching(
            "t",
            &before,
            &after,
            "2026-01-01T00:00:00.000Z",
            "Thread archived.",
            Order::Default,
        )
        .unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0]["type"], "provider-session.detached");
        assert_eq!(
            events[0]["payload"],
            json!({"providerSessionId": "s",
            "detachedAt": "2026-01-01T00:00:00.000Z", "reason": "Thread archived."})
        );
        assert_eq!(events[0]["driver"], "codex");
    }

    #[test]
    fn thread_changes_are_named_by_the_field_that_changed() {
        let base = json!({"archivedAt": null, "runtimeMode": "full-access"});
        let archived = json!({"archivedAt": "x", "runtimeMode": "full-access"});
        assert_eq!(thread_event(&base, &archived), "thread.archived");
        assert_eq!(thread_event(&archived, &base), "thread.unarchived");
        let mode = json!({"archivedAt": null, "runtimeMode": "approval-required"});
        assert_eq!(thread_event(&base, &mode), "thread.runtime-mode-updated");
    }

    #[test]
    fn link_commands_emit_the_reference_sync_event_once() {
        let before = json!({
            "thread": {"id": "t", "pullRequests": [], "issues": []},
            "runs": [], "attempts": [], "nodes": [], "messages": [], "turnItems": []
        });
        let after = json!({
            "thread": {"id": "t", "pullRequests": [{
                "host": "github.com", "repository": "example/repo", "number": 3,
                "url": "https://github.com/example/repo/pull/3"
            }], "issues": []},
            "runs": [], "attempts": [], "nodes": [], "messages": [], "turnItems": []
        });
        let events = diff_for_command(
            "t",
            &before,
            &after,
            "2026-01-02T00:00:00.000Z",
            Order::Default,
            Some("thread.pull-request.link"),
        )
        .unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0]["type"], "thread.pull-request-synced");
        assert_eq!(events[0]["payload"], after["thread"]);
    }

    fn types(events: &[Value]) -> Vec<String> {
        events
            .iter()
            .map(|e| {
                let kind = e["type"].as_str().unwrap();
                match e["payload"]["type"].as_str() {
                    Some(item) if kind == "turn-item.updated" => format!("{kind} {item}"),
                    _ => kind.to_owned(),
                }
            })
            .collect()
    }

    #[test]
    fn a_turns_end_comes_as_the_reference_writes_it() {
        let empty = json!({
            "thread": {"id": "t"}, "runs": [], "attempts": [], "nodes": [], "messages": [],
            "turnItems": [], "providerTurns": [], "providerThreads": []
        });
        let before = json!({
            "thread": {"id": "t"},
            "runs": [{"id": "r", "rootNodeId": "n:root", "status": "running"}],
            "attempts": [{"id": "a", "runId": "r", "rootNodeId": "n:root", "status": "running"}],
            "nodes": [{"id": "n:root", "kind": "root_turn", "runId": "r", "status": "running"},
                      {"id": "n:cmd", "kind": "tool_call", "runId": "r", "status": "running"}],
            "messages": [],
            "turnItems": [{"id": "i:cmd", "type": "command_execution", "nodeId": "n:cmd", "runId": "r",
                           "status": "running", "ordinal": 2}],
            "providerTurns": [{"id": "pt", "runId": "r", "status": "running"}],
            "providerThreads": [{"id": "pth", "driver": "codex", "status": "active"}]
        });
        let mut after = before.clone();
        after["runs"][0]["status"] = json!("interrupted");
        after["attempts"][0]["status"] = json!("interrupted");
        after["nodes"][0]["status"] = json!("interrupted");
        after["nodes"][1]["status"] = json!("interrupted");
        after["turnItems"][0]["status"] = json!("interrupted");
        after["turnItems"].as_array_mut().unwrap().push(
            json!({"id": "i:result", "type": "run_interrupt_result", "nodeId": "n:root",
                         "runId": "r", "status": "interrupted", "ordinal": 3}),
        );
        after["providerTurns"][0]["status"] = json!("interrupted");
        after["providerThreads"][0]["status"] = json!("idle");
        let events = diff("t", &before, &after, "x", Order::End).unwrap();
        // The provider's report (its turn, the items it ends), then the
        // run's finalization: attempt, interrupt result, run, root node,
        // provider thread.
        assert_eq!(
            types(&events),
            [
                "provider-turn.updated",
                "node.updated",
                "turn-item.updated command_execution",
                "run-attempt.updated",
                "turn-item.updated run_interrupt_result",
                "run.updated",
                "node.updated",
                "provider-thread.updated"
            ]
        );
        assert_eq!(events[6]["payload"]["id"], "n:root");
        // A Claude turn's end also carries the thread its adapter reports.
        let mut claude = after.clone();
        claude["providerThreads"][0]["driver"] = json!("claudeAgent");
        let mut claude_before = before.clone();
        claude_before["providerThreads"][0]["driver"] = json!("claudeAgent");
        let events = diff("t", &claude_before, &claude, "x", Order::End).unwrap();
        assert_eq!(types(&events)[1], "provider-thread.updated");
        assert_eq!(types(&events).last().unwrap(), "provider-thread.updated");
        // A run that ended without its turn reporting the end: its error
        // comes with the finalization.
        let mut failed = empty.clone();
        failed["runs"] = json!([{"id": "r", "rootNodeId": "n:root", "status": "failed"}]);
        failed["attempts"] = json!([{"id": "a", "runId": "r", "status": "failed"}]);
        failed["turnItems"] =
            json!([{"id": "e", "type": "error", "nodeId": "n:root", "runId": "r", "ordinal": 1}]);
        let events = diff("t", &empty, &failed, "x", Order::End).unwrap();
        assert_eq!(
            types(&events),
            [
                "run-attempt.created",
                "turn-item.updated error",
                "run.created"
            ]
        );
        // A checkpoint: the checkpoints, its item, then the run.
        let mut captured = empty.clone();
        captured["checkpoints"] = json!([{"id": "c0"}, {"id": "c1"}]);
        captured["turnItems"] = json!([{"id": "ci", "type": "checkpoint", "ordinal": 1}]);
        captured["runs"] = json!([{"id": "r", "status": "completed"}]);
        let events = diff("t", &empty, &captured, "x", Order::Checkpoint).unwrap();
        assert_eq!(
            types(&events),
            [
                "checkpoint.captured",
                "checkpoint.captured",
                "turn-item.updated checkpoint",
                "run.created"
            ]
        );
    }

    #[test]
    fn a_run_starts_as_the_reference_writes_it() {
        let empty = json!({
            "thread": {"id": "t", "activeProviderThreadId": null}, "runs": [], "attempts": [],
            "nodes": [], "messages": [], "turnItems": [], "providerThreads": [],
            "providerSessions": [], "checkpointScopes": []
        });
        let mut sent = empty.clone();
        sent["thread"]["activeProviderThreadId"] = json!("pth");
        sent["providerThreads"] = json!([{"id": "pth", "status": "not_loaded"}]);
        sent["runs"] = json!([{"id": "r", "status": "starting"}]);
        sent["attempts"] = json!([{"id": "a", "status": "pending"}]);
        sent["nodes"] = json!([{"id": "n:root", "kind": "root_turn", "status": "pending"}]);
        sent["checkpointScopes"] = json!([{"id": "s"}]);
        sent["messages"] = json!([{"id": "m", "role": "user"}]);
        sent["turnItems"] =
            json!([{"id": "i", "type": "user_message", "messageId": "m", "ordinal": 1}]);
        // The dispatch: thread, provider thread, run, attempt, root node,
        // scope, then the user's message and its item.
        let events = diff("t", &empty, &sent, "x", Order::Dispatch).unwrap();
        assert_eq!(
            types(&events),
            [
                "thread.metadata-updated",
                "provider-thread.updated",
                "run.created",
                "run-attempt.created",
                "node.updated",
                "checkpoint-scope.created",
                "message.updated",
                "turn-item.updated user_message"
            ]
        );
        // The CLI reports its thread: the session, the provider thread, then
        // the run, attempt and root node, running.
        let mut started = sent.clone();
        started["providerSessions"] = json!([{"id": "ps", "status": "ready"}]);
        started["providerThreads"][0]["status"] = json!("active");
        started["runs"][0]["status"] = json!("running");
        started["attempts"][0]["status"] = json!("running");
        started["nodes"][0]["status"] = json!("running");
        let events = diff("t", &sent, &started, "x", Order::Start).unwrap();
        assert_eq!(
            types(&events),
            [
                "provider-session.attached",
                "provider-session.updated",
                "provider-thread.updated",
                "run.updated",
                "run-attempt.updated",
                "node.updated"
            ]
        );
        // A session that failed to open: its error, the run, its attempt and
        // root node.
        let mut failed = sent.clone();
        failed["runs"][0]["status"] = json!("failed");
        failed["attempts"][0]["status"] = json!("failed");
        failed["nodes"][0]["status"] = json!("failed");
        failed["turnItems"].as_array_mut().unwrap().push(json!({
            "id": "turn-item:run:r:signal:provider-session-open-failure", "type": "error",
            "nodeId": "n:root", "ordinal": 2}));
        let events = diff("t", &sent, &failed, "x", Order::End).unwrap();
        assert_eq!(
            types(&events),
            [
                "turn-item.updated error",
                "run.updated",
                "run-attempt.updated",
                "node.updated"
            ]
        );
    }

    #[test]
    fn a_runtime_request_opens_and_is_answered_as_the_reference_writes_it() {
        let tool = json!({"id": "turn-item:tool", "type": "command_execution", "status": "running",
            "nodeId": "n:tool", "ordinal": 2, "nativeItemRef": {"nativeId": "toolu_1"}});
        let before = json!({
            "thread": {"id": "t"}, "runs": [], "attempts": [], "messages": [],
            "nodes": [{"id": "n:tool", "kind": "tool_call", "status": "running"}],
            "turnItems": [tool], "runtimeRequests": []
        });
        let opened = |driver: &str, kind: &str| {
            let mut after = before.clone();
            after["nodes"]
                .as_array_mut()
                .unwrap()
                .push(json!({"id": "n:card", "kind": "approval_request", "status": "waiting"}));
            after["turnItems"]
                .as_array_mut()
                .unwrap()
                .push(json!({"id": "turn-item:card",
                "type": "approval_request", "status": "waiting", "nodeId": "n:card", "ordinal": 3,
                "nativeItemRef": {"nativeId": "toolu_1"}}));
            after["runtimeRequests"] = json!([{"id": format!("runtime-request:provider:{driver}:x"),
                "status": "pending", "kind": kind, "nativeRequestRef": {"driver": driver, "nativeId": "toolu_1"}}]);
            after
        };
        // Codex: the card's node and card, then the request.
        let codex = diff(
            "t",
            &before,
            &opened("codex", "command"),
            "x",
            Order::RequestOpen,
        )
        .unwrap();
        assert_eq!(
            types(&codex),
            [
                "node.updated",
                "turn-item.updated approval_request",
                "runtime-request.updated"
            ]
        );
        // Claude: the card's node, the request, the card, then the tool call
        // the request is about, reported again.
        let claude_after = opened("claudeAgent", "command");
        let claude = diff("t", &before, &claude_after, "x", Order::RequestOpen).unwrap();
        assert_eq!(
            types(&claude),
            [
                "node.updated",
                "runtime-request.updated",
                "turn-item.updated approval_request",
                "turn-item.updated command_execution"
            ]
        );
        let acp = diff(
            "t",
            &before,
            &opened("antigravity", "command"),
            "x",
            Order::RequestOpen,
        )
        .unwrap();
        assert_eq!(
            types(&acp),
            [
                "node.updated",
                "runtime-request.updated",
                "turn-item.updated approval_request"
            ]
        );
        // A Claude question's tool call is not reported again.
        let question = diff(
            "t",
            &before,
            &opened("claudeAgent", "user_input"),
            "x",
            Order::RequestOpen,
        )
        .unwrap();
        assert_eq!(types(&question).len(), 3);
        // The answer: the request, then its card's node and card.
        let mut answered = claude_after.clone();
        answered["runtimeRequests"][0]["status"] = json!("resolved");
        answered["nodes"][1]["status"] = json!("completed");
        answered["turnItems"][1]["status"] = json!("completed");
        let answer = diff("t", &claude_after, &answered, "x", Order::Answer).unwrap();
        assert_eq!(
            types(&answer),
            [
                "runtime-request.updated",
                "node.updated",
                "turn-item.updated approval_request"
            ]
        );
    }

    #[test]
    fn a_subagent_starts_and_ends_as_the_reference_writes_it() {
        let before = json!({"thread": {"id": "t"}, "nodes": [], "turnItems": [], "subagents": [],
            "contextTransfers": []});
        let mut started = before.clone();
        started["nodes"] = json!([{"id": "n:task", "kind": "subagent", "status": "running"}]);
        started["subagents"] = json!([{"id": "n:task", "status": "running"}]);
        started["turnItems"] =
            json!([{"id": "i:task", "type": "subagent", "nodeId": "n:task", "ordinal": 2}]);
        let events = diff("t", &before, &started, "x", Order::Subagent).unwrap();
        assert_eq!(
            types(&events),
            [
                "node.updated",
                "subagent.updated",
                "turn-item.updated subagent"
            ]
        );
        let mut ended = started.clone();
        ended["nodes"][0]["status"] = json!("completed");
        ended["subagents"][0]["status"] = json!("completed");
        ended["turnItems"][0]["status"] = json!("completed");
        ended["contextTransfers"] = json!([{"id": "ct", "status": "consumed"}]);
        let events = diff("t", &started, &ended, "x", Order::Subagent).unwrap();
        assert_eq!(
            types(&events),
            [
                "subagent.updated",
                "node.updated",
                "turn-item.updated subagent",
                "context-transfer.created"
            ]
        );
    }
}
