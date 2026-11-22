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
pub fn digest(projection: &Value) -> String {
    let hash = Sha256::digest(crate::wire_json::compact(projection).as_bytes());
    hex::encode(&hash[..16])
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
    /// A checkpoint capture (`CheckpointCaptureService`): the checkpoints,
    /// the checkpoint item, then the run and its root node.
    Checkpoint,
    /// A run sent (`ThreadMessageIntake`): the thread, the provider thread
    /// the run opens, the run, its pending attempt and root node, the
    /// checkpoint scope, then the user's message and its item.
    Dispatch,
    /// A turn's start, once the CLI reports its thread: the provider
    /// session, the provider thread, then the run, its attempt and root
    /// node, running.
    Start,
}

impl Order {
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
            crate::Event::RunProvider {
                event: ProviderEvent::ProviderThread { .. },
                ..
            } => Order::Start,
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
            Order::Checkpoint => &[
                Collection("checkpoints"),
                Items(Phase::All),
                Collection("runs"),
                Collection("attempts"),
                LooseNodes,
                Collection("checkpointScopes"),
                LooseMessages,
                Collection("providerSessions"),
                Collection("providerThreads"),
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
    if before["thread"] != after["thread"] && after["thread"].is_object() {
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
    let collections: HashMap<&str, Vec<(bool, Value)>> = COLLECTIONS
        .iter()
        .map(|(name, _, _)| *name)
        .chain(["nodes", "messages", "turnItems"])
        .map(|name| changed(name).map(|records| (name, records)))
        .collect::<Option<_>>()?;
    let push_collection = |out: &mut Vec<Value>, name: &str| {
        let (_, created, updated) = COLLECTIONS
            .iter()
            .find(|(collection, _, _)| *collection == name)
            .copied()
            .unwrap_or((name, "", ""));
        for (is_new, record) in &collections[name] {
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
                        == crate::workspace::provider::CODEX_STREAM_CLOSED))
    };
    // A session that failed to open (`ProviderSessionManager`) writes its
    // error, then the run, its attempt and root node.
    let session_failed = order == Order::End
        && items.iter().any(|item| {
            item["id"]
                .as_str()
                .is_some_and(|id| id.ends_with(":signal:provider-session-open-failure"))
        });
    let steps = if session_failed {
        Order::SESSION_OPEN_FAILURE
    } else {
        order.steps()
    };
    for step in steps {
        match *step {
            Step::Collection(name) => push_collection(&mut out, name),
            Step::ReportedProviderThread => {
                // A turn whose event stream closed reported nothing more.
                let closed = items.iter().any(|item| {
                    item["failure"]["message"] == crate::workspace::provider::CODEX_STREAM_CLOSED
                });
                if turn_reported && !closed {
                    for (_, record) in &collections["providerThreads"] {
                        if record["driver"] == crate::workspace::provider::CLAUDE_AGENT {
                            out.push(event(
                                thread_id,
                                "provider-thread.updated",
                                record,
                                occurred_at,
                            ));
                        }
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
                    if let Some(index) = messages.iter().position(|m| m["id"] == item["messageId"])
                    {
                        out.push(event(
                            thread_id,
                            "message.updated",
                            messages.remove(index),
                            occurred_at,
                        ));
                    }
                    out.push(event(thread_id, "turn-item.updated", item, occurred_at));
                }
            }
        }
    }
    Some(out)
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
}
