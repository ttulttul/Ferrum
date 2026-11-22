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

/// The domain events that take `before` to `after`, in the order the
/// reference's turns emit them: thread, run and attempt, then each changed
/// turn item preceded by its node and message, then provider records.
/// `None` when a record went away, which no event expresses.
pub fn diff(
    thread_id: &str,
    before: &Value,
    after: &Value,
    occurred_at: &str,
) -> Option<Vec<Value>> {
    diff_with(thread_id, before, after, occurred_at, false)
}

/// [`diff`]'s events for every record that is new or changed, also when
/// records went away (which the shared database records by deleting their
/// rows, not with an event).
pub fn diff_all(thread_id: &str, before: &Value, after: &Value, occurred_at: &str) -> Vec<Value> {
    diff_with(thread_id, before, after, occurred_at, true).unwrap_or_default()
}

fn diff_with(
    thread_id: &str,
    before: &Value,
    after: &Value,
    occurred_at: &str,
    allow_removals: bool,
) -> Option<Vec<Value>> {
    let mut out = Vec::new();
    if before["thread"] != after["thread"] && after["thread"].is_object() {
        out.push(event(
            thread_id,
            thread_event(&before["thread"], &after["thread"]),
            &after["thread"],
            occurred_at,
        ));
    }
    let changed = |collection: &str| -> Option<Vec<(bool, Value)>> {
        let old = by_id(before, collection);
        let new = by_id(after, collection);
        if !allow_removals && old.keys().any(|id| !new.contains_key(id)) {
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
    push_collection(&mut out, "runs");
    push_collection(&mut out, "attempts");
    // Nodes and messages go with their turn item; the rest after the run.
    let mut nodes: Vec<&Value> = collections["nodes"].iter().map(|(_, r)| r).collect();
    let mut messages: Vec<&Value> = collections["messages"].iter().map(|(_, r)| r).collect();
    let mut items: Vec<&Value> = collections["turnItems"].iter().map(|(_, r)| r).collect();
    items.sort_by_key(|item| item["ordinal"].as_u64().unwrap_or(0));
    let item_nodes: std::collections::HashSet<&str> = items
        .iter()
        .filter_map(|item| item["nodeId"].as_str())
        .collect();
    let item_messages: std::collections::HashSet<&str> = items
        .iter()
        .filter_map(|item| item["messageId"].as_str())
        .collect();
    for node in nodes.iter().filter(|node| {
        node["id"]
            .as_str()
            .is_none_or(|id| !item_nodes.contains(id))
    }) {
        out.push(event(thread_id, "node.updated", node, occurred_at));
    }
    push_collection(&mut out, "checkpointScopes");
    for message in messages.iter().filter(|m| {
        m["id"]
            .as_str()
            .is_none_or(|id| !item_messages.contains(id))
    }) {
        out.push(event(thread_id, "message.updated", message, occurred_at));
    }
    for item in items {
        if let Some(index) = nodes.iter().position(|node| node["id"] == item["nodeId"]) {
            out.push(event(
                thread_id,
                "node.updated",
                nodes.remove(index),
                occurred_at,
            ));
        }
        if let Some(index) = messages.iter().position(|m| m["id"] == item["messageId"]) {
            out.push(event(
                thread_id,
                "message.updated",
                messages.remove(index),
                occurred_at,
            ));
        }
        out.push(event(thread_id, "turn-item.updated", item, occurred_at));
    }
    for name in [
        "providerSessions",
        "providerThreads",
        "providerTurns",
        "runtimeRequests",
        "subagents",
        "plans",
        "checkpoints",
        "contextHandoffs",
        "contextTransfers",
    ] {
        push_collection(&mut out, name);
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
        let events = diff("t", &before, &after, "2026-01-02T00:00:00.000Z").unwrap();
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
        assert!(diff("t", &after, &before, "x").is_none());
        // Nothing changed, nothing to send.
        assert!(diff("t", &after, &after, "x").unwrap().is_empty());
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
}
