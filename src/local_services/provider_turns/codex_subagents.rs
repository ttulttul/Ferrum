//! The native subagents a Codex turn spawns, as the reference's Codex adapter follows them
//! (`registerSubagentThreads`, `updateSubagentStates`): a `collabAgentToolCall` whose tool is
//! `spawnAgent` starts one subagent per receiver thread, each on a child thread of its own,
//! and every collab call after it (`wait`, `sendInput`, ...) reports the children's states.
//! Codex 0.170 reports a subagent as `subAgentActivity` items instead (`registerSubagentActivity`):
//! `started` names the child thread and its path, which titles the subagent, and `completed` or
//! `interrupted` ends it. Such a subagent's model is read from Codex (`thread/read`, else
//! `thread/resume`), as the reference reads it.
//! Each subagent shows as a `subagent` turn item of the spawning call, which the projection
//! turns into the parent's subagent record, its node and the child's provider thread.
use crate::workspace::provider::ProviderItem;
use indexmap::IndexMap;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

/// One native subagent.
#[derive(Clone, Debug)]
struct Subagent {
    /// `<spawn call id>:<child thread id>`, the item and node it shows as.
    item_id: String,
    /// The child's path (`/root/read_line`) when Codex reported one.
    title: Option<String>,
    prompt: String,
    model: Option<String>,
    reasoning_effort: Option<String>,
    status: &'static str,
    result: Option<String>,
}

/// The turn's subagents, by native child thread id.
#[derive(Debug, Default)]
pub(crate) struct Subagents {
    by_thread: IndexMap<String, Subagent>,
}

/// `isOrchestrationV2WorkActive`.
fn active(status: &str) -> bool {
    matches!(status, "pending" | "running")
}

/// A `CollabAgentStatus` as a subagent's status.
fn status_of(state: &str) -> &'static str {
    match state {
        "pendingInit" => "pending",
        "running" => "running",
        "interrupted" => "interrupted",
        "completed" => "completed",
        "shutdown" => "cancelled",
        // errored, notFound
        _ => "failed",
    }
}

/// `IdAllocator.derive.subagentThread`: the child's app thread, a short hash of the thread
/// id its native thread would otherwise have.
pub(crate) fn child_thread_id(native_thread_id: &str) -> String {
    let legacy = format!(
        "thread:provider:codex:native-thread:{}",
        crate::thread_projection::uri_component(native_thread_id)
    );
    let digest = hex::encode(Sha256::digest(legacy.as_bytes()));
    format!("thread:subagent:{}", &digest[..16])
}

fn text(value: &Value) -> Option<String> {
    value
        .as_str()
        .filter(|text| !text.is_empty())
        .map(str::to_owned)
}

impl Subagents {
    /// The subagent items a `collabAgentToolCall` changes.
    pub(crate) fn observe(&mut self, call: &Value) -> Vec<ProviderItem> {
        let mut changed: Vec<String> = Vec::new();
        if call["tool"] == "spawnAgent" {
            let call_id = call["id"].as_str().unwrap_or_default();
            for native in call["receiverThreadIds"].as_array().into_iter().flatten() {
                let Some(native) = native.as_str() else {
                    continue;
                };
                if self.by_thread.contains_key(native) {
                    continue;
                }
                self.by_thread.insert(
                    native.to_owned(),
                    Subagent {
                        item_id: format!("{call_id}:{native}"),
                        title: None,
                        prompt: call["prompt"].as_str().unwrap_or_default().to_owned(),
                        model: text(&call["model"]),
                        reasoning_effort: text(&call["reasoningEffort"]),
                        status: "running",
                        result: None,
                    },
                );
                changed.push(native.to_owned());
            }
        }
        for (native, state) in call["agentsStates"].as_object().into_iter().flatten() {
            let Some(subagent) = self.by_thread.get_mut(native) else {
                continue;
            };
            // A settled subagent is not reopened by a late state (`emitSubagentTaskUpdate`).
            if active(subagent.status) {
                subagent.status = status_of(state["status"].as_str().unwrap_or_default());
            }
            if let Some(message) = state["message"].as_str() {
                subagent.result = Some(message.to_owned());
            }
            if !changed.contains(native) {
                changed.push(native.clone());
            }
        }
        changed
            .iter()
            .filter_map(|native| Some((native, self.by_thread.get(native)?)))
            .map(|(native, subagent)| item(native, subagent))
            .collect()
    }

    /// The subagent item a `subAgentActivity` changes, and the child whose model is still to
    /// be read, once it starts.
    pub(crate) fn activity(&mut self, activity: &Value) -> (Option<ProviderItem>, Option<String>) {
        let Some(native) = activity["agentThreadId"].as_str() else {
            return (None, None);
        };
        match activity["kind"].as_str() {
            Some("started") => {
                if self.by_thread.contains_key(native) {
                    return (None, None);
                }
                let subagent = Subagent {
                    item_id: format!("{}:{native}", activity["id"].as_str().unwrap_or_default()),
                    title: text(&activity["agentPath"]),
                    prompt: String::new(),
                    model: None,
                    reasoning_effort: None,
                    status: "running",
                    result: None,
                };
                let item = item(native, &subagent);
                self.by_thread.insert(native.to_owned(), subagent);
                (Some(item), Some(native.to_owned()))
            }
            Some(kind @ ("completed" | "interrupted")) => {
                let Some(subagent) = self.by_thread.get_mut(native) else {
                    return (None, None);
                };
                if active(subagent.status) {
                    subagent.status = if kind == "completed" {
                        "completed"
                    } else {
                        "interrupted"
                    };
                }
                (Some(item(native, subagent)), None)
            }
            _ => (None, None),
        }
    }

    /// The child's model and effort, once Codex reported them (`updateSubagentSelection`).
    pub(crate) fn selected(
        &mut self,
        native: &str,
        model: &str,
        effort: Option<&str>,
    ) -> Option<ProviderItem> {
        let subagent = self.by_thread.get_mut(native)?;
        if model.trim().is_empty() || subagent.model.is_some() {
            return None;
        }
        subagent.model = Some(model.to_owned());
        subagent.reasoning_effort = effort.filter(|e| !e.is_empty()).map(str::to_owned);
        Some(item(native, subagent))
    }
}

fn item(native: &str, subagent: &Subagent) -> ProviderItem {
    let mut item = ProviderItem::new(&subagent.item_id, "subagent", subagent.status);
    item.title = subagent.title.clone();
    let mut item = item
        .with("origin", "provider_native")
        .with("childThreadId", child_thread_id(native))
        .with("nativeThreadId", native)
        .with("prompt", subagent.prompt.as_str())
        .with("result", json!(subagent.result))
        .with("model", json!(subagent.model));
    if let Some(effort) = &subagent.reasoning_effort {
        item = item.with("reasoningEffort", effort.as_str());
    }
    item
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_child_thread_id_is_the_reference_digest() {
        assert_eq!(
            child_thread_id("01a1fbcd-1b00-7000-8000-00000000c41d"),
            "thread:subagent:c8b07fc634c67848"
        );
    }

    #[test]
    fn a_spawned_subagent_follows_the_calls_after_it_and_stays_settled() {
        let mut subagents = Subagents::default();
        let spawn = json!({"id": "call_1", "tool": "spawnAgent", "receiverThreadIds": [],
            "prompt": "Read it", "agentsStates": {}});
        assert!(subagents.observe(&spawn).is_empty());
        let spawned = json!({"id": "call_1", "tool": "spawnAgent", "receiverThreadIds": ["c1"],
            "prompt": "Read it", "model": "m", "agentsStates": {"c1": {"status": "pendingInit"}}});
        let items = subagents.observe(&spawned);
        assert_eq!(items.len(), 1);
        assert_eq!(
            (items[0].id.as_str(), items[0].status.as_str()),
            ("call_1:c1", "pending")
        );
        let waited = json!({"id": "call_2", "tool": "wait", "receiverThreadIds": ["c1"],
            "agentsStates": {"c1": {"status": "completed", "message": "Done"}}});
        let items = subagents.observe(&waited);
        assert_eq!(items[0].status, "completed");
        assert_eq!(items[0].fields["result"], "Done");
        let late = json!({"id": "call_3", "tool": "wait",
            "agentsStates": {"c1": {"status": "running", "message": null}}});
        assert_eq!(subagents.observe(&late)[0].status, "completed");
    }

    #[test]
    fn an_activity_starts_a_titled_subagent_whose_model_is_read_later() {
        let mut subagents = Subagents::default();
        let started = json!({"type": "subAgentActivity", "id": "call_9", "kind": "started",
            "agentThreadId": "c9", "agentPath": "/root/read_line", "model": "m"});
        let (item, read) = subagents.activity(&started);
        let item = item.unwrap();
        assert_eq!(
            (item.id.as_str(), item.status.as_str()),
            ("call_9:c9", "running")
        );
        assert_eq!(item.title.as_deref(), Some("/root/read_line"));
        assert_eq!(item.fields["model"], Value::Null);
        assert_eq!(read.as_deref(), Some("c9"));
        // Codex reports the started item twice; the second changes nothing.
        assert_eq!(subagents.activity(&started), (None, None));
        let selected = subagents.selected("c9", "gpt", Some("low")).unwrap();
        assert_eq!(
            (
                selected.fields["model"].as_str(),
                selected.fields["reasoningEffort"].as_str()
            ),
            (Some("gpt"), Some("low"))
        );
        let done = json!({"kind": "completed", "agentThreadId": "c9"});
        assert_eq!(subagents.activity(&done).0.unwrap().status, "completed");
    }
}
