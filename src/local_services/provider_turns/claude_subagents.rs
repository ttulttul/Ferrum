//! The native subagents a Claude turn runs with its `Agent` tool, as the reference's Claude
//! adapter follows them: the call starts a task (`task_started`, `task_type: local_agent`), its
//! progress names what it does (`task_progress`), the child's own messages come with the call
//! as their `parent_tool_use_id`, and the task ends (`task_notification`). The subagent shows as
//! a `subagent` item on its parent (`task:<id>:subagent`); its child thread, made by the control
//! plane, gets the prompt, the child's messages and the task's summary as its result message.
//! The child's records are items tagged with their task (`subagentChild`), which the parent's
//! projection skips.
use crate::workspace::provider::ProviderItem;
use serde_json::{json, Value};
use std::collections::HashMap;

/// The `Agent` calls that may start a task, by tool call: their input.
#[derive(Debug, Default)]
pub(crate) struct ClaudeSubagents {
    calls: HashMap<String, Value>,
    /// Running and ended tasks, by task id.
    tasks: HashMap<String, Task>,
    /// The task each `Agent` call started, by tool call.
    by_call: HashMap<String, String>,
    /// The tasks the current turn started.
    this_turn: std::collections::HashSet<String>,
}

#[derive(Debug, Clone)]
struct Task {
    title: Option<String>,
    prompt: String,
    model: Option<String>,
    status: &'static str,
    progress: Option<String>,
    result: Option<String>,
}

/// `claudeTaskOutcome`.
fn outcome(status: &str) -> &'static str {
    match status {
        "completed" => "completed",
        "stopped" => "cancelled",
        _ => "failed",
    }
}

fn text(value: &Value) -> Option<String> {
    value
        .as_str()
        .map(str::trim)
        .filter(|t| !t.is_empty())
        .map(str::to_owned)
}

impl ClaudeSubagents {
    /// An `Agent` call: shown as its subagent once its task starts, never as a tool call.
    pub(crate) fn call(&mut self, id: &str, input: &Value) {
        self.calls.insert(id.to_owned(), input.clone());
    }

    /// A turn starts: the tasks it starts are its own.
    pub(crate) fn begin_turn(&mut self) {
        self.this_turn.clear();
    }

    /// Whether the turn started a subagent (its usage says so).
    pub(crate) fn ran_any(&self) -> bool {
        !self.this_turn.is_empty()
    }

    /// The task a child's message belongs to, by its `parent_tool_use_id`.
    pub(crate) fn task_of(&self, call: &str) -> Option<String> {
        self.by_call.get(call).cloned()
    }

    /// What a `task_*` frame changes: the subagent item, then the child's records.
    pub(crate) fn frame(&mut self, message: &Value, model: Option<&str>) -> Vec<ProviderItem> {
        let Some(task_id) = message["task_id"].as_str() else {
            return Vec::new();
        };
        match message["subtype"].as_str() {
            Some("task_started") => {
                if message["task_type"] != "local_agent" || self.tasks.contains_key(task_id) {
                    return Vec::new();
                }
                let call = message["tool_use_id"].as_str().unwrap_or_default();
                let input = self.calls.get(call).cloned().unwrap_or(Value::Null);
                self.by_call.insert(call.to_owned(), task_id.to_owned());
                let task = Task {
                    title: text(&message["description"]).or_else(|| text(&input["description"])),
                    prompt: text(&input["prompt"])
                        .or_else(|| text(&message["prompt"]))
                        .unwrap_or_default(),
                    model: text(&input["model"]).or_else(|| model.map(str::to_owned)),
                    status: "running",
                    progress: None,
                    result: None,
                };
                self.tasks.insert(task_id.to_owned(), task);
                self.this_turn.insert(task_id.to_owned());
                self.item(task_id).into_iter().collect()
            }
            Some("task_progress") => {
                let Some(task) = self.tasks.get_mut(task_id) else {
                    return Vec::new();
                };
                let line = text(&message["description"]);
                if task.status != "running" || line.is_none() || task.progress == line {
                    return Vec::new();
                }
                task.progress = line;
                self.item(task_id).into_iter().collect()
            }
            Some("task_notification") => {
                let Some(task) = self.tasks.get_mut(task_id) else {
                    return Vec::new();
                };
                if task.status != "running" {
                    return Vec::new();
                }
                task.status = outcome(message["status"].as_str().unwrap_or_default());
                let mut out = Vec::new();
                // The task's summary is the child's result message, when it ends with the turn
                // that started it (a background subagent's end wakes the session instead).
                let own = self.this_turn.contains(task_id);
                if let Some(summary) = text(&message["summary"]).filter(|_| own) {
                    out.push(child(
                        task_id,
                        ProviderItem::new(
                            format!("task:{task_id}:result"),
                            "assistant_message",
                            "completed",
                        )
                        .with("text", summary)
                        .with("streaming", false),
                    ));
                }
                out.extend(self.item(task_id));
                out
            }
            _ => Vec::new(),
        }
    }

    /// A child's own message: its text, on the child thread, is the subagent's last answer.
    pub(crate) fn child_message(&mut self, task_id: &str, message: &Value) -> Vec<ProviderItem> {
        if message["type"] != "assistant" {
            return Vec::new();
        }
        let joined: Vec<&str> = message["message"]["content"]
            .as_array()
            .into_iter()
            .flatten()
            .filter(|block| block["type"] == "text")
            .filter_map(|block| block["text"].as_str())
            .collect();
        let Some(id) = message["uuid"].as_str() else {
            return Vec::new();
        };
        if joined.is_empty() {
            return Vec::new();
        }
        let body = joined.join("");
        if let Some(task) = self.tasks.get_mut(task_id) {
            task.result = Some(body.clone());
        }
        vec![child(
            task_id,
            ProviderItem::new(id, "assistant_message", "completed")
                .with("text", body)
                .with("streaming", false),
        )]
    }

    fn item(&self, task_id: &str) -> Option<ProviderItem> {
        let task = self.tasks.get(task_id)?;
        let mut item =
            ProviderItem::new(format!("task:{task_id}:subagent"), "subagent", task.status);
        item.title = task.title.clone();
        let mut item = item
            .with("origin", "provider_native")
            .with("claudeTaskId", task_id)
            .with("nodeNativeId", format!("task:{task_id}"))
            .with(
                "nativeItemRef",
                json!({"driver": "claudeAgent", "nativeId": task_id, "strength": "strong"}),
            )
            .with("prompt", task.prompt.as_str())
            .with("result", json!(task.result))
            .with("model", json!(task.model));
        if let Some(progress) = &task.progress {
            item = item.with("progress", progress.as_str());
        }
        Some(item)
    }
}

/// A record of a child thread, tagged with its task.
fn child(task_id: &str, item: ProviderItem) -> ProviderItem {
    item.with("subagentChild", task_id)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_agent_call_runs_a_subagent_that_answers_and_ends() {
        let mut subagents = ClaudeSubagents::default();
        subagents.call(
            "toolu_1",
            &json!({"description": "Read it", "prompt": "Read README.md"}),
        );
        let started = subagents.frame(
            &json!({"subtype": "task_started", "task_id": "t1", "task_type": "local_agent",
                "tool_use_id": "toolu_1", "description": "Read it"}),
            Some("claude-opus-5-5"),
        );
        assert_eq!(
            (started[0].id.as_str(), started[0].status.as_str()),
            ("task:t1:subagent", "running")
        );
        assert_eq!(started[0].fields["prompt"], "Read README.md");
        assert_eq!(subagents.task_of("toolu_1").as_deref(), Some("t1"));
        let answer = subagents.child_message(
            "t1",
            &json!({"type": "assistant", "uuid": "u1", "message": {"content": [{"type": "text", "text": "# Title"}]}}),
        );
        assert_eq!(answer[0].fields["subagentChild"], "t1");
        let ended = subagents.frame(
            &json!({"subtype": "task_notification", "task_id": "t1", "status": "completed", "summary": "Read it"}),
            None,
        );
        assert_eq!(ended[0].id, "task:t1:result");
        assert_eq!(
            (ended[1].status.as_str(), ended[1].fields["result"].as_str()),
            ("completed", Some("# Title"))
        );
        // A background command's task is not a subagent.
        assert!(subagents
            .frame(
                &json!({"subtype": "task_started", "task_id": "b1", "task_type": "local_bash"}),
                None
            )
            .is_empty());
    }
}
