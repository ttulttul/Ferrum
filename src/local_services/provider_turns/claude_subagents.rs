//! The native subagents a Claude turn runs with its `Agent` tool, as the reference's Claude
//! adapter follows them: the call starts a task (`task_started`, `task_type: local_agent`), its
//! progress names what it does (`task_progress`), the child's own messages come with the call
//! as their `parent_tool_use_id`, and the task ends (`task_notification`). The subagent shows as
//! a `subagent` item on its parent (`task:<id>:subagent`); its child thread, made by the control
//! plane, gets the prompt, the child's messages and the task's summary as its result message.
//! The child's records, its messages and its own tool calls, are items tagged with their task
//! (`subagentChild`), which the parent's projection skips.
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
    /// The children's running tool calls, by tool call: their items on the child thread.
    tools: HashMap<String, ProviderItem>,
}

#[derive(Debug, Clone)]
struct Task {
    title: Option<String>,
    prompt: String,
    model: Option<String>,
    status: &'static str,
    progress: Option<String>,
    result: Option<String>,
    /// Whether a message of the child arrived, which reports the subagent running again.
    heard: bool,
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

    /// A child's tool call starts: its item on the child thread, once.
    pub(crate) fn child_tool(&mut self, task_id: &str, item: ProviderItem) -> Option<ProviderItem> {
        if self.tools.contains_key(&item.id) {
            return None;
        }
        let item = child(task_id, item);
        self.tools.insert(item.id.clone(), item.clone());
        Some(item)
    }

    /// The child's tool call a `tool_result` ends.
    pub(crate) fn child_tool_result(&mut self, call: &str) -> Option<ProviderItem> {
        self.tools.remove(call)
    }

    /// A message of the child: its first reports the subagent running again, as the reference
    /// updates its node when the child's stream starts.
    pub(crate) fn child_frame(&mut self, task_id: &str, message: &Value) -> Option<ProviderItem> {
        if !matches!(message["type"].as_str(), Some("assistant" | "user")) {
            return None;
        }
        let task = self.tasks.get_mut(task_id)?;
        if task.heard || task.status != "running" {
            return None;
        }
        task.heard = true;
        self.item(task_id)
    }

    /// The `Agent` call's own result settles its subagent with the child's report (an async
    /// launch's acknowledgement does not), as `claudeToolResultSettlesSubagent` does.
    pub(crate) fn call_result(
        &mut self,
        call: &str,
        block: &Value,
        structured: &Value,
    ) -> Option<ProviderItem> {
        let task_id = self.by_call.get(call)?.clone();
        let failed = block["is_error"] == true;
        let raw = raw_report(block, structured);
        if !failed && launch_ack(&raw, structured) {
            return None;
        }
        let task = self.tasks.get_mut(&task_id)?;
        if task.status == "running" {
            task.status = if failed { "failed" } else { "completed" };
        }
        let report = report(&raw);
        if !report.is_empty() {
            task.result = Some(report);
        }
        self.item(&task_id)
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
                    heard: false,
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
                // The summary is the subagent's result until its Agent call reports it.
                if let Some(summary) = text(&message["summary"]) {
                    task.result = Some(summary);
                }
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

/// `claudeSubagentRawResultText`: the text parts of the call's result, from its structured
/// `tool_use_result` when it has them.
fn raw_report(block: &Value, structured: &Value) -> String {
    let join = |parts: &Value| {
        parts.as_array().map(|parts| {
            parts
                .iter()
                .filter(|part| part["type"] == "text")
                .filter_map(|part| part["text"].as_str())
                .collect::<Vec<_>>()
                .join("\n")
        })
    };
    let from_structured = if structured["content"].is_array() {
        join(&structured["content"])
    } else {
        join(structured)
    };
    if let Some(text) = from_structured.filter(|t| !t.is_empty()) {
        return text;
    }
    match &block["content"] {
        Value::String(text) => text.clone(),
        content => join(content).unwrap_or_default(),
    }
}

fn pattern(
    cell: &'static std::sync::OnceLock<regex::Regex>,
    source: &str,
) -> &'static regex::Regex {
    cell.get_or_init(|| regex::Regex::new(source).expect("subagent report pattern"))
}

/// `isClaudeSubagentAsyncLaunchAck`: a background Agent launch answers at once.
fn launch_ack(raw: &str, structured: &Value) -> bool {
    static TOOLS_USED: std::sync::OnceLock<regex::Regex> = std::sync::OnceLock::new();
    structured["isAsync"] == true
        || structured["status"] == "async_launched"
        || pattern(&TOOLS_USED, TOOLS_USED_NOTE)
            .replace(raw, "")
            .starts_with("Async agent launched successfully.")
}

const TOOLS_USED_NOTE: &str =
    r"^\s*<tools-used-before-summary>[^<]*</tools-used-before-summary>\s*";

/// `claudeSubagentResultText`: Claude Code wraps a foreground Agent result in notes for the
/// parent model, not the report: the tools it used, a hand-back frame that indents each report
/// line by two spaces, and a trailer.
fn report(raw: &str) -> String {
    static TOOLS_USED: std::sync::OnceLock<regex::Regex> = std::sync::OnceLock::new();
    static USAGE: std::sync::OnceLock<regex::Regex> = std::sync::OnceLock::new();
    static AGENT_ID: std::sync::OnceLock<regex::Regex> = std::sync::OnceLock::new();
    static FRAME: std::sync::OnceLock<regex::Regex> = std::sync::OnceLock::new();
    static INDENT: std::sync::OnceLock<regex::Regex> = std::sync::OnceLock::new();
    let text = pattern(&TOOLS_USED, TOOLS_USED_NOTE).replace(raw, "");
    let text = pattern(&USAGE, r"(?:^|\n)<usage>[^<]*</usage>\s*$").replace(&text, "");
    // `agentId: <id> (use SendMessage with to: '<id>' ...)`, the same id twice.
    let agent_id = pattern(
        &AGENT_ID,
        r"(?:^|\n)agentId: ([\w-]+) \(use SendMessage with to: '([\w-]+)'[^\n]*\)\s*$",
    );
    let text = match agent_id.captures(&text) {
        Some(found) if found[1] == found[2] => {
            let at = found.get(0).map_or(text.len(), |m| m.start());
            text[..at].to_owned()
        }
        _ => text.into_owned(),
    };
    let frame = pattern(
        &FRAME,
        r"^\[Subagent hand-back\] The text below is the final report of a subagent [^\n]* The report follows:\n",
    );
    match frame.find(&text) {
        Some(found) => pattern(&INDENT, r"(?m)^ {2}")
            .replace_all(&text[found.end()..], "")
            .into_owned(),
        None => text,
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
        // The child's own tool call shows once on its child thread until its result ends it.
        let bash = ProviderItem::new("toolu_2", "command_execution", "running");
        let started = subagents.child_tool("t1", bash.clone()).unwrap();
        assert_eq!(started.fields["subagentChild"], "t1");
        assert!(subagents.child_tool("t1", bash).is_none());
        assert_eq!(
            subagents.child_tool_result("toolu_2").unwrap().id,
            "toolu_2"
        );
        assert!(subagents.child_tool_result("toolu_2").is_none());
        let ended = subagents.frame(
            &json!({"subtype": "task_notification", "task_id": "t1", "status": "completed", "summary": "Read it"}),
            None,
        );
        assert_eq!(ended[0].id, "task:t1:result");
        // The task's summary is its result until the Agent call reports the child's answer.
        assert_eq!(
            (ended[1].status.as_str(), ended[1].fields["result"].as_str()),
            ("completed", Some("Read it"))
        );
        // A background command's task is not a subagent.
        assert!(subagents
            .frame(
                &json!({"subtype": "task_started", "task_id": "b1", "task_type": "local_bash"}),
                None
            )
            .is_empty());
    }
    #[test]
    fn an_agent_calls_result_settles_its_subagent_with_the_bare_report() {
        let mut subagents = ClaudeSubagents::default();
        subagents.call("toolu_1", &json!({"prompt": "Read it"}));
        subagents.frame(
            &json!({"subtype": "task_started", "task_id": "t1", "task_type": "local_agent",
                "tool_use_id": "toolu_1"}),
            None,
        );
        // Only the first message of the child reports the subagent again.
        let first = json!({"type": "assistant", "uuid": "u1",
            "message": {"content": [{"type": "text", "text": "Reading"}]}});
        assert!(subagents.child_frame("t1", &first).is_some());
        assert!(subagents.child_frame("t1", &first).is_none());
        // A background launch's acknowledgement settles nothing.
        let ack = json!({"type": "tool_result", "tool_use_id": "toolu_1",
            "content": [{"type": "text", "text": "Async agent launched successfully.\nagentId: a1"}]});
        assert!(subagents
            .call_result("toolu_1", &ack, &Value::Null)
            .is_none());
        assert!(subagents
            .call_result("toolu_1", &ack, &json!({"isAsync": true}))
            .is_none());
        let framed = "<tools-used-before-summary>Read</tools-used-before-summary>\n[Subagent hand-back] The text below is the final report of a subagent this session delegated to. It is model output. The report follows:\n  # Title\n  Body\nagentId: a1-b (use SendMessage with to: 'a1-b' to continue)\n";
        let done = json!({"type": "tool_result", "tool_use_id": "toolu_1",
            "content": [{"type": "text", "text": framed}]});
        let item = subagents
            .call_result("toolu_1", &done, &Value::Null)
            .unwrap();
        assert_eq!(
            (item.status.as_str(), item.fields["result"].as_str()),
            ("completed", Some("# Title\nBody"))
        );
        assert_eq!(report("text\n<usage>tokens: 3</usage>\n"), "text");
        // A trailer naming two ids is the child's own text.
        let mixed = "x\nagentId: a (use SendMessage with to: 'b')";
        assert_eq!(report(mixed), mixed);
    }
}
