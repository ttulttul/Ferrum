//! The background tasks a Claude session runs that report their own end (`local_bash`
//! commands), as the reference's adapter keeps them per native session: started by a
//! turn (`task_started`, `background_tasks_changed`), and when one ends while no turn
//! runs (`task_notification`), reported with the wake it starts, so the continuation run
//! names the command and how it ended (`opaqueTaskWakeReport`).
//!
//! A command a `Monitor` tool call starts is a monitor: its events wake Claude with a
//! bare turn and no `task_notification`, which the continuation shows as the monitor
//! reporting new output (`isClaudeMonitorEventWake`).
//!
//! The tasks running now are the provider thread's `pendingBackgroundTasks`
//! (`publishedPendingBackgroundTasks`): each with the command and tool call that
//! launched it, and a monitor marked awaited.
use crate::background_work::{self, Kind, Report};
use indexmap::IndexMap;
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet};

/// What a wake report's child thread starts with when the workspace knows only the Claude
/// task: the control plane names the child thread from it and the thread's provider thread.
pub(crate) const CLAUDE_TASK_CHILD: &str = "claude-task:";

/// `CLAUDE_OPAQUE_BACKGROUND_TASK_KINDS`: the task types that report their own end.
fn opaque_kind(task_type: Option<&str>) -> Option<Kind> {
    match task_type? {
        "local_bash" => Some(Kind::Command),
        _ => None,
    }
}

#[derive(Clone, Debug, Default)]
struct Task {
    kind: Kind,
    description: Option<String>,
    /// The tool call that launched it (`nativeItemId`).
    tool_use_id: Option<String>,
    /// The launching `Bash` or `Monitor` call's command (`backgroundTaskLaunch`).
    command: Option<String>,
}

impl Task {
    /// The task as the provider thread lists it, without what the projection adds
    /// (its item, thread and run).
    fn published(&self, task_id: &str, awaited: bool) -> Value {
        let mut task = json!({"taskId": task_id, "kind": self.kind.wire()});
        if let Some(description) = &self.description {
            task["description"] = json!(description);
        }
        if let Some(id) = &self.tool_use_id {
            task["nativeItemId"] = json!(id);
        }
        if let Some(command) = &self.command {
            task["command"] = json!(command);
        }
        if awaited {
            task["awaited"] = json!(true);
        }
        task
    }
}

/// One session's background tasks.
#[derive(Debug, Default)]
pub(crate) struct Roster {
    /// The tasks running now, by id, in the order they started.
    pending: IndexMap<String, Task>,
    /// Running tasks Claude resumes after (a monitor it started:
    /// `awaitedBackgroundTasksByNativeThread`).
    awaited: HashSet<String>,
    /// The current turn's `Bash` and `Monitor` calls' commands, by tool call
    /// (`activeContext.toolCalls`).
    tool_commands: HashMap<String, String>,
    /// The tool call each task started from (`toolUseIdsByTaskId`).
    tool_use_ids: HashMap<String, String>,
    /// Tasks whose end wakes the session (`wakeEligibleBackgroundTasksByNativeThread`).
    eligible: HashSet<String>,
    /// Tasks seen since, for a notification that comes after the roster dropped them
    /// (`lastKnownOpaqueTasks`).
    known: HashMap<String, Task>,
    /// What ended while no turn ran, for the next wake.
    reports: Vec<Report>,
    /// The last such notification's summary, the wake's text.
    detail: Option<String>,
    /// `Monitor` tool calls whose task has not started yet (`claudeMonitors.calls`).
    monitor_calls: HashSet<String>,
    /// The monitors running now, by task id (`claudeMonitors.tasks`).
    monitors: HashSet<String>,
    /// The session started a turn on its own while a monitor ran: a monitor event.
    monitor_wake: bool,
    /// Background subagents running now (`backgroundedSubagentTaskIds`), by task id: their
    /// description, which labels the wake their end starts.
    subagents: HashMap<String, Option<String>>,
    /// The tasks last sent to the provider thread.
    sent: Vec<Value>,
}

impl Roster {
    fn upsert(&mut self, task_id: &str, task: Task) {
        self.pending.insert(task_id.to_owned(), task.clone());
        self.known.insert(task_id.to_owned(), task);
        self.eligible.insert(task_id.to_owned());
    }

    /// `removePendingBackgroundTask`: a task that ended leaves the roster.
    fn remove(&mut self, task_id: &str) {
        self.pending.shift_remove(task_id);
        self.awaited.remove(task_id);
        self.tool_use_ids.remove(task_id);
    }

    /// The running tasks, as the provider thread lists them.
    pub(crate) fn published(&self) -> Vec<Value> {
        self.pending
            .iter()
            .map(|(id, task)| task.published(id, self.awaited.contains(id)))
            .collect()
    }

    /// The running tasks, when a turn's frame changed them (`emitRosterUpdate`).
    pub(crate) fn changed(&mut self, before: &[Value]) -> Option<Vec<Value>> {
        let tasks = self.published();
        if tasks == before {
            return None;
        }
        self.sent = tasks.clone();
        Some(tasks)
    }

    /// The running tasks, when they differ from what was last sent: a task that
    /// ended while the session was idle leaves with the turn's completed provider
    /// thread.
    pub(crate) fn unsent(&mut self) -> Option<Vec<Value>> {
        let tasks = self.published();
        if tasks == self.sent {
            return None;
        }
        self.sent = tasks.clone();
        Some(tasks)
    }

    /// `backgroundTaskLaunch`: the command of the turn's tool call that launched a task.
    fn launch_command(&self, tool_use_id: Option<&str>) -> Option<String> {
        self.tool_commands.get(tool_use_id?).cloned()
    }

    /// A frame of a running turn: tasks it starts become eligible; one it sees end does
    /// not wake the session.
    pub(crate) fn observe_turn(&mut self, message: &Value) {
        self.observe_subagent_start(message);
        // A background subagent that ends during a turn wakes nothing.
        if message["type"] == "system" && message["subtype"] == "task_notification" {
            if let Some(task_id) = message["task_id"].as_str() {
                self.subagents.remove(task_id);
            }
        }
        // A turn's tool calls are its own (`activeContext.toolCalls`).
        if message["type"] == "system" && message["subtype"] == "init" {
            self.tool_commands.clear();
        }
        self.track_monitor_calls(message);
        self.observe_roster(message);
        if message["type"] == "system" && message["subtype"] == "task_notification" {
            if let Some(task_id) = message["task_id"].as_str() {
                self.remove(task_id);
                self.eligible.remove(task_id);
                self.monitors.remove(task_id);
            }
        }
    }

    /// A subagent that runs in the background (`task_started`, `local_agent`, backgrounded).
    fn observe_subagent_start(&mut self, message: &Value) {
        if message["type"] == "system"
            && message["subtype"] == "task_started"
            && message["task_type"] == "local_agent"
            && message["is_backgrounded"] == true
        {
            if let Some(task_id) = message["task_id"].as_str() {
                let label = message["description"]
                    .as_str()
                    .filter(|d| !d.trim().is_empty())
                    .map(str::to_owned);
                self.subagents.insert(task_id.to_owned(), label);
            }
        }
    }

    /// `trackClaudeMonitorCalls`: a `Monitor` tool call opens until its result returns.
    fn track_monitor_calls(&mut self, message: &Value) {
        let blocks = message["message"]["content"].as_array();
        for block in blocks.into_iter().flatten() {
            match block["type"].as_str() {
                Some("tool_use") => {
                    let (Some(id), Some(name)) = (block["id"].as_str(), block["name"].as_str())
                    else {
                        continue;
                    };
                    if name == "Monitor" {
                        self.monitor_calls.insert(id.to_owned());
                    }
                    if let Some(command) = block["input"]["command"]
                        .as_str()
                        .filter(|_| matches!(name, "Bash" | "Monitor"))
                    {
                        self.tool_commands.insert(id.to_owned(), command.to_owned());
                    }
                }
                Some("tool_result") => {
                    if let Some(id) = block["tool_use_id"].as_str() {
                        self.monitor_calls.remove(id);
                    }
                }
                _ => {}
            }
        }
    }

    /// A frame the session wrote while no turn ran: an eligible task's end is recorded
    /// as a report for the wake it starts.
    pub(crate) fn observe_idle(&mut self, message: &Value) {
        // A turn the session starts on its own while a monitor runs is a monitor event
        // (`monitorEventWakeStarts`).
        if message["type"] == "system" && message["subtype"] == "init" && !self.monitors.is_empty()
        {
            self.monitor_wake = true;
        }
        if message["type"] == "system" && message["subtype"] == "task_notification" {
            let Some(task_id) = message["task_id"].as_str() else {
                return;
            };
            // A background subagent that ends while no turn runs is reported with its wake;
            // the control plane names its child thread (by the session's provider thread).
            if let Some(label) = self.subagents.remove(task_id) {
                let status = message["status"].as_str().unwrap_or_default();
                self.reports.push(Report {
                    kind: Kind::Subagent,
                    label,
                    outcome: background_work::claude_task_outcome(status),
                    child_thread_id: Some(format!("{CLAUDE_TASK_CHILD}{task_id}")),
                    exit_code: None,
                });
                if let Some(summary) = message["summary"].as_str().filter(|s| !s.is_empty()) {
                    self.detail = Some(summary.to_owned());
                }
                return;
            }
            self.monitors.remove(task_id);
            let pending = self.pending.get(task_id).cloned();
            self.remove(task_id);
            if !self.eligible.remove(task_id) {
                return;
            }
            let task = pending.or_else(|| self.known.remove(task_id));
            self.known.remove(task_id);
            let status = message["status"].as_str().unwrap_or_default();
            self.reports.push(Report {
                kind: task.as_ref().map_or(Kind::Command, |t| t.kind),
                label: task.and_then(|t| t.description),
                outcome: background_work::claude_task_outcome(status),
                child_thread_id: None,
                exit_code: None,
            });
            if let Some(summary) = message["summary"].as_str().filter(|s| !s.is_empty()) {
                self.detail = Some(summary.to_owned());
            }
            return;
        }
        self.observe_roster(message);
    }

    fn observe_roster(&mut self, message: &Value) {
        if message["type"] != "system" {
            return;
        }
        match message["subtype"].as_str() {
            Some("task_started") if message["is_backgrounded"] != false => {
                let (Some(task_id), Some(kind)) = (
                    message["task_id"].as_str().filter(|id| !id.is_empty()),
                    opaque_kind(message["task_type"].as_str()),
                ) else {
                    return;
                };
                let tool_use_id = message["tool_use_id"]
                    .as_str()
                    .map(str::trim)
                    .filter(|id| !id.is_empty())
                    .map(str::to_owned);
                if let Some(id) = &tool_use_id {
                    self.tool_use_ids.insert(task_id.to_owned(), id.clone());
                }
                // Only the launching `Monitor` call tells a monitor from a background
                // command (`isClaudeMonitorTask`).
                let started_by_monitor = self.monitors.contains(task_id)
                    || message["tool_use_id"]
                        .as_str()
                        .is_some_and(|id| self.monitor_calls.remove(id));
                let kind = if started_by_monitor {
                    self.monitors.insert(task_id.to_owned());
                    Kind::Monitor
                } else {
                    kind
                };
                // A monitor that starts is one Claude resumes after
                // (`upsertPendingBackgroundTask`).
                if kind == Kind::Monitor
                    && self.pending.get(task_id).map(|t| t.kind) != Some(Kind::Monitor)
                {
                    self.awaited.insert(task_id.to_owned());
                }
                let description = message["description"]
                    .as_str()
                    .filter(|d| !d.trim().is_empty())
                    .map(str::to_owned);
                let command = self.launch_command(tool_use_id.as_deref());
                self.upsert(
                    task_id,
                    Task {
                        kind,
                        description,
                        tool_use_id,
                        command,
                    },
                );
            }
            // A level: the tasks running now (`replacePendingBackgroundTasks`).
            Some("background_tasks_changed") => {
                let tasks: Vec<(String, Task)> = message["tasks"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .filter_map(|entry| {
                        let task_id = entry["task_id"].as_str().filter(|id| !id.is_empty())?;
                        let kind = opaque_kind(entry["task_type"].as_str())?;
                        let kind = if self.monitors.contains(task_id) {
                            Kind::Monitor
                        } else {
                            kind
                        };
                        // What the roster names over what the task had
                        // (`{ ...previous, ...task }`).
                        let previous = self.pending.get(task_id).cloned().unwrap_or_default();
                        let description = entry["description"]
                            .as_str()
                            .filter(|d| !d.trim().is_empty())
                            .map(str::to_owned)
                            .or(previous.description);
                        let tool_use_id = self
                            .tool_use_ids
                            .get(task_id)
                            .cloned()
                            .or(previous.tool_use_id);
                        let command = self
                            .launch_command(tool_use_id.as_deref())
                            .or(previous.command);
                        Some((
                            task_id.to_owned(),
                            Task {
                                kind,
                                description,
                                tool_use_id,
                                command,
                            },
                        ))
                    })
                    .collect();
                self.pending.clear();
                let listed: HashSet<&String> = tasks.iter().map(|(id, _)| id).collect();
                self.awaited.retain(|id| listed.contains(id));
                for (task_id, task) in tasks {
                    self.upsert(&task_id, task);
                }
            }
            _ => {}
        }
    }

    /// The wake's text and notification from what ended since the last wake, which
    /// starts the next one empty.
    /// A wake that reports nothing that ended, started while a monitor ran, is the
    /// monitor reporting new output (a recorded report names finished work, which
    /// outranks Monitor progress).
    pub(crate) fn take_wake(&mut self) -> (Option<String>, Option<Value>) {
        let mut reports = std::mem::take(&mut self.reports);
        let monitor_wake = std::mem::take(&mut self.monitor_wake);
        if reports.is_empty() && monitor_wake {
            reports.push(Report {
                kind: Kind::Monitor,
                label: None,
                outcome: "updated",
                child_thread_id: None,
                exit_code: None,
            });
        }
        (self.detail.take(), background_work::notification(&reports))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn started(id: &str) -> Value {
        json!({"type": "system", "subtype": "task_started", "task_id": id,
            "description": "npm test", "is_backgrounded": true, "task_type": "local_bash"})
    }

    fn task_notification(id: &str, status: &str) -> Value {
        json!({"type": "system", "subtype": "task_notification", "task_id": id,
            "status": status, "summary": "Background command \"npm test\" completed (exit code 0)"})
    }

    #[test]
    fn a_command_that_ends_while_idle_is_reported_with_the_wake() {
        let mut roster = Roster::default();
        roster.observe_turn(&started("b1"));
        roster.observe_turn(
            &json!({"type": "system", "subtype": "background_tasks_changed",
            "tasks": [{"task_id": "b1", "task_type": "local_bash", "description": "npm test"}]}),
        );
        roster.observe_idle(&task_notification("b1", "completed"));
        roster.observe_idle(
            &json!({"type": "system", "subtype": "background_tasks_changed",
            "tasks": []}),
        );
        let (detail, notification) = roster.take_wake();
        assert_eq!(
            detail.as_deref(),
            Some("Background command \"npm test\" completed (exit code 0)")
        );
        assert_eq!(
            notification,
            Some(
                json!({"source": {"kind": "command"}, "outcome": "completed",
                "summary": "Command \"npm test\" finished"})
            )
        );
        // Replayed by the continuation turn, it reports nothing again.
        roster.observe_turn(&task_notification("b1", "completed"));
        assert_eq!(roster.take_wake(), (None, None));
    }

    #[test]
    fn a_command_that_ends_during_a_turn_wakes_nothing() {
        let mut roster = Roster::default();
        roster.observe_turn(&started("b1"));
        roster.observe_turn(&task_notification("b1", "completed"));
        roster.observe_idle(&task_notification("b1", "completed"));
        assert_eq!(roster.take_wake(), (None, None));
    }

    #[test]
    fn a_monitor_event_wake_reports_new_output() {
        let mut roster = Roster::default();
        roster.observe_turn(&json!({"type": "assistant", "message": {"content": [
            {"type": "tool_use", "id": "toolu_m1", "name": "Monitor", "input": {}}]}}));
        roster.observe_turn(&json!({"type": "system", "subtype": "task_started",
            "task_id": "m1", "tool_use_id": "toolu_m1", "description": "tail -f build.log",
            "is_backgrounded": true, "task_type": "local_bash"}));
        roster.observe_turn(&json!({"type": "user", "message": {"content": [
            {"type": "tool_result", "tool_use_id": "toolu_m1", "content": "Monitoring."}]}}));
        roster.observe_idle(&json!({"type": "system", "subtype": "init"}));
        assert_eq!(
            roster.take_wake(),
            (
                None,
                Some(json!({"source": {"kind": "monitor"}, "outcome": "updated",
                    "summary": "Monitor reported new output"}))
            )
        );
        // The monitor ending reports it as finished work, not an event.
        roster.observe_idle(&json!({"type": "system", "subtype": "task_notification",
            "task_id": "m1", "status": "completed", "summary": ""}));
        roster.observe_idle(&json!({"type": "system", "subtype": "init"}));
        assert_eq!(
            roster.take_wake().1.unwrap()["summary"],
            "Monitor \"tail -f build.log\" finished"
        );
    }

    #[test]
    fn running_tasks_are_listed_with_what_launched_them() {
        let mut roster = Roster::default();
        roster.observe_turn(&json!({"type": "assistant", "message": {"content": [
            {"type": "tool_use", "id": "toolu_m1", "name": "Monitor",
             "input": {"command": "tail -f build.log"}},
            {"type": "tool_use", "id": "toolu_b1", "name": "Bash",
             "input": {"command": "npm test", "run_in_background": true}}]}}));
        let before = roster.published();
        roster.observe_turn(&json!({"type": "system", "subtype": "task_started",
            "task_id": "m1", "tool_use_id": "toolu_m1", "description": "tail -f build.log",
            "is_backgrounded": true, "task_type": "local_bash"}));
        let monitor = json!({"taskId": "m1", "kind": "monitor", "description": "tail -f build.log",
            "nativeItemId": "toolu_m1", "command": "tail -f build.log", "awaited": true});
        assert_eq!(roster.changed(&before), Some(vec![monitor.clone()]));
        // A level lists both; the command is not awaited.
        let before = roster.published();
        roster.observe_turn(&json!({"type": "system", "subtype": "task_started",
            "task_id": "b1", "tool_use_id": "toolu_b1", "description": "npm test",
            "is_backgrounded": true, "task_type": "local_bash"}));
        roster.observe_turn(
            &json!({"type": "system", "subtype": "background_tasks_changed",
            "tasks": [{"task_id": "m1", "task_type": "local_bash"},
                      {"task_id": "b1", "task_type": "local_bash", "description": "npm test"}]}),
        );
        let command = json!({"taskId": "b1", "kind": "command", "description": "npm test",
            "nativeItemId": "toolu_b1", "command": "npm test"});
        assert_eq!(
            roster.changed(&before),
            Some(vec![monitor.clone(), command])
        );
        // The command ends while idle: the next turn's end reports the list without it.
        roster.observe_idle(&task_notification("b1", "completed"));
        let before = roster.published();
        roster.observe_turn(&json!({"type": "system", "subtype": "init"}));
        assert_eq!(roster.changed(&before), None);
        assert_eq!(roster.unsent(), Some(vec![monitor]));
        assert_eq!(roster.unsent(), None);
    }

    #[test]
    fn foreground_and_other_tasks_are_not_tracked() {
        let mut roster = Roster::default();
        let mut foreground = started("b1");
        foreground["is_backgrounded"] = json!(false);
        roster.observe_turn(&foreground);
        // A subagent in the foreground, and a task of another type.
        roster.observe_turn(&json!({"type": "system", "subtype": "task_started",
            "task_id": "a1", "task_type": "local_agent", "is_backgrounded": false}));
        roster.observe_turn(&json!({"type": "system", "subtype": "task_started",
            "task_id": "w1", "task_type": "local_workflow", "is_backgrounded": true}));
        roster.observe_idle(&task_notification("b1", "completed"));
        roster.observe_idle(&task_notification("a1", "completed"));
        roster.observe_idle(&task_notification("w1", "completed"));
        assert_eq!(roster.take_wake(), (None, None));
    }

    #[test]
    fn a_background_subagent_that_ends_while_idle_is_reported_with_the_wake() {
        let mut roster = Roster::default();
        roster.observe_turn(
            &json!({"type": "system", "subtype": "task_started", "task_id": "a1",
            "task_type": "local_agent", "is_backgrounded": true, "description": "Read the README"}),
        );
        roster.observe_idle(&json!({"type": "system", "subtype": "task_notification",
            "task_id": "a1", "status": "completed", "summary": "Read the README"}));
        let (detail, notification) = roster.take_wake();
        assert_eq!(detail.as_deref(), Some("Read the README"));
        let notification = notification.unwrap();
        assert_eq!(notification["source"]["kind"], "subagent");
        assert_eq!(
            notification["source"]["childThreadId"],
            format!("{CLAUDE_TASK_CHILD}a1")
        );
        assert_eq!(
            notification["summary"],
            "Subagent \"Read the README\" finished"
        );
        // One that ends during a turn wakes nothing.
        roster.observe_turn(
            &json!({"type": "system", "subtype": "task_started", "task_id": "a2",
            "task_type": "local_agent", "is_backgrounded": true}),
        );
        roster.observe_turn(&json!({"type": "system", "subtype": "task_notification",
            "task_id": "a2", "status": "completed"}));
        roster.observe_idle(&json!({"type": "system", "subtype": "task_notification",
            "task_id": "a2", "status": "completed"}));
        assert_eq!(roster.take_wake(), (None, None));
    }
}
