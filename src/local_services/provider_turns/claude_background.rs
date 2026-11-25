//! The background tasks a Claude session runs that report their own end (`local_bash`
//! commands), as the reference's adapter keeps them per native session: started by a
//! turn (`task_started`, `background_tasks_changed`), and when one ends while no turn
//! runs (`task_notification`), reported with the wake it starts, so the continuation run
//! names the command and how it ended (`opaqueTaskWakeReport`).
//!
//! A command a `Monitor` tool call starts is a monitor: its events wake Claude with a
//! bare turn and no `task_notification`, which the continuation shows as the monitor
//! reporting new output (`isClaudeMonitorEventWake`).
use crate::background_work::{self, Kind, Report};
use serde_json::Value;
use std::collections::{HashMap, HashSet};

/// `CLAUDE_OPAQUE_BACKGROUND_TASK_KINDS`: the task types that report their own end.
fn opaque_kind(task_type: Option<&str>) -> Option<Kind> {
    match task_type? {
        "local_bash" => Some(Kind::Command),
        _ => None,
    }
}

#[derive(Clone, Debug)]
struct Task {
    kind: Kind,
    description: Option<String>,
}

/// One session's background tasks.
#[derive(Debug, Default)]
pub(crate) struct Roster {
    /// The tasks running now, by id.
    pending: HashMap<String, Task>,
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
}

impl Roster {
    fn upsert(&mut self, task_id: &str, task: Task) {
        self.pending.insert(task_id.to_owned(), task.clone());
        self.known.insert(task_id.to_owned(), task);
        self.eligible.insert(task_id.to_owned());
    }

    /// A frame of a running turn: tasks it starts become eligible; one it sees end does
    /// not wake the session.
    pub(crate) fn observe_turn(&mut self, message: &Value) {
        self.track_monitor_calls(message);
        self.observe_roster(message);
        if message["type"] == "system" && message["subtype"] == "task_notification" {
            if let Some(task_id) = message["task_id"].as_str() {
                self.pending.remove(task_id);
                self.eligible.remove(task_id);
                self.monitors.remove(task_id);
            }
        }
    }

    /// `trackClaudeMonitorCalls`: a `Monitor` tool call opens until its result returns.
    fn track_monitor_calls(&mut self, message: &Value) {
        let blocks = message["message"]["content"].as_array();
        for block in blocks.into_iter().flatten() {
            match block["type"].as_str() {
                Some("tool_use") if block["name"] == "Monitor" => {
                    if let Some(id) = block["id"].as_str() {
                        self.monitor_calls.insert(id.to_owned());
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
            self.monitors.remove(task_id);
            if !self.eligible.remove(task_id) {
                return;
            }
            let task = self
                .pending
                .remove(task_id)
                .or_else(|| self.known.remove(task_id));
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
                let description = message["description"]
                    .as_str()
                    .filter(|d| !d.trim().is_empty())
                    .map(str::to_owned);
                self.upsert(task_id, Task { kind, description });
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
                        let description = entry["description"]
                            .as_str()
                            .filter(|d| !d.trim().is_empty())
                            .map(str::to_owned)
                            .or_else(|| {
                                self.pending
                                    .get(task_id)
                                    .and_then(|t| t.description.clone())
                            });
                        Some((task_id.to_owned(), Task { kind, description }))
                    })
                    .collect();
                self.pending.clear();
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
    fn foreground_and_other_tasks_are_not_tracked() {
        let mut roster = Roster::default();
        let mut foreground = started("b1");
        foreground["is_backgrounded"] = json!(false);
        roster.observe_turn(&foreground);
        roster.observe_turn(&json!({"type": "system", "subtype": "task_started",
            "task_id": "a1", "task_type": "local_agent", "is_backgrounded": true}));
        roster.observe_idle(&task_notification("b1", "completed"));
        roster.observe_idle(&task_notification("a1", "completed"));
        assert_eq!(roster.take_wake(), (None, None));
    }
}
