//! The background tasks a Claude session runs that report their own end (`local_bash`
//! commands), as the reference's adapter keeps them per native session: started by a
//! turn (`task_started`, `background_tasks_changed`), and when one ends while no turn
//! runs (`task_notification`), reported with the wake it starts, so the continuation run
//! names the command and how it ended (`opaqueTaskWakeReport`).
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
        self.observe_roster(message);
        if message["type"] == "system" && message["subtype"] == "task_notification" {
            if let Some(task_id) = message["task_id"].as_str() {
                self.pending.remove(task_id);
                self.eligible.remove(task_id);
            }
        }
    }

    /// A frame the session wrote while no turn ran: an eligible task's end is recorded
    /// as a report for the wake it starts.
    pub(crate) fn observe_idle(&mut self, message: &Value) {
        if message["type"] == "system" && message["subtype"] == "task_notification" {
            let Some(task_id) = message["task_id"].as_str() else {
                return;
            };
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
    pub(crate) fn take_wake(&mut self) -> (Option<String>, Option<Value>) {
        let reports = std::mem::take(&mut self.reports);
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
