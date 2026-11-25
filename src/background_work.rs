//! The notification a wake shows for the background work it reports, as the reference's
//! `backgroundWorkNotification` (`provider-core/server/notification.ts`) builds it: the
//! source, the combined outcome, and a summary naming each piece of work and how it ended
//! ("Command "npm test" finished").
use serde_json::{json, Value};

/// What kind of background work a report is about.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Kind {
    /// A subagent's report; Rust's Claude adapter does not track background
    /// subagents yet.
    #[allow(dead_code)]
    Subagent,
    Command,
    Monitor,
    BackgroundTask,
}

impl Kind {
    fn nouns(self) -> (&'static str, &'static str) {
        match self {
            Kind::Subagent => ("Subagent", "subagents"),
            Kind::Command => ("Command", "commands"),
            Kind::Monitor => ("Monitor", "monitors"),
            Kind::BackgroundTask => ("Background task", "background tasks"),
        }
    }

    fn wire(self) -> &'static str {
        match self {
            Kind::Subagent => "subagent",
            Kind::Command => "command",
            Kind::Monitor => "monitor",
            Kind::BackgroundTask => "background_task",
        }
    }
}

/// One piece of background work and how it ended (`BackgroundWorkReport`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Report {
    pub kind: Kind,
    pub label: Option<String>,
    /// `completed`, `failed`, `cancelled`, `updated` or `unknown`.
    pub outcome: &'static str,
    pub child_thread_id: Option<String>,
    pub exit_code: Option<i64>,
}

const LABEL_MAX_LENGTH: usize = 80;

/// `reportLabel`: the label's first line, cut to 80 UTF-16 units with an ellipsis.
pub(crate) fn report_label(label: Option<&str>) -> Option<String> {
    let first = label?.trim().split('\n').next()?.trim();
    if first.is_empty() {
        return None;
    }
    let units: Vec<u16> = first.encode_utf16().collect();
    if units.len() <= LABEL_MAX_LENGTH {
        return Some(first.to_owned());
    }
    let mut end = LABEL_MAX_LENGTH - 1;
    // Do not split a surrogate pair.
    if (0xd800..=0xdbff).contains(&units[end - 1]) && (0xdc00..=0xdfff).contains(&units[end]) {
        end -= 1;
    }
    Some(format!("{}…", String::from_utf16_lossy(&units[..end])))
}

fn outcome_verb(kind: Kind, outcome: &str) -> &'static str {
    match outcome {
        "failed" => "failed",
        "cancelled" => "was stopped",
        "updated" if kind == Kind::Monitor => "reported new output",
        "updated" => "updated",
        _ => "finished",
    }
}

fn combined_outcome(outcomes: &[&'static str]) -> &'static str {
    if outcomes.contains(&"failed") {
        "failed"
    } else if outcomes.contains(&"cancelled") {
        "cancelled"
    } else if !outcomes.is_empty() && outcomes.iter().all(|o| *o == "completed") {
        "completed"
    } else if outcomes.contains(&"updated") {
        "updated"
    } else {
        "unknown"
    }
}

fn exit_suffix(report: &Report) -> String {
    match (report.kind, report.exit_code) {
        (Kind::Command, Some(code)) => format!(" (exit {code})"),
        _ => String::new(),
    }
}

fn named(report: &Report, capitalize: bool, verb: Option<&str>) -> String {
    let noun = report.kind.nouns().0;
    let noun = if capitalize {
        noun.to_owned()
    } else {
        noun.to_lowercase()
    };
    let label = report_label(report.label.as_deref())
        .map(|label| format!(" \"{label}\""))
        .unwrap_or_default();
    let verb = verb.map(|verb| format!(" {verb}")).unwrap_or_default();
    format!("{noun}{label}{verb}{}", exit_suffix(report))
}

const OUTCOME_GROUPS: [&str; 4] = ["failed", "cancelled", "updated", "completed"];

fn group_verb(group: &str, count: usize) -> &'static str {
    match group {
        "failed" => "failed",
        "cancelled" if count == 1 => "was stopped",
        "cancelled" => "were stopped",
        "updated" => "updated",
        _ => "finished",
    }
}

fn join_names(names: &[String]) -> String {
    match names {
        [] => String::new(),
        [one] => one.clone(),
        [rest @ .., last] => format!("{} and {last}", rest.join(", ")),
    }
}

fn summary(reports: &[Report]) -> String {
    let first = &reports[0];
    if reports.len() == 1 {
        let label = report_label(first.label.as_deref())
            .map(|label| format!(" \"{label}\""))
            .unwrap_or_default();
        return format!(
            "{}{label} {}{}",
            first.kind.nouns().0,
            outcome_verb(first.kind, first.outcome),
            exit_suffix(first)
        );
    }
    let groups: Vec<(&str, usize)> = OUTCOME_GROUPS
        .iter()
        .filter_map(|group| {
            let count = reports
                .iter()
                .filter(|r| {
                    let outcome = if r.outcome == "unknown" {
                        "completed"
                    } else {
                        r.outcome
                    };
                    outcome == *group
                })
                .count();
            (count > 0).then_some((*group, count))
        })
        .collect();
    if reports.len() > 3
        || reports
            .iter()
            .all(|r| report_label(r.label.as_deref()).is_none())
    {
        let same_kind = reports.iter().all(|r| r.kind == first.kind);
        let (noun, nouns) = if same_kind {
            first.kind.nouns()
        } else {
            Kind::BackgroundTask.nouns()
        };
        let parts: Vec<String> = groups
            .iter()
            .enumerate()
            .map(|(index, (group, count))| {
                let what = if index == 0 {
                    if *count == 1 {
                        format!(" {}", noun.to_lowercase())
                    } else {
                        format!(" {nouns}")
                    }
                } else {
                    String::new()
                };
                format!("{count}{what} {}", group_verb(group, *count))
            })
            .collect();
        return join_names(&parts);
    }
    if groups.len() > 1 {
        let parts: Vec<String> = reports
            .iter()
            .enumerate()
            .map(|(index, r)| named(r, index == 0, Some(outcome_verb(r.kind, r.outcome))))
            .collect();
        return join_names(&parts);
    }
    let parts: Vec<String> = reports
        .iter()
        .enumerate()
        .map(|(index, r)| named(r, index == 0, None))
        .collect();
    format!(
        "{} {}",
        join_names(&parts),
        group_verb(groups[0].0, reports.len())
    )
}

fn source(reports: &[Report]) -> Value {
    let first = &reports[0];
    if reports[1..].iter().any(|r| r.kind != first.kind) {
        return json!({"kind": "background_task"});
    }
    match (first.kind, &first.child_thread_id) {
        (Kind::Subagent, Some(child)) if reports.len() == 1 => {
            json!({"kind": "subagent", "childThreadId": child})
        }
        (kind, _) => json!({"kind": kind.wire()}),
    }
}

/// `backgroundWorkNotification`: the notification for `reports`, or None for none.
pub(crate) fn notification(reports: &[Report]) -> Option<Value> {
    if reports.is_empty() {
        return None;
    }
    let outcomes: Vec<&'static str> = reports.iter().map(|r| r.outcome).collect();
    Some(json!({
        "source": source(reports),
        "outcome": combined_outcome(&outcomes),
        "summary": summary(reports),
    }))
}

/// `claudeTaskOutcome`: a Claude task notification's status as an outcome.
pub(crate) fn claude_task_outcome(status: &str) -> &'static str {
    match status {
        "completed" => "completed",
        "stopped" => "cancelled",
        _ => "failed",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn command(label: &str, outcome: &'static str) -> Report {
        Report {
            kind: Kind::Command,
            label: Some(label.into()),
            outcome,
            child_thread_id: None,
            exit_code: None,
        }
    }

    #[test]
    fn one_report_names_its_work_and_how_it_ended() {
        assert_eq!(
            notification(&[command("npm test", "completed")]),
            Some(
                json!({"source": {"kind": "command"}, "outcome": "completed",
                "summary": "Command \"npm test\" finished"})
            )
        );
        assert_eq!(
            notification(&[command("npm test", "cancelled")]).unwrap()["summary"],
            "Command \"npm test\" was stopped"
        );
        let monitor = Report {
            kind: Kind::Monitor,
            label: None,
            outcome: "updated",
            child_thread_id: None,
            exit_code: None,
        };
        assert_eq!(
            notification(&[monitor]).unwrap()["summary"],
            "Monitor reported new output"
        );
        assert_eq!(notification(&[]), None);
    }

    #[test]
    fn several_reports_are_grouped_as_the_reference_groups_them() {
        let both = [command("build", "completed"), command("lint", "completed")];
        assert_eq!(
            notification(&both).unwrap()["summary"],
            "Command \"build\" and command \"lint\" finished"
        );
        let mixed = [command("build", "completed"), command("lint", "failed")];
        let n = notification(&mixed).unwrap();
        assert_eq!(
            n["summary"],
            "Command \"build\" finished and command \"lint\" failed"
        );
        assert_eq!(n["outcome"], "failed");
        let many: Vec<Report> = (0..4)
            .map(|i| command(&format!("job {i}"), "completed"))
            .collect();
        assert_eq!(
            notification(&many).unwrap()["summary"],
            "4 commands finished"
        );
    }

    #[test]
    fn labels_keep_their_first_line_and_eighty_units() {
        assert_eq!(report_label(Some("  first\nsecond")), Some("first".into()));
        assert_eq!(report_label(Some("   ")), None);
        let long = "x".repeat(90);
        assert_eq!(report_label(Some(&long)).unwrap().chars().count(), 80);
    }
}
