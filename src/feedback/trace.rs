//! The reference's `FeedbackTrace`: the outline of a thread's last few
//! started turns that a requested feedback report carries, who spoke and
//! which tools ran with no text, input or output, and with `with_text` also
//! the scrubbed message text of the turns the problem spans. Built from the
//! thread's projected runs and turn items.
use super::scrub::scrub_text;
use serde_json::{json, Value};

/// `FEEDBACK_TRACE_TURNS`: turns the outline covers at least.
pub(crate) const TRACE_TURNS: u64 = 5;
const MAX_ENTRIES: usize = 80;
const MAX_TEXT_CHARS: usize = 1_000;
const MAX_BYTES: usize = 16_000;
/// `FEEDBACK_TRACE_ITEM_TYPES`.
pub(crate) const ITEM_TYPES: [&str; 8] = [
    "user_message",
    "assistant_message",
    "dynamic_tool",
    "command_execution",
    "file_change",
    "file_search",
    "web_search",
    "subagent",
];

/// `feedbackTraceWindow`.
pub(crate) fn window(turns: u64) -> u64 {
    turns.max(TRACE_TURNS).min(MAX_ENTRIES as u64)
}

fn built_in_name(name: &str) -> bool {
    let mut chars = name.chars();
    chars.next().is_some_and(|c| c.is_ascii_alphabetic())
        && name.len() <= 64
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
}

fn tool_name(item: &Value, t3_tools: &[String], names_built_ins: bool) -> String {
    match item["type"].as_str().unwrap_or_default() {
        "command_execution" => return "shell".into(),
        "dynamic_tool" => {}
        other => return other.to_owned(),
    }
    let name = item["toolName"].as_str().unwrap_or_default();
    if let Some(t3) = super::resolve_t3_tool_name(name, t3_tools) {
        return t3;
    }
    let mcp = name.len() >= 3 && name[..3].eq_ignore_ascii_case("mcp");
    if names_built_ins && item["toolSource"]["kind"] != "integration" && !mcp && built_in_name(name)
    {
        name.to_owned()
    } else {
        "mcp".into()
    }
}

fn outcome(item: &Value) -> &'static str {
    match item["status"].as_str() {
        Some("failed") => "error",
        Some("completed") => {
            let failed = item["type"] == "command_execution"
                && (item["exitCode"].as_i64().unwrap_or(0) != 0
                    || item["outputIndicatesFailure"] == true);
            if failed {
                "error"
            } else {
                "ok"
            }
        }
        _ => "unfinished",
    }
}

fn millis(value: &Value) -> Option<i64> {
    let text = value.as_str()?;
    let parsed =
        time::OffsetDateTime::parse(text, &time::format_description::well_known::Rfc3339).ok()?;
    Some((parsed.unix_timestamp_nanos() / 1_000_000) as i64)
}

/// What `buildFeedbackTrace` takes.
pub(crate) struct TraceInput<'a> {
    pub runs: &'a [Value],
    pub items: &'a [Value],
    pub turns: u64,
    pub with_text: bool,
    pub sensitive: &'a [String],
    pub t3_tools: &'a [String],
    /// The driver of a run's provider instance.
    pub driver_of: &'a dyn Fn(&Value) -> Option<String>,
    pub submitting_run_id: &'a str,
}

/// `buildFeedbackTrace`.
pub(crate) fn build(input: &TraceInput) -> Value {
    let window = window(input.turns) as usize;
    let mut started: Vec<&Value> = input
        .runs
        .iter()
        .filter(|run| !run["startedAt"].is_null())
        .collect();
    started.sort_by_key(|run| run["ordinal"].as_u64().unwrap_or(0));
    let runs: Vec<&Value> = started[started.len().saturating_sub(window)..].to_vec();
    let first_text_turn = if input.with_text {
        runs.len() as i64 - input.turns as i64 + 1
    } else {
        i64::MAX
    };
    let turn_of = |run_id: &Value| {
        runs.iter()
            .position(|run| run["id"] == *run_id)
            .map(|index| (index + 1, runs[index]))
    };
    let submitting = |item: &Value| {
        item["runId"] == input.submitting_run_id
            && item["type"] == "dynamic_tool"
            && outcome(item) == "unfinished"
            && super::resolve_t3_tool_name(
                item["toolName"].as_str().unwrap_or_default(),
                input.t3_tools,
            )
            .as_deref()
                == Some("t3_feedback")
    };
    let mut truncated = input.turns as usize > window && started.len() >= window;
    let mut picked: Vec<(usize, &Value, &Value)> = input
        .items
        .iter()
        .filter(|item| {
            item["parentItemId"].is_null() && !item["runId"].is_null() && !submitting(item)
        })
        .filter_map(|item| turn_of(&item["runId"]).map(|(turn, run)| (turn, run, item)))
        .collect();
    picked.sort_by_key(|(turn, _, item)| (*turn, item["ordinal"].as_u64().unwrap_or(0)));
    let mut entries: Vec<Value> = Vec::new();
    for (turn, run, item) in picked {
        let kind = item["type"].as_str().unwrap_or_default();
        if kind == "user_message" || kind == "assistant_message" {
            let text = item["text"].as_str().unwrap_or_default();
            let mut entry = json!({
                "turn": turn,
                "type": if kind == "user_message" { "user" } else { "assistant" },
                "length": text.chars().count(),
            });
            if (turn as i64) >= first_text_turn {
                let (scrubbed, cut) = scrub_text(text, input.sensitive, MAX_TEXT_CHARS);
                truncated |= cut;
                entry["text"] = json!(scrubbed);
            }
            entries.push(entry);
            continue;
        }
        let names_built_ins = matches!(
            (input.driver_of)(run).as_deref(),
            Some("claudeAgent" | "codex")
        );
        let mut entry = json!({
            "turn": turn,
            "type": "tool",
            "tool": tool_name(item, input.t3_tools, names_built_ins),
            "outcome": outcome(item),
        });
        if let (Some(start), Some(end)) = (millis(&item["startedAt"]), millis(&item["completedAt"]))
        {
            entry["durationMs"] = json!((end - start).max(0));
        }
        entries.push(entry);
    }
    // Tool calls go first, oldest first, so every turn's messages stay.
    while entries.len() > MAX_ENTRIES {
        let at = entries
            .iter()
            .position(|e| e["type"] == "tool")
            .unwrap_or(0);
        entries.remove(at);
        truncated = true;
    }
    let trace = |entries: &[Value], truncated: bool| {
        let mut text_turns: Vec<u64> = entries
            .iter()
            .filter(|e| e.get("text").is_some())
            .filter_map(|e| e["turn"].as_u64())
            .collect();
        text_turns.sort_unstable();
        text_turns.dedup();
        json!({"turns": runs.len(), "textTurns": text_turns.len(), "truncated": truncated, "entries": entries})
    };
    loop {
        let current = trace(&entries, truncated);
        if current.to_string().len() <= MAX_BYTES {
            return current;
        }
        truncated = true;
        match entries.iter().position(|e| e.get("text").is_some()) {
            None => {
                entries.remove(0);
            }
            Some(at) => {
                if let Some(fields) = entries[at].as_object_mut() {
                    fields.remove("text");
                }
            }
        }
    }
}
