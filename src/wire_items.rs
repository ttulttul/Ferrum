//! Turn items as the reference sends them (its `WireProjection`): command
//! output, tool output and file diffs stay out of thread snapshots and
//! events, marked `outputOmitted`, and `orchestration.getTurnItem` returns
//! them bounded for transport.
use serde_json::{json, Map, Value};

const MAX_DETAIL_STRING_BYTES: usize = 32_768;
const MAX_DYNAMIC_VALUE_BYTES: usize = 16_384;
const MAX_ON_DEMAND_BYTES: usize = 256 * 1024;
const TRUNCATION_MARKER: &str = "\n… output truncated for transport";
const MAX_PARSED_BYTES: usize = 16_384;
const MAX_METADATA_BYTES: usize = 8_192;
const MAX_ID_LENGTH: usize = 256;
const MAX_THREADS: usize = 100;
const MAX_CONTENT_BLOCKS: usize = 32;
const MAX_ENVELOPE_DEPTH: usize = 4;
const MAX_ENVELOPE_NODES: i64 = 128;
const SEND_TO_USER_MAX_LENGTH: usize = 4_000;

/// The reference's truncateDetail: at most `max` UTF-8 bytes, the marker
/// included.
fn truncate(value: &str, max: usize) -> String {
    if value.len() <= max {
        return value.to_owned();
    }
    let mut cut = max - TRUNCATION_MARKER.len();
    while !value.is_char_boundary(cut) {
        cut -= 1;
    }
    format!("{}{TRUNCATION_MARKER}", &value[..cut])
}

fn non_blank(value: &Value) -> bool {
    value.as_str().is_some_and(|text| !text.trim().is_empty())
}

/// The reference's toolOutputIndicatesFailure.
fn output_indicates_failure(text: &str) -> bool {
    use std::sync::LazyLock;
    static HINT: LazyLock<regex::Regex> = LazyLock::new(|| {
        regex::Regex::new(r"(?i)enoent|no such|no files found|not found|commandnotfound|not recognized|cannot (?:find path|be found)|exit").unwrap()
    });
    static FAILURE: LazyLock<regex::Regex> = LazyLock::new(|| {
        regex::Regex::new(r"(?i)file not found|no files found|enoent|no such file|commandnotfoundexception|command not found|is not recognized as the name of a cmdlet|a parameter cannot be found that matches parameter name").unwrap()
    });
    static EXIT: LazyLock<regex::Regex> = LazyLock::new(|| {
        regex::Regex::new(
            r"(?i)exit(?:ed)? with exit code\s+[1-9]\d*|exit code\s*[:\s]\s*[1-9]\d*\b",
        )
        .unwrap()
    });
    let lower = text.to_lowercase();
    HINT.is_match(text)
        && (FAILURE.is_match(text)
            || (lower.contains("cannot find path") && lower.contains("because it does not exist"))
            || (lower.contains("is not recognized") && lower.contains("the term '"))
            || EXIT.is_match(text))
}

/// The reference's hasDynamicValue.
fn has_value(value: &Value) -> bool {
    match value {
        Value::Null => false,
        Value::String(text) => !text.trim().is_empty(),
        Value::Array(items) => !items.is_empty(),
        Value::Object(map) => !map.is_empty(),
        _ => true,
    }
}

/// The reference's summarizeDynamicValue: a value small enough as itself,
/// otherwise its first line.
fn summarize(value: &Value) -> Value {
    let serialized = match value {
        Value::String(text) if text.len() > MAX_DYNAMIC_VALUE_BYTES => text.clone(),
        other => {
            let json = other.to_string();
            if json.len() <= MAX_DYNAMIC_VALUE_BYTES {
                return other.clone();
            }
            other.as_str().map_or(json, str::to_owned)
        }
    };
    let trimmed = serialized.trim_start();
    let mut first = if trimmed.is_empty() {
        "Large tool output".to_owned()
    } else {
        String::new()
    };
    let mut pending_space = false;
    for character in trimmed.chars() {
        if character == '\n' {
            break;
        }
        if character.is_whitespace() {
            pending_space = true;
            continue;
        }
        if pending_space && !first.is_empty() {
            first.push(' ');
        }
        first.push(character);
        pending_space = false;
        if first.chars().count() > 160 {
            break;
        }
    }
    let summary = if first.chars().count() <= 160 {
        first
    } else {
        let head: String = first.chars().take(159).collect();
        format!("{}…", head.trim_end())
    };
    json!({"summary": summary, "truncated": true})
}

struct Budget {
    bytes: usize,
    nodes: i64,
    exceeded: bool,
}

/// The reference's readResult: a tool result envelope's data and whether
/// it reports a failure.
fn read_result(
    value: &Value,
    budget: &mut Budget,
    depth: usize,
) -> (Option<Map<String, Value>>, bool) {
    budget.nodes -= 1;
    if depth > MAX_ENVELOPE_DEPTH || budget.nodes < 0 {
        budget.exceeded = true;
        return (None, false);
    }
    match value {
        Value::String(text) => {
            if text.len() > budget.bytes {
                budget.exceeded = true;
                return (None, false);
            }
            budget.bytes -= text.len();
            match serde_json::from_str::<Value>(text) {
                Ok(parsed) => read_result(&parsed, budget, depth + 1),
                Err(_) => (None, false),
            }
        }
        Value::Array(blocks) => {
            if blocks.len() > MAX_CONTENT_BLOCKS {
                budget.exceeded = true;
                return (None, false);
            }
            let (mut data, mut failed) = (None, false);
            for block in blocks {
                let text = &block["text"];
                let inner = if text.is_object() {
                    if text["text"].is_null() {
                        text
                    } else {
                        &text["text"]
                    }
                } else {
                    text
                };
                let (found, block_failed) = read_result(inner, budget, depth + 1);
                if data.is_none() {
                    data = found;
                }
                failed |= block_failed;
                if budget.exceeded {
                    break;
                }
            }
            (data, failed)
        }
        Value::Object(map) => {
            let failed = map.get("isError") == Some(&json!(true))
                || map.get("is_error") == Some(&json!(true))
                || map.get("_tag") == Some(&json!("OrchestratorMcpFailure"))
                || map.get("error").is_some_and(|error| !error.is_null());
            let content = map
                .get("structuredContent")
                .filter(|v| !v.is_null())
                .or_else(|| map.get("content").filter(|v| !v.is_null()));
            match content {
                Some(content) => {
                    let (data, nested) = read_result(content, budget, depth + 1);
                    (data, failed || nested)
                }
                None => (Some(map.clone()), failed),
            }
        }
        _ => (None, false),
    }
}

fn bounded_id(value: &Value) -> Option<String> {
    let text = value.as_str()?;
    (text.chars().count() <= MAX_ID_LENGTH && !text.trim().is_empty()).then(|| text.to_owned())
}

const HTML_RENDER_MIN_HEIGHT: f64 = 80.0;
const HTML_RENDER_MAX_HEIGHT: f64 = 2000.0;
const HTML_RENDER_MAX_TITLE_LENGTH: usize = 200;

/// `clampHtmlRenderHeight`.
fn clamp_html_height(height: f64) -> i64 {
    // JavaScript's Math.round rounds halves up.
    (height + 0.5)
        .floor()
        .clamp(HTML_RENDER_MIN_HEIGHT, HTML_RENDER_MAX_HEIGHT) as i64
}

/// `readHtmlRenderReference`: a page an `html_render` showed, as a client
/// reads it from the tool's output.
fn html_render_reference(value: &Value) -> Option<Value> {
    let id = value["attachmentId"]
        .as_str()
        .filter(|id| !id.is_empty() && id.encode_utf16().count() <= 256)?;
    let title = value["title"].as_str()?;
    let height = value["height"].as_f64().filter(|h| h.is_finite())?;
    let mut units = 0;
    let title: String = title
        .trim()
        .chars()
        .take_while(|c| {
            units += c.len_utf16();
            units <= HTML_RENDER_MAX_TITLE_LENGTH
        })
        .collect();
    let mut out = json!({
        "attachmentId": id,
        "title": if title.is_empty() { "HTML" } else { title.as_str() },
        "height": clamp_html_height(height),
    });
    // `readMeasuredHeights`: 1 to 24 whole widths, each with a finite height.
    if let Some(entries) = value["heights"]
        .as_array()
        .filter(|e| !e.is_empty() && e.len() <= 24)
    {
        let mut heights: Vec<(i64, i64)> = entries
            .iter()
            .filter_map(|entry| {
                let pair = entry.as_array().filter(|p| p.len() == 2)?;
                let width = pair[0]
                    .as_f64()
                    .filter(|w| w.fract() == 0.0 && (1.0..=10_000.0).contains(w))?;
                let height = pair[1].as_f64().filter(|h| h.is_finite())?;
                Some((width as i64, clamp_html_height(height)))
            })
            .collect();
        if heights.len() == entries.len() {
            heights.sort_by_key(|(width, _)| *width);
            out["heights"] = json!(heights
                .iter()
                .map(|(w, h)| json!([w, h]))
                .collect::<Vec<_>>());
        }
    }
    Some(out)
}

/// The reference's compactDynamicToolOutput: what a client needs from a
/// tool's output without the output itself (failure, created ids, counts).
fn compact_output(value: &Value) -> Option<Value> {
    let mut budget = Budget {
        bytes: MAX_PARSED_BYTES,
        nodes: MAX_ENVELOPE_NODES,
        exceeded: false,
    };
    let (data, failed) = read_result(value, &mut budget, 0);
    let mut out = Map::new();
    if failed {
        out.insert("isError".into(), json!(true));
    }
    if let Some(data) = data.filter(|_| !budget.exceeded) {
        for key in ["threadId", "messageId", "taskId", "scheduledTaskId"] {
            if let Some(id) = data.get(key).and_then(bounded_id) {
                out.insert(key.into(), json!(id));
            }
        }
        if data.get("status") == Some(&json!("rolled_back")) {
            out.insert("status".into(), json!("rolled_back"));
        }
        for key in ["linkedCount", "unlinkedCount"] {
            if let Some(count) = data.get(key).and_then(Value::as_i64) {
                out.insert(key.into(), json!(count));
            }
        }
        if let Some(message) = data.get("sentMessage").and_then(Value::as_str) {
            if !message.trim().is_empty()
                && message.encode_utf16().count() <= SEND_TO_USER_MAX_LENGTH
            {
                out.insert("sentMessage".into(), json!(message));
            }
        }
        if let Some(render) = data.get("htmlRender").and_then(html_render_reference) {
            out.insert("htmlRender".into(), render);
        }
        if let Some(id) = data.get("thread").and_then(|t| bounded_id(&t["threadId"])) {
            out.insert("thread".into(), json!({"threadId": id}));
        }
        if let Some(threads) = data.get("threads").and_then(Value::as_array) {
            let mut complete = threads.len() <= MAX_THREADS;
            let mut listed = Vec::new();
            if complete {
                for entry in threads {
                    let id = bounded_id(&entry["threadId"]);
                    let rolled_back = entry["status"] == "rolled_back";
                    if !entry.is_object() || (id.is_none() && !rolled_back) {
                        complete = false;
                        break;
                    }
                    let mut row = Map::new();
                    if let Some(id) = id {
                        row.insert("threadId".into(), json!(id));
                    }
                    if rolled_back {
                        row.insert("status".into(), json!("rolled_back"));
                    }
                    listed.push(Value::Object(row));
                }
            }
            if complete {
                out.insert("threads".into(), Value::Array(listed));
            } else {
                out.remove("threadId");
                out.remove("status");
            }
        }
    }
    if Value::Object(out.clone()).to_string().len() > MAX_METADATA_BYTES {
        for key in ["threads", "threadId", "status"] {
            out.remove(key);
        }
    }
    (!out.is_empty()).then_some(Value::Object(out))
}

/// A turn item as thread snapshots and events carry it.
pub(crate) fn wire_item(item: &Value) -> Value {
    let mut item = item.clone();
    let Some(map) = item.as_object_mut() else {
        return item;
    };
    match map.get("type").and_then(Value::as_str) {
        Some("command_execution") => {
            let output = map.remove("output");
            let text = output.as_ref().and_then(Value::as_str).unwrap_or_default();
            let failed = match map.get("exitCode").and_then(Value::as_i64) {
                Some(code) => code != 0,
                None => {
                    map.get("outputIndicatesFailure") == Some(&json!(true))
                        || (output.is_some()
                            && output_indicates_failure(&truncate(text, MAX_DETAIL_STRING_BYTES)))
                }
            };
            map.remove("outputIndicatesFailure");
            if failed {
                map.insert("outputIndicatesFailure".into(), json!(true));
            }
            if !text.trim().is_empty() {
                map.insert("outputOmitted".into(), json!(true));
            }
        }
        Some("file_change") => {
            let diff = map.remove("diffStr");
            map.remove("oldStr");
            map.remove("newStr");
            if map.get("status") == Some(&json!("failed")) {
                if let Some(diff) = diff.filter(non_blank) {
                    let bounded =
                        truncate(diff.as_str().unwrap_or_default(), MAX_DETAIL_STRING_BYTES);
                    map.insert("diffStr".into(), json!(bounded));
                }
            }
        }
        Some("dynamic_tool") => {
            let raw = map.remove("output").unwrap_or(Value::Null);
            if let Some(input) = map.get("input").cloned() {
                map.insert("input".into(), summarize(&input));
            }
            if let Some(output) = compact_output(&raw) {
                map.insert("output".into(), output);
            }
            if has_value(&raw) {
                map.insert("outputOmitted".into(), json!(true));
            }
        }
        _ => {}
    }
    item
}

/// The reference's boundDynamicValue.
fn bound_value(value: &Value) -> Value {
    match value {
        Value::String(text) => json!(truncate(text, MAX_ON_DEMAND_BYTES)),
        Value::Null => Value::Null,
        other => {
            let json = other.to_string();
            if json.len() <= MAX_ON_DEMAND_BYTES {
                other.clone()
            } else {
                json!(truncate(&json, MAX_ON_DEMAND_BYTES))
            }
        }
    }
}

/// A turn item as `orchestration.getTurnItem` returns it: everything, with
/// long values bounded.
pub(crate) fn detail_item(item: &Value) -> Value {
    let mut out = item.clone();
    match item["type"].as_str() {
        Some("command_execution") => {
            out["input"] = json!(truncate(
                item["input"].as_str().unwrap_or_default(),
                MAX_ON_DEMAND_BYTES
            ));
            if let Some(output) = item["output"].as_str() {
                out["output"] = json!(truncate(output, MAX_ON_DEMAND_BYTES));
            }
        }
        Some("dynamic_tool") => {
            out["input"] = bound_value(&item["input"]);
            if item.get("output").is_some() {
                out["output"] = bound_value(&item["output"]);
            }
        }
        Some("file_change") => {
            out = wire_item(item);
            let diff = item["diffStr"].as_str();
            if let Some(diff) = diff {
                out["diffStr"] = json!(truncate(diff, MAX_ON_DEMAND_BYTES));
            }
            if diff.is_none_or(|d| d.trim().is_empty()) {
                for key in ["oldStr", "newStr"] {
                    if let Some(text) = item[key].as_str() {
                        out[key] = json!(truncate(text, MAX_ON_DEMAND_BYTES));
                    }
                }
            }
        }
        _ => {}
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_html_render_reference_survives_compaction_as_a_client_reads_it() {
        let output = json!({"structuredContent": {"htmlRender": {
            "attachmentId": "thread-x-00000000-0000-4000-8000-000000000000-html",
            "title": "  Chart  ",
            "height": 4000.4,
            "heights": [[728, 300.5], [320, 10]],
        }, "message": "Shown."}});
        let compact = compact_output(&output).expect("compact output");
        assert_eq!(
            compact["htmlRender"],
            json!({"attachmentId": "thread-x-00000000-0000-4000-8000-000000000000-html", "title": "Chart", "height": 2000, "heights": [[320, 80], [728, 301]]})
        );
        // Heights that are not all valid are left out, the reference stays.
        let bad = json!({"structuredContent": {"htmlRender": {"attachmentId": "a", "title": "", "height": 100, "heights": [[0, 5]]}}});
        assert_eq!(
            compact_output(&bad).unwrap()["htmlRender"],
            json!({"attachmentId": "a", "title": "HTML", "height": 100})
        );
    }

    #[test]
    fn output_stays_out_of_the_wire_and_comes_back_in_detail() {
        let command =
            json!({"type": "command_execution", "input": "ls", "output": "a\n", "exitCode": 0});
        assert_eq!(
            wire_item(&command),
            json!({"type": "command_execution", "input": "ls", "exitCode": 0, "outputOmitted": true})
        );
        assert_eq!(detail_item(&command)["output"], "a\n");
        let failed = json!({"type": "command_execution", "input": "x", "output": "bash: x: command not found"});
        assert_eq!(wire_item(&failed)["outputIndicatesFailure"], true);
        let tool = json!({"type": "dynamic_tool", "toolName": "Read", "input": {"file_path": "a"},
            "output": "1\thello"});
        assert_eq!(
            wire_item(&tool),
            json!({"type": "dynamic_tool", "toolName": "Read", "input": {"file_path": "a"}, "outputOmitted": true})
        );
        let created = json!({"type": "dynamic_tool", "toolName": "t3.thread_launch", "input": {},
            "output": [{"type": "text", "text": "{\"threadId\":\"thread:new\"}"}]});
        assert_eq!(
            wire_item(&created)["output"],
            json!({"threadId": "thread:new"})
        );
        let file = json!({"type": "file_change", "status": "completed", "fileName": "a", "diffStr": "+x",
            "oldStr": "o", "newStr": "n"});
        assert_eq!(
            wire_item(&file),
            json!({"type": "file_change", "status": "completed", "fileName": "a"})
        );
        assert_eq!(detail_item(&file)["diffStr"], "+x");
        assert!(detail_item(&file).get("oldStr").is_none());
        let long = "x".repeat(MAX_ON_DEMAND_BYTES + 10);
        let bounded = truncate(&long, MAX_ON_DEMAND_BYTES);
        assert_eq!(bounded.len(), MAX_ON_DEMAND_BYTES);
        assert!(bounded.ends_with(TRUNCATION_MARKER));
    }
}
