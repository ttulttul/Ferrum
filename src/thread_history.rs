//! Thread history windows and pages, as the reference's `orchestration-v2/threadHistoryPaging.ts`
//! selects them: a bounded snapshot keeps the newest window of a thread's timeline and hands out
//! a cursor to the turns before it, and each history page reads the window before a cursor.
//!
//! A window that holds a user turn stops at `max_user_turns` user turns (150 raw turn starts)
//! or `max_turn_items` rows; any other window, such as an agent-driven thread or the inside of
//! one very long turn, stops at `max_items` rows. Every window also stops at its byte budget, and
//! always admits at least one row so that an oversized item cannot stall paging.
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use serde_json::{json, Map, Value};
use std::collections::{HashMap, HashSet};

/// `THREAD_HISTORY_PAGE_POLICY` and friends.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct Policy {
    pub max_user_turns: Option<usize>,
    pub max_items: usize,
    pub max_turn_items: usize,
    pub max_encoded_bytes: usize,
}

pub(crate) const PAGE_POLICY: Policy = Policy {
    max_user_turns: Some(10),
    max_items: 75,
    max_turn_items: 500,
    max_encoded_bytes: 1_048_576,
};
/// `OLDER_THREAD_USER_TURN_LIMIT`: older pages allow twice the turns of a snapshot.
const OLDER_USER_TURN_LIMIT: usize = 20;
const MAX_RAW_TURNS: usize = 150;
const CURSOR_MAX_LENGTH: usize = 4_096;

/// `ThreadHistoryCursorPayload`.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Cursor {
    pub seq: u64,
    pub st: String,
    pub si: String,
    pub p: u64,
    pub inclusive: bool,
}

/// `encodeThreadHistoryCursor`: the payload as compact JSON, base64url without padding.
pub(crate) fn encode_cursor(seq: u64, st: &str, si: &str, p: u64) -> String {
    URL_SAFE_NO_PAD.encode(json!({"v":1,"seq":seq,"st":st,"si":si,"p":p}).to_string())
}

/// `decodeThreadHistoryCursor`, or `None` for one the reference refuses.
pub(crate) fn decode_cursor(cursor: &str) -> Option<Cursor> {
    if cursor.is_empty() || cursor.len() > CURSOR_MAX_LENGTH {
        return None;
    }
    let bytes = URL_SAFE_NO_PAD.decode(cursor.trim_end_matches('=')).ok()?;
    let value: Value = serde_json::from_slice(&bytes).ok()?;
    let text = |key: &str| {
        value[key]
            .as_str()
            .filter(|s| !s.is_empty())
            .map(str::to_owned)
    };
    if value["v"] != 1 {
        return None;
    }
    let inclusive = match value.get("inclusive") {
        None => false,
        Some(Value::Bool(b)) => *b,
        Some(_) => return None,
    };
    Some(Cursor {
        seq: value["seq"].as_u64()?,
        st: text("st")?,
        si: text("si")?,
        p: value["p"].as_u64()?,
        inclusive,
    })
}

fn bytes_of(value: &Value) -> usize {
    value.to_string().len()
}

fn item(row: &Value) -> &Value {
    &row["item"]
}

fn position(row: &Value) -> u64 {
    row["position"].as_u64().unwrap_or(0)
}

fn is_turn_start(item: &Value) -> bool {
    item["type"] == "user_message"
        && matches!(
            item["inputIntent"].as_str(),
            Some("turn_start" | "queued_turn")
        )
}

/// `isThreadHistoryUserTurn`.
fn is_user_turn(item: &Value) -> bool {
    is_turn_start(item) && item["createdBy"] == "user"
}

/// `threadHistoryTurnStarts`: notifications start wake turns when they are the run's first input.
fn turn_starts(items: &[&Value]) -> Vec<bool> {
    let mut started: HashSet<String> = HashSet::new();
    items
        .iter()
        .map(|item| {
            let kind = item["type"].as_str().unwrap_or_default();
            if kind != "user_message" && kind != "notification" {
                return false;
            }
            let run = item["runId"].as_str();
            let first_input = run.is_some_and(|run| !started.contains(run));
            if let Some(run) = run {
                started.insert(run.to_owned());
            }
            if kind == "notification" {
                first_input
            } else {
                is_turn_start(item)
            }
        })
        .collect()
}

fn renumber(rows: &mut [Value]) {
    for (index, row) in rows.iter_mut().enumerate() {
        row["position"] = json!(index);
    }
}

/// `selectOlderTimelinePage`: the newest rows before `end`, within `policy`. The rows keep their
/// positions in `rows`; the caller renumbers them.
fn select_older(
    rows: &[Value],
    end: usize,
    seq: u64,
    policy: Policy,
    row_cost: &dyn Fn(&Value) -> usize,
) -> (Vec<Value>, Option<String>, bool) {
    let end = end.min(rows.len());
    if end == 0 {
        return (Vec::new(), None, false);
    }
    let candidates: Vec<&Value> = rows[..end].iter().map(item).collect();
    let starts = turn_starts(&candidates);
    let turn_limit = if candidates.iter().any(|item| is_user_turn(item)) {
        policy.max_user_turns
    } else {
        None
    };
    let max_items = if turn_limit.is_none() {
        policy.max_items
    } else {
        policy.max_turn_items
    };
    let mut selected: Vec<&Value> = Vec::new();
    let (mut bytes, mut user_turns, mut raw_turns) = (0usize, 0usize, 0usize);
    for index in (0..end).rev() {
        let row = &rows[index];
        if !selected.is_empty()
            && (selected.len() >= max_items
                || turn_limit
                    .is_some_and(|limit| user_turns >= limit || raw_turns >= MAX_RAW_TURNS))
        {
            break;
        }
        let cost = row_cost(row);
        if !selected.is_empty() && bytes + cost > policy.max_encoded_bytes {
            break;
        }
        selected.push(row);
        bytes += cost;
        if is_user_turn(item(row)) {
            user_turns += 1;
        }
        if starts[index] {
            raw_turns += 1;
        }
    }
    // Inside a turn longer than the window, no selected row starts a user turn; later pages see
    // only such rows and use `max_items`, so this one does too.
    if user_turns == 0 {
        selected.truncate(policy.max_items);
    }
    selected.reverse();
    let oldest = selected.first().copied();
    let oldest_index = oldest.and_then(|oldest| {
        rows.iter().position(|row| {
            row["sourceThreadId"] == oldest["sourceThreadId"]
                && row["sourceItemId"] == oldest["sourceItemId"]
        })
    });
    let has_more = oldest_index.is_some_and(|index| index > 0);
    let next_cursor = oldest.filter(|_| has_more).map(|oldest| {
        encode_cursor(
            seq,
            oldest["sourceThreadId"].as_str().unwrap_or_default(),
            oldest["sourceItemId"].as_str().unwrap_or_default(),
            position(oldest),
        )
    });
    (
        selected.into_iter().cloned().collect(),
        next_cursor,
        has_more,
    )
}

/// `findCursorEndIndex`: where the rows before `cursor` end.
fn cursor_end(rows: &[Value], cursor: &Cursor) -> usize {
    if let Some(index) = rows.iter().position(|row| {
        row["sourceThreadId"].as_str() == Some(cursor.st.as_str())
            && row["sourceItemId"].as_str() == Some(cursor.si.as_str())
    }) {
        return index + usize::from(cursor.inclusive);
    }
    rows.iter()
        .position(|row| {
            if cursor.inclusive {
                row["sourceThreadId"].as_str() == Some(cursor.st.as_str())
                    && item(row)["ordinal"]
                        .as_u64()
                        .unwrap_or_else(|| position(row))
                        > cursor.p
            } else {
                position(row) >= cursor.p
            }
        })
        .unwrap_or(rows.len())
}

/// `missingInterruptRequests`: a page that holds an interrupt result also carries the request
/// it answers, which clients need to read the result.
fn missing_interrupt_requests(rows: &[Value], page: &[Value]) -> Vec<Value> {
    let mut runs: HashSet<&str> = page
        .iter()
        .filter(|row| item(row)["type"] == "run_interrupt_result")
        .filter_map(|row| item(row)["runId"].as_str())
        .collect();
    for row in page {
        if item(row)["type"] == "run_interrupt_request" {
            if let Some(run) = item(row)["runId"].as_str() {
                runs.remove(run);
            }
        }
    }
    if runs.is_empty() {
        return Vec::new();
    }
    rows.iter()
        .filter(|row| {
            item(row)["type"] == "run_interrupt_request"
                && item(row)["runId"]
                    .as_str()
                    .is_some_and(|run| runs.contains(run))
        })
        .cloned()
        .collect()
}

fn default_row_cost(row: &Value) -> usize {
    bytes_of(row)
}

/// `getThreadHistoryPage` without a target entry or a view: the page of `rows` (the thread's
/// whole visible timeline) before `cursor`, as `OrchestrationV2ThreadHistoryPage`. `None` for a
/// cursor the reference cannot read.
pub(crate) fn history_page(
    rows: &[Value],
    subagents: &[Value],
    thread_id: &str,
    cursor: &str,
    seq: u64,
) -> Option<Value> {
    let decoded = decode_cursor(cursor)?;
    let policy = Policy {
        max_user_turns: Some(OLDER_USER_TURN_LIMIT),
        ..PAGE_POLICY
    };
    let indexed: Vec<Value> = rows
        .iter()
        .enumerate()
        .map(|(index, row)| {
            let mut row = row.clone();
            row["position"] = json!(index);
            row
        })
        .collect();
    let end = cursor_end(&indexed, &decoded);
    let (items, page_cursor, page_more) = select_older(&indexed, end, seq, policy, &|_| 0);
    // The rows again within the byte budget, as the reference bounds what it read.
    let bounded_end = items.len();
    let (bounded, bounded_cursor, bounded_more) =
        select_older(&items, bounded_end, seq, policy, &default_row_cost);
    let requests = missing_interrupt_requests(&indexed, &bounded);
    let mut page: Vec<Value> = requests.into_iter().chain(bounded).collect();
    renumber(&mut page);
    let wanted: HashSet<&str> = page
        .iter()
        .filter(|row| item(row)["type"] == "subagent" && row["sourceThreadId"] == thread_id)
        .filter_map(|row| item(row)["subagentId"].as_str())
        .collect();
    let mut out = json!({
        "snapshotSequence": seq,
        "items": page,
        "nextCursor": if bounded_more { bounded_cursor } else { page_cursor },
        "hasMoreHistory": bounded_more || page_more,
    });
    let records: Vec<&Value> = subagents
        .iter()
        .filter(|s| s["id"].as_str().is_some_and(|id| wanted.contains(id)))
        .collect();
    if !records.is_empty() {
        out["subagents"] = json!(records);
    }
    Some(out)
}

fn is_local(thread_id: &str, row: &Value) -> bool {
    row["visibility"] == "local" || row["sourceThreadId"] == thread_id
}

fn pending_request_ids(projection: &Value) -> HashSet<String> {
    projection["runtimeRequests"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|request| request["status"] == "pending")
        .filter_map(|request| request["id"].as_str().map(str::to_owned))
        .collect()
}

/// Clients build the answer panel of an open question or approval from its row, and a run that
/// kept working pushes that row out of the newest turns.
fn is_retained_dependency(item: &Value, pending: &HashSet<String>) -> bool {
    item["type"] == "run_interrupt_request"
        || (matches!(
            item["type"].as_str(),
            Some("user_input_request" | "approval_request")
        ) && item["requestId"]
            .as_str()
            .is_some_and(|id| pending.contains(id)))
}

fn array<'a>(value: &'a Value, key: &str) -> &'a [Value] {
    value[key].as_array().map_or(&[], Vec::as_slice)
}

/// `messagesForBoundedProjection`: the window's messages, the latest and active runs' messages,
/// and the ones clients keep outside the window.
fn bounded_messages(projection: &Value, turn_items: &[Value]) -> Vec<Value> {
    let mut messages: HashSet<String> = turn_items
        .iter()
        .filter_map(|item| item["messageId"].as_str().map(str::to_owned))
        .collect();
    let runs = array(projection, "runs");
    let latest = runs
        .iter()
        .max_by_key(|run| run["ordinal"].as_u64().unwrap_or(0))
        .and_then(|run| run["id"].as_str());
    let mut retained_runs: HashSet<String> = HashSet::new();
    for run in runs {
        let active = matches!(
            run["status"].as_str(),
            Some("preparing" | "starting" | "running" | "waiting" | "queued")
        );
        if active || run["id"].as_str() == latest {
            if let Some(id) = run["id"].as_str() {
                retained_runs.insert(id.to_owned());
            }
            if let Some(message) = run["userMessageId"].as_str() {
                messages.insert(message.to_owned());
            }
        }
    }
    array(projection, "messages")
        .iter()
        .filter(|message| {
            message["id"]
                .as_str()
                .is_some_and(|id| messages.contains(id))
                || message["runId"]
                    .as_str()
                    .is_some_and(|run| retained_runs.contains(run))
                || message
                    .get("delegatedCompletion")
                    .is_some_and(|v| !v.is_null())
                || message["workflowScript"] == true
        })
        .cloned()
        .collect()
}

/// `buildBoundedThreadProjection` with `PAGE_POLICY`: the projection with only its newest window
/// of the timeline, and the cursor to the turns before it.
pub(crate) fn bounded_projection(projection: &Value, seq: u64) -> (Value, Option<String>, bool) {
    let thread_id = projection["thread"]["id"]
        .as_str()
        .unwrap_or_default()
        .to_owned();
    let run_ordinals: HashMap<&str, u64> = array(projection, "runs")
        .iter()
        .filter_map(|run| Some((run["id"].as_str()?, run["ordinal"].as_u64()?)))
        .collect();
    let mut latest_todo: Option<&str> = None;
    let mut latest_todo_ordinal: i64 = -1;
    for plan in array(projection, "plans") {
        let ordinal = plan["runId"].as_str().and_then(|run| run_ordinals.get(run));
        if let Some(&ordinal) = ordinal {
            if plan["kind"] == "todo_list"
                && plan["status"] != "superseded"
                && ordinal as i64 >= latest_todo_ordinal
            {
                latest_todo = plan["id"].as_str();
                latest_todo_ordinal = ordinal as i64;
            }
        }
    }
    let is_latest_todo = |plan: &Value| latest_todo.is_some() && plan["id"].as_str() == latest_todo;
    let mut control = projection.clone();
    control["plans"] = json!(array(projection, "plans")
        .iter()
        .filter(|plan| plan["status"] == "active" || is_latest_todo(plan))
        .collect::<Vec<_>>());
    control["contextHandoffs"] = json!(array(projection, "contextHandoffs")
        .iter()
        .filter(|h| matches!(h["status"].as_str(), Some("pending" | "ready")))
        .collect::<Vec<_>>());
    let pending = pending_request_ids(&control);
    let reserve: usize = array(&control, "turnItems")
        .iter()
        .filter(|item| is_retained_dependency(item, &pending))
        .map(bytes_of)
        .sum();
    let mut bare = control.clone();
    for key in ["messages", "turnItems", "visibleTurnItems"] {
        bare[key] = json!([]);
    }
    let control_bytes = bytes_of(&bare);
    // Control state cannot be paged, so the window keeps half the budget; dependency cards take
    // their reserve from that half.
    let half = (PAGE_POLICY.max_encoded_bytes / 2).saturating_sub(reserve);
    let rest = PAGE_POLICY
        .max_encoded_bytes
        .saturating_sub(reserve)
        .saturating_sub(control_bytes)
        .saturating_sub(1_024);
    let window_policy = Policy {
        max_encoded_bytes: half.max(rest),
        ..PAGE_POLICY
    };
    // Local rows are charged twice: a bounded snapshot also carries them in `turnItems`.
    let cost = |row: &Value| {
        let bytes = bytes_of(row);
        if is_local(&thread_id, row) {
            bytes + bytes_of(item(row))
        } else {
            bytes
        }
    };
    let rows = array(&control, "visibleTurnItems").to_vec();
    let (mut visible, cursor, has_more) =
        select_older(&rows, rows.len(), seq, window_policy, &cost);
    renumber(&mut visible);
    let mut turn_items: Vec<Value> = visible
        .iter()
        .filter(|row| is_local(&thread_id, row))
        .map(|row| item(row).clone())
        .collect();
    let in_window: HashSet<String> = visible
        .iter()
        .filter(|row| is_local(&thread_id, row))
        .filter_map(|row| row["sourceItemId"].as_str().map(str::to_owned))
        .collect();
    for item in array(&control, "turnItems") {
        let id = item["id"].as_str().unwrap_or_default();
        if is_retained_dependency(item, &pending) && !in_window.contains(id) {
            turn_items.push(item.clone());
        }
    }
    let plan_ids: HashSet<&str> = turn_items
        .iter()
        .filter(|i| matches!(i["type"].as_str(), Some("proposed_plan" | "todo_list")))
        .filter_map(|i| i["planId"].as_str())
        .collect();
    let handoff_ids: HashSet<&str> = turn_items
        .iter()
        .filter(|i| i["type"] == "handoff")
        .filter_map(|i| i["contextHandoffId"].as_str())
        .collect();
    let plans: Vec<Value> = array(projection, "plans")
        .iter()
        .filter(|plan| {
            plan["status"] == "active"
                || is_latest_todo(plan)
                || plan["id"].as_str().is_some_and(|id| plan_ids.contains(id))
        })
        .map(|plan| {
            if plan["status"] == "active" || is_latest_todo(plan) {
                return plan.clone();
            }
            let mut plan = plan.clone();
            let fields = plan.as_object_mut().expect("plan");
            if fields.get("kind").and_then(Value::as_str) == Some("proposed_plan") {
                fields.insert("markdown".into(), json!(""));
            } else {
                fields.shift_remove("explanation");
            }
            fields.insert("detailInTurnItem".into(), json!(true));
            plan
        })
        .collect();
    let handoffs: Vec<Value> = array(projection, "contextHandoffs")
        .iter()
        .filter(|h| {
            matches!(h["status"].as_str(), Some("pending" | "ready"))
                || h["id"].as_str().is_some_and(|id| handoff_ids.contains(id))
        })
        .map(|h| {
            if matches!(h["status"].as_str(), Some("pending" | "ready")) {
                return h.clone();
            }
            let mut h = h.clone();
            let fields: &mut Map<String, Value> = h.as_object_mut().expect("handoff");
            fields.insert("summaryText".into(), json!(""));
            fields.insert("detailInTurnItem".into(), json!(true));
            h
        })
        .collect();
    let messages = bounded_messages(&control, &turn_items);
    let mut bounded = control;
    bounded["plans"] = json!(plans);
    bounded["contextHandoffs"] = json!(handoffs);
    bounded["messages"] = json!(messages);
    bounded["turnItems"] = json!(turn_items);
    bounded["visibleTurnItems"] = json!(visible);
    (bounded, cursor, has_more)
}

/// The rows a bounded snapshot reads (`readProjection` with `userTurnLimit`): from the
/// `max_user_turns + 2`th newest user turn on, or the `MAX_RAW_TURNS + 2`th newest turn start when
/// there are fewer user turns, renumbered from 0. The window then selects within them, so its
/// cursor's position counts from there.
fn snapshot_rows(rows: &[Value]) -> Vec<Value> {
    let limit = PAGE_POLICY.max_user_turns.unwrap_or(0) + 2;
    let items: Vec<&Value> = rows.iter().map(item).collect();
    let starts = turn_starts(&items);
    let user_turns: Vec<usize> = (0..rows.len())
        .filter(|&i| starts[i] && is_user_turn(items[i]))
        .collect();
    let anchors: Vec<usize> = (0..rows.len()).filter(|&i| starts[i]).collect();
    let start = if user_turns.len() >= limit {
        user_turns[user_turns.len() - limit]
    } else if anchors.len() >= MAX_RAW_TURNS + 2 {
        anchors[anchors.len() - (MAX_RAW_TURNS + 2)]
    } else {
        0
    };
    let mut window = rows[start..].to_vec();
    renumber(&mut window);
    window
}

/// A bounded `snapshot` stream item with its projection windowed and its history cursor set, as
/// the reference's `buildBoundedThreadStreamSnapshot` gives it. Any other item is left alone.
pub(crate) fn bound_snapshot_item(item: &mut Value) {
    if item["kind"] != "snapshot" || item.get("historyCursor").is_none() {
        return;
    }
    let seq = item["snapshotSequence"].as_u64().unwrap_or(0);
    let mut read = item["projection"].clone();
    read["visibleTurnItems"] = json!(snapshot_rows(array(&read, "visibleTurnItems")));
    let (projection, cursor, has_more) = bounded_projection(&read, seq);
    item["projection"] = projection;
    item["historyCursor"] = json!(cursor);
    item["hasMoreHistory"] = json!(has_more);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(index: usize, kind: &str, user: bool) -> Value {
        let id = format!("item:{index}");
        let mut item = json!({"id": id, "type": kind, "runId": format!("run:{}", index / 2)});
        if user {
            item["inputIntent"] = json!("turn_start");
            item["createdBy"] = json!("user");
        }
        json!({"position": index, "visibility": "local", "sourceThreadId": "thread:t",
            "sourceItemId": id, "item": item})
    }

    fn turns(count: usize) -> Vec<Value> {
        (0..count * 2)
            .map(|i| {
                if i % 2 == 0 {
                    row(i, "user_message", true)
                } else {
                    row(i, "error", false)
                }
            })
            .collect()
    }

    #[test]
    fn a_cursor_reads_back_as_the_reference_writes_it() {
        let cursor = encode_cursor(135, "thread:t", "turn-item:message:message%3A2", 4);
        // The reference's cursor for the same row, from a live run.
        assert_eq!(
            cursor,
            "eyJ2IjoxLCJzZXEiOjEzNSwic3QiOiJ0aHJlYWQ6dCIsInNpIjoidHVybi1pdGVtOm1lc3NhZ2U6bWVzc2FnZSUzQTIiLCJwIjo0fQ"
        );
        let decoded = decode_cursor(&cursor).unwrap();
        assert_eq!((decoded.seq, decoded.p, decoded.inclusive), (135, 4, false));
        assert!(decode_cursor("not-a-cursor").is_none());
        assert!(decode_cursor("").is_none());
        assert!(decode_cursor(&"a".repeat(5000)).is_none());
    }

    #[test]
    fn a_snapshot_keeps_the_newest_ten_user_turns() {
        let rows = turns(12);
        let projection = json!({"thread": {"id": "thread:t"}, "runs": [], "plans": [],
            "contextHandoffs": [], "messages": [], "runtimeRequests": [],
            "turnItems": rows.iter().map(|r| r["item"].clone()).collect::<Vec<_>>(),
            "visibleTurnItems": rows});
        let (bounded, cursor, more) = bounded_projection(&projection, 7);
        assert!(more);
        let visible = bounded["visibleTurnItems"].as_array().unwrap();
        assert_eq!(visible.len(), 20);
        assert_eq!(visible[0]["sourceItemId"], "item:4");
        assert_eq!(visible[0]["position"], 0);
        assert_eq!(bounded["turnItems"].as_array().unwrap().len(), 20);
        let cursor = decode_cursor(&cursor.unwrap()).unwrap();
        assert_eq!((cursor.si.as_str(), cursor.p, cursor.seq), ("item:4", 4, 7));
    }

    #[test]
    fn a_stream_snapshot_counts_its_cursor_from_the_rows_it_read() {
        // 34 turns: the read starts at the 12th newest user turn (turn 22, row 44), so the
        // window's oldest row (turn 24, row 48) is at position 4 there, as the reference says.
        let rows = turns(34);
        let mut item = json!({"kind": "snapshot", "snapshotSequence": 3, "projection": {
            "thread": {"id": "thread:t"}, "visibleTurnItems": rows, "turnItems": [], "messages": [],
            "plans": [], "contextHandoffs": [], "runs": []},
            "historyCursor": null, "hasMoreHistory": false, "latestLocalTurnOrdinal": null});
        bound_snapshot_item(&mut item);
        assert_eq!(item["hasMoreHistory"], true);
        let cursor = decode_cursor(item["historyCursor"].as_str().unwrap()).unwrap();
        assert_eq!((cursor.si.as_str(), cursor.p), ("item:48", 4));
        assert_eq!(
            item["projection"]["visibleTurnItems"][0]["sourceItemId"],
            "item:48"
        );
        // An unbounded snapshot (no historyCursor field) is left alone.
        let mut plain = json!({"kind": "snapshot", "projection": {"visibleTurnItems": []}});
        bound_snapshot_item(&mut plain);
        assert!(plain.get("historyCursor").is_none());
    }

    #[test]
    fn a_short_thread_fits_one_window() {
        let rows = turns(3);
        let projection = json!({"thread": {"id": "thread:t"}, "visibleTurnItems": rows,
            "turnItems": [], "messages": [], "plans": [], "contextHandoffs": [], "runs": []});
        let (_, cursor, more) = bounded_projection(&projection, 1);
        assert_eq!((cursor, more), (None, false));
    }

    #[test]
    fn a_page_reads_the_turns_before_its_cursor() {
        let rows = turns(30);
        // The snapshot's cursor: before the newest ten turns.
        let cursor = encode_cursor(9, "thread:t", "item:40", 40);
        let page = history_page(&rows, &[], "thread:t", &cursor, 9).unwrap();
        let items = page["items"].as_array().unwrap();
        // Older pages allow twenty user turns.
        assert_eq!(items.len(), 40);
        assert_eq!(items[0]["sourceItemId"], "item:0");
        assert_eq!(items[0]["position"], 0);
        assert_eq!(page["hasMoreHistory"], false);
        assert_eq!(page["nextCursor"], Value::Null);
        let cursor = encode_cursor(9, "thread:t", "item:50", 50);
        let page = history_page(&rows, &[], "thread:t", &cursor, 9).unwrap();
        assert_eq!(page["items"][0]["sourceItemId"], "item:10");
        assert_eq!(page["hasMoreHistory"], true);
        let next = decode_cursor(page["nextCursor"].as_str().unwrap()).unwrap();
        assert_eq!((next.si.as_str(), next.p), ("item:10", 10));
        assert!(history_page(&rows, &[], "thread:t", "bad", 9).is_none());
    }

    #[test]
    fn an_interrupt_result_brings_its_request() {
        let mut rows = turns(25);
        rows[5]["item"]["type"] = json!("run_interrupt_request");
        rows[5]["item"]["runId"] = json!("run:x");
        rows[45]["item"]["type"] = json!("run_interrupt_result");
        rows[45]["item"]["runId"] = json!("run:x");
        let cursor = encode_cursor(1, "thread:t", "item:48", 48);
        let page = history_page(&rows, &[], "thread:t", &cursor, 1).unwrap();
        assert_eq!(page["items"][0]["sourceItemId"], "item:5");
    }
}
