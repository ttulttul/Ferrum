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

/// `isConversationHistoryItem`: anything but work rows.
fn is_conversation_item(item: &Value) -> bool {
    !matches!(
        item["type"].as_str(),
        Some("command_execution" | "reasoning" | "file_change" | "dynamic_tool")
    )
}

/// What a history page asks beyond its cursor.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct PageOptions<'a> {
    /// `throughEntryId`: widen the page until it reaches this entry (a message or item id).
    pub through_entry_id: Option<&'a str>,
    /// `view=conversation`: only conversation rows, once the history has a user turn.
    pub conversation_only: bool,
}

/// `olderHistoryPagePolicy`: older pages allow twice a snapshot's turns, and a targeted page
/// widens further so it reaches its entry.
fn older_policy(rows: &[Value], cursor: &Cursor, through_entry_id: Option<&str>) -> Policy {
    let policy = Policy {
        max_user_turns: Some(OLDER_USER_TURN_LIMIT),
        ..PAGE_POLICY
    };
    let Some(target) = through_entry_id else {
        return policy;
    };
    let anchor = cursor_end(rows, cursor);
    let Some(index) = rows.iter().position(|row| {
        item(row)["messageId"]
            .as_str()
            .or_else(|| item(row)["id"].as_str())
            == Some(target)
    }) else {
        return policy;
    };
    if index >= anchor {
        return policy;
    }
    let turns = rows[index..anchor]
        .iter()
        .filter(|row| is_user_turn(item(row)))
        .count();
    let extra = usize::from(!is_user_turn(item(&rows[index])));
    Policy {
        max_user_turns: Some(
            (OLDER_USER_TURN_LIMIT * 2).min(OLDER_USER_TURN_LIMIT.max(turns + extra)),
        ),
        max_items: policy.max_items * 2,
        max_turn_items: policy.max_turn_items * 2,
        ..policy
    }
}

/// `getThreadHistoryPage` (without deferred work): the page of `rows` (the thread's whole
/// visible timeline) before `cursor`, as `OrchestrationV2ThreadHistoryPage`. `None` for a cursor
/// the reference cannot read.
pub(crate) fn history_page(
    rows: &[Value],
    subagents: &[Value],
    thread_id: &str,
    cursor: &str,
    seq: u64,
    options: PageOptions<'_>,
) -> Option<Value> {
    let decoded = decode_cursor(cursor)?;
    let indexed: Vec<Value> = rows
        .iter()
        .enumerate()
        .map(|(index, row)| {
            let mut row = row.clone();
            row["position"] = json!(index);
            row
        })
        .collect();
    let policy = older_policy(&indexed, &decoded, options.through_entry_id);
    let end = cursor_end(&indexed, &decoded);
    let (items, page_cursor, page_more) = select_older(&indexed, end, seq, policy, &|_| 0);
    // A conversation view drops work rows once the history up to the page has a user turn.
    let page_end = items.last().map_or(0, |row| position(row) as usize + 1);
    let has_user_turns = options.conversation_only
        && indexed[..page_end.min(indexed.len())]
            .iter()
            .any(|row| is_user_turn(item(row)));
    let items: Vec<Value> = if has_user_turns {
        items
            .into_iter()
            .filter(|row| is_conversation_item(item(row)))
            .collect()
    } else {
        items
    };
    // The rows again within the byte budget, as the reference bounds what it read.
    let (bounded, bounded_cursor, bounded_more) =
        select_older(&items, items.len(), seq, policy, &default_row_cost);
    let requests = missing_interrupt_requests(&indexed, &bounded);
    let mut page: Vec<Value> = requests.into_iter().chain(bounded).collect();
    renumber(&mut page);
    if options.conversation_only {
        page.retain(|row| is_conversation_item(item(row)));
    }
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

/// `THREAD_HISTORY_SNAPSHOT_ROW_LIMIT`, the snapshot read's row limit.
const SNAPSHOT_ROW_LIMIT: usize = 75 + 2;
/// `THREAD_HISTORY_TURN_SNAPSHOT_ROW_LIMIT`: the row limit once the newest user turn fits whole.
const TURN_SNAPSHOT_ROW_LIMIT: usize = 500 + 2;

fn active(value: &Value, statuses: &[&str]) -> bool {
    value["status"]
        .as_str()
        .is_some_and(|s| statuses.contains(&s))
}

/// The projection a bounded snapshot reads (`getThreadSnapshotWindow` with `userTurnLimit`):
///
/// - rows from the `max_user_turns + 2`th newest user turn on (or the `MAX_RAW_TURNS + 2`th
///   newest turn start when there are fewer user turns), and of those the newest 502 when the
///   newest user turn has 1 to 500 rows, else the newest 77;
/// - the interrupt requests of the results among them, and the thread's newest item;
/// - those rows renumbered from 0 as its timeline, so the window's cursor counts from there, and
///   as its turn items with the pending request cards;
/// - the runs, attempts, nodes, subagents and provider sessions of those rows, and the live ones.
fn snapshot_read(projection: &Value) -> Value {
    let rows = array(projection, "visibleTurnItems");
    let limit = PAGE_POLICY.max_user_turns.unwrap_or(0) + 2;
    let items: Vec<&Value> = rows.iter().map(item).collect();
    let starts = turn_starts(&items);
    let user_turns: Vec<usize> = (0..rows.len())
        .filter(|&i| starts[i] && item(&rows[i])["createdBy"] == "user")
        .collect();
    let anchors: Vec<usize> = (0..rows.len()).filter(|&i| starts[i]).collect();
    let boundary = if user_turns.len() >= limit {
        user_turns[user_turns.len() - limit]
    } else if anchors.len() >= MAX_RAW_TURNS + 2 {
        anchors[anchors.len() - (MAX_RAW_TURNS + 2)]
    } else {
        0
    };
    let newest_turn_rows = user_turns
        .last()
        .map(|&latest| rows.len() - latest.max(boundary));
    let row_limit = match newest_turn_rows {
        Some(n) if (1..=PAGE_POLICY.max_turn_items).contains(&n) => TURN_SNAPSHOT_ROW_LIMIT,
        _ => SNAPSHOT_ROW_LIMIT,
    };
    let first = boundary.max(rows.len().saturating_sub(row_limit));
    let mut kept: Vec<usize> = (first..rows.len()).collect();
    let results: HashSet<&str> = kept
        .iter()
        .filter(|&&i| items[i]["type"] == "run_interrupt_result")
        .filter_map(|&i| items[i]["runId"].as_str())
        .collect();
    kept.extend((0..first).filter(|&i| {
        items[i]["type"] == "run_interrupt_request"
            && items[i]["runId"]
                .as_str()
                .is_some_and(|run| results.contains(run))
    }));
    if !rows.is_empty() {
        kept.push(rows.len() - 1);
    }
    kept.sort_unstable();
    kept.dedup();
    let mut visible: Vec<Value> = kept.iter().map(|&i| rows[i].clone()).collect();
    renumber(&mut visible);
    let mut turn_items: Vec<Value> = kept.iter().map(|&i| items[i].clone()).collect();
    // Pending requests outlive their turn's window; clients read their prompt from the card.
    let pending = pending_request_ids(projection);
    let in_read: HashSet<&str> = turn_items.iter().filter_map(|i| i["id"].as_str()).collect();
    let cards: Vec<Value> = array(projection, "turnItems")
        .iter()
        .filter(|i| {
            matches!(
                i["type"].as_str(),
                Some("approval_request" | "user_input_request")
            )
        })
        .filter(|i| {
            i["requestId"]
                .as_str()
                .is_some_and(|id| pending.contains(id))
        })
        .filter(|i| i["id"].as_str().is_some_and(|id| !in_read.contains(id)))
        .cloned()
        .collect();
    turn_items.extend(cards);
    turn_items.sort_by(|a, b| {
        (a["ordinal"].as_u64(), a["id"].as_str()).cmp(&(b["ordinal"].as_u64(), b["id"].as_str()))
    });

    // The cohort: what the read rows name, and what is live.
    let cohort_runs: HashSet<String> = kept
        .iter()
        .filter_map(|&i| items[i]["runId"].as_str().map(str::to_owned))
        .collect();
    let runs = array(projection, "runs");
    let latest_run = runs
        .iter()
        .filter(|run| !(run["status"] == "queued" && run["queueHeld"] == true))
        .max_by_key(|run| run["ordinal"].as_u64().unwrap_or(0))
        .and_then(|run| run["id"].as_str());
    let keep_run = |run: &Value| {
        active(
            run,
            &["queued", "preparing", "starting", "running", "waiting"],
        ) || run["id"]
            .as_str()
            .is_some_and(|id| cohort_runs.contains(id) || Some(id) == latest_run)
    };
    let kept_runs: Vec<Value> = runs.iter().filter(|run| keep_run(run)).cloned().collect();
    let live = ["pending", "starting", "running", "waiting"];
    let attempts: Vec<Value> = array(projection, "attempts")
        .iter()
        .filter(|a| {
            active(a, &live) || a["runId"].as_str().is_some_and(|r| cohort_runs.contains(r))
        })
        .cloned()
        .collect();
    let mut node_ids: HashSet<String> = HashSet::new();
    let mut add = |id: Option<&str>| {
        if let Some(id) = id {
            node_ids.insert(id.to_owned());
        }
    };
    for &i in &kept {
        add(items[i]["nodeId"].as_str());
    }
    for node in array(projection, "nodes") {
        if active(node, &live) {
            add(node["id"].as_str());
        }
    }
    for attempt in &attempts {
        add(attempt["rootNodeId"].as_str());
    }
    for run in runs {
        if active(
            run,
            &["queued", "preparing", "starting", "running", "waiting"],
        ) || run["id"]
            .as_str()
            .is_some_and(|id| cohort_runs.contains(id))
        {
            add(run["rootNodeId"].as_str());
        }
    }
    for request in array(projection, "runtimeRequests") {
        if active(request, &["pending", "waiting"]) {
            add(request["nodeId"].as_str());
        }
    }
    for subagent in array(projection, "subagents") {
        if active(subagent, &live) {
            add(subagent["parentNodeId"].as_str());
        }
    }
    for turn in array(projection, "providerTurns") {
        if active(turn, &["starting", "running", "waiting"]) {
            add(turn["nodeId"].as_str());
        }
    }
    // Every retained node's ancestors.
    let parents: HashMap<&str, &str> = array(projection, "nodes")
        .iter()
        .filter_map(|n| Some((n["id"].as_str()?, n["parentNodeId"].as_str()?)))
        .collect();
    let mut frontier: Vec<String> = node_ids.iter().cloned().collect();
    while let Some(id) = frontier.pop() {
        if let Some(parent) = parents.get(id.as_str()) {
            if node_ids.insert((*parent).to_owned()) {
                frontier.push((*parent).to_owned());
            }
        }
    }
    let nodes: Vec<Value> = array(projection, "nodes")
        .iter()
        .filter(|n| n["id"].as_str().is_some_and(|id| node_ids.contains(id)))
        .cloned()
        .collect();
    let subagents: Vec<Value> = array(projection, "subagents")
        .iter()
        .filter(|s| {
            active(s, &live)
                || s["runId"].as_str().is_some_and(|r| cohort_runs.contains(r))
                || s["parentNodeId"]
                    .as_str()
                    .is_some_and(|n| node_ids.contains(n))
        })
        .cloned()
        .collect();
    let mut read = projection.clone();
    read["runs"] = json!(kept_runs);
    read["attempts"] = json!(attempts);
    read["nodes"] = json!(nodes);
    read["subagents"] = json!(subagents);
    read["turnItems"] = json!(turn_items);
    read["visibleTurnItems"] = json!(visible);
    read
}

/// A bounded snapshot's projection (`getThreadSnapshotWindow`, then
/// `buildBoundedThreadProjection`), with the cursor to the turns before it and whether there are any.
pub(crate) fn bounded_snapshot(projection: &Value, seq: u64) -> (Value, Option<String>, bool) {
    bounded_projection(&snapshot_read(projection), seq)
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
        let projection = json!({"thread": {"id": "thread:t"}, "visibleTurnItems": rows,
            "turnItems": [], "messages": [], "plans": [], "contextHandoffs": [], "runs": []});
        let (bounded, cursor, more) = bounded_snapshot(&projection, 3);
        assert!(more);
        let cursor = decode_cursor(&cursor.unwrap()).unwrap();
        assert_eq!((cursor.si.as_str(), cursor.p), ("item:48", 4));
        assert_eq!(bounded["visibleTurnItems"][0]["sourceItemId"], "item:48");
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
        let page =
            history_page(&rows, &[], "thread:t", &cursor, 9, PageOptions::default()).unwrap();
        let items = page["items"].as_array().unwrap();
        // Older pages allow twenty user turns.
        assert_eq!(items.len(), 40);
        assert_eq!(items[0]["sourceItemId"], "item:0");
        assert_eq!(items[0]["position"], 0);
        assert_eq!(page["hasMoreHistory"], false);
        assert_eq!(page["nextCursor"], Value::Null);
        let cursor = encode_cursor(9, "thread:t", "item:50", 50);
        let page =
            history_page(&rows, &[], "thread:t", &cursor, 9, PageOptions::default()).unwrap();
        assert_eq!(page["items"][0]["sourceItemId"], "item:10");
        assert_eq!(page["hasMoreHistory"], true);
        let next = decode_cursor(page["nextCursor"].as_str().unwrap()).unwrap();
        assert_eq!((next.si.as_str(), next.p), ("item:10", 10));
        assert!(history_page(&rows, &[], "thread:t", "bad", 9, PageOptions::default()).is_none());
    }

    #[test]
    fn an_interrupt_result_brings_its_request() {
        let mut rows = turns(25);
        rows[5]["item"]["type"] = json!("run_interrupt_request");
        rows[5]["item"]["runId"] = json!("run:x");
        rows[45]["item"]["type"] = json!("run_interrupt_result");
        rows[45]["item"]["runId"] = json!("run:x");
        let cursor = encode_cursor(1, "thread:t", "item:48", 48);
        let page =
            history_page(&rows, &[], "thread:t", &cursor, 1, PageOptions::default()).unwrap();
        assert_eq!(page["items"][0]["sourceItemId"], "item:5");
    }
}
