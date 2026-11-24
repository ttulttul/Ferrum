//! Retry context handoffs (`delta_since_target_last_seen`), matching the
//! reference's `ContextHandoffService`, `ContextHandoffDelivery`,
//! `handoffBudget` and `ProviderTurnStartService.prepareRetryHandoff`.
//!
//! When a provider run starts on a provider thread whose earlier run(s) failed
//! or were interrupted before the provider accepted them, the missed runs'
//! historical items are packaged into a `delta_since_target_last_seen` context
//! handoff, budgeted against the native session's context window, persisted in
//! the thread projection, and prepended inline ahead of the turn's prompt
//! (while missed user attachments ride on the turn's attachment list).
//!
//! Platform-neutral: compiled for both the native server and the WebAssembly
//! control plane.
use crate::thread_projection::{iso, uri_component};
use crate::workspace::provider::ProviderTurn;
use crate::{Run, Thread};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet};

pub(crate) const DEFAULT_HANDOFF_TOKEN_CAP: usize = 16_000;
const HANDOFF_BYTE_CAP: usize = 64_000;
const MIN_HANDOFF_TOKEN_CAP: usize = 1_024;
const MAX_LISTED_OMITTED_ITEMS: usize = 20;

/// Configured token cap (`T3CODE_CONTEXT_HANDOFF_TOKEN_CAP`), clamped to
/// `1_024..=64_000`.
pub(crate) fn handoff_token_cap() -> usize {
    #[cfg(feature = "native")]
    {
        if let Ok(raw) = std::env::var("T3CODE_CONTEXT_HANDOFF_TOKEN_CAP") {
            if let Ok(value) = raw.trim().parse::<i64>() {
                return value.clamp(MIN_HANDOFF_TOKEN_CAP as i64, HANDOFF_BYTE_CAP as i64) as usize;
            }
        }
    }
    DEFAULT_HANDOFF_TOKEN_CAP
}

/// The reference's `OrchestrationV2HistoricalMessage`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct HistoricalMessage {
    pub role: String,
    pub text: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_status: Option<String>,
    pub thread_id: String,
    pub run_id: Option<String>,
    pub item_id: String,
    pub provider_thread_id: Option<String>,
    pub status: String,
    pub kind: String,
}

/// What `select_history` kept and omitted within its byte/token budget.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct SelectedHistory {
    pub messages: Vec<HistoricalMessage>,
    pub omitted_item_ids: Vec<String>,
    pub context: String,
    pub omitted_items: usize,
}

/// Handoff delivery prepared for one turn start.
#[derive(Clone, Debug)]
pub(crate) struct PreparedHandoff {
    /// Rendered inline history to prepend to the prompt (`with_inline_context`),
    /// empty when no handoff text should be prepended.
    pub inline_context: String,
    /// Attachments from missed runs to prepend to the turn's attachments.
    pub missed_attachments: Vec<Value>,
    /// Handoffs in `"pending"` delivery state to commit before starting the turn.
    pub pending: Vec<Value>,
    /// Each delivered handoff together with its `(item_ids, omitted_item_ids)` so
    /// `"inline"` delivery can be recorded once the provider accepts the prompt.
    pub deliverables: Vec<(Value, Vec<String>, Vec<String>)>,
    /// Original handoffs without `delivery` to restore if the turn is refused
    /// before the provider reads the prompt.
    pub unsent: Vec<Value>,
    /// The native thread id known at start time, if any.
    pub native_thread_id: Option<String>,
}

impl PreparedHandoff {
    /// Handoff records updated to `delivery.status = "inline"` once the provider
    /// accepted the prompt.
    pub(crate) fn into_delivered(self, native_thread_id: Option<&str>, at: u64) -> Vec<Value> {
        let now = iso(at);
        let native = native_thread_id.or(self.native_thread_id.as_deref());
        self.deliverables
            .into_iter()
            .map(|(mut handoff, item_ids, omitted_item_ids)| {
                handoff["updatedAt"] = json!(now);
                if let Some(native_id) = native {
                    handoff["delivery"] = json!({
                        "nativeThreadId": native_id,
                        "status": "inline",
                        "omittedItemIds": omitted_item_ids,
                        "itemIds": item_ids,
                    });
                }
                handoff
            })
            .collect()
    }

    /// Handoff records restored without `delivery` when the turn failed before
    /// the provider read the prompt.
    pub(crate) fn into_unsent(self, at: u64) -> Vec<Value> {
        let now = iso(at);
        self.unsent
            .into_iter()
            .map(|mut handoff| {
                handoff["updatedAt"] = json!(now);
                handoff
            })
            .collect()
    }
}

/// The provider prompt for a turn that carries inline context ahead of the
/// user's text (`withInlineContext`).
pub(crate) fn with_inline_context(context: &str, user_text: &str) -> String {
    if context.is_empty() {
        user_text.to_owned()
    } else {
        format!("{context}\n\nUser message:\n{user_text}")
    }
}

/// Wire projection of an `OrchestrationV2ContextHandoff`
/// (`projectContextHandoffForWire`): strips `history` and `delivery` and clears
/// `summaryText`.
pub(crate) fn project_for_wire(handoff: &Value) -> Value {
    let mut projected = handoff.clone();
    if let Some(map) = projected.as_object_mut() {
        map.remove("history");
        map.remove("delivery");
        map.insert("summaryText".into(), json!(""));
    }
    projected
}

/// `isSlashCommandMessage` (`ContextHandoffDelivery.ts` at `fd0331ca`):
/// providers only run a native slash command that opens the prompt, so its turn
/// defers inline context to the next turn. A first word with a second `/` is a
/// path, not a command.
fn is_slash_command_text(text: &str) -> bool {
    let trimmed = text.trim_start();
    let Some(rest) = trimmed.strip_prefix('/') else {
        return false;
    };
    let word_end = rest.find(char::is_whitespace).unwrap_or(rest.len());
    let word = &rest[..word_end];
    !word.is_empty() && !word.contains('/')
}

fn is_slash_command_message(message: &Value) -> bool {
    message["role"].as_str() == Some("user")
        && is_slash_command_text(message["text"].as_str().unwrap_or_default())
}

/// `withMessageSenderNote` (`packages/contracts/src/orchestrationV2.ts` at `fd0331ca`).
fn with_message_sender_note(item: &Value, text: &str) -> String {
    let agent = item["createdBy"].as_str() == Some("agent");
    let note = if agent && item["linkedSender"].is_object() {
        let thread_id = serde_json::to_string(
            item["linkedSender"]["threadId"]
                .as_str()
                .unwrap_or_default(),
        )
        .unwrap_or_default();
        let env_id = serde_json::to_string(
            item["linkedSender"]["environmentId"]
                .as_str()
                .unwrap_or_default(),
        )
        .unwrap_or_default();
        Some(format!(
            "[T3 Code: the agent in T3 thread {thread_id} on linked environment {env_id} sent this message, not the user.]"
        ))
    } else if agent && item["senderThreadId"].is_string() {
        let thread_id = serde_json::to_string(item["senderThreadId"].as_str().unwrap_or_default())
            .unwrap_or_default();
        Some(format!(
            "[T3 Code: the agent in T3 thread {thread_id} sent this message, not the user.]"
        ))
    } else if item["scheduledTaskId"].is_string() {
        let task_id = serde_json::to_string(item["scheduledTaskId"].as_str().unwrap_or_default())
            .unwrap_or_default();
        Some(format!(
            "[T3 Code: scheduled task {task_id} sent this prompt, not the user typing in this chat.]"
        ))
    } else if agent && item["creationSource"].as_str() == Some("mcp") {
        Some(
            "[T3 Code: this message came through T3 Code's tools, not from the user typing in this chat.]"
                .to_owned(),
        )
    } else {
        None
    };
    match note {
        Some(note) if text.trim().is_empty() => note,
        Some(note) => format!("{note}\n\n{text}"),
        None => text.to_owned(),
    }
}

/// `attachmentTokenAllowance` from `handoffBudget.ts`.
pub(crate) fn attachment_token_allowance(attachments: &[Value]) -> usize {
    attachments
        .iter()
        .map(|attachment| {
            if attachment["type"].as_str() == Some("image") {
                8_192
            } else {
                4_096
            }
        })
        .sum()
}

/// `estimateTextTokens`: about four UTF-8 bytes per token.
pub(crate) fn estimate_text_tokens(text: &str) -> usize {
    text.len().div_ceil(4)
}

fn json_byte_length<T: Serialize>(value: &T) -> usize {
    serde_json::to_string(value).map_or(0, |encoded| encoded.len())
}

/// `historicalMessage` from `handoffBudget.ts`.
pub(crate) fn historical_message(
    item: &Value,
    run_status: Option<&str>,
) -> Option<HistoricalMessage> {
    let kind = item["type"].as_str()?;
    let text = match kind {
        "user_message" => with_message_sender_note(item, item["text"].as_str().unwrap_or_default()),
        "assistant_message" => item["text"].as_str().unwrap_or_default().to_owned(),
        "command_execution" => {
            let input = item["input"].as_str().unwrap_or_default();
            let exit_code = match item.get("exitCode") {
                Some(Value::Number(n)) => n.to_string(),
                _ => "unknown".to_owned(),
            };
            let output = item["output"].as_str().unwrap_or_default();
            format!("Command: {input}\nExit code: {exit_code}\n{output}")
        }
        "error" => item["failure"]["message"]
            .as_str()
            .unwrap_or_default()
            .to_owned(),
        "run_interrupt_result" => item["message"].as_str().unwrap_or_default().to_owned(),
        "file_change" => {
            let paths = item["changes"]
                .as_array()
                .filter(|changes| !changes.is_empty())
                .map(|changes| {
                    changes
                        .iter()
                        .filter_map(|change| change["path"].as_str())
                        .collect::<Vec<_>>()
                        .join(", ")
                })
                .unwrap_or_else(|| item["fileName"].as_str().unwrap_or_default().to_owned());
            format!("File change: {paths}")
        }
        "proposed_plan" => item["markdown"].as_str().unwrap_or_default().to_owned(),
        _ => return None,
    };
    let run_id = item["runId"].as_str().map(str::to_owned);
    Some(HistoricalMessage {
        role: if kind == "user_message" {
            "user"
        } else {
            "assistant"
        }
        .to_owned(),
        text,
        run_status: run_id.as_ref().and(run_status).map(str::to_owned),
        thread_id: item["threadId"].as_str().unwrap_or_default().to_owned(),
        run_id,
        item_id: item["id"].as_str().unwrap_or_default().to_owned(),
        provider_thread_id: item["providerThreadId"].as_str().map(str::to_owned),
        status: item["status"].as_str().unwrap_or_default().to_owned(),
        kind: kind.to_owned(),
    })
}

/// `renderHistoricalMessage` from `handoffBudget.ts`.
pub(crate) fn render_historical_message(message: &HistoricalMessage) -> String {
    let run = message.run_id.as_deref().unwrap_or("imported");
    let provider_thread = message.provider_thread_id.as_deref().unwrap_or("none");
    let run_status = message
        .run_status
        .as_deref()
        .map(|status| format!("; run-status={status}"))
        .unwrap_or_default();
    format!(
        "[Historical {}; {}; thread={}; run={}; item={}; provider-thread={}; status={}{}]\n{}",
        message.role,
        message.kind,
        message.thread_id,
        run,
        message.item_id,
        provider_thread,
        message.status,
        run_status,
        message.text
    )
}

/// `historyResponseItems` from `handoffBudget.ts`.
fn history_response_items(messages: &[HistoricalMessage], context: &str) -> Value {
    let mut items = Vec::with_capacity(1 + messages.len());
    items.push(json!({
        "type": "message",
        "role": "user",
        "content": [{"type": "input_text", "text": context}],
    }));
    for message in messages {
        items.push(history_response_item(message));
    }
    Value::Array(items)
}

fn history_response_item(message: &HistoricalMessage) -> Value {
    let content_type = if message.role == "user" {
        "input_text"
    } else {
        "output_text"
    };
    json!({
        "type": "message",
        "role": message.role,
        "content": [{
            "type": content_type,
            "text": render_historical_message(message),
        }],
    })
}

/// `renderHistory` from `handoffBudget.ts`.
pub(crate) fn render_history(messages: &[HistoricalMessage], context: &str) -> String {
    let mut parts = Vec::with_capacity(1 + messages.len());
    parts.push(context.to_owned());
    for message in messages {
        parts.push(render_historical_message(message));
    }
    parts.join("\n\n")
}

/// `historyCost` from `handoffBudget.ts`.
pub(crate) fn history_cost(messages: &[HistoricalMessage], context: &str) -> usize {
    let response_bytes = json_byte_length(&history_response_items(messages, context));
    let rendered_bytes = json_byte_length(&render_history(messages, context));
    response_bytes.max(rendered_bytes) + 256
}

/// `handoffCoverage` from `handoffBudget.ts`.
pub(crate) fn handoff_coverage(
    thread_id: &str,
    from_ordinal: u64,
    to_ordinal: u64,
    items: &[&Value],
) -> String {
    let first = items
        .first()
        .and_then(|item| item["id"].as_str())
        .unwrap_or("none");
    let last = items
        .last()
        .and_then(|item| item["id"].as_str())
        .unwrap_or("none");
    format!(
        "Provider context handoff. Thread: {thread_id}. Covered app runs: {from_ordinal}-{to_ordinal}.\n\
Source item range: {first} through {last}.\n\
Recover an omitted item only when the request needs it, with t3_thread_read({{threadId:\"{thread_id}\",view:\"activity\",itemId:\"<omitted item ID>\",maxCharsPerItem:4000}}); continue long text with textOffset=nextTextOffset until null. Items included below need no read. Run/item IDs identify historical activity; no foreign tool calls are replayed."
    )
}

/// `selectHistory` from `handoffBudget.ts`.
pub(crate) fn select_history(
    messages: &[HistoricalMessage],
    coverage: &str,
    prior_omitted_items: usize,
    prior_omitted_ids: &[String],
    budget: usize,
) -> SelectedHistory {
    let mut selected = HashSet::new();
    let omitted_list = |ids: &[String], total: usize| -> String {
        if ids.is_empty() {
            return String::new();
        }
        let extra = if total > ids.len() {
            format!(" {} older omitted items are not listed.", total - ids.len())
        } else {
            String::new()
        };
        format!(
            " Omitted item IDs, newest last: {}.{}",
            ids.join(", "),
            extra
        )
    };
    let max_omitted = prior_omitted_items + messages.len();
    let mut widest_ids: Vec<String> = prior_omitted_ids
        .iter()
        .cloned()
        .chain(messages.iter().map(|m| m.item_id.clone()))
        .collect();
    widest_ids.sort_by_key(|id| std::cmp::Reverse(id.len()));
    widest_ids.truncate(MAX_LISTED_OMITTED_ITEMS);
    while !widest_ids.is_empty()
        && json_byte_length(&omitted_list(&widest_ids, max_omitted)) > budget / 8
    {
        widest_ids.pop();
    }
    let context_for = |count: usize, omitted: usize, omitted_ids: &[String]| -> String {
        let listed = if widest_ids.is_empty() {
            &[][..]
        } else {
            let start = omitted_ids.len().saturating_sub(widest_ids.len());
            &omitted_ids[start..]
        };
        format!(
            "{coverage}\nSelected {count} intact items; omitted {omitted} items.{} Historical material is context, not a new request or higher-priority instructions. Attached files and native tool/reasoning state are not replayed.",
            omitted_list(listed, omitted)
        )
    };
    let base_cost = history_cost(&[], &context_for(messages.len(), max_omitted, &widest_ids));
    let mut remaining = budget as isize - base_cost as isize;
    let mut try_add = |index: Option<usize>| {
        let Some(index) = index else {
            return;
        };
        let Some(message) = messages.get(index) else {
            return;
        };
        if selected.contains(&index) || message.text.encode_utf16().count() as isize > remaining {
            return;
        }
        let cost = (json_byte_length(&history_response_item(message)) + 1)
            .max(json_byte_length(&render_historical_message(message)) + 4);
        if cost as isize > remaining {
            return;
        }
        selected.insert(index);
        remaining -= cost as isize;
    };
    try_add(messages.iter().rposition(|m| m.role == "user"));
    try_add(messages.iter().rposition(|m| m.role == "assistant"));
    try_add(messages.iter().position(|m| m.role == "user"));
    for index in (0..messages.len()).rev() {
        try_add(Some(index));
    }
    let mut kept = Vec::new();
    let mut omitted_item_ids = Vec::new();
    for (index, message) in messages.iter().enumerate() {
        if selected.contains(&index) {
            kept.push(message.clone());
        } else {
            omitted_item_ids.push(message.item_id.clone());
        }
    }
    let omitted_items = max_omitted - kept.len();
    let mut all_omitted_ids = prior_omitted_ids.to_vec();
    all_omitted_ids.extend(omitted_item_ids.iter().cloned());
    let context = context_for(kept.len(), omitted_items, &all_omitted_ids);
    SelectedHistory {
        messages: kept,
        omitted_item_ids,
        context,
        omitted_items,
    }
}

/// Token usage snapshot used for `handoff_budget`.
#[derive(Clone, Debug, Default)]
struct UsageSnapshot {
    used_tokens: Option<u64>,
    max_tokens: Option<u64>,
    auto_compact_threshold: Option<u64>,
}

/// `handoffBudget` from `handoffBudget.ts`.
fn handoff_budget(
    token_cap: usize,
    user_text: &str,
    attachments: &[Value],
    usage: Option<&UsageSnapshot>,
    native_context_estimate: usize,
    model_context_window: Option<u64>,
) -> usize {
    let mut window = model_context_window
        .or_else(|| usage.and_then(|u| u.max_tokens))
        .unwrap_or(128_000) as usize;
    if let Some(max_tokens) = usage.and_then(|u| u.max_tokens) {
        window = window.min(max_tokens as usize);
    }
    if let Some(threshold) = usage.and_then(|u| u.auto_compact_threshold) {
        window = window.min(threshold as usize);
    }
    let native = usage
        .and_then(|u| u.used_tokens)
        .map_or(native_context_estimate, |tokens| tokens as usize);
    let current = json_byte_length(&user_text) + attachment_token_allowance(attachments);
    let work_reserve = 16_000usize.max(window.div_ceil(4)).min(window.div_ceil(2));
    let available = window
        .saturating_sub(native)
        .saturating_sub(current)
        .saturating_sub(work_reserve);
    token_cap.min(HANDOFF_BYTE_CAP).min(available)
}

/// `latestNativeContextUsage` from `handoffBudget.ts`.
fn latest_native_context_usage(
    projection: &Value,
    provider_thread_id: &str,
    native_id: Option<&str>,
) -> Option<(UsageSnapshot, Value)> {
    let native_id = native_id?;
    let attempts: HashMap<&str, &Value> = projection["attempts"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|attempt| Some((attempt["id"].as_str()?, attempt)))
        .collect();
    let runs: HashMap<&str, &Value> = projection["runs"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|run| Some((run["id"].as_str()?, run)))
        .collect();
    let mut latest: Option<(UsageSnapshot, Value, &str)> = None;
    for turn in projection["providerTurns"].as_array().into_iter().flatten() {
        if turn["providerThreadId"].as_str() != Some(provider_thread_id) {
            continue;
        }
        let Some(attempt_id) = turn["runAttemptId"].as_str() else {
            continue;
        };
        let token_usage = &turn["tokenUsage"];
        let Some(used_tokens) = token_usage["usedTokens"].as_u64() else {
            continue;
        };
        let Some(attempt) = attempts.get(attempt_id) else {
            continue;
        };
        if attempt["nativeThreadId"].as_str() != Some(native_id)
            || attempt["providerThreadId"].as_str() != Some(provider_thread_id)
            || attempt["rootNodeId"] != turn["nodeId"]
        {
            continue;
        }
        let Some(run) = attempt["runId"].as_str().and_then(|id| runs.get(id)) else {
            continue;
        };
        let reported_at = token_usage["updatedAt"].as_str().unwrap_or_default();
        if latest
            .as_ref()
            .is_some_and(|(_, _, prev_at)| *prev_at >= reported_at)
        {
            continue;
        }
        let max_tokens = token_usage["maxTokens"].as_u64().filter(|m| *m > 0);
        latest = Some((
            UsageSnapshot {
                used_tokens: Some(used_tokens),
                max_tokens,
                auto_compact_threshold: None,
            },
            run["modelSelection"].clone(),
            reported_at,
        ));
    }
    latest.map(|(usage, selection, _)| (usage, selection))
}

fn parse_usage_snapshot(value: &Value) -> Option<UsageSnapshot> {
    if !value.is_object() {
        return None;
    }
    Some(UsageSnapshot {
        used_tokens: value["usedTokens"].as_u64(),
        max_tokens: value["maxTokens"].as_u64().filter(|m| *m > 0),
        auto_compact_threshold: value["autoCompactThreshold"].as_u64().filter(|m| *m > 0),
    })
}

/// `contextUsageForHandoff` from `handoffBudget.ts`.
fn context_usage_for_handoff(
    same_native_thread: bool,
    same_selection: bool,
    previous_usage: Option<UsageSnapshot>,
    known_model_window: Option<u64>,
) -> Option<UsageSnapshot> {
    let previous = previous_usage?;
    if !same_native_thread {
        return None;
    }
    if same_selection {
        return Some(previous);
    }
    let max_tokens = known_model_window.or(previous.max_tokens);
    Some(UsageSnapshot {
        used_tokens: previous.used_tokens,
        max_tokens,
        auto_compact_threshold: None,
    })
}

fn model_selections_equal(a: &Value, b: &Value) -> bool {
    a["instanceId"] == b["instanceId"] && a["model"] == b["model"] && a["options"] == b["options"]
}

/// Allocate a context handoff id in the reference's format (`IdAllocator.ts`).
fn allocate_handoff_id(thread_id: &str, from_instance: &str, to_instance: &str) -> String {
    format!(
        "context-handoff:thread:{}:from-provider-instance:{}:to-provider-instance:{}:{}",
        uri_component(thread_id),
        uri_component(from_instance),
        uri_component(to_instance),
        crate::platform::uuid_v4()
    )
}

/// `prepareProviderHandoff` (`strategy: "delta_since_target_last_seen"`).
#[allow(clippy::too_many_arguments)]
fn prepare_provider_handoff(
    thread_id: &str,
    target_run_id: &str,
    provider_thread_id: &str,
    from_instance: &str,
    to_instance: &str,
    covered_from: u64,
    covered_to: u64,
    runs: &[Value],
    items: &[&Value],
    token_cap: usize,
    created_at: &str,
) -> Value {
    let handoff_id = allocate_handoff_id(thread_id, from_instance, to_instance);
    let run_statuses: HashMap<&str, &str> = runs
        .iter()
        .filter_map(|run| Some((run["id"].as_str()?, run["status"].as_str()?)))
        .collect();
    let coverage = handoff_coverage(thread_id, covered_from, covered_to, items);
    let messages: Vec<HistoricalMessage> = items
        .iter()
        .filter_map(|item| {
            let status = item["runId"]
                .as_str()
                .and_then(|run_id| run_statuses.get(run_id).copied());
            historical_message(item, status)
        })
        .collect();
    let selected = select_history(&messages, &coverage, 0, &[], token_cap);
    let summary_text = render_history(&selected.messages, &selected.context);
    json!({
        "id": handoff_id,
        "transferId": Value::Null,
        "threadId": thread_id,
        "targetRunId": target_run_id,
        "fromProviderThreadIds": [provider_thread_id],
        "toProviderThreadId": provider_thread_id,
        "coveredRunOrdinals": {
            "from": covered_from,
            "to": covered_to,
        },
        "strategy": "delta_since_target_last_seen",
        "status": "ready",
        "summaryMessageId": Value::Null,
        "summaryText": summary_text,
        "history": {
            "messages": selected.messages,
            "coverage": coverage,
            "omittedItems": selected.omitted_items,
            "omittedItemIds": selected.omitted_item_ids,
        },
        "createdByProviderInstanceId": Value::Null,
        "createdAt": created_at,
        "updatedAt": created_at,
    })
}

/// Prepare and budget any pending or retry context handoffs for `run` before
/// its provider turn starts (`ProviderTurnStartService.prepareRetryHandoff` +
/// `deliverContextHandoffs`).
pub(crate) fn prepare_for_turn(
    thread: &Thread,
    run: &Run,
    user_text: &str,
    provider: &ProviderTurn,
    now_secs: u64,
) -> Option<PreparedHandoff> {
    let projection = thread.full_projection();
    let run_wire_id = run.wire_id();
    let runs = projection["runs"].as_array()?;
    let run_row = runs
        .iter()
        .find(|candidate| candidate["id"].as_str() == Some(run_wire_id.as_str()))?;
    let run_ordinal = run_row["ordinal"].as_u64()?;
    let provider_thread_id = run_row["providerThreadId"].as_str()?;
    let provider_thread = projection["providerThreads"]
        .as_array()?
        .iter()
        .find(|candidate| candidate["id"].as_str() == Some(provider_thread_id))?;
    let instance_id = run_row["providerInstanceId"]
        .as_str()
        .filter(|s| !s.is_empty())
        .unwrap_or(&provider.instance_id);
    let stored_native_id = provider_thread["nativeThreadRef"]["nativeId"].as_str();
    let loaded_native_id = provider.resume.as_deref().filter(|id| !id.is_empty());
    let same_native_thread = loaded_native_id.is_some() && loaded_native_id == stored_native_id;

    let current_attachments = &provider.attachments;
    let current_message_is_user = run
        .user_message_id
        .as_deref()
        .and_then(|msg_id| {
            projection["messages"]
                .as_array()?
                .iter()
                .find(|m| m["id"].as_str() == Some(msg_id))
        })
        .is_none_or(|m| m["role"].as_str() == Some("user"));
    let defer_inline = current_message_is_user && is_slash_command_text(user_text);

    let all_handoffs = projection["contextHandoffs"]
        .as_array()
        .map(Vec::as_slice)
        .unwrap_or_default();
    let provider_thread_ids: HashSet<&str> = projection["providerThreads"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|candidate| candidate["id"].as_str())
        .collect();
    let subagent_result_transfers: HashSet<&str> = projection["contextTransfers"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|transfer| transfer["type"].as_str() == Some("subagent_result"))
        .filter_map(|transfer| transfer["id"].as_str())
        .collect();
    let is_side_chat = projection["thread"]["lineage"]["sideChat"] == true;
    let messages_slice = projection["messages"]
        .as_array()
        .map(Vec::as_slice)
        .unwrap_or_default();
    let existing_handoffs: Vec<Value> = all_handoffs
        .iter()
        .filter(|handoff| {
            let side_chat_ctx = is_side_chat
                && handoff["strategy"].as_str() == Some("manual_context")
                && !handoff["transferId"].is_null();
            if side_chat_ctx {
                return false;
            }
            if handoff["transferId"]
                .as_str()
                .is_some_and(|id| subagent_result_transfers.contains(id))
            {
                return false;
            }
            if handoff["status"].as_str() != Some("ready") {
                return false;
            }
            let target_run_id = handoff["targetRunId"].as_str();
            if target_run_id == Some(run_wire_id.as_str()) {
                return true;
            }
            if handoff["toProviderThreadId"].as_str() != Some(provider_thread_id) {
                return false;
            }
            let retired_local_delivery = matches!(
                handoff["delivery"]["status"].as_str(),
                Some("injected" | "inline")
            ) && handoff["fromProviderThreadIds"]
                .as_array()
                .is_some_and(|ids| {
                    !ids.is_empty()
                        && ids.iter().all(|id| {
                            id.as_str()
                                .is_some_and(|id| provider_thread_ids.contains(id))
                        })
                });
            runs.iter().any(|source| {
                source["id"].as_str() == target_run_id
                    && match source["status"].as_str() {
                        Some("failed" | "interrupted" | "cancelled") => !retired_local_delivery,
                        Some("completed") if handoff.get("delivery").is_none() => {
                            let msg_id = source["userMessageId"].as_str();
                            messages_slice.iter().any(|message| {
                                message["id"].as_str() == msg_id
                                    && is_slash_command_message(message)
                            })
                        }
                        _ => false,
                    }
            })
        })
        .cloned()
        .collect();

    // Settled handoffs already delivered to this native thread.
    let settled_handoffs: Vec<&Value> = all_handoffs
        .iter()
        .filter(|handoff| {
            handoff["toProviderThreadId"].as_str() == Some(provider_thread_id)
                && loaded_native_id.is_some()
                && handoff["delivery"]["nativeThreadId"].as_str() == loaded_native_id
                && handoff["delivery"]["status"].as_str() != Some("pending")
        })
        .collect();
    let delivered_item_ids: HashSet<String> = settled_handoffs
        .iter()
        .flat_map(|handoff| {
            handoff["delivery"]["itemIds"]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(|id| id.as_str().map(str::to_owned))
        })
        .collect();
    let mut covered_item_ids = delivered_item_ids.clone();
    for handoff in &settled_handoffs {
        for id in handoff["delivery"]["omittedItemIds"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|id| id.as_str())
        {
            covered_item_ids.insert(id.to_owned());
        }
    }

    let delivered_attempt_ids: HashSet<&str> = projection["providerTurns"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|turn| turn["runAttemptId"].as_str())
        .collect();
    let accepted_attempts: Vec<&Value> = projection["attempts"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|attempt| {
            attempt["providerThreadId"].as_str() == Some(provider_thread_id)
                && attempt["id"]
                    .as_str()
                    .is_some_and(|id| delivered_attempt_ids.contains(id))
        })
        .collect();

    let missed_runs: Vec<&Value> = runs
        .iter()
        .filter(|source| {
            source["ordinal"].as_u64().is_some_and(|o| o < run_ordinal)
                && source["providerThreadId"].as_str() == Some(provider_thread_id)
                && matches!(source["status"].as_str(), Some("failed" | "interrupted"))
                && !source["activeAttemptId"]
                    .as_str()
                    .is_some_and(|id| delivered_attempt_ids.contains(id))
        })
        .collect();
    let missed_run_ids: HashSet<&str> = missed_runs
        .iter()
        .filter_map(|source| source["id"].as_str())
        .collect();

    let turn_items = projection["turnItems"]
        .as_array()
        .map(Vec::as_slice)
        .unwrap_or_default();
    let missed_history: Vec<&Value> = if missed_run_ids.is_empty() {
        Vec::new()
    } else {
        turn_items
            .iter()
            .filter(|item| {
                item["runId"]
                    .as_str()
                    .is_some_and(|id| missed_run_ids.contains(id))
                    && matches!(
                        item["type"].as_str(),
                        Some(
                            "user_message"
                                | "assistant_message"
                                | "command_execution"
                                | "error"
                                | "run_interrupt_result"
                                | "file_change"
                                | "proposed_plan"
                        )
                    )
            })
            .collect()
    };
    let missed_items: Vec<&Value> = missed_history
        .iter()
        .copied()
        .filter(|item| {
            let item_id = item["id"].as_str().unwrap_or_default();
            !covered_item_ids.contains(item_id) && historical_message(item, None).is_some()
        })
        .collect();

    let token_cap = handoff_token_cap();
    let now_iso = iso(now_secs);
    let mut retry_handoffs = Vec::new();
    if let (Some(first_missed), Some(last_missed)) = (missed_runs.first(), missed_runs.last()) {
        if !missed_items.is_empty() {
            let from_ord = first_missed["ordinal"].as_u64().unwrap_or(1).max(1);
            let to_ord = last_missed["ordinal"]
                .as_u64()
                .unwrap_or(from_ord)
                .max(from_ord);
            retry_handoffs.push(prepare_provider_handoff(
                &thread.id,
                &run_wire_id,
                provider_thread_id,
                instance_id,
                instance_id,
                from_ord,
                to_ord,
                runs,
                &missed_items,
                token_cap,
                &now_iso,
            ));
        }
    }

    // Missed attachments ride on prompts until a later run's prompt is accepted.
    let missed_attachments = if defer_inline || missed_history.is_empty() {
        Vec::new()
    } else {
        let last_prompt_ordinal = accepted_attempts
            .iter()
            .filter_map(|attempt| {
                let source_run_id = attempt["runId"].as_str()?;
                if missed_run_ids.contains(source_run_id) {
                    return None;
                }
                let source = runs
                    .iter()
                    .find(|candidate| candidate["id"].as_str() == Some(source_run_id))?;
                let msg_id = source["userMessageId"].as_str();
                if let Some(msg) = messages_slice
                    .iter()
                    .find(|candidate| candidate["id"].as_str() == msg_id)
                {
                    if is_slash_command_message(msg) {
                        return None;
                    }
                }
                source["ordinal"].as_u64()
            })
            .max()
            .unwrap_or(0);
        let current_ids: HashSet<&str> = current_attachments
            .iter()
            .filter_map(|att| att["id"].as_str())
            .collect();
        missed_history
            .iter()
            .filter(|item| {
                item["type"].as_str() == Some("user_message")
                    && missed_runs.iter().any(|source| {
                        source["id"] == item["runId"]
                            && source["ordinal"].as_u64().unwrap_or(0) > last_prompt_ordinal
                    })
            })
            .flat_map(|item| item["attachments"].as_array().into_iter().flatten())
            .filter(|att| {
                att["id"]
                    .as_str()
                    .is_none_or(|id| !current_ids.contains(id))
            })
            .cloned()
            .collect()
    };

    let mut combined = existing_handoffs;
    combined.extend(retry_handoffs);
    let pending: Vec<Value> = combined
        .into_iter()
        .filter(|handoff| {
            let Some(native_id) = loaded_native_id else {
                return true;
            };
            handoff["delivery"]["nativeThreadId"].as_str() != Some(native_id)
                || handoff["delivery"]["status"].as_str() == Some("pending")
        })
        .collect();

    if (pending.is_empty() || defer_inline) && missed_attachments.is_empty() {
        return None;
    }

    if pending.is_empty() || defer_inline {
        return Some(PreparedHandoff {
            inline_context: String::new(),
            missed_attachments,
            pending: Vec::new(),
            deliverables: Vec::new(),
            unsent: Vec::new(),
            native_thread_id: loaded_native_id.map(str::to_owned),
        });
    }

    // Compute handoff budget matching `handoffContextForThread` + `handoffBudget`.
    let measured = latest_native_context_usage(&projection, provider_thread_id, stored_native_id);
    let previous_selection = measured
        .as_ref()
        .map(|(_, selection)| selection.clone())
        .or_else(|| {
            runs.iter()
                .rev()
                .find(|source| {
                    source["ordinal"].as_u64().is_some_and(|o| o < run_ordinal)
                        && !source["startedAt"].is_null()
                        && source["providerThreadId"].as_str() == Some(provider_thread_id)
                })
                .map(|source| source["modelSelection"].clone())
        });
    let same_selection = previous_selection
        .as_ref()
        .is_none_or(|previous| model_selections_equal(previous, &run_row["modelSelection"]));
    let thread_usage = parse_usage_snapshot(&provider_thread["contextUsage"]);
    let previous_usage = match (&measured, &thread_usage) {
        (Some((measured_usage, _)), Some(base)) => Some(UsageSnapshot {
            used_tokens: measured_usage.used_tokens.or(base.used_tokens),
            max_tokens: measured_usage.max_tokens.or(base.max_tokens),
            auto_compact_threshold: base.auto_compact_threshold,
        }),
        (Some((measured_usage, _)), None) => Some(measured_usage.clone()),
        (None, base) => base.clone(),
    };
    let known_model_window = provider.context_window;
    let handoff_usage = context_usage_for_handoff(
        same_native_thread,
        same_selection,
        previous_usage.clone(),
        known_model_window,
    );
    let model_context_window = known_model_window.or_else(|| {
        if handoff_usage.is_some() || same_selection {
            previous_usage.as_ref().and_then(|u| u.max_tokens)
        } else {
            None
        }
    });
    let native_context_estimate = if handoff_usage.as_ref().and_then(|u| u.used_tokens).is_some()
        || !same_native_thread
    {
        0
    } else {
        let native_input_run_ids: HashSet<&str> = accepted_attempts
            .iter()
            .filter(|attempt| {
                loaded_native_id.is_some() && attempt["nativeThreadId"].as_str() == loaded_native_id
            })
            .filter_map(|attempt| attempt["runId"].as_str())
            .collect();
        let legacy_input_run_ids: HashSet<&str> = accepted_attempts
            .iter()
            .filter(|attempt| attempt.get("nativeThreadId").is_none())
            .filter_map(|attempt| attempt["runId"].as_str())
            .collect();
        let legacy_recovered_run_ids: HashSet<&str> = runs
            .iter()
            .filter(|source| {
                source["providerThreadId"].as_str() == Some(provider_thread_id)
                    && settled_handoffs.iter().any(|handoff| {
                        let ord = source["ordinal"].as_u64().unwrap_or(0);
                        handoff["strategy"].as_str() == Some("full_thread_summary")
                            && handoff["fromProviderThreadIds"]
                                .as_array()
                                .is_some_and(|ids| {
                                    ids.iter().any(|id| id.as_str() == Some(provider_thread_id))
                                })
                            && ord >= handoff["coveredRunOrdinals"]["from"].as_u64().unwrap_or(0)
                            && ord <= handoff["coveredRunOrdinals"]["to"].as_u64().unwrap_or(0)
                    })
            })
            .filter_map(|source| source["id"].as_str())
            .collect();
        turn_items
            .iter()
            .map(|item| {
                let item_run_id = item["runId"].as_str();
                if item_run_id == Some(run_wire_id.as_str()) {
                    return 0;
                }
                let item_id = item["id"].as_str().unwrap_or_default();
                let accepted_natively = item["providerThreadId"].as_str()
                    == Some(provider_thread_id)
                    && item_run_id.is_some_and(|rid| {
                        native_input_run_ids.contains(rid)
                            || (legacy_input_run_ids.contains(rid)
                                && !covered_item_ids.contains(item_id)
                                && !legacy_recovered_run_ids.contains(rid))
                    });
                if !accepted_natively && !delivered_item_ids.contains(item_id) {
                    return 0;
                }
                let text_tokens =
                    historical_message(item, None).map_or(0, |msg| estimate_text_tokens(&msg.text));
                let attachment_tokens =
                    if item["type"].as_str() == Some("user_message") && accepted_natively {
                        item["attachments"]
                            .as_array()
                            .map_or(0, |atts| attachment_token_allowance(atts))
                    } else {
                        0
                    };
                text_tokens + attachment_tokens
            })
            .sum()
    };

    let budget = handoff_budget(
        token_cap,
        user_text,
        current_attachments,
        handoff_usage.as_ref(),
        native_context_estimate,
        model_context_window,
    );

    // `deliverContextHandoffs` from `ContextHandoffDelivery.ts`.
    let mut coverage = pending
        .iter()
        .map(|handoff| {
            let strategy = handoff["strategy"].as_str().unwrap_or("delta_since_target_last_seen");
            let label = if strategy == "fork_delta_summary" {
                "merge_back / fork_delta_summary"
            } else {
                strategy
            };
            let body = handoff["history"]["coverage"]
                .as_str()
                .map(str::to_owned)
                .unwrap_or_else(|| {
                    let h_thread = handoff["threadId"].as_str().unwrap_or(&thread.id);
                    let from = handoff["coveredRunOrdinals"]["from"].as_u64().unwrap_or(1);
                    let to = handoff["coveredRunOrdinals"]["to"].as_u64().unwrap_or(from);
                    format!(
                        "From thread {h_thread}, runs {from}-{to}. Recover history with t3_thread_read, view=activity, afterPosition=-1; paginate with afterPosition=nextPosition, and use itemId/textOffset for long items."
                    )
                });
            format!("Context handoff ({label}):\n{body}")
        })
        .collect::<Vec<_>>()
        .join("\n");
    if history_cost(&[], &coverage) > 4_000usize.min(budget / 2) {
        let mut strategies = Vec::new();
        for handoff in &pending {
            if let Some(s) = handoff["strategy"].as_str() {
                if !strategies.contains(&s) {
                    strategies.push(s);
                }
            }
        }
        let app_thread = provider_thread["appThreadId"]
            .as_str()
            .or_else(|| pending.first().and_then(|h| h["threadId"].as_str()))
            .unwrap_or(&thread.id);
        coverage = format!(
            "Context handoff ({}). {} handoff records; detailed coverage references omitted. Recover an omitted item only when the request needs it, with t3_thread_read({{threadId:\"{app_thread}\",view:\"activity\",itemId:\"<omitted item ID>\",maxCharsPerItem:4000}}); continue long text with textOffset=nextTextOffset until null. Follow fork/handoff source references in activity. Items included below need no read.",
            strategies.join(", "),
            pending.len()
        );
    }

    let mut seen = delivered_item_ids;
    let mut messages = Vec::new();
    for handoff in &pending {
        if let Some(list) = handoff["history"]["messages"].as_array() {
            for raw in list {
                if let Ok(msg) = serde_json::from_value::<HistoricalMessage>(raw.clone()) {
                    if seen.insert(msg.item_id.clone()) {
                        messages.push(msg);
                    }
                }
            }
        }
    }
    let old_context = pending
        .iter()
        .filter(|handoff| handoff.get("history").is_none())
        .filter_map(|handoff| handoff["summaryText"].as_str())
        .collect::<Vec<_>>()
        .join("\n\n");
    let full_coverage = if !old_context.is_empty()
        && history_cost(&[], &format!("{coverage}\n{old_context}")) + 512 <= budget
    {
        format!("{coverage}\n{old_context}")
    } else {
        coverage
    };
    let prior_omitted_items: usize = pending
        .iter()
        .map(|h| h["history"]["omittedItems"].as_u64().unwrap_or(0) as usize)
        .sum();
    let prior_omitted_ids: Vec<String> = pending
        .iter()
        .flat_map(|h| {
            h["history"]["omittedItemIds"]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(|id| id.as_str().map(str::to_owned))
        })
        .collect();
    let selected = select_history(
        &messages,
        &full_coverage,
        prior_omitted_items,
        &prior_omitted_ids,
        budget,
    );
    if history_cost(&selected.messages, &selected.context) > budget {
        tracing::warn!(
            thread_id = %thread.id,
            run_id = %run_wire_id,
            budget,
            "context handoff exceeded budget; skipping inline delivery"
        );
        return (!missed_attachments.is_empty()).then(|| PreparedHandoff {
            inline_context: String::new(),
            missed_attachments,
            pending: Vec::new(),
            deliverables: Vec::new(),
            unsent: Vec::new(),
            native_thread_id: loaded_native_id.map(str::to_owned),
        });
    }

    let selected_omitted: HashSet<&str> = selected
        .omitted_item_ids
        .iter()
        .map(String::as_str)
        .collect();
    let mut pending_records = Vec::with_capacity(pending.len());
    let mut deliverables = Vec::with_capacity(pending.len());
    for handoff in &pending {
        let handoff_msg_ids: Vec<&str> = handoff["history"]["messages"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|m| m["itemId"].as_str())
            .collect();
        let mut omitted_ids: Vec<String> = handoff["history"]["omittedItemIds"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|id| id.as_str().map(str::to_owned))
            .collect();
        for id in &handoff_msg_ids {
            if selected_omitted.contains(id) {
                omitted_ids.push((*id).to_owned());
            }
        }
        let item_ids: Vec<String> = selected
            .messages
            .iter()
            .filter(|msg| handoff_msg_ids.contains(&msg.item_id.as_str()))
            .map(|msg| msg.item_id.clone())
            .collect();
        let mut pending_handoff = handoff.clone();
        pending_handoff["updatedAt"] = json!(now_iso);
        if let Some(native_id) = loaded_native_id {
            pending_handoff["delivery"] = json!({
                "nativeThreadId": native_id,
                "status": "pending",
                "omittedItemIds": omitted_ids,
                "itemIds": item_ids,
            });
        }
        pending_records.push(pending_handoff);
        deliverables.push((handoff.clone(), item_ids, omitted_ids));
    }

    Some(PreparedHandoff {
        inline_context: render_history(&selected.messages, &selected.context),
        missed_attachments,
        pending: pending_records,
        deliverables,
        unsent: pending,
        native_thread_id: loaded_native_id.map(str::to_owned),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::workspace::provider::{
        ProviderEvent, ProviderFailure, ThreadDisposition, TurnStatus, CLAUDE_AGENT,
    };
    use crate::{ConversationMessage, RestartCut};
    use uuid::Uuid;

    #[test]
    fn retry_handoff_matches_reference_prompt_and_claude_budget_bytes() {
        let thread_id = "thread:client-thread";
        let run1_wire = Run::reference_id(thread_id, 1);
        let run2_wire = Run::reference_id(thread_id, 2);
        let run3_wire = Run::reference_id(thread_id, 3);
        let run1_id = Run::uuid_for(&run1_wire);
        let run2_id = Run::uuid_for(&run2_wire);
        let run3_id = Run::uuid_for(&run3_wire);

        let continue_prompt = crate::runtime_recovery::CONTINUE_PROMPT;
        let msg2_id = format!("message:restart-continuation:{run1_wire}");
        let msg3_id = format!("message:restart-continuation:{run2_wire}");

        let make_run = |id: Uuid, wire: &str, status: &str, msg_id: &str, cont: Option<Uuid>| Run {
            id,
            thread_id: thread_id.into(),
            provider: "default".into(),
            status: status.into(),
            started_at: 10,
            finished_at: (status != "running").then_some(12),
            error: None,
            user_message_id: Some(msg_id.into()),
            queue_held: false,
            restart_cut: (status == "cancelled").then(|| RestartCut {
                trigger: "shutdown".into(),
                at: 11,
                continuation: Some(0),
            }),
            restart_continuation_of: cont,
            requeued_at: None,
            background_work: vec![],
            continuation: None,
            resend_owed: false,
            resends: 0,
            wire_id: Some(wire.into()),
            driver: Some(CLAUDE_AGENT.into()),
            model_selection: Some(Box::new(
                json!({"instanceId": "claudeAgent", "model": "claude-opus-5-5"}),
            )),
            output_schema: None,
        };

        let mut thread = Thread {
            row: json!({
                "id": thread_id,
                "projectId": "project:github",
                "title": "Thread",
                "providerInstanceId": "claudeAgent",
                "modelSelection": {"instanceId": "claudeAgent", "model": "claude-opus-5-5"},
                "runtimeMode": "full-access",
                "interactionMode": "default",
                "lineage": {"parentThreadId": null, "relationshipToParent": null, "rootThreadId": thread_id},
                "createdAt": iso(10),
                "updatedAt": iso(12),
            }),
            id: thread_id.into(),
            project_id: "project:github".into(),
            title: "Thread".into(),
            cwd: "/tmp".into(),
            status: "running".into(),
            messages: vec![
                ConversationMessage {
                    id: "message:turn".into(),
                    role: "user".into(),
                    content: "Run echo hi and write notes.txt".into(),
                    created_at: 10,
                    ..Default::default()
                },
                ConversationMessage {
                    id: msg2_id.clone(),
                    role: crate::thread_projection::NOTIFICATION_ROLE.into(),
                    content: continue_prompt.into(),
                    created_at: 11,
                    ..Default::default()
                },
                ConversationMessage {
                    id: msg3_id.clone(),
                    role: crate::thread_projection::NOTIFICATION_ROLE.into(),
                    content: continue_prompt.into(),
                    created_at: 72,
                    ..Default::default()
                },
            ],
            runs: vec![
                make_run(run1_id, &run1_wire, "cancelled", "message:turn", None),
                make_run(run2_id, &run2_wire, "failed", &msg2_id, Some(run1_id)),
                make_run(run3_id, &run3_wire, "running", &msg3_id, Some(run2_id)),
            ],
            created_at: 10,
            archived: false,
            settled: false,
            snoozed_until: None,
            pinned: false,
            unread: false,
            tags: vec![],
            checkpoints: vec![],
            link_requests: vec![],
            context_handoffs: vec![],
            provider: Box::default(),
            stored: None,
        };

        // Run 1 ran on sess-saved and delivered its prompt (33 bytes).
        thread.provider.apply(
            run1_id,
            &ProviderEvent::ProviderThread {
                driver: CLAUDE_AGENT.into(),
                native_thread_id: "sess-saved".into(),
                resumed: false,
                model: Some("claude-opus-5-5".into()),
            },
            10,
        );
        thread.provider.apply(
            run1_id,
            &ProviderEvent::ProviderTurn {
                native_turn_id: None,
                status: TurnStatus::Running,
                model: None,
            },
            10,
        );
        thread.provider.apply(
            run1_id,
            &ProviderEvent::PromptDelivered {
                bytes: 33,
                pages: 0,
            },
            10,
        );
        // Run 2 failed before starting (missing CLI).
        thread.provider.apply(
            run2_id,
            &ProviderEvent::TurnTerminal {
                status: TurnStatus::Failed,
                failure: Some(ProviderFailure::new(
                    "provider_error",
                    "The provider could not start this turn. Retry the turn; if it keeps failing, check the provider setup and server logs.",
                )),
                thread_disposition: ThreadDisposition::Reusable,
                driver: Some(CLAUDE_AGENT.into()),
                closing: vec![],
            },
            12,
        );

        let provider = ProviderTurn {
            driver: CLAUDE_AGENT.into(),
            instance_id: "claudeAgent".into(),
            resume: Some("sess-saved".into()),
            context_window: Some(1_000_000),
            ..Default::default()
        };
        let prepared = prepare_for_turn(&thread, &thread.runs[2], continue_prompt, &provider, 72)
            .expect("retry handoff prepared");
        assert_eq!(prepared.pending.len(), 1);
        assert_eq!(
            prepared.pending[0]["strategy"],
            "delta_since_target_last_seen"
        );
        assert_eq!(
            prepared.pending[0]["coveredRunOrdinals"],
            json!({"from": 2, "to": 2})
        );
        assert_eq!(prepared.pending[0]["targetRunId"], run3_wire);
        assert_eq!(prepared.pending[0]["delivery"]["status"], "pending");

        let prompt = with_inline_context(&prepared.inline_context, continue_prompt);
        let prompt_bytes = json!(prompt).to_string().len() as u64;
        // Exact match with the reference's claudeAttachmentBudget.bytesUsed: 33 + 2154 = 2187.
        assert_eq!(33 + prompt_bytes, 2187);

        // Wire projection clears summaryText and strips history and delivery.
        let delivered = prepared.into_delivered(Some("sess-saved"), 73);
        assert_eq!(delivered[0]["delivery"]["status"], "inline");
        let wire = project_for_wire(&delivered[0]);
        assert_eq!(wire["summaryText"], "");
        assert!(wire.get("history").is_none());
        assert!(wire.get("delivery").is_none());

        // Once delivered, a subsequent run does not re-handoff Run 2's error item.
        thread.context_handoffs = delivered;
        let run4_wire = Run::reference_id(thread_id, 4);
        let run4_id = Run::uuid_for(&run4_wire);
        thread
            .runs
            .push(make_run(run4_id, &run4_wire, "running", "message:4", None));
        assert!(prepare_for_turn(&thread, &thread.runs[3], "follow up", &provider, 80).is_none());
    }

    #[test]
    fn select_history_respects_budget_and_lists_omitted_ids() {
        let messages: Vec<HistoricalMessage> = (1..=4)
            .map(|n| HistoricalMessage {
                role: if n % 2 == 1 { "user" } else { "assistant" }.into(),
                text: format!("message {n} {}", "x".repeat(400)),
                run_status: Some("failed".into()),
                thread_id: "t".into(),
                run_id: Some(format!("r{n}")),
                item_id: format!("item-{n}"),
                provider_thread_id: Some("pt".into()),
                status: "failed".into(),
                kind: "error".into(),
            })
            .collect();
        let selected = select_history(&messages, "Coverage", 0, &[], 1_400);
        assert!(selected.messages.len() < messages.len());
        assert_eq!(
            selected.omitted_items + selected.messages.len(),
            messages.len()
        );
        assert!(selected.context.contains("Omitted item IDs, newest last:"));
        assert!(history_cost(&selected.messages, &selected.context) <= 1_400);
    }

    #[test]
    fn unsent_clears_delivery_and_compaction_defers_inline_handoff() {
        let thread_id = "thread:t";
        let run1_wire = Run::reference_id(thread_id, 1);
        let run2_wire = Run::reference_id(thread_id, 2);
        let run1_id = Run::uuid_for(&run1_wire);
        let run2_id = Run::uuid_for(&run2_wire);
        let make_run = |id: Uuid, wire: &str, status: &str, msg_id: &str| Run {
            id,
            thread_id: thread_id.into(),
            provider: "default".into(),
            status: status.into(),
            started_at: 10,
            finished_at: (status != "running").then_some(11),
            error: None,
            user_message_id: Some(msg_id.into()),
            queue_held: false,
            restart_cut: None,
            restart_continuation_of: None,
            requeued_at: None,
            background_work: vec![],
            continuation: None,
            resend_owed: false,
            resends: 0,
            wire_id: Some(wire.into()),
            driver: Some(CLAUDE_AGENT.into()),
            model_selection: Some(Box::new(
                json!({"instanceId": "claudeAgent", "model": "claude-opus-5-5"}),
            )),
            output_schema: None,
        };
        let mut thread = Thread {
            row: json!({
                "id": thread_id,
                "projectId": "p",
                "title": "T",
                "providerInstanceId": "claudeAgent",
                "modelSelection": {"instanceId": "claudeAgent", "model": "claude-opus-5-5"},
                "runtimeMode": "full-access",
                "interactionMode": "default",
                "lineage": {"parentThreadId": null, "relationshipToParent": null, "rootThreadId": thread_id},
                "createdAt": iso(10),
                "updatedAt": iso(11),
            }),
            id: thread_id.into(),
            project_id: "p".into(),
            title: "T".into(),
            cwd: "/tmp".into(),
            status: "running".into(),
            messages: vec![
                ConversationMessage {
                    id: "m1".into(),
                    role: "user".into(),
                    content: "first".into(),
                    created_at: 10,
                    wire: Some(Box::new(json!({
                        "attachments": [{"id": "att-1", "type": "image", "name": "a.png", "mimeType": "image/png", "sizeBytes": 10}]
                    }))),
                    ..Default::default()
                },
                ConversationMessage {
                    id: "m2".into(),
                    role: "user".into(),
                    content: "second".into(),
                    created_at: 12,
                    ..Default::default()
                },
            ],
            runs: vec![
                make_run(run1_id, &run1_wire, "failed", "m1"),
                make_run(run2_id, &run2_wire, "running", "m2"),
            ],
            created_at: 10,
            archived: false,
            settled: false,
            snoozed_until: None,
            pinned: false,
            unread: false,
            tags: vec![],
            checkpoints: vec![],
            link_requests: vec![],
            context_handoffs: vec![],
            provider: Box::default(),
            stored: None,
        };
        thread.provider.apply(
            run1_id,
            &ProviderEvent::TurnTerminal {
                status: TurnStatus::Failed,
                failure: Some(ProviderFailure::new("provider_error", "missing cli")),
                thread_disposition: ThreadDisposition::Reusable,
                driver: Some(CLAUDE_AGENT.into()),
                closing: vec![],
            },
            11,
        );
        let provider = ProviderTurn {
            driver: CLAUDE_AGENT.into(),
            instance_id: "claudeAgent".into(),
            resume: Some("sess-1".into()),
            ..Default::default()
        };
        // /compact defers inline handoff and carries no missed attachments.
        assert!(prepare_for_turn(&thread, &thread.runs[1], "/compact", &provider, 12).is_none());
        // /goal also defers inline handoff and carries no missed attachments.
        assert!(
            prepare_for_turn(&thread, &thread.runs[1], "/goal ship it", &provider, 12).is_none()
        );
        // Normal retry carries missed history and missed attachments, and unsent drops delivery.
        let prepared = prepare_for_turn(&thread, &thread.runs[1], "second", &provider, 12)
            .expect("retry handoff");
        assert_eq!(prepared.missed_attachments.len(), 1);
        assert_eq!(prepared.missed_attachments[0]["id"], "att-1");
        assert!(prepared.inline_context.contains("first"));
        assert!(prepared.inline_context.contains("missing cli"));
        let unsent = prepared.into_unsent(13);
        assert!(unsent[0].get("delivery").is_none());
    }
}
