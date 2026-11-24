//! `feedback_request.record`, internal to the server: shows or settles the
//! card in which the user approves a feedback report an agent was asked to
//! send (`dispatchFeedbackRequestRecord` in the reference). A decided card
//! stays decided, and an answered card keeps only what was sent.
use super::CommandHost;
use crate::{CommandReceipt, Event};
use serde_json::{json, Value};
use std::sync::Arc;

pub(super) async fn record<H: CommandHost>(
    state: Arc<H>,
    command: &Value,
    receipt: &CommandReceipt,
) -> anyhow::Result<u64> {
    let thread_id = command["threadId"].as_str().unwrap_or_default().to_owned();
    let run_id = command["runId"].as_str().unwrap_or_default();
    let card_id = command["turnItemId"].as_str().unwrap_or_default();
    let status = command["feedbackStatus"].as_str().unwrap_or_default();
    let pending = status == "pending";
    let card = {
        let store = state.store.read().await;
        let thread = store
            .threads
            .get(&thread_id)
            .ok_or_else(|| anyhow::anyhow!("thread not found"))?;
        let existing = thread.link_requests.iter().find(|c| c["id"] == card_id);
        if existing.is_some_and(|c| c["type"] != "feedback_request" || c["runId"] != run_id) {
            anyhow::bail!("Turn item {card_id} is not a feedback report of run {run_id}.");
        }
        let unchanged = existing.is_some_and(|c| c["feedbackStatus"] != "pending" || pending);
        if unchanged {
            None
        } else {
            let run = thread
                .runs
                .iter()
                .find(|run| run.id.to_string() == run_id || run.wire_id() == run_id)
                .ok_or_else(|| anyhow::anyhow!("Run {run_id} does not exist."))?;
            let ids = thread.provider_run_ids(run);
            if existing.is_none()
                && ids.as_ref().map(|ids| ids.root_node.as_str()) != command["nodeId"].as_str()
            {
                anyhow::bail!(
                    "Node {} is not the root of run {run_id}.",
                    command["nodeId"].as_str().unwrap_or_default()
                );
            }
            let at = crate::thread_projection::iso(crate::now());
            let keep = |key: &str, fallback: Value| existing.map_or(fallback, |c| c[key].clone());
            let mut card = json!({
                "id": card_id,
                "threadId": thread_id,
                "runId": run_id,
                "nodeId": keep("nodeId", command["nodeId"].clone()),
                "providerThreadId": keep("providerThreadId", json!(ids.as_ref().map(|i| i.provider_thread.clone()))),
                "providerTurnId": keep("providerTurnId", json!(ids.as_ref().map(|i| i.provider_turn.clone()))),
                "nativeItemRef": null,
                "parentItemId": null,
                "status": match status {
                    "pending" => "waiting",
                    "sent" => "completed",
                    _ => "cancelled",
                },
                "title": "Feedback for the T3 team",
                "startedAt": keep("startedAt", json!(at)),
                "completedAt": if pending { Value::Null } else { json!(at) },
                "updatedAt": at,
                "type": "feedback_request",
                "preview": command["preview"],
            });
            if command["conversationTrace"].is_object() {
                card["conversationTrace"] = command["conversationTrace"].clone();
            }
            card["feedbackStatus"] = json!(status);
            card[crate::thread_projection::LINK_CARD_POSITION] = match existing {
                Some(existing) => existing[crate::thread_projection::LINK_CARD_POSITION].clone(),
                None => {
                    let index = thread.runs.iter().position(|r| r.id == run.id).unwrap_or(0);
                    json!(thread
                        .provider_records()
                        .items
                        .iter()
                        .filter(|(i, _)| *i == index)
                        .count())
                }
            };
            Some(card)
        }
    };
    let event = match card {
        Some(card) => Event::LinkRequestRecorded {
            thread_id: thread_id.clone(),
            card,
        },
        None => Event::CommandDispatched {
            command_id: receipt.command_id.clone(),
            command_type: "feedback_request.record".into(),
            thread_id: Some(thread_id.clone()),
            payload: command.clone(),
        },
    };
    Ok(state
        .commit_receipted(event, Some(receipt.clone()))
        .await?
        .id)
}
