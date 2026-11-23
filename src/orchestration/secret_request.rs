//! `secret_request.record`, internal to the server: records or updates the
//! card in which an agent asked the user for a secret
//! (`dispatchSecretRequestRecord` in the reference). The secret itself never
//! goes through orchestration; the card only says how the request stands.
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
    let status = command["secretStatus"].as_str().unwrap_or_default();
    let pending = status == "pending";
    let (card, unsnooze) = {
        let store = state.store.read().await;
        let thread = store
            .threads
            .get(&thread_id)
            .ok_or_else(|| anyhow::anyhow!("thread not found"))?;
        let run = thread
            .runs
            .iter()
            .find(|run| run.id.to_string() == run_id || run.wire_id() == run_id);
        let ids = run.and_then(|run| thread.provider_run_ids(run));
        let (Some(run), Some(ids)) = (run, ids.filter(|ids| command["nodeId"] == ids.root_node))
        else {
            anyhow::bail!(
                "Node {} is not the root of run {run_id}.",
                command["nodeId"].as_str().unwrap_or_default()
            );
        };
        let existing = thread.link_requests.iter().find(|c| c["id"] == card_id);
        if existing.is_some_and(|c| c["type"] != "secret_request") {
            anyhow::bail!("Turn item {card_id} is not a secret request.");
        }
        if existing.is_some_and(|c| c["runId"] != run_id) {
            anyhow::bail!("Secret request {card_id} belongs to another run.");
        }
        // A request is answered once, and a retry finds its card as it was asked.
        let unchanged = existing.is_some_and(|c| c["secretStatus"] != "pending" || pending);
        let card = (!unchanged).then(|| {
            let at = crate::thread_projection::iso(crate::now());
            let mut card = json!({
                "id": card_id,
                "threadId": thread_id,
                "runId": run_id,
                "nodeId": command["nodeId"],
                "providerThreadId": ids.provider_thread,
                "providerTurnId": ids.provider_turn,
                "nativeItemRef": null,
                "parentItemId": null,
                "status": match status {
                    "pending" => "waiting",
                    "saved" => "completed",
                    _ => "cancelled",
                },
                "title": command["label"],
                "startedAt": existing.map_or(json!(at), |c| c["startedAt"].clone()),
                "completedAt": if pending { Value::Null } else { json!(at) },
                "updatedAt": at,
                "type": "secret_request",
                "label": command["label"],
                "reason": command["reason"],
            });
            for key in ["url", "placeholder", "delivery"] {
                if let Some(value) = command.get(key).filter(|v| !v.is_null()) {
                    card[key] = value.clone();
                }
            }
            card["secretStatus"] = json!(status);
            // Where the card sits among its run's items: after those it had
            // when it was asked, which an answer keeps.
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
            card
        });
        // A saved secret wakes a snoozed thread, as the reference's
        // `thread.unsnoozed`.
        let unsnooze =
            card.is_some() && status == "saved" && !thread.stored_row()["snoozedUntil"].is_null();
        (card, unsnooze)
    };
    if unsnooze {
        let at = json!(crate::auth_service::iso(crate::auth_service::now()));
        state
            .commit(Event::ThreadPatched {
                thread_id: thread_id.clone(),
                patch: json!({
                    "snoozed_until": null,
                    "row": {"updatedAt": at, "snoozedUntil": null, "snoozedAt": null},
                }),
            })
            .await?;
    }
    let event = match card {
        Some(card) => Event::LinkRequestRecorded {
            thread_id: thread_id.clone(),
            card,
        },
        None => Event::CommandDispatched {
            command_id: receipt.command_id.clone(),
            command_type: "secret_request.record".into(),
            thread_id: Some(thread_id.clone()),
            payload: command.clone(),
        },
    };
    Ok(state
        .commit_receipted(event, Some(receipt.clone()))
        .await?
        .id)
}
