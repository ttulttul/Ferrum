//! A `message.dispatch` that steers the thread's running run
//! (`dispatchMode: steer_active`), as the reference's orchestrator decides
//! and carries it out (`resolveMessageDispatchIntent`'s steer checks and
//! `dispatchSteerIntoRun`'s active steering).
use super::CommandHost;
use crate::{CommandReceipt, ConversationMessage, Event};
use serde_json::{json, Value};
use std::sync::Arc;

/// What a steer message does.
pub(super) enum SteerPlan {
    /// Starts its own run: the target is gone or its turn has ended.
    Start,
    /// Waits behind the target, as a queued message: the target has no
    /// running provider turn yet, a fresh context was asked for, or its
    /// provider cannot be steered by this server.
    Queue,
    /// Joins the target's running provider turn.
    Steer(Target),
}

pub(super) struct Target {
    run: uuid::Uuid,
    wire_id: String,
    steer: Value,
}

fn error(message: String) -> anyhow::Error {
    anyhow::anyhow!(message)
}

/// Decide what the steer message `command` does on `thread_id`.
pub(super) async fn plan<H: CommandHost>(
    state: &Arc<H>,
    thread_id: &str,
    command: &Value,
) -> anyhow::Result<SteerPlan> {
    let store = state.store.read().await;
    let Some(thread) = store.threads.get(thread_id) else {
        return Ok(SteerPlan::Start);
    };
    let target_id = command["dispatchMode"]["targetRunId"]
        .as_str()
        .unwrap_or_default();
    let target = thread.runs.iter().find(|run| run.wire_id() == target_id);
    let record = target.and_then(|run| thread.provider.runs.get(&run.id));
    let ended = target.and_then(|run| thread.provider.terminals.get(&run.id));
    let turn_running = record.is_some_and(|record| record.turn_started) && ended.is_none();
    // Too late to steer: the message is preserved as a new turn.
    if let Some(run) = target {
        let turn_completed = ended
            .is_some_and(|end| end.status == crate::workspace::provider::TurnStatus::Completed);
        if run.status == "completed" || (run.status == "running" && turn_completed) {
            return Ok(SteerPlan::Start);
        }
    }
    if command["freshContext"] == true {
        return Ok(SteerPlan::Queue);
    }
    // Not started yet: it waits for the run, as the reference's steer when
    // ready queues it.
    if target.is_some_and(|run| run.status == "running") && !turn_running {
        return Ok(SteerPlan::Queue);
    }
    let Some(run) = target else {
        return Err(error(format!("Target run {target_id} was not found.")));
    };
    if let Some(message) = run
        .user_message_id
        .as_deref()
        .and_then(|id| thread.messages.iter().find(|m| m.id == id))
    {
        match message.content.trim().to_lowercase().as_str() {
            "/compact" => {
                return Err(error(
                    "Wait for context compaction to finish before steering the thread.".into(),
                ))
            }
            "/logout" => {
                return Err(error(
                    "Wait for sign-out to finish before steering the thread.".into(),
                ))
            }
            _ => {}
        }
    }
    if run.status != "running" {
        return Err(error(format!(
            "Target run {} is {} and cannot be steered.",
            run.wire_id(),
            run.status
        )));
    }
    let Some(ids) = thread.provider_run_ids(run) else {
        return Err(error(format!(
            "No running provider turn found for active run {}.",
            run.wire_id()
        )));
    };
    if !thread.provider.session_live() {
        return Err(error(format!(
            "Provider thread {} has no active provider session for steering.",
            ids.provider_thread
        )));
    }
    if !turn_running {
        return Err(error(format!(
            "No running provider turn found for active run {}.",
            run.wire_id()
        )));
    }
    // Rust steers Codex turns only; a Claude steer waits for the next turn.
    if ids.driver != crate::workspace::provider::CODEX {
        return Ok(SteerPlan::Queue);
    }
    // The message it follows: Rust's message times are whole seconds, too
    // coarse to order it among the provider's messages as the reference
    // does (by creation time).
    let after_message = thread.projection()["messages"]
        .as_array()
        .and_then(|messages| messages.last())
        .map(|message| message["id"].clone())
        .unwrap_or(Value::Null);
    let index = thread.runs.iter().position(|r| r.id == run.id).unwrap_or(0);
    let after = thread
        .provider_records()
        .items
        .iter()
        .filter(|(i, _)| *i == index)
        .count();
    Ok(SteerPlan::Steer(Target {
        run: run.id,
        wire_id: run.wire_id(),
        steer: json!({
            "runId": run.wire_id(),
            crate::thread_projection::LINK_CARD_POSITION: after,
            "nodeId": ids.root_node,
            "providerThreadId": ids.provider_thread,
            "providerTurnId": ids.provider_turn,
            "afterMessageId": after_message,
        }),
    }))
}

/// Record the steer message on its run and hand it to the run's provider
/// turn (Codex `turn/steer`).
pub(super) async fn steer<H: CommandHost>(
    state: Arc<H>,
    receipt: &CommandReceipt,
    thread_id: &str,
    command: &Value,
    target: Target,
) -> anyhow::Result<u64> {
    let message_id = command["messageId"].as_str().unwrap_or_default().to_owned();
    let text = command["text"].as_str().unwrap_or_default().to_owned();
    let message = ConversationMessage {
        id: message_id.clone(),
        role: "user".into(),
        content: text.clone(),
        created_at: crate::now(),
        creation_source: command["creationSource"]
            .as_str()
            .filter(|source| *source != "web")
            .map(str::to_owned),
        steer: Some(Box::new(target.steer)),
        ..Default::default()
    };
    let sequence = state
        .commit_receipted(
            Event::MessageAdded {
                thread_id: thread_id.to_owned(),
                message,
            },
            Some(receipt.clone()),
        )
        .await?
        .id;
    let delivered = state
        .workspace()
        .respond_turn(
            &target.run.to_string(),
            crate::workspace::provider::RuntimeResponse {
                id: message_id,
                steer: Some(text),
                ..Default::default()
            },
        )
        .await;
    if !delivered {
        tracing::warn!(run = %target.wire_id, "the steered run's provider turn was not running");
    }
    Ok(sequence)
}
