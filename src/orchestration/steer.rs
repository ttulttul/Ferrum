//! A `message.dispatch` that steers the thread's running run
//! (`dispatchMode: steer_active`), as the reference's orchestrator decides
//! and carries it out (`resolveMessageDispatchIntent`'s steer checks and
//! `dispatchSteerIntoRun`'s active steering).
use super::CommandHost;
use crate::{CommandReceipt, ConversationMessage, Event};
use serde_json::{json, Value};
use std::sync::Arc;

/// What a steer message does.
pub(crate) enum SteerPlan {
    /// Starts its own run: the target is gone or its turn has ended.
    Start,
    /// Waits behind the target, as a queued message: the target has no
    /// running provider turn yet, a fresh context was asked for, or its
    /// provider cannot be steered by this server.
    Queue,
    /// Joins the target's running provider turn.
    Steer(Target),
}

pub(crate) struct Target {
    run: uuid::Uuid,
    wire_id: String,
    steer: Value,
}

fn error(message: String) -> anyhow::Error {
    anyhow::anyhow!(message)
}

/// Decide what the steer message `command` does on `thread_id`.
pub(crate) async fn plan<H: CommandHost>(
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
    // Rust steers Codex, Claude and Grok turns; another driver's steer
    // waits for the next turn. ACP Registry agents and fx have no active
    // steering (the reference restarts their turn with the message instead).
    if !crate::workspace::provider::runs_turns(&ids.driver)
        || ids.driver == crate::workspace::provider::ACP_REGISTRY
        || ids.driver == crate::workspace::provider::FX
    {
        return Ok(SteerPlan::Queue);
    }
    // The message it follows: message times can tie (older logs hold whole
    // seconds), too coarse to order it among the provider's messages as the
    // reference does (by creation time).
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
/// turn (Codex `turn/steer`; for Claude, a priority `now` user message).
pub(super) async fn steer<H: CommandHost>(
    state: Arc<H>,
    receipt: &CommandReceipt,
    thread_id: &str,
    command: &Value,
    target: Target,
) -> anyhow::Result<u64> {
    steer_with(state, receipt, thread_id, command, target, None).await
}

/// What a steered delegated delivery records beyond a user's steer: its
/// text (the tasks' results), its notification and its own wire fields.
pub(super) struct Delivery {
    pub text: String,
    pub notification: Value,
    pub wire: Value,
}

/// [`steer`], for a delegated task's delivery when `delivery` is given: it
/// joins the running turn as its notification (`dispatchSteerIntoRun` with
/// `delegatedCompletion`).
pub(super) async fn steer_with<H: CommandHost>(
    state: Arc<H>,
    receipt: &CommandReceipt,
    thread_id: &str,
    command: &Value,
    target: Target,
    delivery: Option<Delivery>,
) -> anyhow::Result<u64> {
    let message_id = command["messageId"].as_str().unwrap_or_default().to_owned();
    let text = delivery.as_ref().map_or_else(
        || command["text"].as_str().unwrap_or_default().to_owned(),
        |d| d.text.clone(),
    );
    // What the provider reads: who sent the message, when a person did not
    // type it (`withMessageSenderNote`), ahead of its text.
    let mut sender = json!({});
    for key in [
        "createdBy",
        "creationSource",
        "scheduledTaskId",
        "senderThreadId",
        "linkedSender",
    ] {
        let value = delivery
            .as_ref()
            .and_then(|d| d.wire.get(key))
            .unwrap_or(&command[key]);
        if !value.is_null() {
            sender[key] = value.clone();
        }
    }
    let provider_text = crate::thread_projection::with_message_sender_note(&sender, &text);
    let message = ConversationMessage {
        id: message_id.clone(),
        role: if delivery.is_some() {
            crate::thread_projection::NOTIFICATION_ROLE.into()
        } else {
            "user".into()
        },
        content: text.clone(),
        created_at: crate::now(),
        creation_source: command["creationSource"]
            .as_str()
            .filter(|source| *source != "web")
            .map(str::to_owned),
        notification: delivery.as_ref().map(|d| d.notification.clone()),
        wire: delivery.map(|d| Box::new(d.wire)),
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
                steer: Some(provider_text),
                ..Default::default()
            },
        )
        .await;
    if !delivered {
        // The turn ended before the steer was sent: it is sent again as a
        // run of its own once this run ends, as the reference's steer fails
        // with `turnCompleted` and its EffectWorker dispatches it again.
        tracing::info!(run = %target.wire_id, "the steered turn had ended; sending its message again");
        let untaken = Event::RunProvider {
            run_id: target.run,
            thread_id: thread_id.to_owned(),
            event: crate::workspace::ProviderEvent::SteerUpdated {
                message_id: command["messageId"].as_str().unwrap_or_default().to_owned(),
                status: "completed".into(),
                untaken: true,
            },
            position: None,
        };
        if let Err(error) = state.commit(untaken).await {
            tracing::warn!(%error, "could not record an unsent steer");
        }
    }
    Ok(sequence)
}

/// Restart `command`'s target run in place, when its provider restarts a
/// running turn with new input (`supportsSteeringByInterruptRestart`): the
/// run starts over as its next attempt (`interrupt_restart`), its turn is
/// interrupted, and the message runs on the same run once that turn ends.
/// `None` when the run cannot be restarted that way: it then stops and the
/// message runs next, as for Claude.
pub(super) async fn restart_in_place<H: CommandHost>(
    state: &Arc<H>,
    receipt: &CommandReceipt,
    thread_id: &str,
    command: &Value,
) -> anyhow::Result<Option<u64>> {
    let target_id = command["dispatchMode"]["targetRunId"]
        .as_str()
        .unwrap_or_default();
    let (run, steer, cwd) = {
        let store = state.store.read().await;
        let Some(thread) = store.threads.get(thread_id) else {
            return Ok(None);
        };
        let Some(run) = thread
            .runs
            .iter()
            .find(|run| run.wire_id() == target_id && run.status == "running")
        else {
            return Ok(None);
        };
        let Some(ids) = thread.provider_run_ids(run) else {
            return Ok(None);
        };
        let capabilities: Value =
            serde_json::from_str(include_str!("../provider_capabilities.json")).unwrap_or_default();
        let turns = &capabilities[ids.driver.as_str()]["turns"];
        let turn_running = thread
            .provider
            .runs
            .get(&run.id)
            .is_some_and(|record| record.turn_started)
            && !thread.provider.terminals.contains_key(&run.id);
        let ending = thread
            .provider
            .superseded
            .get(&run.id)
            .and_then(|attempts| attempts.last())
            .is_some_and(|attempt| attempt.open);
        if turns["supportsInterrupt"] != true
            || turns["supportsSteeringByInterruptRestart"] != true
            || !crate::workspace::provider::runs_turns(&ids.driver)
            || !thread.provider.session_live()
            || !turn_running
            || ending
        {
            return Ok(None);
        }
        let run_id = run.wire_id();
        let index = thread.runs.iter().position(|r| r.id == run.id).unwrap_or(0);
        let after = thread
            .provider_records()
            .items
            .iter()
            .filter(|(i, _)| *i == index)
            .count();
        let after_message = thread.projection()["messages"]
            .as_array()
            .and_then(|messages| messages.last())
            .map(|message| message["id"].clone())
            .unwrap_or(Value::Null);
        let (_, next_root) =
            crate::provider_projection::attempt_ids(&run_id, run.steering_restarts + 2);
        let steer = json!({
            "runId": run_id,
            crate::thread_projection::LINK_CARD_POSITION: after,
            "nodeId": next_root,
            "providerThreadId": ids.provider_thread,
            "providerTurnId": null,
            "afterMessageId": after_message,
        });
        (run.clone(), steer, thread.cwd.clone())
    };
    let message_id = command["messageId"].as_str().unwrap_or_default().to_owned();
    let text = command["text"].as_str().unwrap_or_default().to_owned();
    let mut sender = json!({});
    for key in [
        "createdBy",
        "creationSource",
        "scheduledTaskId",
        "senderThreadId",
        "linkedSender",
    ] {
        if !command[key].is_null() {
            sender[key] = command[key].clone();
        }
    }
    let provider_text = crate::thread_projection::with_message_sender_note(&sender, &text);
    state.steering_restarts.lock().await.insert(
        run.id,
        crate::control_state::SteeringRestart {
            cwd,
            text: provider_text,
        },
    );
    // The new attempt first, then its message (`interrupt_restart`).
    let sequence = state
        .commit_receipted(
            Event::RunRestarted {
                run_id: run.id,
                thread_id: thread_id.to_owned(),
                message_id: message_id.clone(),
            },
            Some(receipt.clone()),
        )
        .await?
        .id;
    let message = ConversationMessage {
        id: message_id,
        role: "user".into(),
        content: text,
        created_at: crate::now(),
        creation_source: command["creationSource"]
            .as_str()
            .filter(|source| *source != "web")
            .map(str::to_owned),
        steer: Some(Box::new(steer)),
        ..Default::default()
    };
    state
        .commit(Event::MessageAdded {
            thread_id: thread_id.to_owned(),
            message,
        })
        .await?;
    // The turn is interrupted; its end starts the next attempt.
    if let Some(cancel) = state.cancellations().lock().await.remove(&run.id) {
        let _ = cancel.send(());
    }
    Ok(Some(sequence))
}
