//! Continuation runs for what a provider's CLI does on its own while no
//! turn of its thread runs, as the reference's `ProviderContinuationService`
//! opens them: a wake the workspace reports ([`ProviderWake`]) becomes an
//! agent message from the provider ("Background task completed.", shown
//! as a background-activity notification) queued after any active run. Its
//! run shows what the CLI buffered rather than sending the text
//! ([`crate::workspace::ProviderTurn::continuation`]).
//!
//! One thread's continuations are opened one at a time, in order, and
//! threads do not wait on each other (the reference's keyed runner). A
//! continuation that ends without starting (a queued run cancelled) retires
//! its wake, so the CLI drops what it would have shown and reports the next
//! one (`onRunEndedWithoutStarting`).
use crate::orchestration::{delegation::join_id, CommandHost};
use crate::workspace::provider::ProviderWake;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};
use tokio::sync::mpsc;

/// `CONTINUATION_MESSAGE_TEXT`: a continuation's message when its wake
/// names no detail.
pub(crate) const CONTINUATION_MESSAGE_TEXT: &str = "Background task completed.";

/// The notification a continuation shows when its wake reports none.
fn default_notification() -> Value {
    json!({
        "source": {"kind": "background_task"},
        "outcome": "updated",
        "summary": "Background activity updated",
    })
}

/// Continuations dispatched and not yet ended, by message id: their wake.
fn open() -> &'static Mutex<HashMap<String, ProviderWake>> {
    static OPEN: OnceLock<Mutex<HashMap<String, ProviderWake>>> = OnceLock::new();
    OPEN.get_or_init(Default::default)
}

/// Each thread's lock, so its continuations open one at a time.
fn thread_lock(thread_id: &str) -> Arc<tokio::sync::Mutex<()>> {
    static LOCKS: OnceLock<Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>> = OnceLock::new();
    LOCKS
        .get_or_init(Default::default)
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .entry(thread_id.to_owned())
        .or_default()
        .clone()
}

/// Open a continuation for each wake the workspace reports, until it
/// reports no more.
pub(crate) async fn run<H: CommandHost + 'static>(
    state: Arc<H>,
    mut wakes: mpsc::UnboundedReceiver<ProviderWake>,
) {
    while let Some(wake) = wakes.recv().await {
        let state = state.clone();
        let lock = thread_lock(&wake.app_thread_id);
        tokio::spawn(async move {
            let _held = lock.lock().await;
            dispatch(state, wake).await;
        });
    }
}

/// `dispatchContinuation`: queue the wake's continuation run on its thread,
/// unless the thread is gone or archived, or its provider instance is
/// disabled, which retire the wake instead.
async fn dispatch<H: CommandHost>(state: Arc<H>, wake: ProviderWake) {
    // The reference numbers the message among the messages it read for
    // the request: none, so always the first.
    let ordinal = 1;
    {
        let store = state.store.read().await;
        let thread = store
            .threads
            .get(&wake.app_thread_id)
            .filter(|_| !store.deleted_thread_ids.contains(&wake.app_thread_id));
        match thread {
            Some(thread) if !thread.archived => {}
            _ => {
                tracing::info!(thread_id = %wake.app_thread_id, "a provider wake's thread is gone or archived");
                state.workspace().retire_provider_wake(&wake);
                return;
            }
        }
    }
    let message_id = join_id(
        "message",
        &[
            "thread",
            &wake.app_thread_id,
            "ordinal",
            &ordinal.to_string(),
            &uuid::Uuid::new_v4().to_string(),
        ],
    );
    let command = json!({
        "type": "message.dispatch",
        "commandId": format!("provider-continuation:{message_id}"),
        "threadId": wake.app_thread_id,
        "messageId": message_id,
        "text": wake.detail.as_deref().unwrap_or(CONTINUATION_MESSAGE_TEXT),
        "notification": wake.notification.clone().unwrap_or_else(default_notification),
        "attachments": [],
        "dispatchMode": {"type": "queue_after_active"},
        "createdBy": "agent",
        "creationSource": "provider",
    });
    lock_open().insert(message_id.clone(), wake.clone());
    if let Err(error) =
        crate::orchestration::dispatch_orchestration_command(state.clone(), command).await
    {
        tracing::info!(?error, thread_id = %wake.app_thread_id, "could not open a provider continuation");
        lock_open().remove(&message_id);
        state.workspace().retire_provider_wake(&wake);
    }
}

fn lock_open() -> std::sync::MutexGuard<'static, HashMap<String, ProviderWake>> {
    open().lock().unwrap_or_else(|e| e.into_inner())
}

/// A run of `thread_id` ended: a continuation whose run ended retires its
/// wake. One that started took the wake already, so retiring it is a no-op
/// (the token names the wake it was opened for).
pub(crate) async fn after_run<H: CommandHost>(state: &H, thread_id: &str) {
    let ended: Vec<String> = {
        let store = state.store.read().await;
        let Some(thread) = store.threads.get(thread_id) else {
            return;
        };
        let open = lock_open();
        thread
            .runs
            .iter()
            .filter(|run| run.finished_at.is_some() || run.status == "cancelled")
            .filter_map(|run| run.user_message_id.clone())
            .filter(|id| open.contains_key(id))
            .collect()
    };
    for message_id in ended {
        if let Some(wake) = lock_open().remove(&message_id) {
            state.workspace().retire_provider_wake(&wake);
        }
    }
}
