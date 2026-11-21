//! Running a provider turn: start it in the workspace, record each event in
//! the event log, and acknowledge each event only once it is recorded.
//!
//! The native server and the control plane share this loop. It reaches the
//! machine only through [`Workspace::start_tracked_turn`], and its host
//! starts it with [`crate::runtime::Runtime::spawn`], so it builds for every
//! target.
use crate::{
    control_state::ControlState,
    workspace::{StreamPosition, TurnEvent, TurnExit, TurnItem, TurnItems, TurnSpec, Workspace},
    CheckpointRecord, Event, Run,
};
use serde_json::{json, Value};
use std::{collections::HashMap, sync::Arc};
use tokio::sync::{oneshot, Mutex};
use uuid::Uuid;

/// The provider command when none is configured: echo the prompt.
pub fn echo_command() -> Vec<String> {
    vec!["printf".into(), "%s\\n".into()]
}

/// What a turn needs from the server that runs it.
pub(crate) trait TurnHost:
    std::ops::Deref<Target = ControlState> + crate::workspace::MaybeSendSync
{
    fn workspace(&self) -> &dyn Workspace;
    /// Running runs by id; resolving a sender stops its run.
    fn cancellations(&self) -> &Mutex<HashMap<Uuid, oneshot::Sender<()>>>;
    /// A turn event at `position` is recorded (or deliberately not
    /// recorded); the run's events up to it need not arrive again. Called
    /// before the workspace is told, so a host that keeps positions with
    /// its runs never resumes from before a recorded event.
    fn turn_recorded(&self, _run: &Run, _position: StreamPosition) {}
    /// The run has ended and its end is recorded.
    fn turn_finished(&self, _run: &Run) {}
}

/// Start `spec` for `run` and record it until it ends, recording a failure
/// to start or to finish as the run's end.
pub(crate) async fn launch<H: TurnHost>(
    host: Arc<H>,
    run: Run,
    spec: TurnSpec,
    cancel: oneshot::Receiver<()>,
) {
    let cwd = spec.cwd.clone();
    let result = match host.workspace().start_tracked_turn(spec, cancel).await {
        Ok(turn) => record(&*host, &run, cwd, turn).await,
        Err(error) => Err(error),
    };
    if let Err(error) = result {
        fail(&*host, &run, error).await;
    }
}

async fn fail<H: TurnHost>(host: &H, run: &Run, error: anyhow::Error) {
    host.cancellations().lock().await.remove(&run.id);
    match host
        .commit(Event::RunFinished {
            run_id: run.id,
            thread_id: run.thread_id.clone(),
            status: "failed".into(),
            error: Some(error.to_string()),
        })
        .await
    {
        Ok(_) => host.turn_finished(run),
        Err(write) => tracing::error!(%write, "could not record provider failure"),
    }
}

/// What one turn event becomes.
enum Recorded {
    /// An event for the log.
    Event(Box<Event>),
    /// A notice for live clients that the log does not keep.
    Notice(Value),
    End(TurnExit),
}

fn recorded(run: &Run, event: TurnEvent) -> Recorded {
    let output = |stream: &str, text: String| {
        Recorded::Event(Box::new(Event::RunOutput {
            run_id: run.id,
            thread_id: run.thread_id.clone(),
            stream: stream.into(),
            text,
        }))
    };
    match event {
        TurnEvent::Stdout(text) => {
            if let Ok(event) = serde_json::from_str::<Value>(text.trim()) {
                if event["type"] == "prompt_suggestion.updated"
                    && event["threadId"].is_string()
                    && event["runId"].is_string()
                    && event["text"].is_string()
                {
                    return Recorded::Notice(json!({
                        "threadId": event["threadId"].clone(),
                        "runId": event["runId"].clone(),
                        "text": event["text"].clone(),
                    }));
                }
            }
            output("stdout", text)
        }
        TurnEvent::Stderr(text) => output("stderr", text),
        TurnEvent::Exited(exit) => Recorded::End(exit),
    }
}

/// Record `turn`'s events as `run`'s, acknowledging each once it is in the
/// event log, so a workspace that keeps unacknowledged events redelivers
/// the rest after a reconnect or a restart.
async fn record<H: TurnHost>(
    host: &H,
    run: &Run,
    cwd: String,
    mut turn: TurnItems,
) -> anyhow::Result<()> {
    let done = |position: Option<StreamPosition>| {
        if let Some(position) = position {
            host.turn_recorded(run, position);
            host.workspace().ack(position);
        }
    };
    let mut exit = None;
    while let Some(TurnItem { event, position }) = turn.recv().await {
        match recorded(run, event) {
            Recorded::Event(event) => {
                // An event that could not be recorded is not acknowledged,
                // so the workspace offers it again after a reconnect.
                if host.commit(*event).await.is_ok() {
                    done(position);
                }
            }
            Recorded::Notice(notice) => {
                let _ = host.prompt_suggestion_events.send(notice);
                done(position);
            }
            Recorded::End(status) => {
                host.cancellations().lock().await.remove(&run.id);
                exit = Some((status, position));
                break;
            }
        }
    }
    let Some((
        TurnExit {
            success: ok,
            cancelled,
            status,
        },
        exit_position,
    )) = exit
    else {
        anyhow::bail!("the provider process could not be waited for");
    };
    let thread_id = run.thread_id.clone();
    host.commit(Event::RunFinished {
        run_id: run.id,
        thread_id: thread_id.clone(),
        status: if cancelled {
            "cancelled"
        } else if ok {
            "completed"
        } else {
            "failed"
        }
        .into(),
        error: (!ok && !cancelled).then(|| format!("provider exited with {status}")),
    })
    .await?;
    host.turn_finished(run);
    done(exit_position);
    if ok && !cancelled {
        if let Some(tree) = host.workspace().capture_git_tree(&cwd).await {
            let turn_count = host
                .store
                .read()
                .await
                .threads
                .get(&thread_id)
                .map(|thread| thread.messages.len() as u64)
                .unwrap_or_default();
            let _ = host
                .commit(Event::CheckpointCaptured {
                    thread_id,
                    checkpoint: CheckpointRecord {
                        turn_count,
                        tree,
                        cwd,
                        captured_at: crate::now(),
                        ready: true,
                    },
                })
                .await;
        }
    }
    Ok(())
}
