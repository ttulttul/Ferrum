//! Running a provider turn: start it in the workspace, record each event in
//! the event log, and acknowledge each event only once it is recorded.
//!
//! The native server and the control plane share this loop. It reaches the
//! machine only through [`Workspace::start_tracked_turn`], and its host
//! starts it with [`crate::runtime::Runtime::spawn`], so it builds for every
//! target.
use crate::workspace::provider::ProviderEvent;
use crate::workspace::provider::TurnStatus;
use crate::{
    control_state::ControlState,
    workspace::{StreamPosition, TurnEvent, TurnExit, TurnItem, TurnItems, TurnSpec, Workspace},
    CheckpointRecord, Event, Run,
};
use serde_json::{json, Value};
use std::{collections::HashMap, sync::Arc, time::Duration};
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
    /// A wait on the host's runtime (streamed text is held this long).
    fn turn_wait(&self, duration: Duration) -> crate::runtime::Wait;
    /// Running runs by id; resolving a sender stops its run.
    fn cancellations(&self) -> &Mutex<HashMap<Uuid, oneshot::Sender<()>>>;
    /// `run`'s turn started on workspace stream `stream`, before any of its
    /// events are recorded. A host that resumes turns after a restart keeps
    /// it with the run until [`turn_finished`](Self::turn_finished).
    fn turn_started(&self, _run: &Run, _cwd: &str, _stream: u64) {}
    /// A turn event is recorded (or deliberately not recorded). With a
    /// `position`, the run's events up to it need not arrive again; this is
    /// called before the workspace is told, so a host that keeps positions
    /// with its runs never resumes from before a recorded event.
    fn turn_recorded(&self, _run: &Run, _position: Option<StreamPosition>) {}
    /// The run has ended and its end is recorded.
    fn turn_finished(&self, _run: &Run) {}
    /// The server is stopping: it has recorded how its turns were cut off,
    /// so what they report from now on is not recorded.
    fn stopping(&self) -> bool {
        false
    }
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
        Ok(turn) => {
            if let Some(stream) = turn.stream() {
                host.turn_started(&run, &cwd, stream);
            }
            record(&*host, &run, cwd, turn).await
        }
        Err(error) => Err(error),
    };
    if let Err(error) = result {
        fail(&*host, &run, error).await;
    }
}

/// Record a turn started before a restart, picked up again with
/// [`crate::workspace::RemoteWorkspace::resume_turn`].
pub(crate) async fn resume<H: TurnHost>(host: Arc<H>, run: Run, cwd: String, turn: TurnItems) {
    if let Err(error) = record(&*host, &run, cwd, turn).await {
        fail(&*host, &run, error).await;
    }
}

/// The runs a restart left running, with the position each last recorded.
pub(crate) fn running_runs(store: &crate::Store) -> Vec<(Run, Option<StreamPosition>)> {
    let mut runs: Vec<_> = store
        .threads
        .values()
        .flat_map(|thread| thread.runs.iter())
        .filter(|run| run.status == "running")
        .map(|run| (run.clone(), store.turn_positions.get(&run.id).copied()))
        .collect();
    runs.sort_by_key(|(run, _)| run.started_at);
    runs
}

/// The error of a run a restart cut off.
pub(crate) const RESTART_INTERRUPTION: &str = "interrupted: the server restarted during the turn";

/// End a run that a restart cut off and that cannot be picked up again.
pub(crate) fn interrupted(run: &Run) -> Event {
    Event::RunFinished {
        run_id: run.id,
        thread_id: run.thread_id.clone(),
        status: "failed".into(),
        error: Some(RESTART_INTERRUPTION.into()),
    }
}

pub(crate) async fn fail<H: TurnHost>(host: &H, run: &Run, error: anyhow::Error) {
    host.cancellations().lock().await.remove(&run.id);
    if host.stopping() {
        return;
    }
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

fn recorded(run: &Run, event: TurnEvent, position: Option<StreamPosition>) -> Recorded {
    let output = |stream: &str, text: String| {
        Recorded::Event(Box::new(Event::RunOutput {
            run_id: run.id,
            thread_id: run.thread_id.clone(),
            stream: stream.into(),
            text,
            position,
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
        TurnEvent::Provider(event) => Recorded::Event(Box::new(Event::RunProvider {
            run_id: run.id,
            thread_id: run.thread_id.clone(),
            event,
            position,
        })),
        TurnEvent::Exited(exit) => Recorded::End(exit),
    }
}

/// How long streamed text is held before it is recorded: the reference's
/// adapters' text delta flush interval.
const TEXT_FLUSH: Duration = Duration::from_millis(50);

/// Streamed text a turn holds before recording it, as the reference's
/// adapters do (`ProviderTextDeltaCoalescer`): the deltas that arrive within
/// [`TEXT_FLUSH`] of the first held one become one update per item.
///
/// Held text is recorded before any other event of the turn (an item's end,
/// the turn's end, or anything else), so the log keeps the stream's order
/// and every piece of text is recorded once, as a delta, before its item
/// ends. Held deltas are acknowledged only once they are recorded: the last
/// update of a flush carries the stream position of the newest delta in it,
/// so a restart resumes after the text recorded and before the text that
/// was still held, which the workspace then delivers again.
#[derive(Default)]
struct StreamedText {
    /// Text held per item, in the order the items first streamed.
    held: Vec<(String, String)>,
    /// The stream position of the newest held delta.
    position: Option<StreamPosition>,
    flush_at: Option<crate::runtime::Wait>,
}

impl StreamedText {
    /// Hold `event` if it is streamed text (`None`), or give it back.
    fn take<H: TurnHost>(
        &mut self,
        host: &H,
        event: TurnEvent,
        position: Option<StreamPosition>,
    ) -> Option<TurnEvent> {
        let TurnEvent::Provider(ProviderEvent::TurnItemDelta { item_id, delta }) = event else {
            return Some(event);
        };
        if !delta.is_empty() {
            match self.held.iter_mut().find(|(id, _)| *id == item_id) {
                Some((_, text)) => text.push_str(&delta),
                None => self.held.push((item_id, delta)),
            }
        }
        if position.is_some() {
            self.position = position;
        }
        if self.flush_at.is_none() {
            self.flush_at = Some(host.turn_wait(TEXT_FLUSH));
        }
        None
    }

    /// Record the held text, one update per item, and acknowledge the
    /// deltas it was made of.
    async fn flush<H: TurnHost>(&mut self, host: &H, run: &Run) {
        self.flush_at = None;
        let held = std::mem::take(&mut self.held);
        let position = self.position.take();
        let count = held.len();
        for (index, (item_id, delta)) in held.into_iter().enumerate() {
            if host.stopping() {
                return;
            }
            let last = index + 1 == count;
            let event = Event::RunProvider {
                run_id: run.id,
                thread_id: run.thread_id.clone(),
                event: ProviderEvent::TurnItemDelta { item_id, delta },
                position: if last { position } else { None },
            };
            if let Err(error) = host.commit(event).await {
                // Not acknowledged: the workspace offers it again after a
                // reconnect.
                tracing::error!(%error, "could not record streamed text");
                return;
            }
        }
        if count > 0 || position.is_some() {
            host.turn_recorded(run, position);
            if let Some(position) = position {
                host.workspace().ack(position);
            }
        }
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
        host.turn_recorded(run, position);
        if let Some(position) = position {
            host.workspace().ack(position);
        }
    };
    let mut exit = None;
    let mut text = StreamedText::default();
    loop {
        let next = match text.flush_at.as_mut() {
            None => turn.recv().await,
            Some(flush) => {
                let receive = std::pin::pin!(turn.recv());
                match futures_util::future::select(receive, flush).await {
                    futures_util::future::Either::Left((next, _)) => next,
                    futures_util::future::Either::Right(_) => {
                        text.flush(host, run).await;
                        continue;
                    }
                }
            }
        };
        let Some(TurnItem { event, position }) = next else {
            // Events that stop without the turn's end still report what is held.
            text.flush(host, run).await;
            break;
        };
        if host.stopping() {
            return Ok(());
        }
        if matches!(&event, TurnEvent::Provider(ProviderEvent::TurnItemDelta { delta, .. })
            if delta.is_empty() && text.held.is_empty())
        {
            // No text to record.
            done(position);
            continue;
        }
        let Some(event) = text.take(host, event, position) else {
            continue;
        };
        // Held text is recorded before any later event: an item's end
        // carries its whole text, but its deltas come first, in order.
        text.flush(host, run).await;
        match recorded(run, event, position) {
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
    if host.stopping() {
        return Ok(());
    }
    let thread_id = run.thread_id.clone();
    // A provider turn ends as its `turn.terminal` says, which may have been
    // recorded before a restart; a command turn as its process exited.
    let terminal = host
        .store
        .read()
        .await
        .threads
        .get(&thread_id)
        .and_then(|thread| thread.provider.terminals.get(&run.id).cloned());
    let (run_status, error) = match terminal {
        Some(terminal) => match terminal.status {
            TurnStatus::Completed => ("completed", None),
            TurnStatus::Interrupted => ("interrupted", None),
            TurnStatus::Failed | TurnStatus::Running => (
                "failed",
                Some(terminal.failure.map_or_else(
                    || "The provider turn failed.".to_owned(),
                    |failure| failure.message,
                )),
            ),
        },
        None if cancelled => ("cancelled", None),
        None if ok => ("completed", None),
        None => ("failed", Some(format!("provider exited with {status}"))),
    };
    // A completed turn's checkpoint comes first: the reference's run waits
    // for it, and its capture completes the run.
    if run_status == "completed" {
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
                    thread_id: thread_id.clone(),
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
        if host.stopping() {
            return Ok(());
        }
    }
    host.commit(Event::RunFinished {
        run_id: run.id,
        thread_id,
        status: run_status.into(),
        error,
    })
    .await?;
    host.turn_finished(run);
    done(exit_position);
    Ok(())
}
