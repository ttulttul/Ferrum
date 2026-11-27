//! Running a provider turn: start it in the workspace, record each event in
//! the event log, and acknowledge each event only once it is recorded.
//!
//! The native server and the control plane share this loop. It reaches the
//! machine only through [`Workspace::start_tracked_turn`], and its host
//! starts it with [`crate::runtime::Runtime::spawn`], so it builds for every
//! target.
mod assistant_streaming;
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
    fn turn_settings(&self) -> anyhow::Result<Value>;
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
    /// A completed provider run is visible to a waiting parent before its checkpoint is captured.
    fn provider_run_completed(self: Arc<Self>, _run: &Run) -> Option<crate::runtime::Wait> {
        None
    }
    /// A stopped run's turn has ended, before its checkpoint: what its stop
    /// left for then (its delegated tasks' stop), if anything.
    fn turn_stopped(
        self: Arc<Self>,
        _run: &Run,
    ) -> Option<crate::orchestration::delegation::StopFuture<'static>> {
        None
    }
    /// The server is stopping: it has recorded how its turns were cut off,
    /// so what they report from now on is not recorded.
    fn stopping(&self) -> bool {
        false
    }
    /// A provider session `run`'s turn opened reported what it started with
    /// (Antigravity's `onSessionStarted`): a host that keeps provider status
    /// takes its models from it.
    fn provider_session_started(self: Arc<Self>, _run: &Run, _started: Value) {}
    /// A steering restart's run: its interrupted turn has ended, so its next
    /// attempt starts, in `cwd` with `text`. A host that runs turns starts
    /// it as it starts a run's turn.
    fn restart_turn(self: Arc<Self>, _run: Run, _cwd: String, _text: String) {}
    /// Attach local file paths to `attachments` for a provider turn, when this
    /// host stores uploads locally.
    fn resolve_attachments(&self, attachments: Vec<Value>) -> Vec<Value> {
        attachments
    }
}

/// Start `spec` for `run` and record it until it ends, recording a failure
/// to start or to finish as the run's end.
pub(crate) async fn launch<H: TurnHost>(
    host: Arc<H>,
    run: Run,
    mut spec: TurnSpec,
    mut cancel: oneshot::Receiver<()>,
) {
    // A run stopped before its provider started abandons that start, as the
    // reference's interrupt does: no CLI is launched, and the run ends
    // interrupted.
    if cancel.try_recv().is_ok() {
        host.cancellations().lock().await.remove(&run.id);
        if !host.stopping() {
            let _ = host
                .commit(Event::RunFinished {
                    run_id: run.id,
                    thread_id: run.thread_id.clone(),
                    status: "interrupted".into(),
                    error: None,
                })
                .await;
        }
        return;
    }
    // Prepare and record any retry/pending context handoffs before starting
    // the provider turn, prepending inline context to the prompt and carrying
    // missed attachments forward (`ProviderTurnStartService.prepareRetryHandoff`).
    // A turn that resumes a native session also prepares its resume fallback
    // (`prepareRecoveryHandoff`): the thread's history for a fresh session,
    // should that one not resume.
    // A fork merged back hands this run what the fork did: recorded first, so
    // its handoff is delivered with the prompt like any pending one.
    if let Some(provider) = spec.provider.as_ref() {
        let merge = {
            let store = host.store.read().await;
            store.threads.get(&run.thread_id).and_then(|thread| {
                let transfer = thread.context_transfers.iter().find(|transfer| {
                    transfer["type"] == "merge_back" && transfer["status"] == "pending"
                })?;
                let source = store.threads.get(transfer["sourceThreadId"].as_str()?)?;
                crate::context_handoff::prepare_merge_back(
                    thread,
                    source,
                    &run,
                    provider,
                    crate::now(),
                )
            })
        };
        if let Some(merge) = merge {
            commit_recovery(&*host, &run, &merge).await;
        }
        // So does a run on another provider instance: the thread's history
        // since that instance last saw it.
        let switch = {
            let store = host.store.read().await;
            store.threads.get(&run.thread_id).and_then(|thread| {
                crate::context_handoff::prepare_provider_switch(
                    thread,
                    &run,
                    provider,
                    crate::now(),
                )
            })
        };
        if let Some(switch) = switch {
            commit_recovery(&*host, &run, &switch).await;
        }
    }
    let (mut prepared_handoff, mut recovery) = match spec.provider.as_ref() {
        Some(provider) => {
            let store = host.store.read().await;
            match store.threads.get(&run.thread_id) {
                Some(thread) => {
                    let now = crate::now();
                    let prepared = crate::context_handoff::prepare_for_turn(
                        thread,
                        &run,
                        &spec.prompt,
                        provider,
                        now,
                    );
                    let resumes = provider.resume.as_deref().is_some_and(|id| !id.is_empty());
                    let recovery = resumes
                        .then(|| {
                            crate::context_handoff::prepare_recovery(
                                thread,
                                &run,
                                &spec.prompt,
                                provider,
                                now,
                            )
                        })
                        .flatten();
                    (prepared, recovery)
                }
                None => (None, None),
            }
        }
        None => (None, None),
    };
    // Handoffs the resumed session has no room for go to a fresh one with
    // the thread's history (`handoff_budget_exhausted`).
    if prepared_handoff.as_ref().is_some_and(|p| p.over_budget) {
        if let Some(fallback) = recovery.take() {
            if let Some(provider) = spec.provider.as_mut() {
                provider.resume = None;
            }
            commit_recovery(&*host, &run, &fallback).await;
            prepared_handoff = Some(fallback.delivery);
        }
    }
    // A fork's first turn forks its source natively when the provider can,
    // else starts fresh with the source's history (see `prepare_fork`).
    if recovery.is_none() {
        if let Some(provider) = spec.provider.as_mut() {
            let store = host.store.read().await;
            let fork = store.threads.get(&run.thread_id).and_then(|thread| {
                let transfer = thread.context_transfers.iter().find(|transfer| {
                    transfer["type"] == "fork" && transfer["status"] == "pending"
                })?;
                let source = store.threads.get(transfer["sourceThreadId"].as_str()?)?;
                crate::context_handoff::prepare_fork(
                    thread,
                    source,
                    &run,
                    &spec.prompt,
                    provider,
                    crate::now(),
                )
            });
            drop(store);
            if let Some((mut fallback, source_session)) = fork {
                provider.fork_session = source_session;
                // A side chat's first turn also carries the parent's activity
                // since the side chat started.
                let side_chat = {
                    let store = host.store.read().await;
                    store.threads.get(&run.thread_id).and_then(|thread| {
                        let source = store
                            .threads
                            .get(fallback.transfer["sourceThreadId"].as_str()?)?;
                        crate::context_handoff::prepare_side_chat(
                            thread,
                            source,
                            &run,
                            provider,
                            crate::now(),
                        )
                    })
                };
                if let Some(side_chat) = side_chat {
                    commit_handoffs(&*host, &run, [side_chat.clone()]).await;
                    fallback.delivery.side_chat = Some(side_chat.clone());
                    prepared_handoff
                        .get_or_insert_with(|| crate::context_handoff::PreparedHandoff::empty(None))
                        .side_chat = Some(side_chat);
                }
                recovery = Some(fallback);
            }
        }
    }
    if let (Some(fallback), Some(provider)) = (recovery.as_ref(), spec.provider.as_mut()) {
        provider.recovery_prompt = Some(fallback.prompt(&spec.prompt));
    }
    // Codex takes the history as native items before the prompt instead
    // (`thread/inject_items`), when it can.
    let user_text = spec.prompt.clone();
    if let Some(provider) = spec
        .provider
        .as_mut()
        .filter(|provider| provider.driver == crate::workspace::provider::CODEX)
    {
        if let Some(prepared) = prepared_handoff
            .as_ref()
            .filter(|prepared| !prepared.inject_items.is_null())
        {
            provider.inject_items = Some(prepared.inject_items.clone());
            provider.injected_prompt = Some(prepared.prompt_without_history(&user_text));
        }
        if let Some(fallback) = recovery
            .as_ref()
            .filter(|fallback| !fallback.delivery.inject_items.is_null())
        {
            provider.recovery_inject_items = Some(fallback.delivery.inject_items.clone());
            provider
                .injected_prompt
                .get_or_insert_with(|| fallback.delivery.prompt_without_history(&user_text));
        }
    }
    if let Some(prepared) = prepared_handoff.as_mut() {
        // A Codex turn records its handoffs once its app-server has loaded
        // the native thread (see `record`): the reference prepares them only
        // after the provider thread loads, so a turn whose CLI cannot start
        // leaves none.
        let loads_first = spec
            .provider
            .as_ref()
            .is_some_and(|provider| provider.driver == crate::workspace::provider::CODEX);
        if !loads_first {
            commit_handoffs(&*host, &run, prepared.pending.iter().cloned()).await;
            prepared.recorded = true;
        }
        if !prepared.inline_context.is_empty() || prepared.side_chat.is_some() {
            spec.prompt = prepared.prompt(&spec.prompt);
        }
        if !prepared.missed_attachments.is_empty() {
            if let Some(provider) = spec.provider.as_mut() {
                let mut combined = host.resolve_attachments(prepared.missed_attachments.clone());
                combined.append(&mut provider.attachments);
                provider.attachments = combined;
            }
        }
    }
    let cwd = spec.cwd.clone();
    let result = match host.workspace().start_tracked_turn(spec, cancel).await {
        Ok(turn) => {
            if let Some(stream) = turn.stream() {
                host.turn_started(&run, &cwd, stream);
            }
            record(&host, &run, cwd, turn, prepared_handoff.take(), recovery).await
        }
        Err(error) => {
            if let Some(prepared) = prepared_handoff.take() {
                if prepared.recorded && !host.stopping() {
                    commit_handoffs(&*host, &run, prepared.into_unsent(crate::now())).await;
                }
            }
            Err(error)
        }
    };
    if let Err(error) = result {
        fail(&*host, &run, error).await;
    }
}

/// Record a turn started before a restart, picked up again with
/// [`crate::workspace::RemoteWorkspace::resume_turn`].
pub(crate) async fn resume<H: TurnHost>(host: Arc<H>, run: Run, cwd: String, turn: TurnItems) {
    if let Err(error) = record(&host, &run, cwd, turn, None, None).await {
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

fn recorded(
    run: &Run,
    event: TurnEvent,
    position: Option<StreamPosition>,
    visible_text: Option<String>,
) -> Recorded {
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
            visible_text,
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
///
/// Text recorded before another event, rather than because its window
/// ended, is recorded as `held` and shows only when the window ends, or not
/// at all if its item ends first: live subscribers see streamed text when
/// the reference's adapters send it, whatever else the turn reports
/// meanwhile.
#[derive(Default)]
struct StreamedText {
    /// Text held per item, in the order the items first streamed.
    held: Vec<(String, String)>,
    /// The stream position of the newest held delta.
    position: Option<StreamPosition>,
    /// Items whose text was recorded while held, to show when the window
    /// ends unless their end comes first.
    hidden: Vec<String>,
    flush_at: Option<crate::runtime::Wait>,
}

/// Why held text is recorded.
enum Flush<'a> {
    /// Its window ended, or the turn did: the text shows.
    Window,
    /// Another event of the turn is recorded next, here the end of the
    /// item named, if any: the text is recorded first, in the stream's
    /// order, but shows only when its window ends (or not at all, if its
    /// item's end comes first), as the reference's adapters send it.
    Before(Option<&'a str>),
}

impl StreamedText {
    /// Hold `event` if it is streamed text (`None`), or give it back.
    fn take<H: TurnHost>(
        &mut self,
        host: &H,
        event: TurnEvent,
        position: Option<StreamPosition>,
    ) -> Option<TurnEvent> {
        let TurnEvent::Provider(ProviderEvent::TurnItemDelta { item_id, delta, .. }) = event else {
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
    /// deltas it was made of: shown when its window ends, else recorded as
    /// `held` (see [`Flush`]).
    async fn flush<H: TurnHost>(&mut self, host: &H, run: &Run, why: Flush<'_>) {
        let mut held = std::mem::take(&mut self.held);
        let shown = matches!(why, Flush::Window);
        match why {
            Flush::Window => {
                self.flush_at = None;
                // Text recorded while held shows now, with what came since.
                for item in std::mem::take(&mut self.hidden) {
                    if !held.iter().any(|(id, _)| *id == item) {
                        held.push((item, String::new()));
                    }
                }
            }
            Flush::Before(ending) => {
                for (item, _) in &held {
                    if !self.hidden.contains(item) {
                        self.hidden.push(item.clone());
                    }
                }
                self.hidden.retain(|item| Some(item.as_str()) != ending);
                if self.hidden.is_empty() {
                    self.flush_at = None;
                }
            }
        }
        let position = self.position.take();
        let count = held.len();
        for (index, (item_id, delta)) in held.into_iter().enumerate() {
            if host.stopping() {
                return;
            }
            let last = index + 1 == count;
            let event = Event::RunProvider {
                visible_text: None,
                run_id: run.id,
                thread_id: run.thread_id.clone(),
                event: ProviderEvent::TurnItemDelta {
                    item_id,
                    delta,
                    held: !shown,
                },
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

/// Record each of `handoffs` (`context-handoff.updated`).
async fn commit_handoffs<H: TurnHost>(
    host: &H,
    run: &Run,
    handoffs: impl IntoIterator<Item = Value>,
) {
    for handoff in handoffs {
        let _ = host
            .commit(Event::ContextHandoffUpdated {
                thread_id: run.thread_id.clone(),
                handoff,
                transfer: None,
                turn_item: None,
            })
            .await;
    }
}

/// How long the reference's provider commit batch is busy committing a
/// batch: snapshots that arrive meanwhile wait and share the next one.
const SNAPSHOT_WINDOW: Duration = Duration::from_millis(20);
/// How long an ACP agent's frames read together take to reach the
/// reference's commit worker (see [`Snapshots::reads_first`]).
const ACP_READ_WINDOW: Duration = Duration::from_millis(20);
/// The most snapshots one batch takes (`MAX_EVENTS`).
const SNAPSHOT_BATCH: usize = 64;

/// A turn's item and plan snapshots as the reference's `ProviderCommitBatch`
/// commits a provider stream's: an idle worker takes a snapshot at once and
/// is busy committing it for [`SNAPSHOT_WINDOW`]; snapshots that arrive
/// meanwhile queue and commit together when it is done, and within such a
/// batch a snapshot a later one of the same entity replaces is not sent live
/// ([`crate::control_state::coalesce`]). Anything else the turn reports
/// waits for the queue, after which the worker is idle again.
///
/// What Rust records but sends nothing live (a model or reasoning note, a
/// prompt's delivery) has no counterpart in the reference's stream, so it
/// takes no part: it is recorded at once and leaves the worker as it was.
/// After the turn's terminal nothing batches.
#[derive(Default)]
struct Snapshots {
    queued: Vec<(Event, Option<StreamPosition>)>,
    /// When the batch being committed is done: the worker is busy till then.
    flush_at: Option<crate::runtime::Wait>,
    /// The turn's terminal was seen: snapshots commit one at a time.
    terminal_seen: bool,
    /// An ACP agent's turn: the reference's worker takes a snapshot only
    /// once the ACP stream has handed over what it read with it, so an idle
    /// worker waits [`ACP_READ_WINDOW`] before committing, and snapshots
    /// read together share a batch.
    reads_first: bool,
    /// The turn's provider thread was reported: an ACP agent's later
    /// reports of it (its session's updates) are snapshots of the thread,
    /// as the reference's adapter writes them into the commit batch.
    thread_seen: bool,
}

/// Jobs represented by a single entity in the workspace stream. A tool
/// reports its node and turn item independently to the reference batch, so
/// use its projected snapshot keys instead.
fn grouped_snapshot_entity(event: &Event) -> Option<String> {
    match event {
        Event::RunProvider {
            event: ProviderEvent::TurnItem { item },
            ..
        } => (!item.is_tool_call()).then(|| format!("item:{}", item.id)),
        Event::RunProvider {
            event: ProviderEvent::Plan { .. },
            ..
        } => Some("plan".into()),
        Event::RunProvider {
            event: ProviderEvent::ProviderThread { .. },
            ..
        } => Some("provider-thread".into()),
        _ => None,
    }
}

/// For each snapshot, the projected records a later snapshot replaces.
/// Tool nodes and items enter the batch independently.
fn superseded_snapshots(
    events: &[Event],
    projected: Vec<(Vec<String>, usize)>,
) -> Vec<Vec<String>> {
    let (jobs, grouped): (Vec<_>, Vec<_>) = events
        .iter()
        .zip(projected)
        .map(|(event, (keys, _))| match grouped_snapshot_entity(event) {
            Some(entity) => (vec![entity], Some(keys)),
            None => (keys, None),
        })
        .unzip();
    let mut dropped = crate::control_state::coalesce(&jobs);
    let mut replacements = std::collections::HashMap::new();
    for (index, group) in grouped.iter().enumerate().rev() {
        let Some(keys) = group else { continue };
        let later = replacements
            .entry(&jobs[index][0])
            .or_insert_with(std::collections::HashSet::new);
        if dropped[index].is_empty() {
            later.clear();
        } else {
            // Keep a projected change unless a later member of this
            // replacement chain will publish it. A completed text part can
            // refresh its message and item without changing its node.
            dropped[index] = keys
                .iter()
                .filter(|key| later.contains(key.as_str()))
                .cloned()
                .collect();
        }
        later.extend(keys.iter().map(String::as_str));
    }
    dropped
}

/// Whether a commit sent any live events.
fn sent_live(record: &crate::ServerEvent) -> bool {
    record
        .thread_events
        .as_ref()
        .and_then(|events| events.items.as_ref())
        .is_some_and(|items| !items.is_empty())
}

impl Snapshots {
    fn is_snapshot(&self, event: &TurnEvent) -> bool {
        !self.terminal_seen
            && (matches!(
                event,
                TurnEvent::Provider(ProviderEvent::TurnItem { .. } | ProviderEvent::Plan { .. })
            ) || (self.reads_first
                && self.thread_seen
                && matches!(
                    event,
                    TurnEvent::Provider(ProviderEvent::ProviderThread { .. })
                )))
    }

    /// A snapshot arrived: an idle worker commits it at once and, if it sent
    /// anything live, is busy for [`SNAPSHOT_WINDOW`]; a busy one queues it.
    async fn push<H: TurnHost>(
        &mut self,
        host: &H,
        run: &Run,
        event: Event,
        position: Option<StreamPosition>,
    ) {
        self.queued.push((event, position));
        if self.flush_at.is_none() && self.reads_first {
            self.flush_at = Some(host.turn_wait(ACP_READ_WINDOW));
        } else if self.flush_at.is_none() {
            if self.commit(host, run).await {
                self.flush_at = Some(host.turn_wait(SNAPSHOT_WINDOW));
            }
        } else if self.queued.len() >= SNAPSHOT_BATCH {
            self.commit(host, run).await;
        }
    }

    /// The worker finished its batch: it takes what queued meanwhile, and is
    /// busy again, or is idle.
    async fn tick<H: TurnHost>(&mut self, host: &H, run: &Run) {
        self.flush_at = None;
        if self.commit(host, run).await {
            self.flush_at = Some(host.turn_wait(SNAPSHOT_WINDOW));
        }
    }

    /// Whether `event`, not a snapshot, would send nothing live, so it takes
    /// no part in the batch. Only asked while the worker is busy.
    async fn invisible<H: TurnHost>(host: &H, event: &Event) -> bool {
        host.dry_run(std::slice::from_ref(event))
            .await
            .first()
            .is_some_and(|(_, sent)| *sent == 0)
    }

    /// Another event of the turn: it waits for the queued snapshots, and the
    /// worker is idle after them.
    async fn flush<H: TurnHost>(&mut self, host: &H, run: &Run) {
        self.flush_at = None;
        self.commit(host, run).await;
    }

    /// Commit the queued snapshots in order, each without the live events a
    /// later one replaces, acknowledging each once it is recorded. Whether
    /// any of them sent something live.
    async fn commit<H: TurnHost>(&mut self, host: &H, run: &Run) -> bool {
        let queued = std::mem::take(&mut self.queued);
        if queued.is_empty() {
            return false;
        }
        let events: Vec<Event> = queued.iter().map(|(event, _)| event.clone()).collect();
        let suppress = if events.len() > 1 {
            superseded_snapshots(&events, host.dry_run(&events).await)
        } else {
            vec![Vec::new()]
        };
        let mut sent = false;
        for ((event, position), suppress) in queued.into_iter().zip(suppress) {
            if host.stopping() {
                return sent;
            }
            // Not acknowledged when it could not be recorded: the workspace
            // offers it again after a reconnect.
            let Ok(record) = host.commit_suppressing(event, suppress).await else {
                return sent;
            };
            sent |= sent_live(&record);
            host.turn_recorded(run, position);
            if let Some(position) = position {
                host.workspace().ack(position);
            }
        }
        sent
    }
}

/// Record a resume fallback: its summary handoff, the context transfer it
/// resolves, and the "Recovered context" item at the run's start.
async fn commit_recovery<H: TurnHost>(
    host: &H,
    run: &Run,
    fallback: &crate::context_handoff::Recovery,
) {
    let _ = host
        .commit(Event::ContextHandoffUpdated {
            thread_id: run.thread_id.clone(),
            handoff: fallback.handoff.clone(),
            transfer: Some(fallback.transfer.clone()),
            turn_item: fallback.turn_item.clone(),
        })
        .await;
    // The notice sits right after the run's prompt, ahead of anything its
    // provider reports.
    if let Some(notice) = &fallback.notice {
        let mut items = host
            .store
            .read()
            .await
            .threads
            .get(&run.thread_id)
            .map(|thread| thread.stored_row()["appTurnItems"].clone())
            .and_then(|items| items.as_array().cloned())
            .unwrap_or_default();
        let mut notice = notice.clone();
        notice[crate::thread_projection::LINK_CARD_POSITION] = json!(0);
        items.push(notice);
        let _ = host
            .commit(Event::ThreadPatched {
                thread_id: run.thread_id.clone(),
                patch: json!({"row": {"appTurnItems": items}}),
            })
            .await;
    }
}

/// Record `turn`'s events as `run`'s, acknowledging each once it is in the
/// event log, so a workspace that keeps unacknowledged events redelivers
/// the rest after a reconnect or a restart.
async fn record<H: TurnHost>(
    owner: &Arc<H>,
    run: &Run,
    cwd: String,
    mut turn: TurnItems,
    mut pending_handoff: Option<crate::context_handoff::PreparedHandoff>,
    mut recovery: Option<crate::context_handoff::Recovery>,
) -> anyhow::Result<()> {
    let host: &H = owner;
    let done = |position: Option<StreamPosition>| {
        host.turn_recorded(run, position);
        if let Some(position) = position {
            host.workspace().ack(position);
        }
    };
    let mut exit = None;
    let mut text = StreamedText::default();
    let mut assistant = assistant_streaming::AssistantText::for_run(host, run).await?;
    let mut idle_timer: Option<(u64, crate::runtime::Wait)> = None;
    // The handoffs' history went to the native thread (Codex), not inline.
    let mut injected = false;
    let mut observed_native_thread: Option<String> = pending_handoff
        .as_ref()
        .and_then(|prepared| prepared.native_thread_id.clone());
    let mut snapshots = Snapshots {
        reads_first: run
            .driver
            .as_deref()
            .is_some_and(crate::workspace::provider::is_acp),
        ..Default::default()
    };
    loop {
        use futures_util::FutureExt;
        enum Woke {
            Next(Option<Box<TurnItem>>),
            Text,
            Snapshots,
            Assistant(u64),
        }
        async fn wait(timer: Option<&mut crate::runtime::Wait>) {
            match timer {
                Some(timer) => timer.await,
                None => std::future::pending().await,
            }
        }
        let deadline = assistant.deadline().filter(|_| !snapshots.terminal_seen);
        match deadline {
            Some(at) if idle_timer.as_ref().is_none_or(|(armed, _)| at < *armed) => {
                idle_timer = Some((
                    at,
                    host.turn_wait(Duration::from_millis(at.saturating_sub(crate::now()))),
                ));
            }
            None => idle_timer = None,
            _ => {}
        }
        let woke = futures_util::select_biased! {
            next = turn.recv().fuse() => Woke::Next(next.map(Box::new)),
            () = wait(snapshots.flush_at.as_mut()).fuse() => Woke::Snapshots,
            () = wait(text.flush_at.as_mut()).fuse() => Woke::Text,
            () = wait(idle_timer.as_mut().map(|(_, timer)| timer)).fuse() => Woke::Assistant(idle_timer.take().unwrap().0),
        };
        let next = match woke {
            Woke::Next(next) => next.map(|item| *item),
            Woke::Assistant(at) => {
                let due = assistant.due(at.max(crate::now()));
                if due.is_empty() {
                    continue;
                }
                snapshots.flush(host, run).await;
                for item in due {
                    host.commit(Event::RunProvider {
                        visible_text: item.fields["text"].as_str().map(str::to_owned),
                        thread_id: run.thread_id.clone(),
                        run_id: run.id,
                        position: None,
                        event: ProviderEvent::TurnItem { item },
                    })
                    .await?;
                }
                continue;
            }
            Woke::Snapshots => {
                snapshots.tick(host, run).await;
                continue;
            }
            Woke::Text => {
                // Queued snapshots came before the held text.
                snapshots.flush(host, run).await;
                text.flush(host, run, Flush::Window).await;
                continue;
            }
        };
        let Some(TurnItem {
            mut event,
            position,
        }) = next
        else {
            // Events that stop without the turn's end still report what is held.
            snapshots.flush(host, run).await;
            text.flush(host, run, Flush::Window).await;
            break;
        };
        if host.stopping() {
            return Ok(());
        }
        // A session's start goes to the instance's provider status; the
        // thread's records keep none of it.
        if let TurnEvent::Provider(ProviderEvent::ProviderThread {
            session_started, ..
        }) = &mut event
        {
            if let Some(started) = session_started.take() {
                owner.clone().provider_session_started(run, *started);
            }
        }
        if matches!(&event, TurnEvent::Provider(ProviderEvent::TurnItemDelta { delta, .. })
            if delta.is_empty() && text.held.is_empty())
        {
            // No text to record.
            done(position);
            continue;
        }
        if let TurnEvent::Provider(ProviderEvent::TurnItemDelta { item_id, delta, .. }) = &event {
            if let Some(item) = assistant.append(item_id, delta) {
                event = TurnEvent::Provider(ProviderEvent::TurnItem { item });
            }
        }
        let Some(event) = text.take(host, event, position) else {
            continue;
        };
        // A native subagent's child thread exists before its parent reports it.
        if let TurnEvent::Provider(ProviderEvent::TurnItem { item }) = &event {
            crate::native_subagents::record_child(
                host,
                &run.thread_id,
                run.driver.as_deref().unwrap_or("codex"),
                item,
            )
            .await;
        }
        let visible_text = match &event {
            TurnEvent::Provider(ProviderEvent::TurnItem { item }) => {
                assistant.filter(item, crate::now())
            }
            _ => None,
        };
        let snapshot = snapshots.is_snapshot(&event);
        // While the worker is busy, an event that sends nothing live takes
        // no part in the batch: it is recorded at once.
        let provider_thread = matches!(
            &event,
            TurnEvent::Provider(ProviderEvent::ProviderThread { .. })
        );
        if provider_thread {
            snapshots.thread_seen = true;
        }
        if let TurnEvent::Provider(ProviderEvent::ProviderTurn {
            history_injected: true,
            ..
        }) = &event
        {
            injected = true;
        }
        let accepted_prompt = matches!(
            &event,
            TurnEvent::Provider(
                ProviderEvent::PromptDelivered { .. } | ProviderEvent::ProviderTurn { .. }
            )
        );
        // A prompt's acceptance and its thread's loading record a pending
        // handoff; then they end the batch.
        let records_handoff = pending_handoff.is_some() && (accepted_prompt || provider_thread);
        if !snapshot && !records_handoff && text.held.is_empty() && snapshots.flush_at.is_some() {
            if let TurnEvent::Provider(provider) = &event {
                if !matches!(provider, ProviderEvent::TurnTerminal { .. }) {
                    let candidate = Event::RunProvider {
                        visible_text: None,
                        run_id: run.id,
                        thread_id: run.thread_id.clone(),
                        event: provider.clone(),
                        position,
                    };
                    if Snapshots::invisible(host, &candidate).await {
                        if host.commit(candidate).await.is_ok() {
                            done(position);
                        }
                        continue;
                    }
                }
            }
        }
        // Held text, newer than any queued snapshot, ends their batch; so
        // does anything else.
        if !snapshot || !text.held.is_empty() {
            snapshots.flush(host, run).await;
        }
        if let TurnEvent::Provider(ProviderEvent::ProviderThread {
            native_thread_id,
            resumed,
            ..
        }) = &event
        {
            observed_native_thread = Some(native_thread_id.clone());
            // The native thread loaded: record handoffs held until it did.
            if let Some(prepared) = pending_handoff.as_mut().filter(|p| !p.recorded) {
                commit_handoffs(host, run, prepared.pending.iter().cloned()).await;
                prepared.recorded = true;
            }
            // The session it was to resume could not be, so the workspace
            // started a fresh one with the recovery prompt: record the
            // fallback, whose delivery replaces the resumed session's.
            match recovery.take() {
                // The provider forked the source natively: the fork's
                // transfer resolves to its new session.
                Some(fallback) if *resumed && fallback.native_transfer.is_some() => {
                    if let (Some(mut transfer), Some(driver)) =
                        (fallback.native_transfer, run.driver.as_deref())
                    {
                        transfer["resolution"]["providerThreadRef"] = json!({
                            "driver": driver, "nativeId": native_thread_id, "strength": "strong"
                        });
                        let _ = host
                            .commit(Event::ContextHandoffUpdated {
                                thread_id: run.thread_id.clone(),
                                handoff: Value::Null,
                                transfer: Some(transfer),
                                turn_item: None,
                            })
                            .await;
                    }
                }
                // A provider that reports its running session again (ACP)
                // names the same one.
                Some(fallback) if !resumed && *native_thread_id != fallback.resumes => {
                    commit_recovery(host, run, &fallback).await;
                    commit_handoffs(host, run, fallback.delivery.pending.iter().cloned()).await;
                    let mut delivery = fallback.delivery;
                    delivery.recorded = true;
                    pending_handoff = Some(delivery);
                }
                _ => {}
            }
        }
        // Held text is recorded before any later event: an item's end
        // carries its whole text, but its deltas come first, in order.
        match &event {
            // The turn's end shows what is held, as the reference flushes
            // a turn's text when it ends.
            TurnEvent::Exited(_) | TurnEvent::Provider(ProviderEvent::TurnTerminal { .. }) => {
                text.flush(host, run, Flush::Window).await
            }
            TurnEvent::Provider(ProviderEvent::TurnItem { item }) if item.status != "running" => {
                text.flush(host, run, Flush::Before(Some(&item.id))).await
            }
            _ => text.flush(host, run, Flush::Before(None)).await,
        }
        if matches!(
            &event,
            TurnEvent::Provider(ProviderEvent::TurnTerminal { .. })
        ) {
            snapshots.terminal_seen = true;
        }
        match recorded(run, event, position, visible_text) {
            Recorded::Event(event) if snapshot => {
                snapshots.push(host, run, *event, position).await;
            }
            Recorded::Event(event) => {
                // An event that could not be recorded is not acknowledged,
                // so the workspace offers it again after a reconnect.
                if host.commit(*event).await.is_ok() {
                    done(position);
                    if accepted_prompt {
                        if let Some(prepared) = pending_handoff.take() {
                            let delivered = prepared.into_delivered_as(
                                if injected { "injected" } else { "inline" },
                                observed_native_thread.as_deref(),
                                crate::now(),
                            );
                            commit_handoffs(host, run, delivered).await;
                        }
                    }
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
    if let Some(prepared) = pending_handoff.take() {
        if prepared.recorded && !host.stopping() {
            commit_handoffs(host, run, prepared.into_unsent(crate::now())).await;
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
    // A steering restart's turn ends into the run's next attempt: no
    // checkpoint and no run end (`interrupt_restart`).
    let restart = host.steering_restarts.lock().await.remove(&run.id);
    if let Some(restart) = restart {
        let (open, run_now) = {
            let store = host.store.read().await;
            let thread = store.threads.get(&run.thread_id);
            (
                thread
                    .and_then(|thread| thread.provider.superseded.get(&run.id))
                    .and_then(|attempts| attempts.last())
                    .is_some_and(|attempt| attempt.open),
                thread.and_then(|thread| thread.runs.iter().find(|r| r.id == run.id).cloned()),
            )
        };
        // A turn that ended without saying how ends interrupted.
        if open {
            let _ = host
                .commit(Event::RunProvider {
                    run_id: run.id,
                    thread_id: run.thread_id.clone(),
                    event: ProviderEvent::TurnTerminal {
                        status: TurnStatus::Interrupted,
                        failure: None,
                        thread_disposition: crate::workspace::provider::ThreadDisposition::Reusable,
                        driver: None,
                        closing: Vec::new(),
                    },
                    position: None,
                    visible_text: None,
                })
                .await;
        }
        done(exit_position);
        if let Some(next) = run_now {
            owner.clone().restart_turn(next, restart.cwd, restart.text);
        }
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
    // A stop's delegated tasks stop and settle once its turn has ended, as
    // the reference's stop effect follows its interrupt; a hung child gives
    // up after the reference's 30 s.
    if matches!(run_status, "interrupted" | "cancelled") {
        if let Some(stop) = owner.clone().turn_stopped(run) {
            let wait = host.turn_wait(Duration::from_secs(30));
            futures_util::pin_mut!(wait);
            let _ = futures_util::future::select(stop, wait).await;
        }
    }
    if run_status == "completed" {
        if let Some(completed) = owner.clone().provider_run_completed(run) {
            completed.await;
        }
    }
    // Stopped turns also leave a rollback point. A stopped provider run is
    // already terminal in the projection; only a completed one waits for capture.
    if matches!(run_status, "completed" | "interrupted" | "cancelled") {
        let turn_count = host
            .store
            .read()
            .await
            .threads
            .get(&thread_id)
            .and_then(|thread| {
                // Later input may already be queued. The checkpoint belongs to this
                // run's ordinal, including when its exit is replayed after a reconnect.
                let ordinal = thread.runs.iter().position(|r| r.id == run.id)? as u64 + 1;
                (!thread.checkpoints.iter().any(|c| c.turn_count == ordinal)).then_some(ordinal)
            });
        if let Some(turn_count) = turn_count {
            if let Some(tree) = host.workspace().capture_git_tree(&cwd).await {
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

#[cfg(test)]
mod snapshot_tests {
    use super::*;

    fn snapshot(id: &str, kind: &str, status: &str) -> Event {
        Event::RunProvider {
            visible_text: None,
            run_id: Uuid::nil(),
            thread_id: "t".into(),
            event: ProviderEvent::TurnItem {
                item: crate::workspace::provider::ProviderItem::new(id, kind, status),
            },
            position: None,
        }
    }

    #[test]
    fn a_text_snapshot_is_one_job_of_its_item() {
        // The workspace represents a reply as a single text snapshot.
        // Its completion replaces the preceding running snapshot.
        let events = [
            snapshot("a", "assistant_message", "running"),
            snapshot("a", "assistant_message", "completed"),
            snapshot("b", "assistant_message", "running"),
        ];
        let entities: Vec<Vec<String>> = events
            .iter()
            .map(|event| vec![grouped_snapshot_entity(event).unwrap()])
            .collect();
        let dropped = crate::control_state::coalesce(&entities);
        assert_eq!(dropped[0], vec!["item:a".to_owned()]);
        assert!(dropped[1].is_empty() && dropped[2].is_empty());
    }

    #[test]
    fn a_text_batch_keeps_the_last_change_to_each_record() {
        let events = ["running", "running", "completed", "completed"]
            .map(|status| snapshot("a", "assistant_message", status));
        let all = ["node.updated:a", "message.updated:a", "turn-item.updated:a"]
            .map(str::to_owned)
            .to_vec();
        let text = all[1..].to_vec();
        let projected = vec![
            (all.clone(), 3),
            (text.clone(), 2),
            (all.clone(), 3),
            (text.clone(), 2),
        ];
        // The last report refreshes the text but uses the same native
        // completion time. Its predecessor must still deliver the completed node.
        assert_eq!(
            superseded_snapshots(&events, projected),
            [all, text.clone(), text, vec![]]
        );
    }

    #[test]
    fn command_node_and_item_can_be_superseded_independently() {
        let events = [
            snapshot("call", "command_execution", "running"),
            snapshot("call", "command_execution", "completed"),
        ];
        let node = "node.updated:call".to_owned();
        let item = "turn-item.updated:call".to_owned();
        let both = vec![node, item.clone()];
        // If the running update opens the node, that node survives the
        // completion. The intervening node is already known to the batch.
        let dropped = superseded_snapshots(&events, vec![(both.clone(), 2), (both.clone(), 2)]);
        assert_eq!(dropped, [vec![item.clone()], vec![]]);
        // After approval, the pending tool's node is already running, so
        // only the item changes. Its completion introduces the node to
        // this batch and both item updates must survive.
        let dropped = superseded_snapshots(&events, vec![(vec![item], 1), (both, 2)]);
        assert!(dropped.iter().all(Vec::is_empty));
    }
}
