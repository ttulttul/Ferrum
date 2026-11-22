//! What a server restart does to provider turns, as the reference's
//! ProviderRuntimeRecoveryService and RestartContinuation do it.
//!
//! No provider CLI survives the server that started it. When the server stops
//! (`shutdown`), or starts after stopping without doing so (`startup`), each
//! thread's provider runtime is reconciled:
//!
//! - a run whose provider never took its prompt goes back in the queue, held,
//!   when a user sent it (`rewindUnacceptedRuns`);
//! - every other unfinished run (preparing, starting, running or waiting) is
//!   cut off: it ends cancelled with a notice, its unfinished records end with
//!   it, and waiting runtime requests close;
//! - queued runs wait to be sent, and the provider session stops;
//! - an earlier turn's command still running in the background is cancelled,
//!   and the run whose next turn should hear of it records it
//!   (`run.background-work-cancelled`).
//!
//! A run whose provider turn was still going in a live session, or a settled
//! run that was waiting on background work the restart cut off, is owed a
//! continuation: once the server is up again, it sends the turn's native
//! thread a prompt to continue, as a notification in a new run. A
//! continuation that fails before its provider takes the prompt is tried
//! again after a minute, then after three.
use crate::{Event, RestartCut, Run, Store, Thread};
use serde_json::{json, Value};
use std::collections::HashSet;
use uuid::Uuid;

/// The prompt that continues a cut-off turn, as the reference words it.
pub(crate) const CONTINUE_PROMPT: &str = "Continue where you left off. Your last turn was cut short, so a command or other action it started may have finished or may still be running. Before you repeat one that changes state, confirm it did not take effect and is not still running; if you cannot tell, say so instead of repeating it.";

const RESUMED_SUMMARY: &str = "T3 Code restarted and resumed this turn";
const STOPPED_SUMMARY: &str = "T3 Code restarted and stopped background work";
const NOTE_HEADING: &str = "Note: the T3 server restarted, and this background work was cancelled before it finished. It will not report back on its own, so resume or restart what you still need and wait for its result:";
const MAX_NOTE_ENTRIES: usize = 10;
const MAX_LABEL_LENGTH: usize = 160;

/// A turn that keeps taking the server down is not continued after this
/// many lost processes in a row (the reference's `MAX_CONTINUATION_CHAIN`).
const MAX_PROCESS_LOSSES: u32 = 3;
/// Without a loss count, a chain of continuations this long within
/// [`CONTINUATION_LOOP_WINDOW`] seconds stops.
const CONTINUATION_LOOP_WINDOW: u64 = 5 * 60;

/// Seconds before a continuation that failed to start is tried again, by
/// how many have failed in a row (`continuationStartRetryDelayMs`).
const CONTINUATION_START_RETRY_DELAYS: [u64; 2] = [60, 180];

/// A scheduled prompt whose start restarts cut off this many times is not
/// sent again (the reference's `MAX_RESENDS`).
const MAX_RESENDS: u32 = 3;

/// The summary of the notification that sends `text`, a continuation's
/// prompt: a turn resumed, or only a note of background work stopped.
pub(crate) fn continuation_summary(text: &str) -> &'static str {
    if text.starts_with(NOTE_HEADING) && !text.ends_with(CONTINUE_PROMPT) {
        STOPPED_SUMMARY
    } else {
        RESUMED_SUMMARY
    }
}

/// A run's provider turn: whether the provider took the run's prompt.
fn turn_started(thread: &Thread, run: &Run) -> bool {
    thread.provider.runs.get(&run.id).is_some_and(|record| {
        record.turn_started || record.native_turn_id.is_some() || !record.items.is_empty()
    })
}

/// How `run`'s provider turn stands, if it has one.
fn turn_status(thread: &Thread, run: &Run) -> Option<&'static str> {
    if !turn_started(thread, run) {
        return None;
    }
    use crate::workspace::provider::TurnStatus;
    Some(
        match thread.provider.terminals.get(&run.id).map(|t| t.status) {
            Some(TurnStatus::Completed) => "completed",
            Some(TurnStatus::Failed) => "failed",
            Some(TurnStatus::Interrupted) => "interrupted",
            Some(TurnStatus::Running) | None if run.restart_cut.is_some() => "cancelled",
            Some(TurnStatus::Running) | None => "running",
        },
    )
}

/// A thread's runs as recovery sees them: a run marked running or starting
/// whose provider never took the prompt has not started
/// (`rewindUnacceptedRuns`).
struct View<'a> {
    thread: &'a Thread,
    unaccepted: HashSet<Uuid>,
}

impl<'a> View<'a> {
    fn new(thread: &'a Thread) -> Self {
        let unaccepted = thread
            .runs
            .iter()
            .filter(|run| {
                matches!(run.status.as_str(), "running" | "starting") && !turn_started(thread, run)
            })
            .map(|run| run.id)
            .collect();
        Self { thread, unaccepted }
    }

    fn status<'r>(&self, run: &'r Run) -> &'r str {
        if self.unaccepted.contains(&run.id) {
            "starting"
        } else {
            &run.status
        }
    }

    /// The run has no start (`startedAt`), and continues no earlier run.
    fn never_started(&self, run: &Run) -> bool {
        run.restart_continuation_of.is_none()
            && (matches!(run.status.as_str(), "queued" | "preparing")
                || self.unaccepted.contains(&run.id))
    }

    /// The latest run that started, if any (`latestStartedRun`).
    fn latest_started(&self) -> Option<&'a Run> {
        self.thread.runs.iter().rev().find(|run| {
            run.status != "queued" && run.status != "rolled_back" && !self.never_started(run)
        })
    }
}

/// Whether a thread's item still runs after its turn: one the projection
/// shows running.
fn item_open(thread: &Thread, run: &Run, record: &crate::provider_state::ItemRecord) -> bool {
    use crate::workspace::provider::TurnStatus;
    if !matches!(
        record.item.status.as_str(),
        "pending" | "running" | "waiting"
    ) || run.restart_cut.is_some()
        || thread
            .provider
            .cancelled_items
            .contains_key(&(run.id, record.item.id.clone()))
    {
        return false;
    }
    match thread.provider.terminals.get(&run.id).map(|t| t.status) {
        None | Some(TurnStatus::Running) => true,
        Some(TurnStatus::Completed) => {
            thread.provider.runs.get(&run.id).map(|r| r.driver.as_str())
                == Some(crate::workspace::provider::CODEX)
                && record.item.kind == "command_execution"
        }
        _ => false,
    }
}

fn compact_label(value: Option<&str>, max: usize) -> Option<String> {
    let text = value?.split_whitespace().collect::<Vec<_>>().join(" ");
    if text.is_empty() {
        return None;
    }
    let chars: Vec<char> = text.chars().collect();
    if chars.len() <= max {
        return Some(text);
    }
    Some(chars[..max - 1].iter().collect::<String>() + "…")
}

/// The background work an item of `run` stands for, as the continuation's
/// note names it (`cancelledTurnItemWork`).
fn item_work(driver: &str, record: &crate::provider_state::ItemRecord) -> Option<Value> {
    let item = &record.item;
    let field = |key: &str| item.fields.get(key).and_then(Value::as_str);
    let id = || crate::provider_projection::item_id("turn-item", driver, &item.id);
    match item.kind.as_str() {
        "command_execution" => {
            let mut work = json!({
                "kind": if item.fields.get("monitor") == Some(&json!(true)) { "monitor" } else { "shell" },
                "label": compact_label(field("input"), MAX_LABEL_LENGTH)
                    .or_else(|| compact_label(item.title.as_deref(), MAX_LABEL_LENGTH))
                    .unwrap_or_else(|| "background command".into()),
                "id": id(),
            });
            if field("completionWake") == Some("always") {
                work["awaited"] = json!(true);
            }
            Some(work)
        }
        "dynamic_tool" => Some(json!({
            "kind": if item.fields.get("input").and_then(|i| i.get("persistent")) == Some(&json!(true)) { "monitor" } else { "task" },
            "label": compact_label(item.title.as_deref(), MAX_LABEL_LENGTH)
                .or_else(|| compact_label(field("toolName"), MAX_LABEL_LENGTH))
                .unwrap_or_else(|| "background tool".into()),
            "id": id(),
        })),
        "subagent" => Some(json!({
            "kind": "subagent",
            "label": compact_label(item.title.as_deref(), MAX_LABEL_LENGTH)
                .or_else(|| compact_label(field("prompt"), MAX_LABEL_LENGTH))
                .unwrap_or_else(|| "subagent".into()),
            "id": id(),
        })),
        _ => None,
    }
}

/// Shells and monitors wake a turn only when it said it waits on them
/// (`isAwaitedWork`).
fn awaited(work: &Value) -> bool {
    work["awaited"] == json!(true) || !matches!(work["kind"].as_str(), Some("shell" | "monitor"))
}

/// `current` with `added` that it does not have yet
/// (`mergeRestartCancelledBackgroundWork`).
pub(crate) fn merge_work(current: &[Value], added: &[Value]) -> Vec<Value> {
    let identity = |entry: &Value| {
        entry["id"]
            .as_str()
            .map(str::to_owned)
            .unwrap_or_else(|| format!("{}\u{0}{}", entry["kind"], entry["label"]))
    };
    let mut seen: HashSet<String> = current.iter().map(identity).collect();
    let mut merged = current.to_vec();
    for entry in added {
        if seen.insert(identity(entry)) {
            merged.push(entry.clone());
        }
    }
    merged
}

/// The note that tells a continued turn what the restart cut off
/// (`restartCancelledBackgroundWorkNote`).
fn work_note(work: &[Value]) -> String {
    let mut lines = vec![NOTE_HEADING.to_owned()];
    for entry in work.iter().take(MAX_NOTE_ENTRIES) {
        lines.push(format!(
            "- {}: {}",
            entry["kind"].as_str().unwrap_or("task"),
            entry["label"].as_str().unwrap_or_default()
        ));
    }
    if work.len() > MAX_NOTE_ENTRIES {
        lines.push(format!("- and {} more", work.len() - MAX_NOTE_ENTRIES));
    }
    lines.join("\n")
}

/// The events that reconcile every thread's provider runtime after a
/// restart's `trigger` (`shutdown` or `startup`) at `at` (seconds).
/// `now_iso` is the time a snooze is compared with; `settings` are the
/// effective settings.
pub(crate) fn reconcile(
    store: &Store,
    trigger: &str,
    at: u64,
    now_iso: &str,
    settings: &Value,
) -> Vec<Event> {
    let mut threads: Vec<&Thread> = store.threads.values().collect();
    threads.sort_by(|a, b| a.id.cmp(&b.id));
    threads
        .into_iter()
        .filter_map(|thread| {
            let enabled = continues_after_restart(settings, &thread.project_id);
            reconcile_thread(thread, trigger, at, now_iso, enabled)
        })
        .collect()
}

fn reconcile_thread(
    thread: &Thread,
    trigger: &str,
    at: u64,
    now_iso: &str,
    continue_after_restart: bool,
) -> Option<Event> {
    let view = View::new(thread);
    let pending_request = |run: &Run| {
        thread
            .provider
            .runs
            .get(&run.id)
            .is_some_and(|record| record.requests.iter().any(|r| r.pending()))
    };
    let nonterminal: Vec<&Run> = thread
        .runs
        .iter()
        .filter(|run| {
            matches!(
                view.status(run),
                "preparing" | "starting" | "running" | "waiting"
            )
        })
        .collect();
    // Runs whose provider never took the prompt, with the message they send.
    let unstarted: Vec<(&Run, &crate::ConversationMessage)> = nonterminal
        .iter()
        .filter(|run| {
            view.status(run) == "starting" && view.never_started(run) && !pending_request(run)
        })
        .filter_map(|run| {
            let message = thread.messages.iter().find(|message| {
                Some(&message.id) == run.user_message_id.as_ref() && message.role == "user"
            })?;
            Some((*run, message))
        })
        .collect();
    // A user's message waits in the queue again, where its sender sees it.
    let requeued: Vec<Uuid> = unstarted
        .iter()
        .filter(|(_, message)| message.scheduled_task_id.is_none())
        .map(|(run, _)| run.id)
        .collect();
    // Nobody reviews a scheduled prompt, so it is sent again instead, unless
    // the thread was put away or restarts keep cutting it off.
    let resent: Vec<Uuid> = unstarted
        .iter()
        .filter(|(run, message)| {
            message.scheduled_task_id.is_some()
                && continue_after_restart
                && !put_away(thread, now_iso)
                && run.resends < MAX_RESENDS
        })
        .map(|(run, _)| run.id)
        .collect();
    for (run, message) in &unstarted {
        if message.scheduled_task_id.is_some() && run.resends >= MAX_RESENDS {
            tracing::warn!(
                thread_id = %thread.id,
                run_id = %run.id,
                "Not resending a prompt that restarts cut too many times"
            );
        }
    }
    let cut_runs: Vec<&Run> = nonterminal
        .into_iter()
        .filter(|run| !requeued.contains(&run.id) && !resent.contains(&run.id))
        .collect();

    // What the restart cuts off that the next turn should hear of.
    let mut work: Vec<Value> = Vec::new();
    let mut stale_items = Vec::new();
    for run in &thread.runs {
        let Some(record) = thread.provider.runs.get(&run.id) else {
            continue;
        };
        let cut = cut_runs.iter().any(|cut| cut.id == run.id);
        for item in &record.items {
            if !matches!(
                item.item.kind.as_str(),
                "command_execution" | "dynamic_tool" | "subagent"
            ) {
                continue;
            }
            if cut {
                // A cut turn's own work resumes with it, save a waiting
                // run's and a native subagent's.
                let native_subagent = item.item.kind == "subagent"
                    && item.item.fields.get("origin") == Some(&json!("provider_native"));
                if matches!(item.item.status.as_str(), "pending" | "running" | "waiting")
                    && (view.status(run) == "waiting" || native_subagent)
                {
                    work.extend(item_work(&record.driver, item));
                }
                continue;
            }
            if item_open(thread, run, item) {
                stale_items.push((run.id, item.item.id.clone()));
                work.extend(item_work(&record.driver, item));
            }
        }
    }
    let background: Vec<(Uuid, Vec<Value>)> = match view.latest_started() {
        Some(note_run) if !work.is_empty() && thread.provider.runs.contains_key(&note_run.id) => {
            vec![(note_run.id, merge_work(&note_run.background_work, &work))]
        }
        _ => vec![],
    };

    // The continuation owed after the restart, decided on the thread as the
    // restart found it.
    let continued = continue_after_restart
        .then(|| continuation_source(&view, now_iso))
        .flatten()
        .map(|run| {
            // A graceful stop is no loss. A loss counts on from the losses
            // of the run this one continued, unless that one settled.
            let losses = if trigger == "shutdown" {
                0
            } else {
                1 + run
                    .restart_continuation_of
                    .filter(|_| !matches!(run.status.as_str(), "completed" | "waiting"))
                    .and_then(|source| effect_losses(thread, source))
                    .unwrap_or(0)
            };
            (run.id, losses)
        });
    let cut: Vec<(Uuid, RestartCut)> = cut_runs
        .iter()
        .map(|run| {
            (
                run.id,
                RestartCut {
                    trigger: trigger.to_owned(),
                    at,
                    continuation: continued
                        .filter(|(id, _)| *id == run.id)
                        .map(|(_, losses)| losses),
                },
            )
        })
        .collect();
    let continued = continued.filter(|(id, _)| !cut.iter().any(|(cut, _)| cut == id));
    let held: Vec<Uuid> = thread
        .runs
        .iter()
        .filter(|run| run.status == "queued" && !run.queue_held)
        .map(|run| run.id)
        .collect();
    let provider = &thread.provider;
    let session_open = provider.session_live()
        || provider
            .thread
            .as_ref()
            .is_some_and(|record| record.status == "active");
    if cut.is_empty()
        && held.is_empty()
        && requeued.is_empty()
        && resent.is_empty()
        && stale_items.is_empty()
        && continued.is_none()
        && !session_open
    {
        return None;
    }
    Some(Event::ProviderRuntimeReconciled {
        thread_id: thread.id.clone(),
        cut,
        held,
        requeued,
        resent,
        stale_items,
        background,
        continued,
    })
}

/// The processes lost under the continuation owed to `run`, as its
/// `provider-runtime.continue` effect counts them: its own, or for a
/// continuation retried after failing to start, its source's.
fn effect_losses(thread: &Thread, run: Uuid) -> Option<u32> {
    let mut current = thread.runs.iter().find(|r| r.id == run)?;
    for _ in 0..thread.runs.len() {
        if let Some(losses) = current.owed_continuation() {
            return Some(losses);
        }
        retry_delay(thread, current)?;
        let source = current.restart_continuation_of?;
        current = thread.runs.iter().find(|r| r.id == source)?;
    }
    None
}

/// The run a restart continues, as the reference's `restartContinuationRun`
/// picks it before the restart reconciles the thread: the thread's latest
/// started run, still running in a live session on a known native thread,
/// or settled while it waited on background work, in a thread nobody put
/// away, with nothing waiting for an answer.
fn continuation_source<'a>(view: &View<'a>, now_iso: &str) -> Option<&'a Run> {
    let thread = view.thread;
    let run = thread
        .runs
        .iter()
        .rev()
        .find(|run| run.status != "queued" && !view.never_started(run))?;
    let status = view.status(run);
    let prepared = status == "starting" && run.restart_continuation_of.is_some();
    let settled = matches!(status, "completed" | "waiting");
    if status != "running" && !prepared && !settled {
        return None;
    }
    if put_away(thread, now_iso)
        || (!settled
            && matches!(
                turn_status(thread, run),
                Some("completed" | "failed" | "interrupted")
            ))
        || thread
            .provider
            .runs
            .values()
            .any(|record| record.requests.iter().any(|request| request.pending()))
    {
        return None;
    }
    let provider = &thread.provider;
    let live_turn_required = !prepared && !settled;
    let native_thread = provider
        .thread
        .as_ref()
        .is_some_and(|t| t.native_id.is_some() && (!live_turn_required || t.status == "active"));
    if !provider.runs.contains_key(&run.id)
        || !native_thread
        || (!prepared && !provider.session_live())
    {
        return None;
    }
    if settled {
        return open_awaited_work(thread).then_some(run);
    }
    if live_turn_required && turn_status(thread, run) != Some("running") {
        return None;
    }
    Some(run)
}

/// Whether the thread has background work its agent waits on, still
/// running (`hasOpenAwaitedWork`).
fn open_awaited_work(thread: &Thread) -> bool {
    thread.runs.iter().any(|run| {
        thread.provider.runs.get(&run.id).is_some_and(|record| {
            record.items.iter().any(|item| {
                item_open(thread, run, item)
                    && item_work(&record.driver, item).is_some_and(|w| awaited(&w))
            })
        })
    })
}

/// Archived, deleted, settled or snoozed: a restart does not wake it.
fn put_away(thread: &Thread, now_iso: &str) -> bool {
    let row = thread.stored_row();
    thread.archived
        || thread.settled
        || !row["deletedAt"].is_null()
        || row["settledOverride"] == "settled"
        || row["snoozedUntil"]
            .as_str()
            .or(thread.snoozed_until.as_deref())
            .is_some_and(|until| until > now_iso)
}

/// A continuation the provider failed before it took the prompt
/// (`continuationFailedBeforePrompt`).
fn failed_before_prompt(thread: &Thread, run: &Run) -> bool {
    run.status == "failed" && run.restart_continuation_of.is_some() && !turn_started(thread, run)
}

/// Seconds after it failed that a continuation that failed to start is
/// tried again, or `None` to let it fail (`continuationStartRetryDelayMs`).
fn retry_delay(thread: &Thread, run: &Run) -> Option<u64> {
    if !failed_before_prompt(thread, run) {
        return None;
    }
    // These fail the same way however often they start.
    let class = thread
        .provider
        .terminals
        .get(&run.id)
        .and_then(|terminal| terminal.failure.as_ref())
        .map(|failure| failure.class.as_str());
    if matches!(
        class,
        Some("usage_limit" | "permission_error" | "validation_error")
    ) {
        return None;
    }
    let mut failed = 0;
    let mut current = Some(run);
    while let Some(run) = current.filter(|run| failed_before_prompt(thread, run)) {
        failed += 1;
        current = run
            .restart_continuation_of
            .and_then(|source| thread.runs.iter().find(|r| r.id == source));
        if failed > thread.runs.len() {
            break;
        }
    }
    CONTINUATION_START_RETRY_DELAYS.get(failed - 1).copied()
}

/// A run the reconcile cut off and continues, or a settled one with
/// background work its agent waited on (`isRestartContinuationSource`).
fn continuation_source_run(thread: &Thread, run: &Run) -> bool {
    if note_source(thread, run) {
        run.background_work.iter().any(awaited)
    } else {
        run.status == "cancelled"
    }
}

/// A run whose continuation only notes the background work the restart
/// stopped (`isRestartNoteSource`).
fn note_source(thread: &Thread, run: &Run) -> bool {
    matches!(run.status.as_str(), "completed" | "waiting" | "cancelled")
        && !run.background_work.is_empty()
        && turn_status(thread, run) != Some("cancelled")
}

/// A restart continuation to send.
pub(crate) struct Continuation {
    /// The run it continues.
    pub source: Uuid,
    /// The `message.dispatch` command.
    pub command: Value,
    /// When to send it (seconds).
    pub due: u64,
}

/// The `message.dispatch` commands that continue the runs a restart cut off
/// or woke, and retry those that failed to start, each with the run it
/// continues and when to send it, as the reference's `continueRestartedRun`
/// sends them. `settings` are the effective settings.
pub(crate) fn continuations(store: &Store, settings: &Value, now_iso: &str) -> Vec<Continuation> {
    let mut out = Vec::new();
    let mut threads: Vec<&Thread> = store.threads.values().collect();
    threads.sort_by(|a, b| a.id.cmp(&b.id));
    for thread in threads {
        if !continues_after_restart(settings, &thread.project_id) || put_away(thread, now_iso) {
            continue;
        }
        let view = View::new(thread);
        for (index, source) in thread.runs.iter().enumerate() {
            let (losses, due) = if let Some(losses) = source.owed_continuation() {
                (Some(losses), 0)
            } else if let Some(delay) = retry_delay(thread, source) {
                (
                    source
                        .restart_continuation_of
                        .and_then(|id| effect_losses(thread, id)),
                    source.finished_at.unwrap_or(source.started_at) + delay,
                )
            } else {
                continue;
            };
            let message_id = format!("message:restart-continuation:{}", source.wire_id());
            if thread
                .messages
                .iter()
                .any(|message| message.id == message_id)
                || !(continuation_source_run(thread, source)
                    || failed_before_prompt(thread, source))
            {
                continue;
            }
            // A later run supersedes it.
            if thread.runs[index + 1..].iter().any(|later| {
                later.status != "queued"
                    && !(later.status == "cancelled" && view.never_started(later))
            }) {
                continue;
            }
            let too_many = match losses {
                Some(losses) => losses > MAX_PROCESS_LOSSES,
                None => chain_length(thread, source) >= MAX_PROCESS_LOSSES,
            };
            if too_many {
                tracing::warn!(
                    thread_id = %thread.id,
                    source_run_id = %source.id,
                    "Not continuing a run restarted too many times in a row"
                );
                continue;
            }
            out.push(Continuation {
                source: source.id,
                command: command(thread, source, &message_id),
                due,
            });
        }
    }
    out
}

/// The runs a restart owes their prompt again, by thread, as the reference's
/// restart resend effects send them once the server is up.
pub(crate) fn resends(store: &Store) -> Vec<(String, Uuid)> {
    let mut threads: Vec<&Thread> = store.threads.values().collect();
    threads.sort_by(|a, b| a.id.cmp(&b.id));
    threads
        .into_iter()
        .flat_map(|thread| {
            thread
                .runs
                .iter()
                .filter(|run| {
                    run.resend_owed
                        && matches!(run.status.as_str(), "running" | "starting" | "preparing")
                })
                .map(|run| (thread.id.clone(), run.id))
        })
        .collect()
}

/// How many continuations in a row led to `source` within the loop window.
fn chain_length(thread: &Thread, source: &Run) -> u32 {
    let window_start = source
        .finished_at
        .unwrap_or(source.started_at)
        .saturating_sub(CONTINUATION_LOOP_WINDOW);
    let mut length = 0;
    let mut current = Some(source);
    while let Some(run) = current {
        let Some(previous) = run.restart_continuation_of else {
            break;
        };
        if length >= MAX_PROCESS_LOSSES || run.started_at < window_start {
            break;
        }
        length += 1;
        current = thread.runs.iter().find(|r| r.id == previous);
    }
    length
}

/// `continueThreadsAfterServerUpdate` for `project`: its override, else the
/// setting, which is on by default.
fn continues_after_restart(settings: &Value, project: &str) -> bool {
    settings["projectSettingsOverrides"][project]["continueThreadsAfterServerUpdate"]
        .as_bool()
        .or(settings["continueThreadsAfterServerUpdate"].as_bool())
        .unwrap_or(true)
}

/// The background work cut off under `source` and the runs it continued,
/// until a turn that completed told its agent, and whether that leaves only
/// a note to send (`restartContinuationNote`).
fn note(thread: &Thread, source: &Run) -> (Vec<Value>, bool) {
    let mut current = source;
    let mut work = current.background_work.clone();
    let mut visited = HashSet::from([current.id]);
    while let Some(previous) = current.restart_continuation_of {
        if turn_status(thread, current) == Some("completed") {
            break;
        }
        let Some(previous) = thread.runs.iter().find(|r| r.id == previous) else {
            break;
        };
        if !visited.insert(previous.id) {
            break;
        }
        current = previous;
        work = merge_work(&current.background_work, &work);
    }
    let settled = note_source(thread, current);
    (work, settled)
}

fn command(thread: &Thread, source: &Run, message_id: &str) -> Value {
    let (work, settled) = note(thread, source);
    let text = if work.is_empty() {
        CONTINUE_PROMPT.to_owned()
    } else if settled {
        work_note(&work)
    } else {
        format!("{}\n\n{CONTINUE_PROMPT}", work_note(&work))
    };
    let mut command = json!({
        "type": "message.dispatch",
        "commandId": format!("command:restart-continuation:{}", source.wire_id()),
        "threadId": thread.id,
        "messageId": message_id,
        "text": text,
        "notification": {
            "source": {"kind": "system"},
            "outcome": "updated",
            "summary": continuation_summary(&text),
            "detail": text
        },
        "attachments": [],
        "dispatchMode": {"type": "start_immediately"},
        "createdBy": "agent",
        "creationSource": "server",
        "restartContinuationOfRunId": source.wire_id()
    });
    let selection = &thread.stored_row()["modelSelection"];
    if selection.is_object() {
        command["modelSelection"] = selection.clone();
    }
    command
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::workspace::provider::{
        ProviderEvent, ProviderItem, RuntimeRequestOpen, ThreadDisposition, TurnStatus,
    };

    fn run(status: &str) -> Run {
        Run {
            id: Uuid::new_v4(),
            thread_id: "thread:a".into(),
            provider: "default".into(),
            status: status.into(),
            started_at: 10,
            finished_at: None,
            error: None,
            user_message_id: Some("message:a".into()),
            queue_held: false,
            restart_cut: None,
            restart_continuation_of: None,
            requeued_at: None,
            background_work: vec![],
            continuation: None,
            resend_owed: false,
            resends: 0,
            wire_id: None,
        }
    }

    fn thread(runs: Vec<Run>) -> Thread {
        Thread {
            row: json!({"id": "thread:a", "modelSelection": {"instanceId": "claudeAgent", "model": "m"},
                "deletedAt": null, "snoozedUntil": null}),
            id: "thread:a".into(),
            project_id: "project:a".into(),
            title: "A".into(),
            cwd: "/tmp".into(),
            status: "running".into(),
            messages: vec![],
            runs,
            created_at: 1,
            archived: false,
            settled: false,
            snoozed_until: None,
            pinned: false,
            unread: false,
            tags: vec![],
            checkpoints: vec![],
            link_requests: Vec::new(),
            provider: Default::default(),
            stored: None,
        }
    }

    /// A provider turn that started in a session on native thread `sess-1`
    /// and reported a running command.
    fn started(thread: &mut Thread, run: Uuid) {
        let events = [
            ProviderEvent::ProviderThread {
                driver: "claudeAgent".into(),
                native_thread_id: "sess-1".into(),
                resumed: false,
                model: None,
            },
            ProviderEvent::ProviderTurn {
                native_turn_id: None,
                status: TurnStatus::Running,
                model: None,
            },
            ProviderEvent::TurnItem {
                item: ProviderItem::new("tool-1", "command_execution", "running"),
            },
        ];
        for event in &events {
            thread.provider.apply(run, event, 11);
        }
    }

    fn store(thread: Thread) -> Store {
        let mut store = Store::default();
        store.threads.insert(thread.id.clone(), thread);
        store
    }

    fn apply(store: &mut Store, events: Vec<Event>) {
        for (id, event) in events.into_iter().enumerate() {
            store.apply_record(&crate::ServerEvent {
                id: id as u64 + 1,
                at: 20,
                event,
                receipt: None,
                thread_events: None,
            });
        }
    }

    #[test]
    fn a_running_turn_is_cut_off_and_continued_after_a_shutdown() {
        let first = run("running");
        let id = first.id;
        let mut a = thread(vec![first]);
        started(&mut a, id);
        let mut store = store(a);
        let events = reconcile(&store, "shutdown", 20, "2026-01-01T00:00:00Z", &json!({}));
        apply(&mut store, events);
        let thread = &store.threads["thread:a"];
        let cut = thread.runs[0].restart_cut.clone().unwrap();
        assert_eq!(thread.runs[0].status, "cancelled");
        assert_eq!(
            (cut.trigger.as_str(), cut.continuation),
            ("shutdown", Some(0))
        );
        assert!(!thread.provider.session_live());

        let projection = thread.projection();
        assert_eq!(projection["providerSessions"][0]["status"], "stopped");
        assert_eq!(projection["providerThreads"][0]["status"], "idle");
        assert_eq!(projection["providerTurns"][0]["status"], "cancelled");
        assert_eq!(projection["attempts"][0]["status"], "cancelled");
        let items: Vec<_> = projection["turnItems"]
            .as_array()
            .unwrap()
            .iter()
            .map(|i| {
                (
                    i["type"].as_str().unwrap(),
                    i["status"].as_str().unwrap(),
                    i["title"].clone(),
                )
            })
            .collect();
        assert_eq!(
            items,
            [
                ("command_execution", "cancelled", Value::Null),
                ("system_notice", "completed", json!("Server shut down"))
            ]
        );

        let commands = continuations(&store, &json!({}), "2026-01-01T00:00:00Z");
        assert_eq!(commands.len(), 1);
        assert_eq!(commands[0].source, id);
        assert_eq!(commands[0].command["text"], CONTINUE_PROMPT);
        assert_eq!(
            commands[0].command["restartContinuationOfRunId"],
            id.to_string()
        );
        // The setting, or a project's override, turns it off.
        let off = json!({"continueThreadsAfterServerUpdate": false});
        assert!(continuations(&store, &off, "2026-01-01T00:00:00Z").is_empty());
        let project_off = json!({"projectSettingsOverrides": {"project:a": {"continueThreadsAfterServerUpdate": false}}});
        assert!(continuations(&store, &project_off, "2026-01-01T00:00:00Z").is_empty());
    }

    #[test]
    fn an_idle_thread_only_has_its_session_stopped() {
        let mut first = run("completed");
        first.finished_at = Some(12);
        let id = first.id;
        let mut a = thread(vec![first]);
        started(&mut a, id);
        a.provider.apply(
            id,
            &ProviderEvent::TurnTerminal {
                status: TurnStatus::Completed,
                failure: None,
                thread_disposition: ThreadDisposition::Reusable,
                driver: None,
            },
            12,
        );
        let mut store = store(a);
        let events = reconcile(&store, "startup", 20, "2026-01-01T00:00:00Z", &json!({}));
        assert!(
            matches!(&events[..], [Event::ProviderRuntimeReconciled { cut, .. }] if cut.is_empty())
        );
        apply(&mut store, events);
        let projection = store.threads["thread:a"].projection();
        assert_eq!(projection["runs"][0]["status"], "completed");
        assert_eq!(projection["providerSessions"][0]["status"], "stopped");
        assert!(continuations(&store, &json!({}), "2026-01-01T00:00:00Z").is_empty());
        // Nothing is left to reconcile the next time.
        assert!(reconcile(&store, "startup", 30, "2026-01-01T00:00:00Z", &json!({})).is_empty());
    }

    #[test]
    fn a_turn_waiting_on_the_user_or_put_away_is_not_continued() {
        let first = run("running");
        let id = first.id;
        let mut a = thread(vec![first]);
        started(&mut a, id);
        let mut waiting = a.clone();
        waiting.provider.apply(
            id,
            &ProviderEvent::RuntimeRequestOpened {
                request: RuntimeRequestOpen {
                    id: "perm-1".into(),
                    native_request_id: "perm-1".into(),
                    native_item_id: "tool-1".into(),
                    kind: "command".into(),
                    runtime_mode: "full-access".into(),
                    supports_feedback: false,
                    fields: Default::default(),
                },
            },
            12,
        );
        let mut snoozed = a.clone();
        snoozed.row["snoozedUntil"] = json!("2026-02-01T00:00:00Z");
        for thread in [waiting, snoozed] {
            let events = reconcile(
                &store(thread.clone()),
                "startup",
                20,
                "2026-01-01T00:00:00Z",
                &json!({}),
            );
            let [Event::ProviderRuntimeReconciled { cut, .. }] = &events[..] else {
                panic!("one reconcile event");
            };
            assert_eq!(cut[0].1.continuation, None);
            assert_eq!(cut[0].1.trigger, "startup");
        }
    }

    #[test]
    fn a_turn_that_keeps_crashing_the_server_stops_being_continued() {
        let mut source = run("cancelled");
        source.restart_cut = Some(RestartCut {
            trigger: "startup".into(),
            at: 5,
            continuation: Some(MAX_PROCESS_LOSSES),
        });
        let mut next = run("running");
        next.restart_continuation_of = Some(source.id);
        let id = next.id;
        let mut a = thread(vec![source, next]);
        started(&mut a, id);
        let mut store = store(a);
        let events = reconcile(&store, "startup", 20, "2026-01-01T00:00:00Z", &json!({}));
        apply(&mut store, events);
        let cut = store.threads["thread:a"].runs[1]
            .restart_cut
            .clone()
            .unwrap();
        assert_eq!(cut.continuation, Some(MAX_PROCESS_LOSSES + 1));
        assert!(continuations(&store, &json!({}), "2026-01-01T00:00:00Z").is_empty());
    }

    /// A turn whose provider recorded a native thread but never took the
    /// prompt (Codex answered `thread/start`, not `turn/start`).
    fn accepted_thread_only(thread: &mut Thread, run: Uuid) {
        thread.provider.apply(
            run,
            &ProviderEvent::ProviderThread {
                driver: "codex".into(),
                native_thread_id: "thr-1".into(),
                resumed: false,
                model: None,
            },
            11,
        );
    }

    #[test]
    fn a_users_message_the_provider_never_took_goes_back_in_the_queue() {
        let mut first = run("running");
        first.user_message_id = Some("message:a".into());
        let id = first.id;
        let mut a = thread(vec![first]);
        a.messages.push(crate::ConversationMessage {
            id: "message:a".into(),
            role: "user".into(),
            content: "hi".into(),
            created_at: 10,
            creation_source: None,
            scheduled_task_id: None,
            follows_thread_selection: false,
            notification: None,
            steer: None,
            wire: None,
        });
        accepted_thread_only(&mut a, id);
        let mut store = store(a);
        let events = reconcile(&store, "shutdown", 20, "2026-01-01T00:00:00Z", &json!({}));
        let [Event::ProviderRuntimeReconciled { cut, requeued, .. }] = &events[..] else {
            panic!("one reconcile event");
        };
        assert!(cut.is_empty());
        assert_eq!(requeued, &[id]);
        apply(&mut store, events);
        let thread = &store.threads["thread:a"];
        assert_eq!(
            (thread.runs[0].status.as_str(), thread.runs[0].queue_held),
            ("queued", true)
        );
        let projection = thread.projection();
        assert_eq!(projection["runs"][0]["queuePosition"], 1);
        assert_eq!(projection["attempts"][0]["status"], "pending");
        assert_eq!(projection["attempts"][0]["startedAt"], Value::Null);
        assert!(projection["turnItems"].as_array().unwrap().is_empty());
        assert!(continuations(&store, &json!({}), "2026-01-01T00:00:00Z").is_empty());
    }

    #[test]
    fn a_preparing_run_is_cut_off_and_not_continued() {
        let mut first = run("preparing");
        first.user_message_id = Some("message:a".into());
        let mut store = store(thread(vec![first]));
        let events = reconcile(&store, "startup", 20, "2026-01-01T00:00:00Z", &json!({}));
        apply(&mut store, events);
        let run = &store.threads["thread:a"].runs[0];
        assert_eq!(run.status, "cancelled");
        assert_eq!(run.owed_continuation(), None);
    }

    #[test]
    fn a_command_running_after_its_turn_is_background_work_that_wakes_the_turn() {
        let mut first = run("completed");
        first.finished_at = Some(12);
        let id = first.id;
        let mut a = thread(vec![first]);
        for event in [
            ProviderEvent::ProviderThread {
                driver: "codex".into(),
                native_thread_id: "thr-1".into(),
                resumed: false,
                model: None,
            },
            ProviderEvent::ProviderTurn {
                native_turn_id: Some("turn-1".into()),
                status: TurnStatus::Running,
                model: None,
            },
            ProviderEvent::TurnItem {
                item: ProviderItem::new("exec-1", "command_execution", "running")
                    .with("input", "sleep  60")
                    .with("completionWake", "always"),
            },
            ProviderEvent::TurnTerminal {
                status: TurnStatus::Completed,
                failure: None,
                thread_disposition: ThreadDisposition::Reusable,
                driver: None,
            },
        ] {
            a.provider.apply(id, &event, 11);
        }
        // Running, after the completed turn.
        let command = |projection: &Value| {
            projection["turnItems"]
                .as_array()
                .unwrap()
                .iter()
                .find(|item| item["type"] == "command_execution")
                .unwrap()["status"]
                .clone()
        };
        assert_eq!(command(&a.projection()), "running");
        let mut store = store(a);
        let events = reconcile(&store, "shutdown", 20, "2026-01-01T00:00:00Z", &json!({}));
        apply(&mut store, events);
        let thread = &store.threads["thread:a"];
        let work = &thread.runs[0].background_work;
        assert_eq!(
            work,
            &[
                json!({"kind": "shell", "label": "sleep 60", "awaited": true,
                "id": "turn-item:provider:codex:native-item:exec-1"})
            ]
        );
        assert_eq!(thread.runs[0].continuation, Some(0));
        let projection = thread.projection();
        assert_eq!(command(&projection), "cancelled");
        assert_eq!(
            projection["runs"][0]["restartCancelledBackgroundWork"],
            json!(work)
        );
        let commands = continuations(&store, &json!({}), "2026-01-01T00:00:00Z");
        assert_eq!(commands.len(), 1);
        let text = commands[0].command["text"].as_str().unwrap();
        assert_eq!(text, format!("{NOTE_HEADING}\n- shell: sleep 60"));
        assert_eq!(
            commands[0].command["notification"]["summary"],
            STOPPED_SUMMARY
        );
    }

    #[test]
    fn a_continuation_that_fails_to_start_is_retried_twice() {
        let mut source = run("cancelled");
        source.restart_cut = Some(RestartCut {
            trigger: "shutdown".into(),
            at: 5,
            continuation: Some(1),
        });
        let mut failed = run("failed");
        failed.restart_continuation_of = Some(source.id);
        failed.finished_at = Some(100);
        failed.user_message_id = Some(format!("message:restart-continuation:{}", source.id));
        let mut a = thread(vec![source.clone(), failed.clone()]);
        a.messages.push(crate::ConversationMessage {
            id: format!("message:restart-continuation:{}", source.id),
            role: crate::thread_projection::NOTIFICATION_ROLE.into(),
            content: CONTINUE_PROMPT.into(),
            created_at: 90,
            creation_source: None,
            scheduled_task_id: None,
            follows_thread_selection: false,
            notification: None,
            steer: None,
            wire: None,
        });
        let commands = continuations(&store(a.clone()), &json!({}), "2026-01-01T00:00:00Z");
        assert_eq!(commands.len(), 1);
        assert_eq!((commands[0].source, commands[0].due), (failed.id, 160));
        // The retry carries its source's losses: a crash under it counts on.
        assert_eq!(effect_losses(&a, failed.id), Some(1));
        // A second failure waits three minutes; a third is final.
        let mut second = run("failed");
        second.restart_continuation_of = Some(failed.id);
        second.finished_at = Some(200);
        a.runs.push(second.clone());
        assert_eq!(retry_delay(&a, &second), Some(180));
        let mut third = run("failed");
        third.restart_continuation_of = Some(second.id);
        a.runs.push(third.clone());
        assert_eq!(retry_delay(&a, &third), None);
    }

    #[test]
    fn a_scheduled_prompt_the_provider_never_took_is_sent_again_three_times() {
        let mut first = run("running");
        first.user_message_id = Some("message:a".into());
        let id = first.id;
        let mut a = thread(vec![first]);
        a.messages.push(crate::ConversationMessage {
            id: "message:a".into(),
            role: "user".into(),
            content: "hi".into(),
            created_at: 10,
            creation_source: None,
            scheduled_task_id: Some("scheduled-task:a".into()),
            follows_thread_selection: true,
            notification: None,
            steer: None,
            wire: None,
        });
        accepted_thread_only(&mut a, id);
        let mut store = store(a);
        let events = reconcile(&store, "shutdown", 20, "2026-01-01T00:00:00Z", &json!({}));
        let [Event::ProviderRuntimeReconciled {
            cut,
            requeued,
            resent,
            ..
        }] = &events[..]
        else {
            panic!("one reconcile event");
        };
        assert!(cut.is_empty() && requeued.is_empty());
        assert_eq!(resent, &[id]);
        apply(&mut store, events);
        let thread = &store.threads["thread:a"];
        assert!(thread.runs[0].resend_owed);
        assert_eq!(thread.projection()["attempts"][0]["status"], "pending");
        assert_eq!(resends(&store), [("thread:a".to_owned(), id)]);

        // With continuation off, it is cut off instead.
        let off = json!({"continueThreadsAfterServerUpdate": false});
        let mut thread = store.threads["thread:a"].clone();
        thread.runs[0].resend_owed = false;
        let mut fresh = super::tests::store(thread);
        let events = reconcile(&fresh, "shutdown", 20, "2026-01-01T00:00:00Z", &off);
        let [Event::ProviderRuntimeReconciled { cut, resent, .. }] = &events[..] else {
            panic!("one reconcile event");
        };
        assert!(resent.is_empty());
        assert_eq!(cut.len(), 1);

        // After three resends restarts cut off, it is cut off too.
        let run = &mut fresh.threads.get_mut("thread:a").unwrap().runs[0];
        run.resends = MAX_RESENDS;
        let events = reconcile(&fresh, "shutdown", 20, "2026-01-01T00:00:00Z", &json!({}));
        let [Event::ProviderRuntimeReconciled { cut, resent, .. }] = &events[..] else {
            panic!("one reconcile event");
        };
        assert!(resent.is_empty());
        assert_eq!(cut.len(), 1);
    }

    #[test]
    fn a_subagent_the_cut_turn_waited_on_wakes_its_parent_when_it_finishes() {
        let first = run("running");
        let id = first.id;
        let mut a = thread(vec![first]);
        started(&mut a, id);
        a.row["subagents"] = json!([
            {"id": "node:delegated-task:1", "origin": "app_owned", "runId": id.to_string(),
             "completionWake": "settled_only", "status": "running"},
            {"id": "node:delegated-task:2", "origin": "app_owned", "runId": id.to_string(),
             "completionWake": "settled_only", "status": "completed"}
        ]);
        let mut store = store(a);
        let events = reconcile(&store, "shutdown", 20, "2026-01-01T00:00:00Z", &json!({}));
        apply(&mut store, events);
        let tasks = &store.threads["thread:a"].row["subagents"];
        assert_eq!(tasks[0]["completionWake"], "always");
        assert_eq!(tasks[1]["completionWake"], "settled_only");
    }
}
