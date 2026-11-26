//! Releases whole thread projections nothing has used for a while.
//!
//! A thread read lazily from `statev2.sqlite` (`stored_projection`) is read
//! whole the first time anything needs it, and a commit leaves the projection
//! it planned in memory. On a large database, opening many threads kept every
//! one of them resident. A sweep now returns a thread to its slim records when
//! its whole projection has gone unused for [`IDLE`], no `subscribeThread`
//! stream watches it, and none of its runs is still going. The next use reads
//! it whole again, and every commit still reads each thread it touches whole
//! before planning (`shared_store::hydrate_touched`).
//!
//! Only what the database holds identically is released: a projection read
//! from it, or a planned one whose rows, read again, are equal to it.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// How long a whole projection stays in memory after its last use.
pub(crate) const IDLE: Duration = Duration::from_secs(10 * 60);
/// How often the sweep looks.
pub(crate) const SWEEP: Duration = Duration::from_secs(60);

/// Run statuses after which a run does nothing more.
const SETTLED: [&str; 5] = [
    "completed",
    "failed",
    "cancelled",
    "interrupted",
    "rolled_back",
];

/// How long after its last `subscribeThread` stream closes a thread nothing
/// else uses is released (the sweep's other rules apply too).
pub(crate) const AFTER_LAST_WATCH: Duration = Duration::from_secs(30);
/// How often threads whose last stream closed are looked at.
pub(crate) const LEFT_SWEEP: Duration = Duration::from_secs(15);

#[derive(Default)]
struct WatchState {
    /// Open streams by thread.
    open: HashMap<String, usize>,
    /// When each thread's last stream closed, until it is looked at.
    left: HashMap<String, std::time::Instant>,
}

/// How many open `subscribeThread` streams watch each thread, and which
/// threads' last stream closed lately.
#[derive(Default)]
pub struct Watchers(Arc<Mutex<WatchState>>);

/// One open stream's hold on its thread; dropping it lets the thread go.
pub struct Watch {
    watchers: Arc<Mutex<WatchState>>,
    thread_id: String,
}

impl Watchers {
    pub(crate) fn watch(&self, thread_id: &str) -> Watch {
        let mut state = self.0.lock().unwrap();
        *state.open.entry(thread_id.to_owned()).or_default() += 1;
        state.left.remove(thread_id);
        drop(state);
        Watch {
            watchers: self.0.clone(),
            thread_id: thread_id.to_owned(),
        }
    }

    pub(crate) fn is_watched(&self, thread_id: &str) -> bool {
        self.0.lock().unwrap().open.contains_key(thread_id)
    }

    /// The threads whose last stream closed at least `after` ago, which are
    /// no longer tracked as such.
    fn take_left(&self, after: Duration) -> Vec<String> {
        let mut state = self.0.lock().unwrap();
        let now = std::time::Instant::now();
        let due: Vec<String> = state
            .left
            .iter()
            .filter(|(_, at)| now.duration_since(**at) >= after)
            .map(|(id, _)| id.clone())
            .collect();
        for id in &due {
            state.left.remove(id);
        }
        due
    }
}

impl Drop for Watch {
    fn drop(&mut self) {
        let mut state = self.watchers.lock().unwrap();
        if let Some(count) = state.open.get_mut(&self.thread_id) {
            *count -= 1;
            if *count == 0 {
                state.open.remove(&self.thread_id);
                state
                    .left
                    .insert(self.thread_id.clone(), std::time::Instant::now());
            }
        }
    }
}

/// Whether a thread may be returned to its slim records now.
fn releasable(
    control: &crate::control_state::ControlState,
    thread: &crate::Thread,
    idle: Duration,
) -> bool {
    let Some(stored) = thread.stored.as_ref() else {
        return false;
    };
    stored.is_resident()
        && stored.idle_ms() >= idle.as_millis() as u64
        && thread
            .runs
            .iter()
            .all(|run| SETTLED.contains(&run.status.as_str()))
        && !control.thread_watchers.is_watched(&thread.id)
}

tokio::task_local! {
    /// The threads read whole from the database while a counted call runs
    /// (`counting_reads`).
    static READS: std::cell::RefCell<Vec<String>>;
}

/// Records that `thread_id` was read whole, for the call counting reads.
pub(crate) fn note_read(thread_id: &str) {
    let _ = READS.try_with(|reads| reads.borrow_mut().push(thread_id.to_owned()));
}

/// Runs `call`, and gives the threads it read whole from the database.
pub(crate) async fn counting_reads<F: std::future::Future>(call: F) -> (F::Output, Vec<String>) {
    READS
        .scope(std::cell::RefCell::new(Vec::new()), async {
            let output = call.await;
            let reads = READS.with(|reads| reads.take());
            (output, reads)
        })
        .await
}

/// The most threads one call may read whole and keep: past it, the call's
/// reads are released as soon as it returns, and the server warns.
pub(crate) const CALL_READS_KEPT: usize = 64;

/// Runs `call` (an MCP tool call, an RPC or an HTTP request), and if it read
/// more than [`CALL_READS_KEPT`] threads whole, releases them as it returns
/// and warns: no single request can hold the database in memory until the
/// idle sweep.
pub(crate) async fn bounded_reads<F: std::future::Future>(
    control: &crate::control_state::ControlState,
    name: &str,
    call: F,
) -> F::Output {
    let (output, reads) = counting_reads(call).await;
    if reads.len() > CALL_READS_KEPT {
        let released = release_now(control, &reads).await;
        #[cfg(feature = "native")]
        return_freed_memory();
        tracing::warn!(
            call = name,
            read = reads.len(),
            released = released.read + released.planned,
            "a request read many threads whole; released them as it returned"
        );
    }
    output
}

/// Releases now each of `thread_ids` that may be released at all (no
/// subscriber, no run going; `sweep`), whatever its idle time.
pub(crate) async fn release_now(
    control: &crate::control_state::ControlState,
    thread_ids: &[String],
) -> Released {
    let ids: std::collections::HashSet<&str> = thread_ids.iter().map(String::as_str).collect();
    sweep_where(control, Duration::ZERO, |id| ids.contains(id)).await
}

/// What one sweep released.
#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct Released {
    pub read: usize,
    pub planned: usize,
}

/// Returns every thread idle for `idle` to its slim records.
pub(crate) async fn sweep(
    control: &crate::control_state::ControlState,
    idle: Duration,
) -> Released {
    sweep_where(control, idle, |_| true).await
}

async fn sweep_where(
    control: &crate::control_state::ControlState,
    idle: Duration,
    only: impl Fn(&str) -> bool,
) -> Released {
    // Under the read lock, only cheap checks: which threads qualify.
    let (read, planned, source) = {
        let store = control.store.read().await;
        let mut read = Vec::new();
        let mut planned = Vec::new();
        for (id, thread) in &store.threads {
            if !only(id) || !releasable(control, thread, idle) {
                continue;
            }
            if thread.stored.as_ref().is_some_and(|s| s.is_planned()) {
                planned.push(id.clone());
            } else {
                read.push(id.clone());
            }
        }
        (read, planned, store.stored_source.clone())
    };
    if read.is_empty() && planned.is_empty() {
        return Released::default();
    }
    // A planned projection goes back only when the database holds it: read
    // its rows again, outside any lock, and compare under the write lock.
    let mut reread = Vec::new();
    if let Some(source) = source.as_ref() {
        for id in planned {
            match source.read_both(&id) {
                Ok(Some((whole, slim))) => reread.push((id, whole, slim)),
                Ok(None) => {}
                Err(error) => {
                    tracing::debug!(%error, thread_id = %id, "could not read a thread to release it");
                }
            }
        }
    }
    let mut released = Released::default();
    let mut store = control.store.write().await;
    for id in read {
        let Some(thread) = store.threads.get(&id) else {
            continue;
        };
        // Used or changed since the read-locked look.
        if !releasable(control, thread, idle) {
            continue;
        }
        let thread = store.threads.get_mut(&id).unwrap();
        if thread
            .stored
            .as_mut()
            .is_some_and(|stored| stored.release_read())
        {
            released.read += 1;
        }
    }
    if let Some(source) = source {
        for (id, whole, mut slim) in reread {
            let Some(thread) = store.threads.get(&id) else {
                continue;
            };
            if !releasable(control, thread, idle) {
                continue;
            }
            let Some(stored) = thread.stored.as_ref() else {
                continue;
            };
            // A commit since the read changed it, or the database differs.
            if !stored.try_get().is_ok_and(|held| same_rows(held, &whole)) {
                continue;
            }
            slim.keep_only_shell_records();
            let thread = store.threads.get_mut(&id).unwrap();
            thread.stored = Some(crate::stored_projection::Stored::lazy(
                id.clone(),
                slim,
                source.clone(),
            ));
            released.planned += 1;
        }
    }
    released
}

/// Whether a planned projection `held` holds exactly the database's rows
/// `stored`: every record of every collection, by id, and the thread record.
/// A commit adds a record after the others where a read orders them, so
/// order is not compared. The top-level `updatedAt` is not a row (a read
/// takes it from the thread record), and an instance's shared provider
/// session is one row for all its threads, which another thread's commit
/// changes: the database's copy is the current one.
fn same_rows(held: &serde_json::Value, stored: &serde_json::Value) -> bool {
    use serde_json::Value;
    let by_id = |records: &Value| -> Option<HashMap<String, Value>> {
        let records = records.as_array()?;
        Some(
            records
                .iter()
                .filter(|record| {
                    !record["id"].as_str().is_some_and(|id| {
                        id.starts_with("provider-session:provider-instance:")
                            && id.ends_with(":shared")
                    })
                })
                .map(|record| (record["id"].to_string(), record.clone()))
                .collect(),
        )
    };
    let (Some(held_map), Some(stored_map)) = (held.as_object(), stored.as_object()) else {
        return false;
    };
    fn keys(map: &serde_json::Map<String, Value>) -> Vec<&String> {
        let mut keys: Vec<&String> = map.keys().filter(|key| *key != "updatedAt").collect();
        keys.sort();
        keys
    }
    if keys(held_map) != keys(stored_map) {
        return false;
    }
    stored_map.iter().all(|(key, value)| match key.as_str() {
        "updatedAt" => true,
        "thread" => held_map.get(key) == Some(value),
        _ => match (value.is_array(), by_id(value)) {
            (true, Some(stored)) => by_id(&held_map[key]) == Some(stored),
            _ => held_map.get(key) == Some(value),
        },
    })
}

/// The idle time a debug build may shorten, for measuring this.
#[cfg(feature = "native")]
fn idle() -> Duration {
    #[cfg(debug_assertions)]
    if let Some(seconds) = std::env::var("T3CODE_DEV_PROJECTION_IDLE_SECS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
    {
        return Duration::from_secs(seconds);
    }
    IDLE
}

static FREED: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

/// Notes that a large value (a detached snapshot's projection) was freed, so
/// the next look hands free memory back to the system.
pub(crate) fn freed_some() {
    FREED.store(true, std::sync::atomic::Ordering::Relaxed);
}

/// Releases the threads whose last stream closed [`AFTER_LAST_WATCH`] ago,
/// whose own last use was that long ago too, and that nothing else holds.
pub(crate) async fn release_left(control: &crate::control_state::ControlState) -> Released {
    release_left_after(control, AFTER_LAST_WATCH).await
}

pub(crate) async fn release_left_after(
    control: &crate::control_state::ControlState,
    after: Duration,
) -> Released {
    let left = control.thread_watchers.take_left(after);
    if left.is_empty() {
        return Released::default();
    }
    let ids: std::collections::HashSet<&str> = left.iter().map(String::as_str).collect();
    sweep_where(control, after, |id| ids.contains(id)).await
}

/// Sweeps every [`SWEEP`] until the server stops.
#[cfg(feature = "native")]
pub(crate) async fn run(state: Arc<crate::AppState>) {
    let idle = idle();
    let period = SWEEP.min(idle.max(Duration::from_secs(1)));
    let mut ticker = tokio::time::interval(LEFT_SWEEP);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    ticker.tick().await;
    let mut last_full = tokio::time::Instant::now();
    loop {
        ticker.tick().await;
        let mut released = release_left(&state.control).await;
        if last_full.elapsed() >= period {
            last_full = tokio::time::Instant::now();
            let swept = sweep(&state.control, idle).await;
            released.read += swept.read;
            released.planned += swept.planned;
        }
        let freed = FREED.swap(false, std::sync::atomic::Ordering::Relaxed);
        if released == Released::default() && freed {
            return_freed_memory();
        }
        if released != Released::default() {
            let trimmed = return_freed_memory();
            tracing::info!(
                read = released.read,
                planned = released.planned,
                trimmed,
                "released idle thread projections"
            );
        }
    }
}

/// Hands memory the allocator holds free back to the system. glibc keeps
/// what a thread frees in that thread's arena, so the projections a sweep
/// releases would otherwise stay in the server's resident memory.
#[cfg(feature = "native")]
pub(crate) fn return_freed_memory() -> bool {
    #[cfg(all(target_os = "linux", target_env = "gnu"))]
    {
        // SAFETY: malloc_trim only returns free pages to the system.
        unsafe { libc::malloc_trim(0) != 0 }
    }
    #[cfg(not(all(target_os = "linux", target_env = "gnu")))]
    {
        false
    }
}
