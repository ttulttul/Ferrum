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

/// How many open `subscribeThread` streams watch each thread.
#[derive(Default)]
pub struct Watchers(Arc<Mutex<HashMap<String, usize>>>);

/// One open stream's hold on its thread; dropping it lets the thread go.
pub struct Watch {
    watchers: Arc<Mutex<HashMap<String, usize>>>,
    thread_id: String,
}

impl Watchers {
    pub(crate) fn watch(&self, thread_id: &str) -> Watch {
        *self
            .0
            .lock()
            .unwrap()
            .entry(thread_id.to_owned())
            .or_default() += 1;
        Watch {
            watchers: self.0.clone(),
            thread_id: thread_id.to_owned(),
        }
    }

    pub(crate) fn is_watched(&self, thread_id: &str) -> bool {
        self.0.lock().unwrap().contains_key(thread_id)
    }
}

impl Drop for Watch {
    fn drop(&mut self) {
        let mut watchers = self.watchers.lock().unwrap();
        if let Some(count) = watchers.get_mut(&self.thread_id) {
            *count -= 1;
            if *count == 0 {
                watchers.remove(&self.thread_id);
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
    // Under the read lock, only cheap checks: which threads qualify.
    let (read, planned, source) = {
        let store = control.store.read().await;
        let mut read = Vec::new();
        let mut planned = Vec::new();
        for (id, thread) in &store.threads {
            if !releasable(control, thread, idle) {
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

/// Sweeps every [`SWEEP`] until the server stops.
#[cfg(feature = "native")]
pub(crate) async fn run(state: Arc<crate::AppState>) {
    let idle = idle();
    let period = SWEEP.min(idle.max(Duration::from_secs(1)));
    let mut ticker = tokio::time::interval(period);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    ticker.tick().await;
    loop {
        ticker.tick().await;
        let released = sweep(&state.control, idle).await;
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
fn return_freed_memory() -> bool {
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
