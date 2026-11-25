//! A thread's records as `statev2.sqlite` holds them (docs/shared-store.md),
//! either read whole or read lazily.
//!
//! At start-up a thread read from the database gets only its slim records:
//! the thread row and every small collection, but no nodes and only the turn
//! items its shell row looks at, plus what the shell needs to count the rest
//! (`statev2::read::slim_projections`). That is enough for the thread's typed
//! state and its shell row. Everything else (snapshots, tools, and every
//! commit's plan) asks for the whole projection through [`Stored::get`],
//! which reads it from the database the first time and keeps it. The slim
//! records are never handed out, so nothing can diff or serve them.
//!
//! A whole projection nothing has used for a while can be released again
//! (`projection_eviction`): the thread goes back to its slim records and is
//! read whole the next time anything needs it.
use serde_json::Value;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};

/// Reads one thread's records from the database.
pub(crate) trait Hydrate: Send + Sync {
    /// The whole projection (`statev2::read::thread_projection`).
    fn read(&self, thread_id: &str) -> anyhow::Result<Option<Value>>;
    /// The whole projection and the slim records, read as of one commit, so
    /// a thread can go back to its slim records (`statev2::read::slim_projections`).
    fn read_both(&self, thread_id: &str) -> anyhow::Result<Option<(Value, Slim)>>;
}

/// Milliseconds since this process started, for last use.
fn now_ms() -> u64 {
    crate::platform::monotonic_ms()
}

/// What start-up reads of a thread instead of its whole projection.
#[derive(Debug, Default)]
pub(crate) struct Slim {
    /// The projection with every collection but `nodes` (left empty),
    /// `turnItems` (only the items the shell looks at) and `messages` (only
    /// users' messages and runs' prompts) in full.
    pub projection: Value,
    /// The lowest ordinal of a `user_message` or `assistant_message` turn
    /// item naming each message of `projection`, by message id
    /// (`sortMessagesByTurnItemOrder`).
    pub message_ordinals: HashMap<String, i64>,
    /// How many of the thread's turn items `projection` leaves out, by run
    /// id (the `run_id` column).
    pub unloaded_items: Vec<(Option<String>, u64)>,
}

impl Slim {
    /// Drops what the shell row does not read, once the typed state is
    /// built: the collections only the typed state reads.
    pub(crate) fn keep_only_shell_records(&mut self) {
        let Some(projection) = self.projection.as_object_mut() else {
            return;
        };
        for key in [
            "subagents",
            "providerTurns",
            "checkpointScopes",
            "checkpoints",
            "contextHandoffs",
            "contextTransfers",
        ] {
            if let Some(Value::Array(list)) = projection.get_mut(key) {
                *list = Vec::new();
            }
        }
    }
}

struct Lazy {
    thread_id: String,
    slim: Slim,
    whole: OnceLock<Box<Value>>,
    source: Arc<dyn Hydrate>,
}

#[derive(Clone)]
enum Repr {
    Whole(Box<Value>),
    Lazy(Arc<Lazy>),
}

/// A thread's stored projection.
#[derive(Clone)]
pub(crate) struct Stored(Repr, Arc<AtomicU64>);

impl std::fmt::Debug for Stored {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.0 {
            Repr::Whole(_) => f.write_str("Stored::Whole"),
            Repr::Lazy(lazy) => write!(
                f,
                "Stored::Lazy {{ hydrated: {} }}",
                lazy.whole.get().is_some()
            ),
        }
    }
}

#[cfg(test)]
thread_local! {
    /// Whole projections read on this test thread.
    pub(crate) static HYDRATIONS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

impl Stored {
    /// A projection read (or planned) whole.
    pub(crate) fn whole(projection: Value) -> Self {
        Stored(
            Repr::Whole(Box::new(projection)),
            Arc::new(AtomicU64::new(now_ms())),
        )
    }

    /// A thread's slim records, read whole from `source` when first needed.
    pub(crate) fn lazy(thread_id: String, slim: Slim, source: Arc<dyn Hydrate>) -> Self {
        Stored(
            Repr::Lazy(Arc::new(Lazy {
                thread_id,
                slim,
                whole: OnceLock::new(),
                source,
            })),
            Arc::new(AtomicU64::new(now_ms())),
        )
    }

    /// The whole projection, read from the database the first time when
    /// only the slim records are loaded.
    pub(crate) fn try_get(&self) -> anyhow::Result<&Value> {
        self.1.store(now_ms(), Ordering::Relaxed);
        let lazy = match &self.0 {
            Repr::Whole(projection) => return Ok(projection),
            Repr::Lazy(lazy) => lazy,
        };
        if let Some(projection) = lazy.whole.get() {
            return Ok(projection);
        }
        let started = std::time::Instant::now();
        let projection = lazy.source.read(&lazy.thread_id)?.ok_or_else(|| {
            anyhow::anyhow!(
                "the shared orchestration database has no row for thread {}",
                lazy.thread_id
            )
        })?;
        #[cfg(test)]
        HYDRATIONS.with(|count| count.set(count.get() + 1));
        tracing::debug!(
            thread_id = %lazy.thread_id,
            elapsed_ms = started.elapsed().as_millis() as u64,
            "read a thread's projection from the shared orchestration database"
        );
        // Another reader may have set it meanwhile; both read the same rows.
        let _ = lazy.whole.set(Box::new(projection));
        Ok(lazy.whole.get().expect("just set"))
    }

    /// The whole projection; see [`Stored::try_get`]. A database that fails
    /// to read here fails the caller: callers that change the thread read it
    /// first with `try_get` (`shared_store::hydrate_touched`).
    pub(crate) fn get(&self) -> &Value {
        match self.try_get() {
            Ok(projection) => projection,
            Err(error) => panic!("could not read a stored thread's projection: {error:#}"),
        }
    }

    /// Whether only the slim records are loaded.
    #[cfg(test)]
    pub(crate) fn is_partial(&self) -> bool {
        matches!(&self.0, Repr::Lazy(lazy) if lazy.whole.get().is_none())
    }

    /// Whether the whole projection is held in memory.
    pub(crate) fn is_resident(&self) -> bool {
        match &self.0 {
            Repr::Whole(_) => true,
            Repr::Lazy(lazy) => lazy.whole.get().is_some(),
        }
    }

    /// Milliseconds since the whole projection was last asked for.
    pub(crate) fn idle_ms(&self) -> u64 {
        now_ms().saturating_sub(self.1.load(Ordering::Relaxed))
    }

    /// Releases a whole projection read from the database, keeping the slim
    /// records it was read beside. Nothing else may hold this projection: a
    /// clone of it (a commit's scratch copy) keeps it.
    pub(crate) fn release_read(&mut self) -> bool {
        match &mut self.0 {
            Repr::Lazy(lazy) => Arc::get_mut(lazy).is_some_and(|lazy| lazy.whole.take().is_some()),
            Repr::Whole(_) => false,
        }
    }

    /// Whether this is a projection a commit planned (not one read lazily).
    pub(crate) fn is_planned(&self) -> bool {
        matches!(self.0, Repr::Whole(_))
    }

    /// While only the slim records are loaded: those records, the message
    /// order, and how many turn items they leave out by run id, for the
    /// shell row alone (`Thread::shell`).
    pub(crate) fn slim_for_shell(&self) -> Option<SlimView<'_>> {
        match &self.0 {
            Repr::Lazy(lazy) if lazy.whole.get().is_none() => Some(SlimView {
                projection: &lazy.slim.projection,
                message_ordinals: &lazy.slim.message_ordinals,
                unloaded_items: &lazy.slim.unloaded_items,
            }),
            _ => None,
        }
    }
}

/// See [`Stored::slim_for_shell`].
pub(crate) struct SlimView<'a> {
    pub projection: &'a Value,
    pub message_ordinals: &'a HashMap<String, i64>,
    pub unloaded_items: &'a [(Option<String>, u64)],
}
