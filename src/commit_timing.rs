//! Time spent in each phase of a commit, for measuring what a commit costs
//! (the commit-cost benchmark, and `T3CODE_DEV_COMMIT_TIMING=1` in a debug
//! build). Off by default: then each phase costs one atomic load.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex;
use std::time::{Duration, Instant};

static ENABLED: AtomicBool = AtomicBool::new(false);
static TOTALS: Mutex<BTreeMap<&'static str, (u64, Duration)>> = Mutex::new(BTreeMap::new());

/// Starts recording phases, and forgets what was recorded.
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) fn enable() {
    TOTALS.lock().unwrap().clear();
    ENABLED.store(true, Ordering::Relaxed);
}

/// Each phase's count and total time since [`enable`].
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) fn totals() -> BTreeMap<&'static str, (u64, Duration)> {
    TOTALS.lock().unwrap().clone()
}

/// Times one phase: from now until the returned guard drops.
pub(crate) fn phase(name: &'static str) -> Option<Phase> {
    ENABLED
        .load(Ordering::Relaxed)
        .then(|| Phase(name, Instant::now()))
}

pub(crate) struct Phase(&'static str, Instant);

impl Drop for Phase {
    fn drop(&mut self) {
        let mut totals = TOTALS.lock().unwrap();
        let entry = totals.entry(self.0).or_default();
        entry.0 += 1;
        entry.1 += self.1.elapsed();
    }
}
