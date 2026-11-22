//! The reference's `server.startup` span in `userdata/logs/server.trace.ndjson`.
//!
//! The reference ends its `server.startup` span once its orchestration
//! recovery has run, with the log `V2 orchestration recovery completed` and
//! the recovery's counts as the span's one event. Its restart continuations
//! are `provider-runtime.continue` effects that recovery has already queued by
//! then. This server continues a restarted turn itself once it listens, so it
//! records the span when that pass has dispatched every continuation it owes.
//! Either way, the record says the restart's recovery has decided what to
//! continue (tests wait on it instead of a fixed delay).
//!
//! Only this span is written, as one line in the reference's
//! `effect-span` format.

use std::io::Write;
use std::path::Path;
use std::time::SystemTime;

use serde_json::{json, Value};

/// What a startup's provider runtime recovery did, as the reference's
/// `ProviderRuntimeRecoverySummary` counts it. This server has no effect
/// outbox, so it never retires or requeues effects.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct RecoverySummary {
    pub(crate) terminalized_runs: usize,
    pub(crate) stopped_sessions: usize,
    pub(crate) closed_requests: usize,
}

impl RecoverySummary {
    fn log_message(&self) -> String {
        // Effect's tracer records a log with arguments as the JSON array of
        // its message and arguments, pretty-printed.
        serde_json::to_string_pretty(&json!([
            "V2 orchestration recovery completed",
            {
                "terminalizedRuns": self.terminalized_runs,
                "stoppedSessions": self.stopped_sessions,
                "closedRequests": self.closed_requests,
                "retiredEffects": 0,
                "requeuedEffects": 0,
            }
        ]))
        .unwrap_or_default()
    }
}

fn unix_nanos(at: SystemTime) -> u128 {
    at.duration_since(SystemTime::UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_nanos())
}

fn hex_id(bytes: usize) -> String {
    let id = crate::platform::uuid_v4().simple().to_string();
    id[..bytes * 2].to_owned()
}

/// The `server.startup` span record, as the reference writes it.
pub(crate) fn startup_span(
    mode: &str,
    host: &str,
    port: u16,
    started: SystemTime,
    ended: SystemTime,
    summary: RecoverySummary,
) -> Value {
    let (start, end) = (unix_nanos(started), unix_nanos(ended));
    json!({
        "type": "effect-span",
        "name": "server.startup",
        "traceId": format!("{}{}", hex_id(8), hex_id(8)),
        "spanId": hex_id(8),
        "sampled": true,
        "kind": "server",
        "startTimeUnixNano": start.to_string(),
        "endTimeUnixNano": end.to_string(),
        "durationMs": end.saturating_sub(start) as f64 / 1e6,
        "attributes": {
            "server.mode": mode,
            "server.port": port,
            "server.host": host,
        },
        "events": [{
            "name": summary.log_message(),
            "timeUnixNano": end.to_string(),
            "attributes": {"effect.logLevel": "INFO"},
        }],
        "links": [],
        "exit": {"_tag": "Success"},
    })
}

/// Append `record` to the server trace in `logs`.
pub(crate) fn append(logs: &Path, record: &Value) -> std::io::Result<()> {
    std::fs::create_dir_all(logs)?;
    let mut line = serde_json::to_vec(record)?;
    line.push(b'\n');
    std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(logs.join("server.trace.ndjson"))?
        .write_all(&line)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_startup_span_carries_the_recovery_log_as_the_reference_writes_it() {
        let started = SystemTime::UNIX_EPOCH + std::time::Duration::from_millis(1_000);
        let ended = started + std::time::Duration::from_millis(83);
        let summary = RecoverySummary {
            terminalized_runs: 1,
            stopped_sessions: 1,
            closed_requests: 0,
        };
        let span = startup_span("desktop", "127.0.0.1", 35929, started, ended, summary);
        assert_eq!(span["name"], "server.startup");
        assert_eq!(span["attributes"]["server.port"], 35929);
        assert_eq!(span["durationMs"], 83.0);
        assert_eq!(span["traceId"].as_str().map(str::len), Some(32));
        assert_eq!(span["spanId"].as_str().map(str::len), Some(16));
        let event: Value =
            serde_json::from_str(span["events"][0]["name"].as_str().unwrap()).unwrap();
        assert_eq!(
            event,
            json!(["V2 orchestration recovery completed", {
                "terminalizedRuns": 1, "stoppedSessions": 1, "closedRequests": 0,
                "retiredEffects": 0, "requeuedEffects": 0}])
        );
    }

    #[test]
    fn each_record_is_one_line_of_the_trace() {
        let logs = tempfile::tempdir().unwrap();
        let now = SystemTime::now();
        for port in [1, 2] {
            let span = startup_span("web", "::1", port, now, now, RecoverySummary::default());
            append(logs.path(), &span).unwrap();
        }
        let text = std::fs::read_to_string(logs.path().join("server.trace.ndjson")).unwrap();
        let ports: Vec<Value> = text
            .lines()
            .map(|line| {
                serde_json::from_str::<Value>(line).unwrap()["attributes"]["server.port"].clone()
            })
            .collect();
        assert_eq!(ports, vec![json!(1), json!(2)]);
    }
}
