//! The reference's effect outbox (`orchestration_v2_effect_outbox`), as
//! its `EffectOutbox` keeps it: what a new server process does to the rows
//! the previous one left, the restart continuations it owes, and settling a
//! row this server took over (docs/shared-store.md, 2.4).
use crate::shared_store::PendingContinuation;
use rusqlite::{params, Connection};

/// Effects that only the process that enqueued them can carry out
/// (`PROCESS_BOUND_EFFECT_TYPES`).
const PROCESS_BOUND: &[&str] = &[
    "provider-session.apply-model-selection",
    "provider-session.apply-thread-title",
    "provider-turn.start",
    "provider-turn.interrupt",
    "provider-turn.steer",
    "provider-turn.restart",
    "runtime-request.respond",
];

/// Effects a later process may carry out again
/// (`REPLAY_SAFE_EFFECT_TYPES_AFTER_PROCESS_LOSS`).
const REPLAY_SAFE: &[&str] = &[
    "provider-runtime.continue",
    "provider-session.detach",
    "provider-thread.rollback",
    "checkpoint.capture",
    "scheduled-tasks.cleanup",
    "terminal.cleanup",
    "preview.cleanup",
    "attachment.cleanup",
    "thread-title.generate",
    "delegated-tasks.stop",
];

/// Now as the reference's `DateTime.formatIso` writes it.
pub(crate) fn now_iso() -> String {
    crate::auth_service::iso(crate::auth_service::now())
}

fn quoted(types: &[&str]) -> String {
    types
        .iter()
        .map(|t| format!("'{t}'"))
        .collect::<Vec<_>>()
        .join(", ")
}

/// `reconcileAfterProcessLoss`: the previous process's process-bound effects
/// are cancelled, and replay-safe effects it was running wait to run again.
/// Returns (requeued, cancelled).
pub fn reconcile_after_process_loss(conn: &Connection) -> rusqlite::Result<(usize, usize)> {
    let now = now_iso();
    let cancelled = conn.execute(
        &format!(
            "UPDATE orchestration_v2_effect_outbox
             SET status = 'cancelled', lease_owner = NULL, completed_at = ?1, updated_at = ?1,
                 last_error = 'Cancelled because the server process ended before the effect completed.'
             WHERE status IN ('pending', 'running') AND effect_type IN ({})",
            quoted(PROCESS_BOUND)
        ),
        [&now],
    )?;
    let requeued = conn.execute(
        &format!(
            "UPDATE orchestration_v2_effect_outbox
             SET status = 'pending', lease_owner = NULL, available_at = ?1, updated_at = ?1,
                 last_error = 'Requeued after the previous server process ended.'
             WHERE status = 'running' AND effect_type IN ({})",
            quoted(REPLAY_SAFE)
        ),
        [&now],
    )?;
    Ok((requeued, cancelled))
}

/// Pending `provider-runtime.continue` effects, oldest first.
pub fn pending_continuations(conn: &Connection) -> rusqlite::Result<Vec<PendingContinuation>> {
    let mut statement = conn.prepare(
        "SELECT effect_id, thread_id, payload_json FROM orchestration_v2_effect_outbox
         WHERE status = 'pending' AND effect_type = 'provider-runtime.continue'
         ORDER BY available_at ASC, created_at ASC, effect_id ASC",
    )?;
    let rows = statement.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, String>(2)?,
        ))
    })?;
    let mut out = Vec::new();
    for row in rows {
        let (effect_id, thread_id, payload) = row?;
        let payload: serde_json::Value = serde_json::from_str(&payload).unwrap_or_default();
        let Some(source) = payload["sourceRunId"].as_str() else {
            continue;
        };
        out.push(PendingContinuation {
            effect_id,
            thread_id,
            source_run_id: source.to_owned(),
            process_loss_count: payload["processLossCount"].as_u64().unwrap_or(0) as u32,
        });
    }
    Ok(out)
}

/// Settles a pending effect this server carried out, as the reference's
/// worker claims (attempt counted) and settles it.
pub fn settle(conn: &Connection, effect_id: &str, status: &str) -> rusqlite::Result<()> {
    let now = now_iso();
    conn.execute(
        "UPDATE orchestration_v2_effect_outbox
         SET status = ?2, attempt_count = attempt_count + 1, lease_owner = NULL,
             completed_at = ?3, updated_at = ?3, last_error = NULL
         WHERE effect_id = ?1 AND status IN ('pending', 'running')",
        params![effect_id, status, now],
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn insert(conn: &Connection, id: &str, kind: &str, status: &str, payload: &str) {
        conn.execute(
            "INSERT INTO orchestration_v2_effect_outbox (effect_id, command_id, thread_id, effect_type, payload_json,
               status, attempt_count, available_at, created_at, updated_at)
             VALUES (?1, 'c', 'thread:o', ?2, ?3, ?4, 0, 'x', 'x', 'x')",
            params![id, kind, payload, status],
        )
        .unwrap();
    }

    #[test]
    fn a_new_process_cancels_process_bound_effects_and_requeues_replay_safe_ones() {
        let home = tempfile::tempdir().unwrap();
        let conn = crate::statev2::open(home.path()).unwrap();
        insert(
            &conn,
            "e1",
            "provider-turn.start",
            "pending",
            r#"{"type":"provider-turn.start","runId":"r"}"#,
        );
        insert(
            &conn,
            "e2",
            "provider-runtime.continue",
            "running",
            r#"{"type":"provider-runtime.continue","sourceRunId":"run:1","processLossCount":2}"#,
        );
        insert(
            &conn,
            "e3",
            "checkpoint.capture",
            "succeeded",
            r#"{"type":"checkpoint.capture"}"#,
        );
        assert_eq!(reconcile_after_process_loss(&conn).unwrap(), (1, 1));
        let status = |id: &str| -> String {
            conn.query_row(
                "SELECT status FROM orchestration_v2_effect_outbox WHERE effect_id = ?1",
                [id],
                |r| r.get(0),
            )
            .unwrap()
        };
        assert_eq!(status("e1"), "cancelled");
        assert_eq!(status("e2"), "pending");
        assert_eq!(status("e3"), "succeeded");
        let pending = pending_continuations(&conn).unwrap();
        assert_eq!(
            pending,
            vec![PendingContinuation {
                effect_id: "e2".into(),
                thread_id: "thread:o".into(),
                source_run_id: "run:1".into(),
                process_loss_count: 2
            }]
        );
        settle(&conn, "e2", "succeeded").unwrap();
        assert_eq!(status("e2"), "succeeded");
        assert!(pending_continuations(&conn).unwrap().is_empty());
    }
}
