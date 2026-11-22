//! Native scheduled-task dispatch.
//!
//! The task definitions are shared in statev2.sqlite.  This module is the
//! process-owned part of the service: it claims due rows, turns them into
//! orchestration commands, and records the dispatch result.

use crate::{auth_service, orchestration, platform, AppState};
use serde_json::{json, Value};
use std::sync::Arc;
use time::{Duration, OffsetDateTime, PrimitiveDateTime, Time, Weekday};

const POLL_INTERVAL: std::time::Duration = std::time::Duration::from_secs(1);
const MISSED_FIXED_TIME_GRACE_MS: i64 = 10 * 60 * 1_000;
const RUN_OUTCOME_POLL: std::time::Duration = std::time::Duration::from_millis(50);

fn parse_iso(value: &str) -> Option<i64> {
    OffsetDateTime::parse(value, &time::format_description::well_known::Rfc3339)
        .ok()
        .map(|time| time.unix_timestamp_nanos() as i64 / 1_000_000)
}

pub(crate) fn next_run_at(schedule: &Value, from_ms: i64) -> Option<String> {
    let kind = schedule["type"].as_str()?;
    match kind {
        "interval" => {
            let every = schedule["everyMs"].as_i64()?.max(60_000);
            Some(auth_service::iso(from_ms.saturating_add(every)))
        }
        "once" => schedule["runAt"].as_str().and_then(|run_at| {
            (parse_iso(run_at).is_some_and(|at| at > from_ms)).then(|| run_at.to_owned())
        }),
        "fixed_time" => fixed_time_next(schedule, from_ms),
        _ => None,
    }
}

fn fixed_time_next(schedule: &Value, from_ms: i64) -> Option<String> {
    let time = schedule["timeOfDay"].as_str()?.trim();
    let (hour, minute) = time.split_once(':')?;
    let hour: u8 = hour.parse().ok()?;
    let minute: u8 = minute.parse().ok()?;
    if hour > 23 || minute > 59 {
        return None;
    }
    let now = OffsetDateTime::from_unix_timestamp_nanos(from_ms as i128 * 1_000_000).ok()?;
    let weekdays = schedule["weekdays"].as_array();
    for offset in 0..=7 {
        let date = now.date().checked_add(Duration::days(offset))?;
        let candidate =
            PrimitiveDateTime::new(date, Time::from_hms(hour, minute, 0).ok()?).assume_utc();
        if candidate.unix_timestamp_nanos() <= now.unix_timestamp_nanos() {
            continue;
        }
        let weekday = match candidate.weekday() {
            Weekday::Sunday => 0,
            Weekday::Monday => 1,
            Weekday::Tuesday => 2,
            Weekday::Wednesday => 3,
            Weekday::Thursday => 4,
            Weekday::Friday => 5,
            Weekday::Saturday => 6,
        };
        if weekdays.is_some_and(|days| !days.iter().any(|day| day.as_i64() == Some(weekday as i64)))
        {
            continue;
        }
        return Some(auth_service::iso(
            candidate.unix_timestamp_nanos() as i64 / 1_000_000,
        ));
    }
    None
}

fn task_command_id(task_id: &str, started_at: &str) -> String {
    format!("scheduled-task:{task_id}:{started_at}")
}

fn task_message_id(task_id: &str, started_at: &str) -> String {
    format!("scheduled-task-message:{task_id}:{started_at}")
}

fn dispatch_error(error: &Value) -> String {
    error["message"]
        .as_str()
        .or_else(|| error["detail"].as_str())
        .map(str::to_owned)
        .unwrap_or_else(|| "Could not dispatch scheduled task.".into())
}

fn next_run_after_run(task: &Value, started_ms: i64, completed_ms: i64) -> Option<String> {
    let from = if task["schedule"]["type"] == "once" {
        started_ms
    } else {
        completed_ms
    };
    task["enabled"]
        .as_bool()
        .filter(|enabled| *enabled)
        .and_then(|_| next_run_at(&task["schedule"], from))
}

fn workspace_path(task: &Value, project_path: Option<&str>) -> Option<String> {
    match task["workspaceStrategy"]["type"].as_str() {
        Some("existing_worktree") => task["workspaceStrategy"]["worktreePath"]
            .as_str()
            .map(str::to_owned),
        Some("worktree") => task["workspaceStrategy"]["path"]
            .as_str()
            .map(str::to_owned),
        _ => project_path.map(str::to_owned),
    }
}

/// Dispatches one task at a caller-supplied wall-clock time. Keeping the time
/// as an argument makes due-run behaviour deterministic in tests and avoids
/// coupling the scheduler to a process-global clock.
pub(crate) async fn run_due_at(state: Arc<AppState>, now_ms: i64) {
    let now = auth_service::iso(now_ms);
    let due = {
        let mut database = state.scheduled_database.lock().await;
        let tasks = match database.list() {
            Ok(tasks) => tasks,
            Err(error) => {
                tracing::warn!(%error, "could not list scheduled tasks");
                return;
            }
        };
        let mut claimed = Vec::new();
        for task in tasks {
            let Some(next) = task["nextRunAt"].as_str() else {
                continue;
            };
            if parse_iso(next).is_none_or(|at| at > now_ms)
                || task["enabled"] != true
                || task["lastRunStatus"] == "running"
            {
                continue;
            }
            match database.claim_due(task["id"].as_str().unwrap_or_default(), &now) {
                Ok(Some(mut claimed_task)) => {
                    claimed_task["_previousLastRunStatus"] = task["lastRunStatus"].clone();
                    claimed.push(claimed_task)
                }
                Ok(None) => {}
                Err(error) => tracing::warn!(%error, "could not claim scheduled task"),
            }
        }
        claimed
    };
    for task in due {
        dispatch_claimed(state.clone(), task, now_ms, "scheduled").await;
    }
}

async fn has_queued_occurrence(state: &Arc<AppState>, task: &Value) -> bool {
    let Some(thread_id) = task["threadId"].as_str() else {
        return false;
    };
    let task_id = task["id"].as_str().unwrap_or_default();
    let store = state.store.read().await;
    let Some(thread) = store.threads.get(thread_id) else {
        return false;
    };
    thread.runs.iter().any(|run| {
        run.status == "queued"
            && run
                .user_message_id
                .as_deref()
                .and_then(|message_id| thread.messages.iter().find(|m| m.id == message_id))
                .is_some_and(|message| message.scheduled_task_id.as_deref() == Some(task_id))
    })
}

/// Runs an explicit task immediately. The caller has already applied its
/// access checks. It returns the current public task after the dispatch was
/// accepted or recorded as failed.
pub(crate) async fn run_now(state: Arc<AppState>, id: &str) -> Result<Value, Value> {
    let now_ms = platform::now_ms();
    let now = auth_service::iso(now_ms);
    let task = {
        let mut database = state.scheduled_database.lock().await;
        database
            .claim_manual(id, &now)
            .map_err(|_| json!({"message":"Could not run scheduled task."}))?
            .ok_or_else(|| json!({"message":"Schedule task is already running."}))?
    };
    dispatch_claimed(state.clone(), task, now_ms, "manual").await;
    let task = state
        .scheduled_database
        .lock()
        .await
        .get(id)
        .map_err(|_| json!({"message":"Could not read schedule task."}))?
        .ok_or_else(|| json!({"message":"Schedule task not found."}))?;
    Ok(crate::subscription::scheduled_public_task(&task))
}

/// Accepts a webhook delivery and starts the task asynchronously. The HTTP
/// route has already authenticated the URL and signature before calling this.
pub(crate) async fn run_webhook(state: Arc<AppState>, id: &str) {
    let now_ms = platform::now_ms();
    let now = auth_service::iso(now_ms);
    let task = {
        let mut database = state.scheduled_database.lock().await;
        match database.claim_manual(id, &now) {
            Ok(task) => task,
            Err(error) => {
                tracing::warn!(%error, task_id = %id, "could not claim webhook task");
                return;
            }
        }
    };
    if let Some(task) = task {
        dispatch_claimed(state, task, now_ms, "webhook").await;
    }
}

async fn dispatch_claimed(state: Arc<AppState>, task: Value, started_ms: i64, trigger: &str) {
    let id = task["id"].as_str().unwrap_or_default().to_owned();
    let started_at = task["lastRunAt"]
        .as_str()
        .map(str::to_owned)
        .unwrap_or_else(|| auth_service::iso(started_ms));
    let due_at = task["nextRunAt"].as_str().and_then(parse_iso);
    if trigger == "scheduled"
        && task["schedule"]["type"] == "fixed_time"
        && due_at.is_some_and(|due| started_ms.saturating_sub(due) > MISSED_FIXED_TIME_GRACE_MS)
    {
        let next = next_run_at(&task["schedule"], started_ms);
        let mut database = state.scheduled_database.lock().await;
        if let Err(error) = database.reschedule_missed(
            &id,
            &started_at,
            &auth_service::iso(started_ms),
            "Missed a scheduled run because the server was not running at that time.",
            next.as_deref(),
        ) {
            tracing::warn!(%error, task_id = %id, "could not record missed scheduled task");
        }
        let _ = state.scheduled_task_events.send(());
        return;
    }

    if trigger == "scheduled"
        && task["schedule"]["type"] != "once"
        && has_queued_occurrence(&state, &task).await
    {
        let next = next_run_at(&task["schedule"], started_ms);
        let previous = task["_previousLastRunStatus"].as_str().unwrap_or("never");
        let mut database = state.scheduled_database.lock().await;
        if let Err(error) = database.release_claim(
            &id,
            &started_at,
            previous,
            &auth_service::iso(started_ms),
            next.as_deref(),
            Some("Skipped because the previous scheduled run is still queued."),
        ) {
            tracing::warn!(%error, task_id = %id, "could not release queued scheduled task");
        }
        let _ = state.scheduled_task_events.send(());
        return;
    }

    let outcome = dispatch_to_thread(state.clone(), &task, &started_at).await;
    let completed_ms = platform::now_ms();
    let next = next_run_after_run(&task, started_ms, completed_ms);
    let (status, error, thread_id, message_id) = match outcome {
        Ok((thread_id, message_id)) => ("succeeded", None, Some(thread_id), Some(message_id)),
        Err(error) => ("failed", Some(error), None, None),
    };
    let recorded = {
        let mut database = state.scheduled_database.lock().await;
        let result = database.complete(
            &id,
            &started_at,
            &auth_service::iso(completed_ms),
            status,
            error.as_deref(),
            next.as_deref(),
        );
        if let Err(error) = &result {
            tracing::warn!(%error, task_id = %id, "could not record scheduled task run");
        }
        let _ = state.scheduled_task_events.send(());
        result.unwrap_or(false)
    };
    if recorded {
        if let (Some(thread_id), Some(message_id)) = (thread_id, message_id) {
            tokio::spawn(record_provider_outcome(
                state, id, started_at, thread_id, message_id,
            ));
        }
    }
}

async fn dispatch_to_thread(
    state: Arc<AppState>,
    task: &Value,
    started_at: &str,
) -> Result<(String, String), String> {
    let id = task["id"].as_str().unwrap_or_default();
    let command_id = task_command_id(id, started_at);
    let message_id = task_message_id(id, started_at);
    let project_id = task["projectId"].as_str().unwrap_or_default();
    let (project_path, active_thread) = {
        let store = state.store.read().await;
        let active_thread = task["threadId"].as_str().map(str::to_owned);
        let project_path = if active_thread.is_none() {
            Some(
                store
                    .projects
                    .get(project_id)
                    .map(|project| project.path.clone())
                    .ok_or_else(|| format!("Project {project_id} was not found."))?,
            )
        } else {
            None
        };
        (project_path, active_thread)
    };
    let thread_mode = task["threadMode"].as_str().unwrap_or("new_thread");
    let thread_id = if let Some(thread_id) = active_thread.as_ref() {
        thread_id.clone()
    } else {
        let thread_id = format!("thread:{command_id}");
        let create = json!({
            "type":"thread.create", "commandId":command_id, "threadId":thread_id,
            "projectId":project_id, "title":task["title"],
            "modelSelection":task["modelSelection"], "runtimeMode":task["runtimeMode"],
            "interactionMode":task["interactionMode"], "branch":null,
            "worktreePath":workspace_path(task, project_path.as_deref()), "createdBy":task["createdBy"],
            "creationSource":task["creationSource"]
        });
        orchestration::dispatch_orchestration_command(state.clone(), create)
            .await
            .map_err(|error| dispatch_error(&error))?;
        if thread_mode != "new_thread" {
            let mut database = state.scheduled_database.lock().await;
            if let Ok(Some(mut current)) = database.get(id) {
                current["threadId"] = Value::String(thread_id.clone());
                let _ = database.save(&current);
            }
        }
        thread_id
    };
    let active = state
        .store
        .read()
        .await
        .threads
        .get(&thread_id)
        .is_some_and(|thread| thread.runs.iter().any(|run| run.status == "running"));
    let mut dispatch = json!({
        "type":"message.dispatch", "commandId":format!("{command_id}:message"),
        "threadId":thread_id, "messageId":message_id, "text":task["prompt"],
        "attachments":[], "dispatchMode":{"type":if active {"queue_after_active"} else {"start_immediately"}},
        "createdBy":task["createdBy"], "creationSource":task["creationSource"],
        "scheduledTaskId":id
    });
    if active_thread.is_none() || thread_mode == "new_thread" {
        dispatch["modelSelection"] = task["modelSelection"].clone();
        dispatch["runtimeMode"] = task["runtimeMode"].clone();
        dispatch["interactionMode"] = task["interactionMode"].clone();
    } else if thread_mode == "fresh_session" {
        dispatch["freshContext"] = Value::Bool(true);
    }
    orchestration::dispatch_orchestration_command(state, dispatch)
        .await
        .map_err(|error| dispatch_error(&error))?;
    Ok((thread_id, message_id))
}

async fn record_provider_outcome(
    state: Arc<AppState>,
    task_id: String,
    started_at: String,
    thread_id: String,
    message_id: String,
) {
    for _ in 0..(24 * 60 * 60 * 20) {
        let status = {
            let store = state.store.read().await;
            store.threads.get(&thread_id).and_then(|thread| {
                thread
                    .runs
                    .iter()
                    .find(|run| run.user_message_id.as_deref() == Some(message_id.as_str()))
                    .map(|run| run.status.clone())
            })
        };
        let Some(status) = status else {
            tokio::time::sleep(RUN_OUTCOME_POLL).await;
            continue;
        };
        if !matches!(
            status.as_str(),
            "completed" | "failed" | "cancelled" | "interrupted" | "rolled_back"
        ) {
            tokio::time::sleep(RUN_OUTCOME_POLL).await;
            continue;
        }
        if status != "completed" {
            let error = match status.as_str() {
                "failed" => "The run failed.",
                "interrupted" => "The run was stopped.",
                "cancelled" => "The run was cancelled.",
                "rolled_back" => "The run was rolled back.",
                _ => return,
            };
            let mut database = state.scheduled_database.lock().await;
            let _ = database.record_failure(&task_id, &started_at, error);
        } else {
            let mut database = state.scheduled_database.lock().await;
            if database
                .delete_completed_once(&task_id, &started_at)
                .unwrap_or(false)
            {
                let _ = state.scheduled_task_events.send(());
            }
        }
        return;
    }
}

/// Starts the process-owned scheduler. A change notification wakes it early,
/// while the one-second poll handles tasks written by the other server.
pub(crate) fn spawn(state: Arc<AppState>) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut changes = state.scheduled_task_events.subscribe();
        let now_ms = platform::now_ms();
        let now = auth_service::iso(now_ms);
        let recovered = {
            let mut database = state.scheduled_database.lock().await;
            let tasks = database.list().unwrap_or_default();
            let mut recovered = 0;
            for mut task in tasks {
                if task["lastRunStatus"] != "running" {
                    continue;
                }
                task["lastRunStatus"] = Value::String("failed".into());
                task["lastRunError"] =
                    Value::String("Run was interrupted by a server restart.".into());
                task["updatedAt"] = Value::String(now.clone());
                task["nextRunAt"] = next_run_at(&task["schedule"], now_ms)
                    .map(Value::String)
                    .unwrap_or(Value::Null);
                task["runCount"] = json!(task["runCount"].as_i64().unwrap_or(0) + 1);
                if database.save(&task).is_ok() {
                    recovered += 1;
                }
            }
            recovered
        };
        if recovered > 0 {
            let _ = state.scheduled_task_events.send(());
        }
        loop {
            if state.stopping.load(std::sync::atomic::Ordering::SeqCst) {
                break;
            }
            run_due_at(state.clone(), platform::now_ms()).await;
            tokio::select! {
                _ = tokio::time::sleep(POLL_INTERVAL) => {},
                _ = changes.recv() => {},
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn next_occurrence_supports_intervals_once_and_fixed_times() {
        let now = parse_iso("2026-11-22T10:00:00.000Z").unwrap();
        assert_eq!(
            next_run_at(&json!({"type":"interval","everyMs":60_000}), now),
            Some("2026-11-22T10:01:00.000Z".into())
        );
        assert_eq!(
            next_run_at(
                &json!({"type":"once","runAt":"2026-11-22T09:00:00.000Z"}),
                now,
            ),
            None
        );
        assert_eq!(
            next_run_at(
                &json!({"type":"fixed_time","timeOfDay":"10:30","weekdays":[0,1,2,3,4,5,6]}),
                now,
            ),
            Some("2026-11-22T10:30:00.000Z".into())
        );
    }

    #[test]
    fn one_time_run_uses_its_start_when_computing_next_run() {
        let started = parse_iso("2026-11-22T10:00:00.000Z").unwrap();
        let completed = parse_iso("2026-11-22T10:01:00.000Z").unwrap();
        let task = json!({
            "enabled": true,
            "schedule": {"type":"once","runAt":"2026-11-22T10:00:30.000Z"}
        });
        assert_eq!(
            next_run_after_run(&task, started, completed),
            Some("2026-11-22T10:00:30.000Z".into())
        );
    }
}
