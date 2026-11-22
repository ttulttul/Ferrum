//! The orchestration event model and its in-memory projection. Platform
//! neutral: compiled for the native server and the workers control plane.
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use uuid::Uuid;

/// Seconds since the epoch, from the platform clock.
pub(crate) fn now() -> u64 {
    crate::platform::now_secs()
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Project {
    pub id: String,
    pub name: String,
    pub path: String,
    pub created_at: u64,
}
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ConversationMessage {
    pub id: String,
    pub role: String,
    pub content: String,
    pub created_at: u64,
    /// Where a user's message came from (`creationSource`), when not the
    /// web client.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub creation_source: Option<String>,
    /// The scheduled task that sent the message (`scheduledTaskId`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scheduled_task_id: Option<String>,
    /// A scheduled delivery that named no model, so it runs on the thread's
    /// selection when it starts (`followsThreadSelection`).
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub follows_thread_selection: bool,
    /// What a server-sent notification says (`notification`: source,
    /// outcome, summary), for a message in the notification role.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub notification: Option<serde_json::Value>,
    /// A user's message that steered a running run: `{runId,
    /// afterProviderItems, nodeId, providerThreadId, providerTurnId}`, with
    /// the run's wire id and how many provider items it had then.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub steer: Option<Box<serde_json::Value>>,
    /// The message's own wire fields where they differ from what its role
    /// implies, such as an agent's `createdBy`, its `senderThreadId`, or a
    /// delegated task's `delegatedCompletion`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub wire: Option<Box<serde_json::Value>>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Run {
    pub id: Uuid,
    pub thread_id: String,
    pub provider: String,
    pub status: String,
    pub started_at: u64,
    pub finished_at: Option<u64>,
    pub error: Option<String>,
    #[serde(default)]
    pub user_message_id: Option<String>,
    #[serde(default)]
    pub queue_held: bool,
    /// How a server restart cut the run off, when one did.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub restart_cut: Option<RestartCut>,
    /// The run a restart cut off that this run continues.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub restart_continuation_of: Option<Uuid>,
    /// When a restart put the run back in the queue, because its provider
    /// never took its prompt.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub requeued_at: Option<u64>,
    /// Background work a restart cut off that the run's next turn is told
    /// about (`restartCancelledBackgroundWork`).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub background_work: Vec<serde_json::Value>,
    /// A restart continuation is owed to this settled run, with the lost
    /// processes it counts (the reference's `processLossCount`). A run a
    /// restart cut off keeps it in [`RestartCut::continuation`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub continuation: Option<u32>,
    /// A restart owes the run its prompt again: nobody reviews a scheduled
    /// prompt its provider never took, so it is sent again rather than held
    /// (the reference's `provider-turn.start` restart resend).
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub resend_owed: bool,
    /// How many times a restart sent the run's prompt again.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub resends: u32,
    /// The run's id on the wire and in the shared database, when it is not
    /// [`Run::id`]: a run read from `statev2.sqlite`, or a new run on a thread
    /// read from there, has the reference's `run:thread:…:ordinal:…` id.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub wire_id: Option<String>,
}

fn is_zero(value: &u32) -> bool {
    *value == 0
}

impl Run {
    /// The run's id as clients and the shared database know it.
    pub(crate) fn wire_id(&self) -> String {
        self.wire_id.clone().unwrap_or_else(|| self.id.to_string())
    }

    /// The internal id for a run known by `wire_id`: stable, so a run read
    /// from the shared database keeps its id across restarts.
    pub(crate) fn uuid_for(wire_id: &str) -> Uuid {
        if let Ok(id) = Uuid::parse_str(wire_id) {
            return id;
        }
        use sha2::Digest;
        let digest = sha2::Sha256::digest(wire_id.as_bytes());
        let mut bytes = [0u8; 16];
        bytes.copy_from_slice(&digest[..16]);
        uuid::Builder::from_custom_bytes(bytes).into_uuid()
    }

    /// The reference's id for the `ordinal`th run of `thread_id`.
    pub(crate) fn reference_id(thread_id: &str, ordinal: usize) -> String {
        format!(
            "run:thread:{}:ordinal:{ordinal}",
            crate::thread_projection::uri_component(thread_id)
        )
    }
    /// The continuation a restart owes the run, with its lost processes.
    pub(crate) fn owed_continuation(&self) -> Option<u32> {
        self.continuation
            .or_else(|| self.restart_cut.as_ref()?.continuation)
    }
}

/// A run that a server restart cut off, as the reference's
/// ProviderRuntimeRecoveryService reconciles it.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct RestartCut {
    /// The reconcile's trigger: `shutdown` (the server stopped) or `startup`
    /// (it started again after stopping without one).
    pub trigger: String,
    /// When it was cut off (seconds).
    pub at: u64,
    /// When the run's turn should continue after the restart: how many
    /// processes in a row ended under it (the reference's
    /// `processLossCount`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub continuation: Option<u32>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CheckpointRecord {
    pub turn_count: u64,
    pub tree: String,
    pub cwd: String,
    pub captured_at: u64,
    #[serde(default)]
    pub ready: bool,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Thread {
    #[serde(default)]
    pub(crate) row: serde_json::Value,
    pub id: String,
    pub project_id: String,
    pub title: String,
    pub cwd: String,
    pub status: String,
    pub messages: Vec<ConversationMessage>,
    pub runs: Vec<Run>,
    pub created_at: u64,
    pub archived: bool,
    pub settled: bool,
    pub snoozed_until: Option<String>,
    pub pinned: bool,
    pub unread: bool,
    pub tags: Vec<String>,
    #[serde(default)]
    pub checkpoints: Vec<CheckpointRecord>,
    /// The cards in which the thread's agent asked the user to link another
    /// machine (`link_request` turn items), as last recorded, in the order
    /// they were first asked. Each holds every field but its ordinal.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub link_requests: Vec<serde_json::Value>,
    /// What provider turns left behind; rebuilt from the log.
    #[serde(skip)]
    pub(crate) provider: Box<crate::provider_state::ProviderState>,
    /// The thread's projection as `statev2.sqlite` holds it, when it was read
    /// from there rather than built by this server's events
    /// (docs/shared-store.md).
    #[serde(skip)]
    pub(crate) stored: Option<Box<serde_json::Value>>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", content = "data")]
pub enum Event {
    ProjectCreated(Project),
    ProjectCommitted {
        project: Project,
        row: serde_json::Value,
        command_id: String,
        command_type: String,
        removed_threads: Vec<String>,
    },
    ThreadCreated(Thread),
    /// A link card asked or answered (`link_request.record`): it replaces
    /// the card with its id, or is added after the others.
    LinkRequestRecorded {
        thread_id: String,
        card: serde_json::Value,
    },
    MessageAdded {
        thread_id: String,
        message: ConversationMessage,
    },
    MessageEdited {
        thread_id: String,
        message_id: String,
        content: String,
    },
    RunReordered {
        thread_id: String,
        run_id: Uuid,
        before_run_id: Option<Uuid>,
    },
    RunStarted(Run),
    RunUpdated(Run),
    RunQueued {
        message: ConversationMessage,
        run: Run,
    },
    RunOutput {
        run_id: Uuid,
        thread_id: String,
        stream: String,
        text: String,
        /// Where the output sits in the workspace's stream for the turn,
        /// when the workspace tracks positions. A restarted server resumes
        /// the turn after the last one recorded.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        position: Option<crate::workspace::StreamPosition>,
    },
    /// A provider turn's event, as the workspace reported it.
    RunProvider {
        run_id: Uuid,
        thread_id: String,
        event: crate::workspace::ProviderEvent,
        /// As [`Event::RunOutput`]'s.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        position: Option<crate::workspace::StreamPosition>,
    },
    RunFinished {
        run_id: Uuid,
        thread_id: String,
        status: String,
        error: Option<String>,
    },
    CheckpointCaptured {
        thread_id: String,
        checkpoint: CheckpointRecord,
    },
    CommandDispatched {
        command_id: String,
        command_type: String,
        thread_id: Option<String>,
        payload: serde_json::Value,
    },
    ThreadPatched {
        thread_id: String,
        patch: serde_json::Value,
    },
    ThreadDeleted {
        thread_id: String,
    },
    /// A server restart's reconciliation of a thread's provider runtime: the
    /// running runs it cut off end cancelled, the queued ones wait to be
    /// sent, and the provider session stops.
    /// The one-time import of this server's own threads and projects into
    /// the shared database (docs/shared-store.md, 2.3): they are written
    /// there whole. Nothing changes here but the commit they were last
    /// written at.
    Imported {
        thread_ids: Vec<String>,
        project_ids: Vec<String>,
    },
    /// A thread change detached the thread's provider session, stopping
    /// `run_ids` mid-turn (the reference's `provider-session.detached`).
    ProviderSessionDetached {
        thread_id: String,
        run_ids: Vec<Uuid>,
        reason: String,
    },
    ProviderRuntimeReconciled {
        thread_id: String,
        cut: Vec<(Uuid, RestartCut)>,
        #[serde(default)]
        held: Vec<Uuid>,
        /// Runs whose provider never took the prompt, back in the queue.
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        requeued: Vec<Uuid>,
        /// Runs whose provider never took a scheduled prompt, to be sent
        /// again once the server is up.
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        resent: Vec<Uuid>,
        /// Items of earlier turns still running in the background (run,
        /// native item id), cancelled.
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        stale_items: Vec<(Uuid, String)>,
        /// The background work cancelled, on the run whose next turn is told.
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        background: Vec<(Uuid, Vec<serde_json::Value>)>,
        /// A settled run owed a continuation, with its lost processes.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        continued: Option<(Uuid, u32)>,
    },
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ServerEvent {
    pub id: u64,
    pub at: u64,
    pub event: Event,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) receipt: Option<CommandReceipt>,
    /// The domain events a live thread subscriber gets for this commit. The
    /// commit takes one sequence number per event, ending at `id`. Not
    /// logged: a subscriber that resumes later gets a snapshot.
    #[serde(skip)]
    pub(crate) thread_events: Option<std::sync::Arc<crate::thread_events::ThreadEvents>>,
}

impl Event {
    /// The thread whose projection this event can change.
    pub(crate) fn thread_id(&self) -> Option<&str> {
        match self {
            Event::ThreadCreated(thread) => Some(&thread.id),
            Event::MessageAdded { thread_id, .. }
            | Event::MessageEdited { thread_id, .. }
            | Event::RunReordered { thread_id, .. }
            | Event::RunProvider { thread_id, .. }
            | Event::RunFinished { thread_id, .. }
            | Event::CheckpointCaptured { thread_id, .. }
            | Event::ThreadPatched { thread_id, .. }
            | Event::LinkRequestRecorded { thread_id, .. }
            | Event::ProviderRuntimeReconciled { thread_id, .. }
            | Event::ProviderSessionDetached { thread_id, .. } => Some(thread_id),
            Event::RunStarted(run) | Event::RunUpdated(run) | Event::RunQueued { run, .. } => {
                Some(&run.thread_id)
            }
            Event::CommandDispatched { thread_id, .. } => thread_id.as_deref(),
            // Output lines are not part of the projection.
            _ => None,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct CommandReceipt {
    pub(crate) command_id: String,
    pub(crate) thread_id: String,
    /// The command's `type`, for the shared database's receipt row.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub(crate) command_type: String,
}

#[derive(Default)]
pub struct Store {
    pub projects: HashMap<String, Project>,
    pub(crate) project_rows: indexmap::IndexMap<String, serde_json::Value>,
    /// Projects read from `statev2.sqlite` rather than committed here. The
    /// reference already folded their settings into `settings.json` when it
    /// migrated, so `server.getSettings` does not fold them again.
    pub(crate) stored_project_ids: std::collections::HashSet<String>,
    /// The sequence of the last commit that touched each thread and project,
    /// to tell at start-up whether the shared database changed it since.
    pub(crate) thread_sequences: HashMap<String, u64>,
    pub(crate) project_sequences: HashMap<String, u64>,
    pub(crate) project_receipts: HashMap<String, (String, String)>,
    pub(crate) deleted_thread_ids: std::collections::HashSet<String>,
    pub(crate) command_receipts: HashMap<String, (String, u64)>,
    pub threads: HashMap<String, Thread>,
    /// The last recorded workspace stream position of each running run.
    pub(crate) turn_positions: HashMap<Uuid, crate::workspace::StreamPosition>,
}
impl Store {
    /// The project rows whose fields fold into the settings clients read:
    /// those committed here. The reference already folded the ones read from
    /// `statev2.sqlite` into `settings.json` when it migrated its settings.
    pub(crate) fn settings_project_rows(&self) -> indexmap::IndexMap<String, serde_json::Value> {
        self.project_rows
            .iter()
            .filter(|(id, _)| !self.stored_project_ids.contains(*id))
            .map(|(id, row)| (id.clone(), row.clone()))
            .collect()
    }

    pub(crate) fn apply_record(&mut self, record: &ServerEvent) {
        match &record.event {
            Event::ProjectCreated(project) => {
                self.project_sequences.insert(project.id.clone(), record.id);
            }
            Event::ProjectCommitted {
                project,
                removed_threads,
                ..
            } => {
                self.project_sequences.insert(project.id.clone(), record.id);
                for id in removed_threads {
                    self.thread_sequences.insert(id.clone(), record.id);
                }
            }
            Event::ThreadDeleted { thread_id } | Event::RunOutput { thread_id, .. } => {
                self.thread_sequences.insert(thread_id.clone(), record.id);
            }
            Event::Imported {
                thread_ids,
                project_ids,
            } => {
                for id in thread_ids {
                    self.thread_sequences.insert(id.clone(), record.id);
                }
                for id in project_ids {
                    self.project_sequences.insert(id.clone(), record.id);
                }
            }
            event => {
                if let Some(id) = event.thread_id() {
                    self.thread_sequences.insert(id.to_owned(), record.id);
                }
            }
        }
        self.apply_at(&record.event, record.at);
        if let Some(receipt) = &record.receipt {
            self.command_receipts.insert(
                receipt.command_id.clone(),
                (receipt.thread_id.clone(), record.id),
            );
        }
    }

    /// Apply `event`, committed at `at` (seconds).
    fn apply_at(&mut self, event: &Event, at: u64) {
        match event {
            Event::ProjectCreated(p) => {
                self.project_rows.insert(p.id.clone(), wire_project(p));
                self.projects.insert(p.id.clone(), p.clone());
            }
            Event::ProjectCommitted {
                project,
                row,
                command_id,
                command_type,
                removed_threads,
            } => {
                self.project_rows.insert(project.id.clone(), row.clone());
                self.project_receipts.insert(
                    command_id.clone(),
                    (project.id.clone(), command_type.clone()),
                );
                if row["deletedAt"].is_null() {
                    self.projects.insert(project.id.clone(), project.clone());
                } else {
                    self.projects.remove(&project.id);
                }
                for id in removed_threads {
                    self.threads.remove(id);
                    self.deleted_thread_ids.insert(id.clone());
                }
            }
            Event::ThreadCreated(t) => {
                self.threads.insert(t.id.clone(), t.clone());
            }
            Event::MessageAdded { thread_id, message } => {
                if let Some(t) = self.threads.get_mut(thread_id) {
                    t.messages.push(message.clone());
                }
            }
            Event::LinkRequestRecorded { thread_id, card } => {
                if let Some(t) = self.threads.get_mut(thread_id) {
                    match t.link_requests.iter_mut().find(|c| c["id"] == card["id"]) {
                        Some(existing) => *existing = card.clone(),
                        None => t.link_requests.push(card.clone()),
                    }
                }
            }
            Event::MessageEdited {
                thread_id,
                message_id,
                content,
            } => {
                if let Some(t) = self.threads.get_mut(thread_id) {
                    if let Some(message) = t.messages.iter_mut().find(|m| m.id == *message_id) {
                        message.content = content.clone();
                    }
                }
            }
            Event::RunQueued { message, run } => {
                if let Some(thread) = self.threads.get_mut(&run.thread_id) {
                    thread.messages.push(message.clone());
                    thread.status = "running".into();
                    thread.runs.push(run.clone());
                }
            }
            Event::RunStarted(run) => {
                if let Some(t) = self.threads.get_mut(&run.thread_id) {
                    t.status = "running".into();
                    if let Some(existing) =
                        t.runs.iter_mut().find(|candidate| candidate.id == run.id)
                    {
                        *existing = run.clone();
                    } else {
                        t.runs.push(run.clone());
                    }
                }
            }
            Event::RunUpdated(run) => {
                if let Some(t) = self.threads.get_mut(&run.thread_id) {
                    if let Some(existing) =
                        t.runs.iter_mut().find(|candidate| candidate.id == run.id)
                    {
                        *existing = run.clone();
                    } else {
                        t.runs.push(run.clone());
                    }
                    t.status = match run.status.as_str() {
                        "completed" => "idle".into(),
                        "queued" => "running".into(),
                        status => status.into(),
                    };
                }
            }
            Event::RunReordered {
                thread_id,
                run_id,
                before_run_id,
            } => {
                if let Some(t) = self.threads.get_mut(thread_id) {
                    let Some(index) = t.runs.iter().position(|run| run.id == *run_id) else {
                        return;
                    };
                    let run = t.runs.remove(index);
                    let target = before_run_id
                        .and_then(|id| t.runs.iter().position(|candidate| candidate.id == id))
                        .unwrap_or(t.runs.len());
                    t.runs.insert(target, run);
                }
            }
            Event::RunOutput {
                run_id, position, ..
            } => {
                if let Some(position) = position {
                    self.turn_positions.insert(*run_id, *position);
                }
            }
            Event::RunProvider {
                run_id,
                thread_id,
                event,
                position,
            } => {
                if let Some(position) = position {
                    self.turn_positions.insert(*run_id, *position);
                }
                if let Some(thread) = self.threads.get_mut(thread_id) {
                    thread.provider.apply(*run_id, event, at);
                    if let crate::workspace::ProviderEvent::ProviderThread { driver, .. } = event {
                        // As the turn starts (its session opening), not
                        // when the CLI reports its session later on.
                        if driver == crate::workspace::provider::CLAUDE_AGENT
                            && thread.provider.session_title_run != Some(*run_id)
                        {
                            thread.provider.session_title = Some(thread.title.clone());
                            thread.provider.session_title_run = Some(*run_id);
                        }
                    }
                }
            }
            Event::RunFinished {
                run_id,
                thread_id,
                status,
                error,
            } => {
                self.turn_positions.remove(run_id);
                if let Some(t) = self.threads.get_mut(thread_id) {
                    t.status = if status == "completed" {
                        "idle".into()
                    } else {
                        status.clone()
                    };
                    if let Some(run) = t.runs.iter_mut().find(|r| r.id == *run_id) {
                        run.status = status.clone();
                        run.error = error.clone();
                        run.finished_at = Some(at);
                    }
                }
            }
            Event::CheckpointCaptured {
                thread_id,
                checkpoint,
            } => {
                if let Some(thread) = self.threads.get_mut(thread_id) {
                    if !thread.checkpoints.iter().any(|candidate| {
                        candidate.turn_count == checkpoint.turn_count
                            && candidate.tree == checkpoint.tree
                    }) {
                        thread.checkpoints.push(checkpoint.clone());
                        thread
                            .checkpoints
                            .sort_by_key(|candidate| candidate.turn_count);
                    }
                }
            }
            Event::CommandDispatched { .. } => {}
            Event::ThreadPatched { thread_id, patch } => {
                if let Some(thread) = self.threads.get_mut(thread_id) {
                    if let Some(fields) = patch["row"].as_object() {
                        let mut row = thread.stored_row();
                        row.as_object_mut().unwrap().extend(fields.clone());
                        thread.row = row;
                    }
                    if let Some(value) = patch.get("title").and_then(serde_json::Value::as_str) {
                        thread.title = value.to_owned();
                    }
                    if let Some(value) = patch.get("project_id").and_then(serde_json::Value::as_str)
                    {
                        thread.project_id = value.to_owned();
                    }
                    if let Some(value) = patch.get("archived").and_then(serde_json::Value::as_bool)
                    {
                        thread.archived = value;
                    }
                    if let Some(value) = patch.get("settled").and_then(serde_json::Value::as_bool) {
                        thread.settled = value;
                    }
                    if let Some(value) = patch.get("pinned").and_then(serde_json::Value::as_bool) {
                        thread.pinned = value;
                    }
                    if let Some(value) = patch.get("unread").and_then(serde_json::Value::as_bool) {
                        thread.unread = value;
                    }
                    if let Some(value) = patch.get("status").and_then(serde_json::Value::as_str) {
                        thread.status = value.to_owned();
                    }
                    if let Some(value) = patch.get("snoozed_until") {
                        thread.snoozed_until = value.as_str().map(str::to_owned);
                    }
                    if let Some(values) = patch.get("tags").and_then(serde_json::Value::as_array) {
                        thread.tags = values
                            .iter()
                            .filter_map(serde_json::Value::as_str)
                            .map(str::to_owned)
                            .collect();
                    }
                }
            }
            Event::ThreadDeleted { thread_id } => {
                self.threads.remove(thread_id);
                self.deleted_thread_ids.insert(thread_id.clone());
            }
            Event::Imported { .. } => {}
            Event::ProviderSessionDetached {
                thread_id, run_ids, ..
            } => {
                if let Some(thread) = self.threads.get_mut(thread_id) {
                    thread.provider.detach_session(at, run_ids);
                }
            }
            Event::ProviderRuntimeReconciled {
                thread_id,
                cut,
                held,
                requeued,
                resent,
                stale_items,
                background,
                continued,
            } => {
                for (run_id, _) in cut {
                    self.turn_positions.remove(run_id);
                }
                if let Some(thread) = self.threads.get_mut(thread_id) {
                    for (run_id, restart) in cut {
                        if let Some(run) = thread.runs.iter_mut().find(|r| r.id == *run_id) {
                            run.status = "cancelled".into();
                            run.finished_at = Some(restart.at);
                            run.restart_cut = Some(restart.clone());
                        }
                    }
                    for run in thread.runs.iter_mut().filter(|r| held.contains(&r.id)) {
                        run.queue_held = true;
                    }
                    for run in thread.runs.iter_mut().filter(|r| requeued.contains(&r.id)) {
                        run.status = "queued".into();
                        run.queue_held = true;
                        run.requeued_at = Some(at);
                        run.finished_at = None;
                    }
                    for run in thread.runs.iter_mut().filter(|r| resent.contains(&r.id)) {
                        run.resend_owed = true;
                    }
                    // A blocking wait dies with its turn: an app-owned
                    // subagent the cut run waited on wakes its parent's
                    // continuation when it finishes, as an async task.
                    let cut_ids: Vec<String> = cut.iter().map(|(id, _)| id.to_string()).collect();
                    if let Some(tasks) = thread.row["subagents"].as_array_mut() {
                        for task in tasks.iter_mut().filter(|task| {
                            task["origin"] == "app_owned"
                                && task["completionWake"] == "settled_only"
                                && task["runId"]
                                    .as_str()
                                    .is_some_and(|id| cut_ids.iter().any(|c| c == id))
                                && matches!(
                                    task["status"].as_str(),
                                    Some("pending" | "running" | "waiting")
                                )
                        }) {
                            task["completionWake"] = serde_json::json!("always");
                            task["updatedAt"] =
                                serde_json::json!(crate::auth_service::iso(at as i64 * 1000));
                        }
                    }
                    for (run, item) in stale_items {
                        thread
                            .provider
                            .cancelled_items
                            .insert((*run, item.clone()), at);
                    }
                    for (run_id, work) in background {
                        if let Some(run) = thread.runs.iter_mut().find(|r| r.id == *run_id) {
                            run.background_work =
                                crate::runtime_recovery::merge_work(&run.background_work, work);
                        }
                    }
                    if let Some((run_id, losses)) = continued {
                        if let Some(run) = thread.runs.iter_mut().find(|r| r.id == *run_id) {
                            run.continuation = Some(*losses);
                        }
                    }
                    if !cut.is_empty() {
                        thread.status = "cancelled".into();
                    }
                    thread.provider.stop_session(at);
                }
            }
        }
    }
}

pub(crate) fn iso(time: time::OffsetDateTime) -> String {
    time.format(time::macros::format_description!(
        "[year]-[month]-[day]T[hour]:[minute]:[second].[subsecond digits:3]Z"
    ))
    .expect("valid project timestamp")
}
pub(crate) fn wire_project(p: &Project) -> serde_json::Value {
    let at = iso(
        time::OffsetDateTime::from_unix_timestamp(p.created_at as i64)
            .unwrap_or(time::OffsetDateTime::UNIX_EPOCH),
    );
    serde_json::json!({"id":p.id,"title":p.name,"hasCustomTitle":false,"workspaceRoot":p.path,"repositoryIdentity":null,"faviconPath":null,"projectIcon":null,"defaultModelSelection":null,"defaultThreadEnvMode":null,"autoPull":false,"scripts":[],"pinnedAt":null,"archivedAt":null,"createdAt":at,"updatedAt":at,"deletedAt":null})
}

/// `orchestration.getOlderSettledThreads`: settled threads older than
/// `params.before`, newest first, with each project's count, as of `sequence`.
pub(crate) fn older_settled_threads(
    store: &Store,
    sequence: u64,
    params: &serde_json::Value,
) -> serde_json::Value {
    use serde_json::json;
    let before = params["before"]["settledAt"].as_str().unwrap_or_default();
    let after_id = params["before"]["threadId"].as_str();
    let project_ids = params["projectIds"].as_array();
    let in_projects = |thread: &&crate::Thread| {
        project_ids.is_none_or(|ids| {
            ids.iter()
                .any(|id| id.as_str() == Some(thread.project_id.as_str()))
        })
    };
    let mut older: Vec<&crate::Thread> = store
        .threads
        .values()
        .filter(|thread| {
            thread.settled
                && !thread.archived
                && in_projects(thread)
                && thread.row["settledAt"].as_str().is_some_and(|at| {
                    at < before
                        || (at == before && after_id.is_some_and(|id| thread.id.as_str() > id))
                })
        })
        .collect();
    older.sort_by(|a, b| {
        b.row["settledAt"]
            .as_str()
            .unwrap_or_default()
            .cmp(a.row["settledAt"].as_str().unwrap_or_default())
            .then_with(|| a.id.cmp(&b.id))
    });
    let mut counts = std::collections::BTreeMap::<String, usize>::new();
    for thread in &older {
        *counts.entry(thread.project_id.clone()).or_default() += 1;
    }
    let limit = params["limit"].as_i64().unwrap_or(0).clamp(0, 50) as usize;
    let threads = if limit == 0 {
        Vec::new()
    } else {
        older.into_iter().take(limit).map(|t| t.shell()).collect()
    };
    json!({"snapshotSequence":sequence,"threads":threads,"counts":counts.into_iter().map(|(project_id,count)|json!({"projectId":project_id,"count":count})).collect::<Vec<_>>()})
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn a_finished_run_ends_when_its_record_was_committed() {
        // Live subscribers are sent what a commit changes, worked out on a
        // copy of the thread before the store applies it; and a restart
        // replays the log. Both need the record alone to decide the run's
        // end, not the clock when it is applied.
        let run_id = Uuid::new_v4();
        let mut store = Store::default();
        store.threads.insert(
            "t1".into(),
            Thread {
                row: json!({}),
                id: "t1".into(),
                project_id: "p1".into(),
                title: "T".into(),
                cwd: "/w".into(),
                status: "running".into(),
                messages: vec![],
                runs: vec![serde_json::from_value(json!({
                    "id": run_id,
                    "thread_id": "t1",
                    "provider": "codex",
                    "status": "running",
                    "started_at": 1,
                    "finished_at": null,
                    "error": null,
                }))
                .expect("a run")],
                created_at: 1,
                archived: false,
                settled: false,
                snoozed_until: None,
                pinned: false,
                unread: false,
                tags: vec![],
                checkpoints: vec![],
                link_requests: Vec::new(),
                provider: Box::default(),
                stored: None,
            },
        );
        store.apply_record(&ServerEvent {
            id: 1,
            at: 7,
            event: Event::RunFinished {
                run_id,
                thread_id: "t1".into(),
                status: "completed".into(),
                error: None,
            },
            receipt: None,
            thread_events: None,
        });
        assert_eq!(store.threads["t1"].runs[0].finished_at, Some(7));
    }

    #[test]
    fn a_claude_session_keeps_the_title_its_turn_started_with() {
        // The reference names a Claude session as its turn starts; a title
        // generated meanwhile reaches the session only on its next turn.
        let run_id = Uuid::new_v4();
        let mut store = Store::default();
        store.threads.insert(
            "t1".into(),
            serde_json::from_value(json!({
                "id": "t1", "project_id": "p1", "title": "Seed", "cwd": "/w",
                "status": "running", "messages": [], "runs": [], "created_at": 1,
                "archived": false, "settled": false, "snoozed_until": null,
                "pinned": false, "unread": false, "tags": []
            }))
            .expect("a thread"),
        );
        let thread_event = |at| ServerEvent {
            id: at,
            at,
            event: Event::RunProvider {
                run_id,
                thread_id: "t1".into(),
                event: crate::workspace::ProviderEvent::ProviderThread {
                    driver: crate::workspace::provider::CLAUDE_AGENT.into(),
                    native_thread_id: "s1".into(),
                    resumed: false,
                    model: None,
                },
                position: None,
            },
            receipt: None,
            thread_events: None,
        };
        store.apply_record(&thread_event(1));
        store.threads.get_mut("t1").unwrap().title = "Generated".into();
        // The CLI reports its session again once it is up.
        store.apply_record(&thread_event(2));
        assert_eq!(
            store.threads["t1"].provider.session_title.as_deref(),
            Some("Seed")
        );
    }
}
