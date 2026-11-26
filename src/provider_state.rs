//! What a thread's provider turns leave behind in the store, rebuilt from
//! the recorded [`Event::RunProvider`](crate::Event::RunProvider) events:
//! the native thread (Codex) or session (Claude) the next turn resumes, and
//! each run's items, plan, usage and end, which `thread_projection` turns
//! into the reference's projection records.
use crate::workspace::provider::{self, ProviderTurn};
use crate::workspace::provider::{
    OpenCode2Thread, PlanStep, ProviderEvent, ProviderFailure, ProviderItem, RuntimeRequestOpen,
    ThreadDisposition, TokenUsage, TurnStatus,
};
use serde_json::Value;
use std::collections::HashMap;
use uuid::Uuid;

/// A thread's provider state. Not serialized: the store rebuilds it from
/// the event log.
#[derive(Clone, Debug, Default)]
pub struct ProviderState {
    /// The native thread the next turn resumes, if it may.
    pub native_thread: Option<NativeThread>,
    /// The thread's provider thread record, once a provider turn ran: the
    /// one its latest provider turn ran on.
    pub thread: Option<ThreadRecord>,
    /// The provider threads of the other provider instances the thread ran
    /// on before switching (the reference keeps one per instance), each with
    /// its native thread, which a switch back resumes.
    pub other_threads: Vec<ThreadRecord>,
    /// The provider thread each run's turn ran on, by the run that opened it
    /// (its record's `first_run`).
    pub run_threads: HashMap<Uuid, Uuid>,
    /// Each run's provider turn, by run.
    pub runs: HashMap<Uuid, RunRecord>,
    /// How each run's provider turn ended, from its `turn.terminal`.
    pub terminals: HashMap<Uuid, Terminal>,
    /// What the native session's prompts used of its attachment budget
    /// (bytes, pages), once a prompt reached it.
    pub attachment_budget: Option<(u64, u64)>,
    /// The title the thread had when its Claude session last started a
    /// turn: the session's own title, which the reference names it with
    /// then (and renames only while the session runs). A title the thread
    /// gets later in the turn (a generated one) does not change it.
    pub session_title: Option<String>,
    /// The run whose start recorded [`Self::session_title`].
    pub session_title_run: Option<Uuid>,
    /// When a server restart stopped the provider session, until a turn
    /// starts it again.
    pub session_stopped_at: Option<u64>,
    /// Items a restart cancelled while they ran after their turn (run,
    /// native item id), with when.
    pub cancelled_items: HashMap<(Uuid, String), u64>,
    /// When a thread change (archive, a new workspace, runtime mode or
    /// provider) detached the provider session, until a later turn opens
    /// one: the reference's `provider-session.detached` drops the session's
    /// binding to the thread.
    pub session_detached_at: Option<u64>,
    /// Runs a detach stopped mid-turn: the provider, not the user, ended
    /// them.
    pub detached_runs: std::collections::HashSet<Uuid>,
    /// A fresh context detached the session: the next turn starts a new
    /// native session rather than resuming one, until a turn reports it.
    pub fresh_session: bool,
    /// For a thread read from the shared database: the provider turns its
    /// provider thread already ran and the ordinal of the first run on it,
    /// so the turns this server adds continue that numbering.
    pub prior_turns: Option<(usize, usize)>,
    /// For a thread read from the shared database: the id of the provider
    /// session it ran in, and the token usage (`usedTokens`, `maxTokens`) of
    /// the last turn that ran there on its native thread.
    pub prior_session_id: Option<String>,
    pub prior_usage: Option<(Option<String>, u64, Option<u64>)>,
    /// When a stop or interrupt asked each run to end, by run, and how many
    /// items its turn had reported then (where the request's card goes).
    pub interrupt_requests: HashMap<Uuid, (u64, usize)>,
    /// Why each of those runs was asked to end: the stop's reason, or
    /// "Stopped by user" (`USER_STOP_REASON`).
    pub interrupt_reasons: HashMap<Uuid, String>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Terminal {
    pub status: TurnStatus,
    pub failure: Option<ProviderFailure>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NativeThread {
    pub driver: String,
    pub id: String,
}

/// The provider thread record: named after the run that first used it, as
/// the reference names a new thread's record before the native thread is
/// known.
#[derive(Clone, Debug)]
pub struct ThreadRecord {
    pub driver: String,
    pub first_run: Uuid,
    pub created_at: u64,
    pub updated_at: u64,
    pub native_id: Option<String>,
    /// `not_loaded`, `active`, `idle` or `error`.
    pub status: &'static str,
    /// The conversation's last entry when its last turn ended (Pi).
    pub conversation_head: Option<String>,
    /// The background tasks running now (`pendingBackgroundTasks`), each with
    /// the run it first appeared under (`attributePendingBackgroundTasks`).
    pub background_tasks: Vec<(Value, Uuid)>,
    /// The OpenCode 2 server its turns ran on, as the last one reported it.
    pub opencode2: Option<OpenCode2Thread>,
    /// The title the agent last gave its session (Kiro's focus).
    pub native_title: Option<String>,
    /// What its native session's prompts used of its attachment budget, kept
    /// while the thread runs on another instance's provider thread.
    pub attachment_budget: Option<(u64, u64)>,
    /// When the agent says it last changed its session (fx).
    pub native_updated_at: Option<String>,
    /// The context usage the agent last reported for its session (fx).
    pub context_usage: Option<provider::ContextUsage>,
}

/// The adapter's latest report of the session and turn it belongs to.
#[derive(Clone, Debug)]
pub struct SessionRecord {
    pub report: provider::SessionReport,
    pub turn_status: TurnStatus,
    pub updated_at: u64,
}

/// One run's provider turn.
#[derive(Clone, Debug)]
pub struct RunRecord {
    pub driver: String,
    /// The native turn the provider named at the turn's end (Pi): a ref, or
    /// `null` when the turn wrote no entry.
    pub native_turn_ref: Option<Value>,
    pub started_at: u64,
    /// The CLI started the turn (`provider_turn.updated`), so the run has a
    /// provider turn record.
    pub turn_started: bool,
    pub session: Option<SessionRecord>,
    pub native_turn_id: Option<String>,
    /// The native thread the turn ran on.
    pub native_thread_id: Option<String>,
    /// The turn resumed the native thread it ran on.
    pub resumed: bool,
    pub usage: Option<(TokenUsage, u64)>,
    /// The whole turn's usage (`turnTokenUsage`).
    pub turn_usage: Option<Value>,
    /// The model the turn ran on.
    pub model: Option<String>,
    /// For Claude, the model the CLI reported, which the turn's record shows
    /// from its next usage update or its end on, as the reference's
    /// provider turn updates carry it (`executedModel`).
    pub reported_model: Option<String>,
    /// The CLI received the run's prompt.
    pub prompt_delivered: bool,
    /// Items in the order first seen.
    pub items: Vec<ItemRecord>,
    pub plan: Option<PlanRecord>,
    pub ended_at: Option<u64>,
    /// Runtime requests the CLI made, in order.
    pub requests: Vec<RequestRecord>,
}

/// A runtime request and what became of it.
#[derive(Clone, Debug)]
pub struct RequestRecord {
    pub request: RuntimeRequestOpen,
    pub created_at: u64,
    /// `pending`, `resolved` or `cancelled`.
    pub status: &'static str,
    pub resolved_at: Option<u64>,
    pub decision: Option<String>,
    pub feedback: Option<String>,
    pub answers: Option<Value>,
    /// When the answer could not be handed to the turn.
    pub delivery_failed_at: Option<u64>,
}

impl RequestRecord {
    /// Whether the request still waits for the user.
    pub fn pending(&self) -> bool {
        self.status == "pending"
    }
}

#[derive(Clone, Debug)]
pub struct ItemRecord {
    pub item: ProviderItem,
    pub first_at: u64,
    pub updated_at: u64,
    /// Streamed text recorded while still held, not shown until the item's
    /// next shown update.
    pub held_text: String,
}

#[derive(Clone, Debug)]
pub struct PlanRecord {
    pub steps: Vec<PlanStep>,
    pub explanation: Option<String>,
    pub first_at: u64,
    pub updated_at: u64,
}

impl ProviderState {
    /// Apply `run`'s provider `event`, recorded at `at` (milliseconds).
    pub(crate) fn apply(&mut self, run: Uuid, event: &ProviderEvent, at: u64) {
        if let ProviderEvent::TurnTerminal { closing, .. } = event {
            for event in closing {
                self.apply(run, event, at);
            }
        }
        // A turn after a restart runs in a session started again.
        self.session_stopped_at = None;
        // So does a turn after a detach, but not the detached turn ending.
        if !self.detached_runs.contains(&run) {
            self.session_detached_at = None;
        }
        let driver = match event {
            ProviderEvent::ProviderThread { driver, .. } => driver.clone(),
            _ => self
                .runs
                .get(&run)
                .map(|record| record.driver.clone())
                .or_else(|| self.thread.as_ref().map(|thread| thread.driver.clone()))
                .unwrap_or_else(|| provider::CODEX.into()),
        };
        // A native subagent's own turn is its child thread's (recorded there), nothing of the
        // run that reports it.
        if let ProviderEvent::TurnItem { item } = event {
            if item.kind == "subagent_turn"
                || item.fields.contains_key("subagentThread")
                || item.fields.contains_key("subagentChild")
            {
                return;
            }
        }
        // A native subagent an earlier run started, which a later run reports (it ended after
        // that run's turn), is that run's item (`subagent.updated` names its run).
        if let ProviderEvent::TurnItem { item } = event {
            if item.kind == "subagent" {
                let holder = self
                    .runs
                    .iter()
                    .find(|(id, record)| {
                        **id != run && record.items.iter().any(|known| known.item.id == item.id)
                    })
                    .map(|(id, _)| *id);
                let own = self.runs.get(&run).is_some_and(|record| {
                    record.items.iter().any(|known| known.item.id == item.id)
                });
                if let (Some(holder), false) = (holder, own) {
                    if let Some(known) = self
                        .runs
                        .get_mut(&holder)
                        .and_then(|record| record.items.iter_mut().find(|k| k.item.id == item.id))
                    {
                        known.item = item.clone();
                        known.updated_at = at;
                    }
                    return;
                }
            }
        }
        let record = self.runs.entry(run).or_insert_with(|| RunRecord {
            driver: driver.clone(),
            native_turn_ref: None,
            started_at: at,
            turn_started: false,
            session: None,
            native_turn_id: None,
            native_thread_id: None,
            resumed: false,
            usage: None,
            turn_usage: None,
            model: None,
            reported_model: None,
            prompt_delivered: false,
            items: Vec::new(),
            plan: None,
            ended_at: None,
            requests: Vec::new(),
        });
        match event {
            ProviderEvent::ProviderThread {
                driver,
                native_thread_id,
                resumed,
                opencode2,
                native_title,
                native_updated_at,
                context_usage,
                ..
            } => {
                // A turn on another provider instance moves to that
                // instance's provider thread, opening one the first time; the
                // one it leaves stays, idle.
                if self
                    .thread
                    .as_ref()
                    .is_some_and(|thread| thread.driver != *driver)
                {
                    if let Some(mut left) = self.thread.take() {
                        if left.status == "active" {
                            left.status = "idle";
                        }
                        left.attachment_budget = self.attachment_budget.take();
                        self.other_threads.push(left);
                    }
                    if let Some(at) = self
                        .other_threads
                        .iter()
                        .position(|thread| thread.driver == *driver)
                    {
                        let mut back = self.other_threads.remove(at);
                        self.attachment_budget = back.attachment_budget.take();
                        self.thread = Some(back);
                    }
                }
                // The fresh session a turn opens is the one later turns resume.
                if !self.detached_runs.contains(&run) {
                    self.fresh_session = false;
                }
                record.driver = driver.clone();
                record.native_thread_id = Some(native_thread_id.clone());
                record.resumed = *resumed;
                // A run that starts a new native session starts a new
                // attachment budget.
                if !resumed && !record.prompt_delivered {
                    self.attachment_budget = None;
                }
                self.native_thread = Some(NativeThread {
                    driver: driver.clone(),
                    id: native_thread_id.clone(),
                });
                let thread = self.thread.get_or_insert_with(|| ThreadRecord {
                    driver: driver.clone(),
                    first_run: run,
                    created_at: at,
                    updated_at: at,
                    native_id: None,
                    status: "active",
                    conversation_head: None,
                    background_tasks: Vec::new(),
                    opencode2: None,
                    native_title: None,
                    attachment_budget: None,
                    native_updated_at: None,
                    context_usage: None,
                });
                thread.native_id = Some(native_thread_id.clone());
                if opencode2.is_some() {
                    thread.opencode2 = opencode2.as_deref().cloned();
                }
                if native_title.is_some() {
                    thread.native_title = native_title.clone();
                }
                if native_updated_at.is_some() {
                    thread.native_updated_at = native_updated_at.as_deref().map(str::to_owned);
                }
                if context_usage.is_some() {
                    thread.context_usage = context_usage.as_deref().cloned();
                }
                thread.status = "active";
                thread.updated_at = at;
                self.run_threads.insert(run, thread.first_run);
            }
            ProviderEvent::ProviderTurn {
                native_turn_id,
                native_turn_ref,
                conversation_head,
                session,
                status,
                model,
                ..
            } => {
                if let Some(report) = session {
                    let mut update = (**report).clone();
                    if update.model.is_none() {
                        update.model = record
                            .session
                            .as_mut()
                            .and_then(|prev| prev.report.model.take());
                    }
                    if *status != TurnStatus::Running {
                        if let Some(thread) = self.thread.as_mut() {
                            thread.status = if update.status == provider::SessionStatus::Error {
                                "error"
                            } else {
                                "idle"
                            };
                            thread.updated_at = at;
                        }
                    }
                    record.session = Some(SessionRecord {
                        report: update,
                        turn_status: *status,
                        updated_at: at,
                    });
                }
                // These adapters report the active thread with turn admission.
                if !record.turn_started
                    && (record.driver == provider::PI || record.driver == provider::MUSE)
                {
                    if let Some(thread) = self.thread.as_mut() {
                        thread.updated_at = at;
                    }
                }
                record.turn_started = true;
                if native_turn_ref.is_some() {
                    record.native_turn_ref = native_turn_ref.clone();
                }
                if let (Some(head), Some(thread)) = (conversation_head, self.thread.as_mut()) {
                    thread.conversation_head = Some(head.clone());
                }
                if model.is_some() {
                    if record.driver == provider::CLAUDE_AGENT {
                        record.reported_model = model.clone();
                    } else {
                        record.model = model.clone();
                    }
                }
                if native_turn_id.is_some() {
                    record.native_turn_id = native_turn_id.clone();
                }
            }
            ProviderEvent::TurnItem { item } => {
                match record
                    .items
                    .iter_mut()
                    .find(|known| known.item.id == item.id)
                {
                    Some(known) => {
                        // A native subagent whose model Codex reported changes its record,
                        // not its item (`updateSubagentSelection`).
                        let without_selection = |item: &ProviderItem| {
                            let mut item = item.clone();
                            item.fields.remove("model");
                            item.fields.remove("reasoningEffort");
                            item
                        };
                        let selection_only = item.kind == "subagent"
                            && known.item != *item
                            && without_selection(&known.item) == without_selection(item);
                        known.item = item.clone();
                        if !selection_only {
                            known.updated_at = at;
                        }
                        // An item's end carries all its text.
                        if item.status != "running" {
                            known.held_text.clear();
                        }
                    }
                    None => record.items.push(ItemRecord {
                        item: item.clone(),
                        first_at: at,
                        updated_at: at,
                        held_text: String::new(),
                    }),
                }
            }
            ProviderEvent::TurnItemDelta {
                item_id,
                delta,
                held,
            } => {
                // Streamed text after an item's end changes nothing.
                if let Some(known) = record
                    .items
                    .iter_mut()
                    .find(|known| &known.item.id == item_id && known.item.status == "running")
                {
                    if *held {
                        known.held_text.push_str(delta);
                        return;
                    }
                    let text = known
                        .item
                        .fields
                        .get("text")
                        .and_then(Value::as_str)
                        .unwrap_or_default();
                    let text = format!("{text}{}{delta}", std::mem::take(&mut known.held_text));
                    known.item.fields.insert("text".into(), text.into());
                    known.item.fields.insert("streaming".into(), true.into());
                    known.updated_at = at;
                }
            }
            ProviderEvent::Plan { steps, explanation } => {
                let first_at = record.plan.as_ref().map_or(at, |plan| plan.first_at);
                record.plan = Some(PlanRecord {
                    steps: steps.clone(),
                    explanation: explanation.clone(),
                    first_at,
                    updated_at: at,
                });
            }
            ProviderEvent::Usage { usage } => {
                record.usage = Some((usage.clone(), at));
                if record.reported_model.is_some() {
                    record.model = record.reported_model.clone();
                }
            }
            ProviderEvent::TurnUsage { usage } => record.turn_usage = Some(usage.clone()),
            // A task keeps the run it first appeared under; a new one is this run's.
            ProviderEvent::BackgroundTasks { tasks } => {
                if let Some(thread) = self.thread.as_mut() {
                    let owners: HashMap<&Value, Uuid> = thread
                        .background_tasks
                        .iter()
                        .map(|(task, owner)| (&task["taskId"], *owner))
                        .collect();
                    let tasks: Vec<(Value, Uuid)> = tasks
                        .iter()
                        .map(|task| {
                            let owner = owners.get(&task["taskId"]).copied().unwrap_or(run);
                            (task.clone(), owner)
                        })
                        .collect();
                    if tasks != thread.background_tasks {
                        thread.background_tasks = tasks;
                        thread.updated_at = at;
                    }
                }
            }
            // On the steer message, which the store updates.
            ProviderEvent::SteerUpdated { .. } => {}
            ProviderEvent::PromptDelivered { bytes, pages } => {
                record.prompt_delivered = true;
                let (used_bytes, used_pages) = self.attachment_budget.unwrap_or_default();
                self.attachment_budget = Some((used_bytes + bytes, used_pages + pages));
            }
            ProviderEvent::TurnTerminal {
                status,
                failure,
                thread_disposition,
                driver,
                ..
            } => {
                record.ended_at = Some(at);
                if record.reported_model.is_some() {
                    record.model = record.reported_model.clone();
                }
                if let (None, Some(driver)) = (&self.thread, driver) {
                    record.driver = driver.clone();
                }
                // A turn that ended before its CLI opened a thread still has
                // the provider thread record the reference creates for the
                // run, not loaded.
                let thread = self.thread.get_or_insert_with(|| ThreadRecord {
                    driver: record.driver.clone(),
                    first_run: run,
                    created_at: at,
                    updated_at: at,
                    native_id: None,
                    status: "not_loaded",
                    conversation_head: None,
                    background_tasks: Vec::new(),
                    opencode2: None,
                    native_title: None,
                    attachment_budget: None,
                    native_updated_at: None,
                    context_usage: None,
                });
                self.terminals.insert(
                    run,
                    Terminal {
                        status: *status,
                        failure: failure.clone(),
                    },
                );
                let broken = *thread_disposition == ThreadDisposition::Broken;
                if broken {
                    self.native_thread = None;
                }
                if thread.native_id.is_some() {
                    thread.status = if broken { "error" } else { "idle" };
                    thread.updated_at = at;
                }
            }
            ProviderEvent::RuntimeRequestOpened { request } => {
                record.requests.push(RequestRecord {
                    request: request.clone(),
                    created_at: at,
                    status: "pending",
                    resolved_at: None,
                    decision: None,
                    feedback: None,
                    answers: None,
                    delivery_failed_at: None,
                });
                // A question or approval takes the next item slot, as the
                // reference orders it among the turn's items.
                let kind = if request.kind == "user_input" {
                    "user_input_request"
                } else {
                    "approval_request"
                };
                record.items.push(ItemRecord {
                    item: ProviderItem::new(format!("request:{}", request.id), kind, "waiting"),
                    first_at: at,
                    updated_at: at,
                    held_text: String::new(),
                });
            }
            ProviderEvent::RuntimeRequestAnswered {
                id,
                decision,
                feedback,
                answers,
            } => {
                if let Some(request) = record
                    .requests
                    .iter_mut()
                    .find(|known| &known.request.id == id && known.pending())
                {
                    request.status = "resolved";
                    request.resolved_at = Some(at);
                    request.decision = decision.clone();
                    request.feedback = feedback.clone();
                    request.answers = answers.clone();
                }
            }
            ProviderEvent::RuntimeRequestDeliveryFailed { id } => {
                if let Some(request) = record
                    .requests
                    .iter_mut()
                    .find(|known| &known.request.id == id && known.delivery_failed_at.is_none())
                {
                    request.delivery_failed_at = Some(at);
                    record.items.push(ItemRecord {
                        item: ProviderItem::new(
                            format!("delivery:{id}"),
                            "approval_request",
                            "failed",
                        ),
                        first_at: at,
                        updated_at: at,
                        held_text: String::new(),
                    });
                }
            }
            ProviderEvent::RuntimeRequestClosed { id, .. } => {
                if let Some(request) = record
                    .requests
                    .iter_mut()
                    .find(|known| &known.request.id == id && known.pending())
                {
                    request.status = "cancelled";
                    request.resolved_at = Some(at);
                }
            }
            ProviderEvent::CommandOutputDelta { .. } | ProviderEvent::RuntimeRequest { .. } => {}
        }
    }

    /// A server restart stopped the provider session at `at`: no CLI
    /// survives the server, so an active provider thread is idle.
    pub(crate) fn stop_session(&mut self, at: u64) {
        self.session_stopped_at = Some(at);
        if let Some(thread) = self.thread.as_mut().filter(|t| t.status == "active") {
            thread.status = "idle";
            thread.updated_at = at;
        }
    }

    /// A thread change detached the provider session, stopping `runs`.
    pub(crate) fn detach_session(&mut self, at: u64, runs: &[Uuid], fresh: bool) {
        self.session_detached_at = Some(at);
        if fresh {
            self.fresh_session = true;
        }
        self.detached_runs.extend(runs.iter().copied());
        // A turn the detach stops reports its thread idle when it ends.
        if let Some(thread) = self
            .thread
            .as_mut()
            .filter(|t| t.status == "active" && runs.is_empty())
        {
            thread.status = "idle";
            thread.updated_at = at;
        }
    }

    /// Whether the thread's provider session is open: a turn opened it and
    /// no restart stopped it, nor a thread change detached it, since.
    pub fn session_live(&self) -> bool {
        self.thread.is_some()
            && self.session_stopped_at.is_none()
            && self.session_detached_at.is_none()
    }

    /// The run and record of the request `id`.
    pub fn request(&self, id: &str) -> Option<(Uuid, &RequestRecord)> {
        self.runs.iter().find_map(|(run, record)| {
            record
                .requests
                .iter()
                .find(|request| request.request.id == id)
                .map(|request| (*run, request))
        })
    }

    /// How many provider turns the thread's provider thread ran.
    pub fn provider_turns(&self) -> usize {
        let prior = self.prior_turns.map_or(0, |(turns, _)| turns);
        prior
            + self
                .runs
                .values()
                .filter(|run| {
                    run.turn_started || run.native_turn_id.is_some() || !run.items.is_empty()
                })
                .count()
    }

    /// The native thread a turn of `driver` resumes.
    pub fn resume_for(&self, driver: &str) -> Option<String> {
        if self.fresh_session {
            return None;
        }
        self.native_thread
            .as_ref()
            .filter(|thread| thread.driver == driver)
            .map(|thread| thread.id.clone())
            // A switch back resumes that instance's own native thread.
            .or_else(|| {
                self.other_threads
                    .iter()
                    .find(|thread| thread.driver == driver && thread.status != "error")
                    .and_then(|thread| thread.native_id.clone())
            })
    }
}

type ProviderLaunch = (ProviderTurn, Vec<(String, String)>);

fn provider_instance_id(row: &Value) -> &str {
    row["modelSelection"]["instanceId"]
        .as_str()
        .or(row["providerInstanceId"].as_str())
        .unwrap_or(provider::CODEX)
}

/// Build a real thread's provider launch, including the thread-scoped MCP
/// server when the host has an address the provider can reach. Model probes
/// only need the instance settings and use `provider_turn` directly.
pub(crate) fn thread_provider_turn(
    settings: &Value,
    manifest: &Value,
    store: &crate::Store,
    thread_id: &str,
    oauth: &crate::mcp_oauth::OAuth,
    endpoint: Option<&str>,
) -> std::io::Result<Option<ProviderLaunch>> {
    let Some(thread) = store.threads.get(thread_id) else {
        return Ok(None);
    };
    let row = thread.stored_row();
    let Some((mut provider, env)) = provider_turn(settings, manifest, &row, &thread.provider)
    else {
        return Ok(None);
    };
    if let Some(endpoint) = endpoint {
        let overrides = &settings["projectSettingsOverrides"];
        let project = store
            .project_rows
            .get(&thread.project_id)
            .filter(|row| row["deletedAt"].is_null());
        let browser_overridden = overrides.as_object().is_some_and(|entries| {
            entries
                .values()
                .any(|entry| entry.get("enableAgentBrowserAccess").is_some())
        });
        let device_overridden = overrides.as_object().is_some_and(|entries| {
            entries
                .values()
                .any(|entry| entry.get("enableAgentDeviceAccess").is_some())
        });
        let enable_device_support = settings["enableDeviceSupport"].as_bool().unwrap_or(false);
        let (browser, device) = if project.is_none() && (browser_overridden || device_overridden) {
            (
                if browser_overridden {
                    false
                } else {
                    settings["enableAgentBrowserAccess"]
                        .as_bool()
                        .unwrap_or(true)
                },
                enable_device_support
                    && !device_overridden
                    && settings["enableAgentDeviceAccess"]
                        .as_bool()
                        .unwrap_or(false),
            )
        } else {
            (
                overrides[&thread.project_id]["enableAgentBrowserAccess"]
                    .as_bool()
                    .or(settings["enableAgentBrowserAccess"].as_bool())
                    .unwrap_or(true),
                enable_device_support
                    && overrides[&thread.project_id]["enableAgentDeviceAccess"]
                        .as_bool()
                        .or(settings["enableAgentDeviceAccess"].as_bool())
                        .unwrap_or(false),
            )
        };
        let token =
            oauth.provider_credential(&thread.id, provider_instance_id(&row), browser, device)?;
        provider.mcp = Some(provider::McpServer {
            url: endpoint.to_owned(),
            authorization: format!("Bearer {token}"),
            device_tools: device,
        });
    }
    Ok(Some((provider, env)))
}

/// How a turn of a thread runs its provider: the CLI of the thread's
/// provider instance (from `settings.providerInstances`, or the built-in
/// instance of the same name) with the thread's model selection, resuming
/// the thread's native thread. `None` for an instance whose driver the
/// workspace does not run.
pub(crate) fn provider_turn(
    settings: &Value,
    manifest: &Value,
    row: &Value,
    state: &ProviderState,
) -> Option<ProviderLaunch> {
    let selection = &row["modelSelection"];
    let instance_id = provider_instance_id(row);
    let instance = &settings["providerInstances"][instance_id];
    let driver = instance["driver"].as_str().unwrap_or(instance_id);
    if !provider::runs_turns(driver) {
        return None;
    }
    let config = &instance["config"];
    let text = |key: &str| config[key].as_str().unwrap_or_default().trim().to_owned();
    // Pi's thinking level is its "thinking" option, OpenCode's variant its
    // "variant" (`t3:default` choosing none), fx's effort its "effort".
    let effort_option = match driver {
        provider::PI => "thinking",
        provider::OPENCODE => "variant",
        provider::FX => "effort",
        _ => "reasoningEffort",
    };
    let option = |id: &str| {
        selection["options"].as_array().and_then(|options| {
            options
                .iter()
                .find(|option| option["id"] == id)
                .and_then(|option| option["value"].as_str())
                .map(str::to_owned)
        })
    };
    let reasoning_effort = option(effort_option)
        .filter(|value| !(driver == provider::OPENCODE && value == "t3:default"));
    let env = instance["environment"]
        .as_array()
        .map(|variables| {
            variables
                .iter()
                .filter_map(|variable| {
                    Some((
                        variable["name"].as_str()?.to_owned(),
                        variable["value"].as_str()?.to_owned(),
                    ))
                })
                .collect()
        })
        .unwrap_or_default();
    let claude_launch = if driver == provider::CLAUDE_AGENT {
        crate::model_manifest::claude_launch(manifest, selection)
    } else {
        (None, Value::Null)
    };
    let turn = ProviderTurn {
        driver: driver.to_owned(),
        instance_id: instance_id.to_owned(),
        binary_path: text("binaryPath"),
        launch_args: split_args(&text("launchArgs")),
        home_path: text("homePath"),
        model: selection["model"]
            .as_str()
            .filter(|model| !model.is_empty() && *model != "default")
            .unwrap_or_default()
            .to_owned(),
        reasoning_effort: reasoning_effort.or_else(|| claude_launch.0.clone()),
        resume: state.resume_for(driver),
        runtime_mode: row["runtimeMode"].as_str().unwrap_or_default().to_owned(),
        app_thread_id: row["id"].as_str().unwrap_or_default().to_owned(),
        interaction_mode: row["interactionMode"]
            .as_str()
            .unwrap_or_default()
            .to_owned(),
        extra_dirs: Vec::new(),
        claude_settings: claude_launch.1,
        mcp: None,
        attachments: Vec::new(),
        turn_ordinal: Some(state.provider_turns() as u64 + 1),
        recovery_prompt: None,
        fork_session: None,
        inject_items: None,
        recovery_inject_items: None,
        injected_prompt: None,
        native_turn: String::new(),
        thread_title: row["title"].as_str().unwrap_or_default().to_owned(),
        context_window: (driver == provider::CLAUDE_AGENT)
            .then(|| crate::model_manifest::claude_context_window(manifest, selection))
            .flatten(),
        auth_method: String::new(),
        opencode: (driver == provider::OPENCODE).then(|| provider::OpenCodeLaunch {
            server_url: text("serverUrl"),
            // `settingsSecretLiteral`: a referenced secret is not a password.
            server_password: config["serverPassword"]
                .as_str()
                .unwrap_or_default()
                .to_owned(),
            agent: option("agent").unwrap_or_default(),
        }),
        antigravity: None,
        kiro: (driver == provider::KIRO).then(|| provider::KiroLaunch {
            agent: option("mode").unwrap_or_default(),
            default_model: String::new(),
        }),
        muse: None,
        continuation: false,
    };
    let mut turn = turn;
    if driver == provider::ACP_REGISTRY {
        acp_registry_launch(&mut turn, config);
    }
    Some((turn, env))
}

/// An ACP Registry agent's launch from its instance's settings: a local
/// agent's executable (`commandPath`) and arguments (`commandArgs`); for a
/// registry agent, the executable override, its distribution resolved when
/// the turn launches. The `model` "default" is the agent's own.
fn acp_registry_launch(turn: &mut ProviderTurn, config: &Value) {
    let text = |key: &str| config[key].as_str().unwrap_or_default().trim().to_owned();
    turn.binary_path = text("commandPath");
    turn.launch_args = if config["source"] == "local" {
        config["commandArgs"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|arg| arg.as_str().map(str::to_owned))
            .collect()
    } else {
        Vec::new()
    };
    turn.auth_method = text("authMethodId");
}

/// Split launch arguments as a shell would split plain words and quoted
/// strings (no expansion).
pub(crate) fn split_args(text: &str) -> Vec<String> {
    let mut args = Vec::new();
    let mut current = String::new();
    let mut quote = None;
    let mut started = false;
    let mut chars = text.chars();
    while let Some(c) = chars.next() {
        match (quote, c) {
            (Some(q), c) if c == q => quote = None,
            (Some('"'), '\\') => current.extend(chars.next()),
            (Some(_), c) => current.push(c),
            (None, '"' | '\'') => {
                quote = Some(c);
                started = true;
            }
            (None, '\\') => {
                current.extend(chars.next());
                started = true;
            }
            (None, c) if c.is_whitespace() => {
                if started {
                    args.push(std::mem::take(&mut current));
                    started = false;
                }
            }
            (None, c) => {
                current.push(c);
                started = true;
            }
        }
    }
    if started {
        args.push(current);
    }
    args
}

#[cfg(test)]
mod tests {
    #[test]
    fn a_switch_of_instance_opens_its_own_provider_thread_and_a_switch_back_resumes_the_first() {
        use uuid::Uuid;
        let (first, second, third) = (Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4());
        let mut state = ProviderState::default();
        let thread = |driver: &str, id: &str| ProviderEvent::ProviderThread {
            driver: driver.into(),
            native_thread_id: id.into(),
            resumed: false,
            model: None,
            opencode2: None,
            native_title: None,
            native_updated_at: None,
            context_usage: None,
            session_started: None,
        };
        state.apply(first, &thread(provider::CODEX, "thr_codex"), 1);
        state.apply(second, &thread(provider::CLAUDE_AGENT, "sess_claude"), 2);
        assert_eq!(
            state.thread.as_ref().unwrap().driver,
            provider::CLAUDE_AGENT
        );
        assert_eq!(state.other_threads.len(), 1);
        assert_eq!(state.other_threads[0].status, "idle");
        assert_eq!(state.run_threads[&first], first);
        assert_eq!(state.run_threads[&second], second);
        // Back on Codex, the turn resumes Codex's own native thread and its record.
        assert_eq!(
            state.resume_for(provider::CODEX).as_deref(),
            Some("thr_codex")
        );
        state.apply(third, &thread(provider::CODEX, "thr_codex"), 3);
        assert_eq!(state.thread.as_ref().unwrap().first_run, first);
        assert_eq!(state.run_threads[&third], first);
        assert_eq!(state.other_threads[0].driver, provider::CLAUDE_AGENT);
        assert_eq!(
            state.resume_for(provider::CLAUDE_AGENT).as_deref(),
            Some("sess_claude")
        );
    }

    use super::*;
    use serde_json::json;

    #[test]
    fn a_turn_takes_its_instance_settings_selection_and_native_thread() {
        let settings = json!({"providerInstances": {"work": {
            "driver": "codex",
            "config": {"binaryPath": "/opt/codex", "launchArgs": "--profile 'my work' -c x=1", "homePath": "/h"},
            "environment": [{"name": "A", "value": "1", "sensitive": false}]
        }}});
        let row = json!({"modelSelection": {"instanceId": "work", "model": "gpt-5",
            "options": [{"id": "reasoningEffort", "value": "high"}]}});
        let mut state = ProviderState::default();
        state.apply(
            Uuid::nil(),
            &ProviderEvent::ProviderThread {
                driver: "codex".into(),
                native_thread_id: "thr_1".into(),
                resumed: false,
                model: None,

                opencode2: None,
                native_title: None,
                native_updated_at: None,
                context_usage: None,
                session_started: None,
            },
            1,
        );
        let (turn, env) = provider_turn(&settings, &json!({}), &row, &state).unwrap();
        assert_eq!(turn.driver, "codex");
        assert_eq!(turn.binary_path, "/opt/codex");
        assert_eq!(turn.launch_args, ["--profile", "my work", "-c", "x=1"]);
        assert_eq!(turn.home_path, "/h");
        assert_eq!(turn.model, "gpt-5");
        assert_eq!(turn.reasoning_effort.as_deref(), Some("high"));
        assert_eq!(turn.resume.as_deref(), Some("thr_1"));
        assert_eq!(env, [("A".to_string(), "1".to_string())]);

        // A built-in instance needs no settings; another driver's native
        // thread is not resumed.
        let claude = json!({"modelSelection": {"instanceId": "claudeAgent", "model": "default"}});
        let (turn, _) = provider_turn(&json!({}), &json!({}), &claude, &state).unwrap();
        assert_eq!(
            (turn.driver.as_str(), turn.model.as_str()),
            ("claudeAgent", "")
        );
        assert_eq!(turn.resume, None);
        let other = json!({"modelSelection": {"instanceId": "cursor", "model": "x"}});
        assert!(provider_turn(&json!({}), &json!({}), &other, &state).is_none());
    }

    #[test]
    fn a_fresh_context_resumes_nothing_until_its_turn_opens_a_new_session() {
        use uuid::Uuid;
        let (first, fresh) = (Uuid::new_v4(), Uuid::new_v4());
        let mut state = ProviderState::default();
        let thread = |id: &str| ProviderEvent::ProviderThread {
            driver: provider::CLAUDE_AGENT.into(),
            native_thread_id: id.into(),
            resumed: false,
            model: None,
            opencode2: None,
            native_title: None,
            native_updated_at: None,
            context_usage: None,
            session_started: None,
        };
        state.apply(first, &thread("sess-old"), 1);
        assert_eq!(
            state.resume_for(provider::CLAUDE_AGENT).as_deref(),
            Some("sess-old")
        );
        // A detach for another reason still resumes the session later.
        state.detach_session(2, &[], false);
        assert_eq!(
            state.resume_for(provider::CLAUDE_AGENT).as_deref(),
            Some("sess-old")
        );
        state.detach_session(3, &[], true);
        assert_eq!(state.resume_for(provider::CLAUDE_AGENT), None);
        // The fresh turn's own session is the one later turns resume.
        state.apply(fresh, &thread("sess-new"), 4);
        assert_eq!(
            state.resume_for(provider::CLAUDE_AGENT).as_deref(),
            Some("sess-new")
        );
    }
}
