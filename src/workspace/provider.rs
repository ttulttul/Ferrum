//! Provider turns: a turn run by a coding agent's CLI (Codex, Claude) instead
//! of a plain command.
//!
//! The workspace runs the CLI and translates its protocol into
//! [`ProviderEvent`]s, so what crosses to the control plane is already
//! provider-neutral. The control plane only records these events; it never
//! sees raw CLI output.
//!
//! Event and field names follow the reference server's provider adapter
//! events (`provider_thread.updated`, `turn_item.updated`,
//! `command_output.delta`, `plan.updated`, `turn.terminal`, ...). They carry
//! only what the workspace knows: native ids, item content and statuses. The
//! control plane adds its own ids (thread, run, node, ordinal) and timestamps
//! when it projects them.
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

/// The drivers whose CLI the workspace can run as a provider turn.
pub const CODEX: &str = "codex";
pub const CLAUDE_AGENT: &str = "claudeAgent";
/// Grok's CLI, whose turns run over ACP (`grok agent stdio`).
pub const GROK: &str = "grok";

/// ACP Registry agents, run over ACP in the reference's generic flavor.
pub const ACP_REGISTRY: &str = "acpRegistry";

/// Pi's CLI, whose turns run over its RPC mode (`pi --mode rpc`).
pub const PI: &str = "pi";

/// OpenCode, whose turns run on its HTTP server (`opencode serve`).
pub const OPENCODE: &str = "opencode";
/// Antigravity's `agy` CLI, whose turns run over ACP (`agy acp`).
pub const ANTIGRAVITY: &str = "antigravity";
/// Kiro's CLI, whose turns run over ACP (`kiro-cli acp --agent-engine=v3`).
pub const KIRO: &str = "kiro";
/// fx's CLI, whose turns run over ACP (`fx acp`).
pub const FX: &str = "fx";
/// Muse Code, whose turns run over its own protocol (MSP) on `muse serve`.
pub const MUSE: &str = "muse";

/// Whether the workspace runs turns of `driver`'s CLI.
pub fn runs_turns(driver: &str) -> bool {
    matches!(
        driver,
        CODEX | CLAUDE_AGENT | GROK | ACP_REGISTRY | PI | OPENCODE | ANTIGRAVITY | KIRO | FX | MUSE
    )
}

/// Whether `driver`'s turns run over ACP.
pub fn is_acp(driver: &str) -> bool {
    matches!(driver, GROK | ACP_REGISTRY | ANTIGRAVITY | KIRO | FX)
}

/// Whether the reference gives each of `driver`'s threads a provider
/// session of its own, named after the thread and a random id (the ACP
/// agents, Pi, OpenCode 1.x and Muse).
pub fn thread_session(driver: &str) -> bool {
    is_acp(driver) || driver == PI || driver == OPENCODE || driver == MUSE
}

/// Whether `driver`'s adapter resolves the requests it asks itself, with no
/// decision recorded and its items completed however they were answered
/// (Pi's dialogs, OpenCode's permissions).
pub fn resolves_own_requests(driver: &str) -> bool {
    driver == PI || driver == OPENCODE
}

/// How to run one turn of a provider instance.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProviderTurn {
    /// The instance's driver: [`CODEX`], [`CLAUDE_AGENT`] or [`GROK`].
    pub driver: String,
    /// The provider instance, whose turns share one Codex app-server.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub instance_id: String,
    /// The CLI to run; empty for the driver's default (`codex`, `claude`).
    #[serde(default)]
    pub binary_path: String,
    /// The instance's launch arguments, placed after the driver's own.
    #[serde(default)]
    pub launch_args: Vec<String>,
    /// The CLI's home (`CODEX_HOME`, `CLAUDE_CONFIG_DIR`); empty to inherit.
    #[serde(default)]
    pub home_path: String,
    /// The model to ask for; empty for the CLI's default.
    #[serde(default)]
    pub model: String,
    /// The reasoning effort option of the model selection, if any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reasoning_effort: Option<String>,
    /// The native thread (Codex) or session (Claude) to continue, from the
    /// last [`ProviderEvent::ProviderThread`] of the thread's previous turn.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resume: Option<String>,
    /// The thread's runtime mode (`approval-required`, `auto-accept-edits`,
    /// `auto` or `full-access`), which decides what the CLI may do without
    /// asking. Empty means `full-access`, the reference's default. Every
    /// request the CLI still makes goes to the user, in every mode.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub runtime_mode: String,
    /// The model's context window from the model catalog, the
    /// `maxTokens` of a Claude turn's token usage.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context_window: Option<u64>,
    /// The app thread the turn belongs to, which the CLI's shells see as
    /// `T3CODE_THREAD_ID`.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub app_thread_id: String,
    /// The thread's interaction mode (`default` or `plan`); empty means
    /// `default`.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub interaction_mode: String,
    /// Directories besides the working directory the CLI may read (the
    /// attachments directory).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub extra_dirs: Vec<String>,
    /// Claude Code `--settings` the model selection sets (`fastMode`).
    #[serde(default, skip_serializing_if = "Value::is_null")]
    pub claude_settings: Value,
    /// The T3 MCP server the CLI is given, acting as the thread.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mcp: Option<McpServer>,
    /// The turn's message attachments (`ChatAttachment`s), each with the
    /// `path` of its saved file. Optional and omitted when empty, so an
    /// agentd or control plane that predates it sees the same wire bytes.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub attachments: Vec<Value>,
    /// The ordinal of this turn among its provider thread's turns, which an
    /// ACP turn names its native turn by (`<session>:turn:<ordinal>`).
    /// Optional and omitted when absent, so an agentd or control plane that
    /// predates it sees the same wire bytes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub turn_ordinal: Option<u64>,
    /// The ACP authentication method the instance names (`authMethodId`),
    /// used when the agent asks to sign in; empty for the agent's first.
    /// Optional and omitted when empty, so an agentd or control plane that
    /// predates it sees the same wire bytes.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub auth_method: String,
    /// The prompt to send instead when [`ProviderTurn::resume`] names a
    /// session the CLI cannot resume and a fresh one starts: the turn's prompt
    /// with the thread's recovered context ahead of it (the reference's
    /// `full_thread_summary` resume fallback). Optional and omitted when
    /// unset, so an agentd or control plane that predates it sees the same
    /// wire bytes; without it the fresh session gets the plain prompt.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub recovery_prompt: Option<String>,
    /// For a fork's first turn: the source's native session to fork, which
    /// the CLI continues under a new id when it can. When it cannot, the turn
    /// starts a fresh session with [`ProviderTurn::recovery_prompt`] (the
    /// source's history). Optional and omitted when unset, so an agentd or
    /// control plane that predates it sees the same wire bytes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fork_session: Option<String>,
    /// The context handoff's history as native history items, which Codex
    /// takes with `thread/inject_items` before the prompt instead of inline
    /// (the reference's injectHistory); [`ProviderTurn::recovery_inject_items`]
    /// for a fresh thread taking the resume fallback. When it takes them, the
    /// prompt is [`ProviderTurn::injected_prompt`]. Optional and omitted when
    /// unset, so an agentd or control plane that predates them sees the same
    /// wire bytes; without them the history goes inline.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inject_items: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub recovery_inject_items: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub injected_prompt: Option<String>,
    /// The native turn id a Pi turn is named by before Pi names it
    /// (`<provider thread>:attempt:<attempt>`), which its items' ids extend.
    /// Optional and omitted when empty, so an agentd or control plane that
    /// predates it sees the same wire bytes.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub native_turn: String,
    /// The thread's title, which a Pi session is named after. Optional and
    /// omitted when empty, likewise.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub thread_title: String,
    /// An OpenCode instance's server settings and the selection's agent.
    /// Optional and omitted when absent, so an agentd or control plane that
    /// predates it sees the same wire bytes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub opencode: Option<OpenCodeLaunch>,
    /// What an Antigravity turn's launch resolved besides its command: the
    /// installed version and where each process's temporary folder goes.
    /// Optional and omitted when absent, so an agentd or control plane that
    /// predates it sees the same wire bytes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub antigravity: Option<AntigravityLaunch>,
    /// What a Kiro turn's session is set to besides its model: the thread's
    /// agent and Kiro's own id for its "default" model. Optional and omitted
    /// when absent, so an agentd or control plane that predates it sees the
    /// same wire bytes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kiro: Option<KiroLaunch>,
    /// What a Muse turn resolved from the instance's model catalog (the
    /// model "default" stands for and the reasoning effort it is sent) and
    /// the provider thread its items are named under. Optional and omitted
    /// when absent, so an agentd or control plane that predates it sees the
    /// same wire bytes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub muse: Option<MuseLaunch>,
    /// The turn is a continuation run (`creationSource: "provider"`): it
    /// shows what the thread's running CLI did on its own while idle
    /// rather than sending its prompt, as the reference's Claude adapter
    /// drains its wake buffer. Optional and omitted when false, so an
    /// agentd or control plane that predates it sees the same wire bytes.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub continuation: bool,
}

/// A provider's CLI began work of its own while no turn of its thread ran
/// (a Claude wake turn), which a continuation run of the thread shows
/// (`offerWakeContinuation` in the reference).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProviderWake {
    /// [`CLAUDE_AGENT`].
    pub driver: String,
    /// The app thread whose turns the CLI runs.
    pub app_thread_id: String,
    /// The CLI's native session.
    pub native_thread_id: String,
    /// Names this wake when its continuation ends without starting
    /// ([`crate::workspace::Workspace::retire_provider_wake`]).
    pub token: u64,
    /// The text of the wake's continuation: the summary of the background
    /// task notification that started it, if one did.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
    /// The continuation's notification, naming the background work that
    /// ended (`backgroundWorkNotification`), if any did.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub notification: Option<serde_json::Value>,
    /// How the continuation reaches the provider: `message_text` sends its
    /// text as the run's message (a Codex subagent's result), where a wake
    /// without it shows what the CLI buffered. Additive and optional.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delivery: Option<String>,
}

/// What an OpenCode turn needs beyond the common launch: the configured
/// server (`serverUrl`, `serverPassword`; none for a server of the session's
/// own) and the selection's `agent` option. Each field is omitted when empty.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OpenCodeLaunch {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub server_url: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub server_password: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub agent: String,
}

/// A Kiro turn's session settings beyond the model: the thread's agent
/// (the model selection's `mode` option, Kiro's agent picker) and the model
/// id Kiro's "default" stands for (its `--list-models` default, from the
/// instance's status check; `auto` when unknown). Each is omitted when empty.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct KiroLaunch {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub agent: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub default_model: String,
}

/// A Muse turn's launch data from the instance's status check: the model
/// "default" stands for (the catalog's default, not custom, model; empty
/// when none), the reasoning effort the turn is sent (the selection's when
/// that model lists it, else its default; empty when it has none), and the
/// provider thread record the turn runs on, whose id its items' ids carry.
/// Each is omitted when empty.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MuseLaunch {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub default_model: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub reasoning_effort: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub provider_thread: String,
}

/// An Antigravity launch's installed `agy` version (empty when unknown),
/// which decides the client info it is sent, and the folder each `agy acp`
/// process gets its own `run-XXXXXX` temporary folder in.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AntigravityLaunch {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub version: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub run_temp_parent: String,
    /// Why the launch could not be prepared (the executable is missing, the
    /// sign-in settings are incomplete): the session fails to open with it.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub launch_error: String,
}

/// What an OpenCode 2 thread's [`ProviderEvent::ProviderThread`] reports:
/// the server's version (whose 2.2.5 enables native goals) and the model
/// the OpenCode session reports (`nativeMetadata.modelSelection`), omitted
/// when unknown.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OpenCode2Thread {
    pub version: String,
    #[serde(default, skip_serializing_if = "Value::is_null")]
    pub model_selection: Value,
}

/// Where a provider CLI reaches T3's tools for its thread
/// (`McpProviderSessionConfig` in the reference).
#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct McpServer {
    /// `http://<host>:<port>/mcp` on this server.
    pub url: String,
    /// `Bearer <token>`: the thread's credential.
    pub authorization: String,
    /// Whether the credential carries the `"device"` capability, so the
    /// agent is told about the device tools. Omitted when false, so an
    /// agentd or control plane that predates it sees the same wire bytes.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub device_tools: bool,
}

impl ProviderTurn {
    /// The interaction mode, `default` when unset.
    pub fn interaction_mode(&self) -> &str {
        match self.interaction_mode.as_str() {
            "" => "default",
            mode => mode,
        }
    }

    /// The runtime mode, `full-access` when unset.
    pub fn runtime_mode(&self) -> &str {
        match self.runtime_mode.as_str() {
            "" => "full-access",
            mode => mode,
        }
    }
}

/// One translated provider event. Events of a turn arrive in order and end
/// with exactly one [`ProviderEvent::TurnTerminal`], which the turn's
/// `Exited` event follows.
/// The reference's failure when Codex's app-server closes during a turn.
/// The reference's ProviderWorkspaceMissingError: a turn's folder is gone,
/// so no provider session can open in it.
pub fn workspace_missing(cwd: &str) -> ProviderFailure {
    ProviderFailure::new(
        "provider_error",
        format!("{WORKSPACE_MISSING}{cwd}. Restore the folder at this path before retrying."),
    )
}
/// Whether `failure` is [`workspace_missing`]'s.
pub fn is_workspace_missing(failure: &ProviderFailure) -> bool {
    failure.message.starts_with(WORKSPACE_MISSING)
}
const WORKSPACE_MISSING: &str =
    "This thread's workspace folder no longer exists or is not a directory: ";
pub const CODEX_STREAM_CLOSED: &str = "The provider event stream closed unexpectedly. Retry the turn; if it keeps failing, check the provider and server logs.";

/// How the reference's OpenCode 2 adapter fails every turn when its event
/// stream is lost (`failAll`); its provider session ends in error with it.
pub const OPENCODE2_STREAM_LOST: &str =
    "The OpenCode event stream was lost and could not reconnect.";

/// Whether `version` (`x.y.z`, anything after ignored) is at least
/// `minimum`.
pub fn version_at_least(version: &str, minimum: (u64, u64, u64)) -> bool {
    let parts: Vec<u64> = version
        .split(|c: char| !c.is_ascii_digit())
        .filter(|part| !part.is_empty())
        .take(3)
        .filter_map(|part| part.parse().ok())
        .collect();
    match parts.as_slice() {
        [major, minor, patch, ..] => (*major, *minor, *patch) >= minimum,
        _ => false,
    }
}

/// Whether a failure is the reference's `ProviderAdapterEventStreamError`:
/// the plain category text, or (since #39299) the one naming the exit.
pub fn stream_closed(message: &str) -> bool {
    message.starts_with("The provider event stream closed unexpectedly")
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionStatus {
    Running,
    Ready,
    Error,
}

/// A provider's own session update accompanying its turn report.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SessionReport {
    pub status: SessionStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum ProviderEvent {
    /// The native thread or session the turn runs in. Store
    /// `native_thread_id` on the thread and pass it back as
    /// [`ProviderTurn::resume`] next turn. `resumed` is false when the CLI
    /// started a new native thread (first turn, or the old one was gone).
    #[serde(rename = "provider_thread.updated", rename_all = "camelCase")]
    ProviderThread {
        driver: String,
        native_thread_id: String,
        resumed: bool,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        model: Option<String>,
        /// The OpenCode 2 server the thread's turns run on, which records
        /// them as the reference's OpenCode 2 adapter does (a provider
        /// session shared by the instance's threads, and more). Additive and
        /// skippable: absent for every other driver and for OpenCode 1.x, and
        /// a reader that predates it ignores it.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        opencode2: Option<Box<OpenCode2Thread>>,
        /// The title the agent gave its session (an ACP `session_info_update`
        /// title: Kiro's focus), the provider thread's `nativeMetadata.title`.
        /// Additive and skippable: absent until an agent names its session,
        /// and a reader that predates it ignores it.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        native_title: Option<String>,
        /// When the agent says it last changed its session (an ACP
        /// `session_info_update`'s `updatedAt`: fx's), the provider thread's
        /// `nativeMetadata.updatedAt`. Additive and skippable, as above.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        native_updated_at: Option<Box<str>>,
        /// How much of its context window the agent's session uses (an ACP
        /// `usage_update`: fx's), the provider thread's `contextUsage`.
        /// Additive and skippable: absent until an agent reports it, and a
        /// reader that predates it ignores it.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        context_usage: Option<Box<ContextUsage>>,
        /// What an Antigravity session reported as it started (`{instanceId,
        /// initialize: {agentInfo}, setup, cwd}`: the session's setup result holds
        /// its models, config options and modes), which the instance's provider status takes as
        /// the reference's `onSessionStarted` does. Sent once per session.
        /// Additive and skippable: absent for every other driver and on later
        /// turns, and a reader that predates it ignores it.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        session_started: Option<Box<Value>>,
    },
    /// The turn started running in the CLI.
    #[serde(rename = "provider_turn.updated", rename_all = "camelCase")]
    ProviderTurn {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        native_turn_id: Option<String>,
        /// The native turn the provider named once the turn ended (Pi's
        /// first user entry of the turn, or `null` when it wrote none), and
        /// the conversation's last entry then. Optional and omitted when
        /// absent: a reader that predates them skips them.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        native_turn_ref: Option<Value>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        conversation_head: Option<String>,
        status: TurnStatus,
        /// The model the turn ran on, once the CLI reports it.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        model: Option<String>,
        /// Session state reported by the adapter at this point in the turn.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        session: Option<Box<SessionReport>>,
        /// The turn's context handoff went to the native thread as history
        /// (Codex `thread/inject_items`), not inline with the prompt.
        /// Optional and omitted when false: a reader that predates it skips
        /// it.
        #[serde(default, skip_serializing_if = "std::ops::Not::not")]
        history_injected: bool,
    },
    /// A turn item was created or changed. Later updates of the same
    /// `item.id` replace earlier ones.
    #[serde(rename = "turn_item.updated")]
    TurnItem { item: ProviderItem },
    /// Text appended to a streaming item (`assistant_message`, `reasoning`).
    /// The item's next [`ProviderEvent::TurnItem`] carries the full text.
    #[serde(rename = "turn_item.delta", rename_all = "camelCase")]
    TurnItemDelta {
        item_id: String,
        delta: String,
        /// Text recorded while it is still held (see `turn::StreamedText`):
        /// it shows with the item's next shown update, or not at all if the
        /// item's end (which carries its whole text) comes first, as the
        /// reference's adapters send held text only when its window or its
        /// item ends.
        #[serde(default, skip_serializing_if = "std::ops::Not::not")]
        held: bool,
    },
    /// Output a running command printed. Not needed for the projection: the
    /// command item's final update carries all of it.
    #[serde(rename = "command_output.delta", rename_all = "camelCase")]
    CommandOutputDelta { item_id: String, chunk: String },
    /// The agent's todo list (Codex `turn/plan/updated`, Claude `TodoWrite`).
    #[serde(rename = "plan.updated")]
    Plan {
        steps: Vec<PlanStep>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        explanation: Option<String>,
    },
    /// Token usage so far, in the reference's `tokenUsage` shape: the
    /// context the latest model call used.
    #[serde(rename = "usage.updated")]
    Usage { usage: TokenUsage },
    /// The whole turn's token usage, the reference's `turnTokenUsage`.
    #[serde(rename = "turn_usage.updated")]
    TurnUsage { usage: Value },
    /// The CLI received the turn's prompt, which counts `bytes` and `pages`
    /// against the session's attachment budget (Claude).
    #[serde(rename = "prompt.delivered")]
    PromptDelivered { bytes: u64, pages: u64 },
    /// Where a message that steered the turn stands (the reference's
    /// `steer.updated`): `pending` once it was written to the CLI, then
    /// `completed` once the CLI took it or its turn completed (Claude then
    /// answers it on its own, as a wake), or `cancelled` with a turn that
    /// stopped first. `message_id` is the steer message's id.
    #[serde(rename = "steer.updated", rename_all = "camelCase")]
    SteerUpdated {
        message_id: String,
        status: String,
        /// The turn ended before the steer was sent (`turnCompleted`), so
        /// the server sends its message again as a run of its own. Additive
        /// and skippable: absent means false. (Workspaces no longer report
        /// it; earlier ones reported a Claude steer its completed turn left
        /// unread.)
        #[serde(default, skip_serializing_if = "std::ops::Not::not")]
        untaken: bool,
    },
    /// The background tasks the provider thread runs now, after a change (the
    /// reference's roster `provider_thread.updated`): each task's id, kind and
    /// what launched it (`pendingBackgroundTasks` without `runId`, which the
    /// projection stamps). Additive: a reader that predates it fails only this
    /// event's line, as for `steer.updated`.
    #[serde(rename = "background_tasks.updated")]
    BackgroundTasks { tasks: Vec<Value> },
    /// Recorded by earlier builds, which approved every request themselves.
    /// Kept so their event logs still load; it changes nothing.
    #[serde(rename = "runtime_request.resolved", rename_all = "camelCase")]
    RuntimeRequest {
        native_request_id: String,
        kind: String,
        decision: String,
    },
    /// The CLI asked the user something (an approval or a question) and the
    /// turn waits for the answer, which arrives through
    /// [`crate::workspace::Workspace::respond_turn`].
    #[serde(rename = "runtime_request.opened")]
    RuntimeRequestOpened { request: RuntimeRequestOpen },
    /// The user answered a request. Recorded by the control plane when the
    /// client answers, before the answer goes to the workspace.
    #[serde(rename = "runtime_request.answered", rename_all = "camelCase")]
    RuntimeRequestAnswered {
        id: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        decision: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        feedback: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        answers: Option<Value>,
    },
    /// The answer to a request could not be handed to its turn (it no
    /// longer runs in this workspace).
    #[serde(rename = "runtime_request.delivery_failed")]
    RuntimeRequestDeliveryFailed { id: String },
    /// The CLI withdrew a request, or the turn stopped waiting for it:
    /// `status` is `cancelled`.
    #[serde(rename = "runtime_request.closed")]
    RuntimeRequestClosed { id: String, status: String },
    /// How the turn ended. Always the last provider event of a turn.
    #[serde(rename = "turn.terminal", rename_all = "camelCase")]
    TurnTerminal {
        status: TurnStatus,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        failure: Option<ProviderFailure>,
        /// `reusable` when the next turn may resume the native thread,
        /// `broken` when it must start a new one.
        thread_disposition: ThreadDisposition,
        /// The turn's driver, so a turn that ended before its CLI opened a
        /// thread is still recorded as that driver's.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        driver: Option<String>,
        /// What the turn's end closes, applied first and recorded with it,
        /// as the reference writes them with the turn's end: the turn's
        /// token usage and the items it ends.
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        closing: Vec<ProviderEvent>,
    },
}

/// A request the CLI made that waits for the user, in the reference's
/// `runtime_request.updated` / `approval_request` terms.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RuntimeRequestOpen {
    /// The workspace's id for the request; answers name it.
    pub id: String,
    /// The CLI's id for the request.
    pub native_request_id: String,
    /// The tool call the request is about.
    pub native_item_id: String,
    /// `command`, `file-read`, `file-change`, `mcp-elicitation`,
    /// `permission` or `user_input`.
    pub kind: String,
    /// The runtime mode the turn ran in when the request was made.
    #[serde(default)]
    pub runtime_mode: String,
    /// Whether a decline may carry feedback for the agent.
    #[serde(default)]
    pub supports_feedback: bool,
    /// The request's own turn item fields: `prompt`, `reason`,
    /// `fullAccessOverride`, `appName` and `options` for an approval,
    /// `questions` for a question.
    #[serde(default)]
    pub fields: Map<String, Value>,
}

/// The user's answer to a [`RuntimeRequestOpen`], as the workspace gets it.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RuntimeResponse {
    /// [`RuntimeRequestOpen::id`].
    pub id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub decision: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub feedback: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub answers: Option<Value>,
    /// Not an answer: a message that steers the running turn (its text),
    /// with `id` the message's id (the reference's `provider-turn.steer`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub steer: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TurnStatus {
    Running,
    Completed,
    /// Stopped on request (cancel or interrupt).
    Interrupted,
    Failed,
}

impl TurnStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Running => "running",
            Self::Completed => "completed",
            Self::Interrupted => "interrupted",
            Self::Failed => "failed",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ThreadDisposition {
    Reusable,
    Broken,
}

/// The reference's `OrchestrationV2ProviderFailure`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProviderFailure {
    /// `usage_limit`, `provider_error`, `transport_error`,
    /// `permission_error`, `validation_error` or `unknown`.
    pub class: String,
    pub message: String,
    pub code: Option<String>,
    pub retryable: Option<bool>,
    /// When a usage limit resets, once known.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reset_at: Option<String>,
}

impl ProviderFailure {
    pub fn new(class: &str, message: impl Into<String>) -> Self {
        let mut message: String = message.into();
        if message.trim().is_empty() {
            message = "The provider turn failed.".into();
        }
        // The reference caps failure messages at 4096 characters.
        if message.chars().count() > 4096 {
            message = message.chars().take(4096).collect();
        }
        Self {
            class: class.into(),
            message,
            code: None,
            retryable: None,
            reset_at: None,
        }
    }
}

/// The native id of a turn's first provider retry item (an `error` item
/// with `retry`); the `N`th retry after a recovery is `terminal-failure:N`.
/// The projection names it as the reference does
/// (`terminal-failure:<provider turn>[:N]`), and a failure the retry ends in
/// takes its place, as both are the turn's failure row.
pub const PROVIDER_RETRY_ITEM: &str = "terminal-failure";

/// A turn item in the reference's `OrchestrationV2TurnItem` shape, without
/// the fields the control plane owns.
///
/// `fields` holds the type's own fields, named as the reference names them:
/// `assistant_message` and `reasoning` have `text` and `streaming`;
/// `command_execution` has `input`, `output` and `exitCode`; `file_change`
/// has `fileName`, `diffStr` and `changes`; `dynamic_tool` has `toolName`,
/// `input` and `output`; `web_search` has `patterns`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ProviderItem {
    /// The native item id; stable across updates of the item.
    pub id: String,
    #[serde(rename = "type")]
    pub kind: String,
    /// `running`, `completed`, `failed` or `cancelled`.
    pub status: String,
    #[serde(default)]
    pub title: Option<String>,
    #[serde(flatten)]
    pub fields: Map<String, Value>,
}

impl ProviderItem {
    /// Tool calls whose node and turn item are independent provider snapshots.
    pub(crate) fn is_tool_call(&self) -> bool {
        matches!(
            self.kind.as_str(),
            "command_execution" | "file_change" | "file_search" | "web_search" | "dynamic_tool"
        )
    }

    pub fn new(id: impl Into<String>, kind: &str, status: &str) -> Self {
        Self {
            id: id.into(),
            kind: kind.into(),
            status: status.into(),
            title: None,
            fields: Map::new(),
        }
    }
    pub fn with(mut self, key: &str, value: impl Into<Value>) -> Self {
        self.fields.insert(key.into(), value.into());
        self
    }
    pub fn titled(mut self, title: impl Into<String>) -> Self {
        self.title = Some(title.into());
        self
    }
}

/// The reference's `OrchestrationV2PlanStep`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PlanStep {
    pub step: String,
    /// `pending`, `inProgress` or `completed`.
    pub status: String,
}

/// A provider thread's `contextUsage`, as an agent reports it: the tokens its
/// session holds and its context window, when known.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ContextUsage {
    pub used_tokens: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_tokens: Option<u64>,
}

/// The reference's provider turn `tokenUsage`, without `updatedAt`.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TokenUsage {
    pub used_tokens: u64,
    pub max_tokens: Option<u64>,
    pub input_tokens: u64,
    pub cached_input_tokens: u64,
    pub output_tokens: u64,
    pub reasoning_output_tokens: u64,
    /// How long the prompt cache the call wrote lasts (Claude).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prompt_cache_ttl_ms: Option<u64>,
    /// When that cache expires, counted from the call's start.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prompt_cache_expires_at: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn events_serialize_with_the_reference_names() {
        let item = ProviderItem::new("call_1", "command_execution", "completed")
            .with("input", "ls")
            .with("output", "a\n")
            .with("exitCode", 0);
        assert_eq!(
            serde_json::to_value(ProviderEvent::TurnItem { item }).unwrap(),
            json!({"type":"turn_item.updated","item":{"id":"call_1","type":"command_execution",
                "status":"completed","title":null,"input":"ls","output":"a\n","exitCode":0}})
        );
        assert_eq!(
            serde_json::to_value(ProviderEvent::TurnTerminal {
                status: TurnStatus::Failed,
                failure: Some(ProviderFailure::new("provider_error", "boom")),
                thread_disposition: ThreadDisposition::Reusable,
                driver: Some("codex".into()),
                closing: Vec::new(),
            })
            .unwrap(),
            json!({"type":"turn.terminal","status":"failed","threadDisposition":"reusable","driver":"codex",
                "failure":{"class":"provider_error","message":"boom","code":null,"retryable":null}})
        );
        let thread = json!({"type":"provider_thread.updated","driver":"codex",
            "nativeThreadId":"thr_1","resumed":false});
        let event: ProviderEvent = serde_json::from_value(thread.clone()).unwrap();
        assert_eq!(serde_json::to_value(event).unwrap(), thread);
    }
}
