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

/// How to run one turn of a provider instance.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProviderTurn {
    /// The instance's driver: [`CODEX`] or [`CLAUDE_AGENT`].
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
}

/// Where a provider CLI reaches T3's tools for its thread
/// (`McpProviderSessionConfig` in the reference).
#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct McpServer {
    /// `http://<host>:<port>/mcp` on this server.
    pub url: String,
    /// `Bearer <token>`: the thread's credential.
    pub authorization: String,
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
    },
    /// The turn started running in the CLI.
    #[serde(rename = "provider_turn.updated", rename_all = "camelCase")]
    ProviderTurn {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        native_turn_id: Option<String>,
        status: TurnStatus,
        /// The model the turn ran on, once the CLI reports it.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        model: Option<String>,
    },
    /// A turn item was created or changed. Later updates of the same
    /// `item.id` replace earlier ones.
    #[serde(rename = "turn_item.updated")]
    TurnItem { item: ProviderItem },
    /// Text appended to a streaming item (`assistant_message`, `reasoning`).
    /// The item's next [`ProviderEvent::TurnItem`] carries the full text.
    #[serde(rename = "turn_item.delta", rename_all = "camelCase")]
    TurnItemDelta { item_id: String, delta: String },
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
    /// `completed` once the CLI took it, or `cancelled` with a turn that
    /// ended first. `message_id` is the steer message's id.
    #[serde(rename = "steer.updated", rename_all = "camelCase")]
    SteerUpdated { message_id: String, status: String },
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
