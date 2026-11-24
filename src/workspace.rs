//! The workspace plane: everything the control plane asks of the machine that
//! holds the projects. That covers files, git, terminals, provider processes,
//! previews and devices.
//!
//! The control plane (auth, event log, projections, RPC dispatch and
//! settings) reaches the workspace only through [`Workspace`]. Natively,
//! [`LocalWorkspace`] runs every operation in this process. A server that has
//! no local filesystem or processes uses [`RemoteWorkspace`], which reaches
//! `t3-agentd` over one connection instead.
//!
//! Requests and replies keep the RPC payloads' JSON shapes, so an operation
//! can cross a process boundary unchanged. `Option<Reply>` is `None` when the
//! operation does not know the method name, as the per-service routers did
//! before. Long-lived work (terminal output, preview and device state,
//! provider turns) is a stream of events.
use serde_json::Value;
use std::path::{Path, PathBuf};
use tokio::sync::{broadcast, mpsc, oneshot};

#[cfg(feature = "native")]
mod local;
#[cfg(feature = "native")]
pub use local::LocalWorkspace;
pub mod attachment_prompt;
#[cfg(test)]
pub(crate) mod fake_agentd;
pub mod provider;
mod remote;
pub use provider::{ProviderEvent, ProviderTurn};
#[cfg(feature = "native")]
pub use remote::{connect, NativeLink};
pub use remote::{unavailable, BusFrame, RemoteWorkspace, Transport};

/// A reply payload: the success value, or the error value an RPC fails with.
pub type Reply = Result<Value, Value>;

/// A workspace operation in flight. Native workspaces run on a multi-threaded
/// runtime, so their futures are `Send`; the WebAssembly build's are not.
#[cfg(feature = "native")]
pub type WorkspaceFuture<'a, T> = futures_util::future::BoxFuture<'a, T>;
#[cfg(not(feature = "native"))]
pub type WorkspaceFuture<'a, T> = futures_util::future::LocalBoxFuture<'a, T>;

#[cfg(feature = "native")]
pub trait MaybeSendSync: Send + Sync {}
#[cfg(feature = "native")]
impl<T: Send + Sync> MaybeSendSync for T {}
#[cfg(not(feature = "native"))]
pub trait MaybeSendSync {}
#[cfg(not(feature = "native"))]
impl<T> MaybeSendSync for T {}

/// One provider turn: a process started in `cwd` whose output becomes the
/// run's output.
#[derive(Clone, Debug, Default)]
pub struct TurnSpec {
    /// The executable and its leading arguments; the prompt follows them.
    /// Unused when `provider` is set.
    pub command: Vec<String>,
    pub prompt: String,
    pub cwd: String,
    /// Environment variables (matched case-insensitively) the process must
    /// not inherit.
    pub env_remove: Vec<String>,
    /// Environment variables to set, after `env_remove`: the provider
    /// instance's configured environment.
    pub env: Vec<(String, String)>,
    /// Run a provider's CLI instead of `command`. Its events arrive as
    /// [`TurnEvent::Provider`].
    pub provider: Option<ProviderTurn>,
    /// The run the turn belongs to; answers to its runtime requests name it
    /// (see [`Workspace::respond_turn`]).
    pub turn_id: String,
}

/// What a turn reports, in order. `Exited` is last and follows all output.
/// A command turn reports `Stdout` and `Stderr`; a provider turn reports
/// `Provider` events, ending with [`ProviderEvent::TurnTerminal`].
#[derive(Clone, Debug, PartialEq)]
pub enum TurnEvent {
    /// One line of standard output, with its line ending.
    Stdout(String),
    /// One line of standard error, with its line ending.
    Stderr(String),
    Provider(ProviderEvent),
    Exited(TurnExit),
}

/// Where an event sits in the workspace's stream for it. A workspace that
/// keeps events until they are acknowledged (see [`RemoteWorkspace`])
/// attaches one to each event; [`Workspace::ack`] it once the event is
/// recorded, and pass the last one recorded to
/// [`RemoteWorkspace::resume_turn`] to pick the stream up after a restart.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct StreamPosition {
    pub stream: u64,
    pub seq: u64,
}

/// A turn event and, when the workspace redelivers unacknowledged events,
/// its position.
#[derive(Clone, Debug, PartialEq)]
pub struct TurnItem {
    pub event: TurnEvent,
    pub position: Option<StreamPosition>,
}

/// The events of a turn started with [`Workspace::start_tracked_turn`].
pub struct TurnItems {
    source: TurnItemSource,
    stream: Option<u64>,
}

enum TurnItemSource {
    Plain(mpsc::UnboundedReceiver<TurnEvent>),
    Tracked(mpsc::UnboundedReceiver<TurnItem>),
}

impl TurnItems {
    /// Events from a workspace that does not track positions.
    pub fn untracked(events: mpsc::UnboundedReceiver<TurnEvent>) -> Self {
        Self {
            source: TurnItemSource::Plain(events),
            stream: None,
        }
    }
    pub(crate) fn tracked(stream: u64, items: mpsc::UnboundedReceiver<TurnItem>) -> Self {
        Self {
            source: TurnItemSource::Tracked(items),
            stream: Some(stream),
        }
    }
    /// The workspace stream the events come from, when it tracks positions:
    /// what [`RemoteWorkspace::resume_turn`] takes after a restart.
    pub fn stream(&self) -> Option<u64> {
        self.stream
    }
    /// The next event, or `None` once the turn's events are done.
    pub async fn recv(&mut self) -> Option<TurnItem> {
        match &mut self.source {
            TurnItemSource::Plain(events) => events.recv().await.map(|event| TurnItem {
                event,
                position: None,
            }),
            TurnItemSource::Tracked(items) => items.recv().await,
        }
    }
}

/// The programs git workflows run besides git itself: the GitHub CLI, and
/// the command that drafts commit messages and pull request descriptions
/// (empty for the default).
#[derive(Clone, Debug, Default)]
pub struct GitTools {
    pub gh: PathBuf,
    pub text_generation: Vec<String>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct TurnExit {
    pub success: bool,
    /// Whether the turn ended because it was cancelled.
    pub cancelled: bool,
    /// The exit status as the platform describes it ("exit status: 1").
    pub status: String,
}

#[cfg(feature = "native")]
fn boxed_future<'a, T: Send + 'a>(
    future: impl std::future::Future<Output = T> + Send + 'a,
) -> WorkspaceFuture<'a, T> {
    Box::pin(future)
}
#[cfg(not(feature = "native"))]
fn boxed_future<'a, T: 'a>(
    future: impl std::future::Future<Output = T> + 'a,
) -> WorkspaceFuture<'a, T> {
    Box::pin(future)
}

fn unavailable_reply<'a>(method: String) -> WorkspaceFuture<'a, Reply> {
    let reply = futures_util::future::ready(Err(unavailable(&method)));
    #[cfg(feature = "native")]
    return futures_util::FutureExt::boxed(reply);
    #[cfg(not(feature = "native"))]
    return futures_util::FutureExt::boxed_local(reply);
}

/// The folder of a project started from its title.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NamedProject {
    pub workspace_root: PathBuf,
    /// Why the first commit failed, if it did; the folder is kept.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub commit_error: Option<String>,
}

/// The workspace operations the control plane uses, grouped as the workspace
/// protocol groups them (`fs.*`, `git.*`, `term.*`, `proc.*`/`turn.*`,
/// `preview.*`, `device.*`).
pub trait Workspace: MaybeSendSync {
    // fs.*: `projects.*` and `filesystem.*` file operations.
    fn list_entries<'a>(&'a self, input: &'a Value) -> WorkspaceFuture<'a, Option<Reply>>;
    fn list_ignored_files<'a>(&'a self, input: &'a Value) -> WorkspaceFuture<'a, Option<Reply>>;
    fn search_entries<'a>(&'a self, input: &'a Value) -> WorkspaceFuture<'a, Option<Reply>>;
    fn search_contents<'a>(&'a self, input: &'a Value) -> WorkspaceFuture<'a, Option<Reply>>;
    fn read_file<'a>(&'a self, input: &'a Value) -> WorkspaceFuture<'a, Option<Reply>>;
    fn write_file<'a>(&'a self, input: &'a Value) -> WorkspaceFuture<'a, Option<Reply>>;
    fn file_metadata<'a>(&'a self, input: &'a Value) -> WorkspaceFuture<'a, Option<Reply>>;
    fn browse<'a>(&'a self, input: &'a Value) -> WorkspaceFuture<'a, Option<Reply>>;

    /// The bytes of one of the workspace's own attachments, such as the
    /// pending video a preview recording left where its browser ran: `None`
    /// when the workspace keeps its attachments with the server's, `Err` when
    /// it could not be fetched or is larger than `max_bytes`.
    fn fetch_attachment<'a>(
        &'a self,
        _id: &'a str,
        _max_bytes: u64,
    ) -> WorkspaceFuture<'a, Option<Result<Vec<u8>, String>>> {
        Box::pin(async { None })
    }

    /// Filesystem side of a signed asset. Only native hosts serve these operations in agentd.
    #[cfg(feature = "native")]
    fn prepare_asset(&self, _input: &Value) -> Result<crate::asset_grant::AssetGrant, Value> {
        Err(unavailable("assets.createUrl"))
    }
    #[cfg(feature = "native")]
    fn open_asset<'a>(
        &'a self,
        _claims: &'a Value,
        _name: &'a str,
        _method: &'a axum::http::Method,
        _headers: &'a axum::http::HeaderMap,
    ) -> WorkspaceFuture<'a, axum::response::Response> {
        use axum::response::IntoResponse;
        Box::pin(async { axum::http::StatusCode::NOT_FOUND.into_response() })
    }

    // git.*
    /// A `vcs.*` method. `fetch_upstream` lets a status read start a fetch.
    fn vcs<'a>(
        &'a self,
        method: &'a str,
        input: &'a Value,
        fetch_upstream: bool,
    ) -> WorkspaceFuture<'a, Option<Reply>>;
    /// The status a `subscribeVcsStatus` stream polls: `vcs.refreshStatus`
    /// unless the workspace keeps a fresher reading of its own.
    fn vcs_status<'a>(
        &'a self,
        input: &'a Value,
        fetch_upstream: bool,
        _initial: bool,
    ) -> WorkspaceFuture<'a, Option<Reply>> {
        self.vcs("refreshStatus", input, fetch_upstream)
    }
    /// The branch a `subscribeVcsBranch` stream reports.
    fn vcs_branch<'a>(&'a self, input: &'a Value) -> WorkspaceFuture<'a, Reply>;
    /// A `review.*` method.
    fn review<'a>(&'a self, method: &'a str, input: &'a Value) -> WorkspaceFuture<'a, Reply>;
    /// The tree of `cwd`'s working copy (tracked and untracked files) as a
    /// git tree id, without touching the repository's index.
    fn capture_git_tree<'a>(&'a self, cwd: &'a str) -> WorkspaceFuture<'a, Option<String>>;
    /// `git.resolvePullRequest` or `git.preparePullRequestThread` (`method`
    /// without the `git.` prefix).
    fn pull_request<'a>(
        &'a self,
        method: &'a str,
        input: &'a Value,
        tools: &'a GitTools,
    ) -> WorkspaceFuture<'a, Reply> {
        let _ = (input, tools);
        unavailable_reply(format!("git.{method}"))
    }
    /// `git.runStackedAction`: each progress batch goes to `progress`, and
    /// the reply follows the last one.
    fn stacked_action<'a>(
        &'a self,
        input: &'a Value,
        tools: &'a GitTools,
        progress: mpsc::Sender<Value>,
    ) -> WorkspaceFuture<'a, Reply> {
        let _ = (input, tools, progress);
        unavailable_reply("git.runStackedAction".into())
    }

    // term.*
    /// A `terminal.*` method.
    fn terminal<'a>(
        &'a self,
        method: &'a str,
        input: &'a Value,
    ) -> WorkspaceFuture<'a, Option<Reply>>;
    /// Every terminal's events (snapshots and output), for `terminal.observe`.
    fn terminal_events(&self) -> broadcast::Receiver<Value>;
    fn terminal_metadata(&self) -> Value;
    fn terminal_metadata_events(&self) -> broadcast::Receiver<Value>;

    // proc.* / turn.*
    /// Start a provider turn. Events arrive on the returned channel until
    /// `Exited`; resolving `cancel` stops the process.
    fn start_turn(
        &self,
        spec: TurnSpec,
        cancel: oneshot::Receiver<()>,
    ) -> WorkspaceFuture<'_, anyhow::Result<mpsc::UnboundedReceiver<TurnEvent>>>;
    /// Start a provider turn whose events carry their stream position when
    /// the workspace keeps unacknowledged events. Its caller acknowledges
    /// each positioned event with [`Workspace::ack`] once it has recorded
    /// it; until then the workspace redelivers it after a reconnect.
    fn start_tracked_turn(
        &self,
        spec: TurnSpec,
        cancel: oneshot::Receiver<()>,
    ) -> WorkspaceFuture<'_, anyhow::Result<TurnItems>> {
        let started = self.start_turn(spec, cancel);
        boxed_future(async move { started.await.map(TurnItems::untracked) })
    }
    /// Acknowledge a tracked event and those before it in its stream.
    fn ack(&self, _position: StreamPosition) {}
    /// The models the CLI of `spec`'s provider lists, launched as a turn of
    /// `spec` would launch it: Codex's raw `model/list` entries.
    fn provider_models<'a>(
        &'a self,
        _spec: TurnSpec,
    ) -> WorkspaceFuture<'a, anyhow::Result<Vec<Value>>> {
        boxed_future(async { Err(anyhow::anyhow!("this workspace lists no provider models")) })
    }
    /// Deliver the user's answer to a runtime request of the running turn
    /// `turn_id` ([`TurnSpec::turn_id`]). False when no running turn of this
    /// workspace waits for it.
    fn respond_turn<'a>(
        &'a self,
        _turn_id: &'a str,
        _response: provider::RuntimeResponse,
    ) -> WorkspaceFuture<'a, bool> {
        boxed_future(async { false })
    }
    /// Unload a detached thread's native Codex `thread` from `instance`'s
    /// shared app-server once its turn ended, or close its Claude session's
    /// running CLI, as the reference does.
    fn unload_provider_thread(&self, _instance: &str, _thread: &str) {}
    /// The wakes of provider CLIs this workspace keeps running between
    /// turns ([`provider::ProviderWake`]); taken once. None for a
    /// workspace that reports none.
    fn provider_wakes(&self) -> Option<mpsc::UnboundedReceiver<provider::ProviderWake>> {
        None
    }
    /// The continuation run for `wake` ended without starting: the CLI
    /// drops what it would have shown, and may report its next wake.
    fn retire_provider_wake(&self, _wake: &provider::ProviderWake) {}
    /// Stop the provider processes this workspace shares across turns (the
    /// server is shutting down).
    fn stop_providers(&self) -> WorkspaceFuture<'_, ()> {
        boxed_future(async {})
    }
    /// The local servers a `subscribeDiscoveredLocalServers` stream reports.
    fn local_servers<'a>(&'a self, input: &'a Value) -> WorkspaceFuture<'a, Reply>;
    /// `preview.stopLocalServer`.
    fn stop_local_server<'a>(&'a self, input: &'a Value) -> WorkspaceFuture<'a, Reply>;

    // preview.*
    /// A `preview.*` tab method.
    fn preview<'a>(
        &'a self,
        method: &'a str,
        input: &'a Value,
    ) -> WorkspaceFuture<'a, Option<Reply>>;
    /// `preview.reportProfiles` and `preview.clearProfile`.
    fn preview_profile<'a>(
        &'a self,
        method: &'a str,
        input: &'a Value,
    ) -> WorkspaceFuture<'a, Reply>;
    /// Close the browser pages that match every given filter.
    fn close_preview_pages<'a>(
        &'a self,
        thread: Option<&'a str>,
        tab: Option<&'a str>,
        profile: Option<&'a str>,
    ) -> WorkspaceFuture<'a, ()>;
    fn preview_events(&self) -> broadcast::Receiver<Value>;

    // device.*
    /// A `device.*` method.
    fn device<'a>(
        &'a self,
        method: &'a str,
        input: &'a Value,
    ) -> WorkspaceFuture<'a, Option<Reply>>;
    fn device_state(&self) -> Value;
    fn device_events(&self) -> broadcast::Receiver<Value>;

    // project.*: the directory checks project creation makes.
    fn create_dir_all<'a>(&'a self, path: &'a Path) -> WorkspaceFuture<'a, std::io::Result<()>>;
    fn canonicalize<'a>(&'a self, path: &'a Path) -> WorkspaceFuture<'a, std::io::Result<PathBuf>>;
    fn is_dir<'a>(&'a self, path: &'a Path) -> WorkspaceFuture<'a, bool>;
    /// `projects.createNew`'s folder: a new folder under `root` named after
    /// `name`, holding a git repository whose first commit has a README and
    /// an icon.
    fn create_named_project<'a>(
        &'a self,
        _root: &'a Path,
        _name: &'a str,
    ) -> WorkspaceFuture<'a, std::io::Result<NamedProject>> {
        boxed_future(async {
            Err(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "this workspace cannot create projects",
            ))
        })
    }

    // clone.*: clones that become projects, tracked where git runs.
    /// A clone action (`claim`, `adopt`, `release`, `launch`, `cancel`,
    /// `retry`, `discard`; see [`crate::workspace_protocol::op::CLONE_CALL`]),
    /// or `None` where this workspace does not track clones.
    fn clone_call<'a>(
        &'a self,
        _method: &'a str,
        _input: &'a Value,
    ) -> WorkspaceFuture<'a, Option<Reply>> {
        boxed_future(async { None })
    }
    /// Every tracked clone's snapshot, oldest first (a JSON list).
    fn clone_snapshots(&self) -> Value {
        Value::Array(Vec::new())
    }
    /// The snapshot list after each change.
    fn clone_events(&self) -> broadcast::Receiver<Value> {
        broadcast::channel(1).1
    }
}
