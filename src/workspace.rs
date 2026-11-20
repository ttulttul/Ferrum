//! The workspace plane: everything the control plane asks of the machine that
//! holds the projects. That covers files, git, terminals, provider processes,
//! previews and devices.
//!
//! The control plane (auth, event log, projections, RPC dispatch and
//! settings) reaches the workspace only through [`Workspace`]. Natively,
//! [`LocalWorkspace`] runs every operation in this process. A hosted control
//! plane talks to a workspace daemon over one connection instead.
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
mod remote;
pub use remote::{unavailable, RemoteWorkspace};

/// A reply payload: the success value, or the error value an RPC fails with.
pub type Reply = Result<Value, Value>;

/// A workspace operation in flight. Native workspaces run on a multi-threaded
/// runtime, so their futures are `Send`; a Durable Object's are not.
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
#[derive(Clone, Debug)]
pub struct TurnSpec {
    /// The executable and its leading arguments; the prompt follows them.
    pub command: Vec<String>,
    pub prompt: String,
    pub cwd: String,
    /// Environment variables (matched case-insensitively) the process must
    /// not inherit.
    pub env_remove: Vec<String>,
}

/// What a turn reports, in order. `Exited` is last and follows all output.
#[derive(Clone, Debug, PartialEq)]
pub enum TurnEvent {
    /// One line of standard output, with its line ending.
    Stdout(String),
    /// One line of standard error, with its line ending.
    Stderr(String),
    Exited(TurnExit),
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

fn unavailable_reply<'a>(method: String) -> WorkspaceFuture<'a, Reply> {
    let reply = futures_util::future::ready(Err(unavailable(&method)));
    #[cfg(feature = "native")]
    return futures_util::FutureExt::boxed(reply);
    #[cfg(not(feature = "native"))]
    return futures_util::FutureExt::boxed_local(reply);
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

    // git.*
    /// A `vcs.*` method. `fetch_upstream` lets a status read start a fetch.
    fn vcs<'a>(
        &'a self,
        method: &'a str,
        input: &'a Value,
        fetch_upstream: bool,
    ) -> WorkspaceFuture<'a, Option<Reply>>;
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
    /// The local servers a `subscribeDiscoveredLocalServers` stream reports.
    fn local_servers<'a>(&'a self, input: &'a Value) -> WorkspaceFuture<'a, Reply>;
    /// `preview.stopLocalServer`.
    fn stop_local_server<'a>(&'a self, input: &'a Value) -> WorkspaceFuture<'a, Reply>;

    // preview.*
    /// A `preview.*` tab method.
    fn preview(&self, method: &str, input: &Value) -> Option<Reply>;
    /// `preview.reportProfiles` and `preview.clearProfile`.
    fn preview_profile(&self, method: &str, input: &Value) -> Reply;
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
}
