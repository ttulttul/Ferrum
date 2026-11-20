//! The workspace on this machine (native): every operation runs in-process
//! over the local filesystem, git, PTYs and child processes.
use super::{GitTools, Reply, TurnEvent, TurnExit, TurnSpec, Workspace, WorkspaceFuture};
use crate::local_services::{
    discovery, files, git_workflow, review, DeviceService, PreviewService, TerminalService,
    VcsService,
};
use futures_util::FutureExt;
use serde_json::Value;
use std::path::{Path, PathBuf};
use tokio::{
    io::{AsyncBufReadExt, BufReader},
    process::Command,
    sync::{broadcast, mpsc, oneshot},
};

pub struct LocalWorkspace {
    /// The server's state directory; host services keep their files under it.
    pub(crate) data_dir: PathBuf,
    pub(crate) vcs: VcsService,
    pub(crate) terminals: TerminalService,
    pub(crate) previews: PreviewService,
    pub(crate) devices: DeviceService,
}

impl LocalWorkspace {
    pub(crate) fn new(
        data_dir: PathBuf,
        terminals: TerminalService,
        previews: PreviewService,
        devices: DeviceService,
    ) -> Self {
        Self {
            data_dir,
            vcs: VcsService,
            terminals,
            previews,
            devices,
        }
    }
}

impl Workspace for LocalWorkspace {
    fn list_entries<'a>(&'a self, input: &'a Value) -> WorkspaceFuture<'a, Option<Reply>> {
        files::list_entries(input).boxed()
    }
    fn list_ignored_files<'a>(&'a self, input: &'a Value) -> WorkspaceFuture<'a, Option<Reply>> {
        files::list_ignored_files(input).boxed()
    }
    fn search_entries<'a>(&'a self, input: &'a Value) -> WorkspaceFuture<'a, Option<Reply>> {
        files::search_entries(input).boxed()
    }
    fn search_contents<'a>(&'a self, input: &'a Value) -> WorkspaceFuture<'a, Option<Reply>> {
        files::search_contents(input).boxed()
    }
    fn read_file<'a>(&'a self, input: &'a Value) -> WorkspaceFuture<'a, Option<Reply>> {
        files::read_file(input).boxed()
    }
    fn write_file<'a>(&'a self, input: &'a Value) -> WorkspaceFuture<'a, Option<Reply>> {
        files::write_file(input).boxed()
    }
    fn file_metadata<'a>(&'a self, input: &'a Value) -> WorkspaceFuture<'a, Option<Reply>> {
        files::metadata(input).boxed()
    }
    fn browse<'a>(&'a self, input: &'a Value) -> WorkspaceFuture<'a, Option<Reply>> {
        files::browse(input).boxed()
    }

    fn vcs<'a>(
        &'a self,
        method: &'a str,
        input: &'a Value,
        fetch_upstream: bool,
    ) -> WorkspaceFuture<'a, Option<Reply>> {
        self.vcs
            .call_with(method, input, &self.data_dir, fetch_upstream)
            .boxed()
    }
    fn vcs_branch<'a>(&'a self, input: &'a Value) -> WorkspaceFuture<'a, Reply> {
        self.vcs.branch_status(input).boxed()
    }
    fn review<'a>(&'a self, method: &'a str, input: &'a Value) -> WorkspaceFuture<'a, Reply> {
        review::call(method, input).boxed()
    }
    fn capture_git_tree<'a>(&'a self, cwd: &'a str) -> WorkspaceFuture<'a, Option<String>> {
        capture_git_tree(cwd).boxed()
    }
    fn pull_request<'a>(
        &'a self,
        method: &'a str,
        input: &'a Value,
        tools: &'a GitTools,
    ) -> WorkspaceFuture<'a, Reply> {
        git_workflow::pull_request(self, tools, method, input).boxed()
    }
    fn stacked_action<'a>(
        &'a self,
        input: &'a Value,
        tools: &'a GitTools,
        progress: mpsc::Sender<Value>,
    ) -> WorkspaceFuture<'a, Reply> {
        git_workflow::stream(tools, input, progress).boxed()
    }

    fn terminal<'a>(
        &'a self,
        method: &'a str,
        input: &'a Value,
    ) -> WorkspaceFuture<'a, Option<Reply>> {
        self.terminals.call(method, input).boxed()
    }
    fn terminal_events(&self) -> broadcast::Receiver<Value> {
        self.terminals.subscribe()
    }
    fn terminal_metadata(&self) -> Value {
        self.terminals.metadata_snapshot()
    }
    fn terminal_metadata_events(&self) -> broadcast::Receiver<Value> {
        self.terminals.subscribe_metadata()
    }

    fn start_turn(
        &self,
        spec: TurnSpec,
        cancel: oneshot::Receiver<()>,
    ) -> WorkspaceFuture<'_, anyhow::Result<mpsc::UnboundedReceiver<TurnEvent>>> {
        async move { start_turn(spec, cancel) }.boxed()
    }
    fn local_servers<'a>(&'a self, input: &'a Value) -> WorkspaceFuture<'a, Reply> {
        discovery::scan(&self.terminals, input).boxed()
    }
    fn stop_local_server<'a>(&'a self, input: &'a Value) -> WorkspaceFuture<'a, Reply> {
        discovery::stop(&self.terminals, input).boxed()
    }

    fn preview(&self, method: &str, input: &Value) -> Option<Reply> {
        self.previews.call(method, input)
    }
    fn preview_profile(&self, method: &str, input: &Value) -> Reply {
        self.previews.profile(method, input, &self.data_dir)
    }
    fn close_preview_pages<'a>(
        &'a self,
        thread: Option<&'a str>,
        tab: Option<&'a str>,
        profile: Option<&'a str>,
    ) -> WorkspaceFuture<'a, ()> {
        self.previews.close_pages(thread, tab, profile).boxed()
    }
    fn preview_events(&self) -> broadcast::Receiver<Value> {
        self.previews.subscribe()
    }

    fn device<'a>(
        &'a self,
        method: &'a str,
        input: &'a Value,
    ) -> WorkspaceFuture<'a, Option<Reply>> {
        self.devices.call(method, input, &self.data_dir).boxed()
    }
    fn device_state(&self) -> Value {
        self.devices.snapshot(&self.data_dir)
    }
    fn device_events(&self) -> broadcast::Receiver<Value> {
        self.devices.subscribe()
    }

    fn create_dir_all<'a>(&'a self, path: &'a Path) -> WorkspaceFuture<'a, std::io::Result<()>> {
        tokio::fs::create_dir_all(path).boxed()
    }
    fn canonicalize<'a>(&'a self, path: &'a Path) -> WorkspaceFuture<'a, std::io::Result<PathBuf>> {
        tokio::fs::canonicalize(path).boxed()
    }
    fn is_dir<'a>(&'a self, path: &'a Path) -> WorkspaceFuture<'a, bool> {
        async move { path.is_dir() }.boxed()
    }
}

/// Spawn the turn's process and forward its output lines, then its exit.
fn start_turn(
    spec: TurnSpec,
    mut cancel: oneshot::Receiver<()>,
) -> anyhow::Result<mpsc::UnboundedReceiver<TurnEvent>> {
    let mut command = spec.command;
    let executable = command.remove(0);
    let mut child_command = Command::new(executable);
    for (k, _) in std::env::vars_os() {
        let name = k.to_string_lossy();
        if spec
            .env_remove
            .iter()
            .any(|removed| name.eq_ignore_ascii_case(removed))
        {
            child_command.env_remove(k);
        }
    }
    let mut child = child_command
        .args(command)
        .arg(spec.prompt)
        .current_dir(&spec.cwd)
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()?;
    let (events, receiver) = mpsc::unbounded_channel();
    let mut output_tasks = Vec::new();
    for (reader, stdout) in [
        (
            child
                .stdout
                .take()
                .map(|r| BufReader::new(Box::new(r) as Box<_>)),
            true,
        ),
        (
            child
                .stderr
                .take()
                .map(|r| BufReader::new(Box::new(r) as Box<_>)),
            false,
        ),
    ] {
        let Some(mut reader): Option<BufReader<Box<dyn tokio::io::AsyncRead + Send + Unpin>>> =
            reader
        else {
            continue;
        };
        let events = events.clone();
        output_tasks.push(tokio::spawn(async move {
            let mut line = String::new();
            while reader.read_line(&mut line).await.unwrap_or(0) > 0 {
                let text = std::mem::take(&mut line);
                let _ = events.send(if stdout {
                    TurnEvent::Stdout(text)
                } else {
                    TurnEvent::Stderr(text)
                });
            }
        }));
    }
    tokio::spawn(async move {
        let exit = async {
            let (status, cancelled) = tokio::select! {
                result = child.wait() => (result?, false),
                _ = &mut cancel => {
                    child.kill().await?;
                    (child.wait().await?, true)
                }
            };
            for task in output_tasks {
                let _ = task.await;
            }
            std::io::Result::Ok(TurnExit {
                success: status.success(),
                cancelled,
                status: status.to_string(),
            })
        };
        if let Ok(exit) = exit.await {
            let _ = events.send(TurnEvent::Exited(exit));
        }
    });
    Ok(receiver)
}

/// Write the working copy into a scratch index and return its tree id.
pub(crate) async fn capture_git_tree(cwd: &str) -> Option<String> {
    let cwd = std::path::Path::new(cwd);
    if !cwd.is_dir() {
        return None;
    }
    let probe = Command::new("git")
        .args(["-C", cwd.to_str()?, "rev-parse", "--show-toplevel"])
        .output()
        .await
        .ok()?;
    if !probe.status.success() {
        return None;
    }
    let index = tempfile::NamedTempFile::new().ok()?;
    let index_path = index.path().to_path_buf();
    drop(index);
    let _ = tokio::fs::remove_file(&index_path).await;
    let index_text = index_path.to_str()?.to_owned();
    let mut command = Command::new("git");
    command
        .args(["-C", cwd.to_str()?, "read-tree", "HEAD"])
        .env("GIT_INDEX_FILE", &index_text);
    if !command.status().await.ok()?.success() {
        let _ = tokio::fs::remove_file(&index_path).await;
        return None;
    }
    let mut add = Command::new("git");
    add.args(["-C", cwd.to_str()?, "add", "-A", "--"])
        .arg(".")
        .env("GIT_INDEX_FILE", &index_text);
    if !add.status().await.ok()?.success() {
        let _ = tokio::fs::remove_file(&index_path).await;
        return None;
    }
    let output = Command::new("git")
        .args(["-C", cwd.to_str()?, "write-tree"])
        .env("GIT_INDEX_FILE", &index_text)
        .output()
        .await
        .ok()?;
    let _ = tokio::fs::remove_file(&index_path).await;
    output
        .status
        .success()
        .then(|| String::from_utf8_lossy(&output.stdout).trim().to_owned())
}
