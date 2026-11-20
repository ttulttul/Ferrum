//! The workspace as a hosted control plane sees it: a daemon in another
//! process, reached over one connection. The wire protocol is not wired up
//! yet, so every operation answers as unavailable, with the defect the
//! hosted control plane gives for workspace RPCs today.
use super::{Reply, TurnEvent, TurnSpec, Workspace, WorkspaceFuture};
use futures_util::FutureExt;
use serde_json::{json, Value};
use std::{
    io,
    path::{Path, PathBuf},
};
use tokio::sync::{broadcast, mpsc, oneshot};

/// A workspace daemon that is not connected.
pub struct RemoteWorkspace {
    // Held so subscriptions stay open rather than ending at once.
    events: broadcast::Sender<Value>,
}

impl Default for RemoteWorkspace {
    fn default() -> Self {
        Self {
            events: broadcast::channel(1).0,
        }
    }
}

/// The defect a workspace RPC answers with: the unavailable-contract message
/// the hosted control plane's dispatcher gives.
pub fn unavailable(method: &str) -> Value {
    json!({
        "_tag": "Die",
        "defect": format!("RPC contract is registered but unavailable in the control plane: {method}"),
    })
}

fn io_unavailable() -> io::Error {
    io::Error::new(io::ErrorKind::Unsupported, "the workspace is not connected")
}

#[cfg(feature = "native")]
fn ready<'a, T: Send + 'a>(value: T) -> WorkspaceFuture<'a, T> {
    futures_util::future::ready(value).boxed()
}
#[cfg(not(feature = "native"))]
fn ready<'a, T: 'a>(value: T) -> WorkspaceFuture<'a, T> {
    futures_util::future::ready(value).boxed_local()
}

fn reply<'a>(method: &str) -> WorkspaceFuture<'a, Reply> {
    ready(Err(unavailable(method)))
}
fn routed<'a>(method: &str) -> WorkspaceFuture<'a, Option<Reply>> {
    ready(Some(Err(unavailable(method))))
}

impl Workspace for RemoteWorkspace {
    fn list_entries<'a>(&'a self, _: &'a Value) -> WorkspaceFuture<'a, Option<Reply>> {
        routed("projects.listEntries")
    }
    fn list_ignored_files<'a>(&'a self, _: &'a Value) -> WorkspaceFuture<'a, Option<Reply>> {
        routed("projects.listIgnoredFiles")
    }
    fn search_entries<'a>(&'a self, _: &'a Value) -> WorkspaceFuture<'a, Option<Reply>> {
        routed("projects.searchEntries")
    }
    fn search_contents<'a>(&'a self, _: &'a Value) -> WorkspaceFuture<'a, Option<Reply>> {
        routed("projects.searchContents")
    }
    fn read_file<'a>(&'a self, _: &'a Value) -> WorkspaceFuture<'a, Option<Reply>> {
        routed("projects.readFile")
    }
    fn write_file<'a>(&'a self, _: &'a Value) -> WorkspaceFuture<'a, Option<Reply>> {
        routed("projects.writeFile")
    }
    fn file_metadata<'a>(&'a self, _: &'a Value) -> WorkspaceFuture<'a, Option<Reply>> {
        routed("filesystem.getMetadata")
    }
    fn browse<'a>(&'a self, _: &'a Value) -> WorkspaceFuture<'a, Option<Reply>> {
        routed("filesystem.browse")
    }

    fn vcs<'a>(
        &'a self,
        method: &'a str,
        _: &'a Value,
        _: bool,
    ) -> WorkspaceFuture<'a, Option<Reply>> {
        routed(&format!("vcs.{method}"))
    }
    fn vcs_branch<'a>(&'a self, _: &'a Value) -> WorkspaceFuture<'a, Reply> {
        reply("subscribeVcsBranch")
    }
    fn review<'a>(&'a self, method: &'a str, _: &'a Value) -> WorkspaceFuture<'a, Reply> {
        reply(&format!("review.{method}"))
    }
    fn capture_git_tree<'a>(&'a self, _: &'a str) -> WorkspaceFuture<'a, Option<String>> {
        ready(None)
    }

    fn terminal<'a>(&'a self, method: &'a str, _: &'a Value) -> WorkspaceFuture<'a, Option<Reply>> {
        routed(&format!("terminal.{method}"))
    }
    fn terminal_events(&self) -> broadcast::Receiver<Value> {
        self.events.subscribe()
    }
    fn terminal_metadata(&self) -> Value {
        json!({"terminals":[]})
    }
    fn terminal_metadata_events(&self) -> broadcast::Receiver<Value> {
        self.events.subscribe()
    }

    fn start_turn(
        &self,
        _: TurnSpec,
        _: oneshot::Receiver<()>,
    ) -> WorkspaceFuture<'_, anyhow::Result<mpsc::UnboundedReceiver<TurnEvent>>> {
        ready(Err(anyhow::anyhow!("the workspace is not connected")))
    }
    fn local_servers<'a>(&'a self, _: &'a Value) -> WorkspaceFuture<'a, Reply> {
        reply("subscribeDiscoveredLocalServers")
    }
    fn stop_local_server<'a>(&'a self, _: &'a Value) -> WorkspaceFuture<'a, Reply> {
        reply("preview.stopLocalServer")
    }

    fn preview(&self, method: &str, _: &Value) -> Option<Reply> {
        Some(Err(unavailable(&format!("preview.{method}"))))
    }
    fn preview_profile(&self, method: &str, _: &Value) -> Reply {
        Err(unavailable(&format!("preview.{method}")))
    }
    fn close_preview_pages<'a>(
        &'a self,
        _: Option<&'a str>,
        _: Option<&'a str>,
        _: Option<&'a str>,
    ) -> WorkspaceFuture<'a, ()> {
        ready(())
    }
    fn preview_events(&self) -> broadcast::Receiver<Value> {
        self.events.subscribe()
    }

    fn device<'a>(&'a self, method: &'a str, _: &'a Value) -> WorkspaceFuture<'a, Option<Reply>> {
        routed(&format!("device.{method}"))
    }
    fn device_state(&self) -> Value {
        Value::Null
    }
    fn device_events(&self) -> broadcast::Receiver<Value> {
        self.events.subscribe()
    }

    fn create_dir_all<'a>(&'a self, _: &'a Path) -> WorkspaceFuture<'a, io::Result<()>> {
        ready(Err(io_unavailable()))
    }
    fn canonicalize<'a>(&'a self, _: &'a Path) -> WorkspaceFuture<'a, io::Result<PathBuf>> {
        ready(Err(io_unavailable()))
    }
    fn is_dir<'a>(&'a self, _: &'a Path) -> WorkspaceFuture<'a, bool> {
        ready(false)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn every_rpc_answers_unavailable() {
        let workspace = RemoteWorkspace::default();
        let input = json!({"cwd":"/workspace"});
        assert_eq!(
            workspace.read_file(&input).await,
            Some(Err(unavailable("projects.readFile")))
        );
        assert_eq!(
            workspace.vcs("refreshStatus", &input, true).await,
            Some(Err(unavailable("vcs.refreshStatus")))
        );
        assert_eq!(
            unavailable("vcs.refreshStatus")["defect"],
            "RPC contract is registered but unavailable in the control plane: vcs.refreshStatus"
        );
        assert!(workspace
            .start_turn(
                TurnSpec {
                    command: vec!["true".into()],
                    prompt: String::new(),
                    cwd: "/".into(),
                    env_remove: vec![],
                },
                oneshot::channel().1,
            )
            .await
            .is_err());
    }
}
