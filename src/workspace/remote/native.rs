//! The native host for [`RemoteWorkspace`]: a WebSocket to `t3-agentd`,
//! reconnected with backoff for as long as the link lives.
use super::{RemoteWorkspace, Transport};
use crate::workspace_protocol::{Frame, WireError};
use futures_util::{SinkExt, StreamExt};
use std::{sync::Arc, time::Duration};
use tokio::sync::{mpsc, Notify};
use tokio_tungstenite::tungstenite::{client::IntoClientRequest, Message};

const MAINTAIN_EVERY: Duration = Duration::from_millis(250);
const MAX_BACKOFF: Duration = Duration::from_secs(5);

/// Frames queued for the socket's writer.
struct Outbox(mpsc::UnboundedSender<Frame>);

impl Transport for Outbox {
    fn send(&self, frame: Frame) -> Result<(), WireError> {
        self.0.send(frame).map_err(|_| {
            WireError::new(
                crate::workspace_protocol::ErrorCode::Unavailable,
                "the workspace connection closed",
            )
        })
    }
}

/// A running connection to `t3-agentd`.
pub struct NativeLink {
    task: tokio::task::JoinHandle<()>,
    reconnect: Arc<Notify>,
}

impl NativeLink {
    /// Drop the current socket; the link reconnects and resumes.
    pub fn reconnect(&self) {
        self.reconnect.notify_one();
    }
}

impl Drop for NativeLink {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// Keep `workspace` connected to the agentd at `url` (`ws://host:port/`),
/// presenting `token`. The link stops when dropped.
pub fn connect(workspace: RemoteWorkspace, url: String, token: String) -> NativeLink {
    let reconnect = Arc::new(Notify::new());
    let task = tokio::spawn(run(workspace, url, token, reconnect.clone()));
    NativeLink { task, reconnect }
}

async fn run(workspace: RemoteWorkspace, url: String, token: String, reconnect: Arc<Notify>) {
    let mut backoff = Duration::from_millis(100);
    loop {
        match session(&workspace, &url, &token, &reconnect).await {
            Ok(()) => backoff = Duration::from_millis(100),
            Err(error) => {
                tracing::warn!(%error, %url, "the workspace connection failed");
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(MAX_BACKOFF);
            }
        }
    }
}

/// One connection, from dial to close.
async fn session(
    workspace: &RemoteWorkspace,
    url: &str,
    token: &str,
    reconnect: &Notify,
) -> anyhow::Result<()> {
    let mut request = url.into_client_request()?;
    request
        .headers_mut()
        .insert("authorization", format!("Bearer {token}").parse()?);
    let config = tokio_tungstenite::tungstenite::protocol::WebSocketConfig {
        max_message_size: Some(256 << 20),
        max_frame_size: Some(256 << 20),
        ..Default::default()
    };
    let (socket, _) =
        tokio_tungstenite::connect_async_with_config(request, Some(config), false).await?;
    let (mut sink, mut incoming) = socket.split();
    let (tx, mut rx) = mpsc::unbounded_channel::<Frame>();
    let writer = tokio::spawn(async move {
        while let Some(frame) = rx.recv().await {
            let message = match frame {
                Frame::Text(text) => Message::Text(text),
                Frame::Binary(bytes) => Message::Binary(bytes),
            };
            if sink.send(message).await.is_err() {
                break;
            }
        }
        let _ = sink.close().await;
    });
    let reader = {
        let workspace = workspace.clone();
        tokio::spawn(async move {
            while let Some(Ok(message)) = incoming.next().await {
                let frame = match message {
                    Message::Text(text) => Frame::Text(text),
                    Message::Binary(bytes) => Frame::Binary(bytes),
                    Message::Close(_) => break,
                    _ => continue,
                };
                workspace.receive(frame);
                workspace.flush_acks();
            }
        })
    };
    let result = async {
        workspace.connected(Arc::new(Outbox(tx))).await?;
        tracing::info!(%url, "connected to the workspace");
        let mut reader = reader;
        let mut tick = tokio::time::interval(MAINTAIN_EVERY);
        loop {
            tokio::select! {
                _ = &mut reader => break,
                _ = reconnect.notified() => {
                    reader.abort();
                    break;
                }
                _ = tick.tick() => workspace.maintain().await,
            }
        }
        anyhow::Ok(())
    }
    .await;
    writer.abort();
    workspace.disconnected();
    result
}
