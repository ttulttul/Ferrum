//! A minimal Agent Client Protocol client for status checks: newline-delimited
//! JSON-RPC 2.0 over a CLI's standard input and output, enough to initialize
//! an agent and ask it a question without opening a session (the reference's
//! `AcpSessionRuntime` `initialize` and `request`).
use super::ProcessEnv;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::sync::oneshot;

type Pending = Arc<Mutex<HashMap<i64, oneshot::Sender<Result<Value, Value>>>>>;

pub(super) struct AcpSession {
    _child: tokio::process::Child,
    stdin: tokio::process::ChildStdin,
    pending: Pending,
    next_id: i64,
}

/// Why a request got no result.
#[derive(Debug)]
pub(super) enum AcpError {
    Spawn,
    Closed,
    Error(#[allow(dead_code)] Value),
}

impl AcpSession {
    pub(super) fn start(
        binary: &str,
        args: &[String],
        env: &ProcessEnv,
        cwd: &Path,
    ) -> Result<Self, AcpError> {
        let mut command = super::command(binary, args, env, cwd);
        command
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::null());
        let mut child = command.spawn().map_err(|_| AcpError::Spawn)?;
        let stdin = child.stdin.take().ok_or(AcpError::Spawn)?;
        let stdout = child.stdout.take().ok_or(AcpError::Spawn)?;
        let pending: Pending = Arc::default();
        let reader = pending.clone();
        tokio::spawn(async move {
            let mut lines = BufReader::new(stdout).lines();
            while let Ok(Some(line)) = lines.next_line().await {
                let Ok(message) = serde_json::from_str::<Value>(&line) else {
                    continue;
                };
                if message.get("method").is_some() {
                    continue;
                }
                let Some(id) = message["id"].as_i64() else {
                    continue;
                };
                let sender = reader.lock().unwrap_or_else(|e| e.into_inner()).remove(&id);
                if let Some(sender) = sender {
                    let reply = match message.get("error") {
                        Some(error) => Err(error.clone()),
                        None => Ok(message["result"].clone()),
                    };
                    let _ = sender.send(reply);
                }
            }
            // The agent closed its output: no pending request will be answered.
            reader.lock().unwrap_or_else(|e| e.into_inner()).clear();
        });
        Ok(Self {
            _child: child,
            stdin,
            pending,
            next_id: 0,
        })
    }

    pub(super) async fn request(&mut self, method: &str, params: Value) -> Result<Value, AcpError> {
        let id = self.next_id;
        self.next_id += 1;
        let (sender, receiver) = oneshot::channel();
        self.pending
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(id, sender);
        let mut line =
            json!({"jsonrpc": "2.0", "id": id, "method": method, "params": params}).to_string();
        line.push('\n');
        if self.stdin.write_all(line.as_bytes()).await.is_err() || self.stdin.flush().await.is_err()
        {
            return Err(AcpError::Closed);
        }
        match receiver.await {
            Ok(Ok(result)) => Ok(result),
            Ok(Err(error)) => Err(AcpError::Error(error)),
            Err(_) => Err(AcpError::Closed),
        }
    }

    /// The reference's `initialize` with no client capabilities beyond the
    /// runtime's defaults.
    pub(super) async fn initialize(&mut self, client_name: &str) -> Result<Value, AcpError> {
        self.request(
            "initialize",
            json!({
                "protocolVersion": 2,
                "clientCapabilities": {
                    "fs": {"readTextFile": false, "writeTextFile": false},
                    "terminal": false,
                    "session": {"notices": {}, "compaction": {}},
                },
                "clientInfo": {"name": client_name, "version": "0.0.0"},
            }),
        )
        .await
    }
}
