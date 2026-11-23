//! A minimal Agent Client Protocol client: newline-delimited JSON-RPC 2.0 over
//! a CLI's standard input and output (the reference's `AcpSessionRuntime`).
//! Status checks initialize an agent and ask it a question; text generation
//! opens a session, prompts it and reads its `session/update` notifications.
//! The agent's own requests are refused: a permission is cancelled, as text
//! generation grants none, and anything else is not found.
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
    stdin: Arc<tokio::sync::Mutex<tokio::process::ChildStdin>>,
    pending: Pending,
    next_id: i64,
    /// The agent's notifications (method, params), in order, each before
    /// the reply that followed it.
    pub(super) notifications: tokio::sync::mpsc::UnboundedReceiver<(String, Value)>,
}

/// How the agent's own requests are answered.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Requests {
    /// A permission is cancelled, anything else is not found.
    Cancel,
    /// Text generation's refusals: a permission cancelled, an elicitation
    /// declined, a tool (file, terminal, extension) refused; each reported.
    RefuseTools,
}

/// Why a request got no result.
#[derive(Debug)]
pub(super) enum AcpError {
    Spawn,
    Closed,
    Error(Value),
}

impl AcpError {
    /// The JSON-RPC error code of a refused request.
    pub(super) fn code(&self) -> Option<i64> {
        match self {
            Self::Error(error) => error["code"].as_i64(),
            _ => None,
        }
    }
}

impl AcpSession {
    pub(super) fn start(
        binary: &str,
        args: &[String],
        env: &ProcessEnv,
        cwd: &Path,
    ) -> Result<Self, AcpError> {
        Self::start_with(binary, args, env, cwd, Requests::Cancel)
    }

    /// `start`, answering the agent's own requests by `requests`. With
    /// [`Requests::RefuseTools`] every request also arrives as a
    /// `"$request"` notification, and lines of output that are not JSON as
    /// `"$output"` ones.
    pub(super) fn start_with(
        binary: &str,
        args: &[String],
        env: &ProcessEnv,
        cwd: &Path,
        requests: Requests,
    ) -> Result<Self, AcpError> {
        let mut command = super::command(binary, args, env, cwd);
        command
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(if requests == Requests::RefuseTools {
                std::process::Stdio::piped()
            } else {
                std::process::Stdio::null()
            });
        let mut child = command.spawn().map_err(|_| AcpError::Spawn)?;
        let stdin = child.stdin.take().ok_or(AcpError::Spawn)?;
        let stdout = child.stdout.take().ok_or(AcpError::Spawn)?;
        let pending: Pending = Arc::default();
        let reader = pending.clone();
        let stdin = Arc::new(tokio::sync::Mutex::new(stdin));
        let answers = stdin.clone();
        let (notify, notifications) = tokio::sync::mpsc::unbounded_channel();
        let refuse = requests == Requests::RefuseTools;
        if let Some(stderr) = child.stderr.take() {
            let notify = notify.clone();
            tokio::spawn(async move {
                let mut lines = BufReader::new(stderr).lines();
                while let Ok(Some(line)) = lines.next_line().await {
                    let _ = notify.send(("$output".to_owned(), json!(line)));
                }
            });
        }
        tokio::spawn(async move {
            let mut lines = BufReader::new(stdout).lines();
            while let Ok(Some(line)) = lines.next_line().await {
                let Ok(message) = serde_json::from_str::<Value>(&line) else {
                    if refuse {
                        let _ = notify.send(("$output".to_owned(), json!(line)));
                    }
                    continue;
                };
                if let Some(method) = message["method"].as_str() {
                    match message.get("id").filter(|id| !id.is_null()) {
                        Some(id) => {
                            if refuse {
                                let _ = notify.send(("$request".to_owned(), json!(method)));
                            }
                            let reply = if method == "session/request_permission" {
                                json!({"jsonrpc": "2.0", "id": id,
                                    "result": {"outcome": {"outcome": "cancelled"}}})
                            } else if refuse && method.contains("elicitation") {
                                json!({"jsonrpc": "2.0", "id": id, "result": {"action": "decline"}})
                            } else if refuse {
                                json!({"jsonrpc": "2.0", "id": id, "error": {"code": -32601,
                                    "message": "Tools and user input are disabled for text generation."}})
                            } else {
                                json!({"jsonrpc": "2.0", "id": id,
                                    "error": {"code": -32601, "message": "Method not found"}})
                            };
                            let mut line = reply.to_string();
                            line.push('\n');
                            let mut stdin = answers.lock().await;
                            let _ = stdin.write_all(line.as_bytes()).await;
                            let _ = stdin.flush().await;
                        }
                        None => {
                            let _ = notify.send((method.to_owned(), message["params"].clone()));
                        }
                    }
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
            notifications,
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
        {
            let mut stdin = self.stdin.lock().await;
            if stdin.write_all(line.as_bytes()).await.is_err() || stdin.flush().await.is_err() {
                return Err(AcpError::Closed);
            }
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
        self.initialize_with_meta(client_name, None).await
    }

    /// `initialize` with the runtime's `initializeMeta`.
    pub(super) async fn initialize_with_meta(
        &mut self,
        client_name: &str,
        meta: Option<Value>,
    ) -> Result<Value, AcpError> {
        let mut params = json!({
            "protocolVersion": 2,
            "clientCapabilities": {
                "fs": {"readTextFile": false, "writeTextFile": false},
                "terminal": false,
                "session": {"notices": {}, "compaction": {}},
            },
            "capabilities": {},
            "clientInfo": {"name": client_name, "version": "0.0.0"},
            "info": {"name": client_name, "version": "0.0.0"},
        });
        if let Some(meta) = meta {
            params["_meta"] = meta;
        }
        self.request("initialize", params).await
    }
}
