//! The reference's ACP MCP commands (`AcpMcpStdioBridge`), which ACP agents
//! run from this executable:
//!
//! - `acp-mcp-bridge` relays MCP JSON-RPC between the agent, on standard
//!   input and output, and the thread's MCP endpoint over HTTP. ACP requires
//!   agents to support stdio MCP servers, whereas HTTP support varies, so an
//!   ACP session is given T3's tools this way.
//! - `acp-mcp-call <tool> <arguments-json|->` calls one tool and prints its
//!   result: the terminal fallback for an agent that accepts the MCP server
//!   but never shows its tools to the model.
//!
//! Both read the endpoint and its `Authorization` header from
//! `T3_ACP_MCP_ENDPOINT` and `T3_ACP_MCP_AUTHORIZATION`. Messages are relayed
//! verbatim.
use serde_json::{json, Value};
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt};
use tokio::sync::Mutex;

const MCP_PROTOCOL_VERSION: &str = "2025-06-18";

/// Run `command` (`acp-mcp-bridge` or `acp-mcp-call`) with `args`; the
/// process's exit code.
pub fn run(command: &str, args: &[String]) -> i32 {
    let (Ok(endpoint), Ok(authorization)) = (
        std::env::var("T3_ACP_MCP_ENDPOINT"),
        std::env::var("T3_ACP_MCP_AUTHORIZATION"),
    ) else {
        eprintln!("{command} requires T3_ACP_MCP_ENDPOINT and T3_ACP_MCP_AUTHORIZATION.");
        return 2;
    };
    let runtime = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(error) => {
            eprintln!("{command}: {error}");
            return 1;
        }
    };
    let endpoint = Endpoint {
        url: endpoint,
        authorization,
        client: reqwest::Client::new(),
        session: Mutex::default(),
    };
    if command == "acp-mcp-bridge" {
        runtime.block_on(bridge(Arc::new(endpoint)));
        return 0;
    }
    runtime.block_on(call(endpoint, args))
}

/// The thread's MCP endpoint and the session it opened.
struct Endpoint {
    url: String,
    authorization: String,
    client: reqwest::Client,
    /// `mcp-session-id` and `mcp-protocol-version`, once known.
    session: Mutex<(Option<String>, Option<String>)>,
}

impl Endpoint {
    /// POST one message; the JSON-RPC messages the response carries (a JSON
    /// body, or the `data` of each event of an event stream). Requests the
    /// server makes on the way are answered (`ping`) or refused when
    /// `answer_server` is set.
    async fn post(&self, body: String, answer_server: bool) -> Result<Vec<Value>, String> {
        let (session, version) = self.session.lock().await.clone();
        let mut request = self
            .client
            .post(&self.url)
            .header("content-type", "application/json")
            .header("accept", "application/json, text/event-stream")
            .header("authorization", &self.authorization);
        if let Some(session) = &session {
            request = request.header("mcp-session-id", session);
        }
        if let Some(version) = &version {
            request = request.header("mcp-protocol-version", version);
        }
        let response = request.body(body).send().await.map_err(|e| e.to_string())?;
        if let Some(id) = response
            .headers()
            .get("mcp-session-id")
            .and_then(|value| value.to_str().ok())
        {
            self.session.lock().await.0 = Some(id.to_owned());
        }
        let status = response.status();
        if !status.is_success() {
            return Err(format!(
                "T3 Code MCP endpoint responded with HTTP {}.",
                status.as_u16()
            ));
        }
        let stream = response
            .headers()
            .get("content-type")
            .and_then(|value| value.to_str().ok())
            .is_some_and(|kind| kind.starts_with("text/event-stream"));
        let text = response.text().await.map_err(|e| e.to_string())?;
        let mut payloads = Vec::new();
        let mut push = |value: Value| match value {
            Value::Array(entries) => payloads.extend(entries),
            other => payloads.push(other),
        };
        if stream {
            for event in text.split("\n\n") {
                let data: Vec<&str> = event
                    .lines()
                    .filter_map(|line| line.strip_prefix("data:"))
                    .map(|data| data.strip_prefix(' ').unwrap_or(data))
                    .collect();
                if let Ok(value) = serde_json::from_str::<Value>(&data.join("\n")) {
                    push(value);
                }
            }
        } else if let Ok(value) = serde_json::from_str::<Value>(&text) {
            push(value);
        }
        let mut out = Vec::new();
        for payload in payloads {
            if let Some(version) = payload["result"]["protocolVersion"]
                .as_str()
                .filter(|version| !version.is_empty())
            {
                self.session.lock().await.1 = Some(version.to_owned());
            }
            if answer_server && payload.get("id").is_some() && payload["method"].is_string() {
                let reply = if payload["method"] == "ping" {
                    json!({"jsonrpc": "2.0", "id": payload["id"], "result": {}})
                } else {
                    json!({"jsonrpc": "2.0", "id": payload["id"],
                        "error": {"code": -32601, "message": "Method not found"}})
                };
                let _ = Box::pin(self.post(reply.to_string(), true)).await;
                continue;
            }
            out.push(payload);
        }
        Ok(out)
    }

    /// End the MCP session, if one opened.
    async fn terminate(&self) {
        let (session, version) = self.session.lock().await.clone();
        let Some(session) = session else {
            return;
        };
        let mut request = self
            .client
            .delete(&self.url)
            .header("accept", "application/json, text/event-stream")
            .header("authorization", &self.authorization)
            .header("mcp-session-id", session);
        if let Some(version) = version {
            request = request.header("mcp-protocol-version", version);
        }
        let _ = request.send().await;
    }
}

/// Write one message as a line to standard output.
async fn write_line(out: &Mutex<tokio::io::Stdout>, message: &Value) {
    let mut line = message.to_string();
    line.push('\n');
    let mut out = out.lock().await;
    let _ = out.write_all(line.as_bytes()).await;
    let _ = out.flush().await;
}

/// Relay each line of standard input to the endpoint and what it answers to
/// standard output, until input ends and every relayed message was answered.
/// The session's handshake goes first; the calls after it run concurrently.
async fn bridge(endpoint: Arc<Endpoint>) {
    let out = Arc::new(Mutex::new(tokio::io::stdout()));
    let mut lines = tokio::io::BufReader::new(tokio::io::stdin()).lines();
    let mut running = tokio::task::JoinSet::new();
    while let Ok(Some(line)) = lines.next_line().await {
        if line.trim().is_empty() {
            continue;
        }
        let Ok(envelope) = serde_json::from_str::<Value>(&line) else {
            write_line(
                &out,
                &json!({"jsonrpc": "2.0", "id": null, "error": {"code": -32700, "message": "Parse error"}}),
            )
            .await;
            continue;
        };
        if !envelope.is_object() {
            continue;
        }
        let response = envelope.get("id").is_some()
            && (envelope.get("result").is_some() || envelope.get("error").is_some());
        let session_setup = !response
            && matches!(
                envelope["method"].as_str(),
                Some("initialize" | "notifications/initialized")
            );
        if session_setup {
            // The session's establishment is relayed in order, before
            // anything after it.
            forward(&endpoint, &out, line, &envelope).await;
            continue;
        }
        let (endpoint, out) = (endpoint.clone(), out.clone());
        running.spawn(async move { forward(&endpoint, &out, line, &envelope).await });
    }
    while running.join_next().await.is_some() {}
}

async fn forward(
    endpoint: &Endpoint,
    out: &Mutex<tokio::io::Stdout>,
    line: String,
    envelope: &Value,
) {
    match endpoint.post(line, false).await {
        Ok(payloads) => {
            for payload in payloads {
                write_line(out, &payload).await;
            }
        }
        Err(error) => {
            if let Some(id) = envelope.get("id") {
                let message = if error.starts_with("T3 Code MCP endpoint") {
                    error
                } else {
                    format!("T3 Code MCP bridge request failed: {error}")
                };
                write_line(
                    out,
                    &json!({"jsonrpc": "2.0", "id": id, "error": {"code": -32603, "message": message}}),
                )
                .await;
            }
        }
    }
}

/// `acp-mcp-call`: one tool call in its own MCP session.
async fn call(endpoint: Endpoint, args: &[String]) -> i32 {
    let (Some(tool), Some(arguments)) = (args.first(), args.get(1)) else {
        eprintln!("acp-mcp-call requires <tool> and <arguments-json>, or - to read it from stdin.");
        return 2;
    };
    // Windows PowerShell 5.1 strips the quotes from JSON passed as an
    // argument; standard input keeps them.
    let text = if arguments == "-" {
        let mut text = String::new();
        let _ = tokio::io::stdin().read_to_string(&mut text).await;
        text
    } else {
        arguments.clone()
    };
    let Ok(parsed) = serde_json::from_str::<Value>(&text) else {
        eprintln!("acp-mcp-call arguments must be valid JSON.");
        return 2;
    };
    if !parsed.is_object() {
        eprintln!("acp-mcp-call arguments must be a JSON object.");
        return 2;
    }
    let result = call_tool(&endpoint, tool, parsed).await;
    endpoint.terminate().await;
    match result {
        Ok(mut result) => {
            // Terminal callers see both fields, unlike native MCP clients,
            // which choose one: the text copy of structured content goes.
            if let Some(structured) = result.get("structuredContent").filter(|v| !v.is_null()) {
                let encoded = structured.to_string();
                if let Some(content) = result["content"].as_array_mut() {
                    content.retain(|part| {
                        !(part["type"] == "text" && part["text"] == encoded.as_str())
                    });
                }
            }
            println!("{result}");
            0
        }
        Err(error) => {
            eprintln!("{error}");
            1
        }
    }
}

async fn call_tool(endpoint: &Endpoint, tool: &str, arguments: Value) -> Result<Value, String> {
    let request = |id: &str, method: &str, params: Value| {
        json!({"jsonrpc": "2.0", "id": id, "method": method, "params": params}).to_string()
    };
    let answer = |payloads: Vec<Value>, id: &str, method: &str| -> Result<Value, String> {
        let envelope = payloads.into_iter().find(|entry| entry["id"] == id);
        match envelope {
            Some(envelope) if envelope.get("error").is_none() => Ok(envelope["result"].clone()),
            Some(envelope) => Err(format!(
                "T3 Code MCP {method} failed: {}",
                envelope["error"]
            )),
            None => Err(format!("T3 Code MCP {method} failed.")),
        }
    };
    let payloads = endpoint
        .post(
            request(
                "t3-cli-0",
                "initialize",
                json!({"protocolVersion": MCP_PROTOCOL_VERSION, "capabilities": {},
                    "clientInfo": {"name": "t3-code-acp-cli", "version": "0.0.0"}}),
            ),
            true,
        )
        .await?;
    answer(payloads, "t3-cli-0", "initialize")?;
    endpoint
        .post(
            json!({"jsonrpc": "2.0", "method": "notifications/initialized"}).to_string(),
            true,
        )
        .await?;
    let payloads = endpoint
        .post(
            request(
                "t3-cli-1",
                "tools/call",
                json!({"name": tool, "arguments": arguments}),
            ),
            true,
        )
        .await?;
    answer(payloads, "t3-cli-1", "tools/call")
}
