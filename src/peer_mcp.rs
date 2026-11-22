//! Calls a linked environment's T3 MCP tools as this environment's outside
//! agent (`PeerMcpClient` in the reference).
//!
//! Each call carries the calling agent's limits in `T3-Mode-Limit`, so the
//! peer enforces the narrower of those and the link's own access. One MCP
//! session is kept per peer and address and reused across calls; the peer
//! keeps sessions in memory, so after it restarts it answers 404 and a new
//! session is opened for the same call.
use crate::peer_links::{PeerLinkError, PeerLinks};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::Mutex;

/// The only MCP protocol version a T3 Code `/mcp` registers.
const PROTOCOL_VERSION: &str = "2025-06-18";
const MODE_LIMIT_HEADER: &str = "t3-mode-limit";

/// `OrchestratorMcpFailure`'s codes; a peer's refusal keeps its own.
const FAILURE_CODES: [&str; 16] = [
    "capability_denied",
    "parent_not_active",
    "provider_unavailable",
    "model_unavailable",
    "runtime_mode_escalation_denied",
    "interaction_mode_escalation_denied",
    "task_not_found",
    "task_not_cancellable",
    "thread_not_found",
    "run_not_found",
    "thread_not_sendable",
    "thread_not_interruptible",
    "invalid_request",
    "orchestration_error",
    "thread_credential_required",
    "target_required",
];

/// How a forwarded call fails, as the agent here hears it (`OrchestratorMcpFailure`).
pub(crate) type Failure = (&'static str, String);

const LINKED_ENVIRONMENTS_OFF_THERE: &str =
    "The linked environment has Linked environments turned off. Turn it on in its Settings → Labs.";
const STOPPED_ANSWERING: &str = "The linked environment stopped answering.";
const NO_LONGER_ACCEPTED: &str =
    "The linked environment no longer accepts this link. It may have been revoked there; link it again.";

/// How one attempt ended.
enum Ended {
    /// The peer answered with a refusal: the link works, so only this call fails.
    Refused(Failure),
    /// The link or the peer failed; the listing records why.
    Failed(Failure),
    /// The connection failed or timed out; recorded with its own message.
    Transport(String),
}

impl From<reqwest::Error> for Ended {
    fn from(error: reqwest::Error) -> Self {
        Ended::Transport(error.to_string())
    }
}

/// A peer link problem, as the agent that asked hears it.
fn from_link_error(error: PeerLinkError) -> Failure {
    let code = match error.reason {
        "unknown_link" => "invalid_request",
        "expired" | "incompatible" => "capability_denied",
        _ => "orchestration_error",
    };
    (code, error.message)
}

fn failure_code(code: &str) -> Option<&'static str> {
    FAILURE_CODES.iter().copied().find(|c| *c == code)
}

/// The body of an MCP response: JSON, or a single server-sent event carrying JSON.
fn response_json(content_type: Option<&str>, text: &str) -> String {
    if content_type.is_some_and(|t| t.contains("text/event-stream")) {
        text.lines()
            .filter_map(|line| line.strip_prefix("data:"))
            .map(str::trim)
            .collect()
    } else {
        text.to_owned()
    }
}

/// MCP sessions with linked environments, one per peer and address.
#[derive(Default)]
pub(crate) struct PeerMcpClient {
    sessions: Mutex<HashMap<String, String>>,
}

impl PeerMcpClient {
    async fn post(
        links: &PeerLinks,
        url: &str,
        token: &str,
        body: &Value,
        headers: &[(&str, &str)],
    ) -> Result<reqwest::Response, reqwest::Error> {
        let mut request = links
            .http()
            .post(format!("{url}/mcp"))
            .header("accept", "application/json, text/event-stream")
            .header("authorization", format!("Bearer {token}"))
            .header("content-type", "application/json")
            .body(body.to_string());
        for (name, value) in headers {
            request = request.header(*name, *value);
        }
        request.send().await
    }

    async fn open_session(
        links: &PeerLinks,
        url: &str,
        token: &str,
        limit: &str,
    ) -> Result<String, Ended> {
        let handshake = async {
            let initialized = Self::post(
                links,
                url,
                token,
                &json!({
                    "jsonrpc": "2.0",
                    "id": 0,
                    "method": "initialize",
                    "params": {
                        "protocolVersion": PROTOCOL_VERSION,
                        "capabilities": {},
                        "clientInfo": {"name": "t3code-peer", "version": "1"},
                    },
                }),
                &[(MODE_LIMIT_HEADER, limit)],
            )
            .await?;
            let status = initialized.status().as_u16();
            let session = initialized
                .headers()
                .get("mcp-session-id")
                .and_then(|v| v.to_str().ok())
                .map(str::to_owned);
            if status == 403 {
                return Err(Ended::Failed((
                    "orchestration_error",
                    LINKED_ENVIRONMENTS_OFF_THERE.into(),
                )));
            }
            let Some(session) = session.filter(|_| status == 200) else {
                return Err(Ended::Failed(if status == 401 {
                    ("capability_denied", NO_LONGER_ACCEPTED.into())
                } else {
                    (
                        "orchestration_error",
                        format!("The linked environment refused to start an MCP session (HTTP {status})."),
                    )
                }));
            };
            Self::post(
                links,
                url,
                token,
                &json!({"jsonrpc": "2.0", "method": "notifications/initialized"}),
                &[
                    (MODE_LIMIT_HEADER, limit),
                    ("mcp-session-id", &session),
                    ("mcp-protocol-version", PROTOCOL_VERSION),
                ],
            )
            .await?;
            Ok(session)
        };
        // Only the handshake is bounded: a forwarded tool call may wait for minutes.
        tokio::time::timeout(crate::peer_links::probe_timeout(url), handshake)
            .await
            .unwrap_or_else(|_| Err(Ended::Transport("The MCP handshake timed out.".into())))
    }

    /// Calls `tool` in the linked environment `environment_id` with
    /// `arguments`, limited to `limit` (`<runtime>/<interaction>`).
    pub(crate) async fn call(
        &self,
        links: &PeerLinks,
        environment_id: &str,
        tool: &str,
        arguments: Value,
        limit: &str,
    ) -> Result<Value, Failure> {
        let ended = self
            .attempt(links, environment_id, tool, arguments, limit)
            .await;
        match ended {
            Ok(value) => {
                links.record_outcome(environment_id, None);
                Ok(value)
            }
            Err(Ended::Refused(failure)) => {
                links.record_outcome(environment_id, None);
                Err(failure)
            }
            Err(Ended::Failed(failure)) => {
                links.record_outcome(environment_id, Some(&failure.1));
                Err(failure)
            }
            Err(Ended::Transport(message)) => {
                links.record_outcome(environment_id, Some(&message));
                Err(("orchestration_error", STOPPED_ANSWERING.into()))
            }
        }
    }

    async fn attempt(
        &self,
        links: &PeerLinks,
        environment_id: &str,
        tool: &str,
        arguments: Value,
        limit: &str,
    ) -> Result<Value, Ended> {
        let resolved = links
            .resolve(environment_id)
            .await
            .map_err(|e| Ended::Failed(from_link_error(e)))?;
        let key = format!("{environment_id} {}", resolved.url);
        let body = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/call",
            "params": {"name": tool, "arguments": arguments},
        });
        let call_once = |session: String| {
            let body = &body;
            let resolved = &resolved;
            async move {
                Self::post(
                    links,
                    &resolved.url,
                    &resolved.token,
                    body,
                    &[
                        (MODE_LIMIT_HEADER, limit),
                        ("mcp-session-id", &session),
                        ("mcp-protocol-version", PROTOCOL_VERSION),
                    ],
                )
                .await
            }
        };
        let existing = self.sessions.lock().unwrap().get(&key).cloned();
        let session = match existing {
            Some(session) => session,
            None => {
                let session =
                    Self::open_session(links, &resolved.url, &resolved.token, limit).await?;
                self.sessions
                    .lock()
                    .unwrap()
                    .insert(key.clone(), session.clone());
                session
            }
        };
        let mut response = call_once(session).await?;
        if response.status().as_u16() == 404 {
            // The peer restarted and forgot the session: start a new one for this call.
            let renewed = Self::open_session(links, &resolved.url, &resolved.token, limit).await?;
            self.sessions.lock().unwrap().insert(key, renewed.clone());
            response = call_once(renewed).await?;
        }
        match response.status().as_u16() {
            200 => {}
            401 => {
                return Err(Ended::Failed((
                    "capability_denied",
                    NO_LONGER_ACCEPTED.into(),
                )))
            }
            403 => {
                return Err(Ended::Failed((
                    "orchestration_error",
                    LINKED_ENVIRONMENTS_OFF_THERE.into(),
                )))
            }
            status => {
                return Err(Ended::Failed((
                    "orchestration_error",
                    format!("The linked environment answered HTTP {status}."),
                )))
            }
        }
        let content_type = response
            .headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .map(str::to_owned);
        let text = response.text().await?;
        let unexpected = || {
            Ended::Failed((
                "orchestration_error",
                "The linked environment answered in an unexpected shape.".into(),
            ))
        };
        let body: Value = serde_json::from_str(&response_json(content_type.as_deref(), &text))
            .map_err(|_| unexpected())?;
        if let Some(error) = body.get("error").filter(|e| !e.is_null()) {
            let message = error["message"].as_str().ok_or_else(unexpected)?;
            if !error["code"].is_number() {
                return Err(unexpected());
            }
            return Err(Ended::Refused(("invalid_request", message.to_owned())));
        }
        let Some(result) = body.get("result").filter(|r| !r.is_null()) else {
            return Err(Ended::Failed((
                "invalid_request",
                "The linked environment returned no result.".into(),
            )));
        };
        let content = result["content"].as_array().ok_or_else(unexpected)?;
        let text: String = content
            .iter()
            .map(|part| part["text"].as_str().unwrap_or_default())
            .collect();
        if result["isError"] == true {
            // A refusal keeps the peer's own code, so the agent sees why it was refused.
            let refused = serde_json::from_str::<Value>(&text).ok().and_then(|v| {
                Some((
                    failure_code(v["code"].as_str()?)?,
                    v["message"].as_str()?.to_owned(),
                ))
            });
            return Err(Ended::Refused(refused.unwrap_or_else(|| {
                (
                    "orchestration_error",
                    if text.is_empty() {
                        "The linked environment refused the call.".into()
                    } else {
                        text
                    },
                )
            })));
        }
        serde_json::from_str::<Value>(&text)
            .ok()
            .filter(|answer| crate::mcp_oauth::answer_matches(tool, answer))
            .ok_or_else(|| {
            Ended::Failed((
                "orchestration_error",
                "The linked environment's answer did not match this version. Update both environments.".into(),
            ))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_event_stream_body_is_its_data_lines() {
        assert_eq!(
            response_json(
                Some("text/event-stream"),
                "event: message\ndata: {\"a\":\ndata: 1}\n\n"
            ),
            "{\"a\":1}"
        );
        assert_eq!(response_json(Some("application/json"), "{}"), "{}");
    }

    #[test]
    fn link_errors_map_to_failure_codes() {
        let code = |reason| from_link_error(PeerLinkError::new(reason, "m")).0;
        assert_eq!(code("unknown_link"), "invalid_request");
        assert_eq!(code("expired"), "capability_denied");
        assert_eq!(code("unreachable"), "orchestration_error");
    }
}
