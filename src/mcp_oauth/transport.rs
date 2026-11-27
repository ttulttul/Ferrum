use super::{origin, McpHost};
use axum::{
    body::Bytes,
    extract::State,
    http::{header, HeaderMap, HeaderValue, Method, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use serde_json::{json, Value};
use std::sync::Arc;

fn body(value: Value) -> Response {
    (
        [(header::CONTENT_TYPE, "application/json")],
        format!("{value}\n"),
    )
        .into_response()
}
/// Messages streamed as server-sent events: a notification the call raised,
/// then its answer, as the reference streams a call that changed the tool list.
fn events(messages: &[Value]) -> Response {
    let text: String = messages
        .iter()
        .map(|message| format!("event: message\ndata: {message}\n\n"))
        .collect();
    ([(header::CONTENT_TYPE, "text/event-stream")], text).into_response()
}
fn rpc_error(id: &Value, code: i32, message: &str) -> Response {
    body(json!({"jsonrpc":"2.0","id":id,"error":{"code":code,"message":message}}))
}
pub(super) async fn handle<S: McpHost>(
    State(s): State<Arc<S>>,
    h: HeaderMap,
    method: Method,
    bytes: Bytes,
) -> Response {
    if h.contains_key("origin") {
        return StatusCode::FORBIDDEN.into_response();
    }
    let issuer = origin(&h);
    let token = h
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.split_once(' '))
        .filter(|(scheme, _)| scheme.eq_ignore_ascii_case("bearer"))
        .map(|(_, t)| t.trim())
        .unwrap_or("");
    let granted = (!token.is_empty())
        .then(|| s.oauth().mcp_access(token, &issuer))
        .flatten();
    let Some(granted) = granted else {
        let mut fields = Vec::new();
        if token.is_empty() || token.contains('.') {
            fields.push(format!(
                "resource_metadata=\"{issuer}/.well-known/oauth-protected-resource/mcp\""
            ));
        }
        if !token.is_empty() {
            fields.push("error=\"invalid_token\"".into());
        }
        let challenge = if fields.is_empty() {
            "Bearer".into()
        } else {
            format!("Bearer {}", fields.join(", "))
        };
        return (StatusCode::UNAUTHORIZED,[("cache-control","no-store"),("vary","Accept-Encoding"),("www-authenticate",challenge.as_str())],
            Json(json!({"error":"invalid_mcp_credential","message":"A valid T3 Code MCP credential is required."}))).into_response();
    };
    // The Linked environments beta is off here: links wait until it is on again.
    if granted.peer_link && !s.linked_environments_enabled() {
        return (
            StatusCode::FORBIDDEN,
            [("cache-control", "no-store"), ("vary", "Accept-Encoding")],
            Json(json!({"error":"linked_environments_off","message":"Linked environments is off on this machine. Turn it on in its Settings → Labs."})),
        )
            .into_response();
    }
    let forge = super::tools::thread_has_forge(&*s, &granted).await;
    let link_expires_ms = granted.link_expires_ms;
    let mut response: Response = async {
    let sid = h.get("mcp-session-id").and_then(|v| v.to_str().ok());
    if let Some(sid) = sid {
        if !s.oauth().session_held(sid, token) {
            return StatusCode::NOT_FOUND.into_response();
        }
        if method == Method::DELETE {
            s.oauth().close_session(sid);
            return StatusCode::NO_CONTENT.into_response();
        }
    } else if method == Method::DELETE {
        return StatusCode::BAD_REQUEST.into_response();
    }
    if method != Method::POST {
        return StatusCode::METHOD_NOT_ALLOWED.into_response();
    }
    let Ok(req) = serde_json::from_slice::<Value>(&bytes) else {
        return StatusCode::BAD_REQUEST.into_response();
    };
    let id = &req["id"];
    let operation = req["method"].as_str().unwrap_or_default();
    if req["jsonrpc"] != "2.0" {
        return StatusCode::BAD_REQUEST.into_response();
    }
    if operation == "initialize" {
        let Ok(sid) = s.oauth().open_session(token) else {
            return rpc_error(id, -32603, "The session could not be opened.");
        };
        let mut r = body(
            json!({"jsonrpc":"2.0","id":id,"result":{"protocolVersion":"2025-06-18","capabilities":{"logging":{},"tools":{"listChanged":true},"completions":{}},"serverInfo":{"name":"T3 Code","version":"0.1.13"}}}),
        );
        r.headers_mut()
            .insert("mcp-session-id", HeaderValue::from_str(&sid).unwrap());
        return r;
    }
    if sid.is_none() {
        return StatusCode::BAD_REQUEST.into_response();
    }
    if id.is_null() && operation.starts_with("notifications/") {
        return StatusCode::ACCEPTED.into_response();
    }
    let result = match operation {
        "ping" => json!({}),
        "tools/list" => {
            let deferred = h
                .get("t3-tool-exposure")
                .and_then(|v| v.to_str().ok())
                .is_some_and(|v| v == "deferred");
            json!({"tools":super::tools::listed(&*s, &granted, deferred, forge)})
        }
        "tools/call" => {
            let name = req["params"]["name"].as_str().unwrap_or("");
            let a = &req["params"]["arguments"];
            let limit = h
                .get(super::tools::MODE_LIMIT_HEADER)
                .and_then(|v| v.to_str().ok());
            let scope = granted.thread.clone();
            let mut caller = super::tools::Caller::new(granted, limit);
            caller.origin = Some(issuer.clone());
            if let Some(scope) = &scope {
                caller.thread =
                    super::on_this_thread(super::tools::thread_caller(&*s, scope)).await;
                // A deleted thread's credential ends with it.
                if caller.thread.is_none() {
                    s.oauth().revoke_thread_credential(&scope.thread_id);
                }
                if caller.thread.is_none()
                    && super::tools::needs_caller(name)
                    && super::tools::callable(&*s, &caller, name).is_ok()
                {
                    let super::tools::Refusal::Failure(code, msg) =
                        super::tools::calling_thread_missing()
                    else {
                        unreachable!()
                    };
                    return body(
                        json!({"jsonrpc":"2.0","id":id,"result":{"content":[{"type":"text","text":json!({"_tag":"OrchestratorMcpFailure","code":code,"message":msg}).to_string()}],"isError":true}}),
                    );
                }
            }
            let outcome = match super::tools::callable(&*s, &caller, name)
                .and_then(|()| super::tools::unknown_arguments(name, a))
                .and_then(|()| super::tools::linked_target(&*s, name, a))
            {
                Err(refusal) => Err(refusal),
                Ok(Some(target)) => {
                    super::on_this_thread(super::tools::forward(&s, &caller, name, a, target)).await
                }
                Ok(None) => super::on_this_thread(super::tools::call(&s, &caller, name, a)).await,
            };
            match outcome {
                // A search that loaded tools changes this session's tool list.
                Ok(v)
                    if name == "tool_search"
                        && v["tools"].as_array().is_some_and(|t| !t.is_empty()) =>
                {
                    let result = json!({"content":[{"type":"text","text":v.to_string()}],"structuredContent":v,"isError":false});
                    return events(&[
                        json!({"jsonrpc":"2.0","method":"notifications/tools/list_changed","params":{}}),
                        json!({"jsonrpc":"2.0","id":id,"result":result}),
                    ]);
                }
                // A tool whose answer carries image content gives its whole result.
                Ok(mut v) if v.get(super::tools::html::RAW_RESULT).is_some() => {
                    v[super::tools::html::RAW_RESULT].take()
                }
                Ok(v) => {
                    json!({"content":[{"type":"text","text":v.to_string()}],"structuredContent":v,"isError":false})
                }
                Err(super::tools::Refusal::Rpc(code, message)) => {
                    return rpc_error(id, code, &message)
                }
                Err(super::tools::Refusal::Text(message)) => {
                    json!({"content":[{"type":"text","text":message}],"isError":true})
                }
                Err(super::tools::Refusal::Failure(_, msg)) if super::tools::text_errors(name) => {
                    json!({"content":[{"type":"text","text":msg}],"isError":true})
                }
                Err(super::tools::Refusal::Failure(code, msg)) => {
                    let tag = if matches!(name, "t3_worktree_handoff" | "t3_worktree_status") {
                        "WorktreeMcpFailure"
                    } else {
                        "OrchestratorMcpFailure"
                    };
                    json!({"content":[{"type":"text","text":json!({"_tag":tag,"code":code,"message":msg}).to_string()}],"isError":true})
                }
            }
        }
        _ => return rpc_error(id, -32601, &format!("Method not found: {operation}")),
    };
    body(json!({"jsonrpc":"2.0","id":id,"result":result}))
    }
    .await;
    with_link_expiry(&mut response, link_expires_ms);
    response
}

/// A peer link's session renews each time it is used; the answer says when it now ends
/// (`t3-link-expires-at`), so the linking environment can keep its record current.
pub(crate) fn with_link_expiry(response: &mut Response, link_expires_ms: Option<i64>) {
    if let Some(value) = link_expires_ms
        .map(crate::auth_service::iso)
        .and_then(|iso| HeaderValue::from_str(&iso).ok())
    {
        response.headers_mut().insert("t3-link-expires-at", value);
    }
}
