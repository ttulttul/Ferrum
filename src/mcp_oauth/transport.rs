use super::{origin, McpHost};
use axum::{
    body::Bytes,
    extract::State,
    http::{header, HeaderMap, HeaderValue, Method, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use serde_json::{json, Value};
use std::sync::{Arc, LazyLock};

static TOOLS: LazyLock<Value> = LazyLock::new(|| {
    serde_json::from_str(include_str!("project_tools.json")).expect("MCP project tool definitions")
});

fn body(value: Value) -> Response {
    (
        [(header::CONTENT_TYPE, "application/json")],
        format!("{value}\n"),
    )
        .into_response()
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
    let Some(_granted) = granted else {
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
            json!({"jsonrpc":"2.0","id":id,"result":{"protocolVersion":"2025-06-18","capabilities":{"logging":{},"tools":{"listChanged":true},"completions":{}},"serverInfo":{"name":"T3 Code","version":"0.1.10"}}}),
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
        "tools/list" => json!({"tools":*TOOLS}),
        "tools/call" => {
            let name = req["params"]["name"].as_str().unwrap_or("");
            let a = &req["params"]["arguments"];
            if a.get("environmentId")
                .is_some_and(|id| id != s.environment_id())
            {
                return rpc_error(id, -32602, "Environment is unavailable.");
            }
            let store = s.control_state().store.read().await;
            let content = match name {
                "t3_project_list" => {
                    if a.get("cursor").is_some_and(|n| !n.is_u64())
                        || a.get("limit")
                            .is_some_and(|n| n.as_u64().is_none_or(|n| n == 0))
                    {
                        return rpc_error(id, -32602, "Invalid tool arguments");
                    }
                    let cursor = a["cursor"].as_u64().unwrap_or(0) as usize;
                    let limit = a["limit"].as_u64().unwrap_or(20) as usize;
                    let rows = store
                        .project_rows
                        .values()
                        .filter(|r| r["deletedAt"].is_null())
                        .collect::<Vec<_>>();
                    let end = cursor.saturating_add(limit);
                    Ok(
                        json!({"projects":rows.iter().skip(cursor).take(limit).map(|row| s.project_row(row)).collect::<Vec<_>>(),"nextCursor":(end<rows.len()).then_some(end)}),
                    )
                }
                "t3_project_read" => a["projectId"]
                    .as_str()
                    .and_then(|id| store.project_rows.get(id))
                    .filter(|r| r["deletedAt"].is_null())
                    .map(|row| s.project_row(row))
                    .ok_or("The project was not found."),
                _ => return rpc_error(id, -32602, &format!("Tool '{name}' not found")),
            };
            match content {
                Ok(v) => {
                    json!({"content":[{"type":"text","text":v.to_string()}],"structuredContent":v,"isError":false})
                }
                Err(msg) => {
                    json!({"content":[{"type":"text","text":json!({"_tag":"OrchestratorMcpFailure","code":"invalid_request","message":msg}).to_string()}],"isError":true})
                }
            }
        }
        _ => return rpc_error(id, -32601, &format!("Method not found: {operation}")),
    };
    body(json!({"jsonrpc":"2.0","id":id,"result":result}))
}
