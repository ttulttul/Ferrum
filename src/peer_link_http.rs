//! What this environment serves to an environment linked to it, outside `/mcp`, signed in with
//! the link's own MCP session (the reference's `peer/linkThreadHttp.ts` and `peer/http.ts`):
//!
//! - `GET /api/links/thread/stream`, `/history` and `/item`: one of this environment's threads,
//!   as its owner's thread subscription, history pages and item detail give it, so a client of
//!   the linking environment can read it (`linkThreadStream`).
//! - `POST /api/peer-link/client-pairing`: a one-time pairing credential a full-access link asks
//!   for, for a client of the linking environment (`linkClientPairing`), or a link pairing code
//!   for a link from another environment (`linkPairingForLink`).
//!
//! No browser origin is accepted, and only a peer link's session is.
use crate::auth_service::{Session, STANDARD};
use crate::mcp_oauth::{McpAccess, McpHost};
use crate::subscription::{Signal, Subscription};
use crate::AppState;
use axum::{
    body::{Body, Bytes},
    extract::{Query, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

/// The linking environment sends nothing for a while; a comment line keeps proxies from closing
/// the stream (Cloudflare closes idle ones after about 100 seconds).
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(20);
/// How soon an open stream ends after its link is revoked here or the beta is turned off.
const ACCESS_RECHECK_INTERVAL: Duration = Duration::from_secs(30);
/// How long a client pairing credential minted for a linked environment lasts.
const CLIENT_PAIRING_TTL_MS: i64 = 3 * 60 * 1000;
const CLIENT_LABEL_MAX_LENGTH: usize = 80;
/// What a link's MCP client calls itself (`LINK_CLIENT_NAME_PREFIX`).
const LINK_CLIENT_NAME_PREFIX: &str = "T3 Code · ";
/// The scopes a request may name (`AuthEnvironmentScope`): the standard ones and two more.
const KNOWN_SCOPES: &[&str] = &[
    "orchestration:read",
    "orchestration:operate",
    "settings:write",
    "providers:manage",
    "environment:maintain",
    "preview:operate",
    "diagnostics:read",
    "terminal:read",
    "terminal:operate",
    "filesystem:read",
    "filesystem:write",
    "review:write",
    "source-control:write",
    "access:read",
    "access:write",
    "relay:read",
    "relay:write",
    "relay:configure",
];
/// The access a link pairing code may carry (`AuthMcpClientAccess`).
const LINK_ACCESS: &[&str] = &[
    "read-only",
    "approval-required",
    "auto-accept-edits",
    "auto",
    "full-access",
];
const LINKED_ENVIRONMENTS_OFF: &str =
    "Linked environments is off on this machine. Turn it on in Settings → Labs.";

pub fn routes() -> Router<Arc<AppState>> {
    Router::new()
        .route("/api/links/thread/stream", get(stream))
        .route("/api/links/thread/history", get(history))
        .route("/api/links/thread/item", get(item))
        .route("/api/peer-link/client-pairing", post(client_pairing))
}

fn refusal(status: StatusCode, error: &str) -> Response {
    (
        status,
        [("cache-control", "no-store")],
        Json(json!({ "error": error })),
    )
        .into_response()
}

fn bearer(headers: &HeaderMap) -> &str {
    headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.split_once(' '))
        .filter(|(scheme, _)| scheme.eq_ignore_ascii_case("bearer"))
        .map(|(_, token)| token.trim())
        .unwrap_or("")
}

/// The link session `headers` carry, as `/mcp` admits one.
fn mcp_client(state: &AppState, headers: &HeaderMap) -> Option<McpAccess> {
    let token = bearer(headers);
    let issuer = crate::mcp_oauth::request_origin(headers);
    (!token.is_empty())
        .then(|| state.oauth().mcp_access(token, &issuer))
        .flatten()
}

/// A peer link's session admitted to one of this environment's threads.
struct Admitted {
    thread_id: String,
    token: String,
    issuer: String,
    /// When the link's session now ends, renewed by this read.
    link_expires_ms: Option<i64>,
}

/// `admitLink`: no browser origin, the link's own bearer, the Linked environments beta on here,
/// and a thread that exists. Every access level may read, as each reads through `/mcp`.
async fn admit(
    state: &AppState,
    headers: &HeaderMap,
    params: &HashMap<String, String>,
) -> Result<Admitted, Response> {
    if headers.contains_key("origin") {
        return Err(refusal(StatusCode::FORBIDDEN, "origin_not_allowed"));
    }
    let Some(client) = mcp_client(state, headers) else {
        return Err(refusal(StatusCode::UNAUTHORIZED, "unauthorized"));
    };
    if !client.peer_link {
        return Err(refusal(StatusCode::FORBIDDEN, "not_a_link_session"));
    }
    if !state.linked_environments_enabled() {
        return Err(refusal(StatusCode::FORBIDDEN, "linked_environments_off"));
    }
    let Some(thread_id) = params.get("thread").filter(|t| !t.is_empty()) else {
        return Err(refusal(StatusCode::BAD_REQUEST, "invalid_request"));
    };
    let store = state.store.read().await;
    if !store.threads.contains_key(thread_id) {
        return Err(refusal(StatusCode::NOT_FOUND, "thread_not_found"));
    }
    Ok(Admitted {
        thread_id: thread_id.clone(),
        token: bearer(headers).to_owned(),
        issuer: crate::mcp_oauth::request_origin(headers),
        link_expires_ms: client.link_expires_ms,
    })
}

/// The stream's principal: it reads a thread, which no session's scopes change.
fn link_principal() -> Session {
    serde_json::from_value(
        json!({"id": "peer-link", "permissions": [], "method": "peer-link",
        "expires": 0, "owner": false}),
    )
    .expect("a session record")
}

/// One server-sent event per stream item.
fn event(item: &Value) -> Bytes {
    Bytes::from(format!("data: {item}\n\n"))
}

/// `GET /api/links/thread/stream`: the owner's thread subscription with a bounded snapshot, the
/// same items in the same order, until the link stops being admitted or the reader leaves.
async fn stream(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(params): Query<HashMap<String, String>>,
) -> Response {
    let admitted = match admit(&state, &headers, &params).await {
        Ok(admitted) => admitted,
        Err(response) => return response,
    };
    let principal = link_principal();
    let input = json!({"threadId": admitted.thread_id, "acceptBoundedSnapshot": true});
    let opened = match Subscription::open(
        &state.control,
        &principal,
        "orchestration.subscribeThread",
        &input,
        true,
    )
    .await
    {
        Ok(opened) => opened,
        Err(_) => return refusal(StatusCode::INTERNAL_SERVER_ERROR, "thread_read_failed"),
    };
    let (sender, receiver) = tokio::sync::mpsc::channel::<Result<Bytes, std::io::Error>>(64);
    let link_expires_ms = admitted.link_expires_ms;
    let task_state = state.clone();
    tokio::spawn(async move {
        let send = |batch: &Value| {
            batch
                .as_array()
                .into_iter()
                .flatten()
                .map(event)
                .collect::<Vec<_>>()
        };
        for batch in &opened.first {
            for frame in send(batch) {
                if sender.send(Ok(frame)).await.is_err() {
                    return;
                }
            }
        }
        let mut subscription = opened.subscription;
        let Some(mut source) = opened.source else {
            return;
        };
        let mut heartbeat = tokio::time::interval(HEARTBEAT_INTERVAL);
        heartbeat.tick().await;
        let mut recheck = tokio::time::interval(ACCESS_RECHECK_INTERVAL);
        recheck.tick().await;
        loop {
            tokio::select! {
                signal = source.next() => {
                    let Some(signal) = signal else { return };
                    let resync = matches!(signal, Signal::Resync);
                    let batches = subscription.advance(&task_state.control, &principal, signal).await;
                    // A failed read closes the response; the linking environment reconnects.
                    if resync && batches.is_empty() {
                        return;
                    }
                    for batch in &batches {
                        for frame in send(batch) {
                            if sender.send(Ok(frame)).await.is_err() {
                                return;
                            }
                        }
                    }
                }
                _ = heartbeat.tick() => {
                    if sender.send(Ok(Bytes::from_static(b": ping\n\n"))).await.is_err() {
                        return;
                    }
                }
                _ = recheck.tick() => {
                    let still = task_state
                        .oauth()
                        .mcp_access(&admitted.token, &admitted.issuer)
                        .is_some_and(|c| c.peer_link);
                    if !still || !task_state.linked_environments_enabled() {
                        return;
                    }
                }
                _ = sender.closed() => return,
            }
        }
    });
    let body = Body::from_stream(futures_util::stream::unfold(
        receiver,
        |mut receiver| async move { receiver.recv().await.map(|frame| (frame, receiver)) },
    ));
    let mut response = (
        StatusCode::OK,
        [
            ("content-type", "text/event-stream"),
            // no-transform keeps compression from buffering the stream.
            ("cache-control", "no-store, no-transform"),
            ("x-accel-buffering", "no"),
        ],
        body,
    )
        .into_response();
    crate::mcp_oauth::with_link_expiry(&mut response, link_expires_ms);
    response
}

/// `GET /api/links/thread/history`: the turns before a snapshot's window. Rust's snapshot always
/// holds the whole timeline, so it hands out no history cursor and no cursor reads a page.
async fn history(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(params): Query<HashMap<String, String>>,
) -> Response {
    if let Err(response) = admit(&state, &headers, &params).await {
        return response;
    }
    if params.get("cursor").is_none_or(|c| c.is_empty()) {
        return refusal(StatusCode::BAD_REQUEST, "invalid_request");
    }
    refusal(StatusCode::INTERNAL_SERVER_ERROR, "thread_read_failed")
}

/// `GET /api/links/thread/item`: one item in full, as `orchestration.getTurnItem` gives it.
async fn item(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(params): Query<HashMap<String, String>>,
) -> Response {
    let admitted = match admit(&state, &headers, &params).await {
        Ok(admitted) => admitted,
        Err(response) => return response,
    };
    let Some(item_id) = params.get("item").filter(|i| !i.is_empty()) else {
        return refusal(StatusCode::BAD_REQUEST, "invalid_request");
    };
    let store = state.store.read().await;
    let item = store
        .threads
        .get(&admitted.thread_id)
        .and_then(|thread| thread.turn_item_detail(item_id));
    let mut response = (
        StatusCode::OK,
        [("cache-control", "no-store")],
        Json(json!({ "item": item })),
    )
        .into_response();
    crate::mcp_oauth::with_link_expiry(&mut response, admitted.link_expires_ms);
    response
}

fn forbidden(message: &str) -> Response {
    (
        StatusCode::FORBIDDEN,
        [("cache-control", "no-store"), ("pragma", "no-cache")],
        Json(json!({"_tag": "EnvironmentHttpForbiddenError", "message": message})),
    )
        .into_response()
}

/// An owner's session, which may issue any pairing (the full-access caller could run
/// `t3 auth pairing create` here itself).
fn issuing_session() -> Session {
    serde_json::from_value(json!({"id": "peer-link-issuer", "permissions": STANDARD,
        "method": "peer-link", "expires": 0, "owner": true}))
    .expect("a session record")
}

/// `POST /api/peer-link/client-pairing` (`PeerClientPairingIssuer.issue`).
async fn client_pairing(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let no_store = [("cache-control", "no-store"), ("pragma", "no-cache")];
    // The payload is read first, as the reference's endpoint schema does before its handler.
    let Ok(request) = serde_json::from_slice::<Value>(&body) else {
        return StatusCode::BAD_REQUEST.into_response();
    };
    let scopes: Option<Vec<&str>> = request["scopes"].as_array().and_then(|s| {
        s.iter()
            .map(|scope| scope.as_str().filter(|scope| KNOWN_SCOPES.contains(scope)))
            .collect()
    });
    let client_label = request["clientLabel"].as_str().map(str::trim).unwrap_or("");
    let link_access = request
        .get("link")
        .map(|l| l["access"].as_str().filter(|a| LINK_ACCESS.contains(a)));
    if scopes.is_none() || client_label.is_empty() || matches!(link_access, Some(None)) {
        return StatusCode::BAD_REQUEST.into_response();
    }
    // A browser never holds a link's token; refuse one before reading credentials, as `/mcp` does.
    if headers.contains_key("origin") {
        return forbidden("This route does not accept browser requests.");
    }
    let Some(caller) = mcp_client(&state, &headers) else {
        return (
            StatusCode::UNAUTHORIZED,
            no_store,
            Json(json!({"_tag": "EnvironmentHttpUnauthorizedError",
                "message": "Only a linked environment's session is accepted here."})),
        )
            .into_response();
    };
    if !state.linked_environments_enabled() {
        return forbidden(LINKED_ENVIRONMENTS_OFF);
    }
    // A full-access link's agents already run as this machine's user, so minting here adds no
    // power. Any lower link would gain more than it holds, as would one an administrator's
    // ceiling holds below full access.
    let ceiling = state
        .settings
        .policy
        .summary()
        .map(|s| s["limits"]["maxRuntimeMode"].clone())
        .filter(|m| !m.is_null());
    if !caller.peer_link
        || caller.access != "full-access"
        || ceiling.is_some_and(|m| m != "full-access")
    {
        return forbidden("Only a full-access link may pair a client here.");
    }
    if let Some(message) = crate::access_http::AuthHost::pairing_refusal(&*state) {
        return forbidden(&message);
    }
    let via = caller
        .label
        .strip_prefix(LINK_CLIENT_NAME_PREFIX)
        .unwrap_or(&caller.label)
        .to_owned();
    let owner = issuing_session();
    let issued = match link_access {
        Some(Some(access)) => crate::access_http::AuthHost::auth(&*state).pair(
            &owner,
            &json!({"access": access, "purpose": "peer-link", "label": format!("Link · via {via}")}),
        ),
        _ => {
            // Never more than the client holds where it asked, nor more than a pairing code grants.
            let wanted = scopes.unwrap_or_default();
            let granted: Vec<&str> = STANDARD.iter().copied().filter(|s| wanted.contains(s)).collect();
            if granted.is_empty() {
                return forbidden("The client holds no access a pairing could carry.");
            }
            let label: String = client_label.chars().take(CLIENT_LABEL_MAX_LENGTH).collect();
            crate::access_http::AuthHost::auth(&*state).pair_for(
                &owner,
                &json!({"scopes": granted, "label": format!("{label} · via {via}")}),
                CLIENT_PAIRING_TTL_MS,
            )
        }
    };
    match issued {
        Ok(issued) => (
            StatusCode::OK,
            no_store,
            Json(json!({"credential": issued["credential"], "expiresAt": issued["expiresAt"]})),
        )
            .into_response(),
        Err(_) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            no_store,
            Json(json!({"_tag": "EnvironmentHttpInternalServerError",
                "message": "The pairing could not be created."})),
        )
            .into_response(),
    }
}
