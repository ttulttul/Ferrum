use crate::{
    auth_error,
    auth_service::{Session, STANDARD},
    trace_id, AppState,
};
use axum::{
    body::to_bytes,
    extract::{Path, Request, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Form, Json, Router,
};
use serde::Deserialize;
use serde_json::{json, Value};
use std::{collections::HashMap, sync::Arc};

pub(crate) mod connect;
mod dpop;
mod guest;
pub(crate) use dpop::{contextualize, reason as proof_failure, uses_proof};
pub fn routes() -> Router<Arc<AppState>> {
    Router::new()
        .merge(guest::routes())
        .merge(connect::routes())
        .route("/api/auth/session", get(session))
        .route("/api/auth/browser-session", post(browser_session))
        .route("/oauth/token", post(exchange))
        .route("/api/auth/websocket-ticket", post(ticket))
        .route("/api/auth/websocket-ticket/:use", post(ticket_for))
        .route("/api/auth/pairing-token", post(admin))
        .route("/api/auth/pairing-links/revoke", post(admin))
        .route("/api/auth/clients/revoke", post(admin))
        .route("/api/auth/clients/revoke-others", post(admin))
        .route("/api/auth/connect-devices/approve", post(admin))
        .route("/api/auth/connect-devices/revoke", post(admin))
}
fn header<'a>(h: &'a HeaderMap, name: &str) -> Option<&'a str> {
    h.get(name)?.to_str().ok()
}
fn cookie<'a>(h: &'a HeaderMap, name: &str) -> Option<&'a str> {
    header(h, "cookie")?.split(';').find_map(|pair| {
        let (k, v) = pair.trim().split_once('=')?;
        (k == name && !v.is_empty()).then_some(v)
    })
}
fn credential<'a>(state: &AppState, h: &'a HeaderMap) -> Result<&'a str, &'static str> {
    if let Some(token) = cookie(h, &state.auth.session_cookie_name()) {
        if let Some(origin) = header(h, "origin") {
            let matches_host = url::Url::parse(origin).ok().is_some_and(|url| {
                let host = &url[url::Position::BeforeHost..url::Position::AfterPort];
                !host.is_empty() && header(h, "host").is_some_and(|h| h.eq_ignore_ascii_case(host))
            });
            if !matches_host
                && header(h, "sec-fetch-site") != Some("same-origin")
                && !matches!(origin, "t3code-dev://app" | "t3code://app")
            {
                return Err("invalid_credential");
            }
        }
        if state
            .auth
            .principal(token)
            .is_some_and(|s| s.proof_key.is_some())
        {
            return Err("invalid_proof");
        }
        return Ok(token);
    }
    let authorization = header(h, "authorization").ok_or("missing_credential")?;
    let (scheme, value) = authorization.split_once(' ').ok_or("missing_credential")?;
    if scheme.eq_ignore_ascii_case("DPoP") {
        let t = value.trim();
        let holder = state.auth.principal(t).ok_or("invalid_credential")?;
        let binding = holder.proof_key.as_deref().ok_or("invalid_proof")?;
        dpop::authenticate(&state.auth, header(h, "dpop").unwrap_or(""), binding, t)?;
        return Ok(t);
    }
    if scheme.eq_ignore_ascii_case("Bearer") && !value.trim().is_empty() {
        if state
            .auth
            .principal(value.trim())
            .is_some_and(|s| s.proof_key.is_some())
        {
            return Err("invalid_proof");
        }
        Ok(value.trim())
    } else {
        Err("missing_credential")
    }
}
pub fn principal(state: &AppState, headers: &HeaderMap) -> Result<Session, &'static str> {
    state
        .auth
        .principal(credential(state, headers)?)
        .ok_or("invalid_credential")
}
pub async fn authenticate(state: &AppState, headers: &HeaderMap) -> Result<(), &'static str> {
    principal(state, headers)?
        .unrestricted()
        .then_some(())
        .ok_or("invalid_credential")
}
pub async fn authenticate_owner(state: &AppState, h: &HeaderMap) -> Result<(), &'static str> {
    let s = principal(state, h)?;
    (s.owner && s.unrestricted())
        .then_some(())
        .ok_or("invalid_credential")
}
pub fn scope_error(headers: &HeaderMap, scope: &str) -> Response {
    let legacy = crate::auth_service::permissions::legacy_scope(scope);
    (StatusCode::FORBIDDEN,Json(json!({"_tag":"EnvironmentScopeRequiredError","code":"insufficient_scope","requiredScope":legacy,"requiredPermission":scope,"traceId":trace_id(headers)}))).into_response()
}
fn auth_failure(h: &HeaderMap, e: &str) -> Response {
    if e == "connect_approval_pending" {
        return (StatusCode::FORBIDDEN,Json(json!({"_tag":"EnvironmentConnectApprovalPendingError","code":e,"traceId":trace_id(h)}))).into_response();
    }
    let (status, tag, code) = match e {
        "invalid_scope" => (
            StatusCode::BAD_REQUEST,
            "EnvironmentRequestInvalidError",
            "invalid_request",
        ),
        "current_session_revoke_not_allowed" => (
            StatusCode::FORBIDDEN,
            "EnvironmentOperationForbiddenError",
            "operation_forbidden",
        ),
        "internal_error" => (
            StatusCode::INTERNAL_SERVER_ERROR,
            "EnvironmentInternalError",
            "internal_error",
        ),
        _ => return auth_error(h, e),
    };
    (
        status,
        Json(json!({"_tag":tag,"code":code,"reason":e,"traceId":trace_id(h)})),
    )
        .into_response()
}
const SECRET_HEADERS: [(&str, &str); 2] = [("cache-control", "no-store"), ("pragma", "no-cache")];
async fn session(State(state): State<Arc<AppState>>, h: HeaderMap) -> Json<Value> {
    Json(state.auth.session_state(credential(&state, &h).ok()).await)
}
#[derive(Deserialize)]
struct Bootstrap {
    credential: String,
}
async fn browser_session(
    State(state): State<Arc<AppState>>,
    h: HeaderMap,
    body: Result<Json<Bootstrap>, axum::extract::rejection::JsonRejection>,
) -> Response {
    let Ok(Json(body)) = body else {
        return StatusCode::BAD_REQUEST.into_response();
    };
    let Some(credential) = Some(body.credential.trim()).filter(|s| !s.is_empty()) else {
        return StatusCode::BAD_REQUEST.into_response();
    };
    let name = state.auth.session_cookie_name();
    let previous = cookie(&h, &name);
    match state.auth.browser_session(credential, previous) {
        Ok((v, token)) => {
            let exp = state.auth.principal(&token).unwrap().expires;
            let date=time::OffsetDateTime::from_unix_timestamp(exp/1000).unwrap().format(time::macros::format_description!("[weekday repr:short], [day] [month repr:short] [year] [hour]:[minute]:[second] GMT")).unwrap();
            let secure = if header(&h, "x-forwarded-proto") == Some("https") {
                "; Secure"
            } else {
                ""
            };
            let cookie =
                format!("{name}={token}; Path=/; Expires={date}; HttpOnly{secure}; SameSite=Lax");
            (SECRET_HEADERS, [("set-cookie", cookie)], Json(v)).into_response()
        }
        Err(e) => auth_failure(&h, e),
    }
}
async fn exchange(
    State(s): State<Arc<AppState>>,
    h: HeaderMap,
    Form(input): Form<HashMap<String, String>>,
) -> Response {
    const BOOT: &str = "urn:t3:params:oauth:token-type:environment-bootstrap";
    const ACCESS: &str = "urn:ietf:params:oauth:token-type:access_token";
    let field = |k: &str| input.get(k).map(String::as_str).unwrap_or("");
    if field("grant_type") != "urn:ietf:params:oauth:grant-type:token-exchange"
        || !matches!(field("subject_token_type"), BOOT | ACCESS)
        || field("requested_token_type") != ACCESS
        || field("subject_token").trim().is_empty()
    {
        return StatusCode::BAD_REQUEST.into_response();
    }
    let scopes = input
        .get("scope")
        .map(|v| v.split_whitespace().map(str::to_owned).collect::<Vec<_>>());
    if scopes.as_ref().is_some_and(|scopes| {
        scopes.is_empty() || scopes.iter().any(|p| !STANDARD.contains(&p.as_str()))
    }) {
        return auth_failure(&h, "invalid_scope");
    }
    let renew = field("subject_token_type") == ACCESS;
    let result = if let Some(token) = header(&h, "dpop") {
        match dpop::check(token, None) {
            Ok(checked) => s
                .auth
                .exchange_bound(field("subject_token"), scopes, &checked, renew),
            Err(e) => return auth_failure(&h, e),
        }
    } else if renew {
        let mut r = auth_error(&h, "invalid_credential");
        r.headers_mut()
            .insert("www-authenticate", "DPoP".parse().unwrap());
        return r;
    } else {
        s.auth.exchange(field("subject_token"), scopes)
    };
    match result {
        Ok(v) => (SECRET_HEADERS, Json(v)).into_response(),
        Err(e) => auth_failure(&h, e),
    }
}
async fn ticket(State(s): State<Arc<AppState>>, h: HeaderMap) -> Response {
    issue_ticket(&s, &h, "any").await
}
async fn ticket_for(
    State(s): State<Arc<AppState>>,
    h: HeaderMap,
    Path(kind): Path<String>,
) -> Response {
    if let Err(e) = principal(&s, &h) {
        return auth_error(&h, e);
    }
    if !matches!(kind.as_str(), "rpc" | "media") {
        return StatusCode::BAD_REQUEST.into_response();
    }
    issue_ticket(&s, &h, &kind).await
}
async fn issue_ticket(s: &AppState, h: &HeaderMap, kind: &str) -> Response {
    let token = match credential(s, h) {
        Ok(v) => v,
        Err(e) => return auth_failure(h, e),
    };
    match s.auth.create_websocket_ticket(token, kind).await {
        Ok(v) => (SECRET_HEADERS, Json(v)).into_response(),
        Err(e) => auth_failure(h, e),
    }
}
async fn admin(State(state): State<Arc<AppState>>, req: Request) -> Response {
    let (parts, body) = req.into_parts();
    let p = match principal(&state, &parts.headers) {
        Ok(p) => p,
        Err(e) => return auth_error(&parts.headers, e),
    };
    if !p.permissions.iter().any(|s| s == "access:write") {
        return scope_error(&parts.headers, "access:write");
    }
    if parts.uri.path() == "/api/auth/clients/revoke-others" {
        return match state.auth.revoke_others(&p) {
            Ok(n) => Json(json!({"revokedCount":n})).into_response(),
            Err(e) => auth_failure(&parts.headers, e),
        };
    }
    let json: Value = match to_bytes(body, 64 * 1024)
        .await
        .ok()
        .and_then(|bytes| serde_json::from_slice(&bytes).ok())
    {
        Some(v) if Value::is_object(&v) => v,
        _ => return StatusCode::BAD_REQUEST.into_response(),
    };
    let field = |k: &str| {
        json.get(k)
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|v| !v.is_empty())
    };
    let result = match parts.uri.path() {
        "/api/auth/pairing-token" => state.auth.pair(&p, &json),
        "/api/auth/pairing-links/revoke" => match field("id") {
            Some(id) => state.auth.revoke_pairing(id).map(|v| json!({"revoked":v})),
            None => return StatusCode::BAD_REQUEST.into_response(),
        },
        "/api/auth/clients/revoke" => match field("sessionId") {
            Some(id) => state
                .auth
                .revoke_client(&p, id)
                .map(|v| json!({"revoked":v})),
            None => return StatusCode::BAD_REQUEST.into_response(),
        },
        "/api/auth/connect-devices/approve" => match field("pairingLinkId") {
            Some(id) => state.auth.approve_device(id).map(|v| json!({"approved":v})),
            None => return StatusCode::BAD_REQUEST.into_response(),
        },
        "/api/auth/connect-devices/revoke" => match field("proofKeyThumbprint") {
            Some(id) => state.auth.revoke_device(id).map(|v| json!({"revoked":v})),
            None => return StatusCode::BAD_REQUEST.into_response(),
        },
        _ => return StatusCode::BAD_REQUEST.into_response(),
    };
    match result {
        Ok(v) => Json(v).into_response(),
        Err(e) => auth_failure(&parts.headers, e),
    }
}

pub(crate) fn authenticate_ticket(
    s: &AppState,
    h: &HeaderMap,
    uri: &axum::http::Uri,
    ticket: &str,
    kind: &str,
) -> Result<Session, &'static str> {
    s.auth.check_ticket(ticket, kind, |binding| {
        let query = url::form_urlencoded::parse(uri.query().unwrap_or("").as_bytes())
            .find(|(k, _)| k == "dpop")
            .map(|(_, v)| v);
        let proof = header(h, "dpop")
            .or(query.as_deref())
            .ok_or("invalid_proof")?;
        dpop::authenticate(&s.auth, proof, binding, ticket)
    })
}
