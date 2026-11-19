//! Public-client OAuth flow for the MCP resource. Authorization codes live for one minute.
use crate::{access_http, AppState};
mod transport;
use axum::{
    extract::{OriginalUri, Query, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Form, Json, Router,
};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD as B64, Engine};
use hmac::{Hmac, Mac};
use rand::{rngs::OsRng, RngCore};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{
    collections::HashMap,
    io::Write,
    path::Path,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::Mutex;
use url::Url;

const SCOPES: [&str; 2] = ["orchestration:read", "orchestration:operate"];
const ACCESS: [&str; 5] = [
    "read-only",
    "approval-required",
    "auto-accept-edits",
    "auto",
    "full-access",
];
const SECRET_HEADERS: [(&str, &str); 2] = [("cache-control", "no-store"), ("pragma", "no-cache")];
const UNKNOWN_CLIENT: &str =
    "This sign-in link names an unknown app. Start the sign-in again from it.";

pub struct OAuth {
    key: [u8; 32],
    sessions: Mutex<HashMap<String, (String, Instant)>>,
    codes: Mutex<HashMap<String, Grant>>,
    tokens: Mutex<HashMap<String, Grant>>,
}

#[derive(Deserialize, Serialize)]
struct Client {
    v: u8,
    n: String,
    r: Vec<String>,
}
#[derive(Deserialize, Default)]
struct Authorization {
    client_id: Option<String>,
    redirect_uri: Option<String>,
    response_type: Option<String>,
    code_challenge_method: Option<String>,
    code_challenge: Option<String>,
    state: Option<String>,
    resource: Option<String>,
}
struct Verified {
    name: String,
    id: String,
    redirect: Url,
    challenge: String,
    state: Option<String>,
    issuer: String,
}
struct Grant {
    request: Verified,
    access: String,
    expires: Instant,
}
enum Invalid {
    Page(&'static str),
    Redirect(String),
}

fn random() -> [u8; 32] {
    let mut bytes = [0; 32];
    OsRng.fill_bytes(&mut bytes);
    bytes
}

impl OAuth {
    pub fn open(dir: &Path) -> anyhow::Result<Self> {
        let dir = dir.join("secrets");
        std::fs::create_dir_all(&dir)?;
        let path = dir.join("mcp-oauth-signing-key.bin");
        if !path.exists() {
            let mut file = tempfile::NamedTempFile::new_in(&dir)?;
            file.write_all(&random())?;
            file.as_file().sync_all()?;
            if let Err(error) = file.persist_noclobber(&path) {
                if error.error.kind() != std::io::ErrorKind::AlreadyExists {
                    return Err(error.into());
                }
            }
        }
        let key = std::fs::read(path)?
            .try_into()
            .map_err(|_| anyhow::anyhow!("invalid MCP OAuth signing key"))?;
        Ok(Self {
            key,
            sessions: Mutex::default(),
            codes: Mutex::default(),
            tokens: Mutex::default(),
        })
    }
    fn mac(&self, payload: &str) -> Hmac<Sha256> {
        let mut mac = Hmac::<Sha256>::new_from_slice(&self.key).expect("32-byte HMAC key");
        mac.update(payload.as_bytes());
        mac
    }
    fn client(&self, id: &str) -> Option<Client> {
        let (body, sig) = id.split_once('.')?;
        self.mac(&format!("mcp-client-id.{body}"))
            .verify_slice(&B64.decode(sig).ok()?)
            .ok()?;
        let client: Client = serde_json::from_slice(&B64.decode(body).ok()?).ok()?;
        (client.v == 1).then_some(client)
    }
    fn validate(&self, req: Authorization, issuer: String) -> Result<Verified, Invalid> {
        let id = req.client_id.unwrap_or_default();
        let client = self.client(&id).ok_or(Invalid::Page(UNKNOWN_CLIENT))?;
        let redirect = req
            .redirect_uri
            .as_deref()
            .and_then(redirect_url)
            .filter(|r| {
                client
                    .r
                    .iter()
                    .filter_map(|u| redirect_url(u))
                    .any(|u| matches_redirect(&u, r))
            })
            .ok_or(Invalid::Page(
                "This sign-in link sends you to an address the app did not register.",
            ))?;
        let fail = |error, description: &str| {
            Invalid::Redirect(redirect_to(
                &redirect,
                &[
                    ("error", Some(error)),
                    ("error_description", Some(description)),
                    ("state", req.state.as_deref()),
                    ("iss", Some(&issuer)),
                ],
            ))
        };
        if req.response_type.as_deref() != Some("code") {
            return Err(fail(
                "unsupported_response_type",
                "Only response_type=code is supported.",
            ));
        }
        let challenge = req.code_challenge.unwrap_or_default();
        if req.code_challenge_method.as_deref() != Some("S256")
            || challenge.len() != 43
            || !challenge.bytes().all(base64_char)
        {
            return Err(fail(
                "invalid_request",
                "PKCE with code_challenge_method=S256 is required.",
            ));
        }
        if !resource_matches(req.resource.as_deref(), &issuer) {
            return Err(fail(
                "invalid_target",
                &format!("This server only issues tokens for {issuer}/mcp."),
            ));
        }
        Ok(Verified {
            name: client.n,
            id,
            redirect,
            challenge,
            state: req.state,
            issuer,
        })
    }
    /// The MCP transport alone may use this session; environment RPCs require an owner session.
    pub async fn mcp_access(&self, token: &str, issuer: &str) -> Option<String> {
        let tokens = self.tokens.lock().await;
        tokens
            .get(token)
            .filter(|g| g.expires > Instant::now() && g.request.issuer == issuer)
            .map(|g| g.access.clone())
    }
}

fn base64_char(c: u8) -> bool {
    c.is_ascii_alphanumeric() || matches!(c, b'-' | b'_')
}
fn redirect_url(s: &str) -> Option<Url> {
    if s.len() > 512 || s.contains('#') {
        return None;
    }
    let u = Url::parse(s).ok()?;
    let allowed = u.scheme() == "https"
        || (u.scheme() == "http"
            && matches!(u.host_str(), Some("127.0.0.1" | "localhost" | "[::1]")));
    (allowed && u.username().is_empty() && u.password().is_none()).then_some(u)
}
fn matches_redirect(a: &Url, b: &Url) -> bool {
    a == b
        || (a.scheme() == "http"
            && b.scheme() == "http"
            && a.host() == b.host()
            && a.path() == b.path()
            && a.query() == b.query())
}
fn resource_matches(value: Option<&str>, issuer: &str) -> bool {
    value.is_none_or(|s| {
        Url::parse(s).is_ok_and(|u| {
            u.fragment().is_none()
                && u.origin().ascii_serialization() == issuer
                && u.path().trim_end_matches('/') == "/mcp"
        })
    })
}
fn origin(headers: &HeaderMap) -> String {
    let host = headers
        .get("host")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("localhost");
    let scheme = if headers
        .get("x-forwarded-proto")
        .is_some_and(|v| v == "https")
    {
        "https"
    } else {
        "http"
    };
    Url::parse(&format!("{scheme}://{host}"))
        .ok()
        .map(|u| u.origin().ascii_serialization())
        .unwrap_or_else(|| "http://localhost".into())
}
fn redirect_to(url: &Url, updates: &[(&str, Option<&str>)]) -> String {
    let mut pairs: Vec<(String, String)> = url.query_pairs().into_owned().collect();
    for (key, val) in updates {
        if let Some(value) = val {
            let mut replaced = false;
            pairs.retain_mut(|(k, v)| {
                if k != key {
                    return true;
                }
                if replaced {
                    return false;
                }
                *v = (*value).into();
                replaced = true;
                true
            });
            if !replaced {
                pairs.push(((*key).into(), (*value).into()))
            }
        }
    }
    let mut result = url.clone();
    result.set_query(None);
    result.query_pairs_mut().extend_pairs(pairs);
    result.into()
}
pub fn routes() -> Router<Arc<AppState>> {
    Router::new()
        .route("/.well-known/oauth-protected-resource", get(resource))
        .route("/mcp", axum::routing::any(transport::handle))
        .route("/.well-known/oauth-protected-resource/mcp", get(resource))
        .route("/.well-known/oauth-authorization-server", get(metadata))
        .route("/oauth/mcp/register", post(register))
        .route("/oauth/mcp/authorize", get(authorize))
        .route("/oauth/mcp/approval", post(approval))
        .route("/oauth/mcp/decision", post(decision))
        .route("/oauth/mcp/token", post(token))
}
async fn resource(h: HeaderMap) -> Json<Value> {
    let i = origin(&h);
    Json(
        json!({"resource":format!("{i}/mcp"),"authorization_servers":[i],"scopes_supported":SCOPES,"bearer_methods_supported":["header"],"resource_name":"T3 Code"}),
    )
}
async fn metadata(h: HeaderMap) -> Json<Value> {
    let i = origin(&h);
    Json(
        json!({"issuer":i,"authorization_endpoint":format!("{i}/oauth/mcp/authorize"),"token_endpoint":format!("{i}/oauth/mcp/token"),"registration_endpoint":format!("{i}/oauth/mcp/register"),"response_types_supported":["code"],"grant_types_supported":["authorization_code"],"code_challenge_methods_supported":["S256"],"token_endpoint_auth_methods_supported":["none"],"scopes_supported":SCOPES,"authorization_response_iss_parameter_supported":true}),
    )
}
fn error(code: &str, desc: &str) -> Response {
    (
        StatusCode::BAD_REQUEST,
        Json(json!({"error":code,"error_description":desc})),
    )
        .into_response()
}
async fn register(State(s): State<Arc<AppState>>, Json(input): Json<Value>) -> Response {
    let uris = input["redirect_uris"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    if !(1..=10).contains(&uris.len()) {
        return error(
            "invalid_redirect_uri",
            "Register between 1 and 10 redirect URIs.",
        );
    }
    if uris
        .iter()
        .any(|u| u.as_str().and_then(redirect_url).is_none())
    {
        return error(
            "invalid_redirect_uri",
            "Redirect URIs must be https, or http on localhost, 127.0.0.1 or [::1].",
        );
    }
    if input["software_id"] == "t3code-peer-link" {
        return error(
            "invalid_client_metadata",
            "Linked environments is off on that machine. Turn it on in its Settings → Labs.",
        );
    }
    let name = input["client_name"]
        .as_str()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .unwrap_or("MCP client")
        .chars()
        .take(100)
        .collect::<String>();
    let body = B64.encode(json!({"v":1,"n":name,"r":uris}).to_string());
    let sig = B64.encode(
        s.oauth
            .mac(&format!("mcp-client-id.{body}"))
            .finalize()
            .into_bytes(),
    );
    let id = format!("{body}.{sig}");
    if uris.len() > 5 && id.len() > 10240 {
        return error(
            "invalid_redirect_uri",
            "Redirect URIs are too long to fit in an authorization request.",
        );
    }
    (StatusCode::CREATED,SECRET_HEADERS,Json(json!({"client_id":id,"client_name":name,"redirect_uris":uris,"grant_types":["authorization_code"],"response_types":["code"],"token_endpoint_auth_method":"none"}))).into_response()
}
async fn authorize(
    State(s): State<Arc<AppState>>,
    h: HeaderMap,
    OriginalUri(uri): OriginalUri,
    Query(req): Query<Authorization>,
) -> Response {
    let location = match s.oauth.validate(req, origin(&h)) {
        Ok(_) => format!(
            "/connect-agent{}",
            uri.query().map(|s| format!("?{s}")).unwrap_or_default()
        ),
        Err(Invalid::Redirect(r)) => r,
        Err(Invalid::Page(desc)) => {
            return (
                StatusCode::BAD_REQUEST,
                [
                    ("content-type", "text/html; charset=utf-8"),
                    (
                        "content-security-policy",
                        "default-src 'none'; style-src 'unsafe-inline'; frame-ancestors 'none'",
                    ),
                    ("x-frame-options", "DENY"),
                    ("cache-control", "no-store"),
                    ("referrer-policy", "no-referrer"),
                    ("x-content-type-options", "nosniff"),
                ],
                include_str!("oauth_error.html").replace("{{description}}", desc),
            )
                .into_response()
        }
    };
    (
        StatusCode::FOUND,
        [
            ("location", location.as_str()),
            ("cache-control", "no-store"),
            ("referrer-policy", "no-referrer"),
        ],
    )
        .into_response()
}
fn approval_error(message: &str) -> Response {
    (
        StatusCode::BAD_REQUEST,
        Json(json!({"_tag":"AuthMcpApprovalError","message":message})),
    )
        .into_response()
}
fn invalid(e: Invalid) -> Response {
    match e {
        Invalid::Page(s) => approval_error(s),
        Invalid::Redirect(r) => Json(json!({"redirectTo":r})).into_response(),
    }
}
async fn csrf(s: &AppState, h: &HeaderMap, v: &Verified) -> Option<Hmac<Sha256>> {
    access_http::authenticate_owner(s, h).await.ok()?;
    let holder = access_http::principal(s, h).ok()?.id;
    Some(s.oauth.mac(&format!(
        "mcp-csrf.{}",
        json!([holder, v.id, v.redirect.as_str(), v.challenge])
    )))
}
async fn approval(
    State(s): State<Arc<AppState>>,
    h: HeaderMap,
    Json(req): Json<Authorization>,
) -> Response {
    let v = match s.oauth.validate(req, origin(&h)) {
        Ok(v) => v,
        Err(e) => return invalid(e),
    };
    let mut out = json!({"clientName":v.name,"redirectHost":v.redirect[url::Position::BeforeHost..url::Position::AfterPort],"environmentHost":Url::parse(&v.issuer).unwrap()[url::Position::BeforeHost..url::Position::AfterPort]});
    if let Some(mac) = csrf(&s, &h, &v).await {
        out["csrfToken"] = json!(B64.encode(mac.finalize().into_bytes()));
        out["oneClickAccess"] = json!(ACCESS);
    }
    (SECRET_HEADERS, Json(out)).into_response()
}
#[derive(Deserialize)]
struct Decision {
    authorization: Authorization,
    decision: Value,
}
async fn decision(
    State(s): State<Arc<AppState>>,
    h: HeaderMap,
    Json(req): Json<Decision>,
) -> Response {
    let v = match s.oauth.validate(req.authorization, origin(&h)) {
        Ok(v) => v,
        Err(e) => return invalid(e),
    };
    let target = match req.decision["_tag"].as_str() {
        Some("deny") => redirect_to(
            &v.redirect,
            &[
                ("error", Some("access_denied")),
                ("state", v.state.as_deref()),
                ("iss", Some(&v.issuer)),
            ],
        ),
        Some(mode @ ("browser-session" | "pairing-code")) => {
            let access = req.decision["access"].as_str().unwrap_or("");
            if mode == "pairing-code" {
                let supplied = req.decision["code"].as_str().unwrap_or("");
                if let Err(reason) = s.auth.consume_approval_code(supplied, access, false) {
                    return approval_error(match reason {
                        "insufficient_scope"=>"That pairing code cannot grant this access. It has not been used, so choose an access level it grants, or create a code with the standard scopes.",
                        "not_a_pairing_code"=>"That is not a one-time pairing code.",
                        "unknown_or_used"=>"That pairing code is unknown, expired, or already used.",
                        _=>"The code could not be checked. Try again.",
                    });
                }
            } else {
                let tag = B64
                    .decode(req.decision["csrfToken"].as_str().unwrap_or(""))
                    .unwrap_or_default();
                if !ACCESS.contains(&access)
                    || csrf(&s, &h, &v)
                        .await
                        .is_none_or(|mac| mac.verify_slice(&tag).is_err())
                {
                    return approval_error(
                        "Your session cannot approve this request. Enter a pairing code instead.",
                    );
                }
            }
            let code = B64.encode(random());
            let target = redirect_to(
                &v.redirect,
                &[
                    ("code", Some(&code)),
                    ("state", v.state.as_deref()),
                    ("iss", Some(&v.issuer)),
                ],
            );
            let mut codes = s.oauth.codes.lock().await;
            codes.retain(|_, g| g.expires > Instant::now());
            codes.insert(
                code,
                Grant {
                    request: v,
                    access: access.into(),
                    expires: Instant::now() + Duration::from_secs(60),
                },
            );
            target
        }
        _ => {
            return approval_error(
                "Your session cannot approve this request. Enter a pairing code instead.",
            )
        }
    };
    (SECRET_HEADERS, Json(json!({"redirectTo":target}))).into_response()
}
async fn token(
    State(s): State<Arc<AppState>>,
    h: HeaderMap,
    Form(form): Form<HashMap<String, String>>,
) -> Response {
    let get = |k: &str| form.get(k).map(String::as_str).unwrap_or_default();
    let fail = |code: &str, msg: &str| (SECRET_HEADERS, error(code, msg)).into_response();
    if get("grant_type") != "authorization_code" {
        return fail(
            "unsupported_grant_type",
            "Only authorization_code is supported.",
        );
    }
    let (code, id, redirect, verifier) = (
        get("code"),
        get("client_id"),
        get("redirect_uri"),
        get("code_verifier"),
    );
    if [code, id, redirect, verifier].contains(&"") {
        return fail(
            "invalid_request",
            "code, redirect_uri, client_id and code_verifier are required.",
        );
    }
    if s.oauth.client(id).is_none() {
        return fail("invalid_client", "The client is unknown.");
    }
    let grant = s.oauth.codes.lock().await.remove(code);
    let issuer = origin(&h);
    let Some(mut grant) = grant.filter(|g| {
        g.expires > Instant::now()
            && g.request.id == id
            && g.request.redirect.as_str() == redirect
            && g.request.issuer == issuer
            && resource_matches(form.get("resource").map(String::as_str), &issuer)
            && (43..=128).contains(&verifier.len())
            && verifier
                .bytes()
                .all(|c| base64_char(c) || matches!(c, b'.' | b'~'))
            && g.request.challenge == B64.encode(Sha256::digest(verifier.as_bytes()))
    }) else {
        return fail(
            "invalid_grant",
            "The authorization code is invalid or expired.",
        );
    };
    let token = B64.encode(random());
    let scope = match grant.access.as_str() {
        "full-access" => {
            "orchestration:read orchestration:operate settings:write source-control:write"
        }
        "read-only" => "orchestration:read",
        _ => "orchestration:read orchestration:operate",
    };
    let ttl = 30 * 24 * 3600;
    grant.expires = Instant::now() + Duration::from_secs(ttl);
    let mut tokens = s.oauth.tokens.lock().await;
    tokens.retain(|_, g| g.expires > Instant::now());
    tokens.insert(token.clone(), grant);
    (
        SECRET_HEADERS,
        Json(json!({"access_token":token,"token_type":"Bearer","expires_in":ttl,"scope":scope})),
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        body::{to_bytes, Body},
        http::Request,
    };
    use tower::ServiceExt;

    #[tokio::test]
    async fn auth_code_requires_session_csrf_and_pkce_and_is_spent_once() {
        let dir = tempfile::tempdir().unwrap();
        let auth = crate::auth_service::AuthService::with_bootstrap("fixture-owner");
        let state = AppState::with_auth(dir.path(), vec![], auth).await.unwrap();
        let app = crate::router(state.clone());
        let call = |path: &str, body: Value, cookie: &str| {
            let mut req = Request::post(path)
                .header("host", "localhost")
                .header("content-type", "application/json");
            if !cookie.is_empty() {
                req = req.header("cookie", cookie)
            }
            app.clone()
                .oneshot(req.body(Body::from(body.to_string())).unwrap())
        };
        let body = async |response: Response| {
            serde_json::from_slice::<Value>(&to_bytes(response.into_body(), 1 << 20).await.unwrap())
                .unwrap()
        };
        let session = call(
            "/api/auth/browser-session",
            json!({"credential":"fixture-owner"}),
            "",
        )
        .await
        .unwrap();
        let cookie = session.headers()["set-cookie"]
            .to_str()
            .unwrap()
            .split(';')
            .next()
            .unwrap()
            .to_owned();
        let client = body(
            call(
                "/oauth/mcp/register",
                json!({"redirect_uris":["http://127.0.0.1:1212/cb"]}),
                "",
            )
            .await
            .unwrap(),
        )
        .await;
        let verifier = "v".repeat(64);
        let authorize = json!({"client_id":client["client_id"],"redirect_uri":"http://127.0.0.1:1313/cb","response_type":"code","state":"abc","code_challenge_method":"S256","code_challenge":B64.encode(Sha256::digest(verifier.as_bytes()))});
        let approved = body(
            call("/oauth/mcp/approval", authorize.clone(), &cookie)
                .await
                .unwrap(),
        )
        .await;
        let decision = json!({"authorization":authorize,"decision":{"_tag":"browser-session","access":"read-only","csrfToken":approved["csrfToken"]}});
        assert_eq!(
            call("/oauth/mcp/decision", decision.clone(), "")
                .await
                .unwrap()
                .status(),
            400
        );
        let redir = body(
            call("/oauth/mcp/decision", decision, &cookie)
                .await
                .unwrap(),
        )
        .await;
        let redir = Url::parse(redir["redirectTo"].as_str().unwrap()).unwrap();
        let code = redir.query_pairs().find(|(k, _)| k == "code").unwrap().1;
        let exchange = || {
            let form = url::form_urlencoded::Serializer::new(String::new())
                .extend_pairs([
                    ("client_id", client["client_id"].as_str().unwrap()),
                    ("code", code.as_ref()),
                    ("redirect_uri", "http://127.0.0.1:1313/cb"),
                    ("grant_type", "authorization_code"),
                    ("code_verifier", &verifier),
                ])
                .finish();
            app.clone().oneshot(
                Request::post("/oauth/mcp/token")
                    .header("host", "localhost")
                    .header("content-type", "application/x-www-form-urlencoded")
                    .body(Body::from(form))
                    .unwrap(),
            )
        };
        let token = body(exchange().await.unwrap()).await;
        assert_eq!(token["scope"], "orchestration:read");
        assert_eq!(
            state
                .oauth
                .mcp_access(token["access_token"].as_str().unwrap(), "http://localhost")
                .await
                .as_deref(),
            Some("read-only")
        );
        assert_eq!(
            body(exchange().await.unwrap()).await["error"],
            "invalid_grant"
        );
        let env_response = app
            .oneshot(
                Request::get("/api/projects")
                    .header(
                        "authorization",
                        format!("Bearer {}", token["access_token"].as_str().unwrap()),
                    )
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(env_response.status(), 401);
    }
}
