use super::{header, principal, scope_error, SECRET_HEADERS};
use crate::{
    auth_error,
    auth_service::{iso, now, storage::Storage},
    environment_http, AppState,
};
use axum::{
    body::to_bytes,
    extract::{Request, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use base64::{
    engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD},
    Engine,
};
use jsonwebtoken::{decode, decode_header, Algorithm, DecodingKey, Validation};
use ring::{
    rand::SystemRandom,
    signature::{Ed25519KeyPair, KeyPair},
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::{collections::HashMap, path::Path, sync::Arc, time::Duration};
use uuid::Uuid;

#[derive(Clone, Default, Serialize, Deserialize)]
struct Data {
    config: Option<Config>,
    publish: bool,
    hold: bool,
    replays: HashMap<String, i64>,
    key: Vec<u8>,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Config {
    relay_url: String,
    relay_issuer: Option<String>,
    cloud_user_id: String,
    environment_credential: String,
    cloud_mint_public_key: String,
    endpoint_runtime: Value,
}
impl Config {
    fn issuer(&self) -> &str {
        self.relay_issuer
            .as_deref()
            .unwrap_or(&self.relay_url)
            .trim_end_matches('/')
    }
}
impl Data {
    fn public(&self, reachability: Option<Value>) -> Value {
        let config = self.config.as_ref();
        let mut v = json!({"linked":config.is_some(),"cloudUserId":config.map(|c|&c.cloud_user_id),"relayUrl":config.map(|c|&c.relay_url),"relayIssuer":config.map(Config::issuer),"managedTunnelActive":false,"publishAgentActivity":self.publish,"holdWebhooksWhileOffline":self.hold,"linkRevoked":false});
        if let Some(result) = reachability {
            v["reachability"] = result
        }
        v
    }
}
pub(crate) struct Service {
    data: Storage<Data>,
    latest: std::sync::Mutex<Option<Value>>,
    write: tokio::sync::Mutex<()>,
}
impl Service {
    pub(crate) fn open(dir: &Path) -> anyhow::Result<Self> {
        let data = Storage::<Data>::open(&dir.join("cloud-link.json"))?;
        if data.read(|d| d.key.is_empty()) {
            let k = Ed25519KeyPair::generate_pkcs8(&SystemRandom::new())
                .map_err(|_| anyhow::anyhow!("could not create signing key"))?;
            data.update(|d| {
                d.key = k.as_ref().to_vec();
                Ok(())
            })
            .map_err(anyhow::Error::msg)?;
        }
        Ok(Self {
            data,
            latest: std::sync::Mutex::default(),
            write: tokio::sync::Mutex::default(),
        })
    }
    fn state(&self) -> Value {
        self.data
            .read(|d| d.public(self.latest.lock().unwrap().clone()))
    }
    fn key(&self) -> Option<Ed25519KeyPair> {
        self.data.read(|d| Ed25519KeyPair::from_pkcs8(&d.key).ok())
    }
}
pub(super) fn routes() -> Router<Arc<AppState>> {
    Router::new()
        .route("/api/connect/link-state", get(handler))
        .route("/api/connect/preferences", post(handler))
        .route("/api/connect/unlink", post(handler))
        .route("/api/connect/reachability", post(handler))
        .route("/api/connect/relay-config", post(handler))
        .route("/api/connect/link-proof", post(handler))
        .route("/api/t3-connect/health", post(handler))
        .route("/api/t3-connect/mint-credential", post(handler))
}
fn err(status: StatusCode, msg: &str) -> Response {
    let tag = match status {
        StatusCode::BAD_REQUEST => "EnvironmentHttpBadRequestError",
        StatusCode::CONFLICT => "EnvironmentHttpConflictError",
        StatusCode::UNAUTHORIZED => "EnvironmentHttpUnauthorizedError",
        _ => "EnvironmentHttpInternalServerError",
    };
    (status, Json(json!({"_tag":tag,"message":msg}))).into_response()
}
fn internal() -> Response {
    err(
        StatusCode::INTERNAL_SERVER_ERROR,
        "Could not persist environment relay configuration.",
    )
}
fn forwarded(h: &HeaderMap) -> bool {
    ["x-forwarded-host", "x-forwarded-proto"]
        .iter()
        .any(|k| header(h, k).is_some_and(|v| !v.trim().is_empty()))
}
async fn handler(State(s): State<Arc<AppState>>, req: Request) -> Response {
    let (parts, body) = req.into_parts();
    let path = parts.uri.path();
    let kind = path.rsplit('/').next().unwrap_or_default();
    let owner = if path.starts_with("/api/connect/") {
        let session = match principal(&s, &parts.headers) {
            Ok(p) => p,
            Err(e) => return auth_error(&parts.headers, e),
        };
        let scope = if matches!(kind, "link-state" | "reachability") {
            "relay:read"
        } else {
            "relay:write"
        };
        if !session.has(scope) {
            return scope_error(&parts.headers, scope);
        }
        session.owner
    } else {
        false
    };
    let input = if matches!(
        kind,
        "link-proof" | "preferences" | "relay-config" | "health" | "mint-credential"
    ) {
        match to_bytes(body, 128 * 1024)
            .await
            .ok()
            .and_then(|bytes| serde_json::from_slice::<Value>(&bytes).ok())
        {
            Some(v) if v.is_object() => v,
            _ => return StatusCode::BAD_REQUEST.into_response(),
        }
    } else {
        Value::Null
    };
    let service = &s.connect;
    match kind {
        "link-state" => {
            let mut result = service.state();
            result["linkableHere"] = json!(owner && !forwarded(&parts.headers));
            Json(result).into_response()
        }
        "reachability" => {
            let managed = service.data.read(|d| {
                d.config
                    .as_ref()
                    .is_some_and(|c| !c.endpoint_runtime.is_null())
            });
            let (cause, reason, fix) = if managed {
                ("connector_not_running","The tunnel client (cloudflared) is not running.","Restart T3 Code. If it stays down, check the server log for relay client errors.")
            } else {
                (
                    "not_linked",
                    "The server has not set up a T3 Connect tunnel.",
                    "Run `t3 connect`, then restart T3 Code so it applies the link.",
                )
            };
            let result = json!({"status":"unreachable","checkedAt":iso(now()),"cause":cause,"reason":reason,"hint":null,"fix":fix});
            *service.latest.lock().unwrap() = Some(result.clone());
            Json(result).into_response()
        }
        "unlink" => {
            let _guard = service.write.lock().await;
            if service
                .data
                .update(|d| {
                    d.config = None;
                    d.publish = false;
                    d.hold = false;
                    Ok(())
                })
                .is_err()
            {
                return internal();
            }
            s.guest.set_tunnel_hostname(None);
            Json(json!({"ok":true,"endpointRuntimeStatus":{"status":"disabled"}})).into_response()
        }
        "preferences" => preferences(&s, &input).await,
        "relay-config" => configure(&s, input).await,
        "link-proof" => link_proof(&s, &input, owner, &parts.headers).await,
        "health" | "mint-credential" => signed_request(&s, &input, kind == "health").await,
        _ => StatusCode::NOT_FOUND.into_response(),
    }
}
async fn preferences(state: &AppState, input: &Value) -> Response {
    let Some(publish) = input["publishAgentActivity"].as_bool() else {
        return StatusCode::BAD_REQUEST.into_response();
    };
    let hold = input.get("holdWebhooksWhileOffline");
    if hold.is_some_and(|v| !v.is_boolean()) {
        return StatusCode::BAD_REQUEST.into_response();
    }
    let service = &state.connect;
    let _guard = service.write.lock().await;
    if let Some(hold) = hold.and_then(Value::as_bool) {
        let Some(c) = service.data.read(|d| d.config.clone()) else {
            return err(
                StatusCode::BAD_REQUEST,
                "Link this environment to T3 Connect first.",
            );
        };
        let http = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(Duration::from_secs(10))
            .build()
            .unwrap();
        let url = format!(
            "{}/v1/environments/{}/link-preferences",
            c.relay_url.trim_end_matches('/'),
            state.environment_id
        );
        match http
            .post(url)
            .bearer_auth(c.environment_credential)
            .json(&json!({"holdWebhooksWhileOffline":hold}))
            .send()
            .await
        {
            Ok(r) if r.status().is_success() => {}
            _ => {
                return err(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Could not update T3 Connect webhook settings.",
                )
            }
        }
    }
    if service
        .data
        .update(|d| {
            d.publish = publish;
            if let Some(hold) = hold.and_then(Value::as_bool) {
                d.hold = hold
            }
            Ok(())
        })
        .is_err()
    {
        return internal();
    }
    Json(service.state()).into_response()
}
fn secure(s: &str) -> bool {
    url::Url::parse(s).is_ok_and(|u| {
        u.scheme() == "https"
            && u.host().is_some()
            && u.username().is_empty()
            && u.password().is_none()
            && u.query().is_none()
            && u.fragment().is_none()
    })
}
async fn configure(state: &AppState, input: Value) -> Response {
    if input.get("endpointRuntime").is_none() {
        return StatusCode::BAD_REQUEST.into_response();
    }
    let Ok(mut c) = serde_json::from_value::<Config>(input) else {
        return StatusCode::BAD_REQUEST.into_response();
    };
    let service = &state.connect;
    let _guard = service.write.lock().await;
    for (bad, msg) in [
        (
            !secure(&c.relay_url),
            "Relay URL must be a secure absolute HTTPS URL.",
        ),
        (
            c.relay_issuer.as_ref().is_some_and(|u| !secure(u)),
            "Relay issuer must be a secure absolute HTTPS URL.",
        ),
        (
            c.environment_credential.trim().is_empty(),
            "Relay environment credential is required.",
        ),
        (
            c.cloud_user_id.trim().is_empty(),
            "Cloud user id is required.",
        ),
    ] {
        if bad {
            return err(StatusCode::BAD_REQUEST, msg);
        }
    }
    if service.data.read(|d| {
        d.config
            .as_ref()
            .is_some_and(|old| old.cloud_user_id != c.cloud_user_id)
    }) {
        return err(StatusCode::CONFLICT,"This environment is already linked to a different cloud account. Unlink it before switching accounts.");
    }
    c.cloud_mint_public_key = c.cloud_mint_public_key.replace("\\n", "\n");
    if DecodingKey::from_ed_pem(c.cloud_mint_public_key.as_bytes()).is_err() {
        return err(
            StatusCode::BAD_REQUEST,
            "Cloud mint public key must be a valid Ed25519 public key.",
        );
    }
    if !c.endpoint_runtime.is_null() {
        let Some(kind) = c.endpoint_runtime["providerKind"]
            .as_str()
            .filter(|v| matches!(*v, "manual" | "t3_relay" | "cloudflare_tunnel"))
        else {
            return StatusCode::BAD_REQUEST.into_response();
        };
        if c.endpoint_runtime["connectorToken"]
            .as_str()
            .is_none_or(|s| s.trim().is_empty())
        {
            return StatusCode::BAD_REQUEST.into_response();
        }
        let status = json!({"status":"unsupported","providerKind":kind});
        let body = json!({"_tag":"EnvironmentCloudEndpointUnavailableError","message":"Managed endpoint runtime could not be started.","endpointRuntimeStatus":status});
        return (StatusCode::SERVICE_UNAVAILABLE, Json(body)).into_response();
    }
    if service
        .data
        .update(|d| {
            d.config = Some(c);
            Ok(())
        })
        .is_err()
    {
        return internal();
    }
    state.guest.set_tunnel_hostname(None);
    Json(json!({"ok":true,"endpointRuntimeStatus":{"status":"disabled"}})).into_response()
}
fn sign(k: &Ed25519KeyPair, typ: &str, claims: Value) -> String {
    let h = URL_SAFE_NO_PAD.encode(json!({"alg":"EdDSA","typ":typ}).to_string());
    let p = URL_SAFE_NO_PAD.encode(claims.to_string());
    let message = format!("{h}.{p}");
    format!(
        "{message}.{}",
        URL_SAFE_NO_PAD.encode(k.sign(message.as_bytes()).as_ref())
    )
}
fn loopback(host: &str) -> bool {
    matches!(
        host.trim_matches(['[', ']']).to_ascii_lowercase().as_str(),
        "127.0.0.1" | "localhost" | "::1"
    )
}
async fn link_proof(state: &Arc<AppState>, input: &Value, owner: bool, h: &HeaderMap) -> Response {
    let endpoint = &input["endpoint"];
    let origin = &input["origin"];
    let (Some(challenge), Some(issuer), Some(kind), Some(host), Some(port)) = (
        input["challenge"].as_str(),
        input["relayIssuer"].as_str(),
        endpoint["providerKind"].as_str(),
        origin["localHttpHost"].as_str(),
        origin["localHttpPort"].as_u64(),
    ) else {
        return StatusCode::BAD_REQUEST.into_response();
    };
    if !matches!(kind, "manual" | "cloudflare_tunnel" | "t3_relay")
        || !(1..=65535).contains(&port)
        || ["httpBaseUrl", "wsBaseUrl"]
            .iter()
            .any(|k| endpoint[*k].as_str().is_none_or(|s| s.trim().is_empty()))
    {
        return StatusCode::BAD_REQUEST.into_response();
    }
    let url = url::Url::parse(&format!(
        "http://{}",
        header(h, "host").unwrap_or("127.0.0.1")
    ));
    if port != u64::from(state.auth.port())
        || kind == "t3_relay"
        || forwarded(h)
        || !loopback(host)
        || !url.is_ok_and(|u| {
            (owner || u.host_str().is_some_and(loopback))
                && u.port_or_known_default().map(u64::from) == Some(port)
        })
    {
        return err(StatusCode::BAD_REQUEST, "Invalid managed endpoint origin.");
    }
    let key = match state.connect.key() {
        Some(k) => k,
        None => return internal(),
    };
    let mut der = vec![
        0x30, 0x2a, 0x30, 0x05, 0x06, 0x03, 0x2b, 0x65, 0x70, 0x03, 0x21, 0,
    ];
    der.extend(key.public_key().as_ref());
    let pem = format!(
        "-----BEGIN PUBLIC KEY-----\n{}\n-----END PUBLIC KEY-----",
        STANDARD.encode(der)
    );
    let issued = challenge
        .split('.')
        .nth(1)
        .and_then(|v| URL_SAFE_NO_PAD.decode(v).ok())
        .and_then(|data| serde_json::from_slice::<Value>(&data).ok())
        .and_then(|v| v["iat"].as_i64())
        .filter(|t| *t > 0 && *t < 9_007_199_254_740_991)
        .unwrap_or_else(|| now() / 1000);
    let Json(descriptor) = environment_http::descriptor(State(state.clone())).await;
    let scopes = if kind == "manual" {
        vec!["agent_activity_notifications"]
    } else {
        vec!["agent_activity_notifications", "managed_tunnels"]
    };
    let id = &state.environment_id;
    let proof = sign(
        &key,
        "t3-env-link+jwt",
        json!({"iss":format!("t3-env:{id}"),"aud":issuer.trim().trim_end_matches('/'),"sub":id,"jti":Uuid::new_v4(),"iat":issued,"exp":issued+300,"challenge":challenge,"descriptor":descriptor,"environmentId":id,"environmentPublicKey":pem,"endpoint":endpoint,"origin":origin,"scopes":scopes}),
    );
    (SECRET_HEADERS, Json(proof)).into_response()
}
async fn signed_request(state: &Arc<AppState>, input: &Value, health: bool) -> Response {
    let Some(proof) = input["proof"].as_str().filter(|s| !s.is_empty()) else {
        return StatusCode::BAD_REQUEST.into_response();
    };
    let service = &state.connect;
    let _guard = service.write.lock().await;
    let Some(c) = service.data.read(|d| d.config.clone()) else {
        return err(
            StatusCode::INTERNAL_SERVER_ERROR,
            "Cloud mint public key is not installed for this environment.",
        );
    };
    let kind = if health { "health" } else { "mint" };
    let invalid = || {
        err(
            StatusCode::UNAUTHORIZED,
            &format!("Invalid cloud {kind} request."),
        )
    };
    if !decode_header(proof).is_ok_and(|h| {
        h.typ.as_deref() == Some(&format!("t3-cloud-{kind}+jwt")) && h.alg == Algorithm::EdDSA
    }) {
        return invalid();
    }
    let aud = format!("t3-env:{}", state.environment_id);
    let mut val = Validation::new(Algorithm::EdDSA);
    val.set_required_spec_claims(&["exp", "sub", "iss", "aud"]);
    val.set_issuer(&[c.issuer()]);
    val.set_audience(&[&aud]);
    val.validate_nbf = true;
    val.leeway = 0;
    let Ok(key) = DecodingKey::from_ed_pem(c.cloud_mint_public_key.as_bytes()) else {
        return internal();
    };
    let Ok(token) = decode::<Value>(proof, &key, &val) else {
        return invalid();
    };
    let v = token.claims;
    let (Some(iat), Some(exp), Some(nonce), Some(jti)) = (
        v["iat"].as_i64(),
        v["exp"].as_i64(),
        v["nonce"].as_str(),
        v["jti"].as_str(),
    ) else {
        return invalid();
    };
    let at = now() / 1000;
    let scope = if health {
        "environment:status"
    } else {
        "environment:connect"
    };
    if exp <= iat
        || exp.saturating_sub(iat) > 300
        || iat > at + 60
        || nonce.is_empty()
        || jti.is_empty()
        || v["scope"] != json!([scope])
        || v["sub"] != c.cloud_user_id
        || v["environmentId"] != state.environment_id
    {
        return invalid();
    }
    if !health
        && (v["clientProofKeyThumbprint"]
            .as_str()
            .is_none_or(str::is_empty)
            || v["cnf"]["jkt"] != v["clientProofKeyThumbprint"])
    {
        return invalid();
    }
    if let Err(e) = service.data.update(|d| {
        d.replays.retain(|_, t| *t > at);
        let keys = [format!("{kind}.nonce.{nonce}"), format!("{kind}.jti.{jti}")];
        if keys.iter().any(|k| d.replays.contains_key(k)) {
            return Err("replay");
        }
        for k in keys {
            d.replays.insert(k, exp);
        }
        Ok(())
    }) {
        return if e == "replay" {
            err(
                StatusCode::CONFLICT,
                &format!("Cloud {kind} request was already consumed."),
            )
        } else {
            internal()
        };
    }
    let key = match service.key() {
        Some(k) => k,
        None => return internal(),
    };
    if !health {
        let settings = match tokio::fs::read(state.data_dir.join("settings.json")).await {
            Ok(bytes) => match serde_json::from_slice::<Value>(&bytes) {
                Ok(v) => v,
                Err(_) => return internal(),
            },
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Value::Null,
            Err(_) => return internal(),
        };
        let binding = v["clientProofKeyThumbprint"]
            .as_str()
            .expect("validated thumbprint");
        let Ok((credential, expiry)) = state.auth.issue_connect(
            binding,
            settings["requireConnectDeviceApproval"]
                .as_bool()
                .unwrap_or(false),
        ) else {
            return internal();
        };
        let id = &state.environment_id;
        let jwt = sign(
            &key,
            "t3-env-mint+jwt",
            json!({"iss":aud,"aud":c.issuer(),"sub":id,"jti":Uuid::new_v4(),"iat":at,"exp":expiry/1000,"environmentId":id,"clientProofKeyThumbprint":binding,"requestNonce":nonce,"credential":credential}),
        );
        return (
            SECRET_HEADERS,
            Json(json!({"credential":credential,"expiresAt":iso(expiry),"proof":jwt})),
        )
            .into_response();
    }
    let Json(descriptor) = environment_http::descriptor(State(state.clone())).await;
    let checked = iso(now());
    let id = &state.environment_id;
    let jwt = sign(
        &key,
        "t3-env-health+jwt",
        json!({"iss":aud,"aud":c.issuer(),"sub":id,"jti":Uuid::new_v4(),"iat":at,"exp":at+300,"environmentId":id,"requestNonce":nonce,"status":"online","descriptor":descriptor,"checkedAt":checked}),
    );
    (SECRET_HEADERS,Json(json!({"environmentId":id,"status":"online","descriptor":descriptor,"checkedAt":checked,"proof":jwt}))).into_response()
}
