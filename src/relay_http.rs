//! Standalone relay listener. Each route uses the credential realm declared by RelayApi.
use crate::{
    contracts::HttpContract,
    relay_service::{
        links::{LinkError, FLAGS},
        timestamp,
        tokens::{field, scopes, verified, ACCESS, CHALLENGE, SCOPES},
        RelayService,
    },
    trace_id, HTTP_CONTRACTS,
};
use axum::{
    body::Bytes,
    extract::{Path, State},
    http::{HeaderMap, HeaderValue, Method, StatusCode, Uri},
    response::{IntoResponse, Response},
    routing::{on, MethodFilter},
    Json, Router,
};
use jsonwebtoken::DecodingKey;
use serde_json::{json, Value};
use std::collections::HashMap;
use time::{Duration, OffsetDateTime};
use uuid::Uuid;

const GRANT: &str = "urn:ietf:params:oauth:grant-type:token-exchange";
const JWT: &str = "urn:ietf:params:oauth:token-type:jwt";
const ACCESS_TYPE: &str = "urn:ietf:params:oauth:token-type:access_token";
pub fn router(service: RelayService) -> Router {
    let mut app = Router::new();
    for contract in HTTP_CONTRACTS.iter().filter(|c| c.relay) {
        let method =
            MethodFilter::try_from(Method::from_bytes(contract.method.as_bytes()).unwrap())
                .unwrap();
        app = app.route(
            contract.path,
            on(
                method,
                move |State(s): State<RelayService>,
                      Path(params): Path<HashMap<String, String>>,
                      uri: Uri,
                      headers: HeaderMap,
                      body: Bytes| async move {
                    let cache = matches!(
                        contract.name,
                        "exchangeDpopAccessToken"
                            | "createEnvironmentLinkChallenge"
                            | "listDevices"
                            | "listDevicesV2"
                            | "linkEnvironment"
                            | "connectEnvironment"
                            | "getT3ChatLink"
                    );
                    let req = Call {
                        contract,
                        params,
                        uri,
                        headers,
                        body,
                    };
                    let mut response = match req.handle(&s) {
                        Ok(v) => Json(v).into_response(),
                        Err(e) => e.response(&req.headers),
                    };
                    if cache {
                        response
                            .headers_mut()
                            .insert("cache-control", HeaderValue::from_static("no-store"));
                        response
                            .headers_mut()
                            .insert("pragma", HeaderValue::from_static("no-cache"));
                    }
                    if response.status() == 401 && contract.auth == "RelayDpopClientAuth" {
                        response
                            .headers_mut()
                            .insert("www-authenticate", HeaderValue::from_static("DPoP"));
                    }
                    response
                },
            ),
        );
    }
    app.with_state(service)
}

enum Error {
    Publish(crate::relay_service::activity::PublishError),
    Bad,
    Auth(&'static str),
    Dpop(&'static str),
    Internal(&'static str),
    Link(LinkError),
    Connect(&'static str),
    Hook,
    Telemetry,
    PayloadTooLarge,
}
impl From<rusqlite::Error> for Error {
    fn from(_: rusqlite::Error) -> Self {
        Self::Internal("persistence_failed")
    }
}
impl Error {
    fn response(self, headers: &HeaderMap) -> Response {
        let trace = trace_id(headers);
        let (status, body) = match self {
            Self::Publish(e) => {
                use crate::relay_service::activity::PublishError::*;
                match e {
                    Invalid => (
                        401,
                        json!({"_tag":"RelayAgentActivityPublishProofInvalidError","code":"agent_activity_publish_proof_invalid","reason":"invalid_signature_or_payload","traceId":trace}),
                    ),
                    Expired => (
                        401,
                        json!({"_tag":"RelayAgentActivityPublishProofExpiredError","code":"agent_activity_publish_proof_expired","traceId":trace}),
                    ),
                    Bad => return Self::Bad.response(headers),
                    Storage => return Self::Internal("persistence_failed").response(headers),
                    DeliveryUnavailable => {
                        return Self::Internal("upstream_unavailable").response(headers)
                    }
                }
            }
            Self::Bad => return StatusCode::BAD_REQUEST.into_response(),
            Self::Auth(reason) => (
                401,
                json!({"_tag":"RelayAuthInvalidError","code":"auth_invalid","reason":reason,"traceId":trace}),
            ),
            Self::Dpop(reason) => (
                401,
                json!({"_tag":"RelayAuthInvalidError","code":"auth_invalid","reason":"invalid_dpop","dpopFailureReason":reason,"traceId":trace}),
            ),
            Self::Internal(reason) => (
                500,
                json!({"_tag":"RelayInternalError","code":"internal_error","reason":reason,"traceId":trace}),
            ),
            Self::Link(LinkError::Internal) => {
                return Self::Internal("persistence_failed").response(headers)
            }
            Self::Link(LinkError::Invalid(reason)) => (
                400,
                json!({"_tag":"RelayEnvironmentLinkProofInvalidError","code":"environment_link_proof_invalid","reason":reason,"traceId":trace}),
            ),
            Self::Link(LinkError::Expired) => (
                401,
                json!({"_tag":"RelayEnvironmentLinkProofExpiredError","code":"environment_link_proof_expired","traceId":trace}),
            ),
            Self::Link(LinkError::NoTunnel) => (
                503,
                json!({"_tag":"RelayEnvironmentLinkUnavailableError","code":"environment_link_unavailable","reason":"managed_endpoint_not_configured","traceId":trace}),
            ),
            Self::Connect(reason) => (
                403,
                json!({"_tag":"RelayEnvironmentConnectNotAuthorizedError","code":"environment_connect_not_authorized","reason":reason,"traceId":trace}),
            ),
            Self::Hook => (404, json!({"error":"hook_not_found"})),
            Self::Telemetry => (502, json!({"error":"upstream_unavailable"})),
            Self::PayloadTooLarge => (413, json!({"error":"payload_too_large"})),
        };
        (StatusCode::from_u16(status).unwrap(), Json(body)).into_response()
    }
}
fn dpop_error(reason: &'static str) -> Error {
    if reason == "persistence_failed" {
        Error::Internal(reason)
    } else {
        Error::Dpop(reason)
    }
}
fn required<'a>(v: &'a Value, k: &str) -> Result<&'a str, Error> {
    field(v, k).ok_or(Error::Bad)
}
struct Call {
    contract: &'static HttpContract,
    params: HashMap<String, String>,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
}

impl Call {
    fn header(&self, k: &str) -> Option<&str> {
        self.headers.get(k)?.to_str().ok()
    }
    fn credential(&self, scheme: &str) -> &str {
        self.header("authorization")
            .and_then(|s| s.split_once(' '))
            .filter(|(s, _)| s.eq_ignore_ascii_case(scheme))
            .map(|(_, t)| t.trim_start())
            .unwrap_or("")
    }
    fn json(&self) -> Result<Value, Error> {
        serde_json::from_slice(&self.body).map_err(|_| Error::Bad)
    }
    fn handle(&self, s: &RelayService) -> Result<Value, Error> {
        let issuer = &s.issuer;
        match self.contract.auth {
            "RelayClientAuth" => {
                let owner = s
                    .keys
                    .clerk_subject(self.credential("Bearer"))
                    .ok_or(Error::Auth("invalid_bearer"))?;
                return self.client(s, &owner);
            }
            "RelayDpopClientAuth" => return self.dpop(s),
            "RelayEnvironmentAuth" => return self.environment(s),
            _ => {}
        }
        if matches!(
            self.contract.name,
            "forwardPost" | "forwardPut" | "forwardPatch" | "forwardGet"
        ) {
            // The relay validates these public webhook requests before it looks up
            // an endpoint. This keeps malformed URLs and oversized bodies from
            // consuming endpoint or upstream resources.
            let endpoint_key = self
                .params
                .get("endpointKey")
                .map(String::as_str)
                .unwrap_or_default();
            if endpoint_key.len() != 16
                || !endpoint_key
                    .bytes()
                    .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
            {
                return Err(Error::Hook);
            }
            if self.body.len() > 1_048_576 {
                return Err(Error::PayloadTooLarge);
            }
            // A standalone Rust relay has no configured managed-endpoint
            // provider or upstream delivery queue. A validly shaped request
            // therefore reaches the same public not-found response as an
            // unallocated endpoint in the reference relay.
            return Err(Error::Hook);
        }
        match self.contract.name {
            "health" => s
                .health()
                .map_err(|_| Error::Internal("database_unavailable")),
            "authorizationServer" => Ok(
                json!({"issuer":issuer,"token_endpoint":format!("{issuer}/v1/client/dpop-token"),"grant_types_supported":[GRANT],"token_endpoint_auth_methods_supported":["none"],"dpop_signing_alg_values_supported":["ES256"],"scopes_supported":SCOPES}),
            ),
            "protectedResource" => Ok(
                json!({"resource":issuer,"authorization_servers":[issuer],"scopes_supported":SCOPES,"dpop_bound_access_tokens_required":true,"dpop_signing_alg_values_supported":["ES256"]}),
            ),
            "exchangeDpopAccessToken" => self.exchange(s),
            "ingest" => Err(Error::Telemetry),
            _ => Err(Error::Internal("internal_error")),
        }
    }
    fn exchange(&self, s: &RelayService) -> Result<Value, Error> {
        if self.header("content-type").unwrap_or("").split(';').next()
            != Some("application/x-www-form-urlencoded")
        {
            return Err(Error::Bad);
        }
        let form: HashMap<_, _> = url::form_urlencoded::parse(&self.body).collect();
        let get = |key: &str| {
            form.get(key)
                .map(|s| s.trim())
                .filter(|s| !s.is_empty())
                .ok_or(Error::Bad)
        };
        let client = get("client_id")?;
        if get("grant_type")? != GRANT
            || get("subject_token_type")? != JWT
            || get("requested_token_type")? != ACCESS_TYPE
            || !matches!(client, "t3-mobile" | "t3-web")
        {
            return Err(Error::Bad);
        }
        let scope = scopes(client, get("scope")?).ok_or(Error::Dpop("invalid_proof"))?;
        if get("resource")? != s.issuer {
            return Err(Error::Dpop("invalid_proof"));
        }
        let owner = s
            .keys
            .clerk_subject(get("subject_token")?)
            .ok_or(Error::Auth("invalid_bearer"))?;
        let thumb = s
            .proof(
                self.header("dpop").unwrap_or(""),
                self.contract.method,
                self.uri.path(),
                None,
            )
            .map_err(dpop_error)?;
        let iat = OffsetDateTime::now_utc().unix_timestamp();
        let claims = json!({"iss":s.issuer,"aud":s.issuer,"sub":owner,"jti":Uuid::new_v4(),"iat":iat,"exp":iat+1800,"client_id":client,"scope":scope,"cnf":{"jkt":thumb}});
        let token = s
            .keys
            .mint(ACCESS, &claims)
            .ok_or(Error::Internal("internal_error"))?;
        Ok(
            json!({"access_token":token,"issued_token_type":ACCESS_TYPE,"token_type":"DPoP","expires_in":1800,"scope":scope}),
        )
    }
    fn client(&self, s: &RelayService, owner: &str) -> Result<Value, Error> {
        let name = self.contract.name;
        match name {
            "listEnvironments" => Ok(s.links(owner)?),
            "listDevices" | "listDevicesV2" => Ok(s.list_devices(owner, name == "listDevices")?),
            "unlinkEnvironment" => Ok(json!({"ok":s.unlink(owner,&self.params["environmentId"])?})),
            "releaseEnvironmentTunnel" => {
                Ok(json!({"ok":s.release_tunnel(owner, &self.params["environmentId"])?}))
            }
            "getT3ChatLink" => Ok(json!({"available":false,"link":null})),
            "createT3ChatLinkRequest" => Err(Error::Internal("internal_error")),
            "unlinkT3Chat" => Ok(json!({"ok":true})),
            "createEnvironmentLinkChallenge" | "linkEnvironment" => {
                let body = self.json()?;
                if FLAGS.iter().any(|f| !body[*f].is_boolean()) {
                    return Err(Error::Bad);
                }
                if name == "linkEnvironment" {
                    required(&body, "proof")?;
                    return s.link(owner, &body).map_err(Error::Link);
                }
                let now = OffsetDateTime::now_utc();
                let exp = now + Duration::minutes(5);
                let payload = json!({"kind":"environment_link_challenge","iss":s.issuer,"aud":s.issuer,"sub":owner,"jti":Uuid::new_v4(),"iat":now.unix_timestamp(),"exp":exp.unix_timestamp(),"notificationsEnabled":body[FLAGS[0]],"liveActivitiesEnabled":body[FLAGS[1]],"managedTunnelsEnabled":body[FLAGS[2]]});
                Ok(
                    json!({"challenge":s.keys.mint(CHALLENGE,&payload).ok_or(Error::Internal("internal_error"))?,"expiresAt":timestamp(exp)}),
                )
            }
            _ => Err(Error::Internal("upstream_unavailable")),
        }
    }
    fn dpop(&self, s: &RelayService) -> Result<Value, Error> {
        let invalid = Error::Auth("invalid_bearer");
        let token = self.credential("DPoP");
        let v = s
            .keys
            .verify
            .as_ref()
            .and_then(|key| verified(token, key, ACCESS, &s.issuer, &s.issuer))
            .ok_or(invalid)?;
        if self.header("dpop").is_none_or(|v| v.trim().is_empty()) {
            return Err(Error::Bad);
        }
        let bad = || Error::Auth("invalid_bearer");
        let thumb = field(&v["cnf"], "jkt").ok_or_else(bad)?;
        let owner = field(&v, "sub").ok_or_else(bad)?;
        let iat = v["iat"].as_i64().ok_or_else(bad)?;
        if OffsetDateTime::now_utc().unix_timestamp().abs_diff(iat) > 1800 {
            return Err(bad());
        }
        let scope = scopes(
            field(&v, "client_id").ok_or_else(bad)?,
            field(&v, "scope").ok_or_else(bad)?,
        )
        .ok_or_else(bad)?;
        let needed = match self.contract.name {
            "connectEnvironment" => SCOPES[0],
            "getEnvironmentStatus" => SCOPES[1],
            _ => SCOPES[2],
        };
        if !scope.split(' ').any(|p| p == needed) {
            return Err(Error::Dpop("invalid_proof"));
        }
        if self.contract.name == "connectEnvironment" {
            let b = self.json()?;
            if b.get("clientKeyThumbprint")
                .or(b.get("clientProofKeyThumbprint"))
                .and_then(Value::as_str)
                != Some(thumb)
                || b.get("clientProofKeyThumbprint")
                    .is_some_and(|v| v != thumb)
            {
                return Err(Error::Dpop("invalid_proof"));
            }
        }
        s.proof(
            self.header("dpop").unwrap_or(""),
            self.contract.method,
            self.uri.path(),
            Some((thumb, token)),
        )
        .map_err(dpop_error)?;
        match self.contract.name {
            "registerDevice" => {
                let v = self.json()?;
                validate_device(&v)?;
                s.register_device(owner, v)?;
            }
            "registerLiveActivity" => {
                let v = self.json()?;
                s.register_activity(
                    owner,
                    required(&v, "deviceId")?,
                    required(&v, "activityPushToken")?,
                )?;
            }
            "unregisterDevice" => s.remove_device(owner, &self.params["deviceId"])?,
            "getAgentActivitySnapshot" => return Ok(s.aggregate(owner)?),
            "connectEnvironment" | "getEnvironmentStatus" => {
                let links = s.links(owner)?;
                let link = links["environments"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .find(|e| e["environmentId"] == self.params["environmentId"]);
                return Err(Error::Connect(match link {
                    None => "environment_link_not_found",
                    Some(l) if l["endpoint"]["providerKind"] == "cloudflare_tunnel" => {
                        "managed_endpoint_allocation_not_found"
                    }
                    _ => "endpoint_provider_not_managed",
                }));
            }
            _ => return Err(Error::Internal("internal_error")),
        }
        Ok(json!({"ok":true}))
    }
    fn environment(&self, s: &RelayService) -> Result<Value, Error> {
        let id = &self.params["environmentId"];
        let key = s
            .environment(self.credential("Bearer"), id)?
            .ok_or(Error::Auth("not_authorized"))?;
        match self.contract.name {
            "registerManagedEndpointRecovery" => {
                let body = self.json()?;
                validate_recovery_proof(
                    &body,
                    "register",
                    id,
                    &key,
                    &s.issuer,
                    Some(
                        body.get("tunnelId")
                            .and_then(Value::as_str)
                            .filter(|v| !v.trim().is_empty())
                            .ok_or(Error::Auth("not_authorized"))?,
                    ),
                )?;
                // Provisioning is deliberately delegated to the configured
                // managed-endpoint provider. The standalone relay has no
                // provider credentials, but it must authenticate and validate
                // recovery requests before returning the same upstream error
                // as the TypeScript relay when provisioning is unavailable.
                Err(Error::Internal("upstream_unavailable"))
            }
            "recoverManagedEndpoint" => {
                let body = self.json()?;
                validate_recovery_proof(&body, "recover", id, &key, &s.issuer, None)?;
                Err(Error::Internal("upstream_unavailable"))
            }
            "wakeHeldHooks" => Ok(json!({"pending":false})),
            "publishAgentActivity" => s
                .publish(id, &key, &self.params["threadId"], &self.json()?)
                .map_err(Error::Publish),
            "updateLinkPreferences" => {
                let b = self.json()?;
                let hold = b["holdWebhooksWhileOffline"].as_bool().ok_or(Error::Bad)?;
                s.set_hold(id, &key, hold)?;
                Ok(json!({"holdWebhooksWhileOffline":hold}))
            }
            _ => Err(Error::Internal("upstream_unavailable")),
        }
    }
}

/// Validate the environment-signed proof used by managed tunnel recovery.
///
/// The provider operation itself is optional in the standalone relay, but the
/// proof gate is part of the public API contract. Checking it here keeps an
/// invalid request at the authentication boundary instead of leaking the
/// provider-unavailable response used for an otherwise valid request.
fn validate_recovery_proof(
    body: &Value,
    action: &str,
    environment_id: &str,
    environment_public_key: &str,
    relay_issuer: &str,
    expected_tunnel_id: Option<&str>,
) -> Result<(), Error> {
    let proof = body
        .get("proof")
        .and_then(Value::as_str)
        .filter(|v| !v.trim().is_empty())
        .ok_or(Error::Auth("not_authorized"))?;
    let cloud_user_id = body
        .get("cloudUserId")
        .and_then(Value::as_str)
        .filter(|v| !v.trim().is_empty())
        .ok_or(Error::Auth("not_authorized"))?;
    let origin = body
        .get("origin")
        .and_then(Value::as_object)
        .ok_or(Error::Auth("not_authorized"))?;
    let host = origin
        .get("localHttpHost")
        .and_then(Value::as_str)
        .filter(|v| !v.trim().is_empty())
        .ok_or(Error::Auth("not_authorized"))?;
    let port = origin
        .get("localHttpPort")
        .and_then(Value::as_u64)
        .filter(|p| (1..=65535).contains(p))
        .ok_or(Error::Auth("not_authorized"))?;

    let key = DecodingKey::from_ed_pem(environment_public_key.as_bytes())
        .map_err(|_| Error::Auth("not_authorized"))?;
    let claims = verified(
        proof,
        &key,
        "t3-env-managed-tunnel-recovery+jwt",
        &format!("t3-env:{environment_id}"),
        relay_issuer,
    )
    .ok_or(Error::Auth("not_authorized"))?;
    if claims.get("environmentId").and_then(Value::as_str) != Some(environment_id)
        || claims.get("sub").and_then(Value::as_str) != Some(environment_id)
        || claims.get("cloudUserId").and_then(Value::as_str) != Some(cloud_user_id)
        || claims.get("action").and_then(Value::as_str) != Some(action)
        || claims
            .get("origin")
            .and_then(|v| v.get("localHttpHost"))
            .and_then(Value::as_str)
            != Some(host)
        || claims
            .get("origin")
            .and_then(|v| v.get("localHttpPort"))
            .and_then(Value::as_u64)
            != Some(port)
    {
        return Err(Error::Auth("not_authorized"));
    }
    if action == "register" && claims.get("tunnelId").and_then(Value::as_str) != expected_tunnel_id
    {
        return Err(Error::Auth("not_authorized"));
    }
    Ok(())
}
fn validate_device(v: &Value) -> Result<(), Error> {
    required(v, "label")?;
    required(v, "deviceId")?;
    for k in ["appVersion", "bundleId", "pushToken", "pushToStartToken"] {
        if v.get(k).is_some() {
            required(v, k)?;
        }
    }
    if v.get("apsEnvironment").is_some()
        && !matches!(v["apsEnvironment"].as_str(), Some("sandbox" | "production"))
    {
        return Err(Error::Bad);
    }
    for (k, min) in [("iosMajorVersion", 18), ("androidApiLevel", 24)] {
        if v.get(k).is_some() && v[k].as_u64().is_none_or(|n| n < min) {
            return Err(Error::Bad);
        }
    }
    match required(v, "platform")? {
        "ios" if v.get("iosMajorVersion").is_some() => {}
        "android"
            if v.get("androidApiLevel").is_some()
                && ["iosMajorVersion", "apsEnvironment", "pushToStartToken"]
                    .iter()
                    .all(|k| v.get(*k).is_none()) => {}
        _ => return Err(Error::Bad),
    }
    if [
        "notificationsEnabled",
        "liveActivitiesEnabled",
        "notifyOnApproval",
        "notifyOnInput",
        "notifyOnCompletion",
        "notifyOnFailure",
    ]
    .iter()
    .any(|k| !v["preferences"][*k].is_boolean())
    {
        return Err(Error::Bad);
    }
    Ok(())
}
