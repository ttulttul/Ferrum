pub(crate) mod access;
mod connect;
#[cfg(feature = "native")]
mod external_pairing;
pub(crate) mod permissions;
pub(crate) mod storage;
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use hmac::{Hmac, Mac};
use rand::Rng;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};
use storage::Storage;
use time::OffsetDateTime;

pub const STANDARD: &[&str] = &[
    "orchestration:read",
    "orchestration:operate",
    "settings:write",
    "providers:manage",
    "environment:maintain",
    "preview:operate",
    "diagnostics:read",
    "terminal:read",
    "terminal:operate",
    "source-control:write",
    "filesystem:read",
    "filesystem:write",
    "relay:read",
    "access:read",
    "access:write",
    "relay:write",
];
const LEGACY: &[&str] = &[
    "orchestration:read",
    "orchestration:operate",
    "terminal:operate",
    "review:write",
    "access:read",
    "access:write",
    "relay:read",
    "relay:write",
];
const BROWSER: &str = "browser-session-cookie";
const BEARER: &str = "bearer-access-token";
const DAY: i64 = 86_400_000;
pub(crate) fn now() -> i64 {
    crate::platform::now_ms()
}
pub(crate) fn iso(ms: i64) -> String {
    OffsetDateTime::from_unix_timestamp_nanos(ms as i128 * 1_000_000)
        .unwrap()
        .format(time::macros::format_description!(
            "[year]-[month]-[day]T[hour]:[minute]:[second].[subsecond digits:3]Z"
        ))
        .unwrap()
}
fn random_key() -> [u8; 32] {
    crate::platform::random_bytes()
}
fn signed(secret: &[u8; 32], claims: &Value) -> String {
    let payload = URL_SAFE_NO_PAD.encode(serde_json::to_vec(claims).unwrap());
    let mut mac = Hmac::<Sha256>::new_from_slice(secret).unwrap();
    mac.update(payload.as_bytes());
    format!(
        "{payload}.{}",
        URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes())
    )
}
fn key(secret: &str) -> String {
    URL_SAFE_NO_PAD.encode(Sha256::digest(secret.as_bytes()))
}

#[derive(Clone, Serialize, Deserialize)]
pub struct Session {
    pub id: String,
    pub permissions: Vec<String>,
    pub method: String,
    #[serde(default)]
    issued: i64,
    #[serde(default)]
    client: Value,
    pub expires: i64,
    pub owner: bool,
    #[serde(default)]
    pub proof_key: Option<String>,
    #[serde(default)]
    renewable_until: Option<i64>,
    #[serde(default)]
    connect: bool,
}
impl Session {
    pub fn has(&self, scope: &str) -> bool {
        self.permissions.iter().any(|p| p == scope)
    }
    pub fn unrestricted(&self) -> bool {
        STANDARD
            .iter()
            .all(|scope| self.permissions.iter().any(|p| p == scope))
    }
    fn metadata(&self) -> Value {
        json!({"authenticated":true,"scopes":self.permissions.iter().filter(|s|LEGACY.contains(&s.as_str())).collect::<Vec<_>>(),"permissions":self.permissions,"sessionMethod":self.method,"expiresAt":iso(self.expires)})
    }
}
#[derive(Clone, Serialize, Deserialize)]
struct Grant {
    #[serde(default)]
    created: i64,
    #[serde(default)]
    label: Option<String>,
    #[serde(default)]
    requested_client: Option<Value>,
    id: String,
    scopes: Vec<String>,
    expires: i64,
    purpose: Option<String>,
    access: Option<String>,
    #[serde(default)]
    binding: Option<String>,
    #[serde(default)]
    pending_code: Option<String>,
}
#[derive(Default, Clone, Serialize, Deserialize)]
struct Credentials {
    sessions: HashMap<String, Session>,
    #[serde(default)]
    devices: std::collections::HashSet<String>,
    #[serde(default)]
    used_proofs: HashMap<String, i64>,
    #[serde(default)]
    device_rows: HashMap<String, (i64, Option<Value>)>,
    #[serde(default)]
    last_connected: HashMap<String, i64>,
    #[serde(skip)]
    open_sockets: HashMap<String, usize>,
    links: HashMap<String, Grant>,
    #[serde(default)]
    signing_key: Option<[u8; 32]>,
}
#[derive(Clone)]
struct Ticket {
    session: String,
    audience: String,
    expires: i64,
}
#[derive(Clone)]
pub struct AuthService {
    records: Arc<Storage<Credentials>>,
    tickets: Arc<Mutex<HashMap<String, Ticket>>>,
    bootstrap: String,
    bootstrap_expires: i64,
    port: u16,
    desktop: bool,
    changes: Arc<access::Subscribers>,
    invalidations: tokio::sync::watch::Sender<()>,
    /// The TypeScript `statev2.sqlite` beside our own state, where `t3 auth pairing create`
    /// (the desktop's SSH pairing step) writes one-time links.
    external_pairing: Option<PathBuf>,
}
impl Default for AuthService {
    fn default() -> Self {
        Self::new()
    }
}
impl AuthService {
    pub fn new() -> Self {
        let mut auth =
            Self::with_bootstrap(std::env::var("T3CODE_BOOTSTRAP_CREDENTIAL").unwrap_or_default());
        auth.desktop = false;
        auth
    }
    pub fn with_bootstrap(credential: impl Into<String>) -> Self {
        Self {
            records: Arc::default(),
            tickets: Arc::default(),
            bootstrap: credential.into(),
            bootstrap_expires: now() + DAY,
            port: 3773,
            desktop: true,
            changes: Arc::default(),
            invalidations: tokio::sync::watch::channel(()).0,
            external_pairing: None,
        }
    }
    pub fn with_port(mut self, port: u16) -> Self {
        self.port = port;
        self
    }
    pub fn with_desktop(mut self, desktop: bool) -> Self {
        self.desktop = desktop;
        self
    }
    #[cfg(feature = "native")]
    pub fn open(self, dir: &Path) -> anyhow::Result<Self> {
        self.open_in(Arc::new(crate::persistence::FsStore), dir)
    }
    /// Keep sessions, grants and keys in `environment-auth.json` under `dir`
    /// in `documents`, written through on every change.
    pub fn open_in(
        mut self,
        documents: Arc<dyn crate::persistence::DocumentStore>,
        dir: &Path,
    ) -> anyhow::Result<Self> {
        self.records = Arc::new(Storage::open_in(
            documents,
            &dir.join("environment-auth.json"),
        )?);
        self.external_pairing = Some(dir.join("statev2.sqlite"));
        Ok(self)
    }
    /// Sessions, grants and keys as JSON, for hosts that keep them outside a
    /// state file (the WebAssembly build stores them in its SQL storage).
    pub fn records_json(&self) -> String {
        self.records
            .read(|r| serde_json::to_string(r).expect("credentials serialize"))
    }
    /// Restores what [`records_json`](Self::records_json) produced, in memory only.
    pub fn with_records_json(mut self, json: &str) -> anyhow::Result<Self> {
        self.records = Arc::new(Storage::in_memory(serde_json::from_str(json)?));
        Ok(self)
    }
    pub(crate) fn desktop(&self) -> bool {
        self.desktop
    }
    pub(crate) fn revoke_disallowed(&self, remote: bool, connect: bool) -> anyhow::Result<()> {
        if !remote || !connect {
            self.update(|r| {
                r.sessions
                    .retain(|_, s| s.owner || if s.connect { connect } else { remote });
                r.links
                    .retain(|_, g| if g.binding.is_some() { connect } else { remote });
                Ok(())
            })
            .map_err(anyhow::Error::msg)?;
        }
        Ok(())
    }
    pub(crate) fn port(&self) -> u16 {
        self.port
    }
    pub fn session_cookie_name(&self) -> String {
        format!("t3_session_{}", self.port)
    }
    pub(crate) fn descriptor(&self) -> Value {
        json!({"policy":if self.desktop {"desktop-managed-local"}else{"loopback-browser"},"bootstrapMethods":[if self.desktop {"desktop-bootstrap"}else{"one-time-token"}],"sessionMethods":[BROWSER,BEARER,"dpop-access-token"],"sessionCookieName":self.session_cookie_name(),"serverUpdateScope":"environment:maintain"})
    }
    pub async fn invalidated(&self, session: &Session) {
        let mut changes = self.invalidations.subscribe();
        loop {
            let expiry = self.records.read(|r| {
                r.sessions
                    .values()
                    .filter(|s| s.id == session.id)
                    .map(|s| s.expires)
                    .max()
            });
            let at = now();
            let Some(expires) = expiry.filter(|t| *t > at) else {
                return;
            };
            let timeout = std::time::Duration::from_millis((expires - at) as u64);
            let _ = tokio::time::timeout(timeout, changes.changed()).await;
        }
    }
    pub fn principal(&self, token: &str) -> Option<Session> {
        self.records.read(|r| {
            r.sessions
                .get(&key(token))
                .filter(|s| s.expires > now())
                .cloned()
        })
    }
    pub async fn session_state(&self, token: Option<&str>) -> Value {
        let mut body = json!({"authenticated":false,"auth":self.descriptor()});
        if let Some(s) = token.and_then(|t| self.principal(t)) {
            for (k, v) in s.metadata().as_object().unwrap() {
                body[k] = v.clone()
            }
        }
        body
    }
    pub async fn create_browser_session(&self, credential: &str) -> Result<Value, &'static str> {
        self.create_browser_session_token(credential)
            .await
            .map(|r| r.0)
    }
    pub async fn create_browser_session_token(
        &self,
        credential: &str,
    ) -> Result<(Value, String), &'static str> {
        self.issue(credential, BROWSER, None, None, None)
    }
    pub fn browser_session(
        &self,
        credential: &str,
        previous: Option<&str>,
    ) -> Result<(Value, String), &'static str> {
        self.issue(credential, BROWSER, None, previous, None)
    }
    fn issue(
        &self,
        credential: &str,
        method: &str,
        requested: Option<Vec<String>>,
        replace: Option<&str>,
        proof: Option<&str>,
    ) -> Result<(Value, String), &'static str> {
        let at = now();
        let owner = !credential.is_empty()
            && self.bootstrap_expires > at
            && key(&self.bootstrap) == key(credential);
        let mut requested = requested;
        let result = self.update(|records| {
            let mut connect = false;
            let mut client = crate::access_http::client_metadata();
            let mut permissions = if owner {
                STANDARD.iter().map(|s| s.to_string()).collect()
            } else {
                let grant = match records.links.remove(&key(&credential.to_ascii_uppercase())) {
                    Some(grant) => Some(grant)
                        .filter(|g| g.expires > at && g.purpose.is_none() && g.access.is_none())
                        .ok_or("invalid_credential")?,
                    None => {
                        let grant =
                            self.consume_external_pairing(credential, requested.as_deref(), at)?;
                        // The TypeScript exchange grants the overlap of the requested scopes.
                        if let Some(r) = requested.as_mut() {
                            let mut seen = std::collections::HashSet::new();
                            r.retain(|s| grant.scopes.contains(s) && seen.insert(s.clone()));
                        }
                        grant
                    }
                };
                if let Some(label) = &grant.label {
                    let mut labeled = json!({"label":label});
                    labeled
                        .as_object_mut()
                        .unwrap()
                        .extend(client.as_object().unwrap().clone());
                    labeled["label"] = json!(label);
                    client = labeled;
                }
                if let Some(binding) = &grant.binding {
                    if Some(binding.as_str()) != proof {
                        return Err("invalid_credential");
                    }
                    if grant.pending_code.is_some() && !records.devices.contains(binding) {
                        return Err("connect_approval_pending");
                    }
                    records.devices.insert(binding.clone());
                    records.device_rows.insert(
                        binding.clone(),
                        (at, Some(crate::access_http::client_metadata())),
                    );
                    connect = true;
                }
                grant.scopes
            };
            if let Some(requested) = requested {
                if requested.is_empty() || requested.iter().any(|s| !permissions.contains(s)) {
                    return Err("invalid_scope");
                }
                permissions = requested;
            }
            let ttl = if proof.is_some() { 3_600_000 } else { 30 * DAY };
            let session = Session {
                id: crate::platform::uuid_v4().to_string(),
                owner,
                issued: at,
                client,
                method: method.into(),
                expires: at + ttl,
                permissions,
                proof_key: proof.map(str::to_owned),
                renewable_until: (proof.is_some() && !connect && !owner).then_some(at + 30 * DAY),
                connect,
            };
            if let Some(previous) = replace {
                let k = key(previous);
                if records
                    .sessions
                    .get(&k)
                    .is_some_and(|s| s.method == BROWSER)
                {
                    records.sessions.remove(&k);
                }
            }
            Ok(Self::save_session(records, session, at))
        });
        if matches!(&result, Err("connect_approval_pending")) {
            self.update(|r| {
                if let Some(g) = r
                    .links
                    .get_mut(&key(&credential.to_ascii_uppercase()))
                    .filter(|g| g.binding.as_deref() == proof && g.requested_client.is_none())
                {
                    g.requested_client = Some(crate::access_http::client_metadata());
                }
                Ok(())
            })?;
        }
        if replace.is_some() && result.is_ok() {
            self.invalidations.send_replace(());
        }
        result
    }
    /// Spends a link that `t3 auth pairing create` wrote into the TypeScript store, which is
    /// how the desktop pairs over SSH. Unknown, spent, expired or revoked links all read as
    /// `invalid_credential`, and a link that grants none of the requested scopes as
    /// `scope_not_granted` (unspent), as they do in the TypeScript server.
    fn consume_external_pairing(
        &self,
        credential: &str,
        requested: Option<&[String]>,
        at: i64,
    ) -> Result<Grant, &'static str> {
        let Some(db) = self.external_pairing.as_deref() else {
            return Err("invalid_credential");
        };
        // SSH pairing reads the local TypeScript store; the WebAssembly build has none.
        #[cfg(not(feature = "native"))]
        {
            let _ = (db, credential, requested);
            Err("invalid_credential")
        }
        #[cfg(feature = "native")]
        match external_pairing::consume(db, credential, requested, &iso(at)) {
            Ok(external_pairing::Outcome::Spent(grant)) => Ok(Grant {
                created: at,
                label: grant.label,
                requested_client: None,
                id: grant.id,
                scopes: grant.scopes,
                expires: at,
                purpose: None,
                access: None,
                binding: None,
                pending_code: None,
            }),
            Ok(external_pairing::Outcome::NotFound) => Err("invalid_credential"),
            Ok(external_pairing::Outcome::ScopeNotGranted) => Err("scope_not_granted"),
            Err(error) => {
                tracing::warn!(%error, "could not read pairing links from statev2.sqlite");
                Err("internal_error")
            }
        }
    }
    pub fn consume_approval_code(
        &self,
        code: &str,
        access: &str,
        peer_link: bool,
    ) -> Result<String, &'static str> {
        let k = key(&code.trim().to_ascii_uppercase());
        self.update(|r| {
            let grant = r
                .links
                .get(&k)
                .filter(|g| g.expires > now())
                .ok_or("unknown_or_used")?;
            let needed: &[&str] = match access {
                "read-only" => &["orchestration:read"],
                "auto" | "approval-required" | "auto-accept-edits" => {
                    &["orchestration:read", "orchestration:operate"]
                }
                "full-access" => &[
                    "orchestration:read",
                    "orchestration:operate",
                    "settings:write",
                    "source-control:write",
                ],
                _ => return Err("insufficient_scope"),
            };
            if grant.binding.is_some()
                || grant.purpose.as_deref().is_some_and(|_| !peer_link)
                || (grant.access.is_some() && grant.purpose.is_none() && peer_link)
                || grant.access.as_deref().is_some_and(|bound| bound != access)
                || needed
                    .iter()
                    .any(|scope| !grant.scopes.iter().any(|s| s == scope))
            {
                return Err("insufficient_scope");
            }
            r.links.remove(&k);
            Ok(access.to_owned())
        })
    }

    pub fn exchange(
        &self,
        credential: &str,
        requested: Option<Vec<String>>,
    ) -> Result<Value, &'static str> {
        self.issue(credential, BEARER, requested, None, None)
            .map(|v| v.0)
    }
    pub fn pair(&self, session: &Session, input: &Value) -> Result<Value, &'static str> {
        let access = input
            .get("access")
            .map(|v| v.as_str().ok_or("invalid_scope"))
            .transpose()?;
        let purpose = input
            .get("purpose")
            .map(|v| {
                v.as_str()
                    .filter(|v| *v == "peer-link")
                    .ok_or("invalid_scope")
            })
            .transpose()?;
        if purpose.is_some() && access.is_none() {
            return Err("invalid_scope");
        }
        let prefix = match access {
            None => "",
            Some("read-only") => "READ",
            Some("approval-required") => "SUPERVISED",
            Some("auto-accept-edits") => "EDITS",
            Some("auto") => "AUTO",
            Some("full-access") => "FULL",
            _ => return Err("invalid_scope"),
        };
        let permissions: Vec<String> = if access.is_some() {
            let mut scopes = vec!["orchestration:read"];
            if prefix != "READ" {
                scopes.push("orchestration:operate")
            }
            if prefix == "FULL" {
                scopes.extend(["settings:write", "source-control:write"])
            }
            scopes.into_iter().map(String::from).collect()
        } else if let Some(scopes) = input.get("scopes") {
            serde_json::from_value(scopes.clone()).map_err(|_| "invalid_scope")?
        } else {
            STANDARD.iter().map(|s| s.to_string()).collect()
        };
        if permissions.is_empty()
            || permissions.iter().enumerate().any(|(i, s)| {
                permissions[..i].contains(s)
                    || !STANDARD.contains(&s.as_str())
                    || !session.permissions.contains(s)
            })
        {
            return Err("invalid_scope");
        }
        let label = input
            .get("label")
            .map(|v| {
                v.as_str()
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .ok_or("invalid_scope")
            })
            .transpose()?;
        let id = crate::platform::uuid_v4().to_string();
        let expires = now() + 300_000;
        let alphabet = b"23456789ABCDEFGHJKLMNPQRSTUVWXYZ";
        let mut code: String = (0..12)
            .map(|_| alphabet[crate::platform::rng().gen_range(0..alphabet.len())] as char)
            .collect();
        if !prefix.is_empty() && purpose.is_none() {
            code = format!("{prefix}-{code}")
        }
        let mut response = json!({"id":id,"credential":code});
        if let Some(v) = label {
            response["label"] = json!(v)
        }
        response["expiresAt"] = json!(iso(expires));
        if let Some(v) = purpose {
            response["purpose"] = json!(v)
        }
        if let Some(v) = access {
            response["access"] = json!(v)
        }
        self.update(|r| {
            r.links.retain(|_, g| g.expires > now());
            r.links.insert(
                key(&code),
                Grant {
                    id,
                    created: expires - 300_000,
                    label: label.map(str::to_owned),
                    requested_client: None,
                    expires,
                    scopes: permissions,
                    purpose: purpose.map(String::from),
                    access: access.map(String::from),
                    binding: None,
                    pending_code: None,
                },
            );
            Ok(response)
        })
    }
    pub fn revoke_pairing(&self, id: &str) -> Result<bool, &'static str> {
        self.update(|r| {
            let n = r.links.len();
            r.links.retain(|_, g| g.id != id);
            Ok(r.links.len() != n)
        })
    }
    pub fn revoke_client(&self, current: &Session, id: &str) -> Result<bool, &'static str> {
        if current.id == id {
            return Err("current_session_revoke_not_allowed");
        }
        let result = self.update(|r| {
            let n = r.sessions.len();
            let bound = r
                .sessions
                .values()
                .find(|s| s.id == id)
                .and_then(|s| s.proof_key.clone());
            if let Some(bound) = bound.filter(|b| Some(b) != current.proof_key.as_ref()) {
                Self::remove_device(r, &bound);
            }
            r.sessions.retain(|_, s| s.id != id);
            Ok(n != r.sessions.len())
        });
        self.invalidations.send_replace(());
        result
    }
    pub fn revoke_others(&self, current: &Session) -> Result<usize, &'static str> {
        let result = self.update(|r| {
            let mut n = std::collections::HashSet::new();
            r.devices
                .retain(|key| Some(key) == current.proof_key.as_ref());
            r.device_rows
                .retain(|key, _| Some(key) == current.proof_key.as_ref());
            r.links
                .retain(|_, g| g.binding.is_none() || g.binding == current.proof_key);
            r.sessions.retain(|_, s| {
                if s.id == current.id {
                    return true;
                }
                if s.expires > now() {
                    n.insert(s.id.clone());
                }
                false
            });
            Ok(n.len())
        });
        self.invalidations.send_replace(());
        result
    }
    pub async fn authenticate_bearer(&self, token: &str) -> bool {
        self.principal(token).is_some_and(|s| s.proof_key.is_none())
    }
    pub async fn revoke_session(&self, token: &str) -> bool {
        let result = self
            .update(|r| Ok(r.sessions.remove(&key(token)).is_some()))
            .unwrap_or(false);
        self.invalidations.send_replace(());
        result
    }
    pub async fn create_websocket_ticket(
        &self,
        token_value: &str,
        kind: &str,
    ) -> Result<Value, &'static str> {
        let s = self.principal(token_value).ok_or("invalid_credential")?;
        if !matches!(kind, "any" | "rpc" | "media") {
            return Err("invalid_use");
        }
        let at = now();
        let expires = std::cmp::min(s.expires, at + 300_000);
        let mut claims = json!({"v":1,"kind":"websocket","sid":s.id});
        if kind != "any" {
            claims["aud"] = json!(kind)
        }
        claims["iat"] = json!(at);
        claims["exp"] = json!(expires);
        let ticket = self
            .records
            .read(|r| r.signing_key.map(|k| signed(&k, &claims)))
            .ok_or("internal_error")?;
        let mut tickets = self.tickets.lock().unwrap();
        tickets.retain(|_, t| t.expires > now());
        tickets.insert(
            key(&ticket),
            Ticket {
                session: key(token_value),
                audience: kind.into(),
                expires,
            },
        );
        Ok(json!({"ticket":ticket,"expiresAt":iso(expires)}))
    }
    pub fn ticket_principal(&self, token: &str, kind: &str) -> Option<Session> {
        self.check_ticket(token, kind, |_| Err("invalid_credential"))
            .ok()
    }
    pub(crate) fn check_ticket(
        &self,
        token: &str,
        kind: &str,
        validate: impl FnOnce(&str) -> Result<(), &'static str>,
    ) -> Result<Session, &'static str> {
        let k = key(token);
        let mut tickets = self.tickets.lock().unwrap();
        let t = tickets
            .get(&k)
            .filter(|t| t.expires > now() && (t.audience == "any" || t.audience == kind))
            .ok_or("invalid_credential")?;
        let s = self.records.read(|r| {
            r.sessions
                .get(&t.session)
                .filter(|s| s.expires > now())
                .cloned()
        });
        let session = s.ok_or("invalid_credential")?;
        if t.audience == "rpc" {
            if let Some(binding) = session.proof_key.as_deref() {
                validate(binding)?;
            }
        }
        if kind == "rpc" {
            tickets.remove(&k);
        }
        Ok(session)
    }
    pub async fn consume_ticket(&self, token: &str, kind: &str) -> bool {
        self.ticket_principal(token, kind)
            .is_some_and(|s| s.unrestricted())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn revocation_invalidates_a_persisted_session_and_its_tickets() {
        let dir = tempfile::tempdir().unwrap();
        let service = AuthService::with_bootstrap("owner-secret")
            .open(dir.path())
            .unwrap();
        let (_, owner) = service.browser_session("owner-secret", None).unwrap();
        let owner = service.principal(&owner).unwrap();
        assert!(service.browser_session("random", None).is_err());
        let pairing = service.pair(&owner, &json!({"label":"My phone"})).unwrap();
        let code = pairing["credential"].as_str().unwrap();
        let (_, client) = service.browser_session(code, None).unwrap();
        let id = service.principal(&client).unwrap().id;
        assert!(service.browser_session(code, None).is_err());
        let t = service
            .create_websocket_ticket(&client, "rpc")
            .await
            .unwrap();
        let ticket = t["ticket"].as_str().unwrap();
        assert!(!service.consume_ticket(ticket, "media").await);
        assert!(service.consume_ticket(ticket, "rpc").await);
        assert!(!service.consume_ticket(ticket, "rpc").await);
        let t = service
            .create_websocket_ticket(&client, "media")
            .await
            .unwrap();
        let ticket = t["ticket"].as_str().unwrap();
        assert!(service.consume_ticket(ticket, "media").await);
        assert!(service.revoke_client(&owner, &id).unwrap());
        assert!(!service.revoke_client(&owner, &id).unwrap());
        assert!(!service.consume_ticket(ticket, "media").await);
        assert_eq!(
            service.revoke_client(&owner, &owner.id),
            Err("current_session_revoke_not_allowed")
        );
        let reopened = AuthService::new().open(dir.path()).unwrap();
        assert!(reopened.principal(&client).is_none());
    }
    #[test]
    fn scoped_grants_preserve_restrictions_and_invalidate_revoked_or_spent_codes() {
        let service = AuthService::with_bootstrap("owner");
        let (_, token) = service.browser_session("owner", None).unwrap();
        let owner = service.principal(&token).unwrap();
        let input = json!({"scopes":["orchestration:read"]});
        let revoked = service.pair(&owner, &input).unwrap();
        assert!(service
            .revoke_pairing(revoked["id"].as_str().unwrap())
            .unwrap());
        assert!(service
            .browser_session(revoked["credential"].as_str().unwrap(), None)
            .is_err());
        let issued = service.pair(&owner, &input).unwrap();
        let code = issued["credential"].as_str().unwrap();
        assert!(service
            .exchange(code, Some(vec!["access:write".into()]))
            .is_err());
        let access = service.exchange(code, None).unwrap();
        let read = service
            .principal(access["access_token"].as_str().unwrap())
            .unwrap();
        assert!(!read.unrestricted());
        assert!(!read.owner);
        assert_eq!(read.permissions, ["orchestration:read"]);
        assert!(service.exchange(code, None).is_err());
        let approval = service
            .pair(&owner, &json!({"access":"read-only"}))
            .unwrap();
        let code = approval["credential"].as_str().unwrap();
        assert!(service.browser_session(code, None).is_err());
        assert!(service
            .consume_approval_code(code, "full-access", false)
            .is_err());
        assert!(service
            .consume_approval_code(code, "read-only", false)
            .is_ok());
        assert!(service
            .consume_approval_code(code, "read-only", false)
            .is_err());
    }
}
