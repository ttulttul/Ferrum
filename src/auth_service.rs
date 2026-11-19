pub(crate) mod permissions;
pub(crate) mod storage;
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use hmac::{Hmac, Mac};
use rand::{rngs::OsRng, Rng, RngCore};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{
    collections::HashMap,
    path::Path,
    sync::{Arc, Mutex},
};
use storage::Storage;
use time::OffsetDateTime;
use uuid::Uuid;

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
    (OffsetDateTime::now_utc().unix_timestamp_nanos() / 1_000_000) as i64
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
    let mut bytes = [0; 32];
    OsRng.fill_bytes(&mut bytes);
    bytes
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
    pub expires: i64,
    pub owner: bool,
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
    id: String,
    scopes: Vec<String>,
    expires: i64,
    purpose: Option<String>,
    access: Option<String>,
}
#[derive(Default, Clone, Serialize, Deserialize)]
struct Credentials {
    sessions: HashMap<String, Session>,
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
    invalidations: tokio::sync::watch::Sender<()>,
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
            invalidations: tokio::sync::watch::channel(()).0,
        }
    }
    pub fn with_port(mut self, port: u16) -> Self {
        self.port = port;
        self
    }
    pub fn open(mut self, dir: &Path) -> anyhow::Result<Self> {
        self.records = Arc::new(Storage::open(&dir.join("environment-auth.json"))?);
        Ok(self)
    }
    pub(crate) fn port(&self) -> u16 {
        self.port
    }
    pub fn session_cookie_name(&self) -> String {
        format!("t3_session_{}", self.port)
    }
    fn descriptor(&self) -> Value {
        json!({"policy":if self.desktop {"desktop-managed-local"}else{"loopback-browser"},"bootstrapMethods":[if self.desktop {"desktop-bootstrap"}else{"one-time-token"}],"sessionMethods":[BROWSER,BEARER,"dpop-access-token"],"sessionCookieName":self.session_cookie_name(),"serverUpdateScope":"environment:maintain"})
    }
    pub async fn invalidated(&self, session: &Session) {
        let mut changes = self.invalidations.subscribe();
        loop {
            let active = self.records.read(|r| {
                r.sessions
                    .values()
                    .any(|s| s.id == session.id && s.expires > now())
            });
            if !active {
                return;
            }
            let until_expiry = std::time::Duration::from_millis(
                session.expires.saturating_sub(now()).max(0) as u64,
            );
            if tokio::time::timeout(until_expiry, changes.changed())
                .await
                .is_err()
            {
                return;
            }
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
        self.issue(credential, BROWSER, None, None)
    }
    pub fn browser_session(
        &self,
        credential: &str,
        previous: Option<&str>,
    ) -> Result<(Value, String), &'static str> {
        self.issue(credential, BROWSER, None, previous)
    }
    fn issue(
        &self,
        credential: &str,
        method: &str,
        requested: Option<Vec<String>>,
        replace: Option<&str>,
    ) -> Result<(Value, String), &'static str> {
        let at = now();
        let owner = !credential.is_empty()
            && self.bootstrap_expires > at
            && key(&self.bootstrap) == key(credential);
        let result=self.records.update(|records| {
            let mut permissions = if owner {
                STANDARD.iter().map(|s|s.to_string()).collect()
            } else {
                records.links.remove(&key(&credential.to_ascii_uppercase()))
                    .filter(|g|g.expires>at && g.purpose.is_none() && g.access.is_none())
                    .ok_or("invalid_credential")?.scopes
            };
            if let Some(requested)=requested {
                if requested.is_empty() || requested.iter().any(|s|!permissions.contains(s)){return Err("invalid_scope")}
                permissions=requested;
            }
            let session=Session {
                id:Uuid::new_v4().to_string(), owner, method:method.into(), expires:at+30*DAY, permissions
            };
            if let Some(previous)=replace {
                let k=key(previous);
                if records.sessions.get(&k).is_some_and(|s|s.method==BROWSER){records.sessions.remove(&k);}
            }
            let claims=json!({"v":1,"kind":"session","sid":session.id,"sub":if owner{"desktop-bootstrap"}else{"one-time-token"},"scopes":session.permissions,"method":method,"iat":at,"exp":session.expires});
            let token=signed(records.signing_key.get_or_insert_with(random_key),&claims);
            let response=if method==BROWSER {session.metadata()}else{
                json!({"access_token":token,"issued_token_type":"urn:ietf:params:oauth:token-type:access_token","token_type":"Bearer","expires_in":30*86400,"scope":session.permissions.join(" ")})
            };
            records.sessions.insert(key(&token),session);
            Ok((response,token))
        });
        if replace.is_some() && result.is_ok() {
            self.invalidations.send_replace(());
        }
        result
    }
    pub fn consume_approval_code(
        &self,
        code: &str,
        access: &str,
        peer_link: bool,
    ) -> Result<String, &'static str> {
        let k = key(&code.trim().to_ascii_uppercase());
        self.records.update(|r| {
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
            if grant.purpose.as_deref().is_some_and(|_| !peer_link)
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
        self.issue(credential, BEARER, requested, None).map(|v| v.0)
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
        let id = Uuid::new_v4().to_string();
        let expires = now() + 300_000;
        let alphabet = b"23456789ABCDEFGHJKLMNPQRSTUVWXYZ";
        let mut code: String = (0..12)
            .map(|_| alphabet[OsRng.gen_range(0..alphabet.len())] as char)
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
        self.records.update(|r| {
            r.links.retain(|_, g| g.expires > now());
            r.links.insert(
                key(&code),
                Grant {
                    id,
                    expires,
                    scopes: permissions,
                    purpose: purpose.map(String::from),
                    access: access.map(String::from),
                },
            );
            Ok(response)
        })
    }
    pub fn revoke_pairing(&self, id: &str) -> Result<bool, &'static str> {
        self.records.update(|r| {
            let n = r.links.len();
            r.links.retain(|_, g| g.id != id);
            Ok(r.links.len() != n)
        })
    }
    pub fn revoke_client(&self, current: &Session, id: &str) -> Result<bool, &'static str> {
        if current.id == id {
            return Err("current_session_revoke_not_allowed");
        }
        let result = self.records.update(|r| {
            let n = r.sessions.len();
            r.sessions.retain(|_, s| s.id != id);
            Ok(n != r.sessions.len())
        });
        self.invalidations.send_replace(());
        result
    }
    pub fn revoke_others(&self, current: &Session) -> Result<usize, &'static str> {
        let result = self.records.update(|r| {
            let mut n = 0;
            r.sessions.retain(|_, s| {
                if s.id == current.id {
                    return true;
                }
                if s.expires > now() {
                    n += 1
                }
                false
            });
            Ok(n)
        });
        self.invalidations.send_replace(());
        result
    }
    pub async fn authenticate_bearer(&self, token: &str) -> bool {
        self.principal(token).is_some()
    }
    pub async fn revoke_session(&self, token: &str) -> bool {
        let result = self
            .records
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
        let k = key(token);
        let mut tickets = self.tickets.lock().unwrap();
        let t = tickets
            .get(&k)
            .filter(|t| t.expires > now() && (t.audience == "any" || t.audience == kind))?;
        let s = self.records.read(|r| {
            r.sessions
                .get(&t.session)
                .filter(|s| s.expires > now())
                .cloned()
        });
        if kind == "rpc" {
            tickets.remove(&k);
        }
        s
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
