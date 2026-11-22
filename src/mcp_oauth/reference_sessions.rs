//! MCP client sessions kept the way the TypeScript server keeps them.
//!
//! Another environment links to this one by signing in to `/mcp` with a
//! pairing code (`PeerLinks.link` in the reference). The token it receives is
//! an MCP client session from `EnvironmentAuth.issueMcpClientSession`: claims
//! signed with HMAC-SHA256 under the `server-signing-key` secret
//! (`secrets/server-signing-key.bin`), plus a row in `auth_sessions` in
//! `statev2.sqlite`. `SessionStore.verify` accepts the token while its claims
//! have not expired and its row exists and is not revoked.
//!
//! When this server runs on a home the TypeScript server created, links made
//! there must keep working here and the other way round, and revoking one on
//! either server must cut it off on both. So this module verifies those tokens
//! exactly as `SessionStore.verify` does, and issues new ones into the same
//! table under the same key. It never creates `statev2.sqlite` or its tables:
//! a home without them keeps this server's own token store.
use base64::{engine::general_purpose::URL_SAFE_NO_PAD as B64, Engine};
use hmac::{Hmac, Mac};
use rusqlite::{params, Connection, OpenFlags, OptionalExtension};
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use std::{
    io::Write,
    path::{Path, PathBuf},
    time::Duration,
};

use super::McpAccess;

/// `MCP_CLIENT_SUBJECT` in the reference `EnvironmentAuth`.
const MCP_CLIENT_SUBJECT: &str = "mcp-client";
const BEARER: &str = "bearer-access-token";
const SIGNING_KEY: &str = "server-signing-key.bin";
/// `MCP_CLIENT_SESSION_TTL` in the reference: thirty days.
pub(super) const TTL_MS: i64 = 30 * 24 * 3_600_000;

/// `SessionClaims` in the reference `SessionStore`, in its field order.
#[derive(Serialize, Deserialize)]
struct Claims {
    v: u8,
    kind: String,
    sid: String,
    sub: String,
    scopes: Vec<String>,
    method: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    jkt: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    rtc: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    plk: Option<u8>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    rnw: Option<u8>,
    iat: f64,
    exp: f64,
}

/// A token issued into the TypeScript store.
pub(super) struct Issued {
    pub token: String,
    pub expires_ms: i64,
}

/// The TypeScript server's session store under one state directory.
pub(super) struct ReferenceSessions {
    db: PathBuf,
    secrets: PathBuf,
}

/// `mcpClientScopes` in the reference contracts.
pub(super) fn scopes_for(access: &str) -> &'static [&'static str] {
    match access {
        "read-only" => &["orchestration:read"],
        "full-access" => &[
            "orchestration:read",
            "orchestration:operate",
            "settings:write",
            "source-control:write",
        ],
        _ => &["orchestration:read", "orchestration:operate"],
    }
}

fn has_table(conn: &Connection, table: &str) -> rusqlite::Result<bool> {
    conn.query_row(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?1",
        [table],
        |_| Ok(()),
    )
    .optional()
    .map(|row| row.is_some())
}

impl ReferenceSessions {
    pub(super) fn new(dir: &Path) -> Self {
        Self {
            db: dir.join("statev2.sqlite"),
            secrets: dir.join("secrets"),
        }
    }

    /// The TypeScript database, when this home has one with an `auth_sessions` table.
    fn open(&self) -> rusqlite::Result<Option<Connection>> {
        if !self.db.is_file() {
            return Ok(None);
        }
        // No SQLITE_OPEN_CREATE: a home without the TypeScript database stays without one.
        let conn = Connection::open_with_flags(
            &self.db,
            OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )?;
        conn.busy_timeout(Duration::from_secs(5))?;
        Ok(has_table(&conn, "auth_sessions")?.then_some(conn))
    }

    fn key_path(&self) -> PathBuf {
        self.secrets.join(SIGNING_KEY)
    }

    /// The signing key, if the TypeScript server (or this one) has made it.
    fn key(&self) -> std::io::Result<Option<Vec<u8>>> {
        match std::fs::read(self.key_path()) {
            Ok(bytes) if !bytes.is_empty() => Ok(Some(bytes)),
            Ok(_) => Ok(None),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// `ServerSecretStore.getOrCreateRandom("server-signing-key", 32)`: 32 random
    /// bytes, written once with mode 0600; a key another process wrote first wins.
    fn key_or_create(&self) -> std::io::Result<Vec<u8>> {
        if let Some(key) = self.key()? {
            return Ok(key);
        }
        std::fs::create_dir_all(&self.secrets)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let _ = std::fs::set_permissions(&self.secrets, std::fs::Permissions::from_mode(0o700));
        }
        let mut options = std::fs::OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let key: [u8; 32] = crate::platform::random_bytes();
        match options.open(self.key_path()) {
            Ok(mut file) => {
                file.write_all(&key)?;
                file.sync_all()?;
                Ok(key.to_vec())
            }
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => self
                .key()?
                .ok_or_else(|| std::io::Error::other("empty signing key")),
            Err(e) => Err(e),
        }
    }

    /// What `EnvironmentAuth.authenticateMcpClient` grants `token`, or `None`
    /// when it does not accept it. `Err` is a store that could not be read.
    pub(super) fn verify(&self, token: &str, now_ms: i64) -> anyhow::Result<Option<McpAccess>> {
        let mut parts = token.split('.');
        let (Some(payload), Some(signature)) = (parts.next(), parts.next()) else {
            return Ok(None);
        };
        if payload.is_empty() || signature.is_empty() {
            return Ok(None);
        }
        let Some(key) = self.key()? else {
            return Ok(None);
        };
        let Ok(tag) = B64.decode(signature) else {
            return Ok(None);
        };
        let mut mac = Hmac::<Sha256>::new_from_slice(&key)?;
        mac.update(payload.as_bytes());
        if mac.verify_slice(&tag).is_err() {
            return Ok(None);
        }
        let Some(claims) = B64
            .decode(payload)
            .ok()
            .and_then(|json| serde_json::from_slice::<Claims>(&json).ok())
            .filter(|c| matches!(c.v, 1 | 2) && c.kind == "session")
        else {
            return Ok(None);
        };
        if claims.exp <= now_ms as f64 {
            return Ok(None);
        }
        let Some(conn) = self.open()? else {
            return Ok(None);
        };
        let row = conn
            .query_row(
                "SELECT scopes, revoked_at, client_label FROM auth_sessions WHERE session_id = ?1",
                [&claims.sid],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, Option<String>>(1)?,
                        row.get::<_, Option<String>>(2)?,
                    ))
                },
            )
            .optional()?;
        let Some((scopes, None, label)) = row else {
            return Ok(None);
        };
        if claims.sub != MCP_CLIENT_SUBJECT || claims.method != BEARER {
            return Ok(None);
        }
        let scopes: Vec<String> = serde_json::from_str(&scopes).unwrap_or_default();
        let access = if scopes.iter().any(|s| s == "orchestration:operate") {
            claims.rtc.unwrap_or_else(|| "approval-required".into())
        } else {
            "read-only".into()
        };
        Ok(Some(McpAccess {
            access,
            peer_link: claims.plk == Some(1),
            session_id: claims.sid,
            label: label.unwrap_or_default(),
        }))
    }

    /// `EnvironmentAuth.issueMcpClientSession`: a session row and its signed
    /// token. `None` when this home has no TypeScript store to issue into.
    pub(super) fn issue(
        &self,
        label: &str,
        access: &str,
        peer_link: bool,
        client: &serde_json::Value,
        now_ms: i64,
    ) -> anyhow::Result<Option<Issued>> {
        let Some(conn) = self.open()? else {
            return Ok(None);
        };
        let key = self.key_or_create()?;
        let scopes: Vec<String> = scopes_for(access).iter().map(|s| s.to_string()).collect();
        let expires_ms = now_ms + TTL_MS;
        let claims = Claims {
            v: 1,
            kind: "session".into(),
            sid: crate::platform::uuid_v4().to_string(),
            sub: MCP_CLIENT_SUBJECT.into(),
            scopes: scopes.clone(),
            method: BEARER.into(),
            jkt: None,
            rtc: (access != "read-only").then(|| access.to_owned()),
            plk: peer_link.then_some(1),
            rnw: None,
            iat: now_ms as f64,
            exp: expires_ms as f64,
        };
        let payload = B64.encode(serde_json::to_vec(&json_claims(&claims))?);
        let mut mac = Hmac::<Sha256>::new_from_slice(&key)?;
        mac.update(payload.as_bytes());
        let token = format!("{payload}.{}", B64.encode(mac.finalize().into_bytes()));
        let text = |k: &str| client.get(k).and_then(|v| v.as_str()).map(str::to_owned);
        conn.execute(
            "INSERT INTO auth_sessions (
               session_id, subject, scopes, method, client_label, client_ip_address,
               client_user_agent, client_device_type, client_os, client_browser,
               issued_at, expires_at, revoked_at, proof_key_thumbprint
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, 'bot', ?8, ?9, ?10, ?11, NULL, NULL)",
            params![
                claims.sid,
                MCP_CLIENT_SUBJECT,
                serde_json::to_string(&scopes)?,
                BEARER,
                label,
                text("ipAddress"),
                text("userAgent"),
                text("os"),
                text("browser"),
                crate::auth_service::iso(now_ms),
                crate::auth_service::iso(expires_ms),
            ],
        )?;
        Ok(Some(Issued { token, expires_ms }))
    }
}

/// The claims as the reference encodes them: `iat` and `exp` are integers.
fn json_claims(claims: &Claims) -> serde_json::Value {
    let mut value = serde_json::to_value(claims).expect("claims serialize");
    value["iat"] = serde_json::json!(claims.iat as i64);
    value["exp"] = serde_json::json!(claims.exp as i64);
    value
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    /// The `auth_sessions` shape the TypeScript 0.1.10 migrations leave behind.
    pub(crate) fn create_ts_store(dir: &Path) -> Connection {
        let conn = Connection::open(dir.join("statev2.sqlite")).unwrap();
        conn.execute_batch(
            "CREATE TABLE auth_sessions (
               session_id TEXT PRIMARY KEY,
               subject TEXT NOT NULL,
               scopes TEXT NOT NULL,
               method TEXT NOT NULL,
               client_label TEXT,
               client_ip_address TEXT,
               client_user_agent TEXT,
               client_device_type TEXT NOT NULL DEFAULT 'unknown',
               client_os TEXT,
               client_browser TEXT,
               issued_at TEXT NOT NULL,
               expires_at TEXT NOT NULL,
               last_connected_at TEXT,
               revoked_at TEXT,
               client_surface TEXT,
               client_app_version TEXT,
               proof_key_thumbprint TEXT
             );",
        )
        .unwrap();
        conn
    }

    /// A token signed as the reference `SessionStore.issue` signs one.
    pub(crate) fn reference_token(key: &[u8], claims: serde_json::Value) -> String {
        let payload = B64.encode(serde_json::to_vec(&claims).unwrap());
        let mut mac = Hmac::<Sha256>::new_from_slice(key).unwrap();
        mac.update(payload.as_bytes());
        format!("{payload}.{}", B64.encode(mac.finalize().into_bytes()))
    }

    fn insert_row(conn: &Connection, sid: &str, scopes: &str, revoked: bool) {
        conn.execute(
            "INSERT INTO auth_sessions (session_id, subject, scopes, method, client_label,
               client_device_type, issued_at, expires_at, revoked_at)
             VALUES (?1, 'mcp-client', ?2, 'bearer-access-token', 'T3 Code · fixture', 'bot',
               '2026-01-01T00:00:00.000Z', '2027-01-01T00:00:00.000Z', ?3)",
            params![sid, scopes, revoked.then_some("2026-01-02T00:00:00.000Z")],
        )
        .unwrap();
    }

    #[test]
    fn accepts_a_reference_peer_link_until_it_is_revoked_there() {
        let dir = tempfile::tempdir().unwrap();
        let conn = create_ts_store(dir.path());
        std::fs::create_dir_all(dir.path().join("secrets")).unwrap();
        let key = [7u8; 32];
        std::fs::write(dir.path().join("secrets").join(SIGNING_KEY), key).unwrap();
        let sessions = ReferenceSessions::new(dir.path());
        let now = 1_800_000_000_000i64;
        let claims = |sid: &str| {
            serde_json::json!({"v":1,"kind":"session","sid":sid,"sub":"mcp-client",
                "scopes":["orchestration:read","orchestration:operate"],
                "method":"bearer-access-token","rtc":"auto","plk":1,"iat":now - 1000,"exp":now + 60_000})
        };
        let token = reference_token(&key, claims("11111111-1111-4111-8111-111111111111"));
        // No row yet: unknown to the store.
        assert_eq!(sessions.verify(&token, now).unwrap(), None);
        insert_row(
            &conn,
            "11111111-1111-4111-8111-111111111111",
            r#"["orchestration:read","orchestration:operate"]"#,
            false,
        );
        assert_eq!(
            sessions
                .verify(&token, now)
                .unwrap()
                .map(|a| (a.access, a.peer_link, a.label)),
            Some(("auto".into(), true, "T3 Code · fixture".into()))
        );
        // Expired claims, a forged signature and a revoked row are all refused.
        assert_eq!(sessions.verify(&token, now + 60_000).unwrap(), None);
        let forged = reference_token(&[8u8; 32], claims("11111111-1111-4111-8111-111111111111"));
        assert_eq!(sessions.verify(&forged, now).unwrap(), None);
        conn.execute(
            "UPDATE auth_sessions SET revoked_at = '2026-01-02T00:00:00.000Z'",
            [],
        )
        .unwrap();
        assert_eq!(sessions.verify(&token, now).unwrap(), None);
        // A browser session signed with the same key is not an MCP credential.
        let mut browser = claims("22222222-2222-4222-8222-222222222222");
        browser["sub"] = "browser".into();
        browser["method"] = "browser-session-cookie".into();
        insert_row(
            &conn,
            "22222222-2222-4222-8222-222222222222",
            r#"["orchestration:read"]"#,
            false,
        );
        assert_eq!(
            sessions
                .verify(&reference_token(&key, browser), now)
                .unwrap(),
            None
        );
    }

    #[test]
    fn issues_into_the_reference_store_and_reads_its_own_tokens_back() {
        let dir = tempfile::tempdir().unwrap();
        let sessions = ReferenceSessions::new(dir.path());
        let now = 1_800_000_000_000i64;
        // A home without the TypeScript store keeps this server's own tokens.
        assert!(sessions
            .issue("x", "read-only", true, &serde_json::json!({}), now)
            .unwrap()
            .is_none());
        assert!(!dir.path().join("statev2.sqlite").exists());
        let conn = create_ts_store(dir.path());
        let issued = sessions
            .issue(
                "T3 Code · fixture",
                "read-only",
                true,
                &serde_json::json!({"userAgent":"fixture"}),
                now,
            )
            .unwrap()
            .unwrap();
        assert_eq!(issued.expires_ms, now + TTL_MS);
        assert_eq!(
            sessions
                .verify(&issued.token, now)
                .unwrap()
                .map(|a| (a.access, a.peer_link)),
            Some(("read-only".into(), true))
        );
        let (subject, scopes, label, device, agent): (String, String, String, String, String) = conn
            .query_row(
                "SELECT subject, scopes, client_label, client_device_type, client_user_agent FROM auth_sessions",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?)),
            )
            .unwrap();
        assert_eq!(
            (
                subject.as_str(),
                scopes.as_str(),
                label.as_str(),
                device.as_str(),
                agent.as_str()
            ),
            (
                "mcp-client",
                r#"["orchestration:read"]"#,
                "T3 Code · fixture",
                "bot",
                "fixture"
            )
        );
        let key = std::fs::read(dir.path().join("secrets").join(SIGNING_KEY)).unwrap();
        assert_eq!(key.len(), 32);
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(dir.path().join("secrets").join(SIGNING_KEY))
                .unwrap()
                .permissions()
                .mode();
            assert_eq!(mode & 0o777, 0o600);
        }
    }
}
