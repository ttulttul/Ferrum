//! Pairing links issued out of process into the TypeScript server's database.
//!
//! The desktop app pairs with an SSH host by running `t3 auth pairing create
//! --base-dir ~/.t3 --json` on it (`REMOTE_PAIRING_SCRIPT` in the reference
//! `packages/ssh/src/tunnel.ts`). That CLI writes a one-time link into
//! `userdata/statev2.sqlite` (`auth_pairing_links`) and prints the credential,
//! which the desktop then exchanges at `/oauth/token` on whatever server owns
//! that home. This server keeps its own links in `environment-auth.json`, so it
//! also has to spend links from that table, exactly once, like the TypeScript
//! `PairingGrantStore.consume` does.
//!
//! The table belongs to the TypeScript schema. This module never creates the
//! file, runs migrations, or changes anything but `consumed_at` on the one row
//! it spends.
use rusqlite::{params, Connection, OpenFlags, OptionalExtension};
use std::{path::Path, time::Duration};

/// Subjects with this prefix are agent-approval codes; plain exchanges cannot spend them.
const MCP_APPROVAL_SUBJECT_PREFIX: &str = "mcp-approval:";
const PAIRING_CODE_ALPHABET: &str = "23456789ABCDEFGHJKLMNPQRSTUVWXYZ";
const PAIRING_CODE_LENGTH: usize = 12;

/// A link spent from the TypeScript store.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ExternalGrant {
    pub id: String,
    pub scopes: Vec<String>,
    pub label: Option<String>,
}

/// Issued pairing codes are case-insensitive and may be typed in dashed groups;
/// any other credential is matched as is (`PairingGrantStore.consume`).
pub(crate) fn normalize(credential: &str) -> String {
    let compact: String = credential
        .chars()
        .filter(|c| !c.is_whitespace() && *c != '-')
        .collect();
    let upper = compact.to_ascii_uppercase();
    if upper.len() == PAIRING_CODE_LENGTH
        && upper.chars().all(|c| PAIRING_CODE_ALPHABET.contains(c))
    {
        upper
    } else {
        credential.to_owned()
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

fn has_column(conn: &Connection, table: &str, column: &str) -> rusqlite::Result<bool> {
    let mut statement = conn.prepare(&format!("PRAGMA table_info({table})"))?;
    let mut rows = statement.query([])?;
    while let Some(row) = rows.next()? {
        if row.get::<_, String>(1)? == column {
            return Ok(true);
        }
    }
    Ok(false)
}

/// What happened to a credential presented against the TypeScript store.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Outcome {
    Spent(ExternalGrant),
    /// No spendable link: unknown, spent, expired, revoked, bound, or no TypeScript store.
    NotFound,
    /// A spendable link exists but grants none of the requested scopes. It was not spent.
    ScopeNotGranted,
}

/// Atomically spends an unbound, unexpired, unrevoked link that grants at least one of
/// `requested`, like `AuthPairingLinks.consumeAvailable`: the scope check is part of the
/// UPDATE, so a request that is refused never spends the link.
pub(crate) fn consume(
    db: &Path,
    credential: &str,
    requested: Option<&[String]>,
    now: &str,
) -> rusqlite::Result<Outcome> {
    if !db.is_file() {
        return Ok(Outcome::NotFound);
    }
    // No SQLITE_OPEN_CREATE: a home without the TypeScript database stays without one.
    let conn = Connection::open_with_flags(
        db,
        OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )?;
    conn.busy_timeout(Duration::from_secs(5))?;
    if !has_table(&conn, "auth_pairing_links")? {
        return Ok(Outcome::NotFound);
    }
    // ?1 now, ?2 credential, ?3 MCP approval subject pattern.
    let mut available = String::from(
        "credential = ?2
           AND revoked_at IS NULL
           AND consumed_at IS NULL
           AND expires_at > ?1
           AND subject NOT LIKE ?3",
    );
    // Links bound to a device key or waiting on T3 Connect approval come from the
    // TypeScript server's own Connect flow, never from the CLI; leave them alone.
    if has_column(&conn, "auth_pairing_links", "proof_key_thumbprint")? {
        available.push_str(" AND proof_key_thumbprint IS NULL");
    }
    if has_column(&conn, "auth_pairing_links", "requires_connect_approval")? {
        available.push_str(" AND requires_connect_approval = 0");
    }
    let credential = normalize(credential);
    let subjects = format!("{MCP_APPROVAL_SUBJECT_PREFIX}%");
    let requested_json =
        requested.map(|r| serde_json::to_string(r).unwrap_or_else(|_| "[]".into()));
    let spent = conn
        .query_row(
            &format!(
                "UPDATE auth_pairing_links
                 SET consumed_at = ?1
                 WHERE {available}
                   AND (?4 IS NULL OR EXISTS (
                     SELECT 1 FROM json_each(?4) AS requested
                     WHERE requested.value IN (SELECT value FROM json_each(auth_pairing_links.scopes))
                   ))
                 RETURNING id, scopes, label"
            ),
            params![now, credential, subjects, requested_json],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, Option<String>>(2)?,
                ))
            },
        )
        .optional()?;
    if let Some((id, scopes, label)) = spent {
        return Ok(Outcome::Spent(ExternalGrant {
            id,
            scopes: serde_json::from_str(&scopes).unwrap_or_default(),
            label: label.filter(|l| !l.is_empty()),
        }));
    }
    if requested.is_some() {
        let unspent = conn
            .query_row(
                &format!("SELECT 1 FROM auth_pairing_links WHERE {available}"),
                params![now, credential, subjects],
                |_| Ok(()),
            )
            .optional()?;
        if unspent.is_some() {
            return Ok(Outcome::ScopeNotGranted);
        }
    }
    Ok(Outcome::NotFound)
}

fn open_sessions(db: &Path) -> rusqlite::Result<Option<Connection>> {
    if !db.is_file() {
        return Ok(None);
    }
    let conn = Connection::open_with_flags(
        db,
        OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )?;
    conn.busy_timeout(Duration::from_secs(5))?;
    Ok(has_table(&conn, "auth_sessions")?.then_some(conn))
}

/// The client sessions the TypeScript server keeps in `auth_sessions` (its
/// browsers and desktop app, outside agents, linked environments) that are
/// neither revoked nor expired, as its Connections list shows them
/// (`AuthSessions.listActive`, `toAuthClientSession`): newest first, none of
/// them connected here.
pub(crate) fn client_sessions(db: &Path, now: &str) -> rusqlite::Result<Vec<serde_json::Value>> {
    use serde_json::json;
    let Some(conn) = open_sessions(db)? else {
        return Ok(Vec::new());
    };
    let last_connected = if has_column(&conn, "auth_sessions", "last_connected_at")? {
        "last_connected_at"
    } else {
        "NULL"
    };
    let mut statement = conn.prepare(&format!(
        "SELECT session_id, subject, scopes, method, client_label, client_ip_address,
                client_user_agent, client_device_type, client_os, client_browser,
                issued_at, expires_at, {last_connected}
         FROM auth_sessions
         WHERE revoked_at IS NULL AND expires_at > ?1
         ORDER BY issued_at DESC, session_id DESC"
    ))?;
    let rows = statement.query_map([now], |row| {
        let text = |i: usize| row.get::<_, Option<String>>(i);
        let scopes: Vec<String> =
            serde_json::from_str(&row.get::<_, String>(2)?).unwrap_or_default();
        let legacy: Vec<&String> = scopes
            .iter()
            .filter(|s| super::LEGACY.contains(&s.as_str()))
            .collect();
        // `toClientMetadata`: a field is listed only when it has a value.
        let mut client = serde_json::Map::new();
        for (key, index) in [("label", 4), ("ipAddress", 5), ("userAgent", 6)] {
            if let Some(value) = text(index)?.filter(|v| !v.is_empty()) {
                client.insert(key.into(), json!(value));
            }
        }
        client.insert(
            "deviceType".into(),
            json!(text(7)?.unwrap_or_else(|| "unknown".into())),
        );
        for (key, index) in [("os", 8), ("browser", 9)] {
            if let Some(value) = text(index)?.filter(|v| !v.is_empty()) {
                client.insert(key.into(), json!(value));
            }
        }
        Ok(json!({
            "sessionId": row.get::<_, String>(0)?,
            "subject": row.get::<_, String>(1)?,
            "scopes": legacy,
            "permissions": scopes,
            "method": row.get::<_, String>(3)?,
            "client": client,
            "issuedAt": row.get::<_, String>(10)?,
            "expiresAt": row.get::<_, String>(11)?,
            "lastConnectedAt": text(12)?,
            "connected": false,
            "current": false,
        }))
    })?;
    rows.collect()
}

/// Revokes client sessions the TypeScript server, or this one on a shared
/// home, keeps in `auth_sessions`, as `AuthSessions.revoke` does: `revoked_at`
/// is set once and never cleared. `id` picks one session, and every session of
/// the same T3 Connect device with it (`revokeConnectDevice`); `None` revokes
/// them all (revoking every other client, since this server's own session is
/// not among them). Returns the session ids it revoked.
pub(crate) fn revoke_clients(
    db: &Path,
    id: Option<&str>,
    now: &str,
) -> rusqlite::Result<Vec<String>> {
    let Some(conn) = open_sessions(db)? else {
        return Ok(Vec::new());
    };
    let device: Option<String> = match id {
        Some(id) if has_column(&conn, "auth_sessions", "proof_key_thumbprint")? => conn
            .query_row(
                "SELECT proof_key_thumbprint FROM auth_sessions WHERE session_id = ?1",
                [id],
                |row| row.get(0),
            )
            .optional()?
            .flatten(),
        _ => None,
    };
    let mut statement = conn.prepare(if device.is_some() {
        "UPDATE auth_sessions SET revoked_at = ?1
         WHERE revoked_at IS NULL AND (session_id = ?2 OR proof_key_thumbprint = ?3)
         RETURNING session_id"
    } else {
        "UPDATE auth_sessions SET revoked_at = ?1
         WHERE revoked_at IS NULL AND (?2 IS NULL OR session_id = ?2) AND ?3 IS NULL
         RETURNING session_id"
    })?;
    let ids = statement
        .query_map(params![now, id, device], |row| row.get::<_, String>(0))?
        .collect();
    ids
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    /// The `auth_pairing_links` shape the TypeScript 0.1.10 migrations leave behind.
    pub(crate) fn create_ts_store(db: &Path) -> Connection {
        let conn = Connection::open(db).unwrap();
        conn.execute_batch(
            "CREATE TABLE auth_pairing_links (
               id TEXT PRIMARY KEY,
               credential TEXT NOT NULL UNIQUE,
               method TEXT NOT NULL,
               scopes TEXT NOT NULL,
               subject TEXT NOT NULL,
               label TEXT,
               created_at TEXT NOT NULL,
               expires_at TEXT NOT NULL,
               consumed_at TEXT,
               revoked_at TEXT,
               proof_key_thumbprint TEXT,
               requires_connect_approval INTEGER NOT NULL DEFAULT 0,
               requested_client_json TEXT,
               connect_denied INTEGER NOT NULL DEFAULT 0
             );",
        )
        .unwrap();
        conn
    }

    pub(crate) fn insert(
        conn: &Connection,
        id: &str,
        credential: &str,
        scopes: &[&str],
        subject: &str,
        expires_at: &str,
    ) {
        conn.execute(
            "INSERT INTO auth_pairing_links (id, credential, method, scopes, subject, label, created_at, expires_at)
             VALUES (?1, ?2, 'one-time-token', ?3, ?4, NULL, '2026-01-01T00:00:00.000Z', ?5)",
            params![id, credential, serde_json::to_string(scopes).unwrap(), subject, expires_at],
        )
        .unwrap();
    }

    const NOW: &str = "2026-06-01T00:00:00.000Z";
    const LATER: &str = "2026-06-01T00:05:00.000Z";

    #[test]
    fn spends_a_cli_link_once_and_leaves_others_alone() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("statev2.sqlite");
        let conn = create_ts_store(&db);
        insert(
            &conn,
            "a",
            "ABCDEFGHJKMN",
            &["orchestration:read"],
            "one-time-token",
            LATER,
        );
        insert(
            &conn,
            "old",
            "ZZZZZZZZZZZZ",
            &["orchestration:read"],
            "one-time-token",
            NOW,
        );
        insert(
            &conn,
            "mcp",
            "YYYYYYYYYYYY",
            &["orchestration:read"],
            "mcp-approval:read-only",
            LATER,
        );
        insert(
            &conn,
            "bound",
            "XXXXXXXXXXXX",
            &["orchestration:read"],
            "one-time-token",
            LATER,
        );
        conn.execute(
            "UPDATE auth_pairing_links SET proof_key_thumbprint = 'k' WHERE id = 'bound'",
            [],
        )
        .unwrap();

        // Codes are case-insensitive and may be typed in dashed groups.
        let Outcome::Spent(grant) = consume(&db, "abcd-efgh-jkmn", None, NOW).unwrap() else {
            panic!("CLI link was not spent");
        };
        assert_eq!(grant.id, "a");
        assert_eq!(grant.scopes, vec!["orchestration:read".to_owned()]);
        let consumed: Option<String> = conn
            .query_row(
                "SELECT consumed_at FROM auth_pairing_links WHERE id = 'a'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(consumed.as_deref(), Some(NOW));
        assert_eq!(
            consume(&db, "ABCDEFGHJKMN", None, NOW).unwrap(),
            Outcome::NotFound
        );
        // Expired (expires_at == now), MCP approval codes and device-bound links are not spendable.
        for credential in ["ZZZZZZZZZZZZ", "YYYYYYYYYYYY", "XXXXXXXXXXXX"] {
            assert_eq!(
                consume(&db, credential, None, NOW).unwrap(),
                Outcome::NotFound
            );
        }
        let untouched: i64 = conn
            .query_row(
                "SELECT count(*) FROM auth_pairing_links WHERE consumed_at IS NULL",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(untouched, 3);
    }

    #[test]
    fn rejected_scope_requests_do_not_spend_the_link() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("statev2.sqlite");
        let conn = create_ts_store(&db);
        insert(
            &conn,
            "a",
            "ABCDEFGHJKMN",
            &["orchestration:read", "terminal:read"],
            "one-time-token",
            LATER,
        );
        let disjoint = ["settings:write".to_owned()];
        assert_eq!(
            consume(&db, "ABCDEFGHJKMN", Some(&disjoint), NOW).unwrap(),
            Outcome::ScopeNotGranted
        );
        // An unknown credential with scopes is still just unknown.
        assert_eq!(
            consume(&db, "ABCDEFGHJKMP", Some(&disjoint), NOW).unwrap(),
            Outcome::NotFound
        );
        // Overlap is enough, as in the TypeScript OAuth exchange; the caller narrows the scopes.
        let overlapping = ["orchestration:read".to_owned(), "settings:write".to_owned()];
        assert!(matches!(
            consume(&db, "ABCDEFGHJKMN", Some(&overlapping), NOW).unwrap(),
            Outcome::Spent(_)
        ));
    }

    #[test]
    fn missing_store_or_table_is_not_created() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("statev2.sqlite");
        assert_eq!(
            consume(&db, "ABCDEFGHJKMN", None, NOW).unwrap(),
            Outcome::NotFound
        );
        assert!(!db.exists());
        Connection::open(&db).unwrap();
        assert_eq!(
            consume(&db, "ABCDEFGHJKMN", None, NOW).unwrap(),
            Outcome::NotFound
        );
    }

    /// The `auth_sessions` shape the TypeScript migrations leave behind.
    fn sessions_table(dir: &Path) -> Connection {
        let conn = Connection::open(dir.join("statev2.sqlite")).unwrap();
        conn.execute_batch(
            "CREATE TABLE auth_sessions (
               session_id TEXT PRIMARY KEY, subject TEXT NOT NULL, scopes TEXT NOT NULL,
               method TEXT NOT NULL, client_label TEXT, client_ip_address TEXT,
               client_user_agent TEXT, client_device_type TEXT NOT NULL DEFAULT 'unknown',
               client_os TEXT, client_browser TEXT, issued_at TEXT NOT NULL,
               expires_at TEXT NOT NULL, last_connected_at TEXT, revoked_at TEXT,
               client_surface TEXT, client_app_version TEXT, proof_key_thumbprint TEXT
             );",
        )
        .unwrap();
        conn
    }

    fn session(
        conn: &Connection,
        id: &str,
        subject: &str,
        scopes: &str,
        issued: &str,
        device: Option<&str>,
    ) {
        conn.execute(
            "INSERT INTO auth_sessions (session_id, subject, scopes, method, client_label, client_user_agent,
               client_device_type, client_os, issued_at, expires_at, revoked_at, proof_key_thumbprint)
             VALUES (?1, ?2, ?3, 'browser-session-cookie', ?1, 'Fixture/1.0', 'desktop', 'Linux', ?4,
               '2027-01-01T00:00:00.000Z', NULL, ?5)",
            params![id, subject, scopes, issued, device],
        )
        .unwrap();
    }

    #[test]
    fn lists_live_sessions_newest_first_in_the_reference_shape() {
        let dir = tempfile::tempdir().unwrap();
        let conn = sessions_table(dir.path());
        let db = dir.path().join("statev2.sqlite");
        session(
            &conn,
            "old",
            "one-time-token",
            r#"["orchestration:read","files:read"]"#,
            "2026-01-01T00:00:00.000Z",
            None,
        );
        session(
            &conn,
            "new",
            "desktop-bootstrap",
            r#"["access:write"]"#,
            "2026-02-01T00:00:00.000Z",
            None,
        );
        session(
            &conn,
            "gone",
            "mcp-client",
            "[]",
            "2026-03-01T00:00:00.000Z",
            None,
        );
        conn.execute("UPDATE auth_sessions SET revoked_at = '2026-03-02T00:00:00.000Z' WHERE session_id = 'gone'", [])
            .unwrap();
        let listed = client_sessions(&db, NOW).unwrap();
        assert_eq!(
            listed
                .iter()
                .map(|r| r["sessionId"].as_str().unwrap())
                .collect::<Vec<_>>(),
            ["new", "old"]
        );
        let old = &listed[1];
        assert_eq!(old["scopes"], serde_json::json!(["orchestration:read"]));
        assert_eq!(
            old["permissions"],
            serde_json::json!(["orchestration:read", "files:read"])
        );
        assert_eq!(
            old["client"],
            serde_json::json!({"label":"old","userAgent":"Fixture/1.0","deviceType":"desktop","os":"Linux"})
        );
        assert_eq!(old["connected"], false);
        // Expired sessions are not listed.
        assert!(client_sessions(&db, "2028-01-01T00:00:00.000Z")
            .unwrap()
            .is_empty());
    }

    #[test]
    fn revoking_a_device_session_revokes_the_device_and_others_revokes_all() {
        let dir = tempfile::tempdir().unwrap();
        let conn = sessions_table(dir.path());
        let db = dir.path().join("statev2.sqlite");
        session(
            &conn,
            "phone-1",
            "cloud-connect",
            "[]",
            "2026-01-01T00:00:00.000Z",
            Some("key"),
        );
        session(
            &conn,
            "phone-2",
            "cloud-connect",
            "[]",
            "2026-01-02T00:00:00.000Z",
            Some("key"),
        );
        session(
            &conn,
            "laptop",
            "one-time-token",
            "[]",
            "2026-01-03T00:00:00.000Z",
            None,
        );
        let mut revoked = revoke_clients(&db, Some("phone-1"), NOW).unwrap();
        revoked.sort();
        assert_eq!(revoked, ["phone-1", "phone-2"]);
        assert_eq!(
            revoke_clients(&db, Some("laptop"), NOW).unwrap(),
            ["laptop"]
        );
        assert!(revoke_clients(&db, Some("laptop"), NOW).unwrap().is_empty());
        session(
            &conn,
            "agent",
            "mcp-client",
            "[]",
            "2026-01-04T00:00:00.000Z",
            None,
        );
        assert_eq!(revoke_clients(&db, None, NOW).unwrap(), ["agent"]);
    }
}
