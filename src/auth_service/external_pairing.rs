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

/// Revokes MCP client sessions (linked environments and outside agents) that the
/// TypeScript server, or this one on a shared home, keeps in `auth_sessions`, as
/// `AuthSessions.revoke` does: `revoked_at` is set once and never cleared. `id`
/// picks one session; `None` revokes them all (revoking every other client).
/// Returns the session ids it revoked.
pub(crate) fn revoke_mcp_clients(
    db: &Path,
    id: Option<&str>,
    now: &str,
) -> rusqlite::Result<Vec<String>> {
    if !db.is_file() {
        return Ok(Vec::new());
    }
    let conn = Connection::open_with_flags(
        db,
        OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )?;
    conn.busy_timeout(Duration::from_secs(5))?;
    if !has_table(&conn, "auth_sessions")? {
        return Ok(Vec::new());
    }
    let mut statement = conn.prepare(
        "UPDATE auth_sessions SET revoked_at = ?1
         WHERE subject = 'mcp-client' AND revoked_at IS NULL
           AND (?2 IS NULL OR session_id = ?2)
         RETURNING session_id",
    )?;
    let ids = statement
        .query_map(params![now, id], |row| row.get::<_, String>(0))?
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
}
