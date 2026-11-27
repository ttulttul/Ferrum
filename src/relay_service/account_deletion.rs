//! A verified account-deletion event is acknowledged after the relay has durably queued it.
use super::RelayService;
use axum::http::{HeaderMap, StatusCode};
use base64::{engine::general_purpose::STANDARD, Engine};
use hmac::{Hmac, Mac};
use rusqlite::{params, TransactionBehavior};
use serde::Deserialize;
use serde_json::Value;
use sha2::Sha256;
use std::time::Duration;
use time::OffsetDateTime;

fn verified(secret: &str, headers: &HeaderMap, body: &[u8], now: i64) -> bool {
    let header = |name: &str| {
        headers
            .get(name)
            .and_then(|h| h.to_str().ok())
            .filter(|s| !s.is_empty())
    };
    let (Some(id), Some(timestamp), Some(signatures)) = (
        header("svix-id"),
        header("svix-timestamp"),
        header("svix-signature"),
    ) else {
        return false;
    };
    if !timestamp
        .parse::<i64>()
        .is_ok_and(|t| now.abs_diff(t) <= 300)
    {
        return false;
    }
    let Ok(key) = STANDARD.decode(secret.strip_prefix("whsec_").unwrap_or(secret)) else {
        return false;
    };
    let mut mac = Hmac::<Sha256>::new_from_slice(&key).expect("HMAC accepts a key of any length");
    mac.update(id.as_bytes());
    mac.update(b".");
    mac.update(timestamp.as_bytes());
    mac.update(b".");
    mac.update(body);
    signatures
        .split_whitespace()
        .filter_map(|s| s.strip_prefix("v1,"))
        .filter_map(|s| STANDARD.decode(s).ok())
        .any(|tag| mac.clone().verify_slice(&tag).is_ok())
}

#[derive(Deserialize)]
struct ClerkEvent {
    #[serde(rename = "type")]
    kind: String,
    data: Value,
}

impl RelayService {
    pub(crate) fn clerk_webhook(&self, headers: &HeaderMap, body: &[u8]) -> StatusCode {
        let Some(secret) = self.clerk_webhook_secret.as_deref() else {
            return StatusCode::NOT_FOUND;
        };
        let now = OffsetDateTime::now_utc().unix_timestamp();
        if !verified(secret, headers, body, now) {
            return StatusCode::UNAUTHORIZED;
        }
        let Ok(event) = serde_json::from_slice::<ClerkEvent>(body) else {
            return StatusCode::BAD_REQUEST;
        };
        if event.kind != "user.deleted" {
            return StatusCode::NO_CONTENT;
        }
        let Some(owner) = event.data["id"].as_str().filter(|s| !s.is_empty()) else {
            return StatusCode::BAD_REQUEST;
        };
        match self.db.lock().unwrap().execute(
            "INSERT INTO account_deletion(owner,requested) VALUES(?1,?2) ON CONFLICT(owner) DO NOTHING",
            params![owner,now],
        ) {
            Ok(_) => StatusCode::OK,
            Err(error) => {
                tracing::error!(%error, "could not record account deletion");
                StatusCode::INTERNAL_SERVER_ERROR
            }
        }
    }

    pub async fn sweep_account_deletions(self) {
        let mut ticks = tokio::time::interval(Duration::from_secs(5 * 60));
        ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            ticks.tick().await;
            let relay = self.clone();
            match tokio::task::spawn_blocking(move || {
                relay.process_deletions(OffsetDateTime::now_utc().unix_timestamp())
            })
            .await
            {
                Ok(Ok(())) => {}
                Ok(Err(error)) => tracing::error!(%error,"could not sweep account deletions"),
                Err(error) => tracing::error!(%error,"account deletion task failed"),
            }
        }
    }

    fn process_deletions(&self, now: i64) -> rusqlite::Result<()> {
        let mut db = self.db.lock().unwrap();
        let owners: Vec<String> = db
            .prepare(
                "SELECT owner FROM account_deletion WHERE completed IS NULL
                      ORDER BY attempts, requested LIMIT 25",
            )?
            .query_map([], |row| row.get(0))?
            .collect::<rusqlite::Result<_>>()?;
        for owner in owners {
            let mut delete = || -> rusqlite::Result<()> {
                let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
                // Only erase identity-scoped records for the deleted owner's unshared links.
                for table in ["activity", "credential"] {
                    tx.execute(&format!("DELETE FROM {table} WHERE (id,pubkey) IN
                        (SELECT id,pubkey FROM link WHERE owner=?1 EXCEPT SELECT id,pubkey FROM link WHERE owner<>?1)"),[&owner])?;
                }
                for table in ["link", "consent", "activity_registration", "device"] {
                    tx.execute(&format!("DELETE FROM {table} WHERE owner=?"), [&owner])?;
                }
                // Previously issued access tokens may recreate rows for 30 minutes.
                tx.execute("UPDATE account_deletion SET completed=?2 WHERE owner=?1 AND requested<=?2-2100",params![owner,now])?;
                tx.commit()
            };
            if let Err(error) = delete() {
                tracing::warn!(%error,"account deletion will be retried");
                db.execute(
                    "UPDATE account_deletion SET attempts=attempts+1 WHERE owner=?",
                    [owner],
                )?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deletion_survives_restart_and_repeats_until_old_access_tokens_expire() {
        let dir = tempfile::tempdir().unwrap();
        let address = "127.0.0.1:0".parse().unwrap();
        let mut relay = RelayService::open(dir.path(), address).unwrap();
        relay.clerk_webhook_secret = Some("whsec_dGVzdA==".into());
        let now = OffsetDateTime::now_utc().unix_timestamp();
        let body = br#"{"type":"user.deleted","data":{"id":"departed"}}"#;
        let mut tag = Hmac::<Sha256>::new_from_slice(b"test").unwrap();
        tag.update(format!("fixture.{now}.").as_bytes());
        tag.update(body);
        let mut headers = HeaderMap::new();
        headers.insert("svix-id", "fixture".parse().unwrap());
        headers.insert("svix-timestamp", now.to_string().parse().unwrap());
        headers.insert(
            "svix-signature",
            format!("v1,{}", STANDARD.encode(tag.finalize().into_bytes()))
                .parse()
                .unwrap(),
        );
        assert_eq!(relay.clerk_webhook(&headers, body), StatusCode::OK);
        assert_eq!(relay.clerk_webhook(&headers, body), StatusCode::OK);
        drop(relay);
        let relay = RelayService::open(dir.path(), address).unwrap();
        {
            let db = relay.db.lock().unwrap();
            for (owner, id) in [
                ("departed", "exclusive"),
                ("departed", "shared"),
                ("remaining", "shared"),
            ] {
                db.execute("INSERT INTO link(owner,id,pubkey,label,endpoint,created) VALUES(?,?,'key','','{}','')",params![owner,id]).unwrap();
                db.execute(
                    "INSERT OR IGNORE INTO credential VALUES(?,'key',?)",
                    params![id, id],
                )
                .unwrap();
                db.execute(
                    "INSERT OR IGNORE INTO activity VALUES(?,'key','thread','{}')",
                    [id],
                )
                .unwrap();
            }
        }
        relay.process_deletions(now + 1).unwrap();
        {
            let db = relay.db.lock().unwrap();
            for table in ["credential", "activity", "link"] {
                let ids: Vec<String> = db
                    .prepare(&format!("SELECT id FROM {table}"))
                    .unwrap()
                    .query_map([], |r| r.get(0))
                    .unwrap()
                    .collect::<rusqlite::Result<_>>()
                    .unwrap();
                assert_eq!(ids, ["shared"], "{table}");
            }
            for owner in ["departed", "remaining"] {
                db.execute("INSERT INTO device VALUES(?,'phone','{}','')", [owner])
                    .unwrap();
            }
        }
        relay.process_deletions(now + 2101).unwrap();
        let db = relay.db.lock().unwrap();
        assert_eq!(
            db.query_row("SELECT count(*) FROM device", [], |r| r.get::<_, i64>(0))
                .unwrap(),
            1
        );
        assert_eq!(
            db.query_row("SELECT owner FROM device", [], |r| r.get::<_, String>(0))
                .unwrap(),
            "remaining"
        );
        assert_eq!(
            db.query_row(
                "SELECT count(*) FROM account_deletion WHERE completed IS NOT NULL",
                [],
                |r| r.get::<_, i64>(0)
            )
            .unwrap(),
            1
        );
    }
}
