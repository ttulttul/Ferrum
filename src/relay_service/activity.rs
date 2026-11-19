use super::{
    hash,
    tokens::{field, segment, verified},
    RelayService,
};
use jsonwebtoken::DecodingKey;
use rusqlite::params;
use serde_json::{json, Value};
use time::{format_description::well_known::Rfc3339, OffsetDateTime};

pub(crate) enum PublishError {
    Bad,
    Invalid,
    Expired,
    Storage,
    DeliveryUnavailable,
}

impl RelayService {
    pub(crate) fn publish(
        &self,
        id: &str,
        key: &str,
        thread: &str,
        request: &Value,
    ) -> Result<Value, PublishError> {
        let state = request.get("state").ok_or(PublishError::Bad)?;
        let proof = field(request, "proof").ok_or(PublishError::Bad)?;
        if !state.is_null() {
            for name in [
                "environmentId",
                "threadId",
                "projectTitle",
                "threadTitle",
                "phase",
                "headline",
                "modelTitle",
                "updatedAt",
                "deepLink",
            ] {
                field(state, name).ok_or(PublishError::Bad)?;
            }
            if status(state["phase"].as_str().unwrap()).is_none()
                || state.get("detail").is_some() && field(state, "detail").is_none()
            {
                return Err(PublishError::Bad);
            }
        }
        let decoded = segment(proof, 1).ok_or(PublishError::Invalid)?;
        let now = OffsetDateTime::now_utc().unix_timestamp();
        let exp = decoded["exp"].as_i64().ok_or(PublishError::Invalid)?;
        if exp <= now {
            return Err(PublishError::Expired);
        }
        let public = DecodingKey::from_ed_pem(key.as_bytes()).map_err(|_| PublishError::Invalid)?;
        let claims = verified(
            proof,
            &public,
            "t3-env-activity+jwt",
            &format!("t3-env:{id}"),
            &self.issuer,
        )
        .ok_or(PublishError::Invalid)?;
        if claims["environmentId"] != id
            || claims["sub"] != id
            || claims["threadId"] != thread
            || claims.get("state") != Some(state)
            || !state.is_null() && (state["environmentId"] != id || state["threadId"] != thread)
        {
            return Err(PublishError::Invalid);
        }
        let replay = format!(
            "env-publish:{}",
            hash(&json!({"environmentId":id,"environmentPublicKey":key}).to_string())
        );
        if !self
            .consume(
                &replay,
                field(&claims, "jti").ok_or(PublishError::Invalid)?,
                exp,
            )
            .map_err(|_| PublishError::Storage)?
        {
            return Err(PublishError::Invalid);
        }
        let db = self.db.lock().unwrap();
        if state.is_null() {
            db.execute(
                "DELETE FROM activity WHERE id=? AND pubkey=? AND thread=?",
                params![id, key, thread],
            )
        } else {
            db.execute(
                "INSERT OR REPLACE INTO activity VALUES(?,?,?,?)",
                params![id, key, thread, state.to_string()],
            )
        }
        .map_err(|_| PublishError::Storage)?;
        // A native push backend is required when the account has a delivery target.
        let has_targets:bool=db.query_row("SELECT EXISTS(SELECT 1 FROM link l JOIN consent c USING(owner,id) JOIN device d ON d.owner=l.owner WHERE l.id=? AND l.pubkey=? AND ((c.notify AND json_extract(d.data,'$.pushToken') IS NOT NULL) OR (c.live AND (json_extract(d.data,'$.pushToStartToken') IS NOT NULL OR EXISTS(SELECT 1 FROM activity_registration a WHERE a.owner=l.owner AND a.id=d.id)))))",params![id,key],|r|r.get(0)).map_err(|_|PublishError::Storage)?;
        if has_targets {
            return Err(PublishError::DeliveryUnavailable);
        }
        Ok(json!({"ok":true,"deliveries":[]}))
    }
    pub(crate) fn aggregate(&self, owner: &str) -> rusqlite::Result<Value> {
        let db = self.db.lock().unwrap();
        let mut q=db.prepare("SELECT state FROM activity a JOIN link l USING(id,pubkey) JOIN consent c ON c.owner=l.owner AND c.id=l.id WHERE l.owner=? AND c.live ORDER BY json_extract(state,'$.updatedAt') DESC")?;
        let rows = q.query_map([owner], |r| r.get::<_, String>(0))?;
        let mut items = Vec::new();
        let now = OffsetDateTime::now_utc().unix_timestamp();
        for row in rows {
            let v: Value = serde_json::from_str(&row?).expect("stored activity");
            let Some((rank, label, ttl)) = v["phase"].as_str().and_then(status) else {
                continue;
            };
            let date = v["updatedAt"].as_str().unwrap_or("");
            if !OffsetDateTime::parse(date, &Rfc3339)
                .is_ok_and(|dt| now - dt.unix_timestamp() <= ttl)
            {
                continue;
            }
            let deep = v["deepLink"].as_str().unwrap_or("").trim();
            let deep = if deep.starts_with('/') && !deep.starts_with("//") {
                truncate(deep, 512)
            } else {
                "/".into()
            };
            let row = json!({"environmentId":v["environmentId"],"threadId":v["threadId"],"projectTitle":truncate(v["projectTitle"].as_str().unwrap_or(""),120),
                "threadTitle":truncate(v["threadTitle"].as_str().unwrap_or(""),120),"modelTitle":truncate(v["modelTitle"].as_str().unwrap_or(""),120),
                "phase":v["phase"],"status":label,"updatedAt":date,"deepLink":deep});
            items.push((rank, row));
        }
        let Some((_, newest)) = items.first() else {
            return Ok(json!({"aggregate":null}));
        };
        let latest = newest["updatedAt"].clone();
        let count = items.iter().filter(|(rank, _)| *rank < 3).count();
        let subtitle = if count > 0 {
            "Agent work in progress"
        } else if newest["phase"] == "failed" {
            "Agent work failed"
        } else {
            "Agent work completed"
        };
        items.sort_by_key(|(rank, _)| *rank);
        Ok(
            json!({"aggregate":{"title":"T3 Code","subtitle":subtitle,"activeCount":count,"updatedAt":latest,"activities":items.into_iter().take(5).map(|(_,v)|v).collect::<Vec<_>>()}}),
        )
    }
}
fn truncate(s: &str, n: usize) -> String {
    let s = s.trim();
    if s.encode_utf16().count() <= n {
        return s.to_owned();
    }
    let units = s.encode_utf16().take(n - 3).collect::<Vec<_>>();
    format!("{}...", String::from_utf16_lossy(&units).trim_end())
}
fn status(phase: &str) -> Option<(u8, &'static str, i64)> {
    Some(match phase {
        "waiting_for_approval" => (0, "Approval", 86400),
        "waiting_for_input" => (0, "Input", 86400),
        "starting" => (1, "Connecting", 7200),
        "running" => (1, "Working", 7200),
        "stale" => (2, "Waiting", 86400),
        "completed" => (3, "Done", 900),
        "failed" => (3, "Failed", 900),
        _ => return None,
    })
}
