use super::{
    hash, timestamp,
    tokens::{field, segment, verified, CHALLENGE},
    RelayService,
};
use jsonwebtoken::DecodingKey;
use rusqlite::{params, OptionalExtension};
use serde_json::{json, Value};
use time::OffsetDateTime;
use uuid::Uuid;

pub(crate) const FLAGS: [&str; 3] = [
    "notificationsEnabled",
    "liveActivitiesEnabled",
    "managedTunnelsEnabled",
];
pub(crate) enum LinkError {
    Expired,
    Invalid(&'static str),
    NoTunnel,
    Internal,
}
impl From<rusqlite::Error> for LinkError {
    fn from(_: rusqlite::Error) -> Self {
        Self::Internal
    }
}

impl RelayService {
    pub(crate) fn link(&self, owner: &str, body: &Value) -> Result<Value, LinkError> {
        let invalid = || LinkError::Invalid("invalid_signature_or_scope");
        let token = field(body, "proof").ok_or_else(invalid)?;
        let raw = segment(token, 1).ok_or_else(invalid)?;
        let id = field(&raw, "environmentId").ok_or_else(invalid)?;
        let pem = field(&raw, "environmentPublicKey").ok_or_else(invalid)?;
        let exp = raw["exp"].as_i64().ok_or_else(invalid)?;
        if exp <= OffsetDateTime::now_utc().unix_timestamp() {
            return Err(LinkError::Expired);
        }
        let key = DecodingKey::from_ed_pem(pem.as_bytes()).map_err(|_| invalid())?;
        let v = verified(
            token,
            &key,
            "t3-env-link+jwt",
            &format!("t3-env:{id}"),
            &self.issuer,
        )
        .ok_or_else(invalid)?;
        let scopes = v["scopes"].as_array().ok_or_else(invalid)?;
        if field(&v, "sub") != Some(id)
            || body[FLAGS[2]] == true && !scopes.contains(&json!("managed_tunnels"))
            || (body[FLAGS[0]] == true || body[FLAGS[1]] == true)
                && !scopes.contains(&json!("agent_activity_notifications"))
        {
            return Err(invalid());
        }
        if v["descriptor"]["environmentId"] != id {
            return Err(LinkError::Invalid("descriptor_mismatch"));
        }
        let label = field(&v["descriptor"], "label").ok_or_else(invalid)?;
        if field(&v["descriptor"], "serverVersion").is_none()
            || !v["descriptor"]["capabilities"].is_object()
            || field(&v["descriptor"]["platform"], "os").is_none()
            || field(&v["descriptor"]["platform"], "arch").is_none()
        {
            return Err(invalid());
        }
        let endpoint = &v["endpoint"];
        if !matches!(
            field(endpoint, "providerKind"),
            Some("manual" | "cloudflare_tunnel" | "t3_relay")
        ) {
            return Err(invalid());
        }
        let endpoint = json!({"httpBaseUrl":field(endpoint,"httpBaseUrl").ok_or_else(invalid)?,"wsBaseUrl":field(endpoint,"wsBaseUrl").ok_or_else(invalid)?,"providerKind":endpoint["providerKind"]});
        let host = field(&v["origin"], "localHttpHost").ok_or_else(invalid)?;
        if !v["origin"]["localHttpPort"]
            .as_u64()
            .is_some_and(|p| (1..=65535).contains(&p))
        {
            return Err(invalid());
        }
        let challenge_error = || LinkError::Invalid("challenge_invalid");
        let challenge = verified(
            field(&v, "challenge").ok_or_else(challenge_error)?,
            self.keys.verify.as_ref().ok_or(LinkError::Internal)?,
            CHALLENGE,
            &self.issuer,
            &self.issuer,
        )
        .ok_or_else(challenge_error)?;
        if challenge["kind"] != "environment_link_challenge"
            || challenge["sub"] != owner
            || FLAGS
                .iter()
                .any(|f| body[*f] == true && challenge[*f] != true)
        {
            return Err(challenge_error());
        }
        if !self.consume(pem, field(&v, "jti").ok_or_else(invalid)?, exp)? {
            return Err(LinkError::Invalid("replayed_nonce"));
        }
        if !self.consume(
            "relay-environment-link-challenge",
            field(&challenge, "jti").ok_or_else(challenge_error)?,
            exp.max(challenge["exp"].as_i64().unwrap_or(exp)),
        )? {
            return Err(challenge_error());
        }
        if body[FLAGS[2]] == true {
            if !matches!(host, "localhost" | "127.0.0.1" | "::1" | "[::1]") {
                return Err(LinkError::Invalid("origin_not_allowed"));
            }
            return Err(LinkError::NoTunnel);
        }
        let public = self.keys.public_pem.as_ref().ok_or(LinkError::Internal)?;
        let generate = |n| {
            (0..n)
                .map(|_| Uuid::new_v4().simple().to_string())
                .collect::<String>()
        };
        let credential = format!("t3env_{}_{}", generate(2), generate(3));
        let mut db = self.db.lock().unwrap();
        let tx = db.transaction()?;
        let hold: bool = tx.query_row(
            "SELECT COALESCE(MAX(hold),0) FROM link WHERE id=? AND pubkey=?",
            params![id, pem],
            |r| r.get(0),
        )?;
        tx.execute(
            "DELETE FROM link WHERE id=? AND pubkey=? AND owner!=?",
            params![id, pem, owner],
        )?;
        tx.execute("INSERT INTO link(owner,id,pubkey,label,endpoint,created,hold) VALUES(?,?,?,?,?,?,?) ON CONFLICT(owner,id) DO UPDATE SET pubkey=excluded.pubkey,label=excluded.label,endpoint=excluded.endpoint,hold=excluded.hold",params![owner,id,pem,label,endpoint.to_string(),timestamp(OffsetDateTime::now_utc()),hold])?;
        tx.execute("DELETE FROM credential WHERE NOT EXISTS(SELECT 1 FROM link WHERE link.id=credential.id AND link.pubkey=credential.pubkey)",[])?;
        tx.execute("DELETE FROM consent WHERE NOT EXISTS(SELECT 1 FROM link WHERE consent.id=link.id AND consent.owner=link.owner)",[])?;
        tx.execute(
            "INSERT OR REPLACE INTO consent VALUES(?,?,?,?)",
            params![owner, id, body[FLAGS[1]] == true, body[FLAGS[0]] == true],
        )?;
        tx.execute(
            "INSERT OR REPLACE INTO credential VALUES(?,?,?)",
            params![id, pem, hash(&credential)],
        )?;
        tx.commit()?;
        Ok(
            json!({"ok":true,"cloudUserId":owner,"environmentId":id,"endpoint":endpoint,"endpointRuntime":null,"relayIssuer":self.issuer,"environmentCredential":credential,"cloudMintPublicKey":public}),
        )
    }
    pub(crate) fn links(&self, owner: &str) -> rusqlite::Result<Value> {
        let db = self.db.lock().unwrap();
        let mut q =
            db.prepare("SELECT id,label,endpoint,created FROM link WHERE owner=? ORDER BY rowid")?;
        let rows=q.query_map([owner],|r|{
            let endpoint:String=r.get(2)?;
            Ok(json!({"environmentId":r.get::<_,String>(0)?,"label":r.get::<_,String>(1)?,"endpoint":serde_json::from_str::<Value>(&endpoint).expect("stored endpoint"),"linkedAt":r.get::<_,String>(3)?}))
        })?.collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(json!({"environments":rows}))
    }
    pub(crate) fn unlink(&self, owner: &str, id: &str) -> rusqlite::Result<bool> {
        let mut db = self.db.lock().unwrap();
        let tx = db.transaction()?;
        let n = tx.execute(
            "DELETE FROM link WHERE owner=? AND id=?",
            params![owner, id],
        )?;
        tx.execute("DELETE FROM credential WHERE NOT EXISTS(SELECT 1 FROM link WHERE link.id=credential.id AND link.pubkey=credential.pubkey)",[])?;
        tx.commit()?;
        Ok(n > 0)
    }
    pub(crate) fn environment(&self, token: &str, id: &str) -> rusqlite::Result<Option<String>> {
        self.db
            .lock()
            .unwrap()
            .query_row(
                "SELECT pubkey FROM credential WHERE hash=? AND id=?",
                params![hash(token), id],
                |r| r.get(0),
            )
            .optional()
    }
    pub(crate) fn set_hold(&self, id: &str, key: &str, value: bool) -> rusqlite::Result<()> {
        self.db.lock().unwrap().execute(
            "UPDATE link SET hold=? WHERE id=? AND pubkey=?",
            params![value, id, key],
        )?;
        Ok(())
    }
}
