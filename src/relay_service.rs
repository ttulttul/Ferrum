//! Persistent relay state. Environment identities, cloud users and proof keys occupy separate namespaces.
use rusqlite::{params, Connection};
use serde_json::{json, Value};
use std::{
    env,
    net::SocketAddr,
    path::Path,
    sync::{Arc, Mutex},
};
use time::OffsetDateTime;

pub(crate) mod activity;
pub(crate) mod links;
pub(crate) mod tokens;

#[derive(Clone)]
pub struct RelayService {
    db: Arc<Mutex<Connection>>,
    pub(crate) keys: Arc<tokens::Keys>,
    pub(crate) issuer: String,
}

pub(crate) use crate::dpop_proof::hash;
pub(crate) fn timestamp(dt: OffsetDateTime) -> String {
    dt.format(time::macros::format_description!(
        "[year]-[month]-[day]T[hour]:[minute]:[second].[subsecond digits:3]Z"
    ))
    .expect("UTC timestamp")
}
impl RelayService {
    pub fn open(dir: &Path, listen: SocketAddr) -> anyhow::Result<Self> {
        let dir = dir.join("relay");
        std::fs::create_dir_all(&dir)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o700))?;
        }
        let db = Connection::open(dir.join("state.sqlite3"))?;
        db.execute_batch("CREATE TABLE IF NOT EXISTS proof(key TEXT, nonce TEXT, expires INTEGER NOT NULL, PRIMARY KEY(key,nonce));
        CREATE TABLE IF NOT EXISTS device(owner TEXT,id TEXT,data TEXT NOT NULL,updated TEXT NOT NULL, PRIMARY KEY(owner,id));
        CREATE TABLE IF NOT EXISTS activity_registration(owner TEXT,id TEXT,token TEXT NOT NULL, PRIMARY KEY(owner,id));
        CREATE TABLE IF NOT EXISTS activity(id TEXT,pubkey TEXT,thread TEXT,state TEXT NOT NULL,PRIMARY KEY(id,pubkey,thread));
        CREATE TABLE IF NOT EXISTS consent(owner TEXT,id TEXT,live INTEGER NOT NULL,notify INTEGER NOT NULL,PRIMARY KEY(owner,id));
        CREATE TABLE IF NOT EXISTS link(owner TEXT,id TEXT,pubkey TEXT NOT NULL,label TEXT NOT NULL,endpoint TEXT NOT NULL,created TEXT NOT NULL,hold INTEGER NOT NULL DEFAULT 0, PRIMARY KEY(owner,id));
        CREATE TABLE IF NOT EXISTS credential(id TEXT, pubkey TEXT, hash TEXT NOT NULL UNIQUE,PRIMARY KEY(id,pubkey));")?;
        let issuer = env::var("RELAY_ISSUER")
            .unwrap_or_else(|_| format!("http://{listen}"))
            .trim()
            .trim_end_matches('/')
            .to_owned();
        let url = url::Url::parse(&issuer)?;
        anyhow::ensure!(
            matches!(url.scheme(), "https" | "http")
                && url.host().is_some()
                && url.query().is_none()
                && url.fragment().is_none(),
            "invalid RELAY_ISSUER"
        );
        Ok(Self {
            db: Arc::new(Mutex::new(db)),
            keys: Arc::new(tokens::Keys::from_env()?),
            issuer,
        })
    }

    pub(crate) fn health(&self) -> rusqlite::Result<Value> {
        self.db
            .lock()
            .unwrap()
            .query_row("SELECT 1", [], |_| Ok(json!({"ok":true,"service":"relay"})))
    }
    pub(crate) fn consume(&self, key: &str, jti: &str, expiry: i64) -> rusqlite::Result<bool> {
        let mut db = self.db.lock().unwrap();
        let tx = db.transaction()?;
        tx.execute(
            "DELETE FROM proof WHERE expires<=?",
            [OffsetDateTime::now_utc().unix_timestamp()],
        )?;
        let inserted = tx.execute(
            "INSERT OR IGNORE INTO proof VALUES(?,?,?)",
            params![key, jti, expiry],
        )?;
        tx.commit()?;
        Ok(inserted == 1)
    }
    pub(crate) fn register_device(&self, owner: &str, mut v: Value) -> rusqlite::Result<()> {
        let id = v["deviceId"].as_str().expect("validated").to_owned();
        let mut db = self.db.lock().unwrap();
        let tx = db.transaction()?;
        let updated = timestamp(OffsetDateTime::now_utc());
        for field in ["pushToken", "pushToStartToken"] {
            if let Some(token) = v[field].as_str() {
                let path = format!("$.{field}");
                tx.execute("UPDATE device SET data=json_remove(data,?1),updated=?2 WHERE json_extract(data,?1)=?3",params![path,updated,token])?;
            }
        }
        use rusqlite::OptionalExtension;
        let old: Option<String> = tx
            .query_row(
                "SELECT data FROM device WHERE owner=? AND id=?",
                params![owner, id],
                |r| r.get(0),
            )
            .optional()?;
        if let Some(old) = old {
            let old: Value = serde_json::from_str(&old).expect("stored registration");
            for field in [
                "pushToken",
                "pushToStartToken",
                "bundleId",
                "apsEnvironment",
            ] {
                if v[field].is_null() && !old[field].is_null() {
                    v[field] = old[field].clone();
                }
            }
        }
        tx.execute("INSERT INTO device VALUES(?,?,?,?) ON CONFLICT(owner,id) DO UPDATE SET data=excluded.data,updated=excluded.updated",params![owner,id,v.to_string(),updated])?;
        tx.commit()
    }
    pub(crate) fn list_devices(&self, owner: &str, v1: bool) -> rusqlite::Result<Value> {
        let db = self.db.lock().unwrap();
        let mut q = db.prepare("SELECT data,updated FROM device WHERE owner=? ORDER BY rowid")?;
        let rows = q.query_map([owner], |r| {
            Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?))
        })?;
        let mut devices = Vec::new();
        for row in rows {
            let (raw, updated) = row?;
            let v: Value = serde_json::from_str(&raw).expect("stored registration");
            if v1 && v["platform"] != "ios" {
                continue;
            }
            let p = &v["preferences"];
            devices.push(json!({"deviceId":v["deviceId"],"label":v["label"],"platform":v["platform"],"iosMajorVersion":v["iosMajorVersion"],"androidApiLevel":v["androidApiLevel"],"appVersion":v["appVersion"],
                "notifications":{"enabled":p["notificationsEnabled"]==true && !v["pushToken"].is_null(),"notifyOnApproval":p["notifyOnApproval"],"notifyOnInput":p["notifyOnInput"],"notifyOnCompletion":p["notifyOnCompletion"],"notifyOnFailure":p["notifyOnFailure"]},
                "liveActivities":{"enabled":p["liveActivitiesEnabled"]},"updatedAt":updated}));
        }
        Ok(json!({"devices":devices}))
    }
    pub(crate) fn remove_device(&self, owner: &str, id: &str) -> rusqlite::Result<()> {
        let mut db = self.db.lock().unwrap();
        let tx = db.transaction()?;
        tx.execute(
            "DELETE FROM activity_registration WHERE owner=? AND id=?",
            params![owner, id],
        )?;
        tx.execute(
            "DELETE FROM device WHERE owner=? AND id=?",
            params![owner, id],
        )?;
        tx.commit()
    }
    pub(crate) fn register_activity(
        &self,
        owner: &str,
        id: &str,
        token: &str,
    ) -> rusqlite::Result<()> {
        self.db.lock().unwrap().execute(
            "INSERT OR REPLACE INTO activity_registration VALUES(?,?,?)",
            params![owner, id, token],
        )?;
        Ok(())
    }
}
