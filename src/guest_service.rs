use crate::{
    auth_service::{iso, now, storage::Storage},
    AppState,
};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD as B64, Engine};
use hmac::{Hmac, Mac};
use rand::{rngs::OsRng, RngCore};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::Sha256;
use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::{Arc, RwLock},
};
use tokio::sync::watch;
use uuid::Uuid;

#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct Share {
    pub id: String,
    pub thread_id: String,
    pub host: String,
    created: i64,
    pub expires: i64,
    pub name: Option<String>,
    pub messages: Vec<Value>,
}
impl Share {
    fn public(&self) -> Value {
        json!({"id":self.id,"host":self.host,"createdAt":iso(self.created),"expiresAt":iso(self.expires),"guestName":self.name,"messages":self.messages})
    }
}
#[derive(Clone, Serialize, Deserialize, Default)]
struct Records {
    shares: HashMap<String, Share>,
    secret: Option<[u8; 32]>,
}
#[derive(Clone)]
pub struct GuestService {
    records: Arc<Storage<Records>>,
    host: Arc<RwLock<Option<String>>>,
    changed: watch::Sender<()>,
    directory: Option<PathBuf>,
}
impl Default for GuestService {
    fn default() -> Self {
        Self::new()
    }
}
impl GuestService {
    pub fn new() -> Self {
        Self {
            records: Arc::default(),
            directory: None,
            host: Arc::default(),
            changed: watch::channel(()).0,
        }
    }
    pub fn open(dir: &Path) -> anyhow::Result<Self> {
        Ok(Self {
            directory: Some(dir.join("guest-attachments")),
            records: Arc::new(Storage::open(&dir.join("guest-shares.json"))?),
            ..Self::new()
        })
    }
    pub fn set_tunnel_hostname(&self, host: Option<String>) {
        *self.host.write().unwrap() = host;
        self.changed.send_replace(());
    }
    pub(crate) fn changes(&self) -> watch::Receiver<()> {
        self.changed.subscribe()
    }
    fn sign(key: &[u8; 32], input: &str) -> Vec<u8> {
        let mut mac = Hmac::<Sha256>::new_from_slice(key).unwrap();
        mac.update(input.as_bytes());
        mac.finalize().into_bytes().to_vec()
    }
    fn create_share(&self, id: &str, host: String) -> Result<Value, &'static str> {
        let (result, removed) = self.records.update(|records| {
            let key = records.secret.get_or_insert_with(|| {
                let mut key = [0; 32];
                OsRng.fill_bytes(&mut key);
                key
            });
            let mut bytes = [0; 16];
            OsRng.fill_bytes(&mut bytes);
            let share_id = B64.encode(bytes);
            let mac = Self::sign(key, &format!("guest-invite:{share_id}"));
            let signature = B64.encode(&mac[..16]);
            let at = now();
            let share = Share {
                id: share_id.clone(),
                host: host.clone(),
                thread_id: id.to_owned(),
                created: at,
                expires: at + 86_400_000,
                name: None,
                messages: vec![],
            };
            let url = format!("https://{host}/guest#{share_id}.{signature}");
            let result = json!({"url":url,"share":share.public()});
            let mut removed = Vec::new();
            records.shares.retain(|_, s| {
                if s.thread_id != id && s.expires > at {
                    return true;
                }
                removed.extend(s.messages.iter().flat_map(file_ids));
                false
            });
            records.shares.insert(share_id, share);
            Ok((result, removed))
        })?;
        self.cleanup(removed);
        self.changed.send_replace(());
        Ok(result)
    }
    fn credential(&self, input: &str, invite: bool) -> Result<Share, &'static str> {
        let (payload, sig) = input.split_once('.').ok_or("invalid")?;
        let sig = B64.decode(sig).map_err(|_| "invalid")?;
        self.records.read(|r| {
            let key = r.secret.as_ref().ok_or("invalid")?;
            let id = if invite {
                let expected = Self::sign(key, &format!("guest-invite:{payload}"));
                if sig.len() != 16 || !equal(&expected[..16], &sig) {
                    return Err("invalid");
                }
                payload.to_owned()
            } else {
                let mut mac = Hmac::<Sha256>::new_from_slice(key).unwrap();
                mac.update(payload.as_bytes());
                mac.verify_slice(&sig).map_err(|_| "invalid")?;
                let claims: Value =
                    serde_json::from_slice(&B64.decode(payload).map_err(|_| "invalid")?)
                        .map_err(|_| "invalid")?;
                if claims["kind"] != "session" {
                    return Err("invalid");
                }
                claims["shareId"].as_str().ok_or("invalid")?.to_owned()
            };
            let share = r
                .shares
                .get(&id)
                .ok_or(if invite { "invalid" } else { "inactive" })?;
            if share.expires <= now()
                || self.host.read().unwrap().as_deref() != Some(&share.host)
                || (!invite && share.name.is_none())
            {
                return Err("inactive");
            }
            Ok(share.clone())
        })
    }
    pub(crate) fn redeem(&self, share: &Share, name: &str) -> Result<String, &'static str> {
        let token = self.records.update(|r| {
            let active = r
                .shares
                .get_mut(&share.id)
                .filter(|s| {
                    s.expires > now() && self.host.read().unwrap().as_ref() == Some(&s.host)
                })
                .ok_or("inactive")?;
            if active.name.is_some() {
                return Err("used");
            }
            active.name = Some(name.to_owned());
            let payload = B64.encode(
                serde_json::to_vec(
                    &json!({"kind":"session","threadId":active.thread_id,"shareId":active.id}),
                )
                .unwrap(),
            );
            Ok(format!(
                "{payload}.{}",
                B64.encode(Self::sign(r.secret.as_ref().ok_or("inactive")?, &payload))
            ))
        })?;
        self.changed.send_replace(());
        Ok(token)
    }
    pub(crate) fn submit(
        &self,
        share: &Share,
        text: &str,
        uploads: Vec<(Value, Vec<u8>)>,
    ) -> Result<(), &'static str> {
        use std::io::Write;
        if text.trim().is_empty() && uploads.is_empty() {
            return Err("empty");
        }
        let mut created = Vec::new();
        let result = (|| {
            let mut attachments = vec![];
            for (meta, bytes) in uploads {
                let dir = self.directory.as_ref().ok_or("rejected")?;
                std::fs::create_dir_all(dir).map_err(|_| "rejected")?;
                let id = meta["id"].as_str().ok_or("rejected")?;
                let file = dir.join(id);
                let mut tmp = tempfile::NamedTempFile::new_in(dir).map_err(|_| "rejected")?;
                tmp.write_all(&bytes)
                    .and_then(|()| tmp.as_file().sync_all())
                    .map_err(|_| "rejected")?;
                tmp.persist_noclobber(&file).map_err(|_| "rejected")?;
                created.push(file);
                attachments.push(meta);
            }
            self.records.update(|r|{
                let s=r.shares.get_mut(&share.id).filter(|s|s.expires>now() && s.name.is_some() && self.host.read().unwrap().as_ref()==Some(&s.host)).ok_or("inactive")?;
                let pending=s.messages.iter().filter(|m|m["status"]=="pending");
                if pending.clone().count()>=3{return Err("busy")}
                let size:u64=pending.flat_map(|m|m["attachments"].as_array().into_iter().flatten()).chain(attachments.iter()).filter_map(|a|a["sizeBytes"].as_u64()).sum();
                if size>20*1024*1024{return Err("files_busy")}
                let mut message=json!({"id":Uuid::new_v4().to_string(),"text":text.trim(),"status":"pending","submittedAt":iso(now())});
                if !attachments.is_empty(){message["attachments"]=json!(attachments)}
                s.messages.push(message);Ok(())
            })
        })();
        if result.is_err() {
            for file in created {
                let _ = std::fs::remove_file(file);
            }
        } else {
            self.changed.send_replace(());
        }
        result
    }
    pub(crate) async fn attachment(&self, share: &Share, id: &str) -> Option<(Value, Vec<u8>)> {
        if !id
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, b'-' | b'_'))
        {
            return None;
        }
        let meta = share
            .messages
            .iter()
            .filter(|m| m["status"] != "dismissed")
            .flat_map(|m| m["attachments"].as_array().into_iter().flatten())
            .find(|a| a["id"] == id)?;
        Some((
            meta.clone(),
            tokio::fs::read(self.directory.as_ref()?.join(id))
                .await
                .ok()?,
        ))
    }
    fn cleanup(&self, removed: Vec<String>) {
        if let Some(dir) = &self.directory {
            for id in removed {
                if !id.is_empty()
                    && id
                        .bytes()
                        .all(|c| c.is_ascii_alphanumeric() || matches!(c, b'-' | b'_'))
                {
                    let _ = std::fs::remove_file(dir.join(id));
                }
            }
        }
    }
    pub fn close_thread(&self, id: &str) -> Result<(), &'static str> {
        let removed = self.records.update(|r| {
            let ids = r
                .shares
                .values()
                .filter(|s| s.thread_id == id)
                .flat_map(|s| s.messages.iter())
                .flat_map(file_ids)
                .collect();
            r.shares.retain(|_, s| s.thread_id != id);
            Ok(ids)
        })?;
        self.cleanup(removed);
        self.changed.send_replace(());
        Ok(())
    }
    pub fn resolve_message(
        &self,
        thread: &str,
        id: &str,
        status: &str,
    ) -> Result<(), &'static str> {
        if !matches!(status, "sent" | "dismissed") {
            return Err("rejected");
        }
        let removed = self.records.update(|r| {
            let s = r
                .shares
                .values_mut()
                .find(|s| s.thread_id == thread)
                .ok_or("inactive")?;
            let m = s
                .messages
                .iter_mut()
                .find(|m| m["id"] == id && m["status"] == "pending")
                .ok_or("rejected")?;
            let mut removed = if status == "dismissed" {
                file_ids(m)
            } else {
                vec![]
            };
            m["status"] = json!(status);
            let mut resolved = 0;
            s.messages.reverse();
            s.messages.retain(|m| {
                if m["status"] == "pending" {
                    true
                } else {
                    resolved += 1;
                    if resolved > 20 {
                        removed.extend(file_ids(m));
                        false
                    } else {
                        true
                    }
                }
            });
            s.messages.reverse();
            Ok(removed)
        })?;
        self.cleanup(removed);
        self.changed.send_replace(());
        Ok(())
    }
    pub fn projection(&self, thread: &str) -> Option<Value> {
        self.records.read(|r| {
            r.shares
                .values()
                .find(|s| s.thread_id == thread)
                .map(Share::public)
        })
    }
}
fn file_ids(m: &Value) -> Vec<String> {
    m["attachments"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|a| a["id"].as_str().map(String::from))
        .collect()
}
fn equal(a: &[u8], b: &[u8]) -> bool {
    let key = [0; 32];
    let mut mac = Hmac::<Sha256>::new_from_slice(&key).unwrap();
    mac.update(a);
    let mut expected = Hmac::<Sha256>::new_from_slice(&key).unwrap();
    expected.update(b);
    mac.verify_slice(&expected.finalize().into_bytes()).is_ok()
}
async fn enabled(state: &AppState) -> bool {
    state
        .settings
        .internal()
        .is_ok_and(|v| v["guestSharingEnabled"] == true)
}
pub async fn create(state: &AppState, params: &Value) -> Result<Value, Value> {
    let error = |reason: &str| json!({"_tag":"GuestShareCreateError","reason":reason});
    if !enabled(state).await {
        return Err(error("disabled"));
    }
    let host = state
        .guest
        .host
        .read()
        .unwrap()
        .clone()
        .ok_or_else(|| error("connect_unavailable"))?;
    let id = params["threadId"]
        .as_str()
        .ok_or_else(|| error("thread_unavailable"))?;
    let store = state.store.read().await;
    if store.threads.get(id).is_none_or(|t| t.archived) {
        return Err(error("thread_unavailable"));
    }
    state
        .guest
        .create_share(id, host)
        .map_err(|_| error("read_failed"))
}
pub(crate) async fn access(
    state: &AppState,
    token: &str,
    invite: bool,
) -> Result<Share, &'static str> {
    let s = state.guest.credential(token, invite)?;
    if !enabled(state).await
        || state
            .store
            .read()
            .await
            .threads
            .get(&s.thread_id)
            .is_none_or(|t| t.archived)
    {
        return Err("inactive");
    }
    Ok(s)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn replacement_redeem_pending_limit_and_persistence() {
        let dir = tempfile::tempdir().unwrap();
        let thread = "opaque:guest";
        let svc = GuestService::open(dir.path()).unwrap();
        let host = "linked.example.test".to_owned();
        svc.set_tunnel_hostname(Some(host.clone()));
        let new = || {
            svc.create_share(thread, host.clone()).unwrap()["url"]
                .as_str()
                .unwrap()
                .split_once('#')
                .unwrap()
                .1
                .to_owned()
        };
        let invite = new();
        let share = svc.credential(&invite, true).unwrap();
        let session = svc.redeem(&share, "Joe").unwrap();
        assert_eq!(svc.redeem(&share, "Mallory").unwrap_err(), "used");
        assert!(svc.credential(&invite, false).is_err());
        assert!(svc.credential(&session, false).is_ok());
        for _ in 0..3 {
            svc.submit(&share, "hi", vec![]).unwrap()
        }
        assert_eq!(svc.submit(&share, "hi", vec![]), Err("busy"));
        let share = svc.credential(&session, false).unwrap();
        svc.resolve_message(
            thread,
            share.messages[0]["id"].as_str().unwrap(),
            "dismissed",
        )
        .unwrap();
        svc.submit(&share, "fourth", vec![]).unwrap();
        let restored = GuestService::open(dir.path()).unwrap();
        assert!(restored.credential(&session, false).is_err());
        restored.set_tunnel_hostname(Some(host.clone()));
        assert_eq!(
            restored.credential(&session, false).unwrap().messages.len(),
            4
        );
        new();
        assert!(svc.credential(&session, false).is_err());
        svc.close_thread(thread).unwrap();
        assert!(svc.projection(thread).is_none());
    }
}
