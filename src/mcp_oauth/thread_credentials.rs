//! Credentials for the T3 MCP server a thread's provider CLI is given
//! (`McpSessionRegistry` in the reference).
//!
//! Each provider session gets a bearer token that acts as its thread on
//! `/mcp`: the tools it calls run as that thread, within the thread's own
//! modes. A credential idle for a day stops authenticating until a turn
//! touches it again. A thread reuses its valid token for the same provider.
use crate::persistence::{DocumentStore, WriteMode};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use std::{
    collections::HashMap,
    io,
    path::PathBuf,
    sync::{Arc, Mutex},
};

/// A day without a request or a turn ends a token (`DEFAULT_LIVENESS_WINDOW_MS`).
const LIVENESS_WINDOW_MS: i64 = 24 * 60 * 60 * 1000;

/// Who a thread credential acts as.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ThreadScope {
    pub thread_id: String,
    /// Scopes the session's retry keys, as the reference's `requestNamespace`.
    pub provider_session_id: String,
    pub provider_instance_id: String,
}

#[derive(Clone, Serialize, Deserialize)]
struct Record {
    scope: ThreadScope,
    last_alive_ms: i64,
    #[serde(default)]
    browser_access: bool,
    // Tab assignments only live as long as the broker.
    #[serde(skip)]
    preview_sequence: u64,
    #[serde(skip)]
    preview_tab: Option<(u64, Option<String>)>,
    /// Set when a request finds the token idle; only a turn's touch clears it.
    expired: bool,
}

/// One record per thread. Only the control plane needs durable records: its
/// provider processes can outlive it in agentd.
struct Backing {
    documents: Arc<dyn DocumentStore>,
    dir: PathBuf,
}

impl Backing {
    fn path(&self, thread_id: &str) -> PathBuf {
        self.dir.join(format!("{}.json", super::hashed(thread_id)))
    }
}

pub struct ThreadCredentials {
    records: Mutex<HashMap<String, Record>>,
    key: [u8; 32],
    backing: Option<Backing>,
}

impl Default for ThreadCredentials {
    fn default() -> Self {
        Self {
            records: Default::default(),
            key: crate::platform::random_bytes(),
            backing: None,
        }
    }
}

impl ThreadCredentials {
    pub fn open(
        documents: Arc<dyn DocumentStore>,
        dir: PathBuf,
        key: [u8; 32],
    ) -> io::Result<Self> {
        let mut records = HashMap::new();
        documents.create_private_dir_all(&dir)?;
        for path in documents.list(&dir)? {
            let record: Record = serde_json::from_slice(&documents.read(&path)?)?;
            let token = Self::token(&key, &record.scope);
            records.insert(super::hashed(&token), record);
        }
        Ok(Self {
            records: Mutex::new(records),
            key,
            backing: Some(Backing { documents, dir }),
        })
    }

    // The random session id changes on rotation. Deriving its 32-byte bearer
    // with the environment key lets a woken plane reuse it without storing it.
    fn token(key: &[u8; 32], scope: &ThreadScope) -> String {
        let mut mac = Hmac::<Sha256>::new_from_slice(key).expect("32-byte HMAC key");
        mac.update(b"mcp-thread-session.");
        mac.update(scope.provider_session_id.as_bytes());
        URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes())
    }

    fn save(&self, record: &Record) -> io::Result<()> {
        if let Some(backing) = &self.backing {
            backing.documents.write(
                &backing.path(&record.scope.thread_id),
                &serde_json::to_vec(record)?,
                WriteMode::Private,
            )?;
        }
        Ok(())
    }

    fn mark(&self, record: &mut Record, now_ms: i64, expired: bool) -> io::Result<()> {
        let updated = Record {
            last_alive_ms: now_ms,
            expired,
            ..record.clone()
        };
        self.save(&updated)?;
        *record = updated;
        Ok(())
    }

    /// The bearer token for `thread_id`'s provider session on
    /// `provider_instance_id`: the one it already holds while that is live,
    /// or a new one that replaces any other the thread held.
    pub fn for_thread(
        &self,
        thread_id: &str,
        provider_instance_id: &str,
        now_ms: i64,
    ) -> io::Result<String> {
        self.issue(thread_id, provider_instance_id, now_ms, false)
    }

    pub(super) fn issue(
        &self,
        thread_id: &str,
        provider_instance_id: &str,
        now_ms: i64,
        browser_access: bool,
    ) -> io::Result<String> {
        let mut records = self.records.lock().unwrap();
        if let Some(record) = records.values_mut().find(|r| {
            r.scope.thread_id == thread_id
                && r.scope.provider_instance_id == provider_instance_id
                && r.browser_access == browser_access
                && !r.expired
                && now_ms.saturating_sub(r.last_alive_ms) <= LIVENESS_WINDOW_MS
        }) {
            self.mark(record, now_ms, false)?;
            return Ok(Self::token(&self.key, &record.scope));
        }
        let record = Record {
            scope: ThreadScope {
                thread_id: thread_id.to_owned(),
                provider_session_id: crate::platform::uuid_v4().to_string(),
                provider_instance_id: provider_instance_id.to_owned(),
            },
            last_alive_ms: now_ms,
            expired: false,
            browser_access,
            preview_sequence: 0,
            preview_tab: None,
        };
        self.save(&record)?;
        let token = Self::token(&self.key, &record.scope);
        records.retain(|_, r| r.scope.thread_id != thread_id);
        records.insert(super::hashed(&token), record);
        Ok(token)
    }

    /// The thread `token` acts as, or `None` when it is unknown, revoked or idle too long.
    pub fn resolve(&self, token: &str, now_ms: i64) -> Option<ThreadScope> {
        if token.is_empty() {
            return None;
        }
        let mut records = self.records.lock().unwrap();
        let record = records.get_mut(&super::hashed(token))?;
        if record.expired {
            return None;
        }
        if now_ms.saturating_sub(record.last_alive_ms) > LIVENESS_WINDOW_MS {
            self.mark(record, now_ms, true).ok()?;
            return None;
        }
        self.mark(record, now_ms, false).ok()?;
        Some(record.scope.clone())
    }

    pub(super) fn preview_target(&self, scope: &ThreadScope) -> Option<(u64, Option<String>)> {
        let mut records = self.records.lock().unwrap();
        let record = records.get_mut(&super::hashed(&Self::token(&self.key, scope)))?;
        if record.expired || !record.browser_access {
            return None;
        }
        record.preview_sequence += 1;
        Some((
            record.preview_sequence,
            record.preview_tab.as_ref().and_then(|(_, tab)| tab.clone()),
        ))
    }

    pub(super) fn remember_preview_tab(
        &self,
        scope: &ThreadScope,
        sequence: u64,
        tab: Option<String>,
    ) {
        let mut records = self.records.lock().unwrap();
        if let Some(record) = records.get_mut(&super::hashed(&Self::token(&self.key, scope))) {
            if record
                .preview_tab
                .as_ref()
                .is_none_or(|(last, _)| sequence >= *last)
            {
                record.preview_tab = Some((sequence, tab));
            }
        }
    }

    /// A turn of `thread_id` started: its tokens are live again.
    pub fn touch(&self, thread_id: &str, now_ms: i64) -> io::Result<()> {
        for record in self.records.lock().unwrap().values_mut() {
            if record.scope.thread_id == thread_id {
                self.mark(record, now_ms, false)?;
            }
        }
        Ok(())
    }

    /// Ends every token of `thread_id`, as when the thread is deleted.
    pub fn revoke_thread(&self, thread_id: &str) -> io::Result<()> {
        let mut records = self.records.lock().unwrap();
        records.retain(|_, r| r.scope.thread_id != thread_id);
        if let Some(backing) = &self.backing {
            match backing.documents.remove(&backing.path(thread_id)) {
                Err(e) if e.kind() == io::ErrorKind::NotFound => (),
                result => result?,
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_thread_keeps_its_token_until_it_idles_out() {
        let credentials = ThreadCredentials::default();
        let token = credentials.for_thread("thread-1", "codex", 0).unwrap();
        assert_eq!(
            credentials.for_thread("thread-1", "codex", 1000).unwrap(),
            token
        );
        assert_eq!(
            credentials.resolve(&token, 2000).unwrap().thread_id,
            "thread-1"
        );
        // Idle past the window: refused, until a turn touches it.
        assert!(credentials
            .resolve(&token, 2000 + LIVENESS_WINDOW_MS + 1)
            .is_none());
        assert!(credentials.resolve(&token, 2000).is_none());
        credentials.touch("thread-1", 5000).unwrap();
        assert!(credentials.resolve(&token, 5001).is_some());
    }

    #[test]
    fn reopening_preserves_scope_expiry_rotation_and_revocation() {
        let docs = Arc::new(crate::persistence::MemoryStore::new());
        let dir = PathBuf::from("sessions");
        let key = crate::platform::random_bytes();
        let open = || ThreadCredentials::open(docs.clone(), dir.clone(), key).unwrap();

        let token = open().for_thread("t1", "work", 0).unwrap();
        let scope = open().resolve(&token, 1).unwrap();
        assert!(open().for_thread("t1", "work", 2).unwrap() == token);
        assert_eq!(open().resolve(&token, 3), Some(scope));
        for path in docs.list(&dir).unwrap() {
            assert!(!String::from_utf8(docs.read(&path).unwrap())
                .unwrap()
                .contains(&token));
        }
        assert!(open().resolve(&token, 4 + LIVENESS_WINDOW_MS).is_none());
        assert!(open().resolve(&token, 0).is_none());
        open().touch("t1", 10).unwrap();
        assert!(open().resolve(&token, 11).is_some());
        let other = open().for_thread("t1", "another-codex", 12).unwrap();
        assert!(open().resolve(&token, 13).is_none());
        open().revoke_thread("t1").unwrap();
        assert!(open().resolve(&other, 14).is_none());
    }

    #[test]
    fn preview_access_rotates_with_the_session_and_keeps_the_latest_successful_target() {
        let docs = Arc::new(crate::persistence::MemoryStore::new());
        let key = crate::platform::random_bytes();
        let open =
            || ThreadCredentials::open(docs.clone(), PathBuf::from("sessions"), key).unwrap();
        let credentials = open();
        let token = credentials.issue("thread", "codex", 0, true).unwrap();
        let scope = credentials.resolve(&token, 0).unwrap();
        let (first, _) = credentials.preview_target(&scope).unwrap();
        let (second, _) = credentials.preview_target(&scope).unwrap();
        credentials.remember_preview_tab(&scope, second, Some("tab-2".into()));
        credentials.remember_preview_tab(&scope, first, Some("tab-1".into()));
        assert_eq!(
            credentials.preview_target(&scope).unwrap().1.as_deref(),
            Some("tab-2")
        );
        let credentials = open();
        assert_eq!(credentials.preview_target(&scope).unwrap().1, None);
        let disabled = credentials.issue("thread", "codex", 1, false).unwrap();
        assert_ne!(token, disabled);
        assert!(credentials.preview_target(&scope).is_none());
        let current = credentials.resolve(&disabled, 1).unwrap();
        assert!(credentials.preview_target(&current).is_none());
        assert!(open().preview_target(&current).is_none());
    }

    #[test]
    fn another_provider_or_a_revocation_replaces_the_token() {
        let credentials = ThreadCredentials::default();
        let codex = credentials.for_thread("thread-1", "codex", 0).unwrap();
        let claude = credentials
            .for_thread("thread-1", "claudeAgent", 0)
            .unwrap();
        assert_ne!(codex, claude);
        assert!(credentials.resolve(&codex, 1).is_none());
        credentials.revoke_thread("thread-1").unwrap();
        assert!(credentials.resolve(&claude, 1).is_none());
        assert!(credentials.resolve("", 1).is_none());
    }
}
