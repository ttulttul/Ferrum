//! Credentials for the T3 MCP server a thread's provider CLI is given
//! (`McpSessionRegistry` in the reference).
//!
//! Each provider session gets a bearer token that acts as its thread on
//! `/mcp`: the tools it calls run as that thread, within the thread's own
//! modes. Tokens live in memory only. One idle for a day stops
//! authenticating until a turn of its thread touches it again, and a thread's
//! token is reused across its turns while it is valid for the same provider.
use std::collections::HashMap;
use std::sync::Mutex;

/// A day without a request or a turn ends a token (`DEFAULT_LIVENESS_WINDOW_MS`).
const LIVENESS_WINDOW_MS: i64 = 24 * 60 * 60 * 1000;

/// Who a thread credential acts as.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ThreadScope {
    pub thread_id: String,
    /// Scopes the session's retry keys, as the reference's `requestNamespace`.
    pub provider_session_id: String,
    pub provider_instance_id: String,
}

struct Record {
    scope: ThreadScope,
    token: String,
    last_alive_ms: i64,
    /// Set when a request finds the token idle; only a turn's touch clears it.
    expired: bool,
}

/// The thread credentials this server has issued, by token hash.
#[derive(Default)]
pub struct ThreadCredentials {
    records: Mutex<HashMap<String, Record>>,
}

fn hash(token: &str) -> String {
    use sha2::{Digest, Sha256};
    hex::encode(Sha256::digest(token.as_bytes()))
}

impl ThreadCredentials {
    /// The bearer token for `thread_id`'s provider session on
    /// `provider_instance_id`: the one it already holds while that is live,
    /// or a new one that replaces any other the thread held.
    pub fn for_thread(&self, thread_id: &str, provider_instance_id: &str, now_ms: i64) -> String {
        let mut records = self.records.lock().unwrap();
        if let Some(record) = records.values_mut().find(|r| {
            r.scope.thread_id == thread_id
                && r.scope.provider_instance_id == provider_instance_id
                && !r.expired
                && now_ms - r.last_alive_ms <= LIVENESS_WINDOW_MS
        }) {
            record.last_alive_ms = now_ms;
            return record.token.clone();
        }
        records.retain(|_, r| r.scope.thread_id != thread_id);
        use base64::Engine;
        let token = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .encode(crate::platform::random_bytes::<32>());
        records.insert(
            hash(&token),
            Record {
                scope: ThreadScope {
                    thread_id: thread_id.to_owned(),
                    provider_session_id: crate::platform::uuid_v4().to_string(),
                    provider_instance_id: provider_instance_id.to_owned(),
                },
                token: token.clone(),
                last_alive_ms: now_ms,
                expired: false,
            },
        );
        token
    }

    /// The thread `token` acts as, or `None` when it is unknown, revoked or idle too long.
    pub fn resolve(&self, token: &str, now_ms: i64) -> Option<ThreadScope> {
        if token.is_empty() {
            return None;
        }
        let mut records = self.records.lock().unwrap();
        let record = records.get_mut(&hash(token))?;
        if record.expired {
            return None;
        }
        if now_ms - record.last_alive_ms > LIVENESS_WINDOW_MS {
            record.expired = true;
            return None;
        }
        record.last_alive_ms = now_ms;
        Some(record.scope.clone())
    }

    /// A turn of `thread_id` started: its tokens are live again.
    pub fn touch(&self, thread_id: &str, now_ms: i64) {
        for record in self.records.lock().unwrap().values_mut() {
            if record.scope.thread_id == thread_id {
                record.last_alive_ms = now_ms;
                record.expired = false;
            }
        }
    }

    /// Ends every token of `thread_id`, as when the thread is deleted.
    pub fn revoke_thread(&self, thread_id: &str) {
        self.records
            .lock()
            .unwrap()
            .retain(|_, r| r.scope.thread_id != thread_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_thread_keeps_its_token_until_it_idles_out() {
        let credentials = ThreadCredentials::default();
        let token = credentials.for_thread("thread-1", "codex", 0);
        assert_eq!(credentials.for_thread("thread-1", "codex", 1000), token);
        assert_eq!(
            credentials.resolve(&token, 2000).unwrap().thread_id,
            "thread-1"
        );
        // Idle past the window: refused, until a turn touches it.
        assert!(credentials
            .resolve(&token, 2000 + LIVENESS_WINDOW_MS + 1)
            .is_none());
        assert!(credentials.resolve(&token, 2000).is_none());
        credentials.touch("thread-1", 5000);
        assert!(credentials.resolve(&token, 5001).is_some());
    }

    #[test]
    fn another_provider_or_a_revocation_replaces_the_token() {
        let credentials = ThreadCredentials::default();
        let codex = credentials.for_thread("thread-1", "codex", 0);
        let claude = credentials.for_thread("thread-1", "claudeAgent", 0);
        assert_ne!(codex, claude);
        assert!(credentials.resolve(&codex, 1).is_none());
        credentials.revoke_thread("thread-1");
        assert!(credentials.resolve(&claude, 1).is_none());
        assert!(credentials.resolve("", 1).is_none());
    }
}
