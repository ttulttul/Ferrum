//! Stateful authentication primitives for the environment HTTP contract.
//!
//! The service deliberately keeps credentials in memory. The caller owns
//! persistence and transport authentication, while this module provides the
//! contract-shaped state transitions and token lifecycle.

use std::{
    collections::HashMap,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::sync::RwLock;
use uuid::Uuid;

const SESSION_TTL: Duration = Duration::from_secs(30 * 24 * 60 * 60);
const TICKET_TTL: Duration = Duration::from_secs(60);

#[derive(Clone, Debug)]
pub struct AuthService {
    sessions: std::sync::Arc<RwLock<HashMap<String, Session>>>,
    tickets: std::sync::Arc<RwLock<HashMap<String, Ticket>>>,
}

#[derive(Clone, Debug)]
struct Session {
    scopes: Vec<String>,
    expires_at: u64,
}

#[derive(Clone, Debug)]
struct Ticket {
    session: String,
    use_kind: String,
    expires_at: u64,
}

fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn iso8601(seconds: u64) -> String {
    format!("{seconds}")
}

impl Default for AuthService {
    fn default() -> Self {
        Self::new()
    }
}

impl AuthService {
    pub fn new() -> Self {
        Self {
            sessions: std::sync::Arc::new(RwLock::new(HashMap::new())),
            tickets: std::sync::Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn session_state(&self, bearer: Option<&str>) -> serde_json::Value {
        let session = bearer.and_then(|token| self.sessions.try_read().ok()?.get(token).cloned());
        match session {
            Some(session) if session.expires_at > now() => serde_json::json!({
                "authenticated": true,
                "auth": self.auth_descriptor(),
                "scopes": session.scopes,
                "permissions": {},
                "sessionMethod": "browser-session",
                "expiresAt": iso8601(session.expires_at),
            }),
            _ => serde_json::json!({
                "authenticated": false,
                "auth": self.auth_descriptor(),
                "scopes": [],
                "permissions": {},
            }),
        }
    }

    pub async fn create_browser_session(
        &self,
        credential: &str,
    ) -> Result<serde_json::Value, &'static str> {
        if credential.trim().is_empty() {
            return Err("invalid_credential");
        }
        let token = Uuid::new_v4().to_string();
        let expires_at = now() + SESSION_TTL.as_secs();
        self.sessions.write().await.insert(
            token,
            Session {
                scopes: vec!["orchestration:read".into(), "orchestration:operate".into()],
                expires_at,
            },
        );
        Ok(
            serde_json::json!({"authenticated":true,"scopes":["orchestration:read","orchestration:operate"],"permissions":{},"sessionMethod":"browser-session","expiresAt":iso8601(expires_at)}),
        )
    }

    pub async fn create_websocket_ticket(
        &self,
        bearer: &str,
        use_kind: &str,
    ) -> Result<serde_json::Value, &'static str> {
        let valid = self
            .sessions
            .read()
            .await
            .get(bearer)
            .is_some_and(|session| session.expires_at > now());
        if !valid {
            return Err("missing_credential");
        }
        if !matches!(use_kind, "rpc" | "media") {
            return Err("invalid_use");
        }
        let ticket = Uuid::new_v4().to_string();
        let expires_at = now() + TICKET_TTL.as_secs();
        self.tickets.write().await.insert(
            ticket.clone(),
            Ticket {
                session: bearer.to_owned(),
                use_kind: use_kind.to_owned(),
                expires_at,
            },
        );
        Ok(serde_json::json!({"ticket":ticket,"expiresAt":iso8601(expires_at)}))
    }

    pub async fn consume_ticket(&self, ticket: &str, use_kind: &str) -> bool {
        let mut tickets = self.tickets.write().await;
        let Some(value) = tickets.remove(ticket) else {
            return false;
        };
        value.use_kind == use_kind
            && value.expires_at > now()
            && self.sessions.read().await.contains_key(&value.session)
    }

    pub async fn revoke_session(&self, bearer: &str) -> bool {
        self.sessions.write().await.remove(bearer).is_some()
    }

    fn auth_descriptor(&self) -> serde_json::Value {
        serde_json::json!({"policy":"optional","bootstrapMethods":["browser-session"],"sessionMethods":["browser-session"],"sessionCookieName":"t3_session"})
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn browser_session_and_ticket_lifecycle() {
        let service = AuthService::new();
        assert_eq!(
            service.create_browser_session("").await,
            Err("invalid_credential")
        );
        let result = service.create_browser_session("credential").await.unwrap();
        assert_eq!(result["authenticated"], true);
        let token = service
            .sessions
            .read()
            .await
            .keys()
            .next()
            .cloned()
            .unwrap();
        let ticket = service
            .create_websocket_ticket(&token, "rpc")
            .await
            .unwrap();
        let value = ticket["ticket"].as_str().unwrap();
        assert!(service.consume_ticket(value, "rpc").await);
        assert!(!service.consume_ticket(value, "rpc").await);
    }
}
