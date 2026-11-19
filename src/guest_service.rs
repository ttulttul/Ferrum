//! Guest-share state transitions used by the `/api/guest` routes.

use std::{
    collections::HashMap,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::sync::RwLock;
use uuid::Uuid;

#[derive(Clone, Default)]
pub struct GuestService {
    shares: Arc<RwLock<HashMap<String, GuestShare>>>,
}
#[derive(Clone)]
struct GuestShare {
    token: String,
    expires_at: u64,
    active: bool,
    messages: Vec<serde_json::Value>,
}

fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

impl GuestService {
    pub fn new() -> Self {
        Self::default()
    }
    pub async fn create_share(&self, thread_id: &str, ttl_seconds: u64) -> serde_json::Value {
        let id = Uuid::new_v4().to_string();
        let token = Uuid::new_v4().to_string();
        self.shares.write().await.insert(
            id.clone(),
            GuestShare {
                token: token.clone(),
                expires_at: now() + ttl_seconds,
                active: true,
                messages: vec![],
            },
        );
        serde_json::json!({"shareId":id,"threadId":thread_id,"token":token,"expiresAt":now()+ttl_seconds})
    }
    pub async fn redeem(&self, share_id: &str, token: &str) -> Result<(), &'static str> {
        let shares = self.shares.read().await;
        let Some(share) = shares.get(share_id) else {
            return Err("invalid");
        };
        if share.active && share.expires_at > now() && share.token == token {
            Ok(())
        } else {
            Err("inactive")
        }
    }
    pub async fn submit(
        &self,
        share_id: &str,
        token: &str,
        text: &str,
    ) -> Result<(), &'static str> {
        self.redeem(share_id, token).await?;
        if text.len() > 20_000 {
            return Err("too_large");
        }
        let mut shares = self.shares.write().await;
        shares
            .get_mut(share_id)
            .unwrap()
            .messages
            .push(serde_json::json!({"text":text,"createdAt":now()}));
        Ok(())
    }
    pub async fn close(&self, share_id: &str) -> bool {
        self.shares
            .write()
            .await
            .get_mut(share_id)
            .map(|share| {
                share.active = false;
                true
            })
            .unwrap_or(false)
    }
    pub async fn messages(
        &self,
        share_id: &str,
        token: &str,
    ) -> Result<Vec<serde_json::Value>, &'static str> {
        self.redeem(share_id, token).await?;
        Ok(self
            .shares
            .read()
            .await
            .get(share_id)
            .unwrap()
            .messages
            .clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn guest_share_requires_redeem_token() {
        let service = GuestService::new();
        let share = service.create_share("thread-1", 60).await;
        let id = share["shareId"].as_str().unwrap();
        let token = share["token"].as_str().unwrap();
        assert_eq!(service.redeem(id, "wrong").await, Err("inactive"));
        service.submit(id, token, "hello").await.unwrap();
        assert_eq!(service.messages(id, token).await.unwrap().len(), 1);
        assert!(service.close(id).await);
        assert_eq!(service.messages(id, token).await, Err("inactive"));
    }
}
