//! In-memory relay contract state. Network forwarding and DPoP verification
//! are injected by the host, while this service owns link/device lifecycle.

use std::{collections::HashMap, sync::Arc};
use tokio::sync::RwLock;

#[derive(Clone, Default)]
pub struct RelayService {
    state: Arc<RwLock<RelayState>>,
}
#[derive(Default)]
struct RelayState {
    environments: HashMap<String, serde_json::Value>,
    devices: HashMap<String, serde_json::Value>,
    preferences: HashMap<String, serde_json::Value>,
}

impl RelayService {
    pub fn new() -> Self {
        Self::default()
    }
    pub async fn health(&self) -> serde_json::Value {
        serde_json::json!({"ok":true,"service":"relay"})
    }
    pub async fn list_environments(&self) -> serde_json::Value {
        let state = self.state.read().await;
        serde_json::json!({"environments":state.environments.values().cloned().collect::<Vec<_>>()})
    }
    pub async fn list_devices(&self) -> serde_json::Value {
        let state = self.state.read().await;
        serde_json::json!({"devices":state.devices.values().cloned().collect::<Vec<_>>()})
    }
    pub async fn link_environment(&self, id: &str, label: Option<&str>) -> serde_json::Value {
        let value = serde_json::json!({"environmentId":id,"label":label,"status":"linked"});
        self.state
            .write()
            .await
            .environments
            .insert(id.to_owned(), value.clone());
        value
    }
    pub async fn unlink_environment(&self, id: &str) -> bool {
        self.state.write().await.environments.remove(id).is_some()
    }
    pub async fn environment_status(&self, id: &str) -> serde_json::Value {
        let state = self.state.read().await;
        state
            .environments
            .get(id)
            .cloned()
            .unwrap_or_else(|| serde_json::json!({"environmentId":id,"status":"offline"}))
    }
    pub async fn register_device(&self, id: &str, payload: serde_json::Value) -> serde_json::Value {
        self.state
            .write()
            .await
            .devices
            .insert(id.to_owned(), payload);
        serde_json::json!({"ok":true})
    }
    pub async fn unregister_device(&self, id: &str) -> bool {
        self.state.write().await.devices.remove(id).is_some()
    }
    pub async fn update_preferences(
        &self,
        id: &str,
        preferences: serde_json::Value,
    ) -> serde_json::Value {
        self.state
            .write()
            .await
            .preferences
            .insert(id.to_owned(), preferences.clone());
        preferences
    }
    pub async fn wake_hooks(&self, _id: &str) -> serde_json::Value {
        serde_json::json!({"pending":false})
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn link_and_device_state_round_trip() {
        let relay = RelayService::new();
        relay.link_environment("env-1", Some("Laptop")).await;
        assert_eq!(
            relay.list_environments().await["environments"]
                .as_array()
                .unwrap()
                .len(),
            1
        );
        relay
            .register_device("device-1", serde_json::json!({"platform":"ios"}))
            .await;
        assert_eq!(
            relay.list_devices().await["devices"]
                .as_array()
                .unwrap()
                .len(),
            1
        );
        assert!(relay.unlink_environment("env-1").await);
    }
}
