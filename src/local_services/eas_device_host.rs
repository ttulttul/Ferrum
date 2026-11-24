//! Expo Simulators device hosts (the reference's `EasDeviceHost.ts`): cloud
//! simulators that exist only while an Expo session runs, each billed to the
//! Expo account until it stops.
use serde_json::{json, Value};

/// `EasDeviceHostConfig`.
#[derive(Clone, Debug, PartialEq)]
pub(super) struct EasConfig {
    pub id: String,
    pub label: String,
    pub app_id: Option<String>,
    pub access_token: Value,
    pub max_session_minutes: u64,
    pub idle_minutes: u64,
    pub max_concurrent_sessions: usize,
    /// The saved entry, which `unchanged` compares whole.
    pub raw: Value,
}

impl EasConfig {
    pub fn from_settings(value: &Value) -> Option<Self> {
        if value["kind"] != "eas" {
            return None;
        }
        Some(Self {
            id: value["id"].as_str()?.to_owned(),
            label: value["label"].as_str()?.to_owned(),
            app_id: value["appId"].as_str().map(str::to_owned),
            access_token: value.get("accessToken").cloned().unwrap_or(json!("")),
            max_session_minutes: value["maxSessionMinutes"].as_u64().unwrap_or(30),
            idle_minutes: value["idleMinutes"].as_u64().unwrap_or(10),
            max_concurrent_sessions: value["maxConcurrentSessions"].as_u64().unwrap_or(2) as usize,
            raw: value.clone(),
        })
    }
}

/// What an Expo host keeps between calls.
#[derive(Debug)]
pub(super) struct EasState {
    pub config: EasConfig,
    /// Whether this server can read an Expo credential for the host, per platform.
    pub platforms: Value,
    pub ready: bool,
}

impl EasState {
    pub fn new(config: EasConfig) -> Self {
        Self {
            config,
            platforms: json!([]),
            ready: false,
        }
    }

    /// `summary`: nothing to install, since the hub and agent-device daemon
    /// run in each session.
    pub fn summary(&self) -> Value {
        json!({
            "id": self.config.id,
            "label": self.config.label,
            "kind": "eas",
            "platforms": self.platforms,
            "hubInstalled": true,
            "agentDeviceInstalled": true,
        })
    }
}

/// `PLATFORMS[platform].name`.
pub(super) fn platform_name(platform: &str) -> &'static str {
    if platform == "ios" {
        "iPhone (Expo Simulator)"
    } else {
        "Android (Expo Simulator)"
    }
}

/// `CATALOG`: Expo picks the simulator model; one entry starts a session.
pub(super) fn catalog(host_id: &str) -> Vec<Value> {
    ["ios", "android"]
        .into_iter()
        .map(|platform| {
            json!({
                "hostId": host_id,
                "id": format!("eas-{platform}"),
                "platform": platform,
                "name": platform_name(platform),
                "version": "Cloud",
                "booted": false,
                "physical": false,
            })
        })
        .collect()
}
