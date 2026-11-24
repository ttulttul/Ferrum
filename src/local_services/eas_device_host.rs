//! Expo Simulators device hosts (the reference's `EasDeviceHost.ts`): cloud
//! simulators that exist only while an Expo session runs, each billed to the
//! Expo account until it stops.
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

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

/// A session this host started and tracks (`RunningSession`). Keyed by
/// Expo's session id, which is also the device id.
#[derive(Clone, Debug)]
pub(super) struct RunningSession {
    pub session_id: String,
    pub name: String,
    pub platform: String,
    /// The id the session's preview host (and agent-device) knows the device by.
    pub remote_device_id: String,
    pub preview_origin: String,
    pub preview_token: String,
    pub agent_url: String,
    pub agent_token: String,
    /// Set once a stop has been asked for but Expo has not confirmed it.
    pub stopping: bool,
}

/// `LedgerEntry`: a session this host may have started, recorded by its
/// unique name before Expo is asked to create it.
#[derive(Clone, Debug, PartialEq)]
pub(super) struct LedgerEntry {
    pub name: String,
    pub app_id: String,
    pub recorded_at: f64,
    /// Expo refused the create: nothing should run, but the reaper still looks.
    pub refused: bool,
}

impl LedgerEntry {
    fn to_json(&self) -> Value {
        let mut entry =
            json!({"name": self.name, "appId": self.app_id, "recordedAt": self.recorded_at});
        if self.refused {
            entry["refused"] = json!(true);
        }
        entry
    }

    fn from_json(value: &Value) -> Option<Self> {
        Some(Self {
            name: value["name"].as_str()?.to_owned(),
            app_id: value["appId"].as_str()?.to_owned(),
            recorded_at: value["recordedAt"].as_f64()?,
            refused: match &value["refused"] {
                Value::Null => false,
                Value::Bool(refused) => *refused,
                _ => return None,
            },
        })
    }
}

/// The ledger's file: `<stateDir>/device/eas/<host id>.json`.
pub(super) fn ledger_path(dir: &Path, host_id: &str) -> PathBuf {
    dir.join("device")
        .join("eas")
        .join(format!("{host_id}.json"))
}

/// `readLedger`: a missing ledger is empty; an unreadable one is an error.
pub(super) fn read_ledger(path: &Path) -> Result<Vec<LedgerEntry>, ()> {
    let text = match std::fs::read_to_string(path) {
        Ok(text) => text,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(_) => return Err(()),
    };
    let entries: Vec<Value> = serde_json::from_str(&text).map_err(|_| ())?;
    entries
        .iter()
        .map(LedgerEntry::from_json)
        .collect::<Option<_>>()
        .ok_or(())
}

/// `writeLedger`'s write: the whole ledger, atomically.
pub(super) fn write_ledger(path: &Path, entries: &[LedgerEntry]) -> std::io::Result<()> {
    let parent = path.parent().unwrap_or(Path::new("."));
    std::fs::create_dir_all(parent)?;
    let contents = Value::Array(entries.iter().map(LedgerEntry::to_json).collect()).to_string();
    let temporary = parent.join(format!(".{}.{}", uuid::Uuid::new_v4().simple(), "tmp"));
    std::fs::write(&temporary, contents)?;
    let renamed = std::fs::rename(&temporary, path);
    let _ = std::fs::remove_file(&temporary);
    renamed
}

/// What an Expo host keeps between calls.
#[derive(Debug)]
pub(super) struct EasState {
    pub config: EasConfig,
    /// Whether this server can read an Expo credential for the host, per platform.
    pub platforms: Value,
    pub ready: bool,
    pub sessions: indexmap::IndexMap<String, RunningSession>,
    /// When each session's status was last confirmed (ms).
    pub checked_at: HashMap<String, f64>,
    /// Starts in progress, by session name.
    pub starting: HashSet<String>,
    /// Sessions whose stop after a failed start failed too: name -> session id.
    pub unresolved: HashMap<String, String>,
    /// Creates whose outcome is unknown, and fresh records a previous server
    /// left: counted against the limit until found or `until` (ms).
    pub uncertain: HashMap<String, (LedgerEntry, f64)>,
    /// Bumped whenever the host stops its sessions.
    pub epoch: u64,
    pub reaped_at: f64,
    /// The credential last read, so stopping a billed session does not
    /// depend on the secret still being readable.
    pub last_credential: Option<super::expo_api::Credential>,
}

impl EasState {
    pub fn new(config: EasConfig) -> Self {
        Self {
            config,
            platforms: json!([]),
            ready: false,
            sessions: Default::default(),
            checked_at: HashMap::new(),
            starting: HashSet::new(),
            unresolved: HashMap::new(),
            uncertain: HashMap::new(),
            epoch: 0,
            reaped_at: 0.0,
            last_credential: None,
        }
    }

    /// `ownedNames`.
    pub fn owned_names(&self) -> HashSet<String> {
        self.sessions
            .values()
            .map(|session| session.name.clone())
            .chain(self.starting.iter().cloned())
            .chain(self.unresolved.keys().cloned())
            .collect()
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

/// `PLATFORMS[platform]`: where its preview host serves it, and its client protocol.
pub(super) fn hub_prefix(platform: &str) -> &'static str {
    if platform == "ios" {
        "/vendor/serve-sim"
    } else {
        "/vendor/serve-emu"
    }
}

pub(super) fn preview_prefix(platform: &str) -> &'static str {
    if platform == "ios" {
        ""
    } else {
        "/vendor/serve-emu"
    }
}

pub(super) fn protocol(platform: &str) -> &'static str {
    if platform == "ios" {
        "serve-sim"
    } else {
        "serve-emu"
    }
}

/// Milliseconds since the epoch (`Clock.currentTimeMillis`).
pub(super) fn now_ms() -> f64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as f64)
        .unwrap_or_default()
}
