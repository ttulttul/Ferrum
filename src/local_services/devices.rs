use super::agent_device::{self, AgentEndpoint, StartError};
use super::eas_device_host::{self as eas, EasConfig, EasState};
use super::eas_project_id;
use super::expo_api;
use super::simlock_device_host::{self as simlock, Lease, SimlockConfig, SimlockState};
use super::ssh_device_host::{self as ssh_host, SshConfig};
use super::{invalid, string, timestamp, Reply};
use crate::settings::{
    secrets::{provider_label, ReadMode, Resolver},
    Settings,
};
use futures_util::StreamExt;
use serde_json::{json, Value};
use std::{
    collections::{HashMap, HashSet},
    env,
    ffi::OsString,
    fs,
    path::{Path, PathBuf},
    process::Stdio,
    sync::{Arc, Mutex},
    time::{Duration, Instant, UNIX_EPOCH},
};
use tokio::{process::Child, sync::broadcast};

const HUB: (&str, &str, &str) = ("expo-device-hub", "0.26.1", "dist/server/cli.mjs");
const AGENT: (&str, &str, &str) = ("agent-device", "0.21.41", "bin/agent-device.mjs");
const HUB_READY_TIMEOUT: Duration = Duration::from_secs(30);
const LIST_TIMEOUT: Duration = Duration::from_secs(40);
const BOOT_TIMEOUT: Duration = Duration::from_secs(180);
const AVDMANAGER_LIST_FAILED: &str = "[android-utils] Failed to run `avdmanager list avd`";
const LOCAL_HOST_ID: &str = "local";
const LOCAL_HOST_LABEL: &str = "This machine";
/// Expo Simulators: how often a starting session is polled, how stale a
/// running session's status may get, and how long a start may take.
const EAS_POLL_INTERVAL: Duration = Duration::from_secs(5);
const STATUS_TTL_MS: f64 = 15_000.0;
const EAS_START_TIMEOUT: Duration = Duration::from_secs(15 * 60);
const EAS_PREVIEW_TIMEOUT: Duration = Duration::from_secs(20);
const REAP_INTERVAL_MS: f64 = 60_000.0;
const RECORD_LIFETIME_MS: f64 = 180.0 * 60_000.0;
const RECORD_MAX_AGE_MS: f64 = 24.0 * 60.0 * 60_000.0;
const UNCERTAIN_CREATE_MS: f64 = 5.0 * 60_000.0;
type Tool = (&'static str, &'static str, &'static str);
type HubEnv = (Vec<(OsString, OsString)>, Option<String>);

struct HubRuntime {
    origin: String,
    child: Child,
    emulator_extra_args: Option<String>,
    emulator_stamp: Option<String>,
    state_file: PathBuf,
}

impl Drop for HubRuntime {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.state_file);
    }
}

/// Where a host that routes its own hub requests sends one: the URL, and the
/// headers that authorize it.
pub type HubRoute = (String, Vec<(String, String)>);

/// What SSH host supervision remembers between ticks.
#[derive(Default)]
struct Supervision {
    ticks: u64,
    /// Consecutive missed health probes, by host.
    misses: HashMap<String, u32>,
    /// Hosts waiting to reconnect: when next, and the current backoff.
    reconnecting: HashMap<String, (tokio::time::Instant, Duration)>,
}

/// An SSH host's forwarded hub.
struct SshReady {
    ctx: HostCtx,
    tunnel: ssh_host::Tunnel,
    /// Its agent-device daemon, forwarded here, once agents want the host.
    agent: Option<AgentEndpoint>,
}

pub struct DeviceService {
    settings: Arc<Settings>,
    state: Mutex<Value>,
    events: broadcast::Sender<Value>,
    runtime: tokio::sync::Mutex<Option<HubRuntime>>,
    /// This machine's agent-device daemon, once started (`RunningHost.agentDevice`).
    agent: Mutex<Option<AgentEndpoint>>,
    /// Launchers bound since startup, by agent-device session.
    launchers: tokio::sync::Mutex<HashMap<String, PathBuf>>,
    /// The SSH device hosts from settings (`deviceHosts`) that are not this
    /// machine, in settings order, with each one's latest summary.
    ssh_hosts: Mutex<Vec<(SshConfig, Value)>>,
    /// The SSH hosts whose hub is forwarded here, by id.
    ssh_ready: tokio::sync::Mutex<HashMap<String, SshReady>>,
    /// The SSH hosts this server started something on, which `stop` stops.
    ssh_activated: Mutex<HashSet<String>>,
    /// The SSH hosts agents use (`wantsAgent`), which start with their daemon.
    ssh_wants_agent: Mutex<HashSet<String>>,
    /// The Expo Simulators hosts from settings (`easDeviceHosts`).
    eas_hosts: Mutex<Vec<EasState>>,
    /// Serializes the Expo hosts' session ledgers (`ledgerLock`).
    eas_ledger_lock: tokio::sync::Mutex<()>,
    /// The Simlock hosts from settings (`simlockDeviceHosts`, when the alpha is on).
    simlock_hosts: Mutex<Vec<SimlockState>>,
    /// Launchers on a session a host leases to every thread (a Simlock
    /// lease's); the host ends it, not a retiring thread.
    host_session_launchers: Mutex<HashSet<String>>,
    /// Serializes host reconciles (the reference's lifecycle lock).
    reconcile_lock: tokio::sync::Mutex<()>,
}

const DEVICE_SETTINGS: [(&str, &str); 3] = [
    ("enabled", "enableDeviceSupport"),
    ("agentAccessEnabled", "enableAgentDeviceAccess"),
    ("onboardingCompleted", "deviceOnboardingCompleted"),
];

impl DeviceService {
    pub(crate) fn new(settings: Arc<Settings>, dir: &Path) -> anyhow::Result<Self> {
        if let Err(error) = migrate_device_preferences(&settings, dir) {
            tracing::warn!(%error,"Could not migrate saved device preferences");
        }
        let state = initial(dir, &settings.internal()?);
        // Sessions live in memory, so this removes every launcher a previous run left behind.
        agent_device::prune_launchers_now(dir);
        Ok(Self {
            settings,
            state: Mutex::new(state),
            events: broadcast::channel(128).0,
            runtime: Default::default(),
            agent: Mutex::new(None),
            launchers: Default::default(),
            ssh_hosts: Mutex::new(Vec::new()),
            ssh_ready: Default::default(),
            ssh_activated: Default::default(),
            ssh_wants_agent: Default::default(),
            eas_hosts: Default::default(),
            eas_ledger_lock: Default::default(),
            simlock_hosts: Default::default(),
            host_session_launchers: Default::default(),
            reconcile_lock: Default::default(),
        })
    }
}

fn migrate_device_preferences(settings: &Settings, dir: &Path) -> anyhow::Result<()> {
    // Older Rust servers kept just these booleans in a separate file.
    let legacy = dir.join("device-settings.json");
    if legacy.try_exists()? {
        let old: Value = serde_json::from_slice(&fs::read(&legacy)?)?;
        let configured: Value = match fs::read(dir.join("settings.json")) {
            Ok(bytes) => {
                crate::settings::parse(std::str::from_utf8(&bytes)?).map_err(anyhow::Error::msg)?
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Value::Null,
            Err(e) => return Err(e.into()),
        };
        let mut patch = json!({});
        for (before, after) in DEVICE_SETTINGS {
            if configured.get(after).is_none() {
                if let Some(v) = old[before].as_bool() {
                    patch[after] = json!(v);
                }
            }
        }
        if patch != json!({}) {
            settings
                .update(&json!({"patch":patch}), true)
                .map_err(|_| anyhow::anyhow!("Could not migrate saved device preferences"))?;
        }
        fs::remove_file(legacy)?;
    }
    Ok(())
}

fn home() -> PathBuf {
    env::var_os("HOME")
        .or_else(|| env::var_os("USERPROFILE"))
        .unwrap_or_default()
        .into()
}

fn installed(dir: &Path, spec: Tool) -> Vec<String> {
    let base = super::base_directory(dir).join("tools").join(spec.0);
    let mut versions: Vec<_> = fs::read_dir(&base)
        .into_iter()
        .flatten()
        .flatten()
        .filter_map(|e| {
            let v = e.file_name().into_string().ok()?;
            let path = e.path();
            (fs::read_to_string(path.join(".install-complete"))
                .ok()?
                .trim()
                == v
                && path
                    .join("node_modules")
                    .join(spec.0)
                    .join(spec.2)
                    .is_file())
            .then_some(v)
        })
        .collect();
    versions.sort();
    versions
}

pub(super) fn sdk_root() -> Option<PathBuf> {
    let explicit: Vec<_> = ["ANDROID_HOME", "ANDROID_SDK_ROOT"]
        .iter()
        .filter_map(env::var_os)
        .filter_map(|v| {
            let s = v.to_string_lossy().trim().to_owned();
            (!s.is_empty()).then(|| PathBuf::from(s))
        })
        .collect();
    let mut candidates = explicit.clone();
    candidates.extend([
        home().join("Library/Android/sdk"),
        home().join("Android/Sdk"),
        env::var_os("LOCALAPPDATA")
            .map(PathBuf::from)
            .unwrap_or_else(|| home().join("AppData/Local"))
            .join("Android/Sdk"),
    ]);
    let has_tool = |p: &Path| {
        p.join("platform-tools")
            .join(if cfg!(windows) { "adb.exe" } else { "adb" })
            .is_file()
            || p.join("emulator")
                .join(if cfg!(windows) {
                    "emulator.exe"
                } else {
                    "emulator"
                })
                .is_file()
    };
    if let Some(found) = candidates.into_iter().find(|p| has_tool(p)) {
        return Some(found);
    }
    if let Some(path) = crate::login_shell::path() {
        for p in env::split_paths(&path) {
            let adb = p.join(if cfg!(windows) { "adb.exe" } else { "adb" });
            if !adb.is_file() {
                continue;
            }
            let resolved = fs::canonicalize(&adb).unwrap_or(adb);
            if let Some(root) = resolved.parent().and_then(Path::parent) {
                if has_tool(root) {
                    return Some(root.to_path_buf());
                }
            }
        }
    }
    explicit.into_iter().next()
}

fn availability(platform: &str) -> Value {
    let reason = if platform == "ios" {
        if !cfg!(target_os = "macos") {
            Some("iOS Simulators need macOS with Xcode.".into())
        } else if !Path::new("/usr/bin/xcrun").exists() {
            Some("Xcode command line tools were not found.".into())
        } else {
            None
        }
    } else if let Some(root) = sdk_root() {
        let suffix = if cfg!(windows) { ".exe" } else { "" };
        let avd = if cfg!(windows) {
            "avdmanager.bat"
        } else {
            "avdmanager"
        };
        let dir = root.display();
        if !root.join(format!("platform-tools/adb{suffix}")).is_file() {
            Some(format!("Android SDK Platform-Tools are missing from {dir}. Install them in Android Studio's SDK Manager."))
        } else if !root.join(format!("emulator/emulator{suffix}")).is_file() {
            Some(format!("Android Emulator is missing from {dir}. Install it in Android Studio's SDK Manager."))
        } else if !root.join("cmdline-tools/latest/bin").join(avd).is_file() {
            if root.join("tools/bin").join(avd).is_file() {
                Some(format!("The Android SDK command-line tools in {dir} appear to be an older, unsupported version. Install Android SDK Command-line Tools (latest) in Android Studio's SDK Manager under SDK Tools."))
            } else {
                Some(format!("Android SDK Command-line Tools (latest) are missing from {dir}. Install them in Android Studio's SDK Manager."))
            }
        } else {
            None
        }
    } else {
        Some("Android SDK was not found. Install it with Android Studio or set ANDROID_HOME to your SDK directory.".into())
    };
    let mut r = json!({"platform":platform,"available":reason.is_none()});
    if let Some(s) = reason {
        r["reason"] = json!(s);
    }
    r
}

fn host(dir: &Path, running: bool) -> Value {
    let hub = installed(dir, HUB);
    let agent = installed(dir, AGENT);
    json!({
        "id": LOCAL_HOST_ID,
        "label": LOCAL_HOST_LABEL,
        "platforms": [availability("ios"), availability("android")],
        "tools": {
            "hub": {
                "requiredVersion": HUB.1,
                "installedVersions": hub,
                "runningVersion": running.then_some(HUB.1),
            },
            "agent": {
                "requiredVersion": AGENT.1,
                "installedVersions": agent,
                "runningVersion": if running { agent_device::running_version(dir) } else { None },
            }
        },
        "hubInstalled": hub.iter().any(|v| v == HUB.1),
        "agentDeviceInstalled": agent.iter().any(|v| v == AGENT.1),
        "kind": "local",
    })
}

fn disabled() -> Value {
    json!({
        "_tag": "DeviceHostUnavailableError",
        "hostId": LOCAL_HOST_ID,
        "hostLabel": LOCAL_HOST_LABEL,
        "reason": "Device support is off. Enable it in the Device panel before installing or starting device tools.",
    })
}

fn bad_host(id: &str) -> Value {
    json!({"_tag":"DeviceHostUnavailableError","hostId":id,"reason":"Unknown host."})
}

fn op_err(op: &str, reason: &str) -> Value {
    json!({"_tag":"DeviceOperationError","operation":op,"reason":reason,"cause":null})
}

fn host_err(step: &str, reason: Option<String>) -> Value {
    json!({
        "_tag": "DeviceHostUnavailableError",
        "hostId": LOCAL_HOST_ID,
        "hostLabel": LOCAL_HOST_LABEL,
        "reason": reason.unwrap_or_else(|| format!("Device support failed during {step}.")),
    })
}

fn error_detail(error: &Value) -> String {
    if error["_tag"] == "DeviceHostUnavailableError" {
        if let Some(reason) = error["reason"].as_str() {
            return reason.to_owned();
        }
    }
    error["reason"]
        .as_str()
        .or_else(|| error["message"].as_str())
        .unwrap_or("Device host failed.")
        .to_owned()
}

fn initial(dir: &Path, saved: &Value) -> Value {
    json!({
        "supportsHostRetry": true,
        "supportsToolUpdate": true,
        "supportsToolInspection": true,
        "supportsEasHosts": true,
        "supportsSimlockHosts": true,
        "hostStatus": if saved["enableDeviceSupport"] == true { "idle" } else { "disabled" },
        "hostStatuses": {},
        "devices": [],
        "sessions": [],
        "onboardingCompleted": saved["deviceOnboardingCompleted"] == true,
        "agentAccessEnabled": saved["enableAgentDeviceAccess"] == true,
        "hubBasePath": "/api/device-hub",
        "revision": 0,
        "hosts": [host(dir, false)],
        "easHosts": [],
        "simlockHosts": [],
    })
}

/// The state as clients receive it (`DeviceServiceState`'s encoding): this
/// machine's and SSH hosts in `hosts`, Expo and Simlock hosts in their own lists.
fn wire(state: &Value) -> Value {
    let mut state = state.clone();
    let hosts: Vec<Value> = state["hosts"].as_array().cloned().unwrap_or_default();
    let kind = |kinds: &[&str]| {
        Value::Array(
            hosts
                .iter()
                .filter(|host| kinds.iter().any(|k| host["kind"] == *k))
                .cloned()
                .collect(),
        )
    };
    state["hosts"] = kind(&["local", "ssh"]);
    state["easHosts"] = kind(&["eas"]);
    state["simlockHosts"] = kind(&["simlock"]);
    ordered(&state)
}

/// The state with its keys in the reference's order.
fn ordered(state: &Value) -> Value {
    let mut ordered = json!({});
    for k in [
        "supportsHostRetry",
        "supportsToolUpdate",
        "supportsToolInspection",
        "supportsEasHosts",
        "supportsSimlockHosts",
        "hostStatus",
        "hostStatusDetail",
        "hostStatuses",
        "devices",
        "sessions",
        "bootingDevices",
        "onboardingCompleted",
        "agentAccessEnabled",
        "hubBasePath",
        "revision",
        "hosts",
        "easHosts",
        "simlockHosts",
    ] {
        if let Some(v) = state.get(k) {
            ordered[k] = v.clone();
        }
    }
    ordered
}

fn emulator_binary(root: &Path) -> PathBuf {
    root.join("emulator").join(if cfg!(windows) {
        "emulator.exe"
    } else {
        "emulator"
    })
}

fn adb_binary(root: &Path) -> PathBuf {
    root.join("platform-tools")
        .join(if cfg!(windows) { "adb.exe" } else { "adb" })
}

fn device_host_env(sdk: Option<&Path>) -> Vec<(OsString, OsString)> {
    let Some(root) = sdk else {
        return Vec::new();
    };
    let mut entries = vec![root.join("platform-tools"), root.join("emulator")];
    if let Some(existing) = crate::login_shell::path().or_else(|| env::var_os("PATH")) {
        entries.extend(env::split_paths(&existing));
    }
    let joined = env::join_paths(entries).unwrap_or_default();
    vec![
        (OsString::from("ANDROID_HOME"), root.as_os_str().to_owned()),
        (OsString::from("PATH"), joined),
    ]
}

fn has_word(haystack: &str, needle: &str) -> bool {
    let is_word = |b: u8| b.is_ascii_alphanumeric() || b == b'_';
    let bytes = haystack.as_bytes();
    haystack.match_indices(needle).any(|(start, _)| {
        let end = start + needle.len();
        let before_ok = start == 0 || !is_word(bytes[start - 1]);
        let after_ok = end == bytes.len() || !is_word(bytes[end]);
        before_ok && after_ok
    })
}

fn emulator_stamp(sdk: Option<&Path>) -> Option<String> {
    let meta = fs::metadata(emulator_binary(sdk?)).ok()?;
    let mtime_ms = meta
        .modified()
        .ok()
        .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
        .map(|d| d.as_millis())?;
    Some(format!("{mtime_ms}:{}", meta.len()))
}

async fn run_host_command(
    command: &str,
    args: &[&str],
    sdk: Option<&Path>,
    timeout: Duration,
) -> (String, String, i32) {
    let program: PathBuf = match (command, sdk) {
        ("emulator", Some(root)) => emulator_binary(root),
        ("adb", Some(root)) if adb_binary(root).is_file() => adb_binary(root),
        _ => PathBuf::from(command),
    };
    let mut cmd = crate::login_shell::tokio_command(program);
    cmd.args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    for (k, v) in device_host_env(sdk) {
        cmd.env(k, v);
    }
    match tokio::time::timeout(timeout, cmd.output()).await {
        Ok(Ok(output)) => (
            String::from_utf8_lossy(&output.stdout).into_owned(),
            String::from_utf8_lossy(&output.stderr).into_owned(),
            output.status.code().unwrap_or(127),
        ),
        Ok(Err(error)) => (String::new(), error.to_string(), 127),
        Err(_) => (String::new(), String::new(), 124),
    }
}

async fn hub_environment(sdk: Option<&Path>) -> (Vec<(OsString, OsString)>, Option<String>) {
    let mut vars = device_host_env(sdk);
    vars.push((OsString::from("FORCE_COLOR"), OsString::from("0")));
    vars.push((OsString::from("NO_COLOR"), OsString::from("1")));
    let mut extra_args = env::var("EXPO_DEVICE_HUB_EMULATOR_EXTRA_ARGS")
        .ok()
        .filter(|s| !s.is_empty());
    if let Some(root) = sdk {
        let mut camera_overrides = Vec::new();
        for facing in ["front", "back"] {
            let flag = format!("-help-camera-{facing}");
            let (stdout, stderr, code) =
                run_host_command("emulator", &[&flag], Some(root), Duration::from_secs(5)).await;
            let help = format!("{stdout}\n{stderr}");
            if code == 0 && has_word(&help, "emulated") && !has_word(&help, "imagefile") {
                camera_overrides.push(format!("-camera-{facing} emulated"));
            }
        }
        if !camera_overrides.is_empty() {
            let mut combined = camera_overrides.join(" ");
            if let Some(existing) = extra_args.as_deref() {
                combined.push(' ');
                combined.push_str(existing);
            }
            vars.push((
                OsString::from("EXPO_DEVICE_HUB_EMULATOR_EXTRA_ARGS"),
                OsString::from(&combined),
            ));
            extra_args = Some(combined);
        }
    }
    #[cfg(target_os = "linux")]
    if env::var_os("XDG_RUNTIME_DIR").is_none() {
        use std::os::unix::fs::MetadataExt;
        if let Ok(self_meta) = fs::metadata("/proc/self") {
            let uid = self_meta.uid();
            let runtime_dir = format!("/run/user/{uid}");
            if let Ok(meta) = fs::metadata(&runtime_dir) {
                if meta.is_dir() && meta.uid() == uid {
                    vars.push((
                        OsString::from("XDG_RUNTIME_DIR"),
                        OsString::from(runtime_dir),
                    ));
                }
            }
        }
    }
    (vars, extra_args)
}

fn hub_state_path(dir: &Path) -> PathBuf {
    dir.join("device").join("agent-device").join("hub.json")
}

async fn reap_stale_hub(dir: &Path) {
    let state_path = hub_state_path(dir);
    let Ok(raw) = tokio::fs::read_to_string(&state_path).await else {
        return;
    };
    let Ok(recorded) = serde_json::from_str::<Value>(&raw) else {
        let _ = tokio::fs::remove_file(&state_path).await;
        return;
    };
    let Some(pid) = recorded["pid"].as_u64() else {
        let _ = tokio::fs::remove_file(&state_path).await;
        return;
    };
    let Some(entry_path) = recorded["entryPath"].as_str() else {
        let _ = tokio::fs::remove_file(&state_path).await;
        return;
    };
    #[cfg(unix)]
    {
        let pid_str = pid.to_string();
        let output = tokio::time::timeout(
            Duration::from_secs(5),
            tokio::process::Command::new("ps")
                .args(["-o", "command=", "-p", &pid_str])
                .stdin(Stdio::null())
                .stdout(Stdio::piped())
                .stderr(Stdio::null())
                .kill_on_drop(true)
                .output(),
        )
        .await;
        if let Ok(Ok(out)) = output {
            if out.status.success() && String::from_utf8_lossy(&out.stdout).contains(entry_path) {
                tracing::warn!(
                    pid,
                    "Stopping a device hub left behind by a previous server"
                );
                let _ = tokio::process::Command::new("kill")
                    .args(["-TERM", &pid_str])
                    .stdin(Stdio::null())
                    .stdout(Stdio::null())
                    .stderr(Stdio::null())
                    .status()
                    .await;
            }
        }
    }
    let _ = tokio::fs::remove_file(&state_path).await;
}

async fn record_hub_state(dir: &Path, pid: u32, port: u16, entry: &Path) {
    let path = hub_state_path(dir);
    if let Some(parent) = path.parent() {
        let _ = tokio::fs::create_dir_all(parent).await;
    }
    let payload = json!({
        "pid": pid,
        "port": port,
        "entryPath": entry.to_string_lossy(),
    })
    .to_string();
    let temp = path.with_extension("json.tmp");
    if tokio::fs::write(&temp, payload).await.is_ok() {
        let _ = tokio::fs::rename(&temp, &path).await;
    }
}

async fn install(dir: &Path, spec: Tool) -> Result<PathBuf, Value> {
    let base = super::base_directory(dir)
        .join("tools")
        .join(spec.0)
        .join(spec.1);
    let entry = base.join("node_modules").join(spec.0).join(spec.2);
    if installed(dir, spec).iter().any(|v| v == spec.1) {
        return Ok(entry);
    }
    tokio::fs::create_dir_all(&base)
        .await
        .map_err(|_| op_err("install", "command_failed"))?;
    let package = format!("{}@{}", spec.0, spec.1);
    let status = crate::login_shell::tokio_command("npm")
        .current_dir(&base)
        .args([
            "install",
            "--no-save",
            "--no-audit",
            "--no-fund",
            "--no-package-lock",
            &package,
        ])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .status()
        .await
        .map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                op_err("install", "package_manager_missing")
            } else {
                op_err("install", "command_failed")
            }
        })?;
    if !status.success() || !entry.is_file() {
        return Err(op_err("install", "command_failed"));
    }
    tokio::fs::write(base.join(".install-complete"), format!("{}\n", spec.1))
        .await
        .map_err(|_| op_err("install", "command_failed"))?;
    Ok(entry)
}

fn client() -> reqwest::Client {
    reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(Duration::from_secs(40))
        .build()
        .unwrap()
}

fn hub_client(timeout: Duration) -> reqwest::Client {
    reqwest::Client::builder()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(timeout)
        .build()
        .unwrap()
}

async fn query(request: reqwest::RequestBuilder, op: &str) -> Reply {
    let res = request
        .send()
        .await
        .map_err(|_| op_err(op, "request_failed"))?;
    if !res.status().is_success() {
        return Err(op_err(op, "request_failed"));
    }
    res.json().await.map_err(|_| op_err(op, "request_failed"))
}

fn running_origin(runtime: &mut Option<HubRuntime>) -> Option<String> {
    let hub = runtime.as_mut()?;
    if hub.child.try_wait().ok() == Some(None) {
        Some(hub.origin.clone())
    } else {
        None
    }
}

async fn ensure_hub(runtime: &mut Option<HubRuntime>, dir: &Path) -> Result<String, Value> {
    let sdk = sdk_root();
    let stamp = emulator_stamp(sdk.as_deref());
    let mut computed_env: Option<HubEnv> = None;
    if let Some(hub) = runtime.as_mut() {
        if hub.child.try_wait().ok() == Some(None) {
            if hub.emulator_stamp == stamp {
                return Ok(hub.origin.clone());
            }
            let (vars, extra) = hub_environment(sdk.as_deref()).await;
            if extra == hub.emulator_extra_args {
                hub.emulator_stamp = stamp;
                return Ok(hub.origin.clone());
            }
            computed_env = Some((vars, extra));
            runtime.take();
        }
    }
    let entry = install(dir, HUB).await.map_err(|err| {
        if err["reason"] == "package_manager_missing" {
            host_err(
                "installing device support",
                Some("Device support needs npm, which is not on the T3 server's PATH. Install npm (it comes with Node.js from nodejs.org), restart T3 Code, then retry.".into()),
            )
        } else {
            host_err("installing device support", None)
        }
    })?;
    reap_stale_hub(dir).await;
    let state_file = hub_state_path(dir);
    if let Some(parent) = state_file.parent() {
        let _ = tokio::fs::create_dir_all(parent).await;
    }
    let listener = std::net::TcpListener::bind(("127.0.0.1", 0))
        .map_err(|_| host_err("reserving a port for the device hub", None))?;
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    let (vars, extra_args) = match computed_env {
        Some(pair) => pair,
        None => hub_environment(sdk.as_deref()).await,
    };
    let mut cmd = crate::login_shell::tokio_command("node");
    cmd.arg(&entry).args([
        "--port",
        &port.to_string(),
        "--host",
        "127.0.0.1",
        "--hide-sidebar",
        "--hide-boot-device",
    ]);
    if !cfg!(target_os = "macos") {
        cmd.args(["--platform", "android"]);
    }
    for (k, v) in vars {
        cmd.env(k, v);
    }
    let mut child = cmd
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .map_err(|_| host_err("starting the device hub", None))?;
    let origin = format!("http://127.0.0.1:{port}");
    let readyz = format!("{origin}/readyz");
    let probe_client = hub_client(Duration::from_secs(2));
    let deadline = Instant::now() + HUB_READY_TIMEOUT;
    while Instant::now() < deadline {
        if child.try_wait().ok().flatten().is_some() {
            break;
        }
        if let Ok(res) = probe_client.get(&readyz).send().await {
            if res.status().is_success() {
                if let Some(pid) = child.id() {
                    record_hub_state(dir, pid, port, &entry).await;
                }
                *runtime = Some(HubRuntime {
                    origin: origin.clone(),
                    child,
                    emulator_extra_args: extra_args,
                    emulator_stamp: stamp,
                    state_file,
                });
                return Ok(origin);
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let _ = child.kill().await;
    Err(host_err("waiting for the device hub to answer", None))
}

fn bound_diagnostics(text: &str) -> String {
    let mut chars = text.chars();
    let head: String = chars.by_ref().take(1_999).collect();
    if chars.next().is_some() {
        format!("{head}…")
    } else {
        text.to_owned()
    }
}

fn error_head(error: &str) -> String {
    let mut out = Vec::new();
    for line in error.lines() {
        let trimmed_start = line.trim_start();
        if trimmed_start.len() < line.len() && trimmed_start.starts_with("at ") {
            break;
        }
        let trimmed = line.trim();
        if !trimmed.is_empty() {
            out.push(trimmed);
        }
    }
    out.join(" ")
}

fn is_core_sim_enoent(message: &str, error: &str) -> bool {
    let Some(rest) = message.strip_prefix("[apple-utils] Failed to read CoreSimulator plist at ")
    else {
        return false;
    };
    let Some(before_plist) = rest.strip_suffix("/device.plist:") else {
        return false;
    };
    let Some((_, id)) = before_plist.rsplit_once("/CoreSimulator/Devices/") else {
        return false;
    };
    if id.is_empty() || id.contains('/') {
        return false;
    }
    error.starts_with("Error:") && error.contains("ENOENT:")
        || error.contains("code: 'ENOENT'")
        || error.contains("code: \"ENOENT\"")
}

fn is_simulator_list_failed(message: &str) -> bool {
    let Some(rest) = message.strip_prefix("[apple-utils] ") else {
        return false;
    };
    rest.starts_with("Failed to run `xcrun simctl list devices")
        || rest.starts_with("Failed to parse JSON output:")
        || rest.starts_with("Failed to list devices:")
        || rest.starts_with("`xcrun simctl list devices --json` did not respond within ")
}

/// How a host's commands run: on this machine, or over SSH from the
/// directory its hub runs in (`DeviceHostReady.run`).
#[derive(Clone)]
pub(super) enum HostRun {
    Local,
    Ssh { config: SshConfig, owner: String },
}

/// A ready host: its id, its hub's origin here, how its commands run, its
/// Node, and which platforms it can run (`DeviceHostReady`).
#[derive(Clone)]
pub(super) struct HostCtx {
    pub id: String,
    pub origin: String,
    pub run: HostRun,
    pub node_path: String,
    pub ios: bool,
    pub android: bool,
}

impl HostCtx {
    fn local(origin: &str) -> Self {
        Self {
            id: LOCAL_HOST_ID.into(),
            origin: origin.to_owned(),
            run: HostRun::Local,
            node_path: agent_device::node_executable()
                .map(|p| p.to_string_lossy().into_owned())
                .unwrap_or_else(|| "node".into()),
            ios: availability("ios")["available"] == true,
            android: availability("android")["available"] == true,
        }
    }

    /// `run`: stdout, stderr and the exit code (124 on a timeout, 127 when
    /// it could not run).
    async fn run(
        &self,
        command: &str,
        args: &[&str],
        timeout: Option<Duration>,
    ) -> (String, String, i32) {
        match &self.run {
            HostRun::Local => {
                run_host_command(
                    command,
                    args,
                    sdk_root().as_deref(),
                    timeout.unwrap_or(Duration::from_secs(20)),
                )
                .await
            }
            HostRun::Ssh { config, owner } => {
                let words = std::iter::once(command)
                    .chain(args.iter().copied())
                    .map(ssh_host::quote_remote_arg)
                    .collect::<Vec<_>>()
                    .join(" ");
                // Match the hub cwd so relative executables and PATH entries resolve identically.
                let script = format!("cd \"$HOME/.t3/device/hosts/{owner}\" && exec {words}");
                let identity = config
                    .identity_file
                    .iter()
                    .flat_map(|file| ["-i".to_owned(), file.clone()])
                    .collect::<Vec<_>>();
                match ssh_host::run_ssh(
                    config,
                    &identity,
                    &ssh_host::command_args(&script),
                    None,
                    timeout,
                    true,
                )
                .await
                {
                    Ok((stdout, stderr)) => (stdout, stderr, 0),
                    Err(error) => (error.stdout, error.message, error.exit_code.unwrap_or(127)),
                }
            }
        }
    }
}

struct HubListResult {
    devices: Vec<Value>,
    detail: Option<String>,
    android_discovery_incomplete: bool,
    incomplete_platforms: Vec<&'static str>,
}

async fn list_adb_devices(ctx: &HostCtx) -> Option<Vec<Value>> {
    let (stdout, _, code) = ctx.run("adb", &["devices", "-l"], None).await;
    if code != 0 {
        return None;
    }
    let mut serials = Vec::new();
    for line in stdout.lines() {
        let mut parts = line.split_whitespace();
        if let (Some(serial), Some("device")) = (parts.next(), parts.next()) {
            serials.push(serial.to_owned());
        }
    }
    let mut devices = Vec::with_capacity(serials.len());
    for serial in serials {
        let (getprop_out, _, _) = ctx
            .run("adb", &["-s", &serial, "shell", "getprop"], None)
            .await;
        let mut props = HashMap::new();
        for line in getprop_out.lines() {
            let trimmed = line.trim();
            if let Some(rest) = trimmed.strip_prefix('[') {
                if let Some((key, val_rest)) = rest.split_once("]: [") {
                    if let Some(val) = val_rest.strip_suffix(']') {
                        props.insert(key.to_owned(), val.to_owned());
                    }
                }
            }
        }
        let emulator = props.get("ro.kernel.qemu").map(String::as_str) == Some("1")
            || props.get("ro.boot.qemu").map(String::as_str) == Some("1");
        let avd_name = if emulator {
            let (avd_out, _, _) = ctx
                .run("adb", &["-s", &serial, "emu", "avd", "name"], None)
                .await;
            avd_out
                .lines()
                .map(str::trim)
                .find(|line| !line.is_empty() && *line != "OK" && !line.starts_with("KO"))
                .map(str::to_owned)
        } else {
            None
        };
        let model = props
            .get("ro.product.model")
            .map(String::as_str)
            .filter(|s| !s.is_empty());
        let manufacturer = props
            .get("ro.product.manufacturer")
            .map(String::as_str)
            .filter(|s| !s.is_empty());
        let release = props
            .get("ro.build.version.release")
            .map(String::as_str)
            .filter(|s| !s.is_empty());
        let name = if emulator {
            avd_name.unwrap_or_else(|| serial.clone())
        } else if let Some(model) = model {
            match manufacturer {
                Some(m) => format!("{m} {model}"),
                None => model.to_owned(),
            }
        } else {
            serial.clone()
        };
        let version = match release {
            Some(r) if r.bytes().all(|b| b.is_ascii_digit()) => format!("Android {r}.0"),
            Some(r) => format!("Android {r}"),
            None => "Android".to_owned(),
        };
        devices.push(json!({
            "hostId": ctx.id,
            "id": serial,
            "name": name,
            "platform": "android",
            "version": version,
            "booted": true,
            "physical": !emulator,
        }));
    }
    Some(devices)
}

async fn list_hub(ctx: &HostCtx) -> Result<HubListResult, Value> {
    let origin = &ctx.origin;
    let android_available = ctx.android;
    let ios_available = ctx.ios;
    let request = hub_client(LIST_TIMEOUT).get(format!("{origin}/api/devices"));
    let result: Value = match tokio::time::timeout(LIST_TIMEOUT, request.send()).await {
        Err(_) => {
            let mut incomplete = Vec::new();
            if ios_available {
                incomplete.push("ios");
            }
            if android_available {
                incomplete.push("android");
            }
            return Ok(HubListResult {
                devices: Vec::new(),
                detail: Some(format!(
                    "The device hub did not answer within {} seconds.",
                    LIST_TIMEOUT.as_secs()
                )),
                android_discovery_incomplete: false,
                incomplete_platforms: incomplete,
            });
        }
        Ok(Ok(res)) if res.status().is_success() => res
            .json()
            .await
            .map_err(|_| op_err("list", "request_failed"))?,
        Ok(_) => return Err(op_err("list", "request_failed")),
    };
    let simulators = result["simulators"]
        .as_array()
        .ok_or_else(|| op_err("list", "request_failed"))?;
    let emulators = result["emulators"]
        .as_array()
        .ok_or_else(|| op_err("list", "request_failed"))?;
    let mut devices = Vec::with_capacity(simulators.len() + emulators.len());
    for device in simulators.iter().chain(emulators.iter()) {
        let mut record = json!({"hostId": ctx.id});
        for k in ["id", "platform", "name", "version", "booted", "physical"] {
            record[k] = device[k].clone();
        }
        devices.push(record);
    }
    let hub_errors: Vec<(&str, &str)> = result["errors"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|e| Some((e["message"].as_str()?, e["error"].as_str().unwrap_or(""))))
        .collect();
    let mut details: Vec<String> = hub_errors
        .iter()
        .filter(|(message, error)| {
            (android_available || !message.starts_with("[android-utils] "))
                && !is_core_sim_enoent(message, error)
        })
        .map(|(message, error)| {
            let head = error_head(error);
            if head.is_empty() {
                bound_diagnostics(message)
            } else {
                bound_diagnostics(&format!("{message} {head}"))
            }
        })
        .collect();
    let mut android_discovery_incomplete = false;
    if android_available {
        if hub_errors
            .iter()
            .any(|(message, _)| message.starts_with(AVDMANAGER_LIST_FAILED))
        {
            match list_adb_devices(ctx).await {
                None => {
                    android_discovery_incomplete = true;
                    details.push("Could not list Android devices (adb devices -l).".into());
                }
                Some(adb_devices) => devices.extend(adb_devices),
            }
        }
        let (avds_stdout, _, avds_code) = ctx.run("emulator", &["-list-avds"], None).await;
        if avds_code != 0 {
            android_discovery_incomplete = true;
            details.push(format!(
                "Could not list Android virtual devices (emulator -list-avds, exit code {avds_code}). Discovered devices remain available."
            ));
        } else {
            for name in avds_stdout
                .lines()
                .map(str::trim)
                .filter(|line| !line.is_empty())
            {
                if !devices
                    .iter()
                    .any(|d| d["platform"] == "android" && d["name"] == name)
                {
                    devices.push(json!({
                        "hostId": ctx.id,
                        "id": name,
                        "name": name,
                        "platform": "android",
                        "version": "Android",
                        "booted": false,
                        "physical": false,
                    }));
                }
            }
        }
    }
    let simulators_unlisted = simulators.is_empty()
        && hub_errors
            .iter()
            .any(|(message, _)| is_simulator_list_failed(message));
    let incomplete_platforms = if simulators_unlisted {
        vec!["ios"]
    } else {
        Vec::new()
    };
    Ok(HubListResult {
        devices,
        detail: (!details.is_empty()).then(|| details.join("\n")),
        android_discovery_incomplete,
        incomplete_platforms,
    })
}

/// `refresh`: a host's listing into the state. Only this machine's sets the
/// top-level status detail.
fn apply_list_result(state: &mut Value, dir: &Path, host_id: &str, listed: HubListResult) {
    let mut merged: Vec<Value> = state["devices"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|d| {
            d["hostId"] != host_id
                || d["platform"]
                    .as_str()
                    .is_some_and(|p| listed.incomplete_platforms.contains(&p))
        })
        .cloned()
        .collect();
    for device in listed.devices {
        if device["platform"]
            .as_str()
            .is_none_or(|p| !listed.incomplete_platforms.contains(&p))
        {
            merged.push(device);
        }
    }
    state["devices"] = Value::Array(merged);
    if host_id == LOCAL_HOST_ID {
        state["hosts"] = json!([host(dir, true)]);
        state["hostStatus"] = json!("ready");
        state["hostStatusDetail"] = listed.detail.clone().map_or(Value::Null, Value::from);
    }
    let mut host_status = json!({"status": "ready"});
    if let Some(detail) = &listed.detail {
        host_status["detail"] = json!(detail);
    }
    if listed.android_discovery_incomplete {
        host_status["androidDiscoveryIncomplete"] = json!(true);
    }
    if !listed.incomplete_platforms.is_empty() {
        host_status["incompletePlatforms"] = json!(listed.incomplete_platforms);
    }
    state["hostStatuses"][host_id] = host_status;
}

/// The reference's ffmpeg check, run on the device host as its hub would
/// resolve `SERVE_EMU_FFMPEG` there.
const FFMPEG_CHECK: &str = r#"const { spawnSync } = require("node:child_process");
  const result = spawnSync(process.env.SERVE_EMU_FFMPEG?.trim() || "ffmpeg", ["-hide_banner", "-encoders"], {
    stdio: "inherit", timeout: 10000, killSignal: "SIGKILL",
  });
  process.exitCode = result.error ? 1 : (result.status ?? 1);"#;

async fn check_android_ffmpeg(ctx: &HostCtx) -> Result<(), Value> {
    let (stdout, stderr, code) = ctx.run(&ctx.node_path, &["-e", FFMPEG_CHECK], None).await;
    if code != 0 {
        return Err(json!({
            "_tag": "DevicePlatformUnavailableError",
            "hostId": ctx.id,
            "platform": "android",
            "reason": "Android emulator video needs working ffmpeg with the libx264 encoder. Install it on the device host's PATH or set SERVE_EMU_FFMPEG there, then retry opening the emulator. After changing PATH or SERVE_EMU_FFMPEG, turn device support off and on for SSH hosts, or restart the local environment server.",
        }));
    }
    let combined = format!("{stdout}\n{stderr}");
    let has_x264 = combined.lines().any(|line| {
        let trimmed = line.trim_start();
        let mut parts = trimmed.split_whitespace();
        matches!((parts.next(), parts.next()), (Some(flags), Some("libx264")) if flags.starts_with('V'))
    });
    if !has_x264 {
        return Err(json!({
            "_tag": "DevicePlatformUnavailableError",
            "hostId": ctx.id,
            "platform": "android",
            "reason": "Android emulator video needs the libx264 encoder. Install an ffmpeg build that includes libx264 on the device host, then retry opening the emulator. After changing PATH or SERVE_EMU_FFMPEG, turn device support off and on for SSH hosts, or restart the local environment server.",
        }));
    }
    Ok(())
}

fn boot_diagnostics(boot: &Value) -> Option<String> {
    let mut parts = Vec::new();
    if let Some(err) = boot["error"].as_str().filter(|s| !s.trim().is_empty()) {
        parts.push(err);
    }
    if let Some(errors) = boot["errors"].as_array() {
        for item in errors {
            if let Some(msg) = item["message"].as_str().filter(|s| !s.trim().is_empty()) {
                parts.push(msg);
            }
            if let Some(err) = item["error"].as_str().filter(|s| !s.trim().is_empty()) {
                parts.push(err);
            }
        }
    }
    (!parts.is_empty()).then(|| bound_diagnostics(&parts.join("\n\n")))
}

fn classify_boot_reason(diagnostics: Option<&str>) -> &'static str {
    let Some(text) = diagnostics else {
        return "launch_failed";
    };
    let lower = text.to_ascii_lowercase();
    let disk_space = lower.contains("no space left")
        || (lower.contains("insufficient") && (lower.contains("disk") || lower.contains("space")))
        || (lower.contains("not enough") && (lower.contains("disk") || lower.contains("space")));
    if disk_space {
        "disk_space"
    } else if lower.contains("timed out") || lower.contains("time out") || lower.contains("timeout")
    {
        "timeout"
    } else {
        "launch_failed"
    }
}

async fn shutdown(ctx: &HostCtx, platform: &str, id: &str) -> Reply {
    let origin = ctx.origin.as_str();
    let post_shutdown = |path: String, body: Value| async move {
        let res = query(
            hub_client(Duration::from_secs(15))
                .post(format!("{origin}{path}"))
                .json(&body),
            "shutdown",
        )
        .await?;
        if res["ok"] != true {
            return Err(op_err("shutdown", "hub_rejected"));
        }
        Ok(Value::Null)
    };
    if platform == "ios" {
        match post_shutdown(
            "/vendor/serve-sim/grid/api/shutdown".into(),
            json!({"udid": id}),
        )
        .await
        {
            Ok(v) => Ok(v),
            Err(cause) => match list_hub(ctx).await {
                Ok(listed)
                    if listed
                        .devices
                        .iter()
                        .find(|d| d["id"] == id)
                        .is_some_and(|d| d["booted"] == false) =>
                {
                    Ok(Value::Null)
                }
                _ => Err(cause),
            },
        }
    } else {
        post_shutdown(
            "/api/devices/shutdown".into(),
            json!({"platform": platform, "id": id}),
        )
        .await
    }
}

fn resolve_host_id(hosts: &[Value], host_id: Option<&str>) -> Result<Option<String>, Value> {
    let Some(selector) = host_id.filter(|s| !s.is_empty()) else {
        return Ok(None);
    };
    if let Some(exact) = hosts.iter().find(|h| h["id"].as_str() == Some(selector)) {
        return Ok(exact["id"].as_str().map(str::to_owned));
    }
    let selector_lower = selector.to_lowercase();
    let label_matches: Vec<&Value> = hosts
        .iter()
        .filter(|h| {
            h["label"]
                .as_str()
                .is_some_and(|l| l.to_lowercase() == selector_lower)
        })
        .collect();
    if label_matches.len() == 1 {
        return Ok(label_matches[0]["id"].as_str().map(str::to_owned));
    }
    let candidates: Vec<&Value> = if label_matches.len() > 1 {
        label_matches.clone()
    } else {
        hosts.iter().collect()
    };
    let mut parts: Vec<String> = candidates
        .iter()
        .take(10)
        .map(|h| {
            let label: String = h["label"].as_str().unwrap_or("").chars().take(64).collect();
            let id = h["id"].as_str().unwrap_or("");
            format!("{label} ({id})")
        })
        .collect();
    if candidates.len() > 10 {
        parts.push(format!("{} more", candidates.len() - 10));
    }
    let listed = parts.join(", ");
    let cut_selector: String = selector.chars().take(128).collect();
    let reason = if label_matches.len() > 1 {
        format!("Several device hosts are named {cut_selector}; pass one id: {listed}.")
    } else {
        format!(
            "No device host {cut_selector}. Known hosts: {}.",
            if listed.is_empty() { "none" } else { &listed }
        )
    };
    Err(json!({
        "_tag": "DeviceToolUnavailableError",
        "reason": reason,
    }))
}

fn host_display_name(hosts: &[Value], host_id: &str) -> String {
    match hosts
        .iter()
        .find(|h| h["id"].as_str() == Some(host_id))
        .and_then(|h| h["label"].as_str())
    {
        Some(label) => format!("'{label}' (hostId {host_id})"),
        None => format!("hostId {host_id}"),
    }
}

fn locate_session(
    state: &Value,
    thread_id: &str,
    device_id: &str,
    host_id: Option<&str>,
) -> Result<Option<Value>, Value> {
    let hosts = state["hosts"].as_array().map(Vec::as_slice).unwrap_or(&[]);
    let matches: Vec<&Value> = state["sessions"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|s| s["threadId"] == thread_id && s["deviceId"] == device_id)
        .collect();
    locate(hosts, matches, device_id, host_id)
}

impl DeviceService {
    pub fn subscribe(&self) -> broadcast::Receiver<Value> {
        self.events.subscribe()
    }
    /// The state as clients receive it.
    pub fn snapshot(&self, _dir: &Path) -> Value {
        wire(&self.state.lock().unwrap())
    }

    /// The state with every host in `hosts`, as the service and the device
    /// tools see it (`DeviceServiceState` decoded).
    fn current(&self) -> Value {
        ordered(&self.state.lock().unwrap())
    }
    fn publish(&self, state: &mut Value) {
        // This machine first, then the configured hosts, as the reference's host map orders them.
        let local = state["hosts"]
            .as_array()
            .and_then(|hosts| hosts.iter().find(|h| h["id"] == LOCAL_HOST_ID))
            .cloned();
        let mut hosts: Vec<Value> = local.into_iter().collect();
        hosts.extend(
            self.ssh_hosts
                .lock()
                .unwrap()
                .iter()
                .map(|(_, summary)| summary.clone()),
        );
        hosts.extend(self.eas_hosts.lock().unwrap().iter().map(EasState::summary));
        hosts.extend(
            self.simlock_hosts
                .lock()
                .unwrap()
                .iter()
                .map(SimlockState::summary),
        );
        state["hosts"] = Value::Array(hosts);
        state["revision"] = json!(state["revision"].as_u64().unwrap_or(0) + 1);
        *self.state.lock().unwrap() = state.clone();
        let _ = self.events.send(wire(state));
    }
    pub async fn call(&self, name: &str, input: &Value, dir: &Path) -> Option<Reply> {
        if !matches!(
            name,
            "list"
                | "configure"
                | "open"
                | "close"
                | "detail"
                | "shutdown"
                | "action"
                | "testHost"
                | "easAccount"
                | "simlockIdentity"
        ) {
            return None;
        }
        let reply = self.apply(name, input, dir).await;
        self.prune_launchers(dir).await;
        // `list` and `configure` answer with the state, as clients receive it.
        Some(if matches!(name, "list" | "configure") {
            reply.map(|state| wire(&state))
        } else {
            reply
        })
    }

    pub async fn tool_call(&self, operation: &str, input: &Value, dir: &Path) -> Reply {
        match operation {
            "list" => {
                let thread_id = input["threadId"].as_str().unwrap_or("");
                let state = self.apply("list", &json!({}), dir).await?;
                if state["hostStatus"] == "disabled" {
                    return Err(json!({
                        "_tag": "DeviceToolUnavailableError",
                        "reason": "Device support is off. Ask the user to enable it in the Device panel before installing or starting device tools.",
                    }));
                }
                let hosts = state["hosts"].as_array().map(Vec::as_slice).unwrap_or(&[]);
                let host_id = resolve_host_id(hosts, input["hostId"].as_str())?;
                let host_statuses: serde_json::Map<String, Value> = state["hostStatuses"]
                    .as_object()
                    .into_iter()
                    .flatten()
                    .filter(|(id, _)| {
                        host_id
                            .as_deref()
                            .is_none_or(|wanted| id.as_str() == wanted)
                    })
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect();
                let filtered_hosts: Vec<Value> = hosts
                    .iter()
                    .filter(|h| {
                        host_id
                            .as_deref()
                            .is_none_or(|wanted| h["id"].as_str() == Some(wanted))
                    })
                    .cloned()
                    .collect();
                let filtered_devices: Vec<Value> = state["devices"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .filter(|d| {
                        host_id
                            .as_deref()
                            .is_none_or(|wanted| d["hostId"].as_str() == Some(wanted))
                    })
                    .cloned()
                    .collect();
                let open: Vec<Value> = state["sessions"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .filter(|s| s["threadId"] == thread_id)
                    .map(|s| json!({"hostId": s["hostId"], "deviceId": s["deviceId"]}))
                    .collect();
                Ok(json!({
                    "hostStatuses": host_statuses,
                    "hosts": filtered_hosts,
                    "devices": filtered_devices,
                    "open": open,
                }))
            }
            "close" => {
                let thread_id = input["threadId"].as_str().unwrap_or("");
                let state = self.current();
                let hosts = state["hosts"].as_array().map(Vec::as_slice).unwrap_or(&[]);
                let input_host_id = match input["hostId"].as_str() {
                    Some(h) => resolve_host_id(hosts, Some(h))?,
                    None => None,
                };
                let device_id = input["deviceId"].as_str();
                let open = match device_id {
                    Some(d) => locate_session(&state, thread_id, d, input_host_id.as_deref())?,
                    None => None,
                };
                let host_id = open
                    .as_ref()
                    .and_then(|s| s["hostId"].as_str().map(str::to_owned))
                    .or(input_host_id);
                let mut close_input = json!({"threadId": thread_id});
                if let Some(h) = host_id {
                    close_input["hostId"] = json!(h);
                }
                if let Some(d) = device_id {
                    close_input["deviceId"] = json!(d);
                }
                if let Some(s) = input["shutdown"].as_bool() {
                    close_input["shutdown"] = json!(s);
                }
                let closed = self.apply("close", &close_input, dir).await;
                self.prune_launchers(dir).await;
                closed?;
                Ok(json!({}))
            }
            "open" => self.tool_open(input, dir).await,
            "screenshot" => self.tool_screenshot(input, dir).await,
            _ => Err(json!({
                "_tag": "DeviceToolUnavailableError",
                "reason": format!("Unknown device tool operation: {operation}"),
            })),
        }
    }

    async fn apply(&self, name: &str, input: &Value, dir: &Path) -> Reply {
        match name {
            "simlockIdentity" => {
                let settings = self.settings.internal().map_err(|_| json!({"_tag":"DeviceSimlockIdentityError","reason":"Could not read the saved token."}))?;
                return simlock(input, &settings, &self.settings.resolver).await;
            }
            "easAccount" => {
                let settings = self.settings.internal().map_err(|_| json!({"_tag":"DeviceEasAccountError","reason":"Could not read the saved token."}))?;
                return expo(input, &settings, &self.settings.resolver).await;
            }
            "testHost" => return test_host(input, dir).await,
            _ => {}
        }
        if matches!(name, "open" | "shutdown")
            && input["hostId"]
                .as_str()
                .is_some_and(|id| self.simlock_config(id).is_some())
        {
            return self.simlock_apply(name, input, dir).await;
        }
        if matches!(name, "open" | "shutdown")
            && input["hostId"]
                .as_str()
                .is_some_and(|id| self.eas_config(id).is_some())
        {
            return self.eas_apply(name, input, dir).await;
        }
        let mut runtime = self.runtime.lock().await;
        let mut state = self.current();
        let host_id = input["hostId"].as_str().unwrap_or(LOCAL_HOST_ID);
        if name != "close"
            && host_id != LOCAL_HOST_ID
            && !(matches!(name, "open" | "shutdown") && self.ssh_config(host_id).is_some())
        {
            return Err(bad_host(host_id));
        }
        let preferences = self
            .settings
            .internal()
            .map_err(|_| op_err("settings", "settings_failed"))?;
        let mut enabled = preferences["enableDeviceSupport"] == true;
        if name == "configure" {
            let mut patch = json!({});
            for (key, field) in DEVICE_SETTINGS {
                if let Some(v) = input.get(key) {
                    patch[field] = json!(v
                        .as_bool()
                        .ok_or_else(|| invalid(key, "Expected boolean"))?);
                }
            }
            let settings = self.settings.update(&json!({"patch":patch}), true)
                .map_err(|cause| json!({"_tag":"DeviceOperationError","operation":"configure","reason":"settings_failed","cause":cause}))?;
            enabled = settings["enableDeviceSupport"] == true;
            state["hostStatus"] = json!(if enabled { "idle" } else { "disabled" });
            state["hostStatusDetail"] = Value::Null;
            state["hostStatuses"] = json!({});
            state["agentAccessEnabled"] = settings["enableAgentDeviceAccess"].clone();
            state["onboardingCompleted"] = settings["deviceOnboardingCompleted"].clone();
            if !enabled || input["agentAccessEnabled"] == false {
                self.stop_agent_locked(dir).await;
            }
            if enabled && input["agentAccessEnabled"] == false {
                let hosts: Vec<SshConfig> = self
                    .ssh_hosts
                    .lock()
                    .unwrap()
                    .iter()
                    .map(|(config, _)| config.clone())
                    .collect();
                for config in &hosts {
                    self.ssh_stop_agent(config, dir).await;
                }
            }
            if !enabled {
                let hosts: Vec<SshConfig> = self
                    .ssh_hosts
                    .lock()
                    .unwrap()
                    .iter()
                    .map(|(config, _)| config.clone())
                    .collect();
                for config in &hosts {
                    self.ssh_stop(config, dir).await;
                }
                let simlock_hosts: Vec<SimlockConfig> = self
                    .simlock_hosts
                    .lock()
                    .unwrap()
                    .iter()
                    .map(|host| host.config.clone())
                    .collect();
                for config in simlock_hosts {
                    self.simlock_release_all(config).await;
                }
                let eas_hosts: Vec<EasConfig> = self
                    .eas_hosts
                    .lock()
                    .unwrap()
                    .iter()
                    .map(|host| host.config.clone())
                    .collect();
                for config in eas_hosts {
                    self.eas_stop_all(&config, dir).await;
                }
                runtime.take();
                state["sessions"] = json!([]);
                state["devices"] = json!([]);
                state["bootingDevices"] = json!([]);
            } else if state.get("bootingDevices").is_none() {
                state["bootingDevices"] = Value::Null;
            }
            self.publish(&mut state);
            if enabled
                && settings["enableAgentDeviceAccess"] == true
                && input["agentAccessEnabled"] == true
            {
                self.agent_readiness_locked(&mut runtime, dir).await?;
                state = self.current();
            }
        }
        if name == "list" || name == "configure" {
            if let Some(id) = input["retryHostId"].as_str() {
                if id != LOCAL_HOST_ID {
                    if self.simlock_config(id).is_some() {
                        drop(runtime);
                        if enabled {
                            self.list_simlock_host(id, dir).await;
                        }
                        return Ok(self.current());
                    }
                    if self.ssh_config(id).is_none() {
                        return Err(bad_host(id));
                    }
                    // `retryHost`: only that host, made ready and listed again.
                    drop(runtime);
                    if enabled {
                        self.retry_ssh_host(id, dir).await;
                    }
                    return Ok(self.current());
                }
            }
            if let Some(tool) = input["updateTool"].as_str() {
                install(dir, if tool == "hub" { HUB } else { AGENT }).await?;
            }
            // Keep settings-backed fields fresh if settings changed outside `device.configure`.
            if name == "list" {
                let mut changed = false;
                let desired_agent = json!(preferences["enableAgentDeviceAccess"] == true);
                let desired_onboarding = json!(preferences["deviceOnboardingCompleted"] == true);
                if state["agentAccessEnabled"] != desired_agent {
                    state["agentAccessEnabled"] = desired_agent;
                    changed = true;
                }
                if state["onboardingCompleted"] != desired_onboarding {
                    state["onboardingCompleted"] = desired_onboarding;
                    changed = true;
                }
                if !enabled && state["hostStatus"] != "disabled" {
                    runtime.take();
                    state["hostStatus"] = json!("disabled");
                    state["hostStatusDetail"] = Value::Null;
                    state["hostStatuses"] = json!({});
                    state["devices"] = json!([]);
                    state["sessions"] = json!([]);
                    changed = true;
                } else if enabled && state["hostStatus"] == "disabled" {
                    state["hostStatus"] = json!("idle");
                    changed = true;
                }
                if changed {
                    self.publish(&mut state);
                }
            }
            let fresh_host = host(dir, running_origin(&mut runtime).is_some());
            if input["inspectOnly"] == true || input.get("updateTool").is_some() {
                state["hosts"] = json!([fresh_host.clone()]);
                self.publish(&mut state);
            }
            if input["inspectOnly"] == true && input.get("updateTool").is_none() {
                drop(runtime);
                self.inspect_ssh_hosts(dir).await;
                return Ok(self.current());
            }
            if enabled
                && input["inspectOnly"] != true
                && fresh_host["platforms"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|p| p["available"] == true)
            {
                match ensure_hub(&mut runtime, dir).await {
                    Ok(origin) => match list_hub(&HostCtx::local(&origin)).await {
                        Ok(listed) => {
                            apply_list_result(&mut state, dir, LOCAL_HOST_ID, listed);
                            self.publish(&mut state);
                        }
                        Err(error) => {
                            let detail = error_detail(&error);
                            state["hostStatus"] = json!("failed");
                            state["hostStatusDetail"] = json!(detail);
                            state["hostStatuses"][LOCAL_HOST_ID] =
                                json!({"status": "failed", "detail": detail});
                            self.publish(&mut state);
                        }
                    },
                    Err(error) => {
                        let detail = error_detail(&error);
                        state["hostStatus"] = json!("failed");
                        state["hostStatusDetail"] = json!(detail);
                        state["hostStatuses"][LOCAL_HOST_ID] =
                            json!({"status": "failed", "detail": detail});
                        self.publish(&mut state);
                    }
                }
            }
            if enabled && input["inspectOnly"] != true {
                drop(runtime);
                self.list_ssh_hosts(dir).await;
                let eas_hosts: Vec<String> = self
                    .eas_hosts
                    .lock()
                    .unwrap()
                    .iter()
                    .map(|host| host.config.id.clone())
                    .collect();
                for id in eas_hosts {
                    self.list_eas_host(&id, dir).await;
                }
                let simlock_hosts: Vec<String> = self
                    .simlock_hosts
                    .lock()
                    .unwrap()
                    .iter()
                    .map(|host| host.config.id.clone())
                    .collect();
                for id in simlock_hosts {
                    self.list_simlock_host(&id, dir).await;
                }
                return Ok(self.current());
            }
            return Ok(ordered(&state));
        }
        if name == "close" {
            let thread = string(input, "threadId")?;
            let id = input["deviceId"].as_str();
            let do_shutdown = input["shutdown"] == true;
            let closing: Vec<Value> = state["sessions"]
                .as_array()
                .unwrap()
                .iter()
                .filter(|s| {
                    s["threadId"] == thread
                        && input["hostId"].as_str().is_none_or(|h| s["hostId"] == h)
                        && id.is_none_or(|id| s["deviceId"] == id)
                })
                .cloned()
                .collect();
            let ending: Vec<Value> = if do_shutdown {
                state["sessions"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .filter(|s| {
                        closing
                            .iter()
                            .any(|c| c["hostId"] == s["hostId"] && c["deviceId"] == s["deviceId"])
                    })
                    .cloned()
                    .collect()
            } else {
                closing.clone()
            };
            // A lost SSH tunnel does not establish that the remote daemon stopped.
            for s in &ending {
                let session_host = s["hostId"].as_str().unwrap_or(LOCAL_HOST_ID);
                if self.ssh_config(session_host).is_some()
                    && self.ssh_current(session_host).await.is_none()
                {
                    return Err(json!({
                        "_tag": "DeviceHostUnavailableError",
                        "hostId": session_host,
                        "hostLabel": self.host_label(session_host),
                        "reason": "Device host is reconnecting. Retry close after it reconnects.",
                    }));
                }
            }
            for s in ending.iter().filter(|s| s["hostId"] != LOCAL_HOST_ID) {
                let session_host = s["hostId"].as_str().unwrap_or_default();
                // Each Expo session runs its own daemon, so one that has ended holds no agent sessions.
                let eas_endpoint = self
                    .with_eas(session_host, |host| {
                        host.sessions
                            .get(s["deviceId"].as_str().unwrap_or_default())
                            .map(|session| AgentEndpoint {
                                base_url: session.agent_url.clone(),
                                token: session.agent_token.clone(),
                                entry: PathBuf::new(),
                                state_dir: None,
                            })
                    })
                    .flatten();
                let endpoint = match eas_endpoint {
                    Some(endpoint) => Some(endpoint),
                    None => self.ssh_agent_endpoint(session_host).await,
                };
                if let Some(endpoint) = endpoint {
                    let session = agent_device::session_name(
                        s["threadId"].as_str().unwrap_or(""),
                        session_host,
                        s["deviceId"].as_str().unwrap_or(""),
                    );
                    if agent_device::release_session(&endpoint, &session)
                        .await
                        .is_err()
                    {
                        return Err(op_err("close", "request_failed"));
                    }
                }
            }
            // Open publishes under the same lock; an old close must not release a reopened claim.
            let agent = self.agent.lock().unwrap().clone();
            if let (Some(endpoint), true) = (agent, runtime.is_some()) {
                for s in ending.iter().filter(|s| s["hostId"] == LOCAL_HOST_ID) {
                    let session = agent_device::session_name(
                        s["threadId"].as_str().unwrap_or(""),
                        s["hostId"].as_str().unwrap_or(""),
                        s["deviceId"].as_str().unwrap_or(""),
                    );
                    if agent_device::release_session(&endpoint, &session)
                        .await
                        .is_err()
                    {
                        return Err(op_err("close", "request_failed"));
                    }
                }
            }
            if do_shutdown {
                self.release_launchers(&ending).await;
            }
            if do_shutdown {
                for s in &closing {
                    let session_host = s["hostId"].as_str().unwrap_or(LOCAL_HOST_ID);
                    let dev_id = s["deviceId"].as_str().unwrap_or("");
                    if let Some(config) = self.eas_config(session_host) {
                        if self.with_eas(session_host, |host| host.ready) != Some(true) {
                            return Err(json!({
                                "_tag": "DeviceHostUnavailableError",
                                "hostId": session_host,
                                "reason": "Device host is not running. Reconnect before retrying close.",
                            }));
                        }
                        self.eas_shutdown(&config, dir, dev_id).await?;
                        if let Some(devices) = state["devices"].as_array_mut() {
                            for d in devices.iter_mut() {
                                if d["hostId"] == s["hostId"] && d["id"] == dev_id {
                                    d["booted"] = json!(false);
                                }
                            }
                        }
                        self.publish(&mut state);
                        continue;
                    }
                    if self.simlock_config(session_host).is_some() {
                        // A Simlock host's readiness is its CLI; its shutdown returns the lease.
                        if self.with_simlock(session_host, |host| host.cli.is_some()) != Some(true)
                        {
                            return Err(json!({
                                "_tag": "DeviceHostUnavailableError",
                                "hostId": session_host,
                                "reason": "Device host is not running. Reconnect before retrying close.",
                            }));
                        }
                        self.simlock_shutdown(session_host, dev_id).await;
                        if let Some(devices) = state["devices"].as_array_mut() {
                            for d in devices.iter_mut() {
                                if d["hostId"] == s["hostId"] && d["id"] == dev_id {
                                    d["booted"] = json!(false);
                                }
                            }
                        }
                        self.publish(&mut state);
                        continue;
                    }
                    let current = if session_host == LOCAL_HOST_ID {
                        running_origin(&mut runtime).map(|origin| HostCtx::local(&origin))
                    } else {
                        self.ssh_current(session_host).await
                    };
                    let Some(ctx) = current else {
                        return Err(json!({
                            "_tag": "DeviceHostUnavailableError",
                            "hostId": session_host,
                            "reason": "Device host is not running. Reconnect before retrying close.",
                        }));
                    };
                    let dev_id = s["deviceId"].as_str().unwrap_or("");
                    let platform = s["platform"].as_str().unwrap_or("");
                    shutdown(&ctx, platform, dev_id).await?;
                    if let Some(devices) = state["devices"].as_array_mut() {
                        for d in devices.iter_mut() {
                            if d["hostId"] == s["hostId"] && d["id"] == dev_id {
                                d["booted"] = json!(false);
                            }
                        }
                    }
                    self.publish(&mut state);
                }
            }
            state["sessions"]
                .as_array_mut()
                .unwrap()
                .retain(|s| !ending.iter().any(|e| e == s));
            self.publish(&mut state);
            if do_shutdown {
                for s in &closing {
                    let session_host = s["hostId"].as_str().unwrap_or(LOCAL_HOST_ID);
                    if self.simlock_config(session_host).is_some() {
                        let _ = self.simlock_refresh(session_host, dir).await;
                        continue;
                    }
                    if let Some(config) = self.eas_config(session_host) {
                        self.eas_refresh(&config, dir).await;
                        continue;
                    }
                    let current = if session_host == LOCAL_HOST_ID {
                        running_origin(&mut runtime).map(|origin| HostCtx::local(&origin))
                    } else {
                        self.ssh_current(session_host).await
                    };
                    if let Some(ctx) = current {
                        if let Ok(listed) = list_hub(&ctx).await {
                            state = self.current();
                            apply_list_result(&mut state, dir, &ctx.id, listed);
                            self.publish(&mut state);
                        }
                    }
                }
            }
            return Ok(Value::Null);
        }
        let id = string(input, "deviceId")?;
        if name == "open" {
            let p = string(input, "platform")?;
            let a = if host_id == LOCAL_HOST_ID {
                availability(p)
            } else {
                self.ssh_platform(host_id, p, dir).await
            };
            if a["available"] == false {
                return Err(
                    json!({"_tag":"DevicePlatformUnavailableError","hostId":host_id,"platform":p,"reason":a["reason"]}),
                );
            }
        }
        if !enabled {
            return Err(self.disabled_for(host_id));
        }
        let ctx = if host_id == LOCAL_HOST_ID {
            HostCtx::local(&ensure_hub(&mut runtime, dir).await?)
        } else {
            let ctx = self.ssh_readiness(host_id, dir).await?;
            // Readiness published the host's status.
            state = self.current();
            ctx
        };
        let origin = ctx.origin.clone();
        if name == "shutdown" {
            let result = shutdown(&ctx, string(input, "platform")?, id).await?;
            state["sessions"]
                .as_array_mut()
                .unwrap()
                .retain(|s| s["hostId"] != host_id || s["deviceId"] != id);
            if let Ok(listed) = list_hub(&ctx).await {
                apply_list_result(&mut state, dir, &ctx.id, listed);
            }
            self.publish(&mut state);
            return Ok(result);
        }
        let listed = list_hub(&ctx).await?;
        apply_list_result(&mut state, dir, &ctx.id, listed);
        self.publish(&mut state);
        let device = state["devices"]
            .as_array()
            .and_then(|devices| {
                devices
                    .iter()
                    .find(|d| d["hostId"] == host_id && d["id"] == id)
            })
            .cloned()
            .ok_or_else(|| json!({"_tag":"DeviceNotFoundError","hostId":host_id,"deviceId":id}))?;
        if name == "open" {
            let thread = string(input, "threadId")?;
            if device["platform"] == "android" && device["physical"] != true {
                check_android_ffmpeg(&ctx).await?;
            }
            let mut active_device = device.clone();
            if device["booted"] != true && input["boot"] != false {
                let mut booting = device.clone();
                booting["threadId"] = json!(thread);
                let mut booting_list: Vec<Value> = state
                    .get("bootingDevices")
                    .and_then(Value::as_array)
                    .cloned()
                    .unwrap_or_default();
                booting_list
                    .retain(|e| e["hostId"] != booting["hostId"] || e["id"] != booting["id"]);
                booting_list.push(booting.clone());
                state["bootingDevices"] = Value::Array(booting_list);
                self.publish(&mut state);

                let boot_res = query(
                    hub_client(BOOT_TIMEOUT)
                        .post(format!("{origin}/api/devices/boot"))
                        .json(&json!({
                            "platform": device["platform"],
                            "id": id,
                            "name": device["name"],
                        })),
                    "boot",
                )
                .await;

                if let Some(list) = state
                    .get_mut("bootingDevices")
                    .and_then(Value::as_array_mut)
                {
                    list.retain(|e| e["hostId"] != booting["hostId"] || e["id"] != booting["id"]);
                }
                self.publish(&mut state);

                let boot = boot_res?;
                if boot["ok"] != true {
                    let diagnostics = boot_diagnostics(&boot);
                    let reason = classify_boot_reason(diagnostics.as_deref());
                    let mut err = json!({
                        "_tag": "DeviceBootError",
                        "hostId": host_id,
                        "deviceId": id,
                        "reason": reason,
                        "cause": boot,
                    });
                    if let Some(d) = diagnostics {
                        err["diagnostics"] = json!(d);
                    }
                    return Err(err);
                }
                if device["platform"] == "ios" {
                    query(
                        hub_client(BOOT_TIMEOUT)
                            .post(format!("{origin}/vendor/serve-sim/grid/api/start"))
                            .json(&json!({"udid": id})),
                        "attach stream",
                    )
                    .await?;
                }
                let booted_id = boot["serial"]
                    .as_str()
                    .or(boot["id"].as_str())
                    .unwrap_or(id)
                    .to_owned();
                let refreshed = list_hub(&ctx).await?;
                let platform_incomplete = device["platform"]
                    .as_str()
                    .is_some_and(|p| refreshed.incomplete_platforms.contains(&p));
                apply_list_result(&mut state, dir, &ctx.id, refreshed);
                self.publish(&mut state);
                active_device = if platform_incomplete {
                    None
                } else {
                    state["devices"]
                        .as_array()
                        .and_then(|ds| {
                            ds.iter()
                                .find(|d| d["hostId"] == host_id && d["id"] == booted_id)
                                .or_else(|| {
                                    ds.iter().find(|d| d["hostId"] == host_id && d["id"] == id)
                                })
                        })
                        .cloned()
                }
                .ok_or_else(
                    || json!({"_tag":"DeviceNotFoundError","hostId":host_id,"deviceId":booted_id}),
                )?;
            } else if device["booted"] == true && device["platform"] == "ios" {
                query(
                    hub_client(BOOT_TIMEOUT)
                        .post(format!("{origin}/vendor/serve-sim/grid/api/start"))
                        .json(&json!({"udid": id})),
                    "attach stream",
                )
                .await?;
            }
            if self
                .settings
                .internal()
                .map_err(|_| op_err("settings", "settings_failed"))?["enableDeviceSupport"]
                != true
            {
                return Err(json!({
                    "_tag": "DeviceHostUnavailableError",
                    "hostId": host_id,
                    "hostLabel": self.host_label(host_id),
                    "reason": "Device support was turned off while the device was opening.",
                }));
            }
            let active_id = active_device["id"].as_str().unwrap_or(id);
            let session = json!({
                "threadId": thread,
                "hostId": host_id,
                "deviceId": active_id,
                "platform": active_device["platform"],
                "openedAt": timestamp(),
            });
            let sessions = state["sessions"].as_array_mut().unwrap();
            sessions.retain(|s| {
                s["threadId"] != thread || s["hostId"] != host_id || s["deviceId"] != active_id
            });
            sessions.push(session.clone());
            self.publish(&mut state);
            return Ok(session);
        }
        super::device_actions::handle(
            name,
            &device,
            input,
            &super::base_directory(dir)
                .join("tools")
                .join(HUB.0)
                .join(HUB.1),
        )
        .await
    }
}

const AGENT_ACCESS_UNAVAILABLE: &str = "Agent device access requires enabled device support, agent access, and an available simulator platform on this host.";
const SCREENSHOT_TIMEOUT: Duration = Duration::from_secs(20);
const PNG_SIGNATURE: [u8; 8] = [137, 80, 78, 71, 13, 10, 26, 10];
// The hub passes numeric display IDs that Xcode 26 rejects; iOS 27 needs them for multiple displays.
const DEFAULT_DISPLAY_SCREENSHOT: &str = "f=$(mktemp -t t3-screenshot) || exit 1\ntrap 'rm -f \"$f\"' 0\nxcrun simctl io \"$1\" screenshot --type=png \"$f\" >/dev/null && base64 -i \"$f\"";

fn tool_unavailable(reason: impl Into<String>) -> Value {
    json!({"_tag": "DeviceToolUnavailableError", "reason": reason.into()})
}

fn local_unavailable(reason: &str) -> Value {
    json!({
        "_tag": "DeviceHostUnavailableError",
        "hostId": LOCAL_HOST_ID,
        "hostLabel": LOCAL_HOST_LABEL,
        "reason": reason,
    })
}

/// `locate`: the one match on `host_id` (or on its only host), else the
/// reference's ambiguous-host and other-host errors.
fn locate(
    hosts: &[Value],
    matches: Vec<&Value>,
    device_id: &str,
    host_id: Option<&str>,
) -> Result<Option<Value>, Value> {
    let on_host: Vec<&Value> = matches
        .iter()
        .copied()
        .filter(|s| host_id.is_none_or(|h| s["hostId"] == h))
        .collect();
    let mut host_ids: Vec<&str> = Vec::new();
    for s in &on_host {
        if let Some(id) = s["hostId"].as_str() {
            if !host_ids.contains(&id) {
                host_ids.push(id);
            }
        }
    }
    if host_ids.len() == 1 {
        return Ok(on_host.last().copied().cloned());
    }
    if host_ids.len() > 1 {
        let names = host_ids
            .iter()
            .map(|id| host_display_name(hosts, id))
            .collect::<Vec<_>>()
            .join(", ");
        return Err(tool_unavailable(format!(
            "Device {device_id} is on more than one host: {names}. Pass hostId."
        )));
    }
    let mut elsewhere: Vec<&str> = Vec::new();
    for s in &matches {
        if let Some(id) = s["hostId"].as_str() {
            if !elsewhere.contains(&id) {
                elsewhere.push(id);
            }
        }
    }
    if let (Some(wanted), false) = (host_id, elsewhere.is_empty()) {
        let names = elsewhere
            .iter()
            .map(|id| host_display_name(hosts, id))
            .collect::<Vec<_>>()
            .join(", ");
        return Err(tool_unavailable(format!(
            "Device {device_id} is not on {}. It is on {names}; pass that hostId.",
            host_display_name(hosts, wanted)
        )));
    }
    Ok(None)
}

/// `hostName` in the device handlers.
fn handler_host_name(hosts: &[Value], host_id: &str) -> String {
    host_display_name(hosts, host_id)
}

/// `failedHosts`' detail for `host_id`.
fn failed_detail<'a>(state: &'a Value, host_id: &str) -> Option<&'a str> {
    let status = &state["hostStatuses"][host_id];
    if status["status"] == "failed" {
        status["detail"].as_str().filter(|d| !d.is_empty())
    } else {
        None
    }
}

/// `pickDevice`: the device `device_open` opens.
fn pick_device(
    state: &Value,
    device_id: Option<&str>,
    platform: Option<&str>,
    host_id: Option<&str>,
) -> Result<Value, Value> {
    let hosts = state["hosts"].as_array().map(Vec::as_slice).unwrap_or(&[]);
    let devices: Vec<&Value> = state["devices"].as_array().into_iter().flatten().collect();
    if let Some(device_id) = device_id {
        let matches = devices
            .iter()
            .copied()
            .filter(|d| d["id"] == device_id)
            .collect();
        if let Some(found) = locate(hosts, matches, device_id, host_id)? {
            return Ok(found);
        }
        let mut reason = vec![match host_id {
            None => format!("No device {device_id} on any host."),
            Some(h) => format!("No device {device_id} on {}.", handler_host_name(hosts, h)),
        }];
        let candidates: Vec<String> = match host_id {
            Some(h) => vec![h.to_owned()],
            None => hosts
                .iter()
                .filter_map(|h| h["id"].as_str().map(str::to_owned))
                .collect(),
        };
        for candidate in candidates {
            if let Some(detail) = failed_detail(state, &candidate) {
                reason.push(format!(
                    "{} failed to start: {detail}",
                    handler_host_name(hosts, &candidate)
                ));
            }
        }
        reason.push("Call device_list for current ids.".to_owned());
        return Err(tool_unavailable(reason.join(" ")));
    }
    let host_id = host_id.unwrap_or(LOCAL_HOST_ID);
    let candidates: Vec<&Value> = devices
        .into_iter()
        .filter(|d| d["hostId"] == host_id && platform.is_none_or(|p| d["platform"] == p))
        .collect();
    if candidates.is_empty() {
        return Err(tool_unavailable(
            match (failed_detail(state, host_id), platform) {
                (Some(detail), _) => detail.to_owned(),
                (None, None) => {
                    "No simulators or emulators were found. Call device_list to see why.".to_owned()
                }
                (None, Some(p)) => format!(
                    "No {p} devices were found on host {host_id}. Call device_list to see why."
                ),
            },
        ));
    }
    let first_platform = &candidates[0]["platform"];
    if platform.is_none() && candidates.iter().any(|d| &d["platform"] != first_platform) {
        return Err(tool_unavailable(
            "Both iOS and Android devices are available; pass platform or deviceId.",
        ));
    }
    Ok(candidates
        .iter()
        .find(|d| d["booted"] == true)
        .copied()
        .unwrap_or(candidates[0])
        .clone())
}

/// `extractPng`: the first complete PNG in `bytes`.
pub(super) fn extract_png(bytes: &[u8]) -> Option<&[u8]> {
    let start = bytes.windows(8).position(|w| w == PNG_SIGNATURE)?;
    let mut chunk = start + 8;
    while chunk + 12 <= bytes.len() {
        let length = u32::from_be_bytes(bytes[chunk..chunk + 4].try_into().ok()?) as usize;
        let end = chunk + 12 + length;
        if end > bytes.len() {
            return None;
        }
        if &bytes[chunk + 4..chunk + 8] == b"IEND" {
            return Some(&bytes[start..end]);
        }
        chunk = end;
    }
    None
}

/// `pngDimensions`.
pub(super) fn png_dimensions(png: &[u8]) -> (u32, u32) {
    let word = |at: usize| u32::from_be_bytes(png[at..at + 4].try_into().unwrap());
    if png.len() < 24 || word(0) != 0x8950_4e47 || word(4) != 0x0d0a_1a0a || word(12) != 0x4948_4452
    {
        return (0, 0);
    }
    (word(16), word(20))
}

fn vendor_prefix(platform: &str) -> &'static str {
    if platform == "ios" {
        "/vendor/serve-sim"
    } else {
        "/vendor/serve-emu"
    }
}

fn runs_on_xcode26_runtime(version: &str) -> bool {
    // `Number(undefined) <= 26` is false.
    version
        .strip_prefix("iOS ")
        .map(|rest| {
            rest.chars()
                .take_while(char::is_ascii_digit)
                .collect::<String>()
        })
        .filter(|digits| !digits.is_empty())
        .and_then(|digits| digits.parse::<u64>().ok())
        .is_some_and(|major| major <= 26)
}

/// `DeviceHubOperations.screenshot`.
async fn hub_screenshot(ctx: &HostCtx, device: &Value) -> Result<Vec<u8>, Value> {
    let origin = &ctx.origin;
    let platform = device["platform"].as_str().unwrap_or("");
    let id = device["id"].as_str().unwrap_or("");
    let url = format!(
        "{origin}{}/api/screenshot?device={}",
        vendor_prefix(platform),
        agent_device::encode_uri_component(id)
    );
    let reason = match hub_client(SCREENSHOT_TIMEOUT).post(url).send().await {
        Ok(response) if response.status().is_success() => match response.bytes().await {
            Ok(bytes) => return Ok(bytes.to_vec()),
            Err(_) => "request_failed",
        },
        Ok(_) => "hub_rejected",
        Err(_) => "request_failed",
    };
    if reason == "hub_rejected"
        && platform == "ios"
        && runs_on_xcode26_runtime(device["version"].as_str().unwrap_or(""))
    {
        return default_display_screenshot(ctx, id).await;
    }
    Err(op_err("screenshot", reason))
}

async fn default_display_screenshot(ctx: &HostCtx, udid: &str) -> Result<Vec<u8>, Value> {
    use base64::Engine as _;
    let (stdout, stderr, code) = ctx
        .run(
            "sh",
            &["-c", DEFAULT_DISPLAY_SCREENSHOT, "sh", udid],
            Some(SCREENSHOT_TIMEOUT),
        )
        .await;
    if code != 0 {
        return Err(json!({
            "_tag": "DeviceOperationError",
            "operation": "screenshot",
            "reason": "command_failed",
            "exitCode": code,
            "cause": stderr.trim(),
        }));
    }
    let text: String = stdout.chars().filter(|c| !c.is_whitespace()).collect();
    Ok(base64::engine::general_purpose::STANDARD
        .decode(text)
        .unwrap_or_default())
}

impl DeviceService {
    fn sessions_key(&self) -> HashSet<String> {
        self.state.lock().unwrap()["sessions"]
            .as_array()
            .into_iter()
            .flatten()
            .map(|s| {
                agent_device::session_name(
                    s["threadId"].as_str().unwrap_or(""),
                    s["hostId"].as_str().unwrap_or(""),
                    s["deviceId"].as_str().unwrap_or(""),
                )
            })
            .collect()
    }

    /// `pruneLaunchers`: ends the agent-device sessions of retired device
    /// sessions, then removes their launchers.
    pub(super) async fn prune_launchers(&self, dir: &Path) {
        let mut launchers = self.launchers.lock().await;
        let live = self.sessions_key();
        let retired: Vec<String> = launchers
            .keys()
            .filter(|session| !live.contains(*session))
            .cloned()
            .collect();
        let mut ending = Vec::new();
        for session in retired {
            if let Some(launcher) = launchers.remove(&session) {
                // A session the host leases ends with the host's own release, not here.
                if !self.host_session_launchers.lock().unwrap().remove(&session) {
                    ending.push(launcher);
                }
            }
        }
        futures_util::future::join_all(ending.iter().map(|l| agent_device::release_launcher(l)))
            .await;
        agent_device::prune_launchers(dir, &live).await;
    }

    /// `releaseLaunchers`: ends the agent-device sessions behind `sessions`' launchers.
    async fn release_launchers(&self, sessions: &[Value]) {
        let mut launchers = self.launchers.lock().await;
        for session in sessions {
            let name = agent_device::session_name(
                session["threadId"].as_str().unwrap_or(""),
                session["hostId"].as_str().unwrap_or(""),
                session["deviceId"].as_str().unwrap_or(""),
            );
            if self.host_session_launchers.lock().unwrap().contains(&name) {
                continue;
            }
            if let Some(launcher) = launchers.get(&name).cloned() {
                agent_device::release_launcher(&launcher).await;
                launchers.remove(&name);
            }
        }
    }

    /// `setHostStatus` for the local host.
    fn set_host_status(&self, status: &str, detail: Option<String>) {
        self.set_host_status_for(LOCAL_HOST_ID, status, detail)
    }

    /// `setHostStatus`: a host's status; this machine's is also the top-level one.
    fn set_host_status_for(&self, host_id: &str, status: &str, detail: Option<String>) {
        let mut state = self.current();
        let previous = state["hostStatuses"][host_id].clone();
        let mut next = json!({});
        if previous["androidDiscoveryIncomplete"] == true {
            next["androidDiscoveryIncomplete"] = json!(true);
        }
        if !previous["incompletePlatforms"].is_null() {
            next["incompletePlatforms"] = previous["incompletePlatforms"].clone();
        }
        next["status"] = json!(status);
        if let Some(detail) = &detail {
            next["detail"] = json!(detail);
        }
        if host_id == LOCAL_HOST_ID {
            state["hostStatus"] = json!(status);
            state["hostStatusDetail"] = detail.map_or(Value::Null, Value::from);
        }
        state["hostStatuses"][host_id] = next;
        self.publish(&mut state);
    }

    /// `agentReadinessIfSupported`: the agent-device daemon, started if
    /// needed, or `None` when agent device access is off here.
    pub(super) async fn agent_readiness(&self, dir: &Path) -> Result<Option<AgentEndpoint>, Value> {
        let mut runtime = self.runtime.lock().await;
        self.agent_readiness_locked(&mut runtime, dir).await
    }

    /// `agentReadinessIfSupported` with the host's start lock held.
    async fn agent_readiness_locked(
        &self,
        runtime: &mut Option<HubRuntime>,
        dir: &Path,
    ) -> Result<Option<AgentEndpoint>, Value> {
        let preferences = self
            .settings
            .internal()
            .map_err(|_| op_err("settings", "settings_failed"))?;
        if preferences["enableDeviceSupport"] != true
            || preferences["enableAgentDeviceAccess"] != true
        {
            return Ok(None);
        }
        let platforms = host(dir, false)["platforms"].clone();
        if !platforms
            .as_array()
            .is_some_and(|p| p.iter().any(|p| p["available"] == true))
        {
            return Ok(None);
        }
        let started: Result<AgentEndpoint, Value> = async {
            ensure_hub(runtime, dir).await?;
            let existing = self.agent.lock().unwrap().clone();
            let entry = match existing {
                Some(endpoint) => endpoint.entry,
                None => install(dir, AGENT).await.map_err(|err| {
                    if err["reason"] == "package_manager_missing" {
                        host_err(
                            "installing agent tools",
                            Some("Device support needs npm, which is not on the T3 server's PATH. Install npm (it comes with Node.js from nodejs.org), restart T3 Code, then retry.".into()),
                        )
                    } else {
                        host_err("installing agent tools", None)
                    }
                })?,
            };
            let Some(node) = agent_device::node_executable() else {
                return Err(local_unavailable(
                    "Local device support requires Node.js. Install Node.js and make sure node is on PATH, then retry.",
                ));
            };
            let env = device_host_env(sdk_root().as_deref());
            agent_device::start_daemon(dir, &entry, &node, &env)
                .await
                .map_err(|error| match error {
                    StartError::Host(step) => host_err(step, None),
                    StartError::Timeout => local_unavailable(&format!(
                        "Agent tools did not start within {} ms.",
                        agent_device::DAEMON_READY_TIMEOUT.as_millis()
                    )),
                    StartError::Start(message) => local_unavailable(&message),
                })
        }
        .await;
        match started {
            Ok(endpoint) => {
                *self.agent.lock().unwrap() = Some(endpoint.clone());
                let mut state = self.current();
                state["hosts"] = json!([host(dir, true)]);
                self.publish(&mut state);
                self.set_host_status("ready", None);
                Ok(Some(endpoint))
            }
            Err(error) => {
                self.set_host_status("failed", Some(error_detail(&error)));
                Err(error)
            }
        }
    }

    /// `stopAgent`: stops this machine's agent-device daemon.
    async fn stop_agent_locked(&self, dir: &Path) {
        let base = super::base_directory(dir);
        agent_device::stop_daemon(dir, |version| {
            [version, Some(AGENT.1)]
                .into_iter()
                .flatten()
                .map(|v| {
                    base.join("tools")
                        .join(AGENT.0)
                        .join(v)
                        .join("node_modules")
                        .join(AGENT.0)
                        .join(AGENT.2)
                })
                .find(|entry| entry.is_file())
        })
        .await;
        self.agent.lock().unwrap().take();
    }

    /// `device_open` (the reference's handler, `agentTarget` and `agentLauncher`).
    pub(super) async fn tool_open(&self, input: &Value, dir: &Path) -> Reply {
        let thread_id = input["threadId"].as_str().unwrap_or("").to_owned();
        let state = self.apply("list", &json!({}), dir).await?;
        self.prune_launchers(dir).await;
        if state["hostStatus"] == "disabled" {
            return Err(tool_unavailable(
                "Device support is off. Ask the user to enable it in the Device panel before installing or starting device tools.",
            ));
        }
        let hosts = state["hosts"].as_array().map(Vec::as_slice).unwrap_or(&[]);
        let host_id = resolve_host_id(hosts, input["hostId"].as_str())?;
        let target = pick_device(
            &state,
            input["deviceId"].as_str(),
            input["platform"].as_str(),
            host_id.as_deref(),
        )?;
        // Resolve consent and agent connectivity before booting or registering a session.
        let target_host = target["hostId"]
            .as_str()
            .unwrap_or(LOCAL_HOST_ID)
            .to_owned();
        if self.agent_readiness_for(&target_host, dir).await?.is_none() {
            return Err(tool_unavailable(AGENT_ACCESS_UNAVAILABLE));
        }
        let session = self
            .apply(
                "open",
                &json!({
                    "threadId": thread_id,
                    "hostId": target["hostId"],
                    "deviceId": target["id"],
                    "platform": target["platform"],
                    "workspaceRoot": input["workspaceRoot"],
                }),
                dir,
            )
            .await;
        self.prune_launchers(dir).await;
        let session = session?;
        let session_host = session["hostId"]
            .as_str()
            .unwrap_or(LOCAL_HOST_ID)
            .to_owned();
        let session_device = session["deviceId"].as_str().unwrap_or("").to_owned();
        let after = self.current();
        let device = after["devices"]
            .as_array()
            .and_then(|devices| {
                devices.iter().find(|d| {
                    d["hostId"] == session_host.as_str() && d["id"] == session_device.as_str()
                })
            })
            .cloned()
            .unwrap_or(target);
        // agentTarget: the opened device's config and session.
        let Some(endpoint) = self.agent_readiness_for(&session_host, dir).await? else {
            return Err(json!({
                "_tag": "DeviceHostUnavailableError",
                "hostId": session_host,
                "hostLabel": self.host_label(&session_host),
                "reason": AGENT_ACCESS_UNAVAILABLE,
            }));
        };
        let own_session = agent_device::session_name(&thread_id, &session_host, &session_device);
        let simlock_lease = if self.simlock_config(&session_host).is_some() {
            Some(self.simlock_agent_endpoint(&session_host, &session_device)?)
        } else {
            None
        };
        let eas_session = if self.eas_config(&session_host).is_some() {
            Some(self.eas_agent_endpoint(&session_host, &session_device)?)
        } else {
            None
        };
        let (target_args, agent_session) = match &simlock_lease {
            // A host with a daemon per device: the lease's own session and config.
            Some((lease, url)) => {
                let config = agent_device::device_config_path(dir, &session_host, &session_device);
                agent_device::write_config(
                    &config,
                    &AgentEndpoint {
                        base_url: url.clone(),
                        token: lease.token.clone(),
                        entry: PathBuf::new(),
                        state_dir: Some(lease.dir.clone()),
                    },
                )
                .await
                .map_err(|_| op_err("configure agent", "settings_failed"))?;
                self.host_session_launchers
                    .lock()
                    .unwrap()
                    .insert(own_session.clone());
                (
                    vec![
                        "--config".to_owned(),
                        config.to_string_lossy().into_owned(),
                        "--session".to_owned(),
                        lease.session.clone(),
                    ],
                    lease.session.clone(),
                )
            }
            // An Expo session runs its own daemon, and agent-device selects its
            // device by the preview host's id for it.
            None if eas_session.is_some() => {
                let session = eas_session.as_ref().unwrap();
                let config = agent_device::device_config_path(dir, &session_host, &session_device);
                agent_device::write_config(
                    &config,
                    &AgentEndpoint {
                        base_url: session.agent_url.clone(),
                        token: session.agent_token.clone(),
                        entry: PathBuf::new(),
                        state_dir: None,
                    },
                )
                .await
                .map_err(|_| op_err("configure agent", "settings_failed"))?;
                let mut target_args = agent_device::target_args(
                    device["platform"].as_str().unwrap_or("android"),
                    &session.remote_device_id,
                );
                target_args.extend([
                    "--config".to_owned(),
                    config.to_string_lossy().into_owned(),
                    "--session".to_owned(),
                    own_session.clone(),
                ]);
                (target_args, own_session.clone())
            }
            None => {
                let config = agent_device::config_path(dir, &session_host);
                agent_device::write_config(&config, &endpoint)
                    .await
                    .map_err(|_| op_err("configure agent", "settings_failed"))?;
                let mut target_args = agent_device::target_args(
                    device["platform"].as_str().unwrap_or("android"),
                    &session_device,
                );
                target_args.extend([
                    "--config".to_owned(),
                    config.to_string_lossy().into_owned(),
                    "--session".to_owned(),
                    own_session.clone(),
                ]);
                (target_args, own_session.clone())
            }
        };
        // agentLauncher: bound only while the session is still open.
        let command = {
            let mut launchers = self.launchers.lock().await;
            let live = self.state.lock().unwrap()["sessions"]
                .as_array()
                .into_iter()
                .flatten()
                .any(|s| {
                    s["threadId"] == thread_id.as_str()
                        && s["hostId"] == session_host.as_str()
                        && s["deviceId"] == session_device.as_str()
                });
            if !live {
                return Err(tool_unavailable(
                    "The device was closed while device_open was preparing it. Call device_open again only if you still need it.",
                ));
            }
            let Some(node) = agent_device::node_executable() else {
                return Err(local_unavailable(
                    "Device automation requires Node.js. Install Node.js and make sure node is on PATH, then retry.",
                ));
            };
            let entry = install(dir, AGENT)
                .await
                .map_err(|_| op_err("install agent CLI", "command_failed"))?;
            let launcher =
                agent_device::bind_launcher(dir, &node, &entry, &own_session, &target_args)
                    .await
                    .map_err(|_| {
                        local_unavailable("Could not prepare the agent-device launcher.")
                    })?;
            launchers.insert(own_session.clone(), launcher.clone());
            launcher.to_string_lossy().into_owned()
        };
        Ok(json!({
            "device": device,
            "agentDevice": {
                "command": command,
                "targetArgs": target_args,
                "session": agent_session,
            },
            "quickStart": match &simlock_lease {
                Some((lease, _)) => agent_device::quick_start_on(
                    &device,
                    &[],
                    &command,
                    agent_device::QuickStartHost::Simlock {
                        install_directory: Some(&lease.install_directory),
                    },
                ),
                None if eas_session.is_some() => agent_device::quick_start_on(
                    &device,
                    &[],
                    &command,
                    agent_device::QuickStartHost::Eas,
                ),
                None => agent_device::quick_start(&device, &[], &command),
            },
        }))
    }

    /// `screenshot`: a device's screen as PNG bytes, through its host's hub
    /// (`resolveDevice`, then `extractPng`).
    pub(crate) async fn screenshot(
        &self,
        target_host: &str,
        device_id: &str,
        dir: &Path,
    ) -> Result<(Value, Vec<u8>), Value> {
        if let Some(config) = self.eas_config(target_host) {
            // resolveDevice on an Expo host, then the session's preview host.
            self.eas_readiness(target_host, dir).await?;
            let find = |state: &Value| {
                state["devices"]
                    .as_array()
                    .and_then(|ds| {
                        ds.iter()
                            .find(|d| d["hostId"] == target_host && d["id"] == device_id)
                    })
                    .cloned()
            };
            let device = match find(&self.current()) {
                Some(device) => device,
                None => {
                    self.eas_refresh(&config, dir).await;
                    find(&self.current()).ok_or_else(|| {
                        json!({"_tag":"DeviceNotFoundError","hostId":target_host,"deviceId":device_id})
                    })?
                }
            };
            let bytes = self.eas_screenshot(target_host, device_id).await?;
            let Some(png) = extract_png(&bytes).map(<[u8]>::to_vec) else {
                return Err(json!({
                    "_tag": "DeviceOperationError",
                    "operation": "screenshot",
                    "reason": "hub_rejected",
                    "cause": format!("The hub returned {} bytes without a complete PNG.", bytes.len()),
                }));
            };
            return Ok((device, png));
        }
        if self.simlock_config(target_host).is_some() {
            // resolveDevice on a Simlock host, then the lease's screenshot.
            self.simlock_readiness(target_host, dir).await?;
            let find = |state: &Value| {
                state["devices"]
                    .as_array()
                    .and_then(|ds| {
                        ds.iter()
                            .find(|d| d["hostId"] == target_host && d["id"] == device_id)
                    })
                    .cloned()
            };
            let device = match find(&self.current()) {
                Some(device) => device,
                None => {
                    self.simlock_refresh(target_host, dir).await?;
                    find(&self.current()).ok_or_else(|| {
                        json!({"_tag":"DeviceNotFoundError","hostId":target_host,"deviceId":device_id})
                    })?
                }
            };
            let bytes = self.simlock_screenshot(target_host, device_id).await?;
            let Some(png) = extract_png(&bytes).map(<[u8]>::to_vec) else {
                return Err(json!({
                    "_tag": "DeviceOperationError",
                    "operation": "screenshot",
                    "reason": "hub_rejected",
                    "cause": format!("The hub returned {} bytes without a complete PNG.", bytes.len()),
                }));
            };
            return Ok((device, png));
        }
        if target_host != LOCAL_HOST_ID && self.ssh_config(target_host).is_none() {
            return Err(bad_host(target_host));
        }
        // resolveDevice: a ready host and its device, refreshed if not listed.
        let enabled = self
            .settings
            .internal()
            .map_err(|_| op_err("settings", "settings_failed"))?["enableDeviceSupport"]
            == true;
        if !enabled {
            return Err(self.disabled_for(target_host));
        }
        let mut runtime = self.runtime.lock().await;
        let ctx = if target_host == LOCAL_HOST_ID {
            match ensure_hub(&mut runtime, dir).await {
                Ok(origin) => HostCtx::local(&origin),
                Err(error) => {
                    drop(runtime);
                    self.set_host_status("failed", Some(error_detail(&error)));
                    return Err(error);
                }
            }
        } else {
            self.ssh_readiness(target_host, dir).await?
        };
        let find = |state: &Value| {
            state["devices"]
                .as_array()
                .and_then(|ds| {
                    ds.iter()
                        .find(|d| d["hostId"] == target_host && d["id"] == device_id)
                })
                .cloned()
        };
        let device = match find(&self.current()) {
            Some(device) => device,
            None => {
                let listed = list_hub(&ctx).await?;
                let mut state = self.current();
                apply_list_result(&mut state, dir, &ctx.id, listed);
                self.publish(&mut state);
                find(&state).ok_or_else(|| {
                    json!({"_tag":"DeviceNotFoundError","hostId":target_host,"deviceId":device_id})
                })?
            }
        };
        drop(runtime);
        let bytes = hub_screenshot(&ctx, &device).await?;
        let Some(png) = extract_png(&bytes).map(<[u8]>::to_vec) else {
            return Err(json!({
                "_tag": "DeviceOperationError",
                "operation": "screenshot",
                "reason": "hub_rejected",
                "cause": format!("The hub returned {} bytes without a complete PNG.", bytes.len()),
            }));
        };
        Ok((device, png))
    }

    /// `device_screenshot`: the PNG of a device open in the calling thread.
    pub(super) async fn tool_screenshot(&self, input: &Value, dir: &Path) -> Reply {
        let thread_id = input["threadId"].as_str().unwrap_or("");
        let state = self.snapshot(dir);
        let hosts = state["hosts"].as_array().map(Vec::as_slice).unwrap_or(&[]);
        let host_id = match input["hostId"].as_str() {
            Some(h) => resolve_host_id(hosts, Some(h))?,
            None => None,
        };
        let target = match input["deviceId"].as_str() {
            None => state["sessions"]
                .as_array()
                .into_iter()
                .flatten()
                .rfind(|s| {
                    s["threadId"] == thread_id
                        && host_id.as_deref().is_none_or(|h| s["hostId"] == h)
                })
                .cloned(),
            Some(device_id) => locate_session(&state, thread_id, device_id, host_id.as_deref())?,
        };
        let Some(target) = target else {
            return Err(tool_unavailable(match input["deviceId"].as_str() {
                None => "No device is open in this thread. Call device_open first.".to_owned(),
                Some(id) => {
                    format!("Device {id} is not open in this thread. Call device_open first.")
                }
            }));
        };
        let target_host = target["hostId"].as_str().unwrap_or(LOCAL_HOST_ID);
        let device_id = target["deviceId"].as_str().unwrap_or("");
        let (device, png) = self.screenshot(target_host, device_id, dir).await?;
        let png = png.as_slice();
        use base64::Engine as _;
        let (width, height) = png_dimensions(png);
        Ok(json!({
            "device": device,
            "screenshot": {
                "mimeType": "image/png",
                "data": base64::engine::general_purpose::STANDARD.encode(png),
                "width": width,
                "height": height,
            },
        }))
    }
}

/// `testHost`: a host's summary from the reference's probe, or this
/// machine's when the target is this machine.
async fn test_host(input: &Value, dir: &Path) -> Reply {
    let id = string(input, "id")?;
    let label = string(input, "label")?;
    let target = string(input, "target")?;
    if id == LOCAL_HOST_ID {
        return Err(invalid("id", "The local host id is reserved."));
    }
    if target.starts_with('-') || target.chars().any(|c| c.is_control() || c.is_whitespace()) {
        return Err(invalid("target", "Invalid SSH target"));
    }
    let port = match input.get("port") {
        Some(port) => Some(
            port.as_u64()
                .and_then(|p| u16::try_from(p).ok())
                .filter(|p| *p >= 1)
                .ok_or_else(|| invalid("port", "Invalid port"))?,
        ),
        None => None,
    };
    let identity_file = match input.get("identityFile") {
        Some(_) => Some(string(input, "identityFile")?.to_owned()),
        None => None,
    };
    if super::local_ssh::local_target(input).await {
        return Ok(host(dir, false));
    }
    let config = SshConfig {
        id: id.to_owned(),
        label: label.to_owned(),
        target: target.to_owned(),
        port,
        identity_file,
    };
    ssh_host::probe(&config, &ssh_host::owner(dir, id))
        .await
        .map_err(|error| match ssh_host::probe_failure_message(&error) {
            Some(reason) => json!({
                "_tag": "DeviceHostUnavailableError",
                "hostId": id,
                "hostLabel": label,
                "reason": reason,
                "cause": error.encoded(id),
            }),
            None => json!({
                "_tag": "DeviceOperationError",
                "operation": "probe host",
                "reason": "request_failed",
            }),
        })
}

impl DeviceService {
    fn ssh_config(&self, host_id: &str) -> Option<SshConfig> {
        self.ssh_hosts
            .lock()
            .unwrap()
            .iter()
            .find(|(config, _)| config.id == host_id)
            .map(|(config, _)| config.clone())
    }

    fn ssh_summary(&self, host_id: &str) -> Option<Value> {
        self.ssh_hosts
            .lock()
            .unwrap()
            .iter()
            .find(|(config, _)| config.id == host_id)
            .map(|(_, summary)| summary.clone())
    }

    fn set_ssh_summary(&self, host_id: &str, update: impl FnOnce(&mut Value)) {
        if let Some((_, summary)) = self
            .ssh_hosts
            .lock()
            .unwrap()
            .iter_mut()
            .find(|(config, _)| config.id == host_id)
        {
            update(summary);
        }
    }

    pub(super) fn host_label(&self, host_id: &str) -> String {
        if host_id == LOCAL_HOST_ID {
            return LOCAL_HOST_LABEL.into();
        }
        self.ssh_config(host_id)
            .map(|config| config.label)
            .or_else(|| self.eas_config(host_id).map(|config| config.label))
            .or_else(|| self.simlock_config(host_id).map(|config| config.label))
            .unwrap_or_else(|| host_id.to_owned())
    }

    /// `readiness`'s refusal while device support is off.
    fn disabled_for(&self, host_id: &str) -> Value {
        let mut error = disabled();
        error["hostId"] = json!(host_id);
        error["hostLabel"] = json!(self.host_label(host_id));
        error
    }

    /// `platformAvailability` for an SSH host: its probe, so tooling
    /// installed since is found, or that it cannot be reached.
    async fn ssh_platform(&self, host_id: &str, platform: &str, dir: &Path) -> Value {
        let Some(config) = self.ssh_config(host_id) else {
            return json!({"platform": platform, "available": false});
        };
        match ssh_host::probe(&config, &ssh_host::owner(dir, host_id)).await {
            Ok(probed) => {
                let platforms = probed["platforms"].clone();
                self.set_ssh_summary(host_id, |summary| summary["platforms"] = platforms.clone());
                platforms
                    .as_array()
                    .and_then(|list| list.iter().find(|p| p["platform"] == platform))
                    .cloned()
                    .unwrap_or_else(|| json!({"platform": platform, "available": false}))
            }
            Err(_) => json!({
                "platform": platform,
                "available": false,
                "reason": "Cannot reach device host. Test its SSH connection in Settings.",
            }),
        }
    }

    /// `current`: an SSH host's hub while its tunnel is up.
    async fn ssh_current(&self, host_id: &str) -> Option<HostCtx> {
        let mut ready = self.ssh_ready.lock().await;
        let entry = ready.get_mut(host_id)?;
        if !entry.tunnel.alive() {
            ready.remove(host_id);
            return None;
        }
        Some(entry.ctx.clone())
    }

    /// `readiness` for an SSH host: probes it, starts its hub with the
    /// bootstrap, forwards its ports here, and reports each phase.
    async fn ssh_readiness(&self, host_id: &str, dir: &Path) -> Result<HostCtx, Value> {
        let Some(config) = self.ssh_config(host_id) else {
            return Err(bad_host(host_id));
        };
        let enabled = self
            .settings
            .internal()
            .map_err(|_| op_err("settings", "settings_failed"))?["enableDeviceSupport"]
            == true;
        if !enabled {
            return Err(self.disabled_for(host_id));
        }
        let ctx = match self.ssh_connect(&config, dir).await {
            Ok(ctx) => ctx,
            Err(error) => {
                let actionable = (error.step == "probe")
                    .then(|| ssh_host::probe_failure_message(&error))
                    .flatten();
                let detail = actionable.clone().unwrap_or_else(|| {
                    format!("Device host {host_id} failed while {}.", error.step)
                });
                self.set_host_status_for(host_id, "failed", Some(detail));
                return Err(json!({
                    "_tag": "DeviceHostUnavailableError",
                    "hostId": host_id,
                    "hostLabel": config.label,
                    "reason": actionable.unwrap_or_else(|| format!("Device support failed during {}.", error.step)),
                    "cause": error.encoded(host_id),
                }));
            }
        };
        if self.current()["hostStatuses"][host_id]["status"] != "ready" {
            self.set_host_status_for(host_id, "ready", None);
        }
        Ok(ctx)
    }

    /// `ensureReady` and `connect` for an SSH host.
    async fn ssh_connect(
        &self,
        config: &SshConfig,
        dir: &Path,
    ) -> Result<HostCtx, ssh_host::HostError> {
        let mut ready = self.ssh_ready.lock().await;
        if let Some(entry) = ready.get_mut(&config.id) {
            if entry.tunnel.alive() {
                return Ok(entry.ctx.clone());
            }
            ready.remove(&config.id);
        }
        let owner = ssh_host::owner(dir, &config.id);
        let probed = ssh_host::probe(config, &owner).await?;
        let hub_installed = probed["hubInstalled"] == true;
        let hub_tool = probed["tools"]["hub"].clone();
        self.set_ssh_summary(&config.id, |summary| *summary = probed);
        if hub_installed {
            self.set_host_status_for(&config.id, "starting", None);
        } else {
            self.set_host_status_for(
                &config.id,
                "installing",
                Some(ssh_host::install_message("device hub", &hub_tool)),
            );
        }
        self.ssh_connect_locked(&mut ready, config, dir).await
    }

    /// `connect`: the bootstrap's start (with the agent daemon when agents
    /// want this host), the tunnel, and the host's agent config.
    async fn ssh_connect_locked(
        &self,
        ready: &mut HashMap<String, SshReady>,
        config: &SshConfig,
        dir: &Path,
    ) -> Result<HostCtx, ssh_host::HostError> {
        let owner = ssh_host::owner(dir, &config.id);
        let agent = self.ssh_wants_agent.lock().unwrap().contains(&config.id);
        self.ssh_activated.lock().unwrap().insert(config.id.clone());
        let mut attempt = 0;
        loop {
            let started = ssh_host::start(config, &owner, agent).await?;
            self.set_host_status_for(&config.id, "starting", None);
            let previous_agent = self
                .ssh_summary(&config.id)
                .is_some_and(|summary| summary["agentDeviceInstalled"] == true);
            self.set_ssh_summary(&config.id, |summary| {
                summary["platforms"] = started.platforms.clone();
                match started.tools.is_null() {
                    true => {
                        if let Some(object) = summary.as_object_mut() {
                            object.remove("tools");
                        }
                    }
                    false => summary["tools"] = started.tools.clone(),
                }
                summary["hubInstalled"] = json!(true);
                summary["agentDeviceInstalled"] = json!(agent || previous_agent);
            });
            match ssh_host::open_tunnel(config, &started).await {
                Ok((tunnel, hub_port, daemon_port)) => {
                    let platform = |name: &str| {
                        started.platforms.as_array().is_some_and(|list| {
                            list.iter()
                                .any(|p| p["platform"] == name && p["available"] == true)
                        })
                    };
                    let ctx = HostCtx {
                        id: config.id.clone(),
                        origin: format!("http://127.0.0.1:{hub_port}"),
                        run: HostRun::Ssh {
                            config: config.clone(),
                            owner: owner.clone(),
                        },
                        node_path: started.node_path.clone(),
                        ios: platform("ios"),
                        android: platform("android"),
                    };
                    let endpoint = match (&started.daemon_port, &started.token, &started.entry_path)
                    {
                        (Some(_), Some(token), Some(entry)) => Some(AgentEndpoint {
                            base_url: format!("http://127.0.0.1:{daemon_port}"),
                            token: token.clone(),
                            entry: PathBuf::from(entry),
                            state_dir: None,
                        }),
                        _ => None,
                    };
                    // onReady: agents reach the host's daemon through this tunnel.
                    if let Some(endpoint) = &endpoint {
                        agent_device::write_config(
                            &agent_device::config_path(dir, &config.id),
                            endpoint,
                        )
                        .await
                        .map_err(|_| ssh_host::HostError {
                            step: "configuring agent access".into(),
                            cause: Box::new(ssh_host::Cause::Other),
                        })?;
                    }
                    ready.insert(
                        config.id.clone(),
                        SshReady {
                            ctx: ctx.clone(),
                            tunnel,
                            agent: endpoint,
                        },
                    );
                    self.set_host_status_for(&config.id, "ready", None);
                    return Ok(ctx);
                }
                // SSH binds after the reservation is released, so a competing bind needs fresh ports.
                Err(error)
                    if attempt < 2
                        && ["forwarding ports", "waiting for SSH forward"]
                            .contains(&error.step.as_str()) =>
                {
                    attempt += 1;
                }
                Err(error) => return Err(error),
            }
        }
    }

    /// `ensureAgentReady`: the host made ready, then (`changeAgent(true)`)
    /// reconnected with its agent daemon unless it already has it.
    async fn ssh_agent_ready(
        &self,
        config: &SshConfig,
        dir: &Path,
    ) -> Result<AgentEndpoint, ssh_host::HostError> {
        self.ssh_connect(config, dir).await?;
        let summary = self.ssh_summary(&config.id).unwrap_or_default();
        if summary["agentDeviceInstalled"] == true {
            self.set_host_status_for(&config.id, "starting", None);
        } else {
            self.set_host_status_for(
                &config.id,
                "installing",
                Some(ssh_host::install_message(
                    "agent tools",
                    &summary["tools"]["agent"],
                )),
            );
        }
        let mut ready = self.ssh_ready.lock().await;
        self.ssh_wants_agent
            .lock()
            .unwrap()
            .insert(config.id.clone());
        if let Some(agent) = ready.get(&config.id).and_then(|entry| entry.agent.clone()) {
            return Ok(agent);
        }
        ready.remove(&config.id);
        match self.ssh_connect_locked(&mut ready, config, dir).await {
            Ok(_) => ready
                .get(&config.id)
                .and_then(|entry| entry.agent.clone())
                .ok_or_else(|| ssh_host::HostError {
                    step: "starting agent tools".into(),
                    cause: Box::new(ssh_host::Cause::Other),
                }),
            Err(error) => {
                ready.remove(&config.id);
                let _ =
                    ssh_host::bootstrap(config, &ssh_host::owner(dir, &config.id), "stop-agent")
                        .await;
                Err(error)
            }
        }
    }

    /// `agentReadinessIfSupported` for any host: the agent daemon agents
    /// reach it through, or `None` when agent device access is off.
    pub(super) async fn agent_readiness_for(
        &self,
        host_id: &str,
        dir: &Path,
    ) -> Result<Option<AgentEndpoint>, Value> {
        if host_id == LOCAL_HOST_ID {
            return self.agent_readiness(dir).await;
        }
        if self.eas_config(host_id).is_some() {
            let preferences = self
                .settings
                .internal()
                .map_err(|_| op_err("settings", "settings_failed"))?;
            if preferences["enableDeviceSupport"] != true
                || preferences["enableAgentDeviceAccess"] != true
            {
                return Ok(None);
            }
            // Each session runs its own daemon (`agentDevice: null`).
            self.eas_readiness(host_id, dir).await?;
            let mut state = self.current();
            self.publish(&mut state);
            self.set_host_status_for(host_id, "ready", None);
            return Ok(Some(AgentEndpoint {
                base_url: String::new(),
                token: String::new(),
                entry: PathBuf::new(),
                state_dir: None,
            }));
        }
        if let Some(config) = self.simlock_config(host_id) {
            let preferences = self
                .settings
                .internal()
                .map_err(|_| op_err("settings", "settings_failed"))?;
            if preferences["enableDeviceSupport"] != true
                || preferences["enableAgentDeviceAccess"] != true
            {
                return Ok(None);
            }
            // Each lease is its own daemon session (`agentDevice: null`).
            let (_, entry) = self.simlock_readiness(host_id, dir).await?;
            let mut state = self.current();
            self.publish(&mut state);
            self.set_host_status_for(host_id, "ready", None);
            return Ok(Some(AgentEndpoint {
                base_url: config.agent_device_url(),
                token: String::new(),
                entry,
                state_dir: None,
            }));
        }
        let Some(config) = self.ssh_config(host_id) else {
            return Err(bad_host(host_id));
        };
        let preferences = self
            .settings
            .internal()
            .map_err(|_| op_err("settings", "settings_failed"))?;
        if preferences["enableDeviceSupport"] != true
            || preferences["enableAgentDeviceAccess"] != true
        {
            return Ok(None);
        }
        match self.ssh_agent_ready(&config, dir).await {
            Ok(endpoint) => {
                let mut state = self.current();
                self.publish(&mut state);
                self.set_host_status_for(host_id, "ready", None);
                Ok(Some(endpoint))
            }
            Err(error) => {
                let actionable = (error.step == "probe")
                    .then(|| ssh_host::probe_failure_message(&error))
                    .flatten();
                self.set_host_status_for(
                    host_id,
                    "failed",
                    Some(actionable.clone().unwrap_or_else(|| {
                        format!("Device host {host_id} failed while {}.", error.step)
                    })),
                );
                Err(json!({
                    "_tag": "DeviceHostUnavailableError",
                    "hostId": host_id,
                    "hostLabel": config.label,
                    "reason": actionable.unwrap_or_else(|| format!("Device support failed during {}.", error.step)),
                    "cause": error.encoded(host_id),
                }))
            }
        }
    }

    /// `stopAgent` for an SSH host (`changeAgent(false)`): stops its daemon
    /// and reconnects without it, if it had one.
    async fn ssh_stop_agent(&self, config: &SshConfig, dir: &Path) {
        self.ssh_wants_agent.lock().unwrap().remove(&config.id);
        let mut ready = self.ssh_ready.lock().await;
        if ready
            .get(&config.id)
            .is_none_or(|entry| entry.agent.is_none())
        {
            return;
        }
        ready.remove(&config.id);
        if ssh_host::bootstrap(config, &ssh_host::owner(dir, &config.id), "stop-agent")
            .await
            .is_ok()
        {
            let _ = self.ssh_connect_locked(&mut ready, config, dir).await;
        }
    }

    /// The agent daemon an SSH host's sessions are released on, while connected.
    async fn ssh_agent_endpoint(&self, host_id: &str) -> Option<AgentEndpoint> {
        self.ssh_ready
            .lock()
            .await
            .get(host_id)
            .and_then(|entry| entry.agent.clone())
    }

    /// Every SSH host stopped, as the server shuts down.
    pub(crate) async fn stop_ssh_hosts(&self, dir: &Path) {
        let hosts: Vec<SshConfig> = self
            .ssh_hosts
            .lock()
            .unwrap()
            .iter()
            .map(|(config, _)| config.clone())
            .collect();
        futures_util::stream::iter(hosts)
            .for_each_concurrent(4, |config| async move { self.ssh_stop(&config, dir).await })
            .await;
        let simlock_hosts: Vec<SimlockConfig> = self
            .simlock_hosts
            .lock()
            .unwrap()
            .iter()
            .map(|host| host.config.clone())
            .collect();
        for config in simlock_hosts {
            self.simlock_release_all(config).await;
        }
        let eas_hosts: Vec<EasConfig> = self
            .eas_hosts
            .lock()
            .unwrap()
            .iter()
            .map(|host| host.config.clone())
            .collect();
        // Unlike machines the user owns, every running session here is billed.
        for config in eas_hosts {
            self.eas_stop_all(&config, dir).await;
        }
    }

    /// `stop` for an SSH host: drops its tunnel and stops what it started there.
    async fn ssh_stop(&self, config: &SshConfig, dir: &Path) {
        self.ssh_wants_agent.lock().unwrap().remove(&config.id);
        self.ssh_ready.lock().await.remove(&config.id);
        if self.ssh_activated.lock().unwrap().remove(&config.id) {
            let _ = ssh_host::bootstrap(config, &ssh_host::owner(dir, &config.id), "stop").await;
        }
    }

    /// `list` for the SSH hosts: each one made ready and listed, or its failure as its status.
    async fn list_ssh_hosts(&self, dir: &Path) {
        let hosts: Vec<SshConfig> = self
            .ssh_hosts
            .lock()
            .unwrap()
            .iter()
            .map(|(config, _)| config.clone())
            .collect();
        futures_util::stream::iter(hosts)
            .for_each_concurrent(4, |config| async move {
                self.list_ssh_host(&config.id, dir).await
            })
            .await;
    }

    /// `retryHost` for an SSH host: its agent tools when agents may use it,
    /// else the host alone, then its devices; a failure is its status.
    async fn retry_ssh_host(&self, host_id: &str, dir: &Path) {
        match self.agent_readiness_for(host_id, dir).await {
            Ok(None) => self.list_ssh_host(host_id, dir).await,
            Ok(Some(_)) => {
                let listed = match self.ssh_current(host_id).await {
                    Some(ctx) => list_hub(&ctx).await,
                    None => return,
                };
                match listed {
                    Ok(listed) => {
                        let mut state = self.current();
                        apply_list_result(&mut state, dir, host_id, listed);
                        self.publish(&mut state);
                    }
                    Err(error) => self.set_host_status_for(
                        host_id,
                        "failed",
                        Some(crate::mcp_oauth::format_device_error(&error)),
                    ),
                }
            }
            Err(error) => self.set_host_status_for(
                host_id,
                "failed",
                Some(error["reason"].as_str().unwrap_or_default().to_owned()),
            ),
        }
    }

    /// One SSH host made ready and listed, or its failure as its status.
    async fn list_ssh_host(&self, host_id: &str, dir: &Path) {
        let Some(config) = self.ssh_config(host_id) else {
            return;
        };
        {
            {
                let listed = match self.ssh_readiness(&config.id, dir).await {
                    Ok(ctx) => list_hub(&ctx).await,
                    Err(error) => Err(error),
                };
                match listed {
                    Ok(listed) => {
                        if self.ssh_config(&config.id).as_ref() != Some(&config) {
                            return;
                        }
                        let mut state = self.current();
                        if state["hostStatus"] == "disabled" {
                            return;
                        }
                        apply_list_result(&mut state, dir, &config.id, listed);
                        self.publish(&mut state);
                    }
                    Err(error) => {
                        let detail = if error["_tag"] == "DeviceHostUnavailableError" {
                            error["reason"].as_str().unwrap_or_default().to_owned()
                        } else {
                            crate::mcp_oauth::format_device_error(&error)
                        };
                        self.set_host_status_for(&config.id, "failed", Some(detail));
                    }
                }
            }
        }
    }

    fn eas_config(&self, host_id: &str) -> Option<EasConfig> {
        self.eas_hosts
            .lock()
            .unwrap()
            .iter()
            .find(|host| host.config.id == host_id)
            .map(|host| host.config.clone())
    }

    fn with_eas<T>(&self, host_id: &str, f: impl FnOnce(&mut EasState) -> T) -> Option<T> {
        self.eas_hosts
            .lock()
            .unwrap()
            .iter_mut()
            .find(|host| host.config.id == host_id)
            .map(f)
    }

    /// `resolveSavedExpoCredential`: the host's saved token (a secret, read
    /// now), else `EXPO_TOKEN`, else the `eas login` session.
    async fn eas_credential(&self, config: &EasConfig) -> Result<expo_api::Credential, String> {
        let settings = self
            .settings
            .internal()
            .map_err(|_| "Could not read or save device settings.".to_owned())?;
        let token = device_token(&self.settings.resolver, &settings, &config.access_token).await?;
        expo_api::resolve_credential(&token, &home())
    }

    /// Each host's `platformAvailability`: each server signs in on its own,
    /// so one with no Expo credential answers for itself.
    async fn refresh_eas_platforms(&self) {
        let configs: Vec<EasConfig> = self
            .eas_hosts
            .lock()
            .unwrap()
            .iter()
            .map(|host| host.config.clone())
            .collect();
        for config in configs {
            let available = self.eas_credential(&config).await;
            let platforms: Vec<Value> = ["ios", "android"]
                .into_iter()
                .map(|platform| match &available {
                    Ok(_) => json!({"platform": platform, "available": true}),
                    Err(reason) => {
                        json!({"platform": platform, "available": false, "reason": reason})
                    }
                })
                .collect();
            self.with_eas(&config.id, |host| host.platforms = Value::Array(platforms));
        }
    }

    fn eas_unavailable(config: &EasConfig, reason: impl Into<String>) -> Value {
        json!({
            "_tag": "DeviceHostUnavailableError",
            "hostId": config.id,
            "hostLabel": config.label,
            "reason": reason.into(),
        })
    }

    /// `api`: the host's Expo credential, read now (and kept for stops).
    async fn eas_api(&self, config: &EasConfig) -> Result<expo_api::Credential, Value> {
        match self.eas_credential(config).await {
            Ok(credential) => {
                self.with_eas(&config.id, |host| {
                    host.last_credential = Some(credential.clone())
                });
                Ok(credential)
            }
            Err(reason) => Err(Self::eas_unavailable(config, reason)),
        }
    }

    /// `apiForStop`: stopping a billed session must not depend on the
    /// secret still being readable; starting one must.
    async fn eas_api_for_stop(&self, config: &EasConfig) -> Result<expo_api::Credential, Value> {
        match self.eas_api(config).await {
            Ok(credential) => Ok(credential),
            Err(error) => self
                .with_eas(&config.id, |host| host.last_credential.clone())
                .flatten()
                .ok_or(error),
        }
    }

    /// `writeLedger`, under the host's ledger lock.
    async fn eas_write_ledger(
        &self,
        config: &EasConfig,
        dir: &Path,
        update: impl FnOnce(Vec<eas::LedgerEntry>) -> Vec<eas::LedgerEntry>,
    ) -> Result<(), Value> {
        let _lock = self.eas_ledger_lock.lock().await;
        let path = eas::ledger_path(dir, &config.id);
        let entries = eas::read_ledger(&path).map_err(|_| {
            Self::eas_unavailable(
                config,
                format!(
                    "The record of running Expo Simulators sessions at {} could not be read. Fix or remove it.",
                    path.display()
                ),
            )
        })?;
        eas::write_ledger(&path, &update(entries)).map_err(|_| {
            Self::eas_unavailable(
                config,
                format!(
                    "The record of running Expo Simulators sessions at {} could not be written.",
                    path.display()
                ),
            )
        })
    }

    async fn eas_forget_entry(&self, config: &EasConfig, dir: &Path, name: &str) {
        let name = name.to_owned();
        if self
            .eas_write_ledger(config, dir, |entries| {
                entries
                    .into_iter()
                    .filter(|entry| entry.name != name)
                    .collect()
            })
            .await
            .is_err()
        {
            tracing::warn!("Could not update the Expo Simulators session record");
        }
    }

    async fn eas_mark_refused(&self, config: &EasConfig, dir: &Path, name: &str) {
        let name = name.to_owned();
        if self
            .eas_write_ledger(config, dir, |entries| {
                entries
                    .into_iter()
                    .map(|mut entry| {
                        if entry.name == name {
                            entry.refused = true;
                        }
                        entry
                    })
                    .collect()
            })
            .await
            .is_err()
        {
            tracing::warn!("Could not update the Expo Simulators session record");
        }
    }

    /// `stopSession`: stops a session and, once Expo confirms it, drops it from the ledger.
    async fn eas_stop_session(
        &self,
        config: &EasConfig,
        dir: &Path,
        credential: &expo_api::Credential,
        session_id: &str,
        name: &str,
    ) -> Result<(), ()> {
        match expo_api::stop(credential, session_id).await {
            Ok(_) => {
                self.eas_forget_entry(config, dir, name).await;
                Ok(())
            }
            Err(error) => {
                tracing::warn!(%session_id, error = %error.message(), "Could not stop Expo Simulators session");
                Err(())
            }
        }
    }

    /// `stopRecorded`: finds recorded sessions by name in each app's running
    /// sessions and stops them; the names found and the apps listed.
    async fn eas_stop_recorded(
        &self,
        config: &EasConfig,
        dir: &Path,
        credential: &expo_api::Credential,
        entries: &[eas::LedgerEntry],
    ) -> (HashSet<String>, HashSet<String>) {
        let mut found = HashSet::new();
        let mut listed = HashSet::new();
        let mut apps: Vec<String> = Vec::new();
        for entry in entries {
            if !apps.contains(&entry.app_id) {
                apps.push(entry.app_id.clone());
            }
        }
        for app_id in apps {
            let Ok(active) = expo_api::active_sessions(credential, &app_id).await else {
                continue;
            };
            for entry in entries.iter().filter(|entry| entry.app_id == app_id) {
                let running: Vec<&Value> = active
                    .iter()
                    .filter(|s| s["name"] == entry.name.as_str())
                    .collect();
                if running.is_empty() {
                    continue;
                }
                found.insert(entry.name.clone());
                for session in running {
                    let session_id = session["id"].as_str().unwrap_or_default().to_owned();
                    if self
                        .eas_stop_session(config, dir, credential, &session_id, &entry.name)
                        .await
                        .is_err()
                    {
                        // Still running and billed: owned, counted, and retried by listing.
                        self.with_eas(&config.id, |host| {
                            host.unresolved
                                .insert(entry.name.clone(), session_id.clone())
                        });
                    }
                }
            }
            listed.insert(app_id);
        }
        (found, listed)
    }

    /// `retryUnresolved`: a session Expo already ended is let go even if stopping it keeps failing.
    async fn eas_retry_unresolved(
        &self,
        config: &EasConfig,
        dir: &Path,
        credential: &expo_api::Credential,
        name: &str,
        session_id: &str,
    ) {
        let settled = if self
            .eas_stop_session(config, dir, credential, session_id, name)
            .await
            .is_ok()
        {
            true
        } else {
            match expo_api::session(credential, session_id).await {
                Ok(current) if current.status == "STOPPED" || current.status == "ERRORED" => {
                    self.eas_forget_entry(config, dir, name).await;
                    true
                }
                _ => false,
            }
        };
        if settled {
            self.with_eas(&config.id, |host| host.unresolved.remove(name));
        }
    }

    /// `adoptRecorded`: records a previous server left count against the
    /// limit until a sweep finds them.
    fn eas_adopt_recorded(&self, config: &EasConfig, dir: &Path) {
        let Ok(entries) = eas::read_ledger(&eas::ledger_path(dir, &config.id)) else {
            return;
        };
        let now = eas::now_ms();
        self.with_eas(&config.id, |host| {
            let owned = host.owned_names();
            for entry in entries {
                let until = (entry.recorded_at + UNCERTAIN_CREATE_MS).max(now);
                // A refused create made nothing: its record is for the reaper only.
                if !owned.contains(&entry.name) && !entry.refused {
                    host.uncertain.insert(entry.name.clone(), (entry, until));
                }
            }
        });
    }

    /// `reapLeftovers`: stops recorded sessions this host does not own.
    async fn eas_reap(&self, config: &EasConfig, dir: &Path, credential: &expo_api::Credential) {
        let reaped_at = eas::now_ms();
        self.with_eas(&config.id, |host| host.reaped_at = reaped_at);
        let Ok(entries) = eas::read_ledger(&eas::ledger_path(dir, &config.id)) else {
            tracing::warn!("Could not stop recorded Expo Simulators sessions");
            return;
        };
        let owned = self
            .with_eas(&config.id, |host| host.owned_names())
            .unwrap_or_default();
        let leftovers: Vec<eas::LedgerEntry> = entries
            .into_iter()
            .filter(|entry| !owned.contains(&entry.name))
            .collect();
        let (found, listed) = self
            .eas_stop_recorded(config, dir, credential, &leftovers)
            .await;
        self.with_eas(&config.id, |host| {
            host.uncertain.retain(|name, (entry, until)| {
                !(found.contains(name) || (reaped_at >= *until && listed.contains(&entry.app_id)))
            });
        });
        for entry in &leftovers {
            let age = reaped_at - entry.recorded_at;
            if age > RECORD_LIFETIME_MS {
                self.with_eas(&config.id, |host| host.uncertain.remove(&entry.name));
            }
            if found.contains(&entry.name) {
                continue;
            }
            if (age > RECORD_LIFETIME_MS && listed.contains(&entry.app_id))
                || age > RECORD_MAX_AGE_MS
            {
                self.eas_forget_entry(config, dir, &entry.name).await;
            }
        }
    }

    /// `ensureReady` for an Expo host: adopts records a previous server
    /// left and sweeps them. Without a credential the host is still ready.
    async fn eas_ensure_ready(&self, config: &EasConfig, dir: &Path) {
        if self.with_eas(&config.id, |host| host.ready) == Some(true) {
            return;
        }
        self.eas_adopt_recorded(config, dir);
        if let Ok(credential) = self.eas_api(config).await {
            self.eas_reap(config, dir, &credential).await;
        }
        self.with_eas(&config.id, |host| host.ready = true);
    }

    /// `readiness` for an Expo host.
    async fn eas_readiness(&self, host_id: &str, dir: &Path) -> Result<EasConfig, Value> {
        let config = self.eas_config(host_id).ok_or_else(|| bad_host(host_id))?;
        let enabled = self
            .settings
            .internal()
            .map_err(|_| op_err("settings", "settings_failed"))?["enableDeviceSupport"]
            == true;
        if !enabled {
            return Err(self.disabled_for(host_id));
        }
        self.eas_ensure_ready(&config, dir).await;
        if self.current()["hostStatuses"][host_id]["status"] != "ready" {
            self.set_host_status_for(host_id, "ready", None);
        }
        Ok(config)
    }

    /// `dropEnded`: Expo ends sessions at their run or idle limit; stop
    /// listing those, retry failed stops, and reap now and then.
    async fn eas_drop_ended(&self, config: &EasConfig, dir: &Path) {
        let now = eas::now_ms();
        let Some((stale, unresolved, uncertain, reap_due)) = self.with_eas(&config.id, |host| {
            let stale: Vec<eas::RunningSession> = host
                .sessions
                .values()
                .filter(|session| {
                    session.stopping
                        || now
                            - host
                                .checked_at
                                .get(&session.session_id)
                                .copied()
                                .unwrap_or(0.0)
                            > STATUS_TTL_MS
                })
                .cloned()
                .collect();
            let unresolved: Vec<(String, String)> = host
                .unresolved
                .iter()
                .map(|(n, s)| (n.clone(), s.clone()))
                .collect();
            (
                stale,
                unresolved,
                host.uncertain.len(),
                now - host.reaped_at > REAP_INTERVAL_MS,
            )
        }) else {
            return;
        };
        if stale.is_empty() && unresolved.is_empty() && uncertain == 0 && !reap_due {
            return;
        }
        let reap = reap_due || uncertain > 0;
        if reap {
            self.with_eas(&config.id, |host| host.reaped_at = now);
        }
        let credential = match self.eas_api_for_stop(config).await {
            Ok(credential) => credential,
            Err(_) => {
                // Without a credential no sweep can look; a record Expo would
                // have listed by now still stops counting.
                self.with_eas(&config.id, |host| {
                    host.uncertain.retain(|_, (_, until)| now < *until)
                });
                return;
            }
        };
        for (name, session_id) in unresolved {
            self.eas_retry_unresolved(config, dir, &credential, &name, &session_id)
                .await;
        }
        for session in stale {
            if session.stopping
                && self
                    .eas_stop_session(config, dir, &credential, &session.session_id, &session.name)
                    .await
                    .is_ok()
            {
                self.eas_forget_session(&config.id, &session.session_id);
                continue;
            }
            match expo_api::session(&credential, &session.session_id).await {
                Ok(current) if current.status == "STOPPED" || current.status == "ERRORED" => {
                    self.eas_forget_session(&config.id, &session.session_id);
                    self.eas_forget_entry(config, dir, &session.name).await;
                }
                Ok(_) if !session.stopping => {
                    self.with_eas(&config.id, |host| {
                        host.checked_at.insert(session.session_id.clone(), now)
                    });
                }
                _ => {}
            }
        }
        if reap {
            self.eas_reap(config, dir, &credential).await;
        }
    }

    fn eas_forget_session(&self, host_id: &str, session_id: &str) {
        self.with_eas(host_id, |host| {
            host.sessions.shift_remove(session_id);
            host.checked_at.remove(session_id);
        });
    }

    /// `refresh` for an Expo host: its catalog and running sessions. It ends
    /// devices itself, so sessions on devices it no longer lists go.
    async fn eas_refresh(&self, config: &EasConfig, dir: &Path) {
        let before: Vec<Value> = self.current()["sessions"]
            .as_array()
            .cloned()
            .unwrap_or_default();
        self.eas_drop_ended(config, dir).await;
        self.refresh_eas_platforms().await;
        let mut devices = eas::catalog(&config.id);
        devices.extend(
            self.with_eas(&config.id, |host| {
                host.sessions
                    .values()
                    .map(|session| {
                        json!({
                            "hostId": config.id,
                            "id": session.session_id,
                            "platform": session.platform,
                            "name": eas::platform_name(&session.platform),
                            "version": "Cloud",
                            "booted": true,
                            "physical": false,
                        })
                    })
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default(),
        );
        let mut state = self.current();
        if state["hostStatus"] == "disabled" || self.eas_config(&config.id).is_none() {
            return;
        }
        let ids: Vec<Value> = devices.iter().map(|d| d["id"].clone()).collect();
        if let Some(sessions) = state["sessions"].as_array_mut() {
            sessions.retain(|session| {
                session["hostId"] != config.id.as_str()
                    || !before.contains(session)
                    || ids.contains(&session["deviceId"])
            });
        }
        apply_list_result(
            &mut state,
            dir,
            &config.id,
            HubListResult {
                devices,
                detail: None,
                android_discovery_incomplete: false,
                incomplete_platforms: Vec::new(),
            },
        );
        self.publish(&mut state);
    }

    /// `list` for one Expo host.
    async fn list_eas_host(&self, host_id: &str, dir: &Path) {
        match self.eas_readiness(host_id, dir).await {
            Ok(config) => self.eas_refresh(&config, dir).await,
            Err(error) => {
                if self.eas_config(host_id).is_some()
                    && error["_tag"] == "DeviceHostUnavailableError"
                {
                    let detail = error["reason"].as_str().unwrap_or_default().to_owned();
                    self.set_host_status_for(host_id, "failed", Some(detail));
                }
            }
        }
    }

    /// `startSession`: records the session, asks Expo for it, waits for it
    /// and starts tracking it. A session Expo made is always either tracked
    /// or stopped.
    async fn eas_start_session(
        &self,
        config: &EasConfig,
        dir: &Path,
        device: &Value,
        workspace_root: Option<&str>,
    ) -> Result<String, Value> {
        let started_in = self
            .with_eas(&config.id, |host| host.epoch)
            .unwrap_or_default();
        let name = format!(
            "T3 Code · {} · {:06x}",
            config.label,
            rand::random::<u32>() & 0x00ff_ffff
        );
        // The credential comes first: without one, the answer is how to sign in.
        let credential = self.eas_api(config).await?;
        // reserveSlot
        let refused = self
            .with_eas(&config.id, |host| {
                let pending = host.unresolved.len() + host.uncertain.len();
                if host.sessions.len() + host.starting.len() + pending >= config.max_concurrent_sessions {
                    let max = config.max_concurrent_sessions;
                    let limit = format!(
                        "{} is limited to {max} running {}.",
                        config.label,
                        if max == 1 { "session" } else { "sessions" }
                    );
                    return Some(if pending > 0 {
                        format!(
                            "{limit} {} that failed to start or stop {} still being cleaned up; try again shortly.",
                            if pending == 1 { "A session".to_owned() } else { format!("{pending} sessions") },
                            if pending == 1 { "is" } else { "are" }
                        )
                    } else {
                        format!("{limit} Close one in the Device panel first.")
                    });
                }
                host.starting.insert(name.clone());
                None
            })
            .flatten();
        if let Some(reason) = refused {
            return Err(Self::eas_unavailable(config, reason));
        }
        let result = self
            .eas_start_reserved(
                config,
                dir,
                device,
                &credential,
                (&name, started_in),
                workspace_root,
            )
            .await;
        self.with_eas(&config.id, |host| host.starting.remove(&name));
        result
    }

    async fn eas_start_reserved(
        &self,
        config: &EasConfig,
        dir: &Path,
        device: &Value,
        credential: &expo_api::Credential,
        (name, started_in): (&str, u64),
        workspace_root: Option<&str>,
    ) -> Result<String, Value> {
        // Without a project set for the host, the thread's app config may name one.
        let app_id = match (config.app_id.clone(), workspace_root) {
            (Some(app_id), _) => Some(app_id),
            (None, Some(root)) if !root.is_empty() => {
                let root = PathBuf::from(root);
                tokio::task::spawn_blocking(move || eas_project_id::find_eas_project_id(&root))
                    .await
                    .ok()
                    .flatten()
            }
            (None, _) => None,
        };
        let Some(app_id) = app_id else {
            return Err(Self::eas_unavailable(
                config,
                "No Expo project to run the session under. Choose one for this host in Settings, or open the device from a thread whose project names extra.eas.projectId in app.json or its app config.",
            ));
        };
        let entry = eas::LedgerEntry {
            name: name.to_owned(),
            app_id: app_id.clone(),
            recorded_at: eas::now_ms(),
            refused: false,
        };
        // Recorded first: a session the ledger cannot hold is never started.
        let recorded = entry.clone();
        self.eas_write_ledger(config, dir, move |mut entries| {
            entries.push(recorded);
            entries
        })
        .await?;
        if self.with_eas(&config.id, |host| host.epoch) != Some(started_in) {
            self.eas_forget_entry(config, dir, name).await;
            return Err(Self::eas_unavailable(
                config,
                format!("{} was stopped while the session started.", config.label),
            ));
        }
        let platform = device["platform"].as_str().unwrap_or("ios").to_owned();
        let session_id = match expo_api::create(
            credential,
            &app_id,
            &platform,
            name,
            config.max_session_minutes,
            config.idle_minutes,
        )
        .await
        {
            Ok(id) => id,
            Err(error) => {
                // Unless Expo refused, it may have made the session before
                // failing to answer; unlisted, it stays counted for a while.
                let (found, _) = self
                    .eas_stop_recorded(config, dir, credential, std::slice::from_ref(&entry))
                    .await;
                if !found.contains(name) {
                    if error.rejected {
                        self.eas_mark_refused(config, dir, name).await;
                    } else {
                        let until = eas::now_ms() + UNCERTAIN_CREATE_MS;
                        self.with_eas(&config.id, |host| {
                            host.uncertain
                                .insert(name.to_owned(), (entry.clone(), until))
                        });
                    }
                }
                return Err(Self::eas_unavailable(config, error.message()));
            }
        };
        match self
            .eas_track_session(config, credential, &session_id, name, &platform, started_in)
            .await
        {
            Ok(id) => Ok(id),
            Err(error) => {
                if self
                    .eas_stop_session(config, dir, credential, &session_id, name)
                    .await
                    .is_err()
                {
                    // Still running and billed: owned, counted, and retried by listing.
                    self.with_eas(&config.id, |host| {
                        host.unresolved.insert(name.to_owned(), session_id.clone())
                    });
                }
                Err(error)
            }
        }
    }

    /// `trackSession`: waits for a created session (`awaitRunning`), reads
    /// its preview host's device id, and starts tracking it.
    async fn eas_track_session(
        &self,
        config: &EasConfig,
        credential: &expo_api::Credential,
        session_id: &str,
        name: &str,
        platform: &str,
        started_in: u64,
    ) -> Result<String, Value> {
        let boot_error = |reason: &str| {
            json!({
                "_tag": "DeviceBootError",
                "hostId": config.id,
                "deviceId": session_id,
                "reason": reason,
            })
        };
        let deadline = tokio::time::Instant::now() + EAS_START_TIMEOUT;
        let endpoints = loop {
            if tokio::time::Instant::now() >= deadline {
                return Err(boot_error("timeout"));
            }
            let mut attempt = 0;
            let session = loop {
                match expo_api::session(credential, session_id).await {
                    Ok(session) => break session,
                    Err(_) if attempt < 3 => {
                        attempt += 1;
                        tokio::time::sleep(Duration::from_secs(2)).await;
                    }
                    Err(error) => return Err(Self::eas_unavailable(config, error.message())),
                }
            };
            if session.status == "STOPPED" || session.status == "ERRORED" {
                return Err(boot_error("launch_failed"));
            }
            if session.status == "IN_PROGRESS" {
                if let Some(endpoints) = session.endpoints {
                    break endpoints;
                }
            }
            tokio::time::sleep(EAS_POLL_INTERVAL).await;
        };
        let Some((preview_url, preview_token)) = endpoints.preview.clone() else {
            return Err(boot_error("launch_failed"));
        };
        let origin = url::Url::parse(&preview_url)
            .map(|url| url.origin().ascii_serialization())
            .map_err(|_| boot_error("launch_failed"))?;
        let remote_device_id = self
            .eas_preview_device(&origin, &preview_token, platform)
            .await
            .ok_or_else(|| boot_error("launch_failed"))?;
        if self.with_eas(&config.id, |host| host.epoch) != Some(started_in) {
            return Err(Self::eas_unavailable(
                config,
                format!("{} was stopped while the session started.", config.label),
            ));
        }
        self.with_eas(&config.id, |host| {
            host.sessions.insert(
                session_id.to_owned(),
                eas::RunningSession {
                    session_id: session_id.to_owned(),
                    name: name.to_owned(),
                    platform: platform.to_owned(),
                    remote_device_id,
                    preview_origin: origin.clone(),
                    preview_token: preview_token.clone(),
                    agent_url: endpoints.agent_url.clone(),
                    agent_token: endpoints.agent_token.clone(),
                    stopping: false,
                },
            )
        });
        Ok(session_id.to_owned())
    }

    /// `previewDevice`: the device id the session's preview host serves.
    async fn eas_preview_device(
        &self,
        origin: &str,
        token: &str,
        platform: &str,
    ) -> Option<String> {
        let client = expo_api::client();
        for attempt in 0..4 {
            if attempt > 0 {
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
            let request = client
                .get(format!("{origin}{}/api", eas::preview_prefix(platform)))
                .bearer_auth(token)
                .timeout(EAS_PREVIEW_TIMEOUT)
                .send()
                .await;
            let Ok(response) = request else { continue };
            if !response.status().is_success() {
                continue;
            }
            let Ok(api) = response.json::<Value>().await else {
                continue;
            };
            let key = if platform == "ios" {
                "device"
            } else {
                "serial"
            };
            if let Some(id) = api[key].as_str().filter(|id| !id.is_empty()) {
                return Some(id.to_owned());
            }
        }
        None
    }

    /// `stopTracked`: until Expo confirms the stop, the session stays
    /// tracked, listed, counted, and retried by the next listing.
    async fn eas_stop_tracked(
        &self,
        config: &EasConfig,
        dir: &Path,
        credential: &expo_api::Credential,
        session_id: &str,
    ) -> Result<(), ()> {
        let name = self
            .with_eas(&config.id, |host| {
                host.sessions.get_mut(session_id).map(|session| {
                    session.stopping = true;
                    session.name.clone()
                })
            })
            .flatten()
            .ok_or(())?;
        self.eas_stop_session(config, dir, credential, session_id, &name)
            .await?;
        self.eas_forget_session(&config.id, session_id);
        Ok(())
    }

    /// `stopAll`: every session this host runs stopped (each is billed), and
    /// leftovers reaped.
    async fn eas_stop_all(&self, config: &EasConfig, dir: &Path) {
        self.with_eas(&config.id, |host| host.epoch += 1);
        let Ok(credential) = self.eas_api_for_stop(config).await else {
            return;
        };
        let sessions: Vec<String> = self
            .with_eas(&config.id, |host| host.sessions.keys().cloned().collect())
            .unwrap_or_default();
        for session_id in sessions {
            let _ = self
                .eas_stop_tracked(config, dir, &credential, &session_id)
                .await;
        }
        let unresolved: Vec<(String, String)> = self
            .with_eas(&config.id, |host| {
                host.unresolved
                    .iter()
                    .map(|(n, s)| (n.clone(), s.clone()))
                    .collect()
            })
            .unwrap_or_default();
        for (name, session_id) in unresolved {
            self.eas_retry_unresolved(config, dir, &credential, &name, &session_id)
                .await;
        }
        self.eas_reap(config, dir, &credential).await;
    }

    /// `open` and `shutdown` for an Expo host: a session started (as
    /// `boot`) or stopped (as `shutdown`).
    async fn eas_apply(&self, name: &str, input: &Value, dir: &Path) -> Reply {
        let host_id = input["hostId"].as_str().unwrap_or_default().to_owned();
        let id = string(input, "deviceId")?.to_owned();
        let platform = string(input, "platform")?.to_owned();
        let find = |state: &Value, wanted: &str| {
            state["devices"]
                .as_array()
                .and_then(|devices| {
                    devices
                        .iter()
                        .find(|d| d["hostId"] == host_id.as_str() && d["id"] == wanted)
                })
                .cloned()
        };
        if name == "shutdown" {
            let mut state = self.current();
            if let Some(sessions) = state["sessions"].as_array_mut() {
                sessions
                    .retain(|s| s["hostId"] != host_id.as_str() || s["deviceId"] != id.as_str());
            }
            self.publish(&mut state);
            let config = self.eas_readiness(&host_id, dir).await?;
            self.eas_shutdown(&config, dir, &id).await?;
            self.eas_refresh(&config, dir).await;
            return Ok(Value::Null);
        }
        let thread = string(input, "threadId")?.to_owned();
        let Some(config) = self.eas_config(&host_id) else {
            return Err(bad_host(&host_id));
        };
        let availability = match self.eas_credential(&config).await {
            Ok(_) => json!({"available": true}),
            Err(reason) => json!({"available": false, "reason": reason}),
        };
        if availability["available"] != true {
            return Err(json!({
                "_tag": "DevicePlatformUnavailableError",
                "hostId": host_id,
                "platform": platform,
                "reason": availability["reason"],
            }));
        }
        let config = self.eas_readiness(&host_id, dir).await?;
        self.eas_refresh(&config, dir).await;
        let mut device = find(&self.current(), &id)
            .ok_or_else(|| json!({"_tag":"DeviceNotFoundError","hostId":host_id,"deviceId":id}))?;
        if device["booted"] != true && input["boot"] != false {
            let mut booting = device.clone();
            booting["threadId"] = json!(thread);
            let mut state = self.current();
            let mut list: Vec<Value> = state["bootingDevices"]
                .as_array()
                .cloned()
                .unwrap_or_default();
            list.retain(|e| e["hostId"] != booting["hostId"] || e["id"] != booting["id"]);
            list.push(booting.clone());
            state["bootingDevices"] = Value::Array(list);
            self.publish(&mut state);
            let booted = self
                .eas_start_session(&config, dir, &device, input["workspaceRoot"].as_str())
                .await;
            let mut state = self.current();
            if let Some(list) = state["bootingDevices"].as_array_mut() {
                list.retain(|e| e["hostId"] != booting["hostId"] || e["id"] != booting["id"]);
            }
            self.publish(&mut state);
            let booted_id = booted?;
            self.eas_refresh(&config, dir).await;
            let state = self.current();
            device = find(&state, &booted_id)
                .or_else(|| find(&state, &id))
                .ok_or_else(
                    || json!({"_tag":"DeviceNotFoundError","hostId":host_id,"deviceId":booted_id}),
                )?;
        }
        if self.eas_config(&host_id).as_ref() != Some(&config) {
            return Err(json!({
                "_tag": "DeviceHostUnavailableError",
                "hostId": host_id,
                "hostLabel": config.label,
                "reason": "Host configuration changed. Retry the operation.",
            }));
        }
        if self
            .settings
            .internal()
            .map_err(|_| op_err("settings", "settings_failed"))?["enableDeviceSupport"]
            != true
        {
            return Err(json!({
                "_tag": "DeviceHostUnavailableError",
                "hostId": host_id,
                "hostLabel": config.label,
                "reason": "Device support was turned off while the device was opening.",
            }));
        }
        let active_id = device["id"].as_str().unwrap_or_default().to_owned();
        let session = json!({
            "threadId": thread,
            "hostId": host_id,
            "deviceId": active_id,
            "platform": device["platform"],
            "openedAt": timestamp(),
        });
        let mut state = self.current();
        let sessions = state["sessions"].as_array_mut().unwrap();
        sessions.retain(|s| {
            s["threadId"] != thread.as_str()
                || s["hostId"] != host_id.as_str()
                || s["deviceId"] != active_id.as_str()
        });
        sessions.push(session.clone());
        self.publish(&mut state);
        Ok(session)
    }

    /// `devices.shutdown` for an Expo session.
    async fn eas_shutdown(
        &self,
        config: &EasConfig,
        dir: &Path,
        device_id: &str,
    ) -> Result<(), Value> {
        let tracked = self
            .with_eas(&config.id, |host| host.sessions.contains_key(device_id))
            .unwrap_or(false);
        if !tracked {
            return Ok(());
        }
        let failed = || op_err("shutdown", "request_failed");
        let credential = self.eas_api_for_stop(config).await.map_err(|_| failed())?;
        self.eas_stop_tracked(config, dir, &credential, device_id)
            .await
            .map_err(|_| failed())
    }

    /// `devices.screenshot` for an Expo session, from its preview host.
    async fn eas_screenshot(&self, host_id: &str, device_id: &str) -> Result<Vec<u8>, Value> {
        let failed = || op_err("screenshot", "request_failed");
        let session = self
            .with_eas(host_id, |host| host.sessions.get(device_id).cloned())
            .flatten()
            .ok_or_else(failed)?;
        let response = expo_api::client()
            .post(format!(
                "{}{}/api/screenshot?device={}",
                session.preview_origin,
                eas::preview_prefix(&session.platform),
                agent_device::encode_uri_component(&session.remote_device_id)
            ))
            .bearer_auth(&session.preview_token)
            .timeout(EAS_PREVIEW_TIMEOUT)
            .send()
            .await
            .map_err(|_| failed())?;
        if !response.status().is_success() {
            return Err(failed());
        }
        response
            .bytes()
            .await
            .map(|b| b.to_vec())
            .map_err(|_| failed())
    }

    /// `agentEndpoint` for an Expo session: its own daemon, and the id
    /// agent-device selects its device by.
    fn eas_agent_endpoint(
        &self,
        host_id: &str,
        device_id: &str,
    ) -> Result<eas::RunningSession, Value> {
        let config = self.eas_config(host_id).ok_or_else(|| bad_host(host_id))?;
        self.with_eas(host_id, |host| host.sessions.get(device_id).cloned())
            .flatten()
            .ok_or_else(|| {
                Self::eas_unavailable(&config, "This Expo Simulator is no longer running.")
            })
    }

    fn simlock_config(&self, host_id: &str) -> Option<SimlockConfig> {
        self.simlock_hosts
            .lock()
            .unwrap()
            .iter()
            .find(|host| host.config.id == host_id)
            .map(|host| host.config.clone())
    }

    fn with_simlock<T>(&self, host_id: &str, f: impl FnOnce(&mut SimlockState) -> T) -> Option<T> {
        self.simlock_hosts
            .lock()
            .unwrap()
            .iter_mut()
            .find(|host| host.config.id == host_id)
            .map(f)
    }

    fn simlock_unavailable(config: &SimlockConfig, reason: impl Into<String>) -> Value {
        json!({
            "_tag": "DeviceHostUnavailableError",
            "hostId": config.id,
            "hostLabel": config.label,
            "reason": reason.into(),
        })
    }

    /// `currentToken`: the saved agent token for this gateway, else this
    /// machine's agent-device token when it points at the same gateway.
    async fn simlock_token(&self, config: &SimlockConfig) -> Result<String, Value> {
        let settings = self
            .settings
            .internal()
            .map_err(|_| op_err("settings", "settings_failed"))?;
        let present = |v: &Value| v.is_object() || v.as_str().is_some_and(|s| !s.is_empty());
        let same_origin = |a: &str, b: &str| match (url::Url::parse(a), url::Url::parse(b)) {
            (Ok(a), Ok(b)) => a.origin() == b.origin(),
            _ => false,
        };
        if present(&config.agent_token) {
            return device_token(&self.settings.resolver, &settings, &config.agent_token)
                .await
                .map_err(|reason| Self::simlock_unavailable(config, reason));
        }
        let machine_url = env::var("AGENT_DEVICE_DAEMON_BASE_URL").ok().and_then(|v| {
            v.trim()
                .trim_end_matches('/')
                .strip_suffix(simlock::AGENT_DEVICE_PATH)
                .filter(|url| url::Url::parse(url).is_ok())
                .map(str::to_owned)
        });
        let machine_token = env::var("AGENT_DEVICE_DAEMON_AUTH_TOKEN")
            .ok()
            .map(|s| s.trim().to_owned())
            .filter(|s| !s.is_empty());
        match (machine_url, machine_token) {
            (Some(url), Some(token)) if same_origin(&url, &config.gateway_url) => Ok(token),
            _ => Err(Self::simlock_unavailable(
                config,
                format!("Add an agent token for {} in Settings.", config.label),
            )),
        }
    }

    /// `loadCatalog`: the pool's devices (cached for a minute when every
    /// platform has some), and the host's platforms from them or the failure.
    async fn simlock_load_catalog(&self, host_id: &str, _dir: &Path) -> Result<Vec<Value>, Value> {
        let Some(config) = self.simlock_config(host_id) else {
            return Err(bad_host(host_id));
        };
        let cached = self
            .with_simlock(host_id, |host| {
                host.catalog
                    .as_ref()
                    .filter(|(at, _)| at.elapsed() < simlock::CATALOG_TTL)
                    .map(|(_, devices)| devices.clone())
            })
            .flatten();
        let result: Result<Vec<Value>, Value> = match cached {
            Some(devices) => Ok(devices),
            None => {
                async {
                    let token = self.simlock_token(&config).await?;
                    let gateway = config.gateway();
                    let answer = simlock::request(&gateway, &token, "GET", "/v1/catalog", None)
                        .await
                        .map_err(|error| {
                            Self::simlock_unavailable(
                                &config,
                                simlock::describe_gateway_failure(&error, &gateway, &config.label),
                            )
                        })?;
                    let by_platform = simlock::catalog_devices(&answer).ok_or_else(|| {
                        Self::simlock_unavailable(
                            &config,
                            simlock::describe_gateway_failure(
                                &simlock::GatewayError::Unreadable,
                                &gateway,
                                &config.label,
                            ),
                        )
                    })?;
                    let devices: Vec<Value> =
                        by_platform.iter().flatten().flatten().cloned().collect();
                    // A platform with nothing to offer is usually its workers reconnecting.
                    let keep = !devices.is_empty()
                        && by_platform
                            .iter()
                            .all(|entry| entry.as_ref().is_none_or(|d| !d.is_empty()));
                    self.with_simlock(host_id, |host| {
                        host.catalog = keep.then(|| (Instant::now(), devices.clone()));
                    });
                    Ok(devices)
                }
                .await
            }
        };
        let platforms = match &result {
            Ok(devices) => ["ios", "android"]
                .map(|platform| {
                    if devices.iter().any(|d| d["platform"] == platform) {
                        json!({"platform": platform, "available": true})
                    } else {
                        json!({
                            "platform": platform,
                            "available": false,
                            "reason": format!(
                                "The pool has no {} devices.",
                                if platform == "ios" { "iOS" } else { "Android" }
                            ),
                        })
                    }
                })
                .to_vec(),
            Err(error) => ["ios", "android"]
                .map(|platform| {
                    json!({"platform": platform, "available": false, "reason": error["reason"]})
                })
                .to_vec(),
        };
        self.with_simlock(host_id, |host| host.platforms = Value::Array(platforms));
        result
    }

    /// `platformAvailability` for a Simlock host.
    async fn simlock_platform(&self, host_id: &str, platform: &str, dir: &Path) -> Value {
        let _ = self.simlock_load_catalog(host_id, dir).await;
        self.with_simlock(host_id, |host| {
            host.platforms
                .as_array()
                .and_then(|list| list.iter().find(|p| p["platform"] == platform))
                .cloned()
        })
        .flatten()
        .unwrap_or_else(|| json!({"platform": platform, "available": false}))
    }

    /// `ensureReady` and `readiness` for a Simlock host: this machine's
    /// agent-device CLI, installed on first use.
    async fn simlock_readiness(
        &self,
        host_id: &str,
        dir: &Path,
    ) -> Result<(PathBuf, PathBuf), Value> {
        let Some(config) = self.simlock_config(host_id) else {
            return Err(bad_host(host_id));
        };
        let enabled = self
            .settings
            .internal()
            .map_err(|_| op_err("settings", "settings_failed"))?["enableDeviceSupport"]
            == true;
        if !enabled {
            return Err(self.disabled_for(host_id));
        }
        if let Some(cli) = self
            .with_simlock(host_id, |host| host.cli.clone())
            .flatten()
        {
            if self.current()["hostStatuses"][host_id]["status"] != "ready" {
                self.set_host_status_for(host_id, "ready", None);
            }
            return Ok(cli);
        }
        // A missing token or unreachable gateway does not stop readiness:
        // listing and opening devices say what is wrong instead.
        self.set_host_status_for(host_id, "installing", None);
        let cli = async {
            let node = agent_device::node_executable().ok_or_else(|| {
                (
                    "installing agent tools",
                    Some("Device automation requires Node.js. Install Node.js and make sure node is on PATH, then retry.".to_owned()),
                )
            })?;
            let entry = install(dir, AGENT)
                .await
                .map_err(|_| ("installing agent tools", None))?;
            Ok::<_, (&str, Option<String>)>((node, entry))
        }
        .await;
        match cli {
            Ok(cli) => {
                self.with_simlock(host_id, |host| host.cli = Some(cli.clone()));
                if self.current()["hostStatuses"][host_id]["status"] != "ready" {
                    self.set_host_status_for(host_id, "ready", None);
                }
                Ok(cli)
            }
            Err((step, reason)) => {
                let reason =
                    reason.unwrap_or_else(|| format!("Device support failed during {step}."));
                self.set_host_status_for(host_id, "failed", Some(reason.clone()));
                Err(Self::simlock_unavailable(&config, reason))
            }
        }
    }

    /// `devices.list` for a Simlock host: the pool's models, and the leases held.
    async fn simlock_list(&self, host_id: &str, dir: &Path) -> Result<HubListResult, Value> {
        let available = self
            .simlock_load_catalog(host_id, dir)
            .await
            .map_err(|_| op_err("list", "request_failed"))?;
        let mut devices: Vec<Value> = available
            .into_iter()
            .map(|mut device| {
                device["hostId"] = json!(host_id);
                device
            })
            .collect();
        let leases: Vec<Lease> = self
            .with_simlock(host_id, |host| host.leases.values().cloned().collect())
            .unwrap_or_default();
        devices.extend(leases.into_iter().map(|lease| {
            json!({
                "hostId": host_id,
                "id": lease.lease_id,
                "platform": lease.platform,
                "name": lease.name,
                "version": lease.version,
                "booted": true,
                "physical": false,
            })
        }));
        Ok(HubListResult {
            devices,
            detail: None,
            android_discovery_incomplete: false,
            incomplete_platforms: Vec::new(),
        })
    }

    /// `refresh` for a Simlock host. It ends devices itself (a lease can end
    /// at the gateway), so sessions from before the listing on devices it no
    /// longer lists go.
    async fn simlock_refresh(&self, host_id: &str, dir: &Path) -> Result<(), Value> {
        let before: Vec<Value> = self.current()["sessions"]
            .as_array()
            .cloned()
            .unwrap_or_default();
        let listed = self.simlock_list(host_id, dir).await?;
        if self.simlock_config(host_id).is_none() {
            return Ok(());
        }
        let mut state = self.current();
        if state["hostStatus"] == "disabled" {
            return Ok(());
        }
        let ids: Vec<Value> = listed.devices.iter().map(|d| d["id"].clone()).collect();
        if let Some(sessions) = state["sessions"].as_array_mut() {
            sessions.retain(|session| {
                session["hostId"] != host_id
                    || !before.contains(session)
                    || ids.contains(&session["deviceId"])
            });
        }
        apply_list_result(&mut state, dir, host_id, listed);
        self.publish(&mut state);
        Ok(())
    }

    /// `list` for one Simlock host, or its failure as its status.
    async fn list_simlock_host(&self, host_id: &str, dir: &Path) {
        let result = match self.simlock_readiness(host_id, dir).await {
            Ok(_) => self.simlock_refresh(host_id, dir).await,
            Err(error) => Err(error),
        };
        if let Err(error) = result {
            let detail = if error["_tag"] == "DeviceHostUnavailableError" {
                error["reason"].as_str().unwrap_or_default().to_owned()
            } else {
                crate::mcp_oauth::format_device_error(&error)
            };
            self.set_host_status_for(host_id, "failed", Some(detail));
        }
    }

    /// `openLease`: a lease of one of the pool's models, through this
    /// machine's agent-device CLI and the gateway's agent-device front.
    async fn simlock_open_lease(
        &self,
        host_id: &str,
        device: &Value,
        workspace_root: Option<&str>,
        dir: &Path,
    ) -> Result<String, Value> {
        let Some(config) = self.simlock_config(host_id) else {
            return Err(bad_host(host_id));
        };
        let device_id = device["id"].as_str().unwrap_or_default().to_owned();
        let boot_error = |reason: &str| {
            json!({
                "_tag": "DeviceBootError",
                "hostId": host_id,
                "deviceId": device_id,
                "reason": "launch_failed",
                "diagnostics": simlock::truncate(reason, 1_997),
            })
        };
        let Some((platform, model)) = simlock::parse_catalog_id(&device_id) else {
            return Err(boot_error(
                "This Simlock device is no longer leased. Open it again from the list.",
            ));
        };
        // reserveSlot
        let reserved = self.with_simlock(host_id, |host| {
            if host.leases.len() + host.starting >= host.config.max_leases {
                false
            } else {
                host.starting += 1;
                true
            }
        });
        if reserved != Some(true) {
            let max = config.max_leases;
            return Err(Self::simlock_unavailable(
                &config,
                format!(
                    "{} is limited to {max} {} at once. Close one in the Device panel first.",
                    config.label,
                    if max == 1 { "device" } else { "devices" }
                ),
            ));
        }
        let opened = async {
            let cli = self
                .with_simlock(host_id, |host| host.cli.clone())
                .flatten()
                .ok_or_else(|| {
                    Self::simlock_unavailable(&config, format!("{} is not ready.", config.label))
                })?;
            let suffix = simlock::lease_suffix();
            let session = format!("t3-{suffix}");
            let lease_dir = dir
                .join("device")
                .join("simlock")
                .join(host_id)
                .join(&suffix);
            let config_path = lease_dir.join("agent-device.json");
            let token = self.simlock_token(&config).await?;
            agent_device::write_config(
                &config_path,
                &AgentEndpoint {
                    base_url: config.agent_device_url(),
                    token: token.clone(),
                    entry: PathBuf::new(),
                    state_dir: Some(lease_dir.clone()),
                },
            )
            .await
            .map_err(|_| {
                Self::simlock_unavailable(&config, "Could not write the agent-device config.")
            })?;
            // agent-device ties installs to the directory that opened the session.
            let install_directory = match workspace_root {
                Some(root) => std::fs::canonicalize(root).unwrap_or_else(|_| lease_dir.clone()),
                None => lease_dir.clone(),
            };
            let stops_at_open = self
                .with_simlock(host_id, |host| host.stops)
                .unwrap_or_default();
            let attempt = async {
                simlock::lease_command(
                    &cli,
                    &session,
                    &config_path,
                    &[
                        "connect",
                        "proxy",
                        "--daemon-base-url",
                        &config.agent_device_url(),
                    ],
                )
                .await
                .map_err(|reason| Self::simlock_unavailable(&config, reason))?;
                simlock::agent_device(
                    &cli,
                    &session,
                    &config_path,
                    &["open", "--platform", &platform, "--device", &model],
                    Some(&install_directory),
                    simlock::OPEN_TIMEOUT,
                )
                .await
                .map_err(|reason| Self::simlock_unavailable(&config, reason))?;
                let record = tokio::fs::read_to_string(
                    lease_dir
                        .join("remote-connections")
                        .join(format!("{session}.json")),
                )
                .await
                .ok()
                .and_then(|text| serde_json::from_str::<Value>(&text).ok())
                .and_then(|record| record["leaseId"].as_str().map(str::to_owned))
                .ok_or_else(|| {
                    Self::simlock_unavailable(&config, "agent-device recorded no Simlock lease.")
                })?;
                let now = Instant::now();
                let lease = Lease {
                    lease_id: record.clone(),
                    platform: platform.clone(),
                    name: model.clone(),
                    version: device["version"].as_str().unwrap_or_default().to_owned(),
                    session: session.clone(),
                    dir: lease_dir.clone(),
                    config_path: config_path.clone(),
                    install_directory: install_directory.to_string_lossy().into_owned(),
                    token: token.clone(),
                    deadline: now + Duration::from_secs(config.max_lease_minutes * 60),
                    next_renewal: now
                        + simlock::RENEW_INTERVAL
                            .min(Duration::from_secs(config.max_lease_minutes * 60)),
                };
                let kept = self.with_simlock(host_id, |host| {
                    if host.stops != stops_at_open {
                        return false;
                    }
                    host.leases.insert(lease.lease_id.clone(), lease);
                    true
                });
                if kept != Some(true) {
                    return Err(Self::simlock_unavailable(
                        &config,
                        "Device support stopped while the device was opening.",
                    ));
                }
                Ok(record)
            }
            .await;
            if attempt.is_err() {
                // A failed open may still hold a lease: close the session either way.
                let _ = simlock::lease_command(&cli, &session, &config_path, &["close"]).await;
                let _ = tokio::fs::remove_dir_all(&lease_dir).await;
            }
            attempt
        }
        .await;
        self.with_simlock(host_id, |host| {
            host.starting = host.starting.saturating_sub(1)
        });
        opened.map_err(|error| {
            if error["_tag"] == "DeviceHostUnavailableError" {
                boot_error(error["reason"].as_str().unwrap_or_default())
            } else {
                error
            }
        })
    }

    /// `release`: ends a lease through its session, or over HTTP when the
    /// session is gone, then forgets it.
    async fn simlock_release(&self, host_id: &str, lease: Lease) {
        let Some(config) = self.simlock_config(host_id).or_else(|| None) else {
            let _ = tokio::fs::remove_dir_all(&lease.dir).await;
            return;
        };
        self.simlock_release_with(&config, lease).await;
    }

    async fn simlock_release_with(&self, config: &SimlockConfig, lease: Lease) {
        let cli = self
            .with_simlock(&config.id, |host| host.cli.clone())
            .flatten();
        let closed = match &cli {
            Some(cli) => {
                simlock::lease_command(cli, &lease.session, &lease.config_path, &["close"])
                    .await
                    .is_ok()
            }
            None => false,
        };
        if !closed {
            let route = format!(
                "/v1/leases/{}",
                agent_device::encode_uri_component(&lease.lease_id)
            );
            if simlock::request(&config.gateway(), &lease.token, "DELETE", &route, None)
                .await
                .is_err()
            {
                tracing::warn!(lease = %lease.lease_id, "Could not release Simlock lease");
            }
        }
        self.with_simlock(&config.id, |host| host.leases.shift_remove(&lease.lease_id));
        let _ = tokio::fs::remove_dir_all(&lease.dir).await;
    }

    /// `stopAll`: every lease back to the pool.
    async fn simlock_release_all(&self, config: SimlockConfig) {
        let cli_and_leases = {
            let mut hosts = self.simlock_hosts.lock().unwrap();
            let host = hosts.iter_mut().find(|host| host.config.id == config.id);
            host.map(|host| {
                host.stops += 1;
                host.leases.values().cloned().collect::<Vec<_>>()
            })
        };
        // A removed host's leases are still released with the token they were opened with.
        let leases = cli_and_leases.unwrap_or_default();
        for lease in leases {
            self.simlock_release_with(&config, lease).await;
        }
    }

    /// `shutdown` for a Simlock device: its lease released, if this server holds it.
    async fn simlock_shutdown(&self, host_id: &str, device_id: &str) {
        let lease = self
            .with_simlock(host_id, |host| host.leases.get(device_id).cloned())
            .flatten();
        if let Some(lease) = lease {
            self.simlock_release(host_id, lease).await;
        }
    }

    /// `screenshot` for a Simlock lease, through its session.
    async fn simlock_screenshot(&self, host_id: &str, device_id: &str) -> Result<Vec<u8>, Value> {
        let screenshot_failed = || op_err("screenshot", "request_failed");
        let (lease, cli) = self
            .with_simlock(host_id, |host| {
                (host.leases.get(device_id).cloned(), host.cli.clone())
            })
            .ok_or_else(screenshot_failed)?;
        let (Some(lease), Some(cli)) = (lease, cli) else {
            return Err(screenshot_failed());
        };
        let file = lease.dir.join(format!(
            "screenshot-{:x}.png",
            rand::random::<u32>() & 0x00ff_ffff
        ));
        let file_text = file.to_string_lossy().into_owned();
        let result = match simlock::lease_command(
            &cli,
            &lease.session,
            &lease.config_path,
            &["screenshot", &file_text],
        )
        .await
        {
            Ok(_) => tokio::fs::read(&file)
                .await
                .map_err(|_| screenshot_failed()),
            Err(_) => Err(screenshot_failed()),
        };
        let _ = tokio::fs::remove_file(&file).await;
        result
    }

    /// The renewals due, as each lease's keep-alive does: renewed every two
    /// minutes until Simlock ends it (404) or it reaches its maximum age.
    async fn renew_simlock_leases(&self) {
        let now = Instant::now();
        let due: Vec<(SimlockConfig, Lease)> = self
            .simlock_hosts
            .lock()
            .unwrap()
            .iter()
            .flat_map(|host| {
                host.leases
                    .values()
                    .filter(|lease| lease.next_renewal <= now)
                    .map(|lease| (host.config.clone(), lease.clone()))
                    .collect::<Vec<_>>()
            })
            .collect();
        for (config, lease) in due {
            if Instant::now() >= lease.deadline {
                tracing::info!(lease = %lease.lease_id, "Releasing Simlock lease at its maximum age");
                self.simlock_release_with(&config, lease).await;
                continue;
            }
            let route = format!(
                "/v1/leases/{}/renew",
                agent_device::encode_uri_component(&lease.lease_id)
            );
            match simlock::request(&config.gateway(), &lease.token, "POST", &route, None).await {
                Err(simlock::GatewayError::Status(404)) => {
                    // Simlock ended it (lease_lost, an agent's close, or its TTL): stop listing it.
                    self.with_simlock(&config.id, |host| host.leases.shift_remove(&lease.lease_id));
                    let _ = tokio::fs::remove_dir_all(&lease.dir).await;
                    continue;
                }
                Err(error) => {
                    tracing::warn!(lease = %lease.lease_id, ?error, "Could not renew Simlock lease")
                }
                Ok(_) => {}
            }
            let left = lease.deadline.saturating_duration_since(Instant::now());
            self.with_simlock(&config.id, |host| {
                if let Some(held) = host.leases.get_mut(&lease.lease_id) {
                    held.next_renewal = Instant::now() + simlock::RENEW_INTERVAL.min(left);
                }
            });
        }
    }

    /// `open` and `shutdown` for a Simlock host: a lease taken (as `boot`)
    /// or returned (as `shutdown`).
    async fn simlock_apply(&self, name: &str, input: &Value, dir: &Path) -> Reply {
        let host_id = input["hostId"].as_str().unwrap_or_default().to_owned();
        let id = string(input, "deviceId")?.to_owned();
        let platform = string(input, "platform")?.to_owned();
        let find = |state: &Value, wanted: &str| {
            state["devices"]
                .as_array()
                .and_then(|devices| {
                    devices
                        .iter()
                        .find(|d| d["hostId"] == host_id.as_str() && d["id"] == wanted)
                })
                .cloned()
        };
        if name == "shutdown" {
            // Sessions retire before the lease ends.
            let mut state = self.current();
            if let Some(sessions) = state["sessions"].as_array_mut() {
                sessions
                    .retain(|s| s["hostId"] != host_id.as_str() || s["deviceId"] != id.as_str());
            }
            self.publish(&mut state);
            self.simlock_readiness(&host_id, dir).await?;
            self.simlock_shutdown(&host_id, &id).await;
            let mut state = self.current();
            if let Some(devices) = state["devices"].as_array_mut() {
                for d in devices.iter_mut() {
                    if d["hostId"] == host_id.as_str() && d["id"] == id.as_str() {
                        d["booted"] = json!(false);
                    }
                }
            }
            self.publish(&mut state);
            let _ = self.simlock_refresh(&host_id, dir).await;
            return Ok(Value::Null);
        }
        let thread = string(input, "threadId")?.to_owned();
        let availability = self.simlock_platform(&host_id, &platform, dir).await;
        if availability["available"] != true {
            return Err(json!({
                "_tag": "DevicePlatformUnavailableError",
                "hostId": host_id,
                "platform": platform,
                "reason": availability.get("reason").cloned().unwrap_or(json!("Platform toolchain missing.")),
            }));
        }
        self.simlock_readiness(&host_id, dir).await?;
        self.simlock_refresh(&host_id, dir).await?;
        let mut device = find(&self.current(), &id)
            .ok_or_else(|| json!({"_tag":"DeviceNotFoundError","hostId":host_id,"deviceId":id}))?;
        if device["booted"] != true && input["boot"] != false {
            let mut booting = device.clone();
            booting["threadId"] = json!(thread);
            let mut state = self.current();
            let mut list: Vec<Value> = state["bootingDevices"]
                .as_array()
                .cloned()
                .unwrap_or_default();
            list.retain(|e| e["hostId"] != booting["hostId"] || e["id"] != booting["id"]);
            list.push(booting.clone());
            state["bootingDevices"] = Value::Array(list);
            self.publish(&mut state);
            let booted = self
                .simlock_open_lease(&host_id, &device, input["workspaceRoot"].as_str(), dir)
                .await;
            let mut state = self.current();
            if let Some(list) = state["bootingDevices"].as_array_mut() {
                list.retain(|e| e["hostId"] != booting["hostId"] || e["id"] != booting["id"]);
            }
            self.publish(&mut state);
            let booted_id = booted?;
            self.simlock_refresh(&host_id, dir).await?;
            let state = self.current();
            device = find(&state, &booted_id)
                .or_else(|| find(&state, &id))
                .ok_or_else(
                    || json!({"_tag":"DeviceNotFoundError","hostId":host_id,"deviceId":booted_id}),
                )?;
        }
        if self.simlock_config(&host_id).is_none() {
            return Err(json!({
                "_tag": "DeviceHostUnavailableError",
                "hostId": host_id,
                "reason": "Host configuration changed. Retry the operation.",
            }));
        }
        if self
            .settings
            .internal()
            .map_err(|_| op_err("settings", "settings_failed"))?["enableDeviceSupport"]
            != true
        {
            return Err(json!({
                "_tag": "DeviceHostUnavailableError",
                "hostId": host_id,
                "hostLabel": self.host_label(&host_id),
                "reason": "Device support was turned off while the device was opening.",
            }));
        }
        let active_id = device["id"].as_str().unwrap_or_default().to_owned();
        let session = json!({
            "threadId": thread,
            "hostId": host_id,
            "deviceId": active_id,
            "platform": device["platform"],
            "openedAt": timestamp(),
        });
        let mut state = self.current();
        let sessions = state["sessions"].as_array_mut().unwrap();
        sessions.retain(|s| {
            s["threadId"] != thread.as_str()
                || s["hostId"] != host_id.as_str()
                || s["deviceId"] != active_id.as_str()
        });
        sessions.push(session.clone());
        self.publish(&mut state);
        Ok(session)
    }

    /// `agentEndpoint` for a Simlock lease: its own session at the gateway.
    fn simlock_agent_endpoint(
        &self,
        host_id: &str,
        device_id: &str,
    ) -> Result<(Lease, String), Value> {
        let config = self
            .simlock_config(host_id)
            .ok_or_else(|| bad_host(host_id))?;
        let lease = self
            .with_simlock(host_id, |host| host.leases.get(device_id).cloned())
            .flatten()
            .ok_or_else(|| Self::simlock_unavailable(&config, "This Simlock lease has ended."))?;
        Ok((lease, config.agent_device_url()))
    }

    /// `inspect` for the SSH hosts: each one's probe, or the reference's
    /// note that its versions could not be checked.
    async fn inspect_ssh_hosts(&self, dir: &Path) {
        let hosts: Vec<SshConfig> = self
            .ssh_hosts
            .lock()
            .unwrap()
            .iter()
            .map(|(config, _)| config.clone())
            .collect();
        let probes = futures_util::stream::iter(hosts)
            .map(|config| async move {
                let result = ssh_host::probe(&config, &ssh_host::owner(dir, &config.id)).await;
                (config, result)
            })
            .buffer_unordered(4);
        futures_util::pin_mut!(probes);
        while let Some((config, result)) = probes.next().await {
            {
                let mut hosts = self.ssh_hosts.lock().unwrap();
                let Some((_, summary)) = hosts.iter_mut().find(|(c, _)| *c == config) else {
                    continue;
                };
                match result {
                    Ok(probed) => *summary = probed,
                    Err(_) => {
                        summary["toolInspectionError"] = json!("Cannot check versions. Reconnect the host and check again. Installed tools have not been changed.");
                    }
                }
            }
            let mut state = self.current();
            self.publish(&mut state);
        }
    }

    /// `reconcile`: the SSH hosts in settings that are not this machine.
    /// A removed or changed host goes (with its devices, sessions, status
    /// and agent config), and a new one starts unprobed.
    pub(crate) async fn reconcile_hosts(&self, dir: &Path) {
        let _lock = self.reconcile_lock.lock().await;
        let Ok(settings) = self.settings.internal() else {
            return;
        };
        let mut next: Vec<Value> = settings["deviceHosts"]
            .as_array()
            .cloned()
            .unwrap_or_default();
        super::local_ssh::retain_remote(&mut next).await;
        let next: Vec<SshConfig> = next.iter().filter_map(SshConfig::from_settings).collect();
        let removed_hosts: Vec<SshConfig> = {
            let mut hosts = self.ssh_hosts.lock().unwrap();
            let removed = hosts
                .iter()
                .filter(|(config, _)| !next.contains(config))
                .map(|(config, _)| config.clone())
                .collect::<Vec<_>>();
            hosts.retain(|(config, _)| next.contains(config));
            removed
        };
        let removed: Vec<String> = removed_hosts
            .iter()
            .map(|config| config.id.clone())
            .collect();
        if !removed.is_empty() {
            self.refresh_hosts(dir, &removed);
            // Stop old writers before deleting config files or publishing replacements.
            for config in &removed_hosts {
                self.ssh_stop(config, dir).await;
                let _ = tokio::fs::remove_file(agent_device::config_path(dir, &config.id)).await;
            }
        }
        let added = {
            let mut hosts = self.ssh_hosts.lock().unwrap();
            let mut added = false;
            for config in next {
                if hosts.iter().any(|(c, _)| c.id == config.id) {
                    continue;
                }
                added = true;
                let summary = config.unprobed_summary();
                hosts.push((config, summary));
            }
            added
        };
        // Expo hosts, then Simlock hosts (alpha), follow the SSH hosts; an id two lists share
        // stays the first one's.
        let mut ssh_ids: HashSet<String> = self
            .ssh_hosts
            .lock()
            .unwrap()
            .iter()
            .map(|(config, _)| config.id.clone())
            .collect();
        let mut next_eas: Vec<EasConfig> = Vec::new();
        for config in settings["easDeviceHosts"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(EasConfig::from_settings)
        {
            if !ssh_ids.contains(&config.id) && !next_eas.iter().any(|c| c.id == config.id) {
                next_eas.push(config);
            }
        }
        // Closing an Expo host stops its sessions, so a changed limit or token never leaves a
        // billed session behind.
        let leaving: Vec<EasConfig> = self
            .eas_hosts
            .lock()
            .unwrap()
            .iter()
            .filter(|host| !next_eas.iter().any(|c| c.raw == host.config.raw))
            .map(|host| host.config.clone())
            .collect();
        for config in &leaving {
            self.eas_stop_all(config, dir).await;
        }
        let removed_eas: Vec<String> = {
            let mut hosts = self.eas_hosts.lock().unwrap();
            let removed = hosts
                .iter()
                .filter(|host| !next_eas.iter().any(|c| c.raw == host.config.raw))
                .map(|host| host.config.id.clone())
                .collect::<Vec<_>>();
            hosts.retain(|host| next_eas.iter().any(|c| c.raw == host.config.raw));
            removed
        };
        if !removed_eas.is_empty() {
            self.refresh_hosts(dir, &removed_eas);
        }
        let mut added_eas = false;
        {
            let mut hosts = self.eas_hosts.lock().unwrap();
            for config in &next_eas {
                if hosts.iter().any(|host| host.config.id == config.id) {
                    continue;
                }
                added_eas = true;
                hosts.push(EasState::new(config.clone()));
            }
        }
        self.refresh_eas_platforms().await;
        ssh_ids.extend(next_eas.iter().map(|config| config.id.clone()));
        let mut next_simlock: Vec<SimlockConfig> = Vec::new();
        if settings["simlockDeviceHostsEnabled"] == true {
            for config in settings["simlockDeviceHosts"]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(SimlockConfig::from_settings)
            {
                if !ssh_ids.contains(&config.id) && !next_simlock.iter().any(|c| c.id == config.id)
                {
                    next_simlock.push(config);
                }
            }
        }
        let removed_simlock: Vec<SimlockConfig> = {
            let mut hosts = self.simlock_hosts.lock().unwrap();
            let removed = hosts
                .iter()
                .filter(|host| !next_simlock.iter().any(|c| c.raw == host.config.raw))
                .map(|host| host.config.clone())
                .collect::<Vec<_>>();
            hosts.retain(|host| next_simlock.iter().any(|c| c.raw == host.config.raw));
            removed
        };
        let removed_simlock_ids: Vec<String> = removed_simlock
            .iter()
            .map(|config| config.id.clone())
            .collect();
        if !removed_simlock_ids.is_empty() {
            self.refresh_hosts(dir, &removed_simlock_ids);
            for config in &removed_simlock {
                // Closing a Simlock host returns its leases to the pool.
                self.simlock_release_all(config.clone()).await;
                let _ = tokio::fs::remove_file(agent_device::config_path(dir, &config.id)).await;
            }
        }
        let mut added_simlock = Vec::new();
        {
            let mut hosts = self.simlock_hosts.lock().unwrap();
            for config in next_simlock {
                if hosts.iter().any(|host| host.config.id == config.id) {
                    continue;
                }
                added_simlock.push(config.id.clone());
                hosts.push(SimlockState::new(config));
            }
        }
        // Ask the gateway now, so a refused token shows in Settings before anyone opens a device.
        if settings["enableDeviceSupport"] == true {
            for id in &added_simlock {
                let _ = self.simlock_load_catalog(id, dir).await;
                let mut state = self.current();
                self.publish(&mut state);
            }
        }
        let mut removed = removed;
        removed.extend(removed_eas);
        removed.extend(removed_simlock_ids);
        if added || added_eas || !added_simlock.is_empty() || !removed.is_empty() {
            self.refresh_hosts(dir, &removed);
        }
    }

    /// `refreshHosts`: publishes the hosts, without what removed hosts had.
    fn refresh_hosts(&self, _dir: &Path, removed: &[String]) {
        let mut state = self.current();
        let gone = |id: &Value| {
            id.as_str()
                .is_some_and(|id| removed.iter().any(|r| r == id))
        };
        if let Some(statuses) = state["hostStatuses"].as_object_mut() {
            statuses.retain(|id, _| !removed.iter().any(|r| r == id));
        }
        for key in ["devices", "sessions"] {
            if let Some(list) = state[key].as_array_mut() {
                list.retain(|entry| !gone(&entry["hostId"]));
            }
        }
        self.publish(&mut state);
    }

    /// Reconciles the SSH hosts now and on every settings change, as the
    /// reference's settings subscription does.
    ///
    /// It also supervises connected hosts, as each reference host's connection
    /// does: a tunnel that exits, or three missed health probes 10 seconds
    /// apart, reconnects, backing off from 1 to 30 seconds between attempts.
    pub(crate) async fn watch_hosts(&self, dir: &Path) {
        let mut changes = self.settings.subscribe();
        self.reconcile_hosts(dir).await;
        let mut tick = tokio::time::interval(Duration::from_secs(1));
        let mut supervision = Supervision::default();
        loop {
            tokio::select! {
                change = changes.recv() => match change {
                    Ok(_) | Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                        self.reconcile_hosts(dir).await
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => return,
                },
                _ = tick.tick() => self.supervise_ssh_hosts(dir, &mut supervision).await,
            }
        }
    }

    /// One second of SSH host supervision.
    async fn supervise_ssh_hosts(&self, dir: &Path, supervision: &mut Supervision) {
        self.renew_simlock_leases().await;
        supervision.ticks += 1;
        let probe = supervision.ticks.is_multiple_of(10);
        let mut lost = Vec::new();
        let mut endpoints = Vec::new();
        {
            let mut ready = self.ssh_ready.lock().await;
            for (id, entry) in ready.iter_mut() {
                if !entry.tunnel.alive() {
                    lost.push(id.clone());
                } else if probe {
                    endpoints.push((
                        id.clone(),
                        entry.ctx.origin.clone(),
                        entry.agent.as_ref().map(|agent| agent.base_url.clone()),
                    ));
                }
            }
            supervision.misses.retain(|id, _| ready.contains_key(id));
        }
        let client = reqwest::Client::builder()
            .no_proxy()
            .timeout(Duration::from_secs(5))
            .build()
            .unwrap();
        for (id, hub, daemon) in endpoints {
            let answers = |url: String| {
                let client = client.clone();
                async move {
                    client
                        .get(url)
                        .send()
                        .await
                        .is_ok_and(|response| response.status().as_u16() == 200)
                }
            };
            let alive = answers(format!("{hub}/readyz")).await
                && match daemon {
                    Some(daemon) => answers(format!("{daemon}/health")).await,
                    None => true,
                };
            let misses = supervision.misses.entry(id.clone()).or_default();
            *misses = if alive { 0 } else { *misses + 1 };
            // A loaded host can miss a probe or two while still working.
            if *misses >= 3 {
                lost.push(id);
            }
        }
        for id in lost {
            supervision.misses.remove(&id);
            self.ssh_ready.lock().await.remove(&id);
            if !self.ssh_activated.lock().unwrap().contains(&id) {
                continue;
            }
            self.set_host_status_for(&id, "starting", Some("Reconnecting to device host…".into()));
            supervision.reconnecting.insert(
                id,
                (
                    tokio::time::Instant::now() + Duration::from_secs(1),
                    Duration::from_secs(1),
                ),
            );
        }
        let due: Vec<String> = supervision
            .reconnecting
            .iter()
            .filter(|(_, (at, _))| *at <= tokio::time::Instant::now())
            .map(|(id, _)| id.clone())
            .collect();
        for id in due {
            let config = self.ssh_config(&id);
            let stopped = !self.ssh_activated.lock().unwrap().contains(&id);
            let mut ready = self.ssh_ready.lock().await;
            let (Some(config), false, false) = (config, stopped, ready.contains_key(&id)) else {
                supervision.reconnecting.remove(&id);
                continue;
            };
            match self.ssh_connect_locked(&mut ready, &config, dir).await {
                Ok(_) => {
                    supervision.reconnecting.remove(&id);
                }
                Err(error) => {
                    ready.remove(&id);
                    self.set_host_status_for(
                        &id,
                        "failed",
                        Some(format!("Device host {id} failed while {}.", error.step)),
                    );
                    if let Some((at, delay)) = supervision.reconnecting.get_mut(&id) {
                        *delay = (*delay * 2).min(Duration::from_secs(30));
                        *at = tokio::time::Instant::now() + *delay;
                    }
                }
            }
        }
    }
}

fn saved_host<'a>(settings: &'a Value, key: &str, input: &Value) -> Option<&'a Value> {
    let id = input["hostId"].as_str()?;
    settings[key].as_array()?.iter().find(|h| h["id"] == id)
}

async fn device_token(
    resolver: &Resolver,
    settings: &Value,
    source: &Value,
) -> Result<String, String> {
    if source.is_null() {
        return Ok(String::new());
    }
    resolver.resolve(settings, source, ReadMode::Cached).await.ok_or_else(|| {
        let label = provider_label(settings, source);
        format!("Could not read the credential from the secret provider \"{label}\". Unlock or fix it in Settings, then try again.")
    })
}

async fn simlock(input: &Value, settings: &Value, resolver: &Resolver) -> Reply {
    let fail = |reason: String| json!({"_tag":"DeviceSimlockIdentityError","reason":reason});
    let env_url = env::var("AGENT_DEVICE_DAEMON_BASE_URL").ok().and_then(|v| {
        v.trim()
            .trim_end_matches('/')
            .strip_suffix("/v1/agent-device")
            .filter(|url| url::Url::parse(url).is_ok())
            .map(str::to_owned)
    });
    let env_token = env::var("AGENT_DEVICE_DAEMON_AUTH_TOKEN")
        .ok()
        .map(|s| s.trim().to_owned())
        .filter(|s| !s.is_empty());
    let machine = env_url.as_deref().zip(env_token.as_deref());
    let gateway = input["gatewayUrl"]
        .as_str()
        .filter(|s| !s.is_empty())
        .or_else(|| machine.map(|(url, _)| url))
        .ok_or_else(|| {
            fail("This machine's agent-device does not point at a Simlock gateway.".into())
        })?
        .trim_end_matches('/');
    let same_url = |a: &str, b: &str| match (url::Url::parse(a), url::Url::parse(b)) {
        (Ok(a), Ok(b)) => a.origin() == b.origin(),
        _ => false,
    };
    let saved = saved_host(settings, "simlockDeviceHosts", input);
    let present = |v: &Value| v.is_object() || v.as_str().is_some_and(|s| !s.is_empty());
    let saved = saved.filter(|host| present(&host["agentToken"]));
    let (token, source) = if present(&input["agentToken"]) {
        (
            device_token(resolver, settings, &input["agentToken"])
                .await
                .map_err(fail)?,
            "entered",
        )
    } else if let Some(host) = saved.filter(|h| {
        h["gatewayUrl"]
            .as_str()
            .is_some_and(|url| same_url(url, gateway))
    }) {
        (
            device_token(resolver, settings, &host["agentToken"])
                .await
                .map_err(fail)?,
            "settings",
        )
    } else if let Some((_, token)) = machine.filter(|(url, _)| same_url(url, gateway)) {
        (token.to_owned(), "env")
    } else {
        return Err(fail(
            if saved.is_some() {
                "The saved token is for another gateway. Enter one for this gateway."
            } else {
                "No agent token for this gateway on this machine."
            }
            .into(),
        ));
    };
    let bad = || {
        fail(format!(
            "The Simlock gateway at {gateway} sent an answer T3 could not read."
        ))
    };
    let res = client()
        .get(format!("{gateway}/v1/identity"))
        .bearer_auth(token)
        .send()
        .await
        .map_err(|_| fail(format!("Could not reach the Simlock gateway at {gateway}.")))?;
    if !res.status().is_success() {
        return Err(fail(if matches!(res.status().as_u16(), 401 | 403) {
            format!(
                "The Simlock gateway refused the agent token for {}.",
                if source == "env" {
                    "this machine"
                } else {
                    "this environment"
                }
            )
        } else {
            format!(
                "The Simlock gateway at {gateway} answered HTTP {}.",
                res.status().as_u16()
            )
        }));
    }
    let identity: Value = res.json().await.map_err(|_| bad())?;
    let role = identity["role"].as_str().ok_or_else(bad)?;
    if role != "agent" {
        return Err(fail(format!(
            "That is a {role} token. Create one with --role agent."
        )));
    }
    Ok(json!({
        "gatewayUrl": gateway,
        "signedInAs": identity["name"].as_str().ok_or_else(bad)?,
        "tokenSource": source,
    }))
}

/// `easAccount`: who an Expo credential signs in as, and their apps.
async fn expo(input: &Value, settings: &Value, resolver: &Resolver) -> Reply {
    let fail = |reason: String| json!({"_tag":"DeviceEasAccountError","reason":reason});
    let saved = input["hostId"].as_str().map_or(Value::Null, |id| {
        settings["easDeviceHosts"]
            .as_array()
            .into_iter()
            .flatten()
            .find(|host| host["id"] == id)
            .map(|host| host["accessToken"].clone())
            .unwrap_or(Value::Null)
    });
    let source = input
        .get("accessToken")
        .filter(|v| !v.is_null())
        .unwrap_or(&saved);
    let token = device_token(resolver, settings, source)
        .await
        .map_err(fail)?;
    let credential = expo_api::resolve_credential(&token, &home()).map_err(fail)?;
    // Only bounded text reaches the browser: the credential errors' own
    // guidance, or a category for what Expo answered, never Expo's text.
    let (name, apps) = expo_api::viewer(&credential).await.map_err(|error| {
        tracing::warn!(error = %error.message(), "Expo account check failed");
        fail(match error.status {
            Some(status @ (401 | 403)) => format!("Expo rejected the credential (HTTP {status})."),
            _ if error.rejected => "Expo refused the account check.".into(),
            _ => "Could not complete the Expo account check.".into(),
        })
    })?;
    Ok(json!({"signedInAs": name, "credentialSource": credential.source, "apps": apps}))
}

impl DeviceService {
    /// `upstream`: where a host that routes its own requests sends one, with
    /// the headers that authorize it. `None` for a host whose hub takes every
    /// request; `Some(None)` for a request it does not route (answered 404).
    /// A Simlock lease's stream and input go to the gateway's routes for it.
    pub fn hub_upstream(
        &self,
        host: Option<&str>,
        path: &str,
        query: &str,
    ) -> Option<Option<HubRoute>> {
        if let Some(host) = host.filter(|h| self.eas_config(h).is_some()) {
            return Some(self.eas_upstream(host, path, query));
        }
        let host = host.filter(|h| self.simlock_config(h).is_some())?;
        const INPUT_PATH: &str = "/vendor/serve-sim/helper/ws";
        let stream = path
            .strip_prefix("/vendor/serve-sim/helper/")
            .and_then(|rest| rest.strip_suffix("/stream.avcc"))
            .filter(|id| !id.is_empty() && !id.contains('/'));
        let device = match stream {
            Some(encoded) => match percent_encoding::percent_decode_str(encoded).decode_utf8() {
                Ok(id) => Some(id.into_owned()),
                Err(_) => return Some(None),
            },
            None if path == INPUT_PATH => url::form_urlencoded::parse(query.as_bytes())
                .find(|(k, _)| k == "device")
                .map(|(_, v)| v.into_owned()),
            None => None,
        };
        let Some(device) = device else {
            return Some(None);
        };
        Some(
            self.with_simlock(host, |state| {
                let lease = state.leases.get(&device)?;
                Some((
                    format!(
                        "{}/v1/leases/{}/{}",
                        state.config.gateway(),
                        agent_device::encode_uri_component(&lease.lease_id),
                        if stream.is_some() { "stream" } else { "input" }
                    ),
                    vec![(
                        "authorization".to_owned(),
                        format!("Bearer {}", lease.token),
                    )],
                ))
            })
            .flatten(),
        )
    }

    /// An Expo host's `upstream`: clients name the device by its session id,
    /// the preview host by its own id, so that is swapped in the path and in
    /// the `device` parameter. A session answers only on its own platform's
    /// server. The `sec-websocket-protocol` header is sent only on upgrades.
    fn eas_upstream(&self, host: &str, path: &str, query: &str) -> Option<HubRoute> {
        let platform = ["ios", "android"]
            .into_iter()
            .find(|p| path.starts_with(&format!("{}/", eas::hub_prefix(p))))?;
        let helper = path
            .strip_prefix("/vendor/serve-sim/helper/")
            .and_then(|rest| {
                let (segment, tail) = rest.split_once('/')?;
                (!segment.is_empty()).then_some((segment, tail))
            });
        let device = match helper {
            Some((segment, _)) => Some(
                percent_encoding::percent_decode_str(segment)
                    .decode_utf8()
                    .ok()?
                    .into_owned(),
            ),
            None => url::form_urlencoded::parse(query.as_bytes())
                .find(|(k, _)| k == "device")
                .map(|(_, v)| v.into_owned()),
        };
        self.with_eas(host, |state| {
            let session = match &device {
                Some(id) => state.sessions.get(id)?,
                None if state.sessions.len() == 1 => state.sessions.values().next()?,
                None => return None,
            };
            if session.platform != platform {
                return None;
            }
            let rest = match helper {
                Some((_, tail)) => format!(
                    "/helper/{}/{tail}",
                    agent_device::encode_uri_component(&session.remote_device_id)
                ),
                None => format!(
                    "{}{}",
                    eas::preview_prefix(platform),
                    &path[eas::hub_prefix(platform).len()..]
                ),
            };
            let mut params = url::form_urlencoded::Serializer::new(String::new());
            for (k, v) in url::form_urlencoded::parse(query.as_bytes()) {
                if k == "device" {
                    params.append_pair(&k, &session.remote_device_id);
                } else {
                    params.append_pair(&k, &v);
                }
            }
            let params = params.finish();
            let query = if params.is_empty() {
                String::new()
            } else {
                format!("?{params}")
            };
            Some((
                format!("{}{rest}{query}", session.preview_origin),
                vec![
                    (
                        "authorization".to_owned(),
                        format!("Bearer {}", session.preview_token),
                    ),
                    (
                        "sec-websocket-protocol".to_owned(),
                        format!(
                            "{}.token.{}",
                            eas::protocol(platform),
                            session.preview_token
                        ),
                    ),
                ],
            ))
        })
        .flatten()
    }

    /// `currentReadiness`'s hub: this machine's, or an SSH host's through its tunnel.
    pub async fn hub_origin(&self, host: Option<&str>) -> Option<String> {
        match host.filter(|h| *h != LOCAL_HOST_ID) {
            // An Expo host's sessions route themselves (`hub_upstream`).
            Some(host) if self.eas_config(host).is_some() => self
                .with_eas(host, |state| {
                    state.ready.then(|| "https://api.expo.dev".to_owned())
                })
                .flatten(),
            // A Simlock host is ready once its CLI is; its leases route themselves (`hub_upstream`).
            Some(host) if self.simlock_config(host).is_some() => self
                .with_simlock(host, |state| {
                    state.cli.is_some().then(|| state.config.gateway())
                })
                .flatten(),
            Some(host) => self.ssh_current(host).await.map(|ctx| ctx.origin),
            None => {
                let mut runtime = self.runtime.lock().await;
                running_origin(&mut runtime)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolves_host_by_id_or_case_insensitive_label() {
        let hosts = vec![
            json!({"id": "local", "label": "This machine"}),
            json!({"id": "mac-1", "label": "oliverMac"}),
            json!({"id": "mini-1", "label": "Mac mini"}),
            json!({"id": "mini-2", "label": "Mac mini"}),
        ];
        assert_eq!(resolve_host_id(&hosts, None).unwrap(), None);
        assert_eq!(resolve_host_id(&hosts, Some("")).unwrap(), None);
        assert_eq!(
            resolve_host_id(&hosts, Some("local")).unwrap().as_deref(),
            Some("local")
        );
        assert_eq!(
            resolve_host_id(&hosts, Some("olivermac"))
                .unwrap()
                .as_deref(),
            Some("mac-1")
        );
        let unknown = resolve_host_id(&hosts, Some("studio")).unwrap_err();
        assert!(unknown["reason"]
            .as_str()
            .unwrap()
            .contains("This machine (local), oliverMac (mac-1)"));
        let ambiguous = resolve_host_id(&hosts, Some("mac mini")).unwrap_err();
        assert!(ambiguous["reason"]
            .as_str()
            .unwrap()
            .contains("Mac mini (mini-1), Mac mini (mini-2)"));
    }

    #[test]
    fn locates_sessions_and_reports_ambiguous_or_wrong_host() {
        let state = json!({
            "hosts": [
                {"id": "local", "label": "This machine"},
                {"id": "mac-mini", "label": "Mac Mini"},
            ],
            "sessions": [
                {"threadId": "t1", "hostId": "local", "deviceId": "Pixel_10"},
                {"threadId": "t1", "hostId": "mac-mini", "deviceId": "Pixel_10"},
                {"threadId": "t1", "hostId": "mac-mini", "deviceId": "MAC-UDID"},
            ],
        });
        let ambiguous = locate_session(&state, "t1", "Pixel_10", None).unwrap_err();
        assert_eq!(
            ambiguous["reason"],
            "Device Pixel_10 is on more than one host: 'This machine' (hostId local), 'Mac Mini' (hostId mac-mini). Pass hostId."
        );
        let wrong = locate_session(&state, "t1", "MAC-UDID", Some("local")).unwrap_err();
        assert_eq!(
            wrong["reason"],
            "Device MAC-UDID is not on 'This machine' (hostId local). It is on 'Mac Mini' (hostId mac-mini); pass that hostId."
        );
        let found = locate_session(&state, "t1", "MAC-UDID", None)
            .unwrap()
            .unwrap();
        assert_eq!(found["hostId"], "mac-mini");
        assert!(locate_session(&state, "t1", "missing", None)
            .unwrap()
            .is_none());
    }

    #[test]
    fn extracts_the_first_complete_png_and_its_size() {
        let png = hex::decode("89504e470d0a1a0a0000000d49484452000000030000000208020000001216f14d0000001049444154789c63f8cfc0f01f0418000017f402fe5a7a2d7a0000000049454e44ae426082").unwrap();
        let mut noisy = b"WARNING: two displays\n".to_vec();
        noisy.extend(&png);
        noisy.extend(b"\ntrailer");
        assert_eq!(extract_png(&noisy), Some(png.as_slice()));
        assert_eq!(png_dimensions(&png), (3, 2));
        assert_eq!(extract_png(&png[..png.len() - 4]), None);
        assert_eq!(png_dimensions(b"short"), (0, 0));
    }

    #[test]
    fn picks_the_device_device_open_opens() {
        let state = json!({
            "hosts": [{"id": "local", "label": "This machine"}],
            "hostStatuses": {},
            "devices": [
                {"hostId": "local", "id": "a", "platform": "android", "booted": false},
                {"hostId": "local", "id": "b", "platform": "android", "booted": true},
            ],
        });
        assert_eq!(pick_device(&state, None, None, None).unwrap()["id"], "b");
        assert_eq!(
            pick_device(&state, Some("a"), None, None).unwrap()["id"],
            "a"
        );
        assert_eq!(
            pick_device(&state, Some("z"), None, Some("local")).unwrap_err()["reason"],
            "No device z on 'This machine' (hostId local). Call device_list for current ids."
        );
        assert_eq!(
            pick_device(&state, None, Some("ios"), None).unwrap_err()["reason"],
            "No ios devices were found on host local. Call device_list to see why."
        );
        assert!(runs_on_xcode26_runtime("iOS 26.1"));
        assert!(!runs_on_xcode26_runtime("iOS 27.0"));
        assert!(!runs_on_xcode26_runtime("Android 15"));
    }

    #[test]
    fn formats_error_head_and_filters_core_sim_enoent() {
        assert_eq!(
            error_head("Error: failed\n  more info\n    at foo (a.js:1:1)\n  extra"),
            "Error: failed more info"
        );
        assert!(is_core_sim_enoent(
            "[apple-utils] Failed to read CoreSimulator plist at /Users/a/Library/Developer/CoreSimulator/Devices/1234/device.plist:",
            "Error: ENOENT: no such file"
        ));
        assert!(!is_core_sim_enoent(
            "[apple-utils] Failed to read CoreSimulator plist at /Users/a/Library/Developer/CoreSimulator/Devices/1234/device.plist:",
            "Error: EACCES: permission denied"
        ));
        assert!(has_word("valid modes: emulated, webcam0", "emulated"));
        assert!(!has_word("valid modes: emulated0", "emulated"));
    }
}
