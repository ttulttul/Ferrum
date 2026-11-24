use super::{invalid, string, timestamp, Reply};
use crate::settings::{
    secrets::{provider_label, ReadMode, Resolver},
    Settings,
};
use serde_json::{json, Value};
use std::{
    collections::HashMap,
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

pub struct DeviceService {
    settings: Arc<Settings>,
    state: Mutex<Value>,
    events: broadcast::Sender<Value>,
    runtime: tokio::sync::Mutex<Option<HubRuntime>>,
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
        Ok(Self {
            settings,
            state: Mutex::new(state),
            events: broadcast::channel(128).0,
            runtime: Default::default(),
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
                "runningVersion": null,
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

fn wire(state: &Value) -> Value {
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

struct HubListResult {
    devices: Vec<Value>,
    detail: Option<String>,
    android_discovery_incomplete: bool,
    incomplete_platforms: Vec<&'static str>,
}

async fn list_adb_devices(sdk: Option<&Path>) -> Option<Vec<Value>> {
    let (stdout, _, code) =
        run_host_command("adb", &["devices", "-l"], sdk, Duration::from_secs(20)).await;
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
        let (getprop_out, _, _) = run_host_command(
            "adb",
            &["-s", &serial, "shell", "getprop"],
            sdk,
            Duration::from_secs(20),
        )
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
            let (avd_out, _, _) = run_host_command(
                "adb",
                &["-s", &serial, "emu", "avd", "name"],
                sdk,
                Duration::from_secs(20),
            )
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
            "hostId": LOCAL_HOST_ID,
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

async fn list_hub(origin: &str) -> Result<HubListResult, Value> {
    let sdk = sdk_root();
    let android_available = availability("android")["available"] == true;
    let ios_available = availability("ios")["available"] == true;
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
        let mut record = json!({"hostId": LOCAL_HOST_ID});
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
            match list_adb_devices(sdk.as_deref()).await {
                None => {
                    android_discovery_incomplete = true;
                    details.push("Could not list Android devices (adb devices -l).".into());
                }
                Some(adb_devices) => devices.extend(adb_devices),
            }
        }
        let (avds_stdout, _, avds_code) = run_host_command(
            "emulator",
            &["-list-avds"],
            sdk.as_deref(),
            Duration::from_secs(20),
        )
        .await;
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
                        "hostId": LOCAL_HOST_ID,
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

fn apply_list_result(state: &mut Value, dir: &Path, listed: HubListResult) {
    let mut merged: Vec<Value> = state["devices"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|d| {
            d["hostId"] != LOCAL_HOST_ID
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
    state["hosts"] = json!([host(dir, true)]);
    state["hostStatus"] = json!("ready");
    if let Some(obj) = state.as_object_mut() {
        match &listed.detail {
            Some(detail) => {
                obj.insert("hostStatusDetail".into(), json!(detail));
            }
            None => {
                obj.insert("hostStatusDetail".into(), Value::Null);
            }
        }
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
    state["hostStatuses"][LOCAL_HOST_ID] = host_status;
}

async fn check_android_ffmpeg(sdk: Option<&Path>) -> Result<(), Value> {
    let binary = env::var("SERVE_EMU_FFMPEG")
        .ok()
        .map(|s| s.trim().to_owned())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "ffmpeg".to_owned());
    let (stdout, stderr, code) = run_host_command(
        &binary,
        &["-hide_banner", "-encoders"],
        sdk,
        Duration::from_secs(10),
    )
    .await;
    if code != 0 {
        return Err(json!({
            "_tag": "DevicePlatformUnavailableError",
            "hostId": LOCAL_HOST_ID,
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
            "hostId": LOCAL_HOST_ID,
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

async fn shutdown(origin: &str, platform: &str, id: &str) -> Reply {
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
            Err(cause) => match list_hub(origin).await {
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
        return Err(json!({
            "_tag": "DeviceToolUnavailableError",
            "reason": format!("Device {device_id} is on more than one host: {names}. Pass hostId."),
        }));
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
        return Err(json!({
            "_tag": "DeviceToolUnavailableError",
            "reason": format!(
                "Device {device_id} is not on {}. It is on {names}; pass that hostId.",
                host_display_name(hosts, wanted)
            ),
        }));
    }
    Ok(None)
}

impl DeviceService {
    pub fn subscribe(&self) -> broadcast::Receiver<Value> {
        self.events.subscribe()
    }
    pub fn snapshot(&self, _dir: &Path) -> Value {
        wire(&self.state.lock().unwrap())
    }
    fn publish(&self, state: &mut Value) {
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
        Some(self.apply(name, input, dir).await)
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
                let state = self.snapshot(dir);
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
                self.apply("close", &close_input, dir).await?;
                Ok(json!({}))
            }
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
            "testHost" => return ssh_probe(input, dir).await,
            _ => {}
        }
        let mut runtime = self.runtime.lock().await;
        let mut state = self.snapshot(dir);
        let host_id = input["hostId"].as_str().unwrap_or(LOCAL_HOST_ID);
        if name != "close" && host_id != LOCAL_HOST_ID {
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
            if !enabled {
                runtime.take();
                state["sessions"] = json!([]);
                state["devices"] = json!([]);
                state["bootingDevices"] = json!([]);
            } else if state.get("bootingDevices").is_none() {
                state["bootingDevices"] = Value::Null;
            }
            self.publish(&mut state);
        }
        if name == "list" || name == "configure" {
            if let Some(id) = input["retryHostId"].as_str() {
                if id != LOCAL_HOST_ID {
                    return Err(bad_host(id));
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
            if enabled
                && input["inspectOnly"] != true
                && fresh_host["platforms"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|p| p["available"] == true)
            {
                match ensure_hub(&mut runtime, dir).await {
                    Ok(origin) => match list_hub(&origin).await {
                        Ok(listed) => {
                            apply_list_result(&mut state, dir, listed);
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
            return Ok(wire(&state));
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
            if do_shutdown && !closing.is_empty() {
                let Some(origin) = running_origin(&mut runtime) else {
                    return Err(json!({
                        "_tag": "DeviceHostUnavailableError",
                        "hostId": closing[0]["hostId"],
                        "reason": "Device host is not running. Reconnect before retrying close.",
                    }));
                };
                for s in &closing {
                    let dev_id = s["deviceId"].as_str().unwrap_or("");
                    let platform = s["platform"].as_str().unwrap_or("");
                    shutdown(&origin, platform, dev_id).await?;
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
            if do_shutdown && !closing.is_empty() {
                if let Some(origin) = running_origin(&mut runtime) {
                    if let Ok(listed) = list_hub(&origin).await {
                        apply_list_result(&mut state, dir, listed);
                        self.publish(&mut state);
                    }
                }
            }
            return Ok(Value::Null);
        }
        let id = string(input, "deviceId")?;
        if name == "open" {
            let p = string(input, "platform")?;
            let a = availability(p);
            if a["available"] == false {
                return Err(
                    json!({"_tag":"DevicePlatformUnavailableError","hostId":host_id,"platform":p,"reason":a["reason"]}),
                );
            }
        }
        if !enabled {
            return Err(disabled());
        }
        let origin = ensure_hub(&mut runtime, dir).await?;
        if name == "shutdown" {
            let result = shutdown(&origin, string(input, "platform")?, id).await?;
            state["sessions"]
                .as_array_mut()
                .unwrap()
                .retain(|s| s["hostId"] != host_id || s["deviceId"] != id);
            if let Ok(listed) = list_hub(&origin).await {
                apply_list_result(&mut state, dir, listed);
            }
            self.publish(&mut state);
            return Ok(result);
        }
        let listed = list_hub(&origin).await?;
        apply_list_result(&mut state, dir, listed);
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
                check_android_ffmpeg(sdk_root().as_deref()).await?;
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
                let refreshed = list_hub(&origin).await?;
                let platform_incomplete = device["platform"]
                    .as_str()
                    .is_some_and(|p| refreshed.incomplete_platforms.contains(&p));
                apply_list_result(&mut state, dir, refreshed);
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
                    "hostLabel": LOCAL_HOST_LABEL,
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

async fn ssh_probe(input: &Value, dir: &Path) -> Reply {
    let id = string(input, "id")?;
    let label = string(input, "label")?;
    let target = string(input, "target")?;
    if id == LOCAL_HOST_ID {
        return Err(invalid("id", "The local host id is reserved."));
    }
    if target.starts_with('-') || target.chars().any(|c| c.is_control() || c.is_whitespace()) {
        return Err(invalid("target", "Invalid SSH target"));
    }
    let failed = |reason: &str| json!({"_tag":"DeviceHostUnavailableError","hostId":id,"hostLabel":label,"reason":reason,"cause":null});
    let mut cmd = crate::login_shell::tokio_command("ssh");
    cmd.args(["-o", "BatchMode=yes", "-o", "ConnectTimeout=8"]);
    if let Some(port) = input.get("port") {
        let p = port
            .as_u64()
            .filter(|n| (1..=65535).contains(n))
            .ok_or_else(|| invalid("port", "Invalid port"))?;
        cmd.args(["-p", &p.to_string()]);
    }
    if input.get("identityFile").is_some() {
        cmd.args(["-i", string(input, "identityFile")?]);
    }
    if super::local_ssh::local_target(input).await {
        return Ok(host(dir, false));
    }
    // Probe remote capabilities with a fixed script. No request field enters the remote shell.
    const PROBE: &str = r#"command -v node >/dev/null || exit 42
uname -s
command -v xcrun >/dev/null && echo xcrun
sdk=${ANDROID_HOME:-${ANDROID_SDK_ROOT:-$HOME/Android/Sdk}}
test -x "$sdk/platform-tools/adb" && test -x "$sdk/emulator/emulator" && test -x "$sdk/cmdline-tools/latest/bin/avdmanager" && echo android
command -v expo-device-hub >/dev/null && echo hub
command -v agent-device >/dev/null && echo agent
exit 0"#;
    let output = tokio::time::timeout(
        Duration::from_secs(15),
        cmd.args(["--", target, PROBE])
            .stdin(Stdio::null())
            .kill_on_drop(true)
            .output(),
    )
    .await
    .ok()
    .and_then(Result::ok)
    .ok_or_else(|| failed("Could not connect to this host over SSH."))?;
    if !output.status.success() {
        return Err(failed(if output.status.code() == Some(42) {
            "Connected over SSH, but Node is not on this host's PATH for non-interactive SSH commands."
        } else {
            "Could not connect to this host over SSH."
        }));
    }
    let output = String::from_utf8_lossy(&output.stdout);
    let has = |s: &str| output.lines().any(|l| l.trim() == s);
    let ios = has("Darwin") && has("xcrun");
    let android = has("android");
    let mut platforms = vec![
        json!({"platform":"ios","available":ios}),
        json!({"platform":"android","available":android}),
    ];
    if !ios {
        platforms[0]["reason"] = json!("iOS Simulators need macOS with Xcode.");
    }
    if !android {
        platforms[1]["reason"] = json!("Android SDK was not found. Install it with Android Studio or set ANDROID_HOME on the SSH host.");
    }
    Ok(json!({
        "id": id,
        "label": label,
        "platforms": platforms,
        "hubInstalled": has("hub"),
        "agentDeviceInstalled": has("agent"),
        "kind": "ssh",
    }))
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

async fn expo(input: &Value, settings: &Value, resolver: &Resolver) -> Reply {
    let fail = |s: &str| json!({"_tag":"DeviceEasAccountError","reason":s});
    let from_session = || -> Option<String> {
        let state: Value =
            serde_json::from_slice(&fs::read(home().join(".expo/state.json")).ok()?).ok()?;
        state["auth"]["sessionSecret"].as_str().map(str::to_owned)
    };
    let saved = saved_host(settings, "easDeviceHosts", input);
    let credential = input
        .get("accessToken")
        .filter(|v| !v.is_null())
        .or_else(|| saved.and_then(|h| h.get("accessToken")));
    let token = device_token(resolver, settings, credential.unwrap_or(&Value::Null))
        .await
        .map_err(|reason| fail(&reason))?;
    let (source, token) = Some(token.trim())
        .filter(|s| !s.is_empty())
        .map(|s| ("settings", s.to_owned()))
        .or_else(|| {
            env::var("EXPO_TOKEN")
                .ok()
                .filter(|s| !s.trim().is_empty())
                .map(|s| ("env", s.trim().to_owned()))
        })
        .or_else(|| from_session().map(|s| ("eas-login", s)))
        .ok_or_else(|| {
            fail("No Expo credential on the server: save an access token for this host, set EXPO_TOKEN, or run `eas login`.")
        })?;
    if !token.bytes().all(|b| (0x21..=0x7e).contains(&b)) {
        return Err(fail(&format!(
            "{} is not a usable Expo credential. Replace it.",
            match source {
                "settings" => "The saved access token",
                "env" => "EXPO_TOKEN",
                _ => "The eas login session",
            }
        )));
    }
    let request = client().post("https://api.expo.dev/graphql").json(&json!({"query":"query T3ExpoViewer {meActor {__typename ... on UserActor {username} ... on Robot {firstName} accounts {name appsPaginated(first:100) {edges {node {id fullName}}}}}}"}));
    let req = if source == "eas-login" {
        request.header("expo-session", token)
    } else {
        request.bearer_auth(token)
    };
    let bad = || fail("Could not complete the Expo account check.");
    let response = req.send().await.map_err(|_| bad())?;
    if !response.status().is_success() {
        return Err(if matches!(response.status().as_u16(), 401 | 403) {
            fail(&format!(
                "Expo rejected the credential (HTTP {}).",
                response.status().as_u16()
            ))
        } else {
            bad()
        });
    }
    let result: Value = response.json().await.map_err(|_| bad())?;
    if result.get("errors").is_some() {
        return Err(fail("Expo refused the account check."));
    }
    let actor = &result["data"]["meActor"];
    let name = actor["username"]
        .as_str()
        .or(actor["firstName"].as_str())
        .or(actor["__typename"].as_str())
        .ok_or_else(bad)?;
    let mut apps = Vec::new();
    for account in actor["accounts"].as_array().ok_or_else(bad)? {
        for edge in account["appsPaginated"]["edges"]
            .as_array()
            .ok_or_else(bad)?
        {
            apps.push(json!({"id":edge["node"]["id"],"fullName":edge["node"]["fullName"]}));
        }
    }
    Ok(json!({"signedInAs":name,"credentialSource":source,"apps":apps}))
}

impl DeviceService {
    pub async fn hub_origin(&self, host: Option<&str>) -> Option<String> {
        if host.is_some_and(|h| h != LOCAL_HOST_ID) {
            return None;
        }
        let mut runtime = self.runtime.lock().await;
        running_origin(&mut runtime)
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
