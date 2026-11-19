use super::{invalid, string, timestamp, Reply};
use crate::settings::{
    secrets::{provider_label, ReadMode, Resolver},
    Settings,
};
use serde_json::{json, Value};
use std::{
    env, fs,
    path::{Path, PathBuf},
    process::Stdio,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    process::{Child, Command},
    sync::broadcast,
};

const HUB: (&str, &str, &str) = ("expo-device-hub", "0.22.1", "dist/server/cli.mjs");
const AGENT: (&str, &str, &str) = ("agent-device", "0.21.36", "bin/agent-device.mjs");
type Tool = (&'static str, &'static str, &'static str);
pub struct DeviceService {
    settings: Arc<Settings>,
    state: Mutex<Value>,
    events: broadcast::Sender<Value>,
    runtime: tokio::sync::Mutex<Option<(String, Child)>>,
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
        .filter(|v| !v.is_empty())
        .map(PathBuf::from)
        .collect();
    let mut candidates = explicit.clone();
    candidates.extend([
        home().join("Library/Android/sdk"),
        home().join("Android/Sdk"),
        home().join("AppData/Local/Android/Sdk"),
    ]);
    if let Some(path) = env::var_os("PATH") {
        for p in env::split_paths(&path) {
            if let Ok(p) = fs::canonicalize(p.join(if cfg!(windows) { "adb.exe" } else { "adb" })) {
                if let Some(root) = p.parent().and_then(Path::parent) {
                    candidates.push(root.into());
                }
            }
        }
    }
    candidates
        .into_iter()
        .find(|p| {
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
        })
        .or_else(|| explicit.into_iter().next())
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
    json!({"id":"local","label":"This machine","platforms":[availability("ios"),availability("android")],
        "tools":{"hub":{"requiredVersion":HUB.1,"installedVersions":hub,"runningVersion":running.then_some(HUB.1)},"agent":{"requiredVersion":AGENT.1,"installedVersions":agent,"runningVersion":null}},
        "hubInstalled":hub.iter().any(|v|v==HUB.1),"agentDeviceInstalled":agent.iter().any(|v|v==AGENT.1),"kind":"local"})
}
fn disabled() -> Value {
    json!({"_tag":"DeviceHostUnavailableError","hostId":"local","hostLabel":"This machine","reason":"Device support is off. Enable it in the Device panel before installing or starting device tools."})
}
fn bad_host(id: &str) -> Value {
    json!({"_tag":"DeviceHostUnavailableError","hostId":id,"reason":"Unknown host."})
}
fn op_err(op: &str, reason: &str) -> Value {
    json!({"_tag":"DeviceOperationError","operation":op,"reason":reason,"cause":null})
}
fn initial(dir: &Path, saved: &Value) -> Value {
    json!({"supportsHostRetry":true,"supportsToolUpdate":true,"supportsToolInspection":true,"supportsEasHosts":true,"supportsSimlockHosts":true,"hostStatus":if saved["enableDeviceSupport"]==true{"idle"}else{"disabled"},"hostStatuses":{},"devices":[],"sessions":[],"onboardingCompleted":saved["deviceOnboardingCompleted"]==true,"agentAccessEnabled":saved["enableAgentDeviceAccess"]==true,"hubBasePath":"/api/device-hub","revision":0,"hosts":[host(dir,false)],"easHosts":[],"simlockHosts":[]})
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
    let status = Command::new("npm")
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
        .map_err(|_| op_err("install", "command_failed"))?;
    if !status.success() || !entry.is_file() {
        return Err(op_err("install", "command_failed"));
    }
    tokio::fs::write(base.join(".install-complete"), spec.1)
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
async fn ensure_hub(runtime: &mut Option<(String, Child)>, dir: &Path) -> Result<String, Value> {
    if let Some((origin, child)) = runtime {
        if child.try_wait().ok() == Some(None) {
            return Ok(origin.clone());
        }
    }
    let entry = install(dir, HUB).await?;
    let listener = std::net::TcpListener::bind(("127.0.0.1", 0))
        .map_err(|_| op_err("start", "command_failed"))?;
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    let mut cmd = Command::new("node");
    cmd.arg(entry).args([
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
    let mut child = cmd
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .map_err(|_| op_err("start", "command_failed"))?;
    let origin = format!("http://127.0.0.1:{port}");
    for _ in 0..100 {
        if child.try_wait().ok().flatten().is_some() {
            break;
        }
        if tokio::net::TcpStream::connect(("127.0.0.1", port))
            .await
            .is_ok()
        {
            *runtime = Some((origin.clone(), child));
            return Ok(origin);
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    Err(op_err("start", "command_failed"))
}
async fn list_hub(origin: &str) -> Result<Vec<Value>, Value> {
    let result = query(client().get(format!("{origin}/api/devices")), "list").await?;
    let mut devices = Vec::new();
    for group in ["simulators", "emulators"] {
        for device in result[group]
            .as_array()
            .ok_or_else(|| op_err("list", "request_failed"))?
        {
            let mut record = json!({"hostId":"local"});
            for k in ["id", "platform", "name", "version", "booted", "physical"] {
                record[k] = device[k].clone();
            }
            devices.push(record);
        }
    }
    Ok(devices)
}
async fn shutdown(origin: &str, platform: &str, id: &str) -> Reply {
    let (path, body) = if platform == "ios" {
        ("/vendor/serve-sim/grid/api/shutdown", json!({"udid":id}))
    } else {
        (
            "/api/devices/shutdown",
            json!({"platform":platform,"id":id}),
        )
    };
    let res = query(
        client().post(format!("{origin}{path}")).json(&body),
        "shutdown",
    )
    .await?;
    if res["ok"] != true {
        return Err(op_err("shutdown", "hub_rejected"));
    }
    Ok(Value::Null)
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
        let host_id = input["hostId"].as_str().unwrap_or("local");
        if name != "close" && host_id != "local" {
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
                if id != "local" {
                    return Err(bad_host(id));
                }
            }
            if let Some(tool) = input["updateTool"].as_str() {
                install(dir, if tool == "hub" { HUB } else { AGENT }).await?;
            }
            if input["inspectOnly"] == true || input.get("updateTool").is_some() {
                state["hosts"] = json!([host(dir, runtime.is_some())]);
                self.publish(&mut state);
            }
            if enabled
                && input["inspectOnly"] != true
                && state["hosts"][0]["platforms"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|p| p["available"] == true)
            {
                let origin = ensure_hub(&mut runtime, dir).await?;
                state["devices"] = json!(list_hub(&origin).await?);
                state["hosts"] = json!([host(dir, true)]);
                state["hostStatuses"] = json!({"local":{"status":"ready"}});
                state["hostStatus"] = json!("ready");
                self.publish(&mut state);
            }
            return Ok(wire(&state));
        }
        if name == "close" {
            let thread = string(input, "threadId")?;
            let id = input["deviceId"].as_str();
            let targets: Vec<_> = state["sessions"]
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
            if input["shutdown"] == true && !targets.is_empty() {
                let origin = ensure_hub(&mut runtime, dir).await?;
                for s in &targets {
                    shutdown(
                        &origin,
                        s["platform"].as_str().unwrap(),
                        s["deviceId"].as_str().unwrap(),
                    )
                    .await?;
                }
            }
            state["sessions"].as_array_mut().unwrap().retain(|s| {
                !targets.iter().any(|t| {
                    if input["shutdown"] == true {
                        s["hostId"] == t["hostId"] && s["deviceId"] == t["deviceId"]
                    } else {
                        s == t
                    }
                })
            });
            self.publish(&mut state);
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
            state["devices"] = json!(list_hub(&origin).await?);
            self.publish(&mut state);
            return Ok(result);
        }
        let devices = list_hub(&origin).await?;
        let device = devices
            .iter()
            .find(|d| d["id"] == id)
            .ok_or_else(|| json!({"_tag":"DeviceNotFoundError","hostId":host_id,"deviceId":id}))?;
        if name == "open" {
            let thread = string(input, "threadId")?;
            let mut booted_id = id.to_owned();
            if device["booted"] != true && input["boot"] != false {
                let boot = query(
                    client().post(format!("{origin}/api/devices/boot")).json(
                        &json!({"platform":device["platform"],"id":id,"name":device["name"]}),
                    ),
                    "boot",
                )
                .await?;
                if boot["ok"] != true {
                    return Err(
                        json!({"_tag":"DeviceBootError","hostId":host_id,"deviceId":id,"reason":"launch_failed"}),
                    );
                }
                if let Some(i) = boot["id"].as_str().or(boot["serial"].as_str()) {
                    booted_id = i.to_owned();
                }
            }
            if device["platform"] == "ios" {
                query(
                    client()
                        .post(format!("{origin}/vendor/serve-sim/grid/api/start"))
                        .json(&json!({"udid":booted_id})),
                    "open",
                )
                .await?;
            }
            let session = json!({"threadId":thread,"hostId":host_id,"deviceId":booted_id,"platform":device["platform"],"openedAt":timestamp()});
            let sessions = state["sessions"].as_array_mut().unwrap();
            sessions.retain(|s| {
                s["threadId"] != thread || s["hostId"] != host_id || s["deviceId"] != booted_id
            });
            sessions.push(session.clone());
            state["devices"] = json!(devices);
            self.publish(&mut state);
            return Ok(session);
        }
        super::device_actions::handle(
            name,
            device,
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
    if id == "local" {
        return Err(invalid("id", "The local host id is reserved."));
    }
    if target.starts_with('-') || target.chars().any(|c| c.is_control() || c.is_whitespace()) {
        return Err(invalid("target", "Invalid SSH target"));
    }
    let failed = |reason: &str| json!({"_tag":"DeviceHostUnavailableError","hostId":id,"hostLabel":label,"reason":reason,"cause":null});
    let mut cmd = Command::new("ssh");
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
        platforms[1]["reason"]=json!("Android SDK was not found. Install it with Android Studio or set ANDROID_HOME on the SSH host.");
    }
    Ok(
        json!({"id":id,"label":label,"platforms":platforms,"hubInstalled":has("hub"),"agentDeviceInstalled":has("agent"),"kind":"ssh"}),
    )
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
        let label = provider_label(settings,source);
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
    Ok(
        json!({"gatewayUrl":gateway,"signedInAs":identity["name"].as_str().ok_or_else(bad)?,"tokenSource":source}),
    )
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
    let (source,token)=Some(token.trim()).filter(|s|!s.is_empty()).map(|s|("settings",s.to_owned()))
        .or_else(||env::var("EXPO_TOKEN").ok().filter(|s|!s.trim().is_empty()).map(|s|("env",s.trim().to_owned())))
        .or_else(||from_session().map(|s|("eas-login",s))).ok_or_else(||fail("No Expo credential on the server: save an access token for this host, set EXPO_TOKEN, or run `eas login`."))?;
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
    let request=client().post("https://api.expo.dev/graphql").json(&json!({"query":"query T3ExpoViewer {meActor {__typename ... on UserActor {username} ... on Robot {firstName} accounts {name appsPaginated(first:100) {edges {node {id fullName}}}}}}"}));
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
        if host.is_some_and(|h| h != "local") {
            return None;
        }
        let mut runtime = self.runtime.lock().await;
        let (origin, child) = runtime.as_mut()?;
        if child.try_wait().ok()?.is_some() {
            return None;
        }
        Some(origin.clone())
    }
}
