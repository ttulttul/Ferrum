//! The agent-device side of local device support (the reference's
//! `AgentDeviceShim.ts`, `AgentDeviceTarget.ts` and `LocalDeviceHost`'s
//! daemon): the daemon an agent's `agent-device` commands reach, the config
//! file naming it, and the launchers `device_open` hands agents.
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{
    collections::HashSet,
    ffi::OsString,
    path::{Path, PathBuf},
    process::Stdio,
    time::{Duration, Instant},
};

pub(super) const DAEMON_READY_TIMEOUT: Duration = Duration::from_millis(30_000);
const DAEMON_POLL: Duration = Duration::from_millis(100);
const SHIM_DIR: [&str; 2] = ["device", "bin"];
const TARGETS_DIR: [&str; 2] = ["device", "targets"];
const STALE_RUNTIME: &str = "This agent-device command is out of date because T3 Code moved or updated its runtime. Call device_open again for a fresh one.";
const LAUNCHER_TEMPLATE: &str = include_str!("agent_device_launcher.mjs");
/// agent-device 0.21.41's flags that take a value, so the launcher can find the command past them.
const VALUE_FLAGS: &str = include_str!("agent_device_value_flags.txt");

/// Where agents reach this machine's agent-device daemon.
#[derive(Clone, Debug, PartialEq)]
pub(super) struct AgentEndpoint {
    pub base_url: String,
    pub token: String,
    pub entry: PathBuf,
    /// The agent-device state its sessions keep, for a host whose sessions
    /// are its own (a Simlock lease's directory).
    pub state_dir: Option<PathBuf>,
}

/// `agentDeviceStateDir`.
pub(super) fn state_dir(dir: &Path) -> PathBuf {
    dir.join("device").join("agent-device")
}

fn daemon_file(dir: &Path) -> PathBuf {
    state_dir(dir).join("daemon.json")
}

/// `AgentDeviceDaemonFile`: the daemon's published port and token.
pub(super) fn read_daemon_file(dir: &Path) -> Option<Value> {
    let raw = std::fs::read_to_string(daemon_file(dir)).ok()?;
    let file: Value = serde_json::from_str(&raw).ok()?;
    let port = file["httpPort"].as_i64()?;
    if !(file["token"].is_string()) || file["httpPort"].as_f64() != Some(port as f64) {
        return None;
    }
    if !file["pid"].is_null() && file["pid"].as_i64().is_none() {
        return None;
    }
    if !file["version"].is_null() && !file["version"].is_string() {
        return None;
    }
    Some(file)
}

/// The running daemon's version, when its recorded process is alive (`summary`).
pub(super) fn running_version(dir: &Path) -> Option<String> {
    let file = read_daemon_file(dir)?;
    let pid = file["pid"].as_i64()?;
    if !process_alive(pid) {
        return None;
    }
    file["version"].as_str().map(str::to_owned)
}

#[cfg(unix)]
fn process_alive(pid: i64) -> bool {
    let Ok(pid) = libc::pid_t::try_from(pid) else {
        return false;
    };
    if pid <= 0 {
        return false;
    }
    // SAFETY: kill with signal 0 sends nothing; it only checks the pid.
    if unsafe { libc::kill(pid, 0) } == 0 {
        return true;
    }
    std::io::Error::last_os_error().raw_os_error() == Some(libc::EPERM)
}

#[cfg(not(unix))]
fn process_alive(_pid: i64) -> bool {
    false
}

fn to_endpoint(file: &Value, entry: &Path) -> AgentEndpoint {
    AgentEndpoint {
        base_url: format!("http://127.0.0.1:{}", file["httpPort"]),
        token: file["token"].as_str().unwrap_or_default().to_owned(),
        entry: entry.to_path_buf(),
        state_dir: None,
    }
}

fn loopback_client(timeout: Duration) -> reqwest::Client {
    reqwest::Client::builder()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(timeout)
        .build()
        .unwrap()
}

/// How starting the daemon failed, as the reference's host errors.
pub(super) enum StartError {
    /// `DeviceHostError` while `step`.
    Host(&'static str),
    /// `DeviceHostTimeoutError`.
    Timeout,
    /// `AgentDeviceStartError`'s message.
    Start(String),
}

/// `AgentDeviceStartError.message`.
fn start_error_message(state_dir: &Path, exit_code: Option<i32>, reason: &str) -> String {
    let exit = exit_code
        .map(|code| format!(" (exit code {code})"))
        .unwrap_or_default();
    let detail = match reason {
        "daemon_lock_not_directory" => {
            "Daemon registration ownership could not be verified: daemon.lock is not a directory."
        }
        "daemon_registration_unproven" => "Daemon registration ownership could not be verified.",
        "daemon_retirement_unconfirmed" => "Daemon replacement could not be confirmed.",
        _ => "The AgentDevice startup command failed.",
    };
    let state_dir = state_dir.display();
    let recovery = if reason == "command_failed" {
        format!("Inspect daemon diagnostics in {state_dir}.")
    } else {
        format!("Inspect daemon.lock and daemon diagnostics in {state_dir}. Stop older clients and daemons with their original CLI before manually repairing retained state.")
    };
    format!("Agent tools failed to start{exit}. {detail} {recovery}")
}

/// `startAgentDeviceDaemon`: the recorded daemon when it answers `/health`,
/// else one started with `agent-device devices --json`, which starts and awaits it.
pub(super) async fn start_daemon(
    dir: &Path,
    entry: &Path,
    node: &Path,
    host_env: &[(OsString, OsString)],
) -> Result<AgentEndpoint, StartError> {
    let state = state_dir(dir);
    let _ = tokio::fs::create_dir_all(&state).await;
    if let Some(existing) = read_daemon_file(dir) {
        let health = format!("http://127.0.0.1:{}/health", existing["httpPort"]);
        let alive = match loopback_client(Duration::from_secs(2))
            .get(&health)
            .send()
            .await
        {
            Ok(response) => {
                let ok = response.status().as_u16() == 200;
                response.bytes().await.is_ok() && ok
            }
            Err(_) => false,
        };
        if alive {
            return Ok(to_endpoint(&existing, entry));
        }
        let _ = tokio::fs::remove_file(daemon_file(dir)).await;
    }
    let mut command = tokio::process::Command::new(node);
    command
        .arg(entry)
        .args(["devices", "--json"])
        .envs(host_env.iter().map(|(k, v)| (k, v)))
        .env("AGENT_DEVICE_STATE_DIR", &state)
        .env("AGENT_DEVICE_DAEMON_SERVER_MODE", "http")
        // Disable the five-minute idle timeout; the server owns shutdown.
        .env("AGENT_DEVICE_DAEMON_IDLE_TIMEOUT_MS", "0")
        .env("AGENT_DEVICE_NO_UPDATE_NOTIFIER", "1")
        .env("FORCE_COLOR", "0")
        .env("NO_COLOR", "1")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    if let Some(path) = crate::login_shell::path() {
        if !host_env.iter().any(|(k, _)| k == "PATH") {
            command.env("PATH", path);
        }
    }
    let run = match command.spawn() {
        Ok(child) => tokio::time::timeout(DAEMON_READY_TIMEOUT, child.wait_with_output())
            .await
            .map(|output| output.map_err(|_| ())),
        Err(_) => Ok(Err(())),
    };
    // Discovery may fail after startup; accept a published endpoint or preserve the CLI failure.
    if let Some(started) = read_daemon_file(dir) {
        return Ok(to_endpoint(&started, entry));
    }
    let output = match run {
        Err(_) => return Err(StartError::Timeout),
        Ok(Err(())) => return Err(StartError::Host("starting agent tools")),
        Ok(Ok(output)) => output,
    };
    if !output.status.success() {
        let failure: Option<Value> = serde_json::from_slice(&output.stdout).ok();
        let details = failure
            .as_ref()
            .map(|failure| &failure["error"]["details"])
            .filter(|details| !details.is_null());
        let reason = match details.and_then(|d| d["reason"].as_str()) {
            Some("daemon_registration_unproven") => {
                if details.unwrap()["inspection"]["reason"] == "non-directory" {
                    "daemon_lock_not_directory"
                } else {
                    "daemon_registration_unproven"
                }
            }
            Some("daemon_retirement_unconfirmed") => "daemon_retirement_unconfirmed",
            _ => "command_failed",
        };
        return Err(StartError::Start(start_error_message(
            &state,
            output.status.code(),
            reason,
        )));
    }
    let deadline = Instant::now() + DAEMON_READY_TIMEOUT;
    loop {
        if let Some(file) = read_daemon_file(dir) {
            return Ok(to_endpoint(&file, entry));
        }
        if Instant::now() > deadline {
            return Err(StartError::Timeout);
        }
        tokio::time::sleep(DAEMON_POLL).await;
    }
}

/// `stopAgentDeviceDaemon`: the recorded daemon stopped with its own CLI.
pub(super) async fn stop_daemon(dir: &Path, entry_for: impl Fn(Option<&str>) -> Option<PathBuf>) {
    let file = daemon_file(dir);
    if !file.exists() {
        return;
    }
    let recorded = read_daemon_file(dir);
    if let Some(pid) = recorded.as_ref().and_then(|r| r["pid"].as_i64()) {
        if pid != 0 && !process_alive(pid) {
            let _ = tokio::fs::remove_file(&file).await;
            return;
        }
    }
    let Some(entry) = entry_for(recorded.as_ref().and_then(|r| r["version"].as_str())) else {
        return;
    };
    let Some(node) = node_executable() else {
        return;
    };
    let mut command = tokio::process::Command::new(node);
    command
        .arg(entry)
        .args(["daemon", "stop", "--state-dir"])
        .arg(state_dir(dir))
        .env("AGENT_DEVICE_NO_UPDATE_NOTIFIER", "1")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .kill_on_drop(true);
    if let Some(path) = crate::login_shell::path() {
        command.env("PATH", path);
    }
    if let Ok(child) = command.spawn() {
        let _ = tokio::time::timeout(Duration::from_secs(10), child.wait_with_output()).await;
    }
}

/// `resolveNodeExecutable`: `node` on the login `PATH`.
pub(super) fn node_executable() -> Option<PathBuf> {
    let path = crate::login_shell::path().or_else(|| std::env::var_os("PATH"))?;
    let name = if cfg!(windows) { "node.exe" } else { "node" };
    std::env::split_paths(&path)
        .map(|dir| dir.join(name))
        .find(|candidate| crate::npm_release::is_executable(candidate))
        .map(|found| std::path::absolute(&found).unwrap_or(found))
}

fn key(value: &str) -> String {
    hex::encode(Sha256::digest(value.as_bytes()))[..24].to_owned()
}

/// `agentDeviceConfigPath`: a stable file per host.
pub(super) fn config_path(dir: &Path, host_id: &str) -> PathBuf {
    dir.join("device")
        .join("hosts")
        .join(format!("{}.json", key(host_id)))
}

/// `agentDeviceConfigPath` for a host with a daemon per device.
pub(super) fn device_config_path(dir: &Path, host_id: &str, device_id: &str) -> PathBuf {
    dir.join("device").join("hosts").join(format!(
        "{}.json",
        key(&json!([host_id, device_id]).to_string())
    ))
}

/// `agentDeviceSession`: the agent-device session of one thread's device.
pub(super) fn session_name(thread_id: &str, host_id: &str, device_id: &str) -> String {
    format!(
        "t3-{}",
        key(&json!([thread_id, host_id, device_id]).to_string())
    )
}

/// `writeAgentDeviceConfig`: the endpoint, written atomically and only when it changed.
pub(super) async fn write_config(file: &Path, endpoint: &AgentEndpoint) -> std::io::Result<()> {
    let parent = file.parent().unwrap_or(Path::new("."));
    tokio::fs::create_dir_all(parent).await?;
    let mut content = json!({
        "daemonBaseUrl": endpoint.base_url,
        "daemonAuthToken": endpoint.token,
    });
    if let Some(state_dir) = &endpoint.state_dir {
        content["stateDir"] = json!(state_dir.to_string_lossy());
    }
    let content = content.to_string();
    if tokio::fs::read_to_string(file).await.ok().as_deref() == Some(content.as_str()) {
        return Ok(());
    }
    let temporary = parent.join(format!(".endpoint-{}", uuid::Uuid::new_v4().simple()));
    let result = async {
        tokio::fs::write(&temporary, &content).await?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            tokio::fs::set_permissions(&temporary, std::fs::Permissions::from_mode(0o600)).await?;
        }
        tokio::fs::rename(&temporary, file).await
    }
    .await;
    let _ = tokio::fs::remove_file(&temporary).await;
    result
}

/// `releaseAgentDeviceSession`: ends a session on the running daemon; an
/// already-released one succeeds.
pub(super) async fn release_session(endpoint: &AgentEndpoint, session: &str) -> Result<(), ()> {
    let response = loopback_client(Duration::from_secs(15))
        .post(format!("{}/rpc", endpoint.base_url))
        .bearer_auth(&endpoint.token)
        .json(&json!({
            "jsonrpc": "2.0",
            "id": session,
            "method": "agent_device.command",
            "params": {"command": "close", "session": session, "positionals": [], "flags": {}},
        }))
        .send()
        .await
        .map_err(|_| ())?;
    // agent-device reports an already-released session as HTTP 404.
    if response.status().as_u16() != 404 && !response.status().is_success() {
        return Err(());
    }
    let body: Value = response.json().await.map_err(|_| ())?;
    if body["result"]["ok"] == true || body["error"]["data"]["code"] == "SESSION_NOT_FOUND" {
        Ok(())
    } else {
        Err(())
    }
}

/// `encodeURIComponent`.
pub(super) fn encode_uri_component(value: &str) -> String {
    let mut out = String::new();
    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric() || b"-_.!~*'()".contains(&byte) {
            out.push(byte as char);
        } else {
            out.push_str(&format!("%{byte:02X}"));
        }
    }
    out
}

fn quote_shell(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\"'\"'"))
}

fn launcher_source(node: &Path, entry: &Path, bound: &[String]) -> String {
    let flags: Vec<&str> = VALUE_FLAGS.lines().filter(|l| !l.is_empty()).collect();
    let text = |value: &str| Value::from(value).to_string();
    LAUNCHER_TEMPLATE
        .replace("{{ENTRY}}", &text(&entry.to_string_lossy()))
        .replace("{{BOUND}}", &json!(bound).to_string())
        .replace("{{VALUED}}", &json!(flags).to_string())
        .replace("{{STALE}}", &text(STALE_RUNTIME))
        .replace("{{NODE}}", &text(&node.to_string_lossy()))
}

/// `writeLauncher`: a launcher in `dir`, and the executable agents run.
async fn write_launcher(
    dir: &Path,
    node: &Path,
    entry: &Path,
    bound: &[String],
) -> std::io::Result<PathBuf> {
    tokio::fs::create_dir_all(dir).await?;
    let launcher = dir.join("agent-device-launcher.mjs");
    tokio::fs::write(&launcher, launcher_source(node, entry, bound)).await?;
    let node = node.to_string_lossy();
    if cfg!(windows) {
        let script = format!("@echo off\r\nsetlocal EnableExtensions DisableDelayedExpansion\r\nif not exist \"{node}\" (echo {STALE_RUNTIME} 1>&2 & exit /b 1)\r\nset \"ELECTRON_RUN_AS_NODE=1\"\r\n\"{node}\" \"{}\" %*\r\n", launcher.display());
        let path = dir.join("agent-device.cmd");
        tokio::fs::write(&path, script).await?;
        return Ok(path);
    }
    let script = format!(
        "#!/bin/sh\n[ -x {node} ] || {{ echo {stale} >&2; exit 1; }}\nexport ELECTRON_RUN_AS_NODE=1\nexec {node} {launcher} \"$@\"\n",
        node = quote_shell(&node),
        stale = quote_shell(STALE_RUNTIME),
        launcher = quote_shell(&launcher.to_string_lossy()),
    );
    let path = dir.join("agent-device");
    tokio::fs::write(&path, script).await?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        tokio::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).await?;
    }
    Ok(path)
}

fn under(dir: &Path, parts: [&str; 2]) -> PathBuf {
    dir.join(parts[0]).join(parts[1])
}

/// `ensureAgentDeviceShim` then `bindAgentDeviceLauncher`: the unbound
/// launcher kept current, and one for `session` with its target flags built in.
pub(super) async fn bind_launcher(
    dir: &Path,
    node: &Path,
    entry: &Path,
    session: &str,
    target_args: &[String],
) -> std::io::Result<PathBuf> {
    write_launcher(&under(dir, SHIM_DIR), node, entry, &[]).await?;
    write_launcher(
        &under(dir, TARGETS_DIR).join(session),
        node,
        entry,
        target_args,
    )
    .await
}

/// `pruneAgentDeviceLaunchers`: removes the bound launchers of sessions not in `live`.
pub(super) async fn prune_launchers(dir: &Path, live: &HashSet<String>) {
    let targets = under(dir, TARGETS_DIR);
    let Ok(mut entries) = tokio::fs::read_dir(&targets).await else {
        return;
    };
    while let Ok(Some(entry)) = entries.next_entry().await {
        let name = entry.file_name().to_string_lossy().into_owned();
        if !live.contains(&name) {
            let _ = tokio::fs::remove_dir_all(entry.path()).await;
        }
    }
}

/// The startup prune: no session is live yet.
pub(super) fn prune_launchers_now(dir: &Path) {
    let Ok(entries) = std::fs::read_dir(under(dir, TARGETS_DIR)) else {
        return;
    };
    for entry in entries.flatten() {
        let _ = std::fs::remove_dir_all(entry.path());
    }
}

/// `launchers.release`: runs the launcher's `close`, best effort.
pub(super) async fn release_launcher(launcher: &Path) {
    let Some(node) = node_executable() else {
        return;
    };
    let script = launcher
        .parent()
        .unwrap_or(Path::new("."))
        .join("agent-device-launcher.mjs");
    let mut command = tokio::process::Command::new(node);
    command
        .arg(script)
        .arg("close")
        .env("ELECTRON_RUN_AS_NODE", "1")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    let Ok(child) = command.spawn() else {
        tracing::warn!(launcher = %launcher.display(), "Could not end the agent-device session");
        return;
    };
    match tokio::time::timeout(Duration::from_secs(15), child.wait_with_output()).await {
        Err(_) => {
            tracing::warn!(launcher = %launcher.display(), outcome = "timed-out", "Could not end the agent-device session")
        }
        Ok(Ok(output)) if !output.status.success() => {
            let stderr = String::from_utf8_lossy(&output.stderr);
            if !stderr.contains("SESSION_NOT_FOUND") {
                tracing::warn!(launcher = %launcher.display(), outcome = "failed", "Could not end the agent-device session");
            }
        }
        Ok(Err(_)) => {
            tracing::warn!(launcher = %launcher.display(), "Could not end the agent-device session")
        }
        Ok(Ok(_)) => {}
    }
}

fn shell_word(value: &str) -> String {
    let plain = !value.is_empty()
        && value
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || "_./:-".contains(c));
    if plain {
        value.to_owned()
    } else {
        quote_shell(value)
    }
}

/// `agentDeviceTargetArgs`: the flags agent-device selects a device by.
pub(super) fn target_args(platform: &str, id: &str) -> Vec<String> {
    if platform == "ios" {
        vec![
            "--platform".into(),
            "ios".into(),
            "--udid".into(),
            id.into(),
        ]
    } else {
        vec![
            "--platform".into(),
            "android".into(),
            "--serial".into(),
            id.into(),
        ]
    }
}

/// `agentDeviceQuickStart` for a local device with a bound launcher (no target flags).
/// The host kinds `agentDeviceQuickStart` has notes for.
pub(super) enum QuickStartHost<'a> {
    Machine,
    Simlock { install_directory: Option<&'a str> },
}

pub(super) fn quick_start(device: &Value, target: &[String], command: &str) -> String {
    quick_start_on(device, target, command, QuickStartHost::Machine)
}

pub(super) fn quick_start_on(
    device: &Value,
    target: &[String],
    command: &str,
    host: QuickStartHost,
) -> String {
    let executable = shell_word(command);
    let target = target
        .iter()
        .map(|arg| shell_word(arg))
        .collect::<Vec<_>>()
        .join(" ");
    let run = |args: &str| {
        [executable.as_str(), args, target.as_str()]
            .into_iter()
            .filter(|part| !part.is_empty())
            .collect::<Vec<_>>()
            .join(" ")
    };
    let ios = device["platform"] == "ios";
    let platform_note = match host {
        QuickStartHost::Simlock { install_directory } => [
            Some("This device is leased from a Simlock pool (alpha in T3 Code). The lease returns it to the pool, wiped, when it ends, so call device_close with shutdown: true when you are done. It starts with no apps installed.".to_owned()),
            install_directory.map(|directory| format!("agent-device accepts install for this lease only when run from {directory}.")),
        ]
        .into_iter()
        .flatten()
        .collect::<Vec<_>>()
        .join(" "),
        QuickStartHost::Machine if ios => "First use builds an XCTest runner and can take a couple of minutes; later commands are fast.".to_owned(),
        QuickStartHost::Machine => "The Android snapshot helper installs itself on first use.".to_owned(),
    };
    let name = device["name"].as_str().unwrap_or_default();
    let version = device["version"].as_str().unwrap_or_default();
    let mut lines = vec![
        format!("The user is watching {name} ({version}) in the Device panel."),
        if target.is_empty() {
            format!("Drive it with {executable}. It is bound to this device and session, so it needs no --platform, --udid, --serial, --config or --session flags. Use this exact path; login shells may reset PATH.")
        } else {
            format!("Drive it with {executable}. Use this exact path; login shells may reset PATH. Always pass {target}.")
        },
        "Every app launch, deep link, tap, text entry, alert, setting, screenshot and recording on this device goes through it. Run open <bundle-or-package-id> (apps lists them) before snapshots, CLI screenshots or input; it starts the session they need. Use device_screenshot just to look.".to_owned(),
        format!("  {}  # restarts the app; Expo dev clients connect to their Metro, --metro-port <port> picks it", run("open <bundle-or-package-id> --relaunch")),
        format!("  {}", run("open <bundle-or-package-id> <deep-link-url>")),
        format!("  {}  # accessibility tree with @eN refs", run("snapshot -i")),
        format!("  {}", run("press @e3 --settle")),
        format!("  {}", run("fill @e5 \"text\" --settle")),
        format!("  {}", run("alert accept")),
        format!("  {}  # also text-size, permission grant <name>", run("settings appearance dark")),
        format!(
            "  {}  # {}or call device_screenshot to look",
            run("screenshot /tmp/shot.png"),
            if ios {
                "iOS saves 1x points; add --pixel-density <n> (3 on most iPhones) for device resolution, "
            } else {
                ""
            }
        ),
        format!("  {}  # then record stop", run("record start --scope device /tmp/run.mp4")),
        format!("  {}", run("install <app> <path-to-.app-or-.apk>")),
        format!("Prefer snapshot refs over coordinates. Run {executable} help <command> for flags and {executable} help workflow for guides."),
        "Do not use simctl, idb, adb or ssh for those actions. They stay fine for builds, installs, code sync, Metro, port forwards, logs, test runners and what this command cannot do; come back to it for every UI action and screenshot so the user can watch.".to_owned(),
        "Each device_open returns its own command; opening another device does not retarget this one.".to_owned(),
    ];
    if target.is_empty() {
        lines.push("The command is removed when the device closes or T3 Code restarts. If it is missing, call device_open again.".to_owned());
    }
    lines.push(platform_note);
    lines.join("\n")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hashes_sessions_and_config_paths_as_the_reference() {
        // The reference hashes JSON.stringify([threadId, hostId, deviceId]).
        assert_eq!(
            session_name("thread-1", "local", "emulator-5554"),
            "t3-7043b8d15953e93c6cfc2d55"
        );
        assert_eq!(encode_uri_component("a b/c~'é"), "a%20b%2Fc~'%C3%A9");
        assert_eq!(key("local"), &hex::encode(Sha256::digest(b"local"))[..24]);
        assert!(config_path(Path::new("/s"), "local")
            .to_string_lossy()
            .starts_with("/s/device/hosts/"));
    }

    #[test]
    fn quotes_shell_words_like_the_reference() {
        assert_eq!(shell_word("/a/b-c_d.e:f"), "/a/b-c_d.e:f");
        assert_eq!(shell_word("a b"), "'a b'");
        assert_eq!(shell_word("it's"), "'it'\"'\"'s'");
    }

    #[test]
    fn launcher_source_fills_every_placeholder() {
        let source = launcher_source(
            Path::new("/usr/bin/node"),
            Path::new("/t/agent-device.mjs"),
            &["--session".into(), "t3-x".into()],
        );
        assert!(!source.contains("{{"));
        assert!(source.contains(r#"const bound = ["--session","t3-x"];"#));
        assert!(source.contains(r#"const entry = "/t/agent-device.mjs";"#));
        assert!(source.contains(r#""--wait"]);"#));
    }
}
