//! Simlock device hosts (the reference's `SimlockDeviceHost.ts`, alpha):
//! devices leased from a Simlock gateway the user runs. Opening a device
//! takes a lease through the gateway's agent-device front with this machine's
//! agent-device CLI, so agents drive it with the same session; closing it
//! returns the device to the pool, wiped. A lease left by a server that
//! stopped ends one TTL after its last renewal.
use serde_json::{json, Value};
use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    process::Stdio,
    time::{Duration, Instant},
};

pub(super) const CATALOG_TTL: Duration = Duration::from_millis(60_000);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(15);
/// The gateway queues a lease request for up to five minutes when every device is busy.
pub(super) const OPEN_TIMEOUT: Duration = Duration::from_secs(7 * 60);
const COMMAND_TIMEOUT: Duration = Duration::from_secs(2 * 60);
/// Renewal keeps a watched but idle device.
pub(super) const RENEW_INTERVAL: Duration = Duration::from_secs(2 * 60);
pub(super) const AGENT_DEVICE_PATH: &str = "/v1/agent-device";

/// `SimlockDeviceHostConfig`.
#[derive(Clone, Debug, PartialEq)]
pub(super) struct SimlockConfig {
    pub id: String,
    pub label: String,
    pub gateway_url: String,
    pub agent_token: Value,
    pub max_leases: usize,
    pub max_lease_minutes: u64,
    /// The saved entry, which `unchanged` compares whole.
    pub raw: Value,
}

impl SimlockConfig {
    pub fn from_settings(value: &Value) -> Option<Self> {
        if value["kind"] != "simlock" {
            return None;
        }
        Some(Self {
            id: value["id"].as_str()?.to_owned(),
            label: value["label"].as_str()?.to_owned(),
            gateway_url: value["gatewayUrl"].as_str()?.to_owned(),
            agent_token: value.get("agentToken").cloned().unwrap_or(json!("")),
            max_leases: value["maxLeases"].as_u64().unwrap_or(2) as usize,
            max_lease_minutes: value["maxLeaseMinutes"].as_u64().unwrap_or(60),
            raw: value.clone(),
        })
    }

    /// The gateway without a trailing slash.
    pub fn gateway(&self) -> String {
        self.gateway_url.trim_end_matches('/').to_owned()
    }

    pub fn agent_device_url(&self) -> String {
        format!("{}{AGENT_DEVICE_PATH}", self.gateway())
    }
}

/// A lease this server holds.
#[derive(Clone, Debug)]
pub(super) struct Lease {
    pub lease_id: String,
    pub platform: String,
    pub name: String,
    pub version: String,
    pub session: String,
    pub dir: PathBuf,
    pub config_path: PathBuf,
    pub install_directory: String,
    /// The token the lease was opened with; ending it must not depend on reading the secret again.
    pub token: String,
    /// When the Device panel stops keeping it (`maxLeaseMinutes` after it opened).
    pub deadline: Instant,
    pub next_renewal: Instant,
}

/// What a Simlock host keeps between calls.
#[derive(Debug)]
pub(super) struct SimlockState {
    pub config: SimlockConfig,
    /// The last answer from the gateway; empty until it gives one.
    pub platforms: Value,
    pub catalog: Option<(Instant, Vec<Value>)>,
    pub leases: indexmap::IndexMap<String, Lease>,
    pub starting: usize,
    /// Bumped by every stop, so an open that finishes after one returns its lease.
    pub stops: u64,
    /// This machine's agent-device CLI (node, entry), once the host is ready.
    pub cli: Option<(PathBuf, PathBuf)>,
}

impl SimlockState {
    pub fn new(config: SimlockConfig) -> Self {
        Self {
            config,
            platforms: json!([]),
            catalog: None,
            leases: Default::default(),
            starting: 0,
            stops: 0,
            cli: None,
        }
    }

    /// `summary`.
    pub fn summary(&self) -> Value {
        json!({
            "id": self.config.id,
            "label": self.config.label,
            "kind": "simlock",
            "platforms": self.platforms,
            "hubInstalled": true,
            "agentDeviceInstalled": true,
        })
    }
}

/// How a gateway request failed (`describeGatewayFailure`'s cases).
#[derive(Debug)]
pub(super) enum GatewayError {
    Timeout,
    Status(u16),
    Transport,
    Unreadable,
}

/// `describeGatewayFailure`: what went wrong, without the gateway's own text.
pub(super) fn describe_gateway_failure(error: &GatewayError, gateway: &str, owner: &str) -> String {
    match error {
        GatewayError::Timeout | GatewayError::Transport => {
            format!("Could not reach the Simlock gateway at {gateway}.")
        }
        GatewayError::Status(401 | 403) => {
            format!("The Simlock gateway refused the agent token for {owner}.")
        }
        GatewayError::Status(status) => {
            format!("The Simlock gateway at {gateway} answered HTTP {status}.")
        }
        GatewayError::Unreadable => {
            format!("The Simlock gateway at {gateway} sent an answer T3 could not read.")
        }
    }
}

/// `request`: one gateway call with the agent token, its JSON answer.
pub(super) async fn request(
    gateway: &str,
    token: &str,
    method: &str,
    route: &str,
    body: Option<Value>,
) -> Result<Value, GatewayError> {
    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap();
    let url = format!("{gateway}{route}");
    let builder = match method {
        "GET" => client.get(url),
        "DELETE" => client.delete(url),
        _ => client.post(url).json(&body.unwrap_or_else(|| json!({}))),
    }
    .bearer_auth(token);
    let response = match tokio::time::timeout(REQUEST_TIMEOUT, builder.send()).await {
        Err(_) => return Err(GatewayError::Timeout),
        Ok(Err(_)) => return Err(GatewayError::Transport),
        Ok(Ok(response)) => response,
    };
    if !response.status().is_success() {
        return Err(GatewayError::Status(response.status().as_u16()));
    }
    match tokio::time::timeout(REQUEST_TIMEOUT, response.json::<Value>()).await {
        Err(_) => Err(GatewayError::Timeout),
        Ok(Err(_)) => Err(GatewayError::Unreadable),
        Ok(Ok(value)) => Ok(value),
    }
}

fn platform_version(platform: &str, runtime: Option<&str>) -> String {
    let name = if platform == "ios" { "iOS" } else { "Android" };
    match runtime {
        Some(runtime) => format!("{name} {runtime}"),
        None => name.to_owned(),
    }
}

pub(super) fn catalog_id(platform: &str, model: &str) -> String {
    format!("simlock:{platform}:{model}")
}

/// `parseCatalogId`.
pub(super) fn parse_catalog_id(id: &str) -> Option<(String, String)> {
    let rest = id.strip_prefix("simlock:")?;
    let (platform, model) = rest.split_once(':')?;
    if !matches!(platform, "ios" | "android") || model.is_empty() {
        return None;
    }
    Some((platform.to_owned(), model.to_owned()))
}

/// `fetchCatalog`'s decoding: the pool's models as devices, by platform
/// (`None` for a platform T3 does not run), or `None` when it does not decode.
pub(super) fn catalog_devices(catalog: &Value) -> Option<Vec<Option<Vec<Value>>>> {
    let platforms = catalog["platforms"].as_array()?;
    let mut by_platform = Vec::new();
    for entry in platforms {
        let platform = entry["platform"].as_str()?;
        let runtimes = entry["modelRuntimes"].as_object()?;
        if !matches!(platform, "ios" | "android") {
            by_platform.push(None);
            continue;
        }
        let mut devices = Vec::new();
        for (model, list) in runtimes {
            let list = list.as_array()?;
            let kind = entry["modelClasses"][model].as_str();
            let workers = entry["modelWorkers"][model].as_array();
            if list.is_empty() || workers.is_some_and(|w| w.is_empty()) {
                continue;
            }
            if kind.is_some_and(|kind| kind != "phone" && kind != "tablet") {
                continue;
            }
            devices.push(json!({
                "id": catalog_id(platform, model),
                "platform": platform,
                "name": model,
                "version": platform_version(platform, list.last().and_then(Value::as_str)),
                "booted": false,
                "physical": false,
            }));
        }
        by_platform.push(Some(devices));
    }
    Some(by_platform)
}

/// The environment this machine's agent-device CLI runs with for a lease.
fn agent_environment() -> Vec<(String, String)> {
    let removed = regex::Regex::new(
        "^AGENT_DEVICE_(DAEMON_BASE_URL|DAEMON_AUTH_TOKEN|CONFIG|STATE_DIR|SESSION)$",
    )
    .unwrap();
    let mut env: HashMap<String, String> = std::env::vars()
        .filter(|(name, _)| !removed.is_match(name))
        .collect();
    if let Some(path) = crate::login_shell::path() {
        env.insert("PATH".into(), path.to_string_lossy().into_owned());
    }
    for (name, value) in [
        ("ELECTRON_RUN_AS_NODE", "1"),
        ("AGENT_DEVICE_NO_UPDATE_NOTIFIER", "1"),
        ("NO_COLOR", "1"),
        ("FORCE_COLOR", "0"),
    ] {
        env.insert(name.into(), value.into());
    }
    env.into_iter().collect()
}

/// `describeCliFailure`: agent-device's JSON failure as one sentence.
fn describe_cli_failure(stdout: &str) -> Option<String> {
    let failure: Value = serde_json::from_str(stdout.trim()).ok()?;
    let error = failure.get("error")?;
    let message = error["message"].as_str()?;
    let line = error["details"]["line"].as_str().map(|line| {
        let truncated: String = line.chars().take(300).collect();
        let truncated = if line.chars().count() > 300 {
            format!("{}…", truncated.chars().take(299).collect::<String>())
        } else {
            truncated
        };
        format!("The daemon answered: {truncated}")
    });
    Some(
        [
            Some(message.to_owned()),
            error["hint"].as_str().map(str::to_owned),
            line,
            error["logPath"].as_str().map(|log| format!("Log: {log}")),
        ]
        .into_iter()
        .flatten()
        .collect::<Vec<_>>()
        .join(" "),
    )
}

/// `agentDevice`: this machine's agent-device CLI against a lease's session;
/// its stdout, or the reason it failed.
pub(super) async fn agent_device(
    cli: &(PathBuf, PathBuf),
    session: &str,
    config_path: &Path,
    args: &[&str],
    cwd: Option<&Path>,
    timeout: Duration,
) -> Result<String, String> {
    let mut command = tokio::process::Command::new(&cli.0);
    command
        .arg(&cli.1)
        .args(args)
        .args(["--session", session, "--config"])
        .arg(config_path)
        .arg("--json")
        .env_clear()
        .envs(agent_environment())
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    if let Some(cwd) = cwd {
        command.current_dir(cwd);
    }
    let output = match command.spawn() {
        Ok(child) => tokio::time::timeout(timeout, child.wait_with_output()).await,
        Err(_) => return Err("agent-device could not run.".into()),
    };
    let output = match output {
        Ok(Ok(output)) => output,
        _ => return Err("agent-device could not run.".into()),
    };
    let stdout = String::from_utf8_lossy(&output.stdout).into_owned();
    if !output.status.success() {
        return Err(describe_cli_failure(&stdout).unwrap_or_else(|| {
            format!(
                "agent-device {} failed (exit code {}).",
                args.first().copied().unwrap_or_default(),
                output
                    .status
                    .code()
                    .map_or("none".to_owned(), |code| code.to_string())
            )
        }));
    }
    Ok(stdout)
}

/// `agentDevice` with a lease's timeouts.
pub(super) async fn lease_command(
    cli: &(PathBuf, PathBuf),
    session: &str,
    config_path: &Path,
    args: &[&str],
) -> Result<String, String> {
    agent_device(cli, session, config_path, args, None, COMMAND_TIMEOUT).await
}

/// `truncate` from `@t3tools/shared/String`.
pub(super) fn truncate(text: &str, max: usize) -> String {
    if text.chars().count() <= max {
        return text.to_owned();
    }
    format!(
        "{}…",
        text.chars().take(max.saturating_sub(1)).collect::<String>()
    )
}

/// A random six-hex-digit name, as the reference's lease directories have.
pub(super) fn lease_suffix() -> String {
    format!("{:06x}", rand::random::<u32>() & 0x00ff_ffff)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reads_the_catalog_as_the_reference_does() {
        let catalog = json!({"platforms": [
            {"platform": "ios", "modelRuntimes": {"iPhone 16": ["18.0", "18.2"], "Watch": ["11"], "Gone": []},
             "modelClasses": {"iPhone 16": "phone", "Watch": "watch"}},
            {"platform": "android", "modelRuntimes": {"Pixel 9": ["15"]}, "modelWorkers": {"Pixel 9": []}},
            {"platform": "tvos", "modelRuntimes": {}},
        ]});
        let devices = catalog_devices(&catalog).unwrap();
        assert_eq!(devices[0].as_ref().unwrap().len(), 1);
        assert_eq!(devices[0].as_ref().unwrap()[0]["version"], "iOS 18.2");
        assert_eq!(devices[1].as_ref().unwrap().len(), 0);
        assert!(devices[2].is_none());
        assert_eq!(
            parse_catalog_id("simlock:android:Pixel 9"),
            Some(("android".into(), "Pixel 9".into()))
        );
        assert_eq!(parse_catalog_id("lease-1"), None);
    }

    #[test]
    fn describes_failures_as_the_reference_does() {
        assert_eq!(
            describe_gateway_failure(&GatewayError::Status(401), "http://g", "Lab"),
            "The Simlock gateway refused the agent token for Lab."
        );
        assert_eq!(
            describe_cli_failure(
                r#"{"error":{"message":"No device","hint":"Try later","logPath":"/l"}}"#
            ),
            Some("No device Try later Log: /l".into())
        );
        assert_eq!(truncate("abcdef", 4), "abc…");
    }
}
