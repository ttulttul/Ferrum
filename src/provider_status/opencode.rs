//! OpenCode's status check, as the reference's `checkOpenCodeProviderStatus`
//! and the OpenCode driver's wiring.
//!
//! The runtime is probed first: `opencode --version` (10s), or for a
//! configured server URL its `/api/info` and `/global/health` (5s each). The
//! major version picks the generation:
//!
//! - 1.x: the inventory (`/provider`, `/agent`, `/skill`,
//!   `/command`) of a managed `opencode serve` or the configured server.
//! - 2.x: the model catalog (`/api/model`, `/api/provider`) of a managed
//!   `opencode serve` (with a generated `OPENCODE_PASSWORD`) or the
//!   configured server, read until two reads agree.
//!
//! A managed server stays up for 30 seconds after its last use, so the next
//! check (or the usage read beside it) reuses it. Usage comes from a 2.x
//! server's `/api/experimental/usage/limits`, else from the host's OpenCode Go
//! key. The version advisory comes from how the CLI was installed, per
//! generation.
use super::{
    compact_slash_command, maintenance, models_from_settings, now_iso, probe_env, probe_timeout,
    select_descriptor, unavailable_usage, CommandResult, Draft, ProcessEnv, RunError,
};
use base64::Engine;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, BufReader};

const VERSION_TIMEOUT_MS: u64 = 10_000;
const SERVER_PROBE_TIMEOUT_MS: u64 = 5_000;
const HEALTH_TIMEOUT_MS: u64 = 5_000;
const SERVER_START_TIMEOUT_MS: u64 = 30_000;
const CATALOG_SETTLE_TIMEOUT_MS: u64 = 5_000;
const IDLE_TTL: Duration = Duration::from_secs(30);
const PLUGIN_ACTIVATION_DEADLINE: Duration = Duration::from_secs(120);
const DISPLAY_NAME: &str = "OpenCode";

// ---------------------------------------------------------------------------
// Settings

struct Settings {
    binary: String,
    server_url: String,
    password: String,
    custom: Value,
}

impl Settings {
    fn of(config: &Value) -> Self {
        Self {
            binary: super::cli_binary(config, "opencode"),
            server_url: config["serverUrl"].as_str().unwrap_or("").to_owned(),
            // `settingsSecretLiteral`: a referenced secret is not a password.
            password: config["serverPassword"].as_str().unwrap_or("").to_owned(),
            custom: config["customModels"].clone(),
        }
    }

    fn external(&self) -> bool {
        !self.server_url.trim().is_empty()
    }
}

/// `isLocalOpenCodeServerUrl`.
fn is_local_server_url(url: &str) -> bool {
    let Ok(url) = url::Url::parse(url.trim()) else {
        return false;
    };
    if !matches!(url.scheme(), "http" | "https") {
        return false;
    }
    let host = url.host_str().unwrap_or("");
    host == "localhost"
        || host == "[::1]"
        || regex::Regex::new(r"^127\.(?:\d{1,3}\.){2}\d{1,3}$")
            .unwrap()
            .is_match(host)
}

fn basic(password: &str) -> String {
    format!(
        "Basic {}",
        base64::engine::general_purpose::STANDARD.encode(format!("opencode:{password}"))
    )
}

fn trim_optional(value: &Value) -> Option<String> {
    value
        .as_str()
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .map(str::to_owned)
}

fn locale_compare(a: &str, b: &str) -> std::cmp::Ordering {
    a.to_lowercase()
        .cmp(&b.to_lowercase())
        .then_with(|| b.cmp(a))
}

// ---------------------------------------------------------------------------
// Models

/// `DEFAULT_OPENCODE_MODEL_CAPABILITIES`, for custom models.
fn default_capabilities() -> Value {
    json!({"optionDescriptors": [
        {"id": "variant", "label": "Reasoning", "type": "select", "options": [
            {"id": "low", "label": "Low"}, {"id": "medium", "label": "Medium", "isDefault": true},
            {"id": "high", "label": "High"}, {"id": "xhigh", "label": "Extra High"}],
         "currentValue": "medium"},
        {"id": "agent", "label": "Agent", "type": "select", "options": [
            {"id": "build", "label": "Build", "isDefault": true}, {"id": "plan", "label": "Plan"}],
         "currentValue": "build"}]})
}

fn title_case_slug(value: &str) -> String {
    regex::Regex::new(r"[-_/]+")
        .unwrap()
        .split(value)
        .filter(|s| !s.is_empty())
        .map(|segment| {
            let mut chars = segment.chars();
            chars
                .next()
                .map(|c| c.to_uppercase().collect::<String>() + chars.as_str())
                .unwrap_or_default()
        })
        .collect::<Vec<_>>()
        .join(" ")
}

/// `inferDefaultVariant`.
fn default_variant(provider: &str, variants: &[String]) -> Option<String> {
    if variants.len() == 1 {
        return Some(variants[0].clone());
    }
    let has = |v: &str| variants.iter().any(|x| x == v);
    if provider == "anthropic" || provider.starts_with("google") {
        return has("high").then(|| "high".into());
    }
    if matches!(provider, "openai" | "opencode" | "opencode-go") {
        return if has("medium") {
            Some("medium".into())
        } else if has("high") {
            Some("high".into())
        } else {
            None
        };
    }
    None
}

fn variant_descriptor(provider: &str, variants: &[String]) -> Option<Value> {
    if variants.is_empty() {
        return None;
    }
    let default = default_variant(provider, variants);
    let options: Vec<(String, String, Option<String>, bool)> = variants
        .iter()
        .map(|v| {
            (
                v.clone(),
                title_case_slug(v),
                None,
                Some(v) == default.as_ref(),
            )
        })
        .collect();
    Some(select_descriptor("variant", "Reasoning", &options))
}

/// `openCodeCapabilitiesForModel` (1.x): variants and the primary agents.
fn v1_capabilities(provider: &str, model: &Value, agents: &[Value]) -> Value {
    let variants: Vec<String> = model["variants"]
        .as_object()
        .map(|v| v.keys().cloned().collect())
        .unwrap_or_default();
    let primary: Vec<&str> = agents
        .iter()
        .filter(|a| a["hidden"] != true && matches!(a["mode"].as_str(), Some("primary" | "all")))
        .filter_map(|a| a["name"].as_str())
        .collect();
    let default_agent = primary
        .iter()
        .find(|n| **n == "build")
        .or(primary.first())
        .map(|n| n.to_string());
    let mut descriptors = Vec::new();
    if let Some(d) = variant_descriptor(provider, &variants) {
        descriptors.push(d);
    }
    if !primary.is_empty() {
        let options: Vec<(String, String, Option<String>, bool)> = primary
            .iter()
            .map(|n| {
                (
                    n.to_string(),
                    title_case_slug(n),
                    None,
                    Some(n.to_string()) == default_agent,
                )
            })
            .collect();
        descriptors.push(select_descriptor("agent", "Agent", &options));
    }
    json!({"optionDescriptors": descriptors})
}

/// A model and its release, for `markOpenCodeLegacy`.
struct Release {
    provider: String,
    family: Option<String>,
    released: Option<f64>,
    deprecated: bool,
}

/// `markOpenCodeLegacy`: older releases of a family and deprecated models.
fn mark_legacy(entries: Vec<(Value, Release)>) -> Vec<Value> {
    let key = |r: &Release| r.family.as_ref().map(|f| format!("{}/{f}", r.provider));
    let mut newest: HashMap<String, f64> = HashMap::new();
    for (_, release) in &entries {
        if let (Some(k), Some(released), false) =
            (key(release), release.released, release.deprecated)
        {
            let entry = newest.entry(k).or_insert(released);
            *entry = entry.max(released);
        }
    }
    entries
        .into_iter()
        .map(|(mut model, release)| {
            let latest = key(&release).and_then(|k| newest.get(&k).copied());
            let superseded = matches!((latest, release.released), (Some(l), Some(r)) if r < l);
            if release.deprecated || superseded {
                model["isLegacy"] = json!(true);
            }
            model
        })
        .collect()
}

fn sort_by_name(mut models: Vec<Value>) -> Vec<Value> {
    models.sort_by(|a, b| {
        locale_compare(
            a["name"].as_str().unwrap_or(""),
            b["name"].as_str().unwrap_or(""),
        )
    });
    models
}

/// `Date.parse` for the catalog's release dates.
fn parse_date(value: &Value) -> Option<f64> {
    let text = value.as_str()?;
    let format = time::macros::format_description!("[year]-[month]-[day]");
    if let Ok(date) = time::Date::parse(text, &format) {
        return Some((date.midnight().assume_utc().unix_timestamp() * 1000) as f64);
    }
    time::OffsetDateTime::parse(text, &time::format_description::well_known::Rfc3339)
        .ok()
        .map(|d| (d.unix_timestamp_nanos() / 1_000_000) as f64)
}

/// `openCodeProviderModels` (1.x): connected providers' named models, or
/// `None` when there are none.
fn v1_models(custom: &Value, providers: &Value, agents: &[Value]) -> Option<Vec<Value>> {
    let connected: Vec<&str> = providers["connected"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .collect();
    let mut entries = Vec::new();
    for provider in providers["all"].as_array().into_iter().flatten() {
        let id = provider["id"].as_str().unwrap_or("");
        if !connected.contains(&id) {
            continue;
        }
        for model in provider["models"]
            .as_object()
            .into_iter()
            .flat_map(|m| m.values())
        {
            let Some(name) = trim_optional(&model["name"]) else {
                continue;
            };
            let mut entry = json!({"slug": format!("{id}/{}", model["id"].as_str().unwrap_or("")), "name": name});
            if let Some(sub) = trim_optional(&provider["name"]) {
                entry["subProvider"] = json!(sub);
            }
            entry["isCustom"] = json!(false);
            entry["capabilities"] = v1_capabilities(id, model, agents);
            entries.push((
                entry,
                Release {
                    provider: id.to_owned(),
                    family: model["family"].as_str().map(str::to_owned),
                    released: parse_date(&model["release_date"]),
                    deprecated: model["status"] == "deprecated",
                },
            ));
        }
    }
    let models = sort_by_name(mark_legacy(entries));
    (!models.is_empty()).then(|| models_from_settings(models, custom, default_capabilities()))
}

/// `openCode2ProviderModels`.
fn v2_models(custom: &Value, catalog: &[Value]) -> Vec<Value> {
    let entries = catalog
        .iter()
        .map(|model| {
            let provider = model["providerID"].as_str().unwrap_or("");
            let label = trim_optional(&model["providerName"]).unwrap_or_else(|| provider.to_owned());
            let mut entry = json!({"slug": format!("{provider}/{}", model["id"].as_str().unwrap_or("")),
                                   "name": model["name"]});
            if label.to_lowercase() != DISPLAY_NAME.to_lowercase() {
                entry["subProvider"] = json!(label);
            }
            entry["isCustom"] = json!(false);
            let variants: Vec<String> = model["variants"]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(|v| v["id"].as_str().map(str::to_owned))
                .collect();
            entry["capabilities"] =
                json!({"optionDescriptors": variant_descriptor(provider, &variants).into_iter().collect::<Vec<_>>()});
            (
                entry,
                Release {
                    provider: provider.to_owned(),
                    family: model["family"].as_str().map(str::to_owned),
                    released: model["released"].as_f64(),
                    deprecated: model["deprecated"] == true,
                },
            )
        })
        .collect();
    models_from_settings(
        sort_by_name(mark_legacy(entries)),
        custom,
        default_capabilities(),
    )
}

/// `openCodeSkillsToServerProviderSkills`.
fn v1_skills(skills: &[Value]) -> Vec<Value> {
    let mut list: Vec<Value> = skills
        .iter()
        .filter_map(|skill| {
            let name = trim_optional(&skill["name"])?;
            let path = trim_optional(&skill["location"])?;
            let mut entry = json!({"name": name, "path": path, "enabled": true});
            if let Some(description) = trim_optional(&skill["description"]) {
                entry["description"] = json!(description);
                entry["shortDescription"] = json!(description);
            }
            Some(entry)
        })
        .collect();
    list.sort_by(|a, b| {
        locale_compare(
            a["name"].as_str().unwrap_or(""),
            b["name"].as_str().unwrap_or(""),
        )
    });
    list
}

/// `openCodeCommandsToServerProviderSlashCommands`.
fn v1_commands(commands: &[Value]) -> Vec<Value> {
    let mut list = vec![compact_slash_command()];
    let mut names = vec!["compact".to_owned()];
    for command in commands {
        let Some(name) = trim_optional(&command["name"]) else {
            continue;
        };
        if names.contains(&name) || command["source"] == "skill" {
            continue;
        }
        names.push(name.clone());
        let mut entry = json!({"name": name});
        if let Some(description) = trim_optional(&command["description"]) {
            entry["description"] = json!(description);
        }
        let hints: Vec<&str> = command["hints"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(Value::as_str)
            .collect();
        if let Some(hint) = trim_optional(&json!(hints.join(" "))) {
            entry["input"] = json!({"hint": hint});
        }
        list.push(entry);
    }
    list
}

// ---------------------------------------------------------------------------
// HTTP

struct Response {
    status: u16,
    content_type: Option<String>,
    body: String,
}

fn client() -> reqwest::Client {
    super::http_client()
}

/// A GET, or `Err` with the reference's transport message.
async fn get(
    url: &str,
    authorization: Option<&str>,
    query: &[(&str, &str)],
) -> Result<Response, String> {
    let mut request = client().get(url).query(query);
    if let Some(auth) = authorization {
        request = request.header("authorization", auth);
    }
    let response = request
        .send()
        .await
        .map_err(|_| "fetch failed".to_owned())?;
    let status = response.status().as_u16();
    let content_type = response
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned);
    let body = response
        .text()
        .await
        .map_err(|_| "fetch failed".to_owned())?;
    Ok(Response {
        status,
        content_type,
        body,
    })
}

fn media_type(response: &Response) -> String {
    response
        .content_type
        .as_deref()
        .unwrap_or("")
        .split(';')
        .next()
        .unwrap_or("")
        .trim()
        .to_lowercase()
}

/// A 1.x SDK call (`throwOnError`): the JSON body, or the message
/// `wrapClientError` gives a failure.
async fn sdk_get(
    base: &str,
    path: &str,
    server: &Server,
    directory: &str,
) -> Result<Value, String> {
    let url = format!("{}{path}", base.trim_end_matches('/'));
    let auth = server.password.as_deref().map(basic);
    let response = get(&url, auth.as_deref(), &[("directory", directory)]).await?;
    if response.content_type.as_deref() == Some("text/html") {
        return Err("Request is not supported by this version of OpenCode Server (Server responded with text/html)".into());
    }
    let parsed: Option<Value> = serde_json::from_str(&response.body).ok();
    if (200..300).contains(&response.status) {
        return Ok(parsed.unwrap_or(Value::Null));
    }
    let error = parsed.unwrap_or_else(|| json!(response.body));
    let describe = format!(
        "GET {url}?directory={} → {}",
        urlencode(directory),
        response.status
    );
    Err(match &error {
        Value::Object(map) if !map.is_empty() => error["data"]["message"]
            .as_str()
            .filter(|m| !m.is_empty())
            .or_else(|| error["message"].as_str().filter(|m| !m.is_empty()))
            .or_else(|| error["name"].as_str().filter(|m| !m.is_empty()))
            .map(str::to_owned)
            .unwrap_or(describe),
        Value::String(text) if !text.is_empty() => text.clone(),
        _ => format!("opencode server {describe}: (empty response body)"),
    })
}

fn urlencode(value: &str) -> String {
    percent_encoding::utf8_percent_encode(value, percent_encoding::NON_ALPHANUMERIC).to_string()
}

// ---------------------------------------------------------------------------
// Runtime probe

#[derive(Clone, Copy, PartialEq, Debug)]
enum Generation {
    V1,
    V2,
}

#[derive(Clone, Debug)]
struct Probed {
    generation: Generation,
    version: String,
}

/// `probed`: the generation from the major version.
fn probed(version: Option<String>) -> Option<Probed> {
    let version = version?;
    let major: u64 = maintenance::parse_semver(&version)?.0;
    Some(Probed {
        generation: if major >= 2 {
            Generation::V2
        } else {
            Generation::V1
        },
        version,
    })
}

/// `probeOpenCodeBinary`.
async fn probe_binary(binary: &str, env: &ProcessEnv) -> Result<Probed, String> {
    let timeout = probe_timeout("opencode", VERSION_TIMEOUT_MS, env);
    let output: CommandResult = match super::run_cli(binary, &["--version"], env, timeout).await {
        Ok(output) => output,
        Err(RunError::NotFound) => {
            return Err(format!(
                "Failed to execute '{binary} --version': spawn {binary} ENOENT"
            ))
        }
        Err(RunError::Spawn) => {
            return Err(format!(
                "Failed to execute '{binary} --version': spawn {binary} EACCES"
            ))
        }
        Err(RunError::TimedOut) => {
            return Err(format!(
                "OpenCode CLI version probe timed out after {} seconds.",
                timeout.as_millis() as f64 / 1000.0
            ))
        }
    };
    probed(super::parse_generic_version(&output.stdout)).ok_or_else(|| {
        "Unable to determine OpenCode version from `opencode --version` output.".to_owned()
    })
}

/// `probeOpenCodeServer`: `/api/info` (2.x), then `/global/health` (1.x).
async fn probe_server(url: &str, password: &str, env: &ProcessEnv) -> Result<Probed, String> {
    let Ok(base) = url::Url::parse(url.trim()) else {
        return Err("The OpenCode server URL is not a valid http:// or https:// URL.".into());
    };
    if !matches!(base.scheme(), "http" | "https") {
        return Err("The OpenCode server URL is not a valid http:// or https:// URL.".into());
    }
    let auth = (!password.is_empty()).then(|| basic(password));
    let timeout = probe_timeout("opencode", SERVER_PROBE_TIMEOUT_MS, env);
    for path in ["/api/info", "/global/health"] {
        let mut target = base.clone();
        target.set_path(&format!("{}{path}", base.path().trim_end_matches('/')));
        let response =
            match tokio::time::timeout(timeout, get(target.as_str(), auth.as_deref(), &[])).await {
                Err(_) => {
                    return Err("Timed out while checking the OpenCode server version.".into())
                }
                Ok(Err(_)) => return Err("Couldn't reach the OpenCode server.".into()),
                Ok(Ok(response)) => response,
            };
        if response.status == 401 {
            return Err("401 Unauthorized: the OpenCode server rejected the password.".into());
        }
        if response.status != 200 || media_type(&response) != "application/json" {
            continue;
        }
        let body: Value = serde_json::from_str(&response.body).unwrap_or(Value::Null);
        let version = if path == "/api/info" {
            (body["version"].is_string() && body["pid"].as_f64().is_some_and(|p| p.fract() == 0.0))
                .then(|| body["version"].as_str().unwrap().to_owned())
        } else {
            (body["healthy"] == true && body["version"].is_string())
                .then(|| body["version"].as_str().unwrap().to_owned())
        };
        if let Some(result) = probed(version) {
            return Ok(result);
        }
    }
    Err("The server did not identify itself as OpenCode.".into())
}

/// `formatOpenCodeProbeError`: (installed, message, signed out).
fn format_probe_error(
    detail: &str,
    external: bool,
    inventory: bool,
    server_url: &str,
) -> (bool, String, bool) {
    let detail = detail.trim();
    let detail = (!detail.is_empty()
        && detail != "An error occurred in Effect.tryPromise"
        && detail != "An error occurred in Effect.try")
        .then_some(detail);
    let lower = detail.unwrap_or("").to_lowercase();
    if external {
        let auth = regex::Regex::new(r"(?:^|\s)(?:status(?:\s+code)?(?:\s*[:=]\s*|\s+)|http(?:/\d(?:\.\d)?)?\s+)(?:401|403)\b")
            .unwrap()
            .is_match(&lower)
            || regex::Regex::new(r"\b(?:unauthorized|forbidden)\b").unwrap().is_match(&lower);
        if auth {
            return (
                true,
                "OpenCode server rejected authentication. Check the server URL and password."
                    .into(),
                true,
            );
        }
        if [
            "econnrefused",
            "enotfound",
            "fetch failed",
            "networkerror",
            "timed out",
            "timeout",
            "socket hang up",
        ]
        .iter()
        .any(|n| lower.contains(n))
        {
            return (true, format!("Couldn't reach the configured OpenCode server at {server_url}. Check that the server is running and the URL is correct."), false);
        }
        return (
            true,
            detail
                .unwrap_or("Failed to connect to the configured OpenCode server.")
                .to_owned(),
            false,
        );
    }
    if lower.contains("enoent") || lower.contains("notfound") {
        return (
            false,
            "OpenCode CLI (`opencode`) is not installed or not on PATH.".into(),
            false,
        );
    }
    if lower.contains("quarantine") {
        return (true, "macOS is blocking the OpenCode binary (quarantine). Run `xattr -d com.apple.quarantine $(which opencode)` to fix this.".into(), false);
    }
    if lower.contains("invalid code signature") || lower.contains("corrupted") {
        return (true, "macOS killed the OpenCode process due to an invalid code signature. The binary may be corrupted. Try reinstalling OpenCode.".into(), false);
    }
    let label = if inventory {
        "Failed to load OpenCode provider inventory"
    } else {
        "Failed to execute OpenCode CLI health check"
    };
    (
        true,
        match detail {
            Some(detail) => format!("{label}: {detail}"),
            None => format!("{label}."),
        },
        false,
    )
}

// ---------------------------------------------------------------------------
// Managed servers

/// A running OpenCode server (`OpenCodeServerProcess`).
#[derive(Clone)]
struct Server {
    url: String,
    password: Option<String>,
    version: String,
}

struct Running {
    server: Server,
    child: tokio::process::Child,
    borrowers: usize,
    idle: Option<tokio::task::JoinHandle<()>>,
}

/// `OpenCodeServerOwner`: one managed server per instance and generation,
/// lent to each use and stopped 30 seconds after the last one.
#[derive(Default, Clone)]
pub(crate) struct Owners {
    servers: Arc<tokio::sync::Mutex<HashMap<String, Running>>>,
    probes: Arc<Mutex<HashMap<String, Probed>>>,
}

/// `OPENCODE_CONFIG_CONTENT` for a managed server.
fn config_content(env: &ProcessEnv) -> String {
    env.get("OPENCODE_CONFIG_CONTENT")
        .cloned()
        .or_else(|| std::env::var("OPENCODE_CONFIG_CONTENT").ok())
        .unwrap_or_else(|| "{}".into())
}

fn free_port() -> Result<u16, String> {
    std::net::TcpListener::bind("127.0.0.1:0")
        .and_then(|l| l.local_addr())
        .map(|a| a.port())
        .map_err(|e| format!("Failed to find available port: {e}"))
}

/// `startServerAttempt`: `opencode serve` until it reports its URL, then
/// `verify` reads its version.
async fn start_server(
    binary: &str,
    env: ProcessEnv,
    password: Option<String>,
    timeout: Duration,
    generation: Generation,
    directory: &str,
) -> Result<(Server, tokio::process::Child), StartError> {
    let port = free_port().map_err(|detail| StartError {
        detail,
        database_locked: false,
    })?;
    let args = vec![
        "serve".to_owned(),
        "--hostname=127.0.0.1".to_owned(),
        format!("--port={port}"),
    ];
    let cwd = std::env::current_dir().unwrap_or_default();
    let mut command = super::command(binary, &args, &env, &cwd);
    command
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped());
    let mut child = command.spawn().map_err(|e| StartError {
        detail: format!("Failed to spawn OpenCode server process: {e}"),
        database_locked: false,
    })?;
    let stdout = child.stdout.take();
    let stderr = child.stderr.take();
    let output = Arc::new(Mutex::new((String::new(), String::new())));
    let (url_tx, mut url_rx) = tokio::sync::mpsc::unbounded_channel::<String>();
    if let Some(stdout) = stdout {
        let output = output.clone();
        tokio::spawn(async move {
            let listening = regex::Regex::new(r"(?i)server listening on\s+(https?://\S+)").unwrap();
            let mut lines = BufReader::new(stdout).lines();
            while let Ok(Some(line)) = lines.next_line().await {
                {
                    let mut out = output.lock().unwrap_or_else(|e| e.into_inner());
                    if out.0.len() < 64 * 1024 {
                        out.0.push_str(&line);
                        out.0.push('\n');
                    }
                }
                if let Some(url) = listening.captures(&line) {
                    let _ = url_tx.send(url[1].to_owned());
                }
            }
        });
    }
    if let Some(stderr) = stderr {
        let output = output.clone();
        tokio::spawn(async move {
            let mut lines = BufReader::new(stderr).lines();
            while let Ok(Some(line)) = lines.next_line().await {
                let mut out = output.lock().unwrap_or_else(|e| e.into_inner());
                if out.1.len() < 64 * 1024 {
                    out.1.push_str(&line);
                    out.1.push('\n');
                }
            }
        });
    }
    let exited = |status: Option<std::process::ExitStatus>,
                  output: &Arc<Mutex<(String, String)>>| {
        let (stdout, stderr) = output.lock().unwrap_or_else(|e| e.into_inner()).clone();
        let code = status
            .and_then(|s| s.code())
            .map(|c| c.to_string())
            .unwrap_or_else(|| "null".into());
        let mut parts = vec![format!(
            "OpenCode server exited before startup completed (code: {code})."
        )];
        if !stdout.trim().is_empty() {
            parts.push(format!("stdout:\n{}", stdout.trim()));
        }
        if !stderr.trim().is_empty() {
            parts.push(format!("stderr:\n{}", stderr.trim()));
        }
        StartError {
            detail: parts.join("\n\n"),
            database_locked: regex::Regex::new(r"(?i)SQLITE_BUSY|database is locked")
                .unwrap()
                .is_match(&stderr),
        }
    };
    let timed_out = || StartError {
        detail: format!(
            "Timed out waiting for OpenCode server start after {}ms.",
            timeout.as_millis()
        ),
        database_locked: false,
    };
    let url = tokio::select! {
        url = url_rx.recv() => match url {
            Some(url) => url,
            None => {
                // Output closed without a URL: wait for the exit it implies.
                match tokio::time::timeout(timeout, child.wait()).await {
                    Ok(status) => {
                        tokio::time::sleep(Duration::from_millis(100)).await;
                        return Err(exited(status.ok(), &output));
                    }
                    Err(_) => return Err(timed_out()),
                }
            }
        },
        status = child.wait() => {
            // Let trailing diagnostics drain.
            tokio::time::sleep(Duration::from_millis(100)).await;
            return Err(exited(status.ok(), &output));
        }
        _ = tokio::time::sleep(timeout) => return Err(timed_out()),
    };
    let server = Server {
        url: url.clone(),
        password: password.clone(),
        version: String::new(),
    };
    let version = match generation {
        Generation::V1 => verify_v1(&server, directory, &env).await,
        Generation::V2 => verify_v2(&server, &env).await,
    }
    .map_err(|detail| StartError {
        detail,
        database_locked: false,
    })?;
    Ok((Server { version, ..server }, child))
}

/// Why a server did not start (`OpenCodeRuntimeError` with its `reason`).
struct StartError {
    detail: String,
    database_locked: bool,
}

/// `startOpenCodeServerProcess`: a locked shared database is retried three
/// times, 250ms apart.
async fn start_server_retrying(
    binary: &str,
    env: ProcessEnv,
    password: Option<String>,
    timeout: Duration,
    generation: Generation,
    directory: &str,
) -> Result<(Server, tokio::process::Child), String> {
    let mut attempt = 0;
    loop {
        match start_server(
            binary,
            env.clone(),
            password.clone(),
            timeout,
            generation,
            directory,
        )
        .await
        {
            Ok(started) => return Ok(started),
            Err(error) if error.database_locked && attempt < 3 => {
                attempt += 1;
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
            Err(error) => return Err(error.detail),
        }
    }
}

/// `verifyOpenCodeServerVersion` (1.x `/global/health`).
async fn verify_v1(server: &Server, directory: &str, env: &ProcessEnv) -> Result<String, String> {
    let timeout = probe_timeout("opencode", HEALTH_TIMEOUT_MS, env);
    let health = match tokio::time::timeout(
        timeout,
        sdk_get(&server.url, "/global/health", server, directory),
    )
    .await
    {
        Err(_) => return Err("Timed out while checking the OpenCode server version.".into()),
        Ok(result) => result?,
    };
    if health["healthy"] != true || !health["version"].is_string() {
        return Err("OpenCode server returned an invalid health response.".into());
    }
    let version = health["version"].as_str().unwrap().to_owned();
    if maintenance::parse_semver(&version).is_none() {
        return Err("OpenCode server returned an invalid version.".into());
    }
    Ok(version)
}

/// `verifyServer` (2.x `/api/info`).
async fn verify_v2(server: &Server, env: &ProcessEnv) -> Result<String, String> {
    let timeout = probe_timeout("opencode", SERVER_PROBE_TIMEOUT_MS, env);
    let url = format!("{}/api/info", server.url.trim_end_matches('/'));
    let auth = server.password.as_deref().map(basic);
    let response = match tokio::time::timeout(timeout, get(&url, auth.as_deref(), &[])).await {
        Err(_) => return Err("Timed out waiting for the OpenCode server.".into()),
        Ok(Err(_)) => return Err("Could not reach the OpenCode server.".into()),
        Ok(Ok(response)) => response,
    };
    if response.status == 401 {
        return Err("The OpenCode server rejected the server password.".into());
    }
    if !(200..300).contains(&response.status) {
        return Err(format!(
            "The OpenCode server returned HTTP {}.",
            response.status
        ));
    }
    let info: Value = serde_json::from_str(&response.body).unwrap_or(Value::Null);
    let valid = info["version"].is_string()
        && info["pid"]
            .as_f64()
            .is_some_and(|p| p.fract() == 0.0 && p >= 0.0)
        && info["urls"]
            .as_array()
            .is_some_and(|u| u.iter().all(Value::is_string))
        && info["paths"]["tmp"].is_string();
    if !valid {
        return Err("The server is not an OpenCode 2 server.".into());
    }
    Ok(info["version"].as_str().unwrap().to_owned())
}

impl Owners {
    /// `withServer`: a running managed server (started if need be), held
    /// while `use_` runs.
    async fn with_server<T, F, Fut>(
        &self,
        key: &str,
        start: impl std::future::Future<Output = Result<(Server, tokio::process::Child), String>>,
        use_: F,
    ) -> Result<T, String>
    where
        F: FnOnce(Server) -> Fut,
        Fut: std::future::Future<Output = Result<T, String>>,
    {
        let server = {
            let mut servers = self.servers.lock().await;
            let reusable = match servers.get_mut(key) {
                Some(running) => matches!(running.child.try_wait(), Ok(None)),
                None => false,
            };
            if reusable {
                let running = servers.get_mut(key).unwrap();
                if let Some(idle) = running.idle.take() {
                    idle.abort();
                }
                running.borrowers += 1;
                running.server.clone()
            } else {
                servers.remove(key);
                let (server, child) = start.await?;
                servers.insert(
                    key.to_owned(),
                    Running {
                        server: server.clone(),
                        child,
                        borrowers: 1,
                        idle: None,
                    },
                );
                server
            }
        };
        let result = use_(server.clone()).await;
        let mut servers = self.servers.lock().await;
        if let Some(running) = servers.get_mut(key).filter(|r| r.server.url == server.url) {
            running.borrowers = running.borrowers.saturating_sub(1);
            if running.borrowers == 0 {
                let owner = self.servers.clone();
                let key = key.to_owned();
                let url = server.url.clone();
                running.idle = Some(tokio::spawn(async move {
                    tokio::time::sleep(IDLE_TTL).await;
                    let mut servers = owner.lock().await;
                    if servers
                        .get(&key)
                        .is_some_and(|r| r.server.url == url && r.borrowers == 0)
                    {
                        servers.remove(&key);
                    }
                }));
            }
        }
        result
    }

    /// `makeOpenCodeRuntimeProbe`: a 2.x result is kept for good.
    fn record_probe(&self, key: &str, probe: &Result<Probed, String>) {
        let mut probes = self.probes.lock().unwrap_or_else(|e| e.into_inner());
        match probe {
            Ok(probed) if probed.generation == Generation::V2 => {
                probes.insert(key.to_owned(), probed.clone());
            }
            _ => {
                probes.remove(key);
            }
        }
    }
}

// ---------------------------------------------------------------------------
// 1.x inventory

struct Inventory {
    providers: Value,
    agents: Vec<Value>,
    skills: Vec<Value>,
    commands: Vec<Value>,
}

/// `loadOpenCodeInventory`: the provider list must load; the rest may not.
async fn load_inventory(server: &Server, directory: &str) -> Result<Inventory, String> {
    let list = |path: &'static str| async move {
        sdk_get(&server.url, path, server, directory)
            .await
            .ok()
            .and_then(|v| v.as_array().cloned())
            .unwrap_or_default()
    };
    let (providers, agents, skills, commands) = tokio::join!(
        sdk_get(&server.url, "/provider", server, directory),
        list("/agent"),
        list("/skill"),
        list("/command"),
    );
    let providers = providers?;
    if providers.is_null() {
        return Err("OpenCode provider list was empty.".into());
    }
    Ok(Inventory {
        providers,
        agents,
        skills,
        commands,
    })
}

// ---------------------------------------------------------------------------
// 2.x catalog

/// A 2.x read: `Ok(None)` while its plugins activate (503 from `plugin`).
async fn v2_get(
    server: &Server,
    path: &str,
    directory: Option<&str>,
) -> Result<Option<Value>, String> {
    let url = format!("{}{path}", server.url.trim_end_matches('/'));
    let auth = server.password.as_deref().map(basic);
    let query: Vec<(&str, &str)> = directory
        .map(|d| vec![("location[directory]", d)])
        .unwrap_or_default();
    let response = get(&url, auth.as_deref(), &query).await?;
    let body: Value = serde_json::from_str(&response.body).unwrap_or(Value::Null);
    // A decoded `ServiceUnavailableError` from the `plugin` service.
    if response.status == 503
        && body["_tag"] == "ServiceUnavailableError"
        && body["message"].is_string()
        && body["service"] == "plugin"
    {
        return Ok(None);
    }
    if !(200..300).contains(&response.status) {
        return Err(format!("status={}", response.status));
    }
    Ok(Some(body))
}

fn valid_v2_model(model: &Value) -> bool {
    let finite = |v: &Value| v.as_f64().is_some_and(f64::is_finite);
    let int = |v: &Value| v.as_f64().is_some_and(|n| n.fract() == 0.0);
    model["id"].is_string()
        && model["modelID"].is_string()
        && model["providerID"].is_string()
        && model["name"].is_string()
        && model.get("family").is_none_or(Value::is_string)
        && model["capabilities"]["tools"].is_boolean()
        && model["capabilities"]["input"].is_array()
        && model["capabilities"]["output"].is_array()
        && model["variants"]
            .as_array()
            .is_some_and(|v| v.iter().all(|v| v["id"].is_string()))
        && finite(&model["time"]["released"])
        && model["cost"].is_array()
        && matches!(
            model["status"].as_str(),
            Some("alpha" | "beta" | "deprecated" | "active")
        )
        && model["enabled"].is_boolean()
        && int(&model["limit"]["context"])
        && int(&model["limit"]["output"])
}

/// `loadOpenCode2Catalog`: the models with their providers' names (read for
/// at most 3 seconds).
async fn read_v2_catalog(
    server: &Server,
    directory: Option<&str>,
) -> Result<Option<Vec<Value>>, String> {
    let providers = async {
        match tokio::time::timeout(
            Duration::from_secs(3),
            v2_get(server, "/api/provider", directory),
        )
        .await
        {
            Ok(Ok(Some(body))) => body["data"].as_array().cloned().unwrap_or_default(),
            _ => vec![],
        }
    };
    let (models, providers) = tokio::join!(v2_get(server, "/api/model", directory), providers);
    let Some(body) =
        models.map_err(|_| "The OpenCode server could not list its models.".to_owned())?
    else {
        return Ok(None);
    };
    let Some(models) = body["data"]
        .as_array()
        .filter(|m| m.iter().all(valid_v2_model))
    else {
        return Err("The OpenCode server could not list its models.".into());
    };
    let names: HashMap<&str, &str> = providers
        .iter()
        .filter_map(|p| Some((p["id"].as_str()?, p["name"].as_str()?)))
        .collect();
    Ok(Some(
        models
            .iter()
            .map(|model| {
                let provider = model["providerID"].as_str().unwrap_or("");
                let mut entry = json!({
                    "providerID": provider, "id": model["id"], "name": model["name"],
                    "variants": model["variants"].as_array().cloned().unwrap_or_default()
                        .iter().map(|v| json!({"id": v["id"]})).collect::<Vec<_>>(),
                    "released": model["time"]["released"], "deprecated": model["status"] == "deprecated",
                });
                if let Some(name) = names.get(provider).filter(|n| !n.is_empty()) {
                    entry["providerName"] = json!(name);
                }
                if let Some(family) = model["family"].as_str() {
                    entry["family"] = json!(family);
                }
                entry
            })
            .collect(),
    ))
}

/// `catalogFingerprint`.
fn fingerprint(models: &[Value]) -> String {
    let mut lines: Vec<String> = models
        .iter()
        .map(|m| {
            let mut variants: Vec<&str> = m["variants"]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(|v| v["id"].as_str())
                .collect();
            variants.sort();
            json!([
                m["providerID"],
                m["id"],
                m["name"],
                m.get("providerName"),
                variants
            ])
            .to_string()
        })
        .collect();
    lines.sort();
    lines.join("\n")
}

// ---------------------------------------------------------------------------
// Usage

/// `readServerUsageLimits`: `Ok(None)` when the server has no usage route.
async fn v2_usage(
    server: &Server,
    directory: &str,
    checked_at: &str,
) -> Result<Option<Value>, String> {
    let url = format!(
        "{}/api/experimental/usage/limits",
        server.url.trim_end_matches('/')
    );
    let auth = server.password.as_deref().map(basic);
    let response = get(&url, auth.as_deref(), &[("location[directory]", directory)]).await?;
    if response.status == 404
        || response.status == 405
        || (response.status == 200 && media_type(&response) == "text/html")
    {
        return Ok(None);
    }
    if !(200..300).contains(&response.status) {
        return Err(format!("status={}", response.status));
    }
    let body: Value = serde_json::from_str(&response.body).map_err(|_| "decode".to_owned())?;
    if !body["location"]["directory"].is_string() {
        return Err("decode".into());
    }
    let data = &body["data"];
    if data.is_null() {
        // No Console workspace: usage is unsupported, without a message (#41415).
        return Ok(Some(unavailable_usage(checked_at, "unsupported", None)));
    }
    let workspace = &data["workspace"];
    if !workspace["id"].is_string()
        || !workspace["usageURL"].is_string()
        || !data["budgets"].is_array()
    {
        return Err("decode".into());
    }
    let external = url::Url::parse(workspace["usageURL"].as_str().unwrap())
        .ok()
        .filter(|u| matches!(u.scheme(), "http" | "https"))
        .map(|u| json!({"label": trim_optional(&workspace["name"]).unwrap_or_else(|| workspace["id"].as_str().unwrap().to_owned()), "url": u.as_str()}));
    let go = data.get("go").filter(|g| g.is_object());
    let budgets = data["budgets"].as_array().unwrap();
    if go.is_none() && budgets.is_empty() {
        let mut limits = unavailable_usage(checked_at, "unsupported", None);
        if let Some(external) = external {
            limits["externalUsage"] = external;
        }
        return Ok(Some(limits));
    }
    // Workspace budgets beyond Go's meters are reported as the reference's
    // per-budget windows.
    let mut windows = Vec::new();
    if let Some(go) = go {
        for (key, kind, label, duration) in [
            ("rolling", "session", "Session", Some(300)),
            ("weekly", "weekly", "Weekly", Some(10080)),
            ("monthly", "monthly", "Monthly", None),
        ] {
            let meter = &go[key];
            let used = if meter["limited"] == true {
                100.0
            } else {
                super::clamp_percent(meter["percent"].as_f64().ok_or("decode")?)
            };
            let mut window = json!({"id": format!("go_{key}"), "kind": kind, "label": format!("Go · {label}"),
                "usedPercent": super::js_number(used),
                "resetsAt": crate::auth_service::iso(meter["resetsAt"].as_f64().ok_or("decode")? as i64)});
            if let Some(duration) = duration {
                window["windowDurationMins"] = json!(duration);
            }
            windows.push(window);
        }
    }
    let mut limits = super::usage_limits(checked_at, windows);
    if let Some(external) = external {
        limits["externalUsage"] = external;
    }
    Ok(Some(limits))
}

/// `readOpenCodeGoUsageLimits`: the host's OpenCode Go key, if any (never
/// for a configured server). Reading usage with a key goes to opencode.ai.
async fn go_usage(enabled: bool, external: bool, env: &ProcessEnv, checked_at: &str) -> Value {
    let unsupported = unavailable_usage(checked_at, "unsupported", None);
    if !enabled || external {
        return unsupported;
    }
    let read = async {
        let home = env
            .get("HOME")
            .filter(|h| !h.is_empty())
            .or_else(|| env.get("USERPROFILE").filter(|h| !h.is_empty()))
            .cloned()
            .unwrap_or_else(|| super::home_dir().display().to_string());
        let data_home = env
            .get("XDG_DATA_HOME")
            .filter(|d| !d.is_empty())
            .cloned()
            .unwrap_or_else(|| format!("{home}/.local/share"));
        let contents = match env.get("OPENCODE_AUTH_CONTENT").filter(|c| !c.is_empty()) {
            Some(contents) => contents.clone(),
            None => match std::fs::read_to_string(Path::new(&data_home).join("opencode/auth.json"))
            {
                Ok(text) => text,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => "{}".into(),
                Err(_) => return Err(()),
            },
        };
        let auth: Value = serde_json::from_str(&contents)
            .ok()
            .filter(Value::is_object)
            .ok_or(())?;
        let api = &auth["opencode-go"];
        let go_key = if api["type"] == "api" && api["key"].is_string() {
            api["key"].as_str().map(str::to_owned)
        } else {
            env.get("OPENCODE_API_KEY").cloned()
        }
        .map(|k| k.trim().to_owned())
        .filter(|k| !k.is_empty());
        let database_file = env
            .get("OPENCODE_DB")
            .cloned()
            .unwrap_or_else(|| "opencode.db".into());
        let mut keys: Vec<String> = Vec::new();
        if env
            .get("OPENCODE_AUTH_CONTENT")
            .is_none_or(|c| c.is_empty())
            && database_file != ":memory:"
        {
            let path = Path::new(&data_home).join("opencode").join(&database_file);
            for integration in ["opencode", "opencode-go"] {
                if let Some(key) = stored_key(&path, integration) {
                    keys.push(key);
                }
            }
        }
        keys.extend(go_key);
        keys.dedup();
        let mut keys = keys.into_iter().filter(|k| !k.is_empty());
        let Some(first) = keys.next() else {
            return Ok(None);
        };
        let mut response = go_request(&first).await.ok_or(())?;
        let mut key = first;
        for next in keys {
            if response.status != 401 && response.status != 403 {
                break;
            }
            key = next;
            response = go_request(&key).await.ok_or(())?;
        }
        Ok(Some((key, response)))
    };
    let failed = || {
        unavailable_usage(
            checked_at,
            "probeFailed",
            Some("OpenCode Go could not read usage."),
        )
    };
    match tokio::time::timeout(Duration::from_secs(5), read).await {
        Ok(Ok(Some((key, response)))) => {
            go_limits(&key, &response, checked_at).unwrap_or_else(failed)
        }
        Ok(Ok(None)) => unsupported,
        _ => failed(),
    }
}

async fn go_request(key: &str) -> Option<Response> {
    let response = client()
        .get("https://opencode.ai/zen/go/v1/usage")
        .bearer_auth(key)
        .send()
        .await
        .ok()?;
    let status = response.status().as_u16();
    let body = response.text().await.ok()?;
    Some(Response {
        status,
        content_type: None,
        body,
    })
}

fn go_limits(key: &str, response: &Response, checked_at: &str) -> Option<Value> {
    if response.status == 403 {
        return Some(unavailable_usage(checked_at, "unsupported", None));
    }
    if !(200..300).contains(&response.status) {
        return None;
    }
    let body: Value = serde_json::from_str(&response.body).ok()?;
    let mut windows = Vec::new();
    for (key_name, id, kind, label, duration) in [
        (
            "rolling",
            "go_rolling",
            "session",
            "Go · Session",
            Some(300),
        ),
        ("weekly", "go_weekly", "weekly", "Go · Weekly", Some(10080)),
        ("monthly", "go_monthly", "monthly", "Go · Monthly", None),
    ] {
        let window = &body["usage"][key_name];
        let percent = window["percent"].as_f64().filter(|p| p.is_finite())?;
        let resets = time::OffsetDateTime::parse(
            window["resetsAt"].as_str()?,
            &time::format_description::well_known::Rfc3339,
        )
        .ok()?;
        let mut entry = json!({"id": id, "kind": kind, "label": label,
            "usedPercent": super::js_number(super::clamp_percent(percent)),
            "resetsAt": crate::auth_service::iso((resets.unix_timestamp_nanos() / 1_000_000) as i64)});
        if let Some(duration) = duration {
            entry["windowDurationMins"] = json!(duration);
        }
        windows.push(entry);
    }
    let mut limits = super::usage_limits(checked_at, windows);
    use sha2::Digest;
    limits["credentialFingerprint"] = json!(hex::encode(sha2::Sha256::digest(
        format!("opencode-go\0{key}").as_bytes()
    )));
    Some(limits)
}

/// The selected OpenCode Console key in OpenCode's database.
fn stored_key(path: &Path, integration: &str) -> Option<String> {
    let connection =
        rusqlite::Connection::open_with_flags(path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
            .ok()?;
    let _ = connection.busy_timeout(Duration::from_millis(100));
    let value: String = connection
        .query_row(
            "SELECT value FROM credential WHERE integration_id = ?1 AND active IS NOT 0 ORDER BY active DESC, time_created DESC, id DESC LIMIT 1",
            [integration],
            |row| row.get(0),
        )
        .ok()?;
    let stored: Value = serde_json::from_str(&value).ok()?;
    if stored["type"] != "key" || !stored["key"].is_string() {
        return None;
    }
    if let Some(server) = stored["metadata"]["server"].as_str() {
        let url = url::Url::parse(server).ok()?;
        let origin = url.origin().ascii_serialization();
        if format!("{origin}{}", url.path().trim_end_matches('/')) != "https://opencode.ai/console"
        {
            return None;
        }
    }
    Some(stored["key"].as_str()?.trim().to_owned())
}

// ---------------------------------------------------------------------------
// Maintenance

fn is_native_command_path(path: &str) -> bool {
    let normalized = path.replace('\\', "/").to_lowercase();
    normalized.ends_with("/.opencode/bin/opencode")
        || normalized.ends_with("/.opencode/bin/opencode.exe")
}

/// `openCodeUpdateFor(generation)`.
async fn maintenance_for(
    generation: Option<Generation>,
    binary: &str,
    env: &ProcessEnv,
) -> maintenance::Capabilities {
    let Some(generation) = generation else {
        return maintenance::manual(None, None);
    };
    let definition = maintenance::PackageDefinition {
        provider: "opencode",
        npm_package_name: if generation == Generation::V2 {
            "@opencode/cli"
        } else {
            "opencode-ai"
        },
        native_update: Some(maintenance::NativeUpdate {
            args: &["upgrade", "--method", "curl"],
            is_command_path: Some(is_native_command_path),
            requires_command_path: true,
        }),
        mise: Some(maintenance::MiseTool {
            tool: "opencode",
            upgrade: generation == Generation::V2,
        }),
    };
    maintenance::with_configured_npm(
        maintenance::resolve_package_managed(&definition, maintenance::context(binary, env)).await,
        &super::base_env(&[]),
    )
}

// ---------------------------------------------------------------------------
// Check

/// The 2.x presentation fields.
fn v2_presentation() -> Vec<(&'static str, Value)> {
    vec![
        ("showInteractionModeToggle", json!(true)),
        (
            "supportedRuntimeModes",
            json!([
                "approval-required",
                "auto-accept-edits",
                "auto",
                "full-access"
            ]),
        ),
    ]
}

pub(super) async fn check(
    owners: &Owners,
    instance_id: &str,
    config: &Value,
    env: &ProcessEnv,
    manifest: &Value,
) -> Draft {
    let settings = Settings::of(config);
    let checked_at = now_iso();
    let probe = probe_env(env);
    let cwd = std::env::current_dir()
        .unwrap_or_default()
        .display()
        .to_string();
    let external = settings.external();
    // The runtime probe (`runtimeProbe.refresh`), with the usage read and a
    // loopback server's installed CLI version beside it.
    let probe_result = if external {
        probe_server(&settings.server_url, &settings.password, &probe).await
    } else {
        probe_binary(&settings.binary, &probe).await
    };
    owners.record_probe(instance_id, &probe_result);
    let installed_version = async {
        if is_local_server_url(&settings.server_url) {
            probe_binary(&settings.binary, &probe)
                .await
                .ok()
                .map(|p| p.version)
        } else {
            None
        }
    };
    let maintenance_probe = async {
        let generation = if is_local_server_url(&settings.server_url) {
            match probe_binary(&settings.binary, &probe).await {
                Ok(p) => Some(p.generation),
                Err(_) => probe_result.as_ref().ok().map(|p| p.generation),
            }
        } else {
            probe_result.as_ref().ok().map(|p| p.generation)
        };
        maintenance_for(generation, &settings.binary, env).await
    };
    let usage = usage(
        owners,
        instance_id,
        &settings,
        env,
        &probe_result,
        &cwd,
        &checked_at,
    );
    let provider = provider(
        owners,
        instance_id,
        &settings,
        env,
        &probe_result,
        &cwd,
        &checked_at,
        manifest,
    );
    let (provider, usage, installed_version, caps) =
        tokio::join!(provider, usage, installed_version, maintenance_probe);
    let mut draft = provider;
    draft.usage_limits = Some(usage);
    if let Some(version) = installed_version {
        draft.extra.retain(|(k, _)| *k != "installedVersion");
        draft.extra.push(("installedVersion", json!(version)));
    }
    draft.maintenance = Some(caps);
    draft
}

/// `checkUsageLimits` (10s).
async fn usage(
    owners: &Owners,
    instance_id: &str,
    settings: &Settings,
    env: &ProcessEnv,
    probe: &Result<Probed, String>,
    cwd: &str,
    checked_at: &str,
) -> Value {
    let read = async {
        // The runtime must be known; a failed probe fails the read.
        let generation = probe.as_ref().map_err(Clone::clone)?.generation;
        let legacy = || go_usage(true, settings.external(), env, checked_at);
        if generation == Generation::V1 {
            return Ok::<Value, String>(legacy().await);
        }
        let limits = with_v2_connection(
            owners,
            instance_id,
            settings,
            env,
            cwd,
            |server| async move { v2_usage(&server, cwd, checked_at).await },
        )
        .await?;
        match limits {
            Some(limits) if limits["unavailable"]["reason"] != "unsupported" => Ok(limits),
            other => {
                let legacy = legacy().await;
                Ok(match other {
                    Some(limits) if legacy["unavailable"]["reason"] == "unsupported" => limits,
                    _ => legacy,
                })
            }
        }
    };
    match tokio::time::timeout(Duration::from_secs(10), read).await {
        Ok(Ok(limits)) => limits,
        _ => unavailable_usage(
            checked_at,
            "probeFailed",
            Some("OpenCode could not read workspace usage."),
        ),
    }
}

/// `OpenCode2Server.withConnection`.
async fn with_v2_connection<T, F, Fut>(
    owners: &Owners,
    instance_id: &str,
    settings: &Settings,
    env: &ProcessEnv,
    cwd: &str,
    use_: F,
) -> Result<T, String>
where
    F: FnOnce(Server) -> Fut,
    Fut: std::future::Future<Output = Result<T, String>>,
{
    if settings.external() {
        let server = Server {
            url: settings.server_url.trim().to_owned(),
            password: Some(settings.password.clone()),
            version: String::new(),
        };
        let version = verify_v2(&server, env).await?;
        return use_(Server { version, ..server }).await;
    }
    let password =
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(rand::random::<[u8; 32]>());
    let mut server_env = env.clone();
    server_env.remove("OPENCODE_SERVER_PASSWORD");
    server_env.insert("OPENCODE_PASSWORD".into(), password.clone());
    server_env.insert("OPENCODE_CONFIG_CONTENT".into(), config_content(env));
    let timeout = probe_timeout("opencode", SERVER_START_TIMEOUT_MS, env);
    let start = start_server_retrying(
        &settings.binary,
        server_env,
        Some(password),
        timeout,
        Generation::V2,
        cwd,
    );
    owners
        .with_server(&format!("{instance_id}\0v2"), start, use_)
        .await
}

/// `checkOpenCodeProviderStatus`.
#[allow(clippy::too_many_arguments)]
async fn provider(
    owners: &Owners,
    instance_id: &str,
    settings: &Settings,
    env: &ProcessEnv,
    probe: &Result<Probed, String>,
    cwd: &str,
    checked_at: &str,
    manifest: &Value,
) -> Draft {
    let external = settings.external();
    let fallback = |detail: &str, version: Option<String>, inventory: bool| {
        let (installed, message, signed_out) =
            format_probe_error(detail, external, inventory, &settings.server_url);
        Draft {
            installed,
            version,
            status: "error",
            auth: json!({"status": if signed_out { "unauthenticated" } else { "unknown" }}),
            checked_at: checked_at.to_owned(),
            message: Some(message),
            models: models_from_settings(vec![], &settings.custom, default_capabilities()),
            ..Default::default()
        }
    };
    let probed = match probe {
        Err(detail) => return fallback(detail, None, false),
        Ok(probed) => probed.clone(),
    };
    if probed.generation == Generation::V2 {
        return check_v2(
            owners,
            instance_id,
            settings,
            env,
            cwd,
            &probed.version,
            checked_at,
        )
        .await;
    }
    // The manifest's policy decides which versions work (#39434).
    let refuse_broken = |detected: &str| {
        let advisory = super::provider_compatibility(manifest, "opencode", detected);
        super::broken_version_detail("OpenCode", detected, advisory.as_ref()).map(|message| Draft {
            installed: true,
            version: Some(detected.to_owned()),
            status: "error",
            auth: json!({"status": "unknown"}),
            checked_at: checked_at.to_owned(),
            message: Some(message),
            models: models_from_settings(vec![], &settings.custom, default_capabilities()),
            ..Default::default()
        })
    };
    if let Some(refused) = refuse_broken(&probed.version) {
        return refused;
    }
    let installed_version: Vec<(&'static str, Value)> = if external {
        vec![]
    } else {
        vec![("installedVersion", json!(probed.version))]
    };
    let version = Arc::new(Mutex::new(probed.version.clone()));
    let load = |server: Server| {
        let version = version.clone();
        async move {
            *version.lock().unwrap_or_else(|e| e.into_inner()) = server.version.clone();
            load_inventory(&server, cwd).await
        }
    };
    let inventory = if external {
        let server = Server {
            url: settings.server_url.trim().to_owned(),
            password: (!settings.password.is_empty()).then(|| settings.password.clone()),
            version: String::new(),
        };
        match verify_v1(&server, cwd, env).await {
            Ok(v) => {
                load(Server {
                    version: v,
                    ..server
                })
                .await
            }
            Err(e) => Err(e),
        }
    } else {
        let password = (!settings.password.is_empty())
            .then(|| settings.password.clone())
            .or_else(|| env.get("OPENCODE_SERVER_PASSWORD").cloned());
        let mut server_env = env.clone();
        if let Some(password) = &password {
            server_env.insert("OPENCODE_SERVER_PASSWORD".into(), password.clone());
        }
        server_env.insert("OPENCODE_CONFIG_CONTENT".into(), config_content(env));
        let timeout = probe_timeout("opencode", SERVER_START_TIMEOUT_MS, env);
        let start = start_server_retrying(
            &settings.binary,
            server_env,
            password,
            timeout,
            Generation::V1,
            cwd,
        );
        owners
            .with_server(&format!("{instance_id}\0v1"), start, load)
            .await
    };
    let version = version.lock().unwrap_or_else(|e| e.into_inner()).clone();
    // The server that answered can differ from the CLI the probe ran.
    if version != probed.version {
        if let Some(mut refused) = refuse_broken(&version) {
            refused.extra = installed_version;
            return refused;
        }
    }
    let inventory = match inventory {
        Err(detail) => {
            let mut draft = fallback(&detail, Some(version), true);
            draft.extra = installed_version;
            return draft;
        }
        Ok(inventory) => inventory,
    };
    let catalog = v1_models(&settings.custom, &inventory.providers, &inventory.agents);
    let connected = inventory.providers["connected"]
        .as_array()
        .map(Vec::len)
        .unwrap_or(0);
    let ready = connected > 0 && catalog.is_some();
    let message = if connected > 0 && !ready {
        "OpenCode upstream providers are connected, but it did not list any models yet.".to_owned()
    } else if connected > 0 {
        format!(
            "{connected} upstream provider{} connected through {}.",
            if connected == 1 { "" } else { "s" },
            if external {
                "the configured OpenCode server"
            } else {
                "OpenCode"
            }
        )
    } else if external {
        "Connected to the configured OpenCode server, but it did not report any connected upstream providers.".to_owned()
    } else {
        "OpenCode is available, but it did not report any connected upstream providers.".to_owned()
    };
    Draft {
        installed: true,
        version: Some(version),
        status: if ready { "ready" } else { "warning" },
        auth: json!({"status": if ready { "authenticated" } else { "unknown" }, "type": "opencode"}),
        checked_at: checked_at.to_owned(),
        message: Some(message),
        models: catalog.unwrap_or_else(|| {
            models_from_settings(vec![], &settings.custom, default_capabilities())
        }),
        skills: v1_skills(&inventory.skills),
        slash_commands: v1_commands(&inventory.commands),
        extra: installed_version,
        ..Default::default()
    }
}

/// `checkOpenCode2` with `makeOpenCode2ModelLoader`'s reads.
async fn check_v2(
    owners: &Owners,
    instance_id: &str,
    settings: &Settings,
    env: &ProcessEnv,
    cwd: &str,
    version: &str,
    checked_at: &str,
) -> Draft {
    let directory = (!settings.external()).then_some(cwd);
    let latest: Arc<Mutex<Vec<Value>>> = Arc::default();
    let read = || {
        let latest = latest.clone();
        async move {
            let result = with_v2_connection(
                owners,
                instance_id,
                settings,
                env,
                cwd,
                |server| async move { read_v2_catalog(&server, directory).await },
            )
            .await;
            if let Ok(Some(models)) = &result {
                if !models.is_empty() {
                    *latest.lock().unwrap_or_else(|e| e.into_inner()) = models.clone();
                }
            }
            result
        }
    };
    // The first read waits out plugin activation (503s), for up to 2 minutes.
    let first = tokio::time::timeout(PLUGIN_ACTIVATION_DEADLINE, async {
        loop {
            match read().await {
                Ok(Some(models)) => return Ok(models),
                Ok(None) => tokio::time::sleep(Duration::from_secs(1)).await,
                Err(e) => return Err(e),
            }
        }
    })
    .await;
    let loaded: Result<Vec<Value>, String> = match first {
        Err(_) => Ok(latest.lock().unwrap_or_else(|e| e.into_inner()).clone()),
        Ok(Err(e)) => {
            let latest = latest.lock().unwrap_or_else(|e| e.into_inner()).clone();
            if latest.is_empty() {
                Err(e)
            } else {
                Ok(latest)
            }
        }
        Ok(Ok(first)) => {
            // Two agreeing reads settle a cold server's catalog.
            let settle = async {
                let mut previous = (!first.is_empty()).then(|| fingerprint(&first));
                loop {
                    tokio::time::sleep(Duration::from_millis(250)).await;
                    let models = match read().await {
                        Ok(Some(models)) => models,
                        Ok(None) => vec![],
                        Err(e) => return Err(e),
                    };
                    let current = (!models.is_empty()).then(|| fingerprint(&models));
                    if current.is_some() && current == previous {
                        return Ok(models);
                    }
                    previous = current;
                }
            };
            let timeout = probe_timeout("opencode", CATALOG_SETTLE_TIMEOUT_MS, env);
            match tokio::time::timeout(timeout, settle).await {
                Ok(Ok(models)) => Ok(models),
                Ok(Err(e)) => {
                    let latest = latest.lock().unwrap_or_else(|e| e.into_inner()).clone();
                    if latest.is_empty() {
                        Err(e)
                    } else {
                        Ok(latest)
                    }
                }
                Err(_) => Ok(latest.lock().unwrap_or_else(|e| e.into_inner()).clone()),
            }
        }
    };
    let probe = |status: &'static str, message: String| Draft {
        installed: true,
        version: Some(version.to_owned()),
        status,
        auth: json!({"status": if status == "ready" { "authenticated" } else { "unknown" }}),
        checked_at: checked_at.to_owned(),
        message: Some(message),
        models: vec![],
        slash_commands: vec![compact_slash_command()],
        extra: v2_presentation(),
        ..Default::default()
    };
    match loaded {
        Err(_) => Draft {
            models: models_from_settings(vec![], &settings.custom, default_capabilities()),
            ..probe("error", "OpenCode could not load its model list.".into())
        },
        Ok(models) if models.is_empty() => Draft {
            models: v2_models(&settings.custom, &[]),
            ..probe(
                "warning",
                "OpenCode 2 is running, but it did not list any models yet.".into(),
            )
        },
        Ok(models) => Draft {
            models: v2_models(&settings.custom, &models),
            ..probe(
                "ready",
                format!(
                    "OpenCode {version} lists {} model{}.",
                    models.len(),
                    if models.len() == 1 { "" } else { "s" }
                ),
            )
        },
    }
}

// ---------------------------------------------------------------------------
// Text generation

/// How long a text generation run may take (`GENERATION_TIMEOUT`).
const TEXT_GENERATION_TIMEOUT: Duration = Duration::from_secs(180);

/// `getOpenCodePromptFailure` then `openCodePromptResponseErrorDetail`: what a
/// prompt's `info.error` says, if it is a failure.
fn prompt_failure_detail(error: &Value) -> Option<String> {
    if !error.is_object() {
        return None;
    }
    let name = error["name"]
        .as_str()
        .map(str::trim)
        .filter(|n| !n.is_empty());
    let status = error["data"]["statusCode"].as_i64();
    let has_message = error["data"]["message"]
        .as_str()
        .is_some_and(|m| !m.trim().is_empty());
    if name.is_none() && status.is_none() && !has_message {
        return None;
    }
    Some(
        if name == Some("ProviderAuthError") || matches!(status, Some(401 | 403)) {
            "The model provider rejected the credentials. Check the provider's sign-in or API key in OpenCode.".into()
        } else if status == Some(429) {
            "The model provider's usage or rate limit was reached. Try again later.".into()
        } else if name == Some("ContextOverflowError") {
            "The prompt is too long for the selected model's context window.".into()
        } else if name == Some("MessageAbortedError") {
            "OpenCode stopped the generation.".into()
        } else if let Some(status) = status {
            format!("The model provider failed the request (HTTP {status}).")
        } else {
            "OpenCode could not generate the text.".into()
        },
    )
}

/// A request of the reference's text generation SDK client: JSON, the
/// directory in `x-opencode-directory`, Basic credentials with a password.
async fn text_request(
    server: &Server,
    method: reqwest::Method,
    path: &str,
    directory: &str,
    body: Option<&Value>,
    timeout: Duration,
) -> Result<(u16, Value), ()> {
    let url = format!("{}{path}", server.url.trim_end_matches('/'));
    let mut request = client().request(method, url).timeout(timeout).header(
        "x-opencode-directory",
        crate::thread_projection::uri_component(directory),
    );
    if let Some(password) = &server.password {
        request = request.header("authorization", basic(password));
    }
    if let Some(body) = body {
        request = request
            .header("content-type", "application/json")
            .body(body.to_string());
    }
    let response = request.send().await.map_err(|_| ())?;
    let status = response.status().as_u16();
    let text = response.text().await.map_err(|_| ())?;
    Ok((status, serde_json::from_str(&text).unwrap_or(Value::Null)))
}

/// `runAgainstServer`: a session that may only read, the prompt, its text
/// parts; the session is deleted afterwards.
async fn generate_on(
    server: &Server,
    directory: &str,
    prompt: &str,
    (provider_id, model_id): (&str, &str),
    agent: Option<&str>,
    variant: Option<&str>,
) -> Result<String, String> {
    let created = text_request(
        server,
        reqwest::Method::POST,
        "/session",
        directory,
        Some(&json!({
            "title": "T3 Code generateThreadTitle",
            // OpenCode Zen refuses free-model requests without tools; read stays.
            "permission": [
                {"permission": "*", "pattern": "*", "action": "deny"},
                {"permission": "read", "pattern": "*", "action": "allow"},
            ],
        })),
        TEXT_GENERATION_TIMEOUT,
    )
    .await;
    let session = match created {
        Ok((status, body)) if (200..300).contains(&status) => body,
        _ => return Err("OpenCode session.create request failed.".into()),
    };
    let Some(session_id) = session["id"].as_str().map(str::to_owned) else {
        return Err("OpenCode session.create returned no session payload.".into());
    };
    let result = prompt_session(
        server,
        directory,
        &session_id,
        prompt,
        (provider_id, model_id),
        agent,
        variant,
    )
    .await;
    let _ = text_request(
        server,
        reqwest::Method::DELETE,
        &format!(
            "/session/{}",
            crate::thread_projection::uri_component(&session_id)
        ),
        directory,
        None,
        Duration::from_secs(5),
    )
    .await;
    result
}

async fn prompt_session(
    server: &Server,
    directory: &str,
    session_id: &str,
    prompt: &str,
    (provider_id, model_id): (&str, &str),
    agent: Option<&str>,
    variant: Option<&str>,
) -> Result<String, String> {
    // `resolveOpenCodeVariant`: only a catalog that lacks it drops a variant.
    let variant = match variant {
        Some(variant) => {
            let catalog = text_request(
                server,
                reqwest::Method::GET,
                "/provider",
                directory,
                None,
                Duration::from_secs(10),
            )
            .await
            .ok()
            .filter(|(status, _)| (200..300).contains(status))
            .map(|(_, body)| body);
            let entry = catalog.as_ref().and_then(|catalog| {
                catalog["all"]
                    .as_array()?
                    .iter()
                    .find(|provider| provider["id"] == provider_id)
                    .and_then(|provider| provider["models"].get(model_id))
            });
            match entry {
                Some(model) if model["variants"].get(variant).is_none() => None,
                _ => Some(variant),
            }
        }
        None => None,
    };
    let mut body = json!({
        "model": {"providerID": provider_id, "modelID": model_id},
        "parts": [{"type": "text", "text": prompt}],
    });
    if let Some(agent) = agent {
        body["agent"] = json!(agent);
    }
    if let Some(variant) = variant {
        body["variant"] = json!(variant);
    }
    let (status, result) = text_request(
        server,
        reqwest::Method::POST,
        &format!(
            "/session/{}/message",
            crate::thread_projection::uri_component(session_id)
        ),
        directory,
        Some(&body),
        TEXT_GENERATION_TIMEOUT,
    )
    .await
    .map_err(|_| "OpenCode session.prompt request failed.".to_owned())?;
    if !(200..300).contains(&status) {
        return Err("OpenCode session.prompt request failed.".into());
    }
    if let Some(detail) = prompt_failure_detail(&result["info"]["error"]) {
        return Err(detail);
    }
    let text: String = result["parts"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|part| part["type"] == "text")
        .filter_map(|part| part["text"].as_str())
        .collect();
    let text = text.trim();
    if text.is_empty() {
        return Err("OpenCode returned empty output.".into());
    }
    Ok(text.to_owned())
}

/// `makeOpenCodeTextGeneration`'s run (OpenCode 1.x): the reply to `prompt`
/// from a session on the configured server, or on the managed one the
/// instance shares with its status checks.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn generate_text(
    owners: &Owners,
    instance_id: &str,
    config: &Value,
    env: &ProcessEnv,
    directory: &str,
    prompt: &str,
    model: &str,
    agent: Option<&str>,
    variant: Option<&str>,
) -> Result<String, String> {
    // `parseOpenCodeModelSlug`.
    let trimmed = model.trim();
    let Some((provider_id, model_id)) = trimmed
        .find('/')
        .filter(|&at| at > 0 && at < trimmed.len() - 1)
        .map(|at| (&trimmed[..at], &trimmed[at + 1..]))
    else {
        return Err("OpenCode model selection must use the 'provider/model' format.".into());
    };
    let settings = Settings::of(config);
    let cwd = std::env::current_dir()
        .unwrap_or_default()
        .display()
        .to_string();
    let run = async {
        if settings.external() {
            let server = Server {
                url: settings.server_url.trim().to_owned(),
                password: (!settings.password.is_empty()).then(|| settings.password.clone()),
                version: String::new(),
            };
            let probe =
                probe_server(&settings.server_url, &settings.password, &probe_env(env)).await?;
            if probe.generation == Generation::V2 {
                let version = verify_v2(&server, env).await?;
                let server = Server { version, ..server };
                return generate_v2(&server, directory, prompt, (provider_id, model_id), variant)
                    .await;
            }
            verify_v1(&server, directory, env).await?;
            return generate_on(
                &server,
                directory,
                prompt,
                (provider_id, model_id),
                agent,
                variant,
            )
            .await;
        }
        let probe = owners
            .probes
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(instance_id)
            .cloned();
        let generation = match probe {
            Some(probe) => probe.generation,
            None => {
                probe_binary(&settings.binary, &probe_env(env))
                    .await?
                    .generation
            }
        };
        if generation == Generation::V2 {
            // One OpenCode 2 process serves every directory (`OpenCode2Server`).
            return with_v2_connection(
                owners,
                instance_id,
                &settings,
                env,
                &cwd,
                |server| async move {
                    generate_v2(&server, directory, prompt, (provider_id, model_id), variant).await
                },
            )
            .await;
        }
        let password = (!settings.password.is_empty())
            .then(|| settings.password.clone())
            .or_else(|| env.get("OPENCODE_SERVER_PASSWORD").cloned());
        let mut server_env = env.clone();
        if let Some(password) = &password {
            server_env.insert("OPENCODE_SERVER_PASSWORD".into(), password.clone());
        }
        server_env.insert("OPENCODE_CONFIG_CONTENT".into(), config_content(env));
        let timeout = probe_timeout("opencode", SERVER_START_TIMEOUT_MS, env);
        let start = start_server_retrying(
            &settings.binary,
            server_env,
            password,
            timeout,
            Generation::V1,
            &cwd,
        );
        owners
            .with_server(&format!("{instance_id}\0v1"), start, |server| async move {
                generate_on(
                    &server,
                    directory,
                    prompt,
                    (provider_id, model_id),
                    agent,
                    variant,
                )
                .await
            })
            .await
    };
    match tokio::time::timeout(TEXT_GENERATION_TIMEOUT, run).await {
        Ok(result) => result,
        Err(_) => Err("OpenCode did not finish generating in time.".into()),
    }
}

/// The permissions an OpenCode 2 text generation session has: every tool
/// asks, and each ask is rejected (free models refuse denied read or shell
/// permissions outright).
const V2_GENERATION_PERMISSIONS: [(&str, &str, &str); 1] = [("*", "*", "ask")];
const V2_TOOLS_REFUSED: &str = "Tools are not available while generating text.";
const V2_FAILED: &str = "OpenCode text generation failed.";

/// One OpenCode 2 request with the server's Basic credentials.
async fn v2_request(
    server: &Server,
    method: reqwest::Method,
    path: &str,
    body: Option<&Value>,
    timeout: Duration,
) -> Result<Value, String> {
    let url = format!("{}{path}", server.url.trim_end_matches('/'));
    let mut request = client().request(method, url).timeout(timeout);
    if let Some(password) = &server.password {
        request = request.header("authorization", basic(password));
    }
    if let Some(body) = body {
        request = request
            .header("content-type", "application/json")
            .body(body.to_string());
    }
    let response = request.send().await.map_err(|_| V2_FAILED.to_owned())?;
    if !response.status().is_success() {
        return Err(V2_FAILED.to_owned());
    }
    let text = response.text().await.map_err(|_| V2_FAILED.to_owned())?;
    Ok(serde_json::from_str(&text).unwrap_or(Value::Null))
}

/// OpenCode 2's event stream (`GET /api/event`), once the server accepted
/// the subscription: each `data:` frame's JSON, until the stream ends.
async fn v2_events(server: &Server) -> Result<tokio::sync::mpsc::UnboundedReceiver<Value>, String> {
    let url = format!("{}/api/event", server.url.trim_end_matches('/'));
    let mut request = client().get(url).header("accept", "text/event-stream");
    if let Some(password) = &server.password {
        request = request.header("authorization", basic(password));
    }
    let mut response = tokio::time::timeout(Duration::from_secs(10), request.send())
        .await
        .map_err(|_| V2_FAILED.to_owned())?
        .map_err(|_| V2_FAILED.to_owned())?;
    if !response.status().is_success() {
        return Err(V2_FAILED.to_owned());
    }
    let (events, received) = tokio::sync::mpsc::unbounded_channel();
    tokio::spawn(async move {
        let mut pending = String::new();
        let mut data = String::new();
        while let Ok(Some(chunk)) = response.chunk().await {
            pending.push_str(&String::from_utf8_lossy(&chunk));
            while let Some(end) = pending.find('\n') {
                let line = pending[..end].trim_end_matches('\r').to_owned();
                pending.drain(..=end);
                if line.is_empty() {
                    if !data.is_empty() {
                        if let Ok(event) = serde_json::from_str::<Value>(&data) {
                            if events.send(event).is_err() {
                                return;
                            }
                        }
                        data.clear();
                    }
                } else if let Some(value) = line.strip_prefix("data:") {
                    if !data.is_empty() {
                        data.push('\n');
                    }
                    data.push_str(value.strip_prefix(' ').unwrap_or(value));
                }
            }
        }
    });
    Ok(received)
}

/// `OpenCode2TextGeneration.runOnServer`: a temporary session on the
/// OpenCode 2 server, whose tool requests are all refused; the reply is the
/// text the session ended with, collected from the event stream (subscribed
/// first, as replies arrive only there). The session is removed afterwards.
async fn generate_v2(
    server: &Server,
    directory: &str,
    prompt: &str,
    (provider_id, model_id): (&str, &str),
    variant: Option<&str>,
) -> Result<String, String> {
    let mut events = v2_events(server).await?;
    let mut session_id: Option<String> = None;
    let mut model = json!({"providerID": provider_id, "id": model_id});
    if let Some(variant) = variant {
        model["variant"] = json!(variant);
    }
    let permissions: Vec<Value> = V2_GENERATION_PERMISSIONS
        .iter()
        .map(|(action, resource, effect)| json!({"action": action, "resource": resource, "effect": effect}))
        .collect();
    let generate = async {
        let created = v2_request(
            server,
            reqwest::Method::POST,
            "/api/session",
            // The reference's client sends the optional fields it leaves
            // out as null.
            Some(&json!({
                "id": null,
                "parentID": null,
                "title": "T3 Code generateThreadTitle",
                "agent": null,
                "model": model,
                "location": {"directory": directory},
                "metadata": null,
                "permissions": permissions,
            })),
            TEXT_GENERATION_TIMEOUT,
        )
        .await?;
        let id = created["data"]["id"]
            .as_str()
            .ok_or_else(|| V2_FAILED.to_owned())?
            .to_owned();
        session_id = Some(id.clone());
        let session = crate::thread_projection::uri_component(&id);
        v2_request(
            server,
            reqwest::Method::POST,
            &format!("/api/session/{session}/prompt"),
            Some(&json!({"id": null, "text": prompt, "delivery": null, "resume": null})),
            TEXT_GENERATION_TIMEOUT,
        )
        .await?;
        // Texts by assistant message and ordinal, in the order they ended.
        let mut texts: Vec<(String, String)> = Vec::new();
        let collected = |texts: &[(String, String)]| {
            texts
                .iter()
                .map(|(_, text)| text.as_str())
                .collect::<Vec<_>>()
                .join("\n")
                .trim()
                .to_owned()
        };
        loop {
            let Some(event) = events.recv().await else {
                return Err("The OpenCode event stream was lost.".to_owned());
            };
            if event["data"]["sessionID"].as_str() != Some(id.as_str()) {
                continue;
            }
            match event["type"].as_str().unwrap_or_default() {
                "session.text.ended" => {
                    let key = format!(
                        "{}:{}",
                        event["data"]["assistantMessageID"]
                            .as_str()
                            .unwrap_or_default(),
                        event["data"]["ordinal"]
                    );
                    let text = event["data"]["text"]
                        .as_str()
                        .unwrap_or_default()
                        .to_owned();
                    match texts.iter_mut().find(|(k, _)| *k == key) {
                        Some(entry) => entry.1 = text,
                        None => texts.push((key, text)),
                    }
                }
                "permission.asked" => {
                    let request = event["data"]["id"].as_str().unwrap_or_default();
                    let _ = v2_request(
                        server,
                        reqwest::Method::POST,
                        &format!(
                            "/api/session/{session}/permission/{}/reply",
                            crate::thread_projection::uri_component(request)
                        ),
                        Some(&json!({"decision": "reject", "message": V2_TOOLS_REFUSED})),
                        Duration::from_secs(10),
                    )
                    .await;
                }
                "session.execution.succeeded" => return Ok(collected(&texts)),
                "session.execution.failed" => {
                    return Err("OpenCode could not generate the text.".to_owned())
                }
                "session.execution.interrupted" => {
                    return Err("OpenCode stopped the generation.".to_owned())
                }
                _ => {}
            }
        }
    };
    // The limit covers creating the session and sending the prompt too.
    let result = match tokio::time::timeout(TEXT_GENERATION_TIMEOUT, generate).await {
        Ok(result) => result,
        Err(_) => {
            if let Some(id) = &session_id {
                let _ = v2_request(
                    server,
                    reqwest::Method::POST,
                    &format!(
                        "/api/session/{}/interrupt",
                        crate::thread_projection::uri_component(id)
                    ),
                    None,
                    Duration::from_secs(5),
                )
                .await;
            }
            Err("OpenCode did not finish generating in time.".to_owned())
        }
    };
    if let Some(id) = &session_id {
        let _ = v2_request(
            server,
            reqwest::Method::DELETE,
            &format!(
                "/api/session/{}",
                crate::thread_projection::uri_component(id)
            ),
            None,
            Duration::from_secs(5),
        )
        .await;
    }
    match result? {
        text if text.is_empty() => Err("OpenCode returned empty output.".to_owned()),
        text => Ok(text),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_prompt_failure_is_worded_as_the_reference_words_it() {
        let detail = |error: Value| prompt_failure_detail(&error);
        assert_eq!(detail(json!(null)), None);
        assert_eq!(detail(json!({})), None);
        assert_eq!(
            detail(json!({"name": "ProviderAuthError"})).unwrap(),
            "The model provider rejected the credentials. Check the provider's sign-in or API key in OpenCode."
        );
        assert_eq!(
            detail(json!({"name": "APIError", "data": {"statusCode": 429}})).unwrap(),
            "The model provider's usage or rate limit was reached. Try again later."
        );
        assert_eq!(
            detail(json!({"name": "APIError", "data": {"statusCode": 502}})).unwrap(),
            "The model provider failed the request (HTTP 502)."
        );
        assert_eq!(
            detail(json!({"data": {"message": "boom"}})).unwrap(),
            "OpenCode could not generate the text."
        );
    }

    #[test]
    fn local_server_urls() {
        assert!(is_local_server_url("http://127.0.0.1:4096"));
        assert!(is_local_server_url(" http://localhost "));
        assert!(!is_local_server_url("https://example.com"));
        assert!(!is_local_server_url("ftp://127.0.0.1"));
    }

    #[test]
    fn probe_errors_like_the_reference() {
        let (installed, message, _) = format_probe_error(
            "Failed to execute 'opencode --version': spawn opencode ENOENT",
            false,
            false,
            "",
        );
        assert!(!installed);
        assert_eq!(
            message,
            "OpenCode CLI (`opencode`) is not installed or not on PATH."
        );
        let (_, message, signed_out) = format_probe_error(
            "401 Unauthorized: the OpenCode server rejected the password.",
            true,
            false,
            "x",
        );
        assert!(signed_out);
        assert!(message.contains("rejected authentication"));
    }

    #[test]
    fn variants_default_by_provider() {
        let variants = vec!["low".to_owned(), "high".to_owned()];
        assert_eq!(
            default_variant("anthropic", &variants).as_deref(),
            Some("high")
        );
        assert_eq!(default_variant("mystery", &variants), None);
        assert_eq!(title_case_slug("extra-high_x/y"), "Extra High X Y");
    }
}
