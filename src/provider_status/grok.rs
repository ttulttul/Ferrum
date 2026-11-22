//! Grok's status check, as the reference's `checkGrokProviderStatus`:
//! `grok --version`, `grok models` (sign-in and model slugs, asked again
//! after a logged-out answer), `grok inspect --json` (skills), and an ACP
//! agent that only initializes (`grok agent stdio`), whose result lists the
//! models with their reasoning levels and the slash commands. A signed-in
//! account then has its usage read from `~/.grok/auth.json` and xAI's
//! billing endpoint (`readGrokAccount`).
use super::{
    acp::AcpSession, compact_slash_command, maintenance, models_from_settings, now_iso, probe_env,
    probe_timeout, run_cli, unavailable_usage, CommandResult, Draft, ProcessEnv, RunError,
    AUTH_PROBE_TIMEOUT_MS,
};
use regex::Regex;
use serde_json::{json, Map, Value};
use std::cmp::Ordering;
use std::path::Path;
use std::time::Duration;

const VERSION_TIMEOUT_MS: u64 = 10_000;
const ACP_INITIALIZE_TIMEOUT_MS: u64 = 20_000;
const SKILLS_TIMEOUT_MS: u64 = 4_000;
const DEFAULT_MODEL_SLUG: &str = "grok-build";
const API_KEY_ENV: &str = "XAI_API_KEY";
const NPM_PACKAGE: &str = "@xai-official/grok";
const USAGE_URL: &str = "https://cli-chat-proxy.grok.com/v1/billing?format=credits";

fn empty_capabilities() -> Value {
    json!({"optionDescriptors": []})
}

fn built_in_models() -> Vec<Value> {
    vec![
        json!({"slug": DEFAULT_MODEL_SLUG, "name": "Grok Build", "isCustom": false,
                "capabilities": empty_capabilities()}),
    ]
}

/// `grokModelsFromSettings`.
pub(crate) fn models(config: &Value, built_in: Option<Vec<Value>>) -> Vec<Value> {
    models_from_settings(
        built_in.unwrap_or_else(built_in_models),
        &config["customModels"],
        empty_capabilities(),
    )
}

/// The configured binary, as given (the reference does not expand `~` for
/// Grok), or `grok`.
fn binary(config: &Value) -> String {
    config["binaryPath"]
        .as_str()
        .map(str::trim)
        .filter(|b| !b.is_empty())
        .unwrap_or("grok")
        .to_owned()
}

/// `grokProbeEnvironment`.
fn grok_probe_env(env: &ProcessEnv) -> ProcessEnv {
    let mut env = probe_env(env);
    env.insert("GROK_DISABLE_AUTOUPDATER".into(), "1".into());
    env
}

fn non_empty_string(value: &Value) -> Option<String> {
    value
        .as_str()
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .map(str::to_owned)
}

/// `resolveGrokAcpBaseModelId` (Grok has no slug aliases).
fn base_model_id(model: &str) -> String {
    let trimmed = model.trim();
    if trimmed.is_empty() {
        DEFAULT_MODEL_SLUG.into()
    } else {
        trimmed.into()
    }
}

fn valid_effort_token(value: &str) -> bool {
    Regex::new(r"(?i)^[a-z0-9][a-z0-9._-]{0,31}$")
        .unwrap()
        .is_match(value)
}

const EFFORT_RANK: [&str; 7] = ["none", "minimal", "low", "medium", "high", "xhigh", "max"];

/// `buildGrokModelCapabilities` from an ACP `ModelInfo`.
fn model_capabilities(model: &Value) -> Value {
    let meta = &model["_meta"];
    if !meta.is_object() || meta["supportsReasoningEffort"] == false {
        return empty_capabilities();
    }
    let current_effort = non_empty_string(&meta["reasoningEffort"]);
    struct Choice {
        value: String,
        label: String,
        description: Option<String>,
        advertised_default: bool,
    }
    let mut options: Vec<Choice> = Vec::new();
    for entry in meta["reasoningEfforts"].as_array().into_iter().flatten() {
        if !entry.is_object() {
            continue;
        }
        let raw_value = non_empty_string(&entry["value"]).filter(|v| valid_effort_token(v));
        let raw_id = non_empty_string(&entry["id"]).filter(|v| valid_effort_token(v));
        let Some(value) = raw_value.or(raw_id) else {
            continue;
        };
        if options.iter().any(|o| o.value == value) {
            continue;
        }
        options.push(Choice {
            label: non_empty_string(&entry["label"]).unwrap_or_else(|| value.clone()),
            description: non_empty_string(&entry["description"]),
            advertised_default: entry["default"] == true || entry["isDefault"] == true,
            value,
        });
    }
    let current = current_effort.filter(|c| options.iter().any(|o| o.value == *c));
    let defaults: Vec<&Choice> = options.iter().filter(|o| o.advertised_default).collect();
    let selected_default = defaults
        .iter()
        .find(|o| Some(&o.value) == current.as_ref())
        .or(defaults.first())
        .map(|o| o.value.clone());
    let rank = |value: &str| EFFORT_RANK.iter().position(|r| *r == value);
    if options.iter().all(|o| rank(&o.value).is_some()) {
        options.sort_by_key(|o| rank(&o.value));
    }
    if options.is_empty() {
        return empty_capabilities();
    }
    let choices: Vec<Value> = options
        .iter()
        .map(|o| {
            let mut choice = Map::new();
            choice.insert("id".into(), json!(o.value));
            choice.insert("label".into(), json!(o.label));
            if let Some(description) = &o.description {
                choice.insert("description".into(), json!(description));
            }
            if Some(&o.value) == selected_default.as_ref() {
                choice.insert("isDefault".into(), json!(true));
            }
            Value::Object(choice)
        })
        .collect();
    let mut descriptor = json!({"id": "reasoningEffort", "label": "Reasoning", "type": "select", "options": choices});
    if let Some(value) = current.or(selected_default) {
        descriptor["currentValue"] = json!(value);
    }
    json!({"optionDescriptors": [descriptor]})
}

/// `sessionModelStateFromInitialize`: `_meta.modelState` when it is an ACP
/// `SessionModelState`.
fn model_state(initialized: &Value) -> Option<&Value> {
    let state = &initialized["_meta"]["modelState"];
    let valid = state["currentModelId"].is_string()
        && state["availableModels"].as_array().is_some_and(|models| {
            models.iter().all(|m| {
                m["modelId"].is_string()
                    && m["name"].is_string()
                    && matches!(
                        m.get("description"),
                        None | Some(Value::Null | Value::String(_))
                    )
                    && matches!(m.get("_meta"), None | Some(Value::Null | Value::Object(_)))
            })
        })
        && matches!(
            state.get("_meta"),
            None | Some(Value::Null | Value::Object(_))
        );
    valid.then_some(state)
}

/// `buildGrokModelsFromSessionModelState`.
fn models_from_state(state: Option<&Value>) -> Vec<Value> {
    let Some(state) = state else {
        return vec![];
    };
    let current = state["currentModelId"].as_str().unwrap_or("").trim();
    let mut seen: Vec<String> = Vec::new();
    let mut models = Vec::new();
    for model in state["availableModels"].as_array().into_iter().flatten() {
        let model_id = model["modelId"].as_str().unwrap_or("");
        let slug = base_model_id(model_id);
        if seen.contains(&slug) {
            continue;
        }
        seen.push(slug.clone());
        let name = model["name"].as_str().unwrap_or("").trim();
        let mut entry = json!({"slug": slug, "name": if name.is_empty() { slug.as_str() } else { name }, "isCustom": false});
        if model_id.trim() == current {
            entry["isDefault"] = json!(true);
        }
        entry["capabilities"] = model_capabilities(model);
        models.push(entry);
    }
    models
}

/// `grokSlashCommandsFromInitialize`: `/compact`, then the agent's
/// `availableCommands` (without `always-approve` and `context`).
fn slash_commands(initialized: &Value) -> Vec<Value> {
    let mut commands: Vec<(String, Value)> = vec![("compact".into(), compact_slash_command())];
    for entry in initialized["_meta"]["availableCommands"]
        .as_array()
        .into_iter()
        .flatten()
    {
        let (Some(name), Some(description)) =
            (entry["name"].as_str(), entry["description"].as_str())
        else {
            continue;
        };
        let hint = match entry.get("input") {
            None | Some(Value::Null) => None,
            Some(input) => match input["hint"].as_str() {
                Some(hint) if input.is_object() => Some(hint.trim().to_owned()),
                _ => continue,
            },
        };
        let name = name.trim();
        if name.is_empty()
            || name.eq_ignore_ascii_case("always-approve")
            || name.eq_ignore_ascii_case("context")
        {
            continue;
        }
        let mut command = json!({"name": name});
        if !description.trim().is_empty() {
            command["description"] = json!(description.trim());
        }
        if let Some(hint) = hint.filter(|h| !h.is_empty()) {
            command["input"] = json!({"hint": hint});
        }
        match commands.iter_mut().find(|(n, _)| n == name) {
            Some(existing) => existing.1 = command,
            None => commands.push((name.to_owned(), command)),
        }
    }
    commands.into_iter().map(|(_, c)| c).collect()
}

/// `parseGrokModelsCliOutput`: the sign-in verdict and the bulleted slugs.
fn parse_models_output(output: &str) -> (Option<bool>, Vec<Value>) {
    let authenticated = if Regex::new(r"(?i)you are logged in")
        .unwrap()
        .is_match(output)
    {
        Some(true)
    } else if Regex::new(r"(?i)not authenticated|not logged in")
        .unwrap()
        .is_match(output)
    {
        Some(false)
    } else {
        None
    };
    let bullet = Regex::new(r"^\s*[*-]\s+(\S+)(.*)$").unwrap();
    let separator = Regex::new(r"[-_]").unwrap();
    let default_marker = Regex::new(r"(?i)\(default\)").unwrap();
    let mut seen: Vec<String> = Vec::new();
    let mut models = Vec::new();
    for line in output.split('\n') {
        let line = line.strip_suffix('\r').unwrap_or(line);
        let Some(captures) = bullet.captures(line) else {
            continue;
        };
        let slug = base_model_id(&captures[1]);
        if seen.contains(&slug) {
            continue;
        }
        seen.push(slug.clone());
        let name = separator
            .split(&slug)
            .map(|part| {
                if part.eq_ignore_ascii_case("grok") {
                    "Grok"
                } else {
                    part
                }
            })
            .collect::<Vec<_>>()
            .join(" ");
        let mut model = json!({"slug": slug, "name": name, "isCustom": false});
        if default_marker.is_match(&captures[2]) {
            model["isDefault"] = json!(true);
        }
        model["capabilities"] = empty_capabilities();
        models.push(model);
    }
    (authenticated, models)
}

async fn probe_models(
    binary: &str,
    env: &ProcessEnv,
    timeout: Duration,
) -> (Option<bool>, Vec<Value>) {
    match run_cli(binary, &["models"], env, timeout).await {
        Ok(output) if output.code == 0 => {
            parse_models_output(&format!("{}\n{}", output.stdout, output.stderr))
        }
        _ => (None, vec![]),
    }
}

/// `localeCompare` on skill names, close enough for ASCII names: case-blind
/// first, lower case before upper.
fn locale_compare(a: &str, b: &str) -> Ordering {
    a.to_lowercase()
        .cmp(&b.to_lowercase())
        .then_with(|| b.cmp(a))
}

/// `discoverGrokSkills`: `grok inspect --json` in the server's directory.
async fn discover_skills(binary: &str, env: &ProcessEnv, timeout: Duration) -> Vec<Value> {
    let Ok(output) = run_cli(binary, &["inspect", "--json"], env, timeout).await else {
        return vec![];
    };
    if output.code != 0 {
        return vec![];
    }
    decode_skills(&output.stdout).unwrap_or_default()
}

fn decode_skills(stdout: &str) -> Option<Vec<Value>> {
    let parsed: Value = serde_json::from_str(stdout).ok()?;
    let entries = parsed.as_object()?.get("skills")?.as_array()?;
    let mut by_name: Vec<(String, Value)> = Vec::new();
    for entry in entries {
        if !entry.is_object() {
            continue;
        }
        let name = entry["name"].as_str().map(str::trim).unwrap_or("");
        let source = entry["source"].as_object();
        let path = source
            .and_then(|s| s.get("path"))
            .and_then(Value::as_str)
            .map(str::trim)
            .unwrap_or("");
        if name.is_empty() || path.is_empty() {
            continue;
        }
        let mut skill =
            json!({"name": name, "path": path, "enabled": entry["userInvocable"] != false});
        if let Some(scope) = source
            .and_then(|s| s.get("type"))
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|s| !s.is_empty())
        {
            skill["scope"] = json!(scope);
        }
        if let Some(description) = entry["description"]
            .as_str()
            .map(str::trim)
            .filter(|d| !d.is_empty())
        {
            skill["description"] = json!(description);
        }
        match by_name.iter_mut().find(|(n, _)| n == name) {
            Some(existing) => existing.1 = skill,
            None => by_name.push((name.to_owned(), skill)),
        }
    }
    by_name.sort_by(|a, b| locale_compare(&a.0, &b.0));
    Some(by_name.into_iter().map(|(_, s)| s).collect())
}

/// The ACP agent's `initialize` result (models and slash commands), without
/// `authenticate` or `session/new`.
async fn acp_initialize(binary: &str, env: &ProcessEnv) -> Option<Value> {
    let mut env = env.clone();
    env.insert("GROK_OAUTH2_REFERRER".into(), "t3code".into());
    let cwd = std::env::current_dir().unwrap_or_default();
    let args = vec!["agent".to_owned(), "stdio".to_owned()];
    let mut session = AcpSession::start(binary, &args, &env, &cwd).ok()?;
    session
        .initialize_with_meta(
            "t3-code-provider-probe",
            Some(json!({"clientType": "extension"})),
        )
        .await
        .ok()
}

// ---------------------------------------------------------------------------
// Account

const ALTERNATE_AUTH_ENV: [&str; 12] = [
    "GROK_OIDC_ISSUER",
    "GROK_OIDC_CLIENT_ID",
    "GROK_OAUTH2_ISSUER",
    "GROK_OAUTH2_CLIENT_ID",
    "GROK_OAUTH2_PRINCIPAL_TYPE",
    "GROK_OAUTH2_PRINCIPAL_ID",
    "GROK_AUTH_PROVIDER_COMMAND",
    "GROK_LOCAL_AUTH",
    "GROK_CLI_CHAT_PROXY_BASE_URL",
    "GROK_MODELS_BASE_URL",
    "GROK_CONFIG",
    "GROK_CONFIG_PATH",
];

/// `readGrokCredential`: `Ok(None)` when the CLI's own credential cannot be
/// read safely, `Err` when reading it failed.
fn read_credential(env: &ProcessEnv) -> Result<Option<Value>, ()> {
    let set = |name: &str| env.get(name).is_some_and(|v| !v.trim().is_empty());
    if set(API_KEY_ENV) || ALTERNATE_AUTH_ENV.iter().any(|name| set(name)) {
        return Ok(None);
    }
    let home = env
        .get("GROK_HOME")
        .map(|h| h.trim().to_owned())
        .filter(|h| !h.is_empty())
        .unwrap_or_else(|| {
            let base = env
                .get("HOME")
                .filter(|h| !h.is_empty())
                .or_else(|| env.get("USERPROFILE").filter(|h| !h.is_empty()))
                .cloned()
                .unwrap_or_else(|| super::home_dir().display().to_string());
            Path::new(&base).join(".grok").display().to_string()
        });
    let read = |path: &Path, missing: &str| match std::fs::read_to_string(path) {
        Ok(text) => Ok(text),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(missing.to_owned()),
        Err(_) => Err(()),
    };
    let sections =
        Regex::new(r#"(?m)^\s*(?:\[\[?\s*)?["']?(?:auth|grok_com_config|endpoints)["']?\s*[.\]=]"#)
            .unwrap();
    let home_path = Path::new(&home);
    for config in [
        home_path.join("config.toml"),
        home_path.join("managed_config.toml"),
        home_path.join("requirements.toml"),
        "/etc/grok/managed_config.toml".into(),
        "/etc/grok/requirements.toml".into(),
    ] {
        if sections.is_match(&read(&config, "")?) {
            return Ok(None);
        }
    }
    let contents = match env
        .get("GROK_AUTH")
        .map(|v| v.trim())
        .filter(|v| !v.is_empty())
    {
        Some(contents) => contents.to_owned(),
        None => read(&home_path.join("auth.json"), "{}")?,
    };
    let credentials: Value = serde_json::from_str(&contents).map_err(|_| ())?;
    let object = credentials.as_object().ok_or(())?;
    for entry in object.values() {
        let valid = entry.is_object()
            && ["key", "auth_mode", "email"]
                .iter()
                .all(|k| matches!(entry.get(*k), None | Some(Value::String(_))));
        if !valid {
            return Err(());
        }
    }
    let credential = object
        .get("https://auth.x.ai::b1a00492-073a-47ea-816f-4c329264a828")
        .or_else(|| object.get("https://accounts.x.ai/sign-in"));
    Ok(credential.filter(|c| c["auth_mode"] != "api_key").cloned())
}

/// `parsePeriodBoundary`: milliseconds since the epoch, if valid.
fn period_boundary(value: Option<&str>) -> Option<i64> {
    let value = value?;
    if value != value.trim() {
        return None;
    }
    let pattern = r"^(?:\d{4}|[+-]\d{6})-(?:0[1-9]|1[0-2])-(?:0[1-9]|[12]\d|3[01])T(?:(?:[01]\d|2[0-3]):[0-5]\d(?::[0-5]\d(?:\.\d+)?)?|24:00(?::00(?:\.0+)?)?)(?:Z|[+-](?:[01]\d|2[0-3]):[0-5]\d)$";
    if !Regex::new(pattern).unwrap().is_match(value) {
        return None;
    }
    parse_rfc3339_ms(value)
}

/// Milliseconds since the epoch of an RFC 3339 timestamp (`Date.parse`).
fn parse_rfc3339_ms(value: &str) -> Option<i64> {
    let parsed =
        time::OffsetDateTime::parse(value, &time::format_description::well_known::Rfc3339).ok()?;
    Some((parsed.unix_timestamp_nanos() / 1_000_000) as i64)
}

/// `grokUsageResponseToLimits`.
fn usage_to_limits(response: &Value, checked_at: &str) -> Value {
    let period = &response["config"]["currentPeriod"];
    let period_type = period["type"]
        .as_str()
        .map(|t| t.trim_start_matches("USAGE_PERIOD_TYPE_"));
    let kind = match period_type {
        Some("WEEKLY") => "weekly",
        Some("MONTHLY") => "monthly",
        _ => "other",
    };
    let start = period_boundary(period["start"].as_str());
    let end = period_boundary(period["end"].as_str());
    let mut used = response["config"]["creditUsagePercent"].as_f64();
    if used.is_none()
        && matches!(kind, "weekly" | "monthly")
        && matches!((start, end), (Some(s), Some(e)) if s < e)
    {
        used = Some(0.0);
    }
    let Some(used) = used.filter(|u| u.is_finite()) else {
        return super::usage_limits(checked_at, vec![]);
    };
    let mut window = json!({
        "id": "subscription",
        "kind": kind,
        "label": match kind { "weekly" => "Weekly", "monthly" => "Monthly", _ => "Subscription" },
        "usedPercent": super::js_number(super::clamp_percent(used)),
    });
    if let Some(reset) = period["end"].as_str().and_then(parse_rfc3339_ms) {
        window["resetsAt"] = json!(crate::auth_service::iso(reset));
    }
    if let (Some(s), Some(e)) = (start, end) {
        let minutes = ((e - s) as f64 / 60_000.0).round() as i64;
        if minutes > 0 {
            window["windowDurationMins"] = json!(minutes);
        }
    }
    super::usage_limits(checked_at, vec![window])
}

/// `readGrokAccount`: the account's email and usage limits.
async fn read_account(env: &ProcessEnv) -> (Option<String>, Value) {
    let checked_at = now_iso();
    let probe_failed = unavailable_usage(
        &checked_at,
        "probeFailed",
        Some("Grok could not read usage limits."),
    );
    let Ok(credential) = read_credential(env) else {
        return (None, probe_failed);
    };
    let email = credential
        .as_ref()
        .and_then(|c| non_empty_string(&c["email"]));
    let Some(token) = credential
        .as_ref()
        .and_then(|c| non_empty_string(&c["key"]))
    else {
        return (email, unavailable_usage(&checked_at, "unsupported", None));
    };
    let fetch = async {
        let response = reqwest::Client::new()
            .get(USAGE_URL)
            .bearer_auth(token)
            .send()
            .await
            .ok()?;
        let status = response.status();
        let body: Value = response.json().await.ok()?;
        if status.as_u16() == 403 {
            if let Some(error) = body["error"].as_str() {
                if error.contains("WKE=privacy-mode:") {
                    return Some(unavailable_usage(
                        &checked_at,
                        "probeFailed",
                        Some("Switch your xAI account to Privacy Mode to show Grok usage limits."),
                    ));
                }
            }
        }
        if !status.is_success() {
            return None;
        }
        Some(usage_to_limits(&body, &checked_at))
    };
    let usage = tokio::time::timeout(Duration::from_secs(10), fetch)
        .await
        .ok()
        .flatten()
        .unwrap_or(probe_failed);
    (email, usage)
}

// ---------------------------------------------------------------------------
// Maintenance

/// The reference's Grok `UPDATE` resolver: mise, a Nix store or a Homebrew
/// cask stay with their manager, anything else updates with `grok update`
/// (in its own `$GROK_HOME` for a script install).
async fn maintenance(binary: &str, env: &ProcessEnv) -> maintenance::Capabilities {
    let package = Some(NPM_PACKAGE);
    let Some(context) = maintenance::context(binary, env) else {
        return maintenance::manual(package, None);
    };
    let mise = maintenance::MiseTool {
        tool: "grok",
        upgrade: true,
    };
    if let Some(caps) = maintenance::resolve_mise(package, Some(&mise), &context).await {
        return caps;
    }
    if [&context.resolved_command_path, &context.real_command_path]
        .iter()
        .any(|p| maintenance::is_nix_store(p))
    {
        return maintenance::manual(package, Some(&context.real_command_path));
    }
    if let Some(homebrew) = maintenance::homebrew_ownership(&context.real_command_path) {
        if homebrew.cask {
            return maintenance::resolve_homebrew(package, &homebrew, &context)
                .await
                .unwrap_or_else(|| maintenance::manual(package, Some(&context.real_command_path)));
        }
    }
    let args = vec!["update".to_owned()];
    maintenance::Capabilities {
        package_name: package.map(str::to_owned),
        update: Some(maintenance::Update {
            command: format!(
                "{} update",
                maintenance::quote(&context.resolved_command_path)
            ),
            executable: context.resolved_command_path.clone(),
            args,
            lock_key: "grok".into(),
        }),
        install_path: None,
        manual_command: None,
        compare_versions: None,
    }
}

pub(super) async fn check(config: &Value, env: &ProcessEnv) -> Draft {
    let checked_at = now_iso();
    let probe = grok_probe_env(env);
    let binary = binary(config);
    let api_key = env.get(API_KEY_ENV).is_some_and(|k| !k.trim().is_empty());
    let fallback_models = models(config, None);
    let caps = maintenance(&binary, env).await;
    let failed = |installed: bool, version: Option<String>, message: &str| Draft {
        installed,
        version,
        status: "error",
        auth: json!({"status": "unknown"}),
        checked_at: checked_at.clone(),
        message: Some(message.to_owned()),
        models: fallback_models.clone(),
        maintenance: Some(caps.clone()),
        ..Default::default()
    };
    let version_output: CommandResult = match run_cli(
        &binary,
        &["--version"],
        &probe,
        probe_timeout("grok", VERSION_TIMEOUT_MS, env),
    )
    .await
    {
        Ok(output) => output,
        Err(RunError::NotFound) => {
            return failed(
                false,
                None,
                "Grok CLI (`grok`) is not installed or not on PATH.",
            )
        }
        Err(RunError::Spawn) => {
            return failed(true, None, "Failed to execute Grok CLI health check.")
        }
        Err(RunError::TimedOut) => {
            return failed(
                true,
                None,
                "Grok CLI is installed but timed out while running `grok --version`.",
            )
        }
    };
    let version = super::cli_version(&version_output);
    if version_output.code != 0 {
        return failed(true, version, "Grok CLI is installed but failed to run.");
    }
    let auth_timeout = probe_timeout("grok", AUTH_PROBE_TIMEOUT_MS, env);
    let (mut authenticated, mut cli_models) = probe_models(&binary, &probe, auth_timeout).await;
    // An expired token prints the logged-out line, then the CLI refreshes it.
    if authenticated == Some(false) && !api_key {
        let recheck = probe_models(&binary, &probe, auth_timeout).await;
        if recheck.0.is_some() {
            (authenticated, cli_models) = recheck;
        }
    }
    let mut auth = if api_key {
        json!({"status": "authenticated", "type": "api_key", "label": "xAI API key"})
    } else {
        match authenticated {
            Some(true) => {
                json!({"status": "authenticated", "type": "cached_token", "label": "Grok account"})
            }
            Some(false) => json!({"status": "unauthenticated"}),
            None => json!({"status": "unknown"}),
        }
    };
    let skills = discover_skills(
        &binary,
        &probe,
        probe_timeout("grok", SKILLS_TIMEOUT_MS, env),
    )
    .await;
    let initialized = tokio::time::timeout(
        probe_timeout("grok", ACP_INITIALIZE_TIMEOUT_MS, env),
        acp_initialize(&binary, &probe),
    )
    .await
    .ok()
    .flatten();
    let acp_failed = initialized.is_none();
    let acp_models = initialized
        .as_ref()
        .map(|i| models_from_state(model_state(i)))
        .unwrap_or_default();
    let discovered = if acp_models.is_empty() {
        cli_models
    } else {
        acp_models
    };
    let models = if discovered.is_empty() {
        fallback_models.clone()
    } else {
        models(config, Some(discovered.clone()))
    };
    let incomplete = if acp_failed {
        Some("Grok CLI is installed but ACP initialize failed. Model options may be incomplete.")
    } else if discovered.is_empty() {
        Some("Grok CLI did not list any models. Model options may be incomplete.")
    } else {
        None
    };
    let mut usage_limits = None;
    if auth["status"] == "authenticated" {
        let (email, usage) = read_account(env).await;
        if let Some(email) = email {
            auth["email"] = json!(email);
        }
        usage_limits = Some(usage);
    }
    let base = Draft {
        installed: true,
        version,
        auth: auth.clone(),
        checked_at: checked_at.clone(),
        models,
        skills,
        usage_limits,
        maintenance: Some(caps.clone()),
        ..Default::default()
    };
    if auth["status"] == "unauthenticated" {
        return Draft {
            status: "error",
            message: Some("Grok CLI is installed but not logged in. Run `grok login`.".into()),
            ..base
        };
    }
    Draft {
        status: if incomplete.is_some() {
            "warning"
        } else {
            "ready"
        },
        message: incomplete.map(str::to_owned),
        slash_commands: initialized
            .as_ref()
            .map(slash_commands)
            .unwrap_or_else(|| vec![compact_slash_command()]),
        ..base
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reads_models_and_sign_in_from_the_cli() {
        let (auth, models) = parse_models_output(
            "You are logged in.\n* grok-4 (default)\n- grok_code_fast\n* grok-4\n",
        );
        assert_eq!(auth, Some(true));
        assert_eq!(models.len(), 2);
        assert_eq!(models[0]["name"], "Grok 4");
        assert_eq!(models[0]["isDefault"], true);
        assert_eq!(models[1]["name"], "Grok code fast");
    }

    #[test]
    fn orders_reasoning_levels_from_low_to_high() {
        let caps = model_capabilities(
            &json!({"_meta": {"reasoningEffort": "high", "reasoningEfforts": [
            {"value": "high", "label": "High", "default": true}, {"value": "low"}]}}),
        );
        let descriptor = &caps["optionDescriptors"][0];
        assert_eq!(descriptor["options"][0]["id"], "low");
        assert_eq!(descriptor["currentValue"], "high");
    }

    #[test]
    fn subscription_usage_from_billing() {
        let limits = usage_to_limits(
            &json!({"config": {"creditUsagePercent": 12.5, "currentPeriod": {"type": "USAGE_PERIOD_TYPE_MONTHLY",
                "start": "2026-11-01T00:00:00Z", "end": "2026-12-01T00:00:00Z"}}}),
            "now",
        );
        assert_eq!(limits["windows"][0]["kind"], "monthly");
        assert_eq!(limits["windows"][0]["resetsAt"], "2026-12-01T00:00:00.000Z");
        assert_eq!(limits["windows"][0]["windowDurationMins"], 43200);
    }
}
