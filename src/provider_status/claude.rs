//! Claude's status check, as the reference's `checkClaudeProviderStatus`:
//! `claude --version`, then the Agent SDK's capability probe, a Claude Code
//! session that initializes without a prompt, reads its usage and settings,
//! and stops. No request reaches Anthropic's API.
use super::{
    clamp_percent, compact_slash_command, expand_home, js_number, models_from_settings, now_iso,
    parse_generic_version, probe_env, probe_timeout, run_collect, unavailable_usage, usage_limits,
    Draft, ProcessEnv, ProviderStatus, RunError,
};
use serde_json::{json, Value};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

/// `CLAUDE_VERSION_CHECK_TIMEOUT_MS`.
const VERSION_TIMEOUT_MS: u64 = 20_000;
/// `CAPABILITIES_PROBE_TIMEOUT_MS`.
const CAPABILITIES_TIMEOUT_MS: u64 = 25_000;
/// `DEFAULT_TIMEOUT_MS`: the usage and settings reads after initializing.
const READ_TIMEOUT: Duration = Duration::from_millis(4_000);
/// `CAPABILITIES_PROBE_TTL`: a probe that read usage is reused this long.
const CAPABILITIES_TTL: Duration = Duration::from_secs(5 * 60);
const SDK_VERSION: &str = "0.3.321";
const MANAGED_MCP_CONFIG: &str = "/etc/claude-code/managed-mcp.json";

/// Variables a Claude Code session that started this server exports, which
/// would make a child claude act as part of that session.
pub(crate) const HOST_SESSION_VARIABLES: [&str; 13] = [
    "CLAUDECODE",
    "CLAUDE_CODE_SESSION_ID",
    "CLAUDE_CODE_HOST_SESSION_ID",
    "CLAUDE_CODE_CHILD_SESSION",
    "CLAUDE_CODE_SESSION_ATTENDED",
    "CLAUDE_CODE_MESSAGING_SOCKET",
    "CLAUDE_CODE_MESSAGING_TOKEN",
    "CLAUDE_CODE_EXECPATH",
    "CLAUDE_CODE_DESKTOP_APP_VERSION",
    "CLAUDE_EFFORT",
    "CLAUDE_PID",
    "CLAUDE_AGENT_SDK_VERSION",
    "ELECTRON_RUN_AS_NODE",
];

const EFFORT_LEVELS: [&str; 5] = ["low", "medium", "high", "xhigh", "max"];

/// One settings source's `maxEffortLevel` caps (`ClaudeEffortCapSource`).
#[derive(Clone, Debug, Default)]
struct EffortCap {
    max: Option<String>,
    per_model: Vec<(String, String)>,
}

/// What the capability probe read (`ClaudeCapabilitiesProbe`).
#[derive(Clone, Debug, Default)]
pub(crate) struct Capabilities {
    account: Value,
    workspace_id: Option<String>,
    commands: Vec<Value>,
    models: Vec<Value>,
    usage: Option<Value>,
    usage_checked_at: Option<String>,
    effort_caps: Vec<EffortCap>,
}

enum ProbeError {
    TimedOut,
    Failed,
}

/// The driver's capability probe cache: one entry, kept five minutes when
/// the probe read usage, with the last good probe as a fallback.
#[derive(Default)]
pub(crate) struct CapabilitiesCache {
    key: Value,
    fresh: Option<(Capabilities, Instant)>,
    last_good: Option<Capabilities>,
}

/// The base environment of the Claude processes T3 Code runs
/// (`mergeClaudeProcessEnvironment`).
fn process_env(env: &ProcessEnv) -> ProcessEnv {
    let mut env = env.clone();
    for name in HOST_SESSION_VARIABLES {
        env.remove(name);
    }
    env.insert("CLAUDE_CODE_ENTRYPOINT".into(), "t3-code".into());
    env
}

/// `resolveClaudeHomePath` for a configured `homePath`.
fn configured_home(config: &Value) -> Option<PathBuf> {
    let home = config["homePath"].as_str().unwrap_or("").trim();
    (!home.is_empty()).then(|| absolute(&expand_home(home)))
}

fn absolute(path: &str) -> PathBuf {
    let path = PathBuf::from(path);
    if path.is_absolute() {
        path
    } else {
        std::env::current_dir().unwrap_or_default().join(path)
    }
}

/// The Claude config directory: `homePath`, then `CLAUDE_CONFIG_DIR`, then
/// `~/.claude`.
fn config_dir(config: &Value, env: &ProcessEnv) -> PathBuf {
    configured_home(config)
        .or_else(|| {
            env.get("CLAUDE_CONFIG_DIR")
                .map(|v| v.trim())
                .filter(|v| !v.is_empty())
                .map(absolute)
        })
        .unwrap_or_else(|| super::home_dir().join(".claude"))
}

/// `makeClaudeEnvironment`: a configured home is `CLAUDE_CONFIG_DIR`.
fn claude_env(config: &Value, env: &ProcessEnv) -> ProcessEnv {
    let mut env = env.clone();
    if let Some(home) = configured_home(config) {
        env.insert("CLAUDE_CONFIG_DIR".into(), home.display().to_string());
    }
    env
}

/// `ANTHROPIC_BASE_URL` from Claude's user and managed settings, else the environment.
fn base_url(config: &Value, env: &ProcessEnv, cwd: &Path) -> Option<String> {
    super::claude_skills::read_settings_env(config, Some(cwd), env)
        .anthropic_base_url
        .or_else(|| env.get("ANTHROPIC_BASE_URL").cloned())
}

/// `customClaudeApiEndpointHost` and the endpoint's origin.
fn custom_endpoint(base_url: Option<&str>) -> Option<Option<String>> {
    let url = url::Url::parse(base_url?.trim()).ok()?;
    let host = url.host_str()?;
    if !matches!(url.scheme(), "http" | "https") || host.is_empty() || host == "api.anthropic.com" {
        return None;
    }
    Some(Some(url.origin().ascii_serialization()))
}

fn read_organization_id(config: &Value, env: &ProcessEnv) -> Option<String> {
    let custom = configured_home(config).is_some()
        || env
            .get("CLAUDE_CONFIG_DIR")
            .is_some_and(|v| !v.trim().is_empty());
    let path = if custom {
        config_dir(config, env).join(".claude.json")
    } else {
        super::home_dir().join(".claude.json")
    };
    let text = std::fs::read_to_string(path).ok()?;
    let value: Value = serde_json::from_str(&text).ok()?;
    value["oauthAccount"]["organizationUuid"]
        .as_str()
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .map(str::to_owned)
}

// ---------------------------------------------------------------------------
// Catalog

/// A catalog model with its Claude Code version range.
struct CatalogModel {
    model: Value,
    min_version: Option<String>,
    max_version_exclusive: Option<String>,
    /// The profile's `effortMap`: what an effort option sends Claude Code.
    effort_map: Value,
}

fn catalog(manifest: &Value) -> Vec<CatalogModel> {
    let models = crate::model_manifest::claude_models(manifest, crate::platform::now_ms());
    let source = if manifest["providers"]["claudeAgent"]["models"].is_array() {
        manifest
    } else {
        &crate::model_manifest::BUNDLED
    };
    let entries = source["providers"]["claudeAgent"]["models"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    models
        .into_iter()
        .map(|model| {
            let entry = entries.iter().find(|e| e["slug"] == model["slug"]);
            let adapter = entry.map(|e| &e["adapter"]["claudeCode"]);
            let text = |key: &str| {
                adapter
                    .and_then(|a| a[key].as_str())
                    .map(str::trim)
                    .filter(|v| !v.is_empty())
                    .map(str::to_owned)
            };
            let profiles = &source["providers"]["claudeAgent"]["profiles"];
            let effort_map = entry
                .and_then(|e| e["profile"].as_str())
                .map(|name| profiles[name]["adapter"]["claudeCode"]["effortMap"].clone())
                .unwrap_or(Value::Null);
            CatalogModel {
                min_version: text("minVersion"),
                max_version_exclusive: text("maxVersionExclusive"),
                effort_map,
                model,
            }
        })
        .collect()
}

fn compare(a: &str, b: &str) -> std::cmp::Ordering {
    crate::model_manifest::compare_semver(a, b)
}

fn supports(model: &CatalogModel, version: Option<&str>) -> bool {
    if model.min_version.is_none() && model.max_version_exclusive.is_none() {
        return true;
    }
    let Some(version) = version else {
        return false;
    };
    if model
        .min_version
        .as_deref()
        .is_some_and(|min| compare(version, min).is_lt())
    {
        return false;
    }
    !model
        .max_version_exclusive
        .as_deref()
        .is_some_and(|max| compare(version, max).is_ge())
}

fn update_required(catalog: &[CatalogModel], version: Option<&str>) -> Vec<Value> {
    let Some(version) = version else {
        return vec![];
    };
    catalog
        .iter()
        .filter_map(|entry| {
            let min = entry.min_version.as_deref()?;
            if entry.model["isLegacy"] == true || compare(version, min).is_ge() {
                return None;
            }
            let mut row = json!({"slug": entry.model["slug"], "name": entry.model["name"]});
            if let Some(badge) = entry.model.get("badge") {
                row["badge"] = badge.clone();
            }
            row["minVersion"] = json!(min);
            Some(row)
        })
        .collect()
}

fn upgrade_message(catalog: &[CatalogModel], version: Option<&str>) -> Option<String> {
    let mut unavailable: Vec<&CatalogModel> = catalog
        .iter()
        .filter(|entry| {
            entry
                .min_version
                .as_deref()
                .is_some_and(|min| version.is_none_or(|v| compare(v, min).is_lt()))
        })
        .collect();
    unavailable.sort_by(|a, b| {
        compare(
            a.min_version.as_deref().unwrap(),
            b.min_version.as_deref().unwrap(),
        )
    });
    let entry = unavailable.first()?;
    let label = version
        .map(|v| format!("v{v}"))
        .unwrap_or_else(|| "the installed version".into());
    Some(format!(
        "Claude Code {label} is too old for {}. Upgrade to v{} or newer to access it.",
        entry.model["name"].as_str().unwrap_or(""),
        entry.min_version.as_deref().unwrap()
    ))
}

fn strip_context_suffix(id: &str) -> String {
    id.strip_suffix("[1m]").unwrap_or(id).to_lowercase()
}

fn effort_label(id: &str) -> String {
    match id {
        "low" => "Low",
        "medium" => "Medium",
        "high" => "High",
        "xhigh" => "Extra High",
        "max" => "Max",
        other => other,
    }
    .to_owned()
}

/// `claudeReportedModels`: models Claude Code lists that the catalog and
/// the custom models do not.
fn reported_models(catalog: &[CatalogModel], custom: &Value, reported: &[Value]) -> Vec<Value> {
    let mut known: Vec<String> = catalog
        .iter()
        .flat_map(|entry| {
            let mut ids = vec![entry.model["slug"].as_str().unwrap_or("").to_owned()];
            ids.extend(
                entry.model["aliases"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .filter_map(Value::as_str)
                    .map(str::to_owned),
            );
            ids
        })
        .chain(custom.as_array().into_iter().flatten().filter_map(|entry| {
            entry
                .as_str()
                .or_else(|| entry["slug"].as_str())
                .map(|s| s.trim().to_owned())
                .filter(|s| !s.is_empty())
        }))
        .map(|id| strip_context_suffix(&id))
        .collect();
    let mut models = vec![];
    for info in reported {
        let slug = info["value"].as_str().unwrap_or("").trim().to_owned();
        if slug.is_empty() || slug == "default" {
            continue;
        }
        let ids: Vec<String> = [slug.as_str(), info["resolvedModel"].as_str().unwrap_or("")]
            .iter()
            .map(|id| strip_context_suffix(id))
            .filter(|id| !id.is_empty())
            .collect();
        if ids.iter().any(|id| known.contains(id)) {
            continue;
        }
        known.extend(ids);
        let efforts: Vec<String> = if info["supportsEffort"] == true {
            info["supportedEffortLevels"]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(Value::as_str)
                .map(str::to_owned)
                .collect()
        } else {
            vec![]
        };
        let name = info["displayName"].as_str().unwrap_or("").trim();
        let descriptors = if efforts.is_empty() {
            json!([])
        } else {
            let mut options = vec![json!({"id": "default", "label": "Default", "isDefault": true})];
            options.extend(
                efforts
                    .iter()
                    .map(|id| json!({"id": id, "label": effort_label(id)})),
            );
            json!([{"id": "effort", "label": "Reasoning", "type": "select", "options": options}])
        };
        models.push(json!({
            "slug": slug,
            "name": if name.is_empty() { slug.as_str() } else { name },
            "isCustom": false,
            "capabilities": {"optionDescriptors": descriptors},
        }));
    }
    models
}

/// `claudeProviderModels`: custom models get empty capabilities and the
/// Ultracode toggle.
fn provider_models(built_in: Vec<Value>, custom: &Value) -> Vec<Value> {
    models_from_settings(built_in, custom, json!({"optionDescriptors": []}))
        .into_iter()
        .map(|mut model| {
            if model["isCustom"] == true && !model["capabilities"].is_null() {
                model["capabilities"] =
                    crate::model_manifest::with_ultracode_toggle(&model["capabilities"]);
            }
            model
        })
        .collect()
}

fn effort_rank(level: &str) -> i32 {
    EFFORT_LEVELS
        .iter()
        .position(|l| *l == level)
        .map(|p| p as i32)
        .unwrap_or(-1)
}

/// `resolveClaudeModelSettingsKey`: the catalog model a `modelSettings` key
/// names.
fn settings_key_model(catalog: &[CatalogModel], key: &str) -> Option<String> {
    let lowered = key.trim().to_lowercase();
    let unsuffixed = lowered.strip_suffix("[1m]").unwrap_or(&lowered);
    if ["opus", "sonnet", "haiku", "fable"].contains(&unsuffixed) {
        return catalog
            .iter()
            .find(|e| {
                e.model["isCustom"] != true
                    && e.model["aliases"]
                        .as_array()
                        .is_some_and(|a| a.iter().any(|x| x == unsuffixed))
            })
            .and_then(|e| e.model["slug"].as_str().map(str::to_owned));
    }
    let mut found: Option<String> = None;
    for entry in catalog {
        let slug = entry.model["slug"].as_str().unwrap_or("");
        if entry.model["isCustom"] != true
            && lowered.contains(slug)
            && slug.len() > found.as_ref().map_or(0, |f| f.len())
        {
            found = Some(slug.to_owned());
        }
    }
    found
}

/// The rank of what an effort option sends (`normalizeClaudeCatalogEffort`):
/// the model's profile may map it to another effort, or to none.
fn sent_rank(catalog: &[CatalogModel], option: &str, slug: &str) -> i32 {
    let entry = catalog
        .iter()
        .find(|e| e.model["slug"] == slug)
        .or_else(|| {
            catalog.iter().find(|e| {
                e.model["aliases"].as_array().is_some_and(|aliases| {
                    aliases
                        .iter()
                        .filter_map(Value::as_str)
                        .any(|a| a.eq_ignore_ascii_case(slug))
                })
            })
        });
    match entry.and_then(|e| e.effort_map.get(option)) {
        Some(mapped) => mapped.as_str().map(effort_rank).unwrap_or(-1),
        None => effort_rank(option),
    }
}

/// `capClaudeCatalogEffort`: efforts above a `maxEffortLevel` are relabelled.
fn cap_effort(catalog: &[CatalogModel], models: Vec<Value>, sources: &[EffortCap]) -> Vec<Value> {
    if sources.is_empty() {
        return models;
    }
    let resolved: Vec<EffortCap> = sources
        .iter()
        .map(|source| {
            let mut per_model: Vec<(String, String)> = vec![];
            for (key, level) in &source.per_model {
                let Some(slug) = settings_key_model(catalog, key) else {
                    continue;
                };
                match per_model.iter_mut().find(|(s, _)| *s == slug) {
                    Some(entry) if effort_rank(level) < effort_rank(&entry.1) => {
                        entry.1 = level.clone()
                    }
                    Some(_) => {}
                    None => per_model.push((slug, level.clone())),
                }
            }
            EffortCap {
                max: source.max.clone(),
                per_model,
            }
        })
        .collect();
    models
        .into_iter()
        .map(|mut model| {
            if model["isCustom"] == true || !model["capabilities"]["optionDescriptors"].is_array() {
                return model;
            }
            let slug = model["slug"].as_str().unwrap_or("").to_owned();
            let mut cap: Option<String> = None;
            for EffortCap { max, per_model } in &resolved {
                let level = per_model
                    .iter()
                    .find(|(s, _)| *s == slug)
                    .map(|(_, l)| l.clone())
                    .or_else(|| max.clone());
                if let Some(level) = level {
                    if level != "max"
                        && cap
                            .as_ref()
                            .is_none_or(|c| effort_rank(&level) < effort_rank(c))
                    {
                        cap = Some(level);
                    }
                }
            }
            let Some(cap) = cap else {
                return model;
            };
            let cap_rank = effort_rank(&cap);
            for descriptor in model["capabilities"]["optionDescriptors"]
                .as_array_mut()
                .into_iter()
                .flatten()
            {
                if descriptor["id"] != "effort" || descriptor["type"] != "select" {
                    continue;
                }
                let best = descriptor["options"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .filter_map(|o| {
                        let rank = effort_rank(o["id"].as_str().unwrap_or(""));
                        (rank >= 0 && rank <= cap_rank).then(|| (rank, o["label"].clone()))
                    })
                    .max_by_key(|(rank, _)| *rank);
                let Some((best_rank, best_label)) = best else {
                    continue;
                };
                for option in descriptor["options"].as_array_mut().into_iter().flatten() {
                    let id = option["id"].as_str().unwrap_or("").to_owned();
                    let id_rank = effort_rank(&id);
                    let sent = sent_rank(catalog, &id, &slug);
                    let displayed = if id_rank >= 0 { id_rank } else { sent };
                    let runs = if sent >= 0 { sent } else { id_rank };
                    if displayed > best_rank && runs > best_rank {
                        option["label"] = json!(format!(
                            "{} (capped to {})",
                            option["label"].as_str().unwrap_or(""),
                            best_label.as_str().unwrap_or("")
                        ));
                    }
                }
            }
            model
        })
        .collect()
}

fn parse_effort_source(settings: &Value) -> Option<EffortCap> {
    let level = |v: &Value| {
        v.as_str()
            .filter(|l| EFFORT_LEVELS.contains(l))
            .map(str::to_owned)
    };
    let max = level(&settings["maxEffortLevel"]);
    let per_model: Vec<(String, String)> = settings["modelSettings"]
        .as_object()
        .into_iter()
        .flatten()
        .filter_map(|(model, s)| Some((model.clone(), level(&s["maxEffortLevel"])?)))
        .collect();
    (max.is_some() || !per_model.is_empty()).then_some(EffortCap { max, per_model })
}

/// `parseClaudeEffortCapSources`.
fn parse_effort_sources(settings: &Value) -> Vec<EffortCap> {
    match settings["sources"].as_array() {
        Some(sources) => sources
            .iter()
            .filter_map(|s| parse_effort_source(&s["settings"]))
            .collect(),
        None => parse_effort_source(&settings["effective"])
            .into_iter()
            .collect(),
    }
}

/// `dedupeSlashCommands`.
fn dedupe_commands(commands: Vec<Value>) -> Vec<Value> {
    let mut out: Vec<(String, Value)> = vec![];
    for command in commands {
        let name = command["name"].as_str().unwrap_or("").trim().to_owned();
        if name.is_empty() {
            continue;
        }
        let key = name.to_lowercase();
        match out.iter_mut().find(|(k, _)| *k == key) {
            None => {
                let mut command = command.clone();
                command["name"] = json!(name);
                out.push((key, command));
            }
            Some((_, existing)) => {
                if existing.get("description").is_none() {
                    if let Some(d) = command.get("description") {
                        existing["description"] = d.clone();
                    }
                }
                if existing["input"]["hint"].is_null() && !command["input"]["hint"].is_null() {
                    existing["input"] = json!({"hint": command["input"]["hint"]});
                }
            }
        }
    }
    out.into_iter().map(|(_, c)| c).collect()
}

/// `parseClaudeInitializationCommands`.
fn init_commands(commands: &Value) -> Vec<Value> {
    let trimmed = |v: &Value| {
        v.as_str()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_owned)
    };
    dedupe_commands(
        commands
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|command| {
                let name = trimmed(&command["name"])?;
                let mut row = json!({"name": name});
                if let Some(description) = trimmed(&command["description"]) {
                    row["description"] = json!(description);
                }
                if let Some(hint) = trimmed(&command["argumentHint"]) {
                    row["input"] = json!({"hint": hint});
                }
                Some(row)
            })
            .collect(),
    )
}

// ---------------------------------------------------------------------------
// Auth and usage

fn title_case(value: &str) -> String {
    value
        .split(|c: char| c.is_whitespace() || c == '_' || c == '-')
        .filter(|p| !p.is_empty())
        .map(|p| {
            let mut chars = p.chars();
            let first = chars.next().unwrap().to_uppercase().collect::<String>();
            first + &chars.as_str().to_lowercase()
        })
        .collect::<Vec<_>>()
        .join(" ")
}

fn squash(value: &str) -> String {
    value
        .to_lowercase()
        .chars()
        .filter(|c| !c.is_whitespace() && *c != '_' && *c != '-')
        .collect()
}

fn subscription_label(subscription: &str) -> String {
    match squash(subscription).as_str() {
        "claudemaxsubscription" | "claudemax5xsubscription" | "max" | "maxplan" | "max5" => {
            "Max".into()
        }
        "claudemax20xsubscription" | "max20" => "Ultra".into(),
        "claudeenterprisesubscription" | "enterprise" => "Enterprise".into(),
        "claudeteamsubscription" | "team" => "Team".into(),
        "claudeprosubscription" | "pro" => "Pro".into(),
        "claudefreesubscription" | "free" => "Free".into(),
        "" => title_case(subscription),
        _ => title_case(subscription),
    }
}

fn subscription_auth_label(subscription: &str) -> String {
    let label = subscription_label(subscription);
    let normalized = squash(&label);
    if normalized.starts_with("claude") && normalized.ends_with("subscription") {
        label
    } else if normalized.starts_with("claude") {
        format!("{label} Subscription")
    } else if normalized.ends_with("subscription") {
        format!("Claude {label}")
    } else {
        format!("Claude {label} Subscription")
    }
}

fn has_api_key(source: &Value) -> bool {
    source
        .as_str()
        .is_some_and(|s| !s.is_empty() && s != "none")
}

fn auth_status(account: &Value) -> &'static str {
    if account["apiProvider"]
        .as_str()
        .is_some_and(|p| p != "firstParty")
    {
        return "authenticated";
    }
    if account["tokenSource"] != "none" {
        return "authenticated";
    }
    if has_api_key(&account["apiKeySource"])
        || account["email"].as_str().is_some_and(|e| !e.is_empty())
        || account["subscriptionType"]
            .as_str()
            .is_some_and(|s| !s.is_empty())
    {
        "authenticated"
    } else {
        "unauthenticated"
    }
}

fn auth_metadata(account: &Value) -> Option<(String, String)> {
    let method = squash(account["tokenSource"].as_str().unwrap_or(""));
    if matches!(
        method.as_str(),
        "apikey" | "anthropicapikey" | "anthropicauthtoken"
    ) || has_api_key(&account["apiKeySource"])
    {
        return Some(("apiKey".into(), "Claude API Key".into()));
    }
    let subscription = account["subscriptionType"]
        .as_str()
        .filter(|s| !s.is_empty())?;
    Some((
        subscription.to_owned(),
        subscription_auth_label(subscription),
    ))
}

fn iso_from(value: &Value) -> Option<String> {
    let text = value.as_str().filter(|s| !s.is_empty())?;
    let parsed =
        time::OffsetDateTime::parse(text, &time::format_description::well_known::Rfc3339).ok()?;
    Some(crate::auth_service::iso(
        (parsed.unix_timestamp_nanos() / 1_000_000) as i64,
    ))
}

fn percent(used: &Value, limit: &Value) -> Option<f64> {
    let (used, limit) = (used.as_f64()?, limit.as_f64()?);
    if !used.is_finite() || !limit.is_finite() || used < 0.0 || limit <= 0.0 {
        return None;
    }
    let p = used / limit * 100.0;
    p.is_finite().then(|| clamp_percent(p))
}

fn spend_percent(rate_limits: &Value) -> Option<f64> {
    let spend = &rate_limits["spend"];
    let money = |m: &Value| {
        m["amount_minor"].as_u64().is_some()
            && m["currency"].as_str().is_some_and(|c| !c.is_empty())
            && m["exponent"].as_u64().is_some()
    };
    if spend["enabled"] == true
        && money(&spend["used"])
        && money(&spend["limit"])
        && spend["used"]["currency"].as_str().map(str::trim)
            == spend["limit"]["currency"].as_str().map(str::trim)
        && spend["used"]["exponent"] == spend["limit"]["exponent"]
    {
        if let Some(p) = percent(
            &spend["used"]["amount_minor"],
            &spend["limit"]["amount_minor"],
        ) {
            return Some(p);
        }
    }
    let extra = &rate_limits["extra_usage"];
    if extra["is_enabled"] != true {
        return None;
    }
    let p = percent(&extra["used_credits"], &extra["monthly_limit"])
        .or(extra["utilization"].as_f64())?;
    (p.is_finite() && p >= 0.0).then(|| clamp_percent(p))
}

/// `claudeUsageResponseToLimits`.
fn usage_to_limits(response: &Value, checked_at: &str) -> Value {
    let rate_limits = &response["rate_limits"];
    if response["rate_limits_available"] != true || !rate_limits.is_object() {
        return unavailable_usage(
            checked_at,
            if response["rate_limits_available"] == true {
                "probeFailed"
            } else {
                "unsupported"
            },
            None,
        );
    }
    let mut windows = vec![];
    for (id, kind, label, mins) in [
        ("five_hour", "session", "Session", 300),
        ("seven_day", "weekly", "Weekly", 10080),
    ] {
        let window = &rate_limits[id];
        let Some(used) = window["utilization"].as_f64() else {
            continue;
        };
        let mut row = json!({"id": id, "kind": kind, "label": label, "windowDurationMins": mins,
            "usedPercent": js_number(clamp_percent(used))});
        if let Some(at) = iso_from(&window["resets_at"]) {
            row["resetsAt"] = json!(at);
        }
        windows.push(row);
    }
    for entry in rate_limits["model_scoped"].as_array().into_iter().flatten() {
        let (Some(name), Some(used)) = (
            entry["display_name"].as_str(),
            entry["utilization"].as_f64(),
        ) else {
            continue;
        };
        let token: String = {
            let lowered = name.to_lowercase();
            let mut out = String::new();
            let mut in_run = false;
            for c in lowered.chars() {
                if c.is_ascii_lowercase() || c.is_ascii_digit() {
                    out.push(c);
                    in_run = false;
                } else if !in_run {
                    out.push('_');
                    in_run = true;
                }
            }
            out
        };
        let mut row = json!({"id": format!("seven_day_{token}"), "kind": "weekly",
            "label": format!("Weekly · {name}"), "windowDurationMins": 10080,
            "usedPercent": js_number(clamp_percent(used))});
        if let Some(at) = iso_from(&entry["resets_at"]) {
            row["resetsAt"] = json!(at);
        }
        windows.push(row);
    }
    if let Some(p) = spend_percent(rate_limits) {
        windows.push(
            json!({"id": "monthly_spend", "kind": "monthly", "label": "Monthly spend",
            "usedPercent": js_number(p)}),
        );
    }
    let mut limits = usage_limits(checked_at, windows);
    let extra = &rate_limits["extra_usage"];
    if extra["is_enabled"] == true && extra["monthly_limit"].as_f64() == Some(0.0) {
        limits["zeroSpendCap"] = json!(true);
    }
    limits
}

fn signed_out_message(config_dir: Option<&Path>, cwd: &Path) -> String {
    let configuration = match config_dir {
        Some(dir) => format!(
            " from {}, with CLAUDE_CONFIG_DIR set to {}",
            json!(cwd.display().to_string()),
            json!(dir.display().to_string())
        ),
        None => String::new(),
    };
    format!(
        "Claude could not authenticate. For subscription login, run `claude auth login` on this environment's machine{configuration}, then start a new thread. For API-key authentication, check this instance's configured credentials."
    )
}

// ---------------------------------------------------------------------------
// The capability probe

fn request_id() -> String {
    uuid::Uuid::new_v4().simple().to_string()[..11].to_owned()
}

/// The Agent SDK's capability probe: a Claude Code session in stream-json
/// mode that initializes, reads usage and settings, and is stopped.
async fn probe_capabilities(
    binary: &str,
    env: &ProcessEnv,
    cwd: &Path,
    timeout: Duration,
) -> Result<Capabilities, ProbeError> {
    let mut env = probe_env(env);
    env.insert("ENABLE_CLAUDEAI_MCP_SERVERS".into(), "false".into());
    env.remove("FORCE_CODE_TERMINAL");
    env.insert("CLAUDE_CODE_AUTO_CONNECT_IDE".into(), "0".into());
    env.insert("CLAUDE_CODE_IDE_SKIP_AUTO_INSTALL".into(), "1".into());
    env.insert("CLAUDE_CODE_SDK_READS_SESSION_STATE".into(), "1".into());
    env.insert("CLAUDE_AGENT_SDK_VERSION".into(), SDK_VERSION.into());
    let mut args: Vec<String> = [
        "--output-format",
        "stream-json",
        "--verbose",
        "--input-format",
        "stream-json",
        "--setting-sources=user,project,local",
    ]
    .iter()
    .map(|s| s.to_string())
    .collect();
    if !Path::new(MANAGED_MCP_CONFIG).exists() {
        args.push("--strict-mcp-config".into());
    }
    args.extend([
        "--no-session-persistence".into(),
        "--settings".into(),
        r#"{"disableAllHooks":true}"#.into(),
    ]);
    let mut command = super::command(binary, &args, &env, cwd);
    command
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null());
    let mut child = command.spawn().map_err(|_| ProbeError::Failed)?;
    let mut stdin = child.stdin.take().ok_or(ProbeError::Failed)?;
    let mut lines = BufReader::new(child.stdout.take().ok_or(ProbeError::Failed)?).lines();
    let request = |request: Value| {
        let id = request_id();
        let line = json!({"request_id": id, "type": "control_request", "request": request})
            .to_string()
            + "\n";
        (id, line)
    };
    let (init_id, line) = request(json!({"subtype": "initialize", "systemPrompt": [""]}));
    let result = async {
        stdin
            .write_all(line.as_bytes())
            .await
            .map_err(|_| ProbeError::Failed)?;
        let init = tokio::time::timeout(timeout, read_response(&mut lines, std::slice::from_ref(&init_id)))
            .await
            .map_err(|_| ProbeError::TimedOut)?
            .remove(&init_id)
            .ok_or(ProbeError::Failed)?
            .map_err(|_| ProbeError::Failed)?;
        let usage_checked_at = now_iso();
        let (usage_id, usage_line) = request(json!({"subtype": "get_usage", "skip_behaviors": true}));
        let (settings_id, settings_line) = request(json!({"subtype": "get_settings"}));
        let _ = stdin.write_all(usage_line.as_bytes()).await;
        let _ = stdin.write_all(settings_line.as_bytes()).await;
        let mut reads = tokio::time::timeout(
            READ_TIMEOUT,
            read_response(&mut lines, &[usage_id.clone(), settings_id.clone()]),
        )
        .await
        .unwrap_or_default();
        let usage = reads.remove(&usage_id).and_then(Result::ok).map(|u| {
            json!({"rate_limits_available": u["rate_limits_available"], "rate_limits": u["rate_limits"]})
        });
        let settings = reads.remove(&settings_id).and_then(Result::ok);
        Ok(Capabilities {
            account: init["account"].clone(),
            workspace_id: None,
            commands: init_commands(&init["commands"]),
            models: init["models"].as_array().cloned().unwrap_or_default(),
            usage_checked_at: usage.is_some().then_some(usage_checked_at),
            usage,
            effort_caps: settings.as_ref().map(parse_effort_sources).unwrap_or_default(),
        })
    }
    .await;
    drop(stdin);
    let _ = child.start_kill();
    let _ = child.wait().await;
    result
}

/// Read Claude Code's output until each of `ids` has its control response,
/// or the output ends: each response's payload, or `Err` for an error reply.
async fn read_response(
    lines: &mut tokio::io::Lines<BufReader<tokio::process::ChildStdout>>,
    ids: &[String],
) -> std::collections::HashMap<String, Result<Value, ()>> {
    let mut found = std::collections::HashMap::new();
    while found.len() < ids.len() {
        let Ok(Some(line)) = lines.next_line().await else {
            break;
        };
        let Ok(message) = serde_json::from_str::<Value>(&line) else {
            continue;
        };
        if message["type"] != "control_response" {
            continue;
        }
        let response = &message["response"];
        let Some(id) = response["request_id"].as_str() else {
            continue;
        };
        if !ids.iter().any(|i| i == id) {
            continue;
        }
        let payload = if response["subtype"] == "success" {
            Ok(response["response"].clone())
        } else {
            Err(())
        };
        found.insert(id.to_owned(), payload);
    }
    found
}

/// The cached capability probe (`capabilitiesProbeCache`).
#[allow(clippy::too_many_arguments)]
async fn capabilities(
    service: &ProviderStatus,
    instance_id: &str,
    key: Value,
    config: &Value,
    binary: &str,
    env: &ProcessEnv,
    cwd: &Path,
    timeout: Duration,
) -> Result<Capabilities, ProbeError> {
    {
        let caches = service
            .claude_caps
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if let Some(cache) = caches.get(instance_id).filter(|c| c.key == key) {
            if let Some((caps, at)) = &cache.fresh {
                if at.elapsed() < CAPABILITIES_TTL {
                    return Ok(caps.clone());
                }
            }
        }
    }
    let result = probe_capabilities(binary, env, cwd, timeout).await;
    let mut caches = service
        .claude_caps
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    let cache = caches.entry(instance_id.to_owned()).or_default();
    if cache.key != key {
        *cache = CapabilitiesCache {
            key,
            ..Default::default()
        };
    }
    match result {
        Ok(mut caps) => {
            caps.workspace_id = read_organization_id(config, env);
            cache.fresh = caps.usage.is_some().then(|| (caps.clone(), Instant::now()));
            cache.last_good = Some(caps.clone());
            Ok(caps)
        }
        Err(error) => {
            cache.fresh = None;
            match cache.last_good.take() {
                Some(mut last) if read_organization_id(config, env) == last.workspace_id => {
                    last.usage = None;
                    last.usage_checked_at = None;
                    Ok(last)
                }
                _ => Err(error),
            }
        }
    }
}

/// The config directory holding the claude.ai login and the `.claude.json`
/// naming its account, as the driver resolves them for reset credits.
pub(super) fn credit_paths(config: &Value, env: &ProcessEnv) -> (PathBuf, PathBuf) {
    let resolved = process_env(env);
    let config_dir = config_dir(config, &resolved);
    let account = super::claude_skills::account_config_path(config, &resolved, &config_dir);
    (config_dir, account)
}

/// Drop the cached capability probe, so the next check probes again.
pub(super) fn invalidate_capabilities(service: &ProviderStatus, instance_id: &str) {
    invalidate(service, instance_id);
}

fn invalidate(service: &ProviderStatus, instance_id: &str) {
    service
        .claude_caps
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .remove(instance_id);
}

// ---------------------------------------------------------------------------
// The check

/// `checkClaudeProviderStatus` for an enabled instance.
pub(super) async fn check(
    service: &ProviderStatus,
    instance_id: &str,
    config: &Value,
    env: &ProcessEnv,
    state_dir: &Path,
    manifest: &Value,
) -> Draft {
    let checked_at = now_iso();
    let catalog = catalog(manifest);
    let custom = &config["customModels"];
    let all_models = provider_models(catalog.iter().map(|e| e.model.clone()).collect(), custom);
    let binary = expand_home(
        config["binaryPath"]
            .as_str()
            .map(str::trim)
            .filter(|b| !b.is_empty())
            .unwrap_or("claude"),
    );
    let resolved = process_env(env);
    let claude = claude_env(config, &resolved);
    let draft =
        |installed: bool, version: Option<String>, status: &'static str, message: &str| Draft {
            installed,
            version,
            status,
            auth: json!({"status": "unknown"}),
            checked_at: checked_at.clone(),
            message: Some(message.to_owned()),
            models: all_models.clone(),
            ..Default::default()
        };
    let version_timeout = probe_timeout("claude", VERSION_TIMEOUT_MS, &resolved);
    let result = run_collect(
        &binary,
        &["--version".into()],
        &probe_env(&claude),
        state_dir,
        version_timeout,
    )
    .await;
    let output =
        match result {
            Err(RunError::NotFound) => {
                return draft(
                    false,
                    None,
                    "error",
                    "Claude Agent CLI (`claude`) was not found on PATH.",
                )
            }
            Err(RunError::Spawn) => {
                return draft(
                    true,
                    None,
                    "error",
                    "Failed to execute Claude Agent CLI health check.",
                )
            }
            Err(RunError::TimedOut) => return draft(
                true,
                None,
                "error",
                "Claude Agent CLI is installed but failed to run. Timed out while running command.",
            ),
            Ok(output) => output,
        };
    if output.code != 0 {
        return draft(
            true,
            None,
            "error",
            "Claude Agent CLI is installed but failed to run.",
        );
    }
    let version = parse_generic_version(&format!("{}\n{}", output.stdout, output.stderr));
    let update_required_models = update_required(&catalog, version.as_deref());
    let upgrade = upgrade_message(&catalog, version.as_deref());
    let caps_timeout = probe_timeout("claude", CAPABILITIES_TIMEOUT_MS, &resolved);
    let key = json!({"config": config, "binary": binary});
    let probed = capabilities(
        service,
        instance_id,
        key,
        config,
        &binary,
        &claude,
        state_dir,
        caps_timeout,
    )
    .await;
    let caps = probed.as_ref().ok();
    let supported: Vec<Value> = catalog
        .iter()
        .filter(|e| supports(e, version.as_deref()))
        .map(|e| e.model.clone())
        .collect();
    let mut built_in = supported;
    built_in.extend(reported_models(
        &catalog,
        custom,
        caps.map(|c| c.models.as_slice()).unwrap_or(&[]),
    ));
    let models = cap_effort(
        &catalog,
        provider_models(built_in, custom),
        caps.map(|c| c.effort_caps.as_slice()).unwrap_or(&[]),
    );
    let skills = super::claude_skills::discover(config, state_dir, &resolved);
    let mut commands = vec![compact_slash_command()];
    commands.extend(caps.map(|c| c.commands.clone()).unwrap_or_default());
    let slash_commands = dedupe_commands(commands);
    let base = Draft {
        installed: true,
        version: version.clone(),
        checked_at: checked_at.clone(),
        models,
        update_required_models,
        slash_commands,
        skills,
        ..Default::default()
    };
    let Some(caps) = caps else {
        let message = match probed {
            Err(ProbeError::TimedOut) => format!(
                "Timed out after {}s while checking Claude account status.",
                caps_timeout.as_millis() as f64 / 1000.0
            ),
            _ => "Claude Agent CLI failed while checking account status.".to_owned(),
        };
        return Draft {
            status: "warning",
            auth: json!({"status": "unknown"}),
            message: Some(message),
            ..base
        };
    };
    let base_url = base_url(config, &resolved, state_dir);
    let endpoint = custom_endpoint(base_url.as_deref());
    if endpoint.is_none() && auth_status(&caps.account) == "unauthenticated" {
        invalidate(service, instance_id);
        let config_dir = claude.get("CLAUDE_CONFIG_DIR").map(PathBuf::from);
        let message = match config_dir {
            None => "Claude Code is not authenticated. Run `claude auth login` and try again."
                .to_owned(),
            Some(dir) => {
                signed_out_message(Some(&dir), &absolute(&state_dir.display().to_string()))
            }
        };
        return Draft {
            status: "error",
            auth: json!({"status": "unauthenticated"}),
            message: Some(message),
            ..base
        };
    }
    let mut auth = json!({"status": "authenticated"});
    if let Some(email) = caps.account["email"].as_str().filter(|e| !e.is_empty()) {
        auth["email"] = json!(email);
    }
    if let Some(workspace) = &caps.workspace_id {
        auth["workspaceId"] = json!(workspace);
    }
    if caps.account["apiProvider"] == "bedrock" {
        auth["type"] = json!("bedrock");
        auth["label"] = json!("Amazon Bedrock");
    } else if let Some(origin) = endpoint {
        auth["type"] = json!("customEndpoint");
        auth["label"] = json!("Custom API endpoint");
        if let Some(origin) = origin {
            auth["endpoint"] = json!(origin);
        }
    } else if let Some((kind, label)) = auth_metadata(&caps.account) {
        auth["type"] = json!(kind);
        auth["label"] = json!(label);
    }
    let usage = match &caps.usage {
        None => unavailable_usage(&checked_at, "probeFailed", None),
        Some(response) => usage_to_limits(
            response,
            caps.usage_checked_at.as_deref().unwrap_or(&checked_at),
        ),
    };
    // A subscription's reset credits, when its usage could be read.
    let mut usage = usage;
    let subscribed = caps.account["subscriptionType"]
        .as_str()
        .is_some_and(|s| !s.is_empty());
    if let (true, None, Some(version)) = (subscribed, usage.get("unavailable"), &version) {
        if let Some(credits) =
            super::claude_credits::read(&config_dir(config, &resolved), version).await
        {
            usage["resetCredits"] = credits;
        }
    }
    Draft {
        status: "ready",
        auth,
        message: upgrade,
        usage_limits: Some(usage),
        ..base
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn auth_labels_follow_the_reference() {
        assert_eq!(subscription_auth_label("max"), "Claude Max Subscription");
        assert_eq!(
            subscription_auth_label("claude_max_20x_subscription"),
            "Claude Ultra Subscription"
        );
        assert_eq!(
            subscription_auth_label("custom_plan"),
            "Claude Custom Plan Subscription"
        );
        assert_eq!(
            auth_metadata(&json!({"apiKeySource": "ANTHROPIC_API_KEY"})),
            Some(("apiKey".into(), "Claude API Key".into()))
        );
        assert_eq!(
            auth_status(&json!({"tokenSource": "none"})),
            "unauthenticated"
        );
        assert_eq!(auth_status(&json!({})), "authenticated");
    }

    #[test]
    fn usage_windows_from_get_usage() {
        let limits = usage_to_limits(
            &json!({"rate_limits_available": true, "rate_limits": {
                "seven_day": {"utilization": 40, "resets_at": "2026-11-28T00:00:00Z"},
                "five_hour": {"utilization": 12.5}}}),
            "2026-11-21T00:00:00.000Z",
        );
        assert_eq!(
            limits,
            json!({"checkedAt": "2026-11-21T00:00:00.000Z", "windows": [
            {"id": "five_hour", "kind": "session", "label": "Session", "windowDurationMins": 300, "usedPercent": 12.5},
            {"id": "seven_day", "kind": "weekly", "label": "Weekly", "windowDurationMins": 10080, "usedPercent": 40,
             "resetsAt": "2026-11-28T00:00:00.000Z"}]})
        );
        assert_eq!(
            usage_to_limits(&json!({}), "t")["unavailable"]["reason"],
            "unsupported"
        );
    }

    #[test]
    fn version_gating_uses_the_catalog() {
        let catalog = catalog(&crate::model_manifest::BUNDLED);
        let message = upgrade_message(&catalog, Some("2.1.200")).unwrap();
        assert_eq!(
            message,
            "Claude Code v2.1.200 is too old for Claude Opus 5. Upgrade to v2.1.219 or newer to access it."
        );
        let required: Vec<_> = update_required(&catalog, Some("2.1.200"))
            .into_iter()
            .map(|m| m["slug"].as_str().unwrap().to_owned())
            .collect();
        assert_eq!(
            required,
            [
                "claude-opus-5-5",
                "claude-sonnet-5-5",
                "claude-fable-5-2",
                "claude-haiku-5-5"
            ]
        );
    }
}
