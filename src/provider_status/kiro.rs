//! Kiro's status check, as the reference's `checkKiroProviderStatus`:
//! `kiro-cli --version`, `kiro-cli whoami --format json`, and once signed in
//! `kiro-cli chat --list-models --format json` alongside an ACP agent that
//! only initializes and reads `_kiro/config/template` (the agent picker).
use super::{
    acp::AcpSession, cli_binary, cli_version, compact_slash_command, models_from_settings, now_iso,
    probe_env, probe_timeout, run_cli, select_descriptor, CommandResult, Draft, ProcessEnv,
    RunError, AUTH_PROBE_TIMEOUT_MS,
};
use serde_json::{json, Map, Value};

const VERSION_TIMEOUT_MS: u64 = 10_000;
const DEFAULT_MODEL_SLUG: &str = "default";
const FALLBACK_DEFAULT_MODEL: &str = "auto";
const AGENT_CONFIG_ID: &str = "mode";
const ACP_ARGS: [&str; 3] = ["acp", "--agent-engine=v3", "--auth-method=cli"];

fn empty_capabilities() -> Value {
    json!({"optionDescriptors": []})
}

fn built_in_models() -> Vec<Value> {
    vec![
        json!({"slug": DEFAULT_MODEL_SLUG, "name": "Kiro default", "isCustom": false,
                "capabilities": empty_capabilities()}),
    ]
}

/// `kiroModels`: every model, custom ones included, carries the agent picker.
pub(crate) fn models(
    config: &Value,
    listed: Option<Vec<Value>>,
    agents: Option<&Value>,
) -> Vec<Value> {
    let models = models_from_settings(
        listed.unwrap_or_else(built_in_models),
        &config["customModels"],
        empty_capabilities(),
    );
    let Some(agents) = agents else {
        return models;
    };
    models
        .into_iter()
        .map(|mut model| {
            let mut descriptors = model["capabilities"]["optionDescriptors"]
                .as_array()
                .cloned()
                .unwrap_or_default();
            if descriptors.iter().any(|d| d["id"] == agents["id"]) {
                return model;
            }
            descriptors.push(agents.clone());
            model["capabilities"] = json!({"optionDescriptors": descriptors});
            model
        })
        .collect()
}

/// `kiroDefaultModelId`: Kiro's id for "Kiro default" in a status row, the
/// `default` model's first alias, else `auto`.
pub(crate) fn default_model_id(row: Option<&Value>) -> String {
    row.and_then(|row| row["models"].as_array())
        .into_iter()
        .flatten()
        .find(|model| model["slug"] == DEFAULT_MODEL_SLUG)
        .and_then(|model| model["aliases"][0].as_str())
        .map(str::trim)
        .filter(|alias| !alias.is_empty())
        .unwrap_or(FALLBACK_DEFAULT_MODEL)
        .to_owned()
}

/// `kiroAuthFromWhoami`.
fn auth_from_whoami(output: Option<&CommandResult>, env: &ProcessEnv) -> Value {
    let Some(output) = output else {
        return json!({"status": "unknown"});
    };
    if output.code != 0 {
        return if output.stdout.contains("\"account\":null") {
            json!({"status": "unauthenticated"})
        } else {
            json!({"status": "unknown"})
        };
    }
    let first_line = output
        .stdout
        .trim_start()
        .split('\n')
        .next()
        .unwrap_or("")
        .trim();
    // `KiroWhoami`: `accountType` and `email` are optional strings or null.
    let account = serde_json::from_str::<Value>(first_line).ok().filter(|v| {
        v.is_object()
            && ["accountType", "email"]
                .iter()
                .all(|key| matches!(v.get(*key), None | Some(Value::Null | Value::String(_))))
    });
    let api_key = env
        .get("KIRO_API_KEY")
        .is_some_and(|key| !key.trim().is_empty());
    if account
        .as_ref()
        .is_some_and(|a| a["accountType"] == "ApiKey")
        || api_key
    {
        return json!({"status": "authenticated", "type": "api_key", "label": "Kiro API key"});
    }
    let mut auth = json!({"status": "authenticated", "label": "Kiro account"});
    if let Some(email) = account
        .as_ref()
        .and_then(|a| a["email"].as_str())
        .filter(|e| !e.is_empty())
    {
        auth["email"] = json!(email);
    }
    auth
}

/// Reasoning levels per Kiro model id and its default level (`KIRO_MODEL_EFFORT`).
fn model_effort(model_id: &str) -> Option<(&'static [&'static str], &'static str)> {
    const CLAUDE: &[&str] = &["low", "medium", "high", "xhigh", "max"];
    const CLAUDE_4_6: &[&str] = &["low", "medium", "high", "max"];
    const GPT_5_6: &[&str] = &["none", "low", "medium", "high", "xhigh", "max"];
    const KIMI_K3: &[&str] = &["low", "high", "max"];
    Some(match model_id {
        "claude-fable-5.1" => (CLAUDE, "high"),
        "claude-opus-5.5" => (CLAUDE, "medium"),
        "claude-opus-5" => (CLAUDE, "high"),
        "claude-opus-4.8" => (CLAUDE, "high"),
        "claude-opus-4.7" => (CLAUDE, "xhigh"),
        "claude-opus-4.6" => (CLAUDE_4_6, "high"),
        "claude-sonnet-5.5" => (CLAUDE, "high"),
        "claude-sonnet-5" => (CLAUDE, "high"),
        "claude-sonnet-4.6" => (CLAUDE_4_6, "high"),
        "gpt-5.6-sol" | "gpt-5.6-terra" | "gpt-5.6-luna" => (GPT_5_6, "high"),
        "kimi-k3" => (KIMI_K3, "max"),
        _ => return None,
    })
}

fn model_capabilities(model_id: &str) -> Value {
    let Some((levels, default)) = model_effort(model_id) else {
        return empty_capabilities();
    };
    let label = |level: &str| {
        match level {
            "none" => "None",
            "low" => "Low",
            "medium" => "Medium",
            "high" => "High",
            "xhigh" => "Extra High",
            "max" => "Max",
            other => other,
        }
        .to_owned()
    };
    let options = levels
        .iter()
        .map(|level| (level.to_string(), label(level), None, *level == default))
        .collect::<Vec<_>>();
    json!({"optionDescriptors": [select_descriptor("reasoningEffort", "Reasoning", &options)]})
}

/// `kiroModelsFromList`: the account's models, the default one under the
/// "default" slug with Kiro's id as its alias.
fn models_from_list(output: Option<&CommandResult>) -> Option<Vec<Value>> {
    let output = output.filter(|o| o.code == 0)?;
    let list: Value = serde_json::from_str(output.stdout.trim()).ok()?;
    let entries = list["models"].as_array()?;
    let mut parsed = Vec::new();
    for entry in entries {
        let id = entry["model_id"].as_str()?.to_owned();
        let name = match entry.get("model_name") {
            None => None,
            Some(Value::String(name)) => Some(name.clone()),
            Some(_) => return None,
        };
        parsed.push((id, name));
    }
    let default_id = match list.get("default_model") {
        None => FALLBACK_DEFAULT_MODEL.to_owned(),
        Some(Value::String(id)) => id.clone(),
        Some(_) => return None,
    };
    if parsed.is_empty() {
        return None;
    }
    let listed_default = parsed.iter().position(|(id, _)| *id == default_id);
    let default_model = json!({
        "slug": DEFAULT_MODEL_SLUG,
        "name": match listed_default {
            Some(index) => parsed[index].1.clone().unwrap_or_else(|| parsed[index].0.clone()),
            None => "Kiro default".into(),
        },
        "aliases": [default_id],
        "isCustom": false,
        "isDefault": true,
        "capabilities": empty_capabilities(),
    });
    let mut models: Vec<Value> = parsed
        .iter()
        .enumerate()
        .map(|(index, (id, name))| {
            if Some(index) == listed_default {
                default_model.clone()
            } else {
                json!({"slug": id, "name": name.clone().unwrap_or_else(|| id.clone()),
                       "isCustom": false, "capabilities": model_capabilities(id)})
            }
        })
        .collect();
    if listed_default.is_none() {
        models.insert(0, default_model);
    }
    Some(models)
}

/// `kiroAgentDescriptor`: the agent picker from `_kiro/config/template`,
/// when it offers more than one agent.
fn agent_descriptor(template: &Value) -> Option<Value> {
    // `KiroConfigTemplate` must decode as a whole.
    let options = template["configOptions"].as_array()?;
    for option in options {
        if !option["id"].is_string() || !option["type"].is_string() {
            return None;
        }
        if let Some(choices) = option.get("options") {
            for choice in choices.as_array()? {
                if !choice["value"].is_string() || !choice["name"].is_string() {
                    return None;
                }
                if !matches!(
                    choice.get("description"),
                    None | Some(Value::Null | Value::String(_))
                ) {
                    return None;
                }
            }
        }
    }
    let mode = options
        .iter()
        .find(|o| o["id"] == AGENT_CONFIG_ID && o["type"] == "select")?;
    let default_agent = mode["currentValue"].as_str();
    let choices: Vec<Value> = mode["options"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|choice| {
            let id = choice["value"].as_str()?.trim();
            if id.is_empty() {
                return None;
            }
            let name = choice["name"].as_str().unwrap_or("").trim();
            let mut entry = Map::new();
            entry.insert("id".into(), json!(id));
            entry.insert(
                "label".into(),
                json!(if name.is_empty() { id } else { name }),
            );
            if let Some(description) = choice["description"]
                .as_str()
                .map(str::trim)
                .filter(|d| !d.is_empty())
            {
                entry.insert("description".into(), json!(description));
            }
            if Some(id) == default_agent {
                entry.insert("isDefault".into(), json!(true));
            }
            Some(Value::Object(entry))
        })
        .collect();
    if choices.len() < 2 {
        return None;
    }
    let mut descriptor =
        json!({"id": AGENT_CONFIG_ID, "label": "Agent", "type": "select", "options": choices});
    if let Some(agent) = default_agent {
        if choices.iter().any(|c| c["isDefault"] == true) {
            descriptor["currentValue"] = json!(agent);
        }
    }
    Some(descriptor)
}

async fn probe_agents(binary: &str, env: &ProcessEnv) -> Option<Value> {
    let args: Vec<String> = ACP_ARGS.iter().map(|a| a.to_string()).collect();
    let cwd = std::env::current_dir().unwrap_or_default();
    let mut session = AcpSession::start(binary, &args, env, &cwd).ok()?;
    session.initialize("t3-code-provider-probe").await.ok()?;
    let template = session
        .request("_kiro/config/template", json!({}))
        .await
        .ok()?;
    agent_descriptor(&template)
}

pub(super) async fn check(config: &Value, env: &ProcessEnv) -> Draft {
    let checked_at = now_iso();
    let probe = probe_env(env);
    let binary = cli_binary(config, "kiro-cli");
    let snapshot = |installed: bool,
                    version: Option<String>,
                    status: &'static str,
                    auth: Value,
                    message: Option<&str>,
                    listed: Option<Vec<Value>>,
                    agents: Option<&Value>| Draft {
        installed,
        version,
        status,
        auth,
        checked_at: checked_at.clone(),
        message: message.map(str::to_owned),
        models: models(config, listed, agents),
        slash_commands: vec![compact_slash_command()],
        version_advisory: true,
        ..Default::default()
    };
    let version_output = match run_cli(
        &binary,
        &["--version"],
        &probe,
        probe_timeout("kiro", VERSION_TIMEOUT_MS, env),
    )
    .await
    {
        Ok(output) => output,
        Err(error) => {
            let missing = matches!(error, RunError::NotFound);
            return snapshot(
                !missing,
                None,
                "error",
                json!({"status": "unknown"}),
                Some(if missing {
                    "Kiro CLI (`kiro-cli`) is not installed or not on PATH."
                } else {
                    "Failed to run `kiro-cli --version`."
                }),
                None,
                None,
            );
        }
    };
    let version = cli_version(&version_output);
    if version_output.code != 0 {
        return snapshot(
            true,
            version,
            "error",
            json!({"status": "unknown"}),
            Some("Kiro CLI is installed but failed to run."),
            None,
            None,
        );
    }
    let auth_timeout = probe_timeout("kiro", AUTH_PROBE_TIMEOUT_MS, env);
    let whoami = run_cli(
        &binary,
        &["whoami", "--format", "json"],
        &probe,
        auth_timeout,
    )
    .await
    .ok();
    let auth = auth_from_whoami(whoami.as_ref(), env);
    if auth["status"] == "unauthenticated" {
        return snapshot(
            true,
            version,
            "error",
            auth,
            Some("Kiro CLI is installed but not signed in. Run `kiro-cli login`."),
            None,
            None,
        );
    }
    // Signed out, `--list-models` starts a browser login and the agent probe
    // fails, so both wait for `whoami` to confirm the account.
    let (listed, agents) = if auth["status"] == "authenticated" {
        let list = async {
            let output = run_cli(
                &binary,
                &["chat", "--list-models", "--format", "json"],
                &probe,
                auth_timeout,
            )
            .await
            .ok();
            models_from_list(output.as_ref())
        };
        let agents = async {
            tokio::time::timeout(auth_timeout, probe_agents(&binary, &probe))
                .await
                .ok()
                .flatten()
        };
        tokio::join!(list, agents)
    } else {
        (None, None)
    };
    let authenticated = auth["status"] == "authenticated";
    snapshot(
        true,
        version,
        if authenticated { "ready" } else { "warning" },
        auth,
        (!authenticated)
            .then_some("Could not confirm the Kiro sign-in. Run `kiro-cli whoami` to check."),
        listed,
        agents.as_ref(),
    )
}
