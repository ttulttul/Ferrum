//! Muse Code's status check, as the reference's `checkMuseProviderStatus`:
//! `muse --version` (4s), then a read-only `muse serve` speaking MSP
//! (newline-delimited JSON-RPC 2.0) that initializes, lists its models
//! (`model/list`) and stops. Muse reports no sign-in state, so the row's auth
//! stays unknown. Its version advisory comes from how the CLI was installed:
//! mise, or the official launcher (which updates itself), else manual.
use super::{
    compact_slash_command, maintenance, models_from_settings, now_iso, probe_env, probe_timeout,
    run_cli, Draft, ProcessEnv, RunError,
};
use serde_json::{json, Map, Value};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::sync::oneshot;

const DEFAULT_TIMEOUT_MS: u64 = 4_000;
const STARTUP_TIMEOUT_MS: u64 = 8_000;
const CATALOG_EXTRA: Duration = Duration::from_secs(4);
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(10);
const EFFORT_LABELS: [(&str, &str); 8] = [
    ("none", "None"),
    ("minimal", "Minimal"),
    ("low", "Low"),
    ("medium", "Medium"),
    ("high", "High"),
    ("xhigh", "Extra High"),
    ("max", "Max"),
    ("ultra", "Ultra"),
];
const FALLBACK_EFFORTS: [&str; 5] = ["low", "medium", "high", "xhigh", "max"];

/// `makeMuseEnvironment` under the instance's own variables: an inherited
/// META_API_KEY is dropped so Muse uses its login, and auto-update is off.
pub(crate) fn environment(env: &ProcessEnv, instance: &[(String, String)]) -> ProcessEnv {
    let mut env: ProcessEnv = env
        .iter()
        .filter(|(k, _)| !k.eq_ignore_ascii_case("META_API_KEY"))
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    env.insert("MUSE_NO_AUTO_UPDATE".into(), "1".into());
    for (name, value) in instance {
        env.insert(name.clone(), value.clone());
    }
    env
}

/// `museModelCapabilities`.
fn model_capabilities(model: Option<&Value>) -> Value {
    let reported = model.and_then(|m| m["variants"].as_array());
    let efforts: Vec<&str> = match reported {
        Some(variants) => variants.iter().filter_map(Value::as_str).collect(),
        None => FALLBACK_EFFORTS.to_vec(),
    };
    let efforts: Vec<&str> = efforts
        .into_iter()
        .filter(|e| EFFORT_LABELS.iter().any(|(id, _)| id == e))
        .collect();
    let preferred = if reported.is_some() {
        model.and_then(|m| m["defaultReasoningEffort"].as_str())
    } else {
        Some("high")
    };
    let Some(default) = efforts
        .iter()
        .find(|e| Some(**e) == preferred)
        .or(efforts.first())
        .copied()
    else {
        return json!({"optionDescriptors": []});
    };
    let options: Vec<Value> = efforts
        .iter()
        .map(|id| {
            let mut option = Map::new();
            option.insert("id".into(), json!(id));
            option.insert(
                "label".into(),
                json!(EFFORT_LABELS
                    .iter()
                    .find(|(e, _)| e == id)
                    .map(|(_, l)| *l)
                    .unwrap_or(id)),
            );
            if *id == default {
                option.insert("isDefault".into(), json!(true));
            }
            let description = model
                .and_then(|m| m["reasoningEffortVariants"].as_array())
                .and_then(|variants| variants.iter().find(|v| v["tier"] == *id))
                .and_then(|v| v["description"].as_str())
                .filter(|d| !d.is_empty());
            if let Some(description) = description {
                option.insert("description".into(), json!(description));
            }
            Value::Object(option)
        })
        .collect();
    json!({"optionDescriptors": [{"id": "reasoningEffort", "label": "Reasoning", "type": "select",
                                  "currentValue": default, "options": options}]})
}

pub(crate) fn fallback_capabilities() -> Value {
    model_capabilities(None)
}

/// `parseMuseVersion`.
fn parse_version(output: &str) -> Option<String> {
    regex::Regex::new(r"\b\d+\.\d+\.\d+-R\d+(?:\.\d+)?\b")
        .unwrap()
        .find(output)
        .map(|m| m.as_str().to_owned())
        .or_else(|| super::parse_generic_version(output))
}

/// `compareMuseVersions`: a revision suffix (`-R4`) orders after the base.
pub(crate) fn compare_versions(current: &str, latest: &str) -> std::cmp::Ordering {
    let revision = regex::Regex::new(r"-R\d+(?:\.\d+)?$").unwrap();
    let comparable = |v: &str| {
        let replaced = regex::Regex::new(r"-R(\d+)(?:\.(\d+))?$")
            .unwrap()
            .replace(v, |c: &regex::Captures| {
                format!(
                    "-release.{}.{}",
                    &c[1],
                    c.get(2).map(|m| m.as_str()).unwrap_or("")
                )
            })
            .into_owned();
        match replaced.strip_suffix('.') {
            Some(base) => format!("{base}.0"),
            None => replaced,
        }
    };
    if revision.is_match(current) == revision.is_match(latest) {
        maintenance::compare_semver(&comparable(current), &comparable(latest))
    } else {
        maintenance::compare_semver(
            &revision.replace(current, ""),
            &revision.replace(latest, ""),
        )
    }
}

// ---------------------------------------------------------------------------
// MSP

type Pending = Arc<Mutex<HashMap<i64, oneshot::Sender<Option<Value>>>>>;

struct Msp {
    child: tokio::process::Child,
    stdin: Option<tokio::process::ChildStdin>,
    pending: Pending,
    next_id: i64,
}

impl Msp {
    fn start(binary: &str, env: &ProcessEnv) -> Option<Self> {
        let args: Vec<String> = [
            "serve",
            "--disable-shell",
            "--disable-write",
            "--no-session-log",
        ]
        .iter()
        .map(|a| a.to_string())
        .collect();
        let cwd = std::env::current_dir().unwrap_or_default();
        let mut command = super::command(binary, &args, env, &cwd);
        command
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::null());
        let mut child = command.spawn().ok()?;
        let stdin = child.stdin.take()?;
        let stdout = child.stdout.take()?;
        let pending: Pending = Arc::default();
        let reader = pending.clone();
        tokio::spawn(async move {
            let mut lines = BufReader::new(stdout).lines();
            while let Ok(Some(line)) = lines.next_line().await {
                let Ok(frame) = serde_json::from_str::<Value>(&line) else {
                    continue;
                };
                if frame["jsonrpc"] != "2.0" || frame.get("method").is_some() {
                    continue;
                }
                let Some(id) = frame["id"].as_i64() else {
                    continue;
                };
                let sender = reader.lock().unwrap_or_else(|e| e.into_inner()).remove(&id);
                if let Some(sender) = sender {
                    // Only an object result answers; an error or anything else fails.
                    let result = frame.get("result").filter(|r| r.is_object()).cloned();
                    let _ = sender.send(if frame.get("error").is_some() {
                        None
                    } else {
                        result
                    });
                }
            }
            reader.lock().unwrap_or_else(|e| e.into_inner()).clear();
        });
        Some(Self {
            child,
            stdin: Some(stdin),
            pending,
            next_id: 1,
        })
    }

    async fn write(&mut self, frame: Value) -> Option<()> {
        let mut line = frame.to_string();
        line.push('\n');
        let stdin = self.stdin.as_mut()?;
        stdin.write_all(line.as_bytes()).await.ok()?;
        stdin.flush().await.ok()
    }

    async fn request(&mut self, method: &str, params: Value) -> Option<Value> {
        let id = self.next_id;
        self.next_id += 1;
        let (sender, receiver) = oneshot::channel();
        self.pending
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(id, sender);
        self.write(json!({"jsonrpc": "2.0", "id": id, "method": method, "params": params}))
            .await?;
        receiver.await.ok().flatten()
    }

    /// `close`: end the input and wait for the host to exit.
    async fn close(mut self) {
        self.stdin.take();
        if tokio::time::timeout(SHUTDOWN_TIMEOUT, self.child.wait())
            .await
            .is_err()
        {
            let _ = self.child.kill().await;
        }
    }
}

/// `discoverMuseModels`: Meta's models from a read-only host.
async fn discover_models(binary: &str, env: &ProcessEnv, startup: Duration) -> Option<Vec<Value>> {
    let mut host = Msp::start(binary, env)?;
    let initialized = tokio::time::timeout(
        startup,
        host.request(
            "initialize",
            json!({"clientInfo": {"name": "t3_code", "title": "T3 Code", "version": "1"},
                   "capabilities": {"requestedCapabilities": []}}),
        ),
    )
    .await
    .ok()
    .flatten();
    let ready = initialized
        .is_some_and(|r| r["schema"]["fingerprint"].is_string() && r["schema"]["version"] == 1);
    if !ready {
        host.close().await;
        return None;
    }
    if host
        .write(json!({"jsonrpc": "2.0", "method": "initialized"}))
        .await
        .is_none()
    {
        host.close().await;
        return None;
    }
    let listed = host.request("model/list", json!({})).await;
    host.close().await;
    let catalog = listed?;
    let entries = catalog["models"].as_array()?;
    if !catalog["providerId"].is_string() {
        return None;
    }
    for model in entries {
        let valid = model["modelId"].as_str().is_some_and(|m| !m.is_empty())
            && model["displayLabel"].is_string()
            && model["providerId"].is_string()
            && model["isDefault"].is_boolean()
            && match model.get("variants") {
                None => true,
                Some(Value::String(s)) => s == "unknown",
                Some(Value::Array(v)) => v.iter().all(Value::is_string),
                Some(_) => false,
            }
            && model
                .get("defaultReasoningEffort")
                .is_none_or(Value::is_string)
            && model.get("reasoningEffortVariants").is_none_or(|v| {
                v.as_array().is_some_and(|v| {
                    v.iter().all(|t| {
                        t["tier"].is_string() && t.get("description").is_none_or(Value::is_string)
                    })
                })
            });
        if !valid {
            return None;
        }
    }
    if catalog["providerId"] != "meta" {
        return None;
    }
    let word_start = regex::Regex::new(r"\b[a-z]").unwrap();
    let mut seen: Vec<&str> = Vec::new();
    let mut models = Vec::new();
    for model in entries {
        let id = model["modelId"].as_str().unwrap();
        if model["providerId"] != "meta" || seen.contains(&id) {
            continue;
        }
        seen.push(id);
        let label = model["displayLabel"].as_str().unwrap().trim();
        let label = if label.is_empty() { id } else { label };
        let name = if label == id && label.starts_with("muse-") {
            word_start
                .replace_all(&label.replace('-', " "), |c: &regex::Captures| {
                    c[0].to_uppercase()
                })
                .into_owned()
        } else {
            label.to_owned()
        };
        models.push(
            json!({"slug": id, "name": name, "isCustom": false, "isDefault": model["isDefault"],
                           "capabilities": model_capabilities(Some(model))}),
        );
    }
    Some(models)
}

// ---------------------------------------------------------------------------
// Maintenance

/// `museMaintenance`: mise, else the official launcher (updated by running
/// it with `MUSE_SYNC_UPDATE=1`), else manual.
async fn maintenance(binary: &str, env: &ProcessEnv) -> maintenance::Capabilities {
    // `latestMuseVersion`: an installer without its own lookup is compared
    // with the release channel Muse's launcher follows.
    let channel = env
        .get("MUSE_CHANNEL")
        .filter(|c| !c.is_empty())
        .map(String::as_str)
        .unwrap_or("muse-stable");
    let muse_channel = matches!(channel, "muse-stable" | "muse-canary")
        .then(|| format!("https://api.meta.ai/muse-code/channels/{channel}"));
    let manual = maintenance::Capabilities {
        compare_versions: Some(compare_versions),
        muse_channel: muse_channel.clone(),
        ..maintenance::manual(None, None)
    };
    let Some(context) = maintenance::context(binary, env) else {
        return manual;
    };
    let mise = maintenance::MiseTool {
        tool: "muse",
        upgrade: true,
    };
    if let Some(caps) = maintenance::resolve_mise(None, Some(&mise), &context).await {
        return maintenance::Capabilities {
            compare_versions: Some(compare_versions),
            ..caps
        };
    }
    let launcher = std::fs::metadata(&context.real_command_path)
        .ok()
        .filter(|m| m.len() <= 128 * 1024)
        .and_then(|_| std::fs::read_to_string(&context.real_command_path).ok())
        .is_some_and(|source| {
            source.contains("muse-code/launcher-") && source.contains("MUSE_SYNC_UPDATE")
        });
    if !launcher {
        return manual;
    }
    let mut update_env: Vec<(&str, String)> = Vec::new();
    if let Some(channel) = context.env.get("MUSE_CHANNEL").filter(|c| !c.is_empty()) {
        update_env.push(("MUSE_CHANNEL", channel.clone()));
    }
    update_env.push(("MUSE_NO_AUTO_UPDATE", "0".into()));
    update_env.push(("MUSE_SYNC_UPDATE", "1".into()));
    update_env.push(("MUSE_UPDATE_INTERVAL_SECONDS", "0".into()));
    let command = update_env
        .iter()
        .map(|(k, v)| format!("{k}={}", maintenance::quote(v)))
        .chain([
            maintenance::quote(&context.resolved_command_path),
            "--version".into(),
        ])
        .collect::<Vec<_>>()
        .join(" ");
    maintenance::Capabilities {
        package_name: None,
        update: Some(maintenance::Update {
            command,
            executable: context.resolved_command_path.clone(),
            args: vec!["--version".into()],
            lock_key: format!("muse:{}", context.real_command_path),
            env: None,
        }),
        compare_versions: Some(compare_versions),
        muse_channel,
        ..Default::default()
    }
}

// ---------------------------------------------------------------------------
// Check

pub(super) async fn check(config: &Value, env: &ProcessEnv) -> Draft {
    let checked_at = now_iso();
    let probe = probe_env(env);
    let binary = super::cli_binary(config, "muse");
    let custom = &config["customModels"];
    let caps = maintenance(&binary, env).await;
    let snapshot = |installed: bool,
                    version: Option<String>,
                    status: &'static str,
                    message: Option<String>,
                    models: Vec<Value>| Draft {
        installed,
        version,
        status,
        auth: json!({"status": "unknown"}),
        checked_at: checked_at.clone(),
        message,
        models: models_from_settings(models, custom, fallback_capabilities()),
        slash_commands: vec![compact_slash_command()],
        maintenance: Some(caps.clone()),
        ..Default::default()
    };
    let output = match run_cli(&binary, &["--version"], &probe, probe_timeout("muse", DEFAULT_TIMEOUT_MS, &probe)).await {
        Ok(output) => output,
        Err(RunError::NotFound) => return snapshot(false, None, "error", Some(
            "Muse Code CLI (`muse`) was not found. Install Muse Code and run `muse login` on this T3 server host.".into()), vec![]),
        Err(RunError::Spawn) => return snapshot(false, None, "error", Some(
            "Failed to execute Muse Code CLI. Check its binary path on this T3 server host.".into()), vec![]),
        Err(RunError::TimedOut) => return snapshot(true, None, "error", Some("Muse Code CLI version check timed out.".into()), vec![]),
    };
    let version = if output.code == 0 {
        parse_version(&format!("{}\n{}", output.stdout, output.stderr))
    } else {
        None
    };
    if output.code != 0 {
        let message = if output.stderr.contains("installed binary is missing") {
            "Muse Code has not finished installing. Run `muse --version` on this T3 server host, then refresh its status."
        } else {
            "Muse Code CLI is installed but failed to run."
        };
        return snapshot(true, version, "error", Some(message.into()), vec![]);
    }
    let startup = probe_timeout("muse", STARTUP_TIMEOUT_MS, &probe);
    let catalog = tokio::time::timeout(
        startup + CATALOG_EXTRA,
        discover_models(&binary, &probe, startup),
    )
    .await
    .ok()
    .flatten();
    let Some(models) = catalog else {
        return snapshot(true, version, "error", Some(
            "Muse Code SDK could not read the model catalog. Check your Muse installation and run `muse login` on this T3 server host.".into()), vec![]);
    };
    if models.is_empty() {
        return snapshot(true, version, "warning", Some(
            "Muse Code returned no models. Run `muse login` on this T3 server host and refresh its status.".into()), vec![]);
    }
    snapshot(true, version, "ready", None, models)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn muse_versions_and_revisions() {
        assert_eq!(
            parse_version("muse 2.1.0-R4 (stable)").as_deref(),
            Some("2.1.0-R4")
        );
        assert_eq!(parse_version("muse 2.1.0").as_deref(), Some("2.1.0"));
        assert!(compare_versions("2.1.0-R3", "2.1.0-R10").is_lt());
        assert!(compare_versions("2.1.0-R3", "2.1.1").is_lt());
    }

    #[test]
    fn efforts_fall_back_to_the_standard_tiers() {
        let caps = fallback_capabilities();
        assert_eq!(caps["optionDescriptors"][0]["currentValue"], "high");
        assert_eq!(
            caps["optionDescriptors"][0]["options"]
                .as_array()
                .unwrap()
                .len(),
            5
        );
    }
}
