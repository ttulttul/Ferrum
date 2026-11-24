//! Pi's status check, as the reference's `checkPiProviderStatus`:
//! `pi --version` (at least 0.84.3), then Pi's RPC mode (`pi --mode rpc
//! --no-session --offline`) read for its state, models and commands, with a
//! second, extension-free launch alongside that tells which models text
//! generation can use. An active model Pi cannot run is explained with
//! `pi auth check`.
use super::{
    maintenance, models_from_settings, now_iso, probe_env, probe_timeout, run_cli, Draft,
    ProcessEnv, RunError,
};
use serde_json::{json, Map, Value};
use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::sync::oneshot;

const VERSION_TIMEOUT_MS: u64 = 10_000;
const RPC_DISCOVERY_TIMEOUT_MS: u64 = 30_000;
const TEXT_GENERATION_DISCOVERY_TIMEOUT: Duration = Duration::from_secs(5);
const DEFAULT_TIMEOUT_MS: u64 = 4_000;
const NPM_PACKAGE: &str = "@earendil-works/pi-coding-agent";

const RESERVED: [&str; 17] = [
    "--continue",
    "-c",
    "--export",
    "--fork",
    "--help",
    "-h",
    "--list-models",
    "--mode",
    "--no-session",
    "--print",
    "-p",
    "--resume",
    "-r",
    "--session",
    "--session-id",
    "--version",
    "-v",
];
const WITH_VALUES: [&str; 21] = [
    "--api-key",
    "--append-system-prompt",
    "--exclude-tools",
    "-xt",
    "--extension",
    "-e",
    "--model",
    "--models",
    "--name",
    "-n",
    "--prompt-template",
    "--provider",
    "--session-dir",
    "--skill",
    "--system-prompt",
    "--theme",
    "--thinking",
    "--tools",
    "-t",
    "--tui-mode",
    "--use-theme",
];
const WITHOUT_VALUES: [&str; 19] = [
    "--approve",
    "-a",
    "--no-approve",
    "-na",
    "--no-builtin-tools",
    "-nbt",
    "--no-context-files",
    "-nc",
    "--no-extensions",
    "-ne",
    "--no-prompt-templates",
    "-np",
    "--no-skills",
    "-ns",
    "--no-themes",
    "--no-tools",
    "-nt",
    "--offline",
    "--verbose",
];
const PROJECT_TRUST: [&str; 4] = ["--approve", "-a", "--no-approve", "-na"];
const THINKING_LEVELS: [&str; 7] = ["off", "minimal", "low", "medium", "high", "xhigh", "max"];
const MCP_ENV: [&str; 3] = [
    "T3_MCP_URL",
    "T3_MCP_BEARER_TOKEN",
    "T3_PI_MCP_EXTENSION_PATH",
];

fn empty_capabilities() -> Value {
    json!({"optionDescriptors": []})
}

/// `piModelsFromSettings`: "Pi default", the discovered models, then the
/// custom ones.
pub(crate) fn models(config: &Value, discovered: Vec<Value>) -> Vec<Value> {
    let mut built_in = vec![
        json!({"slug": "default", "name": "Pi default", "isCustom": false,
                                   "capabilities": empty_capabilities()}),
    ];
    built_in.extend(discovered);
    models_from_settings(built_in, &config["customModels"], empty_capabilities())
}

fn binary(config: &Value) -> String {
    config["binaryPath"]
        .as_str()
        .map(str::trim)
        .filter(|b| !b.is_empty())
        .unwrap_or("pi")
        .to_owned()
}

/// `resolvePiLaunchArgs`.
fn resolve_launch_args(launch_args: &str) -> Result<Vec<String>, String> {
    let args: Vec<String> = crate::provider_state::split_args(launch_args)
        .into_iter()
        .flat_map(|arg| match arg.find('=') {
            Some(at) if at > 0 && WITH_VALUES.contains(&&arg[..at]) => {
                vec![arg[..at].to_owned(), arg[at + 1..].to_owned()]
            }
            _ => vec![arg],
        })
        .collect();
    let (mut has_provider, mut has_model) = (false, false);
    let mut index = 0;
    while index < args.len() {
        let arg = &args[index];
        if let Some(reserved) = RESERVED
            .iter()
            .find(|r| arg == *r || (r.starts_with("--") && arg.starts_with(&format!("{r}="))))
        {
            return Err(format!(
                "Pi launch argument '{reserved}' is controlled by T3 Code and cannot be overridden."
            ));
        }
        if arg == "--" {
            return Err("Pi launch arguments cannot include positional prompts.".into());
        }
        if WITH_VALUES.contains(&arg.as_str()) {
            if index + 1 >= args.len() {
                return Err(format!("Pi launch argument '{arg}' requires a value."));
            }
            has_provider |= arg == "--provider";
            has_model |= arg == "--model";
            index += 2;
            continue;
        }
        if WITHOUT_VALUES.contains(&arg.as_str()) || (arg.starts_with("--") && arg.contains('=')) {
            index += 1;
            continue;
        }
        if arg.starts_with("--") {
            if args
                .get(index + 1)
                .is_some_and(|next| !next.starts_with('-') && !next.starts_with('@'))
            {
                index += 1;
            }
            index += 1;
            continue;
        }
        if arg.starts_with('-') {
            return Err(format!(
                "Pi launch argument '{arg}' is not supported by T3 Code."
            ));
        }
        return Err(format!(
            "Pi launch arguments cannot include positional prompt '{arg}'."
        ));
    }
    if has_provider && !has_model {
        return Err("Pi launch argument '--provider' requires '--model'.".into());
    }
    Ok(args)
}

fn without_explicit_extensions(args: &[String]) -> Vec<String> {
    let mut filtered = Vec::new();
    let mut index = 0;
    while index < args.len() {
        let arg = &args[index];
        if arg == "--extension" || arg == "-e" {
            index += 2;
            continue;
        }
        if !(arg.starts_with("--extension=") || arg.starts_with("-e=")) {
            filtered.push(arg.clone());
        }
        index += 1;
    }
    filtered
}

fn without_tool_selection(args: &[String]) -> Vec<String> {
    let mut filtered = Vec::new();
    let mut index = 0;
    while index < args.len() {
        let arg = args[index].as_str();
        if matches!(arg, "--tools" | "-t" | "--exclude-tools" | "-xt") {
            index += 2;
            continue;
        }
        let dropped = matches!(arg, "--no-tools" | "-nt" | "--no-builtin-tools" | "-nbt")
            || ["--tools=", "-t=", "--exclude-tools=", "-xt="]
                .iter()
                .any(|p| arg.starts_with(p));
        if !dropped {
            filtered.push(arg.to_owned());
        }
        index += 1;
    }
    filtered
}

/// `buildPiRpcLaunch` for a short-lived, sessionless discovery process.
fn rpc_launch(
    launch_args: &[String],
    env: &ProcessEnv,
    text_generation: bool,
) -> (Vec<String>, ProcessEnv) {
    let mut launch: Vec<String> = launch_args.to_vec();
    if text_generation {
        launch = without_tool_selection(&without_explicit_extensions(&launch));
    }
    let mut args = vec![
        "--mode".to_owned(),
        "rpc".to_owned(),
        "--no-session".to_owned(),
    ];
    let offline = !launch.iter().any(|a| a == "--offline");
    args.extend(launch);
    if offline {
        args.push("--offline".into());
    }
    if text_generation {
        args.push("--no-extensions".into());
        args.push("--no-tools".into());
    }
    let mut env = env.clone();
    for name in MCP_ENV {
        env.remove(name);
    }
    (args, env)
}

// ---------------------------------------------------------------------------
// RPC

/// Why a Pi RPC read failed (`PiRpcError.operation`, or a timeout).
#[derive(Debug, PartialEq)]
enum RpcError {
    /// The process could not start.
    Spawn,
    /// Pi closed its output or exited (`read`/`exit`).
    Closed,
    /// A request could not be written while Pi's output was still open
    /// (`write`), as when Pi has exited but its output is not drained yet.
    Write,
    /// Pi answered with `success: false`.
    Failed,
    TimedOut,
}

/// Each request's answer: its data, or the error text of `success: false`.
type Pending = Arc<Mutex<HashMap<String, oneshot::Sender<Result<Value, Option<String>>>>>>;

/// `makePiRpcConnection`: JSON lines, requests matched to `response` records
/// by their `t3-<n>` id.
struct Rpc {
    _child: tokio::process::Child,
    stdin: tokio::process::ChildStdin,
    pending: Pending,
    closed: tokio::sync::watch::Receiver<bool>,
    next_id: u64,
    /// The records that are not responses (Pi's events), in order.
    events: tokio::sync::mpsc::UnboundedReceiver<Value>,
    /// The error text of the last `success: false` answer.
    last_error: Option<String>,
}

impl Rpc {
    fn start(
        binary: &str,
        args: &[String],
        env: &ProcessEnv,
        cwd: &Path,
    ) -> Result<Self, RpcError> {
        let mut command = super::command(binary, args, env, cwd);
        command
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::null());
        let mut child = command.spawn().map_err(|_| RpcError::Spawn)?;
        let stdin = child.stdin.take().ok_or(RpcError::Spawn)?;
        let stdout = child.stdout.take().ok_or(RpcError::Spawn)?;
        let pending: Pending = Arc::default();
        let (closed_tx, closed) = tokio::sync::watch::channel(false);
        let (events_tx, events) = tokio::sync::mpsc::unbounded_channel();
        let reader = pending.clone();
        tokio::spawn(async move {
            let mut lines = BufReader::new(stdout).split(b'\n');
            while let Ok(Some(line)) = lines.next_segment().await {
                let line = String::from_utf8_lossy(&line);
                let line = line.strip_suffix('\r').unwrap_or(&line);
                let Ok(record) = serde_json::from_str::<Value>(line) else {
                    continue;
                };
                if record["type"] != "response" {
                    let _ = events_tx.send(record);
                    continue;
                }
                let Some(id) = record["id"].as_str() else {
                    continue;
                };
                let sender = reader.lock().unwrap_or_else(|e| e.into_inner()).remove(id);
                if let Some(sender) = sender {
                    let _ = sender.send(if record["success"] == true {
                        Ok(record["data"].clone())
                    } else {
                        Err(record["error"].as_str().map(str::to_owned))
                    });
                }
            }
            // Mark the output closed before dropping the waiters, so a request
            // registered after the drop still sees it.
            let _ = closed_tx.send(true);
            reader.lock().unwrap_or_else(|e| e.into_inner()).clear();
        });
        Ok(Self {
            _child: child,
            stdin,
            pending,
            closed,
            next_id: 0,
            events,
            last_error: None,
        })
    }

    async fn request(&mut self, kind: &str, timeout: Duration) -> Result<Value, RpcError> {
        self.send_request(json!({"type": kind}), timeout).await
    }

    /// Send `record` (a `type` and its fields) with the next id and wait for
    /// its answer.
    async fn send_request(
        &mut self,
        mut record: Value,
        timeout: Duration,
    ) -> Result<Value, RpcError> {
        let id = format!("t3-{}", self.next_id);
        self.next_id += 1;
        let (sender, receiver) = oneshot::channel();
        self.pending
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(id.clone(), sender);
        if *self.closed.borrow() {
            return Err(RpcError::Closed);
        }
        record["id"] = json!(id);
        let mut line = record.to_string();
        line.push('\n');
        if self.stdin.write_all(line.as_bytes()).await.is_err() || self.stdin.flush().await.is_err()
        {
            // The reference reports a failed write as `write` unless it is
            // already diagnosing a closed output (`read`).
            return Err(if *self.closed.borrow() {
                RpcError::Closed
            } else {
                RpcError::Write
            });
        }
        match tokio::time::timeout(timeout, receiver).await {
            Ok(Ok(Ok(data))) => Ok(data),
            Ok(Ok(Err(error))) => {
                self.last_error = error;
                Err(RpcError::Failed)
            }
            Ok(Err(_)) => Err(RpcError::Closed),
            Err(_) => Err(RpcError::TimedOut),
        }
    }
}

fn record_field<'a>(value: &'a Value, key: &str) -> &'a Value {
    if value.is_object() {
        &value[key]
    } else {
        &Value::Null
    }
}

/// `piRecordString`: any string.
fn record_string(value: &Value, key: &str) -> Option<String> {
    record_field(value, key).as_str().map(str::to_owned)
}

/// commands.ts's `recordString`: a trimmed, non-empty string.
fn trimmed_string(value: &Value, key: &str) -> Option<String> {
    record_field(value, key)
        .as_str()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_owned)
}

/// `clampPiThinkingLevel`.
fn clamp_thinking_level(input: &Value, available: &[&'static str]) -> Option<&'static str> {
    let requested = THINKING_LEVELS
        .iter()
        .position(|l| Some(*l) == input.as_str())?;
    if available.contains(&THINKING_LEVELS[requested]) {
        return Some(THINKING_LEVELS[requested]);
    }
    THINKING_LEVELS[requested + 1..]
        .iter()
        .chain(THINKING_LEVELS[..requested].iter().rev())
        .find(|l| available.contains(l))
        .copied()
        .or(available.first().copied())
}

/// `piModelCapabilities`.
fn model_capabilities(model: &Value, default_thinking: &Value) -> Value {
    let mut descriptors = Vec::new();
    if record_field(model, "reasoning") == true {
        let map = record_field(model, "thinkingLevelMap");
        let map = if map.is_object() { Some(map) } else { None };
        let levels: Vec<&'static str> = THINKING_LEVELS
            .iter()
            .copied()
            .filter(|level| {
                let mapped = map.and_then(|m| m.get(*level));
                if mapped == Some(&Value::Null) {
                    return false;
                }
                if matches!(*level, "xhigh" | "max") {
                    return mapped.is_some();
                }
                true
            })
            .collect();
        if !levels.is_empty() {
            let default = clamp_thinking_level(default_thinking, &levels);
            let label = |level: &str| match level {
                "off" => "Off",
                "minimal" => "Minimal",
                "low" => "Low",
                "medium" => "Medium",
                "high" => "High",
                "xhigh" => "Extra High",
                _ => "Max",
            };
            let options: Vec<Value> = levels
                .iter()
                .map(|level| {
                    let mut option = json!({"id": level, "label": label(level)});
                    if Some(*level) == default {
                        option["isDefault"] = json!(true);
                    }
                    option
                })
                .collect();
            descriptors.push(json!({"id": "thinking", "label": "Thinking", "type": "select", "options": options}));
        }
    }
    if let Some(tiers) = record_field(model, "serviceTiers").as_array() {
        let mut seen = vec!["default".to_owned()];
        let mut options = Vec::new();
        for tier in tiers {
            let Some(id) = record_field(tier, "id").as_str() else {
                continue;
            };
            if id.trim().is_empty() || seen.iter().any(|s| s == id) {
                continue;
            }
            seen.push(id.to_owned());
            let name = record_field(tier, "name")
                .as_str()
                .filter(|n| !n.trim().is_empty());
            let mut option = json!({"id": id, "label": name.unwrap_or(id)});
            if let Some(description) = record_field(tier, "description")
                .as_str()
                .filter(|d| !d.trim().is_empty())
            {
                option["description"] = json!(description);
            }
            options.push(option);
        }
        if !options.is_empty() {
            options.insert(
                0,
                json!({"id": "default", "label": "Standard", "isDefault": true}),
            );
            descriptors.push(
                json!({"id": "serviceTier", "label": "Service Tier", "type": "select",
                                    "options": options, "currentValue": "default"}),
            );
        }
    }
    json!({"optionDescriptors": descriptors})
}

/// `parseDiscoveredModels`.
fn parse_models(data: &Value, state: &Value) -> Option<Vec<Value>> {
    let models = record_field(data, "models").as_array()?;
    let active = record_field(state, "model");
    let active_provider = record_string(active, "provider");
    let active_id = record_string(active, "id");
    let mut seen: Vec<String> = Vec::new();
    let mut parsed = Vec::new();
    for model in models {
        let provider = record_string(model, "provider")?;
        let id = record_string(model, "id")?;
        if provider.trim().is_empty() || id.trim().is_empty() {
            return None;
        }
        let slug = format!("{provider}/{id}");
        if seen.contains(&slug) {
            continue;
        }
        seen.push(slug.clone());
        let active_model =
            Some(&provider) == active_provider.as_ref() && Some(&id) == active_id.as_ref();
        let thinking = if active_model {
            record_field(state, "thinkingLevel").clone()
        } else {
            Value::Null
        };
        let thinking = if thinking.is_string() {
            thinking
        } else {
            Value::Null
        };
        parsed.push(json!({
            "slug": slug,
            "name": record_string(model, "name").unwrap_or_else(|| slug.clone()),
            "subProvider": provider,
            "isCustom": false,
            "capabilities": model_capabilities(model, &thinking),
        }));
    }
    Some(parsed)
}

/// `parsePiDiscoveredCommands` with `/compact` first
/// (`withPiBuiltinSlashCommands`).
fn parse_commands(data: &Value) -> (Vec<Value>, Vec<Value>) {
    let mut slash_commands = vec![json!({"name": "compact",
        "description": "Summarize the conversation and reduce context usage",
        "input": {"hint": "Optional instructions"}})];
    let mut skills = Vec::new();
    for command in record_field(data, "commands")
        .as_array()
        .into_iter()
        .flatten()
    {
        let Some(name) = trimmed_string(command, "name") else {
            continue;
        };
        let description = trimmed_string(command, "description");
        if trimmed_string(command, "source").as_deref() == Some("skill") {
            let name = name.strip_prefix("skill:").unwrap_or(&name).to_owned();
            if name.is_empty() {
                continue;
            }
            let source = record_field(command, "sourceInfo");
            let interface = record_field(command, "interface");
            let path = trimmed_string(source, "path")
                .or_else(|| trimmed_string(command, "path"))
                .unwrap_or_else(|| format!("pi:skill:{name}"));
            let scope = trimmed_string(source, "scope")
                .or_else(|| trimmed_string(command, "location"))
                .map(|scope| match scope.trim().to_lowercase().as_str() {
                    "global" | "personal" => "user".to_owned(),
                    "workspace" | "local" => "project".to_owned(),
                    _ => scope,
                });
            let pick = |key: &str| {
                trimmed_string(command, key)
                    .or_else(|| trimmed_string(source, key))
                    .or_else(|| trimmed_string(interface, key))
            };
            let mut skill = Map::new();
            skill.insert("name".into(), json!(name));
            skill.insert("path".into(), json!(path));
            skill.insert("enabled".into(), json!(true));
            if let Some(description) = description {
                skill.insert("description".into(), json!(description));
            }
            if let Some(scope) = scope {
                skill.insert("scope".into(), json!(scope));
            }
            if let Some(display) = pick("displayName") {
                skill.insert("displayName".into(), json!(display));
            }
            if let Some(short) = pick("shortDescription") {
                skill.insert("shortDescription".into(), json!(short));
            }
            skills.push(Value::Object(skill));
            continue;
        }
        if name == "compact" {
            continue;
        }
        let mut entry = json!({"name": name});
        if let Some(description) = description {
            entry["description"] = json!(description);
        }
        slash_commands.push(entry);
    }
    (slash_commands, skills)
}

struct Inventory {
    state: Value,
    models: Value,
    commands: Value,
}

async fn read_inventory(
    binary: &str,
    env: &ProcessEnv,
    launch_args: &[String],
    timeout: Duration,
    cwd: &Path,
) -> Result<Inventory, RpcError> {
    let (args, env) = rpc_launch(launch_args, env, false);
    let mut rpc = Rpc::start(binary, &args, &env, cwd)?;
    let state = rpc.request("get_state", timeout).await?;
    let models = rpc.request("get_available_models", timeout).await?;
    let commands = rpc
        .request("get_commands", timeout)
        .await
        .unwrap_or(Value::Null);
    Ok(Inventory {
        state,
        models,
        commands,
    })
}

/// `discoverPiCommandsForCwd` (`snapshotForCwd`): Pi's commands and skills in
/// a folder, read over RPC there; any failure fails the scan.
pub(super) async fn folder_commands(
    config: &Value,
    env: &ProcessEnv,
    cwd: &str,
) -> Option<(Vec<Value>, Vec<Value>)> {
    let launch_args =
        resolve_launch_args(config["launchArgs"].as_str().unwrap_or("").trim()).ok()?;
    let timeout = probe_timeout("pi", RPC_DISCOVERY_TIMEOUT_MS, env);
    let read = async {
        let (args, env) = rpc_launch(&launch_args, &probe_env(env), false);
        let mut rpc = Rpc::start(&binary(config), &args, &env, Path::new(cwd)).ok()?;
        rpc.request("get_commands", timeout).await.ok()
    };
    let data = tokio::time::timeout(timeout, read).await.ok().flatten()?;
    Some(parse_commands(&data))
}

/// `discoverPiTextGenerationModelSlugs`: `Some(empty)` when Pi exits
/// before answering, `None` when the read failed otherwise.
async fn text_generation_slugs(
    binary: String,
    env: ProcessEnv,
    launch_args: Vec<String>,
    cwd: std::path::PathBuf,
) -> Option<Vec<String>> {
    let read = async {
        let (args, env) = rpc_launch(&launch_args, &env, true);
        let mut rpc = Rpc::start(&binary, &args, &env, &cwd)?;
        let data = rpc
            .request(
                "get_available_models",
                Duration::from_millis(RPC_DISCOVERY_TIMEOUT_MS),
            )
            .await?;
        parse_models(&data, &Value::Null).ok_or(RpcError::Failed)
    };
    match tokio::time::timeout(TEXT_GENERATION_DISCOVERY_TIMEOUT, read).await {
        Ok(Ok(models)) => Some(
            models
                .iter()
                .filter_map(|m| m["slug"].as_str().map(str::to_owned))
                .collect(),
        ),
        Ok(Err(RpcError::Closed)) => Some(vec![]),
        _ => None,
    }
}

/// A spawned task that is aborted when dropped, as a scoped fiber is
/// interrupted when its scope closes.
struct AbortOnDrop<T>(tokio::task::JoinHandle<T>);

impl<T> Drop for AbortOnDrop<T> {
    fn drop(&mut self) {
        self.0.abort();
    }
}

struct Discovery {
    models: Vec<Value>,
    slash_commands: Vec<Value>,
    skills: Vec<Value>,
    authenticated: bool,
    unavailable_active_model: Option<(String, String)>,
    text_generation_unavailable: bool,
}

async fn discover(
    binary: &str,
    env: &ProcessEnv,
    launch_args: &[String],
    cwd: &Path,
    timeout: Duration,
) -> Result<Discovery, RpcError> {
    // The reference forks this probe into discovery's scope, so a failed
    // discovery interrupts it (and stops its Pi). Dropping the guard on an
    // early return does the same; an unaborted task would launch Pi anyway.
    let mut side = AbortOnDrop(tokio::spawn(text_generation_slugs(
        binary.to_owned(),
        env.clone(),
        launch_args.to_vec(),
        cwd.to_path_buf(),
    )));
    let inventory = match read_inventory(binary, env, launch_args, timeout, cwd).await {
        Err(RpcError::Closed)
            if !launch_args
                .iter()
                .any(|a| PROJECT_TRUST.contains(&a.as_str())) =>
        {
            let mut retry = launch_args.to_vec();
            retry.push("--no-approve".into());
            read_inventory(binary, env, &retry, timeout, cwd).await
        }
        other => other,
    }?;
    let discovered = parse_models(&inventory.models, &inventory.state).ok_or(RpcError::Failed)?;
    let (slash_commands, skills) = parse_commands(&inventory.commands);
    let text_generation = (&mut side.0).await.ok().flatten();
    let active = record_field(&inventory.state, "model");
    let active_provider = record_string(active, "provider");
    let active_id = record_string(active, "id");
    let unavailable_active_model = match (active_provider, active_id) {
        (Some(provider), Some(id))
            if !discovered
                .iter()
                .any(|m| m["subProvider"] == provider.as_str()) =>
        {
            Some((provider, id))
        }
        _ => None,
    };
    let models = match &text_generation {
        None => discovered.clone(),
        Some(slugs) => discovered
            .iter()
            .map(|model| {
                let mut model = model.clone();
                if !slugs.iter().any(|s| model["slug"] == s.as_str()) {
                    model["supportsTextGeneration"] = json!(false);
                }
                model
            })
            .collect(),
    };
    Ok(Discovery {
        authenticated: !discovered.is_empty(),
        models,
        slash_commands,
        skills,
        unavailable_active_model,
        text_generation_unavailable: text_generation.is_some_and(|s| s.is_empty()),
    })
}

/// `describeUnavailablePiModel`.
async fn describe_unavailable_model(
    binary: &str,
    env: &ProcessEnv,
    launch_args: &[String],
    provider: &str,
    id: &str,
    timeout: Duration,
) -> String {
    let label = format!("Pi default ({provider}/{id})");
    let unavailable = format!("{label} uses {provider}, which has no available models in Pi. Choose another model, or check that provider's setup and the Pi launch arguments.");
    if launch_args.iter().any(|a| a == "--api-key") {
        return unavailable;
    }
    let output = run_cli(
        binary,
        &[
            "auth",
            "check",
            "--provider",
            provider,
            "--json",
            "--no-refresh",
        ],
        env,
        timeout,
    )
    .await;
    let missing = output.ok().is_some_and(|o| {
        serde_json::from_str::<Value>(&o.stdout)
            .ok()
            .filter(|v| v.is_object() && matches!(v.get("reason"), None | Some(Value::String(_))))
            .is_some_and(|v| v["reason"] == "credentials_not_configured")
    });
    if missing {
        format!("{label} has no credentials configured for {provider}. Choose another model, or run `pi` in a terminal and use /login or configure its API key.")
    } else {
        unavailable
    }
}

/// The reference's Pi resolver: package-managed, with `pi update --self`.
async fn maintenance(binary: &str, env: &ProcessEnv) -> maintenance::Capabilities {
    let definition = maintenance::PackageDefinition {
        provider: "pi",
        npm_package_name: NPM_PACKAGE,
        native_update: Some(maintenance::NativeUpdate {
            args: &["update", "--self"],
            is_command_path: None,
            requires_command_path: false,
        }),
        mise: Some(maintenance::MiseTool {
            tool: "pi",
            upgrade: true,
        }),
    };
    maintenance::with_configured_npm(
        maintenance::resolve_package_managed(&definition, maintenance::context(binary, env)).await,
        &super::base_env(&[]),
    )
}

pub(super) async fn check(
    config: &Value,
    env: &ProcessEnv,
    caches: &Path,
    manifest: &Value,
) -> Draft {
    let checked_at = now_iso();
    let probe = probe_env(env);
    let binary = binary(config);
    let caps = maintenance(&binary, env).await;
    let fallback = models(config, vec![]);
    let result =
        |status: &'static str, version: Option<String>, installed: bool, message: String| Draft {
            installed,
            version,
            status,
            auth: json!({"status": "unknown"}),
            checked_at: checked_at.clone(),
            message: Some(message),
            models: fallback.clone(),
            maintenance: Some(caps.clone()),
            ..Default::default()
        };
    let version_output = match run_cli(
        &binary,
        &["--version"],
        &probe,
        probe_timeout("pi", VERSION_TIMEOUT_MS, env),
    )
    .await
    {
        Ok(output) => output,
        Err(RunError::NotFound) => return result("error", None, false,
            "Pi CLI (`pi`) is not installed or not on PATH. Install with `npm install -g --ignore-scripts @earendil-works/pi-coding-agent`.".into()),
        Err(RunError::Spawn) => return result("error", None, true, "Failed to execute Pi CLI health check.".into()),
        Err(RunError::TimedOut) => return result("error", None, true,
            "Pi CLI is installed but timed out while running `pi --version`.".into()),
    };
    let version = super::cli_version(&version_output);
    if version_output.code != 0 {
        return result(
            "error",
            version,
            true,
            "Pi CLI is installed but failed to run.".into(),
        );
    }
    let Some(version) = version else {
        return result(
            "error",
            None,
            true,
            "T3 Code could not determine the Pi version. Update Pi, then refresh its status."
                .into(),
        );
    };
    // The manifest's policy decides which versions work (#39434).
    let advisory = super::provider_compatibility(manifest, "pi", &version);
    if let Some(broken) = super::broken_version_detail("Pi", &version, advisory.as_ref()) {
        return result("error", Some(version.clone()), true, broken);
    }
    let launch_args = match resolve_launch_args(config["launchArgs"].as_str().unwrap_or("").trim())
    {
        Ok(args) => args,
        Err(message) => return result("error", Some(version), true, message),
    };
    // Pi runs extensions' session_start in its working directory.
    let cwd = caches.join("pi-status-probe");
    let _ = std::fs::create_dir_all(&cwd);
    let timeout = probe_timeout("pi", RPC_DISCOVERY_TIMEOUT_MS, env);
    let discovery = match tokio::time::timeout(timeout, discover(&binary, &probe, &launch_args, &cwd, timeout)).await {
        Err(_) => return result("ready", Some(version), true,
            "Pi is available, but model and command discovery needs interactive input. The live session will handle it.".into()),
        Ok(Err(_)) => return result("ready", Some(version), true,
            "Pi is available, but T3 Code could not refresh its models and commands. The live session will retry startup.".into()),
        Ok(Ok(discovery)) => discovery,
    };
    let message = if !discovery.authenticated {
        Some("Pi has no usable models. Run `pi` in a terminal and use /login, or configure an API key in ~/.pi/agent.".to_owned())
    } else if let Some((provider, id)) = &discovery.unavailable_active_model {
        Some(
            describe_unavailable_model(
                &binary,
                &probe,
                &launch_args,
                provider,
                id,
                probe_timeout("pi", DEFAULT_TIMEOUT_MS, env),
            )
            .await,
        )
    } else {
        None
    };
    Draft {
        installed: true,
        version: Some(version),
        status: if discovery.authenticated {
            "ready"
        } else {
            "warning"
        },
        auth: json!({"status": if discovery.authenticated { "authenticated" } else { "unauthenticated" }, "type": "pi"}),
        checked_at: checked_at.clone(),
        message,
        models: models(config, discovery.models),
        slash_commands: discovery.slash_commands,
        skills: discovery.skills,
        extra: if discovery.text_generation_unavailable {
            vec![("supportsTextGeneration", json!(false))]
        } else {
            vec![]
        },
        maintenance: Some(caps),
        ..Default::default()
    }
}

// ---------------------------------------------------------------------------
// Text generation

/// How long a text generation run may take (`PI_TIMEOUT_MS`).
const TEXT_GENERATION_TIMEOUT: Duration = Duration::from_secs(180);
/// How long the version check before a thinking option may take.
const TEXT_GENERATION_VERSION_TIMEOUT: Duration = Duration::from_secs(4);
/// A request's own wait, as the reference's connection gives it.
const TEXT_GENERATION_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// `piTurnErrorDetail`: a provider failure on the assistant's `message_end`.
fn turn_error_detail(message: Option<&str>) -> String {
    let status = message.and_then(|message| {
        let code = message.get(..3)?;
        let first = code.chars().next()?;
        (matches!(first, '4' | '5')
            && code.chars().all(|c| c.is_ascii_digit())
            && message[3..]
                .chars()
                .next()
                .is_none_or(|c| !c.is_alphanumeric() && c != '_'))
        .then(|| code.to_owned())
    });
    match status {
        Some(status) => {
            format!("Pi could not generate text: the model provider returned HTTP {status}.")
        }
        None => "Pi could not generate text: the model provider reported an error.".into(),
    }
}

/// `findReportedPiModel` then `resolvePiModelSlug`: a `provider/model` slug
/// as Pi names it.
fn resolve_model_slug(slug: &str, reported: &Value) -> Option<(String, String)> {
    if let Some(found) = reported.as_array().into_iter().flatten().find_map(|model| {
        let provider = record_string(model, "provider")?;
        let id = record_string(model, "id")?;
        (format!("{provider}/{id}") == slug).then_some((provider, id))
    }) {
        return Some(found);
    }
    let separator = slug.find('/')?;
    if separator == 0 || separator == slug.len() - 1 {
        return None;
    }
    Some((
        slug[..separator].to_owned(),
        slug[separator + 1..].to_owned(),
    ))
}

/// `makePiTextGeneration`'s run: Pi's reply to `prompt` (in RPC mode, with
/// no session, extensions or tools), as text, or why there is none.
pub(crate) async fn generate_text(
    config: &Value,
    env: &ProcessEnv,
    cwd: &Path,
    prompt: &str,
    model: &str,
    thinking: Option<&str>,
) -> Result<String, String> {
    match tokio::time::timeout(
        TEXT_GENERATION_TIMEOUT,
        run_text_generation(config, env, cwd, prompt, model, thinking),
    )
    .await
    {
        Ok(result) => result,
        Err(_) => Err("Pi request timed out.".into()),
    }
}

async fn run_text_generation(
    config: &Value,
    env: &ProcessEnv,
    cwd: &Path,
    prompt: &str,
    model: &str,
    thinking: Option<&str>,
) -> Result<String, String> {
    let failed = || "Pi text generation failed.".to_owned();
    let binary = binary(config);
    let thinking = thinking.filter(|level| THINKING_LEVELS.contains(level));
    if thinking.is_some() {
        let version = run_cli(
            &binary,
            &["--version"],
            env,
            TEXT_GENERATION_VERSION_TIMEOUT,
        )
        .await
        .ok()
        .filter(|output| output.code == 0)
        .and_then(|output| super::cli_version(&output));
        // The bundled manifest's policy decides which versions work (#39434).
        let broken = match &version {
            None => Some(
                "T3 Code could not determine the Pi version. Update Pi and try again.".to_owned(),
            ),
            Some(version) => super::broken_version_detail(
                "Pi",
                version,
                super::provider_compatibility(&crate::model_manifest::BUNDLED, "pi", version)
                    .as_ref(),
            ),
        };
        if let Some(broken) = broken {
            return Err(broken);
        }
    }
    let launch_args = resolve_launch_args(config["launchArgs"].as_str().unwrap_or("").trim())?;
    let (args, env) = rpc_launch(&launch_args, env, true);
    let mut rpc = Rpc::start(&binary, &args, &env, cwd).map_err(|_| failed())?;
    if model != "default" {
        // A slug Pi cannot use is refused, rather than quietly using its default.
        let inventory = rpc
            .request("get_available_models", TEXT_GENERATION_REQUEST_TIMEOUT)
            .await
            .unwrap_or(Value::Null);
        let Some((provider, model_id)) =
            resolve_model_slug(model, record_field(&inventory, "models"))
        else {
            return Err(format!(
                "Pi model '{model}' must use provider/model format."
            ));
        };
        match rpc
            .send_request(
                json!({"type": "set_model", "provider": provider, "modelId": model_id}),
                TEXT_GENERATION_REQUEST_TIMEOUT,
            )
            .await
        {
            Ok(_) => {}
            Err(RpcError::Failed) => {
                let detail = rpc
                    .last_error
                    .take()
                    .unwrap_or_else(|| "unknown error".into());
                return Err(format!(
                    "Pi rejected model '{model}': {detail}. If the model comes from a Pi extension or package, text generation cannot use it because it runs Pi with --no-extensions. Pick a built-in Pi model or one defined in Pi's models.json."
                ));
            }
            Err(_) => return Err(failed()),
        }
    }
    if let Some(level) = thinking {
        // A level the model cannot take falls back to Pi's default.
        let _ = rpc
            .send_request(
                json!({"type": "set_thinking_level", "level": level}),
                TEXT_GENERATION_REQUEST_TIMEOUT,
            )
            .await;
    }
    rpc.send_request(
        json!({"type": "prompt", "message": prompt}),
        TEXT_GENERATION_REQUEST_TIMEOUT,
    )
    .await
    .map_err(|_| failed())?;
    // A provider failure arrives on the assistant's `message_end`; only the
    // latest counts, as Pi can retry and still answer.
    let mut turn_error = None;
    loop {
        let Some(event) = rpc.events.recv().await else {
            return Err(failed());
        };
        if event["type"] == "agent_settled" {
            break;
        }
        let message = &event["message"];
        if event["type"] == "message_end"
            && record_string(message, "role").as_deref() == Some("assistant")
        {
            turn_error = (record_string(message, "stopReason").as_deref() == Some("error"))
                .then(|| turn_error_detail(record_string(message, "errorMessage").as_deref()));
        }
    }
    let data = rpc
        .request("get_last_assistant_text", TEXT_GENERATION_REQUEST_TIMEOUT)
        .await
        .map_err(|_| failed())?;
    let text = data["text"].as_str().map(str::trim).unwrap_or_default();
    if text.is_empty() {
        return Err(turn_error.unwrap_or_else(|| "Pi returned empty output.".into()));
    }
    Ok(text.to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn text_generation_names_models_and_failures_as_the_reference_does() {
        let reported = json!([{"provider": "my/org", "id": "m"}]);
        assert_eq!(
            resolve_model_slug("my/org/m", &reported),
            Some(("my/org".into(), "m".into()))
        );
        assert_eq!(
            resolve_model_slug("openai/gpt-5.6", &json!(null)),
            Some(("openai".into(), "gpt-5.6".into()))
        );
        assert_eq!(resolve_model_slug("bare", &json!(null)), None);
        assert_eq!(resolve_model_slug("trailing/", &json!(null)), None);
        assert_eq!(
            turn_error_detail(Some("429 Too Many Requests")),
            "Pi could not generate text: the model provider returned HTTP 429."
        );
        assert_eq!(
            turn_error_detail(Some("4290 tokens")),
            "Pi could not generate text: the model provider reported an error."
        );
        assert_eq!(
            turn_error_detail(None),
            "Pi could not generate text: the model provider reported an error."
        );
    }

    #[test]
    fn launch_args_like_the_reference() {
        assert_eq!(
            resolve_launch_args("--model=a/b --verbose").unwrap(),
            vec!["--model", "a/b", "--verbose"]
        );
        assert!(resolve_launch_args("--mode json")
            .unwrap_err()
            .contains("'--mode' is controlled"));
        assert!(resolve_launch_args("--provider x")
            .unwrap_err()
            .contains("requires '--model'"));
        assert!(resolve_launch_args("hello")
            .unwrap_err()
            .contains("positional prompt 'hello'"));
    }

    #[test]
    fn thinking_levels_clamp_upwards_first() {
        let caps = model_capabilities(
            &json!({"reasoning": true, "thinkingLevelMap": {"minimal": null}}),
            &json!("minimal"),
        );
        let options = &caps["optionDescriptors"][0]["options"];
        assert_eq!(options.as_array().unwrap().len(), 4);
        assert_eq!(options[1]["id"], "low");
        assert_eq!(options[1]["isDefault"], true);
    }
}
