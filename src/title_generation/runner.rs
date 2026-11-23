//! Running a title through a provider CLI, as the reference's
//! `TextGeneration` service does: the selected instance, or a fallback when
//! its snapshot says it cannot generate text, then `codex exec` or
//! `claude -p` with the title's JSON schema.
use serde_json::{json, Value};
use std::time::Duration;
use tokio::io::AsyncWriteExt;

const CLI_TIMEOUT: Duration = Duration::from_secs(180);
const CODEX_MCP_LIST_TIMEOUT: Duration = Duration::from_secs(10);
/// `DEFAULT_TEXT_GENERATION_REASONING_EFFORT`.
const DEFAULT_REASONING_EFFORT: &str = "low";
/// `TEXT_GENERATION_FALLBACK_DRIVERS`.
const FALLBACK_DRIVERS: [&str; 8] = [
    "codex",
    "claudeAgent",
    "cursor",
    "grok",
    "muse",
    "pi",
    "opencode",
    "antigravity",
];
const NO_SESSION_PERSISTENCE: &str = "--no-session-persistence";
const NO_SESSION_PERSISTENCE_REJECTED: &str = "error: unknown option '--no-session-persistence'";
const MANAGED_MCP_CONFIG: &str = "/etc/claude-code/managed-mcp.json";

/// `defaultTextGenerationModel`: a driver's text model, else its default
/// model, else the overall text model.
pub(crate) fn default_text_generation_model(driver: &str) -> &'static str {
    match driver {
        "codex" => "gpt-6-luna",
        "antigravity" => "antigravity-default",
        "claudeAgent" => "claude-haiku-5-5",
        "cursor" => "composer-2",
        "opencode" => "opencode/big-pickle",
        "grok" => "grok-build",
        "fx" | "kiro" | "muse" | "acpRegistry" | "pi" => "default",
        _ => "gpt-6-luna",
    }
}

/// What a title is generated from.
pub(crate) struct Request<'a> {
    pub cwd: &'a str,
    pub prompt: &'a str,
    /// The attachments of the prompt, whose images the CLI is given.
    pub attachments: &'a [Value],
    pub selection: Value,
    pub project_id: &'a str,
}

fn option_string(selection: &Value, id: &str) -> Option<String> {
    selection["options"]
        .as_array()?
        .iter()
        .find(|option| option["id"] == id)?["value"]
        .as_str()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
}

fn option_bool(selection: &Value, id: &str) -> Option<bool> {
    selection["options"]
        .as_array()?
        .iter()
        .find(|option| option["id"] == id)?["value"]
        .as_bool()
}

fn driver_of<'a>(settings: &'a Value, instance_id: &'a str) -> &'a str {
    settings["providerInstances"][instance_id]["driver"]
        .as_str()
        .unwrap_or(instance_id)
}

/// `exhaustedUsageWindow` for a model: a window used up that applies to it
/// and has not reset.
fn exhausted_usage_window(provider: Option<&Value>, now_ms: i64, model: &str) -> bool {
    let Some(windows) = provider.and_then(|p| p["usageLimits"]["windows"].as_array()) else {
        return false;
    };
    windows.iter().any(|window| {
        window["usedPercent"].as_f64().unwrap_or(0.0) >= 100.0
            && window["modelIds"]
                .as_array()
                .is_none_or(|ids| ids.iter().any(|id| id == model))
            && window["providerIds"].as_array().is_none_or(|ids| {
                let prefix = model.split('/').next().unwrap_or_default();
                ids.iter().any(|id| id == prefix)
            })
            && window["resetsAt"]
                .as_str()
                .and_then(|at| {
                    time::OffsetDateTime::parse(at, &time::format_description::well_known::Rfc3339)
                        .ok()
                })
                .is_none_or(|at| at.unix_timestamp_nanos() / 1_000_000 > i128::from(now_ms))
    })
}

/// The selection a title runs with: `selection`, or the reference's
/// fallback when the selected instance's snapshot shows it cannot run.
pub(crate) fn choose_selection(
    settings: &Value,
    snapshots: &[Value],
    project_id: &str,
    selection: Value,
    now_ms: i64,
) -> Value {
    let instance_id = selection["instanceId"].as_str().unwrap_or_default();
    let model = selection["model"].as_str().unwrap_or_default();
    let selected = snapshots.iter().find(|p| p["instanceId"] == instance_id);
    let unusable = selected.is_some_and(|p| {
        p["enabled"] == true
            && (p["supportsTextGeneration"] == false
                || p["models"].as_array().is_some_and(|models| {
                    models
                        .iter()
                        .find(|m| m["slug"] == model)
                        .is_some_and(|m| m["supportsTextGeneration"] == false)
                })
                || (p["status"] == "error"
                    && (p["installed"] != true || p["auth"]["status"] == "unauthenticated")))
    });
    if !unusable && !exhausted_usage_window(selected, now_ms, model) {
        return selection;
    }
    let enabled = |candidate: &str, candidate_model: &str| {
        let Some(p) = snapshots.iter().find(|p| p["instanceId"] == candidate) else {
            return false;
        };
        candidate != instance_id
            && p["enabled"] == true
            && crate::orchestration::provider_instance_enabled(settings, project_id, candidate)
            && p["installed"] == true
            && p["auth"]["status"] != "unauthenticated"
            && p["status"] != "error"
            && p["availability"] != "unavailable"
            && p["supportsTextGeneration"] != false
            && !exhausted_usage_window(Some(p), now_ms, candidate_model)
    };
    let default = &settings["defaultModelSelection"];
    let mut candidates: Vec<(String, String)> = FALLBACK_DRIVERS
        .iter()
        .map(|driver| ((*driver).to_owned(), (*driver).to_owned()))
        .collect();
    if let Some(instances) = settings["providerInstances"].as_object() {
        for (id, instance) in instances {
            let driver = instance["driver"].as_str().unwrap_or_default();
            if FALLBACK_DRIVERS.contains(&driver) {
                candidates.push((id.clone(), driver.to_owned()));
            }
        }
    }
    for (id, driver) in candidates {
        let driver_default =
            json!({"instanceId": id, "model": default_text_generation_model(&driver)});
        let custom = settings["providerInstances"][&id]["config"]["customModels"]
            .as_array()
            .is_some_and(|entries| {
                entries.iter().any(|entry| {
                    entry
                        .as_str()
                        .or_else(|| entry["slug"].as_str())
                        .map(str::trim)
                        == default["model"].as_str()
                })
            });
        let choices = if default["instanceId"] == id.as_str() && custom {
            vec![default.clone(), driver_default]
        } else {
            vec![driver_default]
        };
        for choice in choices {
            if enabled(&id, choice["model"].as_str().unwrap_or_default()) {
                return choice;
            }
        }
    }
    selection
}

/// A CLI run's output.
struct Output {
    stdout: String,
    stderr: String,
    code: Option<i32>,
}

/// Run `program` in `cwd` with `stdin`, killing it (and what it started)
/// at `timeout`. `None` when it timed out.
async fn run_cli(
    program: &str,
    args: &[String],
    cwd: &str,
    env: &[(String, String)],
    env_remove: &[&str],
    stdin: &str,
    timeout: Duration,
) -> std::io::Result<Option<Output>> {
    let mut command = crate::login_shell::tokio_command(program);
    for (name, _) in std::env::vars_os() {
        let text = name.to_string_lossy();
        if env_remove
            .iter()
            .any(|removed| text.eq_ignore_ascii_case(removed))
        {
            command.env_remove(&name);
        }
    }
    for (name, value) in env {
        command.env(name, value);
    }
    #[cfg(unix)]
    command.process_group(0);
    let mut child = command
        .args(args)
        .current_dir(cwd)
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .kill_on_drop(true)
        .spawn()?;
    let pid = child.id();
    if let Some(mut input) = child.stdin.take() {
        let text = stdin.to_owned();
        tokio::spawn(async move {
            let _ = input.write_all(text.as_bytes()).await;
            let _ = input.shutdown().await;
        });
    }
    match tokio::time::timeout(timeout, child.wait_with_output()).await {
        Ok(output) => {
            let output = output?;
            Ok(Some(Output {
                stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
                stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
                code: output.status.code(),
            }))
        }
        Err(_) => {
            #[cfg(unix)]
            if let Some(pid) = pid.and_then(|pid| i32::try_from(pid).ok()) {
                // SAFETY: kill(2) with a negative pid signals that process
                // group; it touches no memory of ours.
                unsafe { libc::kill(-pid, libc::SIGKILL) };
            }
            Ok(None)
        }
    }
}

fn failed(cli: &str, output: &Output) -> String {
    let stderr = output.stderr.trim();
    let stdout = output.stdout.trim();
    let detail = if stderr.is_empty() { stdout } else { stderr };
    if detail.is_empty() {
        format!(
            "{cli} CLI command failed with code {}.",
            output
                .code
                .map_or("null".to_owned(), |code| code.to_string())
        )
    } else {
        format!("{cli} CLI command failed: {detail}")
    }
}

fn instance_env(instance: &Value) -> Vec<(String, String)> {
    instance["environment"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|variable| {
            Some((
                variable["name"].as_str()?.to_owned(),
                variable["value"].as_str()?.to_owned(),
            ))
        })
        .collect()
}

fn config_text(instance: &Value, key: &str) -> String {
    instance["config"][key]
        .as_str()
        .unwrap_or_default()
        .trim()
        .to_owned()
}

/// A temporary directory holding one file, as Effect's `makeTempFile`
/// makes it; removed on drop.
fn temp_file(
    prefix: &str,
    content: &str,
) -> std::io::Result<(tempfile::TempDir, std::path::PathBuf)> {
    let dir = tempfile::Builder::new().prefix(prefix).tempdir()?;
    let path = dir.path().join(prefix.trim_end_matches('-'));
    std::fs::write(&path, content)?;
    Ok((dir, path))
}

/// The launch-argument filter of `codexLaunchArgs.ts`.
fn filter_launch_args(
    args: &[String],
    switches: &[&str],
    value_flags: &[&str],
    prefixes: &[&str],
) -> Vec<String> {
    let mut kept = Vec::new();
    let mut index = 0;
    while index < args.len() {
        let arg = &args[index];
        if switches.contains(&arg.as_str()) {
            kept.push(arg.clone());
        } else if value_flags.contains(&arg.as_str()) {
            if let Some(value) = args.get(index + 1).filter(|value| !value.starts_with('-')) {
                kept.push(arg.clone());
                kept.push(value.clone());
                index += 1;
            }
        } else if prefixes.iter().any(|prefix| arg.starts_with(prefix)) {
            kept.push(arg.clone());
        }
        index += 1;
    }
    kept
}

/// `codexExecLaunchArgs`: what `codex exec` keeps of the launch arguments,
/// without an enable of the hook features.
pub(crate) fn codex_exec_launch_args(args: &[String]) -> Vec<String> {
    let kept = filter_launch_args(
        args,
        &["--strict-config", "--oss"],
        &[
            "--config",
            "-c",
            "--enable",
            "--disable",
            "--local-provider",
            "--profile",
            "-p",
        ],
        &[
            "--config=",
            "-c=",
            "--enable=",
            "--disable=",
            "--local-provider=",
            "--profile=",
            "-p=",
        ],
    );
    let hook = |name: &str| name == "hooks" || name == "codex_hooks";
    kept.iter()
        .enumerate()
        .filter(|(index, arg)| {
            if let Some(name) = arg.strip_prefix("--enable=") {
                return !hook(name);
            }
            if arg.as_str() == "--enable" {
                return !kept.get(index + 1).is_some_and(|next| hook(next));
            }
            !(index
                .checked_sub(1)
                .is_some_and(|previous| kept[previous] == "--enable")
                && hook(arg))
        })
        .map(|(_, arg)| arg.clone())
        .collect()
}

/// `codexMcpListLaunchArgs`.
pub(crate) fn codex_mcp_list_launch_args(args: &[String]) -> Vec<String> {
    filter_launch_args(
        args,
        &[],
        &["--config", "-c", "--enable", "--disable", "--profile", "-p"],
        &[
            "--config=",
            "-c=",
            "--enable=",
            "--disable=",
            "--profile=",
            "-p=",
        ],
    )
}

const CODEX_NO_PLUGIN_ARGS: [&str; 4] = [
    "--config",
    "features.plugins=false",
    "--config",
    "features.apps=false",
];

fn image_paths(state: &crate::AppState, attachments: &[Value]) -> Vec<String> {
    attachments
        .iter()
        .filter(|attachment| attachment["type"] == "image")
        .filter_map(|attachment| {
            let path = crate::asset_http::attachment_file(state, attachment["id"].as_str()?)?;
            (path.is_absolute() && path.is_file()).then(|| path.display().to_string())
        })
        .collect()
}

/// `CodexTextGeneration.runCodexJson`.
async fn run_codex(
    state: &crate::AppState,
    instance: &Value,
    models: &[Value],
    request: &Request<'_>,
    selection: &Value,
) -> Result<Value, String> {
    let requested = selection["model"].as_str().unwrap_or_default();
    let listed: Vec<&Value> = models.iter().filter(|m| m["isCustom"] != true).collect();
    let family = |slug: &str| {
        slug.strip_prefix("openai.")
            .filter(|s| s.starts_with("gpt-"))
            .unwrap_or(slug)
            .to_owned()
    };
    let custom = instance["config"]["customModels"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|entry| entry.as_str().or_else(|| entry["slug"].as_str()))
        .map(str::trim)
        .find(|slug| *slug == requested);
    let matched = models
        .iter()
        .find(|m| m["slug"] == requested)
        .and_then(|m| m["slug"].as_str())
        .or(custom)
        .or_else(|| {
            listed
                .iter()
                .find(|m| {
                    m["slug"]
                        .as_str()
                        .is_some_and(|slug| family(slug) == family(requested))
                })
                .and_then(|m| m["slug"].as_str())
        });
    let model = matched
        .or_else(|| {
            listed
                .iter()
                .find(|m| m["isDefault"] == true)
                .and_then(|m| m["slug"].as_str())
        })
        .or_else(|| {
            listed
                .iter()
                .find(|m| m["isLegacy"] != true)
                .and_then(|m| m["slug"].as_str())
        })
        .or_else(|| listed.first().and_then(|m| m["slug"].as_str()))
        .unwrap_or(requested)
        .to_owned();
    let fell_back = matched.is_none() && model != requested;
    let launch_args = std::env::var("T3CODE_CODEX_LAUNCH_ARGS")
        .ok()
        .map(|args| args.trim().to_owned())
        .filter(|args| !args.is_empty())
        .unwrap_or_else(|| config_text(instance, "launchArgs"));
    let launch_args = crate::provider_state::split_args(&launch_args);
    let effort = (!fell_back)
        .then(|| option_string(selection, "reasoningEffort"))
        .flatten()
        .unwrap_or_else(|| DEFAULT_REASONING_EFFORT.to_owned());
    let service_tier = (!fell_back)
        .then(|| {
            option_string(selection, "serviceTier").or_else(|| {
                (option_bool(selection, "fastMode") == Some(true)).then(|| "fast".to_owned())
            })
        })
        .flatten();
    let binary = match config_text(instance, "binaryPath") {
        path if path.is_empty() => "codex".to_owned(),
        path => path,
    };
    let mut env = vec![(
        "CODEX_INTERNAL_ORIGINATOR_OVERRIDE".to_owned(),
        "T3 Code".to_owned(),
    )];
    env.extend(instance_env(instance));
    let home = config_text(instance, "homePath");
    if !home.is_empty() {
        env.push((
            "CODEX_HOME".into(),
            crate::provider_status::expand_home(&home),
        ));
    }
    let images = image_paths(state, request.attachments);
    let schema = super::prompt::OUTPUT_SCHEMA;
    let pid = std::process::id();
    let (_schema_dir, schema_path) = temp_file(&format!("t3code-codex-schema-{pid}-"), schema)
        .map_err(|_| "Failed to write temp file".to_owned())?;
    let (_output_dir, output_path) = temp_file(&format!("t3code-codex-output-{pid}-"), "")
        .map_err(|_| "Failed to write temp file".to_owned())?;
    let run = async {
        let mut list_args = codex_mcp_list_launch_args(&launch_args);
        list_args.extend(CODEX_NO_PLUGIN_ARGS.map(String::from));
        list_args.extend(["mcp", "list", "--json"].map(String::from));
        let enabled: Vec<String> = match run_cli(
            &binary,
            &list_args,
            request.cwd,
            &env,
            &[],
            "",
            CODEX_MCP_LIST_TIMEOUT,
        )
        .await
        {
            Ok(Some(output)) if output.code == Some(0) => {
                serde_json::from_str::<Vec<Value>>(&output.stdout)
                    .ok()
                    .filter(|servers| {
                        servers
                            .iter()
                            .all(|s| s["name"].is_string() && s["enabled"].is_boolean())
                    })
                    .map(|servers| {
                        servers
                            .iter()
                            .filter(|s| s["enabled"] == true)
                            .filter_map(|s| s["name"].as_str().map(str::to_owned))
                            .collect()
                    })
                    .unwrap_or_default()
            }
            _ => Vec::new(),
        };
        let servers_off = (!enabled.is_empty()).then(|| {
            format!(
                "mcp_servers={{{}}}",
                enabled
                    .iter()
                    .map(|name| format!(
                        "{}={{enabled=false}}",
                        serde_json::to_string(name).unwrap_or_default()
                    ))
                    .collect::<Vec<_>>()
                    .join(",")
            )
        });
        let exec_args = codex_exec_launch_args(&launch_args);
        let sets_table = exec_args.iter().any(|arg| mcp_table().is_match(arg));
        let mut args = vec!["exec".to_owned()];
        if let Some(off) = &servers_off {
            args.extend(["--config".to_owned(), off.clone()]);
        }
        args.extend(exec_args);
        args.extend(CODEX_NO_PLUGIN_ARGS.map(String::from));
        if let Some(off) = servers_off.as_ref().filter(|_| sets_table) {
            args.extend(["--config".to_owned(), off.clone()]);
        }
        for name in enabled
            .iter()
            .filter(|name| !name.contains('.') && !name.contains('='))
        {
            args.extend([
                "--config".to_owned(),
                format!("mcp_servers.{name}.enabled=false"),
            ]);
        }
        args.extend(
            [
                "--ephemeral",
                "--skip-git-repo-check",
                "-s",
                "read-only",
                "--model",
            ]
            .map(String::from),
        );
        args.push(model.clone());
        args.extend([
            "--config".to_owned(),
            format!("model_reasoning_effort=\"{effort}\""),
            "--config".to_owned(),
            "features.codex_hooks=false".to_owned(),
            "--config".to_owned(),
            "features.hooks=false".to_owned(),
        ]);
        if let Some(tier) = &service_tier {
            args.extend(["--config".to_owned(), format!("service_tier=\"{tier}\"")]);
        }
        args.extend([
            "--output-schema".to_owned(),
            schema_path.display().to_string(),
            "--output-last-message".to_owned(),
            output_path.display().to_string(),
        ]);
        for image in &images {
            args.extend(["--image".to_owned(), image.clone()]);
        }
        args.push("-".into());
        match run_cli(
            &binary,
            &args,
            request.cwd,
            &env,
            &[],
            request.prompt,
            CLI_TIMEOUT,
        )
        .await
        {
            Err(_) => Err("Failed to spawn Codex CLI process".to_owned()),
            Ok(None) => Err("Codex CLI request timed out.".to_owned()),
            Ok(Some(output)) if output.code != Some(0) => Err(failed("Codex", &output)),
            Ok(Some(_)) => Ok(()),
        }
    };
    match tokio::time::timeout(CLI_TIMEOUT, run).await {
        Err(_) => return Err("Codex CLI request timed out.".into()),
        Ok(result) => result?,
    }
    let text = std::fs::read_to_string(&output_path)
        .map_err(|_| "Failed to read Codex output file.".to_owned())?;
    decode_title(&text).ok_or_else(|| "Codex returned invalid structured output.".into())
}

fn mcp_table() -> &'static regex::Regex {
    static PATTERN: std::sync::OnceLock<regex::Regex> = std::sync::OnceLock::new();
    PATTERN.get_or_init(|| regex::Regex::new(r"\bmcp_servers\s*=").expect("pattern"))
}

/// The title output decoded strictly: a string `title`, and
/// `needsRefinement` a boolean when present.
fn decode_title(text: &str) -> Option<Value> {
    let value: Value = serde_json::from_str(text).ok()?;
    decode_title_value(&value)
}

/// `extractJsonObject`: the first balanced `{...}` of a model's reply (strings
/// and escapes respected), else the trimmed reply.
pub(crate) fn extract_json_object(raw: &str) -> &str {
    let trimmed = raw.trim();
    let Some(start) = trimmed.find('{') else {
        return trimmed;
    };
    let (mut depth, mut in_string, mut escaping) = (0i64, false, false);
    for (index, ch) in trimmed[start..].char_indices() {
        if in_string {
            if escaping {
                escaping = false;
            } else if ch == '\\' {
                escaping = true;
            } else if ch == '"' {
                in_string = false;
            }
            continue;
        }
        match ch {
            '"' => in_string = true,
            '{' => depth += 1,
            '}' => {
                depth -= 1;
                if depth == 0 {
                    return &trimmed[start..start + index + 1];
                }
            }
            _ => {}
        }
    }
    trimmed
}

/// `decodeJsonReply`: a provider's free-text reply decoded as the title output.
fn decode_json_reply(provider: &str, text: &str) -> Result<Value, String> {
    decode_title(extract_json_object(text))
        .ok_or_else(|| format!("{provider} returned invalid structured output."))
}

fn decode_title_value(value: &Value) -> Option<Value> {
    value["title"].as_str()?;
    if !value["needsRefinement"].is_null() && !value["needsRefinement"].is_boolean() {
        return None;
    }
    Some(value.clone())
}

/// `claudeErrorResult`: the error text Claude reports on its standard
/// output.
fn claude_error_result(stdout: &str) -> Option<String> {
    let output: Value = serde_json::from_str(stdout).ok()?;
    let messages = match output {
        Value::Array(items) => items,
        other => vec![other],
    };
    messages
        .iter()
        .filter(|m| m["is_error"] == true)
        .filter(|m| m["result"].is_null() || m["result"].is_string())
        .filter(|m| {
            m["errors"].is_null()
                || m["errors"]
                    .as_array()
                    .is_some_and(|e| e.iter().all(Value::is_string))
        })
        .map(|m| {
            m["result"]
                .as_str()
                .map(str::to_owned)
                .or_else(|| {
                    m["errors"].as_array().map(|errors| {
                        errors
                            .iter()
                            .filter_map(Value::as_str)
                            .filter(|e| !e.starts_with("[ede_diagnostic]"))
                            .collect::<Vec<_>>()
                            .join("\n")
                    })
                })
                .unwrap_or_default()
                .trim()
                .to_owned()
        })
        .rfind(|text| !text.is_empty())
}

/// `resolveClaudeCatalogApiModelId`: the model's slug, with the suffix an
/// option of its profile picks.
fn claude_api_model(manifest: &Value, selection: &Value) -> String {
    let requested = selection["model"]
        .as_str()
        .unwrap_or_default()
        .trim()
        .to_owned();
    let catalog = &manifest["providers"]["claudeAgent"];
    let Some(entry) = catalog["models"].as_array().and_then(|models| {
        models
            .iter()
            .find(|m| m["slug"] == requested.as_str())
            .or_else(|| {
                models.iter().find(|m| {
                    m["aliases"].as_array().is_some_and(|aliases| {
                        aliases
                            .iter()
                            .filter_map(Value::as_str)
                            .any(|a| a.eq_ignore_ascii_case(&requested))
                    })
                })
            })
    }) else {
        return selection["model"].as_str().unwrap_or_default().to_owned();
    };
    let slug = entry["slug"].as_str().unwrap_or_default().to_owned();
    let profile = &catalog["profiles"][entry["profile"].as_str().unwrap_or_default()];
    let descriptors = profile["capabilities"]["optionDescriptors"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    if let Some(suffixes) = profile["adapter"]["claudeCode"]["modelSuffixes"].as_object() {
        for (option, by_value) in suffixes {
            let descriptor = descriptors.iter().find(|d| d["id"] == option.as_str());
            let value = option_string(selection, option)
                .filter(|v| {
                    descriptor.is_some_and(|d| {
                        d["options"]
                            .as_array()
                            .is_some_and(|o| o.iter().any(|o| o["id"] == v.as_str()))
                    })
                })
                .or_else(|| {
                    let d = descriptor?;
                    d["currentValue"].as_str().map(str::to_owned).or_else(|| {
                        d["options"]
                            .as_array()?
                            .iter()
                            .find(|o| o["isDefault"] == true)?["id"]
                            .as_str()
                            .map(str::to_owned)
                    })
                });
            if let Some(suffix) = value.and_then(|v| by_value[&v].as_str().map(str::to_owned)) {
                return format!("{slug}{suffix}");
            }
        }
    }
    slug
}

/// The `--settings` value of Claude's launch arguments: inline JSON, or a
/// file relative to `cwd` (`readClaudeLaunchArgSettings`).
fn claude_launch_arg_settings(
    launch_args: &[String],
    cwd: &str,
) -> Result<serde_json::Map<String, Value>, String> {
    let mut value = None;
    let mut index = 0;
    while index < launch_args.len() {
        let arg = &launch_args[index];
        if let Some(inline) = arg.strip_prefix("--settings=") {
            value = Some(inline.to_owned());
        } else if arg == "--settings" {
            value = launch_args.get(index + 1).cloned();
            index += 1;
        }
        index += 1;
    }
    let Some(value) = value else {
        return Ok(Default::default());
    };
    let trimmed = value.trim();
    let invalid = || "Invalid Claude --settings launch argument.".to_owned();
    if trimmed.is_empty() {
        return Err(invalid());
    }
    let text = if trimmed.starts_with('{') {
        trimmed.to_owned()
    } else {
        let path = std::path::Path::new(cwd).join(crate::provider_status::expand_home(trimmed));
        std::fs::read_to_string(path).map_err(|_| invalid())?
    };
    match serde_json::from_str::<Value>(&text) {
        Ok(Value::Object(map)) => Ok(map),
        _ => Err(invalid()),
    }
}

/// `ClaudeTextGeneration.runClaudeJson`.
async fn run_claude(
    state: &crate::AppState,
    instance: &Value,
    request: &Request<'_>,
    selection: &Value,
) -> Result<Value, String> {
    let manifest = state.model_manifest.current();
    // Titles run in an empty directory, so the CLI reads nothing of the project.
    let workdir = tempfile::Builder::new()
        .prefix("t3code-claude-title-")
        .tempdir()
        .map_err(|_| "Failed to create title directory".to_owned())?;
    let cwd = workdir.path().display().to_string();
    let (effort, model_settings) = crate::model_manifest::claude_launch(&manifest, selection);
    let launch_args = crate::provider_state::split_args(&config_text(instance, "launchArgs"));
    let mut settings = claude_launch_arg_settings(&launch_args, request.cwd)?;
    settings.insert("disableAllHooks".into(), json!(true));
    if let Some(thinking) = option_bool(selection, "thinking") {
        settings.insert("alwaysThinkingEnabled".into(), json!(thinking));
    }
    if model_settings["fastMode"] == true {
        settings.insert("fastMode".into(), json!(true));
    }
    let images: Vec<Value> = request
        .attachments
        .iter()
        .filter(|attachment| attachment["type"] == "image")
        .filter_map(|attachment| {
            let path = crate::asset_http::attachment_file(state, attachment["id"].as_str()?)?;
            let bytes = std::fs::read(path).ok()?;
            use base64::Engine;
            Some(json!({"type": "image", "source": {"type": "base64",
                "media_type": attachment["mimeType"],
                "data": base64::engine::general_purpose::STANDARD.encode(bytes)}}))
        })
        .collect();
    let stream_json = !images.is_empty();
    let stdin = if stream_json {
        let mut content = vec![json!({"type": "text", "text": request.prompt})];
        content.extend(images);
        format!(
            "{}\n",
            json!({"type": "user", "message": {"role": "user", "content": content}})
        )
    } else {
        request.prompt.to_owned()
    };
    let binary = match config_text(instance, "binaryPath") {
        path if path.is_empty() => "claude".to_owned(),
        path => path,
    };
    let mut env = vec![("CLAUDE_CODE_ENTRYPOINT".to_owned(), "t3-code".to_owned())];
    env.extend(instance_env(instance));
    let home = config_text(instance, "homePath");
    if !home.is_empty() {
        env.push((
            "CLAUDE_CONFIG_DIR".into(),
            crate::provider_status::expand_home(&home),
        ));
    }
    let remove = crate::provider_status::claude::HOST_SESSION_VARIABLES
        .iter()
        .copied()
        .chain(["ELECTRON_RUN_AS_NODE"])
        .collect::<Vec<_>>();
    let managed_mcp = std::path::Path::new(MANAGED_MCP_CONFIG).exists();
    let api_model = claude_api_model(&manifest, selection);
    let args = |persist: bool| {
        let mut args = vec!["-p".to_owned()];
        if stream_json {
            args.extend(
                [
                    "--input-format",
                    "stream-json",
                    "--output-format",
                    "stream-json",
                    "--verbose",
                ]
                .map(String::from),
            );
        } else {
            args.extend(["--output-format", "json"].map(String::from));
        }
        args.extend([
            "--json-schema".to_owned(),
            super::prompt::OUTPUT_SCHEMA.to_owned(),
        ]);
        args.extend(["--model".to_owned(), api_model.clone()]);
        if let Some(effort) = &effort {
            args.extend(["--effort".to_owned(), effort.clone()]);
        }
        args.extend([
            "--settings".to_owned(),
            Value::Object(settings.clone()).to_string(),
        ]);
        args.extend(["--tools", "", "--disable-slash-commands"].map(String::from));
        if persist {
            args.push(NO_SESSION_PERSISTENCE.into());
        }
        if !managed_mcp {
            args.push("--strict-mcp-config".into());
        }
        args.extend(["--permission-mode", "dontAsk"].map(String::from));
        args
    };
    let run = async {
        let mut output = run_cli(
            &binary,
            &args(true),
            &cwd,
            &env,
            &remove,
            &stdin,
            CLI_TIMEOUT,
        )
        .await;
        if let Ok(Some(first)) = &output {
            if first.code != Some(0) && first.stderr.contains(NO_SESSION_PERSISTENCE_REJECTED) {
                output = run_cli(
                    &binary,
                    &args(false),
                    &cwd,
                    &env,
                    &remove,
                    &stdin,
                    CLI_TIMEOUT,
                )
                .await;
            }
        }
        output
    };
    let output = match tokio::time::timeout(CLI_TIMEOUT, run).await {
        Err(_) | Ok(Ok(None)) => return Err("Claude CLI request timed out.".into()),
        Ok(Err(_)) => return Err("Failed to spawn Claude CLI process".into()),
        Ok(Ok(Some(output))) => output,
    };
    let stdout = if stream_json {
        output
            .stdout
            .trim()
            .split('\n')
            .next_back()
            .unwrap_or_default()
            .to_owned()
    } else {
        output.stdout.clone()
    };
    if output.code != Some(0) {
        let detail = claude_error_result(stdout.trim()).unwrap_or_else(|| {
            let stderr = output.stderr.trim();
            if stderr.is_empty() {
                stdout.trim().to_owned()
            } else {
                stderr.to_owned()
            }
        });
        return Err(if detail.is_empty() {
            format!(
                "Claude CLI command failed with code {}.",
                output
                    .code
                    .map_or("null".to_owned(), |code| code.to_string())
            )
        } else {
            format!("Claude CLI command failed: {detail}")
        });
    }
    let parsed: Value = serde_json::from_str(&stdout)
        .map_err(|_| "Claude CLI returned unexpected output format.".to_owned())?;
    let envelope = match &parsed {
        Value::Object(map) if map.contains_key("structured_output") => Some(parsed.clone()),
        Value::Array(messages) if messages.iter().all(|m| m["type"].is_string()) => messages
            .iter()
            .rev()
            .find(|m| m["type"] == "result")
            .cloned(),
        _ => return Err("Claude CLI returned unexpected output format.".into()),
    };
    envelope
        .and_then(|envelope| decode_title_value(&envelope["structured_output"]))
        .ok_or_else(|| "Claude returned invalid structured output.".into())
}

/// `TextGeneration.generateThreadTitle`: the generated, cleaned title, or
/// the error's detail.
pub(crate) async fn generate_thread_title(
    state: &crate::AppState,
    settings: &Value,
    request: Request<'_>,
) -> Result<String, String> {
    let snapshots = crate::server_config::providers::list(state, settings, &Value::Null)
        .as_array()
        .cloned()
        .unwrap_or_default();
    let selection = choose_selection(
        settings,
        &snapshots,
        request.project_id,
        request.selection.clone(),
        crate::platform::now_ms(),
    );
    let instance_id = selection["instanceId"]
        .as_str()
        .unwrap_or_default()
        .to_owned();
    let instance = settings["providerInstances"][&instance_id].clone();
    let output = match driver_of(settings, &instance_id) {
        "codex" => {
            let models = snapshots
                .iter()
                .find(|p| p["instanceId"] == instance_id.as_str())
                .and_then(|p| p["models"].as_array().cloned())
                .unwrap_or_default();
            run_codex(state, &instance, &models, &request, &selection).await?
        }
        "claudeAgent" => run_claude(state, &instance, &request, &selection).await?,
        "grok" => {
            let env = crate::provider_status::base_env(&instance_env(&instance));
            let text = crate::provider_status::grok::generate_text(
                &instance["config"],
                &env,
                std::path::Path::new(request.cwd),
                request.prompt,
                selection["model"].as_str().unwrap_or_default(),
                option_string(&selection, "reasoningEffort").as_deref(),
            )
            .await?;
            decode_json_reply("Grok Agent", &text)?
        }
        "pi" => {
            let env = crate::provider_status::base_env(&instance_env(&instance));
            let text = crate::provider_status::pi::generate_text(
                &instance["config"],
                &env,
                std::path::Path::new(request.cwd),
                request.prompt,
                selection["model"].as_str().unwrap_or("default"),
                option_string(&selection, "thinking").as_deref(),
            )
            .await?;
            decode_json_reply("Pi", &text)?
        }
        _ => {
            return Err(format!(
                "No provider instance registered for id '{instance_id}'."
            ))
        }
    };
    let raw = output["title"].as_str().unwrap_or_default();
    let title = super::prompt::sanitize_thread_title(raw);
    if title.eq_ignore_ascii_case("title") {
        return Err("The model returned the placeholder title \"title\".".into());
    }
    Ok(title)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(text: &str) -> Vec<String> {
        crate::provider_state::split_args(text)
    }

    #[test]
    fn a_reply_s_json_object_is_found_as_the_reference_finds_it() {
        assert_eq!(
            extract_json_object("Sure!\n```json\n{\"title\": \"A {b} \\\"c\\\"\"}\n``` done"),
            "{\"title\": \"A {b} \\\"c\\\"\"}"
        );
        assert_eq!(extract_json_object("  no object "), "no object");
        assert_eq!(
            extract_json_object("{\"title\": \"open"),
            "{\"title\": \"open"
        );
        assert_eq!(
            decode_json_reply("Pi", "{\"title\": \"T\"} trailing").unwrap()["title"],
            "T"
        );
        assert_eq!(
            decode_json_reply("Pi", "not json").unwrap_err(),
            "Pi returned invalid structured output."
        );
    }

    #[test]
    fn exec_keeps_config_and_profile_arguments_but_not_a_hook_enable() {
        assert_eq!(
            codex_exec_launch_args(&args(
                "--oss -c a=1 --enable hooks --enable x --model y --profile=w --verbose"
            )),
            args("--oss -c a=1 --enable x --profile=w")
        );
        assert_eq!(
            codex_mcp_list_launch_args(&args("--oss -c a=1 -p work --enable=hooks")),
            args("-c a=1 -p work --enable=hooks")
        );
    }

    #[test]
    fn an_unusable_selection_falls_back_to_the_first_ready_driver() {
        let settings = json!({"providerInstances": {}});
        let snapshots = vec![
            json!({"instanceId": "codex", "enabled": true, "installed": false, "status": "error",
                   "auth": {"status": "unknown"}, "models": []}),
            json!({"instanceId": "claudeAgent", "enabled": true, "installed": true, "status": "ready",
                   "auth": {"status": "authenticated"}, "models": []}),
        ];
        let chosen = choose_selection(
            &settings,
            &snapshots,
            "p1",
            json!({"instanceId": "codex", "model": "gpt-6-luna"}),
            0,
        );
        assert_eq!(
            chosen,
            json!({"instanceId": "claudeAgent", "model": "claude-haiku-5-5"})
        );
    }

    #[test]
    fn claude_errors_come_from_its_result_message() {
        assert_eq!(
            claude_error_result(r#"{"is_error":true,"result":" Not logged in "}"#).as_deref(),
            Some("Not logged in")
        );
        assert_eq!(claude_error_result("plain"), None);
    }
}
