//! fx's status check, as the reference's `checkFxProviderStatus`:
//! `fx --version`, `fx status --json` (the default backend and model), and
//! for every backend (the built-in gateway, Codex and Grok ones and the
//! endpoints in fx's profile settings) its sign-in and `fx models --json`.
//! None of them opens an ACP session, which fx would save.
use super::{
    cli_binary, cli_version, maintenance, models_from_settings, now_iso, probe_env, probe_timeout,
    run_cli, Draft, ProcessEnv, RunError, AUTH_PROBE_TIMEOUT_MS,
};
use serde_json::{json, Value};

const VERSION_TIMEOUT_MS: u64 = 10_000;
const DEFAULT_MODEL_SLUG: &str = "default";

/// `FX_EFFORT_OPTION_DESCRIPTOR`, which every fx model carries.
fn capabilities() -> Value {
    let options: Vec<Value> = [
        ("auto", "Auto"),
        ("low", "Low"),
        ("medium", "Medium"),
        ("high", "High"),
        ("xhigh", "Extra High"),
        ("max", "Max"),
    ]
    .iter()
    .map(|(id, label)| {
        let mut option = json!({"id": id, "label": label});
        if *id == "medium" {
            option["isDefault"] = json!(true);
        }
        option
    })
    .collect();
    json!({"optionDescriptors": [{"id": "effort", "label": "Effort", "type": "select", "options": options}]})
}

/// fx's built-in backends, in the order its `provider` option lists them:
/// id, name and the command that signs in to it.
const BUILT_IN_BACKENDS: [(&str, &str, &str); 3] = [
    ("gateway", "Vercel AI Gateway", "fx login"),
    ("codex", "Codex subscription", "fx login codex"),
    ("grok", "Grok subscription", "fx login grok"),
];
/// fx 0.0.14+'s ACP mode that `fx status` lists when fx offers Full access.
const FULL_ACCESS_MODE_ID: &str = "full-access";
/// The stable release `fx upgrade` and fx's setup script install.
const LATEST_RELEASE_URL: &str = "https://releases.fx.sh/latest.txt";

/// What `fx status` and `fx models` report for one backend (`FxBackendProbe`).
struct Backend {
    id: String,
    name: String,
    /// `None` when its status could not be read.
    signed_in: Option<bool>,
    sign_in_hint: String,
    models: Option<Vec<String>>,
}

/// `fxModels`: "Default" (named after the model `fx status` reports), the
/// models of every backend that may be signed in, with the backend in the
/// slug, then the custom models.
fn models(config: &Value, default_model: Option<&str>, backends: &[Backend]) -> Vec<Value> {
    let default = json!({
        "slug": DEFAULT_MODEL_SLUG,
        "name": match default_model { Some(model) => format!("Default ({model})"), None => "Default".into() },
        "isCustom": false,
        "isDefault": true,
        "capabilities": capabilities(),
    });
    let mut built_in = vec![default];
    for backend in backends.iter().filter(|b| b.signed_in != Some(false)) {
        let mut seen: Vec<&String> = Vec::new();
        for id in backend.models.iter().flatten() {
            if seen.contains(&id) {
                continue;
            }
            seen.push(id);
            built_in.push(json!({
                "slug": format!("{}:{id}", backend.id),
                "name": id,
                "subProvider": backend.name,
                "isCustom": false,
                "capabilities": capabilities(),
            }));
        }
    }
    models_from_settings(built_in, &config["customModels"], capabilities())
}

/// `fxSubProviders`: each backend, with why a signed-out one cannot run.
fn sub_providers(backends: &[Backend]) -> Value {
    backends
        .iter()
        .map(|backend| {
            if backend.signed_in == Some(false) {
                json!({"name": backend.name, "unavailableReason": backend.sign_in_hint})
            } else {
                json!({"name": backend.name})
            }
        })
        .collect()
}

fn non_empty(value: Option<&str>) -> Option<String> {
    value
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .map(str::to_owned)
}

/// `FxStatus`: an object whose `auth`, `auth_help`, `model` and
/// `model_source`, when present, are strings, and whose `modes` is a list of
/// `{id}`.
#[derive(Default)]
struct Status {
    auth: Option<String>,
    auth_help: Option<String>,
    model: Option<String>,
    model_source: Option<String>,
    modes: Option<Vec<String>>,
}

fn decode_status(stdout: &str) -> Option<Status> {
    let value: Value = serde_json::from_str(stdout).ok()?;
    let object = value.as_object()?;
    let field = |key: &str| match object.get(key) {
        None => Some(None),
        Some(Value::String(text)) => Some(Some(text.clone())),
        Some(_) => None,
    };
    let modes = match object.get("modes") {
        None => None,
        Some(Value::Array(modes)) => Some(
            modes
                .iter()
                .map(|mode| mode.as_object()?.get("id")?.as_str().map(str::to_owned))
                .collect::<Option<Vec<_>>>()?,
        ),
        Some(_) => return None,
    };
    Some(Status {
        auth: field("auth")?,
        auth_help: field("auth_help")?,
        model: field("model")?,
        model_source: field("model_source")?,
        modes,
    })
}

/// `FxModels`: `{"ids": [string, ...]}`.
fn decode_models(stdout: &str) -> Option<Vec<String>> {
    let value: Value = serde_json::from_str(stdout).ok()?;
    value["ids"]
        .as_array()?
        .iter()
        .map(|id| id.as_str().map(str::to_owned))
        .collect()
}

/// `fxAuth`.
fn auth_of(auth: Option<&str>) -> Value {
    match non_empty(auth) {
        None => json!({"status": "unknown"}),
        Some(method) if method == "missing" => json!({"status": "unauthenticated"}),
        Some(method) => json!({"status": "authenticated", "label": method}),
    }
}

/// `fxStatusBackend`: the backend `model_source` names, by its label for a
/// built-in one and its id for a configured endpoint; the gateway otherwise.
fn status_backend(model_source: Option<&str>) -> String {
    match non_empty(model_source) {
        None => "gateway".into(),
        Some(source) => BUILT_IN_BACKENDS
            .iter()
            .find(|(_, name, _)| *name == source)
            .map(|(id, _, _)| (*id).to_owned())
            .unwrap_or(source),
    }
}

/// `readFxConfiguredBackends`: the `providers` keys of fx's profile settings
/// (`$FX_HOME/settings.json`, else `~/.fx/settings.json`) that fx accepts as
/// ids and that are not built-in names.
fn configured_backends(env: &ProcessEnv) -> Vec<String> {
    let trimmed = |name: &str| {
        env.get(name)
            .map(|v| v.trim().to_owned())
            .filter(|v| !v.is_empty())
    };
    let profile = match (trimmed("FX_HOME"), trimmed("HOME")) {
        (Some(fx_home), _) => std::path::PathBuf::from(fx_home),
        (None, Some(home)) => std::path::Path::new(&home).join(".fx"),
        _ => return vec![],
    };
    let Ok(text) = std::fs::read_to_string(profile.join("settings.json")) else {
        return vec![];
    };
    let Ok(Value::Object(settings)) = serde_json::from_str::<Value>(&text) else {
        return vec![];
    };
    let providers = match settings.get("providers") {
        None => return vec![],
        Some(Value::Object(providers)) => providers,
        Some(_) => return vec![],
    };
    let id_rule = regex::Regex::new(r"^[A-Za-z][\w-]*$").unwrap();
    providers
        .keys()
        .filter(|id| {
            id_rule.is_match(id)
                && !BUILT_IN_BACKENDS
                    .iter()
                    .any(|(built_in, _, _)| *built_in == id.to_lowercase())
        })
        .cloned()
        .collect()
}

pub(super) async fn check(config: &Value, env: &ProcessEnv) -> Draft {
    let checked_at = now_iso();
    let probe = probe_env(env);
    let binary = cli_binary(config, "fx");
    let caps = maintenance(&binary, env).await;
    let snapshot = |installed: bool,
                    version: Option<String>,
                    status: &'static str,
                    auth: Value,
                    message: Option<&str>,
                    default_model: Option<&str>,
                    backends: &[Backend],
                    full_access: bool| {
        let mut extra = vec![("supportsTextGeneration", json!(false))];
        if !backends.is_empty() {
            extra.push(("subProviders", sub_providers(backends)));
        }
        if full_access {
            extra.push((
                "supportedRuntimeModes",
                json!(["approval-required", "auto", "full-access"]),
            ));
        }
        Draft {
            installed,
            version,
            status,
            auth,
            checked_at: checked_at.clone(),
            message: message.map(str::to_owned),
            models: models(config, default_model, backends),
            extra,
            maintenance: Some(caps.clone()),
            ..Default::default()
        }
    };
    let version_output = match run_cli(
        &binary,
        &["--version"],
        &probe,
        probe_timeout("fx", VERSION_TIMEOUT_MS, env),
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
                    "fx is not installed or not on PATH."
                } else {
                    "Failed to run `fx --version`."
                }),
                None,
                &[],
                false,
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
            Some("fx is installed but failed to run."),
            None,
            &[],
            false,
        );
    }
    let timeout = probe_timeout("fx", AUTH_PROBE_TIMEOUT_MS, env);
    // `readFxCli`: stdout of a successful run, or none.
    let read = |args: &'static [&'static str], env: ProcessEnv| {
        let binary = binary.clone();
        async move {
            match run_cli(&binary, args, &env, timeout).await {
                Ok(output) if output.code == 0 => Some(output.stdout),
                _ => None,
            }
        }
    };
    let status = read(&["status", "--json"], probe.clone())
        .await
        .and_then(|stdout| decode_status(&stdout))
        .unwrap_or_default();
    // `probeFxBackend`, for every backend at once: `FX_PROVIDER` points
    // `fx status` and `fx models` at it, and any `FX_MODEL` stops fx refusing a
    // backend without a saved model.
    let mut targets: Vec<(String, String, Option<&'static str>)> = BUILT_IN_BACKENDS
        .iter()
        .map(|(id, name, login)| ((*id).to_owned(), (*name).to_owned(), Some(*login)))
        .collect();
    targets.extend(
        configured_backends(env)
            .into_iter()
            .map(|id| (id.clone(), id, None)),
    );
    let backends: Vec<Backend> =
        futures_util::future::join_all(targets.into_iter().map(|(id, name, login)| {
            let mut env = probe.clone();
            env.insert("FX_PROVIDER".into(), id.clone());
            env.insert("FX_MODEL".into(), DEFAULT_MODEL_SLUG.into());
            let read = &read;
            async move {
                let status = read(&["status", "--json"], env.clone())
                    .await
                    .and_then(|stdout| decode_status(&stdout));
                let signed_in = match auth_of(status.as_ref().and_then(|s| s.auth.as_deref()))
                    ["status"]
                    .as_str()
                {
                    Some("authenticated") => Some(true),
                    Some("unauthenticated") => Some(false),
                    _ => None,
                };
                let models = if signed_in == Some(false) {
                    None
                } else {
                    read(&["models", "--json"], env)
                        .await
                        .and_then(|stdout| decode_models(&stdout))
                        .map(|ids| ids.iter().filter_map(|id| non_empty(Some(id))).collect())
                };
                let sign_in_hint = match login {
                    Some(login) => format!("Signed out. Run `{login}` on this machine."),
                    None => non_empty(status.as_ref().and_then(|s| s.auth_help.as_deref()))
                        .unwrap_or_else(|| "fx has no credential for this backend.".into()),
                };
                Backend {
                    id,
                    name,
                    signed_in,
                    sign_in_hint,
                    models,
                }
            }
        }))
        .await;
    let default_model = non_empty(status.model.as_deref());
    let default_backend_id = status_backend(status.model_source.as_deref());
    let default_backend = backends.iter().find(|b| b.id == default_backend_id);
    // A ready snapshot replaces the model list, so every backend that may be
    // signed in must have listed.
    let listed: Vec<&Backend> = backends
        .iter()
        .filter(|b| b.signed_in != Some(false))
        .collect();
    let listing_failed = listed.is_empty() || listed.iter().any(|b| b.models.is_none());
    let status_auth = auth_of(status.auth.as_deref());
    let auth = match backends.iter().find(|b| b.signed_in == Some(true)) {
        Some(signed_in) if status_auth["status"] != "authenticated" => {
            json!({"status": "authenticated", "label": signed_in.name})
        }
        _ => status_auth,
    };
    let signed_out_default;
    let (row_status, message) = match auth["status"].as_str() {
        Some("unauthenticated") => ("error", Some("Run `fx login` to sign in.")),
        Some("unknown") => (
            "warning",
            Some("fx is installed but login state could not be determined."),
        ),
        _ if default_backend.is_some_and(|b| b.signed_in == Some(false)) => {
            signed_out_default = format!(
                "fx's default backend, {}, is signed out. Pick a model on another backend or sign in.",
                default_backend.map(|b| b.name.as_str()).unwrap_or("")
            );
            ("warning", Some(signed_out_default.as_str()))
        }
        _ if listing_failed => (
            "warning",
            Some("fx is installed but model listing failed. Model options may be incomplete."),
        ),
        _ => ("ready", None),
    };
    let full_access = status
        .modes
        .as_ref()
        .is_some_and(|modes| modes.iter().any(|m| m == FULL_ACCESS_MODE_ID));
    snapshot(
        true,
        version,
        row_status,
        auth,
        message,
        default_model.as_deref(),
        &backends,
        full_access,
    )
}

// ---------------------------------------------------------------------------
// Maintenance

/// `makeFxMaintenance`: manual-only for a mise, Nix, MacPorts or non-fx
/// Homebrew install (and on macOS before Darwin 24, where fx after 0.0.13
/// cannot run), Homebrew's `fx-agent` formula through brew, and otherwise
/// `fx upgrade`, with fx's release channel for the latest version.
async fn maintenance(binary: &str, env: &ProcessEnv) -> maintenance::Capabilities {
    let lookup = || {
        Some(std::sync::Arc::new(maintenance::Lookup::FxRelease {
            url: LATEST_RELEASE_URL.into(),
        }))
    };
    let Some(context) = maintenance::context(binary, env) else {
        return maintenance::manual(None, None);
    };
    if cfg!(target_os = "macos") && darwin_major().is_some_and(|major| major < 24) {
        return maintenance::manual(None, None);
    }
    if maintenance::resolve_mise(None, None, &context)
        .await
        .is_some()
    {
        // fx has no mise registry tool, so a mise-owned install stays manual-only.
        return maintenance::Capabilities {
            latest: Some(None),
            lookup: lookup(),
            ..maintenance::manual(None, Some(&context.real_command_path))
        };
    }
    if [&context.resolved_command_path, &context.real_command_path]
        .iter()
        .any(|p| maintenance::is_nix_store(p))
    {
        return maintenance::manual(None, Some(&context.real_command_path));
    }
    if let Some(name) = maintenance::resolve_macports_owner(&context).await {
        return maintenance::Capabilities {
            manual_command: (!name.is_empty()).then(|| format!("sudo port upgrade {name}")),
            ..maintenance::manual(None, Some(&context.real_command_path))
        };
    }
    if let Some(homebrew) = maintenance::homebrew_ownership(&context.real_command_path) {
        if homebrew.cask || homebrew.name != "fx-agent" {
            return maintenance::manual(None, None);
        }
        return maintenance::resolve_homebrew(None, &homebrew, &context)
            .await
            .unwrap_or_else(|| maintenance::manual(None, None));
    }
    maintenance::Capabilities {
        update: Some(maintenance::Update {
            command: format!(
                "{} upgrade",
                maintenance::quote(&context.resolved_command_path)
            ),
            executable: context.resolved_command_path.clone(),
            args: vec!["upgrade".into()],
            lock_key: format!("fx:{}", context.real_command_path),
            env: Some(context.env.clone()),
        }),
        lookup: lookup(),
        ..Default::default()
    }
}

/// The macOS kernel's major release (`os.release()`).
fn darwin_major() -> Option<u32> {
    let output = std::process::Command::new("uname")
        .arg("-r")
        .output()
        .ok()?;
    String::from_utf8_lossy(&output.stdout)
        .trim()
        .split('.')
        .next()?
        .parse()
        .ok()
}
