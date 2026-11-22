//! fx's status check, as the reference's `checkFxProviderStatus`:
//! `fx --version`, then `fx status --json` (sign-in and default model) and
//! `fx models --json`. None of them opens an ACP session, which fx would save.
use super::{
    cli_binary, cli_version, models_from_settings, now_iso, probe_env, probe_timeout, run_cli,
    Draft, ProcessEnv, RunError, AUTH_PROBE_TIMEOUT_MS,
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

/// `fxModels`: "Default" (named after and aliased to the model `fx status`
/// reports), each listed model, then the custom models.
pub(crate) fn models(config: &Value, default_model: Option<&str>, listed: &[String]) -> Vec<Value> {
    let mut default = json!({
        "slug": DEFAULT_MODEL_SLUG,
        "name": match default_model { Some(model) => format!("Default ({model})"), None => "Default".into() },
    });
    if let Some(model) = default_model {
        default["aliases"] = json!([model]);
    }
    default["isCustom"] = json!(false);
    default["isDefault"] = json!(true);
    default["capabilities"] = capabilities();
    let mut built_in = vec![default];
    for (index, id) in listed.iter().enumerate() {
        if id != DEFAULT_MODEL_SLUG && listed.iter().position(|other| other == id) == Some(index) {
            built_in.push(
                json!({"slug": id, "name": id, "isCustom": false, "capabilities": capabilities()}),
            );
        }
    }
    models_from_settings(built_in, &config["customModels"], capabilities())
}

fn non_empty(value: Option<&str>) -> Option<String> {
    value
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .map(str::to_owned)
}

/// `FxStatus`: an object whose `auth` and `model`, when present, are strings.
fn decode_status(stdout: &str) -> Option<(Option<String>, Option<String>)> {
    let value: Value = serde_json::from_str(stdout).ok()?;
    let object = value.as_object()?;
    let field = |key: &str| match object.get(key) {
        None => Some(None),
        Some(Value::String(text)) => Some(Some(text.clone())),
        Some(_) => None,
    };
    Some((field("auth")?, field("model")?))
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

pub(super) async fn check(config: &Value, env: &ProcessEnv) -> Draft {
    let checked_at = now_iso();
    let probe = probe_env(env);
    let binary = cli_binary(config, "fx");
    let snapshot = |installed: bool,
                    version: Option<String>,
                    status: &'static str,
                    auth: Value,
                    message: Option<&str>,
                    default_model: Option<&str>,
                    listed: &[String]| Draft {
        installed,
        version,
        status,
        auth,
        checked_at: checked_at.clone(),
        message: message.map(str::to_owned),
        models: models(config, default_model, listed),
        extra: vec![("supportsTextGeneration", json!(false))],
        version_advisory: true,
        ..Default::default()
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
        );
    }
    let read = |args: &'static [&'static str]| {
        let binary = binary.clone();
        let probe = probe.clone();
        let timeout = probe_timeout("fx", AUTH_PROBE_TIMEOUT_MS, env);
        async move {
            match run_cli(&binary, args, &probe, timeout).await {
                Ok(output) if output.code == 0 => Some(output.stdout),
                _ => None,
            }
        }
    };
    let status = read(&["status", "--json"])
        .await
        .and_then(|stdout| decode_status(&stdout));
    let listed = read(&["models", "--json"])
        .await
        .and_then(|stdout| decode_models(&stdout))
        .map(|ids| {
            ids.iter()
                .filter_map(|id| non_empty(Some(id)))
                .collect::<Vec<_>>()
        });
    let (auth_method, default_model) = status.unwrap_or_default();
    let auth = match non_empty(auth_method.as_deref()) {
        None => json!({"status": "unknown"}),
        Some(method) if method == "missing" => json!({"status": "unauthenticated"}),
        Some(method) => json!({"status": "authenticated", "label": method}),
    };
    let default_model = non_empty(default_model.as_deref());
    let (status, message) = match auth["status"].as_str() {
        Some("unauthenticated") => ("error", Some("Run `fx login` to sign in.")),
        Some("unknown") => (
            "warning",
            Some("fx is installed but login state could not be determined."),
        ),
        _ if listed.is_none() => (
            "warning",
            Some("fx is installed but model listing failed. Model options may be incomplete."),
        ),
        _ => ("ready", None),
    };
    snapshot(
        true,
        version,
        status,
        auth,
        message,
        default_model.as_deref(),
        listed.as_deref().unwrap_or(&[]),
    )
}
