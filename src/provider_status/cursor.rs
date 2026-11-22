//! Cursor's status check, as the reference's `checkCursorProviderStatus`.
//! Cursor has no CLI to probe: the check reads the account and its models
//! from Cursor's HTTP API (`GET /v1/me` and `/v1/models` on
//! `CURSOR_BACKEND_URL`, `https://api.cursor.com` by default) with the
//! instance's `CURSOR_API_KEY` or the key a browser sign-in stored in the
//! server's secret store, then the plan's usage
//! (`DashboardService/GetCurrentPeriodUsage`).
use super::{
    compact_slash_command, models_from_settings, now_iso, probe_timeout, unavailable_usage, Draft,
    ProcessEnv,
};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::path::Path;
use std::time::Duration;

const CATALOG_TIMEOUT_MS: u64 = 15_000;
const DEFAULT_BACKEND_URL: &str = "https://api.cursor.com";
const DEFAULT_API_ENDPOINT: &str = "https://api2.cursor.sh";
const USAGE_WINDOWS: [(&str, &str); 3] = [
    ("totalPercentUsed", "Overall"),
    ("autoPercentUsed", "Cursor Models"),
    ("apiPercentUsed", "Other Models"),
];

fn empty_capabilities() -> Value {
    json!({"optionDescriptors": []})
}

/// `getCursorFallbackModels`: the custom models only.
pub(crate) fn fallback_models(config: &Value) -> Vec<Value> {
    models_from_settings(vec![], &config["customModels"], empty_capabilities())
}

/// The instance's own `CURSOR_API_KEY` (`auth.usesApiKey`).
pub(crate) fn configured_key(env: &ProcessEnv) -> Option<String> {
    env.get("CURSOR_API_KEY")
        .map(|k| k.trim().to_owned())
        .filter(|k| !k.is_empty())
}

/// `ProviderCredentialStore`'s secret name for `(driver, binding)`.
fn credential_key(driver: &str, binding: &str) -> String {
    let digest = Sha256::digest(format!("{}:{driver}{binding}", driver.len()).as_bytes());
    format!("provider-auth-{}", hex::encode(digest))
}

/// The stored sign-in (`Credentials`), when it decodes.
fn decode_credentials(text: &str) -> Option<Value> {
    let value: Value = serde_json::from_str(text).ok()?;
    let finite = |v: &Value| v.as_f64().is_some_and(f64::is_finite);
    let ok = value["version"] == 1
        && value["backendUrl"].is_string()
        && value["apiKey"].is_string()
        && finite(&value["createdAtMs"])
        && value.get("apiKeyExpiresAtMs").is_none_or(finite)
        && value.get("email").is_none_or(Value::is_string);
    ok.then_some(value)
}

/// `makeCursorCredentialStore`'s migration of the legacy
/// `provider-auth/<instance>/cursor.json` into the secret store.
fn migrate_legacy(state_dir: &Path, secret: &Path, instance_id: &str) {
    let legacy = state_dir
        .join("provider-auth")
        .join(percent_encoding::utf8_percent_encode(instance_id, URI_COMPONENT).to_string())
        .join("cursor.json");
    let Ok(text) = std::fs::read_to_string(&legacy) else {
        return;
    };
    let stored_valid = std::fs::read_to_string(secret)
        .ok()
        .and_then(|t| decode_credentials(&t))
        .is_some();
    if !stored_valid && decode_credentials(&text).is_some() {
        if let Some(parent) = secret.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        let temporary = secret.with_extension("bin.tmp");
        if std::fs::write(&temporary, &text).is_err()
            || std::fs::rename(&temporary, secret).is_err()
        {
            let _ = std::fs::remove_file(&temporary);
            return;
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let _ = std::fs::set_permissions(secret, std::fs::Permissions::from_mode(0o600));
        }
    }
    let _ = std::fs::remove_file(&legacy);
}

/// `encodeURIComponent`'s unreserved set.
const URI_COMPONENT: &percent_encoding::AsciiSet = &percent_encoding::NON_ALPHANUMERIC
    .remove(b'-')
    .remove(b'_')
    .remove(b'.')
    .remove(b'!')
    .remove(b'~')
    .remove(b'*')
    .remove(b'\'')
    .remove(b'(')
    .remove(b')');

/// `auth.readApiKey`: the configured key, else an unexpired stored one.
fn read_api_key(
    env: &ProcessEnv,
    secrets: &Path,
    state_dir: &Path,
    instance_id: &str,
) -> Option<String> {
    if let Some(key) = configured_key(env) {
        return Some(key);
    }
    let secret = secrets.join(format!("{}.bin", credential_key("cursor", instance_id)));
    migrate_legacy(state_dir, &secret, instance_id);
    let credentials = decode_credentials(&std::fs::read_to_string(secret).ok()?)?;
    let now = crate::platform::now_ms() as f64;
    match credentials["apiKeyExpiresAtMs"].as_f64() {
        Some(expires) if expires <= now => None,
        _ => credentials["apiKey"].as_str().map(str::to_owned),
    }
}

fn client() -> reqwest::Client {
    super::http_client()
}

/// `CursorSdkCatalogError`.
struct CatalogError {
    authentication_failure: bool,
    status: Option<u16>,
    code: Option<String>,
}

impl CatalogError {
    fn other() -> Self {
        Self {
            authentication_failure: false,
            status: None,
            code: None,
        }
    }

    fn plan_required(&self) -> bool {
        self.code.as_deref() == Some("plan_required")
    }

    fn account_blocked(&self) -> bool {
        self.status == Some(403) && !self.plan_required()
    }
}

async fn catalog_get(base: &str, path: &str, api_key: &str) -> Result<Value, CatalogError> {
    let response = client()
        .get(format!("{base}{path}"))
        .bearer_auth(api_key)
        .send()
        .await
        .map_err(|_| CatalogError::other())?;
    let status = response.status().as_u16();
    if !(200..300).contains(&status) {
        let body: Option<Value> = response.json().await.ok();
        let error = body.as_ref().and_then(|b| {
            let api_error = |e: &Value| {
                (e.is_object()
                    && e.get("code").is_none_or(Value::is_string)
                    && e.get("message").is_none_or(Value::is_string))
                .then(|| e.clone())
            };
            if b["error"].is_object() {
                api_error(&b["error"])
            } else {
                api_error(b)
            }
        });
        return Err(CatalogError {
            authentication_failure: status == 401,
            status: Some(status),
            code: error.and_then(|e| e["code"].as_str().map(str::to_owned)),
        });
    }
    response.json().await.map_err(|_| CatalogError::other())
}

fn optional<F: Fn(&Value) -> bool>(value: &Value, key: &str, check: F) -> bool {
    value.get(key).is_none_or(check)
}

/// `CatalogUser`.
fn valid_user(user: &Value) -> bool {
    user["apiKeyName"].is_string()
        && user["createdAt"].is_string()
        && optional(user, "userId", Value::is_number)
        && ["userEmail", "userFirstName", "userLastName"]
            .iter()
            .all(|k| optional(user, k, Value::is_string))
}

/// `CatalogModels`.
fn valid_models(body: &Value) -> Option<Vec<Value>> {
    let items = body["items"].as_array()?;
    let strings = |v: &Value| v.as_array().is_some_and(|a| a.iter().all(Value::is_string));
    for model in items {
        let valid = model["id"].is_string()
            && model["displayName"].is_string()
            && optional(model, "description", Value::is_string)
            && optional(model, "aliases", strings)
            && optional(model, "parameters", |p| {
                p.as_array().is_some_and(|ps| {
                    ps.iter().all(|p| {
                        p["id"].is_string()
                            && optional(p, "displayName", Value::is_string)
                            && p["values"].as_array().is_some_and(|vs| {
                                vs.iter().all(|v| {
                                    v["value"].is_string()
                                        && optional(v, "displayName", Value::is_string)
                                })
                            })
                    })
                })
            })
            && optional(model, "variants", |vs| {
                vs.as_array().is_some_and(|vs| {
                    vs.iter().all(|v| {
                        v["displayName"].is_string()
                            && v["params"].as_array().is_some_and(|ps| {
                                ps.iter()
                                    .all(|p| p["id"].is_string() && p["value"].is_string())
                            })
                            && optional(v, "description", Value::is_string)
                            && optional(v, "isDefault", Value::is_boolean)
                            && optional(v, "isMaxMode", Value::is_boolean)
                    })
                })
            });
        if !valid {
            return None;
        }
    }
    Some(items.clone())
}

/// `CursorSdkCatalog.read`: the account (unless the team blocks it) and the
/// models, read side by side; either failure fails the read.
async fn read_catalog(
    base: &str,
    api_key: &str,
) -> Result<(Option<Value>, Vec<Value>), CatalogError> {
    let user = async {
        match catalog_get(base, "/v1/me", api_key).await {
            Ok(user) if valid_user(&user) => Ok(Some(user)),
            Ok(_) => Err(CatalogError::other()),
            Err(error) if error.account_blocked() => Ok(None),
            Err(error) => Err(error),
        }
    };
    let models = async {
        let body = catalog_get(base, "/v1/models", api_key).await?;
        valid_models(&body).ok_or_else(CatalogError::other)
    };
    let (user, models) = tokio::join!(user, models);
    Ok((user?, models?))
}

fn title_case_words(value: &str) -> String {
    regex::Regex::new(r"[\s_-]+")
        .unwrap()
        .split(value)
        .filter(|p| !p.is_empty())
        .map(|part| {
            let mut chars = part.chars();
            let first = chars
                .next()
                .map(|c| c.to_uppercase().to_string())
                .unwrap_or_default();
            first + &chars.as_str().to_lowercase()
        })
        .collect::<Vec<_>>()
        .join(" ")
}

fn parameter_priority(id: &str) -> u8 {
    match id {
        "effort" | "reasoning" => 0,
        "context" => 1,
        "fast" => 2,
        "thinking" => 3,
        _ => 4,
    }
}

fn option_id(id: &str) -> &str {
    match id {
        "context" => "contextWindow",
        "fast" => "fastMode",
        other => other,
    }
}

/// `buildCursorCapabilitiesFromSdkModel`.
fn model_capabilities(model: &Value) -> Value {
    let mut parameters: Vec<&Value> = model["parameters"]
        .as_array()
        .into_iter()
        .flatten()
        .collect();
    parameters.sort_by_key(|p| parameter_priority(p["id"].as_str().unwrap_or("")));
    let default_value = |native: &str| {
        model["variants"]
            .as_array()?
            .iter()
            .find(|v| v["isDefault"] == true)?["params"]
            .as_array()?
            .iter()
            .find(|p| p["id"] == native)?["value"]
            .as_str()
            .map(str::to_owned)
    };
    let mut seen: Vec<String> = Vec::new();
    let mut descriptors = Vec::new();
    for parameter in parameters {
        let native = parameter["id"].as_str().unwrap_or("").trim();
        let id = option_id(native);
        if native.is_empty() || id.is_empty() || seen.iter().any(|s| s == id) {
            continue;
        }
        seen.push(id.to_owned());
        let values: Vec<(String, String)> = parameter["values"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|entry| {
                let value = entry["value"].as_str()?.trim();
                if value.is_empty() {
                    return None;
                }
                let label = entry["displayName"]
                    .as_str()
                    .map(str::trim)
                    .filter(|l| !l.is_empty())
                    .unwrap_or(value);
                Some((value.to_owned(), label.to_owned()))
            })
            .collect();
        if values.is_empty() {
            continue;
        }
        let label = parameter["displayName"]
            .as_str()
            .map(str::trim)
            .filter(|l| !l.is_empty())
            .map(str::to_owned)
            .unwrap_or_else(|| title_case_words(id));
        let default = default_value(native);
        let lowered: Vec<String> = values.iter().map(|(v, _)| v.to_lowercase()).collect();
        if values.len() == 2
            && lowered.contains(&"true".into())
            && lowered.contains(&"false".into())
        {
            let mut descriptor = json!({"id": id, "label": label, "type": "boolean"});
            if let Some(d) = default.as_deref().filter(|d| *d == "true" || *d == "false") {
                descriptor["currentValue"] = json!(d == "true");
            }
            descriptors.push(descriptor);
            continue;
        }
        let options: Vec<(String, String, Option<String>, bool)> = values
            .into_iter()
            .map(|(value, label)| {
                let is_default = Some(&value) == default.as_ref();
                (value, label, None, is_default)
            })
            .collect();
        descriptors.push(super::select_descriptor(id, &label, &options));
    }
    json!({"optionDescriptors": descriptors})
}

/// `buildCursorDiscoveredModelsFromSdk`.
fn discovered_models(models: &[Value]) -> Vec<Value> {
    let mut seen: Vec<String> = Vec::new();
    let mut discovered = Vec::new();
    for model in models {
        let slug = model["id"].as_str().unwrap_or("").trim();
        let name = model["displayName"].as_str().unwrap_or("").trim();
        if slug.is_empty() || name.is_empty() || seen.iter().any(|s| s == slug) {
            continue;
        }
        seen.push(slug.to_owned());
        let aliases: Vec<&str> = model["aliases"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(Value::as_str)
            .map(str::trim)
            .filter(|a| !a.is_empty())
            .collect();
        let mut entry = json!({"slug": slug, "name": name});
        if !aliases.is_empty() {
            entry["aliases"] = json!(aliases);
        }
        entry["isCustom"] = json!(false);
        entry["capabilities"] = model_capabilities(model);
        discovered.push(entry);
    }
    discovered
}

/// JavaScript's `Number(value)` for a string or number field.
fn js_number(value: &Value) -> f64 {
    match value {
        Value::Number(n) => n.as_f64().unwrap_or(f64::NAN),
        Value::String(s) if s.trim().is_empty() => 0.0,
        Value::String(s) => s.trim().parse().unwrap_or(f64::NAN),
        Value::Null => 0.0,
        _ => f64::NAN,
    }
}

/// `cursorUsageResponseToLimits`.
fn usage_to_limits(response: &Value, checked_at: &str) -> Value {
    let end = js_number(response.get("billingCycleEnd").unwrap_or(&Value::Null));
    let start = js_number(response.get("billingCycleStart").unwrap_or(&Value::Null));
    let resets_at = (end > 0.0 && end.is_finite() && end.abs() <= 8.64e15)
        .then(|| crate::auth_service::iso(end as i64));
    let duration = if resets_at.is_some() && start > 0.0 {
        ((end - start) / 60_000.0).round() as i64
    } else {
        0
    };
    let mut windows = Vec::new();
    if response["planUsage"].is_object() {
        for (id, label) in USAGE_WINDOWS {
            let Some(used) = response["planUsage"][id].as_f64().filter(|u| u.is_finite()) else {
                continue;
            };
            let mut window = json!({"id": id, "kind": "monthly", "label": label,
                                    "usedPercent": super::js_number(super::clamp_percent(used))});
            if let Some(reset) = &resets_at {
                window["resetsAt"] = json!(reset);
            }
            if duration > 0 {
                window["windowDurationMins"] = json!(duration);
            }
            windows.push(window);
        }
    }
    if windows.is_empty() {
        unavailable_usage(checked_at, "unsupported", None)
    } else {
        super::usage_limits(checked_at, windows)
    }
}

/// `CursorUsageResponse`.
fn valid_usage(body: &Value) -> bool {
    let time = |v: &Value| v.is_string() || v.is_number();
    body.is_object()
        && optional(body, "billingCycleStart", time)
        && optional(body, "billingCycleEnd", time)
        && optional(body, "planUsage", |p| {
            p.is_object()
                && ["totalPercentUsed", "autoPercentUsed", "apiPercentUsed"]
                    .iter()
                    .all(|k| optional(p, k, Value::is_number))
        })
}

/// `readCursorUsageLimits`.
async fn read_usage(config: &Value, env: &ProcessEnv, api_key: &str) -> Value {
    let checked_at = now_iso();
    let read = async {
        let endpoint = config["apiEndpoint"]
            .as_str()
            .map(str::trim)
            .filter(|e| !e.is_empty())
            .or_else(|| {
                env.get("CURSOR_API_ENDPOINT")
                    .map(|e| e.trim())
                    .filter(|e| !e.is_empty())
            })
            .unwrap_or(DEFAULT_API_ENDPOINT);
        let endpoint = endpoint.strip_suffix('/').unwrap_or(endpoint).to_owned();
        let mut token = env
            .get("CURSOR_AUTH_TOKEN")
            .map(|t| t.trim().to_owned())
            .filter(|t| !t.is_empty());
        let api_key = api_key.trim();
        if token.is_none() && !api_key.is_empty() {
            if endpoint != DEFAULT_API_ENDPOINT {
                return Some(unavailable_usage(
                    &checked_at,
                    "unsupported",
                    Some("Cursor usage for an API key requires the default Cursor endpoint."),
                ));
            }
            token = Some(exchange_api_key(api_key).await?);
        }
        let Some(token) = token else {
            return Some(unavailable_usage(&checked_at, "unsupported", None));
        };
        let response = client()
            .post(format!(
                "{endpoint}/aiserver.v1.DashboardService/GetCurrentPeriodUsage"
            ))
            .bearer_auth(token)
            .header("connect-protocol-version", "1")
            .header("x-cursor-client-type", "cli")
            .json(&json!({}))
            .send()
            .await
            .ok()?;
        if !response.status().is_success() {
            return None;
        }
        let body: Value = response.json().await.ok()?;
        valid_usage(&body).then(|| usage_to_limits(&body, &checked_at))
    };
    tokio::time::timeout(Duration::from_secs(10), read)
        .await
        .ok()
        .flatten()
        .unwrap_or_else(|| {
            unavailable_usage(
                &checked_at,
                "probeFailed",
                Some("Cursor could not read usage limits."),
            )
        })
}

/// `exchangeCursorApiKey`: the access token Cursor's RPCs accept for a key.
async fn exchange_api_key(api_key: &str) -> Option<String> {
    let response = client()
        .post(format!("{DEFAULT_API_ENDPOINT}/auth/exchange_user_api_key"))
        .bearer_auth(api_key)
        .json(&json!({}))
        .send()
        .await
        .ok()?;
    if !response.status().is_success() {
        return None;
    }
    let body: Value = response.json().await.ok()?;
    body["accessToken"]
        .as_str()
        .filter(|t| !t.is_empty())
        .map(str::to_owned)
}

pub(super) async fn check(
    config: &Value,
    env: &ProcessEnv,
    instance_id: &str,
    secrets: &Path,
    state_dir: &Path,
) -> Draft {
    let checked_at = now_iso();
    let uses_api_key = configured_key(env).is_some();
    let browser = !uses_api_key;
    let fallback = fallback_models(config);
    let setup = vec![(
        "setup",
        json!({"canAuthenticate": browser, "canInstall": false}),
    )];
    let failed = |auth: &str, message: String| Draft {
        installed: true,
        status: "error",
        auth: json!({"status": auth, "canLogout": browser}),
        checked_at: checked_at.clone(),
        message: Some(message),
        models: fallback.clone(),
        extra: setup.clone(),
        ..Default::default()
    };
    let Some(api_key) = read_api_key(env, secrets, state_dir, instance_id) else {
        return failed(
            "unauthenticated",
            "Sign in with Cursor or add CURSOR_API_KEY in provider settings.".into(),
        );
    };
    let base = std::env::var("CURSOR_BACKEND_URL").unwrap_or_else(|_| DEFAULT_BACKEND_URL.into());
    let base = base.strip_suffix('/').unwrap_or(&base).to_owned();
    let timeout = probe_timeout("cursor", CATALOG_TIMEOUT_MS, env);
    // One slow read is retried before the check reports a timeout.
    let mut result = tokio::time::timeout(timeout, read_catalog(&base, &api_key)).await;
    if result.is_err() {
        result = tokio::time::timeout(timeout, read_catalog(&base, &api_key)).await;
    }
    let (user, models) = match result {
        Err(_) => {
            return failed(
                "unknown",
                format!(
                    "Cursor SDK catalog request timed out after {}ms.",
                    timeout.as_millis()
                ),
            )
        }
        Ok(Err(error)) => {
            let message = if error.authentication_failure {
                if browser {
                    "Cursor sign-in expired or was rejected. Sign in again in provider settings."
                } else {
                    "Cursor SDK authentication failed. Check CURSOR_API_KEY."
                }
            } else if error.plan_required() {
                if browser {
                    "Cursor needs a paid plan to run in T3 Code. Upgrade this account or sign in with a different one."
                } else {
                    "Cursor needs a paid plan to run in T3 Code. Upgrade the account that owns CURSOR_API_KEY or use a key from a paid account."
                }
            } else {
                "Cursor SDK catalog request failed. Check server logs for details."
            };
            return failed(
                if error.authentication_failure {
                    "unauthenticated"
                } else {
                    "unknown"
                },
                message.into(),
            );
        }
        Ok(Ok(catalog)) => catalog,
    };
    let discovered = discovered_models(&models);
    let email = user
        .as_ref()
        .and_then(|u| u["userEmail"].as_str())
        .map(str::trim)
        .filter(|e| !e.is_empty());
    let key_name = user
        .as_ref()
        .and_then(|u| u["apiKeyName"].as_str())
        .map(str::trim)
        .filter(|n| !n.is_empty());
    let label = if browser {
        "Cursor account".to_owned()
    } else {
        match key_name {
            Some(name) => format!("Cursor API key ({name})"),
            None => "Cursor API key".to_owned(),
        }
    };
    let mut auth = json!({"status": "authenticated", "type": if browser { "browser" } else { "api-key" },
                          "label": label, "canLogout": browser});
    if let Some(email) = email {
        auth["email"] = json!(email);
    }
    let warning = discovered
        .is_empty()
        .then_some("Cursor SDK model discovery returned no built-in models.");
    let usage = read_usage(config, env, &api_key).await;
    Draft {
        installed: true,
        status: if warning.is_some() {
            "warning"
        } else {
            "ready"
        },
        auth,
        checked_at,
        message: warning.map(str::to_owned),
        models: models_from_settings(discovered, &config["customModels"], empty_capabilities()),
        slash_commands: vec![compact_slash_command()],
        usage_limits: Some(usage),
        extra: setup,
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn credential_keys_hash_the_driver_and_binding() {
        assert_eq!(
            credential_key("cursor", "cursor"),
            format!(
                "provider-auth-{}",
                hex::encode(Sha256::digest(b"6:cursorcursor"))
            )
        );
    }

    #[test]
    fn booleans_and_selects_from_parameters() {
        let caps = model_capabilities(&json!({
            "parameters": [
                {"id": "fast", "values": [{"value": "true"}, {"value": "false"}]},
                {"id": "effort", "displayName": " Effort ", "values": [{"value": "low"}, {"value": "high", "displayName": "High"}, {"value": " "}]}],
            "variants": [{"displayName": "d", "isDefault": true, "params": [{"id": "fast", "value": "false"}, {"id": "effort", "value": "high"}]}]}));
        let d = &caps["optionDescriptors"];
        assert_eq!(d[0]["id"], "effort");
        assert_eq!(d[0]["currentValue"], "high");
        assert_eq!(
            d[1],
            json!({"id": "fastMode", "label": "Fastmode", "type": "boolean", "currentValue": false})
        );
    }

    #[test]
    fn usage_windows_from_the_dashboard() {
        let limits = usage_to_limits(
            &json!({"billingCycleStart": "1764547200000", "billingCycleEnd": 1767225600000_i64,
                    "planUsage": {"totalPercentUsed": 42.5, "apiPercentUsed": 120}}),
            "now",
        );
        let windows = limits["windows"].as_array().unwrap();
        assert_eq!(windows.len(), 2);
        assert_eq!(windows[0]["id"], "apiPercentUsed");
        assert_eq!(windows[0]["usedPercent"], 100);
        assert_eq!(windows[1]["windowDurationMins"], 44640);
    }
}
