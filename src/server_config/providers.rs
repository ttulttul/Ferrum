use super::DEFAULTS;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::path::PathBuf;

fn continuation(id: &str, driver: &str, config: &Value) -> String {
    let (kind, var, folder) = match driver {
        "codex" => ("codex", "CODEX_HOME", ".codex"),
        "claudeAgent" => ("claude", "CLAUDE_CONFIG_DIR", ".claude"),
        _ => return format!("{driver}:instance:{id}"),
    };
    let home = std::env::var_os("HOME")
        .or_else(|| std::env::var_os("USERPROFILE"))
        .map(PathBuf::from)
        .unwrap_or_default();
    // `resolveClaudeHomePath` and `resolveCodexHomeLayout`: a configured home
    // with `~` expanded, made absolute; Claude then falls back to
    // CLAUDE_CONFIG_DIR, Codex straight to ~/.codex.
    let configured = config["homePath"]
        .as_str()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(|path| match path.strip_prefix("~") {
            Some("") => home.clone(),
            Some(rest) if rest.starts_with('/') => home.join(&rest[1..]),
            _ => PathBuf::from(path),
        });
    let inherited = || {
        (driver == "claudeAgent")
            .then(|| std::env::var(var).ok())
            .flatten()
            .map(|v| v.trim().to_owned())
            .filter(|v| !v.is_empty())
            .map(PathBuf::from)
    };
    let path = configured
        .or_else(inherited)
        .unwrap_or_else(|| home.join(folder));
    let path = if path.is_absolute() {
        path
    } else {
        std::env::current_dir().unwrap_or_default().join(path)
    };
    format!("{kind}:home:{}", path.display())
}

fn models(stock: &Value, config: &Value) -> Value {
    let mut rows = stock.as_array().cloned().unwrap_or_default();
    for entry in config["customModels"].as_array().into_iter().flatten() {
        let slug = entry
            .as_str()
            .or_else(|| entry["slug"].as_str())
            .unwrap_or("")
            .trim();
        if slug.is_empty() {
            continue;
        }
        let name = entry["name"]
            .as_str()
            .filter(|n| !n.trim().is_empty())
            .unwrap_or(slug);
        if let Some(row) = rows.iter_mut().find(|v| v["slug"] == slug) {
            if name != slug {
                row["name"] = json!(name);
                row.as_object_mut().unwrap().shift_remove("shortName");
            }
        } else {
            rows.push(json!({"slug":slug,"name":name,"isCustom":true,"capabilities":entry["capabilities"]}));
        }
    }
    rows.into()
}

#[cfg(feature = "native")]
pub(crate) fn list(state: &crate::AppState, settings: &Value, options: &Value) -> Value {
    let denial = |driver: &str| state.settings.policy.provider_denial(driver);
    // Checks key on the effective settings, whose secrets a client's view
    // redacts.
    let effective = state.settings.internal().ok();
    let effective = effective.as_ref().unwrap_or(settings);
    list_with(
        &state.config_started_at,
        denial,
        |_| None,
        settings,
        options,
        &state.model_manifest.current(),
        |id, row| {
            let driver = row["driver"].as_str().unwrap_or("").to_owned();
            state
                .provider_status
                .overlay(id, &fingerprint(effective, id, denial(&driver)), row)
        },
    )
}

/// The reference's initial snapshot of an enabled instance of `driver`
/// whose status check has not reported yet: whether it is shown installed,
/// and its message. Codex and Claude keep the generic placeholder.
fn initial_status(driver: &str) -> Option<(bool, &'static str)> {
    match driver {
        "fx" => Some((true, "Checking fx CLI availability...")),
        "kiro" => Some((true, "Checking Kiro CLI availability...")),
        "grok" => Some((true, "Checking Grok CLI availability...")),
        "pi" => Some((true, "Checking Pi CLI availability...")),
        "cursor" => Some((true, "Checking Cursor SDK availability...")),
        _ => None,
    }
}

/// What an instance's status check depends on: its settings entry, the
/// managed policy's verdict and `enableProviderUpdateChecks`. A check's
/// result is shown only while these are unchanged, as the reference
/// rebuilds an instance (and its snapshot) when its settings change.
pub(crate) fn fingerprint(settings: &Value, id: &str, denial: Option<String>) -> Value {
    json!({
        "entry": settings["providerInstances"][id],
        "denied": denial,
        "updateChecks": settings["enableProviderUpdateChecks"] != false,
    })
}

/// The provider rows of the config snapshot. `denial` is the managed policy's
/// reason a driver is disallowed, if any. Claude's built-in models come from
/// the model `manifest`; a Codex instance's from `listed`, the models its CLI
/// listed (`model/list`), with the manifest's Codex catalog applied.
/// `overlay` replaces an instance's placeholder row with what its status
/// check found.
pub(crate) fn list_with(
    started_at: &str,
    denial: impl Fn(&str) -> Option<String>,
    listed: impl Fn(&str) -> Option<Vec<Value>>,
    settings: &Value,
    options: &Value,
    manifest: &Value,
    overlay: impl Fn(&str, Value) -> Value,
) -> Value {
    let claude_models: Value =
        crate::model_manifest::claude_models(manifest, crate::platform::now_ms()).into();
    let stock = DEFAULTS["providers"].as_object().unwrap();
    let instances = settings["providerInstances"].as_object();
    let ids = stock.keys().chain(
        instances
            .into_iter()
            .flatten()
            .filter(|(id, _)| !stock.contains_key(*id))
            .map(|(id, _)| id),
    );
    let mut providers = Vec::new();
    for id in ids {
        let entry = &settings["providerInstances"][id];
        let driver = entry["driver"].as_str().unwrap_or(id);
        let Some(catalog) = stock.get(driver) else {
            continue;
        };
        let config = &entry["config"];
        let label = catalog["displayName"].as_str().unwrap_or(driver);
        let denied = denial(driver);
        let enabled = entry["enabled"]
            .as_bool()
            .or(config["enabled"].as_bool())
            .unwrap_or(matches!(driver, "codex" | "claudeAgent"))
            && denied.is_none();
        let mut row = json!({"instanceId":id,"driver":driver,"displayName":entry["displayName"].as_str().unwrap_or(label)});
        if let Some(color) = entry.get("accentColor") {
            row["accentColor"] = color.clone();
        }
        row["continuation"] = json!({"groupKey":continuation(id,driver,config)});
        for key in [
            "showInteractionModeToggle",
            "reportsContextWindow",
            "supportedRuntimeModes",
            "supportsConversationRollback",
            "supportsTextGeneration",
            "setup",
        ] {
            if let Some(v) = catalog.get(key) {
                row[key] = v.clone();
            }
        }
        row["enabled"] = enabled.into();
        row["installed"] = false.into();
        row["version"] = Value::Null;
        row["status"] = json!(if enabled { "warning" } else { "disabled" });
        row["auth"] = match driver {
            "cursor" => json!({"status":"unknown","canLogout":true}),
            "antigravity" => json!({"status":"unknown","type":"oauth-personal"}),
            _ => json!({"status":"unknown"}),
        };
        row["checkedAt"] = json!(started_at);
        row["message"] = json!(denied.clone().unwrap_or_else(|| if enabled {
            format!("{label} provider status has not been checked in this session yet.")
        } else if driver == "opencode"
            && config["serverUrl"]
                .as_str()
                .is_some_and(|u| !u.trim().is_empty())
        {
            format!("{label} is disabled in T3 Code settings. A server URL is configured.")
        } else {
            format!("{label} is disabled in T3 Code settings.")
        }));
        let listed_models = (driver == "codex")
            .then(|| listed(id))
            .flatten()
            .map(|listed| Value::from(crate::codex_models::with_catalog(listed, manifest)));
        let stock_models = if driver == "claudeAgent" {
            &claude_models
        } else if let Some(listed) = &listed_models {
            listed
        } else {
            &catalog["models"]
        };
        row["models"] = models(stock_models, config);
        row["slashCommands"] = catalog["slashCommands"].clone();
        row["skills"] = catalog["skills"].clone();
        if let (true, None, Some((installed, message))) = (enabled, &denied, initial_status(driver))
        {
            // The driver's own snapshot until its first check reports.
            row["installed"] = installed.into();
            row["message"] = json!(message);
        }
        if driver == "cursor" {
            // `stampSnapshot`: a configured CURSOR_API_KEY (the instance's own,
            // else the server's) rules out a browser sign-in.
            let key = entry["environment"]
                .as_array()
                .into_iter()
                .flatten()
                .find(|v| v["name"] == "CURSOR_API_KEY")
                .map(|v| v["value"].as_str().unwrap_or("").to_owned())
                .or_else(|| std::env::var("CURSOR_API_KEY").ok());
            let uses_api_key = key.is_some_and(|k| !k.trim().is_empty());
            row["setup"] = json!({"canAuthenticate": !uses_api_key, "canInstall": false});
            row["auth"]["canLogout"] = json!(!uses_api_key);
            if enabled && denied.is_none() {
                row["slashCommands"] = json!([{"name": "compact", "description": "Summarize the conversation and reduce context usage"}]);
            }
        }
        if matches!(driver, "fx" | "kiro") {
            row["versionAdvisory"] = json!({"status":"unknown","currentVersion":null,"latestVersion":null,"updateCommand":null,"canUpdate":false,"canInstallVersion":false,"checkedAt":started_at,"message":null});
        }
        let mut row = overlay(id, row);
        if driver == "antigravity" && options["workspaceSnapshotsOnDemand"] != true {
            row["workspaceSnapshots"] = json!([]);
        }
        if driver == "opencode" && row.get("usageLimits").is_none() {
            row["usageLimits"] =
                json!({"checkedAt":started_at,"windows":[],"unavailable":{"reason":"unsupported"}});
        }
        if options["workspaceSnapshotsOnDemand"] == true
            && options["providerCatalogsOnDemand"] == true
            && !matches!(driver, "kiro" | "muse")
        {
            if options["providerCatalogDigests"] == true {
                let catalogs = json!([row["slashCommands"], row["skills"]]);
                row["catalogDigest"] =
                    json!(&format!("{:x}", Sha256::digest(catalogs.to_string()))[..16]);
                row["slashCommands"] = json!([]);
                row["skills"] = json!([]);
            } else {
                for cmd in row["slashCommands"].as_array_mut().into_iter().flatten() {
                    *cmd = json!({"name":cmd["name"]});
                }
                for skill in row["skills"].as_array_mut().into_iter().flatten() {
                    if let Some(obj) = skill.as_object_mut() {
                        obj.shift_remove("shortDescription");
                        obj.shift_remove("description");
                    }
                }
            }
        }
        providers.push(row);
    }
    providers.sort_by_cached_key(|p| {
        let driver = p["driver"].as_str().unwrap_or("");
        (
            stock
                .keys()
                .position(|key| key == driver)
                .unwrap_or(usize::MAX),
            p["displayName"].as_str().unwrap_or("").to_lowercase(),
            p["instanceId"].as_str().unwrap_or("").to_owned(),
        )
    });
    providers.into()
}
