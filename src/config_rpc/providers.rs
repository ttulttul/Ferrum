use super::DEFAULTS;
use crate::AppState;
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
    let path = config["homePath"]
        .as_str()
        .filter(|s| !s.trim().is_empty())
        .map(PathBuf::from)
        .or_else(|| std::env::var_os(var).map(PathBuf::from))
        .unwrap_or_else(|| home.join(folder));
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

pub(super) fn list(state: &AppState, settings: &Value, options: &Value) -> Value {
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
        let denied = state.settings.policy.provider_denial(driver);
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
        row["checkedAt"] = json!(state.config_started_at);
        row["message"] = json!(denied.unwrap_or_else(|| if enabled {
            format!("{label} provider status has not been checked in this session yet.")
        } else {
            format!("{label} is disabled in T3 Code settings.")
        }));
        row["models"] = models(&catalog["models"], config);
        row["slashCommands"] = catalog["slashCommands"].clone();
        row["skills"] = catalog["skills"].clone();
        if driver == "antigravity" && options["workspaceSnapshotsOnDemand"] != true {
            row["workspaceSnapshots"] = json!([]);
        }
        if driver == "opencode" {
            row["usageLimits"] = json!({"checkedAt":state.config_started_at,"windows":[],"unavailable":{"reason":"unsupported"}});
        }
        if matches!(driver, "fx" | "kiro") {
            row["versionAdvisory"] = json!({"status":"unknown","currentVersion":null,"latestVersion":null,"updateCommand":null,"canUpdate":false,"canInstallVersion":false,"checkedAt":state.config_started_at,"message":null});
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
