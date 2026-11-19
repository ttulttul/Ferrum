//! Migrations for persistent settings and the entry-specific semantics of a settings patch.
use anyhow::Context;
use rusqlite::{Connection, OpenFlags};
use serde_json::{json, Map, Value};
use std::{collections::HashSet, path::Path};

const LEGACY_PROJECT: [(&str, &str); 3] = [
    (
        "projectAgentBrowserAccessOverrides",
        "enableAgentBrowserAccess",
    ),
    ("projectAutoPullOverrides", "defaultAutoPull"),
    ("projectScriptOverrides", "defaultProjectScripts"),
];
const PROJECT_FIELDS: &[&str] = &[
    "worktreeCleanup",
    "defaultModelSelection",
    "defaultRuntimeMode",
    "defaultThreadEnvMode",
    "newWorktreesStartFromOrigin",
    "worktreeSubmodules",
    "defaultAutoPull",
    "defaultProjectScripts",
    "enableAgentBrowserAccess",
    "enableAgentDeviceAccess",
    "textGenerationModelSelection",
    "threadTitleInstructions",
    "sourceControlWriterModelSelection",
    "reviewGuideModelSelection",
    "reviewGuideInstructions",
    "sourceControlWritingStyle",
    "removeAgentCreditsOnMerge",
    "branchNamingMode",
    "branchNamePrefix",
    "branchNameInstructions",
    "pullRequestMergeMethod",
    "sidebarAutoSettleOnMerge",
    "sidebarAutoSettleAfterDays",
    "sidebarAutoSettleKeepPullRequests",
    "continueThreadsAfterServerUpdate",
    "responseStreamingMode",
    "speechProjectCustomWords",
    "providerInstances",
];
fn fields(value: &Value) -> impl Iterator<Item = (&String, &Value)> {
    value.as_object().into_iter().flatten()
}
fn array(v: &Value) -> Vec<Value> {
    v.as_array().cloned().unwrap_or_default()
}
fn object(v: &Value) -> Map<String, Value> {
    v.as_object().cloned().unwrap_or_default()
}
fn merge(current: &Value, patch: &Value) -> Value {
    if !current.is_object() || !patch.is_object() {
        return patch.clone();
    }
    let mut result = current.clone();
    for (k, v) in fields(patch) {
        result[k] = merge(&current[k], v);
    }
    result
}
fn derive_legacy(s: &mut Value) {
    for (legacy, key) in LEGACY_PROJECT {
        let mut map = json!({});
        for (id, entry) in fields(&s["projectSettingsOverrides"]) {
            if let Some(v) = entry.get(key) {
                map[id] = v.clone();
            }
        }
        s[legacy] = map;
    }
}
fn global_idle(s: &Value) -> &str {
    s["idleCompaction"]
        .as_str()
        .or_else(|| s["compactIdleThreads"].as_str())
        .unwrap_or(if s["compactIdleThreads"] == true {
            "beforeCacheExpires"
        } else {
            "onReturn"
        })
}
fn set_idle(s: &mut Value, mode: &str) {
    s["compactIdleThreads"] = json!(mode == "beforeCacheExpires");
    s["idleCompaction"] = json!(mode);
}
fn normalize_global(s: &mut Value) {
    if let Some(entries) = s["providerInstances"].as_object_mut() {
        for (_, inst) in entries.iter_mut() {
            if let Some(flag) = inst["config"]["enabled"].as_bool() {
                inst["config"]
                    .as_object_mut()
                    .unwrap()
                    .shift_remove("enabled");
                inst["enabled"] = json!(flag && inst["enabled"] != false);
            }
        }
    }
    let mode = global_idle(s).to_owned();
    set_idle(s, &mode);
}
pub(super) fn normalize(s: &mut Value) {
    normalize_global(s);
    derive_legacy(s);
}

fn slug(s: &str) -> bool {
    s.len() <= 64
        && s.bytes().next().is_some_and(|c| c.is_ascii_alphabetic())
        && s.bytes()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, b'-' | b'_'))
}
const HISTORICAL: [&str; 3] = ["cursor", "grok", "opencode"];
const FALLBACK: [(&str, &str); 8] = [
    ("codex", "gpt-6-luna"),
    ("claudeAgent", "claude-haiku-5-5"),
    ("cursor", "composer-2"),
    ("grok", "grok-build"),
    ("muse", "default"),
    ("pi", "default"),
    ("opencode", "opencode/big-pickle"),
    ("antigravity", "antigravity-default"),
];
fn enabled(instance: &Value, driver: &str) -> bool {
    let embedded = instance["config"]["enabled"].as_bool();
    if instance["enabled"] == false || embedded == Some(false) {
        return false;
    }
    instance["enabled"]
        .as_bool()
        .or(embedded)
        .unwrap_or(!matches!(
            driver,
            "cursor" | "grok" | "fx" | "kiro" | "muse" | "pi" | "opencode" | "antigravity"
        ))
}
pub(super) fn effective_selection(s: &mut Value, allowed: impl Fn(&str) -> bool) {
    let instances = &s["providerInstances"];
    let selected = s["textGenerationModelSelection"]["instanceId"]
        .as_str()
        .unwrap_or("");
    let driver = instances[selected]["driver"].as_str().unwrap_or(selected);
    let usable = if let Some(gen) = s["textGenerators"].get(selected) {
        gen["enabled"] == true
    } else if let Some(instance) = instances.get(selected) {
        enabled(instance, driver)
    } else {
        matches!(selected, "codex" | "claudeAgent")
    };
    if usable && driver != "acpRegistry" && allowed(driver) {
        return;
    }
    let candidates =
        FALLBACK
            .iter()
            .map(|(k, m)| (*k, *k, *m))
            .chain(fields(instances).filter_map(|(id, inst)| {
                let driver = inst["driver"].as_str()?;
                let model = FALLBACK.iter().find(|(d, _)| *d == driver)?.1;
                Some((id.as_str(), driver, model))
            }));
    let default = &s["defaultModelSelection"];
    let mut fallback = None;
    for (id, default_driver, model) in candidates {
        let instance = &instances[id];
        let driver = instance["driver"].as_str().unwrap_or(default_driver);
        if !allowed(driver) || !enabled(instance, driver) {
            continue;
        }
        fallback = Some(
            if default["instanceId"] == id
                && instance["config"]["customModels"]
                    .as_array()
                    .is_some_and(|entries| {
                        entries.iter().any(|entry| {
                            entry
                                .as_str()
                                .or_else(|| entry["slug"].as_str())
                                .map(str::trim)
                                == default["model"].as_str()
                        })
                    })
            {
                default.clone()
            } else {
                json!({"instanceId":id,"model":model})
            },
        );
        break;
    }
    if let Some(fallback) = fallback {
        s["textGenerationModelSelection"] = fallback;
    }
}
fn projects_patch(current: &Value, patch: &Value) -> Value {
    let mut canonical = object(&patch["projectSettingsOverrides"]);
    for (legacy, key) in LEGACY_PROJECT {
        for (id, value) in fields(&patch[legacy]) {
            if patch["projectSettingsOverrides"].get(id).is_some() {
                continue;
            }
            let mut entry = object(
                canonical
                    .get(id)
                    .filter(|v| !v.is_null())
                    .unwrap_or(&current["projectSettingsOverrides"][id]),
            );
            if value.is_null() {
                entry.shift_remove(key);
            } else {
                entry.insert(key.into(), value.clone());
            }
            canonical.insert(
                id.clone(),
                if entry.is_empty() {
                    Value::Null
                } else {
                    entry.into()
                },
            );
        }
    }
    let mut entries = object(&current["projectSettingsOverrides"]);
    for (id, patch) in canonical {
        if patch.is_null() {
            entries.shift_remove(&id);
            continue;
        }
        let mut entry = object(&current["projectSettingsOverrides"][&id]);
        entry.retain(|key, _| {
            !PROJECT_FIELDS.contains(&key.as_str())
                || key == "providerInstances" && patch.get(key).is_none()
        });
        for (k, v) in fields(&patch) {
            if PROJECT_FIELDS.contains(&k.as_str()) {
                entry.insert(k.clone(), v.clone());
            }
        }
        entries.insert(id, entry.into());
    }
    entries.retain(|_, e| e.as_object().is_some_and(|e| !e.is_empty()));
    entries.into()
}

fn preset(profile: &str) -> Value {
    let (fetch, health, active, idle, low, battery) = match profile {
        "performance" => (15_000, 60_000, 30_000, 120_000, false, false),
        "battery-saver" => (0, 900_000, 60_000, 600_000, true, true),
        _ => (30_000, 300_000, 30_000, 300_000, true, false),
    };
    json!({"automaticGitFetchInterval":fetch,"providerHealthRefreshInterval":health,"hostPowerMonitorActiveInterval":active,"hostPowerMonitorIdleInterval":idle,"pauseWhenHostLocked":true,"pauseWhenHostLowPower":low,"pauseWhenClientLowPower":low,"pauseWhenOnBattery":battery})
}
fn base(background: &Value) -> &str {
    match background["profile"].as_str() {
        Some("custom") => background["baseProfile"].as_str().unwrap_or("balanced"),
        Some(p) => p,
        None => "balanced",
    }
}
fn background_normalize(background: &Value) -> (Value, Value) {
    let profile = base(background);
    let preset_fields = preset(profile);
    let mut values = preset_fields.clone();
    if background["profile"] == "custom" {
        for (k, v) in fields(&background["overrides"]) {
            if preset_fields.get(k).is_some() {
                values[k] = v.clone();
            }
        }
    }
    if let Some(p) = [profile, "balanced", "performance", "battery-saver"]
        .into_iter()
        .find(|p| preset(p) == values)
    {
        return (
            json!({"schemaVersion":1,"profile":p,"overrides":{}}),
            values,
        );
    }
    let overrides: Map<_, _> = fields(&values)
        .filter(|(k, v)| preset_fields[*k] != **v)
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    (
        json!({"schemaVersion":1,"profile":"custom","baseProfile":profile,"overrides":overrides}),
        values,
    )
}
fn background(s: &Value, patch: &Value) -> (Value, Value) {
    const INTERVALS: [&str; 2] = ["automaticGitFetchInterval", "providerHealthRefreshInterval"];
    let mut current = s["backgroundActivity"].clone();
    if current.is_null() {
        current = json!({"schemaVersion":1,"profile":"balanced","overrides":{}})
    }
    let legacy = s["backgroundActivityProfile"]
        .as_str()
        .unwrap_or("balanced");
    if current["profile"] == "balanced"
        && current["baseProfile"].is_null()
        && fields(&current["overrides"]).next().is_none()
        && (legacy != "balanced"
            || INTERVALS
                .iter()
                .any(|k| !s[*k].is_null() && s[*k] != preset("balanced")[*k]))
    {
        let mut overrides = json!({});
        for k in INTERVALS {
            if s.get(k).is_some() && s[k] != preset(legacy)[k] {
                overrides[k] = s[k].clone();
            }
        }
        current = json!({"schemaVersion":1,"profile":"custom","baseProfile":legacy,"overrides":overrides});
    }
    current = background_normalize(&current).0;
    if let Some(p) = patch.get("backgroundActivity") {
        let mut bg = merge(&current, p);
        if let Some(overrides) = p.get("overrides") {
            bg["overrides"] = overrides.clone();
        }
        return background_normalize(&bg);
    }
    let has_intervals = INTERVALS.iter().any(|k| patch.get(k).is_some());
    let chosen = patch["backgroundActivityProfile"].as_str();
    if has_intervals {
        let mut overrides = if chosen.is_none() && current["profile"] == "custom" {
            current["overrides"].clone()
        } else {
            json!({})
        };
        for k in INTERVALS {
            if let Some(v) = patch.get(k) {
                overrides[k] = v.clone();
            }
        }
        current = json!({"schemaVersion":1,"profile":"custom","baseProfile":chosen.unwrap_or_else(||base(&current)),"overrides":overrides});
    } else if let Some(chosen) = chosen {
        current = json!({"schemaVersion":1,"profile":chosen,"overrides":{}});
    }
    background_normalize(&current)
}

fn preferences(current: &Value, patch: &Value, next: &mut Value) {
    let initial = |key: &str| {
        [
            &patch[key],
            &current[key],
            &patch["migrateModelPreferences"][key],
        ]
        .into_iter()
        .find(|v| !v.is_null())
        .cloned()
        .unwrap_or(Value::Null)
    };
    let mut favorites = initial("favorites");
    if let Some(changes) = patch["setModelFavorites"].as_array() {
        let mut rows = array(&favorites);
        for change in changes {
            let matches =
                |e: &Value| e["provider"] == change["provider"] && e["model"] == change["model"];
            if change["favorite"] == true {
                if !rows.iter().any(matches) {
                    rows.push(json!({"provider":change["provider"],"model":change["model"]}));
                }
            } else {
                rows.retain(|e| !matches(e));
            }
        }
        favorites = rows.into();
    }
    next["favorites"] = favorites;
    let mut prefs = initial("providerModelPreferences");
    let init = || json!({"hiddenModels":[],"modelOrder":[]});
    for change in array(&patch["setModelsHidden"]) {
        let Some(provider) = change["provider"].as_str() else {
            continue;
        };
        if prefs.get(provider).is_none() {
            prefs[provider] = init();
        }
        let mut hidden = array(&prefs[provider]["hiddenModels"]);
        hidden.retain(|m| m != &change["model"]);
        if change["hidden"] == true {
            hidden.push(change["model"].clone());
        }
        prefs[provider]["hiddenModels"] = hidden.into();
    }
    let order = &patch["setProviderModelOrder"];
    if let Some(provider) = order["provider"].as_str() {
        if prefs.get(provider).is_none() {
            prefs[provider] = init();
        }
        prefs[provider]["modelOrder"] = order["modelOrder"].clone();
    }
    next["providerModelPreferences"] = prefs;
}
const ENTRIES: [&str; 5] = [
    "usageLimitSources",
    "textGenerators",
    "secretProviders",
    "usagePriceOverrides",
    "usageModelAliases",
];
pub(super) fn apply(current: &Value, payload: &Value) -> Result<Value, String> {
    let mutation = &payload["providerInstanceMutation"];
    let id = mutation["instanceId"].as_str().unwrap_or("");
    if mutation["operation"] == "create" && current["providerInstances"].get(id).is_some() {
        return Err("create-provider-instance".into());
    }
    let mut next = apply_patch(current, &payload["patch"])?;
    if !mutation.is_null() {
        if !slug(id) {
            return Err("Invalid provider instance id".into());
        }
        match mutation["operation"].as_str() {
            Some("create" | "upsert") if mutation["instance"].is_object() => {
                next["providerInstances"][id] = mutation["instance"].clone()
            }
            Some("remove") => {
                if let Some(map) = next["providerInstances"].as_object_mut() {
                    map.shift_remove(id);
                }
            }
            _ => return Err("Invalid provider instance mutation".into()),
        }
        normalize(&mut next);
    }
    Ok(next)
}
fn apply_patch(current: &Value, patch: &Value) -> Result<Value, String> {
    if !patch.is_object() {
        return Err("Expected a settings patch".into());
    }
    let mut next = current.clone();
    for (k, v) in fields(patch) {
        if matches!(
            k.as_str(),
            "migrateModelPreferences"
                | "setModelFavorites"
                | "setModelsHidden"
                | "setProviderModelOrder"
        ) || LEGACY_PROJECT.iter().any(|(name, _)| name == k)
        {
            continue;
        }
        next[k] = merge(&current[k], v);
    }
    preferences(current, patch, &mut next);
    for key in ENTRIES {
        if let Some(entries) = patch.get(key) {
            let mut map = object(&current[key]);
            for (id, v) in fields(entries) {
                if v.is_null() {
                    map.shift_remove(id);
                } else {
                    map.insert(id.clone(), v.clone());
                }
            }
            next[key] = map.into();
        }
    }
    for key in [
        "providerInstances",
        "defaultModelSelection",
        "fallbackModelSelection",
        "speechPostProcessingModelSelection",
        "defaultProjectScripts",
        "sourceControlWriterModelSelection",
        "reviewGuideModelSelection",
    ] {
        if let Some(v) = patch.get(key) {
            next[key] = v.clone();
        }
    }
    if let Some(cleanup) = patch.get("worktreeCleanup") {
        next["worktreeCleanup"] = if cleanup["mode"] == "custom" {
            let mut rules = json!({});
            for k in [
                "worktreeKeepWhen",
                "worktreeAfterDays",
                "worktreeSettledAfterDays",
                "worktreeOnMerge",
                "worktreeOnDelete",
                "worktreeUnchanged",
            ] {
                if let Some(v) = next["storageCleanup"].get(k) {
                    rules[k] = v.clone();
                }
            }
            if current["worktreeCleanup"]["mode"] == "custom" {
                rules = merge(&rules, &current["worktreeCleanup"]["rules"]);
            }
            for (k, v) in fields(&cleanup["rules"]) {
                rules[k] = v.clone();
            }
            json!({"mode":"custom","rules":rules})
        } else {
            cleanup.clone()
        };
    }
    if let Some(v) = patch
        .get("worktreesDirectory")
        .filter(|v| **v != current["worktreesDirectory"])
    {
        let mut old = array(&current["previousWorktreesDirectories"]);
        let prev = &current["worktreesDirectory"];
        let already = old.contains(prev);
        old.retain(|s| s != v);
        if prev.as_str().is_some_and(|p| !p.is_empty()) && !already {
            old.push(prev.clone());
        }
        next["previousWorktreesDirectories"] = old.into();
    }
    for (kind, host) in fields(&patch["sourceControlHosts"]) {
        let old = &current["sourceControlHosts"][kind];
        let mut entry = object(old);
        for (k, v) in fields(host) {
            entry.insert(k.clone(), v.clone());
        }
        if fields(&host["tokens"]).next().is_some() {
            let merged: Map<_, _> = fields(&old["tokens"])
                .chain(fields(&host["tokens"]))
                .map(|(k, v)| (k.trim().to_lowercase(), v.clone()))
                .collect();
            entry.insert("tokens".into(), merged.into());
        }
        next["sourceControlHosts"][kind] = entry.into();
    }
    if patch.get("projectSettingsOverrides").is_some()
        || LEGACY_PROJECT.iter().any(|(k, _)| patch.get(k).is_some())
    {
        next["projectSettingsOverrides"] = projects_patch(current, patch);
    }
    let (bg, resolved) = background(current, patch);
    next["backgroundActivityProfile"] = json!(base(&bg));
    next["backgroundActivity"] = bg;
    for key in ["automaticGitFetchInterval", "providerHealthRefreshInterval"] {
        next[key] = resolved[key].clone();
    }
    let mode = patch["idleCompaction"]
        .as_str()
        .or_else(|| patch["compactIdleThreads"].as_str())
        .unwrap_or_else(|| match patch["compactIdleThreads"].as_bool() {
            Some(true) => "beforeCacheExpires",
            Some(false) if global_idle(current) != "off" => "onReturn",
            _ => global_idle(current),
        });
    set_idle(&mut next, mode);
    if let Some(p) = patch.get("textGenerationModelSelection") {
        let old = &current["textGenerationModelSelection"];
        let mut selection = json!({"instanceId":p.get("instanceId").unwrap_or(&old["instanceId"]),"model":p.get("model").unwrap_or(&old["model"])});
        let replaces = p.get("instanceId").is_some() || p.get("model").is_some();
        let mut opts = Map::new();
        if !replaces && !p["options"].as_array().is_some_and(Vec::is_empty) {
            for option in array(&old["options"]) {
                if let Some(id) = option["id"].as_str() {
                    opts.insert(id.into(), option["value"].clone());
                }
            }
        }
        for option in array(&p["options"]) {
            if let Some(id) = option["id"].as_str() {
                opts.insert(id.into(), option["value"].clone());
            }
        }
        if !opts.is_empty() {
            selection["options"] = Value::Array(
                opts.into_iter()
                    .map(|(id, value)| json!({"id":id,"value":value}))
                    .collect(),
            );
        }
        next["textGenerationModelSelection"] = selection;
    }
    normalize(&mut next);
    Ok(next)
}

fn table_exists(db: &Connection, name: &str) -> rusqlite::Result<bool> {
    db.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE name=?1 AND type IN ('table','view'))",
        [name],
        |r| r.get(0),
    )
}
fn historical_providers(
    db: Option<&Connection>,
) -> anyhow::Result<(HashSet<String>, HashSet<String>)> {
    let mut drivers = HashSet::new();
    let mut instances = HashSet::new();
    if let Some(conn) = db {
        for table in ["projection_thread_sessions", "provider_session_runtime"] {
            if !table_exists(conn, table)? {
                continue;
            }
            let mut stmt=conn.prepare(&format!("SELECT DISTINCT provider_name,provider_instance_id FROM {table} WHERE provider_name IN ('cursor','grok','opencode')"))?;
            let rows = stmt.query_map([], |r| {
                Ok((r.get::<_, String>(0)?, r.get::<_, Option<String>>(1)?))
            })?;
            for row in rows {
                let (driver, id) = row?;
                instances.insert(id.unwrap_or_else(|| driver.clone()));
                drivers.insert(driver);
            }
        }
    }
    Ok((drivers, instances))
}
fn migrate_providers(s: &mut Value, legacy: &Value, db: Option<&Connection>) -> anyhow::Result<()> {
    let (used_drivers, used_instances) =
        historical_providers(db).context("read-provider-history")?;
    let mut instances = object(&s["providerInstances"]);
    for (name, instance) in &mut instances {
        if instance["enabled"].is_null()
            && instance["driver"]
                .as_str()
                .is_some_and(|d| HISTORICAL.contains(&d))
            && used_instances.contains(name)
        {
            instance["enabled"] = json!(true);
        }
    }
    let mut entries = object(legacy);
    for driver in HISTORICAL {
        entries.entry(driver).or_insert_with(|| json!({}));
    }
    for (driver, blob) in entries {
        if !slug(&driver) || !blob.is_object() || instances.contains_key(&driver) {
            continue;
        }
        let mut config = object(&blob);
        let saved = config.shift_remove("enabled").and_then(|v| v.as_bool());
        let flag = saved.or_else(|| {
            (HISTORICAL.contains(&driver.as_str()) && used_drivers.contains(&driver))
                .then_some(true)
        });
        if config.is_empty() && flag.is_none_or(|flag| flag == enabled(&Value::Null, &driver)) {
            continue;
        }
        let mut instance = json!({"driver":driver});
        if let Some(flag) = flag {
            instance["enabled"] = json!(flag);
        }
        instance["config"] = config.into();
        instances.insert(driver, instance);
    }
    s["providerInstances"] = instances.into();
    Ok(())
}
fn decode_model(text: Option<String>) -> Option<Value> {
    let model = serde_json::from_str(&text?).ok()?;
    super::schema::persisted_field("defaultModelSelection", &model).filter(|m| !m.is_null())
}
fn decode_scripts(text: &str) -> Option<Value> {
    let scripts = serde_json::from_str(text).ok()?;
    super::schema::persisted_field("defaultProjectScripts", &scripts)
        .filter(|v| v.as_array().is_some_and(|a| !a.is_empty()))
}
fn fold_projects(s: &mut Value, raw: &Value, db: Option<&Connection>) -> anyhow::Result<()> {
    if s["projectSettingsFolded"] == true {
        return Ok(());
    }
    let mut entries = object(&s["projectSettingsOverrides"]);
    let mut set = |project: &str, key: &str, value: &Value| {
        let row = entries.entry(project).or_insert_with(|| json!({}));
        if row.get(key).is_none() {
            row[key] = value.clone();
        }
    };
    let mut reset_scripts = HashSet::new();
    let mut seen = false;
    for (legacy, key) in LEGACY_PROJECT {
        let map = raw.get(legacy).unwrap_or(&s[legacy]);
        for (id, v) in fields(map) {
            seen = true;
            if key == "defaultProjectScripts" && v.is_null() {
                reset_scripts.insert(id.clone());
            } else {
                set(id, key, v);
            }
        }
    }
    if let Some(db) = db {
        if table_exists(db, "projection_projects")? {
            let mut stmt=db.prepare("SELECT project_id,default_model_selection_json,default_thread_env_mode,auto_pull,scripts_json FROM projection_projects WHERE deleted_at IS NULL")?;
            let mut rows = stmt.query([])?;
            while let Some(r) = rows.next()? {
                let id: String = r.get(0)?;
                seen = true;
                if let Some(model) = decode_model(r.get(1)?) {
                    set(&id, "defaultModelSelection", &model);
                }
                if let Some(env) = r
                    .get::<_, Option<String>>(2)?
                    .filter(|v| matches!(v.as_str(), "local" | "worktree"))
                {
                    set(&id, "defaultThreadEnvMode", &json!(env));
                }
                if r.get::<_, Option<i64>>(3)? == Some(1) {
                    set(&id, "defaultAutoPull", &json!(true));
                }
                if !reset_scripts.contains(&id) {
                    if let Some(scripts) = r
                        .get::<_, Option<String>>(4)?
                        .and_then(|t| decode_scripts(&t))
                    {
                        set(&id, "defaultProjectScripts", &scripts);
                    }
                }
            }
        }
    }
    if seen {
        entries.retain(|_, e| fields(e).next().is_some());
        s["projectSettingsOverrides"] = entries.into();
        s["projectSettingsFolded"] = json!(true);
        derive_legacy(s);
    }
    Ok(())
}
pub(super) fn migrate(s: &mut Value, raw: &Value, state_dir: &Path) -> anyhow::Result<()> {
    let path = state_dir.join("statev2.sqlite");
    let conn = if path.try_exists()? {
        Some(Connection::open_with_flags(
            path,
            OpenFlags::SQLITE_OPEN_READ_ONLY,
        )?)
    } else {
        None
    };
    if let Some(legacy) = raw.get("providers").filter(|v| v.is_object()) {
        migrate_providers(s, legacy, conn.as_ref())?;
    }
    fold_projects(s, raw, conn.as_ref()).context("read-project-settings")?;
    for kind in ["bitbucket", "github"] {
        if s["sourceControlHosts"].get(kind).is_none() {
            if let Some(host) = raw.get(kind).filter(|v| v.is_object()) {
                s["sourceControlHosts"][kind] = host.clone();
            }
        }
    }
    if raw.get("idleCompaction").is_none() {
        if let Some(legacy) = raw.get("compactIdleThreads") {
            let mode = legacy.as_str().unwrap_or(if legacy == true {
                "beforeCacheExpires"
            } else {
                "onReturn"
            });
            set_idle(s, mode);
        }
    }
    if s["responseStreamingMode"] == "token" {
        s["responseStreamingMode"] = json!("paragraph");
    }
    if let Some(entries) = s["projectSettingsOverrides"].as_object_mut() {
        for row in entries.values_mut() {
            if row["responseStreamingMode"] == "token" {
                row["responseStreamingMode"] = json!("paragraph");
            }
        }
    }
    normalize_global(s);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn migration_prefers_canonical_projects_and_explicit_driver_flags() {
        let temp = tempfile::tempdir().unwrap();
        let conn = Connection::open(temp.path().join("statev2.sqlite")).unwrap();
        conn.execute_batch("CREATE TABLE projection_thread_sessions(provider_name, provider_instance_id);
            INSERT INTO projection_thread_sessions VALUES ('cursor','custom'),('grok',NULL);
            CREATE TABLE projection_projects(project_id,default_model_selection_json,default_thread_env_mode,auto_pull,scripts_json,deleted_at);
            INSERT INTO projection_projects VALUES ('p','{\"provider\":\"codex\",\"model\":\" a \",\"options\":{\"speed\":true}}','worktree',1,'[]',NULL),
            ('removed',NULL,'local',1,'[]','yesterday'),('bad','{}','unknown',0,'[null]',NULL);").unwrap();
        let raw = json!({"providers":{"codex":{"enabled":false,"other":"v"},"cursor":{"enabled":false}},
            "providerInstances":{"custom":{"driver":"cursor","config":{"enabled":false}}},
            "projectSettingsOverrides":{"p":{"defaultAutoPull":false}},
            "projectScriptOverrides":{"p":null},"projectAgentBrowserAccessOverrides":{"p":true},
            "github":{"authMode":"cli"}});
        let mut settings = raw.clone();
        migrate(&mut settings, &raw, temp.path()).unwrap();
        assert_eq!(
            settings["providerInstances"]["custom"],
            json!({"driver":"cursor","config":{},"enabled":false})
        );
        assert_eq!(
            settings["providerInstances"]["grok"],
            json!({"driver":"grok","enabled":true,"config":{}})
        );
        assert!(settings["providerInstances"].get("cursor").is_none());
        assert!(settings["sourceControlHosts"].get("github").is_some());
        let projects = &settings["projectSettingsOverrides"];
        assert_eq!(
            projects["p"],
            json!({"defaultAutoPull":false,"enableAgentBrowserAccess":true,"defaultThreadEnvMode":"worktree","defaultModelSelection":{"instanceId":"codex","model":"a","options":[{"id":"speed","value":true}]}})
        );
        assert!(projects.get("bad").is_none() && projects.get("removed").is_none());
        assert_eq!(settings["projectSettingsFolded"], true);
        settings["projectSettingsOverrides"] = json!({});
        migrate(&mut settings, &json!({}), temp.path()).unwrap();
        assert_eq!(settings["projectSettingsOverrides"], json!({}));
    }
    #[test]
    fn provider_fallback_follows_custom_models_and_explicitly_enabled_instances() {
        let mut state = json!({"providerInstances":{"codex":{"driver":"codex","enabled":false},"claudeAgent":{"driver":"claudeAgent","enabled":false},
            "custom":{"driver":"opencode","enabled":true,"config":{"customModels":[{"slug":"custom-model"}]}}},
            "defaultModelSelection":{"instanceId":"custom","model":"custom-model","options":[{"id":"setting","value":true}]},
            "textGenerationModelSelection":{"instanceId":"absent","model":"lost"}});
        effective_selection(&mut state, |_| true);
        assert_eq!(
            state["textGenerationModelSelection"],
            state["defaultModelSelection"]
        );
        state["providerInstances"]["claudeAgent"]["enabled"] = json!(true);
        effective_selection(&mut state, |driver| driver != "opencode");
        assert_eq!(
            state["textGenerationModelSelection"],
            json!({"instanceId":"claudeAgent","model":"claude-haiku-5-5"})
        );
    }
    #[test]
    fn project_updates_preserve_unknown_stored_fields_and_filter_new_unknown_fields() {
        let current = json!({"projectSettingsOverrides":{"p":{"futureStored":true,"providerInstances":{"codex":{"enabled":false}},"defaultAutoPull":false}}});
        let next=apply(&current,&json!({"patch":{"projectSettingsOverrides":{"p":{"newUnknown":"v","defaultRuntimeMode":"auto"}},"projectAutoPullOverrides":{"p":true}}})).unwrap();
        assert_eq!(
            next["projectSettingsOverrides"]["p"],
            json!({"futureStored":true,"providerInstances":{"codex":{"enabled":false}},"defaultRuntimeMode":"auto"})
        );
        assert_eq!(next["projectAutoPullOverrides"], json!({}));
    }
}
