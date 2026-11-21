//! The provider model manifest, as `ModelManifest.ts` keeps it in the
//! reference server: a bundled copy, refreshed from the upstream repository
//! with a TTL, a retry backoff and an on-disk cache under the state directory.
//! Claude's model list in the config snapshot is derived from it.
//!
//! Decoding, the catalog transformation and the refresh decisions are
//! host-free; reading the network, the cache file and the clock goes through
//! [`ManifestHost`].
use serde_json::{json, Map, Value};
use std::cmp::Ordering;
use std::sync::{Arc, LazyLock, Mutex};

pub(crate) const MODEL_MANIFEST_URL: &str =
    "https://raw.githubusercontent.com/pingdotgg/t3code/main/apps/server/src/provider/model-manifest.json";
pub(crate) const MANIFEST_TTL_MS: i64 = 60 * 60 * 1000;
/// GitHub's raw CDN caches about this long, so a shorter window buys nothing.
pub(crate) const MANIFEST_ON_DEMAND_TTL_MS: i64 = 5 * 60 * 1000;
/// Failed fetches back off so offline reads do not each wait on the network.
pub(crate) const MANIFEST_RETRY_MS: i64 = 5 * 60 * 1000;
pub(crate) const FETCH_TIMEOUT_MS: u64 = 10_000;
const MODEL_NEW_BADGE_DURATION_MS: i64 = 7 * 24 * 60 * 60 * 1000;
/// The cache file's name in the state directory.
pub(crate) const CACHE_FILE: &str = "model-manifest.json";

/// The manifest shipped with this build, a copy of the pinned reference's.
pub(crate) static BUNDLED: LazyLock<Arc<Value>> = LazyLock::new(|| {
    let raw: Value = serde_json::from_str(include_str!("../contracts/model-manifest.json"))
        .expect("bundled model manifest is JSON");
    Arc::new(decode(&raw).expect("bundled model manifest is valid"))
});

// ---------------------------------------------------------------------------
// Decoding

fn trimmed(value: &Value) -> Option<String> {
    let s = value.as_str()?.trim();
    (!s.is_empty()).then(|| s.to_owned())
}

fn opt_trimmed(object: &Map<String, Value>, key: &str) -> Result<Option<String>, ()> {
    match object.get(key) {
        None => Ok(None),
        Some(value) => trimmed(value).map(Some).ok_or(()),
    }
}

fn trimmed_array(value: &Value) -> Option<Vec<Value>> {
    value
        .as_array()?
        .iter()
        .map(|v| trimmed(v).map(Value::String))
        .collect()
}

fn decode_choice(value: &Value) -> Option<Value> {
    let o = value.as_object()?;
    let mut out = Map::new();
    out.insert("id".into(), trimmed(o.get("id")?)?.into());
    out.insert("label".into(), trimmed(o.get("label")?)?.into());
    if let Some(d) = opt_trimmed(o, "description").ok()? {
        out.insert("description".into(), d.into());
    }
    if let Some(v) = o.get("isDefault") {
        out.insert("isDefault".into(), v.as_bool()?.into());
    }
    Some(out.into())
}

/// One `ProviderOptionDescriptor`, with its keys in the contract's order and
/// unknown keys dropped, as the reference's schema decode leaves it.
fn decode_descriptor(value: &Value) -> Option<Value> {
    let o = value.as_object()?;
    let mut out = Map::new();
    out.insert("id".into(), trimmed(o.get("id")?)?.into());
    out.insert("label".into(), trimmed(o.get("label")?)?.into());
    if let Some(d) = opt_trimmed(o, "description").ok()? {
        out.insert("description".into(), d.into());
    }
    match o.get("type")?.as_str()? {
        "select" => {
            out.insert("type".into(), "select".into());
            let options = o.get("options")?.as_array()?;
            let options: Option<Vec<Value>> = options.iter().map(decode_choice).collect();
            out.insert("options".into(), options?.into());
            if let Some(v) = opt_trimmed(o, "currentValue").ok()? {
                out.insert("currentValue".into(), v.into());
            }
            if let Some(v) = o.get("promptInjectedValues") {
                out.insert("promptInjectedValues".into(), trimmed_array(v)?.into());
            }
            if let Some(v) = o.get("role") {
                (v.as_str()? == "reasoning").then_some(())?;
                out.insert("role".into(), v.clone());
            }
        }
        "boolean" => {
            out.insert("type".into(), "boolean".into());
            if let Some(v) = o.get("currentValue") {
                out.insert("currentValue".into(), v.as_bool()?.into());
            }
        }
        _ => return None,
    }
    Some(out.into())
}

fn decode_capabilities(value: &Value) -> Option<Value> {
    let o = value.as_object()?;
    let mut out = Map::new();
    if let Some(list) = o.get("optionDescriptors") {
        let list: Option<Vec<Value>> = list.as_array()?.iter().map(decode_descriptor).collect();
        out.insert("optionDescriptors".into(), list?.into());
    }
    Some(out.into())
}

/// The reference's `parseSemver`: three numeric segments after normalizing,
/// with an optional prerelease.
fn parse_semver(value: &str) -> Option<(u64, u64, u64, Vec<String>)> {
    let without_build = value.trim().split('+').next().unwrap_or("");
    let mut parts = without_build.splitn(2, '-');
    let main = parts.next().unwrap_or("");
    let prerelease = parts.next().unwrap_or("");
    let mut segments: Vec<&str> = main
        .split('.')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .collect();
    while !segments.is_empty() && segments.len() < 3 {
        segments.push("0");
    }
    if segments.len() != 3 {
        return None;
    }
    let first = segments[0].strip_prefix('v').unwrap_or(segments[0]);
    let numbers: Option<Vec<u64>> = [first, segments[1], segments[2]]
        .iter()
        .map(|s| {
            (!s.is_empty() && s.bytes().all(|b| b.is_ascii_digit()))
                .then(|| s.parse().ok())
                .flatten()
        })
        .collect();
    let numbers = numbers?;
    let pre = prerelease
        .split('.')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_owned)
        .collect();
    Some((numbers[0], numbers[1], numbers[2], pre))
}

fn compare_semver(left: &str, right: &str) -> Ordering {
    let (Some(l), Some(r)) = (parse_semver(left), parse_semver(right)) else {
        return left.cmp(right);
    };
    let base = (l.0, l.1, l.2).cmp(&(r.0, r.1, r.2));
    if base != Ordering::Equal {
        return base;
    }
    match (l.3.is_empty(), r.3.is_empty()) {
        (true, true) => return Ordering::Equal,
        (true, false) => return Ordering::Greater,
        (false, true) => return Ordering::Less,
        _ => {}
    }
    for i in 0..l.3.len().max(r.3.len()) {
        let (Some(a), Some(b)) = (l.3.get(i), r.3.get(i)) else {
            return l.3.len().cmp(&r.3.len());
        };
        let numeric = |s: &str| s.bytes().all(|b| b.is_ascii_digit());
        let order = match (numeric(a), numeric(b)) {
            (true, true) => (a.len(), a.as_str()).cmp(&(b.len(), b.as_str())),
            (true, false) => Ordering::Less,
            (false, true) => Ordering::Greater,
            _ => a.cmp(b),
        };
        if order != Ordering::Equal {
            return order;
        }
    }
    Ordering::Equal
}

fn valid_string_record(value: &Value, valid: impl Fn(&Value) -> bool) -> bool {
    value
        .as_object()
        .is_some_and(|o| o.iter().all(|(k, v)| !k.trim().is_empty() && valid(v)))
}

/// `ClaudeProfileAdapterSchema`.
fn valid_claude_profile_adapter(adapter: &Value) -> bool {
    let Some(o) = adapter.as_object() else {
        return false;
    };
    let Some(code) = o.get("claudeCode") else {
        return true;
    };
    let Some(code) = code.as_object() else {
        return false;
    };
    code.get("effortMap")
        .is_none_or(|v| valid_string_record(v, |x| x.is_null() || trimmed(x).is_some()))
        && code.get("modelSuffixes").is_none_or(|v| {
            valid_string_record(v, |x| valid_string_record(x, |y| trimmed(y).is_some()))
        })
        && code
            .get("contextWindowTokens")
            .is_none_or(|v| valid_string_record(v, Value::is_number))
        && code
            .get("fixedContextWindowTokens")
            .is_none_or(Value::is_number)
}

/// `ClaudeModelAdapterSchema`: versions parse, and the minimum is below the
/// exclusive maximum.
fn valid_claude_model_adapter(adapter: &Value) -> bool {
    let Some(o) = adapter.as_object() else {
        return false;
    };
    let Some(code) = o.get("claudeCode") else {
        return true;
    };
    let Some(code) = code.as_object() else {
        return false;
    };
    let version = |key: &str| match code.get(key) {
        None => Some(None),
        Some(v) => trimmed(v).filter(|s| parse_semver(s).is_some()).map(Some),
    };
    let (Some(min), Some(max)) = (version("minVersion"), version("maxVersionExclusive")) else {
        return false;
    };
    match (min, max) {
        (Some(min), Some(max)) => compare_semver(&min, &max) == Ordering::Less,
        _ => true,
    }
}

fn decode_model(value: &Value) -> Option<Value> {
    let o = value.as_object()?;
    let mut out = Map::new();
    out.insert("slug".into(), trimmed(o.get("slug")?)?.into());
    out.insert("name".into(), trimmed(o.get("name")?)?.into());
    for key in ["shortName", "subProvider"] {
        if let Some(v) = opt_trimmed(o, key).ok()? {
            out.insert(key.into(), v.into());
        }
    }
    if let Some(v) = o.get("aliases") {
        out.insert("aliases".into(), trimmed_array(v)?.into());
    }
    let status = o.get("status")?.as_str()?;
    matches!(status, "current" | "legacy").then_some(())?;
    out.insert("status".into(), status.into());
    if let Some(v) = o.get("releasedAt") {
        out.insert("releasedAt".into(), v.as_str()?.into());
    }
    if let Some(v) = opt_trimmed(o, "profile").ok()? {
        out.insert("profile".into(), v.into());
    }
    if let Some(v) = o.get("adapter") {
        out.insert("adapter".into(), v.clone());
    }
    Some(out.into())
}

fn decode_catalog(value: &Value) -> Option<Value> {
    let o = value.as_object()?;
    let mut out = Map::new();
    if let Some(defaults) = o.get("defaults") {
        let d = defaults.as_object()?;
        let mut decoded = Map::new();
        if let Some(chat) = opt_trimmed(d, "chat").ok()? {
            decoded.insert("chat".into(), chat.into());
        }
        out.insert("defaults".into(), decoded.into());
    }
    let mut profiles = Map::new();
    for (name, profile) in o.get("profiles")?.as_object()? {
        let p = profile.as_object()?;
        let mut decoded = Map::new();
        if let Some(c) = p.get("capabilities") {
            decoded.insert("capabilities".into(), decode_capabilities(c)?);
        }
        if let Some(a) = p.get("adapter") {
            decoded.insert("adapter".into(), a.clone());
        }
        profiles.insert(name.clone(), decoded.into());
    }
    out.insert("profiles".into(), profiles.into());
    let models: Option<Vec<Value>> = o
        .get("models")?
        .as_array()?
        .iter()
        .map(decode_model)
        .collect();
    out.insert("models".into(), models?.into());
    Some(out.into())
}

/// Unique slugs, and every profile and default model that is named exists.
fn valid_references(catalog: &Value) -> bool {
    let mut slugs = std::collections::HashSet::new();
    let models_valid = catalog["models"].as_array().into_iter().flatten().all(|m| {
        let slug = m["slug"].as_str().unwrap_or_default();
        slugs.insert(slug.to_owned())
            && m["profile"]
                .as_str()
                .is_none_or(|p| catalog["profiles"].get(p).is_some())
    });
    models_valid
        && catalog["defaults"]["chat"]
            .as_str()
            .is_none_or(|chat| slugs.contains(chat))
}

/// Decode a manifest the way `ModelManifestSchema` does, or `None` where the
/// reference would reject it. The result keeps only the fields the schema
/// knows.
pub(crate) fn decode(raw: &Value) -> Option<Value> {
    let o = raw.as_object()?;
    (o.get("version")? == 1).then_some(())?;
    let mut out = Map::new();
    out.insert("version".into(), 1.into());
    if let Some(v) = o.get("updatedAt") {
        out.insert("updatedAt".into(), v.as_str()?.into());
    }
    if let Some(v) = o.get("compatibility") {
        v.as_array()?
            .iter()
            .all(|p| {
                p.as_object()
                    .is_some_and(|p| p.get("driver").is_some_and(Value::is_string))
            })
            .then_some(())?;
        out.insert("compatibility".into(), v.clone());
    }
    let current = o.get("currentModels")?.as_object()?;
    current
        .values()
        .all(|v| v.as_array().is_some_and(|a| a.iter().all(Value::is_string)))
        .then_some(())?;
    out.insert("currentModels".into(), Value::Object(current.clone()));
    if let Some(providers) = o.get("providers") {
        let mut decoded = Map::new();
        for (driver, catalog) in providers.as_object()? {
            let catalog = decode_catalog(catalog)?;
            valid_references(&catalog).then_some(())?;
            decoded.insert(driver.clone(), catalog);
        }
        if let Some(claude) = decoded.get("claudeAgent") {
            let profiles_ok = claude["profiles"].as_object().is_some_and(|p| {
                p.values()
                    .all(|p| valid_claude_profile_adapter(p.get("adapter").unwrap_or(&json!({}))))
            });
            let models_ok = claude["models"].as_array().is_some_and(|m| {
                m.iter()
                    .all(|m| valid_claude_model_adapter(m.get("adapter").unwrap_or(&json!({}))))
            });
            (profiles_ok && models_ok).then_some(())?;
        }
        out.insert("providers".into(), decoded.into());
    }
    Some(out.into())
}

/// JavaScript's `Date.parse` for the forms manifests use: an ISO date
/// (midnight UTC) or an RFC 3339 timestamp. Anything else is NaN.
fn parse_date_ms(value: &str) -> Option<i64> {
    use time::format_description::well_known::Rfc3339;
    if let Ok(t) = time::OffsetDateTime::parse(value, &Rfc3339) {
        return Some((t.unix_timestamp_nanos() / 1_000_000) as i64);
    }
    let format = time::macros::format_description!("[year]-[month]-[day]");
    time::Date::parse(value, &format)
        .ok()
        .map(|d| d.midnight().assume_utc().unix_timestamp() * 1000)
}

/// A missing or unparsable `updatedAt` ranks below any dated manifest.
pub(crate) fn updated_at_ms(manifest: &Value) -> i64 {
    manifest["updatedAt"]
        .as_str()
        .and_then(parse_date_ms)
        .unwrap_or(0)
}

// ---------------------------------------------------------------------------
// Catalogs

/// `resolveProviderCatalog` followed by `catalogServerModel`: the driver's
/// models as server provider models, with new-model badges as of `now_ms`.
pub(crate) fn provider_models(manifest: &Value, driver: &str, now_ms: i64) -> Option<Vec<Value>> {
    let catalog = manifest["providers"].get(driver)?;
    let default_chat = catalog["defaults"]["chat"].as_str();
    let mut models = Vec::new();
    for entry in catalog["models"].as_array()? {
        let profile = match entry["profile"].as_str() {
            Some(name) => Some(catalog["profiles"].get(name)?),
            None => None,
        };
        let status = entry["status"].as_str().unwrap_or_default();
        let new_badge = entry["releasedAt"]
            .as_str()
            .and_then(parse_date_ms)
            .is_some_and(|released| {
                status == "current"
                    && now_ms >= released
                    && now_ms - released < MODEL_NEW_BADGE_DURATION_MS
            });
        let mut model = Map::new();
        model.insert("slug".into(), entry["slug"].clone());
        model.insert("name".into(), entry["name"].clone());
        for key in ["shortName", "subProvider", "aliases"] {
            if let Some(v) = entry.get(key) {
                model.insert(key.into(), v.clone());
            }
        }
        if new_badge {
            model.insert("badge".into(), "new".into());
        }
        model.insert("isCustom".into(), false.into());
        if default_chat.is_some() && entry["slug"].as_str() == default_chat {
            model.insert("isDefault".into(), true.into());
        }
        if status == "legacy" {
            model.insert("isLegacy".into(), true.into());
        }
        model.insert(
            "capabilities".into(),
            profile
                .and_then(|p| p.get("capabilities"))
                .cloned()
                .unwrap_or(Value::Null),
        );
        models.push(model.into());
    }
    Some(models)
}

/// The catalog entry and profile of the Claude model `model` (slug or
/// alias).
fn claude_entry<'a>(manifest: &'a Value, model: &str) -> Option<(&'a Value, &'a Value)> {
    let catalog = &manifest["providers"]["claudeAgent"];
    let models = catalog["models"].as_array()?;
    let model = model.trim();
    let entry = models
        .iter()
        .find(|entry| entry["slug"] == model)
        .or_else(|| {
            models.iter().find(|entry| {
                entry["aliases"].as_array().is_some_and(|aliases| {
                    aliases
                        .iter()
                        .filter_map(Value::as_str)
                        .any(|alias| alias.eq_ignore_ascii_case(model))
                })
            })
        })?;
    Some((entry, &catalog["profiles"][entry["profile"].as_str()?]))
}

/// What the reference's compileClaudeModelSelection launches a Claude turn
/// with: the `--effort` level, if any, and the `--settings` it sets
/// (`fastMode` for models that offer it).
pub(crate) fn claude_launch(manifest: &Value, selection: &Value) -> (Option<String>, Value) {
    let option = |id: &str| {
        selection["options"]
            .as_array()
            .into_iter()
            .flatten()
            .find(|option| option["id"] == id)
            .map(|option| option["value"].clone())
    };
    let raw = option("effort").and_then(|v| v.as_str().map(|s| s.trim().to_owned()));
    let Some((_, profile)) =
        claude_entry(manifest, selection["model"].as_str().unwrap_or_default())
    else {
        // A model outside the catalog takes a known effort level as is.
        let effort =
            raw.filter(|e| ["low", "medium", "high", "xhigh", "max"].contains(&e.as_str()));
        return (effort, json!({}));
    };
    let capabilities = with_ultracode_toggle(&profile["capabilities"]);
    let descriptors = capabilities["optionDescriptors"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    let effort_descriptor = descriptors.iter().find(|d| d["id"] == "effort");
    let effort = effort_descriptor.and_then(|descriptor| {
        let options = descriptor["options"].as_array()?;
        let valid = |id: &str| options.iter().any(|option| option["id"] == id);
        raw.clone().filter(|raw| valid(raw)).or_else(|| {
            descriptor["currentValue"]
                .as_str()
                .map(str::to_owned)
                .or_else(|| {
                    options
                        .iter()
                        .find(|option| option["isDefault"] == true)
                        .and_then(|option| option["id"].as_str())
                        .map(str::to_owned)
                })
        })
    });
    // The catalog maps some levels to Claude Code's own; null means the
    // level goes in the prompt instead.
    let effort_map = &profile["adapter"]["claudeCode"]["effortMap"];
    let effort = effort.and_then(|effort| match effort_map.get(&effort) {
        Some(Value::String(mapped)) => Some(mapped.clone()),
        Some(_) => None,
        None => Some(effort),
    });
    let mut settings = Map::new();
    if descriptors
        .iter()
        .any(|d| d["id"] == "fastMode" && d["type"] == "boolean")
    {
        let fast = option("fastMode")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        settings.insert("fastMode".into(), json!(fast));
    }
    (effort, Value::Object(settings))
}

/// The reference's resolveClaudeCatalogContextWindowTokens: the context
/// window of the Claude model `selection` names (by slug or alias), from its
/// profile's fixed window or the window its `contextWindow` option picks.
pub(crate) fn claude_context_window(manifest: &Value, selection: &Value) -> Option<u64> {
    let model = selection["model"].as_str()?.trim();
    let catalog = &manifest["providers"]["claudeAgent"];
    let models = catalog["models"].as_array()?;
    let entry = models
        .iter()
        .find(|entry| entry["slug"] == model)
        .or_else(|| {
            models.iter().find(|entry| {
                entry["aliases"].as_array().is_some_and(|aliases| {
                    aliases
                        .iter()
                        .filter_map(Value::as_str)
                        .any(|alias| alias.eq_ignore_ascii_case(model))
                })
            })
        })?;
    let profile = &catalog["profiles"][entry["profile"].as_str()?];
    let runtime = &profile["adapter"]["claudeCode"];
    if let Some(fixed) = runtime["fixedContextWindowTokens"]
        .as_u64()
        .filter(|n| *n > 0)
    {
        return Some(fixed);
    }
    let windows = runtime["contextWindowTokens"].as_object()?;
    let descriptor = profile["capabilities"]["optionDescriptors"]
        .as_array()
        .into_iter()
        .flatten()
        .find(|descriptor| descriptor["id"] == "contextWindow");
    let chosen = selection["options"]
        .as_array()
        .into_iter()
        .flatten()
        .find(|option| option["id"] == "contextWindow")
        .and_then(|option| option["value"].as_str())
        .filter(|value| windows.contains_key(*value))
        .or_else(|| {
            let descriptor = descriptor?;
            descriptor["currentValue"].as_str().or_else(|| {
                descriptor["options"]
                    .as_array()?
                    .iter()
                    .find(|option| option["isDefault"] == true)
                    .and_then(|option| option["id"].as_str())
            })
        })?;
    windows.get(chosen)?.as_u64()
}

/// `withUltracodeToggle`: the manifest keeps Ultracode as an effort level;
/// servers present it as a separate toggle beside the effort select.
pub(crate) fn with_ultracode_toggle(capabilities: &Value) -> Value {
    let Some(descriptors) = capabilities["optionDescriptors"].as_array() else {
        return capabilities.clone();
    };
    let Some(index) = descriptors.iter().position(|d| {
        d["id"] == "effort"
            && d["type"] == "select"
            && d["options"]
                .as_array()
                .is_some_and(|o| o.iter().any(|o| o["id"] == "ultracode"))
    }) else {
        return capabilities.clone();
    };
    let effort = &descriptors[index];
    let options = effort["options"].as_array().cloned().unwrap_or_default();
    let default_id = effort["currentValue"].as_str().or_else(|| {
        options
            .iter()
            .find(|o| o["isDefault"].as_bool().unwrap_or(false))
            .and_then(|o| o["id"].as_str())
    });
    let ultracode_by_default = default_id == Some("ultracode");
    let has_xhigh = options.iter().any(|o| o["id"] == "xhigh");
    let mut next = Vec::new();
    for option in options {
        let mut option = option.as_object().cloned().unwrap_or_default();
        let is_default = option.shift_remove("isDefault");
        let truthy = is_default
            .as_ref()
            .is_some_and(|v| v.as_bool() == Some(true));
        if option["id"] == "ultracode" {
            if !has_xhigh {
                let mut xhigh = json!({"id":"xhigh","label":"Extra High"});
                if truthy {
                    xhigh["isDefault"] = is_default.unwrap();
                }
                next.push(xhigh);
            }
            continue;
        }
        if ultracode_by_default && option["id"] == "xhigh" {
            option.insert("isDefault".into(), true.into());
        } else if truthy && !ultracode_by_default {
            option.insert("isDefault".into(), is_default.unwrap());
        }
        next.push(option.into());
    }
    let mut effort = effort.clone();
    if effort["currentValue"] == "ultracode" {
        effort["currentValue"] = "xhigh".into();
    }
    effort["options"] = next.into();
    let mut toggle = json!({"id":"ultracode","label":"Ultracode","type":"boolean"});
    if ultracode_by_default {
        toggle["currentValue"] = true.into();
    }
    let mut descriptors = descriptors.clone();
    descriptors.splice(index..=index, [effort, toggle]);
    json!({"optionDescriptors": descriptors})
}

/// `resolveClaudeModelCatalog`: Claude's built-in models from `manifest`,
/// falling back to the bundled manifest when it has no Claude catalog.
pub(crate) fn claude_models(manifest: &Value, now_ms: i64) -> Vec<Value> {
    let models = provider_models(manifest, "claudeAgent", now_ms)
        .or_else(|| provider_models(&BUNDLED, "claudeAgent", now_ms))
        .unwrap_or_default();
    models
        .into_iter()
        .map(|mut model| {
            if !model["capabilities"].is_null() {
                model["capabilities"] = with_ultracode_toggle(&model["capabilities"]);
            }
            model
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Refresh

/// What the refresh needs from its host.
pub(crate) trait ManifestHost: Send + Sync {
    fn now_ms(&self) -> i64;
    /// GET the manifest URL with the fetch timeout: the JSON body on a 2xx
    /// reply, otherwise `None`.
    fn fetch(&self) -> impl std::future::Future<Output = Option<Value>> + Send;
    fn read_cache(&self) -> Option<String>;
    /// Write the cache atomically; failures are ignored.
    fn write_cache(&self, contents: &str);
    /// The `enableProviderUpdateChecks` setting; `None` when settings can't
    /// be read.
    fn update_checks_enabled(&self) -> Option<bool>;
}

struct State {
    manifest: Arc<Value>,
    fetched_at_ms: Option<i64>,
    last_attempt_ms: Option<i64>,
    disk_loaded: bool,
}

/// The process's manifest: [`current`](Self::current) never waits on the
/// network; the refreshes run one at a time.
pub(crate) struct ModelManifest<H> {
    host: H,
    state: Mutex<State>,
    refresh_lock: tokio::sync::Mutex<()>,
}

impl<H: ManifestHost> ModelManifest<H> {
    pub(crate) fn new(host: H) -> Self {
        Self {
            host,
            state: Mutex::new(State {
                manifest: BUNDLED.clone(),
                fetched_at_ms: None,
                last_attempt_ms: None,
                disk_loaded: false,
            }),
            refresh_lock: tokio::sync::Mutex::new(()),
        }
    }

    fn ensure_disk_cache_loaded(&self) {
        let mut state = self.state.lock().unwrap();
        if state.disk_loaded {
            return;
        }
        state.disk_loaded = true;
        let Some(cache) = self
            .host
            .read_cache()
            .and_then(|raw| serde_json::from_str::<Value>(&raw).ok())
        else {
            return;
        };
        let (Some(fetched_at), Some(manifest)) =
            (cache["fetchedAtMs"].as_f64(), decode(&cache["manifest"]))
        else {
            return;
        };
        // An older cache never overrides a newer bundled manifest.
        if updated_at_ms(&BUNDLED) > updated_at_ms(&manifest) {
            return;
        }
        state.manifest = Arc::new(manifest);
        state.fetched_at_ms = Some(fetched_at as i64);
    }

    pub(crate) fn current(&self) -> Arc<Value> {
        self.ensure_disk_cache_loaded();
        self.state.lock().unwrap().manifest.clone()
    }

    async fn refresh_with(&self, force: bool, ttl_ms: i64) -> Arc<Value> {
        let _guard = self.refresh_lock.lock().await;
        self.ensure_disk_cache_loaded();
        let now = self.host.now_ms();
        // Future timestamps expire after a clock rollback; the cache crosses restarts.
        let within = |since: Option<i64>, window: i64| {
            since.is_some_and(|since| now >= since && now - since < window)
        };
        {
            let mut state = self.state.lock().unwrap();
            if !force && within(state.fetched_at_ms, ttl_ms) {
                return state.manifest.clone();
            }
            if !force && within(state.last_attempt_ms, MANIFEST_RETRY_MS) {
                return state.manifest.clone();
            }
            if self.host.update_checks_enabled() == Some(false) {
                return state.manifest.clone();
            }
            state.last_attempt_ms = Some(now);
        }
        let fetched = self.host.fetch().await.and_then(|raw| decode(&raw));
        let mut state = self.state.lock().unwrap();
        // A stale CDN copy is rejected like a stale cache.
        let Some(fetched) = fetched.filter(|f| updated_at_ms(f) >= updated_at_ms(&state.manifest))
        else {
            return state.manifest.clone();
        };
        state.manifest = Arc::new(fetched);
        state.fetched_at_ms = Some(now);
        let contents = json!({"fetchedAtMs": now, "manifest": &*state.manifest}).to_string();
        self.host.write_cache(&contents);
        state.manifest.clone()
    }

    /// TTL-gated refresh, as provider checks start it.
    pub(crate) async fn refresh(&self) -> Arc<Value> {
        self.refresh_with(false, MANIFEST_TTL_MS).await
    }

    /// Ignore the TTL and the backoff, as `refreshModels` does.
    pub(crate) async fn force_refresh(&self) -> Arc<Value> {
        self.refresh_with(true, MANIFEST_TTL_MS).await
    }

    /// For a person looking at the model list: tolerates far less staleness.
    pub(crate) async fn refresh_on_demand(&self) -> Arc<Value> {
        self.refresh_with(false, MANIFEST_ON_DEMAND_TTL_MS).await
    }
}

/// The host for the full server: reqwest, the state directory and the
/// settings file.
#[cfg(feature = "native")]
pub(crate) struct NativeHost {
    pub(crate) cache_path: std::path::PathBuf,
    pub(crate) settings: Arc<crate::settings::Settings>,
    pub(crate) http: reqwest::Client,
}

#[cfg(feature = "native")]
impl NativeHost {
    pub(crate) fn new(
        state_dir: &std::path::Path,
        settings: Arc<crate::settings::Settings>,
    ) -> Self {
        Self {
            cache_path: state_dir.join(CACHE_FILE),
            settings,
            http: reqwest::Client::new(),
        }
    }
}

#[cfg(feature = "native")]
impl ManifestHost for NativeHost {
    fn now_ms(&self) -> i64 {
        crate::platform::now_ms()
    }

    fn fetch(&self) -> impl std::future::Future<Output = Option<Value>> + Send {
        let request = self
            .http
            .get(MODEL_MANIFEST_URL)
            .timeout(std::time::Duration::from_millis(FETCH_TIMEOUT_MS));
        async move {
            let response = request.send().await.ok()?.error_for_status().ok()?;
            response.json::<Value>().await.ok()
        }
    }

    fn read_cache(&self) -> Option<String> {
        std::fs::read_to_string(&self.cache_path).ok()
    }

    fn write_cache(&self, contents: &str) {
        let Some(dir) = self.cache_path.parent() else {
            return;
        };
        let write = || -> std::io::Result<()> {
            let mut file = tempfile::NamedTempFile::new_in(dir)?;
            std::io::Write::write_all(&mut file, contents.as_bytes())?;
            file.persist(&self.cache_path).map_err(|e| e.error)?;
            Ok(())
        };
        let _ = write();
    }

    fn update_checks_enabled(&self) -> Option<bool> {
        let settings = self.settings.internal().ok()?;
        Some(
            settings["enableProviderUpdateChecks"]
                .as_bool()
                .unwrap_or(true),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering as AtomicOrdering};

    fn fixture(updated_at: &str, fast_mode: bool) -> Value {
        let mut descriptors = vec![
            json!({"type":"select","id":"effort","label":"Reasoning","extra":1,"options":[
            {"id":"low","label":"Low"},
            {"id":"high","label":"High","isDefault":true},
            {"id":"ultracode","label":"Ultracode"}]}),
        ];
        if fast_mode {
            descriptors.push(json!({"id":"fastMode","label":"Fast Mode","type":"boolean"}));
        }
        json!({
            "version": 1,
            "updatedAt": updated_at,
            "currentModels": {"claudeAgent": ["claude-a"]},
            "providers": {"claudeAgent": {
                "defaults": {"chat": "claude-a"},
                "profiles": {"p": {"capabilities": {"optionDescriptors": descriptors},
                                   "adapter": {"claudeCode": {"effortMap": {"low": null}}}}},
                "models": [
                    {"slug":"claude-a","name":"Claude A","aliases":["a"],"status":"current",
                     "releasedAt":"2026-11-18","profile":"p",
                     "adapter":{"claudeCode":{"minVersion":"2.1.0"}}},
                    {"slug":"claude-old","name":"Claude Old","status":"legacy"}
                ]
            }}
        })
    }

    const NOV_20: i64 = 1_795_132_800_000; // 2026-11-20T00:00:00Z

    #[test]
    fn decodes_and_transforms_claude_models() {
        let manifest = decode(&fixture("2026-11-20T00:00:00Z", true)).unwrap();
        let models = claude_models(&manifest, NOV_20);
        assert_eq!(
            Value::from(models),
            json!([
                {"slug":"claude-a","name":"Claude A","aliases":["a"],"badge":"new","isCustom":false,"isDefault":true,
                 "capabilities":{"optionDescriptors":[
                    {"id":"effort","label":"Reasoning","type":"select","options":[
                        {"id":"low","label":"Low"},{"id":"high","label":"High","isDefault":true},
                        {"id":"xhigh","label":"Extra High"}]},
                    {"id":"ultracode","label":"Ultracode","type":"boolean"},
                    {"id":"fastMode","label":"Fast Mode","type":"boolean"}]}},
                {"slug":"claude-old","name":"Claude Old","isCustom":false,"isLegacy":true,"capabilities":null}
            ])
        );
        // Descriptor keys follow the contract's order, not the file's.
        let text =
            serde_json::to_string(&manifest["providers"]["claudeAgent"]["profiles"]["p"]).unwrap();
        assert!(text.contains(r#"{"id":"effort","label":"Reasoning","type":"select","options""#));
        // The badge lapses after a week.
        let later = claude_models(&manifest, NOV_20 + 7 * 24 * 3600 * 1000);
        assert!(later[0].get("badge").is_none());
    }

    #[test]
    fn ultracode_default_moves_to_xhigh() {
        let caps = json!({"optionDescriptors":[{"id":"effort","label":"R","type":"select","currentValue":"ultracode",
            "options":[{"id":"xhigh","label":"Extra High"},{"id":"ultracode","label":"U","isDefault":true}]}]});
        assert_eq!(
            with_ultracode_toggle(&caps),
            json!({"optionDescriptors":[
                {"id":"effort","label":"R","type":"select","currentValue":"xhigh","options":[{"id":"xhigh","label":"Extra High","isDefault":true}]},
                {"id":"ultracode","label":"Ultracode","type":"boolean","currentValue":true}]})
        );
    }

    #[test]
    fn rejects_invalid_manifests() {
        let mut bad = fixture("2026-11-20T00:00:00Z", false);
        bad["providers"]["claudeAgent"]["models"][0]["profile"] = "missing".into();
        assert!(decode(&bad).is_none());
        let mut bad = fixture("2026-11-20T00:00:00Z", false);
        bad["providers"]["claudeAgent"]["models"][0]["adapter"]["claudeCode"]
            ["maxVersionExclusive"] = "2.0.0".into();
        assert!(decode(&bad).is_none());
        let mut bad = fixture("2026-11-20T00:00:00Z", false);
        bad["version"] = 2.into();
        assert!(decode(&bad).is_none());
    }

    /// The bundled manifest yields the Claude list the reference's pending
    /// provider reports, as recorded in config-defaults.json.
    #[test]
    fn bundled_claude_models_match_reference_defaults() {
        let defaults = &crate::server_config::DEFAULTS["providers"]["claudeAgent"]["models"];
        let expected: Vec<Value> = defaults
            .as_array()
            .unwrap()
            .iter()
            .map(|m| {
                let mut m = m.clone();
                m.as_object_mut().unwrap().shift_remove("badge");
                m
            })
            .collect();
        // Far enough from any release date that no badge applies.
        let models = claude_models(&BUNDLED, 4_000_000_000_000);
        assert_eq!(
            serde_json::to_string(&models).unwrap(),
            serde_json::to_string(&expected).unwrap()
        );
    }

    struct FakeHost {
        now: AtomicI64,
        reply: Mutex<Option<Value>>,
        fetches: AtomicUsize,
        cache: Mutex<Option<String>>,
        checks: Option<bool>,
    }

    impl FakeHost {
        fn new(reply: Option<Value>) -> Self {
            Self {
                now: AtomicI64::new(NOV_20),
                reply: Mutex::new(reply),
                fetches: AtomicUsize::new(0),
                cache: Mutex::new(None),
                checks: Some(true),
            }
        }
        fn advance(&self, ms: i64) {
            self.now.fetch_add(ms, AtomicOrdering::SeqCst);
        }
        fn fetches(&self) -> usize {
            self.fetches.load(AtomicOrdering::SeqCst)
        }
    }

    impl ManifestHost for FakeHost {
        fn now_ms(&self) -> i64 {
            self.now.load(AtomicOrdering::SeqCst)
        }
        fn fetch(&self) -> impl std::future::Future<Output = Option<Value>> + Send {
            self.fetches.fetch_add(1, AtomicOrdering::SeqCst);
            let reply = self.reply.lock().unwrap().clone();
            async move { reply }
        }
        fn read_cache(&self) -> Option<String> {
            self.cache.lock().unwrap().clone()
        }
        fn write_cache(&self, contents: &str) {
            *self.cache.lock().unwrap() = Some(contents.to_owned());
        }
        fn update_checks_enabled(&self) -> Option<bool> {
            self.checks
        }
    }

    const MINUTE: i64 = 60 * 1000;

    #[tokio::test]
    async fn refresh_honours_ttl_backoff_and_cache() {
        let newer = fixture("2099-01-01T00:00:00Z", true);
        let store = ModelManifest::new(FakeHost::new(Some(newer.clone())));
        assert_eq!(store.current(), *BUNDLED);
        // The first refresh fetches, adopts the newer manifest and caches it.
        assert_eq!(store.refresh().await["updatedAt"], "2099-01-01T00:00:00Z");
        assert_eq!(store.host.fetches(), 1);
        let cache: Value = serde_json::from_str(&store.host.read_cache().unwrap()).unwrap();
        assert_eq!(cache["fetchedAtMs"], NOV_20);
        assert_eq!(cache["manifest"]["updatedAt"], "2099-01-01T00:00:00Z");
        // Within the TTL nothing is fetched; on demand allows five minutes.
        store.host.advance(10 * MINUTE);
        store.refresh().await;
        assert_eq!(store.host.fetches(), 1);
        store.refresh_on_demand().await;
        assert_eq!(store.host.fetches(), 2);
        // A forced refresh always fetches.
        store.force_refresh().await;
        assert_eq!(store.host.fetches(), 3);
        // After the TTL a failing fetch keeps the manifest and backs off.
        *store.host.reply.lock().unwrap() = None;
        store.host.advance(61 * MINUTE);
        assert_eq!(store.refresh().await["updatedAt"], "2099-01-01T00:00:00Z");
        assert_eq!(store.host.fetches(), 4);
        store.host.advance(MINUTE);
        store.refresh().await;
        assert_eq!(store.host.fetches(), 4);
        store.host.advance(5 * MINUTE);
        store.refresh().await;
        assert_eq!(store.host.fetches(), 5);
    }

    #[tokio::test]
    async fn cache_and_fetch_never_downgrade() {
        // A cache older than the bundled manifest is ignored.
        let host = FakeHost::new(Some(fixture("2000-01-01T00:00:00Z", false)));
        *host.cache.lock().unwrap() = Some(
            json!({"fetchedAtMs": NOV_20, "manifest": fixture("2000-01-01T00:00:00Z", false)})
                .to_string(),
        );
        let store = ModelManifest::new(host);
        assert_eq!(store.current(), *BUNDLED);
        // So is an older fetched copy, and nothing is written.
        store.host.cache.lock().unwrap().take();
        store.refresh().await;
        assert_eq!(store.host.fetches(), 1);
        assert!(store.host.read_cache().is_none());

        // A fresh newer cache is used without fetching.
        let host = FakeHost::new(None);
        *host.cache.lock().unwrap() = Some(
            json!({"fetchedAtMs": NOV_20 - MINUTE, "manifest": fixture("2099-01-01T00:00:00Z", false)}).to_string(),
        );
        let store = ModelManifest::new(host);
        assert_eq!(store.current()["updatedAt"], "2099-01-01T00:00:00Z");
        store.refresh().await;
        assert_eq!(store.host.fetches(), 0);
    }

    #[tokio::test]
    async fn update_checks_setting_disables_fetch() {
        let mut host = FakeHost::new(Some(fixture("2099-01-01T00:00:00Z", false)));
        host.checks = Some(false);
        let store = ModelManifest::new(host);
        assert_eq!(store.force_refresh().await, *BUNDLED);
        assert_eq!(store.host.fetches(), 0);
    }
}
