//! Native decoder for the registered Effect schemas. Field order is part of the RPC contract.
use regex::Regex;
use serde::Deserialize;
use serde_json::{json, Map, Value};
use std::{collections::HashSet, sync::LazyLock};
use unicode_normalization::UnicodeNormalization;

// ECMAScript WhiteSpace plus LineTerminator excludes NEXT LINE and includes BOM.
fn trim(s: &str) -> &str {
    s.trim_matches(|c: char| (c.is_whitespace() && c != '\u{0085}') || c == '\u{feff}')
}

#[derive(Deserialize)]
struct Contract {
    nodes: Vec<Node>,
    settings: usize,
    patch: usize,
    request: usize,
    #[serde(rename = "secretCheck")]
    secret_check: usize,
    defaults: Value,
}
#[derive(Deserialize)]
struct Node {
    kind: String,
    source: Option<usize>,
    #[serde(default, rename = "nullOmitted")]
    null_omitted: bool,
    #[serde(default, rename = "nullValue")]
    null_value: Option<Value>,
    #[serde(default)]
    fields: Vec<(String, usize)>,
    #[serde(default)]
    record: Vec<(usize, usize)>,
    #[serde(default)]
    members: Vec<usize>,
    item: Option<usize>,
    #[serde(default)]
    value: Value,
    #[serde(default)]
    trim: bool,
    #[serde(default)]
    optional: bool,
    #[serde(default, deserialize_with = "present")]
    default: Option<Value>,
    fallback: Option<String>,
    #[serde(default)]
    transforms: Vec<String>,
    #[serde(default)]
    checks: Vec<(String, Value, String)>,
    #[serde(default)]
    errors: Map<String, Value>,
}
fn present<'de, D: serde::Deserializer<'de>>(d: D) -> Result<Option<Value>, D::Error> {
    Value::deserialize(d).map(Some)
}
static CONTRACT: LazyLock<Contract> = LazyLock::new(|| {
    serde_json::from_str(include_str!("../../contracts/settings.json"))
        .expect("generated settings schema")
});
static PATTERNS: LazyLock<std::collections::HashMap<String, Regex>> = LazyLock::new(|| {
    CONTRACT
        .nodes
        .iter()
        .flat_map(|n| &n.checks)
        .filter_map(|(_, p, _)| p["source"].as_str())
        .map(|s| {
            (
                s.to_owned(),
                Regex::new(&s.replace("\\/", "/")).expect("generated regex"),
            )
        })
        .collect()
});
pub(super) fn defaults() -> Value {
    CONTRACT.defaults.clone()
}
fn kind(v: &Value) -> &'static str {
    match v {
        Value::Object(_) => "object",
        Value::Bool(_) => "boolean",
        Value::Null => "null",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
    }
}
struct Issue {
    message: String,
    path: String,
}
impl Issue {
    fn new(message: impl Into<String>, path: &str) -> Self {
        Self {
            message: message.into(),
            path: path.into(),
        }
    }
    fn display(self) -> String {
        if self.path.is_empty() {
            self.message
        } else {
            format!("{}\n  at {}", self.message, self.path)
        }
    }
}
impl Node {
    fn has(&self, name: &str) -> bool {
        self.transforms.iter().any(|s| s == name)
    }
    fn accepts(&self, v: &Value) -> bool {
        if let Some(id) = self.source {
            return CONTRACT.nodes[id].accepts(v);
        }
        match self.kind.as_str() {
            "Union" => self.members.iter().any(|id| CONTRACT.nodes[*id].accepts(v)),
            "Literal" => &self.value == v,
            "String" => v.is_string(),
            "Objects" => {
                v.is_object()
                    && self.fields.iter().all(|(name, id)| {
                        let n = &CONTRACT.nodes[*id];
                        n.kind != "Literal"
                            || n.value == v[name]
                            || (n.default.is_some() && v.get(name).is_none())
                    })
            }
            "Arrays" => v.is_array(),
            "Declaration" if self.source.is_none() => true,
            "Number" | "Declaration" => v.is_number(),
            "Null" => v.is_null(),
            "Boolean" => v.is_boolean(),
            "Unknown" => true,
            _ => false,
        }
    }
    fn decode(&self, v: &Value, path: &str) -> Result<Option<Value>, Issue> {
        if v.is_null() && self.null_omitted {
            return Ok(None);
        }
        if v.is_null() && self.null_value.is_some() {
            return Ok(self.null_value.clone());
        }
        if self.fallback.as_deref() == Some("omit") && v.is_null() {
            return Ok(None);
        }
        match self.decode_value(v, path) {
            Ok(v) => Ok(Some(v)),
            Err(_) if self.fallback.is_some() => {
                Ok((self.fallback.as_deref() == Some("null")).then_some(Value::Null))
            }
            Err(e) => Err(e),
        }
    }
    fn decode_value(&self, v: &Value, path: &str) -> Result<Value, Issue> {
        let error = || {
            Issue::new(
                self.errors
                    .get(kind(v))
                    .and_then(Value::as_str)
                    .map(str::to_owned)
                    .unwrap_or_else(|| format!("Expected {}", self.kind.to_lowercase())),
                path,
            )
        };
        let input = if let Some(id) = self.source {
            // Effect checks a transformation's entire input before its target.
            // Preserve its decoded fields (including defaults and trimmed keys).
            Some(CONTRACT.nodes[id].decode_value(v, path)?)
        } else {
            None
        };
        let v = input.as_ref().unwrap_or(v);
        let out = match self.kind.as_str() {
            "Declaration" if self.source.is_none() => v.clone(),
            "Unknown" => v.clone(),
            "Null" if v.is_null() => Value::Null,
            "Literal" if &self.value == v => v.clone(),
            "Boolean" if v.is_boolean() => v.clone(),
            "Number" | "Declaration" if v.is_number() => v.clone(),
            // The Effect JSON codec represents non-finite numbers as literals.
            "Number" | "Declaration"
                if self.source.is_some()
                    && matches!(v.as_str(), Some("Infinity" | "-Infinity" | "NaN")) =>
            {
                if self.kind == "Declaration" && v == "NaN" {
                    json!(0)
                } else {
                    v.clone()
                }
            }
            "String" if v.is_string() => {
                if self.trim {
                    json!(trim(v.as_str().unwrap()))
                } else {
                    v.clone()
                }
            }
            "Arrays" => {
                let values = v.as_array().ok_or_else(error)?;
                let mut out = Vec::new();
                for (i, x) in values.iter().enumerate() {
                    if self.has("tags") && !x.is_string() {
                        continue;
                    }
                    if let Some(value) =
                        CONTRACT.nodes[self.item.unwrap()].decode(x, &format!("{path}[{i}]"))?
                    {
                        out.push(value)
                    }
                }
                Value::Array(out)
            }
            "Objects" => {
                let mut src = v.clone();
                if self.has("model") && src.is_object() {
                    if !src.as_object().unwrap().contains_key("model") {
                        return Err(Issue::new("Missing key", &format!("{path}[\"model\"]")));
                    }
                    if src.get("instanceId").is_none() {
                        if let Some(p) = src["provider"].as_str() {
                            src["instanceId"] = json!(p)
                        } else {
                            return Err(Issue::new(
                                "Expected string",
                                &format!("{path}[\"instanceId\"]"),
                            ));
                        }
                    }
                    if src["options"].is_null() {
                        src.as_object_mut().unwrap().shift_remove("options");
                    }
                }
                if self.has("environment") {
                    return self.environment(v, path);
                }
                let obj = src.as_object().ok_or_else(error)?;
                let mut out = Map::new();
                for (k, id) in &self.fields {
                    let child = &CONTRACT.nodes[*id];
                    let p = format!("{path}[{}]", json!(k));
                    let decoded = match obj.get(k) {
                        Some(val) => child.decode(val, &p)?,
                        None if child.default.is_some() => child.default.clone(),
                        None if child.optional => None,
                        None => return Err(Issue::new("Missing key", &p)),
                    };
                    if let Some(val) = decoded {
                        out.insert(k.clone(), val);
                    }
                }
                for (key_node, val_node) in &self.record {
                    for (k, val) in obj {
                        if self.fields.iter().any(|(key, _)| key == k) {
                            continue;
                        }
                        let p = format!("{path}[{}]", json!(k));
                        let decode_key = || CONTRACT.nodes[*key_node].decode(&json!(k), &p);
                        // Defaulted maps decode the entries before encoding their branded keys.
                        let key = if self.default.is_some() {
                            None
                        } else {
                            decode_key()?
                        };
                        let entry = CONTRACT.nodes[*val_node].decode(val, &p)?;
                        if let (Some(Value::String(key)), Some(v)) = (
                            match key {
                                None => decode_key()?,
                                some => some,
                            },
                            entry,
                        ) {
                            out.insert(key, v);
                        }
                    }
                }
                Value::Object(out)
            }
            "Union" => {
                if self.has("legacyOptions") && v.is_object() {
                    let options = v
                        .as_object()
                        .unwrap()
                        .iter()
                        .filter_map(|(id, value)| {
                            let id = trim(id);
                            if id.is_empty() {
                                return None;
                            }
                            match value {
                                Value::String(s) if !trim(s).is_empty() => {
                                    Some(json!({"id":id,"value":trim(s)}))
                                }
                                Value::Bool(_) => Some(json!({"id":id,"value":value})),
                                _ => None,
                            }
                        })
                        .collect();
                    return Ok(Value::Array(options));
                }
                if self.has("legacyStyle") && v.is_string() {
                    let s = trim(v.as_str().unwrap());
                    return Ok(
                        json!({"mode":if s.is_empty(){"repo_conventions"}else{"custom"},"customInstructions":s,"followChangeRequestTemplates":true}),
                    );
                }
                let candidates = self
                    .members
                    .iter()
                    .map(|i| &CONTRACT.nodes[*i])
                    .filter(|m| m.accepts(v))
                    .collect::<Vec<_>>();
                if candidates.len() == 1 {
                    return candidates[0].decode_value(v, path);
                }
                let mut best = None;
                for id in &self.members {
                    let member = &CONTRACT.nodes[*id];
                    match member.decode(v, path) {
                        Ok(Some(value)) => return Ok(value),
                        Ok(None) => {}
                        Err(e) => {
                            if e.path.len() > path.len()
                                && best
                                    .as_ref()
                                    .is_none_or(|old: &Issue| e.path.len() > old.path.len())
                            {
                                best = Some(e)
                            }
                        }
                    }
                }
                return Err(if self.errors.contains_key(kind(v)) {
                    error()
                } else {
                    best.unwrap_or_else(error)
                });
            }
            _ => return Err(error()),
        };
        self.validate(&out, path)?;
        Ok(out)
    }
    fn environment(&self, v: &Value, path: &str) -> Result<Value, Issue> {
        let mut out = Map::new();
        for (key, id) in &self.fields {
            if key == "valueRedacted" {
                continue;
            }
            let val = match key.as_str() {
                "value" => v.get(key).cloned().unwrap_or(json!("")),
                "sensitive" => v.get(key).cloned().unwrap_or(json!(false)),
                _ => v
                    .get(key)
                    .cloned()
                    .ok_or_else(|| Issue::new("Missing key", &format!("{path}[{}]", json!(key))))?,
            };
            if key == "value" && !val.is_string() {
                return Err(Issue::new("Expected string", &format!("{path}[\"value\"]")));
            }
            if let Some(x) = CONTRACT.nodes[*id].decode(&val, &format!("{path}[{}]", json!(key)))? {
                out.insert(key.clone(), x);
            }
        }
        if let Some(v) = v.get("valueRedacted") {
            if !v.is_boolean() {
                return Err(Issue::new(
                    "Expected boolean",
                    &format!("{path}[\"valueRedacted\"]"),
                ));
            }
            out.insert("valueRedacted".into(), v.clone());
        }
        if let Some(reference) = v.get("secret") {
            let id = self.fields.iter().find(|(k, _)| k == "value").unwrap().1;
            if !reference.is_object() {
                return Err(Issue::new(
                    "Expected object",
                    &format!("{path}[\"secret\"]"),
                ));
            }
            let r = CONTRACT.nodes[id].decode_value(reference, &format!("{path}[\"secret\"]"))?;
            out.insert("value".into(), json!(""));
            out.insert("sensitive".into(), json!(true));
            out.insert("secret".into(), r);
        }
        Ok(out.into())
    }
    fn validate(&self, v: &Value, path: &str) -> Result<(), Issue> {
        let length = || {
            v.as_array()
                .map(Vec::len)
                .or_else(|| v.as_str().map(|s| s.encode_utf16().count()))
                .unwrap_or(0) as u64
        };
        for (check, p, expected) in &self.checks {
            let mut message = format!("Expected {expected}");
            let valid = match check.as_str() {
                "isMaxLength" => length() <= p["maxLength"].as_u64().unwrap(),
                "isMinLength" => length() >= p["minLength"].as_u64().unwrap(),
                "isPattern" => {
                    PATTERNS[p["source"].as_str().unwrap()].is_match(v.as_str().unwrap_or(""))
                }
                "isBetween" => {
                    p["minimum"].as_f64() <= v.as_f64() && v.as_f64() <= p["maximum"].as_f64()
                }
                "isGreaterThanOrEqualTo" => v.as_f64() >= p["minimum"].as_f64(),
                "isGreaterThan" => v.as_f64() > p["exclusiveMinimum"].as_f64(),
                "isInt" => v
                    .as_f64()
                    .is_some_and(|x| x.fract() == 0.0 && x.abs() <= 9007199254740991.0),
                "isFinite" => true,
                "filter" if expected == "a non-blank string" => {
                    !trim(v.as_str().unwrap_or("")).is_empty()
                }
                "filter" if v.is_array() => {
                    let rows = v.as_array().unwrap();
                    let mut keys = HashSet::new();
                    if rows.first().is_some_and(|r| r.get("term").is_some()) {
                        message =
                            "Dictionary terms and aliases must be unique and nonempty.".into();
                        rows.iter()
                            .flat_map(|r| {
                                std::iter::once(&r["term"])
                                    .chain(r["aliases"].as_array().into_iter().flatten())
                            })
                            .all(|s| {
                                let k = s
                                    .as_str()
                                    .unwrap()
                                    .trim()
                                    .nfc()
                                    .collect::<String>()
                                    .to_lowercase();
                                !k.is_empty() && keys.insert(k)
                            })
                    } else {
                        message = "Device host ids must be unique.".into();
                        rows.iter().all(|r| keys.insert(r["id"].to_string()))
                    }
                }
                "filter" if v.is_object() => {
                    message = "Idle minutes must be smaller than session minutes.".into();
                    v["idleMinutes"].as_i64() < v["maxSessionMinutes"].as_i64()
                }
                "filter" if self.checks.iter().any(|(c, _, _)| c == "isPattern") => {
                    message = "The local host id is reserved.".into();
                    v != "local"
                }
                "filter" => {
                    message = "Expected <filter>".into();
                    url::Url::parse(v.as_str().unwrap_or("")).is_ok_and(|u| {
                        matches!(u.scheme(), "http" | "https")
                            && u.username().is_empty()
                            && u.password().is_none()
                            && u.query().is_none()
                            && u.fragment().is_none()
                    })
                }
                _ => false,
            };
            if !valid {
                return Err(Issue::new(message, path));
            }
        }
        Ok(())
    }
}
fn decode(id: usize, v: &Value) -> Result<Value, String> {
    CONTRACT.nodes[id]
        .decode_value(v, "")
        .map_err(Issue::display)
}
pub(super) fn persisted_field(name: &str, value: &Value) -> Option<Value> {
    let (_, node) = CONTRACT.nodes[CONTRACT.settings]
        .fields
        .iter()
        .find(|(key, _)| key == name)?;
    CONTRACT.nodes[*node].decode(value, "").ok().flatten()
}
pub(crate) fn normalize(v: &Value) -> Result<Value, String> {
    decode(CONTRACT.settings, v)
}
// DurationFromMillis exposes infinity over RPC. JSON.stringify encodes the same
// number as null in settings.json. Keep this conversion confined to duration slots.
const DURATIONS: [&str; 6] = [
    "/automaticGitFetchInterval",
    "/providerHealthRefreshInterval",
    "/backgroundActivity/overrides/automaticGitFetchInterval",
    "/backgroundActivity/overrides/providerHealthRefreshInterval",
    "/backgroundActivity/overrides/hostPowerMonitorActiveInterval",
    "/backgroundActivity/overrides/hostPowerMonitorIdleInterval",
];
pub(crate) fn normalize_runtime(v: &Value) -> Result<Value, String> {
    let infinities = DURATIONS
        .iter()
        .filter_map(|p| {
            v.pointer(p)?
                .as_str()
                .filter(|s| matches!(*s, "Infinity" | "-Infinity"))
                .map(|s| (*p, s))
        })
        .collect::<Vec<_>>();
    if infinities.is_empty() {
        return normalize(v);
    }
    let mut finite = v.clone();
    for (p, _) in &infinities {
        *finite.pointer_mut(p).unwrap() = json!(0);
    }
    let mut normalized = normalize(&finite)?;
    for (p, value) in infinities {
        *normalized.pointer_mut(p).unwrap() = json!(value);
    }
    Ok(normalized)
}
pub(super) fn encode_disk(mut settings: Value) -> Value {
    for p in DURATIONS {
        if let Some(v) = settings.pointer_mut(p) {
            if matches!(v.as_str(), Some("Infinity" | "-Infinity")) {
                *v = Value::Null
            }
        }
    }
    settings
}
pub(super) fn decode_patch(v: &Value) -> Result<Value, String> {
    decode(CONTRACT.patch, v)
}
pub(crate) fn secret_check(v: &Value) -> Result<Value, String> {
    decode(CONTRACT.secret_check, v)
}

pub(crate) fn request(v: &Value) -> Result<Value, String> {
    decode(CONTRACT.request, v)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    #[ignore = "requires the corpus from scripts/settings_schema_cases.mjs"]
    fn upstream_schema_corpus() {
        let file =
            std::fs::read_to_string(std::env::var("T3_SETTINGS_SCHEMA_CASES").unwrap()).unwrap();
        let mut mismatches = Vec::new();
        for line in file.lines() {
            let case: Value = serde_json::from_str(line).unwrap();
            let settings = case["mode"] == "settings";
            let input = &case["input"];
            let key = case["key"].as_str().unwrap();
            let result = if settings {
                normalize(input)
            } else {
                request(input)
            };
            let actual = match result {
                Ok(v) => {
                    let view = if settings { &v } else { &v["patch"] };
                    match view.get(key) {
                        Some(value) => json!({"value":value}),
                        None => json!({"omitted":true}),
                    }
                }
                Err(message) => json!({"error":message}),
            };
            if actual != case["outcome"] {
                mismatches.push(format!(
                    "{} {} expected={} actual={}",
                    case["mode"], input, case["outcome"], actual
                ))
            }
        }
        assert!(
            mismatches.is_empty(),
            "{} mismatches\n{}",
            mismatches.len(),
            mismatches.join("\n")
        );
    }
}
