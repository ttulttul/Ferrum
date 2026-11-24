//! A run asked to return its result as data, as the reference's
//! `runOutput` handles it: the send's `outputSchema` (a JSON Schema) is
//! checked, its instructions follow the turn's prompt, and what the agent
//! submits with `t3_submit_output` is checked against it before it is kept.
//! The checks and their wording are those of the schema the reference
//! compiles from the JSON Schema (effect's decoder, all issues, excess keys
//! refused where the schema closes an object).
use serde_json::{json, Value};

/// `MAX_SCHEMA_CHARS`: the schema travels with the run and into each prompt.
const MAX_SCHEMA_CHARS: usize = 32_000;

/// Whether `schema` uses a keyword the reference's compiler cannot take.
fn unusable(schema: &Value) -> bool {
    match schema {
        Value::Object(fields) => fields
            .iter()
            .any(|(key, value)| key == "pattern" || key == "patternProperties" || unusable(value)),
        Value::Array(items) => items.iter().any(unusable),
        _ => false,
    }
}

/// `outputSchemaProblem`.
fn schema_problem(schema: &Value) -> Option<String> {
    if crate::wire_json::compact(schema).encode_utf16().count() > MAX_SCHEMA_CHARS {
        return Some(format!(
            "The output schema is larger than {MAX_SCHEMA_CHARS} characters."
        ));
    }
    unusable(schema).then(|| "The output schema is not a usable JSON Schema.".to_owned())
}

/// `isProviderCommand`: a command its provider runs itself.
pub(crate) fn is_provider_command(text: &str, attachments: usize) -> bool {
    if attachments > 0 {
        return false;
    }
    use std::sync::LazyLock;
    // `parseGoalCommand`.
    static GOAL: LazyLock<regex::Regex> =
        LazyLock::new(|| regex::Regex::new(r"^/goal(?:\s+[\s\S]*)?$").expect("pattern"));
    let lower = text.trim().to_lowercase();
    lower == "/compact" || lower == "/logout" || GOAL.is_match(text.trim())
}

/// `outputRequestProblem`: why a message cannot be asked for a result.
pub(crate) fn request_problem(schema: &Value, text: &str, attachments: usize) -> Option<String> {
    if is_provider_command(text, attachments) {
        return Some("A command cannot be asked for a result. Ask for it in a message.".into());
    }
    schema_problem(schema)
}

/// `outputInstructions`: appended to the turn's prompt.
pub(crate) fn instructions(schema: &Value) -> String {
    [
        "A program reads the result of this turn, not a person.".to_owned(),
        "When the work is done, call the `t3_submit_output` tool once with `json` set to JSON text matching this JSON Schema:".to_owned(),
        crate::wire_json::compact(schema),
        "If you could not finish, still submit, with the fields that say so. A rejected submission names what did not fit: correct it and submit again.".to_owned(),
    ]
    .join("\n")
}

fn path_text(path: &[Value]) -> String {
    path.iter()
        .map(|key| format!("[{}]", crate::wire_json::compact(key)))
        .collect()
}

struct Issues(Vec<(String, Vec<Value>)>);

impl Issues {
    fn push(&mut self, message: impl Into<String>, path: &[Value]) {
        self.0.push((message.into(), path.to_vec()));
    }
}

fn type_name(kind: &str) -> &str {
    match kind {
        "integer" => "number",
        other => other,
    }
}

/// How the decoder names what a schema expects, in a union.
fn describe(schema: &Value) -> String {
    if let Some(values) = schema["enum"].as_array() {
        return values
            .iter()
            .map(crate::wire_json::compact)
            .collect::<Vec<_>>()
            .join(" | ");
    }
    if let Some(value) = schema.get("const") {
        return crate::wire_json::compact(value);
    }
    match &schema["type"] {
        Value::String(kind) => type_name(kind).to_owned(),
        Value::Array(kinds) => kinds
            .iter()
            .filter_map(Value::as_str)
            .map(type_name)
            .collect::<Vec<_>>()
            .join(" | "),
        _ => {
            for key in ["anyOf", "oneOf"] {
                if let Some(members) = schema[key].as_array() {
                    return members.iter().map(describe).collect::<Vec<_>>().join(" | ");
                }
            }
            "unknown".into()
        }
    }
}

fn is_type(kind: &str, value: &Value) -> bool {
    match kind {
        "string" => value.is_string(),
        "number" | "integer" => value.is_number(),
        "boolean" => value.is_boolean(),
        "null" => value.is_null(),
        "array" => value.is_array(),
        "object" => value.is_object(),
        _ => true,
    }
}

/// Whether `value` matches `schema` without reporting why.
fn matches(schema: &Value, value: &Value) -> bool {
    let mut issues = Issues(Vec::new());
    check(schema, value, &[], &mut issues);
    issues.0.is_empty()
}

fn check(schema: &Value, value: &Value, path: &[Value], issues: &mut Issues) {
    if !schema.is_object() {
        return;
    }
    for key in ["anyOf", "oneOf"] {
        if let Some(members) = schema[key].as_array() {
            if !members.iter().any(|member| matches(member, value)) {
                issues.push(format!("Expected {}", describe(schema)), path);
            }
            return;
        }
    }
    if let Some(values) = schema["enum"].as_array() {
        if !values.contains(value) {
            issues.push(format!("Expected {}", describe(schema)), path);
        }
        return;
    }
    if let Some(expected) = schema.get("const") {
        if expected != value {
            issues.push(format!("Expected {}", describe(schema)), path);
        }
        return;
    }
    match &schema["type"] {
        Value::Array(kinds)
            if !kinds
                .iter()
                .filter_map(Value::as_str)
                .any(|kind| is_type(kind, value)) =>
        {
            issues.push(format!("Expected {}", describe(schema)), path);
            return;
        }
        Value::String(kind) => {
            if !is_type(kind, value) {
                issues.push(format!("Expected {}", type_name(kind)), path);
                return;
            }
            if kind == "integer" && value.as_f64().is_some_and(|n| n.fract() != 0.0) {
                issues.push("Expected an integer", path);
                return;
            }
        }
        _ => {}
    }
    match value {
        Value::Number(number) => {
            let n = number.as_f64().unwrap_or_default();
            let bound = |key: &str| schema[key].as_f64();
            let show = |key: &str| crate::wire_json::compact(&schema[key]);
            if bound("minimum").is_some_and(|m| n < m) {
                issues.push(
                    format!(
                        "Expected a value greater than or equal to {}",
                        show("minimum")
                    ),
                    path,
                );
            } else if bound("exclusiveMinimum").is_some_and(|m| n <= m) {
                issues.push(
                    format!("Expected a value greater than {}", show("exclusiveMinimum")),
                    path,
                );
            } else if bound("maximum").is_some_and(|m| n > m) {
                issues.push(
                    format!("Expected a value less than or equal to {}", show("maximum")),
                    path,
                );
            } else if bound("exclusiveMaximum").is_some_and(|m| n >= m) {
                issues.push(
                    format!("Expected a value less than {}", show("exclusiveMaximum")),
                    path,
                );
            }
        }
        Value::String(text) => {
            let length = text.chars().count() as u64;
            if schema["minLength"].as_u64().is_some_and(|m| length < m) {
                issues.push(
                    format!(
                        "Expected a string with at least {} code points",
                        schema["minLength"]
                    ),
                    path,
                );
            } else if schema["maxLength"].as_u64().is_some_and(|m| length > m) {
                issues.push(
                    format!(
                        "Expected a string with at most {} code points",
                        schema["maxLength"]
                    ),
                    path,
                );
            }
        }
        Value::Array(items) => {
            let length = items.len() as u64;
            if schema["minItems"].as_u64().is_some_and(|m| length < m) {
                issues.push(
                    format!(
                        "Expected a value with a length of at least {}",
                        schema["minItems"]
                    ),
                    path,
                );
                return;
            }
            if schema["maxItems"].as_u64().is_some_and(|m| length > m) {
                issues.push(
                    format!(
                        "Expected a value with a length of at most {}",
                        schema["maxItems"]
                    ),
                    path,
                );
                return;
            }
            if schema["items"].is_object() {
                for (index, item) in items.iter().enumerate() {
                    let mut at = path.to_vec();
                    at.push(json!(index));
                    check(&schema["items"], item, &at, issues);
                }
            }
        }
        Value::Object(fields) => {
            let properties = schema["properties"].as_object();
            let declared = |key: &str| properties.is_some_and(|p| p.contains_key(key));
            // A closed object reports its excess keys first.
            if schema["additionalProperties"] == false {
                for key in fields.keys().filter(|key| !declared(key)) {
                    let mut at = path.to_vec();
                    at.push(json!(key));
                    issues.push("Expected no excess property", &at);
                }
            }
            let required: Vec<&str> = schema["required"]
                .as_array()
                .map(|r| r.iter().filter_map(Value::as_str).collect())
                .unwrap_or_default();
            let mut keys: Vec<&str> = properties
                .map(|p| p.keys().map(String::as_str).collect())
                .unwrap_or_default();
            keys.extend(required.iter().filter(|key| !declared(key)));
            for key in keys {
                let mut at = path.to_vec();
                at.push(json!(key));
                match fields.get(key) {
                    None if required.contains(&key) => issues.push("Missing key", &at),
                    None => {}
                    Some(field) => {
                        if let Some(property) = properties.and_then(|p| p.get(key)) {
                            check(property, field, &at, issues);
                        }
                    }
                }
            }
            if schema["additionalProperties"].is_object() {
                for (key, field) in fields.iter().filter(|(key, _)| !declared(key)) {
                    let mut at = path.to_vec();
                    at.push(json!(key));
                    check(&schema["additionalProperties"], field, &at, issues);
                }
            }
        }
        _ => {}
    }
}

/// `checkOutput`: the submitted value, kept as submitted, or the rejection
/// as told to the agent (`rejectionText`).
pub(crate) fn check_output(schema: &Value, json: &str) -> Result<Value, String> {
    let Ok(value) = serde_json::from_str::<Value>(json) else {
        return Err("`json` is not valid JSON text.".into());
    };
    if unusable(schema) {
        return Err("The output schema is not a usable JSON Schema.".into());
    }
    let mut issues = Issues(Vec::new());
    check(schema, &value, &[], &mut issues);
    if issues.0.is_empty() {
        return Ok(value);
    }
    let detail: Vec<String> = issues
        .0
        .iter()
        .map(|(message, path)| {
            if path.is_empty() {
                message.clone()
            } else {
                format!("{message}\n  at {}", path_text(path))
            }
        })
        .collect();
    let detail: String = detail.join("\n").chars().take(2000).collect();
    Err(format!("The result does not match the schema.\n{detail}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn submissions_are_checked_with_the_references_wording() {
        let schema = json!({"type":"object","properties":{"answer":{"type":"string"},"count":{"type":"integer","minimum":0},"tags":{"type":"array","items":{"type":"string"}},"mode":{"enum":["a","b"]}},"required":["answer","count"],"additionalProperties":false});
        assert!(check_output(&schema, r#"{"answer":"x","count":2}"#).is_ok());
        assert_eq!(
            check_output(&schema, "not json").unwrap_err(),
            "`json` is not valid JSON text."
        );
        assert_eq!(
            check_output(&schema, r#"{"answer":3}"#).unwrap_err(),
            "The result does not match the schema.\nExpected string\n  at [\"answer\"]\nMissing key\n  at [\"count\"]"
        );
        assert_eq!(
            check_output(&schema, r#"{"answer":"x","count":-1,"extra":1}"#).unwrap_err(),
            "The result does not match the schema.\nExpected no excess property\n  at [\"extra\"]\nExpected a value greater than or equal to 0\n  at [\"count\"]"
        );
        assert_eq!(
            check_output(&schema, r#"{"answer":"x","count":1.5,"tags":[1],"mode":"c"}"#).unwrap_err(),
            "The result does not match the schema.\nExpected an integer\n  at [\"count\"]\nExpected string\n  at [\"tags\"][0]\nExpected \"a\" | \"b\"\n  at [\"mode\"]"
        );
        assert_eq!(
            check_output(&schema, "[]").unwrap_err(),
            "The result does not match the schema.\nExpected object"
        );
        assert_eq!(
            check_output(
                &json!({"anyOf":[{"type":"string"},{"type":"number"}]}),
                "true"
            )
            .unwrap_err(),
            "The result does not match the schema.\nExpected string | number"
        );
        assert_eq!(
            request_problem(&json!({"type":"string","pattern":"a"}), "hi", 0).as_deref(),
            Some("The output schema is not a usable JSON Schema.")
        );
        assert!(request_problem(&json!({}), "/compact", 0).is_some());
    }
}
