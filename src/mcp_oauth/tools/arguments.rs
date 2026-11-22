//! A tool's arguments checked against the input schema it advertises, with
//! the reference's refusal: `Invalid parameters for tool '<name>': <issue>`,
//! as effect's Schema words the first issue of the advertised codec, which
//! takes `null` for an optional field.
use super::*;

/// How effect names a JSON Schema type in "Expected ...".
fn type_name(schema: &Value) -> Option<&'static str> {
    match schema["type"].as_str()? {
        "string" => Some("string"),
        "integer" | "number" => Some("number"),
        "boolean" => Some("boolean"),
        "object" => Some("object"),
        "array" => Some("array"),
        _ => None,
    }
}

fn fits(schema: &Value, value: &Value) -> bool {
    match schema["type"].as_str() {
        Some("string") => value.is_string(),
        Some("integer" | "number") => value.is_number(),
        Some("boolean") => value.is_boolean(),
        Some("object") => value.is_object(),
        Some("array") => value.is_array(),
        _ => true,
    }
}

fn at(path: &[&str]) -> String {
    path.iter().map(|key| format!("[{}]", json!(key))).collect()
}

/// The first issue in `value` against `schema`, at `path`.
fn issue(schema: &Value, value: &Value, optional: bool, path: &[&str]) -> Option<String> {
    let expected = |what: String| Some(format!("Expected {what}\n  at {}", at(path)));
    if let Some(allowed) = schema["enum"].as_array() {
        if !allowed.contains(value) {
            let mut options: Vec<String> = allowed.iter().map(Value::to_string).collect();
            if optional {
                options.push("null".into());
            }
            return expected(options.join(" | "));
        }
        return None;
    }
    if !fits(schema, value) {
        let name = type_name(schema)?;
        return expected(if optional {
            format!("{name} | null")
        } else {
            name.to_owned()
        });
    }
    if let Some(n) = value.as_f64() {
        if let Some(min) = schema["minimum"].as_f64().filter(|min| n < *min) {
            return expected(format!("a value greater than or equal to {min}"));
        }
        if let Some(max) = schema["maximum"].as_f64().filter(|max| n > *max) {
            return expected(format!("a value less than or equal to {max}"));
        }
    }
    if let Some(text) = value.as_str() {
        let length = text.encode_utf16().count() as f64;
        if let Some(max) = schema["maxLength"].as_f64().filter(|max| length > *max) {
            return expected(format!("a value with a length of at most {max}"));
        }
        if let Some(min) = schema["minLength"].as_f64().filter(|min| length < *min) {
            return expected(format!("a value with a length of at least {min}"));
        }
    }
    if let (Some(object), Some(properties)) = (value.as_object(), schema["properties"].as_object())
    {
        let required: Vec<&str> = schema["required"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(Value::as_str)
            .collect();
        for (key, property) in properties {
            let mut inner = path.to_vec();
            inner.push(key);
            let is_required = required.contains(&key.as_str());
            match object.get(key) {
                None if is_required => {
                    return Some(format!("Missing key\n  at {}", at(&inner)));
                }
                None => {}
                Some(Value::Null) if !is_required => {}
                Some(found) => {
                    if let Some(problem) = issue(property, found, !is_required, &inner) {
                        return Some(problem);
                    }
                }
            }
        }
    }
    None
}

/// Refuses arguments `name`'s advertised input schema does not take, as the
/// reference's MCP server does before the tool runs.
pub(super) fn check(
    name: &str,
    args: &Value,
    union_fields: &[(&str, &str)],
) -> Result<(), Refusal> {
    let Some(tool) = TOOLS
        .as_array()
        .and_then(|tools| tools.iter().find(|t| t["name"] == name))
    else {
        return Ok(());
    };
    let empty = json!({});
    let args = if args.is_null() { &empty } else { args };
    // A field the reference declares as a tagged union reads as one, whatever is wrong in it.
    for (field, expected) in union_fields {
        if let Some(value) = args.get(*field).filter(|v| !v.is_null()) {
            if issue(
                &tool["inputSchema"]["properties"][*field],
                value,
                true,
                &[field],
            )
            .is_some()
            {
                return Err(refusal(
                    name,
                    &format!("Expected {expected}\n  at {}", at(&[field])),
                ));
            }
        }
    }
    match issue(&tool["inputSchema"], args, false, &[]) {
        Some(problem) => Err(refusal(name, &problem)),
        None => Ok(()),
    }
}

fn refusal(name: &str, problem: &str) -> Refusal {
    Refusal::Rpc(
        -32602,
        format!("Invalid parameters for tool '{name}': {problem}"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn message(name: &str, args: Value) -> String {
        match check(name, &args, &[]) {
            Err(Refusal::Rpc(_, message)) => message,
            _ => "accepted".into(),
        }
    }

    #[test]
    fn words_issues_as_the_reference_does() {
        assert_eq!(
            message("t3_queue", json!({})),
            "Invalid parameters for tool 't3_queue': Missing key\n  at [\"action\"]"
        );
        assert_eq!(
            message("t3_queue", json!({"action": "list", "limit": "x"})),
            "Invalid parameters for tool 't3_queue': Expected number | null\n  at [\"limit\"]"
        );
        assert_eq!(
            message("t3_queue", json!({"action": "list", "cursor": -1})),
            "Invalid parameters for tool 't3_queue': Expected a value greater than or equal to 0\n  at [\"cursor\"]"
        );
        assert_eq!(
            message("t3_thread_configure", json!({"modelSelection": {}})),
            "Invalid parameters for tool 't3_thread_configure': Missing key\n  at [\"modelSelection\"][\"model\"]"
        );
        assert_eq!(
            message("t3_queue", json!({"action": "list", "threadId": null})),
            "accepted"
        );
    }
}
