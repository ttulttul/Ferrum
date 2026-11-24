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

/// A path as effect prints it: keys quoted, array indexes (`#<n>` here) bare.
fn at(path: &[&str]) -> String {
    path.iter()
        .map(|key| match key.strip_prefix('#') {
            Some(index) if index.parse::<usize>().is_ok() => format!("[{index}]"),
            _ => format!("[{}]", json!(key)),
        })
        .collect()
}

/// Fields whose range the reference checks with `isBetween`, which words both
/// ends as one ("a value between 1 and 50"); other ranges are two checks, and
/// the advertised schema cannot tell them apart.
const BETWEEN: &[(&str, &str)] = &[
    ("t3_thread_search", "limit"),
    ("preview_resize", "width"),
    ("preview_resize", "height"),
];

/// A union member: its `kind`, and its required fields in order.
type Member = (&'static str, &'static [&'static str]);

/// Fields the reference declares as unions tagged by `kind`, each member's
/// required fields in order: the advertised schema merges the members.
const TAGGED: &[(&str, &str, &[Member])] = &[(
    "t3_project_update",
    "projectIcon",
    &[
        ("lucide", &["name", "color"]),
        ("emoji", &["emoji"]),
        ("monogram", &["text", "color"]),
    ],
)];

/// The first issue in `tool`'s argument `value` against `schema`, at `path`.
fn issue(
    tool: &str,
    schema: &Value,
    value: &Value,
    optional: bool,
    path: &[&str],
) -> Option<String> {
    let between = matches!(path, [field] if BETWEEN.contains(&(tool, *field)));
    let expected = |what: String| Some(format!("Expected {what}\n  at {}", at(path)));
    if let Some(allowed) = schema["enum"].as_array() {
        if !allowed.contains(value) {
            // An optional literal union reads without `| null`, unlike other types.
            let options: Vec<String> = allowed.iter().map(Value::to_string).collect();
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
        if tool == "preview_resize" && schema["type"] == "integer" {
            if n.fract() != 0.0 || n.abs() > 9_007_199_254_740_991.0 {
                return expected("an integer".into());
            }
            if schema["exclusiveMinimum"] == 0 && n <= 0.0 {
                return expected("a value greater than 0".into());
            }
        }
        if let (Some(min), Some(max), true) = (
            schema["minimum"].as_f64(),
            schema["maximum"].as_f64(),
            between,
        ) {
            if n < min || n > max {
                return expected(format!("a value between {min} and {max}"));
            }
        }
        if let Some(min) = schema["minimum"].as_f64().filter(|min| n < *min) {
            return expected(format!("a value greater than or equal to {min}"));
        }
        if let Some(max) = schema["maximum"].as_f64().filter(|max| n > *max) {
            return expected(format!("a value less than or equal to {max}"));
        }
    }
    if let Some(text) = value.as_str() {
        let trimmed = crate::settings::schema::trim(text);
        if tool == "preview_evaluate" && path == ["expression"] && trimmed != text {
            return expected("a string with no leading or trailing whitespace".into());
        }
        if matches!(
            tool,
            "preview_evaluate"
                | "preview_set_appearance"
                | "preview_resize"
                | "preview_open"
                | "preview_navigate"
                | "preview_snapshot"
                | "preview_act"
                | "preview_wait_for"
        ) && path == ["tabId"]
        {
            if trimmed.is_empty() {
                return expected("a non-blank string".into());
            }
            if trimmed.encode_utf16().count() > 128 {
                return expected("a value with a length of at most 128".into());
            }
        }
        if tool == "device_close" && matches!(path, ["deviceId"] | ["hostId"]) {
            if trimmed.is_empty() {
                return expected("a non-blank string".into());
            }
            if path == ["hostId"] && trimmed.encode_utf16().count() > 128 {
                return expected("a value with a length of at most 128".into());
            }
        }
        let length = text.encode_utf16().count() as f64;
        if let Some(max) = schema["maxLength"].as_f64().filter(|max| length > *max) {
            return expected(format!("a value with a length of at most {max}"));
        }
        if let Some(min) = schema["minLength"].as_f64().filter(|min| length < *min) {
            return expected(format!("a value with a length of at least {min}"));
        }
    }
    if let Some(items) = value.as_array() {
        let count = items.len() as f64;
        if let Some(min) = schema["minItems"].as_f64().filter(|min| count < *min) {
            return expected(format!("a value with a length of at least {min}"));
        }
        if let Some(max) = schema["maxItems"].as_f64().filter(|max| count > *max) {
            return expected(format!("a value with a length of at most {max}"));
        }
        if let Some(item) = schema.get("items").filter(|i| i.is_object()) {
            for (index, value) in items.iter().enumerate() {
                let segment = format!("#{index}");
                let mut inner = path.to_vec();
                inner.push(&segment);
                if let Some(problem) = issue(tool, item, value, false, &inner) {
                    return Some(problem);
                }
            }
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
                    if let Some(problem) = issue(tool, property, found, !is_required, &inner) {
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
    let Some(tool) = tool_definition(name) else {
        return Ok(());
    };
    let empty = json!({});
    let args = if args.is_null() { &empty } else { args };
    // A field the reference declares as a tagged union reads as one, whatever is wrong in it.
    for (field, expected) in union_fields {
        if let Some(value) = args.get(*field).filter(|v| !v.is_null()) {
            if issue(
                name,
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
    for (_, field, members) in TAGGED.iter().filter(|(tool, _, _)| *tool == name) {
        let Some(object) = args.get(*field).and_then(Value::as_object) else {
            continue;
        };
        let kind = object.get("kind").and_then(Value::as_str);
        if let Some((_, required)) = members.iter().find(|(tag, _)| Some(*tag) == kind) {
            if let Some(missing) = required.iter().find(|key| !object.contains_key(**key)) {
                return Err(refusal(
                    name,
                    &format!("Missing key\n  at {}", at(&[field, missing])),
                ));
            }
        }
    }
    match issue(name, &tool["inputSchema"], args, false, &[]) {
        Some(problem) => Err(refusal(name, &problem)),
        None if name == "preview_resize" => {
            check_resize(args).map_err(|message| refusal(name, message))
        }
        None => Ok(()),
    }
}

fn check_resize(args: &Value) -> Result<(), &'static str> {
    let width = args["width"].as_f64();
    let height = args["height"].as_f64();
    let preset = args["preset"].is_string();
    let orientation = args["orientation"].is_string();
    if width.is_some() != height.is_some() {
        return Err("Custom dimensions require both width and height.");
    }
    match args["mode"].as_str() {
        Some("fill") if preset || width.is_some() || orientation => {
            Err("Fill mode does not accept a preset, dimensions, or orientation.")
        }
        Some("freeform") if width.is_none() || preset || orientation => Err(
            "Freeform mode requires width and height and does not accept a preset or orientation.",
        ),
        Some("preset") if !preset || width.is_some() => {
            Err("Preset mode requires a preset and does not accept custom dimensions.")
        }
        _ if width.zip(height).is_some_and(|(w, h)| w * h > 8_294_400.0) => {
            Err("Custom viewport area must not exceed 8294400 pixels.")
        }
        _ => Ok(()),
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
            message("t3_thread_search", json!({"query": "x", "limit": 51})),
            "Invalid parameters for tool 't3_thread_search': Expected a value between 1 and 50\n  at [\"limit\"]"
        );
        assert_eq!(
            message("t3_worktree_list", json!({"limit": 0})),
            "Invalid parameters for tool 't3_worktree_list': Expected a value greater than or equal to 1\n  at [\"limit\"]"
        );
        assert_eq!(
            message(
                "t3_project_update",
                json!({"projectId": "p", "projectIcon": {"kind": "monogram", "text": "ABC"}})
            ),
            "Invalid parameters for tool 't3_project_update': Missing key\n  at [\"projectIcon\"][\"color\"]"
        );
        assert_eq!(
            message("t3_queue", json!({"action": "list", "threadId": null})),
            "accepted"
        );
    }
}
