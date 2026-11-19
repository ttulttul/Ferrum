use serde_json::{json, Value};
use std::sync::LazyLock;

static CONTRACTS: LazyLock<Value> = LazyLock::new(|| {
    serde_json::from_str(include_str!("../graphql/linear.json"))
        .expect("generated Linear contracts")
});
pub(super) fn query(name: &str) -> &'static str {
    CONTRACTS[name]["query"]
        .as_str()
        .expect("known Linear operation")
}

fn check(value: &Value, schema: &Value, path: &mut String) -> Result<(), String> {
    let problem = |msg: &str, path: &str| {
        if path.is_empty() {
            msg.to_owned()
        } else {
            format!("{msg}\n  at {path}")
        }
    };
    if let Some(s) = schema.get("nullable") {
        return if value.is_null() {
            Ok(())
        } else {
            check(value, s, path)
        };
    }
    if let Some(name) = schema.as_str() {
        let (ok, kind) = match name {
            "ID" | "String" | "DateTime" => (value.is_string(), "string"),
            "Boolean" => (value.is_boolean(), "boolean"),
            "Int" => (
                value.as_i64().and_then(|n| i32::try_from(n).ok()).is_some(),
                "number",
            ),
            "Float" => (value.is_number(), "number"),
            "JSON" | "JSONObject" => (true, "json"),
            _ => (false, name),
        };
        return if ok {
            Ok(())
        } else {
            Err(problem(&format!("Expected {kind}"), path))
        };
    }
    let len = path.len();
    if let Some(items) = schema.as_array() {
        let arr = value
            .as_array()
            .ok_or_else(|| problem("Expected array", path))?;
        for (idx, v) in arr.iter().enumerate() {
            path.push_str(&format!("[{idx}]"));
            check(v, &items[0], path)?;
            path.truncate(len);
        }
        return Ok(());
    }
    let obj = value
        .as_object()
        .ok_or_else(|| problem("Expected object", path))?;
    for (key, s) in schema.as_object().expect("object result shape") {
        path.push_str(&format!("[{}]", json!(key)));
        let field = obj.get(key).ok_or_else(|| problem("Missing key", path))?;
        check(field, s, path)?;
        path.truncate(len);
    }
    Ok(())
}

pub(super) fn decode(op: &str, call: &str, name: &str, data: &Value) -> Result<(), Value> {
    check(data,&CONTRACTS[name]["result"],&mut String::new()).map_err(|msg|{
        let mut err=super::fail(op,call,"failed","Linear returned an unexpected body.");
        err["cause"]["cause"]=json!({"name":"~effect/graphql/GraphQLClientError","message":format!("{name}: DecodeError: {msg}"),
            "cause":{"name":"effect/graphql/GraphQLClientError/DecodeError","message":msg}});
        err
    })
}
