use super::DEFAULTS;
use crate::persistence::DocumentStore;
#[cfg(feature = "native")]
use crate::persistence::WriteMode;
use serde_json::{json, Value};
use std::path::Path;

fn shortcut(key: &str) -> Option<Value> {
    let lower = key.to_lowercase();
    let mut tokens = lower.split('+').map(str::trim).collect::<Vec<_>>();
    if tokens.last() == Some(&"") {
        while tokens.last() == Some(&"") {
            tokens.pop();
        }
        tokens.push("+");
    }
    let mut shortcut = json!({"key":null,"metaKey":false,"ctrlKey":false,"shiftKey":false,"altKey":false,"modKey":false});
    for token in tokens {
        let flag = match token {
            "cmd" | "meta" => "metaKey",
            "ctrl" | "control" => "ctrlKey",
            "alt" | "option" => "altKey",
            "shift" => "shiftKey",
            "mod" => "modKey",
            "" => return None,
            _ if !shortcut["key"].is_null() => return None,
            _ => {
                shortcut["key"] = json!(match token {
                    "space" => " ",
                    "esc" => "escape",
                    s => s,
                });
                continue;
            }
        };
        shortcut[flag] = true.into();
    }
    (!shortcut["key"].is_null()).then_some(shortcut)
}

fn expression(text: &str) -> Option<Value> {
    struct Parser<'a>(&'a str);
    impl Parser<'_> {
        fn take(&mut self, prefix: &str) -> bool {
            self.0 = self.0.trim_start();
            if let Some(rest) = self.0.strip_prefix(prefix) {
                self.0 = rest;
                true
            } else {
                false
            }
        }
        fn parse(&mut self, level: u8, depth: u8) -> Option<Value> {
            if depth > 64 {
                return None;
            }
            if level < 2 {
                let mut left = self.parse(level + 1, depth)?;
                let (operator, node) = if level == 0 {
                    ("||", "or")
                } else {
                    ("&&", "and")
                };
                while self.take(operator) {
                    left = json!({"type":node,"left":left,"right":self.parse(level+1,depth)?});
                }
                return Some(left);
            }
            let mut negations = 0;
            while self.take("!") {
                negations += 1;
                if negations > 64 {
                    return None;
                }
            }
            let mut node = if self.take("(") {
                let node = self.parse(0, depth + 1)?;
                if !self.take(")") {
                    return None;
                }
                node
            } else {
                let s = self.0.trim_start();
                let first = s.as_bytes().first()?;
                if !first.is_ascii_alphabetic() && *first != b'_' {
                    return None;
                }
                let n = s
                    .bytes()
                    .take_while(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-' | b'.'))
                    .count();
                self.0 = &s[n..];
                json!({"type":"identifier","name":&s[..n]})
            };
            for _ in 0..negations {
                node = json!({"type":"not","node":node});
            }
            Some(node)
        }
    }
    let mut p = Parser(text);
    let ast = p.parse(0, 0)?;
    p.0.trim().is_empty().then_some(ast)
}

fn compile(rule: &Value) -> Option<Value> {
    let command = rule["command"].as_str()?;
    let script = command
        .strip_prefix("script.")
        .and_then(|c| c.strip_suffix(".run"))
        .is_some_and(|s| {
            !s.is_empty()
                && s.len() <= 24
                && !s.starts_with('-')
                && s.bytes()
                    .all(|b| b.is_ascii_digit() || b.is_ascii_lowercase() || b == b'-')
        });
    if !script
        && !DEFAULTS["commands"]
            .as_array()?
            .iter()
            .any(|v| v == command)
    {
        return None;
    }
    let key = rule["key"].as_str()?.trim();
    if key.is_empty() || key.encode_utf16().count() > 64 {
        return None;
    }
    let mut value = json!({"command":command,"shortcut":shortcut(key)?});
    if let Some(when) = rule.get("when") {
        let s = when.as_str()?.trim();
        if s.encode_utf16().count() > 256 {
            return None;
        }
        value["whenAst"] = expression(s)?;
    }
    Some(value)
}

/// The effective keybindings and issues for the file at `path` in `documents`.
pub(crate) fn read(documents: &dyn DocumentStore, path: &Path) -> (Value, Value) {
    resolve(match read_text(documents, path) {
        Ok(text) => Ok(Some(text)),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e.to_string()),
    })
}

/// `fs::read_to_string` over a document store, with the same invalid-UTF-8 error.
fn read_text(documents: &dyn DocumentStore, path: &Path) -> std::io::Result<String> {
    String::from_utf8(documents.read(path)?).map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "stream did not contain valid UTF-8",
        )
    })
}

/// The effective keybindings and config issues for the user's file: its text,
/// `None` when there is no file, or the error reading it.
pub(crate) fn resolve(file: Result<Option<String>, String>) -> (Value, Value) {
    let defaults = DEFAULTS["keybindings"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(compile);
    let mut issues = Vec::new();
    let mut custom = Vec::new();
    match file {
        Ok(Some(text)) => match crate::settings::parse(&text) {
            Ok(Value::Array(rows)) => {
                for (index, rule) in rows.iter().enumerate() {
                    if let Some(binding) = compile(rule) {
                        custom.push(binding);
                    } else {
                        issues.push(json!({"kind":"keybindings.invalid-entry","index":index,"message":"SchemaError: Invalid keybinding rule"}));
                    }
                }
            }
            _ => issues.push(
                json!({"kind":"keybindings.malformed-config","message":"expected JSON array"}),
            ),
        },
        Ok(None) => {}
        Err(message) => {
            issues.push(json!({"kind":"keybindings.malformed-config","message":message}))
        }
    }
    let mut all = defaults
        .filter(|rule| !custom.iter().any(|c| c["command"] == rule["command"]))
        .collect::<Vec<_>>();
    all.extend(custom);
    all.drain(..all.len().saturating_sub(256));
    (all.into(), issues.into())
}

#[cfg(feature = "native")]
fn raw_entries(documents: &dyn DocumentStore, path: &Path) -> Result<Vec<Value>, Value> {
    match read_text(documents, path) {
        Ok(text) => match crate::settings::parse(&text) {
            Ok(Value::Array(rows)) => Ok(rows),
            _ => Err(json!({
                "_tag":"Fail",
                "error":{"_tag":"KeybindingsConfigError","detail":"expected JSON array","configPath":path}
            })),
        },
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(DEFAULTS["keybindings"]
            .as_array()
            .cloned()
            .unwrap_or_default()),
        Err(error) => Err(json!({
            "_tag":"Fail",
            "error":{"_tag":"KeybindingsConfigError","detail":"failed to read keybindings config","configPath":path,"cause":error.to_string()}
        })),
    }
}

#[cfg(feature = "native")]
fn same_rule(left: &Value, right: &Value) -> bool {
    match (compile(left), compile(right)) {
        (Some(left), Some(right)) => left == right,
        _ => left == right,
    }
}

#[cfg(feature = "native")]
fn write_entries(
    documents: &dyn DocumentStore,
    path: &Path,
    entries: &[Value],
) -> Result<(), Value> {
    if let Some(parent) = path.parent() {
        documents.create_dir_all(parent).map_err(|error| json!({
            "_tag":"Fail",
            "error":{"_tag":"KeybindingsConfigError","detail":"failed to prepare keybindings config directory","configPath":path,"cause":error.to_string()}
        }))?;
    }
    let contents = serde_json::to_string_pretty(entries).map_err(|error| json!({
        "_tag":"Fail",
        "error":{"_tag":"KeybindingsConfigError","detail":"failed to encode keybindings config","configPath":path,"cause":error.to_string()}
    }))?;
    documents.write(path, format!("{contents}\n").as_bytes(), WriteMode::InPlace).map_err(|error| json!({
        "_tag":"Fail",
        "error":{"_tag":"KeybindingsConfigError","detail":"failed to write keybindings config","configPath":path,"cause":error.to_string()}
    }))
}

#[cfg(feature = "native")]
pub(crate) fn mutate(
    documents: &dyn DocumentStore,
    path: &Path,
    method: &str,
    input: &Value,
) -> Result<Value, Value> {
    let mut entries = raw_entries(documents, path)?;
    let target = if method == "server.removeKeybinding" {
        input.clone()
    } else {
        input.get("replace").cloned().unwrap_or(Value::Null)
    };
    if !target.is_null() {
        entries.retain(|entry| !same_rule(entry, &target));
    }
    if method == "server.upsertKeybinding" {
        let mut rule = serde_json::Map::new();
        rule.insert("key".into(), input["key"].clone());
        rule.insert("command".into(), input["command"].clone());
        if let Some(when) = input.get("when") {
            rule.insert("when".into(), when.clone());
        }
        let rule = Value::Object(rule);
        if compile(&rule).is_none() {
            return Err(json!({
                "_tag":"Fail",
                "error":{"_tag":"KeybindingsConfigError","detail":"Invalid keybinding rule","configPath":path}
            }));
        }
        entries.retain(|entry| !same_rule(entry, &rule));
        entries.push(rule);
    }
    const MAX: usize = 256;
    if entries.len() > MAX {
        entries = entries.split_off(entries.len() - MAX);
    }
    write_entries(documents, path, &entries)?;
    let (keybindings, issues) = read(documents, path);
    Ok(json!({"keybindings":keybindings,"issues":issues}))
}
