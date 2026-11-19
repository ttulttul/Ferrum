use super::DEFAULTS;
use serde_json::{json, Value};
use std::{fs, path::Path};

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

pub(super) fn read(path: &Path) -> (Value, Value) {
    let defaults = DEFAULTS["keybindings"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(compile);
    let mut issues = Vec::new();
    let mut custom = Vec::new();
    match fs::read_to_string(path) {
        Ok(text) => match crate::settings::parse(&text) {
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
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => {
            issues.push(json!({"kind":"keybindings.malformed-config","message":e.to_string()}))
        }
    }
    let mut all = defaults
        .filter(|rule| !custom.iter().any(|c| c["command"] == rule["command"]))
        .collect::<Vec<_>>();
    all.extend(custom);
    all.drain(..all.len().saturating_sub(256));
    (all.into(), issues.into())
}
