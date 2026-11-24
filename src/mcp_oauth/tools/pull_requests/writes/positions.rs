//! Match the old and new line numbers from a unified patch, including renamed and deleted files.
use super::*;
use std::borrow::Cow;

fn header(line: &str, prefix: &str) -> Option<String> {
    let token = line[4..].split('\t').next()?;
    let path = if let Some(quoted) = token.strip_prefix('"').and_then(|p| p.strip_suffix('"')) {
        let mut bytes = Vec::new();
        let mut rest = quoted.as_bytes();
        while let Some((&b, tail)) = rest.split_first() {
            rest = tail;
            if b != b'\\' || rest.is_empty() {
                bytes.push(b);
                continue;
            }
            if rest.len() >= 3 && rest[..3].iter().all(|c| (b'0'..=b'7').contains(c)) {
                bytes.push(((rest[0] - b'0') << 6) | ((rest[1] - b'0') << 3) | (rest[2] - b'0'));
                rest = &rest[3..];
                continue;
            }
            bytes.push(match rest[0] {
                b'a' => 7,
                b'b' => 8,
                b't' => 9,
                b'n' => 10,
                b'v' => 11,
                b'f' => 12,
                b'r' => 13,
                other => other,
            });
            rest = &rest[1..];
        }
        Cow::Owned(String::from_utf8_lossy(&bytes).into_owned())
    } else {
        Cow::Borrowed(token)
    };
    path.strip_prefix(prefix).map(str::to_owned)
}

fn position(patch: &str, path: &str, line: u64, left: bool) -> Option<Value> {
    static HUNK: LazyLock<regex::Regex> =
        LazyLock::new(|| regex::Regex::new(r"^@@ -(\d+)(?:,\d+)? \+(\d+)(?:,\d+)? @@").unwrap());
    let (mut old, mut new) = (0u64, 0u64);
    let (mut old_path, mut matched, mut hunks) = (None, false, false);
    for text in patch.split('\n') {
        if text.starts_with("diff --git ") {
            old_path = None;
            matched = false;
            hunks = false
        } else if !hunks && text.starts_with("--- ") {
            old_path = header(text, "a/")
        } else if !hunks && text.starts_with("+++ ") {
            matched = header(text, "b/").or_else(|| old_path.clone()).as_deref() == Some(path)
        } else if text.starts_with("@@") {
            if let Some(c) = HUNK.captures(text) {
                old = c[1].parse().ok()?;
                new = c[2].parse().ok()?;
                hunks = true
            }
        } else if matched && hunks {
            match text.as_bytes().first() {
                Some(b'+') => {
                    if !left && new == line {
                        return Some(json!({"kind":"added","newLine":new}));
                    }
                    new = new.saturating_add(1)
                }
                Some(b'-') => {
                    if left && old == line {
                        return Some(json!({"kind":"deleted","oldLine":old}));
                    }
                    old = old.saturating_add(1)
                }
                Some(b' ') => {
                    if (if left { old } else { new }) == line {
                        return Some(
                            json!({"kind":"context","newLine":new,"oldLine":old,"side":if left{"left"}else{"right"}}),
                        );
                    }
                    old = old.saturating_add(1);
                    new = new.saturating_add(1)
                }
                _ => {}
            }
        }
    }
    None
}

pub(super) fn draft(patch: &str, comment: &Value) -> Option<Value> {
    let path = comment["path"].as_str()?;
    let left = comment["side"] == "old";
    let mut item = fields(comment, &["path", "body"]);
    item["position"] = position(patch, path, comment["line"].as_u64()?, left)?;
    if let Some(start) = comment["startLine"].as_u64() {
        item["startPosition"] = position(patch, path, start, left)?
    }
    Some(item)
}
