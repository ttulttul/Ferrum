//! Environment theme rules shared by the theme service and the `t3_theme_*`
//! tools: the reference's constants (`themes.json`, generated from the pinned
//! reference), theme ids, the theme file schema and the CSS check.
use serde_json::{json, Map, Value};
use std::sync::LazyLock;

/// `{builtIn, defaultThemeId, unpublishable, cssMaxLength, guide}`.
pub(crate) static CONSTANTS: LazyLock<Value> =
    LazyLock::new(|| serde_json::from_str(include_str!("themes.json")).expect("theme constants"));

/// Every theme a client can show without a file (`MOBILE_THEME_IDS`).
pub(crate) fn built_in() -> Vec<String> {
    strings(&CONSTANTS["builtIn"])
}
/// The stock theme, selected by clearing the default (`MOBILE_DEFAULT_THEME_ID`).
pub(crate) fn default_theme_id() -> &'static str {
    CONSTANTS["defaultThemeId"].as_str().unwrap_or("t3-code")
}
/// Ids nobody may publish: appearance keywords, built-ins and old aliases.
pub(crate) fn unpublishable(id: &str) -> bool {
    CONSTANTS["unpublishable"]
        .as_array()
        .is_some_and(|ids| ids.iter().any(|i| i == id))
}
/// The restyling guide agents read (`THEME_GUIDE`).
pub(crate) fn guide() -> &'static str {
    CONSTANTS["guide"].as_str().unwrap_or_default()
}
fn css_max_length() -> usize {
    CONSTANTS["cssMaxLength"].as_u64().unwrap_or(100_000) as usize
}
/// The largest theme file read or published: the longest CSS in any JSON
/// encoding, at up to 6 bytes a character, plus the colors.
pub(crate) fn max_file_bytes() -> usize {
    6 * css_max_length() + 16 * 1024
}

fn strings(value: &Value) -> Vec<String> {
    value
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|v| v.as_str().map(str::to_owned))
        .collect()
}

/// `EnvironmentThemeId`: lowercase letters, digits and hyphens, at most 48,
/// starting with a letter or digit, and not an appearance keyword.
pub(crate) fn valid_id(id: &str) -> bool {
    let mut chars = id.chars();
    !matches!(id, "system" | "light" | "dark")
        && chars
            .next()
            .is_some_and(|c| c.is_ascii_lowercase() || c.is_ascii_digit())
        && id.len() <= 48
        && chars.all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-')
}

/// `environmentThemeFileHasColors`.
pub(crate) fn has_colors(file: &Value) -> bool {
    (file.get("canvas").is_some() && file.get("accent").is_some())
        || file["colors"].as_object().is_some_and(|c| !c.is_empty())
}

fn at(path: &[&str]) -> String {
    path.iter().map(|key| format!("[{}]", json!(key))).collect()
}

fn hex_color(value: &str) -> bool {
    let digits = value.strip_prefix('#').unwrap_or("");
    value.starts_with('#')
        && matches!(digits.len(), 3 | 6)
        && digits.chars().all(|c| c.is_ascii_hexdigit())
}

/// `TrimmedNonEmptyString` with a maximum length.
fn trimmed(value: &Value, max: usize, path: &[&str]) -> Result<String, String> {
    let Some(text) = value.as_str() else {
        return Err(format!("Expected string\n  at {}", at(path)));
    };
    let text = text.trim();
    if text.is_empty() {
        return Err(format!("Expected a non-blank string\n  at {}", at(path)));
    }
    if text.encode_utf16().count() > max {
        return Err(format!(
            "Expected a value with a length of at most {max}\n  at {}",
            at(path)
        ));
    }
    Ok(text.to_owned())
}

/// `EnvironmentThemeColors`: role names that do not match are dropped, as a
/// record schema drops keys it does not take.
fn colors(value: &Value, path: &[&str]) -> Result<Value, String> {
    let Some(object) = value.as_object() else {
        return Err(format!("Expected object\n  at {}", at(path)));
    };
    let mut out = Map::new();
    for (key, color) in object {
        let mut chars = key.chars();
        let named = chars.next().is_some_and(|c| c.is_ascii_alphabetic())
            && key.len() <= 64
            && chars.all(|c| c.is_ascii_alphanumeric());
        if !named {
            continue;
        }
        let mut inner = path.to_vec();
        inner.push(key);
        out.insert(key.clone(), json!(trimmed(color, 64, &inner)?));
    }
    Ok(Value::Object(out))
}

/// Decodes `value` as an `EnvironmentThemeFile` at `path`, as the reference's
/// schema does: names trimmed, unknown fields and color roles dropped, and the
/// first problem worded as effect words it.
pub(crate) fn decode_file(value: &Value, path: &[&str]) -> Result<Value, String> {
    let Some(object) = value.as_object() else {
        return Err(format!("Expected object\n  at {}", at(path)));
    };
    let field = |key: &str| object.get(key).filter(|v| !v.is_null());
    let inner = |key: &'static str| {
        let mut p = path.to_vec();
        p.push(key);
        p
    };
    let mut out = Map::new();
    if let Some(version) = field("version") {
        if version.as_f64() != Some(1.0) {
            return Err(format!("Expected 1 | null\n  at {}", at(&inner("version"))));
        }
        out.insert("version".into(), json!(1));
    }
    let Some(name) = object.get("name") else {
        return Err(format!("Missing key\n  at {}", at(&inner("name"))));
    };
    out.insert("name".into(), json!(trimmed(name, 48, &inner("name"))?));
    match object.get("appearance").and_then(Value::as_str) {
        Some(appearance @ ("light" | "dark")) => {
            out.insert("appearance".into(), json!(appearance));
        }
        None if object.get("appearance").is_none() => {
            return Err(format!("Missing key\n  at {}", at(&inner("appearance"))));
        }
        _ => {
            return Err(format!(
                "Expected \"light\" | \"dark\"\n  at {}",
                at(&inner("appearance"))
            ))
        }
    }
    for key in ["canvas", "accent"] {
        if let Some(color) = field(key) {
            let path = inner(if key == "canvas" { "canvas" } else { "accent" });
            let Some(text) = color.as_str() else {
                return Err(format!("Expected string\n  at {}", at(&path)));
            };
            if !hex_color(text) {
                return Err(format!(
                    "Expected a string matching the RegExp ^#(?:[0-9a-fA-F]{{3}}|[0-9a-fA-F]{{6}})$\n  at {}",
                    at(&path)
                ));
            }
            out.insert(key.into(), json!(text));
        }
    }
    if let Some(value) = field("colors") {
        out.insert("colors".into(), colors(value, &inner("colors"))?);
    }
    if let Some(value) = field("variants") {
        let path = inner("variants");
        let Some(variants) = value.as_object() else {
            return Err(format!("Expected object\n  at {}", at(&path)));
        };
        let mut decoded = Map::new();
        for key in ["light", "dark"] {
            if let Some(v) = variants.get(key).filter(|v| !v.is_null()) {
                let mut p = path.clone();
                p.push(if key == "light" { "light" } else { "dark" });
                decoded.insert(key.into(), colors(v, &p)?);
            }
        }
        out.insert("variants".into(), Value::Object(decoded));
    }
    if let Some(value) = field("css") {
        let Some(css) = value.as_str() else {
            return Err(format!("Expected string\n  at {}", at(&inner("css"))));
        };
        let max = css_max_length();
        if css.encode_utf16().count() > max {
            return Err(format!(
                "Expected a value with a length of at most {max}\n  at {}",
                at(&inner("css"))
            ));
        }
        out.insert("css".into(), json!(css));
    }
    Ok(Value::Object(out))
}

fn starts_ci(text: &str, at: usize, prefix: &str) -> bool {
    text.get(at..at + prefix.len())
        .is_some_and(|s| s.eq_ignore_ascii_case(prefix))
}

/// CSS whitespace (not U+00A0, which makes an address relative).
fn css_space(c: char) -> bool {
    matches!(c, ' ' | '\t' | '\n' | '\x0c' | '\r')
}

/// A quoted string starting at `at` (`"..."` or `'...'` with no line break),
/// as its end offset.
fn quoted(text: &str, at: usize) -> Option<usize> {
    let quote = text[at..]
        .chars()
        .next()
        .filter(|c| matches!(c, '"' | '\''))?;
    let rest = &text[at + 1..];
    let end = rest.find([quote, '\n', '\r', '\x0c'])?;
    (rest[end..].starts_with(quote)).then_some(at + 1 + end + 1)
}

/// The code with its strings removed, keeping `url(` where a quoted url()
/// can load a file (`withoutStrings`).
fn without_strings(code: &str) -> String {
    let mut out = String::new();
    let mut i = 0;
    while i < code.len() {
        if starts_ci(code, i, "url(") {
            let after = i + 4;
            let spaced = after
                + code[after..]
                    .chars()
                    .take_while(|c| css_space(*c))
                    .map(char::len_utf8)
                    .sum::<usize>();
            let quote_next = code[spaced..].starts_with(['"', '\'']);
            if !quote_next {
                // An unquoted url( runs to the next `)`, kept as written.
                let end = code[after..].find(')').map_or(code.len(), |e| after + e);
                out.push_str(&code[i..end]);
                i = end;
                continue;
            }
            if let Some(end) = quoted(code, spaced) {
                let string = &code[spaced + 1..end];
                if !(starts_ci(string, 0, "data:") || string.starts_with('#')) {
                    out.push_str("url(");
                }
                i = end;
                continue;
            }
        }
        if let Some(end) = quoted(code, i) {
            i = end;
            continue;
        }
        let c = code[i..].chars().next().expect("a character");
        out.push(c);
        i += c.len_utf8();
    }
    out
}

/// `fetchesOutsideStrings`: image-set(), src() or a url() that is not data:
/// or #id.
fn fetches_outside_strings(code: &str) -> bool {
    let lower = code.to_ascii_lowercase();
    if lower.contains("image-set(") {
        return true;
    }
    let word = |c: char| c.is_ascii_alphanumeric() || c == '_';
    if lower
        .match_indices("src(")
        .any(|(i, _)| lower[..i].chars().next_back().is_none_or(|c| !word(c)))
    {
        return true;
    }
    lower.match_indices("url(").any(|(i, _)| {
        let rest = lower[i + 4..].trim_start_matches(css_space);
        let rest = rest.strip_prefix(['"', '\'']).unwrap_or(rest);
        !(rest.starts_with("data:") || rest.starts_with('#'))
    })
}

/// `themeCssLoadsRemoteFiles`: whether theme CSS could load a file.
pub(crate) fn css_loads_remote_files(css: &str) -> bool {
    let mut uncommented = String::new();
    let mut rest = css;
    while let Some(start) = rest.find("/*") {
        uncommented.push_str(&rest[..start]);
        match rest[start + 2..].find("*/") {
            Some(end) => rest = &rest[start + 2 + end + 2..],
            None => {
                rest = &rest[start..];
                break;
            }
        }
    }
    uncommented.push_str(rest);
    let code = without_strings(&uncommented);
    code.to_ascii_lowercase().contains("@import") || fetches_outside_strings(&code)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decodes_theme_files_as_the_reference_does() {
        assert_eq!(
            decode_file(&json!({"name": " ", "appearance": "dark"}), &[]),
            Err("Expected a non-blank string\n  at [\"name\"]".into())
        );
        assert_eq!(
            decode_file(
                &json!({"name": "A", "appearance": "dark", "colors": {"1bad": "#fff"}}),
                &[]
            ),
            Ok(json!({"name": "A", "appearance": "dark", "colors": {}}))
        );
        assert_eq!(
            decode_file(
                &json!({"name": "  Ocean  ", "appearance": "dark", "extra": 1, "canvas": "#000", "accent": "#fff"}),
                &[]
            ),
            Ok(json!({"name": "Ocean", "appearance": "dark", "canvas": "#000", "accent": "#fff"}))
        );
        assert_eq!(
            decode_file(
                &json!({"name": "A", "appearance": "dark", "version": 2}),
                &[]
            ),
            Err("Expected 1 | null\n  at [\"version\"]".into())
        );
    }

    #[test]
    fn finds_css_that_loads_files() {
        assert!(css_loads_remote_files("@import 'x.css';"));
        assert!(css_loads_remote_files(
            "a { background: url(https://x/y.png) }"
        ));
        assert!(css_loads_remote_files("a { background: url( 'x.png' ) }"));
        assert!(!css_loads_remote_files(
            "a { background: url(data:image/png;base64,AA) }"
        ));
        assert!(!css_loads_remote_files("a { mask: url('#m') }"));
        assert!(!css_loads_remote_files("a::after { content: 'url(x)' }"));
        assert!(!css_loads_remote_files("/* url(x) */ a { color: red }"));
        assert!(css_loads_remote_files("a { b: image-set('x.png' 1x) }"));
        assert!(!css_loads_remote_files("a { b: notsrc(1) }"));
    }

    #[test]
    fn reads_theme_ids_as_the_reference_does() {
        assert!(valid_id("ocean-blue"));
        assert!(!valid_id("dark"));
        assert!(!valid_id("-x"));
        assert!(!valid_id("Upper"));
        assert!(unpublishable("t3-code"));
        assert!(built_in().contains(&"grove".to_owned()));
    }
}
