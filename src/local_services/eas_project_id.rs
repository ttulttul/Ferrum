//! `easProjectId.ts`: the Expo project of a workspace's Expo app, read from its app config
//! without running it.
//!
//! The reference reads configs with JavaScript regular expressions that look behind and ahead,
//! which the `regex` crate does not; those checks are written out here as scanners with the same
//! answers. As in JavaScript: `\s` is ECMAScript white space and line terminators (`js_space`),
//! `\b` outside Unicode mode is an ASCII word boundary, `i` folds ASCII only, lengths count UTF-16
//! units, and files are decoded as `TextDecoder` does (a leading byte order mark dropped,
//! malformed bytes replaced). `eas_project_ids.json` holds the reference's answers for a corpus of
//! workspaces (`scripts/reference_eas_project_ids.mjs`), and the tests check every one.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::LazyLock;

use regex::Regex;
use serde_json::Value;

const CONFIG_FILES: [&str; 5] = [
    "app.config.ts",
    "app.config.mts",
    "app.config.js",
    "app.config.mjs",
    "app.config.cjs",
];
const JSON_CONFIG_FILES: [&str; 2] = ["app.config.json", "app.json"];
/// Reading a config is linear in its size per entry; one this large is not an app config.
const MAX_CONFIG_LENGTH: usize = 256 * 1024;
/// An app config mentions its project a few times at most.
const MAX_MENTIONS: usize = 32;

/// JavaScript's `\s`, in a regex class.
const WS: &str = r"[\t\n\x0B\x0C\r \xA0\x{1680}\x{2000}-\x{200A}\x{2028}\x{2029}\x{202F}\x{205F}\x{3000}\x{FEFF}]";

fn regex(pattern: &str) -> Regex {
    Regex::new(&pattern.replace("{WS}", WS)).expect("valid pattern")
}

static UUID: LazyLock<Regex> = LazyLock::new(|| {
    regex(r"\A(?i-u:[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})\z")
});
/// `QUOTED_PROJECT_ID` up to its lookahead (`separator_follows`).
static QUOTED_PROJECT_ID: LazyLock<Regex> =
    LazyLock::new(|| regex(r#"(?-u:\b)(?i-u:projectid)["']?{WS}*:{WS}*["'`]([^"'`\n]*)["'`]"#));
static PROJECT_ID_MENTION: LazyLock<Regex> =
    LazyLock::new(|| regex(r"(?-u:\b)(?i-u:projectid)(?-u:\b)"));
static ASSIGNS_OVER: LazyLock<Regex> =
    LazyLock::new(|| regex(r"(?-u:\b)Object{WS}*\.{WS}*assign(?-u:\b)|(?-u:\b)assign{WS}*\("));
/// `REPLACES_HOLDER`'s assignment, up to its `(?!=)`.
static ASSIGNS_HOLDER: LazyLock<Regex> = LazyLock::new(|| {
    regex(r#"(?:\.{WS}*(?:extra|eas)|\[{WS}*["'`](?:extra|eas)["'`]{WS}*\]){WS}*="#)
});
static DELETE_OR_REQUIRE: LazyLock<Regex> =
    LazyLock::new(|| regex(r"(?-u:\b)delete(?-u:\b)|(?-u:\b)require{WS}*\("));
/// `REPLACES_HOLDER`'s import, up to its lookahead (`imports_expo_config_type`).
static IMPORT: LazyLock<Regex> = LazyLock::new(|| regex(r"(?-u:\b)import{WS}*\("));
static EXTRA_OR_EAS: LazyLock<Regex> =
    LazyLock::new(|| regex(r#"(?:(?-u:\b)(extra|eas)|["'`](extra|eas)["'`]{WS}*\]?){WS}*:{WS}*"#));
static ID_START: LazyLock<Regex> = LazyLock::new(|| regex(r"\A\p{ID_Start}\z"));
static ID_CONTINUE: LazyLock<Regex> = LazyLock::new(|| regex(r"\A\p{ID_Continue}\z"));

fn js_space(c: char) -> bool {
    matches!(
        c,
        '\t' | '\n' | '\u{b}' | '\u{c}' | '\r' | ' ' | '\u{a0}' | '\u{1680}' | '\u{2000}'
            ..='\u{200a}'
                | '\u{2028}'
                | '\u{2029}'
                | '\u{202f}'
                | '\u{205f}'
                | '\u{3000}'
                | '\u{feff}'
    )
}

fn line_terminator(c: char) -> bool {
    matches!(c, '\n' | '\r' | '\u{2028}' | '\u{2029}')
}

fn id_start(c: char) -> bool {
    if c.is_ascii() {
        return c.is_ascii_alphabetic();
    }
    ID_START.is_match(c.encode_utf8(&mut [0; 4]))
}

fn id_continue(c: char) -> bool {
    if c.is_ascii() {
        return c.is_ascii_alphanumeric() || c == '_';
    }
    ID_CONTINUE.is_match(c.encode_utf8(&mut [0; 4]))
}

/// `[\p{ID_Continue}$\u200c\u200d]`: a character an identifier continues with.
fn ident_part(c: char) -> bool {
    id_continue(c) || matches!(c, '$' | '\u{200c}' | '\u{200d}')
}

/// `[\p{ID_Start}$_]`: a character an identifier starts with.
fn ident_first(c: char) -> bool {
    id_start(c) || matches!(c, '$' | '_')
}

fn char_at(source: &str, at: usize) -> Option<char> {
    source.get(at..).and_then(|rest| rest.chars().next())
}

fn char_before(source: &str, at: usize) -> Option<char> {
    source[..at].chars().next_back()
}

fn skip_space(source: &str, mut at: usize) -> usize {
    while let Some(c) = char_at(source, at).filter(|c| js_space(*c)) {
        at += c.len_utf8();
    }
    at
}

fn skip_space_back(source: &str, mut at: usize) -> usize {
    while let Some(c) = char_before(source, at).filter(|c| js_space(*c)) {
        at -= c.len_utf8();
    }
    at
}

/// The end of a `/* */` comment that starts at `at`, if it closes.
fn block_comment_end(source: &str, at: usize) -> Option<usize> {
    source[at + 2..].find("*/").map(|end| at + 2 + end + 2)
}

/// The end of a `//` comment that starts at `at`, before its line terminator.
fn line_comment_end(source: &str, at: usize) -> usize {
    source[at..]
        .char_indices()
        .find(|(_, c)| line_terminator(*c))
        .map_or(source.len(), |(offset, _)| at + offset)
}

/// `SPREAD_FILLER` repeated: space, comments and opening parentheses after `at`.
fn skip_fillers(source: &str, mut at: usize) -> usize {
    loop {
        let rest = &source[at..];
        let next = if rest.starts_with(js_space) {
            skip_space(source, at)
        } else if rest.starts_with("//") {
            line_comment_end(source, at)
        } else if rest.starts_with("/*") {
            match block_comment_end(source, at) {
                Some(end) => end,
                None => return at,
            }
        } else if rest.starts_with('(') {
            at + 1
        } else {
            return at;
        };
        at = next;
    }
}

/// `QUOTED_PROJECT_ID`'s lookahead: space and comments, then a separator or the end.
fn separator_follows(source: &str, mut at: usize) -> bool {
    loop {
        let rest = &source[at..];
        if rest.starts_with(js_space) {
            at = skip_space(source, at);
        } else if rest.starts_with("//") {
            at = line_comment_end(source, at);
        } else if rest.starts_with("/*") {
            match block_comment_end(source, at) {
                Some(end) => at = end,
                None => return false,
            }
        } else {
            return rest.is_empty() || rest.starts_with([',', ';', '}', ')', ']']);
        }
    }
}

/// `HOLDER_END`: nothing more of what ends at `at` (no identifier character, property, call or
/// operator; `?? {}` may follow).
fn holder_ends(source: &str, at: usize) -> bool {
    if char_at(source, at).is_some_and(|c| ident_part(c) || c == '\\') {
        return false;
    }
    let at = skip_space(source, at);
    match char_at(source, at) {
        Some('.' | '[' | '(') => false,
        Some('?') => char_at(source, at + 1) == Some('?'),
        _ => true,
    }
}

/// `\s*\??\.\s*<name>` or `\s*(?:\?\.)?\s*\[\s*["'`]<name>["'`]\s*\]` at `at`: its end.
fn member(source: &str, at: usize, name: &str) -> Option<usize> {
    let start = skip_space(source, at);
    let dotted = {
        let mut at = start;
        if source[at..].starts_with('?') {
            at += 1;
        }
        source[at..]
            .starts_with('.')
            .then(|| skip_space(source, at + 1))
            .filter(|at| source[*at..].starts_with(name))
            .map(|at| at + name.len())
    };
    dotted.or_else(|| {
        let mut at = start;
        if source[at..].starts_with("?.") {
            at = skip_space(source, at + 2);
        }
        if !source[at..].starts_with('[') {
            return None;
        }
        at = skip_space(source, at + 1);
        let rest = &source[at..];
        if !rest.starts_with(['"', '\'', '`']) || !rest[1..].starts_with(name) {
            return None;
        }
        at += 1 + name.len();
        if !source[at..].starts_with(['"', '\'', '`']) {
            return None;
        }
        at = skip_space(source, at + 1);
        source[at..].starts_with(']').then_some(at + 1)
    })
}

#[derive(Clone, Copy)]
enum Holder {
    Extra,
    Eas,
}

/// `HOLDER_AT[holder]` at `at`: `config`'s `extra` (or `extra.eas`), and nothing more of it.
fn holder_at(source: &str, at: usize, holder: Holder) -> bool {
    if !source[at..].starts_with("config") {
        return false;
    }
    let Some(end) = member(source, at + "config".len(), "extra") else {
        return false;
    };
    let end = match holder {
        Holder::Extra => Some(end),
        Holder::Eas => member(source, end, "eas"),
    };
    end.is_some_and(|end| holder_ends(source, end))
}

/// `CONFIG_AT` at `at`: `config` and no more of it.
fn config_at(source: &str, at: usize) -> bool {
    source[at..].starts_with("config") && holder_ends(source, at + "config".len())
}

/// An identifier (`[\p{ID_Start}$_][\p{ID_Continue}$\u200c\u200d]*`) at `at`: its end.
fn identifier(source: &str, at: usize) -> Option<usize> {
    let first = char_at(source, at).filter(|c| ident_first(*c))?;
    let mut end = at + first.len_utf8();
    while let Some(c) = char_at(source, end).filter(|c| ident_part(*c)) {
        end += c.len_utf8();
    }
    Some(end)
}

/// `CONFIG_PART_AT` at `at`: a part of `config` other than `extra` (`config.ios`), and nothing
/// more of it; the last name.
fn config_part_at(source: &str, at: usize) -> Option<&str> {
    if !source[at..].starts_with("config") {
        return None;
    }
    let mut end = at + "config".len();
    let mut last = None;
    loop {
        let mut next = skip_space(source, end);
        if source[next..].starts_with('?') {
            next += 1;
        }
        if !source[next..].starts_with('.') {
            break;
        }
        next = skip_space(source, next + 1);
        let excluded = source[next..].starts_with("extra")
            && !char_at(source, next + "extra".len()).is_some_and(|c| id_continue(c) || c == '$');
        let Some(name_end) = identifier(source, next).filter(|_| !excluded) else {
            break;
        };
        last = Some(&source[next..name_end]);
        end = name_end;
    }
    last.filter(|_| holder_ends(source, end))
}

/// `PART_OPENER` at `at`: the key of the object or array entry whose value opens just before
/// `at` (`ios: { ` before `...config.ios`), read backwards as the reference's lookbehind does.
fn part_opener(source: &str, at: usize) -> Option<&str> {
    let mut at = skip_space_back(source, at);
    if !matches!(char_before(source, at), Some('[' | '{')) {
        return None;
    }
    at = skip_space_back(source, at - 1);
    if char_before(source, at) != Some(':') {
        return None;
    }
    at = skip_space_back(source, at - 1);
    if matches!(char_before(source, at), Some('"' | '\'')) {
        at -= 1;
    }
    let end = at;
    while let Some(c) = char_before(source, at).filter(|c| ident_part(*c)) {
        at -= c.len_utf8();
    }
    if at == end || !char_at(source, at).is_some_and(ident_first) {
        return None;
    }
    let name = &source[at..end];
    if matches!(char_before(source, at), Some('"' | '\'')) {
        at -= 1;
    }
    at = skip_space_back(source, at);
    matches!(char_before(source, at), Some('{' | ',')).then_some(name)
}

#[derive(PartialEq)]
enum ConfigUse {
    Spreads,
    Replaces,
    Unclear,
}

/// `staticConfigUse`: only Expo's `config`, its own `extra`/`extra.eas`, and parts kept in their
/// own entry can carry app.json's id through; other spreads may replace it, and a config that
/// spreads nothing leaves it unclear.
fn static_config_use(source: &str) -> ConfigUse {
    let mut spreads = 0;
    let mut at = 0;
    loop {
        let Some(offset) = source[at..].find("...") else {
            return if spreads > 0 {
                ConfigUse::Spreads
            } else {
                ConfigUse::Unclear
            };
        };
        let next = at + offset;
        at = skip_fillers(source, next + 3);
        if config_at(source, at) {
            spreads += 1;
            continue;
        }
        // Expo reads a returned `expo` entry as the whole config.
        let kept_part = config_part_at(source, at)
            .is_some_and(|part| part_opener(source, next) == Some(part) && part != "expo");
        if kept_part {
            continue;
        }
        if !holder_at(source, at, Holder::Extra) && !holder_at(source, at, Holder::Eas) {
            return ConfigUse::Replaces;
        }
    }
}

/// `SHORTHAND_EXTRA_OR_EAS`: a shorthand `extra` or `eas` entry.
fn shorthand_extra_or_eas(source: &str) -> bool {
    ["extra", "eas"].iter().any(|name| {
        source.match_indices(name).any(|(at, _)| {
            !char_before(source, at).is_some_and(|c| c == '.' || c == '$' || id_continue(c))
                && matches!(
                    char_at(source, skip_space(source, at + name.len())),
                    Some(',' | '}')
                )
        })
    })
}

/// `keepsStaticExtra`: every `extra`/`eas` object starts by spreading the matching `config`
/// entry.
fn keeps_static_extra(source: &str) -> bool {
    if shorthand_extra_or_eas(source) {
        return false;
    }
    let mut entries = 0;
    let mut from = 0;
    while let Some(entry) = EXTRA_OR_EAS.captures_at(source, from) {
        entries += 1;
        if entries > MAX_MENTIONS {
            return false;
        }
        let whole = entry.get(0).expect("whole match");
        from = whole.end();
        let holder = match entry.get(1).or(entry.get(2)).map(|m| m.as_str()) {
            Some("extra") => Holder::Extra,
            _ => Holder::Eas,
        };
        let mut at = whole.end();
        if !source[at..].starts_with('{') {
            return false;
        }
        at = skip_fillers(source, at + 1);
        if !source[at..].starts_with("...") {
            return false;
        }
        at = skip_fillers(source, at + 3);
        if !holder_at(source, at, holder) {
            return false;
        }
    }
    true
}

/// Every match of `pattern` (from each position on, as a regex with a lookahead finds them)
/// for which `accept` holds.
fn any_match(source: &str, pattern: &Regex, accept: impl Fn(usize) -> bool) -> bool {
    let mut from = 0;
    while let Some(found) = pattern.find_at(source, from) {
        if accept(found.end()) {
            return true;
        }
        // Every pattern here starts with an ASCII character.
        from = found.start() + 1;
    }
    false
}

/// `import(...)`'s lookahead: `import("expo/config").ExpoConfig`, which loads only Expo's
/// types.
fn imports_expo_config_type(source: &str, at: usize) -> bool {
    let mut at = skip_space(source, at);
    let Some(quote) = char_at(source, at).filter(|c| matches!(c, '"' | '\'')) else {
        return false;
    };
    at += 1;
    if !source[at..].starts_with("expo/config") {
        return false;
    }
    at += "expo/config".len();
    if char_at(source, at) != Some(quote) {
        return false;
    }
    at = skip_space(source, at + 1);
    if !source[at..].starts_with(')') {
        return false;
    }
    at = skip_space(source, at + 1);
    if !source[at..].starts_with('.') {
        return false;
    }
    at = skip_space(source, at + 1);
    char_at(source, at).is_some_and(|c| c.is_ascii_uppercase())
}

/// `REPLACES_HOLDER`: a config that may replace its entry or itself from where this scan cannot
/// read.
fn replaces_holder(source: &str) -> bool {
    any_match(source, &ASSIGNS_HOLDER, |end| {
        char_at(source, end) != Some('=')
    }) || DELETE_OR_REQUIRE.is_match(source)
        || any_match(source, &IMPORT, |end| {
            !imports_expo_config_type(source, end)
        })
}

/// Decodes a file as `readFileString` does (`TextDecoder`).
fn decode(bytes: &[u8]) -> String {
    let text = String::from_utf8_lossy(bytes);
    text.strip_prefix('\u{feff}').unwrap_or(&text).to_owned()
}

/// The project id a static config names (wrapped in `expo` or not), or `Err` when it does not
/// decode as the reference's schema does.
fn static_project_id(text: &str) -> Result<Option<String>, ()> {
    let value: Value = serde_json::from_str(text).map_err(|_| ())?;
    let object = |value: &Value| value.as_object().cloned().ok_or(());
    let extra = |value: Option<&Value>| -> Result<Option<String>, ()> {
        let Some(extra) = value else {
            return Ok(None);
        };
        let Some(eas) = object(extra)?.get("eas").cloned() else {
            return Ok(None);
        };
        match object(&eas)?.get("projectId") {
            None => Ok(None),
            Some(Value::String(id)) => Ok(Some(id.clone())),
            Some(_) => Err(()),
        }
    };
    let config = object(&value)?;
    let top = extra(config.get("extra"))?;
    match config.get("expo") {
        Some(expo) => extra(object(expo)?.get("extra")),
        None => Ok(top),
    }
}

/// `projectIdsOf`: the project ids an app's configs name, never running them; `None` when a
/// config cannot be read or sets or changes the id other than with a literal.
fn project_ids_of(dir: &Path) -> Option<Vec<String>> {
    let mut ids = Vec::new();
    if let Some(file) = CONFIG_FILES.iter().find(|file| dir.join(file).exists()) {
        let source = std::fs::read(dir.join(file))
            .map(|bytes| decode(&bytes))
            .unwrap_or_else(|_| "projectId".to_owned());
        if source.encode_utf16().count() > MAX_CONFIG_LENGTH {
            return None;
        }
        let mentions = PROJECT_ID_MENTION.find_iter(&source).count();
        if mentions > MAX_MENTIONS {
            return None;
        }
        // Every mention must be a quoted entry.
        let mut entries = Vec::new();
        let mut from = 0;
        while let Some(found) = QUOTED_PROJECT_ID.captures_at(&source, from) {
            let whole = found.get(0).expect("whole match");
            if separator_follows(&source, whole.end()) {
                entries.push((whole.end(), found[1].to_owned()));
                from = whole.end();
            } else {
                from = whole.start() + 1;
            }
        }
        if entries.len() != mentions
            || entries.iter().any(|(_, id)| id.contains("${"))
            || ASSIGNS_OVER.is_match(&source)
            || replaces_holder(&source)
            || entries
                .iter()
                .any(|(end, _)| source[*end..].contains("..."))
        {
            return None;
        }
        let literal: Vec<_> = entries
            .into_iter()
            .map(|(_, id)| id)
            .filter(|id| UUID.is_match(id))
            .collect();
        // Only a config shown to spread `config` and nothing else carries app.json's entry
        // through unchanged.
        if literal.is_empty()
            && (static_config_use(&source) != ConfigUse::Spreads || !keeps_static_extra(&source))
        {
            return None;
        }
        ids.extend(literal.into_iter().map(|id| id.to_lowercase()));
    }
    for file in JSON_CONFIG_FILES {
        let text = match std::fs::read(dir.join(file)) {
            Ok(bytes) => decode(&bytes),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            // A config that is there but cannot be read could name any project.
            Err(_) => return None,
        };
        let id = static_project_id(&text).ok()?;
        if let Some(id) = id.filter(|id| UUID.is_match(id)) {
            ids.push(id.to_lowercase());
        }
        // Expo reads only the first static config it finds.
        break;
    }
    Some(ids)
}

/// `findEasProjectId`: the Expo project of a workspace's Expo app, at its root or one app below
/// `apps/` or `packages/`. `None` when there is none, a config is ambiguous, or the apps
/// disagree, so the caller asks for an explicit choice instead of billing the wrong project.
pub(super) fn find_eas_project_id(workspace_root: &Path) -> Option<String> {
    let children = |parent: &str| -> Vec<PathBuf> {
        let Ok(entries) = std::fs::read_dir(workspace_root.join(parent)) else {
            return Vec::new();
        };
        entries
            .filter_map(Result::ok)
            .map(|entry| entry.path())
            .filter(|path| std::fs::metadata(path).is_ok_and(|info| info.is_dir()))
            .collect()
    };
    let directories = std::iter::once(workspace_root.to_path_buf())
        .chain(children("apps"))
        .chain(children("packages"));
    let mut distinct = BTreeSet::new();
    for directory in directories {
        // An app whose project is unknown could be the one the thread works on.
        distinct.extend(project_ids_of(&directory)?);
    }
    (distinct.len() == 1).then(|| distinct.into_iter().next().expect("one id"))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The reference's answer for every workspace in `eas_project_ids.json`.
    #[test]
    fn matches_the_reference_on_its_corpus() {
        let corpus: Vec<Value> =
            serde_json::from_str(include_str!("eas_project_ids.json")).expect("corpus");
        let mut wrong = Vec::new();
        for case in &corpus {
            let root = tempfile::tempdir().expect("temp dir");
            for (file, content) in case["files"].as_object().expect("files") {
                let path = root.path().join(file);
                std::fs::create_dir_all(path.parent().expect("parent")).expect("parent dir");
                if let Some(text) = content.as_str() {
                    std::fs::write(&path, text).expect("write");
                } else if content["directory"] == true {
                    std::fs::create_dir_all(&path).expect("directory");
                } else if let Some(pad) = content["pad"].as_array() {
                    let text = pad[0].as_str().expect("text");
                    let length = pad[1].as_u64().expect("length") as usize;
                    let units = text.encode_utf16().count();
                    let padded = format!(
                        "{text}{}{}",
                        " ".repeat(length.saturating_sub(units)),
                        pad[2].as_str().expect("tail")
                    );
                    std::fs::write(&path, padded).expect("write");
                } else {
                    let hex = content["hex"].as_str().expect("hex");
                    let bytes: Vec<u8> = (0..hex.len())
                        .step_by(2)
                        .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).expect("hex byte"))
                        .collect();
                    std::fs::write(&path, bytes).expect("write");
                }
            }
            let found = find_eas_project_id(root.path());
            if found.as_deref() != case["expected"].as_str() {
                wrong.push(format!(
                    "{}: reference {:?}, rust {found:?}",
                    case["case"], case["expected"]
                ));
            }
        }
        assert!(
            wrong.is_empty(),
            "{} of {} cases differ:\n{}",
            wrong.len(),
            corpus.len(),
            wrong.join("\n")
        );
    }
}
