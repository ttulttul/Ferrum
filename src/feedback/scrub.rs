//! The reference's `scrubFeedback`: what an agent writes in a feedback report
//! loses anything shaped like a secret, an email address or a URL with
//! credentials, the values the server knows are private to the caller, and
//! file paths, then is capped. The patterns are the reference's; where they
//! look behind or ahead, this checks the neighbouring character itself.
use regex::Regex;
use std::sync::LazyLock;

pub(crate) const TEXT_MAX_CHARS: usize = 300;
const REDACTED: &str = "[redacted]";

fn re(pattern: &str) -> Regex {
    Regex::new(pattern).expect("scrub pattern")
}

static TOKEN_PREFIX: LazyLock<Regex> = LazyLock::new(|| {
    re(
        r"\b(?:sk-|sk_live_|pk_live_|rk_live_|gh[pousr]_|github_pat_|glpat-|xox[abprs]-|xapp-|npm_|hf_|AIza|AKIA|ASIA)[A-Za-z0-9_-]{8,}",
    )
});
static JWT: LazyLock<Regex> =
    LazyLock::new(|| re(r"\beyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+"));
static CREDENTIAL_URL: LazyLock<Regex> =
    LazyLock::new(|| re(r"(?i)\b[a-z][a-z0-9+.-]*://[^\s/@]+@\S+"));
static EMAIL: LazyLock<Regex> = LazyLock::new(|| {
    re(r"[A-Za-z0-9.!#$%&'*+=?^_`{|}~-]+@[A-Za-z0-9-]+(?:\.[A-Za-z0-9-]+)*\.[A-Za-z]{2,}")
});
static QUOTED_PATH: LazyLock<Regex> = LazyLock::new(|| {
    let start = |outside: &str| {
        format!(r"(?:/[^{outside}/]*/|~/|file://|[A-Za-z]:[\\/]|\\\\|\[redacted\][\\/])")
    };
    re(&["\"", "'", "`"]
        .iter()
        .map(|q| format!("{q}{}[^{q}\\n]*{q}", start(&format!("{q}\\n"))))
        .collect::<Vec<_>>()
        .join("|"))
});
static LONG_RUN: LazyLock<Regex> = LazyLock::new(|| re(r"(?:[A-Za-z0-9_+=-]|\[redacted\])+"));
static UNQUOTED_SEGMENT: LazyLock<Regex> = LazyLock::new(|| re(r#"^/[^\s/\\"'`<>(){},;:|]+/"#));
static UNC: LazyLock<Regex> = LazyLock::new(|| re(r"^\\\\[^\s\\/]+\\"));
static DRIVE: LazyLock<Regex> = LazyLock::new(|| re(r"^[A-Za-z]:[\\/]"));

fn word(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '_'
}

fn before(text: &str, at: usize) -> Option<char> {
    text[..at].chars().next_back()
}

/// Whether one of `PATH_START`'s alternatives starts at byte `at`.
fn path_starts(text: &str, at: usize) -> bool {
    let rest = &text[at..];
    let prev = before(text, at);
    if let Some(after) = rest.strip_prefix(REDACTED) {
        if after.starts_with("://") || after.starts_with('/') || after.starts_with('\\') {
            return true;
        }
    }
    if let Some(found) = UNQUOTED_SEGMENT.find(rest) {
        let guard = !prev.is_some_and(|c| word(c) || ".:/~-".contains(c));
        if guard && rest[found.end()..].starts_with(|c: char| !c.is_whitespace()) {
            return true;
        }
    }
    if rest.starts_with("~/") && !prev.is_some_and(|c| word(c) || c == '~' || c == '-') {
        return true;
    }
    if rest.starts_with("file://") && !prev.is_some_and(word) {
        return true;
    }
    if DRIVE.is_match(rest) && !prev.is_some_and(word) {
        return true;
    }
    UNC.is_match(rest) && !prev.is_some_and(|c| word(c) || c == '\\')
}

/// `PATH`: from where a path starts to the end of its line, each redacted.
fn redact_paths(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut at = 0;
    while at < text.len() {
        if path_starts(text, at) {
            out.push_str(REDACTED);
            at = text[at..].find('\n').map_or(text.len(), |n| at + n);
            continue;
        }
        let c = text[at..].chars().next().expect("char");
        out.push(c);
        at += c.len_utf8();
    }
    out
}

/// Each match of `value` not inside a longer identifier.
fn known_spans(text: &str, value: &str) -> Vec<(usize, usize)> {
    let mut spans = Vec::new();
    let mut from = 0;
    while let Some(found) = text[from..].find(value) {
        let start = from + found;
        let end = start + value.len();
        let clear_before = !before(text, start).is_some_and(word);
        let clear_after = !text[end..].chars().next().is_some_and(word);
        if clear_before && clear_after {
            spans.push((start, end));
            from = end;
        } else {
            from = start + text[start..].chars().next().map_or(1, char::len_utf8);
        }
    }
    spans
}

/// `redactAll`: the union of the spans, each run of overlapping ones redacted whole.
fn redact_spans(text: &str, mut spans: Vec<(usize, usize)>) -> String {
    spans.sort_by_key(|(start, _)| *start);
    let mut out = String::new();
    let mut at = 0;
    for (start, end) in spans {
        if start < at {
            at = at.max(end);
            continue;
        }
        out.push_str(&text[at..start]);
        out.push_str(REDACTED);
        at = end;
    }
    out.push_str(&text[at..]);
    out
}

fn entropy(value: &str) -> f64 {
    let mut counts = std::collections::HashMap::new();
    let mut total = 0usize;
    for c in value.chars() {
        *counts.entry(c).or_insert(0usize) += 1;
        total += 1;
    }
    counts
        .values()
        .map(|count| {
            let p = *count as f64 / total as f64;
            -p * p.log2()
        })
        .sum()
}

/// Long identifiers such as tool names have few digits; keys, hashes and ids many.
fn looks_random(value: &str) -> bool {
    value.chars().filter(char::is_ascii_digit).count() >= 2
        && value.chars().any(|c| c.is_ascii_alphabetic())
        && entropy(value) >= 3.5
}

/// `scrubFeedbackText`: the scrubbed text, and whether any was dropped to fit.
pub(crate) fn scrub_text(text: &str, sensitive: &[String], max_chars: usize) -> (String, bool) {
    let window = max_chars * 4;
    let mut scrubbed: String = text.chars().take(window).collect();
    let next = text.chars().nth(window);
    // Only a match in the line the window cut could reach past it: none of that line is kept.
    if next.is_some_and(|c| c != '\n') {
        let keep = scrubbed.rfind('\n').map_or(0, |n| n + 1);
        scrubbed.truncate(keep);
        scrubbed.push_str(REDACTED);
    }
    let mut known: Vec<String> = Vec::new();
    for value in sensitive
        .iter()
        .map(|v| v.trim())
        .filter(|v| v.encode_utf16().count() >= 3)
    {
        for line in value.split('\n').map(str::trim).filter(|l| !l.is_empty()) {
            if !known.iter().any(|k| k == line) {
                known.push(line.to_owned());
            }
        }
    }
    let mut spans: Vec<(usize, usize)> = Vec::new();
    for pattern in [&*CREDENTIAL_URL, &*TOKEN_PREFIX, &*JWT, &*EMAIL] {
        spans.extend(pattern.find_iter(&scrubbed).map(|m| (m.start(), m.end())));
    }
    for value in &known {
        spans.extend(known_spans(&scrubbed, value));
    }
    let scrubbed = redact_spans(&scrubbed, spans);
    let scrubbed = QUOTED_PATH.replace_all(&scrubbed, |caps: &regex::Captures| {
        let quote = &caps[0][..1];
        format!("{quote}{REDACTED}{quote}")
    });
    let scrubbed = redact_paths(&scrubbed);
    let scrubbed = LONG_RUN.replace_all(&scrubbed, |caps: &regex::Captures| {
        let run = &caps[0];
        if run.encode_utf16().count() >= 32 && (run.contains(REDACTED) || looks_random(run)) {
            REDACTED.to_owned()
        } else {
            run.to_owned()
        }
    });
    let count = scrubbed.chars().count();
    (
        scrubbed.chars().take(max_chars).collect(),
        next.is_some() || count > max_chars,
    )
}

/// `scrubFeedback`.
pub(crate) fn scrub(text: &str, sensitive: &[String]) -> String {
    scrub_text(text, sensitive, TEXT_MAX_CHARS).0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn secrets_known_values_and_paths_are_redacted() {
        let sensitive = vec!["thread:abc".to_owned(), "my-project".to_owned()];
        assert_eq!(
            scrub("key sk-abcdefghijkl and a@b.com in thread:abc", &sensitive),
            "key [redacted] and [redacted] in [redacted]"
        );
        assert_eq!(
            scrub("open /home/me/x.txt now\nnext", &[]),
            "open [redacted]\nnext"
        );
        assert_eq!(
            scrub("ran /compact and and/or 3/10", &[]),
            "ran /compact and and/or 3/10"
        );
        assert_eq!(scrub("see \"/tmp/a b\" ok", &[]), "see \"[redacted]\" ok");
        assert_eq!(scrub("my-projects stay", &sensitive), "my-projects stay");
        let (text, cut) = scrub_text(&"x".repeat(310), &[], 300);
        assert_eq!(text.chars().count(), 300);
        assert!(cut);
    }

    #[test]
    fn scrubbing_matches_the_reference() {
        let sensitive: Vec<String> = [
            "thread:client-thread",
            "project:github",
            "/home/me",
            "Provider turn",
            "ab",
        ]
        .iter()
        .map(|s| s.to_string())
        .collect();
        for (text, expected, cut) in [
            ("key sk-abcdefghijkl and a@b.com in thread:client-thread", "key [redacted] and [redacted] in [redacted]", false),
            ("open /home/me/x.txt now\nnext line", "open [redacted]\nnext line", false),
            ("ran /compact and and/or 3/10 /mcp /compact", "ran /compact and and/or 3/10 /mcp /compact", false),
            ("see \"/tmp/a b\" ok and '~/x y' and `C:\\dir\\f`", "see \"[redacted]\" ok and '[redacted]' and `[redacted]`", false),
            ("https://user:pw@host.example/path and http://example.com/x", "[redacted] and http://example.com/x", false),
            ("token eyJhbGciOi.eyJzdWIiOiIx.abc-def_ghi done", "token [redacted] done", false),
            ("id 3f9a8c7b2e1d4f6a9b8c7d6e5f4a3b2c1d0e9f8a and tool_name_with_many_words_but_no_digits_at_all", "id [redacted] and tool_name_with_many_words_but_no_digits_at_all", false),
            ("Provider turn failed in project:github with ab", "[redacted] failed in [redacted] with ab", false),
            ("file:///etc/passwd and \\\\server\\share\\x and ~/notes", "[redacted]", false),
        ] {
            assert_eq!(scrub_text(text, &sensitive, TEXT_MAX_CHARS), (expected.to_owned(), cut), "{text}");
        }
        assert_eq!(
            scrub_text(&"x".repeat(1205), &sensitive, 300),
            ("[redacted]".to_owned(), true)
        );
        let long = format!("line1\n{}", "y".repeat(1300));
        assert_eq!(
            scrub_text(&long, &sensitive, 300),
            ("line1\n[redacted]".to_owned(), true)
        );
        let emoji = scrub_text(&"😀".repeat(310), &sensitive, 300);
        assert_eq!((emoji.0.chars().count(), emoji.1), (300, true));
    }
}
