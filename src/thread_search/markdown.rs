//! Searchable text is made of rendered blocks. Inline formatting joins text,
//! while chips, cards, hard breaks and block boundaries split it.
use pulldown_cmark::{Event, Options, Parser, Tag, TagEnd};
use regex::Regex;
use serde_json::Value;
use std::{
    collections::{HashMap, HashSet},
    sync::LazyLock,
};

static HEADING: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"^\s{0,3}#{1,6}\s+(.+)$").unwrap());
static SKILL: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"\p{Sc}([a-zA-Z0-9][a-zA-Z0-9:_-]*)").unwrap());
static CONTEXT: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"^t3-context://v1/[a-z][a-z0-9-]{0,39}/[a-zA-Z0-9_-]{1,128}$").unwrap()
});
static LEGACY_REVIEW: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r#"(?s)<review_comment\s+[^>]*sectionId="[^"]+"[^>]*>.*?</review_comment>"#).unwrap()
});
static LEGACY_CONTEXT: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?s)\n*<(?:terminal_context|element_context|preview_annotation)>\n- [^\n]+:\n.*?\n</(?:terminal_context|element_context|preview_annotation)>\s*$").unwrap()
});
static POSITION: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?::|#L)(\d+)(?:(?::|C)(\d+)|-L?(\d+))?$").unwrap());
fn skill_labels(text: &str, skills: &[Value]) -> String {
    if skills.is_empty() {
        return text.to_owned();
    }
    SKILL
        .replace_all(text, |c: &regex::Captures| {
            let m = c.get(0).unwrap();
            let name = &c[1];
            let bounded = text[..m.start()]
                .chars()
                .next_back()
                .is_none_or(char::is_whitespace)
                && text[m.end()..]
                    .chars()
                    .next()
                    .is_none_or(char::is_whitespace);
            if bounded && name.bytes().any(|b| b.is_ascii_alphabetic()) {
                if let Some(skill) = skills.iter().find(|s| s["name"] == name) {
                    return skill["displayName"]
                        .as_str()
                        .map(str::trim)
                        .filter(|s| !s.is_empty())
                        .unwrap_or(name)
                        .to_owned();
                }
            }
            m.as_str().to_owned()
        })
        .into_owned()
}
#[derive(Clone)]
struct FileLabel {
    path: String,
    position: String,
}
fn file(href: &str, cwd: Option<&str>, inline: bool) -> Option<FileLabel> {
    let href = percent_encoding::percent_decode_str(href.trim())
        .decode_utf8()
        .ok()?;
    let href = href
        .strip_prefix("file://localhost/")
        .map(|s| format!("/{s}"))
        .unwrap_or_else(|| {
            href.strip_prefix("file://")
                .unwrap_or(&href)
                .replace('\\', "/")
        });
    if href.contains("://")
        || href.starts_with("//")
        || href.starts_with('#')
        || href.starts_with("mailto:")
    {
        return None;
    }
    let mut path = href.as_str();
    let mut position = String::new();
    if let Some(c) = POSITION.captures(path) {
        position = format!(" · L{}", &c[1]);
        if let Some(end) = c.get(3) {
            position += &format!("–{}", end.as_str());
        } else if let Some(col) = c.get(2) {
            position += &format!(":C{}", col.as_str());
        }
        path = &path[..c.get(0)?.start()];
    }
    let name = path.trim_end_matches('/').rsplit('/').next()?;
    let explicit = path.starts_with('/')
        || path.starts_with("./")
        || path.starts_with("../")
        || path.starts_with("~/")
        || path.as_bytes().get(1) == Some(&b':');
    let filename = name.rsplit_once('.').is_some_and(|(stem, ext)| {
        !stem.is_empty()
            && ext.len() <= 16
            && !ext.is_empty()
            && ext.bytes().all(|b| b.is_ascii_alphanumeric())
    }) || matches!(
        name,
        "Makefile" | "Dockerfile" | "README" | "LICENSE" | "Cargo.lock"
    );
    if path.is_empty()
        || (!explicit && !filename && (inline || !path.contains('/')))
        || inline
            && (!path.contains('/') && position.is_empty()
                || path.contains(char::is_whitespace) && !explicit)
    {
        return None;
    }
    let path = if path.starts_with('/') || path.as_bytes().get(1) == Some(&b':') {
        path.to_owned()
    } else {
        format!(
            "{}/{}",
            cwd?.trim_end_matches('/'),
            path.trim_start_matches("./")
        )
    };
    Some(FileLabel { path, position })
}
impl FileLabel {
    fn name(&self) -> &str {
        self.path
            .trim_end_matches('/')
            .rsplit('/')
            .next()
            .unwrap_or_default()
    }
    fn label(&self, suffix: &HashMap<String, String>) -> String {
        format!(
            "{}{}{}",
            self.name(),
            suffix
                .get(&self.path)
                .map(|s| format!(" · {s}"))
                .unwrap_or_default(),
            self.position
        )
    }
}
fn suffixes(files: impl Iterator<Item = FileLabel>) -> HashMap<String, String> {
    let mut groups = HashMap::<String, HashSet<String>>::new();
    for f in files {
        groups
            .entry(f.name().to_owned())
            .or_default()
            .insert(f.path);
    }
    let mut out = HashMap::new();
    for paths in groups.into_values().filter(|g| g.len() > 1) {
        let parents = paths
            .iter()
            .map(|s| {
                s.rsplit_once('/')
                    .map_or("", |p| p.0)
                    .split('/')
                    .filter(|p| !p.is_empty())
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        for (i, (path, dirs)) in paths.iter().zip(&parents).enumerate() {
            if dirs.is_empty() {
                continue;
            }
            let depth = (1..=dirs.len())
                .find(|n| {
                    !parents
                        .iter()
                        .enumerate()
                        .any(|(j, p)| j != i && p.ends_with(&dirs[dirs.len() - n..]))
                })
                .unwrap_or(dirs.len())
                .max(2)
                .min(dirs.len());
            out.insert(path.clone(), dirs[dirs.len() - depth..].join("/"));
        }
    }
    out
}
fn flush(text: &mut String, out: &mut Vec<String>) {
    if text.trim().is_empty() {
        text.clear()
    } else {
        out.push(std::mem::take(text));
    }
}
fn html_text(
    element: scraper::ElementRef<'_>,
    text: &mut String,
    out: &mut Vec<String>,
    pre: bool,
) {
    let name = element.value().name();
    if matches!(name, "script" | "style") {
        return;
    }
    let block = matches!(
        name,
        "address"
            | "article"
            | "aside"
            | "blockquote"
            | "br"
            | "dd"
            | "details"
            | "div"
            | "dl"
            | "dt"
            | "figcaption"
            | "figure"
            | "footer"
            | "h1"
            | "h2"
            | "h3"
            | "h4"
            | "h5"
            | "h6"
            | "header"
            | "hr"
            | "li"
            | "main"
            | "nav"
            | "ol"
            | "p"
            | "pre"
            | "section"
            | "summary"
            | "table"
            | "tbody"
            | "td"
            | "th"
            | "thead"
            | "tr"
            | "ul"
    );
    if block {
        flush(text, out);
    }
    for child in element.children() {
        if let Some(value) = child.value().as_text() {
            if pre || name == "pre" {
                text.push_str(value);
            } else {
                text.push_str(&value.replace("\r\n", "\n").replace('\n', " "));
            }
        } else if let Some(child) = scraper::ElementRef::wrap(child) {
            html_text(child, text, out, pre || name == "pre");
        }
    }
    if block {
        flush(text, out);
    }
}

pub(super) fn segments(item: &Value, cwd: Option<&str>, skills: &[Value]) -> Vec<String> {
    let plan = item["type"] == "proposed_plan";
    let user = item["type"] == "user_message";
    let mut body = item[if plan { "markdown" } else { "text" }]
        .as_str()
        .unwrap_or_default();
    let mut out = vec![];
    if user
        && body
            .get(..11)
            .filter(|p| p.eq_ignore_ascii_case("Ultrathink:"))
            .map_or(body, |_| body[11..].trim_start())
            == super::PLACEHOLDER
    {
        return out;
    }
    if plan {
        let title = body
            .lines()
            .find_map(|l| HEADING.captures(l))
            .and_then(|c| c.get(1))
            .map(|m| m.as_str().trim())
            .unwrap_or("Proposed plan");
        out.push(title.to_owned());
        fn skip(s: &str) -> &str {
            s.split_once('\n')
                .map_or("", |p| p.1)
                .trim_start_matches(['\r', '\n'])
        }
        if body.lines().next().is_some_and(|l| HEADING.is_match(l)) {
            body = skip(body);
        }
        if body
            .lines()
            .next()
            .and_then(|l| HEADING.captures(l))
            .is_some_and(|c| c[1].trim().eq_ignore_ascii_case("summary"))
        {
            body = skip(body);
        }
    } else if !user && body.is_empty() && item["streaming"] != true {
        body = "(empty response)";
    }
    // Old histories serialized context records into the message body. Current
    // messages with structured context may contain these same tags literally.
    let upgraded;
    if user && item["context"].is_null() && body.contains('<') {
        let mut legacy = body;
        while let Some(block) = LEGACY_CONTEXT.find(legacy) {
            legacy = &legacy[..block.start()];
        }
        upgraded = LEGACY_REVIEW.replace_all(legacy, "[context](t3-context://v1/review/legacy)");
        body = &upgraded;
    }
    let parser = || {
        Parser::new_ext(
            body,
            Options::ENABLE_TABLES | Options::ENABLE_STRIKETHROUGH | Options::ENABLE_TASKLISTS,
        )
    };
    let suffix = suffixes(parser().filter_map(|e| match e {
        Event::Start(Tag::Link { dest_url, .. }) => file(&dest_url, cwd, false),
        Event::Code(s) => file(&s, cwd, true),
        _ => None,
    }));
    let mut text = String::new();
    let mut events = parser();
    let mut pre = false;
    let mut insight = false;
    let mut paragraph = false;
    let breaks = user || body.lines().any(|s| s.starts_with("★ Insight"));
    while let Some(e) = events.next() {
        match e {
            Event::Start(Tag::Image { .. }) => {
                for e in events.by_ref() {
                    if e == Event::End(TagEnd::Image) {
                        break;
                    }
                }
            }
            Event::Start(Tag::Link { dest_url, .. }) => {
                let mut label = String::new();
                for e in events.by_ref() {
                    match e {
                        Event::End(TagEnd::Link) => break,
                        Event::Text(s) | Event::Code(s) => label.push_str(&s),
                        Event::SoftBreak => label.push(' '),
                        _ => (),
                    }
                }
                if user && CONTEXT.is_match(&dest_url) {
                    flush(&mut text, &mut out);
                } else if let Some((_, comment)) = crate::statev2::write::parse_citation(&dest_url)
                {
                    flush(&mut text, &mut out);
                    text.push_str(
                        &comment
                            .unwrap_or_default()
                            .replace("\r\n", "\n")
                            .replace('\n', " "),
                    );
                } else if let Some(f) = file(&dest_url, cwd, false) {
                    let stripped = POSITION.replace(&label, "");
                    if !label.is_empty()
                        && !f
                            .path
                            .ends_with(&format!("/{}", stripped.trim_start_matches("./")))
                        && f.path != stripped
                    {
                        text.push_str(&label);
                        text.push(' ');
                    }
                    text.push_str(&f.label(&suffix));
                } else {
                    text.push_str(&label);
                }
            }
            Event::Code(s) => {
                if let Some(f) = file(&s, cwd, true) {
                    text.push_str(&f.label(&suffix));
                    continue;
                }
                text.push_str(&s);
            }
            Event::Text(s) => {
                if breaks && !plan && !user && !pre {
                    let line = s.trim();
                    if line.strip_prefix("★ Insight").is_some_and(|tail| {
                        let fence = tail.trim();
                        fence.chars().count() >= 3 && fence.chars().all(|c| c == '─')
                    }) {
                        insight = true;
                        continue;
                    }
                    if insight && line.chars().count() >= 3 && line.chars().all(|c| c == '─') {
                        insight = false;
                        continue;
                    }
                }
                if paragraph && !plan && !pre {
                    text.push_str(&skill_labels(&s, skills));
                } else {
                    text.push_str(&s);
                }
            }
            Event::Html(s) | Event::InlineHtml(s) if user => text.push_str(&s),
            Event::Html(s) | Event::InlineHtml(s) => {
                let tree = scraper::Html::parse_fragment(&s);
                html_text(tree.root_element(), &mut text, &mut out, pre);
            }
            Event::SoftBreak if !breaks => text.push(' '),
            Event::SoftBreak | Event::HardBreak | Event::Rule => flush(&mut text, &mut out),
            Event::Start(Tag::Emphasis | Tag::Strong | Tag::Strikethrough)
            | Event::End(TagEnd::Emphasis | TagEnd::Strong | TagEnd::Strikethrough) => (),
            Event::Start(t) => {
                flush(&mut text, &mut out);
                if matches!(t, Tag::CodeBlock(_)) {
                    pre = true;
                }
                if matches!(t, Tag::Paragraph | Tag::Item) {
                    paragraph = true;
                }
            }
            Event::End(t) => {
                flush(&mut text, &mut out);
                if t == TagEnd::CodeBlock {
                    pre = false;
                }
                if matches!(t, TagEnd::Paragraph | TagEnd::Item) {
                    paragraph = false;
                }
            }
            _ => (),
        }
    }
    flush(&mut text, &mut out);
    out
}
