//! A page as the reference's `HtmlRender` prepares it: the theme bootstrap
//! injected into its head (`injectHtmlRenderBootstrap`), viewport height units
//! refused (`rejectViewportHeight`), and local images inlined as data URIs
//! (`inlineLocalImages`), all with the reference's limits and messages.
use super::{Error, CONSTANTS, MAX_IMAGE_BYTES, MAX_PAGE_BYTES};
use regex::Regex;
use std::sync::LazyLock;

fn ascii_lower_starts(text: &[u8], at: usize, prefix: &str) -> bool {
    text.len() >= at + prefix.len()
        && text[at..at + prefix.len()].eq_ignore_ascii_case(prefix.as_bytes())
}

fn word(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || byte == b'_'
}

const RAW_TEXT: [&str; 9] = [
    "script", "style", "textarea", "title", "xmp", "iframe", "noembed", "noframes", "noscript",
];

/// `blankNonMarkup`: comments, raw-text elements, `<plaintext>` and
/// templates blanked to spaces, keeping every byte offset.
fn blank_non_markup(html: &str) -> Vec<u8> {
    let bytes = html.as_bytes();
    let mut scan = bytes.to_vec();
    let blank = |scan: &mut Vec<u8>, from: usize, to: usize| scan[from..to].fill(b' ');
    let mut at = 0;
    while at < bytes.len() {
        if bytes[at] != b'<' {
            at += 1;
            continue;
        }
        if bytes[at..].starts_with(b"<!--") {
            let end = find(bytes, b"-->", at + 4).map_or(bytes.len(), |e| e + 3);
            blank(&mut scan, at, end);
            at = end;
            continue;
        }
        let tag = RAW_TEXT.iter().find(|name| {
            ascii_lower_starts(bytes, at + 1, name)
                && bytes.get(at + 1 + name.len()).is_none_or(|b| !word(*b))
        });
        if let Some(name) = tag {
            let end = closing(bytes, at + 1 + name.len(), name).unwrap_or(bytes.len());
            blank(&mut scan, at, end);
            at = end;
            continue;
        }
        if ascii_lower_starts(bytes, at + 1, "plaintext")
            && bytes.get(at + 10).is_none_or(|b| !word(*b))
        {
            blank(&mut scan, at, bytes.len());
            break;
        }
        at += 1;
    }
    // Templates, nested ones included, as one blank run each.
    static TEMPLATE: LazyLock<regex::bytes::Regex> =
        LazyLock::new(|| regex::bytes::Regex::new(r"(?i-u)</?template(?:\s[^>]*)?/?>").unwrap());
    let mut depth = 0usize;
    let mut start = 0;
    let mut runs = Vec::new();
    for found in TEMPLATE.find_iter(&scan) {
        if found.as_bytes()[1] != b'/' {
            if depth == 0 {
                start = found.start();
            }
            depth += 1;
        } else if depth > 0 {
            depth -= 1;
            if depth == 0 {
                runs.push((start, found.end()));
            }
        }
    }
    if depth > 0 {
        runs.push((start, scan.len()));
    }
    for (from, to) in runs {
        blank(&mut scan, from, to);
    }
    scan
}

fn find(haystack: &[u8], needle: &[u8], from: usize) -> Option<usize> {
    haystack
        .get(from..)?
        .windows(needle.len())
        .position(|w| w == needle)
        .map(|p| p + from)
}

/// The end of `</name\s*>` at or after `from`, case-insensitively.
fn closing(bytes: &[u8], from: usize, name: &str) -> Option<usize> {
    let mut at = from;
    while let Some(open) = find(bytes, b"</", at) {
        let after = open + 2;
        if ascii_lower_starts(bytes, after, name) {
            let mut end = after + name.len();
            while end < bytes.len() && bytes[end].is_ascii_whitespace() {
                end += 1;
            }
            if bytes.get(end) == Some(&b'>') {
                return Some(end + 1);
            }
        }
        at = open + 2;
    }
    None
}

fn text_str(key: &str) -> &'static str {
    CONSTANTS[key].as_str().unwrap_or_default()
}

/// `injectHtmlRenderBootstrap`: the meta tags the page lacks, the theme
/// stylesheet and the bootstrap script, at the start of its head.
pub(crate) fn inject_bootstrap(html: &str) -> String {
    let scan_bytes = blank_non_markup(html);
    let scan = std::str::from_utf8(&scan_bytes).unwrap_or_default();
    static CHARSET: LazyLock<Regex> =
        LazyLock::new(|| Regex::new(r"(?i)<meta\s[^>]*charset").unwrap());
    static VIEWPORT: LazyLock<Regex> =
        LazyLock::new(|| Regex::new(r#"(?i)<meta\s[^>]*name\s*=\s*["']?viewport"#).unwrap());
    static HEAD: LazyLock<Regex> =
        LazyLock::new(|| Regex::new(r#"(?i)<head(?:\s(?:[^"'>]|"[^"]*"|'[^']*')*)?>"#).unwrap());
    static HTML: LazyLock<Regex> =
        LazyLock::new(|| Regex::new(r#"(?i)<html(?:\s(?:[^"'>]|"[^"]*"|'[^']*')*)?>"#).unwrap());
    static DOCTYPE: LazyLock<Regex> =
        LazyLock::new(|| Regex::new(r"(?i)^\s*<!doctype[^>]*>").unwrap());
    // The reference reads the first 4096 UTF-16 units for the charset.
    let head_of_scan: String = {
        let mut units = 0;
        scan.chars()
            .take_while(|c| {
                units += c.len_utf16();
                units <= 4096
            })
            .collect()
    };
    let markup = format!(
        "{}{}{}",
        if CHARSET.is_match(&head_of_scan) {
            ""
        } else {
            text_str("charsetMeta")
        },
        if VIEWPORT.is_match(scan) {
            ""
        } else {
            text_str("viewportMeta")
        },
        text_str("bootstrapMarkup"),
    );
    if let Some(open) = HEAD.find(scan) {
        let at = open.end();
        return format!("{}{markup}{}", &html[..at], &html[at..]);
    }
    if let Some(open) = HTML.find(scan) {
        let at = open.end();
        return format!("{}<head>{markup}</head>{}", &html[..at], &html[at..]);
    }
    if let Some(doctype) = DOCTYPE.find(html) {
        let at = doctype.end();
        return format!("{}<head>{markup}</head>{}", &html[..at], &html[at..]);
    }
    format!("<!doctype html><head>{markup}</head>{html}")
}

/// `rejectViewportHeight`: a viewport height unit in a stylesheet or style
/// attribute (comments skipped).
pub(crate) fn reject_viewport_height(html: &str) -> Result<(), Error> {
    static CSS_SOURCE: LazyLock<Regex> = LazyLock::new(|| {
        Regex::new(
            r#"(?is)<!--.*?(?:-->|\z)|<style\b[^>]*(?:>|\z)(.*?)(?:</style\s*>|\z)|\sstyle\s*=\s*(?:"([^"]*)(?:"|\z)|'([^']*)(?:'|\z)|([^\s>]+))"#,
        )
        .unwrap()
    });
    for found in CSS_SOURCE.captures_iter(html) {
        let css = (1..=4)
            .find_map(|i| found.get(i))
            .map_or("", |m| m.as_str());
        if let Some(unit) = viewport_unit(css) {
            return Err(Error::ViewportHeight(unit));
        }
    }
    Ok(())
}

/// `VIEWPORT_HEIGHT_UNIT`: a number with a `vh`, `svh`, `lvh` or `dvh` unit,
/// not inside a word or a base64 run.
fn viewport_unit(css: &str) -> Option<String> {
    static UNIT: LazyLock<Regex> =
        LazyLock::new(|| Regex::new(r"(?i-u)^(?:[0-9]+(?:\.[0-9]+)?|\.[0-9]+)[sld]?vh").unwrap());
    let bytes = css.as_bytes();
    for at in 0..bytes.len() {
        let first = bytes[at];
        if !(first.is_ascii_digit() || first == b'.') {
            continue;
        }
        if at > 0 && (matches!(bytes[at - 1], b'.' | b'+' | b'/') || word(bytes[at - 1])) {
            continue;
        }
        let Some(found) = UNIT.find(&css[at..]) else {
            continue;
        };
        let end = at + found.end();
        if bytes.get(end).is_some_and(|b| word(*b) || *b == b'-') {
            continue;
        }
        return Some(found.as_str().to_owned());
    }
    None
}

const IMAGE_TYPES: [(&str, &str); 10] = [
    ("png", "image/png"),
    ("jpg", "image/jpeg"),
    ("jpeg", "image/jpeg"),
    ("gif", "image/gif"),
    ("webp", "image/webp"),
    ("avif", "image/avif"),
    ("svg", "image/svg+xml"),
    ("bmp", "image/bmp"),
    ("ico", "image/x-icon"),
    ("", ""),
];

fn image_extension(path: &str) -> bool {
    path.rfind('.').is_some_and(|dot| {
        let ext = &path[dot + 1..];
        IMAGE_TYPES
            .iter()
            .any(|(e, _)| !e.is_empty() && ext.eq_ignore_ascii_case(e))
    })
}

fn data_uri_prefix(path: &str) -> String {
    let ext = path
        .rsplit('.')
        .next()
        .unwrap_or_default()
        .to_ascii_lowercase();
    let mime = IMAGE_TYPES
        .iter()
        .find(|(e, _)| !e.is_empty() && *e == ext)
        .map_or("application/octet-stream", |(_, m)| m);
    format!("data:{mime};base64,")
}

/// The length of an absolute path's start at `at`: `/` not followed by `/`,
/// or a drive letter, colon and slash.
fn absolute_start(bytes: &[u8], at: usize) -> Option<usize> {
    match bytes.get(at)? {
        b'/' if bytes.get(at + 1) != Some(&b'/') => Some(1),
        c if c.is_ascii_alphabetic()
            && bytes.get(at + 1) == Some(&b':')
            && matches!(bytes.get(at + 2), Some(b'\\' | b'/')) =>
        {
            Some(3)
        }
        _ => None,
    }
}

fn utf16_len(text: &str) -> usize {
    text.encode_utf16().count()
}

/// `findLocalImages`: absolute image paths in quotes or `url(...)`, as byte
/// spans of the page.
pub(crate) fn find_local_images(html: &str) -> Vec<(usize, usize)> {
    let bytes = html.as_bytes();
    let mut spans = Vec::new();
    let mut at = 0;
    while at < bytes.len() {
        if let Some((start, end, next)) = quoted_image(html, at).or_else(|| url_image(html, at)) {
            spans.push((start, end));
            at = next;
            continue;
        }
        at += 1;
        while at < bytes.len() && !html.is_char_boundary(at) {
            at += 1;
        }
    }
    spans
}

fn quoted_image(html: &str, at: usize) -> Option<(usize, usize, usize)> {
    let bytes = html.as_bytes();
    let quote = *bytes.get(at)?;
    if !matches!(quote, b'"' | b'\'' | b'`') {
        return None;
    }
    let start = at + 1;
    let lead = absolute_start(bytes, start)?;
    let end = (start + lead..bytes.len())
        .find(|i| matches!(bytes[*i], b'\r' | b'\n') || bytes[*i] == quote)?;
    if bytes[end] != quote {
        return None;
    }
    let path = &html[start..end];
    let rest = &html[start + lead..end];
    let dot = rest.rfind('.')?;
    if !image_extension(path) || utf16_len(&rest[..dot]) > 2048 {
        return None;
    }
    Some((start, end, end + 1))
}

fn url_image(html: &str, at: usize) -> Option<(usize, usize, usize)> {
    let bytes = html.as_bytes();
    if !ascii_lower_starts(bytes, at, "url(") {
        return None;
    }
    let mut start = at + 4;
    while start < bytes.len() && (bytes[start] as char).is_whitespace() {
        start += 1;
    }
    let lead = absolute_start(bytes, start)?;
    let end = (start + lead..bytes.len())
        .find(|i| {
            let b = bytes[*i];
            b.is_ascii_whitespace() || matches!(b, b'"' | b'\'' | b'`' | b'(' | b')')
        })
        .unwrap_or(bytes.len());
    let path = &html[start..end];
    let rest = &html[start + lead..end];
    let dot = rest.rfind('.')?;
    if !image_extension(path) || utf16_len(&rest[..dot]) > 2048 {
        return None;
    }
    let mut close = end;
    while close < bytes.len() && (bytes[close] as char).is_whitespace() {
        close += 1;
    }
    (bytes.get(close) == Some(&b')')).then_some((start, end, close + 1))
}

/// Inside a JS string literal a Windows path's backslashes are escaped.
fn file_path_for(reference: &str) -> String {
    let bytes = reference.as_bytes();
    if bytes.len() >= 2 && bytes[0].is_ascii_alphabetic() && bytes[1] == b':' {
        reference.replace("\\\\", "\\")
    } else {
        reference.to_owned()
    }
}

/// `isImageBytes`: judged by the bytes, so a renamed file is not inlined.
fn is_image_bytes(bytes: &[u8]) -> bool {
    let head = &bytes[..bytes.len().min(12)];
    let at = |i: usize, s: &[u8]| head.len() >= i + s.len() && &head[i..i + s.len()] == s;
    if at(0, b"\x89PNG")
        || at(0, b"\xff\xd8\xff")
        || at(0, b"GIF8")
        || at(0, b"\0\0\x01\0")
        || (at(0, b"BM") && at(6, b"\0\0\0\0"))
        || (at(0, b"RIFF") && at(8, b"WEBP"))
        || (head.len() >= 12
            && at(4, b"ftyp")
            && matches!(&head[8..12], b"avif" | b"avis" | b"mif1"))
    {
        return true;
    }
    let text = String::from_utf8_lossy(&bytes[..bytes.len().min(4096)]);
    has_svg_root(text.strip_prefix('\u{feff}').unwrap_or(&text))
}

fn after(text: &str, token: &str, from: usize) -> Option<usize> {
    text.get(from..)?
        .find(token)
        .map(|i| from + i + token.len())
}

fn has_svg_root(text: &str) -> bool {
    let mut at = Some(0);
    while let Some(mut i) = at {
        while text[i..].starts_with(|c: char| c.is_whitespace()) {
            i += text[i..].chars().next().map_or(1, char::len_utf8);
        }
        let rest = &text[i..];
        at = if rest.starts_with("<?") {
            after(text, "?>", i + 2)
        } else if rest.starts_with("<!--") {
            after(text, "-->", i + 4)
        } else if rest.len() >= 9 && rest[..9].eq_ignore_ascii_case("<!doctype") {
            after_doctype(text, i + 9)
        } else {
            return rest.starts_with("<svg")
                && rest[4..].starts_with([' ', '\t', '\r', '\n', '/', '>']);
        };
    }
    false
}

fn after_doctype(text: &str, from: usize) -> Option<usize> {
    let bytes = text.as_bytes();
    let mut subset = false;
    let mut at = from;
    while at < bytes.len() {
        let c = bytes[at];
        if c == b'"' || c == b'\'' {
            at = after(text, if c == b'"' { "\"" } else { "'" }, at + 1)?;
        } else if subset && text[at..].starts_with("<!--") {
            at = after(text, "-->", at + 4)?;
        } else if subset && text[at..].starts_with("<?") {
            at = after(text, "?>", at + 2)?;
        } else if c == b'>' && !subset {
            return Some(at + 1);
        } else {
            if c == b'[' {
                subset = true;
            } else if c == b']' {
                subset = false;
            }
            at += 1;
        }
    }
    None
}

fn base64_len(bytes: u64) -> u64 {
    bytes.div_ceil(3) * 4
}

/// The page with its local images inlined, and the paths that could not be.
pub(crate) struct Inlined {
    pub html: String,
    pub missing: Vec<String>,
}

/// `inlineLocalImages`.
pub(crate) fn inline_local_images(html: &str) -> Result<Inlined, Error> {
    use base64::Engine;
    let references = find_local_images(html);
    let mut unique: Vec<&str> = Vec::new();
    for (start, end) in &references {
        let path = &html[*start..*end];
        if !unique.contains(&path) {
            unique.push(path);
        }
    }
    let files: Vec<(&str, Option<u64>)> = unique
        .iter()
        .map(|path| {
            let size = std::fs::metadata(file_path_for(path))
                .ok()
                .filter(|m| m.is_file())
                .map(|m| m.len());
            (*path, size)
        })
        .collect();
    if let Some((path, Some(size))) = files
        .iter()
        .find(|(_, s)| s.is_some_and(|s| s > MAX_IMAGE_BYTES))
    {
        return Err(Error::ImageTooLarge((*path).to_owned(), *size));
    }
    let size_of = |path: &str| files.iter().find(|(p, _)| *p == path).and_then(|(_, s)| *s);
    let page_bytes = references
        .iter()
        .fold(html.len() as u64, |total, (start, end)| {
            let path = &html[*start..*end];
            match size_of(path) {
                None => total,
                Some(size) => (total + data_uri_prefix(path).len() as u64 + base64_len(size))
                    .saturating_sub(path.len() as u64),
            }
        });
    if page_bytes > MAX_PAGE_BYTES {
        return Err(Error::PageTooLarge(page_bytes));
    }
    let mut read_bytes = 0u64;
    let mut uris: Vec<(&str, String)> = Vec::new();
    for (path, size) in &files {
        if size.is_none() {
            continue;
        }
        use std::io::Read;
        let Ok(file) = std::fs::File::open(file_path_for(path)) else {
            continue;
        };
        let mut bytes = Vec::new();
        if file
            .take(MAX_IMAGE_BYTES + 1)
            .read_to_end(&mut bytes)
            .is_err()
            || !is_image_bytes(&bytes)
        {
            continue;
        }
        if bytes.len() as u64 > MAX_IMAGE_BYTES {
            return Err(Error::ImageTooLarge((*path).to_owned(), bytes.len() as u64));
        }
        read_bytes += base64_len(bytes.len() as u64);
        if read_bytes > MAX_PAGE_BYTES {
            return Err(Error::PageTooLarge(read_bytes));
        }
        let encoded = base64::engine::general_purpose::STANDARD.encode(&bytes);
        uris.push((path, format!("{}{encoded}", data_uri_prefix(path))));
    }
    let mut out = String::with_capacity(html.len());
    let mut cursor = 0;
    for (start, end) in &references {
        let path = &html[*start..*end];
        let Some((_, uri)) = uris.iter().find(|(p, _)| *p == path) else {
            continue;
        };
        out.push_str(&html[cursor..*start]);
        out.push_str(uri);
        cursor = *end;
    }
    out.push_str(&html[cursor..]);
    if out.len() as u64 > MAX_PAGE_BYTES {
        return Err(Error::PageTooLarge(out.len() as u64));
    }
    let missing = files
        .iter()
        .filter(|(path, _)| !uris.iter().any(|(p, _)| p == path))
        .map(|(path, _)| (*path).to_owned())
        .collect();
    Ok(Inlined { html: out, missing })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_bootstrap_goes_after_the_head_outside_comments_and_scripts() {
        let page = "<!doctype html><html><!-- <head> --><script>'<head>'</script><head><title>x</title></head></html>";
        let out = inject_bootstrap(page);
        let at = out
            .find("<head><meta charset")
            .expect("injected after the real head");
        assert!(out[..at].contains("<!-- <head> -->"));
        assert!(out.contains("<style id=\"t3-theme\">"));
        let bare = inject_bootstrap("<p>hi</p>");
        assert!(bare.starts_with("<!doctype html><head><meta charset=\"utf-8\">"));
        assert!(bare.ends_with("</head><p>hi</p>"));
        let typed = inject_bootstrap("<!DOCTYPE html><p>x</p>");
        assert!(typed.starts_with("<!DOCTYPE html><head>"));
        let has_meta = inject_bootstrap(
            "<html><head><meta charset=utf-8><meta name=viewport content=x></head></html>",
        );
        assert!(!has_meta.contains("width=device-width"));
    }

    #[test]
    fn viewport_units_are_found_only_in_css() {
        assert!(reject_viewport_height("<style>.a{height:100vh}</style>").is_err());
        assert!(reject_viewport_height("<div style=\"min-height: 50dvh\">").is_err());
        assert!(reject_viewport_height("<p>100vh in text</p>").is_ok());
        assert!(reject_viewport_height(
            "<style>/* x */ .a{background:url(data:image/png;base64,AA1vh)}</style>"
        )
        .is_ok());
        assert!(reject_viewport_height("<!-- <style>.a{height:1vh}</style> -->").is_ok());
        match reject_viewport_height("<style>.a{height:.5SVH}</style>") {
            Err(Error::ViewportHeight(found)) => assert_eq!(found, ".5SVH"),
            _ => panic!("not found"),
        }
    }

    #[test]
    fn local_images_are_found_in_quotes_and_urls() {
        let page = "<img src=\"/tmp/a.png\"><div style=\"background:url( /tmp/b.JPG )\"></div><img src='//cdn/x.png'><img src=\"relative.png\">";
        let spans: Vec<&str> = find_local_images(page)
            .into_iter()
            .map(|(s, e)| &page[s..e])
            .collect();
        assert_eq!(spans, vec!["/tmp/a.png", "/tmp/b.JPG"]);
    }

    #[test]
    fn images_are_judged_by_their_bytes() {
        assert!(is_image_bytes(b"\x89PNG\r\n\x1a\n...."));
        assert!(is_image_bytes(
            b"<?xml version=\"1.0\"?><!-- c --><svg xmlns=\"x\"/>"
        ));
        assert!(!is_image_bytes(b"SECRET=1\n"));
        assert!(!is_image_bytes(b"<svgx>"));
    }
}
