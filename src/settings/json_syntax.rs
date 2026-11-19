use regex::Regex;
use serde_json::Value;
use std::sync::LazyLock;

// Match the reference's two passes over JSONC, including their handling of incomplete strings.
// A comment becomes one space, so adjacent tokens never run together.
static COMMENTS: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r#"("(?:[^"\\]|\\[^\r\n\u2028\u2029])*")|//[^\r\n]*|/\*(?s:.*?)\*/"#).unwrap()
});
static COMMAS: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r#"("(?:[^"\\]|\\[^\r\n\u2028\u2029])*")|,([\p{Zs}\t\n\r\x0b\x0c\u2028\u2029\uFEFF]*[}\]])"#).unwrap()
});

pub(crate) fn parse(text: &str) -> Result<Value, String> {
    let clean = COMMENTS.replace_all(text, |captures: &regex::Captures<'_>| {
        captures
            .get(1)
            .map_or(" ", |literal| literal.as_str())
            .to_owned()
    });
    let clean = COMMAS.replace_all(&clean, "$1$2");
    // Effect's SchemaGetter.parseJson discards V8's SyntaxError and emits this issue instead.
    serde_json::from_str(&clean).map_err(|_| "Expected a valid JSON string".into())
}

pub(super) fn parse_file(bytes: &[u8]) -> Result<Value, String> {
    // Node's filesystem service uses TextDecoder for UTF-8 settings files.
    let bytes = bytes.strip_prefix(b"\xef\xbb\xbf").unwrap_or(bytes);
    parse(&String::from_utf8_lossy(bytes))
}
