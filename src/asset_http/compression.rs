use async_compression::{
    tokio::bufread::{BrotliEncoder, GzipEncoder, ZlibEncoder},
    Level,
};
use axum::http::{header::*, HeaderMap};
use regex::Regex;
use std::{collections::HashMap, sync::LazyLock};
use tokio::io::{AsyncBufRead, AsyncRead};

// Effect's HTTP middleware prefers br, gzip, then deflate among encodings with positive quality.
pub(super) fn negotiate(
    headers: &mut HeaderMap,
    request: &HeaderMap,
    length: u64,
) -> Option<&'static str> {
    let mime = headers
        .get(CONTENT_TYPE)?
        .to_str()
        .ok()?
        .split(';')
        .next()?
        .trim()
        .to_ascii_lowercase();
    if !(mime.starts_with("text/")
        || mime.ends_with("+json")
        || mime.ends_with("+xml")
        || matches!(
            mime.as_str(),
            "application/json" | "application/javascript" | "application/xml" | "application/wasm"
        ))
    {
        return None;
    }
    headers.insert(VARY, "Accept-Encoding".parse().unwrap());
    if length < 1024 {
        return None;
    }
    static MEMBER: LazyLock<Regex> = LazyLock::new(|| {
        Regex::new(r"^([a-z0-9!#$%&'*+.^_\x60|~-]+)(?:[ \t]*;[ \t]*q=(0(?:\.[0-9]{0,3})?|1(?:\.0{0,3})?))?$").unwrap()
    });
    let accept = request
        .get(ACCEPT_ENCODING)?
        .to_str()
        .ok()?
        .to_ascii_lowercase();
    let mut accepted = HashMap::new();
    for member in accept.split(',').map(str::trim).filter(|s| !s.is_empty()) {
        let caps = MEMBER.captures(member)?;
        accepted.insert(
            caps.get(1)?.as_str(),
            caps.get(2)
                .is_none_or(|q| q.as_str().bytes().any(|b| b.is_ascii_digit() && b != b'0')),
        );
    }
    let algorithm = ["br", "gzip", "deflate"].into_iter().find(|s| {
        accepted
            .get(s)
            .or_else(|| accepted.get("*"))
            .copied()
            .unwrap_or(false)
    })?;
    headers.insert(CONTENT_ENCODING, algorithm.parse().unwrap());
    Some(algorithm)
}

pub(super) fn encoder<'a>(
    algorithm: &str,
    reader: impl AsyncBufRead + Send + Unpin + 'a,
) -> Box<dyn AsyncRead + Send + Unpin + 'a> {
    match algorithm {
        "br" => Box::new(BrotliEncoder::with_quality(reader, Level::Precise(4))),
        "gzip" => Box::new(GzipEncoder::with_quality(reader, Level::Precise(6))),
        _ => Box::new(ZlibEncoder::with_quality(reader, Level::Precise(6))),
    }
}
