use super::{encode_component, text};
use axum::{
    body::Body,
    http::{header::*, HeaderMap, Method, StatusCode},
    response::{IntoResponse, Response},
};
use serde_json::Value;
use std::{io, path::Path, time::UNIX_EPOCH};
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tokio_util::io::ReaderStream;

pub(super) fn valid_mime(mime: &str) -> bool {
    let token = |s: &str| {
        !s.is_empty()
            && s.bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"_!#$&^.+-".contains(&b))
    };
    mime.split_once('/')
        .is_some_and(|(ty, sub)| token(ty) && token(sub))
}

fn disposition(name: Option<&str>) -> String {
    let Some(name) = name else {
        return "attachment".into();
    };
    let name: String = name
        .chars()
        .map(|c| {
            if c.is_control() || matches!(c, '"' | '\\') {
                '_'
            } else {
                c
            }
        })
        .collect();
    let ascii: String = name
        .encode_utf16()
        .map(|c| if c <= 126 { char::from(c as u8) } else { '_' })
        .collect();
    let mut value = format!("attachment; filename=\"{ascii}\"");
    if ascii != name {
        let encoded = encode_component(&name)
            .replace('\'', "%27")
            .replace('(', "%28")
            .replace(')', "%29")
            .replace('*', "%2A");
        value.push_str(&format!("; filename*=UTF-8''{encoded}"));
    }
    value
}

fn response_headers(path: &Path, claims: &Value) -> HeaderMap {
    let mut headers = HeaderMap::new();
    let ext = path
        .extension()
        .and_then(|s| s.to_str())
        .unwrap_or("")
        .to_ascii_lowercase();
    let supplied = claims["mimeType"].as_str().unwrap_or("");
    let bare = supplied.split(';').next().unwrap_or("").trim();
    let lower = bare.to_ascii_lowercase();
    let mut mime = None;
    let mut csp = None;
    if claims["download"] == true {
        headers.insert(
            CONTENT_DISPOSITION,
            disposition(claims["fileName"].as_str()).parse().unwrap(),
        );
        csp = Some("default-src 'none'; sandbox");
        mime = Some(
            if valid_mime(supplied)
                && lower != "text/html"
                && !lower.ends_with("+xml")
                && !lower.ends_with("/xml")
                && !lower.contains("/xml-")
            {
                supplied
            } else {
                "application/octet-stream"
            },
        );
    } else {
        if valid_mime(bare) && (lower.starts_with("audio/") || lower.starts_with("video/")) {
            mime = Some(bare);
        } else if lower == "application/pdf" {
            mime = Some("application/pdf");
        } else if lower == "text/html" || matches!(ext.as_str(), "html" | "htm") {
            mime = Some("text/html; charset=utf-8");
            csp = Some("sandbox allow-scripts allow-forms allow-popups allow-downloads");
        }
        if ext == "svg" {
            csp = Some("default-src 'none'; style-src 'unsafe-inline'; sandbox");
        }
    }
    if let Some(csp) = csp {
        headers.insert(CONTENT_SECURITY_POLICY, csp.parse().unwrap());
    }
    let is_media = mime.is_some_and(|m| {
        let m = m.to_ascii_lowercase();
        m.starts_with("video/") || m.starts_with("audio/")
    });
    headers.insert(
        CACHE_CONTROL,
        if is_media {
            "private, no-store"
        } else {
            "private, max-age=3600"
        }
        .parse()
        .unwrap(),
    );
    headers.insert(X_CONTENT_TYPE_OPTIONS, "nosniff".parse().unwrap());
    if let Some(mime) = mime {
        headers.insert(CONTENT_TYPE, mime.parse().unwrap());
    }
    headers
}

// None means ignore a malformed or multipart range. Err means a valid but unsatisfiable range.
fn byte_range(header: &str, size: u64) -> Option<Result<(u64, u64), ()>> {
    let (unit, range) = header.trim().split_once('=')?;
    if !unit.eq_ignore_ascii_case("bytes") {
        return None;
    }
    let (first, last) = range.split_once('-')?;
    if (first.is_empty() && last.is_empty())
        || !first
            .bytes()
            .chain(last.bytes())
            .all(|c| c.is_ascii_digit())
    {
        return None;
    }
    let number = |s: &str| s.parse::<u64>().unwrap_or(u64::MAX);
    if first.is_empty() {
        let count = number(last);
        return Some(if count == 0 || size == 0 {
            Err(())
        } else {
            Ok((size.saturating_sub(count), size - 1))
        });
    }
    let start = number(first);
    let end = number(last);
    if end < start {
        return None;
    }
    Some(if start >= size {
        Err(())
    } else {
        Ok((start, end.min(size - 1)))
    })
}

pub(super) async fn serve_file(
    path: &Path,
    claims: &Value,
    method: &Method,
    request: &HeaderMap,
) -> io::Result<Response> {
    let mut file = tokio::fs::File::open(path).await?;
    let metadata = file.metadata().await?;
    let size = metadata.len();
    let mut headers = response_headers(path, claims);
    if claims["renderIntent"] == "mcp-app"
        && claims["download"] != true
        && claims["mimeType"] == "text/html"
    {
        const LIMIT: u64 = 5 * 1024 * 1024 + 128 * 1024;
        let mut bytes = Vec::new();
        file.take(LIMIT + 1).read_to_end(&mut bytes).await?;
        if bytes.len() as u64 > LIMIT {
            return Ok(text(
                StatusCode::PAYLOAD_TOO_LARGE,
                "MCP app is too large to preview.",
            ));
        }
        let mut html = mcp_html(&bytes).into_bytes();
        if method != Method::HEAD {
            if let Some(algorithm) =
                super::compression::negotiate(&mut headers, request, html.len() as u64)
            {
                let mut compressed = Vec::new();
                super::compression::encoder(algorithm, html.as_slice())
                    .read_to_end(&mut compressed)
                    .await?;
                html = compressed;
            }
        }
        headers.insert(CONTENT_LENGTH, html.len().into());
        return Ok((
            headers,
            if method == Method::HEAD {
                Body::empty()
            } else {
                Body::from(html)
            },
        )
            .into_response());
    }
    headers.insert(ACCEPT_RANGES, "bytes".parse().unwrap());
    let mut status = StatusCode::OK;
    let mut length = size;
    if method == Method::GET && !request.contains_key(IF_RANGE) {
        if let Some(range) = request
            .get(RANGE)
            .and_then(|h| h.to_str().ok())
            .and_then(|h| byte_range(h, size))
        {
            let Ok((start, end)) = range else {
                headers.insert(CONTENT_RANGE, format!("bytes */{size}").parse().unwrap());
                let empty = futures_util::stream::empty::<io::Result<axum::body::Bytes>>();
                return Ok((
                    StatusCode::RANGE_NOT_SATISFIABLE,
                    headers,
                    Body::from_stream(empty),
                )
                    .into_response());
            };
            file.seek(io::SeekFrom::Start(start)).await?;
            length = end - start + 1;
            status = StatusCode::PARTIAL_CONTENT;
            headers.insert(
                CONTENT_RANGE,
                format!("bytes {start}-{end}/{size}").parse().unwrap(),
            );
        }
    }
    let modified = metadata.modified()?;
    let millis = modified
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    headers.insert(ETAG, format!("\"{size:x}-{millis:x}\"").parse().unwrap());
    headers.insert(
        LAST_MODIFIED,
        httpdate::fmt_http_date(modified).parse().unwrap(),
    );
    headers.entry(CONTENT_TYPE).or_insert_with(|| {
        mime_guess::from_path(path)
            .first_raw()
            .unwrap_or("application/octet-stream")
            .parse()
            .unwrap()
    });
    headers.insert(CONTENT_LENGTH, length.into());
    let range = file.take(length);
    let algorithm = (status == StatusCode::OK)
        .then(|| super::compression::negotiate(&mut headers, request, length))
        .flatten();
    let body = if let Some(algorithm) = algorithm {
        headers.remove(CONTENT_LENGTH);
        Body::from_stream(ReaderStream::new(super::compression::encoder(
            algorithm,
            tokio::io::BufReader::new(range),
        )))
    } else if method == Method::HEAD {
        Body::empty()
    } else {
        Body::from_stream(ReaderStream::new(range))
    };
    Ok((status, headers, body).into_response())
}

fn mcp_html(bytes: &[u8]) -> String {
    use regex::Regex;
    use std::sync::LazyLock;
    static THEME_PREFIX: LazyLock<Regex> = LazyLock::new(|| {
        Regex::new(r#"(?i)^\u{feff}?\s*<!doctype[^>]*>\s*(?:<meta\s+http-equiv="Content-Security-Policy"[^>]*>)?"#).unwrap()
    });
    static BOOTSTRAP_PREFIX: LazyLock<Regex> = LazyLock::new(|| {
        Regex::new(r#"(?i)^(?:\u{feff}?\s*<!doctype[^>]*>\s*)?(?:<meta http-equiv="Content-Security-Policy" content="[^"]*">)?"#).unwrap()
    });
    let bytes = bytes.strip_prefix(&[0xef, 0xbb, 0xbf]).unwrap_or(bytes);
    let mut html = String::from_utf8_lossy(bytes).into_owned();
    let index = match THEME_PREFIX.find(&html) {
        Some(m) => m.end(),
        None => {
            html.insert_str(0, "<!doctype html>");
            "<!doctype html>".len()
        }
    };
    html.insert_str(index, include_str!("mcp_theme.html"));
    let index = BOOTSTRAP_PREFIX.find(&html).unwrap().end();
    html.insert_str(index, include_str!("mcp_cookie.html"));
    // Bind the host's private port to this document before application scripts execute.
    html.insert_str(index, r#"<script>(function(){var host=window.parent,port=new MessageChannel(),own=port.port1;own.onmessage=function(event){var data=event.data;if(!data)return;if("alive" in data)own.postMessage({alive:data.alive});else window.dispatchEvent(new MessageEvent("message",{data:data.message,origin:data.origin,source:host}));};host.postMessage({t3:"t3-mcp-app-document"},"*",[port.port2]);})();</script>"#);
    html
}
