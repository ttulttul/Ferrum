use super::{header, HeaderMap, Request, Response};
use crate::{
    auth_service::AuthService,
    dpop_proof::{verify_proof, CheckedProof},
};
use axum::middleware::Next;
use std::sync::OnceLock;

struct RequestContext {
    method: String,
    url: Option<String>,
    validated: OnceLock<(String, String)>,
    client: serde_json::Value,
}

tokio::task_local! {static CONTEXT:RequestContext;}

// Request metadata comes from the server transport. The per-request cache lets several
// authorization layers examine one proof while consuming its replay marker exactly once.
pub(crate) async fn contextualize(request: Request, next: Next) -> Response {
    #[cfg(feature = "native")]
    let peer = request
        .extensions()
        .get::<axum::extract::ConnectInfo<std::net::SocketAddr>>()
        .map(|p| p.0.ip().to_canonical());
    #[cfg(not(feature = "native"))]
    let peer = None;
    let context = request_context(
        request.method().as_str(),
        request.uri().path(),
        request.headers(),
        peer,
    );
    CONTEXT.scope(context, next.run(request)).await
}

/// Runs `f` with the request context of a request the router did not see,
/// such as a socket upgrade a host accepts itself, so proofs it checks are
/// verified against that request.
pub(crate) fn with_request_context<T>(
    method: &str,
    uri: &axum::http::Uri,
    headers: &HeaderMap,
    f: impl FnOnce() -> T,
) -> T {
    CONTEXT.sync_scope(request_context(method, uri.path(), headers, None), f)
}

fn request_context(
    method: &str,
    path: &str,
    h: &HeaderMap,
    peer: Option<std::net::IpAddr>,
) -> RequestContext {
    let scheme = if header(h, "x-forwarded-proto") == Some("https") {
        "https"
    } else {
        "http"
    };
    let url = header(h, "host").and_then(|host| {
        let mut url = url::Url::parse(&format!("{scheme}://{host}")).ok()?;
        if url.path() != "/"
            || url.query().is_some()
            || url.fragment().is_some()
            || !url.username().is_empty()
            || url.password().is_some()
        {
            return None;
        }
        url.set_path(path);
        Some(url.to_string())
    });
    // Without a socket peer, the client address comes from the CF-Connecting-IP header.
    let peer = peer.or_else(|| {
        header(h, "cf-connecting-ip").and_then(|ip| ip.parse::<std::net::IpAddr>().ok())
    });
    RequestContext {
        client: derive_client(h, peer),
        method: method.to_owned(),
        url,
        validated: OnceLock::new(),
    }
}

pub(super) fn check(
    proof: &str,
    binding: Option<(&str, &str)>,
) -> Result<CheckedProof, &'static str> {
    CONTEXT
        .try_with(|ctx| {
            let url = ctx.url.as_deref().ok_or("invalid_proof")?;
            verify_proof(proof, &ctx.method, url, binding)
        })
        .unwrap_or(Err("invalid_proof"))
}
pub(super) fn authenticate(
    auth: &AuthService,
    proof: &str,
    thumb: &str,
    token: &str,
) -> Result<(), &'static str> {
    CONTEXT
        .try_with(|ctx| {
            if ctx
                .validated
                .get()
                .is_some_and(|(p, t)| p == proof && t == token)
            {
                return Ok(());
            }
            let checked = check(proof, Some((thumb, token)))?;
            auth.consume_proof(&checked)?;
            let _ = ctx.validated.set((proof.to_owned(), token.to_owned()));
            Ok(())
        })
        .unwrap_or(Err("invalid_proof"))
}
pub(crate) fn reason(reason: &str) -> bool {
    matches!(
        reason,
        "invalid_proof"
            | "key_mismatch"
            | "token_mismatch"
            | "request_mismatch"
            | "time_window"
            | "replay"
    )
}
pub(crate) fn uses_proof(h: &HeaderMap) -> bool {
    header(h, "authorization")
        .and_then(|s| s.split_once(' '))
        .is_some_and(|(scheme, _)| scheme.eq_ignore_ascii_case("DPoP"))
        || h.contains_key("dpop")
}

fn derive_client(h: &HeaderMap, peer: Option<std::net::IpAddr>) -> serde_json::Value {
    use serde_json::json;
    let mut v = json!({});
    if let Some(ip) = peer {
        let forwarded = ip
            .is_loopback()
            .then(|| {
                header(h, "x-forwarded-for")?
                    .rsplit(',')
                    .next()?
                    .trim()
                    .parse::<std::net::IpAddr>()
                    .ok()
            })
            .flatten();
        v["ipAddress"] = json!(forwarded.unwrap_or(ip).to_string());
    }
    let ua = header(h, "user-agent")
        .map(str::trim)
        .filter(|s| !s.is_empty());
    if let Some(ua) = ua {
        v["userAgent"] = json!(ua);
    }
    let ua = ua.unwrap_or("").to_lowercase();
    let has = |keys: &[&str]| keys.iter().any(|key| ua.contains(key));
    v["deviceType"] = json!(if ua.is_empty() {
        "unknown"
    } else if has(&["bot", "crawler", "spider", "slurp", "curl", "wget"]) {
        "bot"
    } else if has(&["ipad", "tablet"]) {
        "tablet"
    } else if has(&["iphone", "mobile"]) {
        "mobile"
    } else {
        "desktop"
    });
    for (keys, value) in [
        (&["iphone", "ipad", "ipod"][..], "iOS"),
        (&["android"], "Android"),
        (&["mac os x", "macintosh"], "macOS"),
        (&["windows nt"], "Windows"),
        (&["linux"], "Linux"),
    ] {
        if has(keys) {
            v["os"] = json!(value);
            break;
        }
    }
    for (keys, value) in [
        (&["edg/", "edge/", "edga/", "edgios/"][..], "Edge"),
        (&["opr/", "opios/"], "Opera"),
        (&["firefox/", "fxios/"], "Firefox"),
        (&["electron/"], "Electron"),
        (&["chrome/", "crios/"], "Chrome"),
        (&["safari/"], "Safari"),
    ] {
        if has(keys) {
            v["browser"] = json!(value);
            break;
        }
    }
    v
}
tokio::task_local! {pub(super) static PRESENTED:serde_json::Value;}
pub(crate) fn client_metadata() -> serde_json::Value {
    let client = CONTEXT
        .try_with(|ctx| ctx.client.clone())
        .unwrap_or_else(|_| serde_json::json!({"deviceType":"unknown"}));
    PRESENTED
        .try_with(|v| {
            let mut result = serde_json::json!({});
            if let Some(label) = v.get("label") {
                result["label"] = label.clone();
            }
            for key in ["ipAddress", "userAgent", "deviceType", "os", "browser"] {
                if let Some(val) = v.get(key).or_else(|| client.get(key)) {
                    result[key] = val.clone();
                }
            }
            result
        })
        .unwrap_or(client)
}
