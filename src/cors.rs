//! Browser CORS for every HTTP route, mirroring the reference's
//! `layerBrowserApiCors` (`apps/server/src/http.ts`), which wraps the whole
//! router in Effect's `HttpMiddleware.cors` with the options in
//! `apps/server/src/httpCors.ts`.
//!
//! The desktop renderer (origin `t3code://app`) and web clients reach the
//! server with browser `fetch`, so without these headers every cross-origin
//! call fails as a transport error even though the server answered.
//!
//! The reference only switches to an origin allow-list with credentials when
//! it runs behind a dev server (`config.devUrl`). This server never does (see
//! `RuntimeRecord::dev_url`), so the packaged wildcard mode is the only one:
//!
//! - every `OPTIONS` request, on any path, is answered `204` without reaching
//!   a route, with a fixed method, max-age and header list that does not
//!   echo the request;
//! - every other response, errors included, gets
//!   `access-control-allow-origin: *`;
//! - no `vary`, `access-control-allow-credentials`,
//!   `access-control-expose-headers` or
//!   `access-control-allow-private-network`;
//! - `/mcp` is skipped entirely, because MCP refuses every browser origin.
//!
//! The reference's WebSocket `101` is written by the `ws` library outside the
//! response pipeline, so it carries no CORS header; failed upgrades (401, 426)
//! are ordinary responses and do.

use axum::{
    body::Body,
    extract::Request,
    http::{header, HeaderValue, Method, StatusCode},
    middleware::Next,
    response::Response,
};

/// `browserApiCorsAllowedMethods`, joined with `", "` as Effect does.
pub const ALLOWED_METHODS: &str = "GET, POST, OPTIONS";
/// `browserApiCorsAllowedHeaders`, joined with `","` as Effect does.
pub const ALLOWED_HEADERS: &str =
    "authorization,b3,traceparent,content-type,dpop,x-t3-orchestration-protocol";
pub const MAX_AGE: &str = "600";
/// `MCP_HTTP_PATH`: compared against the path without its query.
const MCP_HTTP_PATH: &str = "/mcp";

pub async fn browser_api(request: Request, next: Next) -> Response {
    if request.uri().path() == MCP_HTTP_PATH {
        return next.run(request).await;
    }
    if request.method() == Method::OPTIONS {
        return preflight();
    }
    let mut response = next.run(request).await;
    if response.status() != StatusCode::SWITCHING_PROTOCOLS {
        // `insert` keeps an existing header's position, as Effect's
        // `setHeaders` does, and appends it otherwise.
        response.headers_mut().insert(
            header::ACCESS_CONTROL_ALLOW_ORIGIN,
            HeaderValue::from_static("*"),
        );
    }
    response
}

fn preflight() -> Response {
    // Header order follows the reference: origin, methods, max-age, headers.
    Response::builder()
        .status(StatusCode::NO_CONTENT)
        .header(header::ACCESS_CONTROL_ALLOW_ORIGIN, "*")
        .header(header::ACCESS_CONTROL_ALLOW_METHODS, ALLOWED_METHODS)
        .header(header::ACCESS_CONTROL_MAX_AGE, MAX_AGE)
        .header(header::ACCESS_CONTROL_ALLOW_HEADERS, ALLOWED_HEADERS)
        .body(Body::empty())
        .expect("valid preflight response")
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{routing::get, Router};
    use tower::ServiceExt;

    #[tokio::test]
    async fn leaves_switching_protocols_untouched() {
        let app = Router::new()
            .route("/ws", get(|| async { StatusCode::SWITCHING_PROTOCOLS }))
            .layer(axum::middleware::from_fn(browser_api));
        let request = Request::builder()
            .uri("/ws")
            .header(header::ORIGIN, "t3code://app")
            .body(Body::empty())
            .unwrap();
        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::SWITCHING_PROTOCOLS);
        assert!(response
            .headers()
            .get(header::ACCESS_CONTROL_ALLOW_ORIGIN)
            .is_none());
    }
}
