//! Web client serving for non-API paths, following `apps/server/src/http.ts`.
//!
//! With a static directory configured the built client is served with an
//! `index.html` fallback for client-side routes. Without one, `/` still
//! answers 200 with a placeholder page: the desktop SSH launcher only adopts
//! a running server whose `GET /` is 2xx.

use std::path::{Component, Path, PathBuf};

use axum::{
    http::{header, Method, StatusCode},
    response::{IntoResponse, Response},
};

const PLACEHOLDER: &str = "<!doctype html>\n<html lang=\"en\"><head><meta charset=\"utf-8\"><title>T3 Code server</title></head>\n<body><h1>T3 Code server</h1><p>This T3 Code server is running. No web client bundle is configured; connect with the T3 Code app, or start the server with <code>--static-dir</code> to serve one here.</p></body></html>\n";

pub(crate) fn no_assets() -> Response {
    (
        StatusCode::SERVICE_UNAVAILABLE,
        [(header::CONTENT_TYPE, "text/plain")],
        "No static directory configured and no dev URL set.",
    )
        .into_response()
}

fn text(status: StatusCode, body: &'static str) -> Response {
    (status, [(header::CONTENT_TYPE, "text/plain")], body).into_response()
}

pub(crate) async fn client(static_dir: Option<&Path>, method: &Method, path: &str) -> Response {
    if method != Method::GET && method != Method::HEAD {
        return no_assets();
    }
    let Some(root) = static_dir else {
        if path == "/" {
            return (
                StatusCode::OK,
                [
                    (header::CONTENT_TYPE, "text/html; charset=utf-8"),
                    (header::CACHE_CONTROL, "no-store"),
                ],
                PLACEHOLDER,
            )
                .into_response();
        }
        return no_assets();
    };
    let Some(relative) = static_relative_path(path) else {
        return text(StatusCode::BAD_REQUEST, "Invalid static file path");
    };
    let mut file = root.join(&relative);
    if file.extension().is_none() {
        file.push("index.html");
    }
    let (file, body) = match tokio::fs::read(&file).await {
        Ok(body) => (file, body),
        Err(_) => {
            let index = root.join("index.html");
            match tokio::fs::read(&index).await {
                Ok(body) => (index, body),
                Err(_) => return text(StatusCode::NOT_FOUND, "Not Found"),
            }
        }
    };
    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, mime_for(&file))],
        body,
    )
        .into_response()
}

/// The request path relative to the static root, or `None` when it would
/// escape the root.
fn static_relative_path(path: &str) -> Option<PathBuf> {
    let path = if path == "/" { "/index.html" } else { path };
    let raw = path.trim_start_matches(['/', '\\']);
    if raw.is_empty() || raw.contains('\0') || raw.contains('\\') {
        return None;
    }
    let mut out = PathBuf::new();
    for component in Path::new(raw).components() {
        match component {
            Component::Normal(part) => out.push(part),
            Component::CurDir => {}
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => return None,
        }
    }
    (!out.as_os_str().is_empty()).then_some(out)
}

fn mime_for(path: &Path) -> &'static str {
    match path
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or_default()
        .to_ascii_lowercase()
        .as_str()
    {
        "html" | "htm" => "text/html; charset=utf-8",
        "js" | "mjs" => "text/javascript; charset=utf-8",
        "css" => "text/css; charset=utf-8",
        "json" | "map" => "application/json",
        "webmanifest" => "application/manifest+json",
        "txt" => "text/plain; charset=utf-8",
        "svg" => "image/svg+xml",
        "png" => "image/png",
        "jpg" | "jpeg" => "image/jpeg",
        "gif" => "image/gif",
        "webp" => "image/webp",
        "ico" => "image/x-icon",
        "woff" => "font/woff",
        "woff2" => "font/woff2",
        "wasm" => "application/wasm",
        _ => "application/octet-stream",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_paths_that_escape_the_root() {
        assert_eq!(static_relative_path("/"), Some(PathBuf::from("index.html")));
        assert_eq!(
            static_relative_path("/assets/app.js"),
            Some(PathBuf::from("assets/app.js"))
        );
        assert_eq!(static_relative_path("/../etc/passwd"), None);
        assert_eq!(static_relative_path("/a/../../b"), None);
        assert_eq!(static_relative_path("/a\\..\\b"), None);
    }

    #[tokio::test]
    async fn placeholder_at_root_without_static_dir() {
        let response = client(None, &Method::GET, "/").await;
        assert_eq!(response.status(), StatusCode::OK);
        let response = client(None, &Method::GET, "/missing").await;
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        let response = client(None, &Method::POST, "/").await;
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn serves_static_dir_with_spa_fallback() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("index.html"), "<p>app</p>").unwrap();
        std::fs::create_dir(dir.path().join("assets")).unwrap();
        std::fs::write(dir.path().join("assets/app.js"), "1").unwrap();
        let root = Some(dir.path());
        for (path, mime) in [
            ("/", "text/html; charset=utf-8"),
            ("/threads/abc", "text/html; charset=utf-8"),
            ("/assets/app.js", "text/javascript; charset=utf-8"),
            ("/assets/gone.js", "text/html; charset=utf-8"),
        ] {
            let response = client(root, &Method::GET, path).await;
            assert_eq!(response.status(), StatusCode::OK, "{path}");
            assert_eq!(response.headers()[header::CONTENT_TYPE], mime, "{path}");
        }
        let response = client(root, &Method::GET, "/../x").await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }
}
