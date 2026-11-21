//! Browser clients (the desktop renderer at `t3code://app`, the web client)
//! call this server with `fetch`, so every route must answer CORS the way the
//! reference's global `HttpMiddleware.cors` layer does: a fixed `204` for any
//! preflight and `access-control-allow-origin: *` on every other response,
//! errors included. `/mcp` is the one exception.
//!
//! Requests go over a raw socket so header order and absence are checked
//! exactly as the wire carries them.

use std::time::Duration;

use t3code_server::{
    auth_service::AuthService,
    config::{RuntimeConfig, ServerPaths},
    serve_bound, AppState,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const PREFLIGHT: [(&str, &str); 4] = [
    ("access-control-allow-origin", "*"),
    ("access-control-allow-methods", "GET, POST, OPTIONS"),
    ("access-control-max-age", "600"),
    (
        "access-control-allow-headers",
        "authorization,b3,traceparent,content-type,dpop,x-t3-orchestration-protocol",
    ),
];

struct Reply {
    status: u16,
    /// Lowercased names with their values, in wire order.
    headers: Vec<(String, String)>,
}

impl Reply {
    fn get(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(n, _)| n == name)
            .map(|(_, v)| v.as_str())
    }
    fn cors(&self) -> Vec<(&str, &str)> {
        self.headers
            .iter()
            .filter(|(n, _)| n.starts_with("access-control-"))
            .map(|(n, v)| (n.as_str(), v.as_str()))
            .collect()
    }
}

async fn send(port: u16, method: &str, path: &str, extra: &[(&str, &str)]) -> Reply {
    let mut stream = tokio::net::TcpStream::connect(("127.0.0.1", port))
        .await
        .unwrap();
    let mut head =
        format!("{method} {path} HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nConnection: close\r\n");
    for (name, value) in extra {
        head.push_str(&format!("{name}: {value}\r\n"));
    }
    head.push_str("\r\n");
    stream.write_all(head.as_bytes()).await.unwrap();
    let mut raw = String::new();
    tokio::time::timeout(Duration::from_secs(10), stream.read_to_string(&mut raw))
        .await
        .unwrap()
        .unwrap();
    let head = raw.split("\r\n\r\n").next().unwrap();
    let mut lines = head.split("\r\n");
    let status = lines.next().unwrap().split(' ').nth(1).unwrap();
    let headers = lines
        .map(|line| {
            let (name, value) = line.split_once(':').unwrap();
            (name.to_ascii_lowercase(), value.trim().to_owned())
        })
        .collect();
    Reply {
        status: status.parse().unwrap(),
        headers,
    }
}

async fn preflight(port: u16, path: &str, origin: Option<&str>) -> Reply {
    let mut headers = vec![
        ("Access-Control-Request-Method", "GET"),
        (
            "Access-Control-Request-Headers",
            "authorization,traceparent",
        ),
    ];
    if let Some(origin) = origin {
        headers.push(("Origin", origin));
    }
    send(port, "OPTIONS", path, &headers).await
}

#[tokio::test]
async fn answers_browser_cors_like_the_reference() {
    let home = tempfile::tempdir().unwrap();
    let paths = ServerPaths::from_base(home.path());
    let config = RuntimeConfig {
        mode: "web".into(),
        ..RuntimeConfig::default()
    };
    let state = AppState::with_config(&paths.state_dir, Vec::new(), AuthService::new(), config)
        .await
        .unwrap();
    // Tests never run the host's provider CLIs.
    state.disable_provider_status_checks();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let (stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();
    let server = tokio::spawn(serve_bound(state, listener, None, async move {
        let _ = stop_rx.await;
    }));

    // Preflights: the same fixed 204 for public, authenticated, POST-only,
    // WebSocket and unknown routes, whatever the Origin (or none).
    for path in [
        "/",
        "/.well-known/t3/environment",
        "/api/auth/session",
        "/oauth/token",
        "/api/auth/websocket-ticket",
        "/api/projects",
        "/api/orchestration/shell",
        "/ws",
        "/no-such-route",
        "/mcp/",
    ] {
        for origin in [Some("t3code://app"), Some("http://localhost:5173"), None] {
            let reply = preflight(port, path, origin).await;
            assert_eq!(reply.status, 204, "OPTIONS {path} {origin:?}");
            assert_eq!(reply.cors(), PREFLIGHT, "OPTIONS {path} {origin:?}");
            assert_eq!(reply.get("vary"), None, "OPTIONS {path} {origin:?}");
            assert_eq!(reply.get("content-type"), None, "OPTIONS {path}");
        }
    }

    // The allowed headers are a fixed list, never an echo of the request,
    // and private-network access is not granted.
    let reply = send(
        port,
        "OPTIONS",
        "/.well-known/t3/environment",
        &[
            ("Origin", "t3code://app"),
            ("Access-Control-Request-Method", "DELETE"),
            ("Access-Control-Request-Headers", "x-foo"),
            ("Access-Control-Request-Private-Network", "true"),
        ],
    )
    .await;
    assert_eq!(reply.status, 204);
    assert_eq!(reply.cors(), PREFLIGHT);

    // Simple requests: the wildcard origin and nothing else, on success and
    // on every kind of error response.
    for (method, path, status) in [
        ("GET", "/.well-known/t3/environment", 200),
        ("GET", "/api/auth/session", 200),
        ("GET", "/", 200),
        ("GET", "/api/projects", 401),
        ("GET", "/api/orchestration/shell", 401),
        ("POST", "/api/auth/websocket-ticket", 401),
        ("GET", "/ws", 426),
        ("GET", "/ws?orchestrationProtocol=2", 401),
    ] {
        for origin in ["t3code://app", "http://localhost:5173"] {
            let reply = send(port, method, path, &[("Origin", origin)]).await;
            assert_eq!(reply.status, status, "{method} {path}");
            assert_eq!(
                reply.cors(),
                [("access-control-allow-origin", "*")],
                "{method} {path} {origin}"
            );
            assert!(
                reply.get("vary").is_none_or(|v| !v.contains("Origin")),
                "{method} {path}"
            );
        }
    }
    // No Origin at all still gets the wildcard, as in the reference.
    let reply = send(port, "GET", "/.well-known/t3/environment", &[]).await;
    assert_eq!(reply.cors(), [("access-control-allow-origin", "*")]);
    // The origin header comes after the route's own headers, as in the
    // reference's 401: content-type, vary, access-control-allow-origin.
    let reply = send(port, "GET", "/api/projects", &[("Origin", "t3code://app")]).await;
    assert_eq!(
        reply
            .headers
            .iter()
            .map(|(n, _)| n.as_str())
            .collect::<Vec<_>>()[..3],
        ["content-type", "vary", "access-control-allow-origin"],
        "{:?}",
        reply.headers
    );

    // MCP refuses browser origins itself and gets no CORS at all.
    for method in ["GET", "OPTIONS"] {
        let reply = send(port, method, "/mcp", &[("Origin", "t3code://app")]).await;
        assert_eq!(reply.status, 403, "{method} /mcp");
        assert!(reply.cors().is_empty(), "{method} /mcp {:?}", reply.headers);
        let reply = send(port, method, "/mcp?x=1", &[]).await;
        assert_eq!(reply.status, 401, "{method} /mcp?x=1");
        assert!(reply.cors().is_empty(), "{method} /mcp?x=1");
    }

    stop_tx.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(15), server)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
}
