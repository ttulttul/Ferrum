//! The desktop's SSH pairing against this server: `t3 auth pairing create` writes a
//! one-time link into the TypeScript `userdata/statev2.sqlite` out of process, then the
//! desktop exchanges it at `/oauth/token` (`bootstrapRemoteBearerSession`) and uses the
//! bearer session. Before this was supported every exchange answered 401.
//!
//! Everything runs in a temporary T3 home on an ephemeral port.

mod support;

use std::time::Duration;

use rusqlite::{params, Connection};
use serde_json::Value;
use t3code_server::{
    auth_service::AuthService,
    config::{RuntimeConfig, ServerPaths},
    serve_bound,
    server_runtime::ServerOwnership,
    AppState,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const STANDARD_SCOPES: &str = r#"["orchestration:read","orchestration:operate","settings:write","providers:manage","environment:maintain","preview:operate","diagnostics:read","terminal:read","terminal:operate","source-control:write","filesystem:read","filesystem:write","relay:read","access:read","access:write","relay:write"]"#;

fn iso(offset: time::Duration) -> String {
    (time::OffsetDateTime::now_utc() + offset)
        .format(time::macros::format_description!(
            "[year]-[month]-[day]T[hour]:[minute]:[second].[subsecond digits:3]Z"
        ))
        .unwrap()
}

/// What `t3 auth pairing create --json` inserts (`PairingGrantStore.issueOneTimeToken`).
fn cli_issue(db: &Connection, id: &str, credential: &str, scopes: &str, ttl: time::Duration) {
    db.execute(
        "INSERT INTO auth_pairing_links (id, credential, method, scopes, subject, label,
           proof_key_thumbprint, requires_connect_approval, created_at, expires_at, consumed_at, revoked_at)
         VALUES (?1, ?2, 'one-time-token', ?3, 'one-time-token', NULL, NULL, 0, ?4, ?5, NULL, NULL)",
        params![id, credential, scopes, iso(time::Duration::ZERO), iso(ttl)],
    )
    .unwrap();
}

async fn request(port: u16, raw: String) -> (u16, Value) {
    let mut stream = tokio::net::TcpStream::connect(("127.0.0.1", port))
        .await
        .unwrap();
    stream.write_all(raw.as_bytes()).await.unwrap();
    let mut response = String::new();
    tokio::time::timeout(
        Duration::from_secs(10),
        stream.read_to_string(&mut response),
    )
    .await
    .unwrap()
    .unwrap();
    let status = response[9..12].parse().unwrap();
    let body = response.split_once("\r\n\r\n").unwrap().1;
    (status, serde_json::from_str(body).unwrap_or(Value::Null))
}

/// The body `bootstrapRemoteBearerSession` sends.
async fn exchange(port: u16, credential: &str, scope: Option<&str>) -> (u16, Value) {
    let mut body = format!(
        "grant_type=urn%3Aietf%3Aparams%3Aoauth%3Agrant-type%3Atoken-exchange&subject_token={credential}\
         &subject_token_type=urn%3At3%3Aparams%3Aoauth%3Atoken-type%3Aenvironment-bootstrap\
         &requested_token_type=urn%3Aietf%3Aparams%3Aoauth%3Atoken-type%3Aaccess_token"
    );
    if let Some(scope) = scope {
        body.push_str(&format!("&scope={}", scope.replace(' ', "+")));
    }
    request(
        port,
        format!(
            "POST /oauth/token HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nConnection: close\r\n\
             Content-Type: application/x-www-form-urlencoded\r\nContent-Length: {}\r\n\r\n{body}",
            body.len()
        ),
    )
    .await
}

async fn bearer(port: u16, method: &str, path: &str, token: &str) -> (u16, Value) {
    request(
        port,
        format!(
            "{method} {path} HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nConnection: close\r\n\
             Authorization: Bearer {token}\r\nContent-Length: 0\r\n\r\n"
        ),
    )
    .await
}

fn consumed(db: &Connection, id: &str) -> bool {
    db.query_row(
        "SELECT consumed_at IS NOT NULL FROM auth_pairing_links WHERE id = ?1",
        [id],
        |row| row.get(0),
    )
    .unwrap()
}

#[tokio::test]
async fn exchanges_a_pairing_token_issued_out_of_process_by_the_cli() {
    let home = tempfile::tempdir().unwrap();
    let paths = ServerPaths::from_base(home.path());
    std::fs::create_dir_all(&paths.state_dir).unwrap();
    // The reference's schema at the pinned migration head, as `t3` creates it.
    let db = t3code_server::statev2::open(&paths.state_dir).unwrap();

    let ownership = ServerOwnership::acquire(&paths.runtime_state).unwrap();
    let config = RuntimeConfig {
        mode: "web".into(),
        ..RuntimeConfig::default()
    };
    support::isolate_host();
    let state = AppState::with_config(&paths.state_dir, Vec::new(), AuthService::new(), config)
        .await
        .unwrap();
    // Tests never run the host's provider CLIs.
    state.disable_provider_status_checks();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let (stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();
    let server = tokio::spawn(serve_bound(state, listener, Some(ownership), async move {
        let _ = stop_rx.await;
    }));

    // Issued while the server runs, by another process writing the shared database.
    let five_minutes = time::Duration::minutes(5);
    cli_issue(&db, "ssh", "ABCDEFGHJKMN", STANDARD_SCOPES, five_minutes);
    let (status, issued) = exchange(port, "ABCDEFGHJKMN", None).await;
    assert_eq!(status, 200, "{issued}");
    assert_eq!(issued["token_type"], "Bearer");
    assert_eq!(
        issued["issued_token_type"],
        "urn:ietf:params:oauth:token-type:access_token"
    );
    assert_eq!(issued["scope"].as_str().unwrap().split(' ').count(), 16);
    assert!(consumed(&db, "ssh"));

    // The bearer session works for the desktop's follow-up requests.
    let token = issued["access_token"].as_str().unwrap();
    let (status, session) = bearer(port, "GET", "/api/auth/session", token).await;
    assert_eq!(status, 200);
    assert_eq!(session["authenticated"], true, "{session}");
    assert_eq!(session["sessionMethod"], "bearer-access-token");
    let (status, ticket) = bearer(port, "POST", "/api/auth/websocket-ticket", token).await;
    assert_eq!(status, 200, "{ticket}");
    assert!(ticket["ticket"].is_string());

    // Single use, like the TypeScript server.
    let (status, replay) = exchange(port, "ABCDEFGHJKMN", None).await;
    assert_eq!(status, 401);
    assert_eq!(replay["reason"], "invalid_credential");

    // Expired and revoked links are refused and left as they were.
    cli_issue(
        &db,
        "old",
        "ZZZZZZZZZZZZ",
        STANDARD_SCOPES,
        -time::Duration::SECOND,
    );
    cli_issue(
        &db,
        "revoked",
        "YYYYYYYYYYYY",
        STANDARD_SCOPES,
        five_minutes,
    );
    db.execute(
        "UPDATE auth_pairing_links SET revoked_at = ?1 WHERE id = 'revoked'",
        [iso(time::Duration::ZERO)],
    )
    .unwrap();
    for (id, code) in [("old", "ZZZZZZZZZZZZ"), ("revoked", "YYYYYYYYYYYY")] {
        let (status, body) = exchange(port, code, None).await;
        assert_eq!(
            (status, &body["reason"]),
            (401, &Value::from("invalid_credential"))
        );
        assert!(!consumed(&db, id), "{id}");
    }

    // A link that grants none of the requested scopes is refused without being spent;
    // an overlapping request narrows the session to the granted scopes.
    cli_issue(
        &db,
        "scoped",
        "XXXXXXXXXXXX",
        r#"["orchestration:read"]"#,
        five_minutes,
    );
    let (status, body) = exchange(port, "XXXXXXXXXXXX", Some("settings:write")).await;
    assert_eq!(status, 400, "{body}");
    assert_eq!(body["reason"], "scope_not_granted");
    assert!(!consumed(&db, "scoped"));
    let (status, body) = exchange(
        port,
        "xxxx-xxxx-xxxx",
        Some("orchestration:read settings:write"),
    )
    .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["scope"], "orchestration:read");
    assert!(consumed(&db, "scoped"));

    stop_tx.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(15), server)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
}
