//! What the desktop SSH launcher needs before it adopts a running server
//! instead of starting a second one: a loopback runtime record naming this
//! process and its real port, and a 2xx from `GET /`.
//!
//! Everything runs in a temporary T3 home on an ephemeral port.

use std::time::Duration;

use serde_json::Value;
use t3code_server::{
    auth_service::AuthService,
    config::{RuntimeConfig, ServerPaths},
    serve_bound,
    server_runtime::{ServerAlreadyRunningError, ServerOwnership},
    AppState,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

async fn get(port: u16, path: &str) -> String {
    let mut stream = tokio::net::TcpStream::connect(("127.0.0.1", port))
        .await
        .unwrap();
    stream
        .write_all(
            format!("GET {path} HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nConnection: close\r\n\r\n")
                .as_bytes(),
        )
        .await
        .unwrap();
    let mut response = String::new();
    tokio::time::timeout(
        Duration::from_secs(10),
        stream.read_to_string(&mut response),
    )
    .await
    .unwrap()
    .unwrap();
    response
}

#[tokio::test]
async fn publishes_bound_port_serves_root_and_clears_record_on_shutdown() {
    let home = tempfile::tempdir().unwrap();
    let paths = ServerPaths::from_base(home.path());
    let ownership = ServerOwnership::acquire(&paths.runtime_state).unwrap();
    let owner_id = ownership.owner_id().to_owned();

    // A second server on the same home is refused while we hold it.
    let refused = ServerOwnership::acquire(&paths.runtime_state).unwrap_err();
    assert!(refused.is::<ServerAlreadyRunningError>(), "{refused:#}");

    let config = RuntimeConfig {
        mode: "web".into(),
        ..RuntimeConfig::default()
    };
    let state = AppState::with_config(&paths.state_dir, Vec::new(), AuthService::new(), config)
        .await
        .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    assert_ne!(port, 0);
    let (stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();
    let server = tokio::spawn(serve_bound(state, listener, Some(ownership), async move {
        let _ = stop_rx.await;
    }));

    // The record is published by the spawned task once it starts serving.
    let raw = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if let Ok(raw) = std::fs::read_to_string(&paths.runtime_state) {
                break raw;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("runtime record was not published");
    let record: Value = serde_json::from_str(&raw).unwrap();
    assert_eq!(record["version"], 1);
    assert_eq!(record["pid"], std::process::id());
    assert_eq!(record["port"], port);
    assert_eq!(record["host"], "127.0.0.1");
    assert_eq!(record["origin"], format!("http://127.0.0.1:{port}"));
    assert_eq!(record["ownerId"], owner_id.as_str());
    assert_eq!(record["serverVersion"], "0.1.10");

    let root = get(port, "/").await;
    assert!(root.starts_with("HTTP/1.1 200"), "{root}");
    assert!(root.contains("T3 Code server"), "{root}");

    stop_tx.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(15), server)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(
        !paths.runtime_state.exists(),
        "runtime record should be cleared on shutdown"
    );
    // The lock is released with the server.
    drop(ServerOwnership::acquire(&paths.runtime_state).unwrap());
}
