//! The Effect RPC WebSocket protocol as the 0.1.10 desktop and web clients speak it.
//!
//! The client runtime (`packages/client-runtime/src/rpc/session.ts` at the pinned
//! upstream) opens `/ws?orchestrationProtocol=2&frameParts=1`, numbers its requests
//! (`"id":0`), and counts the environment as connected only once
//! `subscribeServerConfig` delivers its first `snapshot`. When this server read
//! only string ids, it dropped that request and the client timed out after 90 s.
//!
//! Everything runs in a temporary T3 home on an ephemeral port.

mod support;

use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use t3code_server::{
    auth_service::AuthService,
    config::{RuntimeConfig, ServerPaths},
    serve_bound,
    server_runtime::ServerOwnership,
    AppState,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_tungstenite::tungstenite::{client::IntoClientRequest, Message};

type Socket =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

const CREDENTIAL: &str = "websocket-rpc-test-bootstrap-credential";

/// The payload `RpcSession.connect` sends with the web client's options.
fn client_config_request(id: Value) -> Value {
    json!({
        "_tag":"Request","id":id,"tag":"subscribeServerConfig",
        "payload":{"environmentThemes":true,"usageLimitSources":true,"usageLimitsCommand":true,"workspaceSnapshotsOnDemand":true},
        "traceId":"d7141472973adbc5885c2090be332466","spanId":"3b63c3eea503e7b0","sampled":true,"headers":[]
    })
}

struct Server {
    port: u16,
    cookie: String,
    stop: Option<tokio::sync::oneshot::Sender<()>>,
    task: tokio::task::JoinHandle<anyhow::Result<()>>,
    _home: tempfile::TempDir,
}

impl Server {
    async fn start() -> Self {
        let home = tempfile::tempdir().unwrap();
        let paths = ServerPaths::from_base(home.path());
        std::fs::create_dir_all(&paths.state_dir).unwrap();
        let ownership = ServerOwnership::acquire(&paths.runtime_state).unwrap();
        let config = RuntimeConfig {
            mode: "web".into(),
            ..RuntimeConfig::default()
        };
        let auth = AuthService::with_bootstrap(CREDENTIAL);
        support::isolate_host();
        let state = AppState::with_config(&paths.state_dir, Vec::new(), auth, config)
            .await
            .unwrap();
        // Tests never run the host's provider CLIs.
        state.disable_provider_status_checks();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
        let task = tokio::spawn(serve_bound(state, listener, Some(ownership), async move {
            let _ = stopped.await;
        }));
        let cookie = browser_session(port).await;
        Self {
            port,
            cookie,
            stop: Some(stop),
            task,
            _home: home,
        }
    }

    async fn socket(&self, query: &str) -> Socket {
        let url = format!(
            "ws://127.0.0.1:{}/ws?orchestrationProtocol=2{query}",
            self.port
        );
        let mut request = url.into_client_request().unwrap();
        request
            .headers_mut()
            .insert("Cookie", self.cookie.parse().unwrap());
        let (socket, _) = tokio_tungstenite::connect_async(request).await.unwrap();
        socket
    }

    async fn stop(mut self) {
        let _ = self.stop.take().unwrap().send(());
        let _ = tokio::time::timeout(Duration::from_secs(10), &mut self.task).await;
    }
}

async fn browser_session(port: u16) -> String {
    let body = json!({ "credential": CREDENTIAL }).to_string();
    let mut stream = tokio::net::TcpStream::connect(("127.0.0.1", port))
        .await
        .unwrap();
    let raw = format!(
        "POST /api/auth/browser-session HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\
         Content-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}",
        body.len()
    );
    stream.write_all(raw.as_bytes()).await.unwrap();
    let mut response = String::new();
    tokio::time::timeout(
        Duration::from_secs(10),
        stream.read_to_string(&mut response),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(response.starts_with("HTTP/1.1 200"), "{response}");
    let header = response
        .lines()
        .find(|line| line.to_ascii_lowercase().starts_with("set-cookie:"))
        .expect("session cookie");
    header["set-cookie:".len()..]
        .trim()
        .split(';')
        .next()
        .unwrap()
        .to_owned()
}

async fn send(socket: &mut Socket, frame: Value) {
    socket.send(Message::Text(frame.to_string())).await.unwrap();
}

/// The next text frame, as raw text. Fails the test instead of hanging like the client did.
async fn recv_text(socket: &mut Socket) -> String {
    loop {
        let message = tokio::time::timeout(Duration::from_secs(10), socket.next())
            .await
            .expect("the server answered within 10 s")
            .expect("socket open")
            .unwrap();
        if let Message::Text(text) = message {
            return text;
        }
    }
}

async fn recv(socket: &mut Socket) -> Value {
    serde_json::from_str(&recv_text(socket).await).unwrap()
}

/// What `joinSocketFrameParts` does on the client: parts start with a record separator.
async fn recv_joined(socket: &mut Socket) -> (Value, usize) {
    let mut pending = String::new();
    let mut parts = 0;
    loop {
        let text = recv_text(socket).await;
        parts += 1;
        if let Some(part) = text.strip_prefix('\u{1e}') {
            assert!(part.len() < 48 * 1024, "a part fits the 48 KiB budget");
            pending.push_str(part);
            continue;
        }
        pending.push_str(&text);
        return (serde_json::from_str(&pending).unwrap(), parts);
    }
}

#[tokio::test]
async fn numbered_requests_complete_the_clients_initial_sync() {
    let server = Server::start().await;
    let mut socket = server.socket("&frameParts=1").await;

    send(&mut socket, client_config_request(json!(0))).await;
    let (first, _) = recv_joined(&mut socket).await;
    assert_eq!(first["_tag"], "Chunk");
    // Echoed as the number the client sent, not as a string.
    assert_eq!(first["requestId"], json!(0));
    let snapshot = &first["values"][0];
    assert_eq!(snapshot["type"], "snapshot");
    assert!(snapshot["config"]["environment"]["environmentId"].is_string());

    // The client acknowledges each chunk with the same numeric id.
    send(&mut socket, json!({"_tag":"Ack","requestId":0})).await;
    let sources = recv(&mut socket).await;
    assert_eq!(
        sources,
        json!({"_tag":"Chunk","requestId":0,"values":[{"version":1,"type":"usageLimitSourcesUpdated","payload":{"sources":[]}}]})
    );
    send(&mut socket, json!({"_tag":"Ack","requestId":0})).await;
    let themes = recv(&mut socket).await;
    assert_eq!(
        themes,
        json!({"_tag":"Chunk","requestId":0,"values":[{"version":1,"type":"environmentThemesUpdated","payload":{"themes":[]}}]})
    );
    send(&mut socket, json!({"_tag":"Ack","requestId":0})).await;

    // `RpcSession.probe` follows the snapshot.
    send(
        &mut socket,
        json!({"_tag":"Request","id":1,"tag":"server.probe","payload":{},"headers":[]}),
    )
    .await;
    assert_eq!(
        recv(&mut socket).await,
        json!({"_tag":"Exit","requestId":1,"exit":{"_tag":"Success","value":{}}})
    );

    // Closing the session interrupts the subscription by its numeric id.
    send(&mut socket, json!({"_tag":"Interrupt","requestId":0})).await;
    let exit = recv(&mut socket).await;
    assert_eq!(exit["_tag"], "Exit");
    assert_eq!(exit["requestId"], json!(0));
    assert_eq!(exit["exit"]["cause"][0]["_tag"], "Interrupt");
    server.stop().await;
}

#[tokio::test]
async fn string_and_numeric_ids_are_distinct_requests() {
    let server = Server::start().await;
    let mut socket = server.socket("").await;
    for id in [json!("7"), json!(7)] {
        send(
            &mut socket,
            json!({"_tag":"Request","id":id,"tag":"server.probe","payload":{},"headers":[]}),
        )
        .await;
    }
    let mut ids = vec![recv(&mut socket).await, recv(&mut socket).await]
        .into_iter()
        .map(|exit| {
            assert_eq!(exit["exit"]["_tag"], "Success", "{exit}");
            exit["requestId"].clone()
        })
        .collect::<Vec<_>>();
    ids.sort_by_key(Value::is_number);
    assert_eq!(ids, vec![json!("7"), json!(7)]);
    server.stop().await;
}

#[tokio::test]
async fn invalid_request_ids_are_reported_not_dropped() {
    let server = Server::start().await;
    let mut socket = server.socket("").await;
    for (id, shown) in [
        (None, "undefined"),
        (Some(json!(null)), "null"),
        (Some(json!(true)), "true"),
        (Some(json!({"n":1})), "[object Object]"),
    ] {
        let mut request = json!({"_tag":"Request","tag":"server.probe","payload":{},"headers":[]});
        if let Some(id) = id {
            request["id"] = id;
        }
        send(&mut socket, request).await;
        assert_eq!(
            recv(&mut socket).await,
            json!({"_tag":"Defect","defect":format!("Invalid request id: {shown}")})
        );
    }
    // The socket still serves requests afterwards.
    send(
        &mut socket,
        json!({"_tag":"Request","id":2,"tag":"server.probe","payload":{},"headers":[]}),
    )
    .await;
    assert_eq!(recv(&mut socket).await["requestId"], json!(2));
    server.stop().await;
}

#[tokio::test]
async fn numerically_acked_subscriptions_receive_updates() {
    let server = Server::start().await;
    let mut socket = server.socket("").await;
    // Theme and source events are only sent to clients that opt in.
    send(
        &mut socket,
        json!({"_tag":"Request","id":0,"tag":"subscribeServerConfig","payload":{},"headers":[]}),
    )
    .await;
    let snapshot = recv(&mut socket).await;
    assert_eq!(snapshot["values"][0]["type"], "snapshot");
    send(&mut socket, json!({"_tag":"Ack","requestId":0})).await;
    // A settings change reaches the subscriber once its window allows it.
    send(
        &mut socket,
        json!({"_tag":"Request","id":1,"tag":"server.updateSettings","payload":{"patch":{"confirmTerminalClose":false}},"headers":[]}),
    )
    .await;
    let mut seen_update = false;
    let mut seen_exit = false;
    while !(seen_update && seen_exit) {
        let frame = recv(&mut socket).await;
        match (frame["_tag"].as_str(), frame["requestId"].as_i64()) {
            (Some("Chunk"), Some(0)) => {
                assert_eq!(frame["values"][0]["type"], "settingsUpdated", "{frame}");
                seen_update = true;
            }
            (Some("Exit"), Some(1)) => {
                assert_eq!(frame["exit"]["_tag"], "Success", "{frame}");
                seen_exit = true;
            }
            _ => panic!("unexpected frame {frame}"),
        }
    }
    server.stop().await;
}
