//! Every registered RPC contract answers in the native server.
//!
//! A live desktop trial got `RPC contract is registered but unavailable in the
//! control plane: assets.createUrl` (and `server.updateSettings`,
//! `orchestration.getOlderSettledThreads`) back from this server. The parity
//! suites did not notice because they never sent those methods the payloads the
//! desktop sends. This sweep sends every one of the inventory's WebSocket methods
//! and fails if any answers "registered but unavailable", in either wording.
//!
//! Everything runs in a temporary T3 home on an ephemeral port, with an empty
//! `PATH` so no CLI on the host can run, and with provider status checks off.

mod support;

use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use t3code_server::{
    auth_service::AuthService,
    config::{RuntimeConfig, ServerPaths},
    serve_bound,
    server_runtime::ServerOwnership,
    AppState, RPC_CONTRACTS,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_tungstenite::tungstenite::{client::IntoClientRequest, Message};

type Socket =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

const CREDENTIAL: &str = "rpc-contract-availability-bootstrap-credential";

/// Methods that act on the host process or install software even with an empty
/// payload. They are answered (or not) by code paths other tests cover; the sweep
/// leaves them alone so it never restarts, signals or updates anything.
const NOT_SWEPT: &[&str] = &[
    "server.updateServer",
    "server.updateServerWithProgress",
    "server.commitDesktopUpdate",
    "server.signalProcess",
    "cloud.installRelayClient",
    "device.shutdown",
];

/// Methods in the pinned reference that this server does not serve yet: they answer "registered but unavailable".
/// One that starts answering must come off this list.
const NOT_YET_SERVED: &[&str] = &[
    // The project layout approval store and queries, provider mode discovery and workflow
    // subject lookup added since fd0331ca. Keep the sweep strict when a handler lands.
    "workspace.layoutCommands",
    "workspace.approveLayoutCommands",
    "server.getProviderRuntimeModeLimits",
    "workflows.about",
    "workspace.listFolders",
    "workspace.scanFolders",
    "workspace.taskFolder",
    "workspace.addTaskFolderRepository",
    "workflows.getDefinition",
    "workflows.saveDefinition",
    "workflows.deleteDefinition",
];

/// Prefixes of methods that reach the network or install software once their
/// payload decodes. They get an empty payload, which the shared checks reject
/// before routing; the native dispatch's catch-all covers them either way.
const EMPTY_PAYLOAD_ONLY: &[&str] = &[
    "provider.",
    "pullRequests.",
    "issues.",
    "issueTrackers.",
    "sourceControl.",
    "projectClone.",
    "cloud.",
    "server.prepareAcpRegistryAgent",
    "server.searchAcpRegistry",
    "server.getNpmRelease",
    "server.refreshUsageRates",
    "server.checkOtlpEndpoint",
];

/// What the desktop client sends for the methods a live trial found
/// unavailable on another host.
fn desktop_payload(method: &str) -> Option<Value> {
    Some(match method {
        "assets.createUrl" => json!({"resource":{"_tag":"workspace-file","path":"/tmp/logo.png"}}),
        "server.updateSettings" => json!({"patch":{"compactIdleThreads":true}}),
        "orchestration.getOlderSettledThreads" => {
            json!({"before":{"settledAt":"2030-01-01T00:00:00.000Z","threadId":"thread-1"},"limit":25})
        }
        _ => return None,
    })
}

/// The desktop's payload, or every required key as a placeholder string so
/// the request gets past the shared required-key check.
fn payload(method: &str) -> Value {
    if let Some(payload) = desktop_payload(method) {
        return payload;
    }
    let mut payload = serde_json::Map::new();
    if EMPTY_PAYLOAD_ONLY
        .iter()
        .any(|prefix| method.starts_with(prefix))
    {
        return Value::Object(payload);
    }
    let contract = RPC_CONTRACTS.iter().find(|c| c.tag == method).unwrap();
    for key in contract.required.unwrap_or_default() {
        payload.insert((*key).into(), json!("placeholder"));
    }
    Value::Object(payload)
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

async fn socket(port: u16, cookie: &str) -> Socket {
    let url = format!("ws://127.0.0.1:{port}/ws?orchestrationProtocol=2");
    let mut request = url.into_client_request().unwrap();
    request
        .headers_mut()
        .insert("Cookie", cookie.parse().unwrap());
    tokio_tungstenite::connect_async(request).await.unwrap().0
}

/// The frames the server sends for one request until its `Exit`, or until the
/// quiet period ends for a stream that stays open.
async fn call(port: u16, cookie: &str, method: &str, payload: Value) -> Vec<Value> {
    let mut socket = socket(port, cookie).await;
    let request = json!({
        "_tag":"Request","id":"1","tag":method,"payload":payload,
        "traceId":"d7141472973adbc5885c2090be332466","spanId":"3b63c3eea503e7b0",
        "sampled":true,"headers":[]
    });
    socket
        .send(Message::Text(request.to_string()))
        .await
        .unwrap();
    let mut frames = Vec::new();
    let mut pending = String::new();
    loop {
        let next = tokio::time::timeout(Duration::from_secs(3), socket.next()).await;
        let Ok(Some(Ok(Message::Text(text)))) = next else {
            match next {
                Ok(Some(Ok(_))) => continue,
                _ => break,
            }
        };
        if let Some(part) = text.strip_prefix('\u{1e}') {
            pending.push_str(part);
            continue;
        }
        pending.push_str(&text);
        let frame: Value = serde_json::from_str(&std::mem::take(&mut pending)).unwrap();
        let exit = frame["_tag"] == "Exit";
        frames.push(frame);
        if exit {
            break;
        }
    }
    let _ = socket.close(None).await;
    frames
}

fn unavailable(frames: &[Value]) -> Option<String> {
    frames
        .iter()
        .map(Value::to_string)
        .find(|text| text.contains("registered but unavailable"))
}

struct Server {
    port: u16,
    cookie: String,
    stop: tokio::sync::oneshot::Sender<()>,
    task: tokio::task::JoinHandle<anyhow::Result<()>>,
    _home: tempfile::TempDir,
}

async fn start() -> Server {
    let home = tempfile::tempdir().unwrap();
    // No host CLI (git, gh, a provider) can run, and nothing reads the real home.
    let empty = home.path().join("empty-path");
    std::fs::create_dir_all(&empty).unwrap();
    std::env::set_var("PATH", &empty);
    std::env::set_var("HOME", home.path());
    let paths = ServerPaths::from_base(home.path().join("t3"));
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
    Server {
        port,
        cookie,
        stop,
        task,
        _home: home,
    }
}

impl Server {
    async fn stop(self) {
        let _ = self.stop.send(());
        let _ = tokio::time::timeout(Duration::from_secs(10), self.task).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn no_contract_is_registered_but_unavailable() {
    let server = start().await;
    let methods: Vec<&str> = RPC_CONTRACTS
        .iter()
        .map(|contract| contract.tag)
        .filter(|tag| !NOT_SWEPT.contains(tag))
        .collect();
    let mut failures = Vec::new();
    for chunk in methods.chunks(24) {
        let results = futures_util::future::join_all(chunk.iter().map(|method| {
            let (port, cookie) = (server.port, server.cookie.clone());
            async move {
                let frames = call(port, &cookie, method, payload(method)).await;
                (method.to_string(), unavailable(&frames))
            }
        }))
        .await;
        failures.extend(results.into_iter().filter_map(|(method, failure)| {
            match (NOT_YET_SERVED.contains(&method.as_str()), failure) {
                (false, Some(text)) => Some(format!("{method}: {text}")),
                (true, None) => Some(format!("{method} now answers; take it off NOT_YET_SERVED")),
                _ => None,
            }
        }));
    }
    // The desktop's calls succeed, as on the reference.
    for method in [
        "assets.createUrl",
        "server.updateSettings",
        "orchestration.getOlderSettledThreads",
    ] {
        let frames = call(server.port, &server.cookie, method, payload(method)).await;
        let exit = frames.last().expect("an exit");
        assert_eq!(exit["exit"]["_tag"], "Success", "{method}: {exit}");
    }
    server.stop().await;
    assert!(
        failures.is_empty(),
        "{} contract(s) answered \"registered but unavailable\":\n{}",
        failures.len(),
        failures.join("\n")
    );
}
