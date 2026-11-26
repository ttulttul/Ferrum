//! Opening a thread as the 0.1.10 client does: its subscription, its worktree setup, its
//! project's repository identity and the change request browser's first read.
//!
//! The field sets below are the reference's (`OrchestrationV2Run`,
//! `OrchestrationV2ConversationMessage`, the `user_message` and `error` turn items and
//! `OrchestrationV2ProjectedTurnItem` at the pinned upstream). The client decodes every
//! reply with those schemas; one missing key fails the whole snapshot, which is how a
//! thread failed to open against this server.
//!
//! Everything runs in a temporary T3 home on an ephemeral port.

mod support;

use std::{collections::BTreeSet, path::Path, time::Duration};

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

const CREDENTIAL: &str = "thread-subscription-test-bootstrap-credential";
const THREAD: &str = "thread:client";
const PROJECT: &str = "project:bitbucket";
const REMOTE: &str = "git@ssh.bitbucket.org:example-org/example-repo.git";

struct Server {
    port: u16,
    cookie: String,
    stop: Option<tokio::sync::oneshot::Sender<()>>,
    task: tokio::task::JoinHandle<anyhow::Result<()>>,
    _home: tempfile::TempDir,
}

impl Server {
    /// A server whose provider command fails, as a turn fails without a provider CLI.
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
        let state = AppState::with_config(&paths.state_dir, vec!["false".into()], auth, config)
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
        let (_, body) = http(
            port,
            "POST",
            "/api/auth/browser-session",
            &[],
            Some(json!({ "credential": CREDENTIAL })),
        )
        .await;
        let cookie = body
            .lines()
            .find(|line| line.to_ascii_lowercase().starts_with("set-cookie:"))
            .expect("session cookie")["set-cookie:".len()..]
            .trim()
            .split(';')
            .next()
            .unwrap()
            .to_owned();
        Self {
            port,
            cookie,
            stop: Some(stop),
            task,
            _home: home,
        }
    }

    async fn socket(&self) -> Socket {
        let url = format!("ws://127.0.0.1:{}/ws?orchestrationProtocol=2", self.port);
        let mut request = url.into_client_request().unwrap();
        request
            .headers_mut()
            .insert("Cookie", self.cookie.parse().unwrap());
        tokio_tungstenite::connect_async(request).await.unwrap().0
    }

    /// The JSON body of an authenticated HTTP request.
    async fn http(&self, method: &str, path: &str, body: Option<Value>) -> Value {
        let cookie = format!("Cookie: {}", self.cookie);
        let (status, raw) = http(
            self.port,
            method,
            path,
            &[&cookie, "x-t3-orchestration-protocol: 2"],
            body,
        )
        .await;
        assert_eq!(status, 200, "{method} {path}: {raw}");
        let body = raw.split_once("\r\n\r\n").unwrap().1;
        serde_json::from_str(body).unwrap_or_else(|_| panic!("JSON from {path}: {body}"))
    }

    async fn stop(mut self) {
        let _ = self.stop.take().unwrap().send(());
        let _ = tokio::time::timeout(Duration::from_secs(10), &mut self.task).await;
    }
}

/// One HTTP/1.1 exchange: the status and the whole response, headers included.
async fn http(
    port: u16,
    method: &str,
    path: &str,
    headers: &[&str],
    body: Option<Value>,
) -> (u16, String) {
    let body = body.map(|b| b.to_string()).unwrap_or_default();
    let mut stream = tokio::net::TcpStream::connect(("127.0.0.1", port))
        .await
        .unwrap();
    let mut raw = format!("{method} {path} HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n");
    for header in headers {
        raw.push_str(header);
        raw.push_str("\r\n");
    }
    if !body.is_empty() {
        raw.push_str("Content-Type: application/json\r\n");
    }
    raw.push_str(&format!("Content-Length: {}\r\n\r\n{body}", body.len()));
    stream.write_all(raw.as_bytes()).await.unwrap();
    let mut response = String::new();
    tokio::time::timeout(
        Duration::from_secs(10),
        stream.read_to_string(&mut response),
    )
    .await
    .unwrap()
    .unwrap();
    // Chunked bodies here are single chunks; drop the framing.
    if response
        .to_ascii_lowercase()
        .contains("transfer-encoding: chunked")
    {
        let (head, chunked) = response.split_once("\r\n\r\n").unwrap();
        let body = chunked
            .split_once("\r\n")
            .map(|(_, rest)| rest.rsplit_once("\r\n0\r\n").map_or(rest, |(b, _)| b))
            .unwrap_or_default();
        response = format!("{head}\r\n\r\n{body}");
    }
    let status = response[9..12].parse().unwrap();
    (status, response)
}

async fn send(socket: &mut Socket, frame: Value) {
    socket.send(Message::Text(frame.to_string())).await.unwrap();
}

/// The next frame for request `id`: a `Chunk` (acknowledged, as the client does) or `Exit`.
async fn next_for(socket: &mut Socket, id: u64) -> Value {
    loop {
        let message = tokio::time::timeout(Duration::from_secs(10), socket.next())
            .await
            .expect("the server answered within 10 s")
            .expect("socket open")
            .unwrap();
        let Message::Text(text) = message else {
            continue;
        };
        let frame: Value = serde_json::from_str(&text).unwrap();
        if frame["requestId"] != id {
            continue;
        }
        if frame["_tag"] == "Chunk" {
            send(socket, json!({"_tag":"Ack","requestId":id})).await;
        }
        return frame;
    }
}

async fn request(socket: &mut Socket, id: u64, tag: &str, payload: Value) -> Value {
    send(
        socket,
        json!({"_tag":"Request","id":id,"tag":tag,"payload":payload,"headers":[]}),
    )
    .await;
    next_for(socket, id).await
}

async fn dispatch(socket: &mut Socket, id: u64, command: Value) {
    let reply = request(socket, id, "orchestration.dispatchCommand", command).await;
    assert_eq!(reply["exit"]["_tag"], "Success", "{reply}");
}

fn keys(value: &Value) -> BTreeSet<&str> {
    value
        .as_object()
        .unwrap_or_else(|| panic!("an object: {value}"))
        .keys()
        .map(String::as_str)
        .collect()
}

fn set<'a>(names: &[&'a str]) -> BTreeSet<&'a str> {
    names.iter().copied().collect()
}

const TURN_ITEM_BASE: &[&str] = &[
    "id",
    "threadId",
    "runId",
    "nodeId",
    "providerThreadId",
    "providerTurnId",
    "nativeItemRef",
    "parentItemId",
    "ordinal",
    "status",
    "title",
    "startedAt",
    "completedAt",
    "updatedAt",
    "type",
];

fn git(dir: &Path, args: &[&str]) {
    let output = std::process::Command::new("git")
        .arg("-C")
        .arg(dir)
        .args(args)
        .output()
        .unwrap();
    assert!(output.status.success(), "git {args:?}: {output:?}");
}

/// A project on a checkout with a Bitbucket remote, and a thread whose first turn failed.
async fn seeded(server: &Server, socket: &mut Socket) -> tempfile::TempDir {
    let checkout = tempfile::tempdir().unwrap();
    git(checkout.path(), &["init", "-q"]);
    git(checkout.path(), &["remote", "add", "origin", REMOTE]);
    git(
        checkout.path(),
        &[
            "-c",
            "user.email=test@example.test",
            "-c",
            "user.name=Test",
            "commit",
            "--allow-empty",
            "-qm",
            "Fixture",
        ],
    );
    server
        .http(
            "POST",
            "/api/projects/mutate",
            Some(
                json!({"type":"project.create","commandId":"create-project","projectId":PROJECT,
                "title":"Bitbucket checkout","workspaceRoot":checkout.path()}),
            ),
        )
        .await;
    dispatch(
        socket,
        1,
        json!({"type":"thread.create","commandId":"create-thread","threadId":THREAD,"projectId":PROJECT,
            "title":"Thread","modelSelection":{"instanceId":"codex","model":"fixture"},
            "runtimeMode":"full-access","interactionMode":"default","branch":null,"worktreePath":null,
            "createdBy":"user","creationSource":"web"}),
    )
    .await;
    dispatch(
        socket,
        2,
        json!({"type":"message.dispatch","commandId":"send-1","threadId":THREAD,"messageId":"message:first",
            "text":"Hello","attachments":[],"createdBy":"user","creationSource":"web",
            "dispatchMode":{"type":"start_immediately"}}),
    )
    .await;
    for attempt in 3.. {
        let reply = request(
            socket,
            attempt,
            "orchestration.getThreadProjection",
            json!({"threadId":THREAD}),
        )
        .await;
        if reply["exit"]["value"]["runs"][0]["status"] == "failed" {
            break;
        }
        assert!(attempt < 100, "the turn ends: {reply}");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    checkout
}

#[tokio::test]
async fn a_thread_with_a_turn_decodes_with_the_reference_schemas() {
    let server = Server::start().await;
    let mut socket = server.socket().await;
    let _checkout = seeded(&server, &mut socket).await;

    // The options the 0.1.10 client subscribes with.
    let first = request(
        &mut socket,
        200,
        "orchestration.subscribeThread",
        json!({"threadId":THREAD,"requestCompletionMarker":true,"acceptBoundedSnapshot":true,
            "acceptDeferredWork":true,"acceptCompactTurnItems":true,"acceptCompactIds":true,"acceptDedupedBodies":true}),
    )
    .await;
    assert_eq!(first["_tag"], "Chunk", "{first}");
    let values = first["values"].as_array().unwrap();
    assert_eq!(values.len(), 2);
    assert_eq!(values[1], json!({"kind":"synchronized"}));
    let snapshot = &values[0];
    assert_eq!(
        keys(snapshot),
        set(&[
            "kind",
            "snapshotSequence",
            "historyCursor",
            "hasMoreHistory",
            "latestLocalTurnOrdinal",
            "turnItemsOmitLocalVisible",
            "projection"
        ])
    );
    assert_eq!(snapshot["historyCursor"], Value::Null);
    assert_eq!(snapshot["hasMoreHistory"], false);
    assert_eq!(snapshot["latestLocalTurnOrdinal"], 1_000_002);
    let projection = &snapshot["projection"];
    // Local visible rows carry the turn items; the client restores them.
    assert_eq!(projection["turnItems"], json!([]));

    let run = &projection["runs"][0];
    assert_eq!(
        keys(run),
        set(&[
            "id",
            "threadId",
            "ordinal",
            "providerInstanceId",
            "modelSelection",
            "providerThreadId",
            "userMessageId",
            "rootNodeId",
            "activeAttemptId",
            "status",
            "queuePosition",
            "requestedAt",
            "startedAt",
            "completedAt",
            "checkpointId",
            "contextHandoffId"
        ])
    );
    assert_eq!(run["threadId"], THREAD);
    assert_eq!(run["ordinal"], 1);
    assert_eq!(run["status"], "failed");
    assert_eq!(run["userMessageId"], "message:first");
    assert_eq!(
        run["modelSelection"],
        json!({"instanceId":"codex","model":"fixture"})
    );

    let message = &projection["messages"][0];
    assert_eq!(
        keys(message),
        set(&[
            "createdBy",
            "creationSource",
            "id",
            "threadId",
            "runId",
            "nodeId",
            "role",
            "text",
            "attachments",
            "streaming",
            "createdAt",
            "updatedAt"
        ])
    );
    assert_eq!(message["role"], "user");
    assert_eq!(message["text"], "Hello");
    assert_eq!(message["runId"], run["id"]);

    let rows = projection["visibleTurnItems"].as_array().unwrap();
    assert_eq!(rows.len(), 2);
    for (position, row) in rows.iter().enumerate() {
        assert_eq!(
            keys(row),
            set(&[
                "position",
                "visibility",
                "sourceThreadId",
                "sourceItemId",
                "item"
            ])
        );
        assert_eq!(row["position"], position);
        assert_eq!(row["visibility"], "local");
        assert_eq!(row["sourceItemId"], row["item"]["id"]);
    }
    let user = &rows[0]["item"];
    let mut user_keys = set(TURN_ITEM_BASE);
    user_keys.extend([
        "createdBy",
        "creationSource",
        "messageId",
        "inputIntent",
        "text",
        "textOmitted",
        "attachments",
    ]);
    assert_eq!(keys(user), user_keys);
    assert_eq!(user["id"], "turn-item:message:message%3Afirst");
    assert_eq!(user["ordinal"], 1_000_001);
    // The text is the message's, so it is sent once.
    assert_eq!(
        (&user["text"], &user["textOmitted"]),
        (&json!(""), &json!(true))
    );
    let failure = &rows[1]["item"];
    let mut failure_keys = set(TURN_ITEM_BASE);
    failure_keys.insert("failure");
    assert_eq!(keys(failure), failure_keys);
    assert_eq!(failure["type"], "error");
    assert_eq!(failure["status"], "failed");
    assert_eq!(failure["ordinal"], 1_000_002);
    assert_eq!(
        keys(&failure["failure"]),
        set(&["class", "message", "code", "retryable"])
    );
    assert_eq!(failure["failure"]["class"], "provider_error");

    // Without the options: the whole projection, no bounded fields, full text.
    let plain = request(
        &mut socket,
        201,
        "orchestration.subscribeThread",
        json!({"threadId":THREAD}),
    )
    .await;
    let values = plain["values"].as_array().unwrap();
    assert_eq!(values.len(), 1, "no marker unless asked");
    assert_eq!(
        keys(&values[0]),
        set(&["kind", "snapshotSequence", "projection"])
    );
    let items = values[0]["projection"]["turnItems"].as_array().unwrap();
    assert_eq!(items.len(), 2);
    assert_eq!(items[0]["text"], "Hello");
    assert!(items[0].get("textOmitted").is_none());

    // A later change arrives on the open subscription as the reference's
    // domain events.
    dispatch(
        &mut socket,
        202,
        json!({"type":"message.dispatch","commandId":"send-2","threadId":THREAD,"messageId":"message:second",
            "text":"Again","attachments":[],"createdBy":"user","creationSource":"web",
            "dispatchMode":{"type":"start_immediately"}}),
    )
    .await;
    let (mut message, mut run) = (false, false);
    while !(message && run) {
        let update = next_for(&mut socket, 200).await;
        assert_eq!(update["_tag"], "Chunk", "{update}");
        for item in update["values"].as_array().unwrap() {
            assert_eq!(item["kind"], "event", "{item}");
            let event = &item["event"];
            assert!(item["sequence"].as_u64().is_some(), "{item}");
            match event["type"].as_str() {
                Some("message.updated") if event["payload"]["text"] == "Again" => message = true,
                Some("run.created") if event["payload"]["ordinal"] == 2 => run = true,
                _ => {}
            }
        }
    }

    // The HTTP reads return the same shapes.
    let transcript = server
        .http(
            "GET",
            &format!("/api/orchestration/threads/{THREAD}/transcript"),
            None,
        )
        .await;
    assert_eq!(
        keys(&transcript),
        set(&["threadId", "title", "updatedAt", "items"])
    );
    assert_eq!(transcript["items"][0]["item"]["type"], "user_message");
    let bounded = server
        .http(
            "GET",
            &format!("/api/orchestration/threads/{THREAD}/bounded"),
            None,
        )
        .await;
    assert!(bounded["latestLocalTurnOrdinal"].as_u64() > Some(2_000_000));
    server.stop().await;
}

#[tokio::test]
async fn a_missing_thread_fails_its_subscription_as_the_reference_does() {
    let server = Server::start().await;
    let mut socket = server.socket().await;
    let reply = request(
        &mut socket,
        1,
        "orchestration.subscribeThread",
        json!({"threadId":"thread:missing","requestCompletionMarker":true}),
    )
    .await;
    // An empty chunk instead of this was what the client could not decode ("Missing key at [0]").
    assert_eq!(reply["_tag"], "Exit", "{reply}");
    let error = &reply["exit"]["cause"][0]["error"];
    assert_eq!(error["_tag"], "OrchestrationV2GetThreadProjectionError");
    assert_eq!(error["threadId"], "thread:missing");
    server.stop().await;
}

#[tokio::test]
async fn worktree_setup_starts_empty_and_has_nothing_to_cancel() {
    let server = Server::start().await;
    let mut socket = server.socket().await;
    let first = request(
        &mut socket,
        1,
        "subscribeWorktreeSetup",
        json!({"threadId":THREAD}),
    )
    .await;
    assert_eq!(first, json!({"_tag":"Chunk","requestId":1,"values":[null]}));
    let cancel = request(
        &mut socket,
        2,
        "worktreeSetup.cancel",
        json!({"threadId":THREAD}),
    )
    .await;
    assert_eq!(cancel["exit"]["value"], json!({"cancelled":false}));
    server.stop().await;
}

#[tokio::test]
async fn projects_carry_their_repository_identity() {
    // The canonical SSH alias routes through the public host's REST adapter.
    // Give its anonymous viewer probe the same 401 as the parity fixture.
    let api = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    std::env::set_var(
        "T3CODE_BITBUCKET_API_BASE_URL",
        format!("http://{}/2.0", api.local_addr().unwrap()),
    );
    let api_task = tokio::spawn(async move {
        let router =
            axum::Router::new().fallback(|| async { axum::http::StatusCode::UNAUTHORIZED });
        axum::serve(api, router).await.unwrap();
    });
    let server = Server::start().await;
    let mut socket = server.socket().await;
    let checkout = seeded(&server, &mut socket).await;
    let root = std::fs::canonicalize(checkout.path()).unwrap();
    let expected = json!({
        "canonicalKey":"bitbucket.org/example-org/example-repo",
        "locator":{"source":"git-remote","remoteName":"origin","remoteUrl":REMOTE},
        "rootPath":root,
        "displayName":"example-org/example-repo",
        "provider":"bitbucket",
        "owner":"example-org",
        "name":"example-repo"
    });
    let shell = server.http("GET", "/api/orchestration/shell", None).await;
    assert_eq!(shell["projects"][0]["repositoryIdentity"], expected);
    let subscribed = request(
        &mut socket,
        300,
        "orchestration.subscribeShell",
        json!({"requestCompletionMarker":true}),
    )
    .await;
    assert_eq!(
        subscribed["values"][0]["snapshot"]["projects"][0]["repositoryIdentity"],
        expected
    );

    // Change requests on the host list as the reference lists them with no Bitbucket credential.
    let listed = request(
        &mut socket,
        301,
        "pullRequests.list",
        json!({"state":"open","projectId":PROJECT}),
    )
    .await;
    let error = &listed["exit"]["cause"][0]["error"];
    assert_eq!(error["_tag"], "PullRequestUnavailableError", "{listed}");
    assert_eq!(error["reason"], "cli-unauthenticated");
    assert_eq!(error["provider"], "bitbucket");
    assert_eq!(error["listResult"]["providers"][0]["host"], "bitbucket.org");
    assert_eq!(
        error["listResult"]["errors"][0]["message"],
        "example-org/example-repo could not be read."
    );
    server.stop().await;
    api_task.abort();
}
