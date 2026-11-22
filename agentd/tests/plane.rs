//! The message-driven control plane end to end: a client message starts a
//! run, the turn runs in `t3-agentd` through `RemoteWorkspace`, and its
//! output lands in the plane's event log, also across a server restart in
//! the middle of the turn.
mod support;

use axum::http::HeaderMap;
use serde_json::{json, Value};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use t3_agentd::{Agentd, Config};
use t3code_server::{
    control::{self, Connection, ControlPlane, Host},
    event_log::EventLog,
    persistence::MemoryStore,
    runtime::{ConnId, Runtime, Task, Wait},
    workspace::{connect, LocalWorkspace, NativeLink, RemoteWorkspace},
    Event, ServerEvent,
};

const TOKEN: &str = "plane-test-token";

/// An event log that outlives the plane, as durable storage does.
#[derive(Clone, Default)]
struct SharedLog(Arc<Mutex<Vec<ServerEvent>>>);

impl EventLog for SharedLog {
    fn load(&mut self) -> anyhow::Result<Vec<ServerEvent>> {
        Ok(self.0.lock().unwrap().clone())
    }
    fn append(&mut self, record: &ServerEvent) -> anyhow::Result<()> {
        self.0.lock().unwrap().push(record.clone());
        Ok(())
    }
}

impl SharedLog {
    fn output(&self) -> Vec<String> {
        self.0
            .lock()
            .unwrap()
            .iter()
            .filter_map(|record| match &record.event {
                Event::RunOutput { text, .. } => Some(text.clone()),
                _ => None,
            })
            .collect()
    }

    fn provider_event_values(&self) -> Vec<Value> {
        self.0
            .lock()
            .unwrap()
            .iter()
            .filter_map(|record| match &record.event {
                Event::RunProvider { event, .. } => Some(serde_json::to_value(event).unwrap()),
                _ => None,
            })
            .collect()
    }

    /// The stream positions of the recorded provider events. Only streamed
    /// text, recorded as held updates, has none.
    fn provider_seqs(&self) -> Vec<u64> {
        self.0
            .lock()
            .unwrap()
            .iter()
            .filter_map(|record| match &record.event {
                Event::RunProvider {
                    position, event, ..
                } => match position {
                    Some(position) => Some(position.seq),
                    None => {
                        let value = serde_json::to_value(event).unwrap();
                        assert_eq!(value["type"], "turn_item.delta", "untracked {value}");
                        None
                    }
                },
                _ => None,
            })
            .collect()
    }

    fn provider_events(&self) -> usize {
        self.provider_event_values().len()
    }

    fn run_status(&self) -> Option<String> {
        self.0
            .lock()
            .unwrap()
            .iter()
            .rev()
            .find_map(|record| match &record.event {
                Event::RunFinished { status, .. } => Some(status.clone()),
                _ => None,
            })
    }
}

/// Tokio tasks that stop with the server that spawned them.
#[derive(Default)]
struct StoppableRuntime(Mutex<Vec<tokio::task::JoinHandle<()>>>);

impl Runtime for StoppableRuntime {
    fn spawn(&self, task: Task) {
        self.0.lock().unwrap().push(tokio::spawn(task));
    }
    fn sleep(&self, duration: Duration) -> Wait {
        Box::pin(tokio::time::sleep(duration))
    }
    fn yield_now(&self) -> Wait {
        Box::pin(tokio::task::yield_now())
    }
    // The native link maintains the workspace itself, and this test reads
    // the log rather than delivering to sockets.
    fn set_alarm(&self, _at_ms: i64) {}
    fn push(&self, _conn: ConnId, _frames: Vec<Value>) {}
    fn connections(&self) -> Vec<ConnId> {
        Vec::new()
    }
}

impl StoppableRuntime {
    fn stop(&self) {
        for task in self.0.lock().unwrap().drain(..) {
            task.abort();
        }
    }
}

/// One server: a plane, its runtime and its link to agentd.
struct Server {
    plane: Arc<ControlPlane>,
    runtime: Arc<StoppableRuntime>,
    _link: NativeLink,
}

/// Stopping a server stops its tasks too: nothing it held in memory
/// survives, as when its process exits.
impl Drop for Server {
    fn drop(&mut self) {
        self.runtime.stop();
    }
}

impl Server {
    async fn start(
        epoch: u64,
        url: &str,
        documents: &Arc<MemoryStore>,
        log: &SharedLog,
        command: Vec<String>,
    ) -> Self {
        Self::start_at(epoch, url, documents, log, command, None).await
    }

    async fn start_at(
        epoch: u64,
        url: &str,
        documents: &Arc<MemoryStore>,
        log: &SharedLog,
        command: Vec<String>,
        address: Option<std::net::SocketAddr>,
    ) -> Self {
        let runtime = Arc::new(StoppableRuntime::default());
        let workspace = RemoteWorkspace::new(epoch, None);
        let plane = ControlPlane::open(
            control::Config {
                environment_id: "env-test".into(),
                label: "test".into(),
                bootstrap_credential: "secret".into(),
                cookie_port: 3773,
                provider_command: command,
                managed_policy: None,
                workspace_mcp_endpoint: address
                    .map(|addr| format!("http://{addr}/mcp").parse().unwrap()),
            },
            documents.clone(),
            Box::new(log.clone()),
            Host {
                runtime: runtime.clone(),
                workspace: workspace.clone(),
            },
        )
        .unwrap();
        // The plane resumed its running turns before this connects.
        let link = connect(workspace.clone(), url.into(), TOKEN.into());
        wait_until(|| workspace.is_ready()).await;
        Self {
            plane,
            runtime,
            _link: link,
        }
    }

    fn connection(&self) -> Connection {
        let (_, token) = self.plane.auth.browser_session("secret", None).unwrap();
        let mut headers = HeaderMap::new();
        headers.insert(
            "cookie",
            format!("t3_session_3773={token}").parse().unwrap(),
        );
        self.plane
            .authorize_socket(&headers, &"/ws?orchestrationProtocol=2".parse().unwrap())
            .map_err(|_| "rejected")
            .unwrap()
    }

    async fn send(&self, connection: &mut Connection, tag: &str, payload: Value) -> Value {
        let frame = json!({"_tag":"Request","id":tag,"tag":tag,"payload":payload,"headers":[]});
        let replies = self.plane.on_message(connection, &frame.to_string()).await;
        serde_json::from_str(&replies[0]).unwrap()
    }

    /// A project rooted at `root`, a thread in it, and a message that
    /// starts a run.
    async fn start_run(&self, root: &str, text: &str) {
        self.start_thread(root, "stub", text).await;
    }

    /// A project rooted at `root`, a thread on provider instance
    /// `instance`, and a message that starts a run.
    async fn start_thread(&self, root: &str, instance: &str, text: &str) {
        let mut connection = self.connection();
        let project = self
            .send(
                &mut connection,
                "projects.mutate",
                json!({"type":"project.create","commandId":"c1","projectId":"p1","title":"One","workspaceRoot":root}),
            )
            .await;
        assert_eq!(project["exit"]["_tag"], "Success", "{project}");
        for command in [
            json!({"type":"thread.create","commandId":"c2","threadId":"t1","projectId":"p1","title":"Draft",
                "modelSelection":{"instanceId":instance,"model":"fixture"},"runtimeMode":"full-access",
                "interactionMode":"default","branch":null,"worktreePath":null,"createdBy":"user","creationSource":"web"}),
            json!({"type":"message.dispatch","commandId":"c3","threadId":"t1","messageId":"message:1","text":text,
                "attachments":[],"createdBy":"user","creationSource":"web","dispatchMode":{"type":"start_immediately"}}),
        ] {
            let reply = self
                .send(&mut connection, "orchestration.dispatchCommand", command)
                .await;
            assert_eq!(reply["exit"]["_tag"], "Success", "{reply}");
        }
    }
}

async fn wait_until(mut ready: impl FnMut() -> bool) {
    for _ in 0..800 {
        if ready() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("timed out waiting");
}

/// agentd serving `root`, and its URL.
async fn agentd(dir: &std::path::Path, root: &std::path::Path) -> String {
    support::isolate_host();
    let local = Arc::new(LocalWorkspace::open(&dir.join("state")).unwrap());
    let agentd = Agentd::new(
        local,
        Config {
            token: TOKEN.into(),
            root: root.to_path_buf(),
            extra_roots: Vec::new(),
            buffer_dir: dir.join("streams"),
            buffer_limit: 1 << 20,
            poll_interval: Duration::from_millis(50),
        },
    )
    .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, agentd.router()).await });
    format!("ws://{addr}/")
}

/// A stub provider for the `stub` instance, which no CLI driver runs: prints each word of the prompt on its own line,
/// `delay` seconds apart.
fn stub_provider(delay: &str) -> Vec<String> {
    vec![
        "sh".into(),
        "-c".into(),
        format!("for word in $1; do echo \"$word\"; sleep {delay}; done"),
        "stub-provider".into(),
    ]
}

#[tokio::test]
async fn a_message_runs_a_turn_in_agentd() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("project");
    std::fs::create_dir_all(&root).unwrap();
    let url = agentd(dir.path(), &root).await;
    let documents = Arc::new(MemoryStore::new());
    write_settings(&documents, &stub_settings());
    let log = SharedLog::default();
    let server = Server::start(1, &url, &documents, &log, stub_provider("0")).await;
    server
        .start_run(root.to_str().unwrap(), "one two three")
        .await;
    wait_until(|| log.run_status().is_some()).await;
    assert_eq!(log.run_status().as_deref(), Some("completed"));
    assert_eq!(log.output(), vec!["one\n", "two\n", "three\n"]);
}

#[tokio::test]
async fn a_turn_keeps_going_across_a_server_restart() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("project");
    std::fs::create_dir_all(&root).unwrap();
    let url = agentd(dir.path(), &root).await;
    let documents = Arc::new(MemoryStore::new());
    write_settings(&documents, &stub_settings());
    let log = SharedLog::default();
    let words: Vec<String> = (1..=8).map(|i| format!("w{i}")).collect();
    let server = Server::start(1, &url, &documents, &log, stub_provider("0.2")).await;
    server
        .start_run(root.to_str().unwrap(), &words.join(" "))
        .await;
    wait_until(|| log.output().len() >= 2).await;
    drop(server);
    let before = log.output().len();
    assert!(
        before < words.len(),
        "the restart comes in the middle of the turn"
    );
    // Output keeps coming while no server is connected.
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(
        log.output().len(),
        before,
        "the stopped server records nothing"
    );
    let _server = Server::start(2, &url, &documents, &log, stub_provider("0.2")).await;
    wait_until(|| log.run_status().is_some()).await;
    assert_eq!(log.run_status().as_deref(), Some("completed"));
    let expected: Vec<String> = words.iter().map(|w| format!("{w}\n")).collect();
    assert_eq!(log.output(), expected, "nothing lost or repeated");
}

// ---- Provider turns through the fake Codex and Claude CLIs ----

/// Settings whose `codex` and `claudeAgent` instances run the fake CLI
/// playing `scenario` (from `tests/fixtures/providers`).
fn fake_cli_settings(scenario: &std::path::Path, log: &std::path::Path) -> Value {
    let fixtures =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../tests/fixtures/providers");
    let instance = |driver: &str| {
        json!({
            "driver": driver,
            "config": {"binaryPath": fixtures.join("fake_cli.py").display().to_string()},
            "environment": [
                {"name": "FAKE_PROVIDER_SCENARIO", "value": scenario.display().to_string(), "sensitive": false},
                {"name": "FAKE_PROVIDER_LOG", "value": log.display().to_string(), "sensitive": false}
            ]
        })
    };
    json!({"providerInstances": {"codex": instance("codex"), "claudeAgent": instance("claudeAgent")}})
}

/// Settings with an enabled `stub` instance, whose turns run the plane's
/// configured command (a provider instance must be enabled to take a
/// message).
fn stub_settings() -> Value {
    json!({"providerInstances": {"stub": {"driver": "stub", "enabled": true}}})
}

fn fixture(name: &str) -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../tests/fixtures/providers")
        .join(name)
}

fn write_settings(documents: &MemoryStore, settings: &Value) {
    use t3code_server::persistence::{DocumentStore, WriteMode};
    let path = std::path::Path::new(control::STATE_DIR).join("settings.json");
    documents.create_dir_all(path.parent().unwrap()).unwrap();
    documents
        .write(&path, settings.to_string().as_bytes(), WriteMode::Replace)
        .unwrap();
}

impl Server {
    /// The thread's projection as `getThreadProjection` returns it, and as
    /// a `subscribeThread` snapshot carries it.
    async fn projections(&self) -> (Value, Value) {
        let mut connection = self.connection();
        let got = self
            .send(
                &mut connection,
                "orchestration.getThreadProjection",
                json!({"threadId": "t1"}),
            )
            .await;
        assert_eq!(got["exit"]["_tag"], "Success", "{got}");
        let subscribed = self
            .send(
                &mut connection,
                "orchestration.subscribeThread",
                json!({"threadId": "t1", "requestCompletionMarker": true}),
            )
            .await;
        assert_eq!(subscribed["_tag"], "Chunk", "{subscribed}");
        (
            got["exit"]["value"].clone(),
            subscribed["values"][0]["projection"].clone(),
        )
    }
}

/// What a client renders of a finished provider turn.
fn assert_rendered_turn(projection: &Value, status: &str) {
    let runs = projection["runs"].as_array().unwrap();
    assert_eq!(runs.len(), 1, "{projection:#}");
    assert_eq!(runs[0]["status"], status, "{projection:#}");
    let assistant: Vec<&Value> = projection["messages"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|m| m["role"] == "assistant")
        .collect();
    assert!(
        assistant
            .iter()
            .any(|m| m["text"].as_str().is_some_and(|t| !t.is_empty()) && m["streaming"] == false),
        "an assistant message: {projection:#}"
    );
    let items = projection["turnItems"].as_array().unwrap();
    let kinds: Vec<&str> = items.iter().filter_map(|i| i["type"].as_str()).collect();
    assert!(kinds.contains(&"user_message"), "{kinds:?}");
    assert!(kinds.contains(&"assistant_message"), "{kinds:?}");
    if status == "completed" {
        assert!(
            kinds.contains(&"command_execution") || kinds.contains(&"dynamic_tool"),
            "a tool item: {kinds:?}"
        );
    }
    for item in items {
        assert_eq!(item["threadId"], "t1");
        assert_eq!(item["runId"], runs[0]["id"], "{item:#}");
    }
}

async fn provider_turn(driver: &str, scenario: &str) {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("project");
    std::fs::create_dir_all(&root).unwrap();
    let url = agentd(dir.path(), &root).await;
    let documents = Arc::new(MemoryStore::new());
    write_settings(
        &documents,
        &fake_cli_settings(&fixture(scenario), &dir.path().join("cli.jsonl")),
    );
    let log = SharedLog::default();
    let server = Server::start(1, &url, &documents, &log, Vec::new()).await;
    server
        .start_thread(root.to_str().unwrap(), driver, "Say hello")
        .await;
    wait_until(|| log.run_status().is_some()).await;
    let (got, subscribed) = server.projections().await;
    assert_rendered_turn(&got, "completed");
    assert_eq!(got["turnItems"], subscribed["turnItems"]);
    assert_eq!(got["runs"], subscribed["runs"]);
}

#[tokio::test]
async fn a_codex_turn_projects_what_a_client_renders() {
    provider_turn("codex", "codex_turn.json").await;
}

#[tokio::test]
async fn a_claude_turn_projects_what_a_client_renders() {
    provider_turn("claudeAgent", "claude_turn.json").await;
}

/// The provider keeps running in agentd. Leave its first HTTP call queued on
/// the host listener until the replacement plane is ready to authenticate it.
#[tokio::test]
async fn remote_providers_can_call_mcp_as_their_thread_across_a_plane_restart() {
    for (driver, scenario) in [
        ("codex", "codex_mcp.json"),
        ("claudeAgent", "claude_mcp.json"),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("project");
        std::fs::create_dir_all(&root).unwrap();
        let url = agentd(dir.path(), &root).await;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = Some(listener.local_addr().unwrap());
        let documents = Arc::new(MemoryStore::new());
        let mut settings = fake_cli_settings(&fixture(scenario), &dir.path().join("cli.jsonl"));
        settings["providerInstances"]["work"] = settings["providerInstances"][driver].clone();
        write_settings(&documents, &settings);
        let log = SharedLog::default();
        let server = Server::start_at(1, &url, &documents, &log, Vec::new(), address).await;
        server
            .start_thread(root.to_str().unwrap(), "work", "List my threads")
            .await;
        wait_until(|| log.provider_events() > 0).await;
        drop(server);

        let resumed = Server::start_at(2, &url, &documents, &log, Vec::new(), address).await;
        let router = resumed.plane.router();
        let http = tokio::spawn(async move { axum::serve(listener, router).await });
        wait_until(|| log.run_status().is_some()).await;
        assert_eq!(log.run_status().as_deref(), Some("completed"));
        let calls = std::fs::read_to_string(dir.path().join("cli.jsonl.mcp")).unwrap();
        let calls: Vec<Value> = calls
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        let result = |tool: &str| {
            let call = calls.iter().find(|c| c["request"]["tool"] == tool).unwrap();
            assert_eq!(call["status"], 200);
            assert_eq!(call["answer"]["result"]["isError"], false);
            &call["answer"]["result"]["structuredContent"]
        };
        assert_eq!(result("t3_thread_list")["projectId"], "p1");
        assert_eq!(result("t3_thread_list")["currentThreadId"], "t1");
        assert_eq!(
            result("orchestrator_capabilities")["inheritedProviderInstanceId"],
            "work"
        );
        http.abort();
    }
}

#[tokio::test]
async fn stopping_a_thread_interrupts_its_provider_turn() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("project");
    std::fs::create_dir_all(&root).unwrap();
    let url = agentd(dir.path(), &root).await;
    let documents = Arc::new(MemoryStore::new());
    let cli_log = dir.path().join("cli.jsonl");
    write_settings(
        &documents,
        &fake_cli_settings(&fixture("codex_interrupt.json"), &cli_log),
    );
    let log = SharedLog::default();
    let server = Server::start(1, &url, &documents, &log, Vec::new()).await;
    server
        .start_thread(root.to_str().unwrap(), "codex", "Say hello")
        .await;
    // The scenario waits for the interrupt once its command has started.
    wait_until(|| {
        log.provider_event_values().iter().any(|event| {
            event["item"]["type"] == "command_execution" && event["item"]["status"] == "running"
        })
    })
    .await;
    let mut connection = server.connection();
    let stopped = server
        .send(
            &mut connection,
            "orchestration.dispatchCommand",
            json!({"type":"thread.stop","commandId":"c4","threadId":"t1","createdBy":"user","creationSource":"web"}),
        )
        .await;
    assert_eq!(stopped["exit"]["_tag"], "Success", "{stopped}");
    wait_until(|| log.run_status().is_some()).await;
    let (got, _) = server.projections().await;
    assert_rendered_turn(&got, "interrupted");
    assert!(std::fs::read_to_string(&cli_log)
        .unwrap()
        .contains("turn/interrupt"));
}

/// `scenario` with a pause after each message the CLI sends, so a test can
/// restart the server in the middle of the turn.
fn slowed(scenario: &str, dir: &std::path::Path) -> std::path::PathBuf {
    let mut value: Value =
        serde_json::from_str(&std::fs::read_to_string(fixture(scenario)).unwrap()).unwrap();
    let steps = value["steps"].as_array_mut().unwrap();
    let paced: Vec<Value> = steps
        .drain(..)
        .flat_map(|step| {
            let pause = step.get("send").is_some();
            std::iter::once(step).chain(pause.then(|| json!({"sleep": 0.05})))
        })
        .collect();
    *steps = paced;
    let path = dir.join(scenario);
    std::fs::write(&path, value.to_string()).unwrap();
    path
}

#[tokio::test]
async fn a_provider_turn_keeps_going_across_a_server_restart() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("project");
    std::fs::create_dir_all(&root).unwrap();
    let url = agentd(dir.path(), &root).await;
    let documents = Arc::new(MemoryStore::new());
    let scenario = slowed("codex_turn.json", dir.path());
    write_settings(
        &documents,
        &fake_cli_settings(&scenario, &dir.path().join("cli.jsonl")),
    );
    let log = SharedLog::default();
    let server = Server::start(1, &url, &documents, &log, Vec::new()).await;
    server
        .start_thread(root.to_str().unwrap(), "codex", "Say hello")
        .await;
    wait_until(|| log.provider_events() >= 8).await;
    drop(server);
    let before = log.provider_events();
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert_eq!(
        log.provider_events(),
        before,
        "the stopped server records nothing"
    );
    let server = Server::start(2, &url, &documents, &log, Vec::new()).await;
    wait_until(|| log.run_status().is_some()).await;
    assert_eq!(log.run_status().as_deref(), Some("completed"));
    let (got, _) = server.projections().await;
    assert_rendered_turn(&got, "completed");
    // Every event of the turn is recorded once, in order: the recorded
    // stream positions only rise. The ones skipped are streamed text, which
    // is held and recorded as updates without a position.
    let seqs = log.provider_seqs();
    assert!(seqs.len() > 10, "{seqs:?}");
    assert!(seqs.windows(2).all(|pair| pair[0] < pair[1]), "{seqs:?}");
    let fixture = std::fs::read_to_string(&scenario).unwrap();
    let deltas = fixture.matches("Delta\"").count() + fixture.matches("/delta\"").count();
    let skipped = *seqs.last().unwrap() as usize - seqs.len();
    assert!(
        skipped <= deltas,
        "{skipped} skipped, {deltas} deltas: {seqs:?}"
    );
    let recorded = log.provider_event_values();
    assert_eq!(
        recorded
            .iter()
            .filter(|e| e["type"] == "turn.terminal")
            .count(),
        1
    );
}

/// The frames the plane delivers to `connection` over `rounds` alarms, as a
/// host's alarm loop sends them.
async fn deliveries(server: &Server, connection: &Connection, rounds: usize) -> Vec<String> {
    let mut sent = Vec::new();
    for _ in 0..rounds {
        server.plane.on_alarm().await;
        let signals = server.plane.take_signals();
        sent.extend(server.plane.deliver(connection, &signals).await);
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    sent
}

#[tokio::test]
async fn a_terminal_stream_reports_output_as_it_arrives() {
    support::enable_test_terminal();
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("project");
    std::fs::create_dir_all(&root).unwrap();
    let url = agentd(dir.path(), &root).await;
    let documents = Arc::new(MemoryStore::new());
    let log = SharedLog::default();
    let server = Server::start(1, &url, &documents, &log, Vec::new()).await;
    let mut connection = server.connection();
    let cwd = root.to_str().unwrap();
    let ids = json!({"threadId":"t1","terminalId":"term"});
    let opened = server
        .send(
            &mut connection,
            "terminal.open",
            json!({"threadId":"t1","terminalId":"term","cwd":cwd,"cols":80,"rows":24}),
        )
        .await;
    assert_eq!(opened["exit"]["_tag"], "Success", "{opened}");
    let frame =
        json!({"_tag":"Request","id":"attach","tag":"terminal.attach","payload":ids,"headers":[]});
    let first = server
        .plane
        .on_message(&mut connection, &frame.to_string())
        .await;
    assert!(first.iter().any(|f| f.contains("snapshot")), "{first:?}");
    // Kept with the socket between messages, as a host that suspends does.
    let mut connection: Connection =
        serde_json::from_str(&serde_json::to_string(&connection).unwrap()).unwrap();
    // A while later, the terminal is written to.
    deliveries(&server, &connection, 10).await;
    let written = server
        .send(
            &mut connection,
            "terminal.write",
            json!({"threadId":"t1","terminalId":"term","data":"echo MARK_$((3+4))\n"}),
        )
        .await;
    assert_eq!(written["exit"]["_tag"], "Success", "{written}");
    let mut sent = Vec::new();
    for _ in 0..40 {
        sent.extend(deliveries(&server, &connection, 5).await);
        if sent.iter().any(|f| f.contains("MARK_7")) {
            return;
        }
    }
    panic!("no output on the stream: {sent:?}");
}

/// Methods that act on the host process or install software even with an
/// empty payload; the sweep leaves them alone.
const NOT_SWEPT: &[&str] = &[
    "server.updateServer",
    "server.updateServerWithProgress",
    "server.commitDesktopUpdate",
    "server.signalProcess",
    "cloud.installRelayClient",
    "device.shutdown",
];

/// Contracts the control plane does not serve yet: they still answer
/// "registered but unavailable in the control plane". The list may only
/// shrink. A method that starts answering must come off it, and a method that
/// stops answering fails the sweep.
const NOT_YET_IN_THE_CONTROL_PLANE: &[&str] = &[
    "orchestration.getFullThreadDiff",
    "orchestration.getTurnDiff",
    "provider.chatgpt.import-profile",
    "provider.chatgpt.reconnect-profile",
    "projects.ensureScratch",
    "projects.createFromThread",
    "shell.openInEditor",
    "agentSessions.scan",
    "agentSessions.import",
    "assets.persistChatAttachments",
    "attachments.createUploadUrl",
    "attachments.delete",
    "mcpApps.toolInfo",
    "mcpApps.readResource",
    "mcpApps.updateModelContext",
    "provider.uploadFeedback",
    "provider.auth.start",
    "provider.consumeResetCredit",
    "provider.auth.complete",
    "provider.chatgpt.handoff.subscribe",
    "provider.auth.cancel",
    "provider.auth.removeMethod",
    "provider.auth.logout",
    "provider.auth.subscribe",
    "provider.install.start",
    "provider.install.cancel",
    "provider.install.subscribe",
    "provider.install.remove",
    "git.runStackedAction",
    "server.getNpmRelease",
    "server.updateProvider",
    "server.upsertKeybinding",
    "server.removeKeybinding",
    "server.runStorageCleanup",
    "server.getStorageCleanupReport",
    "server.getDatabaseStorage",
    "server.reclaimDatabaseSpace",
    "peerLinks.list",
    "peerLinks.link",
    "peerLinks.unlink",
    "peerLinks.answerRequest",
    "server.discoverSourceControl",
    "server.searchAcpRegistry",
    "server.prepareAcpRegistryAgent",
    "server.uninstallAcpRegistryManagedBinary",
    "server.acceptAcpRegistryUrlAuth",
    "server.listAcpRegistrySessions",
    "server.importAcpRegistrySession",
    "server.deleteAcpRegistrySession",
    "server.listAcpRegistryProviders",
    "server.setAcpRegistryProvider",
    "server.disableAcpRegistryProvider",
    "server.logoutAcpRegistry",
    "server.getTraceDiagnostics",
    "server.getProcessDiagnostics",
    "server.getHostResources",
    "server.getProcessResourceHistory",
    "server.getResourceTelemetryHistory",
    "server.retryResourceTelemetry",
    "server.listTextGeneratorModels",
    "server.getUsageSummary",
    "server.refreshUsageRates",
    "scheduledTasks.upsert",
    "scheduledTasks.setEnabled",
    "scheduledTasks.delete",
    "scheduledTasks.runNow",
    "scheduledTasks.rotateWebhookToken",
    "secrets.answerRequest",
    "guestShare.create",
    "scheduledTasks.listWebhookDeliveries",
    "scheduledTasks.getWebhookDelivery",
    "cloud.getRelayClientStatus",
    "pullRequests.list",
    "pullRequests.listStats",
    "pullRequests.filterOptions",
    "pullRequests.summary",
    "pullRequests.routing",
    "pullRequests.routingIdentity",
    "pullRequests.stack",
    "pullRequests.linkedThreads",
    "pullRequests.detail",
    "pullRequests.preview",
    "pullRequests.checks",
    "pullRequests.activity",
    "pullRequests.reviewGuide",
    "pullRequests.generateReviewGuide",
    "pullRequests.threadComments",
    "pullRequests.diffFileContents",
    "pullRequests.filesViewed",
    "pullRequests.changeBreakdown",
    "pullRequests.setFilesViewed",
    "pullRequests.runAction",
    "pullRequests.update",
    "pullRequests.comment",
    "pullRequests.updateComment",
    "pullRequests.submitReview",
    "pullRequests.replyToThread",
    "pullRequests.setThreadResolution",
    "pullRequests.setReaction",
    "pullRequests.invalidate",
    "pullRequests.reportState",
    "pullRequests.reviewerCandidates",
    "pullRequests.requestReviewers",
    "pullRequests.labelCandidates",
    "pullRequests.setLabels",
    "issues.list",
    "issues.detail",
    "issues.comments",
    "issues.templates",
    "issues.candidates",
    "issues.create",
    "issues.comment",
    "issues.update",
    "issues.setState",
    "issues.react",
    "issues.linkedThreads",
    "issueTrackers.list",
    "issueTrackers.connect",
    "issueTrackers.bindProject",
    "sourceControl.lookupRepository",
    "sourceControl.cloneRepository",
    "sourceControl.publishRepository",
    "sourceControl.signInStart",
    "sourceControl.signInWait",
    "sourceControl.signInCancel",
    "sourceControl.appAccess",
    "sourceControl.starred",
    "sourceControl.star",
    "projectClone.start",
    "projectClone.cancel",
    "projectClone.retry",
    "subscribeWorktreeSetup",
    "worktreeSetup.cancel",
    "subscribeResourceTelemetry",
    "orchestration.searchThreads",
    "orchestration.searchThread",
    "orchestration.searchThreadStream",
    "orchestration.getArchivedShellSnapshot",
    "orchestration.getThreadFamily",
    "orchestration.getThreadTitles",
    "orchestration.getThreadLinkMetadata",
    "orchestration.getWorktreeThreadIds",
    "orchestration.getWorkflowScript",
    "orchestration.getNativeChildWork",
    "orchestration.stopWorkflow",
    "orchestration.forceStopThread",
    "orchestration.getTurnItem",
    "orchestration.launchThread",
    "orchestration.subscribeCommandOutput",
];

/// The payloads the desktop client sends, for methods whose payload the
/// shared checks decode before routing (so a placeholder never reaches the
/// method), and for the methods a live desktop trial found unavailable.
fn desktop_payload(tag: &str) -> Option<Value> {
    Some(match tag {
        "assets.createUrl" => {
            json!({"resource":{"_tag":"workspace-file","path":"/workspace/logo.png"}})
        }
        "server.updateSettings" => json!({"patch":{"compactIdleThreads":true}}),
        "orchestration.getOlderSettledThreads" => {
            json!({"before":{"settledAt":"2030-01-01T00:00:00.000Z","threadId":"thread-1"},"limit":25})
        }
        "projects.mutate" => json!({"type":"project.update","commandId":"c","projectId":"missing"}),
        "device.action" => json!({"deviceId":"d","action":{"type":"shake"},"type":"shake"}),
        "orchestration.dispatchCommand" => {
            json!({"type":"thread.visit","commandId":"c","threadId":"missing","visitedAt":"2030-01-01T00:00:00.000Z"})
        }
        "orchestration.getTurnDiff" | "orchestration.getFullThreadDiff" => {
            json!({"threadId":"missing","fromTurnCount":0,"toTurnCount":1})
        }
        "mcpApps.updateModelContext" => {
            json!({"threadId":"t","itemId":"i","conversationThreadId":"c","content":[]})
        }
        "provider.auth.start" => json!({"instanceId":"codex","callbackMode":"server"}),
        _ => return None,
    })
}

/// Every required key, as a placeholder string.
fn placeholder_payload(tag: &str) -> Value {
    let contract = t3code_server::RPC_CONTRACTS
        .iter()
        .find(|c| c.tag == tag)
        .unwrap();
    let mut payload = serde_json::Map::new();
    for key in contract.required.unwrap_or_default() {
        payload.insert((*key).into(), json!("placeholder"));
    }
    Value::Object(payload)
}

/// One plane on a connected agentd, for sending single requests.
async fn connected_plane() -> (tempfile::TempDir, Server) {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("project");
    std::fs::create_dir_all(&root).unwrap();
    let url = agentd(dir.path(), &root).await;
    let documents = Arc::new(MemoryStore::new());
    write_settings(&documents, &stub_settings());
    let log = SharedLog::default();
    let server = Server::start(1, &url, &documents, &log, stub_provider("0")).await;
    (dir, server)
}

async fn request(server: &Server, tag: &str, payload: Value) -> Vec<String> {
    let mut connection = server.connection();
    let frame = json!({"_tag":"Request","id":"1","tag":tag,"payload":payload,"headers":[]});
    let replies = tokio::time::timeout(
        Duration::from_secs(10),
        server.plane.on_message(&mut connection, &frame.to_string()),
    )
    .await
    .unwrap_or_default();
    server.plane.disconnect(&connection);
    replies
}

/// The sweep that guards every contract: a live desktop got "registered but
/// unavailable in the control plane" for `assets.createUrl`,
/// `server.updateSettings` and `orchestration.getOlderSettledThreads`, and no
/// test sent them to the plane.
#[tokio::test]
async fn no_contract_newly_answers_registered_but_unavailable_in_the_control_plane() {
    let (_dir, server) = connected_plane().await;
    let mut unavailable = Vec::new();
    for contract in t3code_server::RPC_CONTRACTS {
        if NOT_SWEPT.contains(&contract.tag) {
            continue;
        }
        let payload =
            desktop_payload(contract.tag).unwrap_or_else(|| placeholder_payload(contract.tag));
        let replies = request(&server, contract.tag, payload).await;
        if replies
            .iter()
            .any(|text| text.contains("registered but unavailable"))
        {
            unavailable.push(contract.tag);
        }
    }
    let new: Vec<_> = unavailable
        .iter()
        .filter(|tag| !NOT_YET_IN_THE_CONTROL_PLANE.contains(tag))
        .collect();
    assert!(
        new.is_empty(),
        "these contracts answer \"registered but unavailable in the control plane\": {new:?}"
    );
    let served: Vec<_> = NOT_YET_IN_THE_CONTROL_PLANE
        .iter()
        .filter(|tag| !unavailable.contains(tag))
        .collect();
    assert!(
        served.is_empty(),
        "these contracts now answer in the control plane; take them off NOT_YET_IN_THE_CONTROL_PLANE: {served:?}"
    );
}

fn exit(replies: &[String]) -> Value {
    let reply: Value = serde_json::from_str(replies.last().expect("a reply")).unwrap();
    assert_eq!(reply["_tag"], "Exit", "{reply}");
    reply["exit"].clone()
}

#[tokio::test]
async fn the_control_plane_answers_the_desktops_asset_settings_and_history_calls() {
    let (_dir, server) = connected_plane().await;

    let asset = exit(
        &request(
            &server,
            "assets.createUrl",
            desktop_payload("assets.createUrl").unwrap(),
        )
        .await,
    );
    assert_eq!(asset["_tag"], "Success", "{asset}");
    let url = asset["value"]["relativeUrl"].as_str().unwrap();
    assert!(
        url.starts_with("/api/assets/") && url.ends_with("/logo.png"),
        "{url}"
    );
    assert_eq!(asset["value"]["sourcePath"], "/workspace/logo.png");
    // `<base64url claims>.<base64url HMAC-SHA256>`, as the native server signs.
    let token = url.split('/').nth(3).unwrap();
    let (claims, signature) = token.split_once('.').unwrap();
    assert_eq!(signature.len(), 43, "{token}");
    let claims = String::from_utf8(base64_url_decode(claims)).unwrap();
    let claims: Value = serde_json::from_str(&claims).unwrap();
    assert_eq!(claims["kind"], "workspace-file");
    assert_eq!(claims["filePath"], "/workspace/logo.png");
    let bad = exit(
        &request(
            &server,
            "assets.createUrl",
            json!({"resource":{"_tag":"workspace-file"}}),
        )
        .await,
    );
    assert_eq!(bad["_tag"], "Failure");
    assert!(
        bad["cause"][0]["defect"]
            .as_str()
            .unwrap()
            .starts_with("Expected {"),
        "{bad}"
    );

    let before = exit(&request(&server, "server.getSettings", json!({})).await);
    assert_eq!(before["_tag"], "Success", "{before}");
    let updated = exit(
        &request(
            &server,
            "server.updateSettings",
            json!({"patch":{"defaultRuntimeMode":"approval-required"}}),
        )
        .await,
    );
    assert_eq!(updated["_tag"], "Success", "{updated}");
    assert_eq!(updated["value"]["defaultRuntimeMode"], "approval-required");
    let after = exit(&request(&server, "server.getSettings", json!({})).await);
    assert_eq!(after["value"]["defaultRuntimeMode"], "approval-required");
    let invalid = exit(
        &request(
            &server,
            "server.updateSettings",
            json!({"patch":{"backgroundActivityProfile":"not-a-profile"}}),
        )
        .await,
    );
    assert_eq!(invalid["_tag"], "Failure", "{invalid}");

    let older = exit(
        &request(
            &server,
            "orchestration.getOlderSettledThreads",
            desktop_payload("orchestration.getOlderSettledThreads").unwrap(),
        )
        .await,
    );
    assert_eq!(older["_tag"], "Success", "{older}");
    assert_eq!(older["value"]["threads"], json!([]));
    assert_eq!(older["value"]["counts"], json!([]));
    assert!(older["value"]["snapshotSequence"].is_u64(), "{older}");
}

fn base64_url_decode(text: &str) -> Vec<u8> {
    const ALPHABET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    let mut bits = 0u32;
    let mut count = 0;
    let mut out = Vec::new();
    for byte in text.bytes() {
        bits = bits << 6 | ALPHABET.iter().position(|&c| c == byte).unwrap() as u32;
        count += 6;
        if count >= 8 {
            count -= 8;
            out.push((bits >> count) as u8);
        }
    }
    out
}
