//! The message-driven control plane end to end: a client message starts a
//! run, the turn runs in `t3-agentd` through `RemoteWorkspace`, and its
//! output lands in the plane's event log, also across a server restart in
//! the middle of the turn.
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
        let runtime = Arc::new(StoppableRuntime::default());
        let workspace = RemoteWorkspace::new(epoch, None);
        let plane = ControlPlane::open(
            control::Config {
                environment_id: "env-test".into(),
                label: "test".into(),
                bootstrap_credential: "secret".into(),
                cookie_port: 3773,
                provider_command: command,
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
                "modelSelection":{"instanceId":"stub","model":"fixture"},"runtimeMode":"full-access",
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
