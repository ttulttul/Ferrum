//! The Codex app-server a provider instance shares across its threads, as
//! the reference's provider session for an instance keeps one
//! (`provider-session:provider-instance:<id>:shared`).
//!
//! The process starts with the instance's first turn and is initialized
//! once. Each turn talks to it through a [`Conn`]: replies come back by
//! request id, and notifications and requests by the native thread the
//! turn started or resumed. A thread stays loaded after its turn, so the
//! next turn with the same model and runtime policy skips `thread/resume`;
//! it is unloaded (`thread/unsubscribe`) once idle for
//! [`THREAD_UNLOAD_DELAY`], and the process stops once no turn has run for
//! [`IDLE_RELEASE_DELAY`]. A settings change takes effect when the next
//! process starts, as in the reference.
use super::super::provider_event_log::{Direction, Protocol, ProtocolLog, ProviderEventLog};
use crate::workspace::provider::ProviderTurn;
use crate::workspace::TurnSpec;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::process::Stdio;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::sync::{mpsc, watch};

/// How long an idle thread stays loaded (the reference's idle timeout).
pub(super) const THREAD_UNLOAD_DELAY: Duration = Duration::from_secs(30 * 60);
/// How long the process outlives its last turn.
pub(super) const IDLE_RELEASE_DELAY: Duration = Duration::from_secs(30 * 60);
/// How long an unload waits for the thread's turn to end.
const UNLOAD_WAIT: Duration = Duration::from_secs(15);

/// A host's shared app-servers, by provider instance: one per workspace,
/// as the reference's provider sessions belong to its server.
#[derive(Default)]
pub(crate) struct Registry(Mutex<HashMap<String, Arc<AppServer>>>);

impl Registry {
    fn with<T>(&self, f: impl FnOnce(&mut HashMap<String, Arc<AppServer>>) -> T) -> T {
        f(&mut self.0.lock().unwrap_or_else(|e| e.into_inner()))
    }
}

#[derive(Default)]
struct Routes {
    /// Each open turn's channel, and the native thread it is bound to.
    sessions: HashMap<u64, (mpsc::UnboundedSender<String>, Option<String>)>,
    /// Requests waiting for their reply: the turn that sent them, and the
    /// method.
    replies: HashMap<u64, (u64, String)>,
}

/// `makeCodexProtocolThreadRouter`: the T3 thread each frame belongs to, for
/// the provider event log. A request names it in its config or by its native
/// thread, a reply follows its request, and a frame tied to no thread (such
/// as `initialize`) goes to the global log.
#[derive(Default)]
struct ThreadRouter {
    by_native: HashMap<String, String>,
    by_request: HashMap<String, String>,
}

impl ThreadRouter {
    fn route(&mut self, direction: Direction, frame: &Value) -> Option<String> {
        let id = &frame["id"];
        let request = frame["method"].is_string();
        let key = (id.is_string() || id.is_number()).then(|| {
            let side = if request == (direction == Direction::Outgoing) {
                "client"
            } else {
                "server"
            };
            let id = id.as_str().map_or_else(|| id.to_string(), str::to_owned);
            format!("{side}:{id}")
        });
        if !request {
            let thread = self.by_request.remove(&key?);
            if let (Some(thread), Some(native)) =
                (&thread, frame["result"]["thread"]["id"].as_str())
            {
                self.by_native.insert(native.to_owned(), thread.clone());
            }
            return thread;
        }
        let params = &frame["params"];
        let configured = params["config"]["shell_environment_policy.set.T3CODE_THREAD_ID"].as_str();
        let native = params["threadId"]
            .as_str()
            .or(params["thread"]["id"].as_str());
        let mut thread = match configured {
            Some(configured) => Some(configured.to_owned()),
            None => native.and_then(|native| self.by_native.get(native).cloned()),
        };
        if let (None, Some(native)) = (&thread, native) {
            // A subagent's thread belongs to its parent's.
            thread = params["thread"]["source"]["subAgent"]["thread_spawn"]["parent_thread_id"]
                .as_str()
                .and_then(|parent| self.by_native.get(parent).cloned());
            if let Some(thread) = &thread {
                self.by_native.insert(native.to_owned(), thread.clone());
            }
        }
        if let (Some(thread), Some(key)) = (&thread, key) {
            self.by_request.insert(key, thread.clone());
        }
        thread
    }
}

pub(crate) struct AppServer {
    key: String,
    registry: std::sync::Weak<Registry>,
    stdin: tokio::sync::Mutex<Option<tokio::process::ChildStdin>>,
    next_id: AtomicU64,
    next_session: AtomicU64,
    routes: Mutex<Routes>,
    /// `initialize` was answered.
    pub(super) initialized: tokio::sync::Mutex<bool>,
    /// Each loaded native thread's load key (thread, model, runtime policy).
    loaded: Mutex<HashMap<String, String>>,
    timers: Mutex<HashMap<String, tokio::task::JoinHandle<()>>>,
    idle: Mutex<Option<tokio::task::JoinHandle<()>>>,
    exited: watch::Receiver<Option<Option<std::process::ExitStatus>>>,
    pid: Option<u32>,
    /// Where the frames to and from the process are recorded, and the
    /// thread each belongs to.
    log: Option<(ProtocolLog, Mutex<ThreadRouter>)>,
}

/// One turn's line to the shared app-server.
pub(super) struct Conn {
    pub(super) server: Arc<AppServer>,
    session: u64,
    lines: mpsc::UnboundedReceiver<String>,
}

/// The shared app-server of the turn's instance, started if it has none.
pub(super) fn acquire(
    registry: &Arc<Registry>,
    spec: &TurnSpec,
    provider: &ProviderTurn,
    log: &ProviderEventLog,
) -> std::io::Result<Conn> {
    // The reference shares by instance. A turn naming none (a test's)
    // shares only with turns launched the same way.
    let key = if provider.instance_id.is_empty() {
        json!({"binary": provider.binary_path, "args": provider.launch_args,
            "home": provider.home_path, "env": spec.env})
        .to_string()
    } else {
        provider.instance_id.clone()
    };
    let server = registry.with(|all| -> std::io::Result<Arc<AppServer>> {
        if let Some(server) = all.get(&key).filter(|s| s.exited.borrow().is_none()) {
            return Ok(server.clone());
        }
        let server = AppServer::spawn(registry, &key, spec, provider, log)?;
        all.insert(key.clone(), server.clone());
        Ok(server)
    })?;
    if let Some(idle) = server.idle.lock().unwrap_or_else(|e| e.into_inner()).take() {
        idle.abort();
    }
    let session = server.next_session.fetch_add(1, Ordering::SeqCst);
    let (tx, lines) = mpsc::unbounded_channel();
    server
        .routes
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .sessions
        .insert(session, (tx, None));
    Ok(Conn {
        server,
        session,
        lines,
    })
}

impl AppServer {
    fn spawn(
        registry: &Arc<Registry>,
        key: &str,
        spec: &TurnSpec,
        provider: &ProviderTurn,
        log: &ProviderEventLog,
    ) -> std::io::Result<Arc<Self>> {
        let program = super::program(&provider.binary_path, "codex");
        let mut args = vec![
            "app-server".to_string(),
            "-c".into(),
            "thread_unload_delay_secs=0".into(),
        ];
        args.extend(provider.launch_args.iter().cloned());
        let mut command = crate::login_shell::tokio_command(&program);
        for (name, _) in std::env::vars_os() {
            let text = name.to_string_lossy();
            if spec
                .env_remove
                .iter()
                .any(|removed| text.eq_ignore_ascii_case(removed))
            {
                command.env_remove(&name);
            }
        }
        if !provider.home_path.trim().is_empty() {
            command.env("CODEX_HOME", &provider.home_path);
        }
        // Codex names the client in its requests, as for the reference.
        command.env("CODEX_INTERNAL_ORIGINATOR_OVERRIDE", "T3 Code");
        for (name, value) in &spec.env {
            command.env(name, value);
        }
        #[cfg(unix)]
        command.process_group(0);
        let mut child = command
            .args(&args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()?;
        let stdin = child.stdin.take();
        let stdout = child.stdout.take().expect("stdout is piped");
        if let Some(stderr) = child.stderr.take() {
            tokio::spawn(super::log_stderr(program.clone(), stderr, Arc::default()));
        }
        let (exit_tx, exited) = watch::channel(None);
        let server = Arc::new(Self {
            key: key.to_owned(),
            registry: Arc::downgrade(registry),
            stdin: tokio::sync::Mutex::new(stdin),
            next_id: AtomicU64::new(1),
            next_session: AtomicU64::new(1),
            routes: Mutex::default(),
            initialized: tokio::sync::Mutex::new(false),
            loaded: Mutex::default(),
            timers: Mutex::default(),
            idle: Mutex::default(),
            exited,
            pid: child.id(),
            // The instance's shared provider session.
            log: log
                .protocol(
                    Protocol::Codex {
                        session: super::provider_session_id(spec, provider, false),
                    },
                    None,
                )
                .map(|log| (log, Mutex::default())),
        });
        let reader = Arc::downgrade(&server);
        tokio::spawn(async move {
            let mut lines = BufReader::new(stdout).lines();
            while let Ok(Some(line)) = lines.next_line().await {
                let Some(server) = reader.upgrade() else {
                    break;
                };
                if !line.trim().is_empty() {
                    server.route(line);
                }
            }
            let status = tokio::time::timeout(Duration::from_secs(2), child.wait())
                .await
                .ok()
                .and_then(Result::ok);
            if status.is_none() {
                let _ = child.kill().await;
            }
            let _ = exit_tx.send(Some(status));
            if let Some(server) = reader.upgrade() {
                server.closed();
            }
        });
        Ok(server)
    }

    fn lock_routes(&self) -> std::sync::MutexGuard<'_, Routes> {
        self.routes.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Send a line from the app-server to the turn it belongs to.
    fn route(&self, line: String) {
        let message = serde_json::from_str::<Value>(&line).ok();
        self.log_frame(Direction::Incoming, &line, message.as_ref());
        let Some(message) = message else {
            return;
        };
        let mut routes = self.lock_routes();
        if message.get("method").is_none() {
            let Some((session, method)) = message["id"]
                .as_u64()
                .and_then(|id| routes.replies.remove(&id))
            else {
                return;
            };
            // The turn's thread, bound as Codex names it, so what Codex
            // sends about it next reaches the turn.
            // Only its first: a resume that reads a subagent's thread does not take the
            // turn's own notifications away from it.
            if method == "thread/start" || method == "thread/resume" {
                if let Some(thread) = message["result"]["thread"]["id"].as_str() {
                    if let Some(entry) = routes.sessions.get_mut(&session) {
                        entry.1.get_or_insert_with(|| thread.to_owned());
                    }
                }
            }
            if let Some((tx, _)) = routes.sessions.get(&session) {
                let _ = tx.send(line);
            }
            return;
        }
        let params = &message["params"];
        let thread = params["threadId"]
            .as_str()
            .or(params["thread"]["id"].as_str())
            .or(params["conversationId"].as_str());
        match thread {
            Some(thread) => {
                if let Some((tx, _)) = routes
                    .sessions
                    .values()
                    .find(|(_, bound)| bound.as_deref() == Some(thread))
                {
                    let _ = tx.send(line);
                }
            }
            // Account-wide news (rate limits) goes to every turn.
            None => {
                for (tx, _) in routes.sessions.values() {
                    let _ = tx.send(line.clone());
                }
            }
        }
    }

    /// The process ended: every open turn's channel closes, and the next
    /// turn starts a new process.
    fn closed(&self) {
        let mut routes = self.lock_routes();
        routes.sessions.clear();
        routes.replies.clear();
        drop(routes);
        for (_, timer) in self
            .timers
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .drain()
        {
            timer.abort();
        }
        if let Some(registry) = self.registry.upgrade() {
            registry.with(|all| {
                if all
                    .get(&self.key)
                    .is_some_and(|s| std::ptr::eq(s.as_ref(), self))
                {
                    all.remove(&self.key);
                }
            });
        }
    }

    fn next_id(&self) -> u64 {
        self.next_id.fetch_add(1, Ordering::SeqCst)
    }

    /// Record a frame in the provider event log of the thread it belongs to.
    fn log_frame(&self, direction: Direction, line: &str, frame: Option<&Value>) {
        if let Some((log, router)) = &self.log {
            let thread = frame.and_then(|frame| {
                router
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .route(direction, frame)
            });
            log.line_for(direction, line, thread.as_deref());
        }
    }

    async fn write_line(&self, message: &Value) -> std::io::Result<()> {
        let mut stdin = self.stdin.lock().await;
        let Some(stdin) = stdin.as_mut() else {
            return Err(std::io::ErrorKind::BrokenPipe.into());
        };
        let mut line = message.to_string();
        self.log_frame(Direction::Outgoing, &line, Some(message));
        line.push('\n');
        stdin.write_all(line.as_bytes()).await?;
        stdin.flush().await
    }

    /// Whether `thread` is loaded with `key`; the thread's unload timer
    /// stops either way, as a turn on it starts.
    pub(super) fn holds(&self, thread: &str, key: &str) -> bool {
        if let Some(timer) = self
            .timers
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(thread)
        {
            timer.abort();
        }
        self.loaded
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(thread)
            .is_some_and(|loaded| loaded == key)
    }

    pub(super) fn mark_loaded(&self, thread: &str, key: Option<String>) {
        let mut loaded = self.loaded.lock().unwrap_or_else(|e| e.into_inner());
        match key {
            Some(key) => loaded.insert(thread.to_owned(), key),
            None => loaded.remove(thread),
        };
    }

    fn thread_busy(&self, thread: &str) -> bool {
        self.lock_routes()
            .sessions
            .values()
            .any(|(_, bound)| bound.as_deref() == Some(thread))
    }

    /// `thread/unsubscribe`: Codex unloads the thread (its unload delay is
    /// zero), and its next turn resumes it.
    async fn unload(self: Arc<Self>, thread: String) {
        if self.exited.borrow().is_some() || self.thread_busy(&thread) {
            return;
        }
        self.mark_loaded(&thread, None);
        let id = self.next_id();
        let _ = self
            .write_line(&json!({"id": id, "method": "thread/unsubscribe",
                "params": {"threadId": thread}}))
            .await;
    }

    fn schedule_unload(self: &Arc<Self>, thread: String) {
        let server = Arc::downgrade(self);
        let key = thread.clone();
        let timer = tokio::spawn(async move {
            tokio::time::sleep(THREAD_UNLOAD_DELAY).await;
            if let Some(server) = server.upgrade() {
                server
                    .timers
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .remove(&key);
                server.unload(key).await;
            }
        });
        if let Some(previous) = self
            .timers
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(thread, timer)
        {
            previous.abort();
        }
    }

    fn schedule_release(self: &Arc<Self>) {
        let server = Arc::downgrade(self);
        let timer = tokio::spawn(async move {
            tokio::time::sleep(IDLE_RELEASE_DELAY).await;
            if let Some(server) = server.upgrade() {
                if server.lock_routes().sessions.is_empty() {
                    server.kill();
                }
            }
        });
        if let Some(previous) = self
            .idle
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .replace(timer)
        {
            previous.abort();
        }
    }

    /// Stop the process and everything it started.
    fn kill(&self) {
        #[cfg(unix)]
        if let Some(pid) = self.pid.and_then(|pid| i32::try_from(pid).ok()) {
            // SAFETY: kill(2) with a negative pid signals that process group;
            // it touches no memory of ours.
            unsafe { libc::kill(-pid, libc::SIGKILL) };
        }
    }
}

impl Conn {
    /// The next line Codex sent this turn, or `None` once the process ended.
    pub(super) async fn next_line(&mut self) -> Option<String> {
        self.lines.recv().await
    }

    pub(super) fn next_id(&self) -> u64 {
        self.server.next_id()
    }

    /// Write one message; a request's reply will come back to this turn.
    pub(super) async fn write(&self, message: &Value) -> std::io::Result<()> {
        if let (Some(id), Some(method)) = (message["id"].as_u64(), message["method"].as_str()) {
            self.server
                .lock_routes()
                .replies
                .insert(id, (self.session, method.to_owned()));
        }
        self.server.write_line(message).await
    }

    /// Bind the turn to `thread` (a thread it uses without resuming it).
    pub(super) fn bind(&self, thread: &str) {
        if let Some(entry) = self.server.lock_routes().sessions.get_mut(&self.session) {
            entry.1 = Some(thread.to_owned());
        }
    }

    /// How the process exited, if it has.
    pub(super) async fn exit_status(&mut self) -> Option<std::process::ExitStatus> {
        let mut exited = self.server.exited.clone();
        let _ =
            tokio::time::timeout(Duration::from_secs(2), exited.wait_for(Option::is_some)).await;
        let status = *exited.borrow();
        status.flatten()
    }
}

impl Drop for Conn {
    /// The turn ended: its thread stays loaded until idle, and the process
    /// until no turn has run for a while.
    fn drop(&mut self) {
        let thread = {
            let mut routes = self.server.lock_routes();
            routes
                .replies
                .retain(|_, (session, _)| *session != self.session);
            let thread = routes.sessions.remove(&self.session).and_then(|(_, t)| t);
            let idle = routes.sessions.is_empty();
            drop(routes);
            if idle && self.server.exited.borrow().is_none() {
                self.server.schedule_release();
            }
            thread
        };
        if let Some(thread) = thread {
            if self.server.exited.borrow().is_none() {
                self.server.schedule_unload(thread);
            }
        }
    }
}

/// Unload `thread` from its instance's app-server once its turn ended, as
/// the reference's detach does; the process stays.
pub(crate) fn unload_thread(registry: &Registry, instance: &str, thread: &str) {
    let Some(server) = registry.with(|all| all.get(instance).cloned()) else {
        return;
    };
    let thread = thread.to_owned();
    tokio::spawn(async move {
        let deadline = tokio::time::Instant::now() + UNLOAD_WAIT;
        while server.thread_busy(&thread) && tokio::time::Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        if let Some(timer) = server
            .timers
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&thread)
        {
            timer.abort();
        }
        server.unload(thread).await;
    });
}

/// Stop every shared app-server, once their turns ended or after `grace`
/// (the server is shutting down).
pub(crate) async fn shutdown(registry: &Registry, grace: Duration) {
    let all: Vec<Arc<AppServer>> = registry.with(|all| all.drain().map(|(_, s)| s).collect());
    let deadline = tokio::time::Instant::now() + grace;
    for server in &all {
        while !server.lock_routes().sessions.is_empty() && tokio::time::Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        server.kill();
    }
}
