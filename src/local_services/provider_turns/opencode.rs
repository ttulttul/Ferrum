//! OpenCode 1.x turns on its HTTP server, as the reference's OpenCode
//! adapter (`provider-opencode`, `makeOpenCodeAdapterV2`) runs them.
//!
//! A thread keeps a provider session across turns ([`Sessions`]): the
//! configured server, or an `opencode serve` of its own, with T3's MCP server
//! registered (`POST /mcp`) and the directory's event stream (`GET /event`)
//! open. The OpenCode session (`ses_…`) is the native thread; a later session
//! resumes it with `GET /session/<id>`.
//!
//! A turn sends `POST /session/<id>/prompt_async` with a message id T3 picks,
//! then follows the event stream: text, reasoning and tool parts become
//! items, `permission.asked` asks the user, and the session going idle ends
//! the turn, once OpenCode took the prompt (a stale idle from the previous
//! run does not). Stop aborts the session's run (`POST /session/<id>/abort`)
//! and the turn ends when OpenCode reports the session idle; the server keeps
//! running. An OpenCode 2 CLI or server runs its turns in [`super::opencode2`].
use super::opencode2;
use super::{cancelled, next_response, program, Sink, TurnEnd};
use crate::provider_status::opencode as status;
use crate::workspace::provider::{
    ProviderFailure, ProviderItem, ProviderTurn, RuntimeResponse, ThreadDisposition, TokenUsage,
    TurnStatus, OPENCODE,
};
use crate::workspace::{ProviderEvent, TurnSpec};
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};

/// A request's timeout (the adapter's 10 s for its own calls).
const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
/// Stop's abort (`OPENCODE_STOP_CONFIRM_TIMEOUT_MS`).
const STOP_TIMEOUT: Duration = Duration::from_secs(10);
/// How long a stopped turn waits for OpenCode to report the session idle.
const STOP_SETTLE: Duration = Duration::from_secs(15);
/// How often an idle seen before the prompt was taken is checked again.
const STATUS_POLL: Duration = Duration::from_millis(250);
/// How long a session stays open after its thread's last turn (the
/// reference's idle release for providers that resume natively).
const IDLE_RELEASE_DELAY: Duration = Duration::from_secs(120);
const STREAM_ENDED: &str = "OpenCode event stream ended unexpectedly.";
const STREAM_GONE: &str =
    "OpenCode event stream has ended; reconnect the provider session before starting another turn.";
const NO_ERROR_PAYLOAD: &str = "OpenCode session failed without an error payload.";

/// Where a provider session's events come from.
pub(super) enum Stream {
    Event(Value),
    /// The stream ended, or the server exited: every turn fails.
    Ended(ProviderFailure),
}

/// The threads' provider sessions, by app thread.
#[derive(Default)]
pub(crate) struct Sessions {
    live: Mutex<HashMap<String, Live>>,
    /// The OpenCode 2 provider sessions, one per instance.
    pub(super) v2: opencode2::Instances,
    /// The managed servers, shared with the status checks and text
    /// generation once [`Sessions::share_servers`] hands them over.
    owners: Mutex<status::Owners>,
}

struct Live {
    oc: OpenCode,
    idle: tokio::task::JoinHandle<()>,
}

impl Sessions {
    /// Run turns on the managed servers the status checks use: an OpenCode
    /// 2 instance has one `opencode serve` for everything (`OpenCode2Server`).
    pub(crate) fn share_servers(&self, owners: status::Owners) {
        *self.owners.lock().unwrap_or_else(|e| e.into_inner()) = owners;
    }

    pub(super) fn owners(&self) -> status::Owners {
        self.owners
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    fn live(&self) -> std::sync::MutexGuard<'_, HashMap<String, Live>> {
        self.live.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// The thread's open session, when it was opened as `launch` and still
    /// works; any other is closed.
    async fn take(&self, thread: &str, launch: &str) -> Option<OpenCode> {
        let live = self.live().remove(thread)?;
        live.idle.abort();
        let mut oc = live.oc;
        if oc.launch == launch && oc.dead.is_none() && oc.drain_dead() {
            return Some(oc);
        }
        oc.close().await;
        None
    }

    fn keep(self: &Arc<Self>, thread: String, oc: OpenCode) {
        let sessions: Weak<Self> = Arc::downgrade(self);
        let key = thread.clone();
        let idle = tokio::spawn(async move {
            tokio::time::sleep(IDLE_RELEASE_DELAY).await;
            if let Some(sessions) = sessions.upgrade() {
                let live = sessions.live().remove(&key);
                if let Some(live) = live {
                    live.oc.close().await;
                }
            }
        });
        if let Some(previous) = self.live().insert(thread, Live { oc, idle }) {
            previous.idle.abort();
            tokio::spawn(previous.oc.close());
        }
    }

    /// Close the session holding the native `session`, if one is open (a
    /// detach); an OpenCode 2 session forgets the thread.
    pub(crate) fn close(&self, session: &str) {
        self.v2.unload(session);
        let mut live = self.live();
        let threads: Vec<String> = live
            .iter()
            .filter(|(_, live)| live.oc.session == session)
            .map(|(thread, _)| thread.clone())
            .collect();
        for thread in threads {
            if let Some(live) = live.remove(&thread) {
                live.idle.abort();
                tokio::spawn(live.oc.close());
            }
        }
    }

    pub(crate) async fn shutdown(&self) {
        self.v2.shutdown().await;
        let all: Vec<Live> = self.live().drain().map(|(_, live)| live).collect();
        for live in all {
            live.idle.abort();
            live.oc.close().await;
        }
    }
}

/// One provider session: its server, event stream and OpenCode session.
struct OpenCode {
    /// What it was opened with: a turn opened otherwise opens another.
    launch: String,
    http: reqwest::Client,
    base: String,
    authorization: Option<String>,
    external: bool,
    cwd: String,
    /// Stops the managed server (its process group).
    /// Stops the managed server (its process group), and the task that
    /// does it, which closing waits for.
    stop_server: Option<(oneshot::Sender<()>, tokio::task::JoinHandle<()>)>,
    stream: mpsc::UnboundedReceiver<Stream>,
    stream_task: tokio::task::JoinHandle<()>,
    /// The OpenCode session: the native thread.
    session: String,
    /// The permission rules the session holds.
    permission: Value,
    /// Context windows by `provider/model`, from `GET /provider`.
    windows: HashMap<String, u64>,
    /// The catalog's variants by `provider/model` (`None`: not listed).
    catalog: Option<Value>,
    has_mcp: bool,
    device: bool,
    /// Why the stream failed, once it did.
    dead: Option<ProviderFailure>,
    /// The message id clock: the millisecond and its counter.
    clock: (u64, u64),
    /// The session replaced one that could not be resumed: its first turn
    /// sends the recovery prompt (the thread's context, then the prompt).
    recovered: bool,
}

impl OpenCode {
    /// Whether the session still works: a failure the stream reported while
    /// no turn ran ends it.
    fn drain_dead(&mut self) -> bool {
        let mut kept = Vec::new();
        while let Ok(message) = self.stream.try_recv() {
            match message {
                Stream::Ended(failure) => {
                    self.dead = Some(failure);
                    return false;
                }
                // Events between turns (a late idle) concern no turn.
                Stream::Event(event) => kept.push(event),
            }
        }
        true
    }

    async fn close(mut self) {
        self.stream_task.abort();
        if self.external && !self.session.is_empty() {
            // A configured server keeps running: its session's run stops.
            let path = format!("/session/{}/abort", encode(&self.session));
            let _ = self
                .call(reqwest::Method::POST, &path, None, Duration::from_secs(1))
                .await;
        }
        // The server is stopped before this returns, so a server shutting
        // down leaves no `opencode serve` behind.
        if let Some((stop, stopped)) = self.stop_server.take() {
            let _ = stop.send(());
            let _ = tokio::time::timeout(Duration::from_secs(3), stopped).await;
        }
    }

    /// `makeOpenCodeMessageId`: `msg_`, the time and a counter in hex, then
    /// 28 random characters, an id OpenCode orders after the earlier ones.
    fn message_id(&mut self) -> String {
        use rand::Rng;
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or_default();
        let counter = if self.clock.0 == now {
            self.clock.1 + 1
        } else {
            1
        };
        self.clock = (now, counter);
        let encoded = (now.wrapping_mul(0x1000).wrapping_add(counter)) & ((1u64 << 48) - 1);
        const ALPHABET: &[u8] = b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
        let mut rng = rand::thread_rng();
        let random: String = (0..28)
            .map(|_| ALPHABET[rng.gen_range(0..ALPHABET.len())] as char)
            .collect();
        format!("msg_{encoded:012x}{random}")
    }

    /// A call as the 1.x SDK makes it: the directory as `?directory=` on a
    /// GET, else in `x-opencode-directory`; the JSON body, or the message
    /// `wrapClientError` gives a failure.
    async fn call(
        &self,
        method: reqwest::Method,
        path: &str,
        body: Option<&Value>,
        timeout: Duration,
    ) -> Result<Value, String> {
        let url = format!("{}{path}", self.base.trim_end_matches('/'));
        let mut request = self.http.request(method.clone(), &url).timeout(timeout);
        if method == reqwest::Method::GET {
            request = request.query(&[("directory", self.cwd.as_str())]);
        } else {
            request = request.header("x-opencode-directory", encode(&self.cwd));
        }
        if let Some(authorization) = &self.authorization {
            request = request.header("authorization", authorization);
        }
        if let Some(body) = body {
            request = request
                .header("content-type", "application/json")
                .body(body.to_string());
        }
        let response = request.send().await.map_err(|error| {
            if error.is_timeout() {
                format!("{method} {path} timed out")
            } else {
                "fetch failed".to_owned()
            }
        })?;
        let status = response.status().as_u16();
        let html = response
            .headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .is_some_and(|t| t.starts_with("text/html"));
        let text = response
            .text()
            .await
            .map_err(|_| "fetch failed".to_owned())?;
        if html {
            return Err("Request is not supported by this version of OpenCode Server (Server responded with text/html)".into());
        }
        let parsed: Option<Value> = serde_json::from_str(&text).ok();
        if (200..300).contains(&status) {
            return Ok(parsed.unwrap_or(Value::Null));
        }
        let error = parsed.unwrap_or(Value::Null);
        Err(error["data"]["message"]
            .as_str()
            .or(error["message"].as_str())
            .or(error["name"].as_str())
            .filter(|m| !m.is_empty())
            .map(str::to_owned)
            .unwrap_or_else(|| format!("{method} {url} → {status}")))
    }

    /// `GET /provider`: the models' context windows and variants.
    async fn read_catalog(&mut self) {
        if self.catalog.is_some() {
            return;
        }
        let Ok(catalog) = self
            .call(reqwest::Method::GET, "/provider", None, REQUEST_TIMEOUT)
            .await
        else {
            return;
        };
        for provider in catalog["all"].as_array().into_iter().flatten() {
            let Some(id) = provider["id"].as_str() else {
                continue;
            };
            for (model, entry) in provider["models"].as_object().into_iter().flatten() {
                let window = entry["limit"]["input"]
                    .as_u64()
                    .or(entry["limit"]["context"].as_u64());
                if let Some(window) = window {
                    self.windows.insert(format!("{id}/{model}"), window);
                }
            }
        }
        self.catalog = Some(catalog);
    }

    /// `resolveOpenCodeVariant`: only a catalog that lists the model
    /// without the variant drops it.
    async fn variant(
        &mut self,
        provider: &str,
        model: &str,
        variant: Option<&str>,
    ) -> Option<String> {
        let variant = variant?;
        self.read_catalog().await;
        let entry = self.catalog.as_ref().and_then(|catalog| {
            catalog["all"]
                .as_array()?
                .iter()
                .find(|entry| entry["id"] == provider)
                .and_then(|entry| entry["models"].get(model))
                .cloned()
        });
        match entry {
            Some(model) if model["variants"].get(variant).is_none() => None,
            _ => Some(variant.to_owned()),
        }
    }

    /// `applyPermissionRules`: the session's rules, sent only when changed.
    async fn apply_permission(&mut self, rules: &Value) -> Result<(), String> {
        if &self.permission == rules {
            return Ok(());
        }
        let path = format!("/session/{}", encode(&self.session));
        self.call(
            reqwest::Method::PATCH,
            &path,
            Some(&json!({"permission": rules})),
            REQUEST_TIMEOUT,
        )
        .await?;
        self.permission = rules.clone();
        Ok(())
    }
}

pub(super) fn encode(value: &str) -> String {
    crate::thread_projection::uri_component(value)
}

fn rule(permission: &str, action: &str) -> Value {
    json!({"permission": permission, "pattern": "*", "action": action})
}

/// `openCodePermissionRules` for a runtime mode: full access allows
/// everything; otherwise everything asks but what only reads, and
/// auto-accept-edits also allows edits.
fn permission_rules(runtime_mode: &str) -> Value {
    const RESTRICTED: [&str; 7] = [
        "bash",
        "edit",
        "webfetch",
        "websearch",
        "codesearch",
        "external_directory",
        "doom_loop",
    ];
    const ALLOWED: [&str; 8] = [
        "question",
        "read",
        "glob",
        "grep",
        "lsp",
        "todowrite",
        "task",
        "skill",
    ];
    if runtime_mode == "full-access" {
        return json!([rule("*", "allow"), rule("external_directory", "allow")]);
    }
    let mut rules = vec![rule("*", "deny")];
    rules.extend(RESTRICTED.iter().map(|p| rule(p, "deny")));
    rules.push(rule("*", "ask"));
    rules.extend(RESTRICTED.iter().map(|p| rule(p, "ask")));
    rules.extend(ALLOWED.iter().map(|p| rule(p, "allow")));
    rules.push(json!({"permission": "read", "pattern": "*.env", "action": "ask"}));
    rules.push(json!({"permission": "read", "pattern": "*.env.*", "action": "ask"}));
    rules.push(json!({"permission": "read", "pattern": "*.env.example", "action": "allow"}));
    if runtime_mode == "auto-accept-edits" {
        rules.push(rule("edit", "allow"));
    }
    Value::Array(rules)
}

/// `openCodePermissionRequestKind`.
pub(super) fn request_kind(permission: &str, tool: &str) -> &'static str {
    let permission = permission.to_lowercase();
    let tool = tool.to_lowercase();
    let edits = ["edit", "write", "patch", "apply_patch"];
    if edits.contains(&permission.as_str()) || edits.contains(&tool.as_str()) {
        return "file-change";
    }
    if matches!(
        permission.as_str(),
        "read" | "glob" | "grep" | "lsp" | "external_directory"
    ) || tool == "read"
        || tool.contains("glob")
        || tool.contains("grep")
        || tool.contains("search")
    {
        return "file-read";
    }
    "command"
}

/// What a launch is keyed by.
fn launch_key(spec: &TurnSpec, provider: &ProviderTurn) -> String {
    json!([
        provider.binary_path,
        spec.env,
        spec.cwd,
        provider.opencode,
        provider
            .mcp
            .as_ref()
            .map(|mcp| (&mcp.url, &mcp.authorization)),
    ])
    .to_string()
}

/// The server's environment: this process's, less what the turn removes,
/// with the instance's.
pub(super) fn server_env(spec: &TurnSpec) -> crate::provider_status::ProcessEnv {
    let mut env: crate::provider_status::ProcessEnv = std::env::vars()
        .filter(|(name, _)| {
            !spec
                .env_remove
                .iter()
                .any(|removed| name.eq_ignore_ascii_case(removed))
        })
        .collect();
    env.extend(spec.env.iter().cloned());
    env
}

/// The event stream (`GET /event`), read into `events` until the session
/// closes: a broken connection is retried (3 s, doubling, at most 30 s, with
/// `Last-Event-ID`), a cleanly ended one fails the session. `connected`
/// hears when the first connection was accepted.
async fn pump(
    http: reqwest::Client,
    url: String,
    directory: String,
    authorization: Option<String>,
    events: mpsc::UnboundedSender<Stream>,
    connected: oneshot::Sender<Result<(), String>>,
) {
    let mut connected = Some(connected);
    let mut attempt: u32 = 0;
    let mut last_id: Option<String> = None;
    loop {
        let mut request = http
            .get(&url)
            .query(&[("directory", directory.as_str())])
            .header("accept", "text/event-stream");
        if let Some(authorization) = &authorization {
            request = request.header("authorization", authorization);
        }
        if let Some(id) = &last_id {
            request = request.header("last-event-id", id);
        }
        match request.send().await {
            Ok(mut response) if response.status().is_success() => {
                attempt = 0;
                if let Some(connected) = connected.take() {
                    let _ = connected.send(Ok(()));
                }
                let mut pending = String::new();
                let mut data: Vec<String> = Vec::new();
                let clean = loop {
                    match response.chunk().await {
                        Ok(Some(chunk)) => {
                            pending.push_str(&String::from_utf8_lossy(&chunk));
                            while let Some(end) = pending.find('\n') {
                                let line = pending[..end].trim_end_matches('\r').to_owned();
                                pending.drain(..=end);
                                if line.is_empty() {
                                    if !data.is_empty() {
                                        let frame = data.join("\n");
                                        data.clear();
                                        if let Ok(event) = serde_json::from_str::<Value>(&frame) {
                                            if events.send(Stream::Event(event)).is_err() {
                                                return;
                                            }
                                        }
                                    }
                                } else if let Some(value) = line.strip_prefix("data:") {
                                    data.push(value.strip_prefix(' ').unwrap_or(value).to_owned());
                                } else if let Some(value) = line.strip_prefix("id:") {
                                    last_id = Some(value.trim().to_owned());
                                }
                            }
                        }
                        Ok(None) => break true,
                        Err(_) => break false,
                    }
                };
                if clean {
                    let _ = events.send(Stream::Ended(ProviderFailure::new(
                        "transport_error",
                        STREAM_ENDED,
                    )));
                    return;
                }
            }
            Ok(response) => {
                if let Some(connected) = connected.take() {
                    let _ =
                        connected.send(Err(format!("GET {url} → {}", response.status().as_u16())));
                    return;
                }
            }
            Err(_) => {
                if let Some(connected) = connected.take() {
                    let _ = connected.send(Err("fetch failed".into()));
                    return;
                }
            }
        }
        if events.is_closed() {
            return;
        }
        attempt += 1;
        let delay = Duration::from_secs(3)
            .saturating_mul(1 << (attempt - 1).min(4))
            .min(Duration::from_secs(30));
        tokio::time::sleep(delay).await;
    }
}

/// Open a provider session: its server, T3's MCP server, the event stream,
/// then the thread's OpenCode session (resumed when it still exists).
async fn open(
    spec: &TurnSpec,
    provider: &ProviderTurn,
    launch: String,
    server: status::TurnServer,
) -> Result<(OpenCode, bool), ProviderFailure> {
    let (events, stream) = mpsc::unbounded_channel();
    // A managed server's exit fails the session's turns.
    let stop_server = server.child.map(|mut child| {
        let (stop, stopped) = oneshot::channel::<()>();
        let events = events.clone();
        let watcher = tokio::spawn(async move {
            tokio::select! {
                status = child.wait() => {
                    let code = status
                        .ok()
                        .and_then(|status| status.code())
                        .map_or_else(|| "null".to_owned(), |code| code.to_string());
                    let _ = events.send(Stream::Ended(ProviderFailure::new(
                        "transport_error",
                        format!("OpenCode server exited unexpectedly ({code})."),
                    )));
                }
                _ = stopped => {
                    #[cfg(unix)]
                    if let Some(pid) = child.id().and_then(|pid| i32::try_from(pid).ok()) {
                        // SAFETY: kill(2) with a negative pid signals that
                        // process group; it touches no memory of ours.
                        unsafe { libc::kill(-pid, libc::SIGTERM) };
                    }
                    if tokio::time::timeout(Duration::from_secs(1), child.wait()).await.is_err() {
                        #[cfg(unix)]
                        if let Some(pid) = child.id().and_then(|pid| i32::try_from(pid).ok()) {
                            // SAFETY: as above.
                            unsafe { libc::kill(-pid, libc::SIGKILL) };
                        }
                        let _ = child.kill().await;
                    }
                }
            }
        });
        (stop, watcher)
    });
    let http = crate::provider_status::http_client();
    let authorization = server.password.as_deref().map(status::basic_authorization);
    let mut oc = OpenCode {
        launch,
        http: http.clone(),
        base: server.url.clone(),
        authorization: authorization.clone(),
        external: server.external,
        cwd: spec.cwd.clone(),
        stop_server,
        stream,
        stream_task: tokio::spawn(async {}),
        session: String::new(),
        permission: Value::Null,
        windows: HashMap::new(),
        catalog: None,
        has_mcp: false,
        device: false,
        dead: None,
        clock: (0, 0),
        recovered: false,
    };
    let failed = |oc: OpenCode, detail: String| async move {
        oc.close().await;
        ProviderFailure::new("provider_error", detail)
    };
    // T3's MCP server, on a server of the session's own.
    if let Some(mcp) = provider.mcp.as_ref().filter(|_| !server.external) {
        let body = json!({"name": "t3-code", "config": {"type": "remote", "url": mcp.url,
            "headers": {"Authorization": mcp.authorization}, "oauth": false}});
        if let Err(detail) = oc
            .call(reqwest::Method::POST, "/mcp", Some(&body), REQUEST_TIMEOUT)
            .await
        {
            return Err(failed(oc, detail).await);
        }
        oc.has_mcp = true;
        oc.device = mcp.device_tools;
    }
    let (connected, accepted) = oneshot::channel();
    oc.stream_task = tokio::spawn(pump(
        http,
        format!("{}/event", server.url.trim_end_matches('/')),
        spec.cwd.clone(),
        authorization,
        events,
        connected,
    ));
    match tokio::time::timeout(REQUEST_TIMEOUT, accepted).await {
        Ok(Ok(Ok(()))) => {}
        Ok(Ok(Err(detail))) => return Err(failed(oc, detail).await),
        _ => return Err(failed(oc, "OpenCode event stream did not connect.".into()).await),
    }
    // The thread's session, resumed when it exists, else a new one.
    let rules = permission_rules(provider.runtime_mode());
    if let Some(resume) = provider.resume.as_deref().filter(|id| !id.is_empty()) {
        let path = format!("/session/{}", encode(resume));
        if let Ok(session) = oc
            .call(reqwest::Method::GET, &path, None, REQUEST_TIMEOUT)
            .await
        {
            if session["id"].is_string() {
                oc.session = resume.to_owned();
                oc.permission = session["permission"].clone();
                return Ok((oc, true));
            }
        }
    }
    match oc
        .call(
            reqwest::Method::POST,
            "/session",
            Some(&json!({"permission": rules})),
            REQUEST_TIMEOUT,
        )
        .await
    {
        Ok(session) => match session["id"].as_str() {
            Some(id) => {
                oc.session = id.to_owned();
                oc.permission = rules;
                // A thread whose session could not be resumed starts over
                // with its recovered context.
                oc.recovered = provider.resume.as_deref().is_some_and(|id| !id.is_empty());
                Ok((oc, false))
            }
            None => Err(failed(oc, "OpenCode session.create returned no session.".into()).await),
        },
        Err(detail) => Err(failed(oc, detail).await),
    }
}

/// What opening a turn's provider session gave.
enum Opened {
    /// A 1.x session, and whether it resumed the thread's.
    V1(OpenCode, bool),
    V2(opencode2::Shared),
}

pub(super) async fn run(
    spec: TurnSpec,
    provider: ProviderTurn,
    cancel: oneshot::Receiver<()>,
    mut responses: mpsc::UnboundedReceiver<RuntimeResponse>,
    sink: Sink,
    sessions: Arc<Sessions>,
) {
    let mut cancel = Some(cancel);
    let thread = provider.app_thread_id.clone();
    let launch = launch_key(&spec, &provider);
    // The instance's OpenCode 2 session, when one is open, runs the turn.
    if let Some(shared) = sessions
        .v2
        .open_session(&provider.instance_id, &spec, &provider)
    {
        return opencode2::run(spec, provider, cancel, responses, sink, sessions, shared).await;
    }
    let opened = async {
        if let Some(oc) = sessions.take(&thread, &launch).await {
            return Ok(Opened::V1(oc, true));
        }
        let settings = provider.opencode.clone().unwrap_or_default();
        let binary = program(&provider.binary_path, "opencode");
        let connection = status::connect_turn_server(
            &sessions.owners(),
            &provider.instance_id,
            &binary,
            &settings.server_url,
            &settings.server_password,
            server_env(&spec),
            &spec.cwd,
        )
        .await
        .map_err(|detail| ProviderFailure::new("provider_error", detail))?;
        match connection {
            status::TurnConnection::V1(server) => open(&spec, &provider, launch, server)
                .await
                .map(|(oc, resumed)| Opened::V1(oc, resumed)),
            status::TurnConnection::V2(server) => opencode2::Shared::open(&spec, &provider, server)
                .await
                .map(Opened::V2),
        }
    };
    let opened = tokio::select! {
        opened = opened => opened,
        _ = cancelled(&mut cancel) => return sink.finish(TurnEnd::interrupted(true)),
    };
    let (mut oc, resumed) = match opened {
        Ok(Opened::V1(oc, resumed)) => (oc, resumed),
        Ok(Opened::V2(shared)) => {
            let shared = sessions.v2.keep(&provider.instance_id, shared);
            return opencode2::run(spec, provider, cancel, responses, sink, sessions, shared).await;
        }
        Err(failure) => return sink.finish(TurnEnd::failed(failure, ThreadDisposition::Reusable)),
    };
    let mut turn = Turn::new(&provider, &oc, &sink, &mut responses);
    let end = turn
        .run(&spec, &provider, &mut oc, &mut cancel, resumed)
        .await;
    let broken = end.disposition == ThreadDisposition::Broken;
    sink.finish(end);
    if broken {
        oc.close().await;
    } else {
        sessions.keep(thread, oc);
    }
}

/// A text or reasoning part as it streams.
struct Part {
    kind: &'static str,
    message: String,
    text: String,
    ended: bool,
    synthetic: bool,
    /// Whether it was shown completed.
    done: bool,
}

/// A step's tokens (`step-finish`).
#[derive(Clone, Default)]
struct Step {
    input: u64,
    output: u64,
    reasoning: u64,
    read: u64,
    write: u64,
}

impl Step {
    fn of(tokens: &Value) -> Self {
        let n = |v: &Value| v.as_u64().unwrap_or(0);
        Self {
            input: n(&tokens["input"]),
            output: n(&tokens["output"]),
            reasoning: n(&tokens["reasoning"]),
            read: n(&tokens["cache"]["read"]),
            write: n(&tokens["cache"]["write"]),
        }
    }
}

/// A request waiting for the user: OpenCode's id and what it asked.
struct Pending {
    native: String,
    question: Option<Vec<Value>>,
}

struct Turn<'a> {
    sink: &'a Sink,
    responses: &'a mut mpsc::UnboundedReceiver<RuntimeResponse>,
    session: String,
    /// The synthetic native turn (`<session>:attempt:<attempt>`).
    native_turn: String,
    model: String,
    provider_id: String,
    model_id: String,
    agent: Option<String>,
    variant: Option<String>,
    system: String,
    runtime_mode: String,
    /// The prompt whose taking may let an idle end the turn.
    admission: String,
    accepted: bool,
    observed: bool,
    idle_seen: bool,
    /// The turn's prompts (its first and its steers).
    prompts: HashSet<String>,
    /// The user message OpenCode recorded for the turn's prompt.
    user_message: Option<String>,
    roles: HashMap<String, String>,
    /// Assistant messages and whether they answer this turn's prompts.
    owned: HashMap<String, bool>,
    parts: HashMap<String, Part>,
    part_order: Vec<String>,
    tools: HashMap<String, String>,
    /// Step usage by part id, counted once, and steps waiting for their
    /// message's owner.
    counted: HashSet<String>,
    parked: HashMap<String, Vec<(String, Step)>>,
    totals: Step,
    last_step: Option<Step>,
    pending: HashMap<String, Pending>,
    by_native: HashMap<String, String>,
    interrupted: bool,
    stop_deadline: Option<tokio::time::Instant>,
    failure: Option<ProviderFailure>,
}

impl<'a> Turn<'a> {
    fn new(
        provider: &ProviderTurn,
        oc: &OpenCode,
        sink: &'a Sink,
        responses: &'a mut mpsc::UnboundedReceiver<RuntimeResponse>,
    ) -> Self {
        let attempt = provider
            .native_turn
            .strip_prefix("attempt:")
            .unwrap_or(&provider.native_turn);
        let model = provider.model.trim().to_owned();
        let (provider_id, model_id) = model
            .find('/')
            .filter(|&at| at > 0 && at < model.len() - 1)
            .map(|at| (model[..at].to_owned(), model[at + 1..].to_owned()))
            .unwrap_or_default();
        let agent = provider
            .opencode
            .as_ref()
            .map(|settings| settings.agent.clone())
            .filter(|agent| !agent.is_empty())
            .or_else(|| (provider.interaction_mode() == "plan").then(|| "plan".to_owned()));
        Self {
            sink,
            responses,
            session: oc.session.clone(),
            native_turn: format!("{}:attempt:{attempt}", oc.session),
            system: crate::agent_instructions::opencode_system(oc.has_mcp, oc.device, &model),
            model,
            provider_id,
            model_id,
            agent,
            variant: None,
            runtime_mode: provider.runtime_mode().to_owned(),
            admission: String::new(),
            accepted: false,
            observed: false,
            idle_seen: false,
            prompts: HashSet::new(),
            user_message: None,
            roles: HashMap::new(),
            owned: HashMap::new(),
            parts: HashMap::new(),
            part_order: Vec::new(),
            tools: HashMap::new(),
            counted: HashSet::new(),
            parked: HashMap::new(),
            totals: Step::default(),
            last_step: None,
            pending: HashMap::new(),
            by_native: HashMap::new(),
            interrupted: false,
            stop_deadline: None,
            failure: None,
        }
    }

    /// A prompt's body: its message id, the model, agent, variant and system
    /// text of the turn, and its parts.
    fn prompt_body(&self, message: &str, parts: Vec<Value>) -> Value {
        let mut body = json!({
            "messageID": message,
            "model": {"providerID": self.provider_id, "modelID": self.model_id},
        });
        if let Some(agent) = &self.agent {
            body["agent"] = json!(agent);
        }
        if let Some(variant) = &self.variant {
            body["variant"] = json!(variant);
        }
        body["system"] = json!(self.system);
        body["parts"] = Value::Array(parts);
        body
    }

    async fn send_prompt(&mut self, oc: &mut OpenCode, parts: Vec<Value>) -> Result<(), String> {
        let message = oc.message_id();
        let body = self.prompt_body(&message, parts);
        self.prompts.insert(message.clone());
        self.admission = message;
        self.accepted = false;
        self.observed = false;
        let path = format!("/session/{}/prompt_async", encode(&self.session));
        oc.call(reqwest::Method::POST, &path, Some(&body), REQUEST_TIMEOUT)
            .await?;
        self.accepted = true;
        Ok(())
    }

    async fn run(
        &mut self,
        spec: &TurnSpec,
        provider: &ProviderTurn,
        oc: &mut OpenCode,
        cancel: &mut Option<oneshot::Receiver<()>>,
        resumed: bool,
    ) -> TurnEnd {
        if let Some(failure) = oc.dead.clone() {
            let mut failure = failure;
            failure.message = STREAM_GONE.into();
            return TurnEnd::failed(failure, ThreadDisposition::Broken);
        }
        if self.provider_id.is_empty() {
            return TurnEnd::failed(
                ProviderFailure::new(
                    "provider_error",
                    format!(
                        "OpenCode model '{}' must use provider/model format",
                        self.model
                    ),
                ),
                ThreadDisposition::Reusable,
            );
        }
        self.sink.send(ProviderEvent::ProviderThread {
            driver: OPENCODE.into(),
            native_thread_id: oc.session.clone(),
            resumed,
            model: None,

            opencode2: None,
        });
        let rules = permission_rules(provider.runtime_mode());
        if let Err(detail) = oc.apply_permission(&rules).await {
            return TurnEnd::failed(
                ProviderFailure::new("provider_error", detail),
                ThreadDisposition::Reusable,
            );
        }
        self.variant = oc
            .variant(
                &self.provider_id,
                &self.model_id,
                provider.reasoning_effort.as_deref(),
            )
            .await;
        let text = match provider.recovery_prompt.as_deref() {
            Some(recovery) if std::mem::take(&mut oc.recovered) => recovery,
            _ => spec.prompt.as_str(),
        };
        let parts = prompt_parts(text, &provider.attachments);
        if parts.is_empty() {
            return TurnEnd::failed(
                ProviderFailure::new(
                    "provider_error",
                    "OpenCode turns require text or at least one valid attachment",
                ),
                ThreadDisposition::Reusable,
            );
        }
        self.sink.send(ProviderEvent::ProviderTurn {
            native_turn_id: Some(self.native_turn.clone()),
            native_turn_ref: None,
            conversation_head: None,
            status: TurnStatus::Running,
            model: None,
        });
        let sent = tokio::select! {
            sent = self.send_prompt(oc, parts) => sent,
            _ = cancelled(cancel) => {
                self.stop(oc).await;
                Ok(())
            }
        };
        if let Err(detail) = sent {
            if !self.interrupted {
                self.failure = Some(ProviderFailure::new("provider_error", detail));
                return self.finalize(oc, TurnStatus::Failed).await;
            }
        }
        loop {
            if let Some(end) = self.settle_if_admitted(oc).await {
                return end;
            }
            let message = tokio::select! {
                message = oc.stream.recv() => message,
                _ = cancelled(cancel), if !self.interrupted => {
                    self.stop(oc).await;
                    continue;
                }
                response = next_response(self.responses) => {
                    self.respond(oc, response).await;
                    continue;
                }
                _ = super::sleep_until(self.stop_deadline) => {
                    return self.finalize(oc, TurnStatus::Interrupted).await;
                }
            };
            match message {
                Some(Stream::Event(event)) => {
                    if let Some(end) = self.handle(oc, &event).await {
                        return end;
                    }
                }
                Some(Stream::Ended(failure)) => {
                    oc.dead = Some(failure.clone());
                    self.failure = Some(failure);
                    return self.finalize(oc, TurnStatus::Failed).await;
                }
                None => {
                    let failure = ProviderFailure::new("transport_error", STREAM_ENDED);
                    oc.dead = Some(failure.clone());
                    self.failure = Some(failure);
                    return self.finalize(oc, TurnStatus::Failed).await;
                }
            }
        }
    }

    /// Whether OpenCode took the turn's latest prompt: its reply arrived and
    /// its user message (or a finished answer) was seen.
    fn admitted(&self) -> bool {
        self.accepted && self.observed
    }

    /// An idle seen while the prompt was not yet taken ends the turn once it
    /// is, if `GET /session/status` still finds the session idle.
    async fn settle_if_admitted(&mut self, oc: &mut OpenCode) -> Option<TurnEnd> {
        if !(self.idle_seen && self.admitted()) {
            return None;
        }
        self.idle_seen = false;
        loop {
            match oc
                .call(
                    reqwest::Method::GET,
                    "/session/status",
                    None,
                    REQUEST_TIMEOUT,
                )
                .await
            {
                Ok(statuses) => {
                    let busy = statuses[&self.session]["type"]
                        .as_str()
                        .is_some_and(|kind| kind != "idle");
                    if busy {
                        return None;
                    }
                    return Some(self.idle(oc).await);
                }
                Err(_) => tokio::time::sleep(STATUS_POLL).await,
            }
        }
    }

    async fn idle(&mut self, oc: &mut OpenCode) -> TurnEnd {
        let status = if self.interrupted {
            TurnStatus::Interrupted
        } else if self.failure.is_some() {
            TurnStatus::Failed
        } else {
            TurnStatus::Completed
        };
        self.finalize(oc, status).await
    }

    /// One event of the session.
    async fn handle(&mut self, oc: &mut OpenCode, event: &Value) -> Option<TurnEnd> {
        let properties = &event["properties"];
        let session = properties["sessionID"]
            .as_str()
            .or(properties["info"]["sessionID"].as_str())
            .or(properties["part"]["sessionID"].as_str());
        let kind = event["type"].as_str().unwrap_or_default();
        if session.is_some_and(|s| s != self.session) {
            return None;
        }
        match kind {
            "message.updated" => self.message(&properties["info"]),
            "message.part.updated" => self.part(&properties["part"]),
            "message.part.delta" if properties["field"] == "text" => {
                if let Some(id) = properties["partID"].as_str() {
                    if let Some(part) = self.parts.get_mut(id) {
                        part.text
                            .push_str(properties["delta"].as_str().unwrap_or_default());
                        let id = id.to_owned();
                        self.emit_part(&id);
                    }
                }
            }
            "session.status" if properties["status"]["type"] == "idle" => {
                return self.on_idle(oc).await;
            }
            "session.idle" => return self.on_idle(oc).await,
            "session.error" => {
                let error = &properties["error"];
                let name = error["name"].as_str().unwrap_or_default();
                if name == "ContextOverflowError" {
                    return None;
                }
                if name == "MessageAbortedError" {
                    self.interrupted = true;
                    return Some(self.finalize(oc, TurnStatus::Interrupted).await);
                }
                let message = if error.is_null() {
                    NO_ERROR_PAYLOAD.to_owned()
                } else {
                    status::readable_provider_error(
                        error["data"]["message"].as_str(),
                        error["data"]["statusCode"].as_i64(),
                        error["data"]["responseBody"].as_str(),
                    )
                    .unwrap_or_else(|| name.to_owned())
                };
                let class = if name == "APIError" && error["data"]["statusCode"] == 429 {
                    "usage_limit"
                } else {
                    "provider_error"
                };
                let mut failure = ProviderFailure::new(class, message);
                failure.code = (!name.is_empty()).then(|| name.to_owned());
                self.failure = Some(failure);
                let status = if self.interrupted {
                    TurnStatus::Interrupted
                } else {
                    TurnStatus::Failed
                };
                return Some(self.finalize(oc, status).await);
            }
            "permission.asked" => self.permission(properties),
            "question.asked" => self.question(properties),
            "permission.replied" | "question.replied" => {
                if let Some(native) = properties["requestID"].as_str() {
                    if let Some(id) = self.by_native.remove(native) {
                        self.pending.remove(&id);
                    }
                }
            }
            "question.rejected" => {
                if let Some(native) = properties["requestID"].as_str() {
                    if let Some(id) = self.by_native.remove(native) {
                        self.pending.remove(&id);
                        self.sink.send(ProviderEvent::RuntimeRequestClosed {
                            id,
                            status: "cancelled".into(),
                        });
                    }
                }
            }
            _ => {}
        }
        None
    }

    async fn on_idle(&mut self, oc: &mut OpenCode) -> Option<TurnEnd> {
        if !self.admitted() {
            // A stale idle from before the prompt: checked again once the
            // prompt was taken.
            self.idle_seen = true;
            return None;
        }
        Some(self.idle(oc).await)
    }

    fn message(&mut self, info: &Value) {
        let Some(id) = info["id"].as_str() else {
            return;
        };
        let role = info["role"].as_str().unwrap_or_default();
        self.roles.insert(id.to_owned(), role.to_owned());
        if role == "user" {
            if id == self.admission {
                self.observed = true;
            }
            if self.prompts.contains(id) && self.user_message.is_none() {
                self.user_message = Some(id.to_owned());
                self.sink.send(ProviderEvent::ProviderTurn {
                    native_turn_id: None,
                    native_turn_ref: Some(weak_ref(id)),
                    conversation_head: None,
                    status: TurnStatus::Running,
                    model: None,
                });
            }
            return;
        }
        if role != "assistant" {
            return;
        }
        let owned = info["parentID"]
            .as_str()
            .is_some_and(|parent| self.prompts.contains(parent));
        self.owned.insert(id.to_owned(), owned);
        if let Some(steps) = self.parked.remove(id) {
            if owned {
                for (part, step) in steps {
                    self.count(part, step);
                }
            }
        }
        if info["time"]["completed"].is_number() {
            if self.accepted {
                self.observed = true;
            }
            let ids: Vec<String> = self
                .part_order
                .iter()
                .filter(|part| self.parts[*part].message == id)
                .cloned()
                .collect();
            for part in ids {
                if let Some(part) = self.parts.get_mut(&part) {
                    part.ended = true;
                }
                self.emit_part(&part);
            }
        }
    }

    fn count(&mut self, part: String, step: Step) {
        if !self.counted.insert(part) {
            return;
        }
        self.totals.input += step.input + step.read + step.write;
        self.totals.read += step.read;
        self.totals.write += step.write;
        self.totals.output += step.output + step.reasoning;
        self.totals.reasoning += step.reasoning;
        self.last_step = Some(step);
    }

    fn part(&mut self, part: &Value) {
        let Some(id) = part["id"].as_str() else {
            return;
        };
        let message = part["messageID"].as_str().unwrap_or_default();
        let user = self.roles.get(message).is_some_and(|role| role == "user")
            || self.prompts.contains(message);
        match part["type"].as_str().unwrap_or_default() {
            "step-finish" => {
                let step = Step::of(&part["tokens"]);
                match self.owned.get(message) {
                    Some(true) => self.count(id.to_owned(), step),
                    Some(false) => {}
                    None => self
                        .parked
                        .entry(message.to_owned())
                        .or_default()
                        .push((id.to_owned(), step)),
                }
            }
            kind @ ("text" | "reasoning") => {
                if user {
                    return;
                }
                let entry = self.parts.entry(id.to_owned()).or_insert_with(|| Part {
                    kind: if kind == "text" {
                        "assistant_message"
                    } else {
                        "reasoning"
                    },
                    message: message.to_owned(),
                    text: String::new(),
                    ended: false,
                    synthetic: false,
                    done: false,
                });
                entry.text = part["text"].as_str().unwrap_or_default().to_owned();
                entry.ended = entry.ended || part["time"]["end"].is_number();
                entry.synthetic = part["synthetic"] == true || part["ignored"] == true;
                if !self.part_order.iter().any(|known| known == id) {
                    self.part_order.push(id.to_owned());
                }
                let id = id.to_owned();
                self.emit_part(&id);
            }
            "tool" => {
                let name = part["tool"].as_str().unwrap_or_default().to_owned();
                if let Some(call) = part["callID"].as_str() {
                    self.tools.insert(call.to_owned(), name.clone());
                }
                if name == "question" || name == "task" {
                    return;
                }
                let item = tool_item(id, &name, &part["state"], self.interrupted);
                self.sink.send(ProviderEvent::TurnItem { item });
            }
            _ => {}
        }
    }

    /// `emitTextPart`: a text or reasoning part's whole text.
    fn emit_part(&mut self, id: &str) {
        let Some(part) = self.parts.get_mut(id) else {
            return;
        };
        if part.text.is_empty() || part.synthetic || part.done {
            return;
        }
        part.done = part.ended;
        let status = if part.ended { "completed" } else { "running" };
        let item = ProviderItem::new(id, part.kind, status)
            .with("text", part.text.clone())
            .with("streaming", !part.ended);
        self.sink.send(ProviderEvent::TurnItem { item });
    }

    /// `permission.asked`: an approval the user answers.
    fn permission(&mut self, properties: &Value) {
        let Some(native) = properties["id"].as_str() else {
            return;
        };
        let permission = properties["permission"].as_str().unwrap_or_default();
        let tool = properties["tool"]["callID"]
            .as_str()
            .and_then(|call| self.tools.get(call))
            .cloned()
            .unwrap_or_default();
        let kind = request_kind(permission, &tool);
        let mut open = super::requests::open(native, native, kind, &self.runtime_mode);
        let patterns: Vec<&str> = properties["patterns"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(Value::as_str)
            .collect();
        let prompt = if patterns.is_empty() {
            permission.to_owned()
        } else {
            patterns.join("\n")
        };
        open.fields.insert("prompt".into(), json!(prompt));
        open.fields.insert("title".into(), json!(permission));
        self.by_native.insert(native.to_owned(), open.id.clone());
        self.pending.insert(
            open.id.clone(),
            Pending {
                native: native.to_owned(),
                question: None,
            },
        );
        self.sink
            .send(ProviderEvent::RuntimeRequestOpened { request: open });
    }

    /// `question.asked`: questions the user answers.
    fn question(&mut self, properties: &Value) {
        let Some(native) = properties["id"].as_str() else {
            return;
        };
        let questions: Vec<Value> = properties["questions"]
            .as_array()
            .into_iter()
            .flatten()
            .enumerate()
            .map(|(index, question)| question_entry(index, question))
            .collect();
        let mut open = super::requests::open(native, native, "user_input", &self.runtime_mode);
        open.fields.insert("questions".into(), json!(questions));
        open.fields.insert("title".into(), json!("User input"));
        self.by_native.insert(native.to_owned(), open.id.clone());
        self.pending.insert(
            open.id.clone(),
            Pending {
                native: native.to_owned(),
                question: Some(questions),
            },
        );
        self.sink
            .send(ProviderEvent::RuntimeRequestOpened { request: open });
    }

    /// The user's answer, or a message that steers the turn.
    async fn respond(&mut self, oc: &mut OpenCode, response: RuntimeResponse) {
        if let Some(text) = &response.steer {
            let parts = prompt_parts(text, &[]);
            if !parts.is_empty() {
                let _ = self.send_prompt(oc, parts).await;
            }
            return;
        }
        let Some(pending) = self.pending.get(&response.id) else {
            return;
        };
        let (path, body) = match &pending.question {
            None => {
                let reply = match super::requests::decision(&response) {
                    "accept" => "once",
                    "acceptForSession" | "acceptAlways" => "always",
                    _ => "reject",
                };
                (
                    format!("/permission/{}/reply", encode(&pending.native)),
                    json!({"reply": reply}),
                )
            }
            Some(questions) => {
                let answers = response.answers.clone().unwrap_or(Value::Null);
                let rows: Vec<Value> = questions
                    .iter()
                    .map(|question| {
                        let answer = [&question["id"], &question["header"], &question["question"]]
                            .into_iter()
                            .filter_map(|key| key.as_str())
                            .find_map(|key| answers.get(key))
                            .cloned()
                            .unwrap_or(Value::Null);
                        match answer {
                            Value::String(text) if !text.trim().is_empty() => json!([text]),
                            Value::Array(values) => {
                                json!(values.iter().filter(|v| v.is_string()).collect::<Vec<_>>())
                            }
                            _ => json!([]),
                        }
                    })
                    .collect();
                (
                    format!("/question/{}/reply", encode(&pending.native)),
                    json!({"answers": rows}),
                )
            }
        };
        let _ = oc
            .call(reqwest::Method::POST, &path, Some(&body), REQUEST_TIMEOUT)
            .await;
    }

    /// Stop (`interruptTurn`): abort the session's run and its children's;
    /// the turn ends when OpenCode reports the session idle.
    async fn stop(&mut self, oc: &mut OpenCode) {
        self.interrupted = true;
        self.stop_deadline = Some(tokio::time::Instant::now() + STOP_SETTLE);
        let path = format!("/session/{}/abort", encode(&self.session));
        let _ = oc
            .call(reqwest::Method::POST, &path, None, STOP_TIMEOUT)
            .await;
        let children = format!("/session/{}/children", encode(&self.session));
        if let Ok(children) = oc
            .call(reqwest::Method::GET, &children, None, REQUEST_TIMEOUT)
            .await
        {
            for child in children.as_array().into_iter().flatten() {
                if let Some(id) = child["id"].as_str() {
                    let path = format!("/session/{}/abort", encode(id));
                    let _ = oc
                        .call(reqwest::Method::POST, &path, None, REQUEST_TIMEOUT)
                        .await;
                }
            }
        }
        // An idle before the stop can end the turn now.
        self.accepted = true;
        self.observed = true;
    }

    /// `finalizeTurn`: open parts end, waiting requests are cancelled, then
    /// the turn's usage and end.
    async fn finalize(&mut self, oc: &mut OpenCode, status: TurnStatus) -> TurnEnd {
        let ids = self.part_order.clone();
        for id in ids {
            if let Some(part) = self.parts.get_mut(&id) {
                part.ended = true;
            }
            self.emit_part(&id);
        }
        let mut waiting: Vec<String> = self.pending.drain().map(|(id, _)| id).collect();
        waiting.sort();
        for id in waiting {
            self.sink.send(ProviderEvent::RuntimeRequestClosed {
                id,
                status: "cancelled".into(),
            });
        }
        self.by_native.clear();
        if status == TurnStatus::Completed && self.last_step.is_some() {
            oc.read_catalog().await;
        }
        let turn_usage = if self.counted.is_empty() {
            json!({"usageScope": "main_agent", "usageStatus": "unavailable", "hasSubagents": false})
        } else {
            json!({
                "usageScope": "main_agent",
                "usageStatus": if status == TurnStatus::Completed && self.parked.is_empty() {
                    "complete"
                } else {
                    "partial"
                },
                "inputTokens": self.totals.input,
                "cachedInputTokens": self.totals.read,
                "cacheCreationTokens": self.totals.write,
                "outputTokens": self.totals.output,
                "reasoningTokens": self.totals.reasoning,
                "hasSubagents": false,
            })
        };
        if let Some(step) = &self.last_step {
            let input = step.input + step.read + step.write;
            self.sink.send(ProviderEvent::Usage {
                usage: TokenUsage {
                    used_tokens: input + step.output,
                    max_tokens: oc
                        .windows
                        .get(&format!("{}/{}", self.provider_id, self.model_id))
                        .copied(),
                    input_tokens: input,
                    cached_input_tokens: step.read,
                    output_tokens: step.output,
                    reasoning_output_tokens: step.reasoning,
                    ..TokenUsage::default()
                },
            });
        }
        self.sink
            .send(ProviderEvent::TurnUsage { usage: turn_usage });
        self.sink.send(ProviderEvent::ProviderTurn {
            native_turn_id: None,
            native_turn_ref: self.user_message.as_deref().map(weak_ref),
            conversation_head: self.user_message.clone(),
            status,
            model: None,
        });
        let broken = oc.dead.is_some();
        match status {
            TurnStatus::Interrupted => TurnEnd::interrupted(true),
            TurnStatus::Failed => TurnEnd::failed(
                self.failure.take().unwrap_or_else(|| {
                    ProviderFailure::new("provider_error", "Provider turn failed.")
                }),
                if broken {
                    ThreadDisposition::Broken
                } else {
                    ThreadDisposition::Reusable
                },
            ),
            _ => TurnEnd::completed(),
        }
    }
}

pub(super) fn weak_ref(id: &str) -> Value {
    json!({"driver": OPENCODE, "nativeId": id, "strength": "weak"})
}

/// A question as the reference shows it (`openCodeQuestions`).
fn question_entry(index: usize, question: &Value) -> Value {
    let header = question["header"].as_str().unwrap_or_default().trim();
    let slug: String = {
        let lowered = header.to_lowercase();
        let mut out = String::new();
        let mut dash = false;
        for c in lowered.chars() {
            if c.is_ascii_alphanumeric() || c == '_' || c == '-' {
                out.push(c);
                dash = false;
            } else if !dash {
                out.push('-');
                dash = true;
            }
        }
        out
    };
    let id = if slug.is_empty() {
        format!("question-{index}")
    } else {
        format!("question-{index}-{slug}")
    };
    let fallback = format!("Question {}", index + 1);
    let header_text = if header.is_empty() {
        fallback.clone()
    } else {
        header.to_owned()
    };
    let text = question["question"].as_str().unwrap_or_default().trim();
    let mut entry = json!({
        "id": id,
        "header": header_text,
        "question": if text.is_empty() { header_text.clone() } else { text.to_owned() },
        "options": question["options"].as_array().into_iter().flatten().map(|option| {
            let label = option["label"].as_str().unwrap_or_default().trim();
            let label = if label.is_empty() { "Option" } else { label };
            let description = option["description"].as_str().unwrap_or_default().trim();
            json!({"label": label, "description": if description.is_empty() { label } else { description }})
        }).collect::<Vec<_>>(),
    });
    if let Some(multiple) = question["multiple"].as_bool() {
        entry["multiSelect"] = json!(multiple);
    }
    if let Some(custom) = question["custom"].as_bool() {
        entry["allowCustomAnswer"] = json!(custom);
    }
    entry
}

/// A prompt's parts (`resolvePromptParts`): its text with the attachments'
/// paths, then each attachment OpenCode reads as a file part.
fn prompt_parts(text: &str, attachments: &[Value]) -> Vec<Value> {
    let text = crate::workspace::attachment_prompt::text_with_paths(text, attachments);
    let text = text.trim();
    let mut parts = Vec::new();
    if !text.is_empty() {
        parts.push(json!({"type": "text", "text": text}));
    }
    for attachment in attachments {
        let mime = attachment["mimeType"].as_str().unwrap_or_default();
        if !matches!(
            mime,
            "image/png"
                | "image/jpeg"
                | "image/gif"
                | "image/webp"
                | "text/plain"
                | "application/pdf"
        ) {
            continue;
        }
        let Some(path) = attachment["path"].as_str() else {
            continue;
        };
        let small = std::fs::metadata(path).is_ok_and(|meta| meta.len() <= 20 * 1024 * 1024);
        let Ok(url) = url::Url::from_file_path(path) else {
            continue;
        };
        if !small {
            continue;
        }
        parts.push(json!({"type": "file", "mime": mime,
            "filename": attachment["name"].as_str().unwrap_or_default(), "url": url.as_str()}));
    }
    parts
}

/// A tool part's item (`openCodeToolTurnItem`), by the tool's name.
pub(super) fn tool_item(id: &str, name: &str, state: &Value, interrupted: bool) -> ProviderItem {
    let input = if state["input"].is_object() {
        state["input"].clone()
    } else {
        json!({})
    };
    let metadata = &state["metadata"];
    let lower = name.to_lowercase();
    let kind = if name.starts_with("mcp__") || lower == "todowrite" {
        "dynamic_tool"
    } else if lower.contains("bash") || lower.contains("shell") {
        "command_execution"
    } else if lower.contains("edit") || lower.contains("write") || lower.contains("patch") {
        "file_change"
    } else if lower.contains("web") || lower == "codesearch" || lower == "code_search" {
        "web_search"
    } else if lower == "read" {
        "dynamic_tool"
    } else if ["glob", "grep", "search", "lsp"]
        .iter()
        .any(|word| lower.contains(word))
    {
        "file_search"
    } else {
        "dynamic_tool"
    };
    let exit = metadata["exit"].as_i64().or(metadata["exitCode"].as_i64());
    let status = match state["status"].as_str().unwrap_or_default() {
        "pending" => "pending",
        "completed" if kind == "command_execution" && exit.is_some_and(|code| code != 0) => {
            "failed"
        }
        "completed" => "completed",
        "error"
            if interrupted
                || metadata["interrupted"] == true
                || state["error"] == "Tool execution aborted" =>
        {
            "interrupted"
        }
        "error" => "failed",
        _ => "running",
    };
    let output = match state["status"].as_str() {
        Some("completed") => state["output"].as_str().map(str::to_owned),
        Some("error") => state["error"].as_str().map(str::to_owned),
        _ => None,
    };
    let text = |keys: &[&str]| {
        keys.iter()
            .find_map(|key| input[*key].as_str())
            .map(str::to_owned)
    };
    let nonblank = |keys: &[&str]| {
        keys.iter()
            .find_map(|key| input[*key].as_str().filter(|v| !v.trim().is_empty()))
            .map(str::to_owned)
    };
    let mut item = ProviderItem::new(id, kind, status);
    if let Some(title) = state["title"].as_str() {
        item = item.titled(title);
    }
    match kind {
        "command_execution" => {
            let command = nonblank(&["command", "cmd"])
                .unwrap_or_else(|| serde_json::to_string_pretty(&input).unwrap_or_default());
            item = item.with("input", command);
            if let Some(output) = output {
                item = item.with("output", output);
            }
            if let Some(exit) = exit.filter(|_| state["status"] == "completed") {
                item = item.with("exitCode", exit);
            }
        }
        "file_change" => {
            item = item.with(
                "fileName",
                nonblank(&["filePath", "path", "file"]).unwrap_or_else(|| name.to_owned()),
            );
            if let Some(old) = text(&["oldString", "oldText"]) {
                item = item.with("oldStr", old);
            }
            if let Some(new) = text(&["newString", "content", "newText"]) {
                item = item.with("newStr", new);
            }
            let diff = ["diff", "patch"]
                .iter()
                .find_map(|key| metadata[*key].as_str().filter(|v| !v.trim().is_empty()))
                .map(str::to_owned)
                .or_else(|| {
                    output
                        .clone()
                        .filter(|o| status == "failed" && !o.trim().is_empty())
                });
            if let Some(diff) = diff {
                item = item.with("diffStr", diff);
            }
        }
        "file_search" => {
            if let Some(pattern) = nonblank(&["pattern", "query", "path", "filePath"]) {
                item = item.with("pattern", pattern);
            }
        }
        "web_search" => {
            if let Some(pattern) = nonblank(&["query", "url", "pattern"]) {
                item = item.with("patterns", json!([pattern]));
            }
        }
        _ => {
            if lower == "read" {
                if let Some(path) = nonblank(&["filePath", "path", "file"]) {
                    item = item.titled(format!("Read {path}"));
                }
            }
            item = item.with("toolName", name).with("input", input.clone());
            if let Some(output) = output {
                item = item.with("output", output);
            }
        }
    }
    item
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn runtime_modes_become_permission_rules() {
        let full = permission_rules("full-access");
        assert_eq!(full.as_array().map(Vec::len), Some(2));
        let supervised = permission_rules("approval-required");
        let rules = supervised.as_array().unwrap();
        assert_eq!(rules[0], rule("*", "deny"));
        assert!(rules.contains(&rule("bash", "ask")));
        assert!(!rules.contains(&rule("edit", "allow")));
        assert!(permission_rules("auto-accept-edits")
            .as_array()
            .unwrap()
            .contains(&rule("edit", "allow")));
    }

    #[test]
    fn a_failed_command_and_an_aborted_tool() {
        let state = json!({"status": "completed", "input": {"command": "false"},
            "output": "", "metadata": {"exit": 1}, "title": "false"});
        let item = tool_item("prt_1", "bash", &state, false);
        assert_eq!(item.kind, "command_execution");
        assert_eq!(item.status, "failed");
        let aborted = json!({"status": "error", "input": {}, "error": "Tool execution aborted"});
        assert_eq!(
            tool_item("prt_2", "edit", &aborted, false).status,
            "interrupted"
        );
    }

    #[test]
    fn question_ids_follow_their_headers() {
        let entry = question_entry(
            0,
            &json!({"header": "Schema vs flexibility", "question": "Which?",
            "options": [{"label": "Strict schemas", "description": ""}]}),
        );
        assert_eq!(entry["id"], "question-0-schema-vs-flexibility");
        assert_eq!(entry["options"][0]["description"], "Strict schemas");
    }
}
