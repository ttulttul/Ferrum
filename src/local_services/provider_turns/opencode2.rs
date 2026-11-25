//! OpenCode 2 turns, as the reference's OpenCode 2 adapter
//! (`provider-opencode/src/server/v2/adapter.ts`, `OpenCode2Adapter`) runs
//! them.
//!
//! Unlike 1.x, one provider session serves every thread of an instance
//! ([`Shared`]): it borrows the instance's `opencode serve` (the one the
//! status checks and text generation use) or the configured server for as
//! long as it lasts, follows the server's one event stream
//! (`GET /api/event`), and routes each event to the turn of its OpenCode
//! session (`data.sessionID`). The OpenCode session (`ses_…`) is a thread's
//! native thread: created with the runtime mode's permission rules, or
//! resumed with `GET /api/session/<id>`.
//!
//! Before each turn T3's MCP server is added for the thread
//! (`PUT /api/experimental/mcp/t3-code-<thread>`) and T3's instructions are
//! written to the session (`PUT .../instructions/entries/t3-code`); the turn
//! sends `POST /api/session/<id>/prompt` with the id
//! `msg_t3_turn_<session>:<attempt>`, and ends with
//! `session.execution.succeeded`, `.failed` or `.interrupted`. Stop asks
//! OpenCode to interrupt (`POST /api/session/<id>/interrupt`); the server keeps
//! running. A lost event stream fails every running turn (`failAll`): the
//! reference's reconnect and history backfill are not ported.
use super::opencode::{self, Sessions, Stream};
use super::{cancelled, next_response, Sink, TurnEnd};
use crate::provider_status::opencode as status;
use crate::workspace::provider::{
    OpenCode2Thread, ProviderFailure, ProviderItem, ProviderTurn, RuntimeResponse,
    ThreadDisposition, TokenUsage, TurnStatus, OPENCODE,
};
use crate::workspace::{ProviderEvent, TurnSpec};
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};

/// A reply to one of OpenCode's requests (`REQUEST_REPLY_TIMEOUT`), and
/// most calls a turn makes.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
/// Catalog, agent and MCP calls (`INVENTORY_TIMEOUT`), and the active check.
const INVENTORY_TIMEOUT: Duration = Duration::from_secs(5);
/// Stop's interrupt (`INTERRUPT_TIMEOUT`).
const INTERRUPT_TIMEOUT: Duration = Duration::from_secs(10);
/// How long a stopped turn waits for OpenCode to end its execution.
const STOP_SETTLE: Duration = Duration::from_secs(15);
/// The event stream's silence limit (`OpenCode2SilentStreamError`).
const STREAM_SILENCE: Duration = Duration::from_secs(45);
/// How long the provider session stays open after its last turn (the
/// reference's idle release for providers that resume natively).
const IDLE_RELEASE_DELAY: Duration = Duration::from_secs(120);
const STREAM_LOST: &str = crate::workspace::provider::OPENCODE2_STREAM_LOST;
const STILL_STOPPING: &str =
    "OpenCode is still stopping the previous turn. Send the message again in a moment.";
const SESSION_GONE: &str =
    "The OpenCode session no longer exists. Send the message again to continue in a new session.";
const UNANSWERED: &str = "OpenCode is waiting on a request T3 Code couldn't answer.";
/// A decline's reply message: without one, OpenCode stops the whole
/// execution (which means Cancel).
const DECLINED: &str = "The user declined this request.";
const INSTRUCTIONS_KEY: &str = "t3-code";

/// The OpenCode 2 provider sessions, by instance.
#[derive(Default)]
pub(super) struct Instances {
    live: Mutex<HashMap<String, Live>>,
}

struct Live {
    shared: Arc<Shared>,
    idle: Option<tokio::task::JoinHandle<()>>,
}

impl Instances {
    fn live(&self) -> std::sync::MutexGuard<'_, HashMap<String, Live>> {
        self.live.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// The instance's open session, when it was opened as this turn would
    /// open it and its stream still works; any other is closed.
    pub(super) fn open_session(
        &self,
        instance: &str,
        spec: &TurnSpec,
        provider: &ProviderTurn,
    ) -> Option<Arc<Shared>> {
        let mut live = self.live();
        let entry = live.get_mut(instance)?;
        if entry.shared.launch == launch_key(spec, provider) && entry.shared.dead().is_none() {
            if let Some(idle) = entry.idle.take() {
                idle.abort();
            }
            return Some(entry.shared.clone());
        }
        let stale = live.remove(instance)?;
        retire(stale);
        None
    }

    /// Keep a newly opened session as the instance's.
    pub(super) fn keep(&self, instance: &str, shared: Shared) -> Arc<Shared> {
        let shared = Arc::new(shared);
        let previous = self.live().insert(
            instance.to_owned(),
            Live {
                shared: shared.clone(),
                idle: None,
            },
        );
        if let Some(previous) = previous {
            retire(previous);
        }
        shared
    }

    /// `unloadThread`: forget the thread whose OpenCode session is
    /// `session`, and remove its MCP server.
    pub(super) fn unload(&self, session: &str) {
        for entry in self.live().values() {
            if let Some(thread) = entry.shared.forget_session(session) {
                let shared = entry.shared.clone();
                tokio::spawn(async move { shared.remove_mcp(&thread).await });
            }
        }
    }

    pub(super) async fn shutdown(&self) {
        let all: Vec<Live> = self.live().drain().map(|(_, live)| live).collect();
        for live in all {
            if let Some(idle) = live.idle {
                idle.abort();
            }
            live.shared.close().await;
        }
    }
}

/// A turn of `shared` ended: a broken session closes, one no turn uses
/// closes after [`IDLE_RELEASE_DELAY`].
fn release(sessions: &Arc<Sessions>, instance: &str, shared: &Arc<Shared>) {
    let mut live = sessions.v2.live();
    let Some(entry) = live
        .get_mut(instance)
        .filter(|entry| Arc::ptr_eq(&entry.shared, shared))
    else {
        // A session another launch replaced closes with its last turn.
        if shared.running() == 0 {
            tokio::spawn(shared.clone().close());
        }
        return;
    };
    if shared.dead().is_some() {
        if let Some(entry) = live.remove(instance) {
            tokio::spawn(entry.shared.close());
        }
        return;
    }
    if shared.running() > 0 {
        return;
    }
    let sessions: Weak<Sessions> = Arc::downgrade(sessions);
    let key = instance.to_owned();
    let watched = shared.clone();
    if let Some(idle) = entry.idle.take() {
        idle.abort();
    }
    entry.idle = Some(tokio::spawn(async move {
        tokio::time::sleep(IDLE_RELEASE_DELAY).await;
        let Some(sessions) = sessions.upgrade() else {
            return;
        };
        let closing = {
            let mut live = sessions.v2.live();
            match live.get(&key) {
                Some(entry) if Arc::ptr_eq(&entry.shared, &watched) && watched.running() == 0 => {
                    live.remove(&key)
                }
                _ => None,
            }
        };
        if let Some(entry) = closing {
            entry.shared.close().await;
        }
    }));
}

/// A session no longer the instance's: it closes now, or with the last of
/// the turns it still runs ([`release`]).
fn retire(live: Live) {
    if let Some(idle) = live.idle {
        idle.abort();
    }
    if live.shared.running() == 0 {
        tokio::spawn(live.shared.close());
    }
}

/// What a session is keyed by: a turn of the instance opened otherwise
/// opens another.
fn launch_key(spec: &TurnSpec, provider: &ProviderTurn) -> String {
    json!([
        provider.binary_path,
        spec.env,
        provider
            .opencode
            .as_ref()
            .map(|o| (&o.server_url, &o.server_password))
    ])
    .to_string()
}

/// The T3 MCP server a thread's turns run with on the server.
#[derive(Clone, PartialEq)]
struct Mcp {
    name: String,
    directory: String,
    credential: String,
}

/// A thread's OpenCode session and what T3 last wrote to it
/// (`ThreadState`).
#[derive(Clone, Default)]
struct ThreadState {
    session: String,
    directory: String,
    rules: Option<Value>,
    instructions: Option<String>,
    title: Option<String>,
    agent: String,
    /// The session's model, as OpenCode reports it.
    model: Option<Value>,
    mcp: Option<Mcp>,
    /// Permissions the user allowed for the session (`acceptForSession`).
    grants: Vec<Value>,
    /// The previous turn ended locally while OpenCode may still run it.
    unsettled: bool,
    /// Steers no execution took, taken back before the next prompt.
    stranded: Vec<String>,
}

/// The models a directory's catalog lists: context windows and variants by
/// `provider/model`.
#[derive(Clone, Default)]
struct Catalog {
    windows: HashMap<String, u64>,
    variants: HashMap<String, HashSet<String>>,
}

/// One OpenCode 2 provider session: the server, its event stream and the
/// threads it serves.
pub(super) struct Shared {
    launch: String,
    http: reqwest::Client,
    base: String,
    authorization: Option<String>,
    external: bool,
    version: String,
    /// The managed server's borrow, held while the session is open.
    lease: Mutex<Option<status::Lease>>,
    /// Each running turn's events, by OpenCode session.
    routes: Arc<Mutex<HashMap<String, mpsc::UnboundedSender<Stream>>>>,
    /// Why the stream failed, once it did.
    failure: Arc<Mutex<Option<ProviderFailure>>>,
    threads: Mutex<HashMap<String, ThreadState>>,
    catalogs: Mutex<HashMap<String, Catalog>>,
    /// Agents' path rules by directory, then agent (`agentRules`).
    agents: Mutex<HashMap<String, HashMap<String, Vec<Value>>>>,
    running: std::sync::atomic::AtomicUsize,
    pump: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

impl Shared {
    fn dead(&self) -> Option<ProviderFailure> {
        self.failure
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    fn running(&self) -> usize {
        self.running.load(std::sync::atomic::Ordering::SeqCst)
    }

    /// `openSession`: subscribe to the server's events, then read the
    /// directory's models.
    pub(super) async fn open(
        spec: &TurnSpec,
        provider: &ProviderTurn,
        server: status::V2TurnServer,
    ) -> Result<Self, ProviderFailure> {
        let authorization = server.password.as_deref().map(status::basic_authorization);
        let shared = Self {
            launch: launch_key(spec, provider),
            http: crate::provider_status::http_client(),
            base: server.url.trim_end_matches('/').to_owned(),
            authorization,
            external: server.external,
            version: server.version,
            lease: Mutex::new(server.lease),
            routes: Arc::default(),
            failure: Arc::default(),
            threads: Mutex::default(),
            catalogs: Mutex::default(),
            agents: Mutex::default(),
            running: Default::default(),
            pump: Mutex::default(),
        };
        let mut request = shared
            .http
            .get(format!("{}/api/event", shared.base))
            .header("accept", "text/event-stream");
        if let Some(authorization) = &shared.authorization {
            request = request.header("authorization", authorization);
        }
        let response = match tokio::time::timeout(REQUEST_TIMEOUT, request.send()).await {
            Ok(Ok(response)) if response.status().is_success() => response,
            Ok(Ok(response)) => {
                return Err(ProviderFailure::new(
                    "provider_error",
                    format!("GET /api/event → {}", response.status().as_u16()),
                ))
            }
            _ => {
                return Err(ProviderFailure::new(
                    "provider_error",
                    "OpenCode event stream did not connect.",
                ))
            }
        };
        let pump = tokio::spawn(pump(
            response,
            shared.routes.clone(),
            shared.failure.clone(),
        ));
        *shared.pump.lock().unwrap_or_else(|e| e.into_inner()) = Some(pump);
        shared.read_models(&spec.cwd).await;
        Ok(shared)
    }

    /// The session closes: its threads' MCP servers are removed, the stream
    /// closes, and the managed server is given back.
    async fn close(self: Arc<Self>) {
        let threads: Vec<ThreadState> = self
            .threads
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .drain()
            .map(|(_, thread)| thread)
            .collect();
        for thread in &threads {
            self.remove_mcp(thread).await;
        }
        if let Some(pump) = self.pump.lock().unwrap_or_else(|e| e.into_inner()).take() {
            pump.abort();
        }
        drop(self.lease.lock().unwrap_or_else(|e| e.into_inner()).take());
    }

    /// Forget the thread whose session is `session`, returning its state.
    fn forget_session(&self, session: &str) -> Option<ThreadState> {
        let mut threads = self.threads.lock().unwrap_or_else(|e| e.into_inner());
        let key = threads
            .iter()
            .find(|(_, thread)| thread.session == session)
            .map(|(key, _)| key.clone())?;
        threads.remove(&key)
    }

    async fn remove_mcp(&self, thread: &ThreadState) {
        if let Some(mcp) = &thread.mcp {
            let path = format!("/api/experimental/mcp/{}", opencode::encode(&mcp.name));
            let _ = self
                .call(
                    reqwest::Method::DELETE,
                    &path,
                    Some(&mcp.directory),
                    None,
                    INVENTORY_TIMEOUT,
                )
                .await;
        }
    }

    /// One call with the server's credentials: a location-scoped one names
    /// its directory as `location[directory]`. The JSON reply (none for 204),
    /// or the error OpenCode named.
    async fn call(
        &self,
        method: reqwest::Method,
        path: &str,
        directory: Option<&str>,
        body: Option<&Value>,
        timeout: Duration,
    ) -> Result<Value, CallError> {
        let url = format!("{}{path}", self.base);
        let mut request = self.http.request(method.clone(), &url).timeout(timeout);
        if let Some(directory) = directory {
            request = request.query(&[("location[directory]", directory)]);
        }
        if let Some(authorization) = &self.authorization {
            request = request.header("authorization", authorization);
        }
        if let Some(body) = body {
            request = request
                .header("content-type", "application/json")
                .body(body.to_string());
        }
        let response = request.send().await.map_err(|error| CallError {
            tag: None,
            message: if error.is_timeout() {
                format!("{method} {path} timed out")
            } else {
                "fetch failed".to_owned()
            },
        })?;
        let status = response.status().as_u16();
        let text = response.text().await.unwrap_or_default();
        let parsed: Value = serde_json::from_str(&text).unwrap_or(Value::Null);
        if (200..300).contains(&status) {
            return Ok(parsed);
        }
        let tag = parsed["_tag"].as_str().map(str::to_owned);
        Err(CallError {
            message: parsed["message"]
                .as_str()
                .or(parsed["data"]["message"].as_str())
                .filter(|m| !m.is_empty())
                .map(str::to_owned)
                .or_else(|| tag.clone())
                .unwrap_or_else(|| format!("{method} {path} → {status}")),
            tag,
        })
    }

    /// `readModels`: the directory's context windows and variants (5 s,
    /// failures ignored).
    async fn read_models(&self, directory: &str) {
        let models = self
            .call(
                reqwest::Method::GET,
                "/api/model",
                Some(directory),
                None,
                INVENTORY_TIMEOUT,
            )
            .await;
        let mut catalogs = self.catalogs.lock().unwrap_or_else(|e| e.into_inner());
        let catalog = catalogs.entry(directory.to_owned()).or_default();
        for model in models
            .ok()
            .as_ref()
            .and_then(|models| models["data"].as_array())
            .into_iter()
            .flatten()
        {
            let (Some(provider), Some(id)) = (model["providerID"].as_str(), model["id"].as_str())
            else {
                continue;
            };
            let slug = format!("{provider}/{id}");
            if let Some(window) = model["limit"]["input"]
                .as_u64()
                .or(model["limit"]["context"].as_u64())
            {
                catalog.windows.insert(slug.clone(), window);
            }
            let variants = model["variants"]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(|variant| variant["id"].as_str().map(str::to_owned))
                .collect();
            catalog.variants.insert(slug, variants);
        }
    }

    fn catalog(&self, directory: &str) -> Option<Catalog> {
        self.catalogs
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(directory)
            .cloned()
    }

    /// `pathsFor`: the agents' own path allows (`edit` and
    /// `external_directory` with a specific resource), from `GET /api/agent`
    /// (read again every 250 ms while it lists none, for 5 s).
    async fn agent_paths(&self, directory: &str, agents: &[&str]) -> Vec<Value> {
        let known = self
            .agents
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(directory)
            .cloned();
        let known = match known {
            Some(known) => known,
            None => {
                let deadline = tokio::time::Instant::now() + INVENTORY_TIMEOUT;
                let listed = loop {
                    let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
                    match self
                        .call(
                            reqwest::Method::GET,
                            "/api/agent",
                            Some(directory),
                            None,
                            remaining,
                        )
                        .await
                    {
                        Ok(list) if list["data"].as_array().is_some_and(|a| !a.is_empty()) => {
                            break Some(list)
                        }
                        Ok(_)
                            if tokio::time::Instant::now() + Duration::from_millis(250)
                                < deadline =>
                        {
                            tokio::time::sleep(Duration::from_millis(250)).await;
                        }
                        _ => break None,
                    }
                };
                let Some(listed) = listed else {
                    return Vec::new();
                };
                let known: HashMap<String, Vec<Value>> = listed["data"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .filter_map(|agent| {
                        let rules = agent["permissions"]
                            .as_array()
                            .into_iter()
                            .flatten()
                            .filter(|rule| {
                                rule["effect"] == "allow"
                                    && rule["resource"] != "*"
                                    && (rule["action"] == "edit"
                                        || rule["action"] == "external_directory")
                            })
                            .map(|rule| {
                                json!({"action": rule["action"], "resource": rule["resource"], "effect": "allow"})
                            })
                            .collect();
                        Some((agent["id"].as_str()?.to_owned(), rules))
                    })
                    .collect();
                self.agents
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .insert(directory.to_owned(), known.clone());
                known
            }
        };
        let mut paths: Vec<Value> = Vec::new();
        for agent in agents {
            for rule in known.get(*agent).into_iter().flatten() {
                match paths.iter_mut().find(|known| {
                    known["action"] == rule["action"] && known["resource"] == rule["resource"]
                }) {
                    Some(known) => *known = rule.clone(),
                    None => paths.push(rule.clone()),
                }
            }
        }
        paths
    }

    /// `rulesFor`: the session's permission rules for the turn's modes.
    async fn rules_for(&self, thread: &ThreadState, provider: &ProviderTurn) -> Value {
        let full = provider.runtime_mode() == "full-access";
        let plan = provider.interaction_mode() == "plan";
        let paths = if full && !plan {
            Vec::new()
        } else if plan {
            self.agent_paths(&thread.directory, &[&thread.agent, "plan"])
                .await
        } else {
            self.agent_paths(&thread.directory, &[&thread.agent]).await
        };
        session_rules(
            provider.runtime_mode(),
            plan,
            &paths,
            if full { &[] } else { &thread.grants },
            &mcp_server_name(&provider.app_thread_id),
        )
    }
}

/// A failed call: the error's tag (`SessionNotFoundError`)
/// and message.
#[derive(Debug)]
struct CallError {
    tag: Option<String>,
    message: String,
}

impl CallError {
    fn session_gone(&self) -> bool {
        self.tag.as_deref() == Some("SessionNotFoundError")
    }

    /// `CLEAR_PROMPT_REJECTIONS`: OpenCode refused the prompt outright, so
    /// it took nothing.
    fn clear_rejection(&self) -> bool {
        matches!(
            self.tag.as_deref(),
            Some(
                "InvalidRequestError"
                    | "ConflictError"
                    | "UnauthorizedError"
                    | "CommandNotFoundError"
                    | "SkillNotFoundError"
            )
        )
    }
}

/// The event stream until it ends or goes silent for 45 s: each `data:`
/// frame goes to the turn of its session; the end fails every turn.
async fn pump(
    mut response: reqwest::Response,
    routes: Arc<Mutex<HashMap<String, mpsc::UnboundedSender<Stream>>>>,
    failure: Arc<Mutex<Option<ProviderFailure>>>,
) {
    let mut pending = String::new();
    let mut data: Vec<String> = Vec::new();
    loop {
        let chunk = match tokio::time::timeout(STREAM_SILENCE, response.chunk()).await {
            Ok(Ok(Some(chunk))) => chunk,
            _ => break,
        };
        pending.push_str(&String::from_utf8_lossy(&chunk));
        while let Some(end) = pending.find('\n') {
            let line = pending[..end].trim_end_matches('\r').to_owned();
            pending.drain(..=end);
            if line.is_empty() {
                if data.is_empty() {
                    continue;
                }
                let frame = data.join("\n");
                data.clear();
                let Ok(event) = serde_json::from_str::<Value>(&frame) else {
                    continue;
                };
                let session = event["data"]["sessionID"]
                    .as_str()
                    .or(event["data"]["form"]["sessionID"].as_str());
                let Some(session) = session else { continue };
                let route = routes
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .get(session)
                    .cloned();
                if let Some(route) = route {
                    let _ = route.send(Stream::Event(event));
                }
            } else if let Some(value) = line.strip_prefix("data:") {
                data.push(value.strip_prefix(' ').unwrap_or(value).to_owned());
            }
        }
    }
    // `failAll`: every running turn fails, and the session with them.
    let lost = ProviderFailure::new("transport_error", STREAM_LOST);
    *failure.lock().unwrap_or_else(|e| e.into_inner()) = Some(lost.clone());
    let routes: Vec<_> = routes
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .drain()
        .map(|(_, route)| route)
        .collect();
    for route in routes {
        let _ = route.send(Stream::Ended(lost.clone()));
    }
}

/// `t3McpServerName`: the thread's MCP server name, hashed when long
/// (OpenCode hides tools of names over 64 characters).
fn mcp_server_name(thread: &str) -> String {
    let name = format!(
        "t3-code-{}",
        thread
            .chars()
            .map(|c| if c.is_ascii_alphanumeric() || c == '_' || c == '-' {
                c
            } else {
                '_'
            })
            .collect::<String>()
    );
    if name.len() <= 64 {
        return name;
    }
    use sha2::{Digest, Sha256};
    let digest = hex::encode(Sha256::digest(thread.as_bytes()));
    format!("t3-code-{}", &digest[..16])
}

fn rule(action: &str, effect: &str) -> Value {
    json!({"action": action, "resource": "*", "effect": effect})
}

/// `sessionRules`, last match wins: the runtime mode's rules, the session's
/// grants, plan mode's edit deny, the agents' own path allows, then T3's MCP
/// servers denied but this thread's.
fn session_rules(
    runtime_mode: &str,
    plan: bool,
    paths: &[Value],
    grants: &[Value],
    mcp_name: &str,
) -> Value {
    let mut rules = if runtime_mode == "full-access" {
        vec![rule("*", "allow")]
    } else {
        vec![
            rule("shell", "ask"),
            rule(
                "edit",
                if runtime_mode == "auto-accept-edits" {
                    "allow"
                } else {
                    "ask"
                },
            ),
            rule("external_directory", "ask"),
        ]
    };
    rules.extend(grants.iter().cloned());
    if plan {
        rules.push(rule("edit", "deny"));
    }
    rules.extend(paths.iter().cloned());
    rules.push(json!({"action": "t3-code-*", "resource": "*", "effect": "deny"}));
    rules.push(json!({"action": format!("{mcp_name}_*"), "resource": "*", "effect": "allow"}));
    Value::Array(rules)
}

/// `modelRef`: the selection's model, without a variant the catalog lists
/// the model without. `None` for a slug that is not `provider/model`.
fn model_ref(model: &str, variant: Option<&str>, catalog: Option<&Catalog>) -> Option<Value> {
    let model = model.trim();
    let at = model
        .find('/')
        .filter(|&at| at > 0 && at < model.len() - 1)?;
    let mut reference = json!({"providerID": &model[..at], "id": &model[at + 1..]});
    let listed = catalog.and_then(|catalog| catalog.variants.get(model));
    if let Some(variant) = variant.filter(|v| listed.is_none_or(|listed| listed.contains(*v))) {
        reference["variant"] = json!(variant);
    }
    Some(reference)
}

/// `sameModel`: a session without a variant reports variant "default".
fn same_model(left: &Value, right: Option<&Value>) -> bool {
    let Some(right) = right else { return false };
    let variant = |model: &Value| model["variant"].as_str().unwrap_or("default").to_owned();
    left["providerID"] == right["providerID"]
        && left["id"] == right["id"]
        && variant(left) == variant(right)
}

/// The model selection a session reports (`withReportedModel`).
fn model_selection(instance: &str, model: Option<&Value>) -> Value {
    let Some(model) = model.filter(|m| m["providerID"].is_string() && m["id"].is_string()) else {
        return Value::Null;
    };
    let options = match model["variant"].as_str() {
        Some(variant) => json!([{"id": "variant", "value": variant}]),
        None => json!([]),
    };
    json!({
        "instanceId": instance,
        "model": format!("{}/{}", model["providerID"].as_str().unwrap_or_default(), model["id"].as_str().unwrap_or_default()),
        "options": options,
    })
}

/// `providerMessageTextWithAttachmentPaths`, trimmed.
fn prompt_text(text: &str, attachments: &[Value]) -> String {
    crate::workspace::attachment_prompt::text_with_paths(text, attachments)
        .trim()
        .to_owned()
}

/// `promptFiles`: the native image attachments whose file exists, as
/// `file://` URLs; none for an external server (it may not share T3's
/// attachment directory).
fn prompt_files(attachments: &[Value], external: bool) -> Vec<Value> {
    if external {
        return Vec::new();
    }
    attachments
        .iter()
        .filter(|attachment| {
            matches!(
                attachment["mimeType"].as_str().unwrap_or_default(),
                "image/png" | "image/jpeg" | "image/gif" | "image/webp"
            )
        })
        .filter_map(|attachment| {
            let path = attachment["path"].as_str()?;
            if !std::path::Path::new(path).exists() {
                return None;
            }
            let url = url::Url::from_file_path(path).ok()?;
            Some(json!({"uri": url.as_str(), "name": attachment["name"].as_str().unwrap_or_default()}))
        })
        .collect()
}

/// `formQuestions`: the questions a form asks, or what it asks that T3
/// cannot show.
fn form_questions(form: &Value) -> Result<Vec<Value>, String> {
    let text = |value: &Value, fallback: &str| {
        value
            .as_str()
            .map(str::trim)
            .filter(|v| !v.is_empty())
            .unwrap_or(fallback)
            .to_owned()
    };
    let mut questions = Vec::new();
    for (index, field) in form["fields"].as_array().into_iter().flatten().enumerate() {
        let kind = field["type"].as_str().unwrap_or_default();
        if kind == "external" {
            return Err("a link to open".into());
        }
        if kind != "string" && kind != "multiselect" {
            return Err(format!("a {kind} value"));
        }
        if field["hidden"] == true {
            return Err("a hidden field".into());
        }
        if field["when"]
            .as_array()
            .is_some_and(|when| !when.is_empty())
        {
            return Err("a field that depends on another".into());
        }
        let header = text(&field["title"], &format!("Question {}", index + 1));
        let options: Vec<Value> = field["options"]
            .as_array()
            .into_iter()
            .flatten()
            .map(|option| {
                let label = text(&option["label"], &text(&option["value"], "Option"));
                json!({"label": label, "description": text(&option["description"], &label),
                    "value": option["value"]})
            })
            .collect();
        questions.push(json!({
            "id": field["key"],
            "header": header,
            "question": text(&field["description"], &header),
            "options": options,
            "multiSelect": kind == "multiselect",
            "allowCustomAnswer": field["custom"] == true || options.is_empty(),
        }));
    }
    Ok(questions)
}

/// `formAnswer`: each field's answer, its non-blank strings (a list for a
/// multiple choice, else joined with ", ").
fn form_answer(form: &Value, answers: &Value) -> Value {
    let mut answer = serde_json::Map::new();
    for field in form["fields"].as_array().into_iter().flatten() {
        let Some(key) = field["key"].as_str() else {
            continue;
        };
        let raw = &answers[key];
        let values: Vec<String> = match raw {
            Value::Array(values) => values.iter().collect::<Vec<_>>(),
            other => vec![other],
        }
        .into_iter()
        .filter_map(Value::as_str)
        .filter(|v| !v.trim().is_empty())
        .map(str::to_owned)
        .collect();
        if values.is_empty() {
            continue;
        }
        answer.insert(
            key.to_owned(),
            if field["type"] == "multiselect" {
                json!(values)
            } else {
                json!(values.join(", "))
            },
        );
    }
    Value::Object(answer)
}

/// `isUsageLimit` and the reset a spent Claude plan names.
fn execution_failure(error: &Value) -> ProviderFailure {
    let message = error["message"].as_str();
    let status = error["status"].as_i64();
    let kind = error["type"].as_str().unwrap_or_default();
    let claude = message.and_then(|message| {
        regex::Regex::new(r"^You've hit your [^\n·]*\blimit\b")
            .ok()?
            .is_match(message)
            .then(|| {
                regex::Regex::new(r"\bResets at (\d{4}-\d\d-\d\dT[\d:.]+Z)\.?\s*$")
                    .ok()
                    .and_then(|reset| reset.captures(message).map(|c| c[1].to_owned()))
            })
    });
    let usage_limit = (kind == "provider.quota" && status != Some(402))
        || kind == "provider.rate-limit"
        || claude.is_some();
    let text = status::readable_provider_error(message, status, error["response"]["body"].as_str())
        .unwrap_or_else(|| "Provider turn failed.".to_owned());
    let mut failure = ProviderFailure::new(
        if usage_limit {
            "usage_limit"
        } else {
            "provider_error"
        },
        text,
    );
    failure.code = (!kind.is_empty()).then(|| kind.to_owned());
    failure.reset_at = claude.flatten();
    failure
}

/// One turn on the instance's session: the thread's session made ready,
/// the prompt sent, and its events followed until its execution ends.
pub(super) async fn run(
    spec: TurnSpec,
    provider: ProviderTurn,
    mut cancel: Option<oneshot::Receiver<()>>,
    mut responses: mpsc::UnboundedReceiver<RuntimeResponse>,
    sink: Sink,
    sessions: Arc<Sessions>,
    shared: Arc<Shared>,
) {
    shared
        .running
        .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    let thread_id = provider.app_thread_id.clone();
    let (events, received) = mpsc::unbounded_channel();
    let mut turn = Turn {
        sink: &sink,
        responses: &mut responses,
        shared: &shared,
        provider: &provider,
        events: received,
        state: ThreadState::default(),
        native_turn: String::new(),
        prompt_id: String::new(),
        awaiting_start: false,
        unsent: true,
        interrupted: false,
        stop_deadline: None,
        held_end: None,
        steers: Vec::new(),
        settled_inbox: HashSet::new(),
        texts: Vec::new(),
        tools: Vec::new(),
        pending: HashMap::new(),
        started_steps: 0,
        steps: 0,
        totals: Totals::default(),
        last_step: None,
        models: Vec::new(),
        log: opencode::ProtocolRecorder::new(&sink, &spec, &provider, true),
    };
    let end = turn.run(&spec, events, &mut cancel).await;
    let state = std::mem::take(&mut turn.state);
    if !state.session.is_empty() {
        shared
            .routes
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&state.session);
        if end.disposition == ThreadDisposition::Reusable {
            shared
                .threads
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .insert(thread_id, state);
        }
    }
    sink.finish(end);
    shared
        .running
        .fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
    release(&sessions, &provider.instance_id, &shared);
}

/// A text or reasoning block as it streams.
struct Text {
    id: String,
    kind: &'static str,
    text: String,
}

/// A tool call as it runs: its name and input.
struct Tool {
    id: String,
    name: String,
    input: Value,
}

/// What a request T3 shows asked of OpenCode.
enum Native {
    Permission {
        id: String,
        action: String,
        resources: Vec<String>,
        save: Vec<String>,
    },
    Form {
        id: String,
        form: Value,
    },
}

#[derive(Clone, Copy, Default)]
struct Totals {
    input: u64,
    cached: u64,
    cache_write: u64,
    output: u64,
    reasoning: u64,
}

/// A step's tokens.
#[derive(Clone, Copy, Default)]
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

struct Turn<'a> {
    sink: &'a Sink,
    responses: &'a mut mpsc::UnboundedReceiver<RuntimeResponse>,
    shared: &'a Arc<Shared>,
    provider: &'a ProviderTurn,
    events: mpsc::UnboundedReceiver<Stream>,
    state: ThreadState,
    /// `<session>:attempt:<attempt>`.
    native_turn: String,
    /// `msg_t3_turn_<session>:<attempt>`, the prompt's inbox id.
    prompt_id: String,
    /// Events wait for this turn's execution to start (after a turn that
    /// ended locally).
    awaiting_start: bool,
    /// The prompt is not sent yet.
    unsent: bool,
    interrupted: bool,
    stop_deadline: Option<tokio::time::Instant>,
    /// A completed end held while steers wait to be delivered.
    held_end: Option<TurnStatus>,
    steers: Vec<String>,
    settled_inbox: HashSet<String>,
    texts: Vec<Text>,
    tools: Vec<Tool>,
    pending: HashMap<String, Native>,
    started_steps: u64,
    steps: u64,
    totals: Totals,
    last_step: Option<Step>,
    models: Vec<String>,
    /// Where the turn's session calls and events are recorded.
    log: Option<opencode::ProtocolRecorder>,
}

/// A session call's name in the provider event log: `session`, the
/// path's words (not its ids), then the verb unless it posts
/// (`session.permission.reply`, `session.get`).
fn session_call_name(method: &reqwest::Method, rest: &str) -> String {
    let mut name = String::from("session");
    for segment in rest.split('/').filter(|s| !s.is_empty()) {
        if segment.bytes().all(|b| b.is_ascii_lowercase()) {
            name.push('.');
            name.push_str(segment);
        }
    }
    match method.as_str() {
        "POST" => {}
        "PATCH" => name.push_str(".update"),
        verb => {
            name.push('.');
            name.push_str(&verb.to_ascii_lowercase());
        }
    }
    name
}

impl Turn<'_> {
    fn sessions_path(&self, rest: &str) -> String {
        format!(
            "/api/session/{}{rest}",
            opencode::encode(&self.state.session)
        )
    }

    async fn call(
        &self,
        method: reqwest::Method,
        rest: &str,
        body: Option<&Value>,
        timeout: Duration,
    ) -> Result<Value, CallError> {
        let name = session_call_name(&method, rest);
        if let Some(log) = &self.log {
            log.call(&name, body);
        }
        let reply = self
            .shared
            .call(method, &self.sessions_path(rest), None, body, timeout)
            .await;
        if let (Some(log), Ok(_)) = (&self.log, &reply) {
            log.reply(&name);
        }
        reply
    }

    fn fail(failure: ProviderFailure) -> TurnEnd {
        TurnEnd::failed(failure, ThreadDisposition::Reusable)
    }

    /// The thread's session: kept from its last turn, resumed
    /// (`resumeThread`), or created (`ensureThread`). Whether it resumed.
    async fn ensure_thread(&mut self, spec: &TurnSpec) -> Result<bool, ProviderFailure> {
        let provider = self.provider;
        let kept = self
            .shared
            .threads
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&provider.app_thread_id);
        if let Some(state) = kept {
            self.state = state;
            return Ok(true);
        }
        if self.shared.catalog(&spec.cwd).is_none() {
            self.shared.read_models(&spec.cwd).await;
        }
        if let Some(session) = provider.resume.as_deref().filter(|id| !id.is_empty()) {
            let path = format!("/api/session/{}", opencode::encode(session));
            if let Ok(found) = self
                .shared
                .call(reqwest::Method::GET, &path, None, None, REQUEST_TIMEOUT)
                .await
            {
                let native = &found["data"];
                self.state = ThreadState {
                    session: session.to_owned(),
                    directory: spec.cwd.clone(),
                    rules: native.get("permissions").cloned(),
                    title: native["title"].as_str().map(str::to_owned),
                    agent: native["agent"].as_str().unwrap_or("build").to_owned(),
                    model: native.get("model").cloned(),
                    ..ThreadState::default()
                };
                self.stop_leftover_requests().await;
                let directory = native["location"]["directory"].as_str();
                if directory.is_some_and(|directory| directory != spec.cwd) {
                    let _ = self
                        .call(
                            reqwest::Method::POST,
                            "/move",
                            Some(&json!({"directory": spec.cwd})),
                            REQUEST_TIMEOUT,
                        )
                        .await;
                }
                return Ok(true);
            }
        }
        let model = model_ref(
            &provider.model,
            provider.reasoning_effort.as_deref(),
            self.shared.catalog(&spec.cwd).as_ref(),
        )
        .ok_or_else(|| {
            ProviderFailure::new(
                "provider_error",
                format!(
                    "OpenCode model '{}' must use provider/model format",
                    provider.model
                ),
            )
        })?;
        let mut state = ThreadState {
            directory: spec.cwd.clone(),
            agent: "build".into(),
            ..ThreadState::default()
        };
        let permissions = self.shared.rules_for(&state, provider).await;
        let title = provider.thread_title.trim();
        // A session created with a title is never titled by OpenCode. The
        // reference's client sends the optional fields it leaves out as null.
        let body = json!({
            "id": null,
            "parentID": null,
            "title": if title.is_empty() { Value::Null } else { json!(title) },
            "agent": null,
            "model": model,
            "location": {"directory": spec.cwd},
            "metadata": null,
            "permissions": permissions,
        });
        let created = self
            .shared
            .call(
                reqwest::Method::POST,
                "/api/session",
                None,
                Some(&body),
                REQUEST_TIMEOUT,
            )
            .await
            .map_err(|error| ProviderFailure::new("provider_error", error.message))?;
        let created = &created["data"];
        let Some(id) = created["id"].as_str() else {
            return Err(ProviderFailure::new(
                "provider_error",
                "OpenCode session.create returned no session.",
            ));
        };
        state.session = id.to_owned();
        state.model = created.get("model").cloned();
        if let Some(agent) = created["agent"].as_str() {
            state.agent = agent.to_owned();
        }
        state.title = if title.is_empty() {
            created["title"].as_str().map(str::to_owned)
        } else {
            Some(title.to_owned())
        };
        state.rules = Some(permissions);
        self.state = state;
        Ok(false)
    }

    /// `stopLeftoverRequests`: a session loaded again may still wait on
    /// requests T3 no longer shows; refuse them so it cannot wait forever.
    async fn stop_leftover_requests(&mut self) {
        let permissions = self
            .call(reqwest::Method::GET, "/permission", None, INVENTORY_TIMEOUT)
            .await;
        let forms = self
            .call(reqwest::Method::GET, "/form", None, INVENTORY_TIMEOUT)
            .await;
        let (Ok(permissions), Ok(forms)) = (permissions, forms) else {
            return;
        };
        let ids = |list: &Value| -> Vec<String> {
            list["data"]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(|entry| entry["id"].as_str().map(str::to_owned))
                .collect()
        };
        let (permissions, forms) = (ids(&permissions), ids(&forms));
        if permissions.is_empty() && forms.is_empty() {
            return;
        }
        self.state.unsettled = true;
        for id in permissions {
            self.stop_request(&Native::Permission {
                id,
                action: String::new(),
                resources: Vec::new(),
                save: Vec::new(),
            })
            .await;
        }
        for id in forms {
            self.stop_request(&Native::Form {
                id,
                form: Value::Null,
            })
            .await;
        }
    }

    /// `stopStaleRequest`: reject a permission, cancel a form.
    async fn stop_request(&self, native: &Native) {
        let _ = match native {
            Native::Permission { id, .. } => {
                self.call(
                    reqwest::Method::POST,
                    &format!("/permission/{}/reply", opencode::encode(id)),
                    Some(&json!({"decision": "reject", "message": null})),
                    REQUEST_TIMEOUT,
                )
                .await
            }
            Native::Form { id, .. } => {
                self.call(
                    reqwest::Method::DELETE,
                    &format!("/form/{}", opencode::encode(id)),
                    None,
                    REQUEST_TIMEOUT,
                )
                .await
            }
        };
    }

    /// `writeRules`: the session's rules, sent only when changed.
    async fn write_rules(&mut self) -> Result<(), CallError> {
        let rules = self.shared.rules_for(&self.state, self.provider).await;
        if self.state.rules.as_ref() != Some(&rules) {
            self.call(
                reqwest::Method::PATCH,
                "",
                Some(&json!({"title": null, "metadata": null, "permissions": rules})),
                REQUEST_TIMEOUT,
            )
            .await?;
            self.state.rules = Some(rules);
        }
        Ok(())
    }

    /// `prepareTurn`: T3's MCP server for the thread (on a server of T3's
    /// own), then T3's instructions for the session when they changed.
    async fn prepare(&mut self, spec: &TurnSpec) -> Result<(), CallError> {
        let provider = self.provider;
        let name = mcp_server_name(&provider.app_thread_id);
        let wanted = provider
            .mcp
            .as_ref()
            .filter(|_| !self.shared.external)
            .map(|mcp| Mcp {
                name: name.clone(),
                directory: spec.cwd.clone(),
                credential: mcp.authorization.clone(),
            });
        if self.state.mcp.is_some() && self.state.mcp != wanted {
            let state = self.state.clone();
            self.shared.remove_mcp(&state).await;
            self.state.mcp = None;
        }
        let mut available = false;
        if let (Some(wanted), Some(mcp)) = (wanted, provider.mcp.as_ref()) {
            let fresh = self.state.mcp.is_none();
            let body = json!({"config": {"type": "remote", "url": mcp.url,
                "headers": {"Authorization": wanted.credential}, "oauth": false}});
            available = self
                .shared
                .call(
                    reqwest::Method::PUT,
                    &format!("/api/experimental/mcp/{}", opencode::encode(&name)),
                    Some(&wanted.directory),
                    Some(&body),
                    INVENTORY_TIMEOUT,
                )
                .await
                .is_ok();
            if available {
                self.state.mcp = Some(wanted);
                // Before 2.2.6 OpenCode loads a new server's tools 100 ms
                // after it connects; a prompt sent sooner lacks them.
                if fresh
                    && !crate::workspace::provider::version_at_least(
                        &self.shared.version,
                        (2, 2, 6),
                    )
                {
                    tokio::time::sleep(Duration::from_millis(250)).await;
                }
            }
        }
        let device = provider.mcp.as_ref().is_some_and(|mcp| mcp.device_tools);
        let instructions =
            crate::agent_instructions::opencode2_instructions(available, device, &provider.model);
        if self.state.instructions.as_deref() != Some(instructions.as_str()) {
            let path = format!(
                "/api/experimental/session/{}/instructions/entries/{INSTRUCTIONS_KEY}",
                opencode::encode(&self.state.session)
            );
            self.shared
                .call(
                    reqwest::Method::PUT,
                    &path,
                    None,
                    Some(&json!({"value": instructions})),
                    REQUEST_TIMEOUT,
                )
                .await
                .map(|_| ())?;
            self.state.instructions = Some(instructions);
        }
        Ok(())
    }

    fn report_thread(&self, resumed: bool) {
        self.sink.send(ProviderEvent::ProviderThread {
            driver: OPENCODE.into(),
            native_thread_id: self.state.session.clone(),
            resumed,
            model: None,
            opencode2: Some(Box::new(OpenCode2Thread {
                version: self.shared.version.clone(),
                model_selection: model_selection(
                    &self.provider.instance_id,
                    self.state.model.as_ref(),
                ),
            })),
            native_title: None,
            native_updated_at: None,
            context_usage: None,
            session_started: None,
        });
    }

    /// `beginTurn`: the provider turn, named by the prompt's id (weak).
    fn begin(&mut self) {
        self.sink.send(ProviderEvent::ProviderTurn {
            native_turn_id: Some(self.native_turn.clone()),
            native_turn_ref: Some(opencode::weak_ref(&self.prompt_id)),
            conversation_head: None,
            status: TurnStatus::Running,
            model: None,
            history_injected: false,
        });
    }

    async fn run(
        &mut self,
        spec: &TurnSpec,
        events: mpsc::UnboundedSender<Stream>,
        cancel: &mut Option<oneshot::Receiver<()>>,
    ) -> TurnEnd {
        if let Some(failure) = self.shared.dead() {
            return TurnEnd::failed(failure, ThreadDisposition::Broken);
        }
        let resumed = match self.ensure_thread(spec).await {
            Ok(resumed) => resumed,
            Err(failure) => return Self::fail(failure),
        };
        let provider = self.provider;
        let attempt = provider
            .native_turn
            .strip_prefix("attempt:")
            .unwrap_or(&provider.native_turn)
            .to_owned();
        self.native_turn = format!("{}:attempt:{attempt}", self.state.session);
        self.prompt_id = format!("msg_t3_turn_{}:{attempt}", self.state.session);
        self.shared
            .routes
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(self.state.session.clone(), events);
        self.report_thread(resumed);
        // A turn that ended locally may still run in OpenCode: a new turn
        // waits for its own execution's start, and fails while the previous
        // one still stops.
        if self.state.unsettled {
            let active = self
                .shared
                .call(
                    reqwest::Method::GET,
                    "/api/session/active",
                    None,
                    None,
                    INVENTORY_TIMEOUT,
                )
                .await;
            let still = active
                .as_ref()
                .is_ok_and(|active| active["data"].get(&self.state.session).is_some());
            if still {
                let _ = self
                    .call(reqwest::Method::POST, "/interrupt", None, INTERRUPT_TIMEOUT)
                    .await;
                self.begin();
                return self
                    .finish(
                        TurnStatus::Failed,
                        Some(ProviderFailure::new("provider_error", STILL_STOPPING)),
                    )
                    .await;
            }
            self.state.unsettled = false;
            self.awaiting_start = true;
        }
        let catalog = self.shared.catalog(&spec.cwd);
        let Some(model) = model_ref(
            &provider.model,
            provider.reasoning_effort.as_deref(),
            catalog.as_ref(),
        ) else {
            self.begin();
            let message = format!(
                "OpenCode model '{}' must use provider/model format",
                provider.model
            );
            return self
                .finish(
                    TurnStatus::Failed,
                    Some(ProviderFailure::new("validation_error", message)),
                )
                .await;
        };
        let setup = async {
            // The plan agent first, so the rules keep its own path allows.
            let agent = if provider.interaction_mode() == "plan" {
                "plan"
            } else {
                "build"
            };
            if agent != self.state.agent {
                self.call(
                    reqwest::Method::POST,
                    "/agent",
                    Some(&json!({"agent": agent})),
                    REQUEST_TIMEOUT,
                )
                .await?;
                self.state.agent = agent.to_owned();
            }
            self.write_rules().await?;
            let title = provider.thread_title.trim().to_owned();
            // An empty title would ask OpenCode to title the session.
            if !title.is_empty()
                && self.state.title.as_deref() != Some(title.as_str())
                && self
                    .call(
                        reqwest::Method::PATCH,
                        "",
                        Some(&json!({"title": title, "metadata": null, "permissions": null})),
                        REQUEST_TIMEOUT,
                    )
                    .await
                    .is_ok()
            {
                self.state.title = Some(title);
            }
            if !same_model(&model, self.state.model.as_ref()) {
                self.call(
                    reqwest::Method::POST,
                    "/model",
                    Some(&json!({"model": model})),
                    REQUEST_TIMEOUT,
                )
                .await?;
                self.state.model = Some(model.clone());
            }
            // Steers no execution took would run first.
            for inbox in std::mem::take(&mut self.state.stranded) {
                let path = format!("/inbox/{}", opencode::encode(&inbox));
                if self
                    .call(reqwest::Method::DELETE, &path, None, REQUEST_TIMEOUT)
                    .await
                    .is_err()
                {
                    self.state.stranded.push(inbox);
                }
            }
            self.prepare(spec).await
        };
        if let Err(error) = setup.await {
            return Self::fail(ProviderFailure::new("provider_error", error.message));
        }
        self.begin();
        let text = match provider.recovery_prompt.as_deref() {
            Some(recovery) if !resumed && provider.resume.is_some() => recovery,
            _ => spec.prompt.as_str(),
        };
        let mut body = json!({
            "id": self.prompt_id,
            "text": prompt_text(text, &provider.attachments),
            "delivery": null,
            "resume": null,
        });
        let files = prompt_files(&provider.attachments, self.shared.external);
        if !files.is_empty() {
            body["files"] = json!(files);
        }
        let sent = tokio::select! {
            sent = self.call(reqwest::Method::POST, "/prompt", Some(&body), REQUEST_TIMEOUT) => Some(sent),
            _ = cancelled(cancel) => None,
        };
        match sent {
            None => {
                // Stopped before OpenCode answered: nothing ran yet.
                self.interrupted = true;
                let _ = self
                    .call(reqwest::Method::POST, "/interrupt", None, INTERRUPT_TIMEOUT)
                    .await;
                self.state.unsettled = true;
                return self.finish(TurnStatus::Interrupted, None).await;
            }
            Some(Err(error)) if error.session_gone() => {
                self.state.session.clear();
                let end = self
                    .finish(
                        TurnStatus::Failed,
                        Some(ProviderFailure::new("provider_error", SESSION_GONE)),
                    )
                    .await;
                return TurnEnd::failed(
                    end.failure
                        .unwrap_or_else(|| ProviderFailure::new("provider_error", SESSION_GONE)),
                    ThreadDisposition::Broken,
                );
            }
            Some(Err(error)) => {
                if !error.clear_rejection() {
                    self.state.unsettled = true;
                }
                return self
                    .finish(
                        TurnStatus::Failed,
                        Some(ProviderFailure::new("provider_error", error.message)),
                    )
                    .await;
            }
            Some(Ok(_)) => self.unsent = false,
        }
        loop {
            let message = tokio::select! {
                message = self.events.recv() => message,
                _ = cancelled(cancel), if !self.interrupted => {
                    if let Some(end) = self.stop().await {
                        return end;
                    }
                    continue;
                }
                response = next_response(self.responses) => {
                    if let Some(end) = self.respond(response).await {
                        return end;
                    }
                    continue;
                }
                _ = super::sleep_until(self.stop_deadline) => {
                    self.state.unsettled = true;
                    return self.finish(TurnStatus::Interrupted, None).await;
                }
            };
            match message {
                Some(Stream::Event(event)) => {
                    if let Some(log) = &self.log {
                        log.event(&event);
                    }
                    if let Some(end) = self.handle(&event).await {
                        return end;
                    }
                }
                Some(Stream::Ended(failure)) => {
                    return self.finish_broken(failure).await;
                }
                None => {
                    let failure = self
                        .shared
                        .dead()
                        .unwrap_or_else(|| ProviderFailure::new("transport_error", STREAM_LOST));
                    return self.finish_broken(failure).await;
                }
            }
        }
    }

    async fn finish_broken(&mut self, failure: ProviderFailure) -> TurnEnd {
        let end = self.finish(TurnStatus::Failed, Some(failure.clone())).await;
        TurnEnd::failed(end.failure.unwrap_or(failure), ThreadDisposition::Broken)
    }

    /// One event of the turn's session (`route`, `onTurnEvent`).
    async fn handle(&mut self, event: &Value) -> Option<TurnEnd> {
        let kind = event["type"].as_str().unwrap_or_default();
        let data = &event["data"];
        match kind {
            "permission.asked" => return self.on_permission(data).await,
            "form.created" => return self.on_form(&data["form"]).await,
            "permission.replied" | "form.replied" | "form.cancelled" => {
                let native = if kind == "permission.replied" {
                    &data["requestID"]
                } else {
                    &data["id"]
                };
                let request = self
                    .pending
                    .iter()
                    .find(|(_, pending)| match pending {
                        Native::Permission { id, .. } | Native::Form { id, .. } => {
                            native.as_str() == Some(id.as_str())
                        }
                    })
                    .map(|(request, _)| request.clone());
                if let Some(request) = request {
                    self.pending.remove(&request);
                    let answered = kind == "form.replied"
                        || (kind == "permission.replied" && data["reply"] != "reject");
                    if !answered {
                        self.sink.send(ProviderEvent::RuntimeRequestClosed {
                            id: request,
                            status: "cancelled".into(),
                        });
                    }
                }
                return None;
            }
            "session.inbox.delivered" | "session.inbox.cancelled" => {
                if let Some(inbox) = data["inboxID"].as_str() {
                    if let Some(at) = self.steers.iter().position(|steer| steer == inbox) {
                        self.steers.remove(at);
                        self.settled_inbox.insert(inbox.to_owned());
                        if kind == "session.inbox.cancelled" {
                            return self.end_if_settled().await;
                        }
                    }
                }
                return None;
            }
            _ => {}
        }
        if self.awaiting_start {
            if kind == "session.execution.started" {
                self.awaiting_start = false;
            }
            return None;
        }
        match kind {
            "session.text.started"
            | "session.text.delta"
            | "session.text.ended"
            | "session.reasoning.started"
            | "session.reasoning.delta"
            | "session.reasoning.ended" => {
                let (item, block) = if kind.starts_with("session.text.") {
                    ("assistant_message", "text")
                } else {
                    ("reasoning", "reasoning")
                };
                let id = format!(
                    "{}:{block}:{}",
                    data["assistantMessageID"].as_str().unwrap_or_default(),
                    data["ordinal"]
                );
                let at = match self.texts.iter().position(|text| text.id == id) {
                    Some(at) => at,
                    None => {
                        self.texts.push(Text {
                            id: id.clone(),
                            kind: item,
                            text: String::new(),
                        });
                        self.texts.len() - 1
                    }
                };
                if let Some(delta) = data["delta"].as_str() {
                    self.texts[at].text.push_str(delta);
                }
                let completed = data.get("text").is_some();
                if let Some(text) = data["text"].as_str() {
                    self.texts[at].text = text.to_owned();
                }
                if completed {
                    let text = self.texts.remove(at);
                    emit_text(self.sink, &text, true);
                } else {
                    emit_text(self.sink, &self.texts[at], false);
                }
            }
            "session.tool.input.started" => {
                let name = data["name"].as_str().unwrap_or_default();
                // A question shows as its form; a subagent is not shown.
                if name == "question" || name == "subagent" {
                    return None;
                }
                let id = data["id"].as_str()?;
                self.tools.push(Tool {
                    id: id.to_owned(),
                    name: name.to_owned(),
                    input: json!({}),
                });
                self.emit_tool(id, "running", None, &Value::Null);
            }
            "session.tool.called" => {
                if let Some(id) = data["id"].as_str() {
                    if let Some(tool) = self.tools.iter_mut().find(|tool| tool.id == id) {
                        if data["input"].is_object() {
                            tool.input = data["input"].clone();
                        }
                        self.emit_tool(id, "running", None, &Value::Null);
                    }
                }
            }
            "session.tool.success" => {
                if let Some(id) = data["id"].as_str() {
                    let output = data["content"]
                        .as_array()
                        .into_iter()
                        .flatten()
                        .filter(|part| part["type"] == "text")
                        .filter_map(|part| part["text"].as_str().filter(|t| !t.is_empty()))
                        .collect::<Vec<_>>()
                        .join("\n");
                    self.emit_tool(id, "completed", Some(output), &data["metadata"]);
                    self.tools.retain(|tool| tool.id != id);
                }
            }
            "session.tool.failed" => {
                if let Some(id) = data["id"].as_str() {
                    let status = if data["error"]["type"] == "aborted" {
                        "interrupted"
                    } else {
                        "failed"
                    };
                    let message = data["error"]["message"].as_str().map(str::to_owned);
                    self.emit_tool(id, status, message, &Value::Null);
                    self.tools.retain(|tool| tool.id != id);
                }
            }
            "session.step.started" => {
                self.started_steps += 1;
                let model = format!(
                    "{}/{}",
                    data["model"]["providerID"].as_str().unwrap_or_default(),
                    data["model"]["id"].as_str().unwrap_or_default()
                );
                if !self.models.contains(&model) {
                    self.models.push(model);
                }
            }
            // A step without tokens (missed usage) is not measured.
            "session.step.ended" | "session.step.failed" if data["tokens"].is_object() => {
                let step = Step::of(&data["tokens"]);
                self.steps += 1;
                self.totals.input += step.input + step.read + step.write;
                self.totals.cached += step.read;
                self.totals.cache_write += step.write;
                self.totals.output += step.output + step.reasoning;
                self.totals.reasoning += step.reasoning;
                self.last_step = Some(step);
            }
            "session.execution.started" => self.held_end = None,
            "session.execution.succeeded" => {
                let status = if self.interrupted {
                    TurnStatus::Interrupted
                } else {
                    TurnStatus::Completed
                };
                return self.end_execution(status, None).await;
            }
            "session.execution.interrupted" => {
                return self.end_execution(TurnStatus::Interrupted, None).await;
            }
            "session.execution.failed" => {
                let failure = execution_failure(&data["error"]);
                return self.end_execution(TurnStatus::Failed, Some(failure)).await;
            }
            _ => {}
        }
        None
    }

    /// `endExecution`: a completed end waits for undelivered steers, whose
    /// execution then ends the turn.
    async fn end_execution(
        &mut self,
        status: TurnStatus,
        failure: Option<ProviderFailure>,
    ) -> Option<TurnEnd> {
        if status == TurnStatus::Completed && !self.steers.is_empty() {
            self.held_end = Some(status);
            return None;
        }
        Some(self.finish(status, failure).await)
    }

    async fn end_if_settled(&mut self) -> Option<TurnEnd> {
        match self.held_end {
            Some(status) if self.steers.is_empty() => Some(self.finish(status, None).await),
            _ => None,
        }
    }

    /// `emitTool`: a tool call's item, by its name (`openCodeToolTurnItem`).
    fn emit_tool(&self, id: &str, status: &str, output: Option<String>, metadata: &Value) {
        let Some(tool) = self.tools.iter().find(|tool| tool.id == id) else {
            return;
        };
        // A todo list shows once it is written.
        if tool.name == "todowrite" && status == "running" {
            return;
        }
        let mut state = json!({"input": tool.input});
        match status {
            "running" => state["status"] = json!("running"),
            "completed" => {
                state["status"] = json!("completed");
                state["output"] = json!(output.unwrap_or_default());
                state["metadata"] = metadata.clone();
            }
            _ => {
                state["status"] = json!("error");
                if let Some(output) = output {
                    state["error"] = json!(output);
                }
            }
        }
        let item = opencode::tool_item(id, &tool.name, &state, status == "interrupted");
        self.sink.send(ProviderEvent::TurnItem { item });
    }

    /// `onPermissionAsked`: an approval the user answers, or one a turn that
    /// waits for its execution's start refuses.
    async fn on_permission(&mut self, data: &Value) -> Option<TurnEnd> {
        let id = data["id"].as_str()?.to_owned();
        let action = data["action"].as_str().unwrap_or_default().to_owned();
        let strings = |value: &Value| -> Vec<String> {
            value
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(|v| v.as_str().map(str::to_owned))
                .collect()
        };
        let native = Native::Permission {
            id: id.clone(),
            action: action.clone(),
            resources: strings(&data["resources"]),
            save: strings(&data["save"]),
        };
        if self.awaiting_start {
            self.stop_request(&native).await;
            return None;
        }
        let Native::Permission {
            resources, save, ..
        } = &native
        else {
            return None;
        };
        let tool = data["source"]["id"]
            .as_str()
            .and_then(|call| self.tools.iter().find(|tool| tool.id == call))
            .map(|tool| tool.name.clone())
            .unwrap_or_default();
        let kind = opencode::request_kind(&action, &tool);
        let mut open = super::requests::open(&id, &id, kind, self.provider.runtime_mode());
        let mut options = vec![
            json!({"decision": "cancel", "label": "Cancel"}),
            json!({"decision": "decline", "label": "Decline"}),
        ];
        if !save.is_empty() {
            let label = if save.iter().all(|pattern| pattern == "*") {
                format!("Allow every {action} request this session")
            } else {
                format!("Allow {} this session", save.join(", "))
            };
            options.push(json!({"decision": "acceptForSession", "label": label}));
        }
        options.push(json!({"decision": "accept", "label": "Approve"}));
        let prompt = if resources.is_empty() {
            action.clone()
        } else {
            resources.join("\n")
        };
        open.fields.insert("title".into(), json!(action));
        open.fields.insert("prompt".into(), json!(prompt));
        open.fields.insert("options".into(), json!(options));
        self.pending.insert(open.id.clone(), native);
        self.sink
            .send(ProviderEvent::RuntimeRequestOpened { request: open });
        None
    }

    /// `onFormCreated`: the form's questions, or a turn that fails for a
    /// form T3 cannot show (declined first).
    async fn on_form(&mut self, form: &Value) -> Option<TurnEnd> {
        let id = form["id"].as_str()?.to_owned();
        let native = Native::Form {
            id: id.clone(),
            form: form.clone(),
        };
        if self.awaiting_start {
            self.stop_request(&native).await;
            return None;
        }
        match form_questions(form) {
            Ok(questions) => {
                let mut open =
                    super::requests::open(&id, &id, "user_input", self.provider.runtime_mode());
                open.fields.insert("title".into(), json!("User input"));
                open.fields.insert("questions".into(), json!(questions));
                self.pending.insert(open.id.clone(), native);
                self.sink
                    .send(ProviderEvent::RuntimeRequestOpened { request: open });
                None
            }
            Err(unsupported) => {
                self.stop_request(&native).await;
                self.state.unsettled = true;
                let message = format!(
                    "OpenCode asked for {unsupported}, which T3 Code can't show. The question was declined."
                );
                Some(
                    self.finish(
                        TurnStatus::Failed,
                        Some(ProviderFailure::new("provider_error", message)),
                    )
                    .await,
                )
            }
        }
    }

    /// `respondToRuntimeRequest` (or `steerTurn` for a message).
    async fn respond(&mut self, response: RuntimeResponse) -> Option<TurnEnd> {
        if let Some(text) = &response.steer {
            self.steer(&response.id, text).await;
            return None;
        }
        let native = self.pending.remove(&response.id)?;
        let decision = super::requests::decision(&response);
        let delivered = match &native {
            Native::Permission {
                id,
                action,
                resources,
                save,
            } => {
                if matches!(decision, "acceptForSession" | "acceptAlways") {
                    for resource in save {
                        let grant =
                            json!({"action": action, "resource": resource, "effect": "allow"});
                        if !self.state.grants.contains(&grant) {
                            self.state.grants.push(grant);
                        }
                    }
                    let _ = self.write_rules().await;
                }
                if decision == "decline" {
                    // Steered before the decline, or the model sees only
                    // that the command failed, and retries it.
                    let note = format!(
                        "The user declined the {action} request{}. Do not retry it; continue without it or ask the user how to proceed.",
                        if resources.is_empty() {
                            String::new()
                        } else {
                            format!(" ({})", resources.join(", "))
                        }
                    );
                    let _ = self
                        .call(
                            reqwest::Method::POST,
                            "/synthetic",
                            Some(&json!({"id": null, "text": note, "description": null, "delivery": "steer", "resume": false})),
                            REQUEST_TIMEOUT,
                        )
                        .await;
                }
                let approves = matches!(decision, "accept" | "acceptForSession" | "acceptAlways");
                let body = json!({
                    "decision": if approves { "once" } else { "reject" },
                    "message": if decision == "decline" { json!(DECLINED) } else { Value::Null },
                });
                let path = format!("/permission/{}/reply", opencode::encode(id));
                self.deliver(reqwest::Method::POST, &path, Some(&body))
                    .await
            }
            Native::Form { id, form } => {
                let answers = response.answers.clone();
                match answers.filter(|_| {
                    !matches!(decision, "decline" | "cancel") || response.decision.is_none()
                }) {
                    Some(answers) => {
                        let path = format!("/form/{}/reply", opencode::encode(id));
                        let body = json!({"answer": form_answer(form, &answers)});
                        self.deliver(reqwest::Method::POST, &path, Some(&body))
                            .await
                    }
                    None => {
                        let path = format!("/form/{}", opencode::encode(id));
                        self.deliver(reqwest::Method::DELETE, &path, None).await
                    }
                }
            }
        };
        if delivered {
            return None;
        }
        // OpenCode would wait forever on it: stop that run.
        self.state.unsettled = true;
        let end = self
            .finish(
                TurnStatus::Failed,
                Some(ProviderFailure::new("provider_error", UNANSWERED)),
            )
            .await;
        let _ = self
            .call(
                reqwest::Method::POST,
                "/interrupt",
                None,
                Duration::from_secs(2),
            )
            .await;
        Some(end)
    }

    /// `deliver`: an answer, retried once, within 10 s; a request OpenCode
    /// no longer has counts as answered.
    async fn deliver(&self, method: reqwest::Method, path: &str, body: Option<&Value>) -> bool {
        let attempt = async {
            for _ in 0..2 {
                match self.call(method.clone(), path, body, REQUEST_TIMEOUT).await {
                    Ok(_) => return true,
                    Err(error)
                        if matches!(
                            error.tag.as_deref(),
                            Some(
                                "PermissionNotFoundError"
                                    | "SessionNotFoundError"
                                    | "FormNotFoundError"
                                    | "FormAlreadySettledError"
                            )
                        ) =>
                    {
                        return true
                    }
                    Err(_) => {}
                }
            }
            false
        };
        tokio::time::timeout(REQUEST_TIMEOUT, attempt)
            .await
            .unwrap_or(false)
    }

    /// `steerTurn`: another prompt, delivered at OpenCode's next step.
    async fn steer(&mut self, message: &str, text: &str) {
        if self.interrupted {
            return;
        }
        let inbox = format!("msg_t3_steer_{}:{message}", self.state.session);
        if self.settled_inbox.contains(&inbox) {
            return;
        }
        self.steers.push(inbox.clone());
        self.state.stranded.retain(|known| known != &inbox);
        let body = json!({"id": inbox, "text": prompt_text(text, &[]), "delivery": "steer", "resume": null});
        if let Err(error) = self
            .call(
                reqwest::Method::POST,
                "/prompt",
                Some(&body),
                REQUEST_TIMEOUT,
            )
            .await
        {
            self.steers.retain(|known| known != &inbox);
            if !error.clear_rejection() {
                self.state.stranded.push(inbox);
            }
        }
    }

    /// Stop (`interruptTurn`): OpenCode interrupts the execution, whose end
    /// ends the turn.
    async fn stop(&mut self) -> Option<TurnEnd> {
        self.interrupted = true;
        let reply = self
            .call(reqwest::Method::POST, "/interrupt", None, INTERRUPT_TIMEOUT)
            .await;
        let Ok(reply) = reply else {
            // The turn stays open: its tools may still run.
            self.stop_deadline = Some(tokio::time::Instant::now() + STOP_SETTLE);
            return None;
        };
        if reply["interrupted"] == true {
            self.stop_deadline = Some(tokio::time::Instant::now() + STOP_SETTLE);
            return None;
        }
        if self.held_end.is_some() {
            self.state.unsettled = true;
            let end = self.finish(TurnStatus::Interrupted, None).await;
            self.state.stranded.append(&mut self.steers);
            return Some(end);
        }
        if self.unsent {
            return Some(self.finish(TurnStatus::Interrupted, None).await);
        }
        // Nothing was running to stop.
        self.interrupted = false;
        None
    }

    /// `finishTurn`: requests still waiting are cancelled, open text blocks
    /// and tools end, then the turn's usage and end.
    async fn finish(&mut self, status: TurnStatus, failure: Option<ProviderFailure>) -> TurnEnd {
        self.state.stranded.append(&mut self.steers);
        let mut waiting: Vec<String> = self.pending.drain().map(|(id, _)| id).collect();
        waiting.sort();
        for id in waiting {
            self.sink.send(ProviderEvent::RuntimeRequestClosed {
                id,
                status: "cancelled".into(),
            });
        }
        for text in std::mem::take(&mut self.texts) {
            emit_text(self.sink, &text, true);
        }
        let open: Vec<String> = self.tools.iter().map(|tool| tool.id.clone()).collect();
        for id in open {
            let settled = if status == TurnStatus::Completed {
                "completed"
            } else {
                "interrupted"
            };
            self.emit_tool(&id, settled, None, &Value::Null);
        }
        self.tools.clear();
        let window = self
            .shared
            .catalog(&self.state.directory)
            .and_then(|catalog| catalog.windows.get(self.provider.model.trim()).copied());
        let models = if self.models.is_empty() || self.steps > self.started_steps {
            None
        } else {
            Some(json!(self.models))
        };
        let mut turn_usage = if self.steps == 0 {
            json!({"usageScope": "main_agent", "usageStatus": "unavailable", "hasSubagents": false})
        } else {
            json!({
                "usageScope": "main_agent",
                "usageStatus": if status == TurnStatus::Completed && self.steps >= self.started_steps {
                    "complete"
                } else {
                    "partial"
                },
                "inputTokens": self.totals.input,
                "cachedInputTokens": self.totals.cached,
                "cacheCreationTokens": self.totals.cache_write,
                "outputTokens": self.totals.output,
                "reasoningTokens": self.totals.reasoning,
                "hasSubagents": false,
            })
        };
        if let Some(models) = models {
            turn_usage["models"] = models;
        }
        if let Some(step) = self.last_step {
            let input = step.input + step.read + step.write;
            self.sink.send(ProviderEvent::Usage {
                usage: TokenUsage {
                    used_tokens: input + step.output,
                    max_tokens: window,
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
            native_turn_ref: Some(opencode::weak_ref(&self.prompt_id)),
            conversation_head: None,
            status,
            model: None,
            history_injected: false,
        });
        // A cold server lists models lazily: read them again for the next
        // turn when the window was unknown.
        if window.is_none() && !self.state.directory.is_empty() {
            let shared = self.shared.clone();
            let directory = self.state.directory.clone();
            tokio::spawn(async move { shared.read_models(&directory).await });
        }
        match status {
            TurnStatus::Interrupted => TurnEnd::interrupted(true),
            TurnStatus::Failed => TurnEnd::failed(
                failure.unwrap_or_else(|| {
                    ProviderFailure::new("provider_error", "Provider turn failed.")
                }),
                ThreadDisposition::Reusable,
            ),
            _ => TurnEnd::completed(),
        }
    }
}

/// `emitText`: a text or reasoning block's whole text, once it has any.
fn emit_text(sink: &Sink, text: &Text, completed: bool) {
    if text.text.is_empty() {
        return;
    }
    let item = ProviderItem::new(
        &text.id,
        text.kind,
        if completed { "completed" } else { "running" },
    )
    .with("text", text.text.clone())
    .with("streaming", !completed);
    sink.send(ProviderEvent::TurnItem { item });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rules_end_with_the_threads_mcp_server() {
        let rules = session_rules("approval-required", false, &[], &[], "t3-code-thread_1");
        let rules = rules.as_array().unwrap();
        assert_eq!(rules[0], rule("shell", "ask"));
        assert_eq!(rules[1], rule("edit", "ask"));
        assert_eq!(
            rules.last().unwrap(),
            &json!({"action": "t3-code-thread_1_*", "resource": "*", "effect": "allow"})
        );
        let full = session_rules("full-access", false, &[], &[], "t3-code-x");
        assert_eq!(full[0], rule("*", "allow"));
        assert_eq!(full.as_array().unwrap().len(), 3);
    }

    #[test]
    fn long_thread_ids_are_hashed_into_the_mcp_name() {
        assert_eq!(mcp_server_name("thread:abc"), "t3-code-thread_abc");
        let long = mcp_server_name(&"x".repeat(80));
        assert!(long.starts_with("t3-code-") && long.len() == 24);
    }

    #[test]
    fn a_form_maps_to_questions_and_back() {
        let form = json!({"fields": [{"key": "q0", "title": "Color", "description": "Which?",
            "type": "string", "options": [{"value": "Red", "label": "Red"}], "custom": true}]});
        let questions = form_questions(&form).unwrap();
        assert_eq!(questions[0]["id"], "q0");
        assert_eq!(questions[0]["options"][0]["description"], "Red");
        assert_eq!(
            form_answer(&form, &json!({"q0": "Blue"})),
            json!({"q0": "Blue"})
        );
        let link = json!({"fields": [{"key": "a", "type": "external"}]});
        assert_eq!(form_questions(&link).unwrap_err(), "a link to open");
    }

    #[test]
    fn a_rate_limit_is_a_usage_limit() {
        let failure = execution_failure(&json!({"type": "provider.rate-limit",
            "message": "Rate limit reached.", "status": 429}));
        assert_eq!(failure.class, "usage_limit");
        assert_eq!(failure.code.as_deref(), Some("provider.rate-limit"));
        assert_eq!(
            execution_failure(&json!({"type": "provider.quota", "message": "", "status": 402}))
                .class,
            "provider_error"
        );
        assert!(
            crate::workspace::provider::version_at_least("2.2.6", (2, 2, 5))
                && !crate::workspace::provider::version_at_least("2.0.18", (2, 2, 5))
        );
    }
}
