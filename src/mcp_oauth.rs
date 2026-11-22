//! Public-client OAuth flow for the MCP resource, and the MCP transport.
//! Authorization codes live for one minute.
//!
//! Every grant is a document in the server's [`DocumentStore`]: the signing
//! key, pending authorization codes, issued tokens and MCP sessions. A
//! server that stops between a sign-in's requests, or between MCP calls,
//! picks them up again. Codes and tokens are stored under their SHA-256
//! hashes, never in the clear.
use crate::{
    access_http::{self, AuthHost},
    control_state::ControlState,
    persistence::{DocumentStore, WriteMode},
};
#[cfg(feature = "native")]
mod reference_sessions;
pub(crate) mod themes;
mod thread_credentials;
mod tools;
pub use thread_credentials::ThreadScope;
pub(crate) use tools::{answer_matches, card_command};
mod transport;
use axum::{
    extract::{rejection::JsonRejection, OriginalUri, Query, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Form, Json, Router,
};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD as B64, Engine};
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};
use url::Url;

const SCOPES: [&str; 2] = ["orchestration:read", "orchestration:operate"];
const ACCESS: [&str; 5] = [
    "read-only",
    "approval-required",
    "auto-accept-edits",
    "auto",
    "full-access",
];
const SECRET_HEADERS: [(&str, &str); 2] = [("cache-control", "no-store"), ("pragma", "no-cache")];
const UNKNOWN_CLIENT: &str =
    "This sign-in link names an unknown app. Start the sign-in again from it.";
/// The `software_id` another T3 Code server registers with to link to this one.
const PEER_LINK_SOFTWARE_ID: &str = "t3code-peer-link";
const CODE_TTL_MS: i64 = 60_000;
const TOKEN_TTL_SECS: i64 = 30 * 24 * 3600;
const SESSION_TTL_MS: i64 = 1_800_000;

/// The stored link to another environment, as forwarding checks it.
pub struct PeerLinkInfo {
    pub label: String,
    pub access: String,
    pub expired: bool,
}

/// What the MCP sign-in and transport need from their server.
/// Why a project mutation was refused, as the reference's `ProjectService`
/// errors tell it (`ProjectNotFoundError`, `ProjectConflictError`, ...).
#[derive(Debug)]
pub enum ProjectRefusal {
    NotFound,
    Conflict,
    NotEmpty,
    /// A rule the project would break, with the reference's words.
    Invalid(String),
    /// Anything else: the operation could not be completed.
    Operation,
}

/// Why a theme was not applied (`EnvironmentThemeRefusedError`,
/// `EnvironmentThemeNotSelectedError`).
#[derive(Debug)]
pub enum ThemeRefusal {
    /// The theme cannot be published or selected; the reference's sentence.
    Refused(String),
    /// The settings that select it could not be saved.
    NotSelected,
}

/// The published themes, the one asked for, and the themes directory.
pub type ThemesRead = (Vec<Value>, Option<Value>, String);

pub trait McpHost: AuthHost {
    fn oauth(&self) -> &OAuth;
    fn environment_id(&self) -> &str;
    /// Whether another T3 Code server may link to this one
    /// (`linkedEnvironmentsEnabled`).
    fn linked_environments_enabled(&self) -> bool;
    fn control_state(&self) -> &ControlState;
    /// A project row as MCP tools return it.
    fn project_row(&self, row: &Value) -> Value {
        row.clone()
    }
    /// The environment descriptor (`/.well-known/t3/environment`).
    fn descriptor(&self) -> Value;
    /// The effective settings.
    fn settings(&self) -> Value;
    /// The provider instances, as `server.getConfig` lists them.
    fn providers(&self) -> Value;
    /// Runs one orchestration command as `orchestration.dispatchCommand`
    /// does, returning the sequence it committed at or the failure.
    fn dispatch(self: &Arc<Self>, command: Value) -> HostFuture<Result<u64, Value>>;
    /// `cancelDelegatedTask`: cancels `taskId` on `parentThreadId` with
    /// `stop`, the child's `thread.stop`, after stopping the child's own
    /// delegated tasks.
    fn cancel_delegated_task(
        self: &Arc<Self>,
        parent_thread_id: String,
        task_id: String,
        stop: Value,
    ) -> HostFuture<Result<(), Value>>;
    /// Runs a provider RPC used by the MCP issue and pull-request toolkits.
    /// Native hosts provide this through the same provider service as the
    /// websocket RPC layer. Lightweight control-plane hosts may leave it
    /// unavailable.
    fn provider_dispatch(
        self: &Arc<Self>,
        _method: String,
        _params: Value,
    ) -> HostFuture<Option<Result<Value, Value>>> {
        Box::pin(async { None })
    }
    /// `projects.mutate` with a `project.create` command.
    fn mutate_project(self: &Arc<Self>, command: Value) -> HostFuture<Result<Value, Value>>;
    /// `projects.createNew`: a project started from its title, answering
    /// `{projectId, workspaceRoot, commitError?}` or the failure.
    fn create_named_project(self: &Arc<Self>, name: String) -> HostFuture<Result<Value, Value>>;
    fn sleep(&self, duration: std::time::Duration) -> crate::runtime::Wait;
    /// Runs `task` in the background, as the reference forks work into its
    /// layer's scope: it ends with the process.
    fn spawn(&self, task: crate::runtime::Task);
    /// `projects.mutate`, with why a refused mutation was refused, as the
    /// reference's `ProjectService` errors say.
    fn project_mutation(
        self: &Arc<Self>,
        command: Value,
    ) -> HostFuture<Result<Value, ProjectRefusal>> {
        let mutation = self.mutate_project(command);
        Box::pin(async move { mutation.await.map_err(|_| ProjectRefusal::Operation) })
    }
    /// Whether `project_id` is the built-in "No project" scratch project.
    fn is_scratch_project(self: &Arc<Self>, _project_id: &str) -> HostFuture<bool> {
        Box::pin(async { false })
    }
    /// Ensures the built-in "No project" scratch project exists and returns
    /// its project id.
    fn ensure_scratch_project(self: &Arc<Self>) -> HostFuture<Result<String, String>> {
        Box::pin(async {
            Err("Threads without a project are not available on this environment.".to_owned())
        })
    }
    /// Creates a fresh per-thread folder under the scratch root for
    /// `thread_id`, named from `text`. Returns `Ok(None)` when the server has
    /// no scratch root.
    fn scratch_thread_folder(
        self: &Arc<Self>,
        _thread_id: &str,
        _text: &str,
    ) -> HostFuture<Result<Option<String>, String>> {
        Box::pin(async { Ok(None) })
    }
    /// Applies a settings `patch` (`server.updateSettings`); `Err` when it
    /// cannot be written.
    fn update_settings(self: &Arc<Self>, _patch: Value) -> HostFuture<Result<(), ()>> {
        Box::pin(async { Err(()) })
    }
    /// `vcs.listRefs` for `input` (`query`, `cursor`, `limit`, `refKind`, ...)
    /// in a thread's `worktreePath` while it exists, else its project's
    /// `workspaceRoot`.
    fn list_refs(self: &Arc<Self>, _input: Value) -> HostFuture<Result<Value, ()>> {
        Box::pin(async { Err(()) })
    }
    /// Calls a workspace-backed RPC in the caller's workspace. MCP preview and
    /// device tools use the same operation names as the WebSocket API, which
    /// keeps local and agentd workspaces behaviorally identical.
    fn workspace_call(
        self: &Arc<Self>,
        _method: String,
        _input: Value,
    ) -> HostFuture<Result<Value, Value>> {
        Box::pin(async {
            Err(json!({"_tag":"Unavailable","message":"The workspace is unavailable."}))
        })
    }
    /// The published themes, the one named `theme_id`, and where theme files
    /// live (`EnvironmentThemeService.read`).
    fn theme_read(
        self: &Arc<Self>,
        _theme_id: Option<String>,
    ) -> HostFuture<Result<ThemesRead, ThemeRefusal>> {
        Box::pin(async { Err(ThemeRefusal::NotSelected) })
    }
    /// Selects a theme and appearance, first publishing `theme` as `theme_id`
    /// when given (`EnvironmentThemeService.apply`).
    fn theme_apply(
        self: &Arc<Self>,
        _theme_id: Option<String>,
        _theme: Option<Value>,
        _appearance: Option<String>,
    ) -> HostFuture<Result<(), ThemeRefusal>> {
        Box::pin(async { Err(ThemeRefusal::NotSelected) })
    }
    /// `sourceControl.cloneRepository`, or the failure's `detail`.
    fn clone_repository(self: &Arc<Self>, _input: Value) -> HostFuture<Result<Value, String>> {
        Box::pin(async { Err("The source control operation could not be completed.".to_owned()) })
    }
    /// Every link this server keeps, as `peerLinks.list` gives them.
    fn peer_links_listed(self: &Arc<Self>) -> HostFuture<Result<Vec<Value>, String>> {
        Box::pin(async { Ok(Vec::new()) })
    }
    /// Forgets the link to `environment_id`; whether there was one.
    fn unlink_peer(&self, _environment_id: &str) -> Result<bool, String> {
        Ok(false)
    }
    /// An agent's thread search over the shared store
    /// (`ThreadSearch.searchThreads`): matches, best first, or `Err` when the
    /// store cannot be searched.
    fn search_threads(
        &self,
        _query: String,
        _limit: usize,
        _project: Option<String>,
    ) -> HostFuture<Result<Vec<Value>, ()>> {
        Box::pin(async { Err(()) })
    }
    /// The stored link to `environment_id`, if this server keeps links.
    fn peer_link(&self, _environment_id: &str) -> Option<PeerLinkInfo> {
        None
    }
    /// Calls `tool` in the linked environment `environment_id`, limited to
    /// `limit` (`<runtime>/<interaction>`), sent for the thread `sender`
    /// (`T3-Sender-Thread`) when a thread asks. `None` when this server cannot
    /// reach linked environments.
    fn forward_tool(
        self: &Arc<Self>,
        _environment_id: String,
        _tool: String,
        _arguments: Value,
        _limit: String,
        _sender: Option<String>,
    ) -> Option<HostFuture<Result<Value, (&'static str, String)>>> {
        None
    }
}

/// A host's future. The WebAssembly build runs on one thread, so its
/// futures need not be `Send`.
#[cfg(feature = "native")]
pub type HostFuture<T> = futures_util::future::BoxFuture<'static, T>;
#[cfg(not(feature = "native"))]
pub type HostFuture<T> = futures_util::future::LocalBoxFuture<'static, T>;

/// Lets a router handler await a future that is not `Send`. Only the
/// single-threaded WebAssembly build uses it, where nothing crosses threads.
#[cfg(not(feature = "native"))]
struct OneThread<F>(F);
// SAFETY: the WebAssembly build has one thread, so the future is never sent.
#[cfg(not(feature = "native"))]
unsafe impl<F> Send for OneThread<F> {}
#[cfg(not(feature = "native"))]
impl<F: std::future::Future> std::future::Future for OneThread<F> {
    type Output = F::Output;
    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<F::Output> {
        // SAFETY: the inner future is never moved out of the pinned wrapper.
        unsafe { self.map_unchecked_mut(|s| &mut s.0) }.poll(cx)
    }
}
#[cfg(not(feature = "native"))]
pub(crate) fn on_this_thread<F: std::future::Future>(
    future: F,
) -> impl std::future::Future<Output = F::Output> + Send {
    OneThread(future)
}
#[cfg(feature = "native")]
pub(crate) fn on_this_thread<F: std::future::Future + Send>(future: F) -> F {
    future
}

pub struct OAuth {
    key: [u8; 32],
    documents: Arc<dyn DocumentStore>,
    dir: PathBuf,
    /// Serializes spending a code, so one code never yields two tokens.
    spend: Mutex<()>,
    /// The TypeScript server's session store in the same home, which holds
    /// the MCP client sessions (and so the peer links) it issued.
    #[cfg(feature = "native")]
    reference: reference_sessions::ReferenceSessions,
    /// The credentials of the MCP server each thread's provider CLI is given.
    threads: thread_credentials::ThreadCredentials,
}

#[derive(Deserialize, Serialize)]
struct Client {
    v: u8,
    n: String,
    r: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    p: Option<u8>,
}
#[derive(Deserialize, Default)]
struct Authorization {
    client_id: Option<String>,
    redirect_uri: Option<String>,
    response_type: Option<String>,
    code_challenge_method: Option<String>,
    code_challenge: Option<String>,
    state: Option<String>,
    resource: Option<String>,
}
#[derive(Clone, Deserialize, Serialize)]
struct Verified {
    name: String,
    id: String,
    #[serde(default)]
    peer_link: bool,
    #[serde(with = "url_text")]
    redirect: Url,
    challenge: String,
    state: Option<String>,
    issuer: String,
}
mod url_text {
    use serde::{de::Error, Deserialize, Deserializer, Serializer};
    use url::Url;
    pub fn serialize<S: Serializer>(url: &Url, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(url.as_str())
    }
    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Url, D::Error> {
        Url::parse(&String::deserialize(deserializer)?).map_err(D::Error::custom)
    }
}
/// A pending authorization code or an issued token.
#[derive(Deserialize, Serialize)]
struct Grant {
    request: Verified,
    access: String,
    expires_ms: i64,
}
/// An MCP session, held by one token.
#[derive(Deserialize, Serialize)]
struct Session {
    holder: String,
    expires_ms: i64,
}
/// What an MCP token grants.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct McpAccess {
    pub access: String,
    pub peer_link: bool,
    /// The client session, which scopes its retry keys (`client:<id>`).
    pub session_id: String,
    /// What the client is called, as Connections lists it.
    pub label: String,
    /// The thread a provider session's credential acts as; `None` for a client.
    pub thread: Option<ThreadScope>,
}
enum Invalid {
    Page(&'static str),
    Redirect(String),
}

fn random() -> [u8; 32] {
    crate::platform::random_bytes()
}
fn now_ms() -> i64 {
    crate::platform::now_ms()
}
fn hashed(secret: &str) -> String {
    hex::encode(Sha256::digest(secret.as_bytes()))
}

impl OAuth {
    /// Reads the signing key from `secrets/mcp-oauth-signing-key.bin` under
    /// `dir`, creating it on first use, and keeps grants under `dir`.
    pub fn open(documents: Arc<dyn DocumentStore>, dir: &Path) -> anyhow::Result<Self> {
        let secrets = dir.join("secrets");
        let path = secrets.join("mcp-oauth-signing-key.bin");
        if documents.read_optional(&path)?.is_none() {
            // Written as base64url text, which every document store holds.
            // A key written as its 32 raw bytes reads as well.
            documents.create_private_dir_all(&secrets)?;
            documents.write(&path, B64.encode(random()).as_bytes(), WriteMode::Private)?;
        }
        let stored = documents.read(&path)?;
        let key = match stored.len() {
            32 => stored.try_into().ok(),
            _ => B64
                .decode(stored.trim_ascii())
                .ok()
                .and_then(|bytes| bytes.try_into().ok()),
        }
        .ok_or_else(|| anyhow::anyhow!("invalid MCP OAuth signing key"))?;
        Ok(Self {
            key,
            documents,
            dir: dir.join("mcp-oauth"),
            spend: Mutex::new(()),
            #[cfg(feature = "native")]
            reference: reference_sessions::ReferenceSessions::new(dir),
            threads: Default::default(),
        })
    }
    /// agentd can keep a provider running while the control plane is asleep.
    pub(crate) fn restore_thread_credentials(&mut self) -> std::io::Result<()> {
        self.threads = thread_credentials::ThreadCredentials::open(
            self.documents.clone(),
            self.dir.join("thread-sessions"),
            self.key,
        )?;
        Ok(())
    }
    fn mac(&self, payload: &str) -> Hmac<Sha256> {
        let mut mac = Hmac::<Sha256>::new_from_slice(&self.key).expect("32-byte HMAC key");
        mac.update(payload.as_bytes());
        mac
    }
    fn client(&self, id: &str) -> Option<Client> {
        let (body, sig) = id.split_once('.')?;
        self.mac(&format!("mcp-client-id.{body}"))
            .verify_slice(&B64.decode(sig).ok()?)
            .ok()?;
        let client: Client = serde_json::from_slice(&B64.decode(body).ok()?).ok()?;
        (client.v == 1).then_some(client)
    }

    fn path(&self, kind: &str, name: &str) -> PathBuf {
        self.dir.join(kind).join(format!("{name}.json"))
    }
    fn read<T: serde::de::DeserializeOwned>(&self, kind: &str, name: &str) -> Option<T> {
        let bytes = self
            .documents
            .read_optional(&self.path(kind, name))
            .ok()??;
        serde_json::from_slice(&bytes).ok()
    }
    /// Stores `value`, first dropping this kind's records that expired.
    fn write<T: Serialize>(&self, kind: &str, name: &str, value: &T) -> std::io::Result<()> {
        let dir = self.dir.join(kind);
        let now = now_ms();
        for path in self.documents.list(&dir).unwrap_or_default() {
            let expired = self
                .documents
                .read_optional(&path)
                .ok()
                .flatten()
                .and_then(|bytes| serde_json::from_slice::<Value>(&bytes).ok())
                .is_none_or(|record| record["expires_ms"].as_i64().is_none_or(|at| at <= now));
            if expired {
                let _ = self.documents.remove(&path);
            }
        }
        self.documents.create_private_dir_all(&dir)?;
        self.documents.write(
            &self.path(kind, name),
            &serde_json::to_vec(value)?,
            WriteMode::Private,
        )
    }
    fn remove(&self, kind: &str, name: &str) {
        let _ = self.documents.remove(&self.path(kind, name));
    }

    /// Spends an authorization code: it is gone even when a later check fails.
    fn take_code(&self, code: &str) -> Option<Grant> {
        let _spending = self.spend.lock().unwrap_or_else(|e| e.into_inner());
        let name = hashed(code);
        let grant = self.read::<Grant>("codes", &name)?;
        self.remove("codes", &name);
        Some(grant)
    }

    fn validate(&self, req: Authorization, issuer: String) -> Result<Verified, Invalid> {
        let id = req.client_id.unwrap_or_default();
        let client = self.client(&id).ok_or(Invalid::Page(UNKNOWN_CLIENT))?;
        let redirect = req
            .redirect_uri
            .as_deref()
            .and_then(redirect_url)
            .filter(|r| {
                client
                    .r
                    .iter()
                    .filter_map(|u| redirect_url(u))
                    .any(|u| matches_redirect(&u, r))
            })
            .ok_or(Invalid::Page(
                "This sign-in link sends you to an address the app did not register.",
            ))?;
        let fail = |error, description: &str| {
            Invalid::Redirect(redirect_to(
                &redirect,
                &[
                    ("error", Some(error)),
                    ("error_description", Some(description)),
                    ("state", req.state.as_deref()),
                    ("iss", Some(&issuer)),
                ],
            ))
        };
        if req.response_type.as_deref() != Some("code") {
            return Err(fail(
                "unsupported_response_type",
                "Only response_type=code is supported.",
            ));
        }
        let challenge = req.code_challenge.unwrap_or_default();
        if req.code_challenge_method.as_deref() != Some("S256")
            || challenge.len() != 43
            || !challenge.bytes().all(base64_char)
        {
            return Err(fail(
                "invalid_request",
                "PKCE with code_challenge_method=S256 is required.",
            ));
        }
        if !resource_matches(req.resource.as_deref(), &issuer) {
            return Err(fail(
                "invalid_target",
                &format!("This server only issues tokens for {issuer}/mcp."),
            ));
        }
        Ok(Verified {
            name: client.n,
            id,
            peer_link: client.p == Some(1),
            redirect,
            challenge,
            state: req.state,
            issuer,
        })
    }
    /// The MCP transport alone may use this token; environment RPCs require an owner session.
    ///
    /// Besides this server's own tokens, it accepts the MCP client sessions the
    /// TypeScript server keeps in the same home, as that server checks them.
    pub fn mcp_access(&self, token: &str, issuer: &str) -> Option<McpAccess> {
        if let Some(scope) = self.threads.resolve(token, now_ms()) {
            // A thread acts within its own modes, which each call reads.
            return Some(McpAccess {
                access: "full-access".into(),
                peer_link: false,
                session_id: scope.provider_session_id.clone(),
                label: String::new(),
                thread: Some(scope),
            });
        }
        self.read::<Grant>("tokens", &hashed(token))
            .filter(|g| g.expires_ms > now_ms() && g.request.issuer == issuer)
            .map(|g| McpAccess {
                access: g.access,
                peer_link: g.request.peer_link,
                session_id: hashed(token)[..32].to_owned(),
                label: g.request.name,
                thread: None,
            })
            .or_else(|| self.reference_access(token))
    }
    #[cfg(feature = "native")]
    fn reference_access(&self, token: &str) -> Option<McpAccess> {
        self.reference
            .verify(token, now_ms())
            .unwrap_or_else(|error| {
                tracing::warn!(%error, "MCP client authentication failed.");
                None
            })
    }
    #[cfg(not(feature = "native"))]
    fn reference_access(&self, _token: &str) -> Option<McpAccess> {
        None
    }

    /// The bearer token `thread_id`'s provider CLI calls `/mcp` with.
    pub fn thread_credential(
        &self,
        thread_id: &str,
        provider_instance_id: &str,
    ) -> std::io::Result<String> {
        self.threads
            .for_thread(thread_id, provider_instance_id, now_ms())
    }
    pub(crate) fn provider_credential(
        &self,
        thread: &str,
        instance: &str,
        browser: bool,
    ) -> std::io::Result<String> {
        self.threads.issue(thread, instance, now_ms(), browser)
    }

    /// A turn of `thread_id` started, so its credential stays live.
    pub fn touch_thread_credential(&self, thread_id: &str) {
        if let Err(error) = self.threads.touch(thread_id, now_ms()) {
            tracing::error!(%error, "could not refresh MCP thread credential");
        }
    }
    /// Ends `thread_id`'s credential.
    pub fn revoke_thread_credential(&self, thread_id: &str) {
        if let Err(error) = self.threads.revoke_thread(thread_id) {
            tracing::error!(%error, "could not remove MCP thread credential");
        }
    }

    /// Opens an MCP session for the holder of `token` and returns its id.
    fn open_session(&self, token: &str) -> std::io::Result<String> {
        let id = crate::platform::uuid_v4().to_string();
        let session = Session {
            holder: hashed(token),
            expires_ms: now_ms() + SESSION_TTL_MS,
        };
        self.write("sessions", &id, &session)?;
        Ok(id)
    }
    /// Whether `id` is a live session of `token`'s holder.
    fn session_held(&self, id: &str, token: &str) -> bool {
        uuid::Uuid::parse_str(id).is_ok()
            && self
                .read::<Session>("sessions", id)
                .is_some_and(|s| s.expires_ms > now_ms() && s.holder == hashed(token))
    }
    fn close_session(&self, id: &str) {
        self.remove("sessions", id);
    }
}

fn base64_char(c: u8) -> bool {
    c.is_ascii_alphanumeric() || matches!(c, b'-' | b'_')
}
fn redirect_url(s: &str) -> Option<Url> {
    if s.len() > 512 || s.contains('#') {
        return None;
    }
    let u = Url::parse(s).ok()?;
    let allowed = u.scheme() == "https"
        || (u.scheme() == "http"
            && matches!(u.host_str(), Some("127.0.0.1" | "localhost" | "[::1]")));
    (allowed && u.username().is_empty() && u.password().is_none()).then_some(u)
}
fn matches_redirect(a: &Url, b: &Url) -> bool {
    a == b
        || (a.scheme() == "http"
            && b.scheme() == "http"
            && a.host() == b.host()
            && a.path() == b.path()
            && a.query() == b.query())
}
fn resource_matches(value: Option<&str>, issuer: &str) -> bool {
    value.is_none_or(|s| {
        Url::parse(s).is_ok_and(|u| {
            u.fragment().is_none()
                && u.origin().ascii_serialization() == issuer
                && u.path().trim_end_matches('/') == "/mcp"
        })
    })
}
fn origin(headers: &HeaderMap) -> String {
    let host = headers
        .get("host")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("localhost");
    let scheme = if headers
        .get("x-forwarded-proto")
        .is_some_and(|v| v == "https")
    {
        "https"
    } else {
        "http"
    };
    Url::parse(&format!("{scheme}://{host}"))
        .ok()
        .map(|u| u.origin().ascii_serialization())
        .unwrap_or_else(|| "http://localhost".into())
}
fn redirect_to(url: &Url, updates: &[(&str, Option<&str>)]) -> String {
    let mut pairs: Vec<(String, String)> = url.query_pairs().into_owned().collect();
    for (key, val) in updates {
        if let Some(value) = val {
            let mut replaced = false;
            pairs.retain_mut(|(k, v)| {
                if k != key {
                    return true;
                }
                if replaced {
                    return false;
                }
                *v = (*value).into();
                replaced = true;
                true
            });
            if !replaced {
                pairs.push(((*key).into(), (*value).into()))
            }
        }
    }
    let mut result = url.clone();
    result.set_query(None);
    result.query_pairs_mut().extend_pairs(pairs);
    result.into()
}
pub fn routes<S: McpHost>() -> Router<Arc<S>> {
    Router::new()
        .route("/.well-known/oauth-protected-resource", get(resource))
        .route("/mcp", axum::routing::any(transport::handle::<S>))
        .route("/.well-known/oauth-protected-resource/mcp", get(resource))
        .route("/.well-known/oauth-authorization-server", get(metadata))
        .route("/oauth/mcp/register", post(register::<S>))
        .route("/oauth/mcp/authorize", get(authorize::<S>))
        .route("/oauth/mcp/approval", post(approval::<S>))
        .route("/oauth/mcp/decision", post(decision::<S>))
        .route("/oauth/mcp/token", post(token::<S>))
}
async fn resource(h: HeaderMap) -> Json<Value> {
    let i = origin(&h);
    Json(
        json!({"resource":format!("{i}/mcp"),"authorization_servers":[i],"scopes_supported":SCOPES,"bearer_methods_supported":["header"],"resource_name":"T3 Code"}),
    )
}
async fn metadata(h: HeaderMap) -> Json<Value> {
    let i = origin(&h);
    Json(
        json!({"issuer":i,"authorization_endpoint":format!("{i}/oauth/mcp/authorize"),"token_endpoint":format!("{i}/oauth/mcp/token"),"registration_endpoint":format!("{i}/oauth/mcp/register"),"response_types_supported":["code"],"grant_types_supported":["authorization_code"],"code_challenge_methods_supported":["S256"],"token_endpoint_auth_methods_supported":["none"],"scopes_supported":SCOPES,"authorization_response_iss_parameter_supported":true}),
    )
}
fn error(code: &str, desc: &str) -> Response {
    (
        StatusCode::BAD_REQUEST,
        Json(json!({"error":code,"error_description":desc})),
    )
        .into_response()
}
async fn register<S: McpHost>(State(s): State<Arc<S>>, Json(input): Json<Value>) -> Response {
    let uris = input["redirect_uris"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    if !(1..=10).contains(&uris.len()) {
        return error(
            "invalid_redirect_uri",
            "Register between 1 and 10 redirect URIs.",
        );
    }
    if uris
        .iter()
        .any(|u| u.as_str().and_then(redirect_url).is_none())
    {
        return error(
            "invalid_redirect_uri",
            "Redirect URIs must be https, or http on localhost, 127.0.0.1 or [::1].",
        );
    }
    let peer_link = input["software_id"] == PEER_LINK_SOFTWARE_ID;
    if peer_link && !s.linked_environments_enabled() {
        return error(
            "invalid_client_metadata",
            "Linked environments is off on that machine. Turn it on in its Settings → Labs.",
        );
    }
    let name = input["client_name"]
        .as_str()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .unwrap_or("MCP client")
        .chars()
        .take(100)
        .collect::<String>();
    let mut client = json!({"v":1,"n":name,"r":uris});
    if peer_link {
        client["p"] = json!(1);
    }
    let body = B64.encode(client.to_string());
    let sig = B64.encode(
        s.oauth()
            .mac(&format!("mcp-client-id.{body}"))
            .finalize()
            .into_bytes(),
    );
    let id = format!("{body}.{sig}");
    if uris.len() > 5 && id.len() > 10240 {
        return error(
            "invalid_redirect_uri",
            "Redirect URIs are too long to fit in an authorization request.",
        );
    }
    (StatusCode::CREATED,SECRET_HEADERS,Json(json!({"client_id":id,"client_name":name,"redirect_uris":uris,"grant_types":["authorization_code"],"response_types":["code"],"token_endpoint_auth_method":"none"}))).into_response()
}
async fn authorize<S: McpHost>(
    State(s): State<Arc<S>>,
    h: HeaderMap,
    OriginalUri(uri): OriginalUri,
    Query(req): Query<Authorization>,
) -> Response {
    let location = match s.oauth().validate(req, origin(&h)) {
        Ok(_) => format!(
            "/connect-agent{}",
            uri.query().map(|s| format!("?{s}")).unwrap_or_default()
        ),
        Err(Invalid::Redirect(r)) => r,
        Err(Invalid::Page(desc)) => {
            return (
                StatusCode::BAD_REQUEST,
                [
                    ("content-type", "text/html; charset=utf-8"),
                    (
                        "content-security-policy",
                        "default-src 'none'; style-src 'unsafe-inline'; frame-ancestors 'none'",
                    ),
                    ("x-frame-options", "DENY"),
                    ("cache-control", "no-store"),
                    ("referrer-policy", "no-referrer"),
                    ("x-content-type-options", "nosniff"),
                ],
                include_str!("oauth_error.html").replace("{{description}}", desc),
            )
                .into_response()
        }
    };
    (
        StatusCode::FOUND,
        [
            ("location", location.as_str()),
            ("cache-control", "no-store"),
            ("referrer-policy", "no-referrer"),
        ],
    )
        .into_response()
}
fn approval_error(message: &str) -> Response {
    (
        StatusCode::BAD_REQUEST,
        Json(json!({"_tag":"AuthMcpApprovalError","message":message})),
    )
        .into_response()
}
fn invalid(e: Invalid) -> Response {
    match e {
        Invalid::Page(s) => approval_error(s),
        Invalid::Redirect(r) => Json(json!({"redirectTo":r})).into_response(),
    }
}
async fn csrf<S: McpHost>(s: &S, h: &HeaderMap, v: &Verified) -> Option<Hmac<Sha256>> {
    access_http::authenticate_owner(s, h).await.ok()?;
    let holder = access_http::principal(s, h).ok()?.id;
    Some(s.oauth().mac(&format!(
        "mcp-csrf.{}",
        json!([holder, v.id, v.redirect.as_str(), v.challenge])
    )))
}
async fn approval<S: McpHost>(
    State(s): State<Arc<S>>,
    h: HeaderMap,
    req: Result<Json<Authorization>, JsonRejection>,
) -> Response {
    let Json(req) = match req {
        Ok(req) => req,
        Err(_) => return StatusCode::BAD_REQUEST.into_response(),
    };
    let v = match s.oauth().validate(req, origin(&h)) {
        Ok(v) => v,
        Err(e) => return invalid(e),
    };
    let mut out = json!({"clientName":v.name,"redirectHost":v.redirect[url::Position::BeforeHost..url::Position::AfterPort],"environmentHost":Url::parse(&v.issuer).unwrap()[url::Position::BeforeHost..url::Position::AfterPort]});
    if let Some(mac) = csrf(&*s, &h, &v).await {
        out["csrfToken"] = json!(B64.encode(mac.finalize().into_bytes()));
        out["oneClickAccess"] = json!(ACCESS);
    }
    (SECRET_HEADERS, Json(out)).into_response()
}
#[derive(Deserialize)]
struct Decision {
    authorization: Authorization,
    decision: Value,
}
async fn decision<S: McpHost>(
    State(s): State<Arc<S>>,
    h: HeaderMap,
    req: Result<Json<Decision>, JsonRejection>,
) -> Response {
    let Json(req) = match req {
        Ok(req) => req,
        Err(_) => return StatusCode::BAD_REQUEST.into_response(),
    };
    let v = match s.oauth().validate(req.authorization, origin(&h)) {
        Ok(v) => v,
        Err(e) => return invalid(e),
    };
    let target = match req.decision["_tag"].as_str() {
        Some("deny") => redirect_to(
            &v.redirect,
            &[
                ("error", Some("access_denied")),
                ("state", v.state.as_deref()),
                ("iss", Some(&v.issuer)),
            ],
        ),
        Some(mode @ ("browser-session" | "pairing-code")) => {
            let access = req.decision["access"].as_str().unwrap_or("");
            if mode == "pairing-code" {
                let supplied = req.decision["code"].as_str().unwrap_or("");
                if v.peer_link && approval_code(supplied) {
                    return approval_error(
                        "Linking needs a plain pairing code, not an app approval code.",
                    );
                }
                if let Err(reason) = s
                    .auth()
                    .consume_approval_code(supplied, access, v.peer_link)
                {
                    return approval_error(match reason {
                        "insufficient_scope"=>"That pairing code cannot grant this access. It has not been used, so choose an access level it grants, or create a code with the standard scopes.",
                        "not_a_pairing_code"=>"That is not a one-time pairing code.",
                        "unknown_or_used"=>"That pairing code is unknown, expired, or already used.",
                        _=>"The code could not be checked. Try again.",
                    });
                }
            } else {
                let tag = B64
                    .decode(req.decision["csrfToken"].as_str().unwrap_or(""))
                    .unwrap_or_default();
                if !ACCESS.contains(&access)
                    || csrf(&*s, &h, &v)
                        .await
                        .is_none_or(|mac| mac.verify_slice(&tag).is_err())
                {
                    return approval_error(
                        "Your session cannot approve this request. Enter a pairing code instead.",
                    );
                }
            }
            let code = B64.encode(random());
            let target = redirect_to(
                &v.redirect,
                &[
                    ("code", Some(&code)),
                    ("state", v.state.as_deref()),
                    ("iss", Some(&v.issuer)),
                ],
            );
            let grant = Grant {
                request: v,
                access: access.into(),
                expires_ms: now_ms() + CODE_TTL_MS,
            };
            if s.oauth().write("codes", &hashed(&code), &grant).is_err() {
                return approval_error("The code could not be checked. Try again.");
            }
            target
        }
        _ => {
            return approval_error(
                "Your session cannot approve this request. Enter a pairing code instead.",
            )
        }
    };
    (SECRET_HEADERS, Json(json!({"redirectTo":target}))).into_response()
}
async fn token<S: McpHost>(
    State(s): State<Arc<S>>,
    h: HeaderMap,
    Form(form): Form<HashMap<String, String>>,
) -> Response {
    let get = |k: &str| form.get(k).map(String::as_str).unwrap_or_default();
    let fail = |code: &str, msg: &str| (SECRET_HEADERS, error(code, msg)).into_response();
    if get("grant_type") != "authorization_code" {
        return fail(
            "unsupported_grant_type",
            "Only authorization_code is supported.",
        );
    }
    let (code, id, redirect, verifier) = (
        get("code"),
        get("client_id"),
        get("redirect_uri"),
        get("code_verifier"),
    );
    if [code, id, redirect, verifier].contains(&"") {
        return fail(
            "invalid_request",
            "code, redirect_uri, client_id and code_verifier are required.",
        );
    }
    if s.oauth().client(id).is_none() {
        return fail("invalid_client", "The client is unknown.");
    }
    let grant = s.oauth().take_code(code);
    let issuer = origin(&h);
    let Some(mut grant) = grant.filter(|g| {
        g.expires_ms > now_ms()
            && g.request.id == id
            && g.request.redirect.as_str() == redirect
            && g.request.issuer == issuer
            && resource_matches(form.get("resource").map(String::as_str), &issuer)
            && (43..=128).contains(&verifier.len())
            && verifier
                .bytes()
                .all(|c| base64_char(c) || matches!(c, b'.' | b'~'))
            && g.request.challenge == B64.encode(Sha256::digest(verifier.as_bytes()))
    }) else {
        return fail(
            "invalid_grant",
            "The authorization code is invalid or expired.",
        );
    };
    let scope = match grant.access.as_str() {
        "full-access" => {
            "orchestration:read orchestration:operate settings:write source-control:write"
        }
        "read-only" => "orchestration:read",
        _ => "orchestration:read orchestration:operate",
    };
    // On a home the TypeScript server shares, the session goes where that
    // server keeps its own, so a link made here works there and is revoked
    // there like any other client.
    #[cfg(feature = "native")]
    match s.oauth().reference.issue(
        &grant.request.name,
        &grant.access,
        grant.request.peer_link,
        &access_http::client_metadata(),
        now_ms(),
    ) {
        Ok(Some(issued)) => {
            let expires_in = ((issued.expires_ms - now_ms()) / 1000).max(0);
            return (
                SECRET_HEADERS,
                Json(json!({"access_token":issued.token,"token_type":"Bearer","expires_in":expires_in,"scope":scope})),
            )
                .into_response();
        }
        Ok(None) => {}
        Err(error) => {
            tracing::error!(%error, "Could not issue an MCP client session.");
            return fail("invalid_grant", "The session could not be issued.");
        }
    }
    let token = B64.encode(random());
    grant.expires_ms = now_ms() + TOKEN_TTL_SECS * 1000;
    if s.oauth().write("tokens", &hashed(&token), &grant).is_err() {
        return fail("invalid_grant", "The session could not be issued.");
    }
    (
        SECRET_HEADERS,
        Json(json!({"access_token":token,"token_type":"Bearer","expires_in":TOKEN_TTL_SECS,"scope":scope})),
    )
        .into_response()
}

/// An app approval code (`READ-…`, `FULL-…`), as opposed to a plain pairing code.
fn approval_code(code: &str) -> bool {
    code.trim().split_once('-').is_some_and(|(prefix, rest)| {
        ["READ", "SUPERVISED", "EDITS", "AUTO", "FULL"]
            .iter()
            .any(|p| prefix.eq_ignore_ascii_case(p))
            && rest.len() == 12
            && rest.bytes().all(|c| c.is_ascii_alphanumeric())
    })
}

#[cfg(all(test, feature = "native"))]
mod tests {
    use super::*;
    use crate::AppState;
    use axum::{
        body::{to_bytes, Body},
        http::Request,
    };
    use tower::ServiceExt;

    #[tokio::test]
    async fn auth_code_requires_session_csrf_and_pkce_and_is_spent_once() {
        let dir = tempfile::tempdir().unwrap();
        let auth = crate::auth_service::AuthService::with_bootstrap("fixture-owner");
        let state = AppState::with_auth(dir.path(), vec![], auth).await.unwrap();
        let app = crate::router(state.clone());
        let call = |path: &str, body: Value, cookie: &str| {
            let mut req = Request::post(path)
                .header("host", "localhost")
                .header("content-type", "application/json");
            if !cookie.is_empty() {
                req = req.header("cookie", cookie)
            }
            app.clone()
                .oneshot(req.body(Body::from(body.to_string())).unwrap())
        };
        let body = async |response: Response| {
            serde_json::from_slice::<Value>(&to_bytes(response.into_body(), 1 << 20).await.unwrap())
                .unwrap()
        };
        let session = call(
            "/api/auth/browser-session",
            json!({"credential":"fixture-owner"}),
            "",
        )
        .await
        .unwrap();
        let cookie = session.headers()["set-cookie"]
            .to_str()
            .unwrap()
            .split(';')
            .next()
            .unwrap()
            .to_owned();
        let client = body(
            call(
                "/oauth/mcp/register",
                json!({"redirect_uris":["http://127.0.0.1:1212/cb"]}),
                "",
            )
            .await
            .unwrap(),
        )
        .await;
        let verifier = "v".repeat(64);
        let authorize = json!({"client_id":client["client_id"],"redirect_uri":"http://127.0.0.1:1313/cb","response_type":"code","state":"abc","code_challenge_method":"S256","code_challenge":B64.encode(Sha256::digest(verifier.as_bytes()))});
        let approved = body(
            call("/oauth/mcp/approval", authorize.clone(), &cookie)
                .await
                .unwrap(),
        )
        .await;
        let decision = json!({"authorization":authorize,"decision":{"_tag":"browser-session","access":"read-only","csrfToken":approved["csrfToken"]}});
        assert_eq!(
            call("/oauth/mcp/decision", decision.clone(), "")
                .await
                .unwrap()
                .status(),
            400
        );
        let redir = body(
            call("/oauth/mcp/decision", decision, &cookie)
                .await
                .unwrap(),
        )
        .await;
        let redir = Url::parse(redir["redirectTo"].as_str().unwrap()).unwrap();
        let code = redir.query_pairs().find(|(k, _)| k == "code").unwrap().1;
        let exchange = || {
            let form = url::form_urlencoded::Serializer::new(String::new())
                .extend_pairs([
                    ("client_id", client["client_id"].as_str().unwrap()),
                    ("code", code.as_ref()),
                    ("redirect_uri", "http://127.0.0.1:1313/cb"),
                    ("grant_type", "authorization_code"),
                    ("code_verifier", &verifier),
                ])
                .finish();
            app.clone().oneshot(
                Request::post("/oauth/mcp/token")
                    .header("host", "localhost")
                    .header("content-type", "application/x-www-form-urlencoded")
                    .body(Body::from(form))
                    .unwrap(),
            )
        };
        let token = body(exchange().await.unwrap()).await;
        assert_eq!(token["scope"], "orchestration:read");
        assert_eq!(
            state
                .oauth
                .mcp_access(token["access_token"].as_str().unwrap(), "http://localhost")
                .map(|granted| granted.access),
            Some("read-only".to_owned())
        );
        assert_eq!(
            body(exchange().await.unwrap()).await["error"],
            "invalid_grant"
        );
        let env_response = app
            .oneshot(
                Request::get("/api/projects")
                    .header(
                        "authorization",
                        format!("Bearer {}", token["access_token"].as_str().unwrap()),
                    )
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(env_response.status(), 401);
    }
}
