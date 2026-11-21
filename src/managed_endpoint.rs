//! The T3 Connect managed tunnel: runs cloudflared for the endpoint T3 Connect provisioned.
//!
//! Ports `CloudManagedEndpointRuntime` (`apps/server/src/cloud/ManagedEndpointRuntime.ts`), the
//! startup half of `CloudLink.startManagedTunnelIfOriginConfirmed` and the server wiring in
//! `server.ts`, and `RelayClientLedger`, from the pinned reference.
//!
//! The tunnel's credentials are the reference's: `userdata/secrets/cloud-endpoint-runtime-config.bin`
//! (`{providerKind, connectorToken, tunnelId?, tunnelName?}`), confirmed for this server's local
//! origin by `cloud-endpoint-confirmed-origin.bin`. A server that takes over a T3 home from the
//! TypeScript server therefore runs the same tunnel. The connector is spawned exactly as the
//! reference spawns it, supervised with the same crash-loop backoff, logged to
//! `userdata/logs/relay-client.ndjson` in the reference's format, recorded in
//! `userdata/relay-clients/<pid>.json` so a crashed server's connector is stopped by the next
//! one, and stopped when the server stops.
//!
//! The T3 Connect calls around it (registration, recovery, provisioning a CLI-desired link and
//! releasing the tunnel on shutdown) are in `crate::cloud_link`, which consumes this runtime's
//! recovery requests. Not ported: installing a missing cloudflared.

use crate::relay_client;
use serde_json::{json, Map, Value};
use std::{
    ffi::OsString,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering},
        Arc, Mutex as StdMutex,
    },
    time::{Duration, Instant},
};
use tokio::{
    io::{AsyncBufReadExt, AsyncRead, BufReader},
    process::Child,
    sync::{oneshot, Mutex, Notify},
    task::{AbortHandle, JoinHandle},
};

pub const RELAY_CLIENT_LOG_FILE: &str = "relay-client.ndjson";
const CLOUD_ENDPOINT_RUNTIME_CONFIG: &str = "cloud-endpoint-runtime-config";
const CLOUD_ENDPOINT_CONFIRMED_ORIGIN: &str = "cloud-endpoint-confirmed-origin";
const HTTP2_FALLBACK_STATE: &str = "cloud-endpoint-http2-fallback";
const HTTP2_FALLBACK_TTL_MS: i128 = 24 * 60 * 60_000;
const TUNNEL_AUTHORIZATION_FAILURES_BEFORE_RECOVERY: u32 = 4;
const QUIC_STALLS_BEFORE_HTTP2: usize = 30;
const QUIC_STALL_WINDOW: Duration = Duration::from_secs(5 * 60);
const CONNECTOR_NOT_READY_CHECKS_BEFORE_RESTART: u32 = 6;
const CONNECTOR_NOT_READY_CHECKS_MAX: u32 = 60;
const LOG_MAX_BYTES: u64 = 1024 * 1024;
const LOG_MAX_FILES: u32 = 2;

/// Delays the reference hard-codes; tests shorten them.
#[derive(Clone, Debug)]
pub struct Timings {
    pub stable_uptime: Duration,
    pub backoff_base: Duration,
    pub backoff_max: Duration,
    pub registration_timeout: Duration,
    pub readiness_interval: Duration,
    pub readiness_timeout: Duration,
    /// How long a stopping connector may take after SIGTERM before it is killed. The reference
    /// waits without a limit; a bound keeps a wedged connector from holding up shutdown.
    pub stop_grace: Duration,
}

impl Default for Timings {
    fn default() -> Self {
        Self {
            stable_uptime: Duration::from_secs(30),
            backoff_base: Duration::from_secs(1),
            backoff_max: Duration::from_secs(60),
            registration_timeout: Duration::from_secs(3 * 60),
            readiness_interval: Duration::from_secs(30),
            readiness_timeout: Duration::from_secs(5),
            stop_grace: Duration::from_secs(10),
        }
    }
}

/// `RelayManagedEndpointRuntimeConfig`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RuntimeConfig {
    pub provider_kind: String,
    pub connector_token: String,
    pub tunnel_id: Option<String>,
    pub tunnel_name: Option<String>,
}

impl RuntimeConfig {
    pub fn decode(text: &str) -> Option<Self> {
        Self::from_value(&serde_json::from_str(text).ok()?)
    }

    pub(crate) fn from_value(value: &Value) -> Option<Self> {
        let object = value.as_object()?;
        let provider_kind = object.get("providerKind")?.as_str()?;
        if !matches!(provider_kind, "manual" | "cloudflare_tunnel" | "t3_relay") {
            return None;
        }
        // TrimmedNonEmptyString: trimmed on decode, then non-blank.
        let text = |key: &str| -> Result<Option<String>, ()> {
            match object.get(key) {
                None => Ok(None),
                Some(Value::String(value)) if !value.trim().is_empty() => {
                    Ok(Some(value.trim().to_owned()))
                }
                Some(_) => Err(()),
            }
        };
        Some(Self {
            provider_kind: provider_kind.to_owned(),
            connector_token: text("connectorToken").ok()??,
            tunnel_id: text("tunnelId").ok()?,
            tunnel_name: text("tunnelName").ok()?,
        })
    }

    /// `encodeEndpointRuntimeConfigJson`: the schema's fields, in its order.
    pub fn encode(&self) -> String {
        self.to_value().to_string()
    }

    pub(crate) fn to_value(&self) -> Value {
        let mut map = Map::new();
        map.insert("providerKind".into(), json!(self.provider_kind));
        map.insert("connectorToken".into(), json!(self.connector_token));
        self.add_tunnel(&mut map);
        Value::Object(map)
    }

    /// `runtimeConfigKey`.
    fn key(&self) -> String {
        json!({
            "providerKind": self.provider_kind,
            "connectorToken": self.connector_token,
            "tunnelId": self.tunnel_id,
            "tunnelName": self.tunnel_name,
        })
        .to_string()
    }

    fn add_tunnel(&self, map: &mut Map<String, Value>) {
        if let Some(id) = &self.tunnel_id {
            map.insert("tunnelId".into(), json!(id));
        }
        if let Some(name) = &self.tunnel_name {
            map.insert("tunnelName".into(), json!(name));
        }
    }

    fn with_tunnel(&self, value: Value) -> Value {
        let mut value = value;
        if let Value::Object(map) = &mut value {
            self.add_tunnel(map);
        }
        value
    }
}

/// The server's own origin, as `parseManagedEndpointLocalOrigin` reads it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LocalOrigin {
    pub host: String,
    pub port: u16,
}

impl LocalOrigin {
    /// `http://${loopbackUrlHostForAddress(address)}:${port}`.
    pub fn for_address(address: std::net::SocketAddr) -> Self {
        let host = match address.ip() {
            ip if ip.is_unspecified() => "127.0.0.1".to_owned(),
            std::net::IpAddr::V4(ip) => ip.to_string(),
            std::net::IpAddr::V6(ip) => format!("[{ip}]"),
        };
        Self {
            host,
            port: address.port(),
        }
    }
}

pub(crate) fn read_secret(dir: &Path, name: &str) -> Option<Vec<u8>> {
    std::fs::read(dir.join(format!("{name}.bin"))).ok()
}

pub(crate) fn read_secret_string(dir: &Path, name: &str) -> Option<String> {
    read_secret(dir, name).map(|bytes| String::from_utf8_lossy(&bytes).into_owned())
}

/// `ServerSecretStore.set`: a private temporary file renamed over the secret.
pub(crate) fn write_secret(dir: &Path, name: &str, bytes: &[u8]) -> std::io::Result<()> {
    std::fs::create_dir_all(dir)?;
    let target = dir.join(format!("{name}.bin"));
    let temporary = dir.join(format!("{name}.bin.{}.tmp", crate::platform::uuid_v4()));
    let result = (|| {
        std::fs::write(&temporary, bytes)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&temporary, std::fs::Permissions::from_mode(0o600))?;
        }
        std::fs::rename(&temporary, &target)
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(&temporary);
    }
    result
}

/// `ServerSecretStore.remove`: a missing secret is already removed.
pub(crate) fn remove_secret(dir: &Path, name: &str) -> std::io::Result<()> {
    match std::fs::remove_file(dir.join(format!("{name}.bin"))) {
        Err(error) if error.kind() != std::io::ErrorKind::NotFound => Err(error),
        _ => Ok(()),
    }
}

fn now_millis() -> i128 {
    i128::from(crate::platform::now_ms())
}

fn iso_now() -> String {
    time::OffsetDateTime::from_unix_timestamp_nanos(now_millis() * 1_000_000)
        .unwrap_or(time::OffsetDateTime::UNIX_EPOCH)
        .format(time::macros::format_description!(
            "[year]-[month]-[day]T[hour]:[minute]:[second].[subsecond digits:3]Z"
        ))
        .unwrap_or_default()
}

/// `relay-client.ndjson`: Effect's `Logger.formatJson` entries in a `RotatingFileSink`
/// (1 MiB, two files).
struct RelayLog {
    path: PathBuf,
    size: StdMutex<Option<u64>>,
    write_failure_reported: AtomicBool,
    fibers: AtomicU64,
}

#[derive(Clone, Copy)]
enum Level {
    Info,
    Warn,
}

impl RelayLog {
    fn open(path: PathBuf) -> Self {
        let size = match path.parent().map(std::fs::create_dir_all).transpose() {
            Ok(_) => Some(std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0)),
            Err(error) => {
                tracing::warn!(file_path = %path.display(), %error, "Relay client log unavailable");
                None
            }
        };
        Self {
            path,
            size: StdMutex::new(size),
            write_failure_reported: AtomicBool::new(false),
            fibers: AtomicU64::new(1),
        }
    }

    fn fiber(&self) -> u64 {
        self.fibers.fetch_add(1, Ordering::Relaxed)
    }

    fn write(&self, fiber: u64, level: Level, message: &str, attributes: Value) {
        match level {
            Level::Info => tracing::info!(relay_client = %attributes, "{message}"),
            Level::Warn => tracing::warn!(relay_client = %attributes, "{message}"),
        }
        let level = match level {
            Level::Info => "INFO",
            Level::Warn => "WARN",
        };
        let timestamp = iso_now();
        let entry = json!({
            "message": [message, attributes],
            "level": level,
            "timestamp": timestamp,
            "annotations": {},
            "spans": {},
            "fiberId": format!("#{fiber}"),
        })
        .to_string();
        let bytes = entry.len() as u64 + 1;
        let line = if bytes <= LOG_MAX_BYTES {
            format!("{entry}\n")
        } else {
            format!(
                "{}\n",
                json!({
                    "timestamp": timestamp,
                    "level": level,
                    "message": ["Relay client log entry omitted: exceeds 1 MiB", {"bytes": bytes}],
                })
            )
        };
        let mut size = self.size.lock().unwrap_or_else(|e| e.into_inner());
        let Some(current) = size.as_mut() else {
            return;
        };
        if let Err(error) = self.append(current, line.as_bytes()) {
            if !self.write_failure_reported.swap(true, Ordering::Relaxed) {
                tracing::warn!(file_path = %self.path.display(), %error, "Relay client log unavailable");
            }
            *current = std::fs::metadata(&self.path).map(|m| m.len()).unwrap_or(0);
        }
    }

    fn append(&self, size: &mut u64, line: &[u8]) -> std::io::Result<()> {
        use std::io::Write;
        if *size > 0 && *size + line.len() as u64 > LOG_MAX_BYTES {
            let suffixed = |index: u32| PathBuf::from(format!("{}.{index}", self.path.display()));
            let _ = std::fs::remove_file(suffixed(LOG_MAX_FILES));
            for index in (1..LOG_MAX_FILES).rev() {
                if suffixed(index).exists() {
                    std::fs::rename(suffixed(index), suffixed(index + 1))?;
                }
            }
            if self.path.exists() {
                std::fs::rename(&self.path, suffixed(1))?;
            }
            *size = 0;
        }
        std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)?
            .write_all(line)?;
        *size += line.len() as u64;
        Ok(())
    }
}

/// `RelayClientLedger`: `userdata/relay-clients/<pid>.json` names each running connector and
/// the server that owns it, so the next server stops connectors a crashed one left behind.
/// Process identity comes from `/proc`, so this records nothing off Linux.
struct Ledger {
    directory: PathBuf,
    owner: Option<ProcessIdentity>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ProcessIdentity {
    pid: u32,
    start_time: String,
    command: Option<String>,
}

struct ObservedProcess {
    start_time: String,
    command: String,
    zombie: bool,
}

#[derive(PartialEq, Eq)]
enum Comparison {
    Same,
    Gone,
    Unknown,
}

impl ProcessIdentity {
    fn to_json(&self) -> Value {
        let mut value = json!({"pid": self.pid, "startTime": self.start_time});
        if let Some(command) = &self.command {
            value["command"] = json!(command);
        }
        value
    }

    fn from_json(value: &Value) -> Option<Self> {
        let pid = value.get("pid")?.as_u64()?;
        Some(Self {
            pid: u32::try_from(pid).ok()?,
            start_time: value.get("startTime")?.as_str()?.to_owned(),
            command: match value.get("command") {
                None => None,
                Some(command) => Some(command.as_str()?.to_owned()),
            },
        })
    }
}

fn observe(pid: u32) -> Option<ObservedProcess> {
    if !cfg!(target_os = "linux") {
        return None;
    }
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    let fields: Vec<&str> = stat
        .get(stat.rfind(')')? + 2..)?
        .split_whitespace()
        .collect();
    let start_time = fields.get(19)?.to_string();
    let cmdline = std::fs::read(format!("/proc/{pid}/cmdline")).ok()?;
    let cmdline = String::from_utf8_lossy(&cmdline);
    Some(ObservedProcess {
        start_time,
        command: cmdline
            .strip_suffix('\0')
            .unwrap_or(&cmdline)
            .replace('\0', " "),
        zombie: fields.first() == Some(&"Z"),
    })
}

fn process_exists(pid: u32) -> bool {
    #[cfg(unix)]
    {
        let Ok(pid) = libc::pid_t::try_from(pid) else {
            return false;
        };
        // SAFETY: signal 0 sends nothing; it only checks the pid.
        if unsafe { libc::kill(pid, 0) } == 0 {
            return true;
        }
        std::io::Error::last_os_error().raw_os_error() != Some(libc::ESRCH)
    }
    #[cfg(not(unix))]
    {
        let _ = pid;
        false
    }
}

fn signal_pid(pid: u32, signal: i32) {
    #[cfg(unix)]
    if let Ok(pid) = libc::pid_t::try_from(pid) {
        // SAFETY: plain kill(2) on a pid this server started or recorded.
        unsafe {
            libc::kill(pid, signal);
        }
    }
    #[cfg(not(unix))]
    let _ = (pid, signal);
}

#[cfg(unix)]
const SIGTERM: i32 = libc::SIGTERM;
#[cfg(not(unix))]
const SIGTERM: i32 = 15;
#[cfg(unix)]
const SIGKILL: i32 = libc::SIGKILL;
#[cfg(not(unix))]
const SIGKILL: i32 = 9;

impl Ledger {
    fn open(state_dir: &Path) -> Self {
        let owner = observe(std::process::id()).map(|observed| ProcessIdentity {
            pid: std::process::id(),
            start_time: observed.start_time,
            command: None,
        });
        Self {
            directory: state_dir.join("relay-clients"),
            owner,
        }
    }

    fn compare(identity: &ProcessIdentity) -> Comparison {
        match observe(identity.pid) {
            None if process_exists(identity.pid) => Comparison::Unknown,
            None => Comparison::Gone,
            Some(observed)
                if !observed.zombie
                    && observed.start_time == identity.start_time
                    && identity
                        .command
                        .as_ref()
                        .is_none_or(|command| *command == observed.command) =>
            {
                Comparison::Same
            }
            Some(_) => Comparison::Gone,
        }
    }

    /// Records a started connector; returns the entry to remove once it has stopped.
    fn track(&self, pid: u32) -> Option<PathBuf> {
        if !cfg!(target_os = "linux") {
            return None;
        }
        let client = observe(pid).filter(|observed| !observed.zombie);
        let (Some(owner), Some(client)) = (&self.owner, client) else {
            tracing::warn!(
                pid,
                "Could not record the relay client; it will keep running if this server crashes"
            );
            return None;
        };
        let client = ProcessIdentity {
            pid,
            start_time: client.start_time,
            command: Some(client.command),
        };
        let entry = json!({"version": 1, "client": client.to_json(), "owner": owner.to_json()});
        let path = self.directory.join(format!("{pid}.json"));
        let written = std::fs::create_dir_all(&self.directory).and_then(|()| {
            let temporary = self
                .directory
                .join(format!(".{pid}.json.{}.tmp", crate::platform::uuid_v4()));
            std::fs::write(&temporary, entry.to_string())?;
            std::fs::rename(&temporary, &path).inspect_err(|_| {
                let _ = std::fs::remove_file(&temporary);
            })
        });
        match written {
            Ok(()) => Some(path),
            Err(error) => {
                tracing::warn!(%error, "Could not record the relay client");
                None
            }
        }
    }

    fn forget(path: Option<PathBuf>) {
        if let Some(path) = path {
            if let Err(error) = std::fs::remove_file(&path) {
                if error.kind() != std::io::ErrorKind::NotFound {
                    tracing::warn!(%error, "Could not forget a stopped relay client");
                }
            }
        }
    }

    /// Signals only a process that still has the recorded pid, start time and command line,
    /// so a recycled pid is never touched. Returns whether the client is gone.
    async fn stop_orphan(client: &ProcessIdentity) -> bool {
        let mut state = Self::compare(client);
        if state != Comparison::Same {
            return state == Comparison::Gone;
        }
        tracing::info!(
            pid = client.pid,
            "Stopping a relay client left by a previous T3 Code server"
        );
        for signal in [SIGTERM, SIGKILL] {
            signal_pid(client.pid, signal);
            for _ in 0..40 {
                if state != Comparison::Same {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
                state = Self::compare(client);
            }
            if state != Comparison::Same {
                break;
            }
        }
        state == Comparison::Gone
    }

    async fn reap_orphans(&self) {
        if !cfg!(target_os = "linux") {
            return;
        }
        let Ok(entries) = std::fs::read_dir(&self.directory) else {
            return;
        };
        let pattern = regex::Regex::new(r"^\d+\.json$").expect("valid pattern");
        for entry in entries.flatten() {
            let name = entry.file_name();
            if !pattern.is_match(&name.to_string_lossy()) {
                continue;
            }
            let path = entry.path();
            let decoded = std::fs::read_to_string(&path).ok().and_then(|text| {
                let value: Value = serde_json::from_str(&text).ok()?;
                (value.get("version")? == 1).then_some(())?;
                Some((
                    ProcessIdentity::from_json(value.get("client")?)?,
                    ProcessIdentity::from_json(value.get("owner")?)?,
                ))
            });
            if let Some((client, owner)) = decoded {
                if Self::compare(&owner) != Comparison::Gone || !Self::stop_orphan(&client).await {
                    continue;
                }
            }
            if let Err(error) = std::fs::remove_file(&path) {
                if error.kind() != std::io::ErrorKind::NotFound {
                    tracing::warn!(entry_path = %path.display(), %error, "Could not clean up a recorded relay client");
                }
            }
        }
    }
}

/// `classifyRelayClientOutput`.
fn classify(line: &str) -> &'static str {
    static PATTERNS: std::sync::OnceLock<[regex::Regex; 4]> = std::sync::OnceLock::new();
    let [connected, abrupt, warning, unregistered] = PATTERNS.get_or_init(|| {
        [
            regex::Regex::new(r"(?i)\bRegistered tunnel connection\b").expect("valid"),
            regex::Regex::new(
                r#"\bERR\s+(?:Request failed\s+)?error="Incoming request ended abruptly: context canceled"(?:\s|$)"#,
            )
            .expect("valid"),
            regex::Regex::new(r"\b(?:ERR|WRN|FTL|PNC)\b").expect("valid"),
            regex::Regex::new(r"\bINF\b.*\bUnregistered tunnel connection\b").expect("valid"),
        ]
    });
    if connected.is_match(line) {
        "connected"
    } else if abrupt.is_match(line) {
        "debug"
    } else if warning.is_match(line) {
        "warning"
    } else if unregistered.is_match(line) {
        "unregistered"
    } else {
        "debug"
    }
}

/// `isRejectedRelayClientTunnelOutput`.
fn is_rejected(line: &str) -> bool {
    static PATTERNS: std::sync::OnceLock<[regex::Regex; 2]> = std::sync::OnceLock::new();
    let [side, error] = PATTERNS.get_or_init(|| {
        [
            regex::Regex::new(r"(?i)\bRegister tunnel error from server side\b").expect("valid"),
            regex::Regex::new(
                r#"(?i)error="(?:Unauthorized:[^"]*|Failed to get tunnel|Tunnel not found|Record for tunnel not found|Invalid tunnel secret)""#,
            )
            .expect("valid"),
        ]
    });
    side.is_match(line) && error.is_match(line)
}

fn capture<'a>(pattern: &str, line: &'a str) -> Option<&'a str> {
    regex::Regex::new(pattern)
        .ok()?
        .captures(line)?
        .get(1)
        .map(|m| m.as_str())
}

/// Facts about one running connector, shared with its output and watcher tasks.
struct Connector {
    generation: u64,
    pid: u32,
    config: RuntimeConfig,
    log_fiber: u64,
    registered: AtomicBool,
    exited: AtomicBool,
    rejected: AtomicU32,
    protocol: StdMutex<Option<String>>,
    metrics_address: StdMutex<Option<String>>,
    metrics_known: Notify,
}

impl Connector {
    fn attributes(&self, extra: Option<(&str, Value)>) -> Value {
        let mut map = Map::new();
        map.insert("pid".into(), json!(self.pid));
        self.config.add_tunnel(&mut map);
        if let Some((key, value)) = extra {
            map.insert(key.into(), value);
        }
        Value::Object(map)
    }
}

struct Active {
    connector: Arc<Connector>,
    key: String,
    stop: Option<oneshot::Sender<()>>,
    supervisor: JoinHandle<()>,
    watchers: Vec<AbortHandle>,
    ledger_entry: Option<PathBuf>,
}

struct State {
    desired: Option<RuntimeConfig>,
    active: Option<Active>,
    last_status: Value,
}

/// What the runtime is built from; tests replace the resolver, timings and environment.
pub struct RuntimeOptions {
    pub base_dir: PathBuf,
    pub state_dir: PathBuf,
    pub secrets_dir: PathBuf,
    pub logs_dir: PathBuf,
    /// `ManagedLimits.allowsConnect`: without T3 Connect no connector ever runs.
    pub connect_allowed: bool,
    pub resolver: Box<dyn Fn() -> relay_client::Options + Send + Sync>,
    /// Variables the connector receives besides this server's own environment.
    pub extra_env: Vec<(OsString, OsString)>,
    /// Whether `TUNNEL_TRANSPORT_PROTOCOL` is set, which leaves the transport to cloudflared.
    pub transport_chosen_by_user: bool,
    pub timings: Timings,
}

impl RuntimeOptions {
    pub fn for_paths(paths: &crate::config::ServerPaths, connect_allowed: bool) -> Self {
        let base_dir = paths.base_dir.clone();
        Self {
            base_dir: base_dir.clone(),
            state_dir: paths.state_dir.clone(),
            secrets_dir: paths.secrets.clone(),
            logs_dir: paths.logs.clone(),
            connect_allowed,
            resolver: Box::new(move || relay_client::Options::from_env(&base_dir)),
            extra_env: Vec::new(),
            transport_chosen_by_user: std::env::var_os("TUNNEL_TRANSPORT_PROTOCOL").is_some(),
            timings: Timings::default(),
        }
    }
}

struct Inner {
    options: RuntimeOptions,
    log: RelayLog,
    ledger: Ledger,
    http2: AtomicBool,
    shutting_down: AtomicBool,
    state: Mutex<State>,
    restart_delay: StdMutex<Duration>,
    quic_stalls: StdMutex<Vec<Instant>>,
    not_ready_threshold: AtomicU32,
    generations: AtomicU64,
    current_generation: AtomicU64,
    /// `withLinkStateLock`.
    link_state: Mutex<()>,
    /// `recoveryRequests`: a sliding queue of one.
    recovery: StdMutex<Option<RuntimeConfig>>,
    recovery_ready: Notify,
}

/// `CloudManagedEndpointRuntime`.
#[derive(Clone)]
pub struct ManagedEndpointRuntime {
    inner: Arc<Inner>,
}

impl ManagedEndpointRuntime {
    /// Builds the runtime and stops connectors that crashed servers left running, before any
    /// connector of this server starts.
    pub async fn open(options: RuntimeOptions) -> Self {
        let log = RelayLog::open(options.logs_dir.join(RELAY_CLIENT_LOG_FILE));
        let ledger = Ledger::open(&options.state_dir);
        ledger.reap_orphans().await;
        let http2 = !options.transport_chosen_by_user
            && read_secret(&options.secrets_dir, HTTP2_FALLBACK_STATE)
                .and_then(|bytes| serde_json::from_slice::<Value>(&bytes).ok())
                .and_then(|value| value.get("fellBackAt")?.as_f64())
                .is_some_and(|fell_back_at| {
                    let now = now_millis();
                    let at = fell_back_at as i128;
                    now >= at && now - at < HTTP2_FALLBACK_TTL_MS
                });
        Self {
            inner: Arc::new(Inner {
                options,
                log,
                ledger,
                http2: AtomicBool::new(http2),
                shutting_down: AtomicBool::new(false),
                state: Mutex::new(State {
                    desired: None,
                    active: None,
                    last_status: json!({"status": "disabled"}),
                }),
                restart_delay: StdMutex::new(Duration::ZERO),
                quic_stalls: StdMutex::new(Vec::new()),
                not_ready_threshold: AtomicU32::new(CONNECTOR_NOT_READY_CHECKS_BEFORE_RESTART),
                generations: AtomicU64::new(0),
                current_generation: AtomicU64::new(0),
                link_state: Mutex::new(()),
                recovery: StdMutex::new(None),
                recovery_ready: Notify::new(),
            }),
        }
    }

    /// `applyConfig`: runs the connector for `config`, or stops it for `None`.
    pub async fn apply_config(&self, requested: Option<RuntimeConfig>) -> Value {
        let inner = &self.inner;
        let mut state = inner.state.lock().await;
        let config = requested.filter(|_| inner.options.connect_allowed);
        let unchanged =
            matches!((&state.desired, &config), (Some(d), Some(c)) if d.key() == c.key());
        if !unchanged {
            *inner
                .restart_delay
                .lock()
                .unwrap_or_else(|e| e.into_inner()) = Duration::ZERO;
        }
        state.desired = config.clone();
        reconcile(inner, &mut state, config).await
    }

    /// What the last `applyConfig` (or restart) reported, with `running` only while that
    /// connector still runs. Between a connector's exit and its restart this is the exit as a
    /// `spawn-failed` failure. The reference has no such read; it is for tests and logs.
    pub async fn status(&self) -> Value {
        let state = self.inner.state.lock().await;
        if let Some(active) = &state.active {
            if !active.connector.exited.load(Ordering::SeqCst) {
                return running_status(&active.connector);
            }
            return active.connector.config.with_tunnel(json!({
                "status": "failed",
                "providerKind": "cloudflare_tunnel",
                "failure": "spawn-failed",
                "reason": "Relay client exited.",
            }));
        }
        state.last_status.clone()
    }

    /// The running connector's pid and whether its tunnel has registered, if one runs.
    pub async fn connector(&self) -> Option<(u32, bool)> {
        let state = self.inner.state.lock().await;
        let active = state.active.as_ref()?;
        (!active.connector.exited.load(Ordering::SeqCst)).then(|| {
            (
                active.connector.pid,
                active.connector.registered.load(Ordering::SeqCst),
            )
        })
    }

    /// No connector restarts after this; called as soon as the server starts to stop, since
    /// a terminal or service manager may signal the connector (in this process group) too.
    pub fn begin_shutdown(&self) {
        self.inner.shutting_down.store(true, Ordering::SeqCst);
    }

    /// The layer finalizer: `applyConfig(null)`.
    pub async fn shutdown(&self) {
        self.begin_shutdown();
        self.apply_config(None).await;
    }

    /// `withLinkStateLock`: serializes changes to the stored link and the connector.
    pub async fn lock_link_state(&self) -> tokio::sync::MutexGuard<'_, ()> {
        self.inner.link_state.lock().await
    }

    /// `requestRecovery`: asks for a replacement tunnel; a newer request replaces a pending one.
    pub fn request_recovery(&self, config: RuntimeConfig) {
        *self
            .inner
            .recovery
            .lock()
            .unwrap_or_else(|e| e.into_inner()) = Some(config);
        self.inner.recovery_ready.notify_one();
    }

    /// The next `recoveryRequests` element.
    pub async fn next_recovery_request(&self) -> RuntimeConfig {
        loop {
            let ready = self.inner.recovery_ready.notified();
            if let Some(config) = self
                .inner
                .recovery
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .take()
            {
                return config;
            }
            ready.await;
        }
    }

    pub(crate) fn secrets_dir(&self) -> &Path {
        &self.inner.options.secrets_dir
    }

    pub(crate) fn base_dir(&self) -> &Path {
        &self.inner.options.base_dir
    }

    /// `CloudLink.startManagedTunnelIfOriginConfirmed`: runs the stored tunnel when T3 Connect
    /// confirmed it for this server's origin or, with `require_confirmed_origin` off (the
    /// fallback while T3 Connect stays unreachable), whenever one is stored. `Err` carries the
    /// runtime status of a connector that did not start.
    pub async fn start_if_origin_confirmed(
        &self,
        origin: &LocalOrigin,
        require_confirmed_origin: bool,
    ) -> Result<bool, Value> {
        let _lock = self.lock_link_state().await;
        self.start_if_origin_confirmed_locked(origin, require_confirmed_origin)
            .await
    }

    async fn start_if_origin_confirmed_locked(
        &self,
        origin: &LocalOrigin,
        require_confirmed_origin: bool,
    ) -> Result<bool, Value> {
        let secrets = &self.inner.options.secrets_dir;
        let Some(config) = read_secret_string(secrets, CLOUD_ENDPOINT_RUNTIME_CONFIG)
            .and_then(|text| RuntimeConfig::decode(&text))
            .filter(|config| config.provider_kind == "cloudflare_tunnel")
        else {
            return Ok(false);
        };
        if require_confirmed_origin {
            let confirmed = read_secret_string(secrets, CLOUD_ENDPOINT_CONFIRMED_ORIGIN)
                .and_then(|text| serde_json::from_str::<Value>(&text).ok())
                .is_some_and(|marker| {
                    RuntimeConfig::from_value(&marker["config"])
                        .is_some_and(|stored| stored.key() == config.key())
                        && marker["origin"]["localHttpHost"].as_str().map(str::trim)
                            == Some(origin.host.as_str())
                        && marker["origin"]["localHttpPort"].as_u64()
                            == Some(u64::from(origin.port))
                });
            if !confirmed {
                return Ok(false);
            }
        }
        let status = self.apply_config(Some(config)).await;
        if status["status"] != "running" {
            return Err(status);
        }
        Ok(true)
    }
}

fn request_recovery(inner: &Inner, config: RuntimeConfig) {
    *inner.recovery.lock().unwrap_or_else(|e| e.into_inner()) = Some(config);
    inner.recovery_ready.notify_one();
}

fn running_status(connector: &Connector) -> Value {
    connector.config.with_tunnel(json!({
        "status": "running",
        "providerKind": "cloudflare_tunnel",
        "pid": connector.pid,
    }))
}

/// Stops the connector and waits until its process is gone.
async fn stop_connector(inner: &Inner, mut active: Active) {
    for watcher in &active.watchers {
        watcher.abort();
    }
    if let Some(stop) = active.stop.take() {
        let _ = stop.send(());
    }
    let _ = (&mut active.supervisor).await;
    Ledger::forget(active.ledger_entry.take());
    inner.log.write(
        active.connector.log_fiber,
        Level::Info,
        "Relay client stopped",
        json!({"pid": active.connector.pid}),
    );
}

/// `reconcileConfig`; the caller holds the state lock.
///
/// Boxed: a restart reconciles from a task this spawns, so the future type is recursive.
fn reconcile<'a>(
    inner: &'a Arc<Inner>,
    state: &'a mut State,
    config: Option<RuntimeConfig>,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Value> + Send + 'a>> {
    Box::pin(async move {
        let status = reconcile_inner(inner, state, config).await;
        state.last_status = status.clone();
        status
    })
}

async fn reconcile_inner(
    inner: &Arc<Inner>,
    state: &mut State,
    config: Option<RuntimeConfig>,
) -> Value {
    let config = match config {
        Some(config) if config.provider_kind == "cloudflare_tunnel" => config,
        other => {
            if let Some(active) = state.active.take() {
                stop_connector(inner, active).await;
            }
            return match other {
                Some(config) => {
                    json!({"status": "unsupported", "providerKind": config.provider_kind})
                }
                None => json!({"status": "disabled"}),
            };
        }
    };
    let key = config.key();
    if let Some(active) = &state.active {
        if active.key == key && !active.connector.exited.load(Ordering::SeqCst) {
            return running_status(&active.connector);
        }
    }
    if let Some(active) = state.active.take() {
        stop_connector(inner, active).await;
    }

    let executable = relay_client::resolve(&(inner.options.resolver)()).await;
    if executable["status"] != "available" {
        let unsupported = executable["status"] == "unsupported";
        return config.with_tunnel(json!({
            "status": "failed",
            "providerKind": "cloudflare_tunnel",
            "failure": if unsupported { "unsupported-platform" } else { "not-installed" },
            "reason": if unsupported {
                format!(
                    "Relay client is unsupported on {}-{}.",
                    executable["platform"].as_str().unwrap_or_default(),
                    executable["arch"].as_str().unwrap_or_default()
                )
            } else {
                "The relay client is not installed.".to_owned()
            },
        }));
    }

    let fiber = inner.log.fiber();
    let http2 = inner.http2.load(Ordering::SeqCst);
    let mut command = crate::login_shell::tokio_command(
        executable["executablePath"].as_str().unwrap_or_default(),
    );
    command.args([
        "tunnel",
        "--no-autoupdate",
        "--loglevel",
        "info",
        "--output",
        "default",
        "--metrics",
        "127.0.0.1:0",
    ]);
    if http2 {
        command.args(["--protocol", "http2"]);
    }
    command
        .arg("run")
        .envs(inner.options.extra_env.iter().cloned())
        .env("TUNNEL_TOKEN", &config.connector_token)
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .kill_on_drop(true);
    let mut child = match command.spawn() {
        Ok(child) => child,
        Err(error) => {
            let cause = error.to_string();
            let mut attributes = Map::new();
            attributes.insert("cause".into(), json!(cause));
            config.add_tunnel(&mut attributes);
            inner.log.write(
                fiber,
                Level::Warn,
                "Failed to start relay client",
                Value::Object(attributes),
            );
            return config.with_tunnel(json!({
                "status": "failed",
                "providerKind": "cloudflare_tunnel",
                "failure": "spawn-failed",
                "reason": cause,
            }));
        }
    };
    let Some(pid) = child.id() else {
        return config.with_tunnel(json!({
            "status": "failed",
            "providerKind": "cloudflare_tunnel",
            "failure": "spawn-failed",
            "reason": "Relay client did not start.",
        }));
    };
    let mut started = Map::new();
    started.insert("pid".into(), json!(pid));
    started.insert("source".into(), executable["source"].clone());
    started.insert("version".into(), executable["version"].clone());
    started.insert(
        "protocol".into(),
        json!(if http2 { "http2" } else { "auto" }),
    );
    config.add_tunnel(&mut started);
    inner.log.write(
        fiber,
        Level::Info,
        "Relay client process started; waiting for tunnel connection",
        Value::Object(started),
    );
    let ledger_entry = inner.ledger.track(pid);

    let generation = inner.generations.fetch_add(1, Ordering::SeqCst) + 1;
    inner.current_generation.store(generation, Ordering::SeqCst);
    let connector = Arc::new(Connector {
        generation,
        pid,
        config: config.clone(),
        log_fiber: fiber,
        registered: AtomicBool::new(false),
        exited: AtomicBool::new(false),
        rejected: AtomicU32::new(0),
        protocol: StdMutex::new(None),
        metrics_address: StdMutex::new(None),
        metrics_known: Notify::new(),
    });
    let mut watchers = Vec::new();
    if let Some(stdout) = child.stdout.take() {
        watchers.push(spawn_output(inner, &connector, stdout));
    }
    if let Some(stderr) = child.stderr.take() {
        watchers.push(spawn_output(inner, &connector, stderr));
    }
    watchers
        .push(tokio::spawn(watch_registration(inner.clone(), connector.clone())).abort_handle());
    watchers.push(tokio::spawn(watch_readiness(inner.clone(), connector.clone())).abort_handle());
    let (stop, stopped) = oneshot::channel();
    let supervisor = tokio::spawn(supervise(
        inner.clone(),
        child,
        connector.clone(),
        stopped,
        Instant::now(),
    ));
    let status = running_status(&connector);
    state.active = Some(Active {
        connector,
        key,
        stop: Some(stop),
        supervisor,
        watchers,
        ledger_entry,
    });
    status
}

/// Owns the child: on a stop request it terminates it; when it exits by itself it hands the
/// exit to [`restart_after_exit`].
async fn supervise(
    inner: Arc<Inner>,
    mut child: Child,
    connector: Arc<Connector>,
    stopped: oneshot::Receiver<()>,
    started_at: Instant,
) {
    // The reference leaves the connector's stdin pipe open for its lifetime.
    let _stdin = child.stdin.take();
    tokio::select! {
        status = child.wait() => {
            connector.exited.store(true, Ordering::SeqCst);
            tokio::spawn(restart_after_exit(inner, connector, status, started_at));
        }
        _ = stopped => {
            signal_pid(connector.pid, SIGTERM);
            if tokio::time::timeout(inner.options.timings.stop_grace, child.wait()).await.is_err() {
                let _ = child.start_kill();
                let _ = child.wait().await;
            }
            connector.exited.store(true, Ordering::SeqCst);
        }
    }
}

fn exit_attribute(status: &std::io::Result<std::process::ExitStatus>) -> (&'static str, Value) {
    match status {
        Ok(status) => {
            if let Some(code) = status.code() {
                return ("exitCode", json!(code));
            }
            #[cfg(unix)]
            {
                use std::os::unix::process::ExitStatusExt;
                if let Some(signal) = status.signal() {
                    let name = match signal {
                        libc::SIGTERM => "SIGTERM".to_owned(),
                        libc::SIGKILL => "SIGKILL".to_owned(),
                        libc::SIGINT => "SIGINT".to_owned(),
                        libc::SIGHUP => "SIGHUP".to_owned(),
                        libc::SIGABRT => "SIGABRT".to_owned(),
                        libc::SIGSEGV => "SIGSEGV".to_owned(),
                        other => other.to_string(),
                    };
                    return (
                        "cause",
                        json!(format!(
                            "Process interrupted due to receipt of signal: '{name}'"
                        )),
                    );
                }
            }
            ("cause", json!(status.to_string()))
        }
        Err(error) => ("cause", json!(error.to_string())),
    }
}

/// `superviseConnector` after the child exits: crash-loop backoff, then a restart while the
/// same tunnel is still wanted.
async fn restart_after_exit(
    inner: Arc<Inner>,
    connector: Arc<Connector>,
    status: std::io::Result<std::process::ExitStatus>,
    started_at: Instant,
) {
    let is_current = |state: &State| {
        state
            .active
            .as_ref()
            .is_some_and(|active| active.connector.generation == connector.generation)
    };
    if !is_current(&*inner.state.lock().await) {
        return;
    }
    let uptime = started_at.elapsed();
    let timings = &inner.options.timings;
    let delay = {
        let mut current = inner
            .restart_delay
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if uptime >= timings.stable_uptime {
            *current = Duration::ZERO;
            Duration::ZERO
        } else {
            let delay = *current;
            *current = if delay.is_zero() {
                timings.backoff_base
            } else {
                (delay * 2).min(timings.backoff_max)
            };
            delay
        }
    };
    if !delay.is_zero() {
        let mut attributes = Map::new();
        attributes.insert("pid".into(), json!(connector.pid));
        attributes.insert("uptimeMillis".into(), json!(uptime.as_millis() as u64));
        attributes.insert("restartDelayMillis".into(), json!(delay.as_millis() as u64));
        connector.config.add_tunnel(&mut attributes);
        inner.log.write(
            connector.log_fiber,
            Level::Warn,
            "Relay client is crash-looping; delaying restart",
            Value::Object(attributes),
        );
        tokio::time::sleep(delay).await;
    }
    let mut state = inner.state.lock().await;
    if !is_current(&state) {
        return;
    }
    if let Some(active) = state.active.take() {
        stop_connector(&inner, active).await;
    }
    let Some(desired) = state.desired.clone() else {
        return;
    };
    if inner.shutting_down.load(Ordering::SeqCst)
        || desired.provider_kind != "cloudflare_tunnel"
        || desired.key() != connector.config.key()
    {
        return;
    }
    let mut attributes = Map::new();
    attributes.insert("pid".into(), json!(connector.pid));
    let (key, value) = exit_attribute(&status);
    attributes.insert(key.into(), value);
    connector.config.add_tunnel(&mut attributes);
    inner.log.write(
        connector.log_fiber,
        Level::Warn,
        "Relay client exited; restarting",
        Value::Object(attributes),
    );
    reconcile(&inner, &mut state, Some(desired)).await;
}

fn spawn_output<R>(inner: &Arc<Inner>, connector: &Arc<Connector>, stream: R) -> AbortHandle
where
    R: AsyncRead + Unpin + Send + 'static,
{
    let inner = inner.clone();
    let connector = connector.clone();
    tokio::spawn(async move {
        let mut lines = BufReader::new(stream).lines();
        while let Ok(Some(line)) = lines.next_line().await {
            let line = line.trim();
            if !line.is_empty() {
                observe_output(&inner, &connector, line);
            }
        }
    })
    .abort_handle()
}

/// `observeConnectorOutput`.
fn observe_output(inner: &Arc<Inner>, connector: &Arc<Connector>, line: &str) {
    let output = line.replace(&connector.config.connector_token, "<redacted>");
    let attributes = || connector.attributes(Some(("output", json!(output))));
    let fiber = connector.log_fiber;
    match classify(line) {
        "connected" => {
            connector.rejected.store(0, Ordering::SeqCst);
            connector.registered.store(true, Ordering::SeqCst);
            let protocol = capture(r"\bprotocol=(\w+)", line).map(str::to_owned);
            *connector.protocol.lock().unwrap_or_else(|e| e.into_inner()) = protocol.clone();
            // cloudflared started with `auto` fell back on its own; later starts skip QUIC too.
            if protocol.as_deref() == Some("http2")
                && !inner.options.transport_chosen_by_user
                && !inner.http2.load(Ordering::SeqCst)
            {
                remember_http2_fallback(inner, fiber);
            }
            inner.log.write(
                fiber,
                Level::Info,
                "Relay client tunnel connection registered",
                attributes(),
            );
        }
        "unregistered" => inner.log.write(
            fiber,
            Level::Info,
            "Relay client tunnel connection unregistered",
            attributes(),
        ),
        "warning" => {
            if regex::Regex::new(r"\btimeout: no recent network activity\b")
                .is_ok_and(|pattern| pattern.is_match(line))
            {
                note_quic_stall(inner, connector);
            } else if is_rejected(line) {
                let rejected = connector.rejected.fetch_add(1, Ordering::SeqCst) + 1;
                if rejected >= TUNNEL_AUTHORIZATION_FAILURES_BEFORE_RECOVERY {
                    connector.rejected.store(0, Ordering::SeqCst);
                    inner.log.write(
                        fiber,
                        Level::Warn,
                        "Relay client tunnel was rejected; requesting recovery",
                        attributes(),
                    );
                    request_recovery(inner, connector.config.clone());
                    return;
                }
            }
            inner.log.write(
                fiber,
                Level::Warn,
                "Relay client reported a transport warning",
                attributes(),
            );
        }
        _ => {
            // Debug output stays out of the log, as at the reference's default log level.
            if let Some(address) = capture(
                r"\bStarting metrics server on (127\.0\.0\.1:\d+)/metrics\b",
                line,
            ) {
                *connector
                    .metrics_address
                    .lock()
                    .unwrap_or_else(|e| e.into_inner()) = Some(address.to_owned());
                connector.metrics_known.notify_waiters();
                connector.metrics_known.notify_one();
            }
        }
    }
}

fn remember_http2_fallback(inner: &Inner, fiber: u64) {
    inner.http2.store(true, Ordering::SeqCst);
    let state = json!({"fellBackAt": now_millis() as u64}).to_string();
    if write_secret(
        &inner.options.secrets_dir,
        HTTP2_FALLBACK_STATE,
        state.as_bytes(),
    )
    .is_err()
    {
        inner.log.write(
            fiber,
            Level::Warn,
            "Could not persist the relay client HTTP/2 fallback",
            json!({}),
        );
    }
}

fn note_quic_stall(inner: &Arc<Inner>, connector: &Arc<Connector>) {
    if inner.options.transport_chosen_by_user
        || inner.http2.load(Ordering::SeqCst)
        || inner.current_generation.load(Ordering::SeqCst) != connector.generation
    {
        return;
    }
    let now = Instant::now();
    let stalls = {
        let mut stalls = inner.quic_stalls.lock().unwrap_or_else(|e| e.into_inner());
        stalls.retain(|at| now.duration_since(*at) < QUIC_STALL_WINDOW);
        stalls.push(now);
        stalls.len()
    };
    if stalls >= QUIC_STALLS_BEFORE_HTTP2 {
        tokio::spawn(switch_to_http2(inner.clone(), connector.clone()));
    }
}

async fn switch_to_http2(inner: Arc<Inner>, connector: Arc<Connector>) {
    let mut state = inner.state.lock().await;
    let current = state
        .active
        .as_ref()
        .is_some_and(|active| active.connector.generation == connector.generation);
    let Some(desired) = state.desired.clone() else {
        return;
    };
    if inner.http2.load(Ordering::SeqCst) || !current {
        return;
    }
    remember_http2_fallback(&inner, connector.log_fiber);
    inner.log.write(
        connector.log_fiber,
        Level::Warn,
        "Relay client keeps losing QUIC connections; switching to HTTP/2",
        connector.attributes(None),
    );
    if let Some(active) = state.active.take() {
        stop_connector(&inner, active).await;
    }
    reconcile(&inner, &mut state, Some(desired)).await;
}

/// `watchConnectorRegistration`.
async fn watch_registration(inner: Arc<Inner>, connector: Arc<Connector>) {
    loop {
        tokio::time::sleep(inner.options.timings.registration_timeout).await;
        if connector.registered.load(Ordering::SeqCst) {
            return;
        }
        inner.log.write(
            connector.log_fiber,
            Level::Warn,
            "Relay client has not registered a tunnel connection; requesting recovery",
            connector.attributes(None),
        );
        request_recovery(&inner, connector.config.clone());
    }
}

/// `watchConnectorReadiness`: restarts a connector whose metrics server keeps answering 503.
async fn watch_readiness(inner: Arc<Inner>, connector: Arc<Connector>) {
    let address = loop {
        let known = connector.metrics_known.notified();
        if let Some(address) = connector
            .metrics_address
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
        {
            break address;
        }
        known.await;
    };
    let Ok(client) = reqwest::Client::builder()
        .no_proxy()
        .timeout(inner.options.timings.readiness_timeout)
        .build()
    else {
        return;
    };
    let url = format!("http://{address}/ready");
    let mut not_ready = 0;
    loop {
        tokio::time::sleep(inner.options.timings.readiness_interval).await;
        let Ok(response) = client.get(&url).send().await else {
            continue;
        };
        match response.status().as_u16() {
            200 => {
                not_ready = 0;
                inner
                    .not_ready_threshold
                    .store(CONNECTOR_NOT_READY_CHECKS_BEFORE_RESTART, Ordering::SeqCst);
            }
            503 => {
                not_ready += 1;
                let threshold = inner.not_ready_threshold.load(Ordering::SeqCst);
                if not_ready < threshold {
                    continue;
                }
                not_ready = 0;
                inner.not_ready_threshold.store(
                    (threshold * 2).min(CONNECTOR_NOT_READY_CHECKS_MAX),
                    Ordering::SeqCst,
                );
                inner.log.write(
                    connector.log_fiber,
                    Level::Warn,
                    "Relay client has no tunnel connection; restarting it",
                    {
                        let mut map = Map::new();
                        map.insert("pid".into(), json!(connector.pid));
                        map.insert("notReadyChecks".into(), json!(threshold));
                        connector.config.add_tunnel(&mut map);
                        Value::Object(map)
                    },
                );
                // The supervisor restarts it with the usual crash-loop backoff.
                signal_pid(connector.pid, SIGTERM);
            }
            _ => {}
        }
    }
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::*;

    const TOKEN: &str =
        "eyJhIjoiZmFrZS1hY2NvdW50IiwidCI6ImZha2UtdHVubmVsIiwicyI6ImZha2Utc2VjcmV0In0";

    struct Home {
        dir: tempfile::TempDir,
        binary: PathBuf,
    }

    impl Home {
        fn new() -> Self {
            let dir = tempfile::tempdir().unwrap();
            let tools = dir.path().join("tools-bin");
            std::fs::create_dir_all(&tools).unwrap();
            let binary = tools.join("cloudflared");
            std::fs::copy(
                Path::new(env!("CARGO_MANIFEST_DIR")).join("scripts/fake_cloudflared.py"),
                &binary,
            )
            .unwrap();
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&binary, std::fs::Permissions::from_mode(0o755)).unwrap();
            // A test thread that forks while the copy is open holds its write descriptor
            // until it execs, and exec then fails with ETXTBSY; wait until it can run.
            for _ in 0..200 {
                match crate::login_shell::command(&binary).arg("version").output() {
                    Err(error) if error.raw_os_error() == Some(libc::ETXTBSY) => {
                        std::thread::sleep(Duration::from_millis(10));
                    }
                    _ => break,
                }
            }
            Self { dir, binary }
        }

        fn paths(&self) -> crate::config::ServerPaths {
            crate::config::ServerPaths::from_base(self.dir.path())
        }

        fn secret(&self, name: &str, value: &str) {
            write_secret(&self.paths().secrets, name, value.as_bytes()).unwrap();
        }

        fn options(&self, override_path: Option<&Path>) -> RuntimeOptions {
            let paths = self.paths();
            let base_dir = paths.base_dir.clone();
            let override_path = override_path.map(|p| p.to_string_lossy().into_owned());
            RuntimeOptions {
                resolver: Box::new(move || relay_client::Options {
                    base_dir: base_dir.clone(),
                    platform: relay_client::node_platform().into(),
                    arch: relay_client::node_arch().into(),
                    override_path: override_path.clone(),
                    path_env: Some(String::new()),
                }),
                transport_chosen_by_user: false,
                timings: Timings {
                    stable_uptime: Duration::from_secs(30),
                    backoff_base: Duration::from_millis(200),
                    backoff_max: Duration::from_secs(1),
                    ..Timings::default()
                },
                ..RuntimeOptions::for_paths(&paths, true)
            }
        }

        async fn runtime(&self) -> ManagedEndpointRuntime {
            ManagedEndpointRuntime::open(self.options(Some(&self.binary))).await
        }

        fn records(&self) -> Vec<Value> {
            std::fs::read_to_string(self.binary.with_file_name("fake-cloudflared.jsonl"))
                .unwrap_or_default()
                .lines()
                .map(|line| serde_json::from_str(line).unwrap())
                .collect()
        }

        fn starts(&self) -> Vec<Value> {
            self.records()
                .into_iter()
                .filter(|r| r["event"] == "start")
                .collect()
        }

        fn log(&self) -> Vec<Value> {
            std::fs::read_to_string(self.paths().logs.join(RELAY_CLIENT_LOG_FILE))
                .unwrap_or_default()
                .lines()
                .map(|line| serde_json::from_str(line).unwrap())
                .collect()
        }

        fn messages(&self) -> Vec<String> {
            self.log()
                .iter()
                .map(|entry| entry["message"][0].as_str().unwrap().to_owned())
                .collect()
        }

        fn seed(&self, port: u16) {
            self.secret(CLOUD_ENDPOINT_RUNTIME_CONFIG, &config_json());
            self.secret(
                CLOUD_ENDPOINT_CONFIRMED_ORIGIN,
                &format!(
                    r#"{{"config":{},"origin":{{"localHttpHost":"127.0.0.1","localHttpPort":{port}}}}}"#,
                    config_json()
                ),
            );
        }
    }

    fn config_json() -> String {
        json!({
            "providerKind": "cloudflare_tunnel",
            "connectorToken": TOKEN,
            "tunnelId": "00000000-0000-4000-8000-00000000c0de",
            "tunnelName": "t3coderelay-managedendpoint-test",
        })
        .to_string()
    }

    fn config() -> RuntimeConfig {
        RuntimeConfig::decode(&config_json()).unwrap()
    }

    fn origin(port: u16) -> LocalOrigin {
        LocalOrigin {
            host: "127.0.0.1".into(),
            port,
        }
    }

    async fn wait_for(what: &str, mut ready: impl FnMut() -> bool) {
        for _ in 0..200 {
            if ready() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        panic!("timed out waiting for {what}");
    }

    fn alive(pid: u64) -> bool {
        observe(pid as u32).is_some_and(|p| !p.zombie)
    }

    #[tokio::test]
    async fn without_a_confirmed_origin_no_connector_starts() {
        let home = Home::new();
        let runtime = home.runtime().await;
        assert_eq!(
            runtime.start_if_origin_confirmed(&origin(4000), true).await,
            Ok(false)
        );
        assert_eq!(runtime.status().await, json!({"status": "disabled"}));
        // A stored tunnel confirmed for no origin, or for another one, stays down too.
        home.secret(CLOUD_ENDPOINT_RUNTIME_CONFIG, &config_json());
        assert_eq!(
            runtime.start_if_origin_confirmed(&origin(4000), true).await,
            Ok(false)
        );
        home.seed(4001);
        assert_eq!(
            runtime.start_if_origin_confirmed(&origin(4000), true).await,
            Ok(false)
        );
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert!(home.starts().is_empty());
        assert!(home.log().is_empty());
    }

    #[tokio::test]
    async fn a_policy_without_connect_runs_no_connector() {
        let home = Home::new();
        home.seed(4000);
        let runtime = ManagedEndpointRuntime::open(RuntimeOptions {
            connect_allowed: false,
            ..home.options(Some(&home.binary))
        })
        .await;
        let status = runtime.start_if_origin_confirmed(&origin(4000), true).await;
        assert_eq!(status, Err(json!({"status": "disabled"})));
        assert_eq!(runtime.status().await, json!({"status": "disabled"}));
    }

    #[tokio::test]
    async fn an_unconfirmed_tunnel_starts_only_when_confirmation_is_not_required() {
        let home = Home::new();
        home.secret(CLOUD_ENDPOINT_RUNTIME_CONFIG, &config_json());
        let runtime = home.runtime().await;
        assert_eq!(
            runtime.start_if_origin_confirmed(&origin(4000), true).await,
            Ok(false)
        );
        assert_eq!(
            runtime
                .start_if_origin_confirmed(&origin(4000), false)
                .await,
            Ok(true)
        );
        runtime.shutdown().await;
    }

    #[tokio::test]
    async fn recovery_requests_keep_only_the_newest() {
        let home = Home::new();
        let runtime = home.runtime().await;
        let mut older = config();
        older.connector_token = "older".into();
        runtime.request_recovery(older);
        runtime.request_recovery(config());
        let next = tokio::time::timeout(Duration::from_secs(1), runtime.next_recovery_request());
        assert_eq!(next.await.unwrap(), config());
        let none =
            tokio::time::timeout(Duration::from_millis(100), runtime.next_recovery_request());
        assert!(none.await.is_err());
    }

    #[tokio::test]
    async fn a_missing_binary_reports_not_installed() {
        let home = Home::new();
        let runtime = ManagedEndpointRuntime::open(home.options(None)).await;
        let status = runtime.apply_config(Some(config())).await;
        assert_eq!(
            status,
            json!({
                "status": "failed",
                "providerKind": "cloudflare_tunnel",
                "failure": "not-installed",
                "reason": "The relay client is not installed.",
                "tunnelId": "00000000-0000-4000-8000-00000000c0de",
                "tunnelName": "t3coderelay-managedendpoint-test",
            })
        );
        assert!(home.log().is_empty());
    }

    /// The spawn the reference makes with the same fake, captured by
    /// `scripts/relay_client_parity.py --dump` (see the fixture's note).
    #[tokio::test]
    async fn stored_credentials_start_the_connector_as_the_reference_does() {
        let home = Home::new();
        home.seed(4000);
        let runtime = home.runtime().await;
        assert_eq!(
            runtime.start_if_origin_confirmed(&origin(4000), true).await,
            Ok(true)
        );
        wait_for("registration", || {
            home.messages()
                .contains(&"Relay client tunnel connection registered".to_owned())
        })
        .await;
        let starts = home.starts();
        assert_eq!(starts.len(), 1);
        let fixture: Value =
            serde_json::from_str(include_str!("../tests/relay-client-spawn.json")).unwrap();
        let mut spawned = starts[0].clone();
        assert_eq!(spawned["tunnelToken"], TOKEN);
        spawned["tunnelToken"] = json!("<token>");
        for key in ["pid", "cwd", "event"] {
            spawned.as_object_mut().unwrap().remove(key);
        }
        let mut expected = fixture["spawn"].clone();
        let ignored: Vec<String> =
            serde_json::from_value(fixture["referenceProcessEnv"].clone()).unwrap();
        expected["envAdded"] = json!(expected["envAdded"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|name| !ignored.iter().any(|i| i == name.as_str().unwrap()))
            .collect::<Vec<_>>());
        assert_eq!(spawned, expected);

        let pid = starts[0]["pid"].as_u64().unwrap();
        let status = runtime.status().await;
        assert_eq!(
            status,
            json!({
                "status": "running",
                "providerKind": "cloudflare_tunnel",
                "pid": pid,
                "tunnelId": "00000000-0000-4000-8000-00000000c0de",
                "tunnelName": "t3coderelay-managedendpoint-test",
            })
        );
        assert_eq!(runtime.connector().await, Some((pid as u32, true)));
        // Applying the same tunnel again keeps the running connector.
        assert_eq!(runtime.apply_config(Some(config())).await, status);
        let ledger = home
            .paths()
            .state_dir
            .join(format!("relay-clients/{pid}.json"));
        let entry: Value =
            serde_json::from_str(&std::fs::read_to_string(&ledger).unwrap()).unwrap();
        assert_eq!(entry["version"], 1);
        assert_eq!(entry["client"]["pid"], pid);
        assert_eq!(entry["owner"]["pid"], std::process::id());

        let log = home.log();
        assert_eq!(
            log[0]["message"],
            json!([
                "Relay client process started; waiting for tunnel connection",
                {
                    "pid": pid,
                    "source": "override",
                    "version": "2026.10.4",
                    "protocol": "auto",
                    "tunnelId": "00000000-0000-4000-8000-00000000c0de",
                    "tunnelName": "t3coderelay-managedendpoint-test",
                }
            ])
        );
        let keys: Vec<&str> = log[0]
            .as_object()
            .unwrap()
            .keys()
            .map(String::as_str)
            .collect();
        assert_eq!(
            keys,
            [
                "message",
                "level",
                "timestamp",
                "annotations",
                "spans",
                "fiberId"
            ]
        );
        assert_eq!(log[0]["level"], "INFO");
        let registered = &log[1]["message"][1];
        assert_eq!(
            registered.as_object().unwrap().keys().collect::<Vec<_>>(),
            ["pid", "tunnelId", "tunnelName", "output"]
        );
        assert!(registered["output"]
            .as_str()
            .unwrap()
            .contains("INF Registered tunnel connection"));
        assert!(
            !std::fs::read_to_string(home.paths().logs.join(RELAY_CLIENT_LOG_FILE))
                .unwrap()
                .contains(TOKEN)
        );

        runtime.shutdown().await;
        assert!(!alive(pid));
        assert!(!ledger.exists());
        assert_eq!(runtime.status().await, json!({"status": "disabled"}));
        assert_eq!(home.messages().last().unwrap(), "Relay client stopped");
        assert_eq!(
            home.log().last().unwrap()["message"][1],
            json!({"pid": pid})
        );
        let signals: Vec<Value> = home
            .records()
            .into_iter()
            .filter(|r| r["event"] == "signal")
            .collect();
        assert_eq!(signals.len(), 1);
        assert_eq!(signals[0]["signal"], "SIGTERM");
    }

    #[tokio::test]
    async fn an_exited_connector_restarts_with_backoff() {
        let home = Home::new();
        std::fs::write(
            home.binary.with_file_name("fake-cloudflared-options.json"),
            r#"{"exitAfter": 0.3, "exitTimes": 2}"#,
        )
        .unwrap();
        let runtime = home.runtime().await;
        let first = runtime.apply_config(Some(config())).await;
        assert_eq!(first["status"], "running");
        wait_for("two restarts", || home.starts().len() == 3).await;
        wait_for("the third registration", || {
            home.messages()
                .iter()
                .filter(|m| *m == "Relay client tunnel connection registered")
                .count()
                == 3
        })
        .await;
        // The fake logs an ERR line as it exits; reading it races the exit, as in the reference.
        let messages: Vec<String> = home
            .messages()
            .into_iter()
            .filter(|m| m != "Relay client reported a transport warning")
            .collect();
        let registered = "Relay client tunnel connection registered";
        assert_eq!(
            messages,
            [
                "Relay client process started; waiting for tunnel connection",
                registered,
                "Relay client stopped",
                "Relay client exited; restarting",
                "Relay client process started; waiting for tunnel connection",
                registered,
                "Relay client is crash-looping; delaying restart",
                "Relay client stopped",
                "Relay client exited; restarting",
                "Relay client process started; waiting for tunnel connection",
                registered,
            ]
            .map(str::to_owned)
        );
        let log = home.log();
        let find = |text: &str| -> Vec<&Value> {
            log.iter().filter(|e| e["message"][0] == text).collect()
        };
        let exited = find("Relay client exited; restarting");
        assert_eq!(exited[0]["message"][1]["exitCode"], 3);
        assert_eq!(exited[0]["level"], "WARN");
        assert_eq!(
            exited[0]["message"][1]
                .as_object()
                .unwrap()
                .keys()
                .collect::<Vec<_>>(),
            ["pid", "exitCode", "tunnelId", "tunnelName"]
        );
        let delayed = find("Relay client is crash-looping; delaying restart");
        assert_eq!(delayed[0]["message"][1]["restartDelayMillis"], 200);
        let third = home.starts()[2]["pid"].as_u64().unwrap();
        let status = runtime.status().await;
        assert_eq!(status["status"], "running");
        assert_eq!(status["pid"], third);
        runtime.shutdown().await;
        assert!(!alive(third));
        // A stopped runtime never restarts the connector.
        tokio::time::sleep(Duration::from_millis(500)).await;
        assert_eq!(home.starts().len(), 3);
    }

    #[tokio::test]
    async fn changing_or_clearing_the_tunnel_moves_or_stops_the_connector() {
        let home = Home::new();
        let runtime = home.runtime().await;
        let first = runtime.apply_config(Some(config())).await;
        let mut replacement = config();
        replacement.connector_token = format!("{TOKEN}x");
        replacement.tunnel_id = None;
        let second = runtime.apply_config(Some(replacement)).await;
        assert_ne!(first["pid"], second["pid"]);
        assert!(!alive(first["pid"].as_u64().unwrap()));
        assert_eq!(second.get("tunnelId"), None);
        let unsupported = runtime
            .apply_config(Some(RuntimeConfig {
                provider_kind: "manual".into(),
                ..config()
            }))
            .await;
        assert_eq!(
            unsupported,
            json!({"status": "unsupported", "providerKind": "manual"})
        );
        assert!(!alive(second["pid"].as_u64().unwrap()));
        assert_eq!(
            runtime.apply_config(None).await,
            json!({"status": "disabled"})
        );
    }

    #[tokio::test]
    async fn a_remembered_http2_fallback_starts_with_http2() {
        let home = Home::new();
        home.secret(
            HTTP2_FALLBACK_STATE,
            &json!({"fellBackAt": now_millis() as u64 - 1000}).to_string(),
        );
        let runtime = home.runtime().await;
        runtime.apply_config(Some(config())).await;
        wait_for("a start", || !home.starts().is_empty()).await;
        let argv = home.starts()[0]["argv"].clone();
        assert_eq!(
            argv,
            json!([
                "tunnel",
                "--no-autoupdate",
                "--loglevel",
                "info",
                "--output",
                "default",
                "--metrics",
                "127.0.0.1:0",
                "--protocol",
                "http2",
                "run"
            ])
        );
        runtime.shutdown().await;
    }

    #[tokio::test]
    async fn the_next_server_stops_a_connector_its_crashed_owner_left() {
        let home = Home::new();
        let mut orphan = std::process::Command::new(&home.binary)
            .args(["tunnel", "run"])
            .stdin(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .unwrap();
        let pid = orphan.id();
        wait_for("the orphan", || !home.starts().is_empty()).await;
        let observed = observe(pid).unwrap();
        let directory = home.paths().state_dir.join("relay-clients");
        std::fs::create_dir_all(&directory).unwrap();
        // No process can have this pid, so the owner reads as gone.
        let entry = json!({
            "version": 1,
            "client": {"pid": pid, "startTime": observed.start_time, "command": observed.command},
            "owner": {"pid": u32::MAX - 1, "startTime": "1"},
        });
        std::fs::write(directory.join(format!("{pid}.json")), entry.to_string()).unwrap();
        // An entry whose owner still runs is left alone.
        let live_owner = observe(std::process::id()).unwrap();
        let kept = json!({
            "version": 1,
            "client": {"pid": pid, "startTime": observed.start_time, "command": observed.command},
            "owner": {"pid": std::process::id(), "startTime": live_owner.start_time},
        });
        std::fs::write(directory.join("1.json"), kept.to_string()).unwrap();
        let _runtime = home.runtime().await;
        let _ = orphan.wait();
        assert!(!directory.join(format!("{pid}.json")).exists());
        assert!(directory.join("1.json").exists());
    }

    #[test]
    fn decoding_follows_the_contract() {
        assert_eq!(
            config().tunnel_name.as_deref(),
            Some("t3coderelay-managedendpoint-test")
        );
        assert!(RuntimeConfig::decode(r#"{"providerKind":"cloudflare_tunnel"}"#).is_none());
        assert!(RuntimeConfig::decode(
            r#"{"providerKind":"cloudflare_tunnel","connectorToken":"  "}"#
        )
        .is_none());
        assert!(RuntimeConfig::decode(
            r#"{"providerKind":"cloudflare_tunnel","connectorToken":"t","tunnelId":null}"#
        )
        .is_none());
        assert!(
            RuntimeConfig::decode(r#"{"providerKind":"other","connectorToken":"t"}"#).is_none()
        );
        assert_eq!(
            LocalOrigin::for_address("0.0.0.0:3773".parse().unwrap()),
            origin(3773)
        );
        assert_eq!(
            LocalOrigin::for_address("[::1]:3773".parse().unwrap()).host,
            "[::1]"
        );
        assert_eq!(
            classify("2026-11-20T00:00:00Z INF Registered tunnel connection connIndex=0"),
            "connected"
        );
        assert_eq!(
            classify(
                "x ERR Request failed error=\"Incoming request ended abruptly: context canceled\""
            ),
            "debug"
        );
        assert_eq!(classify("x WRN Serve tunnel error"), "warning");
        assert_eq!(
            classify("x INF Unregistered tunnel connection connIndex=0"),
            "unregistered"
        );
        assert!(is_rejected(
            "x ERR Register tunnel error from server side error=\"Unauthorized: Invalid tunnel secret\""
        ));
    }
}
