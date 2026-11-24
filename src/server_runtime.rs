//! Single-server ownership of a T3 home and the published runtime record.
//!
//! Mirrors `apps/server/src/serverOwnership.ts` and `serverRuntimeState.ts`
//! in the TypeScript reference so that the Rust and TypeScript servers
//! exclude each other on the same state directory, and so that the desktop
//! SSH launcher can discover and adopt a running Rust server.
//!
//! The lock is an exclusive SQLite transaction on `server-owner.sqlite` in the
//! state directory. That file never holds data and must never be unlinked:
//! the OS drops the POSIX advisory lock when the process exits, even on
//! SIGKILL, so a crashed owner never blocks the next start.

use std::{
    fmt, fs,
    io::{self, Read, Write},
    net::IpAddr,
    path::{Path, PathBuf},
    process::Stdio,
    sync::mpsc,
    thread,
    time::{Duration, Instant},
};

use rusqlite::{Connection, ErrorCode};
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;
use time::{
    format_description::{well_known::Rfc3339, FormatItem},
    macros::format_description,
    OffsetDateTime, PrimitiveDateTime,
};
use tracing::{error, warn};

/// `SERVER_EXIT_CODE_STATE_DIR_OWNED` in `@t3tools/contracts`. Supervisors use
/// it to tell "owned by another server" apart from a crash.
pub const EXIT_CODE_STATE_DIR_OWNED: i32 = 78;

/// `packageJson.version` of `apps/server` in the TypeScript reference this
/// port is checked against: pingdotgg/t3code at
/// fd0331ca9b1596ed7018c80f52b7e6730014f593 (`.github/workflows/parity.yml`),
/// which is after 0.1.11 and before 0.1.12. Kept in step with the environment
/// descriptor's `serverVersion`; change both when the pin moves.
pub const SERVER_VERSION: &str = "0.1.11";

pub const OWNER_LOCK_FILE: &str = "server-owner.sqlite";
pub const RUNTIME_STATE_FILE: &str = "server-runtime.json";

/// How long taking the lock waits for a holder to let go, as the
/// reference's `PRAGMA busy_timeout = 250`: an idle service launcher probes
/// the lock for a moment, and a start during that probe must not fail.
pub const OWNER_LOCK_BUSY_TIMEOUT: Duration = Duration::from_millis(250);

/// How often a running server checks that its lock file is still the one it
/// locked.
const LOCK_CHECK_INTERVAL: Duration = Duration::from_secs(5);

/// Another server (TypeScript or Rust) holds the ownership lock, or a
/// pre-lock server is still running on the state directory.
#[derive(Debug)]
pub struct ServerAlreadyRunningError {
    pub state_dir: PathBuf,
}

impl fmt::Display for ServerAlreadyRunningError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "A T3 Code server already owns {}. Stop that server, or use a separate T3 home and pair with it, then retry. No server was stopped. server-owner.sqlite is the ownership lock; do not delete it while a server is running.",
            self.state_dir.display()
        )
    }
}

impl std::error::Error for ServerAlreadyRunningError {}

/// `PersistedServerRuntimeState` as `makePersistedServerRuntimeState` builds
/// it, in the same key order. Optional fields are omitted rather than
/// written as null, matching the TypeScript object spreads. `ownerId` comes
/// last because the reference's `publish` adds it with `{ ...state, ownerId }`.
#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RuntimeRecord {
    pub version: u8,
    pub pid: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub host: Option<String>,
    /// Written only as `true`. This server has no private-network-only
    /// startup mode, so it is never set.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub private_network_only: Option<bool>,
    pub port: u16,
    pub origin: String,
    /// The single dev origin browsers pair through. This server never runs
    /// behind a dev server, so it is never set.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dev_url: Option<String>,
    pub started_at: String,
    /// Written only as `true`, when a `t3` service launcher supervises the
    /// server over its IPC protocol, which this server does not speak.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub service_managed: Option<bool>,
    pub server_version: String,
    /// `null` for no policy, `{source, organization, fingerprint}` for a
    /// loaded one; omitted when unknown.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub managed_policy: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub owner_id: Option<String>,
}

const STARTED_AT: &[FormatItem<'static>] =
    format_description!("[year]-[month]-[day]T[hour]:[minute]:[second].[subsecond digits:3]Z");

impl RuntimeRecord {
    /// `makePersistedServerRuntimeState` for this process. `host` is the
    /// configured host (`None` when the default bind was used), `port` the
    /// listener's real port (so a requested port 0 records the one the OS
    /// picked) and `managed_policy` the `loadedPolicy` value.
    pub fn new(host: Option<IpAddr>, port: u16, managed_policy: Option<Value>) -> Self {
        Self {
            version: 1,
            pid: std::process::id(),
            host: host.map(|host| host.to_string()),
            private_network_only: None,
            port,
            origin: runtime_origin(host, port),
            dev_url: None,
            started_at: OffsetDateTime::now_utc()
                .format(STARTED_AT)
                .unwrap_or_default(),
            service_managed: None,
            server_version: SERVER_VERSION.to_owned(),
            managed_policy,
            owner_id: None,
        }
    }
}

/// `runtimeOriginForConfig`: an unset or wildcard host publishes the IPv4
/// loopback so local clients (the desktop SSH launcher only adopts loopback
/// origins) reach it; a specific host is published as itself, with IPv6
/// addresses bracketed as `formatHostForUrl` does.
pub fn runtime_origin(host: Option<IpAddr>, port: u16) -> String {
    match host {
        Some(IpAddr::V4(v4)) if !v4.is_unspecified() => format!("http://{v4}:{port}"),
        Some(IpAddr::V6(v6)) if !v6.is_unspecified() => format!("http://[{v6}]:{port}"),
        _ => format!("http://127.0.0.1:{port}"),
    }
}

/// `Schema.optional`: the key may be absent, but `null` is not a value.
fn present<'de, D: Deserializer<'de>, T: Deserialize<'de>>(d: D) -> Result<Option<T>, D::Error> {
    T::deserialize(d).map(Some)
}

/// A runtime record as the reference's schema decodes it. Every field is
/// declared so that a record the reference would reject is rejected here
/// too, even where this server reads only a few of them.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
struct PersistedRecord {
    version: u8,
    pid: i64,
    #[serde(default, deserialize_with = "present")]
    owner_id: Option<String>,
    #[serde(default, deserialize_with = "present")]
    host: Option<String>,
    #[serde(default, deserialize_with = "present")]
    private_network_only: Option<bool>,
    port: i64,
    origin: String,
    #[serde(default, deserialize_with = "present")]
    dev_url: Option<String>,
    started_at: String,
    #[serde(default, deserialize_with = "present")]
    service_managed: Option<bool>,
    #[serde(default, deserialize_with = "present")]
    server_version: Option<String>,
    #[serde(default, deserialize_with = "present")]
    managed_policy: Option<Option<LoadedPolicy>>,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct LoadedPolicy {
    source: String,
    organization: String,
    fingerprint: String,
}

/// `readPersistedServerRuntimeState`: `None` when the file is missing,
/// empty or does not decode. Read errors other than a missing file are
/// returned so the caller can report them.
fn read_record(path: &Path) -> io::Result<Option<PersistedRecord>> {
    let raw = match fs::read_to_string(path) {
        Ok(raw) => raw,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e),
    };
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }
    match serde_json::from_str::<PersistedRecord>(trimmed) {
        Ok(record) if record.version == 1 => Ok(Some(record)),
        Ok(_) => Ok(None),
        Err(error) => {
            warn!(path = %path.display(), %error, "failed to decode server runtime state");
            Ok(None)
        }
    }
}

/// `isProcessAlive`: signal 0 only probes existence; EPERM still means alive.
#[cfg(unix)]
fn is_process_alive(pid: i64) -> bool {
    let Ok(pid) = libc::pid_t::try_from(pid) else {
        return false;
    };
    if pid <= 0 {
        return false;
    }
    // SAFETY: kill with signal 0 sends nothing; it only checks the pid.
    if unsafe { libc::kill(pid, 0) } == 0 {
        return true;
    }
    io::Error::last_os_error().raw_os_error() == Some(libc::EPERM)
}

/// Without a liveness probe a stale pre-lock record would block every start,
/// so on other platforms a pre-lock owner is not detected.
#[cfg(not(unix))]
fn is_process_alive(_pid: i64) -> bool {
    false
}

/// `legacyOwnerStartTimeCommand`: the start time of `pid` from `ps`, in
/// UTC, or `None` when `ps` cannot say within two seconds.
fn process_start_time(pid: i64) -> Option<String> {
    let mut child = crate::login_shell::command("ps")
        .args(["-p", &pid.to_string(), "-o", "lstart="])
        .env("LC_ALL", "C")
        .env("TZ", "UTC")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .ok()?;
    let deadline = Instant::now() + Duration::from_secs(2);
    let status = loop {
        match child.try_wait() {
            Ok(Some(status)) => break status,
            Ok(None) if Instant::now() < deadline => thread::sleep(Duration::from_millis(10)),
            _ => {
                let _ = child.kill();
                let _ = child.wait();
                return None;
            }
        }
    };
    let mut output = String::new();
    child
        .stdout
        .take()?
        .take(16_384)
        .read_to_string(&mut output)
        .ok()?;
    status.success().then_some(output)
}

/// `legacyOwnerStartedBeforeRecord`: `ps` reports whole seconds, and an
/// unknown start time stays conservative (the owner counts as live).
fn started_before_record(output: Option<&str>, recorded_at: OffsetDateTime) -> bool {
    const LSTART: &[FormatItem<'static>] = format_description!(
        "[weekday repr:short] [month repr:short] [day padding:none] [hour]:[minute]:[second] [year]"
    );
    let Some(output) = output else {
        return true;
    };
    let normalized = output.split_whitespace().collect::<Vec<_>>().join(" ");
    match PrimitiveDateTime::parse(&normalized, LSTART) {
        Ok(started) => started.assume_utc() <= recorded_at + Duration::from_secs(1),
        Err(_) => true,
    }
}

/// `legacyOwnerIsLive`: a pre-lock server publishes only its runtime record,
/// so a record without an `ownerId` whose process is alive blocks the start
/// unless the process demonstrably started after the record (PID reuse).
fn legacy_owner_is_live(record: &PersistedRecord) -> bool {
    if !is_process_alive(record.pid) {
        return false;
    }
    let Ok(recorded_at) = OffsetDateTime::parse(&record.started_at, &Rfc3339) else {
        return true;
    };
    started_before_record(process_start_time(record.pid).as_deref(), recorded_at)
}

/// Write `contents` to `path` through a temporary file in the same directory
/// and a rename, so readers never observe a partial record.
pub fn write_atomically(path: &Path, contents: &[u8]) -> io::Result<()> {
    let target = fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf());
    let dir = target
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(dir)?;
    let name = target
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_default();
    let mut temp = tempfile::Builder::new()
        .prefix(&format!(".{name}."))
        .suffix(".tmp")
        .tempfile_in(dir)?;
    temp.write_all(contents)?;
    temp.as_file().sync_all()?;
    temp.persist(&target).map_err(|e| e.error)?;
    Ok(())
}

/// Remove the runtime record only when it decodes and still names
/// `owner_id`, so a shutting-down server never deletes a successor's record.
/// Returns whether the file was removed.
pub fn clear_if_owner(path: &Path, owner_id: &str) -> io::Result<bool> {
    let owned = read_record(path)?.is_some_and(|r| r.owner_id.as_deref() == Some(owner_id));
    if !owned {
        return Ok(false);
    }
    match fs::remove_file(path) {
        Ok(()) => Ok(true),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(e) => Err(e),
    }
}

/// Which file a path names, to notice the lock file being replaced.
#[cfg(unix)]
type FileIdentity = (u64, u64);
#[cfg(not(unix))]
type FileIdentity = ();

#[cfg(unix)]
fn file_identity(metadata: &fs::Metadata) -> FileIdentity {
    use std::os::unix::fs::MetadataExt;
    (metadata.dev(), metadata.ino())
}
#[cfg(not(unix))]
fn file_identity(_metadata: &fs::Metadata) -> FileIdentity {}

/// Whether the lock file at `path` is gone or is no longer `identity`.
/// `Err` when it cannot be checked. Stats by path only: opening and closing
/// another descriptor for the inode would release SQLite's POSIX lock.
fn lock_lost(path: &Path, identity: &FileIdentity) -> io::Result<bool> {
    match fs::metadata(path) {
        Ok(metadata) => Ok(file_identity(&metadata) != *identity),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(true),
        Err(e) => Err(e),
    }
}

/// Logs an error once the lock file is deleted or replaced, after which
/// another server could take a lock of its own on this home.
struct LockWatcher {
    stop: Option<mpsc::Sender<()>>,
    thread: Option<thread::JoinHandle<()>>,
}

impl LockWatcher {
    fn spawn(path: PathBuf, identity: FileIdentity, replaced_during_acquire: bool) -> Self {
        let (stop, stopped) = mpsc::channel::<()>();
        let thread = thread::Builder::new()
            .name("t3-owner-lock".into())
            .spawn(move || {
                let mut lost = replaced_during_acquire;
                let mut warned = false;
                while !lost {
                    if stopped.recv_timeout(LOCK_CHECK_INTERVAL)
                        != Err(mpsc::RecvTimeoutError::Timeout)
                    {
                        return;
                    }
                    match lock_lost(&path, &identity) {
                        Ok(now_lost) => {
                            warned = false;
                            lost = now_lost;
                        }
                        Err(error) => {
                            if !warned {
                                warn!(path = %path.display(), %error, "cannot check server ownership lock");
                                warned = true;
                            }
                        }
                    }
                }
                error!(
                    "Server ownership lock at {} was deleted or replaced. Another server may now share this T3 home. Stop any other server using this home, then restart this server. Do not delete server-owner.sqlite while a server is running.",
                    path.display()
                );
            })
            .ok();
        Self {
            stop: Some(stop),
            thread,
        }
    }
}

impl Drop for LockWatcher {
    fn drop(&mut self) {
        drop(self.stop.take());
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

/// The open lock: an exclusive transaction on `server-owner.sqlite`.
struct OwnershipLock {
    connection: Connection,
    identity: FileIdentity,
    replaced_during_acquire: bool,
}

/// `openOwnershipLock`. The lock file never holds data and is never
/// unlinked: the OS releases the lock when the process exits, even on
/// SIGKILL, so a crashed owner never blocks the next start.
fn open_ownership_lock(state_dir: &Path) -> anyhow::Result<OwnershipLock> {
    let failed = |error: anyhow::Error| {
        error.context(format!(
            "Could not acquire server ownership of {}.",
            state_dir.display()
        ))
    };
    let lock_path = state_dir.join(OWNER_LOCK_FILE);
    let open = || -> io::Result<fs::Metadata> {
        fs::create_dir_all(state_dir)?;
        match fs::metadata(&lock_path) {
            Ok(before) => Ok(before),
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                let mut options = fs::OpenOptions::new();
                options.append(true).create(true);
                #[cfg(unix)]
                std::os::unix::fs::OpenOptionsExt::mode(&mut options, 0o644);
                drop(options.open(&lock_path)?);
                fs::metadata(&lock_path)
            }
            Err(e) => Err(e),
        }
    };
    let before = open().map_err(|e| failed(e.into()))?;
    let connection = Connection::open(&lock_path).map_err(|e| failed(e.into()))?;
    let begin = connection
        .busy_timeout(OWNER_LOCK_BUSY_TIMEOUT)
        .and_then(|()| connection.execute_batch("BEGIN EXCLUSIVE;"));
    if let Err(error) = begin {
        if error.sqlite_error_code() == Some(ErrorCode::DatabaseBusy) {
            return Err(ServerAlreadyRunningError {
                state_dir: state_dir.to_path_buf(),
            }
            .into());
        }
        return Err(failed(error.into()));
    }
    let identity = fs::metadata(&lock_path)
        .map(|after| file_identity(&after))
        .map_err(|e| failed(e.into()))?;
    Ok(OwnershipLock {
        replaced_during_acquire: file_identity(&before) != identity,
        connection,
        identity,
    })
}

/// Exclusive ownership of a state directory for the life of this value
/// (`acquireServerOwnership`). Dropping it clears the runtime record (if
/// still ours) and then releases the lock.
pub struct ServerOwnership {
    owner_id: String,
    state_path: PathBuf,
    watcher: Option<LockWatcher>,
    // Taken in Drop after the record is cleared.
    lock: Option<Connection>,
}

impl fmt::Debug for ServerOwnership {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ServerOwnership")
            .field("owner_id", &self.owner_id)
            .field("state_path", &self.state_path)
            .finish()
    }
}

impl ServerOwnership {
    /// Take the lock for the directory containing `state_path`
    /// (`<state dir>/server-runtime.json`), waiting up to
    /// [`OWNER_LOCK_BUSY_TIMEOUT`] for a holder to let go. Fails with
    /// [`ServerAlreadyRunningError`] when another server holds it, or when
    /// the record names a live pre-lock server (one without an `ownerId`).
    pub fn acquire(state_path: impl Into<PathBuf>) -> anyhow::Result<Self> {
        let state_path = state_path.into();
        let state_dir = state_path
            .parent()
            .map(Path::to_path_buf)
            .unwrap_or_else(|| PathBuf::from("."));
        let owner_id = uuid::Uuid::new_v4().to_string();
        let lock = open_ownership_lock(&state_dir)?;
        let mut ownership = Self {
            owner_id,
            state_path,
            watcher: None,
            lock: Some(lock.connection),
        };
        let previous = read_record(&ownership.state_path).unwrap_or_else(|error| {
            warn!(path = %ownership.state_path.display(), %error, "failed to read server runtime state");
            None
        });
        if previous.is_some_and(|p| p.owner_id.is_none() && legacy_owner_is_live(&p)) {
            return Err(ServerAlreadyRunningError { state_dir }.into());
        }
        ownership.watcher = Some(LockWatcher::spawn(
            state_dir.join(OWNER_LOCK_FILE),
            lock.identity,
            lock.replaced_during_acquire,
        ));
        Ok(ownership)
    }

    pub fn owner_id(&self) -> &str {
        &self.owner_id
    }

    pub fn state_path(&self) -> &Path {
        &self.state_path
    }

    /// Atomically publish `record`, stamped with this owner's id.
    pub fn publish(&self, mut record: RuntimeRecord) -> io::Result<()> {
        record.owner_id = Some(self.owner_id.clone());
        let mut json = serde_json::to_vec(&record).map_err(io::Error::other)?;
        json.push(b'\n');
        write_atomically(&self.state_path, &json)
    }
}

impl Drop for ServerOwnership {
    fn drop(&mut self) {
        drop(self.watcher.take());
        if let Err(error) = clear_if_owner(&self.state_path, &self.owner_id) {
            warn!(path = %self.state_path.display(), %error, "failed to clear server runtime state");
        }
        // Closing the connection rolls back the empty transaction and drops
        // the file lock.
        drop(self.lock.take());
    }
}

/// Resolves on SIGINT or (on Unix) SIGTERM.
pub async fn shutdown_signal() {
    let interrupt = async {
        let _ = tokio::signal::ctrl_c().await;
    };
    #[cfg(unix)]
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut signal) => {
                signal.recv().await;
            }
            Err(_) => std::future::pending::<()>().await,
        }
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();
    tokio::select! {
        () = interrupt => {}
        () = terminate => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn runtime_path(dir: &tempfile::TempDir) -> PathBuf {
        dir.path().join("userdata").join(RUNTIME_STATE_FILE)
    }

    fn local(port: u16) -> RuntimeRecord {
        RuntimeRecord::new(Some("127.0.0.1".parse().unwrap()), port, None)
    }

    fn keys(record: &RuntimeRecord) -> Vec<String> {
        let value = serde_json::to_value(record).unwrap();
        value.as_object().unwrap().keys().cloned().collect()
    }

    #[test]
    fn origin_follows_run_time_origin_for_config() {
        let origin =
            |host: Option<&str>, port| runtime_origin(host.map(|h| h.parse().unwrap()), port);
        assert_eq!(origin(None, 3773), "http://127.0.0.1:3773");
        assert_eq!(origin(Some("0.0.0.0"), 3773), "http://127.0.0.1:3773");
        assert_eq!(origin(Some("::"), 3773), "http://127.0.0.1:3773");
        assert_eq!(origin(Some("127.0.0.1"), 4000), "http://127.0.0.1:4000");
        assert_eq!(origin(Some("192.168.1.5"), 4000), "http://192.168.1.5:4000");
        assert_eq!(origin(Some("::1"), 4000), "http://[::1]:4000");
    }

    #[test]
    fn record_matches_typescript_shape() {
        let policy = serde_json::json!({
            "source": "/etc/t3code/policy.json",
            "organization": "Example",
            "fingerprint": "0123456789ab",
        });
        let record = RuntimeRecord {
            private_network_only: Some(true),
            dev_url: Some("http://localhost:5733/".into()),
            service_managed: Some(true),
            owner_id: Some("owner".into()),
            ..RuntimeRecord::new(Some("0.0.0.0".parse().unwrap()), 3773, Some(policy.clone()))
        };
        // Every field, in makePersistedServerRuntimeState's order, then the
        // ownerId that publish appends.
        assert_eq!(
            keys(&record),
            [
                "version",
                "pid",
                "host",
                "privateNetworkOnly",
                "port",
                "origin",
                "devUrl",
                "startedAt",
                "serviceManaged",
                "serverVersion",
                "managedPolicy",
                "ownerId"
            ]
        );
        let value = serde_json::to_value(&record).unwrap();
        assert_eq!(value["version"], 1);
        assert_eq!(value["pid"], std::process::id());
        assert_eq!(value["host"], "0.0.0.0");
        assert_eq!(value["port"], 3773);
        assert_eq!(value["origin"], "http://127.0.0.1:3773");
        assert_eq!(value["serverVersion"], "0.1.11");
        assert_eq!(value["managedPolicy"], policy);
        let started = value["startedAt"].as_str().unwrap();
        assert_eq!(started.len(), "2026-11-20T00:15:37.888Z".len());
        assert!(started.ends_with('Z'));
        assert!(OffsetDateTime::parse(started, &Rfc3339).is_ok());
    }

    #[test]
    fn unset_optional_fields_are_omitted() {
        // No configured host, no policy information: only the required keys.
        let record = RuntimeRecord::new(None, 3773, None);
        assert_eq!(
            keys(&record),
            [
                "version",
                "pid",
                "port",
                "origin",
                "startedAt",
                "serverVersion"
            ]
        );
        assert_eq!(record.origin, "http://127.0.0.1:3773");

        // No policy is `null`, which the schema allows, not an absent key.
        let unmanaged = RuntimeRecord::new(None, 3773, Some(Value::Null));
        let value = serde_json::to_value(&unmanaged).unwrap();
        assert!(value["managedPolicy"].is_null());
        assert!(value.as_object().unwrap().contains_key("managedPolicy"));
        for absent in [
            "host",
            "privateNetworkOnly",
            "devUrl",
            "serviceManaged",
            "ownerId",
        ] {
            assert!(value.get(absent).is_none(), "{absent} should be omitted");
        }
    }

    #[test]
    fn publish_writes_and_atomically_replaces_record() {
        let dir = tempfile::tempdir().unwrap();
        let path = runtime_path(&dir);
        let owner = ServerOwnership::acquire(&path).unwrap();
        owner.publish(local(4000)).unwrap();
        let raw = fs::read_to_string(&path).unwrap();
        assert!(raw.ends_with("}\n"), "{raw:?}");
        let first: Value = serde_json::from_str(&raw).unwrap();
        assert_eq!(first["port"], 4000);
        assert_eq!(first["ownerId"], owner.owner_id());
        assert!(uuid::Uuid::parse_str(owner.owner_id())
            .is_ok_and(|id| id.get_version() == Some(uuid::Version::Random)));

        // Hold the old inode open; an in-place rewrite would change what it
        // reads, a rename leaves it intact.
        let before = fs::read(&path).unwrap();
        let held = fs::File::open(&path).unwrap();
        owner.publish(local(4001)).unwrap();
        let mut old = Vec::new();
        io::Read::read_to_end(&mut &held, &mut old).unwrap();
        assert_eq!(old, before);
        let second: Value = serde_json::from_str(&fs::read_to_string(&path).unwrap()).unwrap();
        assert_eq!(second["port"], 4001);
        let leftovers: Vec<_> = fs::read_dir(path.parent().unwrap())
            .unwrap()
            .flatten()
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .filter(|n| n.ends_with(".tmp"))
            .collect();
        assert!(leftovers.is_empty(), "{leftovers:?}");
    }

    #[test]
    fn drop_clears_only_our_own_record() {
        let dir = tempfile::tempdir().unwrap();
        let path = runtime_path(&dir);

        let owner = ServerOwnership::acquire(&path).unwrap();
        owner.publish(local(4000)).unwrap();
        drop(owner);
        assert!(!path.exists(), "own record should be removed");

        let owner = ServerOwnership::acquire(&path).unwrap();
        owner.publish(local(4000)).unwrap();
        let foreign = r#"{"version":1,"pid":1,"port":4000,"origin":"http://127.0.0.1:4000","startedAt":"x","ownerId":"someone-else"}"#;
        write_atomically(&path, foreign.as_bytes()).unwrap();
        drop(owner);
        assert_eq!(fs::read_to_string(&path).unwrap(), foreign);

        assert!(!clear_if_owner(&path, "not-it").unwrap());
        assert!(clear_if_owner(&path, "someone-else").unwrap());
        assert!(!clear_if_owner(&path, "someone-else").unwrap());
    }

    #[test]
    fn records_the_schema_rejects_are_not_cleared() {
        let dir = tempfile::tempdir().unwrap();
        let path = runtime_path(&dir);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        for invalid in [
            // Not the schema's version.
            r#"{"version":2,"pid":1,"port":4000,"origin":"o","startedAt":"x","ownerId":"me"}"#,
            // Missing origin.
            r#"{"version":1,"pid":1,"port":4000,"startedAt":"x","ownerId":"me"}"#,
            // Schema.optional allows an absent key, not null.
            r#"{"version":1,"pid":1,"port":4000,"origin":"o","startedAt":"x","host":null,"ownerId":"me"}"#,
            // managedPolicy may be null, but not some other shape.
            r#"{"version":1,"pid":1,"port":4000,"origin":"o","startedAt":"x","managedPolicy":{},"ownerId":"me"}"#,
        ] {
            fs::write(&path, invalid).unwrap();
            assert!(!clear_if_owner(&path, "me").unwrap(), "{invalid}");
            assert!(path.exists());
        }
        // Unknown keys are ignored, and a null managedPolicy decodes.
        fs::write(
            &path,
            r#"{"version":1,"pid":1,"port":4000,"origin":"o","startedAt":"x","managedPolicy":null,"extra":1,"ownerId":"me"}"#,
        )
        .unwrap();
        assert!(clear_if_owner(&path, "me").unwrap());
    }

    #[test]
    fn lock_waits_briefly_then_refuses_a_second_holder() {
        let dir = tempfile::tempdir().unwrap();
        let path = runtime_path(&dir);
        let first = ServerOwnership::acquire(&path).unwrap();
        let started = Instant::now();
        let second = ServerOwnership::acquire(&path).unwrap_err();
        let waited = started.elapsed();
        let refused = second
            .downcast_ref::<ServerAlreadyRunningError>()
            .unwrap_or_else(|| panic!("unexpected error: {second:#}"));
        assert_eq!(refused.state_dir, path.parent().unwrap());
        assert!(second.to_string().contains("already owns"));
        assert!(second.to_string().contains("do not delete it"));
        // Waited out the busy timeout, but not rusqlite's 5 s default.
        assert!(waited >= Duration::from_millis(200), "{waited:?}");
        assert!(waited < Duration::from_secs(3), "{waited:?}");
        drop(first);
        let third = ServerOwnership::acquire(&path).unwrap();
        assert!(dir.path().join("userdata").join(OWNER_LOCK_FILE).exists());
        drop(third);
        // The lock file is never unlinked.
        assert!(dir.path().join("userdata").join(OWNER_LOCK_FILE).exists());
    }

    #[test]
    fn lock_released_within_the_busy_timeout_is_taken() {
        let dir = tempfile::tempdir().unwrap();
        let path = runtime_path(&dir);
        let first = ServerOwnership::acquire(&path).unwrap();
        let releaser = thread::spawn(move || {
            thread::sleep(Duration::from_millis(20));
            drop(first);
        });
        let second = ServerOwnership::acquire(&path);
        releaser.join().unwrap();
        second.unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn lock_file_is_created_world_readable_and_watched_by_identity() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let path = runtime_path(&dir);
        let lock_path = dir.path().join("userdata").join(OWNER_LOCK_FILE);
        let owner = ServerOwnership::acquire(&path).unwrap();
        let metadata = fs::metadata(&lock_path).unwrap();
        // 0o644 before the umask, as the reference creates it.
        assert_eq!(metadata.permissions().mode() & 0o133, 0);
        let identity = file_identity(&metadata);
        assert!(!lock_lost(&lock_path, &identity).unwrap());
        fs::remove_file(&lock_path).unwrap();
        assert!(lock_lost(&lock_path, &identity).unwrap());
        fs::write(&lock_path, b"").unwrap();
        assert!(lock_lost(&lock_path, &identity).unwrap());
        drop(owner);
    }

    fn legacy_record(pid: i64, started_at: &str) -> String {
        format!(
            r#"{{"version":1,"pid":{pid},"port":3773,"origin":"http://127.0.0.1:3773","startedAt":"{started_at}"}}"#
        )
    }

    #[cfg(unix)]
    #[test]
    fn a_live_pre_lock_owner_blocks_the_start() {
        let dir = tempfile::tempdir().unwrap();
        let path = runtime_path(&dir);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        let now = OffsetDateTime::now_utc().format(STARTED_AT).unwrap();

        // This process is alive and started before the record: a pre-lock
        // server still running on this home.
        let pid = i64::from(std::process::id());
        fs::write(&path, legacy_record(pid, &now)).unwrap();
        let refused = ServerOwnership::acquire(&path).unwrap_err();
        assert!(refused.is::<ServerAlreadyRunningError>(), "{refused:#}");
        // The refused start leaves the record and the lock alone.
        assert!(path.exists());
        drop(ServerOwnership::acquire(dir.path().join("other").join(RUNTIME_STATE_FILE)).unwrap());

        // A live process that started after the record reuses the pid.
        fs::write(&path, legacy_record(pid, "2000-01-01T00:00:00.000Z")).unwrap();
        drop(ServerOwnership::acquire(&path).unwrap());

        // Records from lock-aware servers carry an ownerId; the lock decides.
        let owned = format!(
            r#"{{"version":1,"pid":{pid},"port":3773,"origin":"http://127.0.0.1:3773","startedAt":"{now}","ownerId":"previous"}}"#
        );
        fs::write(&path, owned).unwrap();
        drop(ServerOwnership::acquire(&path).unwrap());

        // A dead pre-lock owner does not block.
        let mut child = std::process::Command::new("true").spawn().unwrap();
        let dead = i64::from(child.id());
        child.wait().unwrap();
        fs::write(&path, legacy_record(dead, &now)).unwrap();
        drop(ServerOwnership::acquire(&path).unwrap());
    }

    #[test]
    fn ps_start_time_is_compared_with_a_second_of_slack() {
        let recorded = OffsetDateTime::parse("2026-11-20T07:56:25.500Z", &Rfc3339).unwrap();
        assert!(started_before_record(None, recorded));
        assert!(started_before_record(Some("garbage"), recorded));
        assert!(started_before_record(
            Some("Fri Nov 20 07:56:26 2026\n"),
            recorded
        ));
        assert!(!started_before_record(
            Some("Fri Nov 20 07:56:27 2026\n"),
            recorded
        ));
        // ps pads single-digit days with a space.
        let early = OffsetDateTime::parse("2026-11-02T00:00:00Z", &Rfc3339).unwrap();
        assert!(started_before_record(
            Some("Mon Nov  2 00:00:00 2026"),
            early
        ));
        assert!(!started_before_record(
            Some("Mon Nov  2 00:00:02 2026"),
            early
        ));
    }
}
