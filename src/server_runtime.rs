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
    io::{self, Write},
    net::{IpAddr, SocketAddr},
    path::{Path, PathBuf},
    time::Duration,
};

use rusqlite::{Connection, ErrorCode};
use serde::Serialize;
use serde_json::Value;
use time::{format_description::FormatItem, macros::format_description, OffsetDateTime};
use tracing::warn;

/// `SERVER_EXIT_CODE_STATE_DIR_OWNED` in `@t3tools/contracts`. Supervisors use
/// it to tell "owned by another server" apart from a crash.
pub const EXIT_CODE_STATE_DIR_OWNED: i32 = 78;

/// The TypeScript release this server reports parity with. Kept in step with
/// the environment descriptor's `serverVersion`.
pub const SERVER_VERSION: &str = "0.1.10";

pub const OWNER_LOCK_FILE: &str = "server-owner.sqlite";
pub const RUNTIME_STATE_FILE: &str = "server-runtime.json";

/// Another server (TypeScript or Rust) holds the ownership lock.
#[derive(Debug)]
pub struct ServerAlreadyRunningError {
    pub state_dir: PathBuf,
}

impl fmt::Display for ServerAlreadyRunningError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "A T3 Code server already owns {}. Stop that server, or use a separate T3 home and pair with it, then retry. No server was stopped.",
            self.state_dir.display()
        )
    }
}

impl std::error::Error for ServerAlreadyRunningError {}

/// `PersistedServerRuntimeState` with the fields the deployed 0.1.10 writes,
/// in the same order. Optional fields are omitted rather than written as
/// null, matching the TypeScript object spreads.
#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RuntimeRecord {
    pub version: u8,
    pub pid: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub host: Option<String>,
    pub port: u16,
    pub origin: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dev_url: Option<String>,
    pub started_at: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub service_managed: Option<bool>,
    pub server_version: String,
    pub managed_policy: Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub owner_id: Option<String>,
}

const STARTED_AT: &[FormatItem<'static>] =
    format_description!("[year]-[month]-[day]T[hour]:[minute]:[second].[subsecond digits:3]Z");

impl RuntimeRecord {
    /// Describe this process listening on `bound`, the listener's real
    /// address (so a requested port 0 records the port the OS picked).
    pub fn for_listener(bound: SocketAddr) -> Self {
        Self {
            version: 1,
            pid: std::process::id(),
            host: Some(bound.ip().to_string()),
            port: bound.port(),
            origin: runtime_origin(bound.ip(), bound.port()),
            dev_url: None,
            started_at: OffsetDateTime::now_utc()
                .format(STARTED_AT)
                .unwrap_or_default(),
            service_managed: None,
            server_version: SERVER_VERSION.to_owned(),
            managed_policy: Value::Null,
            owner_id: None,
        }
    }
}

/// `runtimeOriginForConfig`: a wildcard bind publishes the IPv4 loopback so
/// local clients (the desktop SSH launcher only adopts loopback origins)
/// reach it; a specific host is published as itself.
pub fn runtime_origin(ip: IpAddr, port: u16) -> String {
    match ip {
        ip if ip.is_unspecified() => format!("http://127.0.0.1:{port}"),
        IpAddr::V4(v4) => format!("http://{v4}:{port}"),
        IpAddr::V6(v6) => format!("http://[{v6}]:{port}"),
    }
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

/// Remove the runtime record only when it still names `owner_id`, so a
/// shutting-down server never deletes a successor's record.
/// Returns whether the file was removed.
pub fn clear_if_owner(path: &Path, owner_id: &str) -> io::Result<bool> {
    let raw = match fs::read_to_string(path) {
        Ok(raw) => raw,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(false),
        Err(e) => return Err(e),
    };
    let owned = serde_json::from_str::<Value>(raw.trim())
        .ok()
        .is_some_and(|v| v.get("ownerId").and_then(Value::as_str) == Some(owner_id));
    if !owned {
        return Ok(false);
    }
    match fs::remove_file(path) {
        Ok(()) => Ok(true),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(e) => Err(e),
    }
}

/// Exclusive ownership of a state directory for the life of this value.
/// Dropping it clears the runtime record (if still ours) and then releases
/// the lock.
pub struct ServerOwnership {
    owner_id: String,
    state_path: PathBuf,
    // Declared last so the record is cleared (in Drop) before the lock goes.
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
    /// (`<state dir>/server-runtime.json`). Fails with
    /// [`ServerAlreadyRunningError`] when another server holds it.
    pub fn acquire(state_path: impl Into<PathBuf>) -> anyhow::Result<Self> {
        let state_path = state_path.into();
        let state_dir = state_path
            .parent()
            .map(Path::to_path_buf)
            .unwrap_or_else(|| PathBuf::from("."));
        let context = || {
            format!(
                "Could not acquire server ownership of {}.",
                state_dir.display()
            )
        };
        fs::create_dir_all(&state_dir).map_err(|e| anyhow::Error::new(e).context(context()))?;
        let lock = Connection::open(state_dir.join(OWNER_LOCK_FILE))
            .map_err(|e| anyhow::Error::new(e).context(context()))?;
        // `PRAGMA busy_timeout = 0; BEGIN EXCLUSIVE;` in the reference. rusqlite
        // defaults to a 5 s busy timeout, which would make a second server
        // wait instead of failing immediately.
        let begin = lock
            .busy_timeout(Duration::ZERO)
            .and_then(|()| lock.execute_batch("BEGIN EXCLUSIVE;"));
        if let Err(error) = begin {
            if error.sqlite_error_code() == Some(ErrorCode::DatabaseBusy) {
                return Err(ServerAlreadyRunningError { state_dir }.into());
            }
            return Err(anyhow::Error::new(error).context(context()));
        }
        Ok(Self {
            owner_id: uuid::Uuid::new_v4().to_string(),
            state_path,
            lock: Some(lock),
        })
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

    #[test]
    fn origin_is_loopback_for_wildcard_binds() {
        assert_eq!(
            runtime_origin("0.0.0.0".parse().unwrap(), 3773),
            "http://127.0.0.1:3773"
        );
        assert_eq!(
            runtime_origin("::".parse().unwrap(), 3773),
            "http://127.0.0.1:3773"
        );
        assert_eq!(
            runtime_origin("127.0.0.1".parse().unwrap(), 4000),
            "http://127.0.0.1:4000"
        );
    }

    #[test]
    fn record_matches_typescript_shape() {
        let record = RuntimeRecord {
            owner_id: Some("owner".into()),
            ..RuntimeRecord::for_listener("0.0.0.0:3773".parse().unwrap())
        };
        let value = serde_json::to_value(&record).unwrap();
        let keys: Vec<_> = value.as_object().unwrap().keys().cloned().collect();
        assert_eq!(
            keys,
            [
                "version",
                "pid",
                "host",
                "port",
                "origin",
                "startedAt",
                "serverVersion",
                "managedPolicy",
                "ownerId"
            ]
        );
        assert_eq!(value["version"], 1);
        assert_eq!(value["pid"], std::process::id());
        assert_eq!(value["host"], "0.0.0.0");
        assert_eq!(value["port"], 3773);
        assert_eq!(value["origin"], "http://127.0.0.1:3773");
        assert!(value["managedPolicy"].is_null());
        let started = value["startedAt"].as_str().unwrap();
        assert_eq!(started.len(), "2026-11-20T00:15:37.888Z".len());
        assert!(started.ends_with('Z'));
    }

    #[test]
    fn publish_writes_and_atomically_replaces_record() {
        let dir = tempfile::tempdir().unwrap();
        let path = runtime_path(&dir);
        let owner = ServerOwnership::acquire(&path).unwrap();
        owner
            .publish(RuntimeRecord::for_listener(
                "127.0.0.1:4000".parse().unwrap(),
            ))
            .unwrap();
        let first: Value = serde_json::from_str(&fs::read_to_string(&path).unwrap()).unwrap();
        assert_eq!(first["port"], 4000);
        assert_eq!(first["ownerId"], owner.owner_id());

        // Hold the old inode open; an in-place rewrite would change what it
        // reads, a rename leaves it intact.
        let before = fs::read(&path).unwrap();
        let held = fs::File::open(&path).unwrap();
        owner
            .publish(RuntimeRecord::for_listener(
                "127.0.0.1:4001".parse().unwrap(),
            ))
            .unwrap();
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
        owner
            .publish(RuntimeRecord::for_listener(
                "127.0.0.1:4000".parse().unwrap(),
            ))
            .unwrap();
        drop(owner);
        assert!(!path.exists(), "own record should be removed");

        let owner = ServerOwnership::acquire(&path).unwrap();
        owner
            .publish(RuntimeRecord::for_listener(
                "127.0.0.1:4000".parse().unwrap(),
            ))
            .unwrap();
        let foreign = r#"{"version":1,"pid":1,"port":4000,"origin":"http://127.0.0.1:4000","startedAt":"x","ownerId":"someone-else"}"#;
        write_atomically(&path, foreign.as_bytes()).unwrap();
        drop(owner);
        assert_eq!(fs::read_to_string(&path).unwrap(), foreign);

        assert!(!clear_if_owner(&path, "not-it").unwrap());
        assert!(clear_if_owner(&path, "someone-else").unwrap());
        assert!(!clear_if_owner(&path, "someone-else").unwrap());
    }

    #[test]
    fn lock_rejects_a_second_holder_until_released() {
        let dir = tempfile::tempdir().unwrap();
        let path = runtime_path(&dir);
        let first = ServerOwnership::acquire(&path).unwrap();
        let second = ServerOwnership::acquire(&path).unwrap_err();
        let refused = second
            .downcast_ref::<ServerAlreadyRunningError>()
            .unwrap_or_else(|| panic!("unexpected error: {second:#}"));
        assert_eq!(refused.state_dir, path.parent().unwrap());
        assert!(second.to_string().contains("already owns"));
        drop(first);
        let third = ServerOwnership::acquire(&path).unwrap();
        assert!(dir.path().join("userdata").join(OWNER_LOCK_FILE).exists());
        drop(third);
        // The lock file is never unlinked.
        assert!(dir.path().join("userdata").join(OWNER_LOCK_FILE).exists());
    }
}
