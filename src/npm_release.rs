//! `server.getNpmRelease`: the published `t3` npm release for a version or track.
//!
//! Mirrors `apps/server/src/cloud/npmRelease.ts` in the reference server. The configured npm reads
//! the release when it is installed, so a private registry or mirror in the user's npm config is
//! honored. Without npm there is no configured registry to honor, so the public registry answers.

use futures_util::future::{BoxFuture, FutureExt, Shared};
use indexmap::IndexMap;
use regex::Regex;
use serde_json::{json, Value};
use std::ffi::{OsStr, OsString};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio::sync::Semaphore;
use tokio::time::Instant;

pub(crate) const SPEC_PATTERN: &str = r"^(?:latest|nightly|\d+\.\d+\.\d+(?:-[0-9A-Za-z.-]+)?)$";
const VERSION_PATTERN: &str = r"^\d+\.\d+\.\d+(?:-[0-9A-Za-z.-]+)?$";
const PUBLIC_REGISTRY: &str = "https://registry.npmjs.org";
const CACHE_CAPACITY: usize = 32;
const CACHE_TTL: Duration = Duration::from_secs(60);
const LOOKUP_TIMEOUT: Duration = Duration::from_secs(15);
const MAX_OUTPUT_BYTES: usize = 256 * 1024;
/// npm loads the whole package history per lookup, so a burst of specs must not spawn one each.
const NPM_PERMITS: usize = 2;

pub(crate) const INVALID_METADATA: &str = "npm returned invalid release metadata.";
pub(crate) const INCOMPLETE_RESPONSE: &str = "npm returned an incomplete response.";
pub(crate) const UNREADABLE_METADATA: &str = "npm could not read release metadata.";
pub(crate) const LOOKUP_FAILED: &str = "npm could not complete the lookup.";
pub(crate) const REGISTRY_UNREACHABLE: &str =
    "npm is not installed and the public npm registry could not be reached.";

/// A release (`{version, databaseMigration?}`), `None` when unpublished, or a
/// `ServerNpmReleaseError` reason.
pub(crate) type Lookup = Result<Option<Value>, &'static str>;
type Pending = Shared<BoxFuture<'static, (Lookup, Instant)>>;

fn regex(cell: &'static OnceLock<Regex>, pattern: &str) -> &'static Regex {
    cell.get_or_init(|| Regex::new(pattern).expect("static pattern"))
}

pub(crate) fn is_valid_spec(spec: &str) -> bool {
    static SPEC: OnceLock<Regex> = OnceLock::new();
    regex(&SPEC, SPEC_PATTERN).is_match(spec)
}

fn is_version(value: &str) -> bool {
    static VERSION: OnceLock<Regex> = OnceLock::new();
    regex(&VERSION, VERSION_PATTERN).is_match(value)
}

/// `{version, t3code?}` with a well-formed version; other keys are ignored like the reference
/// `Schema.Struct`.
fn manifest(value: &Value) -> Option<&serde_json::Map<String, Value>> {
    let object = value.as_object()?;
    object
        .get("version")
        .and_then(Value::as_str)
        .is_some_and(is_version)
        .then_some(object)
}

/// Highest database migration a release's server knows, from its npm `t3` manifest
/// (`npmReleaseDatabaseMigration` in `@t3tools/shared/cliRelease`).
pub(crate) fn database_migration(manifest: &Value) -> Option<i64> {
    let number = manifest
        .get("t3code")?
        .as_object()?
        .get("databaseMigration")?;
    if let Some(value) = number.as_i64() {
        return (value.unsigned_abs() <= MAX_SAFE_INTEGER).then_some(value);
    }
    if number.is_u64() {
        return None;
    }
    // JSON numbers such as `96.0` are integers to JavaScript (`Number.isSafeInteger`).
    let value = number.as_f64()?;
    (value.fract() == 0.0 && value.abs() <= MAX_SAFE_INTEGER as f64).then_some(value as i64)
}
const MAX_SAFE_INTEGER: u64 = (1 << 53) - 1;

/// Decodes `npm view --json` output or a registry document into the RPC result for `spec`.
pub(crate) fn to_release(spec: &str, json: &str) -> Lookup {
    let decoded: Option<Value> = serde_json::from_str(json).ok();
    // npm prints a bare version string when the release has no `t3code` field.
    let manifest = decoded.and_then(|value| match &value {
        Value::Object(_) => manifest(&value).map(|_| value.clone()),
        Value::Array(items) if items.len() == 1 => manifest(&items[0]).map(|_| items[0].clone()),
        Value::String(version) if is_version(version) => Some(json!({"version": version})),
        _ => None,
    });
    let Some(manifest) = manifest else {
        return Err(INVALID_METADATA);
    };
    let version = manifest["version"].as_str().unwrap_or_default();
    if spec != "latest" && spec != "nightly" && version != spec {
        return Err(INVALID_METADATA);
    }
    let mut release = json!({"version": version});
    if let Some(migration) = database_migration(&manifest) {
        release["databaseMigration"] = json!(migration);
    }
    Ok(Some(release))
}

fn is_e404(stdout: &str) -> bool {
    serde_json::from_str::<Value>(stdout)
        .ok()
        .and_then(|value| {
            value
                .get("error")?
                .get("code")?
                .as_str()
                .map(|c| c == "E404")
        })
        .unwrap_or(false)
}

/// First executable `npm` on `path`, like `resolveCommandPath("npm")`.
fn resolve_npm(path: &OsStr) -> Option<PathBuf> {
    if path.is_empty() {
        return None;
    }
    let names: Vec<OsString> = if cfg!(windows) {
        let extensions = std::env::var("PATHEXT").unwrap_or_else(|_| ".COM;.EXE;.BAT;.CMD".into());
        extensions
            .split(';')
            .filter(|ext| !ext.is_empty())
            .map(|ext| format!("npm{}", ext.to_ascii_lowercase()).into())
            .collect()
    } else {
        vec!["npm".into()]
    };
    std::env::split_paths(path)
        .filter(|dir| !dir.as_os_str().is_empty())
        .find_map(|dir| {
            names
                .iter()
                .map(|name| dir.join(name))
                .find(|candidate| is_executable(candidate))
        })
}

fn is_executable(path: &Path) -> bool {
    let Ok(metadata) = std::fs::metadata(path) else {
        return false;
    };
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        metadata.is_file() && metadata.permissions().mode() & 0o111 != 0
    }
    #[cfg(not(unix))]
    {
        metadata.is_file()
    }
}

/// Reads the whole stream, failing once it exceeds `MAX_OUTPUT_BYTES` like the reference
/// process runner's default `"error"` output mode.
async fn collect(mut stream: impl AsyncRead + Unpin) -> Result<Vec<u8>, ()> {
    let mut bytes = Vec::new();
    let mut buffer = [0u8; 16 * 1024];
    loop {
        let read = stream.read(&mut buffer).await.map_err(|_| ())?;
        if read == 0 {
            return Ok(bytes);
        }
        if bytes.len() + read > MAX_OUTPUT_BYTES {
            return Err(());
        }
        bytes.extend_from_slice(&buffer[..read]);
    }
}

struct NpmOutput {
    stdout: Vec<u8>,
    success: bool,
}

/// Runs npm; any spawn, read, output-limit or timeout failure is a failed lookup, as in the
/// reference `ProcessRunner`, where those are errors rather than flagged results.
async fn run_npm(npm: &Path, spec: &str, timeout: Duration) -> Result<NpmOutput, ()> {
    let mut child = crate::login_shell::tokio_command(npm)
        // Named fields keep the output small; the full package document grows with every release.
        .args([
            "view",
            "--global",
            "--json",
            &format!("t3@{spec}"),
            "version",
            "t3code",
        ])
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .map_err(|_| ())?;
    let stdout = child.stdout.take().ok_or(())?;
    let stderr = child.stderr.take().ok_or(())?;
    let run = async {
        let (stdout, stderr) = tokio::join!(collect(stdout), collect(stderr));
        let (stdout, _) = (stdout?, stderr?);
        let status = child.wait().await.map_err(|_| ())?;
        Ok(NpmOutput {
            stdout,
            success: status.success(),
        })
    };
    tokio::time::timeout(timeout, run).await.map_err(|_| ())?
}

#[derive(Clone)]
pub(crate) struct NpmRelease {
    inner: Arc<Inner>,
}

struct Inner {
    /// `PATH` used to find npm; `None` reads the process environment on each lookup.
    path: Option<OsString>,
    registry: String,
    timeout: Duration,
    ttl: Duration,
    permits: Semaphore,
    http: reqwest::Client,
    cache: Mutex<IndexMap<String, Pending>>,
}

impl NpmRelease {
    fn new(path: Option<OsString>, registry: String, timeout: Duration, ttl: Duration) -> Self {
        Self {
            inner: Arc::new(Inner {
                path,
                registry,
                timeout,
                ttl,
                permits: Semaphore::new(NPM_PERMITS),
                http: reqwest::Client::new(),
                cache: Mutex::new(IndexMap::new()),
            }),
        }
    }

    /// The process-wide lookup the RPC layer uses.
    pub(crate) fn global() -> &'static Self {
        static GLOBAL: OnceLock<NpmRelease> = OnceLock::new();
        GLOBAL
            .get_or_init(|| Self::new(None, PUBLIC_REGISTRY.to_owned(), LOOKUP_TIMEOUT, CACHE_TTL))
    }

    /// Cached lookup of a spec already validated against `SPEC_PATTERN`. Concurrent requests for
    /// one spec share a lookup, which finishes even if every requester goes away.
    pub(crate) async fn get(&self, spec: &str) -> Lookup {
        let pending = {
            let mut cache = self.inner.cache.lock().unwrap_or_else(|e| e.into_inner());
            let fresh = cache.shift_remove(spec).filter(|pending| {
                pending
                    .peek()
                    .is_none_or(|(_, done)| done.elapsed() < self.inner.ttl)
            });
            let pending = fresh.unwrap_or_else(|| {
                let this = self.clone();
                let spec = spec.to_owned();
                let task = tokio::spawn(async move { this.lookup(&spec).await });
                async move {
                    let result = task.await.unwrap_or(Err(LOOKUP_FAILED));
                    (result, Instant::now())
                }
                .boxed()
                .shared()
            });
            // Insertion order is recency order, so the front is evicted first.
            cache.insert(spec.to_owned(), pending.clone());
            while cache.len() > CACHE_CAPACITY {
                cache.shift_remove_index(0);
            }
            pending
        };
        pending.await.0
    }

    async fn lookup(&self, spec: &str) -> Lookup {
        let path = self
            .inner
            .path
            .clone()
            .or_else(crate::login_shell::path)
            .unwrap_or_default();
        let Some(npm) = resolve_npm(&path) else {
            return self.lookup_public(spec).await;
        };
        let output = {
            let _permit = self
                .inner
                .permits
                .acquire()
                .await
                .map_err(|_| LOOKUP_FAILED)?;
            run_npm(&npm, spec, self.inner.timeout).await
        };
        let output = output.map_err(|()| {
            tracing::info!("npm release lookup failed");
            LOOKUP_FAILED
        })?;
        let Ok(stdout) = String::from_utf8(output.stdout) else {
            return Err(INCOMPLETE_RESPONSE);
        };
        if !output.success {
            if is_e404(&stdout) {
                return Ok(None);
            }
            tracing::info!("npm release lookup failed: npm exited unsuccessfully");
            return Err(UNREADABLE_METADATA);
        }
        to_release(spec, &stdout)
    }

    async fn lookup_public(&self, spec: &str) -> Lookup {
        let url = format!("{}/t3/{spec}", self.inner.registry);
        let fetch = async {
            let response = self.inner.http.get(url).send().await.ok()?;
            let status = response.status().as_u16();
            if status == 404 {
                return Some(None);
            }
            if !(200..300).contains(&status) {
                return None;
            }
            let body = response.bytes().await.ok()?;
            Some(Some(String::from_utf8_lossy(&body).into_owned()))
        };
        match tokio::time::timeout(self.inner.timeout, fetch).await {
            Ok(Some(Some(json))) => to_release(spec, &json),
            Ok(Some(None)) => Ok(None),
            _ => Err(REGISTRY_UNREACHABLE),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const VERSION: &str = "0.1.5-nightly.20261030.1";

    #[test]
    fn decodes_object_array_and_bare_string() {
        let object = json!({"version": VERSION, "t3code": {"databaseMigration": 42}}).to_string();
        assert_eq!(
            to_release(VERSION, &object),
            Ok(Some(json!({"version": VERSION, "databaseMigration": 42})))
        );
        let array = json!([{"version": VERSION, "t3code": {"databaseMigration": 42}}]).to_string();
        assert_eq!(
            to_release("nightly", &array),
            Ok(Some(json!({"version": VERSION, "databaseMigration": 42})))
        );
        let bare = json!(VERSION).to_string();
        let release = to_release("latest", &bare).unwrap().unwrap();
        assert_eq!(release, json!({"version": VERSION}));
        assert!(release.get("databaseMigration").is_none());
        // A registry document carries many more keys, which are ignored.
        let document = json!({"name": "t3", "version": "0.1.11", "dist": {}, "t3code": {"databaseMigration": 102}});
        assert_eq!(
            to_release("latest", &format!("\n{document}\n")),
            Ok(Some(json!({"version": "0.1.11", "databaseMigration": 102})))
        );
    }

    #[test]
    fn rejects_mismatched_or_malformed_metadata() {
        for json in [
            r#"{"version":"9.9.9"}"#,
            "[]",
            r#"[{"version":"0.1.5-nightly.20261030.1"},{"version":"0.1.5-nightly.20261030.1"}]"#,
            "not json",
            r#""not-a-version""#,
            r#"{"version":"v1.2.3"}"#,
            r#"{"t3code":{"databaseMigration":1}}"#,
            "42",
        ] {
            assert_eq!(to_release(VERSION, json), Err(INVALID_METADATA), "{json}");
        }
        // Tracks accept whatever version they point at.
        assert!(to_release("latest", r#"{"version":"9.9.9"}"#).is_ok());
    }

    #[test]
    fn reads_database_migration_like_the_shared_helper() {
        let migration =
            |t3code: Value| database_migration(&json!({"version": "1.0.0", "t3code": t3code}));
        assert_eq!(migration(json!({"databaseMigration": 96})), Some(96));
        assert_eq!(migration(json!({"databaseMigration": 96.0})), Some(96));
        assert_eq!(migration(json!({"databaseMigration": -3})), Some(-3));
        assert_eq!(
            migration(json!({"databaseMigration": 9007199254740991_i64})),
            Some(9007199254740991)
        );
        assert_eq!(
            migration(json!({"databaseMigration": 9007199254740992_i64})),
            None
        );
        assert_eq!(migration(json!({"databaseMigration": 1.5})), None);
        assert_eq!(migration(json!({"databaseMigration": "96"})), None);
        assert_eq!(migration(json!({})), None);
        assert_eq!(migration(json!([{"databaseMigration": 96}])), None);
        assert_eq!(migration(json!(null)), None);
        assert_eq!(database_migration(&json!({"version": "1.0.0"})), None);
    }

    #[test]
    fn validates_specs() {
        for spec in ["latest", "nightly", "0.1.11", "0.1.5-nightly.20261030.1"] {
            assert!(is_valid_spec(spec), "{spec}");
        }
        for spec in [
            "--registry=https://example.com",
            "../other",
            "other-package@latest",
            "*",
            "",
            "1.2",
            "latest\n",
        ] {
            assert!(!is_valid_spec(spec), "{spec:?}");
        }
    }

    #[cfg(unix)]
    mod fake_npm {
        use super::*;
        use std::os::unix::fs::PermissionsExt;

        /// A PATH holding only a fake `npm` running `body` under `sh`.
        fn fake(dir: &tempfile::TempDir, body: &str) -> OsString {
            let bin = dir.path().join("bin");
            std::fs::create_dir_all(&bin).unwrap();
            let npm = bin.join("npm");
            std::fs::write(&npm, format!("#!/bin/sh\n{body}\n")).unwrap();
            std::fs::set_permissions(&npm, std::fs::Permissions::from_mode(0o755)).unwrap();
            bin.into_os_string()
        }

        fn service(path: OsString, timeout: Duration) -> NpmRelease {
            // An unroutable registry: a configured npm must never fall back to public npm.
            NpmRelease::new(Some(path), "http://127.0.0.1:9".into(), timeout, CACHE_TTL)
        }

        #[tokio::test]
        async fn reads_releases_through_npm_and_caches_them() {
            let dir = tempfile::tempdir().unwrap();
            let log = dir.path().join("calls");
            let path = fake(
                &dir,
                &format!(
                    "echo \"$*\" >> '{}'\nprintf '%s' '{{\"version\":\"{VERSION}\",\"t3code\":{{\"databaseMigration\":42}}}}'",
                    log.display()
                ),
            );
            let npm = service(path, LOOKUP_TIMEOUT);
            let expected = Ok(Some(json!({"version": VERSION, "databaseMigration": 42})));
            let (a, b, c) = tokio::join!(npm.get(VERSION), npm.get(VERSION), npm.get(VERSION));
            assert_eq!(
                (a, b, c),
                (expected.clone(), expected.clone(), expected.clone())
            );
            assert_eq!(npm.get(VERSION).await, expected);
            assert_eq!(
                std::fs::read_to_string(&log).unwrap(),
                format!("view --global --json t3@{VERSION} version t3code\n")
            );
        }

        #[tokio::test]
        async fn expires_cached_releases() {
            let dir = tempfile::tempdir().unwrap();
            let log = dir.path().join("calls");
            let path = fake(
                &dir,
                &format!("echo x >> '{}'\nprintf '\"1.0.0\"'", log.display()),
            );
            let npm = NpmRelease::new(
                Some(path),
                "http://127.0.0.1:9".into(),
                LOOKUP_TIMEOUT,
                Duration::from_millis(50),
            );
            assert_eq!(
                npm.get("1.0.0").await,
                Ok(Some(json!({"version": "1.0.0"})))
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
            assert_eq!(
                npm.get("1.0.0").await,
                Ok(Some(json!({"version": "1.0.0"})))
            );
            assert_eq!(std::fs::read_to_string(&log).unwrap(), "x\nx\n");
        }

        #[tokio::test]
        async fn maps_npm_outcomes_to_reference_errors() {
            let cases: &[(&str, Lookup)] = &[
                ("printf '{\"error\":{\"code\":\"E404\"}}'; exit 1", Ok(None)),
                (
                    "printf '{\"error\":{\"code\":\"E401\",\"detail\":\"sensitive\"}}'; exit 1",
                    Err(UNREADABLE_METADATA),
                ),
                ("exit 9", Err(UNREADABLE_METADATA)),
                ("printf 'not json'", Err(INVALID_METADATA)),
                ("printf '{\"version\":\"9.9.9\"}'", Err(INVALID_METADATA)),
                ("printf '\\377\\376'", Err(INCOMPLETE_RESPONSE)),
                ("head -c 300000 /dev/zero | tr '\\0' a", Err(LOOKUP_FAILED)),
                ("sleep 5", Err(LOOKUP_FAILED)),
            ];
            for (body, expected) in cases {
                let dir = tempfile::tempdir().unwrap();
                // Long enough for a fake npm to start on a loaded machine;
                // shorter than the `sleep 5` case it must cut off.
                let npm = service(fake(&dir, body), Duration::from_secs(2));
                assert_eq!(&npm.get(VERSION).await, expected, "{body}");
            }
        }

        #[tokio::test]
        async fn reports_spawn_failures() {
            let dir = tempfile::tempdir().unwrap();
            // Executable but not runnable: the interpreter does not exist.
            let path = fake(&dir, "");
            std::fs::write(Path::new(&path).join("npm"), "#!/nonexistent/interpreter\n").unwrap();
            let npm = service(path, LOOKUP_TIMEOUT);
            assert_eq!(npm.get(VERSION).await, Err(LOOKUP_FAILED));
        }

        #[tokio::test]
        async fn runs_at_most_two_npm_lookups_at_once() {
            let dir = tempfile::tempdir().unwrap();
            let log = dir.path().join("events");
            let path = fake(
                &dir,
                &format!(
                    "echo start >> '{log}'\nsleep 0.3\necho end >> '{log}'\nprintf '{{\"error\":{{\"code\":\"E404\"}}}}'; exit 1",
                    log = log.display()
                ),
            );
            let npm = service(path, LOOKUP_TIMEOUT);
            let results = tokio::join!(npm.get("1.0.0"), npm.get("1.0.1"), npm.get("1.0.2"));
            assert_eq!(results, (Ok(None), Ok(None), Ok(None)));
            let events = std::fs::read_to_string(&log).unwrap();
            let (mut running, mut peak) = (0, 0);
            for event in events.lines() {
                running += if event == "start" { 1 } else { -1 };
                peak = peak.max(running);
            }
            assert_eq!(peak, 2, "{events}");
        }
    }

    mod public_registry {
        use super::*;
        use axum::{extract::Path as UrlPath, http::StatusCode, routing::get, Router};

        async fn registry() -> String {
            let app = Router::new().route(
                "/t3/:spec",
                get(|UrlPath(spec): UrlPath<String>| async move {
                    match spec.as_str() {
                        "nightly" => (
                            StatusCode::OK,
                            json!({"version": VERSION, "t3code": {"databaseMigration": 42}})
                                .to_string(),
                        ),
                        "latest" => {
                            tokio::time::sleep(Duration::from_secs(5)).await;
                            (StatusCode::OK, String::new())
                        }
                        "5.0.0" => (StatusCode::INTERNAL_SERVER_ERROR, String::new()),
                        _ => (StatusCode::NOT_FOUND, "{}".into()),
                    }
                }),
            );
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            tokio::spawn(async move { axum::serve(listener, app).await });
            format!("http://{address}")
        }

        #[tokio::test]
        async fn falls_back_to_the_public_registry_without_npm() {
            let empty = tempfile::tempdir().unwrap();
            let npm = NpmRelease::new(
                Some(empty.path().as_os_str().to_owned()),
                registry().await,
                Duration::from_millis(500),
                CACHE_TTL,
            );
            assert_eq!(
                npm.get("nightly").await,
                Ok(Some(json!({"version": VERSION, "databaseMigration": 42})))
            );
            assert_eq!(npm.get("9.9.9").await, Ok(None));
            assert_eq!(npm.get("5.0.0").await, Err(REGISTRY_UNREACHABLE));
            assert_eq!(npm.get("latest").await, Err(REGISTRY_UNREACHABLE));
            let unreachable = NpmRelease::new(
                Some(OsString::new()),
                "http://127.0.0.1:9".into(),
                LOOKUP_TIMEOUT,
                CACHE_TTL,
            );
            assert_eq!(unreachable.get("latest").await, Err(REGISTRY_UNREACHABLE));
        }
    }
}
