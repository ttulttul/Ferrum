//! Installing ACP Registry agents (`server.prepareAcpRegistryAgent`) and removing
//! managed binaries (`server.uninstallAcpRegistryManagedBinary`), as the
//! reference's `AcpRegistryCatalog` does.
//!
//! A binary agent is downloaded into `<T3 home>/tools/<agent>/<version>/<platform>`,
//! checked against its declared SHA-256, unpacked (`tar`, or `unzip` for zips) after
//! its listing is checked for unsafe paths, and its command must stay inside the
//! install folder. An npx agent is installed with `npm install --global` under
//! `<T3 home>/tools/<agent>/<version>/npm` (`npm_config_prefix`), a uvx agent with
//! `uv tool install`; each records a receipt under
//! `<T3 home>/caches/acp-registry/package-installs` so a later prepare reuses it.
use crate::acp_registry::AcpError;
use serde_json::{json, Map, Value};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::{Arc, LazyLock, Mutex};
use std::time::{Duration, Instant, SystemTime};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const MAX_ARCHIVE_BYTES: u64 = 1024 * 1024 * 1024;
const MAX_COMMAND_OUTPUT_BYTES: usize = 1024 * 1024;
const MAX_COMMAND_ERROR_TAIL_CHARS: usize = 1_000;
const MAX_ARCHIVE_LISTING_BYTES: usize = 64 * 1024 * 1024;
const MAX_PACKAGE_MANIFEST_BYTES: u64 = 1024 * 1024;
const ARCHIVE_TIMEOUT: Duration = Duration::from_secs(20 * 60);
const PACKAGE_INSTALL_TIMEOUT: Duration = Duration::from_secs(20 * 60);
const PACKAGE_QUERY_TIMEOUT: Duration = Duration::from_secs(30);
const INSTALL_LOCK_RETRIES: u32 = 300;
const INSTALL_LOCK_RETRY_DELAY: Duration = Duration::from_millis(100);
const INSTALL_LOCK_STALE: Duration = Duration::from_secs(5 * 60);
const PREPARED_BINARY_RESERVATION: Duration = Duration::from_secs(30);

/// Where agents install, from the T3 home (`baseDir`).
pub(crate) struct Roots {
    pub caches: PathBuf,
    pub tools: PathBuf,
}

fn err(reason: &'static str, detail: impl Into<String>) -> AcpError {
    AcpError::new(reason, detail)
}

/// Long installs only block operations on the same agent (`withInstallPermit`).
static PERMITS: LazyLock<Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>> =
    LazyLock::new(Mutex::default);
/// Binaries prepared in the last thirty seconds, which an uninstall must leave alone.
static RESERVATIONS: LazyLock<Mutex<HashMap<String, Instant>>> = LazyLock::new(Mutex::default);

fn permit(agent_id: &str) -> Arc<tokio::sync::Mutex<()>> {
    PERMITS
        .lock()
        .unwrap()
        .entry(agent_id.to_owned())
        .or_default()
        .clone()
}

fn reserve(agent_id: &str) {
    let now = Instant::now();
    let mut reservations = RESERVATIONS.lock().unwrap();
    reservations.retain(|_, expires| *expires > now);
    reservations.insert(agent_id.to_owned(), now + PREPARED_BINARY_RESERVATION);
}

fn reserved(agent_id: &str) -> bool {
    let mut reservations = RESERVATIONS.lock().unwrap();
    match reservations.get(agent_id) {
        Some(expires) if *expires > Instant::now() => true,
        Some(_) => {
            reservations.remove(agent_id);
            false
        }
        None => false,
    }
}

fn is_executable(path: &Path) -> bool {
    crate::npm_release::is_executable(path)
}

/// `resolveExecutable`: `name` on the `PATH` in `env`.
fn resolve_executable(name: &str, env: &HashMap<OsString, OsString>) -> Option<PathBuf> {
    let path = env.get(&OsString::from("PATH"))?;
    std::env::split_paths(path)
        .map(|dir| dir.join(name))
        .find(|candidate| is_executable(candidate))
}

/// `withPreferredPath`: `env` with `directory` first on its `PATH`.
fn with_preferred_path(
    env: &HashMap<OsString, OsString>,
    directory: &Path,
) -> HashMap<OsString, OsString> {
    let mut entries = vec![directory.to_path_buf()];
    if let Some(path) = env.get(&OsString::from("PATH")) {
        entries.extend(std::env::split_paths(path).filter(|p| p != directory));
    }
    let mut next = env.clone();
    next.insert(
        "PATH".into(),
        std::env::join_paths(entries).unwrap_or_default(),
    );
    next
}

fn host_environment() -> HashMap<OsString, OsString> {
    std::env::vars_os().collect()
}

/// `runCommand`: stdout, or `install_failed` with the exit code and the end of stderr.
async fn run_command(
    command: &Path,
    args: &[&str],
    cwd: Option<&Path>,
    env: Option<&HashMap<OsString, OsString>>,
    timeout: Option<Duration>,
    max_stdout: usize,
    truncated_reason: &'static str,
) -> Result<String, AcpError> {
    let shown = command.display().to_string();
    let mut process = crate::login_shell::tokio_command(command);
    process
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    if let Some(cwd) = cwd {
        process.current_dir(cwd);
    }
    if let Some(env) = env {
        process.env_clear().envs(env);
    }
    let run = async {
        let mut child = process.spawn().map_err(|_| {
            err(
                "install_failed",
                format!("Could not run ACP Registry install command '{shown}'."),
            )
        })?;
        let mut stdout = child.stdout.take().expect("piped");
        let mut stderr = child.stderr.take().expect("piped");
        let out = tokio::spawn(async move {
            let (mut kept, mut buffer, mut truncated) = (Vec::new(), [0u8; 8192], false);
            while let Ok(n) = stdout.read(&mut buffer).await {
                if n == 0 {
                    break;
                }
                let room = max_stdout.saturating_sub(kept.len());
                truncated |= n > room;
                kept.extend_from_slice(&buffer[..n.min(room)]);
            }
            (kept, truncated)
        });
        let err_task = tokio::spawn(async move {
            let mut all = Vec::new();
            let _ = stderr.read_to_end(&mut all).await;
            all
        });
        let status = child.wait().await.map_err(|_| {
            err(
                "install_failed",
                format!("Could not run ACP Registry install command '{shown}'."),
            )
        })?;
        let (stdout, truncated) = out.await.unwrap_or_default();
        let stderr = String::from_utf8_lossy(&err_task.await.unwrap_or_default()).into_owned();
        if !status.success() {
            let chars: Vec<char> = stderr.chars().collect();
            let tail: String = chars[chars.len().saturating_sub(MAX_COMMAND_ERROR_TAIL_CHARS)..]
                .iter()
                .collect();
            return Err(err(
                "install_failed",
                format!(
                    "ACP Registry install command '{shown}' exited with code {}: {}",
                    status.code().unwrap_or(-1),
                    tail.trim()
                ),
            ));
        }
        // Truncated listings would bypass archive traversal validation.
        if truncated {
            return Err(err(
                truncated_reason,
                format!(
                    "ACP Registry install command '{shown}' produced more output than expected."
                ),
            ));
        }
        Ok(String::from_utf8_lossy(&stdout).into_owned())
    };
    match timeout {
        None => run.await,
        Some(limit) => tokio::time::timeout(limit, run).await.unwrap_or_else(|_| {
            Err(err(
                "install_failed",
                format!("Timed out running ACP Registry install command '{shown}'."),
            ))
        }),
    }
}

/// `normalizeRegistryCommandPath`: the command's path segments, or `None` if unsafe.
fn command_segments(command: &str) -> Option<Vec<String>> {
    let normalized = command.trim().replace('\\', "/");
    let normalized = normalized
        .strip_prefix("./")
        .unwrap_or(&normalized)
        .to_owned();
    let segments: Vec<String> = normalized
        .split('/')
        .filter(|s| !s.is_empty())
        .map(str::to_owned)
        .collect();
    let drive = normalized.as_bytes().get(1) == Some(&b':')
        && normalized.as_bytes()[0].is_ascii_alphabetic();
    (!segments.is_empty()
        && !normalized.starts_with('/')
        && !drive
        && !segments.iter().any(|s| s == "." || s == ".."))
    .then_some(segments)
}

/// `validateArchiveEntries`.
fn safe_listing(output: &str) -> bool {
    output
        .lines()
        .map(str::trim)
        .filter(|e| !e.is_empty())
        .all(|e| e == "./" || command_segments(e).is_some())
}

#[derive(Clone, Copy, PartialEq, Debug)]
enum ArchiveKind {
    Raw,
    TarBz2,
    TarGz,
    Zip,
}

fn archive_kind(url: &str) -> ArchiveKind {
    let path = url::Url::parse(url)
        .map(|u| u.path().to_lowercase())
        .unwrap_or_default();
    if path.ends_with(".tar.gz") || path.ends_with(".tgz") {
        ArchiveKind::TarGz
    } else if path.ends_with(".tar.bz2") || path.ends_with(".tbz2") {
        ArchiveKind::TarBz2
    } else if path.ends_with(".zip") {
        ArchiveKind::Zip
    } else {
        ArchiveKind::Raw
    }
}

fn archive_file_name(kind: ArchiveKind) -> &'static str {
    match kind {
        ArchiveKind::TarGz => "agent.tar.gz",
        ArchiveKind::TarBz2 => "agent.tar.bz2",
        ArchiveKind::Zip => "agent.zip",
        ArchiveKind::Raw => "agent.bin",
    }
}

fn agent_install_root(roots: &Roots, agent: &Value) -> PathBuf {
    let id = agent["id"].as_str().unwrap_or_default();
    let version =
        crate::thread_projection::uri_component(agent["version"].as_str().unwrap_or_default());
    roots.tools.join(id).join(version)
}

/// `assertExecutableInRoot`: the command's real path, inside `root` and executable.
fn executable_in_root(
    root: &Path,
    executable: &Path,
    make_executable: bool,
) -> Result<PathBuf, AcpError> {
    let invalid = |detail: &str| err("archive_invalid", detail);
    let root = std::fs::canonicalize(root).map_err(|_| {
        err(
            "install_failed",
            "Could not resolve the ACP Registry installation directory.",
        )
    })?;
    let real = std::fs::canonicalize(executable).map_err(|_| {
        err(
            "install_failed",
            "Could not resolve the ACP Registry archive command.",
        )
    })?;
    if !real.starts_with(&root) {
        return Err(invalid(
            "ACP Registry archive command resolves outside its installation directory.",
        ));
    }
    // Permissions follow links, so only change the mode once the real path is known to be inside.
    if make_executable {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(&real, std::fs::Permissions::from_mode(0o755));
    }
    if !std::fs::metadata(&real).is_ok_and(|m| m.is_file()) || !is_executable(&real) {
        return Err(invalid(
            "ACP Registry archive command is not a regular executable file.",
        ));
    }
    Ok(real)
}

/// `acquireInstallLock`: an exclusive lock file, taken over once stale.
async fn acquire_lock(lock: &Path) -> Result<(), AcpError> {
    for _ in 0..INSTALL_LOCK_RETRIES {
        match std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(lock)
        {
            Ok(_) => return Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(_) => {
                return Err(err(
                    "install_failed",
                    "Could not acquire the ACP Registry install lock.",
                ))
            }
        }
        let stale = std::fs::metadata(lock)
            .and_then(|m| m.modified())
            .ok()
            .and_then(|m| SystemTime::now().duration_since(m).ok())
            .is_some_and(|age| age > INSTALL_LOCK_STALE);
        if stale {
            let _ = std::fs::remove_file(lock);
            continue;
        }
        tokio::time::sleep(INSTALL_LOCK_RETRY_DELAY).await;
    }
    Err(err(
        "install_failed",
        format!(
            "Timed out waiting for ACP Registry install lock {}.",
            lock.display()
        ),
    ))
}

/// `installBinary`.
async fn install_binary(
    roots: &Roots,
    agent: &Value,
    target_name: &str,
    target: &Value,
) -> Result<PathBuf, AcpError> {
    let id = agent["id"].as_str().unwrap_or_default();
    let version = agent["version"].as_str().unwrap_or_default();
    let cmd = target["cmd"].as_str().unwrap_or_default();
    let Some(segments) = command_segments(cmd) else {
        return Err(err(
            "archive_invalid",
            format!("ACP Registry agent {id} declares an unsafe command path '{cmd}'."),
        ));
    };
    let install_root = agent_install_root(roots, agent).join(target_name);
    let executable = segments.iter().fold(install_root.clone(), |p, s| p.join(s));
    if executable.exists() {
        return executable_in_root(&install_root, &executable, false);
    }
    let parent = install_root
        .parent()
        .expect("install root has a parent")
        .to_path_buf();
    std::fs::create_dir_all(&parent).map_err(|_| {
        err(
            "install_failed",
            format!("Could not create ACP Registry cache for {id}."),
        )
    })?;
    let lock = PathBuf::from(format!("{}.lock", install_root.display()));
    acquire_lock(&lock).await?;
    let result = async {
        if executable.exists() {
            return executable_in_root(&install_root, &executable, false);
        }
        let staging = tempfile::Builder::new()
            .prefix(&format!(".{id}-install-"))
            .tempdir_in(&parent)
            .map_err(|_| {
                err(
                    "install_failed",
                    format!("Could not install ACP Registry agent {id}."),
                )
            })?;
        let extraction = staging.path().join("extracted");
        std::fs::create_dir_all(&extraction).map_err(|_| {
            err(
                "install_failed",
                format!("Could not install ACP Registry agent {id}."),
            )
        })?;
        let archive_url = target["archive"].as_str().unwrap_or_default();
        let kind = archive_kind(archive_url);
        let archive = staging.path().join(archive_file_name(kind));
        let digest = download(archive_url, &archive, id, version).await?;
        if let Some(expected) = target["sha256"].as_str() {
            if digest != expected.to_lowercase() {
                return Err(err(
                    "checksum_mismatch",
                    format!("ACP Registry agent {id} download did not match its declared SHA-256."),
                ));
            }
        }
        if kind == ArchiveKind::Raw {
            let staged = segments.iter().fold(extraction.clone(), |p, s| p.join(s));
            std::fs::create_dir_all(staged.parent().unwrap_or(&extraction))
                .and_then(|_| std::fs::rename(&archive, &staged))
                .map_err(|_| {
                    err(
                        "install_failed",
                        format!("Could not install ACP Registry agent {id}."),
                    )
                })?;
        } else {
            let archive_arg = archive.display().to_string();
            let extraction_arg = extraction.display().to_string();
            let listing = match kind {
                ArchiveKind::Zip => {
                    run_command(
                        Path::new("unzip"),
                        &["-Z1", &archive_arg],
                        None,
                        None,
                        None,
                        MAX_ARCHIVE_LISTING_BYTES,
                        "archive_invalid",
                    )
                    .await?
                }
                ArchiveKind::TarGz => {
                    run_command(
                        Path::new("tar"),
                        &["-tzf", &archive_arg],
                        None,
                        None,
                        None,
                        MAX_ARCHIVE_LISTING_BYTES,
                        "archive_invalid",
                    )
                    .await?
                }
                _ => {
                    run_command(
                        Path::new("tar"),
                        &["-tjf", &archive_arg],
                        None,
                        None,
                        None,
                        MAX_ARCHIVE_LISTING_BYTES,
                        "archive_invalid",
                    )
                    .await?
                }
            };
            if !safe_listing(&listing) {
                return Err(err(
                    "archive_invalid",
                    format!("ACP Registry agent {id} archive contains an unsafe path."),
                ));
            }
            match kind {
                ArchiveKind::Zip => {
                    run_command(
                        Path::new("unzip"),
                        &["-q", &archive_arg, "-d", &extraction_arg],
                        None,
                        None,
                        None,
                        MAX_COMMAND_OUTPUT_BYTES,
                        "archive_invalid",
                    )
                    .await?
                }
                ArchiveKind::TarGz => {
                    run_command(
                        Path::new("tar"),
                        &["-xzf", &archive_arg, "-C", &extraction_arg],
                        None,
                        None,
                        None,
                        MAX_COMMAND_OUTPUT_BYTES,
                        "archive_invalid",
                    )
                    .await?
                }
                _ => {
                    run_command(
                        Path::new("tar"),
                        &["-xjf", &archive_arg, "-C", &extraction_arg],
                        None,
                        None,
                        None,
                        MAX_COMMAND_OUTPUT_BYTES,
                        "archive_invalid",
                    )
                    .await?
                }
            };
        }
        let staged = segments.iter().fold(extraction.clone(), |p, s| p.join(s));
        if !staged.exists() {
            return Err(err(
                "archive_invalid",
                format!("ACP Registry archive for {id} did not contain '{cmd}'."),
            ));
        }
        executable_in_root(&extraction, &staged, true)?;
        let _ = std::fs::remove_dir_all(&install_root);
        std::fs::rename(&extraction, &install_root).map_err(|_| {
            err(
                "install_failed",
                format!("Could not install ACP Registry agent {id}."),
            )
        })?;
        executable_in_root(&install_root, &executable, false)
    }
    .await;
    let _ = std::fs::remove_file(&lock);
    result
}

/// Downloads `url` to `path` within the size cap; the SHA-256 of what arrived.
async fn download(url: &str, path: &Path, id: &str, version: &str) -> Result<String, AcpError> {
    let failed = |cause: Value| {
        let mut error = err(
            "download_failed",
            format!("Could not download ACP Registry agent {id} {version}."),
        );
        error.set_cause(cause);
        error
    };
    let transport =
        || json!({"name": "HttpClientError", "message": format!("Transport error (GET {url})")});
    let client = reqwest::Client::builder()
        .timeout(ARCHIVE_TIMEOUT)
        .build()
        .map_err(|_| failed(transport()))?;
    let mut response = client.get(url).send().await.map_err(|e| {
        if e.is_timeout() {
            err(
                "download_failed",
                format!("Timed out downloading ACP Registry agent {id} {version}."),
            )
        } else {
            failed(transport())
        }
    })?;
    let status = response.status().as_u16();
    if !(200..300).contains(&status) {
        return Err(failed(json!({"name": "HttpClientError",
            "message": format!("StatusCode: non 2xx status code ({status} GET {url})")})));
    }
    let saving = || {
        err(
            "download_failed",
            format!("Could not save ACP Registry agent {id} {version}."),
        )
    };
    let mut file = tokio::fs::File::create(path).await.map_err(|_| saving())?;
    let (mut hash, mut total) = (Sha256::new(), 0u64);
    loop {
        match response.chunk().await {
            Ok(Some(chunk)) => {
                if total + chunk.len() as u64 > MAX_ARCHIVE_BYTES {
                    return Err(err(
                        "archive_invalid",
                        format!(
                            "ACP Registry agent {id} archive exceeds {MAX_ARCHIVE_BYTES} bytes."
                        ),
                    ));
                }
                total += chunk.len() as u64;
                hash.update(&chunk);
                file.write_all(&chunk).await.map_err(|_| saving())?;
            }
            Ok(None) => break,
            Err(_) => return Err(saving()),
        }
    }
    file.flush().await.map_err(|_| saving())?;
    Ok(hex::encode(hash.finalize()))
}

/// `parseNpxPackageSpec` / `parseUvxPackageSpec`: (name, version without a leading v).
fn package_identity(spec: &str, distribution: &str) -> (String, String) {
    let (at, width) = match (distribution, spec.rfind("==")) {
        ("uvx", Some(at)) => (at, 2),
        _ => (spec.rfind('@').unwrap_or(0), 1),
    };
    let version = &spec[(at + width).min(spec.len())..];
    let version = version.strip_prefix(['v', 'V']).unwrap_or(version);
    (spec[..at].to_owned(), version.to_owned())
}

fn package_install_root(roots: &Roots, agent: &Value, distribution: &str) -> PathBuf {
    agent_install_root(roots, agent).join(if distribution == "npx" {
        "npm"
    } else {
        "python"
    })
}

fn package_bin_directory(roots: &Roots, agent: &Value, distribution: &str) -> PathBuf {
    package_install_root(roots, agent, distribution).join("bin")
}

fn receipt_path(roots: &Roots, agent_id: &str, distribution: &str, manager: &Path) -> PathBuf {
    let digest = hex::encode(Sha256::digest(
        format!("{distribution}\0{}", manager.display()).as_bytes(),
    ));
    roots
        .caches
        .join("acp-registry")
        .join("package-installs")
        .join(format!("{agent_id}-{}.json", &digest[..16]))
}

/// The receipt fields, in the reference's order (`AcpRegistryPackageInstallReceipt`).
fn receipt_json(receipt: &Map<String, Value>) -> String {
    let mut ordered = Map::new();
    for key in [
        "agentId",
        "agentVersion",
        "distribution",
        "packageSpec",
        "managerPath",
        "binDirectory",
        "executablePath",
        "packageRoot",
        "packageVersion",
    ] {
        if let Some(value) = receipt.get(key) {
            ordered.insert(key.to_owned(), value.clone());
        }
    }
    format!("{}\n", Value::Object(ordered))
}

fn npm_manifest(path: &Path) -> Option<Value> {
    if std::fs::metadata(path).ok()?.len() > MAX_PACKAGE_MANIFEST_BYTES {
        return None;
    }
    let manifest: Value = serde_json::from_str(&std::fs::read_to_string(path).ok()?).ok()?;
    let valid_bin = match &manifest["bin"] {
        Value::String(s) => s.encode_utf16().count() <= 1024,
        Value::Object(map) => map.iter().all(|(k, v)| {
            k.len() <= 128
                && k.chars().next().is_some_and(|c| c.is_ascii_alphanumeric())
                && k.chars()
                    .all(|c| c.is_ascii_alphanumeric() || "._-".contains(c))
                && v.as_str().is_some_and(|s| s.encode_utf16().count() <= 1024)
        }),
        _ => false,
    };
    (manifest["name"].is_string() && manifest["version"].is_string() && valid_bin)
        .then_some(manifest)
}

/// `readPackageReceipt`: a receipt still matching the agent, its command present.
fn read_receipt(
    roots: &Roots,
    agent: &Value,
    distribution: &str,
    spec: &str,
    manager: &Path,
) -> Option<Map<String, Value>> {
    let text = std::fs::read_to_string(receipt_path(
        roots,
        agent["id"].as_str()?,
        distribution,
        manager,
    ))
    .ok()?;
    let receipt = serde_json::from_str::<Value>(&text)
        .ok()?
        .as_object()?
        .clone();
    let text_of = |k: &str| receipt.get(k).and_then(Value::as_str);
    let bin = package_bin_directory(roots, agent, distribution);
    let executable = PathBuf::from(text_of("executablePath")?);
    let matches = text_of("agentId") == agent["id"].as_str()
        && text_of("agentVersion") == agent["version"].as_str()
        && text_of("distribution") == Some(distribution)
        && text_of("packageSpec") == Some(spec)
        && text_of("managerPath") == Some(manager.to_str()?)
        && text_of("binDirectory") == bin.to_str()
        && executable.parent() == Some(bin.as_path())
        && executable.exists();
    if !matches {
        return None;
    }
    if distribution == "npx" {
        let (name, version) = package_identity(spec, distribution);
        let manifest = npm_manifest(&PathBuf::from(text_of("packageRoot")?).join("package.json"))?;
        if manifest["name"] != name.as_str()
            || manifest["version"] != version.as_str()
            || text_of("packageVersion") != Some(version.as_str())
        {
            return None;
        }
    }
    Some(receipt)
}

fn write_receipt(roots: &Roots, receipt: &Map<String, Value>) -> Result<(), AcpError> {
    let failed = || {
        err(
            "install_failed",
            "Could not record the globally installed ACP Registry command.",
        )
    };
    let manager = PathBuf::from(receipt["managerPath"].as_str().unwrap_or_default());
    let path = receipt_path(
        roots,
        receipt["agentId"].as_str().unwrap_or_default(),
        receipt["distribution"].as_str().unwrap_or_default(),
        &manager,
    );
    let temporary = PathBuf::from(format!("{}.{}.tmp", path.display(), std::process::id()));
    std::fs::create_dir_all(path.parent().expect("receipts have a folder"))
        .map_err(|_| failed())?;
    let _ = std::fs::remove_file(&temporary);
    std::fs::write(&temporary, receipt_json(receipt)).map_err(|_| failed())?;
    let _ = std::fs::remove_file(&path);
    std::fs::rename(&temporary, &path).map_err(|_| failed())
}

/// `parsePackageManagerPath`: the one absolute path a manager printed.
fn manager_path_line(manager: &Path, output: &str) -> Result<PathBuf, AcpError> {
    let lines: Vec<&str> = output
        .lines()
        .map(str::trim)
        .filter(|l| !l.is_empty())
        .collect();
    match lines.as_slice() {
        [line] if Path::new(line).is_absolute() => Ok(PathBuf::from(line)),
        _ => Err(err(
            "install_failed",
            format!(
                "ACP Registry package manager '{}' returned an invalid global path.",
                manager.display()
            ),
        )),
    }
}

/// `npmCommandName`: the command the package's `bin` names for the agent.
fn npm_command_name(agent: &Value, package: &str, manifest: &Value) -> Option<String> {
    let base = package.rsplit('/').next().unwrap_or(package);
    let bin = match &manifest["bin"] {
        Value::String(_) => return Some(base.to_owned()),
        Value::Object(map) => map,
        _ => return None,
    };
    let mut entries: Vec<(&String, &Value)> = bin.iter().collect();
    entries.sort_by(|a, b| a.0.cmp(b.0));
    if entries.len() == 1 {
        return Some(entries[0].0.clone());
    }
    if bin.contains_key(base) {
        return Some(base.to_owned());
    }
    if let Some(id) = agent["id"].as_str().filter(|id| bin.contains_key(*id)) {
        return Some(id.to_owned());
    }
    let targets: std::collections::HashSet<_> = entries.iter().map(|(_, v)| v.as_str()).collect();
    (targets.len() == 1)
        .then(|| entries.first().map(|(k, _)| (*k).clone()))
        .flatten()
}

async fn discover_npm(
    agent: &Value,
    spec: &str,
    manager: &Path,
    env: &HashMap<OsString, OsString>,
) -> Result<Option<Map<String, Value>>, AcpError> {
    let (name, version) = package_identity(spec, "npx");
    let query = |args: &'static [&'static str]| {
        run_command(
            manager,
            args,
            None,
            Some(env),
            Some(PACKAGE_QUERY_TIMEOUT),
            MAX_COMMAND_OUTPUT_BYTES,
            "install_failed",
        )
    };
    let root = manager_path_line(manager, &query(&["root", "--global"]).await?)?;
    let prefix = manager_path_line(manager, &query(&["prefix", "--global"]).await?)?;
    let package_root = name.split('/').fold(root, |p, s| p.join(s));
    let Some(manifest) = npm_manifest(&package_root.join("package.json")) else {
        return Ok(None);
    };
    if manifest["name"] != name.as_str() || manifest["version"] != version.as_str() {
        return Ok(None);
    }
    let Some(command) = npm_command_name(agent, &name, &manifest) else {
        return Ok(None);
    };
    let bin = prefix.join("bin");
    let Some(executable) = resolve_executable(&command, &with_preferred_path(env, &bin)) else {
        return Ok(None);
    };
    let receipt = json!({"agentId": agent["id"], "agentVersion": agent["version"], "distribution": "npx",
        "packageSpec": spec, "managerPath": manager, "binDirectory": bin, "executablePath": executable,
        "packageRoot": package_root, "packageVersion": manifest["version"]});
    Ok(receipt.as_object().cloned())
}

/// `packageCommandCandidates`.
fn package_command_candidates(agent: &Value, package: &str) -> Vec<String> {
    let base = package.rsplit('/').next().unwrap_or(package);
    let mut out: Vec<String> = Vec::new();
    for candidate in [
        agent["id"].as_str().unwrap_or_default(),
        base,
        base.strip_suffix("-acp").unwrap_or(base),
    ] {
        let valid = !candidate.is_empty()
            && candidate.len() <= 128
            && candidate
                .chars()
                .next()
                .is_some_and(|c| c.is_ascii_alphanumeric())
            && candidate
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || "._-".contains(c));
        if valid && !out.iter().any(|o| o == candidate) {
            out.push(candidate.to_owned());
        }
    }
    out
}

async fn discover_uv(
    agent: &Value,
    spec: &str,
    manager: &Path,
    env: &HashMap<OsString, OsString>,
) -> Result<Option<Map<String, Value>>, AcpError> {
    let (name, version) = package_identity(spec, "uvx");
    let tools = run_command(
        manager,
        &["tool", "list"],
        None,
        Some(env),
        Some(PACKAGE_QUERY_TIMEOUT),
        MAX_COMMAND_OUTPUT_BYTES,
        "install_failed",
    )
    .await?;
    let normalize = |s: &str| {
        let lower = s.to_lowercase();
        let mut out = String::new();
        let mut last_dash = false;
        for c in lower.chars() {
            if "._-".contains(c) {
                if !last_dash {
                    out.push('-');
                }
                last_dash = true;
            } else {
                out.push(c);
                last_dash = false;
            }
        }
        out
    };
    let wanted = normalize(&name);
    let installed = tools.lines().any(|line| {
        let mut parts = line.split_whitespace();
        match (parts.next(), parts.next()) {
            (Some(tool), Some(v)) => {
                normalize(tool) == wanted && v.strip_prefix('v') == Some(version.as_str())
            }
            _ => false,
        }
    });
    if !installed {
        return Ok(None);
    }
    let bin_output = run_command(
        manager,
        &["tool", "dir", "--bin"],
        None,
        Some(env),
        Some(PACKAGE_QUERY_TIMEOUT),
        MAX_COMMAND_OUTPUT_BYTES,
        "install_failed",
    )
    .await?;
    let bin = manager_path_line(manager, &bin_output)?;
    let mut only_bin = env.clone();
    only_bin.insert("PATH".into(), bin.clone().into_os_string());
    let Some(executable) = package_command_candidates(agent, &name)
        .iter()
        .find_map(|c| resolve_executable(c, &only_bin))
    else {
        return Ok(None);
    };
    let receipt = json!({"agentId": agent["id"], "agentVersion": agent["version"], "distribution": "uvx",
        "packageSpec": spec, "managerPath": manager, "binDirectory": bin, "executablePath": executable,
        "packageVersion": version});
    Ok(receipt.as_object().cloned())
}

/// `managerBinDirectory`: the toolchain's bin folder for a package manager on PATH.
fn manager_bin_directory(manager: &Path) -> PathBuf {
    let parent = manager.parent().unwrap_or(Path::new("/")).to_path_buf();
    let Ok(link) = std::fs::read_link(manager) else {
        return parent;
    };
    let target = if link.is_absolute() {
        link
    } else {
        parent.join(link)
    };
    // Wrapper links can name the toolchain's bin folder; npm-cli.js points inside npm instead.
    if target.file_name() == manager.file_name() {
        target.parent().map(Path::to_path_buf).unwrap_or(parent)
    } else {
        parent
    }
}

/// `ensureGlobalPackage`.
async fn ensure_package(
    roots: &Roots,
    agent: &Value,
    distribution: &str,
    spec: &str,
) -> Result<Map<String, Value>, AcpError> {
    let id = agent["id"].as_str().unwrap_or_default();
    let manager_name = if distribution == "npx" { "npm" } else { "uv" };
    let environment = host_environment();
    let Some(manager) = resolve_executable(manager_name, &environment) else {
        return Err(err("runner_unavailable", format!(
            "ACP Registry agent {id} requires '{manager_name}', but it is not available on this environment's PATH."
        )));
    };
    let receipt = read_receipt(roots, agent, distribution, spec, &manager);
    if let Some(receipt) = receipt.clone().filter(|_| distribution == "npx") {
        return Ok(receipt);
    }
    let package_root = package_install_root(roots, agent, distribution);
    std::fs::create_dir_all(&package_root).map_err(|_| {
        err(
            "install_failed",
            format!("Could not create the managed install directory for {id}."),
        )
    })?;
    let mut package_env = environment.clone();
    if distribution == "npx" {
        package_env.insert(
            "npm_config_prefix".into(),
            package_root.clone().into_os_string(),
        );
    } else {
        package_env.insert("UV_TOOL_DIR".into(), package_root.clone().into_os_string());
        package_env.insert(
            "UV_TOOL_BIN_DIR".into(),
            package_bin_directory(roots, agent, distribution).into_os_string(),
        );
    }
    let discover = |env: &HashMap<OsString, OsString>| {
        let env = env.clone();
        let (agent, spec, manager) = (agent.clone(), spec.to_owned(), manager.clone());
        let npx = distribution == "npx";
        async move {
            if npx {
                discover_npm(&agent, &spec, &manager, &env).await
            } else {
                discover_uv(&agent, &spec, &manager, &env).await
            }
        }
    };
    // A uv receipt counts only while uv still lists the tool.
    if let Some(receipt) = receipt {
        if discover(&package_env).await?.is_some() {
            return Ok(receipt);
        }
    }
    if let Some(existing) = discover(&package_env).await? {
        write_receipt(roots, &existing)?;
        return Ok(existing);
    }
    let install_env = with_preferred_path(&package_env, &manager_bin_directory(&manager));
    let args: Vec<&str> = if distribution == "npx" {
        vec!["install", "--global", spec]
    } else {
        vec!["tool", "install", "--force", spec]
    };
    run_command(
        &manager,
        &args,
        None,
        Some(&install_env),
        Some(PACKAGE_INSTALL_TIMEOUT),
        MAX_COMMAND_OUTPUT_BYTES,
        "install_failed",
    )
    .await?;
    match discover(&package_env).await? {
        Some(installed) => {
            write_receipt(roots, &installed)?;
            Ok(installed)
        }
        None => Err(err(
            "install_failed",
            format!("ACP Registry installed {spec}, but could not resolve its global command."),
        )),
    }
}

/// How an installed registry agent is launched: its command, the
/// distribution's arguments and environment, and the folder its package
/// manager put the command's siblings in (first on its PATH).
pub(crate) struct AgentLaunch {
    pub command: PathBuf,
    pub args: Vec<String>,
    pub env: Vec<(String, String)>,
    pub bin_directory: Option<PathBuf>,
}

/// `resolveWithRegistry` once the agent and its distribution are known: the
/// agent installed (or the install it has reused) and how it is launched.
/// `command_override` (the instance's `commandPath`) replaces the
/// distribution's executable, keeping its arguments and environment.
pub(crate) async fn launch(
    roots: &Roots,
    agent: &Value,
    kind: &str,
    binary: Option<(&str, &Value)>,
    command_override: &str,
) -> Result<AgentLaunch, AcpError> {
    let id = agent["id"].as_str().unwrap_or_default().to_owned();
    let source = match (kind, binary) {
        ("binary", Some((_, target))) => target,
        _ => &agent["distribution"][kind],
    };
    let args: Vec<String> = source["args"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|arg| arg.as_str().map(str::to_owned))
        .collect();
    let env: Vec<(String, String)> = source["env"]
        .as_object()
        .into_iter()
        .flatten()
        .filter_map(|(name, value)| Some((name.clone(), value.as_str()?.to_owned())))
        .collect();
    if !command_override.is_empty() {
        let command = resolve_executable(command_override, &host_environment()).ok_or_else(|| {
            err("runner_unavailable", format!(
                "ACP Registry agent {id} requires '{command_override}', but it is not available on this provider instance's PATH."
            ))
        })?;
        return Ok(AgentLaunch {
            command,
            args,
            env,
            bin_directory: None,
        });
    }
    let lock = permit(&id);
    let _permit = lock.lock().await;
    match (kind, binary) {
        ("binary", Some((target_name, target))) => {
            let command = install_binary(roots, agent, target_name, target).await?;
            Ok(AgentLaunch {
                command,
                args,
                env,
                bin_directory: None,
            })
        }
        _ => {
            let spec = source["package"].as_str().unwrap_or_default();
            let receipt = ensure_package(roots, agent, kind, spec).await?;
            let path = |key: &str| receipt.get(key).and_then(Value::as_str).map(PathBuf::from);
            let command = path("executablePath").ok_or_else(|| {
                err(
                    "install_failed",
                    format!(
                        "ACP Registry installed {spec}, but could not resolve its global command."
                    ),
                )
            })?;
            Ok(AgentLaunch {
                command,
                args,
                env,
                bin_directory: path("binDirectory"),
            })
        }
    }
}

/// `prepare` once the agent and its distribution are known: install it.
pub(crate) async fn install(
    roots: &Roots,
    agent: &Value,
    kind: &str,
    binary: Option<(&str, &Value)>,
) -> Result<Value, AcpError> {
    let id = agent["id"].as_str().unwrap_or_default().to_owned();
    let lock = permit(&id);
    let _permit = lock.lock().await;
    match (kind, binary) {
        ("binary", Some((target_name, target))) => {
            install_binary(roots, agent, target_name, target).await?;
            reserve(&id);
        }
        _ => {
            let spec = agent["distribution"][kind]["package"]
                .as_str()
                .unwrap_or_default();
            ensure_package(roots, agent, kind, spec).await?;
        }
    }
    Ok(json!({"agentId": id, "version": agent["version"], "distribution": kind, "prepared": true}))
}

/// `uninstallManagedBinary`: removes an agent's downloaded binaries unless a provider
/// instance uses the agent or it was just prepared.
pub(crate) async fn uninstall(
    roots: &Roots,
    settings: Result<Value, ()>,
    agent_id: &str,
) -> Result<Value, AcpError> {
    let lock = permit(agent_id);
    let _permit = lock.lock().await;
    let valid = agent_id.len() <= 128
        && agent_id
            .chars()
            .next()
            .is_some_and(|c| c.is_ascii_lowercase() || c.is_ascii_digit())
        && agent_id
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || "._-".contains(c));
    let settings = settings.map_err(|_| {
        err(
            "install_failed",
            "Could not read provider settings while checking managed ACP binary references.",
        )
    })?;
    if !valid {
        return Err(err(
            "install_failed",
            "ACP Registry managed binary uninstall received an invalid agent ID.",
        ));
    }
    let referenced = settings["providerInstances"]
        .as_object()
        .is_some_and(|instances| {
            instances.values().any(|instance| {
                let config = &instance["config"];
                instance["driver"] == "acpRegistry"
                    && config.is_object()
                    && config["source"] != "local"
                    && config["agentId"].as_str().map(str::trim) == Some(agent_id)
            })
        });
    if referenced {
        RESERVATIONS.lock().unwrap().remove(agent_id);
        return Ok(json!({"agentId": agent_id, "removed": false}));
    }
    if reserved(agent_id) {
        return Ok(json!({"agentId": agent_id, "removed": false}));
    }
    let agent_root = roots.tools.join(agent_id);
    if !agent_root.exists() {
        return Ok(json!({"agentId": agent_id, "removed": false}));
    }
    let failed = || {
        err(
            "install_failed",
            format!("Could not remove managed binaries for ACP Registry agent {agent_id}."),
        )
    };
    let platform =
        regex::Regex::new(r"^(?:darwin|linux|windows)-(?:aarch64|x86_64)$").expect("valid pattern");
    let mut removed = false;
    for version in std::fs::read_dir(&agent_root)
        .map_err(|_| failed())?
        .flatten()
    {
        let version_root = version.path();
        for entry in std::fs::read_dir(&version_root)
            .map_err(|_| failed())?
            .flatten()
        {
            if !platform.is_match(&entry.file_name().to_string_lossy()) {
                continue;
            }
            std::fs::remove_dir_all(entry.path())
                .or_else(|_| std::fs::remove_file(entry.path()))
                .map_err(|_| failed())?;
            removed = true;
        }
        if std::fs::read_dir(&version_root)
            .map_err(|_| failed())?
            .next()
            .is_none()
        {
            std::fs::remove_dir_all(&version_root).map_err(|_| failed())?;
        }
    }
    if std::fs::read_dir(&agent_root)
        .map_err(|_| failed())?
        .next()
        .is_none()
    {
        std::fs::remove_dir_all(&agent_root).map_err(|_| failed())?;
    }
    Ok(json!({"agentId": agent_id, "removed": removed}))
}

/// The binary target for this platform, if the agent installs as a binary.
pub(crate) fn binary_target(agent: &Value) -> Option<(&'static str, &Value)> {
    let candidates: &[&str] = match crate::acp_registry::platform_target() {
        Some("windows-aarch64") => &["windows-aarch64", "windows-x86_64"],
        Some("linux-x86_64") => &["linux-x86_64"],
        Some("linux-aarch64") => &["linux-aarch64"],
        Some("darwin-x86_64") => &["darwin-x86_64"],
        Some("darwin-aarch64") => &["darwin-aarch64"],
        Some(_) => &["windows-x86_64"],
        None => &[],
    };
    candidates
        .iter()
        .find_map(|t| agent["distribution"]["binary"].get(*t).map(|v| (*t, v)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn command_paths_must_stay_relative() {
        assert_eq!(
            command_segments("./bin/agent"),
            Some(vec!["bin".into(), "agent".into()])
        );
        assert_eq!(command_segments("../agent"), None);
        assert_eq!(command_segments("/usr/bin/agent"), None);
        assert_eq!(command_segments("C:/agent.exe"), None);
        assert!(safe_listing("./\nagent/\nagent/run\n"));
        assert!(!safe_listing("agent/../../etc/passwd\n"));
    }

    #[test]
    fn package_specs_split_like_the_reference() {
        assert_eq!(
            package_identity("@scope/agent@1.2.3", "npx"),
            ("@scope/agent".into(), "1.2.3".into())
        );
        assert_eq!(
            package_identity("agent@v2.0.0", "npx"),
            ("agent".into(), "2.0.0".into())
        );
        assert_eq!(
            package_identity("claudette==2.0.0", "uvx"),
            ("claudette".into(), "2.0.0".into())
        );
        assert_eq!(
            archive_kind("https://example.invalid/a.TGZ"),
            ArchiveKind::TarGz
        );
        assert_eq!(
            archive_kind("https://example.invalid/agent"),
            ArchiveKind::Raw
        );
    }

    #[test]
    fn npm_commands_follow_the_bin_map() {
        let agent = json!({"id": "helper"});
        assert_eq!(
            npm_command_name(&agent, "@s/tool", &json!({"bin": "cli.js"})).as_deref(),
            Some("tool")
        );
        assert_eq!(
            npm_command_name(&agent, "@s/tool", &json!({"bin": {"a": "x", "tool": "y"}}))
                .as_deref(),
            Some("tool")
        );
        assert_eq!(
            npm_command_name(&agent, "@s/x", &json!({"bin": {"a": "1", "helper": "2"}})).as_deref(),
            Some("helper")
        );
        assert_eq!(
            npm_command_name(&agent, "@s/x", &json!({"bin": {"a": "1", "b": "2"}})),
            None
        );
    }
}
