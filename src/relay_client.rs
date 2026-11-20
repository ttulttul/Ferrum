//! `cloud.getRelayClientStatus`: where the cloudflared relay client would come from.
//!
//! Mirrors `makeCloudflaredRelayClient(...).resolve` in the reference
//! `packages/shared/src/relayClient.ts`: an override from `T3CODE_CLOUDFLARED_PATH`, then the
//! pinned managed install under `<baseDir>/tools/cloudflared`, then older managed installs, then
//! `PATH`. With none usable the reply is `missing` (or `unsupported` on a platform without a
//! release asset); every variant carries the `version` the client's schema requires, so a host
//! without any relay tooling still answers `{status: "missing", version}`.

use serde_json::{json, Value};
use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    process::Stdio,
    sync::{Mutex, OnceLock},
    time::{Duration, SystemTime},
};

pub const CLOUDFLARED_VERSION: &str = "2026.10.4";
/// Minimum for `--output` support; older connectors exit on that flag.
pub const CLOUDFLARED_MIN_VERSION: &str = "2025.6.1";
const CLOUDFLARED_PATH_ENV_NAME: &str = "T3CODE_CLOUDFLARED_PATH";
const VERSION_PROBE_TIMEOUT: Duration = Duration::from_secs(10);

/// The host as Node names it (`process.platform`, `process.arch`).
pub fn node_platform() -> &'static str {
    match std::env::consts::OS {
        "macos" => "darwin",
        "windows" => "win32",
        other => other,
    }
}

pub fn node_arch() -> &'static str {
    match std::env::consts::ARCH {
        "x86_64" => "x64",
        "aarch64" => "arm64",
        "x86" => "ia32",
        other => other,
    }
}

fn has_release_asset(platform: &str, arch: &str) -> bool {
    matches!(
        (platform, arch),
        ("darwin", "arm64" | "x64") | ("linux", "arm64" | "x64") | ("win32", "x64" | "arm64")
    )
}

fn executable_file_name(platform: &str) -> &'static str {
    if platform == "win32" {
        "cloudflared.exe"
    } else {
        "cloudflared"
    }
}

/// `/\b(\d{4})\.(\d{1,2})\.(\d+)\b/u`, as numbers. JavaScript's `\d` and `\b` are ASCII-only.
fn parse_version(value: &str) -> Option<[u64; 3]> {
    static PATTERN: OnceLock<regex::Regex> = OnceLock::new();
    let pattern = PATTERN.get_or_init(|| {
        regex::Regex::new(r"(?-u)\b([0-9]{4})\.([0-9]{1,2})\.([0-9]+)\b").expect("valid pattern")
    });
    let captures = pattern.captures(value)?;
    let part = |i: usize| captures[i].parse::<u64>().ok();
    Some([part(1)?, part(2)?, part(3)?])
}

pub fn compare_versions(left: &str, right: &str) -> std::cmp::Ordering {
    parse_version(left)
        .unwrap_or_default()
        .cmp(&parse_version(right).unwrap_or_default())
}

/// `parseCloudflaredVersionOutput`: the first version in the output, without leading zeros.
pub fn parse_version_output(output: &str) -> Option<String> {
    parse_version(output).map(|[a, b, c]| format!("{a}.{b}.{c}"))
}

pub struct Options {
    pub base_dir: PathBuf,
    pub platform: String,
    pub arch: String,
    pub override_path: Option<String>,
    pub path_env: Option<String>,
}

impl Options {
    pub fn from_env(base_dir: &Path) -> Self {
        Self {
            base_dir: base_dir.to_path_buf(),
            platform: node_platform().to_owned(),
            arch: node_arch().to_owned(),
            override_path: std::env::var(CLOUDFLARED_PATH_ENV_NAME).ok(),
            path_env: std::env::var("PATH").ok(),
        }
    }
    fn managed_root(&self) -> PathBuf {
        self.base_dir.join("tools").join("cloudflared")
    }
    fn managed_path_for(&self, version: &str) -> PathBuf {
        self.managed_root()
            .join(version)
            .join(format!("{}-{}", self.platform, self.arch))
            .join(executable_file_name(&self.platform))
    }
}

fn executable_metadata(path: &Path, platform: &str) -> Option<std::fs::Metadata> {
    let metadata = std::fs::metadata(path).ok().filter(|m| m.is_file())?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if platform != "win32" && metadata.permissions().mode() & 0o111 == 0 {
            return None;
        }
    }
    let _ = platform;
    Some(metadata)
}

type ProbeKey = (PathBuf, u64, Option<SystemTime>);

/// Successful probes are cached per file revision; failures are retried next time.
async fn probe_version(path: &Path, platform: &str) -> Option<String> {
    static CACHE: OnceLock<Mutex<HashMap<ProbeKey, String>>> = OnceLock::new();
    let metadata = executable_metadata(path, platform)?;
    let key = (path.to_path_buf(), metadata.len(), metadata.modified().ok());
    let cache = CACHE.get_or_init(Default::default);
    if let Some(version) = cache.lock().unwrap_or_else(|e| e.into_inner()).get(&key) {
        return Some(version.clone());
    }
    let child = tokio::process::Command::new(path)
        .arg("version")
        .stdin(Stdio::null())
        .stderr(Stdio::null())
        .stdout(Stdio::piped())
        .kill_on_drop(true)
        .output();
    let output = tokio::time::timeout(VERSION_PROBE_TIMEOUT, child)
        .await
        .ok()?
        .ok()?;
    // The reference reads stdout whatever the exit status.
    let version = parse_version_output(&String::from_utf8_lossy(&output.stdout))?;
    cache
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .insert(key, version.clone());
    Some(version)
}

/// `compatibleCandidate` without an expected version, as `resolve` calls it; only `install`
/// insists that the pinned folder hold the pinned release.
async fn compatible_candidate(path: &Path, source: &str, platform: &str) -> Option<Value> {
    let version = probe_version(path, platform).await?;
    if compare_versions(&version, CLOUDFLARED_MIN_VERSION).is_lt() {
        return None;
    }
    Some(
        json!({"status":"available","executablePath":path.to_string_lossy(),"source":source,"version":version}),
    )
}

/// Managed folders older than the pin, newest first. Newer ones may belong to another server.
fn older_managed_versions(options: &Options) -> Vec<String> {
    let mut entries: Vec<String> = std::fs::read_dir(options.managed_root())
        .map(|dir| {
            dir.filter_map(|entry| entry.ok()?.file_name().into_string().ok())
                .filter(|name| {
                    parse_version(name).is_some()
                        && compare_versions(name, CLOUDFLARED_VERSION).is_lt()
                })
                .collect()
        })
        .unwrap_or_default();
    entries.sort_by(|left, right| compare_versions(right, left));
    entries
}

async fn resolve_path_executable(options: &Options) -> Option<Value> {
    let value = options.path_env.as_deref()?.trim();
    if value.is_empty() {
        return None;
    }
    let delimiter = if options.platform == "win32" {
        ';'
    } else {
        ':'
    };
    for directory in value.split(delimiter) {
        let directory = directory.trim();
        let directory = directory.strip_prefix('"').unwrap_or(directory);
        let directory = directory.strip_suffix('"').unwrap_or(directory);
        if directory.is_empty() {
            continue;
        }
        let candidate = Path::new(directory).join(executable_file_name(&options.platform));
        if executable_metadata(&candidate, &options.platform).is_none() {
            continue;
        }
        if let Some(available) = compatible_candidate(&candidate, "path", &options.platform).await {
            return Some(available);
        }
        tracing::warn!(
            executable_path = %candidate.display(),
            minimum_version = CLOUDFLARED_MIN_VERSION,
            "skipping an incompatible relay client on PATH"
        );
    }
    None
}

pub async fn resolve(options: &Options) -> Value {
    let missing = json!({"status":"missing","version":CLOUDFLARED_VERSION});
    let platform = options.platform.as_str();
    if let Some(path) = options
        .override_path
        .as_deref()
        .map(str::trim)
        .filter(|path| !path.is_empty())
    {
        if let Some(found) = compatible_candidate(Path::new(path), "override", platform).await {
            return found;
        }
        tracing::warn!(
            executable_path = path,
            "{CLOUDFLARED_PATH_ENV_NAME} must point to cloudflared {CLOUDFLARED_MIN_VERSION} or newer"
        );
        return missing;
    }
    let pinned = options.managed_path_for(CLOUDFLARED_VERSION);
    if let Some(found) = compatible_candidate(&pinned, "managed", platform).await {
        return found;
    }
    for version in older_managed_versions(options) {
        let older = options.managed_path_for(&version);
        if let Some(found) = compatible_candidate(&older, "managed", platform).await {
            return found;
        }
    }
    if let Some(found) = resolve_path_executable(options).await {
        return found;
    }
    if has_release_asset(platform, &options.arch) {
        missing
    } else {
        json!({"status":"unsupported","platform":platform,"arch":options.arch,"version":CLOUDFLARED_VERSION})
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;

    fn fake(dir: &Path, version_line: &str) -> PathBuf {
        std::fs::create_dir_all(dir).unwrap();
        let path = dir.join("cloudflared");
        std::fs::write(
            &path,
            format!("#!/bin/sh\n[ \"$1\" = version ] && echo '{version_line}'\n"),
        )
        .unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
        // A child another test forked while the script was open for writing keeps that descriptor
        // until it execs, and running the script meanwhile fails with ETXTBSY. Wait it out.
        for _ in 0..200 {
            match std::process::Command::new(&path).arg("version").output() {
                Err(error) if error.raw_os_error() == Some(libc::ETXTBSY) => {
                    std::thread::sleep(std::time::Duration::from_millis(10));
                }
                _ => break,
            }
        }
        path
    }

    fn options(base: &Path, path_env: Option<String>) -> Options {
        Options {
            base_dir: base.to_path_buf(),
            platform: "linux".into(),
            arch: "x64".into(),
            override_path: None,
            path_env,
        }
    }

    #[tokio::test]
    async fn missing_tooling_reports_missing_with_the_pinned_version() {
        let base = tempfile::tempdir().unwrap();
        let empty = base.path().join("empty-bin");
        std::fs::create_dir_all(&empty).unwrap();
        let status = resolve(&options(base.path(), Some(empty.display().to_string()))).await;
        assert_eq!(
            status,
            json!({"status":"missing","version":CLOUDFLARED_VERSION})
        );
        let status = resolve(&options(base.path(), None)).await;
        assert_eq!(
            status,
            json!({"status":"missing","version":CLOUDFLARED_VERSION})
        );
    }

    #[tokio::test]
    async fn unsupported_platform_names_platform_and_arch() {
        let base = tempfile::tempdir().unwrap();
        let mut options = options(base.path(), None);
        options.platform = "freebsd".into();
        assert_eq!(
            resolve(&options).await,
            json!({"status":"unsupported","platform":"freebsd","arch":"x64","version":CLOUDFLARED_VERSION})
        );
    }

    #[tokio::test]
    async fn path_executable_is_available_when_new_enough() {
        let base = tempfile::tempdir().unwrap();
        let bin = base.path().join("bin");
        let path = fake(&bin, "cloudflared version 2026.03.0 (built 2026-03-01)");
        let path_env = format!(
            "{}:{}",
            base.path().join("nothing").display(),
            bin.display()
        );
        assert_eq!(
            resolve(&options(base.path(), Some(path_env))).await,
            json!({"status":"available","executablePath":path.to_string_lossy(),"source":"path","version":"2026.3.0"})
        );
    }

    #[tokio::test]
    async fn too_old_or_non_executable_path_entries_are_skipped() {
        let base = tempfile::tempdir().unwrap();
        let old = base.path().join("old");
        fake(&old, "cloudflared version 2025.1.0");
        let plain = base.path().join("plain");
        let not_exec = fake(&plain, "cloudflared version 2026.10.4");
        std::fs::set_permissions(&not_exec, std::fs::Permissions::from_mode(0o644)).unwrap();
        let path_env = format!("{}:{}", old.display(), plain.display());
        assert_eq!(
            resolve(&options(base.path(), Some(path_env))).await,
            json!({"status":"missing","version":CLOUDFLARED_VERSION})
        );
    }

    #[tokio::test]
    async fn pinned_managed_install_wins_over_path() {
        let base = tempfile::tempdir().unwrap();
        let bin = base.path().join("bin");
        fake(&bin, "cloudflared version 2026.3.0");
        let opts = options(base.path(), Some(bin.display().to_string()));
        assert_eq!(resolve(&opts).await["source"], "path");
        let pinned_dir = base
            .path()
            .join("tools/cloudflared")
            .join(CLOUDFLARED_VERSION)
            .join("linux-x64");
        let pinned = fake(
            &pinned_dir,
            &format!("cloudflared version {CLOUDFLARED_VERSION} (pinned)"),
        );
        assert_eq!(
            resolve(&opts).await,
            json!({"status":"available","executablePath":pinned.to_string_lossy(),"source":"managed","version":CLOUDFLARED_VERSION})
        );
    }

    #[tokio::test]
    async fn resolve_accepts_any_compatible_version_in_the_pinned_folder() {
        // Only `install` insists on the pinned release; `resolve` reports what the binary says.
        let base = tempfile::tempdir().unwrap();
        let pinned_dir = base
            .path()
            .join("tools/cloudflared")
            .join(CLOUDFLARED_VERSION)
            .join("linux-x64");
        let pinned = fake(&pinned_dir, "cloudflared version 2026.9.0");
        assert_eq!(
            resolve(&options(base.path(), None)).await,
            json!({"status":"available","executablePath":pinned.to_string_lossy(),"source":"managed","version":"2026.9.0"})
        );
    }

    #[tokio::test]
    async fn older_managed_versions_are_tried_newest_first() {
        let base = tempfile::tempdir().unwrap();
        let root = base.path().join("tools/cloudflared");
        fake(
            &root.join("2025.8.0/linux-x64"),
            "cloudflared version 2025.8.0",
        );
        let newest = fake(
            &root.join("2026.1.0/linux-x64"),
            "cloudflared version 2026.1.0",
        );
        // Newer than the pin: may belong to another server, never used.
        fake(
            &root.join("2027.1.0/linux-x64"),
            "cloudflared version 2027.1.0",
        );
        assert_eq!(
            resolve(&options(base.path(), None)).await,
            json!({"status":"available","executablePath":newest.to_string_lossy(),"source":"managed","version":"2026.1.0"})
        );
    }

    #[tokio::test]
    async fn override_is_exclusive() {
        let base = tempfile::tempdir().unwrap();
        let bin = base.path().join("bin");
        fake(&bin, "cloudflared version 2026.3.0");
        let mut opts = options(base.path(), Some(bin.display().to_string()));
        opts.override_path = Some(format!("  {}  ", base.path().join("absent").display()));
        assert_eq!(
            resolve(&opts).await,
            json!({"status":"missing","version":CLOUDFLARED_VERSION})
        );
        let chosen = fake(
            &base.path().join("override"),
            "cloudflared version 2025.6.1",
        );
        opts.override_path = Some(chosen.display().to_string());
        assert_eq!(
            resolve(&opts).await,
            json!({"status":"available","executablePath":chosen.to_string_lossy(),"source":"override","version":"2025.6.1"})
        );
        opts.override_path = Some("   ".into());
        assert_eq!(resolve(&opts).await["source"], "path");
    }

    #[test]
    fn version_parsing_matches_the_reference() {
        assert_eq!(
            parse_version_output("cloudflared version 2026.03.0 (built 2026-03-01-0000 UTC)"),
            Some("2026.3.0".into())
        );
        assert_eq!(parse_version_output("v1.2.3"), None);
        assert!(compare_versions("2025.10.0", "2025.6.1").is_gt());
        assert!(compare_versions("garbage", "2025.6.1").is_lt());
        // Non-ASCII digits are not digits to the reference's pattern.
        assert_eq!(
            parse_version_output("cloudflared version \u{0662}026.1.0"),
            None
        );
    }
}
