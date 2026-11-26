//! The headless browser previews and HTML renders launch, as the reference's
//! `PreviewBrowser` (`apps/server/src/preview/PreviewBrowser.ts`): only the
//! pinned Chrome for Testing `chrome-headless-shell`, kept in
//! `<T3 home>/tools/chrome-headless-shell/<platform>/<version>/` and installed
//! there the first time a caller needs it. The download is checked against
//! the pinned size and SHA-256 before it is unpacked, and the unpacked folder
//! is published with one rename. An install belongs to the server, not to its
//! caller: a caller waits a bounded time and is otherwise told how far it got.
use futures_util::StreamExt;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;
use tokio::io::AsyncWriteExt;
use tokio::sync::watch;

// Update the version, sizes and hashes from the reference's PreviewBrowser.ts
// (each chrome-headless-shell zip of
// https://googlechromelabs.github.io/chrome-for-testing/known-good-versions-with-downloads.json).
pub(crate) const VERSION: &str = "158.0.8127.88";
const ARCHIVES: [(&str, u64, &str); 6] = [
    (
        "linux64",
        132_091_593,
        "63401333cc587645bfaeea7f0c90958b327d249251e23954a47f30112ca23d5b",
    ),
    (
        "linux-arm64",
        129_942_265,
        "9f89b7ef6574e4bbf6ddda9f17f1fab5626e93d6f7d852653232d0d3554df975",
    ),
    (
        "mac-arm64",
        105_060_087,
        "69320f6869c4defbbdac8b8fa83aef65dc63975e83bbd06899b75dbea5c464b3",
    ),
    (
        "mac-x64",
        112_314_759,
        "dc1ac31965cb2ec7794aed8c66127aceea304ae15353cd56fc4b8bde44091548",
    ),
    (
        "win32",
        122_509_314,
        "936ad5e7f530bdd66b7261d0a2ceed76b057d429cf31b770a95fd903a7969856",
    ),
    (
        "win64",
        127_879_225,
        "e935ea2d4633495b5e9ab7bc66bcd982806329c0ee9ecbafc1ac9596a3b34cdb",
    ),
];
const DOWNLOAD_TIMEOUT: Duration = Duration::from_secs(15 * 60);
/// How long a caller waits on an install in progress; provider tool calls time out near 60s.
const WAIT: Duration = Duration::from_secs(45);
const STAGING_STALE_AFTER: Duration = Duration::from_secs(60 * 60);
const NETWORK: &str =
    "The download from storage.googleapis.com failed. Check this machine's network access.";
const UNPACK: &str = "Could not unpack the browser. Check free disk space in T3's home directory.";
const SAVE: &str = "Could not save the browser in T3's home directory.";

/// The host as Node names it (`process.platform`, `process.arch`).
fn node_host() -> (&'static str, &'static str) {
    let platform = match std::env::consts::OS {
        "macos" => "darwin",
        "windows" => "win32",
        other => other,
    };
    let arch = match std::env::consts::ARCH {
        "x86_64" => "x64",
        "aarch64" => "arm64",
        "x86" => "ia32",
        other => other,
    };
    (platform, arch)
}

/// `chromePlatform`: Chrome for Testing's name for the host, if it builds one.
fn chrome_platform(platform: &str, arch: &str) -> Option<&'static str> {
    match (platform, arch) {
        ("linux", "x64") => Some("linux64"),
        ("linux", "arm64") => Some("linux-arm64"),
        ("darwin", "arm64") => Some("mac-arm64"),
        ("darwin", "x64") => Some("mac-x64"),
        ("win32", "ia32") => Some("win32"),
        // There is no Windows arm64 build; Windows on Arm runs the x64 one under emulation.
        ("win32", "x64" | "arm64") => Some("win64"),
        _ => None,
    }
}

#[derive(Clone, Debug)]
pub(crate) struct Release {
    pub version: String,
    pub platform: String,
    pub url: String,
    pub bytes: u64,
    pub sha256: String,
}

fn release_for(platform: &str, arch: &str) -> Option<Release> {
    let chrome = chrome_platform(platform, arch)?;
    let (_, bytes, sha256) = ARCHIVES.iter().find(|(name, ..)| *name == chrome)?;
    Some(Release {
        version: VERSION.into(),
        platform: chrome.into(),
        url: format!("https://storage.googleapis.com/chrome-for-testing-public/{VERSION}/{chrome}/chrome-headless-shell-{chrome}.zip"),
        bytes: *bytes,
        sha256: (*sha256).into(),
    })
}

/// Why no browser can be launched yet, worded as the reference's errors.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum BrowserError {
    /// `PreviewBrowserInstallError`; `id` tells one failed install from the next.
    Install { detail: String, id: u64 },
    /// `PreviewBrowserInstallingError`.
    Installing {
        downloaded: u64,
        total: u64,
        unpacking: bool,
    },
    /// `PreviewBrowserUnsupportedError`.
    Unsupported { platform: String, arch: String },
}

/// `Math.round(bytes / 1_000_000)`.
fn megabytes(bytes: u64) -> u64 {
    (bytes as f64 / 1_000_000.0 + 0.5).floor() as u64
}

impl BrowserError {
    pub(crate) fn message(&self) -> String {
        match self {
            BrowserError::Install { detail, .. } => {
                format!("T3 could not install its headless browser: {detail} Try again.")
            }
            BrowserError::Installing {
                downloaded,
                total,
                unpacking,
            } => {
                let progress = if *unpacking {
                    "unpacking".to_owned()
                } else {
                    format!(
                        "{} of {} MB downloaded",
                        megabytes(*downloaded),
                        megabytes(*total)
                    )
                };
                format!("T3 is installing its headless browser ({progress}). Try again in a minute.")
            }
            BrowserError::Unsupported { platform, arch } => format!(
                "T3's headless browser is not available on {platform}-{arch}: Chrome for Testing has no headless shell for it."
            ),
        }
    }
}

/// A failure inside an install, before it gets its id.
struct Failure(String);

fn failure(detail: &str) -> impl Fn(std::io::Error) -> Failure + '_ {
    move |_| Failure(detail.to_owned())
}

#[derive(Clone)]
enum State {
    Idle,
    Installing(watch::Receiver<Option<Result<PathBuf, BrowserError>>>),
    Installed(PathBuf),
    Failed(BrowserError),
}

#[derive(Default)]
struct Progress {
    downloaded: u64,
    unpacking: bool,
}

pub(crate) struct PreviewBrowser {
    release: Option<Release>,
    host: (String, String),
    install_root: PathBuf,
    executable_name: &'static str,
    wait: Duration,
    state: tokio::sync::Mutex<State>,
    progress: Mutex<Progress>,
    next_failure: std::sync::atomic::AtomicU64,
}

/// The browser of the T3 home `base_dir`, one per home for the server's life.
pub(crate) fn for_home(base_dir: &Path) -> Arc<PreviewBrowser> {
    static BROWSERS: OnceLock<Mutex<HashMap<PathBuf, Arc<PreviewBrowser>>>> = OnceLock::new();
    let (platform, arch) = node_host();
    BROWSERS
        .get_or_init(Default::default)
        .lock()
        .unwrap()
        .entry(base_dir.to_path_buf())
        .or_insert_with(|| {
            Arc::new(PreviewBrowser::new(
                base_dir,
                release_for(platform, arch),
                (platform, arch),
                WAIT,
            ))
        })
        .clone()
}

impl PreviewBrowser {
    fn new(
        base_dir: &Path,
        release: Option<Release>,
        (platform, arch): (&str, &str),
        wait: Duration,
    ) -> Self {
        let folder = release
            .as_ref()
            .map_or_else(|| format!("{platform}-{arch}"), |r| r.platform.clone());
        PreviewBrowser {
            install_root: base_dir
                .join("tools")
                .join("chrome-headless-shell")
                .join(folder),
            executable_name: if platform == "win32" {
                "chrome-headless-shell.exe"
            } else {
                "chrome-headless-shell"
            },
            release,
            host: (platform.into(), arch.into()),
            wait,
            state: tokio::sync::Mutex::new(State::Idle),
            progress: Mutex::new(Progress::default()),
            next_failure: Default::default(),
        }
    }

    /// `installedExecutable`: the release's executable, if it is installed.
    fn installed_executable(&self, release: &Release) -> Option<PathBuf> {
        let executable = self
            .install_root
            .join(&release.version)
            .join(self.executable_name);
        let meta = std::fs::metadata(&executable).ok()?;
        #[cfg(unix)]
        let runnable = {
            use std::os::unix::fs::PermissionsExt;
            meta.permissions().mode() & 0o111 != 0
        };
        #[cfg(not(unix))]
        let runnable = true;
        (meta.is_file() && runnable).then_some(executable)
    }

    /// `installed`: the browser if it is installed, without installing it.
    pub(crate) fn installed(&self) -> Option<PathBuf> {
        self.installed_executable(self.release.as_ref()?)
    }

    /// `executable`: the browser, installing it first if need be. A caller
    /// waits for an install up to `wait`, then hears how far it got.
    pub(crate) async fn executable(self: &Arc<Self>) -> Result<PathBuf, BrowserError> {
        let Some(release) = self.release.clone() else {
            return Err(BrowserError::Unsupported {
                platform: self.host.0.clone(),
                arch: self.host.1.clone(),
            });
        };
        let mut done = match self.join(&release).await {
            State::Installed(executable) => return Ok(executable),
            State::Failed(error) => return Err(error),
            State::Installing(done) => done,
            State::Idle => unreachable!("join never leaves the install idle"),
        };
        let finished = tokio::time::timeout(self.wait, async {
            loop {
                if let Some(result) = done.borrow_and_update().clone() {
                    return result;
                }
                if done.changed().await.is_err() {
                    return Err(BrowserError::Install {
                        detail: "The install stopped.".into(),
                        id: u64::MAX,
                    });
                }
            }
        })
        .await;
        match finished {
            Ok(Ok(executable)) => Ok(executable),
            Ok(Err(error)) => {
                // This caller reports the failure, so the next one tries again.
                let mut state = self.state.lock().await;
                if matches!(&*state, State::Failed(failed) if *failed == error) {
                    *state = State::Idle;
                }
                Err(error)
            }
            Err(_) => {
                let progress = self.progress.lock().unwrap();
                Err(BrowserError::Installing {
                    downloaded: progress.downloaded,
                    total: release.bytes,
                    unpacking: progress.unpacking,
                })
            }
        }
    }

    /// `join`: the install as it stands, starting one when none is installed.
    /// A failure is reported once; the call after it tries again.
    async fn join(self: &Arc<Self>, release: &Release) -> State {
        let mut state = self.state.lock().await;
        match &*state {
            State::Installing(_) => return state.clone(),
            State::Failed(_) => return std::mem::replace(&mut *state, State::Idle),
            _ => {}
        }
        if let Some(executable) = self.installed_executable(release) {
            *state = State::Installed(executable);
            return state.clone();
        }
        *self.progress.lock().unwrap() = Progress::default();
        let (sender, receiver) = watch::channel(None);
        *state = State::Installing(receiver);
        let this = self.clone();
        let release = release.clone();
        tokio::spawn(async move {
            let result =
                this.install(&release)
                    .await
                    .map_err(|Failure(detail)| BrowserError::Install {
                        detail,
                        id: this
                            .next_failure
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed),
                    });
            *this.state.lock().await = match &result {
                Ok(executable) => State::Installed(executable.clone()),
                Err(error) => State::Failed(error.clone()),
            };
            let _ = sender.send(Some(result));
        });
        state.clone()
    }

    async fn install(&self, release: &Release) -> Result<PathBuf, Failure> {
        let save = |_| Failure(SAVE.into());
        tokio::fs::create_dir_all(&self.install_root)
            .await
            .map_err(save)?;
        {
            // Staging shares the install root's filesystem so publishing is one rename.
            let staging = tempfile::Builder::new()
                .prefix(".install-")
                .tempdir_in(&self.install_root)
                .map_err(save)?;
            let archive = staging.path().join("download.zip");
            let unpacked = staging.path().join("browser");
            match tokio::time::timeout(DOWNLOAD_TIMEOUT, self.download(release, &archive)).await {
                Ok(result) => result?,
                Err(_) => return Err(Failure(NETWORK.into())),
            }
            self.progress.lock().unwrap().unpacking = true;
            let (platform, name) = (release.platform.clone(), self.executable_name);
            let (from, to) = (archive.clone(), unpacked.clone());
            tokio::task::spawn_blocking(move || extract(&platform, name, &from, &to))
                .await
                .map_err(|_| Failure(UNPACK.into()))??;
            let destination = self.install_root.join(&release.version);
            let _ = tokio::fs::remove_dir_all(&destination).await;
            tokio::fs::rename(&unpacked, &destination)
                .await
                .map_err(save)?;
        }
        self.remove_old_versions(&release.version);
        Ok(self
            .install_root
            .join(&release.version)
            .join(self.executable_name))
    }

    /// `download`: the pinned archive, checked by size and SHA-256.
    async fn download(&self, release: &Release, archive: &Path) -> Result<(), Failure> {
        let network = || Failure(NETWORK.into());
        let response = crate::provider_status::http_client()
            .get(&release.url)
            .send()
            .await
            .map_err(|_| network())?;
        if !response.status().is_success() {
            return Err(network());
        }
        let identity = response
            .headers()
            .get(reqwest::header::CONTENT_ENCODING)
            .and_then(|v| v.to_str().ok())
            .is_none_or(|v| v.trim().eq_ignore_ascii_case("identity"));
        let length = response
            .headers()
            .get(reqwest::header::CONTENT_LENGTH)
            .and_then(|v| v.to_str().ok())
            .map(|v| v.trim().parse::<f64>().unwrap_or(f64::NAN));
        if identity && length.is_some_and(|length| length != release.bytes as f64) {
            return Err(Failure(
                "The download size did not match the pinned release.".into(),
            ));
        }
        let mut options = tokio::fs::OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        options.mode(0o600);
        let mut file = options.open(archive).await.map_err(|_| network())?;
        let mut hash = Sha256::new();
        let mut stream = response.bytes_stream();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(|_| network())?;
            let downloaded = {
                let mut progress = self.progress.lock().unwrap();
                progress.downloaded += chunk.len() as u64;
                progress.downloaded
            };
            if downloaded > release.bytes {
                return Err(Failure(
                    "The download was larger than the pinned release.".into(),
                ));
            }
            hash.update(&chunk);
            file.write_all(&chunk).await.map_err(|_| network())?;
        }
        file.flush().await.map_err(|_| network())?;
        let downloaded = self.progress.lock().unwrap().downloaded;
        if downloaded != release.bytes || hex::encode(hash.finalize()) != release.sha256 {
            return Err(Failure(
                "The download failed its size or SHA-256 check. Nothing was installed.".into(),
            ));
        }
        Ok(())
    }

    /// Shared homes may still use the newest other version, and staging
    /// folders touched within the last hour may belong to another install.
    fn remove_old_versions(&self, version: &str) {
        let names: Vec<String> = std::fs::read_dir(&self.install_root)
            .into_iter()
            .flatten()
            .flatten()
            .filter_map(|e| e.file_name().into_string().ok())
            .collect();
        let previous = names
            .iter()
            .filter(|name| *name != version && !name.starts_with('.'))
            .max_by(|a, b| numeric_compare(a, b))
            .cloned();
        for name in names {
            if name == version || Some(&name) == previous.as_ref() {
                continue;
            }
            let target = self.install_root.join(&name);
            if name.starts_with(".install-") {
                let fresh = std::fs::metadata(&target)
                    .and_then(|m| m.modified())
                    .ok()
                    .and_then(|modified| modified.elapsed().ok())
                    .is_none_or(|age| age < STAGING_STALE_AFTER);
                if fresh {
                    continue;
                }
            }
            let removed = if target.is_dir() {
                std::fs::remove_dir_all(&target)
            } else {
                std::fs::remove_file(&target)
            };
            if let Err(error) = removed {
                tracing::warn!(name, %error, "could not remove an old preview browser");
            }
        }
    }
}

/// `localeCompare(…, { numeric: true })` for version folder names: runs of
/// digits compare by value.
fn numeric_compare(a: &str, b: &str) -> std::cmp::Ordering {
    fn parts(s: &str) -> Vec<Result<u128, String>> {
        let mut out = Vec::new();
        let mut chars = s.chars().peekable();
        while let Some(&c) = chars.peek() {
            let digit = c.is_ascii_digit();
            let mut run = String::new();
            while let Some(&c) = chars.peek() {
                if c.is_ascii_digit() != digit {
                    break;
                }
                run.push(c);
                chars.next();
            }
            out.push(if digit {
                Ok(run.parse().unwrap_or(u128::MAX))
            } else {
                Err(run.to_lowercase())
            });
        }
        out
    }
    let (a, b) = (parts(a), parts(b));
    for (x, y) in a.iter().zip(&b) {
        let order = match (x, y) {
            (Ok(x), Ok(y)) => x.cmp(y),
            (Err(x), Err(y)) => x.cmp(y),
            (Ok(_), Err(_)) => std::cmp::Ordering::Less,
            (Err(_), Ok(_)) => std::cmp::Ordering::Greater,
        };
        if order.is_ne() {
            return order;
        }
    }
    a.len().cmp(&b.len())
}

/// `extract`: the archive's `chrome-headless-shell-<platform>/` folder into
/// `destination`, refusing any entry that is unexpected or unsafe.
fn extract(
    platform: &str,
    executable_name: &str,
    archive: &Path,
    destination: &Path,
) -> Result<(), Failure> {
    let unpack = failure(UNPACK);
    let file = std::fs::File::open(archive)
        .map_err(|_| Failure("Could not open the verified archive.".into()))?;
    let mut zip = zip::ZipArchive::new(std::io::BufReader::new(file))
        .map_err(|_| Failure("Could not open the verified archive.".into()))?;
    let unsafe_entry = || Failure("The archive contains an unexpected or unsafe entry.".into());
    let root = format!("chrome-headless-shell-{platform}/");
    std::fs::create_dir(destination).map_err(&unpack)?;
    let mut found_executable = false;
    for index in 0..zip.len() {
        let entry = zip
            .by_index_raw(index)
            .map_err(|_| Failure("The archive could not be read.".into()))?;
        let name = entry.name().to_owned();
        let mode = entry.unix_mode().unwrap_or(0);
        let kind = mode & 0o170000;
        let directory = name.ends_with('/');
        let Some(relative) = name.strip_prefix(&root) else {
            return Err(unsafe_entry());
        };
        let traverses = relative
            .split('/')
            .any(|part| part == ".." || part.contains('\\'))
            || relative.starts_with('/');
        let kind_ok = kind == 0 || kind == if directory { 0o040000 } else { 0o100000 };
        let method_ok = matches!(
            entry.compression(),
            zip::CompressionMethod::Stored | zip::CompressionMethod::Deflated
        );
        if traverses || !kind_ok || entry.encrypted() || !method_ok {
            return Err(unsafe_entry());
        }
        let target = destination.join(relative);
        if directory {
            std::fs::create_dir_all(&target).map_err(&unpack)?;
            continue;
        }
        if let Some(parent) = target.parent() {
            std::fs::create_dir_all(parent).map_err(&unpack)?;
        }
        drop(entry);
        let mut entry = zip
            .by_index(index)
            .map_err(|_| Failure("Could not unpack the browser.".into()))?;
        let mut out = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&target)
            .map_err(&unpack)?;
        std::io::copy(&mut entry, &mut out)
            .map_err(|_| Failure("Could not unpack the browser.".into()))?;
        // The binary and its shared libraries carry exec bits in the archive.
        #[cfg(unix)]
        if mode & 0o777 != 0 {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&target, std::fs::Permissions::from_mode(mode & 0o777))
                .map_err(&unpack)?;
        }
        if relative == executable_name {
            found_executable = true;
        }
    }
    if !found_executable {
        return Err(Failure(
            "The archive does not contain chrome-headless-shell.".into(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    /// A zip holding `entries` (name, contents, unix mode).
    fn archive(entries: &[(&str, &[u8], u32)]) -> Vec<u8> {
        let mut zip = zip::ZipWriter::new(std::io::Cursor::new(Vec::new()));
        for (name, contents, mode) in entries {
            let options = zip::write::SimpleFileOptions::default()
                .compression_method(zip::CompressionMethod::Deflated)
                .unix_permissions(*mode);
            if name.ends_with('/') {
                zip.add_directory(*name, options).unwrap();
            } else if mode & 0o170000 == 0o120000 {
                let target = std::str::from_utf8(contents).unwrap();
                zip.add_symlink(*name, target, options).unwrap();
            } else {
                zip.start_file(*name, options).unwrap();
                zip.write_all(contents).unwrap();
            }
        }
        zip.finish().unwrap().into_inner()
    }

    /// A loopback server answering every request with `body`, counting them.
    async fn serve(
        body: Vec<u8>,
        delay: Duration,
    ) -> (String, Arc<std::sync::atomic::AtomicUsize>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/shell.zip", listener.local_addr().unwrap());
        let hits = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let counted = hits.clone();
        tokio::spawn(async move {
            loop {
                let Ok((mut socket, _)) = listener.accept().await else {
                    return;
                };
                counted.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                let body = body.clone();
                tokio::spawn(async move {
                    use tokio::io::AsyncReadExt;
                    let mut request = [0u8; 4096];
                    let _ = socket.read(&mut request).await;
                    tokio::time::sleep(delay).await;
                    let head = format!(
                        "HTTP/1.1 200 OK\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
                        body.len()
                    );
                    let _ = socket.write_all(head.as_bytes()).await;
                    let _ = socket.write_all(&body).await;
                });
            }
        });
        (url, hits)
    }

    fn release(url: String, body: &[u8]) -> Release {
        Release {
            version: "200.0.0.1".into(),
            platform: "linux64".into(),
            url,
            bytes: body.len() as u64,
            sha256: hex::encode(Sha256::digest(body)),
        }
    }

    fn browser(home: &Path, release: Release, wait: Duration) -> Arc<PreviewBrowser> {
        Arc::new(PreviewBrowser::new(
            home,
            Some(release),
            ("linux", "x64"),
            wait,
        ))
    }

    fn shell_zip() -> Vec<u8> {
        archive(&[
            ("chrome-headless-shell-linux64/", b"", 0o755),
            (
                "chrome-headless-shell-linux64/chrome-headless-shell",
                b"#!/bin/sh\n",
                0o755,
            ),
            ("chrome-headless-shell-linux64/lib/libx.so", b"lib", 0o644),
        ])
    }

    #[test]
    fn errors_read_as_the_reference_words_them() {
        assert_eq!(
            BrowserError::Installing {
                downloaded: 12_500_000,
                total: 132_087_165,
                unpacking: false
            }
            .message(),
            "T3 is installing its headless browser (13 of 132 MB downloaded). Try again in a minute."
        );
        assert_eq!(
            BrowserError::Installing {
                downloaded: 0,
                total: 1,
                unpacking: true
            }
            .message(),
            "T3 is installing its headless browser (unpacking). Try again in a minute."
        );
        assert_eq!(
            BrowserError::Install {
                detail: NETWORK.into(),
                id: 0
            }
            .message(),
            "T3 could not install its headless browser: The download from storage.googleapis.com failed. Check this machine's network access. Try again."
        );
        assert_eq!(chrome_platform("win32", "arm64"), Some("win64"));
        assert_eq!(chrome_platform("freebsd", "x64"), None);
        let linux = release_for("linux", "x64").unwrap();
        assert_eq!(linux.url, "https://storage.googleapis.com/chrome-for-testing-public/158.0.8127.88/linux64/chrome-headless-shell-linux64.zip");
        assert_eq!(linux.bytes, 132_091_593);
        assert!(numeric_compare("158.0.10.1", "158.0.9.1").is_gt());
    }

    #[tokio::test]
    async fn installs_the_pinned_archive_once() {
        let home = tempfile::tempdir().unwrap();
        let body = shell_zip();
        let (url, hits) = serve(body.clone(), Duration::ZERO).await;
        let root = home.path().join("tools/chrome-headless-shell/linux64");
        // An older version stays (another server sharing the home may run it); older ones and
        // stale staging folders go.
        for old in ["100.0.0.1", "150.0.0.1"] {
            std::fs::create_dir_all(root.join(old)).unwrap();
        }
        let browser = browser(home.path(), release(url, &body), Duration::from_secs(20));
        assert_eq!(browser.installed(), None);
        let executable = browser.executable().await.unwrap();
        assert_eq!(executable, root.join("200.0.0.1/chrome-headless-shell"));
        assert!(root.join("200.0.0.1/lib/libx.so").is_file());
        assert_eq!(browser.installed(), Some(executable.clone()));
        let mut left: Vec<_> = std::fs::read_dir(&root)
            .unwrap()
            .map(|e| e.unwrap().file_name().into_string().unwrap())
            .collect();
        left.sort();
        assert_eq!(left, ["150.0.0.1", "200.0.0.1"]);
        assert_eq!(browser.executable().await.unwrap(), executable);
        assert_eq!(hits.load(std::sync::atomic::Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn a_bad_download_installs_nothing_and_the_next_call_retries() {
        let home = tempfile::tempdir().unwrap();
        let body = shell_zip();
        let (url, hits) = serve(body.clone(), Duration::ZERO).await;
        let mut pinned = release(url, &body);
        pinned.sha256 = "0".repeat(64);
        let browser = browser(home.path(), pinned, Duration::from_secs(20));
        let error = browser.executable().await.unwrap_err();
        assert_eq!(error.message(), "T3 could not install its headless browser: The download failed its size or SHA-256 check. Nothing was installed. Try again.");
        assert!(!home
            .path()
            .join("tools/chrome-headless-shell/linux64/200.0.0.1")
            .exists());
        browser.executable().await.unwrap_err();
        assert_eq!(hits.load(std::sync::atomic::Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn a_wrong_size_or_an_unsafe_entry_is_refused() {
        let home = tempfile::tempdir().unwrap();
        let body = shell_zip();
        let (url, _) = serve(body.clone(), Duration::ZERO).await;
        let mut pinned = release(url, &body);
        pinned.bytes += 1;
        let error = browser(home.path(), pinned, Duration::from_secs(20))
            .executable()
            .await
            .unwrap_err();
        assert!(error
            .message()
            .contains("The download size did not match the pinned release."));
        for entries in [
            vec![("chrome-headless-shell-linux64/../evil", &b"x"[..], 0o644)],
            vec![("elsewhere/chrome-headless-shell", &b"x"[..], 0o755)],
            vec![(
                "chrome-headless-shell-linux64/link",
                &b"/etc/passwd"[..],
                0o120777,
            )],
        ] {
            let body = archive(&entries);
            let (url, _) = serve(body.clone(), Duration::ZERO).await;
            let home = tempfile::tempdir().unwrap();
            let error = browser(home.path(), release(url, &body), Duration::from_secs(20))
                .executable()
                .await
                .unwrap_err();
            assert!(
                error.message().contains("unexpected or unsafe entry"),
                "{}",
                error.message()
            );
        }
        let body = archive(&[("chrome-headless-shell-linux64/readme", b"x", 0o644)]);
        let (url, _) = serve(body.clone(), Duration::ZERO).await;
        let error = browser(home.path(), release(url, &body), Duration::from_secs(20))
            .executable()
            .await
            .unwrap_err();
        assert!(error
            .message()
            .contains("does not contain chrome-headless-shell"));
    }

    #[tokio::test]
    async fn a_slow_install_reports_its_progress_and_finishes_for_the_next_caller() {
        let home = tempfile::tempdir().unwrap();
        let body = shell_zip();
        let (url, hits) = serve(body.clone(), Duration::from_millis(800)).await;
        let browser = browser(home.path(), release(url, &body), Duration::from_millis(100));
        let error = browser.executable().await.unwrap_err();
        assert!(
            matches!(
                error,
                BrowserError::Installing {
                    downloaded: 0,
                    unpacking: false,
                    ..
                }
            ),
            "{error:?}"
        );
        assert_eq!(
            error.message(),
            "T3 is installing its headless browser (0 of 0 MB downloaded). Try again in a minute."
        );
        tokio::time::sleep(Duration::from_millis(1500)).await;
        assert!(browser.executable().await.is_ok());
        assert_eq!(hits.load(std::sync::atomic::Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn an_unsupported_host_says_so() {
        let home = tempfile::tempdir().unwrap();
        let browser = Arc::new(PreviewBrowser::new(
            home.path(),
            release_for("freebsd", "x64"),
            ("freebsd", "x64"),
            WAIT,
        ));
        assert_eq!(
            browser.executable().await.unwrap_err().message(),
            "T3's headless browser is not available on freebsd-x64: Chrome for Testing has no headless shell for it."
        );
        assert_eq!(browser.installed(), None);
    }
}
