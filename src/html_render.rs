//! `html_preview` and `html_render`, as the reference's `HtmlRender` serves
//! them: a page an agent wrote, given inline or as a file, is checked,
//! bootstrapped with T3's theme and its local images inlined. A preview
//! renders it in a headless browser and returns a screenshot with its height
//! and console output; a render stores it as the thread's `-html` attachment,
//! measured at the widths a client lays it out at, for the thread to show.
//! The browser runs headless on a throwaway profile, reaches only public
//! addresses through a loopback proxy, and is driven over its DevTools pipe.
mod chrome;
mod page;
mod proxy;

use serde_json::{json, Value};
use std::path::{Path, PathBuf};
use std::sync::LazyLock;

pub(crate) use chrome::BrowserError;

const MIB: u64 = 1024 * 1024;
pub(crate) const MAX_IMAGE_BYTES: u64 = 10 * MIB;
pub(crate) const MAX_PAGE_BYTES: u64 = 25 * MIB;
const MIN_PREVIEW_WIDTH: i64 = 240;
const MAX_PREVIEW_WIDTH: i64 = 1_600;
const MAX_CONCURRENT_BROWSERS: usize = 2;
const MEASURE_TIMEOUT_SECONDS: u64 = 6;

pub(crate) static CONSTANTS: LazyLock<Value> = LazyLock::new(|| {
    serde_json::from_str(include_str!("html_render/constants.json")).expect("HTML render constants")
});

fn constant(key: &str) -> i64 {
    CONSTANTS[key].as_i64().unwrap_or_default()
}

fn format_mib(bytes: u64) -> String {
    format!("{:.1} MiB", bytes as f64 / MIB as f64)
}

/// What a page or its render failed with, with the reference's messages.
#[derive(Debug)]
pub(crate) enum Error {
    File(String),
    FileTooLarge(String, u64),
    ImagesNotFound(Vec<String>),
    ImageTooLarge(String, u64),
    PageTooLarge(u64),
    ViewportHeight(String),
    Store,
    NoBrowser,
    Browser(BrowserError),
}

impl Error {
    /// Whether the agent's page is at fault (`invalid_request`), not the server.
    pub(crate) fn invalid_page(&self) -> bool {
        matches!(
            self,
            Error::File(_)
                | Error::FileTooLarge(..)
                | Error::ImagesNotFound(_)
                | Error::ImageTooLarge(..)
                | Error::PageTooLarge(_)
                | Error::ViewportHeight(_)
        )
    }

    pub(crate) fn message(&self) -> String {
        match self {
            Error::File(path) => format!("{path} is not a readable HTML document. Pass an absolute path to a regular .html file that starts with <!doctype html> or <html>, or pass html instead."),
            Error::FileTooLarge(path, size) => format!("{path} is {}; a page must be at most {}.", format_mib(*size), format_mib(MAX_PAGE_BYTES)),
            Error::ImagesNotFound(paths) => format!("These local images could not be read: {}. Use absolute paths to existing image files, or remove them.", paths.join(", ")),
            Error::ImageTooLarge(path, size) => format!("{path} is {}; each local image must be at most {}.", format_mib(*size), format_mib(MAX_IMAGE_BYTES)),
            Error::PageTooLarge(size) => format!("With its images inlined the page is {}; the limit is {}. Use smaller images.", format_mib(*size), format_mib(MAX_PAGE_BYTES)),
            Error::ViewportHeight(found) => format!("The page uses a viewport height unit ({found}). The frame grows to fit the page, so a size that follows the viewport makes it grow again and again. Let content set the height, or use fixed pixel heights."),
            Error::Store => "The HTML render could not be saved.".into(),
            Error::NoBrowser => "T3 could not install its headless browser: this server does not download one, and none was found. Try again.".into(),
            Error::Browser(error) => error.message(),
        }
    }
}

/// A page given inline or as an absolute file path.
pub(crate) enum Source {
    Html(String),
    Path(String),
}

/// Where the server's own state lives, which a page file may not be.
pub(crate) struct Paths {
    pub state_dir: PathBuf,
    pub secrets: PathBuf,
    pub attachments: PathBuf,
}

fn real(path: &Path) -> PathBuf {
    std::fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf())
}

/// `isServerInternalPath`: the secrets, the state databases, `/dev/fd`.
fn server_internal(paths: &Paths, file: &Path) -> bool {
    let (state, secrets) = (real(&paths.state_dir), real(&paths.secrets));
    file.starts_with(&secrets)
        || (file.parent() == Some(state.as_path())
            && file
                .file_name()
                .is_some_and(|name| name.to_string_lossy().contains(".sqlite")))
        || file.starts_with("/dev/fd")
}

/// `readPageFile`: a regular, singly linked `.html` file outside the
/// server's state, opened without following links, that starts like a page.
fn read_page_file(paths: &Paths, file_path: &str) -> Result<String, Error> {
    let unreadable = || Error::File(file_path.to_owned());
    let path = Path::new(file_path);
    if !path.is_absolute() || file_path.starts_with("//") || file_path.starts_with("\\\\") {
        return Err(unreadable());
    }
    let real_path = std::fs::canonicalize(path).map_err(|_| unreadable())?;
    let html_name = real_path
        .extension()
        .is_some_and(|e| e.eq_ignore_ascii_case("html") || e.eq_ignore_ascii_case("htm"));
    if !html_name || server_internal(paths, &real_path) {
        return Err(unreadable());
    }
    let mut options = std::fs::OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);
    }
    let file = options.open(&real_path).map_err(|_| unreadable())?;
    let meta = file.metadata().map_err(|_| unreadable())?;
    #[cfg(unix)]
    let single_link = {
        use std::os::unix::fs::MetadataExt;
        meta.nlink() == 1
    };
    #[cfg(not(unix))]
    let single_link = true;
    if !meta.is_file() || !single_link {
        return Err(unreadable());
    }
    if meta.len() > MAX_PAGE_BYTES {
        return Err(Error::FileTooLarge(file_path.to_owned(), meta.len()));
    }
    use std::io::Read;
    let mut bytes = Vec::with_capacity(meta.len() as usize);
    file.take(meta.len())
        .read_to_end(&mut bytes)
        .map_err(|_| unreadable())?;
    let html = String::from_utf8_lossy(&bytes).into_owned();
    static START: LazyLock<regex::Regex> =
        LazyLock::new(|| regex::Regex::new(r"(?i)^\s*<(?:!doctype\s+html|html)[\s>]").unwrap());
    if START.is_match(&html) {
        Ok(html)
    } else {
        Err(unreadable())
    }
}

fn read_source(paths: &Paths, source: &Source) -> Result<String, Error> {
    match source {
        Source::Html(html) => Ok(html.clone()),
        Source::Path(path) => read_page_file(paths, path),
    }
}

/// The bootstrap goes in first so its head scan never runs over image data.
fn inline(html: &str) -> Result<page::Inlined, Error> {
    page::inline_local_images(&page::inject_bootstrap(html))
}

/// `prepare`: a page ready to publish, every local image found.
fn prepare(html: &str) -> Result<String, Error> {
    page::reject_viewport_height(html)?;
    let inlined = inline(html)?;
    if !inlined.missing.is_empty() {
        return Err(Error::ImagesNotFound(inlined.missing));
    }
    Ok(inlined.html)
}

/// At most two browsers at a time.
static BROWSERS: LazyLock<tokio::sync::Semaphore> =
    LazyLock::new(|| tokio::sync::Semaphore::new(MAX_CONCURRENT_BROWSERS));

/// Chrome keeps its sandbox unless `T3CODE_SERVER_BROWSER_SANDBOX=0`.
fn no_sandbox() -> bool {
    std::env::var("T3CODE_SERVER_BROWSER_SANDBOX").is_ok_and(|v| v == "0")
}

fn executable() -> Option<PathBuf> {
    crate::local_services::browser_executable()
}

/// `clampHtmlRenderHeight`.
fn clamp_height(height: f64) -> i64 {
    (height.round() as i64).clamp(constant("minHeight"), constant("maxHeight"))
}

/// `toSafeThreadAttachmentSegment`.
fn thread_segment(thread_id: &str) -> Option<String> {
    let mut segment = String::new();
    for c in thread_id.trim().to_lowercase().chars() {
        let keep = c.is_ascii_alphanumeric() || c == '_' || c == '-';
        let next = if keep { c } else { '-' };
        if !(next == '-' && segment.ends_with('-')) {
            segment.push(next);
        }
    }
    let trimmed: String = segment
        .trim_matches(|c| c == '-' || c == '_')
        .chars()
        .take(80)
        .collect();
    let segment = trimmed.trim_end_matches(['-', '_']).to_owned();
    match segment.as_str() {
        "" => None,
        "pending" => Some("_pending".into()),
        _ => Some(segment),
    }
}

/// `HtmlRender.preview`: the page rendered at `width` in `appearance`.
pub(crate) async fn preview(
    paths: &Paths,
    source: Source,
    width: Option<f64>,
    appearance: Option<&str>,
) -> Result<Value, Error> {
    let width = width
        .map_or(constant("columnWidth"), |w| w.round() as i64)
        .clamp(MIN_PREVIEW_WIDTH, MAX_PREVIEW_WIDTH) as u64;
    let appearance = if appearance == Some("light") {
        "light"
    } else {
        "dark"
    };
    let html = read_source(paths, &source)?;
    page::reject_viewport_height(&html)?;
    let inlined = inline(&html)?;
    let executable = executable().ok_or(Error::NoBrowser)?;
    let fragment = CONSTANTS["fragments"][appearance]
        .as_str()
        .unwrap_or_default();
    let capture = {
        let _permit = BROWSERS.acquire().await.map_err(|_| Error::NoBrowser)?;
        chrome::capture(
            &executable,
            no_sandbox(),
            &inlined.html,
            width,
            fragment,
            appearance,
        )
        .await
        .map_err(Error::Browser)?
    };
    let mut out = json!({
        "width": width,
        "contentHeight": capture.content_height,
        "capturedHeight": capture.captured_height,
        "consoleMessages": capture.console.iter().map(chrome::ConsoleMessage::to_json).collect::<Vec<_>>(),
    });
    if !inlined.missing.is_empty() {
        out["missingImages"] = json!(inlined.missing);
    }
    out["screenshot"] = json!({
        "mimeType": "image/png",
        "data": capture.png,
        "width": width,
        "height": capture.captured_height,
    });
    Ok(out)
}

/// `measure`: the page's height at each width a client lays it out at, or
/// nothing when no browser can measure it in time.
async fn measure(html: &str) -> Option<Vec<(u64, u64)>> {
    let executable = executable()?;
    let widths: Vec<u64> = CONSTANTS["measureWidths"]
        .as_array()?
        .iter()
        .filter_map(Value::as_u64)
        .collect();
    let fragment = CONSTANTS["fragments"]["dark"].as_str().unwrap_or_default();
    let run = async {
        let _permit = BROWSERS.acquire().await.ok()?;
        chrome::measure(&executable, no_sandbox(), html, &widths, fragment)
            .await
            .map_err(|error| tracing::warn!(reason = %error.reason, "could not measure an HTML render; publishing it without heights"))
            .ok()
    };
    let mut heights =
        tokio::time::timeout(std::time::Duration::from_secs(MEASURE_TIMEOUT_SECONDS), run)
            .await
            .map_err(|_| {
                tracing::warn!(
                    "measuring an HTML render took longer than {MEASURE_TIMEOUT_SECONDS} seconds"
                )
            })
            .ok()
            .flatten()?;
    heights.sort_by_key(|(width, _)| *width);
    Some(heights)
}

/// `HtmlRender.publish`: the page stored as the thread's `-html` attachment,
/// and the reference the thread shows it by.
pub(crate) async fn publish(
    paths: &Paths,
    source: Source,
    thread_id: &str,
    title: &str,
    height: f64,
) -> Result<Value, Error> {
    let html = prepare(&read_source(paths, &source)?)?;
    let segment = thread_segment(thread_id).ok_or(Error::Store)?;
    let attachment_id = format!("{segment}-{}-html", crate::platform::uuid_v4());
    let file = paths.attachments.join(format!("{attachment_id}.html"));
    let max_title = constant("maxTitleLength") as usize;
    let title: String = {
        let mut units = 0;
        title
            .trim()
            .chars()
            .take_while(|c| {
                units += c.len_utf16();
                units <= max_title
            })
            .collect()
    };
    let mut reference = json!({
        "attachmentId": attachment_id,
        "title": if title.is_empty() { "HTML".to_owned() } else { title },
        "height": clamp_height(height),
    });
    std::fs::create_dir_all(&paths.attachments).map_err(|_| Error::Store)?;
    std::fs::write(&file, html.as_bytes()).map_err(|_| Error::Store)?;
    // An abandoned publish leaves no attachment behind.
    struct Remove(Option<PathBuf>);
    impl Drop for Remove {
        fn drop(&mut self) {
            if let Some(file) = self.0.take() {
                let _ = std::fs::remove_file(file);
            }
        }
    }
    let mut guard = Remove(Some(file));
    if let Some(heights) = measure(&html).await {
        reference["heights"] = json!(heights
            .iter()
            .map(|(w, h)| json!([w, h]))
            .collect::<Vec<_>>());
    }
    guard.0 = None;
    Ok(reference)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn thread_segments_match_the_reference() {
        assert_eq!(
            thread_segment("thread:Client--Thread").as_deref(),
            Some("thread-client-thread")
        );
        assert_eq!(thread_segment("  __x__ ").as_deref(), Some("x"));
        assert_eq!(thread_segment("pending").as_deref(), Some("_pending"));
        assert_eq!(thread_segment("::"), None);
    }

    #[test]
    fn a_page_file_must_be_a_single_html_document_outside_the_state() {
        let dir = tempfile::tempdir().unwrap();
        let paths = Paths {
            state_dir: dir.path().join("state"),
            secrets: dir.path().join("state/secrets"),
            attachments: dir.path().join("state/attachments"),
        };
        std::fs::create_dir_all(&paths.secrets).unwrap();
        let page = dir.path().join("page.html");
        std::fs::write(&page, "<!doctype html><p>hi</p>").unwrap();
        assert!(read_page_file(&paths, page.to_str().unwrap()).is_ok());
        let not_html = dir.path().join("notes.html");
        std::fs::write(&not_html, "API_KEY=1").unwrap();
        assert!(matches!(
            read_page_file(&paths, not_html.to_str().unwrap()),
            Err(Error::File(_))
        ));
        let secret = paths.secrets.join("x.html");
        std::fs::write(&secret, "<html></html>").unwrap();
        assert!(read_page_file(&paths, secret.to_str().unwrap()).is_err());
        assert!(read_page_file(&paths, "page.html").is_err());
        #[cfg(unix)]
        {
            let linked = dir.path().join("linked.html");
            std::fs::hard_link(&page, &linked).unwrap();
            assert!(read_page_file(&paths, linked.to_str().unwrap()).is_err());
        }
    }
}
