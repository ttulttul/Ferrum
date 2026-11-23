//! The reference's `headlessChrome`: a headless browser on a throwaway
//! profile, driven over the DevTools pipe (`--remote-debugging-pipe`, fd 3
//! in and fd 4 out, NUL-delimited JSON) so no debugging port is opened. The
//! page is served to the browser by Fetch interception as
//! `http://t3-page.localhost/page.html`; every other main-frame navigation is
//! refused, and the network goes only through the public-address proxy.
use super::proxy;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};

const MAX_CONSOLE_MESSAGES: usize = 20;
const MAX_CONSOLE_TEXT_CHARS: usize = 500;
const VIEWPORT_HEIGHT: u64 = 800;
const MAX_CAPTURE_HEIGHT: u64 = 4_000;
const CAPTURE_TIMEOUT_SECONDS: u64 = 20;
const PAGE_ORIGIN: &str = "http://t3-page.localhost";
const PAGE_URL: &str = "http://t3-page.localhost/page.html";
const PAGE_NAME: &str = "page.html";
const MEASURE_CONCURRENCY: usize = 3;
const SETTLE_EXPRESSION: &str = "document.fonts.ready.then(() => new Promise((resolve) => requestAnimationFrame(() => requestAnimationFrame(() => resolve(true)))))";
const MEASURE_EXPRESSION: &str = "(() => { const root = document.documentElement; return root.scrollHeight > root.clientHeight ? root.scrollHeight : root.getBoundingClientRect().height; })()";

/// `HtmlRenderBrowserError`.
#[derive(Debug, Clone)]
pub(crate) struct BrowserError {
    pub reason: String,
    pub exit_code: Option<i32>,
    pub signal: Option<String>,
}

impl BrowserError {
    pub(crate) fn new(reason: impl Into<String>) -> Self {
        BrowserError {
            reason: reason.into(),
            exit_code: None,
            signal: None,
        }
    }

    pub(crate) fn message(&self) -> String {
        let exit = match (&self.exit_code, &self.signal) {
            (Some(code), _) => format!(" (exit code {code})"),
            (None, Some(signal)) => format!(" (signal {signal})"),
            _ => String::new(),
        };
        format!(
            "Headless Chrome could not render the page: {}{exit}.",
            self.reason
        )
    }
}

/// A console message the page logged (`ConsoleMessage`).
#[derive(Debug, Clone)]
pub(crate) struct ConsoleMessage {
    pub level: &'static str,
    pub text: String,
}

impl ConsoleMessage {
    pub(crate) fn to_json(&self) -> Value {
        json!({"level": self.level, "text": self.text})
    }
}

/// `clipConsoleText`: at most 500 UTF-16 units, never half a surrogate pair.
fn clip(text: &str) -> String {
    let units: Vec<u16> = text.encode_utf16().collect();
    if units.len() <= MAX_CONSOLE_TEXT_CHARS {
        return text.to_owned();
    }
    let last = units[MAX_CONSOLE_TEXT_CHARS - 1];
    let end = if (0xd800..=0xdbff).contains(&last) {
        MAX_CONSOLE_TEXT_CHARS - 1
    } else {
        MAX_CONSOLE_TEXT_CHARS
    };
    format!("{}…", String::from_utf16_lossy(&units[..end]))
}

fn remote_object_text(value: &Value) -> String {
    match &value["value"] {
        Value::String(text) => text.clone(),
        other => value["description"].as_str().map_or_else(
            || {
                if other.is_null() && value.get("value").is_none() {
                    value["type"].as_str().unwrap_or_default().to_owned()
                } else {
                    js_string(other)
                }
            },
            str::to_owned,
        ),
    }
}

/// JavaScript's `String(value)` for a JSON value.
fn js_string(value: &Value) -> String {
    match value {
        Value::Null => "null".into(),
        Value::Bool(b) => b.to_string(),
        Value::Number(n) => match (n.as_i64(), n.as_f64()) {
            (Some(i), _) => i.to_string(),
            (None, Some(f)) => ryu_js::Buffer::new().format(f).to_owned(),
            _ => n.to_string(),
        },
        Value::String(s) => s.clone(),
        Value::Array(items) => items.iter().map(js_string).collect::<Vec<_>>().join(","),
        Value::Object(_) => "[object Object]".into(),
    }
}

fn console_level(kind: &str) -> Option<&'static str> {
    Some(match kind {
        "log" | "debug" | "dir" | "dirxml" | "table" | "trace" => "log",
        "info" => "info",
        "warning" => "warning",
        "error" | "assert" => "error",
        _ => return None,
    })
}

/// `consoleMessageFromEvent`.
fn console_message(method: &str, params: &Value) -> Option<ConsoleMessage> {
    match method {
        "Runtime.consoleAPICalled" => {
            let level = console_level(params["type"].as_str()?)?;
            let args = params["args"].as_array()?;
            Some(ConsoleMessage {
                level,
                text: args
                    .iter()
                    .map(remote_object_text)
                    .collect::<Vec<_>>()
                    .join(" "),
            })
        }
        "Runtime.exceptionThrown" => {
            let details = &params["exceptionDetails"];
            let text = details["text"].as_str()?;
            Some(ConsoleMessage {
                level: "error",
                text: details["exception"]["description"]
                    .as_str()
                    .unwrap_or(text)
                    .to_owned(),
            })
        }
        "Log.entryAdded" => {
            let entry = &params["entry"];
            let level = match entry["level"].as_str()? {
                "error" => "error",
                "warning" => "warning",
                _ => return None,
            };
            let text = entry["text"].as_str()?;
            Some(ConsoleMessage {
                level,
                text: match entry["url"].as_str().filter(|u| !u.is_empty()) {
                    Some(url) => format!("{text} {url}"),
                    None => text.to_owned(),
                },
            })
        }
        _ => None,
    }
}

struct PageState {
    main_frame: String,
    body: Option<Arc<String>>,
    loaded: Option<oneshot::Sender<Result<(), BrowserError>>>,
    console: Vec<ConsoleMessage>,
    omitted: usize,
}

type Pending = HashMap<u64, (String, oneshot::Sender<Result<Value, BrowserError>>)>;

struct Shared {
    pending: Mutex<Pending>,
    pages: Mutex<HashMap<String, PageState>>,
    disconnected: Mutex<Option<BrowserError>>,
    next_id: AtomicU64,
    outgoing: mpsc::UnboundedSender<Vec<u8>>,
}

impl Shared {
    fn post(&self, method: &str, params: Value, session: Option<&str>) {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed) + 1;
        let mut message = json!({"id": id, "method": method, "params": params});
        if let Some(session) = session {
            message["sessionId"] = json!(session);
        }
        let mut bytes = message.to_string().into_bytes();
        bytes.push(0);
        let _ = self.outgoing.send(bytes);
    }

    fn fulfill(&self, session: &str, request: &str, body: &str) {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed) + 1;
        let mut bytes = format!(
            "{{\"id\":{id},\"sessionId\":{},\"method\":\"Fetch.fulfillRequest\",\"params\":{{\"requestId\":{},\"responseCode\":200,\"responseHeaders\":[{{\"name\":\"Content-Type\",\"value\":\"text/html; charset=utf-8\"}}],\"body\":\"",
            json!(session),
            json!(request)
        )
        .into_bytes();
        bytes.extend_from_slice(body.as_bytes());
        bytes.extend_from_slice(b"\"}}\0");
        let _ = self.outgoing.send(bytes);
    }

    fn receive(&self, raw: &[u8]) {
        let Ok(message) = serde_json::from_slice::<Value>(raw) else {
            return;
        };
        if let Some(id) = message["id"].as_u64() {
            let Some((method, reply)) = self.pending.lock().unwrap().remove(&id) else {
                return;
            };
            let _ = reply.send(match message.get("error") {
                Some(_) => Err(BrowserError::new(format!("{method} failed"))),
                None => Ok(message["result"].clone()),
            });
            return;
        }
        let (Some(session), Some(method)) =
            (message["sessionId"].as_str(), message["method"].as_str())
        else {
            return;
        };
        let params = &message["params"];
        let mut pages = self.pages.lock().unwrap();
        let Some(page) = pages.get_mut(session) else {
            return;
        };
        match method {
            "Page.loadEventFired" => {
                if let Some(loaded) = page.loaded.take() {
                    let _ = loaded.send(Ok(()));
                }
            }
            "Fetch.requestPaused" => {
                let Some(request) = params["requestId"].as_str() else {
                    return;
                };
                let Some(url) = params["request"]["url"].as_str() else {
                    return;
                };
                let url = url.split('#').next().unwrap_or_default();
                if url == PAGE_URL {
                    if let Some(body) = page.body.clone() {
                        return self.fulfill(session, request, &body);
                    }
                }
                // Subframes may show other sites; the main frame stays on the page.
                let other_site_frame = params["resourceType"] == "Document"
                    && params["frameId"].as_str() != Some(page.main_frame.as_str())
                    && !url.starts_with(&format!("{PAGE_ORIGIN}/"));
                if other_site_frame {
                    self.post(
                        "Fetch.continueRequest",
                        json!({"requestId": request}),
                        Some(session),
                    );
                } else {
                    self.post(
                        "Fetch.failRequest",
                        json!({"requestId": request, "errorReason": "AccessDenied"}),
                        Some(session),
                    );
                }
            }
            _ => {
                let Some(console) = console_message(method, params) else {
                    return;
                };
                if page.console.len() >= MAX_CONSOLE_MESSAGES {
                    page.omitted += 1;
                    return;
                }
                page.console.push(ConsoleMessage {
                    level: console.level,
                    text: clip(&console.text.replace(PAGE_URL, PAGE_NAME)),
                });
            }
        }
    }

    fn disconnect(&self, error: BrowserError) {
        *self.disconnected.lock().unwrap() = Some(error.clone());
        for (_, (_, reply)) in self.pending.lock().unwrap().drain() {
            let _ = reply.send(Err(error.clone()));
        }
        for page in self.pages.lock().unwrap().values_mut() {
            if let Some(loaded) = page.loaded.take() {
                let _ = loaded.send(Err(error.clone()));
            }
        }
    }

    async fn send(
        &self,
        method: &str,
        params: Value,
        session: Option<&str>,
    ) -> Result<Value, BrowserError> {
        if let Some(error) = self.disconnected.lock().unwrap().clone() {
            return Err(error);
        }
        let id = self.next_id.fetch_add(1, Ordering::Relaxed) + 1;
        let (reply, answer) = oneshot::channel();
        self.pending
            .lock()
            .unwrap()
            .insert(id, (method.to_owned(), reply));
        let mut message = json!({"id": id, "method": method, "params": params});
        if let Some(session) = session {
            message["sessionId"] = json!(session);
        }
        let mut bytes = message.to_string().into_bytes();
        bytes.push(0);
        let _ = self.outgoing.send(bytes);
        answer
            .await
            .unwrap_or_else(|_| Err(BrowserError::new(format!("{method} failed"))))
    }
}

/// A launched browser, closed when dropped.
struct Browser {
    shared: Arc<Shared>,
    child: tokio::process::Child,
    proxy: proxy::Proxy,
    context: String,
    _profile: tempfile::TempDir,
}

impl Browser {
    #[cfg(unix)]
    async fn launch(
        executable: &std::path::Path,
        no_sandbox: bool,
    ) -> Result<Browser, BrowserError> {
        use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let proxy = proxy::start()
            .await
            .map_err(|_| BrowserError::new("the preview proxy could not start"))?;
        let profile = tempfile::Builder::new()
            .prefix("t3-html-preview-")
            .tempdir()
            .map_err(|_| BrowserError::new("a temporary directory could not be created"))?;
        let pipe = || -> Result<(OwnedFd, OwnedFd), BrowserError> {
            let mut fds = [0; 2];
            // SAFETY: `fds` has room for both descriptors pipe(2) writes.
            if unsafe { libc::pipe(fds.as_mut_ptr()) } != 0 {
                return Err(BrowserError::new("the browser could not be started"));
            }
            // SAFETY: pipe(2) returned two new descriptors this process owns.
            Ok(unsafe { (OwnedFd::from_raw_fd(fds[0]), OwnedFd::from_raw_fd(fds[1])) })
        };
        // The browser reads commands on fd 3 and writes replies on fd 4.
        let (browser_in, commands) = pipe()?;
        let (replies, browser_out) = pipe()?;
        let (child_in, child_out) = (browser_in.as_raw_fd(), browser_out.as_raw_fd());
        let mut command = crate::login_shell::tokio_command(executable);
        command
            .args([
                "--headless=new",
                "--remote-debugging-pipe",
                "--no-first-run",
                "--no-default-browser-check",
                "--disable-gpu",
                "--hide-scrollbars",
                "--mute-audio",
                "--block-new-web-contents",
            ])
            .arg(format!("--proxy-server=socks5://127.0.0.1:{}", proxy.port))
            // Loopback would otherwise skip the proxy.
            .arg("--proxy-bypass-list=<-loopback>")
            // WebRTC would otherwise send UDP, which no proxy carries.
            .arg("--force-webrtc-ip-handling-policy=disable_non_proxied_udp");
        if no_sandbox {
            command.arg("--no-sandbox");
        }
        command
            .arg(format!(
                "--user-data-dir={}",
                profile.path().join("profile").display()
            ))
            .arg("about:blank")
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::piped())
            .kill_on_drop(true);
        // SAFETY: dup2 is async-signal-safe and touches only the child's descriptors.
        unsafe {
            command.pre_exec(move || {
                if libc::dup2(child_in, 3) < 0 || libc::dup2(child_out, 4) < 0 {
                    return Err(std::io::Error::last_os_error());
                }
                Ok(())
            });
        }
        let mut child = command
            .spawn()
            .map_err(|_| BrowserError::new("the browser could not be started"))?;
        drop(browser_in);
        drop(browser_out);
        let stderr_tail: Arc<Mutex<String>> = Arc::default();
        if let Some(mut stderr) = child.stderr.take() {
            let tail = stderr_tail.clone();
            tokio::spawn(async move {
                let mut chunk = [0u8; 4096];
                while let Ok(n) = stderr.read(&mut chunk).await {
                    if n == 0 {
                        break;
                    }
                    let mut tail = tail.lock().unwrap();
                    tail.push_str(&String::from_utf8_lossy(&chunk[..n]));
                    let excess = tail.len().saturating_sub(2048);
                    if excess > 0 {
                        let cut = (excess..=tail.len())
                            .find(|i| tail.is_char_boundary(*i))
                            .unwrap_or(0);
                        tail.drain(..cut);
                    }
                }
            });
        }
        let (outgoing, mut queue) = mpsc::unbounded_channel::<Vec<u8>>();
        let shared = Arc::new(Shared {
            pending: Mutex::default(),
            pages: Mutex::default(),
            disconnected: Mutex::default(),
            next_id: AtomicU64::new(0),
            outgoing,
        });
        let mut writer = tokio::fs::File::from_std(std::fs::File::from(commands));
        tokio::spawn(async move {
            while let Some(bytes) = queue.recv().await {
                if writer.write_all(&bytes).await.is_err() {
                    break;
                }
            }
        });
        let mut reader = tokio::fs::File::from_std(std::fs::File::from(replies));
        let reading = shared.clone();
        tokio::spawn(async move {
            let mut partial = Vec::new();
            let mut chunk = vec![0u8; 1 << 16];
            loop {
                match reader.read(&mut chunk).await {
                    Ok(0) | Err(_) => break,
                    Ok(n) => {
                        for byte in &chunk[..n] {
                            if *byte == 0 {
                                reading.receive(&partial);
                                partial.clear();
                            } else {
                                partial.push(*byte);
                            }
                        }
                    }
                }
            }
            reading.disconnect(BrowserError::new("the browser exited unexpectedly"));
        });
        let mut browser = Browser {
            shared,
            child,
            proxy,
            context: String::new(),
            _profile: profile,
        };
        // The default context can stall Fetch interception during navigation.
        let created = browser
            .shared
            .send("Target.createBrowserContext", json!({}), None)
            .await;
        let created = match created {
            Ok(created) => created,
            Err(mut error) => {
                if let Ok(Ok(Some(status))) =
                    tokio::time::timeout(Duration::from_secs(1), async { browser.child.try_wait() })
                        .await
                {
                    error.exit_code = status.code();
                    #[cfg(unix)]
                    {
                        use std::os::unix::process::ExitStatusExt;
                        error.signal = status.signal().map(signal_name);
                    }
                }
                return Err(error);
            }
        };
        browser.context = created["browserContextId"]
            .as_str()
            .ok_or_else(|| {
                BrowserError::new("Target.createBrowserContext returned an unexpected result")
            })?
            .to_owned();
        Ok(browser)
    }

    #[cfg(not(unix))]
    async fn launch(_: &std::path::Path, _: bool) -> Result<Browser, BrowserError> {
        Err(BrowserError::new("the browser could not be started"))
    }

    async fn open_page(&self, width: u64, appearance: &str) -> Result<Page<'_>, BrowserError> {
        let shared = &self.shared;
        let target = shared
            .send(
                "Target.createTarget",
                json!({"url": "about:blank", "browserContextId": self.context}),
                None,
            )
            .await?;
        let target = target["targetId"]
            .as_str()
            .ok_or_else(|| BrowserError::new("Target.createTarget returned an unexpected result"))?
            .to_owned();
        let attached = shared
            .send(
                "Target.attachToTarget",
                json!({"targetId": target, "flatten": true}),
                None,
            )
            .await?;
        let session = attached["sessionId"]
            .as_str()
            .ok_or_else(|| {
                BrowserError::new("Target.attachToTarget returned an unexpected result")
            })?
            .to_owned();
        shared.pages.lock().unwrap().insert(
            session.clone(),
            PageState {
                main_frame: target.clone(),
                body: None,
                loaded: None,
                console: Vec::new(),
                omitted: 0,
            },
        );
        let s = Some(session.as_str());
        shared.send("Page.enable", json!({}), s).await?;
        shared.send("Runtime.enable", json!({}), s).await?;
        shared.send("Log.enable", json!({}), s).await?;
        shared
            .send(
                "Emulation.setEmulatedMedia",
                json!({"features": [{"name": "prefers-color-scheme", "value": appearance}]}),
                s,
            )
            .await?;
        shared
            .send(
                "Page.addScriptToEvaluateOnNewDocument",
                json!({"source": "delete window.RTCPeerConnection; delete window.webkitRTCPeerConnection;", "runImmediately": true}),
                s,
            )
            .await?;
        shared
            .send(
                "Fetch.enable",
                json!({"patterns": [{"urlPattern": format!("{PAGE_ORIGIN}/*")}, {"urlPattern": "*", "resourceType": "Document"}]}),
                s,
            )
            .await?;
        shared
            .send(
                "Emulation.setDeviceMetricsOverride",
                json!({"width": width, "height": VIEWPORT_HEIGHT, "deviceScaleFactor": 1, "mobile": false}),
                s,
            )
            .await?;
        Ok(Page {
            browser: self,
            target,
            session,
            width,
        })
    }

    /// `fakeIpNote`.
    fn fake_ip_note(&self) -> Vec<ConsoleMessage> {
        self.proxy
            .fake_ip_hosts()
            .into_iter()
            .map(|(host, address)| ConsoleMessage {
                level: "warning",
                text: clip(&format!("DNS returned blocked 198.18.0.0/15 addresses, which may come from Clash or Mihomo fake-IP DNS. Inline the resource, add its host to fake-ip-filter, or switch DNS to redir-host. Affected host: {host} ({address})")),
            })
            .collect()
    }

    /// Asks the browser to quit, then lets the drop kill one that stays up.
    async fn close(mut self) {
        if self.shared.disconnected.lock().unwrap().is_none() {
            self.shared.post("Browser.close", json!({}), None);
            let _ = tokio::time::timeout(Duration::from_secs(2), self.child.wait()).await;
        }
        let _ = self.child.start_kill();
    }
}

fn signal_name(signal: i32) -> String {
    match signal {
        libc::SIGKILL => "SIGKILL",
        libc::SIGTERM => "SIGTERM",
        libc::SIGSEGV => "SIGSEGV",
        libc::SIGABRT => "SIGABRT",
        libc::SIGTRAP => "SIGTRAP",
        libc::SIGINT => "SIGINT",
        _ => return format!("signal {signal}"),
    }
    .to_owned()
}

struct Page<'a> {
    browser: &'a Browser,
    target: String,
    session: String,
    width: u64,
}

impl Page<'_> {
    /// Loads `body` (the page, base64) and measures its content height.
    async fn load(&self, body: Arc<String>, fragment: &str) -> Result<u64, BrowserError> {
        let shared = &self.browser.shared;
        let (loaded, wait) = oneshot::channel();
        if let Some(page) = shared.pages.lock().unwrap().get_mut(&self.session) {
            page.body = Some(body);
            page.loaded = Some(loaded);
        }
        let s = Some(self.session.as_str());
        let navigation = shared
            .send(
                "Page.navigate",
                json!({"url": format!("{PAGE_URL}{fragment}")}),
                s,
            )
            .await?;
        if navigation["errorText"]
            .as_str()
            .is_some_and(|t| !t.is_empty())
        {
            return Err(BrowserError::new("the page could not be opened"));
        }
        wait.await
            .unwrap_or_else(|_| Err(BrowserError::new("the browser exited unexpectedly")))?;
        shared
            .send(
                "Runtime.evaluate",
                json!({"expression": SETTLE_EXPRESSION, "awaitPromise": true}),
                s,
            )
            .await?;
        let measured = shared
            .send(
                "Runtime.evaluate",
                json!({"expression": MEASURE_EXPRESSION, "returnByValue": true}),
                s,
            )
            .await?;
        let value = measured["result"]["value"]
            .as_f64()
            .filter(|v| v.is_finite())
            .ok_or_else(|| BrowserError::new("Runtime.evaluate returned an unexpected result"))?;
        Ok(value.ceil().max(0.0) as u64)
    }

    async fn screenshot(&self, height: u64) -> Result<String, BrowserError> {
        let shot = self
            .browser
            .shared
            .send(
                "Page.captureScreenshot",
                json!({"format": "png", "captureBeyondViewport": true, "clip": {"x": 0, "y": 0, "width": self.width, "height": height, "scale": 1}}),
                Some(self.session.as_str()),
            )
            .await?;
        shot["data"].as_str().map(str::to_owned).ok_or_else(|| {
            BrowserError::new("Page.captureScreenshot returned an unexpected result")
        })
    }

    fn console(&self) -> Vec<ConsoleMessage> {
        let pages = self.browser.shared.pages.lock().unwrap();
        let Some(page) = pages.get(&self.session) else {
            return Vec::new();
        };
        let mut out = page.console.clone();
        if page.omitted > 0 {
            out.push(ConsoleMessage {
                level: "warning",
                text: format!("{} more console messages were omitted.", page.omitted),
            });
        }
        out
    }

    async fn close(self) {
        let _ = self
            .browser
            .shared
            .send("Target.closeTarget", json!({"targetId": self.target}), None)
            .await;
        self.browser
            .shared
            .pages
            .lock()
            .unwrap()
            .remove(&self.session);
    }
}

fn page_body(html: &str) -> Arc<String> {
    use base64::Engine;
    Arc::new(base64::engine::general_purpose::STANDARD.encode(html.as_bytes()))
}

/// What `captureHtmlScreenshot` gives.
pub(crate) struct Capture {
    pub png: String,
    pub content_height: u64,
    pub captured_height: u64,
    pub console: Vec<ConsoleMessage>,
}

/// `captureHtmlScreenshot`: the page at `width`, up to 4000px tall.
pub(crate) async fn capture(
    executable: &std::path::Path,
    no_sandbox: bool,
    html: &str,
    width: u64,
    fragment: &str,
    appearance: &str,
) -> Result<Capture, BrowserError> {
    let browser = Browser::launch(executable, no_sandbox).await?;
    let run = async {
        let page = browser.open_page(width, appearance).await?;
        let content_height = page.load(page_body(html), fragment).await?;
        let captured_height = content_height.clamp(1, MAX_CAPTURE_HEIGHT);
        let png = page.screenshot(captured_height).await?;
        let mut console = page.console();
        console.extend(browser.fake_ip_note());
        Ok(Capture {
            png,
            content_height,
            captured_height,
            console,
        })
    };
    let result = tokio::time::timeout(Duration::from_secs(CAPTURE_TIMEOUT_SECONDS), run)
        .await
        .unwrap_or_else(|_| {
            Err(BrowserError::new(format!(
                "the page did not finish loading within {CAPTURE_TIMEOUT_SECONDS} seconds"
            )))
        });
    browser.close().await;
    result
}

/// `measureHtmlHeights`: a fresh load per width, since pages may lay out
/// only at load.
pub(crate) async fn measure(
    executable: &std::path::Path,
    no_sandbox: bool,
    html: &str,
    widths: &[u64],
    fragment: &str,
) -> Result<Vec<(u64, u64)>, BrowserError> {
    use futures_util::stream::{self, StreamExt, TryStreamExt};
    let browser = Browser::launch(executable, no_sandbox).await?;
    let body = page_body(html);
    let result = stream::iter(widths.iter().copied())
        .map(|width| {
            let browser = &browser;
            let body = body.clone();
            async move {
                let page = browser.open_page(width, "dark").await?;
                let height = page.load(body, fragment).await;
                page.close().await;
                Ok::<_, BrowserError>((width, height?))
            }
        })
        .buffered(MEASURE_CONCURRENCY)
        .try_collect()
        .await;
    browser.close().await;
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn console_messages_read_as_the_reference_reads_them() {
        let logged = console_message(
            "Runtime.consoleAPICalled",
            &json!({"type": "warning", "args": [{"type": "string", "value": "a"}, {"type": "number", "value": 2, "description": "2"}, {"type": "undefined"}]}),
        )
        .unwrap();
        assert_eq!(
            (logged.level, logged.text.as_str()),
            ("warning", "a 2 undefined")
        );
        let thrown = console_message(
            "Runtime.exceptionThrown",
            &json!({"exceptionDetails": {"text": "Uncaught", "exception": {"type": "object", "description": "Error: boom"}}}),
        )
        .unwrap();
        assert_eq!(thrown.text, "Error: boom");
        assert!(console_message(
            "Log.entryAdded",
            &json!({"entry": {"level": "info", "text": "x"}})
        )
        .is_none());
        // The 500th unit starts an emoji, so the clip ends before it.
        let long = format!("a{}", "😀".repeat(300));
        let clipped = clip(&long);
        assert!(clipped.ends_with('…'));
        assert_eq!(clipped.encode_utf16().count(), 500);
        assert_eq!(clip(&"😀".repeat(300)).encode_utf16().count(), 501);
    }
}
