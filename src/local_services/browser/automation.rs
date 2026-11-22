//! Thread tools and streamed viewers share one Chromium target. Keep the
//! automation session attached so its media emulation survives individual calls.
use super::*;
use crate::local_services::PreviewService;
use futures_util::stream::SplitSink;
use std::sync::atomic::Ordering;
use tokio::{sync::mpsc, time::timeout};

const CLOSED: &str = "This browser tab is closed. Call preview_open.";
const HUMAN: &str = "A human controls this browser tab. Wait until they release control.";
const DIALOG: &str =
    "A browser dialog is pending. Read preview_status and resolve it with preview_dialog.";

pub(super) struct Session {
    writer: SplitSink<Cdp, Wire>,
    replies: mpsc::Receiver<Value>,
    sequence: u64,
    scheme: String,
    viewport: Size,
    setting: Value,
    zoom: f64,
}

impl Session {
    async fn attach(page: &Page, snap: &Value) -> Result<Self, String> {
        let (socket, _) = connect_async(&page.endpoint)
            .await
            .map_err(|e| e.to_string())?;
        let (writer, mut events) = socket.split();
        let (tx, replies) = mpsc::channel(4);
        let dialog = page.dialog.clone();
        tokio::spawn(async move {
            loop {
                let message = tokio::select! {
                    _ = tx.closed() => break,
                    message = events.next() => message,
                };
                let Some(Ok(message)) = message else { break };
                let Wire::Text(raw) = message else { continue };
                let Ok(reply) = serde_json::from_str::<Value>(&raw) else {
                    continue;
                };
                if reply["id"].is_number() {
                    if tx.send(reply).await.is_err() {
                        break;
                    }
                    continue;
                }
                match reply["method"].as_str() {
                    Some("Page.javascriptDialogOpening") => dialog.store(true, Ordering::Release),
                    Some("Page.javascriptDialogClosed") => dialog.store(false, Ordering::Release),
                    _ => {}
                }
            }
        });
        let mut session = Self {
            writer,
            replies,
            sequence: 0,
            scheme: "system".into(),
            viewport: Size::DEFAULT,
            setting: Value::Null,
            zoom: 1.0,
        };
        session.command("Page.enable", json!({})).await?;
        session.apply_setting(page, snap).await?;
        session
            .appearance(snap["colorScheme"].as_str().unwrap_or("system"))
            .await?;
        Ok(session)
    }

    async fn connected<'a>(
        slot: &'a mut Option<Self>,
        page: &Page,
        snap: &Value,
    ) -> Result<&'a mut Self, String> {
        if slot.is_none() {
            *slot = Some(Self::attach(page, snap).await?);
        }
        Ok(slot.as_mut().expect("attached Chromium session"))
    }

    async fn apply_size(&mut self, page: &Page, size: Size, zoom: f64) -> Result<(), String> {
        let layout = size.layout(zoom);
        let mut metrics = json!({
            "width":layout.width, "height":layout.height, "deviceScaleFactor":2.0 * zoom, "mobile":false
        });
        // Playwright's setViewportSize resets the emulated screen to the viewport.
        // The reference's subsequent CDP zoom override leaves the screen unspecified.
        if zoom == 1.0 {
            metrics["screenWidth"] = json!(size.width);
            metrics["screenHeight"] = json!(size.height);
        }
        self.command("Emulation.setDeviceMetricsOverride", metrics)
            .await?;
        self.viewport = size;
        self.zoom = zoom;
        page.layout.send_replace(layout);
        Ok(())
    }

    async fn apply_setting(&mut self, page: &Page, snap: &Value) -> Result<(), String> {
        let size = Size::fixed(&snap["viewport"])
            .or(page.control.lock().unwrap().requested_size)
            .unwrap_or(Size::DEFAULT);
        let zoom = Self::zoom(snap);
        self.apply_size(page, size, zoom).await?;
        self.setting = snap["viewport"].clone();
        Ok(())
    }

    async fn sync_viewport(&mut self, page: &Page, snap: &Value) -> Result<(), String> {
        if self.setting != snap["viewport"] {
            return self.apply_setting(page, snap).await;
        }
        let zoom = Self::zoom(snap);
        if self.zoom != zoom {
            self.apply_size(page, self.viewport, zoom).await?;
        }
        Ok(())
    }

    fn zoom(snap: &Value) -> f64 {
        snap["zoomFactor"]
            .as_f64()
            .filter(|z| *z > 0.0)
            .unwrap_or(1.0)
    }

    async fn resize(
        &mut self,
        page: &Page,
        previews: &PreviewService,
        setting: Value,
    ) -> Result<Value, String> {
        let change = json!({"threadId":page.thread, "tabId":page.tab, "viewport":setting});
        let snapshot = previews
            .call("resize", &change)
            .expect("resize")
            .map_err(|_| CLOSED.to_owned())?;
        self.apply_setting(page, &snapshot).await?;
        Ok(json!({"tabId":page.tab,"setting":setting,"viewport":self.viewport.value()}))
    }

    async fn send(&mut self, method: &str, params: Value) -> Result<(), String> {
        self.sequence += 1;
        self.writer
            .send(Wire::Text(
                json!({"id":self.sequence,"method":method,"params":params}).to_string(),
            ))
            .await
            .map_err(|_| CLOSED.to_owned())
    }

    async fn command(&mut self, method: &str, params: Value) -> Result<Value, String> {
        self.send(method, params).await?;
        while let Some(reply) = self.replies.recv().await {
            if reply["id"] == self.sequence {
                if let Some(message) = reply["error"]["message"].as_str() {
                    return Err(message.to_owned());
                }
                return Ok(reply["result"].clone());
            }
        }
        Err(CLOSED.to_owned())
    }

    async fn appearance(&mut self, scheme: &str) -> Result<(), String> {
        if scheme != self.scheme {
            let features = if scheme == "system" {
                json!([])
            } else {
                json!([{"name":"prefers-color-scheme","value":scheme}])
            };
            self.command("Emulation.setEmulatedMedia", json!({"features": features}))
                .await?;
            self.scheme = scheme.to_owned();
        }
        Ok(())
    }

    async fn evaluate(&mut self, args: &Value) -> Result<Value, String> {
        let expression = args["expression"].as_str().unwrap_or_default();
        let mut params = json!({"expression":expression, "awaitPromise":args["awaitPromise"].as_bool().unwrap_or(true),"returnByValue":true});
        let mut response = self.command("Runtime.evaluate", params.clone()).await?;
        let failure = &response["exceptionDetails"];
        if expression.contains("await")
            && failure["exception"]["className"] == "SyntaxError"
            && failure.get("scriptId").is_some()
            && failure.get("stackTrace").is_none()
        {
            params["replMode"] = json!(true);
            response = self.command("Runtime.evaluate", params).await?;
        }
        if let Some(error) = response.get("exceptionDetails") {
            return Err(error["exception"]["description"]
                .as_str()
                .or(error["text"].as_str())
                .unwrap_or("Evaluation failed")
                .into());
        }
        let remote = &response["result"];
        let value = remote.get("value").unwrap_or(&remote["description"]);
        Ok(json!({"value":value}))
    }
}

impl Page {
    pub(super) async fn viewer_layout(
        &self,
        previews: &PreviewService,
    ) -> Result<watch::Receiver<Size>, String> {
        let mut guard = self.automation.lock().await;
        let snap = previews.tab(&self.thread, &self.tab).ok_or(CLOSED)?;
        Session::connected(&mut guard, self, &snap).await?;
        Ok(self.layout.subscribe())
    }

    pub(super) async fn viewer_resize(
        &self,
        previews: &PreviewService,
        viewer: Uuid,
        message: &Value,
    ) -> Result<(), String> {
        let mut guard = self.automation.lock().await;
        let Some(session) = guard.as_mut() else {
            return Err(CLOSED.into());
        };
        if self.control.lock().unwrap().owner != Some(viewer) {
            return Ok(());
        }
        if message["type"] == "viewport" {
            if let Some(setting) = viewport::viewer_setting(&message["setting"]) {
                session.resize(self, previews, setting).await?;
            }
        } else if let Some(size) = Size::requested(message) {
            self.control.lock().unwrap().requested_size = Some(size);
            if session.setting["_tag"] == "fill" && session.viewport != size {
                session.apply_size(self, size, session.zoom).await?;
            }
        }
        Ok(())
    }

    // Manager RPCs also change the size of an already running tab. Read the
    // current snapshot inside its command queue so overlapping updates converge.
    pub(in crate::local_services) async fn sync_viewport(&self, previews: &PreviewService) {
        let mut guard = self.automation.lock().await;
        if let (Some(session), Some(snap)) = (guard.as_mut(), previews.tab(&self.thread, &self.tab))
        {
            let _ = session.sync_viewport(self, &snap).await;
        }
    }

    pub(in crate::local_services) async fn automate(
        &self,
        previews: &PreviewService,
        operation: &str,
        args: &Value,
        deadline: Instant,
        timeout_ms: u64,
    ) -> Result<Value, String> {
        let timed_out =
            || format!("Preview automation {operation} timed out after {timeout_ms}ms.");
        let generation = {
            let control = self.control.lock().unwrap();
            if control.owner.is_some() {
                return Err(HUMAN.into());
            }
            control.generation
        };
        let mut guard = tokio::time::timeout_at(deadline, self.automation.lock())
            .await
            .map_err(|_| timed_out())?;
        if Instant::now() >= deadline {
            return Err(timed_out());
        }
        if *self.closed.borrow() {
            return Err(CLOSED.into());
        }
        if self.control.lock().unwrap().generation != generation {
            return Err(
                "Browser control changed. Take a fresh snapshot before trying again.".into(),
            );
        }
        if self.dialog.load(Ordering::Acquire) {
            return Err(DIALOG.into());
        }
        let snap = previews.tab(&self.thread, &self.tab).ok_or(CLOSED)?;
        let session =
            tokio::time::timeout_at(deadline, Session::connected(&mut guard, self, &snap))
                .await
                .map_err(|_| timed_out())??;
        let work = async {
            session.sync_viewport(self, &snap).await?;
            {
                let mut initial = self.initial_url.lock().await;
                if let Some(url) = initial.as_ref() {
                    session.command("Page.navigate", json!({"url":url})).await?;
                    *initial = None;
                }
            }
            match operation {
                "evaluate" => {
                    session
                        .appearance(snap["colorScheme"].as_str().unwrap_or("system"))
                        .await?;
                    session.evaluate(args).await
                }
                "resize" => {
                    let setting =
                        viewport::resolve(args).ok_or("Invalid preview viewport setting")?;
                    session.resize(self, previews, setting).await
                }
                "setColorScheme" => {
                    let scheme = args["colorScheme"].as_str().ok_or("Missing color scheme")?;
                    let adjust =
                        json!({"threadId":self.thread,"tabId":self.tab,"colorScheme":scheme});
                    previews
                        .call("adjust", &adjust)
                        .expect("adjust")
                        .map_err(|_| CLOSED.to_owned())?;
                    session.appearance(scheme).await?;
                    Ok(json!({"tabId":self.tab,"colorScheme":scheme}))
                }
                _ => Err("Unknown preview operation".into()),
            }
        };
        self.activity.lock().unwrap().used = super::super::timestamp();
        let mut result = match tokio::time::timeout_at(deadline, work).await {
            Ok(Ok(value)) => value,
            Ok(Err(_)) if *self.closed.borrow() => return Err(CLOSED.into()),
            Ok(Err(error)) => {
                let reason: Vec<_> = error.encode_utf16().take(500).collect();
                return Err(format!(
                    "Preview automation {operation} failed: {}",
                    String::from_utf16_lossy(&reason)
                ));
            }
            Err(_) => {
                if operation == "evaluate" {
                    let _ = timeout(
                        Duration::from_millis(500),
                        session.send("Runtime.terminateExecution", json!({})),
                    )
                    .await;
                }
                return Err(timed_out());
            }
        };
        if operation == "evaluate" && crate::wire_json::compact(&result["value"]).len() > 64_000 {
            return Err(
                "Preview automation evaluate produced a result larger than 64000 bytes.".into(),
            );
        }
        if let Ok(Ok(tree)) = timeout(
            Duration::from_millis(500),
            session.command("Page.getFrameTree", json!({})),
        )
        .await
        {
            let frame = &tree["frameTree"]["frame"];
            let url = match frame["unreachableUrl"].as_str() {
                Some(url) => url.to_owned(),
                None => format!(
                    "{}{}",
                    frame["url"].as_str().unwrap_or_default(),
                    frame["urlFragment"].as_str().unwrap_or_default()
                ),
            };
            if (url.starts_with("http://") || url.starts_with("https://"))
                && url.encode_utf16().count() <= 4096
            {
                result["toolIcon"] = json!({"_tag":"website","pageUrl":url});
            }
        }
        Ok(result)
    }
}

impl Browsers {
    pub(in crate::local_services) fn priority(
        &self,
        thread: &str,
        id: &str,
        snap: &Value,
    ) -> (bool, String) {
        if let Some(page) = self.page(thread, id) {
            let activity = page.activity.lock().unwrap();
            (activity.viewers > 0, activity.used.clone())
        } else {
            (
                false,
                snap["updatedAt"].as_str().unwrap_or_default().to_owned(),
            )
        }
    }
}
