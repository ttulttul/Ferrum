//! Thread tools and streamed viewers share one Chromium target. Keep the
//! automation session attached so its media emulation survives individual calls.
use super::*;
use crate::local_services::PreviewService;
use futures_util::stream::SplitSink;
use std::sync::atomic::Ordering;
use tokio::{sync::mpsc, time::timeout};

const LIMIT: Duration = Duration::from_secs(15);
const CLOSED: &str = "This browser tab is closed. Call preview_open.";
const HUMAN: &str = "A human controls this browser tab. Wait until they release control.";
const DIALOG: &str =
    "A browser dialog is pending. Read preview_status and resolve it with preview_dialog.";

pub(super) struct Session {
    writer: SplitSink<Cdp, Wire>,
    replies: mpsc::Receiver<Value>,
    sequence: u64,
    scheme: String,
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
        };
        session.command("Page.enable", json!({})).await?;
        let mut initial = page.initial_url.lock().await;
        if page.activity.lock().unwrap().viewers == 0 {
            let zoom = snap["zoomFactor"]
                .as_f64()
                .filter(|zoom| *zoom > 0.0)
                .unwrap_or(1.0);
            let size = |key: &str, default: u64, max: u64| {
                (bounded(&snap["viewport"][key], default, max) as f64 / zoom)
                    .round()
                    .max(1.0) as u64
            };
            session
                .command(
                    "Emulation.setDeviceMetricsOverride",
                    json!({
                        "width":size("width",1280,7680), "height":size("height",800,4320),
                        "deviceScaleFactor":2.0 * zoom, "mobile":false,
                    }),
                )
                .await?;
        }
        session
            .appearance(snap["colorScheme"].as_str().unwrap_or("system"))
            .await?;
        if let Some(url) = initial.as_ref() {
            session.command("Page.navigate", json!({"url":url})).await?;
            *initial = None;
        }
        Ok(session)
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
    pub(in crate::local_services) async fn automate(
        &self,
        previews: &PreviewService,
        operation: &str,
        args: &Value,
    ) -> Result<Value, String> {
        let timed_out = || format!("Preview automation {operation} timed out after 15000ms.");
        let generation = {
            let control = self.control.lock().unwrap();
            if control.owner.is_some() {
                return Err(HUMAN.into());
            }
            control.generation
        };
        let deadline = Instant::now() + LIMIT;
        let mut guard = tokio::time::timeout_at(deadline, self.automation.lock())
            .await
            .map_err(|_| timed_out())?;
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
        if guard.is_none() {
            *guard = Some(
                tokio::time::timeout_at(deadline, Session::attach(self, &snap))
                    .await
                    .map_err(|_| timed_out())??,
            );
        }
        let session = guard.as_mut().unwrap();
        let work = async {
            match operation {
                "evaluate" => {
                    session
                        .appearance(snap["colorScheme"].as_str().unwrap_or("system"))
                        .await?;
                    session.evaluate(args).await
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
