//! Recording a browser tab, as the reference's server browser records one
//! (`startRecording`, `stopRecording`): the tab's screencast frames are drawn
//! onto a canvas in a scratch page of the same browser, whose `MediaRecorder`
//! encodes the video (`recordingEncoderScript`). The stopped recording lands
//! in the attachments directory as a pending attachment for the thread to
//! claim.
use super::*;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::{mpsc, oneshot};

/// `RECORDING_SCREENCAST`.
const QUALITY: u64 = 90;
/// `DEFAULT_BROWSER_RECORDING_FRAME_RATE`.
const FRAME_RATE: u64 = 30;
/// `PROVIDER_SEND_TURN_MAX_FILE_BYTES`.
pub(crate) const MAX_BYTES: u64 = 50 * 1024 * 1024;
const SETTLE_ANIMATIONS: &str = r#"(async () => {
          const paint = () => new Promise(resolve => requestAnimationFrame(() => requestAnimationFrame(resolve)));
          await paint();
          await Promise.allSettled(document.getAnimations()
            .filter(animation => animation.playState === "running" && animation.effect?.getComputedTiming().iterations !== Infinity)
            .map(animation => animation.finished));
          await paint();
        })()"#;

/// A DevTools connection to one target, shared by the tasks that use it.
pub(super) struct Client {
    writer: tokio::sync::Mutex<futures_util::stream::SplitSink<Cdp, Wire>>,
    pending: Mutex<HashMap<u64, oneshot::Sender<Value>>>,
    next: AtomicU64,
}

impl Client {
    async fn connect(
        endpoint: &str,
    ) -> Result<(Arc<Client>, mpsc::UnboundedReceiver<Value>), String> {
        let (socket, _) = connect_async(endpoint).await.map_err(|e| e.to_string())?;
        let (writer, mut reader) = socket.split();
        let client = Arc::new(Client {
            writer: tokio::sync::Mutex::new(writer),
            pending: Mutex::default(),
            next: AtomicU64::new(0),
        });
        let (events, received) = mpsc::unbounded_channel();
        let routing = Arc::downgrade(&client);
        tokio::spawn(async move {
            while let Some(Ok(message)) = reader.next().await {
                let Wire::Text(raw) = message else { continue };
                let Ok(message) = serde_json::from_str::<Value>(&raw) else {
                    continue;
                };
                match message["id"].as_u64() {
                    Some(id) => {
                        let Some(client) = routing.upgrade() else {
                            break;
                        };
                        let waiter = client.pending.lock().unwrap().remove(&id);
                        if let Some(waiter) = waiter {
                            let _ = waiter.send(message);
                        }
                    }
                    None => {
                        let _ = events.send(message);
                    }
                }
            }
            if let Some(client) = routing.upgrade() {
                client.pending.lock().unwrap().clear();
            }
        });
        Ok((client, received))
    }

    async fn command(&self, method: &str, params: Value) -> Result<Value, String> {
        let id = self.next.fetch_add(1, Ordering::Relaxed) + 1;
        let (reply, answer) = oneshot::channel();
        self.pending.lock().unwrap().insert(id, reply);
        self.writer
            .lock()
            .await
            .send(Wire::Text(
                json!({"id": id, "method": method, "params": params}).to_string(),
            ))
            .await
            .map_err(|_| "The browser connection closed.".to_owned())?;
        let reply = answer
            .await
            .map_err(|_| "The browser connection closed.".to_owned())?;
        if let Some(message) = reply["error"]["message"].as_str() {
            return Err(message.to_owned());
        }
        Ok(reply["result"].clone())
    }

    /// `Runtime.evaluate`, awaited, by value; a thrown error is its message.
    async fn evaluate(&self, expression: &str) -> Result<Value, String> {
        let reply = self
            .command(
                "Runtime.evaluate",
                json!({"expression": expression, "awaitPromise": true, "returnByValue": true}),
            )
            .await?;
        if let Some(error) = reply.get("exceptionDetails") {
            return Err(error["exception"]["description"]
                .as_str()
                .or(error["text"].as_str())
                .unwrap_or("Evaluation failed")
                .to_owned());
        }
        Ok(reply["result"]["value"].clone())
    }

    async fn close(&self) {
        let _ = self.writer.lock().await.close().await;
    }
}

/// Hands `frame` (base64 JPEG) to the encoder; `false` once the recording is
/// too large to go on.
async fn encode(encoder: &Client, frame: &str, css_width: u64) -> Result<bool, String> {
    let accepted = encoder
        .evaluate(&format!(
            "window.__t3Recorder.frame({}, {css_width})",
            serde_json::to_string(frame).unwrap_or_default()
        ))
        .await?;
    Ok(accepted != false)
}

/// A recording in progress.
pub(super) struct Recording {
    pub started_at: String,
    encoder: Arc<Client>,
    encoder_target: String,
    origin: String,
    screencast: Arc<Client>,
    frames: Option<watch::Sender<Option<(String, u64)>>>,
    forwarder: tokio::task::JoinHandle<()>,
    deliverer: tokio::task::JoinHandle<()>,
}

impl Recording {
    /// Draws the agent's cursor at a page point, with a click's ring
    /// (`__t3Recorder.cursor`); a recording that fails to take it goes on.
    pub(super) async fn cursor(&self, x: f64, y: f64, click: bool) {
        let _ = self
            .encoder
            .evaluate(&format!("window.__t3Recorder.cursor({x}, {y}, {click})"))
            .await;
    }
}

impl Drop for Recording {
    fn drop(&mut self) {
        self.forwarder.abort();
        self.deliverer.abort();
        let close = format!("{}/json/close/{}", self.origin, self.encoder_target);
        tokio::spawn(async move {
            let _ = reqwest::Client::new().get(close).send().await;
        });
    }
}

fn origin(endpoint: &str) -> Option<String> {
    let url = url::Url::parse(endpoint).ok()?;
    Some(format!("http://{}:{}", url.host_str()?, url.port()?))
}

fn css_width(page: &Page) -> u64 {
    page.layout.borrow().width
}

/// `recordingEncoderScript`: the reference's encoder, as it runs in the scratch page.
fn encoder_script() -> String {
    include_str!("recording_encoder.js")
        .replace("${frameRate}", &FRAME_RATE.to_string())
        .replace(
            "${PROVIDER_SEND_TURN_MAX_FILE_BYTES}",
            &MAX_BYTES.to_string(),
        )
}

impl Recording {
    /// `startRecording`: the encoder seeded with the tab's current frame, then
    /// every screencast frame. `first_frame` is the tab captured now.
    pub(super) async fn start(page: &Page, first_frame: String) -> Result<Recording, String> {
        let origin = origin(&page.endpoint).ok_or("The browser is unavailable.")?;
        let mut address =
            url::Url::parse(&format!("{origin}/json/new")).map_err(|e| e.to_string())?;
        address.set_query(Some("about:blank"));
        let target = reqwest::Client::new()
            .put(address)
            .send()
            .await
            .and_then(reqwest::Response::error_for_status)
            .map_err(|e| e.to_string())?
            .json::<Value>()
            .await
            .map_err(|e| e.to_string())?;
        let encoder_target = target["id"]
            .as_str()
            .ok_or("The browser is unavailable.")?
            .to_owned();
        let close_target = || {
            let close = format!("{origin}/json/close/{encoder_target}");
            async move {
                let _ = reqwest::Client::new().get(close).send().await;
            }
        };
        let started = async {
            let endpoint = target["webSocketDebuggerUrl"]
                .as_str()
                .ok_or("The browser is unavailable.")?;
            let (encoder, _) = Client::connect(endpoint).await?;
            encoder.evaluate(&encoder_script()).await?;
            encode(&encoder, &first_frame, css_width(page)).await?;
            let (screencast, mut events) = Client::connect(&page.endpoint).await?;
            let (frames, mut latest) = watch::channel::<Option<(String, u64)>>(None);
            // Chrome withholds frames while acks are outstanding, so ack on
            // arrival and let a slow encoder skip to the newest frame.
            let acking = screencast.clone();
            let layout = page.layout.clone();
            let offered = frames.clone();
            let forwarder = tokio::spawn(async move {
                while let Some(event) = events.recv().await {
                    if event["method"] != "Page.screencastFrame" {
                        continue;
                    }
                    let params = &event["params"];
                    let acking = acking.clone();
                    let session = params["sessionId"].clone();
                    tokio::spawn(async move {
                        let _ = acking
                            .command("Page.screencastFrameAck", json!({"sessionId": session}))
                            .await;
                    });
                    let width = layout.borrow().width;
                    let width = if width == 0 {
                        params["metadata"]["deviceWidth"].as_f64().unwrap_or(0.0) as u64
                    } else {
                        width
                    };
                    if let Some(data) = params["data"].as_str() {
                        offered.send_replace(Some((data.to_owned(), width)));
                    }
                }
            });
            let drawing = encoder.clone();
            let stopping = screencast.clone();
            let deliverer = tokio::spawn(async move {
                while latest.changed().await.is_ok() {
                    let Some((data, width)) = latest.borrow_and_update().clone() else {
                        continue;
                    };
                    if !encode(&drawing, &data, width).await.unwrap_or(true) {
                        let _ = stopping.command("Page.stopScreencast", json!({})).await;
                    }
                }
            });
            screencast
                .command(
                    "Page.startScreencast",
                    json!({"format": "jpeg", "quality": QUALITY, "everyNthFrame": 1}),
                )
                .await?;
            Ok::<_, String>(Recording {
                started_at: crate::auth_service::iso(crate::auth_service::now()),
                encoder,
                encoder_target: encoder_target.clone(),
                origin: origin.clone(),
                screencast,
                frames: Some(frames),
                forwarder,
                deliverer,
            })
        };
        match started.await {
            Ok(recording) => Ok(recording),
            Err(error) => {
                close_target().await;
                Err(error)
            }
        }
    }

    /// `stopRecording`, up to the video: the screencast stopped and drained,
    /// the tab's final frame added (`final_frame`, when the page can paint),
    /// and the encoder's output. Returns its MIME type and bytes.
    pub(super) async fn stop(
        mut self,
        page: &Page,
        final_frame: Option<String>,
    ) -> Result<(String, Vec<u8>), RecordingError> {
        let _ = self
            .screencast
            .command("Page.stopScreencast", json!({}))
            .await;
        self.forwarder.abort();
        // The deliverer ends once its last frame is drawn.
        drop(self.frames.take());
        let deliverer = std::mem::replace(&mut self.deliverer, tokio::spawn(async {}));
        let _ = deliverer.await;
        self.screencast.close().await;
        if let Some(frame) = final_frame {
            let _ = encode(&self.encoder, &frame, css_width(page)).await;
        }
        let stopped = self
            .encoder
            .evaluate("window.__t3Recorder.stop()")
            .await
            .map_err(RecordingError::Failed)?;
        let bytes = stopped["bytes"].as_u64().unwrap_or(0);
        let count = stopped["count"].as_u64().unwrap_or(0);
        let mime = stopped["mimeType"].as_str().unwrap_or_default().to_owned();
        if bytes > MAX_BYTES {
            return Err(RecordingError::TooLarge);
        }
        if mime.is_empty() || bytes == 0 {
            return Err(RecordingError::Failed(
                "The recording captured no frames.".into(),
            ));
        }
        let mut data = Vec::with_capacity(bytes as usize);
        for index in 0..count {
            let chunk = self
                .encoder
                .evaluate(&format!("window.__t3Recorder.chunk({index})"))
                .await
                .map_err(RecordingError::Failed)?;
            data.extend(
                STANDARD
                    .decode(chunk.as_str().unwrap_or_default())
                    .map_err(|_| {
                        RecordingError::Failed("The recording transfer is incomplete.".into())
                    })?,
            );
        }
        if data.len() as u64 != bytes {
            return Err(RecordingError::Failed(
                "The recording transfer is incomplete.".into(),
            ));
        }
        self.encoder.close().await;
        Ok((mime, data))
    }
}

/// Why a stop gave no recording.
pub(super) enum RecordingError {
    TooLarge,
    Failed(String),
}

/// What the tab looks like after its finite animations end, for the
/// recording's last frame (`captureFinalRecordingFrame`).
pub(super) const FINAL_FRAME_SETTLE: &str = SETTLE_ANIMATIONS;
