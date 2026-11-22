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

    async fn raw_evaluate(
        &mut self,
        expression: &str,
        await_promise: bool,
    ) -> Result<Value, String> {
        let response = self
            .command(
                "Runtime.evaluate",
                json!({"expression": expression, "awaitPromise": await_promise, "returnByValue": true}),
            )
            .await?;
        if let Some(error) = response.get("exceptionDetails") {
            return Err(error["exception"]["description"]
                .as_str()
                .or(error["text"].as_str())
                .unwrap_or("Evaluation failed")
                .to_owned());
        }
        Ok(response["result"]
            .get("value")
            .cloned()
            .unwrap_or(Value::Null))
    }

    async fn page_state(&mut self) -> Result<Value, String> {
        self.raw_evaluate(
            "({url: location.href, title: document.title, ready: document.readyState, loading: document.readyState !== 'complete'})",
            false,
        )
        .await
    }

    async fn navigate(
        &mut self,
        url: &str,
        readiness: &str,
        deadline: Instant,
    ) -> Result<Value, String> {
        self.command("Page.navigate", json!({"url": url})).await?;
        if readiness != "none" {
            loop {
                if Instant::now() >= deadline {
                    return Err("navigation timed out".into());
                }
                let state = self.page_state().await?;
                let ready = state["ready"] == "complete"
                    || (readiness == "domContentLoaded"
                        && matches!(state["ready"].as_str(), Some("interactive" | "complete")));
                if ready {
                    return Ok(state);
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        }
        self.page_state().await
    }

    async fn snapshot(&mut self, args: &Value, page: &Page) -> Result<Value, String> {
        // Keep refs in the DOM so a subsequent preview_act can address the exact element
        // returned by this snapshot. This is intentionally page-local and is invalidated by
        // every new snapshot, just as the reference's accessibility refs are.
        let page_info = self
            .raw_evaluate(
                r#"(() => {
                  const old = document.querySelectorAll('[data-t3-preview-ref]');
                  old.forEach((e) => e.removeAttribute('data-t3-preview-ref'));
                  const all = [...document.querySelectorAll('button,a,input,textarea,select,[role],[tabindex]')];
                  const interactive = all.filter((e) => {
                    const r = e.getBoundingClientRect();
                    return r.width > 0 && r.height > 0 && getComputedStyle(e).visibility !== 'hidden';
                  });
                  const lines = [];
                  interactive.forEach((e, i) => {
                    const ref = `e${i + 1}`;
                    e.setAttribute('data-t3-preview-ref', ref);
                    const role = e.getAttribute('role') || (e.tagName || '').toLowerCase();
                    const name = (e.getAttribute('aria-label') || e.innerText || e.value || '').trim().replace(/\s+/g, ' ').slice(0, 200);
                    lines.push(`- ${role}${name ? ` "${name.replaceAll('"', '\\"')}"` : ''} [ref=t3-preview-${ref}]`);
                  });
                  return {url: location.href, title: document.title, loading: document.readyState !== 'complete', visibleText: document.body?.innerText || '', accessibilityTree: lines.join('\\n')};
                })()"#,
                false,
            )
            .await?;
        let shot = self
            .command("Page.captureScreenshot", json!({"format":"png"}))
            .await?;
        let bytes = STANDARD
            .decode(shot["data"].as_str().unwrap_or_default())
            .map_err(|e| e.to_string())?;
        let width = u32::from_be_bytes(bytes.get(16..20).ok_or("invalid PNG")?.try_into().unwrap());
        let height =
            u32::from_be_bytes(bytes.get(20..24).ok_or("invalid PNG")?.try_into().unwrap());
        let include = args["includeImage"] == true || args["save"] == true;
        let reported_width = if include { width } else { width.min(1280) };
        let reported_height = if include {
            height
        } else if width > 1280 {
            ((height as f64) * (1280.0 / width as f64)).round() as u32
        } else {
            height
        };
        let screenshot = if include {
            if args["save"] == true {
                let path = page
                    .scratch
                    .path()
                    .join(format!("preview-{}.png", uuid::Uuid::new_v4()));
                std::fs::write(&path, &bytes).map_err(|e| e.to_string())?;
                json!({"mimeType":"image/png","data":shot["data"],"width":width,"height":height,"screenshotPath":path})
            } else {
                json!({"mimeType":"image/png","data":shot["data"],"width":width,"height":height})
            }
        } else {
            json!({"width":reported_width,"height":reported_height})
        };
        Ok(json!({
            "url":page_info["url"], "title":page_info["title"], "loading":page_info["loading"],
            "visibleText":page_info["visibleText"].as_str().unwrap_or_default().chars().take(20_000).collect::<String>(),
            "accessibilityTree":page_info["accessibilityTree"], "consoleEntries":[], "networkEntries":[], "actionTimeline":[], "screenshot":screenshot
        }))
    }

    async fn target_point(
        &mut self,
        args: &Value,
        key: &str,
    ) -> Result<Option<(f64, f64)>, String> {
        let locator = args[key].as_str();
        let selector = args["selector"].as_str();
        if locator.is_none() && selector.is_none() {
            return Ok(args["x"].as_f64().zip(args["y"].as_f64()));
        }
        let value = locator.or(selector).unwrap_or_default();
        let literal = serde_json::to_string(value).unwrap();
        let expression = format!(
            r#"(() => {{
              const q = {literal};
              let e;
              if (q.startsWith('aria-ref=')) e = document.querySelector(`[data-t3-preview-ref="${{q.slice(9).replace('t3-preview-','')}}"]`);
              else if (q.startsWith('text=')) e = [...document.querySelectorAll('*')].find((x) => (x.innerText || '').trim().includes(q.slice(5)));
              else e = document.querySelector(q);
              if (!e) return null;
              const r = e.getBoundingClientRect();
              return {{x:r.left+r.width/2,y:r.top+r.height/2}};
            }})()"#
        );
        let point = self.raw_evaluate(&expression, false).await?;
        Ok(point["x"].as_f64().zip(point["y"].as_f64()))
    }

    async fn act(&mut self, args: &Value, deadline: Instant) -> Result<Value, String> {
        let action = args["action"].as_str().ok_or("Missing action")?;
        let point = self.target_point(args, "locator").await?;
        if action == "click" {
            let (x, y) = point
                .or(args["x"].as_f64().zip(args["y"].as_f64()))
                .ok_or("A locator or x and y are required")?;
            let button = args["button"].as_str().unwrap_or("left");
            let count = args["clickCount"].as_u64().unwrap_or(1);
            for ty in ["mousePressed", "mouseReleased"] {
                self.command(
                    "Input.dispatchMouseEvent",
                    json!({"type":ty,"x":x,"y":y,"button":button,"clickCount":count}),
                )
                .await?;
            }
            return Ok(json!({}));
        }
        if action == "hover" {
            let (x, y) = point.ok_or("A locator or x and y are required")?;
            self.command(
                "Input.dispatchMouseEvent",
                json!({"type":"mouseMoved","x":x,"y":y}),
            )
            .await?;
            return Ok(json!({}));
        }
        if action == "type" {
            let text = args["text"].as_str().ok_or("Missing text")?;
            if args["clear"] == true {
                let _ = self.raw_evaluate("document.activeElement?.value = ''; document.activeElement?.dispatchEvent(new Event('input', {bubbles:true}))", false).await?;
            }
            if let Some(value) = args["locator"].as_str().or(args["selector"].as_str()) {
                let literal = serde_json::to_string(value).unwrap();
                let _ = self.raw_evaluate(&format!("(() => {{ const q={literal}; const e=q.startsWith('aria-ref=')?document.querySelector(`[data-t3-preview-ref=\\\"${{q.slice(9).replace('t3-preview-','')}}\\\"]`):document.querySelector(q); e?.focus(); return true; }})()"), false).await?;
            }
            self.command("Input.insertText", json!({"text":text}))
                .await?;
            return Ok(json!({}));
        }
        if action == "select" {
            let values = args["values"].as_array().ok_or("Missing values")?;
            let literal = serde_json::to_string(values).unwrap();
            let locator = serde_json::to_string(
                args["locator"]
                    .as_str()
                    .or(args["selector"].as_str())
                    .unwrap_or("select"),
            )
            .unwrap();
            let result = self.raw_evaluate(&format!(r#"(() => {{ const q={locator}; const e=q.startsWith('aria-ref=')?document.querySelector(`[data-t3-preview-ref="${{q.slice(9).replace('t3-preview-','')}}"]`):document.querySelector(q); if (!e || e.tagName !== 'SELECT') throw Error('This element is not a <select>. Click a custom dropdown, then click the option.'); const wanted={literal}; const selected=[...e.options].filter(o=>wanted.includes(o.value)||wanted.includes(o.label.trim())); selected.forEach(o=>o.selected=true); e.dispatchEvent(new Event('input',{{bubbles:true}})); e.dispatchEvent(new Event('change',{{bubbles:true}})); return selected.map(o=>o.value); }})()"#), false).await?;
            return Ok(json!({"selected":result}));
        }
        if action == "scroll" {
            let dx = args["deltaX"].as_f64().unwrap_or(0.0);
            let dy = args["deltaY"].as_f64().unwrap_or(0.0);
            let target = args["locator"]
                .as_str()
                .or(args["selector"].as_str())
                .map(|s| serde_json::to_string(s).unwrap());
            let expression = if let Some(target) = target {
                format!("(() => {{ const e=document.querySelector({target}); e?.scrollBy({dx},{dy}); return true; }})()")
            } else {
                format!("scrollBy({dx},{dy}); true")
            };
            let _ = self.raw_evaluate(&expression, false).await?;
            return Ok(json!({}));
        }
        if action == "upload" {
            let locator = args["locator"]
                .as_str()
                .or(args["selector"].as_str())
                .ok_or("A locator or selector is required")?;
            let selector = if let Some(ref_id) = locator.strip_prefix("aria-ref=") {
                format!(
                    "[data-t3-preview-ref=\"{}\"]",
                    ref_id.replace("t3-preview-", "")
                )
            } else {
                locator.to_owned()
            };
            let root = self.command("DOM.getDocument", json!({"depth":-1})).await?;
            let root_id = root["root"]["nodeId"]
                .as_u64()
                .ok_or("The page DOM was unavailable")?;
            let node = self
                .command(
                    "DOM.querySelector",
                    json!({"nodeId":root_id,"selector":selector}),
                )
                .await?;
            let node_id = node["nodeId"]
                .as_u64()
                .filter(|id| *id != 0)
                .ok_or("No file input matches the locator")?;
            let paths = args["paths"].as_array().ok_or("Missing paths")?;
            self.command(
                "DOM.setFileInputFiles",
                json!({"nodeId":node_id,"files":paths}),
            )
            .await?;
            return Ok(json!({}));
        }
        if action == "press" {
            let key = args["key"].as_str().ok_or("Missing key")?;
            let modifiers = modifier_mask(&args["modifiers"]);
            self.command(
                "Input.dispatchKeyEvent",
                json!({"type":"rawKeyDown","key":key,"modifiers":modifiers}),
            )
            .await?;
            self.command(
                "Input.dispatchKeyEvent",
                json!({"type":"keyUp","key":key,"modifiers":modifiers}),
            )
            .await?;
            return Ok(json!({}));
        }
        if action == "drag" {
            let source = args["source"].as_str().ok_or("Missing source")?;
            let target = args["target"].as_str().ok_or("Missing target")?;
            let source_args = json!({"locator":source});
            let target_args = json!({"locator":target});
            let a = self
                .target_point(&source_args, "locator")
                .await?
                .ok_or("Source not found")?;
            let b = self
                .target_point(&target_args, "locator")
                .await?
                .ok_or("Target not found")?;
            self.command(
                "Input.dispatchMouseEvent",
                json!({"type":"mousePressed","x":a.0,"y":a.1,"button":"left","buttons":1}),
            )
            .await?;
            self.command(
                "Input.dispatchMouseEvent",
                json!({"type":"mouseMoved","x":b.0,"y":b.1,"button":"left","buttons":1}),
            )
            .await?;
            self.command(
                "Input.dispatchMouseEvent",
                json!({"type":"mouseReleased","x":b.0,"y":b.1,"button":"left"}),
            )
            .await?;
            return Ok(json!({}));
        }
        let _ = deadline;
        Err(format!("Unknown preview action {action}"))
    }

    async fn wait_for(&mut self, args: &Value, deadline: Instant) -> Result<Value, String> {
        let condition = |v: &Value| {
            let locator = serde_json::to_string(
                args["locator"]
                    .as_str()
                    .or(args["selector"].as_str())
                    .unwrap_or(""),
            )
            .unwrap();
            let text = serde_json::to_string(args["text"].as_str().unwrap_or("")).unwrap();
            let url = serde_json::to_string(args["urlIncludes"].as_str().unwrap_or("")).unwrap();
            let _ = v;
            format!(
                r#"(() => {{
              const q={locator}, text={text}, url={url}; let ok=true;
              if (q) {{ const e=q.startsWith('aria-ref=')?document.querySelector(`[data-t3-preview-ref="${{q.slice(9).replace('t3-preview-','')}}"]`):document.querySelector(q); ok=ok&&!!e; }}
              if (text) ok=ok&&(document.body?.innerText||'').includes(text);
              if (url) ok=ok&&location.href.includes(url);
              return ok;
            }})()"#
            )
        };
        loop {
            if Instant::now() >= deadline {
                return Err("wait condition timed out".into());
            }
            if self
                .raw_evaluate(&condition(&Value::Null), false)
                .await?
                .as_bool()
                == Some(true)
            {
                return Ok(json!({}));
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
}

fn modifier_mask(modifiers: &Value) -> u64 {
    modifiers
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .map(|m| match m {
            "Alt" => 1,
            "Control" => 2,
            "Meta" => 4,
            "Shift" => 8,
            _ => 0,
        })
        .sum()
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
            if operation != "open" {
                let mut initial = self.initial_url.lock().await;
                if let Some(url) = initial.as_ref() {
                    session.command("Page.navigate", json!({"url":url})).await?;
                    *initial = None;
                }
            }
            match operation {
                "open" => {
                    let state = if let Some(url) = navigation_url(args) {
                        let state = session.navigate(&url, "load", deadline).await?;
                        previews.page_status(
                            &self.thread,
                            &self.tab,
                            json!({"_tag":"Success","url":state["url"],"title":state["title"]}),
                        );
                        state
                    } else {
                        session.page_state().await?
                    };
                    status_result(previews, self, session, &state).await
                }
                "navigate" => {
                    let url = navigation_url(args).ok_or("Missing url or target")?;
                    let readiness = args["readiness"].as_str().unwrap_or("load");
                    let state = session.navigate(&url, readiness, deadline).await?;
                    previews.page_status(
                        &self.thread,
                        &self.tab,
                        json!({"_tag":"Success","url":state["url"],"title":state["title"]}),
                    );
                    status_result(previews, self, session, &state).await
                }
                "snapshot" => session.snapshot(args, self).await,
                "act" => session.act(args, deadline).await,
                "waitFor" => session.wait_for(args, deadline).await,
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
        if !matches!(operation, "open" | "navigate" | "snapshot") {
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
        }
        Ok(result)
    }
}

fn navigation_url(args: &Value) -> Option<String> {
    let target = &args["target"];
    if target.is_object() {
        if target["kind"] == "url" {
            return target["url"].as_str().and_then(|u| normalize_url(u).ok());
        }
        if target["kind"] == "environment-port" {
            let port = target["port"].as_u64()?;
            let protocol = target["protocol"].as_str().unwrap_or("http");
            let path = target["path"].as_str().unwrap_or("");
            return normalize_url(&format!("{protocol}://localhost:{port}{path}")).ok();
        }
    }
    if let Some(url) = args["url"].as_str() {
        return normalize_url(url).ok();
    }
    None
}

async fn status_result(
    previews: &PreviewService,
    page: &Page,
    session: &mut Session,
    state: &Value,
) -> Result<Value, String> {
    let tabs = previews
        .status_tabs(&page.thread)
        .into_iter()
        .map(|tab| {
            let tab_url = tab["navStatus"]["url"]
                .as_str()
                .filter(|url| *url != "about:blank")
                .map(Value::from)
                .unwrap_or(Value::Null);
            json!({
                "tabId":tab["tabId"],
                "url":tab_url,
                "owner":"agent",
                "ownedByCaller":true,
                "visible":false,
            })
        })
        .collect::<Vec<_>>();
    let url = state["url"]
        .as_str()
        .filter(|url| *url != "about:blank")
        .map(Value::from)
        .unwrap_or(Value::Null);
    Ok(json!({
        "available":true,
        "visible":false,
        "tabId":page.tab,
        "url":url,
        "title":state["title"],
        "loading":state["loading"],
        "loadError":null,
        "control":{"owner":"agent","ownedByCaller":true,"generation":0},
        "dialog":null,
        "fileChooser":null,
        "viewportSetting":session.setting,
        "viewport":session.viewport.value(),
        "tabs":tabs,
        "profiles":[{"id":"default","name":"Default"},{"id":"incognito","name":"Incognito","incognito":true}],
        "downloads":[],
    }))
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
