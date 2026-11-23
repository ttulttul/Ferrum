//! Serve a browser tab through Chromium's local DevTools socket. Browser
//! processes and page targets outlive viewers and close with the preview.
use super::previews::{normalize_url, profile_directory};
use crate::AppState;
use axum::extract::ws::{CloseFrame, Message, WebSocket};
use base64::{engine::general_purpose::STANDARD, Engine};
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use std::{
    borrow::Cow,
    collections::{HashMap, VecDeque},
    env,
    path::{Path, PathBuf},
    process::Stdio,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{net::TcpStream, process::Child, sync::watch, time::Instant};
use tokio_tungstenite::{
    connect_async, tungstenite::Message as Wire, MaybeTlsStream, WebSocketStream,
};
use uuid::Uuid;

mod automation;
mod viewport;
use viewport::Size;

type Cdp = WebSocketStream<MaybeTlsStream<TcpStream>>;
#[derive(Default)]
pub(super) struct Browsers {
    processes: HashMap<String, Browser>,
    pages: HashMap<String, Page>,
}
struct Browser {
    child: Child,
    origin: String,
    scratch: Option<PathBuf>,
}
impl Drop for Browser {
    fn drop(&mut self) {
        let _ = self.child.start_kill();
        if let Some(p) = self.scratch.take() {
            let _ = std::fs::remove_dir_all(p);
        }
    }
}
#[derive(Clone)]
pub(super) struct Page {
    pub thread: String,
    pub tab: String,
    id: String,
    profile: String,
    endpoint: String,
    initial_url: Arc<tokio::sync::Mutex<Option<String>>>,
    automation: Arc<tokio::sync::Mutex<Option<automation::Session>>>,
    activity: Arc<Mutex<Activity>>,
    layout: watch::Sender<Size>,
    dialog: Arc<std::sync::atomic::AtomicBool>,
    closed: watch::Sender<bool>,
    control: Arc<Mutex<Control>>,
    scratch: Arc<tempfile::TempDir>,
    downloads: Arc<Mutex<HashMap<String, String>>>,
    chooser: Arc<Mutex<Option<(Uuid, Uuid, u64)>>>,
    chooser_updates: watch::Sender<Option<Uuid>>,
}
struct Activity {
    viewers: usize,
    used: String,
}
struct Control {
    owner: Option<Uuid>,
    requested_size: Option<Size>,
    generation: u64,
    events: watch::Sender<u64>,
}
impl Page {
    fn set_controller(&self, id: Uuid, take: bool) {
        let mut c = self.control.lock().unwrap();
        if (take && c.owner.is_none()) || (!take && c.owner == Some(id)) {
            c.owner = take.then_some(id);
            c.requested_size = None;
            c.generation += 1;
        }
        c.events.send_replace(c.generation);
    }
    pub fn download(&self, id: &str) -> Option<(PathBuf, String)> {
        let name = self.downloads.lock().unwrap().get(id)?.clone();
        let path = self.scratch.path().join(id);
        path.is_file().then_some((path, name))
    }
    pub fn upload_dir(&self) -> &Path {
        self.scratch.path()
    }
    pub fn active_chooser(&self, id: &str) -> Option<(Uuid, u64)> {
        let (chooser, viewer, node) = (*self.chooser.lock().unwrap())?;
        (chooser.to_string() == id && self.control.lock().unwrap().owner == Some(viewer))
            .then_some((chooser, node))
    }
    pub async fn choose_files(&self, id: &str, files: &[PathBuf]) -> bool {
        let Some((chooser, node)) = self.active_chooser(id) else {
            return false;
        };
        let Ok((mut cdp, _)) = connect_async(&self.endpoint).await else {
            return false;
        };
        if self.active_chooser(id).is_none() {
            return false;
        }
        let mut seq = 0;
        if command(
            &mut cdp,
            &mut seq,
            "DOM.setFileInputFiles",
            json!({"files":files,"backendNodeId":node}),
        )
        .await
        .is_err()
        {
            return false;
        }
        let success = tokio::time::timeout(Duration::from_secs(8), async {
            while let Some(Ok(Wire::Text(raw))) = cdp.next().await {
                if let Ok(v) = serde_json::from_str::<Value>(&raw) {
                    if v["id"] == seq {
                        return v.get("result").is_some();
                    }
                }
            }
            false
        })
        .await
        .unwrap_or(false);
        let mut active = self.chooser.lock().unwrap();
        if success && active.as_ref().is_some_and(|c| c.0 == chooser) {
            *active = None;
            self.chooser_updates.send_replace(Some(chooser));
        }
        success
    }
    fn status(&self, id: Uuid, operate: bool) -> Value {
        let c = self.control.lock().unwrap();
        json!({"type":"control","canOperate":operate,"controller":match c.owner{None=>"unclaimed",Some(owner) if owner==id=>"you",_=>"another-viewer"},"generation":c.generation,"dialog":null})
    }
}

pub(super) fn executable() -> Option<PathBuf> {
    if let Some(p) = env::var_os("PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH")
        .map(PathBuf::from)
        .filter(|p| p.is_file())
    {
        return Some(p);
    }
    let home = env::var_os("HOME").map(PathBuf::from).unwrap_or_default();
    let cache = env::var_os("PLAYWRIGHT_BROWSERS_PATH")
        .map(PathBuf::from)
        .unwrap_or_else(|| home.join(".cache/ms-playwright"));
    let mut browsers: Vec<_> = std::fs::read_dir(cache)
        .into_iter()
        .flatten()
        .flatten()
        .filter(|d| d.file_name().to_string_lossy().starts_with("chromium"))
        .map(|d| d.path())
        .collect();
    browsers.sort();
    for p in browsers.into_iter().rev() {
        for file in [
            "chrome-headless-shell-linux64/chrome-headless-shell",
            "chrome-linux64/chrome",
            "chrome-linux/chrome",
            "chrome-headless-shell-mac-arm64/chrome-headless-shell",
            "chrome-headless-shell-win64/chrome-headless-shell.exe",
        ] {
            if p.join(file).is_file() {
                return Some(p.join(file));
            }
        }
    }
    env::split_paths(&crate::login_shell::path().unwrap_or_default()).find_map(|p| {
        [
            "chromium",
            "chromium-browser",
            "google-chrome",
            "chrome.exe",
        ]
        .into_iter()
        .map(|n| p.join(n))
        .find(|p| p.is_file())
    })
}
impl Browsers {
    pub fn page(&self, thread: &str, tab: &str) -> Option<Page> {
        self.pages.get(tab).filter(|p| p.thread == thread).cloned()
    }
    pub async fn open(&mut self, dir: &Path, snap: &Value) -> Option<Page> {
        let id = snap["tabId"].as_str()?;
        if let Some(page) = self.pages.get(id) {
            return Some(page.clone());
        }
        let profile_id = snap["profileId"].as_str().unwrap_or("default");
        let ephemeral = profile_id == "incognito";
        let key = if ephemeral {
            format!("incognito/{id}")
        } else {
            format!("persistent/{profile_id}")
        };
        if !self.processes.contains_key(&key) {
            let executable = executable()?;
            let path = if ephemeral {
                dir.join("server-browser/scratch")
                    .join(Uuid::new_v4().to_string())
            } else {
                profile_directory(dir, profile_id)
            };
            std::fs::create_dir_all(&path).ok()?;
            let port = std::net::TcpListener::bind(("127.0.0.1", 0))
                .ok()?
                .local_addr()
                .ok()?
                .port();
            let origin = format!("http://127.0.0.1:{port}");
            let mut child = crate::login_shell::tokio_command(executable)
                .args([
                    "--headless",
                    "--no-first-run",
                    "--disable-gpu",
                    "--force-device-scale-factor=2",
                    "--disable-blink-features=AutomationControlled",
                    "--force-color-profile=srgb",
                    "--hide-scrollbars",
                    "--no-default-browser-check",
                    "--disable-dev-shm-usage",
                    "--remote-debugging-address=127.0.0.1",
                ])
                .arg(format!("--remote-debugging-port={port}"))
                .arg(format!("--user-data-dir={}", path.display()))
                .arg("about:blank")
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .kill_on_drop(true)
                .spawn()
                .ok()?;
            let mut ready = false;
            for _ in 0..80 {
                if child.try_wait().ok()?.is_some() {
                    break;
                }
                if TcpStream::connect(("127.0.0.1", port)).await.is_ok() {
                    ready = true;
                    break;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            if !ready {
                return None;
            }
            self.processes.insert(
                key.clone(),
                Browser {
                    child,
                    origin,
                    scratch: ephemeral.then_some(path),
                },
            );
        }
        let browser = self.processes.get(&key)?;
        let url = "about:blank";
        let mut address = url::Url::parse(&format!("{}/json/new", browser.origin)).ok()?;
        address.set_query(Some(
            &url::form_urlencoded::byte_serialize(url.as_bytes()).collect::<String>(),
        ));
        let r = reqwest::Client::new()
            .put(address)
            .send()
            .await
            .ok()?
            .error_for_status()
            .ok()?
            .json::<Value>()
            .await
            .ok()?;
        let page = Page {
            thread: snap["threadId"].as_str()?.to_owned(),
            tab: id.to_owned(),
            id: r["id"].as_str()?.to_owned(),
            profile: key,
            endpoint: r["webSocketDebuggerUrl"].as_str()?.to_owned(),
            initial_url: Arc::new(tokio::sync::Mutex::new(
                snap["navStatus"]["url"].as_str().map(str::to_owned),
            )),
            automation: Default::default(),
            layout: watch::channel(Size::DEFAULT).0,
            dialog: Default::default(),
            activity: Arc::new(Mutex::new(Activity {
                viewers: 0,
                used: super::timestamp(),
            })),
            closed: watch::channel(false).0,
            scratch: Arc::new(tempfile::tempdir().ok()?),
            downloads: Default::default(),
            chooser: Default::default(),
            chooser_updates: watch::channel(None).0,
            control: Arc::new(Mutex::new(Control {
                owner: None,
                requested_size: None,
                generation: 0,
                events: watch::channel(0).0,
            })),
        };
        self.pages.insert(id.to_owned(), page.clone());
        Some(page)
    }
    pub async fn close(&mut self, thread: Option<&str>, tab: Option<&str>, profile: Option<&str>) {
        let ids: Vec<_> = self
            .pages
            .iter()
            .filter(|(id, p)| {
                thread.is_none_or(|t| p.thread == t)
                    && tab.is_none_or(|t| *id == t)
                    && profile.is_none_or(|k| p.profile == format!("persistent/{k}"))
            })
            .map(|(id, _)| id.clone())
            .collect();
        for id in ids {
            let p = self.pages.remove(&id).unwrap();
            p.closed.send_replace(true);
            if let Some(browser) = self.processes.get(&p.profile) {
                let _ = reqwest::Client::new()
                    .get(format!("{}/json/close/{}", browser.origin, p.id))
                    .send()
                    .await;
            }
            if !self.pages.values().any(|page| page.profile == p.profile) {
                if let Some(mut browser) = self.processes.remove(&p.profile) {
                    let _ = browser.child.kill().await;
                }
            }
        }
    }
}
async fn command(cdp: &mut Cdp, i: &mut u64, method: &str, params: Value) -> Result<u64, ()> {
    *i += 1;
    cdp.send(Wire::Text(
        json!({"id":i,"method":method,"params":params}).to_string(),
    ))
    .await
    .map_err(|_| ())?;
    Ok(*i)
}
fn user_command(m: &Value) -> Option<(&'static str, Value)> {
    match m["type"].as_str()? {
        "navigate" => Some((
            "Page.navigate",
            json!({"url":normalize_url(m["url"].as_str()?).ok()?}),
        )),
        "reload" => Some(("Page.reload", json!({"ignoreCache":m["ignoreCache"]==true}))),
        "stopLoading" => Some(("Page.stopLoading", json!({}))),
        "mouse" | "wheel" => {
            let ty = if m["type"] == "wheel" {
                "mouseWheel"
            } else {
                match m["action"].as_str() {
                    Some("down") => "mousePressed",
                    Some("up") => "mouseReleased",
                    _ => "mouseMoved",
                }
            };
            let mut p = json!({"type":ty,"x":m["x"].as_f64().unwrap_or(0.0),"y":m["y"].as_f64().unwrap_or(0.0)});
            for k in ["buttons", "clickCount", "deltaX", "deltaY", "modifiers"] {
                if m[k].is_number() {
                    p[k] = m[k].clone();
                }
            }
            if let Some(button @ ("none" | "left" | "right" | "middle")) = m["button"].as_str() {
                p["button"] = json!(button);
            }
            Some(("Input.dispatchMouseEvent", p))
        }
        "key" => {
            let mut p = json!({"type":if m["action"]=="up"{"keyUp"}else if m["text"].is_string(){"keyDown"}else{"rawKeyDown"}});
            for k in ["key", "code", "text"] {
                if m[k].is_string() {
                    p[k] = m[k].clone();
                }
            }
            Some(("Input.dispatchKeyEvent", p))
        }
        "text" | "paste" => Some(("Input.insertText", json!({"text":m["text"].as_str()?}))),
        "dialog" => Some((
            "Page.handleJavaScriptDialog",
            json!({"accept":m["accept"].as_bool()?,"promptText":m["promptText"].as_str().unwrap_or_default()}),
        )),
        _ => None,
    }
}
pub(super) async fn viewer(
    socket: WebSocket,
    page: Page,
    state: Arc<AppState>,
    params: HashMap<String, String>,
) {
    let viewer = Uuid::new_v4();
    struct ViewerPresence(Page, Uuid);
    impl Drop for ViewerPresence {
        fn drop(&mut self) {
            self.0.set_controller(self.1, false);
            self.0.activity.lock().unwrap().viewers -= 1;
        }
    }
    let _presence = ViewerPresence(page.clone(), viewer);
    page.activity.lock().unwrap().viewers += 1;
    let _ = stream(socket, &page, state, viewer, &params).await;
}

fn jpeg(frame: &Value) -> Option<Vec<u8>> {
    STANDARD.decode(frame["data"].as_str()?).ok()
}

async fn capture_still(
    cdp: &mut Cdp,
    sequence: &mut u64,
    quality: u64,
) -> Result<(u64, Instant), ()> {
    let id = command(
        cdp,
        sequence,
        "Page.captureScreenshot",
        json!({"format":"jpeg","quality":quality}),
    )
    .await?;
    Ok((id, Instant::now() + Duration::from_secs(2)))
}

async fn deadline(at: Option<Instant>) {
    match at {
        Some(at) => tokio::time::sleep_until(at).await,
        None => std::future::pending().await,
    }
}

enum FrameAck {
    Still,
    Live { id: u64, at: Instant },
}
impl FrameAck {
    fn live(id: u64) -> Self {
        Self::Live {
            id,
            at: Instant::now() + Duration::from_millis(100),
        }
    }
}
struct Frame {
    bytes: Vec<u8>,
    ack: FrameAck,
}

// One sent frame awaits viewer acknowledgment. Keep only the newest waiting image,
// releasing superseded live frames back to Chromium without stalling viewer input.
#[derive(Default)]
struct ViewerFrames {
    sent: Option<FrameAck>,
    waiting: Option<Frame>,
    acks: VecDeque<(Instant, u64)>,
    live_count: usize,
}
impl ViewerFrames {
    fn release(&mut self, ack: FrameAck) {
        if let FrameAck::Live { id, at } = ack {
            let index = self.acks.partition_point(|(other, _)| *other <= at);
            self.acks.insert(index, (at, id));
        }
    }
    async fn send(&mut self, socket: &mut WebSocket, frame: Frame) -> Result<(), ()> {
        socket
            .send(Message::Binary(frame.bytes))
            .await
            .map_err(|_| ())?;
        self.sent = Some(frame.ack);
        Ok(())
    }
    async fn offer(&mut self, socket: &mut WebSocket, frame: Frame) -> Result<(), ()> {
        if matches!(frame.ack, FrameAck::Live { .. }) {
            self.live_count += 1;
        }
        if self.sent.is_none() {
            self.send(socket, frame).await?
        } else if let Some(previous) = self.waiting.replace(frame) {
            self.release(previous.ack);
        }
        Ok(())
    }
    async fn acknowledge(&mut self, socket: &mut WebSocket) -> Result<(), ()> {
        if let Some(ack) = self.sent.take() {
            self.release(ack)
        }
        if let Some(frame) = self.waiting.take() {
            self.send(socket, frame).await?
        }
        Ok(())
    }
}
async fn stream(
    mut socket: WebSocket,
    page: &Page,
    state: Arc<AppState>,
    viewer: Uuid,
    params: &HashMap<String, String>,
) -> Result<(), ()> {
    let (mut cdp, _) = connect_async(&page.endpoint).await.map_err(|_| ())?;
    let mut sequence = 0;
    let mut gone = page.closed.subscribe();
    let can_operate = params.get("interactive").is_none_or(|s| s != "false");
    if can_operate {
        page.set_controller(viewer, true);
        drop(page.automation.lock().await);
    }
    let mut control = page.control.lock().unwrap().events.subscribe();
    let owns = page.control.lock().unwrap().owner == Some(viewer);
    command(
        &mut cdp,
        &mut sequence,
        "Page.setInterceptFileChooserDialog",
        json!({"enabled":owns}),
    )
    .await?;
    let param = |k: &str, fallback: u64, max: u64| {
        params
            .get(k)
            .and_then(|s| s.parse::<u64>().ok())
            .filter(|n| *n > 0)
            .unwrap_or(fallback)
            .min(max)
    };
    let quality = param("quality", 70, 100);
    let w = param("maxWidth", 1280, 7680);
    let h = param("maxHeight", 800, 4320);
    let mut layout = page
        .viewer_layout(&state.local.previews)
        .await
        .map_err(|_| ())?;
    command(&mut cdp, &mut sequence, "Page.enable", json!({})).await?;
    // Begin navigating when the target is ready, independently of Chromium's first paint.
    {
        let mut initial = page.initial_url.lock().await;
        if let Some(url) = initial.as_ref() {
            command(&mut cdp, &mut sequence, "Page.navigate", json!({"url":url})).await?;
            *initial = None;
        }
    }
    let start = command(
        &mut cdp,
        &mut sequence,
        "Page.startScreencast",
        json!({"format":"jpeg","quality":quality,"maxWidth":w,"maxHeight":h}),
    )
    .await?;
    let info = || json!({"expression":"JSON.stringify({url:location.href,title:document.title,ready:document.readyState})","returnByValue":true});
    let mut document = command(&mut cdp, &mut sequence, "Runtime.evaluate", info()).await?;
    socket
        .send(Message::Text(page.status(viewer, can_operate).to_string()))
        .await
        .map_err(|_| ())?;
    let initial_viewport = layout.borrow_and_update().event();
    socket
        .send(Message::Text(initial_viewport))
        .await
        .map_err(|_| ())?;
    command(
        &mut cdp,
        &mut sequence,
        "Browser.setDownloadBehavior",
        json!({"behavior":"allowAndName","eventsEnabled":true,"downloadPath":page.scratch.path()}),
    )
    .await?;
    let mut pending_downloads = HashMap::<String, String>::new();
    let mut chooser_updates = page.chooser_updates.subscribe();
    let mut frames = ViewerFrames::default();
    let mut live = false;
    let mut still = None;
    let mut request_still = false;
    let mut settle = None;
    let mut overlap = false;
    loop {
        if request_still && still.is_none() {
            still = Some(capture_still(&mut cdp, &mut sequence, quality).await?);
            request_still = false;
        }
        tokio::select! {
            _=gone.changed()=>{let _=socket.send(Message::Close(Some(CloseFrame{code:4404,reason:Cow::Borrowed("tab closed")}))).await;break;}
            _=layout.changed()=>{
                let event = layout.borrow_and_update().event();
                socket.send(Message::Text(event)).await.map_err(|_| ())?;
            }
            _=control.changed()=>{
                let owns=page.control.lock().unwrap().owner==Some(viewer);
                command(&mut cdp,&mut sequence,"Page.setInterceptFileChooserDialog",json!({"enabled":owns})).await?;
                socket.send(Message::Text(page.status(viewer,can_operate).to_string())).await.map_err(|_|())?;
            }
            _=chooser_updates.changed()=>{
                let id=*chooser_updates.borrow();
                if let Some(id)=id{socket.send(Message::Text(json!({"type":"fileChooserClosed","id":id}).to_string())).await.map_err(|_|())?;}
            }
            event=cdp.next()=>{
                let Some(Ok(Wire::Text(raw)))=event else{break};let Ok(e)=serde_json::from_str::<Value>(&raw)else{continue};
                if e["id"] == start && !live {
                    request_still = true;
                }
                if still.is_some_and(|(id, _)| e["id"] == id) {
                    still=None;
                    if let Some(bytes)=jpeg(&e["result"]){
                        frames.offer(&mut socket,Frame{bytes,ack:FrameAck::Still}).await?;
                    }
                }
                if e["id"]==document{
                    if let Some(doc)=e["result"]["result"]["value"].as_str().and_then(|s|serde_json::from_str::<Value>(s).ok()){
                        if let Some(url)=doc["url"].as_str().filter(|u|*u!="about:blank"){state.local.previews.page_status(&page.thread,&page.tab,json!({"_tag":if doc["ready"]=="complete"{"Success"}else{"Loading"},"url":url,"title":doc["title"].as_str().unwrap_or("")}));}
                    }
                }
                match e["method"].as_str(){
                    Some("Page.javascriptDialogOpening") => page.dialog.store(true, std::sync::atomic::Ordering::Release),
                    Some("Page.javascriptDialogClosed") => page.dialog.store(false, std::sync::atomic::Ordering::Release),
                    Some("Page.fileChooserOpened") if page.control.lock().unwrap().owner==Some(viewer)=>{
                        if let Some(node)=e["params"]["backendNodeId"].as_u64(){
                            let id=Uuid::new_v4();*page.chooser.lock().unwrap()=Some((id,viewer,node));
                            socket.send(Message::Text(json!({"type":"fileChooser","id":id,"multiple":e["params"]["mode"]=="selectMultiple","accept":""}).to_string())).await.map_err(|_|())?;
                        }
                    }
                    Some("Browser.downloadWillBegin"|"Page.downloadWillBegin") if e["params"]["frameId"]==page.id=>{
                        if let Some((guid,name))=e["params"]["guid"].as_str().zip(e["params"]["suggestedFilename"].as_str()).filter(|(id,_)|Uuid::parse_str(id).is_ok()){
                            pending_downloads.insert(guid.to_owned(),name.to_owned());
                        }
                    }
                    Some("Browser.downloadProgress"|"Page.downloadProgress")=>{
                        let event=&e["params"];if let Some(guid)=event["guid"].as_str(){if event["state"]=="completed"{if let Some(name)=pending_downloads.remove(guid){
                            let path=page.scratch.path().join(guid);
                            if let Ok(stat)=std::fs::metadata(path){page.downloads.lock().unwrap().insert(guid.to_owned(),name.clone());
                                if page.control.lock().unwrap().owner==Some(viewer){socket.send(Message::Text(json!({"type":"download","id":guid,"fileName":name,"sizeBytes":stat.len()}).to_string())).await.map_err(|_|())?;}
                            }
                        }}else if event["state"]=="canceled"{pending_downloads.remove(guid);}}
                    }
                    Some("Page.screencastFrame")=>{
                        live=true;
                        let data=&e["params"];
                        if let (Some(bytes),Some(id))=(jpeg(data), data["sessionId"].as_u64()){
                            overlap |= frames.live_count > 0;
                            settle=Some(Instant::now()+Duration::from_millis(200));
                            frames.offer(&mut socket,Frame{bytes,ack:FrameAck::live(id)}).await?;
                        }
                    }
                    Some("Page.loadEventFired"|"Page.frameNavigated")=>{document=command(&mut cdp,&mut sequence,"Runtime.evaluate",info()).await?;}
                    _=>{}
                }
            }
            _ = deadline(still.map(|(_, timeout)| timeout)) => still = None,
            _=deadline(settle)=>{
                settle=None;
                request_still |= overlap;
                overlap=false;
            }
            _=deadline(frames.acks.front().map(|(at,_)|*at))=>{
                let (_,id)=frames.acks.pop_front().expect("ack deadline");
                frames.live_count-=1;
                command(&mut cdp,&mut sequence,"Page.screencastFrameAck",json!({"sessionId":id})).await?;
            }
            msg=socket.recv()=>{
                let raw = match msg {
                    Some(Ok(Message::Text(raw))) => raw,
                    Some(Ok(Message::Ping(_) | Message::Pong(_))) => continue,
                    _ => break,
                };let Ok(m)=serde_json::from_str::<Value>(&raw)else{continue};
                match m["type"].as_str(){
                    Some("ack")=>frames.acknowledge(&mut socket).await?,
                    Some("takeControl") if can_operate=>{page.set_controller(viewer,true);drop(page.automation.lock().await);},
                    Some("releaseControl") if can_operate=>page.set_controller(viewer,false),
                    Some("resize" | "viewport") if page.control.lock().unwrap().owner == Some(viewer) => {
                        page.viewer_resize(&state.local.previews, viewer, &m).await.map_err(|_| ())?;
                    }
                    _ if page.control.lock().unwrap().owner==Some(viewer)=>{if let Some((method,p))=user_command(&m){command(&mut cdp,&mut sequence,method,p).await?;}}
                    _=>{}
                }
            }
        }
    }
    Ok(())
}
