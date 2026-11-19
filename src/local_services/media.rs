use crate::{auth_error, authorize_upgrade, AppState};
use axum::{
    body::Body,
    extract::{
        ws::{CloseFrame, Message, WebSocket},
        FromRequestParts, Request, State, WebSocketUpgrade,
    },
    http::{header, Method, StatusCode},
    response::{IntoResponse, Response},
    routing::{any, get},
    Router,
};
use futures_util::{SinkExt, StreamExt};
use std::{borrow::Cow, sync::Arc};
use tokio_tungstenite::tungstenite::Message as Upstream;
pub fn routes() -> Router<Arc<AppState>> {
    Router::new()
        .route("/api/device-hub/*path", any(hub))
        .route("/api/preview-stream/*path", get(preview))
        .route(
            "/api/preview-stream/upload",
            get(preview)
                .post(preview)
                .layer(axum::extract::DefaultBodyLimit::max(100 * 1024 * 1024)),
        )
}
fn text(status: StatusCode, body: &'static str) -> Response {
    (status, [(header::CONTENT_TYPE, "text/plain")], body).into_response()
}
fn paths(path: &str) -> (bool, bool, bool) {
    let parts: Vec<&str> = path.split('/').collect();
    let read = matches!(
        &parts[..],
        ["", "api", "devices"]
            | ["", "vendor", "serve-sim" | "serve-emu", "api"]
            | ["", "vendor", "serve-sim", "appstate"]
            | ["", "vendor", "serve-sim", "api", "screenshot" | "event-log"]
            | ["", "vendor", "serve-sim", "api", "event-log", "events"]
            | ["", "vendor", "serve-emu", "health"]
            | [
                "",
                "vendor",
                "serve-emu",
                "api",
                "devices"
                    | "screenshot"
                    | "stream-mode"
                    | "stream-settings"
                    | "accessibility"
                    | "fold"
            ]
    ) || matches!(&parts[..],["","vendor","serve-sim","helper",id,"stream.mjpeg"|"stream.avcc"|"config"|"health"|"ax"|"foreground"] | ["","vendor","serve-sim","helper",id,"panel","1"|"3","stream.avcc"] | ["","vendor","serve-sim","helper",id,"webrtc","offer"|"close"] if safe_segment(id));
    let mutate = matches!(
        &parts[..],
        ["", "vendor", "serve-sim", "api", "screenshot"]
            | ["", "vendor", "serve-emu", "webrtc", "offer" | "close"]
            | [
                "",
                "vendor",
                "serve-emu",
                "api",
                "screenshot" | "stream-mode" | "stream-settings" | "fold"
            ]
    ) || matches!(&parts[..],["","vendor","serve-sim","helper",id,"webrtc","offer"|"close"] if safe_segment(id));
    let websocket = matches!(
        path,
        "/api/devices/ws" | "/vendor/serve-sim/helper/ws" | "/vendor/serve-emu/ws"
    );
    (read, mutate, websocket)
}
fn safe_segment(id: &str) -> bool {
    !id.is_empty() && !id.contains(['\\', '%']) && id != "." && id != ".."
}
fn upgrades(req: &Request) -> bool {
    req.headers()
        .get("upgrade")
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.eq_ignore_ascii_case("websocket"))
}
async fn hub(State(state): State<Arc<AppState>>, req: Request) -> Response {
    let path = req
        .uri()
        .path()
        .strip_prefix("/api/device-hub")
        .unwrap_or("/");
    let (read, mutate, ws) = paths(path);
    let upgrade = upgrades(&req);
    if if upgrade { !ws } else { !read && !mutate } {
        return text(StatusCode::NOT_FOUND, "Not Found");
    }
    if !upgrade
        && if matches!(req.method(), &Method::GET | &Method::HEAD) {
            !read
        } else {
            !mutate
        }
    {
        return text(StatusCode::METHOD_NOT_ALLOWED, "Method Not Allowed");
    }
    let session = match authorize_upgrade(&state, req.headers(), req.uri(), "media").await {
        Ok(s) => s,
        Err(e) => return auth_error(req.headers(), e),
    };
    let device_control = (upgrade && path != "/api/devices/ws")
        || (!matches!(req.method(), &Method::GET | &Method::HEAD)
            && ["/api/stream-mode", "/api/stream-settings", "/api/fold"]
                .iter()
                .any(|p| path.ends_with(p)));
    let scope = if device_control {
        "orchestration:operate"
    } else {
        "orchestration:read"
    };
    if !session.has(scope) {
        return crate::access_http::scope_error(req.headers(), scope);
    }
    let (host, query) = {
        let mut host = None;
        let mut query = url::form_urlencoded::Serializer::new(String::new());
        for (k, v) in url::form_urlencoded::parse(req.uri().query().unwrap_or_default().as_bytes())
        {
            match k.as_ref() {
                "wsTicket" => {}
                "hostId" => {
                    host.get_or_insert(v.into_owned());
                }
                _ => {
                    query.append_pair(&k, &v);
                }
            }
        }
        (host, query.finish())
    };
    let Some(origin) = state.devices.hub_origin(host.as_deref()).await else {
        return text(StatusCode::SERVICE_UNAVAILABLE, "Device hub is not running");
    };
    let mut url = format!("{origin}{path}");
    if !query.is_empty() {
        url.push('?');
        url.push_str(&query);
    }
    if upgrade {
        let (mut parts, _) = req.into_parts();
        return match WebSocketUpgrade::from_request_parts(&mut parts, &state).await {
            Ok(ws) => ws.on_upgrade(move |client| async move {tokio::select!{_ = state.auth.invalidated(&session)=>{}, _=bridge(client,url.replacen("http","ws",1))=>{}}}),
            Err(e) => e.into_response(),
        };
    }
    let mut request = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap()
        .request(req.method().clone(), url);
    for (key, value) in req.headers() {
        match key.as_str() {
            "host"
            | "connection"
            | "upgrade"
            | "cookie"
            | "authorization"
            | "dpop"
            | "content-length"
            | "accept-encoding"
            | "x-forwarded-proto"
            | "proxy-authorization"
            | "transfer-encoding" => {}
            k if k.starts_with("sec-websocket-") => {}
            "origin" => request = request.header(key, &origin),
            _ => request = request.header(key, value),
        }
    }
    if !matches!(req.method(), &Method::GET | &Method::HEAD) {
        request = request.body(reqwest::Body::wrap_stream(
            req.into_body().into_data_stream(),
        ));
    }
    let Ok(response) = request.send().await else {
        return text(StatusCode::BAD_GATEWAY, "Bad Gateway");
    };
    let mut out = Response::builder().status(response.status());
    for (k, v) in response.headers() {
        if !matches!(
            k.as_str(),
            "content-encoding"
                | "transfer-encoding"
                | "connection"
                | "set-cookie"
                | "cache-control"
        ) {
            out = out.header(k, v);
        }
    }
    out.header("cache-control", "no-store, no-transform")
        .body(Body::from_stream(response.bytes_stream()))
        .unwrap()
}
async fn bridge(mut socket: WebSocket, url: String) {
    let Ok((mut upstream, _)) = tokio_tungstenite::connect_async(url).await else {
        let _ = socket.close().await;
        return;
    };
    loop {
        tokio::select! {
            incoming=socket.recv()=>{
                let Some(Ok(m))=incoming else{break;};
                let m=match m{Message::Text(x)=>Upstream::Text(x),Message::Binary(x)=>Upstream::Binary(x),Message::Ping(x)=>Upstream::Ping(x),Message::Pong(x)=>Upstream::Pong(x),Message::Close(c)=>Upstream::Close(c.map(|c|tokio_tungstenite::tungstenite::protocol::CloseFrame{code:c.code.into(),reason:c.reason}))};
                if upstream.send(m).await.is_err(){break}
            }
            incoming=upstream.next()=>{
                let Some(Ok(m))=incoming else{break};
                let m=match m{Upstream::Text(x)=>Message::Text(x),Upstream::Binary(x)=>Message::Binary(x),Upstream::Ping(x)=>Message::Ping(x),Upstream::Pong(x)=>Message::Pong(x),Upstream::Close(c)=>Message::Close(c.map(|c|CloseFrame{code:c.code.into(),reason:c.reason})),Upstream::Frame(_)=>continue};
                if socket.send(m).await.is_err(){break;}
            }
        }
    }
}
fn disposition(name: &str) -> String {
    let sanitized: String = name
        .chars()
        .map(|c| {
            if c.is_control() || matches!(c, '"' | '\\') {
                '_'
            } else {
                c
            }
        })
        .collect();
    let ascii: String = sanitized
        .chars()
        .map(|c| if c.is_ascii() { c } else { '_' })
        .collect();
    let mut header = format!("attachment; filename=\"{ascii}\"");
    if ascii != sanitized {
        let extended: String = sanitized
            .bytes()
            .map(|b| {
                if b.is_ascii_alphanumeric() || b"~!.-_".contains(&b) {
                    char::from(b).to_string()
                } else {
                    format!("%{b:02X}")
                }
            })
            .collect();
        header.push_str(&format!("; filename*=UTF-8''{extended}"));
    }
    header
}
async fn download(page: Option<super::browser::Page>, id: &str) -> Response {
    let Some((path, file_name)) = page.and_then(|p| p.download(id)) else {
        return text(StatusCode::NOT_FOUND, "Not Found");
    };
    let Ok(file) = tokio::fs::File::open(path).await else {
        return text(StatusCode::NOT_FOUND, "Not Found");
    };
    let stream = futures_util::stream::try_unfold(file, |mut file| async move {
        use tokio::io::AsyncReadExt;
        let mut bytes = vec![0; 65536];
        let n = file.read(&mut bytes).await?;
        bytes.truncate(n);
        Ok::<_, std::io::Error>((n != 0).then_some((bytes, file)))
    });
    Response::builder()
        .status(200)
        .header("cache-control", "private, max-age=3600")
        .header("x-content-type-options", "nosniff")
        .header("content-disposition", disposition(&file_name))
        .header("content-security-policy", "default-src 'none'; sandbox")
        .header("content-type", "application/octet-stream")
        .body(Body::from_stream(stream))
        .unwrap()
}
async fn upload(req: Request, page: Option<super::browser::Page>, id: &str) -> Response {
    use axum::extract::{FromRequest, Multipart};
    use tokio::io::AsyncWriteExt;
    let expired = || {
        text(
            StatusCode::CONFLICT,
            "The page's file picker is no longer open.",
        )
    };
    let Some(page) = page.filter(|p| p.active_chooser(id).is_some()) else {
        return expired();
    };
    let Ok(mut parts) = Multipart::from_request(req, &()).await else {
        return text(StatusCode::BAD_REQUEST, "Bad Request");
    };
    let base = page.upload_dir().join(uuid::Uuid::new_v4().to_string());
    let mut nbytes = 0;
    let mut files = Vec::new();
    let mut count = 0;
    loop {
        let mut part = match parts.next_field().await {
            Ok(Some(part)) => part,
            Ok(None) => break,
            Err(e) => return e.into_response(),
        };
        count += 1;
        if count > 20 {
            return text(StatusCode::PAYLOAD_TOO_LARGE, "Payload Too Large");
        }
        let Some(name) = part
            .file_name()
            .and_then(|raw| std::path::Path::new(raw).file_name())
            .and_then(|s| s.to_str())
            .filter(|s| !s.is_empty())
            .map(str::to_owned)
        else {
            continue;
        };
        let dir = base.join(count.to_string());
        if tokio::fs::create_dir_all(&dir).await.is_err() {
            return text(StatusCode::INTERNAL_SERVER_ERROR, "Internal Server Error");
        }
        let path = dir.join(name);
        let Ok(mut file) = tokio::fs::File::create(&path).await else {
            return text(StatusCode::INTERNAL_SERVER_ERROR, "Internal Server Error");
        };
        loop {
            match part.chunk().await {
                Ok(Some(bytes)) => {
                    nbytes += bytes.len();
                    if nbytes > 100 * 1024 * 1024 {
                        return text(StatusCode::PAYLOAD_TOO_LARGE, "Payload Too Large");
                    }
                    if file.write_all(&bytes).await.is_err() {
                        return text(StatusCode::INTERNAL_SERVER_ERROR, "Internal Server Error");
                    }
                }
                Ok(None) => break,
                Err(e) => return e.into_response(),
            }
        }
        files.push(path);
    }
    if page.choose_files(id, &files).await {
        StatusCode::NO_CONTENT.into_response()
    } else {
        let _ = tokio::fs::remove_dir_all(base).await;
        expired()
    }
}
async fn preview(State(state): State<Arc<AppState>>, req: Request) -> Response {
    let path = req.uri().path().trim_start_matches("/api/preview-stream/");
    match path {
        "upload" if req.method() != Method::POST => {
            return text(StatusCode::METHOD_NOT_ALLOWED, "Method Not Allowed")
        }
        "ws" if !upgrades(&req) => return text(StatusCode::NOT_FOUND, "Not Found"),
        "upload" | "download" | "ws" => {}
        _ => return text(StatusCode::NOT_FOUND, "Not Found"),
    }
    let session = match authorize_upgrade(&state, req.headers(), req.uri(), "media").await {
        Ok(s) => s,
        Err(e) => return auth_error(req.headers(), e),
    };
    let required = if path == "upload" {
        "preview:operate"
    } else {
        "orchestration:read"
    };
    if !session.has(required) {
        return crate::access_http::scope_error(req.headers(), required);
    }
    let mut params: std::collections::HashMap<String, String> =
        url::form_urlencoded::parse(req.uri().query().unwrap_or("").as_bytes())
            .into_owned()
            .collect();
    if !session.has("preview:operate") {
        params.insert("interactive".into(), "false".into());
    }
    let get = |key: &str| params.get(key).map(String::as_str).unwrap_or("");
    match path {
        "download" | "upload" => {
            let page = state
                .previews
                .browsers
                .lock()
                .await
                .page(get("threadId"), get("tabId"));
            if path == "download" {
                download(page, get("id")).await
            } else {
                let id = get("chooser").to_owned();
                upload(req, page, &id).await
            }
        }
        _ => {
            let Some((thread, tab)) = params
                .get("threadId")
                .zip(params.get("tabId"))
                .filter(|(t, id)| !t.is_empty() && !id.is_empty())
            else {
                return text(StatusCode::NOT_FOUND, "Not Found");
            };
            let (mut parts, _) = req.into_parts();
            let ws = match WebSocketUpgrade::from_request_parts(&mut parts, &state).await {
                Ok(ws) => ws,
                Err(e) => return e.into_response(),
            };
            let mut browsers = state.previews.browsers.lock().await;
            let snapshot = state
                .previews
                .tab(thread, tab)
                .filter(|s| s["runtime"] == "server");
            if let Some(snap) = snapshot {
                let page = browsers.open(&state.data_dir, &snap).await;
                drop(browsers);
                if let Some(page) = page {
                    return ws.on_upgrade(move |socket| async move {
                        tokio::select!{_ = state.auth.invalidated(&session)=>{}, _=super::browser::viewer(socket,page,state.clone(),params)=>{}}
                    });
                }
                return text(StatusCode::SERVICE_UNAVAILABLE, "Service Unavailable");
            }
            ws.on_upgrade(|mut s| async move {
                let _ = s
                    .send(Message::Close(Some(CloseFrame {
                        code: 4404,
                        reason: Cow::Borrowed("tab closed"),
                    })))
                    .await;
            })
        }
    }
}
