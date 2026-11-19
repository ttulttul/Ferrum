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
        .route("/api/preview-stream/upload", get(preview).post(preview))
}
fn text(status: StatusCode, body: &'static str) -> Response {
    (
        status,
        [(header::CONTENT_TYPE, "text/plain")],
        body,
    )
        .into_response()
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
    if let Err(e) = authorize_upgrade(&state, req.headers(), req.uri(), "media").await {
        return auth_error(req.headers(), e);
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
            Ok(ws) => ws.on_upgrade(move |client| bridge(client, url.replacen("http", "ws", 1))),
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
    if let Err(e) = authorize_upgrade(&state, req.headers(), req.uri(), "media").await {
        return auth_error(req.headers(), e);
    }
    match path {
        "download" => text(StatusCode::NOT_FOUND, "Not Found"),
        "upload" => text(
            StatusCode::CONFLICT,
            "The page's file picker is no longer open.",
        ),
        _ => {
            let params: std::collections::HashMap<_, _> =
                url::form_urlencoded::parse(req.uri().query().unwrap_or("").as_bytes())
                    .into_owned()
                    .collect();
            let Some((thread, tab)) = params
                .get("threadId")
                .zip(params.get("tabId"))
                .filter(|(t, id)| !t.is_empty() && !id.is_empty())
            else {
                return text(StatusCode::NOT_FOUND, "Not Found");
            };
            if state.previews.tab(thread, tab).is_some() {
                return text(StatusCode::SERVICE_UNAVAILABLE, "Service Unavailable");
            }
            let (mut parts, _) = req.into_parts();
            match WebSocketUpgrade::from_request_parts(&mut parts, &state).await {
                Ok(ws) => ws.on_upgrade(|mut s| async move {
                    let _ = s
                        .send(Message::Close(Some(CloseFrame {
                            code: 4404,
                            reason: Cow::Borrowed("tab closed"),
                        })))
                        .await;
                }),
                Err(e) => e.into_response(),
            }
        }
    }
}
