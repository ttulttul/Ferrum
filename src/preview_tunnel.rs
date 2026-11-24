//! The preview tunnel (`GET /api/preview/tunnel?port=N`), as the reference's
//! `PreviewTunnel` (#39415): a WebSocket that carries a remote client's
//! preview traffic to one of this machine's loopback ports, one TCP
//! connection per socket, as raw bytes, so pages keep their own origin, HMR
//! sockets and TLS. It serves while the `enablePreviewTunnel` setting is on
//! and no managed policy switches it off, and closes with its session or
//! when the tunnel is switched off.
use crate::server::{authorize_upgrade, AppState};
use axum::{
    extract::{
        ws::{Message, WebSocket},
        FromRequestParts, Request, State, WebSocketUpgrade,
    },
    http::{header, HeaderValue, StatusCode},
    response::{IntoResponse, Response},
};
use futures_util::{SinkExt, StreamExt};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
/// Marks a refusal about the port itself, so a client can tell "nothing
/// listens there" from a proxy answering 502 because the environment is down.
const PORT_REFUSED_HEADER: &str = "x-t3-tunnel-port-refused";
/// Marks a refusal because the tunnel is switched off, by setting or policy.
const DISABLED_HEADER: &str = "x-t3-tunnel-disabled";
const READ_CHUNK_BYTES: usize = 64 * 1024;

/// `allowsPreviewTunnel`: the setting is on and no policy switches it off.
/// Unreadable settings fail closed: the tunnel is raw TCP into the host.
pub(crate) fn allowed(state: &AppState) -> bool {
    let setting = state
        .settings
        .internal()
        .ok()
        .is_some_and(|settings| settings["enablePreviewTunnel"] != false);
    setting && state.settings.policy.allows_preview_tunnel()
}

fn text(status: StatusCode, body: &str, refusal: Option<&'static str>) -> Response {
    let mut response = (status, body.to_owned()).into_response();
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("text/plain; charset=utf-8"),
    );
    if let Some(name) = refusal {
        response
            .headers_mut()
            .insert(name, HeaderValue::from_static("1"));
    }
    response
}

/// The port this server answers on: tunnelling there would hand the page an
/// unauthenticated loopback client.
fn served_port(state: &AppState) -> Option<u16> {
    let endpoint = state.mcp_endpoint.get()?;
    url::Url::parse(endpoint).ok()?.port()
}

/// Dial the loopback port, IPv4 then IPv6.
async fn dial(port: u16) -> Option<TcpStream> {
    for host in ["127.0.0.1", "::1"] {
        if let Ok(Ok(stream)) =
            tokio::time::timeout(CONNECT_TIMEOUT, TcpStream::connect((host, port))).await
        {
            return Some(stream);
        }
    }
    None
}

pub(crate) async fn handler(State(state): State<Arc<AppState>>, request: Request) -> Response {
    let port = url::form_urlencoded::parse(request.uri().query().unwrap_or("").as_bytes())
        .find(|(key, _)| key == "port")
        .and_then(|(_, value)| value.parse::<f64>().ok())
        .filter(|port| port.fract() == 0.0 && (1.0..=65_535.0).contains(port))
        .map(|port| port as u16);
    let upgrade = request
        .headers()
        .get(header::UPGRADE)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.eq_ignore_ascii_case("websocket"));
    let Some(port) = port.filter(|_| upgrade) else {
        return text(StatusCode::BAD_REQUEST, "Bad Request", None);
    };
    let session = match authorize_upgrade(&state, request.headers(), request.uri(), "media").await {
        Ok(session) => session,
        Err(reason) => return crate::access_http::auth_error(request.headers(), reason),
    };
    if !session.has("terminal:operate") {
        return crate::access_http::scope_error(request.headers(), "terminal:operate");
    }
    // Subscribed before the setting is read, so a switch-off while dialling is not missed.
    let mut settings = state.settings.subscribe();
    if !allowed(&state) {
        return text(
            StatusCode::FORBIDDEN,
            "The preview tunnel is turned off on this T3 Code server.",
            Some(DISABLED_HEADER),
        );
    }
    if served_port(&state) == Some(port) {
        return text(
            StatusCode::FORBIDDEN,
            &format!("Port {port} is this T3 Code server's own port."),
            Some(PORT_REFUSED_HEADER),
        );
    }
    // Dialled before the upgrade, so a dead port fails the handshake rather
    // than an open socket. Not 502: a proxy in front swaps an origin 502 for
    // its own page.
    let Some(upstream) = dial(port).await else {
        return text(
            StatusCode::FORBIDDEN,
            &format!("Nothing accepted a connection on loopback port {port}."),
            Some(PORT_REFUSED_HEADER),
        );
    };
    let (mut parts, _body) = request.into_parts();
    let upgrade = match WebSocketUpgrade::from_request_parts(&mut parts, &state).await {
        Ok(upgrade) => upgrade,
        Err(rejection) => return rejection.into_response(),
    };
    upgrade.on_upgrade(move |socket| async move {
        let invalidated = state.auth.invalidated(&session);
        let turned_off = {
            let state = state.clone();
            async move {
                while let Ok(_) | Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) =
                    settings.recv().await
                {
                    if !allowed(&state) {
                        return;
                    }
                }
                // A change stream that ends without switching the tunnel off never does.
                std::future::pending::<()>().await;
            }
        };
        tokio::select! {
            _ = pump(socket, upstream) => {}
            _ = invalidated => {}
            _ = turned_off => {}
        }
    })
}

/// Raw bytes both ways until either side closes.
async fn pump(socket: WebSocket, upstream: TcpStream) {
    let (mut client_tx, mut client_rx) = socket.split();
    let (mut upstream_rx, mut upstream_tx) = upstream.into_split();
    let to_upstream = async {
        while let Some(Ok(message)) = client_rx.next().await {
            let bytes = match message {
                Message::Binary(bytes) => bytes,
                Message::Text(text) => text.into_bytes(),
                Message::Close(_) => break,
                _ => continue,
            };
            if upstream_tx.write_all(&bytes).await.is_err() {
                break;
            }
        }
    };
    let to_client = async {
        let mut buffer = vec![0u8; READ_CHUNK_BYTES];
        loop {
            match upstream_rx.read(&mut buffer).await {
                Ok(0) | Err(_) => break,
                Ok(read) => {
                    // The sink waits for the socket, so a slow client applies backpressure.
                    if client_tx
                        .send(Message::Binary(buffer[..read].to_vec()))
                        .await
                        .is_err()
                    {
                        break;
                    }
                }
            }
        }
        let _ = client_tx.close().await;
    };
    tokio::select! {
        _ = to_upstream => {}
        _ = to_client => {}
    }
}
