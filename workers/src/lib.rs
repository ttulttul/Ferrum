//! The WebAssembly build's entry point for the T3 Code control plane.
//!
//! The worker forwards every request to the `T3EnvironmentDO` named by the
//! request's host, so each hostname is one environment. The object answers
//! HTTP through the control plane's axum router and handles `/ws` itself with
//! a WebSocket API that can suspend between messages: each socket's `Connection` (principal and
//! open streams) lives in the socket's attachment, and every message is one
//! call to `ControlPlane::on_message`.
mod storage;

use std::sync::Arc;
use t3code_server::control::{Config, Connection, ControlPlane};
use tower_service::Service;
use worker::*;

#[event(fetch)]
async fn fetch(req: Request, env: Env, _ctx: Context) -> Result<Response> {
    let host = req
        .headers()
        .get("host")?
        .unwrap_or_default()
        .split(':')
        .next()
        .unwrap_or_default()
        .to_ascii_lowercase();
    let namespace = env.durable_object("T3_ENVIRONMENT")?;
    let stub = namespace.id_from_name(&host)?.get_stub()?;
    stub.fetch_with_request(req).await
}

#[durable_object]
pub struct T3EnvironmentDO {
    state: State,
    sql: SqlStorage,
    plane: Arc<ControlPlane>,
    router: axum::Router,
    started_ms: f64,
    init_ms: f64,
}

fn now_ms() -> f64 {
    t3code_server::platform::now_ms() as f64
}

/// The Workers clock: `Date.now()`, which the runtime advances only across
/// I/O (Spectre mitigation), held at its highest reading for the monotonic one.
struct WorkersClock {
    highest: std::sync::atomic::AtomicU64,
}

impl t3code_server::platform::Clock for WorkersClock {
    fn now_ms(&self) -> i64 {
        js_sys::Date::now() as i64
    }

    fn monotonic_ms(&self) -> u64 {
        let now = self.now_ms().max(0) as u64;
        self.highest
            .fetch_max(now, std::sync::atomic::Ordering::Relaxed)
            .max(now)
    }
}

/// Install the Workers clock (`crypto.getRandomValues` stays the random
/// source) once per process; later instances share it.
fn install_platform() {
    let _ = t3code_server::platform::install(t3code_server::platform::Platform {
        clock: Arc::new(WorkersClock { highest: 0.into() }),
        random: Arc::new(t3code_server::platform::SystemRandom),
    });
}

/// Wasm linear memory only grows, so its size is this process's peak.
fn linear_memory_bytes() -> usize {
    core::arch::wasm32::memory_size(0) * 65536
}

impl DurableObject for T3EnvironmentDO {
    fn new(state: State, env: Env) -> Self {
        console_error_panic_hook::set_once();
        install_platform();
        let started = now_ms();
        let sql = state.storage().sql();
        storage::migrate(&sql).expect("create control plane tables");
        let environment_id = match storage::get(&sql, "environment-id").expect("read kv") {
            Some(id) => id,
            None => {
                let id = t3code_server::platform::uuid_v4().to_string();
                storage::put(&sql, "environment-id", &id).expect("write kv");
                id
            }
        };
        let bootstrap_credential = env
            .secret("T3_BOOTSTRAP_CREDENTIAL")
            .map(|s| s.to_string())
            .or_else(|_| env.var("T3_BOOTSTRAP_CREDENTIAL").map(|v| v.to_string()))
            .unwrap_or_default();
        let label = env
            .var("T3_ENVIRONMENT_LABEL")
            .map(|v| v.to_string())
            .unwrap_or_else(|_| "T3 Cloud".into());
        let documents = storage::SqlDocuments(sql.clone());
        storage::migrate_auth_records(
            &sql,
            &documents,
            &std::path::Path::new(t3code_server::control::STATE_DIR).join("environment-auth.json"),
        )
        .expect("migrate auth records");
        let plane = Arc::new(
            ControlPlane::open(
                Config {
                    environment_id,
                    label,
                    bootstrap_credential,
                    cookie_port: 3773,
                },
                Arc::new(documents),
                Box::new(storage::SqlLog(sql.clone())),
            )
            .expect("open control plane"),
        );
        let router = plane.router();
        let init_ms = now_ms() - started;
        console_log!(
            "T3EnvironmentDO ready in {init_ms} ms: sequence {}, linear memory {} bytes",
            plane.sequence(),
            linear_memory_bytes()
        );
        Self {
            state,
            sql,
            plane,
            router,
            started_ms: started,
            init_ms,
        }
    }

    async fn fetch(&self, req: Request) -> Result<Response> {
        let path = req.path();
        if path == "/__spike/stats" {
            return Response::from_json(&serde_json::json!({
                "initMs": self.init_ms,
                "uptimeMs": now_ms() - self.started_ms,
                "sequence": self.plane.sequence(),
                "linearMemoryBytes": linear_memory_bytes(),
                "sqlBytes": self.sql.database_size(),
                "sockets": self.state.get_websockets().len(),
            }));
        }
        let upgrade = req
            .headers()
            .get("upgrade")?
            .is_some_and(|v| v.eq_ignore_ascii_case("websocket"));
        if path == "/ws" && upgrade {
            return self.upgrade(&req).await;
        }
        self.http(req).await
    }

    async fn websocket_message(
        &self,
        ws: WebSocket,
        message: WebSocketIncomingMessage,
    ) -> Result<()> {
        let text = match message {
            WebSocketIncomingMessage::String(text) => text,
            WebSocketIncomingMessage::Binary(bytes) => String::from_utf8_lossy(&bytes).into_owned(),
        };
        let Some(mut connection) = ws.deserialize_attachment::<Connection>()? else {
            ws.close(Some(1011), Some("missing connection state"))?;
            return Ok(());
        };
        for frame in self.plane.on_message(&mut connection, &text) {
            ws.send_with_str(frame)?;
        }
        ws.serialize_attachment(&connection)?;
        Ok(())
    }

    async fn websocket_close(
        &self,
        ws: WebSocket,
        _code: usize,
        _reason: String,
        _was_clean: bool,
    ) -> Result<()> {
        if let Some(connection) = ws.deserialize_attachment::<Connection>()? {
            self.plane.disconnect(&connection);
        }
        Ok(())
    }
}

impl T3EnvironmentDO {
    async fn upgrade(&self, req: &Request) -> Result<Response> {
        let (headers, uri) = (http_headers(req)?, path_and_query(req)?);
        let connection = match self.plane.authorize_socket(&headers, &uri) {
            Ok(connection) => connection,
            Err(rejection) => return worker_response(rejection).await,
        };
        let pair = WebSocketPair::new()?;
        // The runtime may evict this object between messages;
        // the attachment carries the connection across.
        self.state.accept_web_socket(&pair.server);
        pair.server.serialize_attachment(&connection)?;
        Response::from_websocket(pair.client)
    }

    async fn http(&self, mut req: Request) -> Result<Response> {
        let mut builder = http::Request::builder()
            .method(req.method().as_ref())
            .uri(path_and_query(&req)?);
        for (name, value) in req.headers().entries() {
            builder = builder.header(name, value);
        }
        let body = req.bytes().await.unwrap_or_default();
        let request = builder
            .body(axum::body::Body::from(body))
            .map_err(|e| Error::RustError(e.to_string()))?;
        let response = self
            .router
            .clone()
            .call(request)
            .await
            .map_err(|e| Error::RustError(format!("{e:?}")))?;
        worker_response(response).await
    }
}

fn path_and_query(req: &Request) -> Result<http::Uri> {
    let url = req.url()?;
    let target = match url.query() {
        Some(query) => format!("{}?{query}", url.path()),
        None => url.path().to_owned(),
    };
    target
        .parse()
        .map_err(|e: http::uri::InvalidUri| Error::RustError(e.to_string()))
}

fn http_headers(req: &Request) -> Result<http::HeaderMap> {
    let mut headers = http::HeaderMap::new();
    for (name, value) in req.headers().entries() {
        if let (Ok(name), Ok(value)) = (
            http::HeaderName::from_bytes(name.as_bytes()),
            http::HeaderValue::from_str(&value),
        ) {
            headers.append(name, value);
        }
    }
    Ok(headers)
}

async fn worker_response(response: axum::response::Response) -> Result<Response> {
    let (parts, body) = response.into_parts();
    let bytes = axum::body::to_bytes(body, usize::MAX)
        .await
        .map_err(|e| Error::RustError(e.to_string()))?;
    let headers = Headers::new();
    for (name, value) in &parts.headers {
        headers.append(name.as_str(), value.to_str().unwrap_or_default())?;
    }
    let response = if bytes.is_empty() {
        Response::empty()?
    } else {
        Response::from_bytes(bytes.to_vec())?
    };
    Ok(response
        .with_status(parts.status.as_u16())
        .with_headers(headers))
}
