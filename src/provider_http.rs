use crate::{
    pull_requests,
    voice::{self, SpeechError},
    AppState,
};
use axum::{
    body::Bytes,
    extract::{DefaultBodyLimit, Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use serde::Deserialize;
use serde_json::{json, Value};
use std::sync::Arc;
use uuid::Uuid;

pub fn routes() -> Router<Arc<AppState>> {
    Router::new()
        .route("/api/voice/status", get(local_status))
        .route("/api/voice/models", get(local_models))
        .route("/api/voice/models/prepare", post(prepare_model))
        .route("/api/voice/models/download", post(download_model))
        .route("/api/voice/models/select", post(select_model))
        .route("/api/voice/models/cancel", post(cancel_model))
        .route("/api/voice/models/remove", post(remove_model))
        .route("/api/voice/models/link", post(link_models))
        .route("/api/voice/acceleration", post(update_acceleration))
        .route("/api/voice/model-unload-timeout", post(update_timeout))
        .route("/api/voice/transcribe", post(local_transcribe))
        .route("/api/voice/cloud/keys", get(keys))
        .route("/api/voice/cloud/:provider/key", post(set_key))
        .route("/api/voice/cloud/openrouter/models", get(models))
        .route(
            "/api/voice/cloud/:provider/transcribe",
            post(transcribe).layer(DefaultBodyLimit::max(
                voice::MAX_PCM + voice::MAX_OPTIONS + 4,
            )),
        )
        .route("/api/voice/post-process", post(post_process))
        .route("/api/pull-requests/diff", post(diff))
}

async fn local_status(State(s): State<Arc<AppState>>, h: HeaderMap) -> Response {
    response(&h, s.voice.local_status())
}

#[derive(Deserialize)]
struct Benchmarks {
    benchmarks: Option<String>,
}

async fn local_models(
    State(s): State<Arc<AppState>>,
    h: HeaderMap,
    Query(query): Query<Benchmarks>,
) -> Response {
    response(
        &h,
        s.voice
            .local_models(query.benchmarks.as_deref() == Some("1")),
    )
}

async fn prepare_model(State(s): State<Arc<AppState>>, h: HeaderMap, body: Bytes) -> Response {
    if !body.is_empty() {
        return response(&h, Err(SpeechError::Invalid("invalid_command")));
    }
    match s.voice.prepare_local() {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(error) => response(&h, Err(error)),
    }
}

async fn download_model(
    State(s): State<Arc<AppState>>,
    h: HeaderMap,
    Json(input): Json<Value>,
) -> Response {
    response(&h, s.voice.local_update("download", &input))
}

async fn cancel_model(
    State(s): State<Arc<AppState>>,
    h: HeaderMap,
    Json(input): Json<Value>,
) -> Response {
    response(&h, s.voice.local_update("cancel", &input))
}

async fn remove_model(
    State(s): State<Arc<AppState>>,
    h: HeaderMap,
    Json(input): Json<Value>,
) -> Response {
    response(&h, s.voice.local_update("remove", &input))
}

async fn link_models(
    State(s): State<Arc<AppState>>,
    h: HeaderMap,
    Json(input): Json<Value>,
) -> Response {
    response(&h, s.voice.local_update("link", &input))
}

async fn select_model(
    State(s): State<Arc<AppState>>,
    h: HeaderMap,
    Json(input): Json<Value>,
) -> Response {
    response(&h, s.voice.local_update("select", &input))
}

async fn update_acceleration(
    State(s): State<Arc<AppState>>,
    h: HeaderMap,
    Json(input): Json<Value>,
) -> Response {
    match s.voice.local_update("acceleration", &input) {
        Ok(_) => response(&h, s.voice.local_status()),
        Err(error) => response(&h, Err(error)),
    }
}

async fn update_timeout(
    State(s): State<Arc<AppState>>,
    h: HeaderMap,
    Json(input): Json<Value>,
) -> Response {
    match s.voice.local_update("timeout", &input) {
        Ok(_) => response(&h, s.voice.local_status()),
        Err(error) => response(&h, Err(error)),
    }
}

async fn local_transcribe(State(_s): State<Arc<AppState>>, h: HeaderMap, body: Bytes) -> Response {
    if h.get("content-type")
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.starts_with("application/json"))
    {
        return (
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "Unsupported content-type: application/json",
        )
            .into_response();
    }
    let _ = body;
    response(&h, Err(SpeechError::Invalid("speech_unavailable")))
}

fn response(headers: &HeaderMap, result: Result<Value, SpeechError>) -> Response {
    let (status, tag, code, reason) = match result {
        Ok(value) => return Json(value).into_response(),
        Err(SpeechError::Cloud(provider, reason)) => {
            return (
                StatusCode::UNPROCESSABLE_ENTITY,
                Json(json!({"_tag":"CloudSpeechError","provider":provider,"reason":reason})),
            )
                .into_response()
        }
        Err(SpeechError::Invalid(reason)) => (
            StatusCode::BAD_REQUEST,
            "EnvironmentRequestInvalidError",
            "invalid_request",
            reason,
        ),
        Err(SpeechError::Internal) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            "EnvironmentInternalError",
            "internal_error",
            "internal_error",
        ),
    };
    let trace = headers
        .get("traceparent")
        .and_then(|v| v.to_str().ok())
        .and_then(|p| p.split('-').nth(1))
        .filter(|id| {
            id.len() == 32
                && id.bytes().all(|c| c.is_ascii_hexdigit())
                && id.bytes().any(|c| c != b'0')
        })
        .map(str::to_owned)
        .unwrap_or_else(|| Uuid::new_v4().simple().to_string());
    (
        status,
        Json(json!({"_tag":tag,"code":code,"reason":reason,"traceId":trace})),
    )
        .into_response()
}
async fn keys(State(s): State<Arc<AppState>>, h: HeaderMap) -> Response {
    response(&h, s.voice.cloud_keys().await)
}
async fn set_key(
    State(s): State<Arc<AppState>>,
    h: HeaderMap,
    Path(kind): Path<String>,
    Json(input): Json<Value>,
) -> Response {
    let result = async {
        let kind = voice::provider(&kind)?;
        let key = input
            .get("key")
            .ok_or(SpeechError::Invalid("invalid_command"))?;
        s.voice.set_key(kind, key).await
    }
    .await;
    match result {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(e) => response(&h, Err(e)),
    }
}
async fn models(State(s): State<Arc<AppState>>, h: HeaderMap) -> Response {
    response(&h, s.voice.cloud_models().await)
}
#[derive(Deserialize)]
struct ModelQuery {
    model: Option<String>,
}
async fn transcribe(
    State(s): State<Arc<AppState>>,
    h: HeaderMap,
    Path(kind): Path<String>,
    Query(query): Query<ModelQuery>,
    data: Result<Bytes, axum::extract::rejection::BytesRejection>,
) -> Response {
    response(
        &h,
        async {
            let kind = voice::provider(&kind)?;
            let data = data.map_err(|_| SpeechError::Invalid("invalid_audio"))?;
            s.voice
                .transcribe(kind, query.model.as_deref(), &data)
                .await
        }
        .await,
    )
}
async fn post_process(
    State(s): State<Arc<AppState>>,
    h: HeaderMap,
    Json(input): Json<Value>,
) -> Response {
    response(&h, voice::post_process(&input, &s.provider_command).await)
}
async fn diff(State(s): State<Arc<AppState>>, Json(input): Json<Value>) -> Response {
    if input["projectId"].as_str().is_none()
        || input["repository"].as_str().is_none()
        || input["number"].as_u64().is_none()
    {
        return StatusCode::BAD_REQUEST.into_response();
    }
    let cli = s.issues.lock().await.gh.clone();
    match pull_requests::read(&s, &cli, "diff", &input).await {
        Ok(v) => Json(v).into_response(),
        Err(e) => (StatusCode::SERVICE_UNAVAILABLE, Json(e)).into_response(),
    }
}
