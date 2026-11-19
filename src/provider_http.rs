use crate::{pull_requests::PullRequestService, voice::{CloudProvider, VoiceService}, AppState};
use axum::{extract::Path, routing::{get, post}, Json, Router};
use serde_json::{json, Value};
use std::sync::Arc;

pub fn routes() -> Router<Arc<AppState>> {
    Router::new()
        .route("/api/voice/cloud/keys", get(cloud_keys))
        .route("/api/voice/cloud/openrouter/models", get(cloud_models))
        .route("/api/voice/cloud/:provider/transcribe", post(transcribe))
        .route("/api/voice/post-process", post(post_process))
        .route("/api/pull-requests/diff", post(pull_request_diff))
}

async fn cloud_keys() -> Json<Value> { Json(VoiceService::new().cloud_keys()) }
async fn cloud_models() -> Json<Value> { Json(VoiceService::new().cloud_models()) }
async fn transcribe(Path(provider): Path<String>) -> (axum::http::StatusCode, Json<Value>) {
    let provider = match provider.as_str() { "xai" => CloudProvider::Xai, "openrouter" => CloudProvider::Openrouter, _ => CloudProvider::Openai };
    (axum::http::StatusCode::UNPROCESSABLE_ENTITY, Json(VoiceService::new().transcribe_unavailable(provider)))
}
async fn post_process(Json(input): Json<Value>) -> Json<Value> {
    let text = input.get("text").and_then(Value::as_str).unwrap_or_default();
    let words = input.get("customWords").and_then(Value::as_array).map(|values| values.iter().filter_map(Value::as_str).map(str::to_owned).collect::<Vec<_>>()).unwrap_or_default();
    Json(json!({"text":VoiceService::new().post_process(text, &words)}))
}
async fn pull_request_diff(Json(input): Json<Value>) -> (axum::http::StatusCode, Json<Value>) {
    let service = PullRequestService;
    let Some(patch) = input.get("patch").and_then(Value::as_str) else { return (axum::http::StatusCode::SERVICE_UNAVAILABLE, Json(service.unavailable("cli-unauthenticated", None))); };
    let cursor = input.get("cursor").and_then(Value::as_str);
    let limit = input.get("limit").and_then(Value::as_u64).unwrap_or(64 * 1024) as usize;
    (axum::http::StatusCode::OK, Json(service.diff(patch, cursor, limit)))
}

