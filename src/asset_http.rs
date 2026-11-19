use crate::AppState;
use axum::{
    body::Bytes,
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Router,
};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use hmac::{Hmac, Mac};
use serde_json::Value;
use sha2::Sha256;
use std::{
    path::PathBuf,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

fn key(state: &AppState) -> anyhow::Result<[u8; 32]> {
    let path = state.paths.secrets.join("asset-access-signing-key.bin");
    let bytes = std::fs::read(path)?;
    bytes
        .try_into()
        .map_err(|_| anyhow::anyhow!("invalid asset signing key"))
}

fn verify(state: &AppState, encoded: &str, signature: &str) -> Option<Value> {
    let key = key(state).ok()?;
    let tag = URL_SAFE_NO_PAD.decode(signature).ok()?;
    let mut mac = Hmac::<Sha256>::new_from_slice(&key).ok()?;
    mac.update(encoded.as_bytes());
    mac.verify_slice(&tag).ok()?;
    let bytes = URL_SAFE_NO_PAD.decode(encoded).ok()?;
    let claims: Value = serde_json::from_slice(&bytes).ok()?;
    let expires = claims["expiresAt"].as_u64()?;
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()?
        .as_millis() as u64;
    (expires > now).then_some(claims)
}

fn attachment_path(state: &AppState, id: &str) -> Option<PathBuf> {
    if id.is_empty() || id.contains('/') || id.contains('\\') || id.contains('.') {
        return None;
    }
    let root = std::fs::canonicalize(&state.paths.attachments).ok()?;
    for extension in [
        ".avif", ".bmp", ".gif", ".heic", ".heif", ".ico", ".jpeg", ".jpg", ".png", ".svg",
        ".tiff", ".webp", ".bin",
    ] {
        let path = root.join(format!("{id}{extension}"));
        if path.is_file() {
            return Some(path);
        }
    }
    None
}

fn mime_for_path(path: &std::path::Path) -> &'static str {
    match path
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or_default()
        .to_ascii_lowercase()
        .as_str()
    {
        "png" => "image/png",
        "jpg" | "jpeg" => "image/jpeg",
        "gif" => "image/gif",
        "webp" => "image/webp",
        "svg" => "image/svg+xml",
        "html" | "htm" => "text/html; charset=utf-8",
        "pdf" => "application/pdf",
        _ => "application/octet-stream",
    }
}

async fn upload(
    State(state): State<Arc<AppState>>,
    Path(token): Path<String>,
    body: Bytes,
) -> Response {
    let Some((encoded, signature)) = token.split_once('.') else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let Some(claims) = verify(&state, encoded, signature) else {
        return StatusCode::NOT_FOUND.into_response();
    };
    if claims["kind"] != "attachment-upload"
        || !claims["attachmentId"]
            .as_str()
            .unwrap_or_default()
            .starts_with("pending-")
    {
        return StatusCode::NOT_FOUND.into_response();
    }
    let id = claims["attachmentId"].as_str().unwrap_or_default();
    let expected = claims["sizeBytes"].as_u64().unwrap_or(u64::MAX);
    if body.len() as u64 != expected {
        return (
            StatusCode::BAD_REQUEST,
            format!("Body was {} bytes, expected {expected}.", body.len()),
        )
            .into_response();
    }
    let name = claims["name"].as_str().unwrap_or("attachment");
    let ty = claims["type"].as_str().unwrap_or("image");
    let extension = if ty == "file" {
        std::path::Path::new(name)
            .extension()
            .and_then(|e| e.to_str())
            .map(|e| format!(".{}", e.to_ascii_lowercase()))
            .filter(|e| e.len() <= 11 && e[1..].chars().all(|c| c.is_ascii_alphanumeric()))
            .unwrap_or_else(|| ".bin".into())
    } else {
        match claims["mimeType"].as_str().unwrap_or_default() {
            "image/png" => ".png",
            "image/jpeg" => ".jpg",
            "image/gif" => ".gif",
            "image/webp" => ".webp",
            _ => ".bin",
        }
        .into()
    };
    let _ = tokio::fs::create_dir_all(&state.paths.attachments).await;
    let path = state.paths.attachments.join(format!("{id}{extension}"));
    match tokio::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&path)
        .await
    {
        Ok(mut file) => {
            use tokio::io::AsyncWriteExt;
            if file.write_all(&body).await.is_err() {
                let _ = tokio::fs::remove_file(&path).await;
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Failed to persist upload.",
                )
                    .into_response();
            }
        }
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            if tokio::fs::read(&path).await.ok().as_deref() != Some(body.as_ref()) {
                return (StatusCode::CONFLICT, "Attachment already uploaded.").into_response();
            }
        }
        Err(_) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Failed to persist upload.",
            )
                .into_response()
        }
    }
    StatusCode::NO_CONTENT.into_response()
}

async fn asset(State(state): State<Arc<AppState>>, Path(path): Path<String>) -> Response {
    let Some((token, filename)) = path.split_once('/') else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let Some((encoded, signature)) = token.split_once('.') else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let Some(claims) = verify(&state, encoded, signature) else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let file = if claims["kind"] == "attachment" {
        attachment_path(&state, claims["attachmentId"].as_str().unwrap_or_default())
    } else {
        claims["filePath"]
            .as_str()
            .map(PathBuf::from)
            .filter(|p| p.is_file())
    };
    let Some(file) = file else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let Ok(bytes) = tokio::fs::read(&file).await else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let mime = claims["mimeType"]
        .as_str()
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| mime_for_path(&file).to_owned());
    let mut headers = HeaderMap::new();
    headers.insert("cache-control", "private, max-age=3600".parse().unwrap());
    headers.insert("x-content-type-options", "nosniff".parse().unwrap());
    headers.insert(
        "content-type",
        mime.parse()
            .unwrap_or_else(|_| "application/octet-stream".parse().unwrap()),
    );
    let _ = filename;
    (StatusCode::OK, headers, bytes).into_response()
}

pub fn routes() -> Router<Arc<AppState>> {
    Router::new()
        .route("/api/attachments/upload/*token", post(upload))
        .route("/api/assets/*path", get(asset))
}
