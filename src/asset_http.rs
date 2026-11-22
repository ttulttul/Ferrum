mod compression;
pub(crate) mod grants;
mod image;
mod response;
use crate::AppState;
use crate::{asset_grant, asset_grant::encode_component, config::ServerPaths, rpc_protocol::Reply};
use axum::{
    body::Body,
    extract::{Path, State},
    http::{header, HeaderMap, Method, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Router,
};
use futures_util::StreamExt;
use grants::{attachment_path, workspace_image};
pub(crate) use grants::{delete_pending, file_extension, upload_url};
use image::image_dimensions;
use response::valid_mime;
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::{
    io,
    path::{Path as FsPath, PathBuf},
    sync::Arc,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

fn text(status: StatusCode, message: impl Into<String>) -> Response {
    (
        status,
        [
            (header::CONTENT_TYPE, "text/plain"),
            (header::VARY, "Accept-Encoding"),
        ],
        message.into(),
    )
        .into_response()
}

async fn not_found() -> Response {
    text(StatusCode::NOT_FOUND, "Not Found")
}

fn verify(state: &AppState, token: &str) -> Option<Value> {
    let key: [u8; 32] = std::fs::read(state.paths.secrets.join("asset-access-signing-key.bin"))
        .ok()?
        .try_into()
        .ok()?;
    asset_grant::verify(&key, token)
}

pub(crate) fn create_url(state: &AppState, params: &Value) -> Reply {
    match grants::prepare(&state.paths, params) {
        Ok(grant) => grant.issue(&params["resource"], |s| {
            crate::rpc::sign_asset_payload(state, s)
        }),
        Err(error) => Reply::Failure(error),
    }
}

async fn upload(
    State(state): State<Arc<AppState>>,
    Path(token): Path<String>,
    headers: HeaderMap,
    body: Body,
) -> Response {
    let Some(claims) = verify(&state, &token) else {
        return not_found().await;
    };
    let Some(id) = claims["attachmentId"].as_str() else {
        return not_found().await;
    };
    let Some(expected) = claims["sizeBytes"].as_u64() else {
        return not_found().await;
    };
    if claims["kind"] != "attachment-upload"
        || !id.starts_with("pending-")
        || id.contains(['/', '\\', '.', '\0'])
    {
        return not_found().await;
    }
    if headers
        .get(header::CONTENT_LENGTH)
        .is_some_and(|h| h.to_str().ok().and_then(|v| v.parse().ok()) != Some(expected))
    {
        return text(
            StatusCode::BAD_REQUEST,
            "Content-Length must match the upload size.",
        );
    }
    let extension = if claims["type"] == "file" {
        file_extension(claims["name"].as_str().unwrap_or(""))
    } else {
        match claims["mimeType"].as_str() {
            Some("image/png") => "png",
            Some("image/jpeg") => "jpg",
            Some("image/gif") => "gif",
            Some("image/webp") => "webp",
            _ => "bin",
        }
        .into()
    };
    let path = state.paths.attachments.join(format!("{id}.{extension}"));
    store_upload(&path, body, expected)
        .await
        .unwrap_or_else(|_| {
            text(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Failed to persist upload.",
            )
        })
}

async fn store_upload(path: &FsPath, body: Body, expected: u64) -> io::Result<Response> {
    let directory = path.parent().expect("attachment path has a parent");
    tokio::fs::create_dir_all(directory).await?;
    let (file, part) = tempfile::Builder::new()
        .suffix(".part")
        .tempfile_in(directory)?
        .into_parts();
    let mut file = tokio::fs::File::from_std(file);
    let mut received = 0;
    let mut digest = Sha256::new();
    let mut stream = body.into_data_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(io::Error::other)?;
        received += chunk.len() as u64;
        if received > expected {
            break;
        }
        digest.update(&chunk);
        file.write_all(&chunk).await?;
    }
    if received != expected {
        return Ok(text(
            StatusCode::BAD_REQUEST,
            format!("Body was {received} bytes, expected {expected}."),
        ));
    }
    file.flush().await?;
    drop(file);
    // No reader can see a partial upload. Racing identical retries may both succeed.
    if let Err(error) = part.persist_noclobber(path) {
        if error.error.kind() != io::ErrorKind::AlreadyExists {
            return Err(error.error);
        }
        let mut existing = tokio::fs::File::open(path).await?;
        let mut existing_digest = Sha256::new();
        let mut buf = vec![0; 64 * 1024];
        loop {
            let n = existing.read(&mut buf).await?;
            if n == 0 {
                break;
            }
            existing_digest.update(&buf[..n]);
        }
        if existing_digest.finalize() != digest.finalize() {
            return Ok(text(StatusCode::CONFLICT, "Attachment already uploaded."));
        }
    }
    Ok(StatusCode::NO_CONTENT.into_response())
}

async fn asset(
    State(state): State<Arc<AppState>>,
    Path(path): Path<String>,
    method: Method,
    headers: HeaderMap,
) -> Response {
    let Some((token, filename)) = path.split_once('/') else {
        return not_found().await;
    };
    let Some(claims) = verify(&state, token) else {
        return not_found().await;
    };
    serve(&state.paths, &claims, filename, &method, &headers).await
}

pub(crate) async fn serve(
    paths: &ServerPaths,
    claims: &Value,
    filename: &str,
    method: &Method,
    headers: &HeaderMap,
) -> Response {
    let file = match claims["kind"].as_str() {
        Some("attachment") => claims["attachmentId"]
            .as_str()
            .and_then(|id| attachment_path(paths, id)),
        Some("workspace-file-exact") => claims["relativePath"]
            .as_str()
            .map(FsPath::new)
            .filter(|p| p.file_name().and_then(|n| n.to_str()) == Some(filename))
            .and_then(|p| {
                workspace_image(paths, FsPath::new(claims["workspaceRoot"].as_str()?), p)
            }),
        Some("media-file" | "workspace-file" | "draft-workspace-file") => claims["filePath"]
            .as_str()
            .map(PathBuf::from)
            .filter(|p| p.is_file()),
        _ => None,
    };
    let Some(file) = file else {
        return not_found().await;
    };
    response::serve_file(&file, claims, method, headers)
        .await
        .unwrap_or_else(|_| text(StatusCode::INTERNAL_SERVER_ERROR, "Internal Server Error"))
}

pub fn routes() -> Router<Arc<AppState>> {
    Router::new()
        .route("/api/attachments/upload/", post(not_found))
        .route("/api/attachments/upload/*token", post(upload))
        .route("/api/assets/", get(not_found))
        .route("/api/assets/*path", get(asset))
}

/// The file of the attachment `id` in the attachments directory, if any.
pub(crate) fn attachment_file(state: &AppState, id: &str) -> Option<std::path::PathBuf> {
    grants::attachment_path(&state.paths, id)
}
