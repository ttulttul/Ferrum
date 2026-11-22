//! The plane owns the grant. Agentd resolves the file and produces the same
//! HTTP response as the native router, including ranges and preview headers.
use super::{ControlPlane, STATE_DIR};
use crate::{
    asset_grant::{self, AssetGrant},
    mcp_oauth::on_this_thread,
    persistence::WriteMode,
    rpc_protocol::Reply,
    workspace_protocol::{self as proto, op},
};
use axum::{
    body::Body,
    extract::{Path, State},
    http::{header, HeaderMap, Method, StatusCode},
    response::{IntoResponse, Response},
    routing::get,
    Router,
};
use base64::{
    engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD},
    Engine,
};
use serde_json::{json, Value};
use std::{io, sync::Arc};

impl ControlPlane {
    fn asset_key(&self, create: bool) -> io::Result<[u8; 32]> {
        let mut cached = self.asset_key.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(key) = *cached {
            return Ok(key);
        }
        let dir = std::path::Path::new(STATE_DIR).join("secrets");
        let path = dir.join("asset-access-signing-key.bin");
        let key = match self.documents.read_optional(&path)? {
            Some(stored) => match stored.len() {
                32 => stored.try_into().ok(),
                _ => URL_SAFE_NO_PAD
                    .decode(stored.trim_ascii())
                    .ok()
                    .and_then(|v| v.try_into().ok()),
            }
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "invalid asset signing key")
            })?,
            None if create => {
                let key = crate::platform::random_bytes::<32>();
                self.documents.create_private_dir_all(&dir)?;
                self.documents.write(
                    &path,
                    URL_SAFE_NO_PAD.encode(key).as_bytes(),
                    WriteMode::Private,
                )?;
                key
            }
            None => return Err(crate::persistence::not_found(&path)),
        };
        *cached = Some(key);
        Ok(key)
    }

    pub(super) async fn create_asset_url(&self, params: &Value) -> Reply {
        let mut input = params.clone();
        let resource = &mut input["resource"];
        // A relative path in a thread belongs to its worktree (or project).
        if matches!(
            resource["_tag"].as_str(),
            Some("media-file" | "workspace-file")
        ) {
            if let Some(path) = resource["path"]
                .as_str()
                .filter(|p| !p.is_empty() && std::path::Path::new(p).is_relative())
            {
                let store = self.store_now();
                let Some(thread) = resource["threadId"]
                    .as_str()
                    .and_then(|id| store.threads.get(id))
                else {
                    return Reply::Failure(
                        json!({"_tag":"Fail","error":{"_tag":"AssetWorkspaceContextNotFoundError","resource":resource}}),
                    );
                };
                let root = thread.row["worktreePath"].as_str().unwrap_or(&thread.cwd);
                resource["path"] = json!(std::path::Path::new(root).join(path));
            }
        }
        let result = self.workspace().request(op::ASSET_PREPARE, input).await;
        match proto::decode_reply(result, "assets.createUrl") {
            Ok(value) => match serde_json::from_value::<AssetGrant>(value) {
                Ok(grant) => grant.issue(&params["resource"], |s| {
                    Ok(asset_grant::signature(&self.asset_key(true)?, s))
                }),
                Err(_) => Reply::defect("Invalid asset grant from workspace.".into()),
            },
            Err(reason) => Reply::Failure(reason),
        }
    }
}

fn not_found() -> Response {
    (
        StatusCode::NOT_FOUND,
        [
            (header::CONTENT_TYPE, "text/plain"),
            (header::VARY, "Accept-Encoding"),
        ],
        "Not Found",
    )
        .into_response()
}

/// Dropping the HTTP response also releases its open file in agentd.
struct RemoteBody {
    plane: Arc<ControlPlane>,
    id: u64,
}
impl Drop for RemoteBody {
    fn drop(&mut self) {
        self.plane.workspace().close_asset(self.id);
    }
}

async fn asset(
    State(plane): State<Arc<ControlPlane>>,
    Path(path): Path<String>,
    method: Method,
    headers: HeaderMap,
) -> Response {
    let Some((token, name)) = path.split_once('/') else {
        return not_found();
    };
    let Some(claims) = plane
        .asset_key(false)
        .ok()
        .and_then(|key| asset_grant::verify(&key, token))
    else {
        return not_found();
    };
    let mut forwarded = serde_json::Map::new();
    for key in [header::ACCEPT_ENCODING, header::RANGE, header::IF_RANGE] {
        if let Some(value) = headers.get(&key).and_then(|h| h.to_str().ok()) {
            forwarded.insert(key.to_string(), json!(value));
        }
    }
    let args = json!({"claims":claims,"cwd":claims["workspaceRoot"],"name":name,"method":method.as_str(),"headers":forwarded});
    let Ok(opened) = on_this_thread(plane.workspace().request(op::ASSET_OPEN, args)).await else {
        return not_found();
    };
    let Some(status) = opened["status"]
        .as_u64()
        .and_then(|s| u16::try_from(s).ok())
        .and_then(|s| StatusCode::from_u16(s).ok())
    else {
        return StatusCode::BAD_GATEWAY.into_response();
    };
    let body = if let Some(id) = opened["body"].as_u64() {
        let stream = futures_util::stream::try_unfold(RemoteBody { plane, id }, |body| {
            on_this_thread(async move {
                let value = body
                    .plane
                    .workspace()
                    .request(op::ASSET_READ, json!({"body":body.id}))
                    .await
                    .map_err(io::Error::other)?;
                if value.is_null() {
                    return Ok::<_, io::Error>(None);
                }
                let bytes = STANDARD
                    .decode(
                        value
                            .as_str()
                            .ok_or_else(|| io::Error::other("invalid asset chunk"))?,
                    )
                    .map_err(io::Error::other)?;
                Ok(Some((bytes, body)))
            })
        });
        Body::from_stream(stream)
    } else {
        Body::empty()
    };
    let mut response = Response::builder().status(status);
    // Only asset response headers may cross the workspace boundary.
    for key in [
        header::ACCEPT_RANGES,
        header::CACHE_CONTROL,
        header::CONTENT_DISPOSITION,
        header::CONTENT_ENCODING,
        header::CONTENT_LENGTH,
        header::CONTENT_RANGE,
        header::CONTENT_SECURITY_POLICY,
        header::CONTENT_TYPE,
        header::ETAG,
        header::LAST_MODIFIED,
        header::VARY,
        header::X_CONTENT_TYPE_OPTIONS,
    ] {
        if let Some(v) = opened["headers"][key.as_str()].as_str() {
            response = response.header(key, v);
        }
    }
    response
        .body(body)
        .unwrap_or_else(|_| StatusCode::BAD_GATEWAY.into_response())
}

pub(super) fn routes() -> Router<Arc<ControlPlane>> {
    Router::new()
        .route("/api/assets/", get(|| async { not_found() }))
        .route("/api/assets/*path", get(asset))
}
