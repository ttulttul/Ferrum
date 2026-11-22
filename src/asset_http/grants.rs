use super::{image_dimensions, AppState};
use crate::rpc_protocol::Reply;
use crate::{asset_grant::AssetGrant, config::ServerPaths};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use regex::Regex;
use serde_json::{json, Value};
use std::{
    path::{Path, PathBuf},
    sync::LazyLock,
};

pub(crate) fn file_extension(name: &str) -> String {
    Path::new(name)
        .extension()
        .and_then(|e| e.to_str())
        .map(str::to_ascii_lowercase)
        .filter(|e| {
            !e.is_empty()
                && e != "part"
                && e.len() <= 10
                && e.bytes().all(|c| c.is_ascii_alphanumeric())
        })
        .unwrap_or_else(|| "bin".into())
}

fn upload_string<'a>(params: &'a Value, field: &str, max_len: usize) -> Result<&'a str, String> {
    let raw = params
        .get(field)
        .ok_or("Missing key")?
        .as_str()
        .ok_or("Expected string")?;
    let text = raw.trim();
    if text.is_empty() {
        return Err("Expected a non-blank string".into());
    }
    if text.encode_utf16().count() > max_len {
        return Err(format!(
            "Expected a value with a length of at most {max_len}"
        ));
    }
    Ok(text)
}

fn upload_size(params: &Value, limit: u64) -> Result<u64, String> {
    let size = params
        .get("sizeBytes")
        .ok_or("Missing key")?
        .as_f64()
        .ok_or("Expected number")?;
    if size.fract() != 0.0 || size.abs() > 9_007_199_254_740_991. {
        return Err("Expected an integer".into());
    }
    if size < 1. {
        return Err(format!(
            "Expected a value greater than or equal to {}",
            if size < 0. { 0 } else { 1 }
        ));
    }
    if size > limit as f64 {
        return Err(format!("Expected a value less than or equal to {limit}"));
    }
    Ok(size as u64)
}

pub(crate) fn upload_url(state: &AppState, params: &Value) -> Reply {
    let file = params["type"] == "file";
    let ty = if file { "file" } else { "image" };
    if !file && params.get("type").is_some_and(|t| t != "image") {
        return Reply::defect("Expected \"image\"\n  at [\"type\"]".into());
    }
    let defect = |field: &str, reason: String| {
        let prefix = if file {
            "Expected \"image\"\n  at [\"type\"]\n"
        } else {
            ""
        };
        Reply::defect(format!("{prefix}{reason}\n  at [\"{field}\"]"))
    };
    let mut name = match upload_string(params, "name", 255) {
        Ok(name) => name.to_owned(),
        Err(e) => return defect("name", e),
    };
    let mime =
        if file {
            match upload_string(params, "mimeType", 100) {
                Ok(mime) => mime,
                Err(e) => return defect("mimeType", e),
            }
        } else {
            match params.get("mimeType") {
                None => return defect("mimeType", "Missing key".into()),
                Some(v) => match v.as_str() {
                    Some(mime @ ("image/gif" | "image/jpeg" | "image/png" | "image/webp")) => mime,
                    _ => return defect(
                        "mimeType",
                        "Expected \"image/gif\" | \"image/jpeg\" | \"image/png\" | \"image/webp\""
                            .into(),
                    ),
                },
            }
        };
    let size = match upload_size(params, if file { 50 << 20 } else { 10 << 20 }) {
        Ok(size) => size,
        Err(e) => return defect("sizeBytes", e),
    };
    let mut id = format!("pending-{}", uuid::Uuid::new_v4());
    if file {
        if mime
            .split(';')
            .next()
            .unwrap_or("")
            .trim()
            .eq_ignore_ascii_case("image/svg+xml")
            && !name.to_ascii_lowercase().ends_with(".svg")
        {
            name.push_str(".svg");
        }
        id.push('-');
        id.push_str(&file_extension(&name));
    }
    let now = crate::platform::now_ms();
    sweep_pending(state, now);
    let expires = now + 10 * 60 * 1000;
    let claims = json!({"version":1,"kind":"attachment-upload","type":ty,"attachmentId":id,"name":name,"mimeType":mime,"sizeBytes":size,"expiresAt":expires});
    let encoded = URL_SAFE_NO_PAD.encode(crate::wire_json::compact(&claims));
    match crate::rpc::sign_asset_payload(state, &encoded) {
        Ok(signature) => Reply::Success(
            json!({"attachmentId":id,"relativeUrl":format!("/api/attachments/upload/{encoded}.{signature}"),"expiresAt":expires}),
        ),
        Err(e) => Reply::Failure(
            json!({"_tag":"Fail","error":{"_tag":"AttachmentUploadSigningKeyError","cause":{"name":"Error","message":e.to_string()}}}),
        ),
    }
}

static ID: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
    r"(?i-u)^([a-z0-9_]+(?:-[a-z0-9_]+)*)-[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}(?:-([a-z0-9]{1,10}))?$"
).unwrap()
});

fn id_extension(id: &str) -> Option<String> {
    Some(ID.captures(id)?.get(2)?.as_str().to_ascii_lowercase())
}

fn pending_id(id: &str) -> bool {
    ID.captures(id)
        .is_some_and(|c| c[1].eq_ignore_ascii_case("pending"))
}

pub(crate) async fn delete_pending(state: &AppState, id: &str) {
    if !pending_id(id) {
        return;
    }
    if let Some(path) = attachment_path(&state.paths, id) {
        let mut sidecar = path.as_os_str().to_owned();
        sidecar.push(".txt");
        let _ = tokio::fs::remove_file(path).await;
        let _ = tokio::fs::remove_file(sidecar).await;
    }
}

fn sweep_pending(state: &AppState, now: i64) {
    use std::sync::atomic::Ordering;
    use std::time::UNIX_EPOCH;
    const HOUR: i64 = 3_600_000;
    if state
        .attachment_sweep_ms
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |last| {
            (last == 0 || now - last >= HOUR / 4).then_some(now)
        })
        .is_err()
    {
        return;
    }
    let Ok(entries) = std::fs::read_dir(&state.paths.attachments) else {
        return;
    };
    for entry in entries.flatten() {
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        let age = if name.ends_with(".part") {
            HOUR
        } else {
            let stem = name
                .strip_suffix(".txt")
                .filter(|s| s.contains('.'))
                .unwrap_or(name);
            if !stem.rsplit_once('.').is_some_and(|(id, _)| pending_id(id)) {
                continue;
            }
            24 * HOUR
        };
        if entry
            .metadata()
            .and_then(|m| m.modified())
            .ok()
            .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
            .is_some_and(|t| now - t.as_millis() as i64 > age)
        {
            let _ = std::fs::remove_file(entry.path());
        }
    }
}

pub(super) fn attachment_path(paths: &ServerPaths, id: &str) -> Option<PathBuf> {
    if id.is_empty() || id.contains(['/', '\\', '.', '\0']) {
        return None;
    }
    let candidate = |ext: &str| paths.attachments.join(format!("{id}.{ext}"));
    let stored_file = |p: &Path| p.symlink_metadata().is_ok_and(|m| m.is_file());
    if let Some(ext) = id_extension(id) {
        let path = candidate(&ext);
        return stored_file(&path).then_some(path);
    }
    [
        "avif", "bmp", "gif", "heic", "heif", "ico", "jpeg", "jpg", "png", "svg", "tiff", "webp",
        "bin",
    ]
    .iter()
    .map(|ext| candidate(ext))
    .find(|p| stored_file(p))
}

// hostPreviewMimeTypeFromExtension in the pinned client. Selection for an inline grant
// depends on the stored extension, independent of the supplied filename and MIME type.
fn inline_mime(ext: &str) -> Option<&'static str> {
    Some(match ext {
        "avif" => "image/avif",
        "bmp" => "image/bmp",
        "gif" => "image/gif",
        "heic" => "image/heic",
        "heif" => "image/heif",
        "ico" => "image/x-icon",
        "jpg" | "jpeg" => "image/jpeg",
        "png" => "image/png",
        "svg" => "image/svg+xml",
        "tif" | "tiff" => "image/tiff",
        "webp" => "image/webp",
        "avi" => "video/x-msvideo",
        "mp4" | "m4v" => "video/mp4",
        "mkv" => "video/x-matroska",
        "mov" => "video/quicktime",
        "ogv" => "video/ogg",
        "webm" => "video/webm",
        "mp3" => "audio/mpeg",
        "wav" => "audio/wav",
        "ogg" | "oga" | "opus" => "audio/ogg",
        "flac" => "audio/flac",
        "aac" => "audio/aac",
        "m4a" => "audio/mp4",
        "aiff" => "audio/aiff",
        "weba" => "audio/webm",
        "pdf" => "application/pdf",
        "htm" | "html" => "text/html",
        "ttf" => "font/ttf",
        "otf" => "font/otf",
        "woff" => "font/woff",
        "woff2" => "font/woff2",
        "zip" => "application/zip",
        "jar" => "application/java-archive",
        "tar" => "application/x-tar",
        "gz" | "tgz" => "application/gzip",
        "sqlite" | "sqlite3" | "db" => "application/vnd.sqlite3",
        "docx" => "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        "xlsx" => "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        "pptx" => "application/vnd.openxmlformats-officedocument.presentationml.presentation",
        "odt" => "application/vnd.oasis.opendocument.text",
        "ods" => "application/vnd.oasis.opendocument.spreadsheet",
        "odp" => "application/vnd.oasis.opendocument.presentation",
        "rtf" => "application/rtf",
        "epub" => "application/epub+zip",
        "ipynb" => "application/x-ipynb+json",
        _ => return None,
    })
}

pub(crate) fn prepare(paths: &ServerPaths, params: &Value) -> Result<AssetGrant, Value> {
    let Some(resource) = params.get("resource").and_then(Value::as_object) else {
        return Err(defect(crate::control::ASSET_RESOURCE_DEFECT));
    };
    let tag = resource
        .get("_tag")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let path = resource
        .get("path")
        .and_then(Value::as_str)
        .unwrap_or_default();
    if !matches!(
        tag,
        "workspace-file"
            | "media-file"
            | "draft-workspace-file"
            | "attachment"
            | "tool-output-image"
            | "project-favicon"
            | "native-app-icon"
            | "environment-theme-background"
            | "github-media"
    ) || matches!(
        tag,
        "media-file" | "workspace-file" | "draft-workspace-file"
    ) && path.is_empty()
    {
        return Err(defect(crate::control::ASSET_RESOURCE_DEFECT));
    }
    let file_path = match tag {
        "draft-workspace-file" => resource
            .get("cwd")
            .and_then(Value::as_str)
            .map(|cwd| crate::rpc::expand_user_path(cwd).join(path)),
        "media-file" | "workspace-file" => Some(crate::rpc::expand_user_path(path)),
        _ => None,
    };
    let expires = crate::platform::now_ms() as u64 + 3_600_000;
    if tag == "attachment" {
        return attachment_grant(paths, &params["resource"], expires);
    }
    if tag == "draft-workspace-file"
        && Path::new(path).is_relative()
        && is_draft_image(Path::new(path))
    {
        return draft_image_grant(paths, &params["resource"], expires);
    }
    let claims = json!({"version":1,"kind":tag,"filePath":file_path.as_ref().map(|p|p.to_string_lossy().into_owned()).unwrap_or_default(),"expiresAt":expires});
    let name = Path::new(path)
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("asset");
    let revision = file_path
        .as_ref()
        .and_then(|p| std::fs::metadata(p).ok())
        .map(|m| {
            format!(
                "{}:{}",
                m.len(),
                m.modified()
                    .ok()
                    .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                    .map(|t| t.as_nanos())
                    .unwrap_or(0)
            )
        });
    Ok(AssetGrant {
        claims,
        name: name.to_owned(),
        source_path: file_path,
        revision,
        dimensions: None,
    })
}

fn asset_error(tag: &str, resource: &Value) -> Value {
    json!({"_tag":"Fail","error":{"_tag":tag,"resource":resource}})
}

fn defect(message: &str) -> Value {
    json!({"_tag":"Die","defect":message})
}

fn attachment_grant(
    paths: &ServerPaths,
    resource: &Value,
    mut expires: u64,
) -> Result<AssetGrant, Value> {
    let Some(id) = resource["attachmentId"].as_str().filter(|s| !s.is_empty()) else {
        return Err(defect("Missing key\n  at [\"resource\"][\"attachmentId\"]"));
    };
    let Some(path) = attachment_path(paths, id) else {
        return Err(asset_error("AssetAttachmentNotFoundError", resource));
    };
    let ext = id_extension(id);
    let supplied = resource["mimeType"].as_str();
    let video = supplied
        .and_then(|s| s.split(';').next())
        .map(str::trim)
        .filter(|s| super::valid_mime(s) && s.to_ascii_lowercase().starts_with("video/"));
    let inline = ext
        .as_deref()
        .filter(|_| resource["disposition"] == "inline")
        .and_then(inline_mime);
    let mut claims = json!({"version":1,"kind":"attachment","attachmentId":id});
    if ext.is_some() && inline.is_none() && video.is_none() {
        claims["download"] = json!(true);
    }
    if let Some(name) = resource["fileName"].as_str() {
        claims["fileName"] = json!(name);
    }
    if let Some(mime) = inline.or(video).or(supplied) {
        claims["mimeType"] = json!(mime);
        if mime == "text/html" {
            const BUCKET: u64 = 30 * 60 * 1000;
            expires = expires.div_ceil(BUCKET) * BUCKET;
        }
    }
    if inline == Some("text/html") && resource["renderIntent"] == "mcp-app" {
        claims["renderIntent"] = json!("mcp-app");
    }
    claims["expiresAt"] = json!(expires);
    let name = resource["fileName"]
        .as_str()
        .unwrap_or_else(|| path.file_name().unwrap().to_str().unwrap_or(id));
    let dimensions = if ext.is_none() {
        image_dimensions(&path)
    } else {
        None
    };
    Ok(AssetGrant {
        claims,
        name: name.into(),
        dimensions,
        source_path: None,
        revision: None,
    })
}

pub(crate) fn is_draft_image(path: &Path) -> bool {
    path.extension()
        .and_then(|e| e.to_str())
        .and_then(|e| inline_mime(&e.to_ascii_lowercase()))
        .is_some_and(|m| m.starts_with("image/"))
}

pub(super) fn workspace_image(
    paths: &ServerPaths,
    root: &Path,
    relative: &Path,
) -> Option<PathBuf> {
    let root = root.canonicalize().ok()?;
    let file = root.join(relative).canonicalize().ok()?;
    let secrets = paths
        .secrets
        .canonicalize()
        .unwrap_or_else(|_| paths.secrets.clone());
    let state_dir = paths
        .state_dir
        .canonicalize()
        .unwrap_or_else(|_| paths.state_dir.clone());
    let database = file.parent() == Some(&state_dir)
        && file
            .file_name()
            .is_some_and(|n| n.to_string_lossy().contains(".sqlite"));
    (file.starts_with(root)
        && !file.starts_with(secrets)
        && !file.starts_with("/dev/fd")
        && !database
        && file.is_file())
    .then_some(file)
}

fn draft_image_grant(
    paths: &ServerPaths,
    resource: &Value,
    expires: u64,
) -> Result<AssetGrant, Value> {
    let cwd = crate::rpc::expand_user_path(resource["cwd"].as_str().unwrap_or(""));
    let relative = Path::new(resource["path"].as_str().unwrap_or(""));
    let Some(file) = workspace_image(paths, &cwd, relative) else {
        return Err(asset_error("AssetWorkspaceAssetNotFoundError", resource));
    };
    let Ok(root) = cwd.canonicalize() else {
        return Err(asset_error("AssetWorkspaceAssetNotFoundError", resource));
    };
    let claims = json!({"version":1,"kind":"workspace-file-exact","workspaceRoot":root,"relativePath":relative,"expiresAt":expires});
    Ok(AssetGrant {
        claims,
        name: relative
            .file_name()
            .and_then(|s| s.to_str())
            .unwrap_or("asset")
            .into(),
        dimensions: image_dimensions(&file),
        source_path: None,
        revision: None,
    })
}
