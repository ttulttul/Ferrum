//! Pending uploads claimed for the thread a message goes to, as the
//! reference's `AttachmentClaims.claimPendingAttachments` claims them: each
//! `pending-<uuid>` upload is checked against what the message says it is and
//! copied to the thread's own attachment id. The pending copy stays for a
//! retry, and the pending sweep reclaims it.
use serde_json::{json, Value};
use std::path::{Path, PathBuf};

/// `PROVIDER_SEND_TURN_MAX_ATTACHMENTS`.
const MAX_ATTACHMENTS: usize = 100;
/// `PROVIDER_SEND_TURN_MAX_TOTAL_IMAGE_BYTES`.
const MAX_TOTAL_IMAGE_BYTES: u64 = 80 * 1024 * 1024;
const SUPPORTED_IMAGE_TYPES: [&str; 4] = ["image/gif", "image/jpeg", "image/png", "image/webp"];
const IMAGE_EXTENSIONS: [(&str, &str); 11] = [
    ("image/avif", "avif"),
    ("image/bmp", "bmp"),
    ("image/gif", "gif"),
    ("image/heic", "heic"),
    ("image/heif", "heif"),
    ("image/jpeg", "jpg"),
    ("image/jpg", "jpg"),
    ("image/png", "png"),
    ("image/svg+xml", "svg"),
    ("image/tiff", "tiff"),
    ("image/webp", "webp"),
];
const SAFE_IMAGE_EXTENSIONS: [&str; 12] = [
    "avif", "bmp", "gif", "heic", "heif", "ico", "jpeg", "jpg", "png", "svg", "tiff", "webp",
];

/// `getProviderAttachmentLimitError`.
pub(crate) fn limit_error(attachments: &[Value]) -> Option<String> {
    if attachments.len() > MAX_ATTACHMENTS {
        return Some(format!(
            "You can attach up to {MAX_ATTACHMENTS} files per message or question response."
        ));
    }
    let images: u64 = attachments
        .iter()
        .filter(|a| {
            a["type"] == "image"
                || SUPPORTED_IMAGE_TYPES.contains(
                    &a["mimeType"]
                        .as_str()
                        .unwrap_or_default()
                        .to_ascii_lowercase()
                        .as_str(),
                )
        })
        .filter_map(|a| a["sizeBytes"].as_u64())
        .sum();
    (images > MAX_TOTAL_IMAGE_BYTES).then(|| {
        "Images can total up to 80 MiB per message or question response. Use smaller images or send fewer at once.".to_owned()
    })
}

/// The parts of an attachment id: thread segment, uuid, extension.
fn parse_id(id: &str) -> Option<(String, String, Option<String>)> {
    use std::sync::LazyLock;
    static ID: LazyLock<regex::Regex> = LazyLock::new(|| {
        regex::Regex::new(r"(?i-u)^([a-z0-9_]+(?:-[a-z0-9_]+)*)-([0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12})(?:-([a-z0-9]{1,10}))?$").unwrap()
    });
    if id.contains(['/', '.', '\\']) {
        return None;
    }
    let found = ID.captures(id)?;
    Some((
        found[1].to_ascii_lowercase(),
        found[2].to_ascii_lowercase(),
        found.get(3).map(|e| e.as_str().to_ascii_lowercase()),
    ))
}

/// `attachmentIsPendingUpload`.
pub(crate) fn is_pending(attachment: &Value) -> bool {
    attachment["id"]
        .as_str()
        .and_then(parse_id)
        .is_some_and(|(segment, _, _)| segment == "pending")
}

/// `inferImageExtension`, without its dot.
fn image_extension(mime: &str, name: &str) -> String {
    let key = mime.to_ascii_lowercase();
    if let Some((_, ext)) = IMAGE_EXTENSIONS.iter().find(|(m, _)| *m == key) {
        return (*ext).into();
    }
    if let Some(ext) = mime_guess::get_mime_extensions_str(&key).and_then(|e| e.first()) {
        if SAFE_IMAGE_EXTENSIONS.contains(ext) {
            return (*ext).into();
        }
    }
    let name = name.trim();
    if let Some((_, ext)) = name.rsplit_once('.') {
        let ext = ext.to_ascii_lowercase();
        if (1..=8).contains(&ext.len())
            && ext.bytes().all(|b| b.is_ascii_alphanumeric())
            && SAFE_IMAGE_EXTENSIONS.contains(&ext.as_str())
        {
            return ext;
        }
    }
    "bin".into()
}

/// `attachmentRelativePath`: where `attachment` lives under the attachments directory.
pub(crate) fn relative_path(attachment: &Value) -> Option<String> {
    let id = attachment["id"].as_str()?;
    let name = attachment["name"].as_str().unwrap_or_default();
    match attachment["type"].as_str()? {
        "image" => Some(format!(
            "{id}.{}",
            image_extension(attachment["mimeType"].as_str().unwrap_or_default(), name)
        )),
        "file" => Some(format!("{id}.{}", crate::asset_http::file_extension(name))),
        _ => None,
    }
}

/// `resolveAttachmentPathById`: the stored file of `id`, by its extension
/// or, for an image, the first image extension found.
fn path_by_id(dir: &Path, id: &str) -> Option<PathBuf> {
    let (_, _, extension) = parse_id(id)?;
    let stored = |ext: &str| {
        let path = dir.join(format!("{id}.{ext}"));
        path.symlink_metadata()
            .is_ok_and(|m| m.is_file())
            .then_some(path)
    };
    match extension {
        Some(ext) => stored(&ext),
        None => SAFE_IMAGE_EXTENSIONS
            .iter()
            .chain(["bin"].iter())
            .find_map(|ext| stored(ext)),
    }
}

/// What a claim made: the attachments to send, and the copies it wrote.
pub(crate) struct Claimed {
    pub attachments: Vec<Value>,
    pub paths: Vec<PathBuf>,
}

/// `run`'s message as the thread's projection shows it.
pub(crate) fn run_message(store: &crate::store::Store, run: &crate::Run) -> Option<Value> {
    let message_id = run.user_message_id.as_deref()?;
    let thread = store.threads.get(&run.thread_id)?;
    thread.projection()["messages"]
        .as_array()?
        .iter()
        .find(|message| message["id"] == message_id)
        .cloned()
}

/// Attach the saved file `path` to each attachment (`resolveAttachmentPath`).
pub(crate) fn with_paths(dir: &Path, attachments: Vec<Value>) -> Vec<Value> {
    attachments
        .into_iter()
        .map(|mut attachment| {
            attachment["path"] = match relative_path(&attachment) {
                Some(relative) => json!(dir.join(relative).to_string_lossy()),
                None => Value::Null,
            };
            attachment
        })
        .collect()
}

/// The attachments of `run`'s message, each with the `path` of its saved
/// file (`resolveAttachmentPath`), as a provider turn sends them.
pub(crate) fn turn_attachments(
    dir: &Path,
    store: &crate::store::Store,
    run: &crate::Run,
) -> Vec<Value> {
    let attachments = run_message(store, run)
        .and_then(|message| message["attachments"].as_array().cloned())
        .unwrap_or_default();
    with_paths(dir, attachments)
}

/// `releaseClaimedAttachments`: removes copies a send did not keep.
pub(crate) fn release(paths: &[PathBuf]) {
    for path in paths {
        let _ = std::fs::remove_file(path);
        let mut sidecar = path.as_os_str().to_owned();
        sidecar.push(".txt");
        let _ = std::fs::remove_file(PathBuf::from(sidecar));
    }
}

/// `claimPendingAttachments` for `thread_id`, or the failure's message.
pub(crate) fn claim(dir: &Path, thread_id: &str, attachments: &[Value]) -> Result<Claimed, String> {
    if let Some(error) = limit_error(attachments) {
        return Err(error);
    }
    let mut ids: Vec<&str> = attachments
        .iter()
        .filter_map(|a| a["id"].as_str())
        .collect();
    ids.sort_unstable();
    ids.dedup();
    if ids.len() != attachments.len() {
        return Err("Duplicate attachment ids are not allowed.".into());
    }
    let mut claimed = Claimed {
        attachments: Vec::with_capacity(attachments.len()),
        paths: Vec::new(),
    };
    for attachment in attachments {
        if !is_pending(attachment) {
            claimed.attachments.push(attachment.clone());
            continue;
        }
        match claim_one(dir, thread_id, attachment, &mut claimed.paths) {
            Ok(normalized) => claimed.attachments.push(normalized),
            Err(error) => {
                release(&claimed.paths);
                return Err(error);
            }
        }
    }
    Ok(claimed)
}

fn claim_one(
    dir: &Path,
    thread_id: &str,
    attachment: &Value,
    paths: &mut Vec<PathBuf>,
) -> Result<Value, String> {
    let name = attachment["name"].as_str().unwrap_or_default();
    let cannot = |reason: &str| format!("Attachment '{name}' cannot be sent: {reason}.");
    // `planAttachmentClaim`.
    let id = attachment["id"].as_str().unwrap_or_default();
    let (_, _, extension) = parse_id(id).ok_or_else(|| cannot("invalid attachment id"))?;
    let segment =
        crate::html_render::thread_segment(thread_id).ok_or_else(|| cannot("invalid thread id"))?;
    let current =
        path_by_id(dir, id).ok_or_else(|| cannot("attachment not found (removed or expired)"))?;
    let final_id = match &extension {
        Some(ext) => format!("{segment}-{}-{ext}", crate::platform::uuid_v4()),
        None => format!("{segment}-{}", crate::platform::uuid_v4()),
    };
    let stored_ext = current
        .extension()
        .map(|e| e.to_string_lossy().into_owned())
        .unwrap_or_default();
    let final_path = dir.join(format!("{final_id}.{stored_ext}"));
    let size = std::fs::metadata(&current)
        .map_err(|_| cannot("attachment not found"))?
        .len();
    if Some(size) != attachment["sizeBytes"].as_u64() {
        return Err(cannot("stored size does not match"));
    }
    let mut normalized = attachment.clone();
    normalized["id"] = json!(final_id);
    let svg = attachment["mimeType"]
        .as_str()
        .unwrap_or_default()
        .split(';')
        .next()
        .unwrap_or_default()
        .trim()
        .eq_ignore_ascii_case("image/svg+xml");
    if attachment["type"] == "file" && svg && !name.to_ascii_lowercase().ends_with(".svg") {
        normalized["name"] = json!(format!("{name}.svg"));
    }
    normalized["mimeType"] = json!(attachment["mimeType"]
        .as_str()
        .unwrap_or_default()
        .to_lowercase());
    if relative_path(&normalized).map(|p| dir.join(p)) != Some(final_path.clone()) {
        return Err(cannot("attachment type does not match the upload"));
    }
    std::fs::copy(&current, &final_path)
        .map_err(|_| format!("Failed to claim attachment '{name}' for this thread."))?;
    paths.push(final_path.clone());
    let mut sidecar = current.into_os_string();
    sidecar.push(".txt");
    let sidecar = PathBuf::from(sidecar);
    if sidecar.is_file() {
        let mut copy = final_path.into_os_string();
        copy.push(".txt");
        let copy = PathBuf::from(copy);
        if std::fs::copy(&sidecar, &copy).is_ok() {
            paths.push(copy);
        }
    }
    Ok(normalized)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_pending_upload_is_copied_to_the_thread_once_checked() {
        let dir = tempfile::tempdir().unwrap();
        let pending = "pending-0b9c2c7e-6a2b-4b8f-9f1e-2f5b7c3a1d00";
        std::fs::write(dir.path().join(format!("{pending}.png")), b"12345").unwrap();
        let image = json!({"type": "image", "id": pending, "name": "a.png", "mimeType": "IMAGE/PNG", "sizeBytes": 5});
        let claimed = claim(dir.path(), "thread:One", std::slice::from_ref(&image)).unwrap();
        let id = claimed.attachments[0]["id"].as_str().unwrap().to_owned();
        assert!(id.starts_with("thread-one-"));
        assert_eq!(claimed.attachments[0]["mimeType"], "image/png");
        assert!(dir.path().join(format!("{id}.png")).is_file());
        // The pending copy stays for a retry.
        assert!(dir.path().join(format!("{pending}.png")).is_file());
        let mut wrong = image.clone();
        wrong["sizeBytes"] = json!(6);
        assert_eq!(
            claim(dir.path(), "t", &[wrong]).err().unwrap(),
            "Attachment 'a.png' cannot be sent: stored size does not match."
        );
        let mut jpeg = image.clone();
        jpeg["mimeType"] = json!("image/jpeg");
        assert!(claim(dir.path(), "t", &[jpeg])
            .err()
            .unwrap()
            .ends_with("attachment type does not match the upload."));
        assert_eq!(
            claim(dir.path(), "t", &[image.clone(), image])
                .err()
                .unwrap(),
            "Duplicate attachment ids are not allowed."
        );
        let owned =
            json!({"type": "file", "id": "thread-one-0b9c2c7e-6a2b-4b8f-9f1e-2f5b7c3a1d01-txt"});
        assert!(!is_pending(&owned));
    }
}
