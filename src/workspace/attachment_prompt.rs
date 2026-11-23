//! A turn's message attachments as the provider sees them, as the
//! reference's `attachmentPrompt` writes them: the text names each
//! attachment's saved file, and a supported image also goes as an image the
//! model can read. Shared by every workspace that runs provider turns.
use base64::Engine;
use serde_json::Value;

/// `PROVIDER_SEND_TURN_SUPPORTED_IMAGE_MIME_TYPES`.
const NATIVE_IMAGE_TYPES: [&str; 4] = ["image/gif", "image/jpeg", "image/png", "image/webp"];

/// `isProviderNativeImageAttachment`.
pub fn is_native_image(attachment: &Value) -> bool {
    attachment["type"] == "image"
        && NATIVE_IMAGE_TYPES.contains(
            &attachment["mimeType"]
                .as_str()
                .unwrap_or_default()
                .to_ascii_lowercase()
                .as_str(),
        )
}

/// `providerMessageTextWithAttachmentPaths`: `text`, then a line naming
/// each attachment's saved file (and its extracted text, when a `.txt`
/// sidecar holds it).
pub fn text_with_paths(text: &str, attachments: &[Value]) -> String {
    let mut out = text.to_owned();
    for attachment in attachments {
        let kind = attachment["type"].as_str().unwrap_or_default();
        let name = attachment["name"].as_str().unwrap_or_default();
        let context = match attachment["path"].as_str() {
            None => format!(
                "[Attached {kind} \"{name}\" is unavailable: could not resolve a saved path for attachment '{}'.]",
                attachment["id"].as_str().unwrap_or_default()
            ),
            Some(path) => {
                let sidecar = format!("{path}.txt");
                let extracted = if std::path::Path::new(&sidecar).is_file() {
                    format!(" (text extracted to: {sidecar})")
                } else {
                    String::new()
                };
                format!("[Attached {kind} \"{name}\" is saved at: {path}{extracted}]")
            }
        };
        out = if out.is_empty() {
            context
        } else {
            format!("{out}\n\n{context}")
        };
    }
    out
}

/// The base64 bytes of `attachment`'s saved file, or the reference's
/// failure (`Failed to read attachment '<id>'.`).
pub fn image_base64(attachment: &Value) -> Result<String, String> {
    let id = attachment["id"].as_str().unwrap_or_default();
    let path = attachment["path"]
        .as_str()
        .ok_or_else(|| format!("Invalid attachment id '{id}'"))?;
    let bytes = std::fs::read(path).map_err(|_| format!("Failed to read attachment '{id}'."))?;
    Ok(base64::engine::general_purpose::STANDARD.encode(bytes))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn each_attachment_is_named_after_the_text() {
        let attachments = [
            json!({"type": "image", "id": "a", "name": "shot.png", "mimeType": "image/png", "path": "/x/a.png"}),
            json!({"type": "file", "id": "b", "name": "notes.md", "mimeType": "text/markdown", "path": null}),
        ];
        assert_eq!(
            text_with_paths("Look", &attachments),
            "Look\n\n[Attached image \"shot.png\" is saved at: /x/a.png]\n\n[Attached file \"notes.md\" is unavailable: could not resolve a saved path for attachment 'b'.]"
        );
        assert_eq!(
            text_with_paths("", &attachments[..1]),
            "[Attached image \"shot.png\" is saved at: /x/a.png]"
        );
        assert!(is_native_image(&attachments[0]));
        assert!(!is_native_image(
            &json!({"type": "image", "mimeType": "image/svg+xml"})
        ));
    }
}
