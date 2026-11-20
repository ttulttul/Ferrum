//! The Effect RPC envelope over the T3 WebSocket: request ids, replies,
//! stream chunks and frame parts. Platform neutral, so the native socket loop
//! and the workers control plane frame replies identically.
use serde_json::{json, Value};

/// How one call ends, or one batch of stream values.
pub(crate) enum Reply {
    Success(Value),
    Failure(Value),
    Chunk(Value),
}

impl Reply {
    pub(crate) fn defect(message: String) -> Self {
        Self::Failure(json!({"_tag":"Die","defect":message}))
    }

    pub(crate) fn frame(self, id: &Value) -> Value {
        let exit = match self {
            Self::Success(value) => json!({"_tag":"Success","value":value}),
            Self::Failure(reason) => json!({"_tag":"Failure","cause":[reason]}),
            Self::Chunk(values) => return json!({"_tag":"Chunk","requestId":id,"values":values}),
        };
        json!({"_tag":"Exit","requestId":id,"exit":exit})
    }
}

/// Effect RPC request ids are a string or a number, and every reply echoes the
/// id exactly as the client sent it. Effect 4 clients, including the 0.1.10
/// desktop and web apps, number their requests (`"id":0`). The key keeps the
/// JSON type, so `0` and `"0"` are different requests, as they are in the
/// reference server's `Map`.
pub(crate) fn request_key(id: &Value) -> Option<String> {
    matches!(id, Value::String(_) | Value::Number(_)).then(|| crate::wire_json::compact(id))
}

/// `${value}` in JavaScript, for the reference server's invalid-id defect.
pub(crate) fn js_template_string(value: &Value) -> String {
    match value {
        Value::Null => "null".into(),
        Value::String(text) => text.clone(),
        Value::Object(_) => "[object Object]".into(),
        Value::Array(items) => items
            .iter()
            .map(|item| match item {
                Value::Null => String::new(),
                item => js_template_string(item),
            })
            .collect::<Vec<_>>()
            .join(","),
        other => crate::wire_json::compact(other),
    }
}

/// The client opts into frame parts with `frameParts` in the socket URL. Like
/// the reference server's `splitLargeSocketFrames`, text frames whose UTF-8
/// size may exceed one part go out as consecutive parts, so a slow link keeps
/// delivering frames within the client's liveness timeout.
pub(crate) const SOCKET_FRAME_PARTS_QUERY_PARAM: &str = "frameParts";
pub(crate) const SOCKET_FRAME_PART_BYTES: usize = 48 * 1024;
pub(crate) const SOCKET_FRAME_PART_PREFIX: char = '\u{1e}';

/// Mirrors `splitSocketFrame` in packages/shared/src/socketFrameParts.ts. Every
/// part but the last starts with a record separator.
pub(crate) fn split_socket_frame(frame: &str, part_bytes: usize) -> Vec<String> {
    // JavaScript measures `frame.length` in UTF-16 code units, each at most 3 UTF-8 bytes.
    if frame.encode_utf16().count() * 3 <= part_bytes {
        return vec![frame.to_owned()];
    }
    let prefix_bytes = SOCKET_FRAME_PART_PREFIX.len_utf8();
    let mut parts = Vec::new();
    let mut start = 0;
    let mut bytes = prefix_bytes;
    for (index, character) in frame.char_indices() {
        let size = character.len_utf8();
        if bytes + size > part_bytes {
            parts.push(format!(
                "{SOCKET_FRAME_PART_PREFIX}{}",
                &frame[start..index]
            ));
            start = index;
            bytes = prefix_bytes;
        }
        bytes += size;
    }
    parts.push(frame[start..].to_owned());
    parts
}
