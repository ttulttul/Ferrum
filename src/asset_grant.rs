//! The signed capability used by both asset routers.
use crate::rpc_protocol::Reply;
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use hmac::{Hmac, Mac};
use percent_encoding::{utf8_percent_encode, AsciiSet, NON_ALPHANUMERIC};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::Sha256;
use std::path::PathBuf;

const COMPONENT: &AsciiSet = &NON_ALPHANUMERIC
    .remove(b'-')
    .remove(b'_')
    .remove(b'.')
    .remove(b'!')
    .remove(b'~')
    .remove(b'*')
    .remove(b'\'')
    .remove(b'(')
    .remove(b')');

pub(crate) fn encode_component(text: &str) -> String {
    utf8_percent_encode(text, COMPONENT).to_string()
}

#[derive(Serialize, Deserialize)]
pub struct AssetGrant {
    pub(crate) claims: Value,
    pub(crate) name: String,
    pub(crate) dimensions: Option<Value>,
    pub(crate) source_path: Option<PathBuf>,
    pub(crate) revision: Option<String>,
}

impl AssetGrant {
    pub(crate) fn issue(
        &self,
        resource: &Value,
        sign: impl FnOnce(&str) -> anyhow::Result<String>,
    ) -> Reply {
        let encoded = URL_SAFE_NO_PAD.encode(crate::wire_json::compact(&self.claims));
        let signature = match sign(&encoded) {
            Ok(s) => s,
            Err(error) => {
                return Reply::Failure(
                    json!({"_tag":"Fail","error":{"_tag":"AssetSigningKeyLoadError","resource":resource,"cause":{"name":"Error","message":error.to_string()}}}),
                )
            }
        };
        let name = encode_component(&self.name);
        let mut result = json!({"relativeUrl":format!("/api/assets/{encoded}.{signature}/{name}"),"expiresAt":self.claims["expiresAt"]});
        if let Some(value) = &self.dimensions {
            result["imageDimensions"] = value.clone();
        }
        if let Some(value) = &self.source_path {
            result["sourcePath"] = json!(value);
        }
        if let Some(value) = &self.revision {
            result["fileRevision"] = json!(value);
        }
        Reply::Success(result)
    }
}

pub(crate) fn signature(key: &[u8; 32], payload: &str) -> String {
    let mut mac = Hmac::<Sha256>::new_from_slice(key).expect("HMAC accepts any key");
    mac.update(payload.as_bytes());
    URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes())
}

pub(crate) fn verify(key: &[u8; 32], token: &str) -> Option<Value> {
    let (encoded, signature) = token.split_once('.')?;
    let mut mac = Hmac::<Sha256>::new_from_slice(key).ok()?;
    mac.update(encoded.as_bytes());
    mac.verify_slice(&URL_SAFE_NO_PAD.decode(signature).ok()?)
        .ok()?;
    let claims: Value = serde_json::from_slice(&URL_SAFE_NO_PAD.decode(encoded).ok()?).ok()?;
    (claims["version"] == 1 && claims["expiresAt"].as_u64()? > crate::platform::now_ms() as u64)
        .then_some(claims)
}
