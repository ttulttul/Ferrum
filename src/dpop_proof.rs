//! RFC 9449 DPoP proof checks shared by the environment server and the relay.
//! Pure Rust (jsonwebtoken's RustCrypto backend), so it builds for workers.
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use jsonwebtoken::{decode, decode_header, Algorithm, DecodingKey, Validation};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use time::OffsetDateTime;

pub(crate) fn hash(value: &str) -> String {
    URL_SAFE_NO_PAD.encode(Sha256::digest(value.as_bytes()))
}
pub(crate) fn field<'a>(v: &'a Value, key: &str) -> Option<&'a str> {
    v[key].as_str().filter(|s| !s.trim().is_empty())
}
pub(crate) fn segment(token: &str, index: usize) -> Option<Value> {
    serde_json::from_slice(&URL_SAFE_NO_PAD.decode(token.split('.').nth(index)?).ok()?).ok()
}
pub(crate) struct CheckedProof {
    pub thumbprint: String,
    pub jti: String,
    pub expires: i64,
}
pub(crate) fn verify_proof(
    token: &str,
    method: &str,
    url: &str,
    binding: Option<(&str, &str)>,
) -> Result<CheckedProof, &'static str> {
    let invalid = "invalid_proof";
    let header = decode_header(token).map_err(|_| invalid)?;
    if header.typ.as_deref() != Some("dpop+jwt") || header.alg != Algorithm::ES256 {
        return Err(invalid);
    }
    let raw = segment(token, 0).ok_or(invalid)?;
    let k = &raw["jwk"];
    if k["kty"] != "EC" || k["crv"] != "P-256" || k.get("d").is_some() {
        return Err(invalid);
    }
    let (x, y) = (field(k, "x").ok_or(invalid)?, field(k, "y").ok_or(invalid)?);
    for component in [x, y] {
        if URL_SAFE_NO_PAD
            .decode(component)
            .map_err(|_| invalid)?
            .len()
            != 32
        {
            return Err(invalid);
        }
    }
    let thumb = hash(&json!({"crv":"P-256","kty":"EC","x":x,"y":y}).to_string());
    let key = DecodingKey::from_ec_components(x, y).map_err(|_| invalid)?;
    let mut val = Validation::new(Algorithm::ES256);
    val.required_spec_claims.clear();
    val.validate_exp = false;
    val.validate_aud = false;
    let claims = decode::<Value>(token, &key, &val)
        .map_err(|_| invalid)?
        .claims;
    let jti = field(&claims, "jti").ok_or(invalid)?;
    let iat = claims["iat"].as_i64().ok_or(invalid)?;
    if !field(&claims, "htm").is_some_and(|m| m.eq_ignore_ascii_case(method))
        || claims["htu"] != url
    {
        return Err("request_mismatch");
    }
    if let Some((expected, access)) = binding {
        if thumb != expected {
            return Err("key_mismatch");
        }
        if claims["ath"] != hash(access) {
            return Err("token_mismatch");
        }
    }
    if OffsetDateTime::now_utc().unix_timestamp().abs_diff(iat) > 300 {
        return Err("time_window");
    }
    Ok(CheckedProof {
        thumbprint: thumb,
        jti: jti.to_owned(),
        expires: iat.saturating_add(301),
    })
}
