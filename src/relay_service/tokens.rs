use super::{hash, RelayService};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use jsonwebtoken::{
    decode, decode_header, encode, Algorithm, DecodingKey, EncodingKey, Header, Validation,
};
use serde_json::{json, Value};
use std::env;
use time::OffsetDateTime;

pub(crate) const ACCESS: &str = "t3-relay-dpop-access+jwt";
pub(crate) const CHALLENGE: &str = "t3-link-challenge+jwt";
pub(crate) const SCOPES: [&str; 3] = [
    "environment:connect",
    "environment:status",
    "mobile:registration",
];
pub(crate) struct Keys {
    clerk: Option<DecodingKey>,
    audience: Option<String>,
    clerk_issuer: Option<String>,
    pub public_pem: Option<String>,
    pub verify: Option<DecodingKey>,
    sign: Option<EncodingKey>,
}
impl Keys {
    pub(super) fn from_env() -> anyhow::Result<Self> {
        let pem = |name| env::var(name).ok().map(|s| s.replace("\\n", "\n"));
        let public_pem = pem("CLOUD_MINT_PUBLIC_KEY");
        Ok(Self {
            clerk: pem("CLERK_JWT_KEY")
                .map(|s| DecodingKey::from_rsa_pem(s.as_bytes()))
                .transpose()?,
            audience: env::var("CLERK_JWT_AUDIENCE").ok(),
            clerk_issuer: env::var("CLERK_ISSUER").ok(),
            verify: public_pem
                .as_ref()
                .map(|s| DecodingKey::from_ed_pem(s.as_bytes()))
                .transpose()?,
            sign: pem("CLOUD_MINT_PRIVATE_KEY")
                .map(|s| EncodingKey::from_ed_pem(s.as_bytes()))
                .transpose()?,
            public_pem,
        })
    }
    pub fn clerk_subject(&self, token: &str) -> Option<String> {
        let mut v = Validation::new(Algorithm::RS256);
        v.set_issuer(&[self.clerk_issuer.as_deref()?]);
        v.set_audience(&[self.audience.as_deref()?]);
        v.set_required_spec_claims(&["exp", "aud", "iss", "sub"]);
        v.validate_nbf = true;
        v.leeway = 5;
        let data = decode::<Value>(token, self.clerk.as_ref()?, &v).ok()?;
        Some(field(&data.claims, "sub")?.to_owned())
    }
    pub fn mint(&self, typ: &str, claims: &Value) -> Option<String> {
        let mut h = Header::new(Algorithm::EdDSA);
        h.typ = Some(typ.to_owned());
        encode(&h, claims, self.sign.as_ref()?).ok()
    }
}

pub(crate) fn field<'a>(v: &'a Value, key: &str) -> Option<&'a str> {
    v[key].as_str().filter(|s| !s.trim().is_empty())
}
pub(crate) fn verified(
    token: &str,
    key: &DecodingKey,
    typ: &str,
    issuer: &str,
    aud: &str,
) -> Option<Value> {
    let mut v = Validation::new(Algorithm::EdDSA);
    v.set_required_spec_claims(&["exp", "iss", "sub", "aud"]);
    v.set_issuer(&[issuer]);
    v.set_audience(&[aud]);
    v.leeway = 0;
    v.validate_nbf = true;
    let data = decode::<Value>(token, key, &v).ok()?;
    if data.header.typ.as_deref() != Some(typ)
        || data.claims["iat"].as_i64()? > OffsetDateTime::now_utc().unix_timestamp()
    {
        return None;
    }
    field(&data.claims, "jti")?;
    Some(data.claims)
}
pub(crate) fn scopes(client: &str, value: &str) -> Option<String> {
    if !matches!(client, "t3-mobile" | "t3-web") {
        return None;
    }
    let mut result = Vec::new();
    for s in value.split(' ') {
        if !SCOPES.contains(&s) || client == "t3-web" && s == SCOPES[2] {
            return None;
        }
        if !result.contains(&s) {
            result.push(s)
        }
    }
    Some(result.join(" "))
}
pub(crate) fn segment(token: &str, index: usize) -> Option<Value> {
    serde_json::from_slice(&URL_SAFE_NO_PAD.decode(token.split('.').nth(index)?).ok()?).ok()
}
impl RelayService {
    pub(crate) fn proof(
        &self,
        token: &str,
        method: &str,
        path: &str,
        binding: Option<(&str, &str)>,
    ) -> Result<String, &'static str> {
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
            || claims["htu"] != format!("{}{path}", self.issuer)
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
        match self.consume(&thumb, jti, iat + 301) {
            Ok(true) => Ok(thumb),
            Ok(false) => Err("replay"),
            Err(_) => Err("persistence_failed"),
        }
    }
}
