use super::{header, HeaderMap, Request, Response};
use crate::{
    auth_service::AuthService,
    relay_service::tokens::{verify_proof, CheckedProof},
};
use axum::middleware::Next;
use std::sync::OnceLock;

struct RequestContext {
    method: String,
    url: Option<String>,
    validated: OnceLock<(String, String)>,
}

tokio::task_local! {static CONTEXT:RequestContext;}

// Request metadata comes from the server transport. The per-request cache lets several
// authorization layers examine one proof while consuming its replay marker exactly once.
pub(crate) async fn contextualize(request: Request, next: Next) -> Response {
    let h = request.headers();
    let scheme = if header(h, "x-forwarded-proto") == Some("https") {
        "https"
    } else {
        "http"
    };
    let url = header(h, "host").and_then(|host| {
        let mut url = url::Url::parse(&format!("{scheme}://{host}")).ok()?;
        if url.path() != "/"
            || url.query().is_some()
            || url.fragment().is_some()
            || !url.username().is_empty()
            || url.password().is_some()
        {
            return None;
        }
        url.set_path(request.uri().path());
        Some(url.to_string())
    });
    let context = RequestContext {
        method: request.method().to_string(),
        url,
        validated: OnceLock::new(),
    };
    CONTEXT.scope(context, next.run(request)).await
}

pub(super) fn check(
    proof: &str,
    binding: Option<(&str, &str)>,
) -> Result<CheckedProof, &'static str> {
    CONTEXT
        .try_with(|ctx| {
            let url = ctx.url.as_deref().ok_or("invalid_proof")?;
            verify_proof(proof, &ctx.method, url, binding)
        })
        .unwrap_or(Err("invalid_proof"))
}
pub(super) fn authenticate(
    auth: &AuthService,
    proof: &str,
    thumb: &str,
    token: &str,
) -> Result<(), &'static str> {
    CONTEXT
        .try_with(|ctx| {
            if ctx
                .validated
                .get()
                .is_some_and(|(p, t)| p == proof && t == token)
            {
                return Ok(());
            }
            let checked = check(proof, Some((thumb, token)))?;
            auth.consume_proof(&checked)?;
            let _ = ctx.validated.set((proof.to_owned(), token.to_owned()));
            Ok(())
        })
        .unwrap_or(Err("invalid_proof"))
}
pub(crate) fn reason(reason: &str) -> bool {
    matches!(
        reason,
        "invalid_proof"
            | "key_mismatch"
            | "token_mismatch"
            | "request_mismatch"
            | "time_window"
            | "replay"
    )
}
pub(crate) fn uses_proof(h: &HeaderMap) -> bool {
    header(h, "authorization")
        .and_then(|s| s.split_once(' '))
        .is_some_and(|(scheme, _)| scheme.eq_ignore_ascii_case("DPoP"))
        || h.contains_key("dpop")
}
