//! What a client of this environment reads and asks of an environment this one links to, over
//! the link this environment already holds (the reference's `peer/PeerThreadRelay.ts` and the
//! linking side of `peer/PeerClientPairing.ts`):
//!
//! - `peerLinks.subscribeThread`, `peerLinks.threadHistoryPage` and `peerLinks.threadItem`: a
//!   linked environment's thread, from its `/api/links/thread/*` routes, passed on as they come.
//! - `peerLinks.pairClient` and `peerLinks.pairLink`: a one-time pairing credential from the
//!   linked environment's `/api/peer-link/client-pairing`, for the client itself or for a link
//!   it makes there.
//!
//! Nothing is held here once the client stops listening.
use crate::peer_links::{PeerLink, PeerLinkError, LINKED_ENVIRONMENTS_OFF};
use crate::AppState;
use futures_util::StreamExt;
use serde_json::{json, Value};
use std::time::Duration;

const STREAM_ROUTE: &str = "/api/links/thread/stream";
const HISTORY_ROUTE: &str = "/api/links/thread/history";
const ITEM_ROUTE: &str = "/api/links/thread/item";
const CLIENT_PAIRING_ROUTE: &str = "/api/peer-link/client-pairing";
/// The linked environment sends a heartbeat every 20 seconds, so a stream this quiet has lost
/// its connection without closing it.
const SILENT_STREAM_TIMEOUT: Duration = Duration::from_secs(75);
/// What a linked environment answers with when it renews the link (`LINK_EXPIRES_AT_HEADER`).
pub(crate) const LINK_EXPIRES_AT_HEADER: &str = "t3-link-expires-at";

/// `PeerLinkThreadReadError`.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ReadError {
    reason: &'static str,
    label: Option<String>,
    message: String,
    /// The failure underneath, as the client hears it, when there is one worth naming.
    cause: Option<String>,
}

impl ReadError {
    fn new(reason: &'static str, label: Option<&str>, message: impl Into<String>) -> Self {
        Self {
            reason,
            label: label.map(str::to_owned),
            message: message.into(),
            cause: None,
        }
    }
    fn unreachable(label: &str) -> Self {
        Self::new(
            "unreachable",
            Some(label),
            format!("{label} did not answer."),
        )
    }
    /// A link that cannot be used, as the client that asked hears it.
    fn from_link_error(error: PeerLinkError, label: &str) -> Self {
        let reason = match error.reason {
            "unknown_link" => "unknown_link",
            "expired" => "expired",
            "incompatible" => "incompatible",
            _ => "unreachable",
        };
        Self::new(reason, Some(label), error.message)
    }
    /// The linked environment's refusal, as the client that asked hears it.
    fn from_status(status: u16, code: Option<&str>, label: &str) -> Self {
        match (status, code) {
            (404, Some("thread_not_found")) => Self::new(
                "thread_not_found",
                Some(label),
                format!("That thread is no longer on {label}."),
            ),
            // An address that stopped serving the route, as after a downgrade there.
            (404, _) => Self::new(
                "incompatible",
                Some(label),
                format!("{label} runs a T3 Code version that does not share its threads over a link. Update it to read its threads from here."),
            ),
            (401 | 403, Some("linked_environments_off")) => Self::new(
                "rejected",
                Some(label),
                format!("{label} has Linked environments turned off. Turn it on in its Settings → Labs."),
            ),
            (401 | 403, _) => Self::new(
                "rejected",
                Some(label),
                format!("{label} refused the read. Its link may have been revoked there; link it again."),
            ),
            _ => Self {
                cause: Some(format!("HTTP {status}")),
                ..Self::unreachable(label)
            },
        }
    }
    pub(crate) fn to_json(&self) -> Value {
        let mut error = json!({"_tag":"PeerLinkThreadReadError","reason":self.reason,"label":self.label,"message":self.message});
        if let Some(cause) = &self.cause {
            error["cause"] = json!(cause);
        }
        error
    }
}

/// The linked environment's thread `input` names, opened at `route`: the response once it
/// answers 200, with the link's label and how long the rest of a plain answer may take.
async fn open(
    state: &AppState,
    input: &Value,
    route: &str,
    params: &[(&str, &str)],
) -> Result<(String, reqwest::Response, Duration), ReadError> {
    if !crate::mcp_oauth::McpHost::linked_environments_enabled(state) {
        return Err(ReadError::new("disabled", None, LINKED_ENVIRONMENTS_OFF));
    }
    let environment_id = input["environmentId"].as_str().unwrap_or_default();
    let thread_id = input["threadId"].as_str().unwrap_or_default();
    let link = match state.peer_links.get(environment_id) {
        Ok(Some(link)) => link,
        Ok(None) => {
            return Err(ReadError::new(
                "unknown_link",
                None,
                "This environment is no longer linked to the one this thread is on.",
            ))
        }
        Err(_) => {
            return Err(ReadError::new(
                "unreachable",
                None,
                "This environment could not read its links.",
            ))
        }
    };
    let label = link.label;
    let resolved = state
        .peer_links
        .resolve(environment_id)
        .await
        .map_err(|error| ReadError::from_link_error(error, &label))?;
    if resolved.capabilities["linkThreadStream"] != true {
        return Err(ReadError::from_status(404, None, &label));
    }
    let timeout = crate::peer_links::probe_timeout(&resolved.url);
    let mut query = vec![("thread", thread_id)];
    query.extend_from_slice(params);
    // Bounds the headers only: an open stream stays quiet for as long as the thread does.
    let sent = tokio::time::timeout(
        timeout,
        state
            .peer_links
            .http()
            .get(format!("{}{route}", resolved.url))
            .query(&query)
            .bearer_auth(&resolved.token)
            .send(),
    )
    .await;
    let response = match sent {
        Ok(Ok(response)) => response,
        _ => {
            let failure = ReadError::unreachable(&label);
            state
                .peer_links
                .record_outcome(environment_id, Some(&failure.message));
            return Err(failure);
        }
    };
    let status = response.status().as_u16();
    if status != 200 {
        // A refusal whose body never arrives is still the refusal its status names.
        let code = tokio::time::timeout(timeout, response.json::<Value>())
            .await
            .ok()
            .and_then(Result::ok)
            .and_then(|body| body["error"].as_str().map(str::to_owned));
        let failure = ReadError::from_status(status, code.as_deref(), &label);
        // A refusal is still an answer: the link reached the peer, except where it was turned away.
        let recorded = (failure.reason != "thread_not_found").then_some(failure.message.as_str());
        state.peer_links.record_outcome(environment_id, recorded);
        return Err(failure);
    }
    state.peer_links.record_outcome(environment_id, None);
    record_renewal(state, environment_id, response.headers());
    Ok((label, response, timeout))
}

/// A peer renews a link each time it is used, and says when it now ends.
pub(crate) fn record_renewal(
    state: &AppState,
    environment_id: &str,
    headers: &reqwest::header::HeaderMap,
) {
    if let Some(expires_at) = headers
        .get(LINK_EXPIRES_AT_HEADER)
        .and_then(|value| value.to_str().ok())
    {
        state.peer_links.record_expiry(environment_id, expires_at);
    }
}

async fn read_json(
    state: &AppState,
    input: &Value,
    route: &str,
    params: &[(&str, &str)],
) -> Result<Value, ReadError> {
    let (label, response, timeout) = open(state, input, route, params).await?;
    // A peer that sends headers and then stalls must not leave the read waiting.
    match tokio::time::timeout(timeout, response.json::<Value>()).await {
        Ok(Ok(body)) => Ok(body),
        _ => Err(ReadError::unreachable(&label)),
    }
}

/// `peerLinks.threadHistoryPage`: the turns before a snapshot's window.
pub(crate) async fn history_page(state: &AppState, input: &Value) -> Result<Value, ReadError> {
    let cursor = input["cursor"].as_str().unwrap_or_default();
    read_json(state, input, HISTORY_ROUTE, &[("cursor", cursor)]).await
}

/// `peerLinks.threadItem`: one item in full, for a row the reader opens.
pub(crate) async fn item(state: &AppState, input: &Value) -> Result<Value, ReadError> {
    let item = input["itemId"].as_str().unwrap_or_default();
    read_json(state, input, ITEM_ROUTE, &[("item", item)]).await
}

/// `peerLinks.subscribeThread`: a snapshot of the thread, then its live items, each passed to
/// `send` as it comes, until the client or the peer ends it. A stream the peer ends is an
/// error, so the client asks again.
pub(crate) async fn subscribe<F, Fut>(state: &AppState, input: &Value, send: F) -> ReadError
where
    F: Fn(Value) -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    let (label, response, _) = match open(state, input, STREAM_ROUTE, &[]).await {
        Ok(opened) => opened,
        Err(error) => return error,
    };
    let mut body = response.bytes_stream();
    let mut buffer: Vec<u8> = Vec::new();
    loop {
        let chunk = match tokio::time::timeout(SILENT_STREAM_TIMEOUT, body.next()).await {
            Ok(Some(Ok(chunk))) => chunk,
            // Silent too long, failed, or closed by the peer.
            _ => return ReadError::unreachable(&label),
        };
        buffer.extend_from_slice(&chunk);
        while let Some(end) = find_event_end(&buffer) {
            let event: Vec<u8> = buffer.drain(..end.0 + end.1).collect();
            let text = String::from_utf8_lossy(&event[..end.0]);
            let data: Vec<&str> = text
                .lines()
                .filter_map(|line| {
                    line.strip_prefix("data:")
                        .map(|rest| rest.strip_prefix(' ').unwrap_or(rest))
                })
                .collect();
            if data.is_empty() {
                continue;
            }
            // An item this version cannot read is skipped, as the owner's own client skips it.
            match serde_json::from_str::<Value>(&data.join("\n")) {
                Ok(item) if item["kind"].is_string() && item["kind"] != "unknown-event" => {
                    send(item).await
                }
                _ => {}
            }
        }
    }
}

/// Where the first complete event in `buffer` ends: its length and the blank line's.
fn find_event_end(buffer: &[u8]) -> Option<(usize, usize)> {
    let lf = buffer.windows(2).position(|w| w == b"\n\n").map(|i| (i, 2));
    let crlf = buffer
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .map(|i| (i, 4));
    match (lf, crlf) {
        (Some(a), Some(b)) => Some(if a.0 <= b.0 { a } else { b }),
        (a, b) => a.or(b),
    }
}

/// What this environment's Connections calls the client that asked.
fn client_label(client: &Value) -> String {
    if let Some(label) = client["label"].as_str() {
        return label.to_owned();
    }
    let parts: Vec<&str> = ["browser", "os"]
        .iter()
        .filter_map(|key| client[*key].as_str())
        .collect();
    if parts.is_empty() {
        "T3 Code client".to_owned()
    } else {
        parts.join(" on ")
    }
}

/// `PeerClientPairing.ask`: asks the linked environment for a pairing; `payload` says for whom.
async fn ask(
    state: &AppState,
    environment_id: &str,
    payload: Value,
) -> Result<Value, PeerLinkError> {
    if !crate::mcp_oauth::McpHost::linked_environments_enabled(state) {
        return Err(PeerLinkError::new("disabled", LINKED_ENVIRONMENTS_OFF));
    }
    let Some(stored) = state.peer_links.get(environment_id)? else {
        return Err(PeerLinkError::new(
            "unknown_link",
            "This environment is no longer linked to that one.",
        ));
    };
    let PeerLink {
        environment_id,
        label,
        urls,
        access,
        ..
    } = stored.clone();
    if stored.expired(crate::platform::now_ms()) {
        return Err(PeerLinkError::new(
            "expired",
            format!(
                "The link to {label} expired. Link it again with a new pairing code from {label}."
            ),
        ));
    }
    let unsupported = |reason: &str, name: &str| json!({"type":"unsupported","environmentId":environment_id,"label":name,"urls":urls,"reason":reason});
    // The linked environment refuses this too; asking would only spend a round trip.
    if access != "full-access" {
        return Ok(unsupported("link_access", &label));
    }
    let Ok(peer) = state.peer_links.resolve(&environment_id).await else {
        return Ok(unsupported("unreachable", &label));
    };
    if peer.capabilities["linkClientPairing"] != true
        // One from before this would read the request as a client's and pair the client instead.
        || (payload.get("link").is_some() && peer.capabilities["linkPairingForLink"] != true)
    {
        return Ok(unsupported("version", &peer.label));
    }
    let sent = tokio::time::timeout(crate::peer_links::probe_timeout(&peer.url), async {
        let response = state
            .peer_links
            .http()
            .post(format!("{}{CLIENT_PAIRING_ROUTE}", peer.url))
            .bearer_auth(&peer.token)
            .json(&payload)
            .send()
            .await?;
        let status = response.status();
        let body = response.json::<Value>().await;
        Ok::<_, reqwest::Error>((status, body))
    })
    .await;
    // No cause is logged or returned: a response this version cannot read may hold the credential.
    let (status, body) = match sent {
        Ok(Ok(answer)) => answer,
        _ => return Ok(unsupported("unreachable", &peer.label)),
    };
    let issued = body.ok().filter(|_| status.is_success());
    let credential = issued
        .as_ref()
        .and_then(|b| b["credential"].as_str())
        .filter(|c| !c.trim().is_empty());
    let expires_at = issued
        .as_ref()
        .and_then(|b| b["expiresAt"].as_str())
        .and_then(crate::peer_links::iso_ms);
    match (credential, expires_at) {
        (Some(credential), Some(expires_at)) => Ok(json!({
            "type": "credential", "environmentId": environment_id, "label": peer.label,
            "urls": urls, "credential": credential,
            "expiresAt": crate::auth_service::iso(expires_at),
        })),
        _ => Ok(unsupported("refused", &peer.label)),
    }
}

/// `peerLinks.pairClient`: a credential for the client that asks, with no more than it holds here.
pub(crate) async fn pair_client(
    state: &AppState,
    principal: &crate::auth_service::Session,
    input: &Value,
) -> Result<Value, PeerLinkError> {
    let payload = json!({"scopes": principal.permissions,
        "clientLabel": client_label(principal.client_metadata())});
    ask(
        state,
        input["environmentId"].as_str().unwrap_or_default(),
        payload,
    )
    .await
}

/// `peerLinks.pairLink`: a link pairing code at `access` from the linked environment, which the
/// client links another environment there with.
pub(crate) async fn pair_link(
    state: &AppState,
    principal: &crate::auth_service::Session,
    input: &Value,
) -> Result<Value, PeerLinkError> {
    let payload = json!({"scopes": [], "clientLabel": client_label(principal.client_metadata()),
        "link": {"access": input["access"]}});
    ask(
        state,
        input["environmentId"].as_str().unwrap_or_default(),
        payload,
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn events_end_at_a_blank_line_of_either_kind() {
        assert_eq!(find_event_end(b"data: 1\n\ndata: 2"), Some((7, 2)));
        assert_eq!(find_event_end(b"data: 1\r\n\r\n"), Some((7, 4)));
        assert_eq!(find_event_end(b"data: 1\n"), None);
    }

    #[test]
    fn a_client_is_named_as_connections_names_it() {
        assert_eq!(client_label(&json!({"label": "Desk"})), "Desk");
        assert_eq!(
            client_label(&json!({"browser": "Firefox", "os": "Linux"})),
            "Firefox on Linux"
        );
        assert_eq!(
            client_label(&json!({"deviceType": "bot"})),
            "T3 Code client"
        );
    }

    #[test]
    fn a_refusal_reads_as_the_reference_words_it() {
        let off = ReadError::from_status(403, Some("linked_environments_off"), "Box");
        assert_eq!(off.reason, "rejected");
        assert!(off.message.contains("Linked environments turned off"));
        assert_eq!(
            ReadError::from_status(404, None, "Box").reason,
            "incompatible"
        );
        assert_eq!(
            ReadError::from_status(404, Some("thread_not_found"), "Box").reason,
            "thread_not_found"
        );
        assert_eq!(
            ReadError::from_status(500, None, "Box").reason,
            "unreachable"
        );
    }
}
