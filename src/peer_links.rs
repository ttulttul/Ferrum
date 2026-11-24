//! Links from this environment to others, kept the way the TypeScript server
//! keeps them (`PeerLinks` in the reference).
//!
//! Linking signs this environment in to the peer's `/mcp` as an outside agent
//! with a pairing code from the peer: the same MCP OAuth flow an outside agent
//! uses (register, decide, exchange), without a browser. The peer's session
//! token goes in the server secret store as `secrets/peer-link-<hex>.bin`; a
//! row in `peer_environment_links` names it and keeps the rest.
//!
//! On a home the TypeScript server created, the table lives in its
//! `statev2.sqlite`, so a link made on either server works on both and
//! forgetting it on either forgets it on both. A home without that database
//! keeps the same table in `peer-links.sqlite` beside it, and its rows move
//! into `statev2.sqlite` once that exists.
use futures_util::future::{select_ok, BoxFuture};
use futures_util::FutureExt;
use rusqlite::{params, Connection, OpenFlags, OptionalExtension};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};
use std::time::Duration;
use time::{format_description::well_known::Rfc3339, OffsetDateTime};

/// `MCP_PEER_LINK_SOFTWARE_ID` in the reference contracts.
pub(crate) const SOFTWARE_ID: &str = "t3code-peer-link";
/// The redirect registered for a link. Nothing listens there: the code comes back in the response.
const LINK_REDIRECT_URI: &str = "http://127.0.0.1/t3code-peer-link";
/// How long a listing waits on a link's addresses.
const LIST_PROBE_TIMEOUT: Duration = Duration::from_secs(3);
pub(crate) const LINKED_ENVIRONMENTS_OFF: &str =
    "Linked environments is off on this machine. Turn it on in Settings → Labs.";
const STATE_DB: &str = "statev2.sqlite";
const OWN_DB: &str = "peer-links.sqlite";
const ACCESS: [&str; 5] = [
    "read-only",
    "approval-required",
    "auto-accept-edits",
    "auto",
    "full-access",
];

/// `PeerLinkError` in the reference contracts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PeerLinkError {
    pub reason: &'static str,
    pub message: String,
}

impl PeerLinkError {
    pub(crate) fn new(reason: &'static str, message: impl Into<String>) -> Self {
        Self {
            reason,
            message: message.into(),
        }
    }
    fn storage(cause: impl std::fmt::Display) -> Self {
        tracing::warn!("Peer link storage failed: {cause}");
        Self::new(
            "storage",
            "Linked environments could not be read or saved here. The server log has details.",
        )
    }
    fn unreachable(message: impl Into<String>) -> Self {
        Self::new("unreachable", message)
    }
    pub(crate) fn to_json(&self) -> Value {
        json!({"_tag":"PeerLinkError","reason":self.reason,"message":self.message})
    }
}

type Result<T> = std::result::Result<T, PeerLinkError>;

/// A stored link (`PeerLink` in the reference contracts).
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct PeerLink {
    pub environment_id: String,
    pub label: String,
    pub urls: Vec<String>,
    pub access: String,
    pub linked_at: String,
    pub expires_at: String,
    pub last_reached_at: Option<String>,
    pub last_error: Option<String>,
}

impl PeerLink {
    pub(crate) fn to_json(&self) -> Value {
        json!({
            "environmentId": self.environment_id,
            "label": self.label,
            "urls": self.urls,
            "access": self.access,
            "linkedAt": self.linked_at,
            "expiresAt": self.expires_at,
            "lastReachedAt": self.last_reached_at,
            "lastError": self.last_error,
        })
    }
    pub(crate) fn expired(&self, now_ms: i64) -> bool {
        iso_ms(&self.expires_at).is_none_or(|expires| expires <= now_ms)
    }
}

/// What `link` takes (`PeerLinkCreateInput`).
#[derive(Debug, Clone, Default)]
pub(crate) struct LinkInput {
    pub url: String,
    pub alternate_urls: Vec<String>,
    pub pairing_code: String,
    pub access: String,
    pub expected_environment_id: Option<String>,
    pub expected_label: Option<String>,
}

impl LinkInput {
    /// The RPC payload, or the path of the first field that does not decode.
    pub(crate) fn from_json(value: &Value) -> std::result::Result<Self, String> {
        let text = |key: &str| -> std::result::Result<Option<String>, String> {
            match value.get(key) {
                None => Ok(None),
                Some(Value::String(s)) if !s.trim().is_empty() => Ok(Some(s.trim().to_owned())),
                Some(_) => Err(format!("Expected a non-empty string\n  at [\"{key}\"]")),
            }
        };
        let required =
            |key: &str| text(key)?.ok_or_else(|| format!("Missing key\n  at [\"{key}\"]"));
        let access = match value.get("access").and_then(Value::as_str) {
            Some(a) if ACCESS.contains(&a) => a.to_owned(),
            _ => {
                return Err("Expected \"read-only\" | \"approval-required\" | \"auto-accept-edits\" | \"auto\" | \"full-access\"\n  at [\"access\"]".into())
            }
        };
        let alternate_urls = match value.get("alternateUrls") {
            None => Vec::new(),
            Some(Value::Array(items)) => items
                .iter()
                .map(|v| {
                    v.as_str()
                        .map(str::trim)
                        .filter(|s| !s.is_empty())
                        .map(str::to_owned)
                        .ok_or_else(|| {
                            "Expected a non-empty string\n  at [\"alternateUrls\"]".to_owned()
                        })
                })
                .collect::<std::result::Result<_, _>>()?,
            Some(_) => return Err("Expected an array\n  at [\"alternateUrls\"]".into()),
        };
        Ok(Self {
            url: required("url")?,
            alternate_urls,
            pairing_code: required("pairingCode")?,
            access,
            expected_environment_id: text("expectedEnvironmentId")?,
            expected_label: text("expectedLabel")?,
        })
    }
}

/// A usable link: where to reach the peer and the token to send there.
pub(crate) struct Resolved {
    pub url: String,
    pub token: String,
}

/// What this environment knows of itself when linking.
pub(crate) struct SelfDescriptor {
    pub environment_id: String,
    pub label: String,
}

/// Whether a host is this machine's own loopback (`isLocalLoopbackHost`).
pub(crate) fn is_loopback_host(host: &str) -> bool {
    let host = host
        .trim_start_matches('[')
        .trim_end_matches(']')
        .to_ascii_lowercase();
    host == "localhost"
        || host == "::1"
        || host
            .parse::<std::net::Ipv4Addr>()
            .is_ok_and(|ip| ip.octets()[0] == 127)
}

/// Whether a host is a Tailscale address (`isTailnetHost`).
fn is_tailnet_host(host: &str) -> bool {
    let host = host
        .trim_start_matches('[')
        .trim_end_matches(']')
        .to_ascii_lowercase();
    if host.ends_with(".ts.net") {
        return true;
    }
    host.parse::<std::net::Ipv6Addr>().is_ok_and(|ip| {
        let s = ip.segments();
        s[0] == 0xfd7a && s[1] == 0x115c && s[2] == 0xa1e0
    })
}

/// How long a peer's descriptor or MCP handshake may take before a route counts as unreachable.
pub(crate) fn probe_timeout(origin: &str) -> Duration {
    let loopback = url::Url::parse(origin)
        .ok()
        .and_then(|u| u.host_str().map(is_loopback_host))
        .unwrap_or(false);
    Duration::from_secs(if loopback { 3 } else { 10 })
}

/// The base URL a peer is reached at, normalised to its origin. Plain http is
/// accepted only where the network already encrypts: this machine, or Tailscale.
fn peer_origin(raw: &str) -> Result<String> {
    let parsed = url::Url::parse(raw)
        .ok()
        .filter(|u| matches!(u.scheme(), "http" | "https") && u.host_str().is_some())
        .ok_or_else(|| {
            PeerLinkError::unreachable("That address is not an http or https address.".to_owned())
        })?;
    let host = parsed.host_str().unwrap_or_default();
    if parsed.scheme() == "http" && !is_loopback_host(host) && !is_tailnet_host(host) {
        // The origin, never the address as given: it may hold a username,
        // password or query.
        return Err(PeerLinkError::unreachable(format!(
            "{} is plain http, which would send the link's credentials unencrypted. Use its https address, or its Tailscale address.",
            parsed.origin().ascii_serialization()
        )));
    }
    Ok(parsed.origin().ascii_serialization())
}

pub(crate) fn iso_ms(text: &str) -> Option<i64> {
    OffsetDateTime::parse(text, &Rfc3339)
        .ok()
        .map(|t| (t.unix_timestamp_nanos() / 1_000_000) as i64)
}

fn iso(ms: i64) -> String {
    crate::auth_service::iso(ms)
}

/// What a peer's `/.well-known/t3/environment` says.
#[derive(Clone)]
pub(crate) struct Descriptor {
    pub environment_id: String,
    pub label: String,
    /// It holds a linked agent to its limits and keeps its work within its link.
    pub link_limits_hold: bool,
}

/// Node's name for the system error under a failed connection.
fn connection_code(error: &reqwest::Error) -> Option<&'static str> {
    let mut source: Option<&(dyn std::error::Error + 'static)> = Some(error);
    while let Some(current) = source {
        if let Some(io) = current.downcast_ref::<std::io::Error>() {
            return match io.kind() {
                std::io::ErrorKind::ConnectionRefused => Some("ECONNREFUSED"),
                std::io::ErrorKind::ConnectionReset => Some("ECONNRESET"),
                std::io::ErrorKind::TimedOut => Some("ETIMEDOUT"),
                _ => None,
            };
        }
        let text = current.to_string();
        if text.contains("dns error") || text.contains("failed to lookup address") {
            return Some("ENOTFOUND");
        }
        source = current.source();
    }
    None
}

/// A TLS verification failure's name, if `error` is one.
fn certificate_code(error: &reqwest::Error) -> Option<String> {
    let mut source: Option<&(dyn std::error::Error + 'static)> = Some(error);
    while let Some(current) = source {
        let text = current.to_string();
        if text.contains("certificate") || text.contains("UnknownIssuer") {
            let code = if text.contains("UnknownIssuer")
                || text.contains("self-signed")
                || text.contains("self signed")
            {
                "SELF_SIGNED_CERT_IN_CHAIN"
            } else if text.contains("Expired") || text.contains("expired") {
                "CERT_HAS_EXPIRED"
            } else if text.contains("NotValidForName") || text.contains("not valid for name") {
                "ERR_TLS_CERT_ALTNAME_INVALID"
            } else {
                "UNABLE_TO_VERIFY_LEAF_SIGNATURE"
            };
            return Some(code.into());
        }
        source = current.source();
    }
    None
}

enum ProbeError {
    Http(reqwest::Error),
    Shape,
}

/// Peer links on one home.
pub(crate) struct PeerLinks {
    state_dir: PathBuf,
    secrets: PathBuf,
    http: reqwest::Client,
}

impl PeerLinks {
    pub(crate) fn new(state_dir: &Path, secrets: &Path) -> Self {
        // Only the origin `peer_origin` checked may receive a pairing code or
        // token: a redirect could forward them anywhere.
        let http = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .no_proxy()
            .build()
            .expect("peer HTTP client");
        Self {
            state_dir: state_dir.to_owned(),
            secrets: secrets.to_owned(),
            http,
        }
    }

    pub(crate) fn http(&self) -> &reqwest::Client {
        &self.http
    }

    // --- storage -----------------------------------------------------------

    /// The link table: in `statev2.sqlite` when this home has it, otherwise in
    /// this server's own `peer-links.sqlite`.
    fn open(&self) -> Result<Connection> {
        let state = self.state_dir.join(STATE_DB);
        let own = self.state_dir.join(OWN_DB);
        let conn = if state.is_file() {
            // No SQLITE_OPEN_CREATE: the TypeScript server owns that database.
            let conn = Connection::open_with_flags(
                &state,
                OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_NO_MUTEX,
            )
            .map_err(PeerLinkError::storage)?;
            conn.busy_timeout(Duration::from_secs(5))
                .map_err(PeerLinkError::storage)?;
            ensure_table(&conn).map_err(PeerLinkError::storage)?;
            if own.is_file() {
                adopt_own_links(&conn, &own).map_err(PeerLinkError::storage)?;
            }
            conn
        } else {
            std::fs::create_dir_all(&self.state_dir).map_err(PeerLinkError::storage)?;
            let conn = Connection::open(&own).map_err(PeerLinkError::storage)?;
            conn.busy_timeout(Duration::from_secs(5))
                .map_err(PeerLinkError::storage)?;
            ensure_table(&conn).map_err(PeerLinkError::storage)?;
            conn
        };
        Ok(conn)
    }

    /// Whether this home has anywhere links are kept. Reading a home without
    /// one finds no links and creates nothing.
    fn has_store(&self) -> bool {
        self.state_dir.join(STATE_DB).is_file() || self.state_dir.join(OWN_DB).is_file()
    }

    fn secret_path(&self, name: &str) -> PathBuf {
        self.secrets.join(format!("{name}.bin"))
    }

    /// `ServerSecretStore.set`: written to a temporary file, mode 0600, then renamed.
    fn write_secret(&self, name: &str, value: &[u8]) -> Result<()> {
        use std::io::Write;
        std::fs::create_dir_all(&self.secrets).map_err(PeerLinkError::storage)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let _ = std::fs::set_permissions(&self.secrets, std::fs::Permissions::from_mode(0o700));
        }
        let path = self.secret_path(name);
        let temp = self
            .secrets
            .join(format!("{name}.bin.{}.tmp", crate::platform::uuid_v4()));
        let written = (|| -> std::io::Result<()> {
            let mut options = std::fs::OpenOptions::new();
            options.write(true).create_new(true);
            #[cfg(unix)]
            {
                use std::os::unix::fs::OpenOptionsExt;
                options.mode(0o600);
            }
            let mut file = options.open(&temp)?;
            file.write_all(value)?;
            file.sync_all()?;
            std::fs::rename(&temp, &path)
        })();
        if let Err(e) = written {
            let _ = std::fs::remove_file(&temp);
            return Err(PeerLinkError::storage(e));
        }
        Ok(())
    }

    fn read_secret(&self, name: &str) -> Result<Option<Vec<u8>>> {
        match std::fs::read(self.secret_path(name)) {
            Ok(bytes) => Ok(Some(bytes)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(PeerLinkError::storage(e)),
        }
    }

    fn remove_secret(&self, name: &str) -> Result<()> {
        match std::fs::remove_file(self.secret_path(name)) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(PeerLinkError::storage(e)),
        }
    }

    fn read_row(&self, environment_id: &str) -> Result<Option<Row>> {
        if !self.has_store() {
            return Ok(None);
        }
        let conn = self.open()?;
        conn.query_row(
            &format!("SELECT {COLUMNS} FROM peer_environment_links WHERE environment_id = ?1"),
            [environment_id],
            Row::from_sql,
        )
        .optional()
        .map_err(PeerLinkError::storage)
    }

    fn read_rows(&self) -> Result<Vec<Row>> {
        if !self.has_store() {
            return Ok(Vec::new());
        }
        let conn = self.open()?;
        let mut statement = conn
            .prepare(&format!(
                "SELECT {COLUMNS} FROM peer_environment_links ORDER BY label, environment_id"
            ))
            .map_err(PeerLinkError::storage)?;
        let rows = statement
            .query_map([], Row::from_sql)
            .map_err(PeerLinkError::storage)?
            .collect::<rusqlite::Result<Vec<_>>>()
            .map_err(PeerLinkError::storage)?;
        Ok(rows)
    }

    /// The stored link, without asking the peer whether it answers.
    pub(crate) fn get(&self, environment_id: &str) -> Result<Option<PeerLink>> {
        self.read_row(environment_id)?
            .map(Row::into_link)
            .transpose()
    }

    /// A peer's label follows its machine name.
    fn with_label(&self, mut link: PeerLink, label: &str) -> PeerLink {
        if link.label != label {
            if let Ok(conn) = self.open() {
                if let Err(e) = conn.execute(
                    "UPDATE peer_environment_links SET label = ?1 WHERE environment_id = ?2",
                    params![label, link.environment_id],
                ) {
                    tracing::warn!("could not update a peer link's label: {e}");
                }
            }
            link.label = label.to_owned();
        }
        link
    }

    /// Records the outcome of a call, so listing shows whether the peer answered last time.
    pub(crate) fn record_outcome(&self, environment_id: &str, error: Option<&str>) {
        let Ok(conn) = self.open() else { return };
        let done = match error {
            None => conn.execute(
                "UPDATE peer_environment_links SET last_reached_at = ?1, last_error = NULL WHERE environment_id = ?2",
                params![iso(crate::platform::now_ms()), environment_id],
            ),
            Some(error) => conn.execute(
                "UPDATE peer_environment_links SET last_error = ?1 WHERE environment_id = ?2",
                params![error, environment_id],
            ),
        };
        if let Err(e) = done {
            tracing::warn!("could not record a peer link's outcome: {e}");
        }
    }

    /// Forgets the link here. The peer still lists the session until it is revoked there.
    pub(crate) fn unlink(&self, environment_id: &str) -> Result<bool> {
        Ok(!self.forget(environment_id)?.is_empty())
    }

    /// Forgets the link to `environment_id` and ends its session at the peer
    /// (`PeerLinks.unlink`): whether there was one, and whether the peer revoked it.
    pub(crate) async fn unlink_ending(&self, environment_id: &str) -> Result<(bool, bool)> {
        let forgotten = self.forget(environment_id)?;
        let mut ended_there = false;
        for (urls_json, token) in &forgotten {
            if let Some(token) = token {
                ended_there = self.revoke_at_peer(environment_id, urls_json, token).await;
            }
        }
        Ok((!forgotten.is_empty(), ended_there))
    }

    /// `revokeAtPeer`: the link's token revoked at the first of its addresses that still
    /// answers as the peer; an address can change hands after linking, and only the linked
    /// peer may see the token.
    async fn revoke_at_peer(&self, environment_id: &str, urls_json: &str, token: &str) -> bool {
        let urls: Vec<String> = serde_json::from_str(urls_json).unwrap_or_default();
        if urls.is_empty() {
            return false;
        }
        let attempts: Vec<BoxFuture<'_, std::result::Result<(), ()>>> = urls
            .iter()
            .map(|url| {
                async move {
                    self.answers_as(url, environment_id).await.ok_or(())?;
                    let response = self
                        .http
                        .post(format!("{url}/oauth/mcp/revoke"))
                        .form(&[("token", token)])
                        .timeout(probe_timeout(url))
                        .send()
                        .await
                        .map_err(|_| ())?;
                    response.status().is_success().then_some(()).ok_or(())
                }
                .boxed()
            })
            .collect();
        let revoked = select_ok(attempts).await.is_ok();
        revoked
    }

    /// Deletes the link's rows and secrets: each row's addresses and its token, if readable.
    fn forget(&self, environment_id: &str) -> Result<Vec<(String, Option<String>)>> {
        if !self.has_store() {
            return Ok(Vec::new());
        }
        let conn = self.open()?;
        let mut statement = conn
            .prepare("DELETE FROM peer_environment_links WHERE environment_id = ?1 RETURNING secret_name, urls_json")
            .map_err(PeerLinkError::storage)?;
        let rows = statement
            .query_map([environment_id], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })
            .map_err(PeerLinkError::storage)?
            .collect::<rusqlite::Result<Vec<_>>>()
            .map_err(PeerLinkError::storage)?;
        let mut forgotten = Vec::new();
        for (name, urls_json) in rows {
            // An unreadable token only skips the revocation; the secret is still removed.
            let token = self
                .read_secret(&name)
                .ok()
                .flatten()
                .map(|bytes| String::from_utf8_lossy(&bytes).into_owned());
            self.remove_secret(&name)?;
            forgotten.push((urls_json, token));
        }
        Ok(forgotten)
    }

    // --- the peer ------------------------------------------------------------

    async fn fetch_descriptor(&self, origin: &str) -> std::result::Result<Descriptor, ProbeError> {
        let response = self
            .http
            .get(format!("{origin}/.well-known/t3/environment"))
            .header("accept", "application/json")
            .send()
            .await
            .map_err(ProbeError::Http)?;
        if !response.status().is_success() {
            return Err(ProbeError::Shape);
        }
        let body: Value = response.json().await.map_err(|_| ProbeError::Shape)?;
        let text = |k: &str| body[k].as_str().map(str::to_owned);
        let (Some(environment_id), Some(label)) = (text("environmentId"), text("label")) else {
            return Err(ProbeError::Shape);
        };
        let capabilities = &body["capabilities"];
        let non_empty = |v: &Value| v.as_str().is_some_and(|s| !s.trim().is_empty());
        if !capabilities.is_object()
            || !non_empty(&body["serverVersion"])
            || !non_empty(&body["label"])
            || !body["platform"]["os"].is_string()
            || !body["platform"]["arch"].is_string()
        {
            return Err(ProbeError::Shape);
        }
        Ok(Descriptor {
            environment_id,
            label,
            link_limits_hold: capabilities["mcpModeLimitHeader"] == true
                && capabilities["linkFence"] == true,
        })
    }

    /// The environment answering at `origin`, or why nothing there answers as T3 Code.
    pub(crate) async fn probe(&self, origin: &str) -> Result<Descriptor> {
        let timeout = probe_timeout(origin);
        let attempt = async {
            match self.fetch_descriptor(origin).await {
                // A brief network drop must not list a live peer as down.
                Err(ProbeError::Http(e)) if certificate_code(&e).is_none() => {
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    self.fetch_descriptor(origin).await
                }
                Err(ProbeError::Shape) => {
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    self.fetch_descriptor(origin).await
                }
                other => other,
            }
        };
        match tokio::time::timeout(timeout, attempt).await {
            Ok(Ok(descriptor)) => Ok(descriptor),
            Err(_) => Err(PeerLinkError::unreachable(format!(
                "{origin} did not answer within {} seconds.",
                timeout.as_secs()
            ))),
            Ok(Err(ProbeError::Http(e))) => {
                if let Some(code) = certificate_code(&e) {
                    return Err(PeerLinkError::unreachable(format!(
                        "{origin} presented a certificate this machine rejected ({code})."
                    )));
                }
                if e.is_decode() || e.is_status() {
                    return Err(not_t3(origin));
                }
                Err(PeerLinkError::unreachable(match connection_code(&e) {
                    Some(code) => format!("{origin} did not answer ({code})."),
                    None => format!("{origin} did not answer."),
                }))
            }
            Ok(Err(ProbeError::Shape)) => Err(not_t3(origin)),
        }
    }

    /// Whether `url` answers as `environment_id`.
    async fn answers_as(&self, url: &str, environment_id: &str) -> Option<Descriptor> {
        self.probe(url)
            .await
            .ok()
            .filter(|d| d.environment_id == environment_id)
    }

    /// The first of `urls` that answers as `environment_id`; the addresses race.
    async fn first_answering(
        &self,
        urls: &[String],
        environment_id: &str,
    ) -> Option<(String, Descriptor)> {
        if urls.is_empty() {
            return None;
        }
        let attempts: Vec<BoxFuture<'_, std::result::Result<(String, Descriptor), ()>>> = urls
            .iter()
            .map(|url| {
                async move {
                    self.answers_as(url, environment_id)
                        .await
                        .map(|d| (url.clone(), d))
                        .ok_or(())
                }
                .boxed()
            })
            .collect();
        select_ok(attempts).await.ok().map(|(found, _)| found)
    }

    /// Signs in to the peer's `/mcp` with a pairing code: register, decide, exchange.
    async fn sign_in(
        &self,
        origin: &str,
        input: &LinkInput,
        client_name: &str,
    ) -> Result<(String, i64)> {
        let stopped =
            || PeerLinkError::unreachable(format!("{origin} stopped answering while linking."));
        let incompatible = || {
            PeerLinkError::new(
                "incompatible",
                format!("{origin} answered in a shape this version does not understand. Update both environments."),
            )
        };
        let send = |request: reqwest::RequestBuilder| async move {
            let response = request.send().await.map_err(|_| stopped())?;
            let status = response.status().as_u16();
            let body = response.text().await.map_err(|_| stopped())?;
            Ok::<_, PeerLinkError>((status, body))
        };
        let decode = |body: &str| serde_json::from_str::<Value>(body).map_err(|_| incompatible());

        let (status, body) = send(self.http.post(format!("{origin}/oauth/mcp/register")).json(
            &json!({
                "client_name": client_name,
                "redirect_uris": [LINK_REDIRECT_URI],
                "software_id": SOFTWARE_ID,
            }),
        ))
        .await?;
        let registered = match status {
            200 | 201 => decode(&body)?,
            400 => {
                let error = decode(&body)?;
                let description = error["error_description"]
                    .as_str()
                    .ok_or_else(incompatible)?;
                return Err(PeerLinkError::new(
                    "pairing_rejected",
                    format!("The peer refused to register this environment: {description}"),
                ));
            }
            _ => return Err(stopped()),
        };
        // `AuthMcpRegisteredClient`, every key of which the reference decodes.
        let only = |key: &str, value: &str| {
            registered[key]
                .as_array()
                .is_some_and(|items| items.iter().all(|v| v == value))
        };
        if !registered["client_name"].is_string()
            || !registered["redirect_uris"]
                .as_array()
                .is_some_and(|uris| uris.iter().all(Value::is_string))
            || !only("grant_types", "authorization_code")
            || !only("response_types", "code")
            || registered["token_endpoint_auth_method"] != "none"
        {
            return Err(incompatible());
        }
        let client_id = registered["client_id"]
            .as_str()
            .ok_or_else(incompatible)?
            .to_owned();

        let verifier = b64(&crate::platform::random_bytes::<32>());
        let challenge = b64(&Sha256::digest(verifier.as_bytes()));
        let state = b64(&crate::platform::random_bytes::<16>());
        let resource = format!("{origin}/mcp");
        let (status, body) = send(self.http.post(format!("{origin}/oauth/mcp/decision")).json(&json!({
            "authorization": {
                "response_type": "code",
                "client_id": client_id,
                "redirect_uri": LINK_REDIRECT_URI,
                "code_challenge": challenge,
                "code_challenge_method": "S256",
                "state": state,
                "resource": resource,
            },
            "decision": {"_tag": "pairing-code", "access": input.access, "code": input.pairing_code},
        })))
        .await?;
        let decided = match status {
            200 | 201 => decode(&body)?,
            400 => {
                let error = decode(&body)?;
                let message = error["message"]
                    .as_str()
                    .filter(|_| error["_tag"] == "AuthMcpApprovalError");
                return Err(PeerLinkError::new(
                    "pairing_rejected",
                    message.ok_or_else(incompatible)?,
                ));
            }
            _ => return Err(stopped()),
        };
        let redirect_to = decided["redirectTo"].as_str().ok_or_else(incompatible)?;
        let Ok(redirect) = url::Url::parse(redirect_to) else {
            return Err(PeerLinkError::new(
                "incompatible",
                format!("{origin} answered the sign-in with an address this version cannot read. Update both environments."),
            ));
        };
        let query = |key: &str| {
            redirect
                .query_pairs()
                .find(|(k, _)| k == key)
                .map(|(_, v)| v.into_owned())
        };
        let code =
            match query("code") {
                Some(code) if query("state").as_deref() == Some(state.as_str()) => code,
                _ => return Err(PeerLinkError::new(
                    "pairing_rejected",
                    query("error_description").unwrap_or_else(|| {
                        "The peer did not approve the link. Check the pairing code and try again."
                            .into()
                    }),
                )),
            };

        let (status, body) = send(self.http.post(format!("{origin}/oauth/mcp/token")).form(&[
            ("grant_type", "authorization_code"),
            ("code", code.as_str()),
            ("redirect_uri", LINK_REDIRECT_URI),
            ("client_id", client_id.as_str()),
            ("code_verifier", verifier.as_str()),
            ("resource", resource.as_str()),
        ]))
        .await?;
        let issued = match status {
            200 | 201 => decode(&body)?,
            400 => {
                let error = decode(&body)?;
                let description = error["error_description"]
                    .as_str()
                    .ok_or_else(incompatible)?;
                return Err(PeerLinkError::new(
                    "pairing_rejected",
                    format!("The peer refused the sign-in: {description}"),
                ));
            }
            _ => return Err(stopped()),
        };
        match (
            issued["access_token"].as_str(),
            issued["expires_in"].as_f64(),
            issued["token_type"].as_str(),
        ) {
            (Some(token), Some(expires_in), Some("Bearer")) if issued["scope"].is_string() => {
                Ok((token.to_owned(), (expires_in * 1000.0) as i64))
            }
            _ => Err(incompatible()),
        }
    }

    /// The addresses that answer as `environment_id`, the first one's answer, and every origin kept.
    async fn answering_as(
        &self,
        urls: &[String],
        environment_id: &str,
        name: &str,
    ) -> Result<(String, Descriptor, Vec<String>)> {
        let checks = futures_util::future::join_all(urls.iter().map(|url| async move {
            let origin = peer_origin(url)?;
            let host = url::Url::parse(&origin)
                .ok()
                .and_then(|u| u.host_str().map(str::to_owned))
                .unwrap_or_default();
            // Loopback is this machine, where anything may answer as the one picked.
            if is_loopback_host(&host) {
                return Err(PeerLinkError::unreachable(format!(
                    "{origin} is this machine, not {name}."
                )));
            }
            let descriptor = self.probe(&origin).await?;
            if descriptor.environment_id != environment_id {
                return Err(PeerLinkError::unreachable(format!(
                    "{origin} answered as another environment, {}.",
                    descriptor.label
                )));
            }
            Ok((origin, descriptor))
        }))
        .await;
        let mut origins: Vec<String> = Vec::new();
        let mut first = None;
        let mut reasons = Vec::new();
        for check in checks {
            match check {
                Ok((origin, descriptor)) => {
                    if first.is_none() {
                        first = Some((origin.clone(), descriptor));
                    }
                    if !origins.contains(&origin) {
                        origins.push(origin);
                    }
                }
                Err(e) => reasons.push(e.message),
            }
        }
        match first {
            Some((primary, descriptor)) => Ok((primary, descriptor, origins)),
            None => Err(PeerLinkError::unreachable(format!(
                "This environment could not reach {name}: {}",
                reasons.join(" ")
            ))),
        }
    }

    /// Links the peer at `input.url` (`PeerLinks.link`).
    pub(crate) async fn link(
        &self,
        enabled: bool,
        own: &SelfDescriptor,
        input: &LinkInput,
    ) -> Result<PeerLink> {
        if !enabled {
            return Err(PeerLinkError::new("disabled", LINKED_ENVIRONMENTS_OFF));
        }
        let urls: Vec<String> = std::iter::once(input.url.clone())
            .chain(input.alternate_urls.iter().cloned())
            .collect();
        let (primary, peer, origins) = match &input.expected_environment_id {
            Some(expected) => {
                let name = input.expected_label.as_deref().unwrap_or(expected);
                self.answering_as(&urls, expected, name).await?
            }
            None => {
                let origins = urls
                    .iter()
                    .map(|u| peer_origin(u))
                    .collect::<Result<Vec<_>>>()?;
                let primary = origins[0].clone();
                let peer = self.probe(&primary).await?;
                (primary, peer, origins)
            }
        };
        // An address alone renews a link only where the link already reaches it.
        if input.expected_environment_id.is_none() {
            if let Some(existing) = self.get(&peer.environment_id)? {
                if !existing.expired(crate::platform::now_ms()) && !existing.urls.contains(&primary)
                {
                    return Err(PeerLinkError::new(
                        "pairing_rejected",
                        format!(
                            "{primary} answers as {label}, which is already linked at another address. If {label} moved, forget that link first.",
                            label = existing.label
                        ),
                    ));
                }
            }
        }
        if peer.environment_id == own.environment_id {
            return Err(PeerLinkError::new(
                "self",
                "That address is this environment.",
            ));
        }
        if !peer.link_limits_hold {
            return Err(PeerLinkError::new(
                "incompatible",
                format!(
                    "{} runs a T3 Code version that cannot keep a linked agent's limits. Update it, then link again.",
                    peer.label
                ),
            ));
        }
        let (token, expires_in_ms) = self
            .sign_in(&primary, input, &format!("T3 Code · {}", own.label))
            .await?;
        let now = crate::platform::now_ms();
        let linked = PeerLink {
            environment_id: peer.environment_id.clone(),
            label: peer.label.clone(),
            urls: origins,
            access: input.access.clone(),
            linked_at: iso(now),
            expires_at: iso(now + expires_in_ms),
            last_reached_at: Some(iso(now)),
            last_error: None,
        };
        // The token goes under a name of its own before the row points at it.
        let secret_name = format!(
            "peer-link-{}",
            hex::encode(crate::platform::random_bytes::<16>())
        );
        self.write_secret(&secret_name, token.as_bytes())?;
        let stored = (|| -> Result<Option<Row>> {
            let previous = self.read_row(&linked.environment_id)?;
            let conn = self.open()?;
            conn.execute(
                "INSERT INTO peer_environment_links (
                   environment_id, secret_name, label, urls_json, access, linked_at, expires_at,
                   last_reached_at, last_error, fenced
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, NULL, 1)
                 ON CONFLICT (environment_id) DO UPDATE SET
                   secret_name = excluded.secret_name,
                   fenced = excluded.fenced,
                   label = excluded.label,
                   urls_json = excluded.urls_json,
                   access = excluded.access,
                   linked_at = excluded.linked_at,
                   expires_at = excluded.expires_at,
                   last_reached_at = excluded.last_reached_at,
                   last_error = NULL",
                params![
                    linked.environment_id,
                    secret_name,
                    linked.label,
                    serde_json::to_string(&linked.urls).unwrap_or_else(|_| "[]".into()),
                    linked.access,
                    linked.linked_at,
                    linked.expires_at,
                    linked.linked_at,
                ],
            )
            .map_err(PeerLinkError::storage)?;
            Ok(previous)
        })();
        let previous = match stored {
            Ok(previous) => previous,
            Err(e) => {
                let _ = self.remove_secret(&secret_name);
                return Err(e);
            }
        };
        if let Some(previous) = previous.filter(|p| p.secret_name != secret_name) {
            let _ = self.remove_secret(&previous.secret_name);
        }
        Ok(linked)
    }

    /// Every link with whether it can be used now (`PeerLinkSummary`).
    pub(crate) async fn list(&self) -> Result<Vec<(PeerLink, &'static str)>> {
        let links = self
            .read_rows()?
            .into_iter()
            .map(Row::into_link)
            .collect::<Result<Vec<_>>>()?;
        let now = crate::platform::now_ms();
        Ok(
            futures_util::future::join_all(links.into_iter().map(|link| async move {
                if link.expired(now) {
                    return (link, "expired");
                }
                let answered = tokio::time::timeout(
                    LIST_PROBE_TIMEOUT,
                    self.first_answering(&link.urls, &link.environment_id),
                )
                .await
                .ok()
                .flatten();
                match answered {
                    Some((_, peer)) => (self.with_label(link, &peer.label), "reachable"),
                    None => (link, "unreachable"),
                }
            }))
            .await,
        )
    }
    /// An address that answers as the linked peer, with its token, ready to call.
    pub(crate) async fn resolve(&self, environment_id: &str) -> Result<Resolved> {
        let not_linked = || {
            PeerLinkError::new(
                "unknown_link",
                format!("This environment is not linked to {environment_id}. t3_environment_links lists the linked ones."),
            )
        };
        let row = self.read_row(environment_id)?.ok_or_else(not_linked)?;
        let secret_name = row.secret_name.clone();
        let link = row.into_link()?;
        if link.expired(crate::platform::now_ms()) {
            return Err(PeerLinkError::new(
                "expired",
                format!(
                    "The link to {label} expired. Link it again with a new pairing code from {label}.",
                    label = link.label
                ),
            ));
        }
        let Some(token) = self.read_secret(&secret_name)? else {
            // An unlink between the two reads removes the row before its secret.
            if self.read_row(environment_id)?.is_none() {
                return Err(not_linked());
            }
            return Err(PeerLinkError::new(
                "unknown_link",
                format!(
                    "The link to {} lost its credential. Link it again.",
                    link.label
                ),
            ));
        };
        let Some((url, peer)) = self.first_answering(&link.urls, environment_id).await else {
            return Err(PeerLinkError::unreachable(format!(
                "{} did not answer at {}.",
                link.label,
                link.urls.join(", ")
            )));
        };
        if !peer.link_limits_hold {
            return Err(PeerLinkError::new(
                "incompatible",
                format!(
                    "{} runs a T3 Code version that cannot keep a linked agent's limits. Update it, then link again.",
                    link.label
                ),
            ));
        }
        // A peer's label follows its machine name.
        self.with_label(link, &peer.label);
        Ok(Resolved {
            url,
            token: String::from_utf8_lossy(&token).into_owned(),
        })
    }
}

fn not_t3(origin: &str) -> PeerLinkError {
    PeerLinkError::new(
        "not_a_t3_environment",
        format!("{origin} answered, but not as a T3 Code environment this version understands."),
    )
}

fn b64(bytes: &[u8]) -> String {
    use base64::Engine;
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes)
}

const COLUMNS: &str = "environment_id, secret_name, label, urls_json, access, linked_at, expires_at, last_reached_at, last_error, fenced";

struct Row {
    environment_id: String,
    secret_name: String,
    label: String,
    urls_json: String,
    access: String,
    linked_at: String,
    expires_at: String,
    last_reached_at: Option<String>,
    last_error: Option<String>,
    fenced: i64,
}

impl Row {
    fn from_sql(row: &rusqlite::Row<'_>) -> rusqlite::Result<Self> {
        Ok(Self {
            environment_id: row.get(0)?,
            secret_name: row.get(1)?,
            label: row.get(2)?,
            urls_json: row.get(3)?,
            access: row.get(4)?,
            linked_at: row.get(5)?,
            expires_at: row.get(6)?,
            last_reached_at: row.get(7)?,
            last_error: row.get(8)?,
            fenced: row.get(9)?,
        })
    }

    fn into_link(self) -> Result<PeerLink> {
        let urls: Vec<String> =
            serde_json::from_str(&self.urls_json).map_err(PeerLinkError::storage)?;
        if !ACCESS.contains(&self.access.as_str()) {
            return Err(PeerLinkError::storage(format!(
                "unknown access {}",
                self.access
            )));
        }
        let normal = |text: &str| -> Result<String> {
            iso_ms(text)
                .map(iso)
                .ok_or_else(|| PeerLinkError::storage(format!("invalid date {text}")))
        };
        Ok(PeerLink {
            environment_id: self.environment_id,
            label: self.label,
            urls,
            access: self.access,
            linked_at: normal(&self.linked_at)?,
            // Its token predates link fencing, so the peer would not fence its
            // work: like an expired link, only a new pairing code brings it back.
            expires_at: normal(if self.fenced == 1 {
                &self.expires_at
            } else {
                &self.linked_at
            })?,
            last_reached_at: self.last_reached_at.as_deref().map(normal).transpose()?,
            last_error: self.last_error,
        })
    }
}

/// Migrations 070 and 071 of the reference, which both servers may run.
fn ensure_table(conn: &Connection) -> rusqlite::Result<()> {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS peer_environment_links (
           environment_id TEXT PRIMARY KEY,
           secret_name TEXT NOT NULL,
           label TEXT NOT NULL,
           urls_json TEXT NOT NULL,
           access TEXT NOT NULL,
           linked_at TEXT NOT NULL,
           expires_at TEXT NOT NULL,
           last_reached_at TEXT,
           last_error TEXT
         )",
    )?;
    let fenced: bool = conn
        .prepare("PRAGMA table_info(peer_environment_links)")?
        .query_map([], |row| row.get::<_, String>(1))?
        .collect::<rusqlite::Result<Vec<_>>>()?
        .iter()
        .any(|name| name == "fenced");
    if !fenced {
        conn.execute(
            "ALTER TABLE peer_environment_links ADD COLUMN fenced INTEGER NOT NULL DEFAULT 0",
            [],
        )?;
    }
    Ok(())
}

/// Moves links kept in `peer-links.sqlite` into `statev2.sqlite`, keeping any
/// the TypeScript server holds for the same environment, then removes the file.
fn adopt_own_links(conn: &Connection, own: &Path) -> rusqlite::Result<()> {
    conn.execute("ATTACH DATABASE ?1 AS own", [own.to_string_lossy()])?;
    let moved = (|| -> rusqlite::Result<()> {
        let has_table = conn
            .query_row(
                "SELECT 1 FROM own.sqlite_master WHERE type = 'table' AND name = 'peer_environment_links'",
                [],
                |_| Ok(()),
            )
            .optional()?
            .is_some();
        if has_table {
            conn.execute(
                &format!(
                    "INSERT OR IGNORE INTO main.peer_environment_links ({COLUMNS})
                     SELECT {COLUMNS} FROM own.peer_environment_links"
                ),
                [],
            )?;
        }
        Ok(())
    })();
    conn.execute("DETACH DATABASE own", [])?;
    moved?;
    let _ = std::fs::remove_file(own);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn links() -> (tempfile::TempDir, PeerLinks) {
        let dir = tempfile::tempdir().unwrap();
        let links = PeerLinks::new(dir.path(), &dir.path().join("secrets"));
        (dir, links)
    }

    fn insert(links: &PeerLinks, id: &str, secret: &str, fenced: i64, expires: &str) {
        links
            .open()
            .unwrap()
            .execute(
                &format!("INSERT INTO peer_environment_links ({COLUMNS}) VALUES (?1, ?2, 'Peer', '[\"http://127.0.0.1:1\"]', 'auto', '2026-01-01T00:00:00.000Z', ?3, NULL, NULL, ?4)"),
                params![id, secret, expires, fenced],
            )
            .unwrap();
    }

    #[test]
    fn origins_refuse_plain_http_off_loopback_and_tailnet() {
        assert_eq!(
            peer_origin("http://127.0.0.1:3773/x").unwrap(),
            "http://127.0.0.1:3773"
        );
        assert_eq!(
            peer_origin("http://box.tail1.ts.net").unwrap(),
            "http://box.tail1.ts.net"
        );
        assert_eq!(
            peer_origin("https://example.com/a?b").unwrap(),
            "https://example.com"
        );
        assert_eq!(
            peer_origin("http://example.com").unwrap_err().reason,
            "unreachable"
        );
        assert_eq!(
            peer_origin("ftp://example.com").unwrap_err().message,
            "That address is not an http or https address."
        );
        assert!(
            is_loopback_host("[::1]")
                && is_loopback_host("127.4.0.1")
                && !is_loopback_host("10.0.0.1")
        );
    }

    #[test]
    fn a_link_from_before_fencing_reads_as_expired() {
        let (_dir, links) = links();
        insert(
            &links,
            "env-old",
            "peer-link-old",
            0,
            "2099-01-01T00:00:00.000Z",
        );
        insert(
            &links,
            "env-new",
            "peer-link-new",
            1,
            "2099-01-01T00:00:00.000Z",
        );
        let old = links.get("env-old").unwrap().unwrap();
        assert_eq!(old.expires_at, old.linked_at);
        assert!(old.expired(crate::platform::now_ms()));
        assert!(!links
            .get("env-new")
            .unwrap()
            .unwrap()
            .expired(crate::platform::now_ms()));
    }

    #[test]
    fn unlinking_removes_the_row_and_its_token() {
        let (dir, links) = links();
        links.write_secret("peer-link-abc", b"token").unwrap();
        insert(
            &links,
            "env-a",
            "peer-link-abc",
            1,
            "2099-01-01T00:00:00.000Z",
        );
        assert!(links.unlink("env-a").unwrap());
        assert!(!dir.path().join("secrets/peer-link-abc.bin").exists());
        assert!(!links.unlink("env-a").unwrap());
    }

    #[test]
    fn links_kept_here_move_into_the_typescript_database() {
        let (dir, links) = links();
        insert(
            &links,
            "env-a",
            "peer-link-a",
            1,
            "2099-01-01T00:00:00.000Z",
        );
        Connection::open(dir.path().join(STATE_DB))
            .unwrap()
            .execute_batch("CREATE TABLE auth_sessions (session_id TEXT PRIMARY KEY)")
            .unwrap();
        assert_eq!(links.get("env-a").unwrap().unwrap().environment_id, "env-a");
        assert!(!dir.path().join(OWN_DB).exists());
        let count: i64 = Connection::open(dir.path().join(STATE_DB))
            .unwrap()
            .query_row("SELECT COUNT(*) FROM peer_environment_links", [], |r| {
                r.get(0)
            })
            .unwrap();
        assert_eq!(count, 1);
    }

    #[test]
    fn link_input_decodes_the_rpc_payload() {
        let input = LinkInput::from_json(&json!({"url":" http://127.0.0.1:1 ","pairingCode":"ABC","access":"auto","alternateUrls":["https://x"]})).unwrap();
        assert_eq!(input.url, "http://127.0.0.1:1");
        assert_eq!(input.alternate_urls, ["https://x"]);
        assert!(
            LinkInput::from_json(&json!({"url":"x","pairingCode":"y","access":"root"})).is_err()
        );
    }
}
