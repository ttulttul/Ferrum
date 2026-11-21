//! The T3 Connect calls behind the managed tunnel: registering the stored tunnel for this
//! server's origin, provisioning a CLI-desired link at startup, recovering a rejected tunnel
//! and releasing the tunnel when the server stops.
//!
//! Ports, from the pinned reference, the parts of `CloudLink` (`apps/server/src/cloud/
//! CloudLink.ts`) that the server's cloud link fiber (`server.ts`, `layerCloudDesiredLinkReconcile`)
//! runs, with `managedTunnelStartup.ts`, the non-interactive half of `CliTokenManager`
//! (`getExisting`: read and refresh the stored CLI credential), `environmentKeys.ts`,
//! `CliState.ts`, `relayResponse.ts`, `publicConfig.ts` and `updateHandoff.ts`.
//!
//! Everything lives in the reference's secret store (`userdata/secrets/<name>.bin`) in the
//! reference's encodings, so the TypeScript server and this one can take turns on one T3 home:
//! a server that stops releases the tunnel it provisioned and the next one provisions its own.
//!
//! Requests carry the CLI's OAuth access token (`cloud-cli-oauth-token`) or the environment
//! credential (`cloud-relay-environment-credential`), and proofs signed with the environment key
//! (`cloud-link-ed25519-key-pair`). Signing in (`t3 connect login`) stays with the CLI: without a
//! stored credential nothing is provisioned. The T3 Connect and Clerk addresses come from
//! `T3CODE_RELAY_URL`, `T3CODE_CLERK_PUBLISHABLE_KEY` and `T3CODE_CLERK_CLI_OAUTH_CLIENT_ID`,
//! or, as the reference's published build bakes them in, from the same names prefixed
//! `T3CODE_BUILD_` at compile time. Like Node, HTTPS trusts `NODE_EXTRA_CA_CERTS` besides the
//! bundled roots.
//!
//! Not ported: `watchManagedTunnelOrigin` (re-registering when cloudflared's ingress names
//! another origin), relay request tracing, and the agent awareness catch-up after a relink.

use crate::managed_endpoint::{
    read_secret, read_secret_string, remove_secret, write_secret, LocalOrigin,
    ManagedEndpointRuntime, RuntimeConfig,
};
use base64::{
    engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD},
    Engine,
};
use ed25519_dalek::{
    pkcs8::{DecodePrivateKey, DecodePublicKey, EncodePrivateKey, EncodePublicKey},
    Signer, SigningKey, VerifyingKey,
};
use serde_json::{json, Map, Value};
use std::{
    future::Future,
    path::{Path, PathBuf},
    pin::Pin,
    sync::Arc,
    time::{Duration, Instant},
};

pub const CLOUD_ENDPOINT_RUNTIME_CONFIG: &str = "cloud-endpoint-runtime-config";
pub const CLOUD_ENDPOINT_CONFIRMED_ORIGIN: &str = "cloud-endpoint-confirmed-origin";
pub const CLOUD_CLI_DESIRED_LINK: &str = "cloud-cli-desired-link";
pub const CLOUD_CLI_OAUTH_TOKEN: &str = "cloud-cli-oauth-token";
pub const CLOUD_LINKED_USER_ID: &str = "cloud-linked-user-id";
pub const CLOUD_MINT_PUBLIC_KEY: &str = "cloud-mint-ed25519-public-key";
pub const CLOUD_LINK_REVOKED: &str = "cloud-link-revoked";
pub const CLOUD_LINK_KEY_PAIR: &str = "cloud-link-ed25519-key-pair";
const CLOUD_LINK_PRIVATE_KEY: &str = "cloud-link-ed25519-private-key";
const CLOUD_LINK_PUBLIC_KEY: &str = "cloud-link-ed25519-public-key";
pub const RELAY_URL_SECRET: &str = "cloud-relay-url";
pub const RELAY_ISSUER_SECRET: &str = "cloud-relay-issuer";
pub const RELAY_ENVIRONMENT_CREDENTIAL_SECRET: &str = "cloud-relay-environment-credential";
const RELAY_LINK_PROOF_TYP: &str = "t3-env-link+jwt";
const RELAY_MANAGED_TUNNEL_RECOVERY_TYP: &str = "t3-env-managed-tunnel-recovery+jwt";
const DESKTOP_UPDATE_RESTART_MARKER_FILE: &str = "desktop-update-restart";
const SERVICE_STATE_FILE: &str = "service-state.json";
const SERVICE_STOP_MARKER_FILE: &str = ".service-stopping";

const RELAY_REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
const MANAGED_ENDPOINT_PROVISION_REQUEST_TIMEOUT: Duration = Duration::from_secs(2 * 60);
const OAUTH_REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
const CLOUD_CLI_OAUTH_REFRESH_EARLY_MS: f64 = 5.0 * 60_000.0;
const RELEASE_ON_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(10);
const DESKTOP_UPDATE_RESTART_MARKER_TTL: Duration = Duration::from_secs(60);
const MAX_RELAY_RETRY_AFTER: Duration = Duration::from_secs(15 * 60);
const MANAGED_TUNNEL_RECOVERY_COOLDOWN: Duration = Duration::from_secs(2 * 60);
const MANAGED_TUNNEL_RECOVERY_MAX_COOLDOWN: Duration = Duration::from_secs(30 * 60);
const MANAGED_TUNNEL_RECOVERY_SETTLED_AFTER: Duration = Duration::from_secs(10 * 60);

/// The reference's retry and backoff constants; tests shorten them.
#[derive(Clone, Debug)]
pub struct Timings {
    /// `MANAGED_TUNNEL_FIRST_REGISTRATION_JITTER`: the first registration waits a random part.
    pub first_registration_jitter: Duration,
    /// `MANAGED_TUNNEL_REGISTRATION_RETRY_WINDOW`.
    pub retry_window: Duration,
    /// `relayRetrySchedule`: exponential from this base, doubling.
    pub retry_base: Duration,
    /// `RELAY_RETRY_MAX_DELAY`.
    pub retry_max: Duration,
    /// The runtime-failure retry in `activateManagedTunnelWithRetry`: up to 30 s.
    pub activation_retry_max: Duration,
}

impl Default for Timings {
    fn default() -> Self {
        Self {
            first_registration_jitter: Duration::from_secs(30),
            retry_window: Duration::from_secs(10 * 60),
            retry_base: Duration::from_secs(1),
            retry_max: Duration::from_secs(5 * 60),
            activation_retry_max: Duration::from_secs(30),
        }
    }
}

/// `publicConfig.ts`: where T3 Connect and the CLI's Clerk OAuth client are.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PublicConfig {
    pub relay_url_env: Option<String>,
    pub relay_url_build: Option<String>,
    pub clerk_publishable_key: Option<String>,
    pub clerk_cli_oauth_client_id: Option<String>,
}

fn non_empty(value: Option<String>) -> Option<String> {
    value.filter(|value| !value.is_empty())
}

fn trimmed(value: Option<String>) -> Option<String> {
    value
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
}

impl PublicConfig {
    pub fn from_env() -> Self {
        Self {
            relay_url_env: non_empty(std::env::var("T3CODE_RELAY_URL").ok()),
            relay_url_build: option_env!("T3CODE_BUILD_RELAY_URL")
                .and_then(normalize_secure_relay_url),
            clerk_publishable_key: trimmed(std::env::var("T3CODE_CLERK_PUBLISHABLE_KEY").ok())
                .or_else(|| {
                    trimmed(option_env!("T3CODE_BUILD_CLERK_PUBLISHABLE_KEY").map(str::to_owned))
                }),
            clerk_cli_oauth_client_id: trimmed(
                std::env::var("T3CODE_CLERK_CLI_OAUTH_CLIENT_ID").ok(),
            )
            .or_else(|| {
                trimmed(option_env!("T3CODE_BUILD_CLERK_CLI_OAUTH_CLIENT_ID").map(str::to_owned))
            }),
        }
    }

    /// `hasCloudPublicConfig`.
    pub fn is_complete(&self) -> bool {
        self.relay_url_env
            .as_deref()
            .and_then(normalize_secure_relay_url)
            .or_else(|| self.relay_url_build.clone())
            .is_some()
            && self.clerk_publishable_key.is_some()
            && self.clerk_cli_oauth_client_id.is_some()
    }

    /// `relayUrlConfig`: the runtime value when set (and then it must be valid), else the build's.
    fn relay_url(&self) -> Option<String> {
        match &self.relay_url_env {
            Some(value) => normalize_secure_relay_url(value),
            None => self.relay_url_build.clone(),
        }
    }

    /// `cloudCliOAuthConfig`: the token endpoint and client id.
    fn oauth(&self) -> Option<(String, String)> {
        let key = self.clerk_publishable_key.as_deref()?;
        let client_id = self.clerk_cli_oauth_client_id.clone()?;
        Some((
            format!("{}/oauth/token", clerk_frontend_api_url(key)?),
            client_id,
        ))
    }
}

/// `normalizeSecureRelayUrl`: an HTTPS origin with nothing after it but slashes.
pub fn normalize_secure_relay_url(value: &str) -> Option<String> {
    let url = url::Url::parse(value.trim()).ok()?;
    (url.scheme() == "https"
        && url.username().is_empty()
        && url.password().is_none()
        && url.query().is_none()
        && url.fragment().is_none()
        && !url.path().is_empty()
        && url.path().chars().all(|c| c == '/'))
    .then(|| url.origin().ascii_serialization())
}

/// `clerkFrontendApiUrlFromPublishableKey`.
fn clerk_frontend_api_url(publishable_key: &str) -> Option<String> {
    let encoded = publishable_key
        .split('_')
        .skip(2)
        .collect::<Vec<_>>()
        .join("_");
    let decoded = String::from_utf8(STANDARD.decode(encoded).ok()?).ok()?;
    let frontend_api = decoded.strip_suffix('$').unwrap_or(&decoded);
    if frontend_api.is_empty() || frontend_api.contains('/') {
        return None;
    }
    let url = format!("https://{frontend_api}");
    url::Url::parse(&url).ok()?;
    Some(url)
}

/// `ManagedTunnelRegistrationResult`.
#[derive(Clone, Debug, PartialEq)]
pub enum Registration {
    NotLinked,
    Ready(Value),
    Unavailable,
    Superseded,
    Revoked,
    RecoveryRequired(RuntimeConfig),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Rejection {
    Unauthorized,
    Forbidden,
    Rejected,
    Unavailable,
}

/// The `CloudLink`, `RelayRequestError` and `CloudCliTokenManager` failures these calls have.
#[derive(Clone, Debug, PartialEq)]
pub enum Error {
    Relay {
        rejection: Rejection,
        description: String,
        retry_after: Option<Duration>,
    },
    AuthorizationMissing,
    /// `CloudCliCredentialRefreshError`; `requires_login` for a permanent 4xx.
    Refresh {
        requires_login: bool,
        detail: String,
    },
    EndpointUnavailable(Value),
    AccountMismatch,
    RelayConfigInvalid(&'static str),
    OriginInvalid,
    Internal(String),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Relay { description, .. } => f.write_str(description),
            Self::AuthorizationMissing => f.write_str("T3 Connect CLI authorization is missing."),
            Self::Refresh { requires_login: true, detail } => write!(
                f,
                "Could not refresh the T3 Connect CLI credential{detail}. Run `t3 connect login` to authorize again."
            ),
            Self::Refresh { .. } => f.write_str("Could not refresh the T3 Connect CLI credential."),
            Self::EndpointUnavailable(status) => {
                write!(f, "The managed endpoint runtime did not start: {status}")
            }
            Self::AccountMismatch => {
                f.write_str("This environment is linked to another T3 Connect account.")
            }
            Self::RelayConfigInvalid(field) => write!(f, "Invalid relay configuration ({field})."),
            Self::OriginInvalid => f.write_str("Invalid local origin."),
            Self::Internal(operation) => write!(f, "T3 Connect operation failed: {operation}"),
        }
    }
}

impl Error {
    /// `shouldRetryCloudLink`.
    pub fn should_retry(&self) -> bool {
        match self {
            Self::Relay { rejection, .. } => *rejection == Rejection::Unavailable,
            Self::AuthorizationMissing
            | Self::AccountMismatch
            | Self::RelayConfigInvalid(_)
            | Self::OriginInvalid => false,
            Self::Refresh { requires_login, .. } => !requires_login,
            Self::EndpointUnavailable(_) | Self::Internal(_) => true,
        }
    }

    /// `isRelayUnavailable`.
    fn relay_unavailable(&self) -> bool {
        matches!(
            self,
            Self::Relay {
                rejection: Rejection::Unavailable,
                ..
            }
        )
    }

    fn retry_after(&self) -> Option<Duration> {
        match self {
            Self::Relay { retry_after, .. } => *retry_after,
            _ => None,
        }
    }

    /// The registration and recovery predicate: retryable, but not a connector that failed.
    fn retry_registration(&self) -> bool {
        self.should_retry() && !matches!(self, Self::EndpointUnavailable(_))
    }

    /// `relayRequestError` for a failure that is not the relay's answer.
    fn relay_transport(transport: bool) -> Self {
        Self::Relay {
            rejection: Rejection::Unavailable,
            description: format!(
                "Could not complete the T3 Connect relay request. {} Check this machine's network connection and relay availability, then retry.",
                if transport {
                    "The relay request failed (TransportError)."
                } else {
                    "The relay returned an unexpected response."
                }
            ),
            retry_after: None,
        }
    }
}

type Descriptor = Arc<dyn Fn() -> Pin<Box<dyn Future<Output = Value> + Send>> + Send + Sync>;

pub struct CloudLinkOptions {
    pub environment_id: String,
    pub public: PublicConfig,
    /// `environment.getDescriptor`.
    pub descriptor: Descriptor,
    pub timings: Timings,
}

/// `CloudLink`, for the managed tunnel.
pub struct CloudLink {
    options: CloudLinkOptions,
    endpoint: ManagedEndpointRuntime,
    http: reqwest::Client,
    /// `CloudCliTokenManager`'s semaphore.
    token_lock: tokio::sync::Mutex<()>,
}

/// The HTTP client for T3 Connect and Clerk: no proxy and the bundled roots plus
/// `NODE_EXTRA_CA_CERTS`, as Node's fetch.
fn http_client() -> reqwest::Client {
    let mut builder = reqwest::Client::builder().no_proxy();
    if let Some(path) = std::env::var_os("NODE_EXTRA_CA_CERTS") {
        match std::fs::read(&path)
            .ok()
            .and_then(|pem| reqwest::Certificate::from_pem_bundle(&pem).ok())
        {
            Some(certificates) => {
                for certificate in certificates {
                    builder = builder.add_root_certificate(certificate);
                }
            }
            None => tracing::warn!(
                path = %PathBuf::from(path).display(),
                "Could not read the extra certificates in NODE_EXTRA_CA_CERTS"
            ),
        }
    }
    builder.build().unwrap_or_default()
}

fn now_ms() -> f64 {
    crate::platform::now_ms() as f64
}

fn json_number(value: f64) -> Value {
    if value.fract() == 0.0 && value.abs() < 9_007_199_254_740_992.0 {
        json!(value as i64)
    } else {
        json!(value)
    }
}

/// The reference's `TrimmedNonEmptyString`.
fn text(value: &Value, key: &str) -> Option<String> {
    value
        .get(key)?
        .as_str()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
}

fn origin_value(origin: &LocalOrigin) -> Value {
    json!({"localHttpHost": origin.host, "localHttpPort": origin.port})
}

/// `normalizeRelayIssuer`.
fn normalize_relay_issuer(value: &str) -> String {
    value.trim().trim_end_matches('/').to_owned()
}

fn pem_body(pem: &str) -> Option<Vec<u8>> {
    let pem = pem.replace("\\n", "\n");
    let body: String = pem
        .trim()
        .lines()
        .filter(|line| !line.starts_with("-----"))
        .flat_map(|line| line.chars().filter(|c| !c.is_whitespace()))
        .collect();
    STANDARD.decode(body).ok()
}

/// `KeyFormat.pem`: 64-column base64 between the label lines, ending with a newline.
fn pem(label: &str, der: &[u8]) -> String {
    let encoded = STANDARD.encode(der);
    let lines: Vec<&str> = encoded
        .as_bytes()
        .chunks(64)
        .map(|chunk| std::str::from_utf8(chunk).unwrap_or_default())
        .collect();
    format!(
        "-----BEGIN {label}-----\n{}\n-----END {label}-----\n",
        lines.join("\n")
    )
}

fn sign(key: &SigningKey, typ: &str, claims: &Value) -> String {
    let header = URL_SAFE_NO_PAD.encode(json!({"alg": "EdDSA", "typ": typ}).to_string());
    let payload = URL_SAFE_NO_PAD.encode(claims.to_string());
    let message = format!("{header}.{payload}");
    let signature = URL_SAFE_NO_PAD.encode(key.sign(message.as_bytes()).to_bytes());
    format!("{message}.{signature}")
}

/// `parseRetryAfterMillis`: delay-seconds or an HTTP date, at most 15 minutes.
fn parse_retry_after(value: &str) -> Option<Duration> {
    let value = value.trim();
    let millis = if !value.is_empty() && value.bytes().all(|b| b.is_ascii_digit()) {
        value.parse::<f64>().ok()? * 1000.0
    } else {
        let at = time::OffsetDateTime::parse(value, &time::format_description::well_known::Rfc2822)
            .ok()?;
        (at.unix_timestamp_nanos() / 1_000_000) as f64 - now_ms()
    };
    (millis.is_finite() && millis > 0.0)
        .then(|| Duration::from_millis(millis as u64).min(MAX_RELAY_RETRY_AFTER))
}

const RELAY_PROTECTED_ERROR_TAGS: &[&str] = &[
    "RelayAuthInvalidError",
    "RelayEnvironmentLinkProofExpiredError",
    "RelayEnvironmentLinkProofInvalidError",
    "RelayEnvironmentConnectNotAuthorizedError",
    "RelayEnvironmentEndpointUnavailableError",
    "RelayEnvironmentEndpointTimedOutError",
    "RelayEnvironmentLinkFailedError",
    "RelayEnvironmentLinkUnavailableError",
    "RelayEnvironmentLinkLimitExceededError",
    "RelayAgentActivityPublishProofExpiredError",
    "RelayAgentActivityPublishProofInvalidError",
    "RelayInternalError",
];

/// `recoveryHint`.
fn recovery_hint(tag: &str) -> &'static str {
    match tag {
        "RelayEnvironmentLinkLimitExceededError" => "Unlink an unused environment in T3 Connect, then restart T3 Code on this machine.",
        "RelayAuthInvalidError" => "Run `t3 connect login` to check this machine's authorization. If the stored credential was revoked, sign out with `t3 connect logout`, then run `t3 connect` again. Restart T3 Code after signing in.",
        "RelayEnvironmentLinkProofExpiredError" | "RelayEnvironmentLinkProofInvalidError" => "Check this machine's date and time, update T3 Code, then restart it.",
        _ => "Retry when the relay is available. If this continues, include the trace ID when reporting it.",
    }
}

/// `filterRelayResponse`, then the JSON body.
async fn relay_response(response: reqwest::Response) -> Result<Value, Error> {
    let status = response.status().as_u16();
    if (200..300).contains(&status) {
        return response
            .json::<Value>()
            .await
            .map_err(|_| Error::relay_transport(false));
    }
    let headers = response.headers().clone();
    let body = response.json::<Value>().await.ok();
    let protected = body.as_ref().filter(|body| {
        body["_tag"]
            .as_str()
            .is_some_and(|tag| RELAY_PROTECTED_ERROR_TAGS.contains(&tag))
            && body["message"].is_string()
            && body["traceId"].is_string()
    });
    let header = |name: &str| headers.get(name).and_then(|value| value.to_str().ok());
    let description = match protected {
        Some(error) => format!(
            "T3 Connect: {}. {} Trace ID: {}.",
            error["message"].as_str().unwrap_or_default(),
            recovery_hint(error["_tag"].as_str().unwrap_or_default()),
            error["traceId"].as_str().unwrap_or_default()
        ),
        None => {
            let ray = header("cf-ray")
                .filter(|ray| {
                    (1..=128).contains(&ray.len())
                        && ray.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-')
                })
                .map(|ray| format!(" Cloudflare Ray ID: {ray}."))
                .unwrap_or_default();
            format!("T3 Connect relay returned HTTP {status} without a recognized error response. Check relay access and any proxy or firewall restrictions, then restart T3 Code.{ray}")
        }
    };
    let rejection = match status {
        401 if protected.is_some_and(|e| e["_tag"] == "RelayEnvironmentLinkProofExpiredError") => {
            Rejection::Unavailable
        }
        401 => Rejection::Unauthorized,
        403 => Rejection::Forbidden,
        400..=499 if status != 408 && status != 429 => Rejection::Rejected,
        _ => Rejection::Unavailable,
    };
    let retry_after = (rejection == Rejection::Unavailable && status != 401)
        .then(|| header("retry-after").and_then(parse_retry_after))
        .flatten();
    Err(Error::Relay {
        rejection,
        description,
        retry_after,
    })
}

/// `relayRetrySchedule`'s delay for retry `attempt` (from 0): exponential, jittered, at most
/// the maximum, at least what the relay asked for.
fn relay_retry_delay(timings: &Timings, attempt: u32, error: &Error) -> Duration {
    let exponential = timings
        .retry_base
        .saturating_mul(2u32.saturating_pow(attempt.min(30)));
    let jittered = exponential.mul_f64(rand::Rng::gen_range(&mut crate::platform::rng(), 0.8..1.2));
    let delay = jittered.min(timings.retry_max);
    match error.retry_after() {
        Some(wait) => delay.max(wait),
        None => delay,
    }
}

/// `retryAcrossWindow`: retries while `retry` holds on the relay schedule. At the first sleep
/// that would end past the window, `on_window_exhausted` runs once and retrying continues only
/// while `retry_past_window` holds.
async fn retry_across_window<T, F, Fut, E, EFut>(
    timings: &Timings,
    mut operation: F,
    retry: impl Fn(&Error) -> bool,
    retry_past_window: impl Fn(&Error) -> bool,
    mut on_window_exhausted: E,
) -> Result<T, Error>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, Error>>,
    E: FnMut(Error) -> EFut,
    EFut: Future<Output = ()>,
{
    let started = Instant::now();
    let mut past_window = false;
    let mut attempt = 0;
    loop {
        let error = match operation().await {
            Ok(value) => return Ok(value),
            Err(error) => error,
        };
        let keep_going = if past_window {
            retry_past_window(&error)
        } else {
            retry(&error)
        };
        if !keep_going {
            return Err(error);
        }
        let delay = relay_retry_delay(timings, attempt, &error);
        attempt += 1;
        if !past_window && started.elapsed() + delay >= timings.retry_window {
            past_window = true;
            if !retry_past_window(&error) {
                return Err(error);
            }
            on_window_exhausted(error).await;
        }
        tokio::time::sleep(delay).await;
    }
}

/// `nextManagedTunnelRecoveryCooldown`.
fn next_recovery_cooldown(current: Duration, since_last_recovery: Option<Duration>) -> Duration {
    let settled = current + MANAGED_TUNNEL_RECOVERY_SETTLED_AFTER;
    match since_last_recovery {
        Some(since) if since < settled => (current * 2).min(MANAGED_TUNNEL_RECOVERY_MAX_COOLDOWN),
        _ => MANAGED_TUNNEL_RECOVERY_COOLDOWN,
    }
}

/// `isRetryableManagedEndpointRuntimeStatus`.
fn retryable_runtime_status(status: &Value) -> bool {
    status["status"] == "failed"
        && matches!(
            status["failure"].as_str(),
            Some("not-installed" | "spawn-failed")
        )
}

/// `pendingServiceUpdateExists`.
fn pending_service_update(runtime_dir: &Path) -> bool {
    std::fs::read_to_string(runtime_dir.join(SERVICE_STATE_FILE))
        .ok()
        .and_then(|text| serde_json::from_str::<Value>(&text).ok())
        .is_some_and(|state| state["update"]["status"] == "pending")
}

/// `pendingUpdateHandoffExists`.
fn pending_update_handoff(runtime_dir: &Path) -> bool {
    pending_service_update(runtime_dir) && !runtime_dir.join(SERVICE_STOP_MARKER_FILE).exists()
}

/// `desktopUpdateRestartPending`: reading consumes the marker; it counts for a minute.
fn desktop_update_restart_pending(runtime_dir: &Path) -> bool {
    let path = runtime_dir.join(DESKTOP_UPDATE_RESTART_MARKER_FILE);
    let Ok(metadata) = std::fs::metadata(&path) else {
        return false;
    };
    let _ = std::fs::remove_file(&path);
    metadata
        .modified()
        .ok()
        .and_then(|written| std::time::SystemTime::now().duration_since(written).ok())
        .is_some_and(|age| age < DESKTOP_UPDATE_RESTART_MARKER_TTL)
}

impl CloudLink {
    pub fn new(endpoint: ManagedEndpointRuntime, options: CloudLinkOptions) -> Self {
        Self {
            options,
            endpoint,
            http: http_client(),
            token_lock: tokio::sync::Mutex::new(()),
        }
    }

    fn secrets(&self) -> &Path {
        self.endpoint.secrets_dir()
    }

    fn secret(&self, name: &str) -> Option<String> {
        read_secret_string(self.secrets(), name)
    }

    fn set_secret(&self, name: &str, value: &str) -> Result<(), Error> {
        write_secret(self.secrets(), name, value.as_bytes())
            .map_err(|_| Error::Internal(format!("persist-{name}")))
    }

    fn remove(&self, name: &str) -> Result<(), Error> {
        remove_secret(self.secrets(), name).map_err(|_| Error::Internal(format!("remove-{name}")))
    }

    /// `readCliDesiredCloudLink`.
    fn cli_link_desired(&self) -> bool {
        read_secret(self.secrets(), CLOUD_CLI_DESIRED_LINK).is_some()
    }

    /// `readCliDesiredLinkMode`: "managed" unless it says "publish_only".
    fn cli_link_mode(&self) -> &'static str {
        match read_secret(self.secrets(), CLOUD_CLI_DESIRED_LINK).as_deref() {
            Some(b"publish_only") => "publish_only",
            _ => "managed",
        }
    }

    /// `CloudCliTokenManager.getExisting`: the stored CLI credential, refreshed (and the result
    /// stored) when it expires within five minutes.
    pub async fn cli_token(&self) -> Result<Option<Value>, Error> {
        let _lock = self.token_lock.lock().await;
        let Some(stored) = read_secret(self.secrets(), CLOUD_CLI_OAUTH_TOKEN) else {
            return Ok(None);
        };
        let refresh_failure = |detail: &str| Error::Refresh {
            requires_login: false,
            detail: detail.to_owned(),
        };
        let token: Value =
            serde_json::from_slice(&stored).map_err(|_| refresh_failure("decode"))?;
        let (Some(_), Some(refresh_token), Some(expires_at)) = (
            token["accessToken"].as_str(),
            token["refreshToken"].as_str(),
            token["expiresAtEpochMs"].as_f64(),
        ) else {
            return Err(refresh_failure("decode"));
        };
        if token
            .get("identity")
            .is_some_and(|identity| !identity.is_string())
        {
            return Err(refresh_failure("decode"));
        }
        if expires_at - CLOUD_CLI_OAUTH_REFRESH_EARLY_MS > now_ms() {
            return Ok(Some(token));
        }
        let (endpoint, client_id) = self
            .options
            .public
            .oauth()
            .ok_or_else(|| refresh_failure("config"))?;
        let response = self
            .http
            .post(&endpoint)
            .form(&[
                ("grant_type", "refresh_token"),
                ("refresh_token", refresh_token),
                ("client_id", client_id.as_str()),
            ])
            .timeout(OAUTH_REQUEST_TIMEOUT)
            .send()
            .await
            .map_err(|_| refresh_failure("transport"))?;
        let status = response.status().as_u16();
        if !(200..300).contains(&status) {
            let permanent = (400..500).contains(&status) && status != 408 && status != 429;
            let code = if permanent {
                response
                    .json::<Value>()
                    .await
                    .ok()
                    .and_then(|body| body["error"].as_str().map(str::to_owned))
                    .filter(|code| {
                        matches!(
                            code.as_str(),
                            "invalid_grant"
                                | "invalid_client"
                                | "unauthorized_client"
                                | "unsupported_grant_type"
                                | "invalid_request"
                                | "invalid_scope"
                                | "access_denied"
                        )
                    })
            } else {
                None
            };
            return Err(Error::Refresh {
                requires_login: permanent,
                detail: code
                    .map(|code| format!(" (OAuth error: {code})"))
                    .unwrap_or_default(),
            });
        }
        let body: Value = response
            .json()
            .await
            .map_err(|_| refresh_failure("decode"))?;
        let (Some(access_token), Some(expires_in), Some(_)) = (
            body["access_token"].as_str(),
            body["expires_in"].as_f64(),
            body["token_type"].as_str(),
        ) else {
            return Err(refresh_failure("decode"));
        };
        let mut refreshed = Map::new();
        refreshed.insert("accessToken".into(), json!(access_token));
        refreshed.insert(
            "refreshToken".into(),
            json!(body["refresh_token"].as_str().unwrap_or(refresh_token)),
        );
        refreshed.insert(
            "expiresAtEpochMs".into(),
            json_number(now_ms() + expires_in * 1000.0),
        );
        // The refreshed identity, else the stored one.
        if let Some(identity) = id_token_identity(body["id_token"].as_str())
            .or_else(|| token["identity"].as_str().map(str::to_owned))
        {
            refreshed.insert("identity".into(), json!(identity));
        }
        let refreshed = Value::Object(refreshed);
        self.set_secret(CLOUD_CLI_OAUTH_TOKEN, &refreshed.to_string())
            .map_err(|_| refresh_failure("persist"))?;
        Ok(Some(refreshed))
    }

    /// `getOrCreateEnvironmentKeyPairFromSecretStore`, parsed.
    fn environment_key(&self) -> Result<(SigningKey, String), Error> {
        let stored = self
            .secret(CLOUD_LINK_KEY_PAIR)
            .and_then(|text| serde_json::from_str::<Value>(&text).ok())
            .and_then(|pair| {
                Some((
                    pair["privateKey"].as_str()?.to_owned(),
                    pair["publicKey"].as_str()?.to_owned(),
                ))
            });
        let (private_pem, public_pem) = match stored {
            Some(pair) => pair,
            None => {
                let pair = match (
                    self.secret(CLOUD_LINK_PRIVATE_KEY),
                    self.secret(CLOUD_LINK_PUBLIC_KEY),
                ) {
                    (Some(private), Some(public)) => (private, public),
                    _ => {
                        let key = SigningKey::generate(&mut crate::platform::rng());
                        let private = key
                            .to_pkcs8_der()
                            .map_err(|_| Error::Internal("generate-key".into()))?;
                        let public = key
                            .verifying_key()
                            .to_public_key_der()
                            .map_err(|_| Error::Internal("generate-key".into()))?;
                        (
                            pem("PRIVATE KEY", private.as_bytes()),
                            pem("PUBLIC KEY", public.as_bytes()),
                        )
                    }
                };
                let encoded = json!({"privateKey": pair.0, "publicKey": pair.1}).to_string();
                self.set_secret(CLOUD_LINK_KEY_PAIR, &encoded)?;
                pair
            }
        };
        let key = pem_body(&private_pem)
            .and_then(|der| SigningKey::from_pkcs8_der(&der).ok())
            .ok_or_else(|| Error::Internal("sign".into()))?;
        Ok((key, public_pem))
    }

    /// `relayClientRequest`: a JSON POST with a bearer credential.
    async fn relay_post(
        &self,
        url: &str,
        token: &str,
        payload: Value,
        timeout: Duration,
    ) -> Result<Value, Error> {
        let request = self.http.post(url).bearer_auth(token).json(&payload).send();
        let response = tokio::time::timeout(timeout, async {
            let response = request.await.map_err(|_| Error::relay_transport(true))?;
            relay_response(response).await
        })
        .await;
        response.unwrap_or_else(|_| Err(Error::relay_transport(false)))
    }

    /// `markLinkRevoked`: whether this credential, still the stored one, is now marked revoked.
    async fn mark_link_revoked(&self, credential: &str, cause: &Error) -> Result<bool, Error> {
        let _lock = self.endpoint.lock_link_state().await;
        if self.secret(RELAY_ENVIRONMENT_CREDENTIAL_SECRET).as_deref() != Some(credential) {
            return Ok(false);
        }
        tracing::warn!(%cause, "T3 Connect no longer accepts this environment's link. Link it again to restore remote access.");
        self.set_secret(CLOUD_LINK_REVOKED, "true")?;
        Ok(true)
    }

    /// The stored link: runtime config text, relay URL, cloud user id, environment credential.
    fn stored_link(&self) -> Option<(String, String, String, String)> {
        Some((
            self.secret(CLOUD_ENDPOINT_RUNTIME_CONFIG)?,
            self.secret(RELAY_URL_SECRET)?,
            self.secret(CLOUD_LINKED_USER_ID)?,
            self.secret(RELAY_ENVIRONMENT_CREDENTIAL_SECRET)?,
        ))
    }

    /// `makeManagedTunnelRecoveryProof`.
    fn recovery_proof(
        &self,
        relay_url: &str,
        cloud_user_id: &str,
        action: &str,
        tunnel_id: Option<&str>,
        origin: &LocalOrigin,
    ) -> Result<String, Error> {
        let (key, _) = self
            .environment_key()
            .map_err(|_| Error::Internal("sign-recovery-proof".into()))?;
        let id = &self.options.environment_id;
        let issued_at = crate::platform::now_ms() / 1000;
        let audience = self
            .secret(RELAY_ISSUER_SECRET)
            .unwrap_or_else(|| relay_url.to_owned());
        let mut claims = json!({
            "iss": format!("t3-env:{id}"),
            "aud": normalize_relay_issuer(&audience),
            "sub": id,
            "jti": crate::platform::uuid_v4().to_string(),
            "iat": issued_at,
            "exp": issued_at + 60,
            "environmentId": id,
            "cloudUserId": cloud_user_id,
            "action": action,
        });
        if let Some(tunnel_id) = tunnel_id {
            claims["tunnelId"] = json!(tunnel_id);
        }
        claims["origin"] = origin_value(origin);
        Ok(sign(&key, RELAY_MANAGED_TUNNEL_RECOVERY_TYP, &claims))
    }

    /// `activateManagedTunnel`: runs the tunnel and records its origin as confirmed, unless the
    /// stored config changed meanwhile (`None`).
    async fn activate(
        &self,
        config: &RuntimeConfig,
        config_json: &str,
        origin: &LocalOrigin,
    ) -> Result<Option<Value>, Error> {
        let _lock = self.endpoint.lock_link_state().await;
        if self.secret(CLOUD_ENDPOINT_RUNTIME_CONFIG).as_deref() != Some(config_json) {
            return Ok(None);
        }
        let status = self.endpoint.apply_config(Some(config.clone())).await;
        if status["status"] != "running" {
            return Err(Error::EndpointUnavailable(status));
        }
        let marker = format!(
            "{{\"config\":{},\"origin\":{}}}",
            config.encode(),
            origin_value(origin)
        );
        self.set_secret(CLOUD_ENDPOINT_CONFIRMED_ORIGIN, &marker)?;
        Ok(Some(status))
    }

    /// `activateManagedTunnelWithRetry`.
    async fn activate_with_retry(
        &self,
        config: &RuntimeConfig,
        config_json: &str,
        origin: &LocalOrigin,
        retry_runtime_failures: bool,
    ) -> Result<Option<Value>, Error> {
        let mut attempt = 0u32;
        loop {
            match self.activate(config, config_json, origin).await {
                Err(Error::EndpointUnavailable(status))
                    if retry_runtime_failures && retryable_runtime_status(&status) =>
                {
                    let delay = Duration::from_secs(1u64 << attempt.min(16))
                        .min(self.options.timings.activation_retry_max)
                        .mul_f64(rand::Rng::gen_range(&mut crate::platform::rng(), 0.8..1.2));
                    attempt += 1;
                    tokio::time::sleep(delay).await;
                }
                other => return other,
            }
        }
    }

    /// `registerManagedTunnelRecovery`: tells T3 Connect the stored tunnel serves this origin,
    /// then runs it.
    pub async fn register(
        &self,
        origin: &LocalOrigin,
        retry_runtime_failures: bool,
    ) -> Result<Registration, Error> {
        let Some((config_json, relay_url, cloud_user_id, credential)) = self.stored_link() else {
            return Ok(Registration::NotLinked);
        };
        let Some(config) = RuntimeConfig::decode(&config_json)
            .filter(|config| config.provider_kind == "cloudflare_tunnel")
        else {
            return Ok(Registration::NotLinked);
        };
        let Some(tunnel_id) = config.tunnel_id.clone() else {
            return Ok(Registration::RecoveryRequired(config));
        };
        let id = &self.options.environment_id;
        let proof = self.recovery_proof(
            &relay_url,
            &cloud_user_id,
            "register",
            Some(&tunnel_id),
            origin,
        )?;
        let url = format!(
            "{relay_url}/v1/environments/{}/tunnel/recovery",
            encode_uri_component(id)
        );
        let registered = self
            .relay_post(
                &url,
                &credential,
                json!({
                    "cloudUserId": cloud_user_id,
                    "tunnelId": tunnel_id,
                    "origin": origin_value(origin),
                    "proof": proof,
                }),
                RELAY_REQUEST_TIMEOUT,
            )
            .await;
        let registered = match registered {
            Err(
                error @ Error::Relay {
                    rejection: Rejection::Unauthorized,
                    ..
                },
            ) => {
                return Ok(if self.mark_link_revoked(&credential, &error).await? {
                    Registration::Revoked
                } else {
                    Registration::Superseded
                });
            }
            other => other?,
        };
        let status = match registered["status"].as_str() {
            Some(status @ ("ready" | "recovery_required")) => status,
            _ => return Err(Error::relay_transport(false)),
        };
        self.remove(CLOUD_LINK_REVOKED)?;
        if status == "recovery_required" {
            return Ok(Registration::RecoveryRequired(config));
        }
        Ok(
            match self
                .activate_with_retry(&config, &config_json, origin, retry_runtime_failures)
                .await?
            {
                Some(status) => Registration::Ready(status),
                None => Registration::Superseded,
            },
        )
    }

    /// `recoverManagedTunnel`: asks T3 Connect for a replacement tunnel and runs it.
    pub async fn recover(
        &self,
        origin: &LocalOrigin,
        expected: Option<&RuntimeConfig>,
        retry_runtime_failures: bool,
    ) -> Result<bool, Error> {
        let Some((config_json, relay_url, cloud_user_id, credential)) = self.stored_link() else {
            return Ok(false);
        };
        if let Some(expected) = expected {
            if RuntimeConfig::decode(&config_json).as_ref() != Some(expected) {
                return Ok(false);
            }
        }
        let proof = self.recovery_proof(&relay_url, &cloud_user_id, "recover", None, origin)?;
        let url = format!(
            "{relay_url}/v1/environments/{}/tunnel",
            encode_uri_component(&self.options.environment_id)
        );
        let recovered = self
            .relay_post(
                &url,
                &credential,
                json!({"cloudUserId": cloud_user_id, "origin": origin_value(origin), "proof": proof}),
                MANAGED_ENDPOINT_PROVISION_REQUEST_TIMEOUT,
            )
            .await;
        if let Err(
            error @ Error::Relay {
                rejection: Rejection::Unauthorized,
                ..
            },
        ) = &recovered
        {
            self.mark_link_revoked(&credential, error).await?;
        }
        let recovered = recovered?;
        let endpoint = &recovered["endpoint"];
        let (Some(_), Some(_), Some(_)) = (
            text(endpoint, "httpBaseUrl"),
            text(endpoint, "wsBaseUrl"),
            text(endpoint, "providerKind"),
        ) else {
            return Err(Error::relay_transport(false));
        };
        let config = RuntimeConfig::from_value(&recovered["endpointRuntime"])
            .ok_or_else(|| Error::relay_transport(false))?;
        if config.provider_kind != "cloudflare_tunnel" {
            return Err(Error::Internal("unsupported-recovered-tunnel".into()));
        }
        let encoded = config.encode();
        {
            let _lock = self.endpoint.lock_link_state().await;
            if self.secret(CLOUD_ENDPOINT_RUNTIME_CONFIG).as_deref() != Some(&config_json) {
                return Ok(false);
            }
            self.set_secret(CLOUD_ENDPOINT_RUNTIME_CONFIG, &encoded)?;
            self.remove(CLOUD_ENDPOINT_CONFIRMED_ORIGIN)?;
            self.remove(CLOUD_LINK_REVOKED)?;
        }
        Ok(self
            .activate_with_retry(&config, &encoded, origin, retry_runtime_failures)
            .await?
            .is_some())
    }

    /// `applyCloudRelayConfig` for a link this server just made, with the lock held.
    async fn apply_relay_config(
        &self,
        relay_url: &str,
        link: &Value,
        endpoint_runtime: Option<RuntimeConfig>,
        confirmed_origin: &LocalOrigin,
    ) -> Result<(), Error> {
        let relay_issuer = text(link, "relayIssuer");
        let cloud_user_id = text(link, "cloudUserId").unwrap_or_default();
        let credential = text(link, "environmentCredential").unwrap_or_default();
        let mint_key = text(link, "cloudMintPublicKey").unwrap_or_default();
        if normalize_secure_relay_url(relay_url).is_none() {
            return Err(Error::RelayConfigInvalid("relayUrl"));
        }
        if relay_issuer
            .as_deref()
            .is_some_and(|issuer| normalize_secure_relay_url(issuer).is_none())
        {
            return Err(Error::RelayConfigInvalid("relayIssuer"));
        }
        if credential.is_empty() {
            return Err(Error::RelayConfigInvalid("environmentCredential"));
        }
        if cloud_user_id.is_empty() {
            return Err(Error::RelayConfigInvalid("cloudUserId"));
        }
        if let Some(existing) = self.secret(CLOUD_LINKED_USER_ID) {
            if existing != cloud_user_id {
                return Err(Error::AccountMismatch);
            }
        }
        if pem_body(&mint_key)
            .and_then(|der| VerifyingKey::from_public_key_der(&der).ok())
            .is_none()
        {
            return Err(Error::RelayConfigInvalid("cloudMintPublicKey"));
        }
        if let Some(config) = &endpoint_runtime {
            if config.provider_kind != "cloudflare_tunnel" {
                return Err(Error::EndpointUnavailable(json!({
                    "status": "unsupported",
                    "providerKind": config.provider_kind,
                })));
            }
        }
        self.endpoint.apply_config(None).await;
        self.remove(CLOUD_ENDPOINT_CONFIRMED_ORIGIN)?;
        // The runtime config is stored last, so a partial write never looks managed.
        self.remove(CLOUD_ENDPOINT_RUNTIME_CONFIG)?;
        self.set_secret(RELAY_URL_SECRET, relay_url)?;
        self.set_secret(
            RELAY_ISSUER_SECRET,
            relay_issuer.as_deref().unwrap_or(relay_url),
        )?;
        self.set_secret(CLOUD_LINKED_USER_ID, &cloud_user_id)?;
        self.set_secret(RELAY_ENVIRONMENT_CREDENTIAL_SECRET, &credential)?;
        self.remove(CLOUD_LINK_REVOKED)?;
        self.set_secret(CLOUD_MINT_PUBLIC_KEY, &mint_key)?;
        let Some(config) = endpoint_runtime else {
            return Ok(());
        };
        self.set_secret(CLOUD_ENDPOINT_RUNTIME_CONFIG, &config.encode())?;
        let status = self.endpoint.apply_config(Some(config.clone())).await;
        if status["status"] != "running" {
            return Err(Error::EndpointUnavailable(status));
        }
        let marker = format!(
            "{{\"config\":{},\"origin\":{}}}",
            config.encode(),
            origin_value(confirmed_origin)
        );
        self.set_secret(CLOUD_ENDPOINT_CONFIRMED_ORIGIN, &marker)
    }

    /// `reconcileDesiredCloudLinkWith`: links this environment again with the CLI's credential,
    /// as `t3 connect` did, and runs the tunnel T3 Connect provisions for it.
    async fn reconcile_desired_link_with(
        &self,
        origin: &LocalOrigin,
    ) -> Result<&'static str, Error> {
        let token = self.cli_token().await?.ok_or(Error::AuthorizationMissing)?;
        let access_token = token["accessToken"].as_str().unwrap_or_default().to_owned();
        let mode = self.cli_link_mode();
        let managed = mode != "publish_only";
        let relay_url = self
            .options
            .public
            .relay_url()
            .ok_or_else(|| Error::Internal("relay-url-unconfigured".into()))?;
        let flags = json!({
            "notificationsEnabled": true,
            "liveActivitiesEnabled": true,
            "managedTunnelsEnabled": managed,
        });
        let challenge = self
            .relay_post(
                &format!("{relay_url}/v1/client/environment-link-challenges"),
                &access_token,
                flags.clone(),
                RELAY_REQUEST_TIMEOUT,
            )
            .await?;
        let (Some(challenge), Some(_)) =
            (text(&challenge, "challenge"), text(&challenge, "expiresAt"))
        else {
            return Err(Error::relay_transport(false));
        };
        let proof = self
            .link_proof(&challenge, &relay_url, origin, managed)
            .await?;
        let mut payload = json!({"proof": proof});
        for (key, value) in flags.as_object().into_iter().flatten() {
            payload[key] = value.clone();
        }
        let link = self
            .relay_post(
                &format!("{relay_url}/v1/client/environment-links"),
                &access_token,
                payload,
                MANAGED_ENDPOINT_PROVISION_REQUEST_TIMEOUT,
            )
            .await?;
        let endpoint_runtime = decode_link_response(&link)?;
        self.set_secret(CLOUD_CLI_DESIRED_LINK, mode)?;
        self.apply_relay_config(&relay_url, &link, endpoint_runtime, origin)
            .await?;
        Ok(mode)
    }

    /// `makeCloudLinkProof`.
    async fn link_proof(
        &self,
        challenge: &str,
        relay_url: &str,
        origin: &LocalOrigin,
        managed: bool,
    ) -> Result<String, Error> {
        let (key, public_pem) = self.environment_key()?;
        let now = crate::platform::now_ms() / 1000;
        // Borrow only the relay's clock; the relay still verifies and consumes the challenge.
        let issued_at = challenge
            .split('.')
            .nth(1)
            .and_then(|claims| URL_SAFE_NO_PAD.decode(claims).ok())
            .and_then(|claims| serde_json::from_slice::<Value>(&claims).ok())
            .and_then(|claims| claims["iat"].as_i64())
            .filter(|iat| *iat > 0 && *iat <= 9_007_199_254_740_991)
            .unwrap_or(now);
        let id = &self.options.environment_id;
        let http_base_url = format!("http://{}:{}", origin.host, origin.port);
        let ws_base_url = format!("ws://{}:{}", origin.host, origin.port);
        let descriptor = (self.options.descriptor)().await;
        let claims = json!({
            "iss": format!("t3-env:{id}"),
            "aud": normalize_relay_issuer(relay_url),
            "sub": id,
            "jti": crate::platform::uuid_v4().to_string(),
            "iat": issued_at,
            "exp": issued_at + 5 * 60,
            "challenge": challenge,
            "descriptor": descriptor,
            "environmentId": id,
            "environmentPublicKey": public_pem.trim(),
            "endpoint": {
                "httpBaseUrl": http_base_url,
                "wsBaseUrl": ws_base_url,
                "providerKind": if managed { "cloudflare_tunnel" } else { "manual" },
            },
            "origin": origin_value(origin),
            "scopes": if managed {
                json!(["agent_activity_notifications", "managed_tunnels"])
            } else {
                json!(["agent_activity_notifications"])
            },
        });
        Ok(sign(&key, RELAY_LINK_PROOF_TYP, &claims))
    }

    /// `reconcileDesiredLinkIfStillDesired`.
    async fn reconcile_desired_link(
        &self,
        origin: &LocalOrigin,
    ) -> Result<Option<&'static str>, Error> {
        let _lock = self.endpoint.lock_link_state().await;
        if !self.cli_link_desired() {
            return Ok(None);
        }
        self.reconcile_desired_link_with(origin).await.map(Some)
    }

    /// `releaseManagedTunnelOnShutdown`: gives a CLI-provisioned tunnel back to T3 Connect, since
    /// it is billed while the environment is offline. The next server provisions its own.
    pub async fn release_on_shutdown(&self) -> Result<bool, Error> {
        let Some(config_json) = self.secret(CLOUD_ENDPOINT_RUNTIME_CONFIG) else {
            return Ok(false);
        };
        // Links made by the web or mobile apps recover by credential instead.
        if !self.cli_link_desired() || self.cli_link_mode() != "managed" {
            return Ok(false);
        }
        // Update restarts keep the tunnel: a new one takes minutes to propagate.
        let runtime_dir = self.endpoint.base_dir().join("runtime");
        if desktop_update_restart_pending(&runtime_dir) || pending_update_handoff(&runtime_dir) {
            tracing::info!("Keeping the managed tunnel across the update restart");
            return Ok(false);
        }
        let Some(token) = self.cli_token().await? else {
            return Ok(false);
        };
        // The persisted relay URL: T3CODE_RELAY_URL may have changed since linking.
        let Some(relay_url) = self.secret(RELAY_URL_SECRET) else {
            return Ok(false);
        };
        self.endpoint.apply_config(None).await;
        let url = format!(
            "{relay_url}/v1/client/environment-links/{}/tunnel",
            encode_uri_component(&self.options.environment_id)
        );
        let response = self
            .http
            .delete(&url)
            .bearer_auth(token["accessToken"].as_str().unwrap_or_default())
            .send()
            .await
            .map_err(|_| Error::relay_transport(true))?;
        let body = relay_response(response).await?;
        // ok:false means a concurrent link owns the tunnel; keep its config.
        if body["ok"] != true {
            if !body["ok"].is_boolean() {
                return Err(Error::relay_transport(false));
            }
            return Ok(false);
        }
        // Remove only this shutdown's config, so a fast restart's replacement survives.
        if self.secret(CLOUD_ENDPOINT_RUNTIME_CONFIG).as_deref() == Some(&config_json) {
            self.remove(CLOUD_ENDPOINT_RUNTIME_CONFIG)?;
            self.remove(CLOUD_ENDPOINT_CONFIRMED_ORIGIN)?;
        }
        Ok(true)
    }

    /// The shutdown finalizer: release, within ten seconds, logging the outcome.
    pub async fn release_on_shutdown_logged(&self) {
        match tokio::time::timeout(RELEASE_ON_SHUTDOWN_TIMEOUT, self.release_on_shutdown()).await {
            Ok(Ok(true)) => tracing::info!("Released the managed tunnel on shutdown"),
            Ok(Ok(false)) => {}
            Ok(Err(error)) => tracing::warn!(
                %error,
                "Failed to release the managed tunnel on shutdown; the next link reuses it"
            ),
            Err(_) => tracing::warn!(
                "Failed to release the managed tunnel on shutdown; the next link reuses it"
            ),
        }
    }

    /// `registerManagedTunnel` in `server.ts`: registration retried across the window; once
    /// past it, `fallback` starts the stored tunnel when it is not already running.
    async fn register_with_retry(
        &self,
        origin: &LocalOrigin,
        start_stored_past_window: bool,
    ) -> Registration {
        let result = retry_across_window(
            &self.options.timings,
            || self.register(origin, true),
            Error::retry_registration,
            Error::retry_registration,
            |_| async {
                if start_stored_past_window {
                    self.start_stored_unconfirmed(origin).await;
                }
            },
        )
        .await;
        match result {
            Ok(registration) => {
                if matches!(registration, Registration::Ready(_)) {
                    tracing::info!("T3 Connect managed tunnel recovery registered");
                }
                registration
            }
            Err(error) => {
                tracing::warn!(%error, "Failed to register T3 Connect managed tunnel recovery");
                Registration::Unavailable
            }
        }
    }

    /// `startStoredManagedTunnel`: the stored tunnel without relay confirmation.
    async fn start_stored_unconfirmed(&self, origin: &LocalOrigin) {
        match self.endpoint.start_if_origin_confirmed(origin, false).await {
            Ok(true) => {
                tracing::warn!("T3 Connect started the stored tunnel without relay confirmation")
            }
            Ok(false) => {}
            Err(status) => tracing::warn!(
                endpoint_runtime_status = %status,
                "Failed to start the stored T3 Connect tunnel"
            ),
        }
    }

    /// `makeManagedTunnelRecovery` and the stream that feeds it: one recovery at a time, each
    /// after a cooldown that doubles while recoveries do not settle the tunnel.
    pub async fn serve_recovery_requests(&self, origin: LocalOrigin) {
        let mut last_recovery: Option<Instant> = None;
        let mut cooldown = MANAGED_TUNNEL_RECOVERY_COOLDOWN;
        loop {
            let config = self.endpoint.next_recovery_request().await;
            let since = last_recovery.map(|at| at.elapsed());
            cooldown = next_recovery_cooldown(cooldown, since);
            if let Some(since) = since {
                if cooldown > since {
                    tokio::time::sleep(cooldown - since).await;
                }
            }
            let mut attempt = 0;
            let result = loop {
                match self.recover(&origin, Some(&config), true).await {
                    Err(error) if error.retry_registration() => {
                        let delay = relay_retry_delay(&self.options.timings, attempt, &error)
                            .max(MANAGED_TUNNEL_RECOVERY_COOLDOWN);
                        attempt += 1;
                        tokio::time::sleep(delay).await;
                    }
                    other => break other,
                }
            };
            last_recovery = Some(Instant::now());
            match result {
                Ok(true) => tracing::info!("T3 Connect managed tunnel recovered"),
                Ok(false) => {}
                Err(error) => {
                    tracing::warn!(%error, "Failed to recover the T3 Connect managed tunnel")
                }
            }
        }
    }

    /// The startup half of `server.ts`'s cloud link fiber, run once this server listens.
    pub async fn run_startup(&self, origin: &LocalOrigin) {
        let wants_cli_link = self.options.public.is_complete() && self.cli_link_desired();
        let mode = wants_cli_link.then(|| self.cli_link_mode());
        // Publish-only links must not expose this host through an old managed config.
        let started_confirmed = mode != Some("publish_only")
            && match self.endpoint.start_if_origin_confirmed(origin, true).await {
                Ok(started) => started,
                Err(status) => {
                    tracing::warn!(endpoint_runtime_status = %status, "Failed to start the confirmed T3 Connect tunnel");
                    false
                }
            };
        // Spread first registrations so update waves do not reach T3 Connect at once.
        if !started_confirmed && mode != Some("publish_only") {
            let jitter = self.options.timings.first_registration_jitter.as_millis() as u64;
            let wait = rand::Rng::gen_range(&mut crate::platform::rng(), 0..=jitter);
            tokio::time::sleep(Duration::from_millis(wait)).await;
        }
        let registration = if mode == Some("publish_only") {
            Registration::NotLinked
        } else {
            self.register_with_retry(origin, !started_confirmed).await
        };
        if registration == Registration::Unavailable && !started_confirmed {
            self.start_stored_unconfirmed(origin).await;
        }
        match registration {
            Registration::RecoveryRequired(config) => self.endpoint.request_recovery(config),
            Registration::NotLinked if wants_cli_link => {
                let reconciled = retry_across_window(
                    &self.options.timings,
                    || self.reconcile_desired_link(origin),
                    Error::should_retry,
                    Error::relay_unavailable,
                    |error| async move {
                        tracing::warn!(%error, "T3 Connect relay is still unavailable; retrying the desired link in the background");
                    },
                )
                .await;
                let mode = match reconciled {
                    Ok(Some(mode)) => {
                        tracing::info!("T3 Connect desired link reconciled on startup");
                        Some(mode)
                    }
                    Ok(None) => None,
                    Err(error) => {
                        tracing::warn!(%error, "Failed to reconcile T3 Connect desired link on startup");
                        None
                    }
                };
                if mode == Some("managed") {
                    if let Registration::RecoveryRequired(config) =
                        self.register_with_retry(origin, !started_confirmed).await
                    {
                        self.endpoint.request_recovery(config);
                    }
                }
            }
            _ => {}
        }
    }
}

/// `RelayEnvironmentLinkResponse`, for the endpoint runtime it carries.
fn decode_link_response(link: &Value) -> Result<Option<RuntimeConfig>, Error> {
    let endpoint = &link["endpoint"];
    let valid = link["ok"].is_boolean()
        && [
            "cloudUserId",
            "environmentId",
            "relayIssuer",
            "environmentCredential",
            "cloudMintPublicKey",
        ]
        .iter()
        .all(|key| text(link, key).is_some())
        && ["httpBaseUrl", "wsBaseUrl"]
            .iter()
            .all(|key| text(endpoint, key).is_some())
        && matches!(
            endpoint["providerKind"].as_str(),
            Some("manual" | "cloudflare_tunnel" | "t3_relay")
        );
    if !valid {
        return Err(Error::relay_transport(false));
    }
    match &link["endpointRuntime"] {
        Value::Null => Ok(None),
        value => RuntimeConfig::from_value(value)
            .map(Some)
            .ok_or_else(|| Error::relay_transport(false)),
    }
}

/// `encodeURIComponent`.
fn encode_uri_component(value: &str) -> String {
    const COMPONENT: &percent_encoding::AsciiSet = &percent_encoding::NON_ALPHANUMERIC
        .remove(b'-')
        .remove(b'_')
        .remove(b'.')
        .remove(b'!')
        .remove(b'~')
        .remove(b'*')
        .remove(b'\'')
        .remove(b'(')
        .remove(b')');
    percent_encoding::utf8_percent_encode(value, COMPONENT).to_string()
}

/// `idTokenIdentity`: the email, preferred username or subject an OIDC ID token names.
fn id_token_identity(id_token: Option<&str>) -> Option<String> {
    let payload = id_token?.split('.').nth(1)?;
    let claims: Value = serde_json::from_slice(&URL_SAFE_NO_PAD.decode(payload).ok()?).ok()?;
    ["email", "preferred_username", "sub"]
        .iter()
        .find_map(|key| claims[*key].as_str().filter(|value| !value.is_empty()))
        .map(str::to_owned)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::managed_endpoint::RuntimeOptions;
    use axum::{
        extract::State,
        http::{HeaderMap, Method, StatusCode, Uri},
        response::IntoResponse,
    };
    use std::sync::Mutex as StdMutex;

    #[test]
    fn relay_urls_are_https_origins() {
        assert_eq!(
            normalize_secure_relay_url(" https://relay.example.test/ "),
            Some("https://relay.example.test".into())
        );
        assert_eq!(
            normalize_secure_relay_url("https://relay.example.test:8443//"),
            Some("https://relay.example.test:8443".into())
        );
        for invalid in [
            "http://relay.example.test",
            "https://user@relay.example.test",
            "https://relay.example.test/v1",
            "https://relay.example.test/?a=1",
            "https://relay.example.test/#a",
            "relay.example.test",
        ] {
            assert_eq!(normalize_secure_relay_url(invalid), None, "{invalid}");
        }
    }

    #[test]
    fn the_clerk_token_endpoint_comes_from_the_publishable_key() {
        let key = format!("pk_test_{}", STANDARD.encode("clerk.example.test$"));
        let public = PublicConfig {
            relay_url_env: Some("https://relay.example.test".into()),
            relay_url_build: None,
            clerk_publishable_key: Some(key),
            clerk_cli_oauth_client_id: Some("client".into()),
        };
        assert!(public.is_complete());
        assert_eq!(
            public.oauth(),
            Some((
                "https://clerk.example.test/oauth/token".into(),
                "client".into()
            ))
        );
        let with_path = format!("pk_live_{}", STANDARD.encode("clerk.example.test/x$"));
        assert_eq!(clerk_frontend_api_url(&with_path), None);
        // An invalid runtime relay URL is not replaced by the build's.
        let invalid = PublicConfig {
            relay_url_env: Some("http://relay.example.test".into()),
            relay_url_build: Some("https://build.example.test".into()),
            ..public
        };
        assert_eq!(invalid.relay_url(), None);
        assert!(invalid.is_complete());
    }

    #[test]
    fn path_segments_are_encoded_as_encode_uri_component_does() {
        assert_eq!(
            encode_uri_component("00000000-0000-4000-8000-0000000e0001"),
            "00000000-0000-4000-8000-0000000e0001"
        );
        assert_eq!(encode_uri_component("a b/c~'é"), "a%20b%2Fc~'%C3%A9");
    }

    #[test]
    fn retry_after_and_recovery_cooldowns_follow_the_reference() {
        assert_eq!(parse_retry_after("7"), Some(Duration::from_secs(7)));
        assert_eq!(parse_retry_after("0"), None);
        assert_eq!(parse_retry_after("86400"), Some(MAX_RELAY_RETRY_AFTER));
        assert_eq!(parse_retry_after("soon"), None);
        let minute = Duration::from_secs(60);
        assert_eq!(next_recovery_cooldown(2 * minute, None), 2 * minute);
        assert_eq!(next_recovery_cooldown(2 * minute, Some(minute)), 4 * minute);
        assert_eq!(
            next_recovery_cooldown(2 * minute, Some(12 * minute)),
            2 * minute
        );
        assert_eq!(
            next_recovery_cooldown(20 * minute, Some(minute)),
            30 * minute
        );
    }

    type Answer = (u16, Vec<(&'static str, &'static str)>, Value);

    #[derive(Default)]
    struct Fake {
        requests: StdMutex<Vec<(String, String, Option<String>)>>,
        answers: StdMutex<Vec<Answer>>,
    }

    async fn answer(
        State(fake): State<Arc<Fake>>,
        method: Method,
        uri: Uri,
        headers: HeaderMap,
    ) -> axum::response::Response {
        fake.requests.lock().unwrap().push((
            method.to_string(),
            uri.path().to_owned(),
            headers
                .get("authorization")
                .and_then(|value| value.to_str().ok())
                .map(str::to_owned),
        ));
        let (status, extra, body) = fake.answers.lock().unwrap().remove(0);
        let mut response =
            (StatusCode::from_u16(status).unwrap(), axum::Json(body)).into_response();
        for (name, value) in extra {
            response.headers_mut().insert(name, value.parse().unwrap());
        }
        response
    }

    struct Setup {
        _home: tempfile::TempDir,
        secrets: PathBuf,
        base: PathBuf,
        fake: Arc<Fake>,
        url: String,
        link: CloudLink,
    }

    async fn setup() -> Setup {
        let home = tempfile::tempdir().unwrap();
        let paths = crate::config::ServerPaths::from_base(home.path());
        let fake = Arc::new(Fake::default());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let app = axum::Router::new()
            .fallback(answer)
            .with_state(fake.clone());
        tokio::spawn(async move { axum::serve(listener, app).await });
        let endpoint = ManagedEndpointRuntime::open(RuntimeOptions::for_paths(&paths, true)).await;
        let link = CloudLink::new(
            endpoint,
            CloudLinkOptions {
                environment_id: "env-1".into(),
                public: PublicConfig::default(),
                descriptor: Arc::new(|| Box::pin(async { json!({}) })),
                timings: Timings::default(),
            },
        );
        Setup {
            secrets: paths.secrets.clone(),
            base: paths.base_dir.clone(),
            _home: home,
            fake,
            url,
            link,
        }
    }

    impl Setup {
        fn put(&self, name: &str, value: &str) {
            write_secret(&self.secrets, name, value.as_bytes()).unwrap();
        }

        fn get(&self, name: &str) -> Option<String> {
            read_secret_string(&self.secrets, name)
        }

        fn seed_link(&self) {
            self.put(
                CLOUD_ENDPOINT_RUNTIME_CONFIG,
                r#"{"providerKind":"cloudflare_tunnel","connectorToken":"t","tunnelId":"tunnel-1"}"#,
            );
            self.put(CLOUD_ENDPOINT_CONFIRMED_ORIGIN, "{}");
            self.put(RELAY_URL_SECRET, &self.url);
            self.put(CLOUD_LINKED_USER_ID, "user");
            self.put(RELAY_ENVIRONMENT_CREDENTIAL_SECRET, "credential");
            self.put(CLOUD_CLI_DESIRED_LINK, "managed");
            let expires = crate::platform::now_ms() + 3_600_000;
            self.put(
                CLOUD_CLI_OAUTH_TOKEN,
                &format!(r#"{{"accessToken":"access","refreshToken":"refresh","expiresAtEpochMs":{expires}}}"#),
            );
        }

        fn answer(&self, status: u16, headers: Vec<(&'static str, &'static str)>, body: Value) {
            self.fake
                .answers
                .lock()
                .unwrap()
                .push((status, headers, body));
        }

        fn requests(&self) -> Vec<(String, String, Option<String>)> {
            self.fake.requests.lock().unwrap().clone()
        }
    }

    fn origin() -> LocalOrigin {
        LocalOrigin {
            host: "127.0.0.1".into(),
            port: 3773,
        }
    }

    #[tokio::test]
    async fn registration_without_a_stored_link_asks_nothing() {
        let setup = setup().await;
        assert_eq!(
            setup.link.register(&origin(), false).await,
            Ok(Registration::NotLinked)
        );
        assert!(setup.requests().is_empty());
    }

    #[tokio::test]
    async fn an_unauthorized_registration_marks_the_link_revoked() {
        let setup = setup().await;
        setup.seed_link();
        setup.put(
            CLOUD_LINK_KEY_PAIR,
            &json!({"privateKey": pem("PRIVATE KEY", SigningKey::generate(&mut crate::platform::rng()).to_pkcs8_der().unwrap().as_bytes()), "publicKey": "unused"}).to_string(),
        );
        setup.answer(401, vec![], json!({}));
        assert_eq!(
            setup.link.register(&origin(), false).await,
            Ok(Registration::Revoked)
        );
        assert_eq!(setup.get(CLOUD_LINK_REVOKED).as_deref(), Some("true"));
        assert_eq!(
            setup.requests(),
            vec![(
                "POST".into(),
                "/v1/environments/env-1/tunnel/recovery".into(),
                Some("Bearer credential".into())
            )]
        );
    }

    #[tokio::test]
    async fn a_missing_environment_key_is_created_in_the_reference_format() {
        let setup = setup().await;
        let (key, public) = setup.link.environment_key().unwrap();
        let stored: Value = serde_json::from_str(&setup.get(CLOUD_LINK_KEY_PAIR).unwrap()).unwrap();
        let private = stored["privateKey"].as_str().unwrap();
        assert!(private.starts_with("-----BEGIN PRIVATE KEY-----\n"));
        assert!(private.ends_with("\n-----END PRIVATE KEY-----\n"));
        assert_eq!(stored["publicKey"], public);
        let verifying = VerifyingKey::from_public_key_der(&pem_body(&public).unwrap()).unwrap();
        assert_eq!(verifying, key.verifying_key());
        // Read back, not replaced.
        assert_eq!(setup.link.environment_key().unwrap().1, public);
    }

    #[tokio::test]
    async fn shutdown_releases_a_cli_tunnel_and_forgets_it() {
        let setup = setup().await;
        setup.seed_link();
        setup.answer(200, vec![], json!({"ok": true}));
        assert_eq!(setup.link.release_on_shutdown().await, Ok(true));
        assert_eq!(
            setup.requests(),
            vec![(
                "DELETE".into(),
                "/v1/client/environment-links/env-1/tunnel".into(),
                Some("Bearer access".into())
            )]
        );
        assert_eq!(setup.get(CLOUD_ENDPOINT_RUNTIME_CONFIG), None);
        assert_eq!(setup.get(CLOUD_ENDPOINT_CONFIRMED_ORIGIN), None);
        assert!(setup.get(RELAY_ENVIRONMENT_CREDENTIAL_SECRET).is_some());
    }

    #[tokio::test]
    async fn a_concurrent_link_keeps_its_tunnel() {
        let setup = setup().await;
        setup.seed_link();
        setup.answer(200, vec![], json!({"ok": false}));
        assert_eq!(setup.link.release_on_shutdown().await, Ok(false));
        assert!(setup.get(CLOUD_ENDPOINT_RUNTIME_CONFIG).is_some());
    }

    #[tokio::test]
    async fn update_restarts_and_non_cli_links_keep_the_tunnel() {
        let setup = setup().await;
        setup.seed_link();
        let runtime = setup.base.join("runtime");
        std::fs::create_dir_all(&runtime).unwrap();
        std::fs::write(runtime.join(DESKTOP_UPDATE_RESTART_MARKER_FILE), "").unwrap();
        assert_eq!(setup.link.release_on_shutdown().await, Ok(false));
        // Reading consumed the marker.
        assert!(!runtime.join(DESKTOP_UPDATE_RESTART_MARKER_FILE).exists());
        std::fs::write(
            runtime.join(SERVICE_STATE_FILE),
            r#"{"update":{"status":"pending"}}"#,
        )
        .unwrap();
        assert_eq!(setup.link.release_on_shutdown().await, Ok(false));
        std::fs::remove_file(runtime.join(SERVICE_STATE_FILE)).unwrap();
        setup.put(CLOUD_CLI_DESIRED_LINK, "publish_only");
        assert_eq!(setup.link.release_on_shutdown().await, Ok(false));
        remove_secret(&setup.secrets, CLOUD_CLI_DESIRED_LINK).unwrap();
        assert_eq!(setup.link.release_on_shutdown().await, Ok(false));
        assert!(setup.requests().is_empty());
        assert!(setup.get(CLOUD_ENDPOINT_RUNTIME_CONFIG).is_some());
    }

    #[tokio::test]
    async fn relay_failures_are_classified_as_the_reference_does() {
        let setup = setup().await;
        setup.seed_link();
        let release = |setup: &Setup| {
            setup.put(
                CLOUD_ENDPOINT_RUNTIME_CONFIG,
                r#"{"providerKind":"cloudflare_tunnel","connectorToken":"t"}"#,
            );
        };
        setup.answer(503, vec![("retry-after", "7")], json!({}));
        let error = setup.link.release_on_shutdown().await.unwrap_err();
        assert!(error.should_retry());
        assert_eq!(error.retry_after(), Some(Duration::from_secs(7)));
        release(&setup);
        setup.answer(
            400,
            vec![],
            json!({"_tag": "RelayInternalError", "message": "Bad", "traceId": "t1"}),
        );
        let error = setup.link.release_on_shutdown().await.unwrap_err();
        assert!(!error.should_retry());
        assert!(
            error.to_string().starts_with("T3 Connect: Bad. Retry when"),
            "{error}"
        );
        release(&setup);
        setup.answer(
            401,
            vec![],
            json!({"_tag": "RelayEnvironmentLinkProofExpiredError", "message": "Expired", "traceId": "t2"}),
        );
        assert!(setup
            .link
            .release_on_shutdown()
            .await
            .unwrap_err()
            .should_retry());
        release(&setup);
        setup.answer(401, vec![], json!({}));
        let error = setup.link.release_on_shutdown().await.unwrap_err();
        assert!(matches!(
            error,
            Error::Relay {
                rejection: Rejection::Unauthorized,
                ..
            }
        ));
    }

    #[tokio::test]
    async fn an_expired_cli_token_without_oauth_config_is_a_retryable_refresh_failure() {
        let setup = setup().await;
        setup.put(
            CLOUD_CLI_OAUTH_TOKEN,
            r#"{"accessToken":"a","refreshToken":"r","expiresAtEpochMs":1}"#,
        );
        let error = setup.link.cli_token().await.unwrap_err();
        assert!(matches!(
            error,
            Error::Refresh {
                requires_login: false,
                ..
            }
        ));
        assert!(error.should_retry());
    }

    #[tokio::test]
    async fn retries_stop_at_the_window_unless_allowed_past_it() {
        let timings = Timings {
            retry_window: Duration::from_millis(50),
            retry_base: Duration::from_millis(20),
            ..Timings::default()
        };
        let attempts = std::sync::atomic::AtomicU32::new(0);
        let exhausted = std::sync::atomic::AtomicU32::new(0);
        let unavailable = Error::relay_transport(true);
        let result: Result<(), Error> = retry_across_window(
            &timings,
            || async {
                if attempts.fetch_add(1, std::sync::atomic::Ordering::SeqCst) < 4 {
                    Err(unavailable.clone())
                } else {
                    Ok(())
                }
            },
            Error::should_retry,
            Error::relay_unavailable,
            |_| async {
                exhausted.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            },
        )
        .await;
        assert_eq!(result, Ok(()));
        assert_eq!(exhausted.load(std::sync::atomic::Ordering::SeqCst), 1);
        let result: Result<(), Error> = retry_across_window(
            &timings,
            || async { Err(Error::Internal("x".into())) },
            Error::should_retry,
            Error::relay_unavailable,
            |_| async {},
        )
        .await;
        assert_eq!(result, Err(Error::Internal("x".into())));
    }
}
