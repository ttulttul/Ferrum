//! The ACP Registry catalog (`AcpRegistryCatalog` in the reference's
//! `provider-acp-registry` package): `server.searchAcpRegistry` and the registry
//! lookup behind `server.prepareAcpRegistryAgent`.
//!
//! The index is fetched from the registry's CDN and cached in
//! `<T3 home>/caches/acp-registry/registry.json`. When the CDN cannot be reached, the
//! cached index answers; with no cache, the request fails `registry_unavailable`.
//! Searches reuse a fetched index for five minutes.
use regex::Regex;
use serde_json::{json, Value};
use std::path::{Path, PathBuf};
use std::sync::LazyLock;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;

pub(crate) const REGISTRY_URL: &str =
    "https://cdn.agentclientprotocol.com/registry/v1/latest/registry.json";
const MAX_REGISTRY_BYTES: usize = 1024 * 1024;
const MAX_AGENTS: usize = 512;
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
const SEARCH_MAX_AGE: Duration = Duration::from_secs(5 * 60);
const DEFAULT_SEARCH_LIMIT: usize = 20;

/// `AcpRegistryError`: a reason, its detail, and what caused it.
#[derive(Debug, Clone)]
pub(crate) struct AcpError {
    reason: &'static str,
    detail: String,
    cause: Option<Value>,
}

impl AcpError {
    fn new(reason: &'static str, detail: impl Into<String>) -> Self {
        Self {
            reason,
            detail: detail.into(),
            cause: None,
        }
    }

    fn unavailable(detail: impl Into<String>) -> Self {
        Self::new("registry_unavailable", detail)
    }

    /// `toAcpRegistryOperationError`, as the RPC fails with it.
    pub(crate) fn operation_error(&self) -> Value {
        let mut cause = json!({"name": "AcpRegistryError", "message": self.detail});
        if let Some(inner) = &self.cause {
            cause["cause"] = inner.clone();
        }
        json!({"_tag": "AcpRegistryOperationError", "reason": self.reason, "message": self.detail, "cause": cause})
    }
}

/// The index's agents, each as the registry lists it (validated).
#[derive(Clone)]
struct Index {
    agents: Vec<Value>,
}

#[derive(Default)]
struct Catalog {
    index: Option<Index>,
    /// When the in-memory index was fetched; `None` when it came from the cache.
    fetched_at: Option<Instant>,
}

static CATALOG: LazyLock<Mutex<Catalog>> = LazyLock::new(Mutex::default);

fn regex(pattern: &str) -> Regex {
    Regex::new(pattern).expect("valid pattern")
}

static AGENT_ID: LazyLock<Regex> = LazyLock::new(|| regex(r"^[a-z0-9][a-z0-9._-]*$"));
static VERSION: LazyLock<Regex> = LazyLock::new(|| regex(r"^[A-Za-z0-9][A-Za-z0-9._+-]*$"));
static SHA256: LazyLock<Regex> = LazyLock::new(|| regex(r"(?i)^[a-f0-9]{64}$"));
const RUNNER_VERSION: &str = r"v?[0-9]+\.[0-9]+\.[0-9]+(?:-[0-9A-Za-z.-]+)?(?:\+[0-9A-Za-z.-]+)?";
static NPX_PACKAGE: LazyLock<Regex> = LazyLock::new(|| {
    regex(&format!(
        r"(?i)^(?:@[^/@\s]+/[^@\s]+|[a-z0-9][a-z0-9._-]*)@{RUNNER_VERSION}$"
    ))
});
static UVX_PACKAGE: LazyLock<Regex> = LazyLock::new(|| {
    regex(&format!(
        r"(?i)^[a-z0-9][a-z0-9._-]*(?:@|==){RUNNER_VERSION}$"
    ))
});

fn utf16_len(text: &str) -> usize {
    text.encode_utf16().count()
}

fn bounded(value: &Value, max: usize) -> Option<&str> {
    value.as_str().filter(|s| utf16_len(s) <= max)
}

/// `HttpsUrl`: an absolute https URL without credentials, at most 2048 characters.
fn https_url(value: &Value) -> bool {
    bounded(value, 2048)
        .and_then(|s| url::Url::parse(s).ok())
        .is_some_and(|u| u.scheme() == "https" && u.username().is_empty() && u.password().is_none())
}

fn optional(agent: &Value, key: &str, valid: impl Fn(&Value) -> bool) -> bool {
    agent.get(key).is_none_or(&valid)
}

fn string_array(value: &Value, max_items: usize, max_len: usize) -> bool {
    value.as_array().is_some_and(|items| {
        items.len() <= max_items && items.iter().all(|i| bounded(i, max_len).is_some())
    })
}

fn string_record(value: &Value, key_max: usize, value_max: usize) -> bool {
    value.as_object().is_some_and(|map| {
        map.iter()
            .all(|(k, v)| utf16_len(k) <= key_max && bounded(v, value_max).is_some())
    })
}

fn package_distribution(value: &Value, pattern: &Regex) -> bool {
    value.is_object()
        && bounded(&value["package"], 256).is_some_and(|p| pattern.is_match(p))
        && optional(value, "args", |a| string_array(a, 64, 1024))
        && optional(value, "env", |e| string_record(e, 256, 1024))
}

fn binary_target(value: &Value) -> bool {
    value.is_object()
        && https_url(&value["archive"])
        && bounded(&value["cmd"], 1024).is_some()
        && optional(value, "sha256", |s| {
            s.as_str().is_some_and(|s| SHA256.is_match(s))
        })
        && optional(value, "args", |a| string_array(a, 64, 1024))
        && optional(value, "env", |e| string_record(e, 256, 1024))
}

/// `decodeRegistryAgent`: the agent with its name and version trimmed, or `None`
/// when it does not match the registry's schema.
fn decode_agent(candidate: &Value) -> Option<Value> {
    let mut agent = candidate.as_object()?.clone();
    let trimmed =
        |key: &str, max: usize, agent: &mut serde_json::Map<String, Value>| -> Option<String> {
            let text = agent.get(key)?.as_str()?.trim().to_owned();
            (!text.is_empty() && utf16_len(&text) <= max).then(|| {
                agent.insert(key.to_owned(), json!(text));
                text
            })
        };
    let id = bounded(agent.get("id")?, 128)?;
    if !AGENT_ID.is_match(id) {
        return None;
    }
    trimmed("name", 160, &mut agent)?;
    let version = trimmed("version", 128, &mut agent)?;
    if !VERSION.is_match(&version) {
        return None;
    }
    let agent = Value::Object(agent);
    let distribution = &agent["distribution"];
    let valid = bounded(&agent["description"], 1024).is_some()
        && optional(&agent, "authors", |a| string_array(a, 16, 256))
        && optional(&agent, "license", |l| bounded(l, 128).is_some())
        && optional(&agent, "website", https_url)
        && optional(&agent, "repository", https_url)
        && optional(&agent, "icon", https_url)
        && distribution.is_object()
        && optional(distribution, "binary", |b| {
            b.as_object()
                .is_some_and(|targets| targets.values().all(binary_target))
        })
        && optional(distribution, "npx", |d| {
            package_distribution(d, &NPX_PACKAGE)
        })
        && optional(distribution, "uvx", |d| {
            package_distribution(d, &UVX_PACKAGE)
        });
    valid.then_some(agent)
}

/// `decodeRegistryText`: the index in `text`, keeping the agents that decode.
fn decode_registry(text: &str) -> Result<Index, AcpError> {
    let decoded: Value = serde_json::from_str(text)
        .map_err(|_| AcpError::unavailable("ACP Registry returned invalid JSON."))?;
    let invalid = || AcpError::unavailable("ACP Registry returned an invalid index.");
    let version = decoded["version"]
        .as_str()
        .map(str::trim)
        .ok_or_else(invalid)?;
    if version.is_empty() || utf16_len(version) > 128 || !VERSION.is_match(version) {
        return Err(invalid());
    }
    let agents = decoded["agents"]
        .as_array()
        .filter(|a| a.len() <= MAX_AGENTS)
        .ok_or_else(invalid)?;
    Ok(Index {
        agents: agents.iter().filter_map(decode_agent).collect(),
    })
}

fn cache_path(caches: &Path) -> PathBuf {
    caches.join("acp-registry").join("registry.json")
}

async fn read_cache(caches: &Path) -> Option<Index> {
    let text = tokio::fs::read_to_string(cache_path(caches)).await.ok()?;
    decode_registry(&text).ok()
}

async fn write_cache(caches: &Path, text: &str) {
    let path = cache_path(caches);
    let Some(directory) = path.parent() else {
        return;
    };
    if tokio::fs::create_dir_all(directory).await.is_err() {
        return;
    }
    let temporary = directory.join(format!(
        "registry.json.{}-{}.tmp",
        std::process::id(),
        uuid::Uuid::new_v4()
    ));
    if tokio::fs::write(&temporary, text).await.is_ok()
        && tokio::fs::rename(&temporary, &path).await.is_ok()
    {
        return;
    }
    let _ = tokio::fs::remove_file(&temporary).await;
}

/// `fetchRegistry`: the index from the CDN, cached when it decodes.
async fn fetch(caches: &Path) -> Result<Index, AcpError> {
    let not_fetched = |cause: Value| AcpError {
        reason: "registry_unavailable",
        detail: format!("Could not fetch ACP Registry index from {REGISTRY_URL}."),
        cause: Some(cause),
    };
    let transport = || json!({"name": "HttpClientError", "message": format!("Transport error (GET {REGISTRY_URL})")});
    let client = reqwest::Client::builder()
        .timeout(REQUEST_TIMEOUT)
        .build()
        .map_err(|_| not_fetched(transport()))?;
    let response = client.get(REGISTRY_URL).send().await.map_err(|error| {
        if error.is_timeout() {
            AcpError::unavailable("Timed out fetching ACP Registry index.")
        } else {
            not_fetched(transport())
        }
    })?;
    let status = response.status().as_u16();
    if !(200..300).contains(&status) {
        return Err(not_fetched(json!({"name": "HttpClientError",
            "message": format!("StatusCode: non 2xx status code ({status} GET {REGISTRY_URL})")})));
    }
    let mut body = Vec::new();
    let mut stream = response;
    loop {
        match stream.chunk().await {
            Ok(Some(chunk)) => {
                body.extend_from_slice(&chunk);
                if body.len() > MAX_REGISTRY_BYTES {
                    return Err(AcpError::unavailable(format!(
                        "ACP Registry index exceeds {MAX_REGISTRY_BYTES} bytes."
                    )));
                }
            }
            Ok(None) => break,
            Err(_) => {
                return Err(AcpError::unavailable(
                    "Could not read ACP Registry response body.",
                ))
            }
        }
    }
    let text = String::from_utf8(body)
        .map_err(|_| AcpError::unavailable("ACP Registry index is not valid UTF-8."))?;
    let index = decode_registry(&text)?;
    write_cache(caches, &text).await;
    Ok(index)
}

/// `refreshRegistry`: a fresh index, or the cached one when the CDN fails.
async fn refresh(catalog: &mut Catalog, caches: &Path) -> Result<Index, AcpError> {
    let (index, fetched) = match fetch(caches).await {
        Ok(index) => (index, true),
        Err(error) => match read_cache(caches).await {
            Some(index) => (index, false),
            None => return Err(error),
        },
    };
    catalog.index = Some(index.clone());
    catalog.fetched_at = fetched.then(Instant::now);
    Ok(index)
}

/// `recentRegistry`: the index fetched in the last five minutes, or a refreshed one.
async fn recent(catalog: &mut Catalog, caches: &Path) -> Result<Index, AcpError> {
    if let (Some(index), Some(at)) = (&catalog.index, catalog.fetched_at) {
        if at.elapsed() < SEARCH_MAX_AGE {
            return Ok(index.clone());
        }
    }
    refresh(catalog, caches).await
}

/// `resolveAcpRegistryPlatformTarget` for this build.
fn platform_target() -> Option<&'static str> {
    match (std::env::consts::OS, std::env::consts::ARCH) {
        ("linux", "x86_64") => Some("linux-x86_64"),
        ("linux", "aarch64") => Some("linux-aarch64"),
        ("macos", "x86_64") => Some("darwin-x86_64"),
        ("macos", "aarch64") => Some("darwin-aarch64"),
        ("windows", "x86_64") => Some("windows-x86_64"),
        ("windows", "aarch64") => Some("windows-aarch64"),
        _ => None,
    }
}

/// `resolveAcpRegistryDistribution` with the `auto` preference: the platform's
/// binary, then npx, then uvx. The binary target, if it is the binary.
fn distribution(agent: &Value) -> Option<(&'static str, Option<&Value>)> {
    let candidates: Vec<&str> = match platform_target() {
        // Windows on Arm runs x86_64 builds when there is no native one.
        Some("windows-aarch64") => vec!["windows-aarch64", "windows-x86_64"],
        Some(target) => vec![target],
        None => vec![],
    };
    let dist = &agent["distribution"];
    if let Some(binary) = candidates.iter().find_map(|t| dist["binary"].get(*t)) {
        return Some(("binary", Some(binary)));
    }
    if dist.get("npx").is_some() {
        return Some(("npx", None));
    }
    dist.get("uvx").is_some().then_some(("uvx", None))
}

/// Whether `name` is an executable on the server's PATH (`resolveExecutable`).
fn on_path(name: &str) -> bool {
    let Some(path) = std::env::var_os("PATH") else {
        return false;
    };
    std::env::split_paths(&path).any(|dir| {
        let candidate = dir.join(name);
        crate::npm_release::is_executable(&candidate)
    })
}

/// `searchRank`: lower ranks first; `None` does not match.
fn search_rank(agent: &Value, query: &str) -> Option<u32> {
    let normalized = query.trim().to_lowercase();
    if normalized.is_empty() {
        return Some(100);
    }
    let text = |key: &str| agent[key].as_str().unwrap_or_default().to_lowercase();
    let (id, name, description) = (text("id"), text("name"), text("description"));
    let authors = agent["authors"]
        .as_array()
        .map(|a| {
            a.iter()
                .filter_map(Value::as_str)
                .collect::<Vec<_>>()
                .join(" ")
        })
        .unwrap_or_default()
        .to_lowercase();
    let terms: Vec<&str> = normalized.split_whitespace().collect();
    if id == normalized || name == normalized {
        return Some(0);
    }
    if id.starts_with(&normalized) || name.starts_with(&normalized) {
        return Some(10);
    }
    let identity = format!("{id} {name}");
    let tokens: Vec<&str> = identity
        .split(|c: char| !c.is_ascii_lowercase() && !c.is_ascii_digit())
        .filter(|t| !t.is_empty())
        .collect();
    if terms
        .iter()
        .all(|term| tokens.iter().any(|t| t.starts_with(term)))
    {
        return Some(20);
    }
    if terms
        .iter()
        .all(|term| id.contains(term) || name.contains(term))
    {
        return Some(30);
    }
    if terms.iter().all(|term| authors.contains(term)) {
        return Some(40);
    }
    let everything = format!("{id} {name} {authors} {description}");
    terms
        .iter()
        .all(|term| everything.contains(term))
        .then_some(50)
}

/// `server.searchAcpRegistry`: agents installable here that match `query`, best first.
pub(crate) async fn search(caches: &Path, params: &Value) -> Result<Value, AcpError> {
    let index = recent(&mut *CATALOG.lock().await, caches).await?;
    let query = params["query"].as_str().unwrap_or_default();
    let mut ranked: Vec<(u32, &Value, &'static str, Option<&Value>)> = index
        .agents
        .iter()
        .filter_map(|agent| {
            let (kind, binary) = distribution(agent)?;
            let rank = search_rank(agent, query)?;
            let manager = match kind {
                "npx" => Some("npm"),
                "uvx" => Some("uv"),
                _ => None,
            };
            manager
                .is_none_or(on_path)
                .then_some((rank, agent, kind, binary))
        })
        .collect();
    let lower = |a: &Value| a["name"].as_str().unwrap_or_default().to_lowercase();
    ranked.sort_by(|l, r| {
        l.0.cmp(&r.0)
            .then_with(|| lower(l.1).cmp(&lower(r.1)))
            .then_with(|| l.1["id"].as_str().cmp(&r.1["id"].as_str()))
    });
    let limit = params["limit"]
        .as_u64()
        .map_or(DEFAULT_SEARCH_LIMIT, |l| l as usize);
    let agents: Vec<Value> = ranked
        .into_iter()
        .take(limit)
        .map(|(_, agent, kind, binary)| {
            let or_null = |key: &str| agent.get(key).cloned().unwrap_or(Value::Null);
            let integrity = if kind == "binary" && binary.is_some_and(|b| b.get("sha256").is_some()) {
                "sha256"
            } else {
                "registry"
            };
            json!({"id": agent["id"], "name": agent["name"], "version": agent["version"],
                "description": agent["description"], "authors": agent.get("authors").cloned().unwrap_or(json!([])),
                "license": or_null("license"), "website": or_null("website"), "repository": or_null("repository"),
                "icon": or_null("icon"), "distribution": kind, "integrity": integrity})
        })
        .collect();
    Ok(json!({ "agents": agents }))
}

/// `server.prepareAcpRegistryAgent` as far as the registry: a fresh index (or the
/// cached one), the agent, and a distribution for this platform. Installing agents is
/// not supported here, so a found agent fails `install_failed`.
pub(crate) async fn prepare(caches: &Path, agent_id: &str) -> Result<Value, AcpError> {
    let index = refresh(&mut *CATALOG.lock().await, caches).await?;
    let id = agent_id.trim();
    let agent = index.agents.iter().find(|a| a["id"] == id).ok_or_else(|| {
        AcpError::new(
            "agent_not_found",
            format!("ACP Registry does not contain agent '{id}'."),
        )
    })?;
    if distribution(agent).is_none() {
        let (os, arch) = (std::env::consts::OS, std::env::consts::ARCH);
        let (os, arch) = (
            match os {
                "macos" => "darwin",
                "windows" => "win32",
                other => other,
            },
            match arch {
                "x86_64" => "x64",
                "aarch64" => "arm64",
                other => other,
            },
        );
        return Err(AcpError::new(
            if platform_target().is_none() {
                "unsupported_platform"
            } else {
                "unsupported_distribution"
            },
            format!("ACP Registry agent {id} has no compatible distribution for {os}-{arch}."),
        ));
    }
    Err(AcpError::new(
        "install_failed",
        format!("Could not install ACP Registry agent {id}."),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn agent(id: &str, name: &str, description: &str) -> Value {
        json!({"id": id, "name": name, "version": "1.0.0", "description": description,
            "distribution": {"npx": {"package": format!("{id}@1.0.0")}}})
    }

    #[test]
    fn ranks_follow_the_reference() {
        let claude = agent("claude-acp", "Claude Agent", "ACP wrapper");
        assert_eq!(search_rank(&claude, ""), Some(100));
        assert_eq!(search_rank(&claude, "claude-acp"), Some(0));
        assert_eq!(search_rank(&claude, "claude"), Some(10));
        assert_eq!(search_rank(&claude, "agent"), Some(20));
        assert_eq!(search_rank(&claude, "wrapper"), Some(50));
        assert_eq!(search_rank(&claude, "nothing"), None);
    }

    #[test]
    fn invalid_agents_are_dropped_and_names_trimmed() {
        let text = json!({"version": "1", "agents": [
            agent("good", "  Good  ", "fine"),
            agent("Bad Id", "Bad", "rejected"),
            {"id": "unpinned", "name": "Unpinned", "version": "1", "description": "",
                "distribution": {"npx": {"package": "unpinned@latest"}}},
        ]})
        .to_string();
        let index = decode_registry(&text).unwrap();
        assert_eq!(index.agents.len(), 1);
        assert_eq!(index.agents[0]["name"], "Good");
        assert!(decode_registry("{").is_err());
        assert!(decode_registry(r#"{"version":"1"}"#).is_err());
    }
}
