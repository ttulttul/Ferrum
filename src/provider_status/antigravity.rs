//! Antigravity's status check, as the reference's `checkAntigravityProvider`
//! with the driver's health probe. The probe never starts the runtime (a
//! PyInstaller bundle that unpacks about 1 GB per launch): it checks the
//! sign-in settings and resolves the installed runtime from disk
//! (`AntigravityInstallation.resolve`), a custom executable, the managed
//! install under `<home>/tools/antigravity-acp/<platform>-<arch>`, or
//! the managed Antigravity CLI or `agy` on PATH.
//! A healthy runtime then has its Google quota read from the profile's
//! stored sign-in, when there is one.
use super::{maintenance, now_iso, unavailable_usage, Draft, ProcessEnv};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};
use std::time::Duration;

const RECORD_MAX_BYTES: u64 = 8 * 1024;
const SIGN_IN_MESSAGE: &str = "Sign in with Google to use Antigravity.";
const AUTH_UNCHECKED_MESSAGE: &str =
    "Antigravity is installed. Google account access is not checked yet.";
const DAILY_ENDPOINT: &str = "https://daily-cloudcode-pa.googleapis.com";
const PROD_ENDPOINT: &str = "https://cloudcode-pa.googleapis.com";

/// The instance's sign-in settings (`AntigravityAuthConfig`).
pub(super) struct Auth {
    pub(super) method: String,
    pub(super) api_key: String,
    pub(super) gcp_project: String,
    pub(super) gcp_location: String,
}

impl Auth {
    pub(super) fn of(config: &Value) -> Self {
        let text = |key: &str| config[key].as_str().unwrap_or("").to_owned();
        Self {
            method: config["authMethod"]
                .as_str()
                .unwrap_or("oauth-personal")
                .to_owned(),
            // `settingsSecretLiteral`: a referenced secret is not a key.
            api_key: text("apiKey"),
            gcp_project: text("gcpProject"),
            gcp_location: text("gcpLocation"),
        }
    }

    /// `antigravityAuthLabel`.
    fn label(&self) -> &'static str {
        match self.method.as_str() {
            "oauth-business" => "Gemini Enterprise",
            "gemini-api-key" => "Gemini API key",
            "agent-platform" => "Agent Platform",
            _ => "Google account",
        }
    }

    /// `antigravityAuthConfigIssue`.
    pub(super) fn config_issue(&self) -> Option<&'static str> {
        let gcp = !self.gcp_project.is_empty() && !self.gcp_location.is_empty();
        match self.method.as_str() {
            "oauth-business" if !gcp => Some(
                "Gemini Enterprise needs a GCP project and location in the Antigravity provider settings.",
            ),
            "gemini-api-key" if self.api_key.is_empty() => {
                Some("Enter a Gemini API key in the Antigravity provider settings.")
            }
            "agent-platform" if self.api_key.is_empty() && !gcp => Some(
                "Agent Platform needs an API key, or a GCP project and location, in the Antigravity provider settings.",
            ),
            _ => None,
        }
    }

    fn uses_credentials(&self) -> bool {
        matches!(self.method.as_str(), "gemini-api-key" | "agent-platform")
    }
}

/// Node's `process.platform` and `process.arch`.
fn target() -> (&'static str, &'static str) {
    let platform = match std::env::consts::OS {
        "macos" => "darwin",
        "windows" => "win32",
        other => other,
    };
    let arch = match std::env::consts::ARCH {
        "x86_64" => "x64",
        "aarch64" => "arm64",
        other => other,
    };
    (platform, arch)
}

/// A resolved runtime (`AntigravityExecutable`): what to run and its
/// version, when known before it answers `initialize`.
pub(super) struct Executable {
    pub(super) version: Option<String>,
    pub(super) path: PathBuf,
}

fn is_executable_file(path: &Path, bytes: Option<u64>) -> bool {
    use std::os::unix::fs::PermissionsExt;
    std::fs::metadata(path).is_ok_and(|m| {
        m.is_file() && bytes.is_none_or(|b| m.len() == b) && m.permissions().mode() & 0o111 != 0
    })
}

fn read_record(path: &Path) -> Result<Value, String> {
    let invalid = || "The managed runtime record is invalid. Reinstall Antigravity.".to_owned();
    let meta = std::fs::metadata(path).map_err(|_| read_failure())?;
    if !meta.is_file() || meta.len() > RECORD_MAX_BYTES {
        return Err(invalid());
    }
    let text = std::fs::read_to_string(path).map_err(|_| read_failure())?;
    serde_json::from_str(&text).map_err(|_| read_failure())
}

fn read_failure() -> String {
    "Could not read the Antigravity installation. Reinstall it or set a custom executable path."
        .into()
}

fn release_id(value: &Value) -> Option<&str> {
    value
        .as_str()
        .filter(|id| id.len() == 64 && id.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f')))
}

/// `completedRelease`: the managed release `release`, complete on disk.
fn completed_release(versions: &Path, release: &str) -> Result<Executable, String> {
    let directory = versions.join(release);
    let record = read_record(&directory.join(".install-complete.json"))?;
    let incomplete = || "The managed Antigravity runtime is incomplete. Reinstall it.".to_owned();
    let valid_shape = release_id(&record["releaseId"]).is_some()
        && record["version"].is_string()
        && record["executable"]["name"].is_string()
        && record["executable"]["bytes"].is_number()
        && (record["harness"].is_null()
            || (record["harness"]["name"].is_string() && record["harness"]["bytes"].is_number()));
    if !valid_shape {
        return Err(read_failure());
    }
    let windows = target().0 == "win32";
    let cli = if windows { "agy.exe" } else { "antigravity" };
    // Releases before the Antigravity CLI (an ACP server with a harness)
    // no longer run (#38970).
    if record["releaseId"] == release && record["executable"]["name"] != cli {
        return Err(format!(
            "Antigravity {} predates the Antigravity CLI and no longer works with this T3 Code release. Update it in Antigravity setup.",
            record["version"].as_str().unwrap_or("")
        ));
    }
    let bytes = |v: &Value| {
        v.as_f64()
            .filter(|b| b.fract() == 0.0 && *b > 0.0 && *b <= 9_007_199_254_740_991.0)
            .map(|b| b as u64)
    };
    let executable_bytes = bytes(&record["executable"]["bytes"]);
    if record["releaseId"] != release
        || executable_bytes.is_none()
        || record["version"].as_str().unwrap_or("").trim().is_empty()
        || !is_executable_file(&directory.join(cli), executable_bytes)
    {
        return Err(incomplete());
    }
    Ok(Executable {
        version: record["version"].as_str().map(str::to_owned),
        path: directory.join(cli),
    })
}

fn is_cli(candidates: &[&Path]) -> bool {
    let pattern = regex::Regex::new(r"(?i)^(?:agy|antigravity)(?:\.(?:exe|cmd|bat))?$").unwrap();
    candidates.iter().any(|c| {
        pattern.is_match(
            &c.file_name()
                .map(|n| n.to_string_lossy().into_owned())
                .unwrap_or_default(),
        )
    })
}

/// `fromExternal`: a custom or PATH executable, if it can run.
fn from_external(candidate: &Path, versions: &Path) -> Result<Option<Executable>, String> {
    if !is_executable_file(candidate, None) {
        return Ok(None);
    }
    let real = std::fs::canonicalize(candidate).map_err(|_| read_failure())?;
    let directory = real.parent().map(Path::to_path_buf).unwrap_or_default();
    if let Ok(real_versions) = std::fs::canonicalize(versions) {
        let name = directory
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_default();
        if directory.parent() == Some(real_versions.as_path()) && release_id(&json!(name)).is_some()
        {
            return completed_release(&real_versions, &name).map(Some);
        }
    }
    // The candidate as found, not its real path, so version-manager shims work.
    // Only the agy CLI runs now; an ACP server with a harness does not (#38970).
    Ok(is_cli(&[candidate, &real]).then(|| Executable {
        version: None,
        path: candidate.to_path_buf(),
    }))
}

/// `commandCandidates`: a path as given, else each PATH entry's match.
fn candidates(command: &str, env: &ProcessEnv) -> Vec<PathBuf> {
    if command.contains('/') || command.contains('\\') {
        return maintenance::resolve_command_path(command, env)
            .map(|p| std::env::current_dir().unwrap_or_default().join(p))
            .into_iter()
            .collect();
    }
    env.get("PATH")
        .map(String::as_str)
        .unwrap_or("")
        .split(':')
        .filter(|entry| !entry.is_empty())
        .filter_map(|entry| {
            let mut single = env.clone();
            single.insert("PATH".into(), entry.to_owned());
            maintenance::resolve_command_path(command, &single)
                .map(|p| std::env::current_dir().unwrap_or_default().join(p))
        })
        .collect()
}

/// `AntigravityInstallation.resolve`.
pub(super) fn resolve_executable(
    base: &Path,
    binary_path: &str,
    env: &ProcessEnv,
    manifest: &Value,
) -> Result<Executable, String> {
    let (platform, arch) = target();
    let managed = base
        .join("tools")
        .join("antigravity-acp")
        .join(format!("{platform}-{arch}"));
    let versions = managed.join("versions");
    let override_path = super::expand_home(binary_path.trim());
    if !override_path.is_empty() {
        for candidate in candidates(&override_path, env) {
            if let Some(selected) = from_external(&candidate, &versions)? {
                return Ok(selected);
            }
        }
        return Err(
            "The custom Antigravity executable is missing or not executable, or it is not the agy CLI."
                .into(),
        );
    }
    let active = managed.join("active.json");
    if active.exists() {
        let record = read_record(&active)?;
        let release = release_id(&record["releaseId"]).ok_or_else(read_failure)?;
        let installed = completed_release(&versions, release)?;
        let version = installed.version.clone().unwrap_or_default();
        let withdrawn =
            super::resolve_compatibility(&manifest["compatibility"], "antigravity", Some(&version))
                .or_else(|| {
                    super::resolve_compatibility(
                        &crate::model_manifest::BUNDLED["compatibility"],
                        "antigravity",
                        Some(&version),
                    )
                })
                .is_some_and(|a| matches!(a["status"].as_str(), Some("broken" | "unsupported")));
        if withdrawn {
            return Err(format!(
                "Antigravity {version} no longer works with this T3 Code release. Update it in Antigravity setup."
            ));
        }
        return Ok(installed);
    }
    for command in ["agy"] {
        for candidate in candidates(command, env) {
            if let Some(selected) = from_external(&candidate, &versions)? {
                return Ok(selected);
            }
        }
    }
    let supported =
        matches!(platform, "darwin" | "linux" | "win32") && matches!(arch, "x64" | "arm64");
    Err(if supported {
        "Antigravity ACP runtime is not installed. Install it in this environment's provider settings or set a custom ACP executable path. The Antigravity app, IDE, and CLI are separate installations.".into()
    } else {
        format!("Google does not publish an Antigravity runtime for {platform}-{arch}. Use a supported environment or a custom executable.")
    })
}

/// `resolveAntigravityInstanceDirectories(...).profile`.
fn profile_directory(state_dir: &Path, instance_id: &str) -> PathBuf {
    state_dir
        .join("providers")
        .join("antigravity")
        .join(hex::encode(Sha256::digest(instance_id.as_bytes())))
}

/// `isAntigravityTextGenerationAvailable`: no global hooks or MCP servers
/// in the profile that could act before a helper denies a tool.
pub(super) fn text_generation_available(profile: &Path) -> bool {
    for name in ["hooks.json", "mcp_config.json"] {
        let path = profile.join("config").join(name);
        if !path.exists() {
            continue;
        }
        let Ok(meta) = std::fs::metadata(&path) else {
            return false;
        };
        if !meta.is_file() || meta.len() > 64_000 {
            return false;
        }
        let empty = std::fs::read_to_string(&path).ok().is_some_and(|text| {
            if text.trim().is_empty() {
                return true;
            }
            let Ok(Value::Object(configuration)) = serde_json::from_str::<Value>(&text) else {
                return false;
            };
            let key = if name == "hooks.json" {
                "hooks"
            } else {
                "mcpServers"
            };
            match configuration.get(key) {
                Some(Value::Object(inner)) => inner.is_empty(),
                Some(_) => false,
                None => configuration.is_empty(),
            }
        });
        if !empty {
            return false;
        }
    }
    true
}

// ---------------------------------------------------------------------------
// Usage

fn quota_window(
    id: &str,
    label: &str,
    quota: &Value,
    kind: &str,
    minutes: Option<i64>,
) -> Option<Value> {
    let remaining = quota["remainingFraction"]
        .as_f64()
        .filter(|r| r.is_finite())?;
    let mut window = json!({"id": id, "label": label, "kind": kind,
        "usedPercent": super::js_number((1.0 - remaining.clamp(0.0, 1.0)) * 100.0)});
    if let Some(reset) = quota["resetTime"].as_str().and_then(parse_ms) {
        window["resetsAt"] = json!(crate::auth_service::iso(reset));
    }
    if let Some(minutes) = minutes {
        window["windowDurationMins"] = json!(minutes);
    }
    Some(window)
}

fn parse_ms(text: &str) -> Option<i64> {
    time::OffsetDateTime::parse(text, &time::format_description::well_known::Rfc3339)
        .ok()
        .map(|d| (d.unix_timestamp_nanos() / 1_000_000) as i64)
}

/// `summaryWindows`.
fn summary_windows(summary: &Value, requested_at: i64) -> Vec<Value> {
    let has_group_buckets = summary["groups"].as_array().is_some_and(|g| {
        g.iter()
            .any(|g| g["buckets"].as_array().is_some_and(|b| !b.is_empty()))
    });
    let groups: Vec<Value> = if has_group_buckets {
        summary["groups"].as_array().cloned().unwrap_or_default()
    } else {
        vec![json!({"buckets": summary["buckets"]})]
    };
    let mut windows: Vec<Value> = Vec::new();
    for group in &groups {
        let group_name = group["displayName"]
            .as_str()
            .map(str::trim)
            .filter(|n| !n.is_empty());
        for bucket in group["buckets"].as_array().into_iter().flatten() {
            if bucket["disabled"] == true {
                continue;
            }
            let id = bucket["bucketId"].as_str().unwrap_or("");
            let family = if id.starts_with("gemini-argon-") {
                group_name.unwrap_or("Gemini Argon").to_owned()
            } else if id.starts_with("gemini-carbon-") {
                group_name.unwrap_or("Gemini Carbon").to_owned()
            } else if id.starts_with("gemini-") {
                "Gemini".into()
            } else if id.starts_with("3p-") {
                "Claude & GPT (shared)".into()
            } else {
                group_name.unwrap_or(id).to_owned()
            };
            let window = bucket["window"].as_str();
            let weekly = window == Some("weekly");
            let session = window == Some("5h");
            let minutes = match window {
                Some("5h") => Some(300),
                Some("weekly") => Some(10080),
                _ => None,
            };
            let untouched = bucket["remainingFraction"] == 1
                && bucket.get("description").is_none()
                && minutes.is_some_and(|m| {
                    bucket["resetTime"]
                        .as_str()
                        .and_then(parse_ms)
                        .is_some_and(|reset| (reset - requested_at - m * 60_000).abs() <= 60_000)
                });
            let mut quota = bucket.clone();
            if untouched {
                quota.as_object_mut().map(|o| o.remove("resetTime"));
            }
            let label = format!(
                "{family} · {}",
                if weekly {
                    "Weekly".to_owned()
                } else if session {
                    "5-hour".to_owned()
                } else {
                    bucket["displayName"]
                        .as_str()
                        .map(str::trim)
                        .filter(|n| !n.is_empty())
                        .unwrap_or("Quota")
                        .to_owned()
                }
            );
            let kind = if weekly {
                "weekly"
            } else if session {
                "session"
            } else {
                "other"
            };
            if let Some(window) = quota_window(id, &label, &quota, kind, minutes) {
                match windows.iter_mut().find(|w| w["id"] == id) {
                    Some(existing) => *existing = window,
                    None => windows.push(window),
                }
            }
        }
    }
    windows
}

/// `modelWindows`.
fn model_windows(models: &Value) -> Vec<Value> {
    let mut windows: Vec<Value> = Vec::new();
    for (model_id, model) in models["models"].as_object().into_iter().flatten() {
        let provider = model["modelProvider"].as_str();
        let family = if provider == Some("MODEL_PROVIDER_GOOGLE") || model_id.starts_with("gemini-")
        {
            "Gemini"
        } else if provider == Some("MODEL_PROVIDER_ANTHROPIC") || model_id.starts_with("claude-") {
            "Claude"
        } else if provider == Some("MODEL_PROVIDER_OPENAI") || model_id.starts_with("gpt-") {
            "GPT"
        } else {
            continue;
        };
        for (suffix, quota) in [
            ("quota", &model["quotaInfo"]),
            ("weekly", &model["weeklyQuotaInfo"]),
        ] {
            if !quota.is_object() {
                continue;
            }
            let weekly = suffix == "weekly";
            let Some(window) = quota_window(
                &format!("{}-{suffix}", family.to_lowercase()),
                &format!(
                    "{family} · {}",
                    if weekly { "Weekly" } else { "Reported quota" }
                ),
                quota,
                if weekly { "weekly" } else { "other" },
                weekly.then_some(10080),
            ) else {
                continue;
            };
            match windows.iter_mut().find(|w| w["id"] == window["id"]) {
                Some(existing) => {
                    if window["usedPercent"].as_f64() > existing["usedPercent"].as_f64() {
                        *existing = window;
                    }
                }
                None => windows.push(window),
            }
        }
    }
    windows
}

/// `makeAntigravityUsageProbe(...).probe`.
async fn read_usage(profile: &Path, method: &str) -> Value {
    let checked_at = now_iso();
    if method != "oauth-personal" && method != "oauth-business" {
        return unavailable_usage(&checked_at, "unsupported", None);
    }
    let token_path = profile.join("antigravity-acp").join("acp_token.json");
    if !token_path.exists() {
        return unavailable_usage(
            &checked_at,
            "unsupported",
            Some("Sign in with Google to see Antigravity limits."),
        );
    }
    let failed = || {
        unavailable_usage(
            &checked_at,
            "probeFailed",
            Some("Could not read Antigravity limits. Refresh provider status to retry."),
        )
    };
    let read = async {
        let token: Value =
            serde_json::from_str(&std::fs::read_to_string(&token_path).ok()?).ok()?;
        let field = |k: &str| {
            token[k]
                .as_str()
                .filter(|v| !v.is_empty())
                .map(str::to_owned)
        };
        let (client_id, client_secret, refresh) = (
            field("client_id")?,
            field("client_secret")?,
            field("refresh_token")?,
        );
        let project = token["project_id"].as_str().map(str::to_owned);
        let client = super::http_builder()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(Duration::from_secs(10))
            .build()
            .ok()?;
        let refreshed: Value = client
            .post("https://oauth2.googleapis.com/token")
            .form(&[
                ("client_id", client_id.as_str()),
                ("client_secret", client_secret.as_str()),
                ("refresh_token", refresh.as_str()),
                ("grant_type", "refresh_token"),
            ])
            .send()
            .await
            .ok()?
            .error_for_status()
            .ok()?
            .json()
            .await
            .ok()?;
        let access = refreshed["access_token"]
            .as_str()
            .filter(|t| !t.is_empty())?
            .to_owned();
        refreshed["expires_in"].as_f64().filter(|e| *e > 0.0)?;
        let fingerprint = hex::encode(Sha256::digest(
            format!(
                "{client_id}\0{refresh}\0{}",
                project.as_deref().unwrap_or("")
            )
            .as_bytes(),
        ));
        let account = match client
            .get("https://www.googleapis.com/oauth2/v2/userinfo")
            .bearer_auth(&access)
            .send()
            .await
        {
            Ok(response) => match response.error_for_status() {
                Ok(response) => response
                    .json::<Value>()
                    .await
                    .ok()
                    .and_then(|info| {
                        info["id"]
                            .as_str()
                            .filter(|i| !i.is_empty())
                            .map(str::to_owned)
                    })
                    .map(|id| {
                        hex::encode(Sha256::digest(format!("google-account\0{id}").as_bytes()))
                    })
                    .unwrap_or_else(|| fingerprint.clone()),
                Err(_) => fingerprint.clone(),
            },
            Err(_) => fingerprint.clone(),
        };
        let endpoints = if method == "oauth-business" {
            [PROD_ENDPOINT, DAILY_ENDPOINT]
        } else {
            [DAILY_ENDPOINT, PROD_ENDPOINT]
        };
        let (platform, arch) = target();
        let agent = format!(
            "antigravity/hub/2.24.0 (aidev_client; os_type={platform}; arch={arch}; cl=963137146)"
        );
        let body = match &project {
            Some(project) => json!({"project": project}),
            None => json!({}),
        };
        let fetch = |operation: &'static str| {
            let client = client.clone();
            let access = access.clone();
            let agent = agent.clone();
            let body = body.clone();
            async move {
                let mut all = Vec::new();
                for endpoint in endpoints {
                    let requested_at = crate::platform::now_ms();
                    let response = client
                        .post(format!("{endpoint}/v1internal:{operation}"))
                        .bearer_auth(&access)
                        .header("User-Agent", &agent)
                        .json(&body)
                        .send()
                        .await
                        .ok()
                        .and_then(|r| r.error_for_status().ok());
                    let Some(response) = response else { continue };
                    let Ok(value) = response.json::<Value>().await else {
                        continue;
                    };
                    all.extend(if operation == "retrieveUserQuotaSummary" {
                        summary_windows(&value, requested_at)
                    } else {
                        model_windows(&value)
                    });
                }
                let mut windows: Vec<Value> = Vec::new();
                for window in all {
                    if !windows.iter().any(|w| w["id"] == window["id"]) {
                        windows.push(window);
                    }
                }
                windows
            }
        };
        let mut windows = fetch("retrieveUserQuotaSummary").await;
        if windows.is_empty() {
            windows = fetch("fetchAvailableModels").await;
        }
        if windows.is_empty() {
            return Some(unavailable_usage(
                &checked_at,
                "probeFailed",
                Some("Antigravity did not return usable quota information."),
            ));
        }
        let mut limits = super::usage_limits(&checked_at, windows);
        limits["credentialFingerprint"] = json!(account);
        Some(limits)
    };
    tokio::time::timeout(Duration::from_secs(30), read)
        .await
        .ok()
        .flatten()
        .unwrap_or_else(failed)
}

// ---------------------------------------------------------------------------
// Models

/// `ANTIGRAVITY_DEFAULT_MODEL`: the alias of the session's current model.
const DEFAULT_MODEL_ALIAS: &str = "antigravity-default";
/// `RUNTIME_MODES_WITHOUT_AUTO`.
const RUNTIME_MODES_WITHOUT_AUTO: [&str; 3] =
    ["approval-required", "auto-accept-edits", "full-access"];
const MAX_OPTION_CHOICES: usize = 64;
const MAX_TEXT_LENGTH: usize = 256;
const MAX_DESCRIPTION_LENGTH: usize = 1_024;

type ThinkingLevels = std::collections::HashMap<String, Option<Value>>;

/// What Antigravity sessions told the provider (the reference provider's
/// own draft, which its health checks update rather than replace): the
/// account a session started under, or a sign-in a session asked for.
#[derive(Clone, Debug, Default)]
pub(super) struct Session {
    /// The instance's settings when a session reported.
    pub(super) fingerprint: Value,
    /// A session started (`onSessionStarted`); false once one asked to sign
    /// in (`onAuthRequired`).
    pub(super) signed_in: bool,
    /// `initialize`'s `agentInfo.version`.
    pub(super) version: Option<String>,
    /// `buildAntigravityModelsFromSession`'s models.
    pub(super) models: Vec<Value>,
    /// `antigravityRuntimeModes`.
    pub(super) runtime_modes: Vec<&'static str>,
    /// The thinking options each model reported when selected; `None`
    /// means it has none.
    pub(super) thinking_levels: ThinkingLevels,
}

impl Session {
    /// `onSessionStarted`: the session's account, version, runtime modes and
    /// models.
    pub(super) fn started(&mut self, initialized: &Value, setup: &Value) {
        let options = setup["configOptions"].as_array().map(Vec::as_slice);
        observe_thinking_levels(&mut self.thinking_levels, options);
        self.signed_in = true;
        if let Some(version) = initialized["agentInfo"]["version"]
            .as_str()
            .filter(|v| !v.is_empty())
        {
            self.version = Some(version.to_owned());
        }
        self.runtime_modes = runtime_modes(setup);
        self.models = build_models(setup, &self.thinking_levels);
    }

    /// `onConfigOptionsUpdated`: the models as the session's new options
    /// list them, for a signed-in account.
    pub(super) fn config_options_updated(&mut self, options: &[Value]) {
        observe_thinking_levels(&mut self.thinking_levels, Some(options));
        if self.signed_in {
            self.models = build_models(&json!({ "configOptions": options }), &self.thinking_levels);
        }
    }

    /// `onAuthRequired` (`clearAccountMetadata`): signed out, without models.
    pub(super) fn sign_in_required(&mut self) {
        self.signed_in = false;
        self.models.clear();
        self.runtime_modes = RUNTIME_MODES_WITHOUT_AUTO.to_vec();
        self.thinking_levels.clear();
    }
}

/// `antigravityThinkingConfig`: the session's thinking level option.
fn thinking_config(options: Option<&[Value]>) -> Option<&Value> {
    options?
        .iter()
        .find(|option| option["category"] == "thought_level")
        .filter(|option| option["type"] == "select")
}

/// The session's model option (`id` or `category` "model").
fn model_config(options: Option<&[Value]>) -> Option<&Value> {
    options?
        .iter()
        .find(|option| option["id"] == "model" || option["category"] == "model")
}

/// A select option's choices, groups flattened (`flattenSelectChoices`).
pub(super) fn flatten_choices(option: &Value) -> Vec<&Value> {
    option["options"]
        .as_array()
        .into_iter()
        .flatten()
        .flat_map(|entry| match entry.get("value") {
            Some(_) => vec![entry],
            None => entry["options"].as_array().into_iter().flatten().collect(),
        })
        .collect()
}

/// `observeThinkingLevels`: the selected model's thinking options.
fn observe_thinking_levels(levels: &mut ThinkingLevels, options: Option<&[Value]>) {
    let Some(model) = model_config(options).filter(|m| m["type"] == "select") else {
        return;
    };
    if let Some(current) = model["currentValue"].as_str().filter(|c| !c.is_empty()) {
        levels.insert(current.to_owned(), thinking_config(options).cloned());
    }
}

/// `boundedText`: trimmed and cut to `maximum` characters.
fn bounded_text(value: &Value, maximum: usize) -> String {
    value
        .as_str()
        .unwrap_or("")
        .trim()
        .chars()
        .take(maximum)
        .collect()
}

/// `boundedOpaqueValue`.
fn bounded_opaque(value: &str, maximum: usize) -> Option<String> {
    (!value.is_empty() && value == value.trim() && value.encode_utf16().count() <= maximum)
        .then(|| value.to_owned())
}

/// `choiceId`.
fn choice_id(value: &str) -> Option<String> {
    if value.is_empty() {
        Some("_t3/empty".into())
    } else {
        bounded_opaque(value, MAX_TEXT_LENGTH)
    }
}

/// `acpProviderOptionDescriptors` of the thinking level option alone: its
/// select descriptor, if it has choices.
fn thinking_descriptor(option: &Value) -> Option<Value> {
    if matches!(
        option["category"].as_str(),
        Some("model" | "collaboration_mode")
    ) || option["type"] == "boolean"
    {
        return None;
    }
    let id = bounded_opaque(option["id"].as_str()?, MAX_TEXT_LENGTH)?;
    let mut seen = std::collections::HashSet::new();
    let mut choices = Vec::new();
    for candidate in flatten_choices(option) {
        let Some(choice) = candidate["value"].as_str().and_then(choice_id) else {
            continue;
        };
        if !seen.insert(choice.clone()) {
            continue;
        }
        let label = bounded_text(&candidate["name"], MAX_TEXT_LENGTH);
        let mut entry = serde_json::Map::new();
        entry.insert("id".into(), json!(choice));
        entry.insert(
            "label".into(),
            json!(if label.is_empty() {
                choice.clone()
            } else {
                label
            }),
        );
        let description = bounded_text(&candidate["description"], MAX_DESCRIPTION_LENGTH);
        if !description.is_empty() {
            entry.insert("description".into(), json!(description));
        }
        choices.push(Value::Object(entry));
        if choices.len() == MAX_OPTION_CHOICES {
            break;
        }
    }
    if choices.is_empty() {
        return None;
    }
    let mut descriptor = serde_json::Map::new();
    let label = bounded_text(&option["name"], MAX_TEXT_LENGTH);
    descriptor.insert("id".into(), json!(id));
    descriptor.insert(
        "label".into(),
        json!(if label.is_empty() { id.clone() } else { label }),
    );
    let description = bounded_text(&option["description"], MAX_DESCRIPTION_LENGTH);
    if !description.is_empty() {
        descriptor.insert("description".into(), json!(description));
    }
    descriptor.insert("type".into(), json!("select"));
    let current = option["currentValue"].as_str().and_then(choice_id);
    let listed = current
        .as_ref()
        .is_some_and(|current| choices.iter().any(|c| c["id"] == current.as_str()));
    descriptor.insert("options".into(), Value::Array(choices));
    if listed {
        descriptor.insert("currentValue".into(), json!(current));
    }
    descriptor.insert("role".into(), json!("reasoning"));
    Some(Value::Object(descriptor))
}

/// `thinkingCapabilities`: a model's thinking levels, with the selected
/// level only where this model reported it.
fn thinking_capabilities(config: Option<&Value>, observed: bool) -> Value {
    let Some(mut descriptor) = config.and_then(thinking_descriptor) else {
        return json!({"optionDescriptors": []});
    };
    if !observed {
        if let Some(object) = descriptor.as_object_mut() {
            object.shift_remove("currentValue");
        }
    }
    json!({ "optionDescriptors": [descriptor] })
}

/// `buildAntigravityModelsFromSession`: the session's models (its `model`
/// option, else `models.availableModels`), each once, a later model of the
/// same name labelled with its id, with its thinking levels.
pub(super) fn build_models(setup: &Value, thinking_levels: &ThinkingLevels) -> Vec<Value> {
    let options = setup["configOptions"].as_array().map(Vec::as_slice);
    let config = model_config(options);
    let select = config.filter(|c| c["type"] == "select");
    let current = match select {
        Some(config) => config["currentValue"].as_str(),
        None => setup["models"]["currentModelId"].as_str(),
    };
    let entries: Vec<(String, String)> = match (config, select) {
        (_, Some(select)) => flatten_choices(select)
            .into_iter()
            .filter_map(|entry| {
                Some((
                    entry["value"].as_str()?.to_owned(),
                    entry["name"].as_str().unwrap_or("").to_owned(),
                ))
            })
            .collect(),
        (None, None) => setup["models"]["availableModels"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|model| {
                Some((
                    model["modelId"].as_str()?.to_owned(),
                    model["name"].as_str().unwrap_or("").to_owned(),
                ))
            })
            .collect(),
        (Some(_), None) => Vec::new(),
    };
    let current_thinking = thinking_config(options);
    let mut seen = std::collections::HashSet::new();
    let mut names = std::collections::HashSet::new();
    let mut models = Vec::new();
    for (value, name) in entries {
        if value.trim().is_empty() || !seen.insert(value.clone()) {
            continue;
        }
        let base_name = if name.trim().is_empty() {
            value.clone()
        } else {
            name
        };
        let label = if names.contains(&base_name) {
            format!("{base_name} ({value})")
        } else {
            base_name.clone()
        };
        names.insert(base_name);
        let is_current = Some(value.as_str()) == current;
        let observed: Option<Option<&Value>> = if is_current {
            Some(current_thinking)
        } else {
            thinking_levels.get(&value).map(Option::as_ref)
        };
        let mut model = serde_json::Map::new();
        model.insert("slug".into(), json!(value));
        model.insert("name".into(), json!(label));
        model.insert("isCustom".into(), json!(false));
        if is_current {
            model.insert("isDefault".into(), json!(true));
            model.insert("aliases".into(), json!([DEFAULT_MODEL_ALIAS]));
        }
        model.insert(
            "capabilities".into(),
            match observed {
                None => thinking_capabilities(current_thinking, false),
                Some(observed) => thinking_capabilities(observed, true),
            },
        );
        models.push(Value::Object(model));
    }
    models
}

/// `antigravityRuntimeModes` (with `parseSessionModeState`): Auto only where
/// the session offers agy's own.
fn runtime_modes(setup: &Value) -> Vec<&'static str> {
    let mode_config = setup["configOptions"]
        .as_array()
        .into_iter()
        .flatten()
        .find(|o| o["category"] == "mode" && o["type"] == "select");
    let pairs = |modes: Vec<&Value>, id: &str| -> Vec<(String, String)> {
        modes
            .into_iter()
            .map(|m| {
                (
                    m[id].as_str().unwrap_or("").trim().to_owned(),
                    m["name"].as_str().unwrap_or("").trim().to_owned(),
                )
            })
            .collect()
    };
    let (current, available) = if !setup["modes"].is_null() {
        (
            setup["modes"]["currentModeId"].as_str(),
            pairs(
                setup["modes"]["availableModes"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .collect(),
                "id",
            ),
        )
    } else if let Some(config) = mode_config {
        (
            config["currentValue"].as_str(),
            pairs(flatten_choices(config), "value"),
        )
    } else {
        (None, Vec::new())
    };
    let offers_auto = current.is_some_and(|c| !c.trim().is_empty())
        && available
            .iter()
            .any(|(id, name)| id == "auto" && !name.is_empty());
    if offers_auto {
        vec![
            "approval-required",
            "auto-accept-edits",
            "auto",
            "full-access",
        ]
    } else {
        RUNTIME_MODES_WITHOUT_AUTO.to_vec()
    }
}

/// `applyModelCatalog` with the manifest's Antigravity catalog: legacy
/// flags, and the catalog's default chat model as the default.
fn apply_catalog(models: &mut [Value], manifest: &Value) {
    let catalog = if manifest["providers"]["antigravity"].is_object() {
        &manifest["providers"]["antigravity"]
    } else {
        &crate::model_manifest::BUNDLED["providers"]["antigravity"]
    };
    let entries = catalog["models"].as_array().cloned().unwrap_or_default();
    for model in models.iter_mut() {
        if model["isCustom"] == true {
            continue;
        }
        let legacy = entries
            .iter()
            .find(|e| e["slug"] == model["slug"])
            .is_some_and(|e| e["status"] == "legacy");
        if legacy {
            model["isLegacy"] = json!(true);
        } else if let Some(object) = model.as_object_mut() {
            object.shift_remove("isLegacy");
        }
    }
    let Some(requested) = catalog["defaults"]["chat"].as_str() else {
        return;
    };
    if !models.iter().any(|m| m["slug"] == requested) {
        return;
    }
    let Some(previous) = models
        .iter()
        .position(|m| m["isDefault"] == true && m["slug"] != requested)
    else {
        return;
    };
    let moved: Vec<Value> = models[previous]["aliases"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    if let Some(object) = models[previous].as_object_mut() {
        object.shift_remove("isDefault");
        object.shift_remove("aliases");
    }
    if let Some(model) = models.iter_mut().find(|m| m["slug"] == requested) {
        let mut aliases: Vec<Value> = model["aliases"].as_array().cloned().unwrap_or_default();
        for alias in moved {
            if !aliases.contains(&alias) {
                aliases.push(alias);
            }
        }
        model["isDefault"] = json!(true);
        if !aliases.is_empty() {
            model["aliases"] = Value::Array(aliases);
        }
    }
}

// ---------------------------------------------------------------------------
// Check

/// `checkAntigravityProvider`: the installation probe applied to what
/// sessions reported (`session`), as the reference's `applyCheck` updates
/// its provider's draft.
#[allow(clippy::too_many_arguments)]
pub(super) async fn check(
    config: &Value,
    env: &ProcessEnv,
    instance_id: &str,
    base: &Path,
    state_dir: &Path,
    manifest: &Value,
    session: Option<&Session>,
) -> Draft {
    let auth = Auth::of(config);
    let checked_at = now_iso();
    let profile = profile_directory(state_dir, instance_id);
    let probe = match auth.config_issue() {
        Some(issue) => Err((issue.to_owned(), false)),
        None => resolve_executable(
            base,
            config["binaryPath"].as_str().unwrap_or(""),
            env,
            manifest,
        )
        .map_err(|detail| {
            let missing =
                regex::Regex::new(r"(?i)not installed|missing|incomplete|does not publish")
                    .unwrap()
                    .is_match(&detail);
            (detail, missing)
        }),
    };
    let base_auth = json!({"status": "unknown", "type": auth.method});
    let unchecked = if auth.uses_credentials() {
        format!(
            "Antigravity is installed. Connect with {} to load models.",
            auth.label()
        )
    } else {
        AUTH_UNCHECKED_MESSAGE.to_owned()
    };
    let account = session.map(|session| {
        let auth_value = if session.signed_in {
            json!({"status": "authenticated", "type": auth.method, "label": auth.label()})
        } else if auth.uses_credentials() {
            json!({"status": "unauthenticated", "type": auth.method, "label": auth.label()})
        } else {
            json!({"status": "unauthenticated"})
        };
        let mut models = session.models.clone();
        apply_catalog(&mut models, manifest);
        (session, auth_value, models)
    });
    let sign_in_message = if auth.uses_credentials() {
        format!(
            "Connect with {} to use Antigravity and load models.",
            auth.label()
        )
    } else {
        SIGN_IN_MESSAGE.to_owned()
    };
    match probe {
        Err((detail, missing)) => {
            let message = if detail.trim().is_empty() {
                "Antigravity could not complete its local health check.".to_owned()
            } else {
                detail.trim().to_owned()
            };
            let mut extra = vec![
                ("usageLimits", Value::Null),
                ("supportsTextGeneration", json!(false)),
                ("workspaceSnapshots", json!([])),
            ];
            let (auth_value, version, models) = match account {
                Some((session, auth_value, models)) => {
                    extra.push(("supportedRuntimeModes", json!(session.runtime_modes)));
                    let models = if missing || !session.signed_in {
                        Vec::new()
                    } else {
                        models
                    };
                    (auth_value, session.version.clone(), models)
                }
                None => (base_auth, None, Vec::new()),
            };
            Draft {
                installed: !missing,
                version,
                status: "error",
                auth: auth_value,
                checked_at,
                message: Some(message),
                models,
                extra,
                ..Default::default()
            }
        }
        Ok(executable) => {
            let usage = read_usage(&profile, &auth.method).await;
            let supports_text = text_generation_available(&profile);
            let Some((session, auth_value, models)) = account else {
                return Draft {
                    installed: true,
                    version: executable.version,
                    status: "warning",
                    auth: base_auth,
                    checked_at,
                    message: Some(unchecked),
                    usage_limits: Some(usage),
                    extra: vec![
                        ("supportsTextGeneration", json!(supports_text)),
                        ("workspaceSnapshots", json!([])),
                    ],
                    ..Default::default()
                };
            };
            // A runtime found on PATH has no version until a session's
            // `initialize` names it.
            let version = executable.version.or_else(|| session.version.clone());
            Draft {
                installed: true,
                version,
                status: if session.signed_in {
                    "ready"
                } else {
                    "warning"
                },
                auth: auth_value,
                checked_at,
                message: (!session.signed_in).then_some(sign_in_message),
                models: if session.signed_in {
                    models
                } else {
                    Vec::new()
                },
                usage_limits: Some(usage),
                extra: vec![
                    (
                        "supportsTextGeneration",
                        json!(supports_text && session.signed_in),
                    ),
                    ("workspaceSnapshots", json!([])),
                    ("supportedRuntimeModes", json!(session.runtime_modes)),
                ],
                ..Default::default()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn auth_config_issues_like_the_reference() {
        let auth = Auth::of(&json!({"authMethod": "agent-platform", "gcpProject": "p"}));
        assert!(auth
            .config_issue()
            .unwrap()
            .starts_with("Agent Platform needs"));
        let auth = Auth::of(&json!({"authMethod": "agent-platform", "apiKey": "k"}));
        assert!(auth.config_issue().is_none());
    }

    fn model_config_with(current: &str, options: Value) -> Value {
        json!({"id": "model", "name": "Model", "category": "model", "type": "select",
            "currentValue": current, "options": options})
    }

    #[test]
    fn legacy_session_models_count_only_without_a_model_option() {
        let options = json!([{"value": "a", "name": "A"}, {"value": "b", "name": "B"}]);
        let legacy = json!({"models": {"currentModelId": "a", "availableModels": [
            {"modelId": "a", "name": "A"}, {"modelId": "b", "name": "B"}]}});
        let with_option =
            json!({"models": legacy["models"], "configOptions": [model_config_with("a", options)]});
        let none = ThinkingLevels::new();
        assert_eq!(
            build_models(&legacy, &none),
            build_models(&with_option, &none)
        );
        assert_eq!(
            build_models(&legacy, &none)[0],
            json!({"slug": "a", "name": "A", "isCustom": false,
            "isDefault": true, "aliases": ["antigravity-default"], "capabilities": {"optionDescriptors": []}})
        );
        let empty = json!({"models": legacy["models"], "configOptions": [model_config_with("a", json!([]))]});
        assert!(build_models(&empty, &none).is_empty());
    }

    #[test]
    fn grouped_options_flatten_and_repeated_names_carry_their_ids() {
        let setup = json!({"configOptions": [model_config_with("pro", json!([
            {"group": "Flash", "name": "Flash", "options": [{"value": "f1", "name": "Flash"}, {"value": "f2", "name": "Flash"}]},
            {"group": "Pro", "name": "Pro", "options": [{"value": "pro", "name": "Pro"}, {"value": "f1", "name": "Flash"}]}]))]});
        let models = build_models(&setup, &ThinkingLevels::new());
        let slugs: Vec<&str> = models.iter().map(|m| m["slug"].as_str().unwrap()).collect();
        assert_eq!(slugs, ["f1", "f2", "pro"]);
        assert_eq!(models[1]["name"], "Flash (f2)");
        assert_eq!(models[2]["isDefault"], true);
        assert!(models[0].get("isDefault").is_none());
    }

    #[test]
    fn each_model_offers_the_thinking_levels_it_reported() {
        let thinking = |current: &str| {
            json!({"id": "thinking_level", "name": "Thinking Level", "category": "thought_level",
                "type": "select", "currentValue": current,
                "options": [{"value": "low", "name": "Low"}, {"value": "high", "name": "High"}]})
        };
        let setup = json!({"configOptions": [model_config_with("gemini", json!([
            {"value": "gemini", "name": "Gemini"}, {"value": "claude", "name": "Claude"},
            {"value": "plain", "name": "Plain"}, {"value": "unseen", "name": "Unseen"}])), thinking("high")]});
        let mut levels = ThinkingLevels::new();
        levels.insert("claude".into(), Some(thinking("low")));
        levels.insert("plain".into(), None);
        let models = build_models(&setup, &levels);
        let of = |slug: &str| {
            models.iter().find(|m| m["slug"] == slug).unwrap()["capabilities"]["optionDescriptors"]
                [0]
            .clone()
        };
        assert_eq!(of("gemini")["currentValue"], "high");
        assert_eq!(of("gemini")["role"], "reasoning");
        assert_eq!(of("claude")["currentValue"], "low");
        assert!(of("plain").is_null());
        assert_eq!(of("unseen")["id"], "thinking_level");
        assert!(of("unseen").get("currentValue").is_none());
    }

    #[test]
    fn auto_is_offered_only_where_the_session_lists_it() {
        let modes = |ids: &[&str]| {
            json!({"modes": {"currentModeId": "default",
            "availableModes": ids.iter().map(|id| json!({"id": id, "name": id})).collect::<Vec<_>>()}})
        };
        assert_eq!(runtime_modes(&modes(&["default", "auto"])).len(), 4);
        assert_eq!(
            runtime_modes(&modes(&["default"])),
            RUNTIME_MODES_WITHOUT_AUTO
        );
        assert_eq!(runtime_modes(&json!({})), RUNTIME_MODES_WITHOUT_AUTO);
    }

    #[test]
    fn summary_buckets_become_windows() {
        let windows = summary_windows(
            &json!({"buckets": [
                {"bucketId": "gemini-pro", "window": "weekly", "remainingFraction": 0.25, "resetTime": "2026-12-01T00:00:00Z"},
                {"bucketId": "3p-shared", "window": "5h", "remainingFraction": 1},
                {"bucketId": "off", "disabled": true, "remainingFraction": 0.5}]}),
            0,
        );
        assert_eq!(windows.len(), 2);
        assert_eq!(windows[0]["label"], "Gemini · Weekly");
        assert_eq!(windows[0]["usedPercent"], 75);
        assert_eq!(windows[1]["label"], "Claude & GPT (shared) · 5-hour");
    }
}
