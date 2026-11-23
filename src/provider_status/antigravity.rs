//! Antigravity's status check, as the reference's `checkAntigravityProvider`
//! with the driver's health probe. The probe never starts the runtime (a
//! PyInstaller bundle that unpacks about 1 GB per launch): it checks the
//! sign-in settings and resolves the installed runtime from disk
//! (`AntigravityInstallation.resolve`), a custom executable, the managed
//! install under `<home>/tools/antigravity-acp/<platform>-<arch>`, or
//! `agy_acp_server.par` (with its `localharness_external`) or `agy` on PATH.
//! A healthy runtime then has its Google quota read from the profile's
//! stored sign-in, when there is one.
use super::{maintenance, now_iso, unavailable_usage, Draft, ProcessEnv};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};
use std::time::Duration;

const RECORD_MAX_BYTES: u64 = 8 * 1024;
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

/// A resolved runtime (`AntigravityExecutable`): what to run, the harness
/// beside an ACP server (none for the agy CLI), and its version, when known.
pub(super) struct Executable {
    pub(super) version: Option<String>,
    pub(super) path: PathBuf,
    pub(super) harness: Option<PathBuf>,
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
    let (executable, harness) = if windows {
        ("agy_acp_server.exe", "localharness_external.exe")
    } else {
        ("agy_acp_server.par", "localharness_external")
    };
    let bytes = |v: &Value| {
        v.as_f64()
            .filter(|b| b.fract() == 0.0 && *b > 0.0 && *b <= 9_007_199_254_740_991.0)
            .map(|b| b as u64)
    };
    let executable_name = record["executable"]["name"].as_str().unwrap_or("");
    let names_match = if record["harness"].is_null() {
        executable_name == if windows { "agy.exe" } else { "antigravity" }
    } else {
        executable_name == executable && record["harness"]["name"] == harness
    };
    let executable_bytes = bytes(&record["executable"]["bytes"]);
    let harness_ok = record["harness"].is_null()
        || bytes(&record["harness"]["bytes"]).is_some_and(|b| {
            is_executable_file(
                &directory.join(record["harness"]["name"].as_str().unwrap_or("")),
                Some(b),
            )
        });
    if record["releaseId"] != release
        || !names_match
        || executable_bytes.is_none()
        || record["version"].as_str().unwrap_or("").trim().is_empty()
        || !is_executable_file(&directory.join(executable_name), executable_bytes)
        || !harness_ok
    {
        return Err(incomplete());
    }
    Ok(Executable {
        version: record["version"].as_str().map(str::to_owned),
        path: directory.join(executable_name),
        harness: record["harness"]["name"]
            .as_str()
            .map(|name| directory.join(name)),
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
    if is_cli(&[candidate, &real]) {
        return Ok(Some(Executable {
            version: None,
            path: candidate.to_path_buf(),
            harness: None,
        }));
    }
    let harness = if target().0 == "win32" {
        "localharness_external.exe"
    } else {
        "localharness_external"
    };
    Ok(
        is_executable_file(&directory.join(harness), None).then(|| Executable {
            version: None,
            path: candidate.to_path_buf(),
            harness: Some(directory.join(harness)),
        }),
    )
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
        return Err("The custom Antigravity executable is missing or not executable, or it needs a localharness_external sibling. The agy CLI does not.".into());
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
    let executable = if platform == "win32" {
        "agy_acp_server.exe"
    } else {
        "agy_acp_server.par"
    };
    for command in [executable, "agy"] {
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
// Check

pub(super) async fn check(
    config: &Value,
    env: &ProcessEnv,
    instance_id: &str,
    base: &Path,
    state_dir: &Path,
    manifest: &Value,
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
    match probe {
        Err((detail, missing)) => {
            let message = if detail.trim().is_empty() {
                "Antigravity could not complete its local health check.".to_owned()
            } else {
                detail.trim().to_owned()
            };
            Draft {
                installed: !missing,
                version: None,
                status: "error",
                auth: base_auth,
                checked_at,
                message: Some(message),
                extra: vec![
                    ("usageLimits", Value::Null),
                    ("supportsTextGeneration", json!(false)),
                    ("workspaceSnapshots", json!([])),
                ],
                ..Default::default()
            }
        }
        Ok(executable) => {
            let usage = read_usage(&profile, &auth.method).await;
            Draft {
                installed: true,
                version: executable.version,
                status: "warning",
                auth: base_auth,
                checked_at,
                message: Some(unchecked),
                usage_limits: Some(usage),
                extra: vec![
                    (
                        "supportsTextGeneration",
                        json!(text_generation_available(&profile)),
                    ),
                    ("workspaceSnapshots", json!([])),
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
