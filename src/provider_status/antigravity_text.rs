//! Antigravity's text generation (thread titles), as the reference's
//! `makeAntigravityTextGeneration` with the driver's runtime: a prepared
//! profile, the runtime in an empty temporary directory over ACP, a
//! session that may not use tools, the prompt, and the session's files
//! removed afterwards. See `docs/agent-tools.md`.
use super::acp::{AcpSession, Requests};
use super::antigravity::{resolve_executable, Auth, Executable};
use super::ProcessEnv;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};
use std::time::Duration;

/// How long a text generation run may take (`ANTIGRAVITY_TIMEOUT_MS`).
const TIMEOUT: Duration = Duration::from_secs(180);
const MAX_OUTPUT_CHARS: usize = 128_000;
const DEFAULT_MODEL: &str = "antigravity-default";
const SIGN_IN_REQUIRED: &str = "Sign in to Antigravity in Settings before you continue.";
const AUTH_URL_MARKER: &str = "__T3_ANTIGRAVITY_AUTH_URL__";
const AUTH_URL_LINE: &str = "Open the following link to authenticate the ACP server: ";
const BROWSER_PREFLIGHT_URL: &str = "https://example.invalid/t3-antigravity-browser-preflight";
const PREAMBLE: &str = "Use only the input below. Do not use tools, read or write files, run commands, or ask questions.\nReturn only the requested JSON object.\n\n";
/// Keys the runtime must not inherit (`removedEnvironmentKeys`).
pub(crate) const REMOVED_ENV: [&str; 18] = [
    "GEMINI_API_KEY",
    "GOOGLE_API_KEY",
    "GOOGLE_APPLICATION_CREDENTIALS",
    "GOOGLE_CLOUD_PROJECT",
    "GOOGLE_CLOUD_LOCATION",
    "GOOGLE_CLOUD_QUOTA_PROJECT",
    "GOOGLE_GENAI_USE_VERTEXAI",
    "GCLOUD_PROJECT",
    "CLOUDSDK_CORE_PROJECT",
    "AGY_ACP_CCPA_PROJECT",
    "AGY_ACP_ENABLE_OAUTH",
    "GEMINI_HOME",
    "AGY_ACP_FORCE_FILE_STORAGE",
    "AGY_CLI_DISABLE_AUTO_UPDATE",
    "BROWSER",
    "PYTHONUNBUFFERED",
    "ELECTRON_RUN_AS_NODE",
    "CHROME_CRASHPAD_PIPE_NAME",
];

/// Why a run failed: a message for the user, or a sign-in the runtime asked
/// for.
enum Failure {
    Detail(String),
    SignIn,
    Other,
}

impl Failure {
    fn detail(text: &str) -> Self {
        Self::Detail(text.to_owned())
    }
    fn message(self) -> String {
        match self {
            Self::Detail(detail) => detail,
            Self::SignIn => SIGN_IN_REQUIRED.into(),
            Self::Other => "Antigravity text generation failed.".into(),
        }
    }
}

/// `browserHelperSource`: Antigravity's browser, which only reports the URL
/// it was asked to open.
fn browser_helper_source() -> String {
    format!(
        "process.stderr.on(\"error\",()=>process.exit(0)).write(\"{AUTH_URL_MARKER}\"+JSON.stringify(process.argv[1])+\"\\n\",()=>process.exit(0))"
    )
}

/// `quoteBrowserArgument`.
fn quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\"'\"'"))
}

/// `resolveNodeExecutable` for a packaged server: `node` on PATH.
fn node_executable(env: &ProcessEnv) -> Result<PathBuf, Failure> {
    let unavailable = || {
        Failure::detail(
            "Antigravity sign-in requires Node.js. Install Node.js and make sure node is on PATH, then retry.",
        )
    };
    let found = super::maintenance::resolve_command_path("node", env).ok_or_else(unavailable)?;
    let path = std::env::current_dir().unwrap_or_default().join(found);
    let real = std::fs::canonicalize(&path).map_err(|_| unavailable())?;
    if std::env::current_exe()
        .ok()
        .and_then(|e| std::fs::canonicalize(e).ok())
        == Some(real)
    {
        return Err(unavailable());
    }
    Ok(path)
}

/// The profile `prepareAntigravityProfile` prepares.
struct Profile {
    gemini_home: PathBuf,
    temp_directory: PathBuf,
    browser_command: String,
    ca_bundle: Option<String>,
}

/// `antigravityEnvironment`.
fn environment(profile: &Profile, base: &ProcessEnv, auth: &Auth, temp: &Path) -> ProcessEnv {
    let mut env: ProcessEnv = base
        .iter()
        .filter(|(key, _)| !REMOVED_ENV.contains(&key.to_uppercase().as_str()))
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect();
    if !auth.api_key.is_empty() {
        match auth.method.as_str() {
            "gemini-api-key" => {
                env.insert("GEMINI_API_KEY".into(), auth.api_key.clone());
            }
            "agent-platform" => {
                env.insert("GOOGLE_API_KEY".into(), auth.api_key.clone());
            }
            _ => {}
        }
    }
    if let Some(bundle) = &profile.ca_bundle {
        if !base.contains_key("SSL_CERT_FILE") && !base.contains_key("SSL_CERT_DIR") {
            env.insert("SSL_CERT_FILE".into(), bundle.clone());
        }
    }
    // A personal Google sign-in is stored in files when agy sees a remote
    // session.
    if auth.method == "oauth-personal" {
        let ssh = base
            .get("SSH_CONNECTION")
            .filter(|value| !value.is_empty())
            .cloned()
            .unwrap_or_else(|| "127.0.0.1 0 127.0.0.1 0".into());
        env.insert("SSH_CONNECTION".into(), ssh);
    }
    env.insert(
        "GEMINI_HOME".into(),
        profile.gemini_home.display().to_string(),
    );
    env.insert("AGY_ACP_FORCE_FILE_STORAGE".into(), "1".into());
    env.insert("AGY_CLI_DISABLE_AUTO_UPDATE".into(), "1".into());
    env.insert("BROWSER".into(), profile.browser_command.clone());
    env.insert("PYTHONUNBUFFERED".into(), "1".into());
    env.insert("ELECTRON_RUN_AS_NODE".into(), "1".into());
    env.insert("TMPDIR".into(), temp.display().to_string());
    env
}

/// `antigravityProfileSettings`.
fn profile_settings(auth: &Auth) -> String {
    let mut settings = serde_json::Map::new();
    settings.insert("auth".into(), json!({"type": auth.method}));
    let mut gcp = serde_json::Map::new();
    if !auth.gcp_project.is_empty() {
        gcp.insert("project".into(), json!(auth.gcp_project));
    }
    if !auth.gcp_location.is_empty() {
        gcp.insert("location".into(), json!(auth.gcp_location));
    }
    if !gcp.is_empty() {
        settings.insert("gcp".into(), Value::Object(gcp));
    }
    format!("{}\n", Value::Object(settings))
}

/// `linkAntigravityUserSkills`: the user's global skills, linked into the
/// private profile. A failure leaves them unlinked.
fn link_user_skills(gemini_home: &Path, user_home: &Path) {
    let user = user_home.join(".gemini");
    for relative in ["config/skills", "antigravity-cli/skills"] {
        let link = gemini_home.join(relative);
        let target = user.join(relative);
        let linked = (|| -> std::io::Result<()> {
            if !target.exists() {
                if std::fs::read_link(&link).is_ok() {
                    std::fs::remove_file(&link)?;
                }
                return Ok(());
            }
            let existing = match std::fs::read_link(&link) {
                Ok(points) => Some(link.parent().unwrap_or(Path::new("/")).join(points)),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
                Err(error) => match std::fs::remove_dir(&link) {
                    Ok(()) => None,
                    Err(_) => return Err(error),
                },
            };
            if existing.as_deref() == Some(target.as_path()) {
                return Ok(());
            }
            if existing.is_some() {
                std::fs::remove_file(&link)?;
            }
            if let Some(parent) = link.parent() {
                std::fs::create_dir_all(parent)?;
            }
            std::os::unix::fs::symlink(&target, &link)
        })();
        if linked.is_err() {
            tracing::warn!("Antigravity user skills are not linked into the profile.");
        }
    }
}

fn make_private_dir(directory: &Path) -> Result<(), Failure> {
    use std::os::unix::fs::{DirBuilderExt, PermissionsExt};
    std::fs::DirBuilder::new()
        .recursive(true)
        .mode(0o700)
        .create(directory)
        .map_err(|_| Failure::detail("The Antigravity profile directory could not be created."))?;
    std::fs::set_permissions(directory, std::fs::Permissions::from_mode(0o700)).map_err(|_| {
        Failure::detail("The Antigravity profile directory permissions could not be set.")
    })
}

/// `prepareAntigravityProfile`: Antigravity's private home, its settings,
/// and a browser that only reports a sign-in URL (checked first).
async fn prepare_profile(
    gemini_home: &Path,
    temp_directory: &Path,
    base: &ProcessEnv,
    auth: &Auth,
) -> Result<Profile, Failure> {
    let node = node_executable(base)?;
    let helper = node.display().to_string();
    let source = browser_helper_source();
    let browser_command = [helper.as_str(), "-e", source.as_str(), "--", "%s"]
        .map(quote)
        .join(" ");
    if browser_command.contains(':') || helper.contains(['\r', '\n', '\0']) || helper.contains("%s")
    {
        return Err(Failure::detail(
            "The T3 runtime path cannot be used to suppress Antigravity browser launches.",
        ));
    }
    let ca_bundle = [
        base.get("NIX_SSL_CERT_FILE").cloned(),
        Some("/etc/ssl/certs/ca-bundle.crt".into()),
        Some("/etc/ssl/certs/ca-certificates.crt".into()),
        Some("/etc/pki/tls/certs/ca-bundle.crt".into()),
        Some("/etc/ssl/cert.pem".into()),
    ]
    .into_iter()
    .flatten()
    .find(|candidate| Path::new(candidate).is_absolute() && Path::new(candidate).exists());
    let profile = Profile {
        gemini_home: gemini_home.to_path_buf(),
        temp_directory: temp_directory.to_path_buf(),
        browser_command,
        ca_bundle,
    };
    let preflight_env = environment(&profile, base, auth, &profile.temp_directory);
    let unverified = || Failure::detail("Antigravity browser suppression could not be verified.");
    let mut command = tokio::process::Command::new(&node);
    command
        .args(["-e", &source, "--", BROWSER_PREFLIGHT_URL])
        .env_clear()
        .envs(&preflight_env)
        .stdin(std::process::Stdio::null())
        .kill_on_drop(true);
    let output = match tokio::time::timeout(Duration::from_secs(30), command.output()).await {
        Err(_) => {
            return Err(Failure::detail(
                "Antigravity browser suppression verification timed out.",
            ))
        }
        Ok(output) => output.map_err(|_| unverified())?,
    };
    let expected = format!("{AUTH_URL_MARKER}\"{BROWSER_PREFLIGHT_URL}\"\n");
    if !output.status.success()
        || !output.stdout.is_empty()
        || output.stderr.len() > 4_096
        || output.stderr != expected.as_bytes()
    {
        return Err(unverified());
    }
    let acp = gemini_home.join("antigravity-acp");
    for directory in [gemini_home, acp.as_path(), temp_directory] {
        make_private_dir(directory)?;
    }
    std::fs::write(acp.join("settings.json"), profile_settings(auth))
        .map_err(|_| Failure::detail("The Antigravity profile settings could not be written."))?;
    let user_home = base
        .get("HOME")
        .filter(|home| !home.is_empty())
        .map(PathBuf::from)
        .or_else(dirs_home)
        .unwrap_or_default();
    link_user_skills(gemini_home, &user_home);
    Ok(profile)
}

fn dirs_home() -> Option<PathBuf> {
    std::env::var_os("HOME").map(PathBuf::from)
}

/// `removeAntigravitySessionFiles`: the session's conversation and brain,
/// once its runtime exited, if the session was this run's.
fn remove_session_files(gemini_home: &Path, session_id: &str, cwd: &Path) {
    if uuid::Uuid::parse_str(session_id).map_or(true, |id| id.get_version_num() != 4) {
        return;
    }
    let acp = gemini_home.join("antigravity-acp");
    let base = acp.join("conversations").join(session_id);
    let meta = PathBuf::from(format!("{}.meta", base.display()));
    let Ok(text) = std::fs::read_to_string(&meta) else {
        return;
    };
    let Ok(recorded) = serde_json::from_str::<Value>(&text) else {
        tracing::warn!("Could not remove temporary Antigravity session files.");
        return;
    };
    if recorded["cwd"].as_str() != Some(&cwd.display().to_string()) {
        return;
    }
    for suffix in [".db", ".db-wal", ".db-shm", ".db-journal", ".meta"] {
        let _ = std::fs::remove_file(format!("{}{suffix}", base.display()));
    }
    let _ = std::fs::remove_dir_all(acp.join("brain").join(session_id));
}

/// `hasOnlyRuntimeDirectories`: the empty skill directories ACP creates, and
/// nothing else.
fn has_only_runtime_directories(cwd: &Path) -> bool {
    let Ok(root) = std::fs::canonicalize(cwd) else {
        return false;
    };
    let allowed = [
        root.join(".agents"),
        root.join(".agents").join("skills"),
        root.join(".gemini"),
        root.join(".gemini").join("skills"),
    ];
    let mut directories = vec![root];
    while let Some(directory) = directories.pop() {
        let Ok(entries) = std::fs::read_dir(&directory) else {
            return false;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            let is_dir = std::fs::symlink_metadata(&path).is_ok_and(|m| m.file_type().is_dir());
            if !allowed.contains(&path) || !is_dir {
                return false;
            }
            directories.push(path);
        }
    }
    true
}

/// A sign-in the runtime asked for (`isAntigravitySignInRequiredError`).
fn sign_in_required(error: &Value) -> bool {
    error["code"] == -32000
        || (error["code"] == -32603
            && error["message"]
                .as_str()
                .is_some_and(|m| m.starts_with("No browser is available here")))
}

/// `applyAntigravityAcpModelSelection`: the session's `model` option set to
/// the selected model, when it is not already.
async fn select_model(
    session: &mut AcpSession,
    session_id: &str,
    options: &[Value],
    model: &str,
) -> Result<(), Failure> {
    let failed = || Failure::detail("Could not select the Antigravity model for text generation.");
    let option = options
        .iter()
        .find(|option| option["id"] == "model" && option["type"] == "select");
    let current = option
        .and_then(|o| o["currentValue"].as_str())
        .map(str::to_owned);
    let values: Vec<String> = option
        .and_then(|o| o["options"].as_array())
        .into_iter()
        .flatten()
        .flat_map(|entry| match entry["options"].as_array() {
            Some(group) => group.clone(),
            None => vec![entry.clone()],
        })
        .filter_map(|entry| entry["value"].as_str().map(str::to_owned))
        .collect();
    let explicit = !model.is_empty() && model != DEFAULT_MODEL;
    let resolved = if explicit {
        Some(model.to_owned())
    } else {
        current.clone()
    };
    let Some(resolved) = resolved else {
        return Ok(());
    };
    if !explicit && Some(&resolved) == current.as_ref() {
        return Ok(());
    }
    let listed = values.contains(&resolved);
    let legacy = values
        .iter()
        .filter(|family| resolved.starts_with(&format!("{family}-")))
        .max_by_key(|family| family.len());
    if !listed && legacy.is_none() {
        return Err(failed());
    }
    if Some(&resolved) == current.as_ref() {
        return Ok(());
    }
    let config_id = options
        .iter()
        .find(|o| o["category"] == "model" && o["type"] == "select")
        .and_then(|o| o["id"].as_str())
        .unwrap_or("model");
    session
        .request(
            "session/set_config_option",
            json!({"sessionId": session_id, "configId": if listed { config_id } else { "model" }, "value": resolved}),
        )
        .await
        .map(|_| ())
        .map_err(|_| failed())
}

/// `setMode("default")`: the session's mode, unless it already is.
async fn set_default_mode(
    session: &mut AcpSession,
    session_id: &str,
    setup: &Value,
) -> Result<(), Failure> {
    let options = setup["configOptions"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    let mode_option = options
        .iter()
        .find(|o| o["category"] == "mode" && o["type"] == "select");
    let modes = &setup["modes"];
    let current = mode_option
        .and_then(|o| o["currentValue"].as_str())
        .or_else(|| modes["currentModeId"].as_str())
        .filter(|id| !id.is_empty());
    if current == Some("default") {
        return Ok(());
    }
    let result = if mode_option.is_none() && current.is_some() {
        session
            .request(
                "session/set_mode",
                json!({"sessionId": session_id, "modeId": "default"}),
            )
            .await
    } else {
        let id = mode_option.and_then(|o| o["id"].as_str()).unwrap_or("mode");
        session
            .request(
                "session/set_config_option",
                json!({"sessionId": session_id, "configId": id, "value": "default"}),
            )
            .await
    };
    result.map(|_| ()).map_err(|_| Failure::Other)
}

/// How a thread's Antigravity turns launch `agy acp` (`buildAntigravityAcpSpawnInput`):
/// the resolved executable, its arguments, the prepared profile's environment
/// (without `TMPDIR`: each process gets a `run-XXXXXX` folder of its own
/// under `run_temp_parent`), the keys it must not inherit, the installed
/// version and the sign-in method.
pub(crate) struct TurnLaunch {
    pub(crate) program: String,
    pub(crate) args: Vec<String>,
    pub(crate) env: ProcessEnv,
    pub(crate) run_temp_parent: PathBuf,
    pub(crate) version: Option<String>,
    pub(crate) auth_method: String,
}

/// Prepare a turn's launch: the instance's sign-in settings checked, the
/// executable resolved and the profile prepared, as the driver does before
/// it spawns the runtime.
pub(crate) async fn turn_launch(
    base_dir: &Path,
    state_dir: &Path,
    manifest: &Value,
    instance_id: &str,
    config: &Value,
    env: &ProcessEnv,
) -> Result<TurnLaunch, String> {
    let auth = Auth::of(config);
    if let Some(issue) = auth.config_issue() {
        return Err(issue.to_owned());
    }
    let key = hex::encode(Sha256::digest(instance_id.as_bytes()));
    let gemini_home = state_dir.join("providers").join("antigravity").join(&key);
    let executable = resolve_executable(
        base_dir,
        config["binaryPath"].as_str().unwrap_or(""),
        env,
        manifest,
    )?;
    let runtime_temp = state_dir.join("antigravity-tmp").join(&key[..12]);
    let profile = prepare_profile(&gemini_home, &runtime_temp, env, &auth)
        .await
        .map_err(Failure::message)?;
    let mut spawn_env = environment(&profile, env, &auth, &runtime_temp);
    spawn_env.remove("TMPDIR");
    Ok(TurnLaunch {
        program: executable.path.display().to_string(),
        args: vec![
            "acp".into(),
            "--gemini-dir".into(),
            profile.gemini_home.display().to_string(),
            "--app-data-dir".into(),
            "antigravity-acp".into(),
        ],
        env: spawn_env,
        run_temp_parent: runtime_temp,
        version: executable.version,
        auth_method: auth.method,
    })
}

/// `makeAntigravityTextGeneration`'s run: Antigravity's reply to `prompt`.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn generate_text(
    base_dir: &Path,
    state_dir: &Path,
    manifest: &Value,
    instance_id: &str,
    config: &Value,
    env: &ProcessEnv,
    prompt: &str,
    model: &str,
) -> Result<String, String> {
    match tokio::time::timeout(
        TIMEOUT,
        run(
            base_dir,
            state_dir,
            manifest,
            instance_id,
            config,
            env,
            prompt,
            model,
        ),
    )
    .await
    {
        Ok(result) => result.map_err(Failure::message),
        Err(_) => Err("Antigravity text generation timed out.".into()),
    }
}

#[allow(clippy::too_many_arguments)]
async fn run(
    base_dir: &Path,
    state_dir: &Path,
    manifest: &Value,
    instance_id: &str,
    config: &Value,
    env: &ProcessEnv,
    prompt: &str,
    model: &str,
) -> Result<String, Failure> {
    let auth = Auth::of(config);
    if let Some(issue) = auth.config_issue() {
        return Err(Failure::detail(issue));
    }
    let key = hex::encode(Sha256::digest(instance_id.as_bytes()));
    let gemini_home = state_dir.join("providers").join("antigravity").join(&key);
    if !super::antigravity::text_generation_available(&gemini_home) {
        return Err(Failure::detail(
            "Antigravity text generation is unavailable for profiles with global hooks or MCP configuration. Select another system model.",
        ));
    }
    let cwd = tempfile::Builder::new()
        .prefix("t3-antigravity-text-")
        .rand_bytes(6)
        .tempdir()
        .map_err(|_| Failure::Other)?;
    let executable: Executable = resolve_executable(
        base_dir,
        config["binaryPath"].as_str().unwrap_or(""),
        env,
        manifest,
    )
    .map_err(Failure::Detail)?;
    let runtime_temp = state_dir.join("antigravity-tmp").join(&key[..12]);
    let profile = prepare_profile(&gemini_home, &runtime_temp, env, &auth).await?;
    let run_temp = tempfile::Builder::new()
        .prefix("run-")
        .rand_bytes(6)
        .tempdir_in(&profile.temp_directory)
        .map_err(|_| Failure::detail("Could not create an Antigravity runtime temp directory."))?;
    let spawn_env = environment(&profile, env, &auth, run_temp.path());
    let args: Vec<String> = vec![
        "acp".into(),
        "--gemini-dir".into(),
        profile.gemini_home.display().to_string(),
        "--app-data-dir".into(),
        // The CLI joins this beneath gemini-dir, including when the value is absolute.
        "antigravity-acp".into(),
    ];
    let mut session_id = None;
    let result = prompt_session(
        manifest,
        &executable,
        &args,
        &spawn_env,
        cwd.path(),
        &auth,
        prompt,
        model,
        &mut session_id,
    )
    .await;
    // The runtime has exited (the session dropped it).
    if let Some(session_id) = &session_id {
        remove_session_files(&profile.gemini_home, session_id, cwd.path());
    }
    drop(run_temp);
    let text = result?;
    if !has_only_runtime_directories(cwd.path()) {
        return Err(Failure::detail(
            "Antigravity wrote files during text generation.",
        ));
    }
    if text.is_empty() {
        return Err(Failure::detail(
            "Antigravity returned empty text generation output.",
        ));
    }
    Ok(text)
}

/// The ACP runtime's session: initialize, authenticate, open, mode, model,
/// prompt; the reply's text. The runtime is stopped on return.
#[allow(clippy::too_many_arguments)]
async fn prompt_session(
    manifest: &Value,
    executable: &Executable,
    args: &[String],
    env: &ProcessEnv,
    cwd: &Path,
    auth: &Auth,
    prompt: &str,
    model: &str,
    session_id_out: &mut Option<String>,
) -> Result<String, Failure> {
    let mut session = AcpSession::start_with(
        &executable.path.display().to_string(),
        args,
        env,
        cwd,
        Requests::RefuseTools,
    )
    .map_err(|_| Failure::Other)?;
    let fail = |error: super::acp::AcpError| match error {
        super::acp::AcpError::Error(error) if sign_in_required(&error) => Failure::SignIn,
        _ => Failure::Other,
    };
    // `antigravityClientInfo`: a runtime from 1.5.1 on names T3 Code.
    let named = executable.version.as_deref().is_some_and(|version| {
        let parts: Vec<u64> = version
            .split('.')
            .take(3)
            .map(|part| {
                part.chars()
                    .take_while(char::is_ascii_digit)
                    .collect::<String>()
            })
            .filter_map(|digits| digits.parse().ok())
            .collect();
        parts.len() == 3 && (parts[0], parts[1], parts[2]) >= (1, 5, 1)
    });
    let client = if named {
        json!({"name": "t3-code", "title": "T3 Code", "version": crate::server_runtime::SERVER_VERSION})
    } else {
        json!({"name": "zed", "version": "0.0.0"})
    };
    let initialized = session
        .request(
            "initialize",
            json!({
                "protocolVersion": 2,
                "clientCapabilities": {
                    "fs": {"readTextFile": false, "writeTextFile": false},
                    "terminal": false,
                    "session": {"notices": {}, "compaction": {}},
                },
                "capabilities": {},
                "clientInfo": client,
                "info": client,
            }),
        )
        .await
        .map_err(fail)?;
    // `startCompatible` (#38970): a runtime the manifest rates broken (anything
    // before 1.5) is refused once it names its version, before anything
    // else. The refusal is a transport error, which text generation reports
    // as a plain failure.
    if let Some(version) = initialized["agentInfo"]["version"]
        .as_str()
        .filter(|v| !v.is_empty())
    {
        let advisory = super::provider_compatibility(manifest, "antigravity", version);
        if advisory.is_some_and(|a| a["status"] == "broken") {
            return Err(Failure::Other);
        }
    }
    // Eagerly, with the configured method: Antigravity takes ones it does not list.
    session
        .request("authenticate", json!({"methodId": auth.method}))
        .await
        .map_err(fail)?;
    let new_session = json!({"cwd": cwd, "mcpServers": []});
    let setup = session
        .request("session/new", new_session)
        .await
        .map_err(fail)?;
    let session_id = setup["sessionId"].as_str().unwrap_or_default().to_owned();
    *session_id_out = Some(session_id.clone());
    if uuid::Uuid::parse_str(&session_id).map_or(true, |id| id.get_version_num() != 4) {
        return Err(Failure::detail(
            "Antigravity returned an invalid text helper session ID.",
        ));
    }
    set_default_mode(&mut session, &session_id, &setup).await?;
    let options = setup["configOptions"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    select_model(&mut session, &session_id, &options, model).await?;
    let reply = session
        .request(
            "session/prompt",
            json!({"sessionId": session_id, "prompt": [{"type": "text", "text": format!("{PREAMBLE}{prompt}")}]}),
        )
        .await;
    // What streamed before the reply, and any refusal on the way.
    let mut output = String::new();
    let mut rejected: Option<Failure> = None;
    while let Ok((method, params)) = session.notifications.try_recv() {
        if rejected.is_some() {
            continue;
        }
        match method.as_str() {
            "$request" => {
                let requested = params.as_str().unwrap_or_default();
                rejected = Some(Failure::detail(
                    if requested == "session/request_permission" {
                        "Antigravity text generation requested a tool permission or user input."
                    } else if requested.contains("elicitation") {
                        "Antigravity text generation requested user input."
                    } else {
                        "Antigravity text generation requested a tool or user input."
                    },
                ));
            }
            "$output" => {
                let line = params.as_str().unwrap_or_default();
                if line.starts_with(AUTH_URL_LINE) || line.contains(AUTH_URL_MARKER) {
                    rejected = Some(Failure::SignIn);
                }
            }
            "session/update" => {
                let update = &params["update"];
                let kind = update["sessionUpdate"].as_str().unwrap_or_default();
                if kind == "tool_call" || kind == "tool_call_update" {
                    rejected = Some(Failure::detail(
                        "Antigravity attempted tool work during text generation.",
                    ));
                } else if params["sessionId"] == session_id.as_str()
                    && kind == "agent_message_chunk"
                    && update["content"]["type"] == "text"
                {
                    let text = update["content"]["text"].as_str().unwrap_or_default();
                    if output.chars().count() + text.chars().count() > MAX_OUTPUT_CHARS {
                        rejected = Some(Failure::detail(
                            "Antigravity text generation exceeded the output limit.",
                        ));
                    } else {
                        output.push_str(text);
                    }
                }
            }
            _ => {}
        }
    }
    if let Some(rejected) = rejected {
        return Err(rejected);
    }
    let reply = reply.map_err(fail)?;
    if reply["stopReason"] == "cancelled" {
        return Err(Failure::detail(
            "Antigravity text generation was cancelled.",
        ));
    }
    Ok(output.trim().to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn auth(method: &str, project: &str, location: &str) -> Auth {
        Auth::of(&json!({"authMethod": method, "gcpProject": project, "gcpLocation": location}))
    }

    #[test]
    fn profile_settings_are_written_as_the_reference_writes_them() {
        assert_eq!(
            profile_settings(&auth("oauth-personal", "", "")),
            "{\"auth\":{\"type\":\"oauth-personal\"}}\n"
        );
        assert_eq!(
            profile_settings(&auth("oauth-business", "p1", "us")),
            "{\"auth\":{\"type\":\"oauth-business\"},\"gcp\":{\"project\":\"p1\",\"location\":\"us\"}}\n"
        );
    }

    #[test]
    fn the_browser_command_quotes_each_argument_for_a_shell() {
        assert_eq!(quote("it's"), "'it'\"'\"'s'");
        assert!(browser_helper_source().contains("\"__T3_ANTIGRAVITY_AUTH_URL__\"+JSON.stringify"));
    }

    #[test]
    fn a_sign_in_is_recognised_as_the_reference_recognises_it() {
        assert!(sign_in_required(
            &json!({"code": -32000, "message": "auth"})
        ));
        assert!(sign_in_required(
            &json!({"code": -32603, "message": "No browser is available here to sign in"})
        ));
        assert!(!sign_in_required(
            &json!({"code": -32603, "message": "boom"})
        ));
    }

    #[test]
    fn only_this_run_s_session_files_are_removed() {
        let dir = tempfile::tempdir().unwrap();
        let home = dir.path();
        let session = "0b6a3c1e-5d2f-4a8b-9c7d-1e2f3a4b5c6d";
        let conversations = home.join("antigravity-acp/conversations");
        let brain = home.join("antigravity-acp/brain").join(session);
        std::fs::create_dir_all(&conversations).unwrap();
        std::fs::create_dir_all(&brain).unwrap();
        let base = conversations.join(session);
        let file = |suffix: &str| PathBuf::from(format!("{}{suffix}", base.display()));
        std::fs::write(file(".db"), "x").unwrap();
        std::fs::write(file(".meta"), json!({"cwd": "/elsewhere"}).to_string()).unwrap();
        remove_session_files(home, session, Path::new("/tmp/t3-antigravity-text-abc123"));
        assert!(file(".db").exists(), "another directory's session stays");
        std::fs::write(
            file(".meta"),
            json!({"cwd": "/tmp/t3-antigravity-text-abc123"}).to_string(),
        )
        .unwrap();
        remove_session_files(home, session, Path::new("/tmp/t3-antigravity-text-abc123"));
        assert!(!file(".db").exists() && !file(".meta").exists() && !brain.exists());
    }

    #[test]
    fn only_acp_s_empty_skill_directories_may_appear_in_the_run_directory() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join(".agents/skills")).unwrap();
        std::fs::create_dir_all(dir.path().join(".gemini/skills")).unwrap();
        assert!(has_only_runtime_directories(dir.path()));
        std::fs::write(dir.path().join(".gemini/skills/x.md"), "x").unwrap();
        assert!(!has_only_runtime_directories(dir.path()));
    }
}
