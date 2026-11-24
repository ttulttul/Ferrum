//! How a provider CLI was installed and how it can be updated, as the
//! reference's `maintenanceResolver` decides it: the capabilities behind a
//! snapshot's version advisory (`updateCommand`, `canUpdate`,
//! `canInstallVersion`).
//!
//! The executable is resolved on the instance's `PATH` and followed to its
//! real path; mise, Nix, Homebrew, MacPorts, Vite+, bun, pnpm, yarn, Volta and
//! npm installs are told apart by their paths and, where the reference asks
//! them, by the tools themselves (`mise which`, `brew --prefix`, ...).
use super::ProcessEnv;
use regex::Regex;
use serde_json::{json, Value};
use std::path::{Path, PathBuf};
use std::time::Duration;

const PROBE_TIMEOUT: Duration = Duration::from_secs(10);
const PROBE_MAX_BYTES: usize = 256 * 1024;
const WRAPPER_SCRIPT_MAX_BYTES: u64 = 16 * 1024;
const PROVIDER_UPDATE_ACTION_TOAST_MESSAGE: &str =
    "Install the update now or review provider settings.";

/// A command that updates the CLI (`ProviderMaintenanceCommandAction`).
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct Update {
    pub command: String,
    pub executable: String,
    pub args: Vec<String>,
    pub lock_key: String,
    /// The environment the update runs in, when it is not the server's.
    pub env: Option<ProcessEnv>,
}

/// `ProviderMaintenanceCapabilities`.
#[derive(Clone, Debug, Default)]
pub(crate) struct Capabilities {
    pub package_name: Option<String>,
    pub update: Option<Update>,
    pub install_path: Option<String>,
    pub manual_command: Option<String>,
    /// `compareVersions`, where the driver orders its versions its own way.
    pub compare_versions: Option<fn(&str, &str) -> std::cmp::Ordering>,
    /// `latestVersion`: `None` when unset, `Some(None)` for "only the
    /// installer's own answer counts".
    pub latest: Option<Option<String>>,
    /// `lookupLatestVersion`: how the installer says what it would install.
    pub lookup: Option<std::sync::Arc<Lookup>>,
    /// `latestVersionCacheKey`.
    pub cache_key: Option<String>,
    /// Muse's release channel (`latestMuseVersion`), for an install whose
    /// installer has no lookup of its own.
    pub muse_channel: Option<String>,
}

/// A `lookupLatestVersion` the reference builds for an install method.
#[derive(Debug)]
pub(crate) enum Lookup {
    /// `mise latest <tool>`.
    Mise {
        mise: String,
        tool: String,
        env: ProcessEnv,
    },
    /// `brew info --json=v2 <name>`.
    Homebrew {
        brew: String,
        name: String,
        cask: bool,
        env: ProcessEnv,
    },
    /// `npm outdated -g --prefix`, then `npm ls -g --prefix`.
    NpmGlobal {
        package: String,
        prefix: String,
        env: ProcessEnv,
    },
    /// `pnpm outdated -g`, then `pnpm ls -g`.
    PnpmGlobal { package: String, env: ProcessEnv },
    /// `bun outdated -g`.
    BunGlobal { package: String, env: ProcessEnv },
    /// `npm pack --dry-run` beside Vite+'s managed Node.
    VitePlus { package: String, env: ProcessEnv },
    /// `npm view --global --json <package>@latest version` with the configured npm.
    NpmView {
        npm: String,
        package: String,
        env: ProcessEnv,
    },
    /// The first lookup, then the second when it has no answer.
    Then(std::sync::Arc<Lookup>, std::sync::Arc<Lookup>),
    /// Claude Code's native release channel (`<url>` answers a bare version).
    ClaudeNative { url: String },
    /// Codex's standalone release channel (`tag_name` `rust-vX.Y.Z`).
    CodexNative,
    /// fx's release channel (`fetchLatestFxVersion`): `<url>` answers a bare
    /// `vX.Y.Z` or `X.Y.Z`.
    FxRelease { url: String },
}

/// `ProviderMaintenanceResolutionContext`.
#[derive(Clone, Debug)]
pub(crate) struct Context {
    pub resolved_command_path: String,
    pub real_command_path: String,
    pub env: ProcessEnv,
}

/// `PackageManagedProviderMaintenanceDefinition`.
pub(crate) struct PackageDefinition {
    pub provider: &'static str,
    pub npm_package_name: &'static str,
    pub native_update: Option<NativeUpdate>,
    pub mise: Option<MiseTool>,
}

pub(crate) struct NativeUpdate {
    pub args: &'static [&'static str],
    pub is_command_path: Option<fn(&str) -> bool>,
    pub requires_command_path: bool,
}

pub(crate) struct MiseTool {
    pub tool: &'static str,
    pub upgrade: bool,
}

fn non_empty(value: Option<&str>) -> Option<String> {
    value
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .map(str::to_owned)
}

/// `quoteShellWord` for POSIX shells.
pub(crate) fn quote(word: &str) -> String {
    if Regex::new(r"^[\w./:@=-]+$").unwrap().is_match(word) {
        word.to_owned()
    } else {
        format!("'{}'", word.replace('\'', "'\\''"))
    }
}

/// `makeProviderMaintenanceCapabilities`.
fn capabilities(
    package_name: Option<&str>,
    executable: &str,
    args: &[String],
    lock_key: &str,
    command: Option<String>,
) -> Capabilities {
    let command = command.unwrap_or_else(|| {
        std::iter::once(quote(executable))
            .chain(args.iter().map(|a| quote(a)))
            .collect::<Vec<_>>()
            .join(" ")
    });
    Capabilities {
        package_name: package_name.map(str::to_owned),
        update: Some(Update {
            command,
            executable: executable.to_owned(),
            args: args.to_vec(),
            lock_key: lock_key.to_owned(),
            env: None,
        }),
        ..Default::default()
    }
}

/// The capabilities with the update's environment set.
fn with_update_env(mut caps: Capabilities, env: &ProcessEnv) -> Capabilities {
    if let Some(update) = caps.update.as_mut() {
        update.env = Some(env.clone());
    }
    caps
}

/// `environmentCacheKey`: a digest of the environment, so a cache key never
/// holds its values.
fn environment_key(env: &ProcessEnv) -> String {
    use sha2::Digest;
    hex::encode(sha2::Sha256::digest(
        serde_json::to_string(&env.iter().collect::<Vec<_>>()).unwrap_or_default(),
    ))
}

/// `makeManualOnlyProviderMaintenanceCapabilities`.
pub(crate) fn manual(package_name: Option<&str>, install_path: Option<&str>) -> Capabilities {
    Capabilities {
        package_name: package_name.map(str::to_owned),
        install_path: install_path.map(str::to_owned),
        ..Default::default()
    }
}

fn strings(args: &[&str]) -> Vec<String> {
    args.iter().map(|a| a.to_string()).collect()
}

/// `makeTargetedProviderUpdateAction`.
fn targeted_update(capabilities: &Capabilities, version: &str) -> Option<Update> {
    if !Regex::new(r"^\d+\.\d+\.\d+$").unwrap().is_match(version) {
        return None;
    }
    let update = capabilities.update.as_ref()?;
    let package = capabilities.package_name.as_deref()?;
    if !Regex::new(
        r"^(?:npm-global:|bun-global$|pnpm-global$|vite-plus-global$|yarn-global$|volta$)",
    )
    .unwrap()
    .is_match(&update.lock_key)
    {
        return None;
    }
    let latest = format!("{package}@latest");
    let index = update
        .args
        .iter()
        .position(|arg| *arg == latest || arg == package)?;
    let pinned = format!("{package}@{version}");
    let mut args = update.args.clone();
    let previous = std::mem::replace(&mut args[index], pinned.clone());
    let command = match update.command.rfind(&previous) {
        Some(at) => format!(
            "{}{}{}",
            &update.command[..at],
            pinned,
            &update.command[at + previous.len()..]
        ),
        None => update.command.clone(),
    };
    Some(Update {
        command,
        args,
        ..update.clone()
    })
}

/// `resolveLatestProviderUpdateAction`.
fn latest_update_action(capabilities: &Capabilities, latest: Option<&str>) -> Option<Update> {
    let update = capabilities.update.as_ref()?;
    match latest {
        Some(latest) if update.lock_key == "pnpm-global" => {
            targeted_update(capabilities, latest).or_else(|| Some(update.clone()))
        }
        _ => Some(update.clone()),
    }
}

/// `createProviderVersionAdvisory`.
pub(crate) fn version_advisory(
    capabilities: &Capabilities,
    current: Option<&str>,
    latest: Option<&str>,
    checked_at: &str,
    compare: fn(&str, &str) -> std::cmp::Ordering,
) -> Value {
    let (status, message) = match (current, latest) {
        (Some(current), Some(latest)) if compare(current, latest).is_lt() => (
            "behind_latest",
            Some(if capabilities.update.is_none() {
                match &capabilities.install_path {
                    Some(path) => format!("Version {latest} is out. T3 Code runs {path}; update it with the app or tool that installed it."),
                    None => format!("Version {latest} is out. Update it with the app or tool that installed it."),
                }
            } else {
                PROVIDER_UPDATE_ACTION_TOAST_MESSAGE.to_owned()
            }),
        ),
        (Some(_), Some(_)) => ("current", None),
        _ => ("unknown", None),
    };
    json!({
        "status": status,
        "currentVersion": current,
        "latestVersion": latest,
        "updateCommand": latest_update_action(capabilities, latest)
            .map(|u| u.command)
            .or_else(|| capabilities.manual_command.clone()),
        "canUpdate": capabilities.update.is_some(),
        "canInstallVersion": targeted_update(capabilities, "0.0.0").is_some(),
        "checkedAt": checked_at,
        "message": message,
    })
}

// ---------------------------------------------------------------------------
// Paths

/// `normalizeCommandPath`.
fn normalize(path: &str) -> String {
    path.replace('\\', "/").to_lowercase()
}

/// `resolveCommandPath`: a command with a slash as given, otherwise the
/// first executable of that name on the environment's `PATH`.
pub(crate) fn resolve_command_path(command: &str, env: &ProcessEnv) -> Option<String> {
    if command.contains('/') || command.contains('\\') {
        return crate::npm_release::is_executable(Path::new(command)).then(|| command.to_owned());
    }
    let path = env.get("PATH")?;
    let mut seen = Vec::new();
    for entry in path.split(':') {
        let entry = entry.trim();
        if entry.is_empty() || seen.contains(&entry) {
            continue;
        }
        seen.push(entry);
        let candidate = Path::new(entry).join(command);
        if crate::npm_release::is_executable(&candidate) {
            return Some(normalize_join(&candidate));
        }
    }
    None
}

fn normalize_join(path: &Path) -> String {
    // `path.join` normalizes `.` and `..` segments without touching the disk.
    let mut parts: Vec<std::path::Component> = Vec::new();
    for component in path.components() {
        match component {
            std::path::Component::CurDir => {}
            std::path::Component::ParentDir
                if matches!(parts.last(), Some(std::path::Component::Normal(_))) =>
            {
                parts.pop();
            }
            other => parts.push(other),
        }
    }
    parts.iter().collect::<PathBuf>().display().to_string()
}

fn real_path(path: &str) -> Option<String> {
    std::fs::canonicalize(path)
        .ok()
        .map(|p| p.display().to_string())
}

fn exists(path: &Path) -> bool {
    path.exists()
}

fn parent(path: &str) -> String {
    Path::new(path)
        .parent()
        .map(|p| p.display().to_string())
        .unwrap_or_else(|| ".".into())
}

fn basename(path: &str) -> String {
    Path::new(path)
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_default()
}

fn is_vite_plus_global(path: &str) -> bool {
    Regex::new(r"/(?:\.vite-plus|\.local/share/vite-plus|appdata/local/vite-plus)/bin/")
        .unwrap()
        .is_match(&normalize(path))
}

fn is_bun_global(path: &str) -> bool {
    normalize(path).contains("/.bun/bin/")
}

fn is_yarn_global(path: &str) -> bool {
    Regex::new(r"/yarn/(?:data/)?global/node_modules/")
        .unwrap()
        .is_match(&normalize(path))
}

fn is_mise_command_path(path: &str) -> bool {
    let n = normalize(path);
    (n.contains("/mise/installs/") && !n.contains("/mise/installs/node/"))
        || n.contains("/mise/shims/")
        || n.contains("/mise/command-wrappers/")
}

fn is_mise_executable(path: &str) -> bool {
    Regex::new(r"(?:^|/)mise(?:\.exe)?$")
        .unwrap()
        .is_match(&normalize(path))
}

/// `isNixStoreCommandPath`.
pub(crate) fn is_nix_store(path: &str) -> bool {
    normalize(path).starts_with("/nix/store/")
}

fn is_pnpm_global(path: &str) -> bool {
    let n = normalize(path);
    [
        "/.local/share/pnpm/",
        "/library/pnpm/",
        "/local/share/pnpm/",
        "/appdata/local/pnpm/",
        "/pnpm/global/",
    ]
    .iter()
    .any(|p| n.contains(p))
}

/// `npmGlobalPrefixFromCommandPath`.
fn npm_global_prefix(real: &str, package: &str) -> Option<String> {
    let slash = real.replace('\\', "/");
    let lower = slash.to_lowercase();
    let segment = format!("/lib/node_modules/{}/", package.to_lowercase());
    let index = lower.rfind(&segment)?;
    if lower[..index].contains("/node_modules/") {
        return None;
    }
    if let Some(tool) = Regex::new(r"/mise/installs/([^/]+)/[^/]+$")
        .unwrap()
        .captures(&lower[..index])
    {
        if &tool[1] != "node" {
            return None;
        }
    }
    Some(if index == 0 {
        "/".into()
    } else {
        slash[..index].to_owned()
    })
}

/// `HomebrewOwnership`.
pub(crate) struct Homebrew {
    pub cask: bool,
    pub name: String,
    pub prefix: String,
}

/// `homebrewOwnershipFromCommandPath`.
pub(crate) fn homebrew_ownership(real: &str) -> Option<Homebrew> {
    let captures = Regex::new(r"(?i)^(.*)/(cellar|caskroom)/([^/]+)/[^/]+/")
        .unwrap()
        .captures(&real.replace('\\', "/"))
        .map(|c| (c[1].to_owned(), c[2].to_lowercase(), c[3].to_owned()))?;
    Some(Homebrew {
        cask: captures.1 != "cellar",
        name: captures.2,
        prefix: captures.0,
    })
}

// ---------------------------------------------------------------------------
// Probes

/// `runProbe`: the command's standard output when it exits with one of
/// `ok_codes` within ten seconds and prints at most 256 KiB.
async fn run_probe(
    command: &str,
    args: &[&str],
    env: &ProcessEnv,
    ok_codes: &[i32],
) -> Option<String> {
    let mut process = tokio::process::Command::new(command);
    process
        .args(args)
        .env_clear()
        .envs(std::env::vars())
        .envs(env)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .kill_on_drop(true);
    let child = process.spawn().ok()?;
    let output = tokio::time::timeout(PROBE_TIMEOUT, child.wait_with_output())
        .await
        .ok()?
        .ok()?;
    let code = output.status.code()?;
    if !ok_codes.contains(&code) || output.stdout.len() > PROBE_MAX_BYTES {
        return None;
    }
    Some(String::from_utf8_lossy(&output.stdout).into_owned())
}

fn read_wrapper_script(path: &str) -> String {
    match std::fs::metadata(path) {
        Ok(meta) if meta.len() <= WRAPPER_SCRIPT_MAX_BYTES => {
            std::fs::read_to_string(path).unwrap_or_default()
        }
        _ => String::new(),
    }
}

fn is_mise_wrapper_script(context: &Context) -> bool {
    let script = read_wrapper_script(&context.real_command_path);
    script.starts_with("#!")
        && Regex::new(r"\bmise\s+(?:x|exec)\b")
            .unwrap()
            .is_match(&script)
}

/// The installs `mise ls --json --installed <tool>` lists.
fn mise_tool_installs(listing: &str, tool: &str) -> Vec<Value> {
    let Ok(value) = serde_json::from_str::<Value>(listing) else {
        return vec![];
    };
    let valid = |installs: &Value| {
        installs.as_array().is_some_and(|list| {
            list.iter().all(|i| {
                i["install_path"].is_string()
                    && matches!(
                        i.get("requested_version"),
                        None | Some(Value::Null | Value::String(_))
                    )
                    && matches!(i.get("active"), None | Some(Value::Bool(_)))
            })
        })
    };
    if valid(&value) {
        return value.as_array().cloned().unwrap_or_default();
    }
    match value.as_object() {
        Some(by_tool) if by_tool.values().all(valid) && by_tool.keys().all(|k| k == tool) => {
            by_tool
                .get(tool)
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default()
        }
        _ => vec![],
    }
}

/// `resolveMiseProviderMaintenance`: `None` when mise does not own the
/// executable.
pub(crate) async fn resolve_mise(
    package_name: Option<&str>,
    mise: Option<&MiseTool>,
    context: &Context,
) -> Option<Capabilities> {
    let is_shim = is_mise_executable(&context.real_command_path);
    let looks_mise = is_shim
        || [&context.resolved_command_path, &context.real_command_path]
            .iter()
            .any(|p| is_mise_command_path(p))
        || is_mise_wrapper_script(context);
    let manual_at = |path: &str| Some(manual(package_name, Some(path)));
    let not_owned = |path: &str| if looks_mise { manual_at(path) } else { None };
    let command_name = normalize(&context.resolved_command_path)
        .rsplit('/')
        .next()
        .map(|n| {
            Regex::new(r"\.(?:exe|cmd|bat|ps1)$")
                .unwrap()
                .replace(n, "")
                .into_owned()
        });
    let Some(mise) = mise else {
        return not_owned(&context.real_command_path);
    };
    if is_shim && command_name.as_deref() != Some(mise.tool) {
        return manual_at(&context.real_command_path);
    }
    let real = |target: &str| real_path(target).unwrap_or_else(|| target.to_owned());
    let mise_path = resolve_command_path("mise", &context.env)
        .or_else(|| is_shim.then(|| context.real_command_path.clone()));
    let Some(mise_path) = mise_path else {
        return not_owned(&context.real_command_path);
    };
    let which = non_empty(
        run_probe(&mise_path, &["which", mise.tool], &context.env, &[0])
            .await
            .as_deref(),
    );
    let resolved_path = which.map(|w| real(&w));
    let binary_path = if is_shim {
        resolved_path.clone()
    } else {
        Some(context.real_command_path.clone())
    };
    let Some(binary_path) = binary_path else {
        return manual_at(&context.real_command_path);
    };
    let binary = normalize(&binary_path);
    let runs_binary = resolved_path
        .as_deref()
        .is_some_and(|p| normalize(p) == binary);
    let Some(listing) = run_probe(
        &mise_path,
        &["ls", "--json", "--installed", mise.tool],
        &context.env,
        &[0],
    )
    .await
    else {
        return if runs_binary {
            manual_at(&binary_path)
        } else {
            not_owned(&binary_path)
        };
    };
    let is_under = |path: &str, dir: &str| {
        path.starts_with(&format!("{}/", normalize(dir).trim_end_matches('/')))
    };
    let owning: Vec<Value> = mise_tool_installs(&listing, mise.tool)
        .into_iter()
        .filter(|install| {
            is_under(
                &binary,
                &real(install["install_path"].as_str().unwrap_or("")),
            )
        })
        .collect();
    if owning.is_empty() {
        return not_owned(&binary_path);
    }
    if !runs_binary {
        return manual_at(&binary_path);
    }
    let install = owning
        .iter()
        .find(|c| c["active"] == true && c["requested_version"] == "latest");
    let Some(install) = install.filter(|_| mise.upgrade) else {
        return manual_at(&binary_path);
    };
    let install_path = install["install_path"].as_str().unwrap_or("");
    if !is_shim {
        let tool_dir = Regex::new(r"/[^/]+$")
            .unwrap()
            .replace(install_path.replace('\\', "/").trim_end_matches('/'), "")
            .into_owned();
        let latest_dirs = [tool_dir.clone(), real(&tool_dir)].map(|dir| format!("{dir}/latest"));
        let resolved = normalize(&context.resolved_command_path);
        if !latest_dirs.iter().any(|dir| is_under(&resolved, dir)) {
            return manual_at(&binary_path);
        }
    }
    let configured = run_probe(
        &mise_path,
        &["ls", "--json", "--installed"],
        &context.env,
        &[0],
    )
    .await
    .and_then(|text| serde_json::from_str::<Value>(&text).ok());
    let configured_tool = configured
        .as_ref()
        .and_then(Value::as_object)
        .and_then(|by_tool| {
            by_tool.iter().find_map(|(tool, installs)| {
                installs
                    .as_array()?
                    .iter()
                    .any(|c| c["install_path"] == install_path)
                    .then(|| tool.clone())
            })
        })
        .unwrap_or_else(|| mise.tool.to_owned());
    let args = vec!["upgrade".to_owned(), configured_tool];
    let command = format!("mise {}", args.join(" "));
    let caps = with_update_env(
        capabilities(package_name, &mise_path, &args, "mise", Some(command)),
        &context.env,
    );
    // Settings such as `minimum_release_age` can hold mise behind the
    // registry, so only mise can say what it installs.
    Some(Capabilities {
        latest: Some(None),
        cache_key: Some(
            [
                "mise",
                &mise_path,
                mise.tool,
                &environment_key(&context.env),
            ]
            .join("\0"),
        ),
        lookup: Some(std::sync::Arc::new(Lookup::Mise {
            mise: mise_path.clone(),
            tool: mise.tool.to_owned(),
            env: context.env.clone(),
        })),
        ..caps
    })
}

/// `resolveHomebrewProviderMaintenance`.
pub(crate) async fn resolve_homebrew(
    package_name: Option<&str>,
    homebrew: &Homebrew,
    context: &Context,
) -> Option<Capabilities> {
    let keg_brew = Path::new(&homebrew.prefix).join("bin").join("brew");
    let brew = if exists(&keg_brew) {
        keg_brew.display().to_string()
    } else {
        resolve_command_path("brew", &context.env)?
    };
    let prefix = non_empty(
        run_probe(&brew, &["--prefix"], &context.env, &[0])
            .await
            .as_deref(),
    )?;
    let real_prefix = real_path(&prefix).unwrap_or(prefix);
    if normalize(&real_prefix) != normalize(&homebrew.prefix) {
        return None;
    }
    let args: Vec<String> = if homebrew.cask {
        strings(&["upgrade", "--cask", &homebrew.name])
    } else {
        strings(&["upgrade", &homebrew.name])
    };
    let command = format!("brew {}", args.join(" "));
    let caps = capabilities(package_name, &brew, &args, "homebrew", Some(command));
    // Brew's release can lag npm; only its own channel says what upgrade installs.
    Some(Capabilities {
        latest: Some(None),
        lookup: Some(std::sync::Arc::new(Lookup::Homebrew {
            brew: brew.clone(),
            name: homebrew.name.clone(),
            cask: homebrew.cask,
            env: context.env.clone(),
        })),
        ..caps
    })
}

/// `followVitePlusSystemFirstShim`.
async fn follow_vite_plus_shim(context: Context) -> Context {
    if !is_vite_plus_global(&context.resolved_command_path) {
        return context;
    }
    let shim_dir = parent(&context.resolved_command_path);
    let data_dir = parent(&shim_dir);
    let home = if context.env.contains_key("VP_HOME") && !context.env["VP_HOME"].is_empty() {
        None
    } else {
        Regex::new(r"^(.*)/\.local/share/vite-plus$")
            .unwrap()
            .captures(&data_dir.replace('\\', "/"))
            .map(|c| c[1].to_owned())
    };
    let mut config_dirs = Vec::new();
    if let Some(home) = &home {
        let config_home = context
            .env
            .get("XDG_CONFIG_HOME")
            .filter(|v| !v.is_empty())
            .cloned()
            .unwrap_or_else(|| format!("{home}/.config"));
        config_dirs.push(format!("{config_home}/vite-plus"));
    }
    config_dirs.push(data_dir.clone());
    let config = config_dirs
        .iter()
        .find_map(|dir| std::fs::read_to_string(Path::new(dir).join("config.json")).ok())
        .unwrap_or_default();
    let system_first = serde_json::from_str::<Value>(&config)
        .ok()
        .is_some_and(|c| c.is_object() && c["shimMode"] == "system_first");
    if !system_first {
        return context;
    }
    let dir_key = |dir: &str| normalize(dir).trim_end_matches('/').to_owned();
    let shim_key = dir_key(&shim_dir);
    let mut env = context.env.clone();
    for (name, value) in env.iter_mut() {
        if name.eq_ignore_ascii_case("path") && !value.is_empty() {
            *value = value
                .split(':')
                .filter(|entry| dir_key(entry) != shim_key)
                .collect::<Vec<_>>()
                .join(":");
        }
    }
    let command = Regex::new(r"(?i)\.(?:exe|cmd|bat|ps1)$")
        .unwrap()
        .replace(&basename(&context.resolved_command_path), "")
        .into_owned();
    let Some(resolved) = resolve_command_path(&command, &env) else {
        return context;
    };
    let real = real_path(&resolved).unwrap_or_else(|| resolved.clone());
    Context {
        resolved_command_path: resolved,
        real_command_path: real,
        ..context
    }
}

/// `resolveMacPortsOwner`: the port that installed the executable, `""` for
/// an unusable name, `None` when MacPorts does not own it.
pub(crate) async fn resolve_macports_owner(context: &Context) -> Option<String> {
    let sibling = Path::new(&parent(&context.resolved_command_path)).join("port");
    let port = if exists(&sibling) {
        sibling.display().to_string()
    } else {
        resolve_command_path("port", &context.env)?
    };
    let prefix = format!("{}/", normalize(&parent(&parent(&port))));
    if !normalize(&context.real_command_path).starts_with(&prefix) {
        return None;
    }
    let output = run_probe(
        &port,
        &["provides", &context.real_command_path],
        &context.env,
        &[0],
    )
    .await?;
    let name = Regex::new(r"\bis provided by: (\S+)")
        .unwrap()
        .captures(&output)
        .map(|c| c[1].to_owned())?;
    Some(if Regex::new(r"^[\w.+-]+$").unwrap().is_match(&name) {
        name
    } else {
        String::new()
    })
}

/// `isVoltaPackageInstall`.
fn is_volta_package_install(context: &Context, package: &str) -> bool {
    let volta_home = parent(&parent(&context.resolved_command_path));
    let mut package_dir = Path::new(&volta_home).join("tools/image/packages");
    for part in package.split('/') {
        package_dir = package_dir.join(part);
    }
    if !exists(&package_dir) {
        return false;
    }
    if Regex::new(r"^volta-shim(?:\.exe)?$")
        .unwrap()
        .is_match(&basename(&normalize(&context.real_command_path)))
    {
        return true;
    }
    let script = read_wrapper_script(&context.real_command_path).replace("\r\n", "\n");
    let script = script.strip_suffix('\n').unwrap_or(&script);
    script == "@echo off\nvolta run %~n0 %*"
        || script == "#!/bin/bash\nvolta run \"$(basename $0)\" \"$@\""
}

/// `resolvePackageManagedProviderMaintenance` on a POSIX host.
pub(crate) async fn resolve_package_managed(
    definition: &PackageDefinition,
    context: Option<Context>,
) -> Capabilities {
    let package = definition.npm_package_name;
    let Some(context) = context else {
        return manual(Some(package), None);
    };
    let context = follow_vite_plus_shim(context).await;
    let manual_caps = manual(Some(package), Some(&context.real_command_path));
    let paths = [
        context.resolved_command_path.clone(),
        context.real_command_path.clone(),
    ];
    if let Some(mise) = resolve_mise(Some(package), definition.mise.as_ref(), &context).await {
        return mise;
    }
    if paths.iter().any(|p| is_nix_store(p)) {
        return manual_caps;
    }
    if let Some(name) = resolve_macports_owner(&context).await {
        let mut caps = manual(Some(package), Some(&context.real_command_path));
        if !name.is_empty() {
            caps.manual_command = Some(format!("sudo port upgrade {name}"));
        }
        return caps;
    }
    let native = match &definition.native_update {
        Some(native) => capabilities(
            Some(package),
            &context.resolved_command_path,
            &strings(native.args),
            &format!("{}-native", definition.provider),
            None,
        ),
        None => manual_caps.clone(),
    };
    if let Some(native_update) = &definition.native_update {
        if let Some(is_command_path) = native_update.is_command_path {
            if paths.iter().any(|p| is_command_path(p))
                && npm_global_prefix(&context.real_command_path, package).is_none()
            {
                return native;
            }
        }
    }
    let fallback = if definition
        .native_update
        .as_ref()
        .is_some_and(|n| n.requires_command_path)
        || paths
            .iter()
            .any(|p| normalize(p).contains("/node_modules/"))
    {
        manual_caps.clone()
    } else {
        native
    };
    let lookup = |lookup: Lookup| Some(std::sync::Arc::new(lookup));
    if paths.iter().any(|p| is_vite_plus_global(p)) {
        let caps = capabilities(
            Some(package),
            "vp",
            &strings(&["i", "-g", package]),
            "vite-plus-global",
            None,
        );
        return Capabilities {
            lookup: lookup(Lookup::VitePlus {
                package: package.to_owned(),
                env: context.env.clone(),
            }),
            cache_key: Some(
                ["vite-plus-global", package, &environment_key(&context.env)].join("\0"),
            ),
            ..with_update_env(caps, &context.env)
        };
    }
    let latest = format!("{package}@latest");
    if paths.iter().any(|p| is_bun_global(p)) {
        let caps = capabilities(
            Some(package),
            "bun",
            &strings(&["i", "-g", &latest]),
            "bun-global",
            None,
        );
        return Capabilities {
            lookup: lookup(Lookup::BunGlobal {
                package: package.to_owned(),
                env: context.env.clone(),
            }),
            ..with_update_env(caps, &context.env)
        };
    }
    if paths.iter().any(|p| is_pnpm_global(p)) {
        let version = run_probe("pnpm", &["--version"], &context.env, &[0]).await;
        let mut args = strings(&["add", "-g"]);
        if version.is_some_and(|v| super::satisfies_range(&v, ">=10.4.0")) {
            args.push(format!("--allow-build={package}"));
        }
        args.push(latest);
        let caps = capabilities(Some(package), "pnpm", &args, "pnpm-global", None);
        return Capabilities {
            lookup: lookup(Lookup::PnpmGlobal {
                package: package.to_owned(),
                env: context.env.clone(),
            }),
            ..with_update_env(caps, &context.env)
        };
    }
    if paths.iter().any(|p| is_yarn_global(p)) {
        let caps = capabilities(
            Some(package),
            "yarn",
            &strings(&["global", "add", &latest]),
            "yarn-global",
            None,
        );
        return with_update_env(caps, &context.env);
    }
    if is_volta_package_install(&context, package) {
        let caps = capabilities(
            Some(package),
            "volta",
            &strings(&["install", &latest]),
            "volta",
            None,
        );
        return with_update_env(caps, &context.env);
    }
    if let Some(prefix) = npm_global_prefix(&context.real_command_path, package) {
        let args = strings(&[
            "install",
            "-g",
            "--prefix",
            &prefix,
            &format!("--allow-scripts={package}"),
            &latest,
        ]);
        let caps = capabilities(
            Some(package),
            "npm",
            &args,
            &format!("npm-global:{}", normalize(&prefix)),
            None,
        );
        // Inside min-release-age `@latest` installs an older release than the
        // registry's tag, so only npm's own answer counts.
        return Capabilities {
            latest: Some(None),
            cache_key: Some(
                [
                    "npm-global",
                    &normalize(&prefix),
                    package,
                    &environment_key(&context.env),
                ]
                .join("\0"),
            ),
            lookup: lookup(Lookup::NpmGlobal {
                package: package.to_owned(),
                prefix: prefix.clone(),
                env: context.env.clone(),
            }),
            ..with_update_env(caps, &context.env)
        };
    }
    if let Some(homebrew) = homebrew_ownership(&context.real_command_path) {
        return resolve_homebrew(Some(package), &homebrew, &context)
            .await
            .unwrap_or(fallback);
    }
    fallback
}

/// `resolveProviderMaintenanceCapabilitiesEffect`'s context: the configured
/// binary on the instance's `PATH`, followed to its real path.
pub(crate) fn context(binary_path: &str, env: &ProcessEnv) -> Option<Context> {
    let binary_path = binary_path.trim();
    if binary_path.is_empty() {
        return None;
    }
    let resolved = resolve_command_path(binary_path, env)?;
    let real = real_path(&resolved)?;
    Some(Context {
        resolved_command_path: resolved,
        real_command_path: real,
        env: env.clone(),
    })
}

// ---------------------------------------------------------------------------
// Versions

/// `normalizeSemverVersion`.
fn normalize_semver(version: &str) -> String {
    let without_build = version.trim().split('+').next().unwrap_or("");
    let mut parts = without_build.split('-');
    let main = parts.next().unwrap_or("");
    let prerelease = parts.collect::<Vec<_>>().join("-");
    let mut segments: Vec<String> = main
        .split('.')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_owned)
        .collect();
    while !segments.is_empty() && segments.len() < 3 {
        segments.push("0".into());
    }
    if prerelease.is_empty() {
        segments.join(".")
    } else {
        format!("{}-{prerelease}", segments.join("."))
    }
}

/// `parseSemver`: (major, minor, patch, prerelease identifiers).
pub(crate) fn parse_semver(value: &str) -> Option<(u64, u64, u64, Vec<String>)> {
    let normalized = normalize_semver(value);
    let normalized = normalized.strip_prefix('v').unwrap_or(&normalized);
    let mut parts = normalized.split('-');
    let main = parts.next().unwrap_or("");
    let prerelease = parts.collect::<Vec<_>>().join("-");
    let segments: Vec<&str> = main.split('.').collect();
    if segments.len() != 3
        || !segments
            .iter()
            .all(|s| !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit()))
    {
        return None;
    }
    let number = |s: &str| s.parse::<u64>().ok();
    Some((
        number(segments[0])?,
        number(segments[1])?,
        number(segments[2])?,
        prerelease
            .split('.')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_owned)
            .collect(),
    ))
}

/// `compareSemverVersions` (a version that does not parse compares as text).
pub(crate) fn compare_semver(left: &str, right: &str) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    let (Some(l), Some(r)) = (parse_semver(left), parse_semver(right)) else {
        return left.cmp(right);
    };
    let core = (l.0, l.1, l.2).cmp(&(r.0, r.1, r.2));
    if core != Ordering::Equal {
        return core;
    }
    match (l.3.is_empty(), r.3.is_empty()) {
        (true, true) => return Ordering::Equal,
        (true, false) => return Ordering::Greater,
        (false, true) => return Ordering::Less,
        _ => {}
    }
    let numeric = |s: &str| s.bytes().all(|b| b.is_ascii_digit());
    for index in 0..l.3.len().max(r.3.len()) {
        let (Some(a), Some(b)) = (l.3.get(index), r.3.get(index)) else {
            return if l.3.len() < r.3.len() {
                Ordering::Less
            } else {
                Ordering::Greater
            };
        };
        let compared = match (numeric(a), numeric(b)) {
            (true, true) => {
                let (a, b) = (a.trim_start_matches('0'), b.trim_start_matches('0'));
                a.len().cmp(&b.len()).then_with(|| a.cmp(b))
            }
            (true, false) => Ordering::Less,
            (false, true) => Ordering::Greater,
            _ => a.cmp(b),
        };
        if compared != Ordering::Equal {
            return compared;
        }
    }
    Ordering::Equal
}

// ---------------------------------------------------------------------------
// Latest versions

/// `withConfiguredNpmVersionLookup`: with an npm package and no latest
/// version decided, the configured npm (its registry, auth and proxy) answers
/// after the installer's own lookup.
pub(crate) fn with_configured_npm(caps: Capabilities, host: &ProcessEnv) -> Capabilities {
    let Some(package) = caps.package_name.clone() else {
        return caps;
    };
    if caps.latest.is_some() {
        return caps;
    }
    let mut env = host.clone();
    if let Some(update_env) = caps.update.as_ref().and_then(|u| u.env.as_ref()) {
        env.extend(update_env.clone());
    }
    let Some(npm) = resolve_command_path("npm", &env) else {
        return caps;
    };
    let view = std::sync::Arc::new(Lookup::NpmView {
        npm: npm.clone(),
        package: package.clone(),
        env: env.clone(),
    });
    let installer = installer_key(&caps).unwrap_or_default();
    let lookup = match caps.lookup.clone() {
        Some(first) => std::sync::Arc::new(Lookup::Then(first, view)),
        None => view,
    };
    Capabilities {
        latest: Some(None),
        cache_key: Some(
            [
                "configured-npm",
                &npm,
                &package,
                &environment_key(&env),
                &installer,
            ]
            .join("\0"),
        ),
        lookup: Some(lookup),
        ..caps
    }
}

/// `installerLatestVersionKey`.
fn installer_key(caps: &Capabilities) -> Option<String> {
    caps.lookup.as_ref()?;
    if let Some(key) = &caps.cache_key {
        return Some(key.clone());
    }
    if let Some(update) = &caps.update {
        let mut parts = vec![update.lock_key.clone(), update.executable.clone()];
        parts.extend(update.args.iter().cloned());
        return Some(parts.join("\0"));
    }
    match (&caps.install_path, &caps.package_name) {
        (Some(path), _) => Some(["manual", path].join("\0")),
        (None, Some(package)) => Some(format!("npm-registry\0{package}")),
        _ => None,
    }
}

/// `ProviderLatestVersions`: an answer is kept an hour, a missing one a minute.
type LatestCache = std::collections::HashMap<String, (Option<String>, std::time::Instant)>;
static LATEST: std::sync::LazyLock<std::sync::Mutex<LatestCache>> =
    std::sync::LazyLock::new(Default::default);

async fn cached<F: std::future::Future<Output = Option<String>>>(
    key: &str,
    lookup: F,
) -> Option<String> {
    let now = std::time::Instant::now();
    if let Some((version, expires)) = LATEST.lock().unwrap_or_else(|e| e.into_inner()).get(key) {
        if *expires > now {
            return version.clone();
        }
    }
    let version = lookup.await;
    let ttl = if version.is_some() {
        Duration::from_secs(3600)
    } else {
        Duration::from_secs(60)
    };
    LATEST.lock().unwrap_or_else(|e| e.into_inner()).insert(
        key.to_owned(),
        (version.clone(), std::time::Instant::now() + ttl),
    );
    version
}

/// `resolveLatestProviderVersion`.
pub(crate) async fn resolve_latest(caps: &Capabilities) -> Option<String> {
    if let (Some(key), Some(lookup)) = (installer_key(caps), caps.lookup.clone()) {
        if let Some(version) = cached(&key, run_lookup(lookup)).await {
            return Some(version);
        }
    }
    if let Some(latest) = &caps.latest {
        return latest.clone();
    }
    if let Some(url) = &caps.muse_channel {
        return cached(&format!("muse:{url}"), fetch_muse_channel(url.clone())).await;
    }
    let package = caps.package_name.clone()?;
    cached(&package.clone(), fetch_npm_latest(package)).await
}

/// `fetchNpmLatestVersion`: the public registry's `latest` tag.
async fn fetch_npm_latest(package: String) -> Option<String> {
    let url = format!(
        "https://registry.npmjs.org/{}/latest",
        percent_encoding::utf8_percent_encode(&package, URI_COMPONENT)
    );
    let read = async {
        let response = super::http_client()
            .get(url)
            .header("accept", "application/json")
            .send()
            .await
            .ok()?;
        if !response.status().is_success() {
            return None;
        }
        let body: Value = response.json().await.ok()?;
        non_empty(body["version"].as_str())
    };
    tokio::time::timeout(Duration::from_secs(4), read)
        .await
        .ok()
        .flatten()
}

/// `latestMuseVersion`: the channel manifest's version.
async fn fetch_muse_channel(url: String) -> Option<String> {
    let read = async {
        let response = super::http_client().get(url).send().await.ok()?;
        if !response.status().is_success() {
            return None;
        }
        let manifest: Value = response.json().await.ok()?;
        let version = manifest["version"].as_str()?;
        Regex::new(r"^\d+\.\d+\.\d+-R\d+(?:\.\d+)?$")
            .unwrap()
            .is_match(version)
            .then(|| version.to_owned())
    };
    tokio::time::timeout(Duration::from_secs(4), read)
        .await
        .ok()
        .flatten()
}

/// `encodeURIComponent`'s unreserved set.
const URI_COMPONENT: &percent_encoding::AsciiSet = &percent_encoding::NON_ALPHANUMERIC
    .remove(b'-')
    .remove(b'_')
    .remove(b'.')
    .remove(b'!')
    .remove(b'~')
    .remove(b'*')
    .remove(b'\'')
    .remove(b'(')
    .remove(b')');

/// A JSON record of `{name: {latest?}}`, as `npm/pnpm outdated --json` print it.
fn outdated_latest(text: &str, package: &str) -> Result<Option<String>, ()> {
    let value: Value = serde_json::from_str(text).map_err(|_| ())?;
    let entries = value.as_object().ok_or(())?;
    if entries
        .values()
        .any(|e| !e.is_object() || e.get("latest").is_some_and(|l| !l.is_string()))
    {
        return Err(());
    }
    Ok(non_empty(
        entries.get(package).and_then(|e| e["latest"].as_str()),
    ))
}

fn listed_version(list: &Value, package: &str) -> Option<String> {
    non_empty(list["dependencies"][package]["version"].as_str())
}

fn run_lookup(
    lookup: std::sync::Arc<Lookup>,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Option<String>> + Send>> {
    Box::pin(async move {
        match &*lookup {
            Lookup::Mise { mise, tool, env } => {
                let latest = non_empty(
                    run_probe(mise, &["latest", tool], env, &[0])
                        .await
                        .as_deref(),
                )?;
                (!latest.chars().any(char::is_whitespace)).then_some(latest)
            }
            Lookup::Homebrew {
                brew,
                name,
                cask,
                env,
            } => {
                let info: Value = serde_json::from_str(
                    &run_probe(brew, &["info", "--json=v2", name], env, &[0]).await?,
                )
                .ok()?;
                let raw = if *cask {
                    info["casks"][0]["version"]
                        .as_str()
                        .map(|v| v.split(',').next().unwrap_or("").to_owned())
                } else {
                    info["formulae"][0]["versions"]["stable"]
                        .as_str()
                        .map(str::to_owned)
                };
                non_empty(raw.as_deref()).filter(|v| v != "latest")
            }
            Lookup::NpmGlobal {
                package,
                prefix,
                env,
            } => {
                let mut lookup_env: ProcessEnv = std::env::vars().collect();
                lookup_env.extend(env.clone());
                let npm = resolve_command_path("npm", &lookup_env)?;
                let outdated = run_probe(
                    &npm,
                    &["outdated", "-g", "--prefix", prefix, "--json", package],
                    env,
                    &[0, 1],
                )
                .await?;
                let value: Value = serde_json::from_str(&outdated).ok()?;
                if value.get("error").is_some() {
                    return None;
                }
                if let Some(latest) = outdated_latest(&outdated, package).ok()? {
                    return Some(latest);
                }
                let listed = run_probe(
                    &npm,
                    &[
                        "ls", "-g", "--prefix", prefix, "--depth", "0", "--json", package,
                    ],
                    env,
                    &[0, 1],
                )
                .await?;
                listed_version(&serde_json::from_str(&listed).ok()?, package)
            }
            Lookup::PnpmGlobal { package, env } => {
                let mut lookup_env: ProcessEnv = std::env::vars().collect();
                lookup_env.extend(env.clone());
                let pnpm = resolve_command_path("pnpm", &lookup_env)?;
                let outdated = run_probe(
                    &pnpm,
                    &[
                        "outdated",
                        "-g",
                        "--reporter=silent",
                        "--format",
                        "json",
                        package,
                    ],
                    env,
                    &[0, 1],
                )
                .await?;
                if let Some(latest) = outdated_latest(&outdated, package).ok()? {
                    return Some(latest);
                }
                let listed = run_probe(
                    &pnpm,
                    &[
                        "ls",
                        "-g",
                        "--reporter=silent",
                        "--depth",
                        "0",
                        "--json",
                        package,
                    ],
                    env,
                    &[0],
                )
                .await?;
                listed_version(
                    &serde_json::from_str::<Value>(&listed).ok()?.get(0)?.clone(),
                    package,
                )
            }
            Lookup::BunGlobal { package, env } => {
                let mut lookup_env: ProcessEnv = std::env::vars().collect();
                lookup_env.extend(env.clone());
                let bun = resolve_command_path("bun", &lookup_env)?;
                let mut env = env.clone();
                env.insert("FORCE_COLOR".into(), "0".into());
                let output = run_probe(&bun, &["outdated", "-g", package], &env, &[0]).await?;
                output.split('\n').find_map(|line| {
                    let cells: Vec<&str> = line.split('|').map(str::trim).collect();
                    (cells.len() == 6 && cells[1] == package)
                        .then(|| non_empty(Some(cells[4].trim_end_matches(" *"))))
                        .flatten()
                })
            }
            Lookup::VitePlus { package, env } => {
                let current: Value = serde_json::from_str(
                    &run_probe("vp", &["env", "current", "--json"], env, &[0]).await?,
                )
                .ok()?;
                let node = &current["node"];
                let bin = node["bin_path"].as_str()?;
                if node["installed"] != true
                    || node["source"] == "system PATH"
                    || !node["source"].is_string()
                    || !Path::new(bin).is_absolute()
                {
                    return None;
                }
                let npm = Path::new(bin).parent()?.join("npm").display().to_string();
                let packed = run_probe(
                    &npm,
                    &[
                        "pack",
                        "--dry-run",
                        "--json=false",
                        "--global",
                        &format!("{package}@latest"),
                    ],
                    env,
                    &[0],
                )
                .await?;
                let prefix = format!("{}-", package.trim_start_matches('@').replacen('/', "-", 1));
                let file = packed.trim();
                let version = file.strip_prefix(&prefix)?.strip_suffix(".tgz")?;
                (!version.is_empty() && !version.chars().any(char::is_whitespace))
                    .then(|| version.to_owned())
            }
            Lookup::NpmView { npm, package, env } => {
                let output = run_probe(
                    npm,
                    &[
                        "view",
                        "--global",
                        "--json",
                        &format!("{package}@latest"),
                        "version",
                    ],
                    env,
                    &[0],
                )
                .await?;
                match serde_json::from_str::<Value>(output.trim()).ok()? {
                    Value::String(v) => non_empty(Some(&v)),
                    Value::Array(items) if items.len() == 1 => non_empty(items[0].as_str()),
                    _ => None,
                }
            }
            Lookup::Then(first, second) => match run_lookup(first.clone()).await {
                Some(version) => Some(version),
                None => run_lookup(second.clone()).await,
            },
            Lookup::ClaudeNative { url } => {
                let read = async {
                    let response = super::http_client().get(url).send().await.ok()?;
                    if !response.status().is_success() {
                        return None;
                    }
                    let text = response.text().await.ok()?;
                    let version = text.trim();
                    Regex::new(r"^\d+\.\d+\.\d+$")
                        .unwrap()
                        .is_match(version)
                        .then(|| version.to_owned())
                };
                tokio::time::timeout(Duration::from_secs(4), read)
                    .await
                    .ok()
                    .flatten()
            }
            Lookup::CodexNative => {
                let read = async {
                    let response = super::http_client()
                        .get("https://releases.openai.com/codex/channels/latest")
                        .send()
                        .await
                        .ok()?;
                    if !response.status().is_success() {
                        return None;
                    }
                    let release: Value = response.json().await.ok()?;
                    let tag = release["tag_name"].as_str()?;
                    Regex::new(r"^rust-v(\d+\.\d+\.\d+)$")
                        .unwrap()
                        .captures(tag)
                        .map(|c| c[1].to_owned())
                };
                tokio::time::timeout(Duration::from_secs(4), read)
                    .await
                    .ok()
                    .flatten()
            }
            Lookup::FxRelease { url } => {
                let read = async {
                    let response = super::http_client().get(url).send().await.ok()?;
                    // A failed status reads as an empty body.
                    let body = if response.status().is_success() {
                        response.text().await.ok()?
                    } else {
                        String::new()
                    };
                    Regex::new(r"^v?(\d+\.\d+\.\d+)$")
                        .unwrap()
                        .captures(body.trim())
                        .map(|c| c[1].to_owned())
                };
                tokio::time::timeout(Duration::from_secs(4), read)
                    .await
                    .ok()
                    .flatten()
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn npm_prefixes_like_the_reference() {
        assert_eq!(
            npm_global_prefix("/usr/local/lib/node_modules/@scope/pkg/bin/x", "@scope/pkg"),
            Some("/usr/local".into())
        );
        assert_eq!(
            npm_global_prefix("/a/node_modules/b/lib/node_modules/pkg/x", "pkg"),
            None
        );
    }

    #[test]
    fn pnpm_updates_can_pin_a_version() {
        let caps = capabilities(
            Some("pkg"),
            "pnpm",
            &strings(&["add", "-g", "pkg@latest"]),
            "pnpm-global",
            None,
        );
        assert_eq!(
            targeted_update(&caps, "1.2.3").unwrap().command,
            "pnpm add -g pkg@1.2.3"
        );
        assert!(targeted_update(&caps, "latest").is_none());
    }

    #[test]
    fn compares_versions_like_the_reference() {
        use std::cmp::Ordering::*;
        assert_eq!(compare_semver("0.84.2", "0.84.3"), Less);
        assert_eq!(compare_semver("1.0.0-beta.2", "1.0.0-beta.10"), Less);
        assert_eq!(compare_semver("1.0.0", "1.0.0-rc.1"), Greater);
        assert_eq!(compare_semver("v1.2", "1.2.0"), Equal);
    }

    #[test]
    fn quotes_like_the_reference() {
        assert_eq!(quote("/a b/c"), "'/a b/c'");
        assert_eq!(quote("/usr/bin/grok"), "/usr/bin/grok");
    }
}
