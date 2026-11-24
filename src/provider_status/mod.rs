//! Provider status checks, as the reference's managed provider snapshots
//! (`makeManagedServerProvider`) and provider registry run them.
//!
//! Each enabled instance of every built-in driver is checked once the
//! server is listening, again whenever its settings (or
//! `enableProviderUpdateChecks`) change, when any settings change while it is
//! signed out, on the background-activity interval (five minutes by default),
//! and when a client asks (`server.refreshProviders`). A check runs the
//! instance's CLI from the server's working directory with the server's
//! environment, which `login_shell` filled from the user's login shell, and
//! `MISE_OFFLINE=1`:
//!
//! - Claude: `claude --version` (20s), then a Claude Code session that only
//!   initializes, reads its usage and settings and stops (25s), as the
//!   Agent SDK's capability probe does ([`claude`]).
//! - Codex: one `codex app-server` session that initializes and reads the
//!   account, skills, models and config (20s), with `codex --version` after
//!   a failure ([`codex`]).
//! - fx: `fx --version` (10s), then `fx status --json` and `fx models --json`
//!   (10s each) ([`fx`]).
//! - Kiro: `kiro-cli --version` (10s), `kiro-cli whoami --format json`, and
//!   once signed in `kiro-cli chat --list-models --format json` alongside an
//!   ACP agent that reads its agent picker (10s each) ([`kiro`]).
//! - Grok: `grok --version` (10s), `grok models` (10s), `grok inspect
//!   --json` (4s) and an ACP agent that only initializes (20s), then the
//!   account's usage ([`grok`]).
//! - Pi: `pi --version` (10s), then Pi's RPC mode for its state, models and
//!   commands (30s) ([`pi`]).
//! - Cursor: no CLI; its HTTP API's account and model catalog (15s, once
//!   more after a timeout) with the instance's API key or a stored browser
//!   sign-in, then the plan's usage ([`cursor`]).
//! - OpenCode: `opencode --version` (or a configured server's identity),
//!   then a managed `opencode serve` or the configured server: its 1.x
//!   inventory or its settled 2.x model catalog, and its usage
//!   ([`opencode`]). OpenCode is rechecked on the interval only while it is
//!   not ready.
//! - Antigravity: never started for a check; its sign-in settings and the
//!   installed runtime are read from disk, then its Google quota
//!   ([`antigravity`]).
//! - Muse Code: `muse --version` (4s), then a read-only `muse serve` that
//!   lists its models over MSP ([`muse`]).
//!
//! Grok's and Pi's version advisories come from how their CLI was installed
//! ([`maintenance`]), as the reference's enrichment pass works it out.
//!
//! With `enableProviderUpdateChecks` on, an installed CLI's version advisory
//! compares it with the latest release its installer would install
//! ([`maintenance`]): mise, Homebrew or a global npm/pnpm/bun answers for its
//! own installs, the configured npm (`npm view`) for an npm package, Codex's,
//! Claude's and Muse's release channels for their own installers, else the
//! public npm registry. HTTP lookups follow Node's runtime settings, as the
//! reference's `fetch` does ([`http_builder`]).
//!
//! The result replaces the instance's placeholder row in `server.getConfig`
//! and `server.refreshProviders`, is pushed to config subscribers as
//! `providerStatuses`, and is kept in `<base>/caches/<instanceId>.json`,
//! which the next start shows until its first check finishes. Drivers not
//! listed here are not checked yet: their rows keep the placeholder.
mod acp;
mod antigravity;
pub(crate) mod antigravity_text;
pub(crate) mod claude;
mod claude_credits;
mod claude_skills;
mod codex;
mod cursor;
mod fx;
pub(crate) mod grok;
mod kiro;
mod maintenance;
mod muse;
pub(crate) mod opencode;
pub(crate) mod pi;
pub(crate) mod reset_credits;

use crate::server::AppState;
use serde_json::{json, Map, Value};
use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// The drivers this server checks.
const CHECKED_DRIVERS: [&str; 10] = [
    "codex",
    "claudeAgent",
    "fx",
    "kiro",
    "grok",
    "pi",
    "cursor",
    "opencode",
    "antigravity",
    "muse",
];
/// The drivers whose snapshot gets its version advisory from an enrichment
/// pass after the probe, rather than from the probe itself.
const ENRICHED_DRIVERS: [&str; 7] = [
    "codex",
    "claudeAgent",
    "grok",
    "pi",
    "opencode",
    "muse",
    "fx",
];
/// `AUTH_PROBE_TIMEOUT_MS`.
const AUTH_PROBE_TIMEOUT_MS: u64 = 10_000;
/// `FAILED_CHECK_RECHECK_DELAY`: a periodic check that failed over a ready
/// provider is checked again this soon before it is shown.
const FAILED_CHECK_RECHECK: Duration = Duration::from_secs(30);
/// The background-activity profiles' `providerHealthRefreshInterval`.
const BALANCED_REFRESH_MS: i64 = 5 * 60 * 1000;

/// The environment a provider CLI starts with.
pub(crate) type ProcessEnv = BTreeMap<String, String>;

/// What a check found, before the registry merges it with what it showed:
/// the reference's `ServerProviderDraft` fields that a check sets.
#[derive(Debug, Clone, Default)]
pub(crate) struct Draft {
    pub installed: bool,
    pub version: Option<String>,
    pub status: &'static str,
    pub auth: Value,
    pub checked_at: String,
    pub message: Option<String>,
    pub models: Vec<Value>,
    pub update_required_models: Vec<Value>,
    pub slash_commands: Vec<Value>,
    pub skills: Vec<Value>,
    pub usage_limits: Option<Value>,
    /// Driver-specific fields the snapshot sets over the placeholder's.
    pub extra: Vec<(&'static str, Value)>,
    /// `buildServerProvider` with a driver: the probe's own (manual-only)
    /// version advisory.
    pub version_advisory: bool,
    /// How the CLI was installed, for the version advisory the enrichment
    /// pass adds; `None` is manual-only maintenance.
    pub maintenance: Option<maintenance::Capabilities>,
}

/// What the scheduler checks an instance for.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Reason {
    /// Startup, a settings change or a client's refresh: always published.
    Apply,
    /// The periodic check: a failure over a ready provider waits for a
    /// recheck (`deferFailure`).
    Interval,
}

struct Published {
    /// The instance's settings the row was checked with.
    fingerprint: Value,
    row: Value,
    /// Hydrated from the disk cache, not checked in this process.
    from_cache: bool,
}

#[derive(Default)]
struct State {
    published: HashMap<String, Published>,
}

/// The provider status checks of one server.
pub(crate) struct ProviderStatus {
    caches: PathBuf,
    state_dir: PathBuf,
    secrets: PathBuf,
    base_dir: PathBuf,
    opencode: opencode::Owners,
    state: Mutex<State>,
    claude_caps: Mutex<HashMap<String, claude::CapabilitiesCache>>,
    codex_skills: codex::SkillsHelper,
    reset_credits: reset_credits::Coordinator,
    locks: Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>,
    changes: tokio::sync::broadcast::Sender<()>,
    disabled: std::sync::atomic::AtomicBool,
}

impl ProviderStatus {
    /// The managed OpenCode servers, which text generation borrows too.
    pub(crate) fn opencode(&self) -> &opencode::Owners {
        &self.opencode
    }

    /// The server's home (managed runtimes) and state directories, which
    /// text generation shares with the status checks.
    pub(crate) fn directories(&self) -> (&Path, &Path) {
        (&self.base_dir, &self.state_dir)
    }

    pub(crate) fn new(paths: &crate::config::ServerPaths) -> Self {
        Self {
            caches: paths.caches.clone(),
            state_dir: paths.state_dir.clone(),
            secrets: paths.secrets.clone(),
            base_dir: paths.base_dir.clone(),
            opencode: Default::default(),
            state: Mutex::default(),
            claude_caps: Mutex::default(),
            codex_skills: Default::default(),
            reset_credits: Default::default(),
            locks: Mutex::default(),
            changes: tokio::sync::broadcast::channel(16).0,
            // Off until the server binary turns them on, so a test that
            // builds a server never runs the host's real provider CLIs.
            disabled: std::sync::atomic::AtomicBool::new(true),
        }
    }

    /// Never check: [`run`] and [`refresh`] do nothing.
    pub(crate) fn disable(&self) {
        self.disabled
            .store(true, std::sync::atomic::Ordering::SeqCst);
    }

    /// Check the provider CLIs, as the server binary does.
    pub(crate) fn enable(&self) {
        self.disabled
            .store(false, std::sync::atomic::Ordering::SeqCst);
    }

    fn is_disabled(&self) -> bool {
        self.disabled.load(std::sync::atomic::Ordering::SeqCst)
    }

    /// Fires after a check publishes a new row.
    pub(crate) fn subscribe(&self) -> tokio::sync::broadcast::Receiver<()> {
        self.changes.subscribe()
    }

    /// The row to show for `instance_id`: its last check (or cached status)
    /// while its settings are unchanged, otherwise `pending`, the reference's
    /// placeholder row.
    pub(crate) fn overlay(&self, instance_id: &str, fingerprint: &Value, pending: Value) -> Value {
        self.hydrate_boot_cache_for(instance_id, fingerprint, &pending);
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        match state.published.get(instance_id) {
            Some(published) if published.fingerprint == *fingerprint => published.row.clone(),
            _ => pending,
        }
    }

    /// The reference's boot read of `<cacheDir>/<instanceId>.json`
    /// (`hydrateCachedProvider`), once per instance before its first check.
    fn hydrate_boot_cache_for(&self, instance_id: &str, fingerprint: &Value, pending: &Value) {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if state.published.contains_key(instance_id) {
            return;
        }
        // Only an instance this server checks is hydrated; a cached row is
        // read once, so a failed read is not retried.
        state.published.insert(
            instance_id.to_owned(),
            Published {
                fingerprint: Value::Null,
                row: Value::Null,
                from_cache: true,
            },
        );
        if !CHECKED_DRIVERS.contains(&pending["driver"].as_str().unwrap_or("")) {
            return;
        }
        let Some(cached) = read_cache(&self.caches.join(format!("{instance_id}.json"))) else {
            return;
        };
        if cached["instanceId"] != pending["instanceId"] || cached["driver"] != pending["driver"] {
            return;
        }
        if pending["enabled"] != true || cached["enabled"] != pending["enabled"] {
            return;
        }
        let mut row = pending.clone();
        let object = row.as_object_mut().unwrap();
        object.shift_remove("message");
        let mut models = pending["models"].as_array().cloned().unwrap_or_default();
        for model in cached["models"].as_array().into_iter().flatten() {
            if model["isCustom"] != true && !models.iter().any(|m| m["slug"] == model["slug"]) {
                models.push(model.clone());
            }
        }
        object.insert("models".into(), models.into());
        for key in [
            "installed",
            "version",
            "status",
            "auth",
            "checkedAt",
            "slashCommands",
            "skills",
        ] {
            object.insert(key.into(), cached[key].clone());
        }
        if let Some(message) = cached["message"].as_str().filter(|m| !m.is_empty()) {
            object.insert("message".into(), message.into());
        }
        let row = apply_compatibility(row, &crate::model_manifest::BUNDLED);
        state.published.insert(
            instance_id.to_owned(),
            Published {
                fingerprint: fingerprint.clone(),
                row,
                from_cache: true,
            },
        );
    }

    fn lock_for(&self, instance_id: &str) -> Arc<tokio::sync::Mutex<()>> {
        self.locks
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .entry(instance_id.to_owned())
            .or_default()
            .clone()
    }

    fn published_row(&self, instance_id: &str, fingerprint: &Value) -> Option<Value> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        state
            .published
            .get(instance_id)
            .filter(|p| p.fingerprint == *fingerprint && !p.row.is_null())
            .map(|p| p.row.clone())
    }

    fn checked_in_process(&self, instance_id: &str, fingerprint: &Value) -> bool {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        state
            .published
            .get(instance_id)
            .is_some_and(|p| p.fingerprint == *fingerprint && !p.from_cache)
    }

    /// Keep the row published under `from` for the instance's new settings
    /// `to`; false when there is none to keep.
    fn retag(&self, instance_id: &str, from: &Value, to: &Value) -> bool {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        match state.published.get_mut(instance_id) {
            Some(published) if published.fingerprint == *from && !published.row.is_null() => {
                published.fingerprint = to.clone();
                drop(state);
                let _ = self.changes.send(());
                true
            }
            _ => false,
        }
    }

    fn publish(&self, instance_id: &str, fingerprint: Value, row: Value) {
        write_cache(&self.caches.join(format!("{instance_id}.json")), &row);
        let changed = {
            let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
            let previous = state.published.insert(
                instance_id.to_owned(),
                Published {
                    fingerprint,
                    row: row.clone(),
                    from_cache: false,
                },
            );
            previous.is_none_or(|p| p.row != row)
        };
        if changed {
            let _ = self.changes.send(());
        }
    }
}

/// One instance as the scheduler sees it.
struct Instance {
    id: String,
    driver: String,
    enabled: bool,
    config: Value,
    environment: Vec<(String, String)>,
    fingerprint: Value,
}

/// The ids of the checked instances in `settings`.
pub(crate) fn instance_ids(state: &AppState, settings: &Value) -> Vec<String> {
    instances(state, settings)
        .into_iter()
        .map(|i| i.id)
        .collect()
}

/// The checked instances in `settings`, in the order of the config's rows.
fn instances(state: &AppState, settings: &Value) -> Vec<Instance> {
    let rows = crate::server_config::providers::list_with(
        &state.config_started_at,
        |driver| state.settings.policy.provider_denial(driver),
        |_| None,
        settings,
        &Value::Null,
        &state.model_manifest.current(),
        |_, row| row,
    );
    rows.as_array()
        .into_iter()
        .flatten()
        .filter(|row| CHECKED_DRIVERS.contains(&row["driver"].as_str().unwrap_or("")))
        .map(|row| {
            let id = row["instanceId"].as_str().unwrap_or("").to_owned();
            let entry = &settings["providerInstances"][&id];
            let environment = entry["environment"]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(|variable| {
                    Some((
                        variable["name"].as_str()?.to_owned(),
                        variable["value"].as_str()?.to_owned(),
                    ))
                })
                .collect();
            Instance {
                fingerprint: crate::server_config::providers::fingerprint(
                    settings,
                    &id,
                    state
                        .settings
                        .policy
                        .provider_denial(row["driver"].as_str().unwrap_or("")),
                ),
                driver: row["driver"].as_str().unwrap_or("").to_owned(),
                enabled: row["enabled"] == true,
                config: entry["config"].clone(),
                environment,
                id,
            }
        })
        .collect()
}

/// Check `instance` and publish the result, unless its settings changed
/// while the check ran (the reference discards a replaced instance's late
/// result). Checks of one instance run one at a time.
async fn check_instance(
    state: &Arc<AppState>,
    instance: &Instance,
    reason: Reason,
) -> CheckOutcome {
    let service = &state.provider_status;
    let lock = service.lock_for(&instance.id);
    let _guard = lock.lock().await;
    if !instance.enabled {
        return CheckOutcome::Skipped;
    }
    let manifest = state.model_manifest.current();
    let env = base_env(&instance.environment);
    let draft = match instance.driver.as_str() {
        "claudeAgent" => {
            claude::check(
                service,
                &instance.id,
                &instance.config,
                &env,
                &service.state_dir,
                &manifest,
            )
            .await
        }
        "codex" => codex::check(&instance.config, &env, &manifest).await,
        "fx" => fx::check(&instance.config, &env).await,
        "kiro" => kiro::check(&instance.config, &env).await,
        "grok" => grok::check(&instance.config, &env).await,
        "antigravity" => {
            antigravity::check(
                &instance.config,
                &env,
                &instance.id,
                &service.base_dir,
                &service.state_dir,
                &manifest,
            )
            .await
        }
        "muse" => {
            let env = muse::environment(&base_env(&[]), &instance.environment);
            muse::check(&instance.config, &env).await
        }
        "opencode" => {
            opencode::check(
                &service.opencode,
                &instance.id,
                &instance.config,
                &env,
                &manifest,
            )
            .await
        }
        "cursor" => {
            cursor::check(
                &instance.config,
                &env,
                &instance.id,
                &service.secrets,
                &service.state_dir,
            )
            .await
        }
        "pi" => pi::check(&instance.config, &env, &service.caches, &manifest).await,
        _ => return CheckOutcome::Skipped,
    };
    // The row it replaces: the last one shown for these settings, or the
    // placeholder.
    let Ok(settings) = state.settings.internal() else {
        return CheckOutcome::Skipped;
    };
    let Some(current) = instances(state, &settings)
        .into_iter()
        .find(|i| i.id == instance.id)
    else {
        return CheckOutcome::Skipped;
    };
    if current.fingerprint != instance.fingerprint {
        return CheckOutcome::Skipped;
    }
    let pending = pending_row(state, &settings, &instance.id);
    let previous = service
        .published_row(&instance.id, &instance.fingerprint)
        .unwrap_or_else(|| pending.clone());
    if reason == Reason::Interval
        && draft.status == "error"
        && draft.auth["status"] != "unauthenticated"
        && previous["enabled"] == true
        && previous["status"] == "ready"
    {
        return CheckOutcome::Deferred;
    }
    let probed = draft_row(&pending, &draft);
    let mut row = merge_snapshot(&previous, probed);
    if ENRICHED_DRIVERS.contains(&instance.driver.as_str()) {
        let capabilities = match (&draft.maintenance, instance.driver.as_str()) {
            (Some(capabilities), _) => Some(capabilities.clone()),
            (None, "codex") => Some(codex_maintenance(&instance.config, &env).await),
            (None, "claudeAgent") => Some(claude_maintenance(&instance.config, &env).await),
            (None, _) => None,
        };
        row = with_version_advisory(
            row,
            capabilities.as_ref(),
            settings["enableProviderUpdateChecks"] != false,
        )
        .await;
        if instance.driver == "claudeAgent" {
            row = claude::scope_upgrade_message_to_channel(&manifest, row);
        }
    }
    row = apply_compatibility(row, &manifest);
    let unauthenticated = row["auth"]["status"] == "unauthenticated";
    service.publish(&instance.id, instance.fingerprint.clone(), row);
    if unauthenticated {
        CheckOutcome::SignedOut
    } else {
        CheckOutcome::Published
    }
}

#[derive(PartialEq, Eq)]
enum CheckOutcome {
    Published,
    SignedOut,
    Deferred,
    Skipped,
}

fn pending_row(state: &AppState, settings: &Value, instance_id: &str) -> Value {
    crate::server_config::providers::list_with(
        &state.config_started_at,
        |driver| state.settings.policy.provider_denial(driver),
        |_| None,
        settings,
        &Value::Null,
        &state.model_manifest.current(),
        |_, row| row,
    )
    .as_array()
    .into_iter()
    .flatten()
    .find(|row| row["instanceId"] == instance_id)
    .cloned()
    .unwrap_or(Value::Null)
}

/// Check every enabled instance (or only `only`) now, as
/// `server.refreshProviders` does, and wait for the results.
pub(crate) async fn refresh(state: &Arc<AppState>, only: Option<&str>) {
    if state.provider_status.is_disabled() {
        return;
    }
    let Ok(settings) = state.settings.internal() else {
        return;
    };
    let checks = instances(state, &settings)
        .into_iter()
        .filter(|instance| only.is_none_or(|id| id == instance.id))
        .map(|instance| {
            let state = state.clone();
            async move {
                check_instance(&state, &instance, Reason::Apply).await;
            }
        })
        .collect::<Vec<_>>();
    futures_join_all(checks).await;
}

// ---------------------------------------------------------------------------
// Workspace snapshots

/// `PROVIDER_WORKSPACE_SNAPSHOT_TTL_MS`.
const WORKSPACE_SNAPSHOT_TTL_MS: i64 = 5 * 60_000;
/// `MAX_WORKSPACE_SNAPSHOTS_PER_PROVIDER`.
const MAX_WORKSPACE_SNAPSHOTS: usize = 16;

/// `isProviderWorkspaceSnapshotCurrent` and not pending.
fn workspace_snapshot_current(snapshot: &Value, now_ms: i64) -> bool {
    let pending = snapshot["slashCommandsPending"] == true || snapshot["modelsPending"] == true;
    let checked = snapshot["checkedAt"]
        .as_str()
        .and_then(|at| {
            time::OffsetDateTime::parse(at, &time::format_description::well_known::Rfc3339).ok()
        })
        .map(|at| (at.unix_timestamp_nanos() / 1_000_000) as i64);
    !pending && checked.is_some_and(|at| now_ms - at < WORKSPACE_SNAPSHOT_TTL_MS)
}

/// `upsertProviderWorkspaceSnapshot` for a scan that found `skills`, with the
/// machine snapshot's slash commands (Codex's `snapshotForCwd`).
/// What a driver's folder scan (`snapshotForCwd`) found.
pub(super) struct FolderScan {
    pub skills: Vec<Value>,
    pub slash_commands: Vec<Value>,
    /// `slashCommandsPending` as the driver set it; `None` takes the machine
    /// check's failure (`status === "error"`).
    pub slash_pending: Option<bool>,
}

/// `upsertProviderWorkspaceSnapshot`.
fn upsert_workspace_snapshot(row: &mut Value, cwd: &str, checked_at: &str, scan: FolderScan) {
    let previous = row["workspaceSnapshots"]
        .as_array()
        .and_then(|all| all.iter().find(|s| s["cwd"] == cwd))
        .cloned();
    let pending = scan.slash_pending.unwrap_or(row["status"] == "error");
    let slash_commands = if pending {
        previous
            .as_ref()
            .map(|p| p["slashCommands"].clone())
            .unwrap_or_else(|| json!(scan.slash_commands))
    } else {
        json!(scan.slash_commands)
    };
    let mut snapshot =
        json!({"cwd": cwd, "checkedAt": checked_at, "slashCommands": slash_commands});
    if pending {
        snapshot["slashCommandsPending"] = json!(true);
    }
    snapshot["skills"] = json!(scan.skills);
    if let Some(descriptors) = previous.as_ref().and_then(|p| p.get("optionDescriptors")) {
        snapshot["optionDescriptors"] = descriptors.clone();
    }
    let mut all: Vec<Value> = row["workspaceSnapshots"]
        .as_array()
        .map(|all| all.iter().filter(|s| s["cwd"] != cwd).cloned().collect())
        .unwrap_or_default();
    all.push(snapshot);
    let skip = all.len().saturating_sub(MAX_WORKSPACE_SNAPSHOTS);
    row["workspaceSnapshots"] = json!(all.into_iter().skip(skip).collect::<Vec<_>>());
}

/// The drivers whose folders are scanned here (`snapshotForCwd`).
pub(crate) fn scans_folders(driver: &str) -> bool {
    matches!(driver, "codex" | "claudeAgent" | "grok" | "pi")
}

/// A driver's folder scan, against its published (machine) row.
async fn scan_folder(
    service: &ProviderStatus,
    instance: &Instance,
    row: &Value,
    cwd: &str,
) -> Option<FolderScan> {
    let env = base_env(&instance.environment);
    let machine_commands = || row["slashCommands"].as_array().cloned().unwrap_or_default();
    match instance.driver.as_str() {
        "codex" => Some(FolderScan {
            skills: service
                .codex_skills
                .probe(&instance.config, &env, cwd)
                .await?,
            slash_commands: machine_commands(),
            slash_pending: None,
        }),
        "claudeAgent" => Some(claude::folder_scan(&instance.config, &env, cwd).await),
        "grok" => Some(FolderScan {
            skills: grok::folder_skills(&instance.config, &env, cwd).await?,
            slash_commands: machine_commands(),
            slash_pending: None,
        }),
        "pi" => {
            let (slash_commands, skills) = pi::folder_commands(&instance.config, &env, cwd).await?;
            Some(FolderScan {
                skills,
                slash_commands,
                slash_pending: None,
            })
        }
        _ => None,
    }
}

/// `refreshWorkspaceSnapshot`: scan `cwd` for an enabled instance whose
/// snapshot of it is missing, stale or pending (or always, with `fresh`,
/// which first drops other instances' snapshots of it and checks the
/// instance again), and publish the result on its row. Scans of one folder
/// run one at a time, so a second request waits for the first's result. A
/// failed scan publishes nothing, as the reference recovers from it.
pub(crate) async fn refresh_workspace_snapshot(
    state: &Arc<AppState>,
    instance_id: &str,
    cwd: &str,
    fresh: bool,
) {
    if state.provider_status.is_disabled() {
        return;
    }
    let Ok(settings) = state.settings.internal() else {
        return;
    };
    let Some(instance) = instances(state, &settings)
        .into_iter()
        .find(|instance| instance.id == instance_id)
    else {
        return;
    };
    let service = &state.provider_status;
    if fresh {
        let changed = {
            let mut state = service.state.lock().unwrap_or_else(|e| e.into_inner());
            let mut changed = false;
            for (id, published) in state.published.iter_mut() {
                if id == instance_id {
                    continue;
                }
                if let Some(all) = published.row["workspaceSnapshots"].as_array_mut() {
                    let before = all.len();
                    all.retain(|s| s["cwd"] != cwd);
                    changed |= all.len() != before;
                }
            }
            changed
        };
        if changed {
            let _ = service.changes.send(());
        }
    }
    if !instance.enabled || !scans_folders(&instance.driver) {
        return;
    }
    let lock = service.lock_for(&format!("folder\0{instance_id}\0{cwd}"));
    let _guard = lock.lock().await;
    if fresh {
        refresh(state, Some(instance_id)).await;
    }
    // An instance whose row is not published yet is scanned too, though its
    // result has no row to land on.
    let row = service
        .published_row(&instance.id, &instance.fingerprint)
        .unwrap_or(Value::Null);
    let now_ms = crate::platform::now_ms();
    let current = row["workspaceSnapshots"]
        .as_array()
        .and_then(|all| all.iter().find(|s| s["cwd"] == cwd))
        .is_some_and(|s| workspace_snapshot_current(s, now_ms));
    if current && !fresh {
        return;
    }
    // Use the scan's start for the TTL, not the machine check's time.
    let scanned_at = now_iso();
    let Some(scan) = scan_folder(service, &instance, &row, cwd).await else {
        return;
    };
    let changed = {
        let mut state = service.state.lock().unwrap_or_else(|e| e.into_inner());
        match state.published.get_mut(&instance.id) {
            Some(published)
                if published.fingerprint == instance.fingerprint && !published.row.is_null() =>
            {
                upsert_workspace_snapshot(&mut published.row, cwd, &scanned_at, scan);
                true
            }
            _ => false,
        }
    };
    if changed {
        let _ = service.changes.send(());
    }
}

/// The rows as the reference's refresh answers them: each checked row is
/// the probe's, before the version advisory its enrichment adds (which the
/// next `providerStatuses` event carries).
pub(crate) fn before_enrichment(providers: &Value) -> Value {
    let mut providers = providers.clone();
    for row in providers.as_array_mut().into_iter().flatten() {
        let driver = row["driver"].as_str().unwrap_or("").to_owned();
        if !ENRICHED_DRIVERS.contains(&driver.as_str()) {
            continue;
        }
        // fx's probe builds its row with the manual advisory, which its
        // enrichment then replaces.
        if driver == "fx" && row.get("versionAdvisory").is_some() {
            row["versionAdvisory"] = manual_version_advisory(
                row["version"].as_str(),
                row["checkedAt"].as_str().unwrap_or(""),
            );
        } else if let Some(object) = row.as_object_mut() {
            object.shift_remove("versionAdvisory");
        }
    }
    providers
}

async fn futures_join_all(checks: Vec<impl std::future::Future<Output = ()> + Send + 'static>) {
    let handles: Vec<_> = checks.into_iter().map(tokio::spawn).collect();
    for handle in handles {
        let _ = handle.await;
    }
}

/// The driver's `refreshOnInterval`: OpenCode only rechecks a row that is not
/// ready yet (a failed or still-loading catalog), and Grok one that is not
/// ready (#39623); healthy instances need no recurring checks.
fn refresh_on_interval(driver: &str, row: Option<&Value>) -> bool {
    match (driver, row) {
        ("grok", Some(row)) => row["status"] != "ready",
        ("opencode", Some(row)) => {
            row["enabled"] == true
                && row["installed"] == true
                && row["status"] != "ready"
                && row["auth"]["status"] == "unknown"
        }
        _ => true,
    }
}

/// `resolveServerBackgroundActivitySettings(...).providerHealthRefreshInterval`.
fn refresh_interval_ms(settings: &Value) -> i64 {
    let activity = &settings["backgroundActivity"];
    let preset = |profile: &str| match profile {
        "performance" => 60 * 1000,
        "battery-saver" => 15 * 60 * 1000,
        _ => BALANCED_REFRESH_MS,
    };
    match activity["profile"].as_str() {
        Some("custom") => activity["overrides"]["providerHealthRefreshInterval"]
            .as_f64()
            .map(|ms| ms as i64)
            .unwrap_or_else(|| preset(activity["baseProfile"].as_str().unwrap_or("balanced"))),
        Some(profile) => preset(profile),
        None => BALANCED_REFRESH_MS,
    }
}

/// The checks' scheduler, started once the server listens (the reference
/// waits for `awaitServerReady` so the first client load is not slowed).
pub(crate) async fn run(state: Arc<AppState>) {
    if state.provider_status.is_disabled() {
        return;
    }
    // The first checks read the model catalog the startup manifest fetch
    // brings (the refresh is serialized and TTL-gated, so this only waits for
    // that fetch), as the reference's first checks do.
    let _ = tokio::time::timeout(Duration::from_secs(5), state.model_manifest.refresh()).await;
    let mut seen: HashMap<String, Value> = HashMap::new();
    let mut last_settings = Value::Null;
    let mut next_refresh: HashMap<String, tokio::time::Instant> = HashMap::new();
    let mut recheck: HashMap<String, tokio::time::Instant> = HashMap::new();
    let (done_tx, mut done_rx) = tokio::sync::mpsc::unbounded_channel::<(String, Reason, bool)>();
    let mut running: HashMap<String, Reason> = HashMap::new();
    let mut ticker = tokio::time::interval(Duration::from_millis(250));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            _ = ticker.tick() => {}
            Some((id, reason, deferred)) = done_rx.recv() => {
                running.remove(&id);
                if deferred && reason == Reason::Interval {
                    recheck.insert(id, tokio::time::Instant::now() + FAILED_CHECK_RECHECK);
                }
                continue;
            }
        }
        if *state.closing.borrow() {
            return;
        }
        let Ok(settings) = state.settings.internal() else {
            continue;
        };
        let settings_changed = !last_settings.is_null() && settings != last_settings;
        last_settings = settings.clone();
        let interval_ms = refresh_interval_ms(&settings);
        let now = tokio::time::Instant::now();
        let mut start = Vec::new();
        let current = instances(&state, &settings);
        for instance in &current {
            let previous = seen.get(&instance.id).cloned();
            let mut changed = previous.as_ref() != Some(&instance.fingerprint);
            if changed {
                seen.insert(instance.id.clone(), instance.fingerprint.clone());
            }
            // OpenCode's driver does not recheck when only the snapshot
            // settings (`enableProviderUpdateChecks`) change
            // (`checkProviderOnSettingsChange: () => false`); its row stays.
            if let Some(previous) = previous.as_ref().filter(|_| changed) {
                let settings_only = previous["entry"] == instance.fingerprint["entry"]
                    && previous["denied"] == instance.fingerprint["denied"];
                if instance.driver == "opencode"
                    && settings_only
                    && state
                        .provider_status
                        .retag(&instance.id, previous, &instance.fingerprint)
                {
                    changed = false;
                }
            }
            if !instance.enabled {
                next_refresh.remove(&instance.id);
                continue;
            }
            let interval = || {
                now + if interval_ms <= 0 {
                    Duration::from_secs(60)
                } else {
                    Duration::from_millis(interval_ms as u64)
                }
            };
            // `shouldRefreshOnInterval`: the driver wants a periodic check and
            // a client is watching provider status (generally or this instance).
            let wanted = || {
                refresh_on_interval(
                    &instance.driver,
                    state
                        .provider_status
                        .published_row(&instance.id, &instance.fingerprint)
                        .as_ref(),
                ) && state
                    .background
                    .provider_status_demand(&instance.id, &settings)
            };
            let signed_out = state
                .provider_status
                .published_row(&instance.id, &instance.fingerprint)
                .is_some_and(|row| row["auth"]["status"] == "unauthenticated");
            if changed || (settings_changed && signed_out) {
                start.push((instance, Reason::Apply));
                next_refresh.insert(instance.id.clone(), interval());
                recheck.remove(&instance.id);
            } else if recheck.get(&instance.id).is_some_and(|at| now >= *at) {
                // The quick recheck after a failed periodic check is a periodic
                // check too, so it waits for demand as well.
                recheck.remove(&instance.id);
                next_refresh.insert(instance.id.clone(), interval());
                if wanted() {
                    start.push((instance, Reason::Apply));
                }
            } else if next_refresh.get(&instance.id).is_some_and(|at| now >= *at) {
                next_refresh.insert(instance.id.clone(), interval());
                if interval_ms > 0
                    && state
                        .provider_status
                        .checked_in_process(&instance.id, &instance.fingerprint)
                    && wanted()
                {
                    start.push((instance, Reason::Interval));
                }
            }
        }
        for (instance, reason) in start {
            if running.contains_key(&instance.id) && reason == Reason::Interval {
                continue;
            }
            running.insert(instance.id.clone(), reason);
            let state = state.clone();
            let done = done_tx.clone();
            let instance = Instance {
                id: instance.id.clone(),
                driver: instance.driver.clone(),
                enabled: instance.enabled,
                config: instance.config.clone(),
                environment: instance.environment.clone(),
                fingerprint: instance.fingerprint.clone(),
            };
            tokio::spawn(async move {
                let outcome = check_instance(&state, &instance, reason).await;
                let _ = done.send((instance.id, reason, outcome == CheckOutcome::Deferred));
            });
        }
    }
}

// ---------------------------------------------------------------------------
// Rows

/// `buildServerProvider` over the placeholder row: the draft's fields
/// replace its status fields.
fn draft_row(pending: &Value, draft: &Draft) -> Value {
    let mut row = pending.clone();
    let object = row.as_object_mut().unwrap();
    let enabled = object
        .get("enabled")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    for key in [
        "message",
        "updateRequiredModels",
        "usageLimits",
        "versionAdvisory",
        "compatibilityAdvisory",
    ] {
        object.shift_remove(key);
    }
    object.insert("installed".into(), draft.installed.into());
    object.insert("version".into(), json!(draft.version));
    object.insert(
        "status".into(),
        json!(if enabled { draft.status } else { "disabled" }),
    );
    object.insert("auth".into(), draft.auth.clone());
    object.insert("checkedAt".into(), json!(draft.checked_at));
    if let Some(message) = draft.message.as_ref().filter(|m| !m.is_empty()) {
        object.insert("message".into(), json!(message));
    }
    object.insert("models".into(), json!(draft.models));
    if !draft.update_required_models.is_empty() {
        object.insert(
            "updateRequiredModels".into(),
            json!(draft.update_required_models),
        );
    }
    object.insert("slashCommands".into(), json!(draft.slash_commands));
    object.insert("skills".into(), json!(draft.skills));
    if let Some(usage) = &draft.usage_limits {
        object.insert("usageLimits".into(), usage.clone());
    }
    for (key, value) in &draft.extra {
        object.insert((*key).into(), value.clone());
    }
    if draft.version_advisory {
        object.insert(
            "versionAdvisory".into(),
            manual_version_advisory(draft.version.as_deref(), &draft.checked_at),
        );
    }
    row
}

/// `createProviderVersionAdvisory` for a CLI with manual-only maintenance and
/// no latest version.
fn manual_version_advisory(current: Option<&str>, checked_at: &str) -> Value {
    json!({
        "status": "unknown",
        "currentVersion": current,
        "latestVersion": null,
        "updateCommand": null,
        "canUpdate": false,
        "canInstallVersion": false,
        "checkedAt": checked_at,
        "message": null,
    })
}

fn has_capabilities(model: &Value) -> bool {
    model["capabilities"]["optionDescriptors"]
        .as_array()
        .is_some_and(|d| !d.is_empty())
}

/// `shouldRetainMissingProviderModels`.
fn retain_missing_models(row: &Value) -> bool {
    let installed = row["installed"] == true;
    let status = row["status"].as_str().unwrap_or("");
    let auth = row["auth"]["status"].as_str().unwrap_or("");
    let driver = row["driver"].as_str().unwrap_or("");
    match driver {
        "pi" => return !(installed && matches!(status, "ready" | "warning") && auth != "unknown"),
        "acpRegistry" => {
            return !(installed && status == "ready" && row.get("nativeSessions").is_some())
        }
        "kiro" | "claudeAgent" => return true,
        "antigravity" | "codex" | "opencode" => {}
        _ => return !(installed && status == "ready"),
    }
    if matches!(driver, "antigravity" | "codex")
        && (row["enabled"] != true || auth == "unauthenticated")
    {
        return false;
    }
    let pending_antigravity = driver == "antigravity" && status == "warning" && auth == "unknown";
    let pending = row["enabled"] == true && !installed && status == "warning";
    let incomplete = installed
        && (status == "error" || (matches!(driver, "codex" | "opencode") && status == "warning"));
    pending_antigravity || pending || incomplete
}

/// `mergeProviderSnapshot`: the probed row, keeping models the probe no
/// longer lists where the driver's catalog can omit them.
/// `carrySavedAntigravityAccount`: a health check (which never signs in)
/// keeps a verified Google account a previous snapshot carried.
fn carry_antigravity_account(previous: &Value, next: &mut Value) {
    let carries = next["driver"] == "antigravity"
        && previous["driver"] == "antigravity"
        && next["enabled"] == true
        && next["auth"]["status"] == "unknown"
        && previous["auth"]["status"] == "authenticated"
        && (next["auth"].get("type").is_none() || next["auth"]["type"] == previous["auth"]["type"])
        && (next["installed"] == true || next["status"] == "warning");
    if !carries {
        return;
    }
    let ready = next["installed"] == true && next["status"] == "warning";
    next["auth"] = previous["auth"].clone();
    if ready {
        next["status"] = json!("ready");
        if let Some(object) = next.as_object_mut() {
            object.shift_remove("message");
        }
    }
}

fn merge_snapshot(previous: &Value, mut next: Value) -> Value {
    carry_antigravity_account(previous, &mut next);
    let retain = retain_missing_models(&next);
    let update_required: Vec<&Value> = next["updateRequiredModels"]
        .as_array()
        .map(|models| models.iter().map(|m| &m["slug"]).collect())
        .unwrap_or_default();
    let previous_models = previous["models"].as_array().cloned().unwrap_or_default();
    let retainable: Vec<Value> = previous_models
        .iter()
        .filter(|m| m["isCustom"] != true && !update_required.contains(&&m["slug"]))
        .cloned()
        .collect();
    let next_models = next["models"].as_array().cloned().unwrap_or_default();
    let models = if retain && next_models.is_empty() && !retainable.is_empty() {
        retainable
    } else {
        let mut merged: Vec<Value> = next_models
            .iter()
            .map(
                |model| match previous_models.iter().find(|p| p["slug"] == model["slug"]) {
                    Some(prev) if !has_capabilities(model) && has_capabilities(prev) => {
                        let mut model = model.clone();
                        model["capabilities"] = prev["capabilities"].clone();
                        model
                    }
                    _ => model.clone(),
                },
            )
            .collect();
        if retain {
            for model in retainable {
                if !next_models.iter().any(|n| n["slug"] == model["slug"]) {
                    merged.push(model);
                }
            }
        }
        merged
    };
    // `mergeProviderSnapshot`: folder snapshots outlive a machine check; when
    // the machine models changed, each folder's models are rescanned.
    let models_changed = previous["models"] != json!(&models);
    let previous_snapshots: Option<Vec<Value>> =
        previous["workspaceSnapshots"].as_array().map(|all| {
            all.iter()
                .map(|snapshot| {
                    let mut snapshot = snapshot.clone();
                    if models_changed {
                        snapshot["modelsPending"] = json!(true);
                    }
                    snapshot
                })
                .collect()
        });
    let replaces = next["enabled"] != true
        || next["auth"]["status"] == "unauthenticated"
        || (next["installed"] != true && next["status"] == "error");
    match (
        next["workspaceSnapshots"].as_array().cloned(),
        previous_snapshots,
    ) {
        (None, Some(previous)) => next["workspaceSnapshots"] = json!(previous),
        (Some(own), Some(previous)) if !replaces => {
            let mut all: Vec<Value> = previous
                .into_iter()
                .filter(|p| !own.iter().any(|o| o["cwd"] == p["cwd"]))
                .collect();
            all.extend(own);
            let skip = all.len().saturating_sub(MAX_WORKSPACE_SNAPSHOTS);
            next["workspaceSnapshots"] = json!(all.into_iter().skip(skip).collect::<Vec<_>>());
        }
        _ => {}
    }
    next["models"] = models.into();
    // `shouldRetainMissingOpenCodeMetadata`: an empty list keeps the last one.
    if next["driver"] == "opencode" && retain {
        for key in ["slashCommands", "skills"] {
            if next[key].as_array().is_some_and(Vec::is_empty) && previous.get(key).is_some() {
                next[key] = previous[key].clone();
            }
        }
    }
    next
}

/// `enrichProviderSnapshotWithVersionAdvisory` without a latest-version
/// lookup: the advisory the CLI's maintenance capabilities give.
async fn with_version_advisory(
    mut row: Value,
    capabilities: Option<&maintenance::Capabilities>,
    update_checks: bool,
) -> Value {
    if row["enabled"] != true {
        return row;
    }
    let manual = maintenance::manual(None, None);
    // `installedVersion`, when the snapshot has one, is what an update replaces.
    let current = match row.get("installedVersion") {
        Some(installed) => installed.as_str().map(str::to_owned),
        None => row["version"].as_str().map(str::to_owned),
    };
    // With update checks on, an installed CLI with a version has its latest
    // release looked up, and the advisory is dated then.
    let (latest, checked_at) = if update_checks && row["installed"] == true && current.is_some() {
        let latest = maintenance::resolve_latest(capabilities.unwrap_or(&manual)).await;
        (latest, now_iso())
    } else {
        (None, row["checkedAt"].as_str().unwrap_or("").to_owned())
    };
    row["versionAdvisory"] = maintenance::version_advisory(
        capabilities.unwrap_or(&manual),
        current.as_deref(),
        latest.as_deref(),
        &checked_at,
        capabilities
            .and_then(|c| c.compare_versions)
            .unwrap_or(maintenance::compare_semver),
    );
    row
}

/// An HTTP client for provider checks that follows Node's own runtime
/// settings, as the reference's `fetch` does: proxies from the environment
/// (`HTTPS_PROXY`, `NO_PROXY`) only with `NODE_USE_ENV_PROXY=1`, and the CAs in
/// `NODE_EXTRA_CA_CERTS` trusted beside the built-in ones.
pub(crate) fn http_builder() -> reqwest::ClientBuilder {
    let mut builder = reqwest::Client::builder();
    let env_proxy = std::env::var("NODE_USE_ENV_PROXY").is_ok_and(|v| v == "1");
    if !env_proxy {
        builder = builder.no_proxy();
    }
    if let Some(pem) =
        std::env::var_os("NODE_EXTRA_CA_CERTS").and_then(|path| std::fs::read(path).ok())
    {
        for certificate in reqwest::Certificate::from_pem_bundle(&pem).unwrap_or_default() {
            builder = builder.add_root_certificate(certificate);
        }
    }
    builder
}

pub(crate) fn http_client() -> reqwest::Client {
    http_builder().build().unwrap_or_default()
}

/// The server's own environment (`HostProcess.Environment`).
fn host_env() -> ProcessEnv {
    base_env(&[])
}

/// The reference's Codex resolver: package-managed `@openai/codex`, its
/// standalone install updated with `codex update`, and that install's own
/// release channel for the latest version.
async fn codex_maintenance(config: &Value, env: &ProcessEnv) -> maintenance::Capabilities {
    fn standalone(path: &str) -> bool {
        regex::Regex::new(r"(?is)^(.*)/packages/standalone/")
            .unwrap()
            .is_match(&path.replace('\\', "/"))
    }
    let definition = maintenance::PackageDefinition {
        provider: "codex",
        npm_package_name: "@openai/codex",
        native_update: Some(maintenance::NativeUpdate {
            args: &["update"],
            is_command_path: Some(standalone),
            requires_command_path: true,
        }),
        mise: Some(maintenance::MiseTool {
            tool: "codex",
            upgrade: true,
        }),
    };
    let binary = cli_binary(config, "codex");
    let caps = maintenance::with_configured_npm(
        maintenance::resolve_package_managed(&definition, maintenance::context(&binary, env)).await,
        &host_env(),
    );
    if caps
        .update
        .as_ref()
        .is_some_and(|u| u.lock_key == "codex-native")
    {
        return maintenance::Capabilities {
            latest: Some(None),
            cache_key: Some("https://releases.openai.com/codex/channels/latest".into()),
            lookup: Some(std::sync::Arc::new(maintenance::Lookup::CodexNative)),
            ..caps
        };
    }
    caps
}

/// The reference's Claude resolver: package-managed
/// `@anthropic-ai/claude-code`, its native install updated with `claude
/// update`, and the native release channel its settings choose.
async fn claude_maintenance(config: &Value, env: &ProcessEnv) -> maintenance::Capabilities {
    fn native(path: &str) -> bool {
        let normalized = path.replace('\\', "/").to_lowercase();
        normalized.ends_with("/.local/bin/claude.exe")
            || normalized.contains("/.local/share/claude/")
            || normalized.contains("/claude/versions/")
    }
    let definition = maintenance::PackageDefinition {
        provider: "claudeAgent",
        npm_package_name: "@anthropic-ai/claude-code",
        native_update: Some(maintenance::NativeUpdate {
            args: &["update"],
            is_command_path: Some(native),
            requires_command_path: true,
        }),
        mise: Some(maintenance::MiseTool {
            tool: "claude",
            upgrade: true,
        }),
    };
    let binary = cli_binary(config, "claude");
    let caps = maintenance::with_configured_npm(
        maintenance::resolve_package_managed(&definition, maintenance::context(&binary, env)).await,
        &host_env(),
    );
    if caps
        .update
        .as_ref()
        .is_some_and(|u| u.lock_key == "claudeAgent-native")
    {
        let url = format!(
            "https://downloads.claude.ai/claude-code-releases/{}",
            claude_update_channel()
        );
        return maintenance::Capabilities {
            latest: Some(None),
            cache_key: Some(url.clone()),
            lookup: Some(std::sync::Arc::new(maintenance::Lookup::ClaudeNative {
                url,
            })),
            ..caps
        };
    }
    caps
}

/// `readUpdateChannel`: the last `autoUpdatesChannel` among the user's and
/// the managed Claude Code settings, `latest` by default.
fn claude_update_channel() -> String {
    let managed = match std::env::consts::OS {
        "macos" => PathBuf::from("/Library/Application Support/ClaudeCode"),
        "windows" => PathBuf::from(
            std::env::var("PROGRAMFILES").unwrap_or_else(|_| "C:\\Program Files".into()),
        )
        .join("ClaudeCode"),
        _ => PathBuf::from("/etc/claude-code"),
    };
    let config_dir = std::env::var("CLAUDE_CONFIG_DIR")
        .ok()
        .map(|d| d.trim().to_owned())
        .filter(|d| !d.is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| home_dir().join(".claude"));
    let mut paths = vec![
        config_dir.join("settings.json"),
        managed.join("managed-settings.json"),
    ];
    let mut drop_ins: Vec<String> = std::fs::read_dir(managed.join("managed-settings.d"))
        .into_iter()
        .flatten()
        .flatten()
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .filter(|n| n.ends_with(".json") && !n.starts_with('.'))
        .collect();
    drop_ins.sort();
    paths.extend(
        drop_ins
            .iter()
            .map(|n| managed.join("managed-settings.d").join(n)),
    );
    paths
        .iter()
        .filter_map(|path| {
            let settings: Value =
                serde_json::from_str(&std::fs::read_to_string(path).ok()?).ok()?;
            settings["autoUpdatesChannel"]
                .as_str()
                .filter(|c| matches!(*c, "latest" | "stable"))
                .map(str::to_owned)
        })
        .next_back()
        .unwrap_or_else(|| "latest".into())
}

/// `satisfiesSemverRange`.
pub(crate) fn satisfies_range(raw: &str, range: &str) -> bool {
    let version = raw.trim().trim_start_matches('v');
    let core = version.split('-').next().unwrap_or("");
    let parse = |text: &str| -> Option<(u64, Option<u64>, Option<u64>)> {
        let mut parts = text.split('.');
        let major = parts.next()?.parse().ok()?;
        let minor = match parts.next() {
            Some(p) => Some(p.parse().ok()?),
            None => None,
        };
        let patch = match parts.next() {
            Some(p) => Some(p.parse().ok()?),
            None => None,
        };
        parts.next().is_none().then_some((major, minor, patch))
    };
    let Some((major, minor, patch)) = parse(core) else {
        return false;
    };
    let version = (major, minor.unwrap_or(0), patch.unwrap_or(0));
    let operator_space = regex::Regex::new(r"(\^|>=|>|<=|<|=)\s+(v?\d)").unwrap();
    range.split("||").any(|group| {
        let group = operator_space.replace_all(group.trim(), "$1$2");
        let comparators: Vec<&str> = group.split_whitespace().collect();
        !comparators.is_empty()
            && comparators.iter().all(|comparator| {
                let (operator, rest) = ["^", ">=", "<=", ">", "<", "="]
                    .iter()
                    .find_map(|op| comparator.strip_prefix(op).map(|rest| (*op, rest)))
                    .unwrap_or(("=", comparator));
                let Some((t_major, t_minor, t_patch)) = parse(rest.trim_start_matches('v')) else {
                    return false;
                };
                let target = (t_major, t_minor.unwrap_or(0), t_patch.unwrap_or(0));
                let compared = version.cmp(&target);
                use std::cmp::Ordering::*;
                match operator {
                    "^" => {
                        if compared == Less {
                            return false;
                        }
                        if target.0 > 0 || t_minor.is_none() {
                            return version.0 == target.0;
                        }
                        if target.1 > 0 || t_patch.is_none() {
                            return version.0 == 0 && version.1 == target.1;
                        }
                        version.0 == 0 && version.1 == 0 && version.2 == target.2
                    }
                    ">=" => compared != Less,
                    ">" => compared == Greater,
                    "<=" => compared != Greater,
                    "<" => compared == Less,
                    _ => compared == Equal,
                }
            })
    })
}

/// `resolveProviderCompatibility`.
fn resolve_compatibility(policies: &Value, driver: &str, version: Option<&str>) -> Option<Value> {
    let policy = policies.as_array()?.iter().find(|policy| {
        policy["driver"] == driver
            && satisfies_range(
                crate::server_runtime::SERVER_VERSION,
                policy["t3CodeRange"].as_str().unwrap_or(""),
            )
    })?;
    // Cursor build hashes, Google ACP release prefixes and Muse revisions are
    // stripped; semver prereleases stay unknown.
    let stable = version.map(|v| {
        let unprefixed = v.strip_prefix('v').unwrap_or(v);
        let (pattern, keep) = match driver {
            "cursor" => (r"^(\d{4}\.\d{2}\.\d{2})-[a-f0-9]+$", true),
            "antigravity" => (r"^agy_acp_server_(\d+\.\d+\.\d+)$", true),
            "muse" => (r"^(\d+\.\d+\.\d+)-R\d+(?:\.\d+)?$", true),
            _ => ("", false),
        };
        if keep {
            regex::Regex::new(pattern)
                .unwrap()
                .replace(unprefixed, "$1")
                .into_owned()
        } else {
            unprefixed.to_owned()
        }
    });
    let is_stable = |v: &str| {
        let parts: Vec<&str> = v.split('.').collect();
        parts.len() == 3
            && parts
                .iter()
                .all(|p| !p.is_empty() && p.bytes().all(|b| b.is_ascii_digit()))
    };
    let status = match stable.as_deref().filter(|v| is_stable(v)) {
        Some(v) => policy["ranges"]
            .as_array()
            .into_iter()
            .flatten()
            .find(|entry| satisfies_range(v, entry["range"].as_str().unwrap_or("")))
            .and_then(|entry| entry["status"].as_str())
            .unwrap_or("unknown")
            .to_owned(),
        None => "unknown".to_owned(),
    };
    let message = match status.as_str() {
        "broken" => {
            Some("This provider version is known to be incompatible with this T3 Code release.")
        }
        "unsupported" => {
            Some("This provider version is outside the supported range for this T3 Code release.")
        }
        "graceful" => {
            Some("This provider version has limited compatibility with this T3 Code release.")
        }
        _ => None,
    };
    let recommended_version = policy
        .get("recommendedVersion")
        .cloned()
        .unwrap_or(Value::Null);
    let recommended_range = policy
        .get("recommendedRange")
        .cloned()
        .unwrap_or(Value::Null);
    let recommendation = recommended_version
        .as_str()
        .or(recommended_range.as_str())
        .map(str::to_owned);
    let message = match (message, recommendation) {
        (Some(message), Some(rec)) => json!(format!("{message} Use {rec}.")),
        (Some(message), None) => json!(message),
        _ => Value::Null,
    };
    Some(json!({
        "status": status,
        "message": message,
        "recommendedVersion": recommended_version,
        "recommendedRange": recommended_range,
    }))
}

/// `ModelManifest.providerCompatibility`: the advisory for `version`, from
/// the manifest's policies or else the bundled manifest's.
pub(crate) fn provider_compatibility(
    manifest: &Value,
    driver: &str,
    version: &str,
) -> Option<Value> {
    resolve_compatibility(&manifest["compatibility"], driver, Some(version)).or_else(|| {
        resolve_compatibility(
            &crate::model_manifest::BUNDLED["compatibility"],
            driver,
            Some(version),
        )
    })
}

/// `brokenVersionDetail` (#39434): why a version the manifest marks broken
/// is refused; none for any other.
pub(crate) fn broken_version_detail(
    name: &str,
    version: &str,
    advisory: Option<&Value>,
) -> Option<String> {
    let advisory = advisory.filter(|a| a["status"] == "broken")?;
    let target = advisory["recommendedVersion"]
        .as_str()
        .or(advisory["recommendedRange"].as_str())
        .unwrap_or("a newer version");
    Some(format!(
        "{name} {version} does not work with this T3 Code release. Update {name} to {target}, then retry."
    ))
}

/// `applyProviderCompatibility` with the manifest's policies, falling back
/// to the bundled manifest's.
fn apply_compatibility(mut row: Value, manifest: &Value) -> Value {
    let object = row.as_object_mut().unwrap();
    object.shift_remove("compatibilityAdvisory");
    if object.get("enabled") != Some(&Value::Bool(true))
        || object.get("installed") != Some(&Value::Bool(true))
    {
        return row;
    }
    let driver = row["driver"].as_str().unwrap_or("").to_owned();
    let resolve = |version: Option<&str>| {
        resolve_compatibility(&manifest["compatibility"], &driver, version).or_else(|| {
            resolve_compatibility(
                &crate::model_manifest::BUNDLED["compatibility"],
                &driver,
                version,
            )
        })
    };
    let Some(mut advisory) = resolve(row["version"].as_str()) else {
        return row;
    };
    if let Some(latest) = row["versionAdvisory"]["latestVersion"].as_str() {
        if let Some(latest_advisory) = resolve(Some(latest)) {
            let object = advisory.as_object_mut().unwrap();
            let message = object.shift_remove("message").unwrap_or(Value::Null);
            object.insert(
                "latestVersionStatus".into(),
                latest_advisory["status"].clone(),
            );
            object.insert("message".into(), message);
        }
    }
    let (status, rest) = {
        let object = advisory.as_object().unwrap();
        (object["status"].clone(), object.clone())
    };
    let mut ordered = Map::new();
    ordered.insert("status".into(), status);
    for (key, value) in rest {
        if key != "status" {
            ordered.insert(key, value);
        }
    }
    row["compatibilityAdvisory"] = Value::Object(ordered);
    row
}

// ---------------------------------------------------------------------------
// Cache

fn read_cache(path: &Path) -> Option<Value> {
    let text = std::fs::read_to_string(path).ok()?;
    let text = text.trim();
    if text.is_empty() {
        return None;
    }
    let value: Value = serde_json::from_str(text).ok()?;
    (value["instanceId"].is_string() && value["driver"].is_string() && value["auth"].is_object())
        .then_some(value)
}

/// `writeProviderStatusCache`: the row as indented JSON, written atomically.
fn write_cache(path: &Path, row: &Value) {
    let mut row = row.clone();
    if let Some(object) = row.as_object_mut() {
        object.shift_remove("updateState");
    }
    let Ok(mut text) = serde_json::to_string_pretty(&row) else {
        return;
    };
    text.push('\n');
    if let Some(parent) = path.parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    let temporary = path.with_extension(format!("json.{}.tmp", std::process::id()));
    if std::fs::write(&temporary, text).is_ok() && std::fs::rename(&temporary, path).is_err() {
        let _ = std::fs::remove_file(&temporary);
    }
}

// ---------------------------------------------------------------------------
// Processes

pub(crate) fn now_iso() -> String {
    crate::auth_service::iso(crate::auth_service::now())
}

/// The server's environment, with the login shell's `PATH`, `__MISE_DIFF`
/// and `JAVA_HOME` as of now (a runtime refresh replaces them), and the
/// instance's own variables over it (`mergeProviderInstanceEnvironment`).
pub(crate) fn base_env(instance: &[(String, String)]) -> ProcessEnv {
    let mut env: ProcessEnv = std::env::vars().collect();
    for (name, value) in crate::login_shell::current().variables() {
        env.insert(name.to_owned(), value.to_string_lossy().into_owned());
    }
    for (name, value) in instance {
        env.insert(name.clone(), value.clone());
    }
    env
}

/// `providerProbeEnvironment`: a status check must not make mise download a
/// missing tool.
fn probe_env(env: &ProcessEnv) -> ProcessEnv {
    let mut env = env.clone();
    env.insert("MISE_OFFLINE".into(), "1".into());
    env
}

fn home_dir() -> PathBuf {
    std::env::var_os("HOME")
        .map(PathBuf::from)
        .unwrap_or_default()
}

/// `expandHomePath`: a leading `~` is the home directory.
pub(crate) fn expand_home(path: &str) -> String {
    if path == "~" {
        return home_dir().display().to_string();
    }
    match path.strip_prefix("~/") {
        Some(rest) => home_dir().join(rest).display().to_string(),
        None => path.to_owned(),
    }
}

/// `resolveProviderProbeTimeoutMs`: `T3CODE_<PROVIDER>_PROBE_TIMEOUT_MS`
/// may only extend a deadline.
fn probe_timeout(provider: &str, default_ms: u64, env: &ProcessEnv) -> Duration {
    let name = format!("T3CODE_{}_PROBE_TIMEOUT_MS", provider.to_uppercase());
    let ms = env
        .get(&name)
        .and_then(|v| v.trim().parse::<u64>().ok())
        .filter(|ms| *ms > default_ms && *ms <= 2_147_483_647)
        .unwrap_or(default_ms);
    Duration::from_millis(ms)
}

pub(crate) struct CommandResult {
    pub stdout: String,
    pub stderr: String,
    pub code: i32,
}

pub(crate) enum RunError {
    /// The executable does not exist (`isCommandMissingCause`).
    NotFound,
    /// It could not be started for another reason.
    Spawn,
    TimedOut,
}

fn command(
    program: &str,
    args: &[String],
    env: &ProcessEnv,
    cwd: &Path,
) -> tokio::process::Command {
    let mut command = tokio::process::Command::new(program);
    command
        .args(args)
        .env_clear()
        .envs(env)
        .current_dir(cwd)
        .kill_on_drop(true);
    #[cfg(unix)]
    command.process_group(0);
    command
}

fn spawn_error(error: &std::io::Error) -> RunError {
    if error.kind() == std::io::ErrorKind::NotFound {
        RunError::NotFound
    } else {
        RunError::Spawn
    }
}

/// `spawnAndCollect` under a deadline: the output and exit code, or why
/// there are none. A timed-out command is killed.
async fn run_collect(
    program: &str,
    args: &[String],
    env: &ProcessEnv,
    cwd: &Path,
    timeout: Duration,
) -> Result<CommandResult, RunError> {
    let mut command = command(program, args, env, cwd);
    command
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped());
    let child = command.spawn().map_err(|e| spawn_error(&e))?;
    match tokio::time::timeout(timeout, child.wait_with_output()).await {
        Ok(Ok(output)) => Ok(CommandResult {
            stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
            stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
            code: output.status.code().unwrap_or(-1),
        }),
        Ok(Err(_)) => Err(RunError::Spawn),
        Err(_) => Err(RunError::TimedOut),
    }
}

/// A provider CLI's binary: `binaryPath` (with `~` expanded) or `default`.
fn cli_binary(config: &Value, default: &str) -> String {
    expand_home(
        config["binaryPath"]
            .as_str()
            .map(str::trim)
            .filter(|b| !b.is_empty())
            .unwrap_or(default),
    )
}

/// One run of a provider CLI from the server's working directory, as the
/// reference's `spawnAndCollect` under `Effect.timeoutOption`.
async fn run_cli(
    binary: &str,
    args: &[&str],
    env: &ProcessEnv,
    timeout: Duration,
) -> Result<CommandResult, RunError> {
    let args: Vec<String> = args.iter().map(|a| a.to_string()).collect();
    let cwd = std::env::current_dir().unwrap_or_default();
    run_collect(binary, &args, env, &cwd, timeout).await
}

/// `parseCliVersionResult` with the generic parser: a failed run reports no
/// version.
fn cli_version(result: &CommandResult) -> Option<String> {
    if result.code != 0 {
        return None;
    }
    parse_generic_version(&format!("{}\n{}", result.stdout, result.stderr))
}

/// `buildSelectOptionDescriptor` from (value, label, description, isDefault).
fn select_descriptor(
    id: &str,
    label: &str,
    options: &[(String, String, Option<String>, bool)],
) -> Value {
    let options: Vec<Value> = options
        .iter()
        .map(|(value, label, description, default)| {
            let mut option = json!({"id": value, "label": label});
            if let Some(description) = description.as_ref().filter(|d| !d.is_empty()) {
                option["description"] = json!(description);
            }
            if *default {
                option["isDefault"] = json!(true);
            }
            option
        })
        .collect();
    let mut descriptor = json!({"id": id, "label": label, "type": "select", "options": options});
    if let Some(current) = options.iter().find(|o| o["isDefault"] == true) {
        descriptor["currentValue"] = current["id"].clone();
    }
    descriptor
}

/// `parseGenericCliVersion`.
fn parse_generic_version(output: &str) -> Option<String> {
    regex::Regex::new(r"\bv?(\d+\.\d+\.\d+)\b")
        .unwrap()
        .captures(output)
        .map(|c| c[1].to_owned())
}

/// `makeUnavailableUsageLimits`.
fn unavailable_usage(checked_at: &str, reason: &str, message: Option<&str>) -> Value {
    let mut unavailable = json!({"reason": reason});
    if let Some(message) = message.filter(|m| !m.is_empty()) {
        unavailable["message"] = json!(message);
    }
    json!({"checkedAt": checked_at, "windows": [], "unavailable": unavailable})
}

fn clamp_percent(value: f64) -> f64 {
    if value.is_finite() {
        value.clamp(0.0, 100.0)
    } else {
        0.0
    }
}

/// `makeUsageLimits`: windows sorted by kind, then id.
fn usage_limits(checked_at: &str, mut windows: Vec<Value>) -> Value {
    let rank = |w: &Value| match w["kind"].as_str() {
        Some("session") => 0,
        Some("weekly") => 1,
        Some("monthly") => 2,
        _ => 3,
    };
    windows.sort_by(|a, b| {
        rank(a)
            .cmp(&rank(b))
            .then_with(|| a["id"].as_str().cmp(&b["id"].as_str()))
    });
    json!({"checkedAt": checked_at, "windows": windows})
}

/// A number as JSON the way JavaScript prints it: integral values without a
/// fraction.
fn js_number(value: f64) -> Value {
    if value.fract() == 0.0 && value.abs() < 9.0e15 {
        json!(value as i64)
    } else {
        json!(value)
    }
}

/// `providerModelsFromSettings`: the built-in models with custom names
/// applied, then each custom model not already listed.
fn models_from_settings(
    built_in: Vec<Value>,
    custom: &Value,
    custom_capabilities: Value,
) -> Vec<Value> {
    let entries: Vec<(String, String, Value)> = custom
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|entry| {
            let slug = entry
                .as_str()
                .or_else(|| entry["slug"].as_str())
                .unwrap_or("")
                .trim()
                .to_owned();
            if slug.is_empty() {
                return None;
            }
            let name = entry["name"]
                .as_str()
                .map(str::trim)
                .filter(|n| !n.is_empty())
                .unwrap_or(&slug)
                .to_owned();
            Some((slug, name, entry["capabilities"].clone()))
        })
        .collect();
    if entries.is_empty() {
        return built_in;
    }
    let mut models: Vec<Value> = built_in
        .into_iter()
        .map(|mut model| {
            if let Some((_, name, _)) = entries
                .iter()
                .find(|(slug, name, _)| model["slug"] == slug.as_str() && name != slug)
            {
                model["name"] = json!(name);
                model.as_object_mut().unwrap().shift_remove("shortName");
            }
            model
        })
        .collect();
    for (slug, name, capabilities) in entries {
        if models.iter().any(|m| m["slug"] == slug.as_str()) {
            continue;
        }
        let capabilities = if capabilities.is_null() {
            custom_capabilities.clone()
        } else {
            capabilities
        };
        models.push(
            json!({"slug": slug, "name": name, "isCustom": true, "capabilities": capabilities}),
        );
    }
    models
}

pub(crate) const COMPACT_SLASH_COMMAND: &str = "compact";

fn compact_slash_command() -> Value {
    json!({"name": COMPACT_SLASH_COMMAND, "description": "Summarize the conversation and reduce context usage"})
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn workspace_snapshots_are_upserted_kept_and_expire() {
        let scan = |skills: Vec<Value>| FolderScan {
            skills,
            slash_commands: vec![json!({"name": "compact"})],
            slash_pending: None,
        };
        let mut row =
            json!({"status": "ready", "slashCommands": [{"name": "compact"}], "models": []});
        upsert_workspace_snapshot(
            &mut row,
            "/a",
            "2026-11-23T00:00:00.000Z",
            scan(vec![json!({"name": "s"})]),
        );
        upsert_workspace_snapshot(&mut row, "/b", "2026-11-23T00:00:00.000Z", scan(vec![]));
        upsert_workspace_snapshot(&mut row, "/a", "2026-11-23T00:01:00.000Z", scan(vec![]));
        let all = row["workspaceSnapshots"].as_array().unwrap();
        assert_eq!(
            all.iter()
                .map(|s| s["cwd"].as_str().unwrap())
                .collect::<Vec<_>>(),
            ["/b", "/a"]
        );
        assert_eq!(all[1]["slashCommands"], json!([{"name": "compact"}]));
        let at = |iso: &str| {
            let t =
                time::OffsetDateTime::parse(iso, &time::format_description::well_known::Rfc3339)
                    .unwrap();
            (t.unix_timestamp_nanos() / 1_000_000) as i64
        };
        assert!(workspace_snapshot_current(
            &all[1],
            at("2026-11-23T00:05:59.000Z")
        ));
        assert!(!workspace_snapshot_current(
            &all[1],
            at("2026-11-23T00:06:00.000Z")
        ));
        // A machine check keeps the folders, marking them for a models rescan when models changed.
        let next = json!({"enabled": true, "installed": true, "status": "ready", "auth": {"status": "authenticated"},
                          "models": [{"slug": "m"}]});
        let merged = merge_snapshot(&row, next);
        assert_eq!(merged["workspaceSnapshots"].as_array().unwrap().len(), 2);
        assert_eq!(merged["workspaceSnapshots"][0]["modelsPending"], true);
        assert!(!workspace_snapshot_current(
            &merged["workspaceSnapshots"][0],
            at("2026-11-23T00:01:00.000Z")
        ));
    }
    #[test]
    fn checks_are_off_until_the_binary_turns_them_on() {
        // A server a test builds must never run the host's real provider CLIs.
        let home = std::env::temp_dir().join(format!("t3-status-default-{}", std::process::id()));
        let status = ProviderStatus::new(&crate::config::ServerPaths::from_base(&home));
        assert!(status.is_disabled());
        status.enable();
        assert!(!status.is_disabled());
        status.disable();
        assert!(status.is_disabled());
    }

    #[test]
    fn ranges_match_the_reference() {
        assert!(satisfies_range("2.1.200", ">=2.1.111 <2.1.280"));
        assert!(!satisfies_range("2.1.280", ">=2.1.111 <2.1.280"));
        assert!(satisfies_range("0.1.10", ">=0.0.42"));
        assert!(satisfies_range("1.4.5", "=1.4.5"));
        assert!(satisfies_range("1.9.0", ">=1.5.1 <2"));
        assert!(!satisfies_range("2.0.0", ">=1.5.1 <2"));
        assert!(satisfies_range("1.2.3", "^1"));
        assert!(!satisfies_range("x", ">=1"));
    }

    #[test]
    fn claude_keeps_models_a_probe_drops() {
        let previous = json!({"models": [
            {"slug": "a", "isCustom": false}, {"slug": "b", "isCustom": false, "isLegacy": true},
            {"slug": "c", "isCustom": true}, {"slug": "d", "isCustom": false}]});
        let next = json!({"driver": "claudeAgent", "enabled": true, "installed": true, "status": "ready",
            "auth": {"status": "authenticated"}, "models": [{"slug": "a", "isCustom": false}],
            "updateRequiredModels": [{"slug": "d"}]});
        let merged = merge_snapshot(&previous, next);
        let slugs: Vec<_> = merged["models"]
            .as_array()
            .unwrap()
            .iter()
            .map(|m| m["slug"].clone())
            .collect();
        assert_eq!(slugs, vec![json!("a"), json!("b")]);
    }

    #[test]
    fn codex_drops_models_once_ready() {
        let previous = json!({"models": [{"slug": "old", "isCustom": false}]});
        let next = json!({"driver": "codex", "enabled": true, "installed": true, "status": "ready",
            "auth": {"status": "authenticated"}, "models": [{"slug": "new", "isCustom": false}]});
        assert_eq!(
            merge_snapshot(&previous, next)["models"],
            json!([{"slug": "new", "isCustom": false}])
        );
    }

    #[test]
    fn probe_timeouts_only_extend() {
        let mut env = ProcessEnv::new();
        env.insert("T3CODE_CLAUDE_PROBE_TIMEOUT_MS".into(), "100".into());
        assert_eq!(
            probe_timeout("claude", 20_000, &env),
            Duration::from_millis(20_000)
        );
        env.insert("T3CODE_CLAUDE_PROBE_TIMEOUT_MS".into(), "30000".into());
        assert_eq!(
            probe_timeout("claude", 20_000, &env),
            Duration::from_millis(30_000)
        );
    }

    #[test]
    fn compatibility_advisory_for_an_old_claude() {
        let row = json!({"driver": "claudeAgent", "enabled": true, "installed": true, "version": "2.1.200"});
        let row = apply_compatibility(row, &crate::model_manifest::BUNDLED);
        assert_eq!(
            row["compatibilityAdvisory"],
            json!({
            "status": "graceful",
            "message": "This provider version has limited compatibility with this T3 Code release. Use >=2.1.301.",
            "recommendedVersion": null, "recommendedRange": ">=2.1.301"})
        );
    }
}
