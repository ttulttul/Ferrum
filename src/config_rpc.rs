mod discovery;
use crate::server_config::{keybindings, providers, DEFAULTS};

use crate::{
    auth_service::{iso, now},
    AppState,
};
use serde_json::{json, Value};
use std::{
    env,
    path::Path,
    sync::Arc,
    time::{Duration, SystemTime},
};
use tokio::sync::broadcast;

pub(crate) fn keybindings_mutate(
    state: &AppState,
    method: &str,
    input: &Value,
) -> Result<Value, Value> {
    keybindings::mutate(
        state.documents.as_ref(),
        &state.paths.keybindings,
        method,
        input,
    )
}

fn observability(state: &AppState, settings: &Value) -> Value {
    fn var(name: &str) -> Option<String> {
        env::var(name)
            .ok()
            .map(|s| s.trim().to_owned())
            .filter(|s| !s.is_empty())
    }
    let mut output = json!({"logsDirectoryPath":state.paths.logs,"localTracingEnabled":true});
    let disabled = var("T3CODE_OTEL_SDK_DISABLED")
        .and_then(|v| match v.to_ascii_lowercase().as_str() {
            "yes" | "true" | "1" | "on" => Some(true),
            "no" | "false" | "0" | "off" => Some(false),
            _ => None,
        })
        .unwrap_or_else(|| {
            var("OTEL_SDK_DISABLED").is_some_and(|v| v.eq_ignore_ascii_case("true"))
        });
    for signal in ["Traces", "Metrics", "Logs"] {
        let upper = signal.to_uppercase();
        let prefix = format!("otlp{signal}");
        let url_key = format!("{prefix}Url");
        let vendor = format!("T3CODE_OTLP_{upper}_URL");
        let own = format!("OTEL_EXPORTER_OTLP_{upper}_ENDPOINT");
        let generic = "OTEL_EXPORTER_OTLP_ENDPOINT";
        let exporter = var(&format!("OTEL_{upper}_EXPORTER"))
            .unwrap_or_default()
            .to_lowercase();
        let exporters = exporter.split(',').map(str::trim).collect::<Vec<_>>();
        let endpoint = if disabled {
            None
        } else if let Some(url) = var(&vendor) {
            Some((url, Some(vendor)))
        } else if exporters.contains(&"none") && !exporters.contains(&"otlp") {
            None
        } else if let Some(url) = var(&own) {
            url::Url::parse(&url)
                .ok()
                .filter(|u| matches!(u.scheme(), "https" | "http"))
                .map(|u| (u.to_string(), Some(own)))
        } else if let Some(url) = var(generic) {
            url::Url::parse(&url)
                .ok()
                .filter(|u| matches!(u.scheme(), "http" | "https"))
                .map(|mut u| {
                    u.set_path(&format!(
                        "{}/v1/{}",
                        u.path().trim_end_matches('/'),
                        signal.to_lowercase()
                    ));
                    (u.to_string(), Some(generic.to_owned()))
                })
        } else {
            settings["observability"][&url_key]
                .as_str()
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(|s| (s.to_owned(), None))
        };
        let enabled = endpoint.is_some();
        if let Some((url, variable)) = endpoint {
            output[url_key] = json!(url);
            output[format!("{prefix}Source")] = json!(if variable.is_some() {
                "environment"
            } else {
                "settings"
            });
            if let Some(var) = variable {
                output[format!("{prefix}Variable")] = json!(var);
            }
        }
        output[format!("{prefix}Enabled")] = json!(enabled);
    }
    output
}

pub(crate) async fn snapshot(
    state: &Arc<AppState>,
    references: bool,
    options: &Value,
) -> Result<Value, Value> {
    let mut settings = state.settings.client(references)?;
    let project_rows = state.store.read().await.project_rows.clone();
    crate::rpc::fold_project_rows_into_settings(&mut settings, &project_rows);
    let hostname = hostname::get()
        .map(|s| s.to_string_lossy().into_owned())
        .unwrap_or_default();
    let (keybindings, issues) =
        keybindings::read(state.documents.as_ref(), &state.paths.keybindings);
    let ((editors, reveal), targets, direct) = tokio::join!(
        discovery::editors(),
        discovery::targets(&hostname),
        discovery::direct(state)
    );
    let environment = crate::environment_http::descriptor(axum::extract::State(state.clone()))
        .await
        .0;
    let mut config = json!({"environment":environment,"auth":state.auth.descriptor(),"cwd":env::current_dir().unwrap_or_else(|_|state.paths.base_dir.clone()),"hostname":hostname,
        "keybindingsConfigPath":state.paths.keybindings,"keybindings":keybindings,"issues":issues,"providers":providers::list(state,&settings,options),"availableEditors":editors,"remoteOpenTargets":targets,"directEndpoints":direct,
        "observability":observability(state,&settings),"settings":settings});
    if let Some(policy) = state.settings.policy.summary() {
        config["managedPolicy"] = policy.clone();
    }
    for key in [
        "shellResumeCompletionMarker",
        "shellThreadFamilies",
        "threadLinkMetadata",
        "shellOlderSettled",
    ] {
        config[key] = true.into();
    }
    if let Some(kind) = reveal {
        config["shellRevealInFileManager"] = true.into();
        config["shellRevealInFileManagerKind"] = json!(kind);
    }
    for key in [
        "threadResumeCompletionMarker",
        "threadSnapshotPagination",
        "threadFind",
        "threadFindProgressive",
        "secretProviderSettings",
        "secretReferences",
        "secretRequestReferences",
    ] {
        config[key] = true.into();
    }
    if !inside_git_work_tree(&state.paths.base_dir) {
        config["scratchWorkspaceRoot"] = json!(state.paths.base_dir.join("scratch"));
    }
    config["newProjectsRoot"] = json!(state.paths.base_dir.join("projects"));
    config["serverTime"] = json!(iso(now()));
    Ok(config)
}

/// Scratch folders inside a checkout would inherit its git status, so the
/// reference server offers them only outside one. It asks git, as
/// `GitVcsDriver.detectRepository` does: a missing git, a non-zero exit (such
/// as a repository git refuses for its ownership) or any answer but `true`
/// means the base directory is not in a work tree.
fn inside_git_work_tree(dir: &Path) -> bool {
    crate::login_shell::command("git")
        .arg("rev-parse")
        .arg("--is-inside-work-tree")
        .current_dir(dir)
        .stdin(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .output()
        .is_ok_and(|output| {
            output.status.success() && String::from_utf8_lossy(&output.stdout).trim() == "true"
        })
}

fn revision(path: &Path) -> Option<(SystemTime, u64)> {
    let m = path.metadata().ok()?;
    Some((m.modified().ok()?, m.len()))
}

pub(crate) struct Subscription {
    pub config: Value,
    changes: broadcast::Receiver<(Value, Value)>,
    settings_file: Option<(SystemTime, u64)>,
    bindings_file: Option<(SystemTime, u64)>,
    reload_at: Option<tokio::time::Instant>,
    timer: tokio::time::Interval,
    provider_event: Option<Value>,
    initial_events: std::collections::VecDeque<Value>,
    /// Provider status checks that published, and when the debounced
    /// `providerStatuses` event is due.
    provider_checks: broadcast::Receiver<()>,
    providers_due: Option<tokio::time::Instant>,
}

/// Events the reference server sends right after the snapshot to a client that
/// opted in. Its theme and usage-limit-source services replay their current
/// value first. This server publishes neither, so it answers only when that
/// value is known to be empty: no theme files and no configured sources.
fn initial_events(
    state: &AppState,
    config: &Value,
    options: &Value,
) -> std::collections::VecDeque<Value> {
    let mut events = std::collections::VecDeque::new();
    let no_sources = config["settings"]["usageLimitSources"]
        .as_object()
        .is_none_or(|sources| sources.is_empty());
    if options["usageLimitSources"] == true && no_sources {
        events.push_back(
            json!({"version":1,"type":"usageLimitSourcesUpdated","payload":{"sources":[]}}),
        );
    }
    let no_theme_files = std::fs::read_dir(&state.paths.themes).map_or(true, |entries| {
        !entries
            .flatten()
            .any(|entry| entry.file_name().to_string_lossy().ends_with(".json"))
    });
    if options["environmentThemes"] == true && no_theme_files {
        events.push_back(
            json!({"version":1,"type":"environmentThemesUpdated","payload":{"themes":[]}}),
        );
    }
    events
}
impl Subscription {
    pub async fn open(
        state: &Arc<AppState>,
        references: bool,
        options: &Value,
    ) -> Result<Self, Value> {
        let changes = state.settings.subscribe();
        let provider_checks = state.provider_status.subscribe();
        let settings_file = revision(&state.paths.settings);
        let bindings_file = revision(&state.paths.keybindings);
        let config = snapshot(state, references, options).await?;
        let initial_events = initial_events(state, &config, options);
        Ok(Self {
            initial_events,
            config,
            changes,
            settings_file,
            bindings_file,
            timer: tokio::time::interval(Duration::from_millis(50)),
            reload_at: None,
            provider_event: None,
            provider_checks,
            providers_due: None,
        })
    }

    /// The `providerStatuses` (or `providerStatusDeltas`) event for the
    /// provider rows `view` gives now, if they differ from the last sent.
    fn provider_statuses(
        &mut self,
        state: &AppState,
        view: &Value,
        options: &Value,
    ) -> Option<Value> {
        let providers = providers::list(state, view, options);
        if providers == self.config["providers"] {
            return None;
        }
        let mut rows = providers.clone();
        let deltas = options["providerStatusDeltas"] == true;
        if deltas {
            let previous = self.config["providers"]
                .as_array()
                .cloned()
                .unwrap_or_default();
            for row in rows.as_array_mut().unwrap() {
                if let Some(old) = previous
                    .iter()
                    .find(|v| v["instanceId"] == row["instanceId"])
                {
                    for key in ["models", "skills", "slashCommands"] {
                        if old[key] == row[key] {
                            row.as_object_mut().unwrap().shift_remove(key);
                        }
                    }
                }
            }
        }
        self.config["providers"] = providers;
        Some(
            json!({"version":1,"type":if deltas{"providerStatusDeltas"}else{"providerStatuses"},"payload":{"providers":rows}}),
        )
    }

    pub async fn next(
        &mut self,
        state: &AppState,
        references: bool,
        options: &Value,
    ) -> Result<Value, Value> {
        if let Some(event) = self.initial_events.pop_front() {
            return Ok(event);
        }
        loop {
            if let Some(event) = self.provider_event.take() {
                return Ok(event);
            }
            let view = tokio::select! {
                biased;
                received=self.changes.recv()=>match received{
                    Ok((legacy,refs))=>if references{refs}else{legacy},
                    Err(broadcast::error::RecvError::Lagged(_))=>state.settings.client(references)?,
                    Err(broadcast::error::RecvError::Closed)=>return Err(json!({"_tag":"ServerSettingsError","settingsPath":state.paths.settings,"operation":"read-file"})),
                },
                received=self.provider_checks.recv()=>{
                    if !matches!(received, Err(broadcast::error::RecvError::Closed)) {
                        // `PROVIDER_STATUS_DEBOUNCE_MS`.
                        self.providers_due = Some(tokio::time::Instant::now() + Duration::from_millis(200));
                    }
                    continue;
                }
                _=self.timer.tick()=>{
                    if self.providers_due.is_some_and(|at| tokio::time::Instant::now() >= at) {
                        self.providers_due = None;
                        let view = self.config["settings"].clone();
                        if let Some(event) = self.provider_statuses(state, &view, options) {
                            return Ok(event);
                        }
                    }
                    let keybindings_file=revision(&state.paths.keybindings);
                    if keybindings_file!=self.bindings_file{
                        self.bindings_file=keybindings_file;
                        let (keys,issues)=keybindings::read(state.documents.as_ref(), &state.paths.keybindings);
                        if self.config["keybindings"]!=keys || self.config["issues"]!=issues{
                            self.config["keybindings"]=keys.clone();self.config["issues"]=issues.clone();
                            return Ok(json!({"version":1,"type":"keybindingsUpdated","payload":{"keybindings":keys,"issues":issues}}));
                        }
                    }
                    let settings_file=revision(&state.paths.settings);
                    if settings_file!=self.settings_file{
                        self.settings_file=settings_file;
                        let debounce=Duration::from_millis(100);
                        let age=settings_file.and_then(|(mtime,_)|mtime.elapsed().ok()).unwrap_or_default();
                        self.reload_at=Some(tokio::time::Instant::now()+debounce.saturating_sub(age));
                    }
                    if self.reload_at.is_none_or(|at|tokio::time::Instant::now()<at){continue;}
                    self.reload_at=None;
                    state.settings.client(references)?
                }
            };
            if view["providerInstances"] != self.config["settings"]["providerInstances"] {
                self.provider_event = self.provider_statuses(state, &view, options);
            }
            self.config["settings"] = view.clone();
            return Ok(json!({"version":1,"type":"settingsUpdated","payload":{"settings":view}}));
        }
    }
}
