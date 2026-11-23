//! Codex's status check, as the reference's `checkCodexProviderStatus`: one
//! `codex app-server` session that initializes and reads the account, then
//! the skills, models and config (and, for a ChatGPT login on OpenAI's own
//! route, the rate limits) under one deadline. After a failure,
//! `codex --version` still reports the installed version.
use super::{
    clamp_percent, compact_slash_command, expand_home, js_number, models_from_settings, now_iso,
    probe_env, probe_timeout, run_collect, unavailable_usage, usage_limits, Draft, ProcessEnv,
};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::sync::oneshot;

/// `CODEX_AUTH_PROBE_TIMEOUT_MS`: the handshake and every read.
const PROBE_TIMEOUT_MS: u64 = 20_000;
/// `DEFAULT_TIMEOUT_MS`: the `--version` fallback.
const VERSION_TIMEOUT_MS: u64 = 4_000;
const DISCOVERY_MARGIN: Duration = Duration::from_millis(250);
const CONFIG_TIMEOUT: Duration = Duration::from_millis(3_000);
const CATALOG_REFRESH_TIMEOUT: Duration = Duration::from_millis(3_000);
const CATALOG_REFRESH_POLL: Duration = Duration::from_millis(100);
const CLIENT_NAME: &str = "T3 Code";
const PREFERRED_DEFAULT_MODELS: [&str; 3] = ["gpt-6-astra", "gpt-5.6-sol", "gpt-5.6-terra"];

/// Why the session failed, with the reference client's error messages.
enum SessionError {
    Spawn,
    Exited { code: Option<i32>, stderr: String },
    Request { code: i64, message: String },
}

impl SessionError {
    fn message(&self) -> String {
        match self {
            SessionError::Spawn => "Failed to spawn Codex App Server process".into(),
            SessionError::Exited { code, stderr } => {
                let base = match code {
                    Some(code) => format!("Codex App Server process exited with code {code}"),
                    None => "Codex App Server process exited".into(),
                };
                if stderr.is_empty() {
                    base
                } else {
                    format!("{base}: {stderr}")
                }
            }
            SessionError::Request { message, .. } => message.clone(),
        }
    }
}

type Pending = Arc<Mutex<HashMap<i64, oneshot::Sender<Result<Value, (i64, String)>>>>>;

/// A JSON-RPC session with `codex app-server`.
struct Session {
    child: tokio::process::Child,
    stdin: tokio::process::ChildStdin,
    pending: Pending,
    next_id: i64,
    closed: tokio::sync::watch::Receiver<bool>,
    stderr: Arc<Mutex<String>>,
}

impl Session {
    fn start(
        binary: &str,
        args: &[String],
        env: &ProcessEnv,
        cwd: &Path,
    ) -> Result<Self, SessionError> {
        let mut command = super::command(binary, args, env, cwd);
        command
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped());
        let mut child = command.spawn().map_err(|_| SessionError::Spawn)?;
        let stdin = child.stdin.take().ok_or(SessionError::Spawn)?;
        let stdout = child.stdout.take().ok_or(SessionError::Spawn)?;
        let stderr_pipe = child.stderr.take();
        let pending: Pending = Arc::default();
        let stderr = Arc::new(Mutex::new(String::new()));
        let (closed_tx, closed) = tokio::sync::watch::channel(false);
        if let Some(pipe) = stderr_pipe {
            let stderr = stderr.clone();
            tokio::spawn(async move {
                let mut lines = BufReader::new(pipe).lines();
                while let Ok(Some(line)) = lines.next_line().await {
                    let mut text = stderr.lock().unwrap_or_else(|e| e.into_inner());
                    if text.len() < 8192 {
                        if !text.is_empty() {
                            text.push('\n');
                        }
                        text.push_str(&line);
                    }
                }
            });
        }
        let reader_pending = pending.clone();
        tokio::spawn(async move {
            let mut lines = BufReader::new(stdout).lines();
            while let Ok(Some(line)) = lines.next_line().await {
                let Ok(message) = serde_json::from_str::<Value>(&line) else {
                    continue;
                };
                if message.get("method").is_some() {
                    continue;
                }
                let Some(id) = message["id"].as_i64() else {
                    continue;
                };
                let Some(sender) = reader_pending
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .remove(&id)
                else {
                    continue;
                };
                let reply = match message.get("error") {
                    Some(error) => Err((
                        error["code"].as_i64().unwrap_or(0),
                        error["message"].as_str().unwrap_or("").to_owned(),
                    )),
                    None => Ok(message["result"].clone()),
                };
                let _ = sender.send(reply);
            }
            let _ = closed_tx.send(true);
        });
        Ok(Self {
            child,
            stdin,
            pending,
            next_id: 1,
            closed,
            stderr,
        })
    }

    async fn exited(&mut self) -> SessionError {
        let status = tokio::time::timeout(Duration::from_millis(500), self.child.wait())
            .await
            .ok()
            .and_then(Result::ok);
        // Let the stderr reader catch up with what the CLI wrote last.
        tokio::time::sleep(Duration::from_millis(20)).await;
        let stderr = self
            .stderr
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .trim()
            .to_owned();
        SessionError::Exited {
            code: status.and_then(|s| s.code()),
            stderr,
        }
    }

    /// Send a request; the reply is awaited separately so several can run
    /// at once.
    async fn send(
        &mut self,
        method: &str,
        params: Value,
    ) -> Result<oneshot::Receiver<Result<Value, (i64, String)>>, SessionError> {
        let id = self.next_id;
        self.next_id += 1;
        let (tx, rx) = oneshot::channel();
        self.pending
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(id, tx);
        let line = json!({"id": id, "method": method, "params": params}).to_string() + "\n";
        if self.stdin.write_all(line.as_bytes()).await.is_err() {
            return Err(self.exited().await);
        }
        Ok(rx)
    }

    async fn notify(&mut self, method: &str) {
        let line = json!({"method": method}).to_string() + "\n";
        let _ = self.stdin.write_all(line.as_bytes()).await;
    }

    async fn wait(
        &mut self,
        reply: oneshot::Receiver<Result<Value, (i64, String)>>,
    ) -> Result<Value, SessionError> {
        let mut closed = self.closed.clone();
        tokio::select! {
            result = reply => match result {
                Ok(Ok(value)) => Ok(value),
                Ok(Err((code, message))) => Err(SessionError::Request { code, message }),
                Err(_) => Err(self.exited().await),
            },
            _ = async { let _ = closed.wait_for(|c| *c).await; } => Err(self.exited().await),
        }
    }

    async fn request(&mut self, method: &str, params: Value) -> Result<Value, SessionError> {
        let reply = self.send(method, params).await?;
        self.wait(reply).await
    }

    async fn stop(mut self) {
        let _ = self.child.start_kill();
        let _ = self.child.wait().await;
    }
}

/// What the session found (`CodexAppServerProviderSnapshot`).
#[derive(Default)]
struct Snapshot {
    account: Value,
    endpoint: Option<String>,
    native_usage: bool,
    rate_limits: Option<Result<Value, String>>,
    version: Option<String>,
    models: Option<Vec<Value>>,
    service_tier: Option<String>,
    daybreak_enabled: Option<bool>,
    cyber_unavailable: bool,
    skills: Vec<Value>,
}

fn reasoning_label(effort: &str) -> String {
    match effort {
        "none" => "None",
        "minimal" => "Minimal",
        "low" => "Low",
        "medium" => "Medium",
        "high" => "High",
        "xhigh" => "Extra High",
        "max" => "Max",
        "ultra" => "Ultra",
        other => other,
    }
    .to_owned()
}

fn model_family(slug: &str) -> &str {
    slug.strip_prefix("openai.")
        .filter(|s| s.starts_with("gpt-"))
        .unwrap_or(slug)
}

/// `formatCodexModelName`.
fn format_name(name: &str) -> String {
    let mut out = String::new();
    let rest = if name.len() >= 3 && name[..3].eq_ignore_ascii_case("gpt") {
        out.push_str("GPT");
        &name[3..]
    } else {
        name
    };
    let mut chars = rest.chars().peekable();
    while let Some(c) = chars.next() {
        out.push(c);
        if c == '-' {
            if let Some(next) = chars.peek().copied().filter(char::is_ascii_lowercase) {
                out.push(next.to_ascii_uppercase());
                chars.next();
            }
        }
    }
    out
}

/// `mapCodexModelCapabilities`.
fn model_capabilities(model: &Value) -> Value {
    let slug = model["model"].as_str().unwrap_or("");
    let default_effort = if model_family(slug) == "gpt-6-astra" {
        Some("medium".to_owned())
    } else {
        model["defaultReasoningEffort"].as_str().map(str::to_owned)
    };
    let reasoning: Vec<Value> = model["supportedReasoningEfforts"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|e| e["reasoningEffort"].as_str())
        .map(|effort| {
            let mut option = json!({"id": effort, "label": reasoning_label(effort)});
            if default_effort.as_deref() == Some(effort) {
                option["isDefault"] = json!(true);
            }
            option
        })
        .collect();
    let default_reasoning = reasoning
        .iter()
        .find(|o| o["isDefault"] == true)
        .map(|o| o["id"].clone());
    let tiers: Vec<Value> = match model["serviceTiers"].as_array().filter(|t| !t.is_empty()) {
        Some(tiers) => tiers.clone(),
        None => model["additionalSpeedTiers"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(Value::as_str)
            .map(|id| json!({"id": id, "name": if id == "fast" { "Fast" } else { id }, "description": ""}))
            .collect(),
    };
    let catalog_default = tiers
        .iter()
        .any(|t| t["id"] == model["defaultServiceTier"])
        .then(|| model["defaultServiceTier"].as_str().map(str::to_owned))
        .flatten();
    let default_tier = catalog_default.unwrap_or_else(|| "default".into());
    let mut descriptors = vec![];
    if !reasoning.is_empty() {
        let mut descriptor = json!({"id": "reasoningEffort", "label": "Reasoning", "type": "select", "options": reasoning});
        if let Some(value) = default_reasoning {
            descriptor["currentValue"] = value;
        }
        descriptors.push(descriptor);
    }
    if !tiers.is_empty() {
        let mut options = vec![{
            let mut o = json!({"id": "default", "label": "Standard"});
            if default_tier == "default" {
                o["isDefault"] = json!(true);
            }
            o
        }];
        for tier in &tiers {
            let id = tier["id"].as_str().unwrap_or("");
            let description = if id == "ultrafast" {
                Some("Even faster, more expensive".to_owned())
            } else {
                tier["description"]
                    .as_str()
                    .filter(|d| !d.is_empty())
                    .map(str::to_owned)
            };
            let mut o = json!({"id": id, "label": tier["name"]});
            if let Some(d) = description {
                o["description"] = json!(d);
            }
            if default_tier == id {
                o["isDefault"] = json!(true);
            }
            options.push(o);
        }
        descriptors.push(
            json!({"id": "serviceTier", "label": "Service Tier", "type": "select",
            "options": options, "currentValue": default_tier}),
        );
    }
    let programs: Vec<String> = model["availableAccessPrograms"]["cyber"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .map(str::to_owned)
        .collect();
    if !programs.is_empty() {
        let has_standard = programs.iter().any(|p| p == "standard");
        let default = if has_standard {
            "standard".to_owned()
        } else {
            programs[0].clone()
        };
        let listed: Vec<String> = if has_standard {
            programs.clone()
        } else {
            std::iter::once("standard".to_owned())
                .chain(programs.iter().cloned())
                .collect()
        };
        let options: Vec<Value> = listed
            .iter()
            .map(|p| {
                let label = match p.as_str() {
                    "standard" => "Standard",
                    "daybreakBlue" => "Daybreak Blue",
                    "daybreakRed" => "Daybreak Red",
                    other => other,
                };
                let mut o = json!({"id": p, "label": label});
                if *p == default {
                    o["isDefault"] = json!(true);
                }
                o
            })
            .collect();
        descriptors.push(
            json!({"id": "cyberAccessProgram", "label": "Cyber Access", "type": "select",
            "options": options, "currentValue": default}),
        );
    }
    json!({"optionDescriptors": descriptors})
}

fn parse_models(response: &Value) -> Vec<Value> {
    response["data"]
        .as_array()
        .into_iter()
        .flatten()
        .map(|model| {
            let mut row = json!({"slug": model["model"], "name": format_name(model["displayName"].as_str().unwrap_or("")),
                "isCustom": false});
            if model["isDefault"] == true {
                row["isDefault"] = json!(true);
            }
            row["capabilities"] = model_capabilities(model);
            row
        })
        .collect()
}

/// `applyPreferredCodexDefaultModel`.
fn preferred_default(models: Vec<Value>) -> Vec<Value> {
    let preferred = PREFERRED_DEFAULT_MODELS.iter().find_map(|slug| {
        models
            .iter()
            .find(|m| {
                m["isCustom"] != true && model_family(m["slug"].as_str().unwrap_or("")) == *slug
            })
            .map(|m| m["slug"].clone())
    });
    let Some(preferred) = preferred else {
        return models;
    };
    models
        .into_iter()
        .map(|mut m| {
            if m["slug"] == preferred {
                m["isDefault"] = json!(true);
            } else if m["isDefault"] == true {
                m.as_object_mut().unwrap().shift_remove("isDefault");
            }
            m
        })
        .collect()
}

fn with_select_default(
    models: Vec<Value>,
    id: &str,
    pick: impl Fn(&[Value]) -> Option<String>,
) -> Vec<Value> {
    models
        .into_iter()
        .map(|mut model| {
            for descriptor in model["capabilities"]["optionDescriptors"]
                .as_array_mut()
                .into_iter()
                .flatten()
            {
                if descriptor["id"] != id || descriptor["type"] != "select" {
                    continue;
                }
                let options = descriptor["options"]
                    .as_array()
                    .cloned()
                    .unwrap_or_default();
                let Some(value) = pick(&options) else {
                    continue;
                };
                descriptor["currentValue"] = json!(value);
                descriptor["options"] = options
                    .into_iter()
                    .map(|mut o| {
                        o.as_object_mut().unwrap().shift_remove("isDefault");
                        if o["id"] == value.as_str() {
                            o["isDefault"] = json!(true);
                        }
                        o
                    })
                    .collect();
            }
            model
        })
        .collect()
}

/// `applyConfiguredCodexCyberAccess`.
fn cyber_access(models: Vec<Value>, snapshot: &Snapshot) -> Vec<Value> {
    let offered: Vec<Value> = models
        .into_iter()
        .map(|mut model| {
            let custom = model["isCustom"] == true;
            if let Some(descriptors) = model["capabilities"]["optionDescriptors"].as_array_mut() {
                let mut kept = vec![];
                for mut descriptor in descriptors.drain(..) {
                    if descriptor["id"] != "cyberAccessProgram" || descriptor["type"] != "select" {
                        kept.push(descriptor);
                        continue;
                    }
                    if custom || snapshot.cyber_unavailable {
                        continue;
                    }
                    if snapshot.daybreak_enabled.is_none() {
                        descriptor
                            .as_object_mut()
                            .unwrap()
                            .shift_remove("currentValue");
                        for option in descriptor["options"].as_array_mut().into_iter().flatten() {
                            option.as_object_mut().unwrap().shift_remove("isDefault");
                        }
                    }
                    kept.push(descriptor);
                }
                *descriptors = kept;
            }
            model
        })
        .collect();
    if snapshot.daybreak_enabled != Some(true) {
        return offered;
    }
    with_select_default(offered, "cyberAccessProgram", |options| {
        ["daybreakBlue", "daybreakRed"]
            .iter()
            .find(|p| options.iter().any(|o| o["id"] == **p))
            .map(|p| p.to_string())
    })
}

/// `appendCustomCodexModels`: a bare custom slug borrows the first
/// built-in's capabilities.
fn append_custom(models: Vec<Value>, custom: &Value) -> Vec<Value> {
    let fallback = models
        .iter()
        .find(|m| !m["capabilities"].is_null())
        .map(|m| m["capabilities"].clone())
        .unwrap_or(Value::Null);
    models_from_settings(models, custom, fallback)
}

/// `parseCodexSkillsListResponse`.
fn parse_skills(response: &Value, cwd: &str) -> Vec<Value> {
    let data = response["data"].as_array().cloned().unwrap_or_default();
    let skills: Vec<Value> = match data.iter().find(|e| e["cwd"] == cwd) {
        Some(entry) => entry["skills"].as_array().cloned().unwrap_or_default(),
        None => data
            .iter()
            .flat_map(|e| e["skills"].as_array().cloned().unwrap_or_default())
            .collect(),
    };
    skills
        .iter()
        .map(|skill| {
            let mut row =
                json!({"name": skill["name"], "path": skill["path"], "enabled": skill["enabled"]});
            let text = |v: &Value| v.as_str().filter(|s| !s.is_empty()).map(str::to_owned);
            if let Some(d) = text(&skill["description"]) {
                row["description"] = json!(d);
            }
            if let Some(s) = text(&skill["scope"]) {
                row["scope"] = json!(s);
            }
            if let Some(d) = text(&skill["interface"]["displayName"]) {
                row["displayName"] = json!(d);
            }
            let short = skill["shortDescription"]
                .as_str()
                .or(skill["interface"]["shortDescription"].as_str())
                .filter(|s| !s.is_empty());
            if let Some(s) = short {
                row["shortDescription"] = json!(s);
            }
            row
        })
        .collect()
}

/// `codexRouteBaseUrl`: `Some(None)` is OpenAI's default route, `None` a
/// route that cannot be read.
fn route_base_url(config: &Value) -> Option<Option<String>> {
    if !config.is_object() {
        return None;
    }
    let provider = config["model_provider"].as_str().unwrap_or("openai");
    if provider == "openai" {
        return Some(
            config["openai_base_url"]
                .as_str()
                .filter(|s| !s.is_empty())
                .map(str::to_owned),
        );
    }
    let entry = config["model_providers"].get(provider)?;
    Some(entry["base_url"].as_str().map(str::to_owned))
}

fn routes_to_native_openai(config: &Value) -> bool {
    match route_base_url(config) {
        None => false,
        Some(None) => true,
        Some(Some(url)) => url::Url::parse(&url).is_ok_and(|u| {
            u.scheme() == "https" && matches!(u.host_str(), Some("api.openai.com" | "chatgpt.com"))
        }),
    }
}

fn custom_endpoint(config: &Value) -> Option<String> {
    let url = url::Url::parse(&route_base_url(config)??).ok()?;
    (matches!(url.scheme(), "http" | "https") && !routes_to_native_openai(config))
        .then(|| url.origin().ascii_serialization())
}

fn plan_label(plan: Option<&str>) -> Option<&'static str> {
    Some(match plan? {
        "free" => "ChatGPT Free Subscription",
        "go" => "ChatGPT Go Subscription",
        "plus" => "ChatGPT Plus Subscription",
        "pro" => "ChatGPT Pro 200 Subscription",
        "prolite" => "ChatGPT Pro 100 Subscription",
        "promax" => "ChatGPT Pro 500 Subscription",
        "self_serve_business_prolite" => "ChatGPT Business Premium Subscription",
        "team" | "self_serve_business_usage_based" | "business" => "ChatGPT Business Subscription",
        "ent26" | "enterprise_cbp_automation" | "enterprise_cbp_usage_based" | "enterprise" => {
            "ChatGPT Enterprise Subscription"
        }
        "edu" | "edu_plus" | "edu_pro" => "ChatGPT Edu Subscription",
        "unknown" => "ChatGPT Subscription",
        _ => return None,
    })
}

fn iso_from_epoch(value: &Value) -> Option<String> {
    let seconds = value.as_f64().filter(|v| v.is_finite() && *v > 0.0)?;
    let at = time::OffsetDateTime::from_unix_timestamp_nanos((seconds * 1e9) as i128).ok()?;
    Some(crate::auth_service::iso(
        (at.unix_timestamp_nanos() / 1_000_000) as i64,
    ))
}

/// `codexRateLimitsToLimits`.
fn rate_limits_to_limits(read: &Value, checked_at: &str) -> Value {
    let snapshot = read["rateLimitsByLimitId"]["codex"]
        .as_object()
        .map(|_| &read["rateLimitsByLimitId"]["codex"])
        .unwrap_or(&read["rateLimits"]);
    let mut windows = vec![];
    let limit_id = snapshot["limitId"].as_str().filter(|s| !s.is_empty());
    if limit_id.is_none_or(|id| id == "codex") {
        let monthly_plan = matches!(snapshot["planType"].as_str(), Some("free" | "go"));
        for (id, fallback) in [
            ("primary", if monthly_plan { 43200 } else { 300 }),
            ("secondary", 10080),
        ] {
            let window = &snapshot[id];
            let Some(used) = window["usedPercent"].as_f64().filter(|v| v.is_finite()) else {
                continue;
            };
            let mins = window["windowDurationMins"]
                .as_f64()
                .unwrap_or(fallback as f64);
            let kind = if mins >= 43200.0 {
                "monthly"
            } else if mins >= 10080.0 {
                "weekly"
            } else {
                "session"
            };
            let label = match kind {
                "session" => "Session",
                "weekly" => "Weekly",
                _ => "Monthly",
            };
            let mut row = json!({"id": id, "kind": kind, "label": label,
                "usedPercent": js_number(clamp_percent(used)), "windowDurationMins": js_number(mins)});
            if let Some(at) = iso_from_epoch(&window["resetsAt"]) {
                row["resetsAt"] = json!(at);
            }
            windows.push(row);
        }
        let individual = &snapshot["individualLimit"];
        if let Some(remaining) = individual["remainingPercent"]
            .as_f64()
            .filter(|v| v.is_finite())
        {
            let mut row = json!({"id": "individual", "kind": "monthly", "label": "Monthly credit limit",
                "usedPercent": js_number(clamp_percent(100.0 - remaining))});
            if let Some(at) = iso_from_epoch(&individual["resetsAt"]) {
                row["resetsAt"] = json!(at);
            }
            windows.push(row);
        }
    }
    let mut limits = usage_limits(checked_at, windows);
    let credits = &read["rateLimitResetCredits"];
    if credits.is_object() {
        let next = credits["credits"]
            .as_array()
            .into_iter()
            .flatten()
            .filter(|c| c["status"] == "available")
            .filter_map(|c| c["expiresAt"].as_f64())
            .fold(None::<f64>, |min, v| Some(min.map_or(v, |m| m.min(v))));
        let mut summary =
            json!({"availableCount": credits["availableCount"].as_i64().unwrap_or(0).max(0)});
        if let Some(at) = next.and_then(|v| iso_from_epoch(&json!(v))) {
            summary["nextExpiresAt"] = json!(at);
        }
        limits["resetCredits"] = summary;
    }
    limits
}

/// Wait for `fut`, but give up at `deadline` (and after `limit`, if any).
async fn by_deadline<T>(
    fut: impl std::future::Future<Output = T>,
    deadline: tokio::time::Instant,
    limit: Option<Duration>,
) -> Option<T> {
    let now = tokio::time::Instant::now();
    let mut end = deadline;
    if let Some(limit) = limit {
        end = end.min(now + limit);
    }
    if end <= now {
        return None;
    }
    tokio::time::timeout_at(end, fut).await.ok()
}

async fn list_models(
    session: &Arc<tokio::sync::Mutex<Session>>,
) -> Result<Vec<Value>, SessionError> {
    let mut models = vec![];
    let mut cursor: Option<String> = None;
    loop {
        let params = match &cursor {
            Some(c) => json!({"cursor": c}),
            None => json!({}),
        };
        let reply = session.lock().await.send("model/list", params).await?;
        let response = wait_reply(session, reply).await?;
        models.extend(parse_models(&response));
        cursor = response["nextCursor"]
            .as_str()
            .filter(|c| !c.is_empty())
            .map(str::to_owned);
        if cursor.is_none() {
            return Ok(models);
        }
    }
}

async fn wait_reply(
    session: &Arc<tokio::sync::Mutex<Session>>,
    reply: oneshot::Receiver<Result<Value, (i64, String)>>,
) -> Result<Value, SessionError> {
    let closed = session.lock().await.closed.clone();
    let mut closed = closed;
    tokio::select! {
        result = reply => match result {
            Ok(Ok(value)) => Ok(value),
            Ok(Err((code, message))) => Err(SessionError::Request { code, message }),
            Err(_) => Err(session.lock().await.exited().await),
        },
        _ = async { let _ = closed.wait_for(|c| *c).await; } => Err(session.lock().await.exited().await),
    }
}

fn models_cache_fetched_at(codex_home: &str) -> Option<i64> {
    let text = std::fs::read_to_string(Path::new(codex_home).join("models_cache.json")).ok()?;
    let value: Value = serde_json::from_str(&text).ok()?;
    let at = time::OffsetDateTime::parse(
        value["fetched_at"].as_str()?,
        &time::format_description::well_known::Rfc3339,
    )
    .ok()?;
    Some((at.unix_timestamp_nanos() / 1_000_000) as i64)
}

/// `probeCodexAppServerProvider`.
async fn probe(
    binary: &str,
    args: &[String],
    env: &ProcessEnv,
    cwd: &Path,
    timeout: Duration,
) -> Result<Snapshot, SessionError> {
    let spawned_at = tokio::time::Instant::now();
    let spawned_at_ms = crate::platform::now_ms();
    let mut session = Session::start(binary, args, env, cwd)?;
    let initialize = session
        .request(
            "initialize",
            json!({"clientInfo": {"name": CLIENT_NAME, "title": CLIENT_NAME, "version": crate::server_runtime::SERVER_VERSION},
                   "capabilities": {"experimentalApi": true}}),
        )
        .await;
    let initialize = match initialize {
        Ok(v) => v,
        Err(e) => {
            session.stop().await;
            return Err(e);
        }
    };
    session.notify("initialized").await;
    let version = initialize["userAgent"].as_str().and_then(|ua| {
        regex::Regex::new(r"/([^\s]+)")
            .unwrap()
            .captures(ua)
            .map(|c| c[1].to_owned())
    });
    let account = match session.request("account/read", json!({})).await {
        Ok(v) => v,
        Err(e) => {
            session.stop().await;
            return Err(e);
        }
    };
    if account["account"].is_null() && account["requiresOpenaiAuth"] == true {
        session.stop().await;
        return Ok(Snapshot {
            account,
            version,
            models: Some(vec![]),
            ..Default::default()
        });
    }
    let deadline = spawned_at + timeout - DISCOVERY_MARGIN;
    let cwd_text = cwd.display().to_string();
    let codex_home = initialize["codexHome"].as_str().unwrap_or("").to_owned();
    let chatgpt = account["account"]["type"] == "chatgpt";
    let session = Arc::new(tokio::sync::Mutex::new(session));
    let skills = {
        let session = session.clone();
        let cwd_text = cwd_text.clone();
        async move {
            let reply = session
                .lock()
                .await
                .send("skills/list", json!({"cwds": [cwd_text]}))
                .await
                .ok()?;
            by_deadline(wait_reply(&session, reply), deadline, None)
                .await?
                .ok()
        }
    };
    let models = {
        let session = session.clone();
        async move {
            let first = by_deadline(list_models(&session), deadline, None)
                .await?
                .ok()?;
            if !chatgpt {
                return Some(first);
            }
            let refreshed = by_deadline(
                async {
                    loop {
                        if models_cache_fetched_at(&codex_home)
                            .is_some_and(|at| at >= spawned_at_ms)
                        {
                            return;
                        }
                        tokio::time::sleep(CATALOG_REFRESH_POLL).await;
                    }
                },
                deadline,
                Some(CATALOG_REFRESH_TIMEOUT),
            )
            .await;
            if refreshed.is_none() {
                return Some(first);
            }
            Some(
                by_deadline(list_models(&session), deadline, None)
                    .await
                    .and_then(Result::ok)
                    .unwrap_or(first),
            )
        }
    };
    let config = {
        let session = session.clone();
        let account = account.clone();
        async move {
            let config = async {
                let reply = session
                    .lock()
                    .await
                    .send("config/read", json!({"includeLayers": false}))
                    .await
                    .ok()?;
                by_deadline(wait_reply(&session, reply), deadline, Some(CONFIG_TIMEOUT))
                    .await?
                    .ok()
            }
            .await;
            let native = config.as_ref().is_some_and(|c| {
                account["requiresOpenaiAuth"] == true
                    && account["account"]["type"] != "apiKey"
                    && routes_to_native_openai(&c["config"])
            });
            let rate_limits = if native {
                let read = async {
                    let reply = session
                        .lock()
                        .await
                        .send("account/rateLimits/read", Value::Null)
                        .await;
                    match reply {
                        Ok(reply) => by_deadline(wait_reply(&session, reply), deadline, None).await,
                        Err(e) => Some(Err(e)),
                    }
                }
                .await;
                Some(match read {
                    None => Err("Codex did not answer the usage request.".to_owned()),
                    Some(Ok(value)) => Ok(value),
                    Some(Err(SessionError::Request { code, .. })) => {
                        Err(format!("Codex could not read usage (JSON-RPC {code})."))
                    }
                    Some(Err(SessionError::Spawn)) => {
                        Err("Codex could not be started to read usage.".into())
                    }
                    Some(Err(SessionError::Exited { .. })) => {
                        Err("Codex exited before it could report usage.".into())
                    }
                })
            } else {
                None
            };
            (config.map(|c| c["config"].clone()), native, rate_limits)
        }
    };
    let (skills, models, (config, native_usage, rate_limits)) =
        tokio::join!(skills, models, config);
    if let Ok(session) = Arc::try_unwrap(session) {
        session.into_inner().stop().await;
    }
    let cyber = config.as_ref().map(|c| {
        (
            c["daybreak"].clone(),
            c["features"]["api_key_cyber_access_programs"].clone(),
        )
    });
    Ok(Snapshot {
        endpoint: config.as_ref().and_then(custom_endpoint),
        native_usage,
        rate_limits,
        version,
        models,
        service_tier: config
            .as_ref()
            .and_then(|c| c["service_tier"].as_str().map(str::to_owned)),
        daybreak_enabled: cyber.as_ref().map(|(d, _)| *d == true),
        cyber_unavailable: account["account"]["type"] == "apiKey"
            && cyber.as_ref().is_none_or(|(_, f)| *f == false),
        skills: skills
            .map(|s| parse_skills(&s, &cwd_text))
            .unwrap_or_default(),
        account,
    })
}

/// `checkCodexProviderStatus` for an enabled instance.
/// How T3 Code starts `codex app-server` for an instance
/// (`codexProcessEnvironment` and `codexAppServerArgs`).
struct Launch {
    env: ProcessEnv,
    binary_setting: String,
    binary: String,
    args: Vec<String>,
}

fn launch(config: &Value, env: &ProcessEnv) -> Launch {
    let mut env = probe_env(env);
    env.entry("CODEX_INTERNAL_ORIGINATOR_OVERRIDE".into())
        .or_insert_with(|| CLIENT_NAME.into());
    let home = config["homePath"]
        .as_str()
        .map(str::trim)
        .filter(|h| !h.is_empty())
        .map(expand_home);
    if let Some(home) = &home {
        env.insert("CODEX_HOME".into(), home.clone());
    }
    let binary_setting = config["binaryPath"]
        .as_str()
        .map(str::trim)
        .filter(|b| !b.is_empty())
        .unwrap_or("codex")
        .to_owned();
    let binary = expand_home(&binary_setting);
    let launch_args = env
        .get("T3CODE_CODEX_LAUNCH_ARGS")
        .map(|v| v.trim().to_owned())
        .filter(|v| !v.is_empty())
        .unwrap_or_else(|| {
            config["launchArgs"]
                .as_str()
                .unwrap_or("")
                .trim()
                .to_owned()
        });
    let mut args: Vec<String> = vec![
        "app-server".into(),
        "-c".into(),
        "thread_unload_delay_secs=0".into(),
    ];
    args.extend(crate::provider_state::split_args(&launch_args));
    Launch {
        env,
        binary_setting,
        binary,
        args,
    }
}

/// The directory holding the instance's `auth.json`, which redemptions are
/// serialized on (`effectiveHomePath ?? sharedHomePath`).
pub(super) fn account_key(config: &Value, env: &ProcessEnv) -> String {
    let launch = launch(config, env);
    launch
        .env
        .get("CODEX_HOME")
        .map(|home| home.trim().to_owned())
        .filter(|home| !home.is_empty())
        .unwrap_or_else(|| super::home_dir().join(".codex").display().to_string())
}

/// `consumeResetCredit`: one `account/rateLimitResetCredit/consume` request on
/// a fresh app-server, given 20 seconds. A failure carries the reference's
/// cause when it is one this server can name (an outcome Codex's schema
/// does not allow).
pub(super) async fn consume_reset_credit(
    config: &Value,
    env: &ProcessEnv,
    idempotency_key: &str,
) -> Result<String, Option<Value>> {
    let Launch {
        env, binary, args, ..
    } = launch(config, env);
    let cwd = std::env::current_dir().unwrap_or_default();
    const METHOD: &str = "account/rateLimitResetCredit/consume";
    let request = async {
        let mut session = Session::start(&binary, &args, &env, &cwd).map_err(|_| None)?;
        let outcome = async {
            session
                .request(
                    "initialize",
                    json!({"clientInfo": {"name": CLIENT_NAME, "title": CLIENT_NAME, "version": crate::server_runtime::SERVER_VERSION},
                           "capabilities": {"experimentalApi": true}}),
                )
                .await
                .map_err(|_| None)?;
            session.notify("initialized").await;
            let response = session
                .request(METHOD, json!({"idempotencyKey": idempotency_key}))
                .await
                .map_err(|_| None)?;
            match response["outcome"].as_str() {
                Some(outcome @ ("reset" | "nothingToReset" | "noCredit" | "alreadyRedeemed")) => {
                    Ok(outcome.to_owned())
                }
                Some(_) => Err(Some(json!({
                    "name": "CodexAppServerRequestError",
                    "message": format!("Invalid payload for method '{METHOD}' during 'decode-payload'"),
                    "cause": {"name": "SchemaError", "message": "Expected V2ConsumeAccountRateLimitResetCreditResponse__ConsumeAccountRateLimitResetCreditOutcome\n  at [\"outcome\"]"},
                }))),
                None => Err(None),
            }
        }
        .await;
        session.stop().await;
        outcome
    };
    tokio::time::timeout(Duration::from_secs(20), request)
        .await
        .unwrap_or(Err(None))
}

pub(super) async fn check(config: &Value, env: &ProcessEnv, manifest: &Value) -> Draft {
    let Launch {
        env,
        binary_setting,
        binary,
        args,
    } = launch(config, env);
    let home = config["homePath"]
        .as_str()
        .map(str::trim)
        .filter(|h| !h.is_empty())
        .map(expand_home);
    let checked_at = now_iso();
    let custom = &config["customModels"];
    let empty_models = append_custom(vec![], custom);
    let cwd = std::env::current_dir().unwrap_or_default();
    let timeout = probe_timeout("codex", PROBE_TIMEOUT_MS, &env);
    let result = tokio::time::timeout(timeout, probe(&binary, &args, &env, &cwd, timeout)).await;
    let failed = |installed: bool, version: Option<String>, message: String| Draft {
        installed,
        version,
        status: "error",
        auth: json!({"status": "unknown"}),
        checked_at: checked_at.clone(),
        message: Some(message),
        models: empty_models.clone(),
        ..Default::default()
    };
    let snapshot = match result {
        Err(_) => {
            return failed(
                true,
                None,
                "Timed out while checking Codex app-server provider status.".into(),
            )
        }
        Ok(Err(SessionError::Spawn)) => {
            let hint = if binary_setting == "codex" {
                " Installing ChatGPT or Codex desktop may not add codex to PATH."
            } else {
                " Make sure the configured executable exists and can be run."
            };
            return failed(
                false,
                None,
                format!(
                    "Could not start Codex CLI (`{binary_setting}`). Check Settings → Providers → Codex → Binary path on the server.{hint}"
                ),
            );
        }
        Ok(Err(error)) => {
            let version_timeout = probe_timeout("codex", VERSION_TIMEOUT_MS, &env);
            let version = match run_collect(
                &binary,
                &["--version".into()],
                &env,
                &cwd,
                version_timeout,
            )
            .await
            {
                Ok(output) if output.code == 0 => {
                    regex::Regex::new(r"(?:^|\s)v?(\d+\.\d+\.\d+(?:-[0-9A-Za-z.-]+)?)(?:\s|$)")
                        .unwrap()
                        .captures(&output.stdout)
                        .map(|c| c[1].to_owned())
                }
                _ => None,
            };
            return failed(
                true,
                version,
                format!(
                    "Codex app-server provider probe failed: {}.",
                    error.message()
                ),
            );
        }
        Ok(Ok(snapshot)) => snapshot,
    };
    let account = &snapshot.account["account"];
    let usage_plan = match &snapshot.rate_limits {
        Some(Ok(read)) => read["rateLimits"]["planType"].as_str(),
        _ => None,
    };
    let label = match account["type"].as_str() {
        _ if account.is_null() => None,
        Some("apiKey") => Some("OpenAI API Key"),
        Some("amazonBedrock") => Some("Amazon Bedrock"),
        Some("chatgpt") => {
            let usage = if usage_plan == Some("unknown") {
                None
            } else {
                plan_label(usage_plan)
            };
            usage.or_else(|| plan_label(account["planType"].as_str()))
        }
        _ => None,
    };
    let workspace = match &snapshot.rate_limits {
        Some(Ok(read)) => read["accountId"]
            .as_str()
            .map(str::trim)
            .filter(|s| !s.is_empty()),
        _ => None,
    };
    let mut auth = json!({"status": if account.is_null() { "unknown" } else { "authenticated" }});
    if let Some(kind) = account["type"].as_str() {
        auth["type"] = json!(kind);
    }
    if let Some(label) = label {
        auth["label"] = json!(label);
    }
    if account["type"] == "chatgpt" {
        if let Some(email) = account["email"].as_str() {
            auth["email"] = json!(email);
        }
    }
    if let Some(workspace) = workspace {
        auth["workspaceId"] = json!(workspace);
    }
    let (status, auth, account_message) = if !account.is_null() {
        ("ready", auth, None)
    } else if snapshot.account["requiresOpenaiAuth"] == true {
        let message = match &home {
            Some(home) => format!(
                "Codex CLI is not authenticated. Run `codex login` with CODEX_HOME set to {} and try again.",
                json!(home)
            ),
            None => "Codex CLI is not authenticated. Run `codex login` and try again.".into(),
        };
        ("error", json!({"status": "unauthenticated"}), Some(message))
    } else {
        ("ready", auth, None)
    };
    let usage = if !snapshot.native_usage && account_message.is_none() {
        unavailable_usage(&checked_at, "unsupported", None)
    } else {
        match &snapshot.rate_limits {
            Some(Ok(read)) => rate_limits_to_limits(read, &checked_at),
            Some(Err(failure)) => unavailable_usage(&checked_at, "probeFailed", Some(failure)),
            None => unavailable_usage(&checked_at, "probeFailed", account_message.as_deref()),
        }
    };
    let mut models = preferred_default(append_custom(
        snapshot.models.clone().unwrap_or_default(),
        custom,
    ));
    if let Some(tier) = &snapshot.service_tier {
        models = with_select_default(models, "serviceTier", |options| {
            options
                .iter()
                .find(|o| o["id"] == tier.as_str() || (tier == "fast" && o["id"] == "priority"))
                .and_then(|o| o["id"].as_str().map(str::to_owned))
        });
    }
    let models = cyber_access(models, &snapshot);
    let mut auth = auth;
    if let Some(endpoint) = &snapshot.endpoint {
        auth["endpoint"] = json!(endpoint);
    }
    let message = account_message.or_else(|| {
        snapshot
            .models
            .is_none()
            .then(|| "Codex did not return its model catalog in time.".to_owned())
    });
    let mut draft = Draft {
        installed: true,
        version: snapshot.version.clone(),
        status: if snapshot.models.is_none() {
            "warning"
        } else {
            status
        },
        auth,
        checked_at: checked_at.clone(),
        message,
        models,
        slash_commands: vec![
            compact_slash_command(),
            json!({"name": "goal", "description": "Set a goal Codex keeps working toward until it is done",
                   "input": {"hint": "Objective, or pause, resume, clear"}}),
            json!({"name": "feedback", "description": "Send this thread and Codex logs to OpenAI",
                   "input": {"hint": "Describe the issue (optional)"}}),
        ],
        skills: snapshot.skills,
        usage_limits: Some(usage),
        ..Default::default()
    };
    apply_catalog(&mut draft, manifest);
    draft
}

/// `applyCodexModelCatalog`: catalog legacy flags and default, and the
/// models the installed CLI is too old to list.
fn apply_catalog(draft: &mut Draft, manifest: &Value) {
    let catalog = manifest["providers"]["codex"]["models"]
        .as_array()
        .or_else(|| crate::model_manifest::BUNDLED["providers"]["codex"]["models"].as_array())
        .cloned()
        .unwrap_or_default();
    let default_chat = manifest["providers"]["codex"]["defaults"]["chat"]
        .as_str()
        .map(str::to_owned);
    let entry_for = |slug: &str| {
        catalog
            .iter()
            .find(|e| e["slug"] == slug)
            .or_else(|| catalog.iter().find(|e| e["slug"] == model_family(slug)))
    };
    for model in draft.models.iter_mut() {
        if model["isCustom"] == true {
            continue;
        }
        let slug = model["slug"].as_str().unwrap_or("").to_owned();
        let legacy = entry_for(&slug).is_some_and(|e| e["status"] == "legacy");
        if legacy {
            model["isLegacy"] = json!(true);
        } else {
            model.as_object_mut().unwrap().shift_remove("isLegacy");
        }
    }
    if let Some(requested) = default_chat {
        let slug = draft
            .models
            .iter()
            .find(|m| m["slug"] == requested.as_str())
            .or_else(|| {
                draft.models.iter().find(|m| {
                    m["isCustom"] != true
                        && model_family(m["slug"].as_str().unwrap_or(""))
                            == model_family(&requested)
                })
            })
            .map(|m| m["slug"].clone());
        if let Some(slug) = slug {
            let previous = draft
                .models
                .iter()
                .find(|m| m["isDefault"] == true && m["slug"] != slug)
                .cloned();
            if let Some(previous) = previous {
                let moved: Vec<Value> = previous["aliases"].as_array().cloned().unwrap_or_default();
                for model in draft.models.iter_mut() {
                    if model["slug"] == previous["slug"] {
                        let object = model.as_object_mut().unwrap();
                        object.shift_remove("isDefault");
                        object.shift_remove("aliases");
                    } else if model["slug"] == slug {
                        let mut aliases: Vec<Value> =
                            model["aliases"].as_array().cloned().unwrap_or_default();
                        for alias in &moved {
                            if !aliases.contains(alias) {
                                aliases.push(alias.clone());
                            }
                        }
                        model["isDefault"] = json!(true);
                        if !aliases.is_empty() {
                            model["aliases"] = json!(aliases);
                        }
                    }
                }
            }
        }
    }
    let version = draft
        .version
        .as_deref()
        .map(|v| v.trim_start_matches('v').to_owned());
    let Some(version) = version.filter(|v| crate::model_manifest::parse_semver(v).is_some()) else {
        draft.update_required_models = vec![];
        return;
    };
    let discovered: Vec<String> = draft
        .models
        .iter()
        .map(|m| model_family(m["slug"].as_str().unwrap_or("")).to_owned())
        .collect();
    draft.update_required_models = catalog
        .iter()
        .filter_map(|entry| {
            let slug = entry["slug"].as_str()?;
            if entry["status"] != "current" || discovered.iter().any(|d| d == model_family(slug)) {
                return None;
            }
            let min = entry["adapter"]["codex"]["minVersion"].as_str()?.trim();
            if min.is_empty() || crate::model_manifest::compare_semver(&version, min).is_ge() {
                return None;
            }
            let mut row = json!({"slug": slug, "name": entry["name"]});
            if let Some(badge) = entry.get("badge") {
                row["badge"] = badge.clone();
            }
            row["minVersion"] = json!(min);
            Some(row)
        })
        .collect();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn model_names_follow_the_reference() {
        assert_eq!(format_name("gpt-6-astra"), "GPT-6-Astra");
        assert_eq!(format_name("o4-mini"), "o4-Mini");
    }

    #[test]
    fn capabilities_from_model_list() {
        let caps = model_capabilities(&json!({"model": "gpt-x", "defaultReasoningEffort": "high",
            "supportedReasoningEfforts": [{"reasoningEffort": "low"}, {"reasoningEffort": "high"}]}));
        assert_eq!(
            caps,
            json!({"optionDescriptors": [{"id": "reasoningEffort", "label": "Reasoning", "type": "select",
            "options": [{"id": "low", "label": "Low"}, {"id": "high", "label": "High", "isDefault": true}],
            "currentValue": "high"}]})
        );
    }

    #[test]
    fn native_route_detection() {
        assert!(routes_to_native_openai(&json!({})));
        assert!(!routes_to_native_openai(
            &json!({"openai_base_url": "http://localhost:1234/v1"})
        ));
        assert_eq!(
            custom_endpoint(&json!({"openai_base_url": "http://localhost:1234/v1"})),
            Some("http://localhost:1234".into())
        );
    }
}
