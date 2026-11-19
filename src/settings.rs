//! The shared settings file has three views: sparse, effective, and client-redacted.
//! Serialize the complete read/modify/write transaction, including sidecar secrets.
mod failure;
mod mutations;
pub(crate) mod policy;
pub(crate) mod schema;
pub(crate) mod secrets;

use crate::config::ServerPaths;
mod json_syntax;
pub(crate) use json_syntax::parse;
use serde_json::{json, Value};
use std::{
    fs,
    io::{self, Write},
    path::Path,
    sync::Mutex,
};

pub struct Settings {
    paths: ServerPaths,
    pub(crate) resolver: secrets::Resolver,
    pub(crate) policy: policy::Policy,
    lock: Mutex<Option<LastWrite>>,
    defaults: Value,
    changes: tokio::sync::broadcast::Sender<(Value, Value)>,
}

struct LastWrite {
    settings: Value,
    at: std::time::Instant,
    revision: Option<(std::time::SystemTime, u64)>,
}
fn revision(path: &Path) -> Option<(std::time::SystemTime, u64)> {
    let meta = path.metadata().ok()?;
    Some((meta.modified().ok()?, meta.len()))
}

struct Loaded {
    raw: Value,
    settings: Value,
    invalid: Option<String>,
}

fn parse_settings(raw: &Value, policy: &policy::Policy) -> Result<Value, String> {
    let mut input = policy.with_defaults(raw);
    if input["responseStreamingMode"] == "token" {
        input["responseStreamingMode"] = json!("paragraph");
    }
    if let Some(entries) = input
        .get_mut("projectSettingsOverrides")
        .and_then(Value::as_object_mut)
    {
        for p in entries.values_mut() {
            if p["responseStreamingMode"] == "token" {
                p["responseStreamingMode"] = json!("paragraph")
            }
        }
    }
    let canonical = schema::normalize(&input)?;
    for k in ["providers", "github", "bitbucket"] {
        if raw.get(k).is_some_and(|v| !v.is_object()) {
            return Err(format!("Expected object\n  at [\"{k}\"]"));
        }
    }
    Ok(canonical)
}
fn strip(current: &Value, default: Option<&Value>, built: &Value) -> Option<Value> {
    if default == Some(current) {
        return None;
    }
    if let (Some(fields), Some(defaults)) =
        (current.as_object(), default.and_then(Value::as_object))
    {
        let mut sparse = serde_json::Map::new();
        for (k, v) in fields {
            let d = defaults.get(k);
            let atomic = matches!(
                k.as_str(),
                "backgroundActivity"
                    | "automaticGitFetchInterval"
                    | "providerHealthRefreshInterval"
                    | "sourceControlWriterModelSelection"
                    | "reviewGuideModelSelection"
                    | "textGenerationModelSelection"
                    | "fallbackModelSelection"
                    | "pullRequestMergeMethod"
                    | "defaultThreadEnvMode"
            ) || built.get(k) == Some(&Value::Null);
            let value = if atomic {
                (Some(v) != d).then(|| v.clone())
            } else {
                strip(v, d, built)
            };
            if let Some(value) = value {
                sparse.insert(k.clone(), value);
            }
        }
        return (!sparse.is_empty()).then_some(sparse.into());
    }
    Some(current.clone())
}
// Reinserted explicit policy defaults must still encode in schema order.
fn sparse_order(sparse: Value, normalized: &Value) -> Value {
    match (sparse, normalized.as_object()) {
        (Value::Object(mut fields), Some(shape)) => Value::Object(
            shape
                .iter()
                .filter_map(|(key, v)| {
                    fields
                        .shift_remove(key)
                        .map(|s| (key.clone(), sparse_order(s, v)))
                })
                .collect(),
        ),
        (value, _) => value,
    }
}
fn save(path: &Path, value: &Value) -> io::Result<()> {
    let mut target = path.to_owned();
    for _ in 0..=40 {
        match fs::read_link(&target) {
            Ok(next) => {
                let parent = target.parent().unwrap_or(Path::new("."));
                target = fs::canonicalize(parent)?.join(next);
            }
            Err(e)
                if matches!(
                    e.kind(),
                    io::ErrorKind::InvalidInput | io::ErrorKind::NotFound
                ) =>
            {
                let parent = target.parent().unwrap_or(Path::new("."));
                fs::create_dir_all(parent).map_err(|e| {
                    failure::file(e, "write-file", "makeDirectory", "mkdir", parent)
                })?;
                let staging = tempfile::Builder::new()
                    .prefix("t3-write-")
                    .tempdir_in(parent)
                    .map_err(|e| {
                        failure::file(
                            e,
                            "write-file",
                            "makeTempDirectory",
                            "mkdtemp",
                            &parent.join("t3-write-"),
                        )
                    })?;
                let temp = staging.path().join("contents.tmp");
                let mut file = fs::File::create(&temp)
                    .map_err(|e| failure::file(e, "write-file", "writeFile", "open", &temp))?;
                crate::wire_json::pretty(&mut file, value)?;
                file.write_all(b"\n")?;
                drop(file);
                replace_settings_file(&temp, &target).map_err(|e| {
                    let step = failure::step(e, "rename", "rename", &temp, Some(&target));
                    failure::file(step, "write-file", "rename", "rename", &temp)
                })?;
                return Ok(());
            }
            Err(e) => return Err(e),
        }
    }
    Err(io::Error::other("Too many levels of symbolic links"))
}
#[cfg(not(windows))]
fn replace_settings_file(temp: &Path, target: &Path) -> io::Result<()> {
    fs::rename(temp, target)
}

#[cfg(windows)]
fn replace_settings_file(temp: &Path, target: &Path) -> io::Result<()> {
    use std::os::windows::fs::MetadataExt;
    let version = || match fs::metadata(target) {
        Ok(m) => Ok(Some((
            m.creation_time(),
            m.last_write_time(),
            m.file_size(),
            m.permissions().readonly(),
        ))),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e),
    };
    let initial = version();
    for retry in 0..=20 {
        match fs::rename(temp, target) {
            Ok(()) => return Ok(()),
            Err(e) => {
                let unchanged = initial.as_ref().is_ok_and(|v| {
                    v.is_none_or(|(_, _, _, ro)| !ro)
                        && version().is_ok_and(|current| &current == v)
                });
                if retry == 20 || !matches!(e.raw_os_error(), Some(5 | 32 | 33)) || !unchanged {
                    return Err(e);
                }
                std::thread::sleep(std::time::Duration::from_millis(100));
                if version().ok().as_ref() != initial.as_ref().ok() {
                    return Err(e);
                }
            }
        }
    }
    unreachable!()
}

impl Settings {
    pub(crate) fn open(paths: &ServerPaths) -> anyhow::Result<Self> {
        secrets::ensure_dir(&paths.secrets)?;
        let policy = policy::Policy::load()?;
        let defaults = schema::normalize(&policy.default_settings()).map_err(anyhow::Error::msg)?;
        let service = Self {
            changes: tokio::sync::broadcast::channel(64).0,
            paths: paths.clone(),
            resolver: Default::default(),
            policy,
            lock: Mutex::new(None),
            defaults,
        };
        service.load()?;
        Ok(service)
    }
    fn error(&self, operation: &str) -> Value {
        json!({"_tag":"ServerSettingsError","settingsPath":self.paths.settings,"operation":operation})
    }
    fn io_failure(&self, err: &anyhow::Error, fallback: &str) -> Value {
        err.downcast_ref::<io::Error>()
            .and_then(|e| secrets::rpc_error(&self.paths.settings, e))
            .unwrap_or_else(|| self.error(fallback))
    }
    fn save(&self, settings: &Value, raw: &Value) -> io::Result<()> {
        let normalized = schema::normalize_runtime(settings).map_err(io::Error::other)?;
        let stripped = strip(&normalized, Some(&self.defaults), &schema::defaults())
            .unwrap_or_else(|| json!({}));
        save(
            &self.paths.settings,
            &schema::encode_disk(sparse_order(
                self.policy.keep_stored(&stripped, &normalized, raw),
                &normalized,
            )),
        )
    }
    fn load(&self) -> anyhow::Result<Loaded> {
        let input = match fs::read(&self.paths.settings) {
            Ok(bytes) => json_syntax::parse_file(&bytes),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(json!({})),
            Err(e) => {
                let syscall = if e.kind() == io::ErrorKind::IsADirectory {
                    "read"
                } else {
                    "open"
                };
                return Err(failure::file(
                    e,
                    "read-file",
                    "readFile",
                    syscall,
                    &self.paths.settings,
                )
                .into());
            }
        };
        let decoded = input.and_then(|raw| Ok((parse_settings(&raw, &self.policy)?, raw)));
        let (mut settings, raw) = match decoded {
            Ok(fields) => fields,
            Err(message) => {
                let mut safe = self.defaults.clone();
                safe["continueThreadsAfterServerUpdate"] = json!(false);
                safe["usageHistoryRetentionDays"] = json!("unlimited");
                mutations::normalize(&mut safe);
                return Ok(Loaded {
                    raw: Value::Null,
                    settings: safe,
                    invalid: Some(message),
                });
            }
        };
        let was_folded = settings["projectSettingsFolded"].clone();
        let prior_hosts = settings["sourceControlHosts"].clone();
        mutations::migrate(&mut settings, &raw, &self.paths.state_dir)?;
        let otherwise_changed =
            was_folded != settings["projectSettingsFolded"] || raw.get("providers").is_some();
        let settings = secrets::migrate(
            settings,
            prior_hosts,
            &raw,
            &self.paths.secrets,
            otherwise_changed,
            |disk| self.save(disk, &raw),
        )?;
        Ok(Loaded {
            raw,
            settings,
            invalid: None,
        })
    }
    fn effective(&self, disk: &Value) -> anyhow::Result<Value> {
        let mut value = self
            .policy
            .effective(&secrets::materialize(disk, &self.paths.secrets)?);
        mutations::effective_selection(&mut value, |driver| {
            self.policy.provider_denial(driver).is_none()
        });
        schema::normalize_runtime(&value).map_err(anyhow::Error::msg)
    }
    pub(crate) fn internal(&self) -> anyhow::Result<Value> {
        let mut guard = self.lock.lock().unwrap_or_else(|e| e.into_inner());
        // The upstream watcher debounces a writer's own filesystem events for 100 ms.
        if let Some(written) = guard.as_ref() {
            if written.at.elapsed() < std::time::Duration::from_millis(100)
                && written.revision == revision(&self.paths.settings)
            {
                return self.effective(&written.settings);
            }
        }
        *guard = None;
        self.effective(&self.load()?.settings)
    }
    pub(crate) fn client(&self, references: bool) -> Result<Value, Value> {
        self.internal()
            .map(|v| secrets::redact(&v, references))
            .map_err(|e| self.io_failure(&e, "read-file"))
    }
    pub(crate) fn set_speech_key(&self, kind: &str, key: &Value) -> Result<Value, Value> {
        self.update(
            &json!({"patch":{"cloudSpeechCredentials":{kind:key}}}),
            true,
        )
    }
    pub(crate) fn subscribe(&self) -> tokio::sync::broadcast::Receiver<(Value, Value)> {
        self.changes.subscribe()
    }
    pub(crate) fn update(&self, payload: &Value, references: bool) -> Result<Value, Value> {
        let mut guard = self.lock.lock().unwrap_or_else(|e| e.into_inner());
        *guard = None;
        let loaded = self.load().map_err(|e| self.io_failure(&e, "read-file"))?;
        if let Some(message) = loaded.invalid {
            let mut error = self.error("read-file");
            error["cause"] = json!({"name":"SchemaError","message":message});
            return Err(error);
        }
        for (id, entry) in payload["patch"]["secretProviders"]
            .as_object()
            .into_iter()
            .flatten()
        {
            if entry.is_null() {
                continue;
            }
            let kind = entry["kind"].as_str().unwrap_or("");
            let problem = if kind != "one_password" {
                Some(format!("Unsupported secret provider kind: {kind}."))
            } else if !valid_account(&entry["config"]["account"]) {
                Some(format!("Invalid {kind} config for secret provider {id}."))
            } else {
                None
            };
            if let Some(message) = problem {
                let mut e = self.error("write-secret-provider");
                e["cause"] = json!({"name":"Error","message":message});
                return Err(e);
            }
        }
        let current = &loaded.settings;
        let next = mutations::apply(current, payload).map_err(|operation| {
            let mut e = self.error(&operation);
            if operation == "create-provider-instance" {
                e["providerInstanceId"] = payload["providerInstanceMutation"]["instanceId"].clone()
            }
            e
        })?;
        let next = secrets::restore_references(current, next);
        let mut next = schema::normalize_runtime(&next).map_err(|message| {
            json!({"_tag":"ServerSettingsError","settingsPath":"<memory>","operation":"normalize",
                   "cause":{"name":"SchemaError","message":message}})
        })?;
        mutations::normalize(&mut next);
        let next = self
            .policy
            .vet_write(current, &next, &payload["patch"])
            .map_err(|fields| {
                let mut e = self.error("write-managed-setting");
                if let Some(fields) = fields.as_object() {
                    e.as_object_mut().unwrap().extend(fields.clone());
                }
                e
            })?;
        let prepared = secrets::prepare(current, next);
        prepared
            .persist(&self.paths.secrets, |disk| self.save(disk, &loaded.raw))
            .map_err(|e| self.io_failure(&e.into(), "write-file"))?;
        let effective = self
            .effective(&prepared.disk)
            .map_err(|e| self.io_failure(&e, "read-secret"))?;
        *guard = Some(LastWrite {
            settings: prepared.disk,
            at: std::time::Instant::now(),
            revision: revision(&self.paths.settings),
        });
        let view = secrets::redact(&effective, references);
        if self.changes.receiver_count() > 0 {
            let other = secrets::redact(&effective, !references);
            let views = if references {
                (other, view.clone())
            } else {
                (view.clone(), other)
            };
            let _ = self.changes.send(views);
        }
        Ok(view)
    }
}
fn valid_account(value: &Value) -> bool {
    value.as_str().is_some_and(|s| {
        s.len() <= 253
            && s.as_bytes().first().is_some_and(u8::is_ascii_alphanumeric)
            && s.bytes()
                .all(|c| c.is_ascii_alphanumeric() || matches!(c, b'-' | b'_' | b'.'))
    })
}
