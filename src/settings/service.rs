//! Reads and writes settings.json and its private sidecars as one transaction.
use super::*;
#[cfg(feature = "native")]
use crate::config::ServerPaths;
use crate::persistence::{DocumentStore, WriteMode};
use serde_json::{json, Value};
use std::{
    io,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

pub struct Settings {
    settings_path: PathBuf,
    secrets_dir: PathBuf,
    history_dir: Option<PathBuf>,
    documents: Arc<dyn DocumentStore>,
    #[cfg(feature = "native")]
    pub(crate) resolver: secrets::Resolver,
    pub(crate) policy: policy::Policy,
    lock: Mutex<Option<LastWrite>>,
    defaults: Value,
    #[cfg(feature = "native")]
    pub(crate) cleanup_changed: tokio::sync::Notify,
    changes: tokio::sync::broadcast::Sender<(Value, Value)>,
}

struct LastWrite {
    settings: Value,
    /// Platform monotonic milliseconds at the write.
    at: u64,
    revision: Option<crate::persistence::Revision>,
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
impl Settings {
    /// The settings service over `documents`, which holds settings.json.
    #[cfg(feature = "native")]
    pub(crate) fn open_in(
        paths: &ServerPaths,
        documents: Arc<dyn DocumentStore>,
    ) -> anyhow::Result<Self> {
        let policy = policy::Policy::load(&*documents)?;
        Self::open(
            paths.settings.clone(),
            paths.secrets.clone(),
            Some(paths.state_dir.clone()),
            documents,
            policy,
        )
    }

    pub(crate) fn open_plane(
        dir: &Path,
        documents: Arc<dyn DocumentStore>,
        managed_policy: Option<Value>,
    ) -> anyhow::Result<Self> {
        let policy = managed_policy
            .map(policy::Policy::parse)
            .transpose()?
            .unwrap_or_default();
        Self::open(
            dir.join("settings.json"),
            dir.join("secrets"),
            None,
            documents,
            policy,
        )
    }

    fn open(
        settings_path: PathBuf,
        secrets_dir: PathBuf,
        history_dir: Option<PathBuf>,
        documents: Arc<dyn DocumentStore>,
        policy: policy::Policy,
    ) -> anyhow::Result<Self> {
        secrets::ensure_dir(&*documents, &secrets_dir)?;
        let defaults = schema::normalize(&policy.default_settings()).map_err(anyhow::Error::msg)?;
        let service = Self {
            #[cfg(feature = "native")]
            cleanup_changed: Default::default(),
            changes: tokio::sync::broadcast::channel(64).0,
            settings_path,
            secrets_dir,
            history_dir,
            documents,
            #[cfg(feature = "native")]
            resolver: Default::default(),
            policy,
            lock: Mutex::new(None),
            defaults,
        };
        service.load()?;
        Ok(service)
    }
    fn error(&self, operation: &str) -> Value {
        json!({"_tag":"ServerSettingsError","settingsPath":self.settings_path,"operation":operation})
    }
    fn io_failure(&self, err: &anyhow::Error, fallback: &str) -> Value {
        err.downcast_ref::<io::Error>()
            .and_then(|e| secrets::rpc_error(&self.settings_path, e))
            .unwrap_or_else(|| self.error(fallback))
    }
    fn save(&self, settings: &Value, raw: &Value) -> io::Result<()> {
        let normalized = schema::normalize_runtime(settings).map_err(io::Error::other)?;
        let stripped = strip(&normalized, Some(&self.defaults), &schema::defaults())
            .unwrap_or_else(|| json!({}));
        let mut bytes = Vec::new();
        crate::wire_json::pretty(
            &mut bytes,
            &schema::encode_disk(sparse_order(
                self.policy.keep_stored(&stripped, &normalized, raw),
                &normalized,
            )),
        )?;
        bytes.push(b'\n');
        self.documents
            .write(&self.settings_path, &bytes, WriteMode::ReplaceTarget)
    }
    fn load(&self) -> anyhow::Result<Loaded> {
        let input = match self.documents.read(&self.settings_path) {
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
                    &self.settings_path,
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
        mutations::migrate(&mut settings, &raw, self.history_dir.as_deref())?;
        let otherwise_changed =
            was_folded != settings["projectSettingsFolded"] || raw.get("providers").is_some();
        let settings = secrets::migrate(
            settings,
            prior_hosts,
            &raw,
            &*self.documents,
            &self.secrets_dir,
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
        let mut value = self.policy.effective(&secrets::materialize(
            &*self.documents,
            disk,
            &self.secrets_dir,
        )?);
        mutations::effective_selection(&mut value, |driver| {
            self.policy.provider_denial(driver).is_none()
        });
        schema::normalize_runtime(&value).map_err(anyhow::Error::msg)
    }
    /// The revisions of every document `internal` reads: the settings file
    /// and the secret files it may fill in. While they are unchanged,
    /// `internal` returns the same settings, so a poller can skip re-reading
    /// them.
    pub(crate) fn source_revisions(&self) -> Vec<(PathBuf, Option<crate::persistence::Revision>)> {
        let mut sources = vec![(
            self.settings_path.clone(),
            self.documents.revision(&self.settings_path),
        )];
        let mut secrets = self.documents.list(&self.secrets_dir).unwrap_or_default();
        secrets.sort();
        for path in secrets {
            let revision = self.documents.revision(&path);
            sources.push((path, revision));
        }
        sources
    }
    pub(crate) fn internal(&self) -> anyhow::Result<Value> {
        let mut guard = self.lock.lock().unwrap_or_else(|e| e.into_inner());
        // The upstream watcher debounces a writer's own filesystem events for 100 ms.
        if let Some(written) = guard.as_ref() {
            if crate::platform::monotonic_ms().saturating_sub(written.at) < 100
                && written.revision == self.documents.revision(&self.settings_path)
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
    #[cfg(feature = "native")]
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
            .persist(&*self.documents, &self.secrets_dir, |disk| {
                self.save(disk, &loaded.raw)
            })
            .map_err(|e| self.io_failure(&e.into(), "write-file"))?;
        let effective = self
            .effective(&prepared.disk)
            .map_err(|e| self.io_failure(&e, "read-secret"))?;
        #[cfg(feature = "native")]
        if crate::storage_cleanup::policy_changed(current, &prepared.disk) {
            self.cleanup_changed.notify_one();
        }
        *guard = Some(LastWrite {
            settings: prepared.disk,
            at: crate::platform::monotonic_ms(),
            revision: self.documents.revision(&self.settings_path),
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

#[cfg(test)]
mod source_revision_tests {
    use super::Settings;
    use crate::persistence::{DocumentStore, MemoryStore, WriteMode};
    use std::{path::Path, sync::Arc};

    #[test]
    fn source_revisions_change_with_the_settings_or_a_secret() {
        let documents = Arc::new(MemoryStore::new());
        let dir = Path::new("/plane");
        let settings = Settings::open_plane(dir, documents.clone(), None).unwrap();
        let first = settings.source_revisions();
        assert_eq!(settings.source_revisions(), first, "nothing changed");
        documents
            .write(&dir.join("settings.json"), b"{}", WriteMode::Replace)
            .unwrap();
        let second = settings.source_revisions();
        assert_ne!(second, first, "the settings file changed");
        documents
            .write(
                &dir.join("secrets").join("token.bin"),
                b"x",
                WriteMode::Replace,
            )
            .unwrap();
        assert_ne!(settings.source_revisions(), second, "a secret was written");
    }
}
