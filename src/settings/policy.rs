use super::schema;
#[cfg(feature = "native")]
use crate::persistence::DocumentStore;
use anyhow::{anyhow, ensure, Context, Result};
use serde::Deserialize;
use serde_json::{json, Map, Value};
#[cfg(feature = "native")]
use std::path::Path;
#[cfg(feature = "native")]
mod platform;
mod providers;
#[cfg(feature = "native")]
mod reachability;

#[cfg(feature = "native")]
#[derive(Debug)]
pub struct ManagedPolicyFileError(anyhow::Error);
#[cfg(feature = "native")]
impl std::fmt::Display for ManagedPolicyFileError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:#}", self.0)
    }
}
#[cfg(feature = "native")]
impl std::error::Error for ManagedPolicyFileError {}

#[derive(Default, Debug)]
pub(crate) struct Policy {
    summary: Option<Value>,
    /// `ManagedPolicy.loadedPolicy` in the reference: where the policy came
    /// from and which document it was. `None` when no policy applies.
    #[cfg(feature = "native")]
    loaded: Option<Value>,
    defaults: Vec<Entry>,
    locks: Vec<Entry>,
    providers: providers::Rules,
}
#[derive(Debug)]
struct Entry {
    path: String,
    parts: Vec<String>,
    value: Value,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct Document {
    schema_version: u32,
    organization: String,
    support_url: Option<String>,
    #[serde(default)]
    settings: Map<String, Value>,
    limits: Option<Value>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Setting {
    value: Value,
    locked: bool,
}
const MODES: [&str; 4] = [
    "approval-required",
    "auto-accept-edits",
    "auto",
    "full-access",
];
pub(super) fn dict(v: &Value) -> impl Iterator<Item = (&String, &Value)> {
    v.as_object().into_iter().flatten()
}
pub(super) fn list(v: &Value) -> impl Iterator<Item = &Value> {
    v.as_array().into_iter().flatten()
}
fn object(v: &Value) -> Result<&Map<String, Value>> {
    v.as_object().context("expected an object")
}
fn limited(v: &Value, keys: &[&str]) -> Result<()> {
    for k in object(v)?.keys() {
        ensure!(keys.contains(&k.as_str()), "unexpected policy key {k}");
    }
    Ok(())
}
fn get<'a>(v: &'a Value, path: &[String]) -> Option<&'a Value> {
    path.iter().try_fold(v, |v, k| v.get(k))
}
fn set(v: &mut Value, path: &[String], new: Option<&Value>) {
    let Some((head, rest)) = path.split_first() else {
        return;
    };
    if !v.is_object() {
        *v = json!({});
    }
    if rest.is_empty() {
        if let Some(new) = new {
            v[head] = new.clone();
        } else {
            v.as_object_mut().unwrap().shift_remove(head);
        }
        return;
    }
    set(&mut v[head], rest, new);
}
fn merge(base: &mut Value, overlay: &Value) {
    if let (Some(base), Some(over)) = (base.as_object_mut(), overlay.as_object()) {
        for (k, v) in over {
            if let Some(prev) = base.get_mut(k) {
                merge(prev, v)
            } else {
                base.insert(k.clone(), v.clone());
            }
        }
    } else {
        *base = overlay.clone();
    }
}
fn matches_lock(v: &Value, lock: &Value) -> bool {
    if v.is_object() && lock.is_object() {
        return dict(v).all(|(key, v)| lock.get(key).is_some_and(|l| matches_lock(v, l)));
    }
    v == lock
}
fn adds_keys(existing: &Value, value: &Value) -> bool {
    existing.is_object()
        && value.is_object()
        && dict(value).any(|(k, v)| existing.get(k).is_none_or(|old| adds_keys(old, v)))
}
fn paths(tree: &Value, entry: &Entry) -> Vec<Vec<String>> {
    let mut targets = vec![entry.parts.clone()];
    for (id, overrides) in dict(&tree["projectSettingsOverrides"]) {
        if overrides.get(&entry.parts[0]).is_some() {
            let mut p = vec!["projectSettingsOverrides".into(), id.to_owned()];
            p.extend(entry.parts.clone());
            targets.push(p);
        }
    }
    targets
}
fn above(mode: &Value, ceiling: &Value) -> bool {
    let rank = |v: &Value| MODES.iter().position(|m| v == *m);
    rank(ceiling).is_some_and(|limit| rank(mode).is_some_and(|r| r > limit))
}
/// The document as the reference's `Schema.encode(ManagedPolicyDocument)`
/// leaves it before `loadedPolicy` hashes it: struct keys in schema order,
/// `TrimmedNonEmptyString` fields trimmed, records and `Schema.Unknown`
/// setting values as written. Called with a document `parse` accepts.
#[cfg(feature = "native")]
fn encode_document(raw: &Value) -> Value {
    type Field = (&'static str, fn(&Value) -> Value);
    fn pick(v: &Value, fields: &[Field]) -> Value {
        Value::Object(
            fields
                .iter()
                .filter_map(|(key, encode)| Some(((*key).to_owned(), encode(v.get(*key)?))))
                .collect(),
        )
    }
    fn same(v: &Value) -> Value {
        v.clone()
    }
    fn trimmed(v: &Value) -> Value {
        v.as_str().map_or_else(|| v.clone(), |s| json!(s.trim()))
    }
    fn record(v: &Value, encode: fn(&Value) -> Value) -> Value {
        Value::Object(dict(v).map(|(k, v)| (k.clone(), encode(v))).collect())
    }
    fn setting(v: &Value) -> Value {
        pick(v, &[("value", same), ("locked", same)])
    }
    fn settings(v: &Value) -> Value {
        record(v, setting)
    }
    fn variable(v: &Value) -> Value {
        pick(v, &[("name", trimmed), ("value", same)])
    }
    fn environment(v: &Value) -> Value {
        Value::Array(list(v).map(variable).collect())
    }
    fn instance(v: &Value) -> Value {
        pick(
            v,
            &[
                ("locked", same),
                ("environment", environment),
                ("binaryPath", trimmed),
                ("launchArgs", trimmed),
            ],
        )
    }
    fn instances(v: &Value) -> Value {
        record(v, instance)
    }
    fn providers(v: &Value) -> Value {
        pick(v, &[("allowedDrivers", same), ("instances", instances)])
    }
    fn updates(v: &Value) -> Value {
        pick(
            v,
            &[("mode", same), ("feedUrl", trimmed), ("allowNightly", same)],
        )
    }
    fn limits(v: &Value) -> Value {
        pick(
            v,
            &[
                ("maxRuntimeMode", same),
                ("clients", same),
                ("connect", same),
                ("webhookTasks", same),
                ("telemetry", same),
                ("browserCookieImport", same),
                ("providers", providers),
                ("updates", updates),
            ],
        )
    }
    pick(
        raw,
        &[
            ("schemaVersion", same),
            ("organization", trimmed),
            ("supportUrl", trimmed),
            ("settings", settings),
            ("limits", limits),
        ],
    )
}
impl Policy {
    /// The managed policy, read from its system path in `documents`.
    #[cfg(feature = "native")]
    pub(crate) fn load(documents: &dyn DocumentStore) -> Result<Self> {
        Self::load_document(documents).map_err(|e| ManagedPolicyFileError(e).into())
    }
    #[cfg(feature = "native")]
    fn load_document(documents: &dyn DocumentStore) -> Result<Self> {
        if cfg!(debug_assertions) {
            if let Some(path) = std::env::var_os("T3CODE_DEV_MANAGED_POLICY_PATH") {
                return Self::from_path(documents, Path::new(&path));
            }
        }
        let file = platform::system_path();
        if let Some((source, doc)) = platform::overlay()? {
            ensure!(
                !documents.exists(file)?,
                "policy is set at both {source} and {}; deploy it in one place",
                file.display()
            );
            return Self::parse_at(&source, doc)
                .with_context(|| format!("T3 Code cannot use the managed policy at {source}"));
        }
        Self::from_path(documents, file)
    }
    #[cfg(feature = "native")]
    fn from_path(documents: &dyn DocumentStore, path: &Path) -> Result<Self> {
        let Some(bytes) = documents.read_optional(path)? else {
            return Ok(Self::default());
        };
        serde_json::from_slice(&bytes).map_err(anyhow::Error::from).and_then(|doc|Self::parse_at(&path.display().to_string(),doc)).map_err(|e|anyhow!("T3 Code cannot use the managed policy at {}: {e}. It will not start until an administrator fixes this.",path.display()))
    }
    #[cfg(feature = "native")]
    fn parse_at(source: &str, raw: Value) -> Result<Self> {
        let encoded = encode_document(&raw);
        let mut p = Self::parse(raw)?;
        let digest = <sha2::Sha256 as sha2::Digest>::digest(crate::wire_json::compact(&encoded));
        p.loaded = Some(json!({
            "source": source,
            "organization": encoded["organization"],
            "fingerprint": &hex::encode(digest)[..12],
        }));
        Ok(p)
    }
    pub(crate) fn parse(raw: Value) -> Result<Self> {
        // Optional fields, when present, still have to satisfy their schema.
        for key in ["settings", "limits", "supportUrl"] {
            ensure!(raw.get(key) != Some(&Value::Null), "{key} cannot be null");
        }
        let doc: Document = serde_json::from_value(raw)?;
        ensure!(
            doc.schema_version == 1 && !doc.organization.trim().is_empty(),
            "invalid schemaVersion or organization"
        );
        let mut p = Self::default();
        let mut summary = json!({"organization":doc.organization.trim()});
        if let Some(url) = doc.support_url {
            ensure!(!url.trim().is_empty(), "invalid supportUrl");
            summary["supportUrl"] = json!(url.trim());
        }
        let built = schema::defaults();
        let mut applied = built.clone();
        for (path, value) in doc.settings {
            let setting: Setting = serde_json::from_value(value)?;
            let parts = path.split('.').map(str::to_owned).collect::<Vec<_>>();
            let head = parts[0].as_str();
            ensure!(
                parts.iter().all(|p| !p.is_empty()),
                "\"{path}\" is not a settings path"
            );
            ensure!(head!="observability","\"{path}\" is resolved at startup from each user's environment, settings file and desktop bootstrap input, which a policy cannot override");
            ensure!(
                head != "compactIdleThreads",
                "\"{head}\" follows \"idleCompaction\"; manage \"idleCompaction\" instead"
            );
            ensure!(
                ![
                    "projectSettingsOverrides",
                    "projectAgentBrowserAccessOverrides",
                    "projectAutoPullOverrides",
                    "projectScriptOverrides"
                ]
                .contains(&head)
                    && (built.get(head).is_some()
                        || schema::decode_patch(&json!({head:setting.value}))
                            .is_ok_and(|v| v.get(head).is_some())),
                "\"{path}\" is not a setting this version can manage"
            );
            ensure!(!setting.locked || head!="providerInstances" || parts.len()<=2,"\"{path}\" is one field of a provider instance whose fields override each other; lock \"providerInstances.{}\" as a whole, or pin fields with limits.providers.instances",parts[1]);
            for len in 1..parts.len() {
                let mut nullable = built.clone();
                set(&mut nullable, &parts[..len], Some(&Value::Null));
                ensure!(schema::normalize(&nullable).is_err(),"\"{path}\" is inside a setting that can be empty; manage \"{head}\" as a whole");
            }
            let entry = Entry {
                path,
                parts,
                value: setting.value,
            };
            if setting.locked {
                p.locks.push(entry)
            } else {
                p.defaults.push(entry)
            }
        }
        for e in &p.locks {
            ensure!(
                !(e.path.starts_with("storageCleanup.worktree") || e.path == "storageCleanup")
                    || p.locks.iter().any(|e| e.path == "worktreeCleanup"),
                "\"{}\" can be overridden by \"worktreeCleanup\"; lock \"worktreeCleanup\" too",
                e.path
            );
        }
        for entry in p.defaults.iter().chain(&p.locks) {
            set(&mut applied, &entry.parts, Some(&entry.value));
        }
        let round = schema::normalize(&applied)
            .map_err(|_| anyhow!("a managed setting has a value of the wrong type"))?;
        for e in p.defaults.iter().chain(&p.locks) {
            ensure!(
                get(&round, &e.parts) == Some(&e.value),
                "\"{}\" is not a setting this version can manage, or its value is invalid",
                e.path
            );
        }
        ensure!(!super::secrets::contains_credentials(&round),"a managed setting holds a credential, which every user can read in the policy; deploy credentials another way");
        for e in &p.defaults {
            ensure!(
                !get(&built, &e.parts)
                    .map(|old| adds_keys(old, &e.value))
                    .unwrap_or(e.parts.len() > 1),
                "\"{}\" adds entries users could not remove; lock it instead",
                e.path
            );
        }
        summary["lockedSettings"] = json!(p.locks.iter().map(|e| &e.path).collect::<Vec<_>>());
        if let Some(limits) = doc.limits {
            limited(
                &limits,
                &[
                    "maxRuntimeMode",
                    "clients",
                    "connect",
                    "webhookTasks",
                    "telemetry",
                    "browserCookieImport",
                    "providers",
                    "updates",
                ],
            )?;
            let mut public = json!({});
            if let Some(mode) = limits.get("maxRuntimeMode") {
                ensure!(MODES.iter().any(|m| mode == *m), "invalid maxRuntimeMode");
                public["maxRuntimeMode"] = mode.clone();
            }
            ensure!(
                !p.defaults
                    .iter()
                    .chain(&p.locks)
                    .any(|e| e.path == "defaultRuntimeMode"
                        && above(&e.value, &limits["maxRuntimeMode"])),
                "\"defaultRuntimeMode\" is above \"limits.maxRuntimeMode\""
            );
            if let Some(clients) = limits.get("clients") {
                ensure!(
                    clients.as_array().is_some_and(|a| !a.is_empty()
                        && a.iter().all(|v| matches!(
                            v.as_str(),
                            Some("desktop" | "local-browser" | "remote")
                        ))),
                    "invalid clients"
                );
                public["clients"] = clients.clone();
            }
            for key in [
                "connect",
                "webhookTasks",
                "telemetry",
                "browserCookieImport",
            ] {
                if let Some(flag) = limits.get(key) {
                    ensure!(flag.is_boolean(), "invalid {key}");
                    if key != "connect" || flag == false {
                        public[key] = flag.clone();
                    }
                }
            }
            if limits.get("clients").is_some() && !list(&limits["clients"]).any(|v| v == "remote") {
                public["connect"] = json!(false);
            }
            p.providers = providers::Rules::new(
                limits.get("providers"),
                limits.get("maxRuntimeMode").is_some(),
            )?;
            if let Some(v) = p.providers.summary() {
                public["providers"] = v;
            }
            if let Some(v) = limits.get("updates") {
                limited(v, &["mode", "allowNightly", "feedUrl"])?;
                ensure!(
                    matches!(v["mode"].as_str(), Some("auto" | "off")),
                    "invalid updates mode"
                );
                let mut updates = json!({"mode":v["mode"]});
                if let Some(flag) = v.get("allowNightly") {
                    ensure!(flag.is_boolean(), "invalid allowNightly");
                    updates["allowNightly"] = flag.clone();
                }
                if let Some(url) = v.get("feedUrl") {
                    ensure!(
                        url.as_str().is_some_and(|s| s
                            .trim()
                            .to_ascii_lowercase()
                            .starts_with("https://")
                            && url::Url::parse(s).is_ok_and(|v| v.host().is_some())),
                        "invalid feedUrl"
                    );
                }
                public["updates"] = updates;
            }
            summary["limits"] = public;
        }
        p.summary = Some(summary);
        Ok(p)
    }
    pub(crate) fn summary(&self) -> Option<&Value> {
        self.summary.as_ref()
    }
    /// The runtime record's `managedPolicy`: `{source, organization,
    /// fingerprint}` for the loaded policy, `null` for none.
    #[cfg(feature = "native")]
    pub(crate) fn loaded(&self) -> Value {
        self.loaded.clone().unwrap_or(Value::Null)
    }
    pub(crate) fn allows_connect(&self) -> bool {
        self.summary()
            .is_none_or(|v| v["limits"]["connect"] != false)
    }
    pub(crate) fn allows_client(&self, kind: &str) -> bool {
        self.summary()
            .and_then(|v| v["limits"].get("clients"))
            .is_none_or(|v| list(v).any(|c| c == kind))
    }
    pub(crate) fn pairing_refusal(&self) -> Option<String> {
        (!self.allows_client("remote")).then(|| format!(
            "{}'s policy does not allow remote clients, so this server cannot create pairing links.",
            self.org()
        ))
    }
    #[cfg(feature = "native")]
    pub(crate) fn denial(&self, action: &str) -> String {
        format!("{}'s policy does not allow {action}.", self.org())
    }
    fn org(&self) -> &str {
        self.summary()
            .and_then(|v| v["organization"].as_str())
            .unwrap_or("Your organization")
    }
    pub(crate) fn runtime_denial(&self, mode: &str) -> Option<String> {
        let ceiling = &self.summary()?["limits"]["maxRuntimeMode"];
        let label = ["Supervised", "Auto-accept edits", "Auto", "Full access"]
            [MODES.iter().position(|m| ceiling == *m)?];
        above(&json!(mode), ceiling).then(|| format!("{} limits threads to {label}.", self.org()))
    }
    pub(crate) fn provider_denial(&self, driver: &str) -> Option<String> {
        self.providers.denial(self.org(), driver)
    }
    pub(crate) fn with_defaults(&self, persisted: &Value) -> Value {
        let mut tree = json!({});
        for e in &self.defaults {
            if e.path == "idleCompaction"
                && persisted.get("compactIdleThreads").is_some()
                && persisted.get("idleCompaction").is_none()
            {
                continue;
            }
            set(&mut tree, &e.parts, Some(&e.value));
        }
        merge(&mut tree, persisted);
        tree
    }
    pub(crate) fn default_settings(&self) -> Value {
        let mut result = schema::defaults();
        for e in &self.defaults {
            set(&mut result, &e.parts, Some(&e.value));
        }
        if let Some(idle) = result["idleCompaction"].as_str() {
            result["compactIdleThreads"] = json!(idle == "beforeCacheExpires");
        }
        result
    }
    pub(crate) fn effective(&self, current: &Value) -> Value {
        let mut next = current.clone();
        for e in &self.locks {
            for path in paths(&next, e) {
                set(&mut next, &path, Some(&e.value));
            }
            if e.path == "idleCompaction" {
                next["compactIdleThreads"] = json!(e.value == "beforeCacheExpires");
            }
        }
        if let Some(ceiling) = self
            .summary()
            .and_then(|s| s["limits"].get("maxRuntimeMode"))
        {
            if above(&next["defaultRuntimeMode"], ceiling) {
                next["defaultRuntimeMode"] = ceiling.clone();
            }
            if let Some(projects) = next["projectSettingsOverrides"].as_object_mut() {
                for p in projects.values_mut() {
                    if above(&p["defaultRuntimeMode"], ceiling) {
                        p["defaultRuntimeMode"] = ceiling.clone();
                    }
                }
            }
        }
        self.providers.overlay(&mut next);
        next
    }
    fn locked(&self, path: &str) -> Value {
        json!({"operation":"write-managed-setting","managedSetting":path,"managedBy":self.org()})
    }
    pub(crate) fn check_update(&self, _current: &Value, input: &Value) -> Result<(), String> {
        for e in &self.locks {
            if paths(input, e)
                .iter()
                .any(|p| get(input, p).is_some_and(|v| !matches_lock(v, &e.value)))
            {
                return Err(e.path.clone());
            }
        }
        Ok(())
    }
    pub(crate) fn vet_write(
        &self,
        current: &Value,
        next: &Value,
        patch: &Value,
    ) -> Result<Value, Value> {
        self.check_update(current, patch)
            .map_err(|path| self.locked(&path))?;
        let mut kept = next.clone();
        for e in &self.locks {
            if paths(next, e).iter().any(|p| {
                get(next, p)
                    .is_some_and(|v| Some(v) != get(current, p) && !matches_lock(v, &e.value))
            }) {
                return Err(self.locked(&e.path));
            }
            for p in paths(&kept, e) {
                if (1..p.len()).all(|i| get(current, &p[..i]).is_some()) {
                    set(&mut kept, &p, get(current, &p));
                }
            }
        }
        self.providers.vet(self, current, &mut kept)?;
        Ok(kept)
    }
    pub(crate) fn keep_stored(&self, stripped: &Value, settings: &Value, stored: &Value) -> Value {
        let mut result = stripped.clone();
        let built = schema::defaults();
        for e in &self.defaults {
            let len = e
                .parts
                .iter()
                .position(|s| atomic(s, &built))
                .map_or(e.parts.len(), |n| n + 1);
            restore(&mut result, settings, stored, &e.parts[..len], &built);
            if e.path == "idleCompaction" {
                restore(
                    &mut result,
                    settings,
                    stored,
                    &["compactIdleThreads".into()],
                    &built,
                );
            }
        }
        result
    }
}
fn atomic(key: &str, built: &Value) -> bool {
    matches!(
        key,
        "backgroundActivity"
            | "automaticGitFetchInterval"
            | "providerHealthRefreshInterval"
            | "sourceControlWriterModelSelection"
            | "reviewGuideModelSelection"
            | "textGenerationModelSelection"
            | "fallbackModelSelection"
            | "pullRequestMergeMethod"
    ) || built.get(key) == Some(&Value::Null)
}
fn restore(result: &mut Value, settings: &Value, stored: &Value, path: &[String], built: &Value) {
    let Some(old) = get(stored, path) else { return };
    if !atomic(path.last().unwrap(), built) && old.is_object() {
        for (k, _) in dict(old) {
            let mut p = path.to_vec();
            p.push(k.clone());
            restore(result, settings, stored, &p, built);
        }
    } else if get(result, path).is_none() && get(settings, path) == Some(old) {
        set(result, path, Some(old));
    }
}

#[cfg(all(test, feature = "native"))]
mod tests {
    use super::*;
    use crate::persistence::FsStore;

    #[test]
    fn restores_stored_values_for_echoed_locks_and_applies_project_ceiling() {
        let p=Policy::parse(json!({"schemaVersion":1,"organization":"Example Corp",
            "settings":{"defaultAutoPull":{"value":false,"locked":true},"keepAwakeWhileAgentsWork":{"value":false,"locked":false}},
            "limits":{"maxRuntimeMode":"auto","clients":["desktop"]}})).unwrap();
        let raw = json!({"defaultAutoPull":true,"keepAwakeWhileAgentsWork":true,"projectSettingsOverrides":{"p":{"defaultRuntimeMode":"full-access","defaultAutoPull":true}}});
        let stored = p.with_defaults(&raw);
        assert_eq!(stored, raw);
        let effective = p.effective(&stored);
        assert_eq!(effective["defaultAutoPull"], false);
        assert_eq!(
            effective["projectSettingsOverrides"]["p"]["defaultAutoPull"],
            false
        );
        assert_eq!(
            effective["projectSettingsOverrides"]["p"]["defaultRuntimeMode"],
            "auto"
        );
        assert!(!p.allows_connect());
        assert_eq!(
            p.with_defaults(&json!({}))["keepAwakeWhileAgentsWork"],
            false
        );
        assert_eq!(
            p.vet_write(&stored, &effective, &json!({"defaultAutoPull":false}))
                .unwrap()["defaultAutoPull"],
            true
        );
        let denied = p
            .vet_write(
                &stored,
                &stored,
                &json!({"projectSettingsOverrides":{"p":{"defaultAutoPull":true}}}),
            )
            .unwrap_err();
        assert_eq!(
            denied,
            json!({"operation":"write-managed-setting","managedSetting":"defaultAutoPull","managedBy":"Example Corp"})
        );
    }

    #[test]
    fn preserves_an_explicit_atomic_default_only_when_the_whole_stored_value_matches() {
        let built = schema::defaults();
        let object = &built["backgroundActivity"];
        let policy = Policy::parse(json!({"schemaVersion":1,"organization":"Example",
            "settings":{"backgroundActivity":{"locked":false,"value":object}}}))
        .unwrap();
        let sparse = json!({});
        let partial = json!({"backgroundActivity":{"profile":object["profile"]}});
        assert_eq!(policy.keep_stored(&sparse, &built, &partial), sparse);
        let complete = json!({"backgroundActivity":object});
        assert_eq!(policy.keep_stored(&sparse, &built, &complete), complete);
    }

    #[test]
    fn unknown_fields_invalid_paths_and_credentials_refuse_startup() {
        let base = json!({"schemaVersion":1,"organization":"Example"});
        for settings in [
            json!({"storageCleanup.logsAfterDayz":{"locked":true,"value":99}}),
            json!({"compactIdleThreads":{"locked":true,"value":true}}),
            json!({"providerInstances":{"locked":false,"value":{"codexWork":{"driver":"codex"}}}}),
            json!({"providerInstances":{"locked":true,"value":{"test":{"driver":"antigravity","config":{"apiKey":"secret"}}}}}),
            json!({"projectAutoPullOverrides":{"locked":true,"value":{}}}),
        ] {
            let mut doc = base.clone();
            doc["settings"] = settings;
            assert!(Policy::parse(doc).is_err());
        }
        for limits in [
            json!({"future":1}),
            json!({"clients":[]}),
            json!({"providers":{"allowedDrivers":["does-not-exist"]}}),
        ] {
            let mut doc = base.clone();
            doc["limits"] = limits;
            assert!(Policy::parse(doc).is_err());
        }
        let dir = tempfile::tempdir().unwrap();
        assert!(Policy::from_path(&FsStore, &dir.path().join("policy.json"))
            .unwrap()
            .summary()
            .is_none());
        std::fs::write(dir.path().join("policy.json"), "{").unwrap();
        assert!(Policy::from_path(&FsStore, &dir.path().join("policy.json")).is_err());
    }

    #[test]
    fn loaded_policy_fingerprints_the_schema_encoded_document() {
        assert_eq!(Policy::default().loaded(), Value::Null);
        // Keys out of schema order and untrimmed strings: the reference hashes
        // JSON.stringify of the encoded document, so these are normalized.
        // Expected digest: sha256 of the JSON below, first 12 hex digits.
        // {"schemaVersion":1,"organization":"Example","limits":{"maxRuntimeMode":"auto",
        //  "providers":{"allowedDrivers":["codex"],"instances":{"codex":{"locked":true,
        //  "environment":[{"name":"CODEX_HOME","value":"/managed"}],"launchArgs":"--corp"}}},
        //  "updates":{"mode":"auto","allowNightly":false}}}
        let p = Policy::parse_at("/etc/t3code/policy.json", json!({
            "limits":{"updates":{"allowNightly":false,"mode":"auto"},
                "providers":{"instances":{"codex":{"environment":[{"value":"/managed","name":"CODEX_HOME"}],"launchArgs":" --corp ","locked":true}},"allowedDrivers":["codex"]},
                "maxRuntimeMode":"auto"},
            "organization":"  Example  ","schemaVersion":1
        }))
        .unwrap();
        let loaded = p.loaded();
        assert_eq!(
            loaded.as_object().unwrap().keys().collect::<Vec<_>>(),
            ["source", "organization", "fingerprint"]
        );
        assert_eq!(
            loaded,
            json!({"source":"/etc/t3code/policy.json","organization":"Example","fingerprint":"88197a117d2a"})
        );
        // {"schemaVersion":1,"organization":"Example","settings":{"defaultAutoPull":{"value":false,"locked":true}}}
        let p = Policy::parse_at(
            "/test/policy.json",
            json!({"settings":{"defaultAutoPull":{"locked":true,"value":false}},"organization":"Example","schemaVersion":1}),
        )
        .unwrap();
        assert_eq!(p.loaded()["fingerprint"], "dcac31e18105");
        assert_eq!(p.loaded()["source"], "/test/policy.json");
    }

    #[test]
    fn provider_pins_read_and_write_without_storing_administrator_values() {
        let p=Policy::parse(json!({"schemaVersion":1,"organization":"Example","limits":{"providers":{
            "allowedDrivers":["codex"],"instances":{"codex":{"locked":true,"launchArgs":"--corp","environment":[{"name":"CODEX_HOME","value":"/managed"}]}}
        }}})).unwrap();
        let current = json!({"providerInstances":{"work":{"driver":"codex","config":{"homePath":"/mine","launchArgs":"--local"},"environment":[{"name":"CODEX_HOME","value":"/mine","sensitive":false}]}},
            "projectSettingsOverrides":{},"textGenerators":{"custom":{}},"sourceControlWriterModelSelection":{"instanceId":"pi"}});
        let effective = p.effective(&current);
        assert_eq!(
            effective["providerInstances"]["work"]["config"],
            json!({"launchArgs":"--corp"})
        );
        assert_eq!(
            effective["providerInstances"]["codex"]["config"],
            json!({"launchArgs":"--corp"})
        );
        assert_eq!(
            effective["providerInstances"]["work"]["environment"][0]["value"],
            "/managed"
        );
        assert!(effective["sourceControlWriterModelSelection"].is_null());
        assert_eq!(effective["textGenerators"], json!({}));
        let kept = p.vet_write(&current, &effective, &effective).unwrap();
        assert_eq!(kept["providerInstances"], current["providerInstances"]);
        let mut next = current.clone();
        next["providerInstances"]["pi-work"] = json!({"driver":"pi","enabled":true});
        assert_eq!(
            p.vet_write(&current, &next, &next).unwrap_err(),
            json!({"operation":"use-disallowed-provider","providerInstanceId":"pi-work","managedBy":"Example","providerLabel":"Pi"})
        );
    }
}
