use super::failure;
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
pub(super) use failure::rpc as rpc_error;
use serde_json::{json, Map, Value};
use std::{
    collections::{HashMap, HashSet},
    fs,
    io::{self, Write},
    path::Path,
};
use uuid::Uuid;

const REDACTED: &str = "••••••";

fn encoded(s: &str) -> String {
    URL_SAFE_NO_PAD.encode(s)
}
pub(crate) fn reference(v: &Value) -> bool {
    let (Some(p), Some(r)) = (v["provider"].as_str(), v["reference"].as_str()) else {
        return false;
    };
    !p.is_empty()
        && p.len() <= 64
        && p.as_bytes()[0].is_ascii_alphanumeric()
        && p.bytes()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, b'_' | b'-'))
        && !r.is_empty()
        && r.trim() == r
        && r.encode_utf16().count() <= 1024
        && !r.contains(['\r', '\n'])
}

struct Slot<'a> {
    name: &'a str,
    environment: Option<(&'a str, &'a str)>,
    sensitive: bool,
    value: &'a Value,
    redacted: bool,
}
struct Mapped {
    value: Value,
    redacted: bool,
}
impl Slot<'_> {
    fn keep(&self) -> Mapped {
        Mapped {
            value: self.value.clone(),
            redacted: self.redacted,
        }
    }
    fn literal(&self) -> &str {
        self.value.as_str().unwrap_or("")
    }
}
fn map_fields(
    fields: &mut Value,
    keys: &[&str],
    prefix: &str,
    visit: &mut impl FnMut(Slot) -> Mapped,
) {
    fn one(v: &mut Value, name: &str, visit: &mut impl FnMut(Slot) -> Mapped) {
        if !(v.is_string() || reference(v)) {
            return;
        }
        let mapped = visit(Slot {
            name,
            environment: None,
            sensitive: true,
            redacted: *v == REDACTED,
            value: v,
        });
        *v = if mapped.redacted {
            json!(REDACTED)
        } else {
            mapped.value
        };
    }
    for &key in keys {
        let Some(v) = fields.get_mut(key) else {
            continue;
        };
        let name = if prefix == "voice-cloud" {
            format!("{prefix}-{key}")
        } else if key == "managementKey" && prefix.starts_with("usage-limit-source-") {
            prefix.to_owned()
        } else {
            format!("{prefix}-{}", encoded(key))
        };
        if v.is_object() && !reference(v) {
            let old = std::mem::take(v.as_object_mut().unwrap());
            let mut refs = Map::new();
            let mut strings = Map::new();
            for (host, mut value) in old {
                if !(reference(&value) || value.is_string()) {
                    continue;
                }
                let host = host.trim().to_lowercase();
                let base = if key == "managementKey" {
                    format!("{prefix}-{}", encoded(key))
                } else {
                    name.clone()
                };
                one(&mut value, &format!("{base}-{}", encoded(&host)), visit);
                if reference(&value) {
                    refs.insert(host, value);
                } else {
                    strings.insert(host, value);
                }
            }
            refs.extend(strings);
            *v = Value::Object(refs);
        } else {
            one(v, &name, visit);
        }
    }
}

fn map(settings: &mut Value, mut visit: impl FnMut(Slot) -> Mapped) {
    if let Some(items) = settings
        .get_mut("providerInstances")
        .and_then(Value::as_object_mut)
    {
        for (id, instance) in items {
            let driver = instance["driver"].as_str().unwrap_or("").to_owned();
            let keys: &[&str] = match driver.as_str() {
                "antigravity" => &["apiKey"],
                "opencode" => &["serverPassword"],
                _ => &[],
            };
            if let Some(config) = instance.get_mut("config") {
                map_fields(
                    config,
                    keys,
                    &format!("provider-config-{}-{}", encoded(id), encoded(&driver)),
                    &mut visit,
                );
            }
            if let Some(vars) = instance
                .get_mut("environment")
                .and_then(Value::as_array_mut)
            {
                for var in vars {
                    let Some(n) = var["name"].as_str() else {
                        continue;
                    };
                    let source = var
                        .get("secret")
                        .filter(|v| reference(v))
                        .unwrap_or(&var["value"]);
                    let is_reference = reference(source);
                    let result = visit(Slot {
                        name: &format!("provider-env-{}-{}", encoded(id), encoded(n)),
                        environment: Some((id, n)),
                        sensitive: is_reference || var["sensitive"] == true,
                        value: source,
                        redacted: !is_reference && var["valueRedacted"] == true,
                    });
                    var.as_object_mut().unwrap().shift_remove("valueRedacted");
                    if reference(&result.value) {
                        var["value"] = json!("");
                        var["sensitive"] = json!(true);
                        var["secret"] = result.value;
                    } else {
                        var.as_object_mut().unwrap().shift_remove("secret");
                        var["value"] = result.value;
                    }
                    if result.redacted {
                        var["valueRedacted"] = json!(true);
                    }
                }
            }
        }
    }
    if let Some(hosts) = settings
        .get_mut("sourceControlHosts")
        .and_then(Value::as_object_mut)
    {
        for (kind, config) in hosts {
            let keys: &[&str] = match kind.as_str() {
                "github" => &["tokens", "signIns"],
                "bitbucket" => &["accessToken", "apiToken"],
                "gitcafe" => &["token"],
                _ => &[],
            };
            map_fields(
                config,
                keys,
                &format!("source-control-{}", encoded(kind)),
                &mut visit,
            );
        }
    }
    for (group, field, prefix) in [
        ("usageLimitSources", "managementKey", "usage-limit-source"),
        ("textGenerators", "apiKey", "text-generator"),
    ] {
        if let Some(items) = settings.get_mut(group).and_then(Value::as_object_mut) {
            for (id, config) in items {
                map_fields(
                    config,
                    &[field],
                    &format!("{prefix}-{}", encoded(id)),
                    &mut visit,
                );
            }
        }
    }
    for (group, field, prefix) in [
        ("easDeviceHosts", "accessToken", "device-host"),
        ("simlockDeviceHosts", "agentToken", "simlock-device-host"),
    ] {
        if let Some(items) = settings.get_mut(group).and_then(Value::as_array_mut) {
            for config in items {
                let Some(id) = config["id"].as_str() else {
                    continue;
                };
                map_fields(
                    config,
                    &[field],
                    &format!("{prefix}-{}", encoded(id)),
                    &mut visit,
                );
            }
        }
    }
    if let Some(config) = settings.get_mut("cloudSpeechCredentials") {
        map_fields(
            config,
            &["openai", "xai", "openrouter"],
            "voice-cloud",
            &mut visit,
        );
    }
}

pub(super) fn contains_credentials(settings: &Value) -> bool {
    let mut found = false;
    map(&mut settings.clone(), |s| {
        found |= s.sensitive && !s.redacted && !s.literal().is_empty();
        s.keep()
    });
    found
}

pub(crate) fn redact(settings: &Value, secret_references: bool) -> Value {
    let mut next = settings.clone();
    map(&mut next, |s| {
        if reference(s.value) {
            return s.keep();
        }
        if s.sensitive {
            Mapped {
                value: json!(""),
                redacted: s.redacted || !s.literal().is_empty(),
            }
        } else {
            Mapped {
                value: s.value.clone(),
                redacted: false,
            }
        }
    });
    if !secret_references {
        map(&mut next, |s| {
            if s.environment.is_none() && reference(s.value) {
                Mapped {
                    value: json!(REDACTED),
                    redacted: false,
                }
            } else {
                s.keep()
            }
        });
    }
    for entry in next
        .get_mut("secretProviders")
        .and_then(Value::as_object_mut)
        .into_iter()
        .flat_map(|m| m.values_mut())
    {
        if entry["kind"] != "one_password" {
            if let Some(obj) = entry.as_object_mut() {
                obj.shift_remove("config");
            }
        }
    }
    next
}

pub(crate) fn materialize(disk: &Value, secrets_dir: &Path) -> io::Result<Value> {
    let mut next = disk.clone();
    let mut error = None;
    map(&mut next, |s| {
        if !s.sensitive || !s.redacted || reference(s.value) {
            return Mapped {
                value: s.value.clone(),
                redacted: false,
            };
        }
        match read(secrets_dir, s.name) {
            Ok(bytes) => {
                let text = String::from_utf8_lossy(bytes.as_deref().unwrap_or_default());
                Mapped {
                    value: json!(text.strip_prefix('\u{feff}').unwrap_or(&text)),
                    redacted: s.environment.is_some(),
                }
            }
            Err(e) => {
                error.get_or_insert_with(|| {
                    failure::store(secrets_dir, s.name, "read-secret", s.environment, e)
                });
                s.keep()
            }
        }
    });
    error.map_or(Ok(next), Err)
}

enum Change {
    Set(String, Vec<u8>),
    Remove(String, &'static str),
}
pub(super) struct Prepared {
    pub(super) disk: Value,
    changes: Vec<Change>,
    environment: HashMap<String, (String, String)>,
}

pub(super) fn ensure_dir(dir: &Path) -> io::Result<()> {
    fs::create_dir_all(dir)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(dir, fs::Permissions::from_mode(0o700))?;
    }
    Ok(())
}

fn read(dir: &Path, name: &str) -> io::Result<Option<Vec<u8>>> {
    match fs::read(dir.join(format!("{name}.bin"))) {
        Ok(v) => Ok(Some(v)),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e),
    }
}
fn write(dir: &Path, name: &str, bytes: Option<&[u8]>) -> io::Result<()> {
    let dest = dir.join(format!("{name}.bin"));
    let Some(bytes) = bytes else {
        return match fs::remove_file(dest) {
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
            result => result,
        };
    };
    let tmp = dir.join(format!("{name}.bin.{}.tmp", Uuid::new_v4()));
    let mut opts = fs::OpenOptions::new();
    opts.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        opts.mode(0o600);
    }
    let result = (|| {
        let mut f = opts
            .open(&tmp)
            .map_err(|e| failure::step(e, "writeFile", "open", &tmp, None))?;
        f.write_all(bytes)
            .map_err(|e| failure::step(e, "writeFile", "write", &tmp, None))?;
        f.sync_all()
            .map_err(|e| failure::step(e, "writeFile", "fsync", &tmp, None))?;
        drop(f);
        fs::rename(&tmp, &dest).map_err(|e| failure::step(e, "rename", "rename", &tmp, Some(&dest)))
    })();
    if result.is_err() {
        let _ = fs::remove_file(&tmp);
    }
    result
}
impl Prepared {
    pub(super) fn persist(
        &self,
        dir: &Path,
        write_settings: impl FnOnce(&Value) -> io::Result<()>,
    ) -> io::Result<()> {
        let mut previous = Vec::<(&str, Option<Vec<u8>>)>::new();
        let result = (|| {
            for change in &self.changes {
                let (name, bytes, operation) = match change {
                    Change::Set(name, v) => (name, Some(&v[..]), "write-secret"),
                    Change::Remove(name, op) => (name, None, *op),
                };
                let env = self
                    .environment
                    .get(name)
                    .map(|(id, var)| (id.as_str(), var.as_str()));
                let was = read(dir, name)
                    .map_err(|e| failure::store(dir, name, "read-secret", env, e))?;
                if was.is_none() && bytes.is_none() {
                    continue;
                }
                previous.push((name, was));
                write(dir, name, bytes)
                    .map_err(|e| failure::store(dir, name, operation, env, e))?;
            }
            materialize(&self.disk, dir)?;
            write_settings(&self.disk)
        })();
        if let Err(cause) = result {
            for (name, was) in previous.into_iter().rev() {
                if let Err(e) = write(dir, name, was.as_deref()) {
                    tracing::warn!(error = %e, "failed to roll back settings secret");
                }
            }
            return Err(cause);
        }
        Ok(())
    }
}

fn legacy_slots(raw: &Value) -> Vec<(String, String)> {
    let mut moves = Vec::new();
    for (field, from) in [
        ("accessToken", "bitbucket-access-token"),
        ("apiToken", "bitbucket-api-token"),
    ] {
        if raw["sourceControlHosts"].get("bitbucket").is_none()
            && raw["bitbucket"][field] == REDACTED
        {
            moves.push((
                from.to_owned(),
                format!("source-control-{}-{}", encoded("bitbucket"), encoded(field)),
            ));
        }
    }
    if raw["sourceControlHosts"].get("github").is_none() {
        if let Some(tokens) = raw["github"]["tokens"].as_object() {
            for (host, v) in tokens {
                if v == REDACTED {
                    let host = encoded(&host.trim().to_lowercase());
                    moves.push((
                        format!("github-token-{host}"),
                        format!(
                            "source-control-{}-{}-{host}",
                            encoded("github"),
                            encoded("tokens")
                        ),
                    ));
                }
            }
        }
    }
    moves
}

/// Startup copies legacy slot names before retiring any source. If individual inline
/// values cannot move, keep them intact so they can move on a later settings reload.
pub(super) fn migrate(
    mut settings: Value,
    prior_hosts: Value,
    raw: &Value,
    dir: &Path,
    otherwise_changed: bool,
    save: impl FnOnce(&Value) -> io::Result<()>,
) -> io::Result<Value> {
    let moves = legacy_slots(raw);
    let mut previous = Vec::<(String, Option<Vec<u8>>)>::new();
    let mut store = |name: &str, value: &[u8]| -> io::Result<()> {
        let old = read(dir, name)?;
        write(dir, name, Some(value))?;
        previous.push((name.to_owned(), old));
        Ok(())
    };
    let mut copied = true;
    for (from, to) in &moves {
        if let Err(e) = read(dir, from).and_then(|bytes| match bytes {
            Some(value) => store(to, &value),
            None => Ok(()),
        }) {
            tracing::warn!(error=%e,"could not migrate source control credential");
            copied = false;
        }
    }
    let mut changed =
        otherwise_changed || copied && ["bitbucket", "github"].iter().any(|k| raw[k].is_object());
    if !copied {
        settings["sourceControlHosts"] = prior_hosts;
    }
    let mut disk = settings.clone();
    let mut moved_inline = false;
    map(&mut disk, |s| {
        if s.environment.is_none() && s.sensitive && !s.redacted && !s.literal().is_empty() {
            match store(s.name, s.literal().as_bytes()) {
                Ok(()) => {
                    moved_inline = true;
                    return Mapped {
                        value: json!(""),
                        redacted: true,
                    };
                }
                Err(e) => tracing::warn!(error=%e,"could not migrate inline settings credential"),
            }
        }
        s.keep()
    });
    if moved_inline {
        settings = disk;
        changed = true;
    }
    if changed {
        if let Err(e) = save(&settings) {
            for (name, old) in previous.into_iter().rev() {
                if let Err(error) = write(dir, &name, old.as_deref()) {
                    tracing::warn!(%error,"failed to roll back settings secret");
                }
            }
            return Err(e);
        }
        if copied {
            for (from, _) in &moves {
                let _ = write(dir, from, None);
            }
        }
    }
    Ok(settings)
}

fn origin(url: &Value) -> String {
    let s = url.as_str().unwrap_or("");
    reqwest::Url::parse(s)
        .map(|url| url.origin().ascii_serialization())
        .unwrap_or_else(|_| s.to_owned())
}
pub(super) fn restore_references(current: &Value, mut next: Value) -> Value {
    let mut refs = HashMap::new();
    map(&mut current.clone(), |s| {
        if s.environment.is_none() && reference(s.value) {
            refs.insert(s.name.to_owned(), s.value.clone());
        }
        s.keep()
    });
    if !refs.is_empty() {
        map(&mut next, |s| {
            if s.environment.is_none() && s.redacted {
                if let Some(reference) = refs.get(s.name) {
                    return Mapped {
                        value: reference.clone(),
                        redacted: false,
                    };
                }
            }
            s.keep()
        });
    }
    for host in next
        .get_mut("simlockDeviceHosts")
        .and_then(Value::as_array_mut)
        .into_iter()
        .flatten()
    {
        if let Some(prior) = current["simlockDeviceHosts"]
            .as_array()
            .into_iter()
            .flatten()
            .find(|h| h["id"] == host["id"])
        {
            if origin(&prior["gatewayUrl"]) != origin(&host["gatewayUrl"])
                && (host["agentToken"] == REDACTED
                    || reference(&host["agentToken"]) && host["agentToken"] == prior["agentToken"])
            {
                host["agentToken"] = json!("");
            }
        }
    }
    next
}

pub(super) fn prepare(current: &Value, mut next: Value) -> Prepared {
    let removed = current["secretProviders"]
        .as_object()
        .into_iter()
        .flat_map(|m| m.keys())
        .filter(|id| next["secretProviders"].get(id).is_none())
        .cloned()
        .collect::<HashSet<_>>();
    if !removed.is_empty() {
        map(&mut next, |s| {
            if s.environment.is_none()
                && reference(s.value)
                && removed.contains(s.value["provider"].as_str().unwrap())
            {
                return Mapped {
                    value: json!(""),
                    redacted: false,
                };
            }
            s.keep()
        });
        for item in next
            .get_mut("providerInstances")
            .and_then(Value::as_object_mut)
            .into_iter()
            .flat_map(|m| m.values_mut())
        {
            if let Some(vars) = item.get_mut("environment").and_then(Value::as_array_mut) {
                vars.retain(|v| {
                    let value = v.get("secret").unwrap_or(&v["value"]);
                    !reference(value) || !removed.contains(value["provider"].as_str().unwrap())
                });
            }
        }
    }
    let mut previous = indexmap::IndexSet::new();
    let mut environment = HashMap::new();
    let mut inline = HashMap::new();
    map(&mut current.clone(), |s| {
        if let Some((id, var)) = s.environment {
            environment.insert(s.name.to_owned(), (id.to_owned(), var.to_owned()));
        }
        if s.sensitive && (s.environment.is_some() || !reference(s.value)) {
            previous.insert(s.name.to_owned());
            if !s.redacted && !s.literal().is_empty() {
                inline.insert(s.name.to_owned(), s.literal().to_owned());
            }
        }
        s.keep()
    });
    let mut changes = Vec::new();
    let mut kept = indexmap::IndexSet::new();
    let mut written = HashSet::new();
    let mut to_remove = HashSet::new();
    map(&mut next, |s| {
        if reference(s.value) && s.environment.is_none() {
            return s.keep();
        }
        let name = s.name.to_owned();
        if let Some((id, var)) = s.environment {
            environment.insert(name.clone(), (id.to_owned(), var.to_owned()));
        }
        if s.sensitive && s.redacted {
            kept.insert(name);
            return Mapped {
                value: json!(""),
                redacted: true,
            };
        }
        if s.sensitive && !s.literal().is_empty() {
            written.insert(name.clone());
            changes.push(Change::Set(name, s.literal().as_bytes().to_vec()));
            return Mapped {
                value: json!(""),
                redacted: true,
            };
        }
        to_remove.insert(name.clone());
        changes.push(Change::Remove(name, "remove-secret"));
        Mapped {
            value: s.value.clone(),
            redacted: false,
        }
    });
    for name in &kept {
        if let Some(literal) = inline.get(name).filter(|_| !written.contains(name)) {
            changes.push(Change::Set(name.clone(), literal.as_bytes().to_vec()));
        }
    }
    for name in previous {
        if !written.contains(&name) && !kept.contains(&name) && !to_remove.contains(&name) {
            changes.push(Change::Remove(name, "remove-stale-secret"));
        }
    }
    changes.retain(|change| !matches!(change,Change::Remove(name,_) if kept.contains(name)));
    Prepared {
        disk: next,
        changes,
        environment,
    }
}

#[derive(Default)]
pub(crate) struct Resolver {
    values: std::sync::Mutex<HashMap<(String, String), Cached>>,
    reads: tokio::sync::Mutex<()>,
}

struct Cached {
    account: String,
    value: Option<String>,
}

#[derive(Copy, Clone, PartialEq, Eq)]
pub(crate) enum ReadMode {
    Cached,
    // Settings checks always read, and retain a prior usable runtime credential on failure.
    Check,
    // A recording must forget the credential from a previous upload.
    Invalidate,
}

pub(crate) fn provider_label<'a>(settings: &'a Value, source: &'a Value) -> &'a str {
    let id = source["provider"].as_str().unwrap_or("");
    settings["secretProviders"][id]["label"]
        .as_str()
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .unwrap_or(id)
}

impl Resolver {
    pub(crate) async fn resolve(
        &self,
        settings: &Value,
        source: &Value,
        mode: ReadMode,
    ) -> Option<String> {
        if let Some(s) = source.as_str() {
            return Some(s.to_owned());
        }
        let id = source["provider"].as_str()?;
        let account = settings["secretProviders"][id]["config"]["account"].as_str()?;
        let field = source["reference"].as_str()?;
        if settings["secretProviders"][id]["kind"] != "one_password"
            || !reference(source)
            || !valid_op_reference(account, field)
        {
            return None;
        }
        let key = (id.to_owned(), field.to_owned());
        let cached = || {
            self.values
                .lock()
                .unwrap()
                .get(&key)
                .filter(|c| c.account == account)
                .map(|c| c.value.clone())
        };
        if mode == ReadMode::Cached {
            if let Some(value) = cached() {
                return value;
            }
        }
        let _guard = self.reads.lock().await;
        if mode == ReadMode::Cached {
            if let Some(value) = cached() {
                return value;
            }
        }
        if mode == ReadMode::Invalidate {
            self.values.lock().unwrap().remove(&key);
        }
        let value = read_op(account, field, Path::new("op")).await;
        if mode != ReadMode::Check || value.is_some() || cached().is_none() {
            self.values.lock().unwrap().insert(
                key,
                Cached {
                    account: account.to_owned(),
                    value: value.clone(),
                },
            );
        }
        value
    }
}

fn valid_op_reference(account: &str, field: &str) -> bool {
    use std::sync::LazyLock;
    static ACCOUNT: LazyLock<regex::Regex> =
        LazyLock::new(|| regex::Regex::new(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,252}$").unwrap());
    static FIELD: LazyLock<regex::Regex> = LazyLock::new(|| {
        let name = "[A-Za-z0-9._-](?:[A-Za-z0-9 ._-]*[A-Za-z0-9._-])?";
        regex::Regex::new(&format!(
            r"^op://{name}(?:/{name}){{2,3}}(?:\?[a-z-]+=[A-Za-z0-9_-]+)?$"
        ))
        .unwrap()
    });
    ACCOUNT.is_match(account) && FIELD.is_match(field)
}

async fn read_op(account: &str, field: &str, cli: &Path) -> Option<String> {
    use std::{process::Stdio, time::Duration};
    tokio::time::timeout(Duration::from_secs(45), async {
        let result = tokio::process::Command::new(cli)
            .args([
                "--cache=false",
                "read",
                &format!("--account={account}"),
                "--no-newline",
                field,
            ])
            .stdin(Stdio::null())
            .stderr(Stdio::null())
            .kill_on_drop(true)
            .output()
            .await
            .ok()?;
        let text = String::from_utf8_lossy(&result.stdout);
        (result.status.success() && !text.trim().is_empty()).then(|| text.into_owned())
    })
    .await
    .ok()
    .flatten()
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;
    #[test]
    fn migration_and_failure_restore_the_exact_preceding_files() {
        let dir = tempdir().unwrap();
        let saved = json!({
            "providerInstances":{"sample":{"driver":"opencode","environment":[{"name":"PWD","value":"inline","sensitive":true}],"config":{"serverPassword":"password"}}},
            "cloudSpeechCredentials":{"openai":"speech-token"},
            "sourceControlHosts":{"github":{"tokens":{"Example.TEST":"host-token","other.test":REDACTED}}}
        });
        let raw = json!({"github":{"tokens":{"other.test":REDACTED}}});
        let old = format!("github-token-{}", encoded("other.test"));
        write(dir.path(), &old, Some(b"old-token")).unwrap();
        let apply = |save: fn(&Value) -> io::Result<()>| {
            migrate(saved.clone(), json!({}), &raw, dir.path(), false, save)
        };
        assert!(apply(|_| Err(io::Error::other("test failure"))).is_err());
        assert_eq!(fs::read_dir(dir.path()).unwrap().count(), 1);
        assert_eq!(
            read(dir.path(), &old).unwrap().as_deref(),
            Some(&b"old-token"[..])
        );
        let disk = apply(|_| Ok(())).unwrap();
        assert!(read(dir.path(), &old).unwrap().is_none());
        let plain = materialize(&disk, dir.path()).unwrap();
        assert_eq!(
            plain["sourceControlHosts"]["github"]["tokens"]["example.test"],
            "host-token"
        );
        assert_eq!(
            plain["sourceControlHosts"]["github"]["tokens"]["other.test"],
            "old-token"
        );
        assert_eq!(
            disk["providerInstances"]["sample"]["environment"][0]["value"],
            "inline"
        );
        assert_eq!(plain["cloudSpeechCredentials"]["openai"], "speech-token");
        let update = prepare(
            &disk,
            json!({"cloudSpeechCredentials":{"openai":"replacement"}}),
        );
        assert!(update
            .persist(dir.path(), |_| Err(io::Error::other("late error")))
            .is_err());
        assert_eq!(materialize(&disk, dir.path()).unwrap(), plain);
        update.persist(dir.path(), |_| Ok(())).unwrap();
        assert_eq!(fs::read_dir(dir.path()).unwrap().count(), 1);
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                fs::metadata(dir.path().join("voice-cloud-openai.bin"))
                    .unwrap()
                    .permissions()
                    .mode()
                    & 0o777,
                0o600
            );
        }
    }

    #[test]
    fn startup_skips_unreadable_credentials_and_retries_after_repair() {
        let dir = tempdir().unwrap();
        let raw = json!({"github":{"tokens":{"example.test":REDACTED}}, "cloudSpeechCredentials":{"openai":"fixture"}});
        let folded = json!({"sourceControlHosts":{"github":raw["github"]},"cloudSpeechCredentials":raw["cloudSpeechCredentials"]});
        let old = format!("github-token-{}", encoded("example.test"));
        let old_path = dir.path().join(format!("{old}.bin"));
        let speech_path = dir.path().join("voice-cloud-openai.bin");
        fs::create_dir(&old_path).unwrap();
        fs::create_dir(&speech_path).unwrap();
        let during = migrate(folded.clone(), json!({}), &raw, dir.path(), false, |_| {
            panic!("must leave the original settings alone")
        })
        .unwrap();
        assert_eq!(during["cloudSpeechCredentials"]["openai"], "fixture");
        assert_eq!(during["sourceControlHosts"], json!({}));
        assert!(old_path.is_dir());
        fs::remove_dir(&old_path).unwrap();
        write(dir.path(), &old, Some(b"old-token")).unwrap();
        fs::remove_dir(&speech_path).unwrap();
        let after = migrate(folded, json!({}), &raw, dir.path(), false, |disk| {
            assert_eq!(disk["cloudSpeechCredentials"]["openai"], REDACTED);
            assert!(old_path.is_file());
            Ok(())
        })
        .unwrap();
        assert!(!old_path.exists());
        let visible = materialize(&after, dir.path()).unwrap();
        assert_eq!(visible["cloudSpeechCredentials"]["openai"], "fixture");
        assert_eq!(
            visible["sourceControlHosts"]["github"]["tokens"]["example.test"],
            "old-token"
        );
    }

    #[test]
    fn capabilities_echo_redirect_and_deleted_references() {
        let r = json!({"provider":"vault","reference":"op://Private/item/token"});
        let s = json!({"secretProviders":{"vault":{"kind":"one_password","config":{"account":"acct"}},"newer":{"kind":"newer","config":{"token":"sensitive"}}},
            "providerInstances":{"example":{"driver":"antigravity","config":{"apiKey":r},"environment":[{"name":"A","value":"","sensitive":true,"secret":r}]}},
            "simlockDeviceHosts":[{"id":"one","gatewayUrl":"https://fixture.test/a","agentToken":r}],
            "cloudSpeechCredentials":{"openai":r,"xai":"literal"}
        });
        let old = redact(&s, false);
        assert!(old["secretProviders"]["newer"].get("config").is_none());
        assert_eq!(old["cloudSpeechCredentials"]["openai"], REDACTED);
        assert_eq!(old["cloudSpeechCredentials"]["xai"], REDACTED);
        assert_eq!(
            old["providerInstances"]["example"]["environment"][0]["secret"],
            r
        );
        assert_eq!(redact(&s, true)["cloudSpeechCredentials"]["openai"], r);
        let mut edited = old.clone();
        edited["simlockDeviceHosts"][0]["gatewayUrl"] = json!("https://different.test/b");
        let kept = prepare(&s, restore_references(&s, edited));
        assert_eq!(kept.disk["cloudSpeechCredentials"]["openai"], r);
        assert_eq!(kept.disk["simlockDeviceHosts"][0]["agentToken"], "");
        let mut removed = old;
        removed["secretProviders"]
            .as_object_mut()
            .unwrap()
            .remove("vault");
        let disk = prepare(&s, restore_references(&s, removed)).disk;
        assert_eq!(
            disk["providerInstances"]["example"]["environment"],
            json!([])
        );
        assert_eq!(disk["providerInstances"]["example"]["config"]["apiKey"], "");
        assert_eq!(disk["cloudSpeechCredentials"]["openai"], "");
    }

    #[test]
    fn legacy_clients_receive_host_references_first() {
        let settings = json!({"sourceControlHosts":{"github":{"tokens":{
            "literal.test":"plain", "vault.test":{"provider":"vault","reference":"op://example/item/token"}
        }}}});
        let shaped = redact(&settings, false);
        assert_eq!(
            shaped["sourceControlHosts"]["github"]["tokens"].to_string(),
            format!(r#"{{"vault.test":"{REDACTED}","literal.test":"{REDACTED}"}}"#)
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn one_password_uses_configured_account_and_exact_reference() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempdir().unwrap();
        let cli = dir.path().join("fake-op");
        fs::write(&cli, r#"#!/bin/sh
[ "$#" -eq 5 ] && [ "$1" = "--cache=false" ] && [ "$2" = read ] && [ "$3" = "--account=a.example" ] && [ "$4" = "--no-newline" ] && [ "$5" = "op://Private/example/Voice API key" ] || exit 2
printf %s fixture-only
"#).unwrap();
        fs::set_permissions(&cli, fs::Permissions::from_mode(0o700)).unwrap();
        let account = "a.example";
        let field = "op://Private/example/Voice API key";
        assert!(valid_op_reference(account, field));
        assert_eq!(
            read_op(account, field, &cli).await.as_deref(),
            Some("fixture-only")
        );
        fs::write(&cli, "#!/bin/sh\nprintf '  '\n").unwrap();
        assert_eq!(read_op(account, field, &cli).await, None);
        assert!(!valid_op_reference(account, "--help"));
        assert!(!valid_op_reference("bad account", field));
    }

    #[test]
    fn duplicate_environment_rows_preserve_claimed_literal() {
        let dir = tempdir().unwrap();
        let settings =
            |rows| json!({"providerInstances":{"server":{"driver":"codex","environment":rows}}});
        let previous = settings(json!([{"name":"KEY","value":"old","sensitive":true}]));
        let redacted = json!({"name":"KEY","value":"","sensitive":true,"valueRedacted":true});
        let next = settings(
            json!([redacted,{"name":"KEY","value":"new","sensitive":true},{"name":"KEY","value":"","sensitive":false}]),
        );
        let prepared = prepare(&previous, next);
        // Unmarked duplicate removals cannot erase the redacted row's saved slot.
        prepared.persist(dir.path(), |_| Ok(())).unwrap();
        let materialized = materialize(&prepared.disk, dir.path()).unwrap();
        let vars = &materialized["providerInstances"]["server"]["environment"];
        assert_eq!(vars[0]["value"], "new");
        assert_eq!(vars[1]["value"], "new");
        assert_eq!(vars[2]["value"], "");
        assert_eq!(
            redact(&materialized, true)["providerInstances"]["server"]["environment"][0],
            redacted
        );
    }
}
