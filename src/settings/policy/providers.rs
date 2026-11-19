use super::*;
use std::collections::HashSet;

pub(super) const DRIVERS: [(&str, &str, &[&str]); 11] = [
    (
        "codex",
        "Codex",
        &["binaryPath", "launchArgs", "homePath", "shadowHomePath"],
    ),
    (
        "claudeAgent",
        "Claude",
        &["binaryPath", "launchArgs", "homePath"],
    ),
    ("antigravity", "Antigravity", &["binaryPath"]),
    ("cursor", "Cursor", &["binaryPath"]),
    ("grok", "Grok", &["binaryPath"]),
    ("fx", "fx", &["binaryPath"]),
    ("kiro", "Kiro", &["binaryPath"]),
    ("muse", "Muse Code", &["binaryPath"]),
    ("opencode", "OpenCode", &["binaryPath", "serverUrl"]),
    ("pi", "Pi", &["binaryPath", "launchArgs"]),
    (
        "acpRegistry",
        "ACP Registry",
        &["commandPath", "commandArgs"],
    ),
];
fn known(driver: &str) -> Option<(&'static str, &'static [&'static str])> {
    DRIVERS.iter().find(|d| d.0 == driver).map(|d| (d.1, d.2))
}
fn name(s: &str) -> String {
    if cfg!(windows) {
        s.to_ascii_uppercase()
    } else {
        s.into()
    }
}
fn enabled(v: &Value) -> bool {
    let envelope = v["enabled"].as_bool();
    let config = v["config"]["enabled"].as_bool();
    envelope != Some(false)
        && config != Some(false)
        && envelope.or(config).unwrap_or_else(|| {
            !matches!(
                v["driver"].as_str(),
                Some(
                    "cursor" | "grok" | "fx" | "kiro" | "muse" | "pi" | "opencode" | "antigravity"
                )
            )
        })
}
fn nonempty(v: Option<&Value>) -> Option<&Value> {
    v.filter(|v| v.as_str() != Some("") && !v.as_array().is_some_and(Vec::is_empty))
}
#[derive(Debug, Default)]
pub(super) struct Rules {
    allowed: Option<Vec<String>>,
    rules: Vec<Rule>,
}
#[derive(Debug)]
struct Rule {
    driver: String,
    pin: Option<Value>,
    locked: Vec<String>,
    fill: Vec<String>,
    all_env: bool,
    locked_env: bool,
    variables: Vec<Value>,
}
impl Rules {
    pub(super) fn new(limits: Option<&Value>, ceiling: bool) -> Result<Self> {
        let mut rules = Self::default();
        if let Some(v) = limits {
            limited(v, &["allowedDrivers", "instances"])?;
        }
        let limits = limits.unwrap_or(&Value::Null);
        if let Some(allowed) = limits.get("allowedDrivers") {
            let list = allowed
                .as_array()
                .filter(|a| !a.is_empty())
                .context("invalid allowedDrivers")?;
            let mut items = Vec::new();
            for raw in list {
                let d = raw.as_str().context("invalid driver")?;
                ensure!(
                    known(d).is_some(),
                    "\"{d}\" in allowedDrivers is not a known provider"
                );
                items.push(d.to_owned());
            }
            rules.allowed = Some(items);
        }
        if let Some(pins) = limits.get("instances") {
            for (driver, pin) in object(pins)? {
                let (label, supported) = known(driver)
                    .ok_or_else(|| anyhow!("\"{driver}\" in instances is not a known provider"))?;
                limited(pin, &["locked", "binaryPath", "launchArgs", "environment"])?;
                ensure!(pin["locked"].is_boolean(), "invalid provider lock");
                for key in ["binaryPath", "launchArgs"] {
                    if let Some(v) = pin.get(key) {
                        ensure!(
                            v.as_str().is_some_and(|s| !s.trim().is_empty()),
                            "invalid provider {key}"
                        );
                        ensure!(
                            supported.contains(&key),
                            "the {label} provider has no {key} setting"
                        );
                    }
                }
                if let Some(env) = pin.get("environment") {
                    let mut seen = HashSet::new();
                    for var in env.as_array().context("invalid provider environment")? {
                        limited(var, &["name", "value"])?;
                        let n = var["name"].as_str().context("missing variable name")?;
                        let mut bytes = n.bytes();
                        ensure!(
                            n.len() <= 128
                                && bytes
                                    .next()
                                    .is_some_and(|b| b.is_ascii_alphabetic() || b == b'_')
                                && bytes.all(|b| b.is_ascii_alphanumeric() || b == b'_')
                                && var["value"].is_string(),
                            "invalid provider environment"
                        );
                        ensure!(
                            seen.insert(name(n)),
                            "instances.{driver}.environment repeats a variable"
                        );
                    }
                }
            }
        }
        for (driver, _, fields) in DRIVERS {
            let pin = limits["instances"].get(driver);
            if !ceiling && pin.is_none() {
                continue;
            }
            let mut rule = Rule {
                driver: driver.into(),
                pin: pin.cloned(),
                locked: Vec::new(),
                fill: Vec::new(),
                all_env: ceiling,
                locked_env: pin.is_some_and(|p| p["locked"] == true),
                variables: Vec::new(),
            };
            if let Some(pin) = pin {
                for var in list(&pin["environment"]) {
                    rule.variables
                        .push(json!({"name":var["name"],"value":var["value"],"sensitive":false}));
                }
            }
            for field in fields {
                if ceiling {
                    rule.locked.push(field.to_string());
                } else if pin.is_some_and(|p| p.get(field).is_some()) {
                    if rule.locked_env {
                        rule.locked.push(field.to_string())
                    } else {
                        rule.fill.push(field.to_string())
                    }
                }
            }
            if pin.is_some() && (ceiling || rule.locked_env) {
                match driver {
                    "codex" => rule.locked.push("setupMode".into()),
                    "opencode" => rule.locked.push("serverUrl".into()),
                    _ => (),
                }
                if !ceiling {
                    let (home, fields) = match driver {
                        "codex" => ("CODEX_HOME", &["homePath", "shadowHomePath"][..]),
                        "claudeAgent" => ("CLAUDE_CONFIG_DIR", &["homePath"][..]),
                        _ => ("", &[][..]),
                    };
                    if rule
                        .variables
                        .iter()
                        .any(|v| name(v["name"].as_str().unwrap()) == name(home))
                    {
                        rule.locked.extend(fields.iter().map(|s| s.to_string()));
                    }
                }
            }
            rules.rules.push(rule);
        }
        Ok(rules)
    }
    pub(super) fn summary(&self) -> Option<Value> {
        let mut public = json!({});
        if let Some(allowed) = &self.allowed {
            public["allowedDrivers"] = json!(allowed);
        }
        let mut fields = json!({});
        for rule in &self.rules {
            let mut paths = rule
                .locked
                .iter()
                .map(|s| format!("config.{s}"))
                .collect::<Vec<_>>();
            if rule.all_env {
                paths.push("environment".into());
            } else if rule.locked_env {
                paths.extend(
                    rule.variables
                        .iter()
                        .map(|v| format!("environment.{}", v["name"].as_str().unwrap())),
                );
            }
            if !paths.is_empty() {
                fields[&rule.driver] = json!(paths);
            }
        }
        if fields != json!({}) {
            public["lockedInstanceFields"] = fields;
        }
        (public != json!({})).then_some(public)
    }
    pub(super) fn denial(&self, org: &str, driver: &str) -> Option<String> {
        self.allowed
            .as_ref()
            .is_some_and(|a| !a.iter().any(|d| d == driver))
            .then(|| {
                format!(
                    "{org}'s policy does not allow the {} provider.",
                    known(driver).map_or(driver, |v| v.0)
                )
            })
    }
    fn disallowed(&self, policy: &Policy, id: &str, driver: &str) -> Option<Value> {
        self.denial(policy.org(),driver).map(|_|json!({"operation":"use-disallowed-provider","providerInstanceId":id,"managedBy":policy.org(),"providerLabel":known(driver).map_or(driver,|d|d.0)}))
    }
    pub(super) fn overlay(&self, next: &mut Value) {
        if self.rules.is_empty() && self.allowed.is_none() {
            return;
        }
        for rule in &self.rules {
            if rule.pin.is_some()
                && rule.driver != "acpRegistry"
                && next["providerInstances"].get(&rule.driver).is_none()
            {
                next["providerInstances"][&rule.driver] = json!({"driver":rule.driver});
            }
        }
        if let Some(instances) = next["providerInstances"].as_object_mut() {
            for instance in instances.values_mut() {
                let Some(rule) = self.rules.iter().find(|r| instance["driver"] == r.driver) else {
                    continue;
                };
                rule.overlay(instance);
            }
        }
        if self.allowed.is_some() {
            next["textGenerators"] = json!({});
            for key in [
                "sourceControlWriterModelSelection",
                "reviewGuideModelSelection",
            ] {
                if let Some(id) = next[key]["instanceId"].as_str() {
                    if self
                        .denial(
                            "",
                            next["providerInstances"][id]["driver"]
                                .as_str()
                                .unwrap_or(id),
                        )
                        .is_some()
                    {
                        next[key] = Value::Null;
                    }
                }
            }
        }
    }
    pub(super) fn vet(
        &self,
        policy: &Policy,
        current: &Value,
        next: &mut Value,
    ) -> Result<(), Value> {
        if let Some(instances) = next["providerInstances"].as_object_mut() {
            let mut remove = Vec::new();
            for (id, inst) in instances.iter_mut() {
                let driver = inst["driver"].as_str().unwrap_or("").to_owned();
                let stored = current["providerInstances"].get(id);
                let was_enabled =
                    stored.map_or(id == &driver, |s| s["driver"] == driver && enabled(s));
                if !was_enabled && enabled(inst) {
                    if let Some(e) = self.disallowed(policy, id, &driver) {
                        return Err(e);
                    }
                }
                if let Some(rule) = self.rules.iter().find(|r| r.driver == driver) {
                    let stored = stored.filter(|s| s["driver"] == driver);
                    rule.vet(policy, id, stored.unwrap_or(&Value::Null), inst)?;
                    if stored.is_none()
                        && id == &driver
                        && inst.as_object().is_some_and(|m| m.len() == 1)
                    {
                        remove.push(id.to_owned());
                    }
                }
            }
            for id in remove {
                instances.shift_remove(&id);
            }
        }
        for (project, p) in dict(&next["projectSettingsOverrides"]) {
            for (id, v) in dict(&p["providerInstances"]) {
                if v["enabled"] == true
                    && current["projectSettingsOverrides"][project]["providerInstances"][id]
                        ["enabled"]
                        != true
                {
                    if let Some(e) = self.disallowed(
                        policy,
                        id,
                        next["providerInstances"][id]["driver"]
                            .as_str()
                            .unwrap_or(id),
                    ) {
                        return Err(e);
                    }
                }
            }
        }
        Ok(())
    }
}
impl Rule {
    fn pinned(&self, field: &str) -> Option<&Value> {
        self.pin.as_ref().and_then(|p| p.get(field))
    }
    fn hides(&self, var: &Value) -> bool {
        self.locked.iter().any(|v| v == "launchArgs")
            && name(var["name"].as_str().unwrap_or("")) == "T3CODE_CODEX_LAUNCH_ARGS"
    }
    fn overlay(&self, inst: &mut Value) {
        let mut config = inst["config"].as_object().cloned().unwrap_or_default();
        for k in &self.locked {
            if let Some(pin) = self.pinned(k) {
                config.insert(k.clone(), pin.clone());
            } else {
                config.shift_remove(k);
            }
        }
        for k in &self.fill {
            if nonempty(config.get(k)).is_none() {
                config.insert(k.clone(), self.pinned(k).unwrap().clone());
            }
        }
        let mut environment = Vec::new();
        let mut seen = HashSet::new();
        if !self.all_env {
            for v in list(&inst["environment"]) {
                if self.hides(v) {
                    continue;
                }
                let n = name(v["name"].as_str().unwrap_or(""));
                let pin = self
                    .variables
                    .iter()
                    .find(|p| name(p["name"].as_str().unwrap()) == n);
                if let Some(pin) = pin.filter(|_| self.locked_env) {
                    if !seen.contains(&n) {
                        environment.push(pin.clone());
                    }
                } else {
                    environment.push(v.clone());
                }
                seen.insert(n);
            }
        }
        for v in &self.variables {
            if !seen.contains(&name(v["name"].as_str().unwrap())) {
                environment.push(v.clone());
            }
        }
        let row = inst.as_object_mut().unwrap();
        let had = row.shift_remove("config").is_some();
        row.shift_remove("environment");
        if had || !config.is_empty() {
            row.insert("config".into(), Value::Object(config));
        }
        if !environment.is_empty() {
            row.insert("environment".into(), json!(environment));
        }
    }
    fn vet(
        &self,
        policy: &Policy,
        id: &str,
        stored: &Value,
        next: &mut Value,
    ) -> Result<(), Value> {
        let mut config = next["config"].as_object().cloned().unwrap_or_default();
        for k in self.locked.iter().chain(&self.fill) {
            let value = nonempty(config.get(k));
            let before = nonempty(stored["config"].get(k));
            let harmless =
                (self.driver == "codex" && k == "setupMode" && value == Some(&json!("existing")))
                    || (self.driver == "opencode" && k == "serverUrl" && value == Some(&json!("")));
            if value == self.pinned(k) || value == before || harmless {
                if let Some(old) = before {
                    config.insert(k.clone(), old.clone());
                } else {
                    config.shift_remove(k);
                }
            } else if self.locked.contains(k) {
                return Err(policy.locked(&format!("providerInstances.{id}.config.{k}")));
            }
        }
        let old = list(&stored["environment"]).cloned().collect::<Vec<_>>();
        let mut env = list(&next["environment"]).cloned().collect::<Vec<_>>();
        let echoed = |v: &Value| {
            v["sensitive"] != true
                && self
                    .variables
                    .iter()
                    .any(|p| p["name"] == v["name"] && p["value"] == v["value"])
        };
        if self.all_env {
            if env.iter().any(|v| {
                !echoed(v)
                    && !old
                        .iter()
                        .any(|s| s["value"] == v["value"] && s["sensitive"] == v["sensitive"])
            }) {
                return Err(policy.locked(&format!("providerInstances.{id}.environment")));
            }
            env = old;
        } else {
            for pin in &self.variables {
                let n = pin["name"].as_str().unwrap();
                let same = |v: &Value| name(v["name"].as_str().unwrap_or("")) == name(n);
                let sent = env.iter().find(|v| same(v));
                let kept = old.iter().find(|v| same(v));
                if sent.is_some_and(echoed) || sent == kept {
                    let mut restored = false;
                    let mut entries = Vec::new();
                    for v in env {
                        if same(&v) {
                            if !restored {
                                entries.extend(old.iter().filter(|s| same(s)).cloned());
                                restored = true;
                            }
                        } else {
                            entries.push(v);
                        }
                    }
                    env = entries;
                } else if self.locked_env {
                    return Err(policy.locked(&format!("providerInstances.{id}.environment.{n}")));
                }
            }
            if self.locked.iter().any(|v| v == "launchArgs") {
                env.retain(|v| !self.hides(v));
                env.extend(old.into_iter().filter(|v| self.hides(v)));
            }
        }
        let row = next.as_object_mut().unwrap();
        row.shift_remove("config");
        row.shift_remove("environment");
        if stored.get("config").is_some() || !config.is_empty() {
            row.insert("config".into(), Value::Object(config));
        }
        if !env.is_empty() {
            row.insert("environment".into(), json!(env));
        }
        Ok(())
    }
}
