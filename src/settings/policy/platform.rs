use super::*;

pub(super) fn system_path() -> &'static Path {
    Path::new(if cfg!(target_os = "macos") {
        "/Library/Application Support/T3Code/policy.json"
    } else if cfg!(windows) {
        r"C:\Program Files\T3Code\policy.json"
    } else {
        "/etc/t3code/policy.json"
    })
}
#[cfg(not(any(windows, target_os = "macos")))]
pub(super) fn overlay() -> Result<Option<(String, Value)>> {
    Ok(None)
}
#[cfg(target_os = "macos")]
pub(super) fn overlay() -> Result<Option<(String, Value)>> {
    let path = "/Library/Managed Preferences/com.t3tools.t3code.plist";
    if !Path::new(path).try_exists()? {
        return Ok(None);
    }
    let output = std::process::Command::new("/usr/bin/plutil")
        .args(["-convert", "json", "-o", "-", path])
        .output()?;
    ensure!(
        output.status.success(),
        "the profile at {path} cannot be read"
    );
    let mut v: Value = serde_json::from_slice(&output.stdout)?;
    fn drop_metadata(v: &mut Value) {
        if let Some(m) = v.as_object_mut() {
            m.retain(|k, _| !k.starts_with("Payload") && !k.starts_with('_'));
        }
    }
    drop_metadata(&mut v);
    if v.as_object().is_some_and(|m| m.len() == 1)
        && v["Forced"].as_array().is_some_and(|a| a.len() == 1)
        && v["Forced"][0]
            .as_object()
            .is_some_and(|m| m.len() == 1 && m.contains_key("mcx_preference_settings"))
    {
        v = v["Forced"][0]["mcx_preference_settings"].take();
        drop_metadata(&mut v);
    }
    Ok(Some((path.into(), v)))
}
#[cfg(windows)]
pub(super) fn overlay() -> Result<Option<(String, Value)>> {
    use winreg::{
        enums::{HKEY_LOCAL_MACHINE, KEY_READ, KEY_WOW64_64KEY, REG_DWORD, REG_SZ},
        types::FromRegValue,
        RegKey, RegValue,
    };
    const KEY: &str = r"SOFTWARE\Policies\T3Tools\T3Code";
    let source = format!("HKLM\\{KEY}");
    let access = KEY_READ | KEY_WOW64_64KEY;
    let root = match RegKey::predef(HKEY_LOCAL_MACHINE).open_subkey_with_flags(KEY, access) {
        Ok(k) => k,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e.into()),
    };
    let root_values = root.enum_values().collect::<std::io::Result<Vec<_>>>()?;
    let whole = root_values
        .iter()
        .find(|(n, _)| n.eq_ignore_ascii_case("policy"));
    let mut child_values = Vec::new();
    for child in root.enum_keys() {
        let child = child?;
        ensure!(
            whole.is_some()
                || child.eq_ignore_ascii_case("clients")
                || child.eq_ignore_ascii_case("alloweddrivers"),
            "{child} is not a T3 Code policy key"
        );
        let key = root.open_subkey_with_flags(&child, access)?;
        ensure!(
            key.enum_keys().next().is_none(),
            "{child} contains an unknown T3 Code policy key"
        );
        for pair in key.enum_values() {
            let (name, value) = pair?;
            child_values.push((child.clone(), name, value));
        }
    }
    if let Some((_, raw)) = whole {
        ensure!(
            root_values.len() == 1 && child_values.is_empty(),
            "both a Policy value and Group Policy settings are installed"
        );
        ensure!(raw.vtype == REG_SZ, "Policy is not a REG_SZ");
        return Ok(Some((
            source,
            serde_json::from_str(&String::from_reg_value(raw)?)?,
        )));
    }
    let bool_value = |v: &RegValue| -> Result<bool> {
        ensure!(v.vtype == REG_DWORD, "expected a DWORD");
        match u32::from_reg_value(v)? {
            0 => Ok(false),
            1 => Ok(true),
            _ => anyhow::bail!("expected a DWORD of 0 or 1"),
        }
    };
    let mut doc = json!({"schemaVersion":1});
    let mut groups = std::collections::HashMap::new();
    let mut lists = std::collections::BTreeMap::<String, Vec<String>>::new();
    for (name, raw) in &root_values {
        let n = name.to_ascii_lowercase();
        if matches!(n.as_str(), "updates" | "alloweddrivers" | "clients") {
            let enabled = bool_value(raw)?;
            if enabled && n != "updates" {
                lists.insert(n.clone(), vec![]);
            }
            groups.insert(n, enabled);
        }
    }
    for (name, raw) in root_values {
        let n = name.to_ascii_lowercase();
        if groups.contains_key(&n) {
            continue;
        }
        let (path, typ) = match n.as_str() {
            "organization" => ("organization", 0),
            "supporturl" => ("supportUrl", 0),
            "settings" => ("settings", 1),
            "maxruntimemode" => ("limits.maxRuntimeMode", 0),
            "connect" => ("limits.connect", 2),
            "webhooktasks" => ("limits.webhookTasks", 2),
            "telemetry" => ("limits.telemetry", 2),
            "browsercookieimport" => ("limits.browserCookieImport", 2),
            "providerinstances" => ("limits.providers.instances", 1),
            "updatesmode" => ("limits.updates.mode", 0),
            "updatesfeedurl" => ("limits.updates.feedUrl", 0),
            "allownightly" => ("limits.updates.allowNightly", 2),
            _ => anyhow::bail!("{name} is not a T3 Code policy value"),
        };
        let value = if typ == 2 {
            json!(bool_value(&raw)?)
        } else {
            ensure!(raw.vtype == REG_SZ, "{name} must be REG_SZ");
            let text = String::from_reg_value(&raw)?;
            if text.is_empty() {
                continue;
            }
            if typ == 1 {
                serde_json::from_str(&text)?
            } else {
                json!(text)
            }
        };
        if path.starts_with("limits.updates.") && groups.get("updates") == Some(&false) {
            continue;
        }
        set(
            &mut doc,
            &path.split('.').map(str::to_owned).collect::<Vec<_>>(),
            Some(&value),
        );
    }
    for (group, name, raw) in child_values {
        let g = group.to_ascii_lowercase();
        ensure!(
            matches!(g.as_str(), "clients" | "alloweddrivers"),
            "{group} is not a T3 Code policy key"
        );
        let list = lists.entry(g.clone()).or_default();
        if bool_value(&raw)? {
            let item = if g == "clients" {
                ["local-browser", "desktop", "remote"]
                    .into_iter()
                    .find(|x| x.eq_ignore_ascii_case(&name))
            } else {
                providers::DRIVERS
                    .iter()
                    .map(|d| d.0)
                    .find(|x| x.eq_ignore_ascii_case(&name))
            };
            list.push(item.unwrap_or(&name).into());
        }
    }
    for (g, list) in lists {
        if groups.get(&g) != Some(&false) {
            if g == "clients" {
                doc["limits"]["clients"] = json!(list)
            } else {
                doc["limits"]["providers"]["allowedDrivers"] = json!(list)
            }
        }
    }
    Ok((doc.as_object().unwrap().len() > 1).then_some((source, doc)))
}
