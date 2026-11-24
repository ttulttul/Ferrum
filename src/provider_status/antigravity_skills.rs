//! Antigravity's skills for a folder, as the reference's
//! `discoverAntigravitySkills` (`Drivers/AntigravitySkills.ts`): the user and
//! project skill roots agy loads, the `skills.json` configs (with their
//! inherits and filters), and the skills of the instance's enabled plugins.
//! A read error or an exhausted scan budget fails the scan, so a folder
//! snapshot never caches a partial list.
use serde_json::{json, Value};
use std::collections::{BTreeMap, HashSet};
use std::io::Read;
use std::path::{Component, Path, PathBuf};

const MAX_SKILL_BYTES: usize = 1_000_000;
const MAX_SCAN_BYTES: usize = 8_000_000;
const MAX_SCAN_ENTRIES: usize = 10_000;
const MAX_INHERIT_DEPTH: usize = 10;
const SKILL_READ_CHUNK_BYTES: usize = 1024;

/// Why a scan failed (`AntigravitySkillsProbeError`).
#[derive(Debug)]
pub(super) struct ScanFailed;

struct Budget {
    bytes: usize,
    entries: usize,
}

#[derive(Clone, Default)]
struct Filter {
    include_groups: Vec<Vec<String>>,
    exclude: Vec<String>,
}

impl Filter {
    fn matches(&self, entry: &str) -> bool {
        self.include_groups
            .iter()
            .all(|group| group.iter().any(|g| g == entry))
            && !self.exclude.iter().any(|e| e == entry)
    }

    /// `narrowSkillFilter`.
    fn narrow(&self, entry: &Value) -> Filter {
        let strings = |key: &str| -> Vec<String> {
            entry[key]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(|v| v.as_str().map(str::to_owned))
                .collect()
        };
        let mut next = self.clone();
        let include = strings("include_only");
        if !include.is_empty() {
            next.include_groups.push(include);
        }
        next.exclude.extend(strings("exclude"));
        next
    }
}

/// `path.resolve`: lexical, with `.` and `..` folded.
fn resolve(base: &Path, path: &Path) -> PathBuf {
    let joined = base.join(path);
    let mut out = PathBuf::new();
    for component in joined.components() {
        match component {
            Component::ParentDir => {
                out.pop();
            }
            Component::CurDir => {}
            other => out.push(other.as_os_str()),
        }
    }
    out
}

/// A metadata read that treats a missing path as absent and any other
/// error as a failed scan (`readIfPresent`).
fn stat(path: &Path) -> Result<Option<std::fs::Metadata>, ScanFailed> {
    match std::fs::metadata(path) {
        Ok(meta) => Ok(Some(meta)),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(_) => Err(ScanFailed),
    }
}

fn read_dir(path: &Path) -> Result<Option<Vec<String>>, ScanFailed> {
    match std::fs::read_dir(path) {
        Ok(entries) => Ok(Some(
            entries
                .flatten()
                .filter_map(|e| e.file_name().into_string().ok())
                .collect(),
        )),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(_) => Err(ScanFailed),
    }
}

/// `skillPathSortKey`: Go's `URL.EscapedPath` encoding, as `encodeURI` plus
/// `!'()*?#`.
fn sort_key(entry: &str) -> String {
    const KEEP: &percent_encoding::AsciiSet = &percent_encoding::NON_ALPHANUMERIC
        .remove(b';')
        .remove(b',')
        .remove(b'/')
        .remove(b':')
        .remove(b'@')
        .remove(b'&')
        .remove(b'=')
        .remove(b'+')
        .remove(b'$')
        .remove(b'-')
        .remove(b'_')
        .remove(b'.')
        .remove(b'~');
    percent_encoding::utf8_percent_encode(entry, KEEP).to_string()
}

/// `readSkillFrontmatter`: the file through its second `---`, read in 1 KB
/// chunks; whole chunks spend the budget.
fn read_frontmatter(path: &Path, budget: &mut Budget) -> Result<Option<String>, ScanFailed> {
    let Some(meta) = stat(path)? else {
        return Ok(None);
    };
    if !meta.is_file() {
        return Ok(None);
    }
    let limit = MAX_SKILL_BYTES.min(budget.bytes);
    let mut file = match std::fs::File::open(path) {
        Ok(file) => file.take(limit as u64 + 1),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(_) => return Err(ScanFailed),
    };
    let mut bytes = Vec::new();
    let (mut dashes, mut markers, mut end) = (0, 0, None);
    let mut chunk = [0u8; SKILL_READ_CHUNK_BYTES];
    'read: loop {
        let read = file.read(&mut chunk).map_err(|_| ScanFailed)?;
        if read == 0 {
            break;
        }
        for &byte in &chunk[..read] {
            bytes.push(byte);
            dashes = if byte == b'-' { dashes + 1 } else { 0 };
            if dashes == 3 {
                dashes = 0;
                markers += 1;
                if markers == 2 && end.is_none() {
                    end = Some(bytes.len());
                }
            }
        }
        if end.is_some() {
            break 'read;
        }
    }
    budget.bytes = budget.bytes.saturating_sub(bytes.len());
    if end.unwrap_or(bytes.len()) > limit {
        return Err(ScanFailed);
    }
    let end = end.unwrap_or(bytes.len());
    Ok(Some(String::from_utf8_lossy(&bytes[..end]).into_owned()))
}

/// `parseSkillFrontmatter`: YAML between the first two `---`.
fn parse(contents: &str, file_name: &str) -> Option<(String, Option<String>)> {
    let start = contents.find("---")?;
    let end = contents[start + 3..].find("---")? + start + 3;
    let yaml: serde_yaml::Value = serde_yaml::from_str(contents[start + 3..end].trim()).ok()?;
    let record = match yaml {
        serde_yaml::Value::Null => serde_yaml::Mapping::new(),
        serde_yaml::Value::Mapping(map) => map,
        _ => return None,
    };
    let field = |key: &str| match record.get(key) {
        None | Some(serde_yaml::Value::Null) => Some(None),
        Some(serde_yaml::Value::String(text)) => Some(Some(text.clone())),
        Some(_) => None,
    };
    let name = field("name")?;
    let description = field("description")?;
    let name = name
        .filter(|n| !n.is_empty())
        .unwrap_or_else(|| file_name[..file_name.len() - 3].to_owned());
    // Native names are not trimmed; one that would need trimming is skipped.
    if name.is_empty() || name != name.trim() {
        return None;
    }
    let description = description
        .map(|d| d.trim().to_owned())
        .filter(|d| !d.is_empty());
    Some((name, description))
}

struct Scan {
    budget: Budget,
    skills: BTreeMap<String, Value>,
}

impl Scan {
    fn add_skill_file(&mut self, path: &Path, scope: &str) -> Result<(), ScanFailed> {
        let file_name = path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or_default();
        if file_name.to_lowercase() != "skill.md" || !file_name.ends_with(".md") {
            return Ok(());
        }
        let Some(contents) = read_frontmatter(path, &mut self.budget)? else {
            return Ok(());
        };
        let Some((name, description)) = parse(&contents, file_name) else {
            return Ok(());
        };
        if self.skills.contains_key(&name) {
            return Ok(());
        }
        let mut skill = json!({"name": name});
        if let Some(description) = description {
            skill["description"] = json!(description);
        }
        skill["path"] = json!(path.display().to_string());
        skill["scope"] = json!(scope);
        skill["enabled"] = json!(true);
        self.skills.insert(name, skill);
        Ok(())
    }

    /// `scanDirectory`.
    fn scan_directory(
        &mut self,
        directory: &Path,
        scope: &str,
        scan_children: bool,
        filter: &Filter,
        accept_skill_file: bool,
    ) -> Result<(), ScanFailed> {
        let Some(meta) = stat(directory)? else {
            return Ok(());
        };
        if meta.is_file() && accept_skill_file {
            if self.budget.entries == 0 {
                return Err(ScanFailed);
            }
            self.budget.entries -= 1;
            let parent = directory
                .parent()
                .and_then(|p| p.file_name())
                .and_then(|n| n.to_str())
                .unwrap_or_default();
            if filter.matches(parent) {
                self.add_skill_file(directory, scope)?;
            }
            return Ok(());
        }
        if !meta.is_dir() {
            return Ok(());
        }
        let Some(mut entries) = read_dir(directory)? else {
            return Ok(());
        };
        if entries.len() > self.budget.entries {
            return Err(ScanFailed);
        }
        self.budget.entries -= entries.len();
        entries.sort_by(|a, b| a.encode_utf16().cmp(b.encode_utf16()));
        if let Some(skill_file) = entries.iter().find(|e| e.to_lowercase() == "skill.md") {
            return self.add_skill_file(&directory.join(skill_file), scope);
        }
        if scan_children {
            let mut children: Vec<(String, String)> = entries
                .into_iter()
                .filter(|e| filter.matches(e))
                .map(|e| (sort_key(&e), e))
                .collect();
            children.sort();
            for (_, entry) in children {
                self.scan_directory(
                    &directory.join(entry),
                    scope,
                    false,
                    &Filter::default(),
                    false,
                )?;
            }
        }
        Ok(())
    }

    /// `scanSkillsConfig`.
    fn scan_config(
        &mut self,
        config_path: &Path,
        base: &Path,
        scope: &str,
        filter: &Filter,
        visited: &HashSet<PathBuf>,
        depth: usize,
    ) -> Result<(), ScanFailed> {
        if depth > MAX_INHERIT_DEPTH {
            return Ok(());
        }
        let resolved = resolve(Path::new("/"), config_path);
        if visited.contains(&resolved) {
            return Ok(());
        }
        let Some(meta) = stat(&resolved)? else {
            return Ok(());
        };
        if !meta.is_file() {
            return Ok(());
        }
        if meta.len() > MAX_SKILL_BYTES.min(self.budget.bytes) as u64 {
            return Err(ScanFailed);
        }
        let raw = match std::fs::read_to_string(&resolved) {
            Ok(raw) => raw,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(_) => return Err(ScanFailed),
        };
        self.budget.bytes = self.budget.bytes.saturating_sub(raw.len());
        let Some(parsed) = decode_config(&raw) else {
            return Ok(());
        };
        let mut next = visited.clone();
        next.insert(resolved);
        // agy does not expand `~`, so a `~/` path stays relative to the config.
        let at = |raw: &str| resolve(base, Path::new(raw));
        for inherited in parsed["inherits"].as_array().into_iter().flatten() {
            let path = at(inherited["path"].as_str().unwrap_or(""));
            let parent = path.parent().unwrap_or(Path::new("/")).to_path_buf();
            self.scan_config(
                &path,
                &parent,
                scope,
                &filter.narrow(inherited),
                &next,
                depth + 1,
            )?;
        }
        for entry in parsed["entries"].as_array().into_iter().flatten() {
            let directory = at(entry["path"].as_str().unwrap_or(""));
            self.scan_directory(&directory, scope, true, &filter.narrow(entry), true)?;
        }
        Ok(())
    }
}

/// `decodeSkillsConfig`: `{inherits?, entries?}` of `{path, include_only?, exclude?}`.
fn decode_config(raw: &str) -> Option<Value> {
    let value: Value = serde_json::from_str(raw).ok()?;
    let object = value.as_object()?;
    for key in ["inherits", "entries"] {
        match object.get(key) {
            None | Some(Value::Null) => {}
            Some(Value::Array(items)) => {
                for item in items {
                    let item = item.as_object()?;
                    item.get("path")?.as_str()?;
                    for list in ["include_only", "exclude"] {
                        match item.get(list) {
                            None | Some(Value::Null) => {}
                            Some(Value::Array(names)) if names.iter().all(Value::is_string) => {}
                            Some(_) => return None,
                        }
                    }
                }
            }
            Some(_) => return None,
        }
    }
    Some(value)
}

/// `discoverAntigravitySkills` for a folder, with the user's home and the
/// instance's GEMINI_HOME (`profileDirectory`).
pub(super) fn discover(
    cwd: &Path,
    user_home: &Path,
    profile: &Path,
) -> Result<Vec<Value>, ScanFailed> {
    let gemini = user_home.join(".gemini");
    let roots = [
        (gemini.join("config").join("skills"), "user"),
        (resolve(cwd, Path::new(".gemini/skills")), "project"),
        (gemini.join("antigravity-cli").join("skills"), "user"),
        (resolve(cwd, Path::new(".agents/skills")), "project"),
        (resolve(cwd, Path::new(".agent/skills")), "project"),
    ];
    let mut scan = Scan {
        budget: Budget {
            bytes: MAX_SCAN_BYTES,
            entries: MAX_SCAN_ENTRIES,
        },
        skills: BTreeMap::new(),
    };
    for (directory, scope) in &roots {
        scan.scan_directory(directory, scope, true, &Filter::default(), false)?;
    }
    let configs = [
        (
            resolve(cwd, Path::new(".agents/skills.json")),
            cwd.to_path_buf(),
            "project",
        ),
        (
            resolve(cwd, Path::new(".agent/skills.json")),
            cwd.to_path_buf(),
            "project",
        ),
        (
            gemini.join("config").join("skills.json"),
            gemini.join("config"),
            "user",
        ),
    ];
    for (config, base, scope) in &configs {
        scan.scan_config(config, base, scope, &Filter::default(), &HashSet::new(), 0)?;
    }
    let plugins_directory = profile.join("config").join("plugins");
    if let Some(mut plugins) = read_dir(&plugins_directory)?.filter(|p| !p.is_empty()) {
        let config_path = profile.join("config").join("config.json");
        let config_meta = stat(&config_path)?;
        if config_meta
            .as_ref()
            .is_some_and(|m| m.is_file() && m.len() > MAX_SKILL_BYTES as u64)
        {
            return Err(ScanFailed);
        }
        let config = match config_meta.filter(std::fs::Metadata::is_file) {
            Some(_) => match std::fs::read_to_string(&config_path) {
                Ok(text) => Some(text),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
                Err(_) => return Err(ScanFailed),
            },
            None => None,
        };
        let disabled: HashSet<String> = config
            .and_then(|text| serde_json::from_str::<Value>(&text).ok())
            .and_then(|v| v["plugins"].as_object().cloned())
            .map(|plugins| {
                plugins
                    .into_iter()
                    .filter(|(_, plugin)| plugin["enabled"] == false)
                    .map(|(name, _)| name)
                    .collect()
            })
            .unwrap_or_default();
        plugins.sort_by(|a, b| a.encode_utf16().cmp(b.encode_utf16()));
        for plugin in plugins {
            if disabled.contains(&plugin) {
                continue;
            }
            let directory = plugins_directory.join(&plugin);
            if !stat(&directory)?.is_some_and(|m| m.is_dir()) {
                continue;
            }
            scan.scan_directory(
                &directory.join("skills"),
                "user",
                true,
                &Filter::default(),
                false,
            )?;
        }
    }
    let mut skills: Vec<Value> = scan.skills.into_values().collect();
    skills.sort_by(|a, b| {
        super::claude_skills::locale_compare(
            a["name"].as_str().unwrap_or(""),
            b["name"].as_str().unwrap_or(""),
        )
    });
    Ok(skills)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frontmatter_and_sort_keys_follow_agy() {
        assert_eq!(
            parse("---\nname: ship\ndescription:  Deploys \n---", "SKILL.md"),
            Some(("ship".into(), Some("Deploys".into())))
        );
        assert_eq!(parse("---\n---", "SKILL.md"), Some(("SKILL".into(), None)));
        assert_eq!(parse("---\nname: \" padded\"\n---", "SKILL.md"), None);
        assert_eq!(parse("---\nname: 3\n---", "SKILL.md"), None);
        assert_eq!(parse("no marker", "SKILL.md"), None);
        assert_eq!(sort_key("a b!(c)"), "a%20b%21%28c%29");
    }
}
