//! Cursor's skills for a folder, as the reference's `probeCursorSkills`
//! (`provider-cursor/src/server/skills.ts`): the project and user skill roots
//! and Cursor's synced built-ins, walked without opening an ACP session. A
//! walk that runs out of budget or hits a read error fails the scan.
use super::ProcessEnv;
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::OnceLock;

const MAX_SKILL_DEPTH: usize = 10;
const MAX_SKILL_BYTES: u64 = 1_000_000;
const MAX_SCAN_ENTRIES: usize = 10_000;
const MAX_SCAN_STATS: usize = 100_000;
const MAX_SCAN_BYTES: u64 = 8_000_000;

struct Budget {
    entries: usize,
    stats: usize,
    bytes: u64,
    exhausted: bool,
    incomplete: bool,
}

/// A metadata read: a missing path is absent; any other error leaves the
/// scan incomplete (`orUndefined`).
fn read<T>(result: std::io::Result<T>, budget: &mut Budget) -> Option<T> {
    match result {
        Ok(value) => Some(value),
        Err(error) => {
            if error.kind() != std::io::ErrorKind::NotFound {
                budget.incomplete = true;
            }
            None
        }
    }
}

/// YAML 1.1 booleans (`parseFrontmatterBoolean`; Cursor takes no `y`/`n`).
fn boolean(value: Option<&serde_yaml::Value>) -> Option<bool> {
    match value? {
        serde_yaml::Value::Bool(b) => Some(*b),
        serde_yaml::Value::Number(n) => match n.as_f64() {
            Some(1.0) => Some(true),
            Some(0.0) => Some(false),
            _ => None,
        },
        serde_yaml::Value::String(s) => match s.trim().to_lowercase().as_str() {
            "true" | "yes" | "on" => Some(true),
            "false" | "no" | "off" => Some(false),
            _ => None,
        },
        _ => None,
    }
}

#[derive(Default)]
struct Frontmatter {
    cli_visible: bool,
    description: Option<String>,
    display_name: Option<String>,
    user_invocation_only: bool,
    not_user_invocable: bool,
}

/// `parseSkillFrontmatter`: `None` for frontmatter that does not parse.
fn parse(contents: &str) -> Option<Frontmatter> {
    static PATTERN: OnceLock<regex::Regex> = OnceLock::new();
    let pattern = PATTERN
        .get_or_init(|| regex::Regex::new(r"^---\r?\n((?s:.*?))\r?\n---(?:\r?\n|$)").unwrap());
    let Some(captures) = pattern.captures(contents) else {
        return Some(Frontmatter {
            cli_visible: true,
            ..Default::default()
        });
    };
    let parsed: serde_yaml::Value = serde_yaml::from_str(&captures[1]).ok()?;
    let serde_yaml::Value::Mapping(record) = parsed else {
        return None;
    };
    let surfaces: Vec<String> = match record.get("metadata").and_then(|m| m.get("surfaces")) {
        Some(serde_yaml::Value::Sequence(items)) => items
            .iter()
            .filter_map(|s| s.as_str().map(str::to_owned))
            .collect(),
        Some(serde_yaml::Value::String(list)) => list.split(',').map(str::to_owned).collect(),
        _ => vec![],
    };
    let text = |key: &str| {
        record
            .get(key)
            .and_then(serde_yaml::Value::as_str)
            .map(str::trim)
            .filter(|v| !v.is_empty())
            .map(str::to_owned)
    };
    Some(Frontmatter {
        cli_visible: surfaces.is_empty()
            || surfaces.iter().any(|s| s.trim().to_lowercase() == "cli"),
        description: text("description"),
        display_name: text("name"),
        user_invocation_only: boolean(record.get("disable-model-invocation")) == Some(true),
        not_user_invocable: boolean(record.get("user-invocable")) == Some(false),
    })
}

/// `discoverSkillsInRoot`.
fn root_skills(
    directory: &Path,
    scope: &str,
    budget: &mut Budget,
    scanned: &mut HashSet<PathBuf>,
) -> Vec<Value> {
    let mut skills = Vec::new();
    if budget.exhausted {
        return skills;
    }
    let Some(root) = read(std::fs::canonicalize(directory), budget) else {
        return skills;
    };
    // Symlinked roots may share a tree; its budget is not charged twice.
    if !scanned.insert(root.clone()) {
        return skills;
    }
    let mut visited = HashSet::new();
    visit(&root, &root, 0, scope, budget, &mut visited, &mut skills);
    skills
}

fn visit(
    root: &Path,
    directory: &Path,
    depth: usize,
    scope: &str,
    budget: &mut Budget,
    visited: &mut HashSet<PathBuf>,
    skills: &mut Vec<Value>,
) {
    if budget.exhausted {
        return;
    }
    let Some(resolved) = read(std::fs::canonicalize(directory), budget) else {
        return;
    };
    if !visited.insert(resolved.clone()) {
        return;
    }
    // A symlink out of the root exposes only its own SKILL.md.
    let inside = resolved.starts_with(root);
    if budget.stats == 0 {
        budget.exhausted = true;
        return;
    }
    budget.stats -= 1;
    let skill_path = directory.join("SKILL.md");
    if let Some(meta) = read(std::fs::metadata(&skill_path), budget) {
        if meta.is_file() {
            let mut frontmatter = Some(Frontmatter {
                cli_visible: true,
                ..Default::default()
            });
            if meta.len() <= MAX_SKILL_BYTES && meta.len() <= budget.bytes {
                if let Ok(bytes) = std::fs::read(&skill_path) {
                    budget.bytes -= meta.len();
                    frontmatter = parse(&String::from_utf8_lossy(&bytes));
                }
            }
            let name = directory
                .file_name()
                .map(|n| n.to_string_lossy().trim().to_owned())
                .unwrap_or_default();
            if let Some(frontmatter) = frontmatter.filter(|f| f.cli_visible && !name.is_empty()) {
                let mut skill = json!({"name": name, "path": skill_path.display().to_string(),
                    "scope": scope, "enabled": true});
                if let Some(display) = frontmatter.display_name.filter(|d| *d != name) {
                    skill["displayName"] = json!(display);
                }
                if let Some(description) = frontmatter.description {
                    skill["description"] = json!(description);
                }
                if frontmatter.user_invocation_only {
                    skill["userInvocationOnly"] = json!(true);
                }
                if frontmatter.not_user_invocable {
                    skill["userInvocable"] = json!(false);
                }
                skills.push(skill);
            }
        }
    }
    if !inside {
        return;
    }
    let Some(entries) = read(std::fs::read_dir(directory), budget) else {
        return;
    };
    let mut names: Vec<String> = entries
        .flatten()
        .filter_map(|e| e.file_name().into_string().ok())
        .collect();
    names.sort_by(|a, b| a.encode_utf16().cmp(b.encode_utf16()));
    for entry in names {
        // Installed dependencies would exhaust the shared budget.
        if entry == "node_modules" {
            continue;
        }
        if budget.stats == 0 {
            budget.exhausted = true;
            return;
        }
        budget.stats -= 1;
        let child = directory.join(&entry);
        if !read(std::fs::metadata(&child), budget).is_some_and(|m| m.is_dir()) {
            continue;
        }
        if depth >= MAX_SKILL_DEPTH {
            continue;
        }
        if budget.entries == 0 {
            budget.exhausted = true;
            return;
        }
        budget.entries -= 1;
        visit(root, &child, depth + 1, scope, budget, visited, skills);
    }
}

/// `probeCursorSkills` for a folder: `None` when the walk was incomplete.
pub(super) fn probe(cwd: &str, env: &ProcessEnv) -> Option<Vec<Value>> {
    let trimmed = |key: &str| {
        env.get(key)
            .map(|v| v.trim().to_owned())
            .filter(|v| !v.is_empty())
    };
    let home = trimmed("HOME")
        .or_else(|| trimmed("USERPROFILE"))
        .map(PathBuf::from)
        .unwrap_or_else(super::home_dir);
    let below = |base: &Path, scope: &'static str| {
        [".cursor", ".agents", ".codex", ".claude"]
            .map(|dot| (base.join(dot).join("skills"), scope))
    };
    let mut roots: Vec<(PathBuf, &str)> = below(Path::new(cwd), "project").to_vec();
    roots.extend(below(&home, "user"));
    // Cursor syncs its built-in skills here; same-named user and project skills win.
    roots.push((home.join(".cursor").join("skills-cursor"), "system"));
    let mut budget = Budget {
        entries: MAX_SCAN_ENTRIES,
        stats: MAX_SCAN_STATS,
        bytes: MAX_SCAN_BYTES,
        exhausted: false,
        incomplete: false,
    };
    let mut scanned = HashSet::new();
    let mut by_name: HashMap<String, Value> = HashMap::new();
    for (directory, scope) in roots {
        if budget.exhausted {
            break;
        }
        for skill in root_skills(&directory, scope, &mut budget, &mut scanned) {
            let name = skill["name"].as_str().unwrap_or("").to_owned();
            by_name.entry(name).or_insert(skill);
        }
    }
    if budget.exhausted || budget.incomplete {
        return None;
    }
    let mut skills: Vec<Value> = by_name.into_values().collect();
    skills.sort_by(|a, b| {
        super::claude_skills::locale_compare(
            a["name"].as_str().unwrap_or(""),
            b["name"].as_str().unwrap_or(""),
        )
    });
    Some(skills)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frontmatter_follows_cursor() {
        let f = parse("---\nname: Ship It\ndescription: Deploys\nmetadata:\n  surfaces: cli, ide\nuser-invocable: off\n---\n")
            .unwrap();
        assert!(f.cli_visible && f.not_user_invocable && !f.user_invocation_only);
        assert_eq!(f.display_name.as_deref(), Some("Ship It"));
        assert!(
            !parse("---\nmetadata:\n  surfaces: [ide]\n---\n")
                .unwrap()
                .cli_visible
        );
        assert!(parse("---\n[\n---\n").is_none());
        assert!(parse("plain").unwrap().cli_visible);
    }
}
