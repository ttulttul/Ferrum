//! The skills a Claude status row lists, as the reference's `discoverClaudeSkills`
//! (`ClaudeSkills.ts`): `SKILL.md` folders under the managed, user and project
//! skill directories, `--add-dir` launch directories, inline plugin folders
//! (`CLAUDE_CODE_PLUGIN_DIRS`) and the installed and claude.ai-synced plugins
//! enabled in Claude's settings files, with `skillOverrides` applied. The first
//! skill of a name wins, and the list is sorted by name.
use super::{expand_home, home_dir, ProcessEnv};
use regex::Regex;
use serde_json::{json, Map, Value};
use std::collections::{HashMap, HashSet};
use std::path::{Component, Path, PathBuf};
use std::sync::OnceLock;
use unicode_normalization::UnicodeNormalization;

#[derive(Clone)]
struct Root {
    directory: PathBuf,
    scope: &'static str,
    prefix: Option<String>,
}

// ---------------------------------------------------------------------------
// Paths

/// `path.resolve(base, path)`: lexical, with `.` and `..` folded away.
fn resolve(base: &Path, path: &Path) -> PathBuf {
    let joined = if path.is_absolute() {
        path.to_path_buf()
    } else if base.is_absolute() {
        base.join(path)
    } else {
        std::env::current_dir()
            .unwrap_or_default()
            .join(base)
            .join(path)
    };
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

/// `isInsideDirectory`: the directory itself or anything under it.
fn inside(directory: &Path, candidate: &Path) -> bool {
    candidate.starts_with(directory)
}

fn read_text(path: &Path) -> Option<String> {
    std::fs::read(path)
        .ok()
        .map(|bytes| String::from_utf8_lossy(&bytes).into_owned())
}

/// A directory's entry names, sorted as JavaScript sorts strings.
fn sorted_entries(directory: &Path) -> Vec<String> {
    let mut names: Vec<String> = std::fs::read_dir(directory)
        .map(|entries| {
            entries
                .flatten()
                .filter_map(|entry| entry.file_name().into_string().ok())
                .collect()
        })
        .unwrap_or_default();
    names.sort_by(|a, b| js_compare(a, b));
    names
}

/// JavaScript's default string order: by UTF-16 code unit.
fn js_compare(a: &str, b: &str) -> std::cmp::Ordering {
    a.encode_utf16().cmp(b.encode_utf16())
}

/// `localeCompare` on skill names: ICU's root order for the characters a
/// skill name can hold (`_`, `-`, `:` and other punctuation before digits,
/// digits before letters, letters case-blind), then lower case before upper.
fn locale_compare(a: &str, b: &str) -> std::cmp::Ordering {
    fn weight(c: char) -> (u32, u32) {
        match c {
            '_' => (0, 0),
            '-' => (0, 1),
            ',' => (0, 2),
            ';' => (0, 3),
            ':' => (0, 4),
            '!' => (0, 5),
            '?' => (0, 6),
            '.' => (0, 7),
            '0'..='9' => (2, c as u32),
            'a'..='z' | 'A'..='Z' => (3, c.to_ascii_lowercase() as u32),
            _ if c.is_alphabetic() => (4, c.to_lowercase().next().unwrap_or(c) as u32),
            _ => (1, c as u32),
        }
    }
    a.chars()
        .map(weight)
        .cmp(b.chars().map(weight))
        .then_with(|| {
            for (x, y) in a.chars().zip(b.chars()) {
                if x != y {
                    // Lower case first.
                    return y.cmp(&x);
                }
            }
            a.len().cmp(&b.len())
        })
}

/// `tokenizeCliArgs`.
fn tokenize_cli_args(input: &str) -> Vec<String> {
    let chars: Vec<char> = input.chars().collect();
    let mut tokens = Vec::new();
    let mut current = String::new();
    let mut quote: Option<char> = None;
    let mut quoted = false;
    let mut index = 0;
    while index < chars.len() {
        let c = chars[index];
        if let Some(q) = quote {
            if c == q {
                quote = None;
                quoted = true;
            } else if c == '\\' && q == '"' {
                match chars.get(index + 1) {
                    Some(&next) if matches!(next, '"' | '\\' | '$' | '`') => {
                        current.push(next);
                        index += 1;
                    }
                    _ => current.push(c),
                }
            } else {
                current.push(c);
            }
        } else if c == '\'' || c == '"' {
            quote = Some(c);
            quoted = true;
        } else if matches!(c, ' ' | '\t' | '\n' | '\r' | '\x0c' | '\x0b') {
            if !current.is_empty() || quoted {
                tokens.push(std::mem::take(&mut current));
                quoted = false;
            }
        } else if c == '\\' {
            match chars.get(index + 1) {
                Some(&next) if next.is_whitespace() || next == '\'' || next == '"' => {
                    current.push(next);
                    index += 1;
                }
                _ => current.push(c),
            }
        } else {
            current.push(c);
        }
        index += 1;
    }
    if !current.is_empty() || quoted {
        tokens.push(current);
    }
    tokens
}

/// `claudeLaunchArgDirectories`: every `--add-dir` value.
fn launch_arg_directories(launch_args: &str) -> Vec<String> {
    let mut directories = Vec::new();
    let mut collecting = false;
    for token in tokenize_cli_args(launch_args) {
        if token == "--" {
            break;
        }
        if token == "--add-dir" {
            collecting = true;
        } else if let Some(value) = token.strip_prefix("--add-dir=") {
            directories.push(value.to_owned());
            collecting = false;
        } else if token.starts_with('-') {
            collecting = false;
        } else if collecting {
            directories.push(token);
        }
    }
    directories
}

// ---------------------------------------------------------------------------
// Settings

/// `fromLenientJson`: JSON with comments and trailing commas.
pub(super) fn lenient_json(text: &str) -> Option<Value> {
    static COMMENTS: OnceLock<Regex> = OnceLock::new();
    static COMMAS: OnceLock<Regex> = OnceLock::new();
    let comments = COMMENTS
        .get_or_init(|| Regex::new(r#"("(?:[^"\\]|\\.)*")|//[^\r\n]*|(?s:/\*.*?\*/)"#).unwrap());
    let commas = COMMAS.get_or_init(|| Regex::new(r#"("(?:[^"\\]|\\.)*")|,(\s*[}\]])"#).unwrap());
    let stripped = comments.replace_all(text, |caps: &regex::Captures| match caps.get(1) {
        Some(literal) => literal.as_str().to_owned(),
        None => " ".to_owned(),
    });
    let stripped = commas.replace_all(&stripped, |caps: &regex::Captures| match caps.get(1) {
        Some(literal) => literal.as_str().to_owned(),
        None => caps
            .get(2)
            .map(|b| b.as_str().to_owned())
            .unwrap_or_default(),
    });
    serde_json::from_str(&stripped).ok()
}

fn strict_json(path: &Path) -> Option<Value> {
    serde_json::from_str(&read_text(path)?).ok()
}

/// `claudeManagedDirectory`.
fn managed_directory(env: &ProcessEnv) -> Option<PathBuf> {
    if cfg!(target_os = "macos") {
        Some(PathBuf::from("/Library/Application Support/ClaudeCode"))
    } else if cfg!(windows) {
        windows_value(env, "ProgramFiles").map(|p| PathBuf::from(p).join("ClaudeCode"))
    } else {
        Some(PathBuf::from("/etc/claude-code"))
    }
}

fn windows_value(env: &ProcessEnv, name: &str) -> Option<String> {
    env.iter()
        .find(|(key, _)| key.eq_ignore_ascii_case(name))
        .map(|(_, value)| value.trim().to_owned())
        .filter(|value| !value.is_empty())
}

/// `claudeManagedSettingsPaths`.
fn managed_settings_paths(env: &ProcessEnv) -> Vec<PathBuf> {
    let mut paths = Vec::new();
    if cfg!(windows) {
        if let Some(data) = windows_value(env, "ProgramData") {
            paths.push(
                PathBuf::from(data)
                    .join("ClaudeCode")
                    .join("managed-settings.json"),
            );
        }
    }
    if let Some(directory) = managed_directory(env) {
        paths.push(directory.join("managed-settings.json"));
    }
    paths
}

/// `claudeSettingsPaths`: Claude Code's settings files, lowest precedence first.
fn settings_paths(
    config_dir: &Path,
    cwd: Option<&Path>,
    env: &ProcessEnv,
    repository_root: Option<&Path>,
) -> Vec<PathBuf> {
    let mut paths = vec![config_dir.join("settings.json")];
    if let Some(cwd) = cwd {
        paths.push(cwd.join(".claude").join("settings.json"));
        paths.push(cwd.join(".claude").join("settings.local.json"));
    }
    if let Some(root) = repository_root.filter(|root| Some(*root) != cwd) {
        paths.push(root.join(".claude").join("settings.local.json"));
    }
    paths.extend(managed_settings_paths(env));
    paths
}

/// `resolveClaudeConfigDirPath`: `homePath`, else `CLAUDE_CONFIG_DIR` (relative
/// to the workspace), else `~/.claude`.
pub(super) fn config_dir_path(config: &Value, env: &ProcessEnv, cwd: Option<&Path>) -> PathBuf {
    let home_path = config["homePath"].as_str().unwrap_or("").trim();
    if !home_path.is_empty() {
        return resolve(Path::new("."), Path::new(&expand_home(home_path)));
    }
    let configured = env.get("CLAUDE_CONFIG_DIR").map(|v| v.trim()).unwrap_or("");
    if !configured.is_empty() {
        return resolve(cwd.unwrap_or(Path::new(".")), Path::new(configured));
    }
    home_dir().join(".claude")
}

/// The `env` values of Claude's settings that T3 Code reads (`SettingsEnv`).
#[derive(Default)]
pub(super) struct SettingsEnv {
    pub anthropic_base_url: Option<String>,
    pub plugin_dirs: Option<String>,
}

/// `readClaudeSettingsEnv`: the user and managed settings' `env`; later files win.
pub(super) fn read_settings_env(
    config: &Value,
    cwd: Option<&Path>,
    env: &ProcessEnv,
) -> SettingsEnv {
    let config_dir = config_dir_path(config, env, cwd);
    let mut found = SettingsEnv::default();
    for path in settings_paths(&config_dir, None, env, None) {
        let Some(parsed) = read_text(&path).and_then(|text| lenient_json(&text)) else {
            continue;
        };
        let Some(object) = parsed.as_object() else {
            continue;
        };
        let Some(settings_env) = object.get("env") else {
            continue;
        };
        let Some(settings_env) = settings_env.as_object() else {
            continue;
        };
        let strings = [
            "ANTHROPIC_BASE_URL",
            "CLAUDE_CODE_ENTRYPOINT",
            "CLAUDE_CODE_PLUGIN_DIRS",
        ];
        // A field of the wrong type fails the whole file's decode.
        if strings
            .iter()
            .any(|key| settings_env.get(*key).is_some_and(|v| !v.is_string()))
        {
            continue;
        }
        if let Some(url) = settings_env
            .get("ANTHROPIC_BASE_URL")
            .and_then(Value::as_str)
        {
            found.anthropic_base_url = Some(url.to_owned());
        }
        if let Some(dirs) = settings_env
            .get("CLAUDE_CODE_PLUGIN_DIRS")
            .and_then(Value::as_str)
        {
            found.plugin_dirs = Some(dirs.to_owned());
        }
    }
    found
}

#[derive(Clone, Copy)]
struct SkillOverride {
    enabled: bool,
    user_invocation_only: bool,
}

#[derive(Default)]
struct SkillSettings {
    overrides: HashMap<String, SkillOverride>,
    enabled_plugins: Vec<(String, bool)>,
    managed_plugin_names: HashSet<String>,
    synced_plugins_hidden: bool,
}

impl SkillSettings {
    fn plugin(&self, key: &str) -> Option<bool> {
        self.enabled_plugins
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, enabled)| *enabled)
    }

    fn set_plugin(&mut self, key: &str, enabled: Option<bool>) {
        let position = self.enabled_plugins.iter().position(|(k, _)| k == key);
        match (position, enabled) {
            (Some(index), Some(enabled)) => self.enabled_plugins[index].1 = enabled,
            (None, Some(enabled)) => self.enabled_plugins.push((key.to_owned(), enabled)),
            (Some(index), None) => {
                self.enabled_plugins.remove(index);
            }
            (None, None) => {}
        }
    }
}

/// The parts of one settings file `SkillSettings` decodes; `None` when the
/// whole file fails to decode (one unknown `skillOverrides` value is enough).
struct SettingsFile {
    overrides: Vec<(String, SkillOverride)>,
    enabled_plugins: Vec<(String, Value)>,
    sync_claude_ai_plugins: Option<Value>,
}

fn decode_skill_settings(value: &Value) -> Option<SettingsFile> {
    let object = value.as_object()?;
    let mut overrides = Vec::new();
    if let Some(map) = object.get("skillOverrides") {
        for (name, value) in js_entries(map.as_object()?) {
            let parsed = match value.as_str()? {
                "off" => SkillOverride {
                    enabled: false,
                    user_invocation_only: false,
                },
                "user-invocable-only" => SkillOverride {
                    enabled: true,
                    user_invocation_only: true,
                },
                "on" | "name-only" => SkillOverride {
                    enabled: true,
                    user_invocation_only: false,
                },
                _ => return None,
            };
            overrides.push((name, parsed));
        }
    }
    let enabled_plugins = match object.get("enabledPlugins") {
        Some(map) => js_entries(map.as_object()?)
            .into_iter()
            .map(|(k, v)| (k, v.clone()))
            .collect(),
        None => vec![],
    };
    Some(SettingsFile {
        overrides,
        enabled_plugins,
        sync_claude_ai_plugins: object.get("syncClaudeAiPlugins").cloned(),
    })
}

/// `Object.entries` order: integer keys ascending, then the rest as written.
fn js_entries(map: &Map<String, Value>) -> Vec<(String, &Value)> {
    let is_index = |key: &str| {
        key.parse::<u32>()
            .is_ok_and(|n| n.to_string() == key && n != u32::MAX)
    };
    let mut indexed: Vec<(String, &Value)> = map
        .iter()
        .filter(|(k, _)| is_index(k))
        .map(|(k, v)| (k.clone(), v))
        .collect();
    indexed.sort_by_key(|(k, _)| k.parse::<u32>().unwrap_or(0));
    indexed.extend(
        map.iter()
            .filter(|(k, _)| !is_index(k))
            .map(|(k, v)| (k.clone(), v)),
    );
    indexed
}

/// `pluginNameKey`: Claude compares plugin names case-insensitively.
fn plugin_name_key(name: &str) -> String {
    name.nfc().collect::<String>().to_lowercase()
}

/// `readSkillSettings`.
fn read_skill_settings(config_dir: &Path, cwd: Option<&Path>, env: &ProcessEnv) -> SkillSettings {
    let mut settings = SkillSettings::default();
    let managed: Vec<PathBuf> = managed_settings_paths(env);
    let repository_root = cwd.and_then(find_repository_root);
    let project_settings = cwd.map(|cwd| cwd.join(".claude").join("settings.json"));
    for path in settings_paths(config_dir, cwd, env, repository_root.as_deref()) {
        let Some(contents) = read_text(&path) else {
            continue;
        };
        let Some(parsed) = lenient_json(&contents).and_then(|v| decode_skill_settings(&v)) else {
            continue;
        };
        for (name, value) in parsed.overrides {
            settings.overrides.insert(name, value);
        }
        for (key, enabled) in parsed.enabled_plugins {
            match enabled.as_bool() {
                Some(enabled) => {
                    settings.set_plugin(&key, Some(enabled));
                    if managed.contains(&path) {
                        if let Some(at) = key.find('@') {
                            settings
                                .managed_plugin_names
                                .insert(plugin_name_key(&key[..at]));
                        }
                    }
                }
                // Extended formats replace lower-precedence values without enabling the plugin.
                None => settings.set_plugin(&key, None),
            }
        }
        // Claude ignores this switch in shared project settings, and honors only false.
        if parsed.sync_claude_ai_plugins == Some(Value::Bool(false))
            && Some(&path) != project_settings.as_ref()
        {
            settings.synced_plugins_hidden = true;
        }
    }
    settings
}

/// `findRepositoryRoot`: the nearest directory holding `.git`.
fn find_repository_root(cwd: &Path) -> Option<PathBuf> {
    let mut current = resolve(Path::new("."), cwd);
    loop {
        if current.join(".git").exists() {
            return Some(current);
        }
        if !current.pop() {
            return None;
        }
    }
}

/// `findPluginRepositoryRoot`: a linked worktree shares its main repository's
/// plugin install records.
fn find_plugin_repository_root(cwd: &Path) -> Option<PathBuf> {
    let root = find_repository_root(cwd)?;
    let git_file = read_text(&root.join(".git")).unwrap_or_default();
    let git_file = git_file.trim();
    let Some(git_dir) = git_file.strip_prefix("gitdir:") else {
        return Some(root);
    };
    let git_directory = resolve(&root, Path::new(git_dir.trim()));
    let common = read_text(&git_directory.join("commondir")).unwrap_or_default();
    let worktree = read_text(&git_directory.join("gitdir")).unwrap_or_default();
    if common.trim().is_empty() || worktree.trim().is_empty() {
        return Some(root);
    }
    let common_directory = resolve(&git_directory, Path::new(common.trim()));
    let backlink = std::fs::canonicalize(resolve(&git_directory, Path::new(worktree.trim()))).ok();
    let worktree_git = std::fs::canonicalize(root.join(".git")).ok();
    if git_directory.parent() != Some(common_directory.join("worktrees").as_path())
        || backlink.is_none()
        || backlink != worktree_git
    {
        return Some(root);
    }
    if common_directory
        .file_name()
        .is_some_and(|name| name == ".git")
    {
        common_directory.parent().map(Path::to_path_buf)
    } else {
        Some(common_directory)
    }
}

// ---------------------------------------------------------------------------
// Plugins

/// `PluginManifest`: `.claude-plugin/plugin.json`.
struct Manifest {
    name: Option<String>,
    skills: Option<Vec<String>>,
    default_enabled_false: bool,
}

fn read_manifest(plugin: &Path) -> Option<Manifest> {
    let value = strict_json(&plugin.join(".claude-plugin").join("plugin.json"))?;
    let object = value.as_object()?;
    let name = match object.get("name") {
        None => None,
        Some(name) => Some(name.as_str().filter(|n| !n.is_empty())?.to_owned()),
    };
    let skills = match object.get("skills") {
        None => None,
        Some(Value::String(one)) => Some(vec![one.clone()]),
        Some(Value::Array(many)) => Some(
            many.iter()
                .map(|v| v.as_str().map(str::to_owned))
                .collect::<Option<Vec<_>>>()?,
        ),
        Some(_) => return None,
    };
    Some(Manifest {
        name,
        skills,
        default_enabled_false: object.get("defaultEnabled") == Some(&Value::Bool(false)),
    })
}

/// `pluginSkillRoots`: `skills/`, a root `SKILL.md` when nothing is declared,
/// and the manifest's declared roots that stay inside the plugin.
fn plugin_skill_roots(
    install_path: &Path,
    scope: &'static str,
    prefix: &str,
    manifest: Option<&Manifest>,
) -> Vec<Root> {
    let mut roots = Vec::new();
    let root = |directory: PathBuf| Root {
        directory,
        scope,
        prefix: Some(prefix.to_owned()),
    };
    let default_directory = install_path.join("skills");
    let has_default = default_directory.exists();
    if has_default {
        roots.push(root(default_directory));
    }
    let declared = manifest.and_then(|m| m.skills.as_ref());
    if declared.is_none() && !has_default && install_path.join("SKILL.md").exists() {
        roots.push(root(install_path.to_path_buf()));
    }
    let canonical_install = std::fs::canonicalize(install_path).ok();
    for relative in declared.into_iter().flatten() {
        let directory = resolve(install_path, Path::new(relative));
        let Some(canonical_install) = &canonical_install else {
            continue;
        };
        if !inside(install_path, &directory) {
            continue;
        }
        // The native loader rejects manifest roots that escape through a symlink, too.
        if std::fs::canonicalize(&directory).is_ok_and(|c| inside(canonical_install, &c)) {
            roots.push(root(directory));
        }
    }
    roots
}

fn uuid_like(value: &str) -> bool {
    static PATTERN: OnceLock<Regex> = OnceLock::new();
    PATTERN
        .get_or_init(|| Regex::new(r"(?i)^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$").unwrap())
        .is_match(value)
}

/// `claudeAccountConfigPath`: `.claude.json` in a custom config directory, else
/// in the home directory.
pub(super) fn account_config_path(config: &Value, env: &ProcessEnv, config_dir: &Path) -> PathBuf {
    let custom = !config["homePath"].as_str().unwrap_or("").trim().is_empty()
        || env
            .get("CLAUDE_CONFIG_DIR")
            .is_some_and(|v| !v.trim().is_empty());
    if custom {
        config_dir.join(".claude.json")
    } else {
        home_dir().join(".claude.json")
    }
}

/// `readClaudeOauthAccount`.
fn read_oauth_account(path: &Path) -> Option<Map<String, Value>> {
    let value = match std::fs::read(path) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return None,
        Err(_) => return None,
        Ok(bytes) => serde_json::from_slice::<Value>(&bytes).ok()?,
    };
    let account = value.as_object()?.get("oauthAccount")?.as_object()?;
    if account
        .get("organizationUuid")
        .is_some_and(|v| !v.is_string())
    {
        return None;
    }
    Some(account.clone())
}

/// `resolveSyncedPluginBucket`: the `<org>_<account>` folder Claude syncs the
/// signed-in account's claude.ai plugins into.
fn synced_bucket(config: &Value, config_dir: &Path, env: &ProcessEnv) -> Option<String> {
    let account = read_oauth_account(&account_config_path(config, env, config_dir));
    let organization = match env.get("CLAUDE_CODE_ORGANIZATION_UUID") {
        Some(value) => Some(value.clone()),
        None => account
            .as_ref()
            .and_then(|a| a.get("organizationUuid"))
            .and_then(Value::as_str)
            .map(str::to_owned),
    };
    let environment_account = env
        .get("CLAUDE_CODE_ACCOUNT_UUID")
        .filter(|v| !v.is_empty());
    let token = env
        .get("CLAUDE_CODE_OAUTH_TOKEN")
        .is_some_and(|v| !v.is_empty());
    let account_id = match &account {
        Some(account) if !token => account
            .get("accountUuid")
            .and_then(Value::as_str)
            .map(str::to_owned),
        _ => Some(match environment_account {
            Some(id) if uuid_like(id) => id.clone(),
            _ => "unbound".to_owned(),
        }),
    };
    let organization = organization.filter(|o| !o.is_empty())?;
    let account_id = account_id.filter(|a| !a.is_empty())?;
    if !uuid_like(&organization) || (account_id != "unbound" && !uuid_like(&account_id)) {
        return None;
    }
    Some(format!(
        "{}_{}",
        organization.to_lowercase(),
        account_id.to_lowercase()
    ))
}

/// `expandInlinePluginDirectory`: a plugin folder itself, or else the
/// manifest-bearing plugins directly inside it.
fn expand_inline_plugin_directory(directory: &Path) -> Vec<PathBuf> {
    for component in [
        Path::new(".claude-plugin").join("plugin.json"),
        PathBuf::from("skills"),
        PathBuf::from("SKILL.md"),
    ] {
        if directory.join(component).exists() {
            return vec![directory.to_path_buf()];
        }
    }
    sorted_entries(directory)
        .into_iter()
        .map(|child| directory.join(child))
        .filter(|child| child.join(".claude-plugin").join("plugin.json").exists())
        .collect()
}

/// `discoverInlinePluginSkillRoots`: plugin folders from `CLAUDE_CODE_PLUGIN_DIRS`.
fn inline_plugin_roots(
    plugin_dirs: Option<&str>,
    settings: &SkillSettings,
) -> (Vec<Root>, HashSet<String>) {
    let mut roots = Vec::new();
    let mut names = HashSet::new();
    let mut seen = HashSet::new();
    let separator = if cfg!(windows) { ';' } else { ':' };
    for entry in plugin_dirs.unwrap_or("").split(separator) {
        let directory = expand_home(entry.trim());
        // Claude skips relative entries rather than resolving them against its cwd.
        if directory.is_empty() || !Path::new(&directory).is_absolute() {
            continue;
        }
        for install_path in
            expand_inline_plugin_directory(&resolve(Path::new("/"), Path::new(&directory)))
        {
            if !seen.insert(install_path.clone()) {
                continue;
            }
            let manifest = read_manifest(&install_path);
            let prefix = manifest
                .as_ref()
                .and_then(|m| m.name.clone())
                .unwrap_or_else(|| file_name(&install_path));
            // A `<name>@inline` setting wins; otherwise the manifest's `defaultEnabled` decides.
            let setting = settings.plugin(&format!("{prefix}@inline"));
            if setting == Some(false)
                || (setting.is_none() && manifest.as_ref().is_some_and(|m| m.default_enabled_false))
            {
                continue;
            }
            // Managed enabledPlugins entries, true or false, lock the name against inline copies.
            if settings
                .managed_plugin_names
                .contains(&plugin_name_key(&prefix))
            {
                continue;
            }
            names.insert(plugin_name_key(&prefix));
            roots.extend(plugin_skill_roots(
                &install_path,
                "user",
                &prefix,
                manifest.as_ref(),
            ));
        }
    }
    (roots, names)
}

fn file_name(path: &Path) -> String {
    path.file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_default()
}

/// One `installed_plugins.json` record (`decodeInstalledPlugin`).
struct InstalledPlugin {
    scope: String,
    install_path: String,
    project_path: Option<String>,
}

fn decode_installed_plugin(value: &Value) -> Option<InstalledPlugin> {
    let object = value.as_object()?;
    let scope = object.get("scope")?.as_str()?;
    if !matches!(scope, "user" | "project" | "local" | "managed") {
        return None;
    }
    let install_path = object
        .get("installPath")?
        .as_str()
        .filter(|p| !p.is_empty())?;
    let project_path = match object.get("projectPath") {
        None => None,
        Some(path) => Some(path.as_str().filter(|p| !p.is_empty())?.to_owned()),
    };
    Some(InstalledPlugin {
        scope: scope.to_owned(),
        install_path: install_path.to_owned(),
        project_path,
    })
}

/// `syncedPluginDirectoryName`: reserved characters become `_`, trailing dots
/// and spaces go.
fn synced_directory_name(name: &str) -> String {
    let replaced: String = name
        .chars()
        .map(|c| {
            if matches!(c, '<' | '>' | ':' | '"' | '|' | '?' | '*' | '\\' | '/') {
                '_'
            } else {
                c
            }
        })
        .collect();
    replaced.trim_end_matches(['.', ' ']).to_owned()
}

/// `discoverPluginSkillRoots`: installed plugins enabled in settings, then the
/// account's synced claude.ai plugins.
fn plugin_roots(
    config_dir: &Path,
    cwd: Option<&Path>,
    settings: &SkillSettings,
    inline_names: &HashSet<String>,
    synced_bucket: Option<&str>,
    env: &ProcessEnv,
) -> Vec<Root> {
    let base = cwd.unwrap_or(Path::new("."));
    let plugins_path = match env
        .get("CLAUDE_CODE_PLUGIN_CACHE_DIR")
        .filter(|v| !v.is_empty())
    {
        Some(directory) => resolve(base, Path::new(&expand_home(directory))),
        None => config_dir.join("plugins"),
    };
    let installed = strict_json(&plugins_path.join("installed_plugins.json")).and_then(|value| {
        let plugins = value.as_object()?.get("plugins")?.as_object()?.clone();
        plugins.values().all(Value::is_array).then_some(plugins)
    });
    let mut roots = Vec::new();
    // An enabled inline plugin replaces installed and synced plugins of the same name.
    let mut claimed: HashSet<String> = inline_names.clone();
    // The CLI's process.cwd() resolves symlinks; registry projectPath values remain lexical.
    let workspace =
        cwd.map(|cwd| std::fs::canonicalize(cwd).unwrap_or_else(|_| resolve(Path::new("."), cwd)));
    let mut project_roots: HashMap<PathBuf, Option<PathBuf>> = HashMap::new();
    let repository_root = workspace.as_deref().and_then(find_plugin_repository_root);

    let mut entries: Vec<(String, Vec<Value>)> = installed
        .map(|plugins| {
            plugins
                .into_iter()
                .map(|(k, v)| (k, v.as_array().cloned().unwrap_or_default()))
                .collect()
        })
        .unwrap_or_default();
    // `Object.entries(...).sort()` compares each `[key, records]` pair as "key,...".
    entries.sort_by(|(a, _), (b, _)| js_compare(&format!("{a},"), &format!("{b},")));
    for (key, records) in entries {
        if settings.plugin(&key) != Some(true) {
            continue;
        }
        let applicable: Vec<InstalledPlugin> = records
            .iter()
            .filter_map(decode_installed_plugin)
            .filter(|record| {
                if record.scope == "user" || record.scope == "managed" {
                    return true;
                }
                let (Some(project), Some(workspace)) = (&record.project_path, &workspace) else {
                    return false;
                };
                let project = resolve(Path::new("."), Path::new(project));
                if &project == workspace {
                    return true;
                }
                if repository_root.is_none() {
                    return false;
                }
                let root = project_roots
                    .entry(project.clone())
                    .or_insert_with(|| find_plugin_repository_root(&project));
                *root == repository_root
            })
            .collect();
        // The native loader keeps registry order, preferring the first install that exists.
        let existing = applicable
            .iter()
            .position(|record| resolve(base, Path::new(&record.install_path)).exists());
        let Some(install) = applicable.get(existing.unwrap_or(0)) else {
            continue;
        };
        let scope = if install.scope == "user" || install.scope == "managed" {
            "user"
        } else {
            "project"
        };
        let install_path = resolve(base, Path::new(&install.install_path));
        let manifest = read_manifest(&install_path);
        let prefix = manifest
            .as_ref()
            .and_then(|m| m.name.clone())
            .unwrap_or_else(|| key.split('@').next().unwrap_or("").to_owned());
        if prefix.is_empty() || inline_names.contains(&plugin_name_key(&prefix)) {
            continue;
        }
        // A registry record whose install is gone loads nothing and claims no namespace.
        if existing.is_some() {
            claimed.insert(plugin_name_key(&prefix));
        }
        roots.extend(plugin_skill_roots(
            &install_path,
            scope,
            &prefix,
            manifest.as_ref(),
        ));
    }
    let Some(bucket) = synced_bucket else {
        return roots;
    };

    let synced_path = config_dir.join("plugins").join("synced");
    let bucket_path = synced_path.join(bucket);
    let disabled: HashSet<String> = settings
        .enabled_plugins
        .iter()
        .filter(|(key, enabled)| !enabled && key.ends_with("@synced"))
        .map(|(key, _)| plugin_name_key(&key[..key.len() - 7]))
        .collect();
    if !synced_path.join(format!(".bucket-{bucket}")).exists() {
        return roots;
    }
    let rows = strict_json(&bucket_path.join("manifest.json"))
        .and_then(|value| value.as_object()?.get("plugins")?.as_array().cloned());
    for row in rows.unwrap_or_default() {
        let Some(object) = row.as_object() else {
            continue;
        };
        let (Some(_), Some(name)) = (
            object.get("pluginId").and_then(Value::as_str),
            object.get("name").and_then(Value::as_str),
        ) else {
            continue;
        };
        let generation = match object.get("generation") {
            None => None,
            Some(Value::Number(n)) => n.as_f64(),
            Some(_) => continue,
        };
        let directory_name = synced_directory_name(name);
        if directory_name.is_empty() || directory_name.starts_with('.') {
            continue;
        }
        let folder = match generation {
            Some(g) if g.fract() == 0.0 && g >= 2.0 => format!("{directory_name}~g{}", g as i64),
            _ => directory_name.clone(),
        };
        let plugin_path = bucket_path.join(folder);
        let manifest = read_manifest(&plugin_path);
        let prefix = manifest
            .as_ref()
            .and_then(|m| m.name.clone())
            .unwrap_or_else(|| directory_name.clone());
        // An installed plugin of the same name, then the first synced row, holds the namespace.
        let key = plugin_name_key(&prefix);
        if claimed.contains(&key) || disabled.contains(&key) {
            continue;
        }
        claimed.insert(key);
        roots.extend(plugin_skill_roots(
            &plugin_path,
            "user",
            &prefix,
            manifest.as_ref(),
        ));
    }
    roots
}

// ---------------------------------------------------------------------------
// SKILL.md

#[derive(Default)]
struct Frontmatter {
    name: Option<String>,
    description: Option<String>,
    user_invocation_only: bool,
    not_user_invocable: bool,
}

enum Parsed {
    Missing,
    Malformed,
    Found(Frontmatter),
}

/// Claude accepts YAML 1.1 booleans that a YAML 1.2 parser leaves as strings or numbers.
fn frontmatter_boolean(value: Option<&serde_yaml::Value>) -> Option<bool> {
    match value? {
        serde_yaml::Value::Bool(b) => Some(*b),
        serde_yaml::Value::Number(n) => match n.as_f64() {
            Some(1.0) => Some(true),
            Some(0.0) => Some(false),
            _ => None,
        },
        serde_yaml::Value::String(s) => match s.trim().to_lowercase().as_str() {
            "true" | "yes" | "on" | "y" => Some(true),
            "false" | "no" | "off" | "n" => Some(false),
            _ => None,
        },
        _ => None,
    }
}

/// Repair Claude's accepted `key: a: b` scalars by quoting them.
fn repair_frontmatter(frontmatter: &str) -> String {
    static LINE: OnceLock<Regex> = OnceLock::new();
    static COMMENT: OnceLock<Regex> = OnceLock::new();
    static COLON: OnceLock<Regex> = OnceLock::new();
    static SPECIAL: OnceLock<Regex> = OnceLock::new();
    let line = LINE.get_or_init(|| Regex::new(r"(?m)^([A-Za-z0-9_-]+:[ \t]*)([^\r\n]*)").unwrap());
    let comment = COMMENT.get_or_init(|| Regex::new(r"[ \t]+#").unwrap());
    let colon = COLON.get_or_init(|| Regex::new(r":[ \t]").unwrap());
    let special =
        SPECIAL.get_or_init(|| Regex::new(r#"^(?:["'\[\]{}|>&*!#%@`]|[-?:](?:[ \t]|$))"#).unwrap());
    line.replace_all(frontmatter, |caps: &regex::Captures| {
        let prefix = &caps[1];
        let value = &caps[2];
        let scalar = comment.split(value).next().unwrap_or("");
        if !colon.is_match(scalar) || special.is_match(scalar) {
            return caps[0].to_owned();
        }
        format!(
            "{prefix}{}{}",
            serde_json::to_string(scalar).unwrap_or_default(),
            &value[scalar.len()..]
        )
    })
    .into_owned()
}

fn parse_frontmatter(contents: &str) -> Parsed {
    static PATTERN: OnceLock<Regex> = OnceLock::new();
    let pattern =
        PATTERN.get_or_init(|| Regex::new(r"^---\r?\n((?s:.*?))\r?\n---(?:\r?\n|$)").unwrap());
    let Some(captures) = pattern.captures(contents) else {
        return Parsed::Missing;
    };
    let frontmatter = captures.get(1).map(|m| m.as_str()).unwrap_or("");
    let parsed = serde_yaml::from_str::<serde_yaml::Value>(frontmatter)
        .or_else(|_| serde_yaml::from_str::<serde_yaml::Value>(&repair_frontmatter(frontmatter)));
    let Ok(serde_yaml::Value::Mapping(record)) = parsed else {
        return Parsed::Malformed;
    };
    let text = |key: &str| {
        record
            .get(key)
            .and_then(serde_yaml::Value::as_str)
            .map(str::trim)
            .filter(|v| !v.is_empty())
            .map(str::to_owned)
    };
    Parsed::Found(Frontmatter {
        name: text("name"),
        description: text("description"),
        user_invocation_only: frontmatter_boolean(record.get("disable-model-invocation"))
            == Some(true),
        not_user_invocable: frontmatter_boolean(record.get("user-invocable")) == Some(false),
    })
}

/// Whether `$name` is a whole skill mention for the composer
/// (`SKILL_MENTION_PATTERN`'s first match captures all of `name`).
fn mentionable(name: &str) -> bool {
    static NAME: OnceLock<Regex> = OnceLock::new();
    static AMOUNT: OnceLock<Regex> = OnceLock::new();
    let whole = NAME.get_or_init(|| Regex::new(r"^[a-zA-Z0-9][a-zA-Z0-9:_-]*$").unwrap());
    let amount =
        AMOUNT.get_or_init(|| Regex::new(r"^[0-9][0-9_]*(?:[kKmMbBtT]|[eE][0-9]+)?$").unwrap());
    whole.is_match(name) && name.chars().any(|c| c.is_ascii_alphabetic()) && !amount.is_match(name)
}

/// `.replace(/[^a-zA-Z0-9_-]/g, "-")`, one `-` per UTF-16 unit.
fn sanitize(name: &str) -> String {
    let mut out = String::new();
    for c in name.chars() {
        if c.is_ascii_alphanumeric() || c == '_' || c == '-' {
            out.push(c);
        } else {
            for _ in 0..c.len_utf16() {
                out.push('-');
            }
        }
    }
    out
}

/// `discoverClaudeSkills` with the server's state directory as `cwd`.
pub(super) fn discover(config: &Value, state_dir: &Path, env: &ProcessEnv) -> Vec<Value> {
    let cwd = Some(state_dir);
    let config_dir = config_dir_path(config, env, cwd);
    let settings = read_skill_settings(&config_dir, cwd, env);
    // Most installs have no synced folder; skip reading `.claude.json` for them.
    let bucket = (!settings.synced_plugins_hidden
        && config_dir.join("plugins").join("synced").exists())
    .then(|| synced_bucket(config, &config_dir, env))
    .flatten();
    // Claude applies its settings `env` over the inherited environment.
    let plugin_dirs = read_settings_env(config, cwd, env)
        .plugin_dirs
        .or_else(|| env.get("CLAUDE_CODE_PLUGIN_DIRS").cloned());
    let (inline_roots, inline_names) = inline_plugin_roots(plugin_dirs.as_deref(), &settings);

    let mut roots = Vec::new();
    if let Some(managed) = managed_directory(env) {
        roots.push(Root {
            directory: managed.join(".claude").join("skills"),
            scope: "user",
            prefix: None,
        });
    }
    roots.push(Root {
        directory: config_dir.join("skills"),
        scope: "user",
        prefix: None,
    });
    roots.push(Root {
        directory: state_dir.join(".claude").join("skills"),
        scope: "project",
        prefix: None,
    });
    for directory in launch_arg_directories(config["launchArgs"].as_str().unwrap_or("")) {
        if directory.is_empty() {
            continue;
        }
        roots.push(Root {
            directory: resolve(state_dir, Path::new(&directory))
                .join(".claude")
                .join("skills"),
            scope: "project",
            prefix: None,
        });
    }
    roots.extend(inline_roots);
    roots.extend(plugin_roots(
        &config_dir,
        cwd,
        &settings,
        &inline_names,
        bucket.as_deref(),
        env,
    ));

    let mut skills: Vec<(String, Value)> = Vec::new();
    for root in roots {
        let single = root.prefix.is_some() && root.directory.join("SKILL.md").exists();
        let entries = if single {
            vec![file_name(&root.directory)]
        } else {
            sorted_entries(&root.directory)
        };
        for entry in entries {
            let skill_path = if single {
                root.directory.join("SKILL.md")
            } else {
                root.directory.join(&entry).join("SKILL.md")
            };
            let Some(contents) = read_text(&skill_path) else {
                continue;
            };
            let frontmatter = match parse_frontmatter(&contents) {
                Parsed::Malformed => continue,
                Parsed::Missing => Frontmatter::default(),
                Parsed::Found(frontmatter) => frontmatter,
            };
            let directory_name = entry.trim();
            if directory_name.is_empty() {
                continue;
            }
            let mut name = directory_name.to_owned();
            let prefix = root.prefix.as_deref().filter(|p| !p.is_empty());
            if let Some(prefix) = prefix {
                // Native folder skills use their folder; only a single SKILL.md uses its name field.
                let declared = if single {
                    frontmatter.name.clone().unwrap_or_default()
                } else {
                    String::new()
                };
                let unprefixed = declared
                    .strip_prefix(&format!("{prefix}:"))
                    .map(str::to_owned)
                    .unwrap_or(declared);
                let base = if unprefixed.is_empty() {
                    entry.as_str()
                } else {
                    unprefixed.as_str()
                };
                name = format!("{prefix}:{}", sanitize(base));
                // Advertise only exact native names the shared composer grammar can invoke.
                if !mentionable(&name) {
                    continue;
                }
            }
            if skills.iter().any(|(existing, _)| *existing == name) {
                continue;
            }
            let skill_override = if prefix.is_some() {
                None
            } else {
                settings.overrides.get(&name).copied()
            };
            let user_invocation_only = frontmatter.user_invocation_only
                || skill_override.is_some_and(|o| o.user_invocation_only);
            let mut skill = json!({
                "name": name,
                "path": skill_path.display().to_string(),
                "enabled": skill_override.is_none_or(|o| o.enabled),
                "scope": root.scope,
            });
            if let Some(description) = &frontmatter.description {
                skill["description"] = json!(description);
            }
            if user_invocation_only {
                skill["userInvocationOnly"] = json!(true);
            }
            if frontmatter.not_user_invocable {
                skill["userInvocable"] = json!(false);
            }
            skills.push((name, skill));
        }
    }
    skills.sort_by(|(a, _), (b, _)| locale_compare(a, b));
    skills.into_iter().map(|(_, skill)| skill).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write(path: &Path, text: &str) {
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, text).unwrap();
    }

    #[test]
    fn frontmatter_follows_claude() {
        let Parsed::Found(f) = parse_frontmatter(
            "---\nname: x\ndescription: Deploys: to prod # note\ndisable-model-invocation: yes\nuser-invocable: 0\n---\nbody",
        ) else {
            panic!("frontmatter")
        };
        assert_eq!(f.description.as_deref(), Some("Deploys: to prod"));
        assert!(f.user_invocation_only);
        assert!(f.not_user_invocable);
        assert!(matches!(
            parse_frontmatter("no frontmatter"),
            Parsed::Missing
        ));
        assert!(matches!(
            parse_frontmatter("---\n- a\n---\n"),
            Parsed::Malformed
        ));
        assert!(matches!(
            parse_frontmatter("---\n\n---\n"),
            Parsed::Malformed
        ));
    }

    #[test]
    fn lenient_json_strips_comments_and_trailing_commas() {
        assert_eq!(
            lenient_json("{\"a\": \"//kept\", /* x */ \"b\": [1,], // c\n}"),
            Some(json!({"a": "//kept", "b": [1]}))
        );
    }

    #[test]
    fn launch_args_collect_every_add_dir() {
        assert_eq!(
            launch_arg_directories("--add-dir a 'b c' --model x --add-dir=d -- --add-dir e"),
            vec!["a", "b c", "d"]
        );
    }

    #[test]
    fn names_sort_as_locale_compare() {
        let mut names = vec!["b", "a:z", "a-b", "ab", "A", "a_b", "a1"];
        names.sort_by(|a, b| locale_compare(a, b));
        assert_eq!(names, vec!["A", "a_b", "a-b", "a:z", "a1", "ab", "b"]);
    }

    #[test]
    fn discovers_user_project_and_plugin_skills() {
        let home = tempfile::tempdir().unwrap();
        let config_dir = home.path().join("claude");
        let cwd = home.path().join("state");
        write(
            &config_dir.join("skills/zeta/SKILL.md"),
            "---\ndescription: Last\n---\n",
        );
        write(&config_dir.join("skills/hidden/SKILL.md"), "plain");
        write(&cwd.join(".claude/skills/zeta/SKILL.md"), "shadowed");
        write(&cwd.join(".claude/skills/bad/SKILL.md"), "---\n[\n---\n");
        write(
            &config_dir.join("settings.json"),
            "{\"skillOverrides\": {\"hidden\": \"off\"}, \"enabledPlugins\": {\"tools@market\": true},}",
        );
        let plugin = home.path().join("plugins/tools");
        write(
            &config_dir.join("plugins/installed_plugins.json"),
            &json!({"plugins": {"tools@market": [{"scope": "user", "installPath": plugin}]}})
                .to_string(),
        );
        write(
            &plugin.join("skills/lint it/SKILL.md"),
            "---\nname: other\n---\n",
        );
        let config = json!({"homePath": config_dir});
        let skills = discover(&config, &cwd, &ProcessEnv::new());
        let names: Vec<&str> = skills.iter().map(|s| s["name"].as_str().unwrap()).collect();
        assert_eq!(names, vec!["hidden", "tools:lint-it", "zeta"]);
        assert_eq!(skills[0]["enabled"], false);
        assert_eq!(skills[2]["scope"], "user");
        assert_eq!(skills[2]["description"], "Last");
    }
}
