//! fx's skills for a folder, as the reference's `discoverFxSkills`
//! (`provider-fx/src/server/skills.ts`): the SKILL.md folders fx 0.0.13 reads,
//! scanned directly so no `fx acp` session starts. The header is fx's own
//! line-based format, not YAML.
use super::ProcessEnv;
use serde_json::{json, Value};
use std::collections::HashSet;
use std::io::Read;
use std::path::{Path, PathBuf};

const MAX_FRONTMATTER_BYTES: usize = 64 * 1024;
const MAX_NAME_BYTES: usize = 256;

/// A root fx scans, its menu rank, and whether it follows symlinked skills.
struct Root {
    directory: PathBuf,
    scope: &'static str,
    rank: u8,
    follows_links: bool,
}

const WORKSPACE_ROOTS: [(&str, u8); 7] = [
    (".fx/skills", 1),
    ("skills", 1),
    (".opencode/skills", 2),
    (".codex/skills", 2),
    (".claude/skills", 2),
    (".agents/skills", 2),
    (".claw/skills", 2),
];
const HOME_ROOTS: [(&str, u8); 6] = [
    (".fx/skills", 0),
    (".config/opencode/skills", 2),
    (".codex/skills", 2),
    (".claude/skills", 2),
    (".agents/skills", 2),
    (".claw/skills", 2),
];

/// `decodeSkillFile`: each line decoded strictly; a line with invalid UTF-8
/// ends in a NUL, which the parser rejects. A BOM is kept.
fn decode(bytes: &[u8]) -> String {
    bytes
        .split(|b| *b == b'\n')
        .map(|line| match std::str::from_utf8(line) {
            Ok(text) => text.to_owned(),
            Err(_) => format!("{}\u{0}", String::from_utf8_lossy(line)),
        })
        .collect::<Vec<_>>()
        .join("\n")
}

/// fx trims only spaces and tabs.
fn trim_space_tab(value: &str) -> &str {
    value.trim_matches(|c| c == ' ' || c == '\t')
}

/// `invalidTextCause`: no ASCII control character.
fn has_control(value: &str) -> bool {
    value.chars().any(|c| c < ' ' || c == '\x7f')
}

/// `parseRecognizedValue`: one quoted or bare scalar.
fn recognized_value(raw: &str) -> Option<String> {
    if raw.starts_with('|') || raw.starts_with('>') {
        return None;
    }
    let quote = |c: char| c == '\'' || c == '"';
    let quoted = raw.starts_with(quote) || raw.ends_with(quote);
    if !quoted {
        return Some(raw.to_owned());
    }
    let chars: Vec<char> = raw.chars().collect();
    (chars.len() >= 2 && chars[0] == chars[chars.len() - 1])
        .then(|| chars[1..chars.len() - 1].iter().collect())
}

/// `invalidSkillNameCause`.
fn valid_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= MAX_NAME_BYTES
        && !has_control(name)
        && name != "."
        && name != ".."
        && !name.contains('/')
        && !name.contains('\\')
}

/// `decode_block_description`: `|` keeps line breaks; `>` and `>-` join
/// neighbouring lines with a space and turn a blank line into a break.
fn block_description(lines: &[Option<String>], style: &str) -> String {
    let Some(last) = lines.iter().rposition(Option::is_some) else {
        return String::new();
    };
    let mut text = String::new();
    for index in 0..=last {
        if index > 0 {
            let join = style != "|" && lines[index - 1].is_some() && lines[index].is_some();
            text.push(if join { ' ' } else { '\n' });
        }
        text.push_str(lines[index].as_deref().unwrap_or(""));
    }
    text.trim().to_owned()
}

/// `parseSkillMetadata`: the skill's name and description, or `None` for a
/// header fx refuses. No header names the skill by its folder.
fn parse(contents: &str, folder: &str) -> Option<(String, Option<String>)> {
    let opening = if contents.starts_with("---\r\n") {
        5
    } else if contents.starts_with("---\n") {
        4
    } else if contents == "---" {
        3
    } else {
        return valid_name(folder).then(|| (folder.to_owned(), None));
    };
    let lines: Vec<&str> = contents[opening..].split('\n').collect();
    let unterminated = |index: usize| index == lines.len() - 1;
    let closing = lines.iter().enumerate().position(|(index, line)| {
        let line = if unterminated(index) {
            *line
        } else {
            line.strip_suffix('\r').unwrap_or(line)
        };
        line == "---"
    })?;
    let through_closing = opening + lines[..=closing].join("\n").len();
    let body_start = through_closing + if unterminated(closing) { 0 } else { 1 };
    if body_start.min(contents.len()) > MAX_FRONTMATTER_BYTES {
        return None;
    }
    let header: Vec<&str> = lines[..closing]
        .iter()
        .map(|line| line.strip_suffix('\r').unwrap_or(line))
        .collect();
    let mut name: Option<String> = None;
    let mut description: Option<String> = None;
    let (mut saw_name, mut saw_description, mut previous_recognized) = (false, false, false);
    let mut index = 0;
    while index < header.len() {
        let line = header[index];
        let trimmed = trim_space_tab(line);
        if trimmed.is_empty() || trimmed.starts_with('#') {
            index += 1;
            continue;
        }
        if previous_recognized && (line.starts_with(' ') || line.starts_with('\t')) {
            return None;
        }
        previous_recognized = false;
        let Some(colon) = trimmed.find(':') else {
            index += 1;
            continue;
        };
        let key = trim_space_tab(&trimmed[..colon]);
        let raw = trim_space_tab(&trimmed[colon + 1..]);
        if key == "name" {
            if saw_name {
                return None;
            }
            saw_name = true;
            name = Some(recognized_value(raw)?);
            previous_recognized = true;
        } else if key == "description" {
            if saw_description {
                return None;
            }
            saw_description = true;
            if matches!(raw, ">" | ">-" | "|") {
                let mut block: Vec<Option<String>> = Vec::new();
                let mut base: Option<usize> = None;
                while index + 1 < header.len() {
                    let next = header[index + 1];
                    if trim_space_tab(next).is_empty() {
                        block.push(None);
                        index += 1;
                        continue;
                    }
                    let indent = next.len() - next.trim_start_matches(' ').len();
                    if next[indent..].starts_with('\t') {
                        return None;
                    }
                    if indent == 0 {
                        break;
                    }
                    if base.is_some_and(|base| indent < base) {
                        return None;
                    }
                    let base = *base.get_or_insert(indent);
                    let content = &next[base..];
                    if has_control(content) {
                        return None;
                    }
                    block.push(Some(content.to_owned()));
                    index += 1;
                }
                description = Some(block_description(&block, raw));
            } else {
                let value = recognized_value(raw)?;
                if has_control(&value) {
                    return None;
                }
                description = Some(value);
                previous_recognized = true;
            }
        }
        index += 1;
    }
    let name = name.filter(|n| valid_name(n))?;
    Some((name, description.filter(|d| !d.is_empty())))
}

/// `skillRoots`: the workspace roots of each folder from `cwd` up to, not
/// including, HOME, then the home roots.
fn roots(cwd: &Path, home: &Path) -> Vec<Root> {
    let mut roots = Vec::new();
    let mut current = cwd.to_path_buf();
    while current != home {
        for (relative, rank) in WORKSPACE_ROOTS {
            roots.push(Root {
                directory: current.join(relative),
                scope: "project",
                rank,
                follows_links: true,
            });
        }
        if !current.pop() {
            break;
        }
    }
    for (relative, rank) in HOME_ROOTS {
        roots.push(Root {
            directory: home.join(relative),
            scope: "user",
            rank,
            follows_links: rank != 0,
        });
    }
    roots
}

fn is_link(path: &Path) -> bool {
    std::fs::symlink_metadata(path).is_ok_and(|m| m.file_type().is_symlink())
}

/// `compareUtf8`: byte order.
fn sorted_entries(directory: &Path) -> Vec<String> {
    let mut names: Vec<String> = std::fs::read_dir(directory)
        .map(|entries| {
            entries
                .flatten()
                .filter_map(|e| e.file_name().into_string().ok())
                .collect()
        })
        .unwrap_or_default();
    names.sort();
    names
}

/// `discoverSkillsInRoot`: each skill with its real folder.
fn root_skills(root: &Root) -> Vec<(Value, PathBuf)> {
    // The managed root loads nothing through a link to it or inside it.
    if !root.follows_links
        && std::fs::canonicalize(&root.directory).is_ok_and(|real| real != root.directory)
    {
        return vec![];
    }
    let mut skills = Vec::new();
    for entry in sorted_entries(&root.directory) {
        let folder = root.directory.join(&entry);
        if !root.follows_links && is_link(&folder) {
            continue;
        }
        if !std::fs::metadata(&folder).is_ok_and(|m| m.is_dir()) {
            continue;
        }
        let skill_path = folder.join("SKILL.md");
        if !root.follows_links && is_link(&skill_path) {
            continue;
        }
        if !std::fs::metadata(&skill_path).is_ok_and(|m| m.is_file()) {
            continue;
        }
        // fx reads only the metadata prefix, so a long body never hides a skill.
        let mut prefix = Vec::new();
        let read = std::fs::File::open(&skill_path).and_then(|file| {
            file.take(MAX_FRONTMATTER_BYTES as u64 + 1)
                .read_to_end(&mut prefix)
        });
        if read.is_err() {
            continue;
        }
        let Some((name, description)) = parse(&decode(&prefix), &entry) else {
            continue;
        };
        let mut skill = json!({"name": name, "path": skill_path.display().to_string(),
            "scope": root.scope, "enabled": true});
        if let Some(description) = description {
            skill["description"] = json!(description);
        }
        let real = std::fs::canonicalize(&folder).unwrap_or(folder);
        skills.push((skill, real));
    }
    skills
}

/// `discoverFxSkills`: skills in fx's menu order. A folder reached twice
/// counts once, and only an exact repeat of a name is dropped.
pub(super) fn discover(cwd: &str, env: &ProcessEnv) -> Vec<Value> {
    let Some(home) = env.get("HOME").map(|h| h.trim()).filter(|h| !h.is_empty()) else {
        return vec![];
    };
    let real = |target: &str| std::fs::canonicalize(target).unwrap_or_else(|_| target.into());
    let (home, cwd) = (real(home), real(cwd));
    let mut folders = HashSet::new();
    let mut names = HashSet::new();
    let mut found: Vec<(Value, u8)> = Vec::new();
    for root in roots(&cwd, &home) {
        for (skill, folder) in root_skills(&root) {
            if !folders.insert(folder) {
                continue;
            }
            if !names.insert(skill["name"].as_str().unwrap_or("").to_owned()) {
                continue;
            }
            found.push((skill, root.rank));
        }
    }
    found.sort_by_key(|(_, rank)| *rank);
    found.into_iter().map(|(skill, _)| skill).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn headers_follow_fx() {
        assert_eq!(parse("no header", "folder"), Some(("folder".into(), None)));
        assert_eq!(
            parse(
                "---\nname: \"Ship\"\ndescription: Deploys it\n---\nbody",
                "x"
            ),
            Some(("Ship".into(), Some("Deploys it".into())))
        );
        assert_eq!(
            parse(
                "---\nname: a\ndescription: >\n  one\n  two\n\n  three\n---\n",
                "x"
            ),
            Some(("a".into(), Some("one two\n\nthree".into())))
        );
        assert_eq!(parse("---\nname: a\nname: b\n---\n", "x"), None);
        assert_eq!(parse("---\nname: a\n  more\n---\n", "x"), None);
        assert_eq!(parse("---\nname: a\n", "x"), None);
        assert_eq!(parse("---\ndescription: x\n---\n", "x"), None);
        assert_eq!(parse("---\nname: a/b\n---\n", "x"), None);
        assert_eq!(decode(b"ok\n\xff"), "ok\n\u{fffd}\u{0}");
    }
}
