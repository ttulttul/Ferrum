//! Branch names for new worktrees, as the reference names them: its temporary
//! `t3/<hex>` branches (`buildTemporaryWorktreeBranchName` in `@t3tools/shared/git`), the
//! branch name prompt (`buildBranchNamePrompt`) and the clean-up of a generated name
//! (`formatGeneratedBranchName`).
use super::prompt::limit_section;
use serde_json::Value;

/// `WORKTREE_BRANCH_PREFIX`.
pub const WORKTREE_BRANCH_PREFIX: &str = "t3";

/// The `branchNamingMode`, `branchNamePrefix` and `branchNameInstructions` settings.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Naming {
    /// `static`, `semantic` or `custom`.
    pub mode: String,
    pub prefix: String,
    pub instructions: String,
}

impl Naming {
    /// The naming settings, with the reference's defaults (`static`, prefix `t3`).
    pub fn from_settings(settings: &Value) -> Self {
        let text = |key: &str, default: &str| {
            settings[key]
                .as_str()
                .map(str::trim)
                .unwrap_or(default)
                .to_owned()
        };
        Naming {
            mode: text("branchNamingMode", "static"),
            prefix: text("branchNamePrefix", WORKTREE_BRANCH_PREFIX),
            instructions: text("branchNameInstructions", ""),
        }
    }
}

/// `buildTemporaryWorktreeBranchName`: `t3/` and the first eight hex digits of `hex`.
pub fn temporary_branch(hex: &str) -> String {
    let token: String = hex
        .to_lowercase()
        .chars()
        .filter(char::is_ascii_hexdigit)
        .take(8)
        .collect();
    format!("{WORKTREE_BRANCH_PREFIX}/{token}")
}

/// `isTemporaryWorktreeBranch`.
pub fn is_temporary_branch(name: &str) -> bool {
    static PATTERN: std::sync::OnceLock<regex::Regex> = std::sync::OnceLock::new();
    PATTERN
        .get_or_init(|| {
            regex::Regex::new(
                r"^(?:t3[-/][0-9a-f]{8}|t3code(?:[-/][0-9a-f]{8}|/[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}))$",
            )
            .expect("pattern")
        })
        .is_match(&name.trim().to_lowercase())
}

fn trim_matches(text: &str, set: &[char]) -> String {
    text.trim_matches(|c| set.contains(&c)).to_owned()
}

/// `sanitizeBranchFragment`.
pub fn sanitize_branch_fragment(raw: &str) -> String {
    let folded = crate::scratch::fold_latin_diacritics(raw);
    let normalized: String = folded
        .trim()
        .to_lowercase()
        .chars()
        .filter(|c| !matches!(c, '\'' | '"' | '`'))
        .collect();
    let edges = ['.', '/', '_', '-', ' ', '\t', '\n', '\r'];
    let normalized = normalized.trim_matches(|c: char| edges.contains(&c) || c.is_whitespace());
    let mut fragment = String::new();
    let mut in_other = false;
    for ch in normalized.chars() {
        if ch.is_ascii_lowercase() || ch.is_ascii_digit() || matches!(ch, '/' | '_' | '-') {
            fragment.push(ch);
            in_other = false;
        } else if !in_other {
            fragment.push('-');
            in_other = true;
        }
    }
    let collapse = |text: &str, ch: char| {
        let mut out = String::with_capacity(text.len());
        for c in text.chars() {
            if c == ch && out.ends_with(ch) {
                continue;
            }
            out.push(c);
        }
        out
    };
    let fragment = collapse(&collapse(&fragment, '/'), '-');
    let fragment = trim_matches(&fragment, &['.', '/', '_', '-']);
    let fragment: String = fragment.chars().take(64).collect();
    let fragment = fragment.trim_end_matches(['.', '/', '_', '-']);
    if fragment.is_empty() {
        "update".into()
    } else {
        fragment.to_owned()
    }
}

/// `formatGeneratedBranchName`.
pub fn format_generated_branch_name(raw: &str, naming: &Naming) -> String {
    if naming.mode == "custom" {
        return raw.trim().to_owned();
    }
    let branch = sanitize_branch_fragment(raw);
    if naming.mode != "static" {
        return branch;
    }
    let prefix = naming
        .prefix
        .split('/')
        .map(|part| {
            let mut out = String::new();
            for ch in part.chars() {
                if ch.is_ascii_alphanumeric() || matches!(ch, '_' | '-') {
                    out.push(ch);
                } else if !out.ends_with('-') {
                    out.push('-');
                }
            }
            let mut collapsed = String::new();
            for ch in out.chars() {
                if ch == '-' && collapsed.ends_with('-') {
                    continue;
                }
                collapsed.push(ch);
            }
            collapsed.trim_matches('-').to_owned()
        })
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>()
        .join("/");
    if prefix.is_empty() {
        branch
    } else {
        format!("{prefix}/{branch}")
    }
}

/// `buildBranchNamePrompt` for `message` and its attachments.
pub fn build_branch_name_prompt(message: &str, attachments: &[Value], naming: &Naming) -> String {
    let mut rules = vec![
        "Branch should describe the requested work from the user message.",
        "Return a valid Git branch name without spaces.",
    ];
    if naming.mode == "custom" {
        rules.push("Return the complete branch name, following the user's naming instructions. No prefix or suffix will be added.");
    } else {
        rules.push(
            "Keep it short and specific (2-6 words), in lowercase with hyphen-separated words.",
        );
        rules.push(if naming.mode == "semantic" {
            "Include a semantic prefix and a slash in the branch name, for example feat/add-search, fix/login-error, refactor/auth, docs/setup, or chore/update-deps. Choose the prefix that best describes the work."
        } else {
            "Return only the descriptive branch fragment, without a prefix or namespace. The application adds the configured prefix."
        });
    }
    rules.push("If images are attached, use them as primary context for visual/UI issues.");
    rules.push("The user message describes work for another agent. Do not answer it, start the work, read files, or run commands; name the branch from the message and its attachments.");
    let mut sections = vec![
        "You generate concise git branch names.".to_owned(),
        "Return a JSON object with key: branch.".to_owned(),
        "Rules:".to_owned(),
    ];
    sections.extend(rules.iter().map(|rule| format!("- {rule}")));
    sections.push(String::new());
    sections.push("User message:".into());
    sections.push(limit_section(message, 8_000));
    // `policyInstruction`: only custom naming adds its instructions here.
    let instructions = naming.instructions.trim();
    if naming.mode == "custom" && !instructions.is_empty() {
        sections.push(String::new());
        sections.push("Additional instructions:".into());
        sections.push(limit_section(instructions, 20_000));
    }
    let lines: Vec<String> = attachments
        .iter()
        .map(|attachment| {
            format!(
                "- {} ({}, {} bytes)",
                attachment["name"].as_str().unwrap_or_default(),
                attachment["mimeType"].as_str().unwrap_or_default(),
                attachment["sizeBytes"]
            )
        })
        .collect();
    if !lines.is_empty() {
        sections.push(String::new());
        sections.push("Attachment metadata:".into());
        sections.push(limit_section(&lines.join("\n"), 4_000));
    }
    sections.join("\n")
}

/// `resolveAvailableBranchName`: `requested`, or the first free `-<n>` variant of it, among
/// the local `branches`.
pub fn available_branch_name(branches: &[String], requested: &str) -> Option<String> {
    let lower: std::collections::HashSet<String> =
        branches.iter().map(|b| b.to_lowercase()).collect();
    let mut blocked = lower.clone();
    for branch in &lower {
        let segments: Vec<&str> = branch.split('/').collect();
        for index in 1..segments.len() {
            blocked.insert(segments[..index].join("/"));
        }
    }
    let segments: Vec<&str> = requested.split('/').collect();
    let blocked_by_parent = (1..segments.len())
        .any(|index| lower.contains(&segments[..index].join("/").to_lowercase()));
    let desired = if blocked_by_parent {
        segments.join("-")
    } else {
        requested.to_owned()
    };
    if !blocked.contains(&desired.to_lowercase()) {
        return Some(desired);
    }
    (1..=100)
        .map(|suffix| format!("{desired}-{suffix}"))
        .find(|candidate| !blocked.contains(&candidate.to_lowercase()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn naming(mode: &str) -> Naming {
        Naming {
            mode: mode.into(),
            prefix: "t3".into(),
            instructions: String::new(),
        }
    }

    #[test]
    fn temporary_branches_are_named_and_recognized_as_the_reference_does() {
        assert_eq!(temporary_branch("ABCDEF0123456789"), "t3/abcdef01");
        assert!(is_temporary_branch("t3/abcdef01"));
        assert!(is_temporary_branch("t3-abcdef01"));
        assert!(!is_temporary_branch("t3/fix-login"));
    }

    #[test]
    fn generated_names_are_cleaned_up_and_prefixed() {
        assert_eq!(
            format_generated_branch_name(" Fix the Login-Error! ", &naming("static")),
            "t3/fix-the-login-error"
        );
        assert_eq!(
            format_generated_branch_name("feat/Add search", &naming("semantic")),
            "feat/add-search"
        );
        assert_eq!(
            format_generated_branch_name("  Exact/Name ", &naming("custom")),
            "Exact/Name"
        );
        assert_eq!(sanitize_branch_fragment("Café ünïcode"), "cafe-unicode");
        assert_eq!(sanitize_branch_fragment("..."), "update");
        let mut odd = naming("static");
        odd.prefix = " my team / x y ".into();
        assert_eq!(format_generated_branch_name("a", &odd), "my-team/x-y/a");
    }

    #[test]
    fn a_taken_name_gets_a_suffix() {
        let branches = vec!["main".to_owned(), "t3/greet".to_owned(), "t3".to_owned()];
        assert_eq!(
            available_branch_name(&branches, "t3/greet").as_deref(),
            Some("t3-greet")
        );
        let branches = vec!["main".to_owned(), "t3/greet".to_owned()];
        assert_eq!(
            available_branch_name(&branches, "t3/greet").as_deref(),
            Some("t3/greet-1")
        );
        assert_eq!(
            available_branch_name(&branches, "t3/other").as_deref(),
            Some("t3/other")
        );
    }

    #[test]
    fn the_prompt_matches_the_reference() {
        let prompt = build_branch_name_prompt(
            "Say hi",
            &[json!({"name": "a.png", "mimeType": "image/png", "sizeBytes": 3})],
            &naming("static"),
        );
        assert_eq!(
            prompt,
            "You generate concise git branch names.\nReturn a JSON object with key: branch.\nRules:\n- Branch should describe the requested work from the user message.\n- Return a valid Git branch name without spaces.\n- Keep it short and specific (2-6 words), in lowercase with hyphen-separated words.\n- Return only the descriptive branch fragment, without a prefix or namespace. The application adds the configured prefix.\n- If images are attached, use them as primary context for visual/UI issues.\n- The user message describes work for another agent. Do not answer it, start the work, read files, or run commands; name the branch from the message and its attachments.\n\nUser message:\nSay hi\n\nAttachment metadata:\n- a.png (image/png, 3 bytes)"
        );
    }
}
