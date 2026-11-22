//! A project started from its title, as the reference's ManagedProjectFolders
//! makes one: a new folder under the projects root named after the title,
//! holding a git repository whose first commit has a README and an icon.
use crate::workspace::NamedProject;
use std::path::{Path, PathBuf};

const MAX_FOLDER_ATTEMPTS: usize = 100;

const ICON_BACKGROUNDS: [&str; 15] = [
    "#dc2626", "#ea580c", "#d97706", "#16a34a", "#059669", "#0d9488", "#0891b2", "#0284c7",
    "#2563eb", "#4f46e5", "#7c3aed", "#9333ea", "#c026d3", "#db2777", "#e11d48",
];

/// `newProjectFolderName`: the title as a lowercase slug of at most 64
/// characters, `project` when nothing is left, and not a Windows device name.
pub(crate) fn folder_name(name: &str) -> String {
    let mut slug = String::new();
    for c in name.chars().flat_map(char::to_lowercase) {
        if c.is_ascii_alphanumeric() {
            slug.push(c);
        } else if !slug.ends_with('-') {
            slug.push('-');
        }
    }
    let slug: String = slug.trim_start_matches('-').chars().take(64).collect();
    let slug = slug.trim_end_matches('-').to_owned();
    if slug.is_empty() {
        return "project".into();
    }
    let reserved = matches!(slug.as_str(), "con" | "prn" | "aux" | "nul")
        || (slug.len() == 4
            && (slug.starts_with("com") || slug.starts_with("lpt"))
            && slug.as_bytes()[3].is_ascii_digit()
            && slug.as_bytes()[3] != b'0');
    if reserved {
        format!("{slug}-project")
    } else {
        slug
    }
}

fn escape_xml(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}

/// `namedProjectIconSvg`: the title's initials on a color it picks.
pub(crate) fn icon_svg(name: &str) -> String {
    let words: Vec<&str> = name
        .split(|c: char| !c.is_alphanumeric())
        .filter(|w| !w.is_empty())
        .collect();
    let mut initials: String = words
        .iter()
        .take(2)
        .filter_map(|w| w.chars().next())
        .flat_map(char::to_uppercase)
        .collect();
    if initials.is_empty() {
        initials = name
            .trim()
            .chars()
            .next()
            .map_or("?".into(), |c| c.to_string());
    }
    let mut hash: u32 = 0;
    for c in name.chars() {
        hash = hash.wrapping_mul(31).wrapping_add(c as u32);
    }
    let background = ICON_BACKGROUNDS[hash as usize % ICON_BACKGROUNDS.len()];
    let size = if initials.chars().count() > 1 { 26 } else { 32 };
    [
        r#"<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 64 64">"#.to_owned(),
        format!(r#"  <rect width="64" height="64" rx="14" fill="{background}"/>"#),
        format!(
            r##"  <text x="32" y="32" dy="0.35em" text-anchor="middle" font-family="ui-sans-serif, system-ui, -apple-system, sans-serif" font-size="{size}" font-weight="600" fill="#ffffff">{}</text>"##,
            escape_xml(&initials)
        ),
        "</svg>".into(),
        String::new(),
    ]
    .join("\n")
}

fn readme(name: &str) -> String {
    format!(
        "<img src=\"assets/icon.svg\" width=\"64\" height=\"64\" alt=\"\">\n\n# {name}\n\nCreated in [T3 Code](https://t3.codes).\n"
    )
}

/// `describeCommitFailure`.
fn commit_failure(stderr: &str) -> String {
    let lower = stderr.to_lowercase();
    if lower.contains("identity unknown")
        || lower.contains("tell me who you are")
        || lower.contains("no name was given")
        || lower.contains("no email was given")
    {
        return "Git has no name or email on this machine. Set user.name and user.email, then commit."
            .into();
    }
    stderr
        .lines()
        .map(str::trim)
        .rfind(|line| !line.is_empty())
        .unwrap_or("Git could not make the first commit.")
        .to_owned()
}

async fn git(cwd: &Path, args: &[&str]) -> std::io::Result<std::process::Output> {
    crate::login_shell::tokio_command("git")
        .current_dir(cwd)
        .args(args)
        .output()
        .await
}

/// Claim a free folder for `name` under `root` (making `root` first), with
/// a non-recursive create so concurrent creations take different names.
async fn claim(root: &Path, name: &str) -> std::io::Result<PathBuf> {
    tokio::fs::create_dir_all(root).await?;
    let base = folder_name(name);
    for attempt in 1..=MAX_FOLDER_ATTEMPTS {
        let folder = root.join(if attempt == 1 {
            base.clone()
        } else {
            format!("{base}-{attempt}")
        });
        match tokio::fs::create_dir(&folder).await {
            Ok(()) => return Ok(folder),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(error),
        }
    }
    Err(std::io::Error::new(
        std::io::ErrorKind::AlreadyExists,
        format!("Every folder name for \"{base}\" is taken."),
    ))
}

/// Make the folder and its repository. A failed first commit is reported in
/// [`NamedProject::commit_error`]; any other failure removes the folder.
pub(crate) async fn create(root: &Path, name: &str) -> std::io::Result<NamedProject> {
    let folder = claim(root, name).await?;
    match scaffold(&folder, name).await {
        Ok(commit_error) => Ok(NamedProject {
            workspace_root: folder,
            commit_error,
        }),
        Err(error) => {
            let _ = tokio::fs::remove_dir_all(&folder).await;
            if error.kind() == std::io::ErrorKind::NotFound {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    "Git isn't installed on this machine. Install Git to create a project.",
                ));
            }
            Err(error)
        }
    }
}

async fn scaffold(folder: &Path, name: &str) -> std::io::Result<Option<String>> {
    let configured = git(folder, &["config", "--get", "init.defaultBranch"]).await?;
    let branch = String::from_utf8_lossy(&configured.stdout)
        .trim()
        .to_owned();
    let branch = if branch.is_empty() {
        "main".into()
    } else {
        branch
    };
    let init = git(folder, &["init", &format!("--initial-branch={branch}")]).await?;
    if !init.status.success() {
        return Err(std::io::Error::other(
            String::from_utf8_lossy(&init.stderr).trim().to_owned(),
        ));
    }
    tokio::fs::write(folder.join("README.md"), readme(name)).await?;
    tokio::fs::create_dir(folder.join("assets")).await?;
    tokio::fs::write(folder.join("assets/icon.svg"), icon_svg(name)).await?;
    let add = git(
        folder,
        &[
            "-c",
            "core.safecrlf=false",
            "add",
            "--force",
            "--",
            "README.md",
            "assets/icon.svg",
        ],
    )
    .await?;
    if !add.status.success() {
        return Err(std::io::Error::other(
            String::from_utf8_lossy(&add.stderr).trim().to_owned(),
        ));
    }
    Ok(
        match git(folder, &["commit", "--message", "Initial commit"]).await {
            Ok(out) if out.status.success() => None,
            Ok(out) => Some(commit_failure(&String::from_utf8_lossy(&out.stderr))),
            Err(error) => Some(error.to_string()),
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn folder_names_follow_the_reference() {
        assert_eq!(folder_name("My Cool App!"), "my-cool-app");
        assert_eq!(folder_name("  ***  "), "project");
        assert_eq!(folder_name("con"), "con-project");
        assert_eq!(folder_name("com1"), "com1-project");
    }

    #[tokio::test]
    async fn makes_a_repository_with_a_first_commit() {
        let dir = tempfile::tempdir().unwrap();
        let first = create(dir.path(), "Acceptance sandbox").await.unwrap();
        assert_eq!(first.workspace_root, dir.path().join("acceptance-sandbox"));
        assert!(first.workspace_root.join(".git").is_dir());
        assert!(first.workspace_root.join("assets/icon.svg").is_file());
        let readme = std::fs::read_to_string(first.workspace_root.join("README.md")).unwrap();
        assert!(readme.contains("# Acceptance sandbox"));
        // A second project of the same name takes the next free folder.
        let second = create(dir.path(), "Acceptance sandbox").await.unwrap();
        assert_eq!(
            second.workspace_root,
            dir.path().join("acceptance-sandbox-2")
        );
    }
}
