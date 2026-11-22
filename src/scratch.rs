//! Built-in "No project" scratch project and per-thread scratch folder helpers.
//!
//! Platform-neutral so both the native server and the embeddable control plane
//! share the same `projects.ensureScratch` and per-thread scratch folder
//! semantics through [`Workspace`].
use crate::{control_state::ControlState, workspace::Workspace, Event, Project};
use serde_json::{json, Value};
use std::path::{Path, PathBuf};

pub(crate) const SCRATCH_PROJECT_TITLE: &str = "No project";

const MAX_THREAD_FOLDER_ATTEMPTS: usize = 100;

pub(crate) fn scratch_project_icon() -> Value {
    json!({"kind":"lucide","name":"message-square-dashed","color":"gray"})
}

/// Ensures `<root>` exists in `workspace` and a single active "No project"
/// project is registered for its canonical path. Repeated or concurrent calls
/// return the same `projectId`, and a deleted folder is recreated.
///
/// `commit` records the new project as the caller records projects (so its
/// subscribers hear of it) and answers whether that succeeded.
pub(crate) async fn ensure_project<F, Fut>(
    workspace: &dyn Workspace,
    root: &Path,
    state: &ControlState,
    mutation_lock: &tokio::sync::Mutex<()>,
    commit: F,
) -> Result<String, Value>
where
    F: FnOnce(Event) -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let failure =
        |message: String| json!({"_tag":"OrchestrationDispatchCommandError","message":message});
    workspace
        .create_dir_all(root)
        .await
        .map_err(|error| failure(error.to_string()))?;
    let root = workspace
        .canonicalize(root)
        .await
        .map_err(|error| failure(error.to_string()))?;
    let _guard = mutation_lock.lock().await;
    let projects: Vec<_> = state
        .store
        .read()
        .await
        .projects
        .values()
        .cloned()
        .collect();
    for project in projects {
        let path = Path::new(&project.path);
        if path == root.as_path()
            || workspace.canonicalize(path).await.ok().as_deref() == Some(root.as_path())
        {
            return Ok(project.id);
        }
    }
    let id = crate::platform::uuid_v4().to_string();
    let project = Project {
        id: id.clone(),
        name: SCRATCH_PROJECT_TITLE.into(),
        path: root.to_string_lossy().into_owned(),
        created_at: crate::now(),
    };
    let mut row = crate::store::wire_project(&project);
    row["projectIcon"] = scratch_project_icon();
    if !commit(Event::ProjectCommitted {
        project,
        row,
        command_id: format!("scratch-project:{id}"),
        command_type: "project.create".into(),
        removed_threads: vec![],
    })
    .await
    {
        return Err(failure("Failed to create scratch project.".into()));
    }
    Ok(id)
}

/// Whether `project_id` is the built-in "No project" scratch project for `root`.
pub(crate) async fn is_project(
    workspace: &dyn Workspace,
    root: &Path,
    state: &ControlState,
    project_id: &str,
) -> bool {
    let Some(path) = state
        .store
        .read()
        .await
        .projects
        .get(project_id)
        .map(|project| PathBuf::from(&project.path))
    else {
        return false;
    };
    if path == root {
        return true;
    }
    match (
        workspace.canonicalize(root).await,
        workspace.canonicalize(&path).await,
    ) {
        (Ok(root), Ok(path)) => root == path,
        _ => false,
    }
}

/// Allocates and creates a fresh per-thread folder under `root`, named
/// `YYYY-MM-DD-<words>-<id>` as the reference's `ManagedProjectFolders` does.
pub(crate) async fn folder_for_thread(
    workspace: &dyn Workspace,
    root: &Path,
    thread_id: &str,
    text: &str,
) -> Result<String, String> {
    workspace
        .create_dir_all(root)
        .await
        .map_err(|error| error.to_string())?;
    let root = workspace
        .canonicalize(root)
        .await
        .map_err(|error| error.to_string())?;
    let iso = crate::auth_service::iso(crate::auth_service::now());
    let date = iso.get(..10).unwrap_or("1970-01-01");
    let short = thread_folder_name(date, text, thread_id, false);
    let full = thread_folder_name(date, text, thread_id, true);
    for attempt in 1..=MAX_THREAD_FOLDER_ATTEMPTS {
        let name = match attempt {
            1 => short.clone(),
            2 => full.clone(),
            _ => {
                let uuid = crate::platform::uuid_v4().to_string();
                let suffix = uuid.get(..8).unwrap_or(&uuid);
                format!("{full}-{suffix}")
            }
        };
        let candidate = root.join(name);
        if workspace.is_dir(&candidate).await {
            continue;
        }
        workspace
            .create_dir_all(&candidate)
            .await
            .map_err(|error| error.to_string())?;
        let resolved = workspace
            .canonicalize(&candidate)
            .await
            .unwrap_or(candidate);
        return Ok(resolved.to_string_lossy().into_owned());
    }
    Err("Failed to create the folder for threads without a project.".into())
}

/// Folds accented Latin letters to ASCII; other scripts pass through.
fn fold_latin_diacritics(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for ch in text.chars() {
        let folded: &str = match ch {
            'À' | 'Á' | 'Â' | 'Ã' | 'Ä' | 'Å' | 'Ā' | 'Ă' | 'Ą' => "A",
            'à' | 'á' | 'â' | 'ã' | 'ä' | 'å' | 'ā' | 'ă' | 'ą' => "a",
            'Æ' => "AE",
            'æ' => "ae",
            'Ç' | 'Ć' | 'Ĉ' | 'Ċ' | 'Č' => "C",
            'ç' | 'ć' | 'ĉ' | 'ċ' | 'č' => "c",
            'Ð' | 'Ď' | 'Đ' => "D",
            'ð' | 'ď' | 'đ' => "d",
            'È' | 'É' | 'Ê' | 'Ë' | 'Ē' | 'Ĕ' | 'Ė' | 'Ę' | 'Ě' => "E",
            'è' | 'é' | 'ê' | 'ë' | 'ē' | 'ĕ' | 'ė' | 'ę' | 'ě' => "e",
            'Ĝ' | 'Ğ' | 'Ġ' | 'Ģ' => "G",
            'ĝ' | 'ğ' | 'ġ' | 'ģ' => "g",
            'Ĥ' | 'Ħ' => "H",
            'ĥ' | 'ħ' => "h",
            'Ì' | 'Í' | 'Î' | 'Ï' | 'Ĩ' | 'Ī' | 'Ĭ' | 'Į' | 'İ' => "I",
            'ì' | 'í' | 'î' | 'ï' | 'ĩ' | 'ī' | 'ĭ' | 'į' | 'ı' => "i",
            'Ĵ' => "J",
            'ĵ' => "j",
            'Ķ' => "K",
            'ķ' => "k",
            'ĸ' => "q",
            'Ĺ' | 'Ļ' | 'Ľ' | 'Ŀ' | 'Ł' => "L",
            'ĺ' | 'ļ' | 'ľ' | 'ŀ' | 'ł' => "l",
            'Ñ' | 'Ń' | 'Ņ' | 'Ň' | 'Ŋ' => "N",
            'ñ' | 'ń' | 'ņ' | 'ň' | 'ŋ' => "n",
            'Ò' | 'Ó' | 'Ô' | 'Õ' | 'Ö' | 'Ø' | 'Ō' | 'Ŏ' | 'Ő' => "O",
            'ò' | 'ó' | 'ô' | 'õ' | 'ö' | 'ø' | 'ō' | 'ŏ' | 'ő' => "o",
            'Œ' => "OE",
            'œ' => "oe",
            'Ŕ' | 'Ŗ' | 'Ř' => "R",
            'ŕ' | 'ŗ' | 'ř' => "r",
            'Ś' | 'Ŝ' | 'Ş' | 'Š' => "S",
            'ś' | 'ŝ' | 'ş' | 'š' => "s",
            'ß' => "ss",
            'ẞ' => "SS",
            'Ţ' | 'Ť' | 'Ŧ' => "T",
            'ţ' | 'ť' | 'ŧ' => "t",
            'Þ' => "TH",
            'þ' => "th",
            'Ù' | 'Ú' | 'Û' | 'Ü' | 'Ũ' | 'Ū' | 'Ŭ' | 'Ů' | 'Ű' | 'Ų' => "U",
            'ù' | 'ú' | 'û' | 'ü' | 'ũ' | 'ū' | 'ŭ' | 'ů' | 'ű' | 'ų' => "u",
            'Ŵ' => "W",
            'ŵ' => "w",
            'Ý' | 'Ÿ' | 'Ŷ' => "Y",
            'ý' | 'ÿ' | 'ŷ' => "y",
            'Ź' | 'Ż' | 'Ž' => "Z",
            'ź' | 'ż' | 'ž' => "z",
            '\u{0300}'..='\u{036f}' => continue,
            other => {
                out.push(other);
                continue;
            }
        };
        out.push_str(folded);
    }
    out
}

/// First 5 ASCII lowercase alphanumeric words joined by `-`, capped at 48
/// characters with no trailing `-`.
fn folder_words(text: &str) -> String {
    let folded = fold_latin_diacritics(text).to_ascii_lowercase();
    let mut joined = folded
        .split(|c: char| !c.is_ascii_alphanumeric())
        .filter(|word| !word.is_empty())
        .take(5)
        .collect::<Vec<_>>()
        .join("-");
    if joined.len() > 48 {
        joined.truncate(48);
    }
    while joined.ends_with('-') {
        joined.pop();
    }
    joined
}

fn thread_folder_name(date: &str, text: &str, thread_id: &str, full_id: bool) -> String {
    let words = folder_words(text);
    let id: String = thread_id
        .to_ascii_lowercase()
        .chars()
        .filter(|c| c.is_ascii_alphanumeric())
        .collect();
    let id_part = if full_id {
        id.as_str()
    } else {
        &id[id.len().saturating_sub(8)..]
    };
    [date, words.as_str(), id_part]
        .into_iter()
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>()
        .join("-")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn thread_folder_names_match_reference_slug_rules() {
        assert_eq!(
            thread_folder_name(
                "2026-11-22",
                "Convert these PNGs to WebP, please!",
                "thread:a:0123456789abcdef",
                false,
            ),
            "2026-11-22-convert-these-pngs-to-webp-89abcdef"
        );
        assert_eq!(
            thread_folder_name(
                "2026-11-22",
                "Café über Straße",
                "thread:0123456789abcdef",
                false,
            ),
            "2026-11-22-cafe-uber-strasse-89abcdef"
        );
        let pasted = format!("{}../../etc", "word ".repeat(10_000));
        let name = thread_folder_name("2026-11-22", &pasted, "thread-pasted", false);
        assert!(name.len() <= 80, "{name}");
        assert!(!name.contains(".."), "{name}");
        assert_eq!(name, "2026-11-22-word-word-word-word-word-adpasted");
        assert_eq!(
            thread_folder_name("2026-11-22", "!!!", "thread:abcdef12", false),
            "2026-11-22-abcdef12"
        );
    }
}
