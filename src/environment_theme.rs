//! The environment themes published in `<state>/themes`, as the reference's
//! `EnvironmentThemeService` reads, publishes and selects them: theme files
//! `<id>.json`, each with an optional background image beside it, and the
//! default theme and appearance in the settings.
use crate::mcp_oauth::themes;
use crate::mcp_oauth::ThemeRefusal;
use serde_json::{json, Value};
use std::path::{Path, PathBuf};

const MAX_THEME_FILES: usize = 32;
const MAX_THEME_TOTAL_BYTES: usize = 2 * 1024 * 1024;
const BACKGROUND_EXTENSIONS: [&str; 4] = ["webp", "png", "jpg", "jpeg"];
const MAX_BACKGROUND_BYTES: u64 = 16 * 1024 * 1024;
const INVALID_ID: &str =
    "is not a valid theme id. Use lowercase letters, digits, and hyphens, and not a built-in name.";
const NOT_PUBLISHED: &str =
    "could not be published. The themes directory may be full or not writable.";

fn refused(id: &str, problem: &str, known: Option<&[String]>) -> ThemeRefusal {
    let known = known.map_or(String::new(), |ids| {
        format!(" Available: {}.", ids.join(", "))
    });
    ThemeRefusal::Refused(format!("Theme \"{id}\" {problem}{known}"))
}

/// A regular file's text, never through a symlink or from a FIFO, and only up
/// to `max` bytes (`readThemeFileGuarded`). A UTF-8 BOM is dropped.
pub(crate) fn read_guarded(path: &Path, max: usize) -> Option<String> {
    use std::io::Read;
    #[cfg(unix)]
    let file = {
        use std::os::unix::fs::OpenOptionsExt;
        std::fs::OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
            .open(path)
            .ok()?
    };
    #[cfg(not(unix))]
    let file = {
        if std::fs::symlink_metadata(path)
            .ok()?
            .file_type()
            .is_symlink()
        {
            return None;
        }
        std::fs::File::open(path).ok()?
    };
    let info = file.metadata().ok()?;
    if !info.is_file() || info.len() as usize > max {
        return None;
    }
    let mut bytes = Vec::with_capacity(info.len() as usize);
    file.take(info.len()).read_to_end(&mut bytes).ok()?;
    let text = String::from_utf8_lossy(&bytes).into_owned();
    Some(
        text.strip_prefix('\u{feff}')
            .map(str::to_owned)
            .unwrap_or(text),
    )
}

fn base36(mut n: u128) -> String {
    const DIGITS: &[u8] = b"0123456789abcdefghijklmnopqrstuvwxyz";
    if n == 0 {
        return "0".into();
    }
    let mut out = Vec::new();
    while n > 0 {
        out.push(DIGITS[(n % 36) as usize]);
        n /= 36;
    }
    out.reverse();
    String::from_utf8(out).unwrap_or_default()
}

/// The revision of the background image published beside `id`, if any:
/// its inode, size and modification time in nanoseconds, in base 36.
fn background(dir: &Path, id: &str, entries: &[String]) -> Option<String> {
    for extension in BACKGROUND_EXTENSIONS {
        let name = format!("{id}.{extension}");
        if !entries.contains(&name) {
            continue;
        }
        let Ok(info) = std::fs::symlink_metadata(dir.join(&name)) else {
            continue;
        };
        if !info.is_file() || info.len() > MAX_BACKGROUND_BYTES {
            continue;
        }
        #[cfg(unix)]
        let (inode, nanos) = {
            use std::os::unix::fs::MetadataExt;
            (
                info.ino() as u128,
                (info.mtime() as i128 * 1_000_000_000 + info.mtime_nsec() as i128).max(0) as u128,
            )
        };
        #[cfg(not(unix))]
        let (inode, nanos) = (
            0u128,
            info.modified()
                .ok()
                .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                .map_or(0, |d| d.as_nanos()),
        );
        return Some([inode, info.len() as u128, nanos].map(base36).join("-"));
    }
    None
}

/// The themes `dir` publishes, by file name, within the reference's limits
/// (`readPublishedThemes`): at most 32 files examined and 2 MiB in all.
pub(crate) fn published(dir: &Path) -> Vec<Value> {
    let mut entries: Vec<String> = std::fs::read_dir(dir)
        .map(|entries| {
            entries
                .flatten()
                .map(|entry| entry.file_name().to_string_lossy().into_owned())
                .collect()
        })
        .unwrap_or_default();
    entries.sort();
    let mut themes = Vec::new();
    let mut examined = 0;
    let mut total = 0;
    for entry in &entries {
        let Some(id) = entry.strip_suffix(".json") else {
            continue;
        };
        if !themes::valid_id(id) || themes::unpublishable(id) {
            continue;
        }
        examined += 1;
        if examined > MAX_THEME_FILES {
            break;
        }
        let Some(raw) = read_guarded(&dir.join(entry), themes::max_file_bytes()) else {
            continue;
        };
        if raw.trim().is_empty() {
            continue;
        }
        let Ok(file) = serde_json::from_str::<Value>(&raw)
            .map_err(|_| ())
            .and_then(|v| themes::decode_file(&v, &[]).map_err(|_| ()))
        else {
            continue;
        };
        if !themes::has_colors(&file) {
            continue;
        }
        total += raw.len();
        if total > MAX_THEME_TOTAL_BYTES {
            break;
        }
        let mut theme = json!({"id": id});
        for (key, value) in file.as_object().into_iter().flatten() {
            theme[key] = value.clone();
        }
        if let Some(revision) = background(dir, id, &entries) {
            theme["background"] = json!(revision);
        }
        themes.push(theme);
    }
    themes
}

fn served_ids(dir: &Path) -> Vec<String> {
    published(dir)
        .iter()
        .filter_map(|t| t["id"].as_str().map(str::to_owned))
        .collect()
}

/// What a publish replaced, to put back if the theme cannot be selected.
pub(crate) struct Revert {
    destination: PathBuf,
    previous: Option<String>,
}

impl Revert {
    pub(crate) fn run(self) {
        match self.previous {
            Some(contents) => {
                let _ = replace(&self.destination, &contents);
            }
            None => {
                let _ = std::fs::remove_file(&self.destination);
            }
        }
    }
}

/// Writes `contents` to `destination` through a staging file created
/// exclusively, so a link left at the staging path is never written through.
fn replace(destination: &Path, contents: &str) -> std::io::Result<()> {
    use std::io::Write;
    let staging = PathBuf::from(format!(
        "{}.staging-{}",
        destination.display(),
        std::process::id()
    ));
    let _ = std::fs::remove_file(&staging);
    let written = (|| {
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&staging)?;
        file.write_all(contents.as_bytes())?;
        std::fs::rename(&staging, destination)
    })();
    if written.is_err() {
        let _ = std::fs::remove_file(&staging);
    }
    written
}

/// Publishes `file` as `id` in `dir`, replacing a theme already there
/// (`publish`). Fails when the theme would not be served, or would push
/// another theme out, and then puts back what was there.
pub(crate) fn publish(dir: &Path, id: &str, file: &Value) -> Result<Revert, ThemeRefusal> {
    if themes::unpublishable(id) {
        return Err(refused(id, INVALID_ID, None));
    }
    if !themes::has_colors(file) {
        return Err(refused(
            id,
            "has no colors. Give canvas and accent, or at least one color role.",
            None,
        ));
    }
    if file["css"]
        .as_str()
        .is_some_and(themes::css_loads_remote_files)
    {
        return Err(refused(
            id,
            "has CSS that loads files. Remove @import, image-set(), and url() that is not data: or #id.",
            None,
        ));
    }
    let contents =
        serde_json::to_string_pretty(file).map_err(|_| refused(id, NOT_PUBLISHED, None))?;
    let max = themes::max_file_bytes();
    if contents.len() > max {
        return Err(refused(id, &format!("is larger than {max} bytes."), None));
    }
    let destination = dir.join(format!("{id}.json"));
    let previous = read_guarded(&destination, max);
    if previous.is_none() && std::fs::symlink_metadata(&destination).is_ok() {
        return Err(refused(
            id,
            "would replace a file that could not be backed up. Use another theme id.",
            None,
        ));
    }
    let before = served_ids(dir);
    let revert = Revert {
        destination: destination.clone(),
        previous,
    };
    std::fs::create_dir_all(dir)
        .and_then(|()| replace(&destination, &contents))
        .map_err(|_| refused(id, NOT_PUBLISHED, None))?;
    let served = served_ids(dir);
    if served.iter().any(|s| s == id) && before.iter().all(|b| served.contains(b)) {
        return Ok(revert);
    }
    revert.run();
    Err(refused(id, NOT_PUBLISHED, None))
}

/// `read`: the published themes, and the one named `id`, which must be
/// published.
pub(crate) fn read(
    dir: &Path,
    id: Option<&str>,
) -> Result<(Vec<Value>, Option<Value>), ThemeRefusal> {
    let themes = published(dir);
    let Some(id) = id else {
        return Ok((themes, None));
    };
    let theme = themes.iter().find(|t| t["id"] == id).cloned();
    match theme {
        Some(theme) => Ok((themes, Some(theme))),
        None => {
            let known: Vec<String> = themes
                .iter()
                .filter_map(|t| t["id"].as_str().map(str::to_owned))
                .collect();
            Err(refused(id, "is not a published theme.", Some(&known)))
        }
    }
}

/// `apply`: makes `id` (with `file`, first published as it) the environment
/// theme and `appearance` its mode, by the settings patch `select` writes.
pub(crate) fn apply(
    dir: &Path,
    id: Option<&str>,
    file: Option<&Value>,
    appearance: Option<&str>,
    select: impl FnOnce(Value) -> Result<(), ()>,
) -> Result<(), ThemeRefusal> {
    if let Some(id) = id.filter(|id| !themes::valid_id(id)) {
        return Err(refused(id, INVALID_ID, None));
    }
    // One at a time, so a selection never sees a file a failed publish then takes back.
    static PUBLISHING: std::sync::Mutex<()> = std::sync::Mutex::new(());
    let _publishing = PUBLISHING.lock().unwrap_or_else(|e| e.into_inner());
    // Timestamps make clients apply a choice again even when its value did not
    // change; the stock theme is an empty default with a set time.
    let at = crate::auth_service::iso(crate::platform::now_ms());
    let mut patch = json!({});
    if let Some(id) = id {
        let default = if id == themes::default_theme_id() {
            ""
        } else {
            id
        };
        patch["defaultTheme"] = json!(default);
        patch["defaultThemeSetAt"] = json!(at);
    }
    if let Some(appearance) = appearance {
        patch["defaultAppearance"] = json!(appearance);
        patch["defaultAppearanceSetAt"] = json!(at);
    }
    let Some(id) = id else {
        return select(patch).map_err(|()| ThemeRefusal::NotSelected);
    };
    let Some(file) = file else {
        let mut known = themes::built_in();
        known.extend(served_ids(dir));
        if !known.iter().any(|k| k == id) {
            return Err(refused(
                id,
                "is not a built-in or published theme.",
                Some(&known),
            ));
        }
        return select(patch).map_err(|()| ThemeRefusal::NotSelected);
    };
    let revert = publish(dir, id, file)?;
    select(patch).map_err(|()| {
        revert.run();
        ThemeRefusal::NotSelected
    })
}
