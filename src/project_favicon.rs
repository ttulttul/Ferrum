//! Project favicons: `assets.createUrl` for a `project-favicon` resource and the
//! favicon it serves (`ProjectFaviconResolver` and `issueAssetUrl`'s
//! `project-favicon` case in the reference).
//!
//! A project's favicon is, in order: the project's `faviconPath` override,
//! `t3.json`'s `iconPath`, the first of the well-known icon files, or the icon a
//! page source links. Lookups are cached for ten minutes (one when nothing was
//! found). While a clone is creating the project's checkout, no lookup is made,
//! since a miss cached from a half-cloned tree would outlive the clone; once it
//! lands, cached lookups for the folder are dropped first.
use crate::rpc_protocol::Reply;
use crate::AppState;
use regex::Regex;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::path::{Component, Path, PathBuf};
use std::sync::{LazyLock, Mutex};
use std::time::{Duration, Instant};

const POSITIVE_TTL: Duration = Duration::from_secs(10 * 60);
const NEGATIVE_TTL: Duration = Duration::from_secs(60);
const CACHE_CAPACITY: usize = 512;
const ICON_SOURCE_SCAN_BYTES: usize = 128 * 1024;
const TOKEN_BUCKET_MS: u64 = 30 * 60 * 1000;
const ICNS_MIN_SIZE: u32 = 96;
pub(crate) const FALLBACK_MARKER: &str = "project-favicon-missing";

const CANDIDATES: [&str; 23] = [
    "favicon.svg",
    "favicon.ico",
    "favicon.png",
    "public/favicon.svg",
    "public/favicon.ico",
    "public/favicon.png",
    "app/favicon.ico",
    "app/favicon.png",
    "app/icon.svg",
    "app/icon.png",
    "app/icon.ico",
    "src/favicon.ico",
    "src/favicon.svg",
    "src/app/favicon.ico",
    "src/app/icon.svg",
    "src/app/icon.png",
    "assets/icon.svg",
    "assets/icon.png",
    "assets/logo.svg",
    "assets/logo.png",
    ".idea/icon.svg",
    "Resources/AppIcon.icns",
    "AppIcon.icns",
];

const ICON_SOURCE_FILES: [&str; 7] = [
    "index.html",
    "public/index.html",
    "app/routes/__root.tsx",
    "src/routes/__root.tsx",
    "app/root.tsx",
    "src/root.tsx",
    "src/index.html",
];

/// `PROJECT_FAVICON_EXTENSIONS`: the workspace image previews and `.icns`.
const EXTENSIONS: [&str; 10] = [
    ".avif", ".bmp", ".gif", ".ico", ".jpeg", ".jpg", ".png", ".svg", ".webp", ".icns",
];

pub(crate) fn is_favicon_path(path: &str) -> bool {
    let lower = path.to_lowercase();
    EXTENSIONS.iter().any(|e| lower.ends_with(e))
}

static HTML_LINK: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"(?i)<link\b[^<>]*>").unwrap());
static HTML_ICON_REL: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r#"(?i)\brel=["'](?:icon|shortcut icon)["']"#).unwrap());
static HTML_ICON_HREF: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r#"(?i)\bhref=["']([^"'?]+)"#).unwrap());
static ICON_REL: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r#"(?i)\brel\s*:\s*["'](?:icon|shortcut icon)["']"#).unwrap());
static ICON_HREF: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r#"(?i)\bhref\s*:\s*["']([^"'?]+)"#).unwrap());

/// `extractIconHref`: the first icon link in an HTML page or a route's `links`.
fn icon_href(source: &str) -> Option<String> {
    for tag in HTML_LINK.find_iter(source) {
        if HTML_ICON_REL.is_match(tag.as_str()) {
            if let Some(href) = HTML_ICON_HREF.captures(tag.as_str()) {
                return Some(href[1].to_owned());
            }
        }
    }
    source.split('}').find_map(|run| {
        ICON_REL
            .is_match(run)
            .then(|| ICON_HREF.captures(run).map(|h| h[1].to_owned()))
            .flatten()
    })
}

/// `resolveRelativePathWithinRoot`: `relative` under `root`, lexically, or `None` when
/// it is absolute or leaves the root.
fn within_root(root: &Path, relative: &str) -> Option<PathBuf> {
    if relative.is_empty() || Path::new(relative).is_absolute() {
        return None;
    }
    let mut out = root.to_path_buf();
    for part in Path::new(relative).components() {
        match part {
            Component::Normal(name) => out.push(name),
            Component::CurDir => {}
            Component::ParentDir => {
                if !out.pop() || !out.starts_with(root) {
                    return None;
                }
            }
            _ => return None,
        }
    }
    (out != root && out.starts_with(root)).then_some(out)
}

fn is_file(path: &Path) -> bool {
    std::fs::metadata(path).is_ok_and(|m| m.is_file())
}

/// `findExistingFile`: the first candidate that is a file. An absolute override is
/// taken as it is (`filesystem` scope); workspace candidates must stay in the root.
fn existing(root: &Path, candidates: &[&str], filesystem: bool) -> Option<PathBuf> {
    candidates.iter().find_map(|candidate| {
        let path = if filesystem && Path::new(candidate).is_absolute() {
            PathBuf::from(candidate)
        } else {
            within_root(root, candidate)?
        };
        is_file(&path).then_some(path)
    })
}

/// `T3ProjectFile.iconPath`, trimmed, when `t3.json` names one.
fn project_file_icon(root: &Path) -> Option<String> {
    let raw = std::fs::read_to_string(root.join("t3.json")).ok()?;
    let file = crate::local_services::vcs::worktree::project_file(&raw)?;
    let icon = file.get("iconPath")?.as_str()?.trim();
    (!icon.is_empty() && icon.encode_utf16().count() <= 512).then(|| icon.to_owned())
}

fn icon_from_source(root: &Path, source_file: &str) -> Option<PathBuf> {
    let path = within_root(root, source_file)?;
    if !is_file(&path) {
        return None;
    }
    let mut bytes = std::fs::read(&path).ok()?;
    bytes.truncate(ICON_SOURCE_SCAN_BYTES);
    let href = icon_href(&String::from_utf8_lossy(&bytes))?;
    // Browsers strip surrounding whitespace from URL attributes; file paths keep theirs.
    let clean = href.trim();
    let clean = clean.strip_prefix('/').unwrap_or(clean);
    let public = format!("public/{clean}");
    existing(root, &[public.as_str(), clean], false)
}

/// `normalizeWorkspaceRoot`: the absolute folder, which must exist and be a folder.
pub(crate) fn normalize_root(cwd: &str) -> Result<PathBuf, &'static str> {
    let expanded = crate::rpc::expand_user_path(cwd.trim());
    let absolute = std::path::absolute(&expanded).map_err(|_| "WorkspaceRootStatFailedError")?;
    match std::fs::metadata(&absolute) {
        Ok(m) if m.is_dir() => Ok(absolute),
        Ok(_) => Err("WorkspaceRootNotDirectoryError"),
        Err(_) => Err("WorkspaceRootNotExistsError"),
    }
}

/// `resolvePathUncached`.
fn resolve_uncached(root: &Path, favicon_path: Option<&str>) -> Option<PathBuf> {
    if let Some(found) = favicon_path.and_then(|p| existing(root, &[p], true)) {
        return Some(found);
    }
    if let Some(found) =
        project_file_icon(root).and_then(|icon| existing(root, &[icon.as_str()], false))
    {
        return Some(found);
    }
    existing(root, &CANDIDATES, false).or_else(|| {
        ICON_SOURCE_FILES
            .iter()
            .find_map(|s| icon_from_source(root, s))
    })
}

struct Cached {
    path: Option<PathBuf>,
    expires: Instant,
}

static CACHE: LazyLock<Mutex<HashMap<(String, String), Cached>>> = LazyLock::new(Mutex::default);

fn cache_key(cwd: &Path, favicon_path: Option<&str>) -> (String, String) {
    (
        favicon_path.unwrap_or_default().to_owned(),
        cwd.display().to_string(),
    )
}

fn cached_lookup(root: &Path, favicon_path: Option<&str>) -> Option<PathBuf> {
    let key = cache_key(root, favicon_path);
    let now = Instant::now();
    if let Some(entry) = CACHE.lock().unwrap().get(&key).filter(|e| e.expires > now) {
        return entry.path.clone();
    }
    let path = resolve_uncached(root, favicon_path);
    let ttl = if path.is_some() {
        POSITIVE_TTL
    } else {
        NEGATIVE_TTL
    };
    let mut cache = CACHE.lock().unwrap();
    if cache.len() >= CACHE_CAPACITY {
        cache.retain(|_, e| e.expires > now);
        if cache.len() >= CACHE_CAPACITY {
            if let Some(oldest) = cache
                .iter()
                .min_by_key(|(_, e)| e.expires)
                .map(|(k, _)| k.clone())
            {
                cache.remove(&oldest);
            }
        }
    }
    cache.insert(
        key,
        Cached {
            path: path.clone(),
            expires: now + ttl,
        },
    );
    path
}

/// `resolvePath`: the cached favicon, looked up again if its file went away.
pub(crate) fn resolve(root: &Path, favicon_path: Option<&str>) -> Option<PathBuf> {
    let found = cached_lookup(root, favicon_path)?;
    if is_file(&found) {
        return Some(found);
    }
    CACHE.lock().unwrap().remove(&cache_key(root, favicon_path));
    cached_lookup(root, favicon_path)
}

/// `invalidate`: forgets every lookup for the folder `root`.
pub(crate) fn invalidate(root: &Path) {
    let cwd = root.display().to_string();
    CACHE
        .lock()
        .unwrap()
        .retain(|(_, key_cwd), _| *key_cwd != cwd);
}

/// Where a cloned project's checkout stands, while the clone tracker still knows it.
#[derive(Clone, Copy, PartialEq, Debug)]
pub(crate) enum Checkout {
    Pending,
    Cloned,
}

fn asset_error(tag: &str, resource: &Value) -> Reply {
    Reply::Failure(json!({"_tag": "Fail", "error": {"_tag": tag, "resource": resource}}))
}

/// `assets.createUrl` for a `project-favicon` resource.
pub(crate) async fn create_url(state: &AppState, params: &Value) -> Reply {
    let resource = &params["resource"];
    let cwd = resource["cwd"].as_str().unwrap_or_default();
    let (project_id, project_root, favicon_path) = {
        let store = state.store.read().await;
        let Some(project) = store.projects.values().find(|p| p.path == cwd) else {
            return asset_error("AssetWorkspaceContextNotFoundError", resource);
        };
        let favicon = store
            .project_rows
            .get(&project.id)
            .and_then(|row| row["faviconPath"].as_str())
            .filter(|p| !p.is_empty())
            .map(str::to_owned);
        (project.id.clone(), project.path.clone(), favicon)
    };
    // Clone projects precede their files; clients retry asset URLs after the clone.
    let checkout = state
        .project_clones
        .get(&project_id)
        .filter(|clone| clone["destinationPath"] == project_root.as_str())
        .map(|clone| {
            if clone["phase"] == "done" {
                Checkout::Cloned
            } else {
                Checkout::Pending
            }
        });
    issue(state, resource, cwd, favicon_path.as_deref(), checkout)
}

fn issue(
    state: &AppState,
    resource: &Value,
    cwd: &str,
    favicon_override: Option<&str>,
    checkout: Option<Checkout>,
) -> Reply {
    let Ok(root) = normalize_root(cwd) else {
        return asset_error("AssetWorkspaceRootNormalizationError", resource);
    };
    if checkout == Some(Checkout::Cloned) {
        invalidate(&root);
    }
    let favicon = match checkout {
        Some(Checkout::Pending) => None,
        _ => resolve(&root, favicon_override),
    };
    let external = favicon.as_ref().filter(|found| {
        favicon_override.is_some_and(|o| {
            Path::new(o).is_absolute() && normalized(found) == normalized(Path::new(o))
        })
    });
    let source = match (&favicon, external) {
        (_, Some(path)) => Some(path.display().to_string()),
        (Some(found), None) => found
            .strip_prefix(&root)
            .ok()
            .map(|r| r.display().to_string()),
        (None, None) => None,
    };
    if source.as_deref().is_some_and(|s| !is_favicon_path(s)) {
        return asset_error("AssetPreviewTypeValidationError", resource);
    }
    let canonical = source.as_deref().and_then(|s| {
        if external.is_some() {
            std::fs::canonicalize(s).ok().filter(|c| is_file(c))
        } else {
            let real_root = std::fs::canonicalize(&root).ok()?;
            let file = std::fs::canonicalize(root.join(s)).ok()?;
            (file.starts_with(&real_root) && is_file(&file)).then_some(file)
        }
    });
    if source.is_some() && canonical.is_none() {
        return asset_error("AssetProjectFaviconNotFoundError", resource);
    }
    let now = crate::platform::now_ms() as u64;
    let expires = (now / TOKEN_BUCKET_MS + 2) * TOKEN_BUCKET_MS;
    let claims = match (&canonical, external) {
        (Some(file), Some(_)) => json!({"version": 1, "kind": "project-favicon-external",
            "filePath": file.display().to_string(), "expiresAt": expires}),
        _ => {
            let Ok(real_root) = std::fs::canonicalize(&root) else {
                return asset_error("AssetWorkspaceResolutionError", resource);
            };
            json!({"version": 1, "kind": "project-favicon", "workspaceRoot": real_root.display().to_string(),
                "relativePath": source, "expiresAt": expires})
        }
    };
    let name = match (&source, &canonical) {
        (Some(source), Some(file)) => {
            let Ok(bytes) = std::fs::read(file) else {
                return asset_error("AssetProjectFaviconInspectionError", resource);
            };
            let revision = hex::encode(Sha256::digest(&bytes));
            let base = Path::new(source)
                .file_name()
                .map(|n| n.to_string_lossy().into_owned())
                .unwrap_or_default();
            format!("v{revision}-{base}")
        }
        _ => FALLBACK_MARKER.to_owned(),
    };
    crate::asset_grant::AssetGrant {
        claims,
        name,
        dimensions: None,
        source_path: source.map(PathBuf::from),
        revision: None,
    }
    .issue(resource, |payload| {
        crate::rpc::sign_asset_payload(state, payload)
    })
}

fn normalized(path: &Path) -> PathBuf {
    let mut out = PathBuf::new();
    for part in path.components() {
        match part {
            Component::ParentDir => {
                out.pop();
            }
            Component::CurDir => {}
            other => out.push(other),
        }
    }
    out
}

/// What a favicon claim serves: a file, or the PNG inside an `.icns` icon.
pub(crate) enum Served {
    File(PathBuf),
    Png(Vec<u8>),
}

/// `resolveAsset` for `project-favicon` and `project-favicon-external` claims.
pub(crate) fn served(claims: &Value) -> Option<Served> {
    let file = match claims["kind"].as_str()? {
        "project-favicon" => {
            let relative = claims["relativePath"].as_str()?;
            let root = std::fs::canonicalize(claims["workspaceRoot"].as_str()?).ok()?;
            let file = std::fs::canonicalize(within_root(&root, relative)?).ok()?;
            (file.starts_with(&root) && is_file(&file)).then_some(file)?
        }
        "project-favicon-external" => {
            let path = claims["filePath"].as_str()?;
            let file = std::fs::canonicalize(path).ok()?;
            (file == Path::new(path) && is_file(&file)).then_some(file)?
        }
        _ => return None,
    };
    if !file.display().to_string().to_lowercase().ends_with(".icns") {
        return Some(Served::File(file));
    }
    icns_png(&std::fs::read(&file).ok()?, ICNS_MIN_SIZE).map(Served::Png)
}

/// `extractIcnsPng`: the smallest embedded PNG at least `min_size` wide, else the widest.
fn icns_png(bytes: &[u8], min_size: u32) -> Option<Vec<u8>> {
    let u32_at = |at: usize| {
        bytes
            .get(at..at + 4)
            .map(|b| u32::from_be_bytes([b[0], b[1], b[2], b[3]]))
    };
    if bytes.len() < 8 || u32_at(0)? != u32::from_be_bytes(*b"icns") {
        return None;
    }
    let end = (u32_at(4)? as usize).min(bytes.len());
    let mut best: Option<(&[u8], u32)> = None;
    let mut offset = 8;
    while offset + 8 <= end {
        let length = u32_at(offset + 4)? as usize;
        if length < 8 || offset + length > end {
            break;
        }
        let entry = &bytes[offset + 8..offset + length];
        offset += length;
        let is_png = entry.len() >= 24 && entry.starts_with(&[0x89, b'P', b'N', b'G']);
        if !is_png {
            continue;
        }
        let width = u32::from_be_bytes([entry[16], entry[17], entry[18], entry[19]]);
        let better = match best {
            None => true,
            Some((_, w)) if w < min_size => width > w,
            Some((_, w)) => width >= min_size && width < w,
        };
        if better {
            best = Some((entry, width));
        }
    }
    best.map(|(png, _)| png.to_vec())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn icon_links_come_from_html_and_route_links() {
        assert_eq!(
            icon_href(r#"<link rel="icon" href="/icon.svg?v=2">"#).as_deref(),
            Some("/icon.svg")
        );
        assert_eq!(
            icon_href(r#"links: [{ rel: "icon", href: "/logo.png" }]"#).as_deref(),
            Some("/logo.png")
        );
        assert_eq!(icon_href("<link rel=stylesheet href=a.css>"), None);
    }

    #[test]
    fn workspace_candidates_stay_in_the_root() {
        let root = Path::new("/w/project");
        assert_eq!(
            within_root(root, "public/favicon.svg"),
            Some(PathBuf::from("/w/project/public/favicon.svg"))
        );
        assert_eq!(within_root(root, "../other/favicon.svg"), None);
        assert_eq!(within_root(root, "/etc/passwd"), None);
    }

    #[test]
    fn icns_picks_the_smallest_png_wide_enough() {
        let png = |width: u32| {
            let mut entry = vec![
                0x89, b'P', b'N', b'G', 0x0d, 0x0a, 0x1a, 0x0a, 0, 0, 0, 13, b'I', b'H', b'D', b'R',
            ];
            entry.extend_from_slice(&width.to_be_bytes());
            entry.extend_from_slice(&width.to_be_bytes());
            entry
        };
        let mut file = b"icns".to_vec();
        let entries = [(b"ic11", png(64)), (b"ic07", png(128)), (b"ic08", png(256))];
        let total: usize = 8 + entries.iter().map(|(_, e)| e.len() + 8).sum::<usize>();
        file.extend_from_slice(&(total as u32).to_be_bytes());
        for (kind, entry) in &entries {
            file.extend_from_slice(*kind);
            file.extend_from_slice(&((entry.len() + 8) as u32).to_be_bytes());
            file.extend_from_slice(entry);
        }
        assert_eq!(icns_png(&file, 96), Some(png(128)));
    }
}
