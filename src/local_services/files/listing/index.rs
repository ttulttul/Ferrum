use super::{
    git::{check_ignore, git, git_ignored},
    link_kind, outside_index, portable, Reply, Workspace,
};
use fff_search::{
    file_picker::{FilePicker, FilePickerOptions, FuzzySearchOptions},
    types::{MixedItemRef, PaginationArgs},
    MixedSearchConfig, QueryParser, SharedFilePicker,
};
use ignore::gitignore::{Gitignore, GitignoreBuilder};
use indexmap::IndexMap;
use serde_json::json;
use std::{
    collections::{HashMap, HashSet},
    fs,
    path::{Path, PathBuf},
    sync::{Arc, Mutex, OnceLock},
    time::{Duration, Instant},
};

const INDEX_LIMIT: usize = 25_000;
const INDEX_IDLE_TTL: Duration = Duration::from_secs(15 * 60);

#[derive(Default, Clone)]
pub struct ListingIndexes(Arc<Mutex<HashMap<PathBuf, CachedIndex>>>);

struct CachedIndex {
    index: Arc<WorkspaceIndex>,
    last_used: Instant,
}

#[derive(Default)]
struct WorkspaceIndex {
    picker: SharedFilePicker,
    initial_scan_complete: OnceLock<bool>,
}

impl Drop for WorkspaceIndex {
    fn drop(&mut self) {
        self.picker.shutdown_watches_and_wait();
        if let Ok(mut picker) = self.picker.write() {
            picker.take();
        }
    }
}

impl ListingIndexes {
    fn get(&self, workspace: &Workspace) -> Result<Arc<WorkspaceIndex>, fff_search::Error> {
        let root = &workspace.root;
        let mut indexes = self.0.lock().expect("listing indexes");
        if let Some(cached) = indexes.get_mut(root) {
            cached.last_used = Instant::now();
            return Ok(cached.index.clone());
        }
        let index = Arc::new(WorkspaceIndex::default());
        FilePicker::new_with_shared_state(
            index.picker.clone(),
            Default::default(),
            FilePickerOptions {
                base_path: root.to_string_lossy().into_owned(),
                ..Default::default()
            },
        )?;
        indexes.insert(
            root.clone(),
            CachedIndex {
                index: index.clone(),
                last_used: Instant::now(),
            },
        );
        let cache = Arc::downgrade(&self.0);
        let root = root.clone();
        tokio::spawn(async move {
            let mut remaining = INDEX_IDLE_TTL;
            loop {
                tokio::time::sleep(remaining).await;
                let Some(cache) = cache.upgrade() else {
                    return;
                };
                let mut indexes = cache.lock().expect("listing indexes");
                let Some(entry) = indexes.get(&root) else {
                    return;
                };
                remaining = INDEX_IDLE_TTL.saturating_sub(entry.last_used.elapsed());
                if remaining.is_zero() {
                    let expired = indexes.remove(&root);
                    drop(indexes);
                    tokio::task::spawn_blocking(move || drop(expired));
                    return;
                }
            }
        });
        Ok(index)
    }
}

/// Nearest matching .ignore wins, including explicit re-inclusions of Git exclusions.
#[derive(Default)]
struct IgnoreFiles(HashMap<PathBuf, Option<Gitignore>>);

impl IgnoreFiles {
    fn included(&mut self, path: &Path, directory: bool, root: &Path) -> Option<bool> {
        for ancestor in path
            .parent()?
            .ancestors()
            .take_while(|p| p.starts_with(root))
        {
            let file = self.0.entry(ancestor.to_owned()).or_insert_with(|| {
                let mut builder = GitignoreBuilder::new(ancestor);
                if !ancestor.join(".ignore").is_file() {
                    return None;
                }
                builder.add(ancestor.join(".ignore"));
                builder.build().ok()
            });
            if let Some(file) = file {
                let relative = path.strip_prefix(ancestor).unwrap_or(path);
                let decision = file.matched_path_or_any_parents(relative, directory);
                if !decision.is_none() {
                    return Some(decision.is_whitelist());
                }
            }
        }
        None
    }
}

fn ignored_by_git(path: &str, ignored: &HashSet<String>) -> bool {
    Path::new(path)
        .ancestors()
        .any(|p| ignored.contains(p.to_str().unwrap_or_default()))
}

/// Collect links without following them. Each nested repository classifies its own links.
fn add_links(
    entries: &mut IndexMap<String, &'static str>,
    mut directories: Vec<PathBuf>,
    root: &Path,
    ignored: &HashSet<String>,
    rules: &mut IgnoreFiles,
) {
    let Ok(real_root) = fs::canonicalize(root) else {
        return;
    };
    directories.push(PathBuf::new());
    directories.sort();
    directories.dedup();
    let mut repos = HashSet::new();
    let mut by_repo: HashMap<PathBuf, Vec<(String, &'static str)>> = HashMap::new();
    for directory in directories {
        let dir = root.join(&directory);
        if outside_index(root, &dir) {
            continue;
        }
        if dir.join(".git").symlink_metadata().is_ok() {
            repos.insert(directory.clone());
        }
        let Ok(items) = fs::read_dir(&dir) else {
            continue;
        };
        for item in items.flatten() {
            if !item.file_type().is_ok_and(|ft| ft.is_symlink()) {
                continue;
            }
            let path = directory.join(item.file_name());
            let key = portable(&path);
            entries.shift_remove(&key);
            if outside_index(root, &item.path()) || ignored_by_git(&key, ignored) {
                continue;
            }
            let Some(kind) = link_kind(&real_root, &item.path(), &[]) else {
                continue;
            };
            match rules.included(&item.path(), false, root) {
                Some(false) => continue,
                Some(true) => {
                    entries.insert(key, kind);
                }
                None => {
                    let repo = directory
                        .ancestors()
                        .find(|p| repos.contains(*p))
                        .unwrap_or(Path::new(""));
                    by_repo
                        .entry(repo.to_owned())
                        .or_default()
                        .push((key, kind));
                }
            }
        }
    }
    for (repo, links) in by_repo {
        let cwd = root.join(&repo);
        let prefix = portable(&repo);
        let offset = if prefix.is_empty() {
            0
        } else {
            prefix.len() + 1
        };
        let hidden: HashSet<_> =
            match check_ignore(&cwd, links.iter().map(|(p, _)| &p[offset..]), true) {
                Some(result) if matches!(result.code, 0 | 1) && !result.truncated => {
                    result.paths.into_iter().collect()
                }
                _ if git(&cwd, &["rev-parse", "--git-dir"], None, 4096)
                    .is_some_and(|r| r.code == 0) =>
                {
                    continue
                }
                _ => HashSet::new(),
            };
        entries.extend(
            links
                .into_iter()
                .filter(|(p, _)| !hidden.contains(&p[offset..])),
        );
    }
}

pub(super) fn git_tracked(root: &Path) -> Vec<String> {
    git(root, &["ls-files", "-z"], None, 16 * 1024 * 1024)
        .filter(|result| result.code == 0 && !result.truncated)
        .map(|result| result.paths)
        .unwrap_or_default()
}

fn gitignore_ignored(root: &Path, path: &str) -> bool {
    let mut current = root.to_path_buf();
    let mut ancestors = vec![root.to_owned()];
    for component in Path::new(path).components() {
        current = current.join(component);
        ancestors.push(current.clone());
    }
    for base in ancestors.into_iter().rev() {
        let file = base.join(".gitignore");
        if !file.is_file() {
            continue;
        }
        let mut builder = GitignoreBuilder::new(&base);
        let _ = builder.add(&file);
        if let Ok(matcher) = builder.build() {
            if !matcher
                .matched_path_or_any_parents(
                    Path::new(path)
                        .strip_prefix(&base)
                        .unwrap_or(Path::new(path)),
                    false,
                )
                .is_none()
            {
                return true;
            }
        }
    }
    false
}

fn add_tracked(root: &Path, entries: &mut IndexMap<String, &'static str>) {
    for path in git_tracked(root) {
        if path.is_empty()
            || outside_index(root, &root.join(&path))
            || gitignore_ignored(root, &path)
        {
            continue;
        }
        if fs::metadata(root.join(&path)).is_ok() {
            entries.entry(path).or_insert("file");
        }
    }
}

pub(super) fn recursive(workspace: &Workspace, indexes: &ListingIndexes) -> Reply {
    let root = &workspace.root;
    let index_error = |e: fff_search::Error| {
        workspace.error(
            "search_index_create_failed",
            "WorkspaceSearchIndexCreateFailed",
            format!(
                "Failed to create the workspace search index for '{}'.",
                root.display()
            ),
            Some(&format!("Failed to init file picker: {e}")),
        )
    };
    let index = indexes.get(workspace).map_err(index_error)?;
    let mut rules = IgnoreFiles::default();
    let ignored = git_ignored(root, false)
        .into_iter()
        .filter_map(|p| {
            let path = p.trim_end_matches('/');
            (rules.included(&root.join(path), p.ends_with('/'), root) != Some(true))
                .then(|| path.to_owned())
        })
        .collect();
    let initial_scan_complete = index.initial_scan_complete.get_or_init(|| {
        index
            .picker
            .wait_for_indexing_complete(Duration::from_secs(15))
    });
    let guard = index.picker.read().map_err(index_error)?;
    let picker = guard.as_ref().expect("active listing index");
    let incomplete = !*initial_scan_complete && picker.is_scan_active();
    let parser = QueryParser::new(MixedSearchConfig);
    let result = picker.fuzzy_search_mixed(
        &parser.parse(""),
        None,
        FuzzySearchOptions {
            project_path: Some(picker.base_path()),
            pagination: PaginationArgs {
                offset: 0,
                limit: INDEX_LIMIT + 2,
            },
            ..Default::default()
        },
    );
    let mut visible = result.total_matched;
    let mut entries = IndexMap::new();
    for item in result.items {
        let (path, kind) = match item {
            MixedItemRef::Dir(d) => (d.relative_path(picker), "directory"),
            MixedItemRef::File(f) => (f.relative_path(picker), "file"),
        };
        let path = path.trim_end_matches('/');
        if path.is_empty()
            || outside_index(root, &root.join(path))
            || ignored_by_git(path, &ignored)
        {
            visible -= 1;
        } else if entries.len() < INDEX_LIMIT {
            entries.insert(path.to_owned(), kind);
        }
    }
    let directories = picker
        .get_dirs()
        .iter()
        .filter(|d| !d.is_deleted())
        .map(|d| PathBuf::from(d.relative_path(picker).trim_end_matches('/')))
        .collect();
    drop(guard);
    add_tracked(root, &mut entries);
    add_links(&mut entries, directories, root, &ignored, &mut rules);
    let mut ancestors = Vec::new();
    for path in entries.keys() {
        let mut parent = path.as_str();
        while let Some((dir, _)) = parent.rsplit_once('/') {
            if entries.contains_key(dir) {
                break;
            }
            ancestors.push(dir.to_owned());
            parent = dir;
        }
    }
    entries.extend(ancestors.into_iter().map(|p| (p, "directory")));
    let collator = icu_collator::Collator::try_new(Default::default(), Default::default())
        .expect("compiled default collation data");
    entries.sort_by(|a, _, b, _| collator.compare(a, b));
    let truncated = incomplete || visible > INDEX_LIMIT || entries.len() > INDEX_LIMIT;
    let entries: Vec<_> = entries
        .into_iter()
        .take(INDEX_LIMIT)
        .map(|(path, kind)| json!({"path":path,"kind":kind}))
        .collect();
    Ok(json!({"entries":entries, "truncated":truncated}))
}
