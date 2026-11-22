use super::{
    git::{check_ignore, git, git_ignored},
    link_kind, outside_index, portable, Reply, Workspace,
};
use fff_search::{
    file_picker::{FilePicker, FilePickerOptions, FuzzySearchOptions},
    types::{MixedItemRef, PaginationArgs},
    Casing, GrepMode, GrepSearchOptions, MixedSearchConfig, QueryParser, SharedFilePicker,
};
use ignore::gitignore::{Gitignore, GitignoreBuilder};
use indexmap::IndexMap;
use serde_json::{json, Value};
use std::{
    collections::{HashMap, HashSet},
    fs,
    path::{Path, PathBuf},
    sync::{Arc, Mutex, OnceLock},
    time::{Duration, Instant},
};

const INDEX_LIMIT: usize = 25_000;
const INDEX_IDLE_TTL: Duration = Duration::from_secs(15 * 60);
const SEARCH_PAGE_SIZE: usize = 25_002;
const CONTENT_LINE_MAX_LENGTH: usize = 512;
const CONTENT_MAX_MATCHES_PER_FILE: usize = 100;

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

fn normalized_search_query(input: &str) -> String {
    input
        .trim()
        .trim_start_matches(['@', '.', '/'])
        .to_lowercase()
}

fn ignored_paths(root: &Path) -> HashSet<String> {
    git_ignored(root, false)
        .into_iter()
        .map(|path| path.trim_end_matches('/').to_owned())
        .collect()
}

fn is_ignored_path(ignored: &HashSet<String>, path: &str) -> bool {
    let mut current = Some(path);
    while let Some(candidate) = current {
        if ignored.contains(candidate) {
            return true;
        }
        current = candidate.rsplit_once('/').map(|(parent, _)| parent);
    }
    false
}

fn entry_path(path: &str) -> String {
    path.trim_end_matches('/').replace('\\', "/")
}

fn is_image_path(path: &str) -> bool {
    const EXTENSIONS: &[&str] = &[
        ".avif", ".bmp", ".gif", ".ico", ".heic", ".heif", ".jpeg", ".jpg", ".png", ".svg", ".tif",
        ".tiff", ".webp", ".icns",
    ];
    let lower = path.to_lowercase();
    EXTENSIONS
        .iter()
        .any(|extension| lower.ends_with(extension))
}

fn search_picker(
    indexes: &ListingIndexes,
    workspace: &Workspace,
) -> Result<Arc<WorkspaceIndex>, Value> {
    indexes.get(workspace).map_err(|cause| {
        workspace.error(
            "search_index_create_failed",
            "WorkspaceSearchIndexCreateFailed",
            format!(
                "Failed to create the workspace search index for '{}'.",
                workspace.root.display()
            ),
            Some(&cause.to_string()),
        )
    })
}

pub(super) fn search_entries(
    workspace: &Workspace,
    indexes: &ListingIndexes,
    input: &Value,
) -> Reply {
    let index = search_picker(indexes, workspace)?;
    let complete = index.initial_scan_complete.get_or_init(|| {
        index
            .picker
            .wait_for_indexing_complete(Duration::from_secs(15))
    });
    let guard = index.picker.read().map_err(|cause| {
        workspace.error(
            "search_index_search_failed",
            "WorkspaceSearchIndexSearchFailed",
            cause.to_string(),
            None,
        )
    })?;
    let picker = guard.as_ref().expect("active listing index");
    let mut query = normalized_search_query(input["query"].as_str().unwrap_or_default());
    let limit = input["limit"].as_u64().unwrap_or(100).clamp(1, 10_000) as usize;
    let kind = input["kind"].as_str();
    let image_only = input["imageOnly"].as_bool().unwrap_or(false);
    let ignored = ignored_paths(&workspace.root);
    let parser = QueryParser::new(MixedSearchConfig);
    let mut result = run_name_search(picker, &parser, &query, kind, image_only);
    if result.0.is_empty() && query.contains('{') {
        query = query.replace('{', "");
        result = run_name_search(picker, &parser, &query, kind, image_only);
    }

    let mut indexed_entries = Vec::new();
    for item in result.0 {
        let (path, entry_kind) = match item {
            MixedItemRef::File(file) => (file.relative_path(picker), "file"),
            MixedItemRef::Dir(directory) => (directory.relative_path(picker), "directory"),
        };
        let path = entry_path(&path);
        if path.is_empty() || is_ignored_path(&ignored, &path) {
            continue;
        }
        indexed_entries.push((path, entry_kind));
    }
    let links = search_symlinks(picker, &workspace.root, &ignored);
    let mut ranked_links = links
        .into_iter()
        .filter(|(path, entry_kind)| {
            (kind.is_none() || kind == Some(*entry_kind))
                && (!image_only || (*entry_kind == "file" && is_image_path(path)))
        })
        .filter_map(|(path, entry_kind)| {
            subsequence_score(&path, &query).map(|score| (score, path, entry_kind))
        })
        .collect::<Vec<_>>();
    ranked_links.sort_by_key(|(score, _, _)| *score);
    let mut ordered = Vec::new();
    let mut next_link = 0;
    for (path, entry_kind) in indexed_entries {
        let indexed_score = subsequence_score(&path, &query).unwrap_or(usize::MAX);
        while next_link < ranked_links.len() && ranked_links[next_link].0 < indexed_score {
            let (_, path, entry_kind) = &ranked_links[next_link];
            ordered.push(json!({"path": path, "kind": entry_kind}));
            next_link += 1;
        }
        ordered.push(json!({"path": path, "kind": entry_kind}));
    }
    while next_link < ranked_links.len() {
        let (_, path, entry_kind) = &ranked_links[next_link];
        ordered.push(json!({"path": path, "kind": entry_kind}));
        next_link += 1;
    }
    let truncated_by_limit = ordered.len() > limit;
    let entries = ordered.into_iter().take(limit).collect::<Vec<_>>();
    let matched_count = if image_only {
        entries.len()
    } else if kind == Some("directory") {
        result.1.saturating_sub(1)
    } else {
        result.1
    };
    let truncated = !*complete
        || matched_count > entries.len()
        || truncated_by_limit
        || (image_only && entries.len() >= limit);
    Ok(json!({"entries": entries, "truncated": truncated}))
}

fn run_name_search<'a>(
    picker: &'a fff_search::FilePicker,
    parser: &QueryParser<MixedSearchConfig>,
    query: &'a str,
    kind: Option<&str>,
    image_only: bool,
) -> (Vec<MixedItemRef<'a>>, usize) {
    let parsed = parser.parse(query);
    let options = FuzzySearchOptions {
        project_path: Some(picker.base_path()),
        pagination: PaginationArgs {
            offset: 0,
            limit: SEARCH_PAGE_SIZE,
        },
        ..Default::default()
    };
    if kind == Some("file") || image_only {
        let result = picker.fuzzy_search(&parsed, None, options);
        let items = result
            .items
            .into_iter()
            .filter(|file| !image_only || is_image_path(&file.relative_path(picker)))
            .map(MixedItemRef::File)
            .collect();
        return (items, result.total_matched);
    }
    if kind == Some("directory") {
        let result = picker.fuzzy_search_directories(&parsed, options);
        let items = result.items.into_iter().map(MixedItemRef::Dir).collect();
        return (items, result.total_matched);
    }
    let result = picker.fuzzy_search_mixed(&parsed, None, options);
    (result.items, result.total_matched)
}

fn subsequence_score(value: &str, query: &str) -> Option<usize> {
    if query.is_empty() {
        return Some(0);
    }
    let value = value.to_lowercase();
    let query = query.to_lowercase();
    let query_chars = query.chars().collect::<Vec<_>>();
    let mut query_index = 0;
    let mut first = None;
    let mut previous = None;
    let mut gap_penalty = 0;
    for (value_index, character) in value.chars().enumerate() {
        if query_chars.get(query_index) != Some(&character) {
            continue;
        }
        first.get_or_insert(value_index);
        if let Some(previous) = previous {
            gap_penalty += value_index - previous - 1;
        }
        previous = Some(value_index);
        query_index += 1;
        if query_index == query_chars.len() {
            let first = first.expect("matched character");
            let span_penalty = value_index - first + 1 - query_index;
            let length_penalty = value.chars().count().saturating_sub(query_index).min(64);
            return Some(first * 2 + gap_penalty * 3 + span_penalty + length_penalty);
        }
    }
    None
}

fn search_symlinks(
    picker: &fff_search::FilePicker,
    root: &Path,
    ignored: &HashSet<String>,
) -> IndexMap<String, &'static str> {
    let directories = picker
        .get_dirs()
        .iter()
        .filter(|directory| !directory.is_deleted())
        .map(|directory| PathBuf::from(directory.relative_path(picker).trim_end_matches('/')))
        .collect();
    let mut links = IndexMap::new();
    let mut rules = IgnoreFiles::default();
    add_links(&mut links, directories, root, ignored, &mut rules);
    links
}

fn byte_to_utf16(line: &str, byte_offset: usize) -> usize {
    line.char_indices()
        .take_while(|(index, _)| *index < byte_offset)
        .map(|(_, character)| character.len_utf16())
        .sum()
}

fn unicode_word(character: Option<char>) -> bool {
    character.is_some_and(|character| character.is_alphanumeric() || character == '_')
}

fn whole_word_range(line: &str, start: usize, end: usize) -> bool {
    let before = line[..start.min(line.len())].chars().next_back();
    let after = line
        .get(end.min(line.len())..)
        .and_then(|tail| tail.chars().next());
    !unicode_word(before) && !unicode_word(after)
}

pub(super) fn search_contents(
    workspace: &Workspace,
    indexes: &ListingIndexes,
    input: &Value,
) -> Reply {
    let index = search_picker(indexes, workspace)?;
    let complete = index.initial_scan_complete.get_or_init(|| {
        index
            .picker
            .wait_for_indexing_complete(Duration::from_secs(15))
    });
    let guard = index.picker.read().map_err(|cause| {
        workspace.error(
            "search_index_search_failed",
            "WorkspaceSearchIndexSearchFailed",
            cause.to_string(),
            None,
        )
    })?;
    let picker = guard.as_ref().expect("active listing index");
    let raw_query = input["query"].as_str().unwrap_or_default();
    let case_sensitive = input["caseSensitive"].as_bool().unwrap_or(false);
    let whole_word = input["wholeWord"].as_bool().unwrap_or(false);
    let use_regex = input["useRegex"].as_bool().unwrap_or(false);
    let limit = input["limit"].as_u64().unwrap_or(100).clamp(1, 10_000) as usize;
    let search_query = if use_regex && !case_sensitive {
        format!("(?i){raw_query}")
    } else if case_sensitive {
        raw_query.to_owned()
    } else {
        raw_query.to_lowercase()
    };
    let parser = QueryParser::new(fff_search::GrepConfig);
    let parsed = parser.parse(&search_query);
    let options = GrepSearchOptions {
        max_matches_per_file: CONTENT_MAX_MATCHES_PER_FILE,
        page_limit: limit + 1,
        mode: if use_regex {
            GrepMode::Regex
        } else {
            GrepMode::PlainText
        },
        casing: Some(if use_regex || case_sensitive {
            Casing::Sensitive
        } else {
            Casing::Insensitive
        }),
        smart_case: false,
        time_budget_ms: 250,
        ..Default::default()
    };
    let result = picker.grep(&parsed, &options);
    let ignored = ignored_paths(&workspace.root);
    let mut matches = Vec::new();
    for item in result.matches {
        let Some(file) = result.files.get(item.file_index) else {
            continue;
        };
        let path = entry_path(&file.relative_path(picker));
        if is_ignored_path(&ignored, &path) {
            continue;
        }
        let line_content = item
            .line_content
            .chars()
            .take(CONTENT_LINE_MAX_LENGTH)
            .collect::<String>();
        let ranges: Vec<Value> = item
            .match_byte_offsets
            .iter()
            .filter_map(|(start, end)| {
                let start = *start as usize;
                let end = *end as usize;
                if end > line_content.len() || (whole_word && !whole_word_range(&line_content, start, end)) {
                    return None;
                }
                Some(json!({"start": byte_to_utf16(&line_content, start), "end": byte_to_utf16(&line_content, end)}))
            })
            .collect();
        if whole_word && ranges.is_empty() {
            continue;
        }
        matches.push(json!({
            "path": path,
            "lineNumber": item.line_number,
            "lineContent": line_content,
            "matchRanges": ranges,
        }));
        if matches.len() > limit {
            break;
        }
    }
    let mut response = json!({
        "matches": matches.into_iter().take(limit).collect::<Vec<_>>(),
        "truncated": result.next_file_offset != 0 || !*complete,
    });
    if let Some(error) = result.regex_fallback_error {
        response["regexFallbackError"] = json!(error);
    }
    Ok(response)
}
