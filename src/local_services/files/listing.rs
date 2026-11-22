mod git;
mod index;

use super::Reply;
use crate::local_services::vcs::lexical_path;
use git::{check_ignore, git_ignored};
pub use index::ListingIndexes;
use serde_json::{json, Value};
use std::{
    collections::HashSet,
    fs, io,
    path::{Path, PathBuf},
};

const IGNORED_FILES_LIMIT: usize = 200;

fn portable(path: &Path) -> String {
    let path = path.to_string_lossy();
    if cfg!(windows) {
        path.replace('\\', "/")
    } else {
        path.into_owned()
    }
}

struct Workspace {
    cwd: String,
    root: PathBuf,
}

impl Workspace {
    fn error(&self, failure: &str, cause: &str, message: String, detail: Option<&str>) -> Value {
        let mut value = json!({"_tag": "ProjectListEntriesError", "cwd": self.cwd,
            "normalizedCwd": self.root, "failure": failure,
            "message": format!("Failed to list workspace entries in '{}'.", self.cwd),
            "cause": {"name": cause, "message": message}});
        if let Some(detail) = detail {
            value["detail"] = json!(detail);
        }
        value
    }

    fn check(&self) -> Result<(), Value> {
        let root = self.root.display();
        match fs::metadata(&self.root) {
            Ok(meta) if meta.is_dir() => Ok(()),
            Ok(_) => Err(self.error(
                "workspace_root_not_directory",
                "WorkspaceRootNotDirectoryError",
                format!("Workspace root is not a directory: {root}"),
                None,
            )),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Err(self.error(
                "workspace_root_not_found",
                "WorkspaceRootNotExistsError",
                format!("Workspace root does not exist: {root}"),
                None,
            )),
            Err(_) => Err(self.error(
                "workspace_root_stat_failed",
                "WorkspaceRootStatFailedError",
                format!("Failed to stat workspace root '{root}' during 'validate-existing'."),
                Some("validate-existing"),
            )),
        }
    }
}

async fn list(
    input: &Value,
    operation: impl FnOnce(&Workspace, &Value) -> Reply + Send + 'static,
) -> Option<Reply> {
    let cwd = input["cwd"].as_str()?.trim().to_owned();
    let path = crate::rpc::expand_user_path(&cwd);
    let workspace = Workspace {
        root: lexical_path(&std::env::current_dir().unwrap_or_default().join(path)),
        cwd,
    };
    let input = input.clone();
    Some(
        tokio::task::spawn_blocking(move || {
            workspace.check()?;
            operation(&workspace, &input)
        })
        .await
        .unwrap_or_else(|e| Err(json!({"_tag":"Die", "defect":e.to_string()}))),
    )
}

impl ListingIndexes {
    pub async fn list_entries(&self, input: &Value) -> Option<Reply> {
        let indexes = self.clone();
        list(input, move |workspace, input| {
            match input["directoryPath"].as_str() {
                None => index::recursive(workspace, &indexes),
                Some(directory) => children(workspace, directory.trim()),
            }
        })
        .await
    }

    pub async fn search_entries(&self, input: &Value) -> Option<Reply> {
        let indexes = self.clone();
        list(input, move |workspace, input| {
            index::search_entries(workspace, &indexes, input)
        })
        .await
    }

    pub async fn search_contents(&self, input: &Value) -> Option<Reply> {
        let indexes = self.clone();
        list(input, move |workspace, input| {
            index::search_contents(workspace, &indexes, input)
        })
        .await
    }
}

pub async fn list_ignored_files(input: &Value) -> Option<Reply> {
    list(input, |workspace, input| {
        let mut paths = git_ignored(&workspace.root, true);
        let query = input["query"].as_str().unwrap_or("").to_lowercase();
        paths.retain(|p| !p.ends_with('/') && p.to_lowercase().contains(&query));
        paths.sort_by(|a, b| a.encode_utf16().cmp(b.encode_utf16()));
        let truncated = paths.len() > IGNORED_FILES_LIMIT;
        let files: Vec<_> = paths
            .into_iter()
            .take(IGNORED_FILES_LIMIT)
            .filter_map(|path| {
                let stat = fs::symlink_metadata(workspace.root.join(&path)).ok()?;
                stat.is_file()
                    .then(|| json!({"path":path, "byteLength":stat.len()}))
            })
            .collect();
        Ok(json!({"files":files, "truncated":truncated}))
    })
    .await
}

fn outside_index(root: &Path, path: &Path) -> bool {
    path.strip_prefix(root).map_or(true, |rel| {
        rel.components().any(|c| c.as_os_str() == ".git")
    })
}

fn link_kind(root: &Path, link: &Path, visited: &[PathBuf]) -> Option<&'static str> {
    let target = fs::canonicalize(link).ok()?;
    if outside_index(root, &target) {
        return None;
    }
    let stat = fs::metadata(&target).ok()?;
    if stat.is_file() {
        return Some("file");
    }
    (stat.is_dir() && !visited.iter().any(|folder| folder.starts_with(&target)))
        .then_some("directory")
}

fn children(workspace: &Workspace, directory: &str) -> Reply {
    let root = &workspace.root;
    let target = lexical_path(&root.join(directory));
    let failed = || {
        let message = format!(
            "Failed to read workspace directory '{}' while browsing '{}' from '{}'.",
            target.display(),
            directory,
            root.display()
        );
        workspace.error(
            "directory_list_failed",
            "WorkspaceEntriesReadDirectoryError",
            message.clone(),
            Some(&message),
        )
    };
    let rel = target.strip_prefix(root).map_err(|_| failed())?;
    if Path::new(directory).is_absolute()
        || (!directory.is_empty() && rel.as_os_str().is_empty())
        || outside_index(root, &target)
    {
        return Err(failed());
    }
    let real_root = fs::canonicalize(root).map_err(|_| failed())?;
    let real_target = fs::canonicalize(&target).map_err(|_| failed())?;
    if outside_index(&real_root, &real_target) {
        return Err(failed());
    }
    let mut visited = vec![real_root.clone()];
    let mut ancestor = root.clone();
    let mut git_root = root.clone();
    for segment in rel.components() {
        ancestor.push(segment);
        visited.push(fs::canonicalize(&ancestor).map_err(|_| failed())?);
        if fs::symlink_metadata(ancestor.join(".git")).is_ok() {
            let marker = portable(&ancestor.strip_prefix(&git_root).unwrap().join(".git"));
            if check_ignore(&git_root, std::iter::once(marker.as_str()), false)
                .is_some_and(|o| o.code == 128)
            {
                git_root = ancestor.clone();
            }
        }
    }
    let mut entries = Vec::new();
    for item in fs::read_dir(&real_target).map_err(|_| failed())? {
        let item = item.map_err(|_| failed())?;
        let name = item.file_name();
        if name == ".git" {
            continue;
        }
        let ft = item.file_type().map_err(|_| failed())?;
        let kind = if ft.is_dir() {
            "directory"
        } else if ft.is_file() {
            "file"
        } else if ft.is_symlink() {
            let Some(kind) = link_kind(&real_root, &item.path(), &visited) else {
                continue;
            };
            kind
        } else {
            continue;
        };
        entries.push((portable(&rel.join(name)), kind));
    }
    entries.sort_by(|a, b| a.0.cmp(&b.0));
    let prefix = portable(git_root.strip_prefix(root).unwrap());
    let prefix_len = if prefix.is_empty() {
        0
    } else {
        prefix.len() + 1
    };
    let mut ignored = HashSet::new();
    for chunk in entries.chunks(1000) {
        match check_ignore(
            &git_root,
            chunk.iter().map(|(path, _)| &path[prefix_len..]),
            false,
        ) {
            Some(result) if matches!(result.code, 0 | 1) => ignored.extend(result.paths),
            _ => break,
        }
    }
    let entries: Vec<_> = entries
        .into_iter()
        .map(|(path, kind)| {
            let mut entry = json!({"path":path,"kind":kind});
            if ignored.contains(&path[prefix_len..]) {
                entry["ignored"] = json!(true);
            }
            entry
        })
        .collect();
    Ok(json!({"entries": entries, "truncated": false}))
}
