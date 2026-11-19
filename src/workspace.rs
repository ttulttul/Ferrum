use serde_json::{json, Value};
use std::path::{Path, PathBuf};

type Reply = Result<Value, Value>;

fn error(kind: &str, message: String) -> Value {
    json!({"_tag": kind, "message": message})
}

fn root(input: &Value) -> Result<PathBuf, Value> {
    let cwd = input["cwd"].as_str().ok_or_else(|| {
        error(
            "ProjectEntriesError",
            "A workspace root is required.".into(),
        )
    })?;
    let path = PathBuf::from(cwd);
    if !path.is_dir() {
        return Err(error(
            "ProjectEntriesError",
            format!("Workspace root is not a directory: {cwd}"),
        ));
    }
    Ok(path)
}

fn relative(root: &Path, path: &Path) -> String {
    path.strip_prefix(root)
        .unwrap_or(path)
        .to_string_lossy()
        .replace('\\', "/")
}

fn entry(root: &Path, path: &Path, ignored: bool) -> Option<Value> {
    let metadata = std::fs::symlink_metadata(path).ok()?;
    let kind = if metadata.is_dir() {
        "directory"
    } else if metadata.is_file() {
        "file"
    } else {
        return None;
    };
    let mut value = json!({"path": relative(root, path), "kind": kind});
    if ignored {
        value["ignored"] = json!(true);
    }
    Some(value)
}

fn sorted_entries(root: &Path, directory: &Path) -> std::io::Result<Vec<Value>> {
    let mut values = std::fs::read_dir(directory)?
        .filter_map(Result::ok)
        .filter_map(|item| entry(root, &item.path(), false))
        .collect::<Vec<_>>();
    values.sort_by(|a, b| a["path"].as_str().cmp(&b["path"].as_str()));
    Ok(values)
}

pub async fn list_entries(input: &Value) -> Option<Reply> {
    if !input.get("cwd").is_some() {
        return None;
    }
    let result = (|| {
        let root = root(input)?;
        let directory = input["directoryPath"].as_str().unwrap_or_default();
        let directory = root.join(directory);
        if !directory.is_dir() {
            return Err(error(
                "ProjectEntriesError",
                "Directory does not exist.".into(),
            ));
        }
        Ok(
            json!({"entries": sorted_entries(&root, &directory).map_err(|e| error("ProjectEntriesError", e.to_string()))?, "truncated": false}),
        )
    })();
    Some(result)
}

fn walk(root: &Path, directory: &Path, output: &mut Vec<Value>, limit: usize) {
    if output.len() >= limit {
        return;
    }
    let Ok(items) = std::fs::read_dir(directory) else {
        return;
    };
    let mut paths = items
        .filter_map(Result::ok)
        .map(|e| e.path())
        .collect::<Vec<_>>();
    paths.sort();
    for path in paths {
        if output.len() >= limit {
            break;
        }
        let Some(value) = entry(root, &path, false) else {
            continue;
        };
        let is_dir = value["kind"] == "directory";
        output.push(value);
        if is_dir {
            walk(root, &path, output, limit);
        }
    }
}

pub async fn search_entries(input: &Value) -> Option<Reply> {
    if !input.get("cwd").is_some() {
        return None;
    }
    let result = (|| {
        let root = root(input)?;
        let query = input["query"].as_str().unwrap_or_default().to_lowercase();
        let limit = input["limit"].as_u64().unwrap_or(100).clamp(1, 10_000) as usize;
        let kind_filter = input["kind"].as_str();
        let mut all = Vec::new();
        walk(&root, &root, &mut all, limit.saturating_mul(20));
        let entries = all
            .into_iter()
            .filter(|v| kind_filter.is_none_or(|kind| v["kind"] == kind))
            .filter(|v| {
                v["path"]
                    .as_str()
                    .unwrap_or_default()
                    .to_lowercase()
                    .contains(&query)
            })
            .take(limit)
            .collect::<Vec<_>>();
        Ok(json!({"entries": entries, "truncated": entries.len() == limit}))
    })();
    Some(result)
}

pub async fn search_contents(input: &Value) -> Option<Reply> {
    if !input.get("cwd").is_some() {
        return None;
    }
    let result = (|| {
        let root = root(input)?;
        let query = input["query"].as_str().unwrap_or_default();
        let limit = input["limit"].as_u64().unwrap_or(100).clamp(1, 10_000) as usize;
        let case_sensitive = input["caseSensitive"].as_bool().unwrap_or(false);
        let needle = if case_sensitive {
            query.to_owned()
        } else {
            query.to_lowercase()
        };
        let mut files = Vec::new();
        walk(&root, &root, &mut files, 10_000);
        let mut matches = Vec::new();
        for file in files.into_iter().filter(|v| v["kind"] == "file") {
            if matches.len() >= limit {
                break;
            }
            let path = root.join(file["path"].as_str().unwrap_or_default());
            let Ok(contents) = std::fs::read_to_string(&path) else {
                continue;
            };
            for (index, line) in contents.lines().enumerate() {
                let haystack = if case_sensitive {
                    line.to_owned()
                } else {
                    line.to_lowercase()
                };
                if let Some(start) = haystack.find(&needle) {
                    matches.push(json!({"path":file["path"],"lineNumber":index + 1,"lineContent":line,"matchRanges":[{"start":start,"end":start + needle.len()}]}));
                    if matches.len() >= limit {
                        break;
                    }
                }
            }
        }
        Ok(json!({"matches": matches, "truncated": matches.len() == limit}))
    })();
    Some(result)
}

pub async fn read_file(input: &Value) -> Option<Reply> {
    if !input.get("cwd").is_some() {
        return None;
    }
    let result = (|| {
        let root = root(input)?;
        let relative_path = input["relativePath"].as_str().unwrap_or_default();
        let path = if Path::new(relative_path).is_absolute() {
            PathBuf::from(relative_path)
        } else {
            root.join(relative_path)
        };
        let bytes = match std::fs::read(&path) {
            Ok(bytes) => bytes,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound && input["allowMissing"] == true => {
                return Ok(
                    json!({"relativePath": relative_path, "contents":"", "byteLength":0, "truncated":false, "exists":false}),
                );
            }
            Err(e) => return Err(error("ProjectReadFileError", e.to_string())),
        };
        let text = String::from_utf8(bytes.clone()).map_err(|_| {
            error(
                "ProjectReadFileError",
                "Binary files cannot be read.".into(),
            )
        })?;
        Ok(
            json!({"relativePath": relative_path, "contents":text, "byteLength":bytes.len(), "truncated":false, "exists":true}),
        )
    })();
    Some(result)
}

pub async fn write_file(input: &Value) -> Option<Reply> {
    if !input.get("cwd").is_some() {
        return None;
    }
    let result = (|| {
        let root = root(input)?;
        let relative_path = input["relativePath"].as_str().unwrap_or_default();
        if Path::new(relative_path).is_absolute()
            || relative_path.split('/').any(|part| part == "..")
        {
            return Err(error(
                "ProjectWriteFileError",
                "The path is outside the workspace.".into(),
            ));
        }
        let path = root.join(relative_path);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|e| error("ProjectWriteFileError", e.to_string()))?;
        }
        std::fs::write(&path, input["contents"].as_str().unwrap_or_default())
            .map_err(|e| error("ProjectWriteFileError", e.to_string()))?;
        Ok(json!({"relativePath": relative_path}))
    })();
    Some(result)
}

pub async fn metadata(input: &Value) -> Option<Reply> {
    if !input.get("paths").is_some() {
        return None;
    }
    let entries = input["paths"].as_array().into_iter().flatten().map(|path| {
        let path = path.as_str().unwrap_or_default();
        std::fs::metadata(path).ok().map(|m| json!({"kind": if m.is_file() {"file"} else if m.is_dir() {"directory"} else {"other"}, "byteLength": m.len()}))
    }).collect::<Vec<_>>();
    Some(Ok(json!({"entries": entries})))
}

pub async fn browse(input: &Value) -> Option<Reply> {
    if !input.get("partialPath").is_some() {
        return None;
    }
    let partial = input["partialPath"].as_str().unwrap_or_default();
    let base = input["cwd"]
        .as_str()
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("/"));
    let target = if Path::new(partial).is_absolute() {
        PathBuf::from(partial)
    } else {
        base.join(partial)
    };
    let directory = if target.is_dir() {
        target
    } else {
        target.parent().unwrap_or(Path::new("/")).to_path_buf()
    };
    let entries = match sorted_entries(Path::new("/"), &directory) {
        Ok(items) => items
            .into_iter()
            .filter_map(|v| {
                let name = Path::new(v["path"].as_str()?)
                    .file_name()?
                    .to_string_lossy()
                    .into_owned();
                Some(json!({"name":name,"fullPath":directory.join(&name).to_string_lossy()}))
            })
            .collect::<Vec<_>>(),
        Err(e) => return Some(Err(error("FilesystemBrowseError", e.to_string()))),
    };
    Some(Ok(
        json!({"parentPath":directory.to_string_lossy(),"entries":entries}),
    ))
}
