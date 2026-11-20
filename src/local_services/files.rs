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

fn within(root: &Path, path: &Path) -> bool {
    let Ok(root) = std::fs::canonicalize(root) else {
        return false;
    };
    let Ok(path) = std::fs::canonicalize(path) else {
        return false;
    };
    path.starts_with(root)
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

fn is_ignored(root: &Path, path: &Path) -> bool {
    let Some(relative) = path.strip_prefix(root).ok() else {
        return false;
    };
    std::process::Command::new("git")
        .args([
            "-C",
            root.to_str().unwrap_or_default(),
            "check-ignore",
            "--quiet",
            "--",
            &relative.to_string_lossy(),
        ])
        .status()
        .ok()
        .is_some_and(|status| status.success())
}

fn sorted_entries(root: &Path, directory: &Path) -> std::io::Result<Vec<Value>> {
    let mut values = std::fs::read_dir(directory)?
        .filter_map(Result::ok)
        .filter_map(|item| {
            let path = item.path();
            (path.file_name().and_then(|n| n.to_str()) != Some(".git"))
                .then(|| entry(root, &path, is_ignored(root, &path)))
                .flatten()
        })
        .collect::<Vec<_>>();
    values.sort_by(|a, b| a["path"].as_str().cmp(&b["path"].as_str()));
    Ok(values)
}

pub async fn list_entries(input: &Value) -> Option<Reply> {
    input.get("cwd")?;
    let result = (|| {
        let root = root(input)?;
        let directory = input["directoryPath"].as_str().unwrap_or_default();
        let directory = root.join(directory);
        if !directory.is_dir() || !within(&root, &directory) {
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

pub async fn list_ignored_files(input: &Value) -> Option<Reply> {
    input.get("cwd")?;
    let result = (|| {
        let root = root(input)?;
        let mut files = Vec::new();
        fn visit(root: &Path, directory: &Path, files: &mut Vec<Value>) {
            let Ok(items) = std::fs::read_dir(directory) else {
                return;
            };
            let mut paths = items
                .filter_map(Result::ok)
                .map(|e| e.path())
                .collect::<Vec<_>>();
            paths.sort();
            for path in paths {
                if path.file_name().and_then(|n| n.to_str()) == Some(".git") {
                    continue;
                }
                let directory = path.is_dir();
                let ignored = is_ignored(root, &path);
                if directory && ignored {
                    continue;
                }
                if directory {
                    visit(root, &path, files);
                } else if ignored {
                    if let (Some(relative), Ok(metadata)) =
                        (path.strip_prefix(root).ok(), std::fs::metadata(&path))
                    {
                        files.push(json!({"path": relative.to_string_lossy().replace('\\', "/"), "byteLength": metadata.len()}));
                    }
                }
            }
        }
        visit(&root, &root, &mut files);
        Ok(json!({"files": files, "truncated": false}))
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
        if path.file_name().and_then(|n| n.to_str()) == Some(".git") || is_ignored(root, &path) {
            continue;
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
    input.get("cwd")?;
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
    input.get("cwd")?;
    let result = (|| {
        let root = root(input)?;
        let query = input["query"].as_str().unwrap_or_default();
        let limit = input["limit"].as_u64().unwrap_or(100).clamp(1, 10_000) as usize;
        let case_sensitive = input["caseSensitive"].as_bool().unwrap_or(false);
        let whole_word = input["wholeWord"].as_bool().unwrap_or(false);
        let use_regex = input["useRegex"].as_bool().unwrap_or(false);
        let needle = if case_sensitive {
            query.to_owned()
        } else {
            query.to_lowercase()
        };
        let expression = if use_regex {
            regex::RegexBuilder::new(query)
                .case_insensitive(!case_sensitive)
                .build()
                .ok()
        } else {
            None
        };
        let regex_fallback_error = use_regex && expression.is_none();
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
                let ranges = if let Some(expression) = &expression {
                    expression
                        .find_iter(line)
                        .filter(|m| !whole_word || whole_word_match(line, m.start(), m.end()))
                        .map(|m| json!({"start": m.start(), "end": m.end()}))
                        .collect::<Vec<_>>()
                } else {
                    let mut ranges = Vec::new();
                    let mut offset = 0;
                    while !needle.is_empty() && offset <= haystack.len() {
                        let Some(found) = haystack[offset..].find(&needle) else {
                            break;
                        };
                        let start = offset + found;
                        let end = start + needle.len();
                        if !whole_word || whole_word_match(&haystack, start, end) {
                            ranges.push(json!({"start": start, "end": end}));
                        }
                        offset = end.max(start + 1);
                    }
                    ranges
                };
                if !ranges.is_empty() {
                    matches.push(json!({"path":file["path"],"lineNumber":index + 1,"lineContent":line,"matchRanges":ranges}));
                    if matches.len() >= limit {
                        break;
                    }
                }
            }
        }
        let mut result = json!({"matches": matches, "truncated": matches.len() == limit});
        if regex_fallback_error {
            result["regexFallbackError"] = json!("Invalid regular expression");
        }
        Ok(result)
    })();
    Some(result)
}

fn whole_word_match(text: &str, start: usize, end: usize) -> bool {
    fn word(byte: Option<u8>) -> bool {
        byte.is_some_and(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
    }
    !word(text.as_bytes().get(start.wrapping_sub(1)).copied())
        && !word(text.as_bytes().get(end).copied())
}

pub async fn read_file(input: &Value) -> Option<Reply> {
    input.get("cwd")?;
    let result = (|| {
        let root = root(input)?;
        let relative_path = input["relativePath"].as_str().unwrap_or_default();
        let path = if Path::new(relative_path).is_absolute() {
            PathBuf::from(relative_path)
        } else {
            root.join(relative_path)
        };
        if !Path::new(relative_path).is_absolute() && !within(&root, &path) {
            return Err(error(
                "ProjectReadFileError",
                "The path is outside the workspace.".into(),
            ));
        }
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
    input.get("cwd")?;
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
    input.get("paths")?;
    let entries = input["paths"]
        .as_array()
        .into_iter()
        .flatten()
        .map(|path| {
            let path = path.as_str().unwrap_or_default();
            std::fs::metadata(path).ok().map(|m| {
                let kind = if m.is_file() {
                    "file"
                } else if m.is_dir() {
                    "directory"
                } else {
                    "other"
                };
                let mut value = json!({"kind": kind});
                if m.is_file() {
                    value["byteLength"] = json!(m.len());
                    if let Some(mime) = mime_type(Path::new(path)) {
                        value["mimeType"] = json!(mime);
                    }
                }
                value
            })
        })
        .collect::<Vec<_>>();
    Some(Ok(json!({"entries": entries})))
}

fn mime_type(path: &Path) -> Option<&'static str> {
    match path.extension()?.to_str()?.to_ascii_lowercase().as_str() {
        "js" | "mjs" | "cjs" => Some("text/javascript"),
        "ts" | "tsx" => Some("text/typescript"),
        "jsx" => Some("text/jsx"),
        "json" => Some("application/json"),
        "css" => Some("text/css"),
        "html" | "htm" => Some("text/html"),
        "md" => Some("text/markdown"),
        "py" => Some("text/x-python"),
        "rs" => Some("text/x-rust"),
        "png" => Some("image/png"),
        "jpg" | "jpeg" => Some("image/jpeg"),
        "gif" => Some("image/gif"),
        "svg" => Some("image/svg+xml"),
        "webp" => Some("image/webp"),
        "pdf" => Some("application/pdf"),
        _ => None,
    }
}

pub async fn browse(input: &Value) -> Option<Reply> {
    input.get("partialPath")?;
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
