//! Keep every path an operation names inside the directories agentd serves.
//!
//! Paths are resolved the way the filesystem would: symlinks are followed
//! (dangling ones too, since writing through one creates its target), `..`
//! is applied to the resolved parent, and a path that does not exist yet is
//! judged by its nearest existing ancestor plus the rest of its components.
use serde_json::Value;
use std::{
    io,
    path::{Component, Path, PathBuf},
};
use t3code_server::workspace_protocol::{op, ErrorCode, WireError};

/// Keys whose string values (or arrays of strings) name filesystem paths.
/// Relative values resolve against the nearest enclosing `cwd`.
const PATH_KEYS: &[&str] = &[
    "cwd",
    "path",
    "paths",
    "relativePath",
    "directoryPath",
    "filePath",
    "workspaceRoot",
    "worktreePath",
    "destinationPath",
    "sourcePath",
    "scriptPath",
    "rootPath",
    "oldPath",
    "newPath",
];

/// How many symlinks one path may pass through before it is refused.
const MAX_LINKS: usize = 40;

pub struct Confinement {
    roots: Vec<PathBuf>,
}

impl Confinement {
    /// Confine to `roots`. Each must exist; they are canonicalized here.
    pub fn new(roots: impl IntoIterator<Item = PathBuf>) -> io::Result<Self> {
        let roots = roots
            .into_iter()
            .map(|root| {
                std::fs::create_dir_all(&root)?;
                root.canonicalize()
            })
            .collect::<io::Result<Vec<_>>>()?;
        Ok(Self { roots })
    }

    /// Whether `path` (absolute, or relative to `base`) resolves inside a root.
    pub fn allows(&self, path: &Path, base: Option<&Path>) -> bool {
        let joined = match (path.is_absolute(), base) {
            (true, _) => path.to_path_buf(),
            (false, Some(base)) => base.join(path),
            (false, None) => match std::env::current_dir() {
                Ok(cwd) => cwd.join(path),
                Err(_) => return false,
            },
        };
        match resolve(&joined) {
            Ok(resolved) => self.roots.iter().any(|root| resolved.starts_with(root)),
            Err(_) => false,
        }
    }

    /// Refuse an operation whose arguments name a path outside the roots.
    pub fn check(&self, op_name: &str, args: &Value) -> Result<(), WireError> {
        if matches!(
            op_name,
            op::HELLO
                | op::PING
                | op::ACK
                | op::RESUME
                | op::IDLE_STATE
                | op::STREAM_CLOSE
                | op::TURN_CANCEL
        ) {
            return Ok(());
        }
        if op_name == op::FS_BROWSE {
            self.check_browse(args)?;
        }
        self.walk(args, None)
    }

    fn refuse(path: &str) -> WireError {
        WireError::new(
            ErrorCode::OutsideRoot,
            format!("{path} is outside the directories this workspace serves"),
        )
    }

    fn check_path(&self, value: &str, base: Option<&Path>) -> Result<(), WireError> {
        if value.is_empty() || self.allows(Path::new(value), base) {
            Ok(())
        } else {
            Err(Self::refuse(value))
        }
    }

    fn walk(&self, value: &Value, base: Option<&Path>) -> Result<(), WireError> {
        match value {
            Value::Object(map) => {
                // A `cwd` is itself relative to the enclosing one.
                let own = match map.get("cwd").and_then(Value::as_str) {
                    Some(cwd) if !cwd.is_empty() => {
                        self.check_path(cwd, base)?;
                        Some(match base {
                            Some(base) => base.join(cwd),
                            None => PathBuf::from(cwd),
                        })
                    }
                    _ => None,
                };
                let base = own.as_deref().or(base);
                for (key, item) in map {
                    if key == "cwd" {
                        continue;
                    }
                    if PATH_KEYS.contains(&key.as_str()) {
                        match item {
                            Value::String(s) => self.check_path(s, base)?,
                            Value::Array(items) => {
                                for item in items {
                                    match item {
                                        Value::String(s) => self.check_path(s, base)?,
                                        other => self.walk(other, base)?,
                                    }
                                }
                            }
                            other => self.walk(other, base)?,
                        }
                    } else {
                        self.walk(item, base)?;
                    }
                }
                Ok(())
            }
            Value::Array(items) => items.iter().try_for_each(|item| self.walk(item, base)),
            _ => Ok(()),
        }
    }

    /// `fs.browse` lists `cwd`/`partialPath` when it is a directory and its
    /// parent otherwise, with `/` as the default `cwd`.
    fn check_browse(&self, args: &Value) -> Result<(), WireError> {
        let Some(partial) = args["partialPath"].as_str() else {
            return Ok(());
        };
        let base = args["cwd"].as_str().unwrap_or("/");
        let target = Path::new(base).join(partial);
        let listed = if target.is_dir() {
            target.clone()
        } else {
            target.parent().unwrap_or(Path::new("/")).to_path_buf()
        };
        if self.allows(&listed, None) {
            Ok(())
        } else {
            Err(Self::refuse(&target.to_string_lossy()))
        }
    }
}

/// Resolve an absolute path as the filesystem would, without requiring it
/// to exist.
pub fn resolve(path: &Path) -> io::Result<PathBuf> {
    let mut links = 0;
    resolve_from(PathBuf::from("/"), path, &mut links)
}

fn resolve_from(mut resolved: PathBuf, path: &Path, links: &mut usize) -> io::Result<PathBuf> {
    for component in path.components() {
        match component {
            Component::Prefix(_) | Component::RootDir => resolved = PathBuf::from("/"),
            Component::CurDir => {}
            Component::ParentDir => {
                resolved.pop();
            }
            Component::Normal(name) => {
                let candidate = resolved.join(name);
                match std::fs::symlink_metadata(&candidate) {
                    Ok(meta) if meta.file_type().is_symlink() => {
                        *links += 1;
                        if *links > MAX_LINKS {
                            return Err(io::Error::other("too many symlinks"));
                        }
                        let target = std::fs::read_link(&candidate)?;
                        // A relative target is relative to the link's directory.
                        resolved = resolve_from(resolved, &target, links)?;
                    }
                    _ => resolved = candidate,
                }
            }
        }
    }
    Ok(resolved)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn setup() -> (tempfile::TempDir, PathBuf, PathBuf, Confinement) {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("root");
        let outside = dir.path().join("outside");
        std::fs::create_dir_all(root.join("project")).unwrap();
        std::fs::create_dir_all(&outside).unwrap();
        let confinement = Confinement::new([root.clone()]).unwrap();
        (dir, root, outside, confinement)
    }

    fn code(result: Result<(), WireError>) -> Option<ErrorCode> {
        result.err().map(|e| e.code)
    }

    #[test]
    fn paths_inside_the_root_pass() {
        let (_dir, root, _, c) = setup();
        let project = root.join("project");
        let cwd = project.to_str().unwrap();
        assert!(c
            .check(
                op::FS_READ_FILE,
                &json!({"cwd": cwd, "relativePath": "a/b.txt"})
            )
            .is_ok());
        assert!(c
            .check(op::GIT_VCS, &json!({"method": "x", "input": {"cwd": cwd}}))
            .is_ok());
        assert!(c
            .check(
                op::TURN_START,
                &json!({"command": ["true"], "prompt": "", "cwd": cwd})
            )
            .is_ok());
    }

    #[test]
    fn absolute_paths_outside_the_root_are_refused() {
        let (_dir, root, outside, c) = setup();
        let cwd = root.join("project");
        let out = outside.to_str().unwrap();
        for (op_name, args) in [
            (op::FS_LIST_ENTRIES, json!({"cwd": out})),
            (op::FS_CREATE_DIR_ALL, json!({"path": out})),
            (op::FS_METADATA, json!({"paths": [cwd, "/etc/passwd"]})),
            (op::GIT_CAPTURE_TREE, json!({"cwd": out})),
            (
                op::TERM_CALL,
                json!({"method": "open", "input": {"cwd": out}}),
            ),
            (
                op::TURN_START,
                json!({"command": ["true"], "prompt": "", "cwd": "/"}),
            ),
            (
                op::FS_READ_FILE,
                json!({"cwd": cwd, "relativePath": "/etc/passwd"}),
            ),
            (op::WATCH_VCS_STATUS, json!({"input": {"cwd": out}})),
        ] {
            assert_eq!(
                code(c.check(op_name, &args)),
                Some(ErrorCode::OutsideRoot),
                "{op_name} {args}"
            );
        }
    }

    #[test]
    fn dot_dot_escapes_are_refused() {
        let (_dir, root, _, c) = setup();
        let cwd = root.join("project");
        let cwd = cwd.to_str().unwrap();
        assert_eq!(
            code(c.check(
                op::FS_WRITE_FILE,
                &json!({"cwd": cwd, "relativePath": "../../outside/x"})
            )),
            Some(ErrorCode::OutsideRoot)
        );
        let escaped = format!("{cwd}/../../outside");
        assert_eq!(
            code(c.check(op::FS_LIST_ENTRIES, &json!({"cwd": escaped}))),
            Some(ErrorCode::OutsideRoot)
        );
        // `..` that stays inside is fine.
        assert!(c
            .check(
                op::FS_WRITE_FILE,
                &json!({"cwd": cwd, "relativePath": "a/../b"})
            )
            .is_ok());
    }

    #[test]
    fn symlinks_out_of_the_root_are_refused() {
        let (_dir, root, outside, c) = setup();
        let link = root.join("project/link");
        std::os::unix::fs::symlink(&outside, &link).unwrap();
        assert_eq!(
            code(c.check(op::FS_LIST_ENTRIES, &json!({"cwd": link}))),
            Some(ErrorCode::OutsideRoot)
        );
        // A path that does not exist yet, under the symlinked directory.
        let missing = link.join("new/dir/file.txt");
        assert!(!missing.exists());
        assert_eq!(
            code(c.check(op::FS_CREATE_DIR_ALL, &json!({"path": missing}))),
            Some(ErrorCode::OutsideRoot)
        );
        let cwd = root.join("project");
        assert_eq!(
            code(c.check(
                op::FS_WRITE_FILE,
                &json!({"cwd": cwd, "relativePath": "link/new.txt"})
            )),
            Some(ErrorCode::OutsideRoot)
        );
        // A dangling link whose target is outside: writing would create it.
        let dangling = root.join("project/dangling");
        std::os::unix::fs::symlink(outside.join("not-yet"), &dangling).unwrap();
        assert_eq!(
            code(c.check(
                op::FS_WRITE_FILE,
                &json!({"cwd": cwd, "relativePath": "dangling"})
            )),
            Some(ErrorCode::OutsideRoot)
        );
        // A link that stays inside is fine.
        let inner = root.join("project/inner");
        std::os::unix::fs::symlink(root.join("project"), &inner).unwrap();
        assert!(c
            .check(op::FS_LIST_ENTRIES, &json!({"cwd": inner.join("x/y")}))
            .is_ok());
    }

    #[test]
    fn a_symlinked_root_is_followed() {
        let (dir, root, _, _) = setup();
        let alias = dir.path().join("alias");
        std::os::unix::fs::symlink(&root, &alias).unwrap();
        let c = Confinement::new([alias.clone()]).unwrap();
        assert!(c
            .check(op::FS_LIST_ENTRIES, &json!({"cwd": root.join("project")}))
            .is_ok());
        assert!(c
            .check(op::FS_LIST_ENTRIES, &json!({"cwd": alias.join("project")}))
            .is_ok());
    }

    #[test]
    fn browse_checks_the_directory_it_lists() {
        let (_dir, root, _, c) = setup();
        let cwd = root.join("project");
        assert!(c
            .check(op::FS_BROWSE, &json!({"cwd": cwd, "partialPath": "pro"}))
            .is_ok());
        assert_eq!(
            code(c.check(op::FS_BROWSE, &json!({"partialPath": "/etc/pa"}))),
            Some(ErrorCode::OutsideRoot)
        );
        assert_eq!(
            code(c.check(op::FS_BROWSE, &json!({"partialPath": "etc"}))),
            Some(ErrorCode::OutsideRoot)
        );
    }
}
