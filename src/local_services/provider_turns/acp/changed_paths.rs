//! The paths an ACP call changes, accumulated across its updates and permission request.
use serde_json::Value;
use std::collections::HashSet;
use std::path::Path;

#[derive(Clone, Default)]
pub(super) struct ChangedPaths {
    paths: HashSet<String>,
    unknown: bool,
}

impl ChangedPaths {
    fn add(&mut self, value: &Value) {
        let Some(text) = value.as_str() else {
            self.unknown = true;
            return;
        };
        let scheme = text.split_once(':').is_some_and(|(prefix, _)| {
            prefix.starts_with(|c: char| c.is_ascii_alphabetic())
                && prefix
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b"+.-".contains(&b))
        });
        if text.trim().is_empty()
            || text.contains('\0')
            || (!Path::new(text).is_absolute()
                && (scheme || text.starts_with('~') || text.contains('\\')))
        {
            self.unknown = true;
        } else if !self.unknown {
            self.paths.insert(text.to_owned());
            self.unknown = self.paths.len() > 256;
        }
    }

    pub(super) fn observe(&mut self, call: &Value) {
        if !call["locations"].is_null() {
            if let Some(locations) = call["locations"].as_array() {
                for entry in locations {
                    self.add(&entry["path"]);
                }
            } else {
                self.unknown = true;
            }
        }
        if !call["content"].is_null() {
            if let Some(entries) = call["content"].as_array() {
                for entry in entries {
                    self.observe_content(entry);
                }
            } else {
                self.unknown = true;
            }
        }
        for key in ["path", "filePath", "file_path"] {
            let value = &call["rawInput"][key];
            if !value.is_null() {
                self.add(value);
            }
        }
    }

    pub(super) fn observe_content(&mut self, entry: &Value) {
        match entry["type"].as_str() {
            Some("content" | "terminal") => {}
            Some("diff") if entry["changes"].is_null() => self.add(&entry["path"]),
            Some("diff") => {
                if !entry["path"].is_null() {
                    self.add(&entry["path"]);
                }
                if let Some(changes) = entry["changes"].as_array() {
                    for change in changes {
                        match change["operation"].as_str() {
                            Some("move") => self.add(&change["oldPath"]),
                            Some("add" | "delete" | "modify" | "copy") => {}
                            _ => self.unknown = true,
                        }
                        self.add(&change["path"]);
                    }
                } else {
                    self.unknown = true;
                }
            }
            _ => self.unknown = true,
        }
    }

    pub(super) fn inside(&self, cwd: &Path) -> bool {
        if self.unknown || self.paths.is_empty() {
            return false;
        }
        let Some(root) = super::resolve_existing(cwd) else {
            return false;
        };
        self.paths.iter().all(|p| {
            super::resolve_existing(&cwd.join(p)).is_some_and(|path| path.starts_with(&root))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    #[test]
    fn all_paths_from_notifications_and_requests_must_be_in_the_project() {
        let project = tempfile::tempdir().unwrap();
        let mut paths = ChangedPaths::default();
        assert!(!paths.inside(project.path()));
        paths.observe(&json!({"content":[{"type":"diff","changes":[{"operation":"move","path":"a","oldPath":"b"}]}]}));
        assert!(paths.inside(project.path()));
        paths.observe(&json!({"locations":[{"path":"../another-project/file"}]}));
        assert!(!paths.inside(project.path()));
        let mut incomplete = ChangedPaths::default();
        incomplete.observe(&json!({"locations":[{}],"rawInput":{"path":"a"}}));
        assert!(!incomplete.inside(project.path()));
    }
}
