use crate::settings::{failure, Settings};
use serde_json::{json, Value};
use std::{
    fs, io,
    path::Path,
    time::{Duration, UNIX_EPOCH},
};

pub(super) enum Category {
    BrowserArtifacts,
    Logs,
}

impl Category {
    fn setting(&self) -> &'static str {
        match self {
            Self::BrowserArtifacts => "browserArtifactsAfterDays",
            Self::Logs => "logsAfterDays",
        }
    }

    pub(super) fn clean(
        &self,
        root: &Path,
        now: i64,
        initial: &Value,
        settings: &Settings,
    ) -> Option<Value> {
        let days = initial["storageCleanup"][self.setting()].as_u64()?;
        let cutoff =
            UNIX_EPOCH + Duration::from_millis(now as u64) - Duration::from_secs(days * 86_400);
        let mut removed = Removed::default();
        let result = (|| -> anyhow::Result<()> {
            if !root.try_exists()? {
                return Ok(());
            }
            let real = fs::canonicalize(root).map_err(|e| fs_error(e, "realPath", root))?;
            let parent = root.parent().expect("category directory has a parent");
            let parent = fs::canonicalize(parent).map_err(|e| fs_error(e, "realPath", parent))?;
            if real.parent() != Some(&parent) || real.file_name() != root.file_name() {
                return Ok(());
            }
            let directory = Root::open(&real).map_err(|e| fs_error(e, "readDirectory", &real))?;
            // Node's readdir visits sorted names. Preserve its depth-first order:
            // a failed entry reports exactly the files removed before that error.
            let mut pending = children(&real)?;
            while let Some(path) = pending.pop() {
                // Links are skipped before they are resolved (#40528).
                let meta = fs::symlink_metadata(&path).map_err(|e| fs_error(e, "stat", &path))?;
                if meta.file_type().is_symlink() {
                    continue;
                }
                if fs::canonicalize(&path).map_err(|e| fs_error(e, "realPath", &path))? != path {
                    continue;
                }
                let logs = matches!(self, Self::Logs);
                if logs && meta.is_dir() {
                    pending.extend(children(&path)?);
                } else if meta.is_file()
                    && (!logs || rotated_log(&path.file_name().unwrap().to_string_lossy()))
                    && meta.modified()? < cutoff
                {
                    if settings.internal()?["storageCleanup"][self.setting()].as_u64() != Some(days)
                    {
                        return Ok(());
                    }
                    if directory
                        .remove(path.strip_prefix(&real)?, &meta)
                        .map_err(|e| fs_error(e, "remove", &path))?
                    {
                        removed.files += 1;
                        removed.bytes += meta.len();
                    }
                }
            }
            Ok(())
        })();
        let (kind, label) = match self {
            Self::BrowserArtifacts => ("browser-artifacts", "browser artifacts"),
            Self::Logs => ("logs", "rotated logs"),
        };
        let (outcome, reason) = match result {
            Err(e) => (
                "failed",
                e.to_string().lines().next().unwrap_or("").to_owned(),
            ),
            Ok(()) if removed.files > 0 => {
                ("removed", format!("Removed {} {label}", removed.files))
            }
            Ok(()) => ("kept", "No expired files".to_owned()),
        };
        Some(json!({"kind":kind,"outcome":outcome,"reason":reason,
            "path":null,"projectPath":null,"threadId":null,"threadTitle":null,
            "bytes":(removed.files > 0).then_some(removed.bytes),"files":(removed.files > 0).then_some(removed.files)}))
    }
}

fn children(dir: &Path) -> io::Result<Vec<std::path::PathBuf>> {
    let mut entries = fs::read_dir(dir)
        .and_then(|iter| {
            iter.map(|r| r.map(|e| e.path()))
                .collect::<io::Result<Vec<_>>>()
        })
        .map_err(|e| fs_error(e, "readDirectory", dir))?;
    entries.sort_unstable_by(|a, b| b.cmp(a));
    Ok(entries)
}

fn fs_error(error: io::Error, method: &'static str, path: &Path) -> io::Error {
    failure::file(error, "storage-cleanup", method, method, path)
}

#[derive(Default)]
struct Removed {
    files: u64,
    bytes: u64,
}

fn rotated_log(name: &str) -> bool {
    let Some((base, ordinal)) = name.rsplit_once('.') else {
        return false;
    };
    (base.ends_with(".log") || base.ends_with(".ndjson"))
        && !ordinal.is_empty()
        && ordinal.bytes().all(|b| b.is_ascii_digit())
}

// Resolve directory components relative to the original root descriptor on Unix.
// A rename or symlink swap during a settings read cannot redirect an unlink.
#[cfg(unix)]
struct Root(std::os::fd::OwnedFd);
#[cfg(unix)]
impl Root {
    fn open(path: &Path) -> io::Result<Self> {
        use rustix::fs::{open, Mode, OFlags};
        Ok(Self(open(
            path,
            OFlags::RDONLY | OFlags::CLOEXEC | OFlags::DIRECTORY | OFlags::NOFOLLOW,
            Mode::empty(),
        )?))
    }
    fn remove(&self, relative: &Path, meta: &fs::Metadata) -> io::Result<bool> {
        use rustix::fs::{openat, statat, unlinkat, AtFlags, Mode, OFlags};
        use std::os::unix::fs::MetadataExt;
        let mut parent = self.0.try_clone()?;
        for part in relative.parent().unwrap().components() {
            parent = openat(
                &parent,
                part.as_os_str(),
                OFlags::RDONLY | OFlags::CLOEXEC | OFlags::DIRECTORY | OFlags::NOFOLLOW,
                Mode::empty(),
            )?;
        }
        let name = relative.file_name().unwrap();
        let stat = statat(&parent, name, AtFlags::SYMLINK_NOFOLLOW)?;
        if i128::from(stat.st_dev) != i128::from(meta.dev())
            || i128::from(stat.st_ino) != i128::from(meta.ino())
            || i128::from(stat.st_size) != i128::from(meta.size())
            || i128::from(stat.st_mtime) != i128::from(meta.mtime())
            || i128::from(stat.st_mtime_nsec) != i128::from(meta.mtime_nsec())
        {
            return Ok(false);
        }
        unlinkat(&parent, name, AtFlags::empty())?;
        Ok(true)
    }
}

#[cfg(not(unix))]
struct Root(std::path::PathBuf);
#[cfg(not(unix))]
impl Root {
    fn open(path: &Path) -> io::Result<Self> {
        Ok(Self(path.to_owned()))
    }
    fn remove(&self, relative: &Path, meta: &fs::Metadata) -> io::Result<bool> {
        let path = self.0.join(relative);
        if fs::canonicalize(&path)? != path {
            return Ok(false);
        }
        let current = fs::symlink_metadata(&path)?;
        if !current.is_file()
            || current.modified()? != meta.modified()?
            || current.len() != meta.len()
        {
            return Ok(false);
        }
        fs::remove_file(path)?;
        Ok(true)
    }
}
