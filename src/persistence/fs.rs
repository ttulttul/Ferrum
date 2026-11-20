//! The local filesystem as a [`DocumentStore`] (native).
use super::{DocumentStore, Revision, WriteMode};
use crate::settings::failure;
use std::{
    fs,
    io::{self, Write},
    path::Path,
};

/// Documents at their paths on the local filesystem.
#[derive(Clone, Copy, Debug, Default)]
pub struct FsStore;

impl DocumentStore for FsStore {
    fn read(&self, path: &Path) -> io::Result<Vec<u8>> {
        fs::read(path)
    }

    fn write(&self, path: &Path, bytes: &[u8], mode: WriteMode) -> io::Result<()> {
        match mode {
            WriteMode::InPlace => fs::write(path, bytes),
            WriteMode::Replace => replace(path, bytes),
            WriteMode::ReplaceTarget => replace_target(path, bytes),
            WriteMode::Private => private(path, bytes),
        }
    }

    fn remove(&self, path: &Path) -> io::Result<()> {
        fs::remove_file(path)
    }

    fn create_dir_all(&self, path: &Path) -> io::Result<()> {
        fs::create_dir_all(path)
    }

    fn create_private_dir_all(&self, path: &Path) -> io::Result<()> {
        fs::create_dir_all(path)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(path, fs::Permissions::from_mode(0o700))?;
        }
        Ok(())
    }

    fn exists(&self, path: &Path) -> io::Result<bool> {
        path.try_exists()
    }

    fn revision(&self, path: &Path) -> Option<Revision> {
        let meta = path.metadata().ok()?;
        let modified = meta
            .modified()
            .ok()?
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as i128)
            .unwrap_or_else(|e| -(e.duration().as_nanos() as i128));
        Some(Revision(format!("{modified}:{}", meta.len())))
    }
}

/// A synced temporary file beside `path`, renamed over it.
fn replace(path: &Path, bytes: &[u8]) -> io::Result<()> {
    let dir = path
        .parent()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "document has no directory"))?;
    let mut tmp = tempfile::NamedTempFile::new_in(dir)?;
    tmp.write_all(bytes)?;
    tmp.flush()?;
    tmp.as_file().sync_all()?;
    tmp.persist(path).map_err(|e| e.error)?;
    Ok(())
}

/// A new owner-only temporary file beside `path`, written, synced and
/// renamed over it; removed again on failure. Each failure names its step.
fn private(path: &Path, bytes: &[u8]) -> io::Result<()> {
    let mut name = path.file_name().unwrap_or_default().to_owned();
    name.push(format!(".{}.tmp", crate::platform::uuid_v4()));
    let tmp = path.with_file_name(name);
    let mut opts = fs::OpenOptions::new();
    opts.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        opts.mode(0o600);
    }
    let result = (|| {
        let mut f = opts
            .open(&tmp)
            .map_err(|e| failure::step(e, "writeFile", "open", &tmp, None))?;
        f.write_all(bytes)
            .map_err(|e| failure::step(e, "writeFile", "write", &tmp, None))?;
        f.sync_all()
            .map_err(|e| failure::step(e, "writeFile", "fsync", &tmp, None))?;
        drop(f);
        fs::rename(&tmp, path).map_err(|e| failure::step(e, "rename", "rename", &tmp, Some(path)))
    })();
    if result.is_err() {
        let _ = fs::remove_file(&tmp);
    }
    result
}

/// The reference's settings write: follow links to the final target, create
/// its directory, stage the contents in a temporary directory beside it and
/// rename into place. Each failure names its step, syscall and path.
fn replace_target(path: &Path, bytes: &[u8]) -> io::Result<()> {
    let mut target = path.to_owned();
    for _ in 0..=40 {
        match fs::read_link(&target) {
            Ok(next) => {
                let parent = target.parent().unwrap_or(Path::new("."));
                target = fs::canonicalize(parent)?.join(next);
            }
            Err(e)
                if matches!(
                    e.kind(),
                    io::ErrorKind::InvalidInput | io::ErrorKind::NotFound
                ) =>
            {
                let parent = target.parent().unwrap_or(Path::new("."));
                fs::create_dir_all(parent).map_err(|e| {
                    failure::file(e, "write-file", "makeDirectory", "mkdir", parent)
                })?;
                let staging = tempfile::Builder::new()
                    .prefix("t3-write-")
                    .tempdir_in(parent)
                    .map_err(|e| {
                        failure::file(
                            e,
                            "write-file",
                            "makeTempDirectory",
                            "mkdtemp",
                            &parent.join("t3-write-"),
                        )
                    })?;
                let temp = staging.path().join("contents.tmp");
                let mut file = fs::File::create(&temp)
                    .map_err(|e| failure::file(e, "write-file", "writeFile", "open", &temp))?;
                file.write_all(bytes)?;
                drop(file);
                rename_over(&temp, &target).map_err(|e| {
                    let step = failure::step(e, "rename", "rename", &temp, Some(&target));
                    failure::file(step, "write-file", "rename", "rename", &temp)
                })?;
                return Ok(());
            }
            Err(e) => return Err(e),
        }
    }
    Err(io::Error::other("Too many levels of symbolic links"))
}

#[cfg(not(windows))]
fn rename_over(temp: &Path, target: &Path) -> io::Result<()> {
    fs::rename(temp, target)
}

#[cfg(windows)]
fn rename_over(temp: &Path, target: &Path) -> io::Result<()> {
    use std::os::windows::fs::MetadataExt;
    let version = || match fs::metadata(target) {
        Ok(m) => Ok(Some((
            m.creation_time(),
            m.last_write_time(),
            m.file_size(),
            m.permissions().readonly(),
        ))),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e),
    };
    let initial = version();
    for retry in 0..=20 {
        match fs::rename(temp, target) {
            Ok(()) => return Ok(()),
            Err(e) => {
                let unchanged = initial.as_ref().is_ok_and(|v| {
                    v.is_none_or(|(_, _, _, ro)| !ro)
                        && version().is_ok_and(|current| &current == v)
                });
                if retry == 20 || !matches!(e.raw_os_error(), Some(5 | 32 | 33)) || !unchanged {
                    return Err(e);
                }
                std::thread::sleep(std::time::Duration::from_millis(100));
                if version().ok().as_ref() != initial.as_ref().ok() {
                    return Err(e);
                }
            }
        }
    }
    unreachable!()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn filesystem_keeps_the_contract() {
        let dir = tempfile::tempdir().unwrap();
        super::super::tests::exercise(&FsStore, dir.path());
    }

    #[cfg(unix)]
    #[test]
    fn replace_target_writes_through_a_symlink() {
        let dir = tempfile::tempdir().unwrap();
        let real = dir.path().join("real.json");
        let link = dir.path().join("link.json");
        std::os::unix::fs::symlink(&real, &link).unwrap();
        FsStore
            .write(&link, b"{}\n", WriteMode::ReplaceTarget)
            .unwrap();
        assert!(fs::symlink_metadata(&link)
            .unwrap()
            .file_type()
            .is_symlink());
        assert_eq!(fs::read(&real).unwrap(), b"{}\n");
    }
}
