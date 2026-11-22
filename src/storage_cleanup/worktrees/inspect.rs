use super::*;
use std::time::{Duration, Instant};

#[derive(Clone, Copy, PartialEq)]
pub enum Scan {
    Size,
}

#[cfg(unix)]
fn bytes(meta: &fs::Metadata) -> u64 {
    use std::os::unix::fs::MetadataExt;
    if meta.nlink() == 1 {
        meta.blocks() * 512
    } else {
        0
    }
}
#[cfg(not(unix))]
fn bytes(meta: &fs::Metadata) -> u64 {
    meta.len()
}

pub fn measure(path: &Path, mode: Scan, deadline: Instant) -> Result<u64, String> {
    let root = path
        .canonicalize()
        .map_err(|_| "Worktree could not be fully inspected".to_string())?;
    let mut pending = vec![root.clone()];
    let mut total = 0;
    let mut entries = 1usize;
    while let Some(target) = pending.pop() {
        if Instant::now() > deadline || entries > 2_000_000 {
            return Err("Worktree could not be fully inspected".into());
        }
        let meta = fs::symlink_metadata(&target)
            .map_err(|_| "Worktree could not be fully inspected".to_string())?;
        if meta.file_type().is_symlink() {
            continue;
        }
        if meta.is_dir() {
            for child in fs::read_dir(&target)
                .map_err(|_| "Worktree could not be fully inspected".to_string())?
            {
                let child =
                    child.map_err(|_| "Worktree could not be fully inspected".to_string())?;
                entries += 1;
                if child.path().canonicalize().ok().as_ref() == Some(&child.path()) {
                    pending.push(child.path());
                }
            }
        } else if meta.is_file() && mode == Scan::Size {
            total += bytes(&meta);
        }
    }
    Ok(total)
}
pub fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(30)
}
