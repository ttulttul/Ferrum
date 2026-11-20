//! Persistence for the server's small state documents: settings, auth and
//! cloud-link records, guest shares, scheduled tasks and keybindings.
//!
//! Documents are addressed by the path the native server keeps them at, so
//! error payloads that name a path (`settingsPath`, `configPath`) read the
//! same everywhere. [`FsStore`] (native) is the local filesystem with each
//! document's existing write semantics. A hosted control plane implements
//! [`DocumentStore`] over Durable Object storage (see `workers/src/storage.rs`),
//! and [`MemoryStore`] keeps documents in memory for tests.
use std::{
    collections::HashMap,
    io,
    path::{Path, PathBuf},
    sync::Mutex,
};

#[cfg(feature = "native")]
mod fs;
#[cfg(feature = "native")]
pub use fs::FsStore;

/// How a write reaches storage. Backends with atomic writes treat every mode
/// alike. On the filesystem they keep each document's existing behaviour.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WriteMode {
    /// Overwrite in place (keybindings.json, as the reference writes it).
    InPlace,
    /// Write a temporary file beside the document, sync it, then rename it
    /// over the document (auth, cloud-link, guest and scheduled-task state).
    Replace,
    /// Follow symbolic links to the final target, stage the contents in a
    /// temporary directory beside it, then rename over it. Failures name the
    /// failing step and path as the reference's settings writer does.
    ReplaceTarget,
}

/// An opaque token that changes whenever a document is written. The
/// filesystem uses the modification time and length.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Revision(pub String);

/// Storage for small documents. Absence is an `io::ErrorKind::NotFound`
/// error, so callers keep distinguishing a missing document from a failing
/// read exactly as they did with `std::fs`.
pub trait DocumentStore: Send + Sync {
    fn read(&self, path: &Path) -> io::Result<Vec<u8>>;
    fn write(&self, path: &Path, bytes: &[u8], mode: WriteMode) -> io::Result<()>;
    /// Make `path` and its ancestors exist, for backends with directories.
    fn create_dir_all(&self, path: &Path) -> io::Result<()>;
    /// The document's current revision, or `None` when it does not exist
    /// or cannot be inspected.
    fn revision(&self, path: &Path) -> Option<Revision>;

    /// `read`, with a missing document as `None`.
    fn read_optional(&self, path: &Path) -> io::Result<Option<Vec<u8>>> {
        match self.read(path) {
            Ok(bytes) => Ok(Some(bytes)),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(error),
        }
    }
}

/// Documents in memory, with a counter for revisions.
#[derive(Default)]
pub struct MemoryStore {
    documents: Mutex<HashMap<PathBuf, (Vec<u8>, u64)>>,
    writes: Mutex<u64>,
}

impl MemoryStore {
    pub fn new() -> Self {
        Self::default()
    }
}

impl DocumentStore for MemoryStore {
    fn read(&self, path: &Path) -> io::Result<Vec<u8>> {
        self.documents
            .lock()
            .unwrap()
            .get(path)
            .map(|(bytes, _)| bytes.clone())
            .ok_or_else(|| not_found(path))
    }

    fn write(&self, path: &Path, bytes: &[u8], _mode: WriteMode) -> io::Result<()> {
        let mut writes = self.writes.lock().unwrap();
        *writes += 1;
        self.documents
            .lock()
            .unwrap()
            .insert(path.to_owned(), (bytes.to_vec(), *writes));
        Ok(())
    }

    fn create_dir_all(&self, _path: &Path) -> io::Result<()> {
        Ok(())
    }

    fn revision(&self, path: &Path) -> Option<Revision> {
        self.documents
            .lock()
            .unwrap()
            .get(path)
            .map(|(_, revision)| Revision(revision.to_string()))
    }
}

/// The error a store returns for a missing document.
pub fn not_found(path: &Path) -> io::Error {
    io::Error::new(
        io::ErrorKind::NotFound,
        format!("no such document: {}", path.display()),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The contract every backend keeps; the filesystem runs it in `fs`.
    pub(crate) fn exercise(store: &dyn DocumentStore, dir: &Path) {
        let path = dir.join("nested/state.json");
        assert_eq!(
            store.read(&path).unwrap_err().kind(),
            io::ErrorKind::NotFound
        );
        assert_eq!(store.read_optional(&path).unwrap(), None);
        assert_eq!(store.revision(&path), None);
        store.create_dir_all(path.parent().unwrap()).unwrap();
        // Distinct lengths: the filesystem revision is mtime and length, and
        // mtime can repeat within one clock tick.
        for (index, mode) in [
            WriteMode::InPlace,
            WriteMode::Replace,
            WriteMode::ReplaceTarget,
        ]
        .into_iter()
        .enumerate()
        {
            let before = store.revision(&path);
            let body = format!("{{\"mode\":\"{mode:?}\"{}}}", " ".repeat(index));
            store.write(&path, body.as_bytes(), mode).unwrap();
            assert_eq!(store.read(&path).unwrap(), body.as_bytes());
            let after = store.revision(&path);
            assert!(after.is_some());
            assert_ne!(before, after, "{mode:?} must change the revision");
        }
    }

    #[test]
    fn memory_store_keeps_the_contract() {
        exercise(&MemoryStore::new(), Path::new("/virtual"));
    }
}
