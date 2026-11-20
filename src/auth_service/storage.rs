use crate::persistence::{DocumentStore, WriteMode};
use serde::{de::DeserializeOwned, Serialize};
use std::{
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

/// A small transactional state document. Publish the new in-memory snapshot
/// only after the store accepts the write.
pub(crate) struct Storage<T> {
    document: Option<(Arc<dyn DocumentStore>, PathBuf)>,
    state: Mutex<T>,
}

impl<T: Default> Default for Storage<T> {
    fn default() -> Self {
        Self {
            document: None,
            state: Mutex::default(),
        }
    }
}

impl<T: Default + DeserializeOwned + Serialize + Clone> Storage<T> {
    /// The document at `path` on the local filesystem.
    #[cfg(feature = "native")]
    pub(crate) fn open(path: &Path) -> anyhow::Result<Self> {
        Self::open_in(Arc::new(crate::persistence::FsStore), path)
    }

    /// The document at `path` in `documents`; missing means the default.
    pub(crate) fn open_in(documents: Arc<dyn DocumentStore>, path: &Path) -> anyhow::Result<Self> {
        let state = match documents.read_optional(path)? {
            Some(bytes) => serde_json::from_slice(&bytes)?,
            None => T::default(),
        };
        Ok(Self {
            document: Some((documents, path.to_owned())),
            state: Mutex::new(state),
        })
    }

    /// State kept only in memory; its host persists it (see `AuthService::records_json`).
    pub(crate) fn in_memory(state: T) -> Self {
        Self {
            document: None,
            state: Mutex::new(state),
        }
    }

    pub(crate) fn read<R>(&self, inspect: impl FnOnce(&T) -> R) -> R {
        inspect(&self.state.lock().unwrap())
    }

    pub(crate) fn update<R>(
        &self,
        edit: impl FnOnce(&mut T) -> Result<R, &'static str>,
    ) -> Result<R, &'static str> {
        self.update_observed(edit, |_, _| {})
    }
    pub(crate) fn update_observed<R>(
        &self,
        edit: impl FnOnce(&mut T) -> Result<R, &'static str>,
        committed: impl FnOnce(&T, &T),
    ) -> Result<R, &'static str> {
        let mut current = self.state.lock().unwrap();
        let mut next = current.clone();
        let result = edit(&mut next)?;
        if let Some((documents, path)) = &self.document {
            let bytes = serde_json::to_vec(&next).map_err(|_| "internal_error")?;
            documents
                .write(path, &bytes, WriteMode::Replace)
                .map_err(|_| "internal_error")?;
        }
        committed(&current, &next);
        *current = next;
        Ok(result)
    }
}
