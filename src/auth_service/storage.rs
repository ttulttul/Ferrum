use serde::{de::DeserializeOwned, Serialize};
use std::{
    fs,
    io::{self, Write},
    path::{Path, PathBuf},
    sync::Mutex,
};

/// A small transactional state file. Publish the new in-memory snapshot only after rename succeeds.
pub(crate) struct Storage<T> {
    path: Option<PathBuf>,
    state: Mutex<T>,
}

impl<T: Default> Default for Storage<T> {
    fn default() -> Self {
        Self {
            path: None,
            state: Mutex::default(),
        }
    }
}

impl<T: Default + DeserializeOwned + Serialize + Clone> Storage<T> {
    pub(crate) fn open(path: &Path) -> anyhow::Result<Self> {
        let state = match fs::read(path) {
            Ok(bytes) => serde_json::from_slice(&bytes)?,
            Err(err) if err.kind() == io::ErrorKind::NotFound => T::default(),
            Err(err) => return Err(err.into()),
        };
        Ok(Self {
            path: Some(path.to_owned()),
            state: Mutex::new(state),
        })
    }

    pub(crate) fn read<R>(&self, inspect: impl FnOnce(&T) -> R) -> R {
        inspect(&self.state.lock().unwrap())
    }

    pub(crate) fn update<R>(
        &self,
        edit: impl FnOnce(&mut T) -> Result<R, &'static str>,
    ) -> Result<R, &'static str> {
        let mut current = self.state.lock().unwrap();
        let mut next = current.clone();
        let result = edit(&mut next)?;
        if let Some(path) = &self.path {
            let dir = path.parent().ok_or("internal_error")?;
            let mut tmp = tempfile::NamedTempFile::new_in(dir).map_err(|_| "internal_error")?;
            serde_json::to_writer(&mut tmp, &next).map_err(|_| "internal_error")?;
            tmp.flush()
                .and_then(|_| tmp.as_file().sync_all())
                .map_err(|_| "internal_error")?;
            tmp.persist(path).map_err(|_| "internal_error")?;
        }
        *current = next;
        Ok(result)
    }
}
