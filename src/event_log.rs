//! Where committed orchestration events are kept. The store is rebuilt from
//! the log at start-up, and each commit appends one record before the store
//! applies it.
//!
//! The native server keeps an NDJSON file. The workers control plane keeps the
//! log in Durable Object SQLite storage (see `workers/src/storage.rs`). Tests
//! and throwaway environments can use [`MemoryLog`].
use crate::ServerEvent;

pub trait EventLog {
    /// Every record committed so far, oldest first. Records that no longer
    /// decode are skipped, as the NDJSON reader always has.
    fn load(&mut self) -> anyhow::Result<Vec<ServerEvent>>;
    /// Durably records one event. The caller applies it only after this succeeds.
    fn append(&mut self, record: &ServerEvent) -> anyhow::Result<()>;
}

/// Decodes NDJSON log text, skipping lines that do not parse.
pub fn parse_ndjson(contents: &str) -> Vec<ServerEvent> {
    contents
        .lines()
        .filter_map(|line| serde_json::from_str::<ServerEvent>(line).ok())
        .collect()
}

#[derive(Default)]
pub struct MemoryLog {
    records: Vec<ServerEvent>,
}
impl EventLog for MemoryLog {
    fn load(&mut self) -> anyhow::Result<Vec<ServerEvent>> {
        Ok(self.records.clone())
    }
    fn append(&mut self, record: &ServerEvent) -> anyhow::Result<()> {
        self.records.push(record.clone());
        Ok(())
    }
}

/// `events.jsonl` in the state directory: one JSON record per line.
#[cfg(feature = "native")]
pub struct NdjsonLog {
    path: std::path::PathBuf,
    file: std::fs::File,
}
#[cfg(feature = "native")]
impl NdjsonLog {
    /// Opens (creating) the log for appending, so an unwritable state
    /// directory fails at start-up rather than on the first command.
    pub fn open(path: impl Into<std::path::PathBuf>) -> anyhow::Result<Self> {
        let path = path.into();
        let file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)?;
        Ok(Self { path, file })
    }
}
#[cfg(feature = "native")]
impl EventLog for NdjsonLog {
    fn load(&mut self) -> anyhow::Result<Vec<ServerEvent>> {
        // An unreadable log starts an empty store, as it always has.
        Ok(std::fs::read_to_string(&self.path)
            .map(|contents| parse_ndjson(&contents))
            .unwrap_or_default())
    }
    fn append(&mut self, record: &ServerEvent) -> anyhow::Result<()> {
        use std::io::Write;
        let mut bytes = serde_json::to_vec(record)?;
        bytes.push(b'\n');
        self.file.write_all(&bytes)?;
        self.file.flush()?;
        Ok(())
    }
}
