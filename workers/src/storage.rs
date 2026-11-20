//! SQL storage for the control plane: the orchestration
//! event log, a small key/value table (environment id) and the state
//! documents (auth records, keybindings) behind `DocumentStore`.
use serde::Deserialize;
use std::{io, path::Path};
use t3code_server::{
    event_log::EventLog,
    persistence::{self, DocumentStore, Revision, WriteMode},
    ServerEvent,
};
use worker::SqlStorage;

pub fn migrate(sql: &SqlStorage) -> worker::Result<()> {
    sql.exec(
        "CREATE TABLE IF NOT EXISTS events (id INTEGER PRIMARY KEY, record TEXT NOT NULL)",
        None,
    )?;
    sql.exec(
        "CREATE TABLE IF NOT EXISTS kv (key TEXT PRIMARY KEY, value TEXT NOT NULL)",
        None,
    )?;
    sql.exec(
        "CREATE TABLE IF NOT EXISTS documents (path TEXT PRIMARY KEY, body TEXT NOT NULL, revision INTEGER NOT NULL)",
        None,
    )?;
    Ok(())
}

#[derive(Deserialize)]
struct Value {
    value: String,
}

pub fn get(sql: &SqlStorage, key: &str) -> worker::Result<Option<String>> {
    let rows: Vec<Value> = sql
        .exec("SELECT value FROM kv WHERE key = ?", vec![key.into()])?
        .to_array()?;
    Ok(rows.into_iter().next().map(|row| row.value))
}

pub fn put(sql: &SqlStorage, key: &str, value: &str) -> worker::Result<()> {
    sql.exec(
        "INSERT INTO kv (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        vec![key.into(), value.into()],
    )?;
    Ok(())
}

/// The event log as rows of `events`, one JSON record each.
pub struct SqlLog(pub SqlStorage);

#[derive(Deserialize)]
struct Record {
    record: String,
}

impl EventLog for SqlLog {
    fn load(&mut self) -> anyhow::Result<Vec<ServerEvent>> {
        let rows: Vec<Record> = self
            .0
            .exec("SELECT record FROM events ORDER BY id", None)
            .and_then(|cursor| cursor.to_array())
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        Ok(rows
            .iter()
            .filter_map(|row| serde_json::from_str(&row.record).ok())
            .collect())
    }

    fn append(&mut self, record: &ServerEvent) -> anyhow::Result<()> {
        let text = serde_json::to_string(record)?;
        self.0
            .exec(
                "INSERT INTO events (id, record) VALUES (?, ?)",
                vec![(record.id as i64).into(), text.into()],
            )
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        Ok(())
    }
}

/// State documents as rows of `documents`, keyed by path. Every write is one
/// SQL statement, so it is atomic whatever the `WriteMode`; the revision is a
/// per-document counter. Bodies are UTF-8 (the documents are JSON).
pub struct SqlDocuments(pub SqlStorage);

// SAFETY: this build runs on one thread of a single-threaded wasm
// runtime, so the storage handle is never shared across threads.
unsafe impl Send for SqlDocuments {}
unsafe impl Sync for SqlDocuments {}

#[derive(Deserialize)]
struct Document {
    body: String,
    revision: i64,
}

impl SqlDocuments {
    fn row(&self, path: &Path) -> io::Result<Option<Document>> {
        let rows: Vec<Document> = self
            .0
            .exec(
                "SELECT body, revision FROM documents WHERE path = ?",
                vec![path.to_string_lossy().into_owned().into()],
            )
            .and_then(|cursor| cursor.to_array())
            .map_err(|e| io::Error::other(e.to_string()))?;
        Ok(rows.into_iter().next())
    }
}

impl DocumentStore for SqlDocuments {
    fn read(&self, path: &Path) -> io::Result<Vec<u8>> {
        self.row(path)?
            .map(|row| row.body.into_bytes())
            .ok_or_else(|| persistence::not_found(path))
    }

    fn write(&self, path: &Path, bytes: &[u8], _mode: WriteMode) -> io::Result<()> {
        let body = std::str::from_utf8(bytes)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        self.0
            .exec(
                "INSERT INTO documents (path, body, revision) VALUES (?, ?, 1) \
                 ON CONFLICT(path) DO UPDATE SET body = excluded.body, revision = documents.revision + 1",
                vec![path.to_string_lossy().into_owned().into(), body.into()],
            )
            .map_err(|e| io::Error::other(e.to_string()))?;
        Ok(())
    }

    fn remove(&self, path: &Path) -> io::Result<()> {
        if self.row(path)?.is_none() {
            return Err(persistence::not_found(path));
        }
        self.0
            .exec(
                "DELETE FROM documents WHERE path = ?",
                vec![path.to_string_lossy().into_owned().into()],
            )
            .map_err(|e| io::Error::other(e.to_string()))?;
        Ok(())
    }

    fn create_dir_all(&self, _path: &Path) -> io::Result<()> {
        Ok(())
    }

    fn revision(&self, path: &Path) -> Option<Revision> {
        self.row(path)
            .ok()
            .flatten()
            .map(|row| Revision(row.revision.to_string()))
    }
}

/// Moves auth records kept in `kv` by earlier versions into their document, once.
pub fn migrate_auth_records(
    sql: &SqlStorage,
    documents: &SqlDocuments,
    path: &Path,
) -> io::Result<()> {
    let legacy = get(sql, "auth-records").map_err(|e| io::Error::other(e.to_string()))?;
    if let Some(records) = legacy {
        if documents.read_optional(path)?.is_none() {
            documents.write(path, records.as_bytes(), WriteMode::Replace)?;
        }
        sql.exec("DELETE FROM kv WHERE key = 'auth-records'", None)
            .map_err(|e| io::Error::other(e.to_string()))?;
    }
    Ok(())
}
