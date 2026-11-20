//! Durable Object SQLite storage for the control plane: the orchestration
//! event log and a small key/value table (environment id, auth records).
use serde::Deserialize;
use t3code_server::{event_log::EventLog, ServerEvent};
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
