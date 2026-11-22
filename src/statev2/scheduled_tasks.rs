//! The scheduled_tasks table in the pinned reference database. Run bookkeeping
//! and webhook credentials share the task row. Only public task fields leave the server.
use crate::asset_grant::encode_component;
use rusqlite::{params, Connection, Row, TransactionBehavior};
use serde_json::{json, Value};
use std::path::Path;

pub(crate) struct Database {
    conn: Connection,
    webhook_denial: Option<String>,
}

impl Database {
    pub fn open(dir: &Path, webhook_denial: Option<String>) -> anyhow::Result<Self> {
        let conn = Connection::open_with_flags(
            dir.join(super::FILE_NAME),
            rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE,
        )?;
        super::configure(&conn, false)?;
        let mut db = Self {
            conn,
            webhook_denial,
        };
        db.import(dir)?;
        Ok(db)
    }

    // Rust used to write an array alongside SQLite. Retire it after the
    // transaction commits, so a later restart cannot resurrect a deleted task.
    fn import(&mut self, dir: &Path) -> anyhow::Result<()> {
        let path = dir.join("scheduled-tasks.json");
        let bytes = match std::fs::read(&path) {
            Ok(bytes) => bytes,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(e) => return Err(e.into()),
        };
        let tasks: Vec<Value> = serde_json::from_slice(&bytes)?;
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        for task in tasks {
            let id = task["id"]
                .as_str()
                .ok_or_else(|| anyhow::anyhow!("Scheduled task has no id"))?;
            if !tx.query_row(
                "SELECT EXISTS(SELECT 1 FROM scheduled_tasks WHERE task_id=?1)",
                [id],
                |r| r.get::<_, bool>(0),
            )? {
                write(&tx, &task)?;
            }
        }
        tx.commit()?;
        std::fs::rename(&path, dir.join("scheduled-tasks.imported.json"))?;
        Ok(())
    }

    pub fn list(&self) -> anyhow::Result<Vec<Value>> {
        let mut stmt = self
            .conn
            .prepare("SELECT * FROM scheduled_tasks ORDER BY created_at DESC, task_id ASC")?;
        let mut rows = stmt.query([])?;
        let mut tasks = Vec::new();
        while let Some(row) = rows.next()? {
            tasks.push(self.decode(row)?);
        }
        Ok(tasks)
    }

    pub fn get(&self, id: &str) -> anyhow::Result<Option<Value>> {
        let mut stmt = self
            .conn
            .prepare("SELECT * FROM scheduled_tasks WHERE task_id=?1")?;
        let mut rows = stmt.query([id])?;
        rows.next()?.map(|row| self.decode(row)).transpose()
    }

    fn decode(&self, row: &Row<'_>) -> anyhow::Result<Value> {
        let text = |key| row.get::<_, Option<String>>(key);
        let field = |key| -> anyhow::Result<Value> {
            Ok(serde_json::from_str(&row.get::<_, String>(key)?)?)
        };
        let id: String = row.get("task_id")?;
        let mut task = json!({
            "id":id, "title":text("title")?, "prompt":text("prompt")?, "enabled":row.get::<_,i64>("enabled")? == 1,
            "schedule":field("schedule_json")?, "projectId":text("project_id")?, "threadId":text("thread_id")?,
            "threadMode":text("thread_mode")?, "workspaceStrategy":field("workspace_strategy_json")?,
            "modelSelection":field("model_selection_json")?, "runtimeMode":text("runtime_mode")?,
            "interactionMode":text("interaction_mode")?, "createdBy":text("created_by")?, "creationSource":text("creation_source")?,
            "createdAt":text("created_at")?, "updatedAt":text("updated_at")?, "nextRunAt":text("next_run_at")?,
            "lastRunAt":text("last_run_at")?, "lastRunStatus":text("last_run_status")?, "lastRunError":text("last_run_error")?,
            "runCount":row.get::<_,i64>("run_count")?
        });
        if task["schedule"]["type"] == "webhook" {
            if let Some(token) = text("webhook_token")? {
                let secret = text("webhook_secret")?;
                task["webhook"] = json!({"path":format!("/api/hooks/{}/{token}",encode_component(&id)), "url":null, "hasSecret":secret.is_some()});
                task["_webhookToken"] = token.into();
                task["_webhookSecret"] = json!(secret);
            }
            if let Some(message) = &self.webhook_denial {
                task["blockedByPolicy"] = json!(message);
            }
        }
        Ok(task)
    }

    pub fn save(&mut self, task: &Value) -> anyhow::Result<()> {
        write(&self.conn, task)?;
        Ok(())
    }

    /// Claims one scheduled occurrence. The conditional update is important
    /// because both the control-plane and a restarted server can observe the
    /// same due row before either has dispatched it.
    pub fn claim_due(&mut self, id: &str, now: &str) -> anyhow::Result<Option<Value>> {
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let changed = tx.execute(
            "UPDATE scheduled_tasks
             SET last_run_at=?2,last_run_status='running',last_run_error=NULL,updated_at=?2
             WHERE task_id=?1 AND enabled=1 AND next_run_at IS NOT NULL
               AND next_run_at <= ?2 AND last_run_status <> 'running'",
            params![id, now],
        )?;
        if changed == 0 {
            tx.commit()?;
            return Ok(None);
        }
        tx.commit()?;
        self.get(id)
    }

    /// Claims an explicit manual run, even when the task is disabled. Webhook
    /// tasks are rejected by their callers before reaching this method.
    pub fn claim_manual(&mut self, id: &str, now: &str) -> anyhow::Result<Option<Value>> {
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let changed = tx.execute(
            "UPDATE scheduled_tasks
             SET last_run_at=?2,last_run_status='running',last_run_error=NULL,updated_at=?2
             WHERE task_id=?1 AND last_run_status <> 'running'",
            params![id, now],
        )?;
        if changed == 0 {
            tx.commit()?;
            return Ok(None);
        }
        tx.commit()?;
        self.get(id)
    }

    /// Records the outcome of a claimed dispatch. The timestamp guard prevents
    /// a late provider failure from overwriting a newer run or a recreated row.
    pub fn complete(
        &mut self,
        id: &str,
        started_at: &str,
        completed_at: &str,
        status: &str,
        error: Option<&str>,
        next_run_at: Option<&str>,
    ) -> anyhow::Result<bool> {
        let changed = self.conn.execute(
            "UPDATE scheduled_tasks
             SET updated_at=?3,next_run_at=?4,last_run_status=?5,last_run_error=?6,
                 run_count=run_count+1
             WHERE task_id=?1 AND last_run_status='running' AND last_run_at=?2",
            params![id, started_at, completed_at, next_run_at, status, error],
        )?;
        Ok(changed != 0)
    }

    pub fn record_failure(
        &mut self,
        id: &str,
        started_at: &str,
        error: &str,
    ) -> anyhow::Result<bool> {
        let changed = self.conn.execute(
            "UPDATE scheduled_tasks
             SET last_run_status='failed',last_run_error=?3
             WHERE task_id=?1 AND last_run_status='succeeded' AND last_run_at=?2",
            params![id, started_at, error],
        )?;
        Ok(changed != 0)
    }

    pub fn reschedule_missed(
        &mut self,
        id: &str,
        started_at: &str,
        updated_at: &str,
        error: &str,
        next_run_at: Option<&str>,
    ) -> anyhow::Result<bool> {
        let changed = self.conn.execute(
            "UPDATE scheduled_tasks
             SET updated_at=?3,next_run_at=?4,last_run_status='failed',last_run_error=?5
             WHERE task_id=?1 AND last_run_status='running' AND last_run_at=?2",
            params![id, started_at, updated_at, next_run_at, error],
        )?;
        Ok(changed != 0)
    }

    pub fn delete(&mut self, id: &str) -> anyhow::Result<()> {
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        tx.execute(
            "DELETE FROM scheduled_task_webhook_deliveries WHERE task_id=?1",
            [id],
        )?;
        tx.execute("DELETE FROM scheduled_tasks WHERE task_id=?1", [id])?;
        tx.commit()?;
        Ok(())
    }
}

fn write(conn: &Connection, task: &Value) -> rusqlite::Result<()> {
    let text = |key: &str| task[key].as_str();
    let mode = text("threadMode").unwrap_or(if task["threadId"].is_null() {
        "new_thread"
    } else {
        "continue"
    });
    conn.execute("INSERT INTO scheduled_tasks (
        task_id,title,prompt,enabled,schedule_json,project_id,thread_id,thread_mode,
        workspace_strategy_json,model_selection_json,runtime_mode,interaction_mode,created_by,creation_source,
        created_at,updated_at,next_run_at,last_run_at,last_run_status,last_run_error,run_count,webhook_token,webhook_secret)
        VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?19,?20,?21,?22,?23)
        ON CONFLICT(task_id) DO UPDATE SET
        title=excluded.title, prompt=excluded.prompt, enabled=excluded.enabled, schedule_json=excluded.schedule_json,
        project_id=excluded.project_id, thread_id=excluded.thread_id, thread_mode=excluded.thread_mode,
        workspace_strategy_json=excluded.workspace_strategy_json, model_selection_json=excluded.model_selection_json,
        runtime_mode=excluded.runtime_mode, interaction_mode=excluded.interaction_mode, creation_source=excluded.creation_source,
        updated_at=excluded.updated_at, next_run_at=excluded.next_run_at, last_run_at=excluded.last_run_at,
        last_run_status=excluded.last_run_status,last_run_error=excluded.last_run_error,run_count=excluded.run_count,
        webhook_token=excluded.webhook_token,webhook_secret=excluded.webhook_secret",
        params![text("id"),text("title"),text("prompt"),task["enabled"].as_bool(),task["schedule"].to_string(),
            text("projectId"),text("threadId"),mode,task["workspaceStrategy"].to_string(),task["modelSelection"].to_string(),
            text("runtimeMode"),text("interactionMode"),text("createdBy"),text("creationSource"),text("createdAt"),text("updatedAt"),
            text("nextRunAt"),text("lastRunAt"),text("lastRunStatus"),text("lastRunError"),task["runCount"].as_i64(),
            text("_webhookToken"),text("_webhookSecret")])?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn task(id: &str, created: &str) -> Value {
        json!({"id":id,"title":id,"prompt":"fixture","enabled":false,"schedule":{"type":"interval","everyMs":60000},
               "projectId":"p","threadId":null,"threadMode":"fresh_session","workspaceStrategy":{"type":"root"},
               "modelSelection":{"instanceId":"codex","model":"fixture"},"runtimeMode":"auto","interactionMode":"default",
               "createdBy":"user","creationSource":"web","createdAt":created,"updatedAt":created,
               "nextRunAt":null,"lastRunAt":null,"lastRunStatus":"never","lastRunError":null,"runCount":0})
    }

    #[test]
    fn legacy_import_keeps_sqlite_rows_and_does_not_resurrect_deleted_tasks() {
        let dir = tempfile::tempdir().unwrap();
        let older = "2026-01-01T01:00:00.000Z";
        let newer = "2026-01-02T01:00:00.000Z";
        let conn = super::super::open(dir.path()).unwrap();
        let original = task("a", older);
        write(&conn, &original).unwrap();
        let mut legacy = task("c", newer);
        legacy.as_object_mut().unwrap().remove("threadMode");
        legacy["threadId"] = json!("bound");
        let path = dir.path().join("scheduled-tasks.json");
        std::fs::write(
            &path,
            serde_json::to_vec(&[task("a", newer), legacy, task("b", newer)]).unwrap(),
        )
        .unwrap();
        let mut db = Database::open(dir.path(), None).unwrap();
        assert_eq!(db.get("a").unwrap().unwrap(), original);
        assert_eq!(db.get("c").unwrap().unwrap()["threadMode"], "continue");
        assert_eq!(
            db.list()
                .unwrap()
                .iter()
                .map(|t| t["id"].as_str().unwrap())
                .collect::<Vec<_>>(),
            ["b", "c", "a"]
        );
        db.delete("c").unwrap();
        drop(db);
        assert!(Database::open(dir.path(), None)
            .unwrap()
            .get("c")
            .unwrap()
            .is_none());
        assert!(dir.path().join("scheduled-tasks.imported.json").exists());
    }

    #[test]
    fn due_claim_and_completion_are_idempotent() {
        let dir = tempfile::tempdir().unwrap();
        let _ = super::super::open(dir.path()).unwrap();
        let mut db = Database::open(dir.path(), None).unwrap();
        let mut task = task("due", "2026-01-01T00:00:00.000Z");
        task["enabled"] = json!(true);
        task["nextRunAt"] = json!("2026-01-01T00:01:00.000Z");
        db.save(&task).unwrap();
        let claimed = db
            .claim_due("due", "2026-01-01T00:02:00.000Z")
            .unwrap()
            .unwrap();
        assert_eq!(claimed["lastRunStatus"], "running");
        assert!(db
            .claim_due("due", "2026-01-01T00:02:00.000Z")
            .unwrap()
            .is_none());
        assert!(db
            .complete(
                "due",
                "2026-01-01T00:02:00.000Z",
                "2026-01-01T00:02:01.000Z",
                "succeeded",
                None,
                Some("2026-01-01T00:03:01.000Z"),
            )
            .unwrap());
        let stored = db.get("due").unwrap().unwrap();
        assert_eq!(stored["lastRunStatus"], "succeeded");
        assert_eq!(stored["runCount"], 1);
        assert_eq!(stored["nextRunAt"], "2026-01-01T00:03:01.000Z");
    }
}
