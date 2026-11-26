//! Client RPCs for inspecting and approving the exact worktree layout command a person saw.
use crate::{rpc_protocol::Reply, AppState};
use rusqlite::{params, Connection, OpenFlags};
use serde::Deserialize;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};

#[derive(Deserialize)]
struct Approval {
    kind: Kind,
    // A missing field is invalid; null explicitly withdraws the approval.
    command: Value,
}

#[derive(Deserialize)]
#[serde(rename_all = "lowercase")]
enum Kind {
    Create,
    Remove,
}

impl Kind {
    fn name(&self) -> &'static str {
        match self {
            Self::Create => "create",
            Self::Remove => "remove",
        }
    }
}

fn error(operation: &str, detail: &str) -> Reply {
    Reply::Failure(
        json!({"_tag":"Fail","error":{"_tag":"TaskFolderError","operation":operation,"detail":detail}}),
    )
}

fn commands(
    state_dir: &Path,
    id: &str,
    project: Option<PathBuf>,
    approvals: Vec<Approval>,
) -> rusqlite::Result<Reply> {
    let mut conn = Connection::open_with_flags(
        state_dir.join(crate::statev2::FILE_NAME),
        OpenFlags::SQLITE_OPEN_READ_WRITE,
    )?;
    crate::statev2::configure(&conn, false)?;
    if !approvals.is_empty() {
        let tx = conn.transaction()?;
        for approval in approvals {
            if let Some(command) = approval.command.as_str() {
                tx.execute(
                    "INSERT INTO layout_command_approvals (project_id,kind,command_hash,approved_at)
                     VALUES (?1,?2,?3,?4) ON CONFLICT(project_id,kind) DO UPDATE SET
                     command_hash=excluded.command_hash, approved_at=excluded.approved_at",
                    params![id, approval.kind.name(), hex::encode(Sha256::digest(command.as_bytes())),
                            crate::auth_service::iso(crate::platform::now_ms())],
                )?;
            } else {
                tx.execute(
                    "DELETE FROM layout_command_approvals WHERE project_id=?1 AND kind=?2",
                    params![id, approval.kind.name()],
                )?;
            }
        }
        tx.commit()?;
    }
    let Some(root) = project else {
        return Ok(error("read", &format!("Project {id} was not found.")));
    };
    let file = std::fs::read_to_string(root.join("t3.json"))
        .ok()
        .and_then(|text| crate::local_services::vcs::worktree::project_file(&text));
    let mut rows = Vec::new();
    if let Some(layout) = file.as_ref().and_then(|file| file.get("worktreeLayout")) {
        for kind in ["create", "remove"] {
            if let Some(command) = layout[kind].as_str().map(str::trim) {
                let approved: bool = conn.query_row(
                    "SELECT EXISTS(SELECT 1 FROM layout_command_approvals
                     WHERE project_id=?1 AND kind=?2 AND command_hash=?3)",
                    params![id, kind, hex::encode(Sha256::digest(command.as_bytes()))],
                    |r| r.get(0),
                )?;
                rows.push(json!({"kind":kind,"command":command,"approved":approved}));
            }
        }
    }
    Ok(Reply::Success(json!({"commands": rows})))
}

pub(super) async fn dispatch(state: &AppState, input: &Value, write: bool) -> Reply {
    let Some(id) = input["projectId"].as_str() else {
        return Reply::defect("Expected string\n  at [\"projectId\"]".into());
    };
    let approvals = if write {
        let Ok(values) = serde_json::from_value::<Vec<Approval>>(input["approvals"].clone()) else {
            return Reply::defect(
                "Invalid worktree layout command approvals\n  at [\"approvals\"]".into(),
            );
        };
        if values.iter().any(|v| {
            !v.command.is_null()
                && !v
                    .command
                    .as_str()
                    .is_some_and(|s| s.encode_utf16().count() <= 4096)
        }) {
            return Reply::defect(
                "Invalid worktree layout command text\n  at [\"approvals\"]".into(),
            );
        }
        values
    } else {
        Vec::new()
    };
    let project = state
        .store
        .read()
        .await
        .projects
        .get(id)
        .map(|p| PathBuf::from(&p.path));
    let (state_dir, id) = (state.paths.state_dir.clone(), id.to_owned());
    match tokio::task::spawn_blocking(move || commands(&state_dir, &id, project, approvals)).await {
        Ok(Ok(reply)) => reply,
        _ => error(
            "approve",
            "Could not read the worktree layout command approvals.",
        ),
    }
}
