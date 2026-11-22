pub(crate) use crate::store::wire_project as wire;
use crate::{store::iso, AppState, Event, Project};
use axum::{
    extract::State,
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use serde_json::{json, Value};
use std::{path::PathBuf, sync::Arc};

pub async fn snapshot(State(s): State<Arc<AppState>>) -> Json<Value> {
    let store = s.store.read().await;
    Json(
        json!({"projects":store.project_rows.values().map(crate::repository_identity::with_identity).collect::<Vec<_>>(),"updatedAt":iso(time::OffsetDateTime::now_utc())}),
    )
}

pub(super) async fn mutate(state: &AppState, input: &Value) -> Result<Value, Value> {
    let command = input["commandId"].as_str().unwrap_or_default();
    let id = input["projectId"].as_str().unwrap_or_default();
    let kind = input["type"].as_str().unwrap_or_default();
    let error =
        |msg: &str| json!({"_tag":"ProjectMutationError","commandId":command,"message":msg});
    if command.trim().is_empty()
        || id.trim().is_empty()
        || !matches!(kind, "project.create" | "project.update" | "project.delete")
    {
        return Err(error("Invalid project mutation."));
    }
    let _guard = state.project_mutation_lock.lock().await;
    let store = state.store.read().await;
    if let Some((project_id, op)) = store.project_receipts.get(command) {
        if project_id != id || op != kind {
            return Err(error("Failed to mutate project."));
        }
        return Ok(store.project_rows[id].clone());
    }
    let mut record = store.projects.get(id).cloned();
    let mut row = store.project_rows.get(id).cloned();
    if (kind == "project.create") == row.is_some() || (kind != "project.create" && record.is_none())
    {
        return Err(error("Failed to mutate project."));
    }
    let removed_threads: Vec<_> = store
        .threads
        .values()
        .filter(|t| t.project_id == id && kind == "project.delete")
        .map(|t| t.id.clone())
        .collect();
    if !removed_threads.is_empty() && input["force"] != true {
        return Err(error(&format!("Project {id} is not empty.")));
    }
    drop(store);
    let at = iso(time::OffsetDateTime::now_utc());
    let mut workspace = None;
    if kind == "project.create" || input.get("workspaceRoot").is_some() {
        let raw = input["workspaceRoot"]
            .as_str()
            .filter(|s| !s.trim().is_empty())
            .ok_or_else(|| error("A workspace root is required."))?;
        let dir = PathBuf::from(raw.trim());
        if kind == "project.create" && input["createWorkspaceRootIfMissing"] == true {
            state
                .workspace
                .create_dir_all(&dir)
                .await
                .map_err(|_| error("Failed to mutate project."))?;
        }
        let dir = state
            .workspace
            .canonicalize(&dir)
            .await
            .map_err(|_| error("Failed to mutate project."))?;
        if !state.workspace.is_dir(&dir).await {
            return Err(error("Failed to mutate project."));
        }
        let path = dir.to_string_lossy().into_owned();
        if state
            .store
            .read()
            .await
            .projects
            .values()
            .any(|p| p.id != id && p.path == path)
        {
            return Err(error("Failed to mutate project."));
        }
        workspace = Some(path);
    }
    if kind == "project.create" {
        let name = input["title"]
            .as_str()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .ok_or_else(|| error("A title is required."))?;
        let p = Project {
            id: id.into(),
            name: name.into(),
            path: workspace.as_ref().unwrap().clone(),
            created_at: crate::now(),
        };
        let mut r = wire(&p);
        r["createdAt"] = json!(at);
        row = Some(r);
        record = Some(p);
    }
    let mut p = record.ok_or_else(|| error("Failed to mutate project."))?;
    let mut row = row.ok_or_else(|| error("Failed to mutate project."))?;
    if let Some(path) = workspace {
        p.path = path;
        row["workspaceRoot"] = json!(p.path);
    }
    if let Some(title) = input.get("title") {
        let title = title
            .as_str()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .ok_or_else(|| error("A title is required."))?;
        p.name = title.into();
        row["title"] = json!(title);
        if kind == "project.update" {
            row["hasCustomTitle"] = json!(true);
        }
    }
    if kind == "project.create" || kind == "project.update" {
        for key in [
            "defaultModelSelection",
            "defaultThreadEnvMode",
            "autoPull",
            "projectIcon",
            "scripts",
            "faviconPath",
        ] {
            if let Some(v) = input.get(key) {
                row[key] = v.clone();
            }
        }
        for (key, field) in [("pinned", "pinnedAt"), ("archived", "archivedAt")] {
            if let Some(v) = input[key].as_bool() {
                row[field] = if v { json!(at) } else { Value::Null }
            }
        }
    } else {
        row["deletedAt"] = json!(at);
    }
    row["updatedAt"] = json!(at);
    state
        .commit(Event::ProjectCommitted {
            project: p,
            row: row.clone(),
            command_type: kind.to_owned(),
            command_id: command.to_owned(),
            removed_threads,
        })
        .await
        .map_err(|_| error("Failed to mutate project."))?;
    Ok(row)
}

pub async fn mutate_http(
    State(state): State<Arc<AppState>>,
    h: HeaderMap,
    Json(body): Json<Value>,
) -> Response {
    match mutate(&state,&body).await {
        Ok(row)=>Json(row).into_response(),
        Err(_)=>(StatusCode::BAD_REQUEST,Json(json!({"_tag":"EnvironmentRequestInvalidError","code":"invalid_request","reason":"invalid_command","traceId":crate::trace_id(&h)}))).into_response(),
    }
}
