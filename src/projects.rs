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
        json!({"projects":store.project_rows.values().filter(|row| row["deletedAt"].is_null()).map(crate::repository_identity::with_identity).collect::<Vec<_>>(),"updatedAt":iso(time::OffsetDateTime::now_utc())}),
    )
}

pub(crate) use crate::mcp_oauth::ProjectRefusal as Refusal;

pub(super) async fn mutate(state: &AppState, input: &Value) -> Result<Value, Value> {
    mutate_checked(state, input)
        .await
        .map_err(|(error, _)| error)
}

/// The script ids a project may gain: 1-24 lowercase letters, digits or
/// hyphens, starting with a letter or digit (`SCRIPT_RUN_COMMAND_PATTERN`).
fn valid_script_id(id: &str) -> bool {
    !id.is_empty()
        && id.encode_utf16().count() <= 24
        && id
            .chars()
            .next()
            .is_some_and(|c| c.is_ascii_lowercase() || c.is_ascii_digit())
        && id
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-')
}

/// About how many characters a reader sees in `text` (`Intl.Segmenter`'s
/// graphemes): marks, variation selectors, skin tones and joined emoji do not
/// start a new one.
fn graphemes(text: &str) -> usize {
    let mut count = 0;
    let mut joined = false;
    for c in text.chars() {
        let extends = matches!(c as u32,
            0x0300..=0x036f | 0x1ab0..=0x1aff | 0x20d0..=0x20ff | 0xfe00..=0xfe0f
            | 0x1f3fb..=0x1f3ff | 0xe0020..=0xe007f);
        if c == '\u{200d}' {
            joined = true;
            continue;
        }
        if !extends && !joined {
            count += 1;
        }
        joined = false;
    }
    count
}

/// [`mutate`], with why a refused mutation was refused.
pub(crate) async fn mutate_checked(
    state: &AppState,
    input: &Value,
) -> Result<Value, (Value, Refusal)> {
    let command = input["commandId"].as_str().unwrap_or_default();
    let id = input["projectId"].as_str().unwrap_or_default();
    let kind = input["type"].as_str().unwrap_or_default();
    let fail = |msg: &str, refusal: Refusal| {
        (
            json!({"_tag":"ProjectMutationError","commandId":command,"message":msg}),
            refusal,
        )
    };
    let error = |msg: &str| fail(msg, Refusal::Operation);
    if command.trim().is_empty()
        || id.trim().is_empty()
        || !matches!(kind, "project.create" | "project.update" | "project.delete")
    {
        return Err(error("Invalid project mutation."));
    }
    let guard = state.project_mutation_lock.lock().await;
    let store = state.store.read().await;
    if let Some((project_id, op)) = store.project_receipts.get(command) {
        if project_id != id || op != kind {
            return Err(error("Failed to mutate project."));
        }
        return Ok(store.project_rows[id].clone());
    }
    let mut record = store.projects.get(id).cloned();
    let mut row = store.project_rows.get(id).cloned();
    if kind == "project.create" && row.is_some() {
        return Err(fail(
            "Failed to mutate project.",
            Refusal::Invalid(format!(
                "Project '{id}' already exists and cannot be created twice."
            )),
        ));
    }
    if kind != "project.create" && record.is_none() {
        return Err(fail("Failed to mutate project.", Refusal::NotFound));
    }
    if kind == "project.update" {
        let monogram = &input["projectIcon"];
        if monogram["kind"] == "monogram"
            && graphemes(monogram["text"].as_str().unwrap_or_default()) > 2
        {
            return Err(fail(
                "Failed to mutate project.",
                Refusal::Invalid("Project monograms must contain at most two characters.".into()),
            ));
        }
        let existing: Vec<&Value> = row
            .as_ref()
            .and_then(|r| r["scripts"].as_array())
            .map(|scripts| scripts.iter().map(|s| &s["id"]).collect())
            .unwrap_or_default();
        for script in input["scripts"].as_array().into_iter().flatten() {
            let script_id = script["id"].as_str().unwrap_or_default();
            if !existing.contains(&&script["id"]) && !valid_script_id(script_id) {
                return Err(fail(
                    "Failed to mutate project.",
                    Refusal::Invalid(format!(
                        "Script IDs must be 1-24 lowercase letters, digits or hyphens, starting with a letter or digit (got {} characters).",
                        script_id.encode_utf16().count()
                    )),
                ));
            }
        }
    }
    let removed_threads: Vec<_> = store
        .threads
        .values()
        .filter(|t| t.project_id == id && kind == "project.delete")
        .map(|t| t.id.clone())
        .collect();
    if !removed_threads.is_empty() && input["force"] != true {
        return Err(fail(
            &format!("Project {id} is not empty."),
            Refusal::NotEmpty,
        ));
    }
    drop(store);
    let now = time::OffsetDateTime::now_utc();
    let at = iso(now);
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
            return Err(fail("Failed to mutate project.", Refusal::Conflict));
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
            // The row's `createdAt`, in milliseconds.
            created_at: (now.unix_timestamp_nanos() / 1_000_000).max(0) as u64,
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
        // Repeating a pin or archive keeps the original time.
        for (key, field) in [("pinned", "pinnedAt"), ("archived", "archivedAt")] {
            if let Some(v) = input[key].as_bool() {
                row[field] = match (v, row[field].is_null()) {
                    (true, true) => json!(at),
                    (true, false) => row[field].clone(),
                    (false, _) => Value::Null,
                }
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
    if state.settings.save_project_defaults(id, input).is_err() {
        if kind != "project.delete" {
            return Err(error("Failed to mutate project."));
        }
        tracing::warn!("Failed to clear settings overrides for a deleted project.");
    }
    drop(guard);
    // A deleted project's clone is no longer followed; an unfinished checkout is emptied.
    if kind == "project.delete" {
        state.project_clones.discard(id).await;
    }
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
