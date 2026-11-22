mod git;
mod inspect;
mod policy;

use crate::AppState;
use policy::{Eligibility, Rules};
use serde_json::{json, Value};
use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

struct Snapshot {
    live: Vec<Value>,
    deleted: Vec<Value>,
    projects: Vec<Value>,
}

fn absolute(path: &str) -> PathBuf {
    std::path::absolute(path).unwrap_or_else(|_| PathBuf::from(path))
}

fn inside(root: &Path, target: &Path) -> bool {
    let root = root.canonicalize().unwrap_or_else(|_| root.to_owned());
    let target = target.canonicalize().unwrap_or_else(|_| target.to_owned());
    target.starts_with(&root) && target != root
}

fn settings_roots(settings: &Value, default: &Path, home: &Path) -> Vec<PathBuf> {
    let mut roots = vec![default.to_owned()];
    for key in ["worktreesDirectory", "previousWorktreesDirectories"] {
        let values = if key == "worktreesDirectory" {
            settings[key]
                .as_str()
                .map(|s| vec![s.to_owned()])
                .unwrap_or_default()
        } else {
            settings[key]
                .as_array()
                .map(|a| {
                    a.iter()
                        .filter_map(Value::as_str)
                        .map(str::to_owned)
                        .collect()
                })
                .unwrap_or_default()
        };
        for value in values {
            let expanded = if value == "~" {
                home.to_owned()
            } else if let Some(rest) = value.strip_prefix("~/") {
                home.join(rest)
            } else {
                PathBuf::from(value)
            };
            if expanded.is_absolute() && expanded.parent().is_some() {
                roots.push(expanded);
            }
        }
    }
    roots.sort();
    roots.dedup();
    roots
}

fn project_path(projects: &[Value], id: &str) -> Option<PathBuf> {
    projects
        .iter()
        .find(|p| p["id"] == id)
        .and_then(|p| p["workspaceRoot"].as_str().map(PathBuf::from))
}

fn idle(thread: &Value, now: i64, settings: &Value) -> bool {
    if !thread["activeRunId"].is_null()
        || !thread["pendingRuntimeRequest"].is_null()
        || thread["hasBackgroundWork"] == true
        || !matches!(
            thread["status"].as_str(),
            Some("idle" | "completed" | "interrupted" | "failed" | "cancelled" | "rolled_back")
        )
    {
        return false;
    }
    if thread["status"] != "failed"
        && thread["latestUserMessageAt"]
            .as_str()
            .and_then(crate::peer_links::iso_ms)
            .is_some_and(|at| (at - now).abs() <= 120_000)
    {
        return false;
    }
    if thread["status"] == "failed"
        && thread["lastErrorClass"] == "usage_limit"
        && thread["archivedAt"].is_null()
        && thread["settledOverride"] != "settled"
    {
        if let Some(reset) = thread["usageLimitResetAt"]
            .as_str()
            .and_then(crate::peer_links::iso_ms)
        {
            let completed = thread["latestRunCompletedAt"]
                .as_str()
                .and_then(crate::peer_links::iso_ms)
                .or_else(|| {
                    thread["updatedAt"]
                        .as_str()
                        .and_then(crate::peer_links::iso_ms)
                })
                .unwrap_or(i64::MAX);
            if reset > completed
                && now - reset <= policy::DAY
                && (settings["autoResumeLimitedThreads"] == true
                    || (settings["snoozeLimitedThreads"] == true && reset > now))
            {
                return false;
            }
        }
    }
    true
}

fn terminal_blocks(state: &AppState, path: &Path) -> bool {
    let snapshot = state.local.terminals.metadata_snapshot();
    snapshot["terminals"]
        .as_array()
        .into_iter()
        .flatten()
        .any(|t| {
            matches!(t["status"].as_str(), Some("starting" | "running"))
                && (t["cwd"].as_str().is_some_and(|cwd| absolute(cwd) == path)
                    || t["worktreePath"]
                        .as_str()
                        .is_some_and(|cwd| absolute(cwd) == path))
        })
}

fn entry(
    thread: &Value,
    project: Option<&Path>,
    path: &Path,
    reason: &str,
    outcome: &str,
    bytes: Option<u64>,
) -> Value {
    json!({"kind":"worktree","outcome":outcome,"reason":reason,"path":path,"projectPath":project,
        "threadId":thread["id"],"threadTitle":thread["title"],"bytes":bytes,"files":null})
}

fn remove_candidate(
    _state: &AppState,
    path: &Path,
    rules: &Rules,
    force: bool,
) -> anyhow::Result<u64> {
    let bytes = inspect::measure(path, inspect::Scan::Size, inspect::deadline())
        .map_err(anyhow::Error::msg)?;
    if let Some(reason) = git::changes(path, rules)? {
        anyhow::bail!("{reason}");
    }
    git::remove(path, force)?;
    Ok(bytes)
}

fn worktree_candidates(
    state: &Arc<AppState>,
    snapshot: &Snapshot,
    settings: &Value,
    now: i64,
    entries: &mut Vec<Value>,
) {
    let home = std::env::var_os("HOME")
        .map(PathBuf::from)
        .unwrap_or_default();
    let roots = settings_roots(settings, &state.paths.worktrees, &home);
    let mut seen = HashSet::new();
    for thread in snapshot.live.iter().chain(snapshot.deleted.iter()) {
        let Some(raw) = thread["worktreePath"].as_str() else {
            continue;
        };
        let path = absolute(raw);
        let Ok(canonical) = path.canonicalize() else {
            continue;
        };
        if !seen.insert(canonical.clone()) {
            continue;
        }
        let deleted = thread["deletedAt"].is_string();
        let project = project_path(
            &snapshot.projects,
            thread["projectId"].as_str().unwrap_or_default(),
        );
        let project_root = project.as_deref();
        let Ok(rules) = Rules::resolve(settings, thread["projectId"].as_str().unwrap_or_default())
        else {
            continue;
        };
        let reason = if deleted && !rules.worktree_on_delete {
            "No rules apply"
        } else if !roots.iter().any(|root| inside(root, &canonical)) {
            "Outside the managed worktree folder"
        } else if !deleted && !idle(thread, now, settings) {
            "Thread is running or has pending work"
        } else if terminal_blocks(state, &canonical) {
            "Open terminal"
        } else {
            let activity = policy::activity(thread);
            let eligibility = Eligibility::new(thread, deleted, activity, None, &rules, now);
            if eligibility.removal.is_none() && !eligibility.unchanged && !rules.worktree_on_merge {
                entries.push(entry(
                    thread,
                    project_root,
                    &canonical,
                    &title_case(&eligibility.pending),
                    "kept",
                    None,
                ));
                continue;
            }
            let mut removal = eligibility.removal.clone();
            if removal.is_none() {
                match git::integration(&canonical, project_root, &rules, &eligibility) {
                    Ok(Some(value)) => {
                        removal = Some(value);
                    }
                    Ok(None) => {}
                    Err(error) => {
                        entries.push(entry(
                            thread,
                            project_root,
                            &canonical,
                            &error.to_string(),
                            "failed",
                            None,
                        ));
                        continue;
                    }
                }
            }
            if removal.is_none() {
                entries.push(entry(
                    thread,
                    project_root,
                    &canonical,
                    &title_case(&eligibility.pending),
                    "kept",
                    None,
                ));
                continue;
            }
            match remove_candidate(state, &canonical, &rules, false) {
                Ok(bytes) => {
                    entries.push(entry(
                        thread,
                        project_root,
                        &canonical,
                        &title_case(removal.as_deref().unwrap()),
                        "removed",
                        Some(bytes),
                    ));
                    continue;
                }
                Err(error) => {
                    entries.push(entry(
                        thread,
                        project_root,
                        &canonical,
                        &error.to_string(),
                        "failed",
                        None,
                    ));
                    continue;
                }
            }
        };
        entries.push(entry(
            thread,
            project_root,
            &canonical,
            reason,
            "kept",
            None,
        ));
    }
}

fn title_case(reason: &str) -> String {
    let mut chars = reason.chars();
    chars
        .next()
        .map(|c| c.to_uppercase().collect::<String>() + chars.as_str())
        .unwrap_or_default()
}

fn orphan_entries(
    state: &AppState,
    snapshot: &Snapshot,
    settings: &Value,
    entries: &mut Vec<Value>,
) {
    let home = std::env::var_os("HOME")
        .map(PathBuf::from)
        .unwrap_or_default();
    let roots = settings_roots(settings, &state.paths.worktrees, &home);
    let referenced: HashSet<PathBuf> = snapshot
        .live
        .iter()
        .chain(snapshot.deleted.iter())
        .filter_map(|t| t["worktreePath"].as_str().map(absolute))
        .collect();
    let projects: Vec<PathBuf> = snapshot
        .projects
        .iter()
        .filter_map(|p| p["workspaceRoot"].as_str().map(absolute))
        .collect();
    for root in roots {
        let Ok(dirs) = fs::read_dir(&root) else {
            continue;
        };
        for repo in dirs.flatten().map(|e| e.path()).filter(|p| p.is_dir()) {
            if projects.iter().any(|p| inside(p, &repo)) {
                continue;
            }
            let Ok(children) = fs::read_dir(&repo) else {
                continue;
            };
            for child in children.flatten().map(|e| e.path()).filter(|p| p.is_dir()) {
                let Ok(path) = child.canonicalize() else {
                    continue;
                };
                if referenced.iter().any(|p| p == &path || inside(&path, p))
                    || projects.iter().any(|p| p == &path || inside(&path, p))
                {
                    continue;
                }
                if path
                    .metadata()
                    .and_then(|m| m.modified())
                    .ok()
                    .and_then(|t| t.elapsed().ok())
                    .is_some_and(|age| age < std::time::Duration::from_secs(3600))
                {
                    continue;
                }
                let linked = git::linked(&path);
                let bytes = inspect::measure(&path, inspect::Scan::Size, inspect::deadline()).ok();
                entries.push(json!({"kind":"worktree","outcome":"kept","reason":if linked {"No thread uses this worktree"} else {"No thread uses this folder, and it isn't a git worktree"},"path":path,"projectPath":null,"threadId":null,"threadTitle":null,"bytes":bytes,"files":null}));
            }
        }
    }
}

pub(super) fn clean(state: Arc<AppState>, settings: Value, now: i64) -> Vec<Value> {
    let mut snapshot = Snapshot {
        live: vec![],
        deleted: vec![],
        projects: vec![],
    };
    if let Ok(store) = state.control.store.try_read() {
        snapshot.live = store.threads.values().map(|t| t.shell()).collect();
        snapshot.projects = store.project_rows.values().cloned().collect();
        for project in store.projects.values() {
            if !snapshot.projects.iter().any(|row| row["id"] == project.id) {
                snapshot
                    .projects
                    .push(json!({"id": project.id, "workspaceRoot": project.path}));
            }
        }
    }
    if let Ok(conn) = crate::statev2::open(&state.paths.state_dir) {
        if let Ok(projects) = crate::statev2::read::projects(&conn) {
            for project in projects {
                if !snapshot
                    .projects
                    .iter()
                    .any(|row| row["id"] == project["id"])
                {
                    snapshot.projects.push(project);
                }
            }
        }
        if let Ok(mut query) = conn.prepare("SELECT thread_id, payload_json, deleted_at FROM orchestration_v2_projection_threads WHERE deleted_at IS NOT NULL") {
            if let Ok(rows) = query.query_map([], |row| {
                let id: String = row.get(0)?;
                let text: String = row.get(1)?;
                let deleted: String = row.get(2)?;
                Ok((id, text, deleted))
            }) {
                for row in rows.flatten() {
                    if let Ok(mut thread) = serde_json::from_str::<Value>(&row.1) {
                        thread["id"] = Value::String(row.0);
                        thread["deletedAt"] = Value::String(row.2);
                        snapshot.deleted.push(thread);
                    }
                }
            }
        }
    }
    // Prefer durable deletion tombstones when the in-memory store still has a stale shell.
    let deleted_ids: HashSet<&Value> = snapshot
        .deleted
        .iter()
        .map(|thread| &thread["id"])
        .collect();
    snapshot.live.retain(|thread| {
        !deleted_ids.contains(&thread["id"])
            && !snapshot.deleted.iter().any(|deleted| {
                thread["worktreePath"].is_string()
                    && thread["worktreePath"] == deleted["worktreePath"]
            })
    });
    let mut entries = Vec::new();
    worktree_candidates(&state, &snapshot, &settings, now, &mut entries);
    orphan_entries(&state, &snapshot, &settings, &mut entries);
    entries
}
