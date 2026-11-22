use super::super::vcs::{lexical_path, read};
use crate::AppState;
use serde_json::{json, Value};
use std::path::{Path, PathBuf};

async fn resolved(path: &Path) -> Option<PathBuf> {
    let absolute = lexical_path(&std::path::absolute(path).ok()?);
    match tokio::fs::canonicalize(path).await {
        Ok(real) => Some(real),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Some(absolute),
        Err(_) => None,
    }
}

pub(crate) async fn check(state: &AppState, method: &str, cwd: &str) -> Result<(), Value> {
    let subject = match method {
        "review.getBranchCommits" => "commit list",
        "review.getDiffPreview" => "diff preview",
        "review.getDiffFileContents" => "diff file contents",
        _ => return Ok(()),
    };
    let error = || json!({"_tag":"VcsRepositoryDetectionError","operation":method.replacen("review.","ReviewService.",1),"cwd":cwd,"detail":format!("Review {subject} cwd must stay within the configured workspace root.")});
    let candidate = resolved(Path::new(cwd)).await.ok_or_else(error)?;
    if std::env::current_dir().is_ok_and(|root| candidate.starts_with(root)) {
        return Ok(());
    }

    let settings = state.settings.internal().unwrap_or_default();
    let mut managed = vec![state.paths.worktrees.clone()];
    for setting in settings["worktreesDirectory"].as_str().into_iter().chain(
        settings["previousWorktreesDirectories"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(Value::as_str),
    ) {
        let path = PathBuf::from(crate::provider_status::expand_home(setting));
        if path.is_absolute() {
            let path = lexical_path(&path);
            if path.parent().is_some() {
                managed.push(path);
            }
        }
    }
    let (projects, threads) = {
        let store = state.store.read().await;
        let projects: Vec<_> = store
            .projects
            .values()
            .map(|p| PathBuf::from(&p.path))
            .collect();
        let threads: Vec<_> = store
            .threads
            .values()
            .filter_map(|thread| {
                let path = PathBuf::from(thread.row["worktreePath"].as_str()?);
                let project = store.projects.get(&thread.project_id)?;
                path.is_absolute().then(|| (path, project.path.clone()))
            })
            .collect();
        (projects, threads)
    };
    for path in managed.iter().chain(&projects) {
        if resolved(path)
            .await
            .is_some_and(|real| real.parent().is_some() && candidate.starts_with(real))
        {
            return Ok(());
        }
    }
    for (path, project) in threads {
        let Some(real) = resolved(&path).await else {
            continue;
        };
        if real.parent().is_none() || !candidate.starts_with(&real) {
            continue;
        }
        if managed
            .iter()
            .any(|root| lexical_path(&path).starts_with(root))
        {
            return Ok(());
        }
        let listed = read(&project, &["worktree", "list", "--porcelain", "-z"])
            .await
            .unwrap_or_default();
        for entry in listed
            .split_terminator('\0')
            .filter_map(|field| field.strip_prefix("worktree "))
        {
            if resolved(Path::new(entry)).await.as_ref() == Some(&real) {
                return Ok(());
            }
        }
    }
    Err(error())
}
