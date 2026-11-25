//! The worktree of a delegated child with `isolation: "worktree"`, prepared as the reference's
//! `ThreadLaunchService.prepareRun` prepares a run that waits for its workspace
//! (`workspacePreparation`): a checkout of the parent's commit on a temporary `t3/<hex>` branch,
//! recorded on the child thread, renamed to a generated name, then the project's setup script and
//! the run's release. The progress shows in the thread's worktree setup
//! (`subscribeWorktreeSetup`). A failure fails the run (`prepared-run.fail`) and removes a
//! worktree it never recorded; a run cancelled before its release has its worktree removed.
use crate::mcp_oauth::McpHost;
use crate::title_generation::{branch, runner};
use crate::AppState;
use serde_json::{json, Value};
use std::sync::Arc;
use std::time::Duration;

/// `BRANCH_NAME_TIMEOUT`: the agent waits for its branch name.
const BRANCH_NAME_TIMEOUT: Duration = Duration::from_secs(20);
const STAGES: [&str; 5] = ["fetch", "checkout", "submodules", "setup-script", "agent"];

/// What a preparing run asked for.
struct Preparation {
    project_id: String,
    root: String,
    base_ref: String,
    message: String,
    attachments: Vec<Value>,
    scripts: Vec<Value>,
}

async fn load(state: &AppState, thread_id: &str, run_id: &str) -> Option<Preparation> {
    let store = state.store.read().await;
    let thread = store.threads.get(thread_id)?;
    let run = thread
        .runs
        .iter()
        .find(|run| run.wire_id() == run_id && run.status == "preparing")?;
    let row = thread.stored_row();
    let strategy = &row["runExtras"][run_id]["workspacePreparation"];
    if strategy["type"] != "worktree" {
        return None;
    }
    let project = store
        .project_rows
        .get(&thread.project_id)
        .filter(|row| row["deletedAt"].is_null())?;
    let message = run
        .user_message_id
        .as_ref()
        .and_then(|id| thread.messages.iter().find(|message| &message.id == id));
    Some(Preparation {
        project_id: thread.project_id.clone(),
        root: project["workspaceRoot"].as_str()?.to_owned(),
        base_ref: strategy["baseRef"].as_str()?.to_owned(),
        message: message.map(|m| m.content.clone()).unwrap_or_default(),
        attachments: message
            .and_then(|m| m.wire.as_deref())
            .and_then(|wire| wire["attachments"].as_array().cloned())
            .unwrap_or_default(),
        scripts: project["scripts"].as_array().cloned().unwrap_or_default(),
    })
}

/// Whether the run waits to start in the project's own checkout (`{type: "root"}`).
async fn waits_in_root(state: &AppState, thread_id: &str, run_id: &str) -> bool {
    let store = state.store.read().await;
    store.threads.get(thread_id).is_some_and(|thread| {
        thread
            .runs
            .iter()
            .any(|run| run.wire_id() == run_id && run.status == "preparing")
            && thread.stored_row()["runExtras"][run_id]["workspacePreparation"]["type"] == "root"
    })
}

fn message_of(error: &Value) -> String {
    error["detail"]
        .as_str()
        .or_else(|| error["message"].as_str())
        .unwrap_or("The workspace operation could not be completed.")
        .to_owned()
}

async fn vcs(state: &Arc<AppState>, method: &str, input: Value) -> Result<Value, String> {
    state
        .workspace_call(format!("vcs.{method}"), input)
        .await
        .map_err(|error| message_of(&error))
}

/// Whether `refName` names a commit in the repository at `cwd`.
async fn has_commit(state: &Arc<AppState>, cwd: &str, ref_name: &str) -> bool {
    vcs(
        state,
        "resolveCommit",
        json!({"cwd": cwd, "revision": ref_name}),
    )
    .await
    .is_ok()
}

async fn dispatch(state: &Arc<AppState>, command: Value) -> Result<u64, String> {
    McpHost::dispatch(state, command)
        .await
        .map_err(|error| message_of(&error))
}

/// The generated branch name for the child's task, as the model returned it.
async fn generate_branch_name(
    state: &Arc<AppState>,
    project_id: &str,
    cwd: &str,
    message: &str,
    attachments: &[Value],
) -> Result<(String, bool), String> {
    let settings = state.settings.internal().map_err(|e| e.to_string())?;
    let naming = branch::Naming::from_settings(&crate::title_generation::project_settings(
        &settings, project_id,
    ));
    let writer = &settings["sourceControlWriterModelSelection"];
    let selection = if writer.is_object() {
        writer.clone()
    } else {
        crate::title_generation::project_setting(
            &settings,
            project_id,
            "textGenerationModelSelection",
        )
    };
    let prompt = branch::build_branch_name_prompt(message, attachments, &naming);
    let generated = tokio::time::timeout(
        BRANCH_NAME_TIMEOUT,
        runner::generate_branch_name(
            state,
            &settings,
            runner::Request {
                cwd,
                prompt: &prompt,
                attachments,
                selection,
                project_id,
                output: runner::OutputKind::Branch,
            },
        ),
    )
    .await
    .map_err(|_| "Timed out after 20 seconds.".to_owned())??;
    Ok((
        branch::format_generated_branch_name(&generated, &naming),
        naming.mode == "custom",
    ))
}

/// Prepares `run_id` of `thread_id`, a delegated child's run that waits for its worktree.
pub(crate) async fn prepare(state: Arc<AppState>, command_id: &str, thread_id: &str, run_id: &str) {
    // A run in the project's own checkout has nothing to prepare: it starts.
    if waits_in_root(&state, thread_id, run_id).await {
        let released = dispatch(
            &state,
            json!({
                "type": "prepared-run.release",
                "commandId": format!("{command_id}:release"),
                "threadId": thread_id,
                "runId": run_id,
            }),
        )
        .await;
        if let Err(error) = released {
            tracing::warn!(%error, thread_id, "failed to start a run in its project's checkout");
        }
        return;
    }
    let Some(preparation) = load(&state, thread_id, run_id).await else {
        return;
    };
    let tracker = state.worktree_setup.clone();
    let (cancel, mut cancelled) = tokio::sync::oneshot::channel();
    tracker.begin(
        thread_id,
        None,
        Some(&preparation.base_ref),
        &STAGES,
        cancel,
    );
    let mut created: Option<(String, String)> = None;
    let outcome = run(
        &state,
        command_id,
        thread_id,
        run_id,
        &preparation,
        &mut created,
        &mut cancelled,
    )
    .await;
    match outcome {
        Ok(()) => {
            tracker.finish(thread_id, "done", None);
        }
        Err(Failure { detail, cancelled }) => {
            tracker.finish(
                thread_id,
                if cancelled { "cancelled" } else { "failed" },
                (!cancelled).then_some(detail.as_str()),
            );
            if let Some((path, branch)) = created {
                let removed = vcs(
                    &state,
                    "removeWorktree",
                    json!({"cwd": preparation.root, "path": path, "force": true}),
                )
                .await;
                match removed {
                    Ok(_) => {
                        delete_local_branch(&preparation.root, &branch).await;
                        let _ = dispatch(
                            &state,
                            json!({
                                "type": "thread.metadata.update",
                                "commandId": format!("{command_id}:cancel-workspace"),
                                "threadId": thread_id,
                                "worktreePath": null,
                                "branch": null,
                            }),
                        )
                        .await;
                    }
                    Err(error) => tracing::warn!(
                        %error, path, "failed to remove an abandoned thread worktree"
                    ),
                }
            }
            let still_preparing = state
                .store
                .read()
                .await
                .threads
                .get(thread_id)
                .and_then(|thread| thread.runs.iter().find(|run| run.wire_id() == run_id))
                .is_some_and(|run| run.status == "preparing");
            if still_preparing {
                let message = if cancelled {
                    "Worktree setup cancelled.".to_owned()
                } else {
                    detail
                };
                let failed = dispatch(
                    &state,
                    json!({
                        "type": "prepared-run.fail",
                        "commandId": format!("{command_id}:fail"),
                        "threadId": thread_id,
                        "runId": run_id,
                        "failure": {
                            "class": "validation_error",
                            "message": message,
                            "retryable": false,
                        },
                    }),
                )
                .await;
                if let Err(error) = failed {
                    tracing::warn!(%error, thread_id, "failed to record a worktree preparation failure");
                }
            }
        }
    }
}

/// Deletes the abandoned worktree's branch, as `deleteLocalBranch` with `force`.
async fn delete_local_branch(root: &str, branch: &str) {
    let deleted = crate::login_shell::tokio_command("git")
        .args(["branch", "-D", "--", branch])
        .current_dir(root)
        .env("GIT_TERMINAL_PROMPT", "0")
        .stdin(std::process::Stdio::null())
        .kill_on_drop(true)
        .output()
        .await;
    if !deleted.is_ok_and(|output| output.status.success()) {
        tracing::warn!(branch, "could not delete an abandoned worktree branch");
    }
}

struct Failure {
    detail: String,
    cancelled: bool,
}

impl From<String> for Failure {
    fn from(detail: String) -> Self {
        Failure {
            detail,
            cancelled: false,
        }
    }
}

/// Whether the run still waits for its workspace: a cancelled task or stopped thread ends it.
async fn preparing(state: &AppState, thread_id: &str, run_id: &str) -> bool {
    state
        .store
        .read()
        .await
        .threads
        .get(thread_id)
        .and_then(|thread| thread.runs.iter().find(|run| run.wire_id() == run_id))
        .is_some_and(|run| run.status == "preparing")
}

async fn run(
    state: &Arc<AppState>,
    command_id: &str,
    thread_id: &str,
    run_id: &str,
    preparation: &Preparation,
    created: &mut Option<(String, String)>,
    cancelled: &mut tokio::sync::oneshot::Receiver<()>,
) -> Result<(), Failure> {
    let tracker = state.worktree_setup.clone();
    let root = preparation.root.as_str();
    let stop = |cancelled: &mut tokio::sync::oneshot::Receiver<()>| {
        let fired = !matches!(
            cancelled.try_recv(),
            Err(tokio::sync::oneshot::error::TryRecvError::Empty)
        );
        fired.then(|| Failure {
            detail: "Worktree setup cancelled.".into(),
            cancelled: true,
        })
    };
    // A temporary branch until the generated name arrives.
    let settings = state.settings.internal().unwrap_or_default();
    let naming = branch::Naming::from_settings(&crate::title_generation::project_settings(
        &settings,
        &preparation.project_id,
    ));
    let hex = crate::platform::uuid_v4().simple().to_string();
    let mut branch_name = branch::temporary_branch(&hex);
    if naming.mode == "static" {
        let token = &branch_name[branch_name.len() - 8..];
        let mut fixed = naming.clone();
        fixed.mode = "static".into();
        branch_name = branch::format_generated_branch_name(token, &fixed);
    }
    let name_wanted = !preparation.message.is_empty() && branch::is_temporary_branch(&branch_name);
    let name_generation = async {
        if name_wanted {
            Some(
                generate_branch_name(
                    state,
                    &preparation.project_id,
                    root,
                    &preparation.message,
                    &preparation.attachments,
                )
                .await,
            )
        } else {
            None
        }
    };
    let create = async {
        dispatch(
            state,
            json!({
                "type": "prepared-run.progress",
                "commandId": format!("{command_id}:progress:worktree"),
                "threadId": thread_id,
                "runId": run_id,
                "phase": "worktree",
            }),
        )
        .await?;
        tracker.stage_status(thread_id, "fetch", "skipped", None);
        let mut branch_name = branch_name.clone();
        // An existing branch at one of its parents blocks a nested name.
        let parts: Vec<&str> = branch_name.split('/').collect();
        for depth in 1..parts.len() {
            let parent = format!("refs/heads/{}", parts[..depth].join("/"));
            if has_commit(state, root, &parent).await {
                branch_name = branch_name.replace('/', "-");
                break;
            }
        }
        tracker.stage_status(thread_id, "checkout", "running", None);
        let worktree = vcs(
            state,
            "createWorktree",
            json!({
                "cwd": root,
                "refName": preparation.base_ref,
                "newRefName": branch_name,
                "baseRefName": preparation.base_ref,
                "path": null,
            }),
        )
        .await?;
        let path = worktree["worktree"]["path"]
            .as_str()
            .ok_or("The worktree was created without a path.")?
            .to_owned();
        let branch_name = worktree["worktree"]["refName"]
            .as_str()
            .unwrap_or(&branch_name)
            .to_owned();
        Ok::<_, String>((path, branch_name))
    };
    let (generated, worktree) = tokio::join!(name_generation, create);
    let (path, temporary) = worktree?;
    *created = Some((path.clone(), temporary.clone()));
    tracker.set_workspace(thread_id, &path, &temporary);
    tracker.skip_pending(thread_id, "submodules", "none");
    tracker.stage_status(thread_id, "checkout", "done", None);
    if let Some(failure) = stop(cancelled) {
        return Err(failure);
    }
    dispatch(
        state,
        json!({
            "type": "thread.metadata.update",
            "commandId": format!("{command_id}:workspace"),
            "threadId": thread_id,
            "branch": temporary,
            "worktreePath": path,
        }),
    )
    .await?;
    let mut current_branch = temporary.clone();
    match generated {
        Some(Ok((name, exact))) => {
            let renamed = vcs(
                state,
                "renameBranch",
                json!({"cwd": path, "oldBranch": temporary, "newBranch": name, "exactName": exact}),
            )
            .await;
            match renamed {
                Ok(renamed) => {
                    let new = renamed["branch"].as_str().unwrap_or(&name).to_owned();
                    current_branch = new.clone();
                    if let Some(created) = created.as_mut() {
                        created.1 = new.clone();
                    }
                    let updated = dispatch(
                        state,
                        json!({
                            "type": "thread.metadata.update",
                            "commandId": format!("{command_id}:branch-rename"),
                            "threadId": thread_id,
                            "branch": new,
                            "worktreePath": path,
                            "expectedBranch": temporary,
                            "expectedWorktreePath": path,
                        }),
                    )
                    .await;
                    if let Err(error) = updated {
                        tracing::warn!(%error, thread_id, "parent thread worktree branch update failed");
                    }
                }
                Err(error) => {
                    tracing::warn!(%error, thread_id, "thread worktree branch rename failed")
                }
            }
        }
        Some(Err(error)) => {
            tracing::warn!(%error, thread_id, "thread worktree branch rename failed")
        }
        None => {}
    }
    tracker.set_workspace(thread_id, &path, &current_branch);
    dispatch(
        state,
        json!({
            "type": "prepared-run.progress",
            "commandId": format!("{command_id}:progress:setup"),
            "threadId": thread_id,
            "runId": run_id,
            "phase": "setup",
        }),
    )
    .await?;
    tracker.stage_status(thread_id, "setup-script", "running", None);
    let script = preparation
        .scripts
        .iter()
        .find(|script| script["runOnWorktreeCreate"] == true && script["command"].is_string());
    match script {
        None => {
            tracker.stage_status(thread_id, "setup-script", "skipped", None);
        }
        Some(script) => {
            // The script runs in a terminal of its own; the agent does not wait for it.
            let terminal_id = format!("setup-{}", crate::platform::uuid_v4());
            let command = script["command"].as_str().unwrap_or_default();
            let started = async {
                state
                    .workspace_call(
                        "terminal.open".into(),
                        json!({
                            "threadId": thread_id, "terminalId": terminal_id, "cwd": path,
                            "worktreePath": path, "cols": 120, "rows": 30,
                            "env": {"T3CODE_PROJECT_ROOT": root, "T3CODE_WORKTREE_PATH": path},
                        }),
                    )
                    .await?;
                state
                    .workspace_call(
                        "terminal.write".into(),
                        json!({"threadId": thread_id, "terminalId": terminal_id, "data": format!("{command}\n")}),
                    )
                    .await
            }
            .await;
            match started {
                Ok(_) => {
                    tracker.set_setup_script(
                        thread_id,
                        script["name"].as_str().unwrap_or_default(),
                        command,
                        &terminal_id,
                    );
                    tracker.stage_status(thread_id, "setup-script", "done", None);
                }
                Err(error) => {
                    return Err(
                        format!("Setup script failed to start: {}", message_of(&error)).into(),
                    )
                }
            }
        }
    }
    if let Some(failure) = stop(cancelled) {
        return Err(failure);
    }
    tracker.mark_uncancellable(thread_id);
    tracker.stage_status(thread_id, "agent", "running", None);
    if !preparing(state, thread_id, run_id).await {
        return Err(Failure {
            detail: "Worktree setup cancelled.".into(),
            cancelled: true,
        });
    }
    dispatch(
        state,
        json!({
            "type": "prepared-run.release",
            "commandId": format!("{command_id}:release"),
            "threadId": thread_id,
            "runId": run_id,
        }),
    )
    .await
    .map_err(|detail| {
        // A run that stopped meanwhile keeps no worktree.
        Failure {
            detail,
            cancelled: true,
        }
    })?;
    *created = None;
    tracker.stage_status(thread_id, "agent", "done", None);
    Ok(())
}
