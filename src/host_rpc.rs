pub use crate::local_services::media::routes;
use crate::AppState;
use serde_json::Value;

/// Dispatch a host-backed unary RPC or obtain the first value of its stream.
pub async fn dispatch(
    state: &AppState,
    method: &str,
    input: &Value,
) -> Option<Result<Value, Value>> {
    let (service, name) = method.split_once('.')?;
    match service {
        "projects" => match name {
            "listEntries" => state.workspace.list_entries(input).await,
            "listIgnoredFiles" => state.workspace.list_ignored_files(input).await,
            "searchEntries" => state.workspace.search_entries(input).await,
            "searchContents" => state.workspace.search_contents(input).await,
            "readFile" => state.workspace.read_file(input).await,
            "writeFile" => state.workspace.write_file(input).await,
            _ => None,
        },
        "filesystem" => match name {
            "getMetadata" => state.workspace.file_metadata(input).await,
            "browse" => state.workspace.browse(input).await,
            _ => None,
        },
        "git" if matches!(name, "resolvePullRequest" | "preparePullRequestThread") => {
            let tools = state.git_tools().await;
            Some(state.workspace.pull_request(name, input, &tools).await)
        }
        "review" => Some(state.workspace.review(name, input).await),
        "vcs" => {
            state
                .workspace
                .vcs(
                    name,
                    input,
                    name == "refreshStatus" && automatic_git_fetch(state),
                )
                .await
        }
        "terminal" => state.workspace.terminal(name, input).await,
        "preview" if name == "stopLocalServer" => {
            Some(state.workspace.stop_local_server(input).await)
        }
        "preview" if matches!(name, "reportProfiles" | "clearProfile") => {
            if name == "clearProfile" {
                if let Some(id) = input["profileId"]
                    .as_str()
                    .filter(|s| !s.trim().is_empty() && *s != "incognito")
                {
                    state
                        .workspace
                        .close_preview_pages(None, None, Some(id.trim()))
                        .await;
                }
            }
            Some(state.workspace.preview_profile(name, input))
        }
        "preview" => {
            let reply = state.workspace.preview(name, input);
            if name == "close" && reply.as_ref().is_some_and(Result::is_ok) {
                state
                    .workspace
                    .close_preview_pages(
                        input["threadId"].as_str().map(str::trim),
                        input["tabId"].as_str().map(str::trim),
                        None,
                    )
                    .await;
            }
            reply
        }
        "device" => state.workspace.device(name, input).await,
        _ => None,
    }
}

/// `VcsAutomaticFetchInterval`: a zero interval turns off the fetch a status read starts.
fn automatic_git_fetch(state: &AppState) -> bool {
    state.settings.internal().map_or(true, |settings| {
        settings["automaticGitFetchInterval"].as_f64() != Some(0.0)
    })
}

pub async fn polled_stream(
    state: &AppState,
    method: &str,
    input: &Value,
) -> Option<Result<Value, Value>> {
    if method == "subscribeDiscoveredLocalServers" {
        return Some(state.workspace.local_servers(input).await);
    }
    if !matches!(method, "subscribeVcsBranch" | "subscribeVcsStatus") {
        return None;
    }
    if method == "subscribeVcsBranch" {
        return Some(state.workspace.vcs_branch(input).await);
    }
    let status = match state
        .workspace
        .vcs("refreshStatus", input, automatic_git_fetch(state))
        .await?
    {
        Ok(status) => status,
        Err(e) => return Some(Err(e)),
    };
    let mut local = serde_json::json!({});
    let mut remote = serde_json::json!({});
    for (key, value) in status.as_object()? {
        if matches!(
            key.as_str(),
            "hasUpstream" | "aheadCount" | "behindCount" | "aheadOfDefaultCount" | "pr"
        ) {
            remote[key] = value.clone();
        } else {
            if key != "branchChanges" || input["includeRemote"] != false {
                local[key] = value.clone();
            }
        }
    }
    Some(Ok(
        serde_json::json!({"_tag":"snapshot","local":local,"remote":remote}),
    ))
}
