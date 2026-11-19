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
        "git" if matches!(name, "resolvePullRequest" | "preparePullRequestThread") => {
            Some(crate::local_services::git_workflow::pull_request(state, name, input).await)
        }
        "review" => Some(crate::local_services::review::call(state, name, input).await),
        "vcs" => state.vcs.call(name, input, &state.data_dir).await,
        "terminal" => state.terminals.call(name, input).await,
        "preview" if name == "stopLocalServer" => {
            Some(crate::local_services::discovery::stop(state, input).await)
        }
        "preview" if matches!(name, "reportProfiles" | "clearProfile") => {
            if name == "clearProfile" {
                if let Some(id) = input["profileId"]
                    .as_str()
                    .filter(|s| !s.trim().is_empty() && *s != "incognito")
                {
                    state
                        .previews
                        .close_pages(None, None, Some(id.trim()))
                        .await;
                }
            }
            Some(state.previews.profile(name, input, &state.data_dir))
        }
        "preview" => {
            let reply = state.previews.call(name, input);
            if name == "close" && reply.as_ref().is_some_and(Result::is_ok) {
                state
                    .previews
                    .close_pages(
                        input["threadId"].as_str().map(str::trim),
                        input["tabId"].as_str().map(str::trim),
                        None,
                    )
                    .await;
            }
            reply
        }
        "device" => state.devices.call(name, input, &state.data_dir).await,
        _ => None,
    }
}

pub async fn polled_stream(
    state: &AppState,
    method: &str,
    input: &Value,
) -> Option<Result<Value, Value>> {
    if method == "subscribeDiscoveredLocalServers" {
        return Some(crate::local_services::discovery::scan(state, input).await);
    }
    if !matches!(method, "subscribeVcsBranch" | "subscribeVcsStatus") {
        return None;
    }
    if method == "subscribeVcsBranch" {
        return Some(state.vcs.branch_status(input).await);
    }
    let status = match state
        .vcs
        .call("refreshStatus", input, &state.data_dir)
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
