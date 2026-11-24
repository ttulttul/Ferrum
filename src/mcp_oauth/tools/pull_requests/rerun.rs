use super::*;

pub(super) async fn call<S: McpHost>(host: &Arc<S>, caller: &Caller, args: &Value) -> Outcome {
    writer(&**host, caller).await?;
    require_pull_requests(caller)?;
    let row = thread(&**host, caller, &Value::Null, false, false).await?;
    let project = project_for(host, &row).await;
    if project.is_null() {
        return Err(message(format!("No project in this environment is a checkout of {}, so its pull requests cannot be read here. Add one with t3_project_create.", args["repository"].as_str().unwrap_or(""))));
    }
    let missing = || {
        message("This thread's branch has no known PR. Pass number, or open one with pull_request_create.")
    };
    let opened = visible(&row).into_iter().rev().find(|link| {
        link["source"] == "created"
            && (link["snapshot"].is_null() || link["snapshot"]["headBranch"] == row["branch"])
    });
    let mut named = if args.get("number").is_some() {
        fields(args, &["number", "repository"])
    } else {
        fields(
            if row["branchPullRequest"].is_object() {
                &row["branchPullRequest"]
            } else {
                opened.ok_or_else(missing)?
            },
            &["number", "repository"],
        )
    };
    let identity = &project["repositoryIdentity"];
    if named["repository"].is_null() {
        if let Some((_, repo)) = identity["canonicalKey"]
            .as_str()
            .and_then(|k| k.split_once('/'))
        {
            named["repository"] = json!(repo);
        }
    }
    let target = target::resolve(&named, identity)?;
    let mut input = fields(&target, &["host", "repository", "number"]);
    input["projectId"] = row["projectId"].clone();
    let forge_error = |e: Value| {
        message(if e["_tag"] == "PullRequestOperationError" {
            format!(
                "Pull request operation {} failed: {}",
                e["operation"].as_str().unwrap_or(""),
                e["detail"].as_str().unwrap_or("")
            )
        } else {
            provider_message(&e)
        })
    };
    if args.get("number").is_none()
        && row["branchPullRequest"].is_null()
        && opened.is_some_and(|l| l["snapshot"].is_null())
    {
        let summary = host
            .provider_dispatch("pullRequests.summary".into(), input.clone())
            .await
            .unwrap_or(Err(Value::Null))
            .map_err(forge_error)?;
        if summary["headBranch"] != row["branch"] {
            return Err(missing());
        }
    }
    if let Some(check) = args.get("check") {
        input["check"] = check.clone();
    }
    let checks = host
        .provider_dispatch("pullRequests.rerunChecks".into(), input)
        .await
        .unwrap_or(Err(Value::Null))
        .map_err(forge_error)?;
    if rows(&checks).iter().any(|c| c["rerun"] == true)
        && rows(&row["pullRequests"])
            .iter()
            .any(|l| l["watch"].get("checksPending").is_some())
    {
        dispatch(
            host,
            &row,
            "thread.pull-request-watch.checks-reopen",
            "mcp-ci-rerun-checks",
            json!({}),
            "Could not reopen the watch.",
        )
        .await
        .unwrap_or_else(|_| tracing::warn!("reopening a watch's checks after a rerun failed"));
    }
    let mut answer = target;
    answer["checks"] = checks;
    Ok(answer)
}
