//! Agent-facing pull-request registration, watch and guide tools.
use super::{forge::*, *};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
mod list;
mod rerun;
mod target;
mod writes;

const REPORT: &str = "End your reply with: PR <full URL>, head <SHA>, checks and reviews <state>.";

fn visible(thread: &Value) -> Vec<&Value> {
    rows(&thread["pullRequests"])
        .iter()
        .filter(|l| l["source"] != "stack-dismissed")
        .collect()
}

fn same(left: &Value, right: &Value) -> bool {
    crate::thread_projection::same_link(left, right, false)
}

fn cursor_decode(value: &Value) -> Result<Option<Value>, Refusal> {
    let Some(raw) = value.as_str() else {
        return Ok(None);
    };
    let bytes = URL_SAFE_NO_PAD
        .decode(raw)
        .map_err(|_| message("The list could not be carried on from where it left off."))?;
    serde_json::from_slice(&bytes)
        .map(Some)
        .map_err(|_| message("The list could not be carried on from where it left off."))
}

fn require_pull_requests(caller: &Caller) -> Result<(), Refusal> {
    if caller.granted.thread.is_none() {
        return Err(message(
            "MCP credential does not grant the pull-requests capability.",
        ));
    }
    Ok(())
}

fn cursor_encode(value: &Value) -> Value {
    let bytes = serde_json::to_vec(value).unwrap_or_default();
    json!(URL_SAFE_NO_PAD.encode(bytes))
}

async fn project_for<S: McpHost>(host: &Arc<S>, row: &Value) -> Value {
    host.control_state()
        .store
        .read()
        .await
        .project_rows
        .get(row["projectId"].as_str().unwrap_or_default())
        .map(|p| host.project_row(p))
        .unwrap_or(Value::Null)
}

fn branch_target(row: &Value) -> Option<Value> {
    row["branchPullRequest"]
        .as_object()
        .map(|branch| {
            fields(
                &Value::Object(branch.clone()),
                &["host", "repository", "number", "url"],
            )
        })
        .or_else(|| {
            rows(&row["pullRequests"])
                .iter()
                .rev()
                .find(|link| {
                    link["source"] == "created"
                        && (row["branch"].is_null()
                            || link["snapshot"]["headBranch"] == row["branch"])
                })
                .map(|link| fields(link, &["host", "repository", "number", "url"]))
        })
}

async fn resolved_target<S: McpHost>(host: &Arc<S>, row: &Value, args: &Value) -> Outcome {
    let project = project_for(host, row).await;
    let input = if args["number"].is_null() {
        branch_target(row)
            .ok_or_else(|| message("This thread has no pull request for its branch."))?
    } else {
        args.clone()
    };
    target::resolve(&input, &project["repositoryIdentity"])
}

fn activity_conversation(
    activity: &Value,
    include: &[Value],
    include_resolved: bool,
) -> Option<Value> {
    let wants_threads = include.iter().any(|v| v == "threads");
    let wants_comments = include.iter().any(|v| v == "comments");
    if !wants_threads && !wants_comments {
        return None;
    }
    let threads = rows(&activity["reviewThreads"])
        .iter()
        .filter(|thread| include_resolved || thread["isResolved"] != true)
        .map(|thread| {
            let comments = rows(&thread["comments"]).iter().map(|comment| json!({
                "author": comment["author"]["login"].as_str().or_else(|| comment["author"].as_str()),
                "body": comment["body"], "createdAt": comment["createdAt"]
            })).collect::<Vec<_>>();
            json!({"id":thread["id"],"path":thread["path"],"line":thread["line"],"isResolved":thread["isResolved"],"isOutdated":thread["isOutdated"],"comments":comments,"moreComments":false})
        }).collect::<Vec<_>>();
    let comments = rows(&activity["comments"]).iter().map(|comment| json!({
        "id":comment["id"],"kind":comment["kind"],"author":comment["author"]["login"].as_str().or_else(|| comment["author"].as_str()),"body":comment["body"],"createdAt":comment["createdAt"],"path":comment["path"],"reviewState":comment["reviewState"]
    })).collect::<Vec<_>>();
    let mut result = json!({
        "truncated": activity["commentsTruncated"] == true || activity["reviewThreadsTruncated"] == true,
        "nextCursor": null
    });
    if wants_threads {
        result["reviewThreads"] = Value::Array(threads);
    }
    if wants_comments {
        result["comments"] = Value::Array(comments);
    }
    Some(result)
}

async fn agent_list<S: McpHost>(host: &Arc<S>, caller: &Caller, args: &Value) -> Outcome {
    require_pull_requests(caller)?;
    let row = thread(&**host, caller, args, false, false).await?;
    let mut input = json!({"projectId":row["projectId"],"state":args["state"].as_str().unwrap_or("open"),"involvement":args["involvement"].as_str().unwrap_or("all"),"limit":args["limit"].as_u64().unwrap_or(30).clamp(1,100)});
    if let Some(query) = args["query"].as_str() {
        input["query"] = json!(query);
    }
    if let Some(cursor) = cursor_decode(&args["cursor"])? {
        input["cursors"] = cursor;
    }
    let listed = host
        .provider_dispatch("pullRequests.list".into(), input)
        .await;
    let listed = match listed {
        Some(Ok(value)) => value,
        Some(Err(error)) => return Err(message(provider_message(&error))),
        None => return Err(message(provider_message(&Value::Null))),
    };
    let entries = rows(&listed["entries"]).iter().map(|entry| json!({
        "number":entry["number"],"title":entry["title"],"url":entry["url"],"author":entry["author"]["login"].as_str().or_else(|| entry["author"].as_str()),"headBranch":entry["headBranch"],"baseBranch":entry["baseBranch"],"state":entry["state"],"isDraft":entry["isDraft"],"reviewDecision":entry.get("reviewDecision").cloned().unwrap_or(Value::Null),"checksState":entry.get("checksState").cloned().unwrap_or(Value::Null),"updatedAt":entry["updatedAt"]
    })).collect::<Vec<_>>();
    if entries.is_empty() && rows(&listed["errors"]).is_empty() {
        if let Some(link) = visible(&row).first() {
            let repository = link["repository"].as_str().unwrap_or_default();
            if !repository.is_empty() {
                return Err(message(format!("{repository} could not be read.")));
            }
        }
    }
    let next = if listed["nextCursors"]
        .as_object()
        .is_some_and(|m| !m.is_empty())
    {
        cursor_encode(&listed["nextCursors"])
    } else {
        Value::Null
    };
    Ok(json!({"pullRequests":entries,"truncated":listed["truncated"],"nextCursor":next}))
}

async fn agent_read<S: McpHost>(host: &Arc<S>, caller: &Caller, args: &Value) -> Outcome {
    require_pull_requests(caller)?;
    let row = thread(&**host, caller, args, false, false).await?;
    let target = resolved_target(host, &row, args).await?;
    let mut ref_input = fields(&target, &["host", "repository", "number"]);
    ref_input["projectId"] = row["projectId"].clone();
    ref_input["allowStale"] = json!(false);
    let detail = host
        .provider_dispatch("pullRequests.detail".into(), ref_input.clone())
        .await;
    let detail = match detail {
        Some(Ok(value)) => value,
        Some(Err(error)) => return Err(message(provider_message(&error))),
        None => return Err(message(provider_message(&Value::Null))),
    };
    let include = args["include"].as_array().cloned().unwrap_or_default();
    let activity = match host
        .provider_dispatch("pullRequests.activity".into(), ref_input.clone())
        .await
    {
        Some(Ok(value)) => Some(value),
        Some(Err(error)) => return Err(message(provider_message(&error))),
        None => None,
    };
    let checks = rows(&detail["checks"]);
    let mut passed = 0;
    let mut pending = 0;
    let mut failed = Vec::new();
    let mut incomplete = false;
    for check in checks {
        match check["status"].as_str() {
            Some("failure" | "cancelled") => failed.push(check["name"].clone()),
            Some("pending" | "action-required") => pending += 1,
            _ => passed += 1,
        };
        if check["placeholder"] == true {
            incomplete = true;
        }
    }
    let mut result = fields(&target, &["host", "repository", "number", "url"]);
    for key in [
        "title",
        "state",
        "isDraft",
        "headBranch",
        "baseBranch",
        "headSha",
        "mergeability",
        "behindBy",
        "autoMergeEnabled",
    ] {
        result[key] = detail.get(key).cloned().unwrap_or(Value::Null);
    }
    result["author"] = detail["author"]["login"].clone();
    result["checks"] = json!({"passed":passed,"pending":pending,"failed":failed});
    if incomplete {
        result["checks"]["incomplete"] = json!(true);
    }
    let unresolved = activity
        .as_ref()
        .map(|a| {
            rows(&a["reviewThreads"])
                .iter()
                .filter(|t| t["isResolved"] != true)
                .count()
        })
        .unwrap_or(0);
    result["unresolvedThreads"] = json!(unresolved);
    if include.iter().any(|v| v == "body") {
        result["body"] = detail["body"].clone();
    }
    if include.iter().any(|v| v == "checks") {
        result["checkRuns"] = detail["checks"].clone();
    }
    if let Some(activity) = activity {
        if let Some(conversation) =
            activity_conversation(&activity, &include, args["includeResolved"] == true)
        {
            result["conversation"] = conversation;
        }
    }
    if include.iter().any(|v| v == "diff") {
        let diff = host
            .provider_dispatch("pullRequests.diff".into(), ref_input)
            .await;
        if let Some(Ok(value)) = diff {
            result["diff"] = value;
        }
    }
    Ok(result)
}

async fn agent_logs<S: McpHost>(host: &Arc<S>, caller: &Caller, args: &Value) -> Outcome {
    require_pull_requests(caller)?;
    let row = thread(&**host, caller, args, false, false).await?;
    let target = resolved_target(host, &row, args).await?;
    let mut input = fields(&target, &["host", "repository", "number"]);
    input["projectId"] = row["projectId"].clone();
    input["tailLines"] = json!(args["tailLines"].as_u64().unwrap_or(200).clamp(1, 2000));
    if let Some(check) = args["check"].as_str() {
        input["check"] = json!(check);
    }
    let logs = host
        .provider_dispatch("pullRequests.checkLogs".into(), input)
        .await;
    let logs = match logs {
        Some(Ok(value)) => value,
        Some(Err(error)) => return Err(message(provider_message(&error))),
        None => return Err(message(provider_message(&Value::Null))),
    };
    Ok(
        json!({"host":target["host"],"repository":target["repository"],"number":target["number"],"url":target["url"],"checks":logs}),
    )
}

fn group(groups: &mut Vec<Value>, target: &Value, state: Option<&str>) {
    let index = groups.iter().position(|g| {
        g["host"] == target["host"]
            && g["repository"] == target["repository"]
            && g["state"].as_str() == state
    });
    let index = index.unwrap_or_else(|| {
        let mut g = fields(target, &["host", "repository"]);
        g["numbers"] = json!([]);
        if let Some(state) = state {
            g["state"] = json!(state);
        }
        groups.push(g);
        groups.len() - 1
    });
    groups[index]["numbers"]
        .as_array_mut()
        .unwrap()
        .push(target["number"].clone());
}

async fn change_link<S: McpHost>(
    host: &Arc<S>,
    thread: &Value,
    target: &Value,
    link: bool,
) -> Result<(), Refusal> {
    let mut command = fields(target, &["host", "repository", "number"]);
    if link {
        command["url"] = target["url"].clone();
        command["source"] = json!("agent");
        dispatch(
            host,
            thread,
            "thread.pull-request.link",
            "mcp-pr-link",
            command,
            "Could not link the pull request.",
        )
        .await
    } else {
        dispatch(
            host,
            thread,
            "thread.pull-request.unlink",
            "mcp-pr-unlink",
            command,
            "Could not unlink the pull request.",
        )
        .await
    }
}

/// The reference's pull-request sync reactor reads a newly linked request after
/// the link command commits. Keep the read asynchronous so a batch of links is
/// observed as one burst, as it is by the reactor, and leave unavailable hosts
/// untouched.
async fn sync_link<S: McpHost>(host: &Arc<S>, thread: &Value, target: &Value) {
    // A host whose link command starts the read leaves it to that.
    if host.syncs_linked_pull_requests() {
        return;
    }
    let runtime = Arc::clone(host);
    let mut query = fields(target, &["host", "repository", "number"]);
    query["projectId"] = thread["projectId"].clone();
    query["allowStale"] = json!(false);
    let Some(Ok(summary)) = runtime
        .provider_dispatch("pullRequests.summary".into(), query)
        .await
    else {
        return;
    };
    let mut snapshot = fields(
        &summary,
        &[
            "state",
            "title",
            "headBranch",
            "baseBranch",
            "isDraft",
            "updatedAt",
            "closedAt",
            "mergedAt",
            "author",
            "additions",
            "deletions",
            "changedFiles",
            "reviewDecision",
            "checksState",
            "mergeability",
            "rebaseRequired",
        ],
    );
    snapshot["syncedAt"] = json!(crate::auth_service::iso(crate::auth_service::now()));
    let mut command = fields(target, &["host", "repository", "number"]);
    command["snapshot"] = snapshot;
    command["stack"] = summary.get("stack").cloned().unwrap_or(Value::Null);
    let _ = dispatch(
        &runtime,
        thread,
        "thread.pull-request-link.sync",
        "server:pr-sync",
        command,
        "Could not sync the pull request.",
    )
    .await;
}

async fn batch<S: McpHost>(host: &Arc<S>, thread: &Value, args: &Value, link: bool) -> Outcome {
    let existing = visible(thread);
    let mut changed = Vec::new();
    if !link {
        let mut count = 0;
        for l in &existing {
            let Some(state) = l["snapshot"]["state"].as_str() else {
                continue;
            };
            if rows(&args["states"]).iter().any(|s| s == state) {
                change_link(host, thread, l, false).await?;
                group(&mut changed, l, Some(state));
                count += 1;
            }
        }
        return Ok(
            json!({"unlinked":changed,"unlinkedCount":count,"remaining":existing.len()-count}),
        );
    }
    let mut present = Vec::new();
    let mut linked_targets = Vec::new();
    let mut seen = Vec::new();
    let mut invalid = Vec::new();
    for url in rows(&args["urls"]).iter().filter_map(Value::as_str) {
        let Some(target) = target::parse(url) else {
            invalid.push(url);
            continue;
        };
        if seen.iter().any(|t| same(t, &target)) {
            continue;
        }
        if existing.iter().any(|l| same(l, &target)) {
            group(&mut present, &target, None);
        } else {
            change_link(host, thread, &target, true).await?;
            linked_targets.push(target.clone());
            group(&mut changed, &target, None);
        }
        seen.push(target);
    }
    for target in linked_targets {
        sync_link(host, thread, &target).await;
    }
    let mut result = json!({"linked":changed,"alreadyLinked":present,"linkedCount":seen.len()});
    if !invalid.is_empty() {
        result["invalidUrls"] = json!(invalid);
    }
    if thread["lineage"]["relationshipToParent"] == "subagent" {
        result["next"] = json!(REPORT);
    }
    Ok(result)
}

pub(crate) fn provider_message(e: &Value) -> String {
    if let Some(detail) = e["detail"].as_str().or(e["requirement"].as_str()) {
        return detail.to_owned();
    }
    match e["reason"].as_str() {
        Some("cli-missing") => {
            "The tool this host is read through is not installed or set up.".into()
        }
        Some("cli-unauthenticated") => "This host has no working credentials.".into(),
        Some("repository-without-project") if e["host"].is_string() => format!(
            "No project in T3 Code is on {}. Add any {} project to read its change requests.",
            e["host"].as_str().unwrap(),
            e["host"].as_str().unwrap()
        ),
        _ => "Change requests cannot be browsed for this project's host yet.".into(),
    }
}

async fn watch<S: McpHost>(
    host: &Arc<S>,
    caller: &Caller,
    row: &Value,
    mut target: Value,
    watching: bool,
) -> Outcome {
    let existing = visible(row).into_iter().find(|l| same(l, &target));
    let was = existing.is_some_and(|l| l.get("watch").is_some());
    // CI as the start read saw it (#40866).
    let mut checks_state = Value::Null;
    if watching {
        if row["lineage"]["relationshipToParent"] == "subagent" {
            return Err(message("This thread is a subagent, so it cannot watch pull requests. Its parent thread owns the pull request: finish your task and report back instead."));
        }
        let merged = || message("The pull request is merged, so there is nothing to watch.");
        if existing.is_some_and(|l| l["snapshot"]["state"] == "merged") {
            return Err(merged());
        }
        if !was {
            let mut query = fields(&target, &["host", "repository", "number"]);
            query["projectId"] = row["projectId"].clone();
            query["allowStale"] = json!(false);
            match host
                .provider_dispatch("pullRequests.summary".into(), query)
                .await
            {
                Some(Ok(summary)) if summary["state"] == "merged" => return Err(merged()),
                Some(Ok(summary)) => checks_state = summary["checksState"].clone(),
                Some(Err(e)) if e["_tag"] != "PullRequestOperationError" => {
                    return Err(message(provider_message(&e)))
                }
                None => return Err(message(provider_message(&Value::Null))),
                _ => {}
            }
        }
    }
    let mut command = fields(&target, &["host", "repository", "number"]);
    command["watching"] = json!(watching);
    if watching {
        command["link"] = json!({"url":target["url"],"source":"agent"});
    }
    dispatch(
        host,
        row,
        "thread.pull-request.watch",
        "mcp-pr-watch",
        command,
        "Could not change whether the pull request is watched.",
    )
    .await?;
    let after = thread(
        &**host,
        caller,
        &json!({"threadId":row["id"]}),
        false,
        false,
    )
    .await?;
    target["watching"] = json!(visible(&after)
        .iter()
        .any(|l| same(l, &target) && l.get("watch").is_some()));
    target["wasWatching"] = json!(was);
    if checks_state.is_string() {
        // Checks that already passed never wake the watch.
        if checks_state == "passing" {
            target["next"] = json!("Checks already pass on this head, so no wake will come for them. Act on that now; the watch still wakes you on later changes.");
        }
        target["checksState"] = checks_state;
    }
    Ok(target)
}

fn matching<'a>(known: &'a [Value], target: &Value) -> Option<&'a Value> {
    let key = format!(
        "{}/{}",
        target["host"].as_str()?,
        target["repository"].as_str()?
    );
    known.iter().find(|p| {
        p["repositoryIdentity"]["canonicalKey"]
            .as_str()
            .is_some_and(|s| s.eq_ignore_ascii_case(&key))
    })
}

async fn guides<S: McpHost>(host: &Arc<S>, caller: &Caller, args: &Value, write: bool) -> Outcome {
    if write {
        writer(&**host, caller).await?;
    }
    let rows: Vec<_> = host
        .control_state()
        .store
        .read()
        .await
        .project_rows
        .values()
        .filter(|row| row["deletedAt"].is_null())
        .cloned()
        .collect();
    let mut known: Vec<_> = rows.iter().map(|r| host.project_row(r)).collect();
    let caller_id = caller.thread.as_ref().map(|t| t.project_id.as_str());
    known.sort_by_key(|p| p["id"].as_str() != caller_id);
    let preferred = known.iter().find(|p| p["id"].as_str() == caller_id);
    let targets = if write {
        super::forge::rows(&args["pullRequests"])
    } else {
        std::slice::from_ref(args)
    };
    let mut guides = Vec::new();
    for input in targets {
        let project = matching(&known, input).or(preferred);
        let resolved = target::resolve(
            input,
            project.map_or(&Value::Null, |p| &p["repositoryIdentity"]),
        );
        let mut target = match resolved {
            Ok(t) => t,
            Err(Refusal::Text(reason)) if write => {
                guides.push(json!({"host":input["host"].as_str().unwrap_or(""),"repository":input["repository"].as_str().unwrap_or(""),"number":input["number"].as_u64().unwrap_or(0),"url":input["url"].as_str().unwrap_or(""),"outcome":"failed","detail":reason}));
                continue;
            }
            Err(e) => return Err(e),
        };
        let own = matching(&known, &target);
        let missing=format!("No project in this environment is a checkout of {}, so its pull requests cannot be read here. Add one with t3_project_create.",target["repository"].as_str().unwrap_or(""));
        let result =
            if let Some(project) = own.or(preferred.filter(|_| !write)) {
                let mut params = fields(&target, &["host", "repository", "number"]);
                params["projectId"] = project["id"].clone();
                if write
                    && !host.settings()["reviewGuidesEnabled"]
                        .as_bool()
                        .unwrap_or(false)
                {
                    Err("Review guides are off. Turn them on in Settings → Source control & issues."
                    .to_owned())
                } else if write {
                    match host
                        .provider_dispatch("pullRequests.generateReviewGuide".into(), params)
                        .await
                    {
                        Some(Ok(v)) => Ok(v),
                        Some(Err(e))
                            if own.is_none()
                                && matches!(
                                    e["reason"].as_str(),
                                    Some("provider-unsupported" | "repository-without-project")
                                ) =>
                        {
                            Err(missing)
                        }
                        Some(Err(e)) => Err(provider_message(&e)),
                        None => Err(provider_message(&Value::Null)),
                    }
                } else {
                    match host
                        .provider_dispatch("pullRequests.detail".into(), params)
                        .await
                    {
                        Some(Ok(detail)) => Ok(json!({
                            "guide": null,
                            "run": null,
                            "currentHeadSha": detail["headSha"],
                        })),
                        Some(Err(e)) => Err(provider_message(&e)),
                        None => Err(provider_message(&Value::Null)),
                    }
                }
            } else {
                Err(missing)
            };
        if !write {
            let data = result.map_err(message)?;
            for k in ["guide", "run", "currentHeadSha"] {
                target[k] = data[k].clone();
            }
            target["stale"] = json!(
                !data["guide"].is_null()
                    && !data["currentHeadSha"].is_null()
                    && data["guide"]["headSha"] != data["currentHeadSha"]
            );
            return Ok(target);
        }
        match result {
            Ok(v) => {
                target["outcome"] = if v["started"] == true {
                    json!("started")
                } else {
                    json!("running")
                };
                target["detail"] = Value::Null;
            }
            Err(detail) => {
                target["outcome"] = json!("failed");
                target["detail"] = json!(detail);
            }
        }
        guides.push(target);
    }
    Ok(json!({"guides":guides}))
}

pub(super) async fn call<S: McpHost>(
    host: &Arc<S>,
    caller: &Caller,
    name: &str,
    args: &Value,
) -> Outcome {
    if matches!(
        name,
        "pull_request_create"
            | "pull_request_update"
            | "pull_request_merge"
            | "pull_request_review"
            | "pull_request_respond"
    ) {
        return writes::call(host, caller, name, args).await;
    }
    if name == "pull_request_list" {
        return agent_list(host, caller, args).await;
    }
    if name == "pull_request_read" {
        return agent_read(host, caller, args).await;
    }
    if name == "ci_rerun" {
        return rerun::call(host, caller, args).await;
    }
    if name == "ci_logs" {
        return agent_logs(host, caller, args).await;
    }
    if matches!(
        name,
        "write_pull_request_guides" | "read_pull_request_guide"
    ) {
        return guides(host, caller, args, name == "write_pull_request_guides").await;
    }
    let row = thread(
        &**host,
        caller,
        args,
        name != "list_thread_pull_requests",
        false,
    )
    .await?;
    match name {
        "list_thread_pull_requests" => return Ok(list::page(&visible(&row), args)),
        "link_pull_requests" | "unlink_pull_requests" => {
            return batch(host, &row, args, name == "link_pull_requests").await
        }
        _ => {}
    }
    let project = host
        .control_state()
        .store
        .read()
        .await
        .project_rows
        .get(row["projectId"].as_str().unwrap_or(""))
        .cloned();
    let project = project
        .as_ref()
        .map(|p| host.project_row(p))
        .unwrap_or(Value::Null);
    let mut t = target::resolve(args, &project["repositoryIdentity"])?;
    if matches!(name, "watch_pull_request" | "unwatch_pull_request") {
        return watch(host, caller, &row, t, name == "watch_pull_request").await;
    }
    let existed = visible(&row).iter().any(|l| same(l, &t));
    let link = name == "link_pull_request";
    if link != existed {
        change_link(host, &row, &t, link).await?;
        if link {
            sync_link(host, &row, &t).await;
        }
    }
    if link {
        t["alreadyLinked"] = json!(existed);
        if row["lineage"]["relationshipToParent"] == "subagent" {
            t["next"] = json!(REPORT);
        }
    } else {
        t = fields(&t, &["host", "repository", "number"]);
        t["wasLinked"] = json!(existed);
    }
    Ok(t)
}
