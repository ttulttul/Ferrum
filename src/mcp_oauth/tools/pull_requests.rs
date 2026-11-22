//! Agent-facing pull-request registration, watch and guide tools.
use super::{forge::*, *};
mod list;
mod target;

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
