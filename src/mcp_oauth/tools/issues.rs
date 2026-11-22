//! Issue targets are resolved against the project's own tracker bindings.
//! A standalone Linear URL must also belong to the authenticated workspace.
use super::{forge::*, *};

fn issue_error(e: Value) -> Refusal {
    let unavailable = match e["reason"].as_str() {
        Some("tool-missing") => "The tool this tracker is read through is not installed or set up.",
        Some("unauthenticated") => "This tracker has no working credentials.",
        _ => "Issues cannot be browsed for this project yet.",
    };
    if e["_tag"] == "IssueOperationError" {
        message(format!(
            "Issue operation {} failed: {}",
            e["operation"].as_str().unwrap_or(""),
            e["detail"].as_str().unwrap_or("")
        ))
    } else {
        message(unavailable)
    }
}

async fn rpc<S: McpHost>(host: &Arc<S>, method: &str, params: Value) -> Outcome {
    host.provider_dispatch(method.to_owned(), params)
        .await
        .ok_or_else(|| issue_error(Value::Null))?
        .map_err(issue_error)
}

fn project_scope(scopes: &Value, id: Option<&str>) -> Outcome {
    let id = id.ok_or_else(|| {
        message("Pass projectId: this MCP client is not running inside a T3 thread.")
    })?;
    rows(scopes)
        .iter()
        .find(|s| s["projectId"] == id)
        .cloned()
        .ok_or_else(|| {
            message(format!(
                "Project {id} was not found or has no issue tracker."
            ))
        })
}

struct IssueUrl<'a> {
    tracker: &'static str,
    scope: String,
    key: &'a str,
    workspace: Option<&'a str>,
}

fn parse_issue(url: &url::Url) -> Option<IssueUrl<'_>> {
    let path: Vec<_> = url.path_segments()?.collect();
    match (url.scheme(), url.host_str()?, path.as_slice()) {
        ("https", "linear.app", [workspace, "issue", key, ..])
            if url.port().is_none() && !workspace.is_empty() =>
        {
            static KEY: LazyLock<regex::Regex> =
                LazyLock::new(|| regex::Regex::new(r"^([A-Z][A-Z0-9_]*)-[1-9][0-9]*$").unwrap());
            let captures = KEY.captures(key)?;
            Some(IssueUrl {
                tracker: "linear",
                scope: captures[1].to_owned(),
                key,
                workspace: Some(workspace),
            })
        }
        ("https" | "http", "github.com", [owner, repo, "issues", key, ..])
            if !owner.is_empty()
                && !repo.is_empty()
                && key.bytes().all(|c| c.is_ascii_digit())
                && key.parse::<u64>().ok()? > 0 =>
        {
            Some(IssueUrl {
                tracker: "github",
                scope: format!("{owner}/{repo}"),
                key,
                workspace: None,
            })
        }
        _ => None,
    }
}

struct Target {
    identity: Value,
    detail: Option<Value>,
}

async fn resolve<S: McpHost>(
    host: &Arc<S>,
    scopes: &Value,
    project: Option<&str>,
    args: &Value,
) -> Result<Target, Refusal> {
    let Some(raw) = args["url"].as_str() else {
        let key = args["key"].as_str().ok_or_else(|| {
            message("Pass either url, or key (with projectId when not running inside a T3 thread).")
        })?;
        let mut identity = project_scope(scopes, project)?;
        identity["key"] = json!(key);
        return Ok(Target {
            identity,
            detail: None,
        });
    };
    let invalid =
        || message("Issue operation resolveUrl failed: This is not a recognised issue URL.");
    let url = url::Url::parse(raw).map_err(|_| invalid())?;
    let parsed = parse_issue(&url).ok_or_else(invalid)?;
    let mut identity = rows(scopes)
        .iter()
        .filter(|s| {
            s["tracker"] == parsed.tracker
                && s["host"].as_str()
                    == Some(&url[url::Position::BeforeHost..url::Position::AfterPort])
        })
        .min_by_key(|s| {
            (
                !s["scope"]
                    .as_str()
                    .unwrap_or("")
                    .eq_ignore_ascii_case(&parsed.scope),
                s["projectId"].as_str() != project,
            )
        })
        .cloned()
        .ok_or_else(|| issue_error(Value::Null))?;
    if !identity["scope"]
        .as_str()
        .unwrap_or("")
        .eq_ignore_ascii_case(&parsed.scope)
    {
        identity["scope"] = json!(parsed.scope);
    }
    identity["key"] = json!(parsed.key);
    identity["url"] = json!(raw);
    let detail = if let Some(workspace) = parsed.workspace {
        let detail = rpc(host, "issues.detail", identity.clone()).await?;
        let canonical = detail["url"].as_str().and_then(|u| url::Url::parse(u).ok());
        if canonical
            .as_ref()
            .and_then(parse_issue)
            .and_then(|p| p.workspace)
            != Some(workspace)
        {
            return Err(message("Issue operation resolveUrl failed: This issue URL does not belong to the connected workspace."));
        }
        Some(detail)
    } else {
        None
    };
    Ok(Target { identity, detail })
}

fn summary(row: &Value) -> Value {
    let mut result = fields(
        row,
        &[
            "projectId",
            "tracker",
            "host",
            "scope",
            "key",
            "url",
            "displayKey",
            "title",
            "state",
        ],
    );
    result["author"] = row["author"]["login"].clone();
    for (field, key) in [("assignees", "login"), ("labels", "name")] {
        result[field] = json!(rows(&row[field])
            .iter()
            .map(|v| &v[key])
            .collect::<Vec<_>>());
    }
    result
}

fn read_result(target: &Value, detail: &Value) -> Value {
    let mut result = summary(detail);
    for (key, value) in target.as_object().into_iter().flatten() {
        result[key] = value.clone();
    }
    for key in [
        "writable",
        "capabilities",
        "states",
        "createdAt",
        "updatedAt",
        "closedAt",
        "body",
    ] {
        result[key] = detail[key].clone();
    }
    result["comments"]=json!(rows(&detail["comments"]).iter().map(|c|json!({"id":c["id"],"author":c["author"]["login"],"body":c["body"],"createdAt":c["createdAt"]})).collect::<Vec<_>>());
    result["commentsTruncated"] = json!(!detail["commentsCursor"].is_null());
    result["linkedPullRequests"] = json!(rows(&detail["linkedPullRequests"])
        .iter()
        .map(|pr| fields(pr, &["url", "title", "state", "closesIssue"]))
        .collect::<Vec<_>>());
    result
}

pub(super) async fn call<S: McpHost>(
    host: &Arc<S>,
    caller: &Caller,
    name: &str,
    args: &Value,
) -> Outcome {
    if matches!(
        name,
        "create_issue" | "comment_on_issue" | "set_issue_state"
    ) {
        writer(&**host, caller).await?;
    }
    let thread = if matches!(name, "link_issue" | "unlink_issue" | "list_thread_issues") {
        thread(&**host, caller, args, name != "list_thread_issues", true).await?
    } else {
        Value::Null
    };
    if name == "list_thread_issues" {
        return Ok(
            json!({"issues":rows(&thread["issues"]).iter().map(|item|fields(item,&["projectId","tracker","host","scope","key","url","source","displayKey","title"])).collect::<Vec<_>>()}),
        );
    }
    require_issues(caller)?;
    let project = args["projectId"]
        .as_str()
        .or(thread["projectId"].as_str())
        .or_else(|| caller.thread.as_ref().map(|t| t.project_id.as_str()));
    let scopes = rpc(host, "issues.projectScopes", json!({})).await?;
    if name == "list_issues" {
        let scope = project_scope(&scopes, project)?;
        let mut params = fields(args, &["query", "limit", "cursors"]);
        params["projectId"] = scope["projectId"].clone();
        params["state"] = json!(args["state"].as_str().unwrap_or("open"));
        let result = rpc(host, "issues.list", params).await?;
        let issues = rows(&result["entries"])
            .iter()
            .map(|entry| {
                let mut out = summary(entry);
                out["updatedAt"] = entry["updatedAt"].clone();
                out["commentCount"] = entry["commentCount"].clone();
                out
            })
            .collect::<Vec<_>>();
        return Ok(
            json!({"issues":issues,"nextCursors":result["nextCursors"],"errors":rows(&result["errors"]).iter().map(|e|fields(e,&["projectId","message"])).collect::<Vec<_>>()}),
        );
    }
    if name == "create_issue" {
        let mut scope = project_scope(&scopes, project)?;
        let mut params = scope.clone();
        params["title"] = args["title"].clone();
        params["body"] = json!(args["body"].as_str().unwrap_or(""));
        for field in ["labels", "assignees"] {
            params[field] = json!(rows(&args[field]));
        }
        let result = rpc(host, "issues.create", params).await?;
        for key in ["key", "url", "displayKey"] {
            scope[key] = result[key].clone();
        }
        return Ok(scope);
    }
    let Target {
        mut identity,
        detail,
    } = resolve(host, &scopes, project, args).await?;
    if matches!(name, "comment_on_issue" | "set_issue_state") {
        let mut params = identity.clone();
        let (op, key) = if name == "comment_on_issue" {
            ("issues.comment", "body")
        } else {
            ("issues.setState", "state")
        };
        params[key] = args[key].clone();
        rpc(host, op, params).await?;
        return Ok(identity);
    }
    let existing = rows(&thread["issues"])
        .iter()
        .find(|link| crate::thread_projection::same_link(link, &identity, true));
    if name == "unlink_issue" {
        if let Some(link) = existing {
            dispatch(
                host,
                &thread,
                "thread.issue.unlink",
                "mcp-issue-unlink",
                fields(&identity, &["tracker", "host", "scope", "key"]),
                "Could not change the issue link.",
            )
            .await?;
            identity["url"] = link["url"].clone();
        }
        identity["wasLinked"] = json!(existing.is_some());
        return Ok(identity);
    }
    let detail = match detail {
        Some(detail) => detail,
        None => rpc(host, "issues.detail", identity.clone()).await?,
    };
    if name == "read_issue" {
        return Ok(read_result(&identity, &detail));
    }
    if identity["url"].is_null() {
        identity["url"] = detail["url"].clone();
    }
    if existing.is_none() {
        let mut params = identity.clone();
        params["displayKey"] = detail["displayKey"].clone();
        params["title"] = detail["title"].clone();
        params["source"] = json!("agent");
        dispatch(
            host,
            &thread,
            "thread.issue.link",
            "mcp-issue-link",
            params,
            "Could not change the issue link.",
        )
        .await?;
    }
    identity["alreadyLinked"] = json!(existing.is_some());
    Ok(identity)
}
