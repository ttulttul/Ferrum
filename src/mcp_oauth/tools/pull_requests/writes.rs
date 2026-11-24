use super::*;
mod positions;

fn forge_error(e: &Value) -> String {
    if e["_tag"] == "PullRequestOperationError" {
        format!(
            "Pull request operation {} failed: {}",
            e["operation"].as_str().unwrap_or(""),
            e["detail"].as_str().unwrap_or("")
        )
    } else {
        provider_message(e)
    }
}

struct Pr<'a, S> {
    host: &'a Arc<S>,
    identity: Value,
    reference: Value,
}

impl<S: McpHost> Pr<'_, S> {
    async fn rpc(&self, method: &str, extra: Value) -> Result<Value, String> {
        let mut params = self.reference.clone();
        params.as_object_mut().unwrap().extend(
            extra
                .as_object()
                .into_iter()
                .flatten()
                .map(|(k, v)| (k.clone(), v.clone())),
        );
        self.host
            .provider_dispatch(format!("pullRequests.{method}"), params)
            .await
            .unwrap_or(Err(Value::Null))
            .map_err(|e| forge_error(&e))
    }

    async fn update(&self, args: &Value) -> Result<Value, String> {
        let changes = fields(args, &["title", "body"]);
        if changes.as_object().is_some_and(|o| !o.is_empty()) {
            self.rpc("update", changes).await?;
        }
        for (key, applied) in [("addLabels", true), ("removeLabels", false)] {
            if let Some(labels) = args.get(key) {
                self.rpc("setLabels", json!({"labels":labels,"applied":applied}))
                    .await?;
            }
        }
        if let Some(names) = args["requestReviewers"].as_array() {
            let mut reviewers = Vec::new();
            for name in names.iter().filter_map(Value::as_str) {
                let result = self
                    .rpc("reviewerCandidates", json!({"query":name}))
                    .await?;
                let candidate = rows(&result["candidates"])
                    .iter()
                    .find(|c| {
                        ["login", "id"]
                            .iter()
                            .any(|k| c[*k].as_str().is_some_and(|s| s.eq_ignore_ascii_case(name)))
                    })
                    .ok_or_else(|| {
                        format!("No reviewer named {name} can be requested on this PR.")
                    })?;
                reviewers.push(fields(candidate, &["id", "kind"]));
            }
            self.rpc(
                "requestReviewers",
                json!({"reviewers":reviewers,"requested":true}),
            )
            .await?;
        }
        if let Some(ready) = args["ready"].as_bool() {
            self.rpc(
                "runAction",
                json!({"action":if ready {"ready"} else {"draft"}}),
            )
            .await?;
        }
        if let Some(state) = args["state"].as_str() {
            self.rpc(
                "runAction",
                json!({"action":if state=="closed" {"close"} else {"reopen"}}),
            )
            .await?;
        }
        Ok(self.identity.clone())
    }

    async fn respond(&self, args: &Value) -> Result<Value, String> {
        let thread = args.get("reviewThreadId");
        if let Some(body) = args.get("body") {
            match thread {
                Some(id) => {
                    self.rpc("replyToThread", json!({"threadId":id,"body":body}))
                        .await?
                }
                None => self.rpc("comment", json!({"body":body})).await?,
            };
        }
        if let (Some(id), Some(resolved)) = (thread, args.get("resolved")) {
            self.rpc("setThreadResolution",json!({"threadId":id,"resolved":resolved})).await.map_err(|e| {
                if args.get("body").is_some(){format!("The reply was posted, but setting resolved failed: {e} Retry without body.")}else{e}
            })?;
        }
        let mut result = self.identity.clone();
        result["replied"] = json!(args.get("body").is_some());
        result["resolved"] = args["resolved"].clone();
        Ok(result)
    }

    async fn review(&self, args: &Value) -> Result<Value, String> {
        let comments = rows(&args["comments"]);
        let mut placed = vec![None; comments.len()];
        let mut page = json!({});
        for _ in 0..10 {
            if placed.iter().all(Option::is_some) {
                break;
            }
            let diff = self.rpc("diff", page).await?;
            let patch = diff["patch"].as_str().unwrap_or("");
            for (slot, comment) in placed.iter_mut().zip(comments) {
                if slot.is_none() {
                    *slot = positions::draft(patch, comment);
                }
            }
            if diff["nextCursor"].is_null() {
                break;
            }
            page = json!({"cursor":diff["nextCursor"]});
        }
        if let Some(i) = placed.iter().position(Option::is_none) {
            let c = &comments[i];
            return Err(format!("{} line {} ({} side) is not in this PR's diff. Comment on a changed line or one shown around it.", c["path"].as_str().unwrap_or(""), c.get("startLine").unwrap_or(&c["line"]), c["side"].as_str().unwrap_or("new")));
        }
        self.rpc("submitReview",json!({"verdict":args["verdict"],"body":args["body"].as_str().unwrap_or(""),"comments":placed})).await?;
        Ok(self.identity.clone())
    }

    async fn merge(&self, args: &Value) -> Result<Value, String> {
        let mut op = json!({"action":if args["auto"]==true {"enable-auto-merge"} else {"merge"},"expectedHeadSha":args["expectedHeadSha"]});
        if let Some(method) = args.get("method") {
            op["mergeMethod"] = method.clone()
        }
        if args["stack"] == true {
            if args["auto"] == true {
                return Err("A stack merge cannot wait with auto.".into());
            }
            let stack = self.rpc("stack", json!({"bypassCache":true})).await?;
            let layers = rows(&stack["layers"]);
            let number = &self.identity["number"];
            let end=layers.iter().position(|l|l["number"]==*number).ok_or_else(||format!("PR #{number} is not in a stack this host can merge. Merge it without stack."))?;
            let mut pins = Vec::new();
            for l in layers[..=end].iter().filter(|l| l["state"] != "merged") {
                let actual = l["headSha"].as_str();
                let expected = args["expectedHeadSha"].as_str().unwrap_or("");
                let head = if l["number"] == *number {
                    let prefix =
                        expected.len() >= 7 && expected.bytes().all(|b| b.is_ascii_hexdigit());
                    actual
                        .filter(|h| {
                            *h == expected
                                || prefix
                                    && h.get(..expected.len())
                                        .is_some_and(|p| p.eq_ignore_ascii_case(expected))
                        })
                        .unwrap_or(expected)
                } else {
                    actual.ok_or_else(||format!("The host did not report PR #{}'s head, so the stack cannot be pinned. Try again.",l["number"]))?
                };
                pins.push(json!({"number":l["number"],"headSha":head}));
            }
            op["stackNumber"] = stack["number"].clone();
            op["expectedStackHeads"] = json!(pins);
        }
        self.rpc("runAction", op).await?;
        let mut result = self.identity.clone();
        result["state"] = self.rpc("summary", json!({"allowStale":false})).await?["state"].clone();
        Ok(result)
    }
}

pub(super) async fn call<S: McpHost>(
    host: &Arc<S>,
    caller: &Caller,
    name: &str,
    args: &Value,
) -> Outcome {
    writer(&**host, caller).await?;
    require_pull_requests(caller)?;
    match name {
        "pull_request_update"
            if [
                "title",
                "body",
                "ready",
                "state",
                "addLabels",
                "removeLabels",
                "requestReviewers",
            ]
            .iter()
            .all(|k| args.get(*k).is_none()) =>
        {
            return Err(message(
                "Pass title, body, ready, state, addLabels, removeLabels, or requestReviewers.",
            ))
        }
        "pull_request_respond"
            if args["body"].is_null()
                && (args["reviewThreadId"].is_null() || args["resolved"].is_null()) =>
        {
            return Err(message("Pass body, or reviewThreadId with resolved."))
        }
        "pull_request_respond"
            if args["reviewThreadId"].is_null() && !args["resolved"].is_null() =>
        {
            return Err(message("resolved needs reviewThreadId."))
        }
        _ => {}
    }
    let row = thread(&**host, caller, &Value::Null, false, false).await?;
    let project = project_for(host, &row).await;
    if project.is_null() {
        return Err(message(format!("No project in this environment is a checkout of {}, so its pull requests cannot be read here. Add one with t3_project_create.", args["repository"].as_str().unwrap_or(""))));
    }
    if name == "pull_request_create" {
        return create(host, &row, &project, args).await;
    }
    let branch = if row["branchPullRequest"].is_object() {
        Some(&row["branchPullRequest"])
    } else {
        rows(&row["pullRequests"]).iter().rev().find(|l| {
            l["source"] == "created"
                && (l["snapshot"].is_null() || l["snapshot"]["headBranch"] == row["branch"])
        })
    };
    let mut named = if args.get("number").is_some() {
        fields(args, &["repository", "number"])
    } else {
        fields(branch.ok_or_else(||message("This thread's branch has no known PR. Pass number, or open one with pull_request_create."))?,&["repository","number"])
    };
    if named["repository"].is_null() {
        if let Some((_, repo)) = project["repositoryIdentity"]["canonicalKey"]
            .as_str()
            .and_then(|k| k.split_once('/'))
        {
            named["repository"] = json!(repo)
        }
    }
    let identity = target::resolve(&named, &project["repositoryIdentity"])?;
    let mut reference = fields(&identity, &["host", "repository", "number"]);
    reference["projectId"] = row["projectId"].clone();
    let pr = Pr {
        host,
        identity,
        reference,
    };
    if args["number"].is_null()
        && row["branchPullRequest"].is_null()
        && branch.is_some_and(|l| l["snapshot"].is_null())
        && pr.rpc("summary", json!({})).await.map_err(message)?["headBranch"] != row["branch"]
    {
        return Err(message("This thread's branch has no known PR. Pass number, or open one with pull_request_create."));
    }
    match name {
        "pull_request_update" => pr.update(args).await,
        "pull_request_respond" => pr.respond(args).await,
        "pull_request_review" => pr.review(args).await,
        "pull_request_merge" => pr.merge(args).await,
        _ => unreachable!(),
    }
    .map_err(message)
}

async fn create<S: McpHost>(host: &Arc<S>, row: &Value, project: &Value, args: &Value) -> Outcome {
    let mut input = fields(args, &["title", "body", "base", "draft", "private"]);
    input["cwd"] = row
        .get("worktreePath")
        .filter(|p| p.is_string())
        .unwrap_or(&project["workspaceRoot"])
        .clone();
    let created = host
        .workspace_call("git.createPullRequest".into(), input)
        .await
        .map_err(|e| {
            message(
                e["message"]
                    .as_str()
                    .unwrap_or("Could not open the pull request."),
            )
        })?;
    if created["url"].is_null() {
        return Err(message(
            "The host accepted the PR but does not list it yet. Find it with pull_request_list.",
        ));
    }
    let mut result = target::resolve(&created, &project["repositoryIdentity"])?;
    if args["draft"] == true && created["draftOnCreate"] != true {
        let mut reference = fields(&result, &["host", "repository", "number"]);
        reference["projectId"] = row["projectId"].clone();
        let pr = Pr {
            host,
            identity: result.clone(),
            reference,
        };
        let draft = async {
            if created["created"] == true
                || pr.rpc("summary", json!({"allowStale":false})).await?["isDraft"] != true
            {
                pr.rpc("runAction", json!({"action":"draft"})).await?;
            }
            Ok::<_, String>(())
        }
        .await;
        draft.map_err(|e|message(format!("Git manager failed in createPullRequest: Opened the pull request, but could not make it a draft: {e}")))?;
    }
    if !visible(row).iter().any(|l| same(l, &result)) {
        let mut command = result.clone();
        command["source"] = json!("created");
        dispatch(
            host,
            row,
            "thread.pull-request.link",
            "mcp-pr-create",
            command,
            "Could not link the pull request.",
        )
        .await?;
        sync_link(host, row, &result).await;
    }
    result["created"] = created["created"].clone();
    if row["lineage"]["relationshipToParent"] == "subagent" {
        result["next"] = json!(REPORT)
    }
    Ok(result)
}
