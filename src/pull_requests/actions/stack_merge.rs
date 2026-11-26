use super::{error, provider_error, Github, Reply, OP};
use serde_json::{json, Value};
use tokio::time::{sleep, Duration, Instant};

fn stack_error(name: &str, detail: &str) -> Value {
    let mut err = error(OP, detail);
    err["cause"] = json!({"name":"PullRequestProviderError", "message":format!("github failed in runAction: {detail}"),
                          "cause":{"name":name, "message":detail}});
    err
}

fn invalid() -> Value {
    error(
        OP,
        "GitHub returned an unreadable stack operation response.",
    )
}

fn changed() -> Value {
    stack_error(
        "GitHubStackChangedError",
        "The stack changed. Refresh it before trying again.",
    )
}

fn unsupported() -> Value {
    stack_error(
        "GitHubStackUnsupportedError",
        "This operation is not supported for this stack.",
    )
}

async fn pinned_stack_head(
    gh: &Github<'_>,
    repo: &str,
    number: u64,
    stack_number: u64,
    input: &Value,
) -> Result<String, Value> {
    let stacks = gh
        .json(
            "GET",
            &format!("repos/{repo}/stacks?pull_request={number}"),
            None,
        )
        .await
        .map_err(provider_error)?;
    let stack = stacks
        .as_array()
        .ok_or_else(invalid)?
        .first()
        .ok_or_else(changed)?;
    if stack["number"] != stack_number {
        return Err(changed());
    }
    let layers = stack["pull_requests"]
        .as_array()
        .ok_or_else(invalid)?
        .iter()
        .map(super::super::discussion::stack_layer)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| invalid())?;
    let index = layers
        .iter()
        .position(|layer| layer["number"] == number)
        .ok_or_else(changed)?;
    let target = &layers[index];
    if target["state"] != "open" {
        return Err(unsupported());
    }
    let open: Vec<_> = layers[..=index]
        .iter()
        .filter(|l| l["state"] != "merged")
        .collect();
    let expected = input["expectedStackHeads"].as_array().ok_or_else(changed)?;
    if expected.len() != open.len()
        || open.iter().any(|layer| {
            !layer["headSha"].is_string()
                || !expected.iter().any(|pin| {
                    pin["number"] == layer["number"] && pin["headSha"] == layer["headSha"]
                })
        })
    {
        return Err(changed());
    }
    if open
        .iter()
        .any(|layer| layer["state"] != "open" || layer["isDraft"] == true)
    {
        return Err(unsupported());
    }
    target["headSha"]
        .as_str()
        .map(str::to_owned)
        .ok_or_else(changed)
}

async fn stack_node_id(
    service: &super::super::PullRequestService,
    gh: &Github<'_>,
    repo: &str,
    number: u64,
) -> Result<String, Value> {
    let (owner, name) = repo.split_once('/').ok_or_else(invalid)?;
    let vars = json!({"owner":owner,"name":name,"number":number});
    super::super::review::node_id(service, gh, repo, number, &vars, OP).await
}

pub(super) async fn run(
    service: &super::super::PullRequestService,
    gh: &Github<'_>,
    repo: &str,
    number: u64,
    stack_number: u64,
    input: &Value,
) -> Reply {
    if input["action"] == "disable-auto-merge" {
        // Cancelling applies to the layer on which auto-merge was armed; it needs no head pins.
        let id = stack_node_id(service, gh, repo, number).await?;
        let query = format!("mutation DisableStackAutoMerge($pullRequestId:ID!){{disablePullRequestAutoMerge(input:{{pullRequestId:$pullRequestId,includeStack:true}}){{pullRequest{{...PullRequestActionSummary}}}}}}\n{}", super::SUMMARY);
        let data = gh
            .graphql(&query, json!({"pullRequestId":id}))
            .await
            .map_err(provider_error)?;
        return Ok(data["disablePullRequestAutoMerge"]["pullRequest"].clone());
    }
    let mut head = pinned_stack_head(gh, repo, number, stack_number, input).await?;
    let method = input["mergeMethod"].as_str().unwrap_or("merge");
    if !matches!(method, "merge" | "squash" | "rebase") {
        return Err(error(
            OP,
            &format!("This host cannot merge with the {method} strategy."),
        ));
    }
    if input["action"] == "enable-auto-merge" {
        let node_id = stack_node_id(service, gh, repo, number).await?;
        let query = format!("mutation EnablePullRequestAutoMerge($input:EnablePullRequestAutoMergeInput!){{enablePullRequestAutoMerge(input:$input){{pullRequest{{...PullRequestActionSummary}}}}}}\n{}", super::SUMMARY);
        let mutation = json!({"operationName":"EnablePullRequestAutoMerge","query":query,"variables":{"input":{"pullRequestId":node_id,"expectedHeadOid":head,"includeStack":true,"mergeMethod":method.to_ascii_uppercase()}}});
        let result = gh
            .json("POST", "graphql", Some(mutation))
            .await
            .map_err(provider_error)?;
        if let Some(errors) = result["errors"].as_array() {
            if !errors.iter().any(|e| {
                e["message"]
                    .as_str()
                    .is_some_and(|m| m.contains("already mergeable"))
            }) {
                return Err(provider_error(super::Failure::Refused));
            }
            // The layers may have moved since auto-merge was requested.
            head = pinned_stack_head(gh, repo, number, stack_number, input).await?;
        } else {
            return Ok(result["data"]["enablePullRequestAutoMerge"]["pullRequest"].clone());
        }
    }
    let endpoint = format!("repos/{repo}/pulls/{number}/merge-async");
    let body = json!({"merge_method":method, "merge_action":"default", "sha":head});
    let mut result = gh
        .json("PUT", &endpoint, Some(body))
        .await
        .map_err(provider_error)?;
    let deadline = Instant::now() + Duration::from_secs(300);
    let mut delay = Duration::from_secs(1);
    loop {
        let details = result["details"].as_object().ok_or_else(invalid)?;
        if ["uuid", "message"]
            .iter()
            .any(|key| details.get(*key).is_some_and(|v| !v.is_string()))
        {
            return Err(invalid());
        }
        match result["status"].as_str() {
            Some("merged" | "enqueued") => return Ok(Value::Null),
            Some("failed") => return Err(stack_error("GitHubStackMergeRejectedError","GitHub refused the stack merge. Check the stack's branch rules and merge requirements.")),
            Some("pending") if Instant::now() >= deadline => return Err(stack_error("GitHubStackMergePendingError","The merge is still running on GitHub. Check its status there before submitting another request.")),
            Some("pending") => {}
            _ => return Err(invalid()),
        }
        let uuid = details
            .get("uuid")
            .and_then(Value::as_str)
            .filter(|id| !id.is_empty())
            .ok_or_else(invalid)?;
        let token = crate::asset_grant::encode_component(uuid);
        sleep(delay).await;
        delay = (delay * 2).min(Duration::from_secs(10));
        result = gh
            .json("GET", &format!("{endpoint}/{token}"), None)
            .await
            .map_err(provider_error)?;
    }
}
