use super::{error, failure};
use crate::provider_rpc::{Failure, Github, Reply};
use serde_json::{json, Value};

fn trimmed(v: &Value) -> Option<&str> {
    v.as_str().map(str::trim).filter(|s| !s.is_empty())
}

fn stack_layer(pr: &Value) -> Reply {
    let invalid = || failure("stack", Failure::Invalid);
    let mut layer = json!({"number":pr["number"].as_u64().ok_or_else(invalid)?});
    if let Some(t) = pr["title"].as_str() {
        layer["title"] = json!(t);
    }
    if let Some(draft) = pr["draft"].as_bool() {
        layer["isDraft"] = json!(draft);
    }
    if let Some(sha) = trimmed(&pr["head"]["sha"]) {
        layer["headSha"] = json!(sha);
    }
    layer["headBranch"] = json!(trimmed(&pr["head"]["ref"]).ok_or_else(invalid)?);
    layer["state"] = json!(if trimmed(&pr["merged_at"]).is_some() {
        "merged"
    } else {
        match trimmed(&pr["state"])
            .unwrap_or("")
            .to_ascii_uppercase()
            .as_str()
        {
            "CLOSED" => "closed",
            "MERGED" => "merged",
            _ => "open",
        }
    });
    Ok(layer)
}

pub(super) async fn stack(gh: &Github<'_>, repo: &str, number: u64) -> Reply {
    let base = format!("repos/{repo}/stacks");
    let data = match gh
        .json("GET", &format!("{base}?pull_request={number}"), None)
        .await
    {
        Ok(data) => data,
        Err(Failure::NotFound) => return Ok(Value::Null),
        Err(e) => return Err(failure("stack", e)),
    };
    let invalid = || failure("stack", Failure::Invalid);
    let Some(first) = data.as_array().ok_or_else(invalid)?.first() else {
        return Ok(Value::Null);
    };
    let n = first["number"].as_u64().ok_or_else(invalid)?;
    let details = match gh.json("GET", &format!("{base}/{n}"), None).await {
        Ok(d) => d,
        Err(Failure::NotFound) => return Ok(Value::Null),
        Err(e) => return Err(failure("stack", e)),
    };
    let layers = details["pull_requests"]
        .as_array()
        .ok_or_else(invalid)?
        .iter()
        .map(stack_layer)
        .collect::<Result<Vec<_>, _>>()?;
    let number = details["number"].as_u64().ok_or_else(invalid)?;
    let id = match &details["id"] {
        Value::Number(n) => n.to_string(),
        Value::String(s) => s.clone(),
        Value::Null => trimmed(&details["node_id"])
            .map(str::to_owned)
            .unwrap_or_else(|| number.to_string()),
        _ => return Err(invalid()),
    };
    let base = trimmed(&details["base"])
        .or_else(|| trimmed(&details["base"]["ref"]))
        .ok_or_else(invalid)?;
    let url = trimmed(&details["html_url"])
        .or_else(|| trimmed(&details["url"]))
        .ok_or_else(invalid)?;
    Ok(json!({"id":id,"number":number,"url":url,"base":base,"layers":layers}))
}

fn reactions(groups: &Value, viewer: Option<&str>) -> Vec<Value> {
    let mut items = Vec::new();
    for g in groups.as_array().into_iter().flatten() {
        let content = match trimmed(&g["content"])
            .unwrap_or("")
            .to_ascii_uppercase()
            .as_str()
        {
            "THUMBS_UP" => "thumbs-up",
            "THUMBS_DOWN" => "thumbs-down",
            "LAUGH" => "laugh",
            "HOORAY" => "hooray",
            "CONFUSED" => "confused",
            "HEART" => "heart",
            "ROCKET" => "rocket",
            "EYES" => "eyes",
            _ => continue,
        };
        let mut actors = g["reactors"]["nodes"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|a| trimmed(&a["login"]))
            .collect::<Vec<_>>();
        let count = g["reactors"]["totalCount"]
            .as_u64()
            .unwrap_or_default()
            .max(actors.len() as u64);
        if count == 0 {
            continue;
        }
        if let Some(viewer) = viewer {
            actors.retain(|a| !a.eq_ignore_ascii_case(viewer));
        }
        items.push(json!({"content":content,"count":count,"actors":actors,"viewerHasReacted":g["viewerHasReacted"]==true}));
    }
    items
}

pub(super) async fn comments(gh: &Github<'_>, repo: &str, number: u64, input: &Value) -> Reply {
    const OP: &str = "threadComments";
    let (owner, name) = repo.split_once('/').unwrap();
    let thread_id =
        trimmed(&input["threadId"]).ok_or_else(|| error(OP, "A thread ID is required."))?;
    let after = trimmed(&input["cursor"]).ok_or_else(|| error(OP, "A cursor is required."))?;
    let vars =
        json!({"owner":owner,"name":name,"number":number,"threadId":thread_id,"after":after});
    let data = gh
        .graphql(include_str!("ReviewThreadComments.graphql"), vars)
        .await
        .map_err(|e| failure(OP, e))?;
    let node = &data["node"];
    let pr = trimmed(&data["repository"]["pullRequest"]["id"]);
    if pr.is_none()
        || node["__typename"] != "PullRequestReviewThread"
        || pr != trimmed(&node["pullRequest"]["id"])
    {
        let msg = "The named subject did not belong to the named pull request.";
        return Err(
            json!({"_tag":"PullRequestOperationError","operation":OP,"detail":msg,
          "cause":{"name":"PullRequestProviderError","message":format!("github failed in getReviewThreadComments: {msg}"),"cause":{"name":"GitHubSubjectScopeError","message":msg}}}),
        );
    }
    let mut hidden = 0;
    let mut comments = Vec::new();
    for c in node["comments"]["nodes"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|c| !c.is_null())
    {
        if c["isMinimized"] == true {
            hidden += 1;
            continue;
        }
        let actor = &c["author"];
        let author = trimmed(&actor["login"]).map(|login| {
            let mut a = json!({"login":login,"name":null,"avatarUrl":trimmed(&actor["avatarUrl"])});
            if actor["__typename"] == "Bot" {
                a["isBot"] = json!(true);
            }
            a
        });
        let mut item = json!({"id":c["id"],"author":author,"body":c["body"],"createdAt":c["createdAt"],"editedAt":c["lastEditedAt"],"url":trimmed(&c["url"])});
        if let Some(review) = trimmed(&c["pullRequestReview"]["url"]) {
            item["reviewUrl"] = json!(review);
        }
        item["reactions"] = json!(reactions(
            &c["reactionGroups"],
            trimmed(&data["viewer"]["login"])
        ));
        comments.push(item);
    }
    let info = &node["comments"]["pageInfo"];
    let next = if info["hasNextPage"] == true {
        trimmed(&info["endCursor"])
    } else {
        None
    };
    Ok(json!({"comments":comments,"nextCursor":next,"hiddenCount":hidden}))
}
