//! The small activity projection used by the agent-facing pull-request reader.
//!
//! The provider's full activity API has more fields than the agent contract needs.  Keep this
//! query deliberately narrow so fake forge hosts and real GitHub installations see the same read.
use super::failure;
use crate::provider_rpc::{Failure, Github, Reply};
use serde_json::{json, Value};

fn actor(value: &Value) -> Value {
    let login = value["login"].as_str().filter(|s| !s.trim().is_empty());
    login.map_or(
        Value::Null,
        |login| json!({"login":login,"name":value["name"],"avatarUrl":value["avatarUrl"]}),
    )
}

pub(super) async fn read(gh: &Github<'_>, repository: &str, number: u64) -> Reply {
    let (owner, name) = repository
        .split_once('/')
        .ok_or_else(|| failure("activity", Failure::Invalid))?;
    let data = gh
        .graphql(
            include_str!("PullRequestActivity.graphql"),
            json!({"owner":owner,"name":name,"number":number}),
        )
        .await
        .map_err(|e| failure("activity", e))?;
    let pr = &data["repository"]["pullRequest"];
    if pr.is_null() {
        return Err(failure("activity", Failure::NotFound));
    }
    let mut comments = Vec::new();
    for comment in pr["comments"]["nodes"].as_array().into_iter().flatten() {
        if comment["isMinimized"] == true {
            continue;
        }
        comments.push(json!({
            "id": comment["id"],
            "kind": "issue-comment",
            "author": actor(&comment["author"]),
            "body": comment["body"],
            "createdAt": comment["createdAt"],
            "path": null,
            "reviewState": null,
        }));
    }
    for review in pr["reviews"]["nodes"].as_array().into_iter().flatten() {
        if review["isMinimized"] == true {
            continue;
        }
        comments.push(json!({
            "id": review["id"],
            "kind": "review",
            "author": actor(&review["author"]),
            "body": review["body"],
            "createdAt": review["submittedAt"],
            "path": null,
            "reviewState": review["state"],
        }));
    }
    comments.sort_by(|a, b| a["createdAt"].as_str().cmp(&b["createdAt"].as_str()));
    let comments_truncated = pr["comments"]["pageInfo"]["hasNextPage"] == true
        || pr["reviews"]["pageInfo"]["hasNextPage"] == true;
    Ok(json!({
        "comments": comments,
        "commentCount": comments.len(),
        "commentsTruncated": comments_truncated,
        "reviewThreads": [],
        "commits": [],
    }))
}
