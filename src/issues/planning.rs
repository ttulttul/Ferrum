use super::{failed, nodes};
use crate::provider_rpc::{Failure, Github, Reply, RequestError};
use serde_json::{json, Value};
use std::sync::LazyLock;

const DETAIL: &str = concat!(
    include_str!("graphql/IssueDetail.graphql"),
    include_str!("graphql/IssueDetailFields.graphql")
);
const DETAIL_WITH_STATUS: &str = concat!(
    include_str!("graphql/IssueDetailWithStatus.graphql"),
    include_str!("graphql/IssueDetailFields.graphql"),
    include_str!("graphql/PlanningStatusFields.graphql")
);
const SEARCH: &str = concat!(
    include_str!("graphql/IssueSearch.graphql"),
    include_str!("graphql/IssueSearchFields.graphql")
);
const SEARCH_WITH_STATUS: &str = concat!(
    include_str!("graphql/IssueSearchWithStatus.graphql"),
    include_str!("graphql/IssueSearchFields.graphql"),
    include_str!("graphql/PlanningStatusFields.graphql")
);
const STATUSES: &str = concat!(
    include_str!("graphql/IssueStatuses.graphql"),
    include_str!("graphql/PlanningStatusFields.graphql")
);

async fn preview(gh: &Github<'_>, query: &str, variables: Value) -> Result<Option<Value>, Failure> {
    static STATUS_FIELD: LazyLock<regex::Regex> = LazyLock::new(|| {
        regex::Regex::new(r"(?i)planningStatus|viewerCanSetStatus|\bstatus\b").unwrap()
    });
    let refused = |error: &RequestError| {
        error.kind == Failure::Refused
            && error
                .response
                .as_ref()
                .and_then(|r| r["errors"].as_array())
                .is_some_and(|errors| {
                    errors
                        .iter()
                        .filter_map(|e| e["message"].as_str())
                        .any(|m| STATUS_FIELD.is_match(m))
                })
    };
    match gh.graphql_with_response(query, variables).await {
        Ok(data) => Ok(Some(data)),
        Err(e) if refused(&e) => Ok(None),
        Err(e) => Err(e.kind),
    }
}

async fn read(
    gh: &Github<'_>,
    variables: Value,
    preview_query: &str,
    legacy: &str,
) -> Result<Value, Failure> {
    match preview(gh, preview_query, variables.clone()).await? {
        Some(data) => Ok(data),
        None => gh.graphql(legacy, variables).await,
    }
}

pub(super) async fn detail(gh: &Github<'_>, vars: Value) -> Result<Value, Failure> {
    read(gh, vars, DETAIL_WITH_STATUS, DETAIL).await
}
pub(super) async fn search(gh: &Github<'_>, vars: Value) -> Result<Value, Failure> {
    read(gh, vars, SEARCH_WITH_STATUS, SEARCH).await
}

pub(super) fn state(status: &Value) -> Value {
    let closed = status["category"]["state"] == "CLOSED";
    let mut result =
        json!({"category":if closed { "closed" } else { "open" }, "name": status["name"]});
    if closed {
        if let Some(reason) = status["category"]["stateReason"]
            .as_str()
            .filter(|reason| *reason != "REOPENED")
        {
            result["closeReason"] = json!(reason.to_lowercase().replace('_', "-"));
        }
    }
    result
}

pub(super) fn legacy_states() -> Value {
    json!([{"category":"open","name":"Open"},{"category":"closed","name":"Completed","closeReason":"completed"},{"category":"closed","name":"Not planned","closeReason":"not-planned"},{"category":"closed","name":"Duplicate","closeReason":"duplicate"}])
}

fn invalid_state(detail: &str) -> Value {
    json!({"_tag": "IssueOperationError", "operation": "setState", "detail": detail,
           "cause": {"name": "IssueTrackerError", "message": format!("github failed in setState: {detail}")}})
}

pub(super) async fn set_state(
    gh: &Github<'_>,
    variables: Value,
    issue: &Value,
    id: &str,
    item: &str,
    name: &str,
) -> Reply {
    let fail = |e| failed("setState", e);
    let data = preview(gh, STATUSES, variables)
        .await
        .map_err(fail)?
        .unwrap_or_default();
    let choices = &data["repository"]["planningStatuses"];
    let wanted = if let Some(status) = nodes(choices).find(|status| status["name"] == name) {
        // Choosing an original issue is unavailable here. REST can still close as duplicate.
        if status["category"]["key"] != "duplicate" {
            gh.graphql(
                include_str!("graphql/SetIssueStatus.graphql"),
                json!({"issueId":id,"statusId":status["id"]}),
            )
            .await
            .map_err(fail)?;
            return Ok(Value::Null);
        }
        state(status)
    } else if nodes(choices).next().is_some() {
        return Err(invalid_state(&format!(
            r#"This repository has no "{name}" issue status."#
        )));
    } else {
        legacy_states()
            .as_array()
            .unwrap()
            .iter()
            .find(|state| state["name"] == name)
            .cloned()
            .ok_or_else(|| invalid_state(&format!(r#"A GitHub issue has no "{name}" state."#)))?
    };
    let mut body = json!({"state": wanted["category"]});
    if let Some(reason) = wanted["closeReason"].as_str() {
        let reason = reason.replace('-', "_");
        if issue["state"] == "CLOSED"
            && !issue["stateReason"]
                .as_str()
                .is_some_and(|s| s.eq_ignore_ascii_case(&reason))
        {
            return Err(invalid_state("GitHub cannot change why a closed issue was closed. Reopen it, then close it again."));
        }
        body["state_reason"] = json!(reason);
    }
    gh.json("PATCH", item, Some(body)).await.map_err(fail)?;
    Ok(Value::Null)
}
