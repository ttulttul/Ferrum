use super::{capabilities, error, nodes, trimmed, unsupported};
use crate::{provider_rpc::Reply, AppState, Project};
use regex::Regex;
use serde_json::{json, Value};
use std::sync::LazyLock;
use std::time::Duration;

mod model;
mod response;
pub(super) fn endpoint() -> String {
    std::env::var("T3CODE_LINEAR_API_URL")
        .unwrap_or_else(|_| "https://api.linear.app/graphql".to_owned())
}
pub(super) async fn probe(token: &str) -> Reply {
    Linear::new("viewer", Some(token))?
        .call("viewer", "T3Viewer", json!({}))
        .await
}
pub(super) async fn record(state: &AppState) -> Reply {
    let path = state.issues.lock().await.tracker_file.clone();
    super::trackers::read(&path).await.map_err(|_| {
        error(
            "listProjects",
            "The project's issue tracker binding could not be read.",
        )
    })
}

fn fail(op: &str, call: &str, reason: &str, detail: &str) -> Value {
    let cause =
        json!({"name":"IssueTrackerError","message":format!("linear failed in {call}: {detail}")});
    if reason == "unauthenticated" {
        return json!({"_tag":"IssueUnavailableError","reason":"unauthenticated","tracker":"linear","cause":cause});
    }
    let mut result = error(op, detail);
    if matches!(reason, "not-found" | "rate-limited") {
        result["reason"] = json!(reason);
    }
    result["cause"] = cause;
    result
}

struct Linear<'a> {
    op: &'a str,
    token: Option<&'a str>,
    client: reqwest::Client,
}
impl<'a> Linear<'a> {
    fn new(op: &'a str, token: Option<&'a str>) -> Result<Self, Value> {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(15))
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|_| fail(op, op, "failed", "Linear could not be reached."))?;
        Ok(Self { op, token, client })
    }
    async fn call(&self, call: &str, name: &str, variables: Value) -> Reply {
        let bad = |reason: &str, detail: &str| fail(self.op, call, reason, detail);
        let token = self
            .token
            .ok_or_else(|| bad("unauthenticated", "Linear is not connected on this server."))?;
        let payload =
            json!({"operationName":name,"query":response::query(name),"variables":variables});
        let mut response = self
            .client
            .post(endpoint())
            .header("authorization", token)
            .json(&payload)
            .send()
            .await
            .map_err(|_| bad("failed", "Linear could not be reached."))?;
        let status = response.status().as_u16();
        let reset = response
            .headers()
            .get("x-ratelimit-requests-reset")
            .or_else(|| response.headers().get("x-ratelimit-complexity-reset"))
            .and_then(|h| h.to_str().ok())
            .and_then(|s| s.trim().parse::<u64>().ok());
        let mut content = Vec::new();
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|_| bad("failed", "Linear could not be reached."))?
        {
            if content.len().saturating_add(chunk.len()) > 16 * 1024 * 1024 {
                return Err(bad("failed", "Linear returned an unexpected body."));
            }
            content.extend_from_slice(&chunk);
        }
        let body = serde_json::from_slice::<Value>(&content).unwrap_or(Value::Null);
        let errors = body["errors"].as_array();
        let has = |code: &str| {
            errors.is_some_and(|es| es.iter().any(|e| e["extensions"]["code"] == code))
        };
        if status == 429 || has("RATELIMITED") {
            let mut err = bad("rate-limited", "Linear's rate limit was reached.");
            if let Some(ms) = reset.filter(|n| {
                u128::from(*n)
                    > std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_millis()
            }) {
                let cause = err.as_object_mut().unwrap().shift_remove("cause");
                err["retryAt"] = json!(ms);
                err["cause"] = cause.unwrap_or_default();
            }
            return Err(err);
        }
        if matches!(status, 401 | 403) || has("AUTHENTICATION_ERROR") {
            return Err(bad("unauthenticated", "Linear rejected the API key."));
        }
        if let Some(e) = errors.and_then(|es| es.first()) {
            let msg = trimmed(&e["message"]).unwrap_or("Linear refused the request.");
            if call == "getIssue" && msg.to_lowercase().contains("not found") {
                return Ok(json!({"issue":null}));
            }
            return Err(bad("failed", msg));
        }
        if !(200..300).contains(&status) {
            return Err(bad(
                "failed",
                &format!("Linear returned HTTP {status} with an unexpected body."),
            ));
        }
        let data = &body["data"];
        response::decode(self.op, call, name, data)?;
        Ok(data.clone())
    }
    async fn team(&self, scope: &str, call: &str) -> Reply {
        let data = self
            .call("getTeam", "T3Team", json!({"key":scope.to_uppercase()}))
            .await?;
        if data["teams"]["nodes"][0].is_null() {
            return Err(fail(
                self.op,
                call,
                "failed",
                &format!("No Linear team has the key \"{scope}\"."),
            ));
        }
        Ok(data)
    }
    async fn issue(&self, scope: &str, key: &str, call: &str, read: bool) -> Reply {
        let id = identifier(self.op, call, key)?;
        let result = self
            .call(
                "getIssue",
                "T3Issue",
                json!({"id":id,"comments":if read{50}else{1}}),
            )
            .await?;
        let issue = &result["issue"];
        if issue.is_null() {
            return Err(not_found(self.op, call));
        }
        let team = issue["team"]["key"].as_str().ok_or_else(|| {
            fail(
                self.op,
                call,
                "failed",
                "Linear returned an unexpected body.",
            )
        })?;
        if !read && team != scope.to_uppercase() {
            return Err(fail(
                self.op,
                call,
                "failed",
                &format!(
                    "{} belongs to Linear team {team}, not {}.",
                    issue["identifier"].as_str().unwrap_or(key),
                    scope.to_uppercase()
                ),
            ));
        }
        Ok(issue.clone())
    }
    async fn viewer(&self) -> Value {
        self.call("viewer", "T3Viewer", json!({}))
            .await
            .ok()
            .map(|v| v["viewer"].clone())
            .unwrap_or_default()
    }
    async fn mutate(&self, call: &str, query: &str, field: &str, vars: Value) -> Reply {
        let result = self.call(call, query, vars).await?;
        if result[field]["success"] != true {
            return Err(fail(
                self.op,
                call,
                "failed",
                "Linear did not apply the change.",
            ));
        }
        Ok(Value::Null)
    }
}

fn not_found(op: &str, call: &str) -> Value {
    fail(
        op,
        call,
        "not-found",
        "Linear has no issue by that identifier, or this account cannot see it.",
    )
}
fn identifier(op: &str, call: &str, key: &str) -> Result<String, Value> {
    static ID: LazyLock<Regex> =
        LazyLock::new(|| Regex::new(r"^[A-Za-z][A-Za-z0-9]*-[1-9][0-9]*$").unwrap());
    if !ID.is_match(key.trim()) {
        return Err(not_found(op, call));
    }
    Ok(key.trim().to_ascii_uppercase())
}
fn labels(scope: &Value) -> impl Iterator<Item = &Value> {
    nodes(&scope["teams"]["nodes"][0]["labels"]).chain(nodes(&scope["issueLabels"]))
}
fn issue_fields(op: &str, scope: &Value, input: &Value) -> Reply {
    let mut fields = json!({});
    if let Some(title) = input.get("title") {
        fields["title"] = title.clone();
    }
    if let Some(body) = input.get("body") {
        fields["description"] = body.clone();
    }
    if let Some(wanted) = input["labels"].as_array() {
        let ids = wanted
            .iter()
            .map(|v| {
                let name = v.as_str().unwrap_or("");
                labels(scope)
                    .find(|l| {
                        trimmed(&l["name"])
                            .is_some_and(|s| s.to_lowercase() == name.trim().to_lowercase())
                    })
                    .map(|l| l["id"].clone())
                    .ok_or_else(|| {
                        fail(
                            op,
                            op,
                            "failed",
                            &format!("Linear has no label named \"{name}\" for this team."),
                        )
                    })
            })
            .collect::<Result<Vec<_>, _>>()?;
        fields["labelIds"] = json!(ids);
    }
    if let Some(members) = input["assignees"].as_array() {
        match members.as_slice() {
            [] => {
                if op == "update" {
                    fields["assigneeId"] = Value::Null;
                }
            }
            [member] => {
                let name = member.as_str().unwrap_or("");
                let selected = nodes(&scope["teams"]["nodes"][0]["members"]).find(|m| {
                    m["id"] == name
                        || ["email", "displayName"].iter().any(|k| {
                            m[*k]
                                .as_str()
                                .is_some_and(|v| v.to_lowercase() == name.to_lowercase())
                        })
                });
                fields["assigneeId"] = selected.ok_or_else(|| {
                    fail(
                        op,
                        op,
                        "failed",
                        &format!("\"{name}\" is not a member of this Linear team."),
                    )
                })?["id"]
                    .clone();
            }
            _ => {
                return Err(fail(
                    op,
                    op,
                    "failed",
                    "A Linear issue has one assignee at most.",
                ))
            }
        }
    }
    Ok(fields)
}

pub(super) async fn run(state: &AppState, p: &Project, op: &str, input: &Value) -> Reply {
    let cfg = record(state).await?;
    let binding = &cfg["bindings"][&p.id];
    if binding["tracker"] != "linear"
        || input["host"]
            .as_str()
            .is_some_and(|h| !h.trim().eq_ignore_ascii_case("linear.app"))
    {
        return Err(unsupported());
    }
    let scope = trimmed(&input["scope"]).unwrap_or("");
    let writable = binding["scope"]
        .as_str()
        .is_some_and(|t| t.eq_ignore_ascii_case(scope));
    if !matches!(op, "detail" | "comments" | "candidates" | "templates") && !writable {
        return Err(
            json!({"_tag":"IssueOperationError","operation":op,"detail":"This issue belongs to a repository outside the selected project and cannot be changed from here.","reason":"read-only"}),
        );
    }
    let api = Linear::new(op, super::trackers::binding_token(&cfg, binding))?;
    let key = input["key"].as_str().unwrap_or("");
    match op {
        "detail" => {
            let issue = api.issue(scope, key, "getIssue", true).await?;
            let viewer = api.viewer().await;
            model::detail(&issue, p, scope, writable, &viewer)
        }
        "comments" => {
            let id = identifier(op, "getComments", key)?;
            let data = api
                .call(
                    "getComments",
                    "T3Comments",
                    json!({"id":id,"after":input["cursor"],"first":50}),
                )
                .await?;
            if data["issue"].is_null() {
                return Err(fail(
                    op,
                    "getComments",
                    "not-found",
                    "This issue no longer exists.",
                ));
            }
            Ok(model::comments(
                &data["issue"]["comments"],
                &api.viewer().await,
            ))
        }
        "candidates" | "templates" => {
            let team = api
                .team(
                    scope,
                    if op == "candidates" {
                        "listCandidates"
                    } else {
                        "listTemplates"
                    },
                )
                .await?;
            Ok(if op == "candidates" {
                model::candidates(&team)
            } else {
                model::templates(&team)
            })
        }
        "create" => {
            let team = api.team(scope, "create").await?;
            let mut fields = issue_fields(op, &team, input)?;
            fields["teamId"] = team["teams"]["nodes"][0]["id"].clone();
            let data = api
                .call("createIssue", "T3IssueCreate", json!({"input":fields}))
                .await?;
            let created = &data["issueCreate"]["issue"];
            if data["issueCreate"]["success"] != true
                || !created["identifier"].is_string()
                || !created["url"].is_string()
            {
                return Err(fail(
                    op,
                    "createIssue",
                    "failed",
                    "Linear did not create the issue.",
                ));
            }
            Ok(
                json!({"key":created["identifier"],"displayKey":created["identifier"],"url":created["url"]}),
            )
        }
        "comment" => {
            if trimmed(&input["body"]).is_none() {
                return Err(error(op, "A comment needs some text."));
            }
            let issue = api.issue(scope, key, op, false).await?;
            api.mutate(
                "createComment",
                "T3CommentCreate",
                "commentCreate",
                json!({"input":{"issueId":issue["id"],"body":input["body"]}}),
            )
            .await
        }
        "update" | "setState" => {
            if op == "update"
                && ["title", "body", "labels", "assignees"]
                    .iter()
                    .all(|k| input.get(k).is_none())
            {
                return Err(error(op, "Nothing to change."));
            }
            let issue = api.issue(scope, key, op, false).await?;
            let team = if op == "setState"
                || input.get("labels").is_some()
                || input.get("assignees").is_some()
            {
                api.team(scope, op).await?
            } else {
                Value::Null
            };
            let fields = if op == "setState" {
                let name = input["state"].as_str().unwrap_or("");
                let s = nodes(&team["teams"]["nodes"][0]["states"])
                    .find(|n| n["name"] == name)
                    .ok_or_else(|| {
                        fail(
                            op,
                            op,
                            "failed",
                            &format!("This team's workflow has no \"{name}\" state."),
                        )
                    })?;
                json!({"stateId":s["id"]})
            } else {
                issue_fields(op, &team, input)?
            };
            api.mutate(
                "updateIssue",
                "T3IssueUpdate",
                "issueUpdate",
                json!({"id":issue["identifier"],"input":fields}),
            )
            .await
        }
        "react" => {
            let issue = api.issue(scope, key, "setReaction", false).await?;
            let add = input["reacted"] == true;
            let subject = input["subjectId"].as_str();
            let target = if let Some(id) = subject {
                json!({"commentId":id})
            } else {
                json!({"issueId":issue["id"]})
            };
            let reactions = if subject.is_some() || !add {
                let (query, id) = if let Some(s) = subject {
                    ("T3CommentReactions", json!(s))
                } else {
                    ("T3IssueReactions", issue["id"].clone())
                };
                let data = api.call("listReactions", query, json!({"id":id})).await?;
                if subject.is_some()
                    && (issue["id"].is_null() || data["comment"]["issue"]["id"] != issue["id"])
                {
                    return Err(fail(
                        op,
                        "setReaction",
                        "failed",
                        "The comment does not belong to the selected issue.",
                    ));
                }
                data[if subject.is_some() {
                    "comment"
                } else {
                    "issue"
                }]["reactions"]
                    .clone()
            } else {
                Value::Null
            };
            let content = input["content"].as_str().unwrap_or("");
            if add {
                let mut target = target;
                target["emoji"] = json!(
                    model::EMOJIS
                        .iter()
                        .find(|(c, _, _)| *c == content)
                        .ok_or_else(|| error(op, "Invalid reaction."))?
                        .1
                );
                return api
                    .mutate(
                        "createReaction",
                        "T3ReactionCreate",
                        "reactionCreate",
                        json!({"input":target}),
                    )
                    .await;
            }
            let viewer = api.viewer().await;
            if let Some(r) = reactions.as_array().into_iter().flatten().find(|r| {
                model::content(&r["emoji"]) == Some(content)
                    && viewer["id"].is_string()
                    && r["user"]["id"] == viewer["id"]
            }) {
                return api
                    .mutate(
                        "deleteReaction",
                        "T3ReactionDelete",
                        "reactionDelete",
                        json!({"id":r["id"]}),
                    )
                    .await;
            }
            Ok(Value::Null)
        }
        _ => Err(unsupported()),
    }
}

/// `viewerKey`: an added account reads its own viewer.
pub(super) fn viewer_key(binding: &Value) -> String {
    match binding["accountId"].as_str() {
        None => "linear.app".to_owned(),
        Some(id) => format!("linear.app {id}"),
    }
}

pub(super) async fn page(
    cfg: &Value,
    project: &Project,
    scope: &str,
    input: &Value,
    cursor: &Value,
    viewers: &mut Value,
) -> Reply {
    let binding = &cfg["bindings"][&project.id];
    let api = Linear::new("list", super::trackers::binding_token(cfg, binding))?;
    let viewer = viewer_key(binding);
    let mut filter = json!({"team":{"key":{"eq":scope.trim().to_uppercase()}}});
    if let Some(mode @ ("assigned" | "authored" | "mentioned")) = input["involvement"].as_str() {
        if viewers[&viewer].is_null() {
            let data = api.call("viewer", "T3Viewer", json!({})).await?;
            viewers[&viewer] = json!(trimmed(&data["viewer"]["displayName"])
                .or_else(|| trimmed(&data["viewer"]["id"]))
                .ok_or_else(|| fail(
                    "list",
                    "viewer",
                    "failed",
                    "Linear returned an unexpected body."
                ))?);
        }
        let handle = json!({"displayName":{"eq":viewers[&viewer]}});
        match mode {
            "assigned" => filter["assignee"] = handle,
            "authored" => filter["creator"] = handle,
            _ => filter["subscribers"] = json!({"some":handle}),
        }
    }
    if let Some(s @ ("open" | "closed")) = input["state"].as_str() {
        let field = if s == "open" { "nin" } else { "in" };
        filter["state"] = json!({"type":{field:["completed","canceled","duplicate"]}});
    }
    let mut clauses = Vec::new();
    if let Some(assignee) = input["assignee"].as_str() {
        clauses.push(json!({"assignee":{"displayName":{"eq":assignee}}}));
    }
    for label in input["labels"].as_array().into_iter().flatten() {
        clauses.push(json!({"labels":{"some":{"name":{"eq":label}}}}));
    }
    if !clauses.is_empty() {
        filter["and"] = json!(clauses);
    }
    if let Some(query) = input["query"].as_str() {
        let mut terms = vec![
            json!({"title":{"containsIgnoreCase":query}}),
            json!({"description":{"containsIgnoreCase":query}}),
        ];
        static NUMBER: LazyLock<Regex> =
            LazyLock::new(|| Regex::new(r"^(?:[A-Za-z][A-Za-z0-9]*-|#)?([0-9]+)$").unwrap());
        if let Some(n) = NUMBER
            .captures(query.trim())
            .and_then(|c| c[1].parse::<u64>().ok())
        {
            terms.push(json!({"number":{"eq":n}}));
        }
        filter["or"] = json!(terms);
    }
    let vars = json!({"first":input["limit"].as_u64().unwrap_or(50),"after":cursor,"filter":filter,"orderBy":if input["sort"]=="created"{"createdAt"}else{"updatedAt"}});
    let data = api.call("listIssues", "T3Issues", vars).await?;
    let page = &data["issues"];
    let entries = nodes(page)
        .map(|issue| model::summary(issue, project, scope, 0))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(
        json!({"entries":entries,"cursor":if page["pageInfo"]["hasNextPage"]==true{page["pageInfo"]["endCursor"].clone()}else{Value::Null}}),
    )
}
