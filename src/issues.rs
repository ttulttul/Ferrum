use crate::{
    provider_rpc::{github_remote, project, valid_repository, Failure, Github, Reply},
    AppState,
};
use serde_json::{json, Value};
use std::{collections::BTreeSet, path::Path};

#[derive(Debug)]
pub struct IssueService {
    pub(crate) gh: std::path::PathBuf,
    tracker_file: std::path::PathBuf,
}

impl IssueService {
    pub fn new(dir: std::path::PathBuf) -> Self {
        Self {
            gh: "gh".into(),
            tracker_file: dir.join("issue-trackers.json"),
        }
    }
}

mod linear;
mod reads;
mod templates;
mod trackers;
pub(crate) use trackers::dispatch as tracker_dispatch;

fn error(op: &str, detail: &str) -> Value {
    json!({"_tag":"IssueOperationError","operation":op,"detail":detail})
}
fn unsupported() -> Value {
    json!({"_tag":"IssueUnavailableError","reason":"tracker-unsupported"})
}
fn failed(op: &str, f: Failure) -> Value {
    match f {
        Failure::Missing | Failure::Auth => {
            json!({"_tag":"IssueUnavailableError","reason":if f==Failure::Missing {"tool-missing"}else{"unauthenticated"},"tracker":"github"})
        }
        Failure::NotFound => {
            json!({"_tag":"IssueOperationError","operation":op,"detail":"GitHub could not find the requested issue.","reason":"not-found"})
        }
        Failure::RateLimit => {
            json!({"_tag":"IssueOperationError","operation":op,"detail":"GitHub API rate limit exceeded.","reason":"rate-limited"})
        }
        Failure::Invalid => error(op, &format!("GitHub returned an unreadable {op} response.")),
        Failure::Refused => error(op, "The GitHub issue request failed."),
    }
}

pub(crate) async fn dispatch(
    state: &AppState,
    cli: &Path,
    method: &str,
    input: &Value,
) -> Option<Reply> {
    let op = method.strip_prefix("issues.")?;
    match op {
        "detail" | "comments" | "templates" | "candidates" | "create" | "update" | "comment"
        | "setState" | "react" => Some(run(state, cli, op, input).await),
        "list" => Some(list(state, cli, input).await),
        _ => None,
    }
}

/// The ordered tracker bindings available to MCP. Resolving a key does not
/// query the tracker or list issues on the host.
pub(crate) async fn project_scopes(state: &AppState) -> Reply {
    let config = linear::record(state).await?;
    let mut projects: Vec<_> = state
        .store
        .read()
        .await
        .projects
        .values()
        .cloned()
        .collect();
    projects.sort_by_key(|p| (p.created_at, p.id.clone()));
    let mut scopes = Vec::new();
    for p in projects {
        let binding = &config["bindings"][&p.id];
        if binding["tracker"] == "linear" {
            scopes.push(json!({"projectId":p.id,"tracker":"linear","host":"linear.app","scope":binding["scope"]}));
        }
        if let Some(scope) = github_remote(&p).await {
            scopes.push(
                json!({"projectId":p.id,"tracker":"github","host":"github.com","scope":scope}),
            );
        }
    }
    Ok(Value::Array(scopes))
}

fn selected_fields(input: &Value, keys: &[&str]) -> Value {
    Value::Object(
        keys.iter()
            .filter_map(|key| input.get(*key).map(|v| ((*key).to_owned(), v.clone())))
            .collect(),
    )
}

async fn run(state: &AppState, cli: &Path, op: &str, input: &Value) -> Reply {
    let project = project(state, input).await.ok_or_else(unsupported)?;
    if input["tracker"] == "linear" {
        return linear::run(state, &project, op, input).await;
    }
    let remote = github_remote(&project).await.ok_or_else(unsupported)?;
    if input["tracker"] != "github"
        || input["host"]
            .as_str()
            .is_some_and(|host| !host.trim().eq_ignore_ascii_case("github.com"))
    {
        return Err(unsupported());
    }
    let scope = input["scope"].as_str().unwrap_or("").trim();
    let read = matches!(op, "candidates" | "detail" | "comments" | "templates");
    if !read && !scope.eq_ignore_ascii_case(&remote) {
        return Err(
            json!({"_tag":"IssueOperationError","operation":op,"detail":"This issue belongs to a repository outside the selected project and cannot be changed from here.","reason":"read-only"}),
        );
    }
    if !valid_repository(scope) {
        return Err(error(
            op,
            "A repository was named that GitHub cannot address.",
        ));
    }
    let gh = Github::new(cli, &project.path);
    let (owner, name) = scope.split_once('/').unwrap();
    let endpoint = format!("repos/{scope}/issues");
    let fail = |f| failed(op, f);
    if matches!(op, "detail" | "comments" | "templates") {
        return reads::read(
            &gh,
            &project,
            op,
            scope,
            scope.eq_ignore_ascii_case(&remote),
            input,
        )
        .await;
    }
    if op == "candidates" {
        let raw=gh.graphql("query($owner:String!,$name:String!){repository(owner:$owner,name:$name){labels(first:100){pageInfo{hasNextPage} nodes{name color description}} assignableUsers(first:100){pageInfo{hasNextPage} nodes{login name avatarUrl}}}}",json!({"owner":owner,"name":name})).await.map_err(fail)?;
        let repo = &raw["repository"];
        if repo.is_null() {
            return Err(fail(Failure::NotFound));
        }
        let labels = nodes(&repo["labels"])
            .filter_map(|v| {
                v["name"].as_str().map(
                    |name| json!({"name":name,"color":v["color"],"description":v["description"]}),
                )
            })
            .collect::<Vec<_>>();
        let assignees = nodes(&repo["assignableUsers"])
            .filter_map(actor)
            .map(|mut a| {
                a["id"] = a["login"].clone();
                a
            })
            .collect::<Vec<_>>();
        return Ok(
            json!({"labels":labels,"assignees":assignees,"truncated":repo["labels"]["pageInfo"]["hasNextPage"]==true || repo["assignableUsers"]["pageInfo"]["hasNextPage"]==true}),
        );
    }
    let fields = selected_fields(input, &["title", "body", "labels", "assignees"]);
    if op == "create" {
        if input["title"]
            .as_str()
            .is_none_or(|title| title.trim().is_empty())
            || !input["body"].is_string()
            || !input["labels"].is_array()
            || !input["assignees"].is_array()
        {
            return Err(error(op, "Invalid issue creation input."));
        }
        let result = gh
            .json("POST", &endpoint, Some(fields))
            .await
            .map_err(fail)?;
        let n = result["number"]
            .as_i64()
            .ok_or_else(|| fail(Failure::Invalid))?;
        let url = result["html_url"]
            .as_str()
            .filter(|s| !s.trim().is_empty())
            .ok_or_else(|| fail(Failure::Invalid))?;
        return Ok(json!({"key":n.to_string(),"displayKey":format!("#{n}"),"url":url.trim()}));
    }
    if op == "comment" && input["body"].as_str().is_none_or(|b| b.trim().is_empty()) {
        return Err(error(op, "A comment needs some text."));
    }
    if op == "update" && fields.as_object().is_none_or(|f| f.is_empty()) {
        return Err(error(op, "Nothing to change."));
    }
    let key = input["key"].as_str().unwrap_or("");
    let number = key
        .parse::<i32>()
        .ok()
        .filter(|n| *n > 0 && n.to_string() == key)
        .ok_or_else(|| fail(Failure::NotFound))?;
    let mut vars = json!({"owner":owner,"name":name,"number":number});
    let subject = input.get("subjectId").and_then(Value::as_str);
    let query = if op == "react" && subject.is_some() {
        vars["commentId"] = json!(subject);
        "query($owner:String!,$name:String!,$number:Int!,$commentId:ID!){repository(owner:$owner,name:$name){issue(number:$number){id}} node(id:$commentId){__typename ... on IssueComment{issue{id}}}}"
    } else {
        "query($owner:String!,$name:String!,$number:Int!){repository(owner:$owner,name:$name){issue(number:$number){id state stateReason}}}"
    };
    let data = gh.graphql(query, vars).await.map_err(fail)?;
    let issue = &data["repository"]["issue"];
    let id = issue["id"]
        .as_str()
        .ok_or_else(|| fail(Failure::NotFound))?;
    let item = format!("{endpoint}/{number}");
    match op {
        "update" => {
            let result = gh.json("PATCH", &item, Some(fields)).await.map_err(fail)?;
            if result["labels"].is_array() && result["assignees"].is_array() {
                for (field, key) in [("labels", "name"), ("assignees", "login")] {
                    if let Some(wanted) = input[field].as_array() {
                        let names = |values: &[Value]| {
                            values
                                .iter()
                                .filter_map(|v| v.as_str().or_else(|| v[key].as_str()))
                                .map(|s| s.trim().to_lowercase())
                                .collect::<BTreeSet<_>>()
                        };
                        if names(wanted) != names(result[field].as_array().unwrap()) {
                            return Err(error(op,"GitHub did not apply the labels or assignees. Changing them needs triage access to the repository."));
                        }
                    }
                }
            }
        }
        "comment" => {
            gh.json(
                "POST",
                &format!("{item}/comments"),
                Some(selected_fields(input, &["body"])),
            )
            .await
            .map_err(fail)?;
        }
        "setState" => {
            let state = input["state"].as_str().unwrap_or("");
            let reason = match state {
                "Open" => None,
                "Completed" => Some("completed"),
                "Not planned" => Some("not_planned"),
                "Duplicate" => Some("duplicate"),
                _ => {
                    return Err(error(
                        op,
                        &format!("A GitHub issue has no \"{state}\" state."),
                    ))
                }
            };
            let mut body = json!({"state":if reason.is_some(){"closed"}else{"open"}});
            if let Some(reason) = reason {
                if issue["state"] == "CLOSED"
                    && !issue["stateReason"]
                        .as_str()
                        .is_some_and(|s| s.eq_ignore_ascii_case(reason))
                {
                    return Err(error(op,"GitHub cannot change why a closed issue was closed. Reopen it, then close it again."));
                }
                body["state_reason"] = json!(reason);
            }
            gh.json("PATCH", &item, Some(body)).await.map_err(fail)?;
        }
        "react" => {
            if subject.is_some()
                && (data["node"]["__typename"] != "IssueComment"
                    || data["node"]["issue"]["id"] != id)
            {
                return Err(error(
                    op,
                    "The comment does not belong to the selected issue.",
                ));
            }
            let content = match input["content"].as_str() {
                Some("thumbs-up") => "THUMBS_UP",
                Some("thumbs-down") => "THUMBS_DOWN",
                Some("laugh") => "LAUGH",
                Some("hooray") => "HOORAY",
                Some("confused") => "CONFUSED",
                Some("heart") => "HEART",
                Some("rocket") => "ROCKET",
                Some("eyes") => "EYES",
                _ => return Err(error(op, "Invalid reaction.")),
            };
            let action = match input["reacted"].as_bool() {
                Some(true) => "addReaction",
                Some(false) => "removeReaction",
                None => return Err(error(op, "Invalid reaction.")),
            };
            let query=format!("mutation($subjectId:ID!,$content:ReactionContent!){{{action}(input:{{subjectId:$subjectId,content:$content}}){{clientMutationId}}}}");
            gh.graphql(
                &query,
                json!({"subjectId":subject.unwrap_or(id),"content":content}),
            )
            .await
            .map_err(fail)?;
        }
        _ => unreachable!(),
    }
    Ok(Value::Null)
}

fn nodes(v: &Value) -> impl Iterator<Item = &Value> {
    v["nodes"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|n| !n.is_null())
}
fn actor(v: &Value) -> Option<Value> {
    let login = v["login"].as_str()?.trim();
    (!login.is_empty()).then(
        || json!({"login":login,"name":trimmed(&v["name"]),"avatarUrl":trimmed(&v["avatarUrl"])}),
    )
}
fn trimmed(v: &Value) -> Option<&str> {
    v.as_str().map(str::trim).filter(|s| !s.is_empty())
}

fn capabilities() -> Value {
    json!({"create":true,"edit":true,"comment":true,"labels":true,"assignees":true,"reactions":true,"templates":true,"linkedPullRequests":true,"events":true,"search":true})
}
fn summary(v: &Value) -> Option<Value> {
    if v["__typename"] != "Issue" {
        return None;
    }
    let number = v["number"].as_i64()?;
    let state = match (v["state"].as_str(), v["stateReason"].as_str()) {
        (Some("CLOSED"), Some("COMPLETED")) => {
            json!({"category":"closed","name":"Completed","closeReason":"completed"})
        }
        (Some("CLOSED"), Some("NOT_PLANNED")) => {
            json!({"category":"closed","name":"Not planned","closeReason":"not-planned"})
        }
        (Some("CLOSED"), Some("DUPLICATE")) => {
            json!({"category":"closed","name":"Duplicate","closeReason":"duplicate"})
        }
        (Some("CLOSED"), _) => json!({"category":"closed","name":"Closed"}),
        _ => json!({"category":"open","name":"Open"}),
    };
    let labels = nodes(&v["labels"])
        .filter_map(|l| {
            trimmed(&l["name"]).map(|name| json!({"name":name,"color":trimmed(&l["color"])}))
        })
        .collect::<Vec<_>>();
    Some(
        json!({"key":number.to_string(),"displayKey":format!("#{number}"),"title":v["title"],"url":v["url"],"author":actor(&v["author"]),"state":state,"createdAt":v["createdAt"],"updatedAt":v["updatedAt"],"closedAt":trimmed(&v["closedAt"]),"assignees":nodes(&v["assignees"]).filter_map(actor).collect::<Vec<_>>(),"labels":labels,"commentCount":v["comments"]["totalCount"].as_u64().unwrap_or(0)}),
    )
}
async fn list(state: &AppState, cli: &Path, input: &Value) -> Reply {
    let mut projects = state
        .store
        .read()
        .await
        .projects
        .values()
        .filter(|p| input["projectId"].as_str().is_none_or(|s| s == p.id))
        .cloned()
        .collect::<Vec<_>>();
    projects.sort_by_key(|p| (p.created_at, p.id.clone()));
    let mut seen = BTreeSet::new();
    let mut trackers = Vec::new();
    let mut scopes = Vec::new();
    let mut errors = Vec::new();
    let mut entries = Vec::new();
    let mut viewers = json!({});
    let mut next = json!({});
    let cfg = linear::record(state).await?;
    let mut linear_count = 0;
    // Accounts of one tracker share its host: it is unusable only when every
    // account's read failed for want of a credential.
    let mut linear_accounts: Vec<String> = Vec::new();
    let mut linear_unusable: std::collections::HashMap<String, String> = Default::default();
    let mut linear_usable = false;
    let mut host_order = Vec::new();
    let mut total = 0;
    let mut host_error = None;
    for p in projects {
        if cfg["bindings"][&p.id]["tracker"] == "linear" {
            let scope = cfg["bindings"][&p.id]["scope"].as_str().unwrap_or("");
            linear_count += 1;
            if !host_order.contains(&"linear.app") {
                host_order.push("linear.app");
            }
            // `listCursorKey`: an added account's scopes keep cursors of their own.
            let key = match cfg["bindings"][&p.id]["accountId"].as_str() {
                None => format!("linear.app {}", scope.to_lowercase()),
                Some(id) => format!("linear.app {id}/{}", scope.to_lowercase()),
            };
            if input["host"]
                .as_str()
                .is_none_or(|h| h.trim().eq_ignore_ascii_case("linear.app"))
                && seen.insert(key.clone())
            {
                scopes.push(
                    json!({"projectId":p.id,"tracker":"linear","host":"linear.app","scope":scope}),
                );
                let account = linear::viewer_key(&cfg["bindings"][&p.id]);
                if !linear_accounts.contains(&account) {
                    linear_accounts.push(account.clone());
                }
                let cursor = &input["cursors"][&key];
                if input.get("cursors").is_none() || !cursor.is_null() {
                    match linear::page(&cfg, &p, scope, input, cursor, &mut viewers).await {
                        Ok(page) => {
                            linear_usable = true;
                            entries
                                .extend(page["entries"].as_array().into_iter().flatten().cloned());
                            if !page["cursor"].is_null() {
                                next[&key] = page["cursor"].clone();
                            }
                        }
                        Err(e) => {
                            if !cursor.is_null() {
                                next[&key] = cursor.clone();
                            }
                            let message = if e["reason"] == "unauthenticated" {
                                let detail = e["cause"]["message"]
                                    .as_str()
                                    .and_then(|m| m.split_once(": "))
                                    .map(|(_, s)| s)
                                    .unwrap_or("Linear rejected the API key.");
                                linear_unusable.insert(account.clone(), detail.to_owned());
                                detail
                            } else {
                                linear_usable = true;
                                e["detail"]
                                    .as_str()
                                    .unwrap_or("The issue tracker request failed.")
                            };
                            errors.push(
                                json!({"projectId":p.id,"projectTitle":p.name,"message":message}),
                            );
                        }
                    }
                }
            }
        }
        let Some(scope) = github_remote(&p).await else {
            continue;
        };
        total += 1;
        if !host_order.contains(&"github.com") {
            host_order.push("github.com");
        }
        if input["host"]
            .as_str()
            .is_some_and(|s| !s.trim().eq_ignore_ascii_case("github.com"))
        {
            continue;
        }
        let cursor_key = format!("github.com {}", scope.to_lowercase());
        if !seen.insert(cursor_key.clone()) {
            continue;
        }
        scopes.push(json!({"projectId":p.id,"tracker":"github","host":"github.com","scope":scope}));
        let cursor = &input["cursors"][&cursor_key];
        if input.get("cursors").is_some() && cursor.is_null() {
            continue;
        }
        let gh = Github::new(cli, &p.path);
        let result=async {
            let mut query=format!("is:issue repo:{scope}");
            if let Some(s @ ("open"|"closed"))=input["state"].as_str(){query.push_str(&format!(" is:{s}"))}
            let qualifier=match input["involvement"].as_str(){Some("assigned")=>Some("assignee"),Some("authored")=>Some("author"),Some("mentioned")=>Some("mentions"),_=>None};
            if let Some(q)=qualifier{
                if viewers["github.com"].is_null(){let raw=gh.graphql("query {viewer{login}}",json!({})).await?;viewers["github.com"]=json!(raw["viewer"]["login"].as_str().ok_or(Failure::Invalid)?);}
                query.push_str(&format!(" {q}:{}",viewers["github.com"].as_str().unwrap()));
            }
            let quote=|s:&str| format!("\"{}\"",s.replace('\\',"\\\\").replace('"',"\\\""));
            for label in input["labels"].as_array().into_iter().flatten().filter_map(Value::as_str){query.push_str(&format!(" label:{}",quote(label)))}
            if let Some(user)=input["assignee"].as_str(){query.push_str(&format!(" assignee:{}",quote(user)))}
            query.push_str(if input["sort"]=="created" {" sort:created-desc"} else {" sort:updated-desc"});
            for term in input["query"].as_str().unwrap_or("").split_whitespace(){query.push(' ');query.push_str(&quote(term))}
            let search=gh.graphql("query($q:String!,$cursor:String,$rows:Int!){search(query:$q,type:ISSUE,first:$rows,after:$cursor){pageInfo{hasNextPage endCursor} nodes{__typename ... on Issue{number title url state stateReason createdAt updatedAt closedAt author{login avatarUrl ... on User{name}} assignees(first:20){nodes{login name avatarUrl}} labels(first:20){nodes{name color}} comments{totalCount}}}}}",json!({"q":query,"cursor":cursor,"rows":input["limit"].as_i64().unwrap_or(50).clamp(1,100)})).await?;
            let page=search.get("search").ok_or(Failure::Invalid)?;
            for item in nodes(page).filter_map(summary) {
                let mut row=json!({"tracker":"github","host":"github.com","scope":scope});
                row.as_object_mut().unwrap().extend(item.as_object().unwrap().clone());
                row["projectId"]=json!(p.id);
                row["projectTitle"]=json!(p.name);
                entries.push(row);
            }
            if page["pageInfo"]["hasNextPage"]==true{next[&cursor_key]=page["pageInfo"]["endCursor"].clone()}
            Ok::<_,Failure>(())
        }.await;
        if let Err(f) = result {
            if !cursor.is_null() {
                next[&cursor_key] = cursor.clone()
            }
            let msg = match f {
                Failure::Missing => "GitHub CLI is not installed.",
                Failure::Auth => "GitHub authentication is unavailable. Sign in and retry.",
                Failure::RateLimit => "GitHub API rate limit exceeded.",
                Failure::NotFound => "GitHub could not find the requested issue.",
                _ => "The GitHub issue request failed.",
            };
            if matches!(f, Failure::Missing | Failure::Auth) {
                host_error = Some(msg)
            }
            errors.push(json!({"projectId":p.id,"projectTitle":p.name,"message":msg}));
        }
    }
    if total > 0 {
        trackers.push(json!({"tracker":"github","host":"github.com","projectCount":total,"capabilities":capabilities(),"configured":host_error.is_none(),"detail":host_error}))
    }
    if linear_count > 0 {
        let linear_error = (!linear_usable
            && !linear_accounts.is_empty()
            && linear_accounts
                .iter()
                .all(|a| linear_unusable.contains_key(a)))
        .then(|| linear_unusable[&linear_accounts[0]].clone());
        trackers.push(json!({"tracker":"linear","host":"linear.app","projectCount":linear_count,"capabilities":capabilities(),"configured":linear_error.is_none(),"detail":linear_error}));
    }
    trackers.sort_by_key(|t| host_order.iter().position(|h| t["host"] == *h));
    entries.sort_by(|a, b| b["updatedAt"].as_str().cmp(&a["updatedAt"].as_str()));
    Ok(
        json!({"viewers":viewers,"trackers":trackers,"entries":entries,"errors":errors,"nextCursors":next,"scopes":scopes}),
    )
}
