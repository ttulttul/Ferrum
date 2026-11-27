use super::{actor, capabilities, error, failed, nodes, summary, trimmed};
use crate::{
    provider_rpc::{Failure, Github, Reply},
    Project,
};
use serde_json::{json, Value};
use std::collections::HashSet;

fn reactions(groups: &Value, viewer: Option<&str>) -> Vec<Value> {
    groups.as_array().into_iter().flatten().filter_map(|g|{
        let content=match g["content"].as_str()?{"THUMBS_UP"=>"thumbs-up","THUMBS_DOWN"=>"thumbs-down","LAUGH"=>"laugh","HOORAY"=>"hooray","CONFUSED"=>"confused","HEART"=>"heart","ROCKET"=>"rocket","EYES"=>"eyes",_=>return None};
        let people=nodes(&g["reactors"]).filter_map(|n|trimmed(&n["login"])).collect::<Vec<_>>();
        let count=g["reactors"]["totalCount"].as_u64().unwrap_or(0).max(people.len() as u64);
        let actors=people.into_iter().filter(|p|viewer.is_none_or(|v|!v.eq_ignore_ascii_case(p))).collect::<Vec<_>>();
        (count>0).then(||json!({"content":content,"count":count,"actors":actors,"viewerHasReacted":g["viewerHasReacted"]}))
    }).collect()
}
fn comments(page: &Value, viewer: Option<&str>) -> Value {
    let comments=nodes(page).filter_map(|n|Some(json!({"id":trimmed(&n["id"])?,"author":actor(&n["author"]),"body":n["body"].as_str().unwrap_or(""),"createdAt":n["createdAt"],"url":trimmed(&n["url"]),"reactions":reactions(&n["reactionGroups"],viewer)}))).collect::<Vec<_>>();
    json!({"comments":comments,"nextCursor":if page["pageInfo"]["hasPreviousPage"]==true{page["pageInfo"]["startCursor"].clone()}else{Value::Null}})
}
fn link(v: &Value, closes: bool) -> Option<Value> {
    Some(
        json!({"repository":v["repository"]["nameWithOwner"].as_str()?,"number":v["number"].as_i64()?,"title":v["title"],"url":v["url"],"state":match v["state"].as_str(){Some("MERGED")=>"merged",Some("CLOSED")=>"closed",_=>"open"},"isDraft":v["isDraft"].as_bool().unwrap_or(false),"closesIssue":closes}),
    )
}
async fn collections(gh: &Github<'_>, vars: &Value, issue: &mut Value) -> Result<(), Value> {
    let mut seen = HashSet::new();
    loop {
        let mut args = vars.clone();
        let mut cursor_key = Vec::new();
        for (field, cursor, flag) in [
            ("labels", "labelsCursor", "withLabels"),
            ("assignees", "assigneesCursor", "withAssignees"),
            (
                "closedByPullRequestsReferences",
                "closingPullRequestsCursor",
                "withClosingPullRequests",
            ),
        ] {
            let more = issue[field]["pageInfo"]["hasNextPage"] == true;
            args[flag] = json!(more);
            args[cursor] = if more {
                issue[field]["pageInfo"]["endCursor"].clone()
            } else {
                Value::Null
            };
            if more {
                cursor_key.push(args[cursor].as_str().ok_or_else(||error("detail","GitHub did not return complete issue labels, assignees or linked pull requests."))?.to_owned());
            }
        }
        if cursor_key.is_empty() {
            return Ok(());
        }
        if !seen.insert(cursor_key) {
            return Err(error(
                "detail",
                "GitHub did not return complete issue labels, assignees or linked pull requests.",
            ));
        }
        let raw = gh
            .graphql(include_str!("graphql/IssueCollections.graphql"), args)
            .await
            .map_err(|e| failed("detail", e))?;
        let next = &raw["repository"]["issue"];
        if next.is_null() {
            return Err(failed("detail", Failure::NotFound));
        }
        for field in ["labels", "assignees", "closedByPullRequestsReferences"] {
            if issue[field]["pageInfo"]["hasNextPage"] == true {
                let list=next[field]["nodes"].as_array().ok_or_else(||error("detail","GitHub did not return complete issue labels, assignees or linked pull requests."))?;
                if issue[field]["nodes"].is_null() {
                    issue[field]["nodes"] = json!([])
                }
                issue[field]["nodes"]
                    .as_array_mut()
                    .ok_or_else(|| failed("detail", Failure::Invalid))?
                    .extend(list.iter().cloned());
                issue[field]["pageInfo"] = next[field]["pageInfo"].clone();
            }
        }
    }
}
pub(super) async fn read(
    gh: &Github<'_>,
    project: &Project,
    op: &str,
    scope: &str,
    writable: bool,
    input: &Value,
) -> Reply {
    let (owner, name) = scope.split_once('/').unwrap();
    let mut vars = json!({"owner":owner,"name":name});
    if op == "templates" {
        let data = gh
            .graphql(include_str!("graphql/IssueTemplates.graphql"), vars)
            .await
            .map_err(|e| failed(op, e))?;
        if data["repository"].is_null() {
            return Err(failed(op, Failure::NotFound));
        }
        return super::templates::decode(&data["repository"]);
    }
    let key = input["key"].as_str().unwrap_or("");
    vars["number"] = json!(key
        .parse::<i32>()
        .ok()
        .filter(|n| *n > 0 && n.to_string() == key)
        .ok_or_else(|| failed(op, Failure::NotFound))?);
    if op == "comments" {
        vars["cursor"] = input["cursor"].clone()
    }
    let mut data = if op == "comments" {
        gh.graphql(include_str!("graphql/IssueComments.graphql"), vars.clone())
            .await
    } else {
        super::planning::detail(gh, vars.clone()).await
    }
    .map_err(|e| failed(op, e))?;
    let viewer = trimmed(&data["viewer"]["login"]).map(str::to_owned);
    let repo = &mut data["repository"];
    let triage = matches!(
        repo["viewerPermission"].as_str(),
        Some("ADMIN" | "WRITE" | "TRIAGE" | "MAINTAIN")
    );
    let states: Vec<_> = nodes(&repo["planningStatuses"])
        .map(super::planning::state)
        .collect();
    let issue = &mut repo["issue"];
    if issue.is_null() {
        return Err(failed(op, Failure::NotFound));
    }
    let page = comments(&issue["comments"], viewer.as_deref());
    if op == "comments" {
        return Ok(page);
    }
    collections(gh, &vars, issue).await?;
    let mut result = json!({"tracker":"github","host":"github.com","scope":scope});
    let row = summary(issue).ok_or_else(|| failed(op, Failure::Invalid))?;
    result
        .as_object_mut()
        .unwrap()
        .extend(row.as_object().unwrap().clone());
    let set_state = if !states.is_empty() && issue["viewerCanSetStatus"].is_boolean() {
        &issue["viewerCanSetStatus"]
    } else if issue["state"] == "CLOSED" {
        &issue["viewerCanReopen"]
    } else {
        &issue["viewerCanClose"]
    };
    let states = if states.is_empty() {
        super::planning::legacy_states()
    } else {
        Value::Array(states)
    };
    let permissions = json!({"setState":set_state,"comment":true,"edit":issue["viewerCanUpdate"]!=false,"labels":triage,"assignees":triage});
    let mut links = Vec::new();
    let mut seen = HashSet::new();
    let mut events = Vec::new();
    let mut add_link = |l: Option<Value>| {
        if let Some(link) = l {
            if seen.insert((
                link["repository"].as_str().unwrap().to_lowercase(),
                link["number"].as_i64(),
            )) {
                links.push(link)
            }
        }
    };
    for node in nodes(&issue["closedByPullRequestsReferences"]) {
        add_link(link(node, true))
    }
    for n in nodes(&issue["timelineItems"]) {
        let (kind, detail) = match n["__typename"].as_str() {
            Some("ClosedEvent") => ("closed", None),
            Some("ReopenedEvent") => ("reopened", None),
            Some("LabeledEvent") => ("labeled", trimmed(&n["label"]["name"]).map(str::to_owned)),
            Some("UnlabeledEvent") => {
                ("unlabeled", trimmed(&n["label"]["name"]).map(str::to_owned))
            }
            Some("AssignedEvent") => (
                "assigned",
                trimmed(&n["assignee"]["login"]).map(str::to_owned),
            ),
            Some("UnassignedEvent") => (
                "unassigned",
                trimmed(&n["assignee"]["login"]).map(str::to_owned),
            ),
            Some("RenamedTitleEvent") => {
                ("renamed", trimmed(&n["currentTitle"]).map(str::to_owned))
            }
            Some("CrossReferencedEvent") => {
                let source = &n["source"];
                if source["__typename"] == "PullRequest" {
                    add_link(link(source, false))
                }
                (
                    "referenced",
                    source["repository"]["nameWithOwner"]
                        .as_str()
                        .zip(source["number"].as_u64())
                        .map(|(r, n)| format!("{r}#{n}")),
                )
            }
            _ => continue,
        };
        if let (Some(id), Some(created)) = (trimmed(&n["id"]), trimmed(&n["createdAt"])) {
            events.push(json!({"id":id,"kind":kind,"actor":actor(&n["actor"]),"createdAt":created,"detail":detail}))
        }
    }
    let fields = json!({"projectId":project.id,"projectTitle":project.name,"writable":writable,"capabilities":capabilities(),"viewerPermissions":permissions,"states":states,"viewer":viewer,"body":issue["body"].as_str().unwrap_or(""),"reactions":reactions(&issue["reactionGroups"],viewer.as_deref()),"comments":page["comments"],"commentsCursor":page["nextCursor"],"events":events,"linkedPullRequests":links});
    result
        .as_object_mut()
        .unwrap()
        .extend(fields.as_object().unwrap().clone());
    Ok(result)
}
