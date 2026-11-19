use super::{capabilities, fail, labels, nodes, trimmed};
use crate::{provider_rpc::Reply, Project};
use regex::Regex;
use serde_json::{json, Value};
use std::{collections::HashSet, sync::LazyLock};

pub(super) const EMOJIS: [(&str, &str, &str); 8] = [
    ("thumbs-up", "👍", "+1"),
    ("thumbs-down", "👎", "-1"),
    ("laugh", "😄", "smile"),
    ("hooray", "🎉", "tada"),
    ("confused", "😕", "confused"),
    ("heart", "❤️", "heart"),
    ("rocket", "🚀", "rocket"),
    ("eyes", "👀", "eyes"),
];
pub(super) fn content(emoji: &Value) -> Option<&'static str> {
    EMOJIS
        .iter()
        .find(|(_, e, name)| emoji == *e || emoji == *name)
        .map(|v| v.0)
}
fn username(user: &Value) -> Option<&str> {
    ["displayName", "name", "email", "id"]
        .iter()
        .find_map(|k| user[*k].as_str())
}
fn login(user: &Value) -> Option<&str> {
    trimmed(&user["displayName"]).or_else(|| trimmed(&user["id"]))
}
fn actor(user: &Value, bot: &Value) -> Value {
    if let Some(login) = login(user) {
        return json!({"login":login,"name":user["name"],"avatarUrl":user["avatarUrl"]});
    }
    match bot["userDisplayName"]
        .as_str()
        .or_else(|| bot["name"].as_str())
    {
        Some(name) if !name.is_empty() => {
            json!({"isBot":true,"login":bot["name"].as_str().unwrap_or(name),"name":name,"avatarUrl":bot["avatarUrl"]})
        }
        _ => Value::Null,
    }
}
fn reactions(rows: &Value, viewer: &Value) -> Value {
    let mut groups = indexmap::IndexMap::<&str, (usize, Vec<&str>, bool)>::new();
    for r in rows.as_array().into_iter().flatten() {
        let Some(content) = content(&r["emoji"]) else {
            continue;
        };
        let (n, actors, mine) = groups.entry(content).or_default();
        *n += 1;
        if viewer["id"].is_string() && r["user"]["id"] == viewer["id"] {
            *mine = true;
        } else if let Some(user) = username(&r["user"]) {
            actors.push(user);
        }
    }
    json!(groups.into_iter().map(|(c,(count,actors,mine))|json!({"content":c,"count":count,"actors":actors,"viewerHasReacted":mine})).collect::<Vec<_>>())
}
fn closed(state: &Value) -> bool {
    matches!(
        state["type"].as_str(),
        Some("completed" | "canceled" | "duplicate")
    )
}
fn state(v: &Value) -> Value {
    if closed(v) {
        json!({"category":"closed","name":v["name"],"closeReason":v["type"]})
    } else {
        json!({"category":"open","name":v["name"]})
    }
}
pub(super) fn summary(issue: &Value, p: &Project, scope: &str, count: usize) -> Reply {
    if !issue["identifier"].is_string()
        || !issue["title"].is_string()
        || !issue["url"].is_string()
        || !issue["state"]["name"].is_string()
    {
        return Err(fail(
            "list",
            "listIssues",
            "failed",
            "Linear returned an unexpected body.",
        ));
    }
    let labels = nodes(&issue["labels"])
        .map(|l| json!({"name":l["name"],"color":l["color"]}))
        .collect::<Vec<_>>();
    let assignees = if issue["assignee"].is_null() {
        vec![]
    } else {
        vec![actor(&issue["assignee"], &Value::Null)]
    };
    Ok(
        json!({"tracker":"linear","host":"linear.app","scope":scope,"key":issue["identifier"],"displayKey":issue["identifier"],"title":issue["title"],"url":issue["url"],
        "author":actor(&issue["creator"],&issue["botActor"]),"state":state(&issue["state"]),"createdAt":issue["createdAt"],"updatedAt":issue["updatedAt"],
        "closedAt":issue["completedAt"].as_str().or_else(||issue["canceledAt"].as_str()),"assignees":assignees,"labels":labels,"commentCount":count,"projectId":p.id,"projectTitle":p.name}),
    )
}
pub(super) fn comments(page: &Value, viewer: &Value) -> Value {
    let rows=nodes(page).map(|c|json!({"id":c["id"],"author":actor(&c["user"],&c["botActor"]),"body":c["body"],"createdAt":c["createdAt"],"url":c["url"],"reactions":reactions(&c["reactions"],viewer)})).collect::<Vec<_>>();
    json!({"comments":rows,"nextCursor":if page["pageInfo"]["hasNextPage"]==true{page["pageInfo"]["endCursor"].clone()}else{Value::Null}})
}
fn events(history: &Value) -> Vec<Value> {
    let mut events = Vec::new();
    for entry in nodes(history) {
        let Some(id) = entry["id"].as_str() else {
            continue;
        };
        let person = actor(&entry["actor"], &entry["botActor"]);
        let mut add = |kind: &str, detail: Option<&str>, suffix: &str| {
            events.push(json!({"id":format!("{id}:{suffix}"),"kind":kind,"actor":person,"createdAt":entry["createdAt"],"detail":detail}));
        };
        if !entry["toState"].is_null() && entry["toState"]["id"] != entry["fromState"]["id"] {
            let kind = match (closed(&entry["fromState"]), closed(&entry["toState"])) {
                (false, true) => "closed",
                (true, false) => "reopened",
                _ => "state-changed",
            };
            add(kind, entry["toState"]["name"].as_str(), "state");
        }
        for (key, kind, sign) in [
            ("addedLabels", "labeled", "+"),
            ("removedLabels", "unlabeled", "-"),
        ] {
            for l in entry[key].as_array().into_iter().flatten() {
                add(
                    kind,
                    l["name"].as_str(),
                    &format!("label{sign}{}", l["id"].as_str().unwrap_or("")),
                );
            }
        }
        if entry["fromAssignee"]["id"] != entry["toAssignee"]["id"] {
            if !entry["fromAssignee"].is_null() {
                add("unassigned", username(&entry["fromAssignee"]), "unassigned");
            }
            if !entry["toAssignee"].is_null() {
                add("assigned", username(&entry["toAssignee"]), "assigned");
            }
        }
        if entry["toTitle"].is_string() && entry["toTitle"] != entry["fromTitle"] {
            add("renamed", entry["toTitle"].as_str(), "title");
        }
    }
    events
}
fn links(attachments: &Value) -> Vec<Value> {
    static PR: LazyLock<Regex> = LazyLock::new(|| {
        Regex::new(r"^https://(?:github\.com/([^/]+/[^/]+)/pull|gitlab\.com/(.+?)/-/merge_requests)/([0-9]+)/?(?:[?#]|$)").unwrap()
    });
    let mut seen = HashSet::new();
    nodes(attachments).filter_map(|a|{
        let url=a["url"].as_str()?;
        let c=PR.captures(url)?;
        if !seen.insert(url){return None;}
        let status=a["metadata"]["status"].as_str();
        Some(json!({"repository":c.get(1).or_else(||c.get(2))?.as_str(),"number":c[3].parse::<u64>().ok()?,"title":trimmed(&a["title"]).unwrap_or(url),
            "url":url,"state":match status{Some("merged")=>"merged",Some("closed")=>"closed",_=>"open"},"isDraft":status==Some("draft")||a["metadata"]["draft"]==true,"closesIssue":false}))
    }).collect()
}
pub(super) fn detail(
    issue: &Value,
    p: &Project,
    scope: &str,
    writable: bool,
    viewer: &Value,
) -> Reply {
    let comments = comments(&issue["comments"], viewer);
    let mut row = summary(
        issue,
        p,
        scope,
        comments["comments"].as_array().unwrap().len(),
    )?;
    let mut states = nodes(&issue["team"]["states"]).collect::<Vec<_>>();
    const ORDER: [&str; 7] = [
        "triage",
        "backlog",
        "unstarted",
        "started",
        "completed",
        "canceled",
        "duplicate",
    ];
    states.sort_by(|a, b| {
        let rank = |s: &Value| {
            ORDER
                .iter()
                .position(|k| s["type"] == *k)
                .map_or(0, |i| i + 1)
        };
        rank(a).cmp(&rank(b)).then_with(|| {
            a["position"]
                .as_f64()
                .unwrap_or_default()
                .total_cmp(&b["position"].as_f64().unwrap_or_default())
        })
    });
    let extra = json!({"writable":writable,"capabilities":capabilities(),"viewerPermissions":{"setState":true,"comment":true,"edit":true,"labels":true,"assignees":true},
        "states":states.into_iter().map(state).collect::<Vec<_>>(),"viewer":login(viewer),"body":issue["description"].as_str().unwrap_or(""),
        "reactions":reactions(&issue["reactions"],viewer),"comments":comments["comments"],"commentsCursor":comments["nextCursor"],
        "events":events(&issue["history"]),"linkedPullRequests":links(&issue["attachments"])});
    row.as_object_mut()
        .unwrap()
        .extend(extra.as_object().unwrap().clone());
    Ok(row)
}
pub(super) fn candidates(scope: &Value) -> Value {
    let team = &scope["teams"]["nodes"][0];
    let labels = labels(scope)
        .map(|l| json!({"name":l["name"],"color":l["color"],"description":l["description"]}))
        .collect::<Vec<_>>();
    let users = nodes(&team["members"])
        .map(|u| {
            let mut a = actor(u, &Value::Null);
            a["id"] = u["id"].clone();
            a
        })
        .collect::<Vec<_>>();
    let truncated = [
        &team["states"],
        &team["labels"],
        &team["members"],
        &scope["issueLabels"],
    ]
    .iter()
    .any(|v| v["pageInfo"]["hasNextPage"] == true);
    json!({"labels":labels,"assignees":users,"truncated":truncated})
}
pub(super) fn templates(scope: &Value) -> Value {
    let team = &scope["teams"]["nodes"][0];
    let mut templates = Vec::new();
    let mut external = Vec::new();
    for t in nodes(&team["templates"]).filter(|t| t["type"] == "issue") {
        let data = t["templateData"]
            .as_str()
            .and_then(|s| serde_json::from_str(s).ok())
            .unwrap_or_else(|| t["templateData"].clone());
        let body = t["content"]
            .as_str()
            .or_else(|| data["description"].as_str());
        if t["hasFormFields"] == true || (body.is_none() && !data["descriptionData"].is_null()) {
            let mut url = reqwest::Url::parse("https://linear.app").unwrap();
            url.path_segments_mut().unwrap().extend([
                "team",
                team["key"].as_str().unwrap_or(""),
                "new",
            ]);
            url.query_pairs_mut()
                .append_pair("template", t["id"].as_str().unwrap_or(""));
            external.push(json!({"name":t["name"],"about":t["description"].as_str().unwrap_or(""),"url":url.as_str()}));
        } else {
            let names = data["labelIds"]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(|id| {
                    labels(scope)
                        .find(|l| l["id"] == *id)
                        .map(|l| l["name"].clone())
                })
                .collect::<Vec<_>>();
            let assignees = data["assigneeId"]
                .as_str()
                .filter(|s| !s.is_empty())
                .into_iter()
                .collect::<Vec<_>>();
            templates.push(json!({"key":t["id"],"name":t["name"],"about":t["description"].as_str().unwrap_or(""),"title":data["title"].as_str().unwrap_or(""),
                "body":body.unwrap_or(""),"labels":names,"assignees":assignees}));
        }
    }
    json!({"templates":templates,"externalTemplates":external,"blankIssuesEnabled":true})
}
