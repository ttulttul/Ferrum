use crate::{auth_service, Store, Thread};
use serde_json::{json, Map, Value};

const APP_FIELDS: &[&str] = include!(concat!(env!("OUT_DIR"), "/OrchestrationV2AppThread.rs"));
const SHELL_FIELDS: &[&str] = include!(concat!(env!("OUT_DIR"), "/OrchestrationV2ThreadShell.rs"));

fn ordered(fields: &[&str], row: &Value) -> Value {
    Value::Object(
        fields
            .iter()
            .filter_map(|&k| row.get(k).map(|v| (k.to_string(), v.clone())))
            .collect(),
    )
}

pub(crate) fn created(command: &Value, at: &str) -> Value {
    json!({"createdBy":command["createdBy"],"creationSource":command["creationSource"],
        "id":command["threadId"],"projectId":command["projectId"],"title":command["title"],
        "providerInstanceId":command["modelSelection"]["instanceId"],"modelSelection":command["modelSelection"],
        "runtimeMode":command["runtimeMode"],"interactionMode":command["interactionMode"],
        "branch":command["branch"],"worktreePath":command["worktreePath"],"pullRequests":[],
        "activeProviderThreadId":null,"lineage":{"parentThreadId":null,"relationshipToParent":null,"rootThreadId":command["threadId"]},
        "forkedFrom":null,"createdAt":at,"updatedAt":at,"archivedAt":null,"settledOverride":null,
        "settledAt":null,"snoozedUntil":null,"snoozedAt":null,"lastVisitedAt":null,"deletedAt":null})
}

impl Thread {
    fn turn_items(&self) -> Vec<Value> {
        self.messages
            .iter()
            .enumerate()
            .map(|(ordinal, message)| {
                let at = auth_service::iso((message.created_at as i64).saturating_mul(1000));
                json!({
                    "id": message.id,
                    "threadId": self.id,
                    "runId": null,
                    "nodeId": null,
                    "providerThreadId": null,
                    "providerTurnId": null,
                    "nativeItemRef": null,
                    "parentItemId": null,
                    "ordinal": ordinal,
                    "status": "completed",
                    "title": null,
                    "startedAt": at,
                    "completedAt": at,
                    "updatedAt": at,
                    "type": "user_message",
                    "createdBy": "user",
                    "creationSource": "web",
                    "messageId": message.id,
                    "inputIntent": "turn_start",
                    "text": message.content,
                    "attachments": []
                })
            })
            .collect()
    }

    pub(crate) fn stored_row(&self) -> Value {
        if self.row.is_object() {
            return self.row.clone();
        }
        let at = auth_service::iso((self.created_at as i64).saturating_mul(1000));
        created(
            &json!({"createdBy":"user","creationSource":"web","threadId":self.id,"projectId":self.project_id,
            "title":self.title,"modelSelection":null,"runtimeMode":"full-access","interactionMode":"default","worktreePath":self.cwd}),
            &at,
        )
    }
    pub(crate) fn projection(&self) -> Value {
        let row = self.stored_row();
        let array = |key: &str| {
            row.get(key)
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default()
        };
        let scope_id = format!("scope:{}", self.id);
        let checkpoint_scopes = self
            .checkpoints
            .first()
            .map(|checkpoint| {
                vec![json!({
                    "id":scope_id,
                    "threadId":self.id,
                    "runId":null,
                    "nodeId":format!("node:{}",self.id),
                    "parentScopeId":null,
                    "providerThreadId":null,
                    "kind":"root_run",
                    "ordinalWithinParent":0,
                    "advancesAppRunCount":true,
                    "cwd":checkpoint.cwd,
                    "createdAt":auth_service::iso((checkpoint.captured_at as i64).saturating_mul(1000))
                })]
            })
            .unwrap_or_default();
        let checkpoints = self.checkpoints.iter().map(|checkpoint| json!({
            "id":format!("checkpoint:{}:{}",self.id,checkpoint.turn_count),
            "threadId":self.id,
            "scopeId":scope_id,
            "runId":null,
            "nodeId":format!("node:{}",self.id),
            "parentCheckpointId":null,
            "ordinalWithinScope":checkpoint.turn_count,
            "appRunOrdinal":if checkpoint.turn_count == 0 {Value::Null} else {json!(checkpoint.turn_count)},
            "ref":checkpoint.tree,
            "status":if checkpoint.ready {"ready"} else {"missing"},
            "files":[],
            "capturedAt":auth_service::iso((checkpoint.captured_at as i64).saturating_mul(1000))
        })).collect::<Vec<_>>();
        json!({"thread":ordered(APP_FIELDS,&row),"runs":self.runs,"attempts":array("attempts"),"nodes":array("nodes"),"subagents":array("subagents"),
            "providerSessions":array("providerSessions"),"providerThreads":array("providerThreads"),"providerTurns":array("providerTurns"),"runtimeRequests":array("runtimeRequests"),
            "messages":self.messages,"plans":[],"turnItems":self.turn_items(),"checkpointScopes":checkpoint_scopes,"checkpoints":checkpoints,
            "contextHandoffs":[],"contextTransfers":[],"visibleTurnItems":[],"updatedAt":row["updatedAt"]})
    }
    pub(crate) fn shell(&self) -> Value {
        let mut row = self.stored_row();
        for key in [
            "latestRunId",
            "latestRunRequestedAt",
            "latestRunStartedAt",
            "latestRunCompletedAt",
            "activeRunId",
            "activityRunStartedAt",
            "activityRunStatus",
            "lastError",
            "lastErrorClass",
            "lastErrorCode",
            "usageLimitResetAt",
            "pendingRuntimeRequest",
            "latestVisibleMessage",
            "latestUserMessageAt",
            "latestUserAuthoredMessageAt",
            "goal",
            "unsettledAt",
            "limitRecovery",
            "pinnedAt",
            "autoSettleDisabledAt",
            "pinOrderKey",
            "opens",
            "titleRegeneration",
            "lastTitleRegeneration",
        ] {
            row.as_object_mut()
                .unwrap()
                .entry(key)
                .or_insert(Value::Null);
        }
        for key in [
            "latestRunStopRequested",
            "latestRunBackgroundWake",
            "blockedOnThreadWait",
            "hasActionableProposedPlan",
        ] {
            row[key] = false.into();
        }
        for key in ["pendingBackgroundTasks", "providerInstanceHistory"] {
            row[key] = json!([]);
        }
        row["status"] = json!(self.status);
        row["pendingGuestMessageCount"] = json!(row["guestShare"]["messages"]
            .as_array()
            .into_iter()
            .flatten()
            .filter(|m| m["status"] == "pending")
            .count());
        row["itemCount"] = json!(self.messages.len());
        row["visibleItemCount"] = row["itemCount"].clone();
        ordered(SHELL_FIELDS, &row)
    }
}

fn field<'a>(row: &'a Value, key: &str) -> &'a str {
    row[key].as_str().unwrap_or_default().trim()
}

fn same_link(a: &Value, b: &Value, issue: bool) -> bool {
    field(a, "host").eq_ignore_ascii_case(field(b, "host"))
        && if issue {
            a["tracker"] == b["tracker"]
                && field(a, "scope").eq_ignore_ascii_case(field(b, "scope"))
                && field(a, "key") == field(b, "key")
        } else {
            field(a, "repository").eq_ignore_ascii_case(field(b, "repository"))
                && a["number"] == b["number"]
        }
}

pub(crate) fn link_patch(thread: &Thread, cmd: &Value, at: &str) -> Value {
    let kind = field(cmd, "type");
    let issue = kind.starts_with("thread.issue.");
    let key = if issue { "issues" } else { "pullRequests" };
    let mut links = thread.row[key].as_array().cloned().unwrap_or_default();
    let i = links.iter().position(|l| same_link(l, cmd, issue));
    if kind.ends_with(".unlink") {
        let Some(i) = i else {
            return json!({});
        };
        if !issue && (links[i]["source"] == "stack" || !links[i]["stack"].is_null()) {
            links[i]["source"] = "stack-dismissed".into();
            links[i].as_object_mut().unwrap().shift_remove("watch");
        } else {
            links.remove(i);
        }
    } else if kind == "thread.pull-request-link.sync" {
        let Some(i) = i else {
            return json!({});
        };
        links[i]["snapshot"] = cmd["snapshot"].clone();
        links[i]["stack"] = cmd["stack"].clone();
    } else if let Some(i) = i {
        if issue || links[i]["source"] != "stack-dismissed" || cmd["source"] == "stack" {
            return json!({});
        }
        links[i]["source"] = cmd["source"].clone();
        links[i]["url"] = cmd["url"].clone();
    } else if issue {
        links.push(json!({"tracker":cmd["tracker"],"host":field(cmd,"host").to_lowercase(),
            "scope":field(cmd,"scope"),"key":field(cmd,"key"),"projectId":cmd["projectId"],
            "url":cmd["url"],"source":cmd["source"],"linkedAt":at,"displayKey":cmd["displayKey"],"title":cmd["title"]}));
    } else {
        let mut link = json!({"host":field(cmd,"host").to_lowercase(),"repository":field(cmd,"repository").to_lowercase(),"number":cmd["number"],"url":cmd["url"]});
        if let Some(url) = cmd.get("repositoryWebUrl") {
            link["repositoryWebUrl"] = url.clone();
        }
        link["source"] = cmd["source"].clone();
        link["linkedAt"] = json!(at);
        link["snapshot"] = Value::Null;
        link["stack"] = Value::Null;
        links.push(link);
    }
    let mut patch = Map::new();
    patch.insert(key.into(), Value::Array(links));
    if !issue {
        patch.insert("linkedPullRequest".into(), Value::Null);
    }
    if !kind.ends_with(".sync") {
        patch.insert("updatedAt".into(), json!(at));
    }
    Value::Object(patch)
}

pub(crate) fn watch_patch(thread: &Thread, cmd: &Value, at: &str) -> Value {
    let mut links = thread.row["pullRequests"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    let host = field(cmd, "host");
    let repository = field(cmd, "repository");
    let number = cmd["number"].clone();
    let matches = |link: &Value| {
        field(link, "host").eq_ignore_ascii_case(host)
            && field(link, "repository").eq_ignore_ascii_case(repository)
            && link["number"] == number
            && link["source"] != "stack-dismissed"
    };
    let key_matches = |link: &Value| {
        field(link, "host").eq_ignore_ascii_case(host)
            && field(link, "repository").eq_ignore_ascii_case(repository)
            && link["number"] == number
    };
    let mut index = links.iter().position(matches);
    if cmd["watching"] == true {
        if index.is_none() {
            let Some(link) = cmd.get("link").cloned() else {
                return json!({});
            };
            let ended_watch = links
                .iter()
                .find(|candidate| key_matches(candidate) && candidate["source"] == "stack-dismissed")
                .and_then(|candidate| candidate.get("endedWatch").cloned());
            links.retain(|candidate| !key_matches(candidate));
            let mut created = json!({
                "host": host.to_lowercase(),
                "repository": repository.to_lowercase(),
                "number": number,
                "url": link["url"],
                "source": link["source"],
                "linkedAt": at,
                "snapshot": Value::Null,
                "stack": Value::Null
            });
            if let Some(repository_web_url) = link.get("repositoryWebUrl") {
                created["repositoryWebUrl"] = repository_web_url.clone();
            }
            if let Some(ended_watch) = ended_watch {
                created["endedWatch"] = ended_watch;
            }
            links.push(created);
            index = Some(links.len() - 1);
        }
        let index = index.expect("watch link index exists");
        if links[index].get("watch").is_some() {
            return json!({});
        }
        let previous = links[index].get("endedWatch").cloned();
        let mut watch = previous.unwrap_or_else(|| {
            json!({
                "startedAt": at,
                "headSha": Value::Null,
                "failedChecks": [],
                "passed": false,
                "passedChecks": [],
                "remarksThrough": at,
                "remarkIds": [],
                "conflicting": false,
                "wakes": 0
            })
        });
        watch["startedAt"] = json!(at);
        watch["remarksThrough"] = json!(at);
        watch["remarkIds"] = json!([]);
        watch["wakes"] = json!(0);
        if let Some(object) = links[index].as_object_mut() {
            object.remove("endedWatch");
            object.insert("watch".into(), watch);
        }
    } else if let Some(index) = index {
        let Some(link) = links[index].as_object_mut() else {
            return json!({});
        };
        let Some(previous) = link.remove("watch") else {
            return json!({});
        };
        link.insert("endedWatch".into(), previous);
    } else {
        return json!({});
    }
    json!({"pullRequests":links,"updatedAt":at})
}

pub(crate) fn linked(store: &Store, input: &Value, issue: bool) -> Value {
    let key = if issue { "issues" } else { "pullRequests" };
    let mut rows: Vec<_> = store
        .threads
        .values()
        .filter(|t| {
            t.row["lineage"]["sideChat"] != true
                && t.row[key]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .any(|l| l["source"] != "stack-dismissed" && same_link(l, input, issue))
        })
        .collect();
    rows.sort_by(|a, b| {
        field(&b.row, "updatedAt")
            .cmp(field(&a.row, "updatedAt"))
            .then_with(|| a.id.cmp(&b.id))
    });
    json!({"threads":rows.into_iter().map(|t|json!({"id":t.id,"projectId":t.project_id,"title":t.title,"archivedAt":t.row["archivedAt"],"settled":t.settled})).collect::<Vec<_>>()})
}
