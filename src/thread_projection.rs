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
        json!({"thread":ordered(APP_FIELDS,&row),"runs":self.runs,"attempts":[],"nodes":[],"subagents":[],
            "providerSessions":[],"providerThreads":[],"providerTurns":[],"runtimeRequests":[],
            "messages":self.messages,"plans":[],"turnItems":[],"checkpointScopes":[],"checkpoints":[],
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
