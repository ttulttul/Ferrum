use crate::{provider_rpc::Reply, AppState};
use serde_json::{json, Value};
use std::{path::Path, time::Duration};
use tokio::{fs, io::AsyncWriteExt};
use uuid::Uuid;

fn error(tracker: &str, reason: &str, detail: &str) -> Value {
    json!({"_tag":"IssueTrackerConnectionError","tracker":tracker,"reason":reason,"detail":detail})
}
fn status(connected: bool, account: Value, detail: Value, scopes: Value) -> Value {
    json!({"tracker":"linear","displayName":"Linear","host":"linear.app","scopeNoun":"team","credential":{"label":"Personal API key","helpUrl":"https://linear.app/settings/account/security"},"connected":connected,"account":account,"detail":detail,"scopes":scopes})
}
fn disconnected() -> Value {
    status(false, Value::Null, Value::Null, json!([]))
}
async fn probe(token: &str) -> Reply {
    let failed = |detail: &str| error("linear", "failed", detail);
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(15))
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .map_err(|_| failed("Linear could not be reached."))?;
    let response=client.post("https://api.linear.app/graphql").header("Authorization",token).json(&json!({"query":"query T3Viewer{viewer{id name displayName email avatarUrl} teams(first:250){nodes{id key name}}}","operationName":"T3Viewer","variables":{}})).send().await.map_err(|_|failed("Linear could not be reached."))?;
    let code = response.status().as_u16();
    let body = response.json::<Value>().await.unwrap_or(Value::Null);
    let errors = body["errors"].as_array();
    let has_code =
        |s: &str| errors.is_some_and(|list| list.iter().any(|v| v["extensions"]["code"] == s));
    if code == 429 || has_code("RATELIMITED") {
        return Err(failed("Linear's rate limit was reached."));
    }
    if matches!(code, 401 | 403) || has_code("AUTHENTICATION_ERROR") {
        return Err(error("linear", "rejected", "Linear rejected the API key."));
    }
    if let Some(first) = errors.and_then(|v| v.first()) {
        return Err(failed(
            first["message"]
                .as_str()
                .filter(|s| !s.trim().is_empty())
                .unwrap_or("Linear refused the request."),
        ));
    }
    if !(200..300).contains(&code) {
        return Err(failed(&format!(
            "Linear returned HTTP {code} with an unexpected body."
        )));
    }
    let data = &body["data"];
    let viewer = &data["viewer"];
    let invalid = || failed("Linear returned an unexpected body.");
    let account = ["displayName", "name", "email", "id"]
        .into_iter()
        .find_map(|k| viewer[k].as_str())
        .ok_or_else(invalid)?;
    let scopes=data["teams"]["nodes"].as_array().ok_or_else(invalid)?.iter().map(|t|Ok(json!({"key":t["key"].as_str().ok_or_else(invalid)?,"name":t["name"].as_str().ok_or_else(invalid)?}))).collect::<Result<Vec<_>,Value>>()?;
    Ok(status(true, json!(account), Value::Null, json!(scopes)))
}
async fn read(path: &Path) -> Reply {
    let err = || {
        error(
            "linear",
            "failed",
            "The stored credential could not be read.",
        )
    };
    match fs::read(path).await {
        Ok(bytes) => {
            let v = serde_json::from_slice::<Value>(&bytes).map_err(|_| err())?;
            if !v.is_object() || !v["bindings"].is_object() {
                return Err(err());
            }
            Ok(v)
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            Ok(json!({"token":null,"bindings":{}}))
        }
        _ => Err(err()),
    }
}
async fn write(path: &Path, v: &Value, detail: &str) -> Result<(), Value> {
    let temp = path.with_extension(format!("{}.tmp", Uuid::new_v4()));
    let mut opts = fs::OpenOptions::new();
    opts.create_new(true).write(true);
    #[cfg(unix)]
    opts.mode(0o600);
    let mut f = opts
        .open(&temp)
        .await
        .map_err(|_| error("linear", "failed", detail))?;
    let result = async {
        f.write_all(v.to_string().as_bytes()).await?;
        f.sync_all().await?;
        drop(f);
        fs::rename(&temp, path).await
    }
    .await;
    if result.is_err() {
        let _ = fs::remove_file(temp).await;
        return Err(error("linear", "failed", detail));
    }
    Ok(())
}
pub(crate) async fn dispatch(state: &AppState, method: &str, input: &Value) -> Option<Reply> {
    match method {
        "issueTrackers.list" | "issueTrackers.bindProject" | "issueTrackers.connect" => {
            Some(run(state, method, input).await)
        }
        _ => None,
    }
}
async fn run(state: &AppState, method: &str, input: &Value) -> Reply {
    let tracker = if method == "issueTrackers.connect" {
        input["tracker"].as_str()
    } else {
        input["binding"]["tracker"].as_str()
    };
    if let Some(t) = tracker.filter(|t| *t != "linear") {
        return Err(error(
            t,
            "unknown-tracker",
            "This server has no such issue tracker.",
        ));
    }
    let connected = if method == "issueTrackers.connect" && !input["token"].is_null() {
        let token = input["token"]
            .as_str()
            .filter(|t| !t.trim().is_empty() && t.len() <= 4096)
            .ok_or_else(|| error("linear", "failed", "Linear rejected the API key."))?;
        Some(probe(token).await?)
    } else {
        None
    };
    let guard = state.issues.lock().await;
    let path = &guard.tracker_file;
    let mut record = read(path).await?;
    match method {
        "issueTrackers.list" => {
            drop(guard);
            let connection = if let Some(token) = record["token"].as_str() {
                probe(token)
                    .await
                    .unwrap_or_else(|e| status(true, Value::Null, e["detail"].clone(), json!([])))
            } else {
                disconnected()
            };
            Ok(json!({"trackers":[connection],"bindings":record["bindings"]}))
        }
        "issueTrackers.connect" => {
            record["token"] = input["token"].clone();
            if connected.is_none() {
                record["bindings"] = json!({})
            }
            write(
                path,
                &record,
                if connected.is_some() {
                    "The credential could not be stored."
                } else {
                    "The credential could not be removed."
                },
            )
            .await?;
            Ok(connected.unwrap_or_else(disconnected))
        }
        _ => {
            let project = input["projectId"].as_str().ok_or_else(|| {
                error(
                    "linear",
                    "failed",
                    "The project binding could not be saved.",
                )
            })?;
            if input["binding"].is_null() {
                record["bindings"].as_object_mut().unwrap().remove(project);
            } else {
                record["bindings"][project] =
                    json!({"tracker":input["binding"]["tracker"],"scope":input["binding"]["scope"]})
            }
            write(path, &record, "The project binding could not be saved.").await?;
            Ok(Value::Null)
        }
    }
}
