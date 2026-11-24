use crate::{provider_rpc::Reply, AppState};
use serde_json::{json, Value};
use std::path::Path;
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
/// A connection row of an added account carries its id (`accountFields`).
fn with_account(row: Value, account_id: Option<&str>) -> Value {
    match account_id {
        None => row,
        Some(id) => {
            // After `credential`, where the reference's row places it.
            let mut out = serde_json::Map::new();
            for (key, value) in row.as_object().cloned().unwrap_or_default() {
                let credential = key == "credential";
                out.insert(key, value);
                if credential {
                    out.insert("accountId".into(), json!(id));
                }
            }
            Value::Object(out)
        }
    }
}
/// The stored credential of `account_id` (`None`: the tracker's first).
pub(super) fn account_token<'a>(record: &'a Value, account_id: Option<&str>) -> Option<&'a str> {
    match account_id {
        None => record["token"].as_str(),
        Some(id) => record["accounts"]
            .as_array()?
            .iter()
            .find(|a| a["accountId"] == id)?["token"]
            .as_str(),
    }
}
/// The credential a project's binding reads with: its account's, or the first's.
pub(super) fn binding_token<'a>(record: &'a Value, binding: &Value) -> Option<&'a str> {
    account_token(record, binding["accountId"].as_str())
}
/// `IssueTrackerAccountId`: 1-64 of `[a-z0-9-]`, not starting with `-`.
fn valid_account_id(id: &str) -> bool {
    !id.is_empty()
        && id.len() <= 64
        && id.trim() == id
        && id
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
        && !id.starts_with('-')
}
async fn probe(token: &str) -> Reply {
    let failed = |detail: &str| error("linear", "failed", detail);
    let data = super::linear::probe(token).await.map_err(|e| {
        let detail = e["detail"]
            .as_str()
            .unwrap_or("Linear rejected the API key.");
        let mut err = error(
            "linear",
            if e["reason"] == "unauthenticated" {
                "rejected"
            } else {
                "failed"
            },
            detail,
        );
        err["cause"] = e["cause"].clone();
        err
    })?;
    let viewer = &data["viewer"];
    let invalid = || failed("Linear returned an unexpected body.");
    let account = ["displayName", "name", "email", "id"]
        .into_iter()
        .find_map(|k| viewer[k].as_str())
        .ok_or_else(invalid)?;
    // The workspace's URL key, which its issue links name (upstream #38854).
    let workspace = data["organization"]["urlKey"]
        .as_str()
        .map(str::trim)
        .filter(|key| !key.is_empty())
        .map(str::to_owned);
    let scopes=data["teams"]["nodes"].as_array().ok_or_else(invalid)?.iter().map(|t|Ok(json!({"key":t["key"].as_str().ok_or_else(invalid)?,"name":t["name"].as_str().ok_or_else(invalid)?}))).collect::<Result<Vec<_>,Value>>()?;
    let mut connected = status(true, json!(account), Value::Null, json!(scopes));
    if let Some(workspace) = workspace {
        // Before `scopes`, as the reference's connection lists it.
        let scopes = connected["scopes"].take();
        if let Some(fields) = connected.as_object_mut() {
            fields.remove("scopes");
            fields.insert("workspace".into(), json!(workspace));
            fields.insert("scopes".into(), scopes);
        }
    }
    Ok(connected)
}
pub(super) async fn read(path: &Path) -> Reply {
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
            Ok(json!({"token":null,"bindings":{},"accounts":[]}))
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
    let failed = |detail: &str| error("linear", "failed", detail);
    let account_input = |key: &Value| -> Result<Option<String>, Value> {
        match key {
            Value::Null => Ok(None),
            Value::String(id) if valid_account_id(id) => Ok(Some(id.clone())),
            _ => Err(failed("This account is not saved on the server.")),
        }
    };
    let add_account = method == "issueTrackers.connect" && input["addAccount"] == true;
    // The tracker is asked before the lock, so a slow probe never holds up other settings.
    let connected = if method == "issueTrackers.connect" && !input["token"].is_null() {
        let token = input["token"]
            .as_str()
            .filter(|t| !t.trim().is_empty() && t.len() <= 4096)
            .ok_or_else(|| failed("Linear rejected the API key."))?;
        Some((token.to_owned(), probe(token).await?))
    } else {
        if add_account {
            return Err(failed("A new account needs a credential."));
        }
        None
    };
    let guard = state.issues.lock().await;
    let path = &guard.tracker_file;
    let mut record = read(path).await?;
    if !record["accounts"].is_array() {
        record["accounts"] = json!([]);
    }
    // `requireAccount`: a named account must be one this server saved.
    let saved = |record: &Value, id: &Option<String>| match id {
        None => true,
        Some(id) => record["accounts"]
            .as_array()
            .is_some_and(|all| all.iter().any(|a| a["accountId"] == id.as_str())),
    };
    match method {
        "issueTrackers.list" => {
            drop(guard);
            let current = |token: Option<&str>, id: Option<&str>| {
                let token = token.map(str::to_owned);
                let id = id.map(str::to_owned);
                async move {
                    let row = match token {
                        Some(token) => probe(&token).await.unwrap_or_else(|e| {
                            status(true, Value::Null, e["detail"].clone(), json!([]))
                        }),
                        None => disconnected(),
                    };
                    with_account(row, id.as_deref())
                }
            };
            let first = current(record["token"].as_str(), None).await;
            let added = record["accounts"].as_array().cloned().unwrap_or_default();
            let accounts = futures_util::future::join_all(
                added
                    .iter()
                    .map(|a| current(a["token"].as_str(), a["accountId"].as_str())),
            )
            .await;
            // A project bound with an added account is listed apart, so older
            // clients never read another account's team as the first's.
            let mut bindings = serde_json::Map::new();
            let mut account_bindings = serde_json::Map::new();
            for (project, binding) in record["bindings"].as_object().cloned().unwrap_or_default() {
                if binding.get("accountId").is_some() {
                    account_bindings.insert(project, binding);
                } else {
                    bindings.insert(project, binding);
                }
            }
            Ok(
                json!({"trackers":[first],"accounts":accounts,"bindings":bindings,"accountBindings":account_bindings}),
            )
        }
        "issueTrackers.connect" => {
            let named = account_input(&input["accountId"])?;
            let Some((token, connection)) = connected else {
                // Disconnect: forget the credential, and unbind its projects.
                if !saved(&record, &named) {
                    return Err(failed("This account is not saved on the server."));
                }
                match &named {
                    None => record["token"] = Value::Null,
                    Some(id) => record["accounts"]
                        .as_array_mut()
                        .unwrap()
                        .retain(|a| a["accountId"] != id.as_str()),
                }
                record["bindings"]
                    .as_object_mut()
                    .unwrap()
                    .retain(|_, b| b["accountId"].as_str() != named.as_deref());
                write(path, &record, "The credential could not be removed.").await?;
                return Ok(with_account(disconnected(), named.as_deref()));
            };
            let account_id = if add_account {
                Some(Uuid::new_v4().to_string())
            } else {
                if !saved(&record, &named) {
                    return Err(failed("This account is not saved on the server."));
                }
                named
            };
            // `heldElsewhere`: one credential is one account.
            let held = std::iter::once((None, record["token"].as_str()))
                .chain(
                    record["accounts"]
                        .as_array()
                        .into_iter()
                        .flatten()
                        .map(|a| (a["accountId"].as_str(), a["token"].as_str())),
                )
                .any(|(id, stored)| id != account_id.as_deref() && stored == Some(token.as_str()));
            if held {
                return Err(failed(
                    "This credential is already connected as another account.",
                ));
            }
            match &account_id {
                None => record["token"] = json!(token),
                Some(id) if add_account => record["accounts"].as_array_mut().unwrap().push(
                    json!({"accountId":id,"token":token,"createdAt":crate::auth_service::iso(crate::platform::now_ms())}),
                ),
                Some(id) => {
                    for account in record["accounts"].as_array_mut().unwrap() {
                        if account["accountId"] == id.as_str() {
                            account["token"] = json!(token);
                        }
                    }
                }
            }
            write(
                path,
                &record,
                if add_account {
                    "The account could not be saved."
                } else {
                    "The credential could not be stored."
                },
            )
            .await?;
            Ok(with_account(connection, account_id.as_deref()))
        }
        _ => {
            let project = input["projectId"]
                .as_str()
                .ok_or_else(|| failed("The project binding could not be saved."))?;
            if input["binding"].is_null() {
                // An unbind that does not name the binding's account leaves it alone:
                // older clients offer a project bound with an added account as unbound.
                let named = input["accountId"].as_str();
                let current = &record["bindings"][project];
                if !current.is_null() && current["accountId"].as_str() != named {
                    return Ok(Value::Null);
                }
                record["bindings"].as_object_mut().unwrap().remove(project);
            } else {
                let account_id = account_input(&input["binding"]["accountId"])?;
                if !saved(&record, &account_id) {
                    return Err(failed("This account is not saved on the server."));
                }
                // `tracker`, the account, then `scope`, as the reference lists a binding.
                let mut binding = json!({"tracker":input["binding"]["tracker"]});
                if let Some(id) = &account_id {
                    binding["accountId"] = json!(id);
                }
                binding["scope"] = input["binding"]["scope"].clone();
                record["bindings"][project] = binding;
            }
            write(path, &record, "The project binding could not be saved.").await?;
            Ok(Value::Null)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_binding_reads_with_its_own_account() {
        let record =
            json!({"token": "first", "accounts": [{"accountId": "a-1", "token": "second"}]});
        assert_eq!(
            binding_token(&record, &json!({"tracker": "linear"})),
            Some("first")
        );
        assert_eq!(
            binding_token(&record, &json!({"accountId": "a-1"})),
            Some("second")
        );
        assert_eq!(binding_token(&record, &json!({"accountId": "gone"})), None);
        assert_eq!(
            binding_token(&json!({"token": "only"}), &json!({})),
            Some("only")
        );
        assert!(valid_account_id("0b6a3c1e-5d2f-4a8b-9c7d-1e2f3a4b5c6d"));
        assert!(!valid_account_id("-x") && !valid_account_id("Upper") && !valid_account_id(""));
        let row = with_account(
            json!({"tracker": "linear", "credential": {}, "connected": true}),
            Some("a-1"),
        );
        assert_eq!(
            row.as_object().unwrap().keys().collect::<Vec<_>>(),
            ["tracker", "credential", "accountId", "connected"]
        );
    }
}
