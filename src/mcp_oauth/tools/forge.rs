//! Shared thread access and result shaping for the issue and pull-request toolkits.
use super::*;

pub(super) fn fields(row: &Value, names: &[&str]) -> Value {
    Value::Object(
        names
            .iter()
            .filter_map(|&name| row.get(name).map(|value| (name.to_owned(), value.clone())))
            .collect(),
    )
}

pub(super) fn rows(value: &Value) -> &[Value] {
    value.as_array().map_or(&[], Vec::as_slice)
}

pub(super) fn message(text: impl Into<String>) -> Refusal {
    Refusal::Text(text.into())
}

pub(super) async fn thread<S: McpHost>(
    host: &S,
    caller: &Caller,
    args: &Value,
    write: bool,
    issue: bool,
) -> Outcome {
    let requested = args["threadId"].as_str();
    if write {
        threads::check_targets(host, caller, &[requested]).await?;
    }
    if issue {
        require_issues(caller)?;
    }
    let id = requested
        .or_else(|| caller.thread.as_ref().map(|t| t.id.as_str()))
        .ok_or_else(|| {
            message("Pass threadId: this MCP client is not running inside a T3 thread.")
        })?;
    let store = host.control_state().store.read().await;
    if let Ok(thread) = find_thread(&store, id) {
        return Ok(thread.shell());
    }
    let missing = format!("Thread {id} was not found.");
    Err(if issue {
        message(missing)
    } else {
        threads::thread_not_found_hint(host, &store, caller, id, &missing)
    })
}

pub(super) fn require_issues(caller: &Caller) -> Result<(), Refusal> {
    if caller.granted.thread.is_none() {
        return Err(message(
            "MCP credential does not grant the issues capability.",
        ));
    }
    Ok(())
}

pub(super) async fn writer<S: McpHost>(host: &S, caller: &Caller) -> Result<(), Refusal> {
    threads::writing_caller(host, caller).await?;
    let store = host.control_state().store.read().await;
    if let Some(origin) = threads::fencing_origin(&store, caller) {
        return Err(failure("capability_denied", format!("This work was started from {} through a linked environment, so it cannot start work here that is not its link's. A link paired with full access is not limited this way.", origin["label"].as_str().unwrap_or(""))));
    }
    Ok(())
}

pub(super) async fn dispatch<S: McpHost>(
    host: &Arc<S>,
    thread: &Value,
    kind: &str,
    tag: &str,
    mut params: Value,
    error: &str,
) -> Result<(), Refusal> {
    let id = thread["id"].as_str().ok_or_else(|| message(error))?;
    params["commandId"] = json!(format!("server:{tag}:{id}:{}", crate::platform::uuid_v4()));
    params["threadId"] = json!(id);
    params["type"] = json!(kind);
    host.dispatch(params)
        .await
        .map(|_| ())
        .map_err(|_| message(error))
}
