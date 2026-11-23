//! `html_preview` and `html_render` (the reference's HTML toolkit): an agent
//! checks a page it wrote in a headless browser, then shows it in its
//! thread. The page work is the host's (`McpHost::html_page`); these handlers
//! check the caller and the page source as the reference's do.
use super::*;

/// The marker under which a tool hands the transport its whole MCP result,
/// as a tool whose answer carries image content does.
pub(crate) const RAW_RESULT: &str = "\u{0}callToolResult";

/// `pageSource`: exactly one of `html` and `path`, and a path only from a
/// full-access thread, since the server reads that file for the caller.
fn page_source(caller: &Caller, a: &Value) -> Result<Value, Refusal> {
    let html = a.get("html").filter(|v| !v.is_null());
    let path = a.get("path").filter(|v| !v.is_null());
    match (html, path) {
        (Some(html), None) => Ok(json!({"html": html})),
        (_, Some(path)) if html.is_none() => {
            let full_access = caller
                .thread
                .as_ref()
                .is_some_and(|thread| thread.runtime_mode == "full-access");
            if !full_access {
                return Err(failure(
                    "capability_denied",
                    "Pass html instead of path: only full-access threads may pass a path.",
                ));
            }
            Ok(json!({"path": path}))
        }
        _ => Err(failure(
            "invalid_request",
            "Pass exactly one of html or path.",
        )),
    }
}

/// `imageToolFailure`: a failed image tool's answer, its message in both the
/// text and the structured content.
pub(super) fn image_failure(tag: &str, message: String) -> Value {
    json!({RAW_RESULT: {
        "isError": true,
        "structuredContent": {"error": {
            "_tag": tag,
            "operation": "preview",
            "failureCount": 1,
            "message": message,
        }},
        "content": [{"type": "text", "text": message}],
    }})
}

/// `html_preview` (`readsAsCaller`): the page's screenshot as image content,
/// with its height and console output as text and structured content.
pub(super) async fn preview<S: McpHost>(host: &Arc<S>, caller: &Caller, a: &Value) -> Outcome {
    if caller.granted.thread.is_none() {
        return Err(calling_thread_missing());
    }
    let source = match page_source(caller, a) {
        Ok(source) => source,
        Err(Refusal::Failure(_, message)) => return Ok(image_failure("OrchestratorMcpFailure", message)),
        Err(other) => return Err(other),
    };
    let mut input = source;
    for key in ["width", "appearance"] {
        if let Some(value) = a.get(key).filter(|v| !v.is_null()) {
            input[key] = value.clone();
        }
    }
    match host.html_page("preview", input).await {
        Err((_, message)) => Ok(image_failure("OrchestratorMcpFailure", message)),
        Ok(mut result) => {
            let screenshot = result["screenshot"].take();
            result["screenshot"] = json!({
                "mimeType": screenshot["mimeType"],
                "width": screenshot["width"],
                "height": screenshot["height"],
            });
            Ok(json!({RAW_RESULT: {
                "isError": false,
                "structuredContent": result,
                "content": [
                    {"type": "text", "text": result.to_string()},
                    {"type": "image", "data": screenshot["data"], "mimeType": screenshot["mimeType"]},
                ],
            }}))
        }
    }
}

/// `html_render` (`actsAsCaller`): the page stored for the calling thread to
/// show above the agent's reply.
pub(super) async fn render<S: McpHost>(host: &Arc<S>, caller: &Caller, a: &Value) -> Outcome {
    let Some(scope) = caller.granted.thread.as_ref() else {
        return Err(failure(
            "thread_credential_required",
            "This tool acts as the calling T3 thread, so it needs an agent running inside T3 Code. This MCP client signed in from outside a thread.",
        ));
    };
    threads::writing_caller(&**host, caller).await?;
    let mut input = page_source(caller, a)?;
    input["threadId"] = json!(scope.thread_id);
    input["title"] = a["title"].clone();
    input["height"] = a["height"].clone();
    let reference = host
        .html_page("publish", input)
        .await
        .map_err(|(code, message)| failure(code, message))?;
    Ok(json!({
        "htmlRender": reference,
        "message": "Shown to the reader above your reply. Don't mention or describe the page; reply with only what it doesn't already say.",
    }))
}
