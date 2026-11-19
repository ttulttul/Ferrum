use crate::{issues::IssueService, pull_requests::PullRequestService, voice::{CloudProvider, VoiceService}, AppState};
use serde_json::{json, Value};
use std::sync::Arc;

/// Dispatch provider-backed RPC calls that can be answered from local state or
/// explicit fixture inputs. `None` means the method belongs to another domain.
pub async fn dispatch(_state: &AppState, method: &str, params: &Value) -> Option<Result<Value, Value>> {
    let voice = VoiceService::new();
    let pull_requests = PullRequestService;
    match method {
        "server.getUsageSummary" => Some(Ok(json!({"sources":[],"totalOutputTokens":0,"totalInputTokens":0}))),
        "server.getConfig" => Some(Ok(json!({"providers":[],"configurableProviders":false}))),
        "server.probe" => Some(Ok(json!({}))),
        "server.listTextGeneratorModels" => Some(Ok(json!({"models":[]}))),
        "server.getSettings" => Some(Ok(json!({"settings":{}}))),
        "subscribeServerConfig" => Some(Ok(json!({"version":1,"type":"snapshot","config":{"providers":[]}}))),
        "api.voice.cloud.keys" | "voice.cloudKeys" => Some(Ok(voice.cloud_keys())),
        "api.voice.cloud.models" | "voice.cloudModels" => Some(Ok(voice.cloud_models())),
        "voice.postProcess" | "speech.postProcess" => {
            let text = params.get("text").and_then(Value::as_str).unwrap_or_default();
            let words = params.get("customWords").and_then(Value::as_array).map(|words| words.iter().filter_map(Value::as_str).map(str::to_owned).collect::<Vec<_>>()).unwrap_or_default();
            Some(Ok(json!({"text":voice.post_process(text, &words)})))
        }
        "voice.transcribe" | "speech.transcribe" => {
            let provider = provider(params.get("provider").and_then(Value::as_str));
            Some(Err(voice.transcribe_unavailable(provider)))
        }
        "pullRequests.diff" | "pullRequests.preview" => {
            let Some(patch) = params.get("patch").and_then(Value::as_str) else { return Some(Err(pull_requests.unavailable("cli-unauthenticated", None))); };
            let cursor = params.get("cursor").and_then(Value::as_str);
            let limit = params.get("limit").and_then(Value::as_u64).unwrap_or(64 * 1024) as usize;
            Some(Ok(pull_requests.diff(patch, cursor, limit)))
        }
        "pullRequests.diffFileContents" => {
            Some(Ok(pull_requests.file_contents(params.get("oldContents").and_then(Value::as_str).unwrap_or_default(), params.get("newContents").and_then(Value::as_str).unwrap_or_default())))
        }
        "pullRequests.list" | "pullRequests.summary" | "pullRequests.routing" => Some(Err(pull_requests.unavailable("cli-unauthenticated", None))),
        "issues.list" => Some(Ok(IssueService::new().list(params.get("tracker").and_then(Value::as_str), params.get("state").and_then(Value::as_str)))),
        "issues.create" => {
            let Some(key) = params.get("key").and_then(Value::as_str) else { return Some(Err(IssueService::new().unavailable("tracker-unsupported", params.get("tracker").and_then(Value::as_str)))); };
            let reference = crate::issues::IssueRef { project_id: params.get("projectId").and_then(Value::as_str).unwrap_or_default().into(), tracker: params.get("tracker").and_then(Value::as_str).unwrap_or("github").into(), scope: params.get("scope").and_then(Value::as_str).unwrap_or_default().into(), key: key.into(), host: params.get("host").and_then(Value::as_str).map(str::to_owned) };
            let mut service = IssueService::new();
            Some(Ok(service.create(reference, params.get("title").and_then(Value::as_str).unwrap_or_default().into(), params.get("body").and_then(Value::as_str).unwrap_or_default().into(), strings(params.get("labels")), strings(params.get("assignees")))))
        }
        "issues.detail" | "issues.comments" | "issues.update" | "issues.setState" | "issues.comment" | "issues.react" | "issues.templates" | "issues.candidates" | "issueTrackers.list" | "issueTrackers.connect" | "issueTrackers.bindProject" => Some(Err(IssueService::new().unavailable("tracker-unsupported", params.get("tracker").and_then(Value::as_str)))),
        _ => None,
    }
}

fn strings(value: Option<&Value>) -> Vec<String> { value.and_then(Value::as_array).map(|values| values.iter().filter_map(Value::as_str).map(str::to_owned).collect()).unwrap_or_default() }
fn provider(value: Option<&str>) -> CloudProvider { match value.unwrap_or("openai") { "xai" => CloudProvider::Xai, "openrouter" => CloudProvider::Openrouter, _ => CloudProvider::Openai } }

#[allow(dead_code)]
fn _state_type_is_send_sync(_: Arc<AppState>) {}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn unknown_methods_are_left_for_other_domains() {
        assert!(dispatch_unavailable("unknown.method").await.is_none());
    }
    #[tokio::test]
    async fn voice_errors_when_provider_is_unconfigured() {
        let result = dispatch_unavailable("voice.transcribe").await.unwrap().unwrap_err();
        assert_eq!(result["_tag"], "CloudSpeechError");
    }
    async fn dispatch_unavailable(method: &str) -> Option<Result<Value, Value>> {
        let state = AppState::new(std::env::temp_dir().join("t3-provider-rpc-test"), vec![]).await.unwrap();
        dispatch(&state, method, &json!({})).await
    }
}
