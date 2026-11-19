use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct PullRequestRef { pub project_id: String, pub repository: String, pub number: u64 }

#[derive(Clone, Debug, Default)]
pub struct PullRequestService;

impl PullRequestService {
    pub fn diff(&self, patch: &str, cursor: Option<&str>, limit: usize) -> Value {
        let bytes = patch.as_bytes();
        let offset = cursor.and_then(|value| value.parse::<usize>().ok()).unwrap_or(0).min(bytes.len());
        let end = (offset + limit.max(1)).min(bytes.len());
        let next = (end < bytes.len()).then(|| end.to_string());
        json!({"patch":String::from_utf8_lossy(&bytes[offset..end]),"truncated":next.is_some(),"nextCursor":next,"withheldFilesReadable":false})
    }
    pub fn file_contents(&self, old: &str, new: &str) -> Value { json!({"oldContents":old,"newContents":new}) }
    pub fn unavailable(&self, reason: &str, provider: Option<&str>) -> Value {
        let message = match reason { "cli-missing" => "The tool this host is read through is not installed or set up.", "cli-unauthenticated" => "This host has no working credentials.", "provider-unsupported" => "Change requests cannot be browsed for this project's host yet.", _ => "Pull requests are unavailable." };
        json!({"_tag":"PullRequestUnavailableError","reason":reason,"provider":provider,"message":message})
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test] fn diff_paginates_with_cursor() { let service = PullRequestService; let first = service.diff("abcdef", None, 3); assert_eq!(first["patch"], "abc"); assert_eq!(first["nextCursor"], "3"); let second = service.diff("abcdef", Some("3"), 3); assert_eq!(second["patch"], "def"); assert_eq!(second["nextCursor"], Value::Null); }
    #[test] fn errors_use_contract_tag() { assert_eq!(PullRequestService.unavailable("cli-missing", None)["_tag"], "PullRequestUnavailableError"); }
}
