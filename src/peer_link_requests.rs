//! Answers to the link cards agents show (`PeerLinkRequests.answer` in the
//! reference).
//!
//! An agent asks the user to link another machine with
//! `t3_environment_link`; the card waits in its thread. The user's client
//! answers it with the machine's addresses and a pairing code, which go
//! straight to [`crate::peer_links::PeerLinks::link`] and never into the
//! thread. The card then records how it ended, and the agent hears it as a
//! notification that starts or follows a turn.
use crate::peer_links::LinkInput;
use crate::server::AppState;
use serde_json::{json, Value};
use std::collections::HashSet;
use std::sync::{Arc, Mutex};

/// Why an answer failed before anything was linked (`PeerLinkRequestError`).
pub(crate) struct AnswerError {
    reason: &'static str,
    cause: Option<Value>,
}

impl AnswerError {
    fn new(reason: &'static str) -> Self {
        Self {
            reason,
            cause: None,
        }
    }
    pub(crate) fn to_json(&self) -> Value {
        let mut error = json!({"_tag": "PeerLinkRequestError", "reason": self.reason});
        if let Some(cause) = &self.cause {
            error["cause"] = cause.clone();
        }
        error
    }
}

/// The cards being answered now: one answer per card at a time.
#[derive(Default)]
pub(crate) struct Answering(Mutex<HashSet<String>>);

struct Held<'a>(&'a Answering, String);
impl Drop for Held<'_> {
    fn drop(&mut self) {
        self.0 .0.lock().unwrap().remove(&self.1);
    }
}

/// What the card's ask calls its target: its address, its id, or the user's pick.
fn target(card: &Value) -> String {
    card["url"]
        .as_str()
        .or(card["environmentId"].as_str())
        .unwrap_or("a machine")
        .to_owned()
}

/// The notification an answer sends the agent (`outcomeNotice`). It carries
/// the outcome only, never the pairing code.
fn notice(card: &Value, outcome: &Value) -> (String, Value) {
    let target = target(card);
    let notification = |kind: &str, summary: String| json!({"source": {"kind": "background_task"}, "outcome": kind, "summary": summary});
    match outcome["linkStatus"].as_str() {
        Some("linked") => {
            let label = outcome["linkedLabel"].as_str().unwrap_or(&target).to_owned();
            (
                format!(
                    "The user linked {label} with {} access. Pass environmentId {} to act there.",
                    outcome["linkedAccess"].as_str().unwrap_or_default(),
                    outcome["linkedEnvironmentId"].as_str().unwrap_or_default()
                ),
                notification("completed", format!("Linked {label}")),
            )
        }
        Some("failed") => (
            format!(
                "Linking {target} failed: {}. Call t3_environment_link again if the user still wants it.",
                outcome["failure"].as_str().unwrap_or("the other environment refused")
            ),
            notification("failed", format!("Could not link {target}")),
        ),
        _ => (
            format!("The user declined to link {target}."),
            notification("cancelled", format!("Declined to link {target}")),
        ),
    }
}

/// The outcome of linking the machine the user picked, or of keeping a link.
async fn outcome(state: &AppState, answer: &Value) -> Value {
    let failed = |message: String| json!({"linkStatus": "failed", "failure": message});
    let linked = |link: &crate::peer_links::PeerLink| {
        json!({"linkStatus": "linked", "linkedEnvironmentId": link.environment_id,
               "linkedLabel": link.label, "linkedAccess": link.access})
    };
    match answer["type"].as_str() {
        Some("use-existing") => {
            let id = answer["environmentId"].as_str().unwrap_or_default();
            match state.peer_links.get(id) {
                Ok(Some(link)) if !link.expired(crate::platform::now_ms()) => linked(&link),
                Ok(_) => failed(
                    "That environment is no longer linked here, or its link expired. Link it again.".into(),
                ),
                Err(error) => failed(error.message),
            }
        }
        Some("link") => {
            let urls: Vec<String> = answer["urls"]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(|u| u.as_str().map(str::to_owned))
                .collect();
            let input = LinkInput {
                url: urls.first().cloned().unwrap_or_default(),
                alternate_urls: urls.iter().skip(1).cloned().collect(),
                pairing_code: answer["pairingCode"]
                    .as_str()
                    .unwrap_or_default()
                    .to_owned(),
                access: answer["access"].as_str().unwrap_or_default().to_owned(),
                expected_environment_id: answer["environmentId"].as_str().map(str::to_owned),
                expected_label: answer["label"].as_str().map(str::to_owned),
            };
            let descriptor = crate::environment_http::descriptor_value(state);
            let own = crate::peer_links::SelfDescriptor {
                environment_id: state.environment_id.clone(),
                label: descriptor["label"].as_str().unwrap_or("T3 Code").to_owned(),
            };
            let enabled = crate::mcp_oauth::McpHost::linked_environments_enabled(state);
            match state.peer_links.link(enabled, &own, &input).await {
                Ok(link) => linked(&link),
                Err(error) => failed(error.message),
            }
        }
        _ => json!({"linkStatus": "declined"}),
    }
}

/// `peerLinks.answerRequest`: answers a pending card and tells the agent.
pub(crate) async fn answer(state: &Arc<AppState>, params: &Value) -> Result<(), AnswerError> {
    let thread_id = params["threadId"].as_str().unwrap_or_default().to_owned();
    let card_id = params["turnItemId"].as_str().unwrap_or_default().to_owned();
    let answer = &params["answer"];
    {
        let mut answering = state.link_answers.0.lock().unwrap();
        if !answering.insert(card_id.clone()) {
            return Err(AnswerError::new("already_answered"));
        }
    }
    let _held = Held(&state.link_answers, card_id.clone());
    let card = {
        let store = state.store.read().await;
        let Some(thread) = store.threads.get(&thread_id) else {
            return Err(AnswerError {
                reason: "load_failed",
                cause: Some(
                    json!({"name":"OrchestratorProjectionError","message":format!("Failed to load orchestration projection for thread {thread_id}."),"cause":{"name":"ProjectionStoreThreadNotFoundError","message":format!("No orchestration projection exists for thread {thread_id}.")}}),
                ),
            });
        };
        let card = thread
            .link_requests
            .iter()
            .find(|c| c["id"] == card_id.as_str())
            .filter(|_| thread.stored_row()["deletedAt"].is_null())
            .cloned();
        match card {
            Some(card) if !card["runId"].is_null() && !card["nodeId"].is_null() => card,
            _ => return Err(AnswerError::new("not_found")),
        }
    };
    if card["linkStatus"] != "pending" {
        return Err(AnswerError::new("already_answered"));
    }
    // An id beside an address is what answered there, recorded by an older
    // server: linking that machine could send its pairing code to the address.
    let outcome = if card["url"].is_string()
        && card["environmentId"].is_string()
        && answer["type"] == "link"
        && answer["environmentId"].is_string()
    {
        json!({"linkStatus": "failed", "failure": "This request is from before an update. Ask for the link again."})
    } else {
        outcome(state, answer).await
    };
    let status = outcome["linkStatus"]
        .as_str()
        .unwrap_or("declined")
        .to_owned();
    let (text, notification) = notice(&card, &outcome);
    let mut command =
        crate::mcp_oauth::card_command(&card, &format!("link-request:{card_id}:{status}"));
    if let Some(fields) = outcome.as_object() {
        for (key, value) in fields {
            command[key] = value.clone();
        }
    }
    command["wake"] = json!({
        "messageId": format!("message:link-request:{card_id}"),
        "text": text,
        "notification": notification,
    });
    let answering: Vec<String> = state
        .link_answers
        .0
        .lock()
        .unwrap()
        .iter()
        .cloned()
        .collect();
    command["answeringTurnItemIds"] = json!(answering);
    crate::dispatch_orchestration_command(state.clone(), command)
        .await
        .map(|_| ())
        .map_err(|cause| AnswerError {
            reason: "record_failed",
            cause: Some(cause),
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn notices_say_how_the_card_ended() {
        let card = json!({"url": "https://vps.example.com"});
        let (text, notification) = notice(
            &card,
            &json!({"linkStatus": "linked", "linkedEnvironmentId": "env-1", "linkedLabel": "vps", "linkedAccess": "auto"}),
        );
        assert_eq!(
            text,
            "The user linked vps with auto access. Pass environmentId env-1 to act there."
        );
        assert_eq!(notification["summary"], "Linked vps");
        let (text, notification) = notice(&json!({}), &json!({"linkStatus": "declined"}));
        assert_eq!(text, "The user declined to link a machine.");
        assert_eq!(notification["outcome"], "cancelled");
        let (text, _) = notice(
            &json!({"environmentId": "env-2"}),
            &json!({"linkStatus": "failed", "failure": "nope"}),
        );
        assert_eq!(
            text,
            "Linking env-2 failed: nope. Call t3_environment_link again if the user still wants it."
        );
    }
}
