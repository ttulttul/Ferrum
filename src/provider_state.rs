//! What a thread's provider turns leave behind in the store, rebuilt from
//! the recorded [`Event::RunProvider`](crate::Event::RunProvider) events:
//! the native thread (Codex) or session (Claude) the next turn resumes.
use crate::workspace::provider::{self, ProviderTurn};
use crate::workspace::provider::{ProviderEvent, ProviderFailure, ThreadDisposition, TurnStatus};
use serde_json::Value;
use std::collections::HashMap;
use uuid::Uuid;

/// A thread's provider state. Not serialized: the store rebuilds it from
/// the event log.
#[derive(Clone, Debug, Default)]
pub struct ProviderState {
    /// The native thread the next turn resumes, if it may.
    pub native_thread: Option<NativeThread>,
    /// How each run's provider turn ended, from its `turn.terminal`.
    pub terminals: HashMap<Uuid, Terminal>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Terminal {
    pub status: TurnStatus,
    pub failure: Option<ProviderFailure>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NativeThread {
    pub driver: String,
    pub id: String,
}

impl ProviderState {
    pub(crate) fn apply(&mut self, run: Uuid, event: &ProviderEvent) {
        if let ProviderEvent::TurnTerminal {
            status, failure, ..
        } = event
        {
            self.terminals.insert(
                run,
                Terminal {
                    status: *status,
                    failure: failure.clone(),
                },
            );
        }
        match event {
            ProviderEvent::ProviderThread {
                driver,
                native_thread_id,
                ..
            } => {
                self.native_thread = Some(NativeThread {
                    driver: driver.clone(),
                    id: native_thread_id.clone(),
                });
            }
            ProviderEvent::TurnTerminal {
                thread_disposition: ThreadDisposition::Broken,
                ..
            } => self.native_thread = None,
            _ => {}
        }
    }

    /// The native thread a turn of `driver` resumes.
    pub fn resume_for(&self, driver: &str) -> Option<String> {
        self.native_thread
            .as_ref()
            .filter(|thread| thread.driver == driver)
            .map(|thread| thread.id.clone())
    }
}

/// How a turn of a thread runs its provider: the CLI of the thread's
/// provider instance (from `settings.providerInstances`, or the built-in
/// instance of the same name) with the thread's model selection, resuming
/// the thread's native thread. `None` for an instance whose driver the
/// workspace does not run.
pub(crate) fn provider_turn(
    settings: &Value,
    row: &Value,
    state: &ProviderState,
) -> Option<(ProviderTurn, Vec<(String, String)>)> {
    let selection = &row["modelSelection"];
    let instance_id = selection["instanceId"]
        .as_str()
        .or(row["providerInstanceId"].as_str())
        .unwrap_or(provider::CODEX);
    let instance = &settings["providerInstances"][instance_id];
    let driver = instance["driver"].as_str().unwrap_or(instance_id);
    if driver != provider::CODEX && driver != provider::CLAUDE_AGENT {
        return None;
    }
    let config = &instance["config"];
    let text = |key: &str| config[key].as_str().unwrap_or_default().trim().to_owned();
    let reasoning_effort = selection["options"].as_array().and_then(|options| {
        options
            .iter()
            .find(|option| option["id"] == "reasoningEffort")
            .and_then(|option| option["value"].as_str())
            .map(str::to_owned)
    });
    let env = instance["environment"]
        .as_array()
        .map(|variables| {
            variables
                .iter()
                .filter_map(|variable| {
                    Some((
                        variable["name"].as_str()?.to_owned(),
                        variable["value"].as_str()?.to_owned(),
                    ))
                })
                .collect()
        })
        .unwrap_or_default();
    let turn = ProviderTurn {
        driver: driver.to_owned(),
        binary_path: text("binaryPath"),
        launch_args: split_args(&text("launchArgs")),
        home_path: text("homePath"),
        model: selection["model"]
            .as_str()
            .filter(|model| !model.is_empty() && *model != "default")
            .unwrap_or_default()
            .to_owned(),
        reasoning_effort,
        resume: state.resume_for(driver),
    };
    Some((turn, env))
}

/// Split launch arguments as a shell would split plain words and quoted
/// strings (no expansion).
fn split_args(text: &str) -> Vec<String> {
    let mut args = Vec::new();
    let mut current = String::new();
    let mut quote = None;
    let mut started = false;
    let mut chars = text.chars();
    while let Some(c) = chars.next() {
        match (quote, c) {
            (Some(q), c) if c == q => quote = None,
            (Some('"'), '\\') => current.extend(chars.next()),
            (Some(_), c) => current.push(c),
            (None, '"' | '\'') => {
                quote = Some(c);
                started = true;
            }
            (None, '\\') => {
                current.extend(chars.next());
                started = true;
            }
            (None, c) if c.is_whitespace() => {
                if started {
                    args.push(std::mem::take(&mut current));
                    started = false;
                }
            }
            (None, c) => {
                current.push(c);
                started = true;
            }
        }
    }
    if started {
        args.push(current);
    }
    args
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn a_turn_takes_its_instance_settings_selection_and_native_thread() {
        let settings = json!({"providerInstances": {"work": {
            "driver": "codex",
            "config": {"binaryPath": "/opt/codex", "launchArgs": "--profile 'my work' -c x=1", "homePath": "/h"},
            "environment": [{"name": "A", "value": "1", "sensitive": false}]
        }}});
        let row = json!({"modelSelection": {"instanceId": "work", "model": "gpt-5",
            "options": [{"id": "reasoningEffort", "value": "high"}]}});
        let mut state = ProviderState::default();
        state.apply(
            Uuid::nil(),
            &ProviderEvent::ProviderThread {
                driver: "codex".into(),
                native_thread_id: "thr_1".into(),
                resumed: false,
                model: None,
            },
        );
        let (turn, env) = provider_turn(&settings, &row, &state).unwrap();
        assert_eq!(turn.driver, "codex");
        assert_eq!(turn.binary_path, "/opt/codex");
        assert_eq!(turn.launch_args, ["--profile", "my work", "-c", "x=1"]);
        assert_eq!(turn.home_path, "/h");
        assert_eq!(turn.model, "gpt-5");
        assert_eq!(turn.reasoning_effort.as_deref(), Some("high"));
        assert_eq!(turn.resume.as_deref(), Some("thr_1"));
        assert_eq!(env, [("A".to_string(), "1".to_string())]);

        // A built-in instance needs no settings; another driver's native
        // thread is not resumed.
        let claude = json!({"modelSelection": {"instanceId": "claudeAgent", "model": "default"}});
        let (turn, _) = provider_turn(&json!({}), &claude, &state).unwrap();
        assert_eq!(
            (turn.driver.as_str(), turn.model.as_str()),
            ("claudeAgent", "")
        );
        assert_eq!(turn.resume, None);
        let other = json!({"modelSelection": {"instanceId": "cursor", "model": "x"}});
        assert!(provider_turn(&json!({}), &other, &state).is_none());
    }
}
