//! Provider turns against fake CLIs that replay recorded Codex app-server and
//! Claude Code stream-json transcripts (`tests/fixtures/providers`).
use super::*;
use crate::workspace::provider::{
    ProviderTurn, TurnStatus, ACP_REGISTRY, ANTIGRAVITY, CLAUDE_AGENT, GROK, OPENCODE, PI,
};
use serde_json::{json, Value};
use std::path::{Path, PathBuf};

static TEST_SERVERS: std::sync::LazyLock<std::sync::Arc<ProviderProcesses>> =
    std::sync::LazyLock::new(Default::default);

fn fixtures() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/providers")
}

/// A turn of `driver` run by the fake CLI playing `scenario`. The fake logs
/// what it receives to `log`.
fn fake_spec(driver: &str, scenario: &str, log: &Path, resume: Option<&str>) -> TurnSpec {
    TurnSpec {
        prompt: "Say hello".into(),
        cwd: std::env::temp_dir().display().to_string(),
        env: vec![
            (
                "FAKE_PROVIDER_SCENARIO".into(),
                fixtures().join(scenario).display().to_string(),
            ),
            ("FAKE_PROVIDER_LOG".into(), log.display().to_string()),
        ],
        provider: Some(ProviderTurn {
            driver: driver.into(),
            binary_path: fixtures().join("fake_cli.py").display().to_string(),
            resume: resume.map(str::to_owned),
            // Each turn its own thread: a thread keeps its ACP agent (and what
            // a Stop left) for its next turn, across the tests' shared processes.
            app_thread_id: format!("thread:{scenario}:{}", log.display()),
            ..ProviderTurn::default()
        }),
        ..TurnSpec::default()
    }
}

async fn collect(mut events: mpsc::UnboundedReceiver<TurnEvent>) -> (Vec<ProviderEvent>, TurnExit) {
    let mut provider = vec![];
    let deadline = std::time::Duration::from_secs(120);
    loop {
        match tokio::time::timeout(deadline, events.recv()).await {
            Ok(Some(TurnEvent::Provider(event))) => provider.push(event),
            Ok(Some(TurnEvent::Exited(exit))) => return (provider, exit),
            Ok(Some(other)) => panic!("unexpected event {other:?}"),
            Ok(None) => panic!("the turn ended without exiting"),
            Err(_) => panic!("the turn timed out; events so far: {provider:#?}"),
        }
    }
}

async fn run(spec: TurnSpec) -> (Vec<ProviderEvent>, TurnExit) {
    let (_cancel, cancelled) = oneshot::channel();
    collect(start(spec, cancelled, TEST_SERVERS.clone()).unwrap()).await
}

fn received(log: &Path) -> Vec<Value> {
    std::fs::read_to_string(log)
        .unwrap_or_default()
        .lines()
        .filter_map(|line| serde_json::from_str(line).ok())
        .collect()
}

fn terminal(events: &[ProviderEvent]) -> (TurnStatus, Option<ProviderFailure>) {
    match events.last() {
        Some(ProviderEvent::TurnTerminal {
            status, failure, ..
        }) => (*status, failure.clone()),
        other => panic!("the last event is not terminal: {other:?}"),
    }
}

fn items<'a>(
    events: &'a [ProviderEvent],
    kind: &str,
) -> Vec<&'a crate::workspace::provider::ProviderItem> {
    events
        .iter()
        .filter_map(|event| match event {
            ProviderEvent::TurnItem { item } if item.kind == kind => Some(item),
            _ => None,
        })
        .collect()
}

fn final_text(events: &[ProviderEvent]) -> String {
    items(events, "assistant_message")
        .into_iter()
        .rev()
        .find(|item| item.status == "completed")
        .and_then(|item| item.fields["text"].as_str())
        .unwrap_or_default()
        .to_owned()
}

fn native_thread(events: &[ProviderEvent]) -> (String, bool) {
    events
        .iter()
        .find_map(|event| match event {
            ProviderEvent::ProviderThread {
                native_thread_id,
                resumed,
                ..
            } => Some((native_thread_id.clone(), *resumed)),
            _ => None,
        })
        .expect("the turn names its native thread")
}

#[tokio::test]
async fn codex_turn_streams_text_tool_calls_and_usage() {
    let dir = tempfile::tempdir().unwrap();
    let log = dir.path().join("in.jsonl");
    let (events, exit) = run(fake_spec(CODEX, "codex_turn.json", &log, None)).await;
    assert!(exit.success && !exit.cancelled, "{exit:?}");
    assert_eq!(terminal(&events), (TurnStatus::Completed, None));
    let (thread, resumed) = native_thread(&events);
    assert!(!thread.is_empty() && !resumed);
    assert!(events.iter().any(|e| matches!(
        e,
        ProviderEvent::ProviderTurn {
            native_turn_id: Some(_),
            status: TurnStatus::Running,
            ..
        }
    )));
    assert!(events
        .iter()
        .any(|e| matches!(e, ProviderEvent::TurnItemDelta { .. })));
    assert!(!final_text(&events).is_empty());
    let command = items(&events, "command_execution");
    let done = command
        .iter()
        .find(|item| item.status == "completed")
        .expect("a finished command");
    assert!(done.fields["output"]
        .as_str()
        .is_some_and(|o| !o.is_empty()));
    assert_eq!(done.fields["exitCode"], 0);
    assert!(items(&events, "file_change")
        .iter()
        .any(|item| item.status == "completed"));
    assert!(events
        .iter()
        .any(|e| matches!(e, ProviderEvent::Usage { usage } if usage.output_tokens > 0)));
    let sent = received(&log);
    let methods: Vec<&str> = sent.iter().filter_map(|m| m["method"].as_str()).collect();
    assert_eq!(
        &methods[..4],
        ["initialize", "initialized", "thread/start", "turn/start"]
    );
    let turn_start = sent.iter().find(|m| m["method"] == "turn/start").unwrap();
    assert_eq!(turn_start["params"]["input"][0]["text"], "Say hello");
    assert_eq!(
        turn_start["params"]["sandboxPolicy"],
        json!({"type":"dangerFullAccess"})
    );
}

#[tokio::test]
async fn codex_turn_resumes_the_native_thread() {
    let dir = tempfile::tempdir().unwrap();
    let log = dir.path().join("in.jsonl");
    let (events, exit) = run(fake_spec(
        CODEX,
        "codex_resume.json",
        &log,
        Some("thr_saved"),
    ))
    .await;
    assert!(exit.success, "{exit:?}");
    assert_eq!(native_thread(&events), ("thr_saved".into(), true));
    let resume = received(&log)
        .into_iter()
        .find(|m| m["method"] == "thread/resume")
        .expect("the turn resumed the thread");
    assert_eq!(resume["params"]["threadId"], "thr_saved");
}

#[tokio::test]
async fn codex_turn_starts_over_when_the_thread_is_gone() {
    let dir = tempfile::tempdir().unwrap();
    let log = dir.path().join("in.jsonl");
    let (events, exit) = run(fake_spec(
        CODEX,
        "codex_resume_missing.json",
        &log,
        Some("thr_gone"),
    ))
    .await;
    assert!(exit.success, "{exit:?}");
    let (thread, resumed) = native_thread(&events);
    assert!(thread != "thr_gone" && !resumed);
}

/// Run `scenario` with `turn_id`, answering its first runtime request with
/// `answer` once it opens; the request and the CLI's log.
async fn answered_turn(
    driver: &str,
    scenario: &str,
    turn_id: &str,
    runtime_mode: &str,
    answer: impl FnOnce(
        &crate::workspace::provider::RuntimeRequestOpen,
    ) -> crate::workspace::provider::RuntimeResponse,
) -> (
    crate::workspace::provider::RuntimeRequestOpen,
    Vec<Value>,
    TurnExit,
) {
    let dir = tempfile::tempdir().unwrap();
    let log = dir.path().join("in.jsonl");
    let mut spec = fake_spec(driver, scenario, &log, None);
    spec.turn_id = turn_id.into();
    spec.provider.as_mut().unwrap().runtime_mode = runtime_mode.into();
    let (_cancel, cancelled) = oneshot::channel();
    let mut events = start(spec, cancelled, TEST_SERVERS.clone()).unwrap();
    let request = loop {
        match events.recv().await.expect("the turn waits for an answer") {
            TurnEvent::Provider(ProviderEvent::RuntimeRequestOpened { request }) => break request,
            TurnEvent::Exited(exit) => panic!("the turn ended without asking: {exit:?}"),
            _ => {}
        }
    };
    // Nothing answers the CLI until the user does.
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    assert!(
        !received(&log).iter().any(|m| m["id"].is_string()
            && m.get("method").is_none()
            && m.get("type").is_none()
            || m["type"] == "control_response" && m["response"]["request_id"] == "perm-1"
            || m["type"] == "extension_ui_response"),
        "the request was answered before the user answered it"
    );
    assert!(respond(turn_id, answer(&request)));
    let (_, exit) = collect(events).await;
    (request, received(&log), exit)
}

#[tokio::test]
async fn codex_turn_waits_for_the_users_approval() {
    let (request, log, exit) = answered_turn(
        CODEX,
        "codex_approval.json",
        "turn-codex-approval",
        "approval-required",
        |request| crate::workspace::provider::RuntimeResponse {
            id: request.id.clone(),
            decision: Some("decline".into()),
            ..Default::default()
        },
    )
    .await;
    assert!(exit.success, "{exit:?}");
    assert_eq!(
        (request.kind.as_str(), request.native_request_id.as_str()),
        ("command", "call_1")
    );
    assert_eq!(request.fields["prompt"], "touch x");
    let answer = log
        .iter()
        .find(|m| m["id"] == "approval-1" && m.get("result").is_some())
        .expect("the approval was answered");
    assert_eq!(answer["result"]["decision"], "decline");
}

/// Full access accepts Codex's command approvals without asking (#40773).
#[tokio::test]
async fn codex_full_access_accepts_commands_itself() {
    let dir = tempfile::tempdir().unwrap();
    let log = dir.path().join("in.jsonl");
    let mut spec = fake_spec(CODEX, "codex_approval.json", &log, None);
    spec.turn_id = "turn-codex-full-access".into();
    spec.provider.as_mut().unwrap().runtime_mode = "full-access".into();
    let (events, exit) = run(spec).await;
    assert!(exit.success, "{exit:?}");
    assert!(!events
        .iter()
        .any(|event| matches!(event, ProviderEvent::RuntimeRequestOpened { .. })));
    let log = received(&log);
    let answer = log
        .iter()
        .find(|m| m["id"] == "approval-1" && m.get("result").is_some())
        .expect("the approval was answered");
    assert_eq!(answer["result"]["decision"], "accept");
    let turn = log.iter().find(|m| m["method"] == "turn/start").unwrap();
    assert_eq!(turn["params"]["sandboxPolicy"]["type"], "dangerFullAccess");
}

#[tokio::test]
async fn codex_turn_asks_the_users_question() {
    let (request, log, exit) = answered_turn(
        CODEX,
        "codex_user_input.json",
        "turn-codex-question",
        "approval-required",
        |request| crate::workspace::provider::RuntimeResponse {
            id: request.id.clone(),
            answers: Some(json!({"color": "Blue"})),
            ..Default::default()
        },
    )
    .await;
    assert!(exit.success, "{exit:?}");
    assert_eq!(request.kind, "user_input");
    let answer = log.iter().find(|m| m["id"] == "input-1").unwrap();
    assert_eq!(
        answer["result"],
        json!({"answers": {"color": {"answers": ["Blue"]}}})
    );
    let turn = log.iter().find(|m| m["method"] == "turn/start").unwrap();
    assert_eq!(turn["params"]["approvalPolicy"], "untrusted");
}

#[tokio::test]
async fn claude_turn_waits_for_the_users_approval() {
    let (request, log, exit) = answered_turn(
        CLAUDE_AGENT,
        "claude_approval.json",
        "turn-claude-approval",
        "full-access",
        |request| crate::workspace::provider::RuntimeResponse {
            id: request.id.clone(),
            decision: Some("acceptForSession".into()),
            ..Default::default()
        },
    )
    .await;
    assert!(exit.success, "{exit:?}");
    assert_eq!(request.fields["fullAccessOverride"], "provider_check");
    let answer = log
        .iter()
        .find(|m| m["type"] == "control_response" && m["response"]["request_id"] == "perm-1")
        .unwrap();
    let result = &answer["response"]["response"];
    assert_eq!(result["behavior"], "allow");
    assert_eq!(result["updatedPermissions"][0]["destination"], "session");
}

#[tokio::test]
async fn codex_turn_cancel_interrupts_the_turn() {
    let dir = tempfile::tempdir().unwrap();
    let log = dir.path().join("in.jsonl");
    let (cancel, cancelled) = oneshot::channel();
    let mut events = start(
        fake_spec(CODEX, "codex_interrupt.json", &log, None),
        cancelled,
        TEST_SERVERS.clone(),
    )
    .unwrap();
    // Cancel once the turn is running.
    let mut seen = vec![];
    while let Some(event) = events.recv().await {
        let running = matches!(
            event,
            TurnEvent::Provider(ProviderEvent::ProviderTurn { .. })
        );
        seen.push(event);
        if running {
            break;
        }
    }
    cancel.send(()).unwrap();
    let (rest, exit) = collect(events).await;
    assert!(exit.cancelled && !exit.success, "{exit:?}");
    assert_eq!(terminal(&rest), (TurnStatus::Interrupted, None));
    assert!(received(&log)
        .iter()
        .any(|m| m["method"] == "turn/interrupt"));
}

#[tokio::test]
async fn codex_turn_reports_a_crash() {
    let dir = tempfile::tempdir().unwrap();
    let log = dir.path().join("in.jsonl");
    let (events, exit) = run(fake_spec(CODEX, "codex_crash.json", &log, None)).await;
    assert!(!exit.success && !exit.cancelled);
    let (status, failure) = terminal(&events);
    assert_eq!(status, TurnStatus::Failed);
    let failure = failure.unwrap();
    assert_eq!(failure.class, "unknown");
    assert_eq!(
        failure.message,
        "The provider event stream closed unexpectedly: the provider process exited with code 3. Check the server logs for the provider's output, then retry the turn."
    );
}

#[tokio::test]
async fn codex_turn_reports_a_failed_turn() {
    let dir = tempfile::tempdir().unwrap();
    let log = dir.path().join("in.jsonl");
    let (events, _) = run(fake_spec(CODEX, "codex_failed.json", &log, None)).await;
    let (status, failure) = terminal(&events);
    assert_eq!(status, TurnStatus::Failed);
    let failure = failure.unwrap();
    assert_eq!(failure.class, "usage_limit");
    assert_eq!(failure.message, "You've hit your usage limit.");
}

#[tokio::test]
async fn missing_clis_fail_with_the_reference_errors() {
    for (driver, message) in [
        (CODEX, "Failed to spawn Codex App Server process for command: /nonexistent/codex app-server"),
        (CLAUDE_AGENT, "Claude Code process exited with code 127 before it started. Check the server logs for the provider's output, then retry the turn."),
    ] {
        let spec = TurnSpec {
            prompt: "hi".into(),
            cwd: "/".into(),
            provider: Some(ProviderTurn {
                driver: driver.into(),
                binary_path: "/nonexistent/codex".into(),
                ..ProviderTurn::default()
            }),
            ..TurnSpec::default()
        };
        let (events, exit) = run(spec).await;
        assert!(!exit.success);
        let (status, failure) = terminal(&events);
        assert_eq!(status, TurnStatus::Failed);
        assert_eq!(failure.unwrap().message, message);
    }
}

#[tokio::test]
async fn a_turn_in_a_missing_folder_says_the_folder_is_missing() {
    let dir = tempfile::tempdir().unwrap();
    let cwd = dir.path().join("gone").to_string_lossy().into_owned();
    for driver in [CODEX, CLAUDE_AGENT] {
        let spec = TurnSpec {
            prompt: "hi".into(),
            cwd: cwd.clone(),
            provider: Some(ProviderTurn {
                driver: driver.into(),
                ..ProviderTurn::default()
            }),
            ..TurnSpec::default()
        };
        let (events, exit) = run(spec).await;
        assert!(!exit.success);
        let (status, failure) = terminal(&events);
        assert_eq!(status, TurnStatus::Failed);
        assert_eq!(
            failure.unwrap().message,
            format!("This thread's workspace folder no longer exists or is not a directory: {cwd}. Restore the folder at this path before retrying.")
        );
    }
}

#[tokio::test]
async fn codex_lists_every_page_of_its_models() {
    let dir = tempfile::tempdir().unwrap();
    let log = dir.path().join("in.jsonl");
    let models = super::list_models(fake_spec(CODEX, "codex_models.json", &log, None))
        .await
        .unwrap();
    let slugs: Vec<_> = models
        .iter()
        .map(|m| m["model"].as_str().unwrap())
        .collect();
    assert_eq!(slugs, ["gpt-6-luna", "gpt-5.6-sol"]);
    let requests = received(&log);
    let pages: Vec<_> = requests
        .iter()
        .filter(|m| m["method"] == "model/list")
        .map(|m| m["params"].clone())
        .collect();
    assert_eq!(pages, [json!({}), json!({"cursor": "page-2"})]);
}

#[tokio::test]
async fn claude_turn_streams_text_tool_calls_and_usage() {
    let dir = tempfile::tempdir().unwrap();
    let log = dir.path().join("in.jsonl");
    let (events, exit) = run(fake_spec(CLAUDE_AGENT, "claude_turn.json", &log, None)).await;
    assert!(exit.success && !exit.cancelled, "{exit:?}");
    assert_eq!(terminal(&events), (TurnStatus::Completed, None));
    let (session, resumed) = native_thread(&events);
    assert!(!session.is_empty() && !resumed);
    assert!(events
        .iter()
        .any(|e| matches!(e, ProviderEvent::TurnItemDelta { .. })));
    assert!(!final_text(&events).is_empty());
    let command = items(&events, "command_execution");
    let done = command
        .iter()
        .find(|item| item.status == "completed")
        .expect("a finished command");
    assert!(done.fields["output"]
        .as_str()
        .is_some_and(|o| !o.is_empty()));
    assert!(items(&events, "file_change")
        .iter()
        .any(|item| item.status == "completed"));
    assert!(events
        .iter()
        .any(|e| matches!(e, ProviderEvent::Usage { usage } if usage.output_tokens > 0)));
    let prompt = received(&log)
        .into_iter()
        .find(|m| m["type"] == "user")
        .unwrap();
    // A plain prompt goes as a string, and counts against the attachment budget.
    assert_eq!(prompt["message"]["content"], "Say hello");
    assert!(events.iter().any(|e| matches!(
        e,
        ProviderEvent::PromptDelivered {
            bytes: 11,
            pages: 0
        }
    )));
    // The last call's usage, with its prompt cache lifetime, and the turn's totals.
    let last = events
        .iter()
        .rev()
        .find_map(|e| match e {
            ProviderEvent::Usage { usage } => Some(usage.clone()),
            _ => None,
        })
        .unwrap();
    assert_eq!(last.prompt_cache_ttl_ms, Some(3_600_000));
    assert!(last.prompt_cache_expires_at.is_some());
    let turn = events
        .iter()
        .find_map(|e| match e {
            ProviderEvent::TurnUsage { usage } => Some(usage.clone()),
            _ => None,
        })
        .expect("the turn's usage");
    assert_eq!(turn["usageStatus"], "complete");
    assert!(events
        .iter()
        .any(|e| matches!(e, ProviderEvent::ProviderTurn { model: Some(_), .. })));
    let args = std::fs::read_to_string(dir.path().join("in.jsonl.args")).unwrap();
    assert!(
        args.contains("--permission-mode=bypassPermissions"),
        "{args}"
    );
    assert!(!args.contains("--resume"), "{args}");
    // The Read tool is titled as the reference titles it.
    assert!(items(&events, "dynamic_tool").iter().any(|item| item
        .title
        .as_deref()
        .is_some_and(|t| t.starts_with("Read /"))));
    // A new session's id is chosen before the CLI starts and named first.
    let (first, _) = native_thread(&events);
    assert!(args.contains(&format!("--session-id={first}")), "{args}");
}

#[tokio::test]
async fn claude_turn_resumes_the_native_session() {
    let dir = tempfile::tempdir().unwrap();
    let log = dir.path().join("in.jsonl");
    let (events, exit) = run(fake_spec(
        CLAUDE_AGENT,
        "claude_resume.json",
        &log,
        Some("sess-saved"),
    ))
    .await;
    assert!(exit.success, "{exit:?}");
    assert_eq!(native_thread(&events), ("sess-saved".into(), true));
    let args = std::fs::read_to_string(dir.path().join("in.jsonl.args")).unwrap();
    assert!(args.contains("--resume=sess-saved"), "{args}");
}

/// The text of the first prompt the fake CLI logged in `log`.
fn first_prompt(log: &Path) -> Value {
    received(log)
        .into_iter()
        .find(|m| m["type"] == "user")
        .map(|m| m["message"]["content"].clone())
        .unwrap_or(Value::Null)
}

#[tokio::test]
async fn claude_turn_fails_when_the_session_is_gone_and_the_next_one_recovers() {
    let dir = tempfile::tempdir().unwrap();
    let session = format!("sess-gone-{}", uuid::Uuid::new_v4());
    // The scenario refuses any `--resume`, then plays a fresh turn.
    let log = dir.path().join("first.jsonl");
    let mut spec = fake_spec(
        CLAUDE_AGENT,
        "claude_resume_missing.json",
        &log,
        Some(&session),
    );
    spec.provider.as_mut().unwrap().recovery_prompt = Some("Recovered\n\nHello".into());
    let (events, exit) = run(spec).await;
    // As the reference's ClaudeConversationNotFoundError, the turn fails.
    assert!(!exit.success, "{exit:?}");
    let (status, failure) = terminal(&events);
    assert_eq!(status, TurnStatus::Failed);
    assert!(failure
        .unwrap()
        .message
        .starts_with("Claude Code could not find this conversation to resume."));
    // The next turn starts a fresh session with the recovered context.
    let log = dir.path().join("second.jsonl");
    let mut spec = fake_spec(
        CLAUDE_AGENT,
        "claude_resume_missing.json",
        &log,
        Some(&session),
    );
    spec.provider.as_mut().unwrap().recovery_prompt = Some("Recovered\n\nHello".into());
    let (events, exit) = run(spec).await;
    assert!(exit.success, "{exit:?}");
    let (fresh, resumed) = native_thread(&events);
    assert!(fresh != session && !resumed);
    let args = std::fs::read_to_string(dir.path().join("second.jsonl.args")).unwrap();
    assert!(!args.contains("--resume"), "{args}");
    assert_eq!(first_prompt(&log), json!("Recovered\n\nHello"));
}

#[tokio::test]
async fn claude_turn_recovers_a_session_whose_transcript_holds_no_conversation() {
    let dir = tempfile::tempdir().unwrap();
    let home = dir.path().join("claude-home");
    let project = home.join("projects").join("-workspace");
    std::fs::create_dir_all(&project).unwrap();
    let empty = format!("sess-empty-{}", uuid::Uuid::new_v4());
    std::fs::write(
        project.join(format!("{empty}.jsonl")),
        "{\"type\":\"summary\",\"summary\":\"Fixture\"}\n",
    )
    .unwrap();
    let saved = format!("sess-saved-{}", uuid::Uuid::new_v4());
    std::fs::write(
        project.join(format!("{saved}.jsonl")),
        "{\"type\":\"summary\"}\n{\"type\":\"user\",\"message\":{}}\n",
    )
    .unwrap();
    let turn = |name: &str, session: &str| {
        let log = dir.path().join(name);
        let mut spec = fake_spec(
            CLAUDE_AGENT,
            "claude_resume_missing.json",
            &log,
            Some(session),
        );
        spec.env
            .push(("CLAUDE_CONFIG_DIR".into(), home.display().to_string()));
        spec.provider.as_mut().unwrap().recovery_prompt = Some("Recovered\n\nHello".into());
        (spec, log)
    };
    let (spec, log) = turn("empty.jsonl", &empty);
    let (events, exit) = run(spec).await;
    assert!(exit.success, "{exit:?}");
    let (fresh, resumed) = native_thread(&events);
    assert!(fresh != empty && !resumed);
    assert_eq!(first_prompt(&log), json!("Recovered\n\nHello"));
    // A transcript with a conversation is resumed (the scenario refuses it).
    let (spec, _) = turn("saved.jsonl", &saved);
    let (_, exit) = run(spec).await;
    assert!(!exit.success, "{exit:?}");
    let args = std::fs::read_to_string(dir.path().join("saved.jsonl.args")).unwrap();
    assert!(args.contains(&format!("--resume={saved}")), "{args}");
}

#[tokio::test]
async fn claude_turn_cancel_interrupts_the_turn() {
    let dir = tempfile::tempdir().unwrap();
    let log = dir.path().join("in.jsonl");
    let (cancel, cancelled) = oneshot::channel();
    let mut events = start(
        fake_spec(CLAUDE_AGENT, "claude_interrupt.json", &log, None),
        cancelled,
        TEST_SERVERS.clone(),
    )
    .unwrap();
    while let Some(event) = events.recv().await {
        if matches!(
            event,
            TurnEvent::Provider(ProviderEvent::ProviderTurn { .. })
        ) {
            break;
        }
    }
    cancel.send(()).unwrap();
    let (rest, exit) = collect(events).await;
    assert!(exit.cancelled && !exit.success, "{exit:?}");
    assert_eq!(terminal(&rest), (TurnStatus::Interrupted, None));
    assert!(received(&log)
        .iter()
        .any(|m| m["type"] == "control_request" && m["request"]["subtype"] == "interrupt"));
}

#[tokio::test]
async fn claude_turn_reports_a_crash() {
    let dir = tempfile::tempdir().unwrap();
    let log = dir.path().join("in.jsonl");
    let (events, exit) = run(fake_spec(CLAUDE_AGENT, "claude_crash.json", &log, None)).await;
    assert!(!exit.success && !exit.cancelled);
    let (status, failure) = terminal(&events);
    assert_eq!(status, TurnStatus::Failed);
    let failure = failure.unwrap();
    assert_eq!(failure.message, "Claude Code process exited with code 1.");
    assert_eq!(failure.code.as_deref(), Some("process_exited_nonzero"));
}

/// Runs a real CLI; set `T3_LIVE_PROVIDER` to `codex` or `claudeAgent` and
/// `T3_LIVE_BINARY` to the program. Never run in CI: it calls a paid model.
#[tokio::test]
#[ignore]
async fn live_provider_turn() {
    let driver = std::env::var("T3_LIVE_PROVIDER").unwrap();
    let cwd = std::env::var("T3_LIVE_CWD").unwrap();
    let prompt = std::env::var("T3_LIVE_PROMPT").unwrap();
    let resume = std::env::var("T3_LIVE_RESUME").ok();
    let spec = TurnSpec {
        prompt,
        cwd,
        provider: Some(ProviderTurn {
            driver,
            binary_path: std::env::var("T3_LIVE_BINARY").unwrap_or_default(),
            resume,
            ..ProviderTurn::default()
        }),
        ..TurnSpec::default()
    };
    let (cancel, cancelled) = oneshot::channel();
    let receiver = start(spec, cancelled, TEST_SERVERS.clone()).unwrap();
    if let Ok(seconds) = std::env::var("T3_LIVE_CANCEL_AFTER") {
        let seconds: u64 = seconds.parse().unwrap();
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_secs(seconds)).await;
            let _ = cancel.send(());
        });
    } else {
        std::mem::forget(cancel);
    }
    let (events, exit) = collect(receiver).await;
    for event in &events {
        println!("{}", serde_json::to_string(event).unwrap());
    }
    println!("{exit:?}");
}

#[tokio::test]
async fn an_instance_runs_its_turns_in_one_app_server_on_its_loaded_thread() {
    let dir = tempfile::tempdir().unwrap();
    let log = dir.path().join("provider.log");
    let mut spec = fake_spec("codex", "codex_two_turns.json", &log, None);
    spec.provider.as_mut().unwrap().instance_id = format!("codex-{}", uuid::Uuid::new_v4());
    let (first, exit) = run(spec.clone()).await;
    assert!(exit.success, "{first:#?}");
    let native = first
        .iter()
        .find_map(|event| match event {
            ProviderEvent::ProviderThread {
                native_thread_id, ..
            } => Some(native_thread_id.clone()),
            _ => None,
        })
        .unwrap();
    // The second turn resumes the thread the shared app-server still has
    // loaded, with the same model and runtime policy: no `thread/resume`.
    spec.provider.as_mut().unwrap().resume = Some(native);
    let (second, exit) = run(spec).await;
    assert!(exit.success, "{second:#?}");
    let launches = std::fs::read_to_string(format!("{}.launch", log.display())).unwrap();
    assert_eq!(launches.lines().count(), 1, "one app-server for both turns");
    let methods: Vec<String> = received(&log)
        .iter()
        .filter_map(|message| message["method"].as_str().map(str::to_owned))
        .collect();
    assert_eq!(methods.iter().filter(|m| *m == "initialize").count(), 1);
    assert_eq!(methods.iter().filter(|m| *m == "turn/start").count(), 2);
    assert!(!methods.iter().any(|m| m == "thread/resume"), "{methods:?}");
}

#[tokio::test]
async fn a_claude_thread_sends_its_next_turn_to_its_running_cli() {
    let dir = tempfile::tempdir().unwrap();
    let log = dir.path().join("provider.log");
    let mut spec = fake_spec(CLAUDE_AGENT, "claude_two_turns.json", &log, None);
    let (first, exit) = run(spec.clone()).await;
    assert!(exit.success, "{first:#?}");
    let session = first
        .iter()
        .rev()
        .find_map(|event| match event {
            ProviderEvent::ProviderThread {
                native_thread_id, ..
            } => Some(native_thread_id.clone()),
            _ => None,
        })
        .unwrap();
    // The next turn resumes the session the thread's CLI still runs: it is
    // written to that CLI, not to a new one.
    spec.provider.as_mut().unwrap().resume = Some(session);
    let (second, exit) = run(spec).await;
    assert!(exit.success, "{second:#?}");
    let launches = std::fs::read_to_string(format!("{}.launch", log.display())).unwrap();
    assert_eq!(launches.lines().count(), 1, "one CLI for both turns");
    let prompts = received(&log)
        .iter()
        .filter(|message| message["type"] == "user")
        .count();
    assert_eq!(prompts, 2);
}

/// A steer the turn completes without taking is reported untaken (#39528);
/// it stays queued in the running CLI, so the turn that sends its message
/// again takes it rather than writing it a second time.
#[tokio::test]
async fn an_untaken_claude_steer_is_reported_and_the_next_turn_adopts_it() {
    let dir = tempfile::tempdir().unwrap();
    let log = dir.path().join("in.jsonl");
    let mut spec = fake_spec(CLAUDE_AGENT, "claude_steer_untaken.json", &log, None);
    spec.turn_id = format!("untaken-{}", uuid::Uuid::new_v4());
    let turn_id = spec.turn_id.clone();
    let (_cancel, cancelled) = oneshot::channel();
    let mut events = start(spec.clone(), cancelled, TEST_SERVERS.clone()).unwrap();
    let mut first = Vec::new();
    loop {
        match events.recv().await.expect("the turn runs") {
            TurnEvent::Provider(event) => {
                let running = matches!(&event, ProviderEvent::TurnItem { item }
                    if item.kind == "command_execution" && item.status == "running");
                first.push(event);
                if running {
                    break;
                }
            }
            TurnEvent::Exited(exit) => panic!("the turn ended first: {exit:?}"),
            _ => {}
        }
    }
    let steer = "Delegated task t1 finished.";
    assert!(respond(
        &turn_id,
        crate::workspace::provider::RuntimeResponse {
            id: "message:delivery".into(),
            steer: Some(steer.into()),
            ..Default::default()
        }
    ));
    let (rest, exit) = collect(events).await;
    assert!(exit.success, "{rest:#?}");
    first.extend(rest);
    let settled: Vec<(&str, bool)> = first
        .iter()
        .filter_map(|event| match event {
            ProviderEvent::SteerUpdated {
                message_id,
                status,
                untaken,
            } if message_id == "message:delivery" => Some((status.as_str(), *untaken)),
            _ => None,
        })
        .collect();
    assert_eq!(settled, [("pending", false), ("completed", true)]);
    let session = first
        .iter()
        .rev()
        .find_map(|event| match event {
            ProviderEvent::ProviderThread {
                native_thread_id, ..
            } => Some(native_thread_id.clone()),
            _ => None,
        })
        .unwrap();
    // The message sent again as its own turn, on the same CLI.
    spec.turn_id = format!("resent-{}", uuid::Uuid::new_v4());
    spec.prompt = steer.into();
    spec.provider.as_mut().unwrap().resume = Some(session);
    let (second, exit) = run(spec).await;
    assert!(exit.success, "{second:#?}");
    let written = received(&log)
        .iter()
        .filter(|message| message["type"] == "user")
        .count();
    assert_eq!(written, 2, "the first prompt and the steer, nothing again");
    assert!(!second
        .iter()
        .any(|event| matches!(event, ProviderEvent::PromptDelivered { .. })));
}

#[tokio::test]
async fn a_steer_goes_to_the_running_claude_turn_and_is_pending_until_claude_takes_it() {
    let dir = tempfile::tempdir().unwrap();
    let log = dir.path().join("in.jsonl");
    let mut spec = fake_spec(CLAUDE_AGENT, "claude_steer.json", &log, None);
    spec.turn_id = format!("steer-{}", uuid::Uuid::new_v4());
    let turn_id = spec.turn_id.clone();
    let (_cancel, cancelled) = oneshot::channel();
    let mut events = start(spec, cancelled, TEST_SERVERS.clone()).unwrap();
    // Once the turn runs a command, the user steers it.
    loop {
        match events.recv().await.expect("the turn runs") {
            TurnEvent::Provider(ProviderEvent::TurnItem { item })
                if item.kind == "command_execution" && item.status == "running" =>
            {
                break
            }
            TurnEvent::Exited(exit) => panic!("the turn ended first: {exit:?}"),
            _ => {}
        }
    }
    assert!(respond(
        &turn_id,
        crate::workspace::provider::RuntimeResponse {
            id: "message:steer".into(),
            steer: Some("Also print the date".into()),
            ..Default::default()
        }
    ));
    let (rest, exit) = collect(events).await;
    assert!(exit.success, "{rest:#?}");
    let statuses: Vec<&str> = rest
        .iter()
        .filter_map(|event| match event {
            ProviderEvent::SteerUpdated {
                message_id, status, ..
            } if message_id == "message:steer" => Some(status.as_str()),
            _ => None,
        })
        .collect();
    assert_eq!(statuses, ["pending", "completed"]);
    // Written as the reference writes a steer: priority now, with the uuid
    // Claude echoed.
    let steer = received(&log)
        .into_iter()
        .find(|message| message["priority"] == "now")
        .expect("the steer reached the CLI");
    assert_eq!(steer["type"], "user");
    assert_eq!(
        steer["message"],
        json!({"role": "user", "content": "Also print the date"})
    );
    assert!(steer["uuid"].as_str().is_some_and(|uuid| uuid.len() == 36));
}

#[tokio::test]
async fn grok_turn_streams_reasoning_a_command_and_the_reply() {
    let dir = tempfile::tempdir().unwrap();
    let log = dir.path().join("in.jsonl");
    let (events, exit) = run(fake_spec(GROK, "grok_turn.json", &log, None)).await;
    assert!(exit.success && !exit.cancelled, "{exit:?}");
    assert_eq!(terminal(&events), (TurnStatus::Completed, None));
    assert_eq!(native_thread(&events), ("grok-session-1".into(), false));
    assert!(events.iter().any(|e| matches!(
        e,
        ProviderEvent::ProviderTurn { native_turn_id: Some(id), .. }
            if id == "provider-instance:grok:grok-session-1:turn:1"
    )));
    let reasoning = items(&events, "reasoning");
    assert_eq!(
        reasoning
            .last()
            .map(|item| (item.status.as_str(), &item.fields["text"])),
        Some(("completed", &json!("I'll run the command.")))
    );
    let command = items(&events, "command_execution");
    let done = command.last().expect("the command");
    assert_eq!(
        (
            done.status.as_str(),
            &done.fields["input"],
            &done.fields["exitCode"]
        ),
        ("completed", &json!("echo hi"), &json!(0))
    );
    assert_eq!(done.fields["nativeItemRef"]["nativeId"], "call-1");
    assert_eq!(final_text(&events), "Ran echo hi; it printed hi.");
    // The turn's usage is unavailable, and goes with its end.
    let ProviderEvent::TurnTerminal { closing, .. } = events.last().unwrap() else {
        unreachable!()
    };
    assert!(closing.iter().any(|e| matches!(
        e,
        ProviderEvent::TurnUsage { usage } if usage["usageStatus"] == "unavailable"
    )));
    let sent = received(&log);
    let methods: Vec<&str> = sent.iter().filter_map(|m| m["method"].as_str()).collect();
    assert_eq!(methods, ["initialize", "session/new", "session/prompt"]);
    assert_eq!(
        sent[0]["params"]["_meta"],
        json!({"clientType": "extension"})
    );
    let prompt = &sent[2]["params"];
    assert_eq!(
        prompt["_meta"],
        json!({"promptId": "t3-xai-prompt-1", "requestId": "t3-xai-prompt-1"})
    );
    let text = prompt["prompt"][0]["text"].as_str().unwrap();
    assert!(text.starts_with("<t3_code_instructions>\n## T3 Code interaction mode: Default"));
    assert!(text.ends_with("<user_request>\nSay hello\n</user_request>"));
    assert!(prompt["prompt"][1]["text"]
        .as_str()
        .unwrap()
        .contains("through the Grok harness, as grok-build."));
}

#[tokio::test]
async fn grok_turn_waits_for_the_users_approval() {
    let (request, log, exit) = answered_turn(
        GROK,
        "grok_approval.json",
        "turn-grok-approval",
        "approval-required",
        |request| crate::workspace::provider::RuntimeResponse {
            id: request.id.clone(),
            decision: Some("accept".into()),
            ..Default::default()
        },
    )
    .await;
    assert!(exit.success, "{exit:?}");
    assert_eq!(
        (request.kind.as_str(), request.native_request_id.as_str()),
        ("command", "call-1")
    );
    assert_eq!(request.fields["prompt"], "echo hi");
    // Bash's "always allow" is not offered; Grok's own options map to T3's.
    assert_eq!(
        request.fields["options"],
        json!([{"decision": "cancel", "label": "Cancel"},
            {"decision": "decline", "label": "Decline"},
            {"decision": "accept", "label": "Approve"}])
    );
    let answer = log
        .iter()
        .find(|m| m["id"] == "perm-1" && m.get("result").is_some())
        .expect("the approval was answered");
    assert_eq!(
        answer["result"],
        json!({"outcome": {"outcome": "selected", "optionId": "allow-once"}})
    );
}

#[tokio::test]
async fn grok_turn_fails_with_groks_own_message() {
    let dir = tempfile::tempdir().unwrap();
    let log = dir.path().join("in.jsonl");
    let (events, exit) = run(fake_spec(GROK, "grok_failed.json", &log, None)).await;
    assert!(!exit.success);
    let (status, failure) = terminal(&events);
    assert_eq!(status, TurnStatus::Failed);
    let failure = failure.unwrap();
    assert_eq!(
        (
            failure.class.as_str(),
            failure.message.as_str(),
            failure.code.as_deref()
        ),
        (
            "provider_error",
            "API error (status 400 Bad Request): invalid_request_error: grok prompt error fixture",
            Some("-32603")
        )
    );
}

#[tokio::test]
async fn grok_stop_ends_the_turn_without_a_cancel() {
    let dir = tempfile::tempdir().unwrap();
    let log = dir.path().join("in.jsonl");
    let (cancel, cancelled) = oneshot::channel();
    let mut events = start(
        fake_spec(GROK, "grok_interrupt.json", &log, None),
        cancelled,
        TEST_SERVERS.clone(),
    )
    .unwrap();
    // Stop once the command runs.
    while let Some(event) = events.recv().await {
        if matches!(&event, TurnEvent::Provider(ProviderEvent::TurnItem { item })
            if item.kind == "command_execution" && item.status == "running")
        {
            break;
        }
    }
    cancel.send(()).unwrap();
    let (rest, exit) = collect(events).await;
    assert!(exit.cancelled && !exit.success, "{exit:?}");
    assert_eq!(terminal(&rest), (TurnStatus::Interrupted, None));
    assert!(items(&rest, "command_execution")
        .iter()
        .any(|item| item.status == "interrupted"));
    // Grok's Stop kills its process rather than cancelling the prompt.
    assert!(!received(&log)
        .iter()
        .any(|m| m["method"] == "session/cancel"));
}

#[tokio::test]
async fn missing_grok_fails_to_start_its_session() {
    let spec = TurnSpec {
        prompt: "hi".into(),
        cwd: "/".into(),
        provider: Some(ProviderTurn {
            driver: GROK.into(),
            binary_path: "/nonexistent/grok".into(),
            ..ProviderTurn::default()
        }),
        ..TurnSpec::default()
    };
    let (events, exit) = run(spec).await;
    assert!(!exit.success);
    assert_eq!(terminal(&events).0, TurnStatus::Failed);
    assert!(!events
        .iter()
        .any(|e| matches!(e, ProviderEvent::ProviderTurn { .. })));
}

/// What a Stop leaves is only for a turn that resumes the stopped session:
/// a later turn on the thread that resumes nothing starts a session of its own.
#[tokio::test]
async fn grok_turn_after_a_stop_loads_only_the_session_it_resumes() {
    let dir = tempfile::tempdir().unwrap();
    let stopped_log = dir.path().join("stopped.jsonl");
    let mut spec = fake_spec(GROK, "grok_interrupt.json", &stopped_log, None);
    spec.provider.as_mut().unwrap().app_thread_id = "thread:stop-then-new".into();
    let (cancel, cancelled) = oneshot::channel();
    let mut events = start(spec, cancelled, TEST_SERVERS.clone()).unwrap();
    while let Some(event) = events.recv().await {
        if matches!(&event, TurnEvent::Provider(ProviderEvent::TurnItem { item })
            if item.kind == "command_execution" && item.status == "running")
        {
            break;
        }
    }
    cancel.send(()).unwrap();
    let (_, exit) = collect(events).await;
    assert!(exit.cancelled, "{exit:?}");
    let log = dir.path().join("next.jsonl");
    let mut spec = fake_spec(GROK, "grok_turn.json", &log, None);
    spec.provider.as_mut().unwrap().app_thread_id = "thread:stop-then-new".into();
    let (events, exit) = run(spec).await;
    assert!(exit.success, "{exit:?}");
    assert_eq!(terminal(&events), (TurnStatus::Completed, None));
    let methods: Vec<String> = received(&log)
        .iter()
        .filter_map(|m| m["method"].as_str().map(str::to_owned))
        .collect();
    assert_eq!(methods, ["initialize", "session/new", "session/prompt"]);
}

#[tokio::test]
async fn acp_registry_turn_runs_in_the_generic_flavor() {
    let dir = tempfile::tempdir().unwrap();
    let log = dir.path().join("in.jsonl");
    let (events, exit) = run(fake_spec(ACP_REGISTRY, "acp_turn.json", &log, None)).await;
    assert!(exit.success, "{exit:?}");
    assert_eq!(terminal(&events), (TurnStatus::Completed, None));
    assert_eq!(native_thread(&events), ("acp-session-1".into(), false));
    assert_eq!(final_text(&events), "Ran echo hi; it printed hi.");
    let sent = received(&log);
    let methods: Vec<&str> = sent.iter().filter_map(|m| m["method"].as_str()).collect();
    assert_eq!(methods, ["initialize", "session/new", "session/prompt"]);
    // No Grok `_meta`: neither the client type nor prompt ids.
    assert!(sent[0]["params"].get("_meta").is_none());
    assert_eq!(
        sent[0]["params"]["capabilities"]["elicitation"],
        json!({"form": {}, "url": {}})
    );
    assert!(sent[2]["params"].get("_meta").is_none());
    assert!(sent[2]["params"]["prompt"][1]["text"]
        .as_str()
        .unwrap()
        .contains("through the acpRegistry harness."));
}

#[tokio::test]
async fn acp_registry_stop_cancels_the_prompt_and_keeps_the_agent() {
    let dir = tempfile::tempdir().unwrap();
    let log = dir.path().join("in.jsonl");
    let (cancel, cancelled) = oneshot::channel();
    let mut events = start(
        fake_spec(ACP_REGISTRY, "acp_interrupt.json", &log, None),
        cancelled,
        TEST_SERVERS.clone(),
    )
    .unwrap();
    while let Some(event) = events.recv().await {
        if matches!(&event, TurnEvent::Provider(ProviderEvent::TurnItem { item })
            if item.kind == "command_execution" && item.status == "running")
        {
            break;
        }
    }
    cancel.send(()).unwrap();
    let (rest, exit) = collect(events).await;
    assert!(exit.cancelled && !exit.success, "{exit:?}");
    assert_eq!(terminal(&rest), (TurnStatus::Interrupted, None));
    // The agent reported the call cancelled; T3 interrupted it.
    assert!(items(&rest, "command_execution")
        .iter()
        .any(|item| item.status == "interrupted"));
    let cancels: Vec<Value> = received(&log)
        .into_iter()
        .filter(|m| m["method"] == "session/cancel")
        .collect();
    assert_eq!(cancels.len(), 1);
    assert_eq!(cancels[0]["params"], json!({"sessionId": "acp-session-1"}));
}

#[tokio::test]
async fn acp_registry_auto_mode_answers_requests_itself() {
    let dir = tempfile::tempdir().unwrap();
    let log = dir.path().join("in.jsonl");
    let mut spec = fake_spec(ACP_REGISTRY, "acp_approval.json", &log, None);
    spec.provider.as_mut().unwrap().runtime_mode = "auto".into();
    let (events, exit) = run(spec).await;
    assert!(exit.success, "{exit:?}");
    assert!(!events
        .iter()
        .any(|e| matches!(e, ProviderEvent::RuntimeRequestOpened { .. })));
    let answer = received(&log)
        .into_iter()
        .find(|m| m["id"] == "perm-1")
        .expect("the request was answered");
    assert_eq!(
        answer["result"],
        json!({"outcome": {"outcome": "selected", "optionId": "allow"}})
    );
}

#[tokio::test]
async fn pi_turn_streams_thinking_a_command_and_the_reply() {
    let dir = tempfile::tempdir().unwrap();
    let log = dir.path().join("in.jsonl");
    let (events, exit) = run(fake_spec(PI, "pi_turn.json", &log, None)).await;
    assert!(exit.success, "{exit:?}");
    assert_eq!(terminal(&events), (TurnStatus::Completed, None));
    assert_eq!(
        native_thread(&events),
        ("/pi-sessions/session-1.jsonl".into(), false)
    );
    assert_eq!(
        items(&events, "reasoning")
            .last()
            .map(|item| &item.fields["text"]),
        Some(&json!("I'll run the command."))
    );
    let command = items(&events, "command_execution");
    let done = command.last().expect("the command");
    assert_eq!(
        (
            done.status.as_str(),
            &done.fields["input"],
            &done.fields["exitCode"]
        ),
        ("completed", &json!("echo hi"), &json!(0))
    );
    assert!(events.iter().any(|e| matches!(
        e,
        ProviderEvent::CommandOutputDelta { chunk, .. } if chunk == "hi\n"
    )));
    assert_eq!(final_text(&events), "Ran echo hi; it printed hi.");
    // The session's statistics give the turn's usage.
    assert!(events.iter().any(|e| matches!(
        e,
        ProviderEvent::Usage { usage } if usage.used_tokens == 1814 && usage.max_tokens == Some(400_000)
    )));
    let sent: Vec<String> = received(&log)
        .iter()
        .filter_map(|m| m["type"].as_str().map(str::to_owned))
        .collect();
    assert_eq!(
        sent,
        [
            "get_state",
            "get_available_models",
            "get_entries",
            "prompt",
            "get_state",
            "get_entries",
            "get_session_stats"
        ]
    );
    let prompt = received(&log)
        .into_iter()
        .find(|m| m["type"] == "prompt")
        .unwrap();
    assert_eq!(
        (
            &prompt["message"],
            &prompt["streamingBehavior"],
            &prompt["id"]
        ),
        (&json!("Say hello"), &json!("steer"), &json!("t3-prompt-0"))
    );
}

#[tokio::test]
async fn pi_turn_fails_with_the_models_error() {
    let dir = tempfile::tempdir().unwrap();
    let log = dir.path().join("in.jsonl");
    let (events, exit) = run(fake_spec(PI, "pi_failed.json", &log, None)).await;
    assert!(!exit.success);
    let (status, failure) = terminal(&events);
    assert_eq!(status, TurnStatus::Failed);
    assert_eq!(
        failure.unwrap().message,
        "429 Too Many Requests: rate limited by the fixture"
    );
}

#[tokio::test]
async fn pi_stop_aborts_and_stops_the_process() {
    let dir = tempfile::tempdir().unwrap();
    let log = dir.path().join("in.jsonl");
    let (cancel, cancelled) = oneshot::channel();
    let mut events = start(
        fake_spec(PI, "pi_interrupt.json", &log, None),
        cancelled,
        TEST_SERVERS.clone(),
    )
    .unwrap();
    while let Some(event) = events.recv().await {
        if matches!(&event, TurnEvent::Provider(ProviderEvent::TurnItem { item })
            if item.kind == "command_execution" && item.status == "running")
        {
            break;
        }
    }
    cancel.send(()).unwrap();
    let (rest, exit) = collect(events).await;
    assert!(exit.cancelled && !exit.success, "{exit:?}");
    assert_eq!(terminal(&rest), (TurnStatus::Interrupted, None));
    let sent: Vec<String> = received(&log)
        .iter()
        .filter_map(|m| m["type"].as_str().map(str::to_owned))
        .collect();
    let at = sent
        .iter()
        .position(|kind| kind == "clear_queue")
        .expect("the queue was cleared");
    assert_eq!(
        &sent[at..at + 4],
        ["clear_queue", "abort", "get_state", "get_entries"]
    );
    assert!(!sent.contains(&"get_session_stats".to_owned()));
}

#[tokio::test]
async fn pi_turn_asks_the_user_before_a_command() {
    let (request, log, exit) = answered_turn(
        PI,
        "pi_approval.json",
        "turn-pi-approval",
        "approval-required",
        |request| crate::workspace::provider::RuntimeResponse {
            id: request.id.clone(),
            decision: Some("decline".into()),
            ..Default::default()
        },
    )
    .await;
    assert!(exit.success, "{exit:?}");
    assert_eq!(
        (request.kind.as_str(), request.native_request_id.as_str()),
        ("command", "ui-1")
    );
    assert_eq!(request.fields["title"], "Allow bash?");
    let answer = log
        .iter()
        .find(|m| m["type"] == "extension_ui_response")
        .expect("the dialog was answered");
    assert_eq!(
        answer,
        &json!({"type": "extension_ui_response", "id": "ui-1", "confirmed": false})
    );
}

/// An OpenCode turn on the fake `opencode serve` playing `scenario`; its
/// requests are logged to `<log>.http`.
fn opencode_spec(dir: &Path, scenario: &str, log: &Path) -> TurnSpec {
    let shim = dir.join("opencode");
    std::fs::write(
        &shim,
        format!(
            "#!/bin/sh\nexec python3 {} {} \"$@\"\n",
            fixtures().join("fake_opencode.py").display(),
            fixtures().join(scenario).display()
        ),
    )
    .unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&shim, std::fs::Permissions::from_mode(0o755)).unwrap();
    }
    let mut spec = fake_spec(OPENCODE, scenario, log, None);
    if let Some(provider) = spec.provider.as_mut() {
        provider.binary_path = shim.display().to_string();
        provider.model = "anthropic/claude-opus-5-5".into();
        provider.native_turn = "attempt:attempt-1".into();
    }
    spec
}

fn opencode_requests(log: &Path) -> Vec<Value> {
    std::fs::read_to_string(format!("{}.http", log.display()))
        .unwrap_or_default()
        .lines()
        .filter(|line| line.starts_with('{'))
        .filter_map(|line| serde_json::from_str(line).ok())
        .collect()
}

#[tokio::test]
async fn opencode_turn_streams_reasoning_tools_and_the_reply() {
    let dir = tempfile::tempdir().unwrap();
    let log = dir.path().join("oc.log");
    let (events, exit) = run(opencode_spec(dir.path(), "opencode_turn.json", &log)).await;
    assert!(exit.success, "{exit:?}");
    assert_eq!(terminal(&events), (TurnStatus::Completed, None));
    assert_eq!(native_thread(&events), ("ses_1".into(), false));
    let command = items(&events, "command_execution");
    let done = command.last().expect("the command");
    assert_eq!(
        (
            done.status.as_str(),
            &done.fields["input"],
            &done.fields["exitCode"]
        ),
        ("completed", &json!("echo hi"), &json!(0))
    );
    let edit = items(&events, "file_change");
    assert_eq!(edit.last().unwrap().fields["fileName"], "/w/notes.txt");
    assert_eq!(final_text(&events), "Ran echo hi and wrote notes.txt.");
    // The last step's tokens, with the catalog's input limit as the window.
    assert!(events.iter().any(|e| matches!(
        e,
        ProviderEvent::Usage { usage } if usage.used_tokens == 4380 && usage.max_tokens == Some(180_000)
    )));
    // The turn is named by its session and attempt, then by its user message.
    assert!(events.iter().any(|e| matches!(
        e,
        ProviderEvent::ProviderTurn { native_turn_id: Some(id), .. } if id == "ses_1:attempt:attempt-1"
    )));
    let requests = opencode_requests(&log);
    let session = requests
        .iter()
        .find(|r| r["method"] == "POST" && r["path"] == "/session")
        .expect("a session was created");
    assert_eq!(session["body"]["permission"][0]["action"], "allow");
    let prompt = requests
        .iter()
        .find(|r| r["path"] == "/session/ses_1/prompt_async")
        .expect("the prompt");
    assert!(prompt["body"]["messageID"]
        .as_str()
        .is_some_and(|id| id.starts_with("msg_") && id.len() == 44));
    assert_eq!(
        prompt["body"]["model"],
        json!({"providerID": "anthropic", "modelID": "claude-opus-5-5"})
    );
    assert_eq!(
        prompt["body"]["parts"],
        json!([{"type": "text", "text": "Say hello"}])
    );
    assert!(prompt["body"]["system"]
        .as_str()
        .is_some_and(|s| s.contains("through the OpenCode harness, as anthropic/claude-opus-5-5")));
}

#[tokio::test]
async fn opencode_turn_fails_with_the_providers_error() {
    let dir = tempfile::tempdir().unwrap();
    let log = dir.path().join("oc.log");
    let (events, exit) = run(opencode_spec(dir.path(), "opencode_failed.json", &log)).await;
    assert!(!exit.success);
    let (status, failure) = terminal(&events);
    let failure = failure.expect("a failure");
    assert_eq!(status, TurnStatus::Failed);
    assert_eq!(
        (failure.class.as_str(), failure.message.as_str()),
        ("usage_limit", "Rate limit reached for this model.")
    );
}

#[tokio::test]
async fn opencode_stop_aborts_the_session_and_waits_for_idle() {
    let dir = tempfile::tempdir().unwrap();
    let log = dir.path().join("oc.log");
    let (cancel, cancelled) = oneshot::channel();
    let mut events = start(
        opencode_spec(dir.path(), "opencode_interrupt.json", &log),
        cancelled,
        TEST_SERVERS.clone(),
    )
    .unwrap();
    while let Some(event) = events.recv().await {
        if matches!(&event, TurnEvent::Provider(ProviderEvent::TurnItem { item })
            if item.kind == "command_execution" && item.status == "running")
        {
            break;
        }
    }
    cancel.send(()).unwrap();
    let (rest, exit) = collect(events).await;
    assert!(exit.cancelled && !exit.success, "{exit:?}");
    assert_eq!(terminal(&rest), (TurnStatus::Interrupted, None));
    // The aborted tool ends interrupted, as OpenCode reports it.
    assert_eq!(
        items(&rest, "command_execution")
            .last()
            .map(|i| i.status.as_str()),
        Some("interrupted")
    );
    assert!(opencode_requests(&log)
        .iter()
        .any(|r| r["method"] == "POST" && r["path"] == "/session/ses_1/abort"));
}

#[tokio::test]
async fn antigravity_turn_signs_in_eagerly_and_reads_agys_command_shapes() {
    let dir = tempfile::tempdir().unwrap();
    let log = dir.path().join("in.jsonl");
    let mut spec = fake_spec(ANTIGRAVITY, "agy_turn.json", &log, None);
    if let Some(provider) = spec.provider.as_mut() {
        provider.launch_args = vec!["acp".into()];
        provider.antigravity = Some(crate::workspace::provider::AntigravityLaunch {
            version: "1.6.2".into(),
            run_temp_parent: dir.path().join("tmp").display().to_string(),
            launch_error: String::new(),
        });
    }
    let (events, exit) = run(spec).await;
    assert!(exit.success, "{exit:?}");
    assert_eq!(terminal(&events), (TurnStatus::Completed, None));
    let sent: Vec<Value> = received(&log);
    let methods: Vec<&str> = sent.iter().filter_map(|m| m["method"].as_str()).collect();
    // Signed in right after initialize, before any session request.
    assert_eq!(&methods[..3], ["initialize", "authenticate", "session/new"]);
    let initialize = &sent[0]["params"];
    assert_eq!(initialize["clientCapabilities"]["fs"]["readTextFile"], true);
    assert_eq!(initialize["clientInfo"]["name"], "t3-code");
    // agy's command line and combined output.
    let command = items(&events, "command_execution");
    let done = command.last().expect("the command");
    assert_eq!(
        (
            done.status.as_str(),
            &done.fields["input"],
            &done.fields["output"]
        ),
        ("completed", &json!("echo hi"), &json!("hi\n"))
    );
}

#[tokio::test]
async fn antigravity_launch_error_fails_the_session_open() {
    let dir = tempfile::tempdir().unwrap();
    let log = dir.path().join("in.jsonl");
    let mut spec = fake_spec(ANTIGRAVITY, "agy_turn.json", &log, None);
    if let Some(provider) = spec.provider.as_mut() {
        provider.antigravity = Some(crate::workspace::provider::AntigravityLaunch {
            launch_error: "Antigravity ACP runtime is not installed.".into(),
            ..Default::default()
        });
    }
    let (events, exit) = run(spec).await;
    assert!(!exit.success);
    let (status, failure) = terminal(&events);
    assert_eq!(status, TurnStatus::Failed);
    assert_eq!(
        failure.map(|f| f.message),
        Some("Antigravity ACP runtime is not installed.".into())
    );
    assert!(received(&log).is_empty(), "nothing was launched");
}
