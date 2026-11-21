//! Provider turns against fake CLIs that replay recorded Codex app-server and
//! Claude Code stream-json transcripts (`tests/fixtures/providers`).
use super::*;
use crate::workspace::provider::{ProviderTurn, TurnStatus};
use serde_json::{json, Value};
use std::path::{Path, PathBuf};

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
    collect(start(spec, cancelled).unwrap()).await
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
    let mut events = start(spec, cancelled).unwrap();
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
        !received(&log)
            .iter()
            .any(|m| m["id"].is_string() && m.get("method").is_none()
                || m["type"] == "control_response" && m["response"]["request_id"] == "perm-1"),
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
        "full-access",
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
        "The provider event stream closed unexpectedly. Retry the turn; if it keeps failing, check the provider and server logs."
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
        (CLAUDE_AGENT, "The provider could not start this turn. Retry the turn; if it keeps failing, check the provider setup and server logs."),
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

#[tokio::test]
async fn claude_turn_starts_over_when_the_session_is_gone() {
    let dir = tempfile::tempdir().unwrap();
    let log = dir.path().join("in.jsonl");
    // The scenario refuses any `--resume`, then plays a fresh turn.
    let (events, exit) = run(fake_spec(
        CLAUDE_AGENT,
        "claude_resume_missing.json",
        &log,
        Some("sess-gone"),
    ))
    .await;
    assert!(exit.success, "{exit:?}");
    let (session, resumed) = native_thread(&events);
    assert!(session != "sess-gone" && !resumed);
}

#[tokio::test]
async fn claude_turn_cancel_interrupts_the_turn() {
    let dir = tempfile::tempdir().unwrap();
    let log = dir.path().join("in.jsonl");
    let (cancel, cancelled) = oneshot::channel();
    let mut events = start(
        fake_spec(CLAUDE_AGENT, "claude_interrupt.json", &log, None),
        cancelled,
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
    let receiver = start(spec, cancelled).unwrap();
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
