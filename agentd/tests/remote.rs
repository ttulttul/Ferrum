//! Run workspace operations through `RemoteWorkspace` → `t3-agentd` →
//! `LocalWorkspace` and check they match calling `LocalWorkspace` directly.
use serde_json::json;
use std::{path::Path, process::Command, sync::Arc, time::Duration};
use t3_agentd::{Agentd, Config};
use t3code_server::workspace::provider::TurnStatus;
use t3code_server::workspace::{
    connect, GitTools, LocalWorkspace, NativeLink, ProviderEvent, ProviderTurn, RemoteWorkspace,
    TurnEvent, TurnItem, TurnItems, TurnSpec, Workspace,
};
use tokio::sync::oneshot;

const TOKEN: &str = "remote-test-token";

struct Setup {
    local: Arc<LocalWorkspace>,
    remote: RemoteWorkspace,
    link: NativeLink,
    url: String,
    _dir: tempfile::TempDir,
    root: std::path::PathBuf,
}

mod support;

async fn setup() -> Setup {
    support::isolate_host();
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("project");
    std::fs::create_dir_all(&root).unwrap();
    let local = Arc::new(LocalWorkspace::open(&dir.path().join("state")).unwrap());
    let agentd = Agentd::new(
        local.clone(),
        Config {
            token: TOKEN.into(),
            root: root.clone(),
            extra_roots: vec![dir.path().join("state/worktrees")],
            buffer_dir: dir.path().join("streams"),
            buffer_limit: 1 << 20,
            poll_interval: Duration::from_millis(50),
            prepare_dir: None,
        },
    )
    .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, agentd.router()).await });
    let remote = RemoteWorkspace::new(1, None);
    let url = format!("ws://{addr}/");
    let link = connect(remote.clone(), url.clone(), TOKEN.into());
    wait_until(|| remote.is_ready()).await;
    Setup {
        local,
        remote,
        link,
        url,
        _dir: dir,
        root,
    }
}

async fn wait_until(mut ready: impl FnMut() -> bool) {
    for _ in 0..400 {
        if ready() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("timed out waiting");
}

fn git(dir: &Path, args: &[&str]) {
    let status = Command::new("git")
        .args(["-c", "user.name=Test", "-c", "user.email=test@example.com"])
        .args(args)
        .current_dir(dir)
        .status()
        .unwrap();
    assert!(status.success(), "git {args:?}");
}

async fn run_turn(workspace: &dyn Workspace, spec: TurnSpec) -> Vec<TurnEvent> {
    let (_cancel, cancelled) = oneshot::channel();
    let mut events = workspace.start_turn(spec, cancelled).await.unwrap();
    let mut seen = Vec::new();
    while let Some(event) = tokio::time::timeout(Duration::from_secs(20), events.recv())
        .await
        .expect("the turn finished in time")
    {
        let exited = matches!(event, TurnEvent::Exited(_));
        seen.push(event);
        if exited {
            break;
        }
    }
    seen
}

#[tokio::test]
async fn file_operations_match_the_local_workspace() {
    let s = setup().await;
    let cwd = s.root.to_string_lossy().into_owned();
    let big = "0123456789abcdef\n".repeat(4096);
    let write = json!({"cwd": cwd, "relativePath": "src/big.txt", "contents": big});
    // Writing is idempotent, so both sides can write the same file.
    assert_eq!(
        s.remote.write_file(&write).await,
        s.local.write_file(&write).await
    );
    let small = json!({"cwd": cwd, "relativePath": "README.md", "contents": "# hi\n"});
    assert!(matches!(s.remote.write_file(&small).await, Some(Ok(_))));
    for input in [
        json!({"cwd": cwd, "relativePath": "src/big.txt"}),
        json!({"cwd": cwd, "relativePath": "missing.txt"}),
    ] {
        assert_eq!(
            s.remote.read_file(&input).await,
            s.local.read_file(&input).await
        );
    }
    // agentd refuses paths outside its root before the workspace sees them.
    let escape = json!({"cwd": cwd, "relativePath": "../escape"});
    let refused = s.remote.read_file(&escape).await.unwrap().unwrap_err();
    assert!(refused.to_string().contains("outside"), "{refused}");
    let list = json!({"cwd": cwd});
    assert_eq!(
        s.remote.list_entries(&list).await,
        s.local.list_entries(&list).await
    );
    let search = json!({"cwd": cwd, "query": "big", "limit": 10});
    assert_eq!(
        s.remote.search_entries(&search).await,
        s.local.search_entries(&search).await
    );
    let meta = json!({"path": s.root.join("README.md")});
    assert_eq!(
        s.remote.file_metadata(&meta).await,
        s.local.file_metadata(&meta).await
    );
    let browse = json!({"partialPath": format!("{cwd}/")});
    assert_eq!(
        s.remote.browse(&browse).await,
        s.local.browse(&browse).await
    );

    let nested = s.root.join("a/b/c");
    s.remote.create_dir_all(&nested).await.unwrap();
    assert!(s.remote.is_dir(&nested).await);
    assert_eq!(
        s.remote.canonicalize(&nested).await.unwrap(),
        s.local.canonicalize(&nested).await.unwrap()
    );
    let missing = s.root.join("nope");
    assert_eq!(
        s.remote.canonicalize(&missing).await.unwrap_err().kind(),
        s.local.canonicalize(&missing).await.unwrap_err().kind()
    );
    assert!(!s.remote.is_dir(&missing).await);
    drop(s.link);
}

#[tokio::test]
async fn vcs_status_matches_the_local_workspace() {
    let s = setup().await;
    git(&s.root, &["init", "-q", "-b", "main"]);
    std::fs::write(s.root.join("a.txt"), "a\n").unwrap();
    git(&s.root, &["add", "."]);
    git(&s.root, &["commit", "-q", "-m", "first"]);
    std::fs::write(s.root.join("b.txt"), "b\n").unwrap();
    let input = json!({"cwd": s.root});
    let local = s.local.vcs("refreshStatus", &input, false).await;
    assert!(matches!(local, Some(Ok(_))));
    assert_eq!(s.remote.vcs("refreshStatus", &input, false).await, local);
    // The polled reading comes from agentd's watch.
    assert_eq!(s.remote.vcs_status(&input, false, false).await, local);
    assert_eq!(
        s.remote.vcs_branch(&input).await,
        s.local.vcs_branch(&input).await
    );
    assert_eq!(
        s.remote.vcs("noSuchMethod", &input, false).await,
        s.local.vcs("noSuchMethod", &input, false).await
    );
    // A change shows up in the watch without another request to agentd.
    std::fs::write(s.root.join("c.txt"), "c\n").unwrap();
    let changed = s.local.vcs("refreshStatus", &input, false).await;
    for _ in 0..200 {
        if s.remote.vcs_status(&input, false, false).await == changed {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert_eq!(s.remote.vcs_status(&input, false, false).await, changed);
    assert_eq!(
        s.remote.capture_git_tree(&s.root.to_string_lossy()).await,
        s.local.capture_git_tree(&s.root.to_string_lossy()).await
    );
}

#[tokio::test]
async fn git_workflows_answer_like_the_local_workspace() {
    let s = setup().await;
    let tools = GitTools {
        gh: "gh-not-installed".into(),
        text_generation: vec![],
    };
    let input = json!({"cwd": s.root, "reference": "not-a-pull-request"});
    assert_eq!(
        s.remote
            .pull_request("resolvePullRequest", &input, &tools)
            .await,
        s.local
            .pull_request("resolvePullRequest", &input, &tools)
            .await
    );
    let action = json!({"cwd": s.root, "action": "commit"});
    let (remote_tx, mut remote_rx) = tokio::sync::mpsc::channel(16);
    let (local_tx, mut local_rx) = tokio::sync::mpsc::channel(16);
    let remote = s.remote.stacked_action(&action, &tools, remote_tx).await;
    let local = s.local.stacked_action(&action, &tools, local_tx).await;
    assert_eq!(remote, local);
    let (mut remote_progress, mut local_progress) = (Vec::new(), Vec::new());
    while let Some(batch) = remote_rx.recv().await {
        remote_progress.push(batch);
    }
    while let Some(batch) = local_rx.recv().await {
        local_progress.push(batch);
    }
    assert_eq!(remote_progress.len(), local_progress.len());
}

#[tokio::test]
async fn a_terminal_runs_through_agentd() {
    let s = setup().await;
    support::enable_test_terminal();
    let mut output = s.remote.terminal_events();
    let open =
        json!({"threadId": "t1", "terminalId": "term-1", "cwd": s.root, "cols": 80, "rows": 24});
    let opened = s.remote.terminal("open", &open).await.unwrap().unwrap();
    assert_eq!(opened["status"], "running");
    // The metadata mirror catches up with the workspace's own.
    for _ in 0..200 {
        if s.remote.terminal_metadata() == s.local.terminal_metadata() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert_eq!(s.remote.terminal_metadata(), s.local.terminal_metadata());
    let write =
        json!({"threadId": "t1", "terminalId": "term-1", "data": "echo agentd-$((40+2))\r"});
    assert!(matches!(
        s.remote.terminal("write", &write).await,
        Some(Ok(_))
    ));
    let mut seen = String::new();
    tokio::time::timeout(Duration::from_secs(20), async {
        while !seen.contains("agentd-42") {
            let event = output.recv().await.unwrap();
            seen.push_str(&event.to_string());
        }
    })
    .await
    .expect("the terminal echoed");
    let close = json!({"threadId": "t1", "terminalId": "term-1"});
    assert!(matches!(
        s.remote.terminal("close", &close).await,
        Some(Ok(_))
    ));
    assert_eq!(
        s.remote.terminal("nope", &close).await,
        s.local.terminal("nope", &close).await
    );
}

#[tokio::test]
async fn a_turn_matches_the_local_workspace() {
    let s = setup().await;
    let spec = TurnSpec {
        command: vec![
            "sh".into(),
            "-c".into(),
            "echo out $1; echo err $1 >&2; echo done; exit 3".into(),
            "provider".into(),
        ],
        prompt: "prompt".into(),
        cwd: s.root.to_string_lossy().into_owned(),
        env_remove: vec!["HOME".into()],
        ..TurnSpec::default()
    };
    let remote = run_turn(&s.remote, spec.clone()).await;
    let local = run_turn(&*s.local, spec).await;
    let sort = |events: &[TurnEvent]| {
        // stdout and stderr interleave freely; compare each in order.
        let mut out: Vec<_> = events
            .iter()
            .filter(|e| matches!(e, TurnEvent::Stdout(_)))
            .cloned()
            .collect();
        out.extend(
            events
                .iter()
                .filter(|e| matches!(e, TurnEvent::Stderr(_)))
                .cloned(),
        );
        out.push(events.last().unwrap().clone());
        out
    };
    assert_eq!(sort(&remote), sort(&local));
    assert_eq!(remote.len(), 4);
    assert!(matches!(
        remote.last(),
        Some(TurnEvent::Exited(exit)) if !exit.success && !exit.cancelled && exit.status == "exit status: 3"
    ));

    // Cancelling stops the process, as it does in-process.
    let (cancel, cancelled) = oneshot::channel();
    let mut events = s
        .remote
        .start_turn(
            TurnSpec {
                command: vec![
                    "sh".into(),
                    "-c".into(),
                    "echo started; exec sleep 30".into(),
                ],
                prompt: String::new(),
                cwd: s.root.to_string_lossy().into_owned(),
                env_remove: vec![],
                ..TurnSpec::default()
            },
            cancelled,
        )
        .await
        .unwrap();
    assert_eq!(
        events.recv().await,
        Some(TurnEvent::Stdout("started\n".into()))
    );
    cancel.send(()).unwrap();
    let exit = tokio::time::timeout(Duration::from_secs(20), events.recv())
        .await
        .unwrap();
    assert!(matches!(exit, Some(TurnEvent::Exited(exit)) if exit.cancelled));
}

#[tokio::test]
async fn a_turn_survives_a_dropped_socket() {
    let s = setup().await;
    let (_cancel, cancelled) = oneshot::channel();
    let mut events = s
        .remote
        .start_turn(
            TurnSpec {
                command: vec![
                    "sh".into(),
                    "-c".into(),
                    "for i in $(seq 1 60); do echo line $i; sleep 0.01; done".into(),
                ],
                prompt: String::new(),
                cwd: s.root.to_string_lossy().into_owned(),
                env_remove: vec![],
                ..TurnSpec::default()
            },
            cancelled,
        )
        .await
        .unwrap();
    let mut lines = Vec::new();
    let mut reconnects = 0;
    let exit = loop {
        let event = tokio::time::timeout(Duration::from_secs(20), events.recv())
            .await
            .expect("the turn kept reporting")
            .expect("the turn's events kept coming");
        match event {
            TurnEvent::Stdout(line) => lines.push(line),
            TurnEvent::Stderr(line) => panic!("unexpected stderr {line}"),
            TurnEvent::Provider(event) => panic!("unexpected provider event {event:?}"),
            TurnEvent::Exited(exit) => break exit,
        }
        // Drop the socket twice mid-turn; the turn keeps running in agentd.
        if matches!(lines.len(), 5 | 25) && reconnects < 2 {
            reconnects += 1;
            s.link.reconnect();
            tokio::time::sleep(Duration::from_millis(150)).await;
        }
    };
    assert_eq!(reconnects, 2);
    assert!(exit.success, "{exit:?}");
    assert_eq!(
        lines,
        (1..=60).map(|i| format!("line {i}\n")).collect::<Vec<_>>()
    );
    // Everything was acknowledged once the connection settled.
    wait_until(|| s.remote.is_ready()).await;
    let mut idle = s.remote.idle_state().await.unwrap();
    for _ in 0..200 {
        if idle.unacked_frames == 0 && idle.running_turns == 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
        idle = s.remote.idle_state().await.unwrap();
    }
    assert_eq!(idle.running_turns, 0);
    assert_eq!(idle.unacked_frames, 0);
    assert!(idle.connected);
}

async fn next_item(items: &mut TurnItems) -> TurnItem {
    tokio::time::timeout(Duration::from_secs(20), items.recv())
        .await
        .expect("the turn kept reporting")
        .expect("the turn's events kept coming")
}

#[tokio::test]
async fn a_restarted_consumer_gets_unacknowledged_events_again_once() {
    let s = setup().await;
    let (_cancel, cancelled) = oneshot::channel();
    let mut items = s
        .remote
        .start_tracked_turn(
            TurnSpec {
                command: vec![
                    "sh".into(),
                    "-c".into(),
                    "for i in $(seq 1 20); do echo line $i; sleep 0.02; done".into(),
                ],
                prompt: String::new(),
                cwd: s.root.to_string_lossy().into_owned(),
                env_remove: vec![],
                ..TurnSpec::default()
            },
            cancelled,
        )
        .await
        .unwrap();
    // The first consumer records and acknowledges four lines, receives the
    // fifth, and stops before recording it.
    let mut recorded = Vec::new();
    let mut stored = None;
    let unrecorded = loop {
        let item = next_item(&mut items).await;
        let position = item.position.expect("tracked events carry a position");
        let TurnEvent::Stdout(line) = item.event else {
            panic!("unexpected {:?}", item.event);
        };
        if recorded.len() == 4 {
            break (line, position);
        }
        recorded.push(line);
        s.remote.ack(position);
        stored = Some(position);
    };
    let stored = stored.unwrap();
    assert_eq!(unrecorded.1.seq, stored.seq + 1);
    assert_eq!(s.remote.acked_positions(), vec![stored]);
    drop(items);
    drop(s.link);
    drop(s.remote);

    // A new consumer, as after a server restart, resumes from what it stored.
    let remote = RemoteWorkspace::new(2, None);
    let (_cancel, cancelled) = oneshot::channel();
    let mut items = remote.resume_turn(stored, cancelled).await;
    let _link = connect(remote.clone(), s.url.clone(), TOKEN.into());
    let first = next_item(&mut items).await;
    assert_eq!(first.event, TurnEvent::Stdout(unrecorded.0.clone()));
    assert_eq!(first.position, Some(unrecorded.1));
    recorded.push(unrecorded.0);
    remote.ack(unrecorded.1);
    let exit = loop {
        let item = next_item(&mut items).await;
        let position = item.position.expect("tracked events carry a position");
        match item.event {
            TurnEvent::Stdout(line) => recorded.push(line),
            TurnEvent::Stderr(line) => panic!("unexpected stderr {line}"),
            TurnEvent::Provider(event) => panic!("unexpected provider event {event:?}"),
            TurnEvent::Exited(exit) => {
                remote.ack(position);
                break exit;
            }
        }
        remote.ack(position);
    };
    assert!(exit.success, "{exit:?}");
    // Every line recorded exactly once, in order.
    assert_eq!(
        recorded,
        (1..=20).map(|i| format!("line {i}\n")).collect::<Vec<_>>()
    );
    // agentd let go of the stream once its end was acknowledged too.
    let mut idle = remote.idle_state().await.unwrap();
    for _ in 0..200 {
        if idle.unacked_frames == 0 && idle.open_streams <= 4 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
        idle = remote.idle_state().await.unwrap();
    }
    assert_eq!(idle.unacked_frames, 0);
    assert_eq!(idle.running_turns, 0);
    assert!(remote.acked_positions().is_empty());
}

#[tokio::test]
async fn a_tracked_turn_holds_frames_until_they_are_acknowledged() {
    let s = setup().await;
    let (_cancel, cancelled) = oneshot::channel();
    let mut items = s
        .remote
        .start_tracked_turn(
            TurnSpec {
                command: vec!["sh".into(), "-c".into(), "echo one; echo two".into()],
                prompt: String::new(),
                cwd: s.root.to_string_lossy().into_owned(),
                env_remove: vec![],
                ..TurnSpec::default()
            },
            cancelled,
        )
        .await
        .unwrap();
    let mut positions = Vec::new();
    loop {
        let item = next_item(&mut items).await;
        positions.push(item.position.unwrap());
        if matches!(item.event, TurnEvent::Exited(_)) {
            break;
        }
    }
    // Received but not acknowledged: agentd still holds them.
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(s.remote.idle_state().await.unwrap().unacked_frames > 0);
    s.remote.ack(*positions.last().unwrap());
    let mut idle = s.remote.idle_state().await.unwrap();
    for _ in 0..200 {
        if idle.unacked_frames == 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
        idle = s.remote.idle_state().await.unwrap();
    }
    assert_eq!(idle.unacked_frames, 0);
}

/// A Codex turn played by the fake CLI from `tests/fixtures/providers`.
fn provider_spec(root: &Path, scenario: &str, log: &Path) -> TurnSpec {
    let fixtures = Path::new(env!("CARGO_MANIFEST_DIR")).join("../tests/fixtures/providers");
    TurnSpec {
        prompt: "Say hello".into(),
        cwd: root.to_string_lossy().into_owned(),
        env: vec![
            (
                "FAKE_PROVIDER_SCENARIO".into(),
                fixtures.join(scenario).display().to_string(),
            ),
            ("FAKE_PROVIDER_LOG".into(), log.display().to_string()),
        ],
        provider: Some(ProviderTurn {
            driver: "codex".into(),
            binary_path: fixtures.join("fake_cli.py").display().to_string(),
            ..ProviderTurn::default()
        }),
        ..TurnSpec::default()
    }
}

#[tokio::test]
async fn provider_turn_events_survive_the_protocol_acks_and_reconnects() {
    let s = setup().await;
    let log = s.root.join("local.jsonl");
    let local = run_turn(&*s.local, provider_spec(&s.root, "codex_turn.json", &log)).await;
    assert!(local.len() > 20, "{local:?}");

    let log = s.root.join("remote.jsonl");
    let (_cancel, cancelled) = oneshot::channel();
    let mut items = s
        .remote
        .start_tracked_turn(provider_spec(&s.root, "codex_turn.json", &log), cancelled)
        .await
        .unwrap();
    let mut remote = Vec::new();
    loop {
        let item = next_item(&mut items).await;
        let position = item.position.expect("tracked events carry a position");
        let exited = matches!(item.event, TurnEvent::Exited(_));
        remote.push(item.event);
        s.remote.ack(position);
        if remote.len() == 10 {
            s.link.reconnect();
        }
        if exited {
            break;
        }
    }
    // Native ids come from the recording, so both runs report the same events.
    assert_eq!(remote, local);
    assert!(matches!(
        remote.iter().rev().nth(1),
        Some(TurnEvent::Provider(ProviderEvent::TurnTerminal { .. }))
    ));
}

#[tokio::test]
async fn cancelling_a_remote_provider_turn_interrupts_it() {
    let s = setup().await;
    let log = s.root.join("in.jsonl");
    let (cancel, cancelled) = oneshot::channel();
    let mut items = s
        .remote
        .start_tracked_turn(
            provider_spec(&s.root, "codex_interrupt.json", &log),
            cancelled,
        )
        .await
        .unwrap();
    loop {
        let item = next_item(&mut items).await;
        if matches!(
            item.event,
            TurnEvent::Provider(ProviderEvent::ProviderTurn { .. })
        ) {
            break;
        }
    }
    cancel.send(()).unwrap();
    let mut terminal = None;
    let exit = loop {
        match next_item(&mut items).await.event {
            TurnEvent::Provider(ProviderEvent::TurnTerminal { status, .. }) => {
                terminal = Some(status)
            }
            TurnEvent::Exited(exit) => break exit,
            _ => {}
        }
    };
    assert!(exit.cancelled, "{exit:?}");
    assert_eq!(terminal, Some(TurnStatus::Interrupted));
    let sent = std::fs::read_to_string(&log).unwrap();
    assert!(sent.contains("\"turn/interrupt\""), "{sent}");
}
