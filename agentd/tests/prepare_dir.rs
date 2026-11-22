//! Verify the `--prepare-dir-command` hook end to end through `RemoteWorkspace`
//! -> `t3-agentd` -> `LocalWorkspace`.
#![cfg(unix)]

mod support;

use serde_json::json;
use std::{
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant},
};
use t3_agentd::{Agentd, Config, PrepareDirConfig};
use t3code_server::workspace::{
    connect, LocalWorkspace, NativeLink, RemoteWorkspace, TurnEvent, TurnSpec, Workspace,
};
use tokio::sync::oneshot;

const TOKEN: &str = "prepare-dir-test-token";

struct Harness {
    remote: RemoteWorkspace,
    _link: NativeLink,
    _dir: tempfile::TempDir,
    root: PathBuf,
    extra: PathBuf,
}

async fn start_agentd(prepare_dir: Option<PrepareDirConfig>) -> Harness {
    support::isolate_host();
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("projects");
    let extra = dir.path().join("extra-worktrees");
    std::fs::create_dir_all(&root).unwrap();
    std::fs::create_dir_all(&extra).unwrap();
    let local = Arc::new(LocalWorkspace::open(&dir.path().join("state")).unwrap());
    let agentd = Agentd::new(
        local,
        Config {
            token: TOKEN.into(),
            root: root.clone(),
            extra_roots: vec![extra.clone()],
            buffer_dir: dir.path().join("streams"),
            buffer_limit: 1 << 20,
            poll_interval: Duration::from_millis(50),
            prepare_dir,
        },
    )
    .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, agentd.router()).await });
    let remote = RemoteWorkspace::new(1, None);
    let url = format!("ws://{addr}/");
    let link = connect(remote.clone(), url, TOKEN.into());
    for _ in 0..400 {
        if remote.is_ready() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(remote.is_ready(), "remote workspace connected");
    Harness {
        remote,
        _link: link,
        _dir: dir,
        root,
        extra,
    }
}

fn write_script(dir: &Path, name: &str, body: &str) -> PathBuf {
    let path = dir.join(name);
    std::fs::write(&path, format!("#!/bin/sh\nset -eu\n{body}\n")).unwrap();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o700)).unwrap();
    path
}

async fn run_turn(workspace: &dyn Workspace, cwd: &Path, script: &str) -> Vec<TurnEvent> {
    let (_cancel, cancelled) = oneshot::channel();
    let mut events = workspace
        .start_turn(
            TurnSpec {
                command: vec!["sh".into(), "-c".into(), script.into(), "sh".into()],
                prompt: String::new(),
                cwd: cwd.to_string_lossy().into_owned(),
                ..TurnSpec::default()
            },
            cancelled,
        )
        .await
        .unwrap();
    let mut seen = Vec::new();
    while let Some(event) = tokio::time::timeout(Duration::from_secs(20), events.recv())
        .await
        .expect("turn finished in time")
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
async fn hook_runs_once_before_first_use_with_arg_and_env() {
    let scripts = tempfile::tempdir().unwrap();
    let log = scripts.path().join("invocations.log");
    let hook = write_script(
        scripts.path(),
        "prepare.sh",
        &format!(
            r#"printf '%s|%s\n' "$1" "$T3_PREPARE_DIR" >> "{log}"
mkdir -p "$1"
printf 'prepared\n' > "$1/hello.txt""#,
            log = log.display()
        ),
    );
    let h = start_agentd(Some(PrepareDirConfig {
        command: hook,
        timeout: Duration::from_secs(10),
    }))
    .await;

    let project = h.root.join("lazy-project");
    assert!(!project.exists());

    // First request triggers preparation before reading the file.
    let read = json!({"cwd": project, "relativePath": "hello.txt"});
    let reply = h.remote.read_file(&read).await.unwrap().unwrap();
    assert_eq!(reply["contents"], "prepared\n");

    // Follow-up requests (including a trailing-slash spelling and other op types)
    // must not invoke the hook a second time.
    let with_slash = format!("{}/", project.display());
    let second = h
        .remote
        .read_file(&json!({"cwd": with_slash, "relativePath": "hello.txt"}))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(second["contents"], "prepared\n");
    assert!(matches!(
        h.remote
            .vcs("refreshStatus", &json!({"cwd": project}), false)
            .await,
        Some(Ok(_))
    ));
    let events = run_turn(&h.remote, &project, "cat hello.txt").await;
    assert!(events.contains(&TurnEvent::Stdout("prepared\n".into())));

    let canonical = project.canonicalize().unwrap();
    let lines: Vec<String> = std::fs::read_to_string(&log)
        .unwrap()
        .lines()
        .map(str::to_owned)
        .collect();
    assert_eq!(
        lines,
        vec![format!("{}|{}", canonical.display(), canonical.display())]
    );
}

#[tokio::test]
async fn hook_runs_before_turn_terminal_and_vcs_requests() {
    support::enable_test_terminal();
    let scripts = tempfile::tempdir().unwrap();
    let log = scripts.path().join("invocations.log");
    let hook = write_script(
        scripts.path(),
        "prepare.sh",
        &format!(
            r#"printf '%s\n' "$1" >> "{log}"
mkdir -p "$1"
printf 'ready\n' > "$1/marker.txt"
git -C "$1" init -q -b main"#,
            log = log.display()
        ),
    );
    let h = start_agentd(Some(PrepareDirConfig {
        command: hook,
        timeout: Duration::from_secs(10),
    }))
    .await;

    // 1. Provider / process turn on a directory that does not exist yet.
    let turn_dir = h.root.join("for-turn");
    assert!(!turn_dir.exists());
    let events = run_turn(&h.remote, &turn_dir, "cat marker.txt").await;
    assert!(events.contains(&TurnEvent::Stdout("ready\n".into())));

    // 2. Terminal open on a directory that does not exist yet.
    let term_dir = h.root.join("for-term");
    assert!(!term_dir.exists());
    let open = json!({
        "threadId": "t1",
        "terminalId": "term-1",
        "cwd": term_dir,
        "cols": 80,
        "rows": 24
    });
    let opened = h.remote.terminal("open", &open).await.unwrap().unwrap();
    assert_eq!(opened["status"], "running");
    let _ = h
        .remote
        .terminal("close", &json!({"threadId": "t1", "terminalId": "term-1"}))
        .await;

    // 3. VCS request on a directory that does not exist yet.
    let vcs_dir = h.root.join("for-vcs");
    assert!(!vcs_dir.exists());
    let branch = h.remote.vcs_branch(&json!({"cwd": vcs_dir})).await.unwrap();
    assert_eq!(branch["isRepo"], true);

    let lines: Vec<String> = std::fs::read_to_string(&log)
        .unwrap()
        .lines()
        .map(str::to_owned)
        .collect();
    assert_eq!(lines.len(), 3, "{lines:?}");
}

#[tokio::test]
async fn concurrent_callers_wait_for_one_hook_invocation() {
    let scripts = tempfile::tempdir().unwrap();
    let log = scripts.path().join("invocations.log");
    let hook = write_script(
        scripts.path(),
        "slow-prepare.sh",
        &format!(
            r#"printf '%s\n' "$1" >> "{log}"
sleep 0.3
mkdir -p "$1"
printf 'shared\n' > "$1/data.txt""#,
            log = log.display()
        ),
    );
    let h = start_agentd(Some(PrepareDirConfig {
        command: hook,
        timeout: Duration::from_secs(10),
    }))
    .await;

    let shared = h.root.join("shared-dir");
    assert!(!shared.exists());

    let read = json!({"cwd": shared, "relativePath": "data.txt"});
    let list = json!({"cwd": shared});
    let (r1, r2, r3, l1, t1) = tokio::join!(
        h.remote.read_file(&read),
        h.remote.read_file(&read),
        h.remote.read_file(&read),
        h.remote.list_entries(&list),
        run_turn(&h.remote, &shared, "cat data.txt"),
    );

    for reply in [r1, r2, r3] {
        assert_eq!(reply.unwrap().unwrap()["contents"], "shared\n");
    }
    assert!(matches!(l1, Some(Ok(_))));
    assert!(t1.contains(&TurnEvent::Stdout("shared\n".into())));

    let lines: Vec<String> = std::fs::read_to_string(&log)
        .unwrap()
        .lines()
        .map(str::to_owned)
        .collect();
    assert_eq!(
        lines.len(),
        1,
        "concurrent callers must share one run: {lines:?}"
    );
}

#[tokio::test]
async fn failure_and_timeout_are_tolerated_and_request_proceeds() {
    let scripts = tempfile::tempdir().unwrap();
    let log = scripts.path().join("fail.log");
    let failing = write_script(
        scripts.path(),
        "fail.sh",
        &format!(
            r#"printf '%s\n' "$1" >> "{log}"
echo "restore failed: simulated error" >&2
exit 7"#,
            log = log.display()
        ),
    );
    let h = start_agentd(Some(PrepareDirConfig {
        command: failing,
        timeout: Duration::from_secs(10),
    }))
    .await;

    // Existing directory still succeeds despite non-zero hook exit.
    let existing = h.root.join("existing");
    std::fs::create_dir_all(&existing).unwrap();
    std::fs::write(existing.join("ok.txt"), "still works\n").unwrap();
    let read = json!({"cwd": existing, "relativePath": "ok.txt"});
    assert_eq!(
        h.remote.read_file(&read).await.unwrap().unwrap()["contents"],
        "still works\n"
    );
    // Second request does not re-run the failed hook.
    assert_eq!(
        h.remote.read_file(&read).await.unwrap().unwrap()["contents"],
        "still works\n"
    );
    let count = std::fs::read_to_string(&log).unwrap().lines().count();
    assert_eq!(count, 1);

    // Timeout is tolerated and the request still proceeds.
    let slow = write_script(
        scripts.path(),
        "timeout.sh",
        r#"echo "still working..." >&2
exec sleep 30"#,
    );
    let h_timeout = start_agentd(Some(PrepareDirConfig {
        command: slow,
        timeout: Duration::from_millis(200),
    }))
    .await;
    let slow_dir = h_timeout.root.join("slow-existing");
    std::fs::create_dir_all(&slow_dir).unwrap();
    std::fs::write(slow_dir.join("ok.txt"), "after timeout\n").unwrap();
    let started = Instant::now();
    let reply = h_timeout
        .remote
        .read_file(&json!({"cwd": slow_dir, "relativePath": "ok.txt"}))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(reply["contents"], "after timeout\n");
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "timed-out hook should be terminated promptly: {:?}",
        started.elapsed()
    );

    // A missing hook binary is also tolerated.
    let h_missing = start_agentd(Some(PrepareDirConfig {
        command: scripts.path().join("does-not-exist"),
        timeout: Duration::from_secs(5),
    }))
    .await;
    let dir = h_missing.root.join("ok-dir");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("a.txt"), "fine\n").unwrap();
    assert_eq!(
        h_missing
            .remote
            .read_file(&json!({"cwd": dir, "relativePath": "a.txt"}))
            .await
            .unwrap()
            .unwrap()["contents"],
        "fine\n"
    );
}

#[tokio::test]
async fn hook_skips_extra_roots_and_outside_paths() {
    let scripts = tempfile::tempdir().unwrap();
    let log = scripts.path().join("invocations.log");
    let hook = write_script(
        scripts.path(),
        "prepare.sh",
        &format!(r#"printf '%s\n' "$1" >> "{}""#, log.display()),
    );
    let h = start_agentd(Some(PrepareDirConfig {
        command: hook,
        timeout: Duration::from_secs(10),
    }))
    .await;

    // A directory under extra_roots is allowed by confinement, but not under `--root`.
    let wt = h.extra.join("wt1");
    std::fs::create_dir_all(&wt).unwrap();
    std::fs::write(wt.join("f.txt"), "extra\n").unwrap();
    assert_eq!(
        h.remote
            .read_file(&json!({"cwd": wt, "relativePath": "f.txt"}))
            .await
            .unwrap()
            .unwrap()["contents"],
        "extra\n"
    );

    // A directory outside all roots is refused before any hook runs.
    let err = h
        .remote
        .read_file(&json!({"cwd": "/etc", "relativePath": "hosts"}))
        .await
        .unwrap()
        .unwrap_err();
    assert!(err.to_string().contains("outside"), "{err}");

    assert!(!log.exists(), "hook should not have been invoked");
}

#[tokio::test]
async fn absent_flag_preserves_normal_behavior() {
    let h = start_agentd(None).await;
    let existing = h.root.join("present");
    std::fs::create_dir_all(&existing).unwrap();
    std::fs::write(existing.join("file.txt"), "hello\n").unwrap();
    assert_eq!(
        h.remote
            .read_file(&json!({"cwd": existing, "relativePath": "file.txt"}))
            .await
            .unwrap()
            .unwrap()["contents"],
        "hello\n"
    );

    let missing = h.root.join("missing");
    let err = h
        .remote
        .read_file(&json!({"cwd": missing, "relativePath": "file.txt"}))
        .await
        .unwrap()
        .unwrap_err();
    assert!(!err.is_null());
}
