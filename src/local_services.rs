//! Host-backed RPC services. Each service owns the state shared by its clients.
mod agent_device;
mod browser;
mod device_actions;
mod devices;
pub(crate) mod discovery;
mod eas_device_host;
mod eas_project_id;
mod expo_api;
pub(crate) mod files;
pub(crate) mod git_workflow;
pub(crate) mod local_ssh;
pub(crate) mod media;
pub(crate) mod named_project;
mod previews;
pub(crate) mod provider_event_log;
pub(crate) mod provider_turns;
pub(crate) mod review;
mod simlock_device_host;
mod ssh_device_host;
mod terminals;
pub(crate) mod vcs;
mod vcs_upstream;

pub use devices::DeviceService;

/// `claimPreviewRecording`: a stopped recording's pending attachment, checked
/// against what the stop reported and moved to the thread's id. A retry finds
/// the file already moved.
pub(crate) fn claim_preview_recording(
    attachments: &std::path::Path,
    thread_id: &str,
    artifact: &Value,
) -> Result<Value, String> {
    const FAILED: &str = "Preview recording could not be saved to the agent environment.";
    let Some(uploaded) = artifact["uploadedAttachmentId"].as_str() else {
        return Err(
            "Update the desktop app to transfer recordings. The recording remains on the desktop."
                .into(),
        );
    };
    let size = artifact["sizeBytes"].as_u64().ok_or(FAILED)?;
    let (uuid, extension) = uploaded
        .strip_prefix("pending-")
        .and_then(|rest| rest.rsplit_once('-'))
        .filter(|(uuid, ext)| {
            uuid::Uuid::parse_str(uuid).is_ok_and(|u| u.hyphenated().to_string() == *uuid)
                && !ext.is_empty()
                && ext.len() <= 10
                && ext
                    .bytes()
                    .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit())
        })
        .ok_or(FAILED)?;
    let segment = crate::html_render::thread_segment(thread_id).ok_or(FAILED)?;
    let id = format!("{segment}-{uuid}-{extension}");
    let current = attachments.join(format!("{uploaded}.{extension}"));
    let fin = attachments.join(format!("{id}.{extension}"));
    let valid = |path: &std::path::Path| {
        std::fs::symlink_metadata(path)
            .is_ok_and(|m| m.is_file() && m.len() == size && size > 0 && size <= 50 * 1024 * 1024)
    };
    if current.exists() {
        if !valid(&current) {
            return Err(FAILED.into());
        }
        std::fs::rename(&current, &fin).map_err(|_| FAILED.to_owned())?;
    } else if !valid(&fin) {
        return Err(FAILED.into());
    }
    let mut out = artifact.clone();
    if let Some(fields) = out.as_object_mut() {
        fields.remove("uploadedAttachmentId");
    }
    out["id"] = json!(id);
    out["path"] = json!(fin.to_string_lossy());
    Ok(out)
}

pub use previews::PreviewService;
pub use terminals::TerminalService;
pub use vcs::VcsService;

use serde_json::{json, Value};

fn base_directory(dir: &std::path::Path) -> &std::path::Path {
    crate::config::state_base_dir(dir)
}
type Reply = Result<Value, Value>;

fn invalid(key: &str, message: &str) -> Value {
    json!({"_tag":"Die","defect":format!("{message}\n  at [\"{key}\"]")})
}

fn string<'a>(input: &'a Value, key: &str) -> Result<&'a str, Value> {
    let value = input.get(key).ok_or_else(|| invalid(key, "Missing key"))?;
    let value = value
        .as_str()
        .ok_or_else(|| invalid(key, "Expected string"))?
        .trim();
    if value.is_empty() {
        return Err(invalid(key, "Expected a non-blank string"));
    }
    Ok(value)
}

fn timestamp() -> String {
    time::OffsetDateTime::now_utc()
        .format(time::macros::format_description!(
            "[year]-[month]-[day]T[hour]:[minute]:[second].[subsecond digits:3]Z"
        ))
        .unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    struct Fixture(PathBuf);
    impl Fixture {
        fn new() -> Self {
            let path = std::env::temp_dir().join(format!("t3-host-test-{}", uuid::Uuid::new_v4()));
            std::fs::create_dir(&path).unwrap();
            Self(path)
        }
    }
    impl Drop for Fixture {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    #[tokio::test]
    async fn git_status_refs_and_worktrees_share_a_real_repository() {
        let repo = Fixture::new();
        let data = Fixture::new();
        let cwd = repo.0.to_str().unwrap();
        let input = json!({"cwd":cwd});
        let svc = VcsService::default();
        let call = |name: &'static str, input: Value| {
            let dir = &data.0;
            let svc = &svc;
            async move { svc.call(name, &input, dir).await.unwrap().unwrap() }
        };
        assert_eq!(call("refreshStatus", input.clone()).await["isRepo"], false);
        call("init", input.clone()).await;
        assert!(std::process::Command::new("git")
            .current_dir(cwd)
            .args([
                "-c",
                "user.name=Fixture",
                "-c",
                "user.email=fixture@localhost",
                "commit",
                "--allow-empty",
                "-qm",
                "First"
            ])
            .status()
            .unwrap()
            .success());
        std::fs::write(repo.0.join("tracked.txt"), "a\nb\n").unwrap();
        assert!(std::process::Command::new("git")
            .current_dir(cwd)
            .args(["add", "tracked.txt"])
            .status()
            .unwrap()
            .success());
        std::fs::write(repo.0.join("untracked.txt"), "c\nd\ne\n").unwrap();
        let status = call("refreshStatus", input.clone()).await;
        assert_eq!(status["workingTree"]["insertions"], 2);
        assert_eq!(status["branchChanges"]["insertions"], 5);
        call("createRef", json!({"cwd":cwd,"refName":"side"})).await;
        let refs = call("listRefs", json!({"cwd":cwd,"query":"id","limit":1})).await;
        assert_eq!(refs["totalCount"], 1);
        assert_eq!(refs["refs"][0]["name"], "side");
        let wt = call(
            "createWorktree",
            json!({"cwd":cwd,"refName":"side","path":null}),
        )
        .await;
        let path = wt["worktree"]["path"].as_str().unwrap();
        assert!(std::path::Path::new(path).join(".git").exists());
        // The server-internal operations a delegated task's worktree uses.
        let head = call("resolveCommit", json!({"cwd":cwd,"revision":"HEAD"})).await;
        let sha = head["commitSha"].as_str().unwrap();
        assert_eq!(sha.len(), 40);
        let side = call("resolveCommit", json!({"cwd":path,"revision":sha})).await;
        assert_eq!(side["commitSha"], sha);
        call("createRef", json!({"cwd":cwd,"refName":"t3/abcdef01"})).await;
        call("switchRef", json!({"cwd":path,"refName":"t3/abcdef01"})).await;
        call("createRef", json!({"cwd":cwd,"refName":"t3/greet"})).await;
        let renamed = call(
            "renameBranch",
            json!({"cwd":path,"oldBranch":"t3/abcdef01","newBranch":"t3/greet"}),
        )
        .await;
        assert_eq!(renamed["branch"], "t3/greet-1");
        call("removeWorktree", json!({"cwd":cwd,"path":path})).await;
        assert!(!std::path::Path::new(path).exists());
    }

    /// `vcs.listRefs` takes the default branch from `refs/remotes/origin/HEAD`, as the reference
    /// does, and a status read fetches the tracked remote first. Git 2.48 and newer record
    /// `origin/HEAD` during that fetch, so after a status read the default branch is whatever
    /// Git itself now reports, on any Git version.
    #[tokio::test]
    async fn list_refs_default_branch_follows_origin_head_after_a_status_fetch() {
        let root = Fixture::new();
        let data = Fixture::new();
        let git = |cwd: &std::path::Path, args: &[&str]| {
            let output = std::process::Command::new("git")
                .current_dir(cwd)
                .args(args)
                .env("GIT_CONFIG_NOSYSTEM", "1")
                .env("GIT_CONFIG_GLOBAL", "/dev/null")
                .env("GIT_AUTHOR_NAME", "Fixture")
                .env("GIT_AUTHOR_EMAIL", "fixture@localhost")
                .env("GIT_COMMITTER_NAME", "Fixture")
                .env("GIT_COMMITTER_EMAIL", "fixture@localhost")
                .output()
                .unwrap();
            assert!(output.status.success(), "git {args:?}: {output:?}");
            output.status.success()
        };
        let remote = root.0.join("remote.git");
        let repo = root.0.join("repo");
        git(
            &root.0,
            &[
                "init",
                "-q",
                "--bare",
                "-b",
                "main",
                remote.to_str().unwrap(),
            ],
        );
        git(
            &root.0,
            &["init", "-q", "-b", "main", repo.to_str().unwrap()],
        );
        std::fs::write(repo.join("a.txt"), "a\n").unwrap();
        git(&repo, &["add", "-A"]);
        git(&repo, &["commit", "-qm", "a"]);
        git(
            &repo,
            &["remote", "add", "origin", remote.to_str().unwrap()],
        );
        git(&repo, &["push", "-q", "-u", "origin", "main"]);
        git(&repo, &["branch", "side"]);
        let svc = VcsService::default();
        let input = json!({"cwd":repo.to_str().unwrap()});
        let refs = || async {
            let refs = svc
                .call("listRefs", &input, &data.0)
                .await
                .unwrap()
                .unwrap();
            refs["refs"]
                .as_array()
                .unwrap()
                .iter()
                .map(|r| {
                    (
                        r["name"].as_str().unwrap().to_owned(),
                        r["isDefault"] == true,
                    )
                })
                .collect::<Vec<_>>()
        };
        // A push never records origin/HEAD.
        assert_eq!(
            refs().await,
            [("main".to_owned(), false), ("side".to_owned(), false)]
        );

        svc.call("refreshStatus", &input, &data.0)
            .await
            .unwrap()
            .unwrap();
        let origin_head = std::process::Command::new("git")
            .current_dir(&repo)
            .args([
                "rev-parse",
                "--verify",
                "--quiet",
                "refs/remotes/origin/HEAD",
            ])
            .status()
            .unwrap()
            .success();
        assert_eq!(
            refs().await,
            [("main".to_owned(), origin_head), ("side".to_owned(), false)]
        );

        git(&repo, &["remote", "set-head", "origin", "main"]);
        assert_eq!(
            refs().await,
            [("main".to_owned(), true), ("side".to_owned(), false)]
        );
        // A HEAD that names a branch the remote deleted is no default, as `rev-parse --verify`
        // fails where `symbolic-ref` would still answer.
        git(
            &repo,
            &[
                "symbolic-ref",
                "refs/remotes/origin/HEAD",
                "refs/remotes/origin/side",
            ],
        );
        assert_eq!(
            refs().await,
            [("main".to_owned(), false), ("side".to_owned(), false)]
        );
    }

    #[test]
    fn preview_threads_and_revisions_follow_mutations() {
        let service = PreviewService::default();
        let mut events = service.subscribe();
        let call = |name: &str, input: Value| service.call(name, &input).unwrap().unwrap();
        let tab = call("open", json!({"threadId":"t","url":"localhost:4100"}));
        assert_eq!(
            tab["navStatus"],
            json!({"_tag":"Loading","url":"http://localhost:4100/","title":""})
        );
        let tab_id = tab["tabId"].clone();
        assert_eq!(events.try_recv().unwrap()["revision"], 1);
        call(
            "reportStatus",
            json!({"threadId":"t","tabId":tab_id,"navStatus":{"_tag":"Success","url":"http://localhost:4100/","title":"Home"},"canGoBack":true,"canGoForward":false}),
        );
        assert_eq!(
            call("list", json!({"threadId":"another"}))["sessions"],
            json!([])
        );
        assert_eq!(
            call("list", json!({"threadId":"t"}))["sessions"][0]["canGoBack"],
            true
        );
        call("close", json!({"threadId":"t"}));
        assert_eq!(call("list", json!({"threadId":"t"}))["revision"], 3);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn terminal_writes_reach_the_pty_and_close_is_scoped_to_thread() {
        let svc = TerminalService::default();
        let key = json!({"threadId":"t","terminalId":"term-2"});
        let mut input = key.clone();
        input["cwd"] = json!("/tmp");
        let mut events = svc.subscribe();
        let opened = svc.call("open", &input).await.unwrap().unwrap();
        assert_eq!(opened["label"], "Terminal 2");
        assert!(opened["pid"].as_u64().is_some());
        let mut resize = key.clone();
        resize["rows"] = json!(36);
        resize["cols"] = json!(82);
        svc.call("resize", &resize).await.unwrap().unwrap();
        let mut write = key.clone();
        write["data"] = json!("printf '%s%s!\\n' T3 HOST; stty size\r");
        svc.call("write", &write).await.unwrap().unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(8), async {
            let mut history = String::new();
            while !history.contains("T3HOST!") || !history.contains("36 82") {
                let event = events.recv().await.unwrap();
                if let Some(data) = event["data"].as_str() {
                    history.push_str(data);
                }
            }
        })
        .await
        .unwrap();
        svc.call("close", &json!({"threadId":"other"}))
            .await
            .unwrap()
            .unwrap();
        assert!(svc.call("observe", &key).await.unwrap().is_ok());
        svc.call("close", &key).await.unwrap().unwrap();
        assert_eq!(
            svc.call("observe", &key).await.unwrap().unwrap_err()["_tag"],
            "TerminalSessionLookupError"
        );
    }

    #[tokio::test]
    async fn migrates_device_sidecar_without_overwriting_shared_preferences() {
        let dir = Fixture::new();
        std::fs::write(
            dir.0.join("settings.json"),
            "{ // Explicitly configured by the client\n \"enableDeviceSupport\":false,}",
        )
        .unwrap();
        std::fs::write(
            dir.0.join("device-settings.json"),
            r#"{"enabled":true,"agentAccessEnabled":true,"onboardingCompleted":true}"#,
        )
        .unwrap();
        let server = crate::AppState::new(&dir.0, Vec::new()).await.unwrap();
        let current = server.settings.internal().unwrap();
        assert_eq!(current["enableDeviceSupport"], false);
        assert_eq!(current["enableAgentDeviceAccess"], true);
        assert_eq!(current["deviceOnboardingCompleted"], true);
        assert!(!dir.0.join("device-settings.json").exists());
    }

    #[tokio::test]
    async fn device_settings_survive_restart() {
        let data = Fixture::new();
        let server = crate::AppState::new(data.0.clone(), Vec::new())
            .await
            .unwrap();
        let service = &server.local.devices;
        let updated = service
            .call(
                "configure",
                &json!({"enabled":false,"onboardingCompleted":true}),
                &data.0,
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(updated["hostStatus"], "disabled");
        assert_eq!(updated["revision"], 1);
        assert_eq!(
            server.settings.internal().unwrap()["deviceOnboardingCompleted"],
            true
        );
        assert!(!data.0.join("device-settings.json").exists());
        drop(server);
        let fresh = crate::AppState::new(data.0.clone(), Vec::new())
            .await
            .unwrap();
        assert_eq!(
            fresh.local.devices.snapshot(&data.0)["onboardingCompleted"],
            true
        );
    }
}

#[cfg(test)]
mod claim_tests {
    use super::*;

    #[test]
    fn a_pending_recording_is_claimed_once_for_its_thread() {
        let dir = tempfile::tempdir().unwrap();
        let uuid = "0b9c2c7e-6a2b-4b8f-9f1e-2f5b7c3a1d00";
        let pending = format!("pending-{uuid}-webm");
        std::fs::write(dir.path().join(format!("{pending}.webm")), b"video").unwrap();
        let artifact = json!({"id": pending, "tabId": "t", "path": "x", "mimeType": "video/webm", "sizeBytes": 5, "createdAt": "now", "uploadedAttachmentId": pending});
        let claimed = claim_preview_recording(dir.path(), "thread:One", &artifact).unwrap();
        let id = format!("thread-one-{uuid}-webm");
        assert_eq!(claimed["id"], json!(id));
        assert!(claimed.get("uploadedAttachmentId").is_none());
        assert!(dir.path().join(format!("{id}.webm")).is_file());
        // A retry finds the file already moved.
        assert!(claim_preview_recording(dir.path(), "thread:One", &artifact).is_ok());
        let wrong = json!({"sizeBytes": 6, "uploadedAttachmentId": pending});
        assert!(claim_preview_recording(dir.path(), "thread:One", &wrong).is_err());
        let forged = json!({"sizeBytes": 5, "uploadedAttachmentId": "pending-../../x-webm"});
        assert!(claim_preview_recording(dir.path(), "thread:One", &forged).is_err());
        assert_eq!(
            claim_preview_recording(dir.path(), "thread:One", &json!({"sizeBytes": 5}))
                .unwrap_err(),
            "Update the desktop app to transfer recordings. The recording remains on the desktop."
        );
    }
}
