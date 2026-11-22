use crate::{issues, AppState, Project};
use serde_json::{json, Value};
use std::{path::Path, process::Stdio, time::Duration};
use tokio::io::AsyncWriteExt;

pub(crate) type Reply = Result<Value, Value>;

pub async fn dispatch(state: &AppState, method: &str, params: &Value) -> Option<Reply> {
    if method.starts_with("issueTrackers.") {
        return issues::tracker_dispatch(state, method, params).await;
    }
    if !method.starts_with("issues.") && !crate::pull_requests::handles(method) {
        return None;
    }
    let cli = state.issues.lock().await.gh.clone();
    if let Some(op) = method.strip_prefix("pullRequests.") {
        return Some(crate::pull_requests::read(state, &cli, op, params).await);
    }
    issues::dispatch(state, &cli, method, params).await
}

pub(crate) fn valid_repository(scope: &str) -> bool {
    let Some((owner, repo)) = scope.split_once('/') else {
        return false;
    };
    owner
        .as_bytes()
        .first()
        .is_some_and(u8::is_ascii_alphanumeric)
        && owner
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || c == b'-')
        && !matches!(repo, "" | "." | "..")
        && repo
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || b"-_.".contains(&c))
}

pub(crate) async fn github_remote(project: &Project) -> Option<String> {
    let git = crate::login_shell::tokio_command("git")
        .args(["-C", &project.path, "remote", "get-url", "origin"])
        .kill_on_drop(true)
        .output();
    let result = tokio::time::timeout(Duration::from_secs(5), git)
        .await
        .ok()?
        .ok()?;
    if !result.status.success() {
        return None;
    }
    let remote = std::str::from_utf8(&result.stdout)
        .ok()?
        .trim()
        .trim_end_matches('/');
    let scope = [
        "https://github.com/",
        "http://github.com/",
        "git@github.com:",
        "ssh://git@github.com/",
        "git://github.com/",
    ]
    .iter()
    .find_map(|prefix| {
        remote
            .get(..prefix.len())
            .filter(|p| p.eq_ignore_ascii_case(prefix))
            .map(|_| &remote[prefix.len()..])
    })?;
    let scope = scope.strip_suffix(".git").unwrap_or(scope);
    valid_repository(scope).then(|| scope.to_owned())
}

pub(crate) async fn project(state: &AppState, params: &Value) -> Option<Project> {
    let id = params["projectId"].as_str()?;
    state.store.read().await.projects.get(id).cloned()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Failure {
    Missing,
    Auth,
    RateLimit,
    NotFound,
    Refused,
    Invalid,
}

// Drain both pipes even after the retained prefix fills so the CLI can finish writing.
async fn bounded_read(
    mut input: impl tokio::io::AsyncRead + Unpin,
    limit: usize,
) -> std::io::Result<Vec<u8>> {
    use tokio::io::AsyncReadExt;
    let mut bytes = Vec::new();
    let mut buffer = [0; 8192];
    loop {
        let count = input.read(&mut buffer).await?;
        if count == 0 {
            return Ok(bytes);
        }
        bytes.extend_from_slice(&buffer[..count.min(limit - bytes.len())]);
    }
}

pub(crate) struct Github<'a> {
    pub cli: &'a Path,
    pub cwd: &'a str,
    token: Option<String>,
    host: &'a str,
}

impl<'a> Github<'a> {
    pub fn new(cli: &'a Path, cwd: &'a str) -> Self {
        Self {
            cli,
            cwd,
            token: None,
            host: "github.com",
        }
    }
    pub(crate) fn at_host(cli: &'a Path, cwd: &'a str, host: &'a str) -> Self {
        Self {
            host,
            ..Self::new(cli, cwd)
        }
    }
    pub async fn pin_credential(&mut self) -> Result<(), Failure> {
        let cmd = crate::login_shell::tokio_command(self.cli)
            .args(["auth", "token", "--hostname", self.host])
            .env("GH_PROMPT_DISABLED", "1")
            .current_dir(self.cwd)
            .kill_on_drop(true)
            .output();
        let output = tokio::time::timeout(Duration::from_secs(15), cmd)
            .await
            .map_err(|_| Failure::Auth)?
            .map_err(|_| Failure::Missing)?;
        if !output.status.success() {
            return Err(Failure::Auth);
        }
        let token = String::from_utf8(output.stdout).map_err(|_| Failure::Auth)?;
        let token = token.trim_end_matches(['\r', '\n']);
        if token.is_empty() || !token.bytes().all(|b| b.is_ascii_graphic()) {
            return Err(Failure::Auth);
        }
        self.token = Some(token.to_owned());
        Ok(())
    }
    pub async fn request(
        &self,
        method: &str,
        endpoint: &str,
        accept: &str,
        body: Option<Value>,
    ) -> Result<Vec<u8>, Failure> {
        let mut command = crate::login_shell::tokio_command(self.cli);
        command
            .current_dir(self.cwd)
            .args([
                "api",
                "--hostname",
                self.host,
                "--method",
                method,
                "-H",
                &format!("Accept: {accept}"),
            ])
            .env("GH_PROMPT_DISABLED", "1")
            .env("GH_NO_UPDATE_NOTIFIER", "1")
            .kill_on_drop(true)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        if let Some(token) = &self.token {
            command.env("GH_TOKEN", token);
        }
        if body.is_some() {
            command.args(["--input", "-"]).stdin(Stdio::piped());
        }
        command.arg("--").arg(endpoint);
        tokio::time::timeout(Duration::from_secs(60), async {
            let mut child = command.spawn().map_err(|e| {
                if e.kind() == std::io::ErrorKind::NotFound {
                    Failure::Missing
                } else {
                    Failure::Refused
                }
            })?;
            if let Some(body) = body {
                let mut stdin = child.stdin.take().ok_or(Failure::Refused)?;
                stdin
                    .write_all(body.to_string().as_bytes())
                    .await
                    .map_err(|_| Failure::Refused)?;
            }
            let stdout = child.stdout.take().ok_or(Failure::Refused)?;
            let stderr = child.stderr.take().ok_or(Failure::Refused)?;
            let max_bytes = match accept {
                "application/vnd.github.diff" => 32 * 1024 * 1024 + 1,
                "application/vnd.github.raw+json" => 1024 * 1024 + 1,
                _ => 8 * 1024 * 1024 + 1,
            };
            let stdout_task = tokio::spawn(bounded_read(stdout, max_bytes));
            let stderr_task = tokio::spawn(bounded_read(stderr, 16 * 1024));
            let status = child.wait().await.map_err(|_| Failure::Refused)?;
            let stdout = stdout_task
                .await
                .map_err(|_| Failure::Refused)?
                .map_err(|_| Failure::Refused)?;
            let stderr = stderr_task
                .await
                .map_err(|_| Failure::Refused)?
                .map_err(|_| Failure::Refused)?;
            if status.success() {
                return Ok(stdout);
            }
            let error = String::from_utf8_lossy(&stderr).to_lowercase();
            Err(
                if error.contains("rate limit") || error.contains("http 429") {
                    Failure::RateLimit
                } else if status.code() == Some(4)
                    || error.contains("gh auth login")
                    || error.contains("http 401")
                    || error.contains("bad credentials")
                {
                    Failure::Auth
                } else if error.contains("http 404") {
                    Failure::NotFound
                } else {
                    Failure::Refused
                },
            )
        })
        .await
        .map_err(|_| Failure::Refused)?
    }
    pub async fn json(
        &self,
        method: &str,
        endpoint: &str,
        body: Option<Value>,
    ) -> Result<Value, Failure> {
        let data = self
            .request(method, endpoint, "application/vnd.github+json", body)
            .await?;
        serde_json::from_slice(&data).map_err(|_| Failure::Invalid)
    }
    pub async fn graphql(&self, query: &str, variables: Value) -> Result<Value, Failure> {
        let response = self
            .json(
                "POST",
                "graphql",
                Some(json!({"query":query,"variables":variables})),
            )
            .await?;
        if response["errors"].is_array() {
            return Err(Failure::Refused);
        }
        response.get("data").cloned().ok_or(Failure::Invalid)
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use crate::provider_http;
    use std::os::unix::fs::PermissionsExt;
    use tokio::fs;
    use uuid::Uuid;

    #[tokio::test]
    async fn provider_calls_use_registered_checkout_and_server_credentials() {
        let root = std::env::temp_dir().join(format!("t3-provider-{}", Uuid::new_v4()));
        let state = AppState::new(&root, vec![]).await.unwrap();
        let repo = root.join("repo");
        assert!(tokio::process::Command::new("git")
            .args(["init", "--quiet"])
            .arg(&repo)
            .status()
            .await
            .unwrap()
            .success());
        assert!(tokio::process::Command::new("git")
            .current_dir(&repo)
            .args(["remote", "add", "origin", "git@github.com:example/proj.git"])
            .status()
            .await
            .unwrap()
            .success());
        let id = Uuid::new_v4().to_string();
        state.store.write().await.projects.insert(
            id.clone(),
            Project {
                id: id.clone(),
                path: repo.to_str().unwrap().into(),
                name: "Example".into(),
                created_at: 1,
            },
        );
        let fake = root.join("gh-fixture");
        fs::write(
            &fake,
            r#"#!/bin/sh
set -eu
printf '%s\n' "$@" >> calls
if [ "$1" = auth ]; then printf fixture-token; exit 0; fi
test "$1" = api && test "$2" = --hostname && test "$3" = github.com
for endpoint do :; done
case "$endpoint" in
  user) printf '%s' '{"id":72,"login":"demo"}' ;;
  repos/example/proj/issues)
    cat > request
    printf '%s' '{"number":9,"html_url":"https://github.com/example/proj/issues/9"}' ;;
  graphql)
    cat > request
    if test -f fixture-response; then cat fixture-response; else printf '%s' '{"data":{"repository":{"issue":{"id":"I9","state":"OPEN","stateReason":null}}}}'; fi ;;
  repos/example/proj/issues/9/comments)
    cat > comment
    printf '%s' '{"id":27}' ;;
  repos/example/proj/pulls/7)
    printf '%s\n' 'diff --git a/one b/one' '--- a/one' '+++ b/one' '@@ -1 +1 @@' '-a' '+b' ;;
  'repos/example/proj/pulls/7/files?per_page=30&page=1')
    printf '%s' '[{"filename":"one","status":"modified","patch":"@@ -1 +1 @@\n-a\n+b"}]' ;;
  *) exit 1 ;;
esac
"#,
        )
        .await
        .unwrap();
        fs::set_permissions(&fake, std::fs::Permissions::from_mode(0o700))
            .await
            .unwrap();
        state.issues.lock().await.gh = fake;
        let mut args = json!({"projectId":id,"tracker":"github","scope":"example/other","title":"A title","body":"Body","assignees":[],"labels":[]});
        let err = dispatch(&state, "issues.create", &args)
            .await
            .unwrap()
            .unwrap_err();
        assert_eq!(err["reason"], "read-only");
        assert!(!repo.join("calls").exists());
        args["scope"] = json!("example/proj");
        let result = dispatch(&state, "issues.create", &args)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            result,
            json!({"key":"9","displayKey":"#9","url":"https://github.com/example/proj/issues/9"})
        );
        assert_eq!(
            serde_json::from_slice::<Value>(&fs::read(repo.join("request")).await.unwrap())
                .unwrap(),
            json!({"title":"A title","body":"Body","labels":[],"assignees":[]})
        );
        args["key"] = json!("9");
        assert!(dispatch(&state, "issues.comment", &args)
            .await
            .unwrap()
            .unwrap()
            .is_null());
        assert_eq!(
            fs::read_to_string(repo.join("comment")).await.unwrap(),
            r#"{"body":"Body"}"#
        );

        let issue = json!({"__typename":"Issue","number":9,"title":"Live","url":"https://github.com/example/proj/issues/9","author":{"login":"demo","avatarUrl":null},"state":"OPEN","createdAt":"2026-01-01T00:00:00Z","updatedAt":"2026-01-02T00:00:00Z","body":"Provider body","viewerCanUpdate":true,"viewerCanClose":true,"comments":{"totalCount":1,"pageInfo":{"hasPreviousPage":true,"startCursor":"older"},"nodes":[{"id":"C17","author":{"login":"demo"},"createdAt":"2026-01-02T00:00:00Z","body":"Provider comment","url":null}]}});
        fs::write(repo.join("fixture-response"),json!({"data":{"viewer":{"login":"demo"},"repository":{"viewerPermission":"WRITE","issue":issue}}}).to_string()).await.unwrap();
        let detail = dispatch(&state, "issues.detail", &args)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(detail["body"], "Provider body");
        assert_eq!(detail["writable"], true);
        assert_eq!(detail["comments"][0]["body"], "Provider comment");
        args["cursor"] = json!("older");
        let comments = dispatch(&state, "issues.comments", &args)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(comments["nextCursor"], "older");
        let templates = json!({"issueTemplates":[],"templateDirectory":{"__typename":"Tree","entries":[{"name":"bug.yml","object":{"__typename":"Blob","isTruncated":false,"text":"name: Bug\ndescription: Report a bug\nbody:\n  - type: textarea\n    attributes:\n      label: Steps\n    validations:\n      required: true"}},{"name":"config.yml","object":{"__typename":"Blob","text":"blank_issues_enabled: false","isTruncated":false}}]}});
        fs::write(
            repo.join("fixture-response"),
            json!({"data":{"repository":templates}}).to_string(),
        )
        .await
        .unwrap();
        let forms = dispatch(&state, "issues.templates", &args)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(forms["blankIssuesEnabled"], false);
        assert_eq!(forms["templates"][0]["body"], "### Steps\n\nRequired.");
        let identity = dispatch(
            &state,
            "pullRequests.routingIdentity",
            &json!({"host":"github.com"}),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(identity["viewer"], "demo");
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let app = provider_http::routes().with_state(state.clone());
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let client = reqwest::Client::new();
        let url = format!("http://{address}/api/pull-requests/diff");
        let mut reference =
            json!({"projectId":id,"repository":"example/proj","number":7,"patch":"forged"});
        let full = client.post(&url).json(&reference).send().await.unwrap();
        assert_eq!(full.status(), 200);
        let expected_patch = "diff --git a/one b/one\n--- a/one\n+++ b/one\n@@ -1 +1 @@\n-a\n+b\n";
        assert_eq!(
            full.bytes().await.unwrap().as_ref(),
            json!({"patch":expected_patch,"truncated":false,"nextCursor":null})
                .to_string()
                .as_bytes()
        );
        reference["cursor"] = json!("1");
        reference["host"] = json!("github.com");
        reference["expectedAccountId"] = json!("72");
        assert_eq!(
            client
                .post(&url)
                .json(&reference)
                .send()
                .await
                .unwrap()
                .json::<Value>()
                .await
                .unwrap()["patch"],
            expected_patch
        );
        reference["changeType"] = json!("change");
        reference["oldPath"] = json!("secret");
        reference["newPath"] = json!("secret");
        assert_eq!(
            dispatch(&state, "pullRequests.diffFileContents", &reference)
                .await
                .unwrap()
                .unwrap_err()["reason"],
            "not-found"
        );
        assert!(!fs::read_to_string(repo.join("calls"))
            .await
            .unwrap()
            .contains("contents/secret"));

        let voice = format!("http://{address}/api/voice");
        let empty = br#"{"openai":false,"xai":false,"openrouter":false}"#;
        assert_eq!(
            client
                .get(format!("{voice}/cloud/keys"))
                .send()
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap()
                .as_ref(),
            empty
        );
        let transcribe = client
            .post(format!("{voice}/cloud/openai/transcribe"))
            .body(Vec::new())
            .header(
                "traceparent",
                "00-123456789abcdef0123456789abcdef0-123456789abcdef0-01",
            )
            .send()
            .await
            .unwrap();
        assert_eq!(transcribe.status(), 400);
        assert_eq!(
            transcribe.json::<Value>().await.unwrap()["traceId"],
            "123456789abcdef0123456789abcdef0"
        );
        let opts=br#"{"speechLanguage":"auto","speechTranslateToEnglish":false,"speechCustomWords":[],"speechRemoveFillerWords":true}"#;
        let mut data = (opts.len() as u32).to_le_bytes().to_vec();
        data.extend(opts);
        data.extend(0f32.to_le_bytes());
        let res = client
            .post(format!("{voice}/cloud/openai/transcribe"))
            .body(data)
            .send()
            .await
            .unwrap();
        assert_eq!(res.status(), 422);
        assert_eq!(
            res.bytes().await.unwrap().as_ref(),
            br#"{"_tag":"CloudSpeechError","provider":"openai","reason":"missing-key"}"#
        );
        for value in [json!("fixture-only"), Value::Null] {
            assert_eq!(
                client
                    .post(format!("{voice}/cloud/xai/key"))
                    .json(&json!({"key":value}))
                    .send()
                    .await
                    .unwrap()
                    .status(),
                204
            );
            let restarted = AppState::new(&root, vec![]).await.unwrap();
            assert_eq!(
                restarted.voice.cloud_keys().await.unwrap()["xai"],
                !value.is_null()
            );
        }
        let blank = json!({"options":{"speechPostProcessingPrompt":{"mode":"default","customInstructions":""},"speechCustomWords":[]},"draft":{"text":"","selection":{"start":0,"end":0}},"transcript":"  \n "});
        let processed = client
            .post(format!("{voice}/post-process"))
            .json(&blank)
            .send()
            .await
            .unwrap()
            .json::<Value>()
            .await
            .unwrap();
        assert_eq!(processed, json!({"text":"  \n "}));
        let connection = dispatch(&state, "issueTrackers.list", &json!({}))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(connection["trackers"][0]["connected"], false);
        let binding = json!({"projectId":id,"binding":{"tracker":"linear","scope":"ENG"}});
        assert_eq!(
            dispatch(&state, "issueTrackers.bindProject", &binding)
                .await
                .unwrap()
                .unwrap(),
            Value::Null
        );
        let restarted = AppState::new(&root, vec![]).await.unwrap();
        assert_eq!(
            dispatch(&restarted, "issueTrackers.list", &json!({}))
                .await
                .unwrap()
                .unwrap()["bindings"][id.to_string()],
            binding["binding"]
        );
        server.abort();
        fs::remove_dir_all(root).await.unwrap();
    }
}
