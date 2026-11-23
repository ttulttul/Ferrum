//! A repository's clone URLs, as the host `provider` names reports them
//! (`SourceControlRepositoryService.lookupRepository` and each provider's
//! `getRepositoryCloneUrls` in the reference).
//!
//! GitHub answers through its REST API via `gh api` (which honours the
//! CLI's `http_unix_socket`), GitLab through `glab api`, Bitbucket through its
//! REST API (`T3CODE_BITBUCKET_API_BASE_URL`, with the credential `bitbucket_credential` reads),
//! Azure DevOps through `az repos show`, Forgejo/Gitea through `tea api`,
//! GitCafe through its API with the `cafe` credential helper's token, and
//! Cursor Origin through its API with `origin auth token`'s. Every answer is
//! `{provider, nameWithOwner, url, sshUrl}` (`SourceControlRepositoryInfo`).
use serde_json::{json, Value};
use std::path::Path;
use std::process::Stdio;
use std::time::Duration;
use tokio::io::AsyncWriteExt;

const LOOKUP_FAILED: &str = "The source control operation could not be completed.";
const CLI_TIMEOUT: Duration = Duration::from_secs(60);

/// Why a lookup failed.
pub(crate) enum LookupFailure {
    /// A `SourceControlRepositoryError` that keeps its own operation.
    Repository(Value),
    /// A provider's failure (`SourceControlProviderError` cause): `detail`,
    /// and the error under it. The caller names the operation.
    Provider {
        detail: String,
        cause: Option<Value>,
        /// The provider's `userFacingDetail` (#31239): the repository
        /// error's own detail, when the provider knows why it failed.
        user_facing: Option<String>,
    },
}

impl LookupFailure {
    fn provider(detail: impl Into<String>, cause: Option<Value>) -> Self {
        Self::Provider {
            detail: detail.into(),
            cause,
            user_facing: None,
        }
    }

    fn user_facing(mut self, text: String) -> Self {
        if let Self::Provider { user_facing, .. } = &mut self {
            *user_facing = Some(text);
        }
        self
    }

    /// The `SourceControlRepositoryError` an RPC answers with, for `operation`.
    pub(crate) fn into_error(self, provider: &str, operation: &str) -> Value {
        match self {
            Self::Repository(error) => error,
            Self::Provider {
                detail,
                cause,
                user_facing,
            } => {
                let mut provider_error = json!({
                    "name": "SourceControlProviderError",
                    "message": format!("Source control provider {provider} failed in getRepositoryCloneUrls: {detail}"),
                });
                if let Some(cause) = cause {
                    provider_error["cause"] = cause;
                }
                json!({"_tag": "SourceControlRepositoryError", "provider": provider, "operation": operation,
                    "detail": user_facing.as_deref().unwrap_or(LOOKUP_FAILED), "cause": provider_error})
            }
        }
    }
}

/// `repositoryNotFoundDetail` (#31239).
fn repository_not_found(repository: &str) -> String {
    let repository: String = repository
        .chars()
        .filter(|c| !c.is_control())
        .take(256)
        .collect();
    format!("Repository {repository} was not found. Check the owner and name and try again.")
}

fn info(provider: &str, name_with_owner: &str, url: &str, ssh_url: &str) -> Value {
    json!({"provider": provider, "nameWithOwner": name_with_owner, "url": url, "sshUrl": ssh_url})
}

fn component(value: &str) -> String {
    crate::thread_projection::uri_component(value)
}

/// `VcsProcessExitError`'s message.
fn process_exit(operation: &str, command: &str, cwd: &str, code: i32, detail: &str) -> Value {
    json!({"name": "VcsProcessExitError",
        "message": format!("VCS process failed in {operation}: {command} ({cwd}) exited with {code} - {detail}")})
}

/// What a host CLI printed, or why it could not run.
struct Output {
    code: i32,
    stdout: String,
    stderr: String,
}

async fn run(
    cwd: &str,
    command: &str,
    args: &[&str],
    stdin: Option<&str>,
    env: &[(&str, &str)],
) -> std::io::Result<Output> {
    let mut process = crate::login_shell::tokio_command(command);
    process
        .args(args)
        .current_dir(cwd)
        .kill_on_drop(true)
        .stdin(if stdin.is_some() {
            Stdio::piped()
        } else {
            Stdio::null()
        })
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    for (name, value) in env {
        process.env(name, value);
    }
    let mut child = process.spawn()?;
    if let (Some(input), Some(mut pipe)) = (stdin, child.stdin.take()) {
        pipe.write_all(input.as_bytes()).await?;
    }
    let output = tokio::time::timeout(CLI_TIMEOUT, child.wait_with_output())
        .await
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::TimedOut, "timed out"))??;
    Ok(Output {
        code: output.status.code().unwrap_or(-1),
        stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
        stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
    })
}

/// The clone URLs `provider` reports for `repository`, looked up from `cwd`.
pub(crate) async fn lookup(
    provider: &str,
    repository: &str,
    cwd: &str,
) -> Result<Value, LookupFailure> {
    let repository = repository.trim();
    match provider {
        "unknown" | "" => Err(LookupFailure::Repository(json!({
            "_tag": "SourceControlRepositoryError", "provider": "unknown", "operation": "lookupRepository",
            "detail": "Choose a source control provider before continuing.",
        }))),
        "github" => github(repository, cwd).await,
        "gitlab" => gitlab(repository, cwd).await,
        "bitbucket" => bitbucket(repository).await,
        "azure-devops" => azure(repository, cwd).await,
        "forgejo" => forgejo(repository, cwd).await,
        "gitcafe" => gitcafe(repository, cwd).await,
        "cursor-origin" => origin(repository, cwd).await,
        other => Err(LookupFailure::provider(
            format!("No {other} source control provider is registered."),
            None,
        )),
    }
}

// --- GitHub -----------------------------------------------------------------

const REPOSITORY_NOT_FOUND: &str = "Repository not found. Check the owner and name and try again.";

/// `parseGitHubRepositorySelector`: (host, owner, name).
fn github_selector(selector: &str, default_host: &str) -> Option<(String, String, String)> {
    let without_git = |path: &str| -> String {
        let trimmed = path.trim_end_matches('/');
        trimmed
            .strip_suffix(".git")
            .or_else(|| trimmed.strip_suffix(".GIT"))
            .unwrap_or(trimmed)
            .to_owned()
    };
    let trimmed = selector.trim();
    let lower = trimmed.to_ascii_lowercase();
    if lower.starts_with("http://") || lower.starts_with("https://") {
        let url = url::Url::parse(trimmed).ok()?;
        let path = without_git(url.path());
        let parts: Vec<&str> = path.split('/').filter(|p| !p.is_empty()).collect();
        let host = match url.port() {
            Some(port) => format!("{}:{port}", url.host_str()?),
            None => url.host_str()?.to_owned(),
        };
        return (parts.len() == 2).then(|| {
            (
                host.to_lowercase(),
                parts[0].to_owned(),
                parts[1].to_owned(),
            )
        });
    }
    let path = without_git(trimmed);
    let parts: Vec<&str> = path.split('/').filter(|p| !p.is_empty()).collect();
    if parts.len() == 2 {
        return Some((
            default_host.to_owned(),
            parts[0].to_owned(),
            parts[1].to_owned(),
        ));
    }
    let host = parts.first()?.to_lowercase();
    (parts.len() == 3
        && (host == "github.com" || host.ends_with(".ghe.com") || host.contains("github")))
    .then(|| (host, parts[1].to_owned(), parts[2].to_owned()))
}

fn github_failure(
    failure: crate::provider_rpc::Failure,
    host: &str,
    not_found: &str,
) -> LookupFailure {
    use crate::provider_rpc::Failure;
    let named = |name: &str, message: String| Some(json!({"name": name, "message": message}));
    match failure {
        Failure::Missing => LookupFailure::provider(
            "No GitHub credential on the server. Set GH_TOKEN, or install the GitHub CLI and run `gh auth login`.",
            named("GitHubCliMissingError", format!("No GitHub credential for {host}: set GH_TOKEN, or install the GitHub CLI and run `gh auth login`.")),
        ),
        Failure::Auth => {
            let message = format!("No GitHub credential for {host}: run `gh auth login --hostname {host}`.");
            LookupFailure::provider(message.clone(), named("GitHubNotSignedInError", message))
        }
        Failure::RateLimit => LookupFailure::provider("GitHub API rate limit exceeded. Requests resume when the limit resets.", None),
        Failure::NotFound => LookupFailure::provider(
            not_found,
            named("GitHubApiNotFoundError", "GitHub could not find the requested resource, or the credential cannot see it.".into()),
        ),
        Failure::Refused | Failure::Invalid => LookupFailure::provider("GitHub request failed.", None),
    }
}

async fn github(repository: &str, cwd: &str) -> Result<Value, LookupFailure> {
    let default_host = std::env::var("GH_HOST")
        .map(|h| h.to_lowercase())
        .unwrap_or_else(|_| "github.com".into());
    let cli = Path::new("gh");
    let bare = repository.trim_end_matches(".git");
    let (host, owner, name) = match github_selector(repository, &default_host) {
        Some(found) => found,
        None if !bare.is_empty() && !bare.contains('/') && !bare.contains(char::is_whitespace) => {
            let mut gh = crate::provider_rpc::Github::at_host(cli, cwd, &default_host);
            gh.pin_credential()
                .await
                .map_err(|f| github_failure(f, &default_host, REPOSITORY_NOT_FOUND))?;
            let body = gh
                .request("GET", "user", "application/vnd.github+json", None)
                .await
                .map_err(|f| {
                    github_failure(
                        f,
                        &default_host,
                        "Pull request not found. Check the PR number or URL and try again.",
                    )
                })?;
            let login = serde_json::from_slice::<Value>(&body)
                .ok()
                .and_then(|v| {
                    v["login"]
                        .as_str()
                        .map(str::trim)
                        .filter(|l| !l.is_empty())
                        .map(str::to_owned)
                })
                .ok_or_else(|| LookupFailure::provider("GitHub request failed.", None))?;
            (default_host.clone(), login, bare.to_owned())
        }
        None => {
            let detail = "Repositories are named owner/name.";
            return Err(LookupFailure::provider(
                detail,
                Some(json!({"name": "GitHubRepositoryLookupError", "message": ""})),
            )
            .user_facing(detail.into()));
        }
    };
    let mut gh = crate::provider_rpc::Github::at_host(cli, cwd, &host);
    gh.pin_credential()
        .await
        .map_err(|f| github_failure(f, &host, REPOSITORY_NOT_FOUND))?;
    let body = gh
        .request(
            "GET",
            &format!("repos/{}/{}", component(&owner), component(&name)),
            "application/vnd.github+json",
            None,
        )
        .await
        .map_err(|f| match f {
            crate::provider_rpc::Failure::NotFound => {
                let detail = repository_not_found(&format!("{owner}/{name}"));
                LookupFailure::provider(
                    detail.clone(),
                    Some(json!({"name": "GitHubRepositoryLookupError", "message": "",
                        "cause": {"name": "GitHubApiNotFoundError",
                            "message": "GitHub could not find the requested resource, or the credential cannot see it."}})),
                )
                .user_facing(detail)
            }
            f => github_failure(f, &host, REPOSITORY_NOT_FOUND),
        })?;
    let raw: Value = serde_json::from_slice(&body).unwrap_or(Value::Null);
    match (
        raw["full_name"].as_str(),
        raw["html_url"].as_str(),
        raw["ssh_url"].as_str(),
    ) {
        (Some(full), Some(url), Some(ssh)) => Ok(info("github", full, url, ssh)),
        _ => Err(LookupFailure::provider(
            "GitHub returned an invalid repository.",
            None,
        )),
    }
}

// --- GitLab -----------------------------------------------------------------

/// `parseRepositoryLookup`: the host, when a URL names one, and the project path.
fn gitlab_lookup(repository: &str) -> (Option<String>, String) {
    let trimmed = repository.trim();
    let lower = trimmed.to_ascii_lowercase();
    if !(lower.starts_with("http://") || lower.starts_with("https://")) {
        return (None, repository.to_owned());
    }
    let Ok(url) = url::Url::parse(trimmed) else {
        return (None, repository.to_owned());
    };
    let decoded = percent_encoding::percent_decode_str(url.path())
        .decode_utf8_lossy()
        .into_owned();
    let without_dash = match decoded.find("/-") {
        Some(at) if decoded[at + 2..].is_empty() || decoded[at + 2..].starts_with('/') => {
            decoded[..at].to_owned()
        }
        _ => decoded,
    };
    let path = without_dash.trim_matches('/');
    let path = path.strip_suffix(".git").unwrap_or(path);
    if path.is_empty() {
        return (None, repository.to_owned());
    }
    let host = match url.port() {
        Some(port) => format!("{}:{port}", url.host_str().unwrap_or_default()),
        None => url.host_str().unwrap_or_default().to_owned(),
    };
    (Some(host), path.to_owned())
}

fn gitlab_cli_failure(cwd: &str, repository: &str, output: &Output) -> LookupFailure {
    // `classifyGlabFailure`: glab prints a missing project as a bare 404.
    let stderr = output.stderr.to_lowercase();
    let (kind, detail) = if stderr.contains("glab auth login") || stderr.contains("invalid_grant") {
        ("GitLabCliAuthenticationError", "Authentication failed.")
    } else if stderr.contains("not found") || stderr.contains("404") {
        ("GitLabCliCommandError", "Merge request not found.")
    } else {
        (
            "GitLabCliCommandError",
            "Process exited with a non-zero status.",
        )
    };
    let exit = process_exit("GitLabCli.execute", "glab", cwd, output.code, detail);
    if kind == "GitLabCliAuthenticationError" {
        let detail = "GitLab CLI is not authenticated. Run `glab auth login` and retry.";
        return LookupFailure::provider(
            detail,
            Some(
                json!({"name": kind, "message": format!("GitLab CLI failed in execute: {detail}"), "cause": exit}),
            ),
        );
    }
    let failure = LookupFailure::provider(
        "GitLab CLI command failed.",
        Some(
            json!({"name": kind, "message": "GitLab CLI failed in execute: GitLab CLI command failed.", "cause": exit}),
        ),
    );
    if detail == "Merge request not found." {
        failure.user_facing(repository_not_found(repository))
    } else {
        failure
    }
}

async fn gitlab(repository: &str, cwd: &str) -> Result<Value, LookupFailure> {
    let (host, path) = gitlab_lookup(repository);
    let endpoint = format!("projects/{}", component(&path));
    let mut args = vec!["api"];
    if let Some(host) = &host {
        args.extend(["--hostname", host.as_str()]);
    }
    args.push(&endpoint);
    let output = run(cwd, "glab", &args, None, &[]).await.map_err(|_| {
        let detail = "GitLab CLI (`glab`) is required but not available on PATH.";
        LookupFailure::provider(detail, Some(json!({"name": "GitLabCliUnavailableError", "message": format!("GitLab CLI failed in execute: {detail}")})))
    })?;
    if output.code != 0 {
        return Err(gitlab_cli_failure(cwd, repository, &output));
    }
    let raw: Value = serde_json::from_str(output.stdout.trim()).unwrap_or(Value::Null);
    let text = |k: &str| raw[k].as_str().map(str::trim).filter(|v| !v.is_empty());
    match (
        text("path_with_namespace"),
        text("web_url"),
        text("http_url_to_repo"),
        text("ssh_url_to_repo"),
    ) {
        (Some(name), Some(web), Some(_), Some(ssh)) => Ok(info("gitlab", name, web, ssh)),
        _ => Err(LookupFailure::provider(
            "GitLab CLI returned invalid repository JSON.",
            Some(
                json!({"name": "GitLabRepositoryDecodeError", "message": "GitLab CLI failed in getRepositoryCloneUrls: GitLab CLI returned invalid repository JSON."}),
            ),
        )),
    }
}

// --- Bitbucket --------------------------------------------------------------

async fn bitbucket(repository: &str) -> Result<Value, LookupFailure> {
    let failed = |message: String| {
        LookupFailure::provider(
            "Failed to get repository clone URLs.",
            Some(json!({"name": "BitbucketResponseError", "message": message})),
        )
    };
    // `parseBitbucketRepositorySlug`: the last two segments.
    let normalized = repository.trim();
    let normalized = normalized.strip_suffix(".git").unwrap_or(normalized);
    let parts: Vec<&str> = normalized.split('/').filter(|p| !p.is_empty()).collect();
    if parts.len() < 2 {
        return Err(LookupFailure::provider(
            "Failed to get repository clone URLs.",
            Some(
                json!({"name": "BitbucketRepositoryLocatorError", "message": format!("Bitbucket repository {repository} is not a workspace/repository path.")}),
            ),
        ));
    }
    let (workspace, slug) = (parts[parts.len() - 2], parts[parts.len() - 1]);
    let base = std::env::var("T3CODE_BITBUCKET_API_BASE_URL")
        .unwrap_or_else(|_| "https://api.bitbucket.org/2.0".into());
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .map_err(|_| {
            failed(
                "Bitbucket API failed in getRepository: Could not initialize the request.".into(),
            )
        })?;
    let mut request = client
        .get(format!(
            "{}/repositories/{}/{}",
            base.trim_end_matches('/'),
            component(workspace),
            component(slug)
        ))
        .header("Accept", "application/json");
    match crate::bitbucket_credential::current().await {
        Ok(Some(credential)) => request = credential.apply(request),
        Ok(None) => {}
        Err(message) => {
            return Err(LookupFailure::provider(
                "Failed to get repository clone URLs.",
                Some(json!({"name": "SettingsSecretUnresolvedError", "message": message})),
            ))
        }
    }
    let response = request.send().await.map_err(|_| {
        failed("Bitbucket API failed in getRepository: Bitbucket could not be reached.".into())
    })?;
    let status = response.status().as_u16();
    if !(200..300).contains(&status) {
        let error = failed(format!(
            "Bitbucket API failed in getRepository: Bitbucket returned HTTP {status}."
        ));
        return Err(if status == 404 {
            error.user_facing(repository_not_found(&format!("{workspace}/{slug}")))
        } else {
            error
        });
    }
    let raw: Value = response.json().await.unwrap_or(Value::Null);
    let Some(full) = raw["full_name"].as_str() else {
        return Err(failed(
            "Bitbucket API failed in getRepository: Bitbucket returned an invalid response.".into(),
        ));
    };
    // `normalizeRepositoryCloneUrls`.
    let clone = |name: &str| {
        raw["links"]["clone"]
            .as_array()
            .and_then(|links| {
                links.iter().find(|l| {
                    l["name"]
                        .as_str()
                        .is_some_and(|n| n.eq_ignore_ascii_case(name))
                })
            })
            .and_then(|l| l["href"].as_str())
    };
    let html = raw["links"]["html"]["href"].as_str();
    let http = clone("https").or(html);
    let ssh = clone("ssh").or(http).unwrap_or(full);
    Ok(info("bitbucket", full, http.or(html).unwrap_or(full), ssh))
}

// --- Azure DevOps -----------------------------------------------------------

async fn azure(repository: &str, cwd: &str) -> Result<Value, LookupFailure> {
    // `parseRepositorySpecifier`: the project is the segment before the name.
    let parts: Vec<&str> = repository
        .split('/')
        .map(str::trim)
        .filter(|p| !p.is_empty())
        .collect();
    let name = parts.last().copied().unwrap_or(repository.trim());
    let project = (parts.len() > 1).then(|| parts[parts.len() - 2]);
    let mut args = vec!["repos", "show", "--detect", "true", "--repository", name];
    if let Some(project) = project {
        args.extend(["--project", project]);
    }
    args.extend(["--only-show-errors", "--output", "json"]);
    let output = run(cwd, "az", &args, None, &[]).await.map_err(|_| {
        let detail = "Azure DevOps CLI (`az`) is required but not available on PATH.";
        LookupFailure::provider(detail, Some(json!({"name": "AzureDevOpsCliUnavailableError", "message": format!("Azure DevOps CLI failed in execute: {detail}")})))
    })?;
    if output.code != 0 {
        return Err(LookupFailure::provider(
            "Azure DevOps CLI command failed.",
            Some(
                json!({"name": "AzureDevOpsCommandFailedError", "message": "Azure DevOps CLI failed in execute: Azure DevOps CLI command failed.",
                "cause": process_exit("AzureDevOpsCli.execute", "az", cwd, output.code, "Process exited with a non-zero status.")}),
            ),
        ));
    }
    let raw: Value = serde_json::from_str(output.stdout.trim()).unwrap_or(Value::Null);
    let text = |v: &Value| {
        v.as_str()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_owned)
    };
    match (
        text(&raw["name"]),
        text(&raw["webUrl"]),
        text(&raw["remoteUrl"]),
        text(&raw["sshUrl"]),
    ) {
        (Some(name), Some(_), Some(remote), Some(ssh)) => {
            let owner = match text(&raw["project"]["name"]) {
                Some(project) => format!("{project}/{name}"),
                None => name,
            };
            Ok(info("azure-devops", &owner, &remote, &ssh))
        }
        _ => Err(LookupFailure::provider(
            "Azure DevOps CLI returned invalid repository JSON.",
            Some(
                json!({"name": "AzureDevOpsRepositoryDecodeError", "message": "Azure DevOps CLI failed in getRepositoryCloneUrls: Azure DevOps CLI returned invalid repository JSON."}),
            ),
        )),
    }
}

// --- Forgejo / Gitea --------------------------------------------------------

fn forgejo_cli_failed(cwd: &str, code: i32) -> LookupFailure {
    LookupFailure::provider(
        "Forgejo CLI command failed.",
        Some(json!({"name": "ForgejoCliError", "message": "",
            "cause": process_exit("ForgejoCli.execute", "tea", cwd, code, "Process exited with a non-zero status.")})),
    )
}

async fn forgejo(repository: &str, cwd: &str) -> Result<Value, LookupFailure> {
    let unavailable = || {
        LookupFailure::provider(
            "Forgejo CLI (`tea` or `fj`) is required but not available on PATH.",
            Some(json!({"name": "ForgejoCliError", "message": ""})),
        )
    };
    let logins = run(
        cwd,
        "tea",
        &["login", "list", "--output", "json"],
        None,
        &[],
    )
    .await
    .map_err(|_| unavailable())?;
    if logins.code != 0 {
        return Err(forgejo_cli_failed(cwd, logins.code));
    }
    let logins: Vec<Value> = serde_json::from_str(logins.stdout.trim()).unwrap_or_default();
    // A URL names its host; otherwise the default login (or the only one).
    let (host, path) = match url::Url::parse(repository) {
        Ok(url) if matches!(url.scheme(), "http" | "https") => (
            url.host_str().map(str::to_lowercase),
            url.path()
                .trim_matches('/')
                .trim_end_matches(".git")
                .to_owned(),
        ),
        _ => (
            None,
            repository
                .trim_matches('/')
                .trim_end_matches(".git")
                .to_owned(),
        ),
    };
    let login = logins
        .iter()
        .find(|l| {
            host.as_deref().is_some_and(|h| {
                l["url"]
                    .as_str()
                    .and_then(|u| url::Url::parse(u).ok())
                    .and_then(|u| u.host_str().map(str::to_lowercase))
                    .as_deref()
                    == Some(h)
            })
        })
        .or_else(|| {
            host.is_none()
                .then(|| {
                    logins
                        .iter()
                        .find(|l| l["default"] == "true" || l["default"] == true)
                })
                .flatten()
        })
        .or_else(|| (host.is_none() && logins.len() == 1).then(|| &logins[0]))
        .ok_or_else(|| {
            LookupFailure::provider(
                "No Forgejo login matches this repository. Run `tea login add`.",
                Some(json!({"name": "ForgejoCliError", "message": ""})),
            )
        })?;
    let name = login["name"].as_str().unwrap_or_default();
    let base = login["url"]
        .as_str()
        .unwrap_or_default()
        .trim_end_matches('/');
    let endpoint = format!(
        "{base}/api/v1/repos/{}",
        path.split('/').map(component).collect::<Vec<_>>().join("/")
    );
    let output = run(
        cwd,
        "tea",
        &[
            "api",
            "--include",
            "--login",
            name,
            "--repo",
            &path,
            "--method",
            "GET",
            &endpoint,
        ],
        None,
        &[],
    )
    .await
    .map_err(|_| unavailable())?;
    if output.code != 0 {
        return Err(forgejo_cli_failed(cwd, output.code));
    }
    let status = output
        .stderr
        .lines()
        .find(|s| s.starts_with("HTTP/"))
        .and_then(|s| s.split_whitespace().nth(1))
        .and_then(|s| s.parse::<u16>().ok());
    match status {
        Some(200..=299) => {}
        Some(404) => {
            return Err(LookupFailure::provider(
                "Forgejo repository or pull request was not found.",
                Some(json!({"name": "ForgejoCliError", "message": ""})),
            )
            .user_facing(repository_not_found(&path)))
        }
        _ => {
            return Err(LookupFailure::provider(
                "Forgejo API request failed.",
                Some(json!({"name": "ForgejoCliError", "message": ""})),
            ))
        }
    }
    let raw: Value = serde_json::from_str(output.stdout.trim()).unwrap_or(Value::Null);
    match (
        raw["full_name"].as_str(),
        raw["clone_url"].as_str(),
        raw["ssh_url"].as_str(),
    ) {
        (Some(full), Some(url), Some(ssh)) => Ok(info("forgejo", full, url, ssh)),
        _ => Err(LookupFailure::provider(
            "Forgejo API returned an invalid response.",
            Some(json!({"name": "ForgejoCliError", "message": ""})),
        )),
    }
}

// --- GitCafe ----------------------------------------------------------------

async fn gitcafe(repository: &str, cwd: &str) -> Result<Value, LookupFailure> {
    // `parseGitCafeRemote` for a name: git.cafe unless a URL names another host.
    let (host, path) = match url::Url::parse(repository) {
        Ok(url) if matches!(url.scheme(), "http" | "https") => (
            url.host_str().unwrap_or("git.cafe").to_lowercase(),
            url.path()
                .trim_matches('/')
                .trim_end_matches(".git")
                .to_owned(),
        ),
        _ => (
            "git.cafe".to_owned(),
            repository
                .trim_matches('/')
                .trim_end_matches(".git")
                .to_owned(),
        ),
    };
    let api_failure = |detail: String, inner: Value| {
        LookupFailure::provider(
            detail.clone(),
            Some(
                json!({"name": "GitCafeApiError", "message": format!("GitCafe getRepositoryCloneUrls failed on {host}: {detail}"), "cause": inner}),
            ),
        )
    };
    let token = match std::env::var("CAFE_TOKEN")
        .ok()
        .filter(|t| !t.trim().is_empty())
    {
        Some(token) => token,
        None => {
            let helper_host = format!("https://{host}/api");
            let stdin = format!("protocol=https\nhost={host}\n\n");
            let not_signed_in = format!("No GitCafe credential for {host}: run `cafe auth login --host https://{host}/api`.");
            match run(
                cwd,
                "cafe",
                &["auth", "http", "credential", "--host", &helper_host, "get"],
                Some(&stdin),
                &[("CAFE_NO_UPDATE_CHECK", "1")],
            )
            .await
            {
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                    let detail = format!("No GitCafe credential for {host}: set CAFE_TOKEN, or install the GitCafe CLI and run `cafe auth login`.");
                    return Err(api_failure(
                        detail.clone(),
                        json!({"name": "GitCafeCliMissingError", "message": detail}),
                    ));
                }
                Err(_) => {
                    return Err(api_failure(
                        not_signed_in.clone(),
                        json!({"name": "GitCafeNotSignedInError", "message": not_signed_in}),
                    ))
                }
                Ok(output) => {
                    let password = output
                        .stdout
                        .lines()
                        .find_map(|l| l.strip_prefix("password="))
                        .map(str::to_owned)
                        .filter(|p| !p.is_empty() && output.code == 0);
                    match password {
                        Some(token) => token,
                        None => {
                            return Err(api_failure(
                                not_signed_in.clone(),
                                json!({"name": "GitCafeNotSignedInError", "message": not_signed_in}),
                            ))
                        }
                    }
                }
            }
        }
    };
    let response = reqwest::Client::new()
        .get(format!("https://{host}/api/repos/{path}"))
        .bearer_auth(token)
        .timeout(Duration::from_secs(30))
        .send()
        .await
        .map_err(|_| {
            api_failure(
                "GitCafe could not be reached.".into(),
                json!({"name": "GitCafeRequestError", "message": "GitCafe could not be reached."}),
            )
        })?;
    if !response.status().is_success() {
        let status = response.status().as_u16();
        let detail = if status == 404 {
            "GitCafe repository not found.".to_owned()
        } else {
            format!("GitCafe returned HTTP {status}.")
        };
        let failure = api_failure(
            detail.clone(),
            json!({"name": "GitCafeResponseError", "message": detail}),
        );
        return Err(if status == 404 {
            failure.user_facing(repository_not_found(&path))
        } else {
            failure
        });
    }
    // `cloneUrls(target)`: GitCafe's URLs follow from the host and the path.
    Ok(info(
        "gitcafe",
        &path,
        &format!("https://{host}/{path}"),
        &format!("ssh@{host}:{path}.git"),
    ))
}

// --- Cursor Origin ----------------------------------------------------------

async fn origin(repository: &str, cwd: &str) -> Result<Value, LookupFailure> {
    let failure = |stage: &str, detail: &str| {
        LookupFailure::provider(
            detail,
            Some(
                json!({"name": "OriginCliError", "message": format!("Cursor Origin failed in {stage}: {detail}")}),
            ),
        )
    };
    const NOT_SIGNED_IN: &str =
        "Cursor Origin CLI is not signed in. Run `origin auth login` and retry.";
    let token = match run(cwd, "origin", &["auth", "token"], None, &[]).await {
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            return Err(failure(
                "authToken",
                "Cursor Origin CLI (`origin`) is required but not available on PATH.",
            ))
        }
        Err(_) => return Err(failure(
            "authToken",
            "Cursor Origin CLI could not hand over a session token. Check `origin auth status`.",
        )),
        Ok(output) if output.code != 0 => return Err(failure(
            "authToken",
            "Cursor Origin CLI could not hand over a session token. Check `origin auth status`.",
        )),
        Ok(output) => output.stdout.trim().to_owned(),
    };
    if token.is_empty() {
        return Err(failure("authToken", NOT_SIGNED_IN));
    }
    let path = format!(
        "/v1/origin/repos/{}",
        repository
            .split('/')
            .map(component)
            .collect::<Vec<_>>()
            .join("/")
    );
    let response = reqwest::Client::new()
        .get(format!("https://api.cursor.com{path}"))
        .bearer_auth(&token)
        .header("Accept", "application/json")
        .timeout(Duration::from_secs(30))
        .send()
        .await
        .map_err(|_| failure("getRepositoryCloneUrls", "Could not reach Cursor Origin."))?;
    match response.status().as_u16() {
        200..=299 => {}
        401 => return Err(failure("getRepositoryCloneUrls", NOT_SIGNED_IN)),
        404 => {
            return Err(failure(
                "getRepositoryCloneUrls",
                "Cursor Origin repository not found.",
            ))
        }
        status => {
            return Err(failure(
                "getRepositoryCloneUrls",
                &format!("Cursor Origin returned HTTP {status}."),
            ))
        }
    }
    let raw: Value = response.json().await.unwrap_or(Value::Null);
    // `cloneUrls`: HTTPS stands in for SSH, which Origin does not document.
    let name_with_owner = raw["fullName"]
        .as_str()
        .map(str::to_owned)
        .or_else(|| {
            raw["owner"]["slug"]
                .as_str()
                .zip(raw["name"].as_str())
                .map(|(o, n)| format!("{o}/{n}"))
        })
        .unwrap_or_else(|| repository.to_owned());
    let url = raw["cloneUrl"]
        .as_str()
        .map(str::to_owned)
        .unwrap_or_else(|| format!("https://origin.cursor.com/{name_with_owner}.git"));
    Ok(info("cursor-origin", &name_with_owner, &url, &url))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn github_selectors_name_a_host_owner_and_repository() {
        let s = |v: &str| github_selector(v, "github.com");
        assert_eq!(
            s("org/repo"),
            Some(("github.com".into(), "org".into(), "repo".into()))
        );
        assert_eq!(
            s("https://github.com/org/repo.git/"),
            Some(("github.com".into(), "org".into(), "repo".into()))
        );
        assert_eq!(
            s("github.example.com/org/repo"),
            Some(("github.example.com".into(), "org".into(), "repo".into()))
        );
        assert_eq!(s("a/b/c/d"), None);
        assert_eq!(s("solo"), None);
    }

    #[test]
    fn gitlab_lookups_take_the_host_from_a_url() {
        assert_eq!(gitlab_lookup("org/repo"), (None, "org/repo".into()));
        assert_eq!(
            gitlab_lookup("https://gitlab.example.com/group/sub/repo/-/tree/main"),
            (Some("gitlab.example.com".into()), "group/sub/repo".into())
        );
        assert_eq!(
            gitlab_lookup("https://gitlab.com/org/repo.git"),
            (Some("gitlab.com".into()), "org/repo".into())
        );
    }
}
