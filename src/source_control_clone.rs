//! Cloning a repository into a new folder (`SourceControlRepositoryService`'s
//! `cloneRepository` in the reference).
//!
//! The destination is trimmed, `~` expanded and resolved; it must not exist
//! yet or be an empty folder, and missing parents are made. The remote is the
//! URL or path given, or what the named host reports for `provider` and
//! `repository`. Git clones with piped progress and no credential prompts;
//! progress lines go to the caller, and the last other lines of standard error
//! (credentials redacted) explain a failure. An SSH clone the server refuses
//! is retried over HTTPS unless SSH was asked for, and a clone that runs past
//! its deadline is stopped.
use regex::Regex;
use serde_json::{json, Value};
use std::path::{Path, PathBuf};
use std::sync::LazyLock;
use std::time::Duration;
use tokio::io::AsyncReadExt;

/// Older and mobile synchronous RPCs need a deadline; tracked clones use progress and Cancel.
pub(crate) const CLONE_TIMEOUT: Duration = Duration::from_secs(120);
const OPERATION: &str = "SourceControlRepositoryService.cloneRepository";

/// `SourceControlCloneRepositoryInput`.
#[derive(Clone, Debug, Default)]
pub(crate) struct CloneInput {
    pub provider: Option<String>,
    pub repository: Option<String>,
    pub remote_url: Option<String>,
    pub destination_path: String,
    pub protocol: Option<String>,
}

impl CloneInput {
    /// The RPC payload, or the defect its schema reports.
    pub(crate) fn from_json(value: &Value) -> Result<Self, String> {
        let text = |key: &str| -> Result<Option<String>, String> {
            match value.get(key) {
                None => Ok(None),
                Some(Value::String(s)) if !s.trim().is_empty() => Ok(Some(s.trim().to_owned())),
                Some(Value::String(_)) => {
                    Err(format!("Expected a non-blank string\n  at [\"{key}\"]"))
                }
                Some(_) => Err(format!(
                    "Expected string, got {value}\n  at [\"{key}\"]",
                    value = value[key]
                )),
            }
        };
        let protocol = text("protocol")?;
        if let Some(p) = &protocol {
            if !matches!(p.as_str(), "auto" | "ssh" | "https") {
                return Err(format!(
                    "Expected \"auto\" | \"ssh\" | \"https\", got \"{p}\"\n  at [\"protocol\"]"
                ));
            }
        }
        Ok(Self {
            provider: text("provider")?,
            repository: text("repository")?,
            remote_url: text("remoteUrl")?,
            destination_path: text("destinationPath")?
                .ok_or_else(|| "Missing key\n  at [\"destinationPath\"]".to_owned())?,
            protocol,
        })
    }
}

/// A stage of a clone, as git's progress reports it (`parseGitCloneProgressLine`).
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct Progress {
    pub stage: &'static str,
    pub percent: Option<u64>,
    pub detail: Option<String>,
}

static PERCENT: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?s):\s+(\d+)%\s+\((\d+)/(\d+)\)(.*)$").unwrap());

/// One line of git's progress, or `None` for any other line. Each bare-CR
/// redraw is its own line.
pub(crate) fn parse_progress(line: &str) -> Option<Progress> {
    const STAGES: [(&str, &str); 7] = [
        ("remote: Enumerating objects", "counting"),
        ("remote: Counting objects", "counting"),
        ("remote: Compressing objects", "counting"),
        ("Receiving objects", "receiving"),
        ("Resolving deltas", "resolving"),
        ("Updating files", "checkout"),
        ("Checking out files", "checkout"),
    ];
    let trimmed = line.trim();
    let stage = STAGES
        .iter()
        .find(|(prefix, _)| trimmed.starts_with(prefix))?
        .1;
    let Some(found) = PERCENT.captures(trimmed) else {
        return Some(Progress {
            stage,
            percent: None,
            detail: None,
        });
    };
    let mut tail = found.get(4).map_or("", |m| m.as_str()).trim_end();
    if let Some(rest) = tail.strip_suffix(", done.") {
        tail = rest.trim_end();
    }
    if !tail.is_empty() && !tail.starts_with(',') {
        return Some(Progress {
            stage,
            percent: None,
            detail: None,
        });
    }
    let percent = found[1].parse::<u64>().ok().map(|p| p.min(100));
    let raw = tail.get(1..).unwrap_or("").trim();
    let detail = (!raw.is_empty() && raw != "done.").then(|| raw.to_owned());
    Some(Progress {
        stage,
        percent,
        detail,
    })
}

/// Why a git command failed, by fixed patterns that never quote what matched
/// (`GIT_FAILURE_REASON_PATTERNS`); credentials can appear in its output.
pub(crate) fn git_failure_reason(stderr: &str) -> Option<&'static str> {
    static PATTERNS: LazyLock<Vec<(Regex, &'static str)>> = LazyLock::new(|| {
        [
            (r"(?i)would clobber existing tag", "tag_would_be_clobbered"),
            (r"(?i)is already (?:used by worktree at|checked out at)", "branch_checked_out_in_worktree"),
            (r"(?i)a branch named .+ already exists", "branch_already_exists"),
            (r"(?i)'refs/[^'\n]+' exists; cannot create 'refs/", "ref_name_conflict"),
            (r"(?i)could not lock config file .+: File exists", "config_locked"),
            (
                r"(?i)(?:authentication failed|could not read Username|could not read Password|permission denied \([a-z-]+(?:,[a-z-]+)*\)|permission denied, please try again)",
                "authentication_failed",
            ),
            (r"(?i)host key verification failed", "host_key_unverified"),
            (
                r"(?i)(?:could not read from remote repository|does not appear to be a git repository|repository .+ not found)",
                "remote_unreachable",
            ),
            (r"(?i)fatal: '[^']+' already exists|destination path .+ already exists", "path_already_exists"),
            (
                r"(?im)^fatal: (?:invalid reference: |not a valid object name|needed a single revision|bad revision |'.+' is not a commit and a branch '.+' cannot be created from it)",
                "ref_not_found",
            ),
            (r"(?im)^[ \t]*![ \t]+\[remote rejected\]", "push_rejected_by_remote"),
            (
                r"(?ims)^[ \t]*![ \t]+\[rejected\][^\n]*\((?:non-fast-forward|fetch first)\).*^error: failed to push some refs",
                "push_rejected_non_fast_forward",
            ),
        ]
        .into_iter()
        .map(|(pattern, reason)| (Regex::new(pattern).expect("git failure pattern"), reason))
        .collect()
    });
    PATTERNS
        .iter()
        .find(|(pattern, _)| pattern.is_match(stderr))
        .map(|(_, reason)| *reason)
}

/// A clone URL without its credentials, for clients (`redactRemoteUrl`): git
/// still gets them. A clone URL has no legitimate query, so one is a token.
pub(crate) fn redact_remote_url(remote: &str) -> String {
    let Ok(mut url) = url::Url::parse(remote) else {
        return remote.to_owned();
    };
    if url.username().is_empty() && url.password().is_none() && url.query().is_none() {
        return remote.to_owned();
    }
    let _ = url.set_username("");
    let _ = url.set_password(None);
    url.set_query(None);
    url.to_string()
}

/// Text with the credentials of any URL in it removed (`redactUrlCredentials`).
fn redact_url_credentials(text: &str) -> String {
    static QUOTED_QUERY: LazyLock<Regex> = LazyLock::new(|| {
        Regex::new(r#"(?i)(['"])([a-z][a-z0-9+.-]*://[^\s?]*)\?.*(['"])(\s|:|$)"#).unwrap()
    });
    static QUERY: LazyLock<Regex> =
        LazyLock::new(|| Regex::new(r"(?i)\b([a-z][a-z0-9+.-]*://[^\s?]*)\?[^\s]*").unwrap());
    static USERINFO: LazyLock<Regex> =
        LazyLock::new(|| Regex::new(r"(?i)\b([a-z][a-z0-9+.-]*://)[^\s/?#]+@").unwrap());
    // JavaScript's `(['"])...\1(?=[\s:]|$)`: the closing quote matches the opening one.
    let quoted = QUOTED_QUERY.replace_all(text, |c: &regex::Captures| {
        if c[1] == c[3] {
            format!("{}{}{}{}", &c[1], &c[2], &c[1], &c[4])
        } else {
            c[0].to_owned()
        }
    });
    let queries = QUERY.replace_all(&quoted, "$1");
    USERINFO.replace_all(&queries, "$1").into_owned()
}

/// What a clone URL's transport is (`cloneUrlProtocol`).
struct ParsedUrl {
    protocol: &'static str,
    host: String,
    path: String,
    custom_port: bool,
    scp: bool,
}

fn parse_clone_url(url: &str) -> Option<ParsedUrl> {
    static SCP: LazyLock<Regex> = LazyLock::new(|| {
        Regex::new(r"^(?:[^@/\s]+@)?(\[[^\]@/\s]+\]|[^:/\s\[\]]+):(.+)$").unwrap()
    });
    static SSH: LazyLock<Regex> = LazyLock::new(|| {
        Regex::new(r"^ssh://(?:[^/\s]*@)?(\[[^\]@/\s]+\]|[^@:/\s]+)(:\d+)?/(.+)$").unwrap()
    });
    static HTTPS: LazyLock<Regex> = LazyLock::new(|| {
        Regex::new(r"^https?://(?:[^/\s]*@)?(\[[^\]@/\s]+\]|[^@:/\s]+)(:\d+)?/(.+)$").unwrap()
    });
    let trimmed = url.trim();
    for (pattern, protocol) in [(&*SSH, "ssh"), (&*HTTPS, "https")] {
        if let Some(c) = pattern.captures(trimmed) {
            return Some(ParsedUrl {
                protocol,
                host: c[1].to_owned(),
                path: c[3].to_owned(),
                custom_port: c.get(2).is_some(),
                scp: false,
            });
        }
    }
    // `:(?!/)`: an SCP-like path never starts with a slash after the colon.
    let c = SCP.captures(trimmed)?;
    if c[2].starts_with('/') {
        return None;
    }
    Some(ParsedUrl {
        protocol: "ssh",
        host: c[1].to_owned(),
        path: c[2].to_owned(),
        custom_port: false,
        scp: true,
    })
}

/// The clone URL rewritten to the other transport (`cloneUrlForProtocol`),
/// or `None` where that cannot be done safely, such as a custom port.
pub(crate) fn clone_url_for_protocol(url: &str, protocol: &str) -> Option<String> {
    use crate::thread_projection::uri_component;
    let parsed = parse_clone_url(url)?;
    if parsed.protocol == protocol {
        return Some(url.trim().to_owned());
    }
    if parsed.custom_port {
        return None;
    }
    let host = parsed.host.to_lowercase();
    if parsed.protocol == "ssh" && host == "ssh.dev.azure.com" {
        let parts: Vec<&str> = parsed.path.strip_prefix("v3/")?.split('/').collect();
        return (parts.len() == 3
            && parts
                .iter()
                .all(|p| !p.is_empty() && !p.contains(['?', '#'])))
        .then(|| {
            format!(
                "https://dev.azure.com/{}/{}/_git/{}",
                parts[0], parts[1], parts[2]
            )
        });
    }
    if parsed.protocol == "https" && host == "dev.azure.com" {
        let parts: Vec<&str> = parsed.path.split('/').collect();
        return (parts.len() == 4
            && parts[2] == "_git"
            && parts
                .iter()
                .all(|p| !p.is_empty() && !p.contains(['?', '#'])))
        .then(|| {
            format!(
                "git@ssh.dev.azure.com:v3/{}/{}/{}",
                parts[0], parts[1], parts[3]
            )
        });
    }
    let bitbucket = matches!(
        host.as_str(),
        "bitbucket.org" | "ssh.bitbucket.org" | "altssh.bitbucket.org"
    );
    let host = if bitbucket {
        if protocol == "ssh" {
            "ssh.bitbucket.org".to_owned()
        } else {
            "bitbucket.org".to_owned()
        }
    } else {
        parsed.host
    };
    if protocol == "ssh" {
        let path = percent_encoding::percent_decode_str(&parsed.path)
            .decode_utf8()
            .ok()?;
        return Some(format!("git@{host}:{path}"));
    }
    let path = if parsed.scp {
        parsed
            .path
            .split('/')
            .map(uri_component)
            .collect::<Vec<_>>()
            .join("/")
    } else {
        parsed.path
    };
    Some(format!("https://{host}/{path}"))
}

/// `SourceControlRepositoryError`, as the RPC encodes it.
pub(crate) fn repository_error(
    provider: &str,
    operation: &str,
    detail: &str,
    cause: Option<Value>,
) -> Value {
    let mut error = json!({"_tag": "SourceControlRepositoryError", "provider": provider, "operation": operation, "detail": detail});
    if let Some(cause) = cause {
        error["cause"] = cause;
    }
    error
}

/// `Duration.format`: the largest units first, as "2m" or "1m 30s".
fn format_duration(duration: Duration) -> String {
    let mut ms = duration.as_millis() as u64;
    let mut parts = Vec::new();
    for (unit, size) in [
        ("d", 86_400_000),
        ("h", 3_600_000),
        ("m", 60_000),
        ("s", 1_000),
        ("ms", 1),
    ] {
        if ms >= size {
            parts.push(format!("{}{unit}", ms / size));
            ms %= size;
        }
    }
    if parts.is_empty() {
        "0".to_owned()
    } else {
        parts.join(" ")
    }
}

/// Where the clone goes, normalized (`normalizeDestinationPath`).
pub(crate) fn normalize_destination(raw: &str, home: &Path) -> Result<PathBuf, Value> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err(repository_error(
            "unknown",
            "cloneRepository",
            "Choose a destination path before cloning.",
            None,
        ));
    }
    let expanded = if trimmed == "~" {
        home.to_path_buf()
    } else if let Some(rest) = trimmed
        .strip_prefix("~/")
        .or_else(|| trimmed.strip_prefix("~\\"))
    {
        home.join(rest)
    } else {
        PathBuf::from(trimmed)
    };
    let absolute = if expanded.is_absolute() {
        expanded
    } else {
        std::env::current_dir().unwrap_or_default().join(expanded)
    };
    // `path.resolve`: `.` and `..` are folded without touching the disk.
    let mut resolved = PathBuf::new();
    for part in absolute.components() {
        match part {
            std::path::Component::ParentDir => {
                resolved.pop();
            }
            std::path::Component::CurDir => {}
            other => resolved.push(other),
        }
    }
    Ok(resolved)
}

/// A destination that may receive a clone (`prepareDestination`).
async fn prepare_destination(raw: &str, home: &Path) -> Result<PathBuf, Value> {
    let destination = normalize_destination(raw, home)?;
    let refused = |detail: &str, cause: Option<Value>| {
        repository_error("unknown", "cloneRepository", detail, cause)
    };
    match tokio::fs::metadata(&destination).await {
        Ok(meta) if !meta.is_dir() => {
            let shown = destination.display();
            return Err(refused(
                "Destination path already exists and is not a directory.",
                Some(
                    json!({"name": "PlatformError", "message": format!("BadResource: FileSystem.readDirectory ({shown})"),
                    "cause": {"name": "Error", "message": format!("ENOTDIR: not a directory, scandir '{shown}'")}}),
                ),
            ));
        }
        Ok(_) => {
            let mut entries = tokio::fs::read_dir(&destination).await.map_err(|e| {
                refused(
                    "Destination path already exists and is not a directory.",
                    Some(json!({"name": "PlatformError", "message": e.to_string()})),
                )
            })?;
            if entries.next_entry().await.ok().flatten().is_some() {
                return Err(refused(
                    "Destination path already exists and is not empty.",
                    None,
                ));
            }
        }
        Err(_) => {
            if let Some(parent) = destination.parent().filter(|p| !p.exists()) {
                tokio::fs::create_dir_all(parent).await.map_err(|e| {
                    repository_error(
                        "unknown",
                        "cloneRepository",
                        "The source control operation could not be completed.",
                        Some(json!({"name": "PlatformError", "message": e.to_string()})),
                    )
                })?;
            }
        }
    }
    Ok(destination)
}

/// How one run of git ended badly.
struct CloneFailure {
    error: Value,
    reason: Option<&'static str>,
}

/// Runs `git clone` for `url` into `destination` (`runClone`).
async fn run_clone(
    provider: &str,
    destination: &Path,
    url: &str,
    on_progress: &(dyn Fn(Progress) + Send + Sync),
    timeout: Option<Duration>,
    cancel: &mut Option<tokio::sync::oneshot::Receiver<()>>,
) -> Result<(), CloneFailure> {
    static PATHLESS_QUERY: LazyLock<Regex> =
        LazyLock::new(|| Regex::new(r"(?i)^([a-z][a-z0-9+.-]*://[^/?#]+)\?").unwrap());
    // Git misreads @ in a pathless URL's query as userinfo and echoes its suffix as the host.
    let url = PATHLESS_QUERY.replace(url, "$1/?").into_owned();
    let parent = destination.parent().unwrap_or(Path::new("/")).to_path_buf();
    let name = destination
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_default();
    let cwd = parent.display().to_string();
    let command_error = |reason: Option<&'static str>| {
        let suffix = reason.map(|r| format!(" ({r})")).unwrap_or_default();
        json!({"name": "GitCommandError",
            "message": format!("Git command failed in {OPERATION} ({cwd}): Git command exited with a non-zero status.{suffix}")})
    };
    let mut child = match crate::login_shell::tokio_command("git")
        .args(["clone", "--progress", "--", url.as_str(), name.as_str()])
        .current_dir(&parent)
        // Piped progress at once; no credential prompt that would hang without a terminal.
        .env("GIT_PROGRESS_DELAY", "0")
        .env("GIT_TERMINAL_PROMPT", "0")
        .env("LANGUAGE", "C")
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::piped())
        .kill_on_drop(true)
        .spawn()
    {
        Ok(child) => child,
        Err(error) => {
            return Err(CloneFailure {
                error: repository_error(
                    provider,
                    "cloneRepository",
                    "The repository could not be cloned.",
                    Some(
                        json!({"name": "GitCommandError", "message": format!("Failed to run git in {OPERATION} ({cwd}): {error}")}),
                    ),
                ),
                reason: None,
            })
        }
    };
    let mut stderr = child.stderr.take().expect("stderr is piped");
    // Git mixes progress and diagnostics on standard error: progress goes to
    // the caller, the last other lines explain a failure.
    let read = async {
        let mut all = String::new();
        let mut tail: Vec<String> = Vec::new();
        let mut pending = Vec::new();
        let mut buffer = [0u8; 8192];
        let line = |raw: &[u8], all: &mut String, tail: &mut Vec<String>| {
            let text = String::from_utf8_lossy(raw).into_owned();
            all.push_str(&text);
            all.push('\n');
            if let Some(progress) = parse_progress(&text) {
                on_progress(progress);
                return;
            }
            let trimmed = text.trim();
            if trimmed.is_empty() || trimmed.starts_with("Cloning into") {
                return;
            }
            tail.push(redact_url_credentials(trimmed));
            if tail.len() > 4 {
                tail.remove(0);
            }
        };
        loop {
            let n = stderr.read(&mut buffer).await.unwrap_or(0);
            if n == 0 {
                break;
            }
            for byte in &buffer[..n] {
                if *byte == b'\n' || *byte == b'\r' {
                    line(&pending, &mut all, &mut tail);
                    pending.clear();
                } else {
                    pending.push(*byte);
                }
            }
        }
        if !pending.is_empty() {
            line(&pending, &mut all, &mut tail);
        }
        let status = child.wait().await;
        (status, all, tail)
    };
    let deadline = async {
        match timeout {
            Some(limit) => tokio::time::sleep(limit).await,
            None => std::future::pending::<()>().await,
        }
    };
    let cancelled = async {
        match cancel.as_mut() {
            Some(receiver) => {
                let _ = receiver.await;
            }
            None => std::future::pending::<()>().await,
        }
    };
    tokio::select! {
        (status, all, tail) = read => {
            if status.as_ref().is_ok_and(|s| s.success()) {
                return Ok(());
            }
            let reason = git_failure_reason(&all);
            let detail = if tail.is_empty() { "The repository could not be cloned.".to_owned() } else { tail.join(" ") };
            Err(CloneFailure { error: repository_error(provider, "cloneRepository", &detail, Some(command_error(reason))), reason })
        }
        _ = deadline => {
            // Dropping the read future drops the child, which kills git.
            let limit = timeout.unwrap_or_default();
            Err(CloneFailure {
                error: repository_error(provider, "cloneRepository", &format!("The clone timed out after {}.", format_duration(limit)), None),
                reason: None,
            })
        }
        _ = cancelled => Err(CloneFailure { error: json!({"_tag": "Cancelled"}), reason: None }),
    }
}

/// What a clone produced (`SourceControlCloneRepositoryResult`).
pub(crate) struct Cloned {
    pub cwd: String,
    pub remote_url: String,
    pub repository: Value,
}

/// How a clone may be followed and stopped.
pub(crate) struct CloneOptions<'a> {
    pub on_progress: &'a (dyn Fn(Progress) + Send + Sync),
    /// `None` runs without a deadline.
    pub timeout: Option<Duration>,
    pub cancel: Option<tokio::sync::oneshot::Receiver<()>>,
}

/// A clone prepared for running: where, from what, and as which repository.
pub(crate) struct Prepared {
    pub destination: PathBuf,
    pub remote_url: String,
    pub clone_url: String,
    pub repository: Value,
}

/// `prepareClone`: the destination, then the remote. `lookup` finds the clone
/// URLs a host reports for `provider` and `repository`.
pub(crate) async fn prepare_clone(
    input: &CloneInput,
    home: &Path,
    lookup: impl AsyncFnOnce(&str) -> Result<Value, crate::source_control_lookup::LookupFailure>,
) -> Result<Prepared, Value> {
    let destination = prepare_destination(&input.destination_path, home).await?;
    let provider = input.provider.clone().unwrap_or_else(|| "unknown".into());
    let mut repository = Value::Null;
    let mut remote = input.remote_url.clone();
    if input.provider.is_some() && input.repository.is_some() {
        // Inside a clone the host is asked from the destination's parent.
        let cwd = destination
            .parent()
            .unwrap_or(&destination)
            .display()
            .to_string();
        let found = lookup(&cwd)
            .await
            .map_err(|failure| failure.into_error(&provider, "cloneRepository"))?;
        // `selectRemoteUrl`: SSH unless HTTPS is asked for.
        remote = found[if input.protocol.as_deref() == Some("https") {
            "url"
        } else {
            "sshUrl"
        }]
        .as_str()
        .map(str::to_owned);
        repository = found;
    }
    let Some(clone_url) = remote else {
        return Err(repository_error(
            &provider,
            "cloneRepository",
            "Enter a repository path or clone URL before cloning.",
            None,
        ));
    };
    Ok(Prepared {
        destination,
        remote_url: redact_remote_url(&clone_url),
        clone_url,
        repository,
    })
}

/// `cloneRepository` for a prepared clone, with the SSH-to-HTTPS fallback.
/// `Err` is the RPC failure; a cancelled clone is `{"_tag":"Cancelled"}`.
pub(crate) async fn run_prepared(
    input: &CloneInput,
    prepared: &Prepared,
    mut options: CloneOptions<'_>,
) -> Result<Cloned, Value> {
    let provider = input.provider.clone().unwrap_or_else(|| "unknown".into());
    // `httpsFallbackUrl`: never for SSH asked for, nor for a URL that cannot be rewritten.
    let https = (input.protocol.as_deref() != Some("ssh")
        && parse_clone_url(&prepared.clone_url).is_some_and(|p| p.protocol == "ssh"))
    .then(|| {
        if prepared.repository["sshUrl"].as_str() == Some(prepared.clone_url.as_str()) {
            prepared.repository["url"].as_str().map(str::to_owned)
        } else {
            clone_url_for_protocol(&prepared.clone_url, "https")
        }
    })
    .flatten();
    let first = run_clone(
        &provider,
        &prepared.destination,
        &prepared.clone_url,
        options.on_progress,
        options.timeout,
        &mut options.cancel,
    )
    .await;
    let remote_url = match first {
        Ok(()) => prepared.remote_url.clone(),
        Err(ssh) => {
            let rejected = matches!(
                ssh.reason,
                Some("authentication_failed" | "host_key_unverified")
            );
            let Some(https) = https.filter(|_| rejected) else {
                return Err(ssh.error);
            };
            match run_clone(
                &provider,
                &prepared.destination,
                &https,
                options.on_progress,
                options.timeout,
                &mut options.cancel,
            )
            .await
            {
                Ok(()) => redact_remote_url(&https),
                Err(second) if second.error["_tag"] == "Cancelled" => return Err(second.error),
                Err(second) => {
                    let detail = format!(
                        "{} Cloning over HTTPS instead also failed: {}",
                        ssh.error["detail"].as_str().unwrap_or_default(),
                        second.error["detail"].as_str().unwrap_or_default()
                    );
                    return Err(repository_error(
                        &provider,
                        "cloneRepository",
                        &detail,
                        second.error.get("cause").cloned(),
                    ));
                }
            }
        }
    };
    Ok(Cloned {
        cwd: prepared.destination.display().to_string(),
        remote_url,
        repository: prepared.repository.clone(),
    })
}

/// The clone URLs a host reports for `repository` (`lookupRepository`), looked
/// up from the server's folder, or the `SourceControlRepositoryError` an RPC
/// answers with.
pub(crate) async fn lookup_repository(provider: &str, repository: &str) -> Result<Value, Value> {
    let cwd = std::env::current_dir()
        .unwrap_or_default()
        .display()
        .to_string();
    crate::source_control_lookup::lookup(provider, repository, &cwd)
        .await
        .map_err(|failure| failure.into_error(provider, "lookupRepository"))
}

/// Why `clone_json` did not clone.
pub(crate) enum CloneRefusal {
    /// The input does not decode (`Reply::defect`).
    Defect(String),
    /// `SourceControlRepositoryError`.
    Failure(Value),
}

/// `sourceControl.cloneRepository` for an RPC or tool payload: the
/// `SourceControlCloneRepositoryResult`, run with `timeout` (`None` for none).
pub(crate) async fn clone_json(
    params: &Value,
    timeout: Option<Duration>,
) -> Result<Value, CloneRefusal> {
    let input = CloneInput::from_json(params).map_err(CloneRefusal::Defect)?;
    let home = std::env::var_os("HOME")
        .map(PathBuf::from)
        .unwrap_or_default();
    let provider = input.provider.clone().unwrap_or_default();
    let repository = input.repository.clone().unwrap_or_default();
    let lookup =
        async |cwd: &str| crate::source_control_lookup::lookup(&provider, &repository, cwd).await;
    let prepared = prepare_clone(&input, &home, lookup)
        .await
        .map_err(CloneRefusal::Failure)?;
    let options = CloneOptions {
        on_progress: &|_| {},
        timeout,
        cancel: None,
    };
    let cloned = run_prepared(&input, &prepared, options)
        .await
        .map_err(CloneRefusal::Failure)?;
    Ok(json!({"cwd": cloned.cwd, "remoteUrl": cloned.remote_url, "repository": cloned.repository}))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn progress_lines_give_their_stage_and_percent() {
        assert_eq!(
            parse_progress("Receiving objects:  45% (9/20), 1.20 MiB | 2.00 MiB/s"),
            Some(Progress {
                stage: "receiving",
                percent: Some(45),
                detail: Some("1.20 MiB | 2.00 MiB/s".into())
            })
        );
        assert_eq!(
            parse_progress("Resolving deltas: 100% (3/3), done."),
            Some(Progress {
                stage: "resolving",
                percent: Some(100),
                detail: None
            })
        );
        assert_eq!(
            parse_progress("remote: Enumerating objects: 5, done."),
            Some(Progress {
                stage: "counting",
                percent: None,
                detail: None
            })
        );
        assert_eq!(parse_progress("fatal: repository not found"), None);
    }

    #[test]
    fn failures_are_classified_without_quoting_them() {
        assert_eq!(git_failure_reason("git@h: Permission denied (publickey).\nfatal: Could not read from remote repository."), Some("authentication_failed"));
        assert_eq!(
            git_failure_reason("Host key verification failed."),
            Some("host_key_unverified")
        );
        assert_eq!(
            git_failure_reason("fatal: '/x' does not appear to be a git repository"),
            Some("remote_unreachable")
        );
        assert_eq!(
            git_failure_reason("fatal: unable to access 'http://h/': Connection refused"),
            None
        );
    }

    #[test]
    fn credentials_never_reach_clients() {
        assert_eq!(
            redact_remote_url("https://user:pw@example.com/a/b.git?token=x"),
            "https://example.com/a/b.git"
        );
        assert_eq!(
            redact_remote_url("git@example.com:a/b.git"),
            "git@example.com:a/b.git"
        );
        assert_eq!(
            redact_url_credentials("fatal: unable to access 'https://u:p@h/r.git?t=1': refused"),
            "fatal: unable to access 'https://h/r.git': refused"
        );
    }

    #[test]
    fn clone_urls_switch_transport_where_that_is_safe() {
        assert_eq!(
            clone_url_for_protocol("git@github.com:o/r.git", "https").as_deref(),
            Some("https://github.com/o/r.git")
        );
        assert_eq!(
            clone_url_for_protocol("https://github.com/o/r.git", "ssh").as_deref(),
            Some("git@github.com:o/r.git")
        );
        assert_eq!(
            clone_url_for_protocol("ssh://git@h:2222/o/r.git", "https"),
            None
        );
        assert_eq!(
            clone_url_for_protocol("git@bitbucket.org:o/r.git", "https").as_deref(),
            Some("https://bitbucket.org/o/r.git")
        );
        assert_eq!(
            clone_url_for_protocol("git@ssh.dev.azure.com:v3/org/proj/repo", "https").as_deref(),
            Some("https://dev.azure.com/org/proj/_git/repo")
        );
        assert_eq!(clone_url_for_protocol("/local/path", "https"), None);
        assert_eq!(format_duration(Duration::from_secs(120)), "2m");
    }
}
