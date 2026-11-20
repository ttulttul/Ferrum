//! A project's repository identity, read from its git remotes as the reference's
//! project/RepositoryIdentityResolver.ts does, with the remote parsing of
//! `@t3tools/shared/git` and the built-in source control definitions.
//!
//! The reference also lets a signed-in host refine an identity (a renamed GitHub repository
//! reads under its new name). Hosts here are not asked, so identities are what the remotes say.
use regex::Regex;
use serde_json::{json, Map, Value};
use std::collections::HashMap;
use std::path::Path;
use std::sync::{LazyLock, Mutex};
use std::time::{Duration, Instant};

const POSITIVE_TTL: Duration = Duration::from_secs(15 * 60);
const NEGATIVE_TTL: Duration = Duration::from_secs(60);
const CACHE_CAPACITY: usize = 512;

fn regex(pattern: &str) -> Regex {
    Regex::new(pattern).expect("valid pattern")
}

static SCHEME: LazyLock<Regex> = LazyLock::new(|| regex(r"(?i)^(?:ssh|https?|git)://"));
static SCP_REPOSITORY: LazyLock<Regex> = LazyLock::new(|| {
    regex(r"(?i)^(?:[a-zA-Z0-9._-]+@)?(\[[0-9a-f:.]+\]|[^@:/\\\s\[\]]+):/?([^/\s]+(?:/[^/\s]+)+)$")
});
static SCP_REMOTE: LazyLock<Regex> = LazyLock::new(|| regex(r"^[a-zA-Z0-9._-]+@([^:/]+):"));
static USERLESS_SCP: LazyLock<Regex> =
    LazyLock::new(|| regex(r"^([^@:/\\\s]{2,}):(/?[^/\s]+(?:/[^/\s]+)*/?)$"));
static PATH_AFTER_USER_HOST: LazyLock<Regex> =
    LazyLock::new(|| regex(r"(?i)^[^@/\s]+@[^:/\s]+:((?:[^/\s]+/)+[^/\s]+?)(?:\.git)?/?$"));
static PATH_AFTER_HOST: LazyLock<Regex> =
    LazyLock::new(|| regex(r"(?i)^[^@:/\\\s]{2,}:((?:[^/\s]+/)+[^/\s]+?)(?:\.git)?/?$"));
static PATH_AFTER_URL_HOST: LazyLock<Regex> = LazyLock::new(|| {
    regex(r"(?i)^(?:ssh|https?|git)://[^/]+/((?:[^/\s]+/)+[^/\s]+?)(?:\.git)?/?$")
});
static AZURE_SSH: LazyLock<Regex> = LazyLock::new(|| {
    regex(r"^(ssh\.dev\.azure\.com|vs-ssh\.visualstudio\.com)/v3/([^/]+)/([^/]+)/([^/]+)$")
});
static REMOTE_LINE: LazyLock<Regex> =
    LazyLock::new(|| regex(r"^(\S+)\t(.+) \((fetch|push)\)(?:\s+\[[^\]]+\])?$"));
static GH_RESOLVED: LazyLock<Regex> =
    LazyLock::new(|| regex(r"^remote\.(.+)\.gh-resolved\s+(\S+)$"));

fn strip_git_suffix(path: &str) -> &str {
    let n = path.len();
    if n >= 4 && path.is_char_boundary(n - 4) && path[n - 4..].eq_ignore_ascii_case(".git") {
        &path[..n - 4]
    } else {
        path
    }
}

/// decodeURIComponent, which fails on a malformed escape or bytes that are not UTF-8.
fn decode_uri_component(value: &str) -> Option<String> {
    let bytes = value.as_bytes();
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%' {
            let valid = bytes
                .get(index + 1..index + 3)
                .is_some_and(|hex| hex.iter().all(u8::is_ascii_hexdigit));
            if !valid {
                return None;
            }
            index += 3;
        } else {
            index += 1;
        }
    }
    percent_encoding::percent_decode_str(value)
        .decode_utf8()
        .ok()
        .map(|decoded| decoded.into_owned())
}

fn canonical_segment(segment: &str) -> String {
    match decode_uri_component(segment) {
        // Lower case before encoding: lowering `%C3%84` never reaches the `Ä` it names.
        Some(decoded) => {
            crate::thread_projection::uri_component(&decoded.to_lowercase()).to_lowercase()
        }
        None => segment.to_lowercase(),
    }
}

/// A repository path in one lower-case spelling, however the remote escaped it.
pub fn canonical_repository_path(path: &str) -> String {
    path.split('/')
        .map(canonical_segment)
        .collect::<Vec<_>>()
        .join("/")
}

fn lower_case_remote(value: &str, keep_git_suffix: bool) -> String {
    let trimmed = value.trim().trim_end_matches('/');
    let path = if keep_git_suffix {
        trimmed
    } else {
        strip_git_suffix(trimmed)
    };
    path.to_lowercase()
}

/// The host and repository path a remote names, lower case and without a port.
fn parse_git_remote_repository(value: &str, keep_git_suffix: bool) -> Option<(String, String)> {
    let drop = |path: &str| -> String {
        if keep_git_suffix {
            path.to_owned()
        } else {
            strip_git_suffix(path).to_owned()
        }
    };
    let normalized = lower_case_remote(value, keep_git_suffix);
    if SCHEME.is_match(&normalized) {
        let url = url::Url::parse(&value.trim().to_lowercase()).ok()?;
        let path = drop(url.path().trim_end_matches('/'));
        let segments: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();
        if let Some(host) = url.host_str().filter(|h| !h.is_empty()) {
            if segments.len() > 1 {
                return Some((
                    host.to_owned(),
                    canonical_repository_path(&segments.join("/")),
                ));
            }
        }
    }
    // A userless one-letter prefix is a Windows drive, not an SCP host.
    let bytes = normalized.as_bytes();
    if bytes.len() >= 2 && bytes[0].is_ascii_alphabetic() && bytes[1] == b':' {
        return None;
    }
    let captures = SCP_REPOSITORY.captures(&normalized)?;
    let host = captures.get(1)?.as_str();
    let path = canonical_repository_path(captures.get(2)?.as_str());
    if !host.starts_with('[') {
        return Some((host.to_owned(), path));
    }
    let url = url::Url::parse(&format!("ssh://{host}")).ok()?;
    Some((url.host_str()?.to_owned(), path))
}

/// `host/path` for a remote that names a host, otherwise the remote itself, lower case.
pub fn normalize_git_remote_url(value: &str) -> String {
    match parse_git_remote_repository(value, false) {
        Some((host, path)) => format!("{host}/{path}"),
        None => lower_case_remote(value, false),
    }
}

fn is_azure_hostname(hostname: &str) -> bool {
    hostname == "dev.azure.com"
        || hostname.ends_with(".dev.azure.com")
        || hostname.ends_with(".visualstudio.com")
}

/// The key a project's repository is known by. Azure DevOps names repositories as written and
/// spells SSH remotes differently, so it rewrites the key; other hosts keep the normal form.
pub fn repository_key_from_remote(remote_url: &str) -> String {
    let key = normalize_git_remote_url(remote_url);
    let Some((host, path)) = parse_git_remote_repository(remote_url, true) else {
        return key;
    };
    let written = format!("{host}/{path}");
    if let Some(captures) = AZURE_SSH.captures(&written) {
        let (organization, project, repository) = (&captures[2], &captures[3], &captures[4]);
        return if &captures[1] == "ssh.dev.azure.com" {
            format!("dev.azure.com/{organization}/{project}/_git/{repository}")
        } else {
            format!("{organization}.visualstudio.com/{project}/_git/{repository}")
        };
    }
    if is_azure_hostname(written.split('/').next().unwrap_or_default()) {
        return written;
    }
    key
}

/// The host a remote is on: with its port as written, and as a bare host name.
fn parse_git_remote_host(remote_url: &str) -> Option<(String, String)> {
    let trimmed = remote_url.trim();
    if trimmed.is_empty() {
        return None;
    }
    let scp_host = SCP_REMOTE
        .captures(trimmed)
        .map(|c| c[1].to_owned())
        .or_else(|| {
            USERLESS_SCP
                .captures(trimmed)
                .filter(|c| !c[2].starts_with(':'))
                .map(|c| c[1].to_owned())
        });
    let host = match scp_host {
        Some(host) => host.to_lowercase(),
        None => {
            let url = url::Url::parse(trimmed).ok()?;
            let hostname = url.host_str().unwrap_or_default();
            if url.scheme() == "ssh" {
                hostname.to_lowercase()
            } else {
                match url.port() {
                    Some(port) => format!("{hostname}:{port}").to_lowercase(),
                    None => hostname.to_lowercase(),
                }
            }
        }
    };
    if host.is_empty() {
        return None;
    }
    let hostname = url::Url::parse(&format!("https://{host}"))
        .ok()
        .and_then(|url| url.host_str().map(str::to_lowercase))
        .unwrap_or_else(|| {
            let port = Regex::new(r":\d+$").expect("valid pattern");
            port.replace(&host, "").to_lowercase()
        });
    Some((host, hostname))
}

/// The source control host a remote is on, in the reference's recognition order, or
/// `unknown` for a host no definition claims.
pub fn recognize_remote(remote_url: &str) -> Option<&'static str> {
    let (_, hostname) = parse_git_remote_host(remote_url)?;
    let labels: Vec<&str> = hostname.split('.').collect();
    let kind = if matches!(hostname.as_str(), "git.cafe" | "staging.git.cafe") {
        "gitcafe"
    } else if hostname == "origin.cursor.com" {
        "cursor-origin"
    } else if hostname == "codeberg.org" || labels.contains(&"forgejo") || labels.contains(&"gitea")
    {
        "forgejo"
    } else if hostname == "github.com"
        || hostname.ends_with(".ghe.com")
        || labels.contains(&"github")
    {
        "github"
    } else if hostname == "gitlab.com" || labels.contains(&"gitlab") {
        "gitlab"
    } else if is_azure_hostname(&hostname) {
        "azure-devops"
    } else if hostname == "bitbucket.org"
        || hostname.ends_with(".bitbucket.org")
        || labels.contains(&"bitbucket")
    {
        "bitbucket"
    } else {
        "unknown"
    };
    Some(kind)
}

/// The repository path a remote names, as written.
fn repository_path_from_remote_url(url: &str) -> Option<String> {
    let remote = url.trim();
    let trimmed = if SCHEME.is_match(remote) {
        remote.split(['?', '#']).next().unwrap_or_default()
    } else {
        remote
    };
    if trimmed.is_empty() {
        return None;
    }
    let path = PATH_AFTER_USER_HOST
        .captures(trimmed)
        .or_else(|| {
            PATH_AFTER_HOST
                .captures(trimmed)
                .filter(|c| !c[1].starts_with(':'))
        })
        .or_else(|| PATH_AFTER_URL_HOST.captures(trimmed))
        .map(|c| c[1].trim().to_owned())?;
    (!path.is_empty()).then_some(path)
}

fn repository_path_of(canonical_key: &str, remote_url: &str) -> String {
    let canonical_path = canonical_key
        .split_once('/')
        .map_or("", |(_, path)| path)
        .to_owned();
    let Some(display) = repository_path_from_remote_url(remote_url) else {
        return canonical_path;
    };
    // A host that names repositories literally keeps the `.git` the display path drops.
    let want = canonical_repository_path(&canonical_path);
    [display.clone(), format!("{display}.git")]
        .into_iter()
        .find(|path| canonical_repository_path(path) == want)
        .unwrap_or(canonical_path)
}

/// Remotes on a host, in `git remote -v` order. Local paths would give unrelated
/// repositories one identity, so they are left out.
fn parse_remote_fetch_urls(stdout: &str) -> Vec<(String, String)> {
    let mut remotes: Vec<(String, String)> = Vec::new();
    for line in stdout.split('\n') {
        let line = line.strip_suffix('\r').unwrap_or(line);
        let Some(captures) = REMOTE_LINE.captures(line) else {
            continue;
        };
        let (name, url) = (&captures[1], &captures[2]);
        if &captures[3] != "fetch" || is_local_remote_url(url) {
            continue;
        }
        match remotes.iter_mut().find(|(existing, _)| existing == name) {
            Some(entry) => entry.1 = url.to_owned(),
            None => remotes.push((name.to_owned(), url.to_owned())),
        }
    }
    remotes
}

fn is_local_remote_url(url: &str) -> bool {
    let lower = url.to_ascii_lowercase();
    if lower.starts_with("file:") {
        return true;
    }
    if Regex::new(r"^[a-z][a-z0-9+.-]*://")
        .expect("valid pattern")
        .is_match(&lower)
    {
        return false;
    }
    let bytes = lower.as_bytes();
    if bytes.len() >= 3
        && bytes[0].is_ascii_alphabetic()
        && bytes[1] == b':'
        && matches!(bytes[2], b'/' | b'\\')
    {
        return true;
    }
    // Git treats `host:path` as scp-style only when the colon precedes any slash.
    let colon = url.find(':');
    let slash = url.find(['/', '\\']);
    match (colon, slash) {
        (None, _) => true,
        (Some(colon), Some(slash)) => slash < colon,
        (Some(_), None) => false,
    }
}

/// The remote `gh repo set-default` marked, when exactly one is marked.
fn pick_marked_remote(remotes: &[(String, String)], resolved: &str) -> Option<(String, String)> {
    let marks: Vec<(String, String, String)> = resolved
        .split('\n')
        .filter_map(|line| {
            let captures = GH_RESOLVED.captures(line.trim())?;
            let name = captures[1].to_owned();
            let url = remotes.iter().find(|(n, _)| *n == name)?.1.clone();
            Some((name, url, captures[2].to_owned()))
        })
        .collect();
    let [(name, url, value)] = marks.as_slice() else {
        return None;
    };
    if value == "base" {
        return Some((name.clone(), url.clone()));
    }
    let host = parse_git_remote_host(url).map(|(host, _)| host);
    let marked_path = value.to_lowercase();
    remotes
        .iter()
        .find(|(_, candidate)| {
            parse_git_remote_host(candidate).map(|(host, _)| host) == host
                && normalize_git_remote_url(candidate)
                    .split_once('/')
                    .map_or("", |(_, path)| path)
                    == marked_path
        })
        .cloned()
}

fn pick_primary_remote(remotes: &[(String, String)], resolved: &str) -> Option<(String, String)> {
    if let Some(marked) = pick_marked_remote(remotes, resolved) {
        return Some(marked);
    }
    for preferred in ["upstream", "origin"] {
        if let Some(remote) = remotes.iter().find(|(name, _)| name == preferred) {
            return Some(remote.clone());
        }
    }
    remotes.iter().min_by(|a, b| a.0.cmp(&b.0)).cloned()
}

/// `RepositoryIdentity` for a checkout at `root_path` with these remotes.
pub fn build_identity(
    remote_name: &str,
    remote_url: &str,
    origin_url: Option<&str>,
    root_path: &str,
) -> Value {
    let canonical_key = repository_key_from_remote(remote_url);
    let provider = recognize_remote(remote_url);
    let repository_path = repository_path_of(&canonical_key, remote_url);
    let segments: Vec<&str> = repository_path
        .split('/')
        .filter(|s| !s.is_empty())
        .collect();
    let mut identity = Map::new();
    identity.insert("canonicalKey".into(), json!(canonical_key));
    identity.insert(
        "locator".into(),
        json!({"source":"git-remote","remoteName":remote_name,"remoteUrl":remote_url}),
    );
    identity.insert("rootPath".into(), json!(root_path));
    if !repository_path.is_empty() {
        identity.insert("displayName".into(), json!(repository_path));
    }
    if let Some(provider) = provider {
        identity.insert("provider".into(), json!(provider));
    }
    if let Some(owner) = segments.first() {
        identity.insert("owner".into(), json!(owner));
    }
    if let Some(name) = segments.last() {
        identity.insert("name".into(), json!(name));
    }
    if let Some(origin_url) = origin_url {
        let origin_key = repository_key_from_remote(origin_url);
        if origin_key != canonical_key {
            let mut origin = Map::new();
            let display = repository_path_of(&origin_key, origin_url);
            origin.insert("canonicalKey".into(), json!(origin_key));
            origin.insert("remoteUrl".into(), json!(origin_url));
            if !display.is_empty() {
                origin.insert("displayName".into(), json!(display));
            }
            identity.insert("origin".into(), Value::Object(origin));
        }
    }
    Value::Object(identity)
}

/// The identity of a checkout, given `git remote -v` and the `gh-resolved` marks.
pub fn identity_from_remotes(
    remote_output: &str,
    resolved: &str,
    root_path: &str,
) -> Option<Value> {
    let remotes = parse_remote_fetch_urls(remote_output);
    let (name, url) = pick_primary_remote(&remotes, resolved)?;
    let origin = remotes
        .iter()
        .find(|(n, _)| n == "origin")
        .map(|(_, url)| url.as_str());
    Some(build_identity(&name, &url, origin, root_path))
}

fn git(cwd: &Path, args: &[&str]) -> Option<(bool, String)> {
    let mut command = std::process::Command::new("git");
    command.arg("-C").arg(cwd).args(args);
    // A repository the server itself runs in must not stand in for the project's.
    for key in [
        "GIT_DIR",
        "GIT_WORK_TREE",
        "GIT_INDEX_FILE",
        "GIT_COMMON_DIR",
        "GIT_NAMESPACE",
        "GIT_OBJECT_DIRECTORY",
        "GIT_PREFIX",
    ] {
        command.env_remove(key);
    }
    let output = command
        .stdin(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .output()
        .ok()?;
    Some((
        output.status.success(),
        String::from_utf8_lossy(&output.stdout).into_owned(),
    ))
}

/// The work tree root, or the git dir of a bare repository, and whether it is bare.
fn repository_root(cwd: &Path) -> Option<(String, bool)> {
    // `git -C` cannot enter a missing folder or a file.
    if std::fs::metadata(cwd).is_ok_and(|m| !m.is_dir()) || !cwd.exists() {
        return None;
    }
    let (ok, stdout) = git(cwd, &["rev-parse", "--show-toplevel"])?;
    if ok {
        let root = stdout.strip_suffix('\n').unwrap_or(&stdout);
        return (!root.is_empty()).then(|| (root.to_owned(), false));
    }
    for (git_dir, entry) in [(".", "HEAD"), (".git", ".git")] {
        let target = cwd.join(entry);
        if !target.exists() && std::fs::symlink_metadata(&target).is_err() {
            continue;
        }
        let (ok, stdout) = git(
            cwd,
            &[
                &format!("--git-dir={git_dir}"),
                "rev-parse",
                "--is-bare-repository",
                "--absolute-git-dir",
            ],
        )?;
        if !ok {
            continue;
        }
        let (bare, root) = stdout.split_once('\n').unwrap_or((&stdout, ""));
        let root = root.strip_suffix('\n').unwrap_or(root);
        // A checkout's .git directory has remotes but is not a project root.
        return (bare == "true" && !root.is_empty()).then(|| (root.to_owned(), true));
    }
    None
}

/// Resolves a workspace's identity from git, uncached.
pub fn resolve_uncached(cwd: &Path) -> Option<Value> {
    let (root, bare) = repository_root(cwd)?;
    let root_path = Path::new(&root);
    let prefix: &[&str] = if bare { &["--git-dir=."] } else { &[] };
    let (ok, remotes) = git(root_path, &[prefix, &["remote", "-v"]].concat())?;
    if !ok {
        return None;
    }
    // Exits 1 when no remote has a mark.
    let resolved = git(
        root_path,
        &[
            prefix,
            &["config", "--get-regexp", r"^remote\..*\.gh-resolved$"],
        ]
        .concat(),
    )
    .filter(|(ok, _)| *ok)
    .map(|(_, out)| out)
    .unwrap_or_default();
    identity_from_remotes(&remotes, &resolved, &root)
}

/// Identities by workspace root, with when each was read.
type IdentityCache = HashMap<String, (Instant, Option<Value>)>;
static CACHE: LazyLock<Mutex<IdentityCache>> = LazyLock::new(Mutex::default);

/// A workspace's identity, cached as the reference caches it: found identities for 15
/// minutes, missing ones for one.
pub fn resolve(workspace_root: &str) -> Option<Value> {
    if let Some((at, value)) = CACHE.lock().expect("identity cache").get(workspace_root) {
        let ttl = if value.is_some() {
            POSITIVE_TTL
        } else {
            NEGATIVE_TTL
        };
        if at.elapsed() < ttl {
            return value.clone();
        }
    }
    let value = resolve_uncached(Path::new(workspace_root));
    let mut cache = CACHE.lock().expect("identity cache");
    if cache.len() >= CACHE_CAPACITY {
        cache.retain(|_, (at, value)| {
            at.elapsed()
                < if value.is_some() {
                    POSITIVE_TTL
                } else {
                    NEGATIVE_TTL
                }
        });
        if cache.len() >= CACHE_CAPACITY {
            cache.clear();
        }
    }
    cache.insert(workspace_root.to_owned(), (Instant::now(), value.clone()));
    value
}

/// Forgets cached identities, as a clone or publish does in the reference.
pub fn invalidate(workspace_root: &str) {
    CACHE.lock().expect("identity cache").remove(workspace_root);
}

/// A stored project row with the identity of its workspace filled in.
pub fn with_identity(row: &Value) -> Value {
    let mut row = row.clone();
    if row.get("repositoryIdentity").is_some_and(Value::is_null) {
        if let Some(root) = row["workspaceRoot"].as_str() {
            row["repositoryIdentity"] = resolve(root).unwrap_or(Value::Null);
        }
    }
    row
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Remotes as the pinned reference reads them (scripts/repository_identity_cases.mjs).
    #[test]
    fn remotes_read_as_the_reference_reads_them() {
        let cases = include_str!("../tests/repository-identity-cases.jsonl");
        let mut failures = Vec::new();
        for line in cases.lines().filter(|line| !line.trim().is_empty()) {
            let case: Value = serde_json::from_str(line).unwrap();
            let remote = case["remoteUrl"].as_str().unwrap();
            let key = repository_key_from_remote(remote);
            let actual = json!({
                "remoteUrl": remote,
                "canonicalKey": key,
                "provider": recognize_remote(remote),
                "displayName": repository_path_of(&key, remote),
            });
            if actual != case {
                failures.push(format!("expected {case}\n     got {actual}"));
            }
        }
        assert!(failures.is_empty(), "{}", failures.join("\n"));
    }

    #[test]
    fn identity_prefers_marked_then_upstream_then_origin() {
        let remotes = "origin\tgit@github.com:me/fork.git (fetch)\norigin\tgit@github.com:me/fork.git (push)\n\
                       upstream\thttps://github.com/Org/Project.git (fetch)\nupstream\thttps://github.com/Org/Project.git (push)\n\
                       local\t/srv/mirror.git (fetch)\n";
        let identity = identity_from_remotes(remotes, "", "/work/project").unwrap();
        assert_eq!(
            identity,
            json!({
                "canonicalKey":"github.com/org/project",
                "locator":{"source":"git-remote","remoteName":"upstream","remoteUrl":"https://github.com/Org/Project.git"},
                "rootPath":"/work/project",
                "displayName":"Org/Project",
                "provider":"github",
                "owner":"Org",
                "name":"Project",
                "origin":{"canonicalKey":"github.com/me/fork","remoteUrl":"git@github.com:me/fork.git","displayName":"me/fork"}
            })
        );
        let marked =
            identity_from_remotes(remotes, "remote.origin.gh-resolved base\n", "/work/project")
                .unwrap();
        assert_eq!(marked["locator"]["remoteName"], "origin");
        assert!(marked.get("origin").is_none());
        let by_path = identity_from_remotes(
            remotes,
            "remote.origin.gh-resolved org/project\n",
            "/work/project",
        )
        .unwrap();
        assert_eq!(by_path["locator"]["remoteName"], "upstream");
        // Only local remotes: no identity.
        assert!(identity_from_remotes("local\t/srv/mirror.git (fetch)\n", "", "/w").is_none());
    }

    #[test]
    fn checkouts_resolve_from_git() {
        let dir = tempfile::tempdir().unwrap();
        let run = |args: &[&str]| {
            assert!(std::process::Command::new("git")
                .arg("-C")
                .arg(dir.path())
                .args(args)
                .output()
                .unwrap()
                .status
                .success());
        };
        run(&["init", "-q"]);
        assert_eq!(resolve_uncached(dir.path()), None);
        run(&[
            "remote",
            "add",
            "origin",
            "git@ssh.bitbucket.org:example-org/example-repo.git",
        ]);
        let root = std::fs::canonicalize(dir.path()).unwrap();
        let nested = root.join("nested");
        std::fs::create_dir(&nested).unwrap();
        let identity = resolve_uncached(&nested).unwrap();
        assert_eq!(
            identity["canonicalKey"],
            "ssh.bitbucket.org/example-org/example-repo"
        );
        assert_eq!(identity["provider"], "bitbucket");
        assert_eq!(identity["rootPath"], root.to_str().unwrap());
        assert_eq!(resolve_uncached(&root.join("missing")), None);
    }
}
