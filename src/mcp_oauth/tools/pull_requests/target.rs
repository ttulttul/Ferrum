use super::*;
use url::{Position, Url};

const INVALID: &str =
    "This is not a recognised pull request URL. Pass repository and number instead.";

fn canonical(host: &str, repository: &str) -> (String, String) {
    let host = host.trim().to_lowercase();
    let repo = repository.trim().to_lowercase();
    if let Some(org) = host.strip_suffix(".visualstudio.com") {
        let path = repo.strip_prefix("defaultcollection/").unwrap_or(&repo);
        if path.contains("/_git/") {
            return ("dev.azure.com".into(), format!("{org}/{path}"));
        }
    }
    if host == "ssh.bitbucket.org" {
        return ("bitbucket.org".into(), repo);
    }
    (host, repo)
}

pub(super) fn parse(raw: &str) -> Option<Value> {
    let url = Url::parse(raw).ok()?;
    if !matches!(url.scheme(), "https" | "http") {
        return None;
    }
    let host = url.host_str()?;
    let authority = &url[Position::BeforeHost..Position::AfterPort];
    let path = url.path_segments()?.collect::<Vec<_>>();
    let pair = path.windows(2).enumerate().find(|(i, w)| {
        *i >= 2
            && matches!(
                w[0],
                "pull" | "pulls" | "pull-requests" | "merge_requests" | "pullrequest"
            )
            && !w[1].is_empty()
            && w[1].bytes().all(|b| b.is_ascii_digit())
    })?;
    let (index, parts) = pair;
    let number = parts[1]
        .parse::<u64>()
        .ok()
        .filter(|n| (1..=9_007_199_254_740_991).contains(n))?;
    let (host, repo) = match (parts[0], &path[..index]) {
        ("pull", ["codebase", owner, repo]) if matches!(host, "cursor.com" | "www.cursor.com") => {
            ("origin.cursor.com", format!("{owner}/{repo}"))
        }
        ("pull", [owner, repo]) => (host, format!("{owner}/{repo}")),
        ("pulls", repo) => (authority, repo.join("/")),
        ("pull-requests", [owner, repo])
            if matches!(host, "bitbucket.org" | "ssh.bitbucket.org") =>
        {
            (host, format!("{owner}/{repo}"))
        }
        ("merge_requests", [.., "-"]) if index >= 3 => (host, path[..index - 1].join("/")),
        ("pullrequest", [.., "_git", _]) => (authority, path[..index].join("/")),
        _ => return None,
    };
    if repo.split('/').any(str::is_empty) {
        return None;
    }
    let (host, repo) = canonical(host, &repo);
    let url = if matches!(host.as_str(), "git.cafe" | "staging.git.cafe") && parts[0] == "pull" {
        raw.replacen(
            &format!("/{}/pull/", path[..index].join("/")),
            &format!("/{}/pulls/", path[..index].join("/")),
            1,
        )
    } else {
        raw.to_owned()
    };
    Some(json!({"host":host,"repository":repo,"number":number,"url":url}))
}

pub(super) fn resolve(args: &Value, identity: &Value) -> Outcome {
    if let Some(raw) = args["url"].as_str() {
        return parse(raw).ok_or_else(|| message(INVALID));
    }
    let (Some(repo), Some(number)) = (args["repository"].as_str(), args["number"].as_u64()) else {
        return Err(message("Pass either url, or both repository and number."));
    };
    let project_host = identity["canonicalKey"]
        .as_str()
        .and_then(|s| s.split_once('/'))
        .map(|v| v.0)
        .filter(|_| identity["provider"].is_string());
    let host = args["host"].as_str().or(project_host).ok_or_else(|| {
        message("This thread's project has no recognised remote. Pass host or url.")
    })?;
    let (host, repository) = canonical(host, repo);
    let kind = match host.as_str() {
        "github.com" => Some("github"),
        "bitbucket.org" => Some("bitbucket"),
        "gitlab.com" => Some("gitlab"),
        "codeberg.org" => Some("forgejo"),
        "dev.azure.com" => Some("azure-devops"),
        "origin.cursor.com" => Some("cursor-origin"),
        "git.cafe" | "staging.git.cafe" => Some("gitcafe"),
        _ => identity["provider"].as_str(),
    };
    let marker = match kind {
        Some("github" | "cursor-origin") => "pull",
        Some("bitbucket") => "pull-requests",
        Some("gitlab") => "-/merge_requests",
        Some("forgejo" | "gitcafe") => "pulls",
        Some("azure-devops") => "pullrequest",
        _ => return Err(message(
            "This thread's project is on a host with no known pull request URL shape. Pass url.",
        )),
    };
    let origin = if kind == Some("cursor-origin") {
        "https://cursor.com/codebase".to_owned()
    } else if let Some(url) = identity["locator"]["remoteUrl"]
        .as_str()
        .and_then(|s| Url::parse(s).ok())
        .filter(|u| matches!(u.scheme(), "http" | "https") && u.host_str() == Some(&host))
    {
        url.origin().ascii_serialization()
    } else {
        format!("https://{host}")
    };
    Ok(
        json!({"host":host,"repository":repository,"number":number,"url":format!("{origin}/{repository}/{marker}/{number}")}),
    )
}
