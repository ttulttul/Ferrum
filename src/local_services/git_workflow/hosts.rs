use super::{provider_error, resolved, Reply};
use serde_json::{json, Value};
use std::{process::Stdio, time::Duration};
use tokio::io::AsyncWriteExt;
use url::Url;

pub(super) struct Remote {
    pub kind: &'static str,
    host: String,
    pub(super) repo: String,
}
fn safe_path(s: &str) -> bool {
    s.split('/').all(|p| {
        !p.is_empty()
            && !matches!(p, "." | "..")
            && p.bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"._-%".contains(&b))
    })
}
impl Remote {
    pub fn parse(value: &str) -> Option<Self> {
        let url = if value.contains("://") {
            Url::parse(value).ok()?
        } else {
            let (host, path) = value.split_once(':')?;
            Url::parse(&format!("ssh://{host}/{path}")).ok()?
        };
        let host = url.host_str()?.to_lowercase();
        let repo = url
            .path()
            .trim_matches('/')
            .trim_end_matches(".git")
            .to_owned();
        if !safe_path(&repo) {
            return None;
        }
        let kind = match host.as_str() {
            h if h.split('.').any(|part| part == "gitlab") => "gitlab",
            "bitbucket.org" => "bitbucket",
            "origin.cursor.com" => "cursor-origin",
            "git.cafe" | "staging.git.cafe" => "gitcafe",
            h if h == "codeberg.org" || h.split('.').any(|p| matches!(p, "gitea" | "forgejo")) => {
                "forgejo"
            }
            "dev.azure.com" | "ssh.dev.azure.com" | "vs-ssh.visualstudio.com" => "azure-devops",
            h if h.ends_with(".visualstudio.com") => "azure-devops",
            _ => return None,
        };
        Some(Self { kind, host, repo })
    }
    fn error(&self, cwd: &str, op: &str, reference: &str, detail: &str) -> Value {
        if self.kind == "gitcafe" || self.kind == "forgejo" {
            return json!({"_tag":"SourceControlProviderError","provider":self.kind,"operation":op,"cwd":cwd,"detail":detail});
        }
        let error = provider_error(self.kind, op, cwd, reference, detail);
        if self.kind == "cursor-origin" {
            return json!({"_tag":"SourceControlProviderError","provider":self.kind,"operation":op,"cwd":cwd,"command":"origin","reference":error["reference"],"detail":detail,"cause":{"name":"OriginCliError","message":format!("Cursor Origin failed in {op}: {detail}")}});
        }
        error
    }
    async fn command(
        &self,
        cwd: &str,
        op: &str,
        reference: &str,
        bin: &str,
        args: &[&str],
        input: Option<String>,
    ) -> Result<String, Value> {
        let err = || self.error(cwd, op, reference, &format!("{bin} failed to {op}."));
        let mut cmd = crate::login_shell::tokio_command(bin);
        cmd.args(args)
            .current_dir(cwd)
            .kill_on_drop(true)
            .env("GIT_TERMINAL_PROMPT", "0")
            .env("NO_COLOR", "1")
            .env("CAFE_NO_UPDATE_CHECK", "1")
            .stdin(if input.is_some() {
                Stdio::piped()
            } else {
                Stdio::null()
            })
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        let mut child=cmd.spawn().map_err(|e|{
            if e.kind()!=std::io::ErrorKind::NotFound{return err()}
            if bin=="cafe" {
                let detail=format!("No GitCafe credential for {}: set CAFE_TOKEN, or install the GitCafe CLI and run `cafe auth login`.",self.host);
                let mut failure=self.error(cwd,op,reference,&detail);
                failure["cause"]=json!({"name":"GitCafeApiError","message":format!("GitCafe {op} failed on {}: {detail}",self.host),"cause":{"name":"GitCafeCliMissingError","message":detail}});
                return failure
            }
            if bin=="origin" {
                let detail="Cursor Origin CLI (`origin`) is required but not available on PATH.";
                let stage=if args==["auth","token"]{"authToken"}else{op};
                let mut failure=self.error(cwd,op,reference,detail);
                failure["cause"]=json!({"name":"OriginCliError","message":format!("Cursor Origin failed in {stage}: {detail}"),"cause":{"name":"VcsProcessSpawnError","message":format!("VCS process failed to spawn in CursorOrigin.{stage}: origin ({cwd})"),"cause":{"name":"PlatformError","message":format!("NotFound: ChildProcess.spawn (origin {})",args.join(" ")),"cause":{"name":"Error","message":"spawn origin ENOENT"}}}});
                return failure
            }
            err()
        })?;
        let out = tokio::time::timeout(Duration::from_secs(60), async {
            if let Some(contents) = input {
                child
                    .stdin
                    .take()
                    .unwrap()
                    .write_all(contents.as_bytes())
                    .await?;
            }
            child.wait_with_output().await
        })
        .await
        .ok()
        .and_then(Result::ok)
        .filter(|o| o.status.success())
        .ok_or_else(err)?;
        if bin == "tea" && args.first() == Some(&"api") {
            let headers = String::from_utf8_lossy(&out.stderr);
            let code = headers
                .lines()
                .find(|s| s.starts_with("HTTP/"))
                .and_then(|s| s.split_whitespace().nth(1))
                .and_then(|s| s.parse::<u16>().ok());
            if !code.is_some_and(|s| (200..300).contains(&s)) {
                return Err(err());
            }
        }
        String::from_utf8(out.stdout).map_err(|_| err())
    }
    fn number(&self, reference: &str) -> Option<(String, u64)> {
        if let Ok(n) = reference.parse::<u64>() {
            return (n > 0).then(|| (self.repo.clone(), n));
        }
        let url = Url::parse(reference).ok()?;
        let h = url.host_str()?;
        if self.kind == "cursor-origin" {
            if !matches!(h, "cursor.com" | "www.cursor.com") {
                return None;
            }
        } else if h != self.host {
            return None;
        }
        let path: Vec<_> = url.path_segments()?.filter(|p| !p.is_empty()).collect();
        let idx = path.iter().position(|p| {
            matches!(
                p.to_ascii_lowercase().as_str(),
                "pull" | "pulls" | "pull-requests" | "pullrequest" | "merge_requests"
            )
        })?;
        let n = path.get(idx + 1)?.parse().ok()?;
        let start = usize::from(self.kind == "cursor-origin" && path.first() == Some(&"codebase"));
        let repo = path.get(start..idx)?.join("/");
        let repo = if matches!(self.kind, "bitbucket" | "forgejo") {
            self.repo.clone()
        } else {
            repo
        };
        (n > 0 && safe_path(&repo)).then_some((repo, n))
    }
    async fn tea_login(&self, cwd: &str, reference: &str) -> Result<(String, String), Value> {
        let list = self
            .command(
                cwd,
                "getChangeRequest",
                reference,
                "tea",
                &["login", "list", "--output", "json"],
                None,
            )
            .await?;
        let logins: Value = serde_json::from_str(&list).unwrap_or(Value::Null);
        let rows: Vec<_> = logins
            .as_array()
            .into_iter()
            .flatten()
            .filter(|v| {
                v["url"]
                    .as_str()
                    .and_then(|s| Url::parse(s).ok())
                    .is_some_and(|url| {
                        url.host_str() == Some(&self.host) || v["ssh_host"] == self.host
                    })
            })
            .collect();
        let row = rows
            .iter()
            .find(|v| v["default"] == "true")
            .copied()
            .or_else(|| (rows.len() == 1).then(|| rows[0]));
        row.and_then(|v|Some((v["name"].as_str()?.into(),v["url"].as_str()?.trim_end_matches('/').into())))
            .ok_or_else(||self.error(cwd,"getChangeRequest",reference,"No matching Forgejo login. Use `fj auth login`, `fj auth add-token`, or `tea login add` for this server. For a server under a URL subpath, use `tea login add`. Choose a default when multiple tea accounts match."))
    }
    async fn http(
        &self,
        cwd: &str,
        reference: &str,
        repo: &str,
        suffix: &str,
    ) -> Result<Value, Value> {
        let fail = |s: &str| self.error(cwd, "getChangeRequest", reference, s);
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|_| fail("Could not initialize provider."))?;
        let req = match self.kind {
            "bitbucket" => {
                let base = std::env::var("T3CODE_BITBUCKET_API_BASE_URL")
                    .unwrap_or_else(|_| "https://api.bitbucket.org/2.0".into());
                let req = client.get(format!(
                    "{}/repositories/{repo}/pullrequests{suffix}",
                    base.trim_end_matches('/')
                ));
                match crate::bitbucket_credential::current().await {
                    Ok(Some(credential)) => credential.apply(req),
                    _ => req,
                }
            }
            "cursor-origin" => {
                let token = self
                    .command(
                        cwd,
                        "getChangeRequest",
                        reference,
                        "origin",
                        &["auth", "token"],
                        None,
                    )
                    .await?;
                client
                    .get(format!(
                        "https://api.cursor.com/v1/origin/repos/{repo}/pulls{suffix}"
                    ))
                    .bearer_auth(token.trim())
            }
            "gitcafe" => {
                let cafe_host = std::env::var("CAFE_HOST")
                    .ok()
                    .map(|s| {
                        s.trim_end_matches('/')
                            .trim_end_matches("/api")
                            .trim_start_matches("https://")
                            .to_owned()
                    })
                    .unwrap_or_else(|| "git.cafe".into());
                let token = if cafe_host == self.host {
                    std::env::var("CAFE_TOKEN")
                        .ok()
                        .filter(|s| !s.trim().is_empty())
                } else {
                    None
                };
                let token = match token {
                    Some(t) => t,
                    None => {
                        let hosturl = format!("https://{}/api", self.host);
                        let cred = self
                            .command(
                                cwd,
                                "getChangeRequest",
                                reference,
                                "cafe",
                                &["auth", "http", "credential", "--host", &hosturl, "get"],
                                Some(format!("protocol=https\nhost={}\n\n", self.host)),
                            )
                            .await?;
                        cred.lines()
                            .find_map(|s| s.strip_prefix("password="))
                            .filter(|s| !s.is_empty())
                            .ok_or_else(|| {
                                fail("The GitCafe CLI could not hand over a credential.")
                            })?
                            .to_owned()
                    }
                };
                client
                    .get(format!(
                        "https://{}/api/repos/{repo}/pulls{suffix}",
                        self.host
                    ))
                    .bearer_auth(token.trim())
            }
            _ => unreachable!(),
        };
        let reply = req
            .header("Accept", "application/json")
            .send()
            .await
            .map_err(|_| fail("Could not reach the source control provider."))?;
        if !reply.status().is_success() {
            return Err(fail("Failed to get change request."));
        }
        reply
            .json()
            .await
            .map_err(|_| fail("The provider returned an unreadable response."))
    }
    pub async fn read(&self, cwd: &str, reference: &str) -> Reply {
        let op = "getChangeRequest";
        let parsed = self.number(reference);
        let number = parsed.as_ref().map(|(_, n)| n.to_string());
        let target = parsed
            .as_ref()
            .map(|(r, _)| r.as_str())
            .unwrap_or(&self.repo);
        let raw = match self.kind {
            "gitlab" => {
                self.command(
                    cwd,
                    op,
                    reference,
                    "glab",
                    &["mr", "view", "--output", "json", "--", reference],
                    None,
                )
                .await?
            }
            "azure-devops" => {
                let org = if self.host == "dev.azure.com" {
                    format!(
                        "https://dev.azure.com/{}",
                        self.repo.split('/').next().unwrap_or("")
                    )
                } else if self.repo.starts_with("v3/") {
                    format!(
                        "https://dev.azure.com/{}",
                        self.repo.split('/').nth(1).unwrap_or("")
                    )
                } else {
                    format!("https://{}", self.host)
                };
                let id = number.as_deref().unwrap_or(reference);
                if !id.bytes().all(|b| b.is_ascii_digit()) {
                    return Err(self.error(
                        cwd,
                        op,
                        reference,
                        "Expected an Azure DevOps pull request number.",
                    ));
                }
                self.command(
                    cwd,
                    op,
                    reference,
                    "az",
                    &[
                        "repos",
                        "pr",
                        "show",
                        "--detect",
                        "false",
                        "--id",
                        id,
                        "--only-show-errors",
                        "--output",
                        "json",
                        "--organization",
                        &org,
                    ],
                    None,
                )
                .await?
            }
            "forgejo" => {
                let number = number.as_deref().ok_or_else(|| {
                    self.error(cwd, op, reference, "Specify a pull request number or URL.")
                })?;
                let (login, base) = self.tea_login(cwd, reference).await?;
                self.command(
                    cwd,
                    op,
                    reference,
                    "tea",
                    &[
                        "api",
                        "--include",
                        "--login",
                        &login,
                        "--repo",
                        target,
                        "--method",
                        "GET",
                        &format!("{base}/api/v1/repos/{target}/pulls/{number}"),
                    ],
                    None,
                )
                .await?
            }
            _ => {
                let (repo, n) = parsed.ok_or_else(|| {
                    self.error(
                        cwd,
                        op,
                        reference,
                        match self.kind {
                            "gitcafe" => {
                                "A GitCafe pull request is a number or a git.cafe pull request URL."
                            }
                            "cursor-origin" => {
                                "Specify a pull request number or Cursor Origin pull request URL."
                            }
                            _ => "Expected a Bitbucket pull request number.",
                        },
                    )
                })?;
                let raw = self.http(cwd, reference, &repo, &format!("/{n}")).await?;
                return self.normalize(&raw, &repo).ok_or_else(|| {
                    self.error(
                        cwd,
                        op,
                        reference,
                        "The provider returned an unreadable response.",
                    )
                });
            }
        };
        serde_json::from_str(&raw)
            .ok()
            .and_then(|v| self.normalize(&v, target))
            .ok_or_else(|| {
                self.error(
                    cwd,
                    op,
                    reference,
                    "The provider returned an unreadable response.",
                )
            })
    }
    pub(super) async fn list(
        &self,
        cwd: &str,
        head: &str,
        open_only: bool,
    ) -> Result<Vec<Value>, Value> {
        let state = if open_only { "open" } else { "all" };
        let op = "listChangeRequests";
        let invalid = || self.error(cwd, op, head, "The provider returned an unreadable list.");
        let branch: String = url::form_urlencoded::byte_serialize(head.as_bytes()).collect();
        let rows = match self.kind {
            "gitlab" | "azure-devops" => {
                let (bin, mut args) = if self.kind == "gitlab" {
                    let mut args = vec!["mr", "list", "--source-branch", head];
                    if !open_only {
                        args.push("--all");
                    }
                    args.extend(["--per-page", "100", "--output", "json"]);
                    ("glab", args)
                } else {
                    (
                        "az",
                        vec![
                            "repos",
                            "pr",
                            "list",
                            "--detect",
                            "true",
                            "--source-branch",
                            &branch,
                            "--status",
                            if open_only { "active" } else { "all" },
                            "--top",
                            "100",
                            "--only-show-errors",
                            "--output",
                            "json",
                        ],
                    )
                };
                let scoped;
                if self.kind == "azure-devops" {
                    let parts: Vec<_> = self.repo.split('/').collect();
                    let scope = match parts.as_slice() {
                        ["v3", org, project, repo] => Some((*org, *project, *repo)),
                        [org, project, "_git", repo] if self.host == "dev.azure.com" => {
                            Some((*org, *project, *repo))
                        }
                        [project, "_git", repo] | ["DefaultCollection", project, "_git", repo] => {
                            self.host
                                .strip_suffix(".visualstudio.com")
                                .map(|org| (org, *project, *repo))
                        }
                        _ => None,
                    };
                    if let Some((org, project, repo)) = scope {
                        args[4] = "false";
                        let organization = if self.host.ends_with(".visualstudio.com") {
                            format!("https://{org}.visualstudio.com")
                        } else {
                            format!("https://dev.azure.com/{org}")
                        };
                        scoped = [
                            organization,
                            format!("--project={project}"),
                            format!("--repository={repo}"),
                        ];
                        args.push("--organization");
                        args.extend(scoped.iter().map(String::as_str));
                    }
                }
                let raw = self.command(cwd, op, head, bin, &args, None).await?;
                if raw.trim().is_empty() {
                    return Ok(vec![]);
                }
                serde_json::from_str::<Vec<Value>>(&raw).map_err(|_| invalid())?
            }
            "forgejo" => {
                let (login, base) = self.tea_login(cwd, head).await?;
                let mut found = vec![];
                for page in 1.. {
                    let endpoint = format!("{base}/api/v1/repos/{}/pulls?state={state}&head={branch}&sort=recentupdate&limit=50&page={page}",self.repo);
                    let raw = self
                        .command(
                            cwd,
                            op,
                            head,
                            "tea",
                            &[
                                "api",
                                "--include",
                                "--login",
                                &login,
                                "--repo",
                                &self.repo,
                                "--method",
                                "GET",
                                &endpoint,
                            ],
                            None,
                        )
                        .await?;
                    let rows: Vec<Value> = serde_json::from_str(&raw).map_err(|_| invalid())?;
                    let last = rows.len() < 50;
                    found.extend(
                        rows.into_iter()
                            .filter(|r| r["head"]["ref"] == head || r["head"]["label"] == head),
                    );
                    if last || found.len() >= 20 {
                        break;
                    }
                }
                found
            }
            "gitcafe" => {
                let mut after = None;
                let mut found = vec![];
                loop {
                    let mut pairs = vec![
                        ("sourceBranches", json!([head]).to_string()),
                        ("sort", "newest".into()),
                        ("limit", "20".into()),
                    ];
                    if open_only {
                        pairs.push(("state", "open".into()));
                    }
                    if let Some(cursor) = &after {
                        pairs.push(("after", String::clone(cursor)));
                    }
                    let query = url::form_urlencoded::Serializer::new(String::new())
                        .extend_pairs(pairs)
                        .finish();
                    let page = self
                        .http(cwd, head, &self.repo, &format!("?{query}"))
                        .await?;
                    let rows = page["items"].as_array().ok_or_else(invalid)?;
                    for row in rows {
                        if row["sourceBranch"] == head {
                            let n = row["number"].as_u64().ok_or_else(invalid)?;
                            found.push(self.read(cwd, &n.to_string()).await?);
                            if found.len() == 20 {
                                return Ok(found);
                            }
                        }
                    }
                    let next = page["next"].as_str().map(str::to_owned);
                    if rows.is_empty() || next.is_none() || next == after {
                        return Ok(found);
                    }
                    after = next;
                }
            }
            "cursor-origin" => {
                let page = self
                    .http(
                        cwd,
                        head,
                        &self.repo,
                        &format!("?head={branch}&state={state}&pageSize=20&sortBy=updated"),
                    )
                    .await?;
                page["pullRequests"].as_array().cloned().unwrap_or_default()
            }
            "bitbucket" => {
                let states: &[&str] = if open_only {
                    &["OPEN"]
                } else {
                    &["OPEN", "MERGED", "DECLINED", "SUPERSEDED"]
                };
                let filters = states
                    .iter()
                    .map(|s| format!("state = \"{s}\""))
                    .collect::<Vec<_>>()
                    .join(" OR ");
                let filter = format!("source.branch.name = {} AND ({filters})", json!(head));
                let query = url::form_urlencoded::Serializer::new(String::new())
                    .append_pair("pagelen", "20")
                    .append_pair("sort", "-updated_on")
                    .append_pair("q", &filter)
                    .extend_pairs(states.iter().map(|s| ("state", s)))
                    .finish();
                let page = self
                    .http(cwd, head, &self.repo, &format!("?{query}"))
                    .await?;
                page["values"].as_array().cloned().ok_or_else(invalid)?
            }
            _ => return Ok(vec![]),
        };
        Ok(rows
            .iter()
            .filter_map(|r| self.normalize(r, &self.repo))
            .collect())
    }
    pub(super) fn normalize(&self, v: &Value, target: &str) -> Option<Value> {
        let head_repo = match self.kind {
            "gitlab" => v
                .pointer("/source_project/path_with_namespace")
                .and_then(Value::as_str),
            "bitbucket" => v
                .pointer("/source/repository/full_name")
                .and_then(Value::as_str),
            "forgejo" => v.pointer("/head/repo/full_name").and_then(Value::as_str),
            _ => None,
        };
        let get = |p: &str| v.pointer(p)?.as_str();
        let id = |p: &str| v[p].as_u64().or_else(|| v[p].as_str()?.parse().ok());
        let (n, url, base, head, sha, state) = match self.kind {
            "gitlab" => (
                id("iid")?,
                get("/web_url")?.to_owned(),
                get("/target_branch")?,
                get("/source_branch")?,
                get("/sha"),
                get("/state").unwrap_or("open"),
            ),
            "bitbucket" => (
                id("id")?,
                get("/links/html/href")?.into(),
                get("/destination/branch/name")?,
                get("/source/branch/name")?,
                get("/source/commit/hash"),
                get("/state").unwrap_or("open"),
            ),
            "azure-devops" => {
                let n = id("pullRequestId")?;
                let url = get("/_links/web/href")
                    .map(str::to_owned)
                    .or_else(|| {
                        Some(format!(
                            "{}/pullrequest/{n}",
                            get("/repository/webUrl")?.trim_end_matches('/')
                        ))
                    })
                    .or_else(|| get("/url").map(str::to_owned))?;
                let trim = |s: &'static str| {
                    let v = get(s)?;
                    Some(v.strip_prefix("refs/heads/").unwrap_or(v))
                };
                (
                    n,
                    url,
                    trim("/targetRefName")?,
                    trim("/sourceRefName")?,
                    get("/lastMergeSourceCommit/commitId"),
                    get("/status")?,
                )
            }
            "cursor-origin" => (
                id("number")?,
                get("/webUrl")?.into(),
                get("/base/ref")?,
                get("/head/ref")?,
                get("/head/sha"),
                get("/state").unwrap_or("open"),
            ),
            "forgejo" => {
                let head = get("/head/ref")?;
                let head = if head.starts_with("refs/pull/")
                    && v["flow"] != 1
                    && !v["head"]["repo"].is_null()
                {
                    get("/head/label").unwrap_or(head)
                } else {
                    head
                };
                (
                    id("number")?,
                    get("/html_url")?.into(),
                    get("/base/ref")?,
                    head,
                    get("/head/sha"),
                    get("/state")?,
                )
            }
            "gitcafe" => (
                id("number")?,
                format!("https://{}/{target}/pulls/{}", self.host, id("number")?),
                get("/targetBranch")?,
                get("/sourceBranch")?,
                get("/headOid"),
                get("/state")?,
            ),
            _ => return None,
        };
        let source = if self.kind == "gitcafe" {
            get("/sourceRepo/owner")
                .zip(get("/sourceRepo/name"))
                .map(|(o, n)| format!("{o}/{n}"))
        } else {
            head_repo.map(str::to_owned)
        };
        let cross = match self.kind {
            "gitlab"
                if v["source_project_id"].is_number() && v["target_project_id"].is_number() =>
            {
                v["source_project_id"] != v["target_project_id"]
            }
            "azure-devops" => !v["forkSource"].is_null(),
            _ => source
                .as_deref()
                .is_some_and(|s| !s.eq_ignore_ascii_case(target)),
        };
        let state = if v["merged"] == true || v["mergedAt"].is_string() {
            "merged"
        } else {
            match state.to_lowercase().as_str() {
                "merged" | "completed" => "merged",
                "closed" | "declined" | "superseded" | "abandoned" => "closed",
                _ => "open",
            }
        };
        let draft = match self.kind {
            "forgejo" => v["draft"].as_bool().unwrap_or_else(|| {
                v["title"].as_str().is_some_and(|t| {
                    t.to_ascii_uppercase().starts_with("WIP:")
                        || t.to_ascii_uppercase().starts_with("[WIP]")
                })
            }),
            "gitlab" => v["draft"] == true || v["work_in_progress"] == true,
            "azure-devops" => v["isDraft"] == true,
            _ => v["draft"] == true,
        };
        let updated = match self.kind {
            "azure-devops" => get("/closedDate").or_else(|| get("/creationDate")),
            "bitbucket" => get("/updated_on"),
            "gitlab" | "forgejo" => get("/updated_at"),
            _ => get("/updatedAt"),
        };
        let row = json!({"updatedAt":updated,"isDraft":draft,"number":n,"title":v["title"],"url":url,"baseRefName":base,"headRefName":head,"headRefOid":sha,"state":state,"isCrossRepository":cross,"headRepository":{"nameWithOwner":source},"headRepositoryOwner":{"login":source.as_deref().and_then(|s|s.split('/').next())}});
        resolved(&row).map(|_| row)
    }
    pub async fn local_checkout(
        &self,
        cwd: &str,
        reference: &str,
        number: u64,
        remote: &str,
    ) -> Result<bool, Value> {
        let op = "checkoutChangeRequest";
        let n = number.to_string();
        match self.kind {
            "gitlab" => {
                self.command(
                    cwd,
                    op,
                    reference,
                    "glab",
                    &["mr", "checkout", "--", reference],
                    None,
                )
                .await?;
            }
            "cursor-origin" => {
                self.command(
                    cwd,
                    op,
                    reference,
                    "origin",
                    &["pr", "checkout", "-R", &self.repo, "--remote", remote, &n],
                    None,
                )
                .await?;
            }
            "forgejo" => {
                let (login, _) = self.tea_login(cwd, reference).await?;
                self.command(
                    cwd,
                    op,
                    reference,
                    "tea",
                    &[
                        "pulls", "checkout", "--login", &login, "--repo", &self.repo, "--branch",
                        &n,
                    ],
                    None,
                )
                .await?;
            }
            _ => return Ok(false),
        }
        Ok(true)
    }
    pub fn matches(&self, url: &str, repo: &str) -> bool {
        Self::parse(url).is_some_and(|r| r.host == self.host && r.repo.eq_ignore_ascii_case(repo))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn cursor_origin_decodes_64bit_strings_and_merged_state() {
        let remote = Remote::parse("git@origin.cursor.com:org/repo.git").unwrap();
        assert_eq!(
            remote.number("https://cursor.com/codebase/org/repo/pull/17"),
            Some(("org/repo".into(), 17))
        );
        assert!(remote
            .number("https://unknown.test/org/repo/pull/17")
            .is_none());
        let row=remote.normalize(&json!({"number":"17","title":"Update","webUrl":"https://cursor.com/codebase/org/repo/pull/17","state":"closed","merged":true,"base":{"ref":"main"},"head":{"ref":"feature","sha":"abcd"}}),"org/repo").unwrap();
        assert_eq!(row["headRefOid"], "abcd");
        assert_eq!(
            resolved(&row).unwrap(),
            json!({"number":17,"title":"Update","url":"https://cursor.com/codebase/org/repo/pull/17","baseBranch":"main","headBranch":"feature","state":"merged"})
        );
    }
    #[test]
    fn gitcafe_decodes_fork_identity_from_source_repository() {
        let remote = Remote::parse("ssh@staging.git.cafe:org/repo.git").unwrap();
        let row=remote.normalize(&json!({"number":5,"title":"Fix","sourceBranch":"feature","targetBranch":"main","state":"open","headOid":"abc","sourceRepo":{"owner":"contributor","name":"fork"}}),"org/repo").unwrap();
        assert_eq!(row["url"], "https://staging.git.cafe/org/repo/pulls/5");
        assert_eq!(row["isCrossRepository"], true);
        assert_eq!(row["headRepository"]["nameWithOwner"], "contributor/fork");
        assert_eq!(row["headRepositoryOwner"]["login"], "contributor");
        assert!(Remote::parse("https://git.cafe.unknown.test/org/repo").is_none());
    }
}
