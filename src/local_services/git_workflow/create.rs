//! Push the caller's branch and open its PR using the supplied title and body.
use super::*;
use crate::pull_requests::bitbucket_repository_json;

struct Forge<'a> {
    cwd: &'a str,
    repo: String,
    gh: Option<Github<'a>>,
    bb: Option<hosts::Remote>,
}

impl Forge<'_> {
    async fn base(&self, branch: &str, upstream: Option<&str>, cross: bool) -> String {
        if let Some(base) = read_branch_base(self.cwd, branch).await {
            return base;
        }
        if !cross {
            if let Some(name) = upstream
                .and_then(|s| s.split_once('/'))
                .map(|(_, b)| b)
                .filter(|b| *b != branch)
            {
                return name.into();
            }
        }
        let default = if let Some(gh) = &self.gh {
            gh.json("GET", &format!("repos/{}", self.repo), None)
                .await
                .ok()
                .and_then(|r| r["default_branch"].as_str().map(str::to_owned))
        } else if self.bb.is_some() {
            let path = format!("/repositories/{}", self.repo);
            let model = format!("{path}/branching-model");
            let (repo, model) = tokio::join!(
                bitbucket_repository_json("GET", &path, None),
                bitbucket_repository_json("GET", &model, None)
            );
            let model = model.unwrap_or_default();
            let dev = &model["development"];
            let branch = dev["branch"]["name"]
                .as_str()
                .or(dev["name"].as_str())
                .filter(|s| {
                    !s.is_empty()
                        && *s != "null"
                        && dev["is_valid"] != false
                        && dev["use_mainbranch"] != true
                });
            branch.map(str::to_owned).or_else(|| {
                repo.ok()?
                    .pointer("/mainbranch/name")?
                    .as_str()
                    .map(str::to_owned)
            })
        } else {
            None
        };
        match default {
            Some(base) => base,
            None => default_ref(self.cwd).await.unwrap_or_else(|| "main".into()),
        }
    }

    async fn find(&self, head: &str, source: &str) -> Result<Option<Value>, Value> {
        let rows = if let Some(gh) = &self.gh {
            let (owner, name) = self.repo.split_once('/').unwrap();
            let query = include_str!("PullRequestsByHead.graphql");
            let data = gh
                .graphql(
                    query,
                    json!({"owner":owner,"name":name,"head":head,"states":["OPEN"],"first":100}),
                )
                .await
                .map_err(|e| github_error(e, "listChangeRequests", self.cwd, head))?;
            data["repository"]["pullRequests"]["nodes"]
                .as_array()
                .cloned()
                .ok_or_else(|| {
                    github_error(Failure::Invalid, "listChangeRequests", self.cwd, head)
                })?
        } else if let Some(bb) = &self.bb {
            let filter = format!("source.branch.name = {} AND state = \"OPEN\"", json!(head));
            let query = url::form_urlencoded::Serializer::new(String::new())
                .append_pair("pagelen", "1")
                .append_pair("sort", "-updated_on")
                .append_pair("q", &filter)
                .append_pair("state", "OPEN")
                .finish();
            let path = format!("/repositories/{}/pullrequests?{query}", self.repo);
            let page = bitbucket_repository_json("GET", &path, None)
                .await
                .map_err(|e| {
                    provider_error("bitbucket", "listChangeRequests", self.cwd, head, &e)
                })?;
            page["values"]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(|r| bb.normalize(r, &self.repo))
                .collect()
        } else {
            return Err(provider_error(
                "unknown",
                "listChangeRequests",
                self.cwd,
                head,
                "No unknown source control provider is registered.",
            ));
        };
        Ok(rows.into_iter().find(|r| {
            let from = r["headRepository"]["nameWithOwner"]
                .as_str()
                .or(r["headRepositoryNameWithOwner"].as_str());
            r["headRefName"].as_str().or(r["headBranch"].as_str()) == Some(head)
                && from.is_none_or(|s| s.eq_ignore_ascii_case(source))
                && (self.repo.eq_ignore_ascii_case(source) || from.is_some())
        }))
    }

    async fn open(&self, head: &str, source: &str, base: &str, input: &Value) -> Result<(), Value> {
        let cross = !self.repo.eq_ignore_ascii_case(source);
        let owner = source.split('/').next().unwrap_or("");
        if let Some(gh) = &self.gh {
            let selector = if cross {
                format!("{owner}:{head}")
            } else {
                head.into()
            };
            let mut body = json!({"base":base,"head":selector,"title":input["title"],"body":input["body"],"maintainer_can_modify":true});
            if input["draft"] == true {
                body["draft"] = json!(true)
            }
            gh.json("POST", &format!("repos/{}/pulls", self.repo), Some(body))
                .await
                .map_err(|e| github_error(e, "createChangeRequest", self.cwd, &selector))?;
        } else {
            let mut body = json!({"title":input["title"],"description":input["body"],"source":{"branch":{"name":head}},"destination":{"branch":{"name":base}}});
            if cross {
                body["source"]["repository"] = json!({"full_name":source})
            }
            bitbucket_repository_json(
                "POST",
                &format!("/repositories/{}/pullrequests", self.repo),
                Some(body),
            )
            .await
            .map_err(|detail| {
                provider_error("bitbucket", "createChangeRequest", self.cwd, head, &detail)
            })?;
        }
        Ok(())
    }
}

pub(super) async fn run(tools: &GitTools, cwd: &str, input: &Value) -> Reply {
    let primary = primary(cwd).await;
    let remote = primary.as_ref().map(|r| r.0.as_str()).unwrap_or("origin");
    let url = read(cwd, &["remote", "get-url", "--", remote])
        .await
        .unwrap_or_default();
    let bb = hosts::Remote::parse(&url).filter(|r| r.kind == "bitbucket");
    let remotes = read(cwd, &["remote"]).await.unwrap_or_default();
    let gh_target = if scope(&url).is_some() {
        status::github_target(cwd, &remotes, &url).await
    } else {
        None
    };
    let gh = gh_target.as_ref().map(|_| Github::new(&tools.gh, cwd));
    let repo = gh_target
        .as_deref()
        .and_then(|s| s.split_once('/'))
        .map(|(_, repo)| repo.to_owned())
        .or_else(|| bb.as_ref().map(|b| b.repo.clone()))
        .unwrap_or_default();
    let forge = Forge { cwd, repo, gh, bb };
    if input["private"] == true {
        let (name, terms) = if forge.gh.is_some() {
            ("GitHub", "pull requests")
        } else if forge.bb.is_some() {
            ("Bitbucket", "pull requests")
        } else {
            ("Unknown", "change requests")
        };
        return Err(manager(cwd,"createPullRequest",&format!("{name} has no private {terms}. Nothing was pushed. Leave out private to open a public one.")));
    }
    let branch = branch(cwd).await.ok_or_else(|| {
        manager(
            cwd,
            "runPrStep",
            "Cannot create a pull request from detached HEAD.",
        )
    })?;
    let upstream = read(
        cwd,
        &[
            "rev-parse",
            "--abbrev-ref",
            "--symbolic-full-name",
            "@{upstream}",
        ],
    )
    .await;
    let tracking = read(
        cwd,
        &["config", "--get", &format!("branch.{branch}.remote")],
    )
    .await;
    let push_remote = read(
        cwd,
        &["config", "--get", &format!("branch.{branch}.pushRemote")],
    )
    .await
    .or(read(cwd, &["config", "--get", "remote.pushDefault"]).await);
    let tracked = tracking
        .as_ref()
        .and_then(|r| upstream.as_deref()?.strip_prefix(&format!("{r}/")));
    let separate = push_remote.is_some() && push_remote != tracking;
    let dest = push_remote
        .as_deref()
        .or(tracking.as_deref().filter(|_| tracked == Some(&branch)))
        .unwrap_or(remote);
    let dest_url = read(cwd, &["remote", "get-url", "--", dest])
        .await
        .unwrap_or_default();
    let source = scope(&dest_url)
        .or_else(|| hosts::Remote::parse(&dest_url).map(|r| r.repo))
        .unwrap_or_else(|| forge.repo.clone());
    let cross = !source.eq_ignore_ascii_case(&forge.repo);
    let needs_push = upstream.is_none()
        || separate
        || read(cwd, &["rev-list", "--count", "@{upstream}..HEAD"])
            .await
            .as_deref()
            != Some("0");
    let mut head = tracked.unwrap_or(&branch).to_owned();
    if needs_push {
        let base = forge.base(&branch, upstream.as_deref(), cross).await;
        let publish = branch.strip_prefix(&format!("{dest}/")).unwrap_or(&branch);
        if !cross && base == publish && forge.find(publish, &source).await?.is_none() {
            return Err(manager(
                cwd,
                "runStackedAction",
                &format!("{base} is the base branch. Check out a feature branch to create a PR."),
            ));
        }
        let push = stacked::push(cwd, &branch, primary.as_ref().map(|r| r.0.as_str()))
            .await
            .map_err(|e| {
                manager(
                    cwd,
                    "createPullRequest",
                    &format!(
                        "Could not push {branch}: {}",
                        e["detail"].as_str().unwrap_or("git push failed")
                    ),
                )
            })?;
        if upstream.is_none() && push["status"] == "pushed" && push["setUpstream"] == false {
            return Err(manager(cwd,"createPullRequest",&format!("Pushed {branch} to {}, but could not record it as the branch's upstream. Set the upstream, then try again.",push["upstreamBranch"].as_str().unwrap_or(""))));
        }
        head = push["upstreamBranch"]
            .as_str()
            .and_then(|s| s.strip_prefix(&format!("{dest}/")))
            .unwrap_or(publish)
            .to_owned();
    }
    if let Some(pr) = forge.find(&head, &source).await? {
        return Ok(json!({"url":pr["url"],"created":false}));
    }
    let resolved = forge.base(&branch, upstream.as_deref(), cross).await;
    forge
        .open(
            &head,
            &source,
            input["base"].as_str().unwrap_or(&resolved),
            input,
        )
        .await?;
    let listed = forge
        .find(&head, &source)
        .await
        .ok()
        .flatten()
        .unwrap_or_default();
    Ok(json!({"url":listed["url"],"created":true,"draftOnCreate":forge.gh.is_some()}))
}
