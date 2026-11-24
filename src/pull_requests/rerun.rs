//! GitHub allows one rerun per workflow run at a time. Only use rerun-failed-jobs
//! when the complete list of failed jobs is exactly the user's selection.
use super::{detail, failure, PullRequestService};
use crate::provider_rpc::{Failure, Github, Reply, RequestError};
use futures_util::{stream, StreamExt, TryStreamExt};
use indexmap::IndexMap;
use regex::Regex;
use serde_json::{json, Value};
use std::{collections::HashSet, sync::LazyLock};

const OP: &str = "rerunChecks";
const NOT_JOB: &str = "This check is not a GitHub Actions job, so GitHub cannot re-run it.";
const ONE_JOB: &str =
    "GitHub re-runs one job of a run at a time. Re-run this one after the other finishes.";

fn job_id(url: &str) -> Option<&str> {
    static ACTIONS: LazyLock<Regex> =
        LazyLock::new(|| Regex::new(r"/runs/[0-9]+/jobs?/([0-9]+)(?:[/?#]|$)").unwrap());
    static LEGACY: LazyLock<Regex> = LazyLock::new(|| {
        Regex::new(r"^https://[^/]+/[^/]+/[^/]+/runs/([0-9]+)(?:[/?#]|$)").unwrap()
    });
    ACTIONS
        .captures(url)
        .or_else(|| LEGACY.captures(url))?
        .get(1)
        .map(|m| m.as_str())
}

fn per_job_detail(err: RequestError) -> Result<String, Value> {
    match (err.kind, err.http_status) {
        (Failure::NotFound, _) => Ok(
            "GitHub could not find the requested resource, or the credential cannot see it.".into(),
        ),
        (Failure::Refused, Some(code)) => {
            let body = err.response.unwrap_or_default();
            let mut messages = Vec::new();
            if let Some(message) = body["message"].as_str() {
                messages.push(message.to_owned());
            }
            for row in body["errors"].as_array().into_iter().flatten() {
                if let Some(s) = row.as_str().or(row["message"].as_str()) {
                    messages.push(s.to_owned());
                } else if let (Some(field), Some(code)) =
                    (row["field"].as_str(), row["code"].as_str())
                {
                    messages.push(format!("{field} {code}"));
                }
            }
            Ok(if messages.is_empty() {
                format!("GitHub returned HTTP {code}.")
            } else {
                format!("GitHub returned an error: {}", messages.join("; "))
            })
        }
        _ => Err(failure(OP, err.kind)),
    }
}

async fn request(
    gh: &Github<'_>,
    method: &str,
    path: &str,
) -> Result<Result<Value, String>, Value> {
    match gh
        .request_with_response(method, path, "application/vnd.github+json", None)
        .await
    {
        Ok(body) => Ok(Ok(serde_json::from_slice(&body).unwrap_or(Value::Null))),
        Err(err) => Ok(Err(per_job_detail(err)?)),
    }
}

fn all_selected(page: &Value, selected: &[&str]) -> bool {
    let (Some(total), Some(jobs)) = (page["total_count"].as_u64(), page["jobs"].as_array()) else {
        return false;
    };
    if total > jobs.len() as u64 {
        return false;
    }
    let mut failed = HashSet::new();
    for job in jobs {
        let (Some(id), Some(conclusion)) = (job["id"].as_u64(), job.get("conclusion")) else {
            return false;
        };
        if !conclusion.is_null() && !conclusion.is_string() {
            return false;
        }
        if matches!(
            conclusion.as_str(),
            Some("failure" | "cancelled" | "timed_out" | "startup_failure")
        ) {
            failed.insert(id.to_string());
        }
    }
    failed.len() == selected.len() && selected.iter().all(|id| failed.contains(*id))
}

async fn rerun_group<'a>(
    gh: &Github<'_>,
    root: &str,
    run: u64,
    jobs: Vec<&'a str>,
) -> Result<Vec<(&'a str, Option<String>)>, Value> {
    if jobs.len() > 1 {
        let page = request(
            gh,
            "GET",
            &format!("{root}/runs/{run}/jobs?filter=latest&per_page=100"),
        )
        .await?;
        if page.as_ref().is_ok_and(|p| all_selected(p, &jobs)) {
            let detail = request(gh, "POST", &format!("{root}/runs/{run}/rerun-failed-jobs"))
                .await?
                .err();
            return Ok(jobs.into_iter().map(|id| (id, detail.clone())).collect());
        }
    }
    let first = jobs[0];
    let detail = request(gh, "POST", &format!("{root}/jobs/{first}/rerun"))
        .await?
        .err();
    let mut outcomes = vec![(first, detail)];
    outcomes.extend(
        jobs.into_iter()
            .skip(1)
            .map(|id| (id, Some(ONE_JOB.into()))),
    );
    Ok(outcomes)
}

async fn rerun(gh: &Github<'_>, root: &str, selected: &[Value]) -> Reply {
    let located: Vec<_> = stream::iter(
        selected
            .iter()
            .map(|check| async move {
                let id = job_id(check["url"].as_str().unwrap_or(""));
                let (run, detail) = if let Some(id) = id {
                    match request(gh, "GET", &format!("{root}/jobs/{id}")).await? {
                        Ok(job) => match job["run_id"].as_u64() {
                            Some(run) => (Some(run), None),
                            None => (None, Some(NOT_JOB.into())),
                        },
                        Err(message) => (None, Some(message)),
                    }
                } else {
                    (None, Some(NOT_JOB.into()))
                };
                Ok::<_, Value>((id, run, detail))
            })
            .collect::<Vec<_>>(),
    )
    .buffered(4)
    .try_collect()
    .await?;
    let mut groups = IndexMap::<u64, Vec<&str>>::new();
    for (id, run, _) in &located {
        if let (Some(id), Some(run)) = (id, run) {
            groups.entry(*run).or_default().push(id);
        }
    }
    let results: Vec<_> = stream::iter(
        groups
            .into_iter()
            .map(|(run, jobs)| rerun_group(gh, root, run, jobs))
            .collect::<Vec<_>>(),
    )
    .buffered(2)
    .try_collect()
    .await?;
    let outcomes: IndexMap<_, _> = results.into_iter().flatten().collect();
    let checks = selected
        .iter()
        .zip(located)
        .map(|(check, (id, run, detail))| {
            let detail = detail.or_else(|| outcomes.get(id?).cloned().flatten());
            json!({
                "name": check["name"],
                "rerun": run.is_some() && detail.is_none(),
                "detail": detail,
            })
        });
    Ok(Value::Array(checks.collect()))
}

fn add_workflow(checks: &mut Vec<Value>, run: &Value, status: &str) {
    let Some(id) = run["id"].as_u64() else { return };
    let represented = checks.iter().any(|check| {
        let relevant = check["status"] == "action-required"
            || (status == "pending" && check["status"] == "pending");
        relevant
            && check["url"]
                .as_str()
                .and_then(|url| url.split_once("/actions/runs/"))
                .and_then(|(_, path)| path.split(['/', '?', '#']).next()?.parse::<u64>().ok())
                == Some(id)
    });
    if !represented {
        let name = run["name"]
            .as_str()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map_or_else(|| format!("Workflow run {id}"), str::to_owned);
        checks.push(json!({"name":name,"url":run["html_url"],"status":status}));
    }
}

pub(super) async fn run(
    service: &PullRequestService,
    gh: &Github<'_>,
    project: &crate::Project,
    repo: &str,
    number: u64,
    input: &Value,
) -> Reply {
    let root = format!("repos/{repo}/actions");
    let (data, contexts) = detail::read_core(gh, OP, repo, number).await?;
    let pr = &data["repository"]["pullRequest"];
    let mut checks = detail::check_rows(contexts);
    if pr["state"] == "OPEN" {
        let approvals = async {
            if pr["isCrossRepository"] == true {
                super::actions::fork_runs(gh, repo, number, pr).await
            } else {
                Ok(Vec::new())
            }
        };
        let unfinished = async {
            let Some(sha) = pr["headRefOid"].as_str() else {
                return Ok(Value::Null);
            };
            let path = format!(
                "{root}/runs?head_sha={}&per_page=100",
                crate::asset_grant::encode_component(sha)
            );
            match gh.json("GET", &path, None).await {
                Err(Failure::RateLimit) => Err(failure(OP, Failure::RateLimit)),
                result => Ok(result.unwrap_or(Value::Null)),
            }
        };
        let (approvals, unfinished) = tokio::join!(approvals, unfinished);
        let unfinished = unfinished?;
        for run in unfinished["workflow_runs"]
            .as_array()
            .into_iter()
            .flatten()
            .filter(|r| {
                matches!(
                    r["status"].as_str(),
                    Some("requested" | "queued" | "pending" | "waiting" | "in_progress")
                )
            })
        {
            add_workflow(&mut checks, run, "pending");
        }
        match approvals {
            Ok(runs) => {
                for run in runs {
                    add_workflow(&mut checks, &run, "action-required");
                }
            }
            Err(e) if e["detail"] == "GitHub API rate limit exceeded." => {
                return Err(failure(OP, Failure::RateLimit))
            }
            Err(_) => checks.push(
                json!({"name":"Workflow approval status","url":null,"status":"action-required"}),
            ),
        }
    }
    let selected = detail::select(&checks, input, OP)?;
    if selected.is_empty() {
        return Ok(json!([]));
    }
    let result = rerun(gh, &root, &selected).await;
    service.clear(&project.id, repo, number).await;
    result
}
