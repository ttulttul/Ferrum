use super::{error, provider_error, query_pr, required, Failure, Github, Reply, OP};
use serde_json::{json, Value};
use std::collections::HashSet;

const LIMIT: usize = 1_000;
const PR_HEAD: &str = "query($owner:String!,$name:String!,$number:Int!){repository(owner:$owner,name:$name){pullRequest(number:$number){isCrossRepository headRefName headRefOid headRepositoryOwner{login}}}}";

#[derive(PartialEq, Eq)]
struct Head {
    sha: String,
    branch: String,
    owner: String,
}

async fn head(gh: &Github<'_>, vars: &Value, number: u64) -> Result<Option<Head>, Value> {
    let pr = query_pr(gh, PR_HEAD, vars).await?;
    match pr["isCrossRepository"].as_bool() {
        Some(false) => return Ok(None),
        Some(true) => (),
        None => return Err(provider_error(Failure::Invalid)),
    }
    let missing = |_| {
        error(
            OP,
            &format!("GitHub did not report a complete head revision for #{number}."),
        )
    };
    Ok(Some(Head {
        sha: required(&pr, "headRefOid").map_err(missing)?.to_owned(),
        branch: required(&pr, "headRefName").map_err(missing)?.to_owned(),
        owner: required(&pr["headRepositoryOwner"], "login")
            .map_err(missing)?
            .to_ascii_lowercase(),
    }))
}

async fn verify_head(gh: &Github<'_>, repo: &str, number: u64, head: &Head) -> Result<(), Value> {
    let (owner, name) = repo.split_once('/').unwrap();
    let mut vars = json!({"owner":owner,"name":name,"head":head.branch});
    let mut matches = Vec::new();
    let mut count = 0;
    let mut cursors = HashSet::new();
    loop {
        let data = gh
            .graphql(include_str!("../PullRequestHeads.graphql"), vars.clone())
            .await
            .map_err(provider_error)?;
        let connection = &data["repository"]["pullRequests"];
        let nodes = connection["nodes"]
            .as_array()
            .ok_or_else(|| provider_error(Failure::Invalid))?;
        count += nodes.len();
        if count > LIMIT || cursors.len() > LIMIT / 100 {
            return Err(error(
                OP,
                &format!("GitHub returned more than {LIMIT} pull requests for this head branch."),
            ));
        }
        for pr in nodes {
            if pr["headRefOid"] == head.sha
                && pr["isCrossRepository"] == true
                && pr["headRepositoryOwner"]["login"]
                    .as_str()
                    .is_some_and(|v| v.eq_ignore_ascii_case(&head.owner))
            {
                matches.push(pr["number"].as_u64());
            }
        }
        match connection["pageInfo"]["hasNextPage"].as_bool() {
            Some(false) => break,
            Some(true) => {
                let cursor = required(&connection["pageInfo"], "endCursor")?;
                if !cursors.insert(cursor.to_owned()) {
                    return Err(provider_error(Failure::Invalid));
                }
                vars["after"] = json!(cursor);
            }
            None => return Err(provider_error(Failure::Invalid)),
        }
    }
    if matches != [Some(number)] {
        return Err(error(OP,&format!("The head revision matched {} pull requests instead of uniquely matching #{number}.",matches.len())));
    }
    Ok(())
}

async fn runs(gh: &Github<'_>, repo: &str, head: &Head) -> Result<Vec<Value>, Value> {
    let mut found = Vec::new();
    let mut query = reqwest::Url::parse("https://api.github.com").expect("fixed URL");
    query
        .query_pairs_mut()
        .append_pair("head_sha", &head.sha)
        .append_pair("branch", &head.branch)
        .append_pair("event", "pull_request")
        .append_pair("status", "action_required")
        .append_pair("per_page", "100");
    for page in 1.. {
        let url = format!(
            "repos/{repo}/actions/runs?{}&page={page}",
            query.query().unwrap()
        );
        let response = gh.json("GET", &url, None).await.map_err(provider_error)?;
        let runs = response["workflow_runs"]
            .as_array()
            .ok_or_else(|| provider_error(Failure::Invalid))?;
        for run in runs {
            if !run["id"].is_u64() {
                return Err(provider_error(Failure::Invalid));
            }
            found.push(run.clone());
        }
        if found.len() > LIMIT {
            return Err(error(
                OP,
                &format!("GitHub returned more than {LIMIT} workflow runs awaiting approval."),
            ));
        }
        if runs.len() < 100 {
            break;
        }
    }
    Ok(found)
}

async fn pending(gh: &Github<'_>, repo: &str, number: u64, head: &Head) -> Result<Vec<u64>, Value> {
    verify_head(gh, repo, number, head).await?;
    Ok(runs(gh, repo, head)
        .await?
        .iter()
        .filter_map(|r| r["id"].as_u64())
        .collect())
}

/// The check reader needs both the fork identity and the names of approval-gated runs.
pub(in crate::pull_requests) async fn fork_runs(
    gh: &Github<'_>,
    repo: &str,
    number: u64,
    pr: &Value,
) -> Result<Vec<Value>, Value> {
    let head = Head {
        sha: required(pr, "headRefOid")?.into(),
        branch: required(pr, "headRefName")?.into(),
        owner: required(&pr["headRepositoryOwner"], "login")?.into(),
    };
    let (_, runs) = tokio::try_join!(verify_head(gh, repo, number, &head), runs(gh, repo, &head))?;
    Ok(runs)
}

pub(super) async fn approve(gh: &Github<'_>, repo: &str, vars: &Value, number: u64) -> Reply {
    let Some(initial) = head(gh, vars, number).await? else {
        return Ok(Value::Null);
    };
    for id in pending(gh, repo, number, &initial).await? {
        if head(gh, vars, number).await?.as_ref() != Some(&initial) {
            return Err(error(OP,&format!("The head revision of #{number} changed before its workflows could be approved.")));
        }
        if pending(gh, repo, number, &initial).await?.contains(&id) {
            gh.request(
                "POST",
                &format!("repos/{repo}/actions/runs/{id}/approve"),
                "application/vnd.github+json",
                None,
            )
            .await
            .map_err(provider_error)?;
        }
    }
    Ok(Value::Null)
}
