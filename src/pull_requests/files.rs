use super::{error, failure};
use crate::provider_rpc::{Failure, Github, Reply};
use regex::Regex;
use serde_json::{json, Value};
use std::sync::LazyLock;

fn category(path: &str) -> &'static str {
    static GENERATED: LazyLock<Regex> = LazyLock::new(|| {
        Regex::new(r"(\.generated\.|\.gen\.|\.g\.dart$|\.pb\.go$|_pb2(_grpc)?\.py$|\.pb\.(cc|h)$|\.min\.(js|css)$|\.(js|css)\.map$)").unwrap()
    });
    static MOCK: LazyLock<Regex> = LazyLock::new(|| {
        Regex::new(r"(\.snap$|\.(mocks?|fixtures?|stubs?|fakes?)\.[^.]+$|^(mocks?|fixtures?|stubs?|fakes?)([-_.A-Z]|$))").unwrap()
    });
    static TEST: LazyLock<Regex> = LazyLock::new(|| {
        Regex::new(r"(?i)(\.(test|spec|e2e|eval|stories)\.[^.]+$|_(test|spec)\.[^.]+$|^test_.+\.py$|^conftest\.py$|^(vitest|jest)\.setup\.[^.]+$)").unwrap()
    });
    static CLASS_TEST: LazyLock<Regex> = LazyLock::new(|| {
        Regex::new(r"(Tests?|Spec)\.(java|kt|kts|scala|groovy|cs|php|swift|m)$").unwrap()
    });
    static DOCS: LazyLock<Regex> =
        LazyLock::new(|| Regex::new(r"(?i)\.(md|mdx|markdown|rst|adoc|txt)$").unwrap());
    static NOT_DOCS: LazyLock<Regex> = LazyLock::new(|| {
        Regex::new(r"(?i)^(requirements.*|constraints.*|CMakeLists|robots|llms)\.txt$").unwrap()
    });
    let parts = path.split(['/', '\\']).collect::<Vec<_>>();
    let (name, dirs) = parts.split_last().unwrap();
    let in_dirs = |names: &[&str]| {
        dirs.iter()
            .any(|d| names.iter().any(|s| s.eq_ignore_ascii_case(d)))
    };
    if matches!(
        name.to_ascii_lowercase().as_str(),
        "bun.lock"
            | "bun.lockb"
            | "cargo.lock"
            | "composer.lock"
            | "deno.lock"
            | "flake.lock"
            | "gemfile.lock"
            | "go.sum"
            | "gradle.lockfile"
            | "mix.lock"
            | "npm-shrinkwrap.json"
            | "package-lock.json"
            | "packages.lock.json"
            | "pipfile.lock"
            | "pnpm-lock.yaml"
            | "podfile.lock"
            | "poetry.lock"
            | "pubspec.lock"
            | "uv.lock"
            | "yarn.lock"
    ) || GENERATED.is_match(name)
        || in_dirs(&["__generated__", "_generated", "generated"])
    {
        "generated"
    } else if MOCK.is_match(name)
        || in_dirs(&[
            "__fixtures__",
            "__mocks__",
            "__snapshots__",
            "fakes",
            "fixture",
            "fixtures",
            "mock",
            "mocks",
            "stubs",
            "testdata",
        ])
    {
        "mocks"
    } else if TEST.is_match(name)
        || CLASS_TEST.is_match(name)
        || in_dirs(&[
            "__tests__",
            "__test__",
            "cypress",
            "e2e",
            "evals",
            "spec",
            "specs",
            "test",
            "tests",
        ])
    {
        "tests"
    } else if DOCS.is_match(name) && !NOT_DOCS.is_match(name) {
        "docs"
    } else {
        "code"
    }
}

async fn set(gh: &Github<'_>, vars: Value, input: &Value) -> Reply {
    const OP: &str = "setFilesViewed";
    let invalid = || error(OP, "Invalid viewed file.");
    let files = input["files"]
        .as_array()
        .filter(|f| f.len() <= 500)
        .ok_or_else(invalid)?;
    let toggles = files
        .iter()
        .map(|f| {
            let path = f["path"]
                .as_str()
                .filter(|p| !p.is_empty() && p.encode_utf16().count() <= 4096)
                .ok_or_else(invalid)?;
            let viewed = f["viewed"].as_bool().ok_or_else(invalid)?;
            Ok((path, viewed))
        })
        .collect::<Result<Vec<_>, Value>>()?;
    if toggles.is_empty() {
        return Ok(Value::Null);
    }
    let resp = gh
        .graphql(include_str!("PullRequestNodeId.graphql"), vars)
        .await
        .map_err(|f| failure(OP, f))?;
    let pr = resp["repository"]["pullRequest"]["id"]
        .as_str()
        .ok_or_else(|| failure(OP, Failure::NotFound))?;
    // GitHub executes top-level mutation fields in document order, including repeated paths.
    for chunk in toggles.chunks(100) {
        let mut declarations = String::from("$pr:ID!");
        let mut fields = String::new();
        let mut vars = json!({"pr":pr});
        for (i, (path, viewed)) in chunk.iter().enumerate() {
            let method = if *viewed {
                "markFileAsViewed"
            } else {
                "unmarkFileAsViewed"
            };
            declarations.push_str(&format!(",$p{i}:String!"));
            fields.push_str(&format!(
                "f{i}:{method}(input:{{pullRequestId:$pr,path:$p{i}}}){{clientMutationId}} "
            ));
            vars[format!("p{i}")] = json!(path);
        }
        let query = format!("mutation({declarations}){{{fields}}}");
        gh.graphql(&query, vars).await.map_err(|f| failure(OP, f))?;
    }
    Ok(Value::Null)
}

pub(super) async fn run(
    gh: &Github<'_>,
    repo: &str,
    number: u64,
    op: &str,
    input: &Value,
) -> Reply {
    let (owner, name) = repo.split_once('/').unwrap();
    let mut vars = json!({"owner":owner,"name":name,"number":number});
    if op == "setFilesViewed" {
        return set(gh, vars, input).await;
    }
    let stats = op == "changeBreakdown";
    let query = if stats {
        include_str!("PullRequestFileStats.graphql")
    } else {
        include_str!("PullRequestFilesViewed.graphql")
    };
    let mut files = Vec::new();
    let empty = json!({"additions":0,"deletions":0});
    let mut counts =
        json!({"code":empty,"tests":empty,"mocks":empty,"generated":empty,"docs":empty});
    let mut truncated = false;
    for _ in 0..5 {
        let data = gh
            .graphql(query, vars.clone())
            .await
            .map_err(|f| failure(op, f))?;
        let connection = &data["repository"]["pullRequest"]["files"];
        for file in connection["nodes"]
            .as_array()
            .into_iter()
            .flatten()
            .filter(|n| !n.is_null())
        {
            let path = file["path"]
                .as_str()
                .ok_or_else(|| failure(op, Failure::Invalid))?;
            if path.is_empty() {
                continue;
            }
            if stats {
                let bucket = &mut counts[category(path)];
                for key in ["additions", "deletions"] {
                    let increment = file[key]
                        .as_u64()
                        .ok_or_else(|| failure(op, Failure::Invalid))?;
                    bucket[key] = json!(bucket[key]
                        .as_u64()
                        .unwrap()
                        .checked_add(increment)
                        .ok_or_else(|| failure(op, Failure::Invalid))?);
                }
            } else {
                let state = match file["viewerViewedState"]
                    .as_str()
                    .unwrap_or("")
                    .trim()
                    .to_ascii_uppercase()
                    .as_str()
                {
                    "VIEWED" => "viewed",
                    "DISMISSED" => "dismissed",
                    _ => "unviewed",
                };
                files.push(json!({"path":path,"state":state}));
            }
        }
        truncated = connection["pageInfo"]["hasNextPage"] == true
            && connection["pageInfo"]["endCursor"].is_string();
        if !truncated {
            break;
        }
        vars["after"] = connection["pageInfo"]["endCursor"].clone();
    }
    if stats {
        return Ok(if truncated { Value::Null } else { counts });
    }
    Ok(json!({"files":files,"truncated":truncated}))
}
