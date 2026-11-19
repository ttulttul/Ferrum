use crate::AppState;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::path::Path;
use tokio::process::Command;

pub async fn call(state: &AppState, method: &str, input: &Value) -> Result<Value, Value> {
    match method {
        "getBranchCommits" => branch_commits(state, input).await,
        "getDiffPreview" => diff_preview(state, input).await,
        "getDiffFileContents" => diff_file_contents(state, input).await,
        _ => Err(json!({"_tag":"Die","defect":format!("Unknown review method: {method}")})),
    }
}
fn cwd(input: &Value) -> Result<&str, Value> {
    input
        .get("cwd")
        .and_then(Value::as_str)
        .filter(|s| !s.trim().is_empty())
        .ok_or_else(|| json!({"_tag":"Die","defect":"Missing key\n  at [\"cwd\"]"}))
}
async fn git(cwd: &str, args: &[String]) -> std::io::Result<std::process::Output> {
    Command::new("git")
        .current_dir(cwd)
        .args(args)
        .env("LC_ALL", "C")
        .env("GIT_TERMINAL_PROMPT", "0")
        .output()
        .await
}
fn git_error(operation: &str, cwd: &str, detail: &str) -> Value {
    json!({"_tag":"GitCommandError","operation":operation,"command":"git","cwd":cwd,"detail":detail})
}
fn iso_now() -> String {
    time::OffsetDateTime::now_utc()
        .format(time::macros::format_description!(
            "[year]-[month]-[day]T[hour]:[minute]:[second].[subsecond digits:3]Z"
        ))
        .unwrap_or_default()
}
fn hash_diff(diff: &str, files: &[Value]) -> String {
    let mut h = Sha256::new();
    h.update(crate::wire_json::compact(&json!([diff, files])));
    h.finalize().iter().map(|b| format!("{b:02x}")).collect()
}
fn parse_numstat(text: &str) -> Vec<Value> {
    let fields: Vec<&str> = text.split('\0').collect();
    let mut out = Vec::new();
    let mut i = 0;
    while i < fields.len() {
        let f = fields[i];
        let Some((a, rest)) = f.split_once('\t') else {
            i += 1;
            continue;
        };
        let Some((d, path0)) = rest.split_once('\t') else {
            i += 1;
            continue;
        };
        if path0.is_empty() && i + 2 < fields.len() {
            out.push(json!({"path":fields[i+2],"previousPath":fields[i+1],"additions":a.parse::<u64>().unwrap_or(0),"deletions":d.parse::<u64>().unwrap_or(0)}));
            i += 3;
        } else {
            out.push(json!({"path":path0,"previousPath":Value::Null,"additions":a.parse::<u64>().unwrap_or(0),"deletions":d.parse::<u64>().unwrap_or(0)}));
            i += 1;
        }
    }
    out
}
async fn branch_commits(_state: &AppState, input: &Value) -> Result<Value, Value> {
    let cwd = cwd(input)?;
    let p = git(
        cwd,
        &[
            "rev-parse".into(),
            "--verify".into(),
            "--quiet".into(),
            "HEAD".into(),
        ],
    )
    .await;
    if p.as_ref().is_err() || !p.as_ref().unwrap().status.success() {
        return Ok(json!({"commits":[],"truncated":false}));
    }
    let base = if let Some(b) = input.get("baseRef").and_then(Value::as_str) {
        b.to_owned()
    } else {
        let mut c = None;
        for b in ["origin/HEAD", "origin/main", "main", "master"] {
            if let Ok(o) = git(
                cwd,
                &[
                    "rev-parse".into(),
                    "--verify".into(),
                    format!("{b}^{{commit}}"),
                ],
            )
            .await
            {
                if o.status.success() {
                    c = Some(b.to_owned());
                    break;
                }
            }
        }
        c.unwrap_or_else(|| "HEAD".into())
    };
    let o = git(
        cwd,
        &[
            "log".into(),
            "-z".into(),
            "--no-notes".into(),
            "--no-show-signature".into(),
            "--format=%H%x00%s%x00%cI".into(),
            "--max-count=101".into(),
            format!("{base}..HEAD"),
        ],
    )
    .await
    .map_err(|_| {
        git_error(
            "GitVcsDriver.getReviewBranchCommits",
            cwd,
            "Could not read branch commit metadata.",
        )
    })?;
    if !o.status.success() {
        return Err(git_error(
            "GitVcsDriver.getReviewBranchCommits",
            cwd,
            "Could not read branch commit metadata.",
        ));
    }
    let t = String::from_utf8_lossy(&o.stdout);
    let f: Vec<&str> = t.split('\0').collect();
    let mut c = Vec::new();
    let mut i = 0;
    while i + 2 < f.len() && c.len() < 100 {
        if f[i].is_empty() {
            break;
        }
        c.push(json!({"sha":f[i],"subject":f[i+1].chars().take(300).collect::<String>(),"committedAt":f[i+2]}));
        i += 3
    }
    Ok(json!({"commits":c,"truncated":c.len()>=100}))
}
async fn diff_preview(_state: &AppState, input: &Value) -> Result<Value, Value> {
    let cwd = cwd(input)?;
    let p = git(cwd, &["rev-parse".into(), "--show-toplevel".into()]).await;
    if p.as_ref().is_err() || !p.as_ref().unwrap().status.success() {
        return Ok(json!({"cwd":cwd,"generatedAt":iso_now(),"sources":[]}));
    }
    let sk = input.get("sourceKind").and_then(Value::as_str);
    let mut a = vec![
        "-c".into(),
        "diff.suppressBlankEmpty=false".into(),
        "diff".into(),
        "--find-renames".into(),
        "--no-color".into(),
        "--no-ext-diff".into(),
        "--no-textconv".into(),
        "--submodule=short".into(),
        "--src-prefix=a/".into(),
        "--dst-prefix=b/".into(),
    ];
    if input["ignoreWhitespace"] == true {
        a.push("--ignore-all-space".into())
    }
    let mut base = None;
    for b in [
        input.get("baseRef").and_then(Value::as_str).unwrap_or(""),
        "origin/HEAD",
        "origin/main",
        "main",
        "master",
    ] {
        if b.is_empty() {
            continue;
        }
        if let Ok(o) = git(
            cwd,
            &[
                "rev-parse".into(),
                "--verify".into(),
                format!("{b}^{{commit}}"),
            ],
        )
        .await
        {
            if o.status.success() {
                base = Some(b.to_owned());
                break;
            }
        }
    }
    let branch_name = git(
        cwd,
        &[
            "symbolic-ref".into(),
            "--quiet".into(),
            "--short".into(),
            "HEAD".into(),
        ],
    )
    .await
    .ok()
    .filter(|o| o.status.success())
    .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_owned())
    .unwrap_or_else(|| "HEAD".into());
    let metadata_only = input["metadataOnly"] == true;
    let mut sources = Vec::new();
    let mut add = |id: &str,
                   kind: &str,
                   title: String,
                   br: Value,
                   hr: Value,
                   d: String,
                   fs: Vec<Value>| {
        sources.push(json!({"id":id,"kind":kind,"title":title,"baseRef":br,"headRef":hr,"diff":d,"files":fs,"diffHash":hash_diff(&d,&fs),"truncated":false}));
    };
    if let Some(sha) = input.get("commitSha").and_then(Value::as_str) {
        let parents = git(
            cwd,
            &[
                "rev-list".into(),
                "--parents".into(),
                "-n1".into(),
                sha.to_owned(),
            ],
        )
        .await
        .map_err(|_| {
            git_error(
                "GitVcsDriver.getReviewDiffPreview.commit",
                cwd,
                "Selected commit is invalid or no longer available.",
            )
        })?;
        if !parents.status.success() {
            return Err(git_error(
                "GitVcsDriver.reviewCommit",
                cwd,
                "Selected commit is invalid or no longer available.",
            ));
        }
        let parent_text = String::from_utf8_lossy(&parents.stdout).into_owned();
        let parts: Vec<&str> = parent_text.split_whitespace().collect();
        let parent = parts.get(1).copied().unwrap_or(sha);
        let mut st = a.clone();
        st.extend([
            "--raw".into(),
            "--numstat".into(),
            "-z".into(),
            parent.to_owned(),
            sha.to_owned(),
            "--".into(),
        ]);
        let so = git(cwd, &st).await.map_err(|_| {
            git_error(
                "GitVcsDriver.getReviewDiffPreview.commitStat",
                cwd,
                "Could not read complete diff statistics.",
            )
        })?;
        let fs = parse_numstat(&String::from_utf8_lossy(&so.stdout));
        let mut pt = a.clone();
        pt.extend([
            "--patch".into(),
            parent.to_owned(),
            sha.to_owned(),
            "--".into(),
        ]);
        let po = git(cwd, &pt).await.map_err(|_| {
            git_error(
                "GitVcsDriver.getReviewDiffPreview.commitPatch",
                cwd,
                "Could not read commit patch.",
            )
        })?;
        let d = if metadata_only && input.get("file").is_none() {
            String::new()
        } else {
            String::from_utf8_lossy(&po.stdout).into_owned()
        };
        let subject = git(
            cwd,
            &[
                "show".into(),
                "-s".into(),
                "--format=%s".into(),
                sha.to_owned(),
            ],
        )
        .await
        .ok()
        .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_owned())
        .unwrap_or_default();
        let mut source = json!({"id":sha,"kind":"commit","title":format!("Commit {}", sha.chars().take(7).collect::<String>()),"baseRef":parent,"headRef":sha,"diff":d,"files":fs,"diffHash":hash_diff(&d,&fs),"truncated":false});
        if !subject.is_empty() {
            source["commitSubject"] = json!(subject.chars().take(300).collect::<String>());
        }
        if metadata_only && input.get("file").is_none() {
            source["patchesDeferred"] = json!(true);
        }
        sources.push(source);
        return Ok(json!({"cwd":cwd,"generatedAt":iso_now(),"sources":sources}));
    }
    if sk.is_none() || sk == Some("working-tree") {
        let mut s = a.clone();
        s.extend([
            "--raw".into(),
            "--numstat".into(),
            "-z".into(),
            "HEAD".into(),
        ]);
        let so = git(cwd, &s).await.map_err(|_| {
            git_error(
                "GitVcsDriver.getReviewDiffPreview.stat",
                cwd,
                "Could not read complete diff statistics.",
            )
        })?;
        let fs = parse_numstat(&String::from_utf8_lossy(&so.stdout));
        let mut q = a.clone();
        q.extend(["--patch".into(), "HEAD".into()]);
        let po = git(cwd, &q).await.map_err(|_| {
            git_error(
                "GitVcsDriver.getReviewDiffPreview.patch",
                cwd,
                "Could not read diff patch.",
            )
        })?;
        let d = if metadata_only {
            String::new()
        } else {
            String::from_utf8_lossy(&po.stdout).into_owned()
        };
        add(
            "working-tree",
            "working-tree",
            "Uncommitted".into(),
            json!(input
                .get("baseRef")
                .and_then(Value::as_str)
                .unwrap_or("HEAD")),
            Value::Null,
            d,
            fs,
        );
    }
    if sk.is_none() || sk == Some("branch-range") {
        let b = base.unwrap_or_else(|| "HEAD".into());
        let mut s = a.clone();
        s.extend([
            "--raw".into(),
            "--numstat".into(),
            "-z".into(),
            format!("{b}..HEAD"),
        ]);
        let so = git(cwd, &s).await.map_err(|_| {
            git_error(
                "GitVcsDriver.getReviewDiffPreview.stat",
                cwd,
                "Could not read complete diff statistics.",
            )
        })?;
        let fs = parse_numstat(&String::from_utf8_lossy(&so.stdout));
        let mut q = a.clone();
        q.extend(["--patch".into(), format!("{b}..HEAD")]);
        let po = git(cwd, &q).await.map_err(|_| {
            git_error(
                "GitVcsDriver.getReviewDiffPreview.patch",
                cwd,
                "Could not read diff patch.",
            )
        })?;
        let d = if metadata_only {
            String::new()
        } else {
            String::from_utf8_lossy(&po.stdout).into_owned()
        };
        add(
            "branch-range",
            "branch-range",
            if input.get("baseRef").is_some() {
                format!("Changes vs {b}")
            } else {
                "Changes".into()
            },
            json!(input.get("baseRef").and_then(Value::as_str)),
            json!(branch_name),
            d,
            fs,
        );
    }
    let mut result = json!({"cwd":cwd,"generatedAt":iso_now(),"sources":sources});
    if input["trackedOnly"] == true {
        result["incomplete"] = json!(true);
    }
    Ok(result)
}
async fn diff_file_contents(_state: &AppState, input: &Value) -> Result<Value, Value> {
    let cwd = cwd(input)?;
    let source = input["sourceKind"].as_str().unwrap_or("working-tree");
    let change = input["changeType"].as_str().unwrap_or("change");
    let old = input["oldPath"].as_str().unwrap_or("");
    let new = input["newPath"].as_str().unwrap_or("");
    let p = git(cwd, &["rev-parse".into(), "--show-toplevel".into()]).await;
    if p.as_ref().is_err() || !p.as_ref().unwrap().status.success() {
        return Err(
            json!({"_tag":"VcsUnsupportedOperationError","operation":"ReviewService.getDiffFileContents","kind":"unknown","detail":"Unchanged diff expansion currently requires a Git repository."}),
        );
    }
    let read = |rev: String, path: String| async move {
        git(cwd, &["show".into(), format!("{rev}:{path}")])
            .await
            .ok()
            .filter(|x| x.status.success())
            .map(|x| String::from_utf8_lossy(&x.stdout).into_owned())
    };
    let oc = if change == "new" {
        String::new()
    } else {
        read(
            input["baseRef"].as_str().unwrap_or("HEAD").into(),
            old.into(),
        )
        .await
        .unwrap_or_default()
    };
    let nc = if source == "commit" {
        if change == "deleted" {
            String::new()
        } else {
            read(
                input["headRef"].as_str().unwrap_or("HEAD").into(),
                new.into(),
            )
            .await
            .unwrap_or_default()
        }
    } else if change == "deleted" {
        String::new()
    } else {
        tokio::fs::read_to_string(Path::new(cwd).join(new))
            .await
            .unwrap_or_default()
    };
    Ok(json!({"oldContents":oc,"newContents":nc}))
}
