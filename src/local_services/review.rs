use super::vcs::{branch, checked, read, review_base, run};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{
    collections::HashMap,
    path::Path,
    sync::{LazyLock, Mutex},
    time::{Duration, Instant},
};

const TRACKED_PREVIEW_TTL: Duration = Duration::from_secs(30);
const TRACKED_PREVIEW_CAPACITY: usize = 16;

struct TrackedPreview {
    key: String,
    expires_at: Instant,
    root: String,
    branch: Option<String>,
    label: Option<String>,
    merge_base: Option<String>,
    dirty: Value,
    base: Value,
}

static TRACKED_PREVIEWS: LazyLock<Mutex<HashMap<String, TrackedPreview>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

pub async fn call(method: &str, input: &Value, state_dir: &Path) -> Result<Value, Value> {
    match method {
        "getBranchCommits" => branch_commits(input).await,
        "getDiffPreview" => diff_preview(input).await,
        "getDiffFileContents" => diff_file_contents(input, state_dir).await,
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

fn tracked_preview_key(input: &Value) -> String {
    serde_json::to_string(&json!([
        input["cwd"],
        input["baseRef"].as_str(),
        input["sourceKind"].as_str(),
        input["ignoreWhitespace"] == true,
        input["metadataOnly"] == true,
    ]))
    .unwrap_or_default()
}

fn take_tracked_preview(input: &Value) -> Option<TrackedPreview> {
    if input["completionToken"].is_null() || input["trackedOnly"] == true {
        return None;
    }
    let token = input["completionToken"].as_str()?;
    let mut previews = TRACKED_PREVIEWS.lock().ok()?;
    let preview = previews.remove(token)?;
    (preview.expires_at >= Instant::now() && preview.key == tracked_preview_key(input))
        .then_some(preview)
}

fn remember_tracked_preview(token: String, preview: TrackedPreview) {
    if let Ok(mut previews) = TRACKED_PREVIEWS.lock() {
        previews.retain(|_, entry| entry.expires_at >= Instant::now());
        previews.insert(token, preview);
        while previews.len() > TRACKED_PREVIEW_CAPACITY {
            let Some(oldest) = previews
                .iter()
                .min_by_key(|(_, entry)| entry.expires_at)
                .map(|(token, _)| token.clone())
            else {
                break;
            };
            previews.remove(&oldest);
        }
    }
}
fn parse_numstat(text: &str) -> Vec<Value> {
    let mut types = std::collections::HashMap::new();
    let mut files = Vec::new();
    let mut fields = text.split_terminator('\0');
    while let Some(row) = fields.next() {
        if row.starts_with(':') {
            let status = row.rsplit(' ').next().unwrap_or("");
            if status.starts_with(['R', 'C']) {
                fields.next();
            }
            if let Some(path) = fields.next() {
                let kind = match status {
                    "A" => "new",
                    "D" => "deleted",
                    "R100" => "rename-pure",
                    s if s.starts_with('R') => "rename-changed",
                    _ => "change",
                };
                types.insert(path, kind);
            }
        } else {
            let mut cols = row.splitn(3, '\t');
            let (Some(add), Some(del), Some(path)) = (cols.next(), cols.next(), cols.next()) else {
                continue;
            };
            let (previous, path) = if path.is_empty() {
                (fields.next(), fields.next().unwrap_or(""))
            } else {
                (None, path)
            };
            files.push(json!({"path":path,"previousPath":previous,"changeType":types.get(path),"additions":add.parse::<u64>().unwrap_or(0),"deletions":del.parse::<u64>().unwrap_or(0)}));
        }
    }
    files
}

async fn branch_commits(input: &Value) -> Result<Value, Value> {
    let cwd = cwd(input)?;
    if read(cwd, &["rev-parse", "--verify", "--quiet", "HEAD"])
        .await
        .is_none()
    {
        return Ok(json!({"commits":[],"truncated":false}));
    }
    let (_, base) =
        review_base(cwd, branch(cwd).await.as_deref(), input["baseRef"].as_str()).await?;
    let log = checked(
        cwd,
        "getReviewBranchCommits",
        &[
            "log",
            "-z",
            "--no-notes",
            "--no-show-signature",
            "--format=%H%x00%s%x00%cI",
            "--max-count=101",
            &format!("{base}..HEAD"),
        ],
        "Could not read branch commit metadata.",
    )
    .await?;
    let fields: Vec<_> = log.split_terminator('\0').collect();
    let commits: Vec<_> = fields.chunks_exact(3).take(100).map(|c| json!({"sha":c[0],"subject":c[1].chars().take(300).collect::<String>(),"committedAt":c[2]})).collect();
    Ok(json!({"commits":commits,"truncated":fields.len() > 300}))
}

fn commit_error(cwd: &str, detail: &str) -> Value {
    json!({"_tag":"GitCommandError","operation":"GitVcsDriver.reviewCommit","command":"git cat-file","cwd":cwd,"detail":detail})
}
async fn empty_tree(cwd: &str) -> Result<String, Value> {
    checked(
        cwd,
        "review.emptyTree",
        &["hash-object", "-t", "tree", "--stdin"],
        "Could not resolve empty tree.",
    )
    .await
}
async fn commit_parent(cwd: &str, sha: &str) -> Result<String, Value> {
    let invalid = || commit_error(cwd, "Selected commit is invalid or no longer available.");
    if !matches!(sha.len(), 40 | 64) || !sha.bytes().all(|c| c.is_ascii_hexdigit()) {
        return Err(invalid());
    }
    let object = read(cwd, &["cat-file", "commit", sha])
        .await
        .ok_or_else(invalid)?;
    if let Some(parent) = object
        .lines()
        .nth(1)
        .and_then(|l| l.strip_prefix("parent "))
    {
        if read(cwd, &["cat-file", "-e", &format!("{parent}^{{commit}}")])
            .await
            .is_none()
        {
            return Err(commit_error(
                cwd,
                "Selected commit's parent is not available locally.",
            ));
        }
        Ok(parent.into())
    } else {
        empty_tree(cwd).await
    }
}

pub(super) mod index;
pub(crate) mod workspace;

/// Read raw status and numstat together so that rename classifications match the patch.
async fn review_diff(
    cwd: &str,
    refs: &[&str],
    input: &Value,
    index: &index::ReviewIndex,
) -> Result<Value, Value> {
    let mut args = vec![
        "-c",
        "diff.suppressBlankEmpty=false",
        "diff",
        "--find-renames",
        "--no-color",
        "--no-ext-diff",
        "--no-textconv",
        "--submodule=short",
        "--src-prefix=a/",
        "--dst-prefix=b/",
    ];
    if input["ignoreWhitespace"] == true {
        args.push("--ignore-all-space");
    }
    let paths: Vec<_> = ["path", "previousPath"]
        .into_iter()
        .filter_map(|key| input["file"][key].as_str())
        .map(|p| format!(":(top,literal){p}"))
        .collect();
    let diff = |mode: &[&str]| {
        let mut cmd = crate::login_shell::tokio_command("git");
        cmd.current_dir(cwd)
            .args(&args)
            .args(mode)
            .args(refs)
            .arg("--")
            .args(&paths)
            .kill_on_drop(true);
        if let Some(index) = &index.dir {
            cmd.env("GIT_INDEX_FILE", index.path().join("index"));
        }
        cmd
    };
    let stats = diff(&["--raw", "--numstat", "-z"]).output().await;
    let stats = stats.ok().filter(|o| o.status.success()).ok_or_else(|| {
        git_error(
            "GitVcsDriver.getReviewDiffPreview.stat",
            cwd,
            "Could not read complete diff statistics.",
        )
    })?;
    let files = parse_numstat(&String::from_utf8_lossy(&stats.stdout));
    let deferred = input["metadataOnly"] == true && input["file"].is_null() && !index.omitted;
    let mut patch = String::new();
    let mut truncated = index.omitted;
    if !deferred && !files.is_empty() {
        let limit = if input["file"].is_null() {
            120_000
        } else {
            1024 * 1024
        };
        // Drain only the bounded prefix, then drop the child if the patch exceeds the budget.
        use tokio::io::AsyncReadExt;
        let fail = || {
            git_error(
                "GitVcsDriver.getReviewDiffPreview.patch",
                cwd,
                "Could not read diff patch.",
            )
        };
        let mut child = diff(&["--patch"])
            .stdout(std::process::Stdio::piped())
            .spawn()
            .map_err(|_| fail())?;
        let mut bytes = Vec::new();
        child
            .stdout
            .take()
            .unwrap()
            .take(limit as u64 + 1)
            .read_to_end(&mut bytes)
            .await
            .map_err(|_| fail())?;
        if bytes.len() > limit {
            truncated = true;
            patch = String::from_utf8_lossy(&bytes[..limit]).into_owned();
            patch.push_str("\n\n[truncated]");
        } else {
            if !child.wait().await.map_err(|_| fail())?.success() {
                return Err(fail());
            }
            patch = String::from_utf8_lossy(&bytes).into_owned();
        }
    }
    let mut result = json!({"diff":patch,"truncated":truncated,"diffHash":hash_diff(&patch, if index.omitted { &[] } else { &files })});
    if !index.omitted {
        result["files"] = json!(files);
    }
    if deferred {
        result["patchesDeferred"] = json!(true);
    }
    Ok(result)
}

async fn diff_preview(input: &Value) -> Result<Value, Value> {
    let requested = cwd(input)?;
    let mut result = json!({"cwd":requested,"generatedAt":iso_now(),"sources":[]});
    let tracked = take_tracked_preview(input);
    let root = if let Some(saved) = tracked.as_ref() {
        saved.root.clone()
    } else {
        let Some(root) = read(requested, &["rev-parse", "--show-toplevel"]).await else {
            return Ok(result);
        };
        root
    };
    let cwd = root.as_str();
    let only = input["sourceKind"].as_str();
    let kind = input["file"]["sourceKind"].as_str().or(only);
    let mut sources = Vec::new();
    if input.get("commitSha").is_some() || kind == Some("commit") {
        let sha = input["commitSha"]
            .as_str()
            .filter(|_| input["file"].is_null() || kind == Some("commit"))
            .ok_or_else(|| commit_error(cwd, "Selected commit request is invalid."))?
            .to_ascii_lowercase();
        let parent = commit_parent(cwd, &sha).await?;
        let mut source =
            review_diff(cwd, &[&parent, &sha], input, &index::ReviewIndex::default()).await?;
        source.as_object_mut().unwrap().extend(json!({"id":sha,"kind":"commit","title":format!("Commit {}", &sha[..7]),"baseRef":parent,"headRef":sha}).as_object().unwrap().clone());
        if let Some(message) = read(
            cwd,
            &[
                "show",
                "-s",
                "--no-notes",
                "--no-show-signature",
                "--format=%B",
                &sha,
            ],
        )
        .await
        {
            let subject: String = message
                .lines()
                .next()
                .unwrap_or("")
                .chars()
                .take(300)
                .collect();
            if !subject.trim().is_empty() {
                source["commitSubject"] = json!(subject.trim());
            }
        }
        sources.push(source);
    } else {
        let index = if input["trackedOnly"] == true {
            index::ReviewIndex::default()
        } else {
            index::prepare(cwd, input["file"]["path"].as_str()).await?
        };
        let head = if read(cwd, &["rev-parse", "--verify", "HEAD"])
            .await
            .is_some()
        {
            "HEAD".into()
        } else {
            empty_tree(cwd).await?
        };
        let current = if kind == Some("working-tree") {
            None
        } else if let Some(saved) = tracked.as_ref() {
            saved.branch.clone()
        } else {
            branch(cwd).await
        };
        let (label, base) = if kind == Some("working-tree") {
            (input["baseRef"].as_str().map(str::to_owned), "HEAD".into())
        } else if let Some(saved) = tracked.as_ref() {
            (
                saved.label.clone(),
                saved.merge_base.clone().unwrap_or_else(|| "HEAD".into()),
            )
        } else if input["mergeBase"].as_str().is_some() {
            (
                input["baseRef"].as_str().map(str::to_owned),
                input["mergeBase"].as_str().unwrap().to_owned(),
            )
        } else {
            review_base(cwd, current.as_deref(), input["baseRef"].as_str()).await?
        };
        let can_reuse_tracked = tracked.is_some() && !index.omitted && index.dir.is_none();
        let mut tracked_results = if can_reuse_tracked {
            let saved = tracked.as_ref().unwrap();
            Some((saved.dirty.clone(), saved.base.clone()))
        } else {
            None
        };
        if tracked_results.is_none() {
            let reference = if kind == Some("working-tree") || base == "HEAD" {
                &head
            } else {
                &base
            };
            let dirty = if kind == Some("branch-range") {
                json!({
                    "diff": "",
                    "truncated": false,
                    "diffHash": hash_diff("", &[]),
                    "files": []
                })
            } else {
                review_diff(cwd, &[&head], input, &index).await?
            };
            let base_result = if kind == Some("working-tree") {
                dirty.clone()
            } else {
                review_diff(cwd, &[reference], input, &index).await?
            };
            tracked_results = Some((dirty, base_result));
        }
        let (dirty_result, base_result) = tracked_results.unwrap();
        let reuses_tracked = input["file"].is_null() && input.get("commitSha").is_none();
        let should_remember = input["trackedOnly"] == true
            && reuses_tracked
            && dirty_result["files"].is_array()
            && base_result["files"].is_array();
        for source_kind in ["working-tree", "branch-range"] {
            if only.is_some_and(|only| only != source_kind) {
                continue;
            }
            let working = source_kind == "working-tree";
            let mut source = if kind.is_some_and(|selected| selected != source_kind) {
                let mut empty =
                    json!({"diff":"","files":[],"diffHash":hash_diff("",&[]),"truncated":false});
                if input["metadataOnly"] == true && input["file"].is_null() {
                    empty["patchesDeferred"] = json!(true);
                }
                empty
            } else {
                if working {
                    dirty_result.clone()
                } else {
                    base_result.clone()
                }
            };
            source["id"] = json!(source_kind);
            source["kind"] = json!(source_kind);
            source["title"] = json!(if working {
                "Uncommitted".into()
            } else {
                label
                    .as_ref()
                    .map_or_else(|| "Changes".into(), |base| format!("Changes vs {base}"))
            });
            source["baseRef"] = if working { json!("HEAD") } else { json!(label) };
            source["headRef"] = if working {
                Value::Null
            } else {
                json!(current.as_deref().unwrap_or("HEAD"))
            };
            if !working {
                source["mergeBase"] = json!(base);
            }
            sources.push(source);
        }
        if input["trackedOnly"] == true {
            result["incomplete"] = json!(true);
        }
        if should_remember {
            let token = uuid::Uuid::new_v4().to_string();
            remember_tracked_preview(
                token.clone(),
                TrackedPreview {
                    key: tracked_preview_key(input),
                    expires_at: Instant::now() + TRACKED_PREVIEW_TTL,
                    root: root.clone(),
                    branch: current,
                    label,
                    merge_base: Some(base),
                    dirty: dirty_result,
                    base: base_result,
                },
            );
            result["completionToken"] = json!(token);
        }
    }
    result["sources"] = json!(sources);
    Ok(result)
}

const FILE_LIMIT: usize = 1024 * 1024;

async fn diff_file_contents(input: &Value, state_dir: &Path) -> Result<Value, Value> {
    let cwd = cwd(input)?;
    let operation = "GitVcsDriver.getReviewDiffFileContents";
    let error = |detail: &str| git_error(operation, cwd, detail);
    let Some(root) = read(cwd, &["rev-parse", "--show-toplevel"]).await else {
        return Err(
            json!({"_tag":"VcsUnsupportedOperationError","operation":"ReviewService.getDiffFileContents","kind":"unknown","detail":"Unchanged diff expansion currently requires a Git repository."}),
        );
    };
    let source = input["sourceKind"].as_str().unwrap_or("working-tree");
    let old = input["oldPath"].as_str().unwrap_or("");
    let new = input["newPath"].as_str().unwrap_or("");
    let base = input["baseRef"].as_str();
    let head = input["headRef"].as_str();
    let revision = if source == "commit" {
        let parent = commit_parent(
            cwd,
            head.ok_or_else(|| error("Selected commit is no longer available."))?,
        )
        .await?;
        if base != Some(&parent) {
            return Err(error("Selected commit changed. Refresh the comparison."));
        }
        parent
    } else if source == "branch-range" {
        review_base(cwd, None, base).await?.1
    } else {
        base.unwrap_or("HEAD").into()
    };
    let at_revision = |revision: String, path: String| async move {
        let arg = format!("{revision}:{path}");
        let out = run(cwd, &["show", "--end-of-options", &arg]).await;
        let out = out
            .ok()
            .filter(|o| o.status.success())
            .ok_or_else(|| error(&format!("Could not read diff file '{path}'.")))?;
        if out.stdout.contains(&0) {
            return Err(error(&format!("Cannot expand binary file '{path}'.")));
        }
        if out.stdout.len() > FILE_LIMIT {
            return Err(error(&format!(
                "Diff file '{path}' exceeds the 1 MB expansion limit."
            )));
        }
        Ok(String::from_utf8_lossy(&out.stdout).into_owned())
    };
    let old_contents = if input["changeType"] == "new" {
        String::new()
    } else {
        at_revision(revision, old.into()).await?
    };
    let new_contents = if input["changeType"] == "deleted" {
        String::new()
    } else if source == "commit" {
        at_revision(head.unwrap().into(), new.into()).await?
    } else {
        let fail = |stage: &str, detail: String| json!({"_tag":"GitCommandError","operation":format!("{operation}.workingTree.{stage}"),"command":stage,"cwd":cwd,"detail":detail});
        let root = Path::new(&root);
        let requested = super::vcs::lexical_path(&root.join(new));
        if !requested.starts_with(root) {
            return Err(fail(
                "path.resolve",
                format!("Diff file '{new}' resolves outside the review workspace."),
            ));
        }
        let path = tokio::fs::canonicalize(requested).await.map_err(|_| {
            fail(
                "fs.realPath",
                format!("Could not resolve diff file '{new}'."),
            )
        })?;
        if !path.starts_with(root) {
            return Err(fail(
                "fs.realPath",
                format!("Diff file '{new}' resolves outside the review workspace."),
            ));
        }
        let state = tokio::fs::canonicalize(state_dir)
            .await
            .unwrap_or_else(|_| state_dir.to_owned());
        if path.starts_with(state.join("secrets"))
            || path.starts_with("/dev/fd")
            || (path.parent() == Some(&state)
                && path
                    .file_name()
                    .is_some_and(|n| n.to_string_lossy().contains(".sqlite")))
        {
            return Err(fail(
                "fs.realPath",
                format!("Diff file '{new}' is not available."),
            ));
        }
        let info = tokio::fs::metadata(&path)
            .await
            .map_err(|_| fail("fs.stat", format!("Could not inspect diff file '{new}'.")))?;
        if !info.is_file() {
            return Err(fail("fs.stat", format!("Diff path '{new}' is not a file.")));
        }
        if info.len() > FILE_LIMIT as u64 {
            return Err(fail(
                "fs.stat",
                format!("Diff file '{new}' exceeds the 1 MB expansion limit."),
            ));
        }
        let bytes = tokio::fs::read(path)
            .await
            .map_err(|_| fail("fs.readFile", format!("Could not read diff file '{new}'.")))?;
        if bytes.contains(&0) {
            return Err(fail(
                "fs.readFile",
                format!("Cannot expand binary file '{new}'."),
            ));
        }
        String::from_utf8_lossy(&bytes).into_owned()
    };
    Ok(json!({"oldContents":old_contents,"newContents":new_contents}))
}
