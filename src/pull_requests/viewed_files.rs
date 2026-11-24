//! Viewed files kept by this environment, for hosts that keep none
//! (`viewedFiles: "environment"`, Bitbucket), as the reference's
//! `pullRequestViewedFiles` does: each reader's ticks go to the shared
//! database's `pull_request_files_viewed`, with the version of the file
//! ticked (its blob in the pull request's diff), and a file whose version
//! has moved on since reads back as `dismissed`.
use rusqlite::{params, Connection};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::path::Path;
use std::sync::{LazyLock, Mutex};
use std::time::{Duration, Instant};

/// `MAX_FILES_VIEWED_ROWS`: one read lists at most this many marks.
const MAX_ROWS: usize = 500;
/// `FILE_REVISIONS_CACHE_TTL`: revisions held this long are used as they are.
const REVISIONS_TTL: Duration = Duration::from_secs(60);
/// `MAX_FILE_REVISION_PATHS`: the paths one pull request's held revisions cover.
const MAX_REVISION_PATHS: usize = 1_000;
/// `FILES_VIEWED_CACHE_TTL`.
const RESULT_TTL: Duration = Duration::from_secs(15);
/// `REVISION_PATCH_TTL`: a pull request's diff, read for its revisions.
const PATCH_TTL: Duration = Duration::from_secs(5);
const STORE_DETAIL: &str =
    "This environment could not reach its record of which files you have seen.";

/// Which pull request, as the reference keys its caches: the project, the
/// repository (lowercased) and the number.
pub(super) type Key = (String, String, u64);

struct Held {
    at: Instant,
    asked: Vec<String>,
    revisions: HashMap<String, String>,
}

static HELD: LazyLock<Mutex<HashMap<Key, Held>>> = LazyLock::new(Default::default);
/// A diff's revisions by path, when they were read, and whether it was cut off.
type ReadPatch = (Instant, HashMap<String, String>, bool);
/// A reader's marks (path, the version ticked), by path.
type Marks = Vec<(String, Option<String>)>;

static PATCHES: LazyLock<Mutex<HashMap<Key, ReadPatch>>> = LazyLock::new(Default::default);
static RESULTS: LazyLock<Mutex<HashMap<Key, (Instant, Value)>>> = LazyLock::new(Default::default);
/// Ticks of one pull request are written one at a time, in order
/// (`inFilesViewedOrder`).
static WRITES: LazyLock<tokio::sync::Mutex<()>> = LazyLock::new(Default::default);

/// `pullRequests.invalidate`: what was read of viewed files is read again.
pub(crate) fn forget(key: Option<&Key>) {
    let mut results = RESULTS.lock().unwrap();
    match key {
        Some(key) => {
            results.remove(key);
        }
        None => results.clear(),
    }
}

/// Where the marks are kept, and for whom.
pub(super) struct Scope<'a> {
    pub provider: &'a str,
    pub host: &'a str,
    pub repository: &'a str,
    pub number: u64,
    pub viewer: &'a str,
}

/// A pull request's diff as one patch (what Bitbucket's `/diff` answers),
/// and whether it was cut off.
pub(super) type Patch = (String, bool);

fn store_error(operation: &str) -> Value {
    json!({"_tag": "PullRequestOperationError", "operation": operation, "detail": STORE_DETAIL})
}

fn open(state_dir: &Path) -> rusqlite::Result<Connection> {
    let conn = Connection::open_with_flags(
        state_dir.join(crate::statev2::FILE_NAME),
        rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE,
    )?;
    crate::statev2::configure(&conn, false)?;
    Ok(conn)
}

/// The scope's marks, by path, and whether there were more.
fn marks(state_dir: &Path, scope: &Scope) -> rusqlite::Result<(Marks, bool)> {
    let conn = open(state_dir)?;
    let mut statement = conn.prepare(
        "SELECT path, revision FROM pull_request_files_viewed
         WHERE provider = ?1 AND host = ?2 AND repository = ?3 AND number = ?4 AND viewer = ?5
         ORDER BY path LIMIT ?6",
    )?;
    let mut rows: Marks = statement
        .query_map(
            params![
                scope.provider,
                scope.host,
                scope.repository,
                scope.number as i64,
                scope.viewer,
                (MAX_ROWS + 1) as i64
            ],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?
        .collect::<rusqlite::Result<_>>()?;
    let truncated = rows.len() > MAX_ROWS;
    rows.truncate(MAX_ROWS);
    Ok((rows, truncated))
}

/// Ticks (with their revision) and unticks, in one transaction; an unticked
/// file's row is deleted rather than kept.
fn write(
    state_dir: &Path,
    scope: &Scope,
    files: &[(String, Option<String>, bool)],
    viewed_at: &str,
) -> rusqlite::Result<()> {
    let mut conn = open(state_dir)?;
    let transaction = conn.transaction()?;
    for (path, revision, viewed) in files {
        if *viewed {
            transaction.execute(
                "INSERT INTO pull_request_files_viewed
                   (provider, host, repository, number, viewer, path, revision, viewed_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
                 ON CONFLICT (provider, host, repository, number, viewer, path)
                 DO UPDATE SET revision = excluded.revision, viewed_at = excluded.viewed_at",
                params![
                    scope.provider,
                    scope.host,
                    scope.repository,
                    scope.number as i64,
                    scope.viewer,
                    path,
                    revision,
                    viewed_at
                ],
            )?;
        } else {
            transaction.execute(
                "DELETE FROM pull_request_files_viewed
                 WHERE provider = ?1 AND host = ?2 AND repository = ?3 AND number = ?4
                   AND viewer = ?5 AND path = ?6",
                params![
                    scope.provider,
                    scope.host,
                    scope.repository,
                    scope.number as i64,
                    scope.viewer,
                    path
                ],
            )?;
        }
    }
    transaction.commit()
}

/// `getFileRevisions`: the head's blob of each of `paths`, from the pull
/// request's diff (read again after [`PATCH_TTL`]). A whole diff also names
/// the paths it lacks as deleted (`""`); a cut-off one cannot.
async fn file_revisions<F, Fut>(
    key: &Key,
    paths: &[String],
    read_patch: F,
) -> Result<(HashMap<String, String>, bool), Value>
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = Result<Patch, Value>>,
{
    if paths.is_empty() {
        return Ok((HashMap::new(), false));
    }
    let cached = PATCHES
        .lock()
        .unwrap()
        .get(key)
        .filter(|(at, _, _)| at.elapsed() <= PATCH_TTL)
        .map(|(_, revisions, truncated)| (revisions.clone(), *truncated));
    let (mut revisions, truncated) = match cached {
        Some(read) => read,
        None => {
            let (patch, truncated) = read_patch().await?;
            let revisions = diff_file_revisions(&patch);
            PATCHES
                .lock()
                .unwrap()
                .insert(key.clone(), (Instant::now(), revisions.clone(), truncated));
            (revisions, truncated)
        }
    };
    if !truncated {
        for path in paths {
            revisions.entry(path.clone()).or_default();
        }
    }
    Ok((revisions, !truncated))
}

/// `fileRevisionsOf`: revisions held for the pull request when they cover
/// `paths` and are fresh, else read and held.
async fn revisions_of<F, Fut>(
    key: &Key,
    paths: &[String],
    read_patch: F,
) -> Result<HashMap<String, String>, Value>
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = Result<Patch, Value>>,
{
    {
        let held = HELD.lock().unwrap();
        if let Some(held) = held.get(key) {
            if held.at.elapsed() <= REVISIONS_TTL && paths.iter().all(|p| held.asked.contains(p)) {
                return Ok(held.revisions.clone());
            }
        }
    }
    let (answer, complete) = file_revisions(key, paths, read_patch).await?;
    let mut all = HELD.lock().unwrap();
    let carried = all
        .remove(key)
        .filter(|held| held.at.elapsed() <= REVISIONS_TTL * 10);
    let mut revisions = carried
        .as_ref()
        .map(|h| h.revisions.clone())
        .unwrap_or_default();
    let mut asked = carried
        .as_ref()
        .map(|h| h.asked.clone())
        .unwrap_or_default();
    let mut learned: Vec<String> = if complete {
        let mut known: Vec<String> = answer.keys().cloned().collect();
        known.sort();
        known
    } else {
        Vec::new()
    };
    learned.extend(paths.iter().cloned());
    for path in learned {
        asked.retain(|p| p != &path);
        asked.push(path.clone());
        if let Some(revision) = answer.get(&path) {
            revisions.insert(path, revision.clone());
        }
    }
    while asked.len() > MAX_REVISION_PATHS {
        let dropped = asked.remove(0);
        revisions.remove(&dropped);
    }
    let at = match &carried {
        Some(held) if !revisions.keys().all(|p| answer.contains_key(p)) => held.at,
        _ => Instant::now(),
    };
    all.insert(
        key.clone(),
        Held {
            at,
            asked,
            revisions: revisions.clone(),
        },
    );
    Ok(revisions)
}

/// `environmentFilesViewed`: the reader's marks, each `viewed`, or
/// `dismissed` once the file's head moved on from the version ticked.
pub(super) async fn files_viewed<F, Fut>(
    state_dir: &Path,
    key: &Key,
    scope: &Scope<'_>,
    read_patch: F,
) -> Result<Value, Value>
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = Result<Patch, Value>>,
{
    if let Some((_, result)) = RESULTS
        .lock()
        .unwrap()
        .get(key)
        .filter(|(at, _)| at.elapsed() <= RESULT_TTL)
    {
        return Ok(result.clone());
    }
    let (marks, truncated) = marks(state_dir, scope).map_err(|_| store_error("filesViewed"))?;
    let result = if marks.is_empty() {
        json!({"files": [], "truncated": truncated})
    } else {
        let paths: Vec<String> = marks.iter().map(|(path, _)| path.clone()).collect();
        // Without what the head has of them, every mark reads as viewed.
        let revisions = match revisions_of(key, &paths, read_patch).await {
            Ok(revisions) => Some(revisions),
            Err(error) => {
                tracing::warn!(
                    ?error,
                    "reporting viewed files without what the head has of them"
                );
                None
            }
        };
        let files: Vec<Value> = marks
            .iter()
            .map(|(path, ticked)| {
                let head = revisions.as_ref().and_then(|r| r.get(path));
                let dismissed =
                    matches!((ticked, head), (Some(ticked), Some(head)) if ticked != head);
                json!({"path": path, "state": if dismissed { "dismissed" } else { "viewed" }})
            })
            .collect();
        json!({"files": files, "truncated": truncated})
    };
    RESULTS
        .lock()
        .unwrap()
        .insert(key.clone(), (Instant::now(), result.clone()));
    Ok(result)
}

/// One tick or untick of `setFilesViewed`'s input.
pub(super) struct Press {
    pub path: String,
    pub viewed: bool,
    pub revision: Option<String>,
}

/// `pullRequests.setFilesViewed`'s files: at most 500, each a non-empty path
/// of up to 4096 characters with an optional revision of up to 256.
pub(super) fn presses(input: &Value) -> Option<Vec<Press>> {
    let files = input["files"].as_array().filter(|f| f.len() <= 500)?;
    files
        .iter()
        .map(|file| {
            let path = file["path"]
                .as_str()
                .filter(|p| !p.is_empty() && p.encode_utf16().count() <= 4096)?;
            let viewed = file["viewed"].as_bool()?;
            let revision = match file.get("revision") {
                None | Some(Value::Null) => None,
                Some(revision) => Some(
                    revision
                        .as_str()
                        .map(str::trim)
                        .filter(|r| !r.is_empty() && r.chars().count() <= 256)?
                        .to_owned(),
                ),
            };
            Some(Press {
                path: path.to_owned(),
                viewed,
                revision,
            })
        })
        .collect()
}

/// `environmentSetFilesViewed`: each tick keeps the version shown (else the
/// head's, read fresh; else none), each untick removes its row.
pub(super) async fn set_files_viewed<F, Fut>(
    state_dir: &Path,
    key: &Key,
    scope: &Scope<'_>,
    presses: Vec<Press>,
    read_patch: F,
) -> Result<Value, Value>
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = Result<Patch, Value>>,
{
    let _order = WRITES.lock().await;
    let cleared: Vec<String> = presses
        .iter()
        .filter(|p| p.viewed && p.revision.is_none())
        .map(|p| p.path.clone())
        .collect();
    let revisions = if cleared.is_empty() {
        None
    } else {
        match revisions_of(key, &cleared, read_patch).await {
            Ok(revisions) => Some(revisions),
            Err(error) => {
                tracing::warn!(
                    ?error,
                    "recording viewed files without what the head has of them"
                );
                None
            }
        }
    };
    let files: Vec<(String, Option<String>, bool)> = presses
        .iter()
        .map(|p| {
            let revision = p
                .revision
                .clone()
                .or_else(|| revisions.as_ref().and_then(|r| r.get(&p.path).cloned()));
            (p.path.clone(), revision, p.viewed)
        })
        .collect();
    let viewed_at = crate::auth_service::iso(crate::auth_service::now());
    let written =
        write(state_dir, scope, &files, &viewed_at).map_err(|_| store_error("setFilesViewed"));
    // What was read is read again; a version the reader was shown moves the
    // held revisions on too.
    forget(Some(key));
    if presses.iter().any(|p| p.viewed && p.revision.is_some()) {
        HELD.lock().unwrap().remove(key);
    }
    written.map(|_| Value::Null)
}

/// `parseDiffFileRevisions`: each file's head blob (from its `index` line),
/// by its head path (its old path when deleted).
pub(crate) fn diff_file_revisions(patch: &str) -> HashMap<String, String> {
    let mut revisions = HashMap::new();
    for entry in entries(patch) {
        let path = if entry.deleted {
            entry.old_path
        } else {
            entry.new_path.or(entry.old_path)
        };
        if let (Some(path), Some(revision)) = (path.filter(|p| !p.is_empty()), entry.revision) {
            revisions.insert(path, revision);
        }
    }
    revisions
}

struct Entry {
    old_path: Option<String>,
    new_path: Option<String>,
    deleted: bool,
    revision: Option<String>,
}

fn entries(patch: &str) -> Vec<Entry> {
    let mut entries = Vec::new();
    let mut current: Option<(Entry, bool)> = None;
    for line in patch.split('\n') {
        if let Some(rest) = line.strip_prefix("diff --git ") {
            if let Some((entry, _)) = current.take() {
                entries.push(entry);
            }
            let (old_path, new_path) = header_paths(rest);
            current = Some((
                Entry {
                    old_path,
                    new_path,
                    deleted: false,
                    revision: None,
                },
                false,
            ));
            continue;
        }
        let Some((entry, in_body)) = current.as_mut() else {
            continue;
        };
        if *in_body {
            continue;
        }
        if line.starts_with("@@") {
            *in_body = true;
        } else if let Some(rest) = line.strip_prefix("index ") {
            entry.revision = head_revision(rest);
        } else if line.starts_with("deleted file mode") {
            entry.deleted = true;
        } else if let Some(rest) = line.strip_prefix("rename from ") {
            entry.old_path = Some(unquote(rest));
        } else if let Some(rest) = line.strip_prefix("rename to ") {
            entry.new_path = Some(unquote(rest));
        } else if let Some(rest) = line.strip_prefix("--- ") {
            entry.old_path = side_path(rest, "a/");
        } else if let Some(rest) = line.strip_prefix("+++ ") {
            let side = side_path(rest, "b/");
            if side.is_none() {
                entry.deleted = true;
            }
            entry.new_path = side;
        }
    }
    if let Some((entry, _)) = current {
        entries.push(entry);
    }
    entries
}

fn head_revision(rest: &str) -> Option<String> {
    let after = &rest[rest.find("..")? + 2..];
    let head = after.split(' ').next().unwrap_or("");
    (!head.is_empty()).then(|| head.to_owned())
}

/// Git ends a name with a tab and escapes tabs within names.
fn side_path(rest: &str, prefix: &str) -> Option<String> {
    let token = rest.split('\t').next().unwrap_or("");
    if token == "/dev/null" {
        return None;
    }
    let path = unquote(token);
    Some(path.strip_prefix(prefix).map(str::to_owned).unwrap_or(path))
}

fn header_side(token: &str, prefix: &str) -> Option<String> {
    unquote(token).strip_prefix(prefix).map(str::to_owned)
}

fn quoted_end(rest: &str) -> Option<usize> {
    let bytes = rest.as_bytes();
    let mut at = 1;
    while at < bytes.len() {
        match bytes[at] {
            b'\\' => at += 2,
            b'"' => return Some(at),
            _ => at += 1,
        }
    }
    None
}

/// `headerPaths`: both sides of a `diff --git` line; unquoted names with
/// spaces match where both sides are the same.
fn header_paths(rest: &str) -> (Option<String>, Option<String>) {
    if rest.starts_with('"') {
        return match quoted_end(rest) {
            Some(end) if rest[end + 1..].starts_with(' ') => (
                header_side(&rest[..=end], "a/"),
                header_side(&rest[end + 2..], "b/"),
            ),
            _ => (None, None),
        };
    }
    if rest.ends_with('"') {
        return match rest.find('"') {
            Some(opens) if opens >= 1 && rest[..opens].ends_with(' ') => (
                header_side(&rest[..opens - 1], "a/"),
                header_side(&rest[opens..], "b/"),
            ),
            _ => (None, None),
        };
    }
    if !rest.starts_with("a/") {
        return (None, None);
    }
    let splits: Vec<usize> = rest.match_indices(" b/").map(|(at, _)| at).collect();
    let chosen = splits
        .iter()
        .copied()
        .find(|&at| rest.get(2..at) == rest.get(at + 3..))
        .or_else(|| (splits.len() == 1).then(|| splits[0]));
    match chosen {
        Some(at) => (
            Some(rest[2..at].to_owned()),
            Some(rest[at + 3..].to_owned()),
        ),
        None => (None, None),
    }
}

/// `unquoteGitPatchPath`: a name with or without its quotes, its escapes
/// (named and octal bytes) undone.
fn unquote(token: &str) -> String {
    let body = if token.len() >= 2 && token.starts_with('"') && token.ends_with('"') {
        &token[1..token.len() - 1]
    } else {
        token
    };
    if !body.contains('\\') {
        return body.to_owned();
    }
    let mut bytes = Vec::with_capacity(body.len());
    let chars: Vec<char> = body.chars().collect();
    let mut at = 0;
    while at < chars.len() {
        let c = chars[at];
        if c != '\\' {
            let mut buffer = [0u8; 4];
            bytes.extend_from_slice(c.encode_utf8(&mut buffer).as_bytes());
            at += 1;
            continue;
        }
        let Some(&escaped) = chars.get(at + 1) else {
            bytes.push(b'\\');
            break;
        };
        let named = match escaped {
            '"' => Some(0x22),
            '\\' => Some(0x5c),
            'a' => Some(0x07),
            'b' => Some(0x08),
            'f' => Some(0x0c),
            'n' => Some(0x0a),
            'r' => Some(0x0d),
            't' => Some(0x09),
            'v' => Some(0x0b),
            _ => None,
        };
        if let Some(byte) = named {
            bytes.push(byte);
            at += 2;
            continue;
        }
        let octal: String = chars[at + 1..(at + 4).min(chars.len())].iter().collect();
        if octal.len() == 3 && octal.chars().all(|c| ('0'..='7').contains(&c)) {
            bytes.push(u8::from_str_radix(&octal, 8).unwrap_or(0));
            at += 4;
            continue;
        }
        let mut buffer = [0u8; 4];
        bytes.extend_from_slice(escaped.encode_utf8(&mut buffer).as_bytes());
        at += 2;
    }
    String::from_utf8_lossy(&bytes).into_owned()
}

/// Held revisions and reads for `key` are dropped too, for tests.
#[cfg(test)]
fn reset(key: &Key) {
    HELD.lock().unwrap().remove(key);
    PATCHES.lock().unwrap().remove(key);
    RESULTS.lock().unwrap().remove(key);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn revisions_come_from_index_lines_by_head_path() {
        let patch = "diff --git a/src/a.rs b/src/a.rs\nindex 1111111..2222222 100644\n--- a/src/a.rs\n+++ b/src/a.rs\n@@ -1 +1 @@\n-index 9..9 fake\n+x\n\
diff --git a/old.txt b/old.txt\ndeleted file mode 100644\nindex 3333333..0000000\n--- a/old.txt\n+++ /dev/null\n\
diff --git a/from.rs b/to.rs\nsimilarity index 90%\nrename from from.rs\nrename to to.rs\nindex 4444444..5555555 100644\n\
diff --git \"a/sp ace\\tq.rs\" \"b/sp ace\\tq.rs\"\nindex 6666666..7777777\n\
diff --git a/no-index.rs b/no-index.rs\n";
        let revisions = diff_file_revisions(patch);
        assert_eq!(
            revisions.get("src/a.rs").map(String::as_str),
            Some("2222222")
        );
        assert_eq!(
            revisions.get("old.txt").map(String::as_str),
            Some("0000000")
        );
        assert_eq!(revisions.get("to.rs").map(String::as_str), Some("5555555"));
        assert_eq!(
            revisions.get("sp ace\tq.rs").map(String::as_str),
            Some("7777777")
        );
        assert!(!revisions.contains_key("no-index.rs"));
        assert_eq!(revisions.len(), 4);
    }

    #[tokio::test]
    async fn ticks_keep_their_version_and_a_moved_head_dismisses_them() {
        let home = tempfile::tempdir().unwrap();
        drop(crate::statev2::open(home.path()).unwrap());
        let key: Key = ("project:p".into(), "o/r".into(), 41);
        reset(&key);
        let scope = Scope {
            provider: "bitbucket",
            host: "bitbucket.org",
            repository: "o/r",
            number: 41,
            viewer: "me",
        };
        let patch = || async {
            Ok((
                "diff --git a/a.rs b/a.rs\nindex 1..2 100644\n--- a/a.rs\n+++ b/a.rs\n".to_owned(),
                false,
            ))
        };
        let ticks = presses(&json!({"files": [
            {"path": "a.rs", "viewed": true},
            {"path": "b.rs", "viewed": true, "revision": "old"},
            {"path": "gone.md", "viewed": true},
        ]}))
        .unwrap();
        set_files_viewed(home.path(), &key, &scope, ticks, patch)
            .await
            .unwrap();
        let read = files_viewed(home.path(), &key, &scope, patch)
            .await
            .unwrap();
        assert_eq!(
            read,
            json!({"files": [
                {"path": "a.rs", "state": "viewed"},
                {"path": "b.rs", "state": "dismissed"},
                {"path": "gone.md", "state": "viewed"},
            ], "truncated": false})
        );
        let untick = presses(&json!({"files": [{"path": "b.rs", "viewed": false}]})).unwrap();
        set_files_viewed(home.path(), &key, &scope, untick, patch)
            .await
            .unwrap();
        let read = files_viewed(home.path(), &key, &scope, patch)
            .await
            .unwrap();
        assert_eq!(read["files"].as_array().unwrap().len(), 2);
        // Another reader has no marks.
        let other = Scope {
            viewer: "you",
            ..scope
        };
        forget(Some(&key));
        let read = files_viewed(home.path(), &key, &other, patch)
            .await
            .unwrap();
        assert_eq!(read, json!({"files": [], "truncated": false}));
        reset(&key);
    }

    #[test]
    fn presses_refuse_what_the_contract_refuses() {
        assert!(presses(&json!({"files": [{"path": "", "viewed": true}]})).is_none());
        assert!(
            presses(&json!({"files": [{"path": "a", "viewed": true, "revision": " "}]})).is_none()
        );
        assert!(presses(&json!({"files": [{"path": "a"}]})).is_none());
        assert_eq!(presses(&json!({"files": []})).unwrap().len(), 0);
    }
}
