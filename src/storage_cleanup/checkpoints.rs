//! A deleted thread's tombstone identifies its checkpoint namespaces. Absence
//! from this database says nothing about refs owned by another T3 home.
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use rusqlite::{Connection, OpenFlags};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::io::{Read, Seek, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

const OWNERS: &str = "
    WITH RECURSIVE retained(thread_id) AS (
        SELECT thread_id FROM orchestration_v2_projection_threads WHERE deleted_at IS NULL
        UNION
        SELECT json_extract(t.payload_json, '$.forkedFrom.threadId')
        FROM orchestration_v2_projection_threads t
        JOIN retained ON retained.thread_id = t.thread_id
        WHERE json_extract(t.payload_json, '$.forkedFrom.type') = 'run'
    )
    SELECT t.thread_id, s.scope_id, json_extract(s.payload_json, '$.cwd'),
           json_extract(t.payload_json, '$.worktreePath'), p.workspace_root
    FROM orchestration_v2_projection_threads t
    LEFT JOIN projection_projects p ON p.project_id = t.project_id
    LEFT JOIN orchestration_v2_projection_checkpoint_scopes s ON s.thread_id = t.thread_id
    WHERE t.deleted_at IS NOT NULL
      AND NOT EXISTS (SELECT 1 FROM retained WHERE thread_id = t.thread_id)
      AND NOT EXISTS (SELECT 1 FROM orchestration_v2_effect_outbox e
                      WHERE e.thread_id = t.thread_id AND e.status IN ('pending','running'))";

type Prefixes = BTreeSet<String>;

fn targets(dir: &Path) -> anyhow::Result<BTreeMap<PathBuf, Prefixes>> {
    let conn = Connection::open_with_flags(
        dir.join(crate::statev2::FILE_NAME),
        OpenFlags::SQLITE_OPEN_READ_ONLY,
    )?;
    let mut statement = conn.prepare(OWNERS)?;
    let mut rows = statement.query([])?;
    let mut targets: BTreeMap<PathBuf, Prefixes> = BTreeMap::new();
    while let Some(row) = rows.next()? {
        let thread: String = row.get(0)?;
        let scope: Option<String> = row.get(1)?;
        let mut prefixes = vec![format!(
            "refs/t3/checkpoints/{}/turn/",
            URL_SAFE_NO_PAD.encode(thread)
        )];
        if let Some(scope) = scope {
            let key = hex::encode(&Sha256::digest(scope)[..16]);
            prefixes.push(format!(
                "refs/t3/orchestration-v2/checkpoints/{}/ordinal/",
                URL_SAFE_NO_PAD.encode(key)
            ));
        }
        for i in 2..=4 {
            if let Some(path) = row.get::<_, Option<String>>(i)? {
                targets
                    .entry(path.into())
                    .or_default()
                    .extend(prefixes.iter().cloned());
            }
        }
    }
    Ok(targets)
}

fn git(cwd: &Path) -> Command {
    let mut cmd = crate::login_shell::command("git");
    cmd.current_dir(cwd).env("LC_ALL", "C").stdin(Stdio::null());
    cmd
}

fn common_dir(cwd: &Path) -> Option<PathBuf> {
    let result = git(cwd)
        .args(["rev-parse", "--path-format=absolute", "--git-common-dir"])
        .output()
        .ok()?;
    if !result.status.success() {
        return None;
    }
    Path::new(
        std::str::from_utf8(&result.stdout)
            .ok()?
            .trim_end_matches(['\r', '\n']),
    )
    .canonicalize()
    .ok()
}

pub(super) fn prune(dir: &Path) -> anyhow::Result<()> {
    let mut repos: BTreeMap<PathBuf, (PathBuf, Prefixes)> = BTreeMap::new();
    for (cwd, prefixes) in targets(dir)? {
        if !cwd.is_dir() {
            continue;
        }
        let Some(common) = common_dir(&cwd) else {
            continue;
        };
        repos
            .entry(common)
            .or_insert_with(|| (cwd, Prefixes::new()))
            .1
            .extend(prefixes);
    }
    for (_, (cwd, prefixes)) in repos {
        if let Err(error) = delete_refs(&cwd, &prefixes) {
            tracing::warn!(%error, path = %cwd.display(), "checkpoint ref cleanup failed");
        }
    }
    Ok(())
}

fn delete_refs(cwd: &Path, prefixes: &Prefixes) -> anyhow::Result<()> {
    const MAX_BYTES: u64 = 16 * 1024 * 1024;
    let mut child = git(cwd)
        .args([
            "for-each-ref",
            "--format=%(refname)",
            "refs/t3/checkpoints/",
            "refs/t3/orchestration-v2/checkpoints/",
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()?;
    let mut names = Vec::new();
    let read = child
        .stdout
        .take()
        .expect("piped stdout")
        .take(MAX_BYTES + 1)
        .read_to_end(&mut names);
    // Closing stdout above also releases a writer blocked on a truncated list.
    let status = child.wait()?;
    read?;
    anyhow::ensure!(
        names.len() as u64 <= MAX_BYTES,
        "checkpoint ref list exceeds 16 MiB"
    );
    anyhow::ensure!(status.success(), "git for-each-ref failed");
    let mut input = tempfile::tempfile()?;
    let mut any = false;
    // A non-UTF8 ref cannot be in either namespace with a numeric ordinal.
    // Inspect one line at a time so it cannot hide other owners' refs.
    for line in names.split(|&b| b == b'\n') {
        let Ok(name) = std::str::from_utf8(line) else {
            continue;
        };
        let Some((_, ordinal)) = name.rsplit_once('/') else {
            continue;
        };
        let prefix = &name[..name.len() - ordinal.len()];
        if prefixes.contains(prefix)
            && !ordinal.is_empty()
            && (ordinal == "0" || !ordinal.starts_with('0'))
            && ordinal.bytes().all(|b| b.is_ascii_digit())
        {
            write!(input, "delete {name}\0\0")?;
            any = true;
        }
    }
    if any {
        input.rewind()?;
        let output = git(cwd)
            .args(["update-ref", "--no-deref", "-z", "--stdin"])
            .stdin(input)
            .output()?;
        anyhow::ensure!(output.status.success(), "git update-ref failed");
    }
    Ok(())
}
