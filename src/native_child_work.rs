//! Read-only projection of provider-native Antigravity subagent work.
//!
//! Antigravity stores the parent invocation trajectory in SQLite and each
//! child transcript in its own profile tree. The records are deliberately
//! treated as untrusted. A child is returned only when the parent trajectory
//! proves the invocation and every path resolves to the managed profile.

use anyhow::{anyhow, Result};
use rusqlite::{Connection, Row};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{
    fs,
    path::{Path, PathBuf},
};
use url::Url;

const STEP_METADATA: u64 = 5;
const METADATA_TOOL_CALL: u64 = 4;
const STEP_PLANNER_RESPONSE: u64 = 20;
const PLANNER_TOOL_CALLS: u64 = 7;
const STEP_INVOKE_SUBAGENT: u64 = 143;
const INVOKE_SPECS: u64 = 9;
const INVOKE_RESULTS: u64 = 10;
const MAX_STEP_BYTES: usize = 1024 * 1024;
const MAX_INVOKE_ROWS: usize = 200;
const MAX_SCAN_BYTES: usize = 8 * 1024 * 1024;
const MAX_CHILDREN: usize = 16;
const TRANSCRIPT_BYTE_CAP: usize = 256 * 1024;
const BATCH_TRANSCRIPT_BYTES: usize = 512 * 1024;
const STEP_TEXT_CAP: usize = 4_000;
const TOOL_INPUT_CAP: usize = 1_000;
const TOOL_TEXT_LIMIT: usize = 8_000;
const TOOL_TEXT_TRUNCATED: &str = "[Earlier output truncated]\n\n";

#[derive(Clone, Debug)]
struct Launch {
    role: String,
    prompt: String,
}

#[derive(Clone, Debug)]
struct ChildRecord {
    conversation_id: String,
    log_uri: String,
}

#[derive(Clone, Debug)]
struct InvokeRecord {
    tool_call_id: Option<String>,
    launches: Vec<Launch>,
    children: Vec<ChildRecord>,
}

#[derive(Clone, Debug)]
struct BatchLaunch {
    title: Option<String>,
    prompt: String,
    native_task_id: String,
    run_id: String,
}

fn field(bytes: &[u8], number: u64) -> Result<Vec<&[u8]>> {
    let mut offset = 0usize;
    let mut out = Vec::new();
    while offset < bytes.len() {
        let key = varint(bytes, &mut offset)?;
        let wire = key % 8;
        let field = key / 8;
        match wire {
            0 => {
                let _ = varint(bytes, &mut offset)?;
            }
            1 => offset = offset.checked_add(8).ok_or_else(|| anyhow!("overflow"))?,
            5 => offset = offset.checked_add(4).ok_or_else(|| anyhow!("overflow"))?,
            2 => {
                let length = varint(bytes, &mut offset)? as usize;
                let end = offset
                    .checked_add(length)
                    .ok_or_else(|| anyhow!("overflow"))?;
                if end > bytes.len() {
                    return Err(anyhow!("truncated field"));
                }
                if field == number {
                    out.push(&bytes[offset..end]);
                }
                offset = end;
            }
            _ => return Err(anyhow!("unsupported wire type")),
        }
        if offset > bytes.len() {
            return Err(anyhow!("truncated field"));
        }
    }
    Ok(out)
}

fn varint(bytes: &[u8], offset: &mut usize) -> Result<u64> {
    let mut value = 0u64;
    for shift in (0..64).step_by(7) {
        let byte = *bytes
            .get(*offset)
            .ok_or_else(|| anyhow!("truncated varint"))?;
        *offset += 1;
        value |= u64::from(byte & 0x7f) << shift;
        if byte < 0x80 {
            return Ok(value);
        }
    }
    Err(anyhow!("overlong varint"))
}

fn first(bytes: &[u8], number: u64) -> Result<Option<&[u8]>> {
    Ok(field(bytes, number)?.into_iter().next())
}

fn text(bytes: &[u8], number: u64) -> Option<String> {
    first(bytes, number)
        .ok()
        .flatten()
        .and_then(|value| std::str::from_utf8(value).ok())
        .map(str::to_owned)
}

fn tool_call_id(bytes: &[u8]) -> Option<String> {
    let id = text(bytes, 1)?;
    (text(bytes, 2).as_deref() == Some("invoke_subagent")).then_some(id)
}

fn planner_call_ids(payload: &[u8]) -> Option<Vec<String>> {
    let planner = first(payload, STEP_PLANNER_RESPONSE).ok().flatten()?;
    let calls = field(planner, PLANNER_TOOL_CALLS).ok()?;
    Some(calls.into_iter().filter_map(tool_call_id).collect())
}

fn decode_invoke(payload: &[u8]) -> Option<InvokeRecord> {
    let invoke = first(payload, STEP_INVOKE_SUBAGENT).ok().flatten()?;
    let metadata = first(payload, STEP_METADATA).ok().flatten()?;
    let metadata_call = first(metadata, METADATA_TOOL_CALL).ok().flatten()?;
    let launches = field(invoke, INVOKE_SPECS)
        .ok()?
        .into_iter()
        .map(|spec| Launch {
            role: text(spec, 2).unwrap_or_default(),
            prompt: text(spec, 3).unwrap_or_default(),
        })
        .collect::<Vec<_>>();
    let children = field(invoke, INVOKE_RESULTS)
        .ok()?
        .into_iter()
        .map(|result| ChildRecord {
            conversation_id: text(result, 1).unwrap_or_default(),
            log_uri: text(result, 2).unwrap_or_default(),
        })
        .collect::<Vec<_>>();
    if !children
        .iter()
        .all(|child| valid_native_id(&child.conversation_id))
    {
        return None;
    }
    Some(InvokeRecord {
        tool_call_id: tool_call_id(metadata_call),
        launches,
        children,
    })
}

fn valid_native_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
}

fn describe(launches: &[Launch]) -> (Option<String>, String) {
    if launches.len() == 1 {
        let launch = &launches[0];
        return (Some(launch.role.clone()), launch.prompt.clone());
    }
    (
        Some(
            launches
                .iter()
                .map(|launch| launch.role.as_str())
                .collect::<Vec<_>>()
                .join(", "),
        ),
        launches
            .iter()
            .map(|launch| format!("{}\n{}", launch.role, launch.prompt))
            .collect::<Vec<_>>()
            .join("\n\n"),
    )
}

fn bound_tool_text(value: &str) -> String {
    if value.chars().count() <= TOOL_TEXT_LIMIT {
        return value.to_owned();
    }
    let tail = value
        .chars()
        .rev()
        .take(TOOL_TEXT_LIMIT)
        .collect::<String>()
        .chars()
        .rev()
        .collect::<String>();
    format!("{TOOL_TEXT_TRUNCATED}{tail}")
}

fn native_step_index(native_task_id: &str, root_session_id: &str) -> Option<Option<usize>> {
    let (session, index) = native_task_id.rsplit_once(':')?;
    let index = index.parse::<usize>().ok()?;
    if session != root_session_id {
        return Some(None);
    }
    Some(Some(index))
}

fn blob(row: &Row<'_>, column: usize) -> Option<Vec<u8>> {
    row.get::<_, Vec<u8>>(column).ok()
}

fn find_invoke_record(
    database: &Connection,
    root_session_id: &str,
    native_task_id: &str,
    launch: &BatchLaunch,
    siblings: &[BatchLaunch],
) -> Option<InvokeRecord> {
    if let Some(planner_index) = native_step_index(native_task_id, root_session_id) {
        let planner_index = planner_index?;
        let payload = database
            .query_row(
                "SELECT step_payload FROM steps WHERE idx = ?1 AND step_format = 0 AND length(step_payload) <= ?2",
                (planner_index as i64, MAX_STEP_BYTES as i64),
                |row| Ok(blob(row, 0)),
            )
            .ok()??;
        let call_ids = planner_call_ids(&payload)?;
        if call_ids.len() != 1 {
            return None;
        }
        let mut rows = 0usize;
        let mut bytes = 0usize;
        let mut statement = database
            .prepare("SELECT length(step_payload), step_payload FROM steps WHERE step_type = ?1 AND step_format = 0 AND idx > ?2 ORDER BY idx")
            .ok()?;
        let mut query = statement
            .query((STEP_INVOKE_SUBAGENT as i64, planner_index as i64))
            .ok()?;
        while let Some(row) = query.next().ok()? {
            rows += 1;
            let size = row.get::<_, i64>(0).ok()?.max(0) as usize;
            bytes = bytes.checked_add(size)?;
            if rows > MAX_INVOKE_ROWS || size > MAX_STEP_BYTES || bytes > MAX_SCAN_BYTES {
                return None;
            }
            let payload = blob(row, 1)?;
            if let Some(record) = decode_invoke(&payload) {
                if record.tool_call_id.as_deref() == call_ids.first().map(String::as_str) {
                    return complete(record);
                }
            }
        }
        return None;
    }

    let totals = database
        .query_row(
            "SELECT count(*), coalesce(sum(length(step_payload)), 0), coalesce(max(length(step_payload)), 0), coalesce(sum(step_format != 0), 0) FROM steps WHERE step_type = ?1",
            [STEP_INVOKE_SUBAGENT as i64],
            |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?, row.get::<_, i64>(2)?, row.get::<_, i64>(3)?)),
        )
        .ok()?;
    if totals.0 < 0
        || totals.1 < 0
        || totals.2 < 0
        || totals.3 != 0
        || totals.0 as usize > MAX_INVOKE_ROWS
        || totals.1 as usize > MAX_SCAN_BYTES
        || totals.2 as usize > MAX_STEP_BYTES
    {
        return None;
    }
    let same_text = |candidate: &BatchLaunch| {
        candidate.title == launch.title && candidate.prompt == launch.prompt
    };
    if siblings.iter().any(same_text) {
        return None;
    }
    let mut claimed = Vec::new();
    for sibling in siblings {
        let Some(Some(index)) = native_step_index(&sibling.native_task_id, root_session_id) else {
            continue;
        };
        let payload = database
            .query_row(
                "SELECT step_payload FROM steps WHERE idx = ?1 AND step_format = 0 AND length(step_payload) <= ?2",
                (index as i64, MAX_STEP_BYTES as i64),
                |row| Ok(blob(row, 0)),
            )
            .ok()
            .flatten();
        if let Some(payload) = payload {
            if let Some(ids) = planner_call_ids(&payload) {
                claimed.extend(ids);
            }
        }
    }
    let mut statement = database
        .prepare("SELECT step_payload FROM steps WHERE step_type = ?1 ORDER BY idx")
        .ok()?;
    let rows = statement
        .query_map([STEP_INVOKE_SUBAGENT as i64], |row| Ok(blob(row, 0)))
        .ok()?;
    let mut matches = Vec::new();
    for row in rows {
        let payload = row.ok()??;
        let record = decode_invoke(&payload)?;
        if record
            .tool_call_id
            .as_ref()
            .is_some_and(|id| claimed.iter().any(|claimed_id| claimed_id == id))
        {
            continue;
        }
        let described = describe(
            &record
                .launches
                .iter()
                .map(|launch| Launch {
                    role: bound_tool_text(&launch.role),
                    prompt: bound_tool_text(&launch.prompt),
                })
                .collect::<Vec<_>>(),
        );
        if described.0 == launch.title && described.1 == launch.prompt {
            matches.push(record);
        }
    }
    (matches.len() == 1)
        .then(|| complete(matches.remove(0)))
        .flatten()
}

fn complete(record: InvokeRecord) -> Option<InvokeRecord> {
    (record.launches.len() == record.children.len() && record.children.len() <= MAX_CHILDREN)
        .then_some(record)
}

fn managed_path(root: &Path, segments: &[&str], file: bool) -> Option<PathBuf> {
    let mut current = root.to_path_buf();
    for (index, segment) in segments.iter().enumerate() {
        current.push(segment);
        let metadata = fs::symlink_metadata(&current).ok()?;
        if metadata.file_type().is_symlink() {
            return None;
        }
        let expected_file = file && index == segments.len() - 1;
        if expected_file != metadata.is_file() || (!expected_file && !metadata.is_dir()) {
            return None;
        }
    }
    (fs::canonicalize(&current).ok()?.as_path() == current).then_some(current)
}

fn read_tail(path: &Path, cap: usize) -> Option<(String, bool)> {
    let metadata = fs::metadata(path).ok()?;
    let length = (metadata.len() as usize).min(cap);
    let mut file = fs::File::open(path).ok()?;
    use std::io::{Read, Seek, SeekFrom};
    if metadata.len() as usize > length {
        file.seek(SeekFrom::End(-(length as i64))).ok()?;
    }
    let mut bytes = vec![0u8; length];
    file.read_exact(&mut bytes).ok()?;
    let mut text = String::from_utf8_lossy(&bytes).into_owned();
    let truncated = metadata.len() as usize > length;
    if truncated {
        if let Some(newline) = text.find('\n') {
            text = text[newline + 1..].to_owned();
        }
    }
    Some((text, truncated))
}

fn cap(value: &str, limit: usize) -> String {
    if value.chars().count() <= limit {
        return value.to_owned();
    }
    value.chars().take(limit).collect::<String>() + "…"
}

fn transcript_steps(contents: &str, prompt: &str) -> Vec<Value> {
    let launch_prompt = prompt.trim();
    contents.lines().filter_map(|line| {
        let value: Value = serde_json::from_str(line).ok()?;
        let index = value["step_index"].as_i64().filter(|index| *index >= 0)? as u64;
        let kind = value["type"].as_str()?.to_owned();
        if index == 0 && kind == "SYSTEM_MESSAGE" && !launch_prompt.is_empty()
            && value["content"].as_str().is_some_and(|content| content.contains(launch_prompt))
        {
            return None;
        }
        let text = value["content"].as_str().or_else(|| value["error"].as_str()).map(|s| cap(s, STEP_TEXT_CAP));
        let tool_calls = value["tool_calls"].as_array().into_iter().flatten().filter_map(|call| {
            let name = call["name"].as_str()?.to_owned();
            let input = match &call["args"] {
                Value::Object(object) => {
                    let mut normalized = serde_json::Map::new();
                    for (key, value) in object {
                        if let Some(raw) = value.as_str() {
                            normalized.insert(key.clone(), serde_json::from_str(raw).unwrap_or_else(|_| Value::String(raw.to_owned())));
                        } else {
                            normalized.insert(key.clone(), value.clone());
                        }
                    }
                    serde_json::to_string(&Value::Object(normalized)).ok()?
                }
                Value::Null => "{}".to_owned(),
                value => serde_json::to_string(value).ok()?,
            };
            Some(json!({"name":name,"input":cap(&input, TOOL_INPUT_CAP)}))
        }).collect::<Vec<_>>();
        Some(json!({"index":index,"type":kind,"status":value["status"].as_str(),"text":text,"toolCalls":tool_calls}))
    }).collect()
}

fn profile_root(state_dir: &Path, instance_id: &str) -> Option<PathBuf> {
    let digest = Sha256::digest(instance_id.as_bytes());
    let key = hex::encode(digest);
    fs::canonicalize(state_dir.join("providers").join("antigravity").join(key)).ok()
}

fn file_url_path(value: &str) -> Option<PathBuf> {
    let url = Url::parse(value).ok()?;
    (url.scheme() == "file")
        .then(|| url.to_file_path().ok())
        .flatten()
}

fn read_child(
    profile_root: &Path,
    child: &ChildRecord,
    role: &str,
    prompt: &str,
    cap_bytes: usize,
) -> Value {
    let expected = managed_path(
        profile_root,
        &[
            "antigravity-acp",
            "brain",
            &child.conversation_id,
            ".system_generated",
            "logs",
            "transcript.jsonl",
        ],
        true,
    );
    let transcript = expected.and_then(|expected| {
        let named = file_url_path(&child.log_uri)?;
        if fs::canonicalize(named).ok()? != expected {
            return None;
        }
        let (contents, truncated) = read_tail(&expected, cap_bytes)?;
        Some((transcript_steps(&contents, prompt), truncated))
    });
    json!({
        "conversationId": child.conversation_id,
        "role": role,
        "prompt": prompt,
        "steps": transcript.as_ref().map(|value| value.0.clone()),
        "truncated": transcript.map(|value| value.1).unwrap_or(false)
    })
}

pub(crate) fn read(state_dir: &Path, child: &Value, parent: &Value) -> Vec<Value> {
    let lineage = &child["lineage"];
    let Some(_parent_id) = lineage["parentThreadId"].as_str() else {
        return Vec::new();
    };
    if lineage["relationshipToParent"] != "subagent" || child["forkedFrom"]["type"] != "node" {
        return Vec::new();
    }
    let node_id = child["forkedFrom"]["nodeId"].as_str().unwrap_or_default();
    let Some(subagent) = parent["subagents"].as_array().and_then(|items| {
        items
            .iter()
            .find(|item| item["id"] == node_id && item["childThreadId"] == child["id"])
    }) else {
        return Vec::new();
    };
    if subagent["driver"] != "antigravity"
        || subagent["origin"] != "provider_native"
        || subagent["runId"].is_null()
    {
        return Vec::new();
    }
    let Some(native_task_id) = subagent["nativeTaskRef"]["nativeId"].as_str() else {
        return Vec::new();
    };
    let instance_id = subagent["providerInstanceId"].as_str().unwrap_or_default();
    let runs = parent["runs"].as_array().cloned().unwrap_or_default();
    let provider_threads = parent["providerThreads"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    let attempts = parent["attempts"].as_array().cloned().unwrap_or_default();
    let provider_thread = |run_id: &str| {
        let provider_id =
            runs.iter().find(|run| run["id"] == run_id)?["providerThreadId"].as_str()?;
        provider_threads
            .iter()
            .find(|thread| thread["id"] == provider_id)
            .cloned()
    };
    let session_of = |run_id: &str| {
        let run = runs.iter().find(|run| run["id"] == run_id)?;
        let attempt_id = run["activeAttemptId"].as_str();
        attempts
            .iter()
            .find(|attempt| Some(attempt["id"].as_str().unwrap_or_default()) == attempt_id)
            .and_then(|attempt| attempt["nativeThreadId"].as_str().map(str::to_owned))
            .or_else(|| {
                provider_thread(run_id).and_then(|thread| {
                    thread["nativeThreadRef"]["nativeId"]
                        .as_str()
                        .map(str::to_owned)
                })
            })
    };
    let run_id = subagent["runId"].as_str().unwrap_or_default();
    let root_session = session_of(run_id).or_else(|| {
        provider_thread(run_id).and_then(|thread| {
            thread["nativeThreadRef"]["nativeId"]
                .as_str()
                .map(str::to_owned)
        })
    });
    let Some(root_session) = root_session.filter(|value| valid_native_id(value)) else {
        return Vec::new();
    };
    if provider_thread(run_id)
        .and_then(|thread| thread["providerInstanceId"].as_str().map(str::to_owned))
        .as_deref()
        != Some(instance_id)
    {
        return Vec::new();
    }
    let launches = |item: &Value, native_id: String| BatchLaunch {
        title: item["title"].as_str().map(str::to_owned),
        prompt: item["prompt"].as_str().unwrap_or_default().to_owned(),
        native_task_id: native_id,
        run_id: item["runId"].as_str().unwrap_or_default().to_owned(),
    };
    let siblings = parent["subagents"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|item| {
            item["id"] != subagent["id"]
                && item["driver"] == "antigravity"
                && item["origin"] == "provider_native"
                && item["providerInstanceId"] == instance_id
        })
        .filter_map(|item| {
            item["nativeTaskRef"]["nativeId"]
                .as_str()
                .map(|native_id| launches(item, native_id.to_owned()))
        })
        .filter(|item| {
            item.native_task_id == native_task_id
                || session_of(item.run_id.as_str()).as_deref() == Some(root_session.as_str())
        })
        .collect::<Vec<_>>();
    let profile = profile_root(state_dir, instance_id);
    let Some(profile) = profile else {
        return Vec::new();
    };
    let database_path = managed_path(
        &profile,
        &[
            "antigravity-acp",
            "conversations",
            &format!("{root_session}.db"),
        ],
        true,
    );
    let Some(database_path) = database_path else {
        return Vec::new();
    };
    let Ok(database) =
        Connection::open_with_flags(&database_path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
    else {
        return Vec::new();
    };
    let launch = launches(subagent, native_task_id.to_owned());
    let Some(record) =
        find_invoke_record(&database, &root_session, native_task_id, &launch, &siblings)
    else {
        return Vec::new();
    };
    let byte_cap = TRANSCRIPT_BYTE_CAP.min(BATCH_TRANSCRIPT_BYTES / record.children.len().max(1));
    record
        .children
        .iter()
        .enumerate()
        .map(|(index, child)| {
            read_child(
                &profile,
                child,
                &record.launches[index].role,
                &record.launches[index].prompt,
                byte_cap,
            )
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn varint(mut value: usize) -> Vec<u8> {
        let mut bytes = Vec::new();
        while value >= 0x80 {
            bytes.push((value as u8 & 0x7f) | 0x80);
            value >>= 7;
        }
        bytes.push(value as u8);
        bytes
    }

    fn proto_field(number: usize, bytes: &[u8]) -> Vec<u8> {
        let mut result = varint(number * 8 + 2);
        result.extend(varint(bytes.len()));
        result.extend(bytes);
        result
    }

    fn proto(parts: &[Vec<u8>]) -> Vec<u8> {
        parts.iter().flatten().copied().collect()
    }

    fn text_field(number: usize, value: &str) -> Vec<u8> {
        proto_field(number, value.as_bytes())
    }

    #[test]
    fn reads_proven_child_work_and_filters_launch_envelope() {
        let state = TempDir::new().unwrap();
        let instance = "antigravity";
        let profile = state
            .path()
            .join("providers/antigravity")
            .join(hex::encode(Sha256::digest(instance.as_bytes())));
        let child_id = "child-session";
        let transcript = profile.join(format!(
            "antigravity-acp/brain/{child_id}/.system_generated/logs/transcript.jsonl"
        ));
        fs::create_dir_all(transcript.parent().unwrap()).unwrap();
        fs::write(
            &transcript,
            format!(
                "{}\n{}\n",
                json!({"step_index":0,"type":"SYSTEM_MESSAGE","content":"launch prompt"}),
                json!({"step_index":1,"type":"VIEW_FILE","status":"DONE","content":"marker"})
            ),
        )
        .unwrap();
        let database_path = profile.join("antigravity-acp/conversations/root-session.db");
        fs::create_dir_all(database_path.parent().unwrap()).unwrap();
        let database = Connection::open(&database_path).unwrap();
        database
            .execute_batch(
                "CREATE TABLE steps (idx integer PRIMARY KEY, step_type integer NOT NULL DEFAULT 0, step_format integer NOT NULL DEFAULT 0, step_payload blob)",
            )
            .unwrap();
        let tool_call = proto(&[text_field(1, "call-1"), text_field(2, "invoke_subagent")]);
        let planner = proto(&[proto_field(20, &proto(&[proto_field(7, &tool_call)]))]);
        let metadata = proto(&[proto_field(4, &tool_call)]);
        let spec = proto(&[text_field(2, "Reader"), text_field(3, "launch prompt")]);
        let result = proto(&[
            text_field(1, child_id),
            text_field(2, Url::from_file_path(&transcript).unwrap().as_ref()),
        ]);
        let invoke = proto(&[proto_field(9, &spec), proto_field(10, &result)]);
        let payload = proto(&[
            proto_field(STEP_METADATA as usize, &metadata),
            proto_field(STEP_INVOKE_SUBAGENT as usize, &invoke),
        ]);
        database
            .execute(
                "INSERT INTO steps (idx, step_type, step_payload) VALUES (?1, ?2, ?3)",
                rusqlite::params![1i64, 15i64, planner],
            )
            .unwrap();
        database
            .execute(
                "INSERT INTO steps (idx, step_type, step_payload) VALUES (?1, ?2, ?3)",
                rusqlite::params![2i64, STEP_INVOKE_SUBAGENT as i64, payload],
            )
            .unwrap();
        let parent = json!({
            "id":"parent",
            "subagents":[{"id":"node-1","childThreadId":"child","driver":"antigravity","origin":"provider_native","providerInstanceId":instance,"runId":"run-1","nativeTaskRef":{"nativeId":"root-session:1"},"title":"Reader","prompt":"launch prompt"}],
            "runs":[{"id":"run-1","providerThreadId":"provider-1","activeAttemptId":null}],
            "attempts":[],
            "providerThreads":[{"id":"provider-1","providerInstanceId":instance,"nativeThreadRef":{"nativeId":"root-session"}}]
        });
        let child = json!({"id":"child","lineage":{"parentThreadId":"parent","relationshipToParent":"subagent"},"forkedFrom":{"type":"node","nodeId":"node-1"}});
        let children = read(state.path(), &child, &parent);
        assert_eq!(children.len(), 1);
        assert_eq!(children[0]["conversationId"], child_id);
        assert_eq!(children[0]["role"], "Reader");
        assert_eq!(children[0]["steps"].as_array().unwrap().len(), 1);
    }
}
