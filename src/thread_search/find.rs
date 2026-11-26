//! A find scan and every navigation frame it produces describe one timeline revision.
use super::{markdown, reader};
use indexmap::IndexMap;
use rusqlite::{Connection, OptionalExtension};
use serde_json::{json, Value};
use std::{
    collections::{HashMap, HashSet},
    path::Path,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

struct Document {
    id: String,
    run: Value,
    count: usize,
}
type Scan = Arc<Vec<Document>>;
#[derive(Default)]
pub(crate) struct FindCache {
    scans: IndexMap<String, (u64, Scan)>,
    last: Option<Instant>,
}
impl FindCache {
    fn get(&mut self, key: &str, revision: u64) -> Option<Scan> {
        let now = Instant::now();
        if self
            .last
            .is_some_and(|t| now.duration_since(t) >= Duration::from_secs(60))
        {
            self.scans.clear();
        }
        self.last = Some(now);
        let (seq, docs) = self.scans.shift_remove(key)?;
        if seq != revision {
            return None;
        }
        self.scans.insert(key.to_owned(), (seq, docs.clone()));
        Some(docs)
    }
    fn insert(&mut self, key: String, revision: u64, docs: Scan) {
        self.last = Some(Instant::now());
        self.scans.shift_remove(&key);
        self.scans.insert(key, (revision, docs));
        if self.scans.len() > 8 {
            self.scans.shift_remove_index(0);
        }
    }
}
fn select(docs: &[Document], seq: u64, input: &Value) -> Value {
    let mut entries = vec![];
    let (mut total, mut anchor) = (0, 0);
    let start = &input["start"];
    for doc in docs {
        if start["entryId"] == doc.id {
            anchor = total + (start["occurrence"].as_u64().unwrap_or(0) as usize).min(doc.count);
        }
        if doc.count > 0 {
            entries.push(
                json!({"entryId":doc.id,"runId":doc.run,"count":doc.count,"startIndex":total}),
            );
        }
        total += doc.count;
    }
    let active = if total == 0 {
        0
    } else if let Some(i) = input["index"].as_u64() {
        (i as usize).min(total - 1)
    } else {
        ((anchor as i128 + i128::from(input["offset"].as_i64().unwrap_or(0)))
            .rem_euclid(total as i128)) as usize
    };
    let pos = entries
        .partition_point(|e| e["startIndex"].as_u64().unwrap() <= active as u64)
        .saturating_sub(1);
    let selected=entries.get(pos).map(|e|json!({"entryId":e["entryId"],"runId":e["runId"],"occurrence":active as u64-e["startIndex"].as_u64().unwrap()}));
    let nav = if entries.len() <= 17 {
        entries
    } else {
        (0..17)
            .map(|i| entries[(pos + i + entries.len() - 8) % entries.len()].clone())
            .collect()
    };
    json!({"snapshotSequence":seq,"totalMatches":total,"activeIndex":active,"navigation":nav,"match":selected})
}
fn payloads(db: &Connection, sql: &str, id: &str) -> anyhow::Result<Vec<Value>> {
    let mut stmt = db.prepare_cached(sql)?;
    let mut rows = stmt.query([id])?;
    let mut out = vec![];
    while let Some(row) = rows.next()? {
        out.push(serde_json::from_str(&row.get::<_, String>(0)?)?);
    }
    Ok(out)
}
/// Append the parent's inherited prefix before its local rows through the forked run.
fn timeline(
    db: &Connection,
    id: &str,
    seen: &mut HashSet<String>,
    inherited_run_ids: Option<&HashSet<String>>,
    out: &mut Vec<Value>,
) -> anyhow::Result<()> {
    if !seen.insert(id.to_owned()) {
        return Ok(());
    }
    let row: String = db.query_row(
        "SELECT payload_json FROM orchestration_v2_projection_threads WHERE thread_id=?1",
        [id],
        |r| r.get(0),
    )?;
    let t: Value = serde_json::from_str(&row)?;
    let mut runs = payloads(db, "SELECT payload_json FROM orchestration_v2_projection_runs WHERE thread_id=?1 ORDER BY ordinal", id)?;
    let attempts = payloads(
        db,
        "SELECT payload_json FROM orchestration_v2_projection_run_attempts WHERE thread_id=?1",
        id,
    )?;
    if inherited_run_ids.is_some() {
        for r in &mut runs {
            if r["status"] == "rolled_back" {
                r["status"] = if attempts
                    .iter()
                    .any(|a| a["runId"] == r["id"] && a["status"] == "cancelled")
                {
                    json!("cancelled")
                } else {
                    Value::Null
                };
            }
        }
    }
    let fork = &t["forkedFrom"];
    if fork["type"] == "run" && t["lineage"]["sideChat"] != true {
        if let Some(source) = fork["threadId"].as_str() {
            let ids = crate::statev2::read::fork_source_run_ids(db, id)?;
            timeline(db, source, seen, Some(&ids), out)?;
        }
    }
    let items = payloads(db, "SELECT payload_json FROM orchestration_v2_projection_turn_items
        WHERE thread_id=?1 AND (type IN ('user_message','assistant_message','proposed_plan','user_input_request')
          OR (type='dynamic_tool' AND run_id IN (
            SELECT run_id FROM orchestration_v2_projection_turn_items WHERE thread_id=?1
            AND type='assistant_message' AND json_extract(payload_json,'$.text')='')))
        ORDER BY ordinal,turn_item_id", id)?;
    let p = json!({"runs":runs,"attempts":attempts,"turnItems":items});
    out.extend(
        crate::projection_shell::visible_turn_items(&p)
            .into_iter()
            .filter(|i| {
                inherited_run_ids.is_none_or(|ids| {
                    i["runId"]
                        .as_str()
                        .map_or(t["historyOrigin"] == "v1_import", |id| ids.contains(id))
                })
            })
            .filter(|i| {
                matches!(
                    i["type"].as_str(),
                    Some(
                        "user_message"
                            | "assistant_message"
                            | "proposed_plan"
                            | "dynamic_tool"
                            | "user_input_request"
                    )
                )
            })
            .cloned(),
    );
    Ok(())
}
fn folded(s: &str) -> String {
    if s.is_ascii() {
        return s.to_ascii_lowercase();
    }
    s.chars()
        .map(|c| {
            let lower = c
                .to_lowercase()
                .collect::<String>()
                .replace('ς', "σ")
                .replace(['‘', '’'], "'");
            if lower.encode_utf16().count() == c.len_utf16() {
                lower
            } else {
                c.to_string()
            }
        })
        .collect()
}
fn embedded(item: &Value) -> bool {
    let tool = item["toolName"].as_str().unwrap_or_default();
    let output = &item["output"];
    if (tool == "execute" || tool.rsplit("__").next() == Some("html_render"))
        && item["status"] == "completed"
        && output["isError"] != true
        && output["htmlRender"]["attachmentId"].is_string()
    {
        return true;
    }
    let app = &output["t3McpApp"];
    app["server"]
        .as_str()
        .zip(app["tool"].as_str())
        .is_some_and(|(s, t)| tool == format!("{s}.{t}") || tool == format!("mcp__{s}__{t}"))
}
pub(crate) fn find_thread(
    dir: &Path,
    cache: &Mutex<FindCache>,
    input: &Value,
    first: Option<tokio::sync::oneshot::Sender<Value>>,
) -> anyhow::Result<Option<Value>> {
    let mut db = reader(dir)?;
    let tx = db.transaction()?;
    let id = input["threadId"].as_str().unwrap_or_default();
    let cwd = tx
        .query_row(
            "SELECT coalesce(json_extract(t.payload_json,'$.worktreePath'),p.workspace_root)
         FROM orchestration_v2_projection_threads t JOIN projection_projects p USING(project_id)
         WHERE t.thread_id=?1 AND t.deleted_at IS NULL AND p.deleted_at IS NULL",
            [id],
            |row| row.get::<_, Option<String>>(0),
        )
        .optional()?;
    let Some(cwd) = cwd else { return Ok(None) };
    let seq = tx.query_row(
        "WITH RECURSIVE sources(id) AS (
            SELECT ?1 UNION
            SELECT json_extract(t.payload_json,'$.forkedFrom.threadId')
            FROM orchestration_v2_projection_threads t JOIN sources s ON s.id=t.thread_id
            WHERE json_extract(t.payload_json,'$.forkedFrom.type')='run'
         )
         SELECT coalesce(max(sequence),0) FROM orchestration_events INDEXED BY idx_orch_events_stream_sequence
         WHERE application_event_version=2 AND aggregate_kind='thread' AND stream_id IN (SELECT id FROM sources)",
        [id], |row| row.get::<_,i64>(0),
    )? as u64;
    let skills = input["skills"].as_array().map_or(&[][..], Vec::as_slice);
    let query = input["query"].as_str().unwrap_or_default();
    let key = json!([id, cwd, skills, query]).to_string();
    if let Some(docs) = cache.lock().unwrap().get(&key, seq) {
        return Ok(Some(select(&docs, seq, input)));
    }
    let mut rows = vec![];
    timeline(&tx, id, &mut HashSet::new(), None, &mut rows)?;
    tx.commit()?;
    let answers = rows
        .iter()
        .filter(|i| i["type"] == "user_input_request")
        .filter_map(|i| {
            Some((
                format!(
                    "async-answer:{}",
                    i["questionAnswer"]["requestId"].as_str()?
                ),
                i["runId"].clone(),
            ))
        })
        .collect::<HashMap<_, _>>();
    let embedded = rows
        .iter()
        .filter(|i| i["type"] == "dynamic_tool" && embedded(i))
        .filter_map(|i| i["runId"].as_str().map(str::to_owned))
        .collect::<HashSet<_>>();
    rows.retain(|i| match i["type"].as_str() {
        Some("user_message") => {
            i["messageId"].as_str().and_then(|s| answers.get(s)) != Some(&i["runId"])
        }
        Some("assistant_message" | "proposed_plan") => true,
        _ => false,
    });
    let entry = |i: &Value| {
        i["messageId"]
            .as_str()
            .or(i["id"].as_str())
            .unwrap_or_default()
            .to_owned()
    };
    let anchor = rows
        .iter()
        .position(|r| input["start"]["entryId"] == entry(r))
        .unwrap_or_default();
    let mut first = first
        .filter(|_| input.get("index").is_none() && input["offset"].as_i64().unwrap_or(0) == 0);
    let mut docs = Vec::with_capacity(rows.len());
    let needle = folded(query);
    for step in 0..rows.len() {
        let row = &rows[(anchor + step) % rows.len()];
        let hidden = row["type"] == "assistant_message"
            && row["text"] == ""
            && row["runId"]
                .as_str()
                .is_some_and(|id| embedded.contains(id));
        let count = if hidden || needle.is_empty() {
            0
        } else {
            markdown::segments(row, cwd.as_deref(), skills)
                .iter()
                .map(|s| folded(s).matches(&needle).count())
                .sum()
        };
        let doc = Document {
            id: entry(row),
            run: row["runId"].clone(),
            count,
        };
        let occurrence = if step == 0 && input["start"]["entryId"] == doc.id {
            input["start"]["occurrence"].as_u64().unwrap_or(0) as usize
        } else {
            0
        };
        if count > occurrence {
            if let Some(sender) = first.take() {
                let _=sender.send(json!({"complete":false,"snapshotSequence":seq,"totalMatches":1,"activeIndex":0,"match":{"entryId":doc.id,"runId":doc.run,"occurrence":occurrence}}));
            }
        }
        docs.push(doc);
    }
    // An exhausted anchor can wrap to its own first occurrence.
    if let (Some(sender), Some(doc)) = (first, docs.first().filter(|d| d.count > 0)) {
        let _=sender.send(json!({"complete":false,"snapshotSequence":seq,"totalMatches":1,"activeIndex":0,"match":{"entryId":doc.id,"runId":doc.run,"occurrence":0}}));
    }
    docs.rotate_right(anchor);
    let result = select(&docs, seq, input);
    cache.lock().unwrap().insert(key, seq, Arc::new(docs));
    Ok(Some(result))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn navigation_wraps_seventeen_entries_around_the_selected_match() {
        let docs = (0..21)
            .map(|i| Document {
                id: i.to_string(),
                run: json!("r"),
                count: if i == 2 { 0 } else { 2 },
            })
            .collect::<Vec<_>>();
        let result = select(
            &docs,
            125,
            &json!({"start":{"entryId":"2","occurrence":1},"offset":-1}),
        );
        assert_eq!(
            result["match"],
            json!({"entryId":"1","runId":"r","occurrence":1})
        );
        assert_eq!(result["activeIndex"], 3);
        assert_eq!(result["totalMatches"], 40);
        let nav = result["navigation"].as_array().unwrap();
        assert_eq!(nav.len(), 17);
        assert_eq!(nav[8]["entryId"], "1");
        assert_eq!(nav[0]["entryId"], "14");
        assert_eq!(
            select(&docs, 125, &json!({"index":1000}))["activeIndex"],
            39
        );
    }
}
