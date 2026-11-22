//! Desktop palette search and rendered timeline find, over the shared projection database.
mod find;
mod markdown;
mod older;
mod scoring;

pub(crate) use find::{find_thread, FindCache};
use indexmap::IndexMap;
use regex::Regex;
use rusqlite::{Connection, OpenFlags};
use scoring::Query;
use serde_json::{json, Value};
use std::{path::Path, sync::LazyLock};

const PLACEHOLDER: &str = "[User attached one or more files without additional text. Respond using the conversation context and the attached files.]";
const WHITESPACE: &str = "\t\n\u{b}\u{c}\r \u{a0}\u{1680}\u{2000}\u{2001}\u{2002}\u{2003}\u{2004}\u{2005}\u{2006}\u{2007}\u{2008}\u{2009}\u{200a}\u{2028}\u{2029}\u{202f}\u{205f}\u{3000}\u{feff}";
static LINK: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"!?\[([^\]\n]*)\]\([^)\s]*\)").unwrap());
static MARKS: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"\*\*|`+").unwrap());
const MESSAGE_TEXT: &str =
    "COALESCE(messages.search_text,json_extract(messages.payload_json,'$.text'))";
const FTS_CANDIDATES: &str = "orchestration_v2_message_search MATCH :fts AND orchestration_v2_message_search.rowid >= coalesce((
    SELECT rowid FROM orchestration_v2_message_search WHERE orchestration_v2_message_search MATCH :fts
    ORDER BY rowid DESC LIMIT 1 OFFSET 9999),0)";
const SCAN_CANDIDATES: &str = "messages.rowid > (SELECT coalesce(max(rowid),0) FROM orchestration_v2_projection_messages)-20000
    AND messages.streaming=0 AND (messages.role='assistant' OR
      (messages.role='user' AND coalesce(json_extract(messages.payload_json,'$.createdBy'),'user')='user'))";

fn reader(dir: &Path) -> rusqlite::Result<Connection> {
    Connection::open_with_flags(
        dir.join(crate::statev2::FILE_NAME),
        OpenFlags::SQLITE_OPEN_READ_ONLY,
    )
}
fn snippet(text: &str, offset: usize) -> String {
    let units: Vec<u16> = text.encode_utf16().collect();
    let mut start = offset.saturating_sub(24).min(units.len());
    if start > 0 {
        if let Some(i) = units[start..offset.min(units.len())]
            .iter()
            .position(|&c| char::from_u32(c.into()).is_some_and(char::is_whitespace))
        {
            start += i + 1;
        }
        if units
            .get(start)
            .is_some_and(|u| (0xdc00..=0xdfff).contains(u))
        {
            start += 1;
        }
    }
    let end = (start + 480).min(units.len());
    let raw = String::from_utf16_lossy(&units[start..end]);
    let shown = MARKS
        .replace_all(&raw, "")
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ");
    let snippet = truncate(&shown, 238);
    format!(
        "{}{}{}",
        if start > 0 { "…" } else { "" },
        snippet,
        if end < units.len() || snippet.len() < shown.len() {
            "…"
        } else {
            ""
        }
    )
}
fn truncate(s: &str, units: usize) -> &str {
    let mut len = 0;
    for (i, c) in s.char_indices() {
        len += c.len_utf16();
        if len > units {
            return &s[..i];
        }
    }
    s
}
fn usage(query: &Query, opens: &Value, activity: Option<&str>, now: i64) -> (f64, i64, bool) {
    let at = activity
        .and_then(|s| {
            time::OffsetDateTime::parse(s, &time::format_description::well_known::Rfc3339).ok()
        })
        .map(|at| (at.unix_timestamp_nanos() / 1_000_000) as i64);
    let decay = |at: i64| 2f64.powf(-((now.saturating_sub(at)).max(0) as f64) / (14. * 86400000.));
    let recent = at.map_or(0., decay);
    let opened = opens["openedAt"].as_i64().map_or(0., |at| {
        opens["score"].as_f64().unwrap_or_default() * decay(at)
    });
    let query = truncate(&query.normalized, 32).trim();
    let mut pick = (false, 0);
    for p in opens["picks"].as_array().into_iter().flatten() {
        let at = p["at"].as_i64().unwrap_or_default();
        let q = p["query"].as_str().unwrap_or_default();
        if now.saturating_sub(at) <= 21 * 86400000
            && (q == query || query.encode_utf16().count() >= 3 && q.starts_with(query))
        {
            pick = pick.max((q == query, at));
        }
    }
    (
        (1. + recent + 0.5 * (1. + opened).log2()).min(2.5),
        pick.1,
        pick.0,
    )
}
struct Hit {
    id: String,
    thread: String,
    project: String,
    role: String,
    text: String,
    date: Option<String>,
    quality: f64,
    offset: usize,
    count: usize,
    score: f64,
    picked: i64,
}
/// Where a message search looks and how it ranks: a client's search looks
/// everywhere and counts the user's picks; an agent's looks in one project,
/// leaves out the threads its title matches already list, and ignores picks.
#[derive(Default)]
struct Scope<'a> {
    project: Option<&'a str>,
    excluded: Vec<String>,
    no_picks: bool,
}

fn messages(
    conn: &Connection,
    q: &Query,
    limit: usize,
    now: i64,
    scope: &Scope,
) -> anyhow::Result<Vec<Value>> {
    if q.long.is_empty() {
        return Ok(vec![]);
    }
    let has_index: bool = conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE name='orchestration_v2_message_search')",
        [],
        |row| row.get(0),
    )?;
    let indexed = has_index && q.long.iter().all(|token| token.chars().count() >= 3);
    let mut narrow = q.short.iter().filter(|s| s.is_ascii()).collect::<Vec<_>>();
    if !indexed {
        narrow.extend(&q.long);
    }
    let substrings = json!(narrow
        .iter()
        .map(|s| json!({"token":s,"glob":scoring::glob(s)}))
        .collect::<Vec<_>>())
    .to_string();
    let spellings = json!(q
        .short
        .iter()
        .filter(|s| !s.is_ascii())
        .map(|s| scoring::spellings(s))
        .collect::<Vec<_>>())
    .to_string();
    let (source, candidates, rowid) = if indexed {
        ("orchestration_v2_message_search JOIN orchestration_v2_projection_messages messages ON messages.rowid=orchestration_v2_message_search.rowid",FTS_CANDIDATES,"orchestration_v2_message_search.rowid")
    } else {
        (
            "orchestration_v2_projection_messages messages",
            SCAN_CANDIDATES,
            "messages.rowid",
        )
    };
    let sql = include_str!("thread_search/messages.sql")
        .replace("{source}", source)
        .replace("{candidates}", candidates)
        .replace("{rowid}", rowid)
        .replace("{text}", MESSAGE_TEXT)
        .replace(
            "{folded}",
            &format!("replace(replace({MESSAGE_TEXT},'’',''''),'‘','''')"),
        );
    let mut stmt = conn.prepare(&sql)?;
    for (k, v) in [
        (":substrings", substrings.as_str()),
        (":spellings", &spellings),
        (":whitespace", WHITESPACE),
        (":placeholder", PLACEHOLDER),
    ] {
        stmt.raw_bind_parameter(stmt.parameter_index(k)?.unwrap(), v)?;
    }
    stmt.raw_bind_parameter(
        stmt.parameter_index(":excluded")?.unwrap(),
        json!(scope.excluded).to_string(),
    )?;
    stmt.raw_bind_parameter(stmt.parameter_index(":project")?.unwrap(), scope.project)?;
    if indexed {
        let fts = q
            .long
            .iter()
            .map(|s| format!("\"{}\"", s.replace('"', "\"\"")))
            .collect::<Vec<_>>()
            .join(" ");
        stmt.raw_bind_parameter(stmt.parameter_index(":fts")?.unwrap(), fts)?;
    }
    let mut rows = stmt.raw_query();
    let mut best = IndexMap::<String, Hit>::new();
    while let Some(row) = rows.next()? {
        let source: String = row.get(4)?;
        let text = LINK.replace_all(&source, "$1").into_owned();
        let Some((quality, offset)) = q.grade(&text) else {
            continue;
        };
        let thread: String = row.get(1)?;
        let mut hit = Hit {
            id: row.get(0)?,
            thread: thread.clone(),
            project: row.get(2)?,
            role: row.get(3)?,
            date: row.get(5)?,
            text,
            quality,
            offset,
            count: 1,
            score: 0.,
            picked: 0,
        };
        if let Some(old) = best.get_mut(&thread) {
            if quality < old.quality {
                continue;
            }
            if quality == old.quality {
                old.count += 1;
                if (hit.role == "user", &hit.date) <= (old.role == "user", &old.date) {
                    continue;
                }
                hit.count = old.count;
            }
        }
        best.insert(thread, hit);
    }
    // Read activity and pick history for all surviving threads in a single query.
    let ids = json!(best.keys().collect::<Vec<_>>()).to_string();
    let mut subjects = conn.prepare(&format!(
        "SELECT t.thread_id, json_extract(t.payload_json,'$.opens'), {LAST_USER}
         FROM orchestration_v2_projection_threads t WHERE t.thread_id IN (SELECT value FROM json_each(?1))"
    ))?;
    let mut subjects = subjects.query([ids])?;
    while let Some(row) = subjects.next()? {
        let id: String = row.get(0)?;
        let opens: Option<String> = row.get(1)?;
        let activity: Option<String> = row.get(2)?;
        let opens = opens
            .and_then(|s| serde_json::from_str(&s).ok())
            .unwrap_or_default();
        let (boost, picked, _) = usage(q, &opens, activity.as_deref(), now);
        if let Some(hit) = best.get_mut(&id) {
            hit.score = (1. + (1. + hit.count as f64).log2()) * boost;
            hit.picked = if scope.no_picks { 0 } else { picked };
        }
    }
    let mut hits = best.into_values().collect::<Vec<_>>();
    hits.sort_by(|a, b| {
        (b.quality >= 0.8)
            .cmp(&(a.quality >= 0.8))
            .then(b.picked.cmp(&a.picked))
            .then(b.quality.total_cmp(&a.quality))
            .then(b.score.total_cmp(&a.score))
            .then(b.date.cmp(&a.date))
    });
    Ok(hits
        .into_iter()
        .take(limit)
        .map(|hit| {
            json!({
                "threadId": hit.thread, "projectId": hit.project, "source": hit.role,
                "messageId": hit.id, "messageCreatedAt": hit.date, "quality": hit.quality,
                "hits": hit.count, "snippet": snippet(&hit.text,hit.offset)
            })
        })
        .collect())
}

const LAST_USER: &str = "(SELECT m.updated_at FROM orchestration_v2_projection_messages m
    WHERE m.thread_id=t.thread_id AND m.role='user'
      AND coalesce(json_extract(m.payload_json,'$.createdBy'),'user')='user'
    ORDER BY m.updated_at DESC,m.message_id DESC LIMIT 1)";

/// An agent's thread search (`ThreadSearch.searchThreads`): title, branch,
/// pull request and project matches ranked as the palette ranks them, without
/// the user's picks, then message matches in the other threads, which keep a
/// fifth of the `limit` slots.
pub(crate) fn search_agent_threads(
    dir: &Path,
    query: &str,
    limit: usize,
    project: Option<&str>,
) -> anyhow::Result<Vec<Value>> {
    let mut conn = reader(dir)?;
    let tx = conn.transaction()?;
    let q = Query::new(query);
    let now = crate::platform::now_ms();
    let (candidates, mut titles) = older::active(&tx, &q, project, now)?;
    titles.truncate(limit);
    let scope = Scope {
        project,
        excluded: titles
            .iter()
            .filter_map(|t| t["threadId"].as_str().map(str::to_owned))
            .collect(),
        no_picks: true,
    };
    let found = messages(&tx, &q, 50, now, &scope)?
        .into_iter()
        .filter_map(|mut hit| {
            let (_, _, title) = candidates
                .iter()
                .find(|(id, _, _)| hit["threadId"] == *id)?;
            let object = hit.as_object_mut()?;
            for key in ["quality", "hits", "messageId"] {
                object.remove(key);
            }
            object.insert("title".into(), json!(title));
            Some(hit)
        })
        .collect::<Vec<_>>();
    let slots = found.len().min(limit / 5);
    titles.truncate(limit - slots);
    titles.extend(found);
    titles.truncate(limit);
    Ok(titles)
}

pub(crate) fn search_threads(dir: &Path, input: &Value) -> anyhow::Result<Value> {
    let mut conn = reader(dir)?;
    let tx = conn.transaction()?;
    let q = Query::new(input["query"].as_str().unwrap_or_default());
    let now = crate::platform::now_ms();
    let hits = if input["titlesOnly"] == true {
        vec![]
    } else {
        messages(
            &tx,
            &q,
            input["limit"].as_u64().unwrap_or(50) as usize,
            now,
            &Scope::default(),
        )?
    };
    let mut result = json!({});
    if let Some(before) = input["olderSettledBefore"].as_str() {
        result["threads"] = json!(older::threads(&tx, &q, input, before, &hits, now)?);
        result["snapshotSequence"] = json!(crate::statev2::read::latest_sequence(&tx)?);
    }
    result["matches"] = Value::Array(hits);
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn messages_everywhere(
        db: &Connection,
        q: &Query,
        limit: usize,
        now: i64,
    ) -> anyhow::Result<Vec<Value>> {
        messages(db, q, limit, now, &Scope::default())
    }

    #[test]
    fn accent_and_short_word_filters_agree_with_and_without_the_index() {
        let home = tempfile::tempdir().unwrap();
        let db = crate::statev2::open(home.path()).unwrap();
        db.execute_batch("INSERT INTO projection_projects(project_id,title,workspace_root,scripts_json,created_at,updated_at,auto_pull,has_custom_title)
                VALUES('p','Project','/workspace','[]','2026','2026',0,1);
            INSERT INTO orchestration_v2_projection_threads(thread_id,project_id,title,default_provider,runtime_mode,interaction_mode,created_at,updated_at,payload_json)
                VALUES('t','p','Notes','codex','full-access','default','2026','2026','{}');").unwrap();
        for (id, role, text) in [
            ("a", "assistant", "École fermée à Lyon"),
            ("b", "user", "École fermée aussi"),
            ("c", "user", "ecole cafeteria"),
        ] {
            db.execute("INSERT INTO orchestration_v2_projection_messages(message_id,thread_id,role,streaming,created_at,updated_at,payload_json)
                VALUES(?1,'t',?2,0,'2026-01-01','2026-01-01',?3)",
                rusqlite::params![id,role,json!({"text":text}).to_string()]).unwrap();
        }
        for indexed in [true, false] {
            if !indexed {
                db.execute_batch("DROP TABLE orchestration_v2_message_search")
                    .unwrap();
            }
            let hits = messages_everywhere(&db, &Query::new("ecole fermée"), 50, 0).unwrap();
            assert_eq!(hits.len(), 1);
            assert_eq!(hits[0]["messageId"], "b");
            assert_eq!(hits[0]["quality"], 1.25);
            assert_eq!(hits[0]["hits"], 2);
            assert_eq!(hits[0]["snippet"], "École fermée aussi");
            assert!(!messages_everywhere(&db, &Query::new("fermée éc"), 50, 0)
                .unwrap()
                .is_empty());
            assert!(messages_everywhere(&db, &Query::new("fermée ce"), 50, 0)
                .unwrap()
                .is_empty());
        }
    }

    #[test]
    fn message_boundaries_and_snippets() {
        for (query, text, expected) in [
            ("mode", "JSONModel", Some((0.8, 4))),
            ("cafe", "7cafe52bb8", None),
            ("fts", "drafts", None),
            ("can’t", "I can't", Some((1., 2))),
            ("rust", "A trust", Some((0.3, 3))),
            (
                "lakebed perf",
                "😀 Perf results from Lakebed",
                Some((1.1, 3)),
            ),
        ] {
            assert_eq!(Query::new(query).grade(text), expected, "{query} in {text}");
        }
        let text = format!("{} needle and **some** highlighted text", "🌱 ".repeat(30));
        assert_eq!(
            snippet(&text, 91),
            "…🌱 🌱 🌱 🌱 🌱 🌱 🌱 needle and some highlighted text"
        );
    }

    #[test]
    fn in_thread_find_counts_displayed_chips_and_disclosures() {
        let segments =
            |kind, body| markdown::segments(&json!({"type":kind,"text":body}), Some("/repo"), &[]);
        assert_eq!(
            segments("user_message", "Before **a word**"),
            ["Before a word"]
        );
        assert_eq!(
            segments(
                "assistant_message",
                "<details><summary>Outer</summary><p>body</p></details>"
            ),
            ["Outer", "body"]
        );
        assert_eq!(
            segments(
                "user_message",
                "[hidden](t3-context://v1/terminal/t1)visible"
            ),
            ["visible"]
        );
    }
}
