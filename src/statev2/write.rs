//! Writes a commit to `statev2.sqlite` in one transaction, as the
//! reference's `EventSink` does: the events into `orchestration_events`
//! (application event version 2), each changed record into its
//! `orchestration_v2_projection_*` table with the scalar columns the
//! reference's `ProjectionStore.apply` copies out of it, turn item positions,
//! projects, the command receipt and the projection's high-water mark.
use crate::shared_store::{ProjectWrite, SharedCommit, SharedStore, StoredEvent, ThreadWrite};
use rusqlite::{params, Connection, OptionalExtension, ToSql};
use serde_json::Value;
use std::collections::HashMap;

pub struct Writer {
    conn: Connection,
}

impl Writer {
    pub fn new(conn: Connection) -> Self {
        Self { conn }
    }
}

impl SharedStore for Writer {
    fn pending_continuations(
        &mut self,
    ) -> anyhow::Result<Vec<crate::shared_store::PendingContinuation>> {
        Ok(super::outbox::pending_continuations(&self.conn)?)
    }

    fn pending_title_effects(
        &mut self,
    ) -> anyhow::Result<Vec<crate::title_generation::TitleEffect>> {
        Ok(super::outbox::pending_title_effects(&self.conn)?)
    }

    fn settle_effect(&mut self, effect_id: &str, status: &str) -> anyhow::Result<()> {
        super::outbox::settle(&self.conn, effect_id, status)?;
        Ok(())
    }

    fn shell_replay(
        &mut self,
        after: u64,
        head: u64,
        store: &crate::Store,
    ) -> anyhow::Result<Option<Vec<crate::shell_stream::Change>>> {
        use crate::shell_stream::{transcript_only, Change};
        let tx = self.conn.transaction()?;
        let pruned: Option<i64> = tx
            .query_row(
                "SELECT last_sequence FROM orchestration_v2_projection_metadata
             WHERE projection_name='event-log-pruned-through'",
                [],
                |r| r.get(0),
            )
            .optional()?;
        if pruned.is_some_and(|p| after < p as u64) {
            return Ok(None);
        }
        // Count the restored payload as the reference does, without transferring
        // transcript bodies from SQLite into each subscriber's replay buffer.
        let mut stmt = tx.prepare(
            "SELECT sequence, aggregate_kind, stream_id, event_type, json_extract(payload_json,'$.type'),
                    json_extract(payload_json,'$.lineage.relationshipToParent'),
                    octet_length(CASE WHEN event_type IN ('subagent.updated','turn-item.updated')
                        AND json_type(payload_json,'$.promptSequence')='integer'
                      THEN json_set(json_remove(payload_json,'$.promptSequence'),'$.prompt',
                        (SELECT json_extract(a.payload_json,'$.prompt') FROM orchestration_events a
                         WHERE a.sequence=json_extract(e.payload_json,'$.promptSequence')))
                      ELSE payload_json END)
             FROM orchestration_events e WHERE sequence > ?1 AND sequence <= ?2
               AND (aggregate_kind='project' OR (application_event_version=2 AND aggregate_kind='thread'))
             ORDER BY sequence")?;
        let mut rows = stmt.query(params![after as i64, head as i64])?;
        let mut changes = Vec::new();
        let mut bytes = 0i64;
        let mut children = std::collections::HashSet::new();
        while let Some(row) = rows.next()? {
            bytes += row.get::<_, i64>(6)?;
            if bytes > 8 * 1024 * 1024 {
                return Ok(None);
            }
            let id: String = row.get(2)?;
            let kind: String = row.get(3)?;
            if kind == "thread.created"
                && row.get::<_, Option<String>>(5)?.as_deref() == Some("subagent")
            {
                children.insert(id.clone());
            }
            let project = row.get::<_, String>(1)? == "project";
            if !project
                && transcript_only(&kind, row.get::<_, Option<String>>(4)?.as_deref())
                && (children.contains(&id)
                    || store
                        .threads
                        .get(&id)
                        .is_some_and(|t| t.row["lineage"]["relationshipToParent"] == "subagent"))
            {
                continue;
            }
            changes.push(Change {
                id,
                sequence: row.get::<_, i64>(0)? as u64,
                deleted: kind == "project.deleted",
                project,
            });
        }
        Ok(Some(changes))
    }

    fn write(&mut self, commit: &SharedCommit) -> anyhow::Result<()> {
        // IMMEDIATE, as the reference's writer: take the write lock up front.
        let transaction = self
            .conn
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        for event in &commit.events {
            insert_event(&transaction, event)?;
        }
        for thread in &commit.threads {
            write_thread(&transaction, thread)?;
        }
        // Capture fork membership once, after this transaction's run rows are in place.
        for event in &commit.events {
            let fork = &event.payload["forkedFrom"];
            if event.event_type == "thread.created" && fork["type"] == "run" {
                transaction.execute(
                    "UPDATE orchestration_v2_projection_threads
                     SET fork_run_ids_json = (
                         SELECT json_group_array(run_id) FROM orchestration_v2_projection_runs
                         WHERE thread_id = ?1 AND ordinal <= (
                             SELECT ordinal FROM orchestration_v2_projection_runs
                             WHERE thread_id = ?1 AND run_id = ?2
                         ) AND status <> 'rolled_back'
                     ) WHERE thread_id = ?3 AND fork_run_ids_json IS NULL",
                    params![
                        column(&fork["threadId"]),
                        column(&fork["runId"]),
                        event.stream_id
                    ],
                )?;
            }
        }
        for project in &commit.projects {
            write_project(&transaction, project)?;
        }
        for effect in &commit.effects {
            transaction.execute(
                "INSERT INTO orchestration_v2_effect_outbox (
                   effect_id, command_id, thread_id, effect_type, payload_json, status,
                   attempt_count, available_at, created_at, updated_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, 'pending', 0, ?6, ?6, ?6)
                 ON CONFLICT(effect_id) DO NOTHING",
                params![
                    effect.effect_id,
                    effect.command_id,
                    effect.thread_id,
                    effect.request["type"].as_str(),
                    effect.request.to_string(),
                    super::outbox::now_iso(),
                ],
            )?;
        }
        if let Some(receipt) = &commit.receipt {
            transaction.execute(
                "INSERT INTO orchestration_command_receipts
                   (command_id, aggregate_kind, aggregate_id, accepted_at, result_sequence, status, error, command_type)
                 VALUES (?1, ?2, ?3, ?4, ?5, 'accepted', NULL, ?6)
                 ON CONFLICT(command_id) DO UPDATE SET result_sequence = excluded.result_sequence",
                params![
                    receipt.command_id,
                    receipt.aggregate,
                    receipt.aggregate_id,
                    commit.occurred_at,
                    commit.sequence as i64,
                    if receipt.command_type.is_empty() { "legacy" } else { receipt.command_type.as_str() },
                ],
            )?;
        }
        if let Some(last) = commit
            .events
            .iter()
            .filter(|e| e.aggregate == "thread")
            .map(|e| e.sequence)
            .max()
        {
            transaction.execute(
                "INSERT INTO orchestration_v2_projection_metadata (projection_name, schema_version, last_sequence, updated_at)
                 VALUES ('thread-projections', 2, ?1, ?2)
                 ON CONFLICT(projection_name) DO UPDATE SET
                   schema_version = excluded.schema_version,
                   last_sequence = MAX(last_sequence, excluded.last_sequence),
                   updated_at = excluded.updated_at",
                params![last as i64, commit.occurred_at],
            )?;
        }
        transaction.commit()?;
        Ok(())
    }
}

fn insert_event(conn: &Connection, event: &StoredEvent) -> rusqlite::Result<()> {
    conn.execute(
        "INSERT INTO orchestration_events (
           sequence, event_id, aggregate_kind, stream_id, stream_version, event_type, occurred_at,
           command_id, causation_event_id, correlation_id, actor_kind, payload_json, metadata_json,
           application_event_version)
         VALUES (?1, ?2, ?3, ?4,
           COALESCE((SELECT MAX(stream_version) + 1 FROM orchestration_events
                     WHERE aggregate_kind = ?3 AND stream_id = ?4), 0),
           ?5, ?6, ?7, NULL, ?7, 'server', ?8, ?9, 2)",
        params![
            event.sequence as i64,
            event.event_id(),
            event.aggregate,
            event.stream_id,
            event.event_type,
            event.occurred_at,
            event.command_id,
            event.payload.to_string(),
            event.metadata.to_string(),
        ],
    )?;
    Ok(())
}

/// A JSON value as a column: strings as text, numbers and booleans as
/// numbers, null as NULL, anything else as its JSON text.
fn column(value: &Value) -> rusqlite::types::Value {
    use rusqlite::types::Value as Sql;
    match value {
        Value::Null => Sql::Null,
        Value::Bool(b) => Sql::Integer(*b as i64),
        Value::Number(n) => n
            .as_i64()
            .map(Sql::Integer)
            .or_else(|| n.as_f64().map(Sql::Real))
            .unwrap_or(Sql::Null),
        Value::String(s) => Sql::Text(s.clone()),
        other => Sql::Text(other.to_string()),
    }
}

/// How a collection's records map to their table: the table, its id
/// column, and each scalar column with where its value comes from.
struct Table {
    collection: &'static str,
    table: &'static str,
    id: &'static str,
    columns: &'static [(&'static str, Source)],
}

#[derive(Clone, Copy)]
enum Source {
    /// A payload field.
    Field(&'static str),
    /// A nested payload field.
    Path(&'static str, &'static str),
    /// The thread being written.
    Thread,
    /// A boolean field as 0/1.
    Flag(&'static str),
    /// `search_text` for a message.
    SearchText,
    /// `inline_work_output` for a turn item.
    InlineWorkOutput,
}

use Source::*;

const TABLES: &[Table] = &[
    Table {
        collection: "runs",
        table: "orchestration_v2_projection_runs",
        id: "run_id",
        columns: &[
            ("thread_id", Field("threadId")),
            ("ordinal", Field("ordinal")),
            ("provider", Field("providerInstanceId")),
            ("provider_thread_id", Field("providerThreadId")),
            ("status", Field("status")),
            ("requested_at", Field("requestedAt")),
            ("completed_at", Field("completedAt")),
            ("provider_instance_id", Field("providerInstanceId")),
        ],
    },
    Table {
        collection: "attempts",
        table: "orchestration_v2_projection_run_attempts",
        id: "attempt_id",
        columns: &[
            ("thread_id", Thread),
            ("run_id", Field("runId")),
            ("attempt_ordinal", Field("attemptOrdinal")),
            ("root_node_id", Field("rootNodeId")),
            ("provider", Field("providerInstanceId")),
            ("provider_thread_id", Field("providerThreadId")),
            ("provider_turn_id", Field("providerTurnId")),
            ("status", Field("status")),
            ("provider_instance_id", Field("providerInstanceId")),
        ],
    },
    Table {
        collection: "nodes",
        table: "orchestration_v2_projection_nodes",
        id: "node_id",
        columns: &[
            ("thread_id", Field("threadId")),
            ("run_id", Field("runId")),
            ("parent_node_id", Field("parentNodeId")),
            ("root_node_id", Field("rootNodeId")),
            ("kind", Field("kind")),
            ("status", Field("status")),
            ("provider_thread_id", Field("providerThreadId")),
            ("provider_turn_id", Field("providerTurnId")),
            ("started_at", Field("startedAt")),
            ("completed_at", Field("completedAt")),
        ],
    },
    Table {
        collection: "subagents",
        table: "orchestration_v2_projection_subagents",
        id: "subagent_id",
        columns: &[
            ("thread_id", Field("threadId")),
            ("run_id", Field("runId")),
            ("parent_node_id", Field("parentNodeId")),
            ("provider", Field("providerInstanceId")),
            ("driver", Field("driver")),
            ("provider_instance_id", Field("providerInstanceId")),
            ("provider_thread_id", Field("providerThreadId")),
            ("child_thread_id", Field("childThreadId")),
            ("origin", Field("origin")),
            ("status", Field("status")),
            ("started_at", Field("startedAt")),
            ("completed_at", Field("completedAt")),
            ("updated_at", Field("updatedAt")),
            (
                "completion_delivery_state",
                Path("completionDelivery", "state"),
            ),
        ],
    },
    Table {
        collection: "providerSessions",
        table: "orchestration_v2_projection_provider_sessions",
        id: "provider_session_id",
        columns: &[
            ("thread_id", Thread),
            ("provider", Field("providerInstanceId")),
            ("driver", Field("driver")),
            ("provider_instance_id", Field("providerInstanceId")),
            ("status", Field("status")),
            ("updated_at", Field("updatedAt")),
        ],
    },
    Table {
        collection: "providerThreads",
        table: "orchestration_v2_projection_provider_threads",
        id: "provider_thread_id",
        columns: &[
            ("thread_id", Field("appThreadId")),
            ("owner_node_id", Field("ownerNodeId")),
            ("provider", Field("providerInstanceId")),
            ("driver", Field("driver")),
            ("provider_instance_id", Field("providerInstanceId")),
            ("provider_session_id", Field("providerSessionId")),
            ("status", Field("status")),
            ("first_run_ordinal", Field("firstRunOrdinal")),
            ("last_run_ordinal", Field("lastRunOrdinal")),
            ("updated_at", Field("updatedAt")),
        ],
    },
    Table {
        collection: "providerTurns",
        table: "orchestration_v2_projection_provider_turns",
        id: "provider_turn_id",
        columns: &[
            ("thread_id", Thread),
            ("provider_thread_id", Field("providerThreadId")),
            ("node_id", Field("nodeId")),
            ("run_attempt_id", Field("runAttemptId")),
            ("ordinal", Field("ordinal")),
            ("status", Field("status")),
            ("started_at", Field("startedAt")),
            ("completed_at", Field("completedAt")),
        ],
    },
    Table {
        collection: "runtimeRequests",
        table: "orchestration_v2_projection_runtime_requests",
        id: "runtime_request_id",
        columns: &[
            ("thread_id", Thread),
            ("node_id", Field("nodeId")),
            ("provider_turn_id", Field("providerTurnId")),
            ("kind", Field("kind")),
            ("status", Field("status")),
            ("created_at", Field("createdAt")),
            ("resolved_at", Field("resolvedAt")),
        ],
    },
    Table {
        collection: "messages",
        table: "orchestration_v2_projection_messages",
        id: "message_id",
        columns: &[
            ("thread_id", Field("threadId")),
            ("run_id", Field("runId")),
            ("node_id", Field("nodeId")),
            ("role", Field("role")),
            ("streaming", Flag("streaming")),
            ("created_at", Field("createdAt")),
            ("updated_at", Field("updatedAt")),
            ("search_text", SearchText),
        ],
    },
    Table {
        collection: "plans",
        table: "orchestration_v2_projection_plans",
        id: "plan_id",
        columns: &[
            ("thread_id", Field("threadId")),
            ("run_id", Field("runId")),
            ("node_id", Field("nodeId")),
            ("kind", Field("kind")),
            ("status", Field("status")),
        ],
    },
    Table {
        collection: "turnItems",
        table: "orchestration_v2_projection_turn_items",
        id: "turn_item_id",
        columns: &[
            ("thread_id", Field("threadId")),
            ("run_id", Field("runId")),
            ("node_id", Field("nodeId")),
            ("provider_thread_id", Field("providerThreadId")),
            ("provider_turn_id", Field("providerTurnId")),
            ("parent_item_id", Field("parentItemId")),
            ("ordinal", Field("ordinal")),
            ("type", Field("type")),
            ("status", Field("status")),
            ("updated_at", Field("updatedAt")),
            ("inline_work_output", InlineWorkOutput),
        ],
    },
    Table {
        collection: "checkpointScopes",
        table: "orchestration_v2_projection_checkpoint_scopes",
        id: "scope_id",
        columns: &[
            ("thread_id", Field("threadId")),
            ("run_id", Field("runId")),
            ("node_id", Field("nodeId")),
            ("provider_thread_id", Field("providerThreadId")),
            ("kind", Field("kind")),
            ("ordinal_within_parent", Field("ordinalWithinParent")),
            ("advances_app_run_count", Flag("advancesAppRunCount")),
            ("created_at", Field("createdAt")),
        ],
    },
    Table {
        collection: "checkpoints",
        table: "orchestration_v2_projection_checkpoints",
        id: "checkpoint_id",
        columns: &[
            ("thread_id", Field("threadId")),
            ("scope_id", Field("scopeId")),
            ("run_id", Field("runId")),
            ("node_id", Field("nodeId")),
            ("ordinal_within_scope", Field("ordinalWithinScope")),
            ("app_run_ordinal", Field("appRunOrdinal")),
            ("status", Field("status")),
            ("captured_at", Field("capturedAt")),
        ],
    },
    Table {
        collection: "contextHandoffs",
        table: "orchestration_v2_projection_context_handoffs",
        id: "context_handoff_id",
        columns: &[
            ("thread_id", Field("threadId")),
            ("target_run_id", Field("targetRunId")),
            ("to_provider_thread_id", Field("toProviderThreadId")),
            ("strategy", Field("strategy")),
            ("status", Field("status")),
            ("updated_at", Field("updatedAt")),
        ],
    },
    Table {
        collection: "contextTransfers",
        table: "orchestration_v2_projection_context_transfers",
        id: "context_transfer_id",
        columns: &[
            ("source_thread_id", Field("sourceThreadId")),
            ("target_thread_id", Field("targetThreadId")),
            ("target_run_id", Field("targetRunId")),
            ("type", Field("type")),
            ("status", Field("status")),
            ("updated_at", Field("updatedAt")),
        ],
    },
];

/// `readableSearchText`: the text as it reads, or NULL when that is the
/// text itself. An assistant quote's link (`assistantCitationsToPlainText`)
/// becomes its quoted words and comment, and a file mention's escaped label
/// reads unescaped.
fn search_text(text: &str) -> Option<String> {
    static PATTERN: std::sync::OnceLock<regex::Regex> = std::sync::OnceLock::new();
    let pattern = PATTERN
        .get_or_init(|| regex::Regex::new(r"(!?\[)([^\]\n]*)(\]\([^)\s]*\))").expect("pattern"));
    let quoted = citations_to_plain_text(text);
    let readable = pattern.replace_all(&quoted, |captures: &regex::Captures| {
        let label = captures[2].replace("\\*", "*").replace("\\_", "_");
        format!("{}{}{}", &captures[1], label, &captures[3])
    });
    (readable != text).then(|| readable.into_owned())
}

const CITATION_HREF_PREFIX: &str = "t3-citation://v1/";
const CITATION_MAX_TEXT: usize = 8_000;
const CITATION_MAX_COMMENT: usize = 8_000;
const CITATION_CONTEXT: usize = 32;
const CITATION_MAX_ID: usize = 4_096;
/// `MAX_CITATION_HREF_LENGTH`.
const CITATION_MAX_HREF: usize = 9
    * (CITATION_MAX_TEXT + CITATION_MAX_COMMENT + 3 * CITATION_MAX_ID + 2 * CITATION_CONTEXT)
    + 1_000;

/// A string's length as JavaScript counts it (UTF-16 code units).
fn js_len(text: &str) -> usize {
    text.encode_utf16().count()
}

/// `assistantCitationsToPlainText`: each `[Assistant quote](t3-citation://…)`
/// link that parses as a citation becomes its text, then its comment.
pub(crate) fn citations_to_plain_text(text: &str) -> String {
    static LINK: std::sync::OnceLock<regex::Regex> = std::sync::OnceLock::new();
    let link = LINK.get_or_init(|| {
        regex::Regex::new(&format!(
            r"\[Assistant quote\]\(({}[^\s)]+)\)",
            regex::escape(CITATION_HREF_PREFIX)
        ))
        .expect("pattern")
    });
    link.replace_all(text, |captures: &regex::Captures| {
        let href = &captures[1];
        match parse_citation(href) {
            Some((quote, None)) => quote,
            Some((quote, Some(comment))) => format!("{quote}\nComment: {comment}"),
            None => captures[0].to_owned(),
        }
    })
    .into_owned()
}

/// `parseAssistantCitationHref` and the `AssistantCitation` schema's checks:
/// the citation's text and comment, when `href` is a valid citation.
pub(crate) fn parse_citation(href: &str) -> Option<(String, Option<String>)> {
    if js_len(href) > CITATION_MAX_HREF {
        return None;
    }
    let url = url::Url::parse(href).ok()?;
    let parts: Vec<&str> = url.path().trim_start_matches('/').split('/').collect();
    if url.scheme() != "t3-citation"
        || url.host_str() != Some("v1")
        || parts.len() != 3
        || !url.username().is_empty()
        || url.password().is_some()
        || url.port().is_some()
        || url.fragment().is_some()
    {
        return None;
    }
    let pairs: Vec<(String, String)> = url
        .query_pairs()
        .map(|(k, v)| (k.into_owned(), v.into_owned()))
        .collect();
    let get = |key: &str| -> Vec<&String> {
        pairs
            .iter()
            .filter(|(k, _)| k == key)
            .map(|(_, v)| v)
            .collect()
    };
    let comment = get("comment").first().map(|value| (*value).clone());
    let required = ["text", "start", "end", "prefix", "suffix"];
    if pairs.len() != required.len() + usize::from(comment.is_some())
        || required.iter().any(|key| get(key).len() != 1)
    {
        return None;
    }
    let value = |key: &str| get(key)[0].clone();
    let number = |key: &str| -> Option<u64> {
        let text = value(key);
        (!text.is_empty() && text.len() <= 16 && text.bytes().all(|b| b.is_ascii_digit()))
            .then(|| text.parse().ok())
            .flatten()
    };
    let (start, end) = (number("start")?, number("end")?);
    let ids: Vec<String> = parts
        .iter()
        .map(|part| {
            percent_encoding::percent_decode_str(part)
                .decode_utf8()
                .map(|s| s.into_owned())
        })
        .collect::<Result<_, _>>()
        .ok()?;
    let text = value("text");
    let valid = ids
        .iter()
        .all(|id| !id.trim().is_empty() && js_len(id) <= CITATION_MAX_ID)
        && !text.is_empty()
        && js_len(&text) <= CITATION_MAX_TEXT
        && comment
            .as_ref()
            .is_none_or(|c| js_len(c) <= CITATION_MAX_COMMENT)
        && start <= 9_007_199_254_740_991
        && end <= 9_007_199_254_740_991
        && js_len(&value("prefix")) <= CITATION_CONTEXT
        && js_len(&value("suffix")) <= CITATION_CONTEXT
        && end > start
        && !text.trim().is_empty();
    valid.then_some((text, comment))
}

/// `isInlineWorkOutput` for a `dynamic_tool` item's stored JSON.
fn inline_work_output(item: &Value, payload: &str) -> bool {
    if item["type"] != "dynamic_tool" {
        return false;
    }
    let lower = payload.to_lowercase();
    if [
        "htmlrender",
        "t3mcpapp",
        "\"viewedimagepath\"",
        "sentmessage",
    ]
    .iter()
    .any(|needle| lower.contains(needle))
    {
        return true;
    }
    let Some(start) = lower.find("\"toolname\":\"") else {
        return false;
    };
    let rest = &lower[start + "\"toolname\":\"".len()..];
    static TOOL: std::sync::OnceLock<regex::Regex> = std::sync::OnceLock::new();
    TOOL.get_or_init(|| {
        regex::Regex::new(r#"(?s)(?:delegate.task|send.to.user)""#).expect("pattern")
    })
    .is_match(rest)
}

fn value_of(
    source: Source,
    record: &Value,
    thread_id: &str,
    payload: &str,
) -> rusqlite::types::Value {
    use rusqlite::types::Value as Sql;
    match source {
        Field(key) => column(&record[key]),
        Path(outer, inner) => column(&record[outer][inner]),
        Thread => Sql::Text(thread_id.to_owned()),
        Flag(key) => Sql::Integer((record[key] == true) as i64),
        SearchText => record["text"]
            .as_str()
            .and_then(search_text)
            .map_or(Sql::Null, Sql::Text),
        InlineWorkOutput => Sql::Integer(inline_work_output(record, payload) as i64),
    }
}

fn by_id(projection: &Value, key: &str) -> HashMap<String, Value> {
    projection[key]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|r| Some((r["id"].as_str()?.to_owned(), r.clone())))
        .collect()
}

fn write_thread_row(conn: &Connection, thread: &Value) -> rusqlite::Result<()> {
    // As Rust's run rows fall back for a thread created without a model.
    let instance = thread["providerInstanceId"]
        .as_str()
        .or(thread["modelSelection"]["instanceId"].as_str())
        .unwrap_or("codex")
        .to_owned();
    conn.execute(
        "INSERT INTO orchestration_v2_projection_threads (
           thread_id, project_id, title, default_provider, runtime_mode, interaction_mode,
           active_provider_thread_id, created_at, updated_at, archived_at, deleted_at, payload_json, provider_instance_id)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?4)
         ON CONFLICT(thread_id) DO UPDATE SET
           project_id = excluded.project_id, title = excluded.title,
           default_provider = excluded.default_provider, runtime_mode = excluded.runtime_mode,
           interaction_mode = excluded.interaction_mode,
           active_provider_thread_id = excluded.active_provider_thread_id,
           created_at = excluded.created_at, updated_at = excluded.updated_at,
           archived_at = excluded.archived_at, deleted_at = excluded.deleted_at,
           payload_json = excluded.payload_json, provider_instance_id = excluded.provider_instance_id",
        params![
            column(&thread["id"]),
            column(&thread["projectId"]),
            column(&thread["title"]),
            instance,
            column(&thread["runtimeMode"]),
            column(&thread["interactionMode"]),
            column(&thread["activeProviderThreadId"]),
            column(&thread["createdAt"]),
            column(&thread["updatedAt"]),
            column(&thread["archivedAt"]),
            column(&thread["deletedAt"]),
            thread.to_string(),
        ],
    )?;
    Ok(())
}

fn write_thread(conn: &Connection, write: &ThreadWrite) -> rusqlite::Result<()> {
    let known: bool = conn
        .query_row(
            "SELECT 1 FROM orchestration_v2_projection_threads WHERE thread_id = ?1",
            [&write.thread_id],
            |_| Ok(true),
        )
        .optional()?
        .unwrap_or(false);
    // A thread the database has no row for gets every record.
    let before = if known { write.before.as_ref() } else { None };
    if before.map(|b| &b["thread"]) != Some(&write.after["thread"]) {
        write_thread_row(conn, &write.after["thread"])?;
    }
    if write.deleted {
        return Ok(());
    }
    for table in TABLES {
        let old = before
            .map(|b| by_id(b, table.collection))
            .unwrap_or_default();
        let records = write.after[table.collection]
            .as_array()
            .cloned()
            .unwrap_or_default();
        let current: std::collections::HashSet<&str> =
            records.iter().filter_map(|r| r["id"].as_str()).collect();
        for id in old.keys().filter(|id| !current.contains(id.as_str())) {
            // A provider session leaves a thread by its binding, as the
            // reference's provider-session.detached does: the session row
            // stays (a Codex instance's session is shared by its threads).
            if table.collection == "providerSessions" {
                conn.execute(
                    "DELETE FROM orchestration_v2_projection_provider_session_bindings
                     WHERE provider_session_id = ?1 AND thread_id = ?2",
                    params![id, write.thread_id],
                )?;
                continue;
            }
            conn.execute(
                &format!("DELETE FROM {} WHERE {} = ?1", table.table, table.id),
                [id],
            )?;
            if table.collection == "turnItems" {
                conn.execute(
                    "DELETE FROM orchestration_v2_turn_item_positions WHERE thread_id = ?1 AND turn_item_id = ?2",
                    params![write.thread_id, id],
                )?;
            }
        }
        let changed: Vec<&Value> = records
            .iter()
            .filter(|r| r["id"].as_str().is_some_and(|id| old.get(id) != Some(*r)))
            .collect();
        // Delete before inserting, so records that trade ordinals (a run
        // reorder) never collide on a unique index mid-commit.
        for record in &changed {
            conn.execute(
                &format!("DELETE FROM {} WHERE {} = ?1", table.table, table.id),
                [record["id"].as_str()],
            )?;
            if table.collection == "turnItems" {
                conn.execute(
                    "DELETE FROM orchestration_v2_turn_item_positions WHERE thread_id = ?1 AND turn_item_id = ?2",
                    params![write.thread_id, record["id"].as_str()],
                )?;
            }
        }
        for record in changed {
            write_record(conn, table, &write.thread_id, record)?;
            if table.collection == "providerSessions" {
                conn.execute(
                    "INSERT OR IGNORE INTO orchestration_v2_projection_provider_session_bindings
                       (provider_session_id, thread_id) VALUES (?1, ?2)",
                    params![record["id"].as_str(), write.thread_id],
                )?;
            }
            if table.collection == "turnItems" {
                // The reference allocates item ordinals from this table; keep
                // each item at the ordinal Rust gave it.
                conn.execute(
                    "INSERT OR IGNORE INTO orchestration_v2_turn_item_positions (thread_id, turn_item_id, ordinal)
                     VALUES (?1, ?2, ?3)",
                    params![write.thread_id, record["id"].as_str(), record["ordinal"].as_i64()],
                )?;
            }
        }
    }
    Ok(())
}

fn write_record(
    conn: &Connection,
    table: &Table,
    thread_id: &str,
    record: &Value,
) -> rusqlite::Result<()> {
    let payload = record.to_string();
    let mut names = vec![table.id];
    let mut values: Vec<rusqlite::types::Value> = vec![column(&record["id"])];
    for (name, source) in table.columns {
        names.push(name);
        values.push(value_of(*source, record, thread_id, &payload));
    }
    names.push("payload_json");
    values.push(rusqlite::types::Value::Text(payload));
    let placeholders: Vec<String> = (1..=names.len()).map(|i| format!("?{i}")).collect();
    let sql = format!(
        "INSERT INTO {} ({}) VALUES ({})",
        table.table,
        names.join(", "),
        placeholders.join(", ")
    );
    let params: Vec<&dyn ToSql> = values.iter().map(|v| v as &dyn ToSql).collect();
    conn.execute(&sql, params.as_slice())?;
    Ok(())
}

fn write_project(conn: &Connection, project: &ProjectWrite) -> rusqlite::Result<()> {
    let row = &project.row;
    let json_or_null = |value: &Value| {
        if value.is_null() {
            rusqlite::types::Value::Null
        } else {
            rusqlite::types::Value::Text(value.to_string())
        }
    };
    conn.execute(
        "INSERT INTO projection_projects (
           project_id, title, has_custom_title, workspace_root, default_model_selection_json,
           default_thread_env_mode, auto_pull, favicon_path, project_icon_json, scripts_json,
           pinned_at, archived_at, created_at, updated_at, deleted_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15)
         ON CONFLICT(project_id) DO UPDATE SET
           title = excluded.title, has_custom_title = excluded.has_custom_title,
           workspace_root = excluded.workspace_root,
           default_model_selection_json = excluded.default_model_selection_json,
           default_thread_env_mode = excluded.default_thread_env_mode,
           auto_pull = excluded.auto_pull, favicon_path = excluded.favicon_path,
           project_icon_json = excluded.project_icon_json, scripts_json = excluded.scripts_json,
           pinned_at = excluded.pinned_at, archived_at = excluded.archived_at,
           created_at = excluded.created_at, updated_at = excluded.updated_at,
           deleted_at = excluded.deleted_at",
        params![
            project.project_id,
            column(&row["title"]),
            (row["hasCustomTitle"] == true) as i64,
            column(&row["workspaceRoot"]),
            json_or_null(&row["defaultModelSelection"]),
            column(&row["defaultThreadEnvMode"]),
            (row["autoPull"] == true) as i64,
            column(&row["faviconPath"]),
            json_or_null(&row["projectIcon"]),
            if row["scripts"].is_array() {
                row["scripts"].to_string()
            } else {
                "[]".into()
            },
            column(&row["pinnedAt"]),
            column(&row["archivedAt"]),
            column(&row["createdAt"]),
            column(&row["updatedAt"]),
            column(&row["deletedAt"]),
        ],
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn commit(threads: Vec<ThreadWrite>, events: Vec<StoredEvent>) -> SharedCommit {
        SharedCommit {
            threads,
            projects: vec![],
            sequence: events.last().map_or(0, |e| e.sequence),
            events,
            receipt: None,
            effects: vec![],
            occurred_at: "2026-01-01T00:00:01.000Z".into(),
        }
    }

    fn thread(title: &str) -> Value {
        json!({"id": "thread:w", "projectId": "project:w", "title": title, "providerInstanceId": "codex",
            "runtimeMode": "full-access", "interactionMode": "default", "activeProviderThreadId": null,
            "createdAt": "2026-01-01T00:00:00.000Z", "updatedAt": "2026-01-01T00:00:00.000Z",
            "archivedAt": null, "deletedAt": null})
    }

    #[test]
    fn a_fork_keeps_its_source_runs_across_rollbacks_and_thread_edits() {
        let home = tempfile::tempdir().unwrap();
        let mut writer = Writer::new(crate::statev2::open(home.path()).unwrap());
        let mut source = crate::shared_store::empty_projection(&thread("source"));
        let at = "2026-01-01T00:00:00.000Z";
        source["runs"] = json!([
            {"id":"a","ordinal":1,"threadId":"thread:w","providerInstanceId":"codex",
             "status":"rolled_back","requestedAt":at,"completedAt":at},
            {"id":"b","ordinal":2,"threadId":"thread:w","providerInstanceId":"codex",
             "status":"completed","requestedAt":at,"completedAt":at}
        ]);
        let mut fork = thread("fork");
        fork["id"] = json!("thread:f");
        fork["forkedFrom"] = json!({"type":"run","threadId":"thread:w","runId":"b"});
        let write = |after: Value| ThreadWrite {
            thread_id: after["thread"]["id"].as_str().unwrap().to_owned(),
            before: None,
            after,
            deleted: false,
        };
        writer
            .write(&commit(
                vec![
                    write(source),
                    write(crate::shared_store::empty_projection(&fork)),
                ],
                vec![StoredEvent {
                    sequence: 1,
                    aggregate: "thread",
                    stream_id: "thread:f".into(),
                    event_type: "thread.created".into(),
                    occurred_at: at.into(),
                    command_id: None,
                    payload: fork.clone(),
                    metadata: json!({}),
                }],
            ))
            .unwrap();

        writer
            .conn
            .execute(
                "UPDATE orchestration_v2_projection_runs SET status='rolled_back'",
                [],
            )
            .unwrap();
        fork["title"] = json!("renamed");
        writer
            .write(&commit(
                vec![write(crate::shared_store::empty_projection(&fork))],
                vec![],
            ))
            .unwrap();
        let inherited =
            || crate::statev2::read::fork_source_run_ids(&writer.conn, "thread:f").unwrap();
        assert_eq!(inherited(), ["b".to_owned()].into());

        // Before migration 116, equal timestamps are ambiguous and both runs
        // are visible. A rollback strictly before the fork is excluded.
        writer
            .conn
            .execute(
                "UPDATE orchestration_v2_projection_threads SET fork_run_ids_json=NULL",
                [],
            )
            .unwrap();
        assert_eq!(inherited(), ["a".to_owned(), "b".to_owned()].into());
        writer.conn.execute("UPDATE orchestration_v2_projection_runs SET completed_at='2025-12-31T23:59:59Z' WHERE run_id='a'", []).unwrap();
        assert_eq!(inherited(), ["b".to_owned()].into());
    }

    #[test]
    fn writes_rows_events_and_positions_then_only_what_changed() {
        let home = tempfile::tempdir().unwrap();
        let mut writer = Writer::new(crate::statev2::open(home.path()).unwrap());
        let mut first = crate::shared_store::empty_projection(&thread("A"));
        first["runs"] = json!([{"id": "run:1", "threadId": "thread:w", "ordinal": 1, "providerInstanceId": "codex",
            "providerThreadId": null, "status": "completed", "requestedAt": "x", "completedAt": "y"}]);
        first["turnItems"] = json!([{"id": "item:1", "threadId": "thread:w", "runId": "run:1", "nodeId": null,
            "providerThreadId": null, "providerTurnId": null, "parentItemId": null, "ordinal": 1000001,
            "type": "user_message", "status": "completed", "updatedAt": "x"}]);
        first["messages"] = json!([{"id": "m", "threadId": "thread:w", "runId": "run:1", "nodeId": null,
            "role": "user", "streaming": false, "createdAt": "x", "updatedAt": "x", "text": "see [a\\_b](file:///a_b)"}]);
        let event = StoredEvent {
            sequence: 7,
            aggregate: "thread",
            stream_id: "thread:w".into(),
            event_type: "thread.created".into(),
            occurred_at: "x".into(),
            command_id: Some("c".into()),
            payload: thread("A"),
            metadata: json!({}),
        };
        writer
            .write(&commit(
                vec![ThreadWrite {
                    thread_id: "thread:w".into(),
                    before: None,
                    after: first.clone(),
                    deleted: false,
                }],
                vec![event],
            ))
            .unwrap();
        let conn = &writer.conn;
        let read = crate::statev2::read::thread_projection(conn, "thread:w")
            .unwrap()
            .unwrap();
        assert_eq!(read["runs"], first["runs"]);
        assert_eq!(read["turnItems"], first["turnItems"]);
        let search: Option<String> = conn
            .query_row(
                "SELECT search_text FROM orchestration_v2_projection_messages",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(search.as_deref(), Some("see [a_b](file:///a_b)"));
        let ordinal: i64 = conn
            .query_row(
                "SELECT ordinal FROM orchestration_v2_turn_item_positions",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(ordinal, 1000001);
        let (sequence, version, kind): (i64, i64, String) = conn
            .query_row(
                "SELECT sequence, stream_version, aggregate_kind FROM orchestration_events",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .unwrap();
        assert_eq!((sequence, version, kind.as_str()), (7, 0, "thread"));
        assert_eq!(
            crate::statev2::read::thread_sequence(conn, "thread:w").unwrap(),
            7
        );
        let last: i64 = conn
            .query_row(
                "SELECT last_sequence FROM orchestration_v2_projection_metadata",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(last, 7);

        // A second run with the first one reordered after it.
        let mut second = first.clone();
        second["runs"] = json!([
            {"id": "run:2", "threadId": "thread:w", "ordinal": 1, "providerInstanceId": "codex", "providerThreadId": null, "status": "queued", "requestedAt": "x", "completedAt": null},
            {"id": "run:1", "threadId": "thread:w", "ordinal": 2, "providerInstanceId": "codex", "providerThreadId": null, "status": "completed", "requestedAt": "x", "completedAt": "y"}]);
        second["turnItems"] = json!([]);
        writer
            .write(&commit(
                vec![ThreadWrite {
                    thread_id: "thread:w".into(),
                    before: Some(first),
                    after: second.clone(),
                    deleted: false,
                }],
                vec![],
            ))
            .unwrap();
        let read = crate::statev2::read::thread_projection(&writer.conn, "thread:w")
            .unwrap()
            .unwrap();
        assert_eq!(read["runs"], second["runs"]);
        assert_eq!(read["turnItems"], json!([]));
        let positions: i64 = writer
            .conn
            .query_row(
                "SELECT COUNT(*) FROM orchestration_v2_turn_item_positions",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(positions, 0);
    }

    #[test]
    fn search_text_reads_quotes_and_file_mentions_as_they_read() {
        let href = "t3-citation://v1/env/thread%3Aa/message%3Ab?text=the+quoted+words&start=3&end=9&prefix=&suffix=";
        assert_eq!(
            search_text(&format!("Look: [Assistant quote]({href}) ok")).as_deref(),
            Some("Look: the quoted words ok")
        );
        let commented = format!("{href}&comment=why%3F");
        assert_eq!(
            search_text(&format!("[Assistant quote]({commented})")).as_deref(),
            Some("the quoted words\nComment: why?")
        );
        // Not a citation: kept as written, so nothing extra is stored.
        assert_eq!(
            search_text("[Assistant quote](t3-citation://v1/a/b?text=x)"),
            None
        );
        let backwards = "t3-citation://v1/e/t/m?text=x&start=9&end=3&prefix=&suffix=";
        assert_eq!(
            search_text(&format!("[Assistant quote]({backwards})")),
            None
        );
        assert_eq!(search_text("plain"), None);
        assert_eq!(
            search_text("see [a\\_b](file:///a_b)").as_deref(),
            Some("see [a_b](file:///a_b)")
        );
    }

    #[test]
    fn a_detached_session_leaves_the_thread_by_its_binding() {
        let home = tempfile::tempdir().unwrap();
        let mut writer = Writer::new(crate::statev2::open(home.path()).unwrap());
        let mut attached = crate::shared_store::empty_projection(&thread("A"));
        attached["providerSessions"] = json!([{"id": "provider-session:provider-instance:codex:shared",
            "providerInstanceId": "codex", "driver": "codex", "status": "ready", "updatedAt": "x"}]);
        writer
            .write(&commit(
                vec![ThreadWrite {
                    thread_id: "thread:w".into(),
                    before: None,
                    after: attached.clone(),
                    deleted: false,
                }],
                vec![],
            ))
            .unwrap();
        let detached = crate::shared_store::empty_projection(&thread("A"));
        writer
            .write(&commit(
                vec![ThreadWrite {
                    thread_id: "thread:w".into(),
                    before: Some(attached),
                    after: detached,
                    deleted: false,
                }],
                vec![],
            ))
            .unwrap();
        let count = |sql: &str| -> i64 { writer.conn.query_row(sql, [], |r| r.get(0)).unwrap() };
        assert_eq!(
            count("SELECT COUNT(*) FROM orchestration_v2_projection_provider_sessions"),
            1
        );
        assert_eq!(
            count("SELECT COUNT(*) FROM orchestration_v2_projection_provider_session_bindings"),
            0
        );
    }

    #[test]
    fn a_deleted_thread_keeps_its_records_marked_deleted() {
        let home = tempfile::tempdir().unwrap();
        let mut writer = Writer::new(crate::statev2::open(home.path()).unwrap());
        let projection = crate::shared_store::empty_projection(&thread("A"));
        writer
            .write(&commit(
                vec![ThreadWrite {
                    thread_id: "thread:w".into(),
                    before: None,
                    after: projection.clone(),
                    deleted: false,
                }],
                vec![],
            ))
            .unwrap();
        let mut gone = projection.clone();
        gone["thread"]["deletedAt"] = json!("2026-01-02T00:00:00.000Z");
        writer
            .write(&commit(
                vec![ThreadWrite {
                    thread_id: "thread:w".into(),
                    before: Some(projection),
                    after: gone,
                    deleted: true,
                }],
                vec![],
            ))
            .unwrap();
        assert_eq!(
            crate::statev2::read::deleted_thread_ids(&writer.conn).unwrap(),
            vec!["thread:w".to_string()]
        );
    }
}
