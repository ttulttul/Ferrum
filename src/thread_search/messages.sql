WITH eligible AS MATERIALIZED (
    SELECT t.thread_id, t.project_id
    FROM orchestration_v2_projection_threads t
    JOIN projection_projects p USING (project_id)
    WHERE t.deleted_at IS NULL AND t.archived_at IS NULL AND p.deleted_at IS NULL
      AND json_extract(t.payload_json, '$.lineage.relationshipToParent') IS NOT 'subagent'
      AND json_extract(t.payload_json, '$.lineage.sideChat') IS NOT 1
      AND (:project IS NULL OR t.project_id = :project)
      AND t.thread_id NOT IN (SELECT value FROM json_each(:excluded))
)
SELECT messages.message_id, messages.thread_id, eligible.project_id, messages.role,
       {text}, messages.created_at
FROM {source}
JOIN eligible ON eligible.thread_id = messages.thread_id
WHERE {candidates}
  AND CASE WHEN substr(json_extract(messages.payload_json,'$.text'),1,11) = 'Ultrathink:' COLLATE NOCASE
           THEN ltrim(substr(json_extract(messages.payload_json,'$.text'),12), :whitespace)
           ELSE json_extract(messages.payload_json,'$.text') END COLLATE BINARY <> :placeholder
  AND messages.message_id NOT LIKE 'async-answer:%'
  AND NOT EXISTS (
    SELECT 1 FROM orchestration_v2_projection_runs r
    WHERE r.run_id = messages.run_id AND (r.status = 'rolled_back' OR
        (r.status = 'cancelled' AND NOT EXISTS (
            SELECT 1 FROM orchestration_v2_projection_turn_items i WHERE i.run_id = r.run_id
              AND json_extract(i.payload_json,'$.messageId') = messages.message_id
              AND json_extract(i.payload_json,'$.inputIntent') IS NOT 'queued_turn'
        )))
  )
  AND NOT EXISTS (
    SELECT 1 FROM (SELECT {folded} AS text) AS message, json_each(:substrings) s
    WHERE instr(lower(message.text), json_extract(s.value,'$.token')) = 0
      -- Only these UTF-8 lead bytes can contain a Latin accent in the palette fold.
      AND NOT ((instr(CAST(message.text AS BLOB), x'c3') > 0 OR
           instr(CAST(message.text AS BLOB), x'c4') > 0 OR
           instr(CAST(message.text AS BLOB), x'c5') > 0 OR
           instr(CAST(message.text AS BLOB), x'c6') > 0 OR
           instr(CAST(message.text AS BLOB), x'c7') > 0 OR
           instr(CAST(message.text AS BLOB), x'c8') > 0 OR
           instr(CAST(message.text AS BLOB), x'e1') > 0)
        AND message.text GLOB json_extract(s.value,'$.glob'))
  )
  AND NOT EXISTS (
    SELECT 1 FROM json_each(:spellings) token
    WHERE NOT EXISTS (SELECT 1 FROM json_each(token.value) spelling WHERE instr({folded}, spelling.value) > 0)
  )
ORDER BY {rowid} DESC LIMIT 2000
