# One orchestration database for both servers

Design note, November 2026.

**Goal:** the Rust server and the reference (`t3 serve`, the pinned upstream commit in
`.github/workflows/parity.yml`) can be swapped on one T3 home with no loss:
- threads created on either server are visible on the other, and can be continued there;
- restart recovery works across a switch;
- the effect outbox, queued runs and pending requests survive the switch.

**Today:**
- Rust keeps its own event log (`userdata/events.jsonl`) and rebuilds its store from it at
  start-up.
- The reference keeps everything in `userdata/statev2.sqlite`.
- So each server sees only its own threads.
- In live trials:
  - the desktop's thread subscriptions failed on Rust with
    `OrchestrationV2GetThreadProjectionError`, because Rust had no projection for the
    reference's threads;
  - Rust could not continue a turn that the reference had interrupted.

File references below are to the pinned reference's `apps/server/src`, unless they start
with `src/` or `scripts/` (this repository).

## 1. What the reference does

### 1.1 Opening the database

The code is in `persistence/Sqlite.ts` and `persistence/initializeV2Database.ts`.

- **File:** `<home>/userdata/statev2.sqlite`.
- **Importing V1:** this happens only when `statev2.sqlite` is missing and the V1
  `state.sqlite` exists. The reference then:
  1. makes a `VACUUM INTO` copy of V1 inside `.v2-import-<pid>-XXXXXX/`;
  2. hard-links that copy into place;
  3. replays the V1 threads into V2 lazily (`legacy/LegacyV1ThreadImporter.ts`).
- **Pragmas,** in this order:
  - `busy_timeout = 5000`
  - `foreign_keys = ON`
  - `auto_vacuum = INCREMENTAL`, only when the database has no tables yet
  - `journal_mode = WAL`
  - `synchronous = NORMAL`
  - `checkpoint_fullfsync = ON`
  - `temp_store = MEMORY`
  - `journal_size_limit = 134217728`
- **After migrating:** if `sqlite_stat1` exists, the reference empties it and runs
  `ANALYZE sqlite_schema`.
- **Write transactions** are `BEGIN IMMEDIATE`, with one writer lane per process.
- **Migrations** use the effect `Migrator`, with its ledger in `effect_sql_migrations`
  (`migration_id`, `name`, `created_at`):
  - It runs only the ids above the highest recorded one, each in its own transaction.
  - It refuses a ledger whose V2 ids (55 and up) carry other names.
  - It warns about ids it does not know.
  - The pinned build's head is **98** (`ActiveShellIndexIndexes`).
  - A fresh database also gets one seeded row:
    `orchestration_v2_projection_metadata('thread-projections', 1, 0, now)`.
- **Exclusive owner:** `server-owner.sqlite`, a `BEGIN EXCLUSIVE` held for the life of
  the process, keeps two servers off one home.
  - Rust already takes the same lock (`src/server_runtime.rs`).
  - The CLI migrates only while it holds that lock.

### 1.2 Events, receipts and projections

**Events.** Live V2 events go into the V1 table `orchestration_events`.
- Their columns: `application_event_version = 2`, `aggregate_kind = 'thread'`,
  `stream_id = threadId`, and `stream_version` = the next number in that thread's stream.
- `payload_json` holds only the event's payload. `metadata_json` holds
  `{runId?, nodeId?, driver?, providerInstanceId?, rawEventId?}`.
- `actor_kind` is `provider` when a raw provider event caused the event, and `server`
  otherwise (`persistence/OrchestrationEventStore.ts`).
- Project events go into the same table with `aggregate_kind = 'project'`
  (`project.created`, `project.meta-updated`, `project.deleted`).
- The tables `orchestration_v2_events` and `orchestration_v2_command_receipts` are
  left over from a preview. Their rows were copied into the V1 tables, and nothing reads
  or writes them anymore.

**Receipts** go in `orchestration_command_receipts`, which provides idempotency.

**Payloads are full records.** Nearly every event payload is the whole record, which is
upserted:
- `thread.*` events carry `OrchestrationV2AppThread`.
- `run.created` and `run.updated` carry `Run`.
- The same holds for `turn-item.updated`, `message.updated`, `runtime-request.updated`,
  `provider-thread.updated`, and the rest.

There are three exceptions: `thread.visit-recorded`, `thread.pull-request-link-updated`
and `run.background-work-cancelled`.

**Projections are the source of truth.** The `orchestration_v2_projection_*` tables hold
each record's contract JSON in `payload_json`, plus scalar columns copied from it for
indexing. Projects are kept in V1's `projection_projects`, as scalar columns.
- The reference never rebuilds projections from events.
- Events older than a day are pruned. The newest event of each stream, every
  `thread.created`, and a few others are kept (`ProjectionMaintenance.ts`).

**One write is one transaction** (`EventSink.ts`). In order, it:
1. inserts a placeholder receipt;
2. normalizes turn-item ordinals through `orchestration_v2_turn_item_positions`, in the
   range `runOrdinal * 1e6 + n`;
3. appends the events;
4. applies each event to the projections (`ProjectionStore.apply`) and bumps the
   metadata row's `last_sequence`;
5. enqueues outbox effects;
6. finalizes the receipt with `result_sequence`.

Subscribers are published to only after the commit, through an in-process PubSub.

**Reads.**
- `getThreadProjection` and `subscribeThread` read the thread's projection rows in one
  read transaction, and decode each row strictly with its contract schema.
- A missing thread row, or a row that fails to decode, fails the whole read with
  `OrchestrationV2GetThreadProjectionError`.
- A thread's `snapshotSequence` is the `MAX(sequence)` of its stream. The shell's is the
  maximum over all V2 thread events.
- Shell rows are derived from the projection (`threadShellFromProjection`).

### 1.3 Outbox, queued runs, requests and recovery

**`orchestration_v2_effect_outbox`.**
- Rows are enqueued inside the event transaction.
- A worker (`orchestration-v2:<pid>`) claims a row with an `UPDATE … RETURNING`. That
  sets `status = 'running'`, increments `attempt_count` and sets `lease_owner`.
- It claims at most one running row per thread, in `available_at` order.
- Retries back off up to 30 s, with at most 5 attempts.
- `lease_expires_at` is never used.

**At start-up** (`EffectOutbox.reconcileAfterProcessLoss`):
- Process-bound effects are cancelled: `provider-turn.start`, `interrupt`, `steer`,
  `restart`, `runtime-request.respond`, and applying a model or title to a session.
- Replay-safe effects that were running go back to `pending`: continue, detach,
  rollback, checkpoint capture, cleanups, title generation, and stopping delegated
  tasks.

**Queued runs** are `runs` rows with status `queued`, plus `queuePosition` and
`queueHeld`. Restart recovery holds every queued run (`queueHeld: true`), so nothing
starts by itself after a restart.

**Pending requests** are `runtime_requests` rows with status `pending`. At start-up:
- A live user-input question with questions becomes answerable by message
  (`responseCapability: {type: 'message'}`).
- Every other pending request expires.

**Restart recovery** (`ProviderRuntimeRecoveryService.ts`) reads only projections and
the outbox. It selects threads with live runs, unheld queued runs, pending requests, live
sessions, or open background items. Then it:
- terminalizes the cut-off runs;
- adds a "Server restarted" notice;
- stops sessions;
- under the `continueThreadsAfterServerUpdate` setting, writes a
  `provider-runtime.continue` effect whose `processLossCount` counts lost processes in a
  row.

A run can be continued only if it has all of these:
- a running run with an `activeAttemptId`;
- an active provider thread whose `nativeThreadRef: {driver, nativeId, strength:
  'strong'}` belongs to the app thread;
- a provider session that is not stopped, bound through
  `provider_session_bindings`;
- a running `provider_turns` row for the attempt.

**Native resume:** the next turn reopens the provider with
`nativeThreadRef.nativeId`, which is a Codex thread id or a Claude session id.
`provider_session_runtime` is V1-only.

## 2. Decisions

### 2.1 Rust never migrates, and refuses what it does not support

`src/statev2.rs` opens the database at start-up, before anything else reads state.

**An existing database:**
- Rust reads its ledger on a read-only connection first.
- Rust opens the database only when the ledger is exactly the pinned one: ids 1–98 with
  the pinned names.
- Otherwise Rust stops at start-up, changes nothing, and says what to do:

| Ledger | What Rust tells the user |
|---|---|
| Older head | Start the matching reference once to upgrade the database. |
| Newer head, or unknown ids | Use the newer server. |
| Same ids, different names | Start the build that wrote the database. |

**A missing database:** Rust creates one with the reference's schema at the pinned head.
- The schema is `src/statev2/reference_schema.sql`. `scripts/update_statev2_schema.py`
  generates it from the pinned reference: it runs the reference's own migrations on an
  empty file (`scripts/reference_statev2_schema.mjs`), then writes every schema object's
  stored text, in creation order, plus the ledger.
- CI checks that the file is current (`--check`, in the "Check upstream inventory" step).
- Rust executes the file in one transaction, under the same pragmas, and seeds the
  metadata row. The resulting `sqlite_schema` is identical to the reference's, row for
  row. `scripts/shared_store_parity.py` compares the two, and checks that the reference
  then starts on the file without migrating.
- The database is built in `.statev2-create-<pid>-XXXXXX/` beside the target, then
  hard-linked into place. So a crash never leaves a partial database, and an existing
  database is never replaced.

**Only `state.sqlite`, and no `statev2.sqlite`:** Rust refuses to start. Creating
`statev2.sqlite` would stop the reference from ever importing that home's V1 threads.

**Every connection** uses the reference's pragmas and the same `sqlite_stat1` reset.
Ownership stays with `server-owner.sqlite`, as it is today.

### 2.2 The database is the shared truth; Rust keeps a private journal beside it

Rust's in-memory model (`Store`, `Thread`, `Run`, `ProviderState`) holds state that the
contract records do not carry:
- restart cut-offs, and owed resends and continuations;
- workspace stream positions;
- provider item bookkeeping.

Rebuilding all of that from projection rows alone would lose behaviour that the existing
parity suites check. The schema must also stay exactly the reference's, so Rust cannot
add tables to it. So:

- **`statev2.sqlite` holds everything both servers see:** threads, projects, runs,
  messages, items, requests, provider records, events, receipts and the outbox.
  - Every Rust commit writes its projection changes and domain events there, in one
    transaction, as `EventSink` does.
  - That write happens before anything is applied in memory.
- **`events.jsonl` stays as Rust's private journal** of its internal events. Each record
  keeps the V2 sequence it committed at. At start-up Rust replays the journal, then
  reconciles each thread with the database:
  - **The journal is current.** The database's latest event sequence for the thread is
    the one the journal recorded, so Rust keeps its richer journal state.
  - **The thread changed elsewhere, or exists only in the database.** The reference
    created or changed it while it owned the home. Rust uses the database's records.
  - **The database marks the thread deleted.** Rust deletes it too.

  The database commits before the journal appends. A crash between the two therefore
  leaves the database ahead, and the thread is read from its rows. The result is
  consistent, and only Rust-private detail is lost.
- **Snapshots of a thread read from the database serve its stored records.**
  - Fields that only the reference writes are kept, as is the millisecond precision of
    its timestamps.
  - The thread's shell row is derived from those records the way the reference derives
    it. `src/projection_shell.rs` ports `threadShellFromProjection` and the turn-item
    visibility rule.
- **When Rust changes such a thread** (pull request 2):
  - It computes its own projection before and after the event, as it already does for
    domain events.
  - It applies only the fields that changed to the stored records.
  - For a thread Rust created, the stored records equal Rust's own projection at every
    step, so its existing behaviour is unchanged.
- **Sequences:**
  - Each domain event is one `orchestration_events` row, and its AUTOINCREMENT
    `sequence` is the number subscribers see.
  - Rust computes the domain-event diff for every commit, not only when someone is
    subscribed, since the events are now stored.

Run ids differ between the servers. The reference allocates
`run:thread:<thread>:ordinal:<n>`; Rust uses UUIDs. Records keep whichever id their
server gave them:
- A run Rust reads from the database keeps its id as `Run.wire_id`. Its internal UUID is
  derived from that id (`Run::uuid_for`), so it is the same after every restart.
- Every new run Rust starts takes the reference's scheme, with the next ordinal (pull
  request 4; before that, only runs on threads read from the database did). This way
  both servers list a thread's runs and attempts in ordinal order.
- Everything that leaves Rust or arrives from a client names a run by its wire id:
  records, events, commands, and the restart continuation's ids.

**Typed state for a thread read from the database** (`Thread::from_stored`, pull
request 3). This is what lets Rust queue, start, steer, stop, answer and continue turns
on it. Rust rebuilds:
- its runs, with their statuses, messages and continuation links;
- what was sent to the thread: users' messages, plus the notifications that continue a
  run. Provider messages stay provider records, as on Rust's own threads;
- its checkpoints;
- its provider state:
  - the provider thread record, still named after the run that first used it;
  - the native thread the next turn resumes;
  - the turns that provider thread already ran, so new provider turns continue its
    numbering;
  - its session id;
  - the token usage of its last turn;
  - Claude's attachment budget.

Rust's computed records then name the same entities as the stored ones, and the overlay
applies only what Rust's turns change.

### 2.3 Existing Rust homes: a one-time import, keeping the old file

At start-up, before anything else commits, Rust lists the threads and projects its
journal holds that the database has no row for (`statev2::missing_from_database`):
- deleted threads are skipped;
- a deleted project is imported marked deleted, as the reference keeps it.

Rust then commits one `Event::Imported`. Its shared write puts each listed thread into
the database whole: every record, with `thread.created` and its record events. Each
listed project goes in with `project.created`. Logging that event in the journal records
the import's sequence against those threads and projects, so the next start-up finds the
journal current (2.2) and imports nothing again. If the one transaction fails, Rust
imports one thread or project at a time and logs the ones the database refuses. Those
stay journal-only, and start-up carries on.

`events.jsonl` is not deleted or rewritten; the import only appends its one line. An
older Rust build skips that line, as it skips any line it cannot read, so going back
still finds its log.

Every kind of home keeps working:

| Home has | What happens |
|---|---|
| No reference database, no journal | A fresh database (2.1). |
| Only a journal | The journal is imported. |
| Only `statev2.sqlite` | Its threads are read. |
| Both | The threads only the journal has are imported. |

### 2.4 Effects, queued runs and pending requests

- **Queued runs and pending requests** are projection rows, so they carry over in the
  thread's records. Rust applies the same start-up treatment as the reference: queued
  runs are held, and pending requests become message-answerable or expire.
- **The outbox at start-up** (`src/statev2/outbox.rs`). Before anything else, Rust does
  what the reference's `EffectOutbox.reconcileAfterProcessLoss` does to the previous
  process's rows:
  - process-bound effects (turn start, interrupt, steer, restart, request response,
    model or title change on a session) are cancelled;
  - replay-safe effects the previous process was running go back to `pending`.
- **Restart continuations go through the outbox**, as on the reference:
  - **When Rust's reconcile cuts a run off** that is owed a continuation, the same
    transaction enqueues `effect:restart-continuation:<run>`
    (`provider-runtime.continue`) with the reference's payload:
    - at shutdown, command `command:restart-prepare:<run>` with
      `processLossCount: 0`;
    - at start-up, `command:runtime-reconcile:startup:<thread>:<time>` with the
      processes lost in a row.

    Rows are inserted with `ON CONFLICT DO NOTHING`, so a shutdown's row wins over the
    next start-up's.
  - **Taking over a pending continuation** (`ControlState::adopt_continuations`). After
    its own start-up reconcile, Rust takes over every pending continuation for a thread
    it holds: it marks the source run owed a continuation, which its continuation pass
    then sends, and settles the row as `succeeded`, so the other server never sends it
    too.
  - **The reference's side:** its worker continues the runs Rust cut off from the rows
    Rust wrote.
- **Effect types Rust does not carry out** (title generation, cleanups, checkpoint
  capture, rollback, detach, stopping delegated tasks) stay `pending` for the reference.
  Rust never fails or deletes them.

## 3. Pull requests

1. **Schema and read path** (merged):
   - `src/statev2.rs`: open, guard, create.
   - The generated schema and its CI check.
   - At start-up, projects and threads that only the database has are read into the
     store and served from their stored records: shell, `getThreadProjection`,
     `subscribeThread`.
   - Until pull requests 2 and 3, such a thread can be viewed but not changed. A command
     on it fails clearly, rather than writing state that only Rust would see.
   - `scripts/shared_store_parity.py`, in the `core` shard. It checks:
     - the fresh schema;
     - that the reference opens Rust's database without migrating;
     - that a reference project and thread are identical on both servers;
     - that unsupported heads are refused, and the database is left unchanged.
2. **Write path** (merged):
   - Every Rust commit is written to the database first, in one `BEGIN IMMEDIATE`
     transaction (`src/shared_store.rs` plans it, `src/statev2/write.rs` writes it):
     - one `orchestration_events` row per domain event, plus `thread.created` for a new
       thread, `thread.deleted` for a deleted one, and the project events;
     - each new or changed record into its projection table, with the reference's scalar
       columns; vanished records are deleted;
     - turn-item positions, provider session bindings, `projection_projects`, the command
       receipt, and the metadata row's `last_sequence`.
   - The first commit to a thread the database has no row for writes the whole thread.
   - Event sequences are the ones Rust's subscribers see, continuing after the
     database's highest sequence.
   - A deleted thread keeps its records, with `deletedAt` set, as on the reference.
   - At start-up, a thread or project the database changed after the journal's last
     commit to it is read from the database, and a thread the database deleted is
     deleted (2.2).
   - Threads read from the database can now be renamed, archived, settled, pinned, tagged
     or deleted. Rust changes their stored records field by field (`overlay`), so fields
     only the reference writes are kept. Commands that run, steer, stop or answer turns
     are still refused on them until pull request 3.
   - Every shell row is now derived from the thread's projection, as the reference
     derives it (`src/projection_shell.rs`). Before this, Rust left the run-derived
     fields empty for its own threads.
   - `scripts/shared_store_parity.py` checks that a project and thread made on Rust,
     with a failed turn, read identically on the reference. It also checks that a rename
     Rust made to a reference thread does, and that the reference opens the database
     Rust wrote without migrating. `scripts/decode_v2_projection.mjs` decodes a
     projection with the reference's schema, to find contract mismatches. It found that
     `RunAttempt.nativeThreadId` must be absent rather than null.
3. **Recovery and outbox:**
   - **Turns on threads read from the database:** the typed rebuild and wire run ids
     (2.2). The refusal of turn commands on those threads is gone.
   - **The outbox:** start-up treatment, continuation effects written by Rust's
     reconcile, and pending continuations taken over at start-up (2.4).
   - **Fixes the cross-server runs turned up:**
     - A new provider turn continues the stored provider thread's turn numbering. Before,
       it collided with the reference's turn 1 on a unique index, and the provider events
       were lost.
     - Served messages are ordered by their turn items (`sortMessagesByTurnItemOrder`).
     - Served turn items leave out a rolled-back run's items and undelivered queued
       input, as the reference's snapshot window does.
     - A Claude session keeps its stored id. Context usage and the attachment budget
       carry over.
   - **A failed database write is logged as an error.** The commit fails as a whole,
     with nothing logged to the journal or applied.
   - **Message search text** (`search_text` in `src/statev2/write.rs`) now also turns
     an assistant quote's `t3-citation://v1/…` link into its quoted words and comment,
     as the reference's `assistantCitationsToPlainText` does, with the same validation.
     It was checked against the reference's `readableSearchText` on the same inputs.
   - **Provider turns across a switch,** in `scripts/shared_store_parity.py`:
     - `client_thread_parity.py`'s restart scenarios, run with the two halves on
       different servers, and compared with the reference's own restart;
     - a run cut off mid-turn and continued by the other server, both ways, for Codex
       and Claude;
     - queued messages and a turn whose prompt was never taken;
     - a new turn sent on the other server's thread;
     - a thread made on one server whose first turn runs on the other and calls T3's
       MCP tools (`codex_mcp`, `claude_mcp`). The tool answers must match the reference's
       own run, which shows the thread credential works for a thread the other server
       made;
     - a turn waiting on an approval.
   - **Thread MCP credentials** (#102) are in memory on both servers, so a switch carries
     nothing over. The server that runs a turn gives the CLI a fresh credential, and that
     includes a continuation it takes over from the outbox, as the reference does.
     `cli launch` compares the MCP config of every launch, with the port and token
     masked.

     Known differences are listed in `scripts/shared_store_parity_xfail.json`. Every one
     is a difference Rust already has on one server.
4. **Importing `events.jsonl`:**
   - The one-time import described in 2.3.
   - `tests/shared_store_import.rs` checks that an imported thread reaches the database
     once and that the log keeps its earlier lines.
   - `scripts/shared_store_parity.py` checks that the reference reads an imported Rust
     home's project and thread exactly as Rust does.
   - **Cross-server differences closed:**
     - A queued run has its pending attempt and root node on the thread's provider
       thread, as the reference gives a queued run when it queues it.
     - A queued message is kept as a pending `queued_turn` turn item, which snapshots
       hide until the run starts.
     - A Claude provider turn shows its model from its next usage update or its end on,
       as the reference's provider turn updates carry it.

     - Attempts are listed in their runs' order.
     - Every new run takes the reference's `run:thread:…:ordinal:…` id. The reference
       lists attempts by run id, so Rust's random UUIDs had put them in a random order
       there.

     So a message queued just before a switch from Rust to the reference, a prompt the
     provider never took, and a Claude turn cut off while it waited for an approval all
     read as on the reference. `scripts/shared_store_parity_xfail.json` is now empty, and
     `client_thread_parity_xfail.json` loses the queued-attempt entries.
