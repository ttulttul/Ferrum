# Agent tools

The pinned reference serves its agent tools over `/mcp` to three kinds of caller: a provider
CLI acting as its thread (the thread-scoped credential from PR #102), a plain client signed in
through OAuth, and a linked environment calling through a peer link. This page is the checklist
for serving the same tools from Rust, with the same names, schemas, descriptions, caller checks,
errors and side effects.

## Where the list comes from

`scripts/reference_agent_tools.mjs <reference>` builds the reference's tool registrations the way
its `AgentContextBudget` test does (`McpHttpServer.layerTools` with mocked handler services) and
prints each tool as `tools/list` sends it, with the callers it is listed to.
`scripts/update_agent_tools.py <reference>` writes that to `src/mcp_oauth/tools.json` and
regenerates the checklist below; parity CI runs it with `--check`. Rust lists the tools named in
`SERVED` in `src/mcp_oauth/tools.rs`, taking their definitions from `tools.json` unchanged, so a
served tool's name, description, input and output schemas and annotations are the reference's by
construction. Serving a tool means adding it to `SERVED`, implementing it, and rerunning the script.
`tools.json` keeps every tool's output schema, which decodes answers from linked environments;
`tools/list` leaves out one whose root is not a plain object, as the reference's MCP protocol
layer does (effect's `isToolOutputSchema`).

## Who sees which tools

Every caller lists every tool except `tool_search`, unless the tool's `McpSchema.EnabledWhen`
says otherwise ("Listed to" below):

- **threads only** (`hiddenFromOutsideClients`): tools that act as the calling thread. Plain
  clients and linked callers do not list them, and `McpToolAccess` refuses their calls anyway.
- **not subagents** (`hiddenFromSubagents`): hidden from a thread whose provider session is a
  subagent's.
- **links beta on** (`hiddenWhileLinksOff`): hidden while Settings → Labs → Linked environments is
  off. So a thread lists 79 tools with the beta off and 82 with it on.
- `tool_search` is listed only to a provider session that sends the deferred tool exposure
  header; such a session lists `orchestrator_capabilities`, `delegate_task`, `task_status`,
  `tool_search` and whatever its searches loaded (`McpToolExposure.ts`). Calls are never gated
  by listing.

Rust does not yet filter its listing by caller: all eleven tools it serves are listed to every
caller. The first batch with a "threads only" tool must add the filtering, and `tool_search`
with the deferred listing comes with the delegation batch.

## Caller checks

The "Caller check" column is the tool's declaration in the reference's
`apps/server/src/mcp/McpToolAccess.ts`:

- `reads`: anyone with a token; a linked environment's call is answered there.
- `readsAsCaller`, `actsAsCaller`: a thread caller only; anyone else gets
  `thread_credential_required` ("This tool acts as the calling T3 thread, ..."). `actsAsCaller`
  also refuses read-only clients and a thread that is gone.
- `writes`: refuses read-only clients, a dead caller, and linked callers (unless the tool allows
  them, as uploads do).
- `writesWithCapability`: as `writes`, with the handler's capability checks.
- `writesThreads`: for each named thread other than the caller, its modes may not exceed the
  caller's and it must belong to the same link; a mode raised during the call is reported as
  `runtime_mode_escalation_denied` / `interaction_mode_escalation_denied`.
- `startsThreads`: new threads take modes no broader than the caller's; linked callers' threads
  carry the link's origin.
- `writesEnvironment`: needs a full-access/default calling thread or a full-access client,
  rechecked after waits; refused to linked callers unless allowed (adding a project is).
- `either(a, b)`: one tool, two rules picked per call (status/list/read versus act; create versus
  update).
- `afterForwarding(...)`: records what a linked environment answered here.

## Prompt context and capabilities

Not yet in Rust, and each needs its own PR:

- **Orchestration instructions.** The reference gives every provider CLI the text in
  `packages/provider-core/src/server/orchestrationInstructions.ts`: Codex gets
  `T3_CODE_ORCHESTRATION_SECTIONS` as `additionalContext` entries `t3_code_orchestration_01..03`
  plus `t3_code_runtime` and, with browser or device tools, `t3_code_tools`
  (`apps/server/src/provider/CodexDeveloperInstructions.ts`); Claude Code gets
  `T3_CODE_ORCHESTRATION_INSTRUCTIONS` appended to its system prompt (`ClaudeAdapterV2.ts`), with
  the ToolSearch variant of the device text. Cursor, OpenCode and Pi have their own variants.
  Rust sends none of it, and `cli_launch` in `scripts/client_thread_parity.py` leaves it out of
  the comparison. It goes in after delegation is served, since it tells the model to use
  `delegate_task`, `task_status` and `task_cancel`.
- **`orchestrator_capabilities`.** Rust's `features` say `appOwnedSubagents`,
  `batchThreadCreation` and `scheduledTasks` are false and `maxBatchThreads` is 0; the reference
  always says true, true, true and 20. Each flips when its tools are served. The `providers`
  list is compared separately from the caller's fields in `comparable_mcp`; making it the
  reference's (model listing, pinned models, `omittedModels`, constraints) is its own item.

## Batches

Each batch is one PR from the latest main, with parity: the fake CLI's `mcp` steps call every
tool in the batch through `/mcp` against both servers, as thread, plain client and (where they
differ) linked caller, and the harness compares the answers decoded with the reference's output
schemas.

1. **Inventory** (this page, `tools.json`, the update script and its CI check).
2. **Threads** (served): `t3_thread_update`, `t3_thread_organize`, `t3_thread_configure`,
   `t3_thread_transfer`, `t3_queue`, `t3_pending_request`, `t3_workflow_stop`. Parity:
   scenario `codex thread tools` in `scripts/client_thread_parity.py` (the thread calls them
   on itself, including argument errors) and the read-only client calls in
   `scripts/mcp_parity.py`. Pending, see [Known gaps](#known-gaps).
2b. **Thread search** (served): `t3_thread_search`. It reuses the palette field scorer and
   message search ported for `orchestration.searchThreads` (`src/thread_search`), with the
   agent's options: one project, no palette picks, and message matches only in threads the
   title matches did not list, keeping a fifth of the slots. Parity: the same two suites.
3. **Projects, environment and worktrees** (served): `t3_project_update`, `t3_project_delete`,
   `t3_project_clone`, `t3_environment_preferences_update`, `t3_worktree_list`. Project changes
   go through `projects.mutate`, which now says why it refused one (not found, workspace
   already registered, not empty, or a rule such as the monogram and script id limits).
   `t3_project_clone` answers as the reference does when no source control provider or remote
   can clone; Rust's `sourceControl.cloneRepository` clones nothing yet. Parity: the scenario
   `codex thread tools` and `scripts/mcp_parity.py`. (`t3_environment_link`,
   `t3_environment_links` and `t3_environment_unlink` came with the environment link work,
   listed while Linked environments is on.)
3b. **Themes**: `t3_theme_read`, `t3_theme_set`. They need the reference's
   `EnvironmentThemeService` (published themes in the themes directory, the CSS guard, the
   active theme setting), which Rust does not have yet, so they get their own PR.
4. **Delegation**: listing by caller, `tool_search` and the deferred listing, `delegate_task`,
   `task_status`, `task_cancel`, `t3_background_wait`, `create_threads`, the attachment tools,
   `request_secret`, `send_to_user`.
5. **Prompt context and capabilities**, as above.
6. **Scheduled tasks**: `schedule_task`, `list_scheduled_tasks`, `delete_scheduled_task`,
   `run_scheduled_task_now`.
7. **Pull requests and issues**: the `pullRequests` and `issues` toolkits.
8. **Worktrees, previews, devices and HTML**: `t3_worktree_status`, `t3_worktree_handoff`, the
   `previewControls`, `preview`, `device` and `html` toolkits. These drive the desktop's browser
   and device panels; what Rust lacks for them is listed here when that batch starts.

## Known gaps

Gaps the thread tools surface that live in shared code, not in the tools:

- **Live provider sessions on a thread change.** When a thread's model selection, runtime mode
  or workspace changes, or it is archived or settled, while a provider session is live, the
  reference detaches the session (`provider-session.detached`) and interrupts the running turn.
  Rust records the change and lets the turn run on. So `t3_thread_configure` with
  `modelSelection` and `t3_thread_organize` `archive` answer as the reference does but differ
  in what follows, and the parity scenario leaves them out mid-turn.
- **Title regeneration.** `t3_thread_update` `regenerate_title` records the request as the
  reference does, but Rust generates no title, so the request stays pending.
- **Claude workflows.** Rust's Claude sessions run no workflow tasks, so `t3_workflow_stop`
  can only answer "not running" or "no live provider task".
- **Coverage.** The thread scenario works on the caller's own thread: a fork needs a stable
  run, which a thread's first turn does not have yet. The mode-limit and link-fence checks on
  another thread (`writesThreads`) are implemented but have no parity case yet.

Argument checking with the reference's wording (`Invalid parameters for tool ...`) applies to
the tools served from this batch on (`CHECKED` in `src/mcp_oauth/tools.rs`); the first eleven
still answer "Invalid tool arguments".

## Checklist

<!-- tools:start -->
<!-- Generated by scripts/update_agent_tools.py: 27 of 82 served. -->

| | Tool | Toolkit | Listed to | Caller check |
|---|---|---|---|---|
| [ ] | `t3_attachment_discard` | attachment | every caller | `writes` |
| [ ] | `t3_attachment_prepare_upload` | attachment | every caller | `writes` |
| [ ] | `t3_thread_send_attachments` | attachment | every caller | `writesThreads` |
| [ ] | `device_close` | device | threads only | `actsAsCaller` |
| [ ] | `device_list` | device | threads only | `readsAsCaller` |
| [ ] | `device_open` | device | threads only | `actsAsCaller` |
| [ ] | `device_screenshot` | device | threads only | `readsAsCaller` |
| [x] | `t3_environment_preferences_update` | environment | every caller | `writesEnvironment` |
| [x] | `t3_environment_read` | environment | every caller | `reads` |
| [ ] | `html_preview` | html | threads only | `readsAsCaller` |
| [ ] | `html_render` | html | threads only | `actsAsCaller` |
| [ ] | `comment_on_issue` | issues | every caller | `writesWithCapability` |
| [ ] | `create_issue` | issues | every caller | `writesWithCapability` |
| [ ] | `link_issue` | issues | every caller | `writesThreads` |
| [ ] | `list_issues` | issues | every caller | `reads` |
| [ ] | `list_thread_issues` | issues | every caller | `reads` |
| [ ] | `read_issue` | issues | every caller | `reads` |
| [ ] | `set_issue_state` | issues | every caller | `writesWithCapability` |
| [ ] | `unlink_issue` | issues | every caller | `writesThreads` |
| [ ] | `send_to_user` | message | threads only | `actsAsCaller` |
| [ ] | `create_threads` | orchestrator | threads only | `actsAsCaller` |
| [ ] | `delegate_task` | orchestrator | threads only | `actsAsCaller` |
| [ ] | `delete_scheduled_task` | orchestrator | every caller | `writes` |
| [ ] | `list_scheduled_tasks` | orchestrator | every caller | `reads` |
| [x] | `orchestrator_capabilities` | orchestrator | every caller | `reads` |
| [ ] | `request_secret` | orchestrator | threads only | `actsAsCaller` |
| [ ] | `schedule_task` | orchestrator | every caller | `either(startsThreads, writes)` |
| [ ] | `t3_background_wait` | orchestrator | threads only | `actsAsCaller` |
| [x] | `t3_environment_link` | orchestrator | links beta on | `writesEnvironment` |
| [x] | `t3_environment_links` | orchestrator | links beta on | `reads` |
| [x] | `t3_environment_unlink` | orchestrator | links beta on | `writesEnvironment` |
| [x] | `t3_thread_interrupt` | orchestrator | every caller | `writesThreads` |
| [x] | `t3_thread_list` | orchestrator | every caller | `reads` |
| [x] | `t3_thread_read` | orchestrator | every caller | `reads` |
| [x] | `t3_thread_send` | orchestrator | every caller | `afterForwarding(writesThreads)` |
| [x] | `t3_thread_update` | orchestrator | every caller | `writesThreads` |
| [x] | `t3_thread_wait` | orchestrator | every caller | `reads` |
| [x] | `t3_workflow_stop` | orchestrator | every caller | `writesThreads` |
| [ ] | `task_cancel` | orchestrator | threads only | `actsAsCaller` |
| [ ] | `task_status` | orchestrator | threads only | `actsAsCaller` |
| [ ] | `preview_act` | preview | threads only | `actsAsCaller` |
| [ ] | `preview_dialog` | preview | threads only | `actsAsCaller` |
| [ ] | `preview_evaluate` | preview | threads only | `actsAsCaller` |
| [ ] | `preview_navigate` | preview | threads only | `actsAsCaller` |
| [ ] | `preview_open` | preview | threads only | `actsAsCaller` |
| [ ] | `preview_recording_start` | preview | threads only | `actsAsCaller` |
| [ ] | `preview_recording_stop` | preview | threads only | `actsAsCaller` |
| [ ] | `preview_resize` | preview | threads only | `actsAsCaller` |
| [ ] | `preview_set_appearance` | preview | threads only | `actsAsCaller` |
| [ ] | `preview_snapshot` | preview | threads only | `readsAsCaller` |
| [ ] | `preview_status` | preview | threads only | `readsAsCaller` |
| [ ] | `preview_wait_for` | preview | threads only | `readsAsCaller` |
| [ ] | `t3_preview_close` | previewControls | threads only | `actsAsCaller` |
| [ ] | `t3_preview_list` | previewControls | threads only | `readsAsCaller` |
| [x] | `t3_project_clone` | project | every caller | `writesEnvironment` |
| [x] | `t3_project_create` | project | every caller | `writesEnvironment` |
| [x] | `t3_project_delete` | project | every caller | `writesEnvironment` |
| [x] | `t3_project_list` | project | every caller | `reads` |
| [x] | `t3_project_read` | project | every caller | `reads` |
| [x] | `t3_project_update` | project | every caller | `writesEnvironment` |
| [x] | `t3_thread_launch` | project | every caller | `startsThreads` |
| [ ] | `link_pull_request` | pullRequests | every caller | `writesThreads` |
| [ ] | `link_pull_requests` | pullRequests | every caller | `writesThreads` |
| [ ] | `list_thread_pull_requests` | pullRequests | every caller | `reads` |
| [ ] | `read_pull_request_guide` | pullRequests | every caller | `reads` |
| [ ] | `unlink_pull_request` | pullRequests | every caller | `writesThreads` |
| [ ] | `unlink_pull_requests` | pullRequests | every caller | `writesThreads` |
| [ ] | `unwatch_pull_request` | pullRequests | not subagents | `writesThreads` |
| [ ] | `watch_pull_request` | pullRequests | not subagents | `writesThreads` |
| [ ] | `write_pull_request_guides` | pullRequests | every caller | `writesWithCapability` |
| [ ] | `t3_theme_read` | theme | every caller | `reads` |
| [ ] | `t3_theme_set` | theme | every caller | `writesEnvironment` |
| [ ] | `run_scheduled_task_now` | thread | every caller | `writesEnvironment` |
| [x] | `t3_pending_request` | thread | every caller | `either(reads, writesThreads)` |
| [x] | `t3_queue` | thread | every caller | `either(reads, writesThreads)` |
| [x] | `t3_thread_configure` | thread | every caller | `either(reads, writesThreads)` |
| [x] | `t3_thread_organize` | thread | every caller | `writesThreads` |
| [x] | `t3_thread_search` | thread | every caller | `reads` |
| [x] | `t3_thread_transfer` | thread | every caller | `either(reads, writesThreads)` |
| [ ] | `t3_worktree_handoff` | worktree | threads only | `actsAsCaller` |
| [x] | `t3_worktree_list` | worktree | every caller | `reads` |
| [ ] | `t3_worktree_status` | worktree | threads only | `readsAsCaller` |
<!-- tools:end -->
