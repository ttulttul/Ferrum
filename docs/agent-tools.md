# Agent tools

The pinned reference serves its agent tools over `/mcp` to three kinds of caller: a provider
CLI acting as its thread (the thread-scoped credential from PR #102), a plain client signed in
through OAuth, and a linked environment calling through a peer link. This page is the checklist
for serving the same tools from Rust, with the same names, schemas, descriptions, caller checks,
errors and side effects.

Providers running in agentd receive the same thread scope through the embedding host's
[workspace MCP endpoint](embedding.md#settings-and-provider-turns).

Device discovery, boot, and screenshots depend on the host's iOS Simulator or Android Emulator.
The parity suite exercises the disabled and unavailable-host responses. It does not boot a real
simulator or emulator in CI.

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

Rust lists and refuses tools the same way (`listed` and `callable` in `src/mcp_oauth/tools.rs`):
a client from outside a thread is neither offered a thread-only tool nor may call one ("Tool
'...' not found"), and a deferring session lists the direct tools Rust serves plus what its
`tool_search` loaded. `tool_search` searches the tools Rust serves, not all 82, and a search that
loads tools streams `notifications/tools/list_changed` before its answer, as the reference does.

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
3b. **Themes** (served): `t3_theme_read`, `t3_theme_set`, on a port of the reference's
   `EnvironmentThemeService` (`src/environment_theme.rs`): theme files read through the guarded
   loader within its file count and size limits, background image revisions, publishing through
   a staging file with the previous file put back when the theme is not served or cannot be
   selected, and selection through the `defaultTheme`/`defaultAppearance` settings. The built-in
   and reserved ids, the CSS limit and the restyling guide are generated from the reference into
   `src/mcp_oauth/themes.json` by `scripts/update_agent_tools.py`. `server.subscribeConfig` now
   sends the published themes, and again when the themes directory changes. Parity: the same
   two suites.
4. **Calling session** (served): listing by caller, `tool_search` and the deferred listing,
   `send_to_user`, `t3_background_wait`. Rust's Claude sessions report no background work, so
   `t3_background_wait` matches no task id; Codex cannot mark work as awaited, as in the
   reference. Parity: the scenario `codex thread tools` (including a deferring session) and
   `scripts/mcp_parity.py` (thread-only tools refused to a client).
4b. **Delegation**, in parts. Served: `delegate_task` (both modes) and `task_status`, on a port
   of the reference orchestrator's delegated tasks (`src/orchestration/delegation.rs`):
   - `delegated_task.request` writes the child thread (`makeSubagentChildThread`), the task, its
     node and timeline card on the parent, starts the child's run, and records the
     `subagent_spawn` context transfer, with the reference's derived ids;
   - when the child's run ends, the task is settled from it (`finalizeAppOwnedSubagentTask`):
     status, result, node, card and a `subagent_result` transfer, with the reference's source
     points;
   - completion delivery (`planDelegatedCompletionDelivery`): a parent that is idle, or a task
     with `completionWake: "always"`, gets a delivery run whose message is the reference's
     notification with the results inline (`delegatedCompletionWakeDetail`); `task_status`
     acknowledges it;
   - a blocking wait reads the task when the child's run ends, before it is settled, and
     answers after, as the reference's `waitForTask` does; a wait that ends without a result
     upgrades the task's wake.

   Parity: the scenarios `claude delegate` (a wait) and `claude delegate async` (a delivery run
   on an idle parent) in `scripts/client_thread_parity.py`, with fake CLIs for the parent and
   the child. Still pending, each its own PR: `task_cancel` and cancel cascades,
   `create_threads`, follow-ups to a finished child (`recordSubagentFollowup`), restart recovery
   of delegated tasks, delivery into a parent's running turn (the reference steers it), worktree
   isolation (`isolation: "worktree"` is refused) and tasks in a linked environment (refused).
   `orchestrator_capabilities` still says `appOwnedSubagents` is false until `task_cancel` is
   served.
4c. **Attachments and secrets** (pending): `t3_attachment_prepare_upload`,
   `t3_attachment_discard`, `t3_thread_send_attachments`, `request_secret`.
5. **Prompt context and capabilities**, as above.
6. **Scheduled tasks**: `schedule_task`, `list_scheduled_tasks`, `delete_scheduled_task` and
   `run_scheduled_task_now` share the reference's `scheduled_tasks` table. Thread callers
   default to their project; an outside client can list all projects. Webhook summaries include
   their signing status. Due-run dispatch remains pending.
7. **Pull requests and issues**: the `pullRequests` and `issues` toolkits.
8. **Worktrees, previews, devices and HTML**: `t3_worktree_status`, `t3_worktree_handoff`, the
   `previewControls`, `preview`, `device` and `html` toolkits. These drive the desktop's browser
   and device panels; what Rust lacks for them is listed here when that batch starts.

## Known gaps

Gaps the thread tools surface that live in shared code, not in the tools:

- **Live provider sessions on a thread change** (fixed). As the reference does, Rust now
  detaches the provider session (`provider-session.detached`) when a thread is archived or
  settled, its worktree changes, its runtime mode changes on a driver that cannot switch mode in
  its session (Claude), or its provider instance changes. A detach mid-turn stops the turn:
  - a Codex turn is interrupted in the shared app-server, and ends with a "Stopped by the
    provider" result and no user interrupt request;
  - a Claude turn's process is stopped, and the run fails as its stream closes.

  An instance change while a run is active is refused with the reference's busy error. A model
  change on the same instance, or a Codex runtime-mode change, lets the turn run on. The
  `* mid-turn` scenarios in `scripts/client_thread_parity.py` send these changes over the
  WebSocket while a turn runs.
- **Title regeneration** (fixed). As the reference does, a thread's first user message (seeded
  by the client, or the first after only `/compact` or `/logout`) and a `regenerate_title`
  request record `titleRegeneration` and a `thread-title.generate` outbox effect, and Rust runs
  it (`src/title_generation.rs`): the reference's prompt and thread context, the text
  generation model with the reference's fallback when its instance cannot run, `codex exec` or
  `claude -p` with the title schema, then `thread.title.regeneration.complete`. Either server
  takes over effects the other left pending. Not ported: titles on Cursor, Grok, OpenCode,
  Antigravity, Pi and Muse, custom text generators, the source control lookup of linked PRs and
  issues, and the source thread's history in a fork's title. The `codex title`,
  `claude title fallback` and `codex title failed` scenarios in
  `scripts/client_thread_parity.py` compare each CLI's arguments, prompt and schema, and the
  thread's title afterwards.
- **Steering.** A `message.dispatch` with `steer_active` steers a running Codex turn as the
  reference does (`src/orchestration/steer.rs`): the message joins the run as a `steer`
  user message and Codex gets `turn/steer`; a run whose turn ended starts a new turn, and one
  whose turn has not started queues it. Not ported: steering a Claude turn (the message
  queues), a steer that changes the model selection or carries attachments (the reference
  restarts the turn), and the follow-up turn the reference starts when the provider refuses
  a steer. `t3_thread_send` still refuses `mode: "steer"`. Parity: `codex steer then stop`
  in `scripts/client_thread_parity.py`.
- **One Codex app-server per turn.** The reference runs every Codex thread of an instance in
  one shared app-server, so a delegated child's turn comes into the parent's process; Rust starts
  one per turn. Delegation works on Codex, but its parity is compared on Claude only.
- **One Claude CLI per turn.** Rust starts a Claude CLI for each turn and resumes its session
  (`--resume`); the reference sends later turns, such as a delegated task's delivery, to the CLI
  it already runs. The `claude delegate async` scenario lists the launch and sign-in differences
  as expected failures.
- **Run start.** Rust records a run as running from its start; the reference's run is
  `starting` until its provider turn begins. `task_status` reads a run without a provider
  attempt as `starting`, as the reference reports it.
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
<!-- Generated by scripts/update_agent_tools.py: 59 of 82 served. -->

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
| [x] | `comment_on_issue` | issues | every caller | `writesWithCapability` |
| [x] | `create_issue` | issues | every caller | `writesWithCapability` |
| [x] | `link_issue` | issues | every caller | `writesThreads` |
| [x] | `list_issues` | issues | every caller | `reads` |
| [x] | `list_thread_issues` | issues | every caller | `reads` |
| [x] | `read_issue` | issues | every caller | `reads` |
| [x] | `set_issue_state` | issues | every caller | `writesWithCapability` |
| [x] | `unlink_issue` | issues | every caller | `writesThreads` |
| [x] | `send_to_user` | message | threads only | `actsAsCaller` |
| [ ] | `create_threads` | orchestrator | threads only | `actsAsCaller` |
| [x] | `delegate_task` | orchestrator | threads only | `actsAsCaller` |
| [x] | `delete_scheduled_task` | orchestrator | every caller | `writes` |
| [x] | `list_scheduled_tasks` | orchestrator | every caller | `reads` |
| [x] | `orchestrator_capabilities` | orchestrator | every caller | `reads` |
| [ ] | `request_secret` | orchestrator | threads only | `actsAsCaller` |
| [x] | `schedule_task` | orchestrator | every caller | `either(startsThreads, writes)` |
| [x] | `t3_background_wait` | orchestrator | threads only | `actsAsCaller` |
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
| [x] | `task_cancel` | orchestrator | threads only | `actsAsCaller` |
| [x] | `task_status` | orchestrator | threads only | `actsAsCaller` |
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
| [x] | `t3_preview_close` | previewControls | threads only | `actsAsCaller` |
| [x] | `t3_preview_list` | previewControls | threads only | `readsAsCaller` |
| [x] | `t3_project_clone` | project | every caller | `writesEnvironment` |
| [x] | `t3_project_create` | project | every caller | `writesEnvironment` |
| [x] | `t3_project_delete` | project | every caller | `writesEnvironment` |
| [x] | `t3_project_list` | project | every caller | `reads` |
| [x] | `t3_project_read` | project | every caller | `reads` |
| [x] | `t3_project_update` | project | every caller | `writesEnvironment` |
| [x] | `t3_thread_launch` | project | every caller | `startsThreads` |
| [x] | `link_pull_request` | pullRequests | every caller | `writesThreads` |
| [x] | `link_pull_requests` | pullRequests | every caller | `writesThreads` |
| [x] | `list_thread_pull_requests` | pullRequests | every caller | `reads` |
| [x] | `read_pull_request_guide` | pullRequests | every caller | `reads` |
| [x] | `unlink_pull_request` | pullRequests | every caller | `writesThreads` |
| [x] | `unlink_pull_requests` | pullRequests | every caller | `writesThreads` |
| [x] | `unwatch_pull_request` | pullRequests | not subagents | `writesThreads` |
| [x] | `watch_pull_request` | pullRequests | not subagents | `writesThreads` |
| [x] | `write_pull_request_guides` | pullRequests | every caller | `writesWithCapability` |
| [x] | `t3_theme_read` | theme | every caller | `reads` |
| [x] | `t3_theme_set` | theme | every caller | `writesEnvironment` |
| [x] | `run_scheduled_task_now` | thread | every caller | `writesEnvironment` |
| [x] | `t3_pending_request` | thread | every caller | `either(reads, writesThreads)` |
| [x] | `t3_queue` | thread | every caller | `either(reads, writesThreads)` |
| [x] | `t3_thread_configure` | thread | every caller | `either(reads, writesThreads)` |
| [x] | `t3_thread_organize` | thread | every caller | `writesThreads` |
| [x] | `t3_thread_search` | thread | every caller | `reads` |
| [x] | `t3_thread_transfer` | thread | every caller | `either(reads, writesThreads)` |
| [x] | `t3_worktree_handoff` | worktree | threads only | `actsAsCaller` |
| [x] | `t3_worktree_list` | worktree | every caller | `reads` |
| [x] | `t3_worktree_status` | worktree | threads only | `readsAsCaller` |
<!-- tools:end -->
