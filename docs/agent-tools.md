# Agent tools

The pinned reference serves its agent tools over `/mcp` to three kinds of caller: a provider
CLI acting as its thread (the thread-scoped credential from PR #102), a plain client signed in
through OAuth, and a linked environment calling through a peer link. This page is the checklist
for serving the same tools from Rust, with the same names, schemas, descriptions, caller checks,
errors and side effects.

Providers running in agentd receive the same thread scope through the embedding host's
[workspace MCP endpoint](embedding.md#settings-and-provider-turns).

A thread with Agent browser access can run JavaScript, change the color scheme, and resize an open
server browser tab. The Chromium parity suite exercises these tools against loopback pages
through the fake provider CLI, including control handoff and project-level access refusals.

Device discovery, boot, and screenshots depend on the host's iOS Simulator or Android Emulator.
All four device tools are served on the native server. `scripts/device_tools_parity.py` checks them
against loopback fakes without booting a real simulator or emulator: refusals when access is off, local
hub startup, emulator camera probing, AVD and `adb` fallback discovery, host label resolution, opening
a device with its `agent-device` daemon and bound launcher, screenshots, and closing or shutting down
sessions.

With a forge connected, `ci_rerun` retries failed and cancelled checks on a PR. A caller can
select a check by name. GitHub can retry a run’s failed jobs together when they match the selection
exactly. A successful retry reopens finished check watches on the calling thread. Bitbucket
reports that it cannot rerun checks. `scripts/ci_rerun_parity.py` compares the tool answers,
watch changes and requests to isolated forge fixtures.

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
  off.
- **a forge connected** (`hiddenWithoutForge`): a thread lists these tools when its own project
  uses a supported forge. The server caches the answer per thread for one minute. Outside clients
  list these tools even in an environment with no projects. The forge-write and MCP transport
  parity suites exercise both caller types.
- `tool_search` is listed only to a provider session that sends the deferred tool exposure
  header; such a session lists `orchestrator_capabilities`, `delegate_task`, `task_status`,
  `tool_search` and whatever its searches loaded (`McpToolExposure.ts`). Calls are never gated
  by listing.

Rust lists and refuses tools the same way (`listed` and `callable` in `src/mcp_oauth/tools.rs`):
a client from outside a thread is neither offered a thread-only tool nor may call one ("Tool
'...' not found"), and a deferring session lists the direct tools Rust serves plus what its
`tool_search` loaded. `tool_search` searches the served tools, and a search that
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

- **Agent instructions** (`src/agent_instructions.rs`). Rust sends the reference's text as its
  adapters do, generated into `src/mcp_oauth/instructions.json` by
  `scripts/update_agent_tools.py` (`scripts/reference_agent_instructions.mjs`):
  - Claude Code gets an `initialize` control request, once per CLI and before
    `mcp_set_servers`, whose `appendSystemPrompt` is the reference's: the runtime and pull
    request linking text (`buildRuntimeInstructions`), then, for a thread with T3's MCP server,
    the orchestration text and Claude's own background-wait and `send_to_user` text;
  - each Codex `turn/start` of such a thread carries `additionalContext`: the orchestration
    sections as `t3_code_orchestration_01..`, `t3_code_runtime` with the model and effort, and
    `t3_code_tools` with the browser text (`buildCodexAdditionalContext`);
  - a thread whose credential has the `"device"` capability also gets the device text: Codex's
    `t3_code_tools` adds `deviceTools`, Claude Code's append ends with
    `deviceToolsWithToolSearch`, and Claude's `initialize` registers the reference's
    `SubagentStart` hook (`hook_0`), whose callback answers with that text so native subagents
    get it too. `scripts/device_tools_parity.py` compares the text with devices on and off for
    both providers, and the hook and its answer.

  Each line that names a tool Rust does not serve yet is left out, with a heading left with
  nothing under it, so an agent is never pointed at a tool it cannot call; once those tools are
  served the text is the reference's, unchanged. Every tool those texts name is now served,
  so both go out as the reference sends them. `cli_launch` in
  `scripts/client_thread_parity.py` compares both servers' text, filtered the same way. Not
  sent yet: the rest of Claude's `initialize` (title, prompt suggestions, dialog kinds), and
  the Cursor, OpenCode, Pi and ACP variants. Claude's `initialize` registers the reference's
  `Stop` hook (`hook_0`, or `hook_1` after the device hook) on every query; Rust runs no Claude
  goals, so its callback answers `{}` as the reference's does without an active goal.
- **`orchestrator_capabilities`** answers as the reference's: every provider instance, with
  its constraints (`providerConstraints`; a Rust instance whose driver it cannot run children
  on says so), its models as `listCapabilityModels` lists them (pinned, favourite, preferred
  order, hidden models, `search`, `omittedModels`), their options and defaults, and its
  runtime modes; and the reference's feature flags. Its whole answer is compared.

## Batches

Each batch is one PR from the latest main, with parity: the fake CLI's `mcp` steps call every
tool in the batch through `/mcp` against both servers, as thread, plain client and (where they
differ) linked caller, and the harness compares the answers decoded with the reference's output
schemas.

### 1. Inventory

This page, `tools.json`, the update script and its CI check.

### 2. Threads

Served: `t3_thread_update`, `t3_thread_organize`, `t3_thread_configure`, `t3_thread_transfer`,
`t3_queue`, `t3_pending_request`, `t3_workflow_stop`. Parity: scenario `codex thread tools` in
`scripts/client_thread_parity.py` (the thread calls them on itself, including argument errors) and
the read-only client calls in `scripts/mcp_parity.py`. Pending, see [Known gaps](#known-gaps).

### 2b. Thread search

Served: `t3_thread_search`. It reuses the palette field scorer and message search ported for
`orchestration.searchThreads` (`src/thread_search`), with the agent's options: one project, no
palette picks, and message matches only in threads the title matches did not list, keeping a fifth
of the slots. Parity: the same two suites.

### 3. Projects, environment and worktrees

Served: `t3_project_update`, `t3_project_delete`, `t3_project_clone`,
`t3_environment_preferences_update`, `t3_worktree_list`. Project changes go through
`projects.mutate`, which now says why it refused one (not found, workspace already registered, not
empty, or a rule such as the monogram and script id limits). `t3_project_clone` answers as the
reference does when no source control provider or remote can clone; Rust's
`sourceControl.cloneRepository` clones nothing yet. Parity: the scenario `codex thread tools` and
`scripts/mcp_parity.py`. (`t3_environment_link`, `t3_environment_links` and `t3_environment_unlink`
came with the environment link work, listed while Linked environments is on.)

### 3b. Themes

Served: `t3_theme_read`, `t3_theme_set`, on a port of the reference's `EnvironmentThemeService`
(`src/environment_theme.rs`): theme files read through the guarded loader within its file count and
size limits, background image revisions, publishing through a staging file with the previous file
put back when the theme is not served or cannot be selected, and selection through the
`defaultTheme`/`defaultAppearance` settings. The built-in and reserved ids, the CSS limit and the
restyling guide are generated from the reference into `src/mcp_oauth/themes.json` by
`scripts/update_agent_tools.py`. `server.subscribeConfig` now sends the published themes, and again
when the themes directory changes. Parity: the same two suites.

### 4. Calling session

Served: Listing by caller, `tool_search` and the deferred listing, `send_to_user`,
`t3_background_wait`. Rust's Claude sessions report no background work, so `t3_background_wait`
matches no task id; Codex cannot mark work as awaited, as in the reference. Parity: the scenario
`codex thread tools` (including a deferring session) and `scripts/mcp_parity.py` (thread-only tools
refused to a client).

### 4b. Delegation

In parts. Served: `delegate_task` (both modes), `task_status`, `task_cancel` and `create_threads`,
on a port of the reference orchestrator's delegated tasks (`src/orchestration/delegation.rs`):

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
  upgrades the task's wake;
- `task_cancel` (`cancelTask`, `cancelDelegatedTask`): it checks the child and every task
  under it against the parent's modes, records the run the task settles from
  (`cancelledRunId`, so a completed run ends it interrupted and wakes nobody), stops the
  child's own tasks and then the child, removes the task's queued turn and disposes its
  delivery. A finished task keeps its result. `subagent.stop` on an app-owned task now
  follows the reference (it leaves a task its child still runs to settle from that run)
  instead of stopping the thread;
- a thread's stop reaches its delegated tasks (`delegatedTasksStopEffect`), and a stop or
  interrupt closes the stopped runs' completion cohorts (`disposition: "stopped"`), so their
  tasks' results wake nobody;
- `create_threads` (`createThreads`): up to 20 top-level threads in the caller's project,
  on its branch and worktree, with the reference's stable thread, message and command ids.
  A thread with a prompt starts in the same command (`thread.create` with
  `initialMessage`), its message naming the parent (`senderThreadId`), and
  `thread.created.record` adds a `thread_created` card to the parent's timeline after the
  provider items its run had then;
- follow-ups (`recordSubagentFollowup`): a message that starts a new run on a delegated
  child whose task already finished (a user's send, an MCP send from its parent, or a wake
  that is not a monitor's progress update) becomes a follow-up task on the parent, bound to
  the parent's active run or else its last, with its own card, and the child's run tagged
  with it (`delegatedTaskId`); a wake continues the open follow-up instead. Each of the
  child's tasks settles from its own runs (`delegatedTaskRuns`, `delegatedTaskResultRun`),
  and `task_status` reads a follow-up from them. `t3_thread_send` now uses the reference's
  stable ids, names the sending thread and returns the follow-up's `taskId`;
- restart recovery (`recoverDelegatedTasks`): once a restarted server has dispatched its
  restart continuations, it settles the tasks of children whose work ended without being
  settled (a child whose cut run still awaits its continuation waits for it) and offers
  again each delivery a cohort still holds. A delivery is accepted with its message
  (`acceptDelegatedCompletionDelivery`), and offered under a command id derived from its
  cohort and generation, so a delivery claimed but not sent before a stop is sent once;
- delivery into a running parent turn: a delivery whose tasks all wake the parent always
  steers into the parent's running turn when that turn's steering does not interrupt its
  tools (`dispatchMessage`'s `steer_active`), joining the run as its notification card. A
  Codex turn is steered (`turn/steer`); a Claude turn is not
  (`activeSteeringInterruptsTools`), so its delivery runs once the turn ends. A delegated
  child's Codex thread starts with `threadSource: "subagent"`, as the reference starts it.

**Parity**, in `scripts/client_thread_parity.py`, with fake CLIs for the parent and the child:

- `claude delegate` / `codex delegate`: a wait.
- `claude delegate async` / `codex delegate async`: a delivery run on an idle parent.
- `claude delegate cancel` / `codex delegate cancel`: cancelled while its child runs.
- `claude delegate stop` / `codex delegate stop`: the user stops the parent.
- `claude create threads`: one thread started with its prompt, one left idle.
- `claude delegate followup` / `codex delegate followup`: the parent sends its finished child
  more work, whose result wakes it.
- `claude delegate restart` / `codex delegate restart`: the server restarts while a child runs.
- `claude delegate live` / `codex delegate live`: a child that finishes while its parent's turn
  runs.
- `claude delegate worktree` and `claude delegate remote`: below.

**Still pending**, each its own PR: a cancelled child's pull request watches, a stop's queued delivery
runs (the reference cancels a cohort's queued delivery), and recovery on the control plane (only the
server recovers delegated tasks after a restart).

While a Claude turn runs, the reference shows its delivery as a queued run at once; Rust, which
holds queued runs until a client starts them, records the delivery's run when the turn ends, so only
the final state matches.

**Worktree isolation** (native server only): `isolation: "worktree"` resolves the parent checkout's
commit and gives the child its own worktree there, as the reference's
`ThreadLaunchService.prepareRun` does. The child's first run waits as `preparing` (its
`workspacePreparation` recorded on the run) while the server checks the commit out on a temporary
`t3/<hex>` branch, records it on the child thread, renames the branch to one the text generation
model names, runs the project's worktree setup script and releases the run into the checkout;
`subscribeWorktreeSetup` shows the progress. A failure fails the run and removes a worktree it never
recorded. The commit and the rename use two server-internal Git operations (`resolveCommit`,
`renameBranch`) that clients cannot call; a t3-agentd workspace serves them too, and an older
t3-agentd turns the isolation down as an unresolved commit.

**Differences:** Rust does not wait for a setup script to finish before releasing the run, and a
failed branch rename is only logged (the reference adds a "Branch not renamed" notice to the run).
The control plane still refuses the isolation.

**Parity:** `claude delegate worktree`, in a temporary git repository, where the child reads its own
`t3_worktree_status`.

**Tasks in a linked environment** (`target.environmentId`, native server only), as the reference's
`RemoteDelegation`: the task's thread is launched there through the link (`t3_thread_launch` with
`delegatedFrom`, the project matched by repository when `target.projectId` is omitted, the driver's
instance found with `orchestrator_capabilities` for `target.driverKind`), and the task is recorded
here (`delegated_task.remote.request`: its node and card, no child thread). A follower waits on that
thread through the link (`t3_thread_wait`), reads its last reply (`t3_thread_read`) and records it
as the result (`delegated_task.remote.complete`), which reaches the parent as a local task's result
does: a `wait` reads it, an async task wakes the parent. `task_status` answers from the task's row
and the run's status there; `task_cancel` stops the thread there (`t3_thread_interrupt` with
`stop`). A gone link, thread or run fails the task; anything else is retried with back-off, and open
tasks are followed again after a restart. A follow-up sent to a finished task's thread there with
`t3_thread_send` becomes a task of its own (`RemoteDelegation.followUp`), followed as the first was.

**Pending:** a task whose thread there waits on its own delegated tasks is reported when its run
ends. The control plane still refuses these tasks.

**Parity:** `claude delegate remote` (a wait), `claude delegate remote followup` (a follow-up sent
there becomes a task, whose result wakes the idle parent's running CLI) and `claude delegate remote cancel`
(cancelled while its thread there runs, then cancelled again), against a loopback linked environment that records
each call (`scripts/remote_task_peer.py`). In the cancel, the result is steered into the parent's
running Claude turn, which ends before reading it, and the CLI answers it as its next turn. Both
servers leave such an unread steer to the CLI, counted delivered, and open a wake continuation run
when Claude answers it.

### 4c. Attachments

Served, native server only: `t3_attachment_prepare_upload` issues an upload URL (with `uploadUrl` at
the origin the call reached) for an upload its caller owns; `t3_attachment_discard` deletes it, for
that caller only; `t3_thread_send_attachments` sends pending uploads, or attachments the target
thread already holds, as a new message there, claiming them for it
(`ThreadMessageIntake.sendToThread`). `scripts/attachment_tools_parity.py` compares all three with a
fake agent, posted fixture files and a fake Codex on the target. Unlike the reference, a send to a
thread with a running turn queues behind it rather than steering into it.

**What the tools build on:** a `message.dispatch` claims its pending uploads for the thread
(`ThreadMessageIntake`, `src/attachment_claims.rs`), the message and its turn item keep them, and
the run's provider turn carries them in the optional `ProviderTurn.attachments` (omitted when empty,
so the agentd wire is unchanged without attachments). Codex gets the text naming each file and each
supported image as a data URL; Claude gets each image as a base64 block, then that text. A queued
run's edit that names attachments replaces its message's (claiming pending uploads; a thread file
that is gone is refused, unless the edit is a retry of one accepted), and a launched thread's first
message claims its uploads for the new thread (which then needs its id); a send's copies that no
message keeps go again. `scripts/attachment_flows_parity.py` compares both, and what the provider is
asked.

**Not yet:** attachments on steers and runtime-request answers, Claude's PDF document blocks and its
image budget cap, captured-window context, document text extraction, and a remote agentd reading the
server's attachment files.

### 4d. Secrets

Served: `request_secret` shows a card in the caller's running turn (`secret_request.record`, placed
among its run's items as the reference places it) and waits for `secrets.answerRequest`, up to its
timeout or the run's end, which close the card. An answer is kept as the reference's
`SecretRequests` keeps it, in the server's secrets directory beside the reference's, so either
server reads the other's: a one-use `secret-ref:` (an HMAC of the thread and card under
`secret-request-salt`) over an owner-only `secret-request-<hex>.bin`, and for `delivery: "file"` or
a pasted `value` an owner-only file under `files/`. A secret provider answer reads the value once
through the settings' resolver. `scripts/secret_request_parity.py` compares it all with dummy
values. Values unused after 24 hours, and files given out longer ago, are swept hourly. A new
`schedule_task` is saved as the client's `scheduledTasks.upsert` saves a task, so a webhook task
gets its URL token and signing secret; a signature's `secretRef` is spent by that save (one use, for
its project; a replayed save keeps the secret it stored), and the suite checks which secret the
task's URL then accepts. A webhook request's relay headers (its delivery id and receive time, which
`maxDeliveryAgeMinutes` checks) count only with the relay's signed delivery proof
(`RelayDeliveryProof`).

**Not yet:** the control plane keeps no secrets (`request_secret` there cannot store one), a relay
delivery sent twice is not yet answered as a duplicate, and updating a task with `schedule_task`
still writes it directly.

### 4e. HTML pages

Served, native server only: `html_preview` and `html_render` work as the reference's `HtmlRender`
does (`src/html_render.rs`). A page given inline, or as a file by a full-access thread, is checked
(a regular, singly linked `.html` file outside the server's state that starts like a page; no
viewport height units), gets the reference's theme bootstrap (`scripts/reference_html_render.mjs`
generates `src/html_render/constants.json`) and has its local images inlined, judged by their bytes.
A preview renders it in headless Chromium and returns the screenshot as image content with its
height and console output; a render stores it as the thread's `-html` attachment, measured at the
reference's widths. The browser is the preview tools' headless shell, on a throwaway profile, driven
over its DevTools pipe (no debugging port); the page is served to it from memory, other main-frame
navigations are refused, and its network goes through a SOCKS proxy that connects only to public
addresses (`publicProxy`). A tool's compact output keeps `htmlRender`, as the client reads it.
`scripts/html_render_parity.py` compares both tools in a real headless Chromium, screenshots by
their pixels.

The browser, for these tools and the preview tools, is only the reference's pinned Chrome for
Testing `chrome-headless-shell` (`src/preview_browser.rs`). It is installed in
`<T3 home>/tools/chrome-headless-shell/<platform>/<version>/` the first time a tool needs it,
checked against the pinned size and SHA-256, and a caller waits up to 45 seconds before it is told
the install's progress. `scripts/preview_session_parity.py` compares the tools' answers when the
download fails.

When the browser will not start, the tools explain why as the reference's `PreviewBrowserHost`
does (`src/preview_browser_host.rs`): Chrome refusing its sandbox as root, a host that blocks the
sandbox (AppArmor's user-namespace restriction), or shared libraries it cannot load (found with
`--version`, then `ldd`), each naming the command that fixes it. That command is
`t3code-server browser setup` (`src/browser_setup.rs`, the reference's `t3 browser setup`): as root
it writes and loads the AppArmor profile and installs the libraries and fonts with apt; otherwise
it says what is needed. `scripts/preview_session_parity.py` compares the hints with fake browsers
that fail each way, and `scripts/browser_setup_parity.py` compares the command, with fake
`apparmor_parser` and apt, as root in a user namespace over an overlay of /etc.

**Not yet:** the OpenCode and ACP adapters' recorded renders (`recordHtmlRender`).

### 4f. Preview status, dialogs and recordings

Served, native server only: `preview_status`, `preview_dialog`, `preview_recording_start` and
`preview_recording_stop` act on the server's headless Chromium tab, as the reference's server
browser does. Status reports the tab (or that there is none) with its pending JavaScript dialog
(type, message, default text, from `Page.javascriptDialogOpening`); while a dialog blocks the page
only status and the dialog's answer run, with the recorded address and no title. A recording draws
the tab's screencast onto a canvas in a scratch page of the same browser, whose `MediaRecorder`
encodes it with the reference's encoder script (`update_agent_tools.py` writes
`src/local_services/browser/recording_encoder.js` from the pinned reference); the stop adds the
tab's settled final frame, leaves the video as a pending attachment, and the tool claims it for the
thread (`claimPreviewRecording`). `scripts/preview_session_parity.py` compares the four tools in a
real headless Chromium on a loopback page.

The agent's cursor shows where it acts, as the reference's `pointerFor` does: before a click,
hover or drag, the tab's viewers get `pointer` messages (a move, and for a click its ring) and a
recording draws the cursor and ring into its frames (`__t3Recorder.cursor`), with the reference's
pauses so they can be seen. Nothing waits when no one watches or records.
`scripts/preview_provider_parity.py` compares the messages a passive viewer receives.

A recording is claimed where its browser ran, which keeps the thread's attachments: the
server's own workspace, or, for the control plane, the `t3-agentd` workspace on another machine
(`preview.claimRecording`, an agent-only workspace operation that clients cannot call, carried by
agentd's existing `preview.call`). An older agentd that does not know it fails the claim as before.
`agentd/tests/remote.rs` claims one through agentd.

### 4g. Run output

Served: `t3_thread_send` and `t3_thread_launch` take an `outputSchema` (a JSON Schema), which the
run keeps (`outputSchema`, and a continuation inherits it); a message that asks for a result never
steers, and one the reference cannot ask (`outputRequestProblem`: a provider command, a schema over
32,000 characters or one its compiler refuses, such as `pattern`) is refused. The turn's prompt ends
with the reference's instructions (`outputInstructions`). `t3_submit_output` checks the agent's JSON
against the schema with the reference's wording (`src/run_output.rs`, the issues effect's decoder
reports for the keywords it compiles) and keeps it on the active run, or on a completed run that
still owes it (`run.output.submit`, recorded as the run's `output`). `t3_thread_wait` answers with
`output`, and with `untilOutput` keeps waiting while a completed run owes it.
`scripts/run_output_parity.py` compares it all.

**Not yet:** forgetting an owed result after a fresh-context run (Rust has no fresh context), JSON
Schema keywords beyond the common ones (types, enum, const, anyOf/oneOf, properties, required,
additionalProperties, items, numeric and length bounds), and the control plane's prompt
instructions.

### 4h. Feedback

Served, native server only: `t3_feedback` is listed, and callable, only while the
`agentFeedback.mode` setting is not `off`; otherwise a call is "not found", as on the reference. A
report names a T3 tool (bare, MCP-prefixed or a `t3` command) or `general`, and its text is scrubbed
as the reference's `scrubFeedback` does (`src/feedback/scrub.rs`): secrets, emails, credential URLs,
the caller's thread, project and workspace values, file paths and long random runs become
`[redacted]`. An unprompted report is stored in the shared `statev2.sqlite` `agent_feedback` table,
three per turn and ten per hour. One the user asked for shows a card (`feedback_request.record`)
with the report and, when the caller asks, an outline of the last turns (`src/feedback/trace.rs`);
`agentFeedback.answerRequest` sends it, optionally with the conversation's scrubbed text, or
declines it, once. Rust has no telemetry, so every report is stored unsent (`sent = 0`), as the
reference stores it with telemetry off. `scripts/feedback_parity.py` compares listing, answers,
cards and stored rows in both modes.

### 4i. Workflows

Served without an engine, native server only: `t3_workflow`, listed to outside clients only, answers
as the reference does with an empty workflows folder (`src/workflows.rs`). Durable runs are user
TypeScript files the reference's engine replays, which this server does not run. With
`workflowsEnabled` off (the default) every action is refused; on, with no run in the shared
`statev2.sqlite`, nothing is there; on, with runs the reference kept, those are listed and read
(`retired`, since nothing is registered), their questions shown, the store read and changed, waits
signalled (checked against the wait's schema, kept for a wait not yet open, and once per signal id),
and runs cancelled with what they invoked, as the reference's engine with nothing registered does
over the same tables. `start` and `resume` answer that the workflow is not registered, and `hooks`
is empty.

`scripts/workflow_parity.py` builds history with a fake workflow on the reference (steps, a
question, a channel wait, a failing step, stop, skip and a child run; no network or agents) and
compares every action in the three states and the tables left.

The client's `workflows.subscribe` (a snapshot of runs and questions, sent again once a burst of run
changes settles), `workflows.counts` (sent when a count moves), `workflows.get` and
`workflows.answer` answer the same way, starting over when workflows are turned on or off, and
`workflows.resume` (nothing is registered to resume) and `workflows.cancel` act as the tool's resume
and cancel do; `scripts/workflow_rpc_parity.py` compares them.

**Not yet:** the definition RPCs; running workflows (definitions, triggers, hooks, the sweep),
milestone news of answered questions and ended runs, and stopping an agent step's thread in a linked
environment on cancel.

### 4j. Device hub and sessions

Served, native server only: `device_list`, `device_open`, `device_screenshot` and `device_close`,
gated by the thread's `"device"` capability (`enableDeviceSupport` and effective
`enableAgentDeviceAccess`, including project overrides). `DeviceService` starts the local
`expo-device-hub` with the reference's environment (`FORCE_COLOR=0`, `NO_COLOR=1`, `ANDROID_HOME`,
emulator camera overrides, `hub.json` stale-hub reaping, `/readyz` readiness), lists booted devices,
unbooted AVDs (`emulator -list-avds`) and the `avdmanager`-failure `adb` fallback, resolves host ids
or unique case-insensitive labels, checks `ffmpeg` `libx264` before opening Android emulators, and
closes or shuts down thread sessions cleanly. `scripts/device_tools_parity.py` compares every tool
answer and every request sent to loopback fakes (fake `expo-device-hub`, fake Android SDK, fake
`ffmpeg` and a fake `agent-device` CLI and daemon). `device_open` starts this machine's
`agent-device` daemon (`daemon.json`, `/health`), writes the host's endpoint config, and binds a
launcher per agent-device session (`device/targets/<session>/agent-device`, the reference's launcher
script). Closing a session releases its agent-device session on the daemon (`/rpc` `close`); a
shutdown also runs the launcher's `close`. Turning agent access off stops the daemon with `daemon
stop`. `device_screenshot` is an image tool: hub screenshot, `extractPng`, the Xcode 26 `simctl`
fallback, and the reference's `imageToolFailure` shape. Threads that may use devices get the
reference's device instruction text (see above).

**SSH device hosts** (`SshDeviceHost.ts`, started): the hosts in `deviceHosts` that are not this machine
(`ssh -G`, as `remoteSshDeviceHosts` checks) are listed, kept in step with settings, and dropped
with their devices, sessions, status and agent config when removed. `device.testHost` and
`device.list` with `inspectOnly` run the reference's probe: `ssh` with its options (`baseSshArgs`,
`runSshCommand`), its shell prelude and its Node bootstrap on stdin
(`src/local_services/device_ssh.json`, generated from the pinned reference by
`scripts/update_agent_tools.py`), under the owner name `ownerFor` gives this server, with its
failure messages (connect errors, host keys, Windows hosts) and encoded causes.
`scripts/device_ssh_parity.py` compares them against a fake `ssh` that runs a host's commands under
its own fake home on this machine.

With device support on, listing starts each SSH host as `ensureReady` and `connect` do: probe, the
`installing`/`starting`/`ready` statuses, the bootstrap's `start` (which installs and starts the hub
there), an `ssh -L` tunnel to fresh loopback ports here, and `/readyz` through it (retried with
fresh ports when the forward fails). Its devices are listed through the tunnel, with `adb` and
`emulator` run over SSH from the hub's directory (`run`); opening, booting, the ffmpeg check (the
reference's `node -e` script on the host's Node), closing, shutting down, `retryHostId` and
screenshots work on it as on this machine, the Device panel's hub requests reach it through this
server's proxy (`/api/device-hub/...?hostId=`, as `DeviceHubProxy` routes them, a screenshot of one
device coming from the device service as there), and turning device support off (or removing the
host, or shutting the server down) stops what this server started there.

Agents use SSH hosts as they use this machine (`ensureAgentReady`, `changeAgent`): the host
reconnects with the bootstrap's `agent-start`, the tunnel also forwards its agent-device daemon, the
host's agent config names the forwarded daemon, `device_open` binds this machine's agent-device CLI
to it, closing releases the session through it, and turning agent access off runs `stop-agent` and
reconnects without it. A connected host is supervised as the reference supervises it: a tunnel that
exits, or three missed `/readyz`/`/health` probes 10 seconds apart, reconnects ("Reconnecting to
device host…"), backing off from 1 to 30 seconds. `scripts/device_tools_parity.py`'s `ssh` phases
drive the tools on the fake host and run the returned launcher through the tunnel;
`scripts/device_ssh_parity.py` drops the tunnel and compares the reconnect.

**Not done yet:** device detail and actions on SSH hosts.

**Simlock hosts** (`SimlockDeviceHost.ts`, alpha): with `simlockDeviceHostsEnabled`, the hosts in
`simlockDeviceHosts` (after the SSH hosts; an id two lists share stays the first one's) are listed,
and clients receive them as `simlockHosts`, apart from this machine's and SSH hosts, as
`DeviceServiceState`'s encoding splits them. Their devices are the gateway's catalog (phones and
tablets with a runtime, cached for a minute when every platform has some) and the leases this server
holds. The agent token is the saved one for that gateway, else this machine's agent-device token
when it points at the same gateway. Opening a device takes a lease through this machine's
agent-device CLI (`connect proxy`, `open` from the thread's workspace, the lease recorded under
`remote-connections`), within `maxLeases`; it is renewed every two minutes until the gateway ends it
or it reaches `maxLeaseMinutes`, and released through its session (or `DELETE /v1/leases/<id>`) on
shutdown, removal, turning device support off, or server shutdown. `device_open` binds the lease's
own session and per-device config, with the reference's Simlock quick-start notes; screenshots go
through the lease's session. `scripts/device_tools_parity.py`'s `simlock` phase compares the tools,
the wire state, the launcher and every request to a fake gateway on loopback. The Device panel's
stream and input for a lease go through the hub proxy to the gateway's routes for that lease
(`upstream`), with its token; its `simlock-stream` phase compares them.

**Expo Simulators hosts** (`EasDeviceHost.ts`): the hosts in `easDeviceHosts` are listed
(after the SSH hosts) and sent to clients as `easHosts`; each platform is available when this server
can read an Expo credential for the host (the saved token, then `EXPO_TOKEN`, then the `eas login`
session), and listing shows the two-entry catalog plus the host's running sessions.
`device.easAccount` reads the signed-in account with the reference's GraphQL documents
(`src/local_services/expo_documents.json`, generated from the pinned reference) and its error
categories. Opening a catalog entry starts a session under the host's `appId`, or else the project
the thread's workspace names (`easProjectId.ts`, below): it is recorded in the
host's ledger (`<stateDir>/device/eas/<id>.json`) before Expo creates it, counted against
`maxConcurrentSessions` (with starts in progress, stops that failed and creates whose outcome is
unknown), polled every 5 seconds until it runs (15 minutes at most), and tracked by its preview
host's device id. Listing rechecks sessions older than 15 seconds and drops those Expo ended;
records a previous server left are counted for 5 minutes and reaped (stopped if Expo still runs
them, forgotten once too old). Closing with `shutdown` stops the session; removing the host,
turning device support off and shutting the server down stop all of its sessions. Screenshots come
from the preview host; `device_open` binds agent-device to the session's own daemon (a per-device
config, `--udid`/`--serial` from the preview host) with the reference's Expo note in its quick start,
and closing releases the agent-device session through that daemon. The hub proxy sends the Device
panel's requests for a session to its preview host, swapping the session id for the preview host's
device id and adding the preview token (`serve-sim.token.*`/`serve-emu.token.*` on WebSocket
upgrades).

`scripts/device_eas_parity.py` serves a stateful fake of Expo's fixed HTTPS API on loopback with a
certificate from a generated test CA, behind its own proxy, which tunnels only `api.expo.dev:443`
there, plus each session's preview host and daemon on loopback HTTP; the Rust server trusts the CA
through `T3CODE_TEST_EXTRA_CA_FILE`, which only debug builds read (release builds ignore it). It
compares accounts, hosts, reaping a recorded session, opening, the limit, the ledger, the hub proxy
(HTTP and WebSocket) and closing. `scripts/device_tools_parity.py`'s `eas` and `eas-close` phases
compare the device tools on an Expo host, including the launcher.

**Finding a workspace's Expo project** (`easProjectId.ts`, `src/local_services/eas_project_id.rs`):
a host with no `appId` runs its sessions under the project the thread's workspace names, at its
root or in one app below `apps/` or `packages/`. Configs are never run: a dynamic config
(`app.config.ts` and the like) counts only when every mention of `projectId` is a quoted entry
nothing later can replace, or when it is shown to spread Expo's `config` (and its own `extra` and
`extra.eas`) so that app.json's id carries through; `app.config.json`/`app.json` are read as JSON,
wrapped in `expo` or not. When a config is unreadable or ambiguous, or apps disagree, there is no
answer, and opening says to choose a project. The reference does this with regular expressions
that look behind and ahead, which Rust's `regex` lacks; those are written out as scanners.
`src/local_services/eas_project_ids.json` holds the reference's answers for 165 workspaces (its
own tests' and variants of each rule), generated by `scripts/reference_eas_project_ids.mjs`
through `update_agent_tools.py` (so `--check` keeps it current), and a unit test checks every
one. `scripts/device_tools_parity.py`'s `eas-project` and `eas-no-project` phases open devices on
a host with no `appId` from a workspace that names a project and one that does not.

### 5. Prompt context and capabilities

Served, as above.

### 6. Scheduled tasks

`schedule_task`, `list_scheduled_tasks`, `delete_scheduled_task` and `run_scheduled_task_now` share
the reference's `scheduled_tasks` table. Thread callers default to their project; an outside client
can list all projects. Webhook summaries include their signing status. The native scheduler claims
due rows from SQLite, launches a fresh thread or sends to a bound thread, advances interval,
fixed-time and one-time schedules, records dispatch and provider outcomes, recovers interrupted rows
after restart, and rejects relay deliveries older than `maxDeliveryAgeMinutes`.

### 7. Pull requests and issues

These cover the `pullRequests` and `issues` toolkits. A thread with a connected forge can open a PR
from its checkout, edit it, submit a review, reply to a review conversation and merge the reviewed
head or stack. `scripts/forge_write_parity.py` compares the tool answers and the requests sent to
the GitHub and Bitbucket fixtures.

### 8. Worktrees, previews, devices and HTML

`t3_worktree_status`, `t3_worktree_handoff`, the `previewControls`, `preview`, `device` and `html`
toolkits. These drive the desktop's browser and device panels; what Rust lacks for them is listed
here when that batch starts.

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
  takes over effects the other left pending. A Pi, Grok or OpenCode text generation model
  titles a thread too: Pi in its RPC mode with no session, extensions or tools
  (`provider_status::pi::generate_text`), Grok in an ACP session that asks for approvals and is
  granted none (`provider_status::grok::generate_text`), OpenCode 1.x in a read-only session on
  the configured server or the managed `opencode serve` its status checks share
  (`provider_status::opencode::generate_text`), and Antigravity in an empty temporary directory
  over ACP with its private profile prepared as the reference prepares it (settings, skill links,
  a Node browser helper that only reports a sign-in URL), refusing any tool use, and the
  session's files removed afterwards (`provider_status::antigravity_text`), each reply decoded as
  the reference decodes it (`extractJsonObject`). Not ported: titles on Cursor and Muse (the
  reference runs their SDKs in process) and OpenCode 2 (`OpenCode2TextGeneration`), image
  attachments in an OpenCode title prompt, Antigravity's start-up sweep of old runtime temp
  directories and its refusal to run during a sign-in, custom text generators, the source
  control lookup of linked PRs and issues, and the source thread's history in a fork's title.
  The `codex title`, `claude title fallback`, `codex title failed`, `pi title`, `grok title`,
  `opencode title` and `antigravity title` scenarios in `scripts/client_thread_parity.py`
  compare each CLI's arguments, prompt and schema (for Pi, Grok, OpenCode and Antigravity, the
  launch, Antigravity's launch environment, and every request of the title runs), and the
  thread's title afterwards.
- **Steering.** A `message.dispatch` with `steer_active` steers a running Codex or Claude
  turn as the reference does (`src/orchestration/steer.rs`): the message joins the run as a
  `steer` user message; a run whose turn ended starts a new turn, and one whose turn has not
  started queues it. Codex gets `turn/steer`. Claude's running CLI gets the message with
  priority `now` and a uuid; the steer reads pending (`steer.updated`) until Claude echoes the
  uuid, and one still pending when the turn ends is completed with a completed turn, otherwise
  cancelled. Not ported: a steer that changes the model selection or carries attachments (the
  reference restarts the turn), moving Claude's foreground subagents to the background before a
  steer, the "Steer delivery not confirmed" notice, and the follow-up turn the reference starts
  when the provider refuses a steer. `t3_thread_send` still refuses `mode: "steer"`. Parity:
  `codex steer then stop`, `claude steer` and `claude steer then stop` in
  `scripts/client_thread_parity.py`.
- **Codex delegation parity** (covered). Rust runs an instance's Codex turns in one shared
  app-server, as the reference does (#152), so a delegated child's Codex turn comes into the
  parent's app-server. The Codex delegation scenarios (`codex delegate`, `codex delegate async`,
  `codex delegate followup`, `codex delegate cancel`, `codex delegate stop`,
  `codex delegate restart`, `codex delegate live`) compare all of them alongside Claude's.
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
<!-- Generated by scripts/update_agent_tools.py: 94 of 94 served. -->

| | Tool | Toolkit | Listed to | Caller check |
|---|---|---|---|---|
| [x] | `t3_attachment_discard` | attachment | every caller | `writes` |
| [x] | `t3_attachment_prepare_upload` | attachment | every caller | `writes` |
| [x] | `t3_thread_send_attachments` | attachment | every caller | `writesThreads` |
| [x] | `device_close` | device | threads only | `actsAsCaller` |
| [x] | `device_list` | device | threads only | `readsAsCaller` |
| [x] | `device_open` | device | threads only | `actsAsCaller` |
| [x] | `device_screenshot` | device | threads only | `readsAsCaller` |
| [x] | `t3_environment_preferences_update` | environment | every caller | `writesEnvironment` |
| [x] | `t3_environment_read` | environment | every caller | `reads` |
| [x] | `t3_feedback` | feedback | feedback on | `writes` |
| [x] | `html_preview` | html | threads only | `readsAsCaller` |
| [x] | `html_render` | html | threads only | `actsAsCaller` |
| [x] | `comment_on_issue` | issues | every caller | `writesWithCapability` |
| [x] | `create_issue` | issues | every caller | `writesWithCapability` |
| [x] | `link_issue` | issues | every caller | `writesThreads` |
| [x] | `list_issues` | issues | every caller | `reads` |
| [x] | `list_thread_issues` | issues | every caller | `reads` |
| [x] | `read_issue` | issues | every caller | `reads` |
| [x] | `set_issue_state` | issues | every caller | `writesWithCapability` |
| [x] | `unlink_issue` | issues | every caller | `writesThreads` |
| [x] | `send_to_user` | message | threads only | `actsAsCaller` |
| [x] | `create_threads` | orchestrator | threads only | `actsAsCaller` |
| [x] | `delegate_task` | orchestrator | threads only | `actsAsCaller` |
| [x] | `delete_scheduled_task` | orchestrator | every caller | `writes` |
| [x] | `list_scheduled_tasks` | orchestrator | every caller | `reads` |
| [x] | `orchestrator_capabilities` | orchestrator | every caller | `reads` |
| [x] | `request_secret` | orchestrator | threads only | `actsAsCaller` |
| [x] | `schedule_task` | orchestrator | every caller | `either(startsThreads, writes)` |
| [x] | `t3_background_wait` | orchestrator | threads that can wait | `readsAsCaller` |
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
| [x] | `preview_act` | preview | threads only | `actsAsCaller` |
| [x] | `preview_dialog` | preview | threads only | `actsAsCaller` |
| [x] | `preview_evaluate` | preview | threads only | `actsAsCaller` |
| [x] | `preview_navigate` | preview | threads only | `actsAsCaller` |
| [x] | `preview_open` | preview | threads only | `actsAsCaller` |
| [x] | `preview_recording_start` | preview | threads only | `actsAsCaller` |
| [x] | `preview_recording_stop` | preview | threads only | `actsAsCaller` |
| [x] | `preview_resize` | preview | threads only | `actsAsCaller` |
| [x] | `preview_set_appearance` | preview | threads only | `actsAsCaller` |
| [x] | `preview_snapshot` | preview | threads only | `readsAsCaller` |
| [x] | `preview_status` | preview | threads only | `readsAsCaller` |
| [x] | `preview_wait_for` | preview | threads only | `readsAsCaller` |
| [x] | `t3_preview_close` | previewControls | threads only | `actsAsCaller` |
| [x] | `t3_preview_list` | previewControls | threads only | `readsAsCaller` |
| [x] | `t3_project_clone` | project | every caller | `writesEnvironment` |
| [x] | `t3_project_create` | project | every caller | `writesEnvironment` |
| [x] | `t3_project_delete` | project | every caller | `writesEnvironment` |
| [x] | `t3_project_list` | project | every caller | `reads` |
| [x] | `t3_project_read` | project | every caller | `reads` |
| [x] | `t3_project_update` | project | every caller | `writesEnvironment` |
| [x] | `t3_thread_launch` | project | every caller | `startsThreads` |
| [x] | `ci_logs` | pullRequests | a forge connected | `reads` |
| [x] | `ci_rerun` | pullRequests | a forge connected | `writesWithCapability` |
| [x] | `link_pull_request` | pullRequests | every caller | `writesThreads` |
| [x] | `link_pull_requests` | pullRequests | every caller | `writesThreads` |
| [x] | `list_thread_pull_requests` | pullRequests | every caller | `reads` |
| [x] | `pull_request_create` | pullRequests | a forge connected | `writesWithCapability` |
| [x] | `pull_request_list` | pullRequests | a forge connected | `reads` |
| [x] | `pull_request_merge` | pullRequests | a forge connected | `writesWithCapability` |
| [x] | `pull_request_read` | pullRequests | a forge connected | `reads` |
| [x] | `pull_request_respond` | pullRequests | a forge connected | `writesWithCapability` |
| [x] | `pull_request_review` | pullRequests | a forge connected | `writesWithCapability` |
| [x] | `pull_request_update` | pullRequests | a forge connected | `writesWithCapability` |
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
| [x] | `t3_submit_output` | thread | threads only | `actsAsCaller` |
| [x] | `t3_thread_configure` | thread | every caller | `either(reads, writesThreads)` |
| [x] | `t3_thread_organize` | thread | every caller | `writesThreads` |
| [x] | `t3_thread_search` | thread | every caller | `reads` |
| [x] | `t3_thread_transfer` | thread | every caller | `either(reads, writesThreads)` |
| [x] | `t3_workflow` | workflow | outside clients only | `either(reads, writesOwnerWork)` |
| [x] | `t3_worktree_handoff` | worktree | threads only | `actsAsCaller` |
| [x] | `t3_worktree_list` | worktree | every caller | `reads` |
| [x] | `t3_worktree_status` | worktree | threads only | `readsAsCaller` |
<!-- tools:end -->
