<p align="center">
  <img src="docs/assets/ferrum-social-card.jpg" alt="Ferrum: an unofficial, independent, high-performance Rust implementation of the T3 Code server and relay" width="100%">
</p>

# Ferrum

**An unofficial, independent, high-performance Rust implementation of the T3 Code server and relay.**

> [!WARNING]
> **Alpha-quality software under heavy development.** Do **not** use this for anything important. Expect breaking changes, incomplete features, and data loss.

**No affiliation:** Ferrum is an unofficial, independent Rust port of the headless [T3 Code](https://github.com/pingdotgg/t3code) server (`t3 serve`) and standalone relay. This project has **no affiliation with T3 Code or T3 Tools Inc., and is not endorsed by them in any way**. T3 Code is a product of T3 Tools Inc.

---

## Overview

Ferrum reimplements the TypeScript T3 Code environment server and standalone relay in Rust. Compatibility work targets one pinned upstream commit, listed under [Parity status](#parity-status). At that commit the server declares 261 WebSocket RPC methods, 91 typed HTTP operations (including 32 relay operations), and 58 orchestration dispatch command types.

## Quick start

### Build and run from source

Requires the Rust toolchain pinned in [`rust-toolchain.toml`](rust-toolchain.toml).

```bash
cargo build --locked
target/debug/t3code-server --listen 127.0.0.1:3773 --data-dir .t3
```

To run the standalone relay instead:

```bash
target/debug/t3code-server --relay --listen 127.0.0.1:3774 --data-dir .t3
```

### Switch an existing server with `./install`

From the repository root, `./install` temporarily replaces a running standalone TypeScript server with a Rust build and restores it later (Linux and macOS):

```bash
./install --status     # Show the discovered server and switch state
./install --rust       # Build and start the Rust server in place
./install --original   # Stop Rust and restore the TypeScript server
```

How `./install --rust` works:
- Discovers the live server via `server-runtime.json`, verifies `GET /` returns 2xx, and saves the original command, environment, working directory, and T3 home under `${XDG_STATE_HOME:-~/.local/state}/ferrum-switch` (override the parent directory with `FERRUM_SWITCH_STATE`; a switch saved under the older `t3code-rust-switch` directory is still honored).
- Installs Rust via `rustup` if `cargo` is missing, builds a locked release binary, and starts `t3code-server` on the same host, port, and data directory.
- On Linux, stops, disables, and runtime-masks a discovered user `systemd` service while Rust is running; `./install --original` unmasks and restores it.
- Restores the TypeScript server automatically if the Rust server fails to start.
- Uses the reference update-restart marker (`runtime/desktop-update-restart`) on each switch so the T3 Connect managed tunnel hands over immediately instead of reprovisioning.
- Logs at `info` by default (override with `RUST_LOG`) to `rust-server.log` in the switch state directory.

Safety notes:
- Run `./install --status` first on an unfamiliar machine, and quit the T3 desktop app before switching its server.
- Set `T3CODE_HOME` before running `./install` if your server uses a custom T3 home.
- Refuses to act if it finds multiple live servers, a stale or incomplete runtime record, an unidentifiable background service, a desktop-managed server, or a missing original binary or working directory.

## Configuration

### Directories and environment variables

`T3CODE_HOME` and `--base-dir` select the base directory, with mutable files under `userdata/` (`settings.json`, `keybindings.json`, `themes`, `attachments`, `browser-artifacts`, `logs`, `secrets`, `environment-id`, `anonymous-id`, `server-runtime.json`).

Standard environment variables match the TypeScript server:
- `T3CODE_HOST`, `T3CODE_PORT`, `T3CODE_MODE`, `T3CODE_NO_BROWSER`
- `T3CODE_AUTO_BOOTSTRAP_PROJECT_FROM_CWD`, `T3CODE_LOG_WS_EVENTS`, `T3CODE_DEV_ALLOWED_ORIGINS`
- `T3CODE_TAILSCALE_SERVE`, `T3CODE_TAILSCALE_SERVE_PORT`
- `T3CODE_STATIC_DIR` (or `--static-dir`): serves the built web client at `GET /` when set, or a placeholder page otherwise

### Bootstrap and ownership

- **Desktop bootstrap:** An environment launcher can pass the desktop bootstrap envelope on an inherited file descriptor with `--bootstrap-fd` (bind address, port, T3 home, and bootstrap credential). Exchange the credential at `POST /api/auth/browser-session` and send the resulting cookie to authenticated HTTP endpoints. For a standalone process, set `T3CODE_BOOTSTRAP_CREDENTIAL` to a high-entropy secret.
- **Exclusive ownership:** At startup, the server acquires an exclusive SQLite lock on `userdata/server-owner.sqlite` (waiting up to 250 ms) and exits with status `78` if another Rust or TypeScript server already owns the home directory. Once listening, it atomically writes `userdata/server-runtime.json` and removes it on `SIGINT`/`SIGTERM`.

### Standalone relay

When running with `--relay`, configure:
- `RELAY_ISSUER` (canonical public URL)
- `CLERK_JWT_KEY` (RS256 public PEM), `CLERK_ISSUER`, `CLERK_JWT_AUDIENCE`
- `CLOUD_MINT_PRIVATE_KEY` (Ed25519 PKCS#8 PEM), `CLOUD_MINT_PUBLIC_KEY` (SPKI PEM)

Relay links, mobile registrations, and replay protection persist in SQLite.

### WebSocket RPC

Connect to `/ws?orchestrationProtocol=2` with a session cookie or WebSocket ticket. Clients send Effect RPC messages as JSON text frames (string or numeric request IDs; add `frameParts` to the URL for >48 KiB frame chunking):

```json
{"_tag":"Request","id":"1","tag":"server.probe","payload":{},"headers":[]}
```

```json
{"_tag":"Exit","requestId":"1","exit":{"_tag":"Success","value":{}}}
```

## Status

### Parity status

Parity is tested by running the original TypeScript server and the Rust server side by side and comparing their responses. Each server gets a fresh home directory, the same fake provider CLIs (replaying recorded Codex and Claude sessions, scripted ACP sessions for Grok, ACP Registry agents, Antigravity, Kiro and fx, Pi RPC sessions, Muse MSP hosts, and OpenCode servers), and local stand-ins for outside services. No test reaches the network or a real model.

- **Pinned reference:** [upstream commit `44a53337`](https://github.com/pingdotgg/t3code/tree/44a533378653164dea96435e23c8ea27efaffedb) (`44a533378653164dea96435e23c8ea27efaffedb`). This is the only place the README names it.

| Area | Current status |
| --- | --- |
| CI parity suites | 65 suites across 13 shards on every code change (plus twice nightly); 6 more run on their own |
| Client RPC ([cases](scripts/client_rpc_cases.json)) | 260 client steps and 18 raw RPC comparisons; 0 expected failures ([xfail](scripts/client_rpc_parity_xfail.json)) |
| Live provider turns | 240 provider scenarios; 82 checks expected to fail, 29 of them races ([xfail](scripts/client_thread_parity_xfail.json)) |
| Shared home / server switch | 0 expected failures ([xfail](scripts/shared_store_parity_xfail.json)) |
| Provider status ([suite](scripts/provider_status_parity.py)) | 11 scenarios lack the OpenCode 2 model catalog event subscription ([xfail](scripts/provider_status_parity_xfail.json)) |
| Other suites | 0 known differences |
| Agent tools ([checklist](docs/agent-tools.md#checklist)) | 94 of 94 served |

A suite fails on any unlisted difference, and on any listed expected failure that starts to match.

**Intentional differences:** In `auto-accept-edits`, Antigravity runs a plainly read-only command line without asking, whereas the reference asks about every command. The line must be a single invocation of an allowlisted program (`ls`, `cat`, `head`, `tail`, `wc`, `pwd`, `grep`, `rg`, `find` without `-exec`/`-delete`/`-fprint`, `git status`/`log`/`diff`/`show` and similar), or a pipe of them, with no redirection, chaining, substitution, variables, environment assignments, or write-capable flags; anything else asks, as does every other mode and provider. The `antigravity auto-accept-edits read-only command` scenario records this in `scripts/client_thread_parity_xfail.json`.

### Performance

Ferrum is benchmarked against the pinned reference on the same machine with the same fake provider CLIs (installed size, startup, memory, CPU, RPC latency, and turn throughput). See [`docs/performance.md`](docs/performance.md) for current numbers and methodology; every run is recorded on the [`perf-data`](https://github.com/ttulttul/Ferrum/tree/perf-data) branch.

### What works now

Based on `main` and the parity test suites:

#### AI provider turns
- **Codex & Claude Code:** Launching sessions, streaming responses, tool calls, approvals (`allow`, `allow for session`, `decline`, `approval-required`, `auto-accept-edits`), user-input questions, and text steering. Each thread keeps its Claude CLI running across turns, and an instance's Codex turns share one app-server. When Claude Code starts a turn on its own between turns, the thread gets a continuation run naming the finished background command or monitor output.
- **Grok:** ACP turns in a per-thread `grok agent stdio` process with streaming reasoning, replies, tool calls, Grok's runtime modes, steering, delegated tasks, and T3 tools via `acp-mcp-bridge`. Stop restarts the process and the next turn reloads the session.
- **ACP Registry agents:** Generic ACP turns from a local command or registry distribution (downloaded binary, `npx`, or `uvx`), keeping the agent per thread with T3's MCP endpoint for terminal fallback. Stop sends `session/cancel` and keeps the agent running.
- **Pi:** RPC mode in a per-thread process with the reference's extension written byte for byte (`--extension`), streaming thinking, replies, and tool calls, with approvals and steering. Stop aborts and stops the process; the next turn resumes Pi's session file.
- **OpenCode (1.x & 2):** OpenCode 1.x runs a per-thread `opencode serve` (or configured server) with T3's MCP server and permission rules. OpenCode 2 shares one `opencode serve` per instance with status checks, writing per-thread MCP/instructions before each turn and using OpenCode 2's approvals, question forms, steering, and interrupt APIs.
- **Antigravity (`agy acp`):** Antigravity-flavor ACP with eager sign-in, `session/resume`, command output and exit codes, workspace file reads through T3, approvals and questions, steering (from agy 1.5), and `session/cancel` on Stop.
- **Kiro (`kiro-cli acp --agent-engine=v3 --auth-method=cli`):** Kiro-flavor ACP with hooks, model/effort selection, Autopilot in Full access only, tool outputs, Kiro approval labels, `_session/steer`, and session titles.
- **fx (`fx acp`):** fx-flavor ACP with user fx MCP servers, `backend:model` selection, runtime-mode mapping (`ask`, `auto`/`code`, `full-access`), `session/cancel` on Stop, and session metadata/context usage.
- **Muse Code (`muse serve`):** MSP protocol with `--trust-workspace` (and `--disable-sandbox` in Full access), streaming items and usage, approvals, `turn/interrupt`, `turn/steer`, and quota reset reporting.

#### Orchestration, tools, and platform
- **Delegated & scheduled tasks:** Child tasks (wait or async delivery, progress checks, cancel, follow-ups, worktree isolation, linked machines, new threads), restart recovery, delivery into a parent's running turn, and scheduled task management and execution.
- **Checkpoints, interrupts, usage & recovery:** Reference-compatible checkpoint naming, turn interrupts (stopping spawned child processes), token usage and model reporting, usage-limit reset times, and restart recovery for interrupted turns, retries, rewinds, and background work.
- **Thread titles:** Text generation titling via Codex, Claude, Pi, Grok, OpenCode (1.x and 2), and Antigravity.
- **MCP server & agent tools:** Serving MCP sign-in and all 94 agent tools over `/mcp` with the original's instructions and capabilities ([checklist](docs/agent-tools.md#checklist)).
- **T3 Connect relay & environment links:** Provisioning, running, releasing, and handing off the T3 Connect managed tunnel; incoming and outgoing environment links shared with the TypeScript server on the same home, MCP forwarding to linked machines, and in-conversation link requests (`t3_environment_link`).
- **Provider status checks & shell discovery:** Status checks (installed version, sign-in, usage, models, CLI releases, Claude skills, and Claude/Codex reset-credit redemption) for all built-in providers (Claude Code, Codex, Cursor, OpenCode, Grok, Pi, fx, Kiro, Antigravity, Muse Code) at startup, on settings changes, every 5 minutes while a client watches, and on refresh. Login-shell `PATH` discovery at startup and before source-control discovery.
- **Git, forges, previews & auth:** File operations, PTY terminals, git workflows, GitHub/Linear/Bitbucket provider RPCs (Bitbucket credentials from Settings or `T3CODE_BITBUCKET_*`), Chromium previews and preview tunnel, shared `userdata/` layout, browser session cookies, WebSocket tickets with DPoP checks, CORS, and desktop SSH launcher adoption.
- **Split control/workspace architecture:** Out-of-process workspace execution via `t3-agentd` and an embeddable, `wasm32-unknown-unknown`-compatible control plane (`workers/`). Remote Codex and Claude turns can call the host's MCP endpoint as their thread (see [`docs/embedding.md`](docs/embedding.md#settings-and-provider-turns)).

### Known gaps

#### Threads and history
- **Restarted Claude turns:** If a resumed Claude turn fails, its retry can miss context from the failed attempt.
- **Live provider updates:** A tool's node and turn item coalesce independently in the provider commit batch (upstream #39502). Batch boundaries can still hide intermediate command updates, including during automatic approvals. Short streaming replies, session status and request ordering account for other [live differences](scripts/client_thread_parity_xfail.json).
- **Older databases:** Both servers share the current database format. If a home has only the older TypeScript database, start the matching TypeScript server once to upgrade it (see [`docs/shared-store.md`](docs/shared-store.md)).

#### Agents and tools
- **Devices:** Simulators and emulators work locally, on SSH device hosts, from Simlock pools (alpha), and in Expo Simulators sessions; device detail and actions (settings, foreground app) work on this machine only.
- **Delegated tasks (embeddable control plane):** A child cannot run in its own worktree (`isolation: "worktree"`).
- **Secrets in schedules:** Scheduled tasks cannot use secrets yet, and the embeddable control plane stores no secrets.
- **Claude workflows:** Rust's Claude sessions do not run workflow tasks.

#### AI providers
- **Cursor turns:** Cursor gets status checks, but cannot run agent turns yet.
- **Grok extras:** `ask_user_question`, plan-mode exit, background tasks/monitors, subagents, `$skill` mentions, context usage, session titles, and mid-turn restart parity are not ported yet.
- **ACP Registry extras:** Per-agent adapter quirks (Copilot/Command Code MCP over HTTP, Devin client terminals, Claude ACP subagents/work state, Kimi prompt-outcome watch, GLM per-turn usage, Mistral Vibe extensions, Hermes post-settle continuation, Droid/omp/OpenCode elicitation forms), ACP protocol v2, URL sign-in, plan-mode session modes, custom models, pre-turn status probes, and remote-workspace installation are not ported yet.
- **Pi extras:** Extension dialogs (select/input/editor) are cancelled rather than asked; notices, extension errors, provider retries, `/compact`, service tier, `$skill` chips, subagents, steering status, rollback, workspace handoff, and non-image attachment path text are not ported yet.
- **OpenCode extras:** OpenCode 2 lacks subagents, background shells, image generation jobs, native goals, todo lists, `/compact`, native slash commands, `$skill` mentions, provider retry items, rollback/fork, external client prompts, delivered background reports, Code Mode pages, and event-stream reconnect recovery. OpenCode 1.x lacks subagents, background wakes, todo lists, `/compact` and context-overflow recovery, native slash commands, MCP tool presentation, rollback/fork, orphaned-server ledger, and agent feedback instructions.
- **Antigravity extras:** Post-settle continuation, subagents/child work, session-log error checks, 15-minute inactivity watchdog, client file-read repair, line-placed edit diffs, payload bounds, refusing broken manifest runtimes mid-turn, native `/goal`, and non-image attachments are not ported yet.
- **Kiro extras:** Sub-agents as child threads, `/compact` (`_kiro/session/compact`), `session/fork`, native turn IDs, T3 MCP tool names on `@t3-code/<tool>` calls, slash commands and pushed options as snapshots, `steering_injected` wait/follow-up, refusing steers with attachments, and leaving the turn running on Stop timeout are not ported yet.
- **fx extras:** MCP over ACP for fx 0.0.13/0.0.14 (Rust uses the stdio bridge), `subagent` calls as child threads, interrupt-and-restart steering (Rust queues), distinguishing cancelled vs. failed tool calls after Stop, and form elicitation are not ported yet.
- **Muse Code extras:** Subagents/workflows as child threads, wake turns and continuations, joined turns, `/compact`, session history reads, account usage windows (`usage/changed`), todo lists, provider retry rows, user-input questions, workflow child approvals, image steering, extra workspace roots, agent feedback instructions, and non-image attachment path text are not ported yet.
- **Codex native subagents:** Subagents get their own child threads, wake their parent on late completion, and stop when the parent is stopped, but the parent's live stream can group or order a few subagent updates differently from the original's commit batches (around a stop, and the child's model), and late child updates arrive with the wake run.
- **Claude wakes:** A background subagent that finishes while Claude is idle wakes it with a run naming the subagent (as background commands and Monitor events do); workflow wakes do not yet report what finished, and `pendingBackgroundTasks` lists only background commands and monitors.
- **Steering extras:** Steering with model changes or attachments still differs; Rust also lacks the "delivery not confirmed" notice, foreground subagent handling during a Claude steer, and follow-up turns when a provider refuses a steer.
- **Titles:** Cursor and Muse Code cannot title threads yet; custom title generators, images in OpenCode title prompts, forked-history titles, and Antigravity temp-folder cleanup / sign-in waits are missing.
- **Desktop power reports:** Rust does not receive desktop power reports (battery, screen lock, heat), so it does not pause provider checks for them.

## Testing and parity suites

### Rust tests

```bash
cargo test --locked
```

### Comparing against the TypeScript reference

Prerequisites: Git, GitHub CLI (`gh`), Node 26.13.1, pnpm 12.14.0, Python 3 with `websockets` 16 and `cryptography`, and the Rust toolchain pinned in [`rust-toolchain.toml`](rust-toolchain.toml).

```bash
git clone https://github.com/pingdotgg/t3code /tmp/t3code-reference
git -C /tmp/t3code-reference checkout <pinned commit>  # see Parity status
pnpm --dir /tmp/t3code-reference --filter t3 --filter t3code-relay --filter @t3tools/client-runtime install --frozen-lockfile
cargo build --locked

original="node /tmp/t3code-reference/apps/server/src/bin.ts"
rust="$PWD/target/debug/t3code-server"

python3 scripts/client_initial_sync_parity.py --original-command "$original" --rust-command "$rust" --reference-root /tmp/t3code-reference
python3 scripts/client_rpc_parity.py --original-command "$original" --rust-command "$rust" --reference-root /tmp/t3code-reference
python3 scripts/client_thread_parity.py --original-command "$original" --rust-command "$rust" --reference-root /tmp/t3code-reference
python3 scripts/parity_harness.py --managed --check-required-fields --original-command "$original" --rust-command "$rust"
python3 scripts/orchestration_parity.py --original-command "$original" --rust-command "$rust"
python3 scripts/config_parity.py --original-command "$original" --rust-command "$rust"
python3 scripts/config_snapshot_parity.py --original-command "$original" --rust-command "$rust" --reference-root /tmp/t3code-reference
python3 scripts/auth_parity.py --original-command "$original" --rust-command "$rust"
python3 scripts/mcp_parity.py --original-command "$original" --rust-command "$rust"
python3 scripts/git_workflow_parity.py --original-command "$original" --rust-command "$rust"
python3 scripts/github_provider_parity.py --original-command "$original" --rust-command "$rust"
python3 scripts/relay_parity.py --checkout /tmp/t3code-reference --rust "$rust"
```

Additional parity suites under `scripts/` include `preview_provider_parity.py` (see `--help`), `peer_link_parity.py`, `peer_link_outgoing_parity.py`, `peer_forwarding_parity.py`, `link_request_parity.py`, `client_sessions_parity.py`, `source_control_clone_parity.py`, `source_control_lookup_parity.py`, `project_clone_parity.py`, `project_favicon_parity.py`, `bitbucket_pull_requests_parity.py`, `bitbucket_settings_parity.py`, `bitbucket_pull_request_parity.py`, `acp_install_parity.py`, `linear_provider_parity.py`, `cors_parity.py`, `managed_policy_parity.py`, `settings_mutation_parity.py`, `settings_secrets_parity.py`, `relay_client_parity.py`, and `relay_provisioning_parity.py`. On Linux, pass `--faketime-lib /path/to/libfaketime.so.1` to `github_provider_parity.py` to include timestamped reads.

### Offline isolation

Parity runs stay off the network. `server_env()` in `scripts/parity_harness.py` disables reference telemetry (`T3CODE_TELEMETRY_ENABLED=false`; Rust sends none) and host `gh`/`az` CLI telemetry, and routes HTTP(S) through a non-forwarding loopback proxy (`NO_PROXY` exempts loopback for Node, reqwest, git, npm, and Chromium). Each suite reports any targets the proxy refused.

Local stand-ins provided to both servers:
- **Bitbucket:** Loopback API via `T3CODE_BITBUCKET_API_BASE_URL`.
- **npm releases:** Loopback registry via `npm_config_registry`.
- **ACP Registry:** Seeded `caches/acp-registry/registry.json`, used as fallback when the fixed HTTPS URL is refused.
- **Provider release lookups (`provider_status_parity.py`):** Fake `npm`/`mise` on `PATH` and a loopback HTTPS proxy (`scripts/fake_https_proxy.py`) with a test CA trusted via `NODE_EXTRA_CA_CERTS`.
- **Preview browser:** Pinned `chrome-headless-shell` linked to the Playwright headless shell so nothing downloads.

Targets that remain refused (and are handled identically by both servers): model-manifest refresh from `raw.githubusercontent.com` (both keep the bundled copy), the reference's Codex/maintenance/Muse Code update checks (`registry.npmjs.org`, `api.meta.ai`), and any host `gh`/`az` CLI requests.

### Contract and client-call inventories

- [`contracts/`](contracts/) lists the pinned upstream RPC/HTTP surface (`python3 scripts/update_contract_inventory.py /tmp/t3code-reference`).
- [`docs/client-call-inventory.md`](docs/client-call-inventory.md) lists every server call the client makes and its test coverage (`python3 scripts/update_client_call_inventory.py /tmp/t3code-reference`).

## Project layout and architecture

The workspace contains two Rust crates and a WebAssembly host:

| Path | Description |
| --- | --- |
| [`src/`](src/) (`t3code-server`) | Root crate implementing the HTTP/WebSocket server, auth, settings/policy, orchestration event log (`events.jsonl`) and projections, provider turns, MCP endpoints, and standalone relay. Builds natively (`--features native`, default) and for `wasm32-unknown-unknown` (`--no-default-features --features workers`). |
| [`agentd/`](agentd/) (`t3-agentd`) | Separate workspace process executing local files, git, terminals, provider turns, previews, and device operations over WebSocket so the control plane needs no local filesystem or process access. See [`docs/agentd.md`](docs/agentd.md). |
| [`workers/`](workers/) | Embeddable control plane (`t3code_server::control::ControlPlane`) separating control state from sockets, timers, and storage behind traits (`DocumentStore`, `EventLog`, `Runtime`, `workspace::Transport`). See [`docs/embedding.md`](docs/embedding.md). |
| [`contracts/`](contracts/) | Pinned upstream RPC, HTTP, command, and schema inventories. |
| [`docs/`](docs/) | Design notes, [agent tool checklist](docs/agent-tools.md), [performance benchmarks](docs/performance.md), and [shared database notes](docs/shared-store.md). |
| [`scripts/`](scripts/) | Parity test suites, xfail lists, and inventory generators. |

## Contributing

1. Run `cargo test --locked` and the relevant parity scripts in `scripts/` against the pinned reference checkout.
2. If your change fixes a gap listed in [`scripts/client_rpc_parity_xfail.json`](scripts/client_rpc_parity_xfail.json) or [`scripts/client_thread_parity_xfail.json`](scripts/client_thread_parity_xfail.json), remove that entry so CI enforces it going forward.
3. Keep changes focused and verify links and formatting before opening a pull request.

## About the name

<img src="docs/assets/ferrum-logo.jpg" alt="Ferrum logo: the Fe element tile, atomic number 26" width="128" align="right">

Ferrum is Latin for iron, element 26 (Fe), and rust is what iron becomes. The project was previously named `t3code-rust`; its repository now lives at [github.com/ttulttul/Ferrum](https://github.com/ttulttul/Ferrum).

## License

MIT. See [LICENSE](LICENSE).
