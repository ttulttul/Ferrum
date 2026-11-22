# t3code-rust

> [!WARNING]
> **Alpha-quality software under heavy development.** Do **not** use this for anything important. Expect breaking changes, incomplete features, and data loss.

**No affiliation:** `t3code-rust` is an unofficial, independent Rust port of the headless [T3 Code](https://github.com/pingdotgg/t3code) server (`t3 serve`) and standalone relay. This project has **no affiliation with T3 Code or T3 Tools Inc., and is not endorsed by them in any way**. T3 Code is a product of T3 Tools Inc.

---

## Overview

`t3code-rust` reimplements the TypeScript T3 Code environment server and standalone relay in Rust. Compatibility work targets [upstream commit `052ca497`](https://github.com/pingdotgg/t3code/tree/052ca497cf1103b001327bcd964188a71dfec213), which declares 235 WebSocket RPC methods, 86 typed HTTP operations (including 30 relay operations), and 58 orchestration dispatch command types.

## What works now

Based on `main` and the parity test suites:

- **Codex and Claude Code agent turns:** Launching Codex and Claude Code sessions, streaming responses, tool calls, approvals (allow, allow for session, decline, `approval-required`, `auto-accept-edits`), and user-input / question requests.
- **Checkpoints, interrupts, and token usage:** Reference-compatible checkpoint naming, turn interrupts (including stopping spawned child processes), token usage reporting, model reporting, and usage-limit reset times.
- **Restart recovery:** Resuming and continuing interrupted turns, retries, rewinds, and background work after a server restart.
- **T3 Connect relay tunnel:** Provisioning, running, and releasing the T3 Connect managed tunnel, plus handing off an active tunnel cleanly across Rust/TypeScript switches.
- **Environment links (incoming):** Accepting environment links and pairing tokens issued by the TypeScript server on the same home directory, plus pairing-link administration. Sessions the TypeScript server signed in on the same home are listed under Connections and can be revoked there.
- **Environment links (outgoing):** Linking to another machine, listing and forgetting links (shared with the TypeScript server on the same home), forwarding MCP tool calls to a linked machine, and agents asking you to link one from inside a conversation (`t3_environment_link`), answered in a card.
- **MCP server:** Serving MCP sign-in and thread/project tools over `/mcp`.
- **Login-shell `PATH` discovery:** Reading the login shell's `PATH` at startup and again before source-control discovery, so provider CLIs and tools installed later are found; listing Codex models from its CLI.
- **Provider status checks:** Every built-in provider (Claude Code, Codex, Cursor, OpenCode, Grok, Pi, fx, Kiro, Antigravity and Muse Code) has its status (installed version, sign-in state, usage and models) checked at startup, on settings changes, every 5 minutes and on refresh, and pushed to clients.
- **Configuration, auth, and desktop adoption:** Shared `userdata/` layout (`settings.json`, secrets, managed policy, `server-owner.sqlite` exclusive lock, `server-runtime.json`), browser session cookies, WebSocket tickets with DPoP checks, CORS, and desktop SSH launcher adoption.
- **Git, GitHub, and workspace operations:** File operations, terminals (PTY), git workflows, GitHub/Linear provider RPCs, listing Bitbucket pull requests with the credential from Settings or the `T3CODE_BITBUCKET_*` environment variables (opening one, its checks and its actions aren't ported yet), and Chromium preview support.
- **Split control/workspace architecture:** Out-of-process workspace execution via `t3-agentd` and an embeddable, `wasm32-unknown-unknown`-compatible control plane (`workers/`). Remote Codex and Claude turns can call the host's MCP endpoint as their thread. See [host configuration](docs/embedding.md#settings-and-provider-turns).
- **Parity test suites:** Automated suites comparing HTTP, WebSocket RPC, client initial sync, live provider turns, settings, auth, MCP, git/GitHub, previews, and relay behavior directly against the pinned TypeScript reference.

## What's still missing compared with the original

The [client RPC parity suite](scripts/client_rpc_parity_xfail.json) has no expected failures. Differences still show up during live conversations and in the tools agents can use.

### Threads and history

- **Live updates.** Rust can show the start of a turn or the answer to an approval request in a different order. Interrupting Codex while it waits for approval can leave the turn without a checkpoint. A Claude question can appear after a command that the original shows after the question.
- **Restarts and server switches.** A queued Codex message can appear earlier in the history after a restart. If a resumed Claude turn fails, its retry can be missing context from the failed attempt. Steering and then stopping a Codex turn after switching servers can also change the order of live updates. See the [live-turn cases](scripts/client_thread_parity_xfail.json) and [server-switch cases](scripts/shared_store_parity_xfail.json).
- **Older databases.** Both servers can use the same current database. If a home has only the older TypeScript database, start the matching TypeScript server once to upgrade it. See the [shared database notes](docs/shared-store.md).

### Agents and tools

- **Delegated tasks.** An agent can start a child, check its progress and receive its result. Cancelling the task, sending a follow-up to a finished child, and running a child in a separate worktree or on a linked machine are still missing.
- **Other agent tools.** Agents still lack tools for requesting secrets, sending attachments, working with pull requests and issues, and controlling browser previews and devices. They also lack the instructions the original gives agents about when to use T3 tools. The [agent tool checklist](docs/agent-tools.md#checklist) tracks each tool.
- **Scheduled runs.** Agents can manage schedules and run a task on demand. Rust does not yet start a task automatically when it comes due.

### AI providers

- **Follow-up messages.** Rust starts a new Claude process and resumes the conversation for each turn. It also starts a Codex process per turn, while the original shares one across threads using the same provider instance.
- **Steering.** You can redirect a running Codex turn with a text message. Claude queues the message for later. Changing the model or including an attachment in a steering message still behaves differently.
- **Titles and workflows.** Codex and Claude can generate thread titles. Custom title generators and titles that draw on the source thread’s history after a fork are still missing. Claude workflow tasks are also unsupported.
- **ACP Registry agents.** Rust searches the ACP Registry, installs its agents (downloaded binaries, and npm or uv packages) and removes managed binaries. It cannot run turns with ACP agents yet.
- **Provider settings.** Rust checks all built-in providers, including whether a newer CLI release is out. Claude skill and plugin listings and Claude reset credits are still missing. Periodic checks can continue when nobody is using the app.

## Install and switching

From the repository root, use `./install` to temporarily replace a running standalone TypeScript server with the Rust build and restore it later:

```bash
./install --status     # Show the discovered server and switch state
./install --rust       # Build and start the Rust server in place
./install --original   # Stop Rust and restore the TypeScript server
```

How `./install --rust` works:
- Discovers the live server via `server-runtime.json`, verifies `GET /` answers 2xx, and saves the original command, environment, working directory, and T3 home under `${XDG_STATE_HOME:-~/.local/state}/t3code-rust-switch`.
- Installs Rust via `rustup` if `cargo` is missing, builds a locked release binary, and starts `t3code-server` on the same host, port, and data directory.
- On Linux, stops, disables, and runtime-masks a discovered user `systemd` service while Rust is running; `./install --original` unmasks and restores it.
- Automatically restores the TypeScript server if the Rust server fails to start.
- Uses the reference update-restart marker (`runtime/desktop-update-restart`) on each switch so the T3 Connect managed tunnel is handed over immediately instead of reprovisioning.
- Logs at `info` by default (override with `RUST_LOG`) to `rust-server.log` in the switch state directory.

Notes and safety checks:
- Supports Linux and macOS.
- Run `./install --status` first on an unfamiliar machine, and quit the T3 desktop app before switching its server.
- Set `T3CODE_HOME` before running `./install` if your server uses a custom T3 home.
- Refuses to act if it finds multiple live servers, a stale or incomplete runtime record, an unidentifiable background service, a desktop-managed server, or a missing original binary/working directory.

## Build and run from source

```bash
cargo build --locked
target/debug/t3code-server --listen 127.0.0.1:3773 --data-dir .t3
```

### Bootstrap and configuration

- **Desktop bootstrap:** An environment launcher can pass the desktop bootstrap envelope on an inherited file descriptor with `--bootstrap-fd` (bind address, port, T3 home, and bootstrap credential). Exchange the credential at `POST /api/auth/browser-session` and send the resulting cookie to authenticated HTTP endpoints. For a standalone process, set `T3CODE_BOOTSTRAP_CREDENTIAL` to a high-entropy secret.
- **Directory layout and environment variables:** `T3CODE_HOME` and `--base-dir` select the base directory, with mutable files under `userdata/` (`settings.json`, `keybindings.json`, `themes`, `attachments`, `browser-artifacts`, `logs`, `secrets`, `environment-id`, `anonymous-id`, `server-runtime.json`). Standard variables (`T3CODE_HOST`, `T3CODE_PORT`, `T3CODE_MODE`, `T3CODE_NO_BROWSER`, `T3CODE_AUTO_BOOTSTRAP_PROJECT_FROM_CWD`, `T3CODE_LOG_WS_EVENTS`, `T3CODE_DEV_ALLOWED_ORIGINS`, `T3CODE_TAILSCALE_SERVE`, `T3CODE_TAILSCALE_SERVE_PORT`, `T3CODE_STATIC_DIR`) match the TypeScript server.
- **Exclusive ownership:** At startup, the server acquires an exclusive SQLite lock on `userdata/server-owner.sqlite` (waiting up to 250 ms) and exits with status `78` if another Rust or TypeScript server already owns the home directory. Once listening, it atomically writes `userdata/server-runtime.json` and removes it on `SIGINT`/`SIGTERM`.
- **Static client:** `GET /` serves the built web client from `--static-dir` / `T3CODE_STATIC_DIR` when set, or a placeholder page otherwise.

### WebSocket RPC

Connect to `/ws?orchestrationProtocol=2` with a session cookie or WebSocket ticket. Clients send Effect RPC messages as JSON text frames (string or numeric request IDs; add `frameParts` to the URL for >48 KiB frame chunking):

```json
{"_tag":"Request","id":"1","tag":"server.probe","payload":{},"headers":[]}
```

```json
{"_tag":"Exit","requestId":"1","exit":{"_tag":"Success","value":{}}}
```

### Standalone relay

Run the relay separately with `--relay`:

```bash
target/debug/t3code-server --relay --listen 127.0.0.1:3774 --data-dir .t3
```

Configure `RELAY_ISSUER` (canonical public URL), `CLERK_JWT_KEY` (RS256 public PEM), `CLERK_ISSUER`, `CLERK_JWT_AUDIENCE`, `CLOUD_MINT_PRIVATE_KEY` (Ed25519 PKCS#8 PEM), and `CLOUD_MINT_PUBLIC_KEY` (SPKI PEM). Relay links, mobile registrations, and replay protection persist in SQLite.

## Testing and parity suites

Run the Rust unit and integration tests:

```bash
cargo test --locked
```

### Comparing against the TypeScript reference

Prerequisites: Git, GitHub CLI (`gh`), Node 26.13.1, pnpm 12.14.0, Python 3 with `websockets` 16 and `cryptography`, and the Rust toolchain pinned in [`rust-toolchain.toml`](rust-toolchain.toml).

```bash
git clone https://github.com/pingdotgg/t3code /tmp/t3code-reference
git -C /tmp/t3code-reference checkout 052ca497cf1103b001327bcd964188a71dfec213
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

Parity runs stay off the network. `server_env()` in `scripts/parity_harness.py` turns the reference's telemetry off (`T3CODE_TELEMETRY_ENABLED=false`; the Rust server sends none), and turns off telemetry for the host's `gh` and `az` CLIs. It also sends every server's HTTP(S) through a loopback proxy that forwards nothing; `NO_PROXY` exempts loopback, for Node, reqwest, git, npm and Chromium alike. Each suite's output names any targets the proxy refused. Suites give both servers local stand-ins for what they would fetch:

- Bitbucket: a loopback API through `T3CODE_BITBUCKET_API_BASE_URL`.
- npm releases: a loopback registry through `npm_config_registry`, read by the npm CLI.
- The ACP Registry: a seeded `caches/acp-registry/registry.json`. Both servers fall back to it when the registry's fixed HTTPS URL is refused.
- Provider release lookups in `provider_status_parity.py`: fake `npm`, `mise` and the like on `PATH`, and a loopback HTTPS proxy (`scripts/fake_https_proxy.py`) with a test CA both servers trust through `NODE_EXTRA_CA_CERTS`. It answers registry.npmjs.org and the Codex, Claude and Muse Code release channels, and forwards nothing.
- The preview browser: the reference's pinned `chrome-headless-shell` folder, linked to the Playwright headless shell the Rust server runs, so nothing is downloaded.

Some targets are still refused. Each has no setting that points it elsewhere, and both servers handle the failure the same way:

- The model-manifest refresh from raw.githubusercontent.com. Both servers then keep the bundled copy, which is the same file.
- The reference's Codex, maintenance and Muse Code update checks: registry.npmjs.org and api.meta.ai.
- Anything the host's `gh` and `az` CLIs try.

Additional parity suites under `scripts/` include `preview_provider_parity.py` (Chromium preview frames and file transfers; see `--help`), `peer_link_parity.py`, `peer_link_outgoing_parity.py`, `peer_forwarding_parity.py`, `link_request_parity.py`, `client_sessions_parity.py`, `source_control_clone_parity.py`, `source_control_lookup_parity.py`, `project_clone_parity.py`, `project_favicon_parity.py`, `bitbucket_pull_requests_parity.py`, `bitbucket_settings_parity.py`, `acp_install_parity.py`, `linear_provider_parity.py`, `cors_parity.py`, `managed_policy_parity.py`, `settings_mutation_parity.py`, `settings_secrets_parity.py`, `relay_client_parity.py`, and `relay_provisioning_parity.py`. On Linux, pass `--faketime-lib /path/to/libfaketime.so.1` to `github_provider_parity.py` to include timestamped reads.

Known gaps in `client_rpc_parity.py` and `client_thread_parity.py` are tracked in [`scripts/client_rpc_parity_xfail.json`](scripts/client_rpc_parity_xfail.json) and [`scripts/client_thread_parity_xfail.json`](scripts/client_thread_parity_xfail.json); both suites fail on any unlisted mismatch and on any listed case that starts passing.

Contract and client-call inventories:
- [`contracts/`](contracts/) lists the pinned upstream RPC/HTTP surface (`python3 scripts/update_contract_inventory.py /tmp/t3code-reference`).
- [`docs/client-call-inventory.md`](docs/client-call-inventory.md) lists every server call the client makes and its test coverage (`python3 scripts/update_client_call_inventory.py /tmp/t3code-reference`).

## Architecture overview

The workspace consists of two crates and a WebAssembly host:

- **`t3code-server` (root crate):** Implements the HTTP/WebSocket server, auth, settings/policy, orchestration event log (`events.jsonl`) and projections, provider turn execution, MCP endpoints, and standalone relay. Builds natively (`--features native`, default) and for `wasm32-unknown-unknown` (`--no-default-features --features workers`).
- **`t3-agentd` ([`agentd/`](agentd/)):** Separate workspace process that executes local files, git, terminals, provider turns, previews, and device operations over a WebSocket, allowing the control plane to run without local filesystem or process access. See [`docs/agentd.md`](docs/agentd.md).
- **Embeddable control plane ([`workers/`](workers/)):** `t3code_server::control::ControlPlane` separates control state from sockets, timers, and storage behind traits (`DocumentStore`, `EventLog`, `Runtime`, `workspace::Transport`). See [`docs/embedding.md`](docs/embedding.md).

## Contributing

1. Run `cargo test --locked` and the relevant parity scripts in `scripts/` against the pinned reference checkout.
2. If your change fixes a gap listed in `scripts/client_rpc_parity_xfail.json` or `scripts/client_thread_parity_xfail.json`, remove that entry so the suite enforces it going forward.
3. Keep changes focused and verify links and formatting before opening a pull request.

## License

MIT. See [LICENSE](LICENSE).
