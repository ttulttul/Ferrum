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
- **Environment links (incoming):** Accepting environment links and pairing tokens issued by the TypeScript server on the same home directory, plus pairing-link administration.
- **Environment links (outgoing):** Linking to another machine, listing and forgetting links (shared with the TypeScript server on the same home), forwarding MCP tool calls to a linked machine, and agents asking you to link one from inside a conversation (`t3_environment_link`), answered in a card.
- **MCP server:** Serving MCP sign-in and thread/project tools over `/mcp`.
- **Login-shell `PATH` discovery:** Reading the login shell's `PATH` at startup and again before source-control discovery, so provider CLIs and tools installed later are found; listing Codex models from its CLI.
- **Provider status checks:** Claude Code, Codex, fx, Kiro, Grok, Pi and Cursor status (installed version, sign-in state, usage and models) checked at startup, on settings changes, every 5 minutes and on refresh, and pushed to clients.
- **Configuration, auth, and desktop adoption:** Shared `userdata/` layout (`settings.json`, secrets, managed policy, `server-owner.sqlite` exclusive lock, `server-runtime.json`), browser session cookies, WebSocket tickets with DPoP checks, CORS, and desktop SSH launcher adoption.
- **Git, GitHub, and workspace operations:** File operations, terminals (PTY), git workflows, GitHub/Linear provider RPCs, and Chromium preview support.
- **Split control/workspace architecture:** Out-of-process workspace execution via `t3-agentd` and an embeddable, `wasm32-unknown-unknown`-compatible control plane (`workers/`).
- **Parity test suites:** Automated suites comparing HTTP, WebSocket RPC, client initial sync, live provider turns, settings, auth, MCP, git/GitHub, previews, and relay behavior directly against the pinned TypeScript reference.

## What's still missing compared with the original

The Rust server can already run Codex and Claude Code conversations, manage projects, and share a data folder with the TypeScript original. That said, several features are still missing or behave differently.

### Threads and history

- **Switching between the Rust and TypeScript servers:** Threads and projects created on either server show up on the other, and you can keep working in them: rename, archive or delete them, start new turns, and a turn cut off by a server switch is picked up by the other server. Threads from an earlier Rust-only setup are copied into the shared database once, the first time the Rust server starts with it, and its old event log is kept.
- **Live update order:** As an agent streams a reply, live updates arrive in a different order and grouping than on the TypeScript server. Most conversations still read normally, though some edge cases (like interrupting a turn while waiting for approval) can look slightly different.

### Agents and tools

- **Few built-in T3 tools for agents:** An agent running on the Rust server can use only a small set of T3's built-in tools (for threads, projects, and basic environment info) and does not receive the instructions that describe the rest. Most built-in T3 actions—such as browser previews, scheduled tasks, and pull request helpers—are not available to agents yet.
- **Remote workspace credentials:** When workspaces run in a separate remote process (`t3-agentd`), agents are not yet given credentials to call back into T3's built-in tools.

### AI providers

- **Missing provider status checks (in progress):** OpenCode, Antigravity, and Muse are not checked yet, so they still show as "not checked" in settings. (Claude Code, Codex, fx, Kiro, Grok, Pi, and Cursor are checked.)
- **No update-available checks:** The server does not check online for newer versions of provider command-line tools, so settings always show the latest version as unknown and do not offer an update command.
- **Claude skills and reset credits:** Installed Claude skills and plugins are not listed yet, and Claude reset credits are not shown.
- **Background check timing:** Periodic provider checks always run on their timer instead of pausing when nobody is actively using the app.

### Connecting machines

- **Connected-clients list:** Sessions started on the TypeScript server are not shown in the connected-clients list yet.

### Other

- **Uploaded attachments:** Download links for uploaded attachments return "not found", so attached files and images do not load in the composer or conversation history.
- **File browsing and search:** Project file lists include `.git` and gitignored files. File and content search do not yet support fuzzy matching, regular expressions, or match highlighting.
- **Storage cleanup report:** The storage cleanup report in settings fails to load.

### Details for developers

- **Shared database:** See [`docs/shared-store.md`](docs/shared-store.md) for the `statev2.sqlite` design and remaining migration/outbox work.
- **Expected-failure lists:** Client-visible RPC and live-turn mismatches are tracked in [`scripts/client_rpc_parity_xfail.json`](scripts/client_rpc_parity_xfail.json) and [`scripts/client_thread_parity_xfail.json`](scripts/client_thread_parity_xfail.json) (including `server.getStorageCleanupReport`, and `projects.listEntries` / `searchEntries` / `searchContents`).
- **Agent tools:** Rust currently exposes 11 of the reference's 79 MCP tools and does not inject the orchestration prompt context.

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

Additional parity suites under `scripts/` include `preview_provider_parity.py` (Chromium preview frames and file transfers; see `--help`), `peer_link_parity.py`, `peer_link_outgoing_parity.py`, `peer_forwarding_parity.py`, `link_request_parity.py`, `linear_provider_parity.py`, `cors_parity.py`, `managed_policy_parity.py`, `settings_mutation_parity.py`, `settings_secrets_parity.py`, `relay_client_parity.py`, and `relay_provisioning_parity.py`. On Linux, pass `--faketime-lib /path/to/libfaketime.so.1` to `github_provider_parity.py` to include timestamped reads.

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
