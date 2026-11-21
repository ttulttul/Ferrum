# t3code-rust

A Rust implementation of the headless T3 Code environment server and standalone relay. Compatibility work targets [upstream commit 052ca497](https://github.com/pingdotgg/t3code/tree/052ca497cf1103b001327bcd964188a71dfec213). The pinned source declares 235 WebSocket RPC methods and 86 typed HTTP operations, including 30 relay operations. Its dispatch RPC accepts 58 command types.

## Run

```bash
cargo build --locked
target/debug/t3code-server --listen 127.0.0.1:3773 --data-dir .t3
```

An environment launcher can pass the desktop bootstrap envelope on an inherited file descriptor with `--bootstrap-fd`. It contains the bind address, port, T3 home directory, and desktop bootstrap credential. Exchange the credential at `POST /api/auth/browser-session` and send the resulting cookie to authenticated HTTP operations. For a separate standalone process, set `T3CODE_BOOTSTRAP_CREDENTIAL` to a fresh high-entropy credential before launching.

The Rust binary follows the TypeScript server's configuration layout. `T3CODE_HOME` and `--base-dir` select a base directory, with mutable files under `userdata`. This includes `settings.json`, `keybindings.json`, `themes`, `attachments`, `browser-artifacts`, `logs`, `secrets`, `environment-id`, `anonymous-id`, and `server-runtime.json`. `T3CODE_HOST`, `T3CODE_PORT`, `T3CODE_MODE`, `T3CODE_NO_BROWSER`, `T3CODE_AUTO_BOOTSTRAP_PROJECT_FROM_CWD`, `T3CODE_LOG_WS_EVENTS`, `T3CODE_DEV_ALLOWED_ORIGINS`, `T3CODE_TAILSCALE_SERVE`, and `T3CODE_TAILSCALE_SERVE_PORT` are loaded with the same environment names. `server.getSettings` reads the shared settings file and `server.updateSettings` persists sparse settings atomically through symlinks. Both RPCs return normalized settings with credentials redacted. Clients advertising `settingsSecretReferences` receive supported secret references. Legacy provider and project settings migrate from the shared home and SQLite state. Machine policy supplies defaults, locks, and provider and client restrictions.

Like the TypeScript server, the Rust server owns its state directory exclusively. At startup, before opening any database, it holds an exclusive SQLite transaction on `userdata/server-owner.sqlite` for the life of the process. Taking the lock waits up to 250 ms for a holder to let go, as the TypeScript server does. A second Rust or TypeScript server on the same T3 home therefore refuses to start, and the Rust server exits with status 78 when another server already owns it. It also refuses when `server-runtime.json` names a live pre-lock server (a record without an `ownerId` whose process started before the record was written), and it logs an error if the lock file is later deleted or replaced. Once listening it atomically publishes `userdata/server-runtime.json` in the TypeScript shape. That record holds the real bound port, the configured host when one was given, a loopback `origin` for an unset or wildcard host, and the loaded managed policy (`null` when none applies). The desktop SSH launcher needs this before it adopts a running server instead of starting another. On SIGINT or SIGTERM the record is removed if its `ownerId` is still this process's. `GET /` serves the built web client from `--static-dir` / `T3CODE_STATIC_DIR` when one is set, and otherwise answers 200 with a placeholder page.

Connect to `/ws?orchestrationProtocol=2` with a session cookie or a WebSocket ticket. The client sends Effect RPC messages as JSON text frames. A request id is a string or a number, and replies, `Ack` and `Interrupt` carry it back unchanged; the 0.1.10 clients number their requests from `0`. With `frameParts` in the socket URL, text frames over 48 KiB go out as parts the client joins, as the reference server sends them. For example, the following request obtains the server probe response.

```json
{"_tag":"Request","id":"1","tag":"server.probe","payload":{},"headers":[]}
```

```json
{"_tag":"Exit","requestId":"1","exit":{"_tag":"Success","value":{}}}
```

Workspace operations (files, git, terminals, provider turns, previews and devices) can also run in a separate `t3-agentd` process that the server reaches over a WebSocket, so the server core can run where it has no local filesystem or processes. See [`docs/agentd.md`](docs/agentd.md).

## Evaluate the Rust server in place

From this repository's root, use the switcher to replace a running standalone TypeScript server and restore it later.

```bash
./install --status
./install --rust
./install --original
```

`./install --rust` finds the live server through its `server-runtime.json`, verifies that `GET /` answers 2xx, captures the original command, environment, working directory, and T3 home in a private state directory, installs Rust with rustup when `cargo` is unavailable, builds a locked release binary, and starts the Rust server on the same host, port, and data directory, then confirms that `server-runtime.json` names the Rust process with a loopback origin. On Linux, a discovered user-systemd service is stopped, disabled, and runtime-masked for the duration of the Rust run. `./install --original` stops Rust, removes the mask, restores the service's enable state, and starts the original TypeScript service. A failed Rust start automatically restores the TypeScript process or service. Each switch stops the outgoing server with the reference's update-restart marker (`runtime/desktop-update-restart`), so it keeps its T3 Connect managed tunnel and the incoming server starts that confirmed tunnel at once rather than provisioning a new one, which takes tens of seconds to become routable. The Rust server logs at `info` unless `RUST_LOG` says otherwise; its output goes to `rust-server.log` in the state directory.

The switcher refuses to act when it finds more than one live server, a stale or incomplete runtime record, an unidentifiable background service, a desktop-managed server, a process that cannot be stopped cleanly, or a missing original executable or working directory. It supports Linux and macOS. It does not modify the repository with credentials. Saved process state lives under `${XDG_STATE_HOME:-~/.local/state}/t3code-rust-switch` and is removed after a successful restore.

Set `T3CODE_HOME` before running the command when the server uses a custom T3 home. Run `./install --status` first when evaluating an unfamiliar machine. Quit the T3 desktop app before switching its server. If the server is supervised by a service manager the installer must be able to identify and control that service as the current user.

Launch the relay separately with `target/debug/t3code-server --relay --listen 127.0.0.1:3774 --data-dir .t3`. Set `RELAY_ISSUER` to its canonical public URL. Local RS256 Clerk session JWT verification uses `CLERK_JWT_KEY` (public PEM), `CLERK_ISSUER`, and `CLERK_JWT_AUDIENCE`. Relay signing uses `CLOUD_MINT_PRIVATE_KEY` (Ed25519 PKCS#8 PEM) and `CLOUD_MINT_PUBLIC_KEY` (SPKI PEM). Relay links, mobile registrations and replay protection persist in SQLite.

## Compare against upstream

Use Git, the GitHub CLI, Node 26.13.1, pnpm 12.14.0, Python with `websockets` 16 and `cryptography`, and the Rust toolchain pinned in this repo.

```bash
git clone https://github.com/pingdotgg/t3code /tmp/t3code-reference
git -C /tmp/t3code-reference checkout 052ca497cf1103b001327bcd964188a71dfec213
pnpm --dir /tmp/t3code-reference --filter t3 --filter t3code-relay install --frozen-lockfile
cargo build --locked
cargo test --locked
original="node /tmp/t3code-reference/apps/server/src/bin.ts"
rust="$PWD/target/debug/t3code-server"
pnpm --dir /tmp/t3code-reference --filter @t3tools/client-runtime install --frozen-lockfile
python3 scripts/client_initial_sync_parity.py --original-command "$original" --rust-command "$rust" --reference-root /tmp/t3code-reference
python3 scripts/client_rpc_parity.py --original-command "$original" --rust-command "$rust" --reference-root /tmp/t3code-reference
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

The client initial sync suite runs the pinned client runtime's own RPC session and shell snapshot loader against each server. It requires the environment to finish its initial sync, then checks that each RPC a freshly connected desktop or web client issues ends the same way on both servers. It runs once on an empty server and once with a project and thread. It also compares exact frames for numeric ids, invalid ids, interrupts and frame parts. The client RPC suite connects the way the desktop renderer does (bootstrap credential exchanged for a bearer session, a websocket ticket, `Origin: t3code://app`) and runs the plan in `scripts/client_rpc_cases.json` through the client runtime: connect-time subscriptions, projects, files, threads with live shell and thread subscribers, messages, attachments, terminals, VCS, preview, settings and auth. It compares decoded results after normalizing paths, ids and timestamps, then replays read-only calls over a raw socket and compares exact frames. Known Rust gaps are listed with a reason in `scripts/client_rpc_parity_xfail.json`; the suite fails on any other mismatch and on a listed case that now passes. [`docs/client-call-inventory.md`](docs/client-call-inventory.md) lists every server call the client makes and which suite covers it; regenerate it with `python3 scripts/update_client_call_inventory.py /tmp/t3code-reference`. The managed harness starts both processes on loopback with temporary homes, separate authentication sessions, and the same signing key for deterministic registration. It compares HTTP status, selected headers and raw body bytes, and compares WebSocket application frames without reserializing JSON. The GitHub suite also compares the writes each server makes to an isolated provider. The relay suite runs upstream HTTP handlers with test storage and local keys. The orchestration suite restarts both servers and checks opaque thread IDs, conflict responses and command replay. On Linux, pass `--faketime-lib /path/to/libfaketime.so.1` to the GitHub suite to include timestamped reads. A separate Chromium suite compares rendered frames and file transfers using the same browser installation. See `scripts/preview_provider_parity.py --help`.

The settings suite compares complete RPC frames, sparse settings files and secret sidecars. It exercises schema errors, redaction, credential migration, managed writes, recovery, and restarts. The config snapshot suite compares deterministic fields and complete settings change events. CI also checks the native decoder against inputs evaluated by the pinned Effect schemas.

Fixtures cover specific requests and state transitions. Time, random credentials, provider output and platform-specific behavior require normalization in comparisons. The inventory in `contracts/` lists the upstream surface. Regenerate it with `python3 scripts/update_contract_inventory.py /tmp/t3code-reference`.

## License

MIT. See [LICENSE](LICENSE).
