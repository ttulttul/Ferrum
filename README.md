# t3code-rust

A Rust implementation of the headless T3 Code environment server and standalone relay. Compatibility work targets [upstream commit 052ca497](https://github.com/pingdotgg/t3code/tree/052ca497cf1103b001327bcd964188a71dfec213). The pinned source declares 235 WebSocket RPC methods and 86 typed HTTP operations, including 30 relay operations. Its dispatch RPC accepts 58 command types.

## Run

```bash
cargo build --locked
target/debug/t3code-server --listen 127.0.0.1:3773 --data-dir .t3
```

An environment launcher can pass the desktop bootstrap envelope on an inherited file descriptor with `--bootstrap-fd`. It contains the bind address, port, T3 home directory, and desktop bootstrap credential. Exchange the credential at `POST /api/auth/browser-session` and send the resulting cookie to authenticated HTTP operations. For a separate standalone process, set `T3CODE_BOOTSTRAP_CREDENTIAL` to a fresh high-entropy credential before launching.

The Rust binary follows the TypeScript server's configuration layout. `T3CODE_HOME` and `--base-dir` select a base directory, with mutable files under `userdata`. This includes `settings.json`, `keybindings.json`, `themes`, `attachments`, `browser-artifacts`, `logs`, `secrets`, `environment-id`, `anonymous-id`, and `server-runtime.json`. `T3CODE_HOST`, `T3CODE_PORT`, `T3CODE_MODE`, `T3CODE_NO_BROWSER`, `T3CODE_AUTO_BOOTSTRAP_PROJECT_FROM_CWD`, `T3CODE_LOG_WS_EVENTS`, `T3CODE_DEV_ALLOWED_ORIGINS`, `T3CODE_TAILSCALE_SERVE`, and `T3CODE_TAILSCALE_SERVE_PORT` are loaded with the same environment names. `server.getSettings` reads the shared settings file and `server.updateSettings` persists sparse settings atomically through symlinks. Both RPCs return normalized settings with credentials redacted. Clients advertising `settingsSecretReferences` receive supported secret references. Legacy provider and project settings migrate from the shared home and SQLite state. Machine policy supplies defaults, locks, and provider and client restrictions.

Connect to `/ws?orchestrationProtocol=2` with a session cookie or a WebSocket ticket. The client sends Effect RPC messages as JSON text frames. For example, the following request obtains the server probe response.

```json
{"_tag":"Request","id":"1","tag":"server.probe","payload":{},"headers":[]}
```

```json
{"_tag":"Exit","requestId":"1","exit":{"_tag":"Success","value":{}}}
```

## Evaluate the Rust server in place

From this repository's root, use the switcher to replace a running standalone TypeScript server and restore it later.

```bash
./install --status
./install --rust
./install --original
```

`./install --rust` finds the live server through its `server-runtime.json`, verifies the health endpoint, captures the original command, environment, working directory, and T3 home in a private state directory, installs Rust with rustup when `cargo` is unavailable, builds a locked release binary, stops the TypeScript process, and starts the Rust server on the same host, port, and data directory. `./install --original` reverses the operation with the captured process details. A failed Rust start automatically restores the TypeScript process.

The switcher refuses to act when it finds more than one live server, a stale or incomplete runtime record, a background-service-owned server, a desktop-managed server, a process that cannot be stopped cleanly, or a missing original executable or working directory. It supports Linux and macOS. It does not modify the repository with credentials. Saved process state lives under `${XDG_STATE_HOME:-~/.local/state}/t3code-rust-switch` and is removed after a successful restore.

Set `T3CODE_HOME` before running the command when the server uses a custom T3 home. Run `./install --status` first when evaluating an unfamiliar machine. Quit the T3 desktop app and stop its background service before switching its server, because those supervisors intentionally restart their managed TypeScript process.

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

The managed harness starts both processes on loopback with temporary homes, separate authentication sessions, and the same signing key for deterministic registration. It compares HTTP status, selected headers and raw body bytes, and compares WebSocket application frames without reserializing JSON. The GitHub suite also compares the writes each server makes to an isolated provider. The relay suite runs upstream HTTP handlers with test storage and local keys. The orchestration suite restarts both servers and checks opaque thread IDs, conflict responses and command replay. On Linux, pass `--faketime-lib /path/to/libfaketime.so.1` to the GitHub suite to include timestamped reads. A separate Chromium suite compares rendered frames and file transfers using the same browser installation. See `scripts/preview_provider_parity.py --help`.

The settings suite compares complete RPC frames, sparse settings files and secret sidecars. It exercises schema errors, redaction, credential migration, managed writes, recovery, and restarts. The config snapshot suite compares deterministic fields and complete settings change events. CI also checks the native decoder against inputs evaluated by the pinned Effect schemas.

Fixtures cover specific requests and state transitions. Time, random credentials, provider output and platform-specific behavior require normalization in comparisons. The inventory in `contracts/` lists the upstream surface. Regenerate it with `python3 scripts/update_contract_inventory.py /tmp/t3code-reference`.

## License

MIT. See [LICENSE](LICENSE).
