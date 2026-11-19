# t3code-rust

A Rust implementation of the headless T3 Code environment server and standalone relay. Compatibility work targets [upstream commit 052ca497](https://github.com/pingdotgg/t3code/tree/052ca497cf1103b001327bcd964188a71dfec213). The pinned source declares 235 WebSocket RPC methods and 86 typed HTTP operations, including 30 relay operations. The port is in development. Clients can still distinguish it from the TypeScript server.

## Run

```bash
cargo build --locked
target/debug/t3code-server --listen 127.0.0.1:3773 --data-dir .t3
```

An environment launcher can pass the desktop bootstrap envelope on an inherited file descriptor with `--bootstrap-fd`. It contains the bind address, port, T3 home directory, and desktop bootstrap credential. Exchange the credential at `POST /api/auth/browser-session` and send the resulting cookie to authenticated HTTP operations. For a separate standalone process, set `T3CODE_BOOTSTRAP_CREDENTIAL` to a fresh high-entropy credential before launching.

Connect to `/ws?orchestrationProtocol=2` with a session cookie or a WebSocket ticket. The client sends Effect RPC messages as JSON text frames. For example, the following request obtains the server probe response.

```json
{"_tag":"Request","id":"1","tag":"server.probe","payload":{},"headers":[]}
```

```json
{"_tag":"Exit","requestId":"1","exit":{"_tag":"Success","value":{}}}
```

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
python3 scripts/auth_parity.py --original-command "$original" --rust-command "$rust"
python3 scripts/mcp_parity.py --original-command "$original" --rust-command "$rust"
python3 scripts/git_workflow_parity.py --original-command "$original" --rust-command "$rust"
python3 scripts/github_provider_parity.py --original-command "$original" --rust-command "$rust"
python3 scripts/relay_parity.py --checkout /tmp/t3code-reference --rust "$rust"
```

The managed harness starts both processes on loopback with temporary homes, separate authentication sessions, and the same signing key for deterministic registration. It compares HTTP status, selected headers and raw body bytes, and compares WebSocket application frames without reserializing JSON. The GitHub suite also compares the writes each server makes to an isolated provider. The relay suite runs upstream HTTP handlers with test storage and local keys. A separate Chromium suite compares rendered frames and file transfers using the same browser installation. See `scripts/preview_provider_parity.py --help`.

Fixtures cover specific requests and state transitions. Time, random credentials, provider output and platform-specific behavior still limit comparisons. Remaining work includes orchestration projections and replay, coding-agent sessions, provider-specific integrations, guest access through a managed tunnel, relay provisioning and push delivery, MCP tools, and protocol headers and error cases outside the fixtures. The inventory in `contracts/` lists the upstream surface. Regenerate it with `python3 scripts/update_contract_inventory.py /tmp/t3code-reference`.

## License

MIT. See [LICENSE](LICENSE).
