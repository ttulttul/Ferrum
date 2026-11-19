# t3code-rust

A headless Rust orchestration server for [T3 Code](https://github.com/pingdotgg/t3code). It provides durable project, thread, message, and run state, streams provider subprocess output over WebSocket, and exposes a small HTTP API suitable for web, desktop, and remote clients.

## Run

```bash
cargo run -- --listen 127.0.0.1:3773
```

The default provider is `printf`, which echoes a prompt for local protocol testing. Configure a provider executable with `T3_PROVIDER_COMMAND`, for example `T3_PROVIDER_COMMAND="claude --print"` or `--provider-command codex exec`.

State is stored in `.t3/events.jsonl` by default. Set `T3CODE_DATA_DIR` to use another directory.

## HTTP API

- `GET /health`
- `GET /v1/projects`
- `POST /v1/projects` with `{ "name": "demo", "path": "/workspace/demo" }`
- `POST /v1/projects/:project_id/threads` with `{ "project_id": "...", "title": "...", "cwd": "..." }`
- `GET /v1/threads/:thread_id`
- `POST /v1/threads/:thread_id/messages` with `{ "content": "...", "provider": "default" }`
- `POST /v1/runs/:run_id/cancel`

## WebSocket protocol

Connect to `/ws`, send `{"type":"subscribe"}`, then send `send_message`, `create_thread`, or `cancel_run` commands. Server events are JSON objects with `type: "event"`, an event id, and an event payload. The event log can be replayed on restart to reconstruct state.

## Development

```bash
cargo test
cargo clippy --all-targets --all-features
```

## Original-server parity

The parity harness runs both servers against the same fixture set and compares
HTTP status codes and response bodies byte-for-byte. Point it at an already
running original server, or provide launch commands for both implementations.

```bash
python3 scripts/parity_harness.py \
  --original-command 'pnpm --dir /path/to/t3code dev:server --port 3773' \
  --rust-command 'cargo run -- --listen 127.0.0.1:43773'
```

Add deterministic requests to `tests/parity-fixtures.json`. The initial fixtures
cover the exact protocol-negotiation failure returned by both servers. Requests involving
generated IDs or timestamps should be tested only after the corresponding
protocol contract is made deterministic, since this harness intentionally does
not normalize output before comparing it.
