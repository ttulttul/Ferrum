# t3-agentd

`t3-agentd` runs workspace operations (files, git, terminals, provider turns,
previews and devices) for a T3 Code server that reaches it over a WebSocket.
The server core can then run where it has no local filesystem or processes;
`RemoteWorkspace` stands in for `LocalWorkspace` and forwards every
`Workspace` call.

## Running it

```sh
cargo build --release -p t3-agentd
T3_AGENTD_TOKEN=$(openssl rand -hex 32) \
  target/release/t3-agentd --listen 127.0.0.1:7790 --root ~/projects
```

| Flag / variable | Default | Meaning |
| --- | --- | --- |
| `--listen`, `T3_AGENTD_LISTEN` | `127.0.0.1:7790` | Address to accept the server's WebSocket on (path `/`). |
| `T3_AGENTD_TOKEN` or `T3_AGENTD_TOKEN_FILE` | required | Bearer token the server must present. |
| `--root`, `T3_AGENTD_ROOT` | current directory | Where projects live; reported in `hello`. Operations may not reach outside it. |
| `--state-dir`, `T3_AGENTD_STATE_DIR` | `~/.t3/agentd` | Settings, terminal logs, device state, stream buffers. |
| `--buffer-limit`, `T3_AGENTD_BUFFER_LIMIT` | 64 MiB | Unacknowledged bytes one stream may hold. |
| `--prepare-dir-command`, `T3_AGENTD_PREPARE_DIR_COMMAND` | unset | Program run once per directory under `--root` before it is first used as a working directory. |
| `--prepare-dir-timeout-secs`, `T3_AGENTD_PREPARE_DIR_TIMEOUT_SECS` | `300` | Seconds to wait for `--prepare-dir-command` before killing it and continuing. |

agentd refuses, with error code `outside_root`, any operation that names a
path or working directory outside `--root` or the worktrees directory under
the state directory. That covers file operations, git, terminals, provider
turns and every other op. Paths are resolved with symlinks followed, so
`..` and links that point out of the root are refused too; a path that does
not exist yet is judged by its nearest existing ancestor. This limits what
requests can name; it does not sandbox the programs that turns and terminals
start, which can reach whatever the agentd user can.

When `--prepare-dir-command <PROGRAM>` is set, before agentd first uses a
directory under `--root` as a working directory (for a provider turn,
terminal, process, filesystem or VCS request), it runs
`<PROGRAM> <absolute-directory>` with `T3_PREPARE_DIR` also set to
`<absolute-directory>` and waits for it to finish. The directory does not
need to exist yet, so the program can create or restore it on demand. Each
directory is prepared at most once per agentd process, and concurrent
requests for the same directory wait on the single running invocation. If the
program fails to start, exits non-zero or exceeds
`--prepare-dir-timeout-secs`, agentd logs a warning with the tail of its
stderr and lets the request proceed as it would have without the hook. When
`--prepare-dir-command` is not set, no hook runs.

Settings that workspace services read (device support, tokens for device
providers) come from `settings.json` in the state directory.

## Connecting a server

Natively:

```rust
let remote = RemoteWorkspace::new(epoch, None);
let link = t3code_server::workspace::connect(remote.clone(), "ws://127.0.0.1:7790/".into(), token);
```

`epoch` must rise each time a server adopts the agentd (for example, a
counter the server persists); agentd refuses requests from a lower epoch.

Elsewhere, a host implements `Transport::send(frame)` and:

1. calls `remote.connected(transport).await` once its socket is open;
2. passes each received message to `remote.receive(frame)`, then calls
   `remote.flush_acks()`;
3. calls `remote.maintain().await` every few hundred milliseconds;
4. calls `remote.disconnected()` when the socket closes, then reconnects.

The token travels in the upgrade's `Authorization: Bearer` header, or in
`hello` when `RemoteWorkspace::new` is given it.

## Acknowledging what the server has recorded

Turns started with `Workspace::start_tracked_turn` acknowledge nothing on
their own. Each `TurnItem` carries its `StreamPosition` (`stream`, `seq`);
call `Workspace::ack(position)` once the event is recorded durably (the
native server does this right after committing it to its event log). Until
then agentd keeps the event and sends it again after a reconnect.

After a server restart, pass the last position it recorded to
`RemoteWorkspace::resume_turn(position, cancel)` before connecting. The
events after it arrive again, each once; a stream agentd no longer holds
reports an interrupted exit. `RemoteWorkspace::acked_positions()` lists the
last acknowledged position of every tracked stream still open.

Other streams (terminal, preview and device events, watches) are
acknowledged as they are delivered.

## Protocol

See `src/workspace_protocol.rs` for the frames, op names and error codes.
In short:

- Requests are `{id, op, args}`; replies are `{id, ok}` or
  `{id, err: {code, message, detail?}}`. `rpc` errors carry the exact RPC
  error value `LocalWorkspace` returns.
- Streams (`turn.start`, `term.events`, `term.metadata`, `preview.events`,
  `device.events`, `clone.events`, `git.stackedAction`, `watch.vcsStatus`,
  `watch.vcsBranch`, `watch.localServers`) answer `{stream}` and push
  `{stream, seq, event}`, ending with `{stream, seq, end: {reason}}`.
- agentd keeps unacknowledged frames per stream on disk. After a reconnect
  the server sends `hello`, then `resume {streams: {id: lastSeq}}`; agentd
  replays the rest and closes streams the server did not name. Work keeps
  running while no server is connected. A full buffer ends the stream with
  reason `gap`; a turn then stops and reports an interrupted exit.
- Watches poll every 2 s inside agentd and push only changes.
- Clones that become projects (`projectClone.*`) run in agentd, which
  tracks them: `clone.call` claims a destination, then launches, cancels,
  retries or discards a clone, and `clone.events` sends every clone's
  snapshot after each change. The server creates the project between the
  claim and the launch. A `~` in a clone's destination is read as agentd's
  `$HOME` before the path is checked against the roots. A server keeps
  working with an agentd that predates these ops: it stops asking for
  `clone.events`, and `projectClone.start` reports that the workspace
  cannot clone yet.
- `idle.state` reports running turns, terminals and processes (running
  clones count as processes), open
  streams and unacknowledged frames, for a supervisor deciding when the
  workspace is idle.
