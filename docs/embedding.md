# Embedding the control plane

`t3code-server` can run as a library inside another program. The
message-driven control plane (`t3code_server::control`) holds the auth
records, the orchestration event log and its projections, and the RPC
dispatch. It runs provider turns and workspace methods in a workspace
process, `t3-agentd`, which it reaches through `RemoteWorkspace`. It owns no
sockets, timers or files. The program that embeds it, the *host*, supplies
those:

| The host supplies | Through |
| --- | --- |
| Durable storage for small documents | `persistence::DocumentStore` |
| Durable storage for the event log | `event_log::EventLog` |
| Background tasks and one alarm | `runtime::Runtime` |
| A connection to `t3-agentd` | `workspace::Transport`, plus calls into the plane |
| Client HTTP requests and WebSockets | `ControlPlane::router`, `authorize_socket`, `on_message` |
| A clock and randomness, where `std` has none | `platform::install` |

The crate builds natively (feature `native`, the default) and for
`wasm32-unknown-unknown` (`--no-default-features --features workers`). Under
`workers`, the futures and trait objects below need not be `Send`, and the
plane expects to run on one thread. `workers/` is a complete WebAssembly host
except for the `t3-agentd` connection, and `agentd/tests/plane.rs` is a
complete native one.

## Opening the plane

```rust
pub fn ControlPlane::open(
    config: control::Config,
    documents: Arc<dyn DocumentStore>,
    log: Box<dyn EventLog + Send>,
    host: control::Host,
) -> anyhow::Result<Arc<ControlPlane>>

#[derive(Clone, Default)]
pub struct Config {
    pub environment_id: String,
    pub label: String,
    pub bootstrap_credential: String, // what the pairing exchange accepts
    pub cookie_port: u16,             // the port in `t3_session_<port>`
    pub provider_command: Vec<String>,// a turn's command; the prompt follows
}

pub struct Host {
    pub runtime: Arc<dyn Runtime>,
    pub workspace: RemoteWorkspace,   // not connected yet
}
```

`open` rebuilds the store from the log. It then picks up every turn that was
running when the plane last stopped: each one resumes in `host.workspace`
from the last event the log recorded. Pass a workspace that is not
connected yet, and connect it afterwards. A running run whose turn cannot be
resumed is recorded as failed.

Keep one plane, and one `RemoteWorkspace`, for each environment. A host may
drop the plane whenever no call into it is in progress and open it again
later. Only what it wrote to storage survives, and that is enough.

## Storage

```rust
pub trait DocumentStore: Send + Sync {
    fn read(&self, path: &Path) -> io::Result<Vec<u8>>;   // missing: NotFound
    fn write(&self, path: &Path, bytes: &[u8], mode: WriteMode) -> io::Result<()>;
    fn remove(&self, path: &Path) -> io::Result<()>;
    fn create_dir_all(&self, path: &Path) -> io::Result<()>;
    fn revision(&self, path: &Path) -> Option<Revision>;
    fn list(&self, dir: &Path) -> io::Result<Vec<PathBuf>>; // at any depth
}

pub trait EventLog {
    fn load(&mut self) -> anyhow::Result<Vec<ServerEvent>>;
    fn append(&mut self, record: &ServerEvent) -> anyhow::Result<()>;
}
```

Paths are keys under `control::STATE_DIR`; a key-value store works. The plane
keeps auth records, keybindings, client stream records, and one record per
running turn there. `append` must be durable before it returns, because the
plane acknowledges a turn's event to `t3-agentd` only after that. Each
recorded turn output carries its stream position, which is what lets a
restarted plane resume the turn without losing or repeating output.
`persistence::MemoryStore` and `event_log::MemoryLog` suit tests.

### Settings and provider turns

The plane reads its settings from `settings.json` under `STATE_DIR`, in the
same format the native server keeps. If there is none, or it does not
parse, it uses the defaults. A turn runs the CLI of its thread's provider
instance, found in `settings.providerInstances` or as the built-in
instance of the same name, when the driver is `codex` or `claudeAgent`.
The instance's `config.binaryPath`, `config.launchArgs`, `config.homePath`
and `environment` apply, along with the thread's model and reasoning
effort. Any other instance runs `Config::provider_command`. The native
thread or session a turn reports is resumed by the thread's next turn.

## The runtime

```rust
pub trait Runtime {
    fn now_ms(&self) -> i64 { platform::now_ms() }
    fn spawn(&self, task: Task);          // run a turn in the background
    fn sleep(&self, duration: Duration) -> Wait;
    fn yield_now(&self) -> Wait;
    fn set_alarm(&self, at_ms: i64);      // one alarm; a later call replaces it
    fn push(&self, conn: ConnId, frames: Vec<Value>);
    fn connections(&self) -> Vec<ConnId>;
}
```

The plane uses `spawn` for turns and `set_alarm` to ask for a call back. When
the alarm fires, call `ControlPlane::on_alarm().await`, then deliver (see
below). The plane sets the next alarm itself. `ControlPlane::next_alarm()`
returns the same time (`Option<i64>`, wall-clock milliseconds) for hosts
that would rather compute it after each call:

- now, when a turn recorded something that clients have not been sent;
- every `MAINTAIN_INTERVAL_MS` (250 ms) while the workspace is connected.
  This keeps its streams open, forwards turn cancellations, and reads polled
  state (VCS status and branch, local servers) for clients' streams;
- otherwise never.

The plane does not use `push` or `connections`; the host delivers frames.
A task that `spawn` was given is lost if the process stops. That is safe,
because the next `open` resumes the turn from storage.

## Client HTTP and WebSockets

```rust
pub fn ControlPlane::router(self: &Arc<Self>) -> axum::Router
pub fn ControlPlane::authorize_socket(&self, headers: &HeaderMap, uri: &Uri)
    -> Result<Connection, Response>
pub async fn ControlPlane::on_message(self: &Arc<Self>, connection: &mut Connection, text: &str)
    -> Vec<String>
pub fn ControlPlane::disconnect(&self, connection: &Connection)
pub fn ControlPlane::take_signals(&self) -> Vec<Signal>
pub async fn ControlPlane::deliver(&self, connection: &Connection, signals: &[Signal])
    -> Vec<String>
pub fn ControlPlane::prune_streams<'a>(&self, open: impl IntoIterator<Item = &'a Connection>)
    -> usize
```

- Serve HTTP with `router()`. It answers everything except `/ws`.
- On a `/ws` upgrade, call `authorize_socket`. On success, keep the
  `Connection` with the socket. It is small and serializable, so a host that
  suspends between messages can store it with the socket. On failure, send
  the returned response.
- For each text frame, call `on_message` and send the frames it returns.
  Store the connection again afterwards, because it changes. If a socket's
  messages can be handled concurrently, read its connection only once the
  previous message's call has returned; a copy read earlier lacks the streams
  that call opened, and storing it would close them.
- After each message, alarm and workspace frame, deliver: call
  `take_signals()` once, then `deliver(connection, &signals)` for every open
  socket, and send what each returns.
- When a socket closes, call `disconnect`. When the plane is rebuilt from
  storage, call `prune_streams` with the connections of every socket still
  open, which deletes the stream records of sockets that closed unseen.

Client streams over the workspace (terminal output and metadata, preview
and device events, VCS status and branch, local servers) are stored records
like the plane's other streams. Each one keeps the workspace stream id and
the seq it last handled, or for polled state the last reading it sent. After
a restart or a reconnect, the workspace's streams have new ids. A record
that sees a new id catches up from the workspace's current state and sends
only what changed.

## The workspace connection

The host opens the connection to `t3-agentd` (a WebSocket to its `/`, with
`Authorization: Bearer <token>`, or with the token in `hello`; see
`RemoteWorkspace::new(epoch, token)`) and adapts it to:

```rust
pub trait Transport {
    /// Queue a frame for agentd. An error means the connection is gone.
    fn send(&self, frame: Frame) -> Result<(), WireError>;
}
```

`Frame` is `Text(String)` or `Binary(Vec<u8>)`; send each as that kind of
WebSocket message. Then:

```rust
pub async fn ControlPlane::workspace_connected(&self, transport: Arc<dyn Transport>)
    -> Result<HelloReply, WireError>
pub fn ControlPlane::workspace_frame(&self, frame: Frame)
pub fn ControlPlane::workspace_disconnected(&self)
pub fn ControlPlane::workspace(&self) -> &RemoteWorkspace
```

1. When the socket opens, call `workspace_connected(transport)` while
   already feeding inbound frames to `workspace_frame`, because the
   handshake waits for agentd's replies. It says hello, resumes the streams
   the plane still reads (resumed turns included), and opens the event
   streams.
2. Pass every inbound message to `workspace_frame`. Recorded turn events
   are acknowledged once they are in the log, and other streams' frames as
   they arrive.
3. When the socket closes, call `workspace_disconnected`, then reconnect
   with backoff. Each new server process must present an `epoch` no
   lower than the last one agentd saw, because agentd refuses older ones.
   A start-up timestamp works.
4. Keep calling `on_alarm` when the plane asks.

Natively, `workspace::connect(remote, url, token)` does steps 1 to 4 over a
tokio WebSocket and returns a `NativeLink` that stops when dropped.

The plane's own turns go through `Workspace::start_tracked_turn`. Each
`TurnItem` is recorded in the log, then acknowledged. Turn events of kinds
added later are recorded the same way.

## The WebAssembly entry point

`workers/` builds the plane for `wasm32-unknown-unknown`. Its runtime spawns
tasks on the JavaScript event loop and keeps one storage alarm, and its
alarm handler calls `on_alarm` and then delivers. It holds a
`RemoteWorkspace` with no transport, so workspace methods answer as
unavailable and a turn ends as failed. A host that embeds the library adds
the transport as described above.
