//! An in-memory stand-in for `t3-agentd`, for tests of code that drives a
//! [`RemoteWorkspace`]. It answers on the caller's stack, keeps every stream
//! frame until it is acknowledged, and survives the server side being
//! dropped and rebuilt, as a separate process does.
use super::{RemoteWorkspace, Transport};
use crate::workspace_protocol::{
    self as proto, op, Ack, EndReason, ErrorCode, Frame, HelloReply, Inbound, Resume, ResumeReply,
    StreamBody, StreamEnd, StreamFrame, StreamRef, TurnStart, WireError, WireTurnEvent,
};
use serde_json::{json, Value};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Mutex},
};

#[derive(Default)]
pub(crate) struct FakeAgentd {
    state: Mutex<State>,
}

#[derive(Default)]
struct State {
    next_stream: u64,
    streams: BTreeMap<u64, Stream>,
    /// Turns started, by stream.
    turns: BTreeMap<u64, TurnStart>,
    /// The connected server side, if any.
    peer: Option<RemoteWorkspace>,
    /// The latest stream each event-stream op opened.
    buses: BTreeMap<String, u64>,
    /// Open watches by op, and the reading each op reports.
    watches: Vec<(String, u64)>,
    readings: BTreeMap<String, Value>,
    /// The folders the workspace has, besides its root.
    dirs: BTreeSet<String>,
}

#[derive(Default)]
struct Stream {
    frames: Vec<StreamBody>,
    acked: u64,
    turn: bool,
}

/// The server side's connection to a [`FakeAgentd`].
struct Link {
    agentd: Arc<FakeAgentd>,
    workspace: RemoteWorkspace,
}

impl Transport for Link {
    fn send(&self, frame: Frame) -> Result<(), WireError> {
        self.agentd.handle(&self.workspace, frame);
        Ok(())
    }
}

impl FakeAgentd {
    /// A transport that connects `workspace` to this agentd.
    pub(crate) fn link(self: &Arc<Self>, workspace: &RemoteWorkspace) -> Arc<dyn Transport> {
        self.lock().peer = Some(workspace.clone());
        Arc::new(Link {
            agentd: self.clone(),
            workspace: workspace.clone(),
        })
    }

    /// Drop the connection, as a server restart does.
    pub(crate) fn disconnect(&self) {
        let peer = self.lock().peer.take();
        if let Some(peer) = peer {
            peer.disconnected();
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Lose `stream`, as an agentd that restarted does.
    pub(crate) fn forget(&self, stream: u64) {
        self.lock().streams.remove(&stream);
    }

    /// The event stream `op` (such as `device.events`) reports `event`.
    pub(crate) fn emit_bus(&self, op: &str, event: Value) {
        let stream = self
            .lock()
            .buses
            .get(op)
            .copied()
            .expect("the stream is open");
        self.push(stream, StreamBody::Event(event));
    }

    /// The polled state a watch `op` reads is now `reading`.
    pub(crate) fn set_reading(&self, op: &str, reading: Value) {
        let streams: Vec<u64> = {
            let mut state = self.lock();
            state.readings.insert(op.into(), reading.clone());
            state
                .watches
                .iter()
                .filter(|(watched, _)| watched == op)
                .map(|(_, stream)| *stream)
                .collect()
        };
        for stream in streams {
            self.push(
                stream,
                StreamBody::Event(json!(proto::Polled::Ok(reading.clone()))),
            );
        }
    }

    /// The turns started so far, by stream.
    pub(crate) fn turns(&self) -> BTreeMap<u64, TurnStart> {
        self.lock().turns.clone()
    }

    /// The last acknowledged seq of `stream`.
    pub(crate) fn acked(&self, stream: u64) -> u64 {
        self.lock().streams.get(&stream).map_or(0, |s| s.acked)
    }

    /// Whether every frame of every turn stream is acknowledged.
    pub(crate) fn turns_acked(&self) -> bool {
        self.lock()
            .streams
            .values()
            .filter(|s| s.turn)
            .all(|s| s.acked == s.frames.len() as u64)
    }

    /// The turn on `stream` reports `event`; an exit also ends the stream.
    pub(crate) fn emit(&self, stream: u64, event: WireTurnEvent) {
        let exited = matches!(event, WireTurnEvent::Exited { .. });
        self.push(stream, StreamBody::Event(json!(event)));
        if exited {
            self.push(
                stream,
                StreamBody::End(StreamEnd {
                    reason: EndReason::Done,
                    error: None,
                }),
            );
        }
    }

    fn push(&self, stream: u64, body: StreamBody) {
        let (peer, seq) = {
            let mut state = self.lock();
            let entry = state.streams.entry(stream).or_default();
            entry.frames.push(body.clone());
            let seq = entry.frames.len() as u64;
            (state.peer.clone(), seq)
        };
        if let Some(peer) = peer {
            peer.receive(stream_frame(stream, seq, body));
            peer.flush_acks();
        }
    }

    /// Add a folder to the workspace.
    pub(crate) fn add_dir(&self, path: &str) {
        self.lock().dirs.insert(path.to_owned());
    }

    fn has_dir(&self, path: &str) -> bool {
        path == "/workspace" || self.lock().dirs.contains(path)
    }

    fn open(&self, turn: bool) -> u64 {
        let mut state = self.lock();
        state.next_stream += 1;
        let id = state.next_stream;
        state.streams.insert(
            id,
            Stream {
                turn,
                ..Default::default()
            },
        );
        id
    }

    fn handle(&self, workspace: &RemoteWorkspace, frame: Frame) {
        let request = proto::decode_request(&frame).expect("a request frame");
        let mut replay = Vec::new();
        let result = match request.op.as_str() {
            op::HELLO => Ok(json!(HelloReply {
                version: proto::VERSION,
                epoch: request.args["epoch"].as_u64().unwrap_or_default(),
                root: "/workspace".into(),
                streams: Vec::new(),
            })),
            op::RESUME => {
                let resume: Resume = serde_json::from_value(request.args.clone()).unwrap();
                let state = self.lock();
                let mut missing = Vec::new();
                for (id, after) in resume.streams {
                    match state.streams.get(&id) {
                        Some(stream) => {
                            for (i, body) in stream.frames.iter().enumerate() {
                                let seq = i as u64 + 1;
                                if seq > after {
                                    replay.push(stream_frame(id, seq, body.clone()));
                                }
                            }
                        }
                        None => missing.push(id),
                    }
                }
                Ok(json!(ResumeReply { missing }))
            }
            op::ACK => {
                let ack: Ack = serde_json::from_value(request.args.clone()).unwrap();
                if let Some(stream) = self.lock().streams.get_mut(&ack.stream) {
                    stream.acked = stream.acked.max(ack.seq);
                }
                Ok(Value::Null)
            }
            op::TURN_START => {
                let start: TurnStart = serde_json::from_value(request.args.clone()).unwrap();
                let stream = self.open(true);
                self.lock().turns.insert(stream, start);
                Ok(json!({ "stream": stream }))
            }
            op::TURN_CANCEL => {
                let StreamRef { stream } = serde_json::from_value(request.args.clone()).unwrap();
                if let Some(id) = request.id {
                    workspace.receive(response(id, Ok(Value::Null)));
                }
                self.emit(
                    stream,
                    WireTurnEvent::Exited {
                        success: false,
                        cancelled: true,
                        status: "signal: 15".into(),
                    },
                );
                return;
            }
            op::TERM_EVENTS | op::TERM_METADATA | op::PREVIEW_EVENTS | op::DEVICE_EVENTS => {
                let stream = self.open(false);
                self.lock().buses.insert(request.op.clone(), stream);
                Ok(json!({ "stream": stream }))
            }
            op::WATCH_VCS_STATUS | op::WATCH_VCS_BRANCH | op::WATCH_LOCAL_SERVERS => {
                let stream = self.open(false);
                let reading = {
                    let mut state = self.lock();
                    state.watches.push((request.op.clone(), stream));
                    state.readings.get(&request.op).cloned()
                };
                if let Some(reading) = reading {
                    let body = StreamBody::Event(json!(proto::Polled::Ok(reading)));
                    self.lock()
                        .streams
                        .get_mut(&stream)
                        .unwrap()
                        .frames
                        .push(body.clone());
                    replay.push(stream_frame(stream, 1, body));
                }
                Ok(json!({ "stream": stream }))
            }
            op::STREAM_CLOSE => Ok(Value::Null),
            op::FS_CREATE_DIR_ALL => {
                let path = request.args["path"].as_str().unwrap_or_default();
                let mut state = self.lock();
                let mut dir = std::path::Path::new(path);
                loop {
                    state.dirs.insert(dir.to_string_lossy().into_owned());
                    match dir.parent() {
                        Some(parent) if parent != dir && !parent.as_os_str().is_empty() => {
                            dir = parent
                        }
                        _ => break,
                    }
                }
                Ok(Value::Null)
            }
            op::FS_CANONICALIZE => {
                let path = request.args["path"].as_str().unwrap_or_default();
                if self.has_dir(path) {
                    Ok(json!(path))
                } else {
                    Err(proto::io_error(&std::io::Error::from(
                        std::io::ErrorKind::NotFound,
                    )))
                }
            }
            op::FS_IS_DIR => Ok(json!(
                self.has_dir(request.args["path"].as_str().unwrap_or_default())
            )),
            // The model entries `set_reading` gave for it.
            op::PROVIDER_MODELS => self
                .lock()
                .readings
                .get(op::PROVIDER_MODELS)
                .cloned()
                .ok_or_else(|| WireError::new(ErrorCode::UnknownMethod, "no models")),
            // `terminal.observe`: the snapshot `set_reading` gave for it.
            op::TERM_CALL if request.args["method"] == "observe" => self
                .lock()
                .readings
                .get(op::TERM_CALL)
                .cloned()
                .ok_or_else(|| WireError::new(ErrorCode::UnknownMethod, "no terminal")),
            _ => Err(WireError::new(ErrorCode::UnknownMethod, "not in the fake")),
        };
        if let Some(id) = request.id {
            workspace.receive(response(id, result));
        }
        for frame in replay {
            workspace.receive(frame);
        }
        workspace.flush_acks();
    }
}

fn response(id: u64, result: Result<Value, WireError>) -> Frame {
    proto::inbound_frame(&Inbound::Response(proto::Response { id, result }))
}

fn stream_frame(stream: u64, seq: u64, body: StreamBody) -> Frame {
    proto::inbound_frame(&Inbound::Stream(StreamFrame { stream, seq, body }))
}
