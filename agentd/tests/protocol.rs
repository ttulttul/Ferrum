//! Drive `t3-agentd` with raw protocol frames over a real WebSocket.
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use std::{collections::VecDeque, net::SocketAddr, path::Path, sync::Arc, time::Duration};
use t3_agentd::{Agentd, Config};
use t3code_server::workspace::{LocalWorkspace, Workspace};
use t3code_server::workspace_protocol::{self as proto, Frame};
use tokio::net::TcpStream;
use tokio_tungstenite::{
    tungstenite::{client::IntoClientRequest, Message},
    MaybeTlsStream, WebSocketStream,
};

const TOKEN: &str = "test-token";

async fn start(dir: &Path, buffer_limit: u64) -> (SocketAddr, Arc<LocalWorkspace>) {
    let workspace = Arc::new(LocalWorkspace::open(&dir.join("state")).unwrap());
    let agentd = Agentd::new(
        workspace.clone(),
        Config {
            token: TOKEN.into(),
            root: dir.to_path_buf(),
            buffer_dir: dir.join("streams"),
            buffer_limit,
            poll_interval: Duration::from_millis(50),
        },
    )
    .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, agentd.router()).await });
    (addr, workspace)
}

struct Client {
    ws: WebSocketStream<MaybeTlsStream<TcpStream>>,
    next_id: u64,
    frames: VecDeque<Value>,
}

impl Client {
    async fn connect(addr: SocketAddr, token: Option<&str>) -> Result<Self, String> {
        let mut request = format!("ws://{addr}/").into_client_request().unwrap();
        if let Some(token) = token {
            request
                .headers_mut()
                .insert("authorization", format!("Bearer {token}").parse().unwrap());
        }
        let (ws, _) = tokio_tungstenite::connect_async(request)
            .await
            .map_err(|e| e.to_string())?;
        Ok(Self {
            ws,
            next_id: 0,
            frames: VecDeque::new(),
        })
    }

    async fn adopt(addr: SocketAddr, epoch: u64) -> Self {
        let mut client = Self::connect(addr, Some(TOKEN)).await.unwrap();
        client.hello(epoch).await.unwrap();
        client
    }

    async fn hello(&mut self, epoch: u64) -> Result<Value, Value> {
        self.request("hello", json!({"version": proto::VERSION, "epoch": epoch}))
            .await
    }

    async fn send(&mut self, value: Value) {
        let message = match proto::encode_frame(&value, proto::BULK_THRESHOLD) {
            Frame::Text(text) => Message::Text(text),
            Frame::Binary(bytes) => Message::Binary(bytes),
        };
        self.ws.send(message).await.unwrap();
    }

    async fn read(&mut self) -> Value {
        let message = tokio::time::timeout(Duration::from_secs(20), self.ws.next())
            .await
            .expect("agentd answered in time")
            .expect("the socket is open")
            .unwrap();
        let frame = match message {
            Message::Text(text) => Frame::Text(text),
            Message::Binary(bytes) => Frame::Binary(bytes),
            other => panic!("unexpected message {other:?}"),
        };
        proto::decode_frame(&frame).unwrap()
    }

    async fn request(&mut self, op: &str, args: Value) -> Result<Value, Value> {
        self.next_id += 1;
        let id = self.next_id;
        self.send(json!({"id": id, "op": op, "args": args})).await;
        loop {
            let value = self.read().await;
            if value.get("stream").is_some() {
                self.frames.push_back(value);
            } else if value["id"] == id {
                return match value.get("err") {
                    Some(err) => Err(err.clone()),
                    None => Ok(value["ok"].clone()),
                };
            }
        }
    }

    async fn frame(&mut self) -> Value {
        match self.frames.pop_front() {
            Some(frame) => frame,
            None => self.read().await,
        }
    }

    async fn ack(&mut self, stream: u64, seq: u64) {
        self.send(json!({"op": "ack", "args": {"stream": stream, "seq": seq}}))
            .await;
    }
}

fn turn(script: &str) -> Value {
    json!({
        "command": ["sh", "-c", script, "sh"],
        "prompt": "unused",
        "cwd": "/",
    })
}

#[tokio::test]
async fn refuses_requests_without_the_token() {
    let dir = tempfile::tempdir().unwrap();
    let (addr, _) = start(dir.path(), 1 << 20).await;
    assert!(Client::connect(addr, Some("wrong")).await.is_err());

    let mut client = Client::connect(addr, None).await.unwrap();
    let err = client.request("ping", Value::Null).await.unwrap_err();
    assert_eq!(err["code"], "unauthorized");
    let err = client.hello(1).await.unwrap_err();
    assert_eq!(err["code"], "unauthorized");
    let ok = client
        .request(
            "hello",
            json!({"version": proto::VERSION, "epoch": 1, "token": TOKEN}),
        )
        .await
        .unwrap();
    assert_eq!(ok["epoch"], 1);
    assert_eq!(client.request("ping", Value::Null).await.unwrap(), "pong");

    let mut client = Client::connect(addr, Some(TOKEN)).await.unwrap();
    let err = client.request("ping", Value::Null).await.unwrap_err();
    assert_eq!(err["code"], "hello_required");
    let err = client
        .request("hello", json!({"version": 999, "epoch": 1}))
        .await
        .unwrap_err();
    assert_eq!(err["code"], "version_mismatch");
}

#[tokio::test]
async fn a_newer_epoch_retires_older_connections() {
    let dir = tempfile::tempdir().unwrap();
    let (addr, _) = start(dir.path(), 1 << 20).await;
    let mut first = Client::adopt(addr, 5).await;
    let mut older = Client::connect(addr, Some(TOKEN)).await.unwrap();
    assert_eq!(older.hello(4).await.unwrap_err()["code"], "stale_epoch");
    let mut newer = Client::adopt(addr, 6).await;
    assert_eq!(
        first.request("ping", Value::Null).await.unwrap_err()["code"],
        "stale_epoch"
    );
    assert_eq!(newer.request("ping", Value::Null).await.unwrap(), "pong");
    // The same server may reconnect with its own epoch.
    let mut again = Client::adopt(addr, 6).await;
    assert_eq!(again.request("ping", Value::Null).await.unwrap(), "pong");
}

#[tokio::test]
async fn unary_ops_answer_like_the_local_workspace() {
    let dir = tempfile::tempdir().unwrap();
    let (addr, local) = start(dir.path(), 1 << 20).await;
    let mut client = Client::adopt(addr, 1).await;
    let cwd = dir.path().to_string_lossy().into_owned();
    let big = "x".repeat(100_000);
    let write = json!({"cwd": cwd, "relativePath": "a/big.txt", "contents": big});
    assert!(client.request("fs.writeFile", write).await.is_ok());
    let read = json!({"cwd": cwd, "relativePath": "a/big.txt"});
    let remote = client.request("fs.readFile", read.clone()).await;
    assert_eq!(remote, local.read_file(&read).await.unwrap());
    assert!(remote.unwrap().to_string().contains(&big));

    let missing = json!({"cwd": cwd, "relativePath": "../outside"});
    let remote = client.request("fs.readFile", missing.clone()).await;
    let local_error = local.read_file(&missing).await.unwrap().unwrap_err();
    assert_eq!(remote.unwrap_err()["detail"], local_error);

    let err = client
        .request("git.vcs", json!({"method": "noSuchMethod", "input": {}}))
        .await
        .unwrap_err();
    assert_eq!(err["code"], "unknown_method");
    let err = client.request("fs.nope", json!({})).await.unwrap_err();
    assert_eq!(err["code"], "unknown_op");
    let err = client
        .request("fs.canonicalize", json!({"path": dir.path().join("none")}))
        .await
        .unwrap_err();
    assert_eq!(err["code"], "io");
    assert_eq!(err["detail"]["kind"], "notFound");
    assert_eq!(
        client
            .request("fs.isDir", json!({"path": cwd}))
            .await
            .unwrap(),
        true
    );
    let idle = client.request("idle.state", Value::Null).await.unwrap();
    assert_eq!(idle["runningTurns"], 0);
    assert_eq!(idle["connected"], true);
}

#[tokio::test]
async fn a_turn_survives_a_reconnect_without_loss_or_duplication() {
    let dir = tempfile::tempdir().unwrap();
    let (addr, _) = start(dir.path(), 1 << 20).await;
    let mut client = Client::adopt(addr, 1).await;
    let opened = client
        .request(
            "turn.start",
            turn("for i in $(seq 1 30); do echo line $i; sleep 0.02; done"),
        )
        .await
        .unwrap();
    let stream = opened["stream"].as_u64().unwrap();
    let mut seen = Vec::new();
    // Take a few frames, acknowledge fewer, then drop the socket.
    for _ in 0..5 {
        let frame = client.frame().await;
        assert_eq!(frame["stream"], stream);
        seen.push(frame);
    }
    client.ack(stream, 3).await;
    drop(client);
    tokio::time::sleep(Duration::from_millis(300)).await;

    let mut client = Client::adopt(addr, 1).await;
    let hello = client.hello(1).await.unwrap();
    assert_eq!(hello["streams"][0]["stream"], stream);
    assert_eq!(hello["streams"][0]["op"], "turn.start");
    let last = seen.last().unwrap()["seq"].as_u64().unwrap();
    let resumed = client
        .request("resume", json!({"streams": {stream.to_string(): last}}))
        .await
        .unwrap();
    assert_eq!(resumed["missing"], json!([]));
    loop {
        let frame = client.frame().await;
        let end = frame.get("end").is_some();
        seen.push(frame);
        if end {
            break;
        }
    }
    let seqs: Vec<u64> = seen.iter().map(|f| f["seq"].as_u64().unwrap()).collect();
    assert_eq!(seqs, (1..=seqs.len() as u64).collect::<Vec<_>>());
    let lines: Vec<String> = seen
        .iter()
        .filter(|f| f["event"]["type"] == "stdout")
        .map(|f| f["event"]["text"].as_str().unwrap().to_owned())
        .collect();
    assert_eq!(
        lines,
        (1..=30).map(|i| format!("line {i}\n")).collect::<Vec<_>>()
    );
    let n = seen.len();
    assert_eq!(seen[n - 2]["event"]["type"], "exited");
    assert_eq!(seen[n - 2]["event"]["success"], true);
    assert_eq!(seen[n - 1]["end"]["reason"], "done");
    client.ack(stream, seqs[n - 1]).await;
    let idle = client.request("idle.state", Value::Null).await.unwrap();
    assert_eq!(idle["openStreams"], 0);
    assert_eq!(idle["unackedFrames"], 0);
}

#[tokio::test]
async fn a_full_buffer_ends_the_stream_with_a_gap_and_stops_the_turn() {
    let dir = tempfile::tempdir().unwrap();
    let (addr, _) = start(dir.path(), 4096).await;
    let mut client = Client::adopt(addr, 1).await;
    let opened = client
        .request("turn.start", turn("while true; do echo flood; done"))
        .await
        .unwrap();
    let stream = opened["stream"].as_u64().unwrap();
    // Never acknowledge: the buffer fills and the stream ends.
    let end = loop {
        let frame = client.frame().await;
        if let Some(end) = frame.get("end") {
            break end.clone();
        }
    };
    assert_eq!(end["reason"], "gap");
    let idle = client.request("idle.state", Value::Null).await.unwrap();
    assert_eq!(idle["runningTurns"], 0);
    let _ = stream;
}

#[tokio::test]
async fn unresumed_streams_are_closed_and_cancel_stops_a_turn() {
    let dir = tempfile::tempdir().unwrap();
    let (addr, _) = start(dir.path(), 1 << 20).await;
    let mut client = Client::adopt(addr, 1).await;
    let first = client
        .request("turn.start", turn("sleep 30"))
        .await
        .unwrap()["stream"]
        .as_u64()
        .unwrap();
    let second = client
        .request("turn.start", turn("sleep 30"))
        .await
        .unwrap()["stream"]
        .as_u64()
        .unwrap();
    client
        .request("turn.cancel", json!({"stream": second}))
        .await
        .unwrap();
    let exited = client.frame().await;
    assert_eq!(exited["stream"], second);
    assert_eq!(exited["event"]["cancelled"], true);
    drop(client);

    let mut client = Client::adopt(addr, 2).await;
    let resumed = client
        .request(
            "resume",
            json!({"streams": {second.to_string(): 1, "999": 0}}),
        )
        .await
        .unwrap();
    assert_eq!(resumed["missing"], json!([999]));
    assert_eq!(client.frame().await["end"]["reason"], "done");
    let idle = client.request("idle.state", Value::Null).await.unwrap();
    assert_eq!(
        idle["runningTurns"], 0,
        "the unresumed turn {first} stopped"
    );
}

#[tokio::test]
async fn watches_push_only_changes() {
    let dir = tempfile::tempdir().unwrap();
    let (addr, local) = start(dir.path(), 1 << 20).await;
    let mut client = Client::adopt(addr, 1).await;
    let cwd = dir.path().to_string_lossy().into_owned();
    let input = json!({"cwd": cwd});
    let stream = client
        .request("watch.vcsBranch", json!({"input": input}))
        .await
        .unwrap()["stream"]
        .as_u64()
        .unwrap();
    let first = client.frame().await;
    assert_eq!(first["stream"], stream);
    assert_eq!(first["seq"], 1);
    let expected = proto::Polled::from_routed(Some(local.vcs_branch(&input).await));
    assert_eq!(first["event"], serde_json::to_value(expected).unwrap());
    // Nothing changes, so nothing more arrives across several polls.
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(client.request("ping", Value::Null).await.unwrap(), "pong");
    assert!(client.frames.is_empty());
    client
        .request("stream.close", json!({"stream": stream}))
        .await
        .unwrap();
    let idle = client.request("idle.state", Value::Null).await.unwrap();
    assert_eq!(idle["openStreams"], 0);
}
