use super::{invalid, string, timestamp, Reply};
use portable_pty::{ChildKiller, CommandBuilder, MasterPty, PtySize};
use serde_json::{json, Value};
use std::{
    collections::HashMap,
    io::{Read, Write},
    sync::{Arc, Mutex},
};
use tokio::sync::broadcast;

type Key = (String, String);
const HISTORY_LIMIT: usize = 1024 * 1024;

struct Session {
    master: Box<dyn MasterPty + Send>,
    writer: Box<dyn Write + Send>,
    killer: Box<dyn ChildKiller + Send + Sync>,
    snapshot: Arc<Mutex<Value>>,
}
impl Drop for Session {
    fn drop(&mut self) {
        let _ = self.killer.kill();
    }
}
#[derive(Clone)]
pub struct TerminalService {
    sessions: Arc<Mutex<HashMap<Key, Session>>>,
    events: broadcast::Sender<Value>,
    metadata: broadcast::Sender<Value>,
}
impl Default for TerminalService {
    fn default() -> Self {
        Self {
            sessions: Default::default(),
            events: broadcast::channel(512).0,
            metadata: broadcast::channel(512).0,
        }
    }
}

fn summary(s: &Value) -> Value {
    let mut item = json!({});
    for k in [
        "threadId",
        "terminalId",
        "cwd",
        "worktreePath",
        "status",
        "pid",
        "exitCode",
        "exitSignal",
    ] {
        item[k] = s[k].clone();
    }
    item["hasRunningSubprocess"] = json!(false);
    item["label"] = s["label"].clone();
    item["updatedAt"] = s["updatedAt"].clone();
    item
}
fn size(input: &Value, optional: bool) -> Result<PtySize, Value> {
    let dimension = |key: &str, max: u16, default: u16| match input.get(key) {
        None if optional => Ok(default),
        None => Err(invalid(key, "Missing key")),
        Some(v) => v
            .as_u64()
            .filter(|n| (1..=u64::from(max)).contains(n))
            .map(|v| v as u16)
            .ok_or_else(|| invalid(key, &format!("Expected a value between 1 and {max}"))),
    };
    Ok(PtySize {
        cols: dimension("cols", 1000, 120)?,
        rows: dimension("rows", 500, 30)?,
        pixel_width: 0,
        pixel_height: 0,
    })
}
fn label(id: &str) -> String {
    let lower = id.to_ascii_lowercase();
    if let Some(s) = lower
        .strip_prefix("term-")
        .or_else(|| lower.strip_prefix("terminal-"))
    {
        let (n, tail) = s.split_once('-').map_or((s, None), |(n, t)| (n, Some(t)));
        if !n.is_empty()
            && n.bytes().all(|c| c.is_ascii_digit())
            && tail.map_or(true, |s| s.len() == 36 && uuid::Uuid::parse_str(s).is_ok())
        {
            return format!("Terminal {n}");
        }
    }
    id.chars().take(128).collect()
}
fn emit(snapshot: &mut Value, bus: &broadcast::Sender<Value>, kind: &str, fields: Value) {
    let seq = snapshot["sequence"].as_u64().unwrap_or(0) + 1;
    snapshot["sequence"] = json!(seq);
    snapshot["updatedAt"] = json!(timestamp());
    let mut event = json!({"threadId":snapshot["threadId"],"terminalId":snapshot["terminalId"],"sequence":seq,"type":kind});
    if let Some(fields) = fields.as_object() {
        for (k, v) in fields {
            event[k] = v.clone();
        }
    }
    let _ = bus.send(event);
}
fn decode(bytes: &mut Vec<u8>, eof: bool) -> String {
    let mut text = String::new();
    let mut start = 0;
    while start < bytes.len() {
        match std::str::from_utf8(&bytes[start..]) {
            Ok(s) => {
                text.push_str(s);
                start = bytes.len();
            }
            Err(e) => {
                text.push_str(std::str::from_utf8(&bytes[start..start + e.valid_up_to()]).unwrap());
                start += e.valid_up_to();
                match e.error_len() {
                    Some(n) => {
                        text.push('\u{fffd}');
                        start += n;
                    }
                    None if eof => {
                        text.push('\u{fffd}');
                        start = bytes.len();
                    }
                    None => break,
                }
            }
        }
    }
    bytes.drain(..start);
    text
}
fn trim_history(history: &mut String, max_bytes: usize) {
    let mut start = history.len().saturating_sub(max_bytes);
    while !history.is_char_boundary(start) {
        start += 1;
    }
    if start > 0 {
        history.drain(..start);
    }
}
impl TerminalService {
    pub fn subscribe_metadata(&self) -> broadcast::Receiver<Value> {
        self.metadata.subscribe()
    }
    pub fn metadata_snapshot(&self) -> Value {
        let mut items: Vec<_> = self
            .sessions
            .lock()
            .unwrap()
            .values()
            .map(|s| summary(&s.snapshot.lock().unwrap()))
            .collect();
        items.sort_by(|a, b| {
            b["updatedAt"]
                .as_str()
                .cmp(&a["updatedAt"].as_str())
                .then_with(|| a["threadId"].as_str().cmp(&b["threadId"].as_str()))
                .then_with(|| a["terminalId"].as_str().cmp(&b["terminalId"].as_str()))
        });
        json!({"type":"snapshot","terminals":items})
    }
    pub fn subscribe(&self) -> broadcast::Receiver<Value> {
        self.events.subscribe()
    }

    pub async fn call(&self, name: &str, input: &Value) -> Option<Reply> {
        if !matches!(
            name,
            "open" | "attach" | "observe" | "write" | "resize" | "clear" | "close"
        ) {
            return None;
        }
        let service = self.clone();
        let method = name.to_owned();
        let input = input.clone();
        Some(
            tokio::task::spawn_blocking(move || service.apply(&method, &input))
                .await
                .unwrap_or_else(|e| Err(json!({"_tag":"Die","defect":e.to_string()}))),
        )
    }

    fn launch(&self, input: &Value, thread: &str, id: &str, cwd: &str) -> Result<Session, Value> {
        let grid = size(input, true)?;
        let stat=std::fs::metadata(cwd).map_err(|e|if e.kind()==std::io::ErrorKind::NotFound{json!({"_tag":"TerminalCwdNotFoundError","cwd":cwd})}else{json!({"_tag":"TerminalCwdStatError","cwd":cwd,"cause":{"name":"Error","message":e.to_string()}})})?;
        if !stat.is_dir() {
            return Err(json!({"_tag":"TerminalCwdNotDirectoryError","cwd":cwd}));
        }
        let shell = std::env::var("SHELL").unwrap_or_else(|_| {
            if cfg!(windows) {
                "powershell.exe".into()
            } else {
                "/bin/sh".into()
            }
        });
        let mut cmd = CommandBuilder::new(shell);
        cmd.cwd(cwd);
        cmd.env("TERM", "xterm-256color");
        if let Some(env) = input.get("env") {
            for (k, v) in env
                .as_object()
                .ok_or_else(|| invalid("env", "Expected object"))?
            {
                if k.len() > 128
                    || k.is_empty()
                    || !k.bytes().enumerate().all(|(i, b)| {
                        b == b'_' || b.is_ascii_alphabetic() || (i > 0 && b.is_ascii_digit())
                    })
                {
                    return Err(invalid("env", "Invalid environment variable key"));
                }
                let v = v
                    .as_str()
                    .ok_or_else(|| invalid("env", "Expected string"))?;
                if v.contains('\0') || v.encode_utf16().count() > 8192 {
                    return Err(invalid("env", "Invalid environment variable value"));
                }
                cmd.env(k, v);
            }
        }
        let failure = |e: anyhow::Error| json!({"_tag":"Die","defect":format!("Failed to start terminal: {e}")});
        let pty = portable_pty::native_pty_system()
            .openpty(grid)
            .map_err(failure)?;
        let mut reader = pty.master.try_clone_reader().map_err(failure)?;
        let writer = pty.master.take_writer().map_err(failure)?;
        let mut child = pty.slave.spawn_command(cmd).map_err(failure)?;
        let pid = child.process_id();
        let killer = child.clone_killer();
        drop(pty.slave);
        let snapshot = Arc::new(Mutex::new(
            json!({"threadId":thread,"terminalId":id,"cwd":cwd,"worktreePath":input["worktreePath"],"status":"running","pid":pid,"history":"","exitCode":null,"exitSignal":null,"label":label(id),"updatedAt":timestamp(),"sequence":1,"cols":grid.cols,"rows":grid.rows}),
        ));
        let meta = self.metadata.clone();
        let _ = meta.send(json!({"type":"upsert","terminal":summary(&snapshot.lock().unwrap())}));
        let data = snapshot.clone();
        let bus = self.events.clone();
        let _ = bus.send(json!({"type":"snapshot","snapshot":snapshot.lock().unwrap().clone()}));
        std::thread::spawn(move || {
            let mut remaining = Vec::new();
            let mut buf = [0; 8192];
            loop {
                let n = match reader.read(&mut buf) {
                    Ok(n) => n,
                    Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
                    Err(_) => 0,
                };
                remaining.extend_from_slice(&buf[..n]);
                let text = decode(&mut remaining, n == 0);
                if !text.is_empty() {
                    let mut s = data.lock().unwrap();
                    if s["status"] == "running" {
                        let mut history =
                            s["history"].take().as_str().unwrap_or_default().to_owned();
                        history.push_str(&text);
                        trim_history(&mut history, HISTORY_LIMIT);
                        s["history"] = Value::String(history);
                        emit(&mut s, &bus, "output", json!({"data":text}));
                    }
                }
                if n == 0 {
                    break;
                }
            }
            let code = child.wait().ok().map(|s| s.exit_code());
            let mut s = data.lock().unwrap();
            if s["status"] == "running" {
                s["status"] = json!("exited");
                s["pid"] = Value::Null;
                s["exitCode"] = json!(code);
                emit(
                    &mut s,
                    &bus,
                    "exited",
                    json!({"exitCode":code,"exitSignal":null}),
                );
                let _ = meta.send(json!({"type":"upsert","terminal":summary(&s)}));
            }
        });
        Ok(Session {
            master: pty.master,
            writer,
            killer,
            snapshot,
        })
    }

    fn apply(&self, name: &str, input: &Value) -> Reply {
        let thread = string(input, "threadId")?;
        let mut sessions = self.sessions.lock().unwrap();
        if name == "close" {
            let id = input
                .get("terminalId")
                .map(|_| string(input, "terminalId"))
                .transpose()?;
            sessions.retain(|(t, i), s| {
                if t == thread && id.map_or(true, |id| id == i) {
                    let mut snap = s.snapshot.lock().unwrap();
                    let _ = self
                        .metadata
                        .send(json!({"type":"remove","threadId":t,"terminalId":i}));
                    snap["status"] = json!("closed");
                    emit(&mut snap, &self.events, "closed", Value::Null);
                    false
                } else {
                    true
                }
            });
            return Ok(Value::Null);
        }
        let id = string(input, "terminalId")?;
        let key = (thread.to_owned(), id.to_owned());
        if name == "open" || (name == "attach" && input.get("cwd").is_some()) {
            let cwd = string(input, "cwd")?;
            let launch = sessions.get(&key).map_or(true, |session| {
                let s = session.snapshot.lock().unwrap();
                s["cwd"] != cwd
                    || (s["status"] != "running"
                        && (name == "open" || input["restartIfNotRunning"] == true))
            });
            if launch {
                let new = self.launch(input, thread, id, cwd)?;
                if let Some(old) = sessions.insert(key.clone(), new) {
                    old.snapshot.lock().unwrap()["status"] = json!("closed");
                }
            }
        }
        if name == "resize" {
            let grid = size(input, false)?;
            match sessions.get(&key) {
                None => return Ok(Value::Null),
                Some(s) => {
                    let snap = s.snapshot.lock().unwrap();
                    if snap["status"] != "running"
                        || (snap["cols"] == grid.cols && snap["rows"] == grid.rows)
                    {
                        return Ok(Value::Null);
                    }
                }
            }
        }
        let session = sessions.get_mut(&key).ok_or_else(
            || json!({"_tag":"TerminalSessionLookupError","threadId":thread,"terminalId":id}),
        )?;
        let mut snapshot = session.snapshot.lock().unwrap();
        match name {
            "open" => Ok(snapshot.clone()),
            "observe" | "attach" => {
                let mut snap = snapshot.clone();
                if let Some(n) = input["maxHistoryBytes"].as_u64() {
                    let mut history = snap["history"].as_str().unwrap_or_default().to_owned();
                    trim_history(&mut history, n as usize);
                    snap["history"] = json!(history);
                }
                Ok(json!({"type":"snapshot","snapshot":snap}))
            }
            "clear" => {
                snapshot["history"] = json!("");
                emit(&mut snapshot, &self.events, "cleared", Value::Null);
                Ok(Value::Null)
            }
            "write" | "resize" if snapshot["status"] != "running" => {
                Err(json!({"_tag":"TerminalNotRunningError","threadId":thread,"terminalId":id}))
            }
            "write" => {
                let data = input
                    .get("data")
                    .ok_or_else(|| invalid("data", "Missing key"))?
                    .as_str()
                    .ok_or_else(|| invalid("data", "Expected string"))?;
                if data.is_empty() || data.encode_utf16().count() > 65536 {
                    return Err(invalid("data", "Invalid terminal write length"));
                }
                let bytes = if input["binary"] == true {
                    data.encode_utf16().map(|n| n as u8).collect()
                } else {
                    data.as_bytes().to_vec()
                };
                let pid = snapshot["pid"].clone();
                drop(snapshot);
                session.writer.write_all(&bytes).and_then(|_|session.writer.flush()).map_err(|e|json!({"_tag":"TerminalWriteError","threadId":thread,"terminalId":id,"terminalPid":pid,"cause":{"name":"Error","message":e.to_string()}}))?;
                Ok(Value::Null)
            }
            "resize" => {
                let grid = size(input, false)?;
                session.master.resize(grid).map_err(|e|json!({"_tag":"TerminalResizeError","threadId":thread,"terminalId":id,"terminalPid":snapshot["pid"],"cols":grid.cols,"rows":grid.rows,"cause":{"name":"Error","message":e.to_string()}}))?;
                snapshot["cols"] = json!(grid.cols);
                snapshot["rows"] = json!(grid.rows);
                let fields = json!({"hasRunningSubprocess":false,"label":snapshot["label"],"cols":grid.cols,"rows":grid.rows});
                emit(&mut snapshot, &self.events, "activity", fields);
                let _ = self
                    .metadata
                    .send(json!({"type":"upsert","terminal":summary(&snapshot)}));
                Ok(Value::Null)
            }
            _ => unreachable!(),
        }
    }
}
