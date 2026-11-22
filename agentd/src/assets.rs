//! Pull an asset's existing HTTP response one chunk at a time. A slow HTTP
//! reader never fills the workspace socket or the replay log with file bytes.
use super::{bad_request, Agentd, Frame, OpResult, WireError, Workspace};
use axum::{
    body::HttpBody,
    http::{header, HeaderMap, Method},
};
use base64::{engine::general_purpose::STANDARD, Engine};
use futures_util::TryStreamExt;
use serde_json::{json, Value};
use std::{collections::HashMap, io, time::Duration};
use tokio::{io::AsyncReadExt, sync::mpsc};
use tokio_util::io::StreamReader;

#[derive(Default)]
pub(super) struct Bodies(HashMap<u64, mpsc::Sender<(u64, mpsc::UnboundedSender<Frame>)>>);

impl Bodies {
    pub(super) async fn open(
        &mut self,
        id: Option<u64>,
        workspace: &dyn Workspace,
        args: &Value,
    ) -> OpResult {
        let id = id.ok_or_else(|| bad_request("asset.open requires a request id"))?;
        let method = match args["method"].as_str() {
            Some("HEAD") => Method::HEAD,
            Some("GET") => Method::GET,
            _ => return Err(bad_request("expected GET or HEAD")),
        };
        let mut headers = HeaderMap::new();
        for key in [header::RANGE, header::IF_RANGE, header::ACCEPT_ENCODING] {
            if let Some(value) = args["headers"][key.as_str()].as_str() {
                headers.insert(
                    key,
                    value
                        .parse()
                        .map_err(|_| bad_request("invalid asset request header"))?,
                );
            }
        }
        let name = super::str_arg(args, "name")?;
        let (parts, body) = workspace
            .open_asset(&args["claims"], name, &method, &headers)
            .await
            .into_parts();
        let headers: HashMap<_, _> = parts
            .headers
            .iter()
            .filter_map(|(k, v)| Some((k.as_str(), v.to_str().ok()?)))
            .collect();
        let mut result = json!({"status":parts.status.as_u16(),"headers":headers});
        if method == Method::HEAD || body.is_end_stream() {
            return Ok(result);
        }
        self.0.retain(|_, tx| !tx.is_closed());
        let (tx, mut rx) = mpsc::channel(1);
        self.0.insert(id, tx);
        result["body"] = json!(id);
        tokio::spawn(async move {
            let mut reader = StreamReader::new(body.into_data_stream().map_err(io::Error::other));
            let mut buf = vec![0; 64 * 1024];
            // An open whose caller disappeared before receiving its id also expires.
            while let Ok(Some((request, socket))) =
                tokio::time::timeout(Duration::from_secs(60), rx.recv()).await
            {
                let chunk = reader.read(&mut buf).await;
                let finished = !matches!(&chunk, Ok(n) if *n > 0);
                let value = chunk
                    .map(|n| {
                        if n == 0 {
                            Value::Null
                        } else {
                            json!(STANDARD.encode(&buf[..n]))
                        }
                    })
                    .map_err(|e| super::proto::io_error(&e));
                Agentd::reply(&socket, Some(request), value);
                if finished {
                    break;
                }
            }
        });
        Ok(result)
    }

    pub(super) fn read(
        &self,
        request: Option<u64>,
        socket: &mpsc::UnboundedSender<Frame>,
        args: &Value,
    ) {
        let Some(id) = request else {
            return;
        };
        let body = args["body"].as_u64().and_then(|i| self.0.get(&i));
        if body.is_none_or(|tx| tx.try_send((id, socket.clone())).is_err()) {
            Agentd::reply(
                socket,
                request,
                Err(WireError::new(
                    super::ErrorCode::UnknownStream,
                    "asset body is unavailable",
                )),
            );
        }
    }

    pub(super) fn close(&mut self, args: &Value) {
        if let Some(id) = args["body"].as_u64() {
            self.0.remove(&id);
        }
    }
}
