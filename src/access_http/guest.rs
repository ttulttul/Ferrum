use super::cookie;
use crate::{
    auth_service::iso,
    guest_service::{access, Share},
    AppState,
};
use axum::{
    body::{to_bytes, Body},
    extract::{DefaultBodyLimit, FromRequest, Multipart, Query, Request, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use serde_json::{json, Value};
use std::{collections::HashMap, sync::Arc, time::Duration};
pub(super) fn routes() -> Router<Arc<AppState>> {
    Router::new()
        .route("/api/guest/redeem", post(redeem))
        .route(
            "/api/guest/messages",
            post(messages).layer(DefaultBodyLimit::max(10 * 1024 * 1024 + 65536)),
        )
        .route("/api/guest/session", get(read))
        .route("/api/guest/stream", get(read))
        .route("/api/guest/item", get(read))
        .route("/api/guest/history", get(read))
        .route("/api/guest/render", get(read))
        .route("/api/guest/command-output", get(read))
        .route("/api/guest/attachment", get(read))
        .route("/api/guest/media", get(read))
}
const NO_STORE: [(&str, &str); 1] = [("cache-control", "no-store")];
fn error(reason: &str) -> Response {
    let (status, msg) = match reason {
        "bad_request" => (400, "The request was not valid."),
        "invalid" => (403, "This guest link is not valid."),
        "inactive" => (403, "This guest link is no longer active."),
        "used" => (409, "This guest link was already used."),
        "unavailable" => (403, "This item is not available."),
        "empty" => (400, "Write a message or attach a file."),
        "too_large" => (400, "Attached files can total up to 10 MB per message."),
        "too_many_files" => (400, "Attach up to 4 files to a message."),
        "unsupported_file" => (
            400,
            "This file cannot be attached. Images must be PNG, JPEG, GIF, or WebP.",
        ),
        "files_busy" => (
            409,
            "Wait for the owner to answer your earlier messages before you attach more files.",
        ),
        "busy" => (409, "Wait for the owner to answer your earlier messages."),
        _ => (409, "The owner's server rejected this request."),
    };
    (
        StatusCode::from_u16(status).unwrap(),
        NO_STORE,
        Json(json!({"reason":if reason=="bad_request"{"invalid"}else{reason},"message":msg})),
    )
        .into_response()
}
async fn body(req: Request) -> Option<Value> {
    serde_json::from_slice(&to_bytes(req.into_body(), 65536).await.ok()?).ok()
}
async fn redeem(State(s): State<Arc<AppState>>, req: Request) -> Response {
    let Some(input) = body(req).await else {
        return error("bad_request");
    };
    let Some(name)=input["name"].as_str().map(str::trim).filter(|v|!v.is_empty()&&v.encode_utf16().count()<=40&&!v.chars().any(|c|c.is_control()||matches!(c,'\u{2028}'|'\u{2029}'|'\u{061c}'|'\u{200e}'|'\u{200f}'|'\u{202a}'..='\u{202e}'|'\u{2066}'..='\u{2069}')))else{return error("bad_request")};
    let Some(token) = input["credential"]
        .as_str()
        .map(str::trim)
        .filter(|v| !v.is_empty())
    else {
        return error("bad_request");
    };
    let share = match access(&s, token, true).await {
        Ok(v) => v,
        Err(e) => return error(e),
    };
    match s.guest.redeem(&share,name){Ok(token)=>(StatusCode::NO_CONTENT,NO_STORE,[("set-cookie",format!("t3_guest_{}={token}; Max-Age=86400; Path=/api/guest; HttpOnly; Secure; SameSite=Strict",share.id))]).into_response(),Err(e)=>error(e)}
}
fn session_token<'a>(h: &'a HeaderMap, q: &HashMap<String, String>) -> &'a str {
    q.get("share")
        .and_then(|s| cookie(h, &format!("t3_guest_{s}")))
        .unwrap_or("")
}
async fn messages(
    State(s): State<Arc<AppState>>,
    Query(q): Query<HashMap<String, String>>,
    req: Request,
) -> Response {
    let token = session_token(req.headers(), &q);
    let share = match access(&s, token, false).await {
        Ok(v) => v,
        Err(e) => return error(e),
    };
    if Some(&share.id) != q.get("share") {
        return error("invalid");
    }
    let multipart = req
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.starts_with("multipart/form-data"));
    let (input, uploads) = if multipart {
        match submission(req).await {
            Ok(parsed) => parsed,
            Err(e) => return error(e),
        }
    } else {
        let Some(input) = body(req).await else {
            return error("bad_request");
        };
        (input, vec![])
    };
    let Some(text) = input["text"]
        .as_str()
        .map(str::trim)
        .filter(|s| s.encode_utf16().count() <= 4000)
    else {
        return error("bad_request");
    };
    if input["shareId"] != share.id {
        return error("bad_request");
    }
    match s.guest.submit(&share, text, uploads) {
        Ok(()) => (StatusCode::NO_CONTENT, NO_STORE).into_response(),
        Err(e) => error(e),
    }
}
fn image_matches(mime: &str, data: &[u8]) -> bool {
    match mime {
        "image/png" => data.starts_with(&[137, 80, 78, 71, 13, 10, 26, 10]),
        "image/jpeg" => data.starts_with(&[255, 216, 255]),
        "image/gif" => data.starts_with(b"GIF8"),
        "image/webp" => data.starts_with(b"RIFF") && data.get(8..12) == Some(b"WEBP"),
        _ => false,
    }
}
type Submission = (Value, Vec<(Value, Vec<u8>)>);
async fn submission(req: Request) -> Result<Submission, &'static str> {
    let mut fields = Multipart::from_request(req, &())
        .await
        .map_err(|_| "rejected")?;
    let mut values = json!({"text":""});
    let mut uploads = Vec::new();
    let mut count = 0;
    let mut total = 0;
    while let Some(field) = fields.next_field().await.map_err(|e| {
        if e.status() == StatusCode::PAYLOAD_TOO_LARGE {
            "too_large"
        } else {
            "rejected"
        }
    })? {
        count += 1;
        if count > 8 {
            return Err("too_many_files");
        }
        match field.name().unwrap_or_default() {
            "text" | "shareId" if field.file_name().is_none() => {
                let key = field.name().unwrap().to_owned();
                let text = field.text().await.map_err(|_| "too_large")?;
                if text.len() > 65536 {
                    return Err("too_large");
                }
                values[key] = json!(text.replace("\r\n", "\n"));
            }
            "file" if field.file_name().is_some() => {
                if uploads.len() == 4 {
                    return Err("too_many_files");
                }
                let name:String=field.file_name().unwrap().chars().map(|c|if c.is_control()||matches!(c,'/'|'\\'|'\u{061c}'|'\u{200e}'|'\u{200f}'|'\u{202a}'..='\u{202e}'|'\u{2066}'..='\u{2069}'){'_'}else{c}).take(255).collect();
                let mime = field
                    .content_type()
                    .unwrap_or("application/octet-stream")
                    .split(';')
                    .next()
                    .unwrap_or_default()
                    .trim()
                    .to_ascii_lowercase();
                let kind = if mime.starts_with("image/") {
                    "image"
                } else {
                    "file"
                };
                let data = field.bytes().await.map_err(|_| "too_large")?;
                total += data.len();
                if total > 10 * 1024 * 1024 {
                    return Err("too_large");
                }
                if data.is_empty() || (kind == "image" && !image_matches(&mime, &data)) {
                    return Err("unsupported_file");
                }
                let mime = if mime.is_empty() || mime.len() > 100 {
                    "application/octet-stream"
                } else {
                    &mime
                };
                let attachment = json!({"type":kind,"id":uuid::Uuid::new_v4().simple().to_string(),"name":if name.trim().is_empty(){kind}else{name.trim()},"mimeType":mime,"sizeBytes":data.len()});
                uploads.push((attachment, data.to_vec()));
            }
            _ => {}
        }
    }
    Ok((values, uploads))
}
async fn read(
    State(s): State<Arc<AppState>>,
    Query(q): Query<HashMap<String, String>>,
    req: Request,
) -> Response {
    let route = req.uri().path().rsplit('/').next().unwrap_or_default();
    let required = match route {
        "item" | "command-output" | "render" => Some("item"),
        "history" => Some("cursor"),
        "media" => Some("path"),
        "attachment" => Some("id"),
        _ => None,
    };
    if required.is_some_and(|key| q.get(key).is_none_or(|v| v.is_empty())) {
        return error("bad_request");
    }
    let token = session_token(req.headers(), &q);
    let share = match access(&s, token, false).await {
        Ok(v) => v,
        Err(e) => return error(e),
    };
    if q.get("share") != Some(&share.id) {
        return error("invalid");
    }
    match route {
        "session" => (StatusCode::NO_CONTENT, NO_STORE).into_response(),
        "stream" => stream(s, share, token.to_owned()).await,
        "attachment" => {
            let Some((meta, data)) = s.guest.attachment(&share, &q["id"]).await else {
                return error("unavailable");
            };
            let image = meta["type"] == "image";
            let mut response = Response::builder()
                .header("cache-control", "no-store")
                .header("x-content-type-options", "nosniff")
                .header("content-security-policy", "default-src 'none'; sandbox");
            if image {
                response = response.header(
                    "content-type",
                    meta["mimeType"]
                        .as_str()
                        .unwrap_or("application/octet-stream"),
                )
            } else {
                let name = meta["name"].as_str().unwrap_or("file");
                let encoded = url::form_urlencoded::byte_serialize(name.as_bytes())
                    .collect::<String>()
                    .replace('+', "%20");
                response = response
                    .header("content-type", "application/octet-stream")
                    .header(
                        "content-disposition",
                        format!("attachment; filename*=UTF-8''{encoded}"),
                    );
            }
            response.body(data.into()).unwrap()
        }
        _ => error("unavailable"),
    }
}
async fn view(state: &AppState, s: &Share) -> Value {
    let store = state.store.read().await;
    let thread = store.threads.get(&s.thread_id);
    let title = thread.map(|t| t.title.as_str()).unwrap_or_default();
    let model = thread
        .and_then(|t| t.runs.last())
        .map(|r| r.provider.as_str())
        .unwrap_or("Unknown");
    json!({"type":"view","view":{"shareId":s.id,"title":title,"guestName":s.name,"expiresAt":iso(s.expires),"messages":s.messages,"model":{"driver":null,"name":model}}})
}
async fn stream(state: Arc<AppState>, share: Share, token: String) -> Response {
    let changes = state.guest.changes();
    let initial = format!("data: {}\n\n", view(&state, &share).await);
    let events = futures_util::stream::unfold(
        (Some(initial), false, state, token, changes),
        |(initial, ended, state, token, mut changes)| async move {
            if ended {
                return None;
            }
            let mut end = false;
            let data = if let Some(initial) = initial {
                initial
            } else {
                let changed = tokio::time::timeout(Duration::from_secs(20), changes.changed())
                    .await
                    .is_ok();
                match access(&state, &token, false).await {
                    Ok(s) if changed => format!("data: {}\n\n", view(&state, &s).await),
                    Ok(_) => ": ping\n\n".into(),
                    Err(_) => {
                        end = true;
                        "data: {\"type\":\"ended\"}\n\n".into()
                    }
                }
            };
            Some((
                Ok::<_, std::convert::Infallible>(data),
                (None, end, state, token, changes),
            ))
        },
    );
    (
        [
            ("content-type", "text/event-stream"),
            ("cache-control", "no-store, no-transform"),
            ("x-accel-buffering", "no"),
        ],
        Body::from_stream(events),
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::guest_service;
    use tower::ServiceExt;

    #[tokio::test]
    async fn guest_attachments_require_redeemed_session_and_end_on_close() {
        let dir = tempfile::tempdir().unwrap();
        let state = AppState::new(dir.path(), vec![]).await.unwrap();
        let id = "opaque:guest";
        let thread:crate::Thread=serde_json::from_value(json!({
            "id":id,"project_id":"project","title":"demo","cwd":"/","status":"idle","messages":[],"runs":[],
            "created_at":0,"archived":false,"settled":false,"snoozed_until":null,"pinned":false,"unread":false,"tags":[]
        })).unwrap();
        state
            .store
            .write()
            .await
            .threads
            .insert(id.to_owned(), thread);
        let input = json!({"threadId":id});
        assert_eq!(
            guest_service::create(&state, &input).await.unwrap_err()["reason"],
            "disabled"
        );
        tokio::fs::write(
            dir.path().join("settings.json"),
            br#"{"guestSharingEnabled":true}"#,
        )
        .await
        .unwrap();
        assert_eq!(
            guest_service::create(&state, &input).await.unwrap_err()["reason"],
            "connect_unavailable"
        );
        state.guest.set_tunnel_hostname(Some("guest.test".into()));
        let invite = guest_service::create(&state, &input).await.unwrap();
        let share = invite["share"]["id"].as_str().unwrap();
        let token = invite["url"].as_str().unwrap().split_once('#').unwrap().1;
        let app = routes().with_state(state.clone());
        let redeem = || {
            app.clone().oneshot(
                Request::post("/api/guest/redeem")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        json!({"name":"Guest","credential":token}).to_string(),
                    ))
                    .unwrap(),
            )
        };
        let response = redeem().await.unwrap();
        assert_eq!(response.status(), 204);
        assert_eq!(redeem().await.unwrap().status(), 409);
        let cookie = response.headers()["set-cookie"]
            .to_str()
            .unwrap()
            .split(';')
            .next()
            .unwrap();
        let form=format!("--test\r\nContent-Disposition: form-data; name=\"shareId\"\r\n\r\n{share}\r\n--test\r\nContent-Disposition: form-data; name=\"file\"; filename=\"note.txt\"\r\nContent-Type: text/plain\r\n\r\nhello\r\n--test--\r\n");
        let post = |cookie: &str| {
            app.clone().oneshot(
                Request::post(format!("/api/guest/messages?share={share}"))
                    .header("content-type", "multipart/form-data; boundary=test")
                    .header("cookie", cookie)
                    .body(Body::from(form.clone()))
                    .unwrap(),
            )
        };
        assert_eq!(post("").await.unwrap().status(), 403);
        assert_eq!(post(cookie).await.unwrap().status(), 204);
        let projection = state.guest.projection(id).unwrap();
        let attachment = projection["messages"][0]["attachments"][0]["id"]
            .as_str()
            .unwrap();
        let read = |cookie: &str| {
            app.clone().oneshot(
                Request::get(format!(
                    "/api/guest/attachment?share={share}&id={attachment}"
                ))
                .header("cookie", cookie)
                .body(Body::empty())
                .unwrap(),
            )
        };
        let response = read(cookie).await.unwrap();
        assert_eq!(response.status(), 200);
        assert_eq!(
            response.headers()["content-type"],
            "application/octet-stream"
        );
        assert_eq!(
            to_bytes(response.into_body(), 30).await.unwrap().as_ref(),
            b"hello"
        );
        state.guest.close_thread(id).unwrap();
        assert_eq!(read(cookie).await.unwrap().status(), 403);
        assert!(!dir
            .path()
            .join("guest-attachments")
            .join(attachment)
            .exists());
    }
}
