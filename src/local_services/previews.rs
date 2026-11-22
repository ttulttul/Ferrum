use super::{invalid, string, timestamp, Reply};
use serde_json::{json, Value};
use std::sync::Mutex;
use tokio::sync::broadcast;
use uuid::Uuid;

pub struct PreviewService {
    state: Mutex<State>,
    pub(super) browsers: tokio::sync::Mutex<super::browser::Browsers>,
    events: broadcast::Sender<Value>,
    epoch: Uuid,
}

#[derive(Default)]
struct State {
    tabs: Vec<Value>,
    sequence: u64,
    revision: u64,
}

impl Default for PreviewService {
    fn default() -> Self {
        Self {
            state: Default::default(),
            browsers: Default::default(),
            events: broadcast::channel(512).0,
            epoch: Uuid::new_v4(),
        }
    }
}

fn url_error(raw: &str, reason: &str, protocol: Option<&str>) -> Value {
    let length = raw.encode_utf16().count();
    let suffix = protocol.map(|p| format!(": {p}")).unwrap_or_default();
    let mut error = json!({"_tag":"PreviewInvalidUrlError","inputLength":length,"reason":reason});
    if let Some(p) = protocol {
        error["protocol"] = json!(p);
    }
    error["cause"] = json!({"name":"PreviewUrlNormalizationError","message":format!("Invalid preview URL ({reason}{suffix}; input length {length}). Use an http(s) URL or a schemeless host such as localhost:5173. With preview_open, omit url when creating a new tab to open a blank tab.")});
    error
}

pub(super) fn normalize_url(raw: &str) -> Result<String, Value> {
    let protocol = raw
        .split_once(':')
        .map(|(p, _)| format!("{}:", p.to_ascii_lowercase()));
    if matches!(
        protocol.as_deref(),
        Some(
            "about:"
                | "blob:"
                | "chrome:"
                | "data:"
                | "file:"
                | "ftp:"
                | "javascript:"
                | "mailto:"
                | "sms:"
                | "ssh:"
                | "tel:"
                | "view-source:"
                | "ws:"
                | "wss:"
        )
    ) {
        return Err(url_error(raw, "unsupported-protocol", protocol.as_deref()));
    }
    let lower = raw.to_ascii_lowercase();
    let local = ["localhost", "127.0.0.1", "0.0.0.0", "[::1]", "[::]"]
        .iter()
        .any(|host| {
            lower
                .strip_prefix(host)
                .is_some_and(|s| s.is_empty() || s.starts_with([':', '/']))
        });
    let candidate = if raw.contains("://") {
        raw.to_owned()
    } else {
        format!("{}://{raw}", if local { "http" } else { "https" })
    };
    let scheme = candidate
        .split_once(':')
        .map(|(p, _)| format!("{}:", p.to_ascii_lowercase()));
    let url =
        url::Url::parse(&candidate).map_err(|_| url_error(raw, "parse", scheme.as_deref()))?;
    if !matches!(url.scheme(), "http" | "https") {
        return Err(url_error(raw, "unsupported-protocol", scheme.as_deref()));
    }
    Ok(url.to_string())
}
fn base36(mut n: u64) -> String {
    let mut digits = Vec::new();
    while n > 0 {
        digits.push(char::from_digit((n % 36) as u32, 36).unwrap());
        n /= 36;
    }
    digits.into_iter().rev().collect()
}

impl PreviewService {
    pub fn subscribe(&self) -> broadcast::Receiver<Value> {
        self.events.subscribe()
    }
    fn publish(&self, state: &mut State, kind: &str, session: &Value, request: Option<Value>) {
        state.revision += 1;
        let mut event = json!({"threadId":session["threadId"],"tabId":session["tabId"],"createdAt":timestamp(),"serverEpoch":self.epoch,"revision":state.revision,"type":kind});
        if kind == "failed" {
            for key in ["url", "title", "code", "description", "download"] {
                if let Some(v) = session["navStatus"].get(key) {
                    event[key] = v.clone();
                }
            }
        } else if kind != "closed" {
            event["snapshot"] = session.clone();
        }
        if let Some(v) = request {
            event["request"] = v;
        }
        let _ = self.events.send(event);
    }
    pub fn call(&self, name: &str, input: &Value) -> Option<Reply> {
        if !matches!(
            name,
            "open"
                | "close"
                | "list"
                | "resize"
                | "adjust"
                | "reportStatus"
                | "navigate"
                | "setColorScheme"
        ) {
            return None;
        }
        Some(self.apply(name, input))
    }
    fn apply(&self, name: &str, input: &Value) -> Reply {
        let thread = string(input, "threadId")?;
        let mut state = self.state.lock().unwrap();
        if name == "list" {
            let mut sessions: Vec<_> = state
                .tabs
                .iter()
                .filter(|tab| tab["threadId"] == thread)
                .collect();
            sessions.sort_by_key(|tab| tab["updatedAt"].as_str());
            return Ok(
                json!({"sessions":sessions,"serverEpoch":self.epoch,"revision":state.revision}),
            );
        }
        if name == "open" {
            state.sequence += 1;
            let mut id = format!("tab_{}", base36(state.sequence));
            if input["runtime"] == "server" {
                id.push_str(&format!("_{}", self.epoch));
            }
            let nav = if input.get("url").is_some() {
                json!({"_tag":"Loading","url":normalize_url(string(input,"url")?)?,"title":""})
            } else {
                json!({"_tag":"Idle"})
            };
            let mut tab = json!({"threadId":thread,"tabId":id,"navStatus":nav,"canGoBack":false,"canGoForward":false,"viewport":input.get("viewport").cloned().unwrap_or(json!({"_tag":"fill"}))});
            for key in [
                "colorScheme",
                "zoomFactor",
                "profileId",
                "runtime",
                "reveal",
            ] {
                if let Some(v) = input.get(key) {
                    tab[key] = v.clone();
                }
            }
            tab["updatedAt"] = json!(timestamp());
            state.tabs.push(tab.clone());
            self.publish(&mut state, "opened", &tab, None);
            return Ok(tab);
        }
        if name == "close" {
            let id = input
                .get("tabId")
                .map(|_| string(input, "tabId"))
                .transpose()?;
            let mut i = 0;
            while i < state.tabs.len() {
                let tab = &state.tabs[i];
                if tab["threadId"] == thread && id.is_none_or(|id| tab["tabId"] == id) {
                    let tab = state.tabs.remove(i);
                    self.publish(&mut state, "closed", &tab, None);
                } else {
                    i += 1;
                }
            }
            return Ok(Value::Null);
        }
        let id = string(input, "tabId")?;
        let tab = state
            .tabs
            .iter_mut()
            .find(|tab| tab["threadId"] == thread && tab["tabId"] == id)
            .ok_or_else(
                || json!({"_tag":"PreviewSessionLookupError","threadId":thread,"tabId":id}),
            )?;
        let mut kind = "resized";
        let mut request = None;
        match name {
            "navigate" => {
                let url = normalize_url(string(input, "url")?)?;
                tab["navStatus"] = json!({"_tag":"Loading","url":url,"title":""});
                kind = "navigated";
            }
            "setColorScheme" => {
                tab["colorScheme"] = input
                    .get("colorScheme")
                    .ok_or_else(|| invalid("colorScheme", "Missing key"))?
                    .clone();
                kind = "adjusted";
            }
            "reportStatus" => {
                if tab["runtime"] == "server" {
                    return Err(json!({"_tag":"PreviewControlRequiredError","tabId":id}));
                }
                for key in ["navStatus", "canGoBack", "canGoForward"] {
                    tab[key] = input
                        .get(key)
                        .ok_or_else(|| invalid(key, "Missing key"))?
                        .clone();
                }
                kind = if tab["navStatus"]["_tag"] == "LoadFailed" {
                    "failed"
                } else {
                    "navigated"
                };
            }
            "resize" => {
                tab["viewport"] = input
                    .get("viewport")
                    .ok_or_else(|| invalid("viewport", "Missing key"))?
                    .clone();
            }
            "adjust" => {
                for key in ["colorScheme", "zoomFactor"] {
                    if let Some(v) = input.get(key) {
                        tab[key] = v.clone();
                    }
                }
                let mut payload = json!({});
                if input["hardReload"] == true {
                    payload["hardReload"] = json!(true);
                }
                if let Some(v) = input.get("clear") {
                    payload["clear"] = v.clone();
                }
                if !payload.as_object().unwrap().is_empty() {
                    request = Some(payload);
                }
            }
            _ => unreachable!(),
        }
        tab["updatedAt"] = json!(timestamp());
        let result = tab.clone();
        self.publish(&mut state, kind, &result, request);
        Ok(if name == "reportStatus" {
            Value::Null
        } else {
            result
        })
    }
}

impl PreviewService {
    pub fn tab(&self, thread: &str, id: &str) -> Option<Value> {
        self.state
            .lock()
            .unwrap()
            .tabs
            .iter()
            .find(|t| t["threadId"] == thread && t["tabId"] == id)
            .cloned()
    }
    pub fn profile(&self, method: &str, input: &Value, base: &std::path::Path) -> Reply {
        let directory = base.join("server-browser");
        if method == "clearProfile" {
            let id = string(input, "profileId")?;
            if id == "incognito" {
                return Ok(Value::Null);
            }
            let err = std::fs::remove_dir_all(profile_directory(base, id));
            if let Err(e) = err {
                if e.kind() != std::io::ErrorKind::NotFound {
                    return Err(
                        json!({"_tag":"PreviewClearProfileError","profileId":id,"cause":{"name":"Error","message":e.to_string()}}),
                    );
                }
            }
        } else {
            let mut seen = std::collections::HashSet::from(["default", "incognito"]);
            let mut profiles = Vec::new();
            let default = string(input, "defaultProfileId")?;
            let array = input
                .get("profiles")
                .ok_or_else(|| invalid("profiles", "Missing key"))?
                .as_array()
                .ok_or_else(|| invalid("profiles", "Expected array"))?;
            for p in array {
                let id = string(p, "id")?;
                if seen.insert(id) {
                    profiles.push(json!({"id":id,"name":string(p,"name")?,"kind":"persistent"}));
                }
            }
            let encoded=json!({"profiles":profiles,"defaultProfileId":if seen.contains(default){default}else{"default"}}).to_string();
            std::fs::create_dir_all(&directory)
                .map_err(|e| json!({"_tag":"Die","defect":e.to_string()}))?;
            let temp = directory.join(format!("reported-profiles-{}.tmp", Uuid::new_v4()));
            std::fs::write(&temp, encoded)
                .and_then(|_| std::fs::rename(temp, directory.join("reported-profiles.json")))
                .map_err(|e| json!({"_tag":"Die","defect":e.to_string()}))?;
        }
        Ok(Value::Null)
    }
}

pub(super) fn profile_directory(base: &std::path::Path, id: &str) -> std::path::PathBuf {
    let encoded: String = id
        .bytes()
        .map(|b| {
            if b.is_ascii_alphanumeric() || (b"-_!~*'().".contains(&b) && id != "." && id != "..") {
                char::from(b).to_string()
            } else {
                format!("%{b:02X}")
            }
        })
        .collect();
    base.join("server-browser/profiles").join(encoded)
}
impl PreviewService {
    pub(super) fn page_status(&self, thread: &str, id: &str, nav: Value) {
        let mut state = self.state.lock().unwrap();
        let Some(s) = state
            .tabs
            .iter_mut()
            .find(|s| s["threadId"] == thread && s["tabId"] == id)
        else {
            return;
        };
        if s["navStatus"] == nav {
            return;
        }
        s["navStatus"] = nav;
        s["updatedAt"] = json!(timestamp());
        let s = s.clone();
        self.publish(&mut state, "navigated", &s, None);
    }
    pub async fn close_pages(
        &self,
        thread: Option<&str>,
        tab: Option<&str>,
        profile: Option<&str>,
    ) {
        self.browsers.lock().await.close(thread, tab, profile).await;
    }
}
