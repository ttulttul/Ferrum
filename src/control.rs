//! The control plane: what a hosted environment runs without a workspace.
//!
//! It owns the auth service, the orchestration event log and its in-memory
//! store, and answers HTTP routes and WebSocket RPCs that need nothing from a
//! host. Workspace calls (files, git, terminals, provider processes) belong to
//! the workspace plane, `t3-agentd`, and are not wired up yet; they answer as
//! unavailable.
//!
//! Unlike the native socket loop (`rpc::session`), the RPC handler here is
//! message driven: [`ControlPlane::on_message`] takes one text frame and
//! returns the frames to send. Open streams are recorded in a serializable
//! [`Connection`], so a Durable Object can keep it as a WebSocket attachment
//! and hibernate between messages. Pushing later stream values (for example
//! store changes) goes through [`ControlPlane::commit`] and the host's list of
//! connections; this slice does not push any yet.
//!
//! Platform neutral: the native build compiles and tests it too.
use crate::{
    access_http::{self, AuthHost},
    auth_service::{self, AuthService, Session},
    contracts::RPC_CONTRACTS,
    environment,
    event_log::EventLog,
    rpc_protocol::{js_template_string, request_key, split_socket_frame, Reply},
    rpc_protocol::{SOCKET_FRAME_PARTS_QUERY_PARAM, SOCKET_FRAME_PART_BYTES},
    server_config, Event, ServerEvent, Store,
};
use axum::{
    extract::{Query, State},
    http::{HeaderMap, StatusCode, Uri},
    response::{IntoResponse, Response},
    routing::get,
    Json, Router,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::{
    collections::{BTreeMap, HashMap},
    sync::{Arc, Mutex},
};

/// Where a hosted environment's workspace lives inside the `t3-agentd` container.
pub const WORKSPACE_ROOT: &str = "/workspace";

pub struct Config {
    pub environment_id: String,
    pub label: String,
    /// The desktop bootstrap credential the pairing exchange accepts.
    pub bootstrap_credential: String,
    /// The port in the session cookie name (`t3_session_<port>`).
    pub cookie_port: u16,
}

struct Log {
    store: Store,
    sequence: u64,
    log: Box<dyn EventLog + Send>,
}

pub struct ControlPlane {
    config: Config,
    started_at: String,
    auth: AuthService,
    log: Mutex<Log>,
}
impl AuthHost for ControlPlane {
    fn auth(&self) -> &AuthService {
        &self.auth
    }
}

/// One client socket: who it is and which streams it has open. Serializable,
/// so a host can keep it with the socket while the process sleeps.
#[derive(Clone, Serialize, Deserialize)]
pub struct Connection {
    pub principal: Session,
    pub frame_parts: bool,
    pub references: bool,
    /// Open streams by request key, with the id the client used.
    open: BTreeMap<String, OpenStream>,
}
#[derive(Clone, Serialize, Deserialize)]
struct OpenStream {
    id: Value,
    method: String,
}

impl ControlPlane {
    /// Rebuilds the store from `log`. `auth_records` is what
    /// [`auth_records`](Self::auth_records) returned before a restart.
    pub fn open(
        config: Config,
        auth_records: Option<&str>,
        mut log: Box<dyn EventLog + Send>,
    ) -> anyhow::Result<Self> {
        let mut auth = AuthService::with_bootstrap(config.bootstrap_credential.clone())
            .with_port(config.cookie_port);
        if let Some(records) = auth_records {
            auth = auth.with_records_json(records)?;
        }
        let mut store = Store::default();
        let mut sequence = 0;
        for record in log.load()? {
            sequence = sequence.max(record.id);
            store.apply_record(&record);
        }
        Ok(Self {
            config,
            started_at: auth_service::iso(auth_service::now()),
            auth,
            log: Mutex::new(Log {
                store,
                sequence,
                log,
            }),
        })
    }

    /// The auth service's sessions and grants, for the host to persist after
    /// any request that may have changed them.
    pub fn auth_records(&self) -> String {
        self.auth.records_json()
    }

    pub fn sequence(&self) -> u64 {
        self.log.lock().unwrap().sequence
    }

    /// Appends one event to the log, then applies it to the store.
    pub fn commit(&self, event: Event) -> anyhow::Result<ServerEvent> {
        let mut guard = self.log.lock().unwrap();
        let record = ServerEvent {
            id: guard.sequence + 1,
            at: crate::now(),
            event,
            receipt: None,
        };
        guard.log.append(&record)?;
        guard.sequence = record.id;
        guard.store.apply_record(&record);
        Ok(record)
    }

    pub fn descriptor(&self) -> Value {
        environment::descriptor(
            &self.config.environment_id,
            &self.config.label,
            ("linux", "x64"),
            true,
        )
    }

    /// HTTP routes the control plane answers. `/ws` is the host's: it
    /// upgrades with [`authorize_socket`](Self::authorize_socket) and then
    /// feeds frames to [`on_message`](Self::on_message).
    pub fn router(self: &Arc<Self>) -> Router {
        Router::new()
            .route("/.well-known/t3/environment", get(descriptor))
            .route("/api/orchestration/shell", get(shell_snapshot))
            .merge(access_http::auth_routes::<ControlPlane>())
            .fallback(unavailable)
            .layer(axum::middleware::from_fn(access_http::contextualize))
            .layer(axum::middleware::from_fn(crate::cors::browser_api))
            .with_state(self.clone())
    }

    /// Checks a `/ws` upgrade as the native server does: protocol 2, then a
    /// `wsTicket` or a session credential. On success returns the connection
    /// to keep with the socket.
    #[allow(clippy::result_large_err)] // the rejection is the HTTP response to send
    pub fn authorize_socket(&self, headers: &HeaderMap, uri: &Uri) -> Result<Connection, Response> {
        let query = url::form_urlencoded::parse(uri.query().unwrap_or("").as_bytes());
        if !query
            .clone()
            .any(|(key, value)| key == "orchestrationProtocol" && value == "2")
        {
            let body = json!({
                "code": "orchestration_protocol_incompatible",
                "message": "Update this client to one that supports orchestration protocol 2.",
                "orchestrationProtocolVersion": 2
            });
            return Err((
                StatusCode::UPGRADE_REQUIRED,
                [
                    ("vary", "Accept-Encoding"),
                    ("access-control-allow-origin", "*"),
                ],
                Json(body),
            )
                .into_response());
        }
        let ticket = query
            .into_iter()
            .filter(|(key, _)| key == "wsTicket")
            .last()
            .map(|(_, value)| value.into_owned())
            .filter(|v| !v.trim().is_empty());
        let principal = match ticket {
            Some(ticket) => access_http::authenticate_ticket(self, headers, uri, &ticket, "rpc"),
            None => access_http::principal(self, headers),
        }
        .map_err(|reason| crate::auth_error(headers, reason))?;
        let references = query
            .clone()
            .find(|(k, _)| k == "clientCapabilities")
            .is_some_and(|(_, v)| v.split(',').any(|c| c == "settingsSecretReferences"));
        let frame_parts = query
            .clone()
            .any(|(key, _)| key == SOCKET_FRAME_PARTS_QUERY_PARAM);
        self.auth.socket_presence(&principal.id, true);
        Ok(Connection {
            principal,
            frame_parts,
            references,
            open: BTreeMap::new(),
        })
    }

    pub fn disconnect(&self, connection: &Connection) {
        self.auth.socket_presence(&connection.principal.id, false);
    }

    /// Handles one text frame from a client and returns the frames to send.
    pub fn on_message(&self, connection: &mut Connection, text: &str) -> Vec<String> {
        let Ok(request) = serde_json::from_str::<Value>(text) else {
            return Vec::new();
        };
        let frames = match request["_tag"].as_str() {
            Some("Ping") => vec![json!({"_tag":"Pong"})],
            // Every stream here sends its values at once, so there is no
            // backpressure window to reopen.
            Some("Ack") => Vec::new(),
            Some("Interrupt") => request_key(&request["requestId"])
                .and_then(|key| connection.open.remove(&key))
                .map(|stream| Reply::Failure(json!({"_tag":"Interrupt"})).frame(&stream.id))
                .into_iter()
                .collect(),
            Some("Request") => self.request(connection, &request),
            other => vec![
                json!({"_tag":"Defect","defect":format!("Unknown request tag: {}", other.unwrap_or("undefined"))}),
            ],
        };
        frames
            .iter()
            .flat_map(|frame| {
                let text = crate::wire_json::compact(frame);
                if connection.frame_parts {
                    split_socket_frame(&text, SOCKET_FRAME_PART_BYTES)
                } else {
                    vec![text]
                }
            })
            .collect()
    }

    fn request(&self, connection: &mut Connection, request: &Value) -> Vec<Value> {
        let id = request.get("id").cloned().unwrap_or(Value::Null);
        let Some(key) = request_key(&id) else {
            let shown = if request.get("id").is_some() {
                js_template_string(&id)
            } else {
                "undefined".into()
            };
            return vec![json!({"_tag":"Defect","defect":format!("Invalid request id: {shown}")})];
        };
        if connection.open.contains_key(&key) {
            return Vec::new();
        }
        let method = request["tag"].as_str().unwrap_or("undefined");
        let params = &request["payload"];
        match self.call(&connection.principal, connection.references, method, params) {
            Call::Done(reply) => vec![reply.frame(&id)],
            Call::Stream(values) => {
                connection.open.insert(
                    key,
                    OpenStream {
                        id: id.clone(),
                        method: method.to_owned(),
                    },
                );
                values
                    .into_iter()
                    .map(|batch| Reply::Chunk(batch).frame(&id))
                    .collect()
            }
        }
    }

    fn call(&self, principal: &Session, references: bool, method: &str, params: &Value) -> Call {
        if let Some(contract) = RPC_CONTRACTS.iter().find(|c| c.tag == method) {
            if let Some(key) = contract
                .required
                .into_iter()
                .flatten()
                .find(|key| params.get(**key).is_none())
            {
                return Call::Done(Reply::defect(format!("Missing key\n  at [{key:?}]")));
            }
            if let Some(scope) = auth_service::permissions::missing_rpc(principal, contract, params)
            {
                let legacy = auth_service::permissions::legacy_scope(scope);
                return Call::Done(Reply::Failure(
                    json!({"_tag":"Fail","error":{"_tag":"EnvironmentAuthorizationError","message":format!("The authenticated token is missing required scope: {scope}."),"requiredScope":legacy,"requiredPermission":scope}}),
                ));
            }
        } else {
            return Call::Done(Reply::defect(format!("Unknown request tag: {method}")));
        }
        let _ = references;
        match method {
            "server.probe" => Call::Done(Reply::Success(json!({}))),
            "subscribeServerConfig" => {
                let mut batches = vec![
                    json!([{"version":1,"type":"snapshot","config":self.config_snapshot(params)}]),
                ];
                // As the native server: report empty theme and usage-limit sources to
                // clients that opted in.
                if params["usageLimitSources"] == true {
                    batches.push(json!([{"version":1,"type":"usageLimitSourcesUpdated","payload":{"sources":[]}}]));
                }
                if params["environmentThemes"] == true {
                    batches.push(json!([{"version":1,"type":"environmentThemesUpdated","payload":{"themes":[]}}]));
                }
                Call::Stream(batches)
            }
            "server.getConfig" => Call::Done(Reply::Success(self.config_snapshot(&Value::Null))),
            "server.refreshProviders" => Call::Done(Reply::Success(
                json!({"providers":self.config_snapshot(&Value::Null)["providers"]}),
            )),
            "subscribeServerLifecycle" => {
                let environment = self.descriptor();
                Call::Stream(vec![json!([
                    {"version":1,"sequence":1,"type":"welcome","payload":{"environment":environment,"cwd":WORKSPACE_ROOT,"projectName":"workspace"}},
                    {"version":1,"sequence":2,"type":"ready","payload":{"at":self.started_at,"environment":environment}}
                ])])
            }
            "subscribeAuthAccess" => {
                let (change, _) = self.auth.subscribe_access();
                Call::Stream(vec![json!([change.event(&principal.id, 1)])])
            }
            "subscribeProjectClones" => Call::Stream(vec![json!([[]])]),
            "subscribeDeviceState" => Call::Stream(vec![json!([{
                "supportsHostRetry": true, "supportsToolUpdate": true, "supportsToolInspection": true,
                "supportsEasHosts": true, "supportsSimlockHosts": true, "hostStatus": "disabled",
                "hostStatuses": {}, "devices": [], "sessions": [], "onboardingCompleted": false,
                "agentAccessEnabled": false, "hubBasePath": "/api/device-hub", "revision": 0, "hosts": []
            }])]),
            "server.reportClientActivity" => {
                if !matches!(
                    params["clientKind"].as_str(),
                    Some("web" | "desktop-renderer" | "mobile" | "unknown")
                ) {
                    return Call::Done(Reply::defect(
                        "Expected \"web\" | \"desktop-renderer\" | \"mobile\" | \"unknown\"\n  at [\"clientKind\"]".into(),
                    ));
                }
                Call::Done(Reply::Success(Value::Null))
            }
            "preview.reportProfiles" => Call::Done(Reply::Success(Value::Null)),
            "projects.mutate" => Call::Done(self.create_project(params)),
            "orchestration.subscribeShell" => {
                let guard = self.log.lock().unwrap();
                let sequence = guard.sequence;
                let after = params.get("afterSequence").filter(|value| !value.is_null());
                let resumable = after.and_then(Value::as_u64) == Some(sequence);
                if after.is_some() && !resumable && params["httpSnapshotFallback"] == true {
                    return Call::Stream(vec![json!([{"kind":"snapshot-required"}])]);
                }
                let mut items = Vec::new();
                if !resumable {
                    items.push(json!({"kind":"snapshot","snapshot":environment::shell(&guard.store, sequence)}));
                }
                if params["requestCompletionMarker"] == true {
                    items.push(json!({"kind":"synchronized"}));
                }
                Call::Stream(if items.is_empty() {
                    Vec::new()
                } else {
                    vec![Value::Array(items)]
                })
            }
            "orchestration.subscribeThread" => {
                let guard = self.log.lock().unwrap();
                let sequence = guard.sequence;
                Call::Stream(vec![match params["threadId"]
                    .as_str()
                    .and_then(|id| guard.store.threads.get(id))
                {
                    Some(t) => {
                        json!([{"kind":"snapshot","snapshotSequence":sequence,"projection":t.projection()},{"kind":"synchronized"}])
                    }
                    None => json!([]),
                }])
            }
            _ => Call::Done(Reply::defect(format!(
                "RPC contract is registered but unavailable in the control plane: {method}"
            ))),
        }
    }

    /// `projects.mutate` with `project.create` only. The native server first
    /// creates and canonicalizes the workspace root; that check belongs to the
    /// workspace plane, so the root is recorded as given.
    fn create_project(&self, input: &Value) -> Reply {
        let command = input["commandId"].as_str().unwrap_or_default();
        let id = input["projectId"].as_str().unwrap_or_default();
        let error = |msg: &str| {
            Reply::Failure(
                json!({"_tag":"Fail","error":{"_tag":"ProjectMutationError","commandId":command,"message":msg}}),
            )
        };
        if input["type"] != "project.create" {
            return error("Only project.create is available in the control plane.");
        }
        let (Some(title), Some(root)) = (
            input["title"]
                .as_str()
                .map(str::trim)
                .filter(|s| !s.is_empty()),
            input["workspaceRoot"]
                .as_str()
                .map(str::trim)
                .filter(|s| !s.is_empty()),
        ) else {
            return error("A title and a workspace root are required.");
        };
        if command.trim().is_empty()
            || id.trim().is_empty()
            || self.log.lock().unwrap().store.project_rows.contains_key(id)
        {
            return error("Failed to mutate project.");
        }
        let project = crate::Project {
            id: id.into(),
            name: title.into(),
            path: root.into(),
            created_at: crate::now(),
        };
        let row = crate::store::wire_project(&project);
        match self.commit(Event::ProjectCreated(project)) {
            Ok(_) => Reply::Success(row),
            Err(_) => error("Failed to mutate project."),
        }
    }

    /// The `subscribeServerConfig` snapshot. Host facts describe the
    /// workspace container; settings are the defaults until settings move into
    /// the control plane's storage.
    fn config_snapshot(&self, options: &Value) -> Value {
        let settings = default_settings();
        let (keybindings, issues) = server_config::keybindings::resolve(Ok(None));
        let mut config = json!({
            "environment": self.descriptor(),
            "auth": self.auth.descriptor(),
            "cwd": WORKSPACE_ROOT,
            "hostname": self.config.label,
            "keybindingsConfigPath": "/home/t3/.t3/userdata/keybindings.json",
            "keybindings": keybindings,
            "issues": issues,
            "providers": server_config::providers::list_with(&self.started_at, |_| None, &settings, options),
            "availableEditors": [],
            "remoteOpenTargets": [],
            "directEndpoints": [],
            "observability": {"logsDirectoryPath":"/home/t3/.t3/userdata/logs","localTracingEnabled":true,"otlpTracesEnabled":false,"otlpMetricsEnabled":false,"otlpLogsEnabled":false},
            "settings": settings,
        });
        for key in [
            "shellResumeCompletionMarker",
            "shellThreadFamilies",
            "threadLinkMetadata",
            "shellOlderSettled",
            "threadResumeCompletionMarker",
            "threadSnapshotPagination",
            "threadFind",
            "threadFindProgressive",
            "secretProviderSettings",
            "secretReferences",
            "secretRequestReferences",
        ] {
            config[key] = true.into();
        }
        config["scratchWorkspaceRoot"] = json!(format!("{WORKSPACE_ROOT}/scratch"));
        config["newProjectsRoot"] = json!(format!("{WORKSPACE_ROOT}/projects"));
        config["serverTime"] = json!(auth_service::iso(auth_service::now()));
        config
    }
}

enum Call {
    Done(Reply),
    /// Stream batches sent now; the stream stays open until interrupted.
    Stream(Vec<Value>),
}

fn default_settings() -> Value {
    use crate::settings::schema;
    schema::normalize(&json!({}))
        .and_then(|settings| schema::normalize_runtime(&settings))
        .expect("default settings normalize")
}

async fn descriptor(State(plane): State<Arc<ControlPlane>>) -> Json<Value> {
    Json(plane.descriptor())
}

async fn shell_snapshot(
    State(plane): State<Arc<ControlPlane>>,
    headers: HeaderMap,
    Query(query): Query<HashMap<String, String>>,
) -> Response {
    let session = match access_http::principal(&*plane, &headers) {
        Ok(session) => session,
        Err(reason) => return crate::auth_error(&headers, reason),
    };
    for scope in auth_service::permissions::http_scopes("/api/orchestration/shell", "GET") {
        if !session.has(scope) {
            return access_http::scope_error(&headers, scope);
        }
    }
    if headers
        .get("x-t3-orchestration-protocol")
        .is_none_or(|v| v != "2")
    {
        return StatusCode::BAD_REQUEST.into_response();
    }
    let guard = plane.log.lock().unwrap();
    let sequence = guard.sequence;
    if query.get("resumeFrom").and_then(|v| v.parse::<u64>().ok()) == Some(sequence) {
        return Json(json!({"resumable":true})).into_response();
    }
    Json(environment::shell_snapshot(&guard.store, sequence, &query)).into_response()
}

async fn unavailable(uri: Uri) -> Response {
    (
        StatusCode::NOT_IMPLEMENTED,
        Json(json!({
            "_tag": "EnvironmentHttpUnavailableError",
            "message": format!("{} is not served by the control plane yet.", uri.path())
        })),
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event_log::MemoryLog;

    fn plane() -> ControlPlane {
        ControlPlane::open(
            Config {
                environment_id: "env-test".into(),
                label: "test".into(),
                bootstrap_credential: "secret".into(),
                cookie_port: 3773,
            },
            None,
            Box::new(MemoryLog::default()),
        )
        .unwrap()
    }

    fn owner(plane: &ControlPlane) -> Connection {
        let (_, token) = plane.auth.browser_session("secret", None).unwrap();
        let mut headers = HeaderMap::new();
        headers.insert(
            "cookie",
            format!("t3_session_3773={token}").parse().unwrap(),
        );
        plane
            .authorize_socket(&headers, &"/ws?orchestrationProtocol=2".parse().unwrap())
            .map_err(|_| "rejected")
            .unwrap()
    }

    fn send(plane: &ControlPlane, connection: &mut Connection, frame: Value) -> Vec<Value> {
        plane
            .on_message(connection, &frame.to_string())
            .iter()
            .map(|text| serde_json::from_str(text).unwrap())
            .collect()
    }

    #[test]
    fn answers_the_probe_and_streams_until_interrupted() {
        let plane = plane();
        let mut connection = owner(&plane);
        let probe = send(
            &plane,
            &mut connection,
            json!({"_tag":"Request","id":0,"tag":"server.probe","payload":{},"headers":[]}),
        );
        assert_eq!(
            probe,
            vec![json!({"_tag":"Exit","requestId":0,"exit":{"_tag":"Success","value":{}}})]
        );
        let shell = send(
            &plane,
            &mut connection,
            json!({"_tag":"Request","id":1,"tag":"orchestration.subscribeShell","payload":{"requestCompletionMarker":true},"headers":[]}),
        );
        assert_eq!(shell[0]["values"][1], json!({"kind":"synchronized"}));
        let interrupted = send(
            &plane,
            &mut connection,
            json!({"_tag":"Interrupt","requestId":1}),
        );
        assert_eq!(interrupted[0]["exit"]["cause"][0]["_tag"], "Interrupt");
    }

    #[test]
    fn rejects_sockets_without_a_credential() {
        let plane = plane();
        assert!(plane
            .authorize_socket(
                &HeaderMap::new(),
                &"/ws?orchestrationProtocol=2".parse().unwrap()
            )
            .is_err());
    }

    #[test]
    fn rebuilds_the_store_from_its_log() {
        let plane = plane();
        plane
            .commit(Event::ProjectCreated(crate::Project {
                id: "p1".into(),
                name: "One".into(),
                path: "/workspace/one".into(),
                created_at: 1,
            }))
            .unwrap();
        let records = plane.log.lock().unwrap().log.load().unwrap();
        let mut log = MemoryLog::default();
        for record in &records {
            log.append(record).unwrap();
        }
        let reopened = ControlPlane::open(
            Config {
                environment_id: "env-test".into(),
                label: "test".into(),
                bootstrap_credential: "secret".into(),
                cookie_port: 3773,
            },
            Some(&plane.auth_records()),
            Box::new(log),
        )
        .unwrap();
        assert_eq!(reopened.sequence(), 1);
        assert!(reopened
            .log
            .lock()
            .unwrap()
            .store
            .projects
            .contains_key("p1"));
    }
}
