use super::*;
use tokio::sync::mpsc;

// Upstream queues every access change for each active RPC subscription.
#[derive(Default)]
pub(super) struct Subscribers {
    senders: Mutex<Vec<mpsc::UnboundedSender<Change>>>,
}
impl Subscribers {
    fn subscribe(&self) -> mpsc::UnboundedReceiver<Change> {
        let (tx, rx) = mpsc::unbounded_channel();
        let mut senders = self.senders.lock().unwrap();
        senders.retain(|s| !s.is_closed());
        senders.push(tx);
        rx
    }
    fn is_empty(&self) -> bool {
        let mut senders = self.senders.lock().unwrap();
        senders.retain(|s| !s.is_closed());
        senders.is_empty()
    }
    fn send(&self, change: Change) {
        self.senders
            .lock()
            .unwrap()
            .retain(|s| s.send(change.clone()).is_ok());
    }
}

#[derive(Clone)]
pub(crate) struct Change {
    kind: &'static str,
    payload: Value,
}
impl Change {
    pub(crate) fn event(mut self, current: &str, revision: u64) -> Value {
        if self.kind == "clientUpserted" {
            self.payload["current"] = json!(self.payload["sessionId"] == current);
        }
        if self.kind == "snapshot" {
            for row in self.payload["clientSessions"].as_array_mut().unwrap() {
                row["current"] = json!(row["sessionId"] == current);
            }
        }
        json!({"version":1,"revision":revision,"type":self.kind,"payload":self.payload})
    }
}
fn verification_code(thumb: &str) -> String {
    let alphabet = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    let code = thumb
        .bytes()
        .take(8)
        .map(|ch| {
            b"23456789ABCDEFGHJKLMNPQRSTUVWXYZ"
                [alphabet.iter().position(|c| *c == ch).unwrap_or(0) % 32] as char
        })
        .collect::<String>();
    let (a, b) = code.split_at(code.len().min(4));
    format!("{a}-{b}")
}
fn scopes(scopes: &[String]) -> Vec<&String> {
    scopes
        .iter()
        .filter(|s| LEGACY.contains(&s.as_str()))
        .collect()
}
fn link(g: &Grant) -> Value {
    let mut v = json!({"id":g.id,"scopes":scopes(&g.scopes),"permissions":g.scopes,"subject":if g.binding.is_some(){"cloud-connect"}else{"one-time-token"}});
    if let Some(label) = &g.label {
        v["label"] = json!(label);
    }
    v["createdAt"] = json!(iso(if g.created > 0 {
        g.created
    } else {
        g.expires - 300_000
    }));
    v["expiresAt"] = json!(iso(g.expires));
    if let Some(thumb) = g.binding.as_deref().filter(|_| g.pending_code.is_some()) {
        let mut req = json!({"verificationCode":verification_code(thumb)});
        if let Some(client) = &g.requested_client {
            req["client"] = client.clone();
        }
        v["connectDeviceRequest"] = req;
    }
    v
}
fn snapshot(r: &Credentials, at: i64) -> Value {
    let mut links = r
        .links
        .values()
        .filter(|g| {
            g.expires > at
                && g.access.is_none()
                && !(g.pending_code.is_some()
                    && g.binding.as_ref().is_some_and(|k| r.devices.contains(k)))
        })
        .collect::<Vec<_>>();
    links.sort_by_key(|g| std::cmp::Reverse((g.created, &g.id)));
    let mut sessions = r
        .sessions
        .values()
        .filter(|s| {
            s.renewable_until.unwrap_or(s.expires) > at || r.open_sockets.contains_key(&s.id)
        })
        .collect::<Vec<_>>();
    sessions.sort_by_key(|s| std::cmp::Reverse((s.issued, &s.id, s.expires)));
    sessions.dedup_by_key(|s| &s.id);
    let clients=sessions.into_iter().map(|s|{
        let ttl=if s.proof_key.is_some(){3_600_000}else{30*DAY};
        let issued=if s.issued>0{s.issued}else{s.expires-ttl};
        let subject=if s.owner{"desktop-bootstrap"}else if s.connect{"cloud-connect"}else{"one-time-token"};
        let client=if s.client.is_null(){json!({"deviceType":"unknown"})}else{s.client.clone()};
        json!({
            "sessionId":s.id,"subject":subject,"scopes":scopes(&s.permissions),"permissions":s.permissions,
            "method":s.method,"client":client,"issuedAt":iso(issued),"expiresAt":iso(s.renewable_until.unwrap_or(s.expires)),
            "lastConnectedAt":r.last_connected.get(&s.id).copied().map(iso),"connected":r.open_sockets.contains_key(&s.id),"current":false
        })
    }).collect::<Vec<_>>();
    let mut devices = r
        .devices
        .iter()
        .map(|thumb| {
            let (at, client) = r.device_rows.get(thumb).cloned().unwrap_or_default();
            let mut device =
                json!({"proofKeyThumbprint":thumb,"verificationCode":verification_code(thumb)});
            if let Some(client) = client {
                device["client"] = client;
            }
            device["approvedAt"] = json!(iso(at));
            (std::cmp::Reverse(at), thumb, device)
        })
        .collect::<Vec<_>>();
    devices.sort_by(|a, b| (a.0, a.1).cmp(&(b.0, b.1)));
    json!({"pairingLinks":links.into_iter().map(link).collect::<Vec<_>>(),"clientSessions":clients,"connectDevices":devices.into_iter().map(|d|d.2).collect::<Vec<_>>()})
}
impl AuthService {
    pub(super) fn update<R>(
        &self,
        edit: impl FnOnce(&mut Credentials) -> Result<R, &'static str>,
    ) -> Result<R, &'static str> {
        self.records.update_observed(edit, |previous, committed| {
            if self.changes.is_empty() {
                return;
            }
            let at = now();
            let before = snapshot(previous, at);
            let after = snapshot(committed, at);
            for (table, id, remove, upsert) in [
                (
                    "pairingLinks",
                    "id",
                    "pairingLinkRemoved",
                    "pairingLinkUpserted",
                ),
                (
                    "clientSessions",
                    "sessionId",
                    "clientRemoved",
                    "clientUpserted",
                ),
            ] {
                let old = before[table].as_array().unwrap();
                let new = after[table].as_array().unwrap();
                let old_by_id: HashMap<_, _> = old
                    .iter()
                    .map(|row| (row[id].as_str().unwrap(), row))
                    .collect();
                let new_ids: std::collections::HashSet<_> =
                    new.iter().map(|r| r[id].as_str().unwrap()).collect();
                for row in old
                    .iter()
                    .filter(|row| !new_ids.contains(row[id].as_str().unwrap()))
                {
                    self.changes.send(Change {
                        kind: remove,
                        payload: json!({id:row[id]}),
                    });
                }
                for row in new {
                    let row_id = row[id].as_str().unwrap();
                    let presence_changed = table == "clientSessions"
                        && previous.open_sockets.get(row_id) != committed.open_sockets.get(row_id);
                    if presence_changed || old_by_id.get(row_id).copied() != Some(row) {
                        self.changes.send(Change {
                            kind: upsert,
                            payload: row.clone(),
                        });
                    }
                }
                if table == "clientSessions" {
                    for session in previous.open_sockets.keys().filter(|s| {
                        !new_ids.contains(s.as_str())
                            && !old_by_id.contains_key(s.as_str())
                            && !committed.open_sockets.contains_key(*s)
                    }) {
                        self.changes.send(Change {
                            kind: remove,
                            payload: json!({id:session}),
                        });
                    }
                }
            }
            if before["connectDevices"] != after["connectDevices"] {
                self.changes.send(Change {
                    kind: "snapshot",
                    payload: after,
                });
            }
        })
    }
    pub(crate) fn subscribe_access(&self) -> (Change, mpsc::UnboundedReceiver<Change>) {
        self.records.read(|r| {
            (
                Change {
                    kind: "snapshot",
                    payload: snapshot(r, now()),
                },
                self.changes.subscribe(),
            )
        })
    }
    pub(crate) fn socket_presence(&self, id: &str, connected: bool) {
        let result = self.update(|r| {
            if connected && !r.sessions.values().any(|s| s.id == id) {
                return Ok(());
            }
            if connected {
                if !r.open_sockets.contains_key(id) {
                    r.last_connected.insert(id.to_owned(), now());
                }
                *r.open_sockets.entry(id.to_owned()).or_default() += 1;
            } else if let Some(count) = r.open_sockets.get_mut(id) {
                *count -= 1;
                if *count == 0 {
                    r.open_sockets.remove(id);
                }
            }
            Ok(())
        });
        if let Err(e) = result {
            tracing::warn!(error = e, "Failed to update client connection metadata");
        }
    }
}
