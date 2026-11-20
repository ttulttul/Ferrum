use super::TerminalService;
use super::{invalid, timestamp, Reply};
use serde_json::{json, Value};
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    time::Duration,
};
use tokio::process::Command;

struct Process {
    parent: u32,
    group: u32,
}
async fn process_table() -> HashMap<u32, Process> {
    let Ok(output) = Command::new("ps")
        .args(["-axo", "pid=,ppid=,pgid="])
        .kill_on_drop(true)
        .output()
        .await
    else {
        return HashMap::new();
    };
    String::from_utf8_lossy(&output.stdout)
        .lines()
        .filter_map(|l| {
            let mut n = l.split_whitespace().map(|s| s.parse::<u32>());
            Some((
                n.next()?.ok()?,
                Process {
                    parent: n.next()?.ok()?,
                    group: n.next()?.ok()?,
                },
            ))
        })
        .collect()
}
fn ancestors(table: &HashMap<u32, Process>, mut id: u32) -> HashSet<u32> {
    let mut ids = HashSet::new();
    while id > 1 && ids.insert(id) {
        let Some(p) = table.get(&id) else { break };
        id = p.parent;
    }
    ids
}
async fn listeners(
    terminals: &TerminalService,
    table: &HashMap<u32, Process>,
) -> BTreeMap<u16, Value> {
    let mut sockets = BTreeMap::new();
    let metadata = terminals.metadata_snapshot();
    let terminals: HashMap<u32, _> = metadata["terminals"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|t| {
            Some((
                u32::try_from(t["pid"].as_u64()?).ok()?,
                json!({"threadId":t["threadId"],"terminalId":t["terminalId"]}),
            ))
        })
        .collect();
    let Ok(Ok(output)) = tokio::time::timeout(
        Duration::from_secs(5),
        Command::new("lsof")
            .args(["-nP", "-iTCP", "-sTCP:LISTEN", "-Fpcn"])
            .kill_on_drop(true)
            .output(),
    )
    .await
    else {
        return sockets;
    };
    let mut pid = None;
    let mut name = None;
    let mut owner = None;
    for row in String::from_utf8_lossy(&output.stdout).lines() {
        if let Some(s) = row.strip_prefix('p') {
            pid = s.parse::<u32>().ok();
            name = None;
            owner = pid.and_then(|id| {
                ancestors(table, id)
                    .into_iter()
                    .find_map(|p| terminals.get(&p).cloned())
            });
        }
        if let Some(s) = row.strip_prefix('c') {
            name = Some(s.to_owned());
        }
        if let Some(s) = row.strip_prefix('n') {
            let Some((host, port)) = s.trim_end_matches(" (LISTEN)").rsplit_once(':') else {
                continue;
            };
            if !matches!(
                host,
                "*" | "127.0.0.1" | "0.0.0.0" | "[::1]" | "[::]" | "localhost"
            ) {
                continue;
            }
            let Ok(port) = port.parse::<u16>() else {
                continue;
            };
            let entry=sockets.entry(port).or_insert_with(||json!({"host":"localhost","port":port,"url":format!("http://localhost:{port}"),"processName":name,"pid":pid,"terminal":owner,"agent":null}));
            if entry["pid"] != json!(pid) {
                entry["terminal"] = Value::Null;
                entry["agent"] = Value::Null;
            }
        }
    }
    sockets
}
pub(crate) async fn scan(terminals: &TerminalService, input: &Value) -> Reply {
    let table = process_table().await;
    let sockets = listeners(terminals, &table).await;
    let mut servers: BTreeMap<_, _> = sockets
        .iter()
        .filter(|(_, v)| !v["terminal"].is_null())
        .map(|(p, r)| (*p, r.clone()))
        .collect();
    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(Duration::from_millis(750))
        .build()
        .unwrap();
    for value in input["configuredUrls"]
        .as_array()
        .into_iter()
        .flatten()
        .take(32)
    {
        let Some(raw) = value.as_str() else { continue };
        let Ok(url) = url::Url::parse(raw) else {
            continue;
        };
        let Some(port) = url.port_or_known_default() else {
            continue;
        };
        if !matches!(url.scheme(), "http" | "https")
            || !matches!(
                url.host_str(),
                Some("localhost" | "127.0.0.1" | "[::1]" | "0.0.0.0")
            )
        {
            continue;
        }
        if client.head(url.clone()).send().await.is_ok() {
            let mut server=sockets.get(&port).cloned().unwrap_or_else(||json!({"host":"localhost","port":port,"url":raw,"processName":null,"pid":null,"terminal":null,"agent":null}));
            server["url"] = json!(raw);
            servers.insert(port, server);
        }
    }
    Ok(
        json!({"servers":servers.into_values().collect::<Vec<_>>(),"scannedAt":timestamp(),"configuredUrlProbing":true}),
    )
}
pub(crate) async fn stop(terminals: &TerminalService, input: &Value) -> Reply {
    let err = |s: &str| json!({"_tag":"PreviewStopLocalServerError","reason":s});
    if cfg!(windows) {
        return Err(err("unsupported"));
    }
    let pid = input["pid"]
        .as_u64()
        .and_then(|n| u32::try_from(n).ok())
        .filter(|n| *n > 1)
        .ok_or_else(|| invalid("pid", "Expected a positive integer"))?;
    let port = input["port"]
        .as_u64()
        .and_then(|n| u16::try_from(n).ok())
        .filter(|n| *n != 0)
        .ok_or_else(|| invalid("port", "Expected a positive port"))?;
    let table = process_table().await;
    let sockets = listeners(terminals, &table).await;
    if !sockets
        .get(&port)
        .is_some_and(|s| s["pid"] == pid && !s["terminal"].is_null())
    {
        return Err(err("not-found"));
    }
    let group = table.get(&pid).ok_or_else(|| err("not-found"))?.group;
    let own = std::process::id();
    let protected = ancestors(&table, own);
    if group <= 1
        || table
            .iter()
            .any(|(p, r)| r.group == group && (protected.contains(p) || r.parent == own))
    {
        return Err(err("shared-group"));
    }
    let members: HashSet<_> = table
        .iter()
        .filter(|(_, r)| r.group == group)
        .map(|(p, _)| *p)
        .collect();
    let signal = |s: &'static str| async move {
        let status = Command::new("kill")
            .args([s, "--", &format!("-{group}")])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .await
            .map_err(|_| err("signal-failed"))?;
        if status.success() {
            Ok(())
        } else {
            Err(err("signal-failed"))
        }
    };
    signal("-TERM").await?;
    for _ in 0..20 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let current = process_table().await;
        if !members
            .iter()
            .any(|p| current.get(p).is_some_and(|r| r.group == group))
        {
            return Ok(Value::Null);
        }
    }
    signal("-KILL").await?;
    Ok(Value::Null)
}
