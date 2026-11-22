//! Resolve SSH aliases with the same OpenSSH configuration used for the eventual connection.
use futures_util::{stream, StreamExt};
use serde_json::Value;
use std::{
    collections::{HashMap, HashSet},
    net::IpAddr,
    process::Stdio,
    time::Duration,
};
use tokio::{net::lookup_host, time::timeout};

fn addresses() -> HashSet<IpAddr> {
    if_addrs::get_if_addrs()
        .unwrap_or_default()
        .into_iter()
        .map(|i| i.ip())
        .collect()
}

async fn is_local(host: &Value, local: &HashSet<IpAddr>) -> bool {
    let Some(target) = host["target"].as_str() else {
        return false;
    };
    let mut command = crate::login_shell::tokio_command("ssh");
    command.args([
        "-o",
        "BatchMode=yes",
        "-o",
        "ConnectTimeout=10",
        "-o",
        "RemoteCommand=none",
        "-o",
        "RequestTTY=no",
    ]);
    if let Some(port) = host["port"].as_u64() {
        command.arg("-p").arg(port.to_string());
    }
    command.args([
        "-o",
        "ServerAliveInterval=15",
        "-o",
        "ServerAliveCountMax=3",
        "-G",
    ]);
    if let Some(path) = host["identityFile"].as_str() {
        command.arg("-i").arg(path);
    }
    command
        .arg("--")
        .arg(target)
        .stdin(Stdio::null())
        .stderr(Stdio::null())
        .kill_on_drop(true);
    let Ok(Ok(output)) = timeout(Duration::from_secs(5), command.output()).await else {
        return false;
    };
    if !output.status.success() {
        return false;
    }
    let text = String::from_utf8_lossy(&output.stdout);
    let config: HashMap<&str, &str> = text
        .lines()
        .filter_map(|line| line.split_once(' '))
        .map(|(key, value)| (key, value.trim()))
        .collect();
    if config.get("port") != Some(&"22")
        || ["proxycommand", "proxyjump"]
            .iter()
            .any(|key| config.get(key).is_some_and(|value| *value != "none"))
    {
        return false;
    }
    let Some(hostname) = config.get("hostname") else {
        return false;
    };
    let hostname = hostname.trim_start_matches('[').trim_end_matches(']');
    if hostname.is_empty() {
        return false;
    }
    let is_self = |ip: IpAddr| {
        let ip = ip.to_canonical();
        ip.is_loopback() || local.contains(&ip)
    };
    if let Ok(ip) = hostname.parse::<IpAddr>() {
        return is_self(ip);
    }
    let Ok(Ok(mut resolved)) = timeout(Duration::from_secs(2), lookup_host((hostname, 0))).await
    else {
        return false;
    };
    resolved.next().is_some_and(|first| is_self(first.ip())) && resolved.all(|a| is_self(a.ip()))
}

pub(super) async fn local_target(host: &Value) -> bool {
    is_local(host, &addresses()).await
}

pub(crate) async fn retain_remote(hosts: &mut Vec<Value>) {
    let local = addresses();
    let all = std::mem::take(hosts);
    *hosts = stream::iter(all)
        .map(|host| {
            let local = &local;
            async move {
                let drop = host["kind"] == "ssh" && is_local(&host, local).await;
                (!drop).then_some(host)
            }
        })
        .buffered(4)
        .filter_map(std::future::ready)
        .collect()
        .await;
}
