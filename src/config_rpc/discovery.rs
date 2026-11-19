use super::DEFAULTS;
use crate::AppState;
use serde_json::{json, Value};
use std::{env, net::IpAddr, time::Duration};
use tokio::{net::TcpStream, process::Command, time::timeout};

fn executable(name: &str) -> bool {
    env::var_os("PATH")
        .into_iter()
        .flat_map(|v| env::split_paths(&v).collect::<Vec<_>>())
        .any(|d| {
            let p = d.join(name);
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                p.metadata()
                    .is_ok_and(|m| m.is_file() && m.permissions().mode() & 0o111 != 0)
            }
            #[cfg(windows)]
            {
                ["exe", "bat", "cmd"]
                    .iter()
                    .any(|e| p.with_extension(e).is_file())
            }
        })
}
async fn output(program: &str, args: &[&str]) -> Option<String> {
    let mut command = Command::new(program);
    command.args(args).kill_on_drop(true);
    let out = timeout(Duration::from_secs(2), command.output())
        .await
        .ok()?
        .ok()?;
    out.status
        .success()
        .then(|| String::from_utf8_lossy(&out.stdout).trim().to_owned())
}
pub(super) async fn editors() -> (Value, Option<&'static str>) {
    let reveal = if cfg!(target_os = "macos") && executable("open") {
        Some("finder")
    } else if cfg!(windows) && executable("explorer") {
        Some("file-explorer")
    } else if cfg!(target_os = "linux")
        && ["DISPLAY", "WAYLAND_DISPLAY"]
            .iter()
            .any(|k| env::var(k).is_ok_and(|v| !v.is_empty()))
        && executable("xdg-open")
        && output("xdg-mime", &["query", "default", "inode/directory"])
            .await
            .is_some_and(|s| !s.is_empty())
    {
        Some("files")
    } else {
        None
    };
    let available = DEFAULTS["editors"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|v| {
            if v["id"] == "file-manager" {
                return reveal.is_some();
            }
            v["commands"]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(Value::as_str)
                .any(executable)
        })
        .map(|v| v["id"].clone())
        .collect::<Vec<_>>();
    (available.into(), reveal)
}
async fn ssh(ip: IpAddr) -> bool {
    timeout(Duration::from_millis(300), TcpStream::connect((ip, 22)))
        .await
        .is_ok_and(|r| r.is_ok())
}
pub(super) async fn targets(hostname: &str) -> Value {
    let (ipv4, ipv6, status, user) = tokio::join!(
        ssh(std::net::Ipv4Addr::LOCALHOST.into()),
        ssh(std::net::Ipv6Addr::LOCALHOST.into()),
        output("tailscale", &["status", "--json"]),
        output("id", &["-un"])
    );
    let user = user
        .or_else(|| env::var("USERNAME").ok())
        .filter(|s| !s.is_empty());
    let status = status
        .and_then(|s| serde_json::from_str::<Value>(&s).ok())
        .unwrap_or(Value::Null);
    let mut result = Vec::new();
    let mut add = |kind: &str, host: &str| {
        let mut target = json!({"kind":kind,"host":host});
        if let Some(user) = &user {
            target["username"] = json!(user);
        }
        result.push(target);
    };
    if let Some(dns) = status["Self"]["DNSName"]
        .as_str()
        .map(|s| s.trim_end_matches('.'))
        .filter(|s| !s.is_empty() && status["BackendState"] == "Running")
    {
        let mut route = ipv4 || ipv6;
        if !route {
            for addr in status["TailscaleIPs"]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(|v| v.as_str()?.parse::<std::net::Ipv4Addr>().ok())
            {
                if ssh(addr.into()).await {
                    route = true;
                    break;
                }
            }
            if !route {
                route = output("tailscale", &["debug", "prefs"])
                    .await
                    .and_then(|s| serde_json::from_str::<Value>(&s).ok())
                    .is_some_and(|v| v["RunSSH"] == true);
            }
        }
        if route {
            add("tailscale", dns);
        }
    }
    if ipv4 || ipv6 {
        let short = hostname.split('.').next().unwrap_or("").trim();
        if !short.is_empty() {
            add("mdns", &format!("{short}.local"));
        }
    }
    result.into()
}
pub(super) async fn direct(state: &AppState) -> Value {
    let (Some(host), Some(port)) = (state.runtime_config.host, state.runtime_config.port) else {
        return json!([]);
    };
    if host.is_loopback() {
        return json!([]);
    }
    let interfaces = if_addrs::get_if_addrs().unwrap_or_default();
    let tailnet = interfaces
        .iter()
        .filter(|i| {
            i.name.to_ascii_lowercase().starts_with("tailscale")
                || matches!(i.ip(), IpAddr::V6(v) if v.segments()[..3]==[0xfd7a,0x115c,0xa1e0])
        })
        .map(|i| i.name.as_str())
        .collect::<std::collections::HashSet<_>>();
    let addresses = if host.is_unspecified() {
        interfaces
            .iter()
            .filter(|i| !i.is_loopback() && i.ip().is_ipv4())
            .filter(|i| {
                let name = &i.name;
                let virtual_prefix = [
                    "docker",
                    "br-",
                    "veth",
                    "virbr",
                    "vmnet",
                    "vboxnet",
                    "vEthernet",
                    "podman",
                    "cni",
                    "flannel",
                    "cali",
                    "lxcbr",
                    "lxdbr",
                ];
                let bridge = name.strip_prefix("bridge").is_some_and(|s| {
                    s.as_bytes()
                        .get(..3)
                        .is_some_and(|n| n[0] != b'0' && n.iter().all(u8::is_ascii_digit))
                });
                let container = cfg!(target_os = "linux")
                    && ["ve-", "vb-", "vz-"].iter().any(|p| name.starts_with(p));
                !(bridge || container || virtual_prefix.iter().any(|p| name.starts_with(p)))
            })
            .map(|i| (i.ip(), tailnet.contains(i.name.as_str())))
            .collect()
    } else {
        vec![(
            host,
            interfaces
                .iter()
                .any(|i| i.ip() == host && tailnet.contains(i.name.as_str())),
        )]
    };
    let mut result = Vec::new();
    for (ip, ts) in addresses {
        let addr = match ip {
            IpAddr::V4(v)
                if v.is_private()
                    || (v.octets()[0] == 100 && (64..128).contains(&v.octets()[1])) =>
            {
                ip.to_string()
            }
            IpAddr::V6(v) if v.segments()[..3] == [0xfd7a, 0x115c, 0xa1e0] => format!("[{v}]"),
            _ => continue,
        };
        let entry = json!({"kind":if ts{"tailnet"}else{"lan"},"httpBaseUrl":format!("http://{addr}:{port}/")});
        if !result.contains(&entry) {
            result.push(entry);
        }
    }
    result.into()
}
