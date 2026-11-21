use super::*;
use std::{
    io,
    net::{IpAddr, SocketAddr},
    process::{Output, Stdio},
    time::Duration,
};

#[derive(Debug, PartialEq)]
struct Route {
    port: u16,
    mount: Option<String>,
    host: Option<String>,
    ambiguous: Option<String>,
    owner: Option<String>,
}
fn route_destination(proxy: &str, listen: SocketAddr) -> Option<Option<String>> {
    if proxy.starts_with("unix:") {
        return None;
    }
    let url = if proxy.bytes().all(|b| b.is_ascii_digit()) {
        format!("http://127.0.0.1:{proxy}")
    } else if let Some(rest) = proxy.strip_prefix("https+insecure://") {
        format!("https://{rest}")
    } else if !proxy.contains("://") {
        format!("http://{proxy}")
    } else {
        proxy.to_owned()
    };
    let (scheme, rest) = url.split_once("://")?;
    if !matches!(scheme, "http" | "https" | "tcp") {
        return None;
    }
    let authority = rest.split(['/', '?', '#']).next()?.rsplit('@').next()?;
    let (host, port) = if authority.starts_with('[') {
        let (ip, suffix) = authority.split_once(']')?;
        (
            ip.trim_start_matches('['),
            suffix.strip_prefix(':').unwrap_or(""),
        )
    } else {
        authority.rsplit_once(':').unwrap_or((authority, ""))
    };
    let port = if port.is_empty() {
        if scheme == "https" {
            443
        } else {
            80
        }
    } else {
        port.parse().ok()?
    };
    if port != listen.port() {
        return None;
    }
    let Ok(ip) = host.parse::<IpAddr>() else {
        return Some(Some(host.into()));
    };
    let ip = ip.to_canonical();
    if ip.is_ipv6() && ip.is_unspecified() && listen.ip().is_loopback() {
        return Some(Some(host.into()));
    }
    let exact = if ip.is_ipv4() && ip.is_unspecified() {
        listen.ip() == std::net::Ipv4Addr::LOCALHOST
    } else {
        ip == listen.ip()
    };
    exact.then_some(None)
}
fn routes(config: &Value, listen: SocketAddr) -> Result<Vec<Route>> {
    fn scan(
        v: &Value,
        owner: Option<&str>,
        listen: SocketAddr,
        result: &mut Vec<Route>,
    ) -> Result<()> {
        object(v)?;
        if let Some(tcp) = v.get("TCP") {
            for (port, entry) in object(tcp)? {
                object(entry)?;
                if let Some(dest) = entry.get("TCPForward") {
                    let dest = dest.as_str().context("unreadable TCP forward")?;
                    if let (Ok(port), Some(ambiguous)) = (
                        port.parse(),
                        route_destination(&format!("tcp://{dest}"), listen),
                    ) {
                        result.push(Route {
                            port,
                            ambiguous,
                            mount: None,
                            host: None,
                            owner: owner.map(str::to_owned),
                        });
                    }
                }
            }
        }
        if let Some(web) = v.get("Web") {
            for (address, site) in object(web)? {
                object(site)?;
                if let Some(handlers) = site.get("Handlers") {
                    for (mount, entry) in object(handlers)? {
                        object(entry)?;
                        if let Some(dest) = entry.get("Proxy") {
                            let dest = dest.as_str().context("unreadable proxy")?;
                            let Some((host, port)) = address.rsplit_once(':') else {
                                continue;
                            };
                            if let (Ok(port), Some(ambiguous)) =
                                (port.parse(), route_destination(dest, listen))
                            {
                                result.push(Route {
                                    port,
                                    ambiguous,
                                    mount: Some(mount.clone()),
                                    host: Some(host.into()),
                                    owner: owner.map(str::to_owned),
                                });
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    }
    let mut result = Vec::new();
    if config.is_null() {
        return Ok(result);
    }
    scan(config, None, listen, &mut result)?;
    for (key, is_service) in [("Services", true), ("Foreground", false)] {
        if let Some(nodes) = config.get(key) {
            for (name, node) in object(nodes)? {
                scan(
                    node,
                    Some(if is_service {
                        name.as_str()
                    } else {
                        "foreground"
                    }),
                    listen,
                    &mut result,
                )?;
            }
        }
    }
    Ok(result)
}
async fn cli(args: &[&str]) -> io::Result<Output> {
    tokio::time::timeout(
        Duration::from_secs(4),
        crate::login_shell::tokio_command("tailscale")
            .args(args)
            .stdin(Stdio::null())
            .kill_on_drop(true)
            .output(),
    )
    .await?
}
async fn read(listen: SocketAddr) -> Result<Vec<Route>> {
    let output = match cli(&["serve", "status", "--json"]).await {
        Ok(output) => output,
        Err(e)
            if matches!(
                e.kind(),
                io::ErrorKind::NotFound | io::ErrorKind::NotADirectory
            ) =>
        {
            return Ok(Vec::new())
        }
        Err(e) => return Err(e.into()),
    };
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr).to_ascii_lowercase();
        let absent = [
            "not logged in",
            "logged out",
            "need login",
            "needs login",
            "doesn't appear to be running",
            "tailscaled.service not running",
            "tailscale is stopped",
            "tailscaled: the system cannot find the file specified",
        ]
        .iter()
        .any(|s| stderr.contains(s))
            || stderr.contains("failed to connect to local tailscale")
                && [
                    "connection refused",
                    "no such file or directory",
                    "cannot find the file",
                ]
                .iter()
                .any(|s| stderr.contains(s));
        if absent {
            return Ok(Vec::new());
        }
        anyhow::bail!("Tailscale Serve status unavailable")
    }
    routes(&serde_json::from_slice(&output.stdout)?, listen)
}

impl Policy {
    pub(crate) async fn close_tailscale_routes(&self, listen: SocketAddr) -> Result<()> {
        if self.allows_client("remote") {
            return Ok(());
        }
        let refusal = self.denial("remote clients");
        let refusal = refusal.trim_end_matches('.');
        let _=std::net::TcpListener::bind(listen).map_err(|_|anyhow!("{refusal}, so the server does not start while it cannot bind {listen}; if another app holds that port, the server would take over its Tailscale Serve routes."))?;
        let fetch = || async {
            read(listen).await.map_err(|_| {
                anyhow!(
                    "{refusal}, so the server does not start until Tailscale Serve can be checked."
                )
            })
        };
        let found = fetch().await?;
        if found.is_empty() {
            return Ok(());
        }
        let host = if found.iter().any(|r| r.mount.is_some() && r.owner.is_none()) {
            cli(&["status", "--json"])
                .await
                .ok()
                .filter(|o| o.status.success())
                .and_then(|o| serde_json::from_slice::<Value>(&o.stdout).ok())
                .and_then(|v| {
                    v["Self"]["DNSName"]
                        .as_str()
                        .map(|s| s.trim().trim_end_matches('.').to_ascii_lowercase())
                })
        } else {
            None
        };
        for route in &found {
            if route.ambiguous.is_some() || route.owner.is_some() {
                continue;
            }
            if let Some(mount) = &route.mount {
                if host
                    .as_ref()
                    .zip(route.host.as_ref())
                    .is_some_and(|(a, b)| a.eq_ignore_ascii_case(b))
                {
                    let _ = cli(&[
                        "serve",
                        &format!("--https={}", route.port),
                        &format!("--set-path={mount}"),
                        "off",
                    ])
                    .await;
                }
            } else {
                let _ = cli(&["serve", &format!("--tcp={}", route.port), "off"]).await;
            }
        }
        let left = fetch().await?;
        if !left.is_empty() {
            let list = left
                .iter()
                .map(|r| {
                    format!(
                        "port {}{} here",
                        r.port,
                        r.mount
                            .as_ref()
                            .map(|m| format!(" at {m}"))
                            .unwrap_or_default()
                    )
                })
                .collect::<Vec<_>>()
                .join(", and ");
            anyhow::bail!(
                "{refusal}, so the server does not start while Tailscale Serve forwards {list}."
            );
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn serve_status_identifies_foreground_ambiguous_and_exact_routes() {
        let routes=routes(&json!({"TCP":{"8030":{"TCPForward":"[::ffff:127.0.0.1]:3773"},"8031":{"TCPForward":"127.0.0.1:1000"}},"Foreground":{"session":{"Web":{"host:443":{"Handlers":{"/t":{"Proxy":"http://app:3773/"},"/else":{"Proxy":"unix:/tmp/app.sock"}}}}}}}),"127.0.0.1:3773".parse().unwrap()).unwrap();
        assert_eq!(routes.len(), 2);
        assert_eq!(routes[0].port, 8030);
        assert_eq!(routes[0].ambiguous, None);
        assert_eq!(routes[1].owner.as_deref(), Some("foreground"));
        assert_eq!(routes[1].ambiguous.as_deref(), Some("app"));
        assert!(super::routes(
            &json!({"Web":{"bad":{"Handlers":[]}}}),
            "127.0.0.1:3773".parse().unwrap()
        )
        .is_err());
    }
}
