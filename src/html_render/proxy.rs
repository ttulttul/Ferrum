//! The reference's `publicProxy`: a SOCKS5 proxy on loopback through which
//! the headless browser reaches the network. It connects only to public
//! addresses, checked after resolving (so DNS rebinding cannot reach a local
//! one), and refuses loopback, private, link-local, multicast and this
//! machine's own addresses, IPv4 carried in IPv6 included.
use std::collections::BTreeMap;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

const MAX_EARLY_BYTES: usize = 64 * 1024;
const MAX_FAKE_IP_HOSTS: usize = 3;

fn in_v4(address: Ipv4Addr, network: [u8; 4], prefix: u32) -> bool {
    let mask = if prefix == 0 {
        0
    } else {
        u32::MAX << (32 - prefix)
    };
    u32::from(address) & mask == u32::from(Ipv4Addr::from(network)) & mask
}

fn in_v6(address: Ipv6Addr, network: Ipv6Addr, prefix: u32) -> bool {
    let mask = if prefix == 0 {
        0
    } else {
        u128::MAX << (128 - prefix)
    };
    u128::from(address) & mask == u128::from(network) & mask
}

const LOCAL_V4: [([u8; 4], u32); 9] = [
    ([0, 0, 0, 0], 8),
    ([10, 0, 0, 0], 8),
    ([100, 64, 0, 0], 10),
    ([127, 0, 0, 0], 8),
    ([169, 254, 0, 0], 16),
    ([172, 16, 0, 0], 12),
    ([192, 168, 0, 0], 16),
    ([198, 18, 0, 0], 15),
    ([224, 0, 0, 0], 3),
];

fn local_v6() -> [(Ipv6Addr, u32); 6] {
    [
        (Ipv6Addr::UNSPECIFIED, 96),
        ("64:ff9b:1::".parse().unwrap(), 48),
        ("2001::".parse().unwrap(), 32),
        ("fc00::".parse().unwrap(), 7),
        ("fe80::".parse().unwrap(), 10),
        ("ff00::".parse().unwrap(), 8),
    ]
}

fn own(address: IpAddr) -> bool {
    if_addrs::get_if_addrs()
        .unwrap_or_default()
        .iter()
        .any(|i| i.ip() == address)
}

/// The IPv4 address a NAT64 (`64:ff9b::/96`) or 6to4 (`2002::/16`) address carries.
fn embedded_v4(address: Ipv6Addr) -> Option<Ipv4Addr> {
    let groups = address.segments();
    let at = if in_v6(address, "64:ff9b::".parse().unwrap(), 96) {
        6
    } else if in_v6(address, "2002::".parse().unwrap(), 16) {
        1
    } else {
        return None;
    };
    let (high, low) = (groups[at], groups[at + 1]);
    Some(Ipv4Addr::new(
        (high >> 8) as u8,
        high as u8,
        (low >> 8) as u8,
        low as u8,
    ))
}

fn is_local(address: IpAddr) -> bool {
    if own(address) {
        return true;
    }
    match address {
        IpAddr::V4(v4) => LOCAL_V4.iter().any(|(n, p)| in_v4(v4, *n, *p)),
        // An IPv4-mapped address is that IPv4 address.
        IpAddr::V6(v6) if v6.to_ipv4_mapped().is_some() => {
            is_local(IpAddr::V4(v6.to_ipv4_mapped().unwrap()))
        }
        IpAddr::V6(v6) => {
            local_v6().iter().any(|(n, p)| in_v6(v6, *n, *p))
                || embedded_v4(v6).is_some_and(|v4| is_local(IpAddr::V4(v4)))
        }
    }
}

/// The proxy, while it runs: its port and the hosts that fake-IP DNS answered.
pub(crate) struct Proxy {
    pub port: u16,
    pub fake_ip_hosts: Arc<Mutex<BTreeMap<usize, (String, String)>>>,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for Proxy {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl Proxy {
    /// The fake-IP hosts in the order they were seen.
    pub(crate) fn fake_ip_hosts(&self) -> Vec<(String, String)> {
        self.fake_ip_hosts
            .lock()
            .unwrap()
            .values()
            .cloned()
            .collect()
    }
}

pub(crate) async fn start() -> std::io::Result<Proxy> {
    let listener = TcpListener::bind(("127.0.0.1", 0)).await?;
    let port = listener.local_addr()?.port();
    let fake: Arc<Mutex<BTreeMap<usize, (String, String)>>> = Arc::default();
    let hosts = fake.clone();
    let task = tokio::spawn(async move {
        let mut connections = tokio::task::JoinSet::new();
        loop {
            let Ok((client, _)) = listener.accept().await else {
                break;
            };
            connections.spawn(serve(client, hosts.clone()));
            while connections.try_join_next().is_some() {}
        }
    });
    Ok(Proxy {
        port,
        fake_ip_hosts: fake,
        task,
    })
}

/// A SOCKS5 reply (RFC 1928).
fn reply(code: u8) -> [u8; 10] {
    [5, code, 0, 1, 0, 0, 0, 0, 0, 0]
}

/// The checked public addresses of `host`, or `None` when any is local.
async fn public_addresses(
    host: &str,
    port: u16,
    fake: &Mutex<BTreeMap<usize, (String, String)>>,
) -> Option<Vec<SocketAddr>> {
    let literal = host.parse::<IpAddr>().ok();
    let addresses: Vec<SocketAddr> = match literal {
        Some(ip) => vec![SocketAddr::new(ip, port)],
        None => tokio::net::lookup_host((host, port))
            .await
            .map(|a| a.collect())
            .unwrap_or_default(),
    };
    if addresses.is_empty() {
        return None;
    }
    if !addresses.iter().any(|a| is_local(a.ip())) {
        return Some(addresses);
    }
    let fake_ip = literal.is_none()
        && addresses
            .iter()
            .all(|a| matches!(a.ip(), IpAddr::V4(v4) if in_v4(v4, [198, 18, 0, 0], 15)));
    let mut seen = fake.lock().unwrap();
    if fake_ip && seen.len() < MAX_FAKE_IP_HOSTS && !seen.values().any(|(h, _)| h == host) {
        let order = seen.len();
        seen.insert(order, (host.to_owned(), addresses[0].ip().to_string()));
    }
    None
}

async fn serve(mut client: TcpStream, fake: Arc<Mutex<BTreeMap<usize, (String, String)>>>) {
    let mut data = Vec::new();
    let mut chunk = [0u8; 4096];
    // The greeting: version 5 and a list of methods, which must offer "no authentication".
    loop {
        if data.len() >= 2 && data.len() >= 2 + data[1] as usize {
            break;
        }
        match client.read(&mut chunk).await {
            Ok(0) | Err(_) => return,
            Ok(n) => data.extend_from_slice(&chunk[..n]),
        }
    }
    let methods = 2 + data[1] as usize;
    if data[0] != 5 || !data[2..methods].contains(&0) {
        let _ = client.write_all(&[5, 0xff]).await;
        return;
    }
    data.drain(..methods);
    if client.write_all(&[5, 0]).await.is_err() {
        return;
    }
    // The request: CONNECT to an IPv4 address, a name or an IPv6 address.
    let (command, host, port, end) = loop {
        if data.len() >= 5 {
            if data[0] != 5 || data[2] != 0 {
                let _ = client.write_all(&reply(7)).await;
                return;
            }
            let end = match data[3] {
                1 => 10,
                3 => 7 + data[4] as usize,
                4 => 22,
                _ => {
                    let _ = client.write_all(&reply(7)).await;
                    return;
                }
            };
            if data.len() >= end {
                let host = match data[3] {
                    1 => Ipv4Addr::new(data[4], data[5], data[6], data[7]).to_string(),
                    3 => data[5..5 + data[4] as usize]
                        .iter()
                        .map(|b| *b as char)
                        .collect(),
                    _ => {
                        let mut octets = [0u8; 16];
                        octets.copy_from_slice(&data[4..20]);
                        Ipv6Addr::from(octets).to_string()
                    }
                };
                let port = u16::from_be_bytes([data[end - 2], data[end - 1]]);
                break (data[1], host, port, end);
            }
        }
        match client.read(&mut chunk).await {
            Ok(0) | Err(_) => return,
            Ok(n) => data.extend_from_slice(&chunk[..n]),
        }
    };
    if command != 1 || port == 0 {
        let _ = client.write_all(&reply(7)).await;
        return;
    }
    let mut early = data[end..].to_vec();
    let Some(addresses) = public_addresses(&host, port, &fake).await else {
        let _ = client.write_all(&reply(2)).await;
        return;
    };
    // Bytes the browser sent before the reply wait, up to a bound.
    let mut upstream = None;
    for address in &addresses {
        if let Ok(stream) = TcpStream::connect(address).await {
            upstream = Some(stream);
            break;
        }
    }
    let Some(mut upstream) = upstream else {
        return;
    };
    if early.len() > MAX_EARLY_BYTES {
        return;
    }
    if client.write_all(&reply(0)).await.is_err() {
        return;
    }
    if !early.is_empty() && upstream.write_all(&early).await.is_err() {
        return;
    }
    early.clear();
    let _ = tokio::io::copy_bidirectional(&mut client, &mut upstream).await;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn local_and_carried_addresses_are_refused() {
        for address in [
            "127.0.0.1",
            "10.1.2.3",
            "192.168.1.1",
            "169.254.1.1",
            "100.64.0.1",
            "198.18.0.5",
            "::1",
            "fe80::1",
            "fd00::1",
            "::ffff:127.0.0.1",
            "64:ff9b::7f00:1",
            "2002:7f00:1::",
        ] {
            assert!(is_local(address.parse().unwrap()), "{address}");
        }
        for address in ["93.184.216.34", "2606:4700::1111", "64:ff9b::5db8:d822"] {
            assert!(!is_local(address.parse().unwrap()), "{address}");
        }
    }
}
