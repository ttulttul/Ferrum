use super::DEFAULTS;
use crate::AppState;
use serde_json::{json, Value};
use std::{env, net::IpAddr, time::Duration};
use tokio::{net::TcpStream, time::timeout};

fn executable(name: &str) -> bool {
    crate::login_shell::path()
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
    let mut command = crate::login_shell::tokio_command(program);
    command.args(args).kill_on_drop(true);
    let out = timeout(Duration::from_secs(2), command.output())
        .await
        .ok()?
        .ok()?;
    out.status
        .success()
        .then(|| String::from_utf8_lossy(&out.stdout).trim().to_owned())
}
async fn editors() -> (Value, Option<&'static str>) {
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
async fn targets(hostname: &str) -> Value {
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

/// How long a discovered value is served before it is looked up again: the
/// reference's `EDITOR_DISCOVERY_CACHE_TTL` and `TARGETS_CACHE_TTL`.
const DISCOVERY_TTL: Duration = Duration::from_secs(60);
/// How long a background refresh may run before it is given up, keeping the
/// value it would have replaced (the reference's `REFRESH_TIMEOUT`).
const REFRESH_TIMEOUT: Duration = Duration::from_secs(30);

type Scan<T> = futures_util::future::Shared<futures_util::future::BoxFuture<'static, Option<T>>>;

enum Entry<T> {
    Empty,
    Scanning(Scan<T>),
    Ready {
        value: T,
        expires: std::time::Instant,
        refreshing: bool,
    },
}

/// A value looked up from the host, served as the reference's
/// `makeStaleWhileRevalidate` serves it. The first caller starts the lookup,
/// and every caller until it ends shares it, even if the first goes away.
/// Then the value is served from memory; once it is older than the TTL, the
/// next caller still gets it while one bounded lookup in the background
/// replaces it. A lookup that fails (here: panics or times out) keeps the
/// value it would have replaced, or leaves none to look up again.
pub(super) struct StaleWhileRevalidate<T> {
    ttl: Duration,
    entry: std::sync::Arc<std::sync::Mutex<Entry<T>>>,
}

impl<T: Clone + Send + Sync + 'static> StaleWhileRevalidate<T> {
    pub(super) fn new(ttl: Duration) -> Self {
        Self {
            ttl,
            entry: std::sync::Arc::new(std::sync::Mutex::new(Entry::Empty)),
        }
    }

    /// The value, looked up with `discover` when there is none yet. `None`
    /// only when the first lookup failed.
    pub(super) async fn get<F, Fut>(&self, discover: F) -> Option<T>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = T> + Send + 'static,
    {
        use futures_util::FutureExt;
        let scan = {
            let mut entry = self.entry.lock().unwrap();
            match &mut *entry {
                Entry::Empty => {
                    let task = tokio::spawn(self.settle_after(discover(), None));
                    let scan: Scan<T> = async move { task.await.ok().flatten() }.boxed().shared();
                    *entry = Entry::Scanning(scan.clone());
                    scan
                }
                Entry::Scanning(scan) => scan.clone(),
                Entry::Ready {
                    value,
                    expires,
                    refreshing,
                } => {
                    if !*refreshing && *expires <= std::time::Instant::now() {
                        *refreshing = true;
                        tokio::spawn(self.settle_after(discover(), Some(REFRESH_TIMEOUT)));
                    }
                    return Some(value.clone());
                }
            }
        };
        scan.await
    }

    /// Run a lookup and record what it found, within `limit` if given.
    fn settle_after(
        &self,
        discover: impl std::future::Future<Output = T> + Send + 'static,
        limit: Option<Duration>,
    ) -> impl std::future::Future<Output = Option<T>> + Send + 'static {
        use futures_util::FutureExt;
        let (entry, ttl) = (self.entry.clone(), self.ttl);
        async move {
            let found = std::panic::AssertUnwindSafe(async move {
                match limit {
                    Some(limit) => timeout(limit, discover).await.ok(),
                    None => Some(discover.await),
                }
            })
            .catch_unwind()
            .await
            .ok()
            .flatten();
            let mut entry = entry.lock().unwrap();
            *entry = match (found.clone(), std::mem::replace(&mut *entry, Entry::Empty)) {
                (Some(value), _) => Entry::Ready {
                    value,
                    expires: std::time::Instant::now() + ttl,
                    refreshing: false,
                },
                (None, Entry::Ready { value, expires, .. }) => Entry::Ready {
                    value,
                    expires,
                    refreshing: false,
                },
                (None, _) => Entry::Empty,
            };
            found
        }
    }
}

/// What `server.getConfig` looks up on the host, kept between calls as the
/// reference keeps it: the editors (with the file manager's reveal kind) and
/// the remote open targets for a minute each, and whether the base directory
/// is inside a git work tree once git has answered.
pub(crate) struct Discovery {
    editors: StaleWhileRevalidate<(Value, Option<&'static str>)>,
    targets: StaleWhileRevalidate<Value>,
    in_work_tree: tokio::sync::OnceCell<bool>,
}

impl Default for Discovery {
    fn default() -> Self {
        Self {
            editors: StaleWhileRevalidate::new(DISCOVERY_TTL),
            targets: StaleWhileRevalidate::new(DISCOVERY_TTL),
            in_work_tree: tokio::sync::OnceCell::new(),
        }
    }
}

impl Discovery {
    pub(super) async fn editors(&self) -> (Value, Option<&'static str>) {
        self.editors
            .get(editors)
            .await
            .unwrap_or_else(|| (json!([]), None))
    }

    pub(super) async fn targets(&self, hostname: &str) -> Value {
        let hostname = hostname.to_owned();
        self.targets
            .get(move || async move { targets(&hostname).await })
            .await
            .unwrap_or_else(|| json!([]))
    }

    /// Whether `dir` is inside a git work tree, asked once: the reference
    /// keeps the answer for good, and asks again only after a check that
    /// failed to give one.
    pub(super) async fn in_work_tree(&self, dir: &std::path::Path) -> bool {
        let dir = dir.to_owned();
        self.in_work_tree
            .get_or_try_init(|| async move {
                tokio::task::spawn_blocking(move || super::inside_git_work_tree(&dir))
                    .await
                    .map_err(|_| ())?
            })
            .await
            .copied()
            .unwrap_or(false)
    }
}

#[cfg(test)]
mod tests {
    use super::StaleWhileRevalidate;
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };
    use std::time::Duration;

    fn counting(
        calls: &Arc<AtomicUsize>,
        delay: Duration,
    ) -> impl FnOnce() -> futures_util::future::BoxFuture<'static, usize> {
        let calls = calls.clone();
        move || {
            use futures_util::FutureExt;
            async move {
                tokio::time::sleep(delay).await;
                calls.fetch_add(1, Ordering::SeqCst) + 1
            }
            .boxed()
        }
    }

    #[tokio::test]
    async fn callers_share_the_first_lookup_and_then_read_memory() {
        let cache = StaleWhileRevalidate::new(Duration::from_secs(60));
        let calls = Arc::new(AtomicUsize::new(0));
        let slow = Duration::from_millis(50);
        let (a, b) = tokio::join!(
            cache.get(counting(&calls, slow)),
            cache.get(counting(&calls, slow))
        );
        assert_eq!((a, b), (Some(1), Some(1)));
        assert_eq!(cache.get(counting(&calls, slow)).await, Some(1));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn an_old_value_is_served_while_it_refreshes() {
        let cache = StaleWhileRevalidate::new(Duration::from_millis(20));
        let calls = Arc::new(AtomicUsize::new(0));
        assert_eq!(cache.get(counting(&calls, Duration::ZERO)).await, Some(1));
        tokio::time::sleep(Duration::from_millis(40)).await;
        // Expired: the caller gets the old value at once, and one refresh starts.
        let refresh = Duration::from_millis(50);
        assert_eq!(cache.get(counting(&calls, refresh)).await, Some(1));
        assert_eq!(cache.get(counting(&calls, refresh)).await, Some(1));
        tokio::time::sleep(Duration::from_millis(120)).await;
        assert_eq!(cache.get(counting(&calls, refresh)).await, Some(2));
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn a_first_lookup_that_fails_is_tried_again() {
        let cache = StaleWhileRevalidate::new(Duration::from_secs(60));
        let failed = cache
            .get(|| async {
                panic!("lookup failed");
                #[allow(unreachable_code)]
                0usize
            })
            .await;
        assert_eq!(failed, None);
        assert_eq!(cache.get(|| async { 7usize }).await, Some(7));
    }
}
