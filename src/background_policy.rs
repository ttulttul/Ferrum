//! Whether background work may run, as the reference's `BackgroundPolicy`
//! (`background/BackgroundPolicy.ts`): clients report their activity
//! (`server.reportClientActivity`) as leases that name the scopes they are
//! watching and expire after a TTL, and scoped work such as a periodic
//! provider status check runs only while a client holds a live lease on its
//! scope (a foreground one, unless the `performance` profile is chosen) and
//! the host is not constrained.
//!
//! Leases are keyed by the auth session and the client's own id, and end only
//! when they expire: the reference's per-connection `removeRpcClient`
//! finalizer runs as soon as the socket's handlers are built, not when the
//! socket closes, and every socket's RPC client id is 0 (each socket has its
//! own RPC protocol), so in effect a session holds at most 16 leases and a
//! closed client's report lasts out its TTL.
//!
//! Host power comes from the desktop app's telemetry in the reference. This
//! server has no desktop telemetry receiver, so its host power snapshot stays
//! the reference's initial one, `unknown` and stale, which never constrains
//! work, as with a reference server that no desktop app is attached to.
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

const DEFAULT_LEASE_TTL_MS: f64 = 45_000.0;
const MAX_LEASE_TTL_MS: f64 = 120_000.0;
const MIN_LEASE_TTL_MS: f64 = 1_000.0;
const MAX_CLIENT_ACTIVITY_LEASES_PER_RPC_CLIENT: usize = 16;

/// One client's activity report (`ClientActivityLease`).
#[derive(Clone, Debug)]
struct Lease {
    visible: bool,
    focused: bool,
    recently_interacted: bool,
    low_power_mode: Option<String>,
    scope_keys: Vec<String>,
    updated_at: Instant,
    expires_at: Instant,
}

impl Lease {
    fn active(&self, now: Instant) -> bool {
        self.expires_at > now
    }

    /// `isForegroundLease`.
    fn foreground(&self, now: Instant) -> bool {
        self.active(now) && self.visible && (self.focused || self.recently_interacted)
    }
}

/// `leaseKey`: the auth session and the client's own id (the RPC client id
/// between them is always 0).
type LeaseKey = (String, String);

/// `HostPowerSnapshot`'s fields the policy reads.
#[derive(Clone, Debug)]
pub(crate) struct HostPower {
    pub locked: String,
    pub suspended: bool,
    pub on_battery: String,
    pub low_power_mode: String,
    pub thermal_state: String,
    pub stale: bool,
}

impl HostPower {
    /// The reference's `makeUnknownSnapshot`.
    fn unknown() -> Self {
        HostPower {
            locked: "unknown".into(),
            suspended: false,
            on_battery: "unknown".into(),
            low_power_mode: "unknown".into(),
            thermal_state: "unknown".into(),
            stale: true,
        }
    }
}

/// `ResolvedBackgroundActivitySettings`' fields the policy reads.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct Activity {
    pub performance: bool,
    pub pause_when_host_locked: bool,
    pub pause_when_host_low_power: bool,
    pub pause_when_client_low_power: bool,
    pub pause_when_on_battery: bool,
}

/// `resolveServerBackgroundActivitySettings`: the profile's preset with a
/// custom profile's overrides.
pub(crate) fn resolve_activity(settings: &Value) -> Activity {
    let background = &settings["backgroundActivity"];
    let profile = match background["profile"].as_str() {
        Some("custom") => background["baseProfile"].as_str().unwrap_or("balanced"),
        Some(profile) => profile,
        None => "balanced",
    };
    let (performance, locked, host_low_power, client_low_power, battery) = match profile {
        "performance" => (true, true, false, false, false),
        "battery-saver" => (false, true, true, true, true),
        _ => (false, true, true, true, false),
    };
    let overrides = if background["profile"] == "custom" {
        &background["overrides"]
    } else {
        &Value::Null
    };
    let flag = |key: &str, preset: bool| overrides[key].as_bool().unwrap_or(preset);
    Activity {
        performance,
        pause_when_host_locked: flag("pauseWhenHostLocked", locked),
        pause_when_host_low_power: flag("pauseWhenHostLowPower", host_low_power),
        pause_when_client_low_power: flag("pauseWhenClientLowPower", client_low_power),
        pause_when_on_battery: flag("pauseWhenOnBattery", battery),
    }
}

/// `isHostConstrained`.
fn host_constrained(host: &HostPower, activity: Activity) -> bool {
    if host.stale {
        return false;
    }
    let thermal = matches!(host.thermal_state.as_str(), "serious" | "critical");
    if host.suspended || (activity.pause_when_host_locked && host.locked == "true") || thermal {
        return true;
    }
    if activity.pause_when_host_low_power && host.low_power_mode == "true" {
        return true;
    }
    activity.pause_when_on_battery && host.on_battery == "true"
}

/// `isClientConstrained`.
fn client_constrained(lease: &Lease, activity: Activity) -> bool {
    activity.pause_when_client_low_power && lease.low_power_mode.as_deref() == Some("true")
}

/// A background scope (`BackgroundScope`) as its `scopeKey`, or `None` when
/// it is not one the contract accepts.
pub(crate) fn scope_key(scope: &Value) -> Option<String> {
    let text = |key: &str| scope.get(key).and_then(Value::as_str);
    match text("type")? {
        kind @ ("server-config" | "diagnostics") => Some(kind.to_owned()),
        "provider-status" => match scope.get("instanceId") {
            None => Some("provider-status".into()),
            Some(Value::String(id)) if id.is_empty() => Some("provider-status".into()),
            Some(Value::String(id)) => Some(format!("provider-status:{id}")),
            Some(_) => None,
        },
        kind @ ("vcs-status" | "git-refs") => Some(format!("{kind}:{}", text("cwd")?)),
        "thread" => Some(format!("thread:{}", text("threadId")?)),
        _ => None,
    }
}

/// The decoded parts of a `ClientActivityReportInput`.
struct Report {
    client_id: String,
    visible: bool,
    focused: bool,
    recently_interacted: bool,
    low_power_mode: Option<String>,
    scope_keys: Vec<String>,
    ttl_ms: Option<f64>,
}

fn decode_report(input: &Value) -> Option<Report> {
    let client_id = input.get("clientId")?.as_str()?.trim();
    if client_id.is_empty() || client_id.encode_utf16().count() > 128 {
        return None;
    }
    let flag = |key: &str| input.get(key)?.as_bool();
    let low_power_mode = match input.get("lowPowerMode") {
        None => None,
        Some(Value::String(state)) if matches!(state.as_str(), "true" | "false" | "unknown") => {
            Some(state.clone())
        }
        Some(_) => return None,
    };
    let scope_keys = input
        .get("scopes")?
        .as_array()?
        .iter()
        .map(scope_key)
        .collect::<Option<Vec<_>>>()?;
    let ttl_ms = match input.get("ttlMs") {
        None => None,
        Some(ttl) => Some(ttl.as_f64()?),
    };
    Some(Report {
        client_id: client_id.to_owned(),
        visible: flag("visible")?,
        focused: flag("focused")?,
        recently_interacted: flag("recentlyInteracted")?,
        low_power_mode,
        scope_keys,
        ttl_ms,
    })
}

/// The server's background policy: client activity leases and host power.
pub(crate) struct BackgroundPolicy {
    leases: Mutex<HashMap<LeaseKey, Lease>>,
    host_power: Mutex<HostPower>,
}

impl Default for BackgroundPolicy {
    fn default() -> Self {
        BackgroundPolicy {
            leases: Mutex::new(HashMap::new()),
            host_power: Mutex::new(HostPower::unknown()),
        }
    }
}

impl BackgroundPolicy {
    /// `reportClientActivity`. An input the contract would reject records nothing.
    pub(crate) fn report_client_activity(&self, session_id: &str, input: &Value) {
        let Some(report) = decode_report(input) else {
            return;
        };
        let ttl = report
            .ttl_ms
            .unwrap_or(DEFAULT_LEASE_TTL_MS)
            .clamp(MIN_LEASE_TTL_MS, MAX_LEASE_TTL_MS);
        let now = Instant::now();
        let lease = Lease {
            visible: report.visible,
            focused: report.focused,
            recently_interacted: report.recently_interacted,
            low_power_mode: report.low_power_mode,
            scope_keys: report.scope_keys,
            updated_at: now,
            expires_at: now + Duration::from_millis(ttl as u64),
        };
        let key = (session_id.to_owned(), report.client_id);
        let mut leases = self.leases.lock().unwrap_or_else(|e| e.into_inner());
        upsert(&mut leases, key, lease, now);
    }

    /// `shouldRunScopeWork` for a scope key (see [`scope_key`]).
    pub(crate) fn should_run_scope_work(&self, scope_key: &str, settings: &Value) -> bool {
        let activity = resolve_activity(settings);
        let host = self
            .host_power
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        if host_constrained(&host, activity) {
            return false;
        }
        let now = Instant::now();
        let mut leases = self.leases.lock().unwrap_or_else(|e| e.into_inner());
        leases.retain(|_, lease| lease.active(now));
        leases.values().any(|lease| {
            // `leaseMayRunScopedWork`.
            if !lease.scope_keys.iter().any(|key| key == scope_key)
                || client_constrained(lease, activity)
            {
                return false;
            }
            activity.performance || lease.foreground(now)
        })
    }

    /// The provider status scheduler's demand check: work on the generic
    /// `provider-status` scope or on this instance's.
    pub(crate) fn provider_status_demand(&self, instance_id: &str, settings: &Value) -> bool {
        self.should_run_scope_work("provider-status", settings)
            || self.should_run_scope_work(&format!("provider-status:{instance_id}"), settings)
    }
}

/// `upsertClientActivityLease`: drop expired leases, and make room for a new
/// one by evicting the session's oldest lease past the cap.
fn upsert(leases: &mut HashMap<LeaseKey, Lease>, key: LeaseKey, lease: Lease, now: Instant) {
    leases.retain(|_, current| current.active(now));
    if !leases.contains_key(&key) {
        let session: Vec<(&LeaseKey, &Lease)> = leases
            .iter()
            .filter(|((session, _), _)| *session == key.0)
            .collect();
        if session.len() >= MAX_CLIENT_ACTIVITY_LEASES_PER_RPC_CLIENT {
            if let Some(oldest) = session
                .iter()
                .min_by_key(|(_, lease)| lease.updated_at)
                .map(|(key, _)| (*key).clone())
            {
                leases.remove(&oldest);
            }
        }
    }
    leases.insert(key, lease);
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn report(scopes: Value, visible: bool, focused: bool) -> Value {
        json!({"clientId": "tab-1", "clientKind": "web", "visible": visible, "focused": focused,
               "recentlyInteracted": false, "scopes": scopes, "observedAt": "2026-11-22T00:00:00Z"})
    }

    #[test]
    fn scoped_work_needs_a_live_foreground_lease_on_its_scope() {
        let policy = BackgroundPolicy::default();
        let settings = json!({});
        let provider_status = json!([{"type": "provider-status"}]);
        assert!(!policy.provider_status_demand("codex", &settings));
        policy.report_client_activity("s", &report(provider_status.clone(), true, false));
        // Visible but neither focused nor recently used: background, so balanced waits.
        assert!(!policy.provider_status_demand("codex", &settings));
        let performance = json!({"backgroundActivity": {"profile": "performance"}});
        assert!(policy.provider_status_demand("codex", &performance));
        policy.report_client_activity("s", &report(provider_status, true, true));
        assert!(policy.provider_status_demand("codex", &settings));
        // The same client's next report replaces its lease.
        let codex = json!([{"type": "provider-status", "instanceId": "codex"}]);
        policy.report_client_activity("s", &report(codex, true, true));
        assert!(policy.provider_status_demand("codex", &settings));
        assert!(!policy.provider_status_demand("claudeAgent", &settings));
        assert_eq!(policy.leases.lock().unwrap().len(), 1);
    }

    #[test]
    fn expired_and_low_power_leases_do_not_count() {
        let policy = BackgroundPolicy::default();
        let mut input = report(json!([{"type": "provider-status"}]), true, true);
        input["lowPowerMode"] = json!("true");
        policy.report_client_activity("s", &input);
        assert!(!policy.provider_status_demand("x", &json!({})));
        let custom = json!({"backgroundActivity": {"profile": "custom", "baseProfile": "balanced",
                                                   "overrides": {"pauseWhenClientLowPower": false}}});
        assert!(policy.provider_status_demand("x", &custom));
        let mut leases = policy.leases.lock().unwrap();
        for lease in leases.values_mut() {
            lease.expires_at = Instant::now() - Duration::from_millis(1);
        }
        drop(leases);
        assert!(!policy.provider_status_demand("x", &custom));
    }

    #[test]
    fn invalid_reports_record_nothing_and_sessions_are_capped() {
        let policy = BackgroundPolicy::default();
        policy.report_client_activity("s", &report(json!([{"type": "nope"}]), true, true));
        assert!(policy.leases.lock().unwrap().is_empty());
        for n in 0..20 {
            let mut input = report(json!([]), true, true);
            input["clientId"] = json!(format!("c{n}"));
            policy.report_client_activity("s", &input);
        }
        assert_eq!(
            policy.leases.lock().unwrap().len(),
            MAX_CLIENT_ACTIVITY_LEASES_PER_RPC_CLIENT
        );
    }

    #[test]
    fn a_constrained_host_pauses_scoped_work() {
        let activity = resolve_activity(&json!({}));
        let mut host = HostPower::unknown();
        host.locked = "true".into();
        assert!(!host_constrained(&host, activity));
        host.stale = false;
        assert!(host_constrained(&host, activity));
        host.locked = "false".into();
        host.on_battery = "true".into();
        assert!(!host_constrained(&host, activity));
        assert!(host_constrained(
            &host,
            resolve_activity(&json!({"backgroundActivity": {"profile": "battery-saver"}}))
        ));
        host.thermal_state = "serious".into();
        assert!(host_constrained(&host, activity));
    }
}
