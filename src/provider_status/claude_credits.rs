//! The reset credits a signed-in Claude subscription can redeem, as the
//! reference's `readClaudeResetCredits` (`claudeResetCredits.ts`): the stored
//! claude.ai login's usage, read with `cedar_ember=1`, summarized as the row's
//! `usageLimits.resetCredits`.
use regex::Regex;
use serde_json::{json, Map, Value};
use std::path::Path;
use std::sync::OnceLock;
use std::time::Duration;
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;

const API_BASE: &str = "https://api.anthropic.com";
const TIMEOUT: Duration = Duration::from_secs(10);

/// Claude Code's grouping of grants, in the order the options are listed.
const RESET_KINDS: [(&str, &str, &str); 4] = [
    ("session", "5-hour reset", "Refills your 5-hour limit."),
    ("weekly", "Weekly reset", "Refills your weekly limit."),
    (
        "fable",
        "Fable reset",
        "Refills your Fable allowance and gives back what Fable used on your weekly limit. It doesn't reset your whole week.",
    ),
    ("full", "Full reset", "Refills your 5-hour and weekly limits."),
];

/// A decoded `Grant`.
struct Grant {
    id: String,
    resets_left: u64,
    ends_at: Option<String>,
    kind: &'static str,
}

fn optional_bool(object: &Map<String, Value>, key: &str) -> Option<Option<bool>> {
    match object.get(key) {
        None => Some(None),
        Some(Value::Bool(b)) => Some(Some(*b)),
        Some(_) => None,
    }
}

/// `decodeGrant`, keeping only live grants (not paused, usable now, not ended).
fn live_grant(value: &Value, now_ms: i128) -> Option<Grant> {
    static GRANT_ID: OnceLock<Regex> = OnceLock::new();
    let object = value.as_object()?;
    let id = object.get("id")?.as_str()?;
    if !GRANT_ID
        .get_or_init(|| Regex::new(r"^[a-z0-9_-]{1,40}$").unwrap())
        .is_match(id)
    {
        return None;
    }
    let resets_left = object.get("resets_left")?.as_f64()?;
    if resets_left.fract() != 0.0 || !(0.0..=9_007_199_254_740_991.0).contains(&resets_left) {
        return None;
    }
    let ends_at = match object.get("ends_at") {
        None | Some(Value::Null) => None,
        Some(Value::String(s)) => Some(s.clone()),
        Some(_) => return None,
    };
    let paused = optional_bool(object, "paused")?;
    let usable_now = optional_bool(object, "usable_now")?;
    let clears = match object.get("clears") {
        None => vec![],
        Some(Value::Array(items)) => items.clone(),
        Some(_) => return None,
    };
    let gives_back_weekly = optional_bool(object, "gives_back_weekly")?;
    if paused == Some(true) || usable_now != Some(true) {
        return None;
    }
    if ends_at
        .as_deref()
        .is_some_and(|at| !future_timestamp(at, now_ms))
    {
        return None;
    }
    // A grant that gives back weekly usage is a Fable reset, else its declared
    // type, else what it clears.
    let kind = if gives_back_weekly == Some(true) {
        "fable"
    } else {
        match object.get("type").and_then(Value::as_str) {
            Some("session") => "session",
            Some("weekly") => "weekly",
            Some("full") => "full",
            _ => {
                let session = clears.iter().any(|c| c == "five_hour");
                let other = clears.iter().any(|c| c != "five_hour");
                match (session, other) {
                    (true, false) => "session",
                    (false, true) => "weekly",
                    _ => "full",
                }
            }
        }
    };
    Some(Grant {
        id: id.to_owned(),
        resets_left: resets_left as u64,
        ends_at,
        kind,
    })
}

fn parse_timestamp(value: &str) -> Option<OffsetDateTime> {
    static COMPLETE: OnceLock<Regex> = OnceLock::new();
    let complete = COMPLETE.get_or_init(|| {
        Regex::new(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})$").unwrap()
    });
    if !complete.is_match(value) {
        return None;
    }
    // Rfc3339 also rejects days past the end of their month, as `isFutureTimestamp` does.
    OffsetDateTime::parse(value, &Rfc3339).ok()
}

/// `isFutureTimestamp`.
fn future_timestamp(value: &str, now_ms: i128) -> bool {
    parse_timestamp(value).is_some_and(|at| at.unix_timestamp_nanos() / 1_000_000 > now_ms)
}

/// `DateTime.formatIso`: UTC with milliseconds.
fn iso(value: &str) -> Option<String> {
    let at = parse_timestamp(value)?.to_offset(time::UtcOffset::UTC);
    let format = time::macros::format_description!(
        "[year]-[month]-[day]T[hour]:[minute]:[second].[subsecond digits:3]Z"
    );
    at.format(&format).ok()
}

/// `claudeResetCreditsToContract`.
pub(super) fn to_contract(block: &Value, now_ms: i128) -> Option<Value> {
    let object = block.as_object()?;
    if !object.get("eligible")?.as_bool()? {
        return None;
    }
    let grants = match object.get("grants") {
        None => vec![],
        Some(Value::Array(items)) => items.clone(),
        Some(_) => return None,
    };
    let next_id = match object.get("next_grant_id") {
        None | Some(Value::Null) => None,
        Some(Value::String(s)) => Some(s.clone()),
        Some(_) => return None,
    };
    let live: Vec<Grant> = grants
        .iter()
        .filter_map(|grant| live_grant(grant, now_ms))
        .collect();
    let next = live
        .iter()
        .find(|grant| Some(&grant.id) == next_id.as_ref());
    let expires_at = |grant: Option<&Grant>| grant.and_then(|g| g.ends_at.as_deref()).and_then(iso);
    let mut result = json!({
        "availableCount": if next.is_some() {
            live.iter().map(|g| g.resets_left).sum::<u64>()
        } else {
            0
        },
    });
    if let Some(at) = expires_at(next) {
        result["nextExpiresAt"] = json!(at);
    }
    if let Some(next) = next {
        result["nextCreditId"] = json!(next.id);
        let options: Vec<Value> = RESET_KINDS
            .iter()
            .filter_map(|(kind, label, description)| {
                let grants: Vec<&Grant> = live
                    .iter()
                    .filter(|g| g.resets_left > 0 && g.kind == *kind)
                    .collect();
                let target = grants.first()?;
                let mut option = json!({
                    "creditId": target.id,
                    "label": label,
                    "description": description,
                    "availableCount": grants.iter().map(|g| g.resets_left).sum::<u64>(),
                });
                if let Some(at) = expires_at(Some(target)) {
                    option["expiresAt"] = json!(at);
                }
                Some(option)
            })
            .collect();
        result["options"] = json!(options);
    }
    Some(result)
}

/// `readAccessToken`: the stored claude.ai login (not on macOS, where it is in
/// the keychain).
fn access_token(config_dir: &Path) -> Option<String> {
    if cfg!(target_os = "macos") {
        return None;
    }
    let text = std::fs::read_to_string(config_dir.join(".credentials.json")).ok()?;
    let value: Value = serde_json::from_str(&text).ok()?;
    let token = value
        .as_object()?
        .get("claudeAiOauth")?
        .get("accessToken")?;
    token
        .as_str()
        .map(str::trim)
        .filter(|t| !t.is_empty())
        .map(str::to_owned)
}

/// `readClaudeResetCredits`: `None` without a stored login or on any failure.
pub(super) async fn read(config_dir: &Path, version: &str) -> Option<Value> {
    let token = access_token(config_dir)?;
    let request = async {
        let response = super::http_client()
            .get(format!("{API_BASE}/api/oauth/usage"))
            .query(&[("cedar_ember", "1"), ("skip_spend", "1")])
            .header("authorization", format!("Bearer {token}"))
            .header("anthropic-beta", "oauth-2025-04-20")
            .header(
                "user-agent",
                format!("claude-cli/{version} (external, cli)"),
            )
            .send()
            .await
            .ok()?;
        if !response.status().is_success() {
            return None;
        }
        let body: Value = response.json().await.ok()?;
        let block = body
            .as_object()?
            .get("cedar_ember")
            .cloned()
            .unwrap_or(Value::Null);
        to_contract(&block, crate::platform::now_ms() as i128)
    };
    tokio::time::timeout(TIMEOUT, request).await.ok().flatten()
}

#[cfg(test)]
mod tests {
    use super::*;

    const NOW: i128 = 1_790_000_000_000; // 2026-09-21

    #[test]
    fn credits_group_live_grants_by_kind() {
        let block = json!({"eligible": true, "next_grant_id": "g2", "grants": [
            {"id": "g1", "resets_left": 1, "usable_now": true, "clears": ["five_hour"]},
            {"id": "g2", "resets_left": 2, "usable_now": true, "type": "weekly",
             "ends_at": "2027-01-01T05:00:00.123456+05:00"},
            {"id": "g3", "resets_left": 4, "usable_now": true, "paused": true},
            {"id": "g4", "resets_left": 1, "usable_now": true, "ends_at": "2020-01-01T00:00:00Z"},
            {"id": "g5", "resets_left": 0, "usable_now": true, "gives_back_weekly": true},
            {"id": "G6", "resets_left": 1, "usable_now": true}
        ]});
        assert_eq!(
            to_contract(&block, NOW),
            Some(json!({
                "availableCount": 3,
                "nextExpiresAt": "2027-01-01T00:00:00.123Z",
                "nextCreditId": "g2",
                "options": [
                    {"creditId": "g1", "label": "5-hour reset", "description": "Refills your 5-hour limit.",
                     "availableCount": 1},
                    {"creditId": "g2", "label": "Weekly reset", "description": "Refills your weekly limit.",
                     "availableCount": 2, "expiresAt": "2027-01-01T00:00:00.123Z"}
                ]
            }))
        );
    }

    #[test]
    fn credits_without_a_next_grant_count_none() {
        let block = json!({"eligible": true, "grants": [{"id": "g1", "resets_left": 1, "usable_now": true}]});
        assert_eq!(to_contract(&block, NOW), Some(json!({"availableCount": 0})));
        assert_eq!(to_contract(&json!({"eligible": false}), NOW), None);
        assert_eq!(to_contract(&Value::Null, NOW), None);
        assert!(!future_timestamp("2027-02-30T00:00:00Z", NOW));
    }
}
