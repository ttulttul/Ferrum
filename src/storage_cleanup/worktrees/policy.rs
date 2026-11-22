use super::*;
use serde::Deserialize;

pub(super) const DAY: i64 = 86_400_000;

#[derive(Clone, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub(super) struct Rules {
    pub worktree_keep_when: String,
    pub worktree_after_days: Option<i64>,
    pub worktree_settled_after_days: Option<i64>,
    pub worktree_on_delete: bool,
    pub worktree_on_merge: bool,
    pub worktree_unchanged: bool,
}

impl Rules {
    pub fn resolve(settings: &Value, project: &str) -> anyhow::Result<Self> {
        let policy = settings["projectSettingsOverrides"][project]
            .get("worktreeCleanup")
            .filter(|v| !v.is_null())
            .unwrap_or(&settings["worktreeCleanup"]);
        let mut rules: Self = serde_json::from_value(if policy["mode"] == "custom" {
            policy["rules"].clone()
        } else {
            settings["storageCleanup"].clone()
        })?;
        if policy["mode"] == "off" {
            rules.worktree_after_days = None;
            rules.worktree_settled_after_days = None;
            rules.worktree_on_delete = false;
            rules.worktree_on_merge = false;
            rules.worktree_unchanged = false;
        }
        Ok(rules)
    }
}

fn ms(t: &Value, field: &str) -> Option<i64> {
    t[field].as_str().and_then(crate::peer_links::iso_ms)
}

pub(super) fn activity(t: &Value) -> i64 {
    [
        "createdAt",
        "latestUserMessageAt",
        "latestRunRequestedAt",
        "latestRunStartedAt",
        "latestRunCompletedAt",
    ]
    .iter()
    .filter_map(|k| ms(t, k))
    .max()
    .unwrap_or(i64::MAX)
}

pub(super) struct Eligibility {
    pub removal: Option<String>,
    pub unchanged: bool,
    pub pending: String,
}
impl Eligibility {
    pub fn new(
        t: &Value,
        deleted: bool,
        active: i64,
        settled: Option<&Value>,
        rules: &Rules,
        now: i64,
    ) -> Self {
        let days = (now - active).div_euclid(DAY);
        let settled_for = settled
            .and_then(|s| ms(s, "settledAt"))
            .map(|at| now - at.max(active));
        let unchanged =
            rules.worktree_unchanged && (settled.is_some() || !t["archivedAt"].is_null());
        let removal = if deleted {
            Some("Thread was deleted".to_owned())
        } else if rules
            .worktree_after_days
            .is_some_and(|limit| active < now - limit * DAY)
        {
            Some(format!("Inactive for {days} days"))
        } else {
            settled_for
                .filter(|v| {
                    rules
                        .worktree_settled_after_days
                        .is_some_and(|limit| *v >= limit * DAY)
                })
                .map(|elapsed| format!("Settled for {} days", elapsed.div_euclid(DAY)))
        };
        let pending = if let Some(limit) = rules.worktree_after_days {
            format!("Inactive for {days} of {limit} days")
        } else if let Some(limit) = rules.worktree_settled_after_days {
            settled_for.map_or_else(
                || "Not settled".into(),
                |v| format!("Settled for {} of {limit} days", v.div_euclid(DAY)),
            )
        } else if rules.worktree_on_merge {
            "Not merged".into()
        } else if unchanged {
            "Has commits beyond the default branch".into()
        } else if rules.worktree_unchanged {
            "Not settled".into()
        } else {
            "No rules apply".into()
        };
        Self {
            removal,
            unchanged,
            pending,
        }
    }
}
