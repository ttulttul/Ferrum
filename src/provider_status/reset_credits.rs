//! Redeeming a provider's banked reset credit (`provider.consumeResetCredit`
//! for an instance), as the reference's Claude and Codex drivers do through
//! its `ResetCreditCoordinator` (`resetCreditCoordinator.ts`): redemptions
//! are serialized on the directory that holds the login, one idempotency key
//! is kept until the provider answers, and the instance is then rechecked
//! until a reset shows fresh limits.
use super::{base_env, claude, claude_credits, codex, instances, refresh, ProviderStatus};
use crate::server::AppState;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// One login's redemption lock, holding the idempotency key of a claim that
/// has not been answered yet.
type AccountLock = Arc<tokio::sync::Mutex<Option<String>>>;

/// The accounts' redemption locks (`ResetCreditCoordinator`).
#[derive(Default)]
pub(crate) struct Coordinator {
    accounts: Mutex<HashMap<String, AccountLock>>,
}

impl Coordinator {
    /// `redeem`: reuse the pending idempotency key until an outcome or a
    /// settled failure; other failures keep it for the retry.
    async fn redeem<E, F, Fut>(
        &self,
        account_key: &str,
        consume: F,
        settled: impl Fn(&E) -> bool,
    ) -> Result<&'static str, E>
    where
        F: FnOnce(String) -> Fut,
        Fut: std::future::Future<Output = Result<&'static str, E>>,
    {
        let lock = self
            .accounts
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .entry(account_key.to_owned())
            .or_default()
            .clone();
        let mut pending = lock.lock().await;
        let key = pending
            .clone()
            .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
        *pending = Some(key.clone());
        let result = consume(key).await;
        match &result {
            Ok(_) => *pending = None,
            Err(error) if settled(error) => *pending = None,
            Err(_) => {}
        }
        result
    }
}

/// A failed redemption: `ProviderSetupError`'s detail and cause.
pub(crate) struct Failure {
    pub detail: String,
    /// The driver's `ProviderDriverError`, encoded as a defect.
    pub cause: Option<Value>,
}

fn failure(detail: impl Into<String>) -> Failure {
    Failure {
        detail: detail.into(),
        cause: None,
    }
}

/// A driver's redemption failure: its `ProviderDriverError` (with the error
/// that caused it, when there is one) is the setup error's cause.
fn driver_failure(driver: &str, instance_id: &str, detail: &str, cause: Option<Value>) -> Failure {
    let mut error = json!({
        "name": "ProviderDriverError",
        "message": format!("Provider driver '{driver}' failed to create instance '{instance_id}': {detail}"),
    });
    if let Some(cause) = cause {
        error["cause"] = cause;
    }
    Failure {
        detail: detail.to_owned(),
        cause: Some(error),
    }
}

fn outcome_str(outcome: &str) -> &'static str {
    match outcome {
        "reset" => "reset",
        "nothingToReset" => "nothingToReset",
        "alreadyRedeemed" => "alreadyRedeemed",
        _ => "noCredit",
    }
}

/// The instance's published row.
fn current_row(service: &ProviderStatus, instance_id: &str) -> Option<Value> {
    let state = service.state.lock().unwrap_or_else(|e| e.into_inner());
    state
        .published
        .get(instance_id)
        .map(|p| p.row.clone())
        .filter(|row| !row.is_null())
}

/// `refreshAfterRedemption`: recheck the instance, and for a reset keep
/// rechecking (after 1, 2 and 4 seconds) until its usage limits are fresh.
async fn refresh_after_redemption(
    state: &Arc<AppState>,
    instance_id: &str,
    outcome: &str,
    before_refresh: impl Fn(),
) -> bool {
    let before = current_row(&state.provider_status, instance_id)
        .map(|row| row["usageLimits"]["checkedAt"].clone())
        .unwrap_or(Value::Null);
    let confirmed = || {
        outcome != "reset"
            || current_row(&state.provider_status, instance_id).is_some_and(|row| {
                let limits = &row["usageLimits"];
                limits.is_object()
                    && limits["checkedAt"] != before
                    && limits["unavailable"]["reason"] != "probeFailed"
            })
    };
    let mut delay = Duration::from_secs(1);
    for attempt in 0..4 {
        if attempt > 0 {
            tokio::time::sleep(delay).await;
            delay *= 2;
        }
        before_refresh();
        refresh(state, Some(instance_id)).await;
        if confirmed() {
            return true;
        }
    }
    false
}

/// `provider.consumeResetCredit` for an instance id: the outcome, or the
/// failure's detail. `None` when the instance is not one this module knows
/// (the caller answers for unknown instances).
pub(crate) async fn consume(
    state: &Arc<AppState>,
    instance_id: &str,
    credit_id: Option<&str>,
) -> Option<Result<Value, Failure>> {
    let settings = state.settings.internal().ok()?;
    let instance = instances(state, &settings)
        .into_iter()
        .find(|instance| instance.id == instance_id)?;
    if !instance.enabled {
        return Some(Err(failure("This provider is disabled.")));
    }
    let service = &state.provider_status;
    let env = base_env(&instance.environment);
    let result = match instance.driver.as_str() {
        "claudeAgent" => {
            let (config_dir, account_config_path) = claude::credit_paths(&instance.config, &env);
            let row = current_row(service, instance_id).unwrap_or(Value::Null);
            let credits = &row["usageLimits"]["resetCredits"];
            let grant = match credit_id {
                None => credits["nextCreditId"].as_str().map(str::to_owned),
                Some(id) => credits["options"]
                    .as_array()
                    .is_some_and(|options| options.iter().any(|o| o["creditId"] == id))
                    .then(|| id.to_owned()),
            };
            let version = row["version"].as_str().filter(|v| !v.is_empty());
            let outcome = match (grant, version) {
                (Some(grant), Some(version)) => {
                    service
                        .reset_credits
                        .redeem(
                            &config_dir.display().to_string(),
                            |request_id| async move {
                                claude_credits::consume(
                                    &config_dir,
                                    &account_config_path,
                                    version,
                                    &grant,
                                    &request_id,
                                )
                                .await
                            },
                            |(error, _): &(claude_credits::ClaimError, Option<Value>)| {
                                error.settled()
                            },
                        )
                        .await
                }
                _ => Ok("noCredit"),
            };
            match outcome {
                Err((error, cause)) => {
                    let mut claim =
                        json!({"name": "ClaudeResetCreditError", "message": error.message()});
                    if let Some(cause) = cause {
                        claim["cause"] = cause;
                    }
                    Err(driver_failure(
                        "claudeAgent",
                        instance_id,
                        error.message(),
                        Some(claim),
                    ))
                }
                Ok(outcome) => {
                    let confirmed = refresh_after_redemption(state, instance_id, outcome, || {
                        claude::invalidate_capabilities(service, instance_id)
                    })
                    .await;
                    if confirmed {
                        Ok(outcome)
                    } else {
                        Err(driver_failure("claudeAgent", instance_id, "The reset was applied, but Claude could not confirm the new limits. Refresh to check.", None))
                    }
                }
            }
        }
        "codex" => {
            let account_key = codex::account_key(&instance.config, &env);
            let (config, env) = (&instance.config, &env);
            let outcome = service
                .reset_credits
                .redeem(
                    &account_key,
                    move |key| async move {
                        codex::consume_reset_credit(config, env, &key)
                            .await
                            .map(|outcome| outcome_str(&outcome))
                    },
                    // Codex failures never settle the key: a retry reuses it.
                    |_: &Option<Value>| false,
                )
                .await;
            match outcome {
                Err(cause) => Err(driver_failure(
                    "codex",
                    instance_id,
                    "Codex could not redeem the reset credit.",
                    cause,
                )),
                Ok(outcome) => {
                    if refresh_after_redemption(state, instance_id, outcome, || {}).await {
                        Ok(outcome)
                    } else {
                        Err(driver_failure("codex", instance_id, "The reset was applied, but Codex could not confirm the new limits. Refresh to check.", None))
                    }
                }
            }
        }
        _ => Err(failure("This provider does not bank reset credits.")),
    };
    Some(result.map(|outcome| json!({"outcome": outcome})))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn an_unanswered_claim_keeps_its_key_until_an_answer() {
        let coordinator = Coordinator::default();
        let keys = Mutex::new(Vec::new());
        let attempt = |answer: Result<&'static str, bool>| {
            let keys = &keys;
            let coordinator = &coordinator;
            async move {
                coordinator
                    .redeem(
                        "account",
                        |key| async move {
                            keys.lock().unwrap().push(key);
                            answer
                        },
                        |settled: &bool| *settled,
                    )
                    .await
            }
        };
        // Unanswered: the retry reuses the key.
        assert!(attempt(Err(false)).await.is_err());
        assert!(attempt(Err(false)).await.is_err());
        // A settled failure ends the claim; the next one gets a new key.
        assert!(attempt(Err(true)).await.is_err());
        assert_eq!(attempt(Ok("reset")).await, Ok("reset"));
        assert_eq!(attempt(Ok("reset")).await, Ok("reset"));
        let keys = keys.into_inner().unwrap();
        assert_eq!(keys[0], keys[1]);
        assert_eq!(keys[1], keys[2]);
        assert_ne!(keys[2], keys[3]);
        assert_ne!(keys[3], keys[4]);
    }
}
