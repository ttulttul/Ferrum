//! The Bitbucket credential, as the reference's `BitbucketApi` reads it
//! (`resolveCredential`) for every request: the source-control settings
//! (`sourceControlHosts.bitbucket`, its secrets in the secret store or read from a
//! secret provider) first, then the `T3CODE_BITBUCKET_*` environment variables.
//!
//! An access token wins over an Atlassian email and API token. A value that is not
//! header-safe counts as unset. A secret reference that cannot be read fails
//! instead of falling back, so a request never switches accounts silently. Settings
//! are read again for each request, so a saved change applies at once.
use serde_json::Value;
use std::sync::{Arc, OnceLock};

/// The server's settings, once the server has started. Without them (the workspace
/// daemon), only the environment variables are read.
static SETTINGS: OnceLock<Arc<crate::settings::Settings>> = OnceLock::new();

pub(crate) fn install(settings: Arc<crate::settings::Settings>) {
    let _ = SETTINGS.set(settings);
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum Credential {
    AccessToken(String),
    ApiToken { email: String, token: String },
}

impl Credential {
    pub(crate) fn apply(&self, request: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        match self {
            Credential::AccessToken(token) => request.bearer_auth(token),
            Credential::ApiToken { email, token } => request.basic_auth(email, Some(token)),
        }
    }
}

/// `HEADER_SAFE`: invalid header values can throw errors quoting the credential.
fn header_safe(value: &str) -> bool {
    !value.is_empty() && value.bytes().all(|b| (0x21..=0x7e).contains(&b))
}

/// `credentialFrom`.
fn credential_from(access_token: &str, email: &str, api_token: &str) -> Option<Credential> {
    if header_safe(access_token) {
        return Some(Credential::AccessToken(access_token.to_owned()));
    }
    (header_safe(email) && header_safe(api_token)).then(|| Credential::ApiToken {
        email: email.to_owned(),
        token: api_token.to_owned(),
    })
}

fn from_environment() -> Option<Credential> {
    let read = |name: &str| std::env::var(name).unwrap_or_default();
    credential_from(
        &read("T3CODE_BITBUCKET_ACCESS_TOKEN"),
        &read("T3CODE_BITBUCKET_EMAIL"),
        &read("T3CODE_BITBUCKET_API_TOKEN"),
    )
}

/// `SettingsSecretUnresolvedError`'s message.
fn unresolved(settings: &Value, source: &Value) -> String {
    let label = crate::settings::secrets::provider_label(settings, source);
    format!("Could not read the credential from the secret provider \"{label}\". Unlock or fix it in Settings, then try again.")
}

/// `requireSettingsSecretLiteral` after the secrets are read: the literal, or why a
/// referenced secret could not be read.
async fn literal(
    settings_service: &crate::settings::Settings,
    settings: &Value,
    value: &Value,
) -> Result<String, String> {
    match value {
        Value::Null => Ok(String::new()),
        Value::String(text) => Ok(text.trim().to_owned()),
        reference => settings_service
            .resolver
            .resolve(
                settings,
                reference,
                crate::settings::secrets::ReadMode::Cached,
            )
            .await
            .map(|text| text.trim().to_owned())
            .ok_or_else(|| unresolved(settings, reference)),
    }
}

/// The credential for the next Bitbucket request, or the message of a secret that
/// could not be read.
pub(crate) async fn current() -> Result<Option<Credential>, String> {
    let Some(service) = SETTINGS.get() else {
        return Ok(from_environment());
    };
    // A settings file that cannot be read leaves only the environment, as in the reference.
    let settings = service.internal().unwrap_or_default();
    let bitbucket = &settings["sourceControlHosts"]["bitbucket"];
    let access_token = literal(service, &settings, &bitbucket["accessToken"]).await?;
    let api_token = if header_safe(&access_token) {
        String::new()
    } else {
        literal(service, &settings, &bitbucket["apiToken"]).await?
    };
    let email = bitbucket["email"]
        .as_str()
        .unwrap_or_default()
        .trim()
        .to_owned();
    Ok(credential_from(&access_token, &email, &api_token).or_else(from_environment))
}

/// `SourceControlProviderAuth`, its optional fields as `Option` tags.
fn auth(status: &str, account: Option<&str>, host: Option<&str>, detail: Option<&str>) -> Value {
    let option = |v: Option<&str>| match v {
        Some(v) => serde_json::json!({"_tag": "Some", "value": v}),
        None => serde_json::json!({"_tag": "None"}),
    };
    serde_json::json!({"status": status, "account": option(account), "host": option(host), "detail": option(detail)})
}

/// `authFromCredential`: the sign-in status when the account could not be asked.
fn auth_from_credential(credential: Option<&Credential>) -> Value {
    match credential {
        Some(Credential::AccessToken(_)) => auth("unknown", None, Some("bitbucket.org"), Some("An access token is configured.")),
        Some(Credential::ApiToken { email, .. }) => {
            auth("unknown", Some(email), Some("bitbucket.org"), Some("An API token is configured."))
        }
        None => auth(
            "unauthenticated",
            None,
            Some("bitbucket.org"),
            Some("Add a Bitbucket token in Settings → Source control & issues, or set the T3CODE_BITBUCKET_* environment variables on the server."),
        ),
    }
}

/// `BitbucketApi.probeAuth` within discovery's three seconds: the account `/user`
/// names, or what the configured credential says when it cannot be asked.
pub(crate) async fn probe_auth() -> Value {
    let probe = async {
        let credential = match current().await {
            Err(message) => {
                return auth(
                    "unauthenticated",
                    None,
                    Some("bitbucket.org"),
                    Some(&message),
                )
            }
            // Bitbucket answers an anonymous /user with 401, so there is nothing to ask.
            Ok(None) => return auth_from_credential(None),
            Ok(Some(credential)) => credential,
        };
        let base = std::env::var("T3CODE_BITBUCKET_API_BASE_URL")
            .unwrap_or_else(|_| "https://api.bitbucket.org/2.0".into());
        let answer = async {
            let client = reqwest::Client::builder()
                .redirect(reqwest::redirect::Policy::none())
                .build()
                .ok()?;
            let request = client
                .get(format!("{}/user", base.trim_end_matches('/')))
                .header("Accept", "application/json");
            let response = credential.apply(request).send().await.ok()?;
            if !response.status().is_success() {
                return None;
            }
            let user: Value = response.json().await.ok()?;
            // `BitbucketUserSchema`: each name, when present, a non-blank string.
            let names = ["username", "display_name", "account_id"];
            if !user.is_object()
                || names.iter().any(|k| {
                    user.get(*k)
                        .is_some_and(|v| v.as_str().is_none_or(|s| s.trim().is_empty()))
                })
            {
                return None;
            }
            Some(
                names
                    .iter()
                    .find_map(|k| user[*k].as_str().map(|s| s.trim().to_owned())),
            )
        };
        match answer.await {
            Some(account) => auth(
                "authenticated",
                account.as_deref(),
                Some("bitbucket.org"),
                None,
            ),
            None => auth_from_credential(current().await.ok().flatten().as_ref()),
        }
    };
    tokio::time::timeout(std::time::Duration::from_secs(3), probe)
        .await
        .unwrap_or_else(|_| {
            auth(
                "unknown",
                None,
                None,
                Some("The hosting service did not answer in time. Refresh to try again."),
            )
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn access_tokens_win_and_unsafe_values_count_as_unset() {
        assert_eq!(
            credential_from("tok", "a@b", "api"),
            Some(Credential::AccessToken("tok".into()))
        );
        assert_eq!(
            credential_from("", "a@b", "api"),
            Some(Credential::ApiToken {
                email: "a@b".into(),
                token: "api".into()
            })
        );
        assert_eq!(credential_from("has space", "", ""), None);
        assert_eq!(credential_from("", "a@b", "line\nbreak"), None);
    }
}
