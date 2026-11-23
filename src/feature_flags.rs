//! `T3CODE_FEATURE_FLAGS`, as the reference's `FeatureFlags` service reads it:
//! one comma-separated list in which a bare name turns a flag on and
//! `name=value` sets it. An invalid list turns every flag off.

/// The boolean flags a client is told (`ServerFeatureFlags`).
pub(crate) fn server_feature_flags() -> serde_json::Value {
    let list = std::env::var("T3CODE_FEATURE_FLAGS").ok();
    let (secret_providers, tanstack_source) = parse(list.as_deref()).unwrap_or((false, false));
    serde_json::json!({"secretProviders": secret_providers, "tanstackSource": tanstack_source})
}

/// effect's `Config.Boolean`.
fn boolean(value: &str) -> Option<bool> {
    match value {
        "true" | "yes" | "on" | "1" | "y" => Some(true),
        "false" | "no" | "off" | "0" | "n" => Some(false),
        _ => None,
    }
}

fn parse(list: Option<&str>) -> Option<(bool, bool)> {
    let mut flags = (false, false);
    for entry in list.unwrap_or_default().split(',') {
        let (name, value) = match entry.find('=') {
            Some(at) => (entry[..at].trim(), entry[at + 1..].trim()),
            None => (entry.trim(), "true"),
        };
        match name {
            "secret_providers" => flags.0 = boolean(value)?,
            "tanstack_source" => flags.1 = boolean(value)?,
            _ => {}
        }
    }
    Some(flags)
}

#[cfg(test)]
mod tests {
    use super::parse;

    #[test]
    fn flags_are_read_as_the_reference_reads_them() {
        assert_eq!(parse(None), Some((false, false)));
        assert_eq!(parse(Some("tanstack_source")), Some((false, true)));
        assert_eq!(
            parse(Some(" secret_providers=true , github_app_client_id=Iv23x")),
            Some((true, false))
        );
        assert_eq!(parse(Some("tanstack_source=maybe")), None);
    }
}
