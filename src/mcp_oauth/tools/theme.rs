//! The reference's theme toolkit (`toolkits/theme/handlers.ts`): T3 Code's
//! own themes, read and set by an agent.
use super::super::themes;
use super::super::ThemeRefusal;
use super::*;

fn theme_failure(refusal: ThemeRefusal) -> Refusal {
    match refusal {
        ThemeRefusal::Refused(message) => failure("invalid_request", message),
        ThemeRefusal::NotSelected => failure(
            "orchestration_error",
            "The operation could not be completed.",
        ),
    }
}

pub(super) async fn read<S: McpHost>(host: &Arc<S>, a: &Value) -> Outcome {
    let theme_id = arg_str(a, "themeId")?.map(str::to_owned);
    let (published, theme, dir) = host.theme_read(theme_id).await.map_err(theme_failure)?;
    let mut out = json!({
        "current": host.settings()["defaultTheme"].as_str().unwrap_or(""),
        "builtIn": themes::built_in(),
        "published": published.iter().map(|t| json!({
            "id": t["id"],
            "name": t["name"],
            "hasCss": t["css"].as_str().is_some_and(|css| !css.is_empty()),
            "hasBackground": t.get("background").is_some(),
        })).collect::<Vec<_>>(),
        "themesDir": dir,
        "guide": themes::guide(),
    });
    if let Some(mut theme) = theme {
        if let Some(object) = theme.as_object_mut() {
            object.remove("id");
            object.remove("background");
        }
        out["theme"] = theme;
    }
    Ok(out)
}

pub(super) async fn set<S: McpHost>(host: &Arc<S>, caller: &Caller, a: &Value) -> Outcome {
    // The theme file is decoded with the reference's schema, which the
    // advertised one only outlines; the other arguments as advertised.
    let mut rest = a.as_object().cloned().unwrap_or_default();
    let theme = rest.remove("theme").filter(|t| !t.is_null());
    arguments::check("t3_theme_set", &Value::Object(rest), &[])?;
    // `themeId` is an `EnvironmentThemeId`, whose pattern the advertised schema leaves out.
    if arg_str(a, "themeId")?.is_some_and(|id| !themes::valid_id(id)) {
        return Err(Refusal::Rpc(
            -32602,
            "Invalid parameters for tool 't3_theme_set': Expected a string matching the RegExp ^(?!(?:system|light|dark)$)[a-z0-9](?:[a-z0-9-]{0,47})$\n  at [\"themeId\"]".into(),
        ));
    }
    let theme = match theme {
        Some(theme) => Some(themes::decode_file(&theme, &["theme"]).map_err(|problem| {
            Refusal::Rpc(
                -32602,
                format!("Invalid parameters for tool 't3_theme_set': {problem}"),
            )
        })?),
        None => None,
    };
    projects::environment_writer(&**host, caller, Some(projects::NOT_LINKED_WHAT)).await?;
    let theme_id = arg_str(a, "themeId")?.map(str::to_owned);
    let appearance = arg_str(a, "appearance")?.map(str::to_owned);
    if theme_id.is_none() && (theme.is_some() || appearance.is_none()) {
        return Err(failure(
            "invalid_request",
            "Give `themeId`, `appearance`, or both. `theme` needs a `themeId`.",
        ));
    }
    let published = theme.is_some();
    host.theme_apply(theme_id.clone(), theme, appearance.clone())
        .await
        .map_err(theme_failure)?;
    let mut out = json!({});
    if let Some(id) = theme_id {
        out["themeId"] = json!(id);
    }
    if let Some(appearance) = appearance {
        out["appearance"] = json!(appearance);
    }
    out["published"] = json!(published);
    Ok(out)
}
