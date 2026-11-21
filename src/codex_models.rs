//! Codex's models as the reference lists them: the entries `codex
//! app-server` answers `model/list` with, as server provider models, with the
//! preferred default and the model manifest's Codex catalog applied.
//!
//! Platform neutral: the workspace runs Codex and returns the raw entries.
use serde_json::{json, Map, Value};

/// The reference's `PREFERRED_DEFAULT_CODEX_MODELS`.
const PREFERRED_DEFAULT_MODELS: [&str; 3] = ["gpt-6-astra", "gpt-5.6-sol", "gpt-5.6-terra"];

/// `codexModelFamily`: Bedrock's `openai.` prefix aside, the slug itself.
fn family(slug: &str) -> &str {
    if slug.starts_with("openai.gpt-") {
        &slug["openai.".len()..]
    } else {
        slug
    }
}

/// `formatCodexModelName`: `gpt-6-luna` reads `GPT-6-Luna`.
fn display_name(name: &str) -> String {
    let name = match name.get(..3) {
        Some(prefix) if prefix.eq_ignore_ascii_case("gpt") => format!("GPT{}", &name[3..]),
        _ => name.to_owned(),
    };
    let mut out = String::with_capacity(name.len());
    let mut chars = name.chars().peekable();
    while let Some(c) = chars.next() {
        out.push(c);
        if c == '-' {
            if let Some(next) = chars.peek().copied().filter(char::is_ascii_lowercase) {
                out.push(next.to_ascii_uppercase());
                chars.next();
            }
        }
    }
    out
}

fn effort_label(effort: &str) -> String {
    match effort {
        "none" => "None",
        "minimal" => "Minimal",
        "low" => "Low",
        "medium" => "Medium",
        "high" => "High",
        "xhigh" => "Extra High",
        "max" => "Max",
        "ultra" => "Ultra",
        other => other,
    }
    .to_owned()
}

/// `mapCodexModelCapabilities`: reasoning effort, service tier and cyber
/// access options. Without Codex's config the cyber access program has no
/// default, as the reference leaves it when its config read fails.
fn capabilities(model: &Value) -> Value {
    let slug = model["model"].as_str().unwrap_or_default();
    let default_effort = if family(slug) == "gpt-6-astra" {
        Some("medium")
    } else {
        model["defaultReasoningEffort"].as_str()
    };
    let efforts: Vec<Value> = model["supportedReasoningEfforts"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|entry| entry["reasoningEffort"].as_str())
        .map(|effort| {
            let mut option = json!({"id": effort, "label": effort_label(effort)});
            if Some(effort) == default_effort {
                option["isDefault"] = true.into();
            }
            option
        })
        .collect();
    let mut descriptors = Vec::new();
    if !efforts.is_empty() {
        let current = efforts
            .iter()
            .find(|option| option["isDefault"] == true)
            .map(|option| option["id"].clone());
        let mut descriptor = json!({"id": "reasoningEffort", "label": "Reasoning", "type": "select", "options": efforts});
        if let Some(current) = current {
            descriptor["currentValue"] = current;
        }
        descriptors.push(descriptor);
    }
    let tiers: Vec<Value> = match model["serviceTiers"].as_array() {
        Some(tiers) if !tiers.is_empty() => tiers.clone(),
        _ => model["additionalSpeedTiers"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(Value::as_str)
            .map(|id| json!({"id": id, "name": if id == "fast" { "Fast" } else { id }, "description": ""}))
            .collect(),
    };
    if !tiers.is_empty() {
        let default_tier = model["defaultServiceTier"]
            .as_str()
            .filter(|tier| tiers.iter().any(|t| t["id"] == *tier))
            .unwrap_or("default");
        let mut options = vec![json!({"id": "default", "label": "Standard"})];
        options.extend(tiers.iter().map(|tier| {
            let id = tier["id"].as_str().unwrap_or_default();
            let mut option = json!({"id": id, "label": tier["name"]});
            let description = if id == "ultrafast" {
                "Even faster, more expensive"
            } else {
                tier["description"].as_str().unwrap_or_default()
            };
            if !description.is_empty() {
                option["description"] = description.into();
            }
            option
        }));
        for option in &mut options {
            if option["id"] == default_tier {
                option["isDefault"] = true.into();
            }
        }
        descriptors.push(
            json!({"id": "serviceTier", "label": "Service Tier", "type": "select",
            "options": options, "currentValue": default_tier}),
        );
    }
    let programs: Vec<&str> = model["availableAccessPrograms"]["cyber"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .collect();
    if !programs.is_empty() {
        let listed = if programs.contains(&"standard") {
            programs
        } else {
            std::iter::once("standard").chain(programs).collect()
        };
        let options: Vec<Value> = listed
            .iter()
            .map(|program| {
                let label = match *program {
                    "standard" => "Standard",
                    "daybreakBlue" => "Daybreak Blue",
                    "daybreakRed" => "Daybreak Red",
                    other => other,
                };
                json!({"id": program, "label": label})
            })
            .collect();
        descriptors.push(json!({"id": "cyberAccessProgram", "label": "Cyber Access",
            "type": "select", "options": options}));
    }
    json!({"optionDescriptors": descriptors})
}

/// `parseCodexModelListResponse` over every page's `data`.
pub(crate) fn from_model_list(entries: &[Value]) -> Vec<Value> {
    entries
        .iter()
        .filter_map(|model| {
            let slug = model["model"].as_str()?;
            let name = model["displayName"].as_str().unwrap_or(slug);
            let mut row = Map::new();
            row.insert("slug".into(), slug.into());
            row.insert("name".into(), display_name(name).into());
            row.insert("isCustom".into(), false.into());
            if model["isDefault"] == true {
                row.insert("isDefault".into(), true.into());
            }
            row.insert("capabilities".into(), capabilities(model));
            Some(Value::Object(row))
        })
        .collect()
}

/// `applyPreferredCodexDefaultModel`, then `applyCodexModelCatalog` with the
/// manifest's Codex catalog: legacy models marked, the catalog's default
/// chat model made the default.
pub(crate) fn with_catalog(mut models: Vec<Value>, manifest: &Value) -> Vec<Value> {
    let preferred = PREFERRED_DEFAULT_MODELS.iter().find_map(|wanted| {
        models
            .iter()
            .find(|m| m["isCustom"] != true && m["slug"].as_str().map(family) == Some(wanted))
            .and_then(|m| m["slug"].as_str().map(str::to_owned))
    });
    if let Some(preferred) = preferred {
        move_default(&mut models, &preferred, false);
    }
    let catalog = &manifest["providers"]["codex"];
    let entries = catalog["models"].as_array().cloned().unwrap_or_default();
    for model in &mut models {
        if model["isCustom"] == true {
            continue;
        }
        let slug = model["slug"].as_str().unwrap_or_default().to_owned();
        let entry = entries
            .iter()
            .find(|e| e["slug"] == slug.as_str())
            .or_else(|| entries.iter().find(|e| e["slug"] == family(&slug)));
        let object = model.as_object_mut().expect("a model row");
        if entry.is_some_and(|e| e["status"] == "legacy") {
            object.insert("isLegacy".into(), true.into());
        } else {
            object.shift_remove("isLegacy");
        }
    }
    if let Some(requested) = catalog["defaults"]["chat"].as_str() {
        let slug = models
            .iter()
            .find(|m| m["slug"] == requested)
            .or_else(|| {
                models.iter().find(|m| {
                    m["isCustom"] != true
                        && m["slug"].as_str().map(family) == Some(family(requested))
                })
            })
            .and_then(|m| m["slug"].as_str().map(str::to_owned));
        if let Some(slug) = slug {
            move_default(&mut models, &slug, true);
        }
    }
    models
}

/// Make `slug` the default model. `with_aliases` moves the previous
/// default's aliases to it, as the catalog default does.
fn move_default(models: &mut [Value], slug: &str, with_aliases: bool) {
    let Some(previous) = models
        .iter()
        .find(|m| m["isDefault"] == true && m["slug"] != slug)
        .cloned()
    else {
        if !with_aliases {
            // The preferred default is marked even when none was.
            for model in models.iter_mut() {
                if model["slug"] == slug {
                    model["isDefault"] = true.into();
                }
            }
        }
        return;
    };
    for model in models.iter_mut() {
        let object = model.as_object_mut().expect("a model row");
        if object.get("slug") == previous.get("slug") {
            object.shift_remove("isDefault");
            if with_aliases {
                object.shift_remove("aliases");
            }
        } else if object.get("slug").and_then(Value::as_str) == Some(slug) {
            object.insert("isDefault".into(), true.into());
            if with_aliases {
                let mut aliases: Vec<Value> = object
                    .get("aliases")
                    .and_then(Value::as_array)
                    .cloned()
                    .unwrap_or_default();
                for alias in previous["aliases"].as_array().into_iter().flatten() {
                    if !aliases.contains(alias) {
                        aliases.push(alias.clone());
                    }
                }
                if !aliases.is_empty() {
                    object.insert("aliases".into(), aliases.into());
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(slug: &str, default: bool) -> Value {
        json!({
            "model": slug, "displayName": slug, "isDefault": default,
            "supportedReasoningEfforts": [{"reasoningEffort": "low"}, {"reasoningEffort": "high"}],
            "defaultReasoningEffort": "high",
            "additionalSpeedTiers": ["fast"],
        })
    }

    #[test]
    fn model_list_entries_become_provider_models() {
        let models = from_model_list(&[entry("gpt-6-luna", true)]);
        assert_eq!(models[0]["slug"], "gpt-6-luna");
        assert_eq!(models[0]["name"], "GPT-6-Luna");
        assert_eq!(models[0]["isDefault"], true);
        let descriptors = &models[0]["capabilities"]["optionDescriptors"];
        assert_eq!(descriptors[0]["id"], "reasoningEffort");
        assert_eq!(descriptors[0]["currentValue"], "high");
        assert_eq!(descriptors[0]["options"][1]["label"], "High");
        assert_eq!(descriptors[1]["id"], "serviceTier");
        assert_eq!(descriptors[1]["currentValue"], "default");
        assert_eq!(descriptors[1]["options"][1]["label"], "Fast");
    }

    #[test]
    fn the_preferred_and_catalog_defaults_win() {
        let models = from_model_list(&[
            entry("gpt-6-luna", true),
            entry("gpt-5.6-sol", false),
            entry("gpt-old", false),
        ]);
        let preferred = with_catalog(models.clone(), &Value::Null);
        assert_eq!(preferred[0].get("isDefault"), None);
        assert_eq!(preferred[1]["isDefault"], true);
        let manifest = json!({"providers": {"codex": {
            "defaults": {"chat": "gpt-6-luna"},
            "models": [{"slug": "gpt-old", "status": "legacy"}],
        }}});
        let catalogued = with_catalog(models, &manifest);
        assert_eq!(catalogued[0]["isDefault"], true);
        assert_eq!(catalogued[1].get("isDefault"), None);
        assert_eq!(catalogued[2]["isLegacy"], true);
    }
}
