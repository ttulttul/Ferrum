use super::{error, nodes};
use crate::provider_rpc::Reply;
use serde::Deserialize;
use serde_json::{json, Value};

#[derive(Deserialize)]
#[serde(untagged)]
enum Names {
    Single(String),
    List(Vec<String>),
}
fn names(v: Option<Names>) -> Vec<String> {
    let values = match v {
        Some(Names::Single(s)) => s.split(',').map(str::to_owned).collect(),
        Some(Names::List(v)) => v,
        None => Vec::new(),
    };
    values
        .iter()
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .map(str::to_owned)
        .collect()
}
#[derive(Deserialize)]
struct Form {
    name: String,
    description: String,
    title: Option<String>,
    labels: Option<Names>,
    assignees: Option<Names>,
    body: Vec<Field>,
}
#[derive(Deserialize)]
struct Field {
    #[serde(rename = "type")]
    kind: String,
    id: Option<String>,
    #[serde(default)]
    attributes: Attributes,
    #[serde(default)]
    validations: Validations,
}
#[derive(Deserialize, Default)]
struct Validations {
    #[serde(default)]
    required: bool,
    accept: Option<String>,
}
#[derive(Deserialize, Default)]
struct Attributes {
    label: Option<String>,
    description: Option<String>,
    value: Option<String>,
    placeholder: Option<String>,
    render: Option<String>,
    #[serde(default)]
    multiple: bool,
    default: Option<Indices>,
    #[serde(default)]
    options: Vec<Item>,
}
#[derive(Deserialize)]
#[serde(untagged)]
enum Indices {
    Single(i64),
    Many(Vec<i64>),
}
#[derive(Deserialize)]
#[serde(untagged)]
enum Item {
    Label(String),
    Option {
        label: String,
        #[serde(default)]
        required: bool,
    },
}
fn markdown(f: Field) -> String {
    let a = f.attributes;
    if f.kind == "markdown" {
        return a.value.unwrap_or_default();
    }
    let mut pieces = vec![format!(
        "### {}",
        a.label.or(f.id).as_deref().unwrap_or(&f.kind)
    )];
    if let Some(v) = a.description {
        pieces.push(v)
    }
    if f.validations.required {
        pieces.push("Required.".into())
    }
    if let Some(v) = a.placeholder.filter(|v| !v.is_empty()) {
        pieces.push(format!("Example:\n> {}", v.trim().replace('\n', "\n> ")))
    }
    if let Some(v) = a.render.filter(|v| !v.is_empty()) {
        pieces.push(format!("Format: {v}"))
    }
    if let Some(v) = f.validations.accept.filter(|v| !v.is_empty()) {
        pieces.push(format!("Accepted files: {v}"))
    }
    if f.kind == "dropdown" || f.kind == "checkboxes" {
        let dropdown = f.kind == "dropdown";
        if dropdown {
            pieces.push(
                if a.multiple {
                    "Choose one or more:"
                } else {
                    "Choose one:"
                }
                .into(),
            )
        }
        let defaults = match a.default {
            Some(Indices::Single(i)) => vec![i],
            Some(Indices::Many(v)) => v,
            None => Vec::new(),
        };
        let lines = a
            .options
            .into_iter()
            .enumerate()
            .map(|(i, item)| {
                let (label, required) = match item {
                    Item::Label(s) => (s, false),
                    Item::Option { label, required } => (label, required),
                };
                if dropdown {
                    format!(
                        "- {label}{}",
                        if defaults.contains(&(i as i64)) {
                            " (default)"
                        } else {
                            ""
                        }
                    )
                } else {
                    format!("- [ ] {label}{}", if required { " (required)" } else { "" })
                }
            })
            .collect::<Vec<_>>()
            .join("\n");
        pieces.push(lines);
    }
    if let Some(v) = a.value {
        pieces.push(v)
    }
    pieces.retain(|v| !v.is_empty());
    pieces.join("\n\n")
}
pub(super) fn decode(repo: &Value) -> Reply {
    let mut templates = indexmap::IndexMap::new();
    for t in repo["issueTemplates"].as_array().into_iter().flatten() {
        let Some(key) = t["filename"].as_str().filter(|s| !s.trim().is_empty()) else {
            continue;
        };
        templates.insert(key.rsplit('/').next().unwrap().to_owned(),json!({"key":key,"name":t["name"],"about":t["about"].as_str().unwrap_or(""),"title":t["title"].as_str().unwrap_or(""),"body":t["body"].as_str().unwrap_or(""),"labels":nodes(&t["labels"]).filter_map(|n|n["name"].as_str()).collect::<Vec<_>>(),"assignees":nodes(&t["assignees"]).filter_map(|n|n["login"].as_str()).collect::<Vec<_>>()}));
    }
    let mut blank = true;
    for entry in repo["templateDirectory"]["entries"]
        .as_array()
        .into_iter()
        .flatten()
    {
        let name = entry["name"].as_str().unwrap_or("");
        let lower = name.to_lowercase();
        if !(lower.ends_with(".yaml") || lower.ends_with(".yml")) {
            continue;
        }
        let blob = &entry["object"];
        let text = blob["text"]
            .as_str()
            .filter(|_| blob["__typename"] == "Blob" && blob["isTruncated"] != true)
            .ok_or_else(|| {
                error(
                    "templates",
                    "GitHub returned incomplete issue template contents.",
                )
            })?;
        if matches!(lower.as_str(), "config.yml" | "config.yaml") {
            #[derive(Deserialize)]
            struct Config {
                blank_issues_enabled: Option<bool>,
            }
            let config: Option<Config> = serde_yaml::from_str(text).map_err(|_| {
                error(
                    "templates",
                    "The repository has an unreadable issue template or configuration.",
                )
            })?;
            blank = config.and_then(|c| c.blank_issues_enabled).unwrap_or(true);
        } else if let Ok(form) = serde_yaml::from_str::<Form>(text) {
            if form.name.trim().is_empty() {
                continue;
            }
            templates.insert(name.to_owned(),json!({"key":name,"name":form.name.trim(),"about":form.description,"title":form.title.unwrap_or_default(),"body":form.body.into_iter().map(markdown).filter(|v|!v.is_empty()).collect::<Vec<_>>().join("\n\n"),"labels":names(form.labels),"assignees":names(form.assignees)}));
        }
    }
    Ok(json!({"templates":templates.into_values().collect::<Vec<_>>(),"blankIssuesEnabled":blank}))
}
