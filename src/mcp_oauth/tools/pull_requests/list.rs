use super::*;
use std::collections::{HashMap, HashSet};

struct Chain {
    kind: &'static str,
    members: Vec<(usize, usize)>,
}

/// Order native stacks by the snapshot covering the most linked layers.
/// For other links the base branch must match exactly one head in the repository.
fn chains(links: &[&Value]) -> Vec<Chain> {
    let mut native = indexmap::IndexMap::<_, Vec<usize>>::new();
    let mut heads = HashMap::new();
    for (i, link) in links.iter().enumerate() {
        if let Some(id) = link["stack"]["id"].as_str() {
            native
                .entry((&link["host"], &link["repository"], id))
                .or_default()
                .push(i);
        } else if let Some(head) = link["snapshot"]["headBranch"].as_str() {
            heads
                .entry((&link["host"], &link["repository"], head))
                .and_modify(|p| *p = None)
                .or_insert(Some(i));
        }
    }
    let mut result = Vec::new();
    let mut placed = HashSet::new();
    for mut members in native.into_values() {
        let numbers: HashSet<_> = members
            .iter()
            .map(|i| links[*i]["number"].as_u64())
            .collect();
        let best = members
            .iter()
            .rev()
            .map(|i| rows(&links[*i]["stack"]["layers"]))
            .max_by_key(|layers| {
                (
                    layers
                        .iter()
                        .filter(|l| numbers.contains(&l["number"].as_u64()))
                        .count(),
                    layers.len(),
                )
            })
            .unwrap_or_default();
        members.sort_by_key(|i| {
            best.iter()
                .position(|l| l["number"] == links[*i]["number"])
                .unwrap_or(best.len())
        });
        placed.extend(members.iter().copied());
        result.push(Chain {
            kind: "native",
            members: members
                .into_iter()
                .enumerate()
                .map(|(depth, i)| (i, depth))
                .collect(),
        });
    }
    let mut children = vec![Vec::new(); links.len()];
    let mut roots = Vec::new();
    for (i, l) in links
        .iter()
        .enumerate()
        .filter(|(i, _)| !placed.contains(i))
    {
        let parent = l["snapshot"]["baseBranch"]
            .as_str()
            .and_then(|b| {
                heads
                    .get(&(&l["host"], &l["repository"], b))
                    .copied()
                    .flatten()
            })
            .filter(|p| *p != i);
        if let Some(p) = parent {
            children[p].push(i);
        } else {
            roots.push(i);
        }
    }
    for root in roots {
        let mut stack = vec![(root, 0)];
        let mut members = Vec::new();
        while let Some((i, depth)) = stack.pop() {
            if placed.insert(i) {
                members.push((i, depth));
                stack.extend(children[i].iter().rev().map(|child| (*child, depth + 1)));
            }
        }
        result.push(Chain {
            kind: "derived",
            members,
        });
    }
    for i in (0..links.len()).filter(|i| !placed.contains(i)) {
        result.push(Chain {
            kind: "derived",
            members: vec![(i, 0)],
        });
    }
    result
}

pub(super) fn page(links: &[&Value], args: &Value) -> Value {
    let chains = chains(links);
    let mut page = Vec::new();
    for (i, l) in links.iter().enumerate() {
        let state = l["snapshot"]["state"].as_str().unwrap_or("open");
        if args["states"]
            .as_array()
            .is_some_and(|states| !states.iter().any(|s| s == state))
        {
            continue;
        }
        let position = format!(
            "{} {} {} {:010}",
            l["linkedAt"].as_str().unwrap_or(""),
            l["host"].as_str().unwrap_or(""),
            l["repository"].as_str().unwrap_or(""),
            l["number"].as_u64().unwrap_or(0)
        );
        if args["cursor"]
            .as_str()
            .is_none_or(|cursor| position.as_str() > cursor)
        {
            page.push((position, i));
        }
    }
    page.sort_by(|a, b| a.0.cmp(&b.0));
    let more = page.len() > 50;
    page.truncate(50);
    let cursor = more.then(|| &page.last().unwrap().0);
    let mut stacks = vec![Value::Null; links.len()];
    let mut listed_chains = Vec::new();
    for chain in chains {
        if !chain
            .members
            .iter()
            .any(|(i, _)| page.iter().any(|(_, j)| i == j))
        {
            continue;
        }
        listed_chains.push(json!({"kind":chain.kind,"numbers":chain.members.iter().map(|(i,_)|&links[*i]["number"]).collect::<Vec<_>>()}));
        if chain.members.len() > 1 {
            for (i, depth) in &chain.members {
                stacks[*i] =
                    json!({"kind":chain.kind,"position":depth+1,"size":chain.members.len()});
            }
        }
    }
    let prs = page
        .iter()
        .map(|(_, i)| {
            let link = links[*i];
            let mut result = fields(link, &["host", "repository", "number", "url", "source"]);
            result["watching"] = json!(link.get("watch").is_some());
            for key in ["state", "title", "headBranch", "baseBranch", "isDraft"] {
                result[key] = link["snapshot"][key].clone();
            }
            result["stack"] = stacks[*i].take();
            result
        })
        .collect::<Vec<_>>();
    json!({"pullRequests":prs,"chains":listed_chains,"nextCursor":cursor})
}
