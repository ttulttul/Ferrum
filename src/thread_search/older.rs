//! Palette field scoring for omitted older settled threads.
use super::{
    scoring::{fold, starts, Query},
    usage, LAST_USER,
};
use regex::Regex;
use rusqlite::{params, Connection};
use serde_json::Value;
use std::{collections::HashSet, sync::LazyLock};

struct Field {
    chars: Vec<char>,
    starts: Vec<bool>,
    weight: f64,
    context: bool,
}
impl Field {
    fn new(s: &str, weight: f64, context: bool) -> Self {
        let original = s.chars().collect::<Vec<_>>();
        Self {
            chars: fold(s).chars().collect(),
            starts: (0..original.len())
                .map(|i| starts(&original, i) && original[i].is_alphanumeric())
                .collect(),
            weight,
            context,
        }
    }
    fn word_end(&self, i: usize) -> usize {
        let mut end = i + 1;
        while end < self.chars.len() && self.chars[end].is_alphanumeric() && !self.starts[end] {
            end += 1
        }
        end
    }
    fn token(&self, t: &[char], primary: bool, typos: bool) -> f64 {
        if t.is_empty() {
            return 0.;
        }
        let mut best: f64 = 0.;
        let s = &self.chars;
        for (i, part) in s.windows(t.len()).enumerate().filter(|(_, p)| *p == t) {
            let end = i + part.len();
            if self.starts[i] || !t[0].is_alphanumeric() {
                if end == s.len() || self.starts[end] || !s[end].is_alphanumeric() {
                    return 1.;
                }
                let run = s[end..].iter().take_while(|c| c.is_alphanumeric()).count() + t.len();
                best = best.max(0.7 + 0.2 * t.len() as f64 / run as f64);
            } else if primary && t.len() >= 4 {
                best = best.max(0.3);
            }
        }
        if best > 0.3 {
            return best;
        }
        for i in (0..s.len()).filter(|i| self.starts[*i]) {
            if primary && s[i] == t[0] {
                let (mut at, mut rest, mut runs, mut long) = (i, t, 0, true);
                while !rest.is_empty() && at < s.len() {
                    let end = self.word_end(at);
                    let word = &s[at..end];
                    if word.starts_with(rest) {
                        if runs > 0
                            && (word == rest
                                || rest.len() >= 3
                                || Some(i) == self.starts.iter().position(|b| *b)
                                || long)
                        {
                            return 0.6;
                        }
                        break;
                    }
                    let Some(tail) = rest.strip_prefix(word) else {
                        break;
                    };
                    long &= word.len() >= 3;
                    runs += 1;
                    rest = tail;
                    at = end;
                    while at < s.len() && !self.starts[at] {
                        at += 1
                    }
                }
            }
        }
        if !typos || t.len() < 3 || !t.iter().any(|c| c.is_alphabetic()) {
            return best;
        }
        for i in (0..s.len()).filter(|i| self.starts[*i]) {
            if s[i] != t[0] && !(t.len() > 3 && s[i] == t[1] && s.get(i + 1) == Some(&t[0])) {
                continue;
            }
            let run = s[i..].iter().take_while(|c| c.is_alphanumeric()).count();
            for len in [t.len(), t.len() + 1, t.len() - 1] {
                if len > run || len < t.len() && t.len() <= 4 {
                    continue;
                }
                let a = &s[i..i + len];
                let same = a.iter().zip(t).take_while(|(a, b)| a == b).count();
                let (a, b) = (&a[same..], &t[same..]);
                if (a.len() > 1 && b.len() > 1 && a[0] == b[1] && a[1] == b[0] && a[2..] == b[2..])
                    || (!(len == t.len() && t.len() <= 4)
                        && (a.get(1..) == Some(b)
                            || b.get(1..) == Some(a)
                            || a.get(1..) == b.get(1..)))
                {
                    return 0.45;
                }
            }
        }
        best
    }
}
static BRANCH: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^(?:t3code|t3)/(.+)|^t3-([0-9a-f]{8})$").unwrap());
static PR: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^(?:pr|pull request|pull)\s*(#?\d)").unwrap());
static FLAT: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?:^| )t3-([0-9a-f]{1,8})( |$)").unwrap());
fn branch(s: &str) -> String {
    BRANCH.replace(s, "$1$2").into_owned()
}
struct Subject {
    id: String,
    project: String,
    title: String,
    fields: Vec<Field>,
    refs: Vec<String>,
    boost: f64,
    pick: i64,
    exact: bool,
}
struct Rank {
    strong: bool,
    class: i32,
    whole: bool,
    score: f64,
}
impl Subject {
    fn rank(&self, query: &str, field_query: &str, typos: bool) -> Option<Rank> {
        let words = field_query.split_whitespace().collect::<Vec<_>>();
        let filler = |w: &str| {
            matches!(
                w,
                "a" | "an" | "and" | "for" | "in" | "of" | "on" | "the" | "to" | "with"
            )
        };
        let optional = words.iter().any(|w| !filler(w));
        let (mut total, mut count, mut whole, mut strong, mut primary) =
            (0., 0, optional, true, false);
        let mut explains = vec![true; self.fields.len()];
        let mut after_context = explains.clone();
        for word in &words {
            let t = word.chars().collect::<Vec<_>>();
            let mut best: f64 = 0.;
            let mut at_start = false;
            let mut matched = Vec::new();
            for (i, f) in self.fields.iter().enumerate() {
                let score = f.token(&t, i == 0, typos);
                primary |= i == 0 && score > 0.;
                best = best.max(score * f.weight);
                at_start |= !f.context && score >= 0.7;
                matched.push(score > 0.);
            }
            if best == 0. {
                if optional && filler(word) {
                    continue;
                }
                count = 0;
                break;
            }
            if !filler(word) {
                whole &= word.encode_utf16().count() >= 3 && best >= 1.;
            }
            let context = self
                .fields
                .iter()
                .zip(&matched)
                .any(|(f, b)| f.context && *b);
            for (i, b) in matched.into_iter().enumerate() {
                if !filler(word) || !optional {
                    explains[i] &= b;
                    after_context[i] &= context || b;
                }
            }
            strong &= at_start;
            total += best;
            count += 1;
        }
        let quality = if count > 0 {
            if !primary
                && words.len() > 1
                && !explains.contains(&true)
                && !after_context.contains(&true)
            {
                return None;
            }
            let title = self.fields[0].chars.iter().collect::<String>();
            total / count as f64
                + if title == field_query {
                    0.5
                } else if words.len() > 1 && title.contains(field_query) {
                    0.25
                } else {
                    0.
                }
        } else {
            let digit = query.chars().any(char::is_numeric);
            let found = query.encode_utf16().count() >= 8
                && self.refs.iter().any(|r| {
                    let r = r.to_lowercase();
                    if !digit {
                        return r.starts_with(query);
                    }
                    let uncut = |s: &str| !s.chars().next().is_some_and(char::is_numeric);
                    r.find(query).is_some_and(|i| uncut(&r[i + query.len()..]))
                        || query.strip_prefix(&r).is_some_and(uncut)
                });
            if !found {
                return None;
            }
            whole = false;
            strong = true;
            0.6
        };
        Some(Rank {
            strong,
            whole,
            class: if quality >= 1.5 {
                3
            } else if quality >= 0.7 {
                2
            } else if quality >= 0.4 {
                1
            } else {
                0
            },
            score: quality * self.boost,
        })
    }
}
/// The palette's subjects for threads read as `(thread_id, payload_json,
/// project title, latest user message, project_id)` rows. Without `picks`,
/// as for an agent's search, the user's palette picks do not count.
fn subjects(
    rows: &mut rusqlite::Rows,
    q: &Query,
    environment_label: Option<&str>,
    now: i64,
    picks: bool,
) -> anyhow::Result<Vec<Subject>> {
    let mut subjects = Vec::new();
    while let Some(r) = rows.next()? {
        let id: String = r.get(0)?;
        let t: Value = serde_json::from_str(&r.get::<_, String>(1)?)?;
        let mut fields = vec![Field::new(
            t["title"].as_str().unwrap_or_default(),
            1.,
            false,
        )];
        let mut refs = vec![id.clone()];
        let mut add_pr = |pr: &Value, title: bool| {
            if let Some(n) = pr["number"].as_u64() {
                fields.push(Field::new(&format!("#{n}"), 1., false));
                if let Some(s) = pr["snapshot"]["title"].as_str().filter(|_| title) {
                    fields.push(Field::new(s, 0.5, false));
                }
                if let Some(url) = pr["url"].as_str() {
                    refs.push(url.to_owned());
                }
                if let Some(repo) = pr["repository"].as_str() {
                    refs.push(format!("{repo}#{n}"));
                }
            }
        };
        add_pr(&t["branchPullRequest"], false);
        if let Some(prs) = t["pullRequests"].as_array().filter(|prs| !prs.is_empty()) {
            let mut prs = prs
                .iter()
                .filter(|p| p["source"] != "stack-dismissed")
                .collect::<Vec<_>>();
            prs.sort_by_key(|p| {
                std::cmp::Reverse(
                    p["snapshot"]["updatedAt"]
                        .as_str()
                        .or(p["linkedAt"].as_str())
                        .unwrap_or_default(),
                )
            });
            for (i, p) in prs.into_iter().enumerate() {
                add_pr(p, i < 3);
            }
        } else {
            add_pr(&t["linkedPullRequest"], false);
        }
        if let Some(s) = t["branch"].as_str() {
            fields.push(Field::new(&branch(s), 0.6, false));
        }
        fields.push(Field::new(&r.get::<_, String>(2)?, 0.25, true));
        if let Some(s) = environment_label {
            fields.push(Field::new(s, 0.25, true));
        }
        let activity: Option<String> = r.get(3)?;
        let (boost, pick, exact) = usage(
            q,
            &t["opens"],
            activity.as_deref().or(t["createdAt"].as_str()),
            now,
        );
        subjects.push(Subject {
            id,
            project: r.get(4)?,
            title: t["title"].as_str().unwrap_or_default().to_owned(),
            fields,
            refs,
            boost,
            pick: if picks { pick } else { 0 },
            exact: picks && exact,
        });
    }
    Ok(subjects)
}

/// The subjects that match, best first (`rankSearchItems` with `scoreThreadSearch`).
fn ranked<'a>(subjects: &'a [Subject], q: &Query) -> Vec<&'a Subject> {
    let field_query = branch(
        PR.replace(&q.normalized, "$1")
            .trim_start_matches('#')
            .trim_start(),
    );
    let field_query = FLAT.replace_all(&field_query, " $1$2");
    let rank = |typos| {
        subjects
            .iter()
            .filter_map(|s| {
                s.rank(&q.normalized, field_query.trim(), typos)
                    .map(|r| (s, r))
            })
            .collect::<Vec<_>>()
    };
    let mut ranked = rank(false);
    if !ranked.iter().any(|(_, r)| r.strong) {
        ranked = rank(true);
    }
    ranked.sort_by(|(a, x), (b, y)| {
        let key = |s: &Subject, r: &Rank| {
            (
                r.class.min(2),
                if s.exact { s.pick } else { 0 },
                r.class,
                s.pick,
                r.whole,
            )
        };
        key(b, y).cmp(&key(a, x)).then(y.score.total_cmp(&x.score))
    });
    ranked.into_iter().map(|(s, _)| s).collect()
}

const COLUMNS: &str = "t.thread_id,t.payload_json,p.title,{LAST_USER},t.project_id";
const VISIBLE: &str = "t.archived_at IS NULL AND t.deleted_at IS NULL AND p.deleted_at IS NULL
        AND json_extract(t.payload_json,'$.lineage.relationshipToParent') IS NOT 'subagent' AND json_extract(t.payload_json,'$.lineage.sideChat') IS NOT 1";

pub(super) fn threads(
    db: &Connection,
    q: &Query,
    input: &Value,
    before: &str,
    hits: &[Value],
    now: i64,
) -> anyhow::Result<Vec<Value>> {
    let columns = COLUMNS.replace("{LAST_USER}", LAST_USER);
    let mut stmt=db.prepare(&format!("SELECT {columns} FROM orchestration_v2_projection_threads t JOIN projection_projects p USING(project_id)
        WHERE {VISIBLE}
        AND json_extract(t.payload_json,'$.settledOverride')='settled' AND json_extract(t.payload_json,'$.settledAt')<?1
        AND (?2 IS NULL OR t.project_id IN (SELECT value FROM json_each(?2)))"))?;
    let mut rows = stmt.query(params![
        before,
        input["projectIds"]
            .as_array()
            .map(|_| input["projectIds"].to_string())
    ])?;
    let subjects = subjects(&mut rows, q, input["environmentLabel"].as_str(), now, true)?;
    let mut ids = ranked(&subjects, q)
        .into_iter()
        .take(15)
        .map(|s| s.id.as_str())
        .collect::<Vec<_>>();
    let eligible = subjects
        .iter()
        .map(|s| s.id.as_str())
        .collect::<HashSet<_>>();
    let extra = hits
        .iter()
        .filter_map(|h| h["threadId"].as_str())
        .filter(|id| eligible.contains(id) && !ids.contains(id))
        .take(5)
        .collect::<Vec<_>>();
    ids.extend(extra);
    let mut out = vec![];
    for id in ids {
        if let Some(p) = crate::statev2::read::thread_projection(db, id)? {
            out.push(crate::Thread::from_stored(p, None).shell());
        }
    }
    Ok(out)
}

/// An active thread an agent's search may return: `(id, project id, title)`.
pub(super) type Candidate = (String, String, String);

/// For an agent's search (`ThreadSearch.searchThreads`): every active thread,
/// in `project` if given, as `(id, project id, title)`, and those that match
/// by title, branch, pull request or project, best first. Picks do not count.
pub(super) fn active(
    db: &Connection,
    q: &Query,
    project: Option<&str>,
    now: i64,
) -> anyhow::Result<(Vec<Candidate>, Vec<Value>)> {
    let columns = COLUMNS.replace("{LAST_USER}", LAST_USER);
    let mut stmt = db.prepare(&format!("SELECT {columns} FROM orchestration_v2_projection_threads t JOIN projection_projects p USING(project_id)
        WHERE {VISIBLE} AND (?1 IS NULL OR t.project_id = ?1)"))?;
    let mut rows = stmt.query(params![project])?;
    let subjects = subjects(&mut rows, q, None, now, false)?;
    let matches = ranked(&subjects, q)
        .into_iter()
        .map(|s| serde_json::json!({"threadId": s.id, "projectId": s.project, "title": s.title, "source": "title"}))
        .collect();
    let all = subjects
        .into_iter()
        .map(|s| (s.id, s.project, s.title))
        .collect();
    Ok((all, matches))
}
