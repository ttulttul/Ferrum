//! Delivery of whole assistant-message snapshots, as in the reference's
//! assistantStreaming filter. The event log keeps the full source text as
//! well as the prefix the client has seen, including across a reconnect.
use crate::workspace::provider::ProviderItem;
use indexmap::IndexMap;
use regex::Regex;
use std::sync::LazyLock;

const IDLE_MS: u64 = 800;
const UPDATE_MS: u64 = 400;

static LIST: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^[ \t]*(?:[-*+]|[0-9]{1,9}[.)])[ \t]").unwrap());
static TITLE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"^ {0,3}(?:#{1,6}(?:[ \t]|$)|\*\*(?:[^*]|\*[^*])+\*\*:?$)").unwrap()
});

/// State at the last terminated line. A rewritten snapshot starts a new scan.
#[derive(Default)]
struct Scan {
    previous: String,
    line_start: usize,
    boundary: usize,
    title: bool,
    fence: Option<(u8, usize, usize)>,
}

impl Scan {
    fn paragraph<'a>(&mut self, text: &'a str) -> &'a str {
        if !text.starts_with(&self.previous) {
            *self = Self::default();
        }
        let from = self.previous.len();
        self.previous.push_str(&text[from..]);
        for (at, _) in text[from..].match_indices('\n') {
            let end = from + at + 1;
            let line = text[self.line_start..end - 1].trim_end_matches([' ', '\t', '\r']);
            self.line(line, end);
            self.line_start = end;
        }
        let partial = &text[self.line_start..];
        let list_started = self.fence.is_none()
            && self.line_start > 0
            && !self.title
            && LIST.find(partial).is_some_and(|m| {
                !partial[m.end()..]
                    .trim_matches([' ', '\t', '\r'])
                    .is_empty()
            });
        &text[..if list_started {
            self.line_start
        } else {
            self.boundary
        }]
    }

    fn line(&mut self, line: &str, end: usize) {
        if self.fence.is_none() && self.line_start > 0 && !self.title && LIST.is_match(line) {
            self.boundary = self.line_start;
        }
        let body = line.trim_start_matches(' ');
        let indent = line.len() - body.len();
        let marker = body.as_bytes().first().copied().unwrap_or(0);
        let width = body.bytes().take_while(|b| *b == marker).count();
        if matches!(marker, b'`' | b'~')
            && width >= 3
            && (marker != b'`' || !body[width..].contains('`'))
        {
            match self.fence {
                None => {
                    self.fence = Some((marker, width, indent));
                    self.title = false;
                }
                Some((m, w, i))
                    if m == marker && width >= w && indent <= i + 3 && body.len() == width =>
                {
                    self.fence = None;
                    self.boundary = end;
                }
                _ => {}
            }
        } else if self.fence.is_none() {
            if line.is_empty() && self.line_start > 0 {
                if !self.title {
                    self.boundary = end;
                }
            } else {
                let is_title = TITLE.is_match(line);
                if self.line_start > 0 && !self.title && line.starts_with('#') && is_title {
                    self.boundary = self.line_start;
                }
                self.title = is_title;
            }
        }
    }
}

struct Track {
    latest: ProviderItem,
    scan: Scan,
    visible: String,
    delivered_at: Option<u64>,
    due_at: Option<u64>,
}

pub(super) struct AssistantText {
    turn_only: bool,
    tracks: IndexMap<String, Track>,
}

impl AssistantText {
    fn new(turn_only: bool) -> Self {
        Self {
            turn_only,
            tracks: IndexMap::new(),
        }
    }

    pub(super) async fn for_run<H: super::TurnHost>(
        host: &H,
        run: &crate::Run,
    ) -> anyhow::Result<Self> {
        let settings = host.turn_settings()?;
        let store = host.store.read().await;
        let thread = store.threads.get(&run.thread_id);
        let mode = thread
            .and_then(|t| {
                settings["projectSettingsOverrides"][&t.project_id]["responseStreamingMode"]
                    .as_str()
            })
            .or_else(|| settings["responseStreamingMode"].as_str());
        let mut filter = Self::new(mode == Some("turn"));
        if let Some(previous) = thread.and_then(|t| t.provider.runs.get(&run.id)) {
            for record in &previous.items {
                if let Some((visible, received_at)) = &record.visible_text {
                    filter.restore(&record.item, visible, *received_at, record.updated_at);
                }
            }
        }
        Ok(filter)
    }

    fn restore(&mut self, item: &ProviderItem, visible: &str, received_at: u64, delivered_at: u64) {
        if item.status != "running" {
            return;
        }
        let held = !self.turn_only && item.fields["text"].as_str() != Some(visible);
        self.tracks.insert(
            item.id.clone(),
            Track {
                latest: item.clone(),
                scan: Scan::default(),
                visible: visible.to_owned(),
                delivered_at: (!visible.is_empty()).then_some(delivered_at),
                due_at: held.then_some(received_at.saturating_add(IDLE_MS)),
            },
        );
    }

    /// Some adapters can follow a whole snapshot with a delta. Extend the
    /// source text, including any suffix the client hasn't seen.
    pub(super) fn append(&self, id: &str, delta: &str) -> Option<ProviderItem> {
        let mut item = self.tracks.get(id)?.latest.clone();
        let serde_json::Value::String(text) = item.fields.get_mut("text")? else {
            return None;
        };
        text.push_str(delta);
        Some(item)
    }

    pub(super) fn filter(&mut self, item: &ProviderItem, now: u64) -> Option<String> {
        if item.kind != "assistant_message"
            || item.fields.contains_key("subagentThread")
            || item.fields.contains_key("subagentChild")
        {
            return None;
        }
        if item.status != "running"
            || item.fields.get("streaming").and_then(|v| v.as_bool()) != Some(true)
        {
            self.tracks.shift_remove(&item.id);
            return None;
        }
        let text = item.fields.get("text")?.as_str()?;
        // A delta stream opens with an empty item; StreamedText owns its deltas.
        if text.is_empty() && !self.tracks.contains_key(&item.id) {
            return None;
        }
        let track = self
            .tracks
            .entry(item.id.clone())
            .and_modify(|track| track.latest.clone_from(item))
            .or_insert_with(|| Track {
                latest: item.clone(),
                scan: Scan::default(),
                visible: String::new(),
                delivered_at: None,
                due_at: None,
            });
        if self.turn_only {
            return Some(String::new());
        }
        if track
            .delivered_at
            .is_none_or(|at| now.saturating_sub(at) >= UPDATE_MS)
        {
            let ready = track.scan.paragraph(text);
            if !track.visible.starts_with(ready) {
                track.visible = ready.to_owned();
                track.delivered_at = Some(now);
            }
        }
        track.due_at = (text != track.visible).then_some(now.saturating_add(IDLE_MS));
        Some(track.visible.clone())
    }

    pub(super) fn deadline(&self) -> Option<u64> {
        self.tracks.values().filter_map(|track| track.due_at).min()
    }

    pub(super) fn due(&mut self, now: u64) -> Vec<ProviderItem> {
        let mut result = Vec::new();
        for track in self.tracks.values_mut() {
            if track.due_at.is_some_and(|at| at <= now) {
                track.due_at = None;
                let item = track.latest.clone();
                track.visible = item.fields["text"].as_str().unwrap().to_owned();
                track.delivered_at = Some(now);
                result.push(item);
            }
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn snapshot(text: &str) -> ProviderItem {
        ProviderItem::new("m", "assistant_message", "running")
            .with("text", text)
            .with("streaming", true)
    }

    #[test]
    fn short_snapshot_ends_before_idle_or_delivers_once_after_a_pause() {
        let mut filter = AssistantText::new(false);
        let mut end = snapshot("done");
        end.status = "completed".into();
        assert_eq!(filter.filter(&snapshot("working"), 10).as_deref(), Some(""));
        assert_eq!(filter.filter(&end, 20), None);
        assert!(filter.due(810).is_empty());

        filter.filter(&snapshot("pause"), 1000);
        let latest = filter.append("m", " here").unwrap();
        filter.filter(&latest, 1050);
        assert!(filter.due(1800).is_empty());
        assert_eq!(filter.due(1850), vec![snapshot("pause here")]);
        assert!(filter.due(3000).is_empty());
    }

    #[test]
    fn paragraph_boundaries_handle_headings_lists_fences_and_rewrites() {
        let mut scan = Scan::default();
        for (text, ready) in [
            ("Intro\n\n## Results\n\n", "Intro\n\n"),
            (
                "Intro\n\n## Results\n\n- first\n- s",
                "Intro\n\n## Results\n\n- first\n",
            ),
            ("A\n\n    ~~~rs\ncode\n\n", "A\n\n"),
            (
                "A\n\n    ~~~rs\ncode\n\n     ~~~~\nnext",
                "A\n\n    ~~~rs\ncode\n\n     ~~~~\n",
            ),
            ("**Result**\n\nBody\n\nNext", "**Result**\n\nBody\n\n"),
        ] {
            assert_eq!(scan.paragraph(text), ready);
        }
        let mut filter = AssistantText::new(false);
        assert_eq!(filter.filter(&snapshot("A\n\nB"), 0).unwrap(), "A\n\n");
        assert_eq!(
            filter.filter(&snapshot("A\n\nB\n\nC"), 350).unwrap(),
            "A\n\n"
        );
        assert_eq!(
            filter.filter(&snapshot("A\n\nB\n\nCD"), 410).unwrap(),
            "A\n\nB\n\n"
        );
    }

    #[test]
    fn a_reconnected_snapshot_keeps_its_idle_deadline_and_turn_mode_has_none() {
        let item = snapshot("A\n\npart");
        let mut filter = AssistantText::new(false);
        filter.restore(&item, "A\n\n", 800, 500);
        assert_eq!(filter.deadline(), Some(1600));
        assert_eq!(filter.due(1600), vec![item.clone()]);
        let mut filter = AssistantText::new(true);
        assert_eq!(filter.filter(&item, 0).unwrap(), "");
        assert_eq!(filter.deadline(), None);
    }
}
