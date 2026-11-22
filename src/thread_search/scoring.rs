//! The palette's message token, boundary, phrase and proximity rules (paletteSearch.ts).
use regex::Regex;
use std::{collections::BTreeMap, sync::LazyLock};
use unicode_normalization::{char::is_combining_mark, UnicodeNormalization};

static QUOTES: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r#"^\s*["'`“”‘’]+|["'`“”‘’]+\s*$"#).unwrap());
static SYMBOL: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"\p{So}").unwrap());

pub(super) fn fold(text: &str) -> String {
    if text.is_ascii() {
        return text.to_ascii_lowercase();
    }
    text.chars()
        .map(|c| {
            let folded = c
                .to_string()
                .nfkd()
                .filter(|c| !is_combining_mark(*c))
                .flat_map(char::to_lowercase)
                .map(|x| match x {
                    'ς' => 'σ',
                    '‘' | '’' => '\'',
                    _ => x,
                })
                .collect::<String>();
            if folded.encode_utf16().count() == 1 && c.len_utf16() == 1 {
                folded
            } else {
                c.to_lowercase().collect()
            }
        })
        .collect()
}
fn cjk(c: char) -> bool {
    matches!(c, '\u{3040}'..='\u{30ff}'|'\u{3400}'..='\u{4dbf}'|'\u{4e00}'..='\u{9fff}'|'\u{f900}'..='\u{faff}')
}
pub(super) fn starts(text: &[char], i: usize) -> bool {
    let c = text[i];
    let prev = i.checked_sub(1).map(|n| text[n]).unwrap_or('\0');
    let before = i.checked_sub(2).map(|n| text[n]).unwrap_or('\0');
    let next = text.get(i + 1).copied().unwrap_or('\0');
    (!c.is_alphanumeric() && !SYMBOL.is_match(c.encode_utf8(&mut [0; 4])))
        || (!(matches!(prev, '\'' | '’') && before.is_alphanumeric())
            && (!prev.is_alphanumeric()
                || prev.is_lowercase() && c.is_uppercase()
                || prev.is_uppercase() && c.is_uppercase() && next.is_lowercase()
                || prev.is_numeric() != c.is_numeric()
                || cjk(c)))
}

pub(super) struct Query {
    pub words: Vec<String>,
    pub normalized: String,
    pub long: Vec<String>,
    pub short: Vec<String>,
    phrase: Option<Regex>,
    folded_phrase: Option<Regex>,
}

impl Query {
    pub fn new(query: &str) -> Self {
        let unquoted = QUOTES.replace_all(query, "");
        let normalized = fold(&unquoted.nfc().collect::<String>())
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" ");
        let words: Vec<String> = unquoted
            .replace(['‘', '’'], "'")
            .nfc()
            .collect::<String>()
            .to_lowercase()
            .split_whitespace()
            .map(str::to_owned)
            .collect();
        let (long, short) = words.iter().cloned().partition(|s| {
            let length = s.chars().count();
            length >= 3
                || length == 2
                    && s.chars()
                        .any(|c| cjk(c) || matches!(c, '\u{ac00}'..='\u{d7af}'))
        });
        let phrase = |words: Vec<String>| {
            if words.len() < 2 {
                return None;
            }
            let pattern = words
                .iter()
                .map(|s| regex::escape(s))
                .collect::<Vec<_>>()
                .join(r"\s+");
            Regex::new(&format!("(?i){pattern}")).ok()
        };
        Self {
            phrase: phrase(words.clone()),
            folded_phrase: phrase(
                words
                    .iter()
                    .map(|s| fold(s).chars().filter(|c| !is_combining_mark(*c)).collect())
                    .collect(),
            ),
            words,
            normalized,
            long,
            short,
        }
    }

    /// Best positions of each token in source-character offsets, converted to UTF-16 for distance.
    pub fn grade(&self, text: &str) -> Option<(f64, usize)> {
        let chars: Vec<char> = text.chars().collect();
        let folded = fold(text);
        let lower = text.to_lowercase();
        let plain = if lower.encode_utf16().count() == text.encode_utf16().count() {
            &lower
        } else {
            &folded
        };
        let positions = |s: &str| {
            s.char_indices()
                .map(|(i, _)| i)
                .chain(std::iter::once(s.len()))
                .collect::<Vec<_>>()
        };
        let plain_offsets = positions(plain);
        let folded_offsets = positions(&folded);
        let mut utf16 = vec![0];
        for c in &chars {
            utf16.push(utf16.last().unwrap() + c.len_utf16());
        }
        let mut quality: f64 = 1.;
        let mut places = Vec::new();
        for (word, token) in self.long.iter().chain(&self.short).enumerate() {
            let mut found =
                token_positions(&chars, plain, &plain_offsets, token, self.words.len() == 1);
            if found.0 < 1. && (!text.is_ascii() || !token.is_ascii()) {
                let token = fold(token)
                    .chars()
                    .filter(|c| !is_combining_mark(*c))
                    .collect::<String>();
                if !token.is_empty() {
                    found = token_positions(
                        &chars,
                        &folded,
                        &folded_offsets,
                        &token,
                        self.words.len() == 1,
                    );
                }
            }
            if found.1.is_empty() || found.0 == 0.3 && token.chars().count() < 4 {
                return None;
            }
            if word < self.long.len() {
                quality = quality.min(found.0);
            }
            places.extend(found.1.into_iter().map(|p| (utf16[p], word)));
        }
        for (pat, s) in [(&self.phrase, plain), (&self.folded_phrase, &folded)] {
            if let Some(pat) = pat {
                let s = s.replace(['‘', '’'], "'");
                let mut pos = 0;
                while let Some(m) = pat.find_at(&s, pos) {
                    if !s[..m.start()]
                        .chars()
                        .next_back()
                        .is_some_and(char::is_alphanumeric)
                        && !s[m.end()..]
                            .chars()
                            .next()
                            .is_some_and(char::is_alphanumeric)
                    {
                        return Some((quality + 0.25, s[..m.start()].encode_utf16().count()));
                    }
                    pos = m.start() + s[m.start()..].chars().next()?.len_utf8();
                }
            }
        }
        places.sort_by_key(|p| p.0);
        let mut counts = vec![0usize; self.words.len()];
        let (mut covered, mut left, mut start, mut span) = (0, 0, 0, usize::MAX);
        for &(end, word) in &places {
            if counts[word] == 0 {
                covered += 1;
            }
            counts[word] += 1;
            while covered == counts.len() {
                let (first, word) = places[left];
                if end - first < span {
                    span = end - first;
                    start = first;
                }
                counts[word] -= 1;
                if counts[word] == 0 {
                    covered -= 1;
                }
                left += 1;
            }
        }
        Some((
            quality
                + if self.long.len() > 1 && span <= 60 {
                    0.1
                } else {
                    0.
                },
            start,
        ))
    }
}

fn token_positions(
    chars: &[char],
    s: &str,
    offsets: &[usize],
    token: &str,
    first_only: bool,
) -> (f64, Vec<usize>) {
    let (mut best, mut hits) = (0., Vec::new());
    let (mut run_start, mut run_end, mut hex) = (0, 0, false);
    let mut pos = 0;
    while let Some(at) = s[pos..].find(token).map(|i| i + pos) {
        let start = offsets.binary_search(&at).unwrap();
        let end = offsets.binary_search(&(at + token.len())).unwrap();
        pos = offsets[start + 1];
        if end > chars.len() {
            break;
        }
        if end > run_end {
            run_start = start;
            while run_start > 0 && chars[run_start - 1].is_alphanumeric() {
                run_start -= 1;
            }
            run_end = end;
            while run_end < chars.len() && chars[run_end].is_alphanumeric() {
                run_end += 1;
            }
            let run = &chars[run_start..run_end];
            hex = run.len() >= 7
                && run.iter().all(char::is_ascii_hexdigit)
                && run.iter().any(char::is_ascii_digit);
        }
        if hex && start > run_start {
            continue;
        }
        let score = if !starts(chars, start) {
            0.3
        } else if end == chars.len() || !chars[end].is_alphanumeric() || starts(chars, end) {
            1.
        } else {
            0.8
        };
        if score > best {
            best = score;
            hits.clear();
        }
        if score == best {
            hits.push(start);
        }
        if first_only && score == 1. {
            break;
        }
    }
    (best, hits)
}

static ACCENTS: LazyLock<BTreeMap<char, String>> = LazyLock::new(|| {
    let mut map = BTreeMap::<char, String>::new();
    for c in (0xc0..0x250)
        .chain(0x1e00..0x1f00)
        .filter_map(char::from_u32)
    {
        let s = fold(&c.to_string());
        if s.len() == 1 {
            let letter = s.chars().next().unwrap();
            if letter.is_ascii_lowercase() {
                map.entry(letter).or_default().push(c);
            }
        }
    }
    map
});
pub(super) fn glob(token: &str) -> String {
    let mut s = "*".to_owned();
    for c in token.chars() {
        let folded = fold(&c.to_string());
        if folded.len() == 1 && folded.as_bytes()[0].is_ascii_lowercase() {
            let letter = folded.chars().next().unwrap();
            s.push('[');
            s.push(letter);
            s.push(letter.to_ascii_uppercase());
            if let Some(forms) = ACCENTS.get(&letter) {
                s.push_str(forms);
            }
            s.push(']');
        } else if matches!(c, '*' | '?' | '[') {
            s.push_str(&format!("[{c}]"));
        } else {
            s.push(c);
        }
    }
    s.push('*');
    s
}
pub(super) fn spellings(token: &str) -> Vec<String> {
    let mut prefixes = vec![String::new()];
    for c in token.chars() {
        let plain = c
            .to_string()
            .nfd()
            .filter(|c| !is_combining_mark(*c))
            .collect::<String>();
        let mut forms = vec![
            c.to_string(),
            c.to_uppercase().collect(),
            plain.to_uppercase(),
            plain,
        ];
        forms.sort();
        forms.dedup();
        prefixes = prefixes
            .iter()
            .flat_map(|p| forms.iter().map(move |f| format!("{p}{f}")))
            .collect();
    }
    prefixes
}
