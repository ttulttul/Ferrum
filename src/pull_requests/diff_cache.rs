//! The reference retains up to 128 diff pages and 32 MiB of UTF-16 patch text.
//! A write invalidates the PR, including loads that began before the write.
use super::Reply;
use indexmap::IndexMap;
use serde_json::Value;
use std::{
    collections::hash_map::RandomState,
    future::Future,
    hash::{BuildHasher, Hash},
    time::{Duration, Instant},
};
use tokio::sync::Mutex;

#[derive(Default, Debug)]
pub(super) struct DiffCache {
    credentials: RandomState,
    pages: Mutex<Pages>,
}

type Key = (String, String, u64, u64, String);

#[derive(Debug, Default)]
struct Pages {
    epoch: u64,
    entries: IndexMap<Key, (Instant, usize, Value)>,
}

impl DiffCache {
    pub(super) fn scope(&self, credential: &impl Hash) -> u64 {
        self.credentials.hash_one(credential)
    }

    pub(super) async fn clear(&self, project: &str, repo: &str, number: u64) {
        let mut pages = self.pages.lock().await;
        pages.epoch = pages.epoch.wrapping_add(1);
        pages.entries.retain(|(p, r, n, _, _), _| {
            p != project || !r.eq_ignore_ascii_case(repo) || *n != number
        });
    }

    pub(super) async fn read(
        &self,
        project: &str,
        repo: &str,
        number: u64,
        scope: u64,
        input: &Value,
        load: impl Future<Output = Reply>,
    ) -> Reply {
        const BUDGET: usize = 32 * 1024 * 1024;
        let key = (
            project.to_owned(),
            repo.to_ascii_lowercase(),
            number,
            scope,
            input.to_string(),
        );
        let started = Instant::now();
        let epoch = {
            let pages = self.pages.lock().await;
            if let Some((expires, _, value)) = pages.entries.get(&key) {
                if *expires > started {
                    return Ok(value.clone());
                }
            }
            pages.epoch
        };
        let value = load.await?;
        let size = value["patch"].as_str().unwrap_or("").encode_utf16().count() * 2;
        let mut pages = self.pages.lock().await;
        if pages.epoch == epoch && size <= BUDGET {
            pages.entries.shift_remove(&key);
            let now = Instant::now();
            pages.entries.retain(|_, (expires, _, _)| *expires > now);
            let mut used: usize = pages.entries.values().map(|(_, size, _)| size).sum();
            while pages.entries.len() >= 128 || used + size > BUDGET {
                if let Some((_, (_, evicted, _))) = pages.entries.shift_remove_index(0) {
                    used -= evicted;
                }
            }
            let ttl = if input["commit"].is_null() { 60 } else { 600 };
            pages.entries.insert(
                key,
                (started + Duration::from_secs(ttl), size, value.clone()),
            );
        }
        Ok(value)
    }
}
