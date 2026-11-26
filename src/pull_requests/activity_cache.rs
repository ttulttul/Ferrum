//! Share a change request's activity across the activity and commit-diff reads.
//! Removing a slot on a write also invalidates any load still filling that slot.
use super::Reply;
use indexmap::IndexMap;
use serde_json::Value;
use std::{
    future::Future,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::{Mutex, OnceCell};

type Key = (String, String, u64, u64);
type Slot = Arc<OnceCell<(Instant, Value)>>;

#[derive(Default, Debug)]
pub(super) struct ActivityCache(Mutex<IndexMap<Key, Slot>>);

impl ActivityCache {
    pub(super) async fn clear(&self, project: &str, repo: &str, number: u64) {
        self.0.lock().await.retain(|(p, r, n, _), _| {
            p != project || !r.eq_ignore_ascii_case(repo) || *n != number
        });
    }

    pub(super) async fn read(
        &self,
        project: &str,
        repo: &str,
        number: u64,
        scope: u64,
        load: impl Future<Output = Reply>,
    ) -> Reply {
        let key = (project.to_owned(), repo.to_ascii_lowercase(), number, scope);
        let slot = {
            let mut entries = self.0.lock().await;
            entries.retain(|_, slot| {
                slot.get()
                    .is_none_or(|(at, _)| at.elapsed() < Duration::from_secs(60))
            });
            if entries.len() >= 128 && !entries.contains_key(&key) {
                entries.shift_remove_index(0);
            }
            entries.entry(key).or_default().clone()
        };
        let (_, value) = slot
            .get_or_try_init(|| async { Ok::<_, Value>((Instant::now(), load.await?)) })
            .await?;
        Ok(value.clone())
    }
}
