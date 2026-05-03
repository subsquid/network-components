use std::sync::Arc;

use dashmap::DashMap;
use tokio::sync::Mutex;

/// Per-key serialisation for cache-miss validation calls.
///
/// When N concurrent requests arrive for the same `key_id` and the cache
/// is `UNDEFINED`, only the first one calls Network API; the others wait
/// on this mutex and re-check the cache once they acquire it.
///
/// This bounds the bad-key flood guarantee from "1 call per 15s under
/// sequential load" to "1 call per 15s under any load shape".
pub struct Singleflight {
    inner: DashMap<String, Arc<Mutex<()>>>,
}

impl Singleflight {
    pub fn new() -> Self {
        Self {
            inner: DashMap::new(),
        }
    }

    /// Acquire the per-key lock. The caller must drop the returned guard
    /// before the next `acquire` for this key proceeds.
    pub async fn acquire(&self, key: &str) -> SingleflightGuard {
        let lock = self
            .inner
            .entry(key.to_string())
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone();
        let guard = lock.clone().lock_owned().await;
        SingleflightGuard {
            guard: Some(guard),
            map: &self.inner,
            key: key.to_string(),
            lock,
        }
    }
}

impl Default for Singleflight {
    fn default() -> Self {
        Self::new()
    }
}

pub struct SingleflightGuard<'a> {
    guard: Option<tokio::sync::OwnedMutexGuard<()>>,
    map: &'a DashMap<String, Arc<Mutex<()>>>,
    key: String,
    lock: Arc<Mutex<()>>,
}

impl Drop for SingleflightGuard<'_> {
    fn drop(&mut self) {
        // Release the inner mutex so its Arc clone is decremented BEFORE
        // remove_if runs its strong_count check. Then remove_if itself is
        // the only authoritative read (the DashMap shard is locked while
        // it runs, so no concurrent acquire can race in between).
        self.guard.take();
        self.map
            .remove_if(&self.key, |_, v| Arc::strong_count(v) <= 2);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::time::Duration;

    #[tokio::test]
    async fn coalesces_concurrent_acquires() {
        let sf = Arc::new(Singleflight::new());
        let in_flight = Arc::new(AtomicU32::new(0));
        let max_observed = Arc::new(AtomicU32::new(0));

        let mut handles = Vec::new();
        for _ in 0..32 {
            let sf = sf.clone();
            let inflight = in_flight.clone();
            let max = max_observed.clone();
            handles.push(tokio::spawn(async move {
                let _g = sf.acquire("k").await;
                let n = inflight.fetch_add(1, Ordering::SeqCst) + 1;
                max.fetch_max(n, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(2)).await;
                inflight.fetch_sub(1, Ordering::SeqCst);
            }));
        }
        for h in handles {
            h.await.unwrap();
        }
        assert_eq!(
            max_observed.load(Ordering::SeqCst),
            1,
            "only one task may hold the per-key slot at a time"
        );
    }

    #[tokio::test]
    async fn distinct_keys_run_in_parallel() {
        let sf = Arc::new(Singleflight::new());
        // Both should acquire concurrently — no contention across keys.
        let g1 = sf.acquire("k1").await;
        let g2 = sf.acquire("k2").await;
        drop(g1);
        drop(g2);
    }

    #[tokio::test]
    async fn entries_are_cleaned_up() {
        let sf = Singleflight::new();
        let g = sf.acquire("k").await;
        drop(g);
        assert_eq!(sf.inner.len(), 0, "map must not leak idle entries");
    }

    // TOCTOU collapse: under a tight pattern of acquire/drop/
    // acquire on the same key, the cleanup must never tear down an entry
    // that another acquirer is mid-claim of. The DashMap shard lock taken
    // by `remove_if` is the only authoritative read.
    #[tokio::test]
    async fn drop_then_immediate_reacquire_is_safe() {
        let sf = Arc::new(Singleflight::new());
        let mut handles = Vec::new();
        for _ in 0..64 {
            let sf = sf.clone();
            handles.push(tokio::spawn(async move {
                for _ in 0..32 {
                    let _g = sf.acquire("k").await;
                    tokio::task::yield_now().await;
                }
            }));
        }
        for h in handles {
            h.await.unwrap();
        }
        // After the storm completes, the map must end empty (no leak).
        assert_eq!(sf.inner.len(), 0);
    }
}
