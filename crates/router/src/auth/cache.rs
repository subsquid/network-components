use std::sync::Arc;
use std::time::{Duration, Instant};

use super::clock::{Clock, SystemClock};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum KeyState {
    Exists { user_id: String },
    Deleted,
    /// Network API was unreachable on the most recent attempt for this key.
    /// Cached briefly so the singleflight queue can drain without each
    /// waiter re-issuing a 250ms timeout. Recovery TTL is short (1s) so
    /// when Network API comes back, the cache un-poisons quickly.
    FailedRecently,
}

#[derive(Clone)]
struct Entry {
    state: KeyState,
    deadline: Instant,
}

const TTL_EXISTS: Duration = Duration::from_secs(60);
const TTL_DELETED: Duration = Duration::from_secs(15);
const TTL_FAIL_OPEN: Duration = Duration::from_secs(1);

pub struct KeyCache {
    inner: moka::sync::Cache<String, Entry>,
    clock: Arc<dyn Clock>,
}

impl KeyCache {
    pub fn new(capacity: u64) -> Self {
        Self::with_clock(capacity, Arc::new(SystemClock))
    }

    pub fn with_clock(capacity: u64, clock: Arc<dyn Clock>) -> Self {
        // Configure moka with a TTL ceiling equal to the longest semantic
        // TTL we use (TTL_EXISTS = 60s). Without it, expired entries stay
        // physically resident until LRU eviction triggers under capacity
        // pressure — which means a key flood evicts *valid* entries first
        // (LRU sees them as "older" than freshly-touched expired ones).
        // Per-entry semantic TTLs (Deleted=15s, FailedRecently=1s) are
        // shorter than this ceiling and remain enforced by `get` returning
        // None past the deadline; the moka TTL is just the upper bound that
        // guarantees eventual physical eviction of any entry.
        Self {
            inner: moka::sync::Cache::builder()
                .max_capacity(capacity)
                .time_to_live(TTL_EXISTS)
                .build(),
            clock,
        }
    }

    /// Read the current state for `token`.
    ///
    /// Note: the deadline check and the entry return are not atomic — the
    /// clock can advance past the deadline in the few ns between the check
    /// and the return. For 1s+ TTLs this is inconsequential, and we never
    /// rely on monotonic eviction at the boundary.
    ///
    /// We deliberately DO NOT call `invalidate(token)` on expired entries
    /// here. Pre-fix that did, and was racy: between reading the expired
    /// entry and the invalidate call, another writer could have replaced
    /// the entry with a fresh one (different value, fresh deadline);
    /// `invalidate` deletes by key and would wipe that fresh write.
    /// Instead we rely on (a) moka's `time_to_live` for eventual physical
    /// eviction and (b) `put_*` overwriting the slot whenever the next
    /// validate result arrives, so an expired entry is just a soft-miss
    /// that the next request resolves through the normal cache-miss path.
    pub fn get(&self, token: &str) -> Option<KeyState> {
        let entry = self.inner.get(token)?;
        if self.clock.now() >= entry.deadline {
            None
        } else {
            Some(entry.state)
        }
    }

    pub fn put_exists(&self, token: String, user_id: String, expires_at: Option<Instant>) {
        let now = self.clock.now();
        if let Some(exp) = expires_at {
            if exp <= now {
                self.put_deleted(token);
                return;
            }
        }
        let default_deadline = now + TTL_EXISTS;
        let deadline = match expires_at {
            Some(exp) => default_deadline.min(exp),
            None => default_deadline,
        };
        let entry = Entry {
            state: KeyState::Exists { user_id },
            deadline,
        };
        self.inner.insert(token, entry);
    }

    pub fn put_deleted(&self, token: String) {
        let deadline = self.clock.now() + TTL_DELETED;
        let entry = Entry {
            state: KeyState::Deleted,
            deadline,
        };
        self.inner.insert(token, entry);
    }

    /// Brief sentinel so concurrent waiters for the same token don't all
    /// re-issue the validate call after the leader returns FailOpen.
    pub fn put_failed_recently(&self, token: String) {
        let deadline = self.clock.now() + TTL_FAIL_OPEN;
        let entry = Entry {
            state: KeyState::FailedRecently,
            deadline,
        };
        self.inner.insert(token, entry);
    }

    #[cfg(test)]
    fn entry_count(&self) -> u64 {
        self.inner.run_pending_tasks();
        self.inner.entry_count()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::clock::TestClock;

    fn cache() -> (KeyCache, Arc<TestClock>) {
        let clock = TestClock::new();
        let cache = KeyCache::with_clock(10_000, clock.clone());
        (cache, clock)
    }

    fn exists(user: &str) -> KeyState {
        KeyState::Exists {
            user_id: user.into(),
        }
    }

    #[test]
    fn get_undefined_returns_none() {
        let (c, _) = cache();
        assert_eq!(c.get("unknown"), None, "UNDEFINED must be None, not Deleted");
    }

    #[test]
    fn put_exists_then_get() {
        let (c, _) = cache();
        c.put_exists("tok".into(), "u1".into(), None);
        assert_eq!(c.get("tok"), Some(exists("u1")));
    }

    #[test]
    fn put_deleted_then_get() {
        let (c, _) = cache();
        c.put_deleted("tok".into());
        assert_eq!(c.get("tok"), Some(KeyState::Deleted));
    }

    #[test]
    fn exists_default_ttl_60s() {
        let (c, clock) = cache();
        c.put_exists("tok".into(), "u".into(), None);
        clock.advance(Duration::from_secs(59));
        assert_eq!(c.get("tok"), Some(exists("u")));
        clock.advance(Duration::from_secs(2));
        assert_eq!(c.get("tok"), None);
    }

    #[test]
    fn deleted_default_ttl_15s() {
        let (c, clock) = cache();
        c.put_deleted("tok".into());
        clock.advance(Duration::from_secs(14));
        assert_eq!(c.get("tok"), Some(KeyState::Deleted));
        clock.advance(Duration::from_secs(2));
        assert_eq!(c.get("tok"), None);
    }

    #[test]
    fn expires_at_clamped_inside_window() {
        let (c, clock) = cache();
        let exp = clock.now() + Duration::from_secs(30);
        c.put_exists("tok".into(), "u".into(), Some(exp));
        clock.advance(Duration::from_secs(29));
        assert_eq!(c.get("tok"), Some(exists("u")));
        clock.advance(Duration::from_secs(2));
        assert_eq!(c.get("tok"), None, "clamp should expire at 30s, not 60s");
    }

    #[test]
    fn expires_at_already_expired_stores_deleted() {
        let (c, clock) = cache();
        let exp = clock.now() - Duration::from_secs(1);
        c.put_exists("tok".into(), "u".into(), Some(exp));
        assert_eq!(
            c.get("tok"),
            Some(KeyState::Deleted),
            "already-expired must downgrade to Deleted"
        );
        clock.advance(Duration::from_secs(14));
        assert_eq!(c.get("tok"), Some(KeyState::Deleted));
        clock.advance(Duration::from_secs(2));
        assert_eq!(c.get("tok"), None);
    }

    #[test]
    fn undefined_ne_deleted_regression() {
        let (c, _) = cache();
        assert_eq!(c.get("tok"), None);
        c.put_exists("tok".into(), "u".into(), None);
        let g = c.get("tok");
        assert!(matches!(g, Some(KeyState::Exists { .. })));
        assert_ne!(g, Some(KeyState::Deleted));
        assert_ne!(g, None);
    }

    #[test]
    fn capacity_evicts_under_pressure() {
        let clock = TestClock::new();
        let cache = KeyCache::with_clock(100, clock.clone());
        for i in 0..1_000 {
            cache.put_exists(format!("tok{i}"), "u".into(), None);
        }
        let count = cache.entry_count();
        assert!(
            count <= 100,
            "cache must respect capacity bound; got {count}"
        );
        // Evicted entries are UNDEFINED (None), not DELETED.
        let mut undefined = 0;
        for i in 0..1_000 {
            if cache.get(&format!("tok{i}")).is_none() {
                undefined += 1;
            }
        }
        assert!(undefined > 0, "some early entries must have been evicted");
    }
}
