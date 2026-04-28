use std::collections::HashMap;
use std::sync::Mutex;

use prometheus::IntCounterVec;

/// Bounded-cardinality "top-N" sketch (Space-Saving / Metwally, with one
/// deliberate deviation). Keeps at most `capacity` keys; on overflow, the
/// lowest-count entry is replaced with the incoming key, which starts at
/// **count 1** — not `min_count + 1` as in the canonical Metwally
/// algorithm. The deviation prevents a flood of unique one-hit keys from
/// inheriting and inflating the min count, which would otherwise let them
/// crowd out a stable heavy hitter. The trade-off (slower convergence for
/// late-arriving genuine heavy hitters) is acceptable here because the
/// goal is bounded Prometheus label cardinality, not exact rank.
/// Returns the evicted key so the caller can clear any external labels
/// for it (e.g. Prometheus).
pub struct TopKeys {
    inner: Mutex<Inner>,
}

struct Inner {
    capacity: usize,
    counts: HashMap<String, u64>,
}

#[derive(Debug, Clone)]
pub struct ObserveResult {
    /// Whether the caller should emit a metric for this `key_id`.
    pub emit: bool,
    /// If non-None, the caller should clear any external label for this id.
    pub evicted: Option<String>,
}

impl TopKeys {
    pub fn new(capacity: usize) -> Self {
        Self {
            inner: Mutex::new(Inner {
                capacity,
                counts: HashMap::new(),
            }),
        }
    }

    /// Update the sketch and emit the corresponding Prometheus metric
    /// updates atomically under the same lock. This prevents a race where
    /// two concurrent observers' eviction-then-emit sequence could
    /// interleave such that an evicted label is re-added (silently
    /// breaking the cardinality bound).
    pub fn observe_into(&self, key_id: &str, counter: &IntCounterVec) -> ObserveResult {
        let mut g = self.inner.lock().unwrap();
        let result = Self::update_locked(&mut g, key_id);
        if let Some(ref evicted) = result.evicted {
            let _ = counter.remove_label_values(&[evicted.as_str()]);
        }
        if result.emit {
            counter.with_label_values(&[key_id]).inc();
        }
        result
    }

    /// Sketch-only update without metric side effects. Exposed for unit
    /// tests; production callers should use `observe_into` so the metric
    /// mutation is serialised with the sketch update.
    #[cfg(test)]
    pub fn observe(&self, key_id: &str) -> ObserveResult {
        let mut g = self.inner.lock().unwrap();
        Self::update_locked(&mut g, key_id)
    }

    fn update_locked(g: &mut Inner, key_id: &str) -> ObserveResult {
        if let Some(c) = g.counts.get_mut(key_id) {
            *c += 1;
            return ObserveResult {
                emit: true,
                evicted: None,
            };
        }
        if g.counts.len() < g.capacity {
            g.counts.insert(key_id.to_string(), 1);
            return ObserveResult {
                emit: true,
                evicted: None,
            };
        }
        // Cap full → replace the min entry. New keys start at count 1
        // (rather than `min_count + 1` per Metwally) so a flood of unique
        // one-hit keys cannot inflate their own counts and crowd out
        // stable heavy hitters. The cost: a genuine late-arriving heavy
        // hitter starts from 1 and may take many observations to climb.
        // Acceptable here — we want stable bounded Prometheus labels, not
        // Space-Saving rank optimality.
        let min_key = g
            .counts
            .iter()
            .min_by_key(|(_, c)| *c)
            .map(|(k, _)| k.clone())
            .expect("counts non-empty when capacity reached");
        g.counts.remove(&min_key);
        g.counts.insert(key_id.to_string(), 1);
        ObserveResult {
            emit: true,
            evicted: Some(min_key),
        }
    }

    #[cfg(test)]
    pub fn contains(&self, key_id: &str) -> bool {
        self.inner.lock().unwrap().counts.contains_key(key_id)
    }

    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.inner.lock().unwrap().counts.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn requests_by_key_bounded_cardinality() {
        let tk = TopKeys::new(100);
        let mut tracked: HashSet<String> = HashSet::new();
        for i in 0..10_000 {
            let k = format!("k{i}");
            let r = tk.observe(&k);
            if r.emit {
                tracked.insert(k);
            }
            if let Some(e) = r.evicted {
                tracked.remove(&e);
            }
        }
        assert!(
            tracked.len() <= 100,
            "tracked label set must stay ≤ capacity (got {})",
            tracked.len()
        );
        assert!(tk.len() <= 100);
    }

    #[test]
    fn top_keys_are_present() {
        let tk = TopKeys::new(10);
        for _ in 0..100 {
            tk.observe("A");
        }
        // Now churn with many one-hit keys.
        for i in 0..1_000 {
            tk.observe(&format!("nobody{i}"));
        }
        assert!(
            tk.contains("A"),
            "high-count key A must remain tracked through churn"
        );
        assert!(tk.len() <= 10);
    }

    #[test]
    fn observe_returns_evicted_on_overflow() {
        let tk = TopKeys::new(2);
        let r1 = tk.observe("a");
        assert!(r1.emit && r1.evicted.is_none());
        let r2 = tk.observe("b");
        assert!(r2.emit && r2.evicted.is_none());
        let r3 = tk.observe("c");
        assert!(r3.emit && r3.evicted.is_some());
    }

    // sketch update + Prometheus mutation are atomic.
    // Many threads observing distinct keys must end with the registered
    // label set bounded by capacity. Pre-fix, a stale `with_label_values`
    // racing with another thread's `remove_label_values` could resurrect
    // an evicted label.
    #[test]
    fn observe_into_keeps_prometheus_cardinality_bounded_under_concurrency() {
        use prometheus::core::Collector;
        use prometheus::{opts, register_int_counter_vec, IntCounterVec};
        use std::sync::Arc;
        use std::thread;

        // Use a unique metric per test to avoid colliding with the global
        // REQUESTS_BY_KEY across other tests.
        let counter: IntCounterVec = register_int_counter_vec!(
            opts!(
                "topkeys_concurrency_test_counter",
                "Concurrency test for TopKeys::observe_into"
            ),
            &["key"]
        )
        .unwrap();
        let tk = Arc::new(TopKeys::new(50));
        let mut handles = Vec::new();
        for t in 0..16 {
            let tk = tk.clone();
            let counter = counter.clone();
            handles.push(thread::spawn(move || {
                for i in 0..1_000 {
                    let k = format!("t{t}_k{i}");
                    tk.observe_into(&k, &counter);
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        let metrics = counter.collect();
        let total_labels: usize = metrics.iter().map(|m| m.get_metric().len()).sum();
        assert!(
            total_labels <= 50,
            "Prometheus cardinality must stay ≤ capacity even under \
             concurrent observe_into calls; got {total_labels}"
        );
    }

    // new keys arriving on overflow start at count 1, not at
    // `min_count + 1`. A flood of distinct keys cannot inflate their own
    // counts and crowd out a stable heavy hitter (A:100 here).
    #[test]
    fn new_keys_start_at_one_not_inheriting_min_count() {
        let tk = TopKeys::new(3);
        for _ in 0..100 {
            tk.observe("A"); // A reaches count 100
        }
        for _ in 0..2 {
            tk.observe("filler"); // ensure capacity is full
        }
        // At this point counts = {A:100, filler:2, ...} or similar; cap=3.
        // Overflow with a new key:
        let _ = tk.observe("first_overflow");
        // first_overflow should have count 1, not min+1 (=3) — meaning the
        // next overflow will evict it back out, not promote it past A.
        for i in 0..50 {
            tk.observe(&format!("nobody{i}"));
        }
        assert!(tk.contains("A"), "A must remain through churn");
    }
}
