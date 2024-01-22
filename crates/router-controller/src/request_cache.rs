use std::time::{Duration, Instant};
use std::collections::HashMap;

use crate::controller::{Url, Dataset};


#[derive(Clone)]
struct RequestInfo {
    url: Url,
    expiration: Instant,
}


impl RequestInfo {
    fn new(val: Url, expiration: Instant) -> RequestInfo {
        RequestInfo {
            url: val,
            expiration,
        }
    }

    fn is_expired(&self, now: Instant) -> bool {
        now > self.expiration
    }
}


pub struct RequestCache {
    inner: parking_lot::RwLock<RequestCacheInner>,
}


struct RequestCacheInner {
    requests: HashMap<(Dataset, u32), RequestInfo>,
    lifetime: Duration,
    cleanup_interval: Duration,
    cleanup_time: Instant,
}


impl RequestCacheInner {
    fn new() -> RequestCacheInner {
        let interval = Duration::from_secs(60 * 5);
        RequestCacheInner {
            requests: HashMap::new(),
            lifetime: Duration::from_secs(90),
            cleanup_interval: interval,
            cleanup_time: Instant::now() + interval,
        }
    }

    fn get(&self, key: &(Dataset, u32), now: Instant) -> Option<Url> {
        if let Some(val) = self.requests.get(key) {
            if !val.is_expired(now) {
                return Some(val.url.clone())
            }
        }
        None
    }

    fn insert(&mut self, key: (Dataset, u32), value: Url, now: Instant) {
        let req = RequestInfo::new(value, now + self.lifetime);
        self.requests.insert(key, req);

        if now > self.cleanup_time {
            self.remove_expired(now);
            self.cleanup_time = now + self.cleanup_interval;
        }
    }

    fn remove_expired(&mut self, now: Instant) {
        self.requests.retain(|_, value| !value.is_expired(now));
    }
}


impl RequestCache {
    pub fn new() -> RequestCache {
        RequestCache {
            inner: parking_lot::RwLock::new(RequestCacheInner::new()),
        }
    }

    pub fn get(&self, key: &(Dataset, u32)) -> Option<Url> {
        let now = Instant::now();
        let inner = self.inner.read();
        inner.get(key, now)
    }

    pub fn insert(&self, key: (Dataset, u32), value: Url) {
        let now = Instant::now();
        let mut inner = self.inner.write();
        inner.insert(key, value, now);
    }
}
