use std::time::{Duration, Instant};
use std::collections::HashMap;

use crate::controller::{Url, Dataset};


#[derive(Clone)]
struct RequestInfo {
    url: Url,
    expiration: Instant,
}


impl RequestInfo {
    fn new(val: Url, duration: Duration) -> RequestInfo {
        RequestInfo {
            url: val,
            expiration: Instant::now() + duration,
        }
    }

    fn is_expired(&self) -> bool {
        Instant::now() > self.expiration
    }
}


pub struct RequestTracker {
    inner: parking_lot::RwLock<RequestTrackerInner>,
}


struct RequestTrackerInner {
    requests: HashMap<(Dataset, u32), RequestInfo>,
    lifetime: Duration,
    cleanup_interval: Duration,
    cleanup_time: Instant,
}


impl RequestTrackerInner {
    fn new() -> RequestTrackerInner {
        let interval = Duration::from_secs(60 * 5);
        RequestTrackerInner {
            requests: HashMap::new(),
            lifetime: Duration::from_secs(90),
            cleanup_interval: interval,
            cleanup_time: Instant::now() + interval,
        }
    }

    fn get(&self, key: &(Dataset, u32)) -> Option<Url> {
        if let Some(val) = self.requests.get(key) {
            if !val.is_expired() {
                return Some(val.url.clone())
            }
        }
        None
    }

    fn insert(&mut self, key: (Dataset, u32), value: Url) {
        let req = RequestInfo::new(value, self.lifetime);
        self.requests.insert(key, req);

        if Instant::now() > self.cleanup_time {
            self.remove_expired();
            self.cleanup_time = Instant::now() + self.cleanup_interval;
        }
    }

    fn remove_expired(&mut self) {
        self.requests.retain(|_, value| !value.is_expired());
    }
}


impl RequestTracker {
    pub fn new() -> RequestTracker {
        RequestTracker {
            inner: parking_lot::RwLock::new(RequestTrackerInner::new()),
        }
    }

    pub fn get(&self, key: &(Dataset, u32)) -> Option<Url> {
        let inner = self.inner.read();
        inner.get(key)
    }

    pub fn insert(&self, key: (Dataset, u32), value: Url) {
        let mut inner = self.inner.write();
        inner.insert(key, value);
    }
}
