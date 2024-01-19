use std::collections::HashMap;

use crate::rate::RateMeter;
use crate::controller::Url;


pub struct WorkersRate {
    inner: parking_lot::RwLock<HashMap<Url, RateMeter>>,
}


impl WorkersRate {
    pub fn new() -> WorkersRate {
        WorkersRate {
            inner: parking_lot::RwLock::new(HashMap::new())
        }
    }

    pub fn get_rate(&self, url: &Url) -> u32 {
        let inner = self.inner.read();
        let rate = inner.get(url)
            .map(|rate| rate.get_rate(None))
            .unwrap_or(0);
        rate
    }

    pub fn inc(&self, url: &Url) {
        let mut inner = self.inner.write();
        inner.entry(url.clone())
            .and_modify(|rate| rate.inc(1, None))
            .or_insert_with(|| {
                let mut rate = RateMeter::new();
                rate.inc(1, None);
                rate
            });
    }

    pub fn clear(&self) {
        let mut inner = self.inner.write();
        inner.clear();
    }
}
