use std::sync::Arc;
use std::time::Instant;

pub trait Clock: Send + Sync + 'static {
    fn now(&self) -> Instant;
}

pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> Instant {
        Instant::now()
    }
}

#[cfg(test)]
pub struct TestClock {
    now: std::sync::Mutex<Instant>,
}

#[cfg(test)]
impl TestClock {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            now: std::sync::Mutex::new(Instant::now()),
        })
    }

    pub fn advance(&self, d: std::time::Duration) {
        let mut g = self.now.lock().unwrap();
        *g += d;
    }
}

#[cfg(test)]
impl Clock for TestClock {
    fn now(&self) -> Instant {
        *self.now.lock().unwrap()
    }
}

pub fn system_clock() -> Arc<dyn Clock> {
    Arc::new(SystemClock)
}
