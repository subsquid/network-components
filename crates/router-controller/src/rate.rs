use std::time::SystemTime;


pub struct RateMeter {
    window: Vec<u32>,
    last_update: u64,
    slot_duration_seconds: u64,
}


impl RateMeter {
    pub fn new(window_size: usize, slot_duration_seconds: u64) -> RateMeter {
        RateMeter {
            window: vec![0; window_size],
            last_update: 0,
            slot_duration_seconds,
        }
    }

    pub fn inc(&mut self, count: u32, now: Option<u64>) {
        let window_len = self.window.len() as u64;
        let now = self.to_time(now);
        let mut cutoff = now.saturating_sub(window_len);
        if self.last_update > cutoff {
            while cutoff > self.last_update - window_len {
                self.window[(cutoff % window_len) as usize] = 0;
                cutoff -= 1;
            }
        } else {
            self.window.fill(0);
        }
        self.window[(now % window_len) as usize] += count;
        self.last_update = now;
    }

    pub fn get_rate(&self, now: Option<u64>) -> u32 {
        let now = self.to_time(now);
        let cutoff = now.saturating_sub(self.window.len() as u64);
        let mut time = self.last_update;
        let mut rate = 0;
        while time > cutoff {
            rate += self.window[(time % self.window.len() as u64) as usize];
            time -= 1;
        }
        rate
    }

    fn to_time(&self, now: Option<u64>) -> u64 {
        let now = now.unwrap_or_else(|| {
            SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs()
        });
        let time_value = (now / self.slot_duration_seconds).max(self.last_update);
        assert!(time_value > self.window.len() as u64);
        time_value
    }
}

#[cfg(test)]
mod tests {
    use std::thread;
    use std::time::Duration;

    use crate::rate::RateMeter;

    #[test]
    fn check_multiple_inc() {
        let mut rate = RateMeter::new(1, 1);
        rate.inc(1, None);
        rate.inc(2, None);

        let value = rate.get_rate(None);
        assert!(value == 3)
    }

    #[test]
    fn check_rate_after_window_round() {
        let mut rate = RateMeter::new(1, 1);
        rate.inc(1, None);

        let value = rate.get_rate(None);
        assert!(value == 1);

        thread::sleep(Duration::from_secs(1));
        let value = rate.get_rate(None);
        assert!(value == 0);
    }
}
