use std::time::SystemTime;


pub struct RateMeter {
    window: Vec<u32>,
    time: u64,
}


impl RateMeter {
    pub fn new() -> RateMeter {
        RateMeter {
            window: vec![0; 37],
            time: 0,
        }
    }

    pub fn inc(&mut self, count: u32, now: Option<u64>) {
        let window_len = self.window.len() as u64;
        let now = self.to_time(now);
        let mut cutoff = now.saturating_sub(window_len);
        if self.time > cutoff {
            while cutoff < self.time - window_len {
                self.window[(cutoff % window_len) as usize] = 0;
                cutoff += 1;
            }
        } else {
            self.window.iter_mut().for_each(|x| *x = 0);
        }
        self.window[(now % window_len) as usize] += count;
        self.time = now;
    }

    pub fn get_rate(&self, now: Option<u64>) -> u32 {
        let now = self.to_time(now);
        let cutoff = now.saturating_sub(self.window.len() as u64);
        let mut time = self.time;
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
        let time_value = (now / 5).max(self.time);
        assert!(time_value > self.window.len() as u64);
        time_value
    }
}
