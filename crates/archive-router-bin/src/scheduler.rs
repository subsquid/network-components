use archive_router::ArchiveRouter;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

pub fn start(router: Arc<Mutex<ArchiveRouter>>, interval: Duration) {
    thread::spawn(move || loop {
        thread::sleep(interval);
        let mut router = router.lock().unwrap();
        router.schedule().ok(); // .ok() disables warning about an unused result
    });
}
