use archive_router::dataset::DatasetStorage;
use archive_router::ArchiveRouter;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

pub fn start<S: DatasetStorage + Send + 'static>(
    router: Arc<Mutex<ArchiveRouter<S>>>,
    interval: Duration,
) {
    thread::spawn(move || loop {
        thread::sleep(interval);
        let mut router = router.lock().unwrap();
        router.schedule().ok(); // .ok() disables warning about an unused result
    });
}
