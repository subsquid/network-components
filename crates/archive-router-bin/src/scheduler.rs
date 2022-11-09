use archive_router::ArchiveRouter;
use std::sync::{Arc, Mutex};
use std::time::Duration;

pub fn start(router: Arc<Mutex<ArchiveRouter>>, interval: Duration) {
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(interval).await;
            let mut router = router.lock().unwrap();
            router.schedule();
        }
    });
}
