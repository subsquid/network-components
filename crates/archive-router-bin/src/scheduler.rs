use archive_router::dataset::DatasetStorage;
use archive_router::ArchiveRouter;
use std::sync::{Arc, Mutex};
use std::time::Duration;

pub fn start(router: Arc<Mutex<ArchiveRouter>>, storage: Arc<DatasetStorage>, interval: Duration) {
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(interval).await;
            let ranges = match storage.get_data_ranges().await {
                Ok(ranges) => ranges,
                Err(e) => {
                    eprintln!("Error occured while scheduling {:?}", e);
                    continue;
                }
            };
            let mut router = router.lock().unwrap();
            router.schedule(&ranges).ok(); // .ok() disables warning about an unused result
        }
    });
}
