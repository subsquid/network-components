use archive_router::dataset::DatasetStorage;
use archive_router::ArchiveRouter;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tracing::{debug, error};

pub fn start(
    router: Arc<Mutex<ArchiveRouter>>,
    storage: Arc<tokio::sync::Mutex<DatasetStorage>>,
    interval: Duration,
) {
    debug!("started scheduling task with {:?} interval", &interval);
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(interval).await;
            debug!("started scheduling");
            let mut storage = storage.lock().await;
            let ranges = match storage.get_data_ranges().await {
                Ok(ranges) => ranges,
                Err(e) => {
                    error!("error occured while dataset syncronization: {:?}", e);
                    continue;
                }
            };
            let mut router = router.lock().unwrap();
            router.update_ranges(ranges);
            router.schedule();
            debug!("finished scheduling");
        }
    });
}
