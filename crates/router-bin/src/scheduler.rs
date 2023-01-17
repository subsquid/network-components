use archive_router::config::Config;
use archive_router::dataset::Storage;
use archive_router_controller::controller::Controller;
use std::collections::HashMap;
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use tracing::debug;

pub fn start(
    controller: Arc<Controller<Config>>,
    mut storages: HashMap<String, Box<dyn Storage + Send>>,
    interval: Duration,
) {
    tokio::task::spawn_blocking(move || {
        debug!("started scheduling task with {:?} interval", interval);
        loop {
            thread::sleep(interval);
            debug!("started scheduling");
            controller.schedule(|dataset, next_block| {
                debug!("downloading chunks for {}", dataset);
                let storage = storages.get_mut(dataset).unwrap();
                storage.get_chunks(next_block).map_err(|_| ())
            });
            debug!("finished scheduling");
        }
    });
}
