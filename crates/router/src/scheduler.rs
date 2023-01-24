use std::collections::HashMap;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use tracing::{error, info};

use router_controller::controller::Controller;

use crate::dataset::Storage;

pub fn start(
    controller: Arc<Controller>,
    mut storages: HashMap<String, Box<dyn Storage + Send>>,
    interval: Duration,
) {
    tokio::task::spawn_blocking(move || {
        info!("started scheduling task with {:?} interval", interval);
        loop {
            thread::sleep(interval);
            info!("started scheduling");
            controller.schedule(|dataset, next_block| {
                info!("downloading new chunks for {}", dataset);
                let storage = storages.get_mut(dataset).unwrap();
                match storage.get_chunks(next_block) {
                    Ok(chunks) => {
                        info!("found new chunks in {}: {:?}", dataset, chunks);
                        Ok(chunks)
                    }
                    Err(err) => {
                        error!("failed to download new chunks for {}: {:?}", dataset, err);
                        Err(())
                    }
                }
            });
            info!("finished scheduling");
        }
    });
}
