use std::collections::HashMap;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use tracing::{error, info};

use router_controller::controller::Controller;
use router_controller::data_chunk::DataChunk;

use crate::dataset::Storage;
use crate::metrics::{DATASET_HEIGHT, DATASET_SYNC_ERRORS};

pub fn start(
    controller: Arc<Controller>,
    storages: HashMap<String, Arc<dyn Storage + Sync + Send>>,
    interval: Duration,
) {
    tokio::task::spawn_blocking(move || {
        info!("syncing available datasets before scheduling");

        let get_chunks =
            |storage: &Arc<dyn Storage + Sync + Send>, next_block, dataset: &String| {
                info!("downloading new chunks for {}", dataset);
                match storage.get_chunks(next_block) {
                    Ok(chunks) => {
                        info!("found new chunks in {}: {:?}", dataset, chunks);

                        if let Some(chunk) = chunks.last() {
                            DATASET_HEIGHT
                                .with_label_values(&[dataset])
                                .set(chunk.last_block().into())
                        }

                        Ok(chunks)
                    }
                    Err(err) => {
                        error!("failed to download new chunks for {}: {:?}", dataset, err);
                        DATASET_SYNC_ERRORS.with_label_values(&[dataset]).inc();
                        Err(())
                    }
                }
            };

        let rt_handle = tokio::runtime::Handle::current();
        let mut handles = Vec::with_capacity(storages.len());
        for (dataset, storage) in storages.clone() {
            let handle = tokio::task::spawn_blocking(move || {
                let result = get_chunks(&storage, 0, &dataset);
                (dataset, result)
            });
            handles.push(handle);
        }
        let mut preloaded_chunks: HashMap<String, Result<Vec<DataChunk>, ()>> = HashMap::new();
        for handle in handles {
            let (dataset, result) = rt_handle.block_on(handle).unwrap();
            preloaded_chunks.insert(dataset, result);
        }

        controller.sync_datasets(|dataset, _| preloaded_chunks.remove(dataset).unwrap());

        info!("started scheduling task with {:?} interval", interval);
        loop {
            thread::sleep(interval);
            info!("started scheduling");
            controller.schedule(|dataset, next_block| {
                let storage = storages.get(dataset).unwrap();
                get_chunks(storage, next_block, dataset)
            });
            info!("finished scheduling");
        }
    });
}
