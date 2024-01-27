use std::sync::Arc;
use std::time::{Duration, Instant};

use tracing::{error, info};

use router_controller::controller::Controller;

use crate::dataset::Storage;
use crate::metrics::{DATASET_HEIGHT, DATASET_SYNC_ERRORS};

type Dataset = (String, String, Arc<dyn Storage + Sync + Send>);

async fn update_datasets(controller: &Arc<Controller>, datasets: &Vec<Dataset>) {
    let mut tasks = Vec::with_capacity(datasets.len());

    for (name, dataset, storage) in datasets.clone() {
        let controller = controller.clone();
        tasks.push(tokio::spawn(async move {
            let next_block = controller.get_height(&name)
                .expect("dataset must be supported")
                .map(|height| height + 1)
                .unwrap_or(0)
                .try_into()
                .expect("next block can't be negative");

            let chunks = match storage.get_chunks(next_block).await {
                Ok(chunks) => {
                    info!("found new chunks in {}: {:?}", dataset, chunks);
                    if let Some(chunk) = chunks.last() {
                        DATASET_HEIGHT
                            .with_label_values(&[&dataset])
                            .set(chunk.last_block().into())
                    }
                    chunks
                },
                Err(err) => {
                    error!("failed to download new chunks for {}: {:?}", dataset, err);
                    DATASET_SYNC_ERRORS.with_label_values(&[&dataset]).inc();
                    return
                }
            };

            if let Err(err) = controller.update_dataset(&dataset, chunks) {
                error!("failed to update dataset {}: {:?}", dataset, err);
            }
        }));
    }

    for task in tasks {
        task.await.unwrap();
    }
}

pub fn start(
    controller: Arc<Controller>,
    datasets: Vec<Dataset>,
    interval: Duration,
) {
    tokio::spawn(async move {
        let schedule_time = Instant::now() + Duration::from_secs(90);

        info!("updating datasets before scheduling");
        update_datasets(&controller, &datasets).await;

        let now = Instant::now();
        if let Some(duration) = schedule_time.checked_duration_since(now) {
            tokio::time::sleep(duration).await;
        }
        controller.schedule();

        info!("started scheduling task with {:?} interval", interval);
        loop {
            tokio::time::sleep(interval).await;
            info!("started scheduling");
            update_datasets(&controller, &datasets).await;
            controller.schedule();
            info!("finished scheduling");
        }
    });
}
