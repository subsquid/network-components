use std::sync::Arc;
use std::time::{Duration, Instant};

use tracing::{error, info};

use router_controller::controller::Controller;

use crate::dataset::Dataset;
use crate::metrics::{DATASET_HEIGHT, DATASET_SYNC_ERRORS};

async fn update_datasets(controller: &Arc<Controller>, datasets: &Vec<Dataset>) {
    let mut tasks = Vec::with_capacity(datasets.len());

    for dataset in datasets.clone() {
        let controller = controller.clone();
        tasks.push(tokio::spawn(async move {
            let next_block = controller.get_height(dataset.name())
                .expect("dataset must be supported")
                .map(|height| u32::try_from(height).expect("next block can't be negative") + 1)
                .unwrap_or(dataset.start_block().unwrap_or(0));

            let chunks = match dataset.storage().get_chunks(next_block).await {
                Ok(chunks) => {
                    info!("found new chunks in {}: {:?}", dataset.url(), chunks);
                    if let Some(chunk) = chunks.last() {
                        DATASET_HEIGHT
                            .with_label_values(&[&dataset.url()])
                            .set(chunk.last_block().into())
                    }
                    chunks
                },
                Err(err) => {
                    error!("failed to download new chunks for {}: {:?}", dataset.url(), err);
                    DATASET_SYNC_ERRORS.with_label_values(&[dataset.url()]).inc();
                    return
                }
            };

            if let Err(err) = controller.update_dataset(dataset.url(), chunks) {
                error!("failed to update dataset {}: {:?}", dataset.url(), err);
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
