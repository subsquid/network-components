use std::time::Duration;

use aws_sdk_s3 as s3;
use aws_sdk_s3::types::Object;
use nonempty::NonEmpty;
use tokio::sync::mpsc::{self, Receiver};

use crate::data_chunk::DataChunk;
use crate::scheduling_unit::{bundle_chunks, SchedulingUnit};

#[derive(Clone)]
struct S3Storage {
    bucket: String,
    client: s3::Client,
}

impl S3Storage {
    pub fn new(bucket: String, client: s3::Client) -> Self {
        Self { bucket, client }
    }

    async fn list_objects(&self, last_key: &mut Option<String>) -> anyhow::Result<Vec<Object>> {
        let s3_result = self
            .client
            .list_objects_v2()
            .bucket(&self.bucket)
            .set_start_after(last_key.clone())
            .send()
            .await?;

        let objects = match s3_result.contents {
            Some(contents) if !contents.is_empty() => contents,
            _ => return Ok(vec![]),
        };

        *last_key = Some(
            objects
                .last()
                .unwrap()
                .key()
                .ok_or_else(|| anyhow::anyhow!("Object key missing"))?
                .to_string(),
        );

        Ok(objects)
    }

    async fn download_available_chunks(&self, last_key: &mut Option<String>) -> Vec<DataChunk> {
        let mut result = Vec::new();
        loop {
            match self.list_objects(last_key).await {
                Ok(objects) if objects.is_empty() => break,
                Ok(objects) => {
                    result.extend(objects.into_iter().filter_map(|obj| self.obj_to_chunk(obj)))
                }
                Err(e) => {
                    log::error!("Error listing objects: {e:?}");
                    break;
                }
            }
        }
        result
    }

    fn obj_to_chunk(&self, obj: Object) -> Option<DataChunk> {
        let key = match obj.key() {
            Some(key) if key.ends_with("blocks.parquet") => key.to_string(),
            _ => return None,
        };
        let chunk = match DataChunk::new(&self.bucket, &key) {
            Ok(chunk) => chunk,
            Err(_) => {
                log::error!("Invalid data chunk: {}", key);
                return None;
            }
        };
        Some(chunk)
    }

    pub fn get_incoming_chunks(self) -> Receiver<NonEmpty<DataChunk>> {
        let (sender, receiver) = mpsc::channel(100);
        tokio::spawn(async move {
            log::info!("Reading chunks from bucket {}", self.bucket);
            let mut last_key = None;
            loop {
                let chunks = self.download_available_chunks(&mut last_key).await;
                let chunks = match NonEmpty::from_vec(chunks) {
                    Some(chunks) => chunks,
                    None => {
                        log::info!("Now more chunks for now. Waiting...");
                        tokio::time::sleep(Duration::from_secs(300)).await;
                        continue;
                    }
                };
                log::info!(
                    "Downloaded {} new chunks from bucket {}",
                    chunks.len(),
                    self.bucket
                );

                match sender.send(chunks).await {
                    Err(_) => return,
                    Ok(_) => continue,
                }
            }
        });
        receiver
    }
}

pub async fn get_incoming_units(
    s3_endpoint: String,
    buckets: Vec<String>,
    unit_size: usize,
) -> anyhow::Result<Receiver<SchedulingUnit>> {
    let config = aws_config::from_env()
        .endpoint_url(s3_endpoint)
        .load()
        .await;
    let client = s3::Client::new(&config);

    let (unit_sender, unit_receiver) = mpsc::channel(100);

    for bucket in buckets {
        let storage = S3Storage::new(bucket, client.clone());
        let incoming_chunks = storage.get_incoming_chunks();
        bundle_chunks(incoming_chunks, unit_sender.clone(), unit_size);
    }
    Ok(unit_receiver)
}
