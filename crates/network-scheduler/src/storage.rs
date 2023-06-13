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
    last_key: Option<String>,
    last_block: Option<u32>,
}

impl S3Storage {
    pub fn new(bucket: String, client: s3::Client) -> Self {
        Self {
            bucket,
            client,
            last_key: None,
            last_block: None,
        }
    }

    async fn list_next_objects(&self) -> anyhow::Result<Vec<Object>> {
        let s3_result = self
            .client
            .list_objects_v2()
            .bucket(&self.bucket)
            .set_start_after(self.last_key.clone())
            .send()
            .await?;

        let objects = match s3_result.contents {
            Some(contents) if !contents.is_empty() => contents,
            _ => return Ok(vec![]),
        };

        Ok(objects)
    }

    async fn list_next_chunks(&mut self) -> Option<impl Iterator<Item = DataChunk> + '_> {
        let objects = match self.list_next_objects().await {
            Ok(objects) => objects,
            Err(e) => {
                log::error!("Error listing objects: {e:?}");
                return None;
            }
        };

        let mut chunks = objects
            .into_iter()
            .filter_map(|obj| self.obj_to_chunk(obj))
            .peekable();
        chunks.peek().is_some().then_some(chunks)
    }

    fn obj_to_chunk(&mut self, obj: Object) -> Option<DataChunk> {
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
        log::debug!("Downloaded chunk {chunk:?}");

        let next_block = self.last_block.map(|x| x + 1).unwrap_or_default();
        if chunk.block_range.begin != next_block {
            log::error!(
                "Blocks {} to {} missing from {}",
                next_block,
                chunk.block_range.begin,
                self.bucket
            );
            return None;
        }

        self.last_key = Some(key);
        self.last_block = Some(chunk.block_range.end);
        Some(chunk)
    }

    async fn list_all_new_chunks(&mut self) -> Vec<DataChunk> {
        let mut result = Vec::new();
        while let Some(chunks) = self.list_next_chunks().await {
            result.extend(chunks)
        }
        result
    }

    pub fn get_incoming_chunks(mut self) -> Receiver<NonEmpty<DataChunk>> {
        let (sender, receiver) = mpsc::channel(100);
        tokio::spawn(async move {
            log::info!("Reading chunks from bucket {}", self.bucket);
            loop {
                let chunks = self.list_all_new_chunks().await;
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
