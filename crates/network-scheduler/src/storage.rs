use std::ops::Deref;
use std::time::Duration;

use aws_sdk_s3 as s3;
use aws_sdk_s3::error::SdkError;
use aws_sdk_s3::types::Object;
use itertools::Itertools;
use nonempty::NonEmpty;
use tokio::sync::mpsc::{self, Receiver};

use crate::cli::Config;
use crate::data_chunk::DataChunk;
use crate::scheduler::Scheduler;
use crate::scheduling_unit::{bundle_chunks, SchedulingUnit};

const SCHEDULER_STATE_KEY: &str = "scheduler_state.json";

#[derive(Clone)]
struct DatasetStorage {
    bucket: String,
    client: s3::Client,
    last_key: Option<String>,
    last_block: Option<u32>,
}

#[derive(Debug, Clone)]
struct S3Object {
    prefix: String,
    // common prefix identifies all objects belonging to a single chunk
    file_name: String,
    size: u64,
}

impl S3Object {
    fn key(&self) -> String {
        format!("{}/{}", self.prefix, self.file_name)
    }
}

impl TryFrom<Object> for S3Object {
    type Error = &'static str;

    fn try_from(obj: Object) -> Result<Self, Self::Error> {
        let key = obj.key.ok_or("Object key missing")?;
        let (prefix, file_name) = match key.rsplit_once('/') {
            Some((prefix, file_name)) => (prefix.to_string(), file_name.to_string()),
            None => return Err("Invalid key (no prefix)"),
        };
        let size = obj.size as u64;
        Ok(Self {
            prefix,
            file_name,
            size,
        })
    }
}

impl DatasetStorage {
    pub fn new(bucket: String, client: s3::Client) -> Self {
        Self {
            bucket,
            client,
            last_key: None,
            last_block: None,
        }
    }

    async fn list_all_new_objects(&self) -> anyhow::Result<Vec<S3Object>> {
        let mut last_key = self.last_key.clone();
        let mut result = Vec::new();
        loop {
            let s3_result = self
                .client
                .list_objects_v2()
                .bucket(&self.bucket)
                .set_start_after(last_key)
                .send()
                .await?;
            let objects: NonEmpty<S3Object> = match s3_result
                .contents
                .and_then(|objects| {
                    objects
                        .into_iter()
                        .map(TryInto::try_into)
                        .collect::<Result<Vec<S3Object>, &'static str>>()
                        .ok()
                })
                .and_then(NonEmpty::from_vec)
            {
                Some(objects) => objects,
                None => break,
            };
            last_key = Some(objects.last().key());
            result.extend(objects)
        }

        Ok(result)
    }

    async fn list_all_new_chunks(&mut self) -> anyhow::Result<Vec<DataChunk>> {
        let objects = match NonEmpty::from_vec(self.list_all_new_objects().await?) {
            None => return Ok(Vec::new()),
            Some(objects) => objects,
        };
        let last_key = objects.last().key();
        let chunks = objects
            .into_iter()
            .group_by(|obj| obj.prefix.clone())
            .into_iter()
            .map(|(_, objects)| self.objects_to_chunk(objects))
            .collect::<anyhow::Result<Vec<DataChunk>>>()?;

        // Verify if chunks are continuous
        let mut next_block = self.last_block.map(|x| x + 1).unwrap_or_default();
        for chunk in chunks.iter() {
            if chunk.block_range.begin != next_block {
                anyhow::bail!(
                    "Blocks {} to {} missing from {}",
                    next_block,
                    chunk.block_range.begin - 1,
                    self.bucket
                );
            }
            next_block = chunk.block_range.end + 1;
        }

        self.last_key = Some(last_key);
        self.last_block = chunks.last().map(|c| c.block_range.end);
        Ok(chunks)
    }

    fn objects_to_chunk(
        &self,
        objs: impl IntoIterator<Item = S3Object>,
    ) -> anyhow::Result<DataChunk> {
        let mut last_key = None;
        let mut blocks_file_present = false;
        let mut size_bytes = 0u64;
        for obj in objs {
            last_key = Some(obj.key());
            blocks_file_present |= obj.file_name == "blocks.parquet";
            size_bytes += obj.size;
        }
        let last_key = match last_key {
            Some(key) => key,
            None => anyhow::bail!("Empty object group"),
        };
        if !blocks_file_present {
            anyhow::bail!("blocks.parquet missing")
        }
        let chunk = match DataChunk::new(&self.bucket, &last_key, size_bytes) {
            Ok(chunk) => chunk,
            Err(_) => anyhow::bail!("Invalid data chunk"),
        };
        log::debug!("Downloaded chunk {chunk:?}");

        Ok(chunk)
    }

    pub fn get_incoming_chunks(mut self) -> Receiver<NonEmpty<DataChunk>> {
        let (sender, receiver) = mpsc::channel(100);
        tokio::spawn(async move {
            log::info!("Reading chunks from bucket {}", self.bucket);
            loop {
                let chunks = match self.list_all_new_chunks().await {
                    Ok(chunks) => chunks,
                    Err(e) => {
                        log::error!("Error getting data chunks: {e:?}");
                        tokio::time::sleep(Duration::from_secs(60)).await;
                        continue;
                    }
                };
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

pub struct S3Storage {
    client: s3::Client,
    config: &'static Config,
}

impl S3Storage {
    pub async fn new() -> Self {
        let config = Config::get();
        let s3_config = aws_config::from_env()
            .endpoint_url(config.s3_endpoint.clone())
            .load()
            .await;
        let client = s3::Client::new(&s3_config);
        Self { client, config }
    }

    pub async fn get_incoming_units(&self) -> Receiver<SchedulingUnit> {
        let (unit_sender, unit_receiver) = mpsc::channel(100);

        for bucket in self.config.dataset_buckets.clone() {
            let storage = DatasetStorage::new(bucket, self.client.clone());
            let incoming_chunks = storage.get_incoming_chunks();
            bundle_chunks(
                incoming_chunks,
                unit_sender.clone(),
                self.config.scheduling_unit_size,
            );
        }
        unit_receiver
    }

    pub async fn load_scheduler(&self) -> anyhow::Result<Scheduler> {
        let api_result = self
            .client
            .get_object()
            .bucket(&self.config.scheduler_state_bucket)
            .key(SCHEDULER_STATE_KEY)
            .send()
            .await;
        let bytes = match api_result {
            Ok(res) => res.body.collect().await?.to_vec(),
            Err(SdkError::ServiceError(e)) if e.err().is_no_such_key() => {
                log::warn!("Scheduler state not found. Initializing with blank state.");
                return Ok(Scheduler::default());
            }
            Err(e) => return Err(anyhow::anyhow!(e)),
        };
        Ok(serde_json::from_slice(&bytes)?)
    }

    pub async fn save_scheduler<T: Deref<Target = Scheduler>>(
        &self,
        scheduler: T,
    ) -> anyhow::Result<()> {
        let state = serde_json::to_vec(scheduler.deref())?;
        self.client
            .put_object()
            .bucket(&self.config.scheduler_state_bucket)
            .key(SCHEDULER_STATE_KEY)
            .body(state.into())
            .send()
            .await?;
        Ok(())
    }
}
