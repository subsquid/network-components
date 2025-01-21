use std::fmt::Display;
use std::io::Write;
use std::sync::Arc;
use std::time::Duration;

use aws_sdk_s3 as s3;
use aws_sdk_s3::error::SdkError;
use aws_sdk_s3::types::Object;
use chrono::Utc;
use flate2::write::GzEncoder;
use flate2::Compression;
use itertools::Itertools;
use nonempty::NonEmpty;
use sha2::{Digest, Sha256};
use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio::sync::Mutex;

use sqd_network_transport::util::{CancellationToken, TaskManager};

use crate::cli::Config;
use crate::data_chunk::DataChunk;
use crate::prometheus_metrics;
use crate::scheduler::Scheduler;
use crate::scheduling_unit::{bundle_chunks, SchedulingUnit};
use sqd_messages::assignments::{Assignment, NetworkAssignment, NetworkState};

const ASSIGNMENT_EFFECTIVE_FROM_TIMEOUT_SEC: u64 = 30;

#[derive(Clone)]
struct DatasetStorage {
    bucket: String,
    client: s3::Client,
    last_key: Option<String>,
    last_block: Option<u64>,
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
        let size = obj.size.unwrap_or_default() as u64;
        Ok(Self {
            prefix,
            file_name,
            size,
        })
    }
}

impl DatasetStorage {
    pub fn new(bucket: impl ToString, client: s3::Client) -> Self {
        Self {
            bucket: bucket.to_string(),
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
            prometheus_metrics::s3_request();
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

    // TODO: consider accepting `&self` instead.
    // The async operation is blocking the entire object only to quickly mutate the state at the end.
    async fn list_all_new_chunks(&mut self) -> anyhow::Result<Vec<DataChunk>> {
        let objects = match NonEmpty::from_vec(self.list_all_new_objects().await?) {
            None => return Ok(Vec::new()),
            Some(objects) => objects,
        };
        let last_key = objects.last().key();
        let chunks = objects
            .into_iter()
            .chunk_by(|obj| obj.prefix.clone())
            .into_iter()
            .map(|(_, objects)| self.objects_to_chunk(objects))
            .collect::<anyhow::Result<Vec<DataChunk>>>()?;

        // Verify if chunks are continuous
        let mut next_block = self.last_block.map(|x| x + 1);
        for chunk in chunks.iter() {
            if next_block.is_some_and(|next_block| chunk.block_range.begin != next_block) {
                anyhow::bail!(
                    "Blocks {} to {} missing from {}",
                    next_block.unwrap(),
                    chunk.block_range.begin - 1,
                    self.bucket
                );
            }
            next_block = Some(chunk.block_range.end + 1);
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
        let mut filenames = Vec::new();
        for obj in objs {
            last_key = Some(obj.key());
            blocks_file_present |= obj.file_name == "blocks.parquet";
            filenames.push(obj.file_name);
            size_bytes += obj.size;
        }
        let last_key = match last_key {
            Some(key) => key,
            None => anyhow::bail!("Empty object group"),
        };
        if !blocks_file_present {
            // block.parquet is always the last file written in a chunk.
            // So if it is present, all the other files are present too
            anyhow::bail!("blocks.parquet missing")
        }
        let chunk = match DataChunk::new(&self.bucket, &last_key, size_bytes, filenames) {
            Ok(chunk) => chunk,
            Err(_) => anyhow::bail!("Invalid data chunk"),
        };
        log::debug!("Downloaded chunk {chunk:?}");

        Ok(chunk)
    }

    pub async fn get_incoming_chunks(
        mut self,
        sender: Sender<NonEmpty<DataChunk>>,
        cancel_token: CancellationToken,
    ) {
        log::info!("Reading chunks from bucket {}", self.bucket);
        loop {
            let chunks_res = tokio::select! {
                res = self.list_all_new_chunks() => res,
                _ = cancel_token.cancelled() => break,
            };
            let chunks = match chunks_res {
                Ok(chunks) => chunks,
                Err(e) => {
                    log::error!("Error getting data chunks: {e:?}");
                    tokio::select! {
                        _ = tokio::time::sleep(Duration::from_secs(60)) => continue,
                        _ = cancel_token.cancelled() => break,
                    }
                }
            };
            let chunks = match NonEmpty::from_vec(chunks) {
                Some(chunks) => chunks,
                None => {
                    log::info!("Now more chunks for now. Waiting...");
                    tokio::select! {
                        _ = tokio::time::sleep(Duration::from_secs(300)) => continue,
                        _ = cancel_token.cancelled() => break,
                    }
                }
            };
            log::info!(
                "Downloaded {} new chunks from bucket {}",
                chunks.len(),
                self.bucket
            );

            match sender.send(chunks).await {
                Err(_) => break,
                Ok(_) => continue,
            }
        }
        log::info!("Chunks stream ended");
    }
}

#[derive(Clone)]
pub struct S3Storage {
    client: s3::Client,
    config: &'static Config,
    scheduler_state_key: String,
    task_manager: Arc<Mutex<TaskManager>>,
}

impl S3Storage {
    pub async fn new(scheduler_id: impl Display) -> Self {
        let config = Config::get();
        let s3_config = aws_config::from_env()
            .endpoint_url(&config.s3_endpoint)
            .load()
            .await;
        let client = s3::Client::new(&s3_config);
        let scheduler_state_key = format!("scheduler_{scheduler_id}.json");
        Self {
            client,
            config,
            scheduler_state_key,
            task_manager: Default::default(),
        }
    }

    pub async fn get_incoming_units(&self) -> Receiver<SchedulingUnit> {
        let (unit_sender, unit_receiver) = mpsc::channel(100);
        let mut task_manager = self.task_manager.lock().await;
        let scheduling_unit_size = self.config.scheduling_unit_size;
        for bucket in self.config.dataset_buckets.iter() {
            let (chunk_sender, chunk_receiver) = mpsc::channel(100);
            let storage = DatasetStorage::new(bucket, self.client.clone());
            task_manager
                .spawn(|cancel_token| storage.get_incoming_chunks(chunk_sender, cancel_token));
            task_manager.spawn(|cancel_token| {
                bundle_chunks(
                    chunk_receiver,
                    unit_sender.clone(),
                    scheduling_unit_size,
                    cancel_token,
                )
            });
        }
        unit_receiver
    }

    pub async fn load_scheduler(&self) -> anyhow::Result<Scheduler> {
        let api_result = self
            .client
            .get_object()
            .bucket(&self.config.scheduler_state_bucket)
            .key(&self.scheduler_state_key)
            .send()
            .await;
        prometheus_metrics::s3_request();
        let bytes = match api_result {
            Ok(res) => res.body.collect().await?.to_vec(),
            Err(SdkError::ServiceError(e)) if e.err().is_no_such_key() => {
                log::warn!("Scheduler state not found. Initializing with blank state.");
                return Ok(Scheduler::default());
            }
            Err(e) => return Err(anyhow::anyhow!(e)),
        };
        let mut scheduler = Scheduler::from_json(bytes.as_ref())?;
        // List of datasets could have changed since last run, need to clear deprecated units
        scheduler.clear_deprecated_units();
        Ok(scheduler)
    }

    pub async fn save_scheduler(&self, scheduler_state: Vec<u8>) {
        log::debug!("Saving scheduler state");
        let _ = self
            .client
            .put_object()
            .bucket(&self.config.scheduler_state_bucket)
            .key(&self.scheduler_state_key)
            .body(scheduler_state.into())
            .send()
            .await
            .map_err(|e| log::error!("Error saving scheduler state: {e:?}"));
        prometheus_metrics::s3_request();
    }

    pub async fn save_assignment(&self, assignment: Assignment) {
        log::debug!("Encoding assignment");
        let mut encoder = GzEncoder::new(Vec::new(), Compression::best());
        let _ = encoder.write_all(serde_json::to_vec(&assignment).unwrap().as_slice());
        let compressed_bytes = encoder.finish().unwrap();
        log::debug!("Saving assignment");
        let mut hasher = Sha256::new();
        hasher.update(compressed_bytes.as_slice());
        let hash = hasher.finalize();
        let network = Config::get().network.clone();
        let current_time = Utc::now();
        let timestamp = current_time.format("%FT%T");
        let filename: String = format!("assignments/{network}/{timestamp}_{hash:X}.json.gz");

        let saving_result = self
            .client
            .put_object()
            .bucket(&self.config.scheduler_state_bucket)
            //.bucket("network-scheduler-state")
            .key(&filename)
            .body(compressed_bytes.into())
            .send()
            .await;
        prometheus_metrics::s3_request();
        match saving_result {
            Ok(_) => {}
            Err(e) => {
                log::error!("Error saving assignment: {e:?}");
                return;
            }
        }

        let system_time = std::time::SystemTime::now();
        let mut effective_from = system_time
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        effective_from += ASSIGNMENT_EFFECTIVE_FROM_TIMEOUT_SEC;

        let network_state = NetworkState {
            network: Config::get().network.clone(),
            assignment: NetworkAssignment {
                url: format!("https://metadata.sqd-datasets.io/{filename}"),
                id: format!("{timestamp}_{hash:X}"),
                effective_from,
            },
        };
        let contents = serde_json::to_vec(&network_state).unwrap();
        let _ = self
            .client
            .put_object()
            .bucket(&self.config.scheduler_state_bucket)
            //.bucket("network-scheduler-state")
            .key(Config::get().network_state_name.clone())
            .body(contents.into())
            .send()
            .await
            .map_err(|e| log::error!("Error saving link to assignment: {e:?}"));
        prometheus_metrics::s3_request();
        self.cleanup_assignments().await;
    }

    pub async fn cleanup_assignments(&self) {
        let network = Config::get().network.clone();
        let path = format!("assignments/{network}/");
        let Ok(files) = self
            .client
            .list_objects()
            .bucket(&self.config.scheduler_state_bucket)
            .prefix(path)
            .send()
            .await
        else {
            log::error!("Failed to list assignments");
            return;
        };
        prometheus_metrics::s3_request();
        for v in files
            .contents()
            .iter()
            .rev()
            .skip(Config::get().assignment_history_len)
        {
            let Some(ref filepath) = v.key else {
                log::error!("Failed to fetch filepath");
                return;
            };
            let result = self
                .client
                .delete_object()
                .bucket(&self.config.scheduler_state_bucket)
                .key(filepath)
                .send()
                .await;
            prometheus_metrics::s3_request();
            match result {
                Ok(_) => log::debug!("S3 object {filepath} deleted."),
                Err(err) => {
                    log::error!("Can not delete S3 object {filepath}: {err:?}");
                    return;
                }
            }
        }
    }
}
