use std::cmp::min;
use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::str::FromStr;
use std::time::Duration;

use aws_sdk_s3 as s3;
use aws_sdk_s3::types::Object;
use sha3::{Digest, Sha3_256};
use subsquid_network_transport::PeerId;
use tokio::sync::mpsc::{self, Receiver, Sender};

use router_controller::messages::WorkerState;
use router_controller::range::Range;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ChunkId([u8; 32]);

impl ChunkId {
    pub fn distance(&self, peer_id: &PeerId) -> [u8; 32] {
        // Take 32 *last* bytes of the peer ID, because initial bytes of a multihash are metadata
        let peer_id = peer_id.to_bytes();
        let num_bytes = min(32, peer_id.len());
        let mut result = [0u8; 32];
        result[..num_bytes].copy_from_slice(&peer_id[(peer_id.len() - num_bytes)..]);

        // Compute XOR with chunk ID bytes
        for i in 0..32 {
            result[i] ^= self.0[i];
        }
        result
    }
}

impl Display for ChunkId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        hex::encode(self.0).fmt(f)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct DataChunk {
    dataset_url: String,
    block_range: Range,
}

impl Display for DataChunk {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        format!(
            "{}/{}-{}",
            self.dataset_url, self.block_range.begin, self.block_range.end
        )
        .fmt(f)
    }
}

impl DataChunk {
    pub fn new(bucket: &str, chunk_str: &str) -> anyhow::Result<Self> {
        let chunk = router_controller::data_chunk::DataChunk::from_str(chunk_str)
            .map_err(|_| anyhow::anyhow!("Invalid chunk: {chunk_str}"))?;
        Ok(Self {
            dataset_url: format!("s3://{bucket}"),
            block_range: chunk.into(),
        })
    }

    pub fn id(&self) -> ChunkId {
        let mut result = [0u8; 32];
        let mut hasher = Sha3_256::default();
        hasher.update(self.to_string().as_bytes());
        Digest::finalize_into(hasher, result.as_mut_slice().into());
        ChunkId(result)
    }
}

pub fn chunks_to_worker_state(chunks: impl IntoIterator<Item = DataChunk>) -> WorkerState {
    let datasets = chunks
        .into_iter()
        .fold(
            HashMap::<String, Vec<Range>>::new(),
            |mut datasets, chunk| {
                datasets
                    .entry(chunk.dataset_url)
                    .or_default()
                    .push(chunk.block_range);
                datasets
            },
        )
        .into_iter()
        .map(|(dataset, ranges)| (dataset, ranges.into()))
        .collect();
    WorkerState { datasets }
}

#[derive(Clone)]
struct S3Storage {
    bucket: String,
    client: s3::Client,
}

impl S3Storage {
    pub fn new(bucket: String, client: s3::Client) -> anyhow::Result<Self> {
        Ok(Self { bucket, client })
    }

    async fn list_objects(
        &self,
        last_key: Option<String>,
    ) -> anyhow::Result<Option<(String, Vec<Object>)>> {
        let s3_result = self
            .client
            .list_objects_v2()
            .bucket(&self.bucket)
            .set_start_after(last_key)
            .send()
            .await?;

        let objects = match s3_result.contents {
            Some(contents) if !contents.is_empty() => contents,
            _ => return Ok(None),
        };

        let last_key = objects
            .last()
            .unwrap()
            .key()
            .ok_or_else(|| anyhow::anyhow!("Object key missing"))?
            .to_string();

        return Ok(Some((last_key, objects)));
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

    pub async fn send_chunks(self, sender: Sender<DataChunk>) {
        log::info!("Reading chunks from bucket {}", self.bucket);
        let mut last_key = None;
        loop {
            let objects = match self.list_objects(last_key.clone()).await {
                Ok(Some((key, objects))) => {
                    last_key = Some(key);
                    objects
                }
                Ok(None) => {
                    log::info!("Now more chunks for now. Waiting...");
                    tokio::time::sleep(Duration::from_secs(60)).await;
                    continue;
                }
                Err(e) => {
                    log::error!("Error listing objects: {e:?}");
                    tokio::time::sleep(Duration::from_secs(60)).await;
                    continue;
                }
            };

            let chunks: Vec<DataChunk> = objects
                .into_iter()
                .filter_map(|obj| self.obj_to_chunk(obj))
                .collect();
            log::info!(
                "Downloaded {} new chunks from bucket {}",
                chunks.len(),
                self.bucket
            );
            for chunk in chunks {
                match sender.send(chunk).await {
                    Err(_) => return,
                    Ok(_) => continue,
                }
            }
        }
    }
}

pub async fn get_incoming_chunks(
    s3_endpoint: String,
    buckets: Vec<String>,
) -> anyhow::Result<Receiver<DataChunk>> {
    let config = aws_config::from_env()
        .endpoint_url(s3_endpoint)
        .load()
        .await;
    let client = s3::Client::new(&config);

    let (sender, receiver) = mpsc::channel(1024);

    for bucket in buckets {
        let storage = S3Storage::new(bucket, client.clone())?;
        let sender = sender.clone();
        tokio::spawn(storage.send_chunks(sender));
    }
    Ok(receiver)
}
