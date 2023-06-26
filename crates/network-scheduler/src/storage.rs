use std::time::Duration;

use aws_sdk_s3 as s3;
use aws_sdk_s3::types::Object;
use itertools::Itertools;
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

impl S3Storage {
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
        let objects = self.list_all_new_objects().await?;
        objects
            .into_iter()
            .group_by(|obj| obj.prefix.clone())
            .into_iter()
            .map(|(_, objects)| self.objects_to_chunk(objects))
            .collect()
    }

    // async fn list_next_chunks(&mut self) -> Option<impl Iterator<Item = DataChunk> + '_> {
    //     let objects = match self.list_next_objects().await {
    //         Ok(objects) => objects,
    //         Err(e) => {
    //             log::error!("Error listing objects: {e:?}");
    //             return None;
    //         }
    //     };
    //
    //     chunks.peek().is_some().then_some(chunks)
    // }

    // async fn chunk_size(&self, key: &str) -> anyhow::Result<u64> {
    //     let prefix = match key.rsplit_once('/') {
    //         Some((prefix, _)) => prefix,
    //         None => anyhow::bail!("Invalid key"),
    //     };
    //     Ok(self
    //         .client
    //         .list_objects_v2()
    //         .bucket(&self.bucket)
    //         .prefix(prefix)
    //         .send()
    //         .await?
    //         .contents()
    //         .ok_or_else(|| anyhow::anyhow!("Empty response from storage"))?
    //         .iter()
    //         .map(|o| o.size() as u64)
    //         .sum())
    // }

    fn objects_to_chunk(
        &mut self,
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

        let next_block = self.last_block.map(|x| x + 1).unwrap_or_default();
        if chunk.block_range.begin != next_block {
            anyhow::bail!(
                "Blocks {} to {} missing from {}",
                next_block,
                chunk.block_range.begin,
                self.bucket
            );
        }

        self.last_key = Some(last_key);
        self.last_block = Some(chunk.block_range.end);
        Ok(chunk)
    }

    // async fn list_all_new_chunks(&mut self) -> Vec<DataChunk> {
    //     let mut result = Vec::new();
    //     while let Some(chunks) = self.list_next_chunks().await {
    //         result.extend(chunks)
    //     }
    //     result
    // }

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
