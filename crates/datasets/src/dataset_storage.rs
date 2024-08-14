use aws_sdk_s3 as s3;
use aws_sdk_s3::types::Object;
use itertools::Itertools;
use nonempty::NonEmpty;

use crate::data_chunk::DataChunk;
// use crate::prometheus_metrics;

#[derive(Clone)]
pub struct DatasetStorage {
    pub bucket: String,
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

    pub async fn list_all_new_chunks(&mut self) -> anyhow::Result<Vec<DataChunk>> {
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
            // prometheus_metrics::s3_request();
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
        log::debug!("Downloaded chunk {chunk}");

        Ok(chunk)
    }
}
