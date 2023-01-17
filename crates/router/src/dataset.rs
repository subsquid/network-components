use crate::error::Error;
use archive_router_controller::data_chunk::DataChunk;
use aws_sdk_s3::Client;
use tokio::runtime::Handle;

pub trait Storage {
    /// Get data chunks in the dataset.
    fn get_chunks(&mut self, next_block: u32) -> Result<Vec<DataChunk>, Error>;
}

fn invalid_object_key(key: &str) -> Error {
    Error::InvalidLayoutError(format!("invalid object key - {key}"))
}

fn extract_last_block(last_key: &str) -> u32 {
    let segments = last_key.split('/').collect::<Vec<&str>>();
    let range = segments[1].split('-').collect::<Vec<&str>>();
    range[1].parse().unwrap()
}

pub struct S3Storage {
    client: Client,
    bucket: String,
    last_key: Option<String>,
}

impl Storage for S3Storage {
    fn get_chunks(&mut self, next_block: u32) -> Result<Vec<DataChunk>, Error> {
        let mut objects = vec![];
        if let Some(last_key) = &self.last_key {
            let last_block = extract_last_block(last_key);
            assert_eq!(last_block + 1, next_block);
        }

        let handle = Handle::current();
        let mut builder = self.client.list_objects_v2().bucket(&self.bucket);
        if let Some(last_key) = &self.last_key {
            builder = builder.start_after(last_key);
        }
        let output = handle
            .block_on(builder.send())
            .map_err(|err| Error::ReadDatasetError(Box::new(err)))?;

        let mut continuation_token = output.next_continuation_token.clone();
        if let Some(contents) = output.contents() {
            objects.extend_from_slice(contents);
        }
        while let Some(token) = continuation_token {
            let future = self
                .client
                .list_objects_v2()
                .bucket(&self.bucket)
                .continuation_token(token)
                .send();
            let output = handle
                .block_on(future)
                .map_err(|err| Error::ReadDatasetError(Box::new(err)))?;
            continuation_token = output.next_continuation_token.clone();
            if let Some(contents) = output.contents() {
                objects.extend_from_slice(contents);
            }
        }

        let mut last_key = None;
        let mut chunks = vec![];
        for chunk in objects.chunks_exact(3) {
            let blocks_key = chunk[0]
                .key()
                .ok_or_else(|| Error::InvalidLayoutError("invalid object key".into()))?;
            if !blocks_key.ends_with("blocks.parquet") {
                break;
            }
            let logs_key = chunk[1]
                .key()
                .ok_or_else(|| Error::InvalidLayoutError("invalid object key".into()))?;
            if !logs_key.ends_with("logs.parquet") {
                break;
            }
            let transactions_key = chunk[2]
                .key()
                .ok_or_else(|| Error::InvalidLayoutError("invalid object key".into()))?;
            if !transactions_key.ends_with("transactions.parquet") {
                break;
            }

            let splitted = blocks_key.split('/').collect::<Vec<_>>();
            let top = splitted
                .first()
                .ok_or_else(|| invalid_object_key(blocks_key))?
                .parse()
                .map_err(|_| invalid_object_key(blocks_key))?;
            let folder = splitted
                .get(1)
                .ok_or_else(|| invalid_object_key(blocks_key))?;
            let block_range = folder.split('-').collect::<Vec<_>>();

            let from = block_range
                .first()
                .ok_or_else(|| invalid_object_key(blocks_key))?
                .parse()
                .map_err(|_| invalid_object_key(blocks_key))?;
            let to = block_range
                .get(1)
                .ok_or_else(|| invalid_object_key(blocks_key))?
                .parse()
                .map_err(|_| invalid_object_key(blocks_key))?;

            last_key = Some(transactions_key);

            let data_chunk = DataChunk::new(top, from, to);
            chunks.push(data_chunk);
        }

        if last_key.is_some() {
            self.last_key = last_key.map(|key| key.to_string());
        }

        Ok(chunks)
    }
}

impl S3Storage {
    pub fn new(client: Client, bucket: String) -> Self {
        S3Storage {
            client,
            bucket,
            last_key: None,
        }
    }
}
