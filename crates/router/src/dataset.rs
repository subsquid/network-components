use std::str::FromStr;

use crate::error::Error;
use aws_sdk_s3::Client;
use router_controller::data_chunk::DataChunk;
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

        let mut chunks = vec![];
        for object in &objects {
            if let Some(key) = object.key() {
                if key.ends_with("blocks.parquet") {
                    match DataChunk::from_str(key) {
                        Ok(chunk) => chunks.push(chunk),
                        Err(..) => return Err(invalid_object_key(key)),
                    }
                }
            }
        }

        if let Some(last_chunk) = chunks.last() {
            for object in objects.iter().rev() {
                if let Some(key) = object.key() {
                    let chunk = DataChunk::from_str(key).unwrap();
                    if &chunk == last_chunk {
                        self.last_key = Some(key.to_string());
                        break;
                    }
                }
            }
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
