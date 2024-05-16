use std::str::FromStr;

use aws_sdk_s3::Client;
use router_controller::data_chunk::DataChunk;

#[async_trait::async_trait]
pub trait Storage {
    /// Get data chunks in the dataset.
    async fn get_chunks(&self, next_block: u32) -> Result<Vec<DataChunk>, String>;
}

fn invalid_object_key(key: &str) -> String {
    format!("invalid object key - {key}")
}

fn trim_trailing_slash(value: &str) -> String {
    let mut value = value.to_string();
    value.pop();
    value
}

pub struct S3Storage {
    client: Client,
    bucket: String,
}

#[async_trait::async_trait]
impl Storage for S3Storage {
    async fn get_chunks(&self, next_block: u32) -> Result<Vec<DataChunk>, String> {
        let mut objects = vec![];

        let prefix = None;
        let tops = self.ls(prefix).await?;

        let mut top: Option<String> = None;
        for item in tops.into_iter().rev() {
            let block_num = item.parse::<u32>().map_err(|err| err.to_string())?;
            top = Some(item);
            if block_num <= next_block {
                break;
            }
        }

        if let Some(top) = top {
            let prefix = format!("{}/", top);
            let top_chunks = self.ls(Some(&prefix)).await?;

            let mut next_chunk: Option<DataChunk> = None;
            for chunk in top_chunks {
                let chunk = DataChunk::from_str(&chunk)
                    .map_err(|_| format!("invalid chunk name - {}", &chunk))?;
                if chunk.first_block() == next_block || next_block == 0 {
                    next_chunk = Some(chunk);
                    break;
                }
            }

            if let Some(chunk) = next_chunk {
                let start_after = chunk.to_string();
                let output = self
                    .client
                    .list_objects_v2()
                    .bucket(&self.bucket)
                    .start_after(start_after)
                    .send()
                    .await
                    .map_err(|err| err.to_string())?;
                let mut continuation_token = output.next_continuation_token.clone();
                if let Some(contents) = output.contents() {
                    objects.extend_from_slice(contents);
                }
                while let Some(token) = continuation_token {
                    let output = self
                        .client
                        .list_objects_v2()
                        .bucket(&self.bucket)
                        .continuation_token(token)
                        .send()
                        .await
                        .map_err(|err| err.to_string())?;
                    continuation_token = output.next_continuation_token.clone();
                    if let Some(contents) = output.contents() {
                        objects.extend_from_slice(contents);
                    }
                }
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

        Ok(chunks)
    }
}

impl S3Storage {
    pub fn new(client: Client, bucket: String) -> Self {
        S3Storage { client, bucket }
    }

    async fn ls(&self, prefix: Option<&str>) -> Result<Vec<String>, String> {
        let mut builder = self
            .client
            .list_objects_v2()
            .bucket(&self.bucket)
            .delimiter('/');
        if let Some(prefix) = prefix {
            builder = builder.prefix(prefix)
        }
        let output = builder.send()
            .await
            .map_err(|err| err.to_string())?;
        let mut items = vec![];
        if let Some(prefixes) = output.common_prefixes() {
            for prefix in prefixes {
                if let Some(value) = prefix.prefix() {
                    let value = trim_trailing_slash(value);
                    items.push(value);
                }
            }
        }
        Ok(items)
    }
}
