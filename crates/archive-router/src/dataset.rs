use crate::error::Error;
use aws_sdk_s3::Client;
use serde::{Deserialize, Serialize};
use std::fs;
use std::io;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DataRange {
    pub from: i32,
    pub to: i32,
}

/// Directory in the dataset containing data for a certain range of blocks.
#[derive(Clone, Debug)]
pub struct DataDir {
    /// First block covered by the dir.
    pub from: i32,
    /// Last block covered by the dir (inclusive).
    pub to: i32,
    /// Size of dir in bytes.
    pub size: u64,
}

#[async_trait::async_trait]
pub trait Storage {
    /// Get data directories in the dataset.
    async fn get_data_directories(&mut self) -> Result<Vec<DataDir>, Error>;
}

pub struct DatasetStorage {
    storage: Box<dyn Storage + Send + Sync>,
}

impl DatasetStorage {
    pub fn new(storage: Box<dyn Storage + Send + Sync>) -> DatasetStorage {
        DatasetStorage { storage }
    }

    /**
    Get dataset ranges (data scheduling units).

    The resulting ranges are sorted and guaranteed to cover continues range of blocks
    starting from block 0.
    */
    pub async fn get_data_ranges(&mut self) -> Result<Vec<DataRange>, Error> {
        let mut dirs = self.storage.get_data_directories().await?;
        dirs.sort_by_key(|dir| dir.from);
        let mut ranges = vec![];

        for dir in dirs {
            if ranges.is_empty() {
                if dir.from == 0 {
                    ranges.push(dir);
                } else {
                    break;
                }
            } else {
                let current = ranges.last_mut().unwrap();
                if current.to + 1 != dir.from {
                    break;
                }
                if current.size > 20 * 1024 * 1024 * 1024 {
                    ranges.push(dir);
                } else {
                    current.size = dir.size;
                    current.to = dir.to;
                }
            }
        }

        // TODO: avoid extra iteration
        let ranges = ranges
            .into_iter()
            .map(|dir| DataRange {
                from: dir.from,
                to: dir.to,
            })
            .collect();
        Ok(ranges)
    }
}

fn dir_size(dir: &fs::DirEntry) -> io::Result<u64> {
    fs::read_dir(dir.path())?.try_fold(0, |size, entry| {
        let entry = entry?;
        Ok(size + entry.metadata()?.len())
    })
}

pub struct LocalStorage {
    dataset: String,
}

impl LocalStorage {
    pub fn new(dataset: String) -> Self {
        LocalStorage { dataset }
    }
}

fn invalid_folder_name(folder_name: &String) -> Error {
    Error::InvalidLayoutError(format!("invalid folder name - {folder_name}"))
}

#[async_trait::async_trait]
impl Storage for LocalStorage {
    async fn get_data_directories(&mut self) -> Result<Vec<DataDir>, Error> {
        let mut dirs = vec![];
        let dataset_dir = fs::read_dir(&self.dataset)?;
        for subfolder in dataset_dir {
            let subfolder = subfolder?;
            let subfolder_dir = fs::read_dir(subfolder.path())?;
            for folder in subfolder_dir {
                let folder = folder?;
                let folder_name = folder.file_name().into_string().map_err(|_| {
                    Error::InvalidLayoutError("parquet folder name contains invalid unicode".into())
                })?;

                let block_range = folder_name.split('-').collect::<Vec<&str>>();
                if block_range.len() != 2 {
                    return Err(invalid_folder_name(&folder_name));
                }

                let from = match block_range[0].parse() {
                    Ok(from) => from,
                    Err(..) => return Err(invalid_folder_name(&folder_name)),
                };
                let to = match block_range[1].parse() {
                    Ok(to) => to,
                    Err(..) => return Err(invalid_folder_name(&folder_name)),
                };

                dirs.push(DataDir {
                    from,
                    to,
                    size: dir_size(&folder)?,
                });
            }
        }
        Ok(dirs)
    }
}

fn invalid_object_key(key: &str) -> Error {
    Error::InvalidLayoutError(format!("invalid object key - {key}"))
}

pub struct S3Storage {
    client: Client,
    bucket: String,
    dirs: Vec<DataDir>,
    last_key: Option<String>,
}

#[async_trait::async_trait]
impl Storage for S3Storage {
    async fn get_data_directories(&mut self) -> Result<Vec<DataDir>, Error> {
        let mut objects = vec![];

        let mut builder = self.client.list_objects_v2().bucket(&self.bucket);
        if let Some(last_key) = &self.last_key {
            builder = builder.start_after(last_key);
        }
        let output = builder
            .send()
            .await
            .map_err(|err| Error::ReadDatasetError(Box::new(err)))?;

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
                .map_err(|err| Error::ReadDatasetError(Box::new(err)))?;
            continuation_token = output.next_continuation_token.clone();
            if let Some(contents) = output.contents() {
                objects.extend_from_slice(contents);
            }
        }

        let mut last_key = None;
        let mut dirs = vec![];
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
            let folder = splitted
                .get(1)
                .ok_or_else(|| invalid_object_key(blocks_key))?;
            let block_range = folder.split('-').collect::<Vec<_>>();
            let size = chunk.iter().fold(0, |size, o| size + o.size());

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

            let dir = DataDir {
                from,
                to,
                size: size.try_into().unwrap(),
            };
            dirs.push(dir);
        }

        if last_key.is_some() {
            self.last_key = last_key.map(|key| key.to_string());
        }

        self.dirs.append(&mut dirs.clone());
        Ok(dirs)
    }
}

impl S3Storage {
    pub fn new(client: Client, bucket: String) -> Self {
        S3Storage {
            client,
            bucket,
            dirs: vec![],
            last_key: None,
        }
    }
}
