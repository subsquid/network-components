use crate::error::Error;
use aws_sdk_s3::Client;
use serde::{Deserialize, Serialize};
use std::fs;
use std::io;

#[derive(Clone, Serialize, Deserialize)]
pub struct DataRange {
    pub from: i32,
    pub to: i32,
}

/// Directory in the dataset containing data for a certain range of blocks.
#[derive(Debug)]
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
    async fn get_data_directories(&self) -> Result<Vec<DataDir>, Error>;
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
    pub async fn get_data_ranges(&self) -> Result<Vec<DataRange>, Error> {
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

    pub async fn get_dataset_range(&self) -> Result<DataRange, Error> {
        let ranges = self.get_data_ranges().await?;

        if ranges.is_empty() {
            return Ok(DataRange { from: -1, to: -1 });
        }

        Ok(DataRange {
            from: ranges.first().unwrap().from,
            to: ranges.last().unwrap().to,
        })
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

#[async_trait::async_trait]
impl Storage for LocalStorage {
    async fn get_data_directories(&self) -> Result<Vec<DataDir>, Error> {
        let mut dirs = vec![];
        let dataset_dir = fs::read_dir(&self.dataset)?;
        for subfolder in dataset_dir {
            let subfolder = subfolder?;
            let subfolder_dir = fs::read_dir(subfolder.path())?;
            for folder in subfolder_dir {
                let folder = folder?;
                let folder_name = folder.file_name().into_string()?;

                let block_range = folder_name.split('-').collect::<Vec<&str>>();
                if block_range.len() != 2 {
                    return Err(Error::ParquetFolderNameError(Box::new(folder_name)));
                }

                let from = match block_range[0].parse() {
                    Ok(from) => from,
                    Err(..) => return Err(Error::ParquetFolderNameError(Box::new(folder_name))),
                };
                let to = match block_range[1].parse() {
                    Ok(to) => to,
                    Err(..) => return Err(Error::ParquetFolderNameError(Box::new(folder_name))),
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

pub struct S3Storage {
    client: Client,
    bucket: String,
}

#[async_trait::async_trait]
impl Storage for S3Storage {
    async fn get_data_directories(&self) -> Result<Vec<DataDir>, Error> {
        let objects = self
            .client
            .list_objects()
            .bucket(&self.bucket)
            .send()
            .await
            .map_err(|_| Error::ReadDatasetError)?;
        let dirs = objects
            .contents()
            .unwrap_or_default()
            .chunks(3)
            .map(|chunk| {
                let key = chunk[0].key().ok_or(Error::ReadDatasetError)?;
                let splitted = key.split('/').collect::<Vec<_>>();
                let folder = splitted.get(1).ok_or(Error::ReadDatasetError)?;
                let block_range = folder.split('-').collect::<Vec<_>>();
                let size = chunk.iter().fold(0, |size, o| size + o.size());

                let from = block_range
                    .first()
                    .ok_or_else(|| Error::ParquetFolderNameError(Box::new(folder.to_string())))?
                    .parse()
                    .map_err(|_| Error::ParquetFolderNameError(Box::new(folder.to_string())))?;
                let to = block_range
                    .get(1)
                    .ok_or_else(|| Error::ParquetFolderNameError(Box::new(folder.to_string())))?
                    .parse()
                    .map_err(|_| Error::ParquetFolderNameError(Box::new(folder.to_string())))?;

                Ok(DataDir {
                    from,
                    to,
                    size: size.try_into().unwrap(),
                })
            })
            .collect::<Result<Vec<DataDir>, Error>>()?;
        Ok(dirs)
    }
}

impl S3Storage {
    pub fn new(client: Client, bucket: String) -> Self {
        S3Storage { client, bucket }
    }
}
