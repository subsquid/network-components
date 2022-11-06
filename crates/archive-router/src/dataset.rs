use crate::error::Error;
use serde::{Deserialize, Serialize};
use std::fs;
use std::io;

#[derive(Clone, Serialize, Deserialize)]
pub struct DataRange {
    pub from: i32,
    pub to: i32,
}

/// Directory in the dataset containing data for a certain range of blocks.
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

                let block_range = folder_name.split_whitespace().collect::<Vec<&str>>();
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
