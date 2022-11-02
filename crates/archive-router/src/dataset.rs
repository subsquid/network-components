use crate::error::Error;
use std::fs;
use std::io;

/// Directory in the dataset containing data for a certain range of blocks.
pub struct DataDir {
    /// First block covered by the dir.
    pub from: i32,
    /// Last block covered by the dir (inclusive).
    pub to: i32,
    /// Size of dir in bytes.
    pub size: u64,
}

pub trait DatasetStorage {
    /// Get data directories in the dataset.
    fn get_data_directories(&self, dataset: &str) -> Result<Vec<DataDir>, Error>;
}

fn dir_size(dir: &fs::DirEntry) -> io::Result<u64> {
    fs::read_dir(dir.path())?.try_fold(0, |size, entry| {
        let entry = entry?;
        Ok(size + entry.metadata()?.len())
    })
}

pub struct LocalDatasetStorage;

impl DatasetStorage for LocalDatasetStorage {
    fn get_data_directories(&self, dataset: &str) -> Result<Vec<DataDir>, Error> {
        let mut dirs = vec![];
        let dataset_dir = fs::read_dir(dataset)?;
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
