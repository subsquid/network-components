use std::ffi::OsString;
use std::fmt::Debug;
use std::io;

pub enum Error {
    NoRequestedData,
    NoSuitableWorker,
    ReadDatasetError(io::Error),
    ParquetFolderNameError(Box<dyn Debug>),
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Self {
        Error::ReadDatasetError(err)
    }
}

impl From<OsString> for Error {
    fn from(string: OsString) -> Self {
        Error::ParquetFolderNameError(Box::new(string))
    }
}
