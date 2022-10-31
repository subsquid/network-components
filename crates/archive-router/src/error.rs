use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use std::ffi::OsString;
use std::fmt::Debug;
use std::io;

#[derive(Debug)]
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

impl IntoResponse for Error {
    fn into_response(self) -> Response {
        let (status, error_message) = match self {
            Error::NoRequestedData => (
                StatusCode::BAD_REQUEST,
                "dataset doesn't have requested data".to_string(),
            ),
            Error::NoSuitableWorker => (
                StatusCode::SERVICE_UNAVAILABLE,
                "no suitable worker".to_string(),
            ),
            Error::ParquetFolderNameError(name) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("dataset has invalid parquet folder - {:?}", name),
            ),
            Error::ReadDatasetError(_) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "dataset read error".to_string(),
            ),
        };

        (status, error_message).into_response()
    }
}
