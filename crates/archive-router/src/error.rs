use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use std::fmt::Debug;
use std::io;

#[derive(Debug)]
pub enum Error {
    NoRequestedData,
    NoSuitableWorker,
    ReadDatasetError(Box<dyn std::error::Error>),
    InvalidLayoutError(String),
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Self {
        Error::ReadDatasetError(Box::new(err))
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
            Error::InvalidLayoutError(msg) => (StatusCode::INTERNAL_SERVER_ERROR, msg),
            Error::ReadDatasetError(..) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "dataset read error".to_string(),
            ),
        };

        (status, error_message).into_response()
    }
}
