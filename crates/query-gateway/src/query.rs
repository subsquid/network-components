use std::fmt::{Display, Formatter};
use std::time::Duration;

use axum::http::StatusCode;
use derivative::Derivative;
use tokio::sync::oneshot;

use subsquid_messages::{query_result, OkResult};
use subsquid_network_transport::PeerId;

use crate::config::DatasetId;

#[derive(Derivative, Debug)]
pub struct Query {
    pub dataset_id: DatasetId,
    pub query: String,
    pub worker_id: PeerId,
    pub timeout: Duration,
    pub profiling: bool,
    #[derivative(Debug = "ignore")]
    pub result_sender: oneshot::Sender<QueryResult>,
}

#[derive(Debug, Clone)]
pub enum QueryResult {
    Ok(OkResult),
    BadRequest(String),
    ServerError(String),
    NoAllocation,
    Timeout,
}

impl From<query_result::Result> for QueryResult {
    fn from(result: query_result::Result) -> Self {
        match result {
            query_result::Result::Ok(ok) => Self::Ok(ok),
            query_result::Result::BadRequest(err) => Self::BadRequest(err),
            query_result::Result::ServerError(err) => Self::ServerError(err),
            query_result::Result::NoAllocation(()) => Self::NoAllocation,
        }
    }
}

impl Display for QueryResult {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            QueryResult::Ok(_) => write!(f, "OK"),
            QueryResult::BadRequest(e) => write!(f, "{}", e),
            QueryResult::ServerError(e) => write!(f, "{}", e),
            QueryResult::NoAllocation => write!(f, "Not enough compute units allocated"),
            QueryResult::Timeout => write!(f, "Query execution timed out"),
        }
    }
}

impl QueryResult {
    pub fn status_code(&self) -> StatusCode {
        match self {
            QueryResult::Ok(_) => StatusCode::OK,
            QueryResult::BadRequest(_) => StatusCode::BAD_REQUEST,
            QueryResult::ServerError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            QueryResult::NoAllocation => StatusCode::FORBIDDEN,
            QueryResult::Timeout => StatusCode::GATEWAY_TIMEOUT,
        }
    }
}
