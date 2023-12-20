use std::time::Duration;

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
    Timeout,
}

impl From<query_result::Result> for QueryResult {
    fn from(result: query_result::Result) -> Self {
        match result {
            query_result::Result::Ok(ok) => Self::Ok(ok),
            query_result::Result::BadRequest(err) => Self::BadRequest(err),
            query_result::Result::ServerError(err) => Self::ServerError(err),
        }
    }
}
