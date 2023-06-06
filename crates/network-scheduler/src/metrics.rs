use router_controller::messages::{QueryExecuted, QueryFinished, QuerySubmitted};
use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Metrics {
    timestamp: u64, // Milliseconds since UNIX epoch
    peer_id: String,
    #[serde(flatten)]
    event: MetricsEvent,
}

impl Metrics {
    pub fn new(peer_id: impl ToString, event: impl Into<MetricsEvent>) -> Self {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("We're after 1970")
            .as_millis()
            .try_into()
            .expect("But before 2554");
        Self {
            timestamp,
            peer_id: peer_id.to_string(),
            event: event.into(),
        }
    }

    pub fn to_json_line(&self) -> anyhow::Result<Vec<u8>> {
        let mut vec = serde_json::to_vec(self)?;
        vec.push(b'\n');
        Ok(vec)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "event")]
pub enum MetricsEvent {
    QuerySubmitted(QuerySubmitted),
    QueryFinished(QueryFinished),
    QueryExecuted(QueryExecuted),
}

impl From<QuerySubmitted> for MetricsEvent {
    fn from(value: QuerySubmitted) -> Self {
        Self::QuerySubmitted(value)
    }
}

impl From<QueryFinished> for MetricsEvent {
    fn from(value: QueryFinished) -> Self {
        Self::QueryFinished(value)
    }
}

impl From<QueryExecuted> for MetricsEvent {
    fn from(value: QueryExecuted) -> Self {
        Self::QueryExecuted(value)
    }
}
