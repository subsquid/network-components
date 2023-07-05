use std::pin::Pin;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use tokio::fs::OpenOptions;
use tokio::io::{AsyncWrite, AsyncWriteExt};

use router_controller::messages::{Ping, QueryExecuted, QueryFinished, QuerySubmitted};
use subsquid_network_transport::PeerId;

use crate::cli::Cli;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Metrics {
    timestamp: u64,
    // Milliseconds since UNIX epoch
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
        let json_str = serde_json::to_string(self)?;
        let vec = format!("metrics: {json_str}\n").into_bytes();
        Ok(vec)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "event")]
pub enum MetricsEvent {
    Ping(Ping),
    QuerySubmitted(QuerySubmitted),
    QueryFinished(QueryFinished),
    QueryExecuted(QueryExecuted),
}

impl MetricsEvent {
    pub fn name(&self) -> &'static str {
        match self {
            MetricsEvent::Ping(_) => "Ping",
            MetricsEvent::QuerySubmitted(_) => "QuerySubmitted",
            MetricsEvent::QueryFinished(_) => "QueryFinished",
            MetricsEvent::QueryExecuted(_) => "QueryExecuted",
        }
    }
}

impl From<Ping> for MetricsEvent {
    fn from(value: Ping) -> Self {
        Self::Ping(value)
    }
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

pub struct MetricsWriter {
    output: Pin<Box<dyn AsyncWrite>>,
    enabled_metrics: Vec<String>,
}

impl MetricsWriter {
    pub async fn from_cli(cli: &Cli) -> anyhow::Result<Self> {
        let output: Pin<Box<dyn AsyncWrite>> = match &cli.metrics_path {
            Some(path) => {
                let metrics_file = OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(path)
                    .await?;
                Box::pin(metrics_file)
            }
            None => Box::pin(tokio::io::stdout()),
        };
        let enabled_metrics = cli.metrics.clone();
        Ok(Self {
            output,
            enabled_metrics,
        })
    }

    fn metric_enabled(&self, event: &MetricsEvent) -> bool {
        let event_name = event.name();
        self.enabled_metrics.iter().any(|s| s == event_name)
    }

    pub async fn write_metrics(
        &mut self,
        peer_id: PeerId,
        msg: impl Into<MetricsEvent>,
    ) -> anyhow::Result<()> {
        let metrics = Metrics::new(peer_id, msg);
        if self.metric_enabled(&metrics.event) {
            let json_line = metrics.to_json_line()?;
            self.output.write_all(json_line.as_slice()).await?;
        }
        Ok(())
    }
}
