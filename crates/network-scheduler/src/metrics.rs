use std::pin::Pin;
use std::time::Instant;

use serde::Serialize;
use tokio::fs::OpenOptions;
use tokio::io::{AsyncWrite, AsyncWriteExt};

use router_controller::messages::{Ping, QueryExecuted, QueryFinished, QuerySubmitted};
use subsquid_network_transport::PeerId;

use crate::cli::Cli;
use crate::worker_registry::ActiveWorker;

#[derive(Debug, Clone, Serialize)]
pub struct Metrics {
    #[serde(with = "serde_millis")]
    timestamp: Instant,
    #[serde(skip_serializing_if = "Option::is_none")]
    peer_id: Option<String>,
    #[serde(flatten)]
    event: MetricsEvent,
}

impl Metrics {
    pub fn new(peer_id: Option<String>, event: impl Into<MetricsEvent>) -> Self {
        Self {
            timestamp: Instant::now(),
            peer_id,
            event: event.into(),
        }
    }

    pub fn to_json_line(&self) -> anyhow::Result<Vec<u8>> {
        let mut vec = serde_json::to_vec(self)?;
        vec.push(b'\n');
        Ok(vec)
    }
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "event")]
pub enum MetricsEvent {
    Ping(Ping),
    QuerySubmitted(QuerySubmitted),
    QueryFinished(QueryFinished),
    QueryExecuted(QueryExecuted),
    WorkersSnapshot { active_workers: Vec<ActiveWorker> },
}

impl MetricsEvent {
    pub fn name(&self) -> &'static str {
        match self {
            MetricsEvent::Ping(_) => "Ping",
            MetricsEvent::QuerySubmitted(_) => "QuerySubmitted",
            MetricsEvent::QueryFinished(_) => "QueryFinished",
            MetricsEvent::QueryExecuted(_) => "QueryExecuted",
            MetricsEvent::WorkersSnapshot { .. } => "WorkersSnapshot",
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

impl From<Vec<ActiveWorker>> for MetricsEvent {
    fn from(active_workers: Vec<ActiveWorker>) -> Self {
        Self::WorkersSnapshot { active_workers }
    }
}

pub struct MetricsWriter {
    output: Pin<Box<dyn AsyncWrite + Send + Sync>>,
    enabled_metrics: Vec<String>,
}

impl MetricsWriter {
    pub async fn from_cli(cli: &Cli) -> anyhow::Result<Self> {
        let output: Pin<Box<dyn AsyncWrite + Send + Sync>> = match &cli.metrics_path {
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
        peer_id: Option<PeerId>,
        msg: impl Into<MetricsEvent>,
    ) -> anyhow::Result<()> {
        let peer_id = peer_id.map(|id| id.to_string());
        let metrics = Metrics::new(peer_id, msg);
        if self.metric_enabled(&metrics.event) {
            let json_line = metrics.to_json_line()?;
            self.output.write_all(json_line.as_slice()).await?;
        }
        Ok(())
    }
}
