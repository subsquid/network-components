use base64::prelude::BASE64_URL_SAFE_NO_PAD;
use base64::Engine;
use serde::Deserialize;
use serde_with::{serde_as, DurationSeconds};
use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::path::Path;
use std::time::Duration;
use subsquid_network_transport::PeerId;
use tokio::sync::OnceCell;

static CONFIG: OnceCell<Config> = OnceCell::const_new();

fn default_worker_inactive_threshold() -> Duration {
    Duration::from_secs(120)
}

fn default_worker_greylist_time() -> Duration {
    Duration::from_secs(600)
}

fn default_query_timeout() -> Duration {
    Duration::from_secs(60)
}

fn default_summary_print_interval() -> Duration {
    Duration::from_secs(30)
}

fn default_workers_update_interval() -> Duration {
    Duration::from_secs(180)
}

/// This struct exists not to confuse dataset name with it's encoded ID
#[derive(Debug, Clone, Hash, PartialEq, Eq, Deserialize)]
pub struct DatasetId(pub String);

impl Display for DatasetId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl DatasetId {
    pub fn from_url(url: impl AsRef<[u8]>) -> Self {
        Self(BASE64_URL_SAFE_NO_PAD.encode(url))
    }
}

#[serde_as]
#[derive(Debug, Clone, Deserialize)]
pub struct ComputeUnitsConfig {
    /// Threshold below which new computation units will be allocated to the worker
    pub minimum: u32,
    /// How many computation units to allocate when remaining units drop below threshold
    pub allocate: u32,
}

#[serde_as]
#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub scheduler_id: PeerId,
    pub send_metrics: bool,
    #[serde_as(as = "DurationSeconds")]
    #[serde(
        rename = "worker_inactive_threshold_sec",
        default = "default_worker_inactive_threshold"
    )]
    pub worker_inactive_threshold: Duration,
    #[serde_as(as = "DurationSeconds")]
    #[serde(
        rename = "worker_greylist_time_sec",
        default = "default_worker_greylist_time"
    )]
    pub worker_greylist_time: Duration,
    #[serde_as(as = "DurationSeconds")]
    #[serde(
        rename = "default_query_timeout_sec",
        default = "default_query_timeout"
    )]
    pub default_query_timeout: Duration,
    #[serde_as(as = "DurationSeconds")]
    #[serde(
        rename = "summary_print_interval_sec",
        default = "default_summary_print_interval"
    )]
    pub summary_print_interval: Duration,
    #[serde_as(as = "DurationSeconds")]
    #[serde(
        rename = "workers_update_interval_sec",
        default = "default_workers_update_interval"
    )]
    pub workers_update_interval: Duration,
    pub available_datasets: HashMap<String, DatasetId>,
}

impl Config {
    pub async fn read(config_path: impl AsRef<Path>) -> anyhow::Result<()> {
        let file_contents = tokio::fs::read(config_path).await?;
        let config = serde_yaml::from_slice(file_contents.as_slice())?;
        CONFIG.set(config)?;
        Ok(())
    }

    #[inline(always)]
    pub fn get() -> &'static Self {
        CONFIG.get().expect("Config not initialized")
    }

    pub fn dataset_id(&self, dataset: &str) -> Option<DatasetId> {
        self.available_datasets.get(dataset).cloned()
    }
}
