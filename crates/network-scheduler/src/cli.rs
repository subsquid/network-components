use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use clap::Parser;
use contract_client::RpcArgs;
use serde::{Deserialize, Serialize};
use serde_with::{serde_as, DurationSeconds};
use tokio::sync::OnceCell;

use subsquid_network_transport::cli::TransportArgs;

static CONFIG: OnceCell<Config> = OnceCell::const_new();

#[serde_as]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    #[serde_as(as = "DurationSeconds")]
    #[serde(rename = "schedule_interval_sec")]
    pub schedule_interval: Duration,
    #[serde_as(as = "DurationSeconds")]
    #[serde(rename = "worker_inactive_timeout_sec")]
    pub worker_inactive_timeout: Duration,
    #[serde_as(as = "DurationSeconds")]
    #[serde(rename = "worker_stale_timeout_sec")]
    pub worker_stale_timeout: Duration,
    #[serde_as(as = "DurationSeconds")]
    #[serde(rename = "worker_unreachable_timeout_sec")]
    pub worker_unreachable_timeout: Duration,
    pub replication_factor: usize,
    pub scheduling_unit_size: usize,
    pub worker_storage_bytes: u64,
    pub mixed_units_ratio: f64,
    pub mixing_recent_unit_weight: f64,
    pub s3_endpoint: String,
    pub dataset_buckets: Vec<String>,
    pub scheduler_state_bucket: String,
}

impl Config {
    #[inline(always)]
    pub fn get() -> &'static Self {
        CONFIG.get().expect("Config not initialized")
    }

    pub fn worker_monitoring_interval(&self) -> Duration {
        self.worker_inactive_timeout / 2
    }
}

#[derive(Parser)]
#[command(version)]
pub struct Cli {
    #[command(flatten)]
    pub transport: TransportArgs,

    #[command(flatten)]
    pub rpc: RpcArgs,

    #[arg(
        long,
        env,
        help = "HTTP metrics server listen addr",
        default_value = "0.0.0.0:8000"
    )]
    pub http_listen_addr: SocketAddr,

    #[arg(
        long,
        env,
        help = "Path to save metrics. If not present, stdout is used."
    )]
    pub metrics_path: Option<PathBuf>,

    #[arg(
        long,
        env,
        help = "Choose which metrics should be printed.",
        value_delimiter = ',',
        num_args = 0..,
        default_value = "QuerySubmitted,QueryFinished,WorkersSnapshot"
    )]
    pub metrics: Vec<String>,

    #[arg(
        short,
        long,
        env = "CONFIG_PATH",
        help = "Path to config file",
        default_value = "config.yml"
    )]
    config: PathBuf,
}

impl Cli {
    pub async fn read_config(&self) -> anyhow::Result<()> {
        let file_contents = tokio::fs::read(&self.config).await?;
        let config = serde_yaml::from_slice(file_contents.as_slice())?;
        CONFIG.set(config)?;
        Ok(())
    }
}
