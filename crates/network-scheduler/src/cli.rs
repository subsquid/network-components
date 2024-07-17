use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use clap::Parser;
use semver::VersionReq;
use serde::{Deserialize, Serialize};
use serde_with::{serde_as, DurationSeconds};
use tokio::sync::OnceCell;

use subsquid_network_transport::TransportArgs;

static CONFIG: OnceCell<Config> = OnceCell::const_new();

fn default_worker_version() -> VersionReq {
    ">=1.0.0-rc3".parse().unwrap()
}

#[serde_as]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub schedule_interval_epochs: u32,
    #[serde_as(as = "DurationSeconds")]
    #[serde(rename = "worker_inactive_timeout_sec")]
    pub worker_inactive_timeout: Duration,
    #[serde_as(as = "DurationSeconds")]
    #[serde(rename = "worker_monitoring_interval_sec")]
    pub worker_monitoring_interval: Duration,
    #[serde_as(as = "DurationSeconds")]
    #[serde(rename = "worker_stale_timeout_sec")]
    pub worker_stale_timeout: Duration,
    #[serde_as(as = "DurationSeconds")]
    #[serde(rename = "worker_unreachable_timeout_sec")]
    pub worker_unreachable_timeout: Duration,
    #[serde_as(as = "DurationSeconds")]
    #[serde(rename = "failed_dial_retry_sec")]
    pub failed_dial_retry: Duration,
    #[serde_as(as = "DurationSeconds")]
    #[serde(rename = "successful_dial_retry_sec")]
    pub successful_dial_retry: Duration,
    #[serde_as(as = "DurationSeconds")]
    #[serde(rename = "signature_refresh_interval_sec")]
    pub signature_refresh_interval: Duration,
    pub replication_factor: usize, // this is minimum
    pub dynamic_replication: bool,
    pub dyn_rep_capacity_share: f64,
    pub scheduling_unit_size: usize,
    pub worker_storage_bytes: u64,
    pub mixed_units_ratio: f64,
    pub mixing_recent_unit_weight: f64,
    pub s3_endpoint: String,
    #[serde(default = "default_storage_domain")]
    pub storage_domain: String,
    pub dataset_buckets: Vec<String>,
    pub scheduler_state_bucket: String,
    #[serde(skip_serializing)]
    pub cloudflare_storage_secret: String,
    #[serde(default = "default_worker_version")]
    pub supported_worker_versions: VersionReq,
    #[serde(default = "default_worker_version")]
    pub recommended_worker_versions: VersionReq,
    #[serde(default)]
    pub jail_unreachable: bool,
}

impl Config {
    #[inline(always)]
    pub fn get() -> &'static Self {
        CONFIG.get().expect("Config not initialized")
    }
}

fn default_storage_domain() -> String {
    "sqd-datasets.io".to_string()
}

#[derive(Parser)]
#[command(version)]
pub struct Cli {
    #[command(flatten)]
    pub transport: TransportArgs,

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
