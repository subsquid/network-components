use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use clap::Parser;
use semver::VersionReq;
use serde::{Deserialize, Serialize};
use serde_with::{serde_as, DurationSeconds};
use tokio::sync::OnceCell;

use sqd_network_transport::TransportArgs;

static CONFIG: OnceCell<Config> = OnceCell::const_new();

fn default_worker_version() -> VersionReq {
    ">=2.0.0".parse().unwrap()
}

fn default_assignment_history_len() -> usize {
    1000
}

fn default_assignment_refresh_interval() -> Duration {
    Duration::from_secs(60)
}

fn default_assignment_delay() -> Duration {
    Duration::from_secs(30)
}

fn default_worker_status_request_timeout() -> Duration {
    Duration::from_secs(5)
}

fn default_concurrent_worker_status_requests() -> usize {
    500
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
    pub network_state_name: String,
    #[serde(default = "default_network_state_url")]
    pub network_state_url: String,
    pub scheduler_state_bucket: String,
    #[serde(skip_serializing)]
    pub cloudflare_storage_secret: String,
    #[serde(default = "default_worker_version")]
    pub supported_worker_versions: VersionReq,
    #[serde(default = "default_worker_version")]
    pub recommended_worker_versions: VersionReq,
    #[serde(default)]
    pub jail_unreachable: bool,
    #[serde(default)]
    pub ignore_existing_conns: bool,
    #[serde(default = "num_cpus::get")]
    pub ping_processing_threads: usize,
    #[serde_as(as = "DurationSeconds")]
    #[serde(
        rename = "assignment_refresh_interval_sec",
        default = "default_assignment_refresh_interval"
    )]
    pub assignment_refresh_interval: Duration,
    #[serde_as(as = "DurationSeconds")]
    #[serde(rename = "assignment_delay_sec", default = "default_assignment_delay")]
    pub assignment_delay: Duration,
    #[serde(default = "default_assignment_history_len")]
    pub assignment_history_len: usize,
    #[serde(skip_serializing, skip_deserializing, default)]
    pub network: String,
    #[serde(default = "default_true")]
    pub gossipsub_worker_status: bool,
    #[serde(default = "default_false")]
    pub poll_worker_status: bool,
    #[serde_as(as = "DurationSeconds")]
    #[serde(
        rename = "worker_status_request_timeout_sec",
        default = "default_worker_status_request_timeout"
    )]
    pub worker_status_request_timeout: Duration,
    #[serde(default = "default_concurrent_worker_status_requests")]
    pub concurrent_worker_status_requests: usize,
}

impl Config {
    #[inline(always)]
    pub fn get() -> &'static Self {
        CONFIG.get().expect("Config not initialized")
    }
}

fn default_true() -> bool {
    true
}

fn default_false() -> bool {
    false
}

fn default_storage_domain() -> String {
    "sqd-datasets.io".to_string()
}

fn default_network_state_url() -> String {
    "https://metadata.sqd-datasets.io".to_string()
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
        let mut config: Config = serde_yaml::from_slice(file_contents.as_slice())?;
        config.network = match self.transport.rpc.network {
            sqd_contract_client::Network::Tethys => "tethys".to_string(),
            sqd_contract_client::Network::Mainnet => "mainnet".to_string(),
        };
        CONFIG.set(config)?;
        Ok(())
    }
}
