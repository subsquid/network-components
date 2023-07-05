use std::path::PathBuf;

use clap::Parser;
use serde::{Deserialize, Serialize};

use subsquid_network_transport::util::BootNode;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub schedule_interval_sec: u64,
    pub replication_factor: usize,
    pub scheduling_unit_size: usize,
    pub worker_storage_bytes: u64,
    pub s3_endpoint: String,
    pub buckets: Vec<String>,
}

#[derive(Parser)]
pub struct Cli {
    #[arg(short, long, env = "KEY_PATH", help = "Path to libp2p key file")]
    pub key: Option<PathBuf>,

    #[arg(
        short,
        long,
        env = "LISTEN_ADDR",
        help = "Listen addr",
        default_value = "/ip4/0.0.0.0/tcp/0"
    )]
    pub listen: String,

    #[arg(long, env, help = "Connect to boot node '<peer_id> <address>'.")]
    pub boot_nodes: Vec<BootNode>,

    #[arg(
        long,
        env,
        help = "Bootstrap kademlia. Makes node discoverable by others."
    )]
    pub bootstrap: bool,

    #[arg(
        long,
        env,
        help = "Blockchain RPC URL",
        default_value = "http://127.0.0.1:8545/"
    )]
    pub rpc_url: String,

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
    default_value = "Ping,QuerySubmitted,QueryFinished,QueryExecuted"
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
    pub async fn config(&self) -> anyhow::Result<Config> {
        let file_contents = tokio::fs::read(&self.config).await?;
        Ok(serde_yaml::from_slice(file_contents.as_slice())?)
    }
}
