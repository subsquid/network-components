use std::path::PathBuf;

use clap::Parser;
use serde::{Deserialize, Serialize};
use subsquid_network_transport::util::BootNode;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub schedule_interval_sec: u64,
    pub replication_factor: usize,
    pub scheduling_unit_size: usize,
    pub s3_endpoint: String,
    pub buckets: Vec<String>,
}

#[derive(Parser)]
pub struct Cli {
    #[arg(short, long, help = "Path to libp2p key file")]
    pub key: Option<PathBuf>,

    #[arg(
        short,
        long,
        help = "Listen addr",
        default_value = "/ip4/0.0.0.0/tcp/0"
    )]
    pub listen: String,

    #[arg(long, help = "Connect to boot node '<peer_id> <address>'.")]
    pub boot_nodes: Vec<BootNode>,

    #[arg(long, help = "Bootstrap kademlia. Makes node discoverable by others.")]
    pub bootstrap: bool,

    #[arg(long, help = "Path to save metrics", default_value = "metrics.jsonl")]
    pub metrics: PathBuf,

    #[arg(
        long,
        help = "Blockchain RPC URL",
        default_value = "http://127.0.0.1:8545/"
    )]
    pub rpc_url: String,

    #[arg(
        short,
        long,
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
