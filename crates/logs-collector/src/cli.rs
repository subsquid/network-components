use std::time::Duration;

use clap::Parser;
use collector_utils::ClickhouseArgs;
use sqd_network_transport::TransportArgs;

#[derive(Parser)]
#[command(version)]
pub struct Cli {
    #[command(flatten)]
    pub transport: TransportArgs,

    #[command(flatten)]
    pub clickhouse: ClickhouseArgs,

    /// Interval at which logs are collected and saved to persistent storage (seconds)
    #[arg(
        long,
        env = "COLLECTION_INTERVAL_SEC",
        value_parser = parse_seconds,
        default_value = "120"
    )]
    pub collection_interval: Duration,

    /// Number of workers processed in parallel
    #[arg(long, env, default_value_t = 30)]
    pub concurrent_workers: usize,

    /// Interval at which registered workers are updated (seconds)
    #[arg(long,
        env = "WORKER_UPDATE_INTERVAL_SEC",
        value_parser = parse_seconds,
        default_value = "300"
    )]
    pub worker_update_interval: Duration,
}

fn parse_seconds(s: &str) -> anyhow::Result<Duration> {
    Ok(Duration::from_secs(s.parse()?))
}
