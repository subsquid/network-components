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
        env = "DUMPING_INTERVAL_SEC",
        value_parser = parse_seconds,
        default_value = "120"
    )]
    pub dumping_interval: Duration,

    /// Number of log collectors in group
    #[arg(long, env, default_value_t = 1)]
    pub collector_group_size: usize,

    /// Index of tis log collector in group
    #[arg(long, env, default_value_t = 0)]
    pub collector_index: usize,

    /// Interval at which registered workers are updated (seconds)
    #[arg(long,
        env = "PORTAL_UPDATE_INTERVAL_SEC",
        value_parser = parse_seconds,
        default_value = "300"
    )]
    pub portal_update_interval: Duration,

    /// Timeout for log requests to workers
    #[arg(
        long,
        env = "REQUEST_TIMEOUT_SEC",
        value_parser = parse_seconds,
        default_value = "20"
    )]
    pub request_timeout: Duration,

    /// Timeout for workers lookup
    #[arg(
        long,
        env = "LOOKUP_TIMEOUT_SEC",
        alias = "lookup-timeout",
        value_parser = parse_seconds,
        default_value = "10"
    )]
    pub connect_timeout: Duration,

    /// Whether the logs should be structured in JSON format
    #[arg(long, env)]
    pub json_log: bool,
}

fn parse_seconds(s: &str) -> anyhow::Result<Duration> {
    Ok(Duration::from_secs(s.parse()?))
}
