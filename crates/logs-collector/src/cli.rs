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

    /// Interval between the starts of collection rounds while workers have a backlog
    /// (seconds). When a round leaves logs uncollected (a worker had more than 5 pages,
    /// or the buffer filled up), the next round starts this long after the previous one
    /// started instead of waiting for the full collection interval. Capped at the
    /// collection interval. Disabled when unset. Every round runs the watermark query
    /// in ClickHouse and requests every registered worker, so while a backlog lasts
    /// this multiplies that load by up to collection interval / this value.
    #[arg(long, env = "BACKLOG_COLLECTION_INTERVAL_SEC", value_parser = parse_seconds)]
    pub backlog_collection_interval: Option<Duration>,

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

    /// Shard of workers this instance collects logs from, in 0..TOTAL_SHARDS
    #[arg(long, env, default_value_t = 0)]
    pub shard: u8,

    /// Number of shards the workers are split into by the last byte of their peer ID
    #[arg(long, env, default_value_t = 1)]
    pub total_shards: u8,
}

fn parse_seconds(s: &str) -> anyhow::Result<Duration> {
    Ok(Duration::from_secs(s.parse()?))
}
