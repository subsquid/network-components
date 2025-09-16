use clap::Parser;
use sqd_network_transport::TransportArgs;

use collector_utils::ClickhouseArgs;

#[derive(Parser)]
#[command(version)]
pub struct Cli {
    #[command(flatten)]
    pub transport: TransportArgs,

    #[command(flatten)]
    pub clickhouse: ClickhouseArgs,

    #[arg(
        long,
        env,
        help = "Interval at which registered workers are updated (seconds)",
        default_value = "300"
    )]
    pub worker_update_interval_sec: u32,

    #[arg(long, env, default_value_t = 10)]
    pub connect_timeout_sec: u32,

    #[arg(long, env, default_value_t = 15)]
    pub request_timeout_sec: u32,

    #[arg(long, env, default_value_t = 60)]
    pub request_interval_sec: u32,

    #[arg(long, env, default_value_t = 10)]
    pub concurrent_requests: usize,

    #[arg(long, env, default_value_t = 0)]
    pub shard: u8,

    #[arg(long, env, default_value_t = 1)]
    pub total_shards: u8,
}
