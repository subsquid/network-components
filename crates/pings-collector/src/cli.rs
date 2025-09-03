use std::path::PathBuf;

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
        help = "Interval at which logs are saved to persistent storage (seconds)",
        default_value = "120"
    )]
    pub storage_sync_interval_sec: u32,

    #[arg(
        long,
        env,
        help = "Interval at which registered workers are updated (seconds)",
        default_value = "300"
    )]
    pub worker_update_interval_sec: u32,

    #[arg(
        long,
        env,
        help = "Path to store the local pings buffer",
        default_value = "."
    )]
    pub buffer_dir: PathBuf,

    #[arg(long, env, default_value_t = 10)]
    pub connect_timeout_sec: u32,

    #[arg(long, env, default_value_t = 15)]
    pub request_timeout_sec: u32,

    #[arg(long, env, default_value_t = 60)]
    pub request_interval_sec: u32,

    #[arg(long, env, default_value_t = 10)]
    pub concurrent_requests: usize,
}
