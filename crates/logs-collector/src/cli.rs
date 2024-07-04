use clap::{Args, Parser};
use subsquid_network_transport::TransportArgs;

#[derive(Args)]
pub struct ClickhouseArgs {
    #[arg(long, env)]
    pub clickhouse_url: String,
    #[arg(long, env)]
    pub clickhouse_database: String,
    #[arg(long, env)]
    pub clickhouse_user: String,
    #[arg(long, env)]
    pub clickhouse_password: String,
}

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
        help = "Time after which logs from previous epoch are no longer accepted (seconds)",
        default_value = "600"
    )]
    pub epoch_seal_timeout_sec: u32,

    #[arg(
        long,
        env,
        help = "Minimum interval between pings from a worker (seconds)",
        default_value = "15"
    )]
    pub min_ping_interval_sec: u32,
}
