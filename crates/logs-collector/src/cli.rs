use clap::{Args, Parser};
use contract_client::RpcArgs;
use subsquid_network_transport::cli::TransportArgs;

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

    #[command(flatten)]
    pub rpc: RpcArgs,

    #[arg(
        long,
        env,
        help = "Interval at which logs are saved to persistent storage (seconds)",
        default_value = "120"
    )]
    pub storage_sync_interval_sec: u32,
}
