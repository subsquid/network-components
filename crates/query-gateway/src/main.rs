use std::net::SocketAddr;
use std::path::PathBuf;

use clap::Parser;
use contract_client::RpcArgs;
use env_logger::Env;

use subsquid_network_transport::cli::TransportArgs;
use subsquid_network_transport::transport::P2PTransportBuilder;

use crate::config::Config;

mod allocations;
mod chain_updates;
mod client;
mod config;
mod http_server;
mod metrics;
mod network_state;
mod query;
mod scheme_extractor;
mod server;
mod task;

#[derive(Parser)]
#[command(version)]
struct Cli {
    #[command(flatten)]
    pub transport: TransportArgs,

    #[command(flatten)]
    pub rpc: RpcArgs,

    #[arg(
        long,
        env = "HTTP_LISTEN_ADDR",
        help = "HTTP server listen addr",
        default_value = "0.0.0.0:8000"
    )]
    http_listen: SocketAddr,

    #[arg(long, env, help = "Path to config file", default_value = "config.yml")]
    config_path: PathBuf,

    #[arg(
        long,
        env,
        help = "Path to allocations database file",
        default_value = "allocations.db"
    )]
    allocations_db_path: PathBuf,
}

const PING_TOPIC: &str = "worker_ping";

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Init logger and parse arguments and config
    env_logger::Builder::from_env(Env::default().default_filter_or("info, ethers_providers=warn"))
        .init();
    let args: Cli = Cli::parse();
    Config::read(&args.config_path).await?;

    // Build P2P transport
    let transport_builder = P2PTransportBuilder::from_cli(args.transport).await?;
    let keypair = transport_builder.keypair();
    let (incoming_messages, transport_handle) = transport_builder.run().await?;

    // Subscribe to dataset state updates (from p2p pub-sub)
    transport_handle.subscribe(PING_TOPIC).await?;

    // Instantiate contract client and check RPC connection
    let contract_client = contract_client::get_client(&args.rpc).await?;
    anyhow::ensure!(
        contract_client
            .is_client_registered(keypair.public().to_peer_id())
            .await?,
        "Client not registered on chain"
    );

    // Start query client
    let query_client = client::get_client(
        keypair,
        incoming_messages,
        transport_handle,
        contract_client,
        args.allocations_db_path,
    )
    .await?;

    // Start HTTP server
    http_server::run_server(query_client, &args.http_listen).await
}
