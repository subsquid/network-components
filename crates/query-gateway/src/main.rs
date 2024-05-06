use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use clap::Parser;
use contract_client::RpcArgs;
use env_logger::Env;

use subsquid_network_transport::TransportArgs;
use subsquid_network_transport::{GatewayConfig, P2PTransportBuilder};
use tokio::sync::RwLock;

use crate::config::Config;
use crate::network_state::NetworkState;

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

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Init logger and parse arguments and config
    env_logger::Builder::from_env(Env::default().default_filter_or("info, ethers_providers=warn"))
        .init();
    let args: Cli = Cli::parse();
    Config::read(&args.config_path).await?;

    // Build P2P transport
    let transport_builder = P2PTransportBuilder::from_cli(args.transport).await?;
    let local_peer_id = transport_builder.local_peer_id();
    let (incoming_messages, transport_handle) =
        transport_builder.build_gateway(GatewayConfig::new(Config::get().logs_collector_id))?;

    // Instantiate contract client and check RPC connection
    let contract_client = contract_client::get_client(&args.rpc).await?;
    anyhow::ensure!(
        contract_client.is_client_registered(local_peer_id).await?,
        "Client not registered on chain"
    );

    // Initialize allocated/spent CU metrics with zeros
    let workers = contract_client.active_workers().await?;
    metrics::init_workers(workers.iter().map(|w| w.peer_id.to_string()));
    let network_state = Arc::new(RwLock::new(NetworkState::new(workers)));

    // Start query client
    let query_client = client::get_client(
        local_peer_id,
        incoming_messages,
        transport_handle,
        contract_client,
        network_state.clone(),
        args.allocations_db_path,
    )
    .await?;

    // Start HTTP server
    http_server::run_server(query_client, network_state, &args.http_listen).await
}
