use std::path::PathBuf;

use clap::Parser;
use simple_logger::SimpleLogger;

use subsquid_network_transport::cli::TransportArgs;
use subsquid_network_transport::transport::P2PTransportBuilder;

use crate::config::Config;

mod client;
mod config;
mod http_server;

#[derive(Parser)]
struct Cli {
    #[command(flatten)]
    pub transport: TransportArgs,

    #[arg(
        long,
        env = "HTTP_LISTEN_ADDR",
        help = "HTTP server listen addr",
        default_value = "0.0.0.0:8000"
    )]
    http_listen: String,

    #[arg(
        short,
        long,
        env = "CONFIG_PATH",
        help = "Path to config file",
        default_value = "config.yml"
    )]
    config: PathBuf,

    #[arg(
        short,
        long,
        env,
        help = "Blockchain RPC URL",
        default_value = "http://127.0.0.1:8545/"
    )]
    rpc_url: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Init logger and parse arguments and config
    SimpleLogger::new()
        .with_level(log::LevelFilter::Info)
        .with_module_level("ethers_providers", log::LevelFilter::Warn)
        .env()
        .init()?;
    let args: Cli = Cli::parse();
    let http_listen_addr = args.http_listen.parse()?;
    let config: Config = serde_yaml::from_slice(tokio::fs::read(args.config).await?.as_slice())?;

    // Build P2P transport
    let transport_builder = P2PTransportBuilder::from_cli(args.transport).await?;
    let (msg_receiver, msg_sender, subscription_sender) = transport_builder.run().await?;

    // Subscribe to dataset state updates (from p2p pub-sub)
    for (dataset, dataset_id) in config.available_datasets.iter() {
        log::info!("Tracking dataset {dataset} ID={dataset_id}");
        subscription_sender
            .send((dataset_id.to_string(), true))
            .await?;
    }

    // Subscribe to worker set updates (from blockchain)
    let worker_updates = contract_client::get_client(&args.rpc_url)
        .await?
        .active_workers_stream()
        .await?;

    // Start query client
    let query_client = client::get_client(config, msg_receiver, msg_sender, worker_updates).await?;

    // Start HTTP server
    http_server::run_server(query_client, &http_listen_addr).await
}
