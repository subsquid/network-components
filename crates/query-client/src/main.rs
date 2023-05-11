use clap::Parser;
use grpc_libp2p::transport::P2PTransportBuilder;
use grpc_libp2p::util::{get_keypair, BootNode};
use serde::Deserialize;
use simple_logger::SimpleLogger;
use std::collections::HashMap;
use std::path::PathBuf;

use crate::client::DatasetId;

mod client;
mod http_server;

#[derive(Parser)]
struct Cli {
    #[arg(short, long, help = "Path to libp2p key file")]
    key: Option<PathBuf>,

    #[arg(
        long,
        help = "P2P network listen addr",
        default_value = "/ip4/0.0.0.0/tcp/0"
    )]
    p2p_listen: String,

    #[arg(long, help = "HTTP server listen addr", default_value = "0.0.0.0:8000")]
    http_listen: String,

    #[arg(short, long, help = "Connect to boot node '<peer_id> <address>'.")]
    boot_nodes: Vec<BootNode>,

    #[arg(
        short,
        long,
        help = "Path to config file",
        default_value = "config.yml"
    )]
    config: PathBuf,

    #[arg(
        short,
        long,
        help = "Blockchain RPC URL",
        default_value = "http://127.0.0.1:8545/"
    )]
    rpc_url: String,
}

#[derive(Debug, Clone, Deserialize)]
struct Config {
    available_datasets: HashMap<String, DatasetId>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Init logger and parse arguments and config
    SimpleLogger::new()
        .with_level(log::LevelFilter::Info)
        .env()
        .init()?;
    let args = Cli::parse();
    let p2p_listen_addr = args.p2p_listen.parse()?;
    let http_listen_addr = args.http_listen.parse()?;
    let config: Config = serde_yaml::from_slice(tokio::fs::read(args.config).await?.as_slice())?;

    // Build P2P transport
    let keypair = get_keypair(args.key).await?;
    let mut transport_builder = P2PTransportBuilder::from_keypair(keypair);
    transport_builder.listen_on(std::iter::once(p2p_listen_addr));
    transport_builder.boot_nodes(args.boot_nodes);
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
        .await;

    // Start query client
    let query_client = client::get_client(
        config.available_datasets,
        msg_receiver,
        msg_sender,
        worker_updates,
    )
    .await?;

    // Start HTTP server
    http_server::run_server(query_client, &http_listen_addr).await
}
