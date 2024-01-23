use clap::Parser;
use env_logger::Env;

use subsquid_network_transport::transport::P2PTransportBuilder;

use crate::cli::Cli;
use crate::metrics::MetricsWriter;
use crate::server::Server;
use crate::storage::S3Storage;

mod cli;
mod data_chunk;
mod messages;
mod metrics;
mod metrics_server;
mod scheduler;
mod scheduling_unit;
mod server;
mod storage;
mod worker_state;

const PING_TOPIC: &str = "worker_ping";

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Init logger and parse arguments and config
    env_logger::Builder::from_env(
        Env::default().default_filter_or("info, aws_config=warn, ethers_providers=warn"),
    )
    .init();
    let args: Cli = Cli::parse();
    args.read_config().await?;

    // Open file for writing metrics
    let metrics_writer = MetricsWriter::from_cli(&args).await?;

    // Build P2P transport
    let transport_builder = P2PTransportBuilder::from_cli(args.transport).await?;
    let (incoming_messages, transport_handle) = transport_builder.run().await?;

    // Subscribe to receive worker pings
    transport_handle.subscribe(PING_TOPIC).await?;

    // Get scheduling units
    let storage = S3Storage::new().await;
    let incoming_units = storage.get_incoming_units().await;
    let scheduler = storage.load_scheduler().await?;
    let contract_client = contract_client::get_client(&args.rpc).await?;

    Server::new(
        incoming_messages,
        incoming_units,
        transport_handle,
        scheduler,
        metrics_writer,
    )
    .run(contract_client, storage, args.http_listen_addr)
    .await
}
