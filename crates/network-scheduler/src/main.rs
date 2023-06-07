use std::time::Duration;

use clap::Parser;
use simple_logger::SimpleLogger;
use subsquid_network_transport::transport::P2PTransportBuilder;
use subsquid_network_transport::util::get_keypair;
use tokio::fs::OpenOptions;

use crate::cli::Cli;
use crate::server::Server;

mod chunks;
mod cli;
mod metrics;
mod scheduler;
mod scheduling_unit;
mod server;
mod worker_registry;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Init logger and parse arguments and config
    SimpleLogger::new()
        .with_level(log::LevelFilter::Info)
        .env()
        .init()?;
    let args = Cli::parse();
    let config = args.config().await?;
    let schedule_interval = Duration::from_secs(config.schedule_interval_sec);

    // Open file for writing metrics
    let metrics_path = args.metrics;
    let metrics_file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(metrics_path)
        .await?;

    // Build P2P transport
    let keypair = get_keypair(args.key).await?;
    let mut transport_builder = P2PTransportBuilder::from_keypair(keypair);
    let listen_addr = args.listen.parse()?;
    transport_builder.listen_on(std::iter::once(listen_addr));
    transport_builder.boot_nodes(args.boot_nodes);
    transport_builder.bootstrap(args.bootstrap);
    let (incoming_messages, message_sender, _) = transport_builder.run().await?;

    // Get worker updates from blockchain
    let client = contract_client::get_client(&args.rpc_url).await?;
    let worker_updates = client.active_workers_stream().await?;

    // Get scheduling units
    let incoming_units = chunks::get_incoming_units(
        config.s3_endpoint,
        config.buckets,
        config.scheduling_unit_size,
    )
    .await?;

    Server::new(
        incoming_messages,
        worker_updates,
        incoming_units,
        message_sender,
        schedule_interval,
        config.replication_factor,
        metrics_file,
    )
    .run()
    .await;

    Ok(())
}
