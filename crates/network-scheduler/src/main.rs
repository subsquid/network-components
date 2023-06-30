use std::time::Duration;

use clap::Parser;
use simple_logger::SimpleLogger;

use subsquid_network_transport::transport::P2PTransportBuilder;
use subsquid_network_transport::util::get_keypair;

use crate::cli::Cli;
use crate::scheduler::Scheduler;
use crate::server::Server;
use crate::worker_registry::WorkerRegistry;

mod cli;
mod data_chunk;
mod metrics;
mod scheduler;
mod scheduling_unit;
mod server;
mod storage;
mod worker_registry;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Init logger and parse arguments and config
    SimpleLogger::new()
        .with_level(log::LevelFilter::Info)
        .with_module_level("ethers_providers", log::LevelFilter::Warn)
        .env()
        .init()?;
    let args = Cli::parse();
    let config = args.config().await?;
    let schedule_interval = Duration::from_secs(config.schedule_interval_sec);

    // Open file for writing metrics
    let metrics_output = args.metrics_output().await?;

    // Build P2P transport
    let keypair = get_keypair(args.key).await?;
    let mut transport_builder = P2PTransportBuilder::from_keypair(keypair);
    let listen_addr = args.listen.parse()?;
    transport_builder.listen_on(std::iter::once(listen_addr));
    transport_builder.boot_nodes(args.boot_nodes);
    transport_builder.bootstrap(args.bootstrap);
    let (incoming_messages, message_sender, _) = transport_builder.run().await?;

    // Get scheduling units
    let incoming_units = storage::get_incoming_units(
        config.s3_endpoint,
        config.buckets,
        config.scheduling_unit_size,
    )
    .await?;

    let worker_registry = WorkerRegistry::init(&args.rpc_url).await?;
    let scheduler = Scheduler::new(config.replication_factor, config.worker_storage_bytes);

    Server::new(
        incoming_messages,
        incoming_units,
        message_sender,
        worker_registry,
        scheduler,
        schedule_interval,
        metrics_output,
    )
    .run()
    .await;

    Ok(())
}
