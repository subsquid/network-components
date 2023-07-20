use std::time::Duration;

use clap::Parser;
use env_logger::Env;

use subsquid_network_transport::transport::P2PTransportBuilder;

use crate::cli::Cli;
use crate::metrics::MetricsWriter;
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
    env_logger::Builder::from_env(
        Env::default().default_filter_or("info, aws_config=warn, ethers_providers=warn"),
    )
    .init();
    let args: Cli = Cli::parse();
    let config = args.config().await?;
    let schedule_interval = Duration::from_secs(config.schedule_interval_sec);

    // Open file for writing metrics
    let metrics_writer = MetricsWriter::from_cli(&args).await?;

    // Build P2P transport
    let transport_builder = P2PTransportBuilder::from_cli(args.transport).await?;
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
        metrics_writer,
    )
    .run()
    .await;

    Ok(())
}
