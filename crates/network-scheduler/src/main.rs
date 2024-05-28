use clap::Parser;
use env_logger::Env;
use prometheus_client::registry::Registry;

use subsquid_network_transport::P2PTransportBuilder;

use crate::cli::Cli;
use crate::metrics::MetricsWriter;
use crate::server::Server;
use crate::storage::S3Storage;

mod cli;
mod data_chunk;
mod metrics;
mod metrics_server;
mod prometheus_metrics;
mod scheduler;
mod scheduling_unit;
mod server;
mod signature;
mod storage;
mod worker_state;

#[cfg(not(target_env = "msvc"))]
use tikv_jemallocator::Jemalloc;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

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
    let mut metrics_registry = Registry::default();
    subsquid_network_transport::metrics::register_metrics(&mut metrics_registry);
    prometheus_metrics::register_metrics(&mut metrics_registry);

    // Build P2P transport
    let transport_builder = P2PTransportBuilder::from_cli(args.transport).await?;
    let contract_client: Box<dyn contract_client::Client> = transport_builder.contract_client();
    let local_peer_id = transport_builder.local_peer_id();
    let (incoming_messages, transport_handle) =
        transport_builder.build_scheduler(Default::default())?;

    // Get scheduling units
    let storage = S3Storage::new(local_peer_id).await;
    let incoming_units = storage.get_incoming_units().await;
    let scheduler = storage.load_scheduler().await?;

    Server::new(
        incoming_messages,
        incoming_units,
        transport_handle,
        scheduler,
        metrics_writer,
    )
    .run(
        contract_client,
        storage,
        args.http_listen_addr,
        metrics_registry,
    )
    .await
}
