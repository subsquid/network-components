use clap::Parser;
use env_logger::Env;
use prometheus_client::registry::Registry;

use sqd_network_transport::{get_agent_info, AgentInfo, P2PTransportBuilder, SchedulerConfig};

use crate::cli::{Cli, Config};
use crate::server::Server;
use crate::storage::S3Storage;

mod cli;
mod data_chunk;
mod metrics_server;
mod parquet;
mod prometheus_metrics;
mod scheduler;
mod scheduling_unit;
mod server;
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
    let mut metrics_registry = Registry::default();
    sqd_network_transport::metrics::register_metrics(&mut metrics_registry);
    prometheus_metrics::register_metrics(&mut metrics_registry);

    // Build P2P transport
    let agent_info = get_agent_info!();
    let transport_builder = P2PTransportBuilder::from_cli(args.transport, agent_info).await?;
    let contract_client: Box<dyn sqd_contract_client::Client> = transport_builder.contract_client();
    let local_peer_id = transport_builder.local_peer_id();
    let scheduler_config = SchedulerConfig {
        ignore_existing_conns: Config::get().ignore_existing_conns,
        ..Default::default()
    };
    let (incoming_messages, transport_handle) =
        transport_builder.build_scheduler(scheduler_config)?;

    // Get scheduling units
    let storage = S3Storage::new(local_peer_id).await;
    let incoming_units = storage.get_incoming_units().await;
    let scheduler = storage.load_scheduler().await?;

    Server::new(incoming_units, transport_handle, scheduler)
        .run(
            contract_client,
            storage,
            args.http_listen_addr,
            metrics_registry,
            incoming_messages,
        )
        .await
}
