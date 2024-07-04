use clap::Parser;
use env_logger::Env;
use std::sync::Arc;
use std::time::Duration;
use subsquid_network_transport::P2PTransportBuilder;

use crate::cli::Cli;
use crate::collector::LogsCollector;
use crate::server::Server;
use crate::storage::ClickhouseStorage;

mod cli;
mod collector;
mod server;
mod storage;
mod utils;

#[cfg(not(target_env = "msvc"))]
use tikv_jemallocator::Jemalloc;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Init logger and parse arguments
    env_logger::Builder::from_env(
        Env::default().default_filter_or("info, aws_config=warn, ethers_providers=warn"),
    )
    .init();
    let args: Cli = Cli::parse();

    // Build P2P transport
    let transport_builder = P2PTransportBuilder::from_cli(args.transport).await?;
    let contract_client: Arc<dyn contract_client::Client> =
        transport_builder.contract_client().into();
    let (incoming_messages, transport_handle) =
        transport_builder.build_logs_collector(Default::default())?;

    let storage = ClickhouseStorage::new(args.clickhouse).await?;
    let epoch_seal_timeout = Duration::from_secs(args.epoch_seal_timeout_sec as u64);
    let min_ping_interval = Duration::from_secs(args.min_ping_interval_sec as u64);
    let logs_collector = LogsCollector::new(
        storage,
        contract_client.clone(),
        epoch_seal_timeout,
        min_ping_interval,
    );
    let storage_sync_interval = Duration::from_secs(args.storage_sync_interval_sec as u64);
    let worker_update_interval = Duration::from_secs(args.worker_update_interval_sec as u64);
    Server::new(incoming_messages, transport_handle, logs_collector)
        .run(
            contract_client,
            storage_sync_interval,
            worker_update_interval,
        )
        .await
}
