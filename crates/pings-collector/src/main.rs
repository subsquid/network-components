use std::sync::Arc;
use std::time::Duration;

use clap::Parser;
use env_logger::Env;
use sqd_network_transport::{get_agent_info, AgentInfo, P2PTransportBuilder};

use collector_utils::ClickhouseStorage;

use crate::cli::Cli;
use crate::server::Server;

mod cli;
mod server;

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
    let agent_info = get_agent_info!();
    let transport_builder = P2PTransportBuilder::from_cli(args.transport, agent_info).await?;
    let contract_client: Arc<dyn sqd_contract_client::Client> =
        transport_builder.contract_client().into();
    let (incoming_pings, transport_handle) =
        transport_builder.build_pings_collector(Default::default())?;

    let storage = ClickhouseStorage::new(args.clickhouse).await?;
    let storage_sync_interval = Duration::from_secs(args.storage_sync_interval_sec as u64);
    let worker_update_interval = Duration::from_secs(args.worker_update_interval_sec as u64);
    Server::new(incoming_pings, transport_handle)
        .run(
            contract_client,
            storage_sync_interval,
            worker_update_interval,
            args.buffer_dir,
            storage,
        )
        .await
}
