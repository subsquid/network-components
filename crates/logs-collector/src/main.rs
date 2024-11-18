use std::sync::Arc;

use clap::Parser;
use env_logger::Env;
use sqd_network_transport::util::CancellationToken;
use sqd_network_transport::{get_agent_info, AgentInfo, P2PTransportBuilder};

use collector_utils::ClickhouseStorage;

use crate::cli::Cli;
use crate::collector::LogsCollector;
use crate::server::Server;

mod cli;
mod collector;
mod server;

#[cfg(not(target_env = "msvc"))]
use tikv_jemallocator::Jemalloc;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

fn create_cancellation_token() -> anyhow::Result<CancellationToken> {
    use tokio::signal::unix::{signal, SignalKind};

    let token = CancellationToken::new();
    let copy = token.clone();
    let mut sigint = signal(SignalKind::interrupt())?;
    let mut sigterm = signal(SignalKind::terminate())?;
    tokio::spawn(async move {
        tokio::select!(
            _ = sigint.recv() => {
                copy.cancel();
            },
            _ = sigterm.recv() => {
                copy.cancel();
            },
        );
    });
    Ok(token)
}

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
    let contract_client: Arc<_> = transport_builder.contract_client().into();
    let transport = transport_builder.build_logs_collector(Default::default())?;

    let storage = ClickhouseStorage::new(args.clickhouse).await?;
    let logs_collector = LogsCollector::new(storage);
    let cancellation_token = create_cancellation_token()?;

    Server::new(transport, logs_collector)
        .run(
            contract_client,
            args.collection_interval,
            args.worker_update_interval,
            args.concurrent_workers,
            cancellation_token,
        )
        .await
}
