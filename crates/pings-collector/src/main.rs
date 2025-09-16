use std::sync::Arc;
use std::time::Duration;

use clap::Parser;
use sqd_network_transport::{get_agent_info, AgentInfo, P2PTransportBuilder, PingsCollectorConfig};

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

fn setup_tracing(json: bool) -> anyhow::Result<()> {
    let env_filter = tracing_subscriber::EnvFilter::builder().parse_lossy(
        std::env::var(tracing_subscriber::EnvFilter::DEFAULT_ENV)
            .unwrap_or(format!("info,{}=debug", std::env!("CARGO_CRATE_NAME"))),
    );

    if json {
        tracing_subscriber::fmt()
            .with_env_filter(env_filter)
            .with_target(false)
            .json()
            .with_span_list(false)
            .flatten_event(true)
            .init();
    } else {
        tracing_subscriber::fmt()
            .with_env_filter(env_filter)
            .with_target(false)
            .compact()
            .init();
    };
    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Init logger and parse arguments
    setup_tracing(false)?;
    let args: Cli = Cli::parse();

    // Build P2P transport
    let agent_info = get_agent_info!();
    let transport_builder = P2PTransportBuilder::from_cli(args.transport, agent_info).await?;

    let contract_client: Arc<dyn sqd_contract_client::Client> =
        transport_builder.contract_client().into();
    let transport_handle = transport_builder.build_pings_collector(PingsCollectorConfig {
        request_timeout: Duration::from_secs(args.request_timeout_sec as u64),
        connect_timeout: Duration::from_secs(args.connect_timeout_sec as u64),
        ..Default::default()
    })?;

    let storage = ClickhouseStorage::new(args.clickhouse).await?;
    let worker_update_interval = Duration::from_secs(args.worker_update_interval_sec as u64);

    let request_interval = Duration::from_secs(args.request_interval_sec as u64);
    let concurrency_limit = args.concurrent_requests;

    Server::new(transport_handle, request_interval, concurrency_limit)
        .run(contract_client, worker_update_interval, storage)
        .await
}
