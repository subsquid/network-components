use std::sync::Arc;

use clap::Parser;
use sqd_network_transport::util::CancellationToken;
use sqd_network_transport::{
    get_agent_info, AgentInfo, P2PTransportBuilder, PortalLogsCollectorConfig,
};

use collector_utils::ClickhouseStorage;

use crate::cli::Cli;
use crate::collector::PortalLogsCollector;
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
    let args: Cli = Cli::parse();
    setup_tracing(args.json_log)?;

    // Build P2P transport
    let agent_info = get_agent_info!();
    let transport_builder = P2PTransportBuilder::from_cli(args.transport, agent_info).await?;
    let contract_client: Arc<_> = transport_builder.contract_client().into();
    let config = PortalLogsCollectorConfig::new();

    let transport = transport_builder.build_portal_logs_collector(config)?;

    let storage = ClickhouseStorage::new(args.clickhouse).await?;
    let logs_collector = PortalLogsCollector::new(storage);
    let cancellation_token = create_cancellation_token()?;

    Server::<ClickhouseStorage>::new(
        transport.1,
        transport.0,
        logs_collector,
        args.collector_index,
        args.collector_group_size,
    )
    .run(
        contract_client,
        args.dumping_interval,
        args.portal_update_interval,
        cancellation_token,
    )
    .await
}
