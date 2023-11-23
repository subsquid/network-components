use clap::Parser;
use env_logger::Env;
use std::time::Duration;
use subsquid_network_transport::transport::P2PTransportBuilder;
use subsquid_network_transport::Subscription;

use crate::cli::Cli;
use crate::collector::LogsCollector;
use crate::server::Server;
use crate::storage::ClickhouseStorage;

mod cli;
mod collector;
mod server;
mod storage;
mod utils;

const LOGS_TOPIC: &str = "worker_query_logs";

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
    let (incoming_messages, message_sender, subscription_sender) = transport_builder.run().await?;

    // Subscribe to receive worker pings
    subscription_sender
        .send(Subscription {
            topic: LOGS_TOPIC.to_string(),
            subscribed: true,
            allow_unordered: false,
        })
        .await?;

    let storage = ClickhouseStorage::new(args.clickhouse).await?;
    let logs_collector = LogsCollector::new(storage);
    let storage_sync_interval = Duration::from_secs(args.storage_sync_interval_sec as u64);
    Server::new(incoming_messages, message_sender, logs_collector)
        .run(storage_sync_interval)
        .await
}
