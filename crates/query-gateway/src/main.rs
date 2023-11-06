use std::path::PathBuf;
use std::time::Duration;

use clap::Parser;
use env_logger::Env;

use subsquid_network_transport::cli::TransportArgs;
use subsquid_network_transport::transport::P2PTransportBuilder;
use subsquid_network_transport::Subscription;

use crate::config::Config;

mod client;
mod config;
mod http_server;

#[derive(Parser)]
#[command(version)]
struct Cli {
    #[command(flatten)]
    pub transport: TransportArgs,

    #[arg(
        long,
        env = "HTTP_LISTEN_ADDR",
        help = "HTTP server listen addr",
        default_value = "0.0.0.0:8000"
    )]
    http_listen: String,

    #[arg(
        short,
        long,
        env = "CONFIG_PATH",
        help = "Path to config file",
        default_value = "config.yml"
    )]
    config: PathBuf,

    #[arg(
        short,
        long,
        env,
        help = "Blockchain RPC URL",
        default_value = "http://127.0.0.1:8545/"
    )]
    rpc_url: String,
}

const PING_TOPIC: &str = "worker_ping";

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Init logger and parse arguments and config
    env_logger::Builder::from_env(Env::default().default_filter_or("info, ethers_providers=warn"))
        .init();
    let args: Cli = Cli::parse();
    let http_listen_addr = args.http_listen.parse()?;
    let config: Config = serde_yaml::from_slice(tokio::fs::read(args.config).await?.as_slice())?;

    // Build P2P transport
    let transport_builder = P2PTransportBuilder::from_cli(args.transport).await?;
    let local_peer_id = transport_builder.local_peer_id();
    let (msg_receiver, msg_sender, subscription_sender) = transport_builder.run().await?;

    // Subscribe to dataset state updates (from p2p pub-sub)
    subscription_sender
        .send(Subscription {
            topic: PING_TOPIC.to_string(),
            subscribed: true,
            allow_unordered: false,
        })
        .await?;

    // Subscribe to worker set updates (from blockchain)
    let worker_updates = contract_client::get_client(&args.rpc_url)
        .await?
        .active_workers_stream()
        .await?;

    // Start query client
    let query_client = client::get_client(
        config,
        local_peer_id,
        msg_receiver,
        msg_sender,
        worker_updates,
    )
    .await?;

    // Wait one worker ping cycle before starting to serve
    tokio::time::sleep(Duration::from_secs(20)).await;

    // Start HTTP server
    http_server::run_server(query_client, &http_listen_addr).await
}
