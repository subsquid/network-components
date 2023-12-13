use std::path::PathBuf;

use clap::Parser;
use contract_client::RpcArgs;
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

    #[command(flatten)]
    pub rpc: RpcArgs,

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
    let keypair = transport_builder.keypair();
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
    let contract_client = contract_client::get_client(&args.rpc).await?;

    // Start query client
    let query_client =
        client::get_client(config, keypair, msg_receiver, msg_sender, contract_client).await?;

    // Start HTTP server
    http_server::run_server(query_client, &http_listen_addr).await
}
