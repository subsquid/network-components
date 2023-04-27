use clap::Parser;
use duration_string::DurationString;
use simple_logger::SimpleLogger;
use std::path::PathBuf;
use std::str::FromStr;
use tokio::io::{AsyncBufReadExt, BufReader};

use grpc_libp2p::transport::P2PTransportBuilder;
use grpc_libp2p::util::{get_keypair, BootNode};

use crate::client::{Config, Query, QueryClient};

mod client;

#[derive(Parser)]
struct Cli {
    #[arg(short, long, help = "Path to libp2p key file")]
    pub key: Option<PathBuf>,

    #[arg(
        short,
        long,
        help = "Listen addr",
        default_value = "/ip4/0.0.0.0/tcp/0"
    )]
    pub listen: String,

    #[arg(short, long, help = "Connect to boot node '<peer_id> <address>'.")]
    boot_nodes: Vec<BootNode>,

    #[arg(short, long, help = "Path to output directory [default: temp dir]")]
    pub output_dir: Option<PathBuf>,

    #[arg(short, long, help = "Query timeout", default_value = "1m")]
    pub query_timeout: DurationString,

    #[arg(short, long, help = "Subscribe to dataset")]
    pub datasets: Vec<String>,

    #[arg(
        short,
        long,
        help = "Path to config file",
        default_value = "config.yml"
    )]
    pub config: PathBuf,

    #[arg(
        short,
        long,
        help = "Blockchain RPC URL",
        default_value = "http://127.0.0.1:8545/"
    )]
    pub rpc_url: String,
}

fn parse_query(line: String) -> anyhow::Result<Query> {
    let mut parts: Vec<String> = line.splitn(3, ' ').map(|s| s.to_string()).collect();
    anyhow::ensure!(
        parts.len() == 3,
        "Expected format: <dataset> <start_block> <query>"
    );
    let query = parts.pop().expect("parts length is 3");
    let start_block = match u32::from_str(&parts.pop().expect("parts length is 3")) {
        Ok(x) => x,
        Err(_) => anyhow::bail!("Invalid start block"),
    };
    let dataset = parts.pop().expect("parts length is 3");
    Ok(Query {
        dataset,
        start_block,
        query,
    })
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Init logger and parse arguments and config
    SimpleLogger::new()
        .with_level(log::LevelFilter::Info)
        .env()
        .init()?;
    let args = Cli::parse();
    let config: Config = serde_yaml::from_slice(tokio::fs::read(args.config).await?.as_slice())?;

    // Build transport
    let keypair = get_keypair(args.key).await?;
    let mut transport_builder = P2PTransportBuilder::from_keypair(keypair);
    let listen_addr = args.listen.parse()?;
    transport_builder.listen_on(std::iter::once(listen_addr));
    transport_builder.boot_nodes(args.boot_nodes);
    let (msg_receiver, msg_sender, subscription_sender) = transport_builder.run().await?;

    // Subscribe to dataset updates
    for dataset in args.datasets {
        log::info!("Tracking dataset {dataset}");
        let encoded_dataset = config
            .available_datasets
            .get(dataset.as_str())
            .ok_or_else(|| anyhow::anyhow!("Dataset unavailable: {dataset}"))?
            .to_string();
        subscription_sender.send((encoded_dataset, true)).await?;
    }

    // Start query client
    let query_sender = QueryClient::start(
        args.output_dir,
        args.rpc_url,
        args.query_timeout.into(),
        config,
        msg_receiver,
        msg_sender,
    )
    .await?;

    // Read queries from stdin and execute
    let mut reader = BufReader::new(tokio::io::stdin()).lines();
    while let Some(line) = reader.next_line().await? {
        let query = match parse_query(line) {
            Ok(query) => query,
            Err(e) => {
                log::error!("{e}");
                continue;
            }
        };
        query_sender.send(query).await?;
    }

    Ok(())
}
