use clap::Parser;
use ethers::types::U256;
use libp2p::PeerId;
use simple_logger::SimpleLogger;

use contract_client;
use contract_client::RpcArgs;

#[derive(Parser)]
struct Cli {
    #[command(flatten)]
    rpc: RpcArgs,
    client_id: PeerId,
    worker_ids: Option<Vec<U256>>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    SimpleLogger::new()
        .with_level(log::LevelFilter::Info)
        .env()
        .init()?;
    let cli: Cli = Cli::parse();

    let client = contract_client::get_client(&cli.rpc).await?;
    let allocations = client
        .current_allocations(cli.client_id, cli.worker_ids)
        .await?;
    allocations.iter().for_each(|w| println!("{w:?}"));
    Ok(())
}
