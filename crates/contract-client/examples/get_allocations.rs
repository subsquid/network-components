use clap::{Parser, Subcommand};
use simple_logger::SimpleLogger;

use contract_client;
use contract_client::{get_allocations_client, Allocation, RpcArgs};

#[derive(Parser)]
#[command(version)]
struct Cli {
    #[command(flatten)]
    pub rpc: RpcArgs,
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Subcommand)]
enum Command {
    Allocate {
        worker_onchain_id: u64,
        computation_units: u64,
    },
    List {
        from_block: Option<u64>,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    SimpleLogger::new()
        .with_level(log::LevelFilter::Info)
        .env()
        .init()?;

    let cli: Cli = Cli::parse();
    let client = get_allocations_client(&cli.rpc).await?;

    println!("Wallet address: {}", client.address());
    let cus = client.available_cus().await?;
    println!("Available compute units: {}", cus);

    if let Some(Command::Allocate {
        worker_onchain_id,
        computation_units,
    }) = cli.command
    {
        println!("Allocating {computation_units} to worker {worker_onchain_id}");
        client
            .allocate_cus(vec![Allocation {
                worker_onchain_id: worker_onchain_id.into(),
                computation_units: computation_units.into(),
            }])
            .await?;
    }

    if let Some(Command::List { from_block }) = cli.command {
        let (allocations, last_block) = client.allocated_cus(from_block).await?;
        println!("Allocations up to block {last_block}:");
        allocations.into_iter().for_each(|a| println!("{a:?}"));
    }

    Ok(())
}
