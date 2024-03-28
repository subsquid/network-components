use simple_logger::SimpleLogger;

use contract_client;
use contract_client::RpcArgs;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    SimpleLogger::new()
        .with_level(log::LevelFilter::Info)
        .env()
        .init()?;

    let rpc_url = std::env::args()
        .nth(1)
        .unwrap_or("http://127.0.0.1:8545/".to_string());
    let l1_rpc_url = std::env::args().nth(2);

    let client = contract_client::get_client(&RpcArgs {
        rpc_url,
        l1_rpc_url,
    })
    .await?;
    let workers = client.active_workers().await?;
    workers.iter().for_each(|w| println!("{w:?}"));
    println!("{t:?}");
    Ok(())
}
