use simple_logger::SimpleLogger;

use contract_client;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    SimpleLogger::new()
        .with_level(log::LevelFilter::Info)
        .env()
        .init()?;

    let rpc_url = std::env::args()
        .nth(1)
        .unwrap_or("http://127.0.0.1:8545/".to_string());

    let client = contract_client::get_client(&rpc_url).await?;
    let mut worker_stream = client.active_workers_stream().await?;
    while let Some(workers) = worker_stream.recv().await {
        workers.iter().for_each(|w| println!("{w:?}"));
    }
    Ok(())
}
