use contract_client::Client;
use simple_logger::SimpleLogger;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    SimpleLogger::new()
        .with_level(log::LevelFilter::Info)
        .env()
        .init()?;

    let client = Client::new("http://127.0.0.1:8545/")?;
    let mut worker_stream = client.active_workers_stream().await;
    while let Some(workers) = worker_stream.recv().await {
        workers.iter().for_each(|w| println!("{w:?}"));
    }
    Ok(())
}
