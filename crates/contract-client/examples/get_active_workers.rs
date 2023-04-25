use contract_client::Client;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let client = Client::new("http://127.0.0.1:8545/")?;
    let workers = client.get_active_workers().await?;
    workers.iter().for_each(|w| println!("{w:?}"));
    Ok(())
}
