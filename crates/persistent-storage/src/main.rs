use persistent_storage::write_to_ipfs;
use reqwest::Client;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = Client::new();
    let hash = write_to_ipfs(client, "Foo".to_string()).await;
    println!("{:#?}", hash);
    Ok(())
}
