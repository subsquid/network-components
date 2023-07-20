use ethers::prelude::{rand, Wallet};
use persistent_storage::CrustClient;
use reqwest::{Client, Url};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = Client::new();
    let wallet = Wallet::new(&mut rand::thread_rng());
    let gateway_url = Url::parse("https://crustipfs.xyz")?;
    let crust = CrustClient::new(&wallet, &client, &gateway_url);
    let hash = crust.write_to_ipfs("Foo").await;
    println!("{:#?}", hash);
    Ok(())
}
