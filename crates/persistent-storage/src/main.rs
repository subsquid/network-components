use persistent_storage::CrustClient;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let crust = CrustClient::with_random_wallet().await?;
    let hash = crust.write_to_ipfs("Foo").await;
    println!("{:#?}", hash);
    Ok(())
}
