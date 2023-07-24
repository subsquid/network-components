use persistent_storage::crust_client::CrustClient;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let crust = CrustClient::with_random_wallet().await?;
    let cid = crust.write_to_ipfs("24.07.2023:10:15 TEST").await?;
    let pin = crust.pin_to_ipfs(&cid).await?;
    println!("{:#?}", pin);
    println!("{:#?}", crust.auth_key);
    Ok(())
}
