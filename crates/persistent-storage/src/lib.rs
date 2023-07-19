use base64::{engine::general_purpose, Engine as _};
use ethers::prelude::*;
use ethers_signers::WalletError;
use reqwest::{multipart, Client};
use serde::Deserialize;

#[derive(Deserialize)]
#[allow(non_snake_case)]
struct IpfsCreateResponse {
    Hash: String,
}

#[derive(Debug, thiserror::Error)]
pub enum IPFSError {
    #[error("Authentication message signature error: {0:?}")]
    AuthSigning(#[from] WalletError),
    #[error("Invalid Peer ID: {0:?}")]
    Network(#[from] reqwest::Error),
}

pub async fn write_to_ipfs(client: Client, file: String) -> Result<String, IPFSError> {
    let auth_key = get_auth_key().await?;
    let form = multipart::Form::new().text("file", file);
    Ok(client
        .post("https://crustipfs.xyz/api/v0/add")
        .multipart(form)
        .bearer_auth(auth_key)
        .send()
        .await?
        .json::<IpfsCreateResponse>()
        .await?
        .Hash)
}

async fn get_auth_key() -> Result<String, WalletError> {
    let wallet = Wallet::new(&mut rand::thread_rng());
    let address = format!("{:#?}", wallet.address());
    let sig = wallet.sign_message(&address).await?;
    let plain_auth_key = format!("eth-{address}:{sig}");
    Ok(general_purpose::STANDARD.encode(plain_auth_key.as_bytes()))
}
