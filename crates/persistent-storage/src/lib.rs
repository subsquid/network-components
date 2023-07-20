use base64::{engine::general_purpose, Engine as _};
use ethers::prelude::*;
use ethers_core::k256::ecdsa::{
    signature::hazmat::PrehashSigner, RecoveryId, Signature as RecoverableSignature,
};
use ethers_signers::WalletError;
use reqwest::{multipart, Client, Url};
use serde::Deserialize;

#[derive(Deserialize)]
struct IpfsCreateResponse {
    #[serde(rename = "Hash")]
    hash: String,
}

#[derive(Debug, thiserror::Error)]
pub enum CrustError {
    #[error("Authentication message signature error: {0:?}")]
    AuthSigning(#[from] WalletError),
    #[error("Invalid Peer ID: {0:?}")]
    Network(#[from] reqwest::Error),
}

#[derive(Debug)]
pub struct CrustClient {
    http_client: Client,
    auth_key: String,
    gateway_url: Url,
}

impl CrustClient {
    pub async fn new<D: Sync + Send + PrehashSigner<(RecoverableSignature, RecoveryId)>>(
        wallet: Wallet<D>,
        http_client: Client,
        gateway_url: Url,
    ) -> Result<CrustClient, WalletError> {
        Ok(CrustClient {
            http_client,
            auth_key: get_auth_key(wallet).await?,
            gateway_url,
        })
    }

    pub async fn with_random_wallet() -> Result<CrustClient, WalletError> {
        let client = Client::new();
        let wallet = Wallet::new(&mut rand::thread_rng());
        let url = Url::parse("https://crustipfs.xyz").unwrap();
        CrustClient::new(wallet, client, url).await
    }

    pub async fn write_to_ipfs(&self, file: &str) -> Result<String, CrustError> {
        let form = multipart::Form::new().text("file", file.to_string());
        Ok(self
            .http_client
            .post(self.gateway_url.join("/api/v0/add").unwrap().as_str())
            .multipart(form)
            .bearer_auth(&self.auth_key)
            .send()
            .await?
            .json::<IpfsCreateResponse>()
            .await?
            .hash)
    }
}

async fn get_auth_key<D: Sync + Send + PrehashSigner<(RecoverableSignature, RecoveryId)>>(
    wallet: Wallet<D>,
) -> Result<String, WalletError> {
    let address = format!("{:#?}", wallet.address());
    let sig = wallet.sign_message(&address).await?;
    let plain_auth_key = format!("eth-{address}:{sig}");
    Ok(general_purpose::STANDARD.encode(plain_auth_key.as_bytes()))
}
