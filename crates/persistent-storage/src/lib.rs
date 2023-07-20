use base64::{engine::general_purpose, Engine as _};
use ethers::prelude::*;
use ethers_core::k256::ecdsa::{
    signature::hazmat::PrehashSigner, RecoveryId, Signature as RecoverableSignature,
};
use ethers_signers::WalletError;
use reqwest::{multipart, Client};
use serde::Deserialize;

#[derive(Deserialize)]
struct IpfsCreateResponse {
    #[serde(rename = "Hash")]
    hash: String,
}

#[derive(Debug, thiserror::Error)]
pub enum IPFSError {
    #[error("Authentication message signature error: {0:?}")]
    AuthSigning(#[from] WalletError),
    #[error("Invalid Peer ID: {0:?}")]
    Network(#[from] reqwest::Error),
}

#[derive(Debug)]
pub struct CrustClient<'a, D: Sync + Send + PrehashSigner<(RecoverableSignature, RecoveryId)>> {
    client: &'a Client,
    wallet: &'a Wallet<D>,
}

impl<'a, D: Sync + Send + PrehashSigner<(RecoverableSignature, RecoveryId)>> CrustClient<'a, D> {
    pub fn new(wallet: &'a Wallet<D>, client: &'a Client) -> CrustClient<'a, D> {
        CrustClient { client, wallet }
    }

    pub async fn write_to_ipfs(&self, file: &str) -> Result<String, IPFSError> {
        let auth_key = self.get_auth_key().await?;
        let form = multipart::Form::new().text("file", file.to_string());
        Ok(self
            .client
            .post("https://crustipfs.xyz/api/v0/add")
            .multipart(form)
            .bearer_auth(auth_key)
            .send()
            .await?
            .json::<IpfsCreateResponse>()
            .await?
            .hash)
    }

    async fn get_auth_key(&self) -> Result<String, WalletError> {
        let address = format!("{:#?}", self.wallet.address());
        let sig = self.wallet.sign_message(&address).await?;
        let plain_auth_key = format!("eth-{address}:{sig}");
        Ok(general_purpose::STANDARD.encode(plain_auth_key.as_bytes()))
    }
}
