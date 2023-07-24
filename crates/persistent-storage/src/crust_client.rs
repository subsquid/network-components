use std::collections::HashMap;

use base64::{engine::general_purpose, Engine as _};
use ethers::prelude::*;
use ethers_core::k256::ecdsa::{
    signature::hazmat::PrehashSigner, RecoveryId, Signature as RecoverableSignature,
};
pub use ethers_signers::WalletError;
use reqwest::{multipart, Client, Url};
use serde::Deserialize;

#[derive(Deserialize)]
struct IpfsCreateResponse {
    #[serde(rename = "Hash")]
    hash: String,
}

#[derive(Deserialize, Debug)]
#[allow(dead_code)]
struct Pin {
    cid: String,
    name: String,
}

#[derive(Deserialize, Debug)]
#[allow(dead_code)]
pub struct IpfsPinResponse {
    #[serde(rename = "requestid")]
    request_id: String,
    status: String,
    created: String,
    pin: Pin,
}

#[derive(Deserialize)]
#[allow(dead_code)]
struct IpfsPinBody {
    cid: String,
    name: String,
}

#[derive(Debug)]
pub struct CrustClient {
    http_client: Client,
    pub auth_key: String,
    ipfs_gateway_url: Url,
    ipfs_pinning_url: Url,
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
            ipfs_gateway_url: gateway_url,
            ipfs_pinning_url: Url::parse("https://pin.crustcode.com/").unwrap(),
        })
    }

    pub async fn with_random_wallet() -> Result<CrustClient, WalletError> {
        let client = Client::new();
        let wallet = Wallet::new(&mut rand::thread_rng());
        let url = Url::parse("https://gw.crustfiles.app").unwrap();
        CrustClient::new(wallet, client, url).await
    }

    pub async fn write_to_ipfs(&self, file: &str) -> Result<String, reqwest::Error> {
        let form = multipart::Form::new().text("file", file.to_string());
        Ok(self
            .http_client
            .post(self.ipfs_gateway_url.join("/api/v0/add").unwrap())
            .multipart(form)
            .bearer_auth(&self.auth_key)
            .send()
            .await?
            .json::<IpfsCreateResponse>()
            .await?
            .hash)
    }

    pub async fn pin_to_ipfs(&self, cid: &str) -> Result<IpfsPinResponse, reqwest::Error> {
        let body = HashMap::from([("cid", cid)]);

        self.http_client
            .post(self.ipfs_pinning_url.join("/psa/pins").unwrap())
            .json(&body)
            .bearer_auth(&self.auth_key)
            .send()
            .await?
            .json::<IpfsPinResponse>()
            .await
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
