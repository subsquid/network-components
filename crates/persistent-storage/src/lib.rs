pub mod crust_client;
use async_trait::async_trait;

pub struct CrustPersistentStorage {
    client: crust_client::CrustClient,
}

#[async_trait]
pub trait PersistentStorage {
    async fn store(
        &self,
        data: &str,
    ) -> Result<crust_client::IpfsPinResponse, Box<dyn std::error::Error>>;
}

impl CrustPersistentStorage {
    pub fn new(client: crust_client::CrustClient) -> Self {
        Self { client }
    }
}

#[async_trait]
impl PersistentStorage for CrustPersistentStorage {
    async fn store(
        &self,
        data: &str,
    ) -> Result<crust_client::IpfsPinResponse, Box<dyn std::error::Error>> {
        let cid = self.client.write_to_ipfs(data).await?;
        Ok(self.client.pin_to_ipfs(&cid).await?)
    }
}
