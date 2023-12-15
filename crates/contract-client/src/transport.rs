use crate::ClientError;
use ethers::prelude::{Http, Provider, Ws};

pub enum Transport {
    Http(Provider<Http>),
    Ws(Provider<Ws>),
}

impl Transport {
    pub async fn connect(rpc_url: &str) -> Result<Self, ClientError> {
        if rpc_url.starts_with("http") {
            Ok(Transport::Http(Provider::try_from(rpc_url)?))
        } else if rpc_url.starts_with("ws") {
            Ok(Transport::Ws(Provider::connect(rpc_url).await?))
        } else {
            Err(ClientError::InvalidProtocol)
        }
    }
}
