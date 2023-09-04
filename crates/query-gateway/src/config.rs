use base64::prelude::BASE64_URL_SAFE_NO_PAD;
use base64::Engine;
use serde::{Deserialize, Deserializer};
use std::collections::HashMap;
use std::fmt::{Display, Formatter};

/// This struct exists not to confuse dataset name with it's encoded ID
#[derive(Debug, Clone, Hash, PartialEq, Eq, Deserialize)]
pub struct DatasetId(pub String);

impl Display for DatasetId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl DatasetId {
    pub fn from_url(url: impl AsRef<[u8]>) -> Self {
        Self(BASE64_URL_SAFE_NO_PAD.encode(url))
    }
}

/// This struct exists because `PeerId` doesn't implement `Deserialize`
#[derive(Debug, Clone, Copy)]
pub struct PeerId(pub subsquid_network_transport::PeerId);

impl<'de> Deserialize<'de> for PeerId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let peer_id = String::deserialize(deserializer)?
            .parse()
            .map_err(|_| serde::de::Error::custom("Invalid peer ID"))?;
        Ok(Self(peer_id))
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub scheduler_id: PeerId,
    pub send_metrics: bool,
    pub available_datasets: HashMap<String, DatasetId>,
}
