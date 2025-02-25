use std::fmt::{Display, Formatter};
use std::str::FromStr;

use once_cell::sync::OnceCell;
use serde::{Deserialize, Serialize};
use serde_with::{hex::Hex, serde_as};
use sha3::{Digest, Sha3_256};

use sqd_messages::{assignments, Range};

use crate::cli::Config;

#[serde_as]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Ord, PartialOrd, Serialize, Deserialize)]
pub struct ChunkId(#[serde_as(as = "Hex")] [u8; 32]);

impl Display for ChunkId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        hex::encode(self.0).fmt(f)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, derivative::Derivative)]
#[derivative(PartialEq)]
pub struct DataChunk {
    #[serde(alias = "dataset_url")]
    pub dataset_id: String,
    #[serde(default)]
    pub download_url: String,
    pub block_range: Range,
    pub size_bytes: u64,
    pub chunk_str: String,
    #[serde(default)]
    pub filenames: Vec<String>,
    #[derivative(PartialEq = "ignore")]
    pub summary: Option<assignments::ChunkSummary>,
    #[serde(skip)]
    id: OnceCell<ChunkId>,
}

impl Display for DataChunk {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        format!(
            "{}/{}-{}",
            self.dataset_id, self.block_range.begin, self.block_range.end
        )
        .fmt(f)
    }
}

impl DataChunk {
    pub fn new(
        bucket: &str,
        chunk_str: &str,
        size_bytes: u64,
        filenames: Vec<String>,
    ) -> anyhow::Result<Self> {
        let chunk = sqd_messages::data_chunk::DataChunk::from_str(chunk_str)
            .map_err(|_| anyhow::anyhow!("Invalid chunk: {chunk_str}"))?;
        Ok(Self {
            dataset_id: format!("s3://{bucket}"),
            download_url: format!("https://{bucket}.{}", Config::get().storage_domain),
            chunk_str: chunk.to_string(),
            block_range: chunk.into(),
            size_bytes,
            filenames,
            id: Default::default(),
            summary: None,
        })
    }

    pub fn id(&self) -> &ChunkId {
        self.id.get_or_init(|| {
            let mut result = [0u8; 32];
            let mut hasher = Sha3_256::default();
            hasher.update(self.to_string().as_bytes());
            Digest::finalize_into(hasher, result.as_mut_slice().into());
            ChunkId(result)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_chunk() {
        let chunk = DataChunk {
            dataset_id: "s3://squidnet".to_string(),
            download_url: "https://squidnet.sqd-datasets.io".to_string(),
            block_range: Range::new(0, 1000),
            size_bytes: 0,
            chunk_str: "/00000/00001-01000-fa1f6773".to_string(),
            filenames: vec![],
            summary: None,
            id: Default::default(),
        };
        assert_eq!(chunk.to_string(), "s3://squidnet/0-1000");
        assert_eq!(
            *chunk.id(),
            ChunkId([
                0xcd, 0x62, 0xa3, 0xf1, 0xf2, 0x48, 0x5b, 0x3d, 0x88, 0x0d, 0x77, 0x03, 0x75, 0xe4,
                0x52, 0x3e, 0x63, 0x8f, 0x08, 0xdf, 0xf0, 0x98, 0x89, 0x9c, 0xbf, 0xbc, 0x02, 0xf3,
                0x52, 0xea, 0x53, 0x90
            ])
        );
    }
}
