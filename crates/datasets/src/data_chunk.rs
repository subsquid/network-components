use std::fmt::{Display, Formatter};
use std::str::FromStr;

use serde::{Deserialize, Serialize};
use serde_with::{hex::Hex, serde_as};
use sha3::{Digest, Sha3_256};

use subsquid_messages::Range;

#[serde_as]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Ord, PartialOrd, Serialize, Deserialize)]
pub struct ChunkId(#[serde_as(as = "Hex")] pub [u8; 32]);

impl Display for ChunkId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        hex::encode(self.0).fmt(f)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DataChunk {
    #[serde(alias = "dataset_url")]
    pub dataset_id: String,
    #[serde(default)]
    pub bucket: String,
    pub block_range: Range,
    pub size_bytes: u64,
    pub chunk_str: String,
    #[serde(default)]
    pub filenames: Vec<String>,
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
        let chunk = subsquid_messages::data_chunk::DataChunk::from_str(chunk_str)
            .map_err(|_| anyhow::anyhow!("Invalid chunk: {chunk_str}"))?;
        Ok(Self {
            dataset_id: format!("s3://{bucket}"),
            bucket: bucket.to_string(),
            chunk_str: chunk.to_string(),
            block_range: chunk.into(),
            size_bytes,
            filenames,
        })
    }

    pub fn id(&self) -> ChunkId {
        let mut result = [0u8; 32];
        let mut hasher = Sha3_256::default();
        hasher.update(self.to_string().as_bytes());
        Digest::finalize_into(hasher, result.as_mut_slice().into());
        ChunkId(result)
    }
}
