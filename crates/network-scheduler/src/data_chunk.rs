use std::fmt::{Display, Formatter};
use std::str::FromStr;

use itertools::Itertools;
use serde::{Deserialize, Serialize};
use serde_with::{hex::Hex, serde_as};
use sha3::{Digest, Sha3_256};

use subsquid_messages::{Range, WorkerState};

#[serde_as]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Ord, PartialOrd, Serialize, Deserialize)]
pub struct ChunkId(#[serde_as(as = "Hex")] [u8; 32]);

impl Display for ChunkId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        hex::encode(self.0).fmt(f)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DataChunk {
    pub dataset_url: String,
    pub block_range: Range,
    pub size_bytes: u64,
    pub chunk_str: String,
}

impl Display for DataChunk {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        format!(
            "{}/{}-{}",
            self.dataset_url, self.block_range.begin, self.block_range.end
        )
        .fmt(f)
    }
}

impl DataChunk {
    pub fn new(bucket: &str, chunk_str: &str, size_bytes: u64) -> anyhow::Result<Self> {
        let chunk = subsquid_messages::data_chunk::DataChunk::from_str(chunk_str)
            .map_err(|_| anyhow::anyhow!("Invalid chunk: {chunk_str}"))?;
        Ok(Self {
            dataset_url: format!("s3://{bucket}"),
            block_range: chunk.into(),
            size_bytes,
            chunk_str: chunk_str.to_string(),
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

pub fn chunks_to_worker_state(chunks: impl IntoIterator<Item = DataChunk>) -> WorkerState {
    let datasets = chunks
        .into_iter()
        .map(|chunk| (chunk.dataset_url, chunk.block_range))
        .into_grouping_map()
        .collect();
    WorkerState { datasets }
}

#[cfg(test)]
mod tests {
    use subsquid_messages::range::RangeSet;

    use super::*;

    #[test]
    fn test_chunk() {
        let chunk = DataChunk {
            dataset_url: "s3://squidnet".to_string(),
            block_range: Range::new(0, 1000),
            size_bytes: 0,
        };
        assert_eq!(chunk.to_string(), "s3://squidnet/0-1000");
        assert_eq!(
            chunk.id(),
            ChunkId([
                0xcd, 0x62, 0xa3, 0xf1, 0xf2, 0x48, 0x5b, 0x3d, 0x88, 0x0d, 0x77, 0x03, 0x75, 0xe4,
                0x52, 0x3e, 0x63, 0x8f, 0x08, 0xdf, 0xf0, 0x98, 0x89, 0x9c, 0xbf, 0xbc, 0x02, 0xf3,
                0x52, 0xea, 0x53, 0x90
            ])
        );
    }

    #[test]
    #[rustfmt::skip]
    fn test_worker_state() {
        let chunks = vec![
            DataChunk {
                dataset_url: "s3://squidnet".to_string(),
                block_range: Range::new(0, 1000),
                size_bytes: 0,
            },
            DataChunk {
                dataset_url: "s3://squidnet".to_string(),
                block_range: Range::new(500, 1500),
                size_bytes: 0,
            },
            DataChunk {
                dataset_url: "s3://pepenet".to_string(),
                block_range: Range::new(1234, 5678),
                size_bytes: 0,
            },
        ];

        assert_eq!(chunks_to_worker_state(chunks), WorkerState {
            datasets: vec![
                ("s3://squidnet".to_string(), RangeSet { ranges: vec![Range::new(0, 1500)] }),
                ("s3://pepenet".to_string(), RangeSet { ranges: vec![Range::new(1234, 5678)] }),
            ].into_iter().collect()
        })
    }
}
