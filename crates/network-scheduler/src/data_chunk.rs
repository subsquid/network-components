use std::fmt::{Display, Formatter};
use std::str::FromStr;

use itertools::Itertools;
use sha3::{Digest, Sha3_256};

use router_controller::messages::WorkerState;
use router_controller::range::Range;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Ord, PartialOrd)]
pub struct ChunkId([u8; 32]);

impl Display for ChunkId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        hex::encode(self.0).fmt(f)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct DataChunk {
    pub dataset_url: String,
    pub block_range: Range,
    pub size_bytes: u64,
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
        let chunk = router_controller::data_chunk::DataChunk::from_str(chunk_str)
            .map_err(|_| anyhow::anyhow!("Invalid chunk: {chunk_str}"))?;
        Ok(Self {
            dataset_url: format!("s3://{bucket}"),
            block_range: chunk.into(),
            size_bytes,
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
    use router_controller::range::RangeSet;
    use subsquid_network_transport::PeerId;

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
    fn test_distance() {
        let peer_id: PeerId = "12D3KooWQER7HEpwsvqSzqzaiV36d3Bn6DZrnwEunnzS76pgZkMU"
            .parse()
            .unwrap();
        let chunk = DataChunk {
            dataset_url: "s3://squidnet".to_string(),
            block_range: Range::new(0, 1000),
            size_bytes: 0,
        };
        assert_eq!(
            chunk.id().distance(&peer_id),
            [
                0x1b, 0x4e, 0x42, 0x76, 0x72, 0x9c, 0x4b, 0xab, 0x9f, 0x86, 0x7c, 0xc3, 0xb4, 0xcd,
                0xe6, 0x01, 0x05, 0x18, 0x16, 0x62, 0x8b, 0x1d, 0x00, 0x25, 0x41, 0x2f, 0x8a, 0xe0,
                0xed, 0xa9, 0xc2, 0x2f
            ]
        )
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
