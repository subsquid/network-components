use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::str::FromStr;

use itertools::Itertools;
use once_cell::sync::OnceCell;
use serde::{Deserialize, Serialize};
use serde_with::{hex::Hex, serde_as};
use sha3::{Digest, Sha3_256};

use sqd_messages::{AssignedChunk, DatasetChunks, Range, WorkerAssignment, WorkerState};

use crate::cli::Config;

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
    #[serde(alias = "dataset_url")]
    pub dataset_id: String,
    #[serde(default)]
    pub download_url: String,
    pub block_range: Range,
    pub size_bytes: u64,
    pub chunk_str: String,
    #[serde(default)]
    pub filenames: Vec<String>,
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

pub fn chunks_to_worker_state(chunks: impl IntoIterator<Item = DataChunk>) -> WorkerState {
    let datasets = chunks
        .into_iter()
        .map(|chunk| (chunk.dataset_id, chunk.block_range))
        .into_grouping_map()
        .collect();
    WorkerState { datasets }
}

#[allow(deprecated)]
pub fn chunks_to_assignment(chunks: impl Iterator<Item = DataChunk>) -> WorkerAssignment {
    let mut known_filenames: HashMap<String, u32> = HashMap::new();
    let mut filename_to_index = |filename: String| {
        let next_index = known_filenames.len() as u32;
        *known_filenames.entry(filename).or_insert(next_index)
    };
    #[allow(clippy::redundant_closure)]
    let dataset_chunks = chunks
        .into_group_map_by(|chunk| chunk.dataset_id.clone())
        .into_iter()
        .map(|(dataset_id, chunks)| DatasetChunks {
            dataset_id: dataset_id.clone(),
            download_url: chunks.first().unwrap().download_url.clone(),
            chunks: chunks
                .into_iter()
                .map(|chunk| AssignedChunk {
                    path: chunk.chunk_str,
                    filenames: chunk
                        .filenames
                        .into_iter()
                        .map(|filename| filename_to_index(filename))
                        .collect(),
                })
                .collect(),
        })
        .collect();
    WorkerAssignment {
        dataset_chunks,
        http_headers: Default::default(),
        known_filenames: known_filenames
            .into_iter()
            .sorted_by_key(|(_filename, index)| *index)
            .map(|(filename, _index)| filename)
            .collect(),
    }
}

#[cfg(test)]
mod tests {
    use sqd_messages::range::RangeSet;

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

    #[test]
    #[rustfmt::skip]
    #[allow(deprecated)]
    fn test_conversion() {
        let chunks = vec![
            DataChunk {
                dataset_id: "s3://squidnet".to_string(),
                download_url: "https://squidnet.sqd-datasets.io".to_string(),
                block_range: Range::new(0, 1000),
                size_bytes: 0,
                chunk_str: "/00000/00001-01000-fa1f6773".to_string(),
                filenames: vec![
                    "blocks.parquet".to_string(),
                    "transactions.parquet".to_string(),
                    "logs.parquet".to_string(),
                ],
                id: Default::default(),
            },
            DataChunk {
                dataset_id: "s3://squidnet".to_string(),
                download_url: "https://squidnet.sqd-datasets.io".to_string(),
                block_range: Range::new(500, 1500),
                size_bytes: 0,
                chunk_str: "/00000/00500-01500-82315a24".to_string(),
                filenames: vec!["blocks.parquet".to_string(), "traces.parquet".to_string()],
                id: Default::default(),
            },
            DataChunk {
                dataset_id: "s3://pepenet".to_string(),
                download_url: "https://pepenet.sqd-datasets.io".to_string(),
                block_range: Range::new(1234, 5678),
                size_bytes: 0,
                chunk_str: "00000/01234-05678-b4357d89".to_string(),
                filenames: vec!["blocks.parquet".to_string(), "transactions.parquet".to_string()],
                id: Default::default(),
            },
        ];

        assert_eq!(chunks_to_worker_state(chunks.clone()), WorkerState {
            datasets: vec![
                ("s3://squidnet".to_string(), RangeSet { ranges: vec![Range::new(0, 1500)] }),
                ("s3://pepenet".to_string(), RangeSet { ranges: vec![Range::new(1234, 5678)] }),
            ].into_iter().collect()
        });

        assert_eq!(chunks_to_assignment(chunks.into_iter()), WorkerAssignment {
            known_filenames: vec![
                "blocks.parquet".to_string(),
                "transactions.parquet".to_string(),
                "logs.parquet".to_string(),
                "traces.parquet".to_string(),
            ],
            dataset_chunks: vec![
                DatasetChunks {
                    dataset_id: "s3://squidnet".to_string(),
                    download_url: "https://squidnet.sqd-datasets.io".to_string(),
                    chunks: vec![
                        AssignedChunk {
                            path: "/00000/00001-01000-fa1f6773".to_string(),
                            filenames: vec![0, 1, 2],
                        },
                        AssignedChunk {
                            path: "/00000/00500-01500-82315a24".to_string(),
                            filenames: vec![0, 3],
                        },
                    ],
                },
                DatasetChunks{
                    dataset_id: "s3://pepenet".to_string(),
                    download_url: "https://pepenet.sqd-datasets.io".to_string(),
                    chunks: vec![
                        AssignedChunk {
                            path: "00000/01234-05678-b4357d89".to_string(),
                            filenames: vec![0, 1],
                        }
                    ]
                }
            ],
            ..Default::default()
        });
    }
}
