use std::fmt::{Display, Formatter};

use nonempty::NonEmpty;
use serde::{Deserialize, Serialize};
use subsquid_network_transport::task_manager::CancellationToken;
use tokio::sync::mpsc::{Receiver, Sender};

use crate::data_chunk::{ChunkId, DataChunk};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SchedulingUnit {
    pub chunks: NonEmpty<DataChunk>,
}

pub type UnitId = ChunkId;

impl SchedulingUnit {
    pub fn from_slice(chunks: &[DataChunk]) -> Self {
        let chunks = NonEmpty::from_slice(chunks).expect("Empty slice");
        Self { chunks }
    }

    pub fn num_chunks(&self) -> usize {
        self.chunks.len()
    }

    pub fn size_bytes(&self) -> u64 {
        self.chunks.iter().map(|x| x.size_bytes).sum()
    }

    pub fn id(&self) -> UnitId {
        // ID of the unit is just ID of the first chunk. This way, when an incomplete unit is filled
        // later, it will still have the same ID.
        self.chunks.first().id()
    }

    pub fn dataset_url(&self) -> &str {
        self.chunks.first().dataset_url.as_str()
    }

    pub fn begin(&self) -> u32 {
        self.chunks.first().block_range.begin
    }
}

impl IntoIterator for SchedulingUnit {
    type Item = DataChunk;
    type IntoIter = <NonEmpty<DataChunk> as IntoIterator>::IntoIter;

    fn into_iter(self) -> Self::IntoIter {
        self.chunks.into_iter()
    }
}

impl Display for SchedulingUnit {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}/{}-{} ({} chunks)",
            self.chunks.first().dataset_url,
            self.chunks.first().block_range.begin,
            self.chunks.last().block_range.end,
            self.chunks.len()
        )
    }
}

pub async fn bundle_chunks(
    mut chunk_receiver: Receiver<NonEmpty<DataChunk>>,
    unit_sender: Sender<SchedulingUnit>,
    unit_size: usize,
    cancel_token: CancellationToken,
) {
    log::info!("Starting chunks bundler");
    let mut incomplete_unit: Option<SchedulingUnit> = None;
    loop {
        let mut chunks = tokio::select! {
            Some(chunks) = chunk_receiver.recv() => chunks,
            _ = cancel_token.cancelled() => break,
            else => break,
        };

        // Put all the remaining chunks from last round before the new ones.
        // If there was an incomplete unit, it will be filled and sent again.
        if let Some(unit) = incomplete_unit.take() {
            let new_chunks = std::mem::replace(&mut chunks, unit.chunks);
            chunks.append(&mut new_chunks.into())
        }

        let chunks: Vec<DataChunk> = chunks.into();
        for chunks in chunks.chunks(unit_size) {
            let unit = SchedulingUnit::from_slice(chunks);
            if unit.num_chunks() < unit_size {
                incomplete_unit = Some(unit.clone())
            }
            if unit_sender.send(unit).await.is_err() {
                log::info!("Scheduling unit receiver dropped");
                return;
            }
        }
    }
    log::info!("Stopping chunks bundler");
}
