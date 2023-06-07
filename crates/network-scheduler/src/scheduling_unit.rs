use nonempty::NonEmpty;
use tokio::sync::mpsc::{Receiver, Sender};

use crate::chunks::{ChunkId, DataChunk};

#[derive(Debug, Clone, PartialEq)]
pub struct SchedulingUnit {
    pub chunks: NonEmpty<DataChunk>,
}

pub type UnitId = ChunkId;

impl SchedulingUnit {
    pub fn from_slice(chunks: &[DataChunk]) -> Self {
        let chunks = NonEmpty::from_slice(chunks).expect("Empty slice");
        Self { chunks }
    }

    pub fn size(&self) -> usize {
        self.chunks.len()
    }

    pub fn id(&self) -> UnitId {
        // ID of the unit is just ID of the first chunk. This way, when a incomplete unit is filled
        // later, it will still have the same ID.
        self.chunks.first().id()
    }
}

impl IntoIterator for SchedulingUnit {
    type Item = DataChunk;
    type IntoIter = <NonEmpty<DataChunk> as IntoIterator>::IntoIter;

    fn into_iter(self) -> Self::IntoIter {
        self.chunks.into_iter()
    }
}

pub fn bundle_chunks(
    mut incoming_chunks: Receiver<NonEmpty<DataChunk>>,
    unit_sender: Sender<SchedulingUnit>,
    unit_size: usize,
) {
    tokio::spawn(async move {
        log::info!("Starting chunks bundler");
        let mut incomplete_unit: Option<SchedulingUnit> = None;
        while let Some(mut chunks) = incoming_chunks.recv().await {
            // Put all the remaining chunks from last round before the new ones.
            // If there was an incomplete unit, it will be filled and sent again.
            if let Some(unit) = incomplete_unit.take() {
                let new_chunks = std::mem::replace(&mut chunks, unit.chunks);
                chunks.append(&mut new_chunks.into())
            }

            let chunks: Vec<DataChunk> = chunks.into();
            for chunks in chunks.chunks(unit_size) {
                let unit = SchedulingUnit::from_slice(chunks);
                if unit.size() < unit_size {
                    incomplete_unit = Some(unit.clone())
                }
                if let Err(_) = unit_sender.send(unit).await {
                    log::info!("Scheduling unit receiver dropped");
                    return;
                }
            }
        }
        log::info!("Chunks stream ended");
    });
}
