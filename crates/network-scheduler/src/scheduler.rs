use crate::chunks::{chunks_to_worker_state, ChunkId, DataChunk};
use router_controller::messages::WorkerState;
use std::collections::{HashMap, HashSet};
use subsquid_network_transport::PeerId;

pub struct Scheduler {
    known_chunks: HashMap<ChunkId, DataChunk>,
    unassigned_chunks: HashSet<ChunkId>,
    worker_states: HashMap<PeerId, Vec<ChunkId>>,
    replication_factor: usize,
}

impl Scheduler {
    pub fn new(replication_factor: usize) -> Self {
        Self {
            known_chunks: Default::default(),
            unassigned_chunks: Default::default(),
            worker_states: Default::default(),
            replication_factor,
        }
    }

    pub fn new_chunk(&mut self, chunk: DataChunk) {
        let chunk_id = chunk.id();
        if self.known_chunks.insert(chunk_id, chunk).is_some() {
            log::warn!("Chunk duplicate received: {chunk_id}");
        }
        self.unassigned_chunks.insert(chunk_id);
    }

    pub fn get_worker_state(&self, worker_id: &PeerId) -> WorkerState {
        let chunks = match self.worker_states.get(worker_id) {
            None => return Default::default(),
            Some(chunks) => chunks.iter().map(|chunk_id| self.get_chunk(chunk_id)),
        };
        chunks_to_worker_state(chunks)
    }

    fn get_chunk(&self, chunk_id: &ChunkId) -> DataChunk {
        self.known_chunks
            .get(chunk_id)
            .expect("Unknown chunk")
            .clone()
    }

    pub fn schedule(&mut self, mut workers: Vec<PeerId>) {
        self.update_workers(&workers);

        log::info!(
            "Starting scheduling. num_workers: {} num_chunks: {}",
            workers.len(),
            self.unassigned_chunks.len()
        );
        // For every chunk, assign X workers with closest IDs
        for chunk_id in self.unassigned_chunks.drain() {
            workers.sort_by_cached_key(|worker_id| chunk_id.distance(worker_id));
            for worker_id in workers.iter().take(self.replication_factor) {
                log::debug!("Assigning chunk {chunk_id} to worker {worker_id}");
                self.worker_states
                    .get_mut(worker_id)
                    .expect("Workers should be updated")
                    .push(chunk_id);
            }
        }
        log::info!("Scheduling complete.")
    }

    /// Clear existing assignments if worker set changed
    fn update_workers(&mut self, workers: &Vec<PeerId>) {
        let new_workers: HashSet<&PeerId> = HashSet::from_iter(workers.iter());
        let old_workers: HashSet<&PeerId> = HashSet::from_iter(self.worker_states.keys());
        if new_workers != old_workers {
            log::info!("Active worker set changed. Chunks will be rescheduled.");
            self.worker_states.clear();
            for worker_id in new_workers {
                self.worker_states.insert(worker_id.clone(), vec![]);
            }
            self.unassigned_chunks = HashSet::from_iter(self.known_chunks.keys().cloned());
        }
    }
}
