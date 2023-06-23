use std::collections::{HashMap, HashSet};

use router_controller::messages::WorkerState;
use subsquid_network_transport::PeerId;

use crate::data_chunk::chunks_to_worker_state;
use crate::scheduling_unit::{SchedulingUnit, UnitId};

pub struct Scheduler {
    known_units: HashMap<UnitId, SchedulingUnit>,
    unassigned_units: HashSet<UnitId>,
    worker_states: HashMap<PeerId, Vec<UnitId>>,
    replication_factor: usize,
}

impl Scheduler {
    pub fn new(replication_factor: usize) -> Self {
        Self {
            known_units: Default::default(),
            unassigned_units: Default::default(),
            worker_states: Default::default(),
            replication_factor,
        }
    }

    pub fn new_unit(&mut self, unit: SchedulingUnit) {
        let unit_id = unit.id();
        if self.known_units.insert(unit_id, unit).is_none() {
            // If a unit is known, it should have been assigned already
            self.unassigned_units.insert(unit_id);
        }
    }

    pub fn get_worker_state(&self, worker_id: &PeerId) -> Option<WorkerState> {
        let chunks = match self.worker_states.get(worker_id) {
            None => return None,
            Some(units) => units.iter().flat_map(|unit_id| self.get_unit(unit_id)),
        };
        Some(chunks_to_worker_state(chunks))
    }

    fn get_unit(&self, unit_id: &UnitId) -> SchedulingUnit {
        self.known_units
            .get(unit_id)
            .expect("Unknown scheduling unit")
            .clone()
    }

    pub fn schedule(&mut self, mut workers: Vec<PeerId>) {
        self.update_workers(&workers);

        log::info!(
            "Starting scheduling. num_workers: {} num_units: {}",
            workers.len(),
            self.unassigned_units.len()
        );
        if workers.len() < self.replication_factor {
            log::warn!(
                "Not enough workers for replication: {}/{}",
                workers.len(),
                self.replication_factor
            );
        }
        // For every chunk, assign X workers with closest IDs
        for unit_id in self.unassigned_units.drain() {
            workers.sort_by_cached_key(|worker_id| unit_id.distance(worker_id));
            for worker_id in workers.iter().take(self.replication_factor) {
                log::debug!("Assigning unit {unit_id} to worker {worker_id}");
                self.worker_states
                    .get_mut(worker_id)
                    .expect("Workers should be updated")
                    .push(unit_id);
            }
        }
        log::info!("Scheduling complete.");
        for (worker_id, state) in self.worker_states.iter() {
            log::info!("Worker {worker_id}: {} units assigned", state.len());
        }
    }

    /// Clear existing assignments if worker set changed
    fn update_workers(&mut self, workers: &[PeerId]) {
        let new_workers: HashSet<&PeerId> = HashSet::from_iter(workers.iter());
        let old_workers: HashSet<&PeerId> = HashSet::from_iter(self.worker_states.keys());
        if new_workers != old_workers {
            log::info!("Active worker set changed. Chunks will be rescheduled.");
            self.worker_states.clear();
            for worker_id in new_workers {
                self.worker_states.insert(*worker_id, vec![]);
            }
            self.unassigned_units = HashSet::from_iter(self.known_units.keys().cloned());
        }
    }
}
