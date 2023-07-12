use std::collections::{HashMap, HashSet};
use std::fmt::{Display, Formatter};

use subsquid_network_transport::PeerId;

use crate::data_chunk::chunks_to_worker_state;
use crate::scheduling_unit::{SchedulingUnit, UnitId};

#[derive(Debug, Clone)]
struct WorkerState {
    pub assigned_units: HashSet<UnitId>,
    pub total_stored_bytes: u64,
    pub max_stored_bytes: u64,
}

impl WorkerState {
    pub fn new(max_stored_bytes: u64) -> Self {
        Self {
            assigned_units: Default::default(),
            total_stored_bytes: 0,
            max_stored_bytes,
        }
    }

    pub fn try_assign_unit(&mut self, unit_id: UnitId, unit_size: u64) -> bool {
        if self.total_stored_bytes + unit_size > self.max_stored_bytes {
            return false;
        }
        if self.assigned_units.insert(unit_id) {
            self.total_stored_bytes += unit_size;
        }
        true
    }
}

impl Display for WorkerState {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} units assigned ({}/{} bytes)",
            self.assigned_units.len(),
            self.total_stored_bytes,
            self.max_stored_bytes
        )
    }
}

pub struct Scheduler {
    known_units: HashMap<UnitId, SchedulingUnit>,
    unassigned_units: HashSet<UnitId>,
    worker_states: HashMap<PeerId, WorkerState>,
    replication_factor: usize,
    worker_storage_bytes: u64,
}

impl Scheduler {
    pub fn new(replication_factor: usize, worker_storage_bytes: u64) -> Self {
        Self {
            known_units: Default::default(),
            unassigned_units: Default::default(),
            worker_states: Default::default(),
            replication_factor,
            worker_storage_bytes,
        }
    }

    pub fn new_unit(&mut self, unit: SchedulingUnit) {
        let unit_id = unit.id();
        if self.known_units.insert(unit_id, unit).is_none() {
            // If a unit is known, it should have been assigned already
            self.unassigned_units.insert(unit_id);
        }
    }

    pub fn get_worker_state(
        &self,
        worker_id: &PeerId,
    ) -> Option<router_controller::messages::WorkerState> {
        let chunks = match self.worker_states.get(worker_id) {
            None => return None,
            Some(state) => state
                .assigned_units
                .iter()
                .flat_map(|unit_id| self.get_unit(unit_id)),
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

        let unassigned_units = std::mem::take(&mut self.unassigned_units);

        // For every chunk, assign X workers with closest IDs
        for unit_id in unassigned_units {
            let unit = self.get_unit(&unit_id);
            log::debug!("Scheduling unit {unit}");

            workers.sort_by_cached_key(|worker_id| unit_id.distance(worker_id));
            let mut num_assigned_workers = 0;
            for worker_id in workers.iter() {
                log::debug!("Assigning unit {unit_id} to worker {worker_id}");
                num_assigned_workers +=
                    self.worker_states
                        .entry(*worker_id)
                        .or_insert_with(|| WorkerState::new(self.worker_storage_bytes))
                        .try_assign_unit(unit_id, unit.size_bytes()) as usize;
                if num_assigned_workers >= self.replication_factor {
                    break;
                }
            }
            if num_assigned_workers < self.replication_factor {
                self.unassigned_units.insert(unit_id);
            }
        }

        log::info!("Scheduling complete.");
        for (worker_id, state) in self.worker_states.iter() {
            log::info!("Worker {worker_id}: {state}");
        }
        if !self.unassigned_units.is_empty() {
            log::warn!(
                "Not enough workers to assign {} units",
                self.unassigned_units.len()
            )
        }
    }

    /// Clear existing assignments if worker set changed
    fn update_workers(&mut self, workers: &[PeerId]) {
        let new_workers: HashSet<&PeerId> = HashSet::from_iter(workers.iter());
        let old_workers: HashSet<&PeerId> = HashSet::from_iter(self.worker_states.keys());
        if new_workers != old_workers {
            log::info!("Active worker set changed. Chunks will be rescheduled.");
            self.worker_states.clear();
            self.unassigned_units = HashSet::from_iter(self.known_units.keys().cloned());
        }
    }
}
