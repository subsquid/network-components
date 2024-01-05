use std::collections::{BinaryHeap, HashMap, HashSet};
use std::fmt::{Display, Formatter};
use std::time::{Duration, SystemTime};

use iter_num_tools::lin_space;
use itertools::Itertools;
use rand::prelude::SliceRandom;
use rand::{thread_rng, Rng};
use random_choice::random_choice;
use serde::{Deserialize, Serialize};
use serde_with::{serde_as, TimestampMilliSeconds};

use contract_client::{Address, Worker};
use subsquid_messages::range::RangeSet;
use subsquid_messages::{pong::Status as WorkerStatus, PingV2};
use subsquid_network_transport::PeerId;

use crate::cli::Config;
use crate::data_chunk::{chunks_to_worker_state, DataChunk};
use crate::scheduling_unit::{SchedulingUnit, UnitId};

pub const SUPPORTED_WORKER_VERSIONS: [&str; 2] = ["0.1.5", "0.1.6"];

#[serde_as]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerState {
    pub peer_id: PeerId,
    pub address: Address,
    #[serde_as(as = "Option<TimestampMilliSeconds>")]
    pub last_ping: Option<SystemTime>,
    pub version: Option<String>,
    pub jailed: bool,
    pub assigned_units: HashSet<UnitId>,
    pub assigned_bytes: u64, // Can be outdated, source of truth is assigned_units
    pub stored_ranges: HashMap<String, RangeSet>, // dataset -> ranges
    pub stored_bytes: u64,
    #[serde(default)]
    pub num_missing_chunks: u32,
    #[serde_as(as = "TimestampMilliSeconds")]
    #[serde(default = "SystemTime::now")]
    pub last_assignment: SystemTime,
}

impl WorkerState {
    pub fn new(peer_id: PeerId, address: Address) -> Self {
        Self {
            peer_id,
            address,
            last_ping: None,
            version: None,
            jailed: false,
            assigned_units: HashSet::new(),
            stored_ranges: HashMap::new(),
            stored_bytes: 0,
            assigned_bytes: 0,
            num_missing_chunks: 0,
            last_assignment: SystemTime::now(),
        }
    }

    fn time_since_last_ping(&self) -> Option<Duration> {
        self.last_ping.and_then(|ping| ping.elapsed().ok())
    }

    /// Register ping msg from a worker.
    pub fn ping_v2(&mut self, msg: PingV2) {
        self.last_ping = Some(SystemTime::now());
        self.version = msg.version;
        self.stored_ranges = msg
            .stored_ranges
            .into_iter()
            .map(|r| (r.url, r.ranges.into()))
            .collect();
        self.stored_bytes = msg.stored_bytes.unwrap_or_default();
    }

    pub fn is_active(&self) -> bool {
        self.time_since_last_ping()
            .is_some_and(|x| x < Config::get().worker_inactive_timeout)
    }

    pub fn remaining_capacity(&self) -> u64 {
        Config::get()
            .worker_storage_bytes
            .saturating_sub(self.assigned_bytes)
    }

    pub fn try_assign_unit(&mut self, unit_id: UnitId, unit_size: u64) -> bool {
        if unit_size > self.remaining_capacity() {
            return false; // Not enough capacity
        }
        if self.assigned_units.insert(unit_id) {
            self.assigned_bytes += unit_size;
            return true; // Successfully assigned
        }
        false // Unit was already assigned before
    }

    pub fn remove_unit(&mut self, unit_id: &UnitId, unit_size: u64) {
        if self.assigned_units.remove(unit_id) {
            self.assigned_bytes -= unit_size;
        }
    }

    /// Assigned unit's size has increased. Unassing the unit if it doesn't fit anymore.
    /// Return true iff the unit remained assigned.
    pub fn try_expand_unit(&mut self, unit_id: &UnitId, old_size: u64, new_size: u64) -> bool {
        let size_diff = new_size - old_size;
        if self.remaining_capacity() > size_diff {
            self.assigned_bytes += size_diff;
            true
        } else {
            assert!(self.assigned_units.remove(unit_id));
            self.assigned_bytes -= old_size;
            false
        }
    }

    pub fn assigned_chunks<'a>(
        &'a self,
        units_map: &'a HashMap<UnitId, SchedulingUnit>,
    ) -> impl Iterator<Item = DataChunk> + 'a {
        self.assigned_units.iter().flat_map(|unit_id| {
            units_map
                .get(unit_id)
                .expect("Unknown scheduling unit")
                .clone()
        })
    }

    fn count_missing_chunks<'a>(&'a self, units: &'a HashMap<UnitId, SchedulingUnit>) -> u32 {
        self.assigned_chunks(units)
            .map(|chunk| match self.stored_ranges.get(&chunk.dataset_url) {
                Some(range_set) if range_set.includes(chunk.block_range) => 0,
                _ => 1,
            })
            .sum()
    }

    /// Check if the worker is making progress with downloading missing chunks.
    /// If it's not, jail the worker and return Err with unassigned units' IDs.
    /// Assumes the worker is not jailed.
    pub fn check_download_progress<'a>(
        &'a mut self,
        units: &'a HashMap<UnitId, SchedulingUnit>,
    ) -> Result<(), Vec<UnitId>> {
        assert!(!self.jailed);
        if self
            .last_assignment
            .elapsed()
            .is_ok_and(|d| d < Config::get().worker_stale_timeout)
        {
            return Ok(());
        }

        let num_missing_chunks = self.count_missing_chunks(units);
        if num_missing_chunks == 0 {
            log::debug!("Worker {} is fully synced", self.peer_id);
            self.num_missing_chunks = num_missing_chunks;
            Ok(())
        } else if num_missing_chunks < self.num_missing_chunks {
            log::debug!(
                "Worker {} is making progress {} -> {} chunks missing",
                self.peer_id,
                self.num_missing_chunks,
                num_missing_chunks
            );
            self.num_missing_chunks = num_missing_chunks;
            Ok(())
        } else {
            log::debug!(
                "Worker {} has not downloaded any chunks since last check",
                self.peer_id
            );
            Err(self.jail())
        }
    }

    pub fn reset_download_progress<'a>(&'a mut self, units: &'a HashMap<UnitId, SchedulingUnit>) {
        self.num_missing_chunks = self.count_missing_chunks(units);
        self.last_assignment = SystemTime::now();
    }

    /// Jail the worker, unassign all units and return their IDs.
    pub fn jail(&mut self) -> Vec<UnitId> {
        log::info!("Jailing worker {}", self.peer_id);
        self.jailed = true;
        self.assigned_bytes = 0;
        self.num_missing_chunks = 0;
        self.assigned_units.drain().collect()
    }

    pub fn release(&mut self) {
        log::info!("Releasing worker {}", self.peer_id);
        self.jailed = false;
    }
}

impl Display for WorkerState {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}: {} units assigned ({} bytes)",
            self.peer_id,
            self.assigned_units.len(),
            self.assigned_bytes,
        )
    }
}

#[derive(Default, Serialize, Deserialize)]
pub struct Scheduler {
    known_units: HashMap<UnitId, SchedulingUnit>,
    units_assignments: HashMap<UnitId, Vec<PeerId>>,
    worker_states: HashMap<PeerId, WorkerState>,
}

impl Scheduler {
    pub fn clear_deprecated_units(&mut self) {
        let dataset_urls: HashSet<String> = Config::get()
            .dataset_buckets
            .iter()
            .map(|bucket| format!("s3://{bucket}"))
            .collect();
        let deprecated_unit_ids: Vec<UnitId> = self
            .known_units
            .iter()
            .filter_map(|(unit_id, unit)| {
                (!dataset_urls.contains(unit.dataset_url())).then_some(*unit_id)
            })
            .collect();
        for unit_id in deprecated_unit_ids.iter() {
            let unit = self.known_units.remove(unit_id).expect("unknown unit");
            log::info!("Removing deprecated scheduling unit {unit}");
            let unit_size = unit.size_bytes();
            self.units_assignments
                .remove(unit_id)
                .unwrap_or_default()
                .into_iter()
                .for_each(|worker_id| {
                    self.worker_states
                        .get_mut(&worker_id)
                        .expect("unknown worker")
                        .remove_unit(unit_id, unit_size)
                });
        }
    }

    /// Register ping msg from a worker. Returns worker status if ping was accepted, otherwise None
    pub fn ping_v2(&mut self, worker_id: PeerId, msg: PingV2) -> WorkerStatus {
        let version = msg.version.clone().unwrap_or_default();
        if !SUPPORTED_WORKER_VERSIONS.iter().any(|v| *v == version) {
            log::debug!("Worker {worker_id} version not supported: {}", version);
            return WorkerStatus::UnsupportedVersion(());
        }
        let worker_state = match self.worker_states.get_mut(&worker_id) {
            None => {
                log::debug!("Worker {worker_id} not registered");
                return WorkerStatus::NotRegistered(());
            }
            Some(worker_state) => worker_state,
        };
        worker_state.ping_v2(msg);
        if worker_state.jailed {
            return WorkerStatus::Jailed(());
        }
        let state = chunks_to_worker_state(worker_state.assigned_chunks(&self.known_units));
        WorkerStatus::Active(state)
    }

    pub fn known_units(&self) -> HashMap<UnitId, SchedulingUnit> {
        self.known_units.clone()
    }

    pub fn new_unit(&mut self, unit: SchedulingUnit) {
        let unit_id = unit.id();
        let unit_size = unit.size_bytes();
        let unit_str = unit.to_string();
        match self.known_units.insert(unit_id, unit) {
            None => {
                // New unit
                log::debug!("New scheduling unit: {unit_str}");
                self.units_assignments.insert(
                    unit_id,
                    Vec::with_capacity(Config::get().replication_factor),
                );
            }
            Some(old_unit) => {
                // New chunks added to an existing unit
                let old_size = old_unit.size_bytes();
                if old_size == unit_size {
                    return;
                }
                log::debug!(
                    "Scheduling unit {unit_str} resized from {old_size} bytes to {unit_size} bytes"
                );
                self.units_assignments
                    .get_mut(&unit_id)
                    .expect("No assignment entry for unit")
                    .retain(|worker_id| {
                        self.worker_states
                            .get_mut(worker_id)
                            .expect("Unknown worker")
                            .try_expand_unit(&unit_id, old_size, unit_size)
                    });
            }
        }
    }

    pub fn all_workers(&self) -> Vec<WorkerState> {
        self.worker_states.values().cloned().collect()
    }

    pub fn active_workers(&self) -> Vec<WorkerState> {
        self.worker_states
            .values()
            .filter(|w| w.is_active())
            .cloned()
            .collect()
    }

    fn get_worker(&mut self, worker_id: &PeerId) -> &mut WorkerState {
        self.worker_states
            .get_mut(worker_id)
            .expect("Unknown worker")
    }

    fn num_replicas(&self, unit_id: &UnitId) -> usize {
        self.units_assignments
            .get(unit_id)
            .map(|x| x.len())
            .unwrap_or_default()
    }

    pub fn schedule(&mut self) {
        log::info!(
            "Starting scheduling. Total registered workers: {} Total units: {}",
            self.worker_states.len(),
            self.known_units.len()
        );
        self.release_jailed_workers();
        self.mix_random_units();
        self.assign_units();
    }

    pub fn update_workers(&mut self, workers: Vec<Worker>) {
        log::info!("Updating workers");
        let mut old_workers = std::mem::take(&mut self.worker_states);

        // For each of the new workers, find an existing state or create a blank one
        self.worker_states = workers
            .into_iter()
            .map(|w| {
                let worker_state = old_workers
                    .remove(&w.peer_id)
                    .unwrap_or_else(|| WorkerState::new(w.peer_id, w.address));
                (w.peer_id, worker_state)
            })
            .collect();

        // Workers which remained in the map are no longer registered
        for (_, worker) in old_workers {
            log::info!("Worker unregistered: {worker:?}");
            for unit_id in worker.assigned_units {
                self.units_assignments
                    .get_mut(&unit_id)
                    .expect("unknown unit")
                    .retain(|id| *id != worker.peer_id);
            }
        }
    }

    fn release_jailed_workers(&mut self) {
        log::info!("Releasing jailed workers");
        self.worker_states
            .values_mut()
            .filter(|w| w.jailed)
            .for_each(|w| w.release());
    }

    /// Jail workers which don't send pings.
    pub fn jail_inactive_workers(&mut self) {
        log::info!("Jailing inactive workers");
        let mut num_jailed_workers = 0;
        let mut num_unassigned_units = 0;

        self.worker_states
            .values_mut()
            .filter(|w| !w.jailed && !w.is_active())
            .for_each(|w| {
                let units = w.jail();
                num_jailed_workers += 1;
                num_unassigned_units += units.len();
                for unit_id in units {
                    self.units_assignments
                        .get_mut(&unit_id)
                        .expect("Unit assignment missing")
                        .retain(|id| *id != w.peer_id)
                }
            });

        log::info!("Jailed {num_jailed_workers} workers. Unassigned {num_unassigned_units} units");
        if num_unassigned_units > 0 {
            self.assign_units();
        }
    }

    /// Jail workers which don't make download progress.
    pub fn jail_stale_workers(&mut self) {
        log::info!("Jailing stale workers");
        let mut num_jailed_workers = 0;
        let mut num_unassigned_units = 0;

        self.worker_states
            .values_mut()
            .filter(|w| !w.jailed)
            .for_each(|w| {
                let units = match w.check_download_progress(&self.known_units) {
                    Ok(_) => return,
                    Err(units) => units,
                };
                num_jailed_workers += 1;
                num_unassigned_units += units.len();
                for unit_id in units {
                    self.units_assignments
                        .get_mut(&unit_id)
                        .expect("Unit assignment missing")
                        .retain(|id| *id != w.peer_id)
                }
            });

        log::info!("Jailed {num_jailed_workers} workers. Unassigned {num_unassigned_units} units");
        if num_unassigned_units > 0 {
            self.assign_units();
        }
    }

    fn mix_random_units(&mut self) {
        log::info!("Mixing random units");

        // Group units by dataset and unassign random fraction of units for each dataset
        let grouped_units = self
            .known_units
            .iter()
            .filter(|(unit_id, _)| self.num_replicas(unit_id) > 0)
            .into_group_map_by(|(_, unit)| unit.dataset_url());

        for (dataset_url, mut dataset_units) in grouped_units {
            // Sort units from oldest to newest and give them weights making
            // the most recent units more likely to be re-assigned
            dataset_units.sort_by_cached_key(|(_, unit)| unit.begin());
            let num_units = dataset_units.len();
            let num_mixed = ((num_units as f64) * Config::get().mixed_units_ratio) as usize;
            let max_weight = Config::get().mixing_recent_unit_weight;
            let weights: Vec<f64> = lin_space(1.0..=max_weight, num_units).collect();
            let mixed_units =
                random_choice().random_choice_f64(&dataset_units, &weights, num_mixed);
            log::info!("Mixing {num_mixed} out of {num_units} units for dataset {dataset_url}");

            // For each of the randomly selected units, remove one random replica
            for (unit_id, unit) in mixed_units {
                let holder_ids = self
                    .units_assignments
                    .get_mut(*unit_id)
                    .expect("no empty assignments");
                let random_idx = thread_rng().gen_range(0..holder_ids.len());
                let holder_id = holder_ids.remove(random_idx);
                self.worker_states
                    .get_mut(&holder_id)
                    .expect("Unknown worker")
                    .remove_unit(unit_id, unit.size_bytes());
            }
        }
    }

    fn assign_units(&mut self) {
        log::info!("Assigning units");

        // Only active and non-jailed workers are eligible for assignment
        let mut workers: Vec<&WorkerState> = self
            .worker_states
            .values()
            .filter(|w| w.is_active() && !w.jailed)
            .collect();

        // Randomly shuffle workers, then use a heap based on remaining capacity to make
        // the data distribution as uniform as possible
        workers.shuffle(&mut thread_rng());
        let mut workers: BinaryHeap<(u64, PeerId)> = workers
            .into_iter()
            .map(|w| (w.remaining_capacity(), w.peer_id))
            .collect();

        // Use a heap based on nuber of missing replicas so that units are assigned
        // more evenly if there is not enough worker capacity for all
        let rep_factor = Config::get().replication_factor;
        let mut units: BinaryHeap<(usize, u64, UnitId)> = self
            .known_units
            .iter()
            .filter_map(|(unit_id, unit)| {
                let missing_replicas = rep_factor - self.num_replicas(unit_id);
                (missing_replicas > 0).then_some((missing_replicas, unit.size_bytes(), *unit_id))
            })
            .collect();

        log::info!(
            "Workers available: {}  Units to assign: {}",
            workers.len(),
            units.len()
        );

        while let Some((missing_replicas, unit_size, unit_id)) = units.pop() {
            let mut rejected_workers = vec![];
            let mut found_worker = false;
            while let Some((remaining_capacity, worker_id)) = workers.pop() {
                if self
                    .get_worker(&worker_id)
                    .try_assign_unit(unit_id, unit_size)
                {
                    log::debug!("Assigned unit {unit_id} to worker {worker_id}");
                    found_worker = true;
                    workers.push((remaining_capacity - unit_size, worker_id));
                    self.units_assignments
                        .get_mut(&unit_id)
                        .expect("No unit assignment")
                        .push(worker_id);
                    break;
                } else {
                    rejected_workers.push((remaining_capacity, worker_id))
                }
            }
            if found_worker && missing_replicas > 1 {
                log::debug!("Unit {unit_id} still has {missing_replicas} missing replicas");
                units.push((missing_replicas - 1, unit_size, unit_id));
            }
            workers.extend(rejected_workers);
        }

        log::info!(
            "Assignment complete. {} units are missing some replicas",
            self.units_assignments
                .values()
                .filter(|workers| workers.len() < rep_factor)
                .count()
        );
        self.worker_states
            .values_mut()
            .filter(|w| w.is_active() && !w.jailed)
            .for_each(|w| {
                w.reset_download_progress(&self.known_units);
                log::info!("{w}")
            });
    }
}
