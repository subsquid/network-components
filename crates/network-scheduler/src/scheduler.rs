use std::cmp::max;
use std::collections::{BinaryHeap, HashMap, HashSet};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use dashmap::mapref::one::RefMut;
use dashmap::DashMap;
use iter_num_tools::lin_space;
use itertools::Itertools;
use parking_lot::RwLock;
use prometheus_client::metrics::gauge::Atomic;
use rand::prelude::SliceRandom;
use rand::{thread_rng, Rng};
use random_choice::random_choice;
use serde::{Deserialize, Serialize};
use tokio::time::Instant;

use sqd_contract_client::Worker;
use sqd_messages::HttpHeader;
use sqd_messages::{pong::Status as WorkerStatus, Ping};
use sqd_network_transport::PeerId;

use crate::cli::Config;
use crate::data_chunk::chunks_to_assignment;
use crate::prometheus_metrics;
use crate::scheduling_unit::{SchedulingUnit, UnitId};
use crate::worker_state::{JailReason, WorkerState};

const WORKER_ID_HEADER: &str = "worker-id";
const WORKER_SIGNATURE_HEADER: &str = "worker-signature";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChunkStatus {
    pub begin: u32,
    pub end: u32,
    pub size_bytes: u64,
    pub assigned_to: Vec<Arc<str>>, // Will deserialize duplicated, but it's short-lived
    pub downloaded_by: Vec<Arc<str>>, // Will deserialize duplicated, but it's short-lived
}

#[derive(Default, Clone, Serialize, Deserialize)]
pub struct Scheduler {
    known_units: Arc<DashMap<UnitId, SchedulingUnit>>,
    units_assignments: Arc<DashMap<UnitId, Vec<PeerId>>>,
    worker_states: Arc<DashMap<PeerId, WorkerState>>,
    chunks_summary: Arc<RwLock<HashMap<String, Vec<ChunkStatus>>>>, // dataset -> chunks statuses
    last_schedule_epoch: Arc<AtomicU32>,
}

impl Scheduler {
    pub fn to_json(&self) -> anyhow::Result<Vec<u8>> {
        Ok(serde_json::to_vec(self)?)
    }

    pub fn from_json(payload: &[u8]) -> anyhow::Result<Self> {
        Ok(serde_json::from_slice(payload)?)
    }

    pub fn last_schedule_epoch(&self) -> u32 {
        self.last_schedule_epoch.get()
    }

    pub fn clear_deprecated_units(&mut self) {
        let dataset_urls: HashSet<String> = Config::get()
            .dataset_buckets
            .iter()
            .map(|bucket| format!("s3://{bucket}"))
            .collect();
        let deprecated_unit_ids = self
            .known_units
            .iter()
            .filter_map(|unit| (!dataset_urls.contains(unit.dataset_url())).then_some(*unit.key()))
            .collect_vec();
        for unit_id in deprecated_unit_ids.iter() {
            let (_, unit) = self.known_units.remove(unit_id).expect("unknown unit");
            log::info!("Removing deprecated scheduling unit {unit}");
            let unit_size = unit.size_bytes();
            self.units_assignments
                .remove(unit_id)
                .map(|(_, workers)| workers)
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

    pub fn regenerate_signatures(&self) {
        let start = Instant::now();
        for mut state in self.worker_states.iter_mut() {
            state.regenerate_signature();
        }
        prometheus_metrics::exec_time("regen_signatures", start.elapsed());
    }

    /// Register ping msg from a worker. Returns worker status if ping was accepted, otherwise None
    pub fn ping(&self, worker_id: PeerId, msg: Ping) -> Option<WorkerStatus> {
        if !msg.version_matches(&Config::get().supported_worker_versions) {
            log::debug!(
                "Worker {worker_id} version not supported: {:?}",
                msg.version
            );
            return Some(WorkerStatus::UnsupportedVersion(()));
        }
        let mut worker_state = match self.worker_states.get_mut(&worker_id) {
            None => {
                log::debug!("Worker {worker_id} not registered");
                return None;
            }
            Some(worker_state) => worker_state,
        };
        worker_state.ping(msg);
        if worker_state.jailed {
            return Some(WorkerStatus::Jailed(worker_state.jail_reason_str()));
        }
        let assigned_chunks = worker_state.assigned_chunks(&self.known_units);
        let mut assignment = chunks_to_assignment(assigned_chunks);
        add_signature_headers(&mut assignment, &worker_id, worker_state.value());
        Some(WorkerStatus::Active(assignment))
    }

    pub fn workers_to_dial(&self) -> Vec<PeerId> {
        self.worker_states
            .iter()
            .filter_map(|worker_ref| {
                if !worker_ref.is_active() {
                    return None; // Worker not active - don't dial
                }
                let Some(last_dial) = worker_ref.last_dial_time else {
                    return Some(worker_ref.peer_id); // Worker has never been dialed - dial now
                };
                let time_since_last_dial = last_dial.elapsed().expect("time doesn't go backwards");
                let retry_interval = if worker_ref.last_dial_ok {
                    Config::get().successful_dial_retry
                } else {
                    Config::get().failed_dial_retry
                };
                (time_since_last_dial > retry_interval).then_some(worker_ref.peer_id)
            })
            .collect()
    }

    pub fn worker_dialed(&self, worker_id: PeerId, reachable: bool) {
        log::info!("Dialed worker {worker_id}. reachable={reachable}");
        let start = Instant::now();
        match self.worker_states.get_mut(&worker_id) {
            Some(mut worker_state) => worker_state.dialed(reachable),
            None => log::error!("Unknown worker dialed: {worker_id}"),
        }
        prometheus_metrics::exec_time("worker_dialed", start.elapsed());
    }

    pub fn known_units(&self) -> DashMap<UnitId, SchedulingUnit> {
        (*self.known_units).clone()
    }

    pub fn total_data_size(&self) -> u64 {
        self.known_units.iter().map(|u| u.size_bytes()).sum()
    }

    pub fn new_unit(&self, unit: SchedulingUnit) {
        let start = Instant::now();
        let unit_id = *unit.id();
        let unit_size = unit.size_bytes();
        let unit_str = unit.to_string();
        let old_size = match self.known_units.insert(unit_id, unit) {
            None => {
                // New unit
                log::debug!("New scheduling unit: {unit_str}");
                self.units_assignments.insert(
                    unit_id,
                    Vec::with_capacity(Config::get().replication_factor),
                );
                return;
            }
            Some(old_unit) => old_unit.size_bytes(),
        };
        log::debug!(
            "Scheduling unit {unit_str} resized from {old_size} bytes to {unit_size} bytes"
        );
        // Clone to avoid locking unit_assignments and worker_states simultaneously
        let mut holders = self
            .units_assignments
            .get(&unit_id)
            .expect("No assignment entry for unit")
            .clone();
        holders.retain(|worker_id| {
            let mut worker = self
                .worker_states
                .get_mut(worker_id)
                .expect("Unknown worker");
            let retained = worker.try_expand_unit(&unit_id, old_size, unit_size);
            worker.reset_download_progress(&self.known_units);
            retained
        });
        self.units_assignments
            .get_mut(&unit_id)
            .expect("No assignment entry for unit")
            .retain(|worker_id| holders.contains(worker_id));
        prometheus_metrics::exec_time("new_unit", start.elapsed());
    }

    pub fn all_workers(&self) -> Vec<WorkerState> {
        self.worker_states.iter().map(|w| w.clone()).collect()
    }

    pub fn active_workers(&self) -> Vec<WorkerState> {
        self.worker_states
            .iter()
            .filter(|w| w.is_active())
            .map(|w| w.clone())
            .collect()
    }

    fn get_worker(&self, worker_id: &PeerId) -> RefMut<PeerId, WorkerState> {
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

    pub fn schedule(&self, epoch: u32) {
        log::info!(
            "Starting scheduling. Total registered workers: {} Total units: {}",
            self.worker_states.len(),
            self.known_units.len()
        );
        self.release_jailed_workers();
        self.mix_random_units();
        self.assign_units();
        self.last_schedule_epoch.store(epoch, Ordering::Relaxed);
    }

    pub fn update_workers(&self, workers: Vec<Worker>) {
        log::info!("Updating workers");
        let new_workers: HashMap<_, _> = workers
            .into_iter()
            .map(|w| (w.peer_id, w.address))
            .collect();
        let current_workers: HashSet<_> = self.worker_states.iter().map(|w| *w.key()).collect();

        // Remove workers which are no longer registered
        current_workers
            .iter()
            .filter(|peer_id| !new_workers.contains_key(peer_id))
            .for_each(|peer_id| {
                let (_, worker) = self
                    .worker_states
                    .remove(peer_id)
                    .expect("current_workers are based of worker_states");
                log::info!("Worker unregistered: {worker:?}");
                for unit_id in worker.assigned_units {
                    self.units_assignments
                        .get_mut(&unit_id)
                        .expect("unknown unit")
                        .retain(|id| *id != worker.peer_id);
                }
            });

        // Create blank states for newly registered workers
        new_workers
            .into_iter()
            .filter(|w| !current_workers.contains(&w.0))
            .for_each(|(peer_id, address)| {
                log::info!("New worker registered: {peer_id} {address}");
                self.worker_states
                    .insert(peer_id, WorkerState::new(peer_id, address));
            });
    }

    fn release_jailed_workers(&self) {
        log::info!("Releasing jailed workers");
        let start = Instant::now();
        let release_unreachable = !Config::get().jail_unreachable;
        self.worker_states
            .iter_mut()
            .filter(|w| w.jailed && w.is_active() && (release_unreachable || !w.is_unreachable()))
            .for_each(|mut w| w.release());
        prometheus_metrics::exec_time("release_workers", start.elapsed());
    }

    /// Jail workers which don't send pings.
    pub fn jail_inactive_workers(&self) -> bool {
        log::info!("Jailing inactive workers");
        self.jail_workers(|w| !w.is_active(), JailReason::Inactive)
    }

    /// Jail workers which don't make download progress.
    pub fn jail_stale_workers(&self) -> bool {
        log::info!("Jailing stale workers");
        self.jail_workers(
            |w| !w.check_download_progress(&self.known_units),
            JailReason::Stale,
        )
    }

    pub fn jail_unreachable_workers(&self) -> bool {
        if Config::get().jail_unreachable {
            log::info!("Jailing unreachable workers");
            self.jail_workers(|w| w.is_unreachable(), JailReason::Unreachable)
        } else {
            log::info!("Jailing unreachable workers is disabled");
            self.worker_states
                .iter()
                .filter(|w| w.ever_been_active() && w.is_unreachable())
                .for_each(|w| log::info!("Worker  {} is unreachable", w.peer_id));
            false
        }
    }

    fn jail_workers(
        &self,
        mut criterion: impl FnMut(&mut WorkerState) -> bool,
        reason: JailReason,
    ) -> bool {
        let start = Instant::now();

        let jailed = self
            .worker_states
            .iter_mut()
            .filter(|w| !w.jailed) // Don't jail workers that are already jailed
            .filter(|w| w.ever_been_active()) // Don't jail workers that haven't been started yet
            .filter_map(|mut w| {
                if criterion(&mut w) {
                    let units = w.jail(reason);
                    Some((w.peer_id, units))
                } else {
                    None
                }
            })
            .collect_vec();

        for (worker_id, units) in jailed.iter() {
            for unit_id in units {
                self.units_assignments
                    .get_mut(unit_id)
                    .expect("unknown unit")
                    .retain(|id| id != worker_id)
            }
        }

        let num_jailed_workers = jailed.len();
        let num_unassigned_units: usize = jailed.iter().map(|(_, u)| u.len()).sum();
        log::info!("Jailed {num_jailed_workers} workers. Unassigned {num_unassigned_units} units");
        if num_unassigned_units > 0 {
            self.assign_units();
        }
        prometheus_metrics::exec_time("jail_workers", start.elapsed());
        num_jailed_workers > 0
    }

    fn mix_random_units(&self) {
        log::info!("Mixing random units");
        let start = Instant::now();

        // Group units by dataset and unassign random fraction of units for each dataset
        let grouped_units = self
            .known_units
            .iter()
            .filter(|u| self.num_replicas(u.key()) > 0)
            .map(|u| u.clone())
            .into_group_map_by(|u| u.dataset_url().to_owned());

        for (dataset_url, mut dataset_units) in grouped_units {
            // Sort units from oldest to newest and give them weights making
            // the most recent units more likely to be re-assigned
            dataset_units.sort_by_cached_key(|u| u.begin());
            let num_units = dataset_units.len();
            let num_mixed = ((num_units as f64) * Config::get().mixed_units_ratio) as usize;
            let max_weight = Config::get().mixing_recent_unit_weight;
            let weights = lin_space(1.0..=max_weight, num_units).collect_vec();
            let mixed_units =
                random_choice().random_choice_f64(&dataset_units, &weights, num_mixed);
            log::info!("Mixing {num_mixed} out of {num_units} units for dataset {dataset_url}");

            // For each of the randomly selected units, remove one random replica
            for unit in mixed_units {
                self.remove_random_replica(unit.id())
            }
        }
        prometheus_metrics::exec_time("mix_units", start.elapsed());
    }

    fn remove_random_replica(&self, unit_id: &UnitId) {
        let holder_id = {
            let mut holder_ids = self
                .units_assignments
                .get_mut(unit_id)
                .expect("cannot remove replica: no assignees");
            let random_idx = thread_rng().gen_range(0..holder_ids.len());
            holder_ids.remove(random_idx)
        };
        let unit_size = self
            .known_units
            .get(unit_id)
            .expect("Unknown unit")
            .size_bytes();
        self.worker_states
            .get_mut(&holder_id)
            .expect("Unknown worker")
            .remove_unit(unit_id, unit_size);
    }

    fn clear_redundant_replicas(&self, rep_factor: usize) {
        log::info!("Clearing redundant replicas");
        let units_to_clear = self
            .units_assignments
            .iter()
            .filter_map(|u| (u.len() > rep_factor).then_some((*u.key(), u.len() - rep_factor)))
            .collect_vec();

        for (unit_id, num_redundant) in units_to_clear {
            for _ in 0..num_redundant {
                self.remove_random_replica(&unit_id);
            }
        }
    }

    fn assign_units(&self) {
        log::info!("Assigning units");
        let start = Instant::now();

        // Only active and non-jailed workers are eligible for assignment
        let mut workers = self
            .worker_states
            .iter()
            .filter(|w| w.is_active() && !w.jailed)
            .collect_vec();

        // Randomly shuffle workers, then use a heap based on remaining capacity to make
        // the data distribution as uniform as possible
        workers.shuffle(&mut thread_rng());
        let mut workers: BinaryHeap<(u64, PeerId)> = workers
            .into_iter()
            .map(|w| (w.remaining_capacity(), w.peer_id))
            .collect();

        // Compute replication factor based on total available capacity
        let data_size = self.total_data_size();
        let rep_factor = match replication_factor(data_size, workers.len() as u64) {
            Some(rep_factor) => rep_factor,
            None => return,
        };

        self.clear_redundant_replicas(rep_factor);

        // Use a heap based on the number of missing replicas so that units are assigned
        // more evenly if there is not enough worker capacity for all
        let mut units: BinaryHeap<(usize, u64, UnitId)> = self
            .known_units
            .iter()
            .filter_map(|unit| {
                let missing_replicas = rep_factor
                    .checked_sub(self.num_replicas(unit.key()))
                    .expect("Redundant replicas");
                (missing_replicas > 0).then_some((missing_replicas, unit.size_bytes(), *unit.key()))
            })
            .collect();

        log::info!(
            "Workers available: {}  Units to assign: {} Replication factor: {rep_factor}",
            workers.len(),
            units.len()
        );
        prometheus_metrics::active_workers(workers.len());
        prometheus_metrics::total_units(self.known_units.len());
        prometheus_metrics::replication_factor(rep_factor);

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

        let incomplete_units = self
            .units_assignments
            .iter()
            .filter(|assignment| assignment.len() < rep_factor)
            .count();
        log::info!("Assignment complete. {incomplete_units} units are missing some replicas");

        prometheus_metrics::units_assigned(
            self.units_assignments
                .iter()
                .map(|assignment| assignment.len()),
        );
        prometheus_metrics::partially_assigned_units(incomplete_units);

        self.worker_states
            .iter_mut()
            .filter(|w| w.is_active() && !w.jailed)
            .for_each(|mut w| {
                w.reset_download_progress(&self.known_units);
                log::info!("{}", *w)
            });
        prometheus_metrics::exec_time("assign_units", start.elapsed());
    }

    pub fn get_chunks_summary(&self) -> HashMap<String, Vec<ChunkStatus>> {
        self.chunks_summary.read().clone()
    }

    pub fn update_chunks_summary(&self, summary: HashMap<String, Vec<ChunkStatus>>) {
        *self.chunks_summary.write() = summary;
    }
}

fn add_signature_headers(
    assignment: &mut sqd_messages::WorkerAssignment,
    worker_id: &PeerId,
    worker_state: &WorkerState,
) {
    assignment.http_headers.extend([
        HttpHeader {
            name: WORKER_ID_HEADER.to_string(),
            value: worker_id.to_string(),
        },
        HttpHeader {
            name: WORKER_SIGNATURE_HEADER.to_string(),
            value: worker_state
                .signature
                .clone()
                .expect("Worker signature not initialized"),
        },
    ]);
}

fn replication_factor(data_size_bytes: u64, num_workers: u64) -> Option<usize> {
    if data_size_bytes == 0 {
        return None;
    }
    let conf = Config::get();
    if !conf.dynamic_replication {
        return Some(conf.replication_factor);
    }
    // We aim to fill `dynamic_replication_factor` * total available capacity
    let target_capacity =
        (conf.worker_storage_bytes * num_workers) as f64 * conf.dyn_rep_capacity_share;
    // How many times would the whole data fit in the target capacity
    let rep_factor = (target_capacity / data_size_bytes as f64).floor() as usize;
    // Replication factor set in the config is treated as minimum
    Some(max(conf.replication_factor, rep_factor))
}
