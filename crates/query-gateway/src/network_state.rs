use std::cmp::max;
use std::collections::{HashMap, HashSet};
use std::time::Instant;

use rand::prelude::IteratorRandom;
use tabled::Tabled;

use crate::config::{Config, DatasetId};
use contract_client::Worker;
use subsquid_messages::RangeSet;
use subsquid_network_transport::PeerId;

#[derive(Default)]
struct DatasetState {
    worker_ranges: HashMap<PeerId, RangeSet>,
    highest_seen_block: u32,
}

impl DatasetState {
    pub fn get_workers_with_block(&self, block: u32) -> impl Iterator<Item = PeerId> + '_ {
        self.worker_ranges
            .iter()
            .filter_map(move |(peer_id, range_set)| range_set.has(block).then_some(*peer_id))
    }

    pub fn update(&mut self, peer_id: PeerId, state: RangeSet) {
        if let Some(range) = state.ranges.last() {
            self.highest_seen_block = max(self.highest_seen_block, range.end)
        }
        self.worker_ranges.insert(peer_id, state);
    }

    pub fn highest_indexable_block(&self) -> u32 {
        let range_set: RangeSet = self
            .worker_ranges
            .values()
            .cloned()
            .flat_map(|r| r.ranges)
            .into();
        match range_set.ranges.first() {
            Some(range) if range.begin == 0 => range.end,
            _ => 0,
        }
    }
}

#[derive(Tabled)]
pub struct DatasetSummary<'a> {
    #[tabled(rename = "dataset")]
    name: &'a String,
    #[tabled(rename = "highest indexable block")]
    highest_indexable_block: u32,
    #[tabled(rename = "highest seen block")]
    highest_seen_block: u32,
}

impl<'a> DatasetSummary<'a> {
    fn new(name: &'a String, state: Option<&DatasetState>) -> Self {
        match state {
            None => Self {
                name,
                highest_indexable_block: 0,
                highest_seen_block: 0,
            },
            Some(state) => Self {
                name,
                highest_indexable_block: state.highest_indexable_block(),
                highest_seen_block: state.highest_seen_block,
            },
        }
    }
}

#[derive(Default)]
pub struct NetworkState {
    dataset_states: HashMap<DatasetId, DatasetState>,
    last_pings: HashMap<PeerId, Instant>,
    worker_greylist: HashMap<PeerId, Instant>,
    workers_without_allocation: HashSet<PeerId>,
    registered_workers: HashSet<PeerId>,
    unreachable_workers: HashSet<PeerId>,
}

impl NetworkState {
    pub fn new(workers: Vec<Worker>) -> Self {
        let mut network_state = Self::default();
        network_state.update_registered_workers(workers);
        network_state
    }

    pub fn find_worker(&self, dataset_id: &DatasetId, start_block: u32) -> Option<PeerId> {
        log::debug!("Looking for worker dataset_id={dataset_id}, start_block={start_block}");
        let dataset_state = match self.dataset_states.get(dataset_id) {
            None => return None,
            Some(state) => state,
        };

        // Choose a random active worker having the requested start_block
        let mut worker = dataset_state
            .get_workers_with_block(start_block)
            .filter(|peer_id| self.worker_available(peer_id, false))
            .choose(&mut rand::thread_rng());

        // If no worker is found, try grey-listed workers
        if worker.is_none() {
            worker = dataset_state
                .get_workers_with_block(start_block)
                .filter(|peer_id| self.worker_available(peer_id, true))
                .choose(&mut rand::thread_rng());
        }

        worker
    }

    fn worker_available(&self, worker_id: &PeerId, allow_greylisted: bool) -> bool {
        self.registered_workers.contains(worker_id)
            && self.worker_has_allocation(worker_id)
            && self.worker_active(worker_id)
            && self.worker_reachable(worker_id)
            && (allow_greylisted || !self.worker_greylisted(worker_id))
    }

    fn worker_active(&self, worker_id: &PeerId) -> bool {
        let inactive_threshold = Config::get().worker_inactive_threshold;
        self.last_pings
            .get(worker_id)
            .is_some_and(|t| *t + inactive_threshold > Instant::now())
    }

    fn worker_greylisted(&self, worker_id: &PeerId) -> bool {
        let greylist_time = Config::get().worker_greylist_time;
        self.worker_greylist
            .get(worker_id)
            .is_some_and(|t| *t + greylist_time > Instant::now())
    }

    fn worker_reachable(&self, worker_id: &PeerId) -> bool {
        !self.unreachable_workers.contains(worker_id)
    }

    pub fn registered_workers(&self) -> Vec<PeerId> {
        self.registered_workers.iter().cloned().collect()
    }

    pub fn worker_dialed(&mut self, worker_id: PeerId, reachable: bool) {
        log::debug!("Worker {worker_id} dialed. reachable={reachable}");
        if reachable {
            self.unreachable_workers.remove(&worker_id);
        } else {
            self.unreachable_workers.insert(worker_id);
        }
    }

    pub fn reset_allocations_cache(&mut self) {
        self.workers_without_allocation.clear();
    }

    pub fn no_allocation_for_worker(&mut self, worker_id: PeerId) {
        self.workers_without_allocation.insert(worker_id);
    }

    pub fn worker_has_allocation(&self, worker_id: &PeerId) -> bool {
        !self.workers_without_allocation.contains(worker_id)
    }

    pub fn update_dataset_states(
        &mut self,
        worker_id: PeerId,
        mut worker_state: HashMap<DatasetId, RangeSet>,
    ) {
        self.last_pings.insert(worker_id, Instant::now());
        for dataset_id in Config::get().available_datasets.values() {
            let dataset_state = worker_state
                .remove(dataset_id)
                .unwrap_or_else(RangeSet::empty);
            self.dataset_states
                .entry(dataset_id.clone())
                .or_default()
                .update(worker_id, dataset_state);
        }
    }

    pub fn update_registered_workers(&mut self, workers: Vec<Worker>) {
        log::debug!("Updating registered workers: {workers:?}");
        self.registered_workers = workers.into_iter().map(|w| w.peer_id).collect();
        self.unreachable_workers = &self.unreachable_workers & &self.registered_workers;
    }

    pub fn greylist_worker(&mut self, worker_id: PeerId) {
        log::info!("Grey-listing worker {worker_id}");
        self.worker_greylist.insert(worker_id, Instant::now());
    }

    pub fn get_height(&self, dataset_id: &DatasetId) -> Option<u32> {
        self.dataset_states
            .get(dataset_id)
            .map(|state| state.highest_indexable_block())
    }

    pub fn summary(&self) -> impl Iterator<Item = DatasetSummary> {
        Config::get()
            .available_datasets
            .iter()
            .map(|(name, id)| DatasetSummary::new(name, self.dataset_states.get(id)))
    }
}
