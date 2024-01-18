use std::cmp::max;
use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

use rand::prelude::IteratorRandom;
use tabled::Tabled;

use crate::config::DatasetId;
use contract_client::Worker;
use subsquid_messages::RangeSet;
use subsquid_network_transport::PeerId;

#[derive(Default)]
struct DatasetState {
    worker_ranges: HashMap<PeerId, RangeSet>,
    height: u32,
}

impl DatasetState {
    pub fn get_workers_with_block(&self, block: u32) -> impl Iterator<Item = PeerId> + '_ {
        self.worker_ranges
            .iter()
            .filter_map(move |(peer_id, range_set)| range_set.has(block).then_some(*peer_id))
    }

    pub fn update(&mut self, peer_id: PeerId, state: RangeSet) {
        if let Some(range) = state.ranges.last() {
            self.height = max(self.height, range.end)
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
                highest_seen_block: state.height,
            },
        }
    }
}

#[derive(Default)]
pub struct NetworkState {
    dataset_states: HashMap<DatasetId, DatasetState>,
    last_pings: HashMap<PeerId, Instant>,
    worker_greylist: HashMap<PeerId, Instant>,
    registered_workers: HashSet<PeerId>,
    available_datasets: HashMap<String, DatasetId>,
    worker_inactive_threshold: Duration,
    worker_greylist_time: Duration,
}

impl NetworkState {
    pub fn new(
        available_datasets: HashMap<String, DatasetId>,
        worker_inactive_threshold: Duration,
        worker_greylist_time: Duration,
    ) -> Self {
        Self {
            available_datasets,
            worker_inactive_threshold,
            worker_greylist_time,
            ..Default::default()
        }
    }

    pub fn get_dataset_id(&self, dataset: &str) -> Option<DatasetId> {
        self.available_datasets.get(dataset).cloned()
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
            .filter(|peer_id| self.worker_is_active(peer_id, false))
            .choose(&mut rand::thread_rng());

        // If no worker is found, try grey-listed workers
        if worker.is_none() {
            worker = dataset_state
                .get_workers_with_block(start_block)
                .filter(|peer_id| self.worker_is_active(peer_id, true))
                .choose(&mut rand::thread_rng());
        }

        worker
    }

    fn worker_is_active(&self, worker_id: &PeerId, allow_greylisted: bool) -> bool {
        // Check if worker is registered on chain
        if !self.registered_workers.contains(worker_id) {
            return false;
        }

        let now = Instant::now();

        // Check if the last ping wasn't too long ago
        match self.last_pings.get(worker_id) {
            None => return false,
            Some(ping) if (*ping + self.worker_inactive_threshold) < now => return false,
            _ => (),
        };

        if allow_greylisted {
            return true;
        }

        // Check if the worker is (still) grey-listed
        !matches!(
            self.worker_greylist.get(worker_id),
            Some(instant) if (*instant + self.worker_greylist_time) > now
        )
    }

    pub fn update_dataset_states(
        &mut self,
        worker_id: PeerId,
        mut worker_state: HashMap<DatasetId, RangeSet>,
    ) {
        self.last_pings.insert(worker_id, Instant::now());
        for dataset_id in self.available_datasets.values() {
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
    }

    pub fn greylist_worker(&mut self, worker_id: PeerId) {
        log::info!("Grey-listing worker {worker_id}");
        self.worker_greylist.insert(worker_id, Instant::now());
    }

    pub fn get_height(&self, dataset_id: &DatasetId) -> Option<u32> {
        self.dataset_states
            .get(dataset_id)
            .map(|state| state.height)
    }

    pub fn summary(&self) -> impl Iterator<Item = DatasetSummary> {
        self.available_datasets
            .iter()
            .map(|(name, id)| DatasetSummary::new(name, self.dataset_states.get(id)))
    }
}
