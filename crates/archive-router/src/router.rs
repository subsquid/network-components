use crate::dataset::DataRange;
use crate::error::Error;
use crate::util::get_random_item;
use serde::{Deserialize, Serialize};
use std::time::{Duration, SystemTime};
use url::Url;
use uuid::Uuid;

type Dataset = String;

#[derive(Clone, Serialize, Deserialize)]
pub struct WorkerState {
    pub dataset: Dataset,
    pub ranges: Vec<DataRange>,
}

pub struct Worker {
    pub id: Uuid,
    pub url: Url,
    pub desired_state: WorkerState,
    pub current_state: WorkerState,
    pub last_ping: SystemTime,
}

/// Checks whether the given block is within a data range.
fn includes(data_range: &DataRange, block: i32) -> bool {
    data_range.from <= block && data_range.to >= block
}

fn state_includes(state: &WorkerState, block: i32) -> bool {
    state.ranges.iter().any(|range| includes(range, block))
}

pub struct ArchiveRouter {
    workers: Vec<Worker>,
    ranges: Vec<DataRange>,
    dataset: Dataset,
    replication: usize,
    min_workers: usize,
}

impl ArchiveRouter {
    pub fn new(dataset: Dataset, replication: usize, min_workers: usize) -> Self {
        ArchiveRouter {
            workers: vec![],
            ranges: vec![],
            dataset,
            replication,
            min_workers,
        }
    }

    pub fn ping(&mut self, worker_id: Uuid, worker_url: Url, state: WorkerState) -> &WorkerState {
        let now = SystemTime::now();
        let index = self.workers.iter().position(|w| w.id == worker_id);
        let worker = if let Some(index) = index {
            let worker = &mut self.workers[index];
            worker.url = worker_url;
            worker.current_state = state;
            worker.last_ping = now;
            worker
        } else {
            let worker = Worker {
                id: worker_id,
                url: worker_url,
                desired_state: state.clone(),
                current_state: state,
                last_ping: now,
            };
            self.workers.push(worker);
            self.workers.last().unwrap()
        };
        &worker.desired_state
    }

    pub fn get_worker(&self, start_block: i32) -> Result<&Url, Error> {
        if !includes(&self.get_dataset_range(), start_block) {
            return Err(Error::NoRequestedData);
        }

        let now = SystemTime::now();

        let eligible_workers = self
            .workers
            .iter()
            .filter(|w| {
                if w.current_state.dataset != self.dataset {
                    return false;
                }

                if now.duration_since(w.last_ping).unwrap() > Duration::from_secs(60) {
                    return false;
                }

                state_includes(&w.current_state, start_block)
                    && state_includes(&w.desired_state, start_block)
            })
            .collect::<Vec<&Worker>>();

        if eligible_workers.is_empty() {
            return Err(Error::NoSuitableWorker);
        }

        let worker = get_random_item(&eligible_workers).unwrap();
        Ok(&worker.url)
    }

    pub fn get_dataset_range(&self) -> DataRange {
        if self.ranges.is_empty() {
            return DataRange { from: -1, to: -1 };
        }

        DataRange {
            from: self.ranges.first().unwrap().from,
            to: self.ranges.last().unwrap().to,
        }
    }

    /// Distributes data ranges among available workers
    pub fn schedule(&mut self) {
        if self.workers.len() < self.min_workers {
            return;
        }

        let now = SystemTime::now();

        // remove dead workers
        self.workers
            .retain(|w| now.duration_since(w.last_ping).unwrap() < Duration::from_secs(10 * 60));

        // remove dead ranges from desired state
        for w in &mut self.workers {
            if w.desired_state.dataset == self.dataset {
                w.desired_state
                    .ranges
                    .retain(|i| self.ranges.iter().any(|r| r.from == i.from))
            } else {
                w.desired_state.dataset = self.dataset.clone();
                w.desired_state.ranges = vec![];
            }
        }

        for range in &self.ranges {
            let holders: Vec<usize> = self
                .workers
                .iter()
                .enumerate()
                .filter_map(|(i, w)| {
                    match w.desired_state.ranges.iter().any(|r| r.from == range.from) {
                        true => Some(i),
                        false => None,
                    }
                })
                .collect();

            // handle over allocation
            for i in holders.iter().skip(self.replication) {
                let holder = &mut self.workers[*i];
                holder.desired_state.ranges.retain(|r| r.from != range.from);
            }

            // last range can grow
            // if it is the case we need to update its boundary in the desired state
            for i in &holders {
                let holder = &mut self.workers[*i];
                for r in &mut holder.desired_state.ranges {
                    if r.from == range.from {
                        r.to = range.to;
                    } else {
                        break;
                    }
                }
            }

            if holders.len() < self.replication {
                let mut holders: Vec<Uuid> =
                    holders.into_iter().map(|h| self.workers[h].id).collect();

                // sort workers by amount of data they hold in ascending order
                self.workers.sort_by_key(|w| w.desired_state.ranges.len());

                // find a suitable worker
                for worker in &mut self.workers {
                    if !holders.iter().any(|&h| h == worker.id) {
                        worker.desired_state.ranges.push(range.clone());
                        holders.push(worker.id);
                        if holders.len() == self.replication {
                            break;
                        }
                    }
                }
            }
        }
    }

    pub fn update_ranges(&mut self, ranges: Vec<DataRange>) {
        self.ranges = ranges;
    }
}
