use std::cmp::{max, min};
use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::{Duration, SystemTime};
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;

use serde::Deserialize;

use crate::atom::Atom;
use crate::data_chunk::DataChunk;
use crate::range::{Range, RangeSet};
use crate::request_cache::RequestCache;
use crate::workers_rate::WorkersRate;

pub type WorkerId = String;
pub type Url = String;
pub type Dataset = Url;
pub type WorkerState = HashMap<Dataset, RangeSet>;


const INITIAL_VALUE: i64 = -2;
const EMPTY_VALUE: i64 = -1;


#[derive(Clone)]
struct Worker {
    id: WorkerId,
    desired_state: Arc<WorkerState>,
    info: Arc<Atom<WorkerInfo>>,
    is_managed: bool
}


struct WorkerInfo {
    url: Url,
    state: WorkerState,
    suspended: bool,
    last_ping: SystemTime
}


#[derive(Deserialize, Debug)]
pub struct PingMessage {
    pub worker_id: WorkerId,
    pub worker_url: Url,
    pub state: WorkerState,
    #[serde(default)]
    pub pause: bool
}


type Wi = usize;
type Ui = usize;
type Assignment = Vec<HashSet<Ui>>;


struct Schedule {
    datasets: HashMap<Dataset, Vec<DataChunk>>,
    assignment: HashMap<Dataset, Assignment>
}


struct DatasetHeight {
    confirmed: AtomicI64,
    pending: AtomicI64,
}


pub struct Controller {
    schedule: parking_lot::Mutex<Schedule>,
    workers: Atom<Vec<Worker>>,
    workers_rate: WorkersRate,
    request_cache: RequestCache,
    datasets_height: HashMap<Dataset, DatasetHeight>,
    managed_datasets: HashMap<String, (Dataset, Option<u32>)>,
    managed_workers: HashSet<WorkerId>,
    data_replication: usize,
    data_management_unit: usize
}


unsafe impl Send for Controller {}
unsafe impl Sync for Controller {}


impl Controller {
    #[tracing::instrument(level="debug", skip(self))]
    pub fn get_worker(&self, dataset_name: &str, first_block: u32) -> Result<Option<Url>, String> {
        let (dataset, start_block) = match self.managed_datasets.get(dataset_name) {
            Some(ds) => ds,
            None => {
                return Err("unknown dataset".to_string())
            }
        };

        if let Some(start_block) = start_block {
            if first_block < *start_block {
                return Err(format!("{} dataset starts from {} block", dataset_name, start_block))
            }
        }

        let now = SystemTime::now();

        let select_candidate = |w: &Worker| {
            if !w.desired_state.get(dataset).map_or(false, |ranges| ranges.has(first_block)) {
                return None
            }
            let info = w.info.get();
            if info.suspended {
                tracing::debug!(worker=w.id, "Worker is suspended");
                return None
            }
            if let Ok(duration) = now.duration_since(info.last_ping) {
                if duration > Duration::from_secs(30) {
                    tracing::debug!(worker=w.id, "Worker is inactive");
                    return None
                }
            }
            if info.state.get(dataset).map_or(false, |ranges| ranges.has(first_block)) {
                Some(info)
            } else {
                tracing::debug!(worker=w.id, "Worker hasn't downloaded the chunk yet");
                None
            }
        };

        let workers = self.workers.get();

        let candidates = {
            let managed: Vec<_> = workers.iter()
                .filter(|w| w.is_managed)
                .filter_map(select_candidate)
                .collect();
            if !managed.is_empty() {
                managed
            } else {
                workers.iter()
                    .filter(|w| !w.is_managed)
                    .filter_map(select_candidate)
                    .collect()
            }
        };

        let worker = match candidates.len() {
            0 => None,
            1 => {
                let worker = &candidates[0];
                self.workers_rate.inc(&worker.url, now);
                Some(&worker.url)
            },
            len => {
                let key = (dataset.clone(), first_block);

                let next_worker = if let Some(url) = self.request_cache.get(&key) {
                    let index = candidates.iter().position(|info| info.url == url);
                    if let Some(index) = index {
                        let worker = &candidates[(index + 1) % len];
                        Some(worker)
                    } else {
                        None
                    }
                } else {
                    None
                };

                let worker = if let Some(worker) = next_worker {
                    worker
                } else {
                    let least_used_worker = candidates
                        .iter()
                        .min_by_key(|info| self.workers_rate.get_rate(&info.url, now));
                    least_used_worker.unwrap()

                };

                self.request_cache.insert(key, worker.url.clone());
                self.workers_rate.inc(&worker.url, now);

                Some(&worker.url)
            }
        }.map(|url| Self::format_worker_url(url, dataset));

        Ok(worker)
    }

    pub fn get_height(&self, dataset_name: &str) -> Result<Option<i32>, String> {
        let (dataset, _) = match self.managed_datasets.get(dataset_name) {
            Some(ds) => ds,
            None => return Err("unknown dataset".to_string())
        };

        match self.datasets_height
            .get(dataset)
            .map(|height| height.pending.load(Ordering::Relaxed)) {
                Some(INITIAL_VALUE) | None => Ok(None),
                Some(value) => Ok(value.try_into().ok()),
            }
    }

    pub fn get_confirmed_height(&self, dataset_name: &str) -> Result<Option<i32>, String> {
        let (dataset, _) = match self.managed_datasets.get(dataset_name) {
            Some(ds) => ds,
            None => return Err("unknown dataset".to_string())
        };

        match self.datasets_height.get(dataset) {
            Some(height) => {
                let confirmed = height.confirmed.load(Ordering::Relaxed);
                let pending = height.pending.load(Ordering::Relaxed);

                if confirmed == INITIAL_VALUE {
                    return Ok(None)
                }

                if confirmed == pending {
                    return Ok(confirmed.try_into().ok())
                }

                let block_num = pending.try_into().unwrap();
                let worker = self.get_worker(dataset_name, block_num)?;
                if worker.is_some() {
                    height.confirmed.store(pending, Ordering::Relaxed);
                    Ok(pending.try_into().ok())
                } else {
                    Ok(confirmed.try_into().ok())
                }
            }
            None => Ok(None)
        }
    }

    fn format_worker_url(base: &Url, dataset: &Dataset) -> String {
        format!("{}/{}", base, URL_SAFE_NO_PAD.encode(dataset))
    }

    pub fn ping(&self, msg: PingMessage) -> Arc<WorkerState> {
        let info = Arc::new(WorkerInfo {
            url: msg.worker_url,
            state: msg.state,
            suspended: msg.pause,
            last_ping: SystemTime::now()
        });

        let mut desired_state: Option<Arc<WorkerState>> = None;

        self.workers.update(|workers| {
            if let Some(w) = workers.iter().find(|w| w.id == msg.worker_id) {
                w.info.set(info.clone());
                desired_state = Some(w.desired_state.clone());
                None
            } else {
                let new_worker = Worker {
                    id: msg.worker_id.clone(),
                    desired_state: Arc::new(info.state.clone()),
                    info: Arc::new(Atom::new(info.clone())),
                    is_managed: self.managed_workers.contains(&msg.worker_id)
                };
                desired_state = Some(new_worker.desired_state.clone());
                self.workers_rate.clear();
                Some(Arc::new(
                    workers.iter().cloned().chain(std::iter::once(new_worker)).collect()
                ))
            }
        });

        desired_state.unwrap()
    }

    pub fn update_dataset(&self, dataset: &Dataset, mut new_chunks: Vec<DataChunk>) -> Result<(), &str> {
        let mut schedule = self.schedule.lock();
        let chunks = match schedule.datasets.get_mut(dataset) {
            Some(chunks) => chunks,
            None => return Err("unknown dataset")
        };

        if Self::import_new_chunks(chunks, &mut new_chunks) {
            let height = self.datasets_height.get(dataset).unwrap();
            if let Some(chunk) = chunks.last() {
                let last_block = chunk.last_block();
                let value = height.confirmed.load(Ordering::Relaxed);
                if value == INITIAL_VALUE {
                    height.confirmed.store(last_block.into(), Ordering::Relaxed);
                    height.pending.store(last_block.into(), Ordering::Relaxed);
                } else {
                    height.pending.store(last_block.into(), Ordering::Relaxed);
                }
            } else {
                let value = height.confirmed.load(Ordering::Relaxed);
                if value == INITIAL_VALUE {
                    height.confirmed.store(EMPTY_VALUE, Ordering::Relaxed);
                    height.pending.store(EMPTY_VALUE, Ordering::Relaxed);
                }
            }
        }

        Ok(())
    }

    pub fn schedule(&self) {
        let mut schedule_lock = self.schedule.lock();
        let schedule = schedule_lock.deref_mut();
        let mut workers = self.workers.get().deref().clone();

        Self::remove_dead_workers(&mut workers);

        let managed_workers: Vec<_> = workers.iter().filter(|w| w.is_managed).cloned().collect();
        if managed_workers.len() < self.managed_workers.len() {
            log::warn!(
                "{} out of {} workers available. Skipping scheduling",
                managed_workers.len(),
                self.managed_workers.len()
            );
            self.workers.set(Arc::new(workers));
            return;
        }

        let mut desired_state: Vec<WorkerState> = std::iter::repeat_with(HashMap::new)
            .take(managed_workers.len())
            .collect();

        for (dataset, chunks) in schedule.datasets.iter_mut() {
            let plan = self.schedule_dataset(
                &managed_workers,
                &mut schedule.assignment,
                dataset,
                chunks
            );
            for (w, ranges) in plan.into_iter().enumerate() {
                desired_state[w].insert(dataset.clone(), ranges);
            }
        }

        for worker in workers.iter_mut().filter(|w| w.is_managed) {
            let i = managed_workers.iter().position(|w| w.id == worker.id).unwrap();
            worker.desired_state = Arc::new(desired_state[i].clone());
        }

        self.workers.set(Arc::new(workers));
    }

    fn remove_dead_workers(workers: &mut Vec<Worker>) {
        let now = SystemTime::now();
        workers.retain(|w| {
            w.is_managed || {
                let since_last_ping = now
                    .duration_since(w.info.get().last_ping)
                    .unwrap_or(Duration::from_secs(0));
                since_last_ping < Duration::from_secs(5 * 60)
            }
        })
    }

    fn import_new_chunks(chunks: &mut Vec<DataChunk>, new_chunks: &mut Vec<DataChunk>) -> bool {
        let mut next_block = chunks.last().map_or(0, |c| c.last_block() + 1);
        new_chunks.sort();
        // check, that the new chunks are non-overlapping and span a continuous range
        for (i, c) in new_chunks.iter().enumerate() {
            if next_block != c.first_block() && next_block != 0 {
                let p = if i > 0 {
                    &new_chunks[i - 1]
                } else {
                    chunks.last().unwrap()
                };
                if next_block > c.first_block() {
                    log::error!("Received overlapping chunks: {} and {}", p, c);
                    return false
                } else {
                    log::error!("There is a gap between {} and {}", p, c);
                    return false
                }
            }
            next_block = c.last_block() + 1
        }
        chunks.append(new_chunks);
        true
    }

    fn schedule_dataset(
        &self,
        workers: &[Worker],
        assignment_map: &mut HashMap<Dataset, Assignment>,
        dataset: &Dataset,
        chunks: &Vec<DataChunk>
    ) -> Vec<RangeSet> {
        let units: Vec<Range> = chunks.chunks(self.data_management_unit).map(|unit| {
            Range::new(
                unit.first().unwrap().first_block(),
                unit.last().unwrap().last_block()
            )
        }).collect();

        let no_state = RangeSet::empty();
        let infos: Vec<_> = workers.iter().map(|w| w.info.get()).collect();

        let actual: Assignment = infos.iter()
            .map(|info| {
                let s = info.state.get(dataset).unwrap_or(&no_state);
                units.iter().enumerate().filter_map(|(i, &u)| {
                    if s.has(u.begin()) {
                        Some(i)
                    } else {
                        None
                    }
                }).collect()
            })
            .collect();

        if let Some(goal) = assignment_map.get_mut(dataset) {
            for u in 0..units.len() {
                let n_holders = Self::get_holders(goal, &u).count();
                if n_holders < self.data_replication {
                    Self::assign(goal, self.data_replication - n_holders, u)
                }
            }
        } else {
            let mut goal = actual.clone();

            for u in 0..units.len() {
                let mut holders: Vec<_> = Self::get_holders(&goal, &u).collect();
                if holders.len() > self.data_replication {
                    holders.sort_by_key(|&w| goal[w].len());
                    for &w in holders.iter().skip(self.data_replication) {
                        goal[w].remove(&u);
                    }
                } else {
                    Self::assign(&mut goal, self.data_replication - holders.len(), u);
                }
            }

            {
                let mut order: Vec<Wi> = (0..goal.len()).collect();
                let lst = goal.len() - 1;  // TODO: fix "attempt to subtract with overflow"
                let target_size = goal.iter().map(|a| a.len()).sum::<usize>() / goal.len();
                loop {
                    order.sort_by_key(|i| goal[*i].len());
                    let s = order[0];
                    let l = order[lst];
                    let l_size = goal[l].len();
                    let s_size = goal[s].len();
                    if l_size - s_size < 2 {
                        break
                    }
                    let to_move = max(1, min(target_size - s_size, l_size - target_size));
                    for u in Self::select_randomly(to_move, goal[l].difference(&goal[s]).cloned()) {
                        goal[s].insert(u);
                        goal[l].remove(&u);
                    }
                }
            }

            assignment_map.insert(dataset.clone(), goal);
        }

        let mut plan = assignment_map.get(dataset).unwrap().clone();

        let actual_and_planned: Assignment = (0..workers.len())
            .map(|w| {
                actual[w].intersection(&plan[w]).cloned().collect()
            })
            .collect();

        for u in 0..units.len() {
            let n_holders = Self::get_holders(&actual_and_planned, &u).count();
            if n_holders < self.data_replication {
                for w in Self::get_holders(&actual, &u)
                    .filter(|&w| !actual_and_planned[w].contains(&u))
                    .take(self.data_replication - n_holders)
                {
                    plan[w].insert(u);
                }
            }
        }

        plan.iter().map(|a| {
            let ranges: Vec<Range> = a.iter().map(|&u| units[u].clone()).collect();
            RangeSet::from(ranges)
        }).collect()
    }

    fn get_holders<'a>(assignment: &'a Assignment, u: &'a Ui) -> impl Iterator<Item = Wi> + 'a {
        (0..assignment.len()).filter(|&w| assignment[w].contains(u))
    }

    fn assign(goal: &mut Assignment, replicas: usize, u: Ui) {
        for w in Self::select_randomly(
            replicas,
            (0..goal.len()).filter(|&w| !goal[w].contains(&u))
        ) {
            goal[w].insert(u);
        }
    }

    fn select_randomly<T, I: IntoIterator<Item = T>>(count: usize, candidates: I) -> Vec<T> {
        if count == 0 {
            return Vec::new()
        }

        let mut vec: Vec<T> = candidates.into_iter().collect();
        if count >= vec.len() {
            return vec
        }

        let mut offset = 0;
        let mut len = vec.len();
        while len > 0 && offset < count {
            let i = rand::random::<usize>() % len;
            vec.swap(offset, offset + i);
            offset += 1;
            len -= 1;
        }
        vec.truncate(count);
        vec
    }
}


pub struct ControllerBuilder {
    managed_datasets: HashMap<String, (Dataset, Option<u32>)>,
    managed_workers: HashSet<WorkerId>,
    replication: usize,
    data_management_unit: usize,
}


impl ControllerBuilder {
    pub fn new() -> Self {
        ControllerBuilder {
            managed_datasets: HashMap::new(),
            managed_workers: HashSet::new(),
            replication: 1,
            data_management_unit: 50
        }
    }

    pub fn set_data_replication(&mut self, n: usize) -> &mut Self {
        self.replication = n;
        self
    }

    pub fn set_data_management_unit(&mut self, n_chunks: usize) -> &mut Self {
        self.data_management_unit = n_chunks;
        self
    }

    pub fn add_worker(&mut self, worker_id: WorkerId) -> &mut Self {
        self.managed_workers.insert(worker_id);
        self
    }

    pub fn set_workers<I>(&mut self, workers: I) -> &mut Self
        where I: IntoIterator<Item = WorkerId>
    {
        self.managed_workers.clear();
        self.managed_workers.extend(workers);
        self
    }

    pub fn add_dataset(&mut self, name: String, dataset: (Dataset, Option<u32>)) -> &mut Self {
        self.managed_datasets.insert(name, dataset);
        self
    }

    pub fn set_datasets<I>(&mut self, datasets: I) -> &mut Self
        where I: IntoIterator<Item = (String, (Dataset, Option<u32>))>
    {
        self.managed_datasets.clear();
        self.managed_datasets.extend(datasets);
        self
    }

    pub fn build(&self) -> Controller {
        Controller {
            schedule: parking_lot::Mutex::new(Schedule {
                datasets: self.managed_datasets.iter()
                    .map(|(_name, (ds, _))| (ds.clone(), Vec::new()))
                    .collect(),
                assignment: HashMap::new()
            }),
            datasets_height: self.managed_datasets.values()
                .map(|(name, _)| {
                    let height = DatasetHeight {
                        confirmed: AtomicI64::new(INITIAL_VALUE),
                        pending: AtomicI64::new(INITIAL_VALUE),
                    };
                    (name.clone(), height)
                })
                .collect(),
            workers: Atom::new(Arc::new(Vec::new())),
            workers_rate: WorkersRate::new(),
            request_cache: RequestCache::new(),
            managed_datasets: self.managed_datasets.clone(),
            managed_workers: self.managed_workers.clone(),
            data_replication: self.replication,
            data_management_unit: self.data_management_unit
        }
    }
}


#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::ops::Deref;

    use crate::controller::{ControllerBuilder, PingMessage};
    use crate::data_chunk::DataChunk;

    #[test]
    fn basic() {
        let controller = ControllerBuilder::new()
            .set_data_management_unit(1)
            .set_data_replication(2)
            .set_workers((0..8).map(|i| i.to_string()))
            .set_datasets((0..2).map(|i| (i.to_string(), (i.to_string(), None))))
            .build();

        let chunks = vec![
            vec![
                DataChunk::new(0, 0, 10, "".to_string()),
                DataChunk::new(0, 11, 200, "".to_string())
            ],
            vec![
                DataChunk::new(0, 0, 5, "".to_string()),
                DataChunk::new(6, 6, 20, "".to_string())
            ]
        ];

        for w in 0..8 {
            controller.ping(PingMessage {
                worker_id: w.to_string(),
                worker_url: w.to_string(),
                state: HashMap::new(),
                pause: false
            });
        }

        controller.update_dataset(&"0".to_string(), chunks[0].clone()).unwrap();
        controller.update_dataset(&"1".to_string(), chunks[1].clone()).unwrap();
        controller.schedule();

        let desired_state: Vec<_> = (0..8).map(|w| {
            controller.ping(PingMessage {
                worker_id: w.to_string(),
                worker_url: w.to_string(),
                state: HashMap::new(),
                pause: false
            })
        }).collect();

        assert_eq!(controller.get_worker("0", 5).unwrap(), None);

        for (w, state) in desired_state.iter().enumerate() {
            controller.ping(PingMessage {
                worker_id: w.to_string(),
                worker_url: w.to_string(),
                state: state.deref().clone(),
                pause: false
            });
        }

        let holders: Vec<_> = desired_state.iter().enumerate().filter_map(|(wi, s)| {
            s.get("0").map(|range| if range.has(10) && range.has(0) {
                Some(format!("{}/MA", wi))
            } else {
                None
            }).unwrap_or(None)
        }).collect();

        assert_eq!(holders.len(), 2);
        assert!(holders.contains(&controller.get_worker("0", 0).unwrap().unwrap()));
        assert!(holders.contains(&controller.get_worker("0", 5).unwrap().unwrap()));
        assert!(holders.contains(&controller.get_worker("0", 10).unwrap().unwrap()));
    }
}
