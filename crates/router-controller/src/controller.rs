use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use rand::prelude::SliceRandom;
use std::cmp::{max, min};
use std::collections::{HashMap, HashSet};
use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use crate::atom::Atom;
use crate::data_chunk::DataChunk;
use crate::messages::{Ping, WorkerState};
use crate::range::{Range, RangeSet};

pub type WorkerId = String;
pub type Url = String;
pub type Dataset = Url;

#[derive(Clone)]
struct Worker {
    desired_state: Arc<WorkerState>,
    info: Arc<Atom<WorkerInfo>>,
    is_managed: bool,
}

struct WorkerInfo {
    id: WorkerId,
    url: Url,
    state: WorkerState,
    suspended: bool,
    last_ping: SystemTime,
}

type Wi = usize;
type Ui = usize;
type Assignment = Vec<HashSet<Ui>>;

struct Schedule {
    datasets: HashMap<Dataset, Vec<DataChunk>>,
    assignment: HashMap<Dataset, Assignment>,
}

pub struct Controller {
    schedule: parking_lot::Mutex<Schedule>,
    workers: Atom<Vec<Worker>>,
    managed_datasets: HashMap<String, Dataset>,
    managed_workers: HashSet<WorkerId>,
    data_replication: usize,
    data_management_unit: usize,
}

unsafe impl Send for Controller {}
unsafe impl Sync for Controller {}

impl Controller {
    pub fn get_worker(&self, dataset_name: &str, first_block: u32) -> Option<(WorkerId, Url)> {
        let dataset = match self.managed_datasets.get(dataset_name) {
            Some(ds) => ds,
            None => return None,
        };

        let now = SystemTime::now();

        let select_candidate = |w: &Worker| {
            if !w
                .desired_state
                .get(dataset)
                .map_or(false, |ranges| ranges.has(first_block))
            {
                return None;
            }
            let info = w.info.get();
            if info.suspended {
                return None;
            }
            if now.duration_since(info.last_ping).unwrap() > Duration::from_secs(30) {
                return None;
            }
            if info
                .state
                .get(dataset)
                .map_or(false, |ranges| ranges.has(first_block))
            {
                Some(info)
            } else {
                None
            }
        };

        let workers = self.workers.get();

        let candidates = {
            let managed: Vec<_> = workers
                .iter()
                .filter(|w| w.is_managed)
                .filter_map(select_candidate)
                .collect();
            if managed.len() > 0 {
                managed
            } else {
                workers
                    .iter()
                    .filter(|w| !w.is_managed)
                    .filter_map(select_candidate)
                    .collect()
            }
        };

        candidates
            .choose(&mut rand::thread_rng())
            .map(|info| (info.id.clone(), Self::format_worker_url(&info.url, dataset)))
    }

    fn format_worker_url(base: &Url, dataset: &Dataset) -> String {
        format!("{}/{}", base, URL_SAFE_NO_PAD.encode(dataset))
    }

    pub fn ping(&self, msg: Ping) -> Arc<WorkerState> {
        let info = Arc::new(WorkerInfo {
            id: msg.worker_id.clone(),
            url: msg.worker_url,
            state: msg.state.unwrap_or_default(),
            suspended: msg.pause,
            last_ping: SystemTime::now(),
        });

        let mut desired_state: Option<Arc<WorkerState>> = None;

        self.workers.update(|workers| {
            if let Some(w) = workers.iter().find(|w| w.info.get().id == msg.worker_id) {
                w.info.set(info.clone());
                desired_state = Some(w.desired_state.clone());
                None
            } else {
                let new_worker = Worker {
                    desired_state: Arc::new(info.state.clone()),
                    info: Arc::new(Atom::new(info.clone())),
                    is_managed: self.managed_workers.contains(&msg.worker_id),
                };
                desired_state = Some(new_worker.desired_state.clone());
                Some(Arc::new(
                    workers
                        .iter()
                        .cloned()
                        .chain(std::iter::once(new_worker))
                        .collect(),
                ))
            }
        });

        desired_state.unwrap()
    }

    pub fn schedule<F>(&self, mut f: F)
    where
        F: FnMut(&Dataset, u32) -> Result<Vec<DataChunk>, ()>,
    {
        let mut schedule_lock = self.schedule.lock();
        let schedule = schedule_lock.deref_mut();
        let mut workers = self.workers.get().deref().clone();

        Self::remove_dead_workers(&mut workers);

        let managed_workers: Vec<_> = workers.iter().filter(|w| w.is_managed).cloned().collect();
        if managed_workers.len() < self.managed_workers.len() {
            self.workers.set(Arc::new(workers));
            return;
        }

        let mut desired_state: Vec<WorkerState> = std::iter::repeat_with(Default::default)
            .take(managed_workers.len())
            .collect();

        for (dataset, chunks) in schedule.datasets.iter_mut() {
            if Self::import_new_chunks(chunks, |next_block| f(dataset, next_block)) {
                let plan = self.schedule_dataset(
                    &managed_workers,
                    &mut schedule.assignment,
                    dataset,
                    chunks,
                );
                for (w, ranges) in plan.into_iter().enumerate() {
                    desired_state[w].insert(dataset.clone(), ranges);
                }
            }
        }

        for worker in workers.iter_mut().filter(|w| w.is_managed) {
            let i = managed_workers
                .iter()
                .position(|w| w.info.get().id == worker.info.get().id)
                .unwrap();
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

    fn import_new_chunks<F>(chunks: &mut Vec<DataChunk>, f: F) -> bool
    where
        F: FnOnce(u32) -> Result<Vec<DataChunk>, ()>,
    {
        let mut next_block = chunks.last().map_or(0, |c| c.last_block() + 1);
        match f(next_block) {
            Err(_) => false,
            Ok(mut new_chunks) => {
                new_chunks.sort();
                // check, that the new chunks are non-overlapping and span a continuous range
                for (i, c) in new_chunks.iter().enumerate() {
                    if next_block != c.first_block() {
                        let p = if i > 0 {
                            &new_chunks[i - 1]
                        } else {
                            chunks.last().unwrap()
                        };
                        if next_block > c.first_block() {
                            panic!("Received overlapping chunks: {} and {}", p, c)
                        } else {
                            panic!("There is a gap between {} and {}", p, c)
                        }
                    }
                    next_block = c.last_block() + 1
                }
                chunks.append(&mut new_chunks);
                true
            }
        }
    }

    fn schedule_dataset(
        &self,
        workers: &[Worker],
        assignment_map: &mut HashMap<Dataset, Assignment>,
        dataset: &Dataset,
        chunks: &Vec<DataChunk>,
    ) -> Vec<RangeSet> {
        let units: Vec<Range> = chunks
            .chunks(self.data_management_unit)
            .map(|unit| {
                Range::new(
                    unit.first().unwrap().first_block(),
                    unit.last().unwrap().last_block(),
                )
            })
            .collect();

        let no_state = RangeSet::empty();
        let infos: Vec<_> = workers.iter().map(|w| w.info.get()).collect();

        let actual: Assignment = infos
            .iter()
            .map(|info| {
                let s = info.state.get(dataset).unwrap_or(&no_state);
                units
                    .iter()
                    .enumerate()
                    .filter_map(|(i, &u)| if s.has(u.begin) { Some(i) } else { None })
                    .collect()
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
                let lst = goal.len() - 1; // FIXME: This sometimes panics (underflow)
                let target_size = goal.iter().map(|a| a.len()).sum::<usize>() / goal.len();
                loop {
                    order.sort_by_key(|i| goal[*i].len());
                    let s = order[0];
                    let l = order[lst];
                    let l_size = goal[l].len();
                    let s_size = goal[s].len();
                    if l_size - s_size < 2 {
                        break;
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
            .map(|w| actual[w].intersection(&plan[w]).cloned().collect())
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

        plan.iter()
            .map(|a| {
                let ranges: Vec<Range> = a.iter().map(|&u| units[u].clone()).collect();
                RangeSet::from(ranges)
            })
            .collect()
    }

    fn get_holders<'a>(assignment: &'a Assignment, u: &'a Ui) -> impl Iterator<Item = Wi> + 'a {
        (0..assignment.len()).filter(|&w| assignment[w].contains(u))
    }

    fn assign(goal: &mut Assignment, replicas: usize, u: Ui) {
        for w in Self::select_randomly(replicas, (0..goal.len()).filter(|&w| !goal[w].contains(&u)))
        {
            goal[w].insert(u);
        }
    }

    fn select_randomly<T, I: IntoIterator<Item = T>>(count: usize, candidates: I) -> Vec<T> {
        if count == 0 {
            return Vec::new();
        }

        let mut vec: Vec<T> = candidates.into_iter().collect();
        if count >= vec.len() {
            return vec;
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
    managed_datasets: HashMap<String, Dataset>,
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
            data_management_unit: 50,
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
    where
        I: IntoIterator<Item = WorkerId>,
    {
        self.managed_workers.clear();
        self.managed_workers.extend(workers);
        self
    }

    pub fn add_dataset(&mut self, name: String, dataset: Dataset) -> &mut Self {
        self.managed_datasets.insert(name, dataset);
        self
    }

    pub fn set_datasets<I>(&mut self, datasets: I) -> &mut Self
    where
        I: IntoIterator<Item = (String, Dataset)>,
    {
        self.managed_datasets.clear();
        self.managed_datasets.extend(datasets);
        self
    }

    pub fn build(&self) -> Controller {
        Controller {
            schedule: parking_lot::Mutex::new(Schedule {
                datasets: self
                    .managed_datasets
                    .iter()
                    .map(|(_name, ds)| (ds.clone(), Vec::new()))
                    .collect(),
                assignment: HashMap::new(),
            }),
            workers: Atom::new(Arc::new(Vec::new())),
            managed_datasets: self.managed_datasets.clone(),
            managed_workers: self.managed_workers.clone(),
            data_replication: self.replication,
            data_management_unit: self.data_management_unit,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::ops::Deref;

    use crate::controller::{ControllerBuilder, Ping};
    use crate::data_chunk::DataChunk;

    #[test]
    fn basic() {
        let controller = ControllerBuilder::new()
            .set_data_management_unit(1)
            .set_data_replication(2)
            .set_workers((0..8).map(|i| i.to_string()))
            .set_datasets((0..2).map(|i| (i.to_string(), i.to_string())))
            .build();

        let chunks = vec![
            vec![DataChunk::new(0, 0, 10), DataChunk::new(0, 11, 200)],
            vec![DataChunk::new(0, 0, 5), DataChunk::new(6, 6, 20)],
        ];

        for w in 0..8 {
            controller.ping(Ping {
                worker_id: w.to_string(),
                worker_url: w.to_string(),
                state: Some(Default::default()),
                pause: false,
            });
        }

        controller.schedule(|ds, _from_block| Ok(chunks[ds.parse::<usize>().unwrap()].clone()));

        let desired_state: Vec<_> = (0..8)
            .map(|w| {
                controller.ping(Ping {
                    worker_id: w.to_string(),
                    worker_url: w.to_string(),
                    state: Some(Default::default()),
                    pause: false,
                })
            })
            .collect();

        assert_eq!(controller.get_worker("0", 5), None);

        for (w, state) in desired_state.iter().enumerate() {
            controller.ping(Ping {
                worker_id: w.to_string(),
                worker_url: w.to_string(),
                state: Some(state.deref().clone()),
                pause: false,
            });
        }

        let holders: Vec<_> = desired_state
            .iter()
            .enumerate()
            .filter_map(|(wi, s)| {
                s.get("0")
                    .map(|range| {
                        if range.has(10) && range.has(0) {
                            Some((wi.to_string(), format!("{}/MA", wi)))
                        } else {
                            None
                        }
                    })
                    .unwrap_or(None)
            })
            .collect();

        assert_eq!(holders.len(), 2);
        assert!(holders.contains(&controller.get_worker("0", 0).unwrap()));
        assert!(holders.contains(&controller.get_worker("0", 5).unwrap()));
        assert!(holders.contains(&controller.get_worker("0", 10).unwrap()));
    }
}
