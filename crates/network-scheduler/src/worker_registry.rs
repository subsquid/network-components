use std::collections::{HashMap, HashSet};
use std::time::Duration;

use subsquid_network_transport::PeerId;
use tokio::time::Instant;

const WORKER_INACTIVE_TIMEOUT: Duration = Duration::from_secs(60);

#[derive(Default)]
pub struct WorkerRegistry {
    workers: HashSet<PeerId>,
    pings: HashMap<PeerId, Instant>,
}

impl WorkerRegistry {
    pub async fn ping(&mut self, worker_id: PeerId) {
        log::debug!("Got ping from {worker_id}");
        if self.workers.contains(&worker_id) {
            self.pings.insert(worker_id, Instant::now());
        }
    }

    pub async fn update_workers(&mut self, new_workers: impl IntoIterator<Item = PeerId>) {
        self.workers = new_workers.into_iter().collect();
        log::info!("Registered workers set updated: {:?}", self.workers);
        self.pings.retain(|id, _| self.workers.contains(id));
    }

    pub async fn active_workers(&self) -> Vec<PeerId> {
        self.workers
            .iter()
            .filter_map(|id| match self.pings.get(id) {
                Some(last_ping) if last_ping.elapsed() < WORKER_INACTIVE_TIMEOUT => Some(*id),
                _ => None,
            })
            .collect()
    }
}
