use std::collections::{HashMap, HashSet};
use std::time::Duration;

use tokio::time::Instant;

use subsquid_network_transport::PeerId;

const WORKER_INACTIVE_TIMEOUT: Duration = Duration::from_secs(60);

pub struct WorkerRegistry {
    client: Box<dyn contract_client::Client>,
    workers: HashSet<PeerId>,
    pings: HashMap<PeerId, Instant>,
}

impl WorkerRegistry {
    pub async fn init(rpc_url: &str) -> anyhow::Result<Self> {
        let client = contract_client::get_client(rpc_url).await?;
        let mut registry = Self {
            client,
            workers: Default::default(),
            pings: Default::default(),
        };
        // Need to get new workers immediately, otherwise they wouldn't be updated until the first
        // call to `active_workers`, so all pings would be discarded.
        registry.update_workers().await?;
        Ok(registry)
    }

    pub async fn ping(&mut self, worker_id: PeerId) {
        log::debug!("Got ping from {worker_id}");
        if self.workers.contains(&worker_id) {
            self.pings.insert(worker_id, Instant::now());
        }
    }

    async fn update_workers(&mut self) -> anyhow::Result<()> {
        let new_workers = self.client.active_workers().await?;
        self.workers = new_workers.into_iter().map(|w| w.peer_id).collect();
        log::info!("Registered workers set updated: {:?}", self.workers);
        self.pings.retain(|id, _| self.workers.contains(id));
        Ok(())
    }

    pub async fn active_workers(&mut self) -> Vec<PeerId> {
        if let Err(e) = self.update_workers().await {
            log::error!("Error updating worker set: {e:?}")
        }
        self.workers
            .iter()
            .filter_map(|id| match self.pings.get(id) {
                Some(last_ping) if last_ping.elapsed() < WORKER_INACTIVE_TIMEOUT => Some(*id),
                _ => None,
            })
            .collect()
    }
}
