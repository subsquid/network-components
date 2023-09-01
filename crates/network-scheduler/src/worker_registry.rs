use std::collections::{HashMap, HashSet};
use std::time::Duration;

use serde::{Serialize, Serializer};
use std::time::Instant;

use router_controller::messages::{Ping, RangeSet};
use subsquid_network_transport::PeerId;

pub const WORKER_INACTIVE_TIMEOUT: Duration = Duration::from_secs(60);
pub const SUPPORTED_WORKER_VERSIONS: [&str; 1] = ["0.1.1"];

#[derive(Debug, Clone, Serialize)]
pub struct Worker {
    #[serde(serialize_with = "serialize_peer_id")]
    pub peer_id: PeerId,
    #[serde(with = "serde_millis")]
    pub last_ping: Instant,
    pub stored_bytes: u64,
    pub version: String,
}

impl Worker {
    pub fn new(peer_id: PeerId, stored_bytes: u64, version: String) -> Self {
        Self {
            peer_id,
            last_ping: Instant::now(),
            stored_bytes,
            version,
        }
    }

    pub fn is_available(&self) -> bool {
        self.last_ping.elapsed() < WORKER_INACTIVE_TIMEOUT
            && SUPPORTED_WORKER_VERSIONS.iter().any(|v| *v == self.version)
    }
}

fn serialize_peer_id<S: Serializer>(peer_id: &PeerId, serializer: S) -> Result<S::Ok, S::Error> {
    if serializer.is_human_readable() {
        peer_id.to_string().serialize(serializer)
    } else {
        peer_id.to_bytes().serialize(serializer)
    }
}

pub struct WorkerRegistry {
    client: Box<dyn contract_client::Client>,
    registered_workers: HashSet<PeerId>,
    active_workers: HashMap<PeerId, Worker>,
    stored_ranges: HashMap<PeerId, HashMap<String, RangeSet>>,
}

impl WorkerRegistry {
    pub async fn init(rpc_url: &str) -> anyhow::Result<Self> {
        let client = contract_client::get_client(rpc_url).await?;
        let mut registry = Self {
            client,
            registered_workers: Default::default(),
            active_workers: Default::default(),
            stored_ranges: Default::default(),
        };
        // Need to get new workers immediately, otherwise they wouldn't be updated until the first
        // call to `active_workers`, so all pings would be discarded.
        registry.update_workers().await?;
        Ok(registry)
    }

    pub async fn ping(&mut self, worker_id: PeerId, msg: Ping) {
        log::debug!("Got ping from {worker_id}");
        if self.registered_workers.contains(&worker_id) {
            self.active_workers.insert(
                worker_id,
                Worker::new(worker_id, msg.stored_bytes, msg.version),
            );
            self.stored_ranges
                .insert(worker_id, msg.state.unwrap_or_default().datasets);
        }
    }

    async fn update_workers(&mut self) -> anyhow::Result<()> {
        let new_workers = self.client.active_workers().await?;
        self.registered_workers = new_workers.into_iter().map(|w| w.peer_id).collect();
        log::info!(
            "Registered workers set updated: {:?}",
            self.registered_workers
        );
        self.active_workers
            .retain(|id, _| self.registered_workers.contains(id));
        self.stored_ranges
            .retain(|id, _| self.registered_workers.contains(id));
        Ok(())
    }

    /// Get workers which meet all the following conditions:
    ///   a) registered on-chain,
    ///   b) running supported version,
    ///   c) sent ping withing the last `WORKER_INACTIVE_TIMEOUT` period.
    pub async fn available_workers(&mut self) -> Vec<Worker> {
        if let Err(e) = self.update_workers().await {
            log::error!("Error updating worker set: {e:?}")
        }
        self.active_workers
            .iter()
            .filter_map(|(_, w)| w.is_available().then(|| w.clone()))
            .collect()
    }

    /// Get workers which meet the following conditions:
    ///   a) registered on-chain,
    ///   b) sent at least one ping.
    pub async fn active_workers(&mut self) -> Vec<Worker> {
        if let Err(e) = self.update_workers().await {
            log::error!("Error updating worker set: {e:?}")
        }
        self.active_workers.values().cloned().collect()
    }

    pub fn stored_ranges(&self, worker_id: &PeerId) -> HashMap<String, RangeSet> {
        self.stored_ranges
            .get(worker_id)
            .cloned()
            .unwrap_or_default()
    }
}
