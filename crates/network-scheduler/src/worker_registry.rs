use std::collections::HashMap;
use std::time::Duration;
use std::time::Instant;

use serde::{Serialize, Serializer};

use contract_client::Address;
use router_controller::messages::{Ping, RangeSet};
use subsquid_network_transport::PeerId;

pub const SUPPORTED_WORKER_VERSIONS: [&str; 1] = ["0.1.4"];

fn worker_version_supported(ver: &str) -> bool {
    SUPPORTED_WORKER_VERSIONS.iter().any(|v| *v == ver)
}

#[derive(Debug, Clone, Serialize)]
pub struct Worker {
    #[serde(serialize_with = "serialize_peer_id")]
    pub peer_id: PeerId,
    pub address: Address,
    #[serde(with = "serde_millis")]
    pub last_ping: Instant,
    pub stored_bytes: u64,
    pub version: String,
}

impl Worker {
    pub fn new(peer_id: PeerId, address: Address, stored_bytes: u64, version: String) -> Self {
        Self {
            peer_id,
            address,
            last_ping: Instant::now(),
            stored_bytes,
            version,
        }
    }

    pub fn time_since_last_ping(&self) -> Duration {
        self.last_ping.elapsed()
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
    registered_workers: HashMap<PeerId, Address>,
    known_workers: HashMap<PeerId, Worker>,
    stored_ranges: HashMap<PeerId, HashMap<String, RangeSet>>,
    min_ping_interval: Duration,
    worker_inactive_timeout: Duration,
}

impl WorkerRegistry {
    pub async fn init(
        rpc_url: &str,
        min_ping_interval_sec: u64,
        worker_inactive_timeout_sec: u64,
    ) -> anyhow::Result<Self> {
        let client = contract_client::get_client(rpc_url).await?;
        let mut registry = Self {
            client,
            registered_workers: Default::default(),
            known_workers: Default::default(),
            stored_ranges: Default::default(),
            min_ping_interval: Duration::from_secs(min_ping_interval_sec),
            worker_inactive_timeout: Duration::from_secs(worker_inactive_timeout_sec),
        };
        // Need to get new workers immediately, otherwise they wouldn't be updated until the first
        // run of the scheduling, so all pings would be discarded.
        registry.update_workers().await;
        Ok(registry)
    }

    /// Register ping msg from a worker. Returns true if ping was accepted.
    pub async fn ping(&mut self, worker_id: PeerId, msg: Ping) -> bool {
        log::debug!("Got ping from {worker_id}");
        if !worker_version_supported(&msg.version) {
            log::debug!("Worker {worker_id} version not supported: {}", msg.version);
            return false;
        }

        let addr = match self.registered_workers.get(&worker_id) {
            Some(addr) => addr,
            None => return false,
        };

        if let Some(prev_state) = self.known_workers.get(&worker_id) {
            if Instant::now().duration_since(prev_state.last_ping) < self.min_ping_interval {
                log::warn!("Worker {worker_id} sending pings too often");
                return false;
            }
        }

        self.known_workers.insert(
            worker_id,
            Worker::new(worker_id, addr.clone(), msg.stored_bytes, msg.version),
        );
        self.stored_ranges
            .insert(worker_id, msg.state.unwrap_or_default().datasets);
        true
    }

    pub async fn update_workers(&mut self) {
        let new_workers = match self.client.active_workers().await {
            Ok(new_workers) => new_workers,
            Err(e) => return log::error!("Error updating worker set: {e:?}"),
        };
        self.registered_workers = new_workers
            .into_iter()
            .map(|w| (w.peer_id, w.address))
            .collect();
        log::debug!(
            "Registered workers set updated: {:?}",
            self.registered_workers
        );
        self.known_workers
            .retain(|id, _| self.registered_workers.contains_key(id));
        self.stored_ranges
            .retain(|id, _| self.registered_workers.contains_key(id));
    }

    /// Get workers which meet all the following conditions:
    ///   a) registered on-chain,
    ///   b) running supported version,
    ///   c) sent ping withing the last `worker_inactive_timeout` period.
    pub async fn active_workers(&self) -> Vec<Worker> {
        self.known_workers
            .iter()
            .filter_map(|(_, w)| {
                (w.time_since_last_ping() < self.worker_inactive_timeout).then(|| w.clone())
            })
            .collect()
    }

    /// Get workers which meet the following conditions:
    ///   a) registered on-chain,
    ///   b) running supported version,
    ///   c) sent at least one ping.
    pub async fn known_workers(&self) -> Vec<Worker> {
        self.known_workers.values().cloned().collect()
    }

    pub fn stored_ranges(&self, worker_id: &PeerId) -> HashMap<String, RangeSet> {
        self.stored_ranges
            .get(worker_id)
            .cloned()
            .unwrap_or_default()
    }
}
