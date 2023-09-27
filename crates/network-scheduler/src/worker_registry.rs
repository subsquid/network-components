use std::collections::HashMap;
use std::time::Duration;
use std::time::Instant;

use serde::{Serialize, Serializer};

use contract_client::Address;
use router_controller::messages::{Ping, RangeSet};
use subsquid_network_transport::PeerId;

pub const WORKER_INACTIVE_TIMEOUT: Duration = Duration::from_secs(60);
pub const SUPPORTED_WORKER_VERSIONS: [&str; 2] = ["0.1.2", "0.1.3"];

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

    pub fn is_active(&self) -> bool {
        self.last_ping.elapsed() < WORKER_INACTIVE_TIMEOUT
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
}

impl WorkerRegistry {
    pub async fn init(rpc_url: &str) -> anyhow::Result<Self> {
        let client = contract_client::get_client(rpc_url).await?;
        let mut registry = Self {
            client,
            registered_workers: Default::default(),
            known_workers: Default::default(),
            stored_ranges: Default::default(),
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
    ///   c) sent ping withing the last `WORKER_INACTIVE_TIMEOUT` period.
    pub async fn active_workers(&self) -> Vec<Worker> {
        self.known_workers
            .iter()
            .filter_map(|(_, w)| w.is_active().then(|| w.clone()))
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
