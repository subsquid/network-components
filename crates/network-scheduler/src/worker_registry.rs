use std::collections::{HashMap, HashSet};
use std::time::Duration;

use serde::{Serialize, Serializer};
use std::time::Instant;

use router_controller::messages::Ping;
use subsquid_network_transport::PeerId;

pub const WORKER_INACTIVE_TIMEOUT: Duration = Duration::from_secs(60);

#[derive(Debug, Clone, Serialize)]
pub struct ActiveWorker {
    #[serde(serialize_with = "serialize_peer_id")]
    pub peer_id: PeerId,
    #[serde(with = "serde_millis")]
    pub last_ping: Instant,
    pub stored_bytes: u64,
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
    pings: HashMap<PeerId, Instant>,
    stored_bytes: HashMap<PeerId, u64>,
}

impl WorkerRegistry {
    pub async fn init(rpc_url: &str) -> anyhow::Result<Self> {
        let client = contract_client::get_client(rpc_url).await?;
        let mut registry = Self {
            client,
            registered_workers: Default::default(),
            pings: Default::default(),
            stored_bytes: Default::default(),
        };
        // Need to get new workers immediately, otherwise they wouldn't be updated until the first
        // call to `active_workers`, so all pings would be discarded.
        registry.update_workers().await?;
        Ok(registry)
    }

    pub async fn ping(&mut self, worker_id: PeerId, msg: &Ping) {
        log::debug!("Got ping from {worker_id}");
        if self.registered_workers.contains(&worker_id) {
            self.pings.insert(worker_id, Instant::now());
            self.stored_bytes.insert(worker_id, msg.stored_bytes);
        }
    }

    async fn update_workers(&mut self) -> anyhow::Result<()> {
        let new_workers = self.client.active_workers().await?;
        self.registered_workers = new_workers.into_iter().map(|w| w.peer_id).collect();
        log::info!(
            "Registered workers set updated: {:?}",
            self.registered_workers
        );
        self.pings
            .retain(|id, _| self.registered_workers.contains(id));
        self.stored_bytes
            .retain(|id, _| self.registered_workers.contains(id));
        Ok(())
    }

    pub async fn active_workers(&mut self) -> Vec<ActiveWorker> {
        if let Err(e) = self.update_workers().await {
            log::error!("Error updating worker set: {e:?}")
        }
        self.registered_workers
            .iter()
            .filter_map(|id| match self.pings.get(id) {
                Some(last_ping) if last_ping.elapsed() < WORKER_INACTIVE_TIMEOUT => {
                    Some(ActiveWorker {
                        peer_id: *id,
                        last_ping: *last_ping,
                        stored_bytes: self.stored_bytes.get(id).cloned().unwrap_or_default(),
                    })
                }
                _ => None,
            })
            .collect()
    }
}
