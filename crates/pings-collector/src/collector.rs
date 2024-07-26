use subsquid_messages::Ping;
use subsquid_network_transport::PeerId;

use collector_utils::{PingRow, Storage};

pub struct PingsCollector<T: Storage + Sync> {
    storage: T,
    buffered_pings: Vec<PingRow>, // FIXME: Use persistent buffer
}

impl<T: Storage + Sync> PingsCollector<T> {
    pub fn new(storage: T) -> Self {
        Self {
            storage,
            buffered_pings: Vec::new(),
        }
    }

    pub fn collect_ping(&mut self, worker_id: PeerId, ping: Ping) {
        log::debug!("Collecting ping from {worker_id}");
        log::trace!("Ping collected: {ping:?}");
        let ping_row = match ping.try_into() {
            Ok(row) => row,
            Err(e) => return log::error!("Invalid ping from {worker_id}: {e}"),
        };
        self.buffered_pings.push(ping_row);
    }

    pub async fn storage_sync(&mut self) -> anyhow::Result<()> {
        log::info!("Syncing state with storage");
        self.storage
            .store_pings(self.buffered_pings.iter().cloned())
            .await?;

        log::info!("Clearing buffered pings");
        self.buffered_pings.clear();
        self.buffered_pings.shrink_to_fit();

        Ok(())
    }
}
