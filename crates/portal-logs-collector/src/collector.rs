use parking_lot::Mutex;
use sqd_messages::QueryFinished;
use sqd_network_transport::PeerId;

use collector_utils::{QueryFinishedRow, Storage};

pub struct PortalLogsCollector<T: Storage + Sync> {
    storage: T,
    buffered_logs: Mutex<Vec<QueryFinishedRow>>,
}

impl<T: Storage + Sync> PortalLogsCollector<T> {
    pub fn new(storage: T) -> Self {
        Self {
            storage,
            buffered_logs: Default::default(),
        }
    }

    pub fn buffer_logs(&self, worker_id: PeerId, logs: Vec<QueryFinished>) {
        log::debug!("Buffering {} logs from {worker_id}", logs.len());
        log::trace!("Logs buffered: {logs:?}");
        let rows = logs.into_iter().filter_map(|log| {
            QueryFinishedRow::try_from(log)
                .map_err(|e| log::warn!("Invalid log message from {worker_id}: {e}"))
                .ok()
        });
        self.buffered_logs.lock().extend(rows);
        // TODO: limit memory usage
    }

    pub async fn dump_buffer(&self) -> anyhow::Result<()> {
        let logs: Vec<_> = self.buffered_logs.lock().drain(..).collect();
        log::info!("Dumping {} logs to storage", logs.len());
        if !logs.is_empty() {
            self.storage.store_portal_logs(logs.into_iter()).await?;
        }
        Ok(())
    }
}
