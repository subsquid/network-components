use std::collections::HashMap;

use sqd_messages::QueryExecuted;
use sqd_network_transport::PeerId;

use collector_utils::{QueryExecutedRow, Storage};

pub struct LogsCollector<T: Storage + Sync> {
    storage: T,
    buffered_logs: Vec<QueryExecutedRow>,
}

impl<T: Storage + Sync> LogsCollector<T> {
    pub fn new(storage: T) -> Self {
        Self {
            storage,
            buffered_logs: Vec::new(),
        }
    }

    pub fn buffer_logs(&mut self, worker_id: PeerId, logs: Vec<QueryExecuted>) {
        log::debug!("Buffering {} logs from {worker_id}", logs.len());
        log::trace!("Logs buffered: {logs:?}");
        let rows = logs.into_iter().filter_map(|log| {
            QueryExecutedRow::try_from(log, worker_id)
                .map_err(|e| log::warn!("Invalid log message from {worker_id}: {e}"))
                .ok()
        });
        self.buffered_logs.extend(rows);
        // TODO: limit memory usage
    }

    pub async fn dump_buffer(&mut self) -> anyhow::Result<()> {
        log::info!("Dumping {} logs to storage", self.buffered_logs.len());
        self.storage
            .store_logs(self.buffered_logs.drain(..))
            .await?;
        Ok(())
    }

    pub async fn last_timestamps(&mut self) -> anyhow::Result<HashMap<String, u64>> {
        let timestamps = self.storage.get_last_stored().await?;
        Ok(timestamps)
    }
}
