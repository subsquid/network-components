use std::collections::HashMap;

use parking_lot::Mutex;
use sqd_messages::QueryExecuted;
use sqd_network_transport::PeerId;

use collector_utils::{QueryExecutedRow, Storage};

pub struct LogsCollector<T: Storage + Sync> {
    storage: T,
    buffered_logs: Mutex<Vec<QueryExecutedRow>>,
}

impl<T: Storage + Sync> LogsCollector<T> {
    pub fn new(storage: T) -> Self {
        Self {
            storage,
            buffered_logs: Default::default(),
        }
    }

    pub fn buffer_logs(&self, worker_id: PeerId, logs: Vec<QueryExecuted>) {
        log::debug!("Buffering {} logs from {worker_id}", logs.len());
        log::trace!("Logs buffered: {logs:?}");
        let rows = logs.into_iter().filter_map(|log| {
            QueryExecutedRow::try_from(log, worker_id)
                .map_err(|e| log::warn!("Invalid log message from {worker_id}: {e}"))
                .ok()
        });
        self.buffered_logs.lock().extend(rows);
        // TODO: limit memory usage
    }

    pub async fn dump_buffer(&mut self) -> anyhow::Result<()> {
        let logs = self.buffered_logs.get_mut().drain(..);
        log::info!("Dumping {} logs to storage", logs.len());
        self.storage.store_logs(logs).await?;
        Ok(())
    }

    pub async fn last_timestamps(&mut self) -> anyhow::Result<HashMap<String, u64>> {
        let timestamps = self.storage.get_last_stored().await?;
        Ok(timestamps)
    }
}
