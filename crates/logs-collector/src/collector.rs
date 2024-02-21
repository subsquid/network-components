use crate::storage::{LogsStorage, QueryExecutedRow};
use crate::utils::timestamp_now_ms;
use std::collections::HashMap;
use subsquid_messages::QueryExecuted;
use subsquid_network_transport::PeerId;

pub struct LogsCollector<T: LogsStorage> {
    storage: T,
    last_stored: HashMap<String, (u64, u64)>, // Last sequence number & timestamp saved in storage for each worker
    buffered_logs: HashMap<PeerId, Vec<QueryExecutedRow>>, // Local buffer, persisted periodically
}

impl<T: LogsStorage> LogsCollector<T> {
    pub fn new(storage: T) -> Self {
        Self {
            storage,
            last_stored: HashMap::new(),
            buffered_logs: HashMap::new(),
        }
    }

    pub fn collect_logs(&mut self, worker_id: PeerId, logs: Vec<QueryExecuted>) {
        log::debug!("Collecting logs from {worker_id}");
        log::trace!("Logs collected: {logs:?}");
        let mut rows: Vec<QueryExecutedRow> = logs
            .into_iter()
            .filter_map(|log| match log.try_into() {
                Ok(log) => Some(log),
                Err(e) => {
                    log::error!("Invalid log message: {e}");
                    None
                }
            })
            .collect();

        let buffered = self.buffered_logs.entry(worker_id).or_default();

        // * Sequence number of the last buffered log, or last stored log if there is none buffered,
        //   increased by one (because that's the expected sequence number of the **next** log),
        //   defaults to 0 if there are no logs buffered or stored.
        // * Timestamp of the last buffered or stored log, defaults to 0.
        let (mut next_seq_no, mut last_timestamp) = buffered
            .last()
            .map(|r| (r.seq_no, r.worker_timestamp))
            .or_else(|| self.last_stored.get(&worker_id.to_string()).cloned())
            .map(|(seq_no, ts)| (seq_no + 1, ts))
            .unwrap_or_default();

        // Remove already buffered/stored logs, sort to determine if there are gaps in the sequence
        rows.retain(|r| r.seq_no >= next_seq_no);
        rows.sort_by_cached_key(|r| r.seq_no);

        for row in rows {
            let seq_no = row.seq_no;
            if seq_no > next_seq_no {
                log::error!("Gap in worker {worker_id} logs from {next_seq_no} to {seq_no}",)
            }
            let timestamp = row.worker_timestamp;
            let now = timestamp_now_ms();
            if timestamp >= now || timestamp <= last_timestamp {
                log::error!("Invalid log timestamp: {last_timestamp} <?< {timestamp} <?< {now}");
                continue;
            }
            buffered.push(row);
            next_seq_no = seq_no + 1;
            last_timestamp = timestamp;
        }
    }

    pub async fn storage_sync(&mut self) -> anyhow::Result<HashMap<String, u64>> {
        log::info!("Syncing state with storage");
        self.storage
            .store_logs(
                self.buffered_logs
                    .iter()
                    .flat_map(|(_, logs)| logs)
                    .cloned(),
            )
            .await?;
        self.last_stored = self.storage.get_last_stored().await?;
        self.buffered_logs.clear();
        let sequence_numbers = self
            .last_stored
            .iter()
            .map(|(peer_id, (seq_no, _))| (peer_id.clone(), *seq_no))
            .collect();
        Ok(sequence_numbers)
    }
}
