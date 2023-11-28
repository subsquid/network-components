use crate::storage::LogsStorage;
use crate::utils::timestamp_now_ms;
use std::collections::HashMap;
use subsquid_messages::QueryExecuted;
use subsquid_network_transport::PeerId;

pub struct LogsCollector<T: LogsStorage> {
    storage: T,
    last_stored: HashMap<String, (u64, u64)>, // Last sequence number & timestamp saved in storage for each worker
    buffered_logs: HashMap<PeerId, Vec<QueryExecuted>>, // Local buffer, persisted periodically
}

impl<T: LogsStorage> LogsCollector<T> {
    pub fn new(storage: T) -> Self {
        Self {
            storage,
            last_stored: HashMap::new(),
            buffered_logs: HashMap::new(),
        }
    }

    pub fn collect_logs(&mut self, worker_id: PeerId, mut queries_executed: Vec<QueryExecuted>) {
        log::debug!("Collecting logs from {worker_id}: {queries_executed:?}");

        let buffered = self.buffered_logs.entry(worker_id).or_default();

        // * Sequence number of the last buffered log, or last stored log if there is none buffered,
        //   increased by one (because that's the expected sequence number of the **next** log),
        //   defaults to 0 if there are no logs buffered or stored.
        // * Timestamp of the last buffered or stored log, defaults to 0.
        let (mut next_seq_no, mut last_timestamp) = buffered
            .last()
            .map(|q| (q.seq_no, q.timestamp_ms))
            .or_else(|| self.last_stored.get(&worker_id.to_string()).cloned())
            .map(|(seq_no, ts)| (seq_no + 1, ts))
            .unwrap_or_default();

        // Remove already buffered/stored logs, sort to determine if there are gaps in the sequence
        queries_executed.retain(|q| q.seq_no >= next_seq_no);
        queries_executed.sort_by_cached_key(|q| q.seq_no);

        for query_executed in queries_executed {
            let seq_no = query_executed.seq_no;
            if seq_no > next_seq_no {
                log::error!("Gap in worker {worker_id} logs from {next_seq_no} to {seq_no}",)
            }
            let timestamp = query_executed.timestamp_ms;
            let now = timestamp_now_ms();
            if timestamp >= now || timestamp <= last_timestamp {
                log::error!("Invalid log timestamp: {last_timestamp} <?< {timestamp} <?< {now}");
                continue;
            }
            buffered.push(query_executed);
            next_seq_no = seq_no + 1;
            last_timestamp = timestamp;
        }
    }

    pub async fn storage_sync(&mut self) -> anyhow::Result<HashMap<String, u64>> {
        log::info!("Syncing state with storage");
        self.storage
            .store_logs(self.buffered_logs.iter().flat_map(|(_, logs)| logs))
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
