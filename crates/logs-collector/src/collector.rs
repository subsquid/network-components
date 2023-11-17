use crate::storage::LogsStorage;
use router_controller::messages::{QueryExecuted, QueryLogs};
use std::collections::HashMap;
use subsquid_network_transport::PeerId;

pub struct LogsCollector<T: LogsStorage> {
    storage: T,
    sequence_numbers: HashMap<String, u32>, // Last sequence number saved in storage for each worker
    queries_executed: HashMap<PeerId, Vec<QueryExecuted>>, // Local buffer, persisted periodically
}

impl<T: LogsStorage> LogsCollector<T> {
    pub fn new(storage: T) -> Self {
        Self {
            storage,
            sequence_numbers: HashMap::new(),
            queries_executed: HashMap::new(),
        }
    }

    pub fn collect_logs(
        &mut self,
        worker_id: PeerId,
        QueryLogs {
            mut queries_executed,
        }: QueryLogs,
    ) {
        log::debug!("Collecting logs from {worker_id}: {queries_executed:?}");
        let buffered = self.queries_executed.entry(worker_id).or_default();

        // Sequence number of the last buffered log, or last stored log if there is none buffered,
        // increased by one (because that's the expected sequence number of the **next** log),
        // defaults to 0 if there are no logs buffered or stored.
        let mut next_seq_no = buffered
            .last()
            .map(|q| q.seq_no)
            .or_else(|| self.sequence_numbers.get(&worker_id.to_string()).cloned())
            .map(|seq_no| seq_no + 1)
            .unwrap_or_default();

        // Remove already buffered/stored logs, sort to determine if there are gaps in the sequence
        queries_executed.retain(|q| q.seq_no >= next_seq_no);
        queries_executed.sort_by_cached_key(|q| q.seq_no);

        for query_executed in queries_executed {
            let seq_no = query_executed.seq_no;
            if seq_no > next_seq_no {
                log::error!("Gap in worker {worker_id} logs from {next_seq_no} to {seq_no}",)
            }
            buffered.push(query_executed);
            next_seq_no = seq_no + 1;
        }
    }

    pub async fn storage_sync(&mut self) -> anyhow::Result<HashMap<String, u32>> {
        log::info!("Syncing state with storage");
        self.storage
            .store_logs(self.queries_executed.iter().flat_map(|(_, logs)| logs))
            .await?;
        self.sequence_numbers = self.storage.get_last_seq_numbers().await?;
        self.queries_executed.clear();
        Ok(self.sequence_numbers.clone())
    }
}
