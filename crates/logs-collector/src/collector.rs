use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use subsquid_messages::QueryExecuted;
use subsquid_network_transport::PeerId;

use collector_utils::{timestamp_now_ms, QueryExecutedRow, Storage};

type SeqNo = u64;

pub struct LogsCollector<T: Storage + Sync> {
    storage: T,
    contract_client: Arc<dyn contract_client::Client>,
    epoch_seal_timeout: Duration,
    last_stored: HashMap<String, (SeqNo, u64)>, // Last sequence number & timestamp saved in storage for each worker
    buffered_logs: HashMap<String, BTreeMap<SeqNo, QueryExecutedRow>>, // Local buffer, persisted periodically
}

impl<T: Storage + Sync> LogsCollector<T> {
    pub fn new(
        storage: T,
        contract_client: Arc<dyn contract_client::Client>,
        epoch_seal_timeout: Duration,
    ) -> Self {
        Self {
            storage,
            contract_client,
            epoch_seal_timeout,
            last_stored: HashMap::new(),
            buffered_logs: HashMap::new(),
        }
    }

    pub fn collect_logs(&mut self, worker_id: PeerId, logs: Vec<QueryExecuted>) {
        log::debug!("Collecting logs from {worker_id}");
        log::trace!("Logs collected: {logs:?}");
        let mut rows: Vec<QueryExecutedRow> = logs
            .into_iter()
            .filter_map(|log| {
                log.try_into()
                    .map_err(|e| log::warn!("Invalid log message from {worker_id}: {e}"))
                    .ok()
            })
            .collect();

        let worker_id = worker_id.to_string();

        // Logs with sequence numbers below `from_seq_no` and timestamps earlier than since_timestamp
        // have already been stored in DB, so we don't have to collect them.
        let (from_seq_no, since_timestamp) = self
            .last_stored
            .get(&worker_id)
            .map(|(seq_no, ts)| (seq_no + 1, *ts))
            .unwrap_or_default(); // default to zeros
        let now = timestamp_now_ms();
        rows.retain(|r| {
            r.seq_no >= from_seq_no
                && r.worker_timestamp >= since_timestamp
                && r.worker_timestamp <= now // Logs cannot come from the future
        });

        let buffered = self.buffered_logs.entry(worker_id).or_default();
        for row in rows {
            buffered.insert(row.seq_no, row);
        }
    }

    pub async fn storage_sync(&mut self) -> anyhow::Result<HashMap<String, SeqNo>> {
        log::info!("Syncing state with storage");
        let logs_to_store = self.get_logs_to_store().await?;
        self.storage.store_logs(logs_to_store).await?;
        self.last_stored = self.storage.get_last_stored().await?;
        self.clear_buffer();
        let sequence_numbers = self
            .last_stored
            .iter()
            .map(|(peer_id, (seq_no, _))| (peer_id.clone(), *seq_no))
            .collect();
        Ok(sequence_numbers)
    }

    async fn get_logs_to_store(
        &self,
    ) -> anyhow::Result<impl Iterator<Item = QueryExecutedRow> + '_> {
        let epoch_start = self.contract_client.current_epoch_start().await?;
        let epoch_sealed = SystemTime::now() > epoch_start + self.epoch_seal_timeout;
        log::info!("Retrieving logs to store. epoch_sealed={epoch_sealed}");

        Ok(self
            .buffered_logs
            .iter()
            .flat_map(move |(worker_id, logs)| {
                let mut worker_logs = vec![];
                let mut next_seq_no = self
                    .last_stored
                    .get(worker_id)
                    .map(|(seq_no, _)| *seq_no + 1)
                    .unwrap_or_default();
                for (seq_no, log) in logs {
                    // If epoch is sealed, we need to store all logs available from previous epoch,
                    // don't care about gap. Otherwise, we store logs until the first gap.
                    let timestamp = UNIX_EPOCH + Duration::from_millis(log.worker_timestamp);
                    if !(epoch_sealed && timestamp < epoch_start) && *seq_no > next_seq_no {
                        log::debug!(
                            "Gap in logs from {next_seq_no} to {seq_no} worker_id={worker_id}"
                        );
                        break;
                    }

                    next_seq_no = seq_no + 1;
                    worker_logs.push(log.clone());
                    continue;
                }
                log::debug!("Storing logs below seq_no {next_seq_no} for worker {worker_id}");
                worker_logs
            }))
    }

    fn clear_buffer(&mut self) {
        log::info!("Clearing buffered logs");
        for (worker_id, (seq_no, _)) in self.last_stored.iter() {
            let buffered = match self.buffered_logs.get_mut(worker_id) {
                None => continue,
                Some(buffered) => buffered,
            };
            log::debug!("Removing logs up to {seq_no} for worker {worker_id}");
            *buffered = buffered.split_off(&(seq_no + 1)); // +1 because split_off is inclusive
        }
    }
}
