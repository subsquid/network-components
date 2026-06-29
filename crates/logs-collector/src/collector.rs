use std::collections::HashMap;

use parking_lot::Mutex;
use sqd_messages::QueryExecuted;
use sqd_network_transport::PeerId;

use collector_utils::{QueryExecutedRow, Storage};

/// Default maximum estimated size (bytes) of a single INSERT batch sent to
/// ClickHouse. Buffers larger than this are split into several INSERTs so that
/// an oversized batch can't fail repeatedly and stall log collection.
const DEFAULT_MAX_BATCH_SIZE: usize = 32 << 20; // 32 MiB

/// Default maximum estimated size (bytes) of logs kept in memory between dumps.
/// Once reached, further logs are dropped for the current round; since the
/// ClickHouse watermark hasn't advanced for them, they are re-collected on the
/// next round once the buffer drains.
const DEFAULT_MAX_BUFFER_SIZE: usize = 256 << 20; // 256 MiB

struct Buffer {
    logs: Vec<QueryExecutedRow>,
    /// Running estimate of `logs`' memory footprint, kept in sync on push/take.
    size: usize,
}

pub struct LogsCollector<T: Storage + Sync> {
    storage: T,
    buffer: Mutex<Buffer>,
    max_batch_size: usize,
    max_buffer_size: usize,
}

impl<T: Storage + Sync> LogsCollector<T> {
    pub fn new(storage: T) -> Self {
        let max_batch_size = env_size("MAX_INSERT_BATCH_BYTES", DEFAULT_MAX_BATCH_SIZE);
        let max_buffer_size =
            env_size("MAX_BUFFER_BYTES", DEFAULT_MAX_BUFFER_SIZE).max(max_batch_size);
        Self {
            storage,
            buffer: Mutex::new(Buffer {
                logs: Vec::new(),
                size: 0,
            }),
            max_batch_size,
            max_buffer_size,
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

        let mut buffer = self.buffer.lock();
        let mut dropped = 0;
        for row in rows {
            if buffer.size >= self.max_buffer_size {
                dropped += 1;
                continue;
            }
            buffer.size += row.estimated_size();
            buffer.logs.push(row);
        }
        if dropped > 0 {
            log::warn!(
                "Buffer full ({} bytes), dropped {dropped} logs from {worker_id}; \
                 they will be re-collected once the buffer drains",
                self.max_buffer_size
            );
        }
    }

    /// Whether the buffer has reached its memory limit. Callers use this to stop
    /// collecting more logs that would only be dropped (see `buffer_logs`).
    pub fn is_full(&self) -> bool {
        self.buffer.lock().size >= self.max_buffer_size
    }

    pub async fn dump_buffer(&mut self) -> anyhow::Result<()> {
        let logs = {
            let buffer = self.buffer.get_mut();
            buffer.size = 0;
            std::mem::take(&mut buffer.logs)
        };
        let total = logs.len();
        if total == 0 {
            return Ok(());
        }

        // Flush in memory-bounded chunks so no single INSERT can be too large.
        // On failure the unstored chunks are dropped, but they are re-collected
        // next round: the ClickHouse watermark is MAX(worker_timestamp) per worker
        // (see get_last_stored), and a single worker's logs are appended to the
        // buffer in ascending-timestamp order (collect_logs in server.rs requests
        // pages sequentially per worker). Chunks split the buffer at contiguous
        // positions, so any un-stored row has a timestamp >= the advanced
        // watermark and will be requested again. This ordering invariant is what
        // makes partial-failure safe; it breaks if collection is ever parallelized
        // within a single worker.
        let mut stored = 0;
        let mut chunk: Vec<QueryExecutedRow> = Vec::new();
        let mut chunk_size = 0;
        for row in logs {
            let row_size = row.estimated_size();
            if chunk_size + row_size > self.max_batch_size {
                if chunk.is_empty() {
                    // A single row exceeds the batch limit. Sending it alone is the
                    // best we can do; warn so a persistently-failing oversized row
                    // (which would stall this worker) is visible.
                    log::warn!(
                        "Single log row from {} is {row_size} bytes, exceeding the \
                         {} byte batch limit",
                        row.worker_id(),
                        self.max_batch_size
                    );
                } else {
                    let count = chunk.len();
                    self.flush_chunk(std::mem::take(&mut chunk), stored, total)
                        .await?;
                    stored += count;
                    chunk_size = 0;
                }
            }
            chunk_size += row_size;
            chunk.push(row);
        }
        if !chunk.is_empty() {
            let count = chunk.len();
            self.flush_chunk(chunk, stored, total).await?;
            stored += count;
        }

        log::info!("Dumped {stored} logs to storage");
        Ok(())
    }

    async fn flush_chunk(
        &self,
        chunk: Vec<QueryExecutedRow>,
        stored: usize,
        total: usize,
    ) -> anyhow::Result<()> {
        self.storage
            .store_logs(chunk.into_iter())
            .await
            .inspect_err(|e| log::warn!("Stored {stored}/{total} logs before failure: {e:?}"))
    }

    pub async fn last_timestamps(&mut self) -> anyhow::Result<HashMap<String, u64>> {
        let timestamps = self.storage.get_last_stored().await?;
        Ok(timestamps)
    }
}

fn env_size(var: &str, default: usize) -> usize {
    match std::env::var(var) {
        Ok(value) => match value.parse() {
            Ok(parsed) => parsed,
            Err(e) => {
                log::warn!("Invalid {var}={value:?}: {e}; using default {default}");
                default
            }
        },
        Err(_) => default,
    }
}
