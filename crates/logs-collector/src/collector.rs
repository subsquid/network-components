use std::collections::{HashMap, VecDeque};
use std::future::Future;

use parking_lot::Mutex;
use sqd_messages::QueryExecuted;
use sqd_network_transport::PeerId;
use tokio::sync::Notify;

use collector_utils::{QueryExecutedRow, Storage};

use crate::metrics;

/// Default maximum estimated size (bytes) of a single INSERT batch sent to
/// ClickHouse. The buffer is stored a batch at a time as soon as one has
/// accumulated, so no INSERT can grow large enough to fail repeatedly and stall
/// log collection.
const DEFAULT_MAX_BATCH_SIZE: usize = 32 << 20; // 32 MiB

/// Default estimated size (bytes) of buffered logs above which collection waits
/// for the writer to catch up. Bounds memory when ClickHouse is slower than the
/// workers; nothing is dropped. Exceeded by at most one page, since pages are
/// buffered whole; the pages of workers waiting for room are held outside it.
const DEFAULT_MAX_BUFFER_SIZE: usize = 256 << 20; // 256 MiB

struct Buffer {
    /// Whole pages, appended in the order they were collected. A worker's pages
    /// are collected sequentially, so its rows are in ascending-timestamp order.
    logs: VecDeque<QueryExecutedRow>,
    /// Running estimate of `logs`' memory footprint, kept in sync on push/take.
    size: usize,
}

pub struct LogsCollector<T: Storage + Sync> {
    storage: T,
    buffer: Mutex<Buffer>,
    /// A batch's worth of logs is buffered: wakes the writer (`store_batches_until`).
    batch_ready: Notify,
    /// A batch was taken off the buffer: wakes the workers waiting in `buffer_logs`.
    room: Notify,
    max_batch_size: usize,
    max_buffer_size: usize,
    /// Queries longer than this are truncated before being buffered.
    max_query_bytes: Option<usize>,
}

impl<T: Storage + Sync> LogsCollector<T> {
    pub fn new(storage: T, max_query_bytes: Option<usize>) -> Self {
        Self {
            max_query_bytes,
            ..Self::with_limits(
                storage,
                env_size("MAX_INSERT_BATCH_BYTES", DEFAULT_MAX_BATCH_SIZE),
                env_size("MAX_BUFFER_BYTES", DEFAULT_MAX_BUFFER_SIZE),
            )
        }
    }

    pub(crate) fn with_limits(storage: T, max_batch_size: usize, max_buffer_size: usize) -> Self {
        let max_buffer_size = max_buffer_size.max(max_batch_size);
        metrics::BUFFER_MAX_BYTES.set(max_buffer_size as i64);
        Self {
            storage,
            buffer: Mutex::new(Buffer {
                logs: VecDeque::new(),
                size: 0,
            }),
            batch_ready: Notify::new(),
            room: Notify::new(),
            max_batch_size,
            max_buffer_size,
            max_query_bytes: None,
        }
    }

    /// Adds a page of a worker's logs to the buffer, waiting for room if it has
    /// reached its limit. Nothing is dropped: once collected, logs are only lost if
    /// storing them fails (see `store_batches_until`).
    pub async fn buffer_logs(&self, worker_id: PeerId, mut rows: Vec<QueryExecutedRow>) {
        tracing::debug!(worker_id = %worker_id, logs = rows.len(), "Buffering logs");
        if let Some(max_bytes) = self.max_query_bytes {
            for row in &mut rows {
                row.truncate_query(max_bytes);
            }
        }
        let size: usize = rows.iter().map(QueryExecutedRow::estimated_size).sum();
        let mut waited = false;
        loop {
            // Registered before checking the buffer, so that a batch taken off it
            // in between isn't missed.
            let room = self.room.notified();
            tokio::pin!(room);
            room.as_mut().enable();
            {
                let mut buffer = self.buffer.lock();
                if buffer.size < self.max_buffer_size {
                    buffer.size += size;
                    buffer.logs.extend(rows);
                    metrics::BUFFER_BYTES.set(buffer.size as i64);
                    if buffer.size >= self.max_batch_size {
                        self.batch_ready.notify_one();
                    }
                    return;
                }
            }
            if !waited {
                tracing::debug!(
                    worker_id = %worker_id,
                    logs = rows.len(),
                    "Buffer full, waiting for room"
                );
                waited = true;
            }
            room.await;
        }
    }

    /// Stores the buffer a batch at a time as batches accumulate, until `collected`
    /// completes, and then whatever is left. Fails at the first INSERT that fails,
    /// leaving the rest of the buffer in place.
    ///
    /// The caller must then stop collecting and `clear` the buffer instead of
    /// storing any more of it: the ClickHouse watermark is MAX(worker_timestamp)
    /// per worker (see `get_last_stored`), and batches take the buffer's rows in
    /// order, so as long as nothing is stored past a failed batch every unstored
    /// row is above the watermark and is collected again next round. A later
    /// batch stored after the failed one would advance the watermark past the
    /// failed rows for good. This holds because a worker's pages are collected
    /// sequentially; it breaks if collection is ever parallelized within a worker.
    ///
    /// A failed batch isn't retried: if the INSERT reached ClickHouse before it
    /// failed, a retry would store it twice, whereas the next round resumes from
    /// whatever the watermark says was stored.
    pub async fn store_batches_until(
        &self,
        collected: impl Future<Output = ()>,
    ) -> anyhow::Result<()> {
        tokio::pin!(collected);
        let mut stored = 0;
        loop {
            tokio::select! {
                biased;
                _ = self.batch_ready.notified() => self.store_batches(false, &mut stored).await?,
                _ = &mut collected => break,
            }
        }
        self.store_batches(true, &mut stored).await?;
        tracing::info!(logs = stored, "Dumped logs to storage");
        Ok(())
    }

    /// Discards the buffer after a failed INSERT (see `store_batches_until`).
    pub fn clear(&self) {
        let mut buffer = self.buffer.lock();
        let dropped = buffer.logs.len();
        buffer.logs.clear();
        buffer.size = 0;
        metrics::BUFFER_BYTES.set(0);
        if dropped > 0 {
            tracing::warn!(
                logs = dropped,
                "Dropped unstored logs; they will be collected again next round"
            );
        }
    }

    /// Stores every full batch in the buffer, and with `everything` the remainder
    /// too, counting the stored rows in `stored`.
    async fn store_batches(&self, everything: bool, stored: &mut usize) -> anyhow::Result<()> {
        while let Some(batch) = self.take_batch(everything) {
            // Before the INSERT rather than after, so workers refill the buffer
            // while it runs.
            self.room.notify_waiters();
            let count = batch.len();
            self.store_batch(batch, *stored).await?;
            *stored += count;
        }
        Ok(())
    }

    /// Takes the next batch off the front of the buffer: a full one, or with
    /// `everything` whatever is buffered. `None` when there is no such batch.
    fn take_batch(&self, everything: bool) -> Option<Vec<QueryExecutedRow>> {
        let mut buffer = self.buffer.lock();
        if buffer.logs.is_empty() || (!everything && buffer.size < self.max_batch_size) {
            return None;
        }
        let mut batch = Vec::new();
        let mut batch_size = 0;
        while let Some(row) = buffer.logs.front() {
            let row_size = row.estimated_size();
            if batch_size + row_size > self.max_batch_size {
                if !batch.is_empty() {
                    break;
                }
                // A single row exceeds the batch limit. Sending it alone is the
                // best we can do; warn so a persistently-failing oversized row
                // (which would stall this worker) is visible.
                tracing::warn!(
                    worker_id = row.worker_id(),
                    row_bytes = row_size,
                    max_batch_bytes = self.max_batch_size,
                    "Single log row exceeds the batch limit"
                );
            }
            batch_size += row_size;
            batch.extend(buffer.logs.pop_front());
        }
        buffer.size = buffer.size.saturating_sub(batch_size);
        metrics::BUFFER_BYTES.set(buffer.size as i64);
        tracing::debug!(
            logs = batch.len(),
            batch_bytes = batch_size,
            buffered_logs = buffer.logs.len(),
            "Taking a batch off the buffer"
        );
        Some(batch)
    }

    async fn store_batch(&self, batch: Vec<QueryExecutedRow>, stored: usize) -> anyhow::Result<()> {
        let lags: Vec<f64> = batch
            .iter()
            .map(|row| row.collector_timestamp.saturating_sub(row.worker_timestamp) as f64 / 1000.0)
            .collect();
        self.storage
            .store_logs(batch.into_iter())
            .await
            .inspect(|()| {
                metrics::LOGS_STORED.inc_by(lags.len() as u64);
                lags.iter().for_each(|&lag| metrics::LOG_LAG.observe(lag));
                tracing::debug!(logs = lags.len(), "Stored a batch of logs");
            })
            .inspect_err(|e| {
                tracing::warn!(
                    stored,
                    logs = lags.len(),
                    error = format!("{e:#}"),
                    "Couldn't store all logs"
                )
            })
    }

    pub async fn last_timestamps(&self) -> anyhow::Result<HashMap<String, u64>> {
        let timestamps = self.storage.get_last_stored().await?;
        Ok(timestamps)
    }
}

/// Converts a worker's logs into rows, dropping the invalid ones. Verifying every
/// log's signature makes this CPU-bound, so it belongs on a blocking thread.
pub fn rows_from_logs(worker_id: PeerId, logs: Vec<QueryExecuted>) -> Vec<QueryExecutedRow> {
    tracing::trace!(worker_id = %worker_id, ?logs, "Logs received");
    let mut invalid = HashMap::<&str, usize>::new();
    let rows = logs
        .into_iter()
        .filter_map(|log| {
            QueryExecutedRow::try_from(log, worker_id)
                .map_err(|reason| *invalid.entry(reason).or_default() += 1)
                .ok()
        })
        .collect();
    for (reason, count) in invalid {
        metrics::LOGS_DISCARDED
            .get_or_create(&[("reason", "invalid")])
            .inc_by(count as u64);
        tracing::warn!(worker_id = %worker_id, count, reason, "Dropped invalid logs");
    }
    rows
}

fn env_size(var: &str, default: usize) -> usize {
    match std::env::var(var) {
        Ok(value) => match value.parse() {
            Ok(parsed) => parsed,
            Err(e) => {
                tracing::warn!(var, value, error = %e, default, "Invalid size setting, using the default");
                default
            }
        },
        Err(_) => default,
    }
}
