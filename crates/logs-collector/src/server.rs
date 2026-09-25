use std::collections::HashSet;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use collector_utils::Storage;
use parking_lot::Mutex;
use sqd_contract_client::{Client as ContractClient, Worker};
use sqd_messages::{LogsRequest, QueryLogs};
use sqd_network_transport::util::{CancellationToken, TaskManager};
use sqd_network_transport::{LogsCollectorTransport, PeerId};
use tokio::task::{JoinError, JoinSet};
use tokio::time::Instant;

use crate::collector::{rows_from_logs, LogsCollector};
use crate::metrics;

const MAX_PAGES: usize = 5;

/// Where worker logs are requested from: the P2P transport, or a fake in tests.
pub trait LogsSource: Send + Sync + 'static {
    fn request_logs(
        &self,
        worker_id: PeerId,
        request: LogsRequest,
    ) -> impl Future<Output = anyhow::Result<QueryLogs>> + Send;
}

impl LogsSource for LogsCollectorTransport {
    async fn request_logs(
        &self,
        worker_id: PeerId,
        request: LogsRequest,
    ) -> anyhow::Result<QueryLogs> {
        LogsCollectorTransport::request_logs(self, worker_id, request)
            .await
            // The `Debug` form is what has always been logged for these errors.
            .map_err(|e| anyhow::anyhow!("{e:?}"))
    }
}

pub struct Server<L, T>
where
    L: LogsSource,
    T: Storage + Send + Sync + 'static,
{
    transport_handle: L,
    logs_collector: LogsCollector<T>,
    registered_workers: Arc<Mutex<HashSet<PeerId>>>,
    /// Only workers whose peer ID's last byte modulo `total_shards` is `shard` are collected from.
    shard: u8,
    total_shards: u8,
}

impl<L, T> Server<L, T>
where
    L: LogsSource,
    T: Storage + Send + Sync + 'static,
{
    pub fn new(
        transport: L,
        logs_collector: LogsCollector<T>,
        shard: u8,
        total_shards: u8,
    ) -> Self {
        Self {
            transport_handle: transport,
            logs_collector,
            registered_workers: Default::default(),
            shard,
            total_shards,
        }
    }

    pub async fn run(
        self,
        contract_client: Arc<dyn ContractClient>,
        collection_interval: Duration,
        backlog_collection_interval: Option<Duration>,
        worker_update_interval: Duration,
        concurrent_workers: usize,
        cancellation_token: CancellationToken,
    ) -> anyhow::Result<()> {
        tracing::info!(
            shard = self.shard,
            total_shards = self.total_shards,
            "Starting logs collector server"
        );

        // Get registered workers from chain
        let workers = contract_client.active_workers().await?;
        *self.registered_workers.lock() = filter_peer_ids(workers, self.shard, self.total_shards);

        let mut task_manager = TaskManager::default();
        self.spawn_worker_update_task(&mut task_manager, contract_client, worker_update_interval);

        Arc::new(self)
            .run_collecting_task(
                collection_interval,
                backlog_collection_interval,
                concurrent_workers,
                cancellation_token.child_token(),
            )
            .await;

        tracing::info!("Server shutting down");
        task_manager.await_stop().await;
        Ok(())
    }

    async fn run_collecting_task(
        self: Arc<Self>,
        interval: Duration,
        backlog_interval: Option<Duration>,
        concurrent_jobs: usize,
        cancel_token: CancellationToken,
    ) {
        // A backlog never makes the next round start later than it would anyway.
        let backlog_interval = backlog_interval.map(|backlog| backlog.min(interval));
        tracing::info!(
            interval_secs = interval.as_secs(),
            backlog_interval_secs = backlog_interval.map(|backlog| backlog.as_secs()),
            "Starting log collection"
        );
        let mut interval = tokio::time::interval(interval);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                // With a backlog the next tick is often already due, and shutdown
                // shouldn't have to win a coin flip against it.
                biased;
                _ = cancel_token.cancelled() => break,
                _ = interval.tick() => (),
            };
            let round_start = Instant::now();

            let workers = self.registered_workers.lock().clone();
            tracing::info!(workers = workers.len(), "Collecting logs from workers");
            metrics::COMMON.workers.set(workers.len() as i64);
            let last_timestamps = match self.logs_collector.last_timestamps().await {
                Ok(timestamps) => timestamps,
                Err(e) => {
                    metrics::STORAGE_ERRORS
                        .get_or_create(&[("operation", "read")])
                        .inc();
                    tracing::warn!(error = format!("{e:#}"), "Couldn't read last stored logs");
                    continue;
                }
            };

            // The workers furthest behind go first.
            let mut workers: Vec<_> = workers
                .into_iter()
                .map(|worker_id| {
                    let from_timestamp_ms = last_timestamps
                        .get(&worker_id.to_string())
                        .map(|ts| ts + 1)
                        .unwrap_or(0);
                    (worker_id, from_timestamp_ms)
                })
                .collect();
            workers.sort_by_key(|(_, from_timestamp_ms)| *from_timestamp_ms);

            let collected = self
                .collect_round(workers, concurrent_jobs, &cancel_token)
                .await;
            metrics::COMMON.observe_round(round_start.elapsed());
            let backlogged_workers = match collected {
                Ok(backlogged) => backlogged,
                Err(e) => {
                    metrics::STORAGE_ERRORS
                        .get_or_create(&[("operation", "insert")])
                        .inc();
                    // Don't start the next round early: it would fetch the same logs
                    // again, most likely only to fail the same way.
                    tracing::warn!(error = format!("{e:#}"), "Couldn't store logs");
                    continue;
                }
            };
            metrics::BACKLOGGED_WORKERS.set(backlogged_workers as i64);

            if backlogged_workers == 0 {
                continue;
            }
            match backlog_interval {
                Some(backlog_interval) => {
                    let next_round = round_start + backlog_interval;
                    tracing::info!(
                        backlogged_workers,
                        next_round_in_secs = next_round
                            .saturating_duration_since(Instant::now())
                            .as_secs(),
                        "Workers have more logs, starting the next round early"
                    );
                    interval.reset_at(next_round);
                }
                None => tracing::info!(
                    backlogged_workers,
                    "Workers have more logs, collecting them next round"
                ),
            }
        }
    }

    /// Collects the workers' logs, `concurrent_jobs` workers at a time, storing them
    /// in batches as they come in. Returns how many workers may have logs left for
    /// the next round.
    ///
    /// Fails if a batch couldn't be stored. The round stops right there: the workers
    /// still being collected are cancelled and the buffer is discarded, so that their
    /// logs are collected again next round (see `LogsCollector::store_batches_until`
    /// for why nothing more may be stored).
    async fn collect_round(
        self: &Arc<Self>,
        workers: Vec<(PeerId, u64)>,
        concurrent_jobs: usize,
        cancel_token: &CancellationToken,
    ) -> anyhow::Result<usize> {
        let collected = CancellationToken::new();
        let failed = CancellationToken::new();

        let writer = async {
            let stored = self
                .logs_collector
                .store_batches_until(collected.cancelled())
                .await;
            if stored.is_err() {
                failed.cancel();
            }
            stored
        };

        let producers = async {
            let mut workers = workers.into_iter();
            let mut tasks = JoinSet::new();
            let mut backlogged = 0;
            loop {
                while tasks.len() < concurrent_jobs {
                    let Some((worker_id, from_timestamp_ms)) = workers.next() else {
                        break;
                    };
                    // A task per worker, so collecting isn't confined to this thread.
                    let server = self.clone();
                    tasks.spawn(
                        async move { server.collect_logs(worker_id, from_timestamp_ms).await },
                    );
                }
                tokio::select! {
                    biased;
                    _ = cancel_token.cancelled() => break,
                    _ = failed.cancelled() => break,
                    joined = tasks.join_next() => match joined {
                        Some(joined) => backlogged += usize::from(has_backlog(joined)),
                        None => break,
                    },
                }
            }
            // Nothing may be buffered once the writer stores the rest, or the buffer
            // is discarded: a page buffered after that would be stored twice, or never.
            tasks.shutdown().await;
            collected.cancel();
            backlogged
        };

        // Both run in this task: the writer only waits on ClickHouse and the producers
        // only wait on the workers' tasks, so neither holds the other up.
        let (stored, backlogged) = tokio::join!(writer, producers);
        if stored.is_err() {
            self.logs_collector.clear();
        }
        stored.map(|()| backlogged)
    }

    /// Returns `true` if the worker may have logs left that this round didn't collect
    /// because it had more than `MAX_PAGES` pages. A failed request returns `false`,
    /// so unreachable workers never make rounds start early.
    async fn collect_logs(&self, worker_id: PeerId, mut from_timestamp_ms: u64) -> bool {
        let mut last_query_id = None;
        for page in 0..MAX_PAGES {
            tracing::debug!(worker_id = %worker_id, page, from_timestamp_ms, "Collecting logs");
            let logs = match metrics::COMMON
                .observe_request(self.transport_handle.request_logs(
                    worker_id,
                    LogsRequest {
                        from_timestamp_ms,
                        last_received_query_id: last_query_id,
                    },
                ))
                .await
            {
                Ok(logs) => logs,
                Err(e) => {
                    tracing::warn!(worker_id = %worker_id, error = format!("{e:#}"), "Error getting logs");
                    return false;
                }
            };

            let Some(last_log) = logs.queries_executed.last() else {
                return false;
            };
            last_query_id = last_log.query.as_ref().map(|q| q.query_id.clone());
            from_timestamp_ms = last_log.timestamp_ms;

            // Verifying signatures is CPU-bound: keep it off the threads driving the network.
            let rows = tokio::task::spawn_blocking(move || {
                rows_from_logs(worker_id, logs.queries_executed)
            });
            let rows = match rows.await {
                Ok(rows) => rows,
                Err(e) => {
                    tracing::error!(worker_id = %worker_id, error = %e, "Couldn't process logs");
                    return false;
                }
            };

            self.logs_collector.buffer_logs(worker_id, rows).await;
            if !logs.has_more {
                return false;
            }
        }
        tracing::debug!(
            worker_id = %worker_id,
            pages = MAX_PAGES,
            "Logs didn't fit in one round, continuing next round"
        );
        true
    }

    fn spawn_worker_update_task(
        &self,
        task_manager: &mut TaskManager,
        contract_client: Arc<dyn ContractClient>,
        interval: Duration,
    ) {
        tracing::info!("Starting worker update task");
        let registered_workers = self.registered_workers.clone();
        let (shard, total_shards) = (self.shard, self.total_shards);
        let contract_client: Arc<dyn ContractClient> = contract_client;
        let task = move |_| {
            let registered_workers = registered_workers.clone();
            let contract_client = contract_client.clone();
            async move {
                let workers = match contract_client.active_workers().await {
                    Ok(workers) => workers,
                    Err(e) => {
                        return tracing::error!(error = ?e, "Error getting registered workers")
                    }
                };
                *registered_workers.lock() = filter_peer_ids(workers, shard, total_shards);
            }
        };
        task_manager.spawn_periodic(task, interval);
    }
}

/// What a joined `collect_logs` task found: whether its worker has logs left.
fn has_backlog(joined: Result<bool, JoinError>) -> bool {
    joined.unwrap_or_else(|e| {
        tracing::error!(error = %e, "Log collection task failed");
        false
    })
}

fn filter_peer_ids(workers: Vec<Worker>, shard: u8, total_shards: u8) -> HashSet<PeerId> {
    workers
        .into_iter()
        .map(|w| w.peer_id)
        .filter(|peer_id| {
            let last_byte = *peer_id.to_bytes().last().expect("a peer ID is never empty");
            last_byte % total_shards == shard
        })
        .collect()
}

#[cfg(test)]
mod tests;
